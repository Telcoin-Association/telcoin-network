//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use crate::{
    codec::{PeerExchangeCodec, TNCodec, TNMessage},
    error::NetworkError,
    kad::{node_record_key, KadStore},
    metrics::{
        ConnectionLimitReason, InboundDenial, InboundFailureOutcome, PeerManagerMetrics,
        SwarmMetrics,
    },
    peers::{self, LoadPenalty, PeerEvent, PeerManager, Penalty, PutRecordRate},
    quic_incoming::QuicIncomingLimits,
    record_exchange::{RecordCodec, RecordExchange, RecordResponse},
    send_or_log_error,
    service_class::{InboundOccupancy, ServiceClass},
    stream::{StreamBehavior, StreamEvent},
    types::{
        GossipPayload, KadQuery, NetworkCommand, NetworkEvent, NetworkHandle, NetworkInfo,
        NetworkResponseMessage, NetworkResponseSender, NetworkResult, NetworkType, NetworkTypeExt,
        NodeRecord, RecordDomain, ResponseChannel, RpcInfo,
    },
    PeerExchangeMap,
};
use futures::StreamExt as _;
use libp2p::{
    connection_limits::{self, ConnectionLimits},
    gossipsub::{
        self, Event as GossipEvent, IdentTopic, Message as GossipMessage, MessageAcceptance,
        PublishError, Topic, TopicHash,
    },
    kad::{self, store::RecordStore, Mode, QueryId},
    request_response::{
        self, Codec, Event as ReqResEvent, InboundFailure as ReqResInboundFailure,
        InboundRequestId, OutboundFailure as ReqResOutboundFailure, OutboundRequestId,
        ProtocolSupport,
    },
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr, PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use lru::LruCache;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::ErrorKind,
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tn_config::{
    KeyConfig, LibP2pConfig, NetworkConfig, PeerConfig, SwarmNetworkBudget, MAX_GOSSIP_MESSAGE_SIZE,
};
use tn_types::{
    encode, now, BlsPublicKey, BlsSigner, Database, NetworkKeypair, NetworkPublicKey, TaskSpawner,
    TnSender, TrySendOutcome, WorkerId,
};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tracing::{debug, error, info, instrument, trace, warn};

/// An inbound request that the swarm forwarded to the application and has not yet answered.
#[derive(Debug)]
struct PendingInbound {
    /// The cancel notice to the handler when the request ends.
    notify: oneshot::Sender<()>,
    /// The class that counts this request in the pending occupancy.
    class: ServiceClass,
    /// The time that the swarm forwarded the request, for the service time histogram.
    received: Instant,
}

#[cfg(test)]
#[path = "tests/network_tests.rs"]
mod network_tests;

#[cfg(test)]
#[path = "tests/committee_seeding.rs"]
mod committee_seeding;

#[cfg(test)]
#[path = "tests/network_budget_tests.rs"]
mod network_budget_tests;

#[cfg(test)]
#[path = "tests/inbound_service_tests.rs"]
mod inbound_service_tests;

#[cfg(test)]
#[path = "tests/admission_contention.rs"]
mod admission_contention;

#[cfg(test)]
#[path = "tests/loop_budget_tests.rs"]
mod loop_budget_tests;

/// The unit of work that [`ConsensusNetwork::run`] services next, as chosen by
/// [`next_loop_event`].
#[derive(Debug)]
enum LoopEvent<E, C> {
    /// The record-refresh interval ticked.
    Refresh,
    /// Retry connected peers whose record retrieval was deferred by a budget.
    RecordRetry,
    /// The swarm produced an event.
    Swarm(E),
    /// The command channel produced a command.
    Command(C),
    /// Every command sender is gone, so the network loop must shut down.
    CommandsClosed,
}

/// Wait for the next unit of work of the network loop: a record-refresh tick, a swarm event or a
/// command.
///
/// [`ConsensusNetwork::run`] calls this once per loop iteration. `events` is generic so tests can
/// drive this exact function with a synthetic, always-ready event source.
///
/// Every call first spends one unit of the tokio cooperative budget. The swarm stream spends no
/// budget (libp2p events, QUIC accepts and futures channels are not budget-aware). Without this
/// charge, a flood of ready swarm events never makes the loop return `Pending`, so the task never
/// yields: other tasks on the same worker thread do not run and, on a `current_thread` runtime,
/// the time driver does not turn, so no interval fires. With the charge, the loop serves at most
/// one budget of iterations per scheduler poll, then yields and wakes itself. The charge comes
/// before the `select!`, so a yield never drops a swarm event or a command.
async fn next_loop_event<S, C>(
    record_refresh: &mut tokio::time::Interval,
    events: &mut S,
    commands: &mut Receiver<C>,
) -> LoopEvent<S::Item, C>
where
    S: futures::Stream + futures::stream::FusedStream + Unpin,
{
    tokio::task::coop::consume_budget().await;
    tokio::select! {
        _ = record_refresh.tick() => LoopEvent::Refresh,
        event = events.select_next_some() => LoopEvent::Swarm(event),
        command = commands.recv() => command.map_or(LoopEvent::CommandsClosed, LoopEvent::Command),
    }
}

/// Hard cap on the number of distinct peers retained in
/// [`ConsensusNetwork::published_to_peers`], the de-dup set that records which peers we have
/// already pushed our [`NodeRecord`] to.
///
/// Without a cap this set grows once per distinct `PeerId` ever connected and is never cleaned up
/// (a `PeerId` is a peer-minted cryptographic identity, so a churn of fresh identities grows it
/// without bound), which on a RAM-capped node is a slow but guaranteed OOM. Backing the set with a
/// capacity-bounded LRU caps its resident size to this many entries (~64-80 B each, so well under
/// 1 MB) while preserving the de-dup intent: an actively (re)connecting peer is promoted on every
/// connect and so is never the eviction victim, and only a peer absent long enough to fall out of
/// the LRU is re-pushed to on its eventual return - at worst once, which is self-limiting.
///
/// The value is a generous multiple of the live-peer target (`PeerConfig::max_peers()` defaults to
/// ~33), so the LRU only ever evicts peers well outside the current working set. See issue #828.
const MAX_PUBLISHED_TO_PEERS: NonZeroUsize = NonZeroUsize::new(10_000).expect("10_000 is nonzero");

/// Maximum encoded kademlia message size in bytes, including the record and protocol overhead.
///
/// Pin the 16 KiB wire limit explicitly so libp2p upgrades cannot silently widen the inbound
/// bandwidth allowed by the per-source `PutRecord` limits. The codec applies this bound before
/// records reach the store, whose larger value limit is not the effective wire bound.
const MAX_KAD_PACKET_SIZE: usize = 16 * 1024;

/// Whether inbound record processing was completed or deferred by the shared PUT budget.
enum PutOutcome {
    /// The record was handled, including ordinary validation or ban rejection.
    Processed,
    /// The shared rate limiter shed the record before validation.
    Shed,
}

pub(crate) use tn_node_record::MAX_ADVERTISED_MULTIADDRS;

/// Freshness of a validated incoming record relative to the locally stored value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordFreshness {
    /// No record is stored, or the incoming timestamp is strictly newer.
    Newer,
    /// Both timestamps and the complete signed values match.
    Identical,
    /// The timestamp is older, or equal with a different signed value.
    Older,
    /// A stored or incoming value cannot be decoded for comparison.
    Undecodable,
}

/// Maximum number of concurrent established connections a single peer may hold, across both
/// directions (inbound and outbound).
///
/// libp2p reports every established connection to the swarm but imposes no per-peer ceiling of its
/// own. The peer-count admission gate (`PeerConfig::max_peers`) counts *distinct* `PeerId`s, so one
/// peer holding many simultaneous connections still counts as one, and the inbound admission
/// callback rejects only self-connections and banned peers. Without this cap a single unbanned peer
/// could open connections up to the OS / QUIC file-descriptor and memory limits. Installing a
/// [`connection_limits::Behaviour`] with this per-peer bound closes that gap (issue #1010).
///
/// The value is generous headroom over legitimate use: a peer needs at most one inbound and one
/// outbound connection concurrently (this node dials with `PeerCondition::Disconnected`, so it does
/// not stack redundant outbound dials), and brief reconnection churn adds only a small transient
/// overlap. Eight leaves room for that churn while bounding a hostile peer to a fixed, small number
/// of connections instead of an unbounded fan-out.
const MAX_ESTABLISHED_CONNECTIONS_PER_PEER: u32 = 8;

/// Memory-only ceiling on pending inbound connections (accepted handshakes that are not yet
/// established) for one swarm.
///
/// This is a last-resort memory bound, not an admission policy. With QUIC Retry enabled (the
/// default), the listener validates the source address before accepting a handshake, so occupying
/// a slot requires a validated round trip. If `retry_unvalidated_incoming` is disabled as an
/// operator rollback, a forged QUIC Initial datagram can hold a slot for the transport timeout
/// (about 10 seconds, see [`connection_limits_behaviour`]). A full budget refuses every new inbound
/// handshake, committee peers included, so the value is sized to bound memory only.
///
/// The value does not depend on [`PeerConfig`]. The peer manager has no inbound admission ceiling
/// for this budget to mirror: it admits every connection that is not banned and disconnects excess
/// peers that are not important only after establishment, and validators and allowlisted peers have
/// no count ceiling. A dial that this budget refuses is retried only by `dial_peer_bls` (committee
/// dials at epoch start, with a bounded backoff that gives up once other peers are connected).
///
/// Established connections do not count against this budget, so connected peers are not affected
/// when it is full, and neither are this node's own outbound dials.
const MAX_PENDING_INCOMING_CONNECTIONS: u32 = 1024;

/// Minimum time between two operator warnings about inbound connections that a
/// `connection_limits` bound refuses (see [`InboundDenialWarning`]).
const INBOUND_DENIAL_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Rate limit for the operator warning about inbound connections that a `connection_limits` bound
/// refuses.
///
/// Every refusal is counted in the `tn_network.inbound_connections_denied_total` metric and logged
/// at `debug`. The warning fires only when refusals persist: the first refusal opens a window, and
/// the first refusal at least [`INBOUND_DENIAL_WARN_INTERVAL`] after the window opened fires the
/// warning with the number of refusals in the window and closes the window.
#[derive(Debug, Default)]
struct InboundDenialWarning {
    /// When the current window opened, or `None` if no refusal was counted since the last warning.
    window_start: Option<tokio::time::Instant>,
    /// The number of refusals counted in the current window.
    denied: u64,
}

impl InboundDenialWarning {
    /// Count one refusal at `now`. Return the number of refusals in the window when the warning is
    /// due, and `None` otherwise.
    fn record(&mut self, now: tokio::time::Instant) -> Option<u64> {
        let window_start = *self.window_start.get_or_insert(now);
        self.denied = self.denied.saturating_add(1);
        (now.saturating_duration_since(window_start) >= INBOUND_DENIAL_WARN_INTERVAL)
            .then(|| std::mem::take(self).denied)
    }
}

/// Build the [`connection_limits::Behaviour`] for one swarm.
///
/// It sets the following bounds:
/// - at most the process allocation's per-peer ceiling, or [`MAX_ESTABLISHED_CONNECTIONS_PER_PEER`]
///   without an allocation, concurrent established connections per peer (issue #1010);
/// - at most the process allocation's established connections in total, when configured;
/// - at most `max_pending_incoming` concurrent pending inbound connections in total (production
///   passes [`MAX_PENDING_INCOMING_CONNECTIONS`]).
///
/// Both directions and all peer classes consume the process allocation: this behaviour must never
/// install bypass peer IDs.
///
/// Pending inbound slot lifecycle (libp2p `connection_limits::Behaviour` owns every slot):
/// - acquire: `handle_pending_inbound_connection` takes a slot, keyed by `ConnectionId`, only when
///   the count is below the budget. Otherwise it denies the connection with
///   [`connection_limits::Exceeded`] and takes no slot. A refusal by an earlier sub-behaviour (for
///   example the banned-IP check in `peer_manager`) happens before this point, so it takes no slot
///   either.
/// - release: `handle_established_inbound_connection` frees the slot before the per-peer check, and
///   `FromSwarm::ListenFailure` frees it on every other outcome. The swarm emits `ListenFailure`
///   when a pending hook refuses the connection, when an established hook refuses it, and when the
///   pending upgrade fails, times out or is aborted. The slots are a set of `ConnectionId`s, so a
///   second release of the same id and a release of an id that holds no slot free nothing.
///
/// Hold time: the libp2p `SwarmBuilder` wraps the transport in a `TransportTimeout` with a 10
/// second default, which is shorter than the configured QUIC `handshake_timeout`. So an unfinished
/// inbound handshake holds its slot for about 10 seconds at most, not for the QUIC value.
///
/// The pending inbound ceiling applies to each swarm separately. Each primary and worker network
/// builds its own [`TNBehavior`], so the host total is this ceiling times the number of swarms.
///
/// Pending outgoing and per-direction established caps stay unbounded. Established totals are
/// unbounded without a process allocation. Shared by [`TNBehavior::new`] and the regression tests
/// so all of them exercise the identical limits.
fn connection_limits_behaviour(
    max_pending_incoming: u32,
    budget: Option<SwarmNetworkBudget>,
) -> connection_limits::Behaviour {
    connection_limits::Behaviour::new(
        ConnectionLimits::default()
            .with_max_established_per_peer(Some(
                budget.map_or(MAX_ESTABLISHED_CONNECTIONS_PER_PEER, |budget| {
                    budget.connections_per_peer()
                }),
            ))
            .with_max_established(budget.map(|budget| budget.connections()))
            .with_max_pending_incoming(Some(max_pending_incoming)),
    )
}

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior composes multiple sub-behaviors:
/// - `peer_manager`: Connection management and peer scoring
/// - `gossipsub`: Flood publishing for certificates and batches
/// - `req_res`: Point-to-point request-response messages
/// - `peer_exchange`: Dedicated request-response protocol for the goodbye exchange
/// - `record_exchange`: Bounded retrieval of a connected peer's current signed record
/// - `kademlia`: Distributed hash table for peer discovery
/// - `stream`: Stream-based bulk data transfer for state sync
///
/// **Field order matters**: `NetworkBehaviour` derive calls `handle_established_*_connection`
/// on sub-behaviors in declaration order, short-circuiting on `Err(ConnectionDenied)`.
/// `peer_manager` must be first so banned-peer denials fire before other behaviors
/// (e.g. `req_res`) register the connection in their internal state.
#[derive(NetworkBehaviour)]
pub(crate) struct TNBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
{
    /// The peer manager — first so banned-peer denials short-circuit
    /// before other behaviors register the connection.
    pub(crate) peer_manager: peers::PeerManager,
    /// Per-peer established connection ceiling (issue #1010), optional per-swarm process
    /// allocation, and memory-only ceiling on pending inbound connections (see
    /// [`connection_limits_behaviour`]).
    ///
    /// Placed immediately after `peer_manager` so self / banned denials still fire first (a banned
    /// peer or IP is rejected before it is counted here or takes a pending slot), and before the
    /// remaining behaviors so an over-cap connection is denied before `req_res` / `gossipsub` /
    /// `kademlia` register any per-peer state for it.
    pub(crate) connection_limits: connection_limits::Behaviour,
    /// The gossipsub network behavior.
    pub(crate) gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    pub(crate) req_res: request_response::Behaviour<C>,
    /// Dedicated request-response behavior for the peer-exchange goodbye.
    ///
    /// Preferred over the [`PeerExchangeMap`] variants embedded in the consensus
    /// request enums; goodbyes fall back to the embedded variant when the peer
    /// has not upgraded yet. The embedded variants stay on the wire until the
    /// coordinated `/0.0.2` protocol bump.
    pub(crate) peer_exchange: request_response::Behaviour<PeerExchangeCodec>,
    /// Signed self-record retrieval, isolated from consensus RPC stream capacity.
    pub(crate) record_exchange: request_response::Behaviour<RecordCodec>,
    /// Used for peer discovery.
    pub(crate) kademlia: kad::Behaviour<KadStore<DB>>,
    /// Stream-based sync behavior for bulk data transfer.
    pub(crate) stream: StreamBehavior,
}

impl<C, DB> TNBehavior<C, DB>
where
    C: Codec + Send + Clone + 'static,
    DB: Database,
{
    /// Create a new instance of Self.
    ///
    /// The request-response behaviours are consensus RPCs, goodbyes and record retrieval.
    pub(crate) fn new(
        local_peer_id: PeerId,
        gossipsub: gossipsub::Behaviour,
        req_res: (
            request_response::Behaviour<C>,
            request_response::Behaviour<PeerExchangeCodec>,
            request_response::Behaviour<RecordCodec>,
        ),
        kademlia: kad::Behaviour<KadStore<DB>>,
        peer_config: &PeerConfig,
        metrics: PeerManagerMetrics,
        stream_protocol: StreamProtocol,
    ) -> Self {
        let peer_manager = PeerManager::new(local_peer_id, peer_config, metrics);
        let connection_limits = connection_limits_behaviour(MAX_PENDING_INCOMING_CONNECTIONS, None);
        let (req_res, peer_exchange, record_exchange) = req_res;
        let stream = StreamBehavior::new(stream_protocol);
        Self {
            peer_manager,
            connection_limits,
            gossipsub,
            req_res,
            peer_exchange,
            record_exchange,
            kademlia,
            stream,
        }
    }
}

/// A goodbye dispatched on the dedicated peer-exchange protocol, awaiting the ack.
///
/// Holds everything needed to fall back to the legacy embedded exchange if the
/// peer turns out not to support the dedicated protocol.
#[derive(Debug)]
struct PendingGoodbye {
    /// The exchange map, retained so an `UnsupportedProtocols` failure can resend
    /// it as the embedded legacy variant.
    exchange: PeerExchangeMap,
    /// Notifies the disconnect-deadline task how the goodbye resolved.
    ///
    /// Dropping the sender wakes the task, which disconnects: the correct default
    /// for every resolution except a legacy fallback.
    notify: oneshot::Sender<GoodbyeOutcome>,
}

/// How a goodbye on the dedicated peer-exchange protocol resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GoodbyeOutcome {
    /// The peer acked the exchange: safe to disconnect immediately.
    Acked,
    /// The peer does not support the dedicated protocol; the goodbye was re-sent
    /// on the legacy embedded path, which owns the disconnect from here.
    FellBack,
}

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
pub struct ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>>,
{
    /// The gossip network for flood publishing sealed batches.
    swarm: Swarm<TNBehavior<TNCodec<Req, Res>, DB>>,
    /// The stream for forwarding network events.
    event_stream: Events,
    /// The sender for network handles.
    handle: Sender<NetworkCommand<Req, Res>>,
    /// The receiver for processing network handle requests.
    commands: Receiver<NetworkCommand<Req, Res>>,
    /// The collection of authorized publishers per topic.
    ///
    /// This set must be updated at the start of each epoch. It is used to verify messages
    /// published on certain topics. These are updated when the caller subscribes to a topic.
    authorized_publishers: HashMap<String, Option<HashSet<BlsPublicKey>>>,
    /// The collection of pending _graceful_ disconnects.
    ///
    /// This node disconnects from new peers if it already has the target number of peers.
    /// For these types of "peer exchange / discovery disconnects", the node shares peer records
    /// before disconnecting. This keeps track of the number of disconnects to ensure resources
    /// aren't starved while waiting for the peer's ack.
    pending_px_disconnects: HashMap<OutboundRequestId, PeerId>,
    /// The collection of pending goodbyes on the dedicated peer-exchange protocol.
    ///
    /// Tracked separately from `pending_px_disconnects`: request ids are scoped to
    /// the behaviour that issued them, so ids from the dedicated protocol could
    /// collide with the legacy req-res ids. Each entry retains the exchange map so
    /// a goodbye that fails with `UnsupportedProtocols` can fall back to the
    /// legacy variant embedded in the consensus request enum.
    pending_goodbyes: HashMap<OutboundRequestId, PendingGoodbye>,
    /// The collection of pending outbound requests.
    ///
    /// Callers include a oneshot channel for the network to return response. The caller is
    /// responsible for decoding message bytes and reporting peers who return bad data. Peers that
    /// send messages that fail to decode must receive an application score penalty.
    outbound_requests: HashMap<(PeerId, OutboundRequestId), NetworkResponseSender<Res>>,
    /// The collection of pending inbound requests.
    ///
    /// Callers include a oneshot channel for the network to return a cancellation notice. The
    /// caller is responsible for decoding message bytes and reporting peers who return bad
    /// data. Peers that send messages that fail to decode must receive an application score
    /// penalty.
    inbound_requests: HashMap<InboundRequestId, PendingInbound>,
    /// The pending inbound requests by service class. Changes only with `inbound_requests`.
    inbound_pending: InboundOccupancy,
    /// The collection of kademlia record requests.
    ///
    /// When the application layer makes a request, the swarm stores the kad::QueryId and the
    /// the bls key associated with the desired authority's [NodeRecord]. The query runs until
    /// the last step. During this time, results are tracked and compared to one another to
    /// ensure the latest valid record is used for the peer's info.
    kad_record_queries: HashMap<QueryId, PendingKadQuery>,
    /// The configurables for the libp2p consensus network implementation.
    config: LibP2pConfig,
    /// Track peers we have a connection with.
    ///
    /// This explicitly tracked and is a VecDeque so we can use to round robin requests without an
    /// explicit peer.
    connected_peers: VecDeque<PeerId>,
    /// Key manager, provide the BLS public key and sign peer records published to kademlia.
    key_config: KeyConfig,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The signed [NodeRecord].
    ///
    /// The external address is self-reported and unconfirmed.
    node_record: NodeRecord,
    /// The `(chain, role)` domain this node signs and verifies records for.
    ///
    /// Folded into every [NodeRecord] signature so a record signed for one
    /// network never verifies on another (GHSA-cc64-wfq5-56ph).
    record_domain: RecordDomain,
    /// Signature-verified bytes for this process and domain, bounded by the live-peer budget.
    /// Persisted records alone never authorize skipping signature verification.
    verified_peer_records: LruCache<kad::RecordKey, Vec<u8>>,
    /// Peers we have already pushed our [NodeRecord] to.
    ///
    /// A peer needs our record before it can resolve our BLS key, so we push it on
    /// `PeerConnected`. The last-connection close clears this marker because the receiver
    /// relinquishes connection-owned retention and needs another advertisement on reconnect.
    ///
    /// A bounded LRU limits metadata for concurrent connections and failed publication attempts.
    /// Entries survive intermediate connection closes and are removed on the last close. The
    /// resident cap is [`MAX_PUBLISHED_TO_PEERS`].
    published_to_peers: LruCache<PeerId, ()>,
    /// Bounded record retrievals and cooldown history, independent of push suppression.
    record_exchange: RecordExchange,
    /// Heartbeat cadence for retrying deferred retrievals, clamped above zero.
    record_retry_interval: Duration,
    /// Prometheus metrics for swarm-level events (gossip, requests).
    metrics: SwarmMetrics,
    /// Rate limit for the warning about inbound connections that a `connection_limits` bound
    /// refuses.
    inbound_denial_warning: InboundDenialWarning,
    /// Decision counters of this swarm's QUIC listener (Retry, Accept, Refuse, Ignore,
    /// budget yields), mirrored into [`SwarmMetrics`] once per event-loop iteration.
    quic_incoming: std::sync::Arc<libp2p::quic::IncomingStats>,
}

impl<Req, Res, DB, Events> ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>> + Send + 'static,
{
    /// Convenience method for spawning a primary network instance.
    pub fn new_for_primary(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
    ) -> NetworkResult<Self> {
        let network_key = key_config.primary_network_keypair().clone();
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            NetworkType::Primary,
            external_addr,
            None,
        )
    }

    /// Convenience method for spawning a worker network instance.
    #[allow(clippy::too_many_arguments)]
    pub fn new_for_worker(
        worker_id: WorkerId,
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        db: DB,
        task_manager: TaskSpawner,
        external_addr: Multiaddr,
        rpc: Option<RpcInfo>,
    ) -> NetworkResult<Self> {
        let network_key = key_config.worker_network_keypair(worker_id);
        Self::new(
            network_config,
            event_stream,
            key_config,
            network_key,
            db,
            task_manager,
            NetworkType::Worker(worker_id),
            external_addr,
            rpc,
        )
    }

    /// Create a new instance of Self.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        network_config: &NetworkConfig,
        event_stream: Events,
        key_config: KeyConfig,
        keypair: NetworkKeypair,
        db: DB,
        task_spawner: TaskSpawner,
        network_type: NetworkType,
        external_addr: Multiaddr,
        rpc: Option<RpcInfo>,
    ) -> NetworkResult<Self> {
        let budget = network_config.swarm_budget().map_err(std::io::Error::other)?;
        let quic_config = network_config.quic_config().with_budget(budget);
        // Namespace every wire protocol by the genesis chain id so nodes on
        // different chains never negotiate a connection. The id is stamped onto
        // the network config from genesis at node startup; see
        // `NetworkConfig::set_chain_id`.
        let chain_id = network_config.libp2p_config().chain_id;
        // The `(chain, role)` domain every NodeRecord this node signs and verifies
        // is scoped to: a record signed for one network never verifies on another
        // (GHSA-cc64-wfq5-56ph).
        let record_domain = RecordDomain::new(chain_id, network_type);

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // explicitly set default
            .heartbeat_interval(Duration::from_secs(1))
            // explicitly set default
            .validation_mode(gossipsub::ValidationMode::Strict)
            // TN specific: filter against authorized_publishers for certain topics
            .validate_messages()
            // Gossipsub negotiates its own `/meshsub` protocol, independent of the
            // req-res/kad/stream names below, so without this it is the one wire
            // protocol two chains still share: namespacing the topics keeps their
            // messages apart but still lets cross-chain peers negotiate a gossip
            // substream. Folding the chain id into the protocol id closes that gap.
            // The builder appends `/1.1.0` and `/1.0.0`, yielding
            // `/tn-meshsub-{chain_id}/1.1.0` and `/tn-meshsub-{chain_id}/1.0.0`.
            .protocol_id_prefix(crate::types::gossip_protocol_id_prefix(chain_id))
            .build()?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .map_err(NetworkError::GossipBehavior)?;

        let tn_codec =
            TNCodec::<Req, Res>::new(network_config.libp2p_config().max_rpc_message_size);

        let req_res = request_response::Behaviour::with_codec(
            tn_codec,
            vec![(network_type.req_res_protocol(chain_id)?, ProtocolSupport::Full)],
            request_response::Config::default(),
        );

        // Dedicated goodbye protocol: the same hardened codec under its own wire
        // name, so the peer-exchange map no longer has to ride inside the consensus
        // request enums. The embedded variants remain as the fallback for
        // not-yet-upgraded peers until the coordinated `/0.0.2` bump.
        let px_codec = PeerExchangeCodec::new(network_config.libp2p_config().max_rpc_message_size);
        let peer_exchange = request_response::Behaviour::with_codec(
            px_codec,
            vec![(network_type.peer_exchange_protocol(chain_id)?, ProtocolSupport::Full)],
            request_response::Config::default(),
        );
        // Reuse the kad wire ceiling in both codec directions. Two shared stream slots allow
        // simultaneous inbound and outbound retrievals, with the existing dial timeout.
        let record_exchange = request_response::Behaviour::with_codec(
            RecordCodec::new(MAX_KAD_PACKET_SIZE),
            vec![(network_type.record_exchange_protocol(chain_id)?, ProtocolSupport::Full)],
            request_response::Config::default()
                .with_max_concurrent_streams(2)
                .with_request_timeout(network_config.peer_config().dial_timeout),
        );
        let record_retry_interval =
            Duration::from_secs(network_config.peer_config().heartbeat_interval)
                .max(Duration::from_secs(1));
        let peer_id: PeerId = keypair.public().into();
        let mut kad_config = libp2p::kad::Config::new(network_type.kad_protocol(chain_id)?);
        // manually add peers
        kad_config.set_kbucket_inserts(kad::BucketInserts::Manual);
        let libp2p = network_config.libp2p_config();
        kad_config.set_kbucket_size(libp2p.k_bucket_size);
        configure_record_jobs(&mut kad_config);
        kad_config
            .set_max_packet_size(MAX_KAD_PACKET_SIZE)
            .set_record_ttl(Some(libp2p.kad_record_ttl))
            .set_record_filtering(kad::StoreInserts::FilterBoth)
            .set_query_timeout(Duration::from_secs(60))
            .set_provider_record_ttl(Some(libp2p.kad_record_ttl));
        let mut kad_store = KadStore::new(db.clone(), peer_id, &key_config, network_type);

        // Load the kad records from the DB into the local peer cache, verifying each
        // against this node's `(chain, role)` domain so a record poisoned onto the
        // store by a pre-fix node (GHSA-cc64-wfq5-56ph) is scrubbed on load instead of
        // re-promoted. Collect entries that fail to decode/verify, or whose key is
        // broken, for removal.
        let mut known = Vec::new();
        let mut corrupt = Vec::new();
        for record in kad_store.records() {
            match BlsPublicKey::from_literal_bytes(record.key.as_ref()) {
                Ok(key) => {
                    match NodeRecord::decode_and_verify(record.value.as_ref(), record_domain, &key)
                    {
                        Some((_key, node_record)) => known.push((key, node_record.info)),
                        None => corrupt.push(record.key.clone()),
                    }
                }
                // How did we get a KAD record with a broken key?
                Err(error) => {
                    error!(target: "network-kad", ?error, "Invalid/corrupt KAD DB store!");
                    corrupt.push(record.key.clone());
                }
            }
        }

        // Purge corrupt records before moving the store into the kademlia behaviour
        // so its record accounting stays accurate.
        for key in corrupt {
            warn!(target: "network-kad", ?key, "removing invalid record from kad store (undecodable or wrong signing domain)");
            kad_store.remove(&key);
        }

        // Give the provider tables the same tolerant startup load: purge any provider
        // rows whose bytes no longer decode (schema/version skew or corruption) so the
        // first post-restart provider read can not panic the ConsensusNetwork task and
        // then repeat that panic on every restart (issue #999).
        let purged_providers = kad_store.scrub_corrupt_providers();
        if purged_providers > 0 {
            warn!(
                target: "network-kad",
                purged_providers,
                "purged undecodable provider records from kad store at startup"
            );
        }

        kad_store
            .enable_retention()
            .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()))?;
        let kademlia = kad::Behaviour::with_config(peer_id, kad_store, kad_config);

        // create custom behavior
        let stream_protocol = crate::types::stream_protocol(network_type, chain_id)?;
        let mut behavior = TNBehavior::new(
            peer_id,
            gossipsub,
            (req_res, peer_exchange, record_exchange),
            kademlia,
            network_config.peer_config(),
            PeerManagerMetrics::new_for(&network_type),
            stream_protocol,
        );
        behavior.connection_limits =
            connection_limits_behaviour(MAX_PENDING_INCOMING_CONNECTIONS, budget);

        // Promote the surviving records into the local peer cache. The store's contents are
        // peer-fillable (arbitrary signature-valid third-party records held as DHT storage
        // duty), so entries are restored UNPINNED and stay prunable at the first committee
        // rotation. Our own record is skipped: both primary and worker key their record by
        // the primary BLS key, and there is no point caching ourselves as a known peer.
        let own_key = key_config.primary_public_key();
        let mut restored: usize = 0;
        for (key, info) in known {
            if key == own_key {
                continue;
            }
            behavior.peer_manager.add_restored_peer(key, info);
            kad_store.record_timestamp(&crate::kad::node_record_key(&key)).into_iter().for_each(
                |timestamp| {
                    behavior.peer_manager.restore_record_timestamp(key, timestamp);
                },
            );
            restored += 1;
        }
        if restored > 0 {
            info!(target: "network-kad", restored, "restored persisted kad records into the local peer cache");
        }

        let network_pubkey = keypair.public().into();

        // QUIC listener hardening: Retry for unvalidated addresses and bounded incoming queues.
        let quic_incoming = std::sync::Arc::new(libp2p::quic::IncomingStats::default());
        let quic_limits = QuicIncomingLimits::new(
            network_config.peer_config().max_priority_peers(),
            MAX_ESTABLISHED_CONNECTIONS_PER_PEER,
        );
        let quic_stats = std::sync::Arc::clone(&quic_incoming);

        // create swarm
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic_config(|config| {
                let mut config = quic_config.apply_to(config);
                quic_limits.apply(
                    &mut config,
                    network_config.quic_config().retry_unvalidated_incoming,
                    quic_stats,
                );
                config
            })
            .with_behaviour(|_| behavior)
            .map_err(|_| NetworkError::BuildSwarm)?
            .with_swarm_config(|c| {
                c.with_idle_connection_timeout(
                    network_config.libp2p_config().max_idle_connection_timeout,
                )
            })
            .build();

        // set external address
        swarm.add_external_address(external_addr.clone());

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        let config = network_config.libp2p_config().clone();
        let pending_px_disconnects = HashMap::with_capacity(config.max_px_disconnects);
        let pending_goodbyes = HashMap::with_capacity(config.max_px_disconnects);
        let node_record = Self::create_node_record(
            record_domain,
            external_addr.clone(),
            &key_config,
            network_pubkey,
            rpc,
        );

        Ok(Self {
            swarm,
            handle,
            commands,
            event_stream,
            authorized_publishers: Default::default(),
            outbound_requests: Default::default(),
            inbound_requests: Default::default(),
            inbound_pending: InboundOccupancy::default(),
            kad_record_queries: Default::default(),
            config,
            connected_peers: VecDeque::new(),
            pending_px_disconnects,
            pending_goodbyes,
            key_config,
            task_spawner,
            node_record,
            record_domain,
            verified_peer_records: LruCache::new(
                NonZeroUsize::new(network_config.peer_config().max_peers())
                    .unwrap_or(NonZeroUsize::MIN),
            ),
            published_to_peers: LruCache::new(MAX_PUBLISHED_TO_PEERS),
            record_exchange: RecordExchange::new(
                MAX_PUBLISHED_TO_PEERS,
                network_config.peer_config().max_peers(),
                record_retry_interval,
            ),
            record_retry_interval,
            metrics: SwarmMetrics::new_for(&network_type).with_capacity(&quic_config, budget),
            inbound_denial_warning: InboundDenialWarning::default(),
            quic_incoming,
        })
    }

    /// Return a [NetworkHandle] to send commands to this network.
    pub fn network_handle(&self) -> NetworkHandle<Req, Res> {
        NetworkHandle::new(self.handle.clone())
    }

    /// Configure ordered externally reachable endpoints independently of the swarm's listeners.
    ///
    /// Must be called before running the network. Uses the same signed schema and domain as a
    /// single-address record and replaces the constructor's external address completely.
    pub fn with_advertised_addresses(mut self, addresses: Vec<Multiaddr>) -> NetworkResult<Self> {
        let record = NodeRecord::build_multi(
            self.record_domain,
            self.node_record.info.pubkey.clone(),
            addresses,
            self.node_record.info.rpc.clone(),
            |data| self.key_config.request_signature_direct(data),
        )?;
        self.node_record.info.multiaddrs.iter().for_each(|address| {
            self.swarm.remove_external_address(address);
        });
        record.info.multiaddrs.iter().cloned().for_each(|address| {
            self.swarm.add_external_address(address);
        });
        self.node_record = record;
        Ok(self)
    }

    /// Create and sign this node's [NodeRecord].
    fn create_node_record(
        domain: RecordDomain,
        external_addr: Multiaddr,
        key_config: &KeyConfig,
        network_pubkey: NetworkPublicKey,
        rpc: Option<RpcInfo>,
    ) -> NodeRecord {
        NodeRecord::build(domain, network_pubkey, external_addr, rpc, |data| {
            key_config.request_signature_direct(data)
        })
    }

    /// Re-sign our configured network information and publish it with a fresh timestamp.
    ///
    /// `provide_our_data` replaces the local store entry before publishing, so subsequent
    /// direct pushes and record lookups use the new signed value. Our local copy keeps
    /// `expires: None`; Kademlia assigns the configured TTL to outbound copies. The libp2p-kad
    /// record job is disabled (see [`configure_record_jobs`]), so this is the only periodic
    /// republication of our record.
    fn refresh_own_record(&mut self) {
        self.node_record
            .refresh(self.record_domain, |data| self.key_config.request_signature_direct(data));
        self.provide_our_data();
    }

    /// Return a kademlia record keyed on our BlsPublicKey with our peer_id and network addresses.
    /// Return None if we don't have any confirmed external addresses yet.
    fn get_peer_record(&self) -> kad::Record {
        let key = node_record_key(&self.key_config.primary_public_key());
        // Leave `expires: None` for our OWN record. The local row keeps the value given to
        // `put_record`, so `None` never lapses on our read path. `put_record` and
        // `put_record_to` fill a fresh `now + kad_record_ttl` into each outbound copy, so the
        // configured `kad_record_ttl` still drives the wire-level expiry that remote peers store.
        kad::Record {
            key: key.clone(),
            value: encode(&self.node_record),
            publisher: Some(*self.swarm.local_peer_id()),
            expires: None,
        }
    }

    /// Verify the address list in Record was signed by the key and the kad record's publisher
    /// matches the network key.
    fn peer_record_valid(&self, record: &kad::Record) -> Option<(BlsPublicKey, NodeRecord)> {
        let key = BlsPublicKey::from_literal_bytes(record.key.as_ref()).ok()?;

        // decode (with legacy fallback for pre-upgrade peers) and verify bls signature
        let cached = self
            .verified_peer_records
            .peek(&record.key)
            .is_some_and(|value| *value == record.value);
        let (pubkey, node_record) = if cached {
            (key, NodeRecord::try_decode_compat(&record.value)?)
        } else {
            NodeRecord::decode_and_verify(record.value.as_ref(), self.record_domain, &key)?
        };

        // The shared decoder validates nonempty, bounded IP/QUIC endpoints before BLS verification.
        if node_record.info.multiaddrs.len() > MAX_ADVERTISED_MULTIADDRS {
            warn!(
                target: "network-kad",
                count = node_record.info.multiaddrs.len(),
                max = MAX_ADVERTISED_MULTIADDRS,
                "NodeRecord validation failed: advertised multiaddr count exceeds cap"
            );
            return None;
        }

        // verify publisher matches the network public key in the record
        // this prevents replay attacks where malicious nodes republish outdated records
        let expected_peer_id: PeerId = node_record.info.pubkey.clone().into();
        if record.publisher != Some(expected_peer_id) {
            warn!(
                target: "network-kad",
                "NodeRecord validation failed: publisher {:?} doesn't match network key (expected {:?})",
                record.publisher, expected_peer_id
            );
            return None;
        }

        Some((pubkey, node_record))
    }

    /// Publish and provide our network addresses and peer id under our BLS public key for
    /// discovery.
    fn provide_our_data(&mut self) {
        let record = self.get_peer_record();
        info!(target: "network-kad", ?record, "Providing our record to kademlia for peer {:?}", self.swarm.local_peer_id());
        let key = record.key.clone();
        if let Err(err) = self.swarm.behaviour_mut().kademlia.put_record(record, kad::Quorum::One) {
            match &err {
                kad::store::Error::ValueTooLarge => error!(
                    target: "network-kad",
                    "node record exceeds kad value-size limit; RPC endpoint NOT advertised to peers ({err})"
                ),
                _ => error!(target: "network-kad", "Failed to store record locally: {err}"),
            }
        }
        if let Err(err) = self.swarm.behaviour_mut().kademlia.start_providing(key) {
            error!(target: "network-kad", "Failed to start providing key: {err}");
        }
    }

    /// Push our [NodeRecord] directly to a newly-connected peer.
    ///
    /// Used on first-time connections so the remote peer can resolve our BLS key
    /// without waiting for the kad publication interval (12h). Callers must
    /// short-circuit on reconnects - see [`Self::published_to_peers`]
    fn publish_our_data_to_peer(&mut self, peer: PeerId) {
        let record = self.get_peer_record();
        info!(target: "network-kad", "Publishing our record to peer {peer:?}");
        let _ = self.swarm.behaviour_mut().kademlia.put_record_to(
            record,
            vec![peer].into_iter(),
            kad::Quorum::One,
        );
    }

    /// Record that we have pushed our [`NodeRecord`] to `peer_id`, returning `true` the first time
    /// we see a peer (i.e. when a direct push is warranted) and `false` for a peer we have already
    /// pushed to.
    ///
    /// Backed by the capacity-bounded [`Self::published_to_peers`] LRU so this de-dup gate cannot
    /// grow without bound. A hit promotes the peer to most-recently-used, so an actively
    /// (re)connecting peer is never evicted and never re-pushed to; only a peer absent long enough
    /// to fall out of the LRU is pushed to again on its eventual return.
    fn mark_published_to_peer(&mut self, peer_id: PeerId) -> bool {
        self.published_to_peers.put(peer_id, ()).is_none()
    }

    /// Run the network loop to process incoming gossip.
    pub async fn run(mut self) -> NetworkResult<()> {
        // add peer record if address confirmed
        self.swarm.behaviour_mut().kademlia.set_mode(Some(Mode::Server));
        self.provide_our_data();

        // Startup already published our record. Refresh only after the first full interval,
        // and skip missed ticks to avoid a burst of signing and publication after a stall.
        let mut record_refresh = tokio::time::interval_at(
            tokio::time::Instant::now() + self.config.kad_publication_interval,
            self.config.kad_publication_interval,
        );
        record_refresh.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut record_retry = tokio::time::interval(self.record_retry_interval);
        record_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let event = tokio::select! {
                event = next_loop_event(&mut record_refresh, &mut self.swarm, &mut self.commands) => event,
                _ = record_retry.tick() => LoopEvent::RecordRetry,
            };
            match event {
                LoopEvent::Refresh => self.refresh_own_record(),
                LoopEvent::RecordRetry => self.retry_record_requests(),
                LoopEvent::Swarm(event) => {
                    if let Err(e) = self.process_event(event).await {
                        error!(target: "network", ?e, "network event error");
                        if let NetworkError::AllListenersClosed = e {
                            // In this case go ahead and kill the node.
                            return Err(e);
                        }
                    }
                }
                LoopEvent::Command(c) => {
                    if let Err(e) = self.process_command(c) {
                        error!(target: "network", ?e, "network command error")
                    }
                }
                LoopEvent::CommandsClosed => {
                    info!(target: "network", "network shutting down...");
                    return Ok(());
                }
            }

            // refresh in-flight gauges once per loop iteration (scrape-interval freshness)
            self.metrics.set_pending(self.goodbyes_in_flight(), self.outbound_requests.len());
            self.metrics.set_established_connections(
                self.swarm.network_info().connection_counters().num_established(),
            );
            let (pending, deferred) = self.record_exchange.counts();
            self.metrics.set_record_exchange_pending(pending, deferred);
            self.metrics.record_quic_incoming(&self.quic_incoming);
        }
    }

    /// Process events from the swarm.
    #[instrument(level = "trace", target = "network::events", skip(self), fields(topics = ?self.authorized_publishers.keys()))]
    async fn process_event(
        &mut self,
        event: SwarmEvent<TNBehaviorEvent<TNCodec<Req, Res>, DB>>,
    ) -> NetworkResult<()> {
        match event {
            SwarmEvent::ConnectionClosed { peer_id, num_established: 0, .. } => {
                // Connection-owned rows are gone at the receiver too. A reconnect needs a fresh
                // direct advertisement even when the bounded publication cache saw this peer
                // before.
                self.published_to_peers.pop(&peer_id);
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .release_connected(&peer_id)
                    .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()))?;
            }
            SwarmEvent::Behaviour(behavior) => match behavior {
                TNBehaviorEvent::Gossipsub(event) => self.process_gossip_event(event)?,
                TNBehaviorEvent::ReqRes(event) => self.process_reqres_event(event)?,
                TNBehaviorEvent::PeerExchange(event) => self.process_peer_exchange_event(event)?,
                TNBehaviorEvent::RecordExchange(event) => {
                    self.process_record_exchange_event(event)?
                }
                TNBehaviorEvent::PeerManager(event) => self.process_peer_manager_event(event)?,
                // `connection_limits::Behaviour` emits no events (its `ToSwarm` is `Infallible`);
                // this arm is uninhabited and exists only to keep the match exhaustive over the
                // derived event enum.
                TNBehaviorEvent::ConnectionLimits(event) => match event {},
                TNBehaviorEvent::Kademlia(event) => self.process_kad_event(event)?,
                TNBehaviorEvent::Stream(event) => self.process_stream_event(event)?,
            },
            SwarmEvent::ExternalAddrConfirmed { address } => {
                // protocol expects static IP address
                // emit warning if peers report different external address
                let expected = &self.node_record.info().multiaddrs;
                if !expected.contains(&address) {
                    warn!(target: "network", ?expected, reported=?address, "peer reporting different external addr:")
                }
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                debug!(
                    target: "network",
                    ?address,
                    "listener address expired"
                );
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                // log listener errors
                error!(
                    target: "network",
                    ?listener_id,
                    ?error,
                    "listener error"
                );
            }
            SwarmEvent::ListenerClosed { addresses, reason, .. } => {
                // log errors
                if let Err(e) = reason {
                    error!(target: "network", ?e, "listener unexpectedly closed");
                }

                // critical failure
                if self.swarm.listeners().count() == 0 {
                    error!(target: "network", ?addresses, "no listeners for swarm - network shutting down");
                    return Err(NetworkError::AllListenersClosed);
                }
            }
            // an inbound connection refused by a `connection_limits` bound (the pending inbound
            // ceiling, the per-peer established ceiling or the total established ceiling); count
            // it by the bound that the refusal names, and log only the configured limit and the
            // fixed limit description, never peer-supplied data
            SwarmEvent::IncomingConnectionError {
                error: libp2p::swarm::ListenError::Denied { cause },
                ..
            } => {
                cause.downcast_ref::<connection_limits::Exceeded>().into_iter().for_each(
                    |exceeded| {
                        // classify by the refusal text, not by the peer id: both established
                        // ceilings refuse after authentication, so a peer id cannot tell them apart
                        let reason = ConnectionLimitReason::from_exceeded(exceeded);
                        self.metrics.record_connection_limit_rejection(reason);
                        let denial = InboundDenial::from_reason(reason);
                        self.metrics.record_inbound_denied(&denial);
                        self.inbound_denial_warning
                            .record(tokio::time::Instant::now())
                            .into_iter()
                            .for_each(|denied| {
                                warn!(
                                    target: "network",
                                    denied,
                                    window = ?INBOUND_DENIAL_WARN_INTERVAL,
                                    "inbound connections keep being refused by connection limits"
                                );
                            });
                    },
                );
            }
            SwarmEvent::OutgoingConnectionError {
                error: libp2p::swarm::DialError::Denied { cause },
                ..
            } => {
                cause.downcast_ref::<connection_limits::Exceeded>().into_iter().for_each(
                    |exceeded| {
                        self.metrics.record_connection_limit_rejection(
                            ConnectionLimitReason::from_exceeded(exceeded),
                        );
                    },
                );
            }
            // other events handled by peer manager and other behaviors
            _ => {}
        }
        Ok(())
    }

    /// Process commands for the network.
    fn process_command(&mut self, command: NetworkCommand<Req, Res>) -> NetworkResult<()> {
        match command {
            NetworkCommand::StartListening { multiaddr, reply } => {
                let res = self.swarm.listen_on(multiaddr);
                send_or_log_error!(reply, res, "StartListening");
            }
            NetworkCommand::GetListener { reply } => {
                let addrs = self.swarm.listeners().cloned().collect();
                send_or_log_error!(reply, addrs, "GetListeners");
            }
            NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, network_pubkey, addr, reply } => {
                let admission = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .pin_records([bls_pubkey])
                    .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()));
                if admission.is_ok() {
                    self.swarm.behaviour_mut().peer_manager.add_trusted_peer_and_dial(
                        bls_pubkey,
                        NetworkInfo {
                            pubkey: network_pubkey,
                            multiaddrs: vec![addr],
                            timestamp: now(),
                            rpc: None,
                        },
                        reply,
                    );
                    self.refresh_explicit_peers();
                    self.query_missing_required_records();
                } else {
                    let _ = reply.send(admission);
                }
            }
            NetworkCommand::AddExplicitPeer { bls_pubkey, network_pubkey, addr, reply } => {
                let result = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .pin_records([bls_pubkey])
                    .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()))
                    .map(|_| {
                        self.swarm.behaviour_mut().peer_manager.add_known_peer(
                            bls_pubkey,
                            NetworkInfo {
                                pubkey: network_pubkey,
                                multiaddrs: vec![addr],
                                timestamp: now(),
                                rpc: None,
                            },
                        )
                    });
                let _ = reply.send(result);
                self.query_missing_required_records();
            }
            NetworkCommand::AddBootstrapPeers { peers, reply } => {
                // update peer manager: always pin bootstrap peers (even when a record already
                // exists, e.g. restored unpinned from persistence), but never overwrite an
                // existing record with the config-derived stub. an rpc endpoint the operator
                // configured for the peer is carried through so it is usable before the peer's
                // own record is learned; `cache_known_peer` strips it if malformed
                let result = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .pin_records(peers.keys().copied())
                    .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()))
                    .map(|_| {
                        let peer = &mut self.swarm.behaviour_mut().peer_manager;
                        peers.into_iter().for_each(|(bls, info)| {
                            peer.add_bootstrap_peer(
                                bls,
                                NetworkInfo {
                                    pubkey: info.network_key,
                                    multiaddrs: vec![info.network_address],
                                    timestamp: now(),
                                    rpc: info.rpc,
                                },
                            );
                        });
                    });
                let _ = reply.send(result);
                self.query_missing_required_records();
            }
            NetworkCommand::SeedCommitteePeers { peers, reply } => {
                let result = self.swarm.behaviour_mut().peer_manager.seed_committee_peers(peers);
                let _ = reply.send(result);
            }
            NetworkCommand::Dial { peer_id, peer_addr, reply } => {
                self.swarm.behaviour_mut().peer_manager.dial_peer(
                    peer_id,
                    vec![peer_addr],
                    Some(reply),
                );
            }
            NetworkCommand::DialBls { bls_key, reply } => {
                debug!(target: "network", "command for dial bls {bls_key}");
                if let Some((peer_id, peer_addr)) =
                    self.swarm.behaviour().peer_manager.auth_to_peer(bls_key)
                {
                    self.swarm.behaviour_mut().peer_manager.dial_peer(
                        peer_id,
                        peer_addr,
                        Some(reply),
                    );
                } else {
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::LocalPeerId { reply } => {
                let peer_id = *self.swarm.local_peer_id();
                send_or_log_error!(reply, peer_id, "LocalPeerId");
            }
            NetworkCommand::Publish { topic, msg, reply } => {
                // Enforce `MAX_GOSSIP_MESSAGE_SIZE` at origination, symmetrically with the
                // receive-side check in `verify_gossip`. Honest peers reject an oversized payload
                // as `RejectReason::TooLarge` and Fatal-attribute it to the
                // relaying peer; on the first hop that relayer is the originator,
                // so a node that published an oversized message would be banned by
                // its own neighbours. Refuse locally with a clear error instead, so
                // origination and forwarding apply the identical bound.
                let res = if msg.len() > MAX_GOSSIP_MESSAGE_SIZE {
                    Err(PublishError::MessageTooLarge)
                } else {
                    self.swarm.behaviour_mut().gossipsub.publish(TopicHash::from_raw(topic), msg)
                };
                if res.is_ok() {
                    self.metrics.record_gossip_published();
                }
                send_or_log_error!(reply, res, "Publish");
            }
            NetworkCommand::Subscribe { topic, publishers, reply } => {
                let sub: IdentTopic = Topic::new(&topic);
                let res = self.swarm.behaviour_mut().gossipsub.subscribe(&sub);
                self.authorized_publishers.insert(topic, publishers);
                send_or_log_error!(reply, res, "Subscribe");
            }
            NetworkCommand::Unsubscribe { topic, reply } => {
                let sub: IdentTopic = Topic::new(&topic);
                let was_subscribed = self.swarm.behaviour_mut().gossipsub.unsubscribe(&sub);
                // Removing the entry is required, not hygiene: `verify_gossip` reads an absent
                // entry as "topic not subscribed here" and rejects. Leaving a stale entry behind
                // pins this topic to the allowlist of whichever committee was current when it was
                // last subscribed, so a later committee's honest authors would be rejected as
                // unauthorized.
                self.authorized_publishers.remove(&topic);
                send_or_log_error!(reply, was_subscribed, "Unsubscribe");
            }
            NetworkCommand::ConnectedPeerIds { reply } => {
                let res = self.swarm.behaviour().peer_manager.connected_or_dialing_peers();
                debug!(target: "network", ?res, "peer manager connected peers:");
                send_or_log_error!(reply, res, "ConnectedPeers");
            }
            NetworkCommand::EstablishedPeerCount { reply } => {
                send_or_log_error!(reply, self.connected_peers.len(), "EstablishedPeerCount");
            }
            NetworkCommand::ConnectedPeers { reply } => {
                let peers = self
                    .swarm
                    .behaviour()
                    .peer_manager
                    .connected_or_dialing_peers()
                    .iter()
                    .flat_map(|id| self.swarm.behaviour().peer_manager.peer_to_bls(id))
                    .collect();
                debug!(target: "network", ?peers, "peer manager connected peers:");
                send_or_log_error!(reply, peers, "ConnectedPeers");
            }
            NetworkCommand::PeerScore { peer_id, reply } => {
                let opt_score = self.swarm.behaviour().peer_manager.peer_score(&peer_id);
                send_or_log_error!(reply, opt_score, "PeerScore");
            }
            NetworkCommand::AllPeers { reply } => {
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .all_peers()
                    .map(|(peer_id, vec)| (*peer_id, vec.into_iter().cloned().collect()))
                    .collect();

                send_or_log_error!(reply, collection, "AllPeers");
            }
            NetworkCommand::MeshPeers { topic, reply } => {
                let topic: IdentTopic = Topic::new(&topic);
                let collection = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .mesh_peers(&topic.into())
                    .cloned()
                    .collect();
                send_or_log_error!(reply, collection, "MeshPeers");
            }
            NetworkCommand::SendRequest { peer, request, reply } => {
                debug!(target: "network", "send request for bls {peer}");
                if let Some((peer, addr)) = self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
                    debug!(target: "network", "trying to send to {peer} at {addr:?}");
                    let request_id = self
                        .swarm
                        .behaviour_mut()
                        .req_res
                        .send_request_with_addresses(&peer, request, addr);
                    self.outbound_requests.insert((peer, request_id), reply);
                } else {
                    // Best effort to return an error to caller.
                    let _ = reply.send(Err(NetworkError::PeerMissing));
                }
            }
            NetworkCommand::SendRequestDirect { peer, request, reply } => {
                let request_id = self.swarm.behaviour_mut().req_res.send_request(&peer, request);
                self.outbound_requests.insert((peer, request_id), reply);
            }
            NetworkCommand::SendRequestAny { request, reply } => {
                // Rotating an empty list will panic...
                if !self.connected_peers.is_empty() {
                    self.connected_peers.rotate_left(1);
                }
                if let Some(peer) = self.connected_peers.front() {
                    let request_id = self.swarm.behaviour_mut().req_res.send_request(peer, request);
                    self.outbound_requests.insert((*peer, request_id), reply);
                } else {
                    // Ignore error since this means other end lost interest and we don't really
                    // care.
                    let _ = reply.send(Err(NetworkError::NoPeers));
                }
            }
            NetworkCommand::SendResponse { response, channel, reply } => {
                let res = self
                    .swarm
                    .behaviour_mut()
                    .req_res
                    .send_response(channel.into_inner(), response);
                send_or_log_error!(reply, res, "SendResponse");
            }
            NetworkCommand::PendingRequestCount { reply } => {
                let count = self.outbound_requests.len();
                send_or_log_error!(reply, count, "SendResponse");
            }
            NetworkCommand::ReportPenalty { peer, penalty } => {
                debug!(target: "network", "penalty reported for peer {peer}");
                if let Some((peer, _)) = self.swarm.behaviour().peer_manager.auth_to_peer(peer) {
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, penalty);
                } else {
                    warn!(target: "peer-manager", ?peer, "unable to assess penalty for peer");
                }
            }
            NetworkCommand::DisconnectPeer { peer_id, reply } => {
                // this is called after timeout for disconnected peer exchanges
                let res = self.swarm.disconnect_peer_id(peer_id);
                send_or_log_error!(reply, res, "DisconnectPeer");
            }
            NetworkCommand::PeersForExchange { reply } => {
                let peers = self.swarm.behaviour_mut().peer_manager.peers_for_exchange();
                send_or_log_error!(reply, peers, "PeersForExchange");
            }
            NetworkCommand::UpdateCommittees { previous, current, next } => {
                // The network mirrors three of the on-chain registry's committees: previous,
                // current, and next. Peers in any of the three count as validators so the
                // just-completed committee is not pruned while late gossip may still arrive and
                // next-epoch peers are protected before they begin voting. (NVV support and
                // late-gossip acceptance remain future work.)
                //
                // All three slots are set directly from authoritative state every epoch (no
                // positional rotation), so current/previous self-correct against on-chain state and
                // any peer that exits the three-slot window is demoted.
                info!(target: "network", this_node=?self.swarm.local_peer_id(), "updating previous/current/next committees");
                let retention = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .retain_committees(previous.iter().chain(&current).chain(&next).copied())
                    .map_err(|error| NetworkError::StoreKademliaRecord(error.to_string()));
                // Authoritative membership and identity confirmation must not depend on a
                // capacity rejection or a failed database deletion during retention cleanup.
                self.swarm.behaviour_mut().peer_manager.update_committees(previous, current, next);
                self.refresh_explicit_peers();
                self.query_missing_required_records();
                retention?;
            }
            NetworkCommand::PrepareCommitteeDial { committee } => {
                // Deadlock-breaker pre-dial: forgive bans so the committee can be dialed without
                // mutating the committee slots (the real slot update follows shortly after).
                self.swarm.behaviour_mut().peer_manager.prepare_committee_dial(committee);
            }
            NetworkCommand::FindAuthorities { bls_keys } => {
                // Fetch signed records for unknown peers and unresolved configured dial hints.
                self.swarm.behaviour_mut().peer_manager.find_authorities(bls_keys);
            }
            NetworkCommand::GetValidatorRpc { bls_key, reply } => {
                let rpc = self.swarm.behaviour().peer_manager.get_rpc(&bls_key);
                send_or_log_error!(reply, rpc, "GetValidatorRpc");
            }
            NetworkCommand::GetAllValidatorRpcs { reply } => {
                let rpcs = self.swarm.behaviour_mut().peer_manager.current_committee_rpcs();
                send_or_log_error!(reply, rpcs, "GetAllValidatorRpcs");
            }
            NetworkCommand::OpenStream { peer, reply } => {
                // Look up the peer's PeerId from their BLS key
                let (peer_id, addrs) = match self.swarm.behaviour().peer_manager.auth_to_peer(peer)
                {
                    Some((id, addrs)) => (id, addrs),
                    None => {
                        debug!(
                            target: "network",
                            ?peer,
                            "OpenStream: peer not found"
                        );
                        let _ = reply.send(Err(NetworkError::PeerMissing));
                        return Ok(());
                    }
                };

                debug!(
                    target: "network",
                    ?peer_id,
                    "opening stream to peer"
                );

                // Pass the reply channel directly to the stream behavior.
                // The stream (or error) will be returned to the caller via oneshot
                // without any intermediate tracking.
                self.swarm.behaviour_mut().stream.open_stream(peer_id, addrs, reply);
            }
            #[cfg(test)]
            NetworkCommand::KadStoreGet { key, reply } => {
                let record_key = node_record_key(&key);
                let record = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .get(&record_key)
                    .map(|cow| cow.into_owned());
                let _ = reply.send(record);
            }
        }

        Ok(())
    }

    /// Reconcile gossip mesh privileges for connected peers after committee rotation.
    ///
    /// Operator trust survives rotation; committee protection ends when the final slot expires.
    fn refresh_explicit_peers(&mut self) {
        let peers: Vec<_> = self.swarm.connected_peers().copied().collect();
        peers.iter().for_each(|peer| self.refresh_explicit_peer(peer));
    }

    /// Reconcile one connected peer's mesh privileges after discovery, trust changes, or a ban.
    fn refresh_explicit_peer(&mut self, peer: &PeerId) {
        let manager = &self.swarm.behaviour().peer_manager;
        let protected = self.swarm.is_connected(peer)
            && manager.peer_is_important(peer)
            && !manager.peer_banned(peer);
        if protected {
            self.swarm.behaviour_mut().gossipsub.add_explicit_peer(peer);
        } else {
            self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(peer);
        }
    }

    /// Refill required rows after ownership updates, even if the restored peer cache already
    /// resolves a key. Queries hold no persistent ownership and are deduplicated by requested key.
    fn query_missing_required_records(&mut self) {
        self.swarm
            .behaviour_mut()
            .kademlia
            .store_mut()
            .missing_required_records()
            .into_iter()
            .for_each(|key| {
                if self.kad_record_queries.values().all(|query| query.query.request != key) {
                    let id = self.swarm.behaviour_mut().kademlia.get_record(node_record_key(&key));
                    self.kad_record_queries.insert(id, key.into());
                }
            });
    }

    /// Process gossip events.
    fn process_gossip_event(&mut self, event: GossipEvent) -> NetworkResult<()> {
        match event {
            GossipEvent::Message { propagation_source, message_id, message } => {
                trace!(target: "network", topic=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?message, "message received from publisher");
                self.metrics.record_gossip_received();
                // verify message was published by authorized node
                let msg_acceptance = self.verify_gossip(&message);
                trace!(target: "network", ?msg_acceptance, "gossip message verification status");

                // report message validation results to propagate valid messages
                if !self.swarm.behaviour_mut().gossipsub.report_message_validation_result(
                    &message_id,
                    &propagation_source,
                    msg_acceptance.into(),
                ) {
                    error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, "error reporting message validation result");
                }

                // process gossip in application layer
                match msg_acceptance {
                    GossipAcceptance::Accept => {
                        // A peer is `Connected` before its `NodeRecord` resolves its BLS
                        // identity, so a live mesh neighbor can relay a message before
                        // `peer_to_bls` can resolve it. Deliver the accepted payload
                        // regardless and carry the relayer as `Option`: the author is
                        // already authenticated by `verify_gossip`, the relayer identity
                        // is only used for penalty attribution, and dropping here would
                        // lose the message for good because gossipsub has already cached
                        // `message_id` and will not re-deliver it once the identity
                        // resolves. The consumer skips the (unattributable) penalty while
                        // the relayer is unresolved.
                        let relayer =
                            self.swarm.behaviour().peer_manager.peer_to_bls(&propagation_source);
                        if relayer.is_none() {
                            debug!(
                                target: "network",
                                ?propagation_source,
                                ?message_id,
                                "delivering accepted gossip with unresolved relayer identity; consensus-layer penalty skipped"
                            );
                        }
                        // Resolve the author's BLS identity too. The message is already
                        // authenticated, but deep validation in the application layer (the
                        // worker's batch checks) runs after this `Accept`, and an author-content
                        // fault it surfaces must be charged to the author, not the forwarder
                        // (see issue #819). The `Option` reflects the two fallible lookups it is
                        // built from, `message.source` and the `peer_to_bls` index, not any
                        // topic policy; on `None` the consumer skips the author penalty.
                        let author = message
                            .source
                            .as_ref()
                            .and_then(|id| self.swarm.behaviour().peer_manager.peer_to_bls(id));
                        // forward gossip to handler; a full queue or a queue with no
                        // subscriber counts as shed
                        let forwarded = self
                            .event_stream
                            .try_send_outcome(accepted_gossip_event(message, relayer, author));
                        self.metrics.record_forward(ServiceClass::Gossip, &forwarded);
                        if forwarded.inspect_err(|e| {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?propagation_source, ?message_id, ?e, "failed to forward gossip!");
                        }).is_err() {
                            // ignore failures at the epoch boundary
                            // During epoch change the event_stream reciever can be closed.
                            return Ok(());
                        }
                    }
                    GossipAcceptance::Reject(reason) => {
                        self.metrics.record_gossip_rejected();
                        // Resolve both candidate culprits, then let `reason` decide accountability
                        // (see `RejectReason::penalty`): an oversized payload is charged to the
                        // relaying peer, an unauthorized author to the author, each only once its
                        // identity has resolved. The relaying peer is never penalized for an
                        // author fault (#801/#785).
                        let relayer =
                            self.swarm.behaviour().peer_manager.peer_to_bls(&propagation_source);
                        let author_id = message.source;
                        let author = author_id
                            .as_ref()
                            .and_then(|id| self.swarm.behaviour().peer_manager.peer_to_bls(id));
                        let topic = &message.topic;
                        match reason.penalty(relayer.is_some(), author.is_some()) {
                            RejectPenalty::FatalRelayer => {
                                warn!(
                                    target: "network",
                                    ?topic,
                                    "oversized gossip - applying fatal penalty to propagation source: {propagation_source:?}"
                                );
                                self.swarm
                                    .behaviour_mut()
                                    .peer_manager
                                    .process_penalty(propagation_source, Penalty::Fatal);
                            }
                            RejectPenalty::FatalAuthor => {
                                // `author.is_some()` guarantees `author_id` is `Some`.
                                if let Some(author_id) = author_id {
                                    warn!(
                                        target: "network",
                                        ?author_id,
                                        ?topic,
                                        "unauthorized-author gossip - applying fatal penalty to the author, not the forwarding relayer: {propagation_source:?}"
                                    );
                                    self.swarm
                                        .behaviour_mut()
                                        .peer_manager
                                        .process_penalty(author_id, Penalty::Fatal);
                                }
                            }
                            RejectPenalty::Skip => {
                                debug!(
                                    target: "network",
                                    ?reason,
                                    ?topic,
                                    ?propagation_source,
                                    "rejecting gossip without an attributable penalty (unresolved relayer/author, or this node's committee-view lag)"
                                );
                            }
                        }
                    }
                }
            }
            GossipEvent::Subscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?topic, "gossipsub event - subscribed")
            }
            GossipEvent::Unsubscribed { peer_id, topic } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?topic, "gossipsub event - unsubscribed")
            }
            GossipEvent::GossipsubNotSupported { peer_id } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, "gossipsub event - not supported");
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Fatal);
            }
            GossipEvent::SlowPeer { peer_id, failed_messages } => {
                trace!(target: "network", topics=?self.authorized_publishers.keys(), ?peer_id, ?failed_messages, "gossipsub event - slow peer");
                self.swarm
                    .behaviour_mut()
                    .peer_manager
                    .process_penalty(peer_id, Penalty::Load(LoadPenalty::SlowPeer));
            }
        }

        Ok(())
    }

    /// Process req/res events.
    fn process_reqres_event(&mut self, event: ReqResEvent<Req, Res>) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id: _ } => {
                match message {
                    request_response::Message::Request { request_id, request, channel } => {
                        debug!(target: "network", ?peer, ?request, "request received");
                        // intercept peer exchange messages
                        if let Some(peers) = request.peer_exchange_msg() {
                            debug!(target: "network", ?peers, "processing peer exchange");
                            self.swarm.behaviour_mut().peer_manager.process_peer_exchange(peers);
                            // send empty ack and ignore errors
                            let ack = PeerExchangeMap::default().into();
                            let _ = self.swarm.behaviour_mut().req_res.send_response(channel, ack);

                            // initiate disconnect from this peer to prevent redial attempts
                            debug!(target: "peer-manager", ?peer, "initiating reciprocal disconnect after px");
                            self.swarm.behaviour_mut().peer_manager.disconnect_peer(peer, false);
                            return Ok(());
                        }

                        // We should not be able to recieve a message from an unknown peer so this
                        // should always work. It is possible (mostly in
                        // testing) to have a race where we don't know the requester YET.
                        // If so send an error back but this should be so infrequent on a real
                        // network that we can ignore and it should not
                        // cause any lasting damage if triggered.
                        if let Some(bls) = self.swarm.behaviour().peer_manager.peer_to_bls(&peer) {
                            let class = request.service_class();
                            let (notify, cancel) = oneshot::channel();
                            // forward request to handler without blocking other events
                            let forwarded =
                                self.event_stream.try_send_outcome(NetworkEvent::Request {
                                    peer: bls,
                                    request,
                                    channel: ResponseChannel::new(peer, channel),
                                    cancel,
                                });
                            self.metrics.record_forward(class, &forwarded);
                            // Only queued requests become pending. An unsubscribed queue
                            // drops the response channel, and epoch-boundary errors are ignored.
                            if !forwarded.inspect_err(|e| {
                                error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                            }).is_ok_and(|outcome| outcome == TrySendOutcome::Queued) {
                                return Ok(());
                            }

                            // store the request and cancel duplicate requests
                            //
                            // NOTE: the request id is internally generated, so this should not
                            // happen
                            self.add_inbound(class);
                            if let Some(duplicate) = self.inbound_requests.insert(
                                request_id,
                                PendingInbound { notify, class, received: Instant::now() },
                            ) {
                                // cancel if this is a duplicate request
                                warn!(target: "network", ?peer, "duplicate request id from peer");
                                self.close_inbound(duplicate);
                            }
                        } else if let Err(e) = self.event_stream.try_send(NetworkEvent::Error(
                            format!("requesting peer unknown: {peer:?}"),
                            ResponseChannel::new(peer, channel),
                        )) {
                            error!(target: "network", topics=?self.authorized_publishers.keys(), ?request_id, ?e, "failed to forward request!");
                            // ignore failures at the epoch boundary
                            // During epoch change the event_stream reciever can be closed.
                            return Ok(());
                        }
                    }
                    request_response::Message::Response { request_id, response } => {
                        // check if response associated with PX disconnect
                        if self.pending_px_disconnects.remove(&request_id).is_some() {
                            let _ = self.swarm.disconnect_peer_id(peer);
                        }

                        // try to forward response to original caller
                        let _ = self.outbound_requests.remove(&(peer, request_id)).map(|ack| {
                            // The response payload is genuine (we still hold the
                            // matching outbound request). If the responder's BLS
                            // identity has not resolved yet, report a transient
                            // `PeerUnresolved` rather than a misleading `PeerMissing`
                            // so the caller does not retry a request that succeeded.
                            let resolved = self.swarm.behaviour().peer_manager.peer_to_bls(&peer);
                            let _ = ack.send(resolve_response(resolved, response));
                        });
                    }
                }
            }
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, "Outbound failure for req/res");
                // handle px disconnects
                //
                // px attempts to support peer discovery, but failures are okay
                // this node disconnects after a px timeout
                if self.pending_px_disconnects.remove(&request_id).is_some() {
                    debug!(target: "network", "outbound failure expected because of px disconnect");
                    return Ok(());
                }

                self.metrics.record_outbound_failure(&error);

                // Differentiate transport-level failures (peer disconnect, dial fail) from
                // protocol-level violations. Transport failures are common on WAN and should
                // not contribute to ban score; otherwise N in-flight requests at disconnect
                // time cause N * Medium = instant ban.
                match &error {
                    ReqResOutboundFailure::DialFailure
                    | ReqResOutboundFailure::ConnectionClosed => {
                        // transport-level: no penalty
                    }
                    ReqResOutboundFailure::Io(e) => match e.kind() {
                        ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::BrokenPipe
                        | ErrorKind::Interrupted => {
                            // transport flap on WAN — no penalty
                        }
                        _ => {
                            warn!(
                                target: "network",
                                ?e, ?peer, ?request_id,
                                "outbound IO failure (likely codec violation)"
                            );
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer, Penalty::Medium);
                        }
                    },
                    ReqResOutboundFailure::Timeout => {
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_penalty(peer, Penalty::Load(LoadPenalty::Timeout));
                    }
                    // Not penalized. Failing to negotiate a common protocol is honest
                    // version/role skew (the peer runs a different/older/role-distinct
                    // protocol set), not misbehavior — the same not-the-peer's-fault
                    // class as `DialFailure`/`ConnectionClosed` above. Penalizing it
                    // bans not-yet-upgraded peers during rolling upgrades and would turn
                    // the #765 chain-id protocol split into a network partition. Warn for
                    // operator visibility only.
                    ReqResOutboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, "outbound failure: unsupported protocol (not penalized)");
                    }
                }

                // try to forward error to original caller
                let _ = self.outbound_requests.remove(&(peer, request_id)).map(|ack| {
                    let _ = ack.send(Err(NetworkError::Outbound(error.into())));
                });
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                // Dropped, unforwarded requests have no tracked class or occupancy. Ignore
                // their failures, including ResponseOmission after an unsubscribed queue.
                if !self.inbound_requests.contains_key(&request_id) {
                    return Ok(());
                }
                // classify before the match below takes `error` apart
                let outcome = InboundFailureOutcome::from_failure(&error);
                debug!(target: "network", ?peer, ?error, pending=?self.inbound_requests, "Inbound failure for req/res");
                debug!(target: "network", my_id=?self.swarm.local_peer_id(), "this node");
                match &error {
                    ReqResInboundFailure::Io(e) => match e.kind() {
                        ErrorKind::ConnectionReset
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                        | ErrorKind::BrokenPipe
                        | ErrorKind::Interrupted => {
                            // transport flap on WAN — no penalty
                        }
                        _ => {
                            warn!(
                                target: "network",
                                ?e, ?peer, ?request_id,
                                "inbound IO failure (likely codec violation)"
                            );
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer, Penalty::Medium);
                        }
                    },
                    // Not penalized. The local peer supports none of the protocols the
                    // remote requested: honest version/role skew, not misbehavior (the
                    // inbound mirror of the outbound arm above). Penalizing it bans
                    // not-yet-upgraded peers during rolling upgrades and is a prerequisite
                    // blocker for the #765 chain-id protocol split. Warn for operator
                    // visibility only.
                    ReqResInboundFailure::UnsupportedProtocols => {
                        warn!(target: "network", ?peer, ?request_id, ?error, "inbound failure: unsupported protocol (not penalized)");
                    }
                    ReqResInboundFailure::Timeout | ReqResInboundFailure::ConnectionClosed => {
                        // peer dropped or stalled mid-request — expected on WAN, no penalty
                    }
                    ReqResInboundFailure::ResponseOmission => { /* ignore local error */ }
                }

                // count the failure under the class of the forwarded request, then forward
                // cancelation to handler and release the class occupancy
                self.inbound_requests.remove(&request_id).into_iter().for_each(|entry| {
                    self.metrics.record_inbound_failure(entry.class, outcome);
                    self.close_inbound(entry);
                });
            }

            ReqResEvent::ResponseSent { request_id, .. } => {
                if let Some(entry) = self.inbound_requests.remove(&request_id) {
                    self.metrics.record_service_time(entry.class, entry.received.elapsed());
                    self.close_inbound(entry);
                }
            }
        }

        Ok(())
    }

    /// Count one forwarded inbound request of `class` as pending and export the occupancy.
    fn add_inbound(&mut self, class: ServiceClass) {
        self.inbound_pending = self.inbound_pending.added(class);
        self.metrics.set_inbound_pending(class, self.inbound_pending.pending(class));
    }

    /// End a pending inbound request: notify the handler and release the class occupancy.
    ///
    /// Every removal of an `inbound_requests` entry calls this once, so the swarm releases each
    /// added request exactly once.
    fn close_inbound(&mut self, entry: PendingInbound) {
        let _ = entry.notify.send(());
        self.inbound_pending = self.inbound_pending.released(entry.class);
        self.metrics.set_inbound_pending(entry.class, self.inbound_pending.pending(entry.class));
    }

    /// Request the connected peer's self-record, coalescing pending and deferred work.
    fn request_current_record(&mut self, peer: PeerId) {
        if self.swarm.is_connected(&peer) && !self.swarm.behaviour().peer_manager.peer_banned(&peer)
        {
            if self.record_exchange.allow_request(peer) {
                let request = self.swarm.behaviour_mut().record_exchange.send_request(&peer, ());
                self.record_exchange.track(peer, request);
                self.metrics.record_exchange("sent");
            } else {
                self.defer_record_request(peer);
            }
        }
    }

    /// Queue a retry and count each newly deferred peer once.
    fn defer_record_request(&mut self, peer: PeerId) {
        if self.record_exchange.defer(peer) {
            self.metrics.record_exchange("deferred");
        }
    }

    /// Retry only connected peers, at most one bounded batch per heartbeat.
    fn retry_record_requests(&mut self) {
        self.record_exchange.take_deferred().into_iter().for_each(|peer| {
            if self.swarm.is_connected(&peer)
                && !self.swarm.behaviour().peer_manager.peer_banned(&peer)
            {
                self.metrics.record_exchange("retry");
            }
            self.request_current_record(peer);
        });
    }

    /// Handle authenticated record retrieval without involving consensus request queues.
    fn process_record_exchange_event(
        &mut self,
        event: ReqResEvent<(), RecordResponse>,
    ) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, .. } => match message {
                request_response::Message::Request { channel, .. } => {
                    // Rate and ban checks precede cloning or encoding the signed record.
                    let response = (!self.swarm.behaviour().peer_manager.peer_banned(&peer)
                        && self.record_exchange.allow_response(peer))
                    .then(|| (self.key_config.primary_public_key(), self.node_record.clone()));
                    let outcome = if response.is_some() { "served" } else { "refused" };
                    self.swarm
                        .behaviour_mut()
                        .record_exchange
                        .send_response(channel, response)
                        .map(|()| self.metrics.record_exchange(outcome))
                        .unwrap_or_else(|_| self.metrics.record_exchange("response_unavailable"));
                    Ok(())
                }
                request_response::Message::Response { request_id, response } => {
                    self.process_record_response(peer, request_id, response)
                }
            },
            ReqResEvent::OutboundFailure { peer, request_id, error, .. } => {
                let kind = match &error {
                    ReqResOutboundFailure::DialFailure => "outbound_dial_failure",
                    ReqResOutboundFailure::Timeout => "outbound_timeout",
                    ReqResOutboundFailure::ConnectionClosed => "outbound_connection_closed",
                    ReqResOutboundFailure::UnsupportedProtocols => "outbound_unsupported_protocols",
                    ReqResOutboundFailure::Io(error) if error.kind() == ErrorKind::InvalidData => {
                        "outbound_invalid_data"
                    }
                    ReqResOutboundFailure::Io(_) => "outbound_io",
                };
                self.metrics.record_exchange(kind);
                if self.record_exchange.finish(peer, request_id) {
                    match error {
                        ReqResOutboundFailure::UnsupportedProtocols => {
                            // Capability failure is penalty-exempt. Known legacy identities
                            // use the existing deduplicated, bounded Kademlia query pipeline.
                            self.request_legacy_record(peer);
                        }
                        ReqResOutboundFailure::Io(error)
                            if error.kind() == ErrorKind::InvalidData =>
                        {
                            self.swarm
                                .behaviour_mut()
                                .peer_manager
                                .process_penalty(peer, Penalty::Medium);
                        }
                        ReqResOutboundFailure::DialFailure
                        | ReqResOutboundFailure::Timeout
                        | ReqResOutboundFailure::ConnectionClosed
                        | ReqResOutboundFailure::Io(_) => {
                            self.defer_record_request(peer);
                        }
                    }
                }
                Ok(())
            }
            ReqResEvent::InboundFailure { peer, error, .. } => {
                let kind = match &error {
                    ReqResInboundFailure::Timeout => "inbound_timeout",
                    ReqResInboundFailure::ConnectionClosed => "inbound_connection_closed",
                    ReqResInboundFailure::UnsupportedProtocols => "inbound_unsupported_protocols",
                    ReqResInboundFailure::ResponseOmission => "inbound_response_omission",
                    ReqResInboundFailure::Io(error) if error.kind() == ErrorKind::InvalidData => {
                        "inbound_invalid_data"
                    }
                    ReqResInboundFailure::Io(_) => "inbound_io",
                };
                self.metrics.record_exchange(kind);
                if matches!(&error, ReqResInboundFailure::Io(error) if error.kind() == ErrorKind::InvalidData)
                {
                    self.swarm.behaviour_mut().peer_manager.process_penalty(peer, Penalty::Medium);
                }
                debug!(target: "network", ?peer, ?error, "record retrieval inbound failure");
                Ok(())
            }
            ReqResEvent::ResponseSent { .. } => Ok(()),
        }
    }

    /// Apply only solicited responses, with the authenticated peer as the publisher.
    ///
    /// Sharing the inbound put pipeline preserves domain, BLS/network binding, rate,
    /// publisher, freshness, committee and persistence checks. Remote expiry is not trusted.
    fn process_record_response(
        &mut self,
        peer: PeerId,
        request_id: OutboundRequestId,
        response: RecordResponse,
    ) -> NetworkResult<()> {
        if self.record_exchange.finish(peer, request_id) {
            if response.is_none() {
                self.metrics.record_exchange("remote_refused");
                self.defer_record_request(peer);
            } else {
                self.metrics.record_exchange("received");
            }
            response
                .map(|(key, record)| kad::Record {
                    key: node_record_key(&key),
                    value: encode(&record),
                    publisher: Some(peer),
                    expires: std::time::Instant::now().checked_add(self.config.kad_record_ttl),
                })
                .map(|record| {
                    self.apply_put_record(peer, record).map(|outcome| {
                        if matches!(outcome, PutOutcome::Shed) {
                            self.defer_record_request(peer);
                        }
                    })
                })
                .transpose()
                .map(|_| ())
        } else {
            Ok(())
        }
    }

    /// Fall back once per allowed attempt for legacy peers in a tracked committee.
    ///
    /// A legacy peer whose identity is unknown still uses ordinary first-connect pushes and
    /// discovery. No extra consensus RPC or unsolicited push is introduced by the fallback.
    /// Only committee query results can refresh discovery, and querying the already known key
    /// cannot discover a legacy peer's rotated BLS key.
    fn request_legacy_record(&mut self, peer: PeerId) {
        self.swarm
            .behaviour()
            .peer_manager
            .peer_to_bls(&peer)
            .filter(|_| self.swarm.behaviour().peer_manager.is_peer_validator(&peer))
            .filter(|key| self.kad_record_queries.values().all(|query| query.query.request != *key))
            .into_iter()
            .for_each(|key| {
                let query = self.swarm.behaviour_mut().kademlia.get_record(node_record_key(&key));
                self.kad_record_queries.insert(query, key.into());
                self.metrics.record_exchange("legacy_fallback");
            });
    }

    /// Process events from the dedicated peer-exchange goodbye protocol.
    ///
    /// Mirrors the legacy embedded peer-exchange handling in
    /// [`Self::process_reqres_event`]: an inbound exchange updates the peer manager,
    /// receives an empty ack, and triggers a reciprocal disconnect. Failures are
    /// never penalized: a goodbye precedes a disconnect, so there is no
    /// relationship left to protect. The one failure that changes course is
    /// outbound `UnsupportedProtocols` (honest version skew, penalty-exempt): the
    /// exchange is re-sent as the legacy variant embedded in the consensus request
    /// enum so not-yet-upgraded peers still receive it.
    fn process_peer_exchange_event(
        &mut self,
        event: ReqResEvent<PeerExchangeMap, PeerExchangeMap>,
    ) -> NetworkResult<()> {
        match event {
            ReqResEvent::Message { peer, message, connection_id: _ } => match message {
                request_response::Message::Request { request_id: _, request, channel } => {
                    debug!(target: "network", ?peer, ?request, "processing peer exchange (dedicated protocol)");
                    self.swarm.behaviour_mut().peer_manager.process_peer_exchange(request);
                    // send empty ack and ignore errors
                    let _ = self
                        .swarm
                        .behaviour_mut()
                        .peer_exchange
                        .send_response(channel, PeerExchangeMap::default());

                    // initiate disconnect from this peer to prevent redial attempts
                    debug!(target: "peer-manager", ?peer, "initiating reciprocal disconnect after px");
                    self.swarm.behaviour_mut().peer_manager.disconnect_peer(peer, false);
                }
                request_response::Message::Response { request_id, response: _ } => {
                    // goodbye acked: disconnect immediately (the ack payload is
                    // reserved for a future reciprocal exchange and ignored today)
                    if let Some(pending) = self.pending_goodbyes.remove(&request_id) {
                        let _ = pending.notify.send(GoodbyeOutcome::Acked);
                        let _ = self.swarm.disconnect_peer_id(peer);
                    }
                }
            },
            ReqResEvent::OutboundFailure { peer, request_id, error, connection_id: _ } => {
                debug!(target: "network", ?peer, ?error, "Outbound failure for peer exchange");
                if let Some(pending) = self.pending_goodbyes.remove(&request_id) {
                    match &error {
                        // Not penalized: honest version skew, the same class the main
                        // req-res handler exempts. The peer predates the dedicated
                        // protocol, so re-send the exchange as the embedded legacy
                        // variant, which owns the disconnect from here.
                        ReqResOutboundFailure::UnsupportedProtocols => {
                            debug!(
                                target: "peer-manager",
                                ?peer,
                                "peer exchange protocol unsupported - falling back to embedded exchange"
                            );
                            self.send_legacy_goodbye(peer, pending.exchange);
                            let _ = pending.notify.send(GoodbyeOutcome::FellBack);
                        }
                        // Any other failure means no ack is coming: dropping the
                        // notify sender wakes the deadline task, which disconnects.
                        // No penalty: px supports discovery and failures are okay.
                        ReqResOutboundFailure::DialFailure
                        | ReqResOutboundFailure::ConnectionClosed
                        | ReqResOutboundFailure::Io(_)
                        | ReqResOutboundFailure::Timeout => {}
                    }
                }
            }
            ReqResEvent::InboundFailure { peer, request_id, error, connection_id: _ } => {
                // never penalized: the exchange is best-effort and both sides
                // disconnect afterwards regardless
                debug!(target: "network", ?peer, ?request_id, ?error, "Inbound failure for peer exchange");
            }
            ReqResEvent::ResponseSent { peer, .. } => {
                trace!(target: "network", ?peer, "peer exchange ack sent");
            }
        }

        Ok(())
    }

    /// The number of graceful goodbyes currently awaiting resolution, across the
    /// dedicated peer-exchange protocol and the embedded legacy path.
    ///
    /// Both paths share the `max_px_disconnects` budget so the combined pending
    /// count keeps the original bound.
    fn goodbyes_in_flight(&self) -> usize {
        self.pending_goodbyes.len() + self.pending_px_disconnects.len()
    }

    /// Send a goodbye on the dedicated peer-exchange protocol and schedule the
    /// disconnect.
    ///
    /// The spawned task disconnects once the goodbye resolves or after
    /// `px_disconnect_timeout`, whichever comes first, unless the goodbye fell
    /// back to the embedded legacy path, which schedules its own disconnect.
    fn send_goodbye(&mut self, peer_id: PeerId, exchange: PeerExchangeMap) {
        let (notify, done) = oneshot::channel();
        let request_id =
            self.swarm.behaviour_mut().peer_exchange.send_request(&peer_id, exchange.clone());
        self.pending_goodbyes.insert(request_id, PendingGoodbye { exchange, notify });

        let timeout = self.config.px_disconnect_timeout;
        let handle = self.network_handle();

        // spawn task
        let task_name = format!("goodbye-{peer_id}");
        self.task_spawner.spawn_task(task_name, async move {
            // disconnect after the goodbye resolves (ack / failure / deadline)
            // unless the legacy fallback took over the disconnect
            let fell_back = tokio::time::timeout(timeout, done)
                .await
                .ok()
                .and_then(|resolved| resolved.ok())
                .is_some_and(|outcome| outcome == GoodbyeOutcome::FellBack);
            if !fell_back {
                let _ = handle.disconnect_peer(peer_id).await;
            }
            Ok(())
        });
    }

    /// Send a goodbye as the [`PeerExchangeMap`] variant embedded in the legacy
    /// consensus request enum.
    ///
    /// The fallback for peers that do not support the dedicated peer-exchange
    /// protocol yet; removal is coordinated with the `/0.0.2` protocol bump.
    fn send_legacy_goodbye(&mut self, peer_id: PeerId, peer_exchange: PeerExchangeMap) {
        // guard: skip PX if peer already disconnected
        if !self.swarm.is_connected(&peer_id) {
            debug!(target: "peer-manager", ?peer_id, "peer already disconnected, skipping PX");
        } else if self.goodbyes_in_flight() < self.config.max_px_disconnects {
            // attempt to exchange peer information if limits allow
            let (reply, done) = oneshot::channel();
            let request_id =
                self.swarm.behaviour_mut().req_res.send_request(&peer_id, peer_exchange.into());
            self.outbound_requests.insert((peer_id, request_id), reply);

            let timeout = self.config.px_disconnect_timeout;
            let handle = self.network_handle();

            // spawn task
            let task_name = format!("peer-exchange-{peer_id}");
            self.task_spawner.spawn_task(task_name, async move {
                // ignore errors and disconnect after px attempt
                let _res = tokio::time::timeout(timeout, done).await;
                let _ = handle.disconnect_peer(peer_id).await;
                Ok(())
            });

            // insert to pending px disconnects
            self.pending_px_disconnects.insert(request_id, peer_id);
        } else {
            // too many px disconnects pending so disconnect without px
            let _ = self.swarm.disconnect_peer_id(peer_id);
        }
    }

    /// Specific logic to accept gossip messages.
    ///
    /// Messages are only published by current committee nodes and must be within max size.
    fn verify_gossip(&self, gossip: &GossipMessage) -> GossipAcceptance {
        // verify message size against the network-wide protocol constant (not per-node config):
        // the reject path attributes an oversized payload to the relaying peer, which is sound only
        // if every honest node applies the identical bound. See `MAX_GOSSIP_MESSAGE_SIZE`.
        if gossip.data.len() > MAX_GOSSIP_MESSAGE_SIZE {
            return GossipAcceptance::Reject(RejectReason::TooLarge);
        }

        let GossipMessage { topic, .. } = gossip;

        // Ensure the publisher is authorized. Semantics per topic entry:
        //   - absent  => topic not subscribed here: reject.
        //   - `None`  => subscribed, any publisher allowed (open topic): accept.
        //   - `Some`  => subscribed, committee-restricted: accept only a resolved BLS key that is
        //     in the allowlist.
        if gossip.source.is_some_and(|id| {
            let bls_key = self.swarm.behaviour().peer_manager.peer_to_bls(&id);
            self.authorized_publishers.get(topic.as_str()).is_some_and(|auth| {
                auth.as_ref().is_none_or(|set| bls_key.is_some_and(|key| set.contains(&key)))
            })
        }) {
            GossipAcceptance::Accept
        } else {
            GossipAcceptance::Reject(RejectReason::UnauthorizedAuthor)
        }
    }

    /// Process an event from the peer manager.
    fn process_peer_manager_event(&mut self, event: PeerEvent) -> NetworkResult<()> {
        match event {
            PeerEvent::DisconnectPeer(peer_id) => {
                debug!(target: "network", ?peer_id, "peer manager: disconnect peer");
                // remove from request-response
                // NOTE: gossipsub handle `FromSwarm::ConnectionClosed`
                let _ = self.swarm.disconnect_peer_id(peer_id);

                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
            }
            PeerEvent::PeerDisconnected(peer_id) => {
                debug!(target: "network", ?peer_id, "peer disconnected event from peer manager");

                // Check if there are any connections still in the pool
                if self.swarm.is_connected(&peer_id) {
                    warn!(
                        target: "network",
                        ?peer_id,
                        "PeerDisconnected event but swarm still has connections - forcing disconnect"
                    );
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);

                let keys = self
                    .outbound_requests
                    .iter()
                    .filter_map(
                        |((p_id, req_id), _)| {
                            if *p_id == peer_id {
                                Some((*p_id, *req_id))
                            } else {
                                None
                            }
                        },
                    )
                    .collect::<Vec<_>>();

                // remove from outbound_requests and send error
                for k in keys {
                    let _ = self.outbound_requests.remove(&k).map(|ack| {
                        let _ = ack.send(Err(NetworkError::Disconnected));
                    });
                }
            }
            PeerEvent::DisconnectPeerX(peer_id, peer_exchange) => {
                debug!(target: "peer-manager", this_node=?self.swarm.local_peer_id(), ?peer_id, "disconnecting from peer with exchange info");

                // guard: skip PX if peer already disconnected
                if !self.swarm.is_connected(&peer_id) {
                    debug!(target: "peer-manager", ?peer_id, "peer already disconnected, skipping PX");
                } else if self.goodbyes_in_flight() < self.config.max_px_disconnects {
                    // attempt to exchange peer information if limits allow,
                    // preferring the dedicated protocol (falls back to the
                    // embedded legacy variant on `UnsupportedProtocols`)
                    self.send_goodbye(peer_id, peer_exchange);
                } else {
                    // too many px disconnects pending so disconnect without px
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                }

                // remove peer from kad - will redial if necessary
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);

                // remove from connected peers
                self.connected_peers.retain(|peer| *peer != peer_id);
            }
            PeerEvent::PeerConnected(peer_id, addr) => {
                // Defense in depth: even if the peer-manager `handle_established_*_connection`
                // path lets a banned peer reach this event (observed in adiri testnet logs),
                // refuse to register the connection with kademlia/gossipsub. Otherwise the
                // banned peer ends up in the kad routing table and triggers a redial loop.
                if self.swarm.behaviour().peer_manager.peer_banned(&peer_id) {
                    debug!(
                        target: "network",
                        ?peer_id,
                        "PeerConnected for banned peer — refusing to register"
                    );
                    let _ = self.swarm.disconnect_peer_id(peer_id);
                    return Ok(());
                }

                // register peer for request-response behaviour
                // NOTE: gossipsub handles `FromSwarm::ConnectionEstablished`
                self.swarm.add_peer_address(peer_id, addr.clone());
                // add as a kademlia peer
                self.swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);

                // Each newly connected peer needs a direct record push. Concurrent connections
                // share the publication marker; the last close clears it for a reconnect.
                if self.mark_published_to_peer(peer_id) {
                    self.publish_our_data_to_peer(peer_id);
                }
                // Pull independently of either side's push-cache history. The response
                // identifies the current BLS key even when it changed while disconnected.
                self.request_current_record(peer_id);

                // manage connected peers for
                self.connected_peers.push_back(peer_id);

                // if this is a trusted/validator (important) peer, mark it as explicit in gossipsub
                if self.swarm.behaviour().peer_manager.peer_is_important(&peer_id) {
                    self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
            }
            PeerEvent::Banned(peer_id) => {
                warn!(target: "network", ?peer_id, "peer banned");
                self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                // blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.blacklist_peer(&peer_id);
                // remove from kad routing table
                self.swarm.behaviour_mut().kademlia.remove_peer(&peer_id);
            }
            PeerEvent::Unbanned(peer_id) => {
                debug!(target: "network", ?peer_id, "peer unbanned");
                // remove blacklist gossipsub
                self.swarm.behaviour_mut().gossipsub.remove_blacklisted_peer(&peer_id);
            }
            PeerEvent::MissingAuthorities(missing) => {
                // Polling callers such as `current_committee_rpcs` report a member as
                // missing on every call until its signed metadata reaches `known_peers`, so the
                // same key arrives here repeatedly while its lookup is still in flight.
                // Issue at most one live `get_record` per key: skip keys already tracked
                // in `kad_record_queries` (issue #1135). The map is safe as the dedupe
                // source because every terminal query path removes its entry (see
                // `close_kad_query`), so a skipped key becomes queryable again as soon
                // as its current query ends. The removal there runs before any result
                // filtering, so even a query whose record is dropped as stale or
                // non-committee re-arms the key.
                for bls_key in missing {
                    if self.kad_record_queries.values().all(|q| q.query.request != bls_key) {
                        let key = node_record_key(&bls_key);
                        let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
                        self.kad_record_queries.insert(query_id, bls_key.into());
                    } else {
                        trace!(target: "network-kad", ?bls_key, "kad record query already in flight");
                    }
                }
            }
            PeerEvent::Discovery => {
                let peer_id = PeerId::random();
                self.swarm.behaviour_mut().kademlia.get_closest_peers(peer_id);
            }
        }

        Ok(())
    }

    /// Process events from the stream behavior.
    ///
    /// This handles inbound and outbound stream events for bulk data transfer.
    fn process_stream_event(&mut self, event: StreamEvent) -> NetworkResult<()> {
        match event {
            StreamEvent::InboundStream { peer, stream } => {
                debug!(
                    target: "network",
                    ?peer,
                    "inbound stream received"
                );
                // Forward the raw stream to the application layer, which reads it
                // as a typed sync stream.
                self.swarm.behaviour().peer_manager.peer_to_bls(&peer).map_or_else(
                    || warn!(target: "network", ?peer, "received inbound stream from unknown peer"),
                    |bls| {
                        let forwarded = self
                            .event_stream
                            .try_send_outcome(NetworkEvent::InboundStream { peer: bls, stream });
                        self.metrics.record_forward(ServiceClass::Other, &forwarded);
                        forwarded.err().into_iter().for_each(|e| {
                            error!(target: "network", ?e, "failed to forward inbound stream");
                        });
                    },
                );
            }
            StreamEvent::OutboundFailure { peer, failure }
            | StreamEvent::InboundFailure { peer, failure } => {
                // Classified for scoring but reported metrics-only until telemetry
                // confirms the classification does not fire on healthy peers (see
                // #739). Once confirmed, the matching penalty is enforced via
                // `peer_manager.process_penalty(peer, penalty)`.
                failure.penalty().map_or_else(
                    || trace!(target: "network", ?peer, ?failure, "stream failure (no penalty)"),
                    |penalty| {
                        debug!(
                            target: "network",
                            ?peer, ?failure, ?penalty,
                            "stream failure classified (metrics-only, not enforced)"
                        )
                    },
                );
            }
        }
        Ok(())
    }

    /// Process event from kademlia behavior.
    fn process_kad_event(&mut self, event: kad::Event) -> NetworkResult<()> {
        match event {
            kad::Event::InboundRequest { request } => {
                trace!(target: "network-kad", "inbound {request:?}");
                match request {
                    kad::InboundRequest::FindNode { num_closer_peers: _ } => {}
                    kad::InboundRequest::GetProvider {
                        num_closer_peers: _,
                        num_provider_peers: _,
                    } => {}
                    kad::InboundRequest::AddProvider { record } => {
                        self.process_kad_add_provider(record);
                    }
                    kad::InboundRequest::GetRecord { num_closer_peers: _, present_locally: _ } => {}
                    kad::InboundRequest::PutRecord { source, connection: _, record } => {
                        if let Some(record) = record {
                            self.process_kad_put_request(source, record)?;
                        }
                    }
                }
            }
            kad::Event::OutboundQueryProgressed { id: query_id, result, stats: _, step } => {
                match result {
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        key,
                        providers,
                        ..
                    })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            ?providers,
                            "kad::GetProviders::Ok"
                        );
                    }
                    kad::QueryResult::GetProviders(Err(err)) => {
                        error!(target: "network-kad", "Failed to get providers: {err:?}");
                    }
                    kad::QueryResult::GetRecord(Ok(kad::GetRecordOk::FoundRecord(
                        kad::PeerRecord { record, peer },
                    ))) => {
                        if let Some((key, node_record)) = self.peer_record_valid(&record) {
                            trace!(target: "network-kad", "Got record {key} {node_record:?}");
                            // Only a matching requested key may supply a required store row. Query
                            // ownership itself is temporary and cannot admit an unrelated record.
                            // Our own key is skipped, so a queried copy cannot replace the
                            // `expires: None` row that `provide_our_data` keeps for it.
                            let observed = now();
                            let timestamp =
                                self.admission_timestamp(key, node_record.info.timestamp, observed);
                            let freshness = self.record_freshness(&record, timestamp, observed);
                            if self
                                .kad_record_queries
                                .get(&query_id)
                                .is_some_and(|query| query.query.request == key)
                                && key != self.key_config.primary_public_key()
                                && matches!(
                                    freshness,
                                    RecordFreshness::Newer | RecordFreshness::Identical
                                )
                            {
                                let record = if freshness == RecordFreshness::Identical {
                                    self.preserve_record_expiry(record)
                                } else {
                                    record
                                };
                                // Mirror libp2p's inbound-put cap, so a queried copy never
                                // outlives `kad_record_ttl`. A responder that answers from its own
                                // `expires: None` row sends ttl 0, which decodes back to `None`.
                                // The cap runs after the merge, so a stored `None` cannot win.
                                let cap = std::time::Instant::now()
                                    .checked_add(self.config.kad_record_ttl);
                                let expires = cap
                                    .map(|cap| {
                                        record.expires.map_or(cap, |expires| expires.min(cap))
                                    })
                                    .or(record.expires);
                                self.swarm.behaviour_mut().kademlia.store_mut().put_with_timestamp(kad::Record { expires, ..record }, Some(timestamp))
                                    .unwrap_or_else(|error| {
                                        debug!(target: "network-kad", ?key, ?error,
                                            "queried binding could not be retained; discovery remains available");
                                    });
                            }
                            self.process_kad_query_result(
                                &query_id,
                                key,
                                node_record,
                                peer,
                                step.last,
                            );
                        } else {
                            trace!(target: "network-kad", "Received invalid peer record!");

                            // assess penalty for invalid peer record
                            if let Some(peer_id) = peer {
                                self.swarm
                                    .behaviour_mut()
                                    .peer_manager
                                    .process_penalty(peer_id, Penalty::Fatal);
                            }

                            // ensure query cleaned up
                            if step.last {
                                self.close_kad_query(&query_id);
                            }
                        }
                    }
                    kad::QueryResult::GetRecord(Ok(
                        kad::GetRecordOk::FinishedWithNoAdditionalRecord { cache_candidates },
                    )) => {
                        debug!(target: "network-kad", ?cache_candidates, "FinishedWithNoAdditionalRecord - failed to find record");
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::GetRecord(Err(err)) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::GetRecord::Err"
                        );
                        self.close_kad_query(&query_id);
                    }
                    kad::QueryResult::PutRecord(Ok(kad::PutRecordOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::PutRecordOk"
                        );
                    }
                    kad::QueryResult::PutRecord(Err(err)) => {
                        debug!(target: "network-kad", "Failed to put record: {err:?}");
                    }
                    kad::QueryResult::StartProviding(Ok(kad::AddProviderOk { key })) => {
                        debug!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(key.as_ref()),
                            "kad::StartProviding::Ok"
                        );
                    }
                    kad::QueryResult::StartProviding(Err(err)) => {
                        warn!(
                            target: "network-kad",
                            key = ?BlsPublicKey::from_literal_bytes(err.key().as_ref()),
                            ?err,
                            "kad::StartProviding::Err"
                        );
                    }
                    kad::QueryResult::GetClosestPeers(Ok(result)) => {
                        // process peers for potential discovery attempts
                        debug!(target: "network-kad", ?result, "GetClosestPeers for discovery");
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .process_peers_for_discovery(result.peers);
                    }
                    kad::QueryResult::GetClosestPeers(Err(err)) => {
                        // A timed-out query still carries the peers it located before
                        // expiring. Recover them for discovery instead of letting the
                        // catch-all discard the whole query: discovery only runs when
                        // the node is short on peers, and that same low-connectivity
                        // state is what makes queries slow enough to time out, so
                        // dropping the partial results starves discovery exactly when
                        // it is most needed.
                        let peers = partial_peers_from_get_closest_timeout(err);
                        debug!(
                            target: "network-kad",
                            recovered = peers.len(),
                            "GetClosestPeers timed out; recovering partial discovery results"
                        );
                        self.swarm.behaviour_mut().peer_manager.process_peers_for_discovery(peers);
                    }
                    _ => {}
                }
            }
            kad::Event::RoutingUpdated { peer, is_new_peer, addresses, bucket_range, old_peer } => {
                debug!(target: "network-kad", "routing updated peer {peer:?} new {is_new_peer} addrs {addresses:?} bucketr {bucket_range:?} old {old_peer:?}");

                // update newly added peer
                if is_new_peer {
                    self.swarm.behaviour_mut().peer_manager.update_routing_for_peer(&peer, true);

                    // update old peer if evicted from routing table
                    if let Some(old) = old_peer {
                        self.swarm
                            .behaviour_mut()
                            .peer_manager
                            .update_routing_for_peer(&old, false);
                    }
                }
            }
            kad::Event::UnroutablePeer { peer } => {
                // unknown peer queried a record - noop
                trace!(target: "network-kad", "unroutable peer {peer:?}")
            }
            kad::Event::RoutablePeer { peer, address } => {
                // kad discovered a new peer - peer is added to table on `PeerEvent::Connected`
                trace!(target: "network-kad", "routable peer {peer:?}/{address:?}");
            }
            kad::Event::PendingRoutablePeer { peer, address } => {
                trace!(target: "network-kad", "pending routable peer {peer:?}/{address:?}")
            }
            kad::Event::ModeChanged { new_mode } => {
                trace!(target: "network-kad", "mode changed {new_mode:?}")
            }
        }
        Ok(())
    }

    /// Process an inbound kad put request.
    fn process_kad_put_request(
        &mut self,
        source: PeerId,
        record: kad::Record,
    ) -> NetworkResult<()> {
        self.apply_put_record(source, record).map(|_| ())
    }

    /// Apply the shared PUT checks, reporting a shed response so its requester can retry.
    fn apply_put_record(
        &mut self,
        source: PeerId,
        mut record: kad::Record,
    ) -> NetworkResult<PutOutcome> {
        // check if source or publisher are banned
        let publisher_is_banned = record
            .publisher
            .map(|peer| self.swarm.behaviour().peer_manager.peer_banned(&peer))
            .unwrap_or(true); // reject records without publisher
        let source_is_banned = self.swarm.behaviour().peer_manager.peer_banned(&source);

        // reject record
        if publisher_is_banned || source_is_banned {
            error!(target: "network-kad", ?publisher_is_banned, ?source_is_banned, ?source, publisher=?record.publisher, "rejecting put request for record");
            // Do NOT `remove_record(&record.key)` on the reject path. Kademlia runs
            // with `StoreInserts::FilterBoth`, so this inbound record was never
            // written to the store; the only record `remove_record` can delete is one
            // the local node itself published (libp2p removes a key only when the
            // stored record's publisher is our own peer id; see libp2p-kad
            // behaviour.rs). The sole locally-published record is our own discovery
            // record, keyed on our BLS public key with `expires: None`, so an
            // unauthenticated PUT carrying `publisher = None` and `key = our own key`
            // would delete it. Because we only re-provide at startup, that deletion
            // then persists until restart. Reject with a penalty only; never mutate
            // the store on a key supplied by the sender.

            // assess penalty for pushing record without publisher
            if record.publisher.is_none() {
                trace!(target: "network-kad", ?source, "processing fatal penalty for missing publisher");
                self.swarm.behaviour_mut().peer_manager.process_penalty(source, Penalty::Fatal);
            }

            // return early
            return Ok(PutOutcome::Processed);
        }

        // Rate limit inbound put requests per source, independent of ban state, before the
        // expensive signature verify and kad store write below. A valid self-signed record
        // (publisher == source == attacker) clears the ban check above and is never penalized on
        // the accept path, so without this a single unbanned peer can flood valid records and
        // force repeated ~1ms BLS verifies plus MDBX writes on the network task that also relays
        // consensus gossip, starving the event loop (GHSA-f6rq-62rr-4h9g). Banned sources already
        // returned above, so this bounds the unbanned population. Honest kad replication
        // fan-in can cross the shed threshold as the network grows, so shedding carries no
        // penalty (a shed record is redundant: up to `replication_factor` other peers re-put
        // it hourly). A source past the flood threshold is scored once per window, then on every
        // message above the hard cutoff so a sustained flood promptly triggers disconnection.
        match self.swarm.behaviour_mut().peer_manager.put_record_rate_limited(source) {
            PutRecordRate::Flooding => {
                debug!(target: "network-kad", ?source, "put record flood: penalizing source");
                self.swarm
                    .behaviour_mut()
                    .peer_manager
                    .process_penalty(source, Penalty::Load(LoadPenalty::KademliaFlood));
                Ok(PutOutcome::Processed)
            }
            PutRecordRate::Shed => {
                trace!(target: "network-kad", ?source, "shedding rate limited put request");
                Ok(PutOutcome::Shed)
            }
            PutRecordRate::Allowed => {
                self.peer_record_valid(&record).map(|(key, value)| {
                    // verify record signature and ensure publisher matches record's network key
                    if record.value.len() <= MAX_KAD_PACKET_SIZE {
                        self.verified_peer_records.put(record.key.clone(), record.value.clone());
                    }

                    let observed = now();
                    let timestamp = self.admission_timestamp(key, value.info.timestamp, observed);
                    let freshness = self.record_freshness(&record, timestamp, observed);
                    let should_store = if freshness == RecordFreshness::Identical {
                        // A relayed identical copy can carry less remaining TTL. Refreshing it must
                        // not shorten the lifetime we already accepted. None means no expiry.
                        self.swarm.behaviour_mut().kademlia.store_mut().get(&record.key).is_none_or(
                            |existing| {
                                record.expires = existing
                                    .expires
                                    .zip(record.expires)
                                    .map(|(old, new)| old.max(new));
                                record.expires != existing.expires
                            },
                        )
                    } else {
                        true
                    };
                    trace!(target: "network-kad", "Got record {key} {value:?}");

                    // Confirm before the fallible store write, including for equal or older records.
                    // The peer manager never reads the store. It caches the record for a committee
                    // member or a pinned (operator-provisioned) key, relays included, with the
                    // freshness check waived only while the entry is still a config stub; for any
                    // other key it only confirms the sender's own identity and requires source to
                    // match the advertised one.
                    self.swarm
                        .behaviour_mut()
                        .peer_manager
                        .add_self_advertised_peer_with_timestamp(
                            source, key, value.info, timestamp, observed,
                        );

                    // Signature and publisher validation preceded confirmation. Only the
                    // authenticated transport source's own live binding gains connection ownership.
                    if record.publisher == Some(source) && self.swarm.is_connected(&source) {
                        self.swarm.behaviour_mut().kademlia.store_mut()
                            .retain_connected(source, record.key.clone())
                            .unwrap_or_else(|error| {
                                warn!(target: "network-kad", ?source, ?error,
                                    "connected binding could not be retained; identity remains confirmed");
                            });
                    }

                    // Store newer records and refresh the expiry of byte-identical republishes.
                    match freshness {
                        RecordFreshness::Newer | RecordFreshness::Identical => {
                            // Capacity is remotely triggerable. Match the add-provider path instead of
                            // propagating expected rejections to the run loop's per-event error log.
                            if should_store {
                                self.swarm.behaviour_mut().kademlia.store_mut().put_with_timestamp(record, Some(timestamp)).unwrap_or_else(
                                |error| match error {
                                    kad::store::Error::MaxRecords => {
                                        debug!(target: "network-kad", ?source, "dropping inbound kad record: store at capacity");
                                    }
                                    kad::store::Error::ValueTooLarge | kad::store::Error::MaxProvidedKeys => {
                                        warn!(target: "network-kad", ?source, ?error, "dropping inbound kad record");
                                    }
                                },
                                );
                            }
                        }
                        RecordFreshness::Older | RecordFreshness::Undecodable => {
                            // A peer republishing a slightly stale (but signature-valid) record is
                            // expected after restarts and benign. The local store keeps the newer
                            // version. Log only; no penalty.
                            trace!(target: "network-kad", ?source, "ignoring stale but valid kad record");
                        }
                    }
                }).unwrap_or_else(|| {
                    warn!(target: "network-kad", "Received invalid peer record!");

                    // assess penalty for invalid peer record
                    trace!(target: "network-kad", ?source, "processing fatal penalty for invalid peer record");
                    self.swarm.behaviour_mut().peer_manager.process_penalty(source, Penalty::Fatal);
                });
                Ok(PutOutcome::Processed)
            }
        }
    }

    /// Process an inbound kad add-provider request.
    ///
    /// Brought to parity with [`Self::process_kad_put_request`]. Kademlia runs
    /// under [`kad::StoreInserts::FilterBoth`], so this arm is the sole write
    /// path for inbound provider records: an attacker-supplied record would
    /// otherwise be persisted with no ban or authenticity check, unlike every
    /// other sender-supplied write. libp2p has already verified that a provider
    /// record's `provider` equals the authenticated request source, so gating on
    /// [`PeerManager::peer_banned`] rejects records from banned peers (including a
    /// peer banned at the application layer that the `PutRecord` path would also
    /// reject) before anything reaches the store. Records from banned peers are
    /// dropped rather than written. See issue #1001.
    ///
    /// Two further bounds keep one unbanned peer from starving the network task
    /// through this write path (GHSA-5475-xf29-3rv8). First, a per-provider rate
    /// limit ([`PeerManager::add_provider_rate_limited`]): each admitted message
    /// costs a row decode, merge, re-encode, insert, and a physical MDBX commit,
    /// and repeating `AddProvider` for an already-stored key skips the store's
    /// capacity gate, so an unbounded stream would run that work at line rate;
    /// over-budget messages are dropped with `Penalty::Load(LoadPenalty::KademliaRateLimit)`
    /// at Medium weight. Second, the
    /// expected capacity rejection is logged at `debug!` and never propagated:
    /// once the provider table saturates, `MaxProvidedKeys` is remotely
    /// triggerable, so propagating it would amplify a flood in the run-loop's
    /// per-event `error!`. Other store rejections remain visible at `warn!`, and
    /// database failures are logged at `error!` by the store with their cause.
    /// Rate-limit drops are counted separately for the primary and worker networks.
    fn process_kad_add_provider(&mut self, record: Option<kad::ProviderRecord>) {
        // The ban check borrows the swarm immutably and yields an owned `Option`
        // before the rate-limit and store steps borrow it mutably, so no two
        // borrows are held at once.
        let permitted = record
            .filter(|record| !self.swarm.behaviour().peer_manager.peer_banned(&record.provider));

        permitted.into_iter().for_each(|record| {
            let provider = record.provider;
            if self.swarm.behaviour_mut().peer_manager.add_provider_rate_limited(provider) {
                trace!(target: "network-kad", ?provider, "rate limiting inbound add provider");
                self.metrics.record_add_provider_rate_limited();
                self.swarm.behaviour_mut().peer_manager.process_penalty(provider, Penalty::Load(LoadPenalty::KademliaRateLimit));
            } else {
                self.swarm.behaviour_mut().kademlia.store_mut().add_provider(record).unwrap_or_else(
                    |error| match error {
                        kad::store::Error::MaxProvidedKeys => {
                            debug!(target: "network-kad", ?provider, "dropping inbound provider record: store at capacity");
                        }
                        kad::store::Error::ValueTooLarge | kad::store::Error::MaxRecords => {
                            warn!(target: "network-kad", ?provider, ?error, "dropping inbound provider record");
                        }
                    },
                );
            }
        });
    }

    /// Reuse retained metadata for the same signed timestamp, or admit it exactly once.
    fn admission_timestamp(
        &mut self,
        key: BlsPublicKey,
        signed: tn_types::TimestampSec,
        observed: tn_types::TimestampSec,
    ) -> crate::freshness::RecordTimestamp {
        self.swarm
            .behaviour()
            .peer_manager
            .record_timestamp(&key, signed)
            .or_else(|| {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .store_mut()
                    .record_timestamp(&crate::kad::node_record_key(&key))
                    .filter(|timestamp| timestamp.matches(signed))
            })
            .unwrap_or_else(|| crate::freshness::RecordTimestamp::admit(signed, observed))
    }

    /// Check the local kad store to compare record timestamps.
    ///
    /// Compare local admission metadata and signed bytes. Ordinary stale records cannot replace
    /// newer records, while cached future timestamps have a bounded repair path. Identical
    /// republishes refresh only DHT expiry, and conflicting equal signed timestamps stay stale.
    /// It is the caller's responsibility to ensure records are verified and valid.
    fn record_freshness(
        &mut self,
        record: &kad::Record,
        incoming: crate::freshness::RecordTimestamp,
        observed: tn_types::TimestampSec,
    ) -> RecordFreshness {
        let store = self.swarm.behaviour_mut().kademlia.store_mut();

        store.get(&record.key).map_or(RecordFreshness::Newer, |existing| {
            NodeRecord::try_decode_compat(&existing.value).map_or(
                RecordFreshness::Undecodable,
                |stored| {
                    let cached = store.record_timestamp(&record.key).unwrap_or_else(|| {
                        crate::freshness::RecordTimestamp::legacy(stored.info.timestamp, observed)
                    });
                    if existing.value == record.value {
                        RecordFreshness::Identical
                    } else if incoming.supersedes(cached, observed) {
                        RecordFreshness::Newer
                    } else {
                        RecordFreshness::Older
                    }
                },
            )
        })
    }

    /// Logic to process a kad record query result.
    ///
    /// The record arrives pre-validated — the caller already checked the signature
    /// and publisher via [`Self::peer_record_valid`]. This method checks:
    /// - the returned key matches the request
    /// - the latest node record is used
    fn process_kad_query_result(
        &mut self,
        query_id: &QueryId,
        key: BlsPublicKey,
        new_record: NodeRecord,
        peer: Option<PeerId>,
        is_last_step: bool,
    ) {
        // return if query id unknown - should not happen
        let observed = now();
        let timestamp = self.admission_timestamp(key, new_record.info.timestamp, observed);
        let Some(query) = self.kad_record_queries.get_mut(query_id) else { return };

        // ensure returned value matches request
        if query.query.request == key {
            query.consider_with_timestamp(new_record, timestamp, observed);
        } else {
            // assess penalty for returning record that doesn't match key
            if let Some(peer_id) = peer {
                trace!(target: "network-kad", ?peer_id, "processing fatal penalty for query record key mismatch");
                self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Fatal);
            }
        }

        // handle last step
        if is_last_step {
            self.close_kad_query(query_id);
        }
    }

    /// Preserve the longest accepted lifetime when a query returns an identical signed record.
    fn preserve_record_expiry(&mut self, mut record: kad::Record) -> kad::Record {
        record.expires = self
            .swarm
            .behaviour_mut()
            .kademlia
            .store_mut()
            .get(&record.key)
            .map_or(record.expires, |existing| {
                existing.expires.zip(record.expires).map(|(old, new)| old.max(new))
            });
        record
    }

    /// Cleanup kad record queries (called on last step).
    ///
    /// Promote the winning result into the peer manager's bounded discovery cache. Verified
    /// matching results may also fill an independently owned committee or pinned store row in
    /// [`Self::process_kad_event`]. The query grants no persistent ownership, and third-party
    /// periodic replication is disabled by [`configure_record_jobs`].
    fn close_kad_query(&mut self, query_id: &QueryId) {
        self.kad_record_queries
            .remove(query_id)
            .and_then(|query| {
                let key = query.query.request;
                query.into_result().map(|(record, timestamp)| (key, record, timestamp))
            })
            .into_iter()
            .for_each(|(key, node_record, timestamp)| {
                let peer: PeerId = node_record.info.pubkey.clone().into();
                self.swarm.behaviour_mut().peer_manager.add_discovered_peer_with_timestamp(
                    key,
                    node_record.info,
                    timestamp,
                );
                self.refresh_explicit_peer(&peer);
            });
    }
}

/// Internal query state with local ordering metadata, preserving the public [`KadQuery`] shape.
#[derive(Debug)]
pub(crate) struct PendingKadQuery {
    /// Requested authority and best authenticated record.
    query: KadQuery,
    /// Admission ceiling of the winning result, retained until the query closes.
    timestamp: Option<crate::freshness::RecordTimestamp>,
}

impl From<BlsPublicKey> for PendingKadQuery {
    fn from(key: BlsPublicKey) -> Self {
        Self { query: key.into(), timestamp: None }
    }
}

impl PendingKadQuery {
    /// Retain the freshest verified result under the shared local admission policy.
    #[cfg(test)]
    pub(crate) fn consider(&mut self, record: NodeRecord, observed: tn_types::TimestampSec) {
        let timestamp = crate::freshness::RecordTimestamp::admit(record.info.timestamp, observed);
        self.consider_with_timestamp(record, timestamp, observed);
    }

    /// Retain a verified result without renewing a timestamp already admitted elsewhere.
    fn consider_with_timestamp(
        &mut self,
        record: NodeRecord,
        timestamp: crate::freshness::RecordTimestamp,
        observed: tn_types::TimestampSec,
    ) {
        if self.timestamp.is_none_or(|cached| timestamp.supersedes(cached, observed)) {
            self.query.result = Some(record);
            self.timestamp = Some(timestamp);
        }
    }

    /// Consume the winning record together with its original admission ceiling.
    pub(crate) fn into_result(self) -> Option<(NodeRecord, crate::freshness::RecordTimestamp)> {
        self.query.result.zip(self.timestamp)
    }
}

/// Disable the libp2p-kad periodic record job.
///
/// On libp2p-kad 0.49, publication and replication are one `PutRecordJob`. Each run sends every
/// stored record that is not locally authored, so a replication interval of `None` alone does not
/// stop third-party replication. `ConsensusNetwork::refresh_own_record` republishes our record
/// on `kad_publication_interval`. Retained committee, pin, and connection bindings are served on
/// demand.
pub(crate) fn configure_record_jobs(config: &mut kad::Config) {
    config.set_publication_interval(None).set_replication_interval(None);
}

/// Enum if the received gossip is initially accepted for further processing.
///
/// This is necessary because libp2p does not impl `PartialEq` on [MessageAcceptance].
/// This impl does not map to `MessageAcceptance::Ignore`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GossipAcceptance {
    /// The message is considered valid, and it should be delivered and forwarded to the network.
    Accept,
    /// The message is considered invalid, and it should be rejected. The [`RejectReason`]
    /// records who is accountable for the rejection.
    Reject(RejectReason),
}

/// Why `verify_gossip` rejected a message.
///
/// The variant records *who* the fault is attributable to, which the reject path uses to decide
/// whether the relaying peer may be penalized. Rejecting a message never propagates it, regardless
/// of the reason; the distinction only governs peer scoring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RejectReason {
    /// The payload exceeds [`MAX_GOSSIP_MESSAGE_SIZE`]. That bound is a compile-time protocol
    /// constant, identical on every honest node, and enforced on both the publish path (the size
    /// guard in the `NetworkCommand::Publish` handler) and the receive path here. An honest node
    /// therefore never originates an oversized payload, and under gossipsub `Strict` validation
    /// never forwards one either: a peer that delivers an oversized payload is itself misbehaving,
    /// so the relaying peer is accountable.
    TooLarge,
    /// The message author is absent, has no resolved BLS identity, or is not an authorized
    /// publisher for the topic. The fault is the author's, not the forwarder's: an honest relayer
    /// merely forwarded content the author is responsible for, so the relaying peer is never
    /// penalized (the reject-path analogue of #801/#785). The resolved author is charged instead;
    /// an honest author authorized under a neighbouring committee view is spared by the committee
    /// exemption in the peer manager.
    UnauthorizedAuthor,
}

/// The peer-scoring outcome for a rejected gossip message. Rejecting never propagates the message;
/// this only decides which peer, if any, is penalized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RejectPenalty {
    /// Fatally penalize the relaying peer (`propagation_source`).
    FatalRelayer,
    /// Fatally penalize the message author (`GossipMessage::source`).
    FatalAuthor,
    /// Do not penalize any peer.
    Skip,
}

impl RejectReason {
    /// Decide the peer-scoring outcome for this reject, given whether the relaying peer's and the
    /// message author's BLS identities have resolved.
    ///
    /// An oversized payload is charged to the relaying peer: the size bound is a compile-time
    /// protocol constant ([`MAX_GOSSIP_MESSAGE_SIZE`]), identical on every honest node and enforced
    /// on both the publish and receive paths, so an honest peer never originates one and (under
    /// gossipsub `Strict` validation) never forwards one, making a delivered oversized payload
    /// relayer misbehavior. An unauthorized
    /// author is charged to the *author*, never the forwarder — an honest relayer merely forwarded
    /// content the author is responsible for (the reject-path analogue of #801/#785), and an honest
    /// author authorized under a neighbouring committee view is spared downstream by the committee
    /// exemption in the peer manager. Either penalty is skipped until the accountable peer's
    /// identity resolves: unattributable otherwise (the same join-window race the Accept path
    /// documents), which for the author also covers the anonymous-message and view-lag cases.
    fn penalty(self, relayer_resolved: bool, author_resolved: bool) -> RejectPenalty {
        match self {
            RejectReason::TooLarge if relayer_resolved => RejectPenalty::FatalRelayer,
            RejectReason::TooLarge => RejectPenalty::Skip,
            RejectReason::UnauthorizedAuthor if author_resolved => RejectPenalty::FatalAuthor,
            RejectReason::UnauthorizedAuthor => RejectPenalty::Skip,
        }
    }
}

impl From<GossipAcceptance> for MessageAcceptance {
    fn from(value: GossipAcceptance) -> Self {
        match value {
            GossipAcceptance::Accept => MessageAcceptance::Accept,
            GossipAcceptance::Reject(_) => MessageAcceptance::Reject,
        }
    }
}

impl<Req, Res, DB, Events> std::fmt::Debug for ConsensusNetwork<Req, Res, DB, Events>
where
    Req: TNMessage,
    Res: TNMessage,
    DB: Database,
    Events: TnSender<NetworkEvent<Req, Res>>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusNetwork")
            .field("authorized_publishers", &self.authorized_publishers)
            .field("pending_px_disconnects", &self.pending_px_disconnects)
            .field("pending_goodbyes", &self.pending_goodbyes)
            .field("outbound_requests", &self.outbound_requests.len())
            .field("inbound_requests", &self.inbound_requests.len())
            .field("config", &self.config)
            .field("connected_peers", &self.connected_peers)
            .field("swarm", &"<swarm>") // Skip detailed debug for swarm
            .finish()
    }
}

/// Peers a kademlia `GetClosestPeers` query located before it timed out.
///
/// Kademlia reports a timed-out query as [`kad::GetClosestPeersError::Timeout`],
/// whose payload carries the closest peers found so far. Those peers are still
/// valid discovery candidates, so they are recovered for the discovery pool
/// rather than discarded along with the failed query.
pub(crate) fn partial_peers_from_get_closest_timeout(
    err: kad::GetClosestPeersError,
) -> Vec<kad::PeerInfo> {
    let kad::GetClosestPeersError::Timeout { peers, .. } = err;
    peers
}

/// Pair a response payload with the responding peer's resolved BLS identity.
///
/// The payload is genuine whenever this node still holds the matching outbound
/// request, so a peer whose identity has not resolved yet (it connected before
/// its `NodeRecord` populated the confirmed-identity index) is reported as a
/// transient [`NetworkError::PeerUnresolved`] rather than the misleading
/// [`NetworkError::PeerMissing`].
fn resolve_response<Res: TNMessage>(
    resolved: Option<BlsPublicKey>,
    response: Res,
) -> NetworkResult<NetworkResponseMessage<Res>> {
    resolved
        .map(|peer| NetworkResponseMessage { peer, result: response })
        .ok_or(NetworkError::PeerUnresolved)
}

/// Build the application event for an accepted gossip message.
///
/// `relayer` is the relaying peer's BLS identity, or `None` while its
/// `NodeRecord` has not yet resolved. The accepted payload is delivered in
/// either case: the author is already authenticated during gossip verification,
/// and dropping an unresolved-relayer message would lose it permanently because
/// gossipsub has already cached its `message_id`. The relayer is carried so the
/// consumer can attribute a penalty only when the identity is known.
fn accepted_gossip_event<Req, Res>(
    message: GossipMessage,
    relayer: Option<BlsPublicKey>,
    author: Option<BlsPublicKey>,
) -> NetworkEvent<Req, Res> {
    NetworkEvent::Gossip(Box::new(GossipPayload { message, relayer, author }))
}
