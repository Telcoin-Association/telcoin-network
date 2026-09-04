//! Forward transactions accepted by a non-committee ("observer") worker to the committee.
//!
//! Instead of pushing raw transaction bytes over the libp2p worker protocol, an observer
//! forwards each transaction to the JSON-RPC endpoint the owning validator advertised on its
//! worker record (issue #804), so the submitter gets the same RPC experience they would get
//! talking to a validator directly. Routing mirrors [`submit_txn_if_mine`]: a transaction is
//! sent to the validator whose committee slot owns the sender, so all transactions from one
//! account converge on a single validator and nonce ordering is preserved. A validator that has
//! not advertised an endpoint (or is momentarily unreachable) is skipped in favor of one that
//! has. The fallback order rotates per node and per forward (issue #1173), so redirected
//! traffic spreads across the committee instead of concentrating on the lowest-keyed
//! validator.
//!
//! Four properties worth knowing at this boundary:
//!
//! - Transport security is whatever the validator advertised. Endpoint validation accepts both `https://`
//!   and plain `http://` URLs, and the forwarder adds no encryption of its own, so a plain-HTTP
//!   advertisement carries the signed raw transaction bytes in cleartext, readable by any on-path
//!   observer. (The bytes are already-signed transactions destined for a public mempool, so
//!   confidentiality is limited to submission timing, not contents.)
//! - A considered rejection stops the fallback chain only once a second validator confirms it. If a
//!   validator's RPC rejects the transaction itself (bad nonce, invalid, wrong fork), honest
//!   validators would all repeat the rejection - they share consensus state - but the sender
//!   routing above makes one validator the sole gatekeeper for its accounts, so a single fabricated
//!   rejection must not be able to censor an account (issue #1167): the first rejection is held as
//!   advisory, and a later delivery both overrides it and is surfaced as a byzantine signal
//!   ([`ForwarderMetrics::record_rejection_overridden`]). Endpoint-local failures (timeout,
//!   transport error, full pool, internal error, a refusal tied to one validator's own pool
//!   contents or admission config) fall through to the next advertised validator, and "already
//!   known" counts as delivered. The symmetric fabrication - a validator that answers success and
//!   drops the transaction - is still trusted at this boundary: a fabricated success is
//!   indistinguishable from an honest one here, and catching it would cost redundant delivery or
//!   inclusion tracking on every honest-path forward (issue #1167 records that trade).
//! - The dial target is chosen by a committee member, not by this node. The endpoint arrives inside
//!   a BLS-signed node record, so an arbitrary network peer cannot inject one, but a committee
//!   member can still advertise any host it likes and every observer will dial it unattended.
//!   [`ForwardTargetPolicy`] therefore refuses non-public hosts at the dial site by default, so a
//!   committee member cannot aim an observer's outbound HTTP at hosts inside that observer's own
//!   perimeter (issue #1092).
//! - Delivery feeds back into admission and the pool (issue #1145). Admission counts an endpoint as
//!   usable the moment its URL resolves, because the lazy HTTP client never dials until the first
//!   send. Two signals close the gap that leaves: an endpoint that fails at the connection level is
//!   demoted from admission for [`UNREACHABLE_COOLDOWN`] at the failure itself (a later delivery
//!   through the same endpoint lifts the demotion), so later batches are refused (and stay in the
//!   caller's pool) instead of being admitted against a dead endpoint; and a transaction that got
//!   no verdict is returned to the worker's own pool, so the batch builder repackages it instead of
//!   losing it.
//!
//! [`submit_txn_if_mine`]: tn_types::BatchValidation::submit_txn_if_mine

use crate::{
    metrics::{ForwardDropReason, ForwarderMetrics},
    recover_raw_transaction, WorkerTxPool,
};
use alloy::{
    providers::{Provider as _, RootProvider},
    rpc::{
        client::RpcClient,
        json_rpc::{RequestPacket, ResponsePacket},
    },
    transports::{
        http::Client, utils::guess_local_url, RpcError, TransportError, TransportErrorKind,
        TransportFut, TransportResult,
    },
};
use futures::{future::OptionFuture, StreamExt};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::{Ipv4Addr, Ipv6Addr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tn_types::{BlsPublicKey, RpcInfo, TaskSpawner, TxnForwarder};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{timeout, Instant},
};
use tracing::{debug, info, warn};
use url::{Host, Url};

/// Bounds a single validator's `eth_sendRawTransaction` round-trip so one unresponsive endpoint
/// cannot stall the fallback chain; on timeout the forward tries the next validator.
const FORWARD_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the total time spent forwarding one transaction across all its fallback validators, so a
/// whole unresponsive committee cannot make a single transaction cost `committee_size` back-to-back
/// [`FORWARD_SEND_TIMEOUT`]s. When it elapses the transaction is left unforwarded and the next one
/// proceeds.
const FORWARD_TX_BUDGET: Duration = Duration::from_secs(15);

/// Bounds the wall time one spawned forward spends working through its batch, so a task's
/// lifetime is a constant rather than a function of `transactions.len()`.
///
/// [`FORWARD_TX_BUDGET`] bounds one transaction, which on its own is not a bound on the task: a
/// full batch against an unresponsive committee costs `transactions.len()` budgets back to
/// back, which for a gas-full batch of roughly 1,400 transfers is close to six hours with the
/// batch's bytes pinned throughout. This is the per-task ceiling that makes that worst case a
/// constant. It also caps each transaction's own budget once less than one remains (see
/// [`next_txn_budget`]), so a task cannot overrun it by a trailing [`FORWARD_TX_BUDGET`].
///
/// Sized to clear a healthy full batch with room to spare rather than to be tight: the forward
/// loop is sequential, so a gas-full batch at a few tens of milliseconds per transaction is
/// already tens of seconds of legitimate work. Transactions still unforwarded when it elapses
/// are abandoned and counted, which forwarding being best-effort permits.
const FORWARD_BATCH_BUDGET: Duration = Duration::from_secs(120);

/// Bounds how many `forward-txns` tasks may be alive at once across every clone of one
/// forwarder.
///
/// A batch that arrives with every permit taken is dropped rather than queued: awaiting a permit
/// would move the unboundedness from live tasks into pending callers instead of removing it, and
/// shedding at the door is strictly better than shedding by OOM. Dropping is already this path's
/// failure mode when no endpoint is usable, and forwarding is documented as best-effort by its
/// only caller.
///
/// Sized against what one permit can pin. A sealed batch is capped at `max_batch_size()`
/// (1,000,000 bytes), so a full complement of forwards holds on the order of 64 MB of
/// transaction bytes, each for at most [`FORWARD_BATCH_BUDGET`]. It is not smaller because a
/// healthy forward is not instantaneous: the task walks its batch one transaction at a time, so
/// a gas-full batch occupies its permit for seconds even when every endpoint answers promptly,
/// and a tighter cap would shed batches an unstressed node could have delivered.
const MAX_CONCURRENT_FORWARDS: usize = 64;

/// Bounds how many bytes of one validator RPC response body the forwarder will buffer.
///
/// The dial target is chosen by a committee member (issue #1092), so the response is as
/// remote-controlled as the endpoint: alloy's stock HTTP transport buffers the entire body
/// before parsing, on success and error statuses alike, and [`FORWARD_SEND_TIMEOUT`] bounds
/// only how long that read may take, not how much it may hold (issue #1275). A
/// `send_raw_transaction` verdict is a few hundred bytes, so 64 KiB is orders of magnitude of
/// headroom, and with every permit taken the response buffers hold at most
/// [`MAX_CONCURRENT_FORWARDS`] x 64 KiB = 4 MiB instead of whatever a byzantine endpoint
/// cares to stream inside the timeout.
const MAX_FORWARD_RESPONSE_BYTES: usize = 64 * 1024;

/// Bounds how many bytes of remote-controlled text a [`Disposition`] reason may carry.
///
/// Reasons reach `warn!` logs (a confirmed rejection and a rejection override both print them
/// under the default filter) and the held-rejection mirror that lives for the rest of a walk,
/// so an unclamped reason turns each forwarded transaction into a log write and a retained
/// allocation of the remote server's choosing (issue #1275). Clamped once, at classification,
/// so every sink downstream inherits the bound. 256 bytes keeps every legitimate reth
/// rejection message intact.
const MAX_REASON_BYTES: usize = 256;

/// JSON-RPC error codes reth reserves for conditions that are validator-local or indeterminate,
/// never cleanly a verdict on the transaction: `-32003` is a full pool (`TxPoolOverflow`) plus
/// reth's catch-all for invalid-transaction variants with no explicit code of their own (for
/// example insufficient funds), and `-32603` is an internal/IO error. The forward falls through
/// to the next advertised validator on these: retrying a deterministic verdict caught in the
/// `-32003` mix costs at most one bounded pass over the committee, while stopping on a full pool
/// would drop a transaction another validator may still accept.
const TRANSIENT_RPC_CODES: [i64; 2] = [-32003, -32603];

/// Substring of reth's `eth_sendRawTransaction` error message when the transaction is already in a
/// validator's pool (`code -32000`). Treated as a successful delivery, not a failure to retry.
const ALREADY_KNOWN_MESSAGE: &str = "already known";

/// How long an endpoint that failed at the connection level (send timeout, or a transport error
/// with no server response) is held out of admission.
///
/// Admission ([`TxnForwarder::forward_txns`]) counts an endpoint as usable the moment its URL
/// resolves, because the lazy HTTP client never dials until the first send. This cooldown is the
/// delivery signal that closes that gap (issue #1145): once a send proves the endpoint dead, later
/// batches are refused (and therefore stay in the caller's pool) instead of being admitted
/// against it, until the cooldown elapses and one batch probes it again.
///
/// Sized against the two failure directions. Shorter would re-probe a dead endpoint more often,
/// and each probe batch pays real send timeouts before its transactions are requeued; longer
/// would keep refusing admission after an endpoint recovers, delaying delivery that could
/// succeed. Thirty seconds is a few tens of batch delays (default 1s): a brief endpoint blip
/// costs one cooldown of pooled (not lost) transactions, while a dead endpoint is probed at a
/// rate the forward budget absorbs without stacking tasks.
const UNREACHABLE_COOLDOWN: Duration = Duration::from_secs(30);

/// How long a forward task waits after its start before its first requeue.
///
/// Admission returns to the batch builder synchronously, but the prune that marks this batch's
/// transactions as mined runs on the builder loop concurrently with this task. A requeue that
/// lands before that prune hands the transactions straight back to it and loses them. One
/// second is orders of magnitude above the channel hops the prune path spends, and one batch
/// cadence, so it costs the recovery nothing observable while closing the race.
const REQUEUE_GRACE: Duration = Duration::from_secs(1);

/// The JSON-RPC code reth answers with for both chain-wide verdicts and the node-local refusals
/// in [`NODE_LOCAL_MESSAGES`] (`EthRpcErrorCode::InvalidInput`). The message carve-out is scoped
/// to this code so a matching substring under any other code keeps its terminal meaning.
const INVALID_INPUT_RPC_CODE: i64 = -32000;

/// Substrings of reth `eth_sendRawTransaction` error messages (code `-32000`) that describe one
/// validator's pool contents or one operator's admission config rather than a verdict on the
/// transaction itself. Reth maps almost every pool refusal to `-32000`, the same code it uses
/// for chain-wide verdicts, so the code axis cannot make this split; the message can. The
/// forward tries the next advertised validator on these, since a validator with different pool
/// contents or config may accept the same bytes.
const NODE_LOCAL_MESSAGES: [&str; 4] = [
    // `RpcPoolError::Underpriced`: priced below that operator's `--txpool.minimal-protocol-fee`.
    "transaction underpriced",
    // `RpcPoolError::ReplaceUnderpriced`: a same-nonce transaction already sits in that
    // validator's pool and the fee bump is below its `--txpool.pricebump`. Subsumed by the
    // entry above under `contains`, pinned separately so a reth rewording of one message
    // cannot silently drop coverage of the other.
    "replacement transaction underpriced",
    // `RpcPoolError::ExceedsFeeCap`: the fee is above that node's `--rpc.txfeecap`.
    "exceeds the configured cap",
    // `RpcPoolError::AddressAlreadyReserved`: blob-vs-regular sender exclusivity against that
    // validator's current pool contents.
    "address already reserved",
];

/// Whether the forwarder may dial an advertised endpoint whose host is not a public internet
/// address.
///
/// The endpoint is chosen by a committee member and dialed by an unattended observer process, so
/// an unconstrained target lets a committee member direct an observer's outbound HTTP at hosts
/// only that observer can reach. Production deployments run [`Self::PublicOnly`]; single-host and
/// docker-compose deployments, where validators legitimately advertise `127.0.0.1`, opt in to
/// [`Self::AllowPrivate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardTargetPolicy {
    /// Dial only hosts that could be a public internet peer. Refuses loopback, private-use,
    /// link-local, unique-local, shared-address-space, unspecified and non-unicast literals, and
    /// the reserved `localhost`/`.local` names. This is the default.
    PublicOnly,
    /// Dial whatever a committee member advertised, including addresses inside this node's own
    /// network. Only appropriate where every committee member is under the same operator.
    AllowPrivate,
}

impl ForwardTargetPolicy {
    /// Build the policy from the operator's `allow_private_forward_targets` setting, which is
    /// `false` (that is, [`Self::PublicOnly`]) unless the operator sets it.
    pub fn from_allow_private(allow_private: bool) -> Self {
        if allow_private {
            Self::AllowPrivate
        } else {
            Self::PublicOnly
        }
    }

    /// Decide whether `url` may be dialed under this policy.
    fn check(&self, url: &Url) -> Result<(), RefusedTarget> {
        match self {
            Self::AllowPrivate => Ok(()),
            Self::PublicOnly => check_public_host(url),
        }
    }
}

/// Why [`ForwardTargetPolicy::PublicOnly`] refused an advertised endpoint as a dial target.
///
/// Carried instead of a message string so the refusal reason is a closed set the log line and the
/// tests agree on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefusedTarget {
    /// The URL has no host component at all, so there is nothing to dial.
    MissingHost,
    /// Loopback: `127.0.0.0/8`, `::1`, or a name reserved to loopback by RFC 6761 (`localhost`,
    /// `*.localhost`).
    Loopback,
    /// RFC 1918 private-use: `10/8`, `172.16/12`, `192.168/16`.
    PrivateUse,
    /// Link-local: RFC 3927 `169.254.0.0/16` (which carries cloud instance metadata), RFC 4291
    /// `fe80::/10`, or an RFC 6762 multicast-DNS name (`*.local`).
    LinkLocal,
    /// RFC 4193 unique-local: `fc00::/7`.
    UniqueLocal,
    /// RFC 6598 shared address space: `100.64.0.0/10`, used for carrier-grade NAT and by several
    /// container networks.
    SharedAddressSpace,
    /// "This network": `0.0.0.0/8` (RFC 1122) or the unspecified address `::`.
    Unspecified,
    /// Multicast, broadcast or reserved: never a valid unicast dial target.
    NotUnicast,
}

impl std::fmt::Display for RefusedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::MissingHost => "no host",
            Self::Loopback => "loopback",
            Self::PrivateUse => "private-use (RFC 1918)",
            Self::LinkLocal => "link-local",
            Self::UniqueLocal => "unique-local (RFC 4193)",
            Self::SharedAddressSpace => "shared address space (RFC 6598)",
            Self::Unspecified => "unspecified",
            Self::NotUnicast => "not a unicast address",
        })
    }
}

/// Refuse `url` unless its host could be a public internet peer.
///
/// This is a literal check on the host as written: it never resolves DNS, so it costs nothing on
/// the cache-miss path and cannot be steered by a resolver the advertiser controls. The cost is
/// that a hostname resolving to a private address still passes, which is why the two reserved
/// name suffixes that are *defined* to be local ([`RefusedTarget::Loopback`] for `localhost`,
/// [`RefusedTarget::LinkLocal`] for `.local`) are refused by name. Refusing an arbitrary hostname
/// that merely happens to resolve inside the perimeter needs resolution plus address pinning in
/// the connector (otherwise a rebinding resolver defeats it) and is tracked separately.
///
/// URL host parsing normalizes the alternative IPv4 spellings (`http://2130706433/`,
/// `http://0177.0.0.1/`) into [`Host::Ipv4`] before this sees them, so those reach the same
/// predicate as dotted-quad.
fn check_public_host(url: &Url) -> Result<(), RefusedTarget> {
    url.host().ok_or(RefusedTarget::MissingHost).and_then(|host| match host {
        Host::Ipv4(addr) => check_public_ipv4(addr),
        Host::Ipv6(addr) => check_public_ipv6(addr),
        Host::Domain(name) => check_public_domain(name),
    })
}

/// Refuse an IPv4 literal that is not globally reachable.
fn check_public_ipv4(addr: Ipv4Addr) -> Result<(), RefusedTarget> {
    let [first, second, _, _] = addr.octets();
    if addr.is_loopback() {
        Err(RefusedTarget::Loopback)
    } else if addr.is_private() {
        Err(RefusedTarget::PrivateUse)
    } else if addr.is_link_local() {
        Err(RefusedTarget::LinkLocal)
    } else if first == 0 {
        // 0.0.0.0/8 "this network"; `is_unspecified` covers only 0.0.0.0 itself.
        Err(RefusedTarget::Unspecified)
    } else if first == 100 && (64..128).contains(&second) {
        // 100.64.0.0/10.
        Err(RefusedTarget::SharedAddressSpace)
    } else if first >= 224 {
        // 224.0.0.0/4 multicast, 240.0.0.0/4 reserved, and 255.255.255.255 broadcast.
        Err(RefusedTarget::NotUnicast)
    } else {
        Ok(())
    }
}

/// Refuse an IPv6 literal that is not globally reachable.
///
/// The IPv6-specific blocks are checked before the embedded-IPv4 fallback so `::1` is reported as
/// loopback rather than as the `0.0.0.1` it maps to. The fallback itself is what closes the
/// `http://[::ffff:127.0.0.1]/` spelling of a loopback address, which no IPv6 predicate catches.
fn check_public_ipv6(addr: Ipv6Addr) -> Result<(), RefusedTarget> {
    let [first, ..] = addr.segments();
    if addr.is_loopback() {
        Err(RefusedTarget::Loopback)
    } else if addr.is_unspecified() {
        Err(RefusedTarget::Unspecified)
    } else if first & 0xfe00 == 0xfc00 {
        // fc00::/7.
        Err(RefusedTarget::UniqueLocal)
    } else if first & 0xffc0 == 0xfe80 {
        // fe80::/10.
        Err(RefusedTarget::LinkLocal)
    } else if addr.is_multicast() {
        // ff00::/8.
        Err(RefusedTarget::NotUnicast)
    } else {
        embedded_ipv4(addr).map_or(Ok(()), check_public_ipv4)
    }
}

/// Extract the IPv4 address an IPv6 literal embeds, for the prefixes a translating gateway routes
/// back to that IPv4 destination.
///
/// Covers the two forms [`Ipv6Addr::to_ipv4`] recognizes, IPv4-compatible `::a.b.c.d` and
/// IPv4-mapped `::ffff:a.b.c.d`, plus two it does not:
///
/// - the RFC 6052 NAT64 well-known prefix `64:ff9b::/96`, which a NAT64 gateway translates to the
///   embedded address. This is the default egress path for IPv6-only cloud and container subnets,
///   so without it `http://[64:ff9b::a9fe:a9fe]/` reaches `169.254.169.254` on exactly the
///   deployments where `http://169.254.169.254/` is correctly refused.
/// - the RFC 3056 6to4 prefix `2002::/16`, which carries the IPv4 address in its next 32 bits.
///
/// RFC 6052 also allows network-specific prefixes at other lengths. Those use an operator's own
/// prefix and cannot be enumerated statically, so they are out of reach of a literal check; only
/// the well-known prefix is fixed enough to match.
fn embedded_ipv4(addr: Ipv6Addr) -> Option<Ipv4Addr> {
    let [first, second, third, fourth, fifth, sixth, seventh, eighth] = addr.segments();
    addr.to_ipv4().or_else(|| {
        ((first, second, third, fourth, fifth, sixth) == (0x0064, 0xff9b, 0, 0, 0, 0))
            .then(|| ipv4_from_segments(seventh, eighth))
            .or_else(|| (first == 0x2002).then(|| ipv4_from_segments(second, third)))
    })
}

/// Rebuild an IPv4 address from the two 16-bit segments that carry it inside an IPv6 literal.
fn ipv4_from_segments(high: u16, low: u16) -> Ipv4Addr {
    let [a, b] = high.to_be_bytes();
    let [c, d] = low.to_be_bytes();
    Ipv4Addr::new(a, b, c, d)
}

/// Refuse the two name suffixes that are reserved to resolve locally.
///
/// Every other hostname passes: see [`check_public_host`] for why resolution is out of scope here.
fn check_public_domain(name: &str) -> Result<(), RefusedTarget> {
    // The absolute form (`localhost.`) resolves identically to the relative one.
    let name = name.trim_end_matches('.').to_ascii_lowercase();
    if name == "localhost" || name.ends_with(".localhost") {
        // RFC 6761 section 6.3: reserved, resolves to loopback.
        Err(RefusedTarget::Loopback)
    } else if name == "local" || name.ends_with(".local") {
        // RFC 6762: multicast DNS, resolves only on the local link.
        Err(RefusedTarget::LinkLocal)
    } else {
        Ok(())
    }
}

/// Per-endpoint state the forwarder keeps across batch seals, behind one lock.
#[derive(Default)]
struct EndpointCache {
    /// HTTP providers cached per advertised endpoint so consecutive batch seals reuse the
    /// underlying connection pool instead of paying a fresh TCP+TLS handshake per validator
    /// per seal. Entries for endpoints that are no longer advertised are evicted on each
    /// forward, so the cache stays bounded by the current committee's advertisement set.
    providers: BTreeMap<String, RootProvider>,
    /// Endpoints already refused by [`ForwardTargetPolicy`], so the refusal is logged once per
    /// advertisement instead of once per batch seal. Evicted alongside `providers`, so a
    /// committee member that re-advertises a refused endpoint is logged again.
    ///
    /// Evicting is the deliberate side of the trade: it bounds this set by the current committee's
    /// advertisement set, at the cost of letting a member that toggles its advertisement re-arm
    /// the warning. Never evicting would silence that, but would let a member rotating through
    /// fresh URLs grow the set without bound, which is the worse failure for an unattended node.
    refused: BTreeSet<String>,
    /// Endpoints demoted after a connection-level send failure, keyed to the moment of
    /// demotion. A demoted endpoint is skipped by [`WorkerRpcForwarder::cached_providers`],
    /// and so does not count toward admission, until [`UNREACHABLE_COOLDOWN`] elapses or the
    /// endpoint leaves the advertisement set, whichever comes first. Bounded the same way as
    /// `providers`: entries for endpoints no longer advertised are evicted on each forward.
    unreachable: BTreeMap<String, Instant>,
}

/// Forwards observer transactions to validators over their advertised JSON-RPC endpoints.
#[derive(Clone)]
pub struct WorkerRpcForwarder {
    /// Spawner used to run the (best-effort, non-blocking) forward on a background task.
    task_spawner: TaskSpawner,
    /// Which advertised hosts this node is willing to dial.
    policy: ForwardTargetPolicy,
    /// Providers and refusals for the currently advertised endpoints.
    cache: Arc<Mutex<EndpointCache>>,
    /// Admission control for the background forwards: one permit per live `forward-txns` task,
    /// [`MAX_CONCURRENT_FORWARDS`] of them.
    ///
    /// Shared through the `Arc` by every clone of this forwarder, which is what makes the cap
    /// node-wide rather than per-clone. A permit is taken before the spawn and moved into the
    /// task, so capacity comes back when a forward actually finishes.
    forwards_in_flight: Arc<Semaphore>,
    /// The worker's own transaction pool, where a forward task returns every transaction that
    /// got no verdict (issue #1145). The batch builder prunes a batch's transactions as mined
    /// the moment the batch is admitted, so an admitted-then-undelivered transaction is
    /// otherwise in no pool and no table. `None` means there is no pool to return them to
    /// (tests); undelivered transactions are then dropped as before, with the same warnings.
    requeue_pool: Option<WorkerTxPool>,
    /// Rotation counter for the fallback dial order.
    ///
    /// Starts at a random per-process value and advances once per spawned forward: see
    /// [`rotated_fallbacks`] for why the order must differ per node and per forward. Shared
    /// through the `Arc` by every clone of this forwarder, so the rotation is node-wide like
    /// the forward cap.
    fallback_rotation: Arc<AtomicU64>,
}

impl std::fmt::Debug for WorkerRpcForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerRpcForwarder")
    }
}

impl WorkerRpcForwarder {
    /// Create a new forwarder that runs forwards on `task_spawner` and dials only the advertised
    /// hosts `policy` admits. `requeue_pool` is the worker's own transaction pool, where a
    /// forward task returns transactions that got no verdict; `None` drops them as before.
    ///
    /// Registering the forwarder's counters here means a node that never sheds still exports
    /// them from start, so an absent series stays distinguishable from a broken exporter.
    pub fn new(
        task_spawner: TaskSpawner,
        policy: ForwardTargetPolicy,
        requeue_pool: Option<WorkerTxPool>,
    ) -> Self {
        ForwarderMetrics::init();
        Self {
            task_spawner,
            policy,
            cache: Arc::new(Mutex::new(EndpointCache::default())),
            forwards_in_flight: Arc::new(Semaphore::new(MAX_CONCURRENT_FORWARDS)),
            requeue_pool,
            // Random, not zero: observers restarted together must not share one rotation
            // phase, and no committee key can buy a fixed position in the order (issue
            // #1173). This seeds fairness, not secrecy: predicting it moves no trust
            // boundary.
            fallback_rotation: Arc::new(AtomicU64::new(rand::random())),
        }
    }

    /// Resolve the advertised endpoints to providers, reusing cached ones where the endpoint
    /// is unchanged. An endpoint the policy refuses, or a malformed URL, is dropped here so the
    /// per-transaction loop only sees usable endpoints. The endpoint string is reparsed into the
    /// exact URL type the provider expects. `RootProvider` needs no fillers: the transactions are
    /// already signed raw bytes, and `send_raw_transaction` submits them as-is.
    ///
    /// The policy is applied on the cache-miss path only, which is where the dial target is turned
    /// into a live provider: a refused endpoint is never inserted, so it can never be served from
    /// the cache on a later seal.
    fn cached_providers(
        &self,
        validator_rpcs: &[(BlsPublicKey, RpcInfo)],
    ) -> BTreeMap<BlsPublicKey, (String, RootProvider)> {
        let mut cache = self.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let advertised: BTreeSet<String> =
            validator_rpcs.iter().map(|(_, rpc)| rpc.http.to_string()).collect();
        cache.providers.retain(|url, _| advertised.contains(url));
        cache.refused.retain(|url| advertised.contains(url));
        // A demotion expires two ways: its cooldown elapses, or the endpoint leaves the
        // advertisement set (the same bound that keeps `providers` finite).
        cache.unreachable.retain(|url, demoted| {
            advertised.contains(url) && demoted.elapsed() < UNREACHABLE_COOLDOWN
        });
        validator_rpcs
            .iter()
            .filter_map(|(key, rpc)| {
                let url = rpc.http.to_string();
                // A demoted endpoint sits out admission entirely: it is neither served from
                // the provider cache nor re-created, so a committee whose only advertised
                // endpoint just proved unreachable refuses the next batch (keeping its
                // transactions pooled) instead of admitting it (issue #1145).
                (!cache.unreachable.contains_key(&url)).then_some(())?;
                cache
                    .providers
                    .get(&url)
                    .cloned()
                    .or_else(|| {
                        self.policy
                            .check(&rpc.http)
                            .map_err(|reason| {
                                // Log once per advertisement, not once per seal, so an operator
                                // can tell a refused endpoint from an unreachable one without the
                                // line repeating on every batch.
                                if cache.refused.insert(url.clone()) {
                                    warn!(
                                        target: "worker::forward",
                                        validator = %key,
                                        endpoint = %rpc.http,
                                        %reason,
                                        "refusing to forward to a non-public advertised RPC \
                                         endpoint; set `allow_private_forward_targets` to permit it"
                                    );
                                }
                            })
                            .ok()
                            .and_then(|()| {
                                url.parse()
                                    .map_err(|err| {
                                        debug!(
                                            target: "worker::forward",
                                            endpoint = %rpc.http,
                                            %err,
                                            "skipping validator with an unparseable RPC endpoint"
                                        );
                                    })
                                    .ok()
                            })
                            .map(|parsed| {
                                let provider = capped_provider(parsed);
                                cache.providers.insert(url.clone(), provider.clone());
                                provider
                            })
                    })
                    .map(|provider| (*key, (url, provider)))
            })
            .collect()
    }

    /// Spawn the background task that forwards one batch, holding `permit` for its lifetime.
    ///
    /// Split out from [`TxnForwarder::forward_txns`] so the admission decision and the work it
    /// admits read separately. The task is bounded in both directions that matter: `permit` caps
    /// how many of these exist at once ([`MAX_CONCURRENT_FORWARDS`]), and the batch deadline
    /// caps how long this one lives ([`FORWARD_BATCH_BUDGET`]). The transaction bytes a node can
    /// have pinned in forwards at any moment are therefore the product of two constants rather
    /// than a function of its inbound rate.
    fn spawn_forward(
        &self,
        permit: OwnedSemaphorePermit,
        transactions: Vec<Vec<u8>>,
        committee_slots: Vec<BlsPublicKey>,
        committee_size: u64,
        providers: BTreeMap<BlsPublicKey, (String, RootProvider)>,
    ) {
        // Fallback order: every usable endpoint, so a transaction whose owning validator has
        // not advertised (or is unreachable) can still reach the committee. Rotated per
        // forward, so the redirect load of a downed owner spreads across the committee
        // instead of landing on the lowest-keyed validator every time (issue #1173).
        let fallbacks = rotated_fallbacks(
            providers.keys().cloned().collect(),
            self.fallback_rotation.fetch_add(1, Ordering::Relaxed),
        );
        let cache = Arc::clone(&self.cache);
        let requeue_pool = self.requeue_pool.clone();

        self.task_spawner.spawn_task("forward-txns", async move {
            // Moved in rather than released when `forward_txns` returned, so capacity comes
            // back when this forward actually finishes.
            let _permit = permit;
            let deadline = Instant::now() + FORWARD_BATCH_BUDGET;
            // The point in time the first requeue may land; see [`REQUEUE_GRACE`].
            let requeue_ready = Instant::now() + REQUEUE_GRACE;
            let queued = transactions.len();
            let mut delivered = 0_usize;
            let mut rejected = 0_usize;
            let mut unreached = 0_usize;
            // Transactions that got no verdict, and how many of those the worker's own pool
            // accepted back (issue #1145). Requeueing happens per transaction, as each verdict
            // lands, so an epoch-boundary abort of this task loses at most the transactions it
            // had not walked yet rather than the whole batch.
            let mut no_verdict = 0_usize;
            let mut requeued = 0_usize;
            // Endpoints this task proved unreachable. Each demotion is published to the shared
            // cache at the failure itself, so admission for batches sealed while this task is
            // still walking stops counting the endpoint at once, and the set doubles as the
            // within-batch skip list, so later transactions here do not re-pay the send
            // timeout against a proven-dead endpoint. A later delivery through the same
            // endpoint clears both. Mutex-wrapped like the held rejection: the walk holds
            // only a shared reference ([`walk_fallback_chain`]).
            let unreachable_endpoints: Mutex<BTreeSet<String>> = Mutex::new(BTreeSet::new());
            // How many transactions the loop pulled before the batch deadline ended it;
            // everything at and past this index is the abandoned (also undelivered) tail.
            let mut pulled = 0_usize;
            // `map_while` is what ends the batch at the deadline: it runs as the loop pulls each
            // transaction, so every iteration sees the budget left at that moment and the batch
            // stops at the first transaction that finds none.
            let budgeted = transactions.iter().enumerate().map_while(|(idx, tx)| {
                next_txn_budget(deadline, Instant::now()).map(|left| (idx, tx, left))
            });

            for (idx, tx, txn_budget) in budgeted {
                pulled = idx + 1;
                // A budget below [`FORWARD_TX_BUDGET`] means the batch deadline is what clamped
                // it (see [`next_txn_budget`]). On its own that says nothing about how this
                // transaction ends: a clamped budget can still carry a whole fast-failing
                // fallback chain to a real verdict, so the tally below also requires that the
                // budget actually expired.
                let deadline_clamped = txn_budget < FORWARD_TX_BUDGET;
                // Route by sender so all transactions from one account land on the same
                // validator (matches `submit_txn_if_mine`), then fall back to any endpoint.
                let owner = owning_validator(tx, committee_size, &committee_slots);
                let ordered = owner.into_iter().chain(fallbacks.iter().cloned());

                // Bound the whole fallback chain for this transaction: even if every advertised
                // validator accepts the connection but never answers, one transaction cannot cost
                // more than `FORWARD_TX_BUDGET` before the next transaction proceeds.
                // A first rejection the walk has banked escapes the budget through this slot:
                // cancellation drops the walk and everything in it, and a verdict a validator
                // already gave must not vanish with it (issue #1167).
                let held_rejection = Mutex::new(None);
                let chain = timeout(
                    txn_budget,
                    walk_fallback_chain(
                        tx.as_slice(),
                        ordered,
                        &providers,
                        &held_rejection,
                        &unreachable_endpoints,
                        &cache,
                    ),
                )
                .await;
                // `Err` from the timeout is the budget itself expiring, as opposed to the
                // chain returning [`ForwardOutcome::NoEndpointReached`] as a verdict; the
                // tally below needs the two apart, so remember which happened before
                // collapsing them into one outcome.
                let budget_expired = chain.is_err();
                let outcome = expired_walk_outcome(chain, &held_rejection);

                match outcome {
                    ForwardOutcome::Delivered => delivered += 1,
                    ForwardOutcome::Rejected(reason) => {
                        rejected += 1;
                        warn!(
                            target: "worker::forward",
                            reason = %reason,
                            "the forwarded transaction was rejected; no validator accepted it"
                        )
                    }
                    // A deadline-clamped budget expired mid-chain: the batch budget, not
                    // endpoint behavior, is what cut this transaction short, so it belongs to
                    // the abandoned remainder below, not to `unreached`. Both conditions
                    // matter: an expired *full* budget is endpoint behavior (a chain of
                    // unresponsive validators), and a verdict reached inside a clamped budget
                    // is a real verdict; each of those counts as `unreached` in the arm below.
                    ForwardOutcome::NoEndpointReached if budget_expired && deadline_clamped => {
                        no_verdict += 1;
                        requeued +=
                            requeue_one(requeue_pool.as_ref(), requeue_ready, tx.clone()).await;
                        warn!(
                            target: "worker::forward",
                            "batch budget expired before this transaction could reach a validator RPC"
                        )
                    }
                    ForwardOutcome::NoEndpointReached => {
                        unreached += 1;
                        no_verdict += 1;
                        requeued +=
                            requeue_one(requeue_pool.as_ref(), requeue_ready, tx.clone()).await;
                        warn!(
                            target: "worker::forward",
                            "could not forward transaction to any advertised validator RPC"
                        )
                    }
                }
            }

            // The per-transaction losses, counted per batch rather than per event. A rejection
            // is a considered verdict a validator saw; an unreached transaction exhausted its
            // whole fallback chain. Both are transactions this node accepted and never
            // delivered, so both subtract from the queued denominator - see
            // [`ForwardDropReason`] for which of the two is alertable.
            if rejected > 0 {
                ForwarderMetrics::record_txns_dropped(
                    ForwardDropReason::Rejected,
                    u64::try_from(rejected).unwrap_or(u64::MAX),
                );
            }
            if unreached > 0 {
                ForwarderMetrics::record_txns_dropped(
                    ForwardDropReason::Unreached,
                    u64::try_from(unreached).unwrap_or(u64::MAX),
                );
            }

            // Everything the batch deadline cut off: whatever `map_while` stopped short of,
            // plus any transaction whose deadline-clamped budget expired mid-chain. Derived
            // from the individual outcome tallies rather than from a count of loop iterations
            // so a sliver-budget transaction counts as abandoned instead of passing as a
            // full-budget attempt. Counted as well as logged: a batch the node accepted and
            // then gave up on is an absorbed failure, invisible otherwise.
            let abandoned = queued.saturating_sub(delivered + rejected + unreached);
            if abandoned > 0 {
                warn!(
                    target: "worker::forward",
                    abandoned,
                    delivered,
                    rejected,
                    unreached,
                    budget_secs = FORWARD_BATCH_BUDGET.as_secs(),
                    "batch forward budget elapsed; abandoning the rest of the batch"
                );
                ForwarderMetrics::record_txns_abandoned(
                    u64::try_from(abandoned).unwrap_or(u64::MAX),
                );
            }

            // Report the endpoints this task demoted. Each was already published to admission
            // at its failure; an endpoint that later delivered was cleared again and is not
            // counted here.
            let demoted = unreachable_endpoints
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len();
            if demoted > 0 {
                warn!(
                    target: "worker::forward",
                    demoted,
                    cooldown_secs = UNREACHABLE_COOLDOWN.as_secs(),
                    "demoting unreachable validator RPC endpoints from forward admission"
                );
                ForwarderMetrics::record_endpoints_demoted(
                    u64::try_from(demoted).unwrap_or(u64::MAX),
                );
            }

            // The abandoned tail never got a verdict either: requeue it the same way. The
            // builder pruned the whole batch as mined at admission, so a transaction that is
            // not requeued is in no pool and no table. A considered rejection is a verdict
            // and is never requeued; a requeued transaction that was in fact delivered is
            // dropped by the destination pool as a duplicate on the next forward
            // ("already known").
            no_verdict += queued.saturating_sub(pulled);
            requeued += futures::stream::iter(transactions.into_iter().skip(pulled))
                .then(|tx| requeue_one(requeue_pool.as_ref(), requeue_ready, tx))
                .fold(0_usize, |acc, added| async move { acc + added })
                .await;
            if no_verdict > 0 {
                info!(
                    target: "worker::forward",
                    undelivered = no_verdict,
                    requeued,
                    "returned undelivered forwarded transactions to the worker's pool"
                );
            }
            Ok(())
        });
    }
}

/// Return one undelivered transaction to the worker's own pool, no earlier than `ready_at`.
///
/// The wait orders the requeue after the batch builder's prune of this batch (the ack+prune
/// path this task races; see [`REQUEUE_GRACE`]). It is a point in time, not a per-call delay,
/// so only a task's first requeue can actually sleep. Returns 1 when the pool accepted the
/// transaction back, 0 otherwise: with no pool to return to (tests), or bytes that no longer
/// recover, or a pool refusal (full, replaced, already executed), the transaction is dropped
/// exactly as the whole path dropped it before issue #1145. The metric moves per transaction,
/// so an epoch-boundary abort of the task cannot erase requeues that already happened.
async fn requeue_one(pool: Option<&WorkerTxPool>, ready_at: Instant, tx: Vec<u8>) -> usize {
    tokio::time::sleep_until(ready_at).await;
    let added = OptionFuture::from(pool.and_then(|pool| {
        recover_raw_transaction(&tx)
            .ok()
            .map(|recovered| pool.add_recovered_transaction_external(recovered))
    }))
    .await
    .is_some_and(|outcome| outcome.is_ok());
    if added {
        ForwarderMetrics::record_txns_requeued(1);
    }
    usize::from(added)
}

impl TxnForwarder for WorkerRpcForwarder {
    fn forward_txns(
        &self,
        transactions: Vec<Vec<u8>>,
        committee_slots: Vec<BlsPublicKey>,
        validator_rpcs: Vec<(BlsPublicKey, RpcInfo)>,
    ) -> bool {
        let committee_size = committee_slots.len() as u64;
        let queued = transactions.len();
        let queued_total = u64::try_from(queued).unwrap_or(u64::MAX);
        if queued > 0 {
            // The base series the drop and abandon counters read against: every transaction
            // handed to the forwarder, counted before any admission decision.
            ForwarderMetrics::record_txns_queued(queued_total);
        }
        // Nothing to forward, or nowhere to forward it to: neither is a fault, so neither
        // warns - the worker warns and counts its own guard on the production path. But a
        // batch with nowhere to go is still dropped, so it is counted (issue #1133).
        let targets_known = committee_size > 0 && !validator_rpcs.is_empty();
        if queued > 0 && !targets_known {
            ForwarderMetrics::record_txns_dropped(ForwardDropReason::EmptyCommittee, queued_total);
        }
        let discovered =
            (queued > 0 && targets_known).then(|| self.cached_providers(&validator_rpcs));
        // Endpoints were advertised but none of them resolved to a usable provider. That is a
        // fault, and the batch is refused for it so the caller keeps its transactions.
        let admissible = discovered.filter(|providers| {
            let usable = !providers.is_empty();
            if !usable {
                warn!(
                    target: "worker::forward",
                    "no usable validator RPC endpoints; refusing the batch so the caller keeps \
                     its transactions"
                );
                ForwarderMetrics::record_txns_dropped(
                    ForwardDropReason::NoUsableEndpoint,
                    queued_total,
                );
            }
            usable
        });

        // Admission control. Reached after the cache refresh above, so eviction of
        // no-longer-advertised endpoints keeps happening while batches are being shed, and
        // before any spawn here, so a batch this node has no capacity to forward never becomes
        // a forward task holding its bytes. (`disburse_txns` calls this synchronously and keeps
        // the batch pooled on refusal: issue #1132.) Try-acquire, not acquire: see
        // [`MAX_CONCURRENT_FORWARDS`] for why a batch that finds no permit is refused rather
        // than queued behind one.
        admissible.is_some_and(|providers| {
            Arc::clone(&self.forwards_in_flight).try_acquire_owned().map_or_else(
                |_| {
                    warn!(
                        target: "worker::forward",
                        transactions = queued,
                        capacity = MAX_CONCURRENT_FORWARDS,
                        "forward capacity exhausted; refusing a sealed batch rather than queueing it"
                    );
                    ForwarderMetrics::record_batch_shed();
                    // The transaction-level count beside the batch-level one, so shed batches
                    // subtract from the queued denominator like every other drop.
                    ForwarderMetrics::record_txns_dropped(ForwardDropReason::BatchShed, queued_total);
                    false
                },
                move |permit| {
                    self.spawn_forward(
                        permit,
                        transactions,
                        committee_slots,
                        committee_size,
                        providers,
                    );
                    true
                },
            )
        })
    }
}

/// Budget for the next transaction of a batch whose whole-batch deadline is `deadline`.
///
/// `None` once `now` has reached the deadline, which is how a batch stops early rather than
/// running for `transactions.len()` per-transaction budgets. Otherwise the per-transaction
/// [`FORWARD_TX_BUDGET`], clamped to whatever is left, so the last transaction a batch attempts
/// cannot carry its task past [`FORWARD_BATCH_BUDGET`].
fn next_txn_budget(deadline: Instant, now: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(now);
    (!remaining.is_zero()).then(|| remaining.min(FORWARD_TX_BUDGET))
}

/// The fallback dial order for one spawned forward: `fallbacks` rotated left by `counter`,
/// reduced modulo the list length.
///
/// Without rotation the order is the raw byte sort of the committee's BLS public keys, the
/// same on every observer. Every observer then redirects a downed owner's transactions to the
/// same lowest-keyed reachable validator at the same time, and a validator can grind its BLS
/// keypair offline so its key sorts first and that position becomes permanent (issue #1173).
/// The counter starts at a per-process random value, so observers walk different orders and a
/// ground key buys nothing, and it advances once per spawned forward, so one observer also
/// spreads consecutive batches. Owner-first routing is unchanged: the owner is dialed ahead
/// of this list (see [`WorkerRpcForwarder::spawn_forward`]).
fn rotated_fallbacks(fallbacks: Vec<BlsPublicKey>, counter: u64) -> Vec<BlsPublicKey> {
    let count = fallbacks.len();
    let start = rotation_start(counter, count);
    fallbacks.iter().cycle().skip(start).take(count).cloned().collect()
}

/// Starting offset into a fallback list of `fallback_count` entries: `counter` reduced modulo
/// the length. Total for every input: an empty list gets offset zero rather than a division
/// by zero, and a length past `u64` (impossible on any real target) also degrades to zero
/// rather than truncating.
fn rotation_start(counter: u64, fallback_count: usize) -> usize {
    u64::try_from(fallback_count)
        .ok()
        .filter(|count| *count > 0)
        .and_then(|count| usize::try_from(counter % count).ok())
        .unwrap_or_default()
}

/// Return the BLS key of the committee slot that owns `tx_bytes`, matching the receiver-side
/// routing in `submit_txn_if_mine`. Returns `None` if the transaction cannot be recovered or the
/// derived slot is out of range.
fn owning_validator(
    tx_bytes: &[u8],
    committee_size: u64,
    committee_slots: &[BlsPublicKey],
) -> Option<BlsPublicKey> {
    let recovered = recover_raw_transaction(tx_bytes).ok()?;
    let sender = recovered.signer();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&sender.as_slice()[0..8]);
    let slot = (u64::from_le_bytes(bytes) % committee_size) as usize;
    committee_slots.get(slot).cloned()
}

/// Walk one transaction across the `ordered` validators until one delivers it or a second one
/// confirms a considered rejection.
///
/// The advisory-until-confirmed rejection is byzantine hardening (issue #1167). Sender routing
/// hands every transaction from one account to the same owning validator first, so under a
/// stop-on-first-verdict rule that one validator's word decided the transaction's fate: a
/// byzantine owner could fabricate a rejection and censor the account, with no honest validator
/// ever consulted. A first considered rejection is therefore held rather than returned, and the
/// walk goes on until a second validator either rejects too - an honest rejection repeats
/// everywhere, because validators share consensus state - or delivers the transaction, which
/// overrides the held rejection and is counted and logged as a byzantine signal
/// ([`ForwarderMetrics::record_rejection_overridden`]). A rejection that reaches the end of the
/// chain unconfirmed stands: no other validator was reachable to consult, so the one verdict
/// there is decides. The same rule survives the caller's per-transaction budget cancelling the
/// walk mid-chain: the held verdict is mirrored into `held`, and [`expired_walk_outcome`] reads
/// it back rather than letting cancellation erase it. Confirmation raises the collusion a
/// censoring rejection needs from one validator to two; it is hardening against a single liar,
/// not a quorum guarantee.
///
/// The cost is one extra RPC round-trip per genuinely rejected transaction, which is rare on
/// this path: every transaction here already passed this observer's own pool validation, so a
/// considered rejection normally means the state moved (a nonce race), not spam. The symmetric
/// fabrication - a validator that answers success and drops the transaction - is deliberately
/// not defended here: a fabricated success is indistinguishable from an honest one at this
/// boundary, and catching it would cost redundant delivery or inclusion tracking on every
/// honest-path forward. Issue #1167 records that trade.
///
/// The walk also carries the endpoint demotion of issue #1145: an endpoint already recorded in
/// `unreachable` is skipped, a send that produced no JSON-RPC verdict demotes its endpoint -
/// into `unreachable` and into the admission side of `cache`, at the failure itself - and a
/// later delivery through the same endpoint lifts a demotion this batch recorded.
async fn walk_fallback_chain(
    tx: &[u8],
    ordered: impl Iterator<Item = BlsPublicKey>,
    providers: &BTreeMap<BlsPublicKey, (String, RootProvider)>,
    held: &Mutex<Option<(BlsPublicKey, String)>>,
    unreachable: &Mutex<BTreeSet<String>>,
    cache: &Mutex<EndpointCache>,
) -> ForwardOutcome {
    use futures::TryStreamExt as _;
    // Each advertised validator at most once, in fallback order, with its endpoint and
    // provider attached. The unreachable filter is the within-batch half of the demotion
    // (issue #1145): an endpoint this task already proved dead is not re-dialed, and the
    // filter runs as the stream pulls each target, so it also sees demotions recorded
    // earlier in this same walk.
    let mut tried = BTreeSet::new();
    let targets = ordered
        .filter(|key| tried.insert(*key))
        .filter_map(|key| providers.get(&key).map(|(endpoint, provider)| (key, endpoint, provider)))
        .filter(|(_, endpoint, _)| {
            !unreachable.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).contains(*endpoint)
        });
    // A short-circuiting fold: `Ok` carries the held first rejection (which validator gave it
    // and why) between attempts, `Err` carries a terminal verdict out of the walk.
    futures::stream::iter(targets)
        .map(Ok)
        .try_fold(
            None::<(BlsPublicKey, String)>,
            |pending_rejection, (key, endpoint, provider)| async move {
                // Bound each validator's round-trip: an endpoint that accepts the connection but
                // never answers must not stall the whole fallback chain.
                let disposition = timeout(FORWARD_SEND_TIMEOUT, provider.send_raw_transaction(tx))
                    .await
                    .map_or_else(
                        |_elapsed| Disposition::Unreachable("send timed out".to_string()),
                        |res| res.err().map_or(Disposition::Delivered, classify_error),
                    );

                match disposition {
                    Disposition::Delivered => {
                        // A delivery proves the endpoint reachable again: lift any demotion this
                        // task recorded, locally and in admission, so a single earlier blip (one
                        // timed-out send against an otherwise healthy validator) cannot hold a
                        // delivering endpoint out (issue #1145).
                        if unreachable
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .remove(endpoint)
                        {
                            cache
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .unreachable
                                .remove(endpoint);
                        }
                        pending_rejection.iter().for_each(|(rejecting, reason)| {
                        ForwarderMetrics::record_rejection_overridden();
                        warn!(
                            target: "worker::forward",
                            rejected_by = %rejecting,
                            delivered_by = %key,
                            reason = %reason,
                            "one validator rejected a transaction another validator accepted; \
                             the rejecting validator answered from divergent state or is byzantine"
                        );
                    });
                        Err(ForwardOutcome::Delivered)
                    }
                    Disposition::Rejected(reason) => {
                        // A second considered rejection confirms the held one. The `tried` set
                        // keeps the two verdicts from distinct validators, which is what a single
                        // byzantine validator cannot produce alone.
                        if pending_rejection.is_some() {
                            Err(ForwardOutcome::Rejected(reason))
                        } else {
                            debug!(
                                target: "worker::forward",
                                validator = %key,
                                reason = %reason,
                                "validator rejected the forwarded transaction; walking on for a \
                                 second verdict before giving up on it"
                            );
                            // Mirror the verdict into `held` so it survives the caller's budget
                            // cancelling this walk mid-chain (see [`expired_walk_outcome`]).
                            *held.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                Some((key, reason.clone()));
                            Ok(Some((key, reason)))
                        }
                    }
                    // Endpoint-local, but the server answered (full pool, internal error, or a
                    // node-local refusal): reachable, so not demoted, and the transaction's fate
                    // is unknown here; try the next validator.
                    Disposition::TryNext(reason) => {
                        debug!(
                            target: "worker::forward",
                            reason = %reason,
                            "validator RPC did not accept the forwarded transaction; trying next"
                        );
                        Ok(pending_rejection)
                    }
                    // The endpoint gave no verdict: publish the demotion at the failure itself,
                    // not at task end, so batches sealed while this task is still walking already
                    // find the endpoint unusable at admission and the cooldown runs from the
                    // failure (issue #1145). No verdict is no confirmation either: the held
                    // rejection, if any, walks on unchanged.
                    Disposition::Unreachable(reason) => {
                        if unreachable
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .insert(endpoint.clone())
                        {
                            cache
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .unreachable
                                .insert(endpoint.clone(), Instant::now());
                        }
                        debug!(
                            target: "worker::forward",
                            reason = %reason,
                            "validator RPC endpoint was unreachable; trying next"
                        );
                        Ok(pending_rejection)
                    }
                }
            },
        )
        .await
        // End of the chain with a held, unconfirmed rejection: report it rather than erase it.
        .map(|pending_rejection| {
            pending_rejection.map_or(ForwardOutcome::NoEndpointReached, |(_, reason)| {
                ForwardOutcome::Rejected(reason)
            })
        })
        .unwrap_or_else(|verdict| verdict)
}

/// The transaction's outcome once its budget-timed walk resolves.
///
/// `Ok` is the walk's own verdict. `Err` means [`FORWARD_TX_BUDGET`] (or the batch deadline's
/// clamp of it) expired and cancelled the walk mid-chain; a rejection the walk had already
/// banked in `held` still stands - the same rule as reaching the end of the chain unconfirmed
/// (issue #1167) - and only a walk cut short with no verdict at all reports
/// [`ForwardOutcome::NoEndpointReached`]. Without this read-back, a fast rejection followed by
/// unresponsive fallbacks would be tallied as unreached or abandoned, both defined as "never
/// handed to a validator", which would be false.
fn expired_walk_outcome(
    chain: Result<ForwardOutcome, tokio::time::error::Elapsed>,
    held: &Mutex<Option<(BlsPublicKey, String)>>,
) -> ForwardOutcome {
    chain.unwrap_or_else(|_elapsed| {
        held.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .map_or(ForwardOutcome::NoEndpointReached, |(_, reason)| {
                ForwardOutcome::Rejected(reason)
            })
    })
}

/// What to do after one timed attempt to forward a transaction to one validator.
#[derive(Debug, PartialEq, Eq)]
enum Disposition {
    /// The validator accepted the transaction, or it was already in a validator's pool.
    Delivered,
    /// The validator returned a considered rejection of the transaction itself (bad nonce,
    /// invalid, wrong fork). Honest validators would all repeat it, but one validator's
    /// verdict alone is not trusted with the transaction's fate (issue #1167): the chain
    /// holds a first rejection as advisory and walks on until a second validator confirms
    /// or contradicts it.
    Rejected(String),
    /// This endpoint answered but gave no verdict (a transient full pool, an internal error,
    /// or a refusal tied to this validator's own pool contents or admission config); the
    /// transaction may still be accepted by another validator.
    TryNext(String),
    /// This endpoint produced no JSON-RPC verdict (send timeout, transport failure, or a
    /// response that carries none). The transaction may still be accepted by another
    /// validator, and the endpoint is demoted from admission for [`UNREACHABLE_COOLDOWN`]
    /// unless a later delivery through it clears the demotion (issue #1145).
    Unreachable(String),
}

/// Terminal result of forwarding one transaction across the ordered validators.
#[derive(Debug, PartialEq, Eq)]
enum ForwardOutcome {
    /// A validator accepted it (or already had it).
    Delivered,
    /// Two validators independently gave a considered rejection - or one did and no further
    /// validator was reachable to confirm or contradict it - so the chain stopped without
    /// delivery.
    Rejected(String),
    /// No advertised validator gave a verdict (all timed out or were unreachable).
    NoEndpointReached,
}

/// The forwarder's HTTP transport: alloy's reqwest transport with a response-size cap.
///
/// Mirrors the service `alloy_transport_http::Http<Client>` provides, with one difference: the
/// body is pulled chunk by chunk and refused the moment it exceeds
/// [`MAX_FORWARD_RESPONSE_BYTES`], where the stock transport buffers the entire body before
/// any parse (issue #1275). reqwest's `Client` has no response-size knob of its own, so the
/// bound has to live at this layer. An oversized body surfaces as a transport error, which
/// [`classify_error`] maps to [`Disposition::Unreachable`]: the endpoint is demoted like any
/// other endpoint that produced no verdict.
#[derive(Clone, Debug)]
struct CappedHttp {
    /// The HTTP client, alloy's own re-exported `reqwest::Client`, so the TLS backend is
    /// exactly what `RootProvider::new_http` would have used.
    client: Client,
    /// The advertised endpoint this transport dials.
    url: Url,
}

impl CappedHttp {
    /// One JSON-RPC round trip with the response read capped.
    ///
    /// Follows the stock reqwest transport step for step - post the request, keep the body
    /// regardless of status so a JSON-RPC verdict in an error-status body still classifies,
    /// map non-success statuses to HTTP errors, then deserialize - except the body arrives
    /// through [`collect_capped`].
    async fn send_capped(self, req: RequestPacket) -> TransportResult<ResponsePacket> {
        use futures::TryStreamExt as _;
        let resp = self
            .client
            .post(self.url)
            .json(&req)
            .headers(req.headers())
            .send()
            .await
            .map_err(TransportErrorKind::custom)?;
        let status = resp.status();
        let chunks = futures::stream::try_unfold(resp, |mut resp| async move {
            resp.chunk().await.map(|next| next.map(|bytes| (bytes.to_vec(), resp)))
        })
        .map_err(TransportErrorKind::custom);
        let body = collect_capped(chunks).await?;
        match status.is_success() {
            true => serde_json::from_slice(&body)
                .map_err(|err| TransportError::deser_err(err, String::from_utf8_lossy(&body))),
            false => Err(TransportErrorKind::http_error(
                status.as_u16(),
                String::from_utf8_lossy(&body).into_owned(),
            )),
        }
    }
}

/// The tower plumbing that lets [`CappedHttp`] stand where alloy's stock HTTP transport
/// stands. `poll_ready` is always ready for the same reason the stock transport's is:
/// reqwest applies its own backpressure internally.
impl tower::Service<RequestPacket> for CappedHttp {
    type Response = ResponsePacket;
    type Error = TransportError;
    type Future = TransportFut<'static>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        Box::pin(self.clone().send_capped(req))
    }
}

/// Build the provider the forwarder dials one advertised endpoint with.
///
/// Drop-in for `RootProvider::new_http(url)`, routed through [`CappedHttp`] so every response
/// read is bounded by [`MAX_FORWARD_RESPONSE_BYTES`].
fn capped_provider(url: Url) -> RootProvider {
    let is_local = guess_local_url(url.as_str());
    RootProvider::new(RpcClient::new(CappedHttp { client: Client::new(), url }, is_local))
}

/// Accumulate a response body from `chunks`, refusing it once it exceeds
/// [`MAX_FORWARD_RESPONSE_BYTES`].
///
/// The refusal aborts the fold, which drops the stream, which abandons the connection: an
/// oversized body is not just absent from the result, it stops being read.
async fn collect_capped(
    chunks: impl futures::Stream<Item = TransportResult<Vec<u8>>>,
) -> TransportResult<Vec<u8>> {
    use futures::TryStreamExt as _;
    chunks
        .try_fold(Vec::<u8>::new(), |buf, chunk| async move {
            let total = buf.len().saturating_add(chunk.len());
            (total <= MAX_FORWARD_RESPONSE_BYTES)
                // Append in place: rebuilding the accumulator per chunk would be quadratic in
                // chunk count, and the chunking is remote-controlled too.
                .then(|| {
                    let mut buf = buf;
                    buf.extend_from_slice(&chunk);
                    buf
                })
                .ok_or_else(|| {
                    TransportErrorKind::custom_str(&format!(
                        "response body exceeds the {MAX_FORWARD_RESPONSE_BYTES}-byte forward cap"
                    ))
                })
        })
        .await
}

/// Clamp one remote-controlled reason string to [`MAX_REASON_BYTES`].
///
/// The cut lands on a `char` boundary at or below the cap, and a clamped reason says how much
/// it dropped, so a truncated log line reads as truncated instead of as the whole message.
fn clamp_reason(message: String) -> String {
    match message.len() <= MAX_REASON_BYTES {
        true => message,
        false => {
            let cut = (0..=MAX_REASON_BYTES)
                .rev()
                .find(|idx| message.is_char_boundary(*idx))
                .unwrap_or(0);
            let kept = message.get(..cut).unwrap_or_default();
            format!("{kept} [truncated {} of {} bytes]", message.len() - cut, message.len())
        }
    }
}

/// Classify a failed `send_raw_transaction` by whether the server returned a JSON-RPC error (a
/// verdict about the transaction) or the transport failed (an endpoint problem).
///
/// Every reason built here is clamped to [`MAX_REASON_BYTES`]: several of them embed
/// remote-controlled bytes (a JSON-RPC error message, an HTTP error's response body), and this
/// is the one choke point every log site and the held-rejection mirror sit downstream of
/// (issue #1275).
fn classify_error(err: RpcError<TransportErrorKind>) -> Disposition {
    match err {
        // The server returned a JSON-RPC verdict: classify it by code and message.
        RpcError::ErrorResp(payload) => {
            classify_server_error(payload.code, payload.message.to_string())
        }
        // Local faults: the request could not even be built or used. They say nothing about
        // the endpoint, so try the next validator without demoting this one.
        RpcError::SerError(err) => Disposition::TryNext(clamp_reason(err.to_string())),
        RpcError::LocalUsageError(err) => Disposition::TryNext(clamp_reason(err.to_string())),
        // No usable verdict from the remote side: a transport failure, a response that does
        // not parse, an empty response, or a capability the endpoint lacks. All demote.
        RpcError::Transport(err) => Disposition::Unreachable(clamp_reason(err.to_string())),
        RpcError::DeserError { err, .. } => Disposition::Unreachable(clamp_reason(err.to_string())),
        RpcError::NullResp => Disposition::Unreachable("null response".to_string()),
        RpcError::UnsupportedFeature(feature) => {
            Disposition::Unreachable(clamp_reason(feature.to_string()))
        }
    }
}

/// Classify a server-side JSON-RPC rejection of a forwarded transaction.
///
/// The message is remote-controlled, so it is clamped before it can reach a log or the held
/// rejection (issue #1275). Clamping before the needle checks below is safe for honest
/// servers: every message the needles match is far shorter than the cap.
fn classify_server_error(code: i64, message: String) -> Disposition {
    let message = clamp_reason(message);
    let lowered = message.to_ascii_lowercase();
    match () {
        // A full pool or an internal error is validator-local; another validator may accept it.
        () if TRANSIENT_RPC_CODES.contains(&code) => Disposition::TryNext(message),
        // Already in a validator's pool: the transaction is delivered.
        () if lowered.contains(ALREADY_KNOWN_MESSAGE) => Disposition::Delivered,
        // A refusal tied to this validator's own pool contents or admission config, not a
        // verdict: the next validator may accept the same bytes. Scoped to `-32000` so the
        // substrings cannot hijack a verdict that arrives under any other code.
        () if code == INVALID_INPUT_RPC_CODE
            && NODE_LOCAL_MESSAGES.iter().any(|needle| lowered.contains(needle)) =>
        {
            Disposition::TryNext(message)
        }
        // Every validator shares consensus state, so a considered rejection repeats everywhere.
        () => Disposition::Rejected(format!("code {code}: {message}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use rand::{rngs::StdRng, SeedableRng};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tn_types::{BlsKeypair, TaskManager};

    /// A reason at the cap passes through untouched.
    #[test]
    fn clamp_reason_keeps_short_reasons() {
        let reason = "a".repeat(MAX_REASON_BYTES);
        assert_eq!(clamp_reason(reason.clone()), reason);
    }

    /// An oversized reason is cut on a `char` boundary and says how much it dropped.
    #[test]
    fn clamp_reason_truncates_on_a_char_boundary() {
        // Multi-byte scalars straddle the cap, so a byte-index cut would split one.
        let reason = format!("{}XYZW", "\u{1d54f}".repeat(MAX_REASON_BYTES / 4));
        let clamped = clamp_reason(reason.clone());
        let kept = clamped.split(" [truncated").next().unwrap_or_default();
        assert!(kept.len() <= MAX_REASON_BYTES);
        assert!(reason.starts_with(kept));
        assert!(clamped.contains("[truncated 4 of"));
    }

    /// A remote rejection message reaches the `Rejected` reason bounded, not verbatim.
    #[test]
    fn classify_server_error_bounds_remote_reasons() {
        let huge = "x".repeat(1024 * 1024);
        let reason = match classify_server_error(3, huge) {
            Disposition::Rejected(reason) => reason,
            Disposition::Delivered | Disposition::TryNext(_) | Disposition::Unreachable(_) => {
                String::new()
            }
        };
        assert!(!reason.is_empty());
        assert!(reason.len() <= MAX_REASON_BYTES + 64, "reason kept {} bytes", reason.len());
    }

    /// An HTTP-error transport reason, whose `Display` embeds the response body, is bounded.
    #[test]
    fn classify_error_bounds_transport_reasons() {
        let err = TransportErrorKind::http_error(500, "x".repeat(1024 * 1024));
        let reason = match classify_error(err) {
            Disposition::Unreachable(reason) => reason,
            Disposition::Delivered | Disposition::Rejected(_) | Disposition::TryNext(_) => {
                String::new()
            }
        };
        assert!(!reason.is_empty());
        assert!(reason.len() <= MAX_REASON_BYTES + 64, "reason kept {} bytes", reason.len());
    }

    /// Chunks up to the cap accumulate; the first chunk that would push past it aborts.
    #[tokio::test]
    async fn collect_capped_bounds_the_body() {
        let chunk = || Ok::<_, TransportError>(vec![0u8; 16 * 1024]);
        let under = futures::stream::iter((0..4).map(|_| chunk()));
        let body = collect_capped(under).await;
        assert_eq!(body.map(|body| body.len()).unwrap_or(0), MAX_FORWARD_RESPONSE_BYTES);

        let over = futures::stream::iter((0..5).map(|_| chunk()));
        assert!(collect_capped(over).await.is_err());
    }

    /// Serve one canned HTTP response on a loopback socket, then drain the connection.
    ///
    /// Draining until the client hangs up (instead of closing right after the write) keeps
    /// the socket alive while reqwest finishes sending the request, so the test never races
    /// a reset against the response read.
    async fn serve_one_response(response: String) -> eyre::Result<Url> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        tokio::spawn(async move {
            let served = async {
                use tokio::io::AsyncWriteExt as _;
                let (mut socket, _) = listener.accept().await?;
                let (mut reader, mut writer) = socket.split();
                writer.write_all(response.as_bytes()).await?;
                tokio::io::copy(&mut reader, &mut tokio::io::sink()).await
            };
            served.await.ok();
        });
        Ok(format!("http://{addr}/").parse()?)
    }

    /// Wrap a JSON-RPC body in a minimal HTTP/1.1 response.
    fn http_response(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
    }

    /// The capped transport is a working JSON-RPC transport for responses under the cap.
    #[tokio::test]
    async fn capped_provider_round_trips_a_small_response() -> eyre::Result<()> {
        let body = r#"{"jsonrpc":"2.0","id":0,"result":"0x10"}"#;
        let url = serve_one_response(http_response("200 OK", body)).await?;
        let number = capped_provider(url).get_block_number().await?;
        assert_eq!(number, 0x10);
        Ok(())
    }

    /// A JSON-RPC error object in a small response still classifies: the cap changes the
    /// outcome only for oversized bodies, not for verdicts.
    #[tokio::test]
    async fn capped_provider_preserves_rpc_error_verdicts() -> eyre::Result<()> {
        let body = r#"{"jsonrpc":"2.0","id":0,"error":{"code":-32000,"message":"already known"}}"#;
        let url = serve_one_response(http_response("200 OK", body)).await?;
        let disposition =
            capped_provider(url).get_block_number().await.map_or_else(classify_error, |number| {
                Disposition::TryNext(format!("unexpected success: {number}"))
            });
        assert_eq!(disposition, Disposition::Delivered);
        Ok(())
    }

    /// A response past the cap is refused as a transport error instead of buffered whole.
    #[tokio::test]
    async fn capped_provider_refuses_an_oversized_response() -> eyre::Result<()> {
        let body = "x".repeat(MAX_FORWARD_RESPONSE_BYTES + 1);
        let url = serve_one_response(http_response("200 OK", &body)).await?;
        let outcome = capped_provider(url).get_block_number().await;
        let reason = outcome.err().map(|err| err.to_string()).unwrap_or_default();
        assert!(reason.contains("forward cap"), "unexpected outcome: {reason}");
        Ok(())
    }

    /// A forwarder under the shipped default: only public hosts may be dialed.
    fn test_forwarder() -> WorkerRpcForwarder {
        test_forwarder_with(ForwardTargetPolicy::PublicOnly)
    }

    fn test_forwarder_with(policy: ForwardTargetPolicy) -> WorkerRpcForwarder {
        // Leak the manager: a dropped TaskManager latches its one-shot shutdown,
        // which would cancel any task later spawned through the spawner.
        WorkerRpcForwarder::new(
            Box::leak(Box::new(TaskManager::default())).get_spawner(),
            policy,
            None,
        )
    }

    /// [`MAX_CONCURRENT_FORWARDS`] as the width the semaphore API wants.
    fn max_permits() -> u32 {
        u32::try_from(MAX_CONCURRENT_FORWARDS).unwrap_or(u32::MAX)
    }

    /// An endpoint that completes the TCP handshake and then never answers.
    ///
    /// This is the condition the batch budget exists for: a refused connection fails fast and
    /// costs nothing, so only an endpoint that accepts and hangs makes a forward spend its
    /// budget. Accepted connections are held for the life of the process; the collect never
    /// finishes, which is what keeps them open. `map_while` is what ends the accept loop on a
    /// persistent accept failure (say EMFILE once enough sockets are held): `incoming()` yields
    /// `Err` repeatedly in that state, and a plain `flatten` would swallow every one and leave
    /// this thread busy-spinning for the rest of the process.
    fn blackhole_endpoint() -> eyre::Result<String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        std::thread::spawn(move || {
            let _held: Vec<std::net::TcpStream> =
                listener.incoming().map_while(Result::ok).collect();
        });
        Ok(format!("http://{addr}"))
    }

    /// JSON-RPC success payload for a scripted endpoint: a well-formed
    /// `eth_sendRawTransaction` result (a transaction hash), which alloy deserializes as a
    /// delivery.
    const DELIVERY_BODY: &str = r#"{"jsonrpc":"2.0","id":__ID__,"result":"0x0000000000000000000000000000000000000000000000000000000000000000"}"#;

    /// JSON-RPC considered-rejection payload: reth's shape for a verdict on the transaction
    /// itself, which `classify_server_error` maps to [`Disposition::Rejected`].
    const REJECTION_BODY: &str =
        r#"{"jsonrpc":"2.0","id":__ID__,"error":{"code":-32000,"message":"nonce too low"}}"#;

    /// JSON-RPC transient-error payload: a full pool is validator-local, so
    /// `classify_server_error` maps it to [`Disposition::TryNext`].
    const TRANSIENT_BODY: &str =
        r#"{"jsonrpc":"2.0","id":__ID__,"error":{"code":-32003,"message":"txpool is full"}}"#;

    /// The `content-length` a request's head promises, `None` when the header is absent.
    fn content_length(head: &str) -> Option<usize> {
        head.lines().find_map(|line| {
            line.split_once(':').and_then(|(name, value)| {
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
        })
    }

    /// The JSON-RPC id of `request`, `"0"` if none is found.
    ///
    /// A canned response must echo the request's id, or alloy treats it as an unsolicited
    /// message and fails the call as a transport error, which would turn every scripted
    /// verdict into [`Disposition::TryNext`].
    fn request_id(request: &str) -> String {
        request
            .split_once("\"id\":")
            .map(|(_, rest)| {
                rest.trim_start().chars().take_while(char::is_ascii_digit).collect::<String>()
            })
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| "0".to_string())
    }

    /// Read one HTTP request off `stream`: headers, then as much body as `content-length`
    /// promises. Stops with whatever has arrived on connection close or a read error.
    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        use std::{io::Read as _, ops::ControlFlow};
        // At most 64 KiB of request: far above what one `eth_sendRawTransaction` POST needs,
        // and a hard stop so a runaway peer cannot spin this fixture thread forever.
        let outcome = (0_usize..64).try_fold(Vec::new(), |raw, _read_attempt| {
            let mut chunk = [0_u8; 1024];
            stream.read(&mut chunk).ok().filter(|count| *count > 0).map_or(
                // EOF or a read error: answer with whatever has arrived.
                ControlFlow::Break(raw.clone()),
                |count| {
                    let grown = [raw.as_slice(), &chunk[..count]].concat();
                    let text = String::from_utf8_lossy(&grown);
                    let complete = text.split_once("\r\n\r\n").is_some_and(|(head, body)| {
                        content_length(head).is_none_or(|length| body.len() >= length)
                    });
                    if complete {
                        ControlFlow::Break(grown)
                    } else {
                        ControlFlow::Continue(grown)
                    }
                },
            )
        });
        let raw = match outcome {
            ControlFlow::Break(raw) | ControlFlow::Continue(raw) => raw,
        };
        String::from_utf8_lossy(&raw).into_owned()
    }

    /// Serve one HTTP request on `stream` with the canned JSON-RPC `payload`, echoing the
    /// request's id, then close the connection.
    fn serve_one(mut stream: std::net::TcpStream, payload: &str) -> std::io::Result<()> {
        use std::io::Write as _;
        let request = read_http_request(&mut stream);
        let body = payload.replace("__ID__", &request_id(&request));
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: \
             {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes())
    }

    /// An endpoint that answers every request with the canned JSON-RPC `payload` and counts
    /// the requests it serves.
    ///
    /// Every response closes its connection, so alloy re-dials per call and `hits` counts
    /// exactly the `eth_sendRawTransaction` attempts this validator's endpoint received.
    fn scripted_endpoint(payload: &'static str, hits: Arc<AtomicUsize>) -> eyre::Result<String> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        std::thread::spawn(move || {
            listener.incoming().map_while(Result::ok).for_each(|stream| {
                hits.fetch_add(1, Ordering::SeqCst);
                let _ = serve_one(stream, payload);
            });
        });
        Ok(format!("http://{addr}"))
    }

    /// The provider-map shape the walk takes: each validator key with its endpoint URL and
    /// provider attached, as [`WorkerRpcForwarder::cached_providers`] builds it.
    type ScriptedProviders = BTreeMap<BlsPublicKey, (String, RootProvider)>;

    /// Providers for `endpoints`, keyed `test_key(1)`, `test_key(2)`, ... in order, plus the
    /// matching walk order for [`walk_fallback_chain`].
    fn scripted_chain(
        endpoints: &[String],
    ) -> eyre::Result<(Vec<BlsPublicKey>, ScriptedProviders)> {
        let keys: Vec<BlsPublicKey> =
            (1_u8..).zip(endpoints).map(|(seed, _)| test_key(seed)).collect();
        let providers: Result<BTreeMap<_, _>, _> = keys
            .iter()
            .zip(endpoints)
            .map(|(key, url)| {
                url.parse().map(|parsed: Url| (*key, (parsed.to_string(), capped_provider(parsed))))
            })
            .collect();
        providers.map(|providers| (keys, providers)).map_err(Into::into)
    }

    /// An endpoint that answers every `eth_sendRawTransaction` with success and counts its
    /// hits: [`scripted_endpoint`] with the delivery payload, the shape the rotation test
    /// reads, since which endpoint absorbed a forward is otherwise invisible from outside
    /// the task.
    fn counting_ok_endpoint() -> eyre::Result<(String, Arc<AtomicUsize>)> {
        let hits = Arc::new(AtomicUsize::new(0));
        scripted_endpoint(DELIVERY_BODY, Arc::clone(&hits)).map(|url| (url, hits))
    }

    /// One recorder capture. [`metrics_util::debugging::Snapshotter::snapshot`] drains the
    /// accumulated values with it, so each test takes exactly one snapshot after its recorded
    /// scope ends and every lookup below reads that capture; a second snapshot would read
    /// zeros.
    type Snapshot = Vec<(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )>;

    /// Read one counter out of a recorder snapshot, `None` if the series is not registered.
    fn counter(snapshot: &Snapshot, name: &str) -> Option<u64> {
        snapshot.iter().find_map(|(key, _, _, value)| {
            (key.key().name() == name).then_some(value).and_then(|value| match value {
                DebugValue::Counter(count) => Some(*count),
                DebugValue::Gauge(_) | DebugValue::Histogram(_) => None,
            })
        })
    }

    /// The dropped-transactions counter for one `reason`, `None` if that series is absent.
    ///
    /// [`counter`] cannot serve here: every reason shares the metric name, so a name-only
    /// lookup returns whichever series the snapshot happens to list first.
    fn dropped_counter(snapshot: &Snapshot, reason: &str) -> Option<u64> {
        snapshot.iter().find_map(|(key, _, _, value)| {
            (key.key().name() == "tn_reth.forwarded_txns_dropped_total"
                && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason))
            .then_some(value)
            .and_then(|value| match value {
                DebugValue::Counter(count) => Some(*count),
                DebugValue::Gauge(_) | DebugValue::Histogram(_) => None,
            })
        })
    }

    fn test_key(seed: u8) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
    }

    fn test_rpc(url: &str) -> eyre::Result<RpcInfo> {
        Ok(RpcInfo { http: url.parse()?, ws: None })
    }

    fn cached_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder
            .cache
            .lock()
            .map(|cache| cache.providers.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn refused_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder
            .cache
            .lock()
            .map(|cache| cache.refused.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn demoted_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder
            .cache
            .lock()
            .map(|cache| cache.unreachable.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Refuse `url` under the default policy, or fail with the URL that slipped through.
    fn refusal(url: &str) -> eyre::Result<RefusedTarget> {
        let parsed: Url = url.parse()?;
        ForwardTargetPolicy::PublicOnly
            .check(&parsed)
            .err()
            .ok_or_else(|| eyre::eyre!("{url} was accepted as a dial target but must be refused"))
    }

    #[test]
    fn cached_providers_resolves_every_advertised_endpoint() -> eyre::Result<()> {
        // Public hosts: the loopback fixtures this test used before are refused by the default
        // policy, and cache mechanics are what is under test here.
        let forwarder = test_forwarder();
        let rpcs = vec![
            (test_key(1), test_rpc("http://validator-one.example.com:8545")?),
            (test_key(2), test_rpc("http://validator-two.example.com:8546")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        assert_eq!(resolved.len(), 2);
        assert_eq!(cached_urls(&forwarder).len(), 2);
        Ok(())
    }

    #[test]
    fn cached_providers_reuses_cache_and_evicts_stale_endpoints() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let live = (test_key(1), test_rpc("http://validator-one.example.com:8545")?);
        let stale = (test_key(2), test_rpc("http://validator-two.example.com:8546")?);
        let first = forwarder.cached_providers(&[live.clone(), stale]);
        assert_eq!(first.len(), 2);

        // A repeat advertisement resolves from the cache without growing it; the endpoint
        // that is no longer advertised is evicted.
        let second = forwarder.cached_providers(&[live]);

        assert_eq!(second.len(), 1);
        assert_eq!(
            cached_urls(&forwarder),
            vec!["http://validator-one.example.com:8545/".to_string()]
        );
        Ok(())
    }

    /// The per-transaction budget is clamped to what is left of the batch budget, and runs out
    /// with it. This is what makes a task's lifetime a constant instead of a per-batch quantity.
    #[test]
    fn next_txn_budget_clamps_to_the_batch_deadline_then_ends_the_batch() {
        let now = Instant::now();

        // A whole batch budget left: a transaction still gets only its own budget.
        assert_eq!(next_txn_budget(now + FORWARD_BATCH_BUDGET, now), Some(FORWARD_TX_BUDGET));
        // Less than one transaction's budget left: clamped, so the last transaction a batch
        // attempts cannot carry its task past the batch deadline.
        let sliver = FORWARD_TX_BUDGET / 3;
        assert_eq!(next_txn_budget(now + sliver, now), Some(sliver));
        // At the deadline, and past it: no budget, which is how the batch stops.
        assert_eq!(next_txn_budget(now, now), None);
        assert_eq!(next_txn_budget(now, now + FORWARD_TX_BUDGET), None);
    }

    /// With every permit in flight, an arriving batch is dropped at the door.
    ///
    /// The bound has to be on live forwards rather than on arrival rate, because arrival rate is
    /// set by inbound transaction volume and completion rate by remote endpoints. Neither is
    /// this node's to control, so nothing couples them but this.
    #[test]
    fn forward_txns_sheds_a_batch_when_every_permit_is_in_flight() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        // A public-host fixture: this test never dials (no permit is free), and the default
        // policy must admit the endpoint so the batch reaches admission at all.
        let rpcs = vec![(test_key(1), test_rpc("http://validator-one.example.com:8545")?)];
        // Stand in for `MAX_CONCURRENT_FORWARDS` forwards already running.
        let _in_flight =
            Arc::clone(&forwarder.forwards_in_flight).try_acquire_many_owned(max_permits())?;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs);
        });
        let snapshot = snapshotter.snapshot().into_vec();

        // No permit was free, so none was taken, and the shed is visible as a counter rather
        // than only as a log line - at batch grain and, since #1133, at transaction grain
        // against the queued denominator.
        assert_eq!(forwarder.forwards_in_flight.available_permits(), 0);
        assert_eq!(counter(&snapshot, "tn_reth.forwarded_batches_shed_total"), Some(1));
        assert_eq!(counter(&snapshot, "tn_reth.forwarded_txns_queued_total"), Some(1));
        assert_eq!(dropped_counter(&snapshot, "batch_shed"), Some(1));
        Ok(())
    }

    /// A batch that reaches the forwarder with an empty committee is dropped without a warn by
    /// design; the reason-labeled counter is what keeps the loss visible (issue #1133).
    #[test]
    fn forward_txns_counts_a_batch_with_no_committee_as_dropped() -> eyre::Result<()> {
        let rpcs = vec![(test_key(1), test_rpc("http://validator-one.example.com:8545")?)];

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // Built inside the recorder scope so `ForwarderMetrics::init` registers every series
        // with this recorder; the zero assertions below read those registrations.
        let forwarder = metrics::with_local_recorder(&recorder, || {
            let forwarder = test_forwarder();
            forwarder.forward_txns(vec![vec![0_u8; 32], vec![1_u8; 32]], vec![], rpcs);
            forwarder
        });
        let snapshot = snapshotter.snapshot().into_vec();

        assert_eq!(counter(&snapshot, "tn_reth.forwarded_txns_queued_total"), Some(2));
        assert_eq!(dropped_counter(&snapshot, "empty_committee"), Some(2));
        assert_eq!(dropped_counter(&snapshot, "no_usable_endpoint"), Some(0));
        // Dropped at the routing check: no forward task was ever spawned.
        assert_eq!(forwarder.forwards_in_flight.available_permits(), MAX_CONCURRENT_FORWARDS);
        Ok(())
    }

    /// A committee is present but nobody advertised an endpoint: the same routing dead end
    /// as an empty committee, counted under the same reason.
    #[test]
    fn forward_txns_counts_a_batch_with_no_advertised_endpoint_as_dropped() -> eyre::Result<()> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let forwarder = metrics::with_local_recorder(&recorder, || {
            let forwarder = test_forwarder();
            forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], vec![]);
            forwarder
        });
        let snapshot = snapshotter.snapshot().into_vec();

        assert_eq!(counter(&snapshot, "tn_reth.forwarded_txns_queued_total"), Some(1));
        assert_eq!(dropped_counter(&snapshot, "empty_committee"), Some(1));
        assert_eq!(dropped_counter(&snapshot, "no_usable_endpoint"), Some(0));
        assert_eq!(forwarder.forwards_in_flight.available_permits(), MAX_CONCURRENT_FORWARDS);
        Ok(())
    }

    /// Endpoints were advertised but the policy refused every one: the batch is dropped and
    /// the drop is counted under its own reason, apart from the empty-committee no-op.
    #[test]
    fn forward_txns_counts_a_batch_with_no_usable_endpoint_as_dropped() -> eyre::Result<()> {
        // The shipped default policy refuses the loopback advertisement, so the committee is
        // present but no endpoint resolves to a provider.
        let rpcs = vec![(test_key(1), test_rpc("http://127.0.0.1:8545")?)];

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let forwarder = metrics::with_local_recorder(&recorder, || {
            let forwarder = test_forwarder();
            forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs);
            forwarder
        });
        let snapshot = snapshotter.snapshot().into_vec();

        assert_eq!(counter(&snapshot, "tn_reth.forwarded_txns_queued_total"), Some(1));
        assert_eq!(dropped_counter(&snapshot, "no_usable_endpoint"), Some(1));
        assert_eq!(dropped_counter(&snapshot, "empty_committee"), Some(0));
        assert_eq!(forwarder.forwards_in_flight.available_permits(), MAX_CONCURRENT_FORWARDS);
        Ok(())
    }

    /// A forward takes exactly one permit before it is spawned and returns it when the task
    /// ends, so capacity tracks forwards that are actually running.
    #[tokio::test]
    async fn forward_txns_holds_one_permit_for_the_life_of_the_task() -> eyre::Result<()> {
        // A port with nothing behind it: the forward fails fast, so the task ends promptly.
        let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = closed.local_addr()?;
        drop(closed);

        // The manager outlives the forward: dropping it would shut the spawned task down and
        // release the permit for the wrong reason.
        let manager = TaskManager::default();
        // `AllowPrivate`: the fixture endpoint is a loopback socket, which the shipped default
        // policy would refuse before the admission control under test was ever reached.
        let forwarder =
            WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::AllowPrivate, None);
        let rpcs = vec![(test_key(1), test_rpc(&format!("http://{addr}"))?)];
        forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs);

        // The spawned task has not been polled yet, so the permit it holds is observable here.
        assert_eq!(forwarder.forwards_in_flight.available_permits(), MAX_CONCURRENT_FORWARDS - 1);

        // Draining every permit can only complete once the task has dropped the one it took.
        let drained = timeout(
            Duration::from_secs(30),
            Arc::clone(&forwarder.forwards_in_flight).acquire_many_owned(max_permits()),
        )
        .await;
        assert!(drained.is_ok(), "forward task never released its permit");
        Ok(())
    }

    /// A batch stops at [`FORWARD_BATCH_BUDGET`] instead of paying one budget per transaction.
    ///
    /// Every advertised slot hides behind its own blackholed endpoint, so each transaction
    /// burns a fresh fallback chain of accept-and-hang sends. (Distinct endpoints matter
    /// since issue #1145: demotion retires an endpoint after its first hung send, so a single
    /// shared endpoint would cost one send timeout for the whole batch and the ceiling under
    /// test would never bind.) Twenty transactions is five minutes of work, and without a
    /// per-task ceiling the task would live for all of it with the batch's bytes pinned. Time
    /// is virtual here, so the test measures that shape without waiting for it.
    #[tokio::test(start_paused = true)]
    async fn forward_txns_abandons_the_rest_of_a_batch_at_the_batch_budget() -> eyre::Result<()> {
        let manager = TaskManager::default();
        // `AllowPrivate`: the fixture endpoints are loopback sockets, which the shipped default
        // policy would refuse before the admission control under test was ever reached.
        let forwarder =
            WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::AllowPrivate, None);
        // Enough distinct endpoints that demotion cannot drain the pool of undialed targets
        // before the batch budget binds: the budget covers eight full transaction chains, and
        // each chain demotes at most three endpoints.
        let slots: Vec<BlsPublicKey> = (1..=30_u8).map(test_key).collect();
        let rpcs = slots
            .iter()
            .map(|key| {
                blackhole_endpoint().and_then(|endpoint| test_rpc(&endpoint).map(|rpc| (*key, rpc)))
            })
            .collect::<eyre::Result<Vec<_>>>()?;

        let start = Instant::now();
        forwarder.forward_txns(vec![vec![0_u8; 32]; 20], slots, rpcs);
        let drained = timeout(
            Duration::from_secs(3600),
            Arc::clone(&forwarder.forwards_in_flight).acquire_many_owned(max_permits()),
        )
        .await;
        assert!(drained.is_ok(), "forward task was still running an hour in");

        // Positive control: the budget has to be what stopped the batch. If the endpoint had
        // failed fast instead of hanging, the batch would finish in no time and the ceiling
        // below would hold vacuously.
        let elapsed = start.elapsed();
        assert!(
            elapsed >= FORWARD_BATCH_BUDGET,
            "batch finished in {elapsed:?}, so the budget was never the binding limit"
        );
        assert!(
            elapsed <= FORWARD_BATCH_BUDGET + FORWARD_TX_BUDGET,
            "batch ran {elapsed:?}, past its {FORWARD_BATCH_BUDGET:?} budget"
        );
        Ok(())
    }

    /// A first considered rejection no longer decides a transaction's fate on its own: a
    /// later delivery overrides it, and the override is visible as a counter (issue #1167).
    ///
    /// This is the issue's attack: a byzantine owning validator fabricates a rejection to
    /// censor the sender's account. The fallback validator must still be consulted, and its
    /// delivery must win.
    #[test]
    fn a_lone_rejection_is_overridden_by_a_later_delivery() -> eyre::Result<()> {
        let owner_hits = Arc::new(AtomicUsize::new(0));
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let endpoints = vec![
            scripted_endpoint(REJECTION_BODY, Arc::clone(&owner_hits))?,
            scripted_endpoint(DELIVERY_BODY, Arc::clone(&fallback_hits))?,
        ];
        let (keys, providers) = scripted_chain(&endpoints)?;

        // A local recorder, with the walk block_on-driven inside its scope: the recorder is
        // thread-local, so the override counter must be incremented on this thread.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let outcome = metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread().enable_all().build().map(|runtime| {
                runtime.block_on(walk_fallback_chain(
                    &[0_u8; 32],
                    keys.iter().cloned(),
                    &providers,
                    &Mutex::new(None),
                    &Mutex::new(BTreeSet::new()),
                    &Mutex::new(EndpointCache::default()),
                ))
            })
        })?;
        let snapshot = snapshotter.snapshot().into_vec();

        assert_eq!(outcome, ForwardOutcome::Delivered);
        assert_eq!(owner_hits.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 1);
        assert_eq!(counter(&snapshot, "tn_reth.forwarded_rejections_overridden_total"), Some(1));
        Ok(())
    }

    /// Two independent considered rejections end the walk without delivery: the second
    /// verdict is the confirmation, and no validator after it is dialed.
    ///
    /// The polarity control for the override test: an accepting validator sits third in the
    /// order, and a confirmed rejection must stop the walk before reaching it.
    #[tokio::test]
    async fn a_second_rejection_confirms_and_stops_the_walk() -> eyre::Result<()> {
        let untouched_hits = Arc::new(AtomicUsize::new(0));
        let endpoints = vec![
            scripted_endpoint(REJECTION_BODY, Arc::new(AtomicUsize::new(0)))?,
            scripted_endpoint(REJECTION_BODY, Arc::new(AtomicUsize::new(0)))?,
            scripted_endpoint(DELIVERY_BODY, Arc::clone(&untouched_hits))?,
        ];
        let (keys, providers) = scripted_chain(&endpoints)?;

        let outcome = walk_fallback_chain(
            &[0_u8; 32],
            keys.iter().cloned(),
            &providers,
            &Mutex::new(None),
            &Mutex::new(BTreeSet::new()),
            &Mutex::new(EndpointCache::default()),
        )
        .await;

        assert_eq!(outcome, ForwardOutcome::Rejected("code -32000: nonce too low".to_string()));
        assert_eq!(untouched_hits.load(Ordering::SeqCst), 0);
        Ok(())
    }

    /// One rejection with nothing after it but an unreachable endpoint: the sole verdict
    /// stands, and the endpoint-local failure neither confirms nor erases it.
    #[tokio::test]
    async fn a_sole_unconfirmed_rejection_still_stands() -> eyre::Result<()> {
        // A port with nothing behind it: the fallback fails fast as an endpoint problem.
        let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = closed.local_addr()?;
        drop(closed);
        let endpoints = vec![
            scripted_endpoint(REJECTION_BODY, Arc::new(AtomicUsize::new(0)))?,
            format!("http://{addr}"),
        ];
        let (keys, providers) = scripted_chain(&endpoints)?;

        let held = Mutex::new(None);
        let outcome = walk_fallback_chain(
            &[0_u8; 32],
            keys.iter().cloned(),
            &providers,
            &held,
            &Mutex::new(BTreeSet::new()),
            &Mutex::new(EndpointCache::default()),
        )
        .await;

        assert_eq!(outcome, ForwardOutcome::Rejected("code -32000: nonce too low".to_string()));
        // The verdict was also banked for the caller: a budget cutting this walk short would
        // read the same rejection back instead of erasing it.
        assert!(held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|(_, reason)| reason == "code -32000: nonce too low"));
        Ok(())
    }

    /// A held advisory rejection survives the per-transaction budget cutting the walk short:
    /// the caller reads the banked verdict back instead of tallying the transaction as
    /// unreached or abandoned, both of which claim no validator was ever handed it.
    #[tokio::test]
    async fn a_budget_cut_walk_still_reports_the_held_rejection() -> eyre::Result<()> {
        let held = Mutex::new(Some((test_key(1), "code -32000: nonce too low".to_string())));
        let cut = timeout(Duration::from_millis(0), std::future::pending::<ForwardOutcome>()).await;
        eyre::ensure!(cut.is_err(), "a zero-budget timeout must elapse");

        let outcome = expired_walk_outcome(cut, &held);

        assert_eq!(outcome, ForwardOutcome::Rejected("code -32000: nonce too low".to_string()));
        // The verdict is consumed on read, and the polarity control holds: with nothing
        // banked, a cut walk reports that no endpoint was reached.
        let cut = timeout(Duration::from_millis(0), std::future::pending::<ForwardOutcome>()).await;
        eyre::ensure!(cut.is_err(), "a zero-budget timeout must elapse");
        assert_eq!(expired_walk_outcome(cut, &held), ForwardOutcome::NoEndpointReached);
        Ok(())
    }

    /// A transient validator-local error between the rejection and the delivery is not a
    /// verdict: it must neither confirm the held rejection nor stop the walk from reaching
    /// the validator that accepts.
    #[tokio::test]
    async fn a_transient_error_does_not_confirm_a_rejection() -> eyre::Result<()> {
        let endpoints = vec![
            scripted_endpoint(REJECTION_BODY, Arc::new(AtomicUsize::new(0)))?,
            scripted_endpoint(TRANSIENT_BODY, Arc::new(AtomicUsize::new(0)))?,
            scripted_endpoint(DELIVERY_BODY, Arc::new(AtomicUsize::new(0)))?,
        ];
        let (keys, providers) = scripted_chain(&endpoints)?;

        let outcome = walk_fallback_chain(
            &[0_u8; 32],
            keys.iter().cloned(),
            &providers,
            &Mutex::new(None),
            &Mutex::new(BTreeSet::new()),
            &Mutex::new(EndpointCache::default()),
        )
        .await;

        assert_eq!(outcome, ForwardOutcome::Delivered);
        Ok(())
    }

    #[test]
    fn cached_providers_refuses_non_public_endpoints_but_keeps_public_ones() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        // The public endpoint shares the slice with the refused ones, so a reject-everything
        // regression cannot pass this assertion.
        let public = (test_key(1), test_rpc("http://validator.example.com:8545")?);
        let rpcs = vec![
            public.clone(),
            (test_key(2), test_rpc("http://169.254.169.254/latest/meta-data/")?),
            (test_key(3), test_rpc("http://127.0.0.1:8545")?),
            (test_key(4), test_rpc("http://10.0.0.1:8545")?),
            (test_key(5), test_rpc("http://192.168.1.1:8545")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        // Only the public validator has a provider, and only its URL is cached.
        assert_eq!(resolved.keys().cloned().collect::<Vec<_>>(), vec![public.0]);
        assert_eq!(cached_urls(&forwarder), vec!["http://validator.example.com:8545/".to_string()]);
        // Every refused endpoint is recorded once so its warning is not repeated per seal.
        assert_eq!(refused_urls(&forwarder).len(), 4);
        Ok(())
    }

    #[test]
    fn cached_providers_never_caches_a_refused_endpoint() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let rpcs = vec![(test_key(1), test_rpc("http://10.0.0.1:8545")?)];

        // A refused endpoint is never cached, so a later seal re-runs the check rather than
        // serving a provider the policy already rejected.
        assert!(forwarder.cached_providers(&rpcs).is_empty());
        assert!(forwarder.cached_providers(&rpcs).is_empty());
        assert!(cached_urls(&forwarder).is_empty());
        // ... and the refusal is remembered once, so the operator warning does not repeat.
        assert_eq!(refused_urls(&forwarder), vec!["http://10.0.0.1:8545/".to_string()]);

        // Dropping the advertisement clears the record, so a re-advertisement warns again.
        assert!(forwarder.cached_providers(&[]).is_empty());
        assert!(refused_urls(&forwarder).is_empty());
        Ok(())
    }

    #[test]
    fn cached_providers_dials_private_endpoints_under_the_local_opt_in() -> eyre::Result<()> {
        // Single-host and docker-compose deployments advertise loopback legitimately.
        let forwarder = test_forwarder_with(ForwardTargetPolicy::AllowPrivate);
        let rpcs = vec![
            (test_key(1), test_rpc("http://127.0.0.1:8545")?),
            (test_key(2), test_rpc("http://10.0.0.1:8545")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        assert_eq!(resolved.len(), 2);
        assert_eq!(cached_urls(&forwarder).len(), 2);
        assert!(refused_urls(&forwarder).is_empty());
        Ok(())
    }

    #[test]
    fn public_only_refuses_every_non_global_host_form() -> eyre::Result<()> {
        // IPv4 special-purpose blocks.
        assert_eq!(refusal("http://127.0.0.1:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://127.255.255.254/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://10.0.0.1:8545")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://172.16.0.1/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://192.168.1.1:8545")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://169.254.169.254/latest/meta-data/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://0.0.0.0:8545")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://0.1.2.3/")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://100.64.0.1/")?, RefusedTarget::SharedAddressSpace);
        assert_eq!(refusal("http://100.127.255.255/")?, RefusedTarget::SharedAddressSpace);
        assert_eq!(refusal("http://224.0.0.1/")?, RefusedTarget::NotUnicast);
        assert_eq!(refusal("http://255.255.255.255/")?, RefusedTarget::NotUnicast);

        // Alternative IPv4 spellings the URL host parser normalizes before the check sees them.
        assert_eq!(refusal("http://2130706433/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://0177.0.0.1/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://0x7f.0.0.1/")?, RefusedTarget::Loopback);

        // IPv6 special-purpose blocks, including the IPv4-mapped spelling of a loopback address
        // that no IPv6 predicate catches.
        assert_eq!(refusal("http://[::1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[::ffff:127.0.0.1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[::ffff:10.0.0.1]/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://[::ffff:169.254.169.254]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[::]/")?, RefusedTarget::Unspecified);
        assert_eq!(refusal("http://[fc00::1]/")?, RefusedTarget::UniqueLocal);
        assert_eq!(refusal("http://[fd12:3456::1]:8545")?, RefusedTarget::UniqueLocal);
        assert_eq!(refusal("http://[fe80::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[febf::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[ff02::1]/")?, RefusedTarget::NotUnicast);

        // Prefixes a translating gateway routes back to an embedded IPv4 destination, which
        // `Ipv6Addr::to_ipv4` does not recognize. `64:ff9b::a9fe:a9fe` is the cloud
        // instance-metadata address reached over NAT64.
        assert_eq!(refusal("http://[64:ff9b::a9fe:a9fe]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[64:ff9b::7f00:1]:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[64:ff9b::a00:1]/")?, RefusedTarget::PrivateUse);
        assert_eq!(refusal("http://[2002:a9fe:a9fe::1]/")?, RefusedTarget::LinkLocal);
        assert_eq!(refusal("http://[2002:7f00:1::1]/")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://[2002:c0a8:101::1]/")?, RefusedTarget::PrivateUse);

        // Names reserved to resolve locally, in relative, absolute and subdomain form.
        assert_eq!(refusal("http://localhost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://LocalHost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://localhost./")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://validator.localhost:8545")?, RefusedTarget::Loopback);
        assert_eq!(refusal("http://validator.local:8545")?, RefusedTarget::LinkLocal);
        Ok(())
    }

    #[test]
    fn public_only_admits_public_hosts() -> eyre::Result<()> {
        // Public literals and hostnames must still resolve, or the filter is a blanket reject.
        // Each entry sits just outside a refused block, so an off-by-one in a mask shows up here.
        [
            "http://8.8.8.8:8545",
            "https://1.1.1.1/",
            "http://172.32.0.1/",      // just above the 172.16/12 private block
            "http://100.128.0.1/",     // just above the 100.64/10 shared block
            "http://169.253.0.1/",     // just below the 169.254/16 link-local block
            "http://126.255.255.255/", // just below 127/8
            "http://223.255.255.255/", // just below the 224/4 multicast block
            "https://validator.example.com:8545/",
            "http://[2001:4860:4860::8888]/",
            "http://[::ffff:8.8.8.8]/", // IPv4-mapped, but the mapped address is public
            "http://[64:ff9b::808:808]/", // NAT64, but the embedded address is public
            "http://[2002:808:808::1]/", // 6to4, but the embedded address is public
            "http://[64:ff9c::a9fe:a9fe]/", // not the NAT64 well-known prefix: not unmapped
            "http://[fbff::1]/",        // just below fc00::/7
            "http://[fe7f::1]/",        // just below fe80::/10
            "http://not-localhost.example.com/",
            "http://localhosting.example.com/",
        ]
        .into_iter()
        .try_for_each(|url| {
            let parsed: Url = url.parse()?;
            assert_eq!(
                ForwardTargetPolicy::PublicOnly.check(&parsed),
                Ok(()),
                "{url} must be accepted as a dial target"
            );
            eyre::Ok(())
        })
    }

    #[test]
    fn from_allow_private_defaults_to_public_only() {
        assert_eq!(ForwardTargetPolicy::from_allow_private(false), ForwardTargetPolicy::PublicOnly);
        assert_eq!(
            ForwardTargetPolicy::from_allow_private(true),
            ForwardTargetPolicy::AllowPrivate
        );
    }

    #[test]
    fn classify_server_error_maps_reth_rejections() -> eyre::Result<()> {
        // A duplicate ("already known", code -32000) is a delivery, not a failure to retry.
        assert_eq!(
            classify_server_error(-32000, "already known".to_string()),
            Disposition::Delivered
        );
        // A full pool (code -32003) is transient: fall through to the next validator.
        assert_eq!(
            classify_server_error(-32003, "txpool is full".to_string()),
            Disposition::TryNext("txpool is full".to_string())
        );
        // An internal error (code -32603) is also validator-local: try the next validator.
        assert_eq!(
            classify_server_error(-32603, "database error".to_string()),
            Disposition::TryNext("database error".to_string())
        );
        // A considered rejection (nonce too low, code -32000) stops the fallback chain.
        assert!(matches!(
            classify_server_error(-32000, "nonce too low: next nonce 42, tx nonce 41".to_string()),
            Disposition::Rejected(_)
        ));
        Ok(())
    }

    /// Reth answers `-32000` for refusals that depend on one validator's pool contents or one
    /// operator's admission config. These are not verdicts on the transaction: the forward tries
    /// the next advertised validator instead of dropping the transaction as rejected.
    #[test]
    fn classify_server_error_tries_next_on_node_local_refusals() {
        [
            "transaction underpriced",
            "replacement transaction underpriced",
            "tx fee (2000000000000000000 wei) exceeds the configured cap (1000000000000000000 wei)",
            "address already reserved",
            // The scan is case-insensitive, matching the `already known` handling.
            "Transaction Underpriced",
        ]
        .into_iter()
        .for_each(|message| {
            assert_eq!(
                classify_server_error(-32000, message.to_string()),
                Disposition::TryNext(message.to_string())
            )
        });
        // An adjacent chain-wide verdict stays terminal: sharing words with a carve-out entry
        // is not a match.
        assert!(matches!(
            classify_server_error(-32000, "gas required exceeds allowance (21000)".to_string()),
            Disposition::Rejected(_)
        ));
        // The carve-out is scoped to code -32000: the same substring under another code (for
        // example a revert reason echoed under reth's code 3) stays terminal.
        assert!(matches!(
            classify_server_error(3, "execution reverted: transaction underpriced".to_string()),
            Disposition::Rejected(_)
        ));
    }

    /// With no advertised endpoint, or with only endpoints the policy refuses, the batch is
    /// not admitted: `forward_txns` returns `false` so the caller keeps its transactions.
    #[test]
    fn forward_txns_refuses_when_no_endpoint_is_usable() -> eyre::Result<()> {
        let forwarder = test_forwarder();

        // No committee validator has advertised an endpoint.
        assert!(!forwarder.forward_txns(vec![vec![0u8; 8]], vec![test_key(1)], vec![]));

        // The only advertised endpoint is private, so the `PublicOnly` policy refuses it and
        // no provider resolves.
        let refused = vec![(test_key(1), test_rpc("http://10.0.0.1:8545")?)];
        assert!(!forwarder.forward_txns(vec![vec![0u8; 8]], vec![test_key(1)], refused));
        Ok(())
    }

    /// One public advertised endpoint resolves a provider, so the batch is admitted to a
    /// forward task and `forward_txns` returns `true`. Admission is not delivery: the
    /// background task is free to fail and is not awaited here.
    #[tokio::test]
    async fn forward_txns_admits_when_a_provider_resolves() -> eyre::Result<()> {
        // Keep the task manager alive so the spawned forward task is tracked normally.
        let task_manager = TaskManager::default();
        let forwarder = WorkerRpcForwarder::new(
            task_manager.get_spawner(),
            ForwardTargetPolicy::PublicOnly,
            None,
        );
        let rpcs = vec![(test_key(1), test_rpc("http://validator.example.com:8545")?)];

        assert!(forwarder.forward_txns(vec![vec![0u8; 8]], vec![test_key(1)], rpcs));
        Ok(())
    }

    /// An endpoint that fails at the connection level is demoted when its forward task ends,
    /// so the next batch against the same advertisement is refused (`forward_txns` returns
    /// `false` and the caller keeps its transactions) instead of being admitted against an
    /// endpoint that just proved dead (issue #1145).
    #[tokio::test]
    async fn forward_txns_demotes_an_unreachable_endpoint_then_refuses_the_next_batch(
    ) -> eyre::Result<()> {
        // A port with nothing behind it: the URL resolves (so the first batch is admitted)
        // and the send then fails with a transport error and no server response, which is
        // the connection-level failure that demotes.
        let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = closed.local_addr()?;
        drop(closed);

        // The manager outlives the forward: dropping it would shut the spawned task down
        // before it could record the demotion.
        let manager = TaskManager::default();
        // `AllowPrivate`: the fixture endpoint is a loopback socket, which the shipped default
        // policy would refuse before the demotion under test was ever reached.
        let forwarder =
            WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::AllowPrivate, None);
        let endpoint = format!("http://{addr}");
        let rpcs = vec![(test_key(1), test_rpc(&endpoint)?)];

        // The first batch is admitted: admission counts an endpoint as usable the moment its
        // URL resolves, because the lazy HTTP client has not dialed yet.
        assert!(forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs.clone()));

        // Draining every permit can only complete once the forward task has dropped the one
        // it took, and the task records the demotion before releasing its permit.
        let drained = timeout(
            Duration::from_secs(30),
            Arc::clone(&forwarder.forwards_in_flight).acquire_many_owned(max_permits()),
        )
        .await;
        assert!(drained.is_ok(), "forward task never released its permit");
        // Return the drained permits so the next admission attempt is not refused for
        // capacity, which would pass the assertion below for the wrong reason.
        drop(drained);

        // The demotion names the endpoint, under the exact (normalized) key the cache uses.
        assert_eq!(demoted_urls(&forwarder), vec![test_rpc(&endpoint)?.http.to_string()]);

        // The same advertisement now resolves no usable endpoint: the batch is refused.
        assert!(!forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs));
        Ok(())
    }

    /// A demoted endpoint re-enters admission once [`UNREACHABLE_COOLDOWN`] elapses: batches
    /// sealed inside the window are refused so the caller keeps its transactions, and the
    /// first seal after the window is admitted again. Time is virtual here, so the test
    /// measures the whole window without waiting for it.
    #[tokio::test(start_paused = true)]
    async fn demoted_endpoint_reenters_admission_after_the_cooldown() -> eyre::Result<()> {
        // Keep the task manager alive so the re-admitted forward task is tracked normally.
        let manager = TaskManager::default();
        let forwarder =
            WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::PublicOnly, None);
        // The exact key the cache stores: URL parsing normalizes the advertisement (say, a
        // trailing slash), so the key is computed from the parsed URL, never hand-written.
        let rpc = test_rpc("http://validator.example.com:8545")?;
        let advertised = rpc.http.to_string();
        let rpcs = vec![(test_key(1), rpc)];
        forwarder
            .cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .unreachable
            .insert(advertised, Instant::now());

        // Inside the cooldown the only advertised endpoint sits out admission: refused.
        assert!(!forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs.clone()));

        // Past the cooldown the demotion expires and the same advertisement is admitted.
        tokio::time::advance(UNREACHABLE_COOLDOWN + Duration::from_secs(1)).await;
        assert!(forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(1)], rpcs));
        Ok(())
    }

    /// The rotation offset is the counter reduced modulo the list length, total for every
    /// input a caller can produce.
    #[test]
    fn rotation_start_wraps_and_is_total() {
        assert_eq!(rotation_start(0, 3), 0);
        assert_eq!(rotation_start(5, 3), 2);
        assert_eq!(rotation_start(u64::MAX, 3), 0);
        assert_eq!(rotation_start(7, 1), 0);
        // An empty list has no offset to pick: zero, not a division by zero.
        assert_eq!(rotation_start(9, 0), 0);
    }

    /// Rotation permutes the fallback list without dropping or duplicating an entry, and a
    /// full cycle of counters returns to the identity order.
    #[test]
    fn rotated_fallbacks_rotates_left_without_losing_entries() {
        let (a, b, c) = (test_key(1), test_key(2), test_key(3));
        assert_eq!(rotated_fallbacks(vec![a, b, c], 0), vec![a, b, c]);
        assert_eq!(rotated_fallbacks(vec![a, b, c], 1), vec![b, c, a]);
        assert_eq!(rotated_fallbacks(vec![a, b, c], 2), vec![c, a, b]);
        // The cycle closes: three fallbacks, counter three, identity again.
        assert_eq!(rotated_fallbacks(vec![a, b, c], 3), vec![a, b, c]);
        assert_eq!(rotated_fallbacks(Vec::new(), 7), Vec::new());
    }

    /// Consecutive forwards dial a different first fallback: the rotation counter, not the
    /// key sort, picks where the fallback walk starts, and it advances once per spawned
    /// forward.
    ///
    /// Three counting endpoints, transactions that recover no owner, and the counter pinned
    /// at zero: four single-transaction batches must land on endpoint one, two, three, then
    /// one again. Without rotation all four land on the lowest-keyed endpoint.
    #[tokio::test]
    async fn spawned_forwards_rotate_the_first_fallback_dialed() -> eyre::Result<()> {
        let (url_one, hits_one) = counting_ok_endpoint()?;
        let (url_two, hits_two) = counting_ok_endpoint()?;
        let (url_three, hits_three) = counting_ok_endpoint()?;
        let manager = TaskManager::default();
        // `AllowPrivate`: the fixture endpoints are loopback sockets, which the shipped
        // default policy would refuse before the rotation under test was ever reached.
        let forwarder =
            WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::AllowPrivate, None);
        forwarder.fallback_rotation.store(0, Ordering::Relaxed);
        // Pair each sorted committee key with one endpoint: `providers` iterates in key
        // order, so the sorted pairing makes "which endpoint is fallback N" exact.
        let sorted: Vec<BlsPublicKey> = [test_key(1), test_key(2), test_key(3)]
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let urls = [url_one, url_two, url_three];
        let rpcs = sorted
            .iter()
            .zip(urls.iter())
            .map(|(key, url)| test_rpc(url).map(|rpc| (*key, rpc)))
            .collect::<eyre::Result<Vec<_>>>()?;
        let counts = || {
            [
                hits_one.load(Ordering::Relaxed),
                hits_two.load(Ordering::Relaxed),
                hits_three.load(Ordering::Relaxed),
            ]
        };

        // One batch of one unrecoverable transaction, awaited to completion, then the hit
        // counts must equal `expected`. No owner is recovered, so the first fallback is the
        // first dial, and the permit drain is what awaits the spawned task: capacity only
        // returns when the forward finishes.
        let settle = |expected: [usize; 3]| {
            let forwarder = &forwarder;
            let sorted = &sorted;
            let rpcs = &rpcs;
            let counts = &counts;
            async move {
                assert!(forwarder.forward_txns(vec![vec![0_u8; 32]], sorted.clone(), rpcs.clone()));
                let drained = timeout(
                    Duration::from_secs(30),
                    Arc::clone(&forwarder.forwards_in_flight).acquire_many_owned(max_permits()),
                )
                .await??;
                drop(drained);
                assert_eq!(counts(), expected);
                eyre::Ok(())
            }
        };
        settle([1, 0, 0]).await?;
        settle([1, 1, 0]).await?;
        settle([1, 1, 1]).await?;
        settle([2, 1, 1]).await?;
        Ok(())
    }
}
