use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{
        behaviour::ConnectionEstablished,
        dial_opts::{DialOpts, PeerCondition},
        ConnectionClosed, ConnectionDenied, ConnectionHandler, ConnectionId, DialFailure,
        FromSwarm, NetworkBehaviour, NotifyHandler, THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId, Stream, StreamProtocol,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, time::Interval};

use crate::{
    error::NetworkError,
    stream::{
        handler::{HandlerCommand, StreamHandler, StreamHandlerEvent},
        upgrade::{StreamError, StreamFailure},
    },
    types::{NetworkResult, NetworkType},
};

/// The protocol identifier for streaming data.
pub(crate) const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn-stream/0.0.1");

/// Maximum outbound stream opens buffered before new opens are rejected.
const MAX_PENDING_OPENS: usize = 1024;

/// Maximum behaviour-to-swarm events buffered before events are shed.
const MAX_EVENTS: usize = 1024;

/// Total time budget for an outbound open, covering dial, connection, and
/// substream negotiation. Once exceeded the caller receives a timeout error.
const OPEN_DEADLINE: Duration = Duration::from_secs(15);

/// How often stale pending opens are swept.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Window over which inbound streams are counted for rate limiting.
const INBOUND_RATE_WINDOW: Duration = Duration::from_secs(1);

/// Maximum inbound streams accepted from a single peer per [`INBOUND_RATE_WINDOW`].
const MAX_INBOUND_PER_WINDOW: usize = 256;

/// Events emitted by the stream behavior to the swarm/application layer.
#[derive(Debug)]
pub(crate) enum StreamEvent {
    /// An inbound stream was accepted and is ready for the application to handle.
    InboundStream {
        /// The peer that opened the stream.
        peer: PeerId,
        /// The established stream for reading/writing data.
        stream: Stream,
    },
    /// An outbound stream open failed; classified for peer scoring.
    OutboundFailure {
        /// The peer the open targeted.
        peer: PeerId,
        /// The classified failure.
        failure: StreamFailure,
    },
    /// An inbound stream was rejected; classified for peer scoring.
    InboundFailure {
        /// The peer that opened the stream.
        peer: PeerId,
        /// The classified failure.
        failure: StreamFailure,
    },
}

/// Lifecycle phase of a pending outbound open.
#[derive(Debug)]
enum OpenPhase {
    /// The peer is connected; dispatch the open to its handler.
    Connected,
    /// The peer is not connected; a dial must be issued.
    NeedsDial,
    /// A dial has been issued; waiting for the connection to establish.
    AwaitingConnection,
}

/// An outbound stream open being driven to completion.
struct PendingOpen {
    /// The peer to open the stream to.
    peer: PeerId,
    /// Known dial addresses, used if the peer is not connected.
    addrs: Vec<Multiaddr>,
    /// Channel for returning the established stream (or error) to the caller.
    reply: oneshot::Sender<NetworkResult<Stream>>,
    /// When this open expires if no stream has been established.
    deadline: Instant,
    /// The current lifecycle phase.
    phase: OpenPhase,
}

/// Per-peer inbound stream rate window.
struct InboundWindow {
    /// Streams accepted in the current window.
    count: usize,
    /// When the current window started.
    started: Instant,
}

/// The network behavior for streaming data.
///
/// Manages stream establishment across peer connections. An outbound open
/// dispatches immediately when the peer is connected, dials on demand (using
/// addresses supplied by the caller) when it is not, and resolves to an explicit
/// error if the peer cannot be reached or the open times out. Inbound streams
/// are rate limited per peer, and failures are classified for peer scoring.
pub(crate) struct StreamBehavior {
    /// Protocols every connection handler advertises (legacy `/tn-stream` first,
    /// then the per-role sync protocol), cloned into each new handler.
    protocols: Vec<StreamProtocol>,
    /// Events to emit to the swarm/application.
    events: VecDeque<StreamEvent>,
    /// Outbound opens being driven to completion.
    pending: Vec<PendingOpen>,
    /// Peers with at least one established connection.
    connected: HashSet<PeerId>,
    /// Per-peer inbound rate windows.
    inbound: HashMap<PeerId, InboundWindow>,
    /// Periodic timer that expires stale pending opens.
    sweep: Interval,
}

impl std::fmt::Debug for StreamBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamBehavior")
            .field("events_count", &self.events.len())
            .field("pending_count", &self.pending.len())
            .field("connected_count", &self.connected.len())
            .finish()
    }
}

impl StreamBehavior {
    /// Create a new instance of the stream behavior for `network_type`.
    ///
    /// Handlers advertise the legacy `/tn-stream/0.0.1` upgrade first, so
    /// existing opens keep negotiating it, followed by the role's sync protocol,
    /// so a responder also accepts inbound sync streams.
    pub(crate) fn new(network_type: NetworkType) -> Self {
        let protocols = vec![TN_STREAM_PROTOCOL, network_type.sync_protocol()];
        Self {
            protocols,
            events: VecDeque::new(),
            pending: Vec::new(),
            connected: HashSet::new(),
            inbound: HashMap::new(),
            sweep: tokio::time::interval(SWEEP_INTERVAL),
        }
    }

    /// Initiate an outbound stream to a peer.
    ///
    /// If the peer is connected the open is dispatched immediately. Otherwise it
    /// is dialed using `addrs`; with no addresses an explicit
    /// [`StreamError::NotConnected`] is returned. The established stream (or
    /// error) is delivered through `reply`.
    pub(crate) fn open_stream(
        &mut self,
        peer: PeerId,
        addrs: Vec<Multiaddr>,
        reply: oneshot::Sender<NetworkResult<Stream>>,
    ) {
        match () {
            () if self.pending.len() >= MAX_PENDING_OPENS => {
                let _ = reply.send(Err(NetworkError::Stream(StreamError::TooManyPending)));
            }
            () if self.connected.contains(&peer) => {
                self.queue_open(peer, addrs, reply, OpenPhase::Connected)
            }
            () if addrs.is_empty() => {
                let _ = reply.send(Err(NetworkError::Stream(StreamError::NotConnected)));
            }
            () => self.queue_open(peer, addrs, reply, OpenPhase::NeedsDial),
        }
    }

    /// Enqueue an outbound open with a fresh deadline.
    fn queue_open(
        &mut self,
        peer: PeerId,
        addrs: Vec<Multiaddr>,
        reply: oneshot::Sender<NetworkResult<Stream>>,
        phase: OpenPhase,
    ) {
        self.pending.push(PendingOpen {
            peer,
            addrs,
            reply,
            deadline: Instant::now() + OPEN_DEADLINE,
            phase,
        });
    }

    /// Queue an event to the swarm, shedding it if the buffer is saturated.
    fn push_event(&mut self, event: StreamEvent) {
        if self.events.len() < MAX_EVENTS {
            self.events.push_back(event);
        }
    }

    /// Fail and remove every pending open whose deadline has passed.
    fn expire_stale(&mut self) {
        let now = Instant::now();
        let (expired, live): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.pending).into_iter().partition(|o| o.deadline <= now);
        self.pending = live;
        expired.into_iter().for_each(|open| {
            let _ = open.reply.send(Err(NetworkError::Stream(StreamError::Timeout)));
            self.push_event(StreamEvent::OutboundFailure {
                peer: open.peer,
                failure: StreamFailure::Timeout,
            });
        });
    }

    /// Mark a peer connected and make any of its pending opens dispatch-ready.
    ///
    /// Promotes opens regardless of phase: an open may still be `NeedsDial` if
    /// the peer connected via another behaviour's dial before this behaviour
    /// emitted its own, and dialing an already-connected peer would fail the
    /// `PeerCondition::Disconnected` dial.
    fn on_connected(&mut self, peer: PeerId) {
        self.connected.insert(peer);
        self.pending
            .iter_mut()
            .filter(|o| o.peer == peer)
            .for_each(|o| o.phase = OpenPhase::Connected);
    }

    /// Mark a peer disconnected and re-queue its dispatch-ready opens for dialing.
    ///
    /// A `Connected` open whose peer just dropped must not be dispatched to a
    /// handler that no longer exists: libp2p silently drops a `NotifyHandler` to
    /// a gone peer, stranding the caller's reply. Re-dialing recovers the open or
    /// fails it explicitly.
    fn on_disconnected(&mut self, peer: PeerId) {
        self.connected.remove(&peer);
        self.inbound.remove(&peer);
        self.pending
            .iter_mut()
            .filter(|o| o.peer == peer && matches!(o.phase, OpenPhase::Connected))
            .for_each(|o| o.phase = OpenPhase::NeedsDial);
    }

    /// Fail opens that were awaiting a connection to a peer whose dial failed.
    fn on_dial_failed(&mut self, peer: PeerId) {
        let (failed, live): (Vec<_>, Vec<_>) = std::mem::take(&mut self.pending)
            .into_iter()
            .partition(|o| o.peer == peer && matches!(o.phase, OpenPhase::AwaitingConnection));
        self.pending = live;
        failed.into_iter().for_each(|open| {
            let _ = open.reply.send(Err(NetworkError::Stream(StreamError::NotConnected)));
            self.push_event(StreamEvent::OutboundFailure {
                peer: open.peer,
                failure: StreamFailure::DialFailure,
            });
        });
    }

    /// Record an inbound stream and report whether it exceeds the per-peer rate.
    fn inbound_rate_limited(&mut self, peer: PeerId) -> bool {
        let now = Instant::now();
        let window = self.inbound.entry(peer).or_insert(InboundWindow { count: 0, started: now });
        if now.duration_since(window.started) >= INBOUND_RATE_WINDOW {
            window.count = 0;
            window.started = now;
        }
        window.count += 1;
        window.count > MAX_INBOUND_PER_WINDOW
    }
}

impl NetworkBehaviour for StreamBehavior {
    type ConnectionHandler = StreamHandler;
    type ToSwarm = StreamEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(StreamHandler::new(self.protocols.clone()))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(StreamHandler::new(self.protocols.clone()))
    }

    fn on_swarm_event(&mut self, event: FromSwarm<'_>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished { peer_id, .. }) => {
                self.on_connected(peer_id);
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                remaining_established: 0,
                ..
            }) => {
                // last connection to the peer closed
                self.on_disconnected(peer_id);
            }
            FromSwarm::DialFailure(DialFailure { peer_id: Some(peer_id), .. }) => {
                self.on_dial_failed(peer_id);
            }
            _ => {
                // `FromSwarm` is non-exhaustive; remaining events are not relevant here.
            }
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        match event {
            StreamHandlerEvent::InboundStream { stream } => {
                if self.inbound_rate_limited(peer_id) {
                    // drop the stream; report for scoring
                    self.push_event(StreamEvent::InboundFailure {
                        peer: peer_id,
                        failure: StreamFailure::InboundRateLimited,
                    });
                } else {
                    self.push_event(StreamEvent::InboundStream { peer: peer_id, stream });
                }
            }
            StreamHandlerEvent::OutboundFailure { failure } => {
                self.push_event(StreamEvent::OutboundFailure { peer: peer_id, failure });
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // Expire stale opens on each timer tick.
        while self.sweep.poll_tick(cx).is_ready() {
            self.expire_stale();
        }

        // Emit pending events.
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ToSwarm::GenerateEvent(event));
        }

        // Dispatch the first open whose peer is connected.
        if let Some(i) = self.pending.iter().position(|o| matches!(o.phase, OpenPhase::Connected)) {
            let open = self.pending.remove(i);
            return Poll::Ready(ToSwarm::NotifyHandler {
                peer_id: open.peer,
                handler: NotifyHandler::Any,
                event: HandlerCommand::OpenStream { reply: open.reply },
            });
        }

        // Dial the first open whose peer is not yet connected, coalescing all of
        // that peer's opens onto the single dial so they do not each trigger one.
        let next_dial = self
            .pending
            .iter()
            .find(|o| matches!(o.phase, OpenPhase::NeedsDial))
            .map(|o| (o.peer, o.addrs.clone()));
        if let Some((peer, addrs)) = next_dial {
            self.pending
                .iter_mut()
                .filter(|o| o.peer == peer && matches!(o.phase, OpenPhase::NeedsDial))
                .for_each(|o| o.phase = OpenPhase::AwaitingConnection);
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer)
                    .condition(PeerCondition::Disconnected)
                    .addresses(addrs)
                    .build(),
            });
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr() -> Multiaddr {
        "/ip4/127.0.0.1/tcp/1".parse().expect("valid multiaddr")
    }

    #[tokio::test]
    async fn registers_sync_protocol_after_legacy() {
        // a worker advertises its per-worker sync protocol, legacy first
        let worker = StreamBehavior::new(NetworkType::Worker(3));
        assert_eq!(
            worker.protocols,
            vec![TN_STREAM_PROTOCOL, StreamProtocol::new("/tn-worker-3-sync/1.0.0")]
        );
        // a primary advertises its own sync protocol, legacy first
        let primary = StreamBehavior::new(NetworkType::Primary);
        assert_eq!(
            primary.protocols,
            vec![TN_STREAM_PROTOCOL, StreamProtocol::new("/tn-primary-sync/1.0.0")]
        );
    }

    #[tokio::test]
    async fn open_to_unreachable_peer_fails_fast_with_not_connected() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, rx) = oneshot::channel();

        // not connected and no addresses to dial: explicit error, nothing queued
        behavior.open_stream(peer, Vec::new(), tx);

        let result = rx.await.expect("reply delivered");
        assert!(matches!(result, Err(NetworkError::Stream(StreamError::NotConnected))));
        assert!(behavior.pending.is_empty());
    }

    #[tokio::test]
    async fn open_when_connected_is_queued_for_dispatch() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        behavior.connected.insert(peer);
        let (tx, _rx) = oneshot::channel();

        behavior.open_stream(peer, Vec::new(), tx);

        assert_eq!(behavior.pending.len(), 1);
        assert!(matches!(behavior.pending[0].phase, OpenPhase::Connected));
    }

    #[tokio::test]
    async fn open_when_disconnected_with_addrs_needs_dial() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, _rx) = oneshot::channel();

        behavior.open_stream(peer, vec![addr()], tx);

        assert_eq!(behavior.pending.len(), 1);
        assert!(matches!(behavior.pending[0].phase, OpenPhase::NeedsDial));
    }

    #[tokio::test]
    async fn open_over_capacity_is_rejected() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        behavior.connected.insert(peer);
        for _ in 0..MAX_PENDING_OPENS {
            let (tx, _rx) = oneshot::channel();
            behavior.open_stream(peer, Vec::new(), tx);
        }
        assert_eq!(behavior.pending.len(), MAX_PENDING_OPENS);

        let (tx, rx) = oneshot::channel();
        behavior.open_stream(peer, Vec::new(), tx);

        let result = rx.await.expect("reply delivered");
        assert!(matches!(result, Err(NetworkError::Stream(StreamError::TooManyPending))));
    }

    #[tokio::test]
    async fn connection_promotes_awaiting_open() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, _rx) = oneshot::channel();
        behavior.open_stream(peer, vec![addr()], tx);
        behavior.pending[0].phase = OpenPhase::AwaitingConnection;

        behavior.on_connected(peer);

        assert!(behavior.connected.contains(&peer));
        assert!(matches!(behavior.pending[0].phase, OpenPhase::Connected));
    }

    #[tokio::test]
    async fn connection_promotes_open_still_needing_dial() {
        // The peer may connect (via another behaviour's dial) while our open is
        // still NeedsDial; it must dispatch rather than dial an already-connected
        // peer (which would fail the PeerCondition::Disconnected dial).
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, _rx) = oneshot::channel();
        behavior.open_stream(peer, vec![addr()], tx);
        assert!(matches!(behavior.pending[0].phase, OpenPhase::NeedsDial));

        behavior.on_connected(peer);

        assert!(matches!(behavior.pending[0].phase, OpenPhase::Connected));
    }

    #[tokio::test]
    async fn dial_failure_fails_awaiting_open() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, rx) = oneshot::channel();
        behavior.open_stream(peer, vec![addr()], tx);
        behavior.pending[0].phase = OpenPhase::AwaitingConnection;

        behavior.on_dial_failed(peer);

        let result = rx.await.expect("reply delivered");
        assert!(matches!(result, Err(NetworkError::Stream(StreamError::NotConnected))));
        assert!(behavior.pending.is_empty());
    }

    #[tokio::test]
    async fn expired_open_times_out_and_reports_failure() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        let (tx, rx) = oneshot::channel();
        behavior.open_stream(peer, vec![addr()], tx);
        // force the deadline into the past
        behavior.pending[0].deadline = Instant::now() - Duration::from_secs(1);

        behavior.expire_stale();

        let result = rx.await.expect("reply delivered");
        assert!(matches!(result, Err(NetworkError::Stream(StreamError::Timeout))));
        assert!(behavior.pending.is_empty());
        assert!(matches!(
            behavior.events.pop_front(),
            Some(StreamEvent::OutboundFailure { failure: StreamFailure::Timeout, .. })
        ));
    }

    #[tokio::test]
    async fn inbound_rate_limit_trips_after_threshold() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        for _ in 0..MAX_INBOUND_PER_WINDOW {
            assert!(!behavior.inbound_rate_limited(peer));
        }
        assert!(behavior.inbound_rate_limited(peer));
    }

    #[tokio::test]
    async fn inbound_window_resets_after_interval() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        for _ in 0..=MAX_INBOUND_PER_WINDOW {
            behavior.inbound_rate_limited(peer);
        }
        assert!(behavior.inbound_rate_limited(peer));
        // age the window past the interval; the next stream starts a fresh window
        behavior.inbound.get_mut(&peer).expect("window exists").started =
            Instant::now() - INBOUND_RATE_WINDOW;
        assert!(!behavior.inbound_rate_limited(peer));
    }

    #[tokio::test]
    async fn disconnect_requeues_connected_open_for_dial() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        behavior.connected.insert(peer);
        let (tx, _rx) = oneshot::channel();
        behavior.open_stream(peer, vec![addr()], tx);
        assert!(matches!(behavior.pending[0].phase, OpenPhase::Connected));

        behavior.on_disconnected(peer);

        assert!(!behavior.connected.contains(&peer));
        assert!(matches!(behavior.pending[0].phase, OpenPhase::NeedsDial));
    }

    #[tokio::test]
    async fn push_event_sheds_when_full() {
        let mut behavior = StreamBehavior::new(NetworkType::Primary);
        let peer = PeerId::random();
        for _ in 0..MAX_EVENTS {
            behavior
                .push_event(StreamEvent::OutboundFailure { peer, failure: StreamFailure::Timeout });
        }
        assert_eq!(behavior.events.len(), MAX_EVENTS);

        behavior.push_event(StreamEvent::OutboundFailure { peer, failure: StreamFailure::Timeout });

        assert_eq!(behavior.events.len(), MAX_EVENTS);
    }
}
