//! Admission-owned worker streams retained across epoch receiver handoff.

use super::{
    shed_sync_stream, try_admit_sync, BlsPublicKey, NetworkEvent, Req, Res, Stream,
    SyncStreamPermit, WorkerMetrics, WorkerNetworkHandle, WorkerSyncAdmission,
    SYNC_REQUEST_READ_TIMEOUT,
};
use futures::{task::AtomicWaker, StreamExt};
use parking_lot::Mutex;
use std::{
    collections::VecDeque,
    sync::Arc,
    task::{Context, Poll},
};
use tn_config::NetworkServeConfig;
use tn_types::{
    SendError, TnReceiver, TnSender, TryRecvError, TrySendError, TrySendOutcome, WorkerId,
};
use tokio::{sync::Notify, time::Instant};

/// A stream admitted before buffering, with an arrival-based request deadline.
pub struct AdmittedSyncStream<S = Stream> {
    /// Authenticated peer whose per-peer admission slot is held.
    peer: BlsPublicKey,
    /// Stream payload retained until the next epoch consumes it or it expires.
    stream: S,
    /// Shared global and per-peer admission ownership.
    permit: SyncStreamPermit,
    /// Time at which ingress accepted the stream.
    arrived: Instant,
    /// Opening-request deadline, including time spent waiting for an epoch.
    deadline: Instant,
}

impl<S> std::fmt::Debug for AdmittedSyncStream<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdmittedSyncStream")
            .field("peer", &self.peer)
            .field("arrived", &self.arrived)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl<S> AdmittedSyncStream<S> {
    /// Bind the admitted payload to its original arrival deadline.
    pub(super) fn new(peer: BlsPublicKey, stream: S, permit: SyncStreamPermit) -> Self {
        let arrived = Instant::now();
        Self { peer, stream, permit, arrived, deadline: arrived + SYNC_REQUEST_READ_TIMEOUT }
    }

    /// Transfer the payload and its admission ownership to the epoch handler.
    pub(super) fn into_parts(self) -> (BlsPublicKey, S, SyncStreamPermit, Instant, Instant) {
        (self.peer, self.stream, self.permit, self.arrived, self.deadline)
    }
}

/// Explicit terminal state for the process-lifetime stream lane.
enum PendingStreams<S> {
    /// Every queued entry owns a permit from the existing shared admission pool.
    Open(VecDeque<AdmittedSyncStream<S>>),
    /// Node shutdown has released all queued streams and their permits.
    Closed,
}

/// The stream-only lane; the admission pool bounds queued plus active entries.
pub(super) struct StreamLane<S = Stream> {
    /// Pending entries or the terminal shutdown state.
    pending: Mutex<PendingStreams<S>>,
    /// Wake the sole epoch receiver after enqueue, expiry, or shutdown.
    receiver: AtomicWaker,
    /// Wake the process-lifetime expiry task when its next deadline changes.
    changed: Notify,
}

impl<S> StreamLane<S> {
    /// Create an empty lane; no receiver subscription is needed to retain admitted streams.
    pub(super) fn new() -> Self {
        Self {
            pending: Mutex::new(PendingStreams::Open(VecDeque::new())),
            receiver: AtomicWaker::new(),
            changed: Notify::new(),
        }
    }

    /// Queue one already-admitted stream, or hand back ownership after node shutdown.
    pub(super) fn push(
        &self,
        stream: AdmittedSyncStream<S>,
    ) -> Result<(), Box<AdmittedSyncStream<S>>> {
        let result = match &mut *self.pending.lock() {
            PendingStreams::Open(pending) => {
                pending.push_back(stream);
                Ok(())
            }
            PendingStreams::Closed => Err(Box::new(stream)),
        };
        self.receiver.wake();
        self.changed.notify_one();
        result
    }

    /// Reserve the shared permit before retaining any payload during an epoch gap.
    fn admit(
        &self,
        pool: &WorkerSyncAdmission,
        peer: BlsPublicKey,
        stream: S,
    ) -> Result<(), Box<StreamForwardError<S>>> {
        let decision = try_admit_sync(&pool.stream_semaphore, &pool.peers, peer)
            .map_or(StreamAdmission::Denied, |permit| StreamAdmission::Admitted(Box::new(permit)));
        match decision {
            StreamAdmission::Admitted(permit) => {
                self.push(AdmittedSyncStream::new(peer, stream, *permit)).map_err(|admitted| {
                    let (peer, stream, permit, _, _) = (*admitted).into_parts();
                    drop(permit);
                    Box::new(StreamForwardError::Closed { peer, stream })
                })
            }
            StreamAdmission::Denied => Err(Box::new(StreamForwardError::Denied { peer, stream })),
        }
    }

    /// Poll the sole epoch receiver without losing an enqueue notification.
    pub(super) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<AdmittedSyncStream<S>>> {
        self.receiver.register(cx.waker());
        self.expire();
        match &mut *self.pending.lock() {
            PendingStreams::Open(pending) => pending.pop_front().map_or(Poll::Pending, |stream| {
                self.changed.notify_one();
                Poll::Ready(Some(stream))
            }),
            PendingStreams::Closed => Poll::Ready(None),
        }
    }

    /// Release expired pending payloads and their existing admission permits.
    fn expire(&self) {
        match &mut *self.pending.lock() {
            PendingStreams::Open(pending) => {
                pending.retain(|stream| stream.deadline > Instant::now());
            }
            PendingStreams::Closed => {}
        }
    }

    /// Release all queued ownership and wake a receiver during final node shutdown.
    pub(super) fn close(&self) {
        *self.pending.lock() = PendingStreams::Closed;
        self.receiver.wake();
        self.changed.notify_one();
    }

    /// Wait for the next queued deadline or a producer/state change.
    async fn wait_for_expiry(&self) -> bool {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let deadline = match &*self.pending.lock() {
            PendingStreams::Open(pending) => Some(
                pending
                    .iter()
                    .map(|stream| stream.deadline)
                    .min()
                    .unwrap_or_else(|| Instant::now() + SYNC_REQUEST_READ_TIMEOUT),
            ),
            PendingStreams::Closed => None,
        };
        futures::future::OptionFuture::from(deadline.map(|deadline| async move {
            tokio::select! {
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(deadline) => {},
            }
            self.expire();
            true
        }))
        .await
        .unwrap_or(false)
    }
}

/// Close the lane when its tracked process-lifetime expiry task is stopped.
struct ExpiryOwner<S>(Arc<StreamLane<S>>);

impl<S> Drop for ExpiryOwner<S> {
    /// Shutdown releases queued permits even if no epoch receiver is present.
    fn drop(&mut self) {
        self.0.close();
    }
}

impl<S: Send + 'static> StreamLane<S> {
    /// Expire pending streams independently of epoch progress and close on task shutdown.
    pub(super) fn run_expiry(self: Arc<Self>) -> impl std::future::Future<Output = ()> + Send {
        let owner = ExpiryOwner(self.clone());
        async move {
            let _owner = owner;
            futures::stream::unfold(self, |lane| async move {
                lane.wait_for_expiry().await.then_some(((), lane))
            })
            .for_each(|()| async {})
            .await;
        }
    }
}

/// Admission and task ownership installed before the worker swarm starts.
enum IngressBinding {
    /// The swarm must not forward before startup has installed its shared pool.
    Unbound,
    /// The same admission pool used by every epoch's worker handler.
    Bound(Box<BoundIngress>),
}

/// Shared startup resources owned by the bound worker ingress lane.
struct BoundIngress {
    /// The same admission pool used by every epoch's worker handler.
    admission: WorkerSyncAdmission,
    /// Handle retaining the existing shed task and source epoch behavior.
    handle: WorkerNetworkHandle,
    /// Metrics for this worker's retained stream lane.
    metrics: WorkerMetrics,
}

/// Two worker event lanes: epoch-scoped messages and admission-owned sync streams.
pub struct WorkerEventChannel<Events> {
    /// Original epoch channel, preserving its subscription and capacity semantics.
    events: Events,
    /// Process-lifetime lane bounded by the existing shared admission pool.
    streams: Arc<StreamLane>,
    /// Startup binding shared by sender clones.
    binding: Arc<Mutex<IngressBinding>>,
}

impl<Events: Clone> Clone for WorkerEventChannel<Events> {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            streams: self.streams.clone(),
            binding: self.binding.clone(),
        }
    }
}

impl<Events> std::fmt::Debug for WorkerEventChannel<Events> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("WorkerEventChannel").finish_non_exhaustive()
    }
}

impl<Events> WorkerEventChannel<Events> {
    /// Wrap the existing epoch channel without changing its queue capacity.
    pub fn new(events: Events) -> Self {
        Self {
            events,
            streams: Arc::new(StreamLane::new()),
            binding: Arc::new(Mutex::new(IngressBinding::Unbound)),
        }
    }

    /// Install the shared admission pool and node-lifetime cleanup before swarm execution.
    pub fn bind(&self, handle: &WorkerNetworkHandle, serve: &NetworkServeConfig, id: WorkerId) {
        let mut binding = self.binding.lock();
        assert!(matches!(&*binding, IngressBinding::Unbound), "worker ingress already bound");
        *binding = IngressBinding::Bound(Box::new(BoundIngress {
            admission: handle.sync_admission(serve).clone(),
            handle: handle.clone(),
            metrics: WorkerMetrics::new_for_worker(id),
        }));
        let expiry = self.streams.clone().run_expiry();
        handle.get_sync_task_spawner().spawn_task("worker stream ingress expiry", async move {
            expiry.await;
            Ok(())
        });
    }

    /// Acquire the existing sole epoch receiver lease alongside the persistent stream lane.
    pub fn subscribe_with<Receiver>(
        &self,
        subscribe: impl FnOnce(&Events) -> Receiver,
    ) -> WorkerEventReceiver<Receiver> {
        WorkerEventReceiver {
            events: subscribe(&self.events),
            streams: self.streams.clone(),
            turn: LaneTurn::Epoch,
        }
    }

    /// Admit before enqueue; excess streams use the unchanged bounded shed path.
    fn forward_stream(
        &self,
        peer: BlsPublicKey,
        stream: Stream,
    ) -> Result<TrySendOutcome, Box<TrySendError<NetworkEvent<Req, Res>>>> {
        match &*self.binding.lock() {
            IngressBinding::Unbound => {
                Err(Box::new(TrySendError::Closed(NetworkEvent::InboundStream { peer, stream })))
            }
            IngressBinding::Bound(binding) => {
                let BoundIngress { admission, handle, metrics } = binding.as_ref();
                self.streams
                    .admit(admission, peer, stream)
                    .map(|()| TrySendOutcome::Queued)
                    .or_else(|error| match *error {
                        StreamForwardError::Closed { peer, stream } => {
                            Err(Box::new(TrySendError::Closed(NetworkEvent::InboundStream {
                                peer,
                                stream,
                            })))
                        }
                        StreamForwardError::Denied { peer, stream } => {
                            shed_sync_stream(
                                &admission.shed_semaphore,
                                metrics,
                                handle.get_sync_task_spawner(),
                                handle.epoch(),
                                peer,
                                stream,
                            );
                            Ok(TrySendOutcome::Queued)
                        }
                    })
            }
        }
    }
}

/// Stream admission expressed without a second permit pool.
enum StreamAdmission {
    /// Ownership reserved from the existing global and per-peer allowance.
    Admitted(Box<SyncStreamPermit>),
    /// Existing capacity is exhausted.
    Denied,
}

/// Forwarding failures retain the payload for the existing transport or shed path.
enum StreamForwardError<S> {
    /// Existing admission capacity was exhausted before any buffering.
    Denied {
        /// Authenticated caller whose admission was denied.
        peer: BlsPublicKey,
        /// Payload handed to the bounded shed path.
        stream: S,
    },
    /// Node shutdown closed the lane and released the reserved admission ownership.
    Closed {
        /// Authenticated caller whose transport is closing.
        peer: BlsPublicKey,
        /// Payload returned to the transport caller.
        stream: S,
    },
}

impl<Events: TnSender<NetworkEvent<Req, Res>> + Sync> TnSender<NetworkEvent<Req, Res>>
    for WorkerEventChannel<Events>
{
    async fn send(
        &self,
        event: NetworkEvent<Req, Res>,
    ) -> Result<(), SendError<NetworkEvent<Req, Res>>> {
        match event {
            NetworkEvent::InboundStream { peer, stream } => {
                self.forward_stream(peer, stream).map(|_| ()).map_err(|error| match *error {
                    TrySendError::Full(event)
                    | TrySendError::Closed(event)
                    | TrySendError::Broadcast(event) => SendError(event),
                })
            }
            event @ (NetworkEvent::Request { .. }
            | NetworkEvent::Gossip(_)
            | NetworkEvent::Error(_, _)) => self.events.send(event).await,
        }
    }

    fn try_send(
        &self,
        event: NetworkEvent<Req, Res>,
    ) -> Result<(), TrySendError<NetworkEvent<Req, Res>>> {
        self.try_send_outcome(event).map(|_| ())
    }

    fn try_send_outcome(
        &self,
        event: NetworkEvent<Req, Res>,
    ) -> Result<TrySendOutcome, TrySendError<NetworkEvent<Req, Res>>> {
        match event {
            NetworkEvent::InboundStream { peer, stream } => {
                self.forward_stream(peer, stream).map_err(|error| *error)
            }
            event @ (NetworkEvent::Request { .. }
            | NetworkEvent::Gossip(_)
            | NetworkEvent::Error(_, _)) => self.events.try_send_outcome(event),
        }
    }
}

/// Worker input distinguishes ordinary epoch events from streams already admitted at ingress.
pub enum WorkerIngressEvent<S = Stream> {
    /// Existing gossip and RPC behavior, or a legacy receiver's raw stream.
    Network(NetworkEvent<Req, Res>),
    /// Stream retained through handoff with its original admission and deadline.
    Sync(AdmittedSyncStream<S>),
}

impl<S> std::fmt::Debug for WorkerIngressEvent<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Network(_) => formatter.debug_tuple("Network").finish_non_exhaustive(),
            Self::Sync(stream) => formatter.debug_tuple("Sync").field(stream).finish(),
        }
    }
}

impl From<NetworkEvent<Req, Res>> for WorkerIngressEvent {
    fn from(event: NetworkEvent<Req, Res>) -> Self {
        Self::Network(event)
    }
}

/// Alternate ready lanes so neither a gossip burst nor sync traffic starves the other.
#[derive(Debug)]
enum LaneTurn {
    /// Give an epoch-scoped message the first opportunity.
    Epoch,
    /// Give an admitted stream the first opportunity.
    Streams,
}

/// A sole epoch receiver lease that leaves pending admitted streams intact when dropped.
pub struct WorkerEventReceiver<Events, S = Stream> {
    /// The original epoch receiver owns and restores its exclusive lease.
    events: Events,
    /// Stream ownership stays process-wide between epoch receivers.
    streams: Arc<StreamLane<S>>,
    /// Lane polled first on the next receive.
    turn: LaneTurn,
}

impl<Events, S> std::fmt::Debug for WorkerEventReceiver<Events, S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkerEventReceiver")
            .field("turn", &self.turn)
            .finish_non_exhaustive()
    }
}

impl<Events: TnReceiver<NetworkEvent<Req, Res>>, S> WorkerEventReceiver<Events, S> {
    /// Poll one lane and convert its payload without changing ownership.
    fn poll_lane(
        &mut self,
        lane: &LaneTurn,
        cx: &mut Context<'_>,
    ) -> Poll<Option<WorkerIngressEvent<S>>> {
        match lane {
            LaneTurn::Epoch => {
                self.events.poll_recv(cx).map(|event| event.map(WorkerIngressEvent::Network))
            }
            LaneTurn::Streams => {
                self.streams.poll_recv(cx).map(|stream| stream.map(WorkerIngressEvent::Sync))
            }
        }
    }
}

impl<Events: TnReceiver<NetworkEvent<Req, Res>>, S: Send> TnReceiver<WorkerIngressEvent<S>>
    for WorkerEventReceiver<Events, S>
{
    async fn recv(&mut self) -> Option<WorkerIngressEvent<S>> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    fn try_recv(&mut self) -> Result<WorkerIngressEvent<S>, TryRecvError> {
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        match self.poll_recv(&mut cx) {
            Poll::Ready(event) => event.ok_or(TryRecvError::Disconnected),
            Poll::Pending => Err(TryRecvError::Empty),
        }
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<WorkerIngressEvent<S>>> {
        let first = std::mem::replace(&mut self.turn, LaneTurn::Epoch);
        let second = match first {
            LaneTurn::Epoch => LaneTurn::Streams,
            LaneTurn::Streams => LaneTurn::Epoch,
        };
        match self.poll_lane(&first, cx) {
            Poll::Ready(event) => {
                self.turn = second;
                Poll::Ready(event)
            }
            Poll::Pending => {
                self.turn = first;
                self.poll_lane(&second, cx)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::{max_sync_frame_size, read_frame, write_frame, SyncFrame, WorkerSyncRequest},
        *,
    };
    use futures::io::Cursor;
    use std::collections::BTreeSet;

    /// Reserve an actual shared-pool permit for a deterministic lane payload.
    fn admitted<S>(pool: &WorkerSyncAdmission, stream: S) -> eyre::Result<AdmittedSyncStream<S>> {
        let peer = BlsPublicKey::default();
        try_admit_sync(&pool.stream_semaphore, &pool.peers, peer)
            .map(|permit| AdmittedSyncStream::new(peer, stream, permit))
            .ok_or_else(|| eyre::eyre!("expected admission capacity"))
    }

    /// Construct an ordinary epoch event without networking or request-channel fixtures.
    fn gossip() -> NetworkEvent<Req, Res> {
        NetworkEvent::Gossip(Box::new(tn_network_libp2p::types::GossipPayload {
            message: tn_network_libp2p::GossipMessage {
                source: None,
                data: Vec::new(),
                sequence_number: None,
                topic: tn_network_libp2p::TopicHash::from_raw("worker-ingress-regression"),
            },
            relayer: None,
            author: None,
            receipt: None,
        }))
    }

    /// Simultaneously ready lanes alternate, including the actual wrapper's epoch forwarding.
    #[tokio::test]
    async fn ready_lanes_alternate_without_starving_epoch_events() -> eyre::Result<()> {
        let serve = NetworkServeConfig::default();
        let pool = WorkerSyncAdmission::new(&serve, 0);
        let lane = Arc::new(StreamLane::new());
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let channel = WorkerEventChannel::new(tx);
        assert!(matches!(channel.try_send_outcome(gossip()), Ok(TrySendOutcome::Queued)));
        assert!(matches!(channel.try_send_outcome(gossip()), Ok(TrySendOutcome::Queued)));
        (0..2).try_for_each(|_| {
            eyre::ensure!(
                lane.admit(&pool, BlsPublicKey::default(), ()).is_ok(),
                "expected admission"
            );
            Ok::<_, eyre::Report>(())
        })?;
        let mut receiver = WorkerEventReceiver { events: rx, streams: lane, turn: LaneTurn::Epoch };
        assert!(matches!(receiver.recv().await, Some(WorkerIngressEvent::Network(_))));
        assert!(matches!(receiver.recv().await, Some(WorkerIngressEvent::Sync(_))));
        assert!(matches!(receiver.recv().await, Some(WorkerIngressEvent::Network(_))));
        assert!(matches!(receiver.recv().await, Some(WorkerIngressEvent::Sync(_))));
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(pool.peers.lock().is_empty());
        Ok(())
    }

    /// Streams arriving after the old epoch receiver drops are delivered only to its successor.
    #[tokio::test]
    async fn epoch_receiver_gap_preserves_stream_permit_and_source_epoch() -> eyre::Result<()> {
        let serve = NetworkServeConfig::default();
        let pool = WorkerSyncAdmission::new(&serve, 0);
        let lane = Arc::new(StreamLane::new());
        let (_old_tx, old_rx) = tokio::sync::mpsc::channel::<NetworkEvent<Req, Res>>(1);
        let old =
            WorkerEventReceiver { events: old_rx, streams: lane.clone(), turn: LaneTurn::Epoch };
        drop(old);

        let mut request_stream = Cursor::new(Vec::new());
        let source_epoch = 7;
        write_frame(
            &mut request_stream,
            &SyncFrame::Req(WorkerSyncRequest::Batches {
                batch_digests: BTreeSet::new(),
                epoch: source_epoch,
            }),
            &mut Vec::new(),
            &mut Vec::new(),
            max_sync_frame_size(source_epoch),
        )
        .await?;
        request_stream.set_position(0);
        eyre::ensure!(
            lane.admit(&pool, BlsPublicKey::default(), request_stream).is_ok(),
            "gap admission failed"
        );
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream() - 1);

        let (_next_tx, next_rx) = tokio::sync::mpsc::channel::<NetworkEvent<Req, Res>>(1);
        let mut next =
            WorkerEventReceiver { events: next_rx, streams: lane, turn: LaneTurn::Epoch };
        let event = next.recv().await.ok_or_else(|| eyre::eyre!("handoff lost the stream"))?;
        match event {
            WorkerIngressEvent::Sync(stream) => {
                let (_, mut stream, permit, arrived, deadline) = stream.into_parts();
                assert_eq!(deadline - arrived, SYNC_REQUEST_READ_TIMEOUT);
                let frame = read_frame::<_, WorkerSyncRequest>(
                    &mut stream,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    max_sync_frame_size(source_epoch),
                )
                .await?;
                assert!(
                    matches!(frame, SyncFrame::Req(WorkerSyncRequest::Batches { epoch, .. }) if epoch == source_epoch)
                );
                assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream() - 1);
                drop(permit);
            }
            WorkerIngressEvent::Network(_) => eyre::bail!("expected the handoff stream"),
        }
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(pool.peers.lock().is_empty());
        Ok(())
    }

    /// Pending streams and active exchanges share the existing global and per-peer caps.
    #[test]
    fn pending_and_active_streams_share_admission_bounds() -> eyre::Result<()> {
        let serve = NetworkServeConfig::default();
        let pool = WorkerSyncAdmission::new(&serve, 0);
        let lane = StreamLane::new();
        (0..super::super::MAX_PENDING_REQUESTS_PER_PEER).try_for_each(|_| {
            eyre::ensure!(
                lane.admit(&pool, BlsPublicKey::default(), ()).is_ok(),
                "expected admission"
            );
            Ok::<_, eyre::Report>(())
        })?;
        assert!(
            try_admit_sync(&pool.stream_semaphore, &pool.peers, BlsPublicKey::default()).is_none()
        );
        let active: Vec<_> = (0..pool.stream_semaphore.available_permits())
            .map(|_| pool.stream_semaphore.try_acquire_owned())
            .collect::<Result<_, _>>()?;
        assert_eq!(pool.stream_semaphore.available_permits(), 0);
        assert_eq!(pool.shed_semaphore.available_permits(), serve.worker_shed());
        lane.close();
        assert_eq!(
            pool.stream_semaphore.available_permits(),
            super::super::MAX_PENDING_REQUESTS_PER_PEER
        );
        drop(active);
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(pool.peers.lock().is_empty());
        Ok(())
    }

    /// The process-lifetime task expires pending ownership even with no epoch receiver.
    #[tokio::test(start_paused = true)]
    async fn receiver_gap_expiry_releases_permits_without_a_consumer() -> eyre::Result<()> {
        let serve = NetworkServeConfig::default();
        let pool = WorkerSyncAdmission::new(&serve, 0);
        let lane = Arc::new(StreamLane::new());
        let task = tokio::spawn(lane.clone().run_expiry());
        eyre::ensure!(lane.push(admitted(&pool, ())?).is_ok(), "lane unexpectedly closed");
        tokio::task::yield_now().await;
        tokio::time::advance(SYNC_REQUEST_READ_TIMEOUT).await;
        tokio::task::yield_now().await;
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(pool.peers.lock().is_empty());
        task.abort();
        assert!(task.await.is_err());
        Ok(())
    }

    /// Shutdown before the expiry future's first poll still closes and drains the lane.
    #[tokio::test]
    async fn unpolled_expiry_owner_releases_pending_and_rejects_late_streams() -> eyre::Result<()> {
        let serve = NetworkServeConfig::default();
        let pool = WorkerSyncAdmission::new(&serve, 0);
        let lane = Arc::new(StreamLane::new());
        let expiry = lane.clone().run_expiry();
        eyre::ensure!(lane.push(admitted(&pool, ())?).is_ok(), "lane unexpectedly closed");
        drop(expiry);
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(lane.push(admitted(&pool, ())?).is_err());
        assert_eq!(pool.stream_semaphore.available_permits(), serve.batch_stream());
        assert!(pool.peers.lock().is_empty());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(matches!(lane.poll_recv(&mut cx), Poll::Ready(None)));
        Ok(())
    }
}
