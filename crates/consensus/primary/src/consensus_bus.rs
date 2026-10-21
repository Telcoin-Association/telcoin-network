//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, metrics::PrimaryMetrics,
    proposer::OurDigestMessage, state_sync::CertificateManagerCommand, RecentBlocks,
};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tn_config::Parameters;
use tn_network_libp2p::{types::NetworkEvent, ServiceClass, TNMessage};
use tn_storage::consensus::{ConsensusChain, ConsensusChainError};
use tn_types::{
    deconstruct_nonce, BlockNumHash, CanonicalExecutionReader, Certificate, CommittedSubDag,
    ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord, EpochVote, Header,
    Round, TnReceiver, TnSender, CHANNEL_CAPACITY,
};
use tokio::{
    sync::{
        broadcast, mpsc,
        watch::{self, error::RecvError},
    },
    time::error::Elapsed,
};
use tracing::{error, info, warn};

/// Depth of the bounded `sync_output` queue.
///
/// `sync_output` is a bounded mpsc: once this many outputs are queued, the state-sync producer's
/// `send().await` blocks until the subscriber takes one. The producer's `wait_for_execution` only
/// throttles non-empty outputs (an empty output produces no block to wait for), so while it
/// replays empty outputs from a local pack this queue is its only throttle. A broadcast here would
/// drop the oldest outputs once the producer ran a buffer ahead, and the subscriber cannot recover
/// an output it never received.
///
/// Items are full [`ConsensusOutput`]s (subdag + batches), so the depth bounds the memory a
/// lagging subscriber pins and the outputs refetched after an abnormal teardown. 256 is about
/// 2.5 s of subscriber work at 7-13 ms per saved output.
pub const SYNC_OUTPUT_CHANNEL_CAPACITY: usize = 256;
/// Depth of the bounded `consensus_output` queue.
///
/// `consensus_output` is a bounded mpsc: once this many outputs are queued, the subscriber's
/// `send().await` waits for the epoch manager to receive one, and the epoch manager in turn waits
/// on the bounded engine queue (`to_engine`). A broadcast here would drop the oldest outputs once
/// the subscriber ran a ring ahead of the engine, and the epoch manager would exit on the resulting
/// gap.
///
/// Items are full [`ConsensusOutput`]s, so the depth caps the memory held between the subscriber
/// and the epoch manager: about 125 MB at the hardware-requirements benchmark's 1.0-1.25 MB
/// average output, 550 MB at its 5.5 MB maximum.
const CONSENSUS_OUTPUT_CHANNEL_CAPACITY: usize = 100;
/// Capacity for the `exex_certificates` broadcast.
///
/// Certificates are small but arrive every round forever; a lagging ExEx reconciles via its
/// native `Lagged` handling rather than needing a deep buffer.
const EXEX_CERTIFICATES_CHANNEL_CAPACITY: usize = 1_000;
/// Capacity for the `exex_consensus_output` broadcast.
///
/// Items are full [`ConsensusOutput`]s. ExEx handles `Lagged` natively (surfaced as
/// `TnExExNotification::Lagged` reconciliation), so match the depth of the engine-bound
/// `consensus_output` queue instead of buffering hundreds of MB for a slow ExEx.
const EXEX_CONSENSUS_OUTPUT_CHANNEL_CAPACITY: usize = 100;
/// Capacity of the epoch pack request queue.
///
/// Producers wait while the queue is full, which paces them to the fetch workers. The fetch workers
/// that drain the queue must never wait on it: a worker that waits for room in its own queue stops
/// the consumption that would make room (see [`ConsensusBusApp::try_requeue_epoch_pack_file`]).
pub const EPOCH_REQUEST_QUEUE_CAPACITY: usize = 1024;
/// Byte weight of the fixed fields of a header (author, round, epoch, timestamp, latest
/// execution block) and its encoding overhead. Payload entries and parents are charged on top,
/// because they are the only parts of a header that a peer can grow.
const HEADER_FIXED_BYTES: usize = 256;
/// Byte weight of one header payload entry: a 32-byte batch digest and its worker id.
const HEADER_PAYLOAD_ENTRY_BYTES: usize = 40;
/// Byte weight of one header parent: a 32-byte header digest.
const HEADER_PARENT_BYTES: usize = 32;
/// Byte weight of a certificate on top of its header: the aggregate BLS signature, the signer
/// bitmap and the encoding overhead.
const CERTIFICATE_FIXED_BYTES: usize = 256;
/// Byte weight of an epoch-record request: an optional number and an optional hash.
const EPOCH_RECORD_REQUEST_BYTES: usize = 64;
/// Byte weight of an event with no variable-length part, such as an inbound stream.
const FIXED_EVENT_BYTES: usize = 64;
/// One mebibyte, for the byte caps in [`primary_event_limit`].
const MIB: usize = 1024 * 1024;
/// How often a budgeted [`QueChannel`] writes a summary log of the messages that it shed over
/// budget. The swarm counts every shed message in `tn_network_inbound_requests_shed_total` with
/// `reason="queue_budget"`; the log only tells an operator to look there.
const QUEUE_BUDGET_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// The item and byte caps of the primary network event queue for `class`.
///
/// Every class has its own caps, so gossip or other traffic at its caps cannot take the room that
/// the committee classes (votes and epoch-record requests) need. Each cap is near the honest
/// burst that the queue must hold between two epochs' event loops:
///
/// - `Vote`: a committee member receives about one vote request per peer per round, so 1,000 holds
///   about ten rounds of a 100-member committee. A vote request with its parent certificates weighs
///   a few KiB to tens of KiB, so 64 MiB holds the item cap at 64 KiB each.
/// - `EpochRecord`: requests from peers that catch up on epoch records. Each weighs
///   [`EPOCH_RECORD_REQUEST_BYTES`], so 1,000 of them fit in 1 MiB.
/// - `Gossip`: certificates, consensus results and epoch votes from the committee, a few KiB each.
///   4,000 holds about 40 rounds of certificates from a 100-member committee, and 32 MiB holds the
///   item cap at 8 KiB each.
/// - `Other`: inbound streams and error events, 1,000 items and 8 MiB.
/// - `CertificateSync` and `Batch`: the primary queue does not carry these classes today, so they
///   get small caps.
///
/// The item caps sum to 8,000, below [`CHANNEL_CAPACITY`], so the underlying queue never fills
/// and a budgeted send never reports [`tn_types::TrySendError::Full`].
fn primary_event_limit(class: ServiceClass) -> QueueLimit {
    match class {
        ServiceClass::Vote => QueueLimit::new(1_000, 64 * MIB),
        ServiceClass::EpochRecord => QueueLimit::new(1_000, MIB),
        ServiceClass::CertificateSync => QueueLimit::new(500, 4 * MIB),
        ServiceClass::Batch => QueueLimit::new(500, 4 * MIB),
        ServiceClass::Gossip => QueueLimit::new(4_000, 32 * MIB),
        ServiceClass::Other => QueueLimit::new(1_000, 8 * MIB),
    }
}

/// The budget charge of one primary network event.
///
/// The swarm decodes a request before it forwards it and does not pass the encoded length, so
/// the byte weight is a deterministic estimate: the variable-length parts of the event (header
/// payload and parents, parent certificates, gossip data, error text) plus fixed overheads. A
/// peer can grow only the variable-length parts, so the estimate follows what a peer can make the
/// queue hold.
fn primary_event_charge(
    event: &NetworkEvent<crate::network::Req, crate::network::Res>,
) -> QueueCharge {
    match event {
        NetworkEvent::Request { request, .. } => {
            QueueCharge::new(request.service_class(), primary_request_bytes(request))
        }
        NetworkEvent::Gossip(gossip) => {
            QueueCharge::new(ServiceClass::Gossip, gossip.message.data.len())
        }
        NetworkEvent::Error(message, _) => {
            QueueCharge::new(ServiceClass::Other, FIXED_EVENT_BYTES.saturating_add(message.len()))
        }
        NetworkEvent::InboundStream { .. } => {
            QueueCharge::new(ServiceClass::Other, FIXED_EVENT_BYTES)
        }
    }
}

/// The estimated byte weight of a primary request.
fn primary_request_bytes(request: &crate::network::Req) -> usize {
    match request {
        crate::network::Req::Vote { header, parents } => {
            parents.iter().map(certificate_bytes).fold(header_bytes(header), usize::saturating_add)
        }
        crate::network::Req::EpochRecord { .. } => EPOCH_RECORD_REQUEST_BYTES,
        // The swarm answers a peer exchange itself and does not forward it to this queue.
        crate::network::Req::PeerExchange { .. } => FIXED_EVENT_BYTES,
    }
}

/// The estimated byte weight of a header.
fn header_bytes(header: &Header) -> usize {
    HEADER_FIXED_BYTES
        .saturating_add(header.payload().len().saturating_mul(HEADER_PAYLOAD_ENTRY_BYTES))
        .saturating_add(header.parents().len().saturating_mul(HEADER_PARENT_BYTES))
}

/// The estimated byte weight of a certificate.
fn certificate_bytes(certificate: &Certificate) -> usize {
    CERTIFICATE_FIXED_BYTES.saturating_add(header_bytes(certificate.header()))
}

/// Whether a primary network event that a previous epoch's event loop left queued is still worth
/// handling in `epoch`.
///
/// A request that the swarm already ended (its cancel notice fired or its sender is gone) and a
/// vote request for a header from an earlier epoch are stale. Everything else is kept: gossip,
/// including the epoch votes that arrive between two epochs' event loops (epoch-record voting
/// depends on them), epoch-record requests, error events and inbound streams.
fn primary_event_is_current(
    event: &mut NetworkEvent<crate::network::Req, crate::network::Res>,
    epoch: Epoch,
) -> bool {
    match event {
        NetworkEvent::Request { request, cancel, .. } => {
            matches!(cancel.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty))
                && primary_request_is_current(request, epoch)
        }
        NetworkEvent::Gossip(_) | NetworkEvent::Error(..) | NetworkEvent::InboundStream { .. } => {
            true
        }
    }
}

/// Whether a primary request is still worth handling in `epoch`.
fn primary_request_is_current(request: &crate::network::Req, epoch: Epoch) -> bool {
    match request {
        crate::network::Req::Vote { header, .. } => header.epoch() >= epoch,
        crate::network::Req::EpochRecord { .. } | crate::network::Req::PeerExchange { .. } => true,
    }
}

/// The class and byte weight that a budgeted [`QueChannel`] charges one message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueueCharge {
    /// The class whose caps the message counts against.
    class: ServiceClass,
    /// The byte weight of the message.
    bytes: usize,
}

impl QueueCharge {
    /// Charge a message of `class` that weighs `bytes`.
    fn new(class: ServiceClass, bytes: usize) -> Self {
        Self { class, bytes }
    }
}

/// The item cap and the byte cap of one class on a budgeted [`QueChannel`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct QueueLimit {
    /// The most messages of the class that the channel holds at once.
    items: usize,
    /// The most bytes of the class that the channel holds at once.
    bytes: usize,
}

impl QueueLimit {
    /// Hold at most `items` messages and `bytes` bytes of one class.
    fn new(items: usize, bytes: usize) -> Self {
        Self { items, bytes }
    }
}

/// The messages of one class that a budgeted [`QueChannel`] holds.
#[derive(Clone, Copy, Debug, Default)]
struct ClassUsage {
    /// Messages held.
    items: usize,
    /// Bytes held.
    bytes: usize,
}

/// The occupancy and the shed counts of a [`QueueBudget`].
#[derive(Debug, Default)]
struct BudgetState {
    /// The messages held, per class.
    queued: HashMap<ServiceClass, ClassUsage>,
    /// The messages shed over budget since the last summary log, per class label.
    shed: BTreeMap<&'static str, u64>,
    /// When the last summary log was written.
    last_report: Option<Instant>,
}

impl BudgetState {
    /// Count one message of `class` shed over budget. When no summary log was written in the
    /// last [`QUEUE_BUDGET_REPORT_INTERVAL`], return the counts to log and restart them.
    fn record_shed(&mut self, class: ServiceClass) -> Option<BTreeMap<&'static str, u64>> {
        let count = self.shed.entry(class.label()).or_default();
        *count = count.saturating_add(1);
        let now = Instant::now();
        let due = self
            .last_report
            .is_none_or(|last| now.saturating_duration_since(last) >= QUEUE_BUDGET_REPORT_INTERVAL);
        due.then(|| {
            self.last_report = Some(now);
            std::mem::take(&mut self.shed)
        })
    }
}

/// The per-class item and byte budget of a [`QueChannel`], shared by its clones and its receiver.
///
/// A sender charges a message before it queues it, and the receiver releases the charge when it
/// receives the message, so the budget counts the messages in the queue.
struct QueueBudget<T> {
    /// The channel name for the summary log.
    name: &'static str,
    /// The class and byte weight of a message. It must be deterministic, because the receiver
    /// computes it again to release the charge.
    charge: fn(&T) -> QueueCharge,
    /// The caps of a class.
    limit: fn(ServiceClass) -> QueueLimit,
    /// The occupancy and the shed counts.
    state: Mutex<BudgetState>,
}

impl<T> std::fmt::Debug for QueueBudget<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueBudget")
            .field("name", &self.name)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl<T> QueueBudget<T> {
    /// Charge `value` against the caps of its class. Return `None`, and count the message as
    /// shed, when the class already holds its item cap or `value` would pass its byte cap.
    fn try_charge(&self, value: &T) -> Option<QueueCharge> {
        let charge = (self.charge)(value);
        let limit = (self.limit)(charge.class);
        let mut state = self.state.lock();
        let usage = state.queued.entry(charge.class).or_default();
        let bytes = usage.bytes.saturating_add(charge.bytes);
        if usage.items < limit.items && bytes <= limit.bytes {
            *usage = ClassUsage { items: usage.items.saturating_add(1), bytes };
            return Some(charge);
        }
        let report = state.record_shed(charge.class);
        drop(state);
        report.into_iter().for_each(|shed| {
            warn!(target: "primary", queue = self.name, ?shed, "queue shed messages over their class budget");
        });
        None
    }

    /// Release the charge that `value` held while queued.
    fn release(&self, value: &T) {
        self.release_charge((self.charge)(value));
    }

    /// Release `charge`, saturating at zero.
    fn release_charge(&self, charge: QueueCharge) {
        let mut state = self.state.lock();
        let usage = state.queued.entry(charge.class).or_default();
        *usage = ClassUsage {
            items: usage.items.saturating_sub(1),
            bytes: usage.bytes.saturating_sub(charge.bytes),
        };
    }
}

/// Release the charge that a queued `value` held in `budget`, when the channel has a budget.
fn release_queued<T>(budget: Option<&QueueBudget<T>>, value: &T) {
    budget.into_iter().for_each(|budget| budget.release(value));
}

/// What the budget of a [`QueChannel`] decided for one message.
#[derive(Clone, Copy, Debug)]
enum BudgetCheck {
    /// The channel has no budget.
    Unbudgeted,
    /// The message fits the caps of its class and holds this charge until it is received.
    Charged(QueueCharge),
    /// The class of the message is at its item or byte cap, so the message is dropped.
    OverBudget,
}

/// Wrapper around a receiver and a subs count to make sure only one of these exists at a time.
/// Note this does NOT implement Clone on purpose, do not implement it else managing subscriptions
/// will break.
#[derive(Debug)]
struct QueChanReceiver<T> {
    receiver: Option<mpsc::Receiver<T>>,
    container: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
    /// Flag to signal the sender that this receiver has been dropped.
    subscribed: Arc<AtomicBool>,
    /// If true then never set subscribed to false.
    always_subscribed: bool,
    /// The budget of the channel, if any. Receiving a message releases its charge.
    budget: Option<Arc<QueueBudget<T>>>,
}

/// Use the Drop to decrement subs and signal unsubscribed.
impl<T> Drop for QueChanReceiver<T> {
    fn drop(&mut self) {
        if !self.always_subscribed {
            self.subscribed.store(false, Ordering::Release);
        }
        (*self.container.lock()) = self.receiver.take();
    }
}

/// Wrapper around an mpsc channel.  It allows a channel to exist for application lifetime
/// even if used for epoch messages.  It tracks subscibers so that each epoch will be able to
/// "subscribe" to the channel (after the last epoch has dropped it's subscription).
#[derive(Debug)]
pub struct QueChannel<T> {
    channel: mpsc::Sender<T>,
    // Putting this in a lock is unfortunate but if we want an mpsc under the hood is needed.
    receiver: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
    /// Tracks whether a receiver is currently subscribed.
    /// When `false`, `send()` and `try_send()` become no-ops.
    subscribed: Arc<AtomicBool>,
    /// If true then set subscribed to false which will que messages even when no subscribers
    /// active.
    always_subscribed: bool,
    /// The per-class item and byte budget, if any. A message over budget is dropped.
    budget: Option<Arc<QueueBudget<T>>>,
}

impl<T> QueChannel<T> {
    /// Create a new QueChannel.
    pub fn new() -> Self {
        Self::build(CHANNEL_CAPACITY, false, None)
    }

    /// Create a new QueChannel that will que messages even when no subscribers.
    pub fn new_always_subscribed() -> Self {
        Self::build(CHANNEL_CAPACITY, true, None)
    }

    /// Create a QueChannel like [`Self::new_always_subscribed`] that caps the messages that it
    /// holds per service class.
    ///
    /// `charge` gives the class and byte weight of a message. It must be deterministic, because
    /// the receiver computes it again to release the charge. `limit` gives the item cap and byte
    /// cap of a class. A message whose class is at either cap is dropped, and
    /// `try_send_outcome` reports [`tn_types::TrySendOutcome::OverBudget`]. Each class has its own
    /// caps, so a flood of one class cannot take the room of another. When the item caps sum to
    /// at most [`CHANNEL_CAPACITY`], the underlying queue never fills.
    fn new_always_subscribed_budgeted(
        name: &'static str,
        charge: fn(&T) -> QueueCharge,
        limit: fn(ServiceClass) -> QueueLimit,
    ) -> Self {
        let budget = QueueBudget { name, charge, limit, state: Mutex::new(BudgetState::default()) };
        Self::build(CHANNEL_CAPACITY, true, Some(Arc::new(budget)))
    }

    /// Create a new QueChannel that queues at most `capacity` messages.
    ///
    /// Once `capacity` messages are queued, `send().await` waits for the subscriber to receive
    /// one and `try_send` returns [`tn_types::TrySendError::Full`], so a slow subscriber
    /// backpressures the producer instead of losing messages.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is 0, like [`tokio::sync::mpsc::channel`].
    pub fn with_capacity(capacity: usize) -> Self {
        Self::build(capacity, false, None)
    }

    /// Build the channel. With `always_subscribed` it queues messages even with no subscriber.
    /// With a `budget` it caps the messages that it holds per service class.
    fn build(
        capacity: usize,
        always_subscribed: bool,
        budget: Option<Arc<QueueBudget<T>>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        let subscribed = Arc::new(AtomicBool::new(always_subscribed));
        Self { channel: tx, receiver, subscribed, always_subscribed, budget }
    }

    /// Subscribe to receive messages on this channel.
    ///
    /// Must be called in synchronous `spawn()` methods, BEFORE spawning async tasks.
    /// Can only be called once at a time (returned receiver restores on Drop).
    pub fn subscribe(&self) -> impl TnReceiver<T> + 'static
    where
        T: Send + 'static,
    {
        let receiver = self.receiver.lock().take();
        if receiver.is_none() {
            panic!("Another subscription is already in use!")
        }
        self.subscribed.store(true, Ordering::Release);
        QueChanReceiver {
            receiver,
            container: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            budget: self.budget.clone(),
        }
    }

    /// Subscribe like [`Self::subscribe`], first discarding anything a previous subscriber left
    /// queued.
    ///
    /// For channels whose messages only mean something to the subscriber that was live when they
    /// were sent. The drain happens here rather than when the old receiver drops: a receiver can
    /// drop while its producer is still blocked in `send().await`, and that send would land after
    /// a drain-on-drop. By the time the next subscriber subscribes, the old producer has been
    /// aborted with its epoch's tasks and the new producer is not spawned yet (callers subscribe
    /// before spawning it), so nothing races this drain.
    ///
    /// # Panics
    ///
    /// Panics if another subscription is in use, like [`Self::subscribe`].
    pub fn subscribe_fresh(&self) -> impl TnReceiver<T> + 'static
    where
        T: Send + 'static,
    {
        let mut receiver = self.receiver.lock().take();
        let Some(rx) = receiver.as_mut() else { panic!("Another subscription is already in use!") };
        let discarded = std::iter::from_fn(|| rx.try_recv().ok())
            .inspect(|value| release_queued(self.budget.as_deref(), value))
            .count();
        if discarded > 0 {
            warn!(target: "primary", discarded, "discarded messages a previous subscriber left queued");
        }
        self.subscribed.store(true, Ordering::Release);
        QueChanReceiver {
            receiver,
            container: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            budget: self.budget.clone(),
        }
    }

    /// Subscribe like [`Self::subscribe`], first discarding the queued messages that `keep`
    /// rejects.
    ///
    /// For an always-subscribed channel whose messages must survive the gap between two
    /// subscribers, where some of what the previous subscriber left queued is stale for the next
    /// one. `keep` sees each queued message once, oldest first. Kept messages go back into the
    /// queue in order and keep their budget charge; a message sent while this runs can land
    /// ahead of them. Discarded messages release their charge. As for [`Self::subscribe_fresh`],
    /// callers subscribe before they spawn the next consumer.
    ///
    /// # Panics
    ///
    /// Panics if another subscription is in use, like [`Self::subscribe`].
    pub fn subscribe_retain(
        &self,
        mut keep: impl FnMut(&mut T) -> bool,
    ) -> impl TnReceiver<T> + 'static
    where
        T: Send + 'static,
    {
        let mut receiver = self.receiver.lock().take();
        let Some(rx) = receiver.as_mut() else { panic!("Another subscription is already in use!") };
        let queued: Vec<T> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        let (kept, discarded, lost) = queued.into_iter().fold(
            (0_usize, 0_usize, 0_usize),
            |(kept, discarded, lost), mut value| {
                if !keep(&mut value) {
                    release_queued(self.budget.as_deref(), &value);
                    return (kept, discarded.saturating_add(1), lost);
                }
                // A kept message still holds its charge, so the queue has room for it when the
                // item caps sum to at most its capacity. Release the charge if the queue refuses.
                self.channel.try_send(value).map_or_else(
                    |error| {
                        release_queued(self.budget.as_deref(), &error.into_inner());
                        (kept, discarded, lost.saturating_add(1))
                    },
                    |()| (kept.saturating_add(1), discarded, lost),
                )
            },
        );
        if discarded > 0 || lost > 0 {
            warn!(target: "primary", kept, discarded, lost, "discarded stale messages a previous subscriber left queued");
        }
        self.subscribed.store(true, Ordering::Release);
        QueChanReceiver {
            receiver,
            container: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            budget: self.budget.clone(),
        }
    }

    /// Charge `value` to the budget of the channel, if it has one.
    fn check_budget(&self, value: &T) -> BudgetCheck {
        self.budget.as_deref().map_or(BudgetCheck::Unbudgeted, |budget| {
            budget.try_charge(value).map_or(BudgetCheck::OverBudget, BudgetCheck::Charged)
        })
    }

    /// Release the charge of a message that the underlying queue did not take.
    fn refund(&self, check: BudgetCheck) {
        match check {
            BudgetCheck::Charged(charge) => {
                self.budget.iter().for_each(|budget| budget.release_charge(charge))
            }
            BudgetCheck::Unbudgeted | BudgetCheck::OverBudget => {}
        }
    }
}

impl<T> Default for QueChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for QueChannel<T> {
    fn clone(&self) -> Self {
        Self {
            channel: self.channel.clone(),
            receiver: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            budget: self.budget.clone(),
        }
    }
}

impl<T: Send + 'static> TnSender<T> for QueChannel<T> {
    async fn send(&self, value: T) -> Result<(), tn_types::SendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(());
        }
        let check = self.check_budget(&value);
        match check {
            // Drop a message over budget, as for a channel with no subscriber.
            BudgetCheck::OverBudget => Ok(()),
            BudgetCheck::Unbudgeted | BudgetCheck::Charged(_) => {
                Ok(self.channel.send(value).await.inspect_err(|_| self.refund(check))?)
            }
        }
    }

    /// Queue `value` without waiting, or drop it while no receiver is subscribed or while its
    /// class is over budget.
    fn try_send(&self, value: T) -> Result<(), tn_types::TrySendError<T>> {
        self.try_send_outcome(value).map(|_outcome| ())
    }

    /// Queue `value` like [`TnSender::try_send`], and report
    /// [`tn_types::TrySendOutcome::Unsubscribed`] when no receiver is subscribed and
    /// [`tn_types::TrySendOutcome::OverBudget`] when its class is over budget, so the network
    /// layer counts the drop as shed instead of as forwarded.
    fn try_send_outcome(
        &self,
        value: T,
    ) -> Result<tn_types::TrySendOutcome, tn_types::TrySendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(tn_types::TrySendOutcome::Unsubscribed);
        }
        let check = self.check_budget(&value);
        match check {
            BudgetCheck::OverBudget => Ok(tn_types::TrySendOutcome::OverBudget),
            BudgetCheck::Unbudgeted | BudgetCheck::Charged(_) => Ok(self
                .channel
                .try_send(value)
                .map(|()| tn_types::TrySendOutcome::Queued)
                .inspect_err(|_| self.refund(check))?),
        }
    }
}

impl<T: Send + 'static> TnReceiver<T> for QueChanReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        let value = self.receiver.as_mut().expect("receiver").recv().await;
        value.inspect(|value| release_queued(self.budget.as_deref(), value))
    }

    fn try_recv(&mut self) -> Result<T, tn_types::TryRecvError> {
        Ok(self
            .receiver
            .as_mut()
            .expect("receiver")
            .try_recv()
            .inspect(|value| release_queued(self.budget.as_deref(), value))?)
    }

    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<T>> {
        self.receiver
            .as_mut()
            .expect("receiver")
            .poll_recv(cx)
            .map(|value| value.inspect(|value| release_queued(self.budget.as_deref(), value)))
    }
}

/// Has sync completed?
#[derive(Copy, Clone, Debug, Default)]
pub enum NodeMode {
    /// This is a full CVV that can participate in consensus.
    /// The mode indicates fully-synced and actively voting
    /// in the current committee.
    #[default]
    CvvActive,
    /// This node can only follow consensus via consensus output.
    /// It is staked and is "catching up" to participate after a failure
    /// during the epoch. This mode allows the node to sync past the
    /// garbage collection window and rejoin the committee.
    CvvInactive,
    /// Node that is following consensus output. This may or may not be a
    /// staked node. The defining characteristic is it's NOT in the current committee.
    Observer,
}

impl NodeMode {
    /// True if this node is an active CVV.
    pub fn is_active_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive)
    }

    /// True if this node is a CVV (i.e. staked and able to participate in a committee).
    pub fn is_cvv(&self) -> bool {
        matches!(self, NodeMode::CvvActive | NodeMode::CvvInactive)
    }

    /// True if this node is an inactive CVV (catching up to rejoin the committee).
    pub fn is_cvv_inactive(&self) -> bool {
        matches!(self, NodeMode::CvvInactive)
    }

    /// True if this node is only an obsever and will never participate in an committee.
    pub fn is_observer(&self) -> bool {
        matches!(self, NodeMode::Observer)
    }
}

/// Convert the consensus-layer [`NodeMode`] into the serializable [`tn_types::NodeMode`] used at
/// the RPC boundary. Exhaustive over every variant so a new mode fails to compile until it is
/// mapped here.
impl From<NodeMode> for tn_types::NodeMode {
    fn from(mode: NodeMode) -> Self {
        match mode {
            NodeMode::CvvActive => tn_types::NodeMode::CvvActive,
            NodeMode::CvvInactive => tn_types::NodeMode::CvvInactive,
            NodeMode::Observer => tn_types::NodeMode::Observer,
        }
    }
}

/// The thread-safe inner type that holds all the channels for inner-consensus
/// communication between different tasks.
/// This contains things that exist for the app lifetime.
#[derive(Debug)]
pub struct ConsensusBusAppInner {
    /// Outputs the highest committed round & corresponding gc_round in the consensus.
    tx_committed_round_updates: watch::Sender<Round>,

    /// An epoch we need an epoch record for.
    tx_requested_missing_epoch: watch::Sender<Epoch>,

    /// Signals a new round
    tx_primary_round_updates: watch::Sender<Round>,

    /// Watch tracking most recent blocks
    tx_recent_blocks: watch::Sender<RecentBlocks>,

    /// Watch tracking most recently seen consensus header.
    tx_last_consensus_header: watch::Sender<Option<ConsensusHeader>>,
    /// Watch tracking the last gossipped epoch, consensus block number, hash and consensus bytes.
    tx_last_published_consensus_num_hash: watch::Sender<(Epoch, u64, ConsensusHeaderDigest)>,
    /// Watch requesting the fetch task backfill a bottom gap the gossip-driven walk cannot reach.
    ///
    /// Published by the observer's `state_sync::spawn_stream_consensus_headers` when its catch-up
    /// loop stalls, carrying `(epoch, target_number, target_hash, floor)`: fill the consensus
    /// range `floor..=target_number` (walking back from the verified gossip tip
    /// `target_hash`). Consumed by `state_sync::spawn_fetch_recent_consensus`, which owns the
    /// in-flight accounting. This is deliberately separate from the gossip watermark so a gap
    /// below it can still be recovered.
    tx_consensus_gap: watch::Sender<Option<(Epoch, u64, ConsensusHeaderDigest, u64)>>,

    /// Verified consensus OUTPUTs (header + batches) delivered to a following/catching-up
    /// subscriber for execution. Filled by the state-sync forward drain; used only by
    /// non-active nodes. A bounded queue (`SYNC_OUTPUT_CHANNEL_CAPACITY`), so a subscriber that
    /// falls behind blocks the producer instead of losing outputs.
    sync_output: QueChannel<ConsensusOutput>,
    /// Saved consensus outputs on their way to execution.
    ///
    /// The subscriber sends each output once it is saved; the epoch manager receives it and
    /// forwards it to the engine. Bounded by `CONSENSUS_OUTPUT_CHANNEL_CAPACITY`, so a slow epoch
    /// manager blocks the subscriber instead of losing outputs.
    consensus_output: QueChannel<ConsensusOutput>,

    /// Broadcast channel for verified certificates (ExEx).
    ///
    /// Fed from the consensus-following path (gossip-verified certificates on
    /// Observer / inactive-CVV nodes), never from the validator hot path. There
    /// is no own/peer split — a follower has no certificates of its own.
    exex_certificates: broadcast::Sender<Certificate>,
    /// Broadcast channel for the full consensus output (ExEx).
    ///
    /// Fed from the consensus-following path when a `ConsensusHeader` is
    /// reconstructed into a `ConsensusOutput` for execution. `ConsensusOutput`
    /// is cheap to clone (Arc-backed), so no outer `Arc` is needed.
    exex_consensus_output: broadcast::Sender<ConsensusOutput>,

    /// Status of sync?
    tx_sync_status: watch::Sender<NodeMode>,

    /// Produce new epoch certs as they are recieved.
    new_epoch_votes: QueChannel<EpochVote>,
    /// Watch channel to communicate the current epoch record to the vote collector.
    tx_epoch_record: watch::Sender<Option<EpochRecord>>,
    /// The que channel for primary network events.
    primary_network_events: QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>>,
    /// Sender for epoch records that need to have a pack file downloaded.
    epoch_request_queue_tx: tokio::sync::mpsc::Sender<EpochPackRequest>,
    /// Reciever for epoch records to download pack files for.
    epoch_request_queue_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<EpochPackRequest>>>,
    /// The epochs with a pack request queued or being fetched.
    ///
    /// Not reset between epochs: a request outlives the epoch that queued it.
    epoch_pack_claims: EpochPackClaims,
    /// Channel to request consensus headers to cache.
    /// Fields are epoch, consensus number, consensus digest and consensus output bytes.
    consensus_request_queue: QueChannel<(Epoch, u64, ConsensusHeaderDigest)>,
    /// Prometheus metrics for the primary's consensus pipeline.
    ///
    /// Lives on the app-lifetime bus because the bus already reaches every consensus
    /// component (proposer, certifier, certificate manager, consensus driver).
    metrics: PrimaryMetrics,

    /// Canonical execution-DB fallback for `wait_for_execution` (installed once the execution
    /// engine is built); empty until then and in engine-less tests/tools.
    canonical_reader: CanonicalReaderSlot,
}

/// The epochs that have a pack request queued or being fetched.
///
/// Every queued [`EpochPackRequest`] carries the claim on its epoch, and dropping the request
/// releases it, so an epoch is queued at most once until its fetch ends, however it ends. The set
/// is only ever locked for one insert or remove, never across an await.
#[derive(Clone, Debug, Default)]
struct EpochPackClaims(Arc<Mutex<HashSet<Epoch>>>);

impl EpochPackClaims {
    /// Claim `epoch`, or return `None` if a request for it is already queued or being fetched.
    fn claim(&self, epoch: Epoch) -> Option<EpochPackClaim> {
        let claimed = self.0.lock().insert(epoch);
        claimed.then(|| EpochPackClaim { epoch, claims: self.clone() })
    }
}

/// An [`EpochPackRequest`]'s claim on its epoch; dropping it releases the epoch.
///
/// Releasing on drop frees the epoch on every path that ends a request: a finished fetch, a skip,
/// shutdown, a panic, a cancelled send and dropping the queue.
struct EpochPackClaim {
    /// The claimed epoch.
    epoch: Epoch,
    /// The set to release the epoch from.
    claims: EpochPackClaims,
}

impl Drop for EpochPackClaim {
    fn drop(&mut self) {
        self.claims.0.lock().remove(&self.epoch);
    }
}

/// Prints only the claimed epoch, not the shared set behind it.
impl std::fmt::Debug for EpochPackClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EpochPackClaim").field("epoch", &self.epoch).finish_non_exhaustive()
    }
}

/// A request to download the pack file for one epoch.
///
/// The request holds the claim on its epoch, and dropping the request releases the epoch, so no
/// second request for the epoch is queued until this one is done with. The fields are private so a
/// caller cannot take the request apart and drop the claim partway through a fetch.
#[derive(Debug)]
pub struct EpochPackRequest {
    /// The record of the epoch before the requested one, which verifies the requested epoch.
    ///
    /// It has no setter because a stored epoch record is final.
    previous_epoch_record: EpochRecord,
    /// The record of the requested epoch.
    epoch_record: EpochRecord,
    /// This request's claim on its epoch.
    claim: EpochPackClaim,
    /// Counts records [`Self::set_epoch_record`] drops for a different epoch.
    metrics: PrimaryMetrics,
}

impl EpochPackRequest {
    /// The requested epoch.
    pub fn epoch(&self) -> Epoch {
        self.claim.epoch
    }

    /// The record of the epoch before the requested one.
    pub fn previous_epoch_record(&self) -> &EpochRecord {
        &self.previous_epoch_record
    }

    /// The record of the requested epoch.
    pub fn epoch_record(&self) -> &EpochRecord {
        &self.epoch_record
    }

    /// Replace the record of the requested epoch, for example with a newer one that arrived after
    /// the request was queued.
    ///
    /// Returns whether the record was applied. A record for a different epoch is dropped, counted
    /// and logged, since only a caller bug can offer one.
    pub fn set_epoch_record(&mut self, record: EpochRecord) -> bool {
        let requested = self.claim.epoch;
        if record.epoch != requested {
            self.metrics.epoch_pack_record_epoch_mismatch_total.increment(1);
            warn!(target: "primary", requested, offered = record.epoch,
                "dropped an epoch record offered to the pack request of a different epoch");
            return false;
        }
        self.epoch_record = record;
        true
    }
}

/// Late-bound handle to the canonical execution database, used by
/// [`ConsensusBusApp::wait_for_execution`] as a fallback when a queried execution tip has been
/// evicted from the in-memory `recent_blocks` ring.
///
/// The bus is constructed before the execution engine exists, so the reader is installed later
/// (see [`ConsensusBusApp::set_canonical_reader`]). It stays empty in tests and tools that never
/// wire an engine, in which case `wait_for_execution` keeps its original ring-only behavior.
#[derive(Default)]
struct CanonicalReaderSlot(Mutex<Option<Arc<dyn CanonicalExecutionReader>>>);

impl std::fmt::Debug for CanonicalReaderSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalReaderSlot").field("installed", &self.0.lock().is_some()).finish()
    }
}

impl CanonicalReaderSlot {
    /// Install (or replace) the canonical reader. Last writer wins, so a per-epoch engine rebuild
    /// simply refreshes the handle.
    fn install(&self, reader: Arc<dyn CanonicalExecutionReader>) {
        *self.0.lock() = Some(reader);
    }

    /// Confirm that `block` is the canonical execution block persisted at its height.
    ///
    /// Returns `false` when no reader is installed, the height is above the persisted tip, or the
    /// DB hash differs (a genuine fork).
    fn confirms(&self, block: &BlockNumHash) -> bool {
        // Clone the handle out and drop the lock before the DB read: never hold a lock across I/O.
        let reader = self.0.lock().clone();
        reader
            .and_then(|reader| reader.canonical_execution_hash(block.number))
            .is_some_and(|hash| hash == block.hash)
    }
}

/// The thread-safe inner type that holds all the channels for inner-consensus
/// communication between different tasks.
/// This contains things that exist for the app lifetime.
#[derive(Clone, Debug)]
pub struct ConsensusBusApp {
    /// Inner reference for quick clones.
    inner: Arc<ConsensusBusAppInner>,
}

impl ConsensusBusApp {
    /// Create a new application consensus bus.
    pub fn new() -> Self {
        // Using default GC depth for blocks to keep in memory.  This should
        // allow for twice the blocks as would be needed for a margin of safety
        // (some testing liked this).  Using the default to not overly complicate
        // creation of the bus.
        // This is basically for testing.
        Self::new_with_recent_blocks(Parameters::default_gc_depth())
    }

    pub fn new_with_recent_blocks(recent_blocks: u32) -> Self {
        let (tx_committed_round_updates, _) = watch::channel(Round::default());

        let (tx_requested_missing_epoch, _) = watch::channel(Epoch::default());

        let (tx_primary_round_updates, _) = watch::channel(0u32);
        let (tx_last_consensus_header, _) = watch::channel(None);
        let (tx_last_published_consensus_num_hash, _) =
            watch::channel((0, 0, ConsensusHeaderDigest::default()));
        let (tx_consensus_gap, _) = watch::channel(None);

        let (tx_recent_blocks, _) = watch::channel(RecentBlocks::new(recent_blocks as usize));
        let (tx_sync_status, _) = watch::channel(NodeMode::default());

        let sync_output = QueChannel::with_capacity(SYNC_OUTPUT_CHANNEL_CAPACITY);
        let consensus_output = QueChannel::with_capacity(CONSENSUS_OUTPUT_CHANNEL_CAPACITY);

        let (exex_certificates, _) = broadcast::channel(EXEX_CERTIFICATES_CHANNEL_CAPACITY);
        let (exex_consensus_output, _) = broadcast::channel(EXEX_CONSENSUS_OUTPUT_CHANNEL_CAPACITY);

        let (tx_epoch_record, _) = watch::channel(None);

        let (epoch_request_queue_tx, epochs_rx) =
            tokio::sync::mpsc::channel(EPOCH_REQUEST_QUEUE_CAPACITY);
        let epoch_request_queue_rx = Arc::new(tokio::sync::Mutex::new(epochs_rx));
        Self {
            inner: Arc::new(ConsensusBusAppInner {
                tx_committed_round_updates,
                tx_requested_missing_epoch,

                tx_primary_round_updates,
                tx_recent_blocks,
                tx_last_consensus_header,
                tx_last_published_consensus_num_hash,
                tx_consensus_gap,
                sync_output,
                consensus_output,
                exex_certificates,
                exex_consensus_output,
                tx_sync_status,
                new_epoch_votes: QueChannel::new(),
                tx_epoch_record,
                primary_network_events: QueChannel::new_always_subscribed_budgeted(
                    "primary_network_events",
                    primary_event_charge,
                    primary_event_limit,
                ),
                epoch_request_queue_tx,
                epoch_request_queue_rx,
                epoch_pack_claims: EpochPackClaims::default(),
                consensus_request_queue: QueChannel::new(),
                // new_with_labels (not Default): binds to the recorder active at bus
                // construction instead of caching the first-bound recorder process-wide
                metrics: PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new()),
                canonical_reader: CanonicalReaderSlot::default(),
            }),
        }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    pub fn reset_for_epoch(&self) {
        self.inner.tx_committed_round_updates.send_replace(Round::default());
        self.inner.tx_primary_round_updates.send_replace(0u32);
        // Drop any pending gap request from the prior epoch so the fetch task never backfills a
        // stale (epoch, number, hash) after we have moved on; the new epoch's stream task will
        // re-signal if it stalls.
        self.inner.tx_consensus_gap.send_replace(None);
    }

    /// Contains the highest committed round & corresponding gc_round for consensus.
    pub fn committed_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner.tx_committed_round_updates
    }

    /// Returns the current committed round value.
    pub fn committed_round(&self) -> Round {
        *self.inner.tx_committed_round_updates.borrow()
    }

    /// Contains the last requested epoch to retrieve a record.
    pub fn requested_missing_epoch(&self) -> &watch::Sender<Epoch> {
        &self.inner.tx_requested_missing_epoch
    }

    /// Update requested missing epoch if epoch is newer than the current value.
    /// Return true if it updates.
    pub fn set_request_missing_epoch_if_newer(&self, epoch: Epoch) -> bool {
        self.requested_missing_epoch().send_if_modified(|state| {
            if epoch > *state {
                // Not sure we can sanity check this epoch.  However if it is bogus the code
                // to handle it should be fine, it stops when out of epochs.
                *state = epoch;
                true
            } else {
                false
            }
        })
    }

    /// Prometheus metrics handles for the primary's consensus pipeline.
    pub fn metrics(&self) -> &PrimaryMetrics {
        &self.inner.metrics
    }

    /// Signals a new round
    pub fn primary_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner.tx_primary_round_updates
    }

    /// Returns the current primary round value.
    pub fn primary_round(&self) -> Round {
        *self.inner.tx_primary_round_updates.borrow()
    }

    /// Track recent blocks.
    pub fn recent_blocks(&self) -> &watch::Sender<RecentBlocks> {
        &self.inner.tx_recent_blocks
    }

    /// Returns the latest executed block's number and hash.
    pub fn latest_execution_block_num_hash(&self) -> BlockNumHash {
        self.inner.tx_recent_blocks.borrow().latest_execution_block_num_hash()
    }

    /// Returns the last consensus round processed by the engine.
    pub fn last_consensus_round(&self) -> Round {
        self.inner.tx_recent_blocks.borrow().last_consensus_round()
    }

    /// Returns the maximum number of recent blocks that can be held.
    pub fn recent_blocks_capacity(&self) -> u64 {
        self.inner.tx_recent_blocks.borrow().block_capacity()
    }

    /// Track the latest consensus header we have seen.
    /// Note, this should be a valid header (authenticated by it's epoch's committee).
    pub fn last_consensus_header(&self) -> &watch::Sender<Option<ConsensusHeader>> {
        &self.inner.tx_last_consensus_header
    }

    /// Update last consensus header if header is newer than the current value.
    /// Return true if it was updated.
    pub fn send_last_consensus_header_if_newer(&self, header: ConsensusHeader) -> bool {
        self.last_consensus_header().send_if_modified(|state| {
            if header.number > state.as_ref().map(|h| h.number).unwrap_or_default() {
                // Update our last seen valid consensus header if it is newer.
                *state = Some(header);
                true
            } else {
                false
            }
        })
    }

    /// Watch used by the observer catch-up loop to ask the fetch task to backfill a bottom gap.
    ///
    /// Send `Some((epoch, target_number, target_hash, floor))` via `send_replace` to request the
    /// consensus range `floor..=target_number`; subscribe to consume it in the fetch task. The
    /// watch coalesces (only the latest request survives), which is intended: the requester
    /// re-signals with a fresh floor if a request is superseded.
    pub fn consensus_gap_request(
        &self,
    ) -> &watch::Sender<Option<(Epoch, u64, ConsensusHeaderDigest, u64)>> {
        &self.inner.tx_consensus_gap
    }

    /// Track the latest published consensus header block number and hash seen on the gossip
    /// network. This value will have been verified and can be trusted to be the correct hash
    /// for block number.  DO NOT send unverified values to this watch.
    pub fn last_published_consensus_num_hash(
        &self,
    ) -> &watch::Sender<(Epoch, u64, ConsensusHeaderDigest)> {
        &self.inner.tx_last_published_consensus_num_hash
    }

    /// Update the last published consensus epoch, number and hash if number is greater than current
    /// value. Return true if it was updated.
    pub fn publish_consensus_num_hash_if_newer(
        &self,
        epoch: Epoch,
        number: u64,
        hash: ConsensusHeaderDigest,
    ) -> bool {
        self.last_published_consensus_num_hash().send_if_modified(|state| {
            if number > state.1 {
                state.0 = epoch;
                state.1 = number;
                state.2 = hash;
                true
            } else {
                false
            }
        })
    }

    /// Returns the latest verified consensus block number and hash from gossip.
    pub fn published_consensus_num_hash(&self) -> (Epoch, u64, ConsensusHeaderDigest) {
        *self.inner.tx_last_published_consensus_num_hash.borrow()
    }

    /// Bounded queue carrying saved consensus outputs (each includes its consensus chain block) to
    /// the epoch manager, which forwards them to the engine for execution. `send().await` waits
    /// while the epoch manager is a full queue behind; with no subscriber a send is a no-op.
    pub fn consensus_output(&self) -> &impl TnSender<ConsensusOutput> {
        &self.inner.consensus_output
    }

    /// Bounded queue delivering verified consensus OUTPUTs (header + batches) to a
    /// following/catching-up subscriber for execution. Used when not participating in consensus.
    /// `send().await` waits while the subscriber is a full queue behind; with no subscriber a
    /// send is a no-op.
    pub fn sync_output(&self) -> &impl TnSender<ConsensusOutput> {
        &self.inner.sync_output
    }

    /// Status of initial sync operation.
    pub fn node_mode(&self) -> &watch::Sender<NodeMode> {
        &self.inner.tx_sync_status
    }

    /// Returns the current node mode.
    pub fn current_node_mode(&self) -> NodeMode {
        *self.inner.tx_sync_status.borrow()
    }

    /// Returns true if this node is a CVV (active or inactive).
    ///
    /// A CVV is a staked node that can participate in a committee,
    /// regardless of whether it's currently active or catching up.
    pub fn is_cvv(&self) -> bool {
        self.inner.tx_sync_status.borrow().is_cvv()
    }

    /// Returns true if this node is an active CVV (Committee Voting Validator).
    ///
    /// This is a helper method that borrows the node mode watch channel
    /// and checks if the node is actively participating in consensus.
    pub fn is_active_cvv(&self) -> bool {
        self.inner.tx_sync_status.borrow().is_active_cvv()
    }

    /// Returns true if this node is an inactive CVV.
    ///
    /// An inactive CVV is a staked node that is catching up to rejoin
    /// the committee after a failure during the epoch.
    pub fn is_cvv_inactive(&self) -> bool {
        self.inner.tx_sync_status.borrow().is_cvv_inactive()
    }

    /// Return the channel for primary network events.
    pub fn primary_network_events(
        &self,
    ) -> &impl TnSender<NetworkEvent<crate::network::Req, crate::network::Res>> {
        &self.inner.primary_network_events
    }

    /// Return the channel for primary network events.  Returns a concrete clone.
    pub fn primary_network_events_cloned(
        &self,
    ) -> QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner.primary_network_events.clone()
    }

    /// New epoch certs as they are recieved.
    /// Any vote on the channel has been verified:
    /// - It is for an EpochRecord we generated and is NOT certified yet.
    /// - It is from a committee member for the epoch (from our EpochRecord).
    /// - It has a valid signature.
    pub fn new_epoch_votes(&self) -> &impl TnSender<EpochVote> {
        &self.inner.new_epoch_votes
    }

    /// Watch channel for the current epoch record.
    /// The epoch vote collector observes this to know when a new epoch starts.
    pub fn epoch_record_watch(&self) -> &watch::Sender<Option<EpochRecord>> {
        &self.inner.tx_epoch_record
    }

    /// Provide a subscriber (Receiver) for new_epoch_votes.
    /// Any vote on the channel has been verified:
    /// - It is for an EpochRecord we generated and is NOT certified yet.
    /// - It is from a committee member for the epoch (from our EpochRecord).
    /// - It has a valid signature.
    pub fn subscribe_new_epoch_votes(&self) -> impl TnReceiver<EpochVote> {
        self.inner.new_epoch_votes.subscribe()
    }

    /// Provide a subscriber (Receiver) for primary network events.
    pub fn subscribe_primary_network_events(
        &self,
    ) -> impl TnReceiver<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner.primary_network_events.subscribe()
    }

    /// Provide a subscriber (Receiver) for primary network events in `epoch`, first discarding
    /// the events that a previous epoch's event loop left queued and that are stale in `epoch`.
    ///
    /// A request that the swarm already ended and a vote request for a header from an earlier
    /// epoch are discarded. Gossip (including the epoch votes that arrive between two epochs'
    /// event loops), epoch-record requests, error events and inbound streams are kept.
    pub fn subscribe_primary_network_events_for_epoch(
        &self,
        epoch: Epoch,
    ) -> impl TnReceiver<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner
            .primary_network_events
            .subscribe_retain(|event| primary_event_is_current(event, epoch))
    }

    /// Provide a subscription (Receiver) for consensus output.
    ///
    /// Outputs a previous subscriber left queued are discarded, so the new subscriber only sees
    /// what its own producer sends. Only one subscription can be live at a time.
    ///
    /// # Panics
    ///
    /// Panics if the previous subscription has not been dropped yet.
    pub fn subscribe_consensus_output(&self) -> impl TnReceiver<ConsensusOutput> {
        self.inner.consensus_output.subscribe_fresh()
    }

    /// Provide a subscription(Receiver) to verified sync consensus outputs.
    ///
    /// Outputs a previous subscriber left queued are discarded, so the new subscriber only sees
    /// what its own producer sends. Only one subscription can be live at a time.
    ///
    /// # Panics
    ///
    /// Panics if the previous subscription has not been dropped yet.
    pub fn subscribe_sync_output(&self) -> impl TnReceiver<ConsensusOutput> {
        self.inner.sync_output.subscribe_fresh()
    }

    /// Broadcast sender for verified certificates (ExEx).
    pub fn exex_certificates(&self) -> &broadcast::Sender<Certificate> {
        &self.inner.exex_certificates
    }

    /// Broadcast sender for the full consensus output (ExEx).
    pub fn exex_consensus_output(&self) -> &broadcast::Sender<ConsensusOutput> {
        &self.inner.exex_consensus_output
    }

    /// Send `value` on `sender` only when at least one ExEx receiver is listening.
    ///
    /// The clone is skipped entirely when no ExEx is registered — the broadcast
    /// payload (a full `ConsensusOutput`) can be large, so this guard keeps the
    /// follow path cheap when nobody is listening.
    fn notify_exex<T: Clone>(sender: &broadcast::Sender<T>, value: &T) {
        if sender.receiver_count() > 0 {
            let _ = sender.send(value.clone());
        }
    }

    /// Notify ExEx subscribers about a verified certificate.
    ///
    /// Called from the consensus-following path (Observer / inactive CVV) after a
    /// gossiped certificate verifies against its committee — never from the
    /// validator hot path.
    pub fn notify_exex_certificate(&self, certificate: &Certificate) {
        Self::notify_exex(&self.inner.exex_certificates, certificate);
    }

    /// Notify ExEx subscribers about a full consensus output.
    ///
    /// Called from the consensus-following path when a `ConsensusHeader` is
    /// reconstructed into a `ConsensusOutput` for execution.
    pub fn notify_exex_consensus_output(&self, output: &ConsensusOutput) {
        Self::notify_exex(&self.inner.exex_consensus_output, output);
    }

    /// Subscribe to verified certificate notifications (ExEx).
    pub fn subscribe_exex_certificates(&self) -> broadcast::Receiver<Certificate> {
        self.inner.exex_certificates.subscribe()
    }

    /// Subscribe to full consensus output notifications (ExEx).
    pub fn subscribe_exex_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        self.inner.exex_consensus_output.subscribe()
    }

    /// Install the canonical execution-DB reader used by [`Self::wait_for_execution`] as a
    /// fallback when a queried tip has been evicted from the `recent_blocks` ring.
    ///
    /// Called once by the node manager after the execution engine is built; a per-epoch engine
    /// rebuild simply refreshes the handle. Bus instances that never wire an engine (tests,
    /// tooling) leave it unset and keep the original ring-only fork check.
    pub fn set_canonical_reader(&self, reader: Arc<dyn CanonicalExecutionReader>) {
        self.inner.canonical_reader.install(reader);
    }

    /// Will resolve once we have executed block.
    ///
    /// Return an error if we do not execute the requested block by block number.
    /// Note if the chain is not advancing this may never return.
    pub async fn wait_for_execution(
        &self,
        block: BlockNumHash,
    ) -> Result<(), WaitForExecutionElapsed> {
        let mut watch_execution_result = self.recent_blocks().subscribe();
        let target_number = block.number;
        // Make sure that our recent blocks is not empty.  If it is we can have a race around block
        // 0.
        while self.recent_blocks().borrow().is_empty() {
            watch_execution_result.changed().await?;
        }
        let mut current_number = self.latest_execution_block_num_hash().number;
        while current_number < target_number {
            watch_execution_result.changed().await?;
            current_number = self.latest_execution_block_num_hash().number;
        }
        if self.recent_blocks().borrow().contains_execution_hash(block.hash) {
            // Once we see our hash, should happen when current_number == target_number- trust
            // digesting for this, we are done.
            Ok(())
        } else if self.inner.canonical_reader.confirms(&block) {
            // Not in the in-memory ring, but the local execution DB holds exactly this block as
            // canonical at its height: the ring merely evicted it (its window is `gc_depth` deep),
            // so this is a stale ring, not a fork.
            Ok(())
        } else {
            // Not in the ring and not confirmed canonical in the DB: a genuine execution fork, or
            // the block is simply not persisted yet. Fail hard.
            Err(WaitForExecutionElapsed())
        }
    }

    /// Will resolve once we have executed the consensus for hash.
    ///
    /// Note if the chain is not advancing this may never return.
    pub async fn wait_for_consensus_execution(
        &self,
        hash: ConsensusHeaderDigest,
    ) -> Result<(), WaitForExecutionElapsed> {
        let mut watch_execution_result = self.recent_blocks().subscribe();
        if self.recent_blocks().borrow().contains_consensus(hash) {
            return Ok(());
        }
        while watch_execution_result.changed().await.is_ok() {
            if self.recent_blocks().borrow().contains_consensus(hash) {
                return Ok(());
            }
        }
        Err(WaitForExecutionElapsed())
    }

    /// Returns the ConsensusHeader that created the last executed block if it can be found.
    /// If we are not starting at genesis or a new epoch, then not finding this indicates a database
    /// issue.
    ///
    /// `Ok(None)` strictly means the header is legitimately absent: the latest executed block
    /// has no `parent_beacon_block_root` (genesis) or the digest is unknown to local storage. A
    /// lookup that FAILS is logged and returned as `Err` - it used to be swallowed into `None`,
    /// which let startup silently resume from a default header at number 0. Under the
    /// epoch-gated seed-signature serde, pre-fork packs stay decodable so the error path should
    /// never fire; it exists so any future format change halts startup loudly instead of
    /// silently corrupting the chain.
    pub async fn last_executed_consensus_block(
        &self,
        consensus_chain: &ConsensusChain,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        let block = self.recent_blocks().borrow().latest_execution_block();
        let header = block.header();
        let (epoch, _) = deconstruct_nonce(header.nonce.into());
        let parent_beacon_block_root = header.parent_beacon_block_root;
        if let Some(consensus_hash) = parent_beacon_block_root {
            consensus_chain
                .consensus_header_by_digest(epoch, consensus_hash.into())
                .await
                .inspect_err(|e| {
                    error!(target: "primary", "failed to load the last executed consensus header from storage: {e}");
                })
        } else {
            // The latest executed block has no parent consensus header (genesis): legitimately
            // absent, not a failure.
            Ok(None)
        }
    }

    /// Returns the ConsensusHeader that was processed.
    /// If we are not starting at genesis or a new epoch, then not finding this indicates a database
    /// issue.
    ///
    /// `Ok(None)` strictly means the header is legitimately absent from local storage; a lookup
    /// that FAILS is logged and returned as `Err` (it used to be swallowed into `None`) so
    /// callers halt or degrade loudly instead of silently resuming from a default header at
    /// number 0. Under the epoch-gated seed-signature serde, pre-fork packs stay decodable so
    /// the error path should never fire; it exists so any future format change halts startup
    /// loudly instead of silently corrupting the chain.
    pub async fn last_consensus_block(
        &self,
        consensus_chain: &ConsensusChain,
    ) -> Result<Option<ConsensusHeader>, ConsensusChainError> {
        let latest_consensus = self.recent_blocks().borrow().latest_consensus_block_num_hash();
        let epoch = consensus_chain.epochs().number_to_epoch(latest_consensus.number);
        consensus_chain
            .consensus_header_by_digest(epoch, latest_consensus.hash)
            .await
            .inspect_err(|e| {
                error!(target: "primary", "failed to load the last processed consensus header from storage: {e}");
            })
    }

    /// Send a request to download the epoch pack file for the provided EpochRecord.
    ///
    /// Returns at once if a request for the same epoch is already queued or being fetched; that
    /// request keeps the record it was queued with, and the fetcher refreshes a stale record from
    /// the epoch records after a failed attempt. Otherwise waits for room in the queue.
    pub async fn request_epoch_pack_file(
        &self,
        previous_epoch_record: EpochRecord,
        epoch_record: EpochRecord,
    ) {
        let Some(claim) = self.inner.epoch_pack_claims.claim(epoch_record.epoch) else {
            return;
        };
        let request = EpochPackRequest {
            previous_epoch_record,
            epoch_record,
            claim,
            metrics: self.inner.metrics.clone(),
        };
        let _ = self.inner.epoch_request_queue_tx.send(request).await;
    }

    /// Put an epoch pack request back on the queue without waiting for room.
    ///
    /// Returns the request when the queue is full so the caller can hold it and offer it again
    /// later. Only the queue's consumers call this: a consumer that waits for room in its own
    /// queue stops the consumption that would make room, and once every consumer waits that way
    /// every producer waits forever (#1563). Producers call [`Self::request_epoch_pack_file`],
    /// which waits.
    ///
    /// A producer already queued as a waiter takes a freed slot before this call can, so a
    /// held request goes back only once the producers stop waiting.
    ///
    /// The request keeps its claim, so no producer can queue a second request for the epoch while
    /// this one is held or queued.
    pub fn try_requeue_epoch_pack_file(
        &self,
        request: EpochPackRequest,
    ) -> Option<EpochPackRequest> {
        match self.inner.epoch_request_queue_tx.try_send(request) {
            Ok(()) => None,
            Err(mpsc::error::TrySendError::Full(request)) => Some(request),
            // cannot happen while the bus exists: the inner struct owns the receiver
            Err(mpsc::error::TrySendError::Closed(_)) => None,
        }
    }

    /// Send a request to download the epoch pack file for the provided Epoch.
    /// Use this when you only have the epoch vs the EpochRecords.
    pub async fn request_epoch_pack_file_by_epoch(
        &self,
        current_epoch: Epoch,
        consensus_chain: &ConsensusChain,
    ) {
        // We are starting a new epoch behind consensus so make sure we have the pack file.
        // If we still have epochs to fetch then add to the queue until we are out of epoch records.
        // For epoch 0: `saturating_sub(1)` yields 0, so record_by_epoch(0) would return the
        // *real* epoch-0 record- use a synthetic epoch record instead.
        let maybe_previous = if current_epoch == 0 {
            consensus_chain.epochs().record_by_epoch(0).await.map(|r| EpochRecord {
                committee: r.committee.clone(),
                next_committee: r.committee.clone(),
                ..EpochRecord::default()
            })
        } else {
            consensus_chain.epochs().record_by_epoch(current_epoch.saturating_sub(1)).await
        };
        let Some(previous_epoch_record) = maybe_previous else {
            info!(
                target: "primary",
                current_epoch,
                "skipping epoch pack request: previous epoch record is missing"
            );
            return;
        };
        let Some(epoch_record) = consensus_chain.epochs().record_by_epoch(current_epoch).await
        else {
            info!(
                target: "primary",
                current_epoch,
                "skipping epoch pack request: epoch record is missing"
            );
            return;
        };
        // If the pack file is missing or incomplete request it.
        // Note since we have an epoch record this is a past epoch
        // not the current epoch.
        if consensus_chain.is_epoch_complete(&epoch_record).await {
            info!(
                target: "primary",
                current_epoch,
                "skipping epoch pack request: epoch pack is already complete"
            );
            return;
        }
        self.request_epoch_pack_file(previous_epoch_record, epoch_record).await;
    }

    /// Retrieve the next request to down load an epoch pack file.
    /// Will not resolve until a request is ready and will only ever
    /// provide each request once.  Returns None when the underlying channel closes.
    /// The epoch stays claimed until the returned request is dropped.
    pub async fn get_next_epoch_pack_file_request(&self) -> Option<EpochPackRequest> {
        self.inner.epoch_request_queue_rx.lock().await.recv().await
    }

    /// Channel to request consensus headers to be fetched and cached.
    pub fn consensus_request_queue(&self) -> &impl TnSender<(Epoch, u64, ConsensusHeaderDigest)> {
        &self.inner.consensus_request_queue
    }

    /// Subscribe to consensus header fetch queue.
    pub fn subscribe_consensus_request_queue(
        &self,
    ) -> impl TnReceiver<(Epoch, u64, ConsensusHeaderDigest)> {
        self.inner.consensus_request_queue.subscribe()
    }
}

impl Default for ConsensusBusApp {
    fn default() -> Self {
        Self::new()
    }
}

/// The thread-safe inner type that holds all the channels for inner-consensus
/// communication between different tasks.
/// These are things that are refreshed each Epoch.
#[derive(Clone, Debug)]
struct ConsensusBusEpochInner {
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    new_certificates: QueChannel<Certificate>,
    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    certificate_fetcher: QueChannel<CertificateFetcherCommand>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    parents: QueChannel<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    our_digests: QueChannel<OurDigestMessage>,
    /// Sends newly created headers to the `Certifier`.
    headers: QueChannel<Header>,
    /// Updates when headers were committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    committed_own_headers: QueChannel<(Round, Vec<Round>)>,

    /// Outputs the sequence of ordered certificates to the application layer.
    sequence: QueChannel<CommittedSubDag>,

    /// Messages to the Certificate Manager.
    certificate_manager: QueChannel<CertificateManagerCommand>,
}

impl ConsensusBusEpochInner {
    fn new() -> Self {
        Self {
            new_certificates: QueChannel::new(),
            certificate_fetcher: QueChannel::new(),
            parents: QueChannel::new(),
            our_digests: QueChannel::new(),
            headers: QueChannel::new(),
            committed_own_headers: QueChannel::new(),
            sequence: QueChannel::new(),
            certificate_manager: QueChannel::new(),
        }
    }
}

/// The type that holds the collection of send/sync channels for
/// inter-task communication during consensus.
#[derive(Clone, Debug)]
pub struct ConsensusBus {
    /// The inner type to make this thread-safe and cheap to own.
    /// This is for stuff that lasts the app lifetime.
    /// Note do not need an Arc because ConsensusBusApp is internally Arced.
    inner_app: ConsensusBusApp,
    /// The inner type to make this thread-safe and cheap to own.
    /// This is for stuff that lasts an epoch lifetime.
    inner_epoch: Arc<ConsensusBusEpochInner>,
}

impl Default for ConsensusBus {
    fn default() -> Self {
        Self::new()
    }
}

/// This contains the shared consensus channels.
/// A new bus can be created with new() but there should only ever be one created (except for
/// tests). This allows us to not create and pass channels all over the place ad-hoc.
/// It also makes it much easier to find where channels are fed and consumed.
impl ConsensusBus {
    /// Create a new consensus bus.
    pub fn new() -> Self {
        // Using default GC depth for blocks to keep in memory.  This should
        // allow for twice the blocks as would be needed for a margin of safety
        // (some testing liked this).  Using the default to not overly complicate
        // creation of the bus.
        // This is basically for testing.
        Self::new_with_args(Parameters::default_gc_depth())
    }

    /// Create a new consensus bus.
    /// Store recent_blocks number of the last generated execution blocks.
    pub fn new_with_args(recent_blocks: u32) -> Self {
        let inner_app = ConsensusBusApp::new_with_recent_blocks(recent_blocks);
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new());
        Self { inner_app, inner_epoch }
    }

    /// Create a new consensus bus.
    /// Store recent_blocks number of the last generated execution blocks.
    pub fn new_with_app(inner_app: ConsensusBusApp) -> Self {
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new());
        Self { inner_app, inner_epoch }
    }

    /// Return a reference to the contained ConsensusBusApp.
    pub fn app(&self) -> &ConsensusBusApp {
        &self.inner_app
    }

    /// New certificates.
    ///
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    /// Can only be subscribed to once.
    pub fn new_certificates(&self) -> &impl TnSender<Certificate> {
        &self.inner_epoch.new_certificates
    }

    /// Missing certificates.
    ///
    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn certificate_fetcher(&self) -> &impl TnSender<CertificateFetcherCommand> {
        &self.inner_epoch.certificate_fetcher
    }

    /// Valid quorum of certificates' ids.
    ///
    /// Sends a valid quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn parents(&self) -> &impl TnSender<(Vec<Certificate>, Round)> {
        &self.inner_epoch.parents
    }

    /// Batches' digests from our workers.
    /// Can only be subscribed to once.
    pub fn our_digests(&self) -> &impl TnSender<OurDigestMessage> {
        &self.inner_epoch.our_digests
    }

    /// Sends newly created headers to the `Certifier`.
    /// Can only be subscribed to once.
    pub fn headers(&self) -> &impl TnSender<Header> {
        &self.inner_epoch.headers
    }

    /// Updates when headers are committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    /// Can only be subscribed to once.
    pub fn committed_own_headers(&self) -> &impl TnSender<(Round, Vec<Round>)> {
        &self.inner_epoch.committed_own_headers
    }

    /// Outputs the sequence of ordered certificates from consensus.
    /// Can only be subscribed to once.
    pub fn sequence(&self) -> &impl TnSender<CommittedSubDag> {
        &self.inner_epoch.sequence
    }

    /// Channel for forwarding newly received certificates for verification.
    ///
    /// These channels are used to communicate with the long-running CertificateManager task.
    /// Can only be subscribed to once.
    pub(crate) fn certificate_manager(&self) -> &impl TnSender<CertificateManagerCommand> {
        &self.inner_epoch.certificate_manager
    }

    /// Returns true if this node is a CVV (active or inactive).
    ///
    /// A CVV is a staked node that can participate in a committee,
    /// regardless of whether it's currently active or catching up.
    pub fn is_cvv(&self) -> bool {
        self.inner_app.is_cvv()
    }

    /// Returns true if this node is an active CVV (Committee Voting Validator).
    ///
    /// This is a helper method that borrows the node mode watch channel
    /// and checks if the node is actively participating in consensus.
    pub fn is_active_cvv(&self) -> bool {
        self.inner_app.is_active_cvv()
    }

    /// Returns true if this node is an inactive CVV.
    ///
    /// An inactive CVV is a staked node that is catching up to rejoin
    /// the committee after a failure during the epoch.
    pub fn is_cvv_inactive(&self) -> bool {
        self.inner_app.is_cvv_inactive()
    }

    //
    //=== Channel subscription methods
    //
    // These must be called in synchronous spawn() methods, BEFORE spawning async tasks.
    // This ensures the channel is active before any messages are sent.
    //

    /// Subscribe to new certificates from the primary.
    pub fn subscribe_new_certificates(&self) -> impl TnReceiver<Certificate> {
        self.inner_epoch.new_certificates.subscribe()
    }

    /// Subscribe to certificates with missing parents from the `Synchronizer`.
    pub fn subscribe_certificate_fetcher(&self) -> impl TnReceiver<CertificateFetcherCommand> {
        self.inner_epoch.certificate_fetcher.subscribe()
    }

    /// Subscribe to a valid quorum of certificates' ids to the `Proposer` (along with their round).
    pub fn subscribe_parents(&self) -> impl TnReceiver<(Vec<Certificate>, Round)> {
        self.inner_epoch.parents.subscribe()
    }

    /// Subscribe to batches' digests from our workers.
    pub fn subscribe_our_digests(&self) -> impl TnReceiver<OurDigestMessage> {
        self.inner_epoch.our_digests.subscribe()
    }

    /// Subscribe to newly created headers.
    pub fn subscribe_headers(&self) -> impl TnReceiver<Header> {
        self.inner_epoch.headers.subscribe()
    }

    /// Subscribe to updates when headers were committed by consensus.
    /// NOTE: this does not mean the header was executed yet.
    pub fn subscribe_committed_own_headers(&self) -> impl TnReceiver<(Round, Vec<Round>)> {
        self.inner_epoch.committed_own_headers.subscribe()
    }

    /// Subscribe to sequence of ordered certificates to the application layer.
    pub fn subscribe_sequence(&self) -> impl TnReceiver<CommittedSubDag> {
        self.inner_epoch.sequence.subscribe()
    }

    /// Subscribe to messages intended for the Certificate Manager.
    pub(crate) fn subscribe_certificate_manager(
        &self,
    ) -> impl TnReceiver<CertificateManagerCommand> {
        self.inner_epoch.certificate_manager.subscribe()
    }
}

/// Error for wait_for_execution().
#[derive(Copy, Clone, Debug)]
pub struct WaitForExecutionElapsed();

impl From<Elapsed> for WaitForExecutionElapsed {
    fn from(_: Elapsed) -> Self {
        Self()
    }
}

impl From<RecvError> for WaitForExecutionElapsed {
    fn from(_: RecvError) -> Self {
        Self()
    }
}

impl Error for WaitForExecutionElapsed {}
impl std::fmt::Display for WaitForExecutionElapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod exex_receiver_count_tests {
    use super::ConsensusBusApp;

    #[test]
    fn exex_senders_have_no_receivers_until_subscribed() {
        // The follow-path send sites guard on `receiver_count() > 0` so they skip
        // the (potentially large) clone+send when no ExEx is registered. That
        // optimization relies on the bus starting with zero ExEx receivers — the
        // initial receivers are dropped at construction.
        let bus = ConsensusBusApp::new();
        assert_eq!(bus.exex_certificates().receiver_count(), 0);
        assert_eq!(bus.exex_consensus_output().receiver_count(), 0);

        // Subscribing (as the ExEx manager does) makes the guards fire.
        let certs = bus.subscribe_exex_certificates();
        let output = bus.subscribe_exex_consensus_output();
        assert_eq!(bus.exex_certificates().receiver_count(), 1);
        assert_eq!(bus.exex_consensus_output().receiver_count(), 1);

        // Dropping the subscriptions (e.g. the non-critical manager dies) returns
        // to zero, so the guards skip work again.
        drop(certs);
        drop(output);
        assert_eq!(bus.exex_certificates().receiver_count(), 0);
        assert_eq!(bus.exex_consensus_output().receiver_count(), 0);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        primary_event_limit, ConsensusBusApp, EpochPackRequest, QueChannel, QueueCharge,
        QueueLimit, CONSENSUS_OUTPUT_CHANNEL_CAPACITY, EPOCH_REQUEST_QUEUE_CAPACITY,
        SYNC_OUTPUT_CHANNEL_CAPACITY,
    };
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::{collections::VecDeque, task::Poll, time::Duration};
    use tn_network_libp2p::ServiceClass;
    use tn_types::{
        CommittedSubDag, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch,
        EpochRecord, TnReceiver as _, TnSender as _, TryRecvError, TrySendError, TrySendOutcome,
        CHANNEL_CAPACITY,
    };

    /// The smallest output that can sit in `sync_output` or `consensus_output`; only its number
    /// matters here.
    fn output(number: u64) -> ConsensusOutput {
        ConsensusOutput::new(
            CommittedSubDag::default(),
            ConsensusHeaderDigest::default(),
            number,
            false,
            VecDeque::new(),
            vec![],
        )
    }

    /// An epoch record that carries only its epoch.
    fn epoch_record(epoch: Epoch) -> EpochRecord {
        EpochRecord { epoch, ..EpochRecord::default() }
    }

    /// Fill the empty epoch request queue with `EPOCH_REQUEST_QUEUE_CAPACITY` epochs from
    /// `first` on, and return the first epoch that no longer fits.
    async fn fill_epoch_request_queue(bus: &ConsensusBusApp, first: Epoch) -> Epoch {
        let capacity =
            Epoch::try_from(EPOCH_REQUEST_QUEUE_CAPACITY).expect("the capacity fits an epoch");
        for epoch in first..first + capacity {
            bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(epoch)).await;
        }
        first + capacity
    }

    /// Take the next epoch pack request, failing the test if none is queued.
    async fn next_request(bus: &ConsensusBusApp) -> EpochPackRequest {
        tokio::time::timeout(Duration::from_secs(1), bus.get_next_epoch_pack_file_request())
            .await
            .expect("a request is queued")
            .expect("the request queue is open")
    }

    /// Take the next epoch pack request and return its epoch.
    async fn next_epoch(bus: &ConsensusBusApp) -> Epoch {
        next_request(bus).await.epoch()
    }

    /// Whether the epoch request queue is empty, checked without waiting.
    async fn epoch_request_queue_is_empty(bus: &ConsensusBusApp) -> bool {
        // unconstrained: once the task's coop budget is spent, the receiver lock reports pending
        // without looking at the queue
        let next = tokio::task::unconstrained(bus.get_next_epoch_pack_file_request());
        tokio::pin!(next);
        futures::poll!(next.as_mut()).is_pending()
    }

    /// A re-queue into the full epoch request queue hands the request back instead of waiting,
    /// and once a request is taken the re-queue fits and drains after everything queued before it.
    #[tokio::test]
    async fn try_requeue_epoch_pack_file_returns_the_request_when_full() {
        let bus = ConsensusBusApp::new();
        // a fetch worker holds epoch 0 while producers fill the queue behind it
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(0)).await;
        let request = next_request(&bus).await;
        let end = fill_epoch_request_queue(&bus, 1).await;

        let request =
            bus.try_requeue_epoch_pack_file(request).expect("a full queue hands the request back");
        assert_eq!(request.epoch(), 0, "the same request comes back");

        assert_eq!(next_epoch(&bus).await, 1);
        assert!(
            bus.try_requeue_epoch_pack_file(request).is_none(),
            "the freed slot takes the re-queued request"
        );
        for expected in 2..end {
            assert_eq!(next_epoch(&bus).await, expected);
        }
        assert_eq!(next_epoch(&bus).await, 0, "the re-queued request drains last");
        assert!(epoch_request_queue_is_empty(&bus).await);
    }

    /// A producer waiting on the full epoch request queue takes the slot a consumer frees before a
    /// re-queue can, so a held request goes back only once the producers stop waiting.
    #[tokio::test]
    async fn waiting_producer_takes_a_freed_slot_before_a_requeue() {
        let bus = ConsensusBusApp::new();
        let next = fill_epoch_request_queue(&bus, 0).await;
        // unconstrained: the fill spends the task's coop budget, and a send polled without budget
        // reports pending before it joins the queue of senders waiting for a slot
        let waiting = tokio::task::unconstrained(
            bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(next)),
        );
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending(), "a producer waits on a full queue");

        let request = next_request(&bus).await;
        assert_eq!(request.epoch(), 0);
        assert!(
            bus.try_requeue_epoch_pack_file(request).is_some(),
            "the waiting producer holds the freed slot"
        );
        assert!(
            futures::poll!(waiting.as_mut()).is_ready(),
            "the waiting producer's send completes"
        );
    }

    /// An epoch is queued once: a second request is refused while the first is queued or held by a
    /// fetch, and the epoch can be queued again once that request is dropped.
    #[tokio::test]
    async fn epoch_pack_request_is_queued_once_until_released() {
        let bus = ConsensusBusApp::new();
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        let request = next_request(&bus).await;
        assert_eq!(request.epoch(), 7);
        assert!(epoch_request_queue_is_empty(&bus).await, "a queued epoch is not queued twice");

        // a fetch worker holds the dequeued request
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        assert!(epoch_request_queue_is_empty(&bus).await, "an epoch being fetched is not queued");

        drop(request);
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        assert_eq!(next_epoch(&bus).await, 7, "a released epoch is queued again");
    }

    /// A re-queued request carries its claim back into the queue, so a producer's request for the
    /// same epoch is refused and the epoch drains once.
    #[tokio::test]
    async fn requeued_epoch_pack_request_keeps_its_claim() {
        let bus = ConsensusBusApp::new();
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        let request = next_request(&bus).await;
        assert!(bus.try_requeue_epoch_pack_file(request).is_none(), "the queue has room");

        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        assert_eq!(next_epoch(&bus).await, 7);
        assert!(epoch_request_queue_is_empty(&bus).await, "the re-queued epoch drains once");
    }

    /// A producer that stops waiting on the full queue releases its epoch, so a later request for
    /// it is queued.
    #[tokio::test]
    async fn cancelled_epoch_pack_send_releases_its_claim() {
        let bus = ConsensusBusApp::new();
        let next = fill_epoch_request_queue(&bus, 0).await;
        // boxed so dropping it drops the send; unconstrained because the fill spends the task's
        // coop budget, and a send polled without budget reports pending before it joins the queue
        // of senders waiting for a slot
        let mut waiting = Box::pin(tokio::task::unconstrained(
            bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(next)),
        ));
        assert!(futures::poll!(waiting.as_mut()).is_pending(), "a producer waits on a full queue");
        tokio::time::timeout(
            Duration::from_secs(1),
            bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(next)),
        )
        .await
        .expect("a request for the epoch the waiting send claimed returns at once");
        drop(waiting);

        assert_eq!(next_epoch(&bus).await, 0);
        tokio::time::timeout(
            Duration::from_secs(1),
            bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(next)),
        )
        .await
        .expect("the freed slot takes the request");
        for expected in 1..next {
            assert_eq!(next_epoch(&bus).await, expected);
        }
        assert_eq!(next_epoch(&bus).await, next, "the cancelled send's epoch is queued again");
        assert!(epoch_request_queue_is_empty(&bus).await);
    }

    /// A fetch that panics while it holds a request still releases the epoch.
    #[tokio::test]
    async fn panicking_fetch_releases_its_claim() {
        let bus = ConsensusBusApp::new();
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        let fetch = tokio::spawn({
            let bus = bus.clone();
            async move {
                let request = bus.get_next_epoch_pack_file_request().await;
                if request.is_some() {
                    panic!("the fetch panics while it holds the request");
                }
            }
        });
        let error = fetch.await.expect_err("the fetch panicked");
        assert!(error.is_panic(), "the fetch ended by panicking");

        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        assert_eq!(next_epoch(&bus).await, 7, "the panicked fetch released its epoch");
    }

    /// A record for another epoch is dropped and counted, while a record for the requested epoch
    /// replaces the queued one.
    #[tokio::test]
    async fn set_epoch_record_drops_and_counts_a_record_for_another_epoch() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // the bus registers its metrics with the recorder that is active while it is built
        let bus = metrics::with_local_recorder(&recorder, ConsensusBusApp::new);
        bus.request_epoch_pack_file(EpochRecord::default(), epoch_record(7)).await;
        let mut request = next_request(&bus).await;
        let queued = request.epoch_record().clone();
        let newer = |epoch| EpochRecord {
            final_consensus: ConsensusNumHash { number: 42, ..Default::default() },
            ..epoch_record(epoch)
        };

        assert!(!request.set_epoch_record(newer(8)), "a record for another epoch is dropped");
        assert_eq!(request.epoch_record(), &queued, "the dropped record leaves the queued one");
        let mismatches =
            snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
                (key.key().name() == "tn_primary.epoch_pack_record_epoch_mismatch_total")
                    .then_some(value)
            });
        assert!(
            matches!(mismatches, Some(DebugValue::Counter(1))),
            "the dropped record is counted once, got {mismatches:?}"
        );

        assert!(request.set_epoch_record(newer(7)), "a record for the requested epoch is applied");
        assert_eq!(request.epoch_record(), &newer(7));
    }

    /// A bounded que channel queues exactly its capacity, then makes the producer wait.
    #[tokio::test]
    async fn que_channel_with_capacity_applies_backpressure() {
        let channel = QueChannel::<u64>::with_capacity(4);
        let mut rx = channel.subscribe();
        for item in 0..4 {
            assert!(channel.send(item).await.is_ok(), "a send within capacity completes");
        }
        assert!(matches!(channel.try_send(4), Err(TrySendError::Full(4))));

        // a fifth send waits for room instead of dropping or overwriting anything
        let fifth = channel.send(4);
        tokio::pin!(fifth);
        assert!(futures::poll!(fifth.as_mut()).is_pending(), "a send into a full queue waits");

        // one recv frees a slot and the waiting send completes
        assert_eq!(rx.recv().await, Some(0));
        assert!(matches!(futures::poll!(fifth.as_mut()), Poll::Ready(Ok(()))));
        for expected in 1..=4 {
            assert_eq!(rx.try_recv(), Ok(expected));
        }
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    /// A burst of three times the capacity against a slow consumer arrives complete and in
    /// order.
    #[tokio::test]
    async fn que_channel_burst_loses_nothing() {
        const CAPACITY: usize = 8;
        const ITEMS: u64 = CAPACITY as u64 * 3;
        let channel = QueChannel::<u64>::with_capacity(CAPACITY);
        let mut rx = channel.subscribe();
        let producer = tokio::spawn({
            let channel = channel.clone();
            async move {
                for item in 0..ITEMS {
                    assert!(channel.send(item).await.is_ok(), "send failed");
                }
            }
        });

        for expected in 0..ITEMS {
            let item = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("the next item arrives")
                .expect("the channel stays open");
            assert_eq!(item, expected);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        producer.await.expect("the producer finishes");
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    /// `sync_output` is a bounded queue: once full it reports `Full`, which a broadcast never
    /// does. A new subscription starts empty even though the previous one left the queue full.
    #[test]
    fn sync_output_is_bounded_backpressured_queue() {
        let bus = ConsensusBusApp::new();
        let rx = bus.subscribe_sync_output();
        for number in 1..=SYNC_OUTPUT_CHANNEL_CAPACITY as u64 {
            assert!(bus.sync_output().try_send(output(number)).is_ok(), "output {number} fits");
        }
        assert!(matches!(
            bus.sync_output().try_send(output(SYNC_OUTPUT_CHANNEL_CAPACITY as u64 + 1)),
            Err(TrySendError::Full(_))
        ));

        drop(rx);
        let mut rx = bus.subscribe_sync_output();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// `consensus_output` is a bounded queue: once full it reports `Full`, a further send waits
    /// for the epoch manager to receive, and every output then arrives in order. A broadcast here
    /// would overwrite the oldest unread outputs instead.
    #[tokio::test]
    async fn consensus_output_is_bounded_backpressured_queue() {
        const CAPACITY: u64 = CONSENSUS_OUTPUT_CHANNEL_CAPACITY as u64;
        let bus = ConsensusBusApp::new();
        let mut rx = bus.subscribe_consensus_output();
        for number in 1..=CAPACITY {
            assert!(
                bus.consensus_output().try_send(output(number)).is_ok(),
                "output {number} fits"
            );
        }
        assert!(matches!(
            bus.consensus_output().try_send(output(CAPACITY + 1)),
            Err(TrySendError::Full(_))
        ));

        // the next send waits for room instead of dropping or overwriting anything
        let next = bus.consensus_output().send(output(CAPACITY + 1));
        tokio::pin!(next);
        assert!(futures::poll!(next.as_mut()).is_pending(), "a send into a full queue waits");

        // one recv frees a slot and the waiting send completes
        assert_eq!(rx.recv().await.map(|received| received.number()), Some(1));
        assert!(matches!(futures::poll!(next.as_mut()), Poll::Ready(Ok(()))));
        for expected in 2..=CAPACITY + 1 {
            assert_eq!(rx.try_recv().map(|received| received.number()), Ok(expected));
        }
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// With no subscriber a `consensus_output` send is a no-op: more sends than the queue holds
    /// all complete without waiting, and a later subscription starts empty.
    #[tokio::test]
    async fn consensus_output_send_without_subscriber_is_noop() {
        let bus = ConsensusBusApp::new();
        for number in 1..=CONSENSUS_OUTPUT_CHANNEL_CAPACITY as u64 + 1 {
            let sent = tokio::time::timeout(
                Duration::from_secs(1),
                bus.consensus_output().send(output(number)),
            )
            .await
            .unwrap_or_else(|_| panic!("send {number} without a subscriber waited"));
            assert!(sent.is_ok(), "send {number} without a subscriber succeeds");
        }

        let mut rx = bus.subscribe_consensus_output();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    }

    /// A new `consensus_output` subscription starts empty: outputs a previous epoch manager left
    /// unread are discarded rather than handed to the next epoch, and what is sent afterwards
    /// arrives.
    #[test]
    fn consensus_output_resubscribe_discards_leftovers() {
        let bus = ConsensusBusApp::new();
        let rx = bus.subscribe_consensus_output();
        assert!(bus.consensus_output().try_send(output(1)).is_ok());
        assert!(bus.consensus_output().try_send(output(2)).is_ok());
        drop(rx);

        let mut rx = bus.subscribe_consensus_output();
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
        assert!(bus.consensus_output().try_send(output(3)).is_ok());
        assert_eq!(rx.try_recv().map(|received| received.number()), Ok(3));
    }

    /// `subscribe_fresh` drops what a previous subscriber left unread and then delivers what is
    /// sent to it.
    #[test]
    fn subscribe_fresh_discards_previous_subscriber_leftovers() {
        let channel = QueChannel::<u64>::with_capacity(4);
        let rx = channel.subscribe();
        assert!(channel.try_send(1).is_ok());
        assert!(channel.try_send(2).is_ok());
        drop(rx);

        let mut rx = channel.subscribe_fresh();
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
        assert!(channel.try_send(3).is_ok());
        assert_eq!(rx.try_recv(), Ok(3));
    }

    /// Plain `subscribe` still hands leftovers to the next subscriber; `primary_network_events`
    /// relies on messages surviving a resubscribe.
    #[test]
    fn subscribe_keeps_previous_subscriber_leftovers() {
        let channel = QueChannel::<u64>::with_capacity(4);
        let rx = channel.subscribe();
        assert!(channel.try_send(1).is_ok());
        assert!(channel.try_send(2).is_ok());
        drop(rx);

        let mut rx = channel.subscribe();
        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    /// A budgeted test message: its class, its byte weight and its epoch.
    type Budgeted = (ServiceClass, usize, Epoch);

    /// Charge a test message its class and byte weight.
    fn budgeted_charge(message: &Budgeted) -> QueueCharge {
        QueueCharge::new(message.0, message.1)
    }

    /// Two messages and 100 bytes for every class.
    fn budgeted_limit(_class: ServiceClass) -> QueueLimit {
        QueueLimit::new(2, 100)
    }

    /// An always-subscribed channel with the test budget.
    fn budgeted_channel() -> QueChannel<Budgeted> {
        QueChannel::new_always_subscribed_budgeted("test", budgeted_charge, budgeted_limit)
    }

    /// What `try_send_outcome` reported for `message`, or `None` for an error.
    fn outcome(channel: &QueChannel<Budgeted>, message: Budgeted) -> Option<TrySendOutcome> {
        channel.try_send_outcome(message).ok()
    }

    /// With no subscriber, each class queues up to its item cap and its byte cap and sheds the
    /// rest as over budget. A class at its caps does not stop another class, so votes and
    /// epoch-record requests still queue while gossip and other traffic are at their caps. A
    /// received message releases its charge.
    #[test]
    fn budget_caps_each_class_by_items_and_bytes() {
        let channel = budgeted_channel();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        // Gossip reaches its item cap.
        assert_eq!(outcome(&channel, (ServiceClass::Gossip, 10, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::Gossip, 10, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::Gossip, 10, 0)), over);

        // Other reaches its byte cap first: 60 + 60 passes 100, 60 + 40 does not.
        assert_eq!(outcome(&channel, (ServiceClass::Other, 60, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::Other, 60, 0)), over);
        assert_eq!(outcome(&channel, (ServiceClass::Other, 40, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::Other, 0, 0)), over);

        // The committee classes keep their own share.
        assert_eq!(outcome(&channel, (ServiceClass::Vote, 100, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::EpochRecord, 10, 0)), queued);

        // Receiving the first gossip releases its charge, so one more gossip fits.
        let mut rx = channel.subscribe();
        assert_eq!(rx.try_recv(), Ok((ServiceClass::Gossip, 10, 0)));
        assert_eq!(outcome(&channel, (ServiceClass::Gossip, 10, 0)), queued);
        assert_eq!(outcome(&channel, (ServiceClass::Gossip, 10, 0)), over);

        // Only the messages reported as queued were queued.
        let classes: Vec<_> =
            std::iter::from_fn(|| rx.try_recv().ok()).map(|message| message.0).collect();
        assert_eq!(
            classes,
            [
                ServiceClass::Gossip,
                ServiceClass::Other,
                ServiceClass::Other,
                ServiceClass::Vote,
                ServiceClass::EpochRecord,
                ServiceClass::Gossip,
            ]
        );
    }

    /// `subscribe_retain` drops the leftovers that `keep` rejects and releases their charge, and
    /// hands the rest to the next subscriber in order: a vote from the previous epoch is
    /// discarded, while gossip sent between the two subscribers survives.
    #[test]
    fn subscribe_retain_discards_rejected_leftovers_and_keeps_the_rest() {
        let channel = budgeted_channel();
        let rx = channel.subscribe();
        assert!(channel.try_send((ServiceClass::Vote, 10, 0)).is_ok());
        assert!(channel.try_send((ServiceClass::Gossip, 10, 1)).is_ok());
        assert!(channel.try_send((ServiceClass::Vote, 10, 1)).is_ok());
        drop(rx);

        let mut rx =
            channel.subscribe_retain(|message| message.0 != ServiceClass::Vote || message.2 >= 1);
        assert_eq!(rx.try_recv(), Ok((ServiceClass::Gossip, 10, 1)));
        assert_eq!(rx.try_recv(), Ok((ServiceClass::Vote, 10, 1)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        // The discarded vote released its charge, so the vote class takes two more.
        assert_eq!(outcome(&channel, (ServiceClass::Vote, 10, 1)), Some(TrySendOutcome::Queued));
        assert_eq!(outcome(&channel, (ServiceClass::Vote, 10, 1)), Some(TrySendOutcome::Queued));
        assert_eq!(
            outcome(&channel, (ServiceClass::Vote, 10, 1)),
            Some(TrySendOutcome::OverBudget)
        );
    }

    /// The item caps of the primary network event queue sum to at most the channel capacity, so
    /// a budgeted send never finds the underlying queue full.
    #[test]
    fn primary_event_item_caps_fit_the_channel() {
        let items: usize =
            ServiceClass::ALL.iter().map(|class| primary_event_limit(*class).items).sum();
        assert!(items <= CHANNEL_CAPACITY, "{items}");
    }
}
