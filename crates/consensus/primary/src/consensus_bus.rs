//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, metrics::PrimaryMetrics,
    proposer::OurDigestMessage, state_sync::CertificateManagerCommand, ExecutionCheck,
    RecentBlocks,
};
use parking_lot::Mutex;
use std::{
    collections::HashSet,
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tn_config::Parameters;
use tn_network_libp2p::types::NetworkEvent;
use tn_storage::consensus::{ConsensusChain, ConsensusChainError};
use tn_types::{
    deconstruct_nonce, BlockNumHash, BlockNumber, CanonicalExecutionReader, Certificate,
    CommittedSubDag, ConsensusHeader, ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord,
    EpochVote, Header, HeaderDigest, Round, TnReceiver, TnSender, CHANNEL_CAPACITY,
};
use tokio::{
    sync::{
        broadcast, mpsc,
        watch::{self, error::RecvError},
    },
    time::{error::Elapsed, Instant},
};
use tracing::{error, info, warn};

/// How long [`ConsensusBusApp::wait_for_execution`] waits before asking the canonical database
/// again after it could not answer ([`ExecutionCheck::Unconfirmed`]).
///
/// Persistence is not signalled on the `recent_blocks` watch, so the wait polls. Short enough
/// that a voter retries several times inside its execution wait.
const EXECUTION_CHECK_RETRY_DELAY: Duration = Duration::from_millis(200);
/// [`ConsensusBusApp::wait_for_execution`] logs its first retry of an unanswered database read and
/// then every this many retries (about every 5 s at [`EXECUTION_CHECK_RETRY_DELAY`]).
const EXECUTION_CHECK_RETRY_WARN_EVERY: u64 = 25;

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
/// Depth of the bounded `uncertified_own_headers` queue.
///
/// The certifier queues one report per own header whose certification a newer own header
/// cancelled, and never waits for room: a full queue drops the report, and the header's digests
/// then wait for the proposer's next own commit as they did before the report existed. A cancel
/// needs a newer header, which the proposer builds in the same loop that drains this queue, so the
/// queue rarely holds more than one report.
pub(crate) const UNCERTIFIED_OWN_HEADERS_CHANNEL_CAPACITY: usize = 64;

/// What an always-subscribed [`QueChannel`] holds while no consumer holds its receiver.
///
/// A live consumer drains the channel as messages arrive, so the channel can fill only in the gap
/// between two consumers and before the first one. The policy names the messages worth holding in
/// that gap and caps them. Each held message has an author, and the policy also caps the messages
/// of each author, so one author cannot take the space of the other authors. The channel drops
/// every other message that arrives in the gap.
///
/// A policy can also keep messages of which only the newest of each author matters (see
/// [`GapWeight::newest`]). The channel keeps those apart from the queue, one for each author, under
/// their own caps, so they and the queued messages cannot take each other's space.
pub(crate) struct GapPolicy<T> {
    /// The channel name for the log.
    name: &'static str,
    /// The weight of a message to hold, or `None` for a message to drop. The second argument is
    /// the epoch of the last consumer, or `None` before the first consumer.
    hold: fn(&T, Option<Epoch>) -> Option<GapWeight>,
    /// The most messages that the channel holds in one gap.
    max_items: usize,
    /// The most bytes that the channel holds in one gap.
    max_bytes: usize,
    /// The most messages of one author that the channel holds in one gap.
    max_per_author: usize,
    /// The most authors whose newest message the channel keeps in one gap.
    max_newest: usize,
    /// The most bytes of newest messages that the channel keeps in one gap.
    max_newest_bytes: usize,
}

impl<T> GapPolicy<T> {
    /// Hold the messages that `hold` weighs, at most `max_items` of them, `max_bytes` bytes and
    /// `max_per_author` messages of one author in one gap.
    pub(crate) fn new(
        name: &'static str,
        hold: fn(&T, Option<Epoch>) -> Option<GapWeight>,
        max_items: usize,
        max_bytes: usize,
        max_per_author: usize,
    ) -> Self {
        Self {
            name,
            hold,
            max_items,
            max_bytes,
            max_per_author,
            max_newest: 0,
            max_newest_bytes: 0,
        }
    }

    /// Also keep the newest message of each author, for the messages that `hold` weighs with
    /// [`GapWeight::newest`]: at most `max_authors` authors and `max_bytes` bytes in one gap,
    /// apart from the caps of the queued messages. Without this the policy keeps none of them.
    pub(crate) fn keep_newest(self, max_authors: usize, max_bytes: usize) -> Self {
        Self { max_newest: max_authors, max_newest_bytes: max_bytes, ..self }
    }

    /// The most messages that the channel holds in one gap.
    #[cfg(test)]
    pub(crate) fn max_items(&self) -> usize {
        self.max_items
    }
}

impl<T> std::fmt::Debug for GapPolicy<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GapPolicy")
            .field("name", &self.name)
            .field("max_items", &self.max_items)
            .field("max_bytes", &self.max_bytes)
            .field("max_per_author", &self.max_per_author)
            .field("max_newest", &self.max_newest)
            .field("max_newest_bytes", &self.max_newest_bytes)
            .finish_non_exhaustive()
    }
}

/// The author of a message that a [`GapPolicy`] holds: the key of the cap per author.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct GapAuthor(Vec<u8>);

impl GapAuthor {
    /// The author with the identity bytes `id`.
    pub(crate) fn new(id: Vec<u8>) -> Self {
        Self(id)
    }
}

/// The weight of a message that a [`GapPolicy`] holds: its byte length and its author.
#[derive(Clone, Debug)]
pub(crate) struct GapWeight {
    /// The byte weight of the message.
    bytes: usize,
    /// The author of the message.
    author: GapAuthor,
    /// The rank of a message that the channel keeps only the newest of for each author, or `None`
    /// for a message that the channel queues.
    rank: Option<u64>,
}

impl GapWeight {
    /// The weight of a message of `bytes` bytes from `author`.
    pub(crate) fn new(bytes: usize, author: GapAuthor) -> Self {
        Self { bytes, author, rank: None }
    }

    /// The weight of a message of `bytes` bytes from `author` of which the channel keeps only the
    /// newest of each author in a gap, apart from the queue.
    ///
    /// The channel keeps one such message for each author. A message of a higher `rank` replaces
    /// the kept one, and a message of a lower or equal rank does not.
    pub(crate) fn newest(bytes: usize, author: GapAuthor, rank: u64) -> Self {
        Self { bytes, author, rank: Some(rank) }
    }
}

/// The newest message of one author that a held [`QueChannel`] keeps apart from the queue in a gap.
#[derive(Debug)]
struct GapNewest<T> {
    /// The rank of the message: a message of a higher rank from the same author replaces it.
    rank: u64,
    /// The byte weight of the message.
    bytes: usize,
    /// The message.
    value: T,
}

/// Whether a consumer holds the receiver of a held [`QueChannel`], and what the channel held and
/// dropped since the last consumer left.
#[derive(Debug)]
struct GapState<T> {
    /// A consumer holds the receiver. Subscribe sets it and the receiver's drop clears it.
    present: bool,
    /// The epoch of the last consumer, or `None` before the first one. The policy uses it to
    /// decide what to hold in the next gap.
    epoch: Option<Epoch>,
    /// Messages held in this gap.
    items: usize,
    /// Bytes held in this gap.
    bytes: usize,
    /// Messages dropped in this gap because the policy does not hold them.
    dropped: usize,
    /// Messages that the policy holds, dropped in this gap at the item cap, the byte cap or the
    /// cap of their author. A kept newest message that a newer one of its author replaced counts
    /// here too.
    over_budget: usize,
    /// Messages held in this gap, per author. An entry exists only for an author with a held
    /// message, so the item cap also caps the entries.
    authors: std::collections::HashMap<GapAuthor, usize>,
    /// The newest message of each author kept apart from the queue in this gap. The next consumer
    /// gets them on subscribe, after the queued ones. The map is sorted by author, so the delivery
    /// order does not depend on a hash: by rank, then by author.
    newest: std::collections::BTreeMap<GapAuthor, GapNewest<T>>,
    /// Bytes of the newest messages kept in this gap.
    newest_bytes: usize,
}

impl<T> Default for GapState<T> {
    fn default() -> Self {
        Self {
            present: false,
            epoch: None,
            items: 0,
            bytes: 0,
            dropped: 0,
            over_budget: 0,
            authors: Default::default(),
            newest: Default::default(),
            newest_bytes: 0,
        }
    }
}

impl<T> GapState<T> {
    /// The state when a consumer of `epoch` subscribes: present, with the gap counters at zero.
    fn with_consumer(epoch: Option<Epoch>) -> Self {
        Self { present: true, epoch, ..Self::default() }
    }

    /// Hold a message of `weight` when it fits the caps of `policy` and the cap of its author,
    /// else count it over budget.
    fn hold(&mut self, weight: GapWeight, policy: &GapPolicy<T>) -> GapAdmit {
        let total = self.bytes.saturating_add(weight.bytes);
        let authored = self.authors.get(&weight.author).copied().unwrap_or_default();
        let fits = self.items < policy.max_items
            && total <= policy.max_bytes
            && authored < policy.max_per_author;
        if fits {
            self.items = self.items.saturating_add(1);
            self.bytes = total;
            self.authors.insert(weight.author.clone(), authored.saturating_add(1));
            GapAdmit::Held(weight)
        } else {
            self.over_budget = self.over_budget.saturating_add(1);
            GapAdmit::OverBudget
        }
    }

    /// Release a held message of `weight`, saturating at zero. Remove its author from the count
    /// when the author has no other held message.
    fn release(&mut self, weight: &GapWeight) {
        self.items = self.items.saturating_sub(1);
        self.bytes = self.bytes.saturating_sub(weight.bytes);
        let left = self.authors.remove(&weight.author).map_or(0, |held| held.saturating_sub(1));
        if left > 0 {
            self.authors.insert(weight.author.clone(), left);
        }
    }

    /// Count a message that the policy does not hold as dropped.
    fn drop_unheld(&mut self) -> GapAdmit {
        self.dropped = self.dropped.saturating_add(1);
        GapAdmit::Unheld
    }

    /// Keep `value` of `weight` and `rank` as the newest message of its author when `rank` is
    /// above the rank of the kept message of that author and it fits the caps of `policy`, else
    /// count it over budget. Return whether the gap kept it.
    ///
    /// An author with no kept message takes a new slot only below the author cap of `policy`.
    fn keep_newest(
        &mut self,
        weight: GapWeight,
        rank: u64,
        value: T,
        policy: &GapPolicy<T>,
    ) -> bool {
        let kept = self.newest.get(&weight.author).map(|kept| (kept.rank, kept.bytes));
        let newer =
            kept.map_or(self.newest.len() < policy.max_newest, |(kept_rank, _)| rank > kept_rank);
        let freed = kept.map_or(0, |(_, bytes)| bytes);
        let total = self.newest_bytes.saturating_sub(freed).saturating_add(weight.bytes);
        if newer && total <= policy.max_newest_bytes {
            // The replaced message of the author is dropped at the cap of its author.
            if kept.is_some() {
                self.over_budget = self.over_budget.saturating_add(1);
            }
            self.newest_bytes = total;
            self.newest.insert(weight.author, GapNewest { rank, bytes: weight.bytes, value });
            true
        } else {
            self.over_budget = self.over_budget.saturating_add(1);
            false
        }
    }

    /// Take the kept newest messages, in the order the next consumer gets them: by rank, and by
    /// author for an equal rank.
    fn take_newest(&mut self) -> impl Iterator<Item = T> {
        self.newest_bytes = 0;
        let mut newest: Vec<_> = std::mem::take(&mut self.newest).into_values().collect();
        // The map iterates by author and the sort is stable, so an equal rank stays by author.
        newest.sort_by_key(|kept| kept.rank);
        newest.into_iter().map(|kept| kept.value)
    }
}

/// The gap policy of a [`QueChannel`] and its state, shared by its clones and its receiver.
#[derive(Debug)]
struct GapHold<T> {
    /// What the channel holds in a gap.
    policy: GapPolicy<T>,
    /// The consumer presence, the gap counters and the kept newest messages.
    state: Mutex<GapState<T>>,
}

impl<T> GapHold<T> {
    /// Start in a gap: no consumer has subscribed yet.
    fn new(policy: GapPolicy<T>) -> Self {
        Self { policy, state: Mutex::new(GapState::default()) }
    }

    /// Decide what the channel does with `value`. With a consumer present, it queues every
    /// message. A message of which the policy keeps only the newest of each author is left to
    /// [`Self::keep_newest`].
    fn admit(&self, value: &T) -> GapAdmit {
        let mut state = self.state.lock();
        if state.present {
            GapAdmit::Open
        } else {
            (self.policy.hold)(value, state.epoch)
                .map(|weight| match weight.rank {
                    Some(rank) => GapAdmit::Newest(weight, rank),
                    None => state.hold(weight, &self.policy),
                })
                .unwrap_or_else(|| state.drop_unheld())
        }
    }

    /// Keep `value` of `weight` and `rank` as the newest message of its author in this gap, and
    /// return whether the gap kept it. Give `value` back when a consumer subscribed after
    /// [`Self::admit`] weighed it, so the caller queues it for that consumer.
    fn keep_newest(&self, weight: GapWeight, rank: u64, value: T) -> Result<bool, T> {
        let mut state = self.state.lock();
        if state.present {
            Err(value)
        } else {
            Ok(state.keep_newest(weight, rank, value, &self.policy))
        }
    }

    /// Release the space and the author share that a held message of `weight` no longer takes.
    fn release(&self, weight: &GapWeight) {
        self.state.lock().release(weight);
    }

    /// Start the turn of a new consumer.
    ///
    /// Discard the queued messages that the policy does not hold: the requests, errors and
    /// streams that the previous consumer left. Put the held ones back in order, then queue the
    /// kept newest messages by rank, mark the consumer present and reset the gap counters. Write
    /// one log with what the gap dropped and what this discards. A held message sent while this
    /// runs can land ahead of the held ones and the newest ones. A message queued for the new
    /// consumer lands after them. The consumer of `epoch` sets the epoch of the next gap. With
    /// `None` the epoch does not change.
    fn enter(&self, rx: &mut mpsc::Receiver<T>, tx: &mpsc::Sender<T>, epoch: Option<Epoch>) {
        let queued: Vec<T> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        // Use the epoch of the gap in which the messages arrived, not the epoch of the new
        // consumer, so the held messages of the last gap stay held.
        let gap_epoch = self.state.lock().epoch;
        let (kept, discarded) =
            queued.into_iter().fold((0_usize, 0_usize), |(kept, discarded), value| {
                if (self.policy.hold)(&value, gap_epoch).is_some() {
                    // The queue was just drained, so it has room for the held messages. Count
                    // one that it still refuses as discarded.
                    tx.try_send(value).map_or((kept, discarded.saturating_add(1)), |()| {
                        (kept.saturating_add(1), discarded)
                    })
                } else {
                    (kept, discarded.saturating_add(1))
                }
            });
        let mut state = self.state.lock();
        let mut gap = std::mem::replace(&mut *state, GapState::with_consumer(epoch.or(gap_epoch)));
        // Queue the newest messages under the lock: a sender that finds the consumer present
        // waits for the lock, so its message lands after them.
        let (kept, discarded) =
            gap.take_newest().fold((kept, discarded), |(kept, discarded), value| {
                tx.try_send(value).map_or((kept, discarded.saturating_add(1)), |()| {
                    (kept.saturating_add(1), discarded)
                })
            });
        drop(state);
        let shed = gap.dropped.saturating_add(gap.over_budget).saturating_add(discarded);
        if shed > 0 {
            warn!(
                target: "primary",
                queue = self.policy.name,
                kept,
                dropped = gap.dropped,
                over_budget = gap.over_budget,
                discarded,
                "queue dropped messages while no consumer was subscribed"
            );
        }
    }

    /// End the turn of the consumer: the next gap starts.
    fn leave(&self) {
        self.state.lock().present = false;
    }
}

/// What the gap policy of a [`QueChannel`] decided for one message.
#[derive(Clone, Debug)]
enum GapAdmit {
    /// The channel has no gap policy, or a consumer holds the receiver: queue the message.
    Open,
    /// The policy holds the message, which has this weight: queue it.
    Held(GapWeight),
    /// The policy keeps only the newest message of the author, and the message has this weight
    /// and rank: keep it apart from the queue with [`GapHold::keep_newest`].
    Newest(GapWeight, u64),
    /// The policy does not hold the message: drop it.
    Unheld,
    /// The gap already holds the item cap, the byte cap or the cap of the message author: drop
    /// the message.
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
    /// The gap policy of the channel, if any. Dropping the receiver starts a gap.
    gap: Option<Arc<GapHold<T>>>,
}

/// Use the Drop to decrement subs and signal unsubscribed.
impl<T> Drop for QueChanReceiver<T> {
    fn drop(&mut self) {
        if !self.always_subscribed {
            self.subscribed.store(false, Ordering::Release);
        }
        self.gap.iter().for_each(|gap| gap.leave());
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
    /// The gap policy, if any: what the channel holds while no consumer holds its receiver.
    gap: Option<Arc<GapHold<T>>>,
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

    /// Create a QueChannel like [`Self::new_always_subscribed`] that applies `policy` while no
    /// consumer holds its receiver.
    ///
    /// In the gap between two consumers, and before the first one, the channel queues only the
    /// messages that `policy` holds, up to its item cap, its byte cap and its cap per author.
    /// `try_send_outcome` reports any other message as [`tn_types::TrySendOutcome::Unsubscribed`]
    /// and a held message past a cap as [`tn_types::TrySendOutcome::OverBudget`]. The messages of
    /// which `policy` keeps only the newest of each author wait apart from the queue, and one that
    /// does not replace the kept message of its author also reports over budget. While a consumer
    /// holds the receiver, the channel queues every message with no extra cap. Each subscribe
    /// discards the queued messages that `policy` does not hold, keeps the held ones in order and
    /// queues the kept newest ones after them.
    pub(crate) fn new_always_subscribed_held(policy: GapPolicy<T>) -> Self {
        Self::build(CHANNEL_CAPACITY, true, Some(Arc::new(GapHold::new(policy))))
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
    /// With a `gap` policy it holds only some messages while no consumer holds the receiver.
    fn build(capacity: usize, always_subscribed: bool, gap: Option<Arc<GapHold<T>>>) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        let subscribed = Arc::new(AtomicBool::new(always_subscribed));
        Self { channel: tx, receiver, subscribed, always_subscribed, gap }
    }

    /// Subscribe to receive messages on this channel.
    ///
    /// Must be called in synchronous `spawn()` methods, BEFORE spawning async tasks.
    /// Can only be called once at a time (returned receiver restores on Drop).
    /// On a channel with a gap policy, this first discards the queued messages that the policy
    /// does not hold (see [`Self::new_always_subscribed_held`]).
    pub fn subscribe(&self) -> impl TnReceiver<T> + 'static
    where
        T: Send + 'static,
    {
        self.subscribe_in_epoch(None)
    }

    /// Subscribe like [`Self::subscribe`] as the consumer of `epoch`.
    ///
    /// On a channel with a gap policy, the policy uses `epoch` to decide what to hold in the gap
    /// after this consumer. With `None` the channel keeps the epoch of the last consumer.
    pub(crate) fn subscribe_in_epoch(&self, epoch: Option<Epoch>) -> impl TnReceiver<T> + 'static
    where
        T: Send + 'static,
    {
        let mut receiver = self.receiver.lock().take();
        let Some(rx) = receiver.as_mut() else { panic!("Another subscription is already in use!") };
        self.gap.iter().for_each(|gap| gap.enter(rx, &self.channel, epoch));
        self.subscribed.store(true, Ordering::Release);
        QueChanReceiver {
            receiver,
            container: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            gap: self.gap.clone(),
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
        let discarded = std::iter::from_fn(|| rx.try_recv().ok()).count();
        if discarded > 0 {
            warn!(target: "primary", discarded, "discarded messages a previous subscriber left queued");
        }
        self.gap.iter().for_each(|gap| gap.enter(rx, &self.channel, None));
        self.subscribed.store(true, Ordering::Release);
        QueChanReceiver {
            receiver,
            container: self.receiver.clone(),
            subscribed: self.subscribed.clone(),
            always_subscribed: self.always_subscribed,
            gap: self.gap.clone(),
        }
    }

    /// What the gap policy, if any, decides for `value`.
    fn admit(&self, value: &T) -> GapAdmit {
        self.gap.as_deref().map_or(GapAdmit::Open, |gap| gap.admit(value))
    }

    /// Keep `value` of `weight` and `rank` as the newest message of its author, and return whether
    /// the gap kept it. Give `value` back when a consumer subscribed since the gap policy weighed
    /// it, or when the channel has no gap policy.
    fn keep_newest(&self, weight: GapWeight, rank: u64, value: T) -> Result<bool, T> {
        match self.gap.as_deref() {
            Some(gap) => gap.keep_newest(weight, rank, value),
            None => Err(value),
        }
    }

    /// Release the hold of a message that the underlying queue did not take.
    fn refund(&self, admit: GapAdmit) {
        match admit {
            GapAdmit::Held(weight) => self.gap.iter().for_each(|gap| gap.release(&weight)),
            GapAdmit::Open | GapAdmit::Newest(..) | GapAdmit::Unheld | GapAdmit::OverBudget => {}
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
            gap: self.gap.clone(),
        }
    }
}

impl<T: Send + 'static> TnSender<T> for QueChannel<T> {
    async fn send(&self, value: T) -> Result<(), tn_types::SendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(());
        }
        let admit = self.admit(&value);
        match admit {
            // Drop a message that the gap policy drops, as for a channel with no subscriber.
            GapAdmit::Unheld | GapAdmit::OverBudget => Ok(()),
            GapAdmit::Newest(weight, rank) => match self.keep_newest(weight, rank, value) {
                Ok(_kept) => Ok(()),
                Err(value) => Ok(self.channel.send(value).await?),
            },
            GapAdmit::Open | GapAdmit::Held(_) => {
                Ok(self.channel.send(value).await.inspect_err(|_| self.refund(admit))?)
            }
        }
    }

    /// Queue `value` without waiting, or drop it while no receiver is subscribed or while the
    /// gap policy drops it.
    fn try_send(&self, value: T) -> Result<(), tn_types::TrySendError<T>> {
        self.try_send_outcome(value).map(|_outcome| ())
    }

    /// Queue `value` like [`TnSender::try_send`], and report
    /// [`tn_types::TrySendOutcome::Unsubscribed`] when no receiver is subscribed or the gap
    /// policy does not hold `value`, and [`tn_types::TrySendOutcome::OverBudget`] when the gap
    /// already holds its item cap, its byte cap or the cap of the author of `value`, so the network
    /// layer counts the drop as shed instead of as forwarded. A message that the gap keeps as the
    /// newest of its author reports [`tn_types::TrySendOutcome::Queued`], and one that does not
    /// replace the kept message of its author reports [`tn_types::TrySendOutcome::OverBudget`].
    fn try_send_outcome(
        &self,
        value: T,
    ) -> Result<tn_types::TrySendOutcome, tn_types::TrySendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(tn_types::TrySendOutcome::Unsubscribed);
        }
        let admit = self.admit(&value);
        match admit {
            GapAdmit::Unheld => Ok(tn_types::TrySendOutcome::Unsubscribed),
            GapAdmit::OverBudget => Ok(tn_types::TrySendOutcome::OverBudget),
            GapAdmit::Newest(weight, rank) => match self.keep_newest(weight, rank, value) {
                Ok(true) => Ok(tn_types::TrySendOutcome::Queued),
                Ok(false) => Ok(tn_types::TrySendOutcome::OverBudget),
                Err(value) => {
                    Ok(self.channel.try_send(value).map(|()| tn_types::TrySendOutcome::Queued)?)
                }
            },
            GapAdmit::Open | GapAdmit::Held(_) => Ok(self
                .channel
                .try_send(value)
                .map(|()| tn_types::TrySendOutcome::Queued)
                .inspect_err(|_| self.refund(admit))?),
        }
    }
}

impl<T: Send + 'static> TnReceiver<T> for QueChanReceiver<T> {
    async fn recv(&mut self) -> Option<T> {
        self.receiver.as_mut().expect("receiver").recv().await
    }

    fn try_recv(&mut self) -> Result<T, tn_types::TryRecvError> {
        Ok(self.receiver.as_mut().expect("receiver").try_recv()?)
    }

    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<T>> {
        self.receiver.as_mut().expect("receiver").poll_recv(cx)
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

/// This node's rejoins in one epoch: how many promotions of its catching-up CVV back to an active
/// CVV failed, and when the latest promotion happened.
///
/// Held in a node-lifetime watch ([`ConsensusBusApp::rejoin_state`]) and keyed by epoch instead of
/// being cleared by [`ConsensusBusApp::reset_for_epoch`]: a promotion restarts the epoch's tasks
/// without changing the epoch, and the restarted tasks must still see it. The first promotion in
/// another epoch starts a new count, and a reader asking about another epoch sees no promotion.
///
/// A promotion is recorded as pending ([`Self::record_promotion`]) and the first demotion after it
/// settles it ([`Self::record_demotion`]): the promotion failed when that demotion came inside
/// [`tn_types::REJOIN_DEMOTION_GRACE`] of it, or was one the grace held until it ended. Only failed
/// promotions count toward [`tn_types::MAX_REJOIN_ATTEMPTS_PER_EPOCH`]; a promotion that holds past
/// the grace never does. A failed promotion on a stalled consensus head is counted apart
/// ([`Self::stalled_failures`]): the cap does not apply to that rule, which backs off instead.
///
/// The watch lives as long as the process. A restart clears the grace, which is right because a
/// restart is not a rejoin, and clears the failure count too, so the per-epoch cap does not span
/// restarts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RejoinState {
    /// The epoch the count and the stamp belong to.
    epoch: Epoch,
    /// Promotions back to an active CVV in `epoch` that a demotion followed inside the grace, the
    /// count the rejoin cap reads. Promotions on a stalled consensus head are not in it.
    failed: u32,
    /// Failed promotions on a stalled consensus head in `epoch`.
    failed_stalled: u32,
    /// When the latest promotion in `epoch` happened, on the tokio clock so a test can pause it.
    promoted_at: Option<Instant>,
    /// True from the latest promotion in `epoch` until the first demotion after it settles it.
    pending: bool,
    /// True when the latest promotion in `epoch` was made on a stalled consensus head.
    stalled: bool,
}

impl RejoinState {
    /// Record a promotion in `epoch` at `now`: stamped `now` and pending until a demotion settles
    /// it. `stalled` marks a promotion on a stalled consensus head. A promotion in an epoch other
    /// than the one held starts a new count.
    pub fn record_promotion(&mut self, epoch: Epoch, now: Instant, stalled: bool) {
        self.enter_epoch(epoch);
        self.promoted_at = Some(now);
        self.pending = true;
        self.stalled = stalled;
    }

    /// Settle the latest promotion in `epoch` after a demotion at `now`: it failed when `now` lies
    /// inside [`tn_types::REJOIN_DEMOTION_GRACE`] of the promotion or `released_hold` says the
    /// grace held this demotion until it ended. A failed promotion counts toward the cap, or
    /// toward [`Self::stalled_failures`] when it was made on a stalled consensus head. Only the
    /// first demotion after a promotion settles it, and a demotion in another epoch settles
    /// nothing. Returns true when the state changed.
    pub fn record_demotion(&mut self, epoch: Epoch, now: Instant, released_hold: bool) -> bool {
        if self.epoch != epoch || !self.pending {
            return false;
        }
        self.pending = false;
        let inside_grace = self.promoted_at.is_some_and(|promoted_at| {
            now.saturating_duration_since(promoted_at) < tn_types::REJOIN_DEMOTION_GRACE
        });
        if !(released_hold || inside_grace) {
            return true;
        }
        if self.stalled {
            self.failed_stalled = self.failed_stalled.saturating_add(1);
        } else {
            self.failed = self.failed.saturating_add(1);
        }
        true
    }

    /// Start a new count for `epoch` unless it is the epoch held, as an inactive node does when it
    /// enters a new epoch. The readers already see a new epoch as zero attempts and no promotion,
    /// so this only makes the reset explicit. A restart inside the held epoch keeps the count, so
    /// the per-epoch cap survives the mode changes between attempts. Returns true when the state
    /// changed.
    pub fn enter_epoch(&mut self, epoch: Epoch) -> bool {
        if self.epoch == epoch {
            return false;
        }
        *self = Self { epoch, ..Self::default() };
        true
    }

    /// The failed promotions recorded in `epoch` that the rejoin cap reads, those on a stalled
    /// consensus head excluded; zero for any other epoch.
    pub fn attempts(&self, epoch: Epoch) -> u32 {
        if self.epoch == epoch {
            self.failed
        } else {
            0
        }
    }

    /// When the latest promotion in `epoch` happened; `None` if there was none in that epoch.
    pub fn promoted_at(&self, epoch: Epoch) -> Option<Instant> {
        if self.epoch == epoch {
            self.promoted_at
        } else {
            None
        }
    }

    /// The failed promotions on a stalled consensus head recorded in `epoch`; zero for any other
    /// epoch. The rejoin cap does not read them; the stalled-head rule backs off by them.
    pub fn stalled_failures(&self, epoch: Epoch) -> u32 {
        if self.epoch == epoch {
            self.failed_stalled
        } else {
            0
        }
    }

    /// True when the latest promotion in `epoch` has not been settled by a demotion yet.
    pub fn is_pending(&self, epoch: Epoch) -> bool {
        self.epoch == epoch && self.pending
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
    /// This node's rejoins in the current epoch (see [`RejoinState`]).
    ///
    /// Not reset between epochs: the state is keyed by epoch.
    tx_rejoin_state: watch::Sender<RejoinState>,

    /// Produce new epoch certs as they are recieved.
    new_epoch_votes: QueChannel<EpochVote>,
    /// Watch channel to communicate the current epoch record to the vote collector.
    tx_epoch_record: watch::Sender<Option<EpochRecord>>,
    /// The newest epoch for which the record collector stored a downloaded, certified epoch
    /// record (`None` until it stores one).
    tx_certified_record_epoch: watch::Sender<Option<Epoch>>,
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

    /// The installed reader, if any.
    ///
    /// The handle is cloned out so the caller reads the database without this lock: never hold a
    /// lock across I/O.
    fn reader(&self) -> Option<Arc<dyn CanonicalExecutionReader>> {
        self.0.lock().clone()
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
        let (tx_rejoin_state, _) = watch::channel(RejoinState::default());

        let sync_output = QueChannel::with_capacity(SYNC_OUTPUT_CHANNEL_CAPACITY);
        let consensus_output = QueChannel::with_capacity(CONSENSUS_OUTPUT_CHANNEL_CAPACITY);

        let (exex_certificates, _) = broadcast::channel(EXEX_CERTIFICATES_CHANNEL_CAPACITY);
        let (exex_consensus_output, _) = broadcast::channel(EXEX_CONSENSUS_OUTPUT_CHANNEL_CAPACITY);

        let (tx_epoch_record, _) = watch::channel(None);
        let (tx_certified_record_epoch, _) = watch::channel(None);

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
                tx_rejoin_state,
                new_epoch_votes: QueChannel::new(),
                tx_epoch_record,
                tx_certified_record_epoch,
                primary_network_events: QueChannel::new_always_subscribed_held(
                    crate::network::gap_policy::primary_event_gap_policy(),
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
    ///
    /// This is the gossip path: f+1 validator signatures move the watch forward and never back. A
    /// certified epoch record is stronger evidence and uses
    /// [`Self::publish_certified_consensus_num_hash`].
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

    /// Publish the `final_consensus` of a CERTIFIED epoch record to the last-published watch.
    ///
    /// A certificate is stronger evidence than f+1 gossip signatures (issue #1771). This moves
    /// the watch forward like [`Self::publish_consensus_num_hash_if_newer`], and it also REPLACES
    /// a stored value that cannot be a real consensus output given the record. Epochs and
    /// consensus numbers advance together, so a stored `(epoch, number)` agrees with the record
    /// only when `stored.epoch <= epoch` and `stored.number <= number` are both true or both
    /// false, and the same `(epoch, number)` must carry the same hash. A stored value that fails
    /// either rule is a gossip pin that f+1 signers fabricated; the certified value replaces it
    /// so gossip-driven tip tracking resumes. Return true if the watch changed.
    pub fn publish_certified_consensus_num_hash(
        &self,
        epoch: Epoch,
        number: u64,
        hash: ConsensusHeaderDigest,
    ) -> bool {
        self.last_published_consensus_num_hash().send_if_modified(|state| {
            let (stored_epoch, stored_number, stored_hash) = *state;
            let newer = number > stored_number;
            let impossible = (stored_epoch <= epoch) != (stored_number <= number);
            let same_slot_other_hash =
                stored_epoch == epoch && stored_number == number && stored_hash != hash;
            if newer || impossible || same_slot_other_hash {
                *state = (epoch, number, hash);
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

    /// This node's rejoins in the current epoch.
    ///
    /// The executor's subscriber records each promotion of a catching-up CVV here before it flips
    /// the node mode; the network handler reads it to hold the activity-window demotion for
    /// [`tn_types::REJOIN_DEMOTION_GRACE`] after a promotion. The network handler settles the
    /// promotion when it demotes the node, and the subscriber settles it, if no demotion site did,
    /// when it starts catching up again (see [`RejoinState::record_demotion`]).
    pub fn rejoin_state(&self) -> &watch::Sender<RejoinState> {
        &self.inner.tx_rejoin_state
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

    /// Watch carrying the newest epoch for which the record collector stored a downloaded,
    /// certified [`EpochRecord`].
    ///
    /// The epoch manager wakes on it to check whether such a record shows that the epoch it runs
    /// already closed on the network. It is a wake-up only: the epoch manager decides from the
    /// epoch store, never from this value.
    pub fn certified_record_epoch(&self) -> &watch::Sender<Option<Epoch>> {
        &self.inner.tx_certified_record_epoch
    }

    /// Publish that the record collector stored a certified record for `epoch`.
    /// Return true if `epoch` is the newest so far; watchers are woken only then.
    pub fn set_certified_record_epoch_if_newer(&self, epoch: Epoch) -> bool {
        self.certified_record_epoch().send_if_modified(|state| {
            if state.is_none_or(|stored| epoch > stored) {
                *state = Some(epoch);
                true
            } else {
                false
            }
        })
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
    ///
    /// First discard the events that the previous epoch's event loop left queued and that the gap
    /// policy does not hold (see [`crate::network::gap_policy`]). The held gossip, including the
    /// epoch votes that arrive between two epochs, stays queued in order. `epoch` is the epoch of
    /// the event loop that subscribes. In the gap after this event loop, the gap policy holds only
    /// the votes for this epoch and the epoch before it.
    pub fn subscribe_primary_network_events(
        &self,
        epoch: Epoch,
    ) -> impl TnReceiver<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner.primary_network_events.subscribe_in_epoch(Some(epoch))
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

    /// Check `block`, an execution block a header names, against this node's execution without
    /// waiting (see [`ExecutionCheck`]).
    ///
    /// The `recent_blocks` ring answers first, comparing the number and the hash. A miss at or
    /// below the executed tip goes to the canonical database, with the restored-state floor as the
    /// lower bound: the ring keeps only the final block of each recent output, and its window is
    /// `gc_depth` deep. The watch borrow is released before the database read.
    pub fn check_execution_block(&self, block: BlockNumHash) -> ExecutionCheck {
        let ring = self.recent_blocks().borrow().check_execution_block(block);
        ring.unwrap_or_else(|| {
            ExecutionCheck::from_canonical(block, self.inner.canonical_reader.reader().as_deref())
        })
    }

    /// Will resolve once execution has reached `block` and the check matched it.
    ///
    /// This is the one execution check the voter, the subscriber and state-sync make (see
    /// [`Self::check_execution_block`]). It waits while the check is [`ExecutionCheck::NotYet`],
    /// for the next executed output, and while it is [`ExecutionCheck::Unconfirmed`], asking the
    /// database again every [`EXECUTION_CHECK_RETRY_DELAY`]; each retry counts in
    /// `tn_primary.execution_check_retries_total`, and the first and every
    /// [`EXECUTION_CHECK_RETRY_WARN_EVERY`]th log a warning. Note if the chain is not advancing
    /// this may never return.
    ///
    /// # Errors
    ///
    /// [`ExecutionCheckError::Fork`] when this node holds a different block at the number,
    /// [`ExecutionCheckError::BelowFloor`] when the number is below this node's restored-state
    /// floor (it cannot check the block; not a fork) and [`ExecutionCheckError::Closed`] when the
    /// `recent_blocks` watch closes.
    pub async fn wait_for_execution(&self, block: BlockNumHash) -> Result<(), ExecutionCheckError> {
        let mut watch_execution_result = self.recent_blocks().subscribe();
        let mut retries: u64 = 0;
        loop {
            match self.check_execution_block(block) {
                ExecutionCheck::Match => return Ok(()),
                ExecutionCheck::Fork => return Err(ExecutionCheckError::Fork),
                ExecutionCheck::BelowFloor { floor } => {
                    return Err(ExecutionCheckError::BelowFloor { floor })
                }
                ExecutionCheck::NotYet => watch_execution_result.changed().await?,
                ExecutionCheck::Unconfirmed => {
                    // the ring is pushed only after a block is persisted, so a database that
                    // cannot answer at or below the tip is failing reads: keep asking, visibly
                    retries = retries.saturating_add(1);
                    self.metrics().execution_check_retries_total.increment(1);
                    if retries == 1 || retries.is_multiple_of(EXECUTION_CHECK_RETRY_WARN_EVERY) {
                        warn!(
                            target: "primary",
                            block_number = block.number,
                            block_hash = %block.hash,
                            retries,
                            "execution block not readable from the canonical db; retrying"
                        );
                    }
                    tokio::time::sleep(EXECUTION_CHECK_RETRY_DELAY).await
                }
            }
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
        self.with_last_consensus_block(consensus_chain, ConsensusHeader::clone).await
    }

    /// `f` of the ConsensusHeader that was processed (see [`Self::last_consensus_block`], whose
    /// absence and errors this shares), without copying the header out: for a caller that needs
    /// a few of its fields on a hot path, such as the check for falling behind on every gossiped
    /// certificate.
    pub async fn with_last_consensus_block<R>(
        &self,
        consensus_chain: &ConsensusChain,
        f: impl FnOnce(&ConsensusHeader) -> R,
    ) -> Result<Option<R>, ConsensusChainError> {
        let latest_consensus = self.recent_blocks().borrow().latest_consensus_block_num_hash();
        let epoch = consensus_chain.epochs().number_to_epoch(latest_consensus.number);
        consensus_chain
            .with_consensus_header_by_digest(epoch, latest_consensus.hash, f)
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
    /// Own headers that will never be certified because a newer own header cancelled their vote
    /// collection, as `(round, digest)`, from the `Certifier` to the `Proposer`.
    ///
    /// Bounded by [`UNCERTIFIED_OWN_HEADERS_CHANNEL_CAPACITY`]; the certifier only `try_send`s.
    uncertified_own_headers: QueChannel<(Round, HeaderDigest)>,

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
            uncertified_own_headers: QueChannel::with_capacity(
                UNCERTIFIED_OWN_HEADERS_CHANNEL_CAPACITY,
            ),
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

    /// Reports own headers whose certification a newer own header cancelled, as
    /// `(round, digest)`, so the `Proposer` can put their batch digests back in its queue.
    ///
    /// Bounded: send with `try_send` and drop the report when the queue is full.
    /// Can only be subscribed to once.
    pub(crate) fn uncertified_own_headers(&self) -> &impl TnSender<(Round, HeaderDigest)> {
        &self.inner_epoch.uncertified_own_headers
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

    /// Subscribe to the own headers whose certification a newer own header cancelled.
    pub(crate) fn subscribe_uncertified_own_headers(
        &self,
    ) -> impl TnReceiver<(Round, HeaderDigest)> {
        self.inner_epoch.uncertified_own_headers.subscribe()
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

/// Why [`ConsensusBusApp::wait_for_execution`] did not confirm an execution block.
#[derive(Copy, Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ExecutionCheckError {
    /// This node holds a different block at the number: execution has forked.
    #[error("execution fork: the local chain holds a different block at this number")]
    Fork,
    /// The number is below this node's restored-state floor, so this node cannot check the
    /// block. Not a fork.
    #[error("execution block is below the restored-state floor {floor}")]
    BelowFloor {
        /// The node's restored-state floor.
        floor: BlockNumber,
    },
    /// The `recent_blocks` watch closed before the check finished.
    #[error("the recent blocks watch closed")]
    Closed,
}

impl From<RecvError> for ExecutionCheckError {
    fn from(_: RecvError) -> Self {
        Self::Closed
    }
}

/// Error for wait_for_consensus_execution().
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
        ConsensusBusApp, EpochPackRequest, GapAdmit, GapAuthor, GapHold, GapPolicy, GapWeight,
        QueChannel, CONSENSUS_OUTPUT_CHANNEL_CAPACITY, EPOCH_REQUEST_QUEUE_CAPACITY,
        SYNC_OUTPUT_CHANNEL_CAPACITY,
    };
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::{collections::VecDeque, task::Poll, time::Duration};
    use tn_types::{
        CommittedSubDag, ConsensusHeaderDigest, ConsensusNumHash, ConsensusOutput, Epoch,
        EpochRecord, TnReceiver as _, TnSender as _, TryRecvError, TrySendError, TrySendOutcome,
        B256,
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

    /// A consensus header digest that differs per `byte`.
    fn digest(byte: u8) -> ConsensusHeaderDigest {
        ConsensusHeaderDigest::from(B256::repeat_byte(byte))
    }

    /// The issue #1771 pin: f+1 gossip signers set a number near `u64::MAX`, and the next
    /// certified epoch record replaces it even though its number is lower.
    #[tokio::test]
    async fn certified_publish_replaces_gossip_pin() {
        let bus = ConsensusBusApp::new();
        assert!(bus.publish_consensus_num_hash_if_newer(0, u64::MAX - 1, digest(1)));
        assert!(
            bus.publish_certified_consensus_num_hash(3, 100, digest(3)),
            "a certified record must replace a pin its epoch proves impossible"
        );
        assert_eq!(bus.published_consensus_num_hash(), (3, 100, digest(3)));
    }

    /// A tip that agrees with a certified record is kept: a later epoch at a greater number is
    /// what honest gossip produces after the record's epoch closed.
    #[tokio::test]
    async fn certified_publish_keeps_consistent_later_tip() {
        let bus = ConsensusBusApp::new();
        assert!(bus.publish_consensus_num_hash_if_newer(5, 1000, digest(5)));
        assert!(
            !bus.publish_certified_consensus_num_hash(4, 900, digest(4)),
            "a consistent later tip must not move back to an older certified record"
        );
        assert_eq!(bus.published_consensus_num_hash(), (5, 1000, digest(5)));
    }

    /// From the initial state a certified record moves the watch forward exactly like the gossip
    /// path, and publishing the same record again changes nothing.
    #[tokio::test]
    async fn certified_publish_moves_forward_once() {
        let bus = ConsensusBusApp::new();
        assert!(bus.publish_certified_consensus_num_hash(2, 50, digest(2)));
        assert_eq!(bus.published_consensus_num_hash(), (2, 50, digest(2)));
        assert!(
            !bus.publish_certified_consensus_num_hash(2, 50, digest(2)),
            "the same certified value must not report a change"
        );
    }

    /// A stored claim of a later epoch at a number the certified record proves belongs to its own
    /// epoch is replaced: consensus numbers never stand still across an epoch boundary.
    #[tokio::test]
    async fn certified_publish_replaces_later_epoch_at_record_number() {
        let bus = ConsensusBusApp::new();
        assert!(bus.publish_consensus_num_hash_if_newer(9, 100, digest(9)));
        assert!(bus.publish_certified_consensus_num_hash(4, 100, digest(4)));
        assert_eq!(bus.published_consensus_num_hash(), (4, 100, digest(4)));
    }

    /// A stored value at the certified record's own `(epoch, number)` but with another hash is
    /// replaced by the certified hash.
    #[tokio::test]
    async fn certified_publish_replaces_same_slot_other_hash() {
        let bus = ConsensusBusApp::new();
        assert!(bus.publish_consensus_num_hash_if_newer(4, 100, digest(7)));
        assert!(bus.publish_certified_consensus_num_hash(4, 100, digest(4)));
        assert_eq!(bus.published_consensus_num_hash(), (4, 100, digest(4)));
    }

    /// A gap test message: whether the test policy holds it, and its byte weight.
    type Gap = (bool, usize);

    /// Hold the messages marked held, at their byte weight, all from one author.
    fn gap_hold(message: &Gap, _: Option<Epoch>) -> Option<GapWeight> {
        message.0.then(|| GapWeight::new(message.1, GapAuthor::new(vec![0])))
    }

    /// An always-subscribed channel that holds at most two messages and 100 bytes in a gap. The
    /// cap per author is above the item cap, so it does not limit these messages.
    fn gap_channel() -> QueChannel<Gap> {
        QueChannel::new_always_subscribed_held(GapPolicy::new("test", gap_hold, 2, 100, 3))
    }

    /// What `try_send_outcome` reported for `message`, or `None` for an error.
    fn outcome(channel: &QueChannel<Gap>, message: Gap) -> Option<TrySendOutcome> {
        channel.try_send_outcome(message).ok()
    }

    /// With no consumer, the channel queues the messages that the policy holds and drops the
    /// rest as unsubscribed.
    #[test]
    fn gap_queues_only_held_messages() {
        let channel = gap_channel();
        assert_eq!(outcome(&channel, (true, 10)), Some(TrySendOutcome::Queued));
        assert_eq!(outcome(&channel, (false, 10)), Some(TrySendOutcome::Unsubscribed));

        let mut rx = channel.subscribe();
        assert_eq!(rx.try_recv(), Ok((true, 10)));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    /// With no consumer, a held message past the item cap or the byte cap reports over budget.
    #[test]
    fn gap_caps_held_messages_by_items_and_bytes() {
        let channel = gap_channel();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        // The byte cap: 60 + 60 passes 100, 60 + 40 does not.
        assert_eq!(outcome(&channel, (true, 60)), queued);
        assert_eq!(outcome(&channel, (true, 60)), over);
        assert_eq!(outcome(&channel, (true, 40)), queued);
        // The item cap: two messages are held, so a third with no bytes is over budget.
        assert_eq!(outcome(&channel, (true, 0)), over);
        // A message that the policy does not hold is still unsubscribed.
        assert_eq!(outcome(&channel, (false, 0)), Some(TrySendOutcome::Unsubscribed));
    }

    /// With a consumer subscribed, the channel queues every message with no cap.
    #[test]
    fn consumer_present_queues_every_message() {
        let channel = gap_channel();
        let mut rx = channel.subscribe();
        (0..5).for_each(|_| {
            assert_eq!(outcome(&channel, (false, 60)), Some(TrySendOutcome::Queued));
            assert_eq!(outcome(&channel, (true, 60)), Some(TrySendOutcome::Queued));
        });
        assert_eq!(std::iter::from_fn(|| rx.try_recv().ok()).count(), 10);
    }

    /// Subscribe discards the leftovers that the policy does not hold, keeps the held ones in
    /// order and resets the gap counters. Dropping the receiver starts a new gap.
    #[test]
    fn subscribe_discards_unheld_leftovers_and_resets_the_gap() {
        let channel = gap_channel();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        // The consumer leaves these two queued.
        let rx = channel.subscribe();
        assert_eq!(outcome(&channel, (false, 1)), queued);
        assert_eq!(outcome(&channel, (true, 2)), queued);
        drop(rx);

        // In the gap, held messages queue up to the caps and the rest drop.
        assert_eq!(outcome(&channel, (true, 3)), queued);
        assert_eq!(outcome(&channel, (false, 4)), Some(TrySendOutcome::Unsubscribed));
        assert_eq!(outcome(&channel, (true, 5)), queued);
        assert_eq!(outcome(&channel, (true, 6)), over);

        let mut rx = channel.subscribe();
        let kept: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(kept, [(true, 2), (true, 3), (true, 5)]);
        drop(rx);

        // The next gap starts with its counters at zero, so it holds two more.
        assert_eq!(outcome(&channel, (true, 7)), queued);
        assert_eq!(outcome(&channel, (true, 8)), queued);
        assert_eq!(outcome(&channel, (true, 9)), over);
    }

    /// A gap test message with an author: the author id and the byte weight.
    type Authored = (u8, usize);

    /// Hold every authored message, at its byte weight.
    fn authored_hold(message: &Authored, _: Option<Epoch>) -> Option<GapWeight> {
        Some(GapWeight::new(message.1, GapAuthor::new(vec![message.0])))
    }

    /// A gap policy that holds at most ten messages, 100 bytes and two messages of one author.
    fn authored_policy() -> GapPolicy<Authored> {
        GapPolicy::new("test", authored_hold, 10, 100, 2)
    }

    /// With no consumer, a held message past the cap of its author reports over budget, and the
    /// messages of another author still queue.
    #[test]
    fn gap_caps_held_messages_per_author() {
        let channel = QueChannel::new_always_subscribed_held(authored_policy());
        let outcome = |message: Authored| channel.try_send_outcome(message).ok();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), over);
        assert_eq!(outcome((2, 1)), queued);
        assert_eq!(outcome((2, 1)), queued);
        assert_eq!(outcome((2, 1)), over);
    }

    /// Subscribe resets the count of each author, so the next gap holds the full share of an
    /// author again.
    #[test]
    fn subscribe_resets_the_author_counts() {
        let channel = QueChannel::new_always_subscribed_held(authored_policy());
        let outcome = |message: Authored| channel.try_send_outcome(message).ok();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), over);

        let mut rx = channel.subscribe();
        assert_eq!(std::iter::from_fn(|| rx.try_recv().ok()).count(), 2);
        drop(rx);

        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), queued);
        assert_eq!(outcome((1, 1)), over);
    }

    /// Release frees the share of the author of a held message, so the refund after a failed
    /// send does not count against that author.
    #[test]
    fn release_frees_the_author_share() {
        let hold = GapHold::new(authored_policy());
        assert!(matches!(hold.admit(&(1, 1)), GapAdmit::Held(_)));
        assert!(matches!(hold.admit(&(1, 1)), GapAdmit::Held(_)));
        assert!(matches!(hold.admit(&(1, 1)), GapAdmit::OverBudget));

        hold.release(&GapWeight::new(1, GapAuthor::new(vec![1])));
        assert!(matches!(hold.admit(&(1, 1)), GapAdmit::Held(_)));
        assert!(matches!(hold.admit(&(1, 1)), GapAdmit::OverBudget));
    }

    /// A gap test message: its rank, or `None` for a message to queue, its author and its byte
    /// weight.
    type Ranked = (Option<u64>, u8, usize);

    /// Keep the newest ranked message of each author, and hold the others in the queue.
    fn ranked_hold(message: &Ranked, _: Option<Epoch>) -> Option<GapWeight> {
        let (rank, author, bytes) = *message;
        let author = GapAuthor::new(vec![author]);
        Some(match rank {
            Some(rank) => GapWeight::newest(bytes, author, rank),
            None => GapWeight::new(bytes, author),
        })
    }

    /// A gap policy that holds at most two queued messages and keeps the newest message of at
    /// most two authors and 25 bytes.
    fn ranked_policy() -> GapPolicy<Ranked> {
        GapPolicy::new("test", ranked_hold, 2, 100, 2).keep_newest(2, 25)
    }

    /// With no consumer, the channel keeps the newest message of each author apart from the
    /// queue: a higher rank replaces the kept message, a lower or equal rank does not, and the
    /// author cap, the byte cap and the queue caps each hold on their own. Subscribe queues the
    /// kept messages by rank after the held ones, and a message sent after subscribe queues.
    #[test]
    fn gap_keeps_the_newest_message_of_each_author() {
        let channel = QueChannel::new_always_subscribed_held(ranked_policy());
        let outcome = |message: Ranked| channel.try_send_outcome(message).ok();
        let queued = Some(TrySendOutcome::Queued);
        let over = Some(TrySendOutcome::OverBudget);

        assert_eq!(outcome((Some(5), 1, 10)), queued);
        assert_eq!(outcome((Some(4), 1, 10)), over, "a lower rank does not replace");
        assert_eq!(outcome((Some(5), 1, 10)), over, "an equal rank does not replace");
        assert_eq!(outcome((Some(6), 1, 10)), queued, "a higher rank replaces");
        assert_eq!(outcome((Some(1), 2, 16)), over, "26 bytes pass the byte cap");
        assert_eq!(outcome((Some(1), 2, 15)), queued);
        assert_eq!(outcome((Some(9), 3, 0)), over, "a third author passes the author cap");
        assert_eq!(outcome((Some(7), 1, 11)), over, "the replacement passes the byte cap");
        assert_eq!(outcome((None, 1, 10)), queued);
        assert_eq!(outcome((None, 2, 10)), queued);
        assert_eq!(outcome((None, 3, 10)), over, "the queue keeps its own item cap");

        let mut rx = channel.subscribe();
        let kept: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(kept, [(None, 1, 10), (None, 2, 10), (Some(1), 2, 15), (Some(6), 1, 10)]);

        assert_eq!(outcome((Some(1), 2, 15)), queued);
        assert_eq!(rx.try_recv(), Ok((Some(1), 2, 15)));
    }

    /// Subscribe empties the kept newest messages, so the next gap keeps the full caps again.
    #[test]
    fn subscribe_resets_the_newest_messages() {
        let channel = QueChannel::new_always_subscribed_held(ranked_policy());
        let outcome = |message: Ranked| channel.try_send_outcome(message).ok();
        let queued = Some(TrySendOutcome::Queued);

        assert_eq!(outcome((Some(5), 1, 10)), queued);
        assert_eq!(outcome((Some(5), 2, 10)), queued);
        drop(channel.subscribe());

        assert_eq!(outcome((Some(1), 3, 10)), queued, "a lower rank than the last gap's");
        assert_eq!(outcome((Some(1), 4, 15)), queued);
    }

    /// A newest message that a consumer subscribed after the weighing of goes back to the
    /// caller, to queue for that consumer. A policy without newest caps keeps none.
    #[test]
    fn newest_message_weighed_before_subscribe_goes_back() {
        let hold = GapHold::new(ranked_policy());
        let GapAdmit::Newest(weight, rank) = hold.admit(&(Some(1), 1, 10)) else {
            panic!("a ranked message is kept apart from the queue")
        };
        hold.state.lock().present = true;
        assert_eq!(hold.keep_newest(weight, rank, (Some(1), 1, 10)), Err((Some(1), 1, 10)));

        let channel =
            QueChannel::new_always_subscribed_held(GapPolicy::new("test", ranked_hold, 2, 100, 2));
        assert_eq!(
            channel.try_send_outcome((Some(1), 1, 0)).ok(),
            Some(TrySendOutcome::OverBudget)
        );
    }

    // bench5 f3-rejoin

    /// The rejoin state counts failed promotions per epoch: a promotion in another epoch starts a
    /// new count, a reader asking about another epoch sees none, and `reset_for_epoch` keeps it.
    #[test]
    fn rejoin_state_is_keyed_by_epoch() {
        use super::RejoinState;

        let start = tokio::time::Instant::now();
        let later = start + Duration::from_secs(1);
        let mut state = RejoinState::default();
        assert_eq!((state.attempts(4), state.promoted_at(4)), (0, None));

        state.record_promotion(4, start, false);
        assert!(state.record_demotion(4, later, false), "a demotion inside the grace fails it");
        state.record_promotion(4, later, false);
        assert!(state.record_demotion(4, later, false));
        assert_eq!((state.attempts(4), state.promoted_at(4)), (2, Some(later)));
        assert_eq!((state.attempts(3), state.promoted_at(3)), (0, None), "another epoch sees none");

        state.record_promotion(5, start, false);
        assert_eq!((state.attempts(5), state.promoted_at(5)), (0, Some(start)), "a new count");
        assert_eq!((state.attempts(4), state.promoted_at(4)), (0, None), "the old count is gone");

        let bus = ConsensusBusApp::new();
        bus.rejoin_state().send_modify(|state| {
            state.record_promotion(4, start, false);
            state.record_demotion(4, start, false);
        });
        bus.reset_for_epoch();
        assert_eq!(bus.rejoin_state().borrow().attempts(4), 1, "reset_for_epoch keeps it");
    }

    /// Only a promotion that a demotion follows inside the rejoin grace counts: one that holds
    /// past the grace never does, unless the demotion is one the grace held until it ended. A
    /// promotion is settled once, by the first demotion after it, and a demotion in another epoch
    /// or without a pending promotion changes nothing.
    #[test]
    fn rejoin_state_counts_only_failed_promotions() {
        use super::RejoinState;
        use tn_types::REJOIN_DEMOTION_GRACE;

        let start = tokio::time::Instant::now();
        let mut state = RejoinState::default();
        assert!(!state.record_demotion(4, start, false), "nothing to settle without a promotion");

        // demoted a moment before the grace ends: failed
        state.record_promotion(4, start, false);
        assert!(state.is_pending(4));
        let just_inside = start + REJOIN_DEMOTION_GRACE - Duration::from_millis(1);
        assert!(state.record_demotion(4, just_inside, false));
        assert!(!state.is_pending(4), "the demotion settles the promotion");
        assert_eq!(state.attempts(4), 1);
        assert!(!state.record_demotion(4, just_inside, false), "settled once");
        assert_eq!(state.attempts(4), 1);

        // held past the grace, then demoted for something else: not failed
        state.record_promotion(4, start, false);
        assert!(state.record_demotion(4, start + REJOIN_DEMOTION_GRACE, false));
        assert_eq!(state.attempts(4), 1, "a promotion that outlasts the grace does not count");

        // a demotion the grace held until it ended: failed
        state.record_promotion(4, start, false);
        let after = start + REJOIN_DEMOTION_GRACE + Duration::from_millis(50);
        assert!(state.record_demotion(4, after, true));
        assert_eq!(state.attempts(4), 2, "a released hold counts");

        // a failed promotion on a stalled head is counted apart from the cap
        state.record_promotion(4, start, true);
        assert!(state.record_demotion(4, start, false));
        assert_eq!((state.attempts(4), state.stalled_failures(4)), (2, 1));
        assert_eq!(state.stalled_failures(5), 0, "another epoch sees none");

        // a demotion in another epoch settles nothing
        state.record_promotion(4, start, false);
        assert!(!state.record_demotion(5, start, false));
        assert!(state.is_pending(4));
        assert_eq!(state.attempts(4), 2);
    }
}
