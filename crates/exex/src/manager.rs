//! ExEx Manager — fans out lifecycle notifications to all registered ExExes.
//!
//! The manager subscribes to 3 sources:
//! 1. Verified certificates (from ConsensusBus, consensus-following path)
//! 2. Full consensus outputs (from ConsensusBus, consensus-following path)
//! 3. Canonical state notifications (from reth's BlockchainProvider)
//!
//! It wraps each into the appropriate [`TnExExNotification`] variant and fans out
//! to all registered ExExes using non-blocking `try_send`.
//!
//! # Delivery contract
//!
//! Fan-out never blocks: a slow ExEx that fills its bounded channel has
//! notifications dropped rather than back-pressuring the node. Drops are not
//! silent — the next successful delivery is preceded by a
//! [`TnExExNotification::Lagged`] marker carrying a best-effort count, so the
//! ExEx can detect the gap and reconcile via [`replay`](crate::replay).

use crate::{Chain, TnExExEvent, TnExExNotification};
use futures::stream::{
    self, select_all, select_with_strategy, PollNext, Stream, StreamExt, TryStreamExt,
};
use reth_provider::CanonStateNotification;
use std::sync::Arc;
use tn_reth::CanonStateNotificationStream;
use tn_types::{BlockNumber, Certificate, ConsensusOutput};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream, ReceiverStream};
use tracing::{debug, error, warn};

/// Default notification channel capacity per ExEx.
///
/// Operators can override this per-ExEx via `TnBuilder::install_exex_with_capacity`
/// — a heavyweight indexer on a high-throughput chain may need a wider buffer to
/// avoid a `Lagged` → replay → fall-behind catch-up thrash loop.
const EXEX_CHANNEL_CAPACITY: usize = 256;

/// Per-ExEx delivery state: the notification sender, lag tracking, and the event
/// receiver used for `FinishedHeight` reporting.
struct ExExHandle {
    /// ExEx name (for logging).
    name: String,
    /// Bounded, non-blocking notification sender.
    notifications: mpsc::Sender<TnExExNotification>,
    /// Highest durably-processed height reported by the ExEx, if any.
    finished_height: Option<BlockNumber>,
    /// Notifications dropped since the last successful delivery.
    ///
    /// When non-zero, the next successful send is preceded by a
    /// [`TnExExNotification::Lagged`] marker so the ExEx can detect the gap.
    dropped: u64,
}

impl ExExHandle {
    fn new(name: String, notifications: mpsc::Sender<TnExExNotification>) -> Self {
        Self { name, notifications, finished_height: None, dropped: 0 }
    }

    /// Deliver a notification without ever blocking.
    ///
    /// If notifications were previously dropped, a [`TnExExNotification::Lagged`]
    /// marker is delivered first so the ExEx can reconcile. If the channel is
    /// still full, the drop count is accumulated and surfaced later.
    fn send(&mut self, notification: &TnExExNotification) {
        // Surface any pending gap before the next live notification.
        if self.dropped > 0 {
            match self.notifications.try_send(TnExExNotification::Lagged { missed: self.dropped }) {
                Ok(()) => {
                    warn!(
                        target: "exex::manager",
                        exex = %self.name,
                        missed = self.dropped,
                        "surfaced Lagged marker after dropped notifications",
                    );
                    self.dropped = 0;
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Still backed up — fold this notification into the gap.
                    self.dropped = self.dropped.saturating_add(1);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    error!(target: "exex::manager", exex = %self.name, "ExEx notification channel closed");
                    return;
                }
            }
        }

        match self.notifications.try_send(notification.clone()) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.dropped = self.dropped.saturating_add(1);
                warn!(
                    target: "exex::manager",
                    exex = %self.name,
                    dropped = self.dropped,
                    "ExEx notification channel full; dropping (will surface as Lagged)",
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                error!(target: "exex::manager", exex = %self.name, "ExEx notification channel closed");
            }
        }
    }
}

/// The ExEx manager fans out lifecycle notifications to all registered ExExes.
///
/// It is driven by [`TnExExManager::run`], an `async fn` following the same pattern as
/// `ExecutorEngine`. The manager runs for the lifetime of the node (spawned on
/// `node_task_manager`).
pub struct TnExExManager {
    /// Subscription to canonical state notifications from reth's BlockchainProvider.
    canon_state_stream: CanonStateNotificationStream,
    /// Stream of verified certificates from the consensus-following path.
    certs_stream: BroadcastStream<Certificate>,
    /// Stream of full consensus outputs from the consensus-following path.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// Per-ExEx delivery state (one per registered ExEx).
    exexes: Vec<ExExHandle>,
    /// Per-ExEx event receivers for `FinishedHeight` reporting, indexed in step with `exexes`.
    ///
    /// `FinishedHeight` is latest-wins, so a small bounded channel is sufficient — the ExEx
    /// reports with a non-blocking `try_send`, and only the maximum reported height matters.
    event_rxs: Vec<mpsc::Receiver<TnExExEvent>>,
    /// Watch sender for the minimum finished height across all ExExes.
    min_finished_height_tx: watch::Sender<Option<BlockNumber>>,
    /// Highest canonical block delivered as `ChainExecuted`. Used to detect gaps
    /// when reth's canonical-state stream drops notifications (it skips broadcast
    /// lag silently). `None` until the first commit.
    last_canon_tip: Option<BlockNumber>,
}

impl TnExExManager {
    /// Create a new ExEx manager.
    pub fn new(
        canon_state_stream: CanonStateNotificationStream,
        rx_certs: broadcast::Receiver<Certificate>,
        rx_consensus_output: broadcast::Receiver<ConsensusOutput>,
        exex_txs: Vec<(String, mpsc::Sender<TnExExNotification>)>,
        event_rxs: Vec<mpsc::Receiver<TnExExEvent>>,
    ) -> (Self, TnExExManagerHandle) {
        let exexes: Vec<ExExHandle> =
            exex_txs.into_iter().map(|(name, tx)| ExExHandle::new(name, tx)).collect();
        let (min_finished_height_tx, min_finished_height_rx) = watch::channel(None);

        let manager = Self {
            canon_state_stream,
            certs_stream: BroadcastStream::new(rx_certs),
            consensus_output_stream: BroadcastStream::new(rx_consensus_output),
            exexes,
            event_rxs,
            min_finished_height_tx,
            last_canon_tip: None,
        };

        let handle = TnExExManagerHandle { min_finished_height: min_finished_height_rx };

        (manager, handle)
    }

    /// Run the ExEx manager event loop for the lifetime of the node.
    ///
    /// The three notification sources and the per-ExEx event receivers are merged into a single
    /// priority-ordered stream (certificates > consensus output > canonical state > events) and
    /// folded over [`Router`], which fans each event out to every ExEx and recomputes the minimum
    /// finished height. The loop ends with `Ok(())` when any source stream closes; it never errors.
    pub async fn run(self) -> eyre::Result<()> {
        let Self {
            canon_state_stream,
            certs_stream,
            consensus_output_stream,
            exexes,
            event_rxs,
            min_finished_height_tx,
            last_canon_tip,
        } = self;

        let router = Router { exexes, min_finished_height_tx, last_canon_tip };

        // Map each source into a `RouterEvent`. The three source streams carry a trailing `Closed`
        // sentinel so a stream end terminates the fold (mirroring the old
        // `Poll::Ready(None) => return Poll::Ready(Ok(()))`); the event streams carry none, since
        // an ExEx dropping its event sender must not shut down the shared manager.
        let certs = certs_stream
            .map(RouterEvent::Cert)
            .chain(stream::once(async { RouterEvent::Closed("certificates") }));
        let consensus = consensus_output_stream
            .map(RouterEvent::Consensus)
            .chain(stream::once(async { RouterEvent::Closed("consensus output") }));
        let canon = canon_state_stream
            .map(RouterEvent::Canon)
            .chain(stream::once(async { RouterEvent::Closed("canonical state") }));
        let events = select_all(event_rxs.into_iter().enumerate().map(|(idx, rx)| {
            ReceiverStream::new(rx).map(move |ev| RouterEvent::Event { idx, ev })
        }));

        // Merge the four sources under a bounded-fairness round-robin schedule (see
        // `merge_sources`): the priority ordering certs > consensus > canon > events is kept as
        // throughput shares, but no source can be deferred indefinitely by a continuously-ready
        // higher-priority feed.
        let merged = merge_sources(certs, consensus, canon, events);

        // The fold ends only by observing a closed source stream (signalled as `StreamEnded`); it
        // never yields a real error, so the result is discarded and the manager reports `Ok(())`.
        let _ = merged
            .map(Ok::<RouterEvent, StreamEnded>)
            .try_fold(router, |mut router, event| async move {
                router.handle(event)?;
                Ok(router)
            })
            .await;

        Ok(())
    }
}

/// Round-robins the two sides of a merge: returns the current choice and flips the stored
/// `PollNext` state, so each side is polled first on alternating polls. This is the round-robin
/// idiom documented for [`select_with_strategy`]. The state is seeded from `PollNext::default()`
/// (`Left`), so the left (higher-priority) side still wins the very first poll.
fn round_robin(last: &mut PollNext) -> PollNext {
    last.toggle()
}

/// Merges the manager's four input streams into one, in priority order
/// certs > consensus output > canonical state > events.
///
/// Each of the three nested [`select_with_strategy`] layers uses [`round_robin`] rather than a
/// constant `PollNext::Left` bias. Round-robin at every layer keeps the priority *ordering* as a
/// bounded bias: when all four sources are continuously ready the merge yields them in the fixed
/// proportion `certs : consensus : canon : events == 4 : 2 : 1 : 1` (i.e. 1/2 > 1/4 > 1/8 = 1/8,
/// monotone in priority), while capping the deferral of any single source to one poll of its
/// sibling. A continuously-ready certificate or consensus feed therefore can no longer starve the
/// canonical-state or ExEx-event streams, which a constant `PollNext::Left` bias permitted without
/// any upper bound.
fn merge_sources<T>(
    certs: impl Stream<Item = T>,
    consensus: impl Stream<Item = T>,
    canon: impl Stream<Item = T>,
    events: impl Stream<Item = T>,
) -> impl Stream<Item = T> {
    select_with_strategy(
        certs,
        select_with_strategy(
            consensus,
            select_with_strategy(canon, events, round_robin),
            round_robin,
        ),
        round_robin,
    )
}

/// A single merged input to the [`TnExExManager`] event loop.
enum RouterEvent {
    /// A verified certificate (or a broadcast-lag marker) from the consensus-following path.
    Cert(Result<Certificate, BroadcastStreamRecvError>),
    /// A full consensus output (or a broadcast-lag marker) from the consensus-following path.
    Consensus(Result<ConsensusOutput, BroadcastStreamRecvError>),
    /// A canonical state notification from reth's `BlockchainProvider`.
    Canon(CanonStateNotification),
    /// A progress event from the ExEx at index `idx` in `exexes`.
    Event { idx: usize, ev: TnExExEvent },
    /// A source stream ended; the manager should stop.
    Closed(&'static str),
}

/// Signals that a source stream closed, terminating the manager fold.
struct StreamEnded;

/// Fan-out and finished-height bookkeeping for the manager event loop (the manager's state minus
/// its input streams), threaded through the fold in [`TnExExManager::run`].
struct Router {
    exexes: Vec<ExExHandle>,
    min_finished_height_tx: watch::Sender<Option<BlockNumber>>,
    last_canon_tip: Option<BlockNumber>,
}

impl Router {
    /// Dispatch a single merged event; `Err(StreamEnded)` stops the fold.
    fn handle(&mut self, event: RouterEvent) -> Result<(), StreamEnded> {
        match event {
            RouterEvent::Cert(res) => {
                self.deliver_broadcast(res, "certificates", |cert| {
                    TnExExNotification::CertificateAccepted { certificate: Box::new(cert) }
                });
                Ok(())
            }
            RouterEvent::Consensus(res) => {
                self.deliver_broadcast(res, "consensus output", |output| {
                    TnExExNotification::ConsensusOutput { output }
                });
                Ok(())
            }
            RouterEvent::Canon(notification) => {
                self.handle_canon(notification);
                Ok(())
            }
            RouterEvent::Event { idx, ev } => {
                self.handle_event(idx, ev);
                Ok(())
            }
            RouterEvent::Closed(which) => {
                debug!(target: "exex::manager", stream = which, "stream ended");
                Err(StreamEnded)
            }
        }
    }

    /// Fan out a notification to all registered ExExes (non-blocking).
    fn fan_out(&mut self, notification: &TnExExNotification) {
        for handle in &mut self.exexes {
            handle.send(notification);
        }
    }

    /// Deliver a broadcast item, mapping it to its notification — or, when the broadcast lagged,
    /// surfacing a `Lagged` marker so the contract is uniform across every manager-side input.
    fn deliver_broadcast<T>(
        &mut self,
        res: Result<T, BroadcastStreamRecvError>,
        kind: &str,
        into_notification: impl FnOnce(T) -> TnExExNotification,
    ) {
        let notification = res.map(into_notification).unwrap_or_else(|err| match err {
            BroadcastStreamRecvError::Lagged(missed) => {
                warn!(target: "exex::manager", missed, "{kind} stream lagged; surfaced Lagged");
                TnExExNotification::Lagged { missed }
            }
        });
        self.fan_out(&notification);
    }

    /// Handle a canonical state notification.
    fn handle_canon(&mut self, notification: CanonStateNotification) {
        match notification {
            // reth's canonical-state stream drops broadcast lag silently, so a starved manager can
            // miss commits. `handle_canon_commit` detects the resulting discontinuity and surfaces
            // a `Lagged` marker before delivering, so stateful ExExes reconcile rather
            // than carry a silent hole.
            CanonStateNotification::Commit { new } => self.handle_canon_commit(new),
            // TN's BFT consensus has immediate finality, so reth should only ever emit `Commit`. A
            // `Reorg` violates that invariant — log it loudly, but degrade gracefully by treating
            // the new side as a commit. The manager is spawned once and never respawned, so
            // panicking here (it previously did) would kill the shared fan-out for every ExEx for
            // the node's lifetime.
            //
            // Note: `old` is discarded, and if `new`'s blocks are not strictly newer than the last
            // delivered tip the downstream height-dedup (`dedup_chain_executed`) may drop this
            // degraded commit. Acceptable precisely because the arm is unreachable under TN's
            // immediate-finality consensus.
            CanonStateNotification::Reorg { old, new } => {
                error!(
                    target: "exex::manager",
                    old_blocks = old.len(),
                    new_blocks = new.len(),
                    "unexpected Reorg for finalized state; degrading to commit of the new side",
                );
                self.handle_canon_commit(new);
            }
        }
    }

    /// Handle a canonical commit: detect any block-number gap (reth drops canonical-stream lag
    /// silently), surface it as `Lagged`, then deliver the executed chain. Shared by the `Commit`
    /// and degraded `Reorg` arms.
    fn handle_canon_commit(&mut self, new: Arc<Chain>) {
        let range = new.range();
        let first = *range.start();
        canon_gap(&mut self.last_canon_tip, first, *range.end()).into_iter().for_each(|missed| {
            warn!(
                target: "exex::manager",
                missed,
                first,
                "canonical ChainExecuted gap detected; surfacing Lagged",
            );
            self.fan_out(&TnExExNotification::Lagged { missed });
        });
        self.fan_out(&TnExExNotification::ChainExecuted { new });
    }

    /// Record an ExEx progress event, then recompute the minimum finished height.
    fn handle_event(&mut self, idx: usize, event: TnExExEvent) {
        match event {
            TnExExEvent::FinishedHeight(height) => {
                self.exexes
                    .get_mut(idx)
                    .into_iter()
                    .for_each(|handle| handle.finished_height = Some(height));
            }
        }
        self.recompute_min();
    }

    /// Recompute the minimum finished height across all ExExes and publish it.
    fn recompute_min(&mut self) {
        // Minimum finished height: None if any ExEx hasn't reported yet.
        let min_height = self
            .exexes
            .iter()
            .try_fold(None, |acc: Option<BlockNumber>, h| {
                let height = h.finished_height?;
                Some(Some(acc.map_or(height, |a: BlockNumber| a.min(height))))
            })
            .flatten();

        self.min_finished_height_tx.send_replace(min_height);
    }
}

impl std::fmt::Debug for TnExExManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TnExExManager")
            .field("num_exexes", &self.exexes.len())
            .field(
                "finished_heights",
                &self.exexes.iter().map(|e| e.finished_height).collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// Cheaply cloneable handle for querying ExEx manager state.
#[derive(Clone, Debug)]
pub struct TnExExManagerHandle {
    /// The minimum finished height across all ExExes.
    min_finished_height: watch::Receiver<Option<BlockNumber>>,
}

impl TnExExManagerHandle {
    /// Returns the current minimum finished height across all ExExes.
    ///
    /// Returns `None` if any ExEx hasn't reported a finished height yet.
    pub fn min_finished_height(&self) -> Option<BlockNumber> {
        *self.min_finished_height.borrow()
    }
}

/// Returns the default ExEx notification channel capacity.
///
/// Used when an ExEx is registered with `TnBuilder::install_exex`; use
/// `install_exex_with_capacity` to override it per-ExEx.
pub const fn exex_channel_capacity() -> usize {
    EXEX_CHANNEL_CAPACITY
}

/// Resolve an operator-supplied ExEx notification channel capacity to a valid
/// bounded-channel buffer size.
///
/// [`tokio::sync::mpsc::channel`] panics when constructed with a zero-length
/// buffer, and the capacity passed to `TnBuilder::install_exex_with_capacity` is
/// otherwise unvalidated. Clamping a `0` up to `1` here keeps a misconfigured
/// ExEx registration from panicking the node at startup; every other value
/// passes through unchanged.
pub const fn resolve_exex_channel_capacity(capacity: usize) -> usize {
    if capacity == 0 {
        1
    } else {
        capacity
    }
}

/// Returns `Some(missed)` — the count of skipped block numbers — when `first`
/// is not contiguous with the last delivered canonical tip, and advances
/// `last_tip` monotonically to `tip`.
///
/// Returns `None` on the first commit (no baseline) and for contiguous or
/// out-of-order/duplicate commits. `last_tip` never moves backward.
fn canon_gap(
    last_tip: &mut Option<BlockNumber>,
    first: BlockNumber,
    tip: BlockNumber,
) -> Option<u64> {
    let missed = match *last_tip {
        Some(prev) if first > prev.saturating_add(1) => Some(first - prev - 1),
        _ => None,
    };
    // Advance monotonically; an out-of-order/duplicate commit never rewinds.
    *last_tip = Some((*last_tip).map_or(tip, |p| p.max(tip)));
    missed
}

#[cfg(test)]
mod tests {
    use super::*;

    // A cheap, distinguishable filler notification. `Lagged` is the only variant
    // constructible without building consensus/execution types, so we use a
    // sentinel `missed` that can't be confused with a manager-generated marker.
    const FILLER: u64 = u64::MAX;
    fn filler() -> TnExExNotification {
        TnExExNotification::Lagged { missed: FILLER }
    }

    #[tokio::test]
    async fn full_channel_drops_then_surfaces_lagged_marker() {
        let (tx, mut rx) = mpsc::channel(2);
        let mut handle = ExExHandle::new("test".to_string(), tx);

        // Fill the channel (capacity 2).
        handle.send(&filler());
        handle.send(&filler());
        assert_eq!(handle.dropped, 0);

        // The next two sends find the channel full and are dropped (and counted).
        handle.send(&filler());
        handle.send(&filler());
        assert_eq!(handle.dropped, 2);

        // Drain the two buffered notifications.
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: FILLER })));
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: FILLER })));

        // With space available, the next send surfaces the gap as a `Lagged`
        // marker (missed == 2) BEFORE the notification, and resets the count.
        handle.send(&filler());
        assert_eq!(handle.dropped, 0);
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: 2 })));
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: FILLER })));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn delivers_without_lagged_when_not_full() {
        let (tx, mut rx) = mpsc::channel(4);
        let mut handle = ExExHandle::new("test".to_string(), tx);

        handle.send(&filler());
        assert_eq!(handle.dropped, 0);
        // Exactly one notification, no spurious Lagged marker.
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: FILLER })));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn resolve_exex_channel_capacity_clamps_zero_to_one() {
        // A `0` capacity (e.g. `install_exex_with_capacity(.., 0, ..)`) would
        // panic `mpsc::channel(0)` at node startup; it must clamp up to 1.
        assert_eq!(resolve_exex_channel_capacity(0), 1);
        // Every other value passes through unchanged.
        assert_eq!(resolve_exex_channel_capacity(1), 1);
        assert_eq!(resolve_exex_channel_capacity(256), 256);
    }

    #[tokio::test]
    async fn resolved_zero_capacity_builds_a_valid_channel_without_panicking() {
        // The exact construction the node performs at startup must not panic for
        // a `0` capacity once routed through `resolve_exex_channel_capacity`.
        let (tx, _rx) = mpsc::channel::<TnExExNotification>(resolve_exex_channel_capacity(0));
        assert_eq!(tx.capacity(), 1);
    }

    #[test]
    fn canon_gap_detects_discontinuities_and_advances_monotonically() {
        let mut tip = None;

        // First commit: no baseline → no gap, tip set to the commit's tip.
        assert_eq!(canon_gap(&mut tip, 1, 3), None);
        assert_eq!(tip, Some(3));

        // Contiguous multi-block commit (4..=6 follows 3) → no gap.
        assert_eq!(canon_gap(&mut tip, 4, 6), None);
        assert_eq!(tip, Some(6));

        // Contiguous single-block commit (7 follows 6) → no gap.
        assert_eq!(canon_gap(&mut tip, 7, 7), None);
        assert_eq!(tip, Some(7));

        // Gap: last tip 7, next commit starts at 11 → blocks 8,9,10 missing.
        assert_eq!(canon_gap(&mut tip, 11, 13), Some(3));
        assert_eq!(tip, Some(13));

        // Out-of-order/duplicate commit (older range) → no gap, tip never rewinds.
        assert_eq!(canon_gap(&mut tip, 5, 8), None);
        assert_eq!(tip, Some(13));
    }

    // Tags which source a merged item came from, so the merge tests can assert both
    // fairness (no source is starved) and the preserved priority ordering.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Source {
        Cert,
        Consensus,
        Canon,
        Event,
    }

    #[test]
    fn round_robin_seeds_left_then_alternates() {
        // Seeded from the default (`Left`), certificates win the first poll; the state then
        // flips on every call, so neither side is ever polled first twice in a row.
        let mut state = PollNext::default();
        assert!(matches!(round_robin(&mut state), PollNext::Left));
        assert!(matches!(round_robin(&mut state), PollNext::Right));
        assert!(matches!(round_robin(&mut state), PollNext::Left));
        assert!(matches!(round_robin(&mut state), PollNext::Right));
    }

    #[tokio::test]
    async fn merge_bounds_low_priority_starvation() {
        // The highest-priority source is continuously ready and infinite; the lowest-priority
        // source carries a single, distinguishable item. Under the round-robin schedule that lone
        // event must surface within a small, bounded number of polls rather than be deferred
        // forever behind the ready certificate feed.
        //
        // Confirm-by-mutation: reverting `round_robin` to a constant `PollNext::Left` (the original
        // `prefer_left`) leaves the `Event` unreachable while the certificate stream stays ready,
        // so `saw_event` stays false and this assertion fails.
        let certs = stream::repeat(Source::Cert);
        let consensus = stream::empty::<Source>();
        let canon = stream::empty::<Source>();
        let events = stream::once(async { Source::Event });

        let observed: Vec<Source> =
            merge_sources(certs, consensus, canon, events).take(8).collect().await;

        let saw_event = observed.contains(&Source::Event);
        assert!(saw_event, "low-priority event starved under a ready cert feed: {observed:?}");
    }

    #[tokio::test]
    async fn merge_preserves_priority_ordering_without_flattening() {
        // With all four sources continuously ready, the round-robin nest yields the fixed
        // proportion certs : consensus : canon : events == 4 : 2 : 1 : 1 over each 8-item cycle.
        // Assert the priority ordering survives as a strict, monotone bias (certs > consensus >
        // canon) with the two lowest tying (canon == events), i.e. the fix does not silently
        // flatten priority into a uniform round-robin across all four sources.
        //
        // Confirm-by-mutation: under a constant `PollNext::Left` every item is a certificate, so
        // `consensus_n > canon_n` (0 > 0) fails.
        const N: usize = 8 * 100;
        let certs = stream::repeat(Source::Cert);
        let consensus = stream::repeat(Source::Consensus);
        let canon = stream::repeat(Source::Canon);
        let events = stream::repeat(Source::Event);

        let observed: Vec<Source> =
            merge_sources(certs, consensus, canon, events).take(N).collect().await;

        let count = |want: Source| observed.iter().filter(|s| **s == want).count();
        let certs_n = count(Source::Cert);
        let consensus_n = count(Source::Consensus);
        let canon_n = count(Source::Canon);
        let events_n = count(Source::Event);

        assert!(certs_n > consensus_n, "certs {certs_n} !> consensus {consensus_n}");
        assert!(consensus_n > canon_n, "consensus {consensus_n} !> canon {canon_n}");
        assert_eq!(canon_n, events_n, "canon {canon_n} != events {events_n} (lowest two must tie)");
        // The exact 4:2:1:1 shares document the schedule the ordering rests on.
        assert_eq!((certs_n, consensus_n, canon_n, events_n), (N / 2, N / 4, N / 8, N / 8));
    }
}
