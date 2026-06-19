//! ExEx Manager — fans out lifecycle notifications to all registered ExExes.
//!
//! The manager subscribes to 4 sources:
//! 1. Own certificates (from ConsensusBus)
//! 2. Peer certificates (from ConsensusBus)
//! 3. Committed sub-DAGs (from ConsensusBus)
//! 4. Canonical state notifications (from reth's BlockchainProvider)
//!
//! It wraps each into the appropriate [`TnExExNotification`] variant and fans out
//! to all registered ExExes using non-blocking `try_send`.
//!
//! # Delivery contract
//!
//! Fan-out never blocks: a slow ExEx that fills its bounded channel has
//! notifications dropped rather than back-pressuring consensus or execution.
//! Drops are not silent — the next successful delivery is preceded by a
//! [`TnExExNotification::Lagged`] marker carrying a best-effort count, so the
//! ExEx can detect the gap and reconcile via [`replay`](crate::replay).

use crate::{Chain, TnExExEvent, TnExExNotification};
use futures::StreamExt;
use reth_provider::CanonStateNotification;
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_reth::CanonStateNotificationStream;
use tn_types::{BlockNumber, Certificate, CommittedSubDag};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::{errors::BroadcastStreamRecvError, BroadcastStream};
use tracing::{debug, error, warn};

/// Notification channel capacity per ExEx.
const EXEX_CHANNEL_CAPACITY: usize = 64;

/// Per-ExEx delivery state: the notification sender, lag tracking, and the event
/// receiver used for `FinishedHeight` reporting.
struct ExExHandle {
    /// ExEx name (for logging).
    name: String,
    /// Bounded, non-blocking notification sender.
    notifications: mpsc::Sender<TnExExNotification>,
    /// Event receiver for progress reports from the ExEx.
    events: mpsc::UnboundedReceiver<TnExExEvent>,
    /// Highest durably-processed height reported by the ExEx, if any.
    finished_height: Option<BlockNumber>,
    /// Notifications dropped since the last successful delivery.
    ///
    /// When non-zero, the next successful send is preceded by a
    /// [`TnExExNotification::Lagged`] marker so the ExEx can detect the gap.
    dropped: u64,
}

impl ExExHandle {
    fn new(
        name: String,
        notifications: mpsc::Sender<TnExExNotification>,
        events: mpsc::UnboundedReceiver<TnExExEvent>,
    ) -> Self {
        Self { name, notifications, events, finished_height: None, dropped: 0 }
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
/// It implements [`Future`] following the same pattern as `ExecutorEngine`.
/// The manager runs for the lifetime of the node (spawned on `node_task_manager`).
pub struct TnExExManager {
    /// Subscription to canonical state notifications from reth's BlockchainProvider.
    canon_state_stream: CanonStateNotificationStream,
    /// Stream of own certificates from ConsensusBus.
    own_certs_stream: BroadcastStream<Certificate>,
    /// Stream of peer certificates from ConsensusBus.
    peer_certs_stream: BroadcastStream<Certificate>,
    /// Stream of committed sub-DAGs from ConsensusBus.
    committed_sub_dags_stream: BroadcastStream<Arc<CommittedSubDag>>,
    /// Per-ExEx delivery state (one per registered ExEx).
    exexes: Vec<ExExHandle>,
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
        rx_own_certs: broadcast::Receiver<Certificate>,
        rx_peer_certs: broadcast::Receiver<Certificate>,
        rx_committed_sub_dags: broadcast::Receiver<Arc<CommittedSubDag>>,
        exex_txs: Vec<(String, mpsc::Sender<TnExExNotification>)>,
        event_rxs: Vec<mpsc::UnboundedReceiver<TnExExEvent>>,
    ) -> (Self, TnExExManagerHandle) {
        let exexes: Vec<ExExHandle> = exex_txs
            .into_iter()
            .zip(event_rxs)
            .map(|((name, tx), events)| ExExHandle::new(name, tx, events))
            .collect();
        let (min_finished_height_tx, min_finished_height_rx) = watch::channel(None);

        let manager = Self {
            canon_state_stream,
            own_certs_stream: BroadcastStream::new(rx_own_certs),
            peer_certs_stream: BroadcastStream::new(rx_peer_certs),
            committed_sub_dags_stream: BroadcastStream::new(rx_committed_sub_dags),
            exexes,
            min_finished_height_tx,
            last_canon_tip: None,
        };

        let handle = TnExExManagerHandle { min_finished_height: min_finished_height_rx };

        (manager, handle)
    }

    /// Fan out a notification to all registered ExExes (non-blocking).
    fn fan_out(&mut self, notification: &TnExExNotification) {
        for handle in &mut self.exexes {
            handle.send(notification);
        }
    }

    /// Handle a canonical commit: detect any block-number gap (reth drops
    /// canonical-stream lag silently), surface it as `Lagged`, then deliver the
    /// executed chain. Shared by the `Commit` and degraded `Reorg` arms.
    fn handle_canon_commit(&mut self, new: Arc<Chain>) {
        let range = new.range();
        if let Some(missed) = canon_gap(&mut self.last_canon_tip, *range.start(), *range.end()) {
            warn!(
                target: "exex::manager",
                missed,
                first = *range.start(),
                "canonical ChainExecuted gap detected; surfacing Lagged",
            );
            self.fan_out(&TnExExNotification::Lagged { missed });
        }
        self.fan_out(&TnExExNotification::ChainExecuted { new });
    }

    /// Poll all event receivers for FinishedHeight updates and recompute the
    /// minimum finished height across all ExExes.
    fn poll_events(&mut self, cx: &mut Context<'_>) {
        for handle in &mut self.exexes {
            while let Poll::Ready(Some(event)) = handle.events.poll_recv(cx) {
                match event {
                    TnExExEvent::FinishedHeight(height) => {
                        handle.finished_height = Some(height);
                    }
                }
            }
        }

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

impl futures::Future for TnExExManager {
    type Output = eyre::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Poll own certificates stream
        loop {
            match this.own_certs_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(cert))) => {
                    let notification = TnExExNotification::CertificateAccepted {
                        certificate: Box::new(cert),
                        is_own: true,
                    };
                    this.fan_out(&notification);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    // Surface the broadcast lag so the `Lagged` contract is uniform
                    // across every manager-side input, not just the canon stream.
                    this.fan_out(&TnExExNotification::Lagged { missed: n });
                    warn!(target: "exex::manager", missed = n, "own certificates stream lagged; surfaced Lagged");
                }
                Poll::Ready(None) => {
                    debug!(target: "exex::manager", "own certificates stream ended");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => break,
            }
        }

        // Poll peer certificates stream
        loop {
            match this.peer_certs_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(cert))) => {
                    let notification = TnExExNotification::CertificateAccepted {
                        certificate: Box::new(cert),
                        is_own: false,
                    };
                    this.fan_out(&notification);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    this.fan_out(&TnExExNotification::Lagged { missed: n });
                    warn!(target: "exex::manager", missed = n, "peer certificates stream lagged; surfaced Lagged");
                }
                Poll::Ready(None) => {
                    debug!(target: "exex::manager", "peer certificates stream ended");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => break,
            }
        }

        // Poll committed sub-DAGs stream
        loop {
            match this.committed_sub_dags_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(sub_dag))) => {
                    let notification = TnExExNotification::ConsensusCommitted { sub_dag };
                    this.fan_out(&notification);
                }
                Poll::Ready(Some(Err(BroadcastStreamRecvError::Lagged(n)))) => {
                    this.fan_out(&TnExExNotification::Lagged { missed: n });
                    warn!(target: "exex::manager", missed = n, "committed sub-dags stream lagged; surfaced Lagged");
                }
                Poll::Ready(None) => {
                    debug!(target: "exex::manager", "committed sub-dags stream ended");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => break,
            }
        }

        // Poll canonical state stream
        loop {
            match this.canon_state_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(notification)) => match notification {
                    // reth's canonical-state stream drops broadcast lag silently,
                    // so a starved manager can miss commits. `handle_canon_commit`
                    // detects the resulting discontinuity and surfaces a `Lagged`
                    // marker before delivering, so stateful ExExes reconcile rather
                    // than carry a silent hole.
                    CanonStateNotification::Commit { new } => this.handle_canon_commit(new),
                    // TN's BFT consensus has immediate finality, so reth should
                    // only ever emit `Commit`. A `Reorg` violates that invariant —
                    // log it loudly, but degrade gracefully by treating the new
                    // side as a commit. The manager is spawned once and never
                    // respawned, so panicking here (it previously did) would kill
                    // the shared fan-out for every ExEx for the node's lifetime.
                    CanonStateNotification::Reorg { old, new } => {
                        error!(
                            target: "exex::manager",
                            old_blocks = old.len(),
                            new_blocks = new.len(),
                            "unexpected Reorg for finalized state; degrading to commit of the new side",
                        );
                        this.handle_canon_commit(new);
                    }
                },
                Poll::Ready(None) => {
                    debug!(target: "exex::manager", "canonical state stream ended");
                    return Poll::Ready(Ok(()));
                }
                Poll::Pending => break,
            }
        }

        // Poll ExEx events
        this.poll_events(cx);

        Poll::Pending
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

/// Returns the ExEx notification channel capacity.
pub const fn exex_channel_capacity() -> usize {
    EXEX_CHANNEL_CAPACITY
}

/// Returns `Some(missed)` — the count of skipped block numbers — when `first`
/// is not contiguous with the last delivered canonical tip, and advances
/// `last_tip` monotonically to `tip`.
///
/// Returns `None` on the first commit (no baseline) and for contiguous or
/// out-of-order/duplicate commits. `last_tip` never moves backward.
fn canon_gap(last_tip: &mut Option<BlockNumber>, first: BlockNumber, tip: BlockNumber) -> Option<u64> {
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
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let mut handle = ExExHandle::new("test".to_string(), tx, event_rx);

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
        let (_event_tx, event_rx) = mpsc::unbounded_channel();
        let mut handle = ExExHandle::new("test".to_string(), tx, event_rx);

        handle.send(&filler());
        assert_eq!(handle.dropped, 0);
        // Exactly one notification, no spurious Lagged marker.
        assert!(matches!(rx.try_recv(), Ok(TnExExNotification::Lagged { missed: FILLER })));
        assert!(rx.try_recv().is_err());
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
}
