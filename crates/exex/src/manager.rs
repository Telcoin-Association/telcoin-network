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

use crate::{TnExExEvent, TnExExNotification};
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
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, warn};

/// Notification channel capacity per ExEx.
const EXEX_CHANNEL_CAPACITY: usize = 64;

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
    /// Per-ExEx notification senders (one per registered ExEx).
    exex_txs: Vec<(String, mpsc::Sender<TnExExNotification>)>,
    /// Per-ExEx event receivers for FinishedHeight reporting.
    event_rxs: Vec<mpsc::UnboundedReceiver<TnExExEvent>>,
    /// Per-ExEx finished heights.
    finished_heights: Vec<Option<BlockNumber>>,
    /// Watch sender for the minimum finished height across all ExExes.
    min_finished_height_tx: watch::Sender<Option<BlockNumber>>,
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
        let num_exexes = exex_txs.len();
        let (min_finished_height_tx, min_finished_height_rx) = watch::channel(None);

        let manager = Self {
            canon_state_stream,
            own_certs_stream: BroadcastStream::new(rx_own_certs),
            peer_certs_stream: BroadcastStream::new(rx_peer_certs),
            committed_sub_dags_stream: BroadcastStream::new(rx_committed_sub_dags),
            exex_txs,
            event_rxs,
            finished_heights: vec![None; num_exexes],
            min_finished_height_tx,
        };

        let handle = TnExExManagerHandle { min_finished_height: min_finished_height_rx };

        (manager, handle)
    }

    /// Fan out a notification to all registered ExExes.
    fn fan_out(&self, notification: &TnExExNotification) {
        for (name, tx) in &self.exex_txs {
            if let Err(e) = tx.try_send(notification.clone()) {
                match e {
                    mpsc::error::TrySendError::Full(_) => {
                        warn!(
                            target: "exex::manager",
                            exex = %name,
                            "ExEx notification channel full, dropping notification"
                        );
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        error!(
                            target: "exex::manager",
                            exex = %name,
                            "ExEx notification channel closed"
                        );
                    }
                }
            }
        }
    }

    /// Poll all event receivers for FinishedHeight updates.
    fn poll_events(&mut self, cx: &mut Context<'_>) {
        for (i, rx) in self.event_rxs.iter_mut().enumerate() {
            while let Poll::Ready(Some(event)) = rx.poll_recv(cx) {
                match event {
                    TnExExEvent::FinishedHeight(height) => {
                        self.finished_heights[i] = Some(height);
                    }
                }
            }
        }

        // Compute minimum finished height: None if any ExEx hasn't reported yet
        let min_height = self
            .finished_heights
            .iter()
            .try_fold(None, |acc: Option<BlockNumber>, h| {
                let height = (*h)?;
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
                    let notification =
                        TnExExNotification::CertificateAccepted { certificate: Box::new(cert), is_own: true };
                    this.fan_out(&notification);
                }
                Poll::Ready(Some(Err(e))) => {
                    warn!(target: "exex::manager", ?e, "own certificates stream error");
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
                Poll::Ready(Some(Err(e))) => {
                    warn!(target: "exex::manager", ?e, "peer certificates stream error");
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
                Poll::Ready(Some(Err(e))) => {
                    warn!(target: "exex::manager", ?e, "committed sub-dags stream error");
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
                    CanonStateNotification::Commit { new } => {
                        let notification = TnExExNotification::ChainExecuted { new };
                        this.fan_out(&notification);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
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
            .field("num_exexes", &self.exex_txs.len())
            .field("finished_heights", &self.finished_heights)
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
