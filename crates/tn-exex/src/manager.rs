// SPDX-License-Identifier: MIT OR Apache-2.0
//! ExEx manager for distributing notifications and coordinating multiple ExEx tasks.

use crate::{TnExExEvent, TnExExNotification};
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::PollSender;
use tn_types::{BlockNumHash, Certificate, CommittedSubDag};

/// The lowest finished height among all ExEx tasks.
///
/// This is used to track the minimum block height that all ExExes have processed,
/// which can be used for pruning and progress monitoring.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FinishedTnExExHeight {
    /// The lowest block that all ExExes have finished processing.
    /// `None` if no ExExes have reported any progress yet.
    pub height: Option<BlockNumHash>,
}

impl FinishedTnExExHeight {
    /// Creates a new `FinishedTnExExHeight` with the given block.
    pub fn new(height: BlockNumHash) -> Self {
        Self { height: Some(height) }
    }

    /// Updates the finished height to the minimum of the current and new heights.
    pub fn update(&mut self, new_height: Option<BlockNumHash>) {
        match (self.height, new_height) {
            (Some(current), Some(new)) if new.number < current.number => {
                self.height = Some(new);
            }
            (None, Some(new)) => {
                self.height = Some(new);
            }
            _ => {}
        }
    }
}

/// Handle for communicating with a single ExEx task.
///
/// Each ExEx has its own handle that manages bidirectional communication:
/// - Notifications are sent TO the ExEx via `sender`
/// - Events are received FROM the ExEx via `receiver`
#[derive(Debug)]
pub struct TnExExHandle {
    /// Unique identifier for this ExEx.
    pub id: String,

    /// Channel for sending notifications to the ExEx.
    /// Uses PollSender for backpressure-aware sending.
    pub sender: PollSender<TnExExNotification>,

    /// Channel for receiving events from the ExEx.
    pub receiver: mpsc::UnboundedReceiver<TnExExEvent>,

    /// Monotonically increasing ID for notifications sent to this ExEx.
    pub next_notification_id: usize,

    /// The last block height this ExEx reported as finished.
    /// Used to skip sending notifications for blocks already processed.
    pub finished_height: Option<BlockNumHash>,
}

impl TnExExHandle {
    /// Creates a new ExEx handle.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for the ExEx
    /// * `node_head` - Current chain head (used to initialize finished_height)
    ///
    /// # Returns
    ///
    /// A tuple of:
    /// - `TnExExHandle` for the manager to use
    /// - `mpsc::UnboundedSender<TnExExEvent>` for the ExEx to send events
    /// - `mpsc::Receiver<TnExExNotification>` for the ExEx to receive notifications
    pub fn new(
        id: String,
        node_head: BlockNumHash,
    ) -> (Self, mpsc::UnboundedSender<TnExExEvent>, mpsc::Receiver<TnExExNotification>) {
        // Create channels with appropriate buffer sizes
        let (notification_tx, notification_rx) = mpsc::channel(64);
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let handle = Self {
            id,
            sender: PollSender::new(notification_tx),
            receiver: event_rx,
            next_notification_id: 0,
            // Initialize to node head - ExEx will start from here
            finished_height: Some(node_head),
        };

        (handle, event_tx, notification_rx)
    }
}

/// Manager that coordinates multiple ExEx tasks.
///
/// The manager runs as a background Future that:
/// 1. Receives notifications from the execution engine
/// 2. Buffers them internally
/// 3. Distributes them to all registered ExExes
/// 4. Tracks progress via ExEx events
/// 5. Applies backpressure when buffers fill
pub struct TnExExManager {
    /// Handles for all registered ExExes.
    exex_handles: Vec<TnExExHandle>,

    /// Channel for receiving notifications from the execution engine.
    handle_rx: mpsc::UnboundedReceiver<TnExExNotification>,

    /// Internal buffer of notifications pending delivery.
    /// Tuple of (notification_id, notification).
    buffer: VecDeque<(usize, TnExExNotification)>,

    /// Maximum number of notifications to buffer before applying backpressure.
    max_capacity: usize,

    /// Shared atomic capacity counter for the handle to check.
    current_capacity: Arc<AtomicUsize>,

    /// Watch channel to signal readiness to the handle.
    is_ready: watch::Sender<bool>,

    /// Watch channel to broadcast the lowest finished height across all ExExes.
    finished_height: watch::Sender<FinishedTnExExHeight>,

    /// Monotonically increasing notification ID.
    next_notification_id: usize,

    /// Optional broadcast receiver for ExEx certificate notifications from consensus.
    /// `None` when no ConsensusBus is connected (e.g., in tests or when no ExExes installed).
    exex_certificates_rx: Option<broadcast::Receiver<Arc<Certificate>>>,

    /// Optional broadcast receiver for ExEx committed sub-DAG notifications from consensus.
    exex_committed_sub_dags_rx: Option<broadcast::Receiver<Arc<CommittedSubDag>>>,
}

impl TnExExManager {
    /// Creates a new ExEx manager with the given handles.
    ///
    /// # Arguments
    ///
    /// * `handles` - Vec of ExEx handles to manage
    /// * `max_capacity` - Maximum buffer size before backpressure (default: 64)
    /// * `exex_certificates_rx` - Optional broadcast receiver for certificate notifications
    /// * `exex_committed_sub_dags_rx` - Optional broadcast receiver for committed sub-DAG notifications
    ///
    /// # Returns
    ///
    /// Tuple of (TnExExManager, TnExExManagerHandle)
    pub fn new(
        handles: Vec<TnExExHandle>,
        max_capacity: Option<usize>,
        exex_certificates_rx: Option<broadcast::Receiver<Arc<Certificate>>>,
        exex_committed_sub_dags_rx: Option<broadcast::Receiver<Arc<CommittedSubDag>>>,
    ) -> (Self, TnExExManagerHandle) {
        let (handle_tx, handle_rx) = mpsc::unbounded_channel();
        let max_capacity = max_capacity.unwrap_or(64);
        let current_capacity = Arc::new(AtomicUsize::new(max_capacity));
        let (is_ready_tx, is_ready_rx) = watch::channel(true);
        let (finished_height_tx, finished_height_rx) = watch::channel(FinishedTnExExHeight::default());

        let num_exexs = handles.len();
        
        // Update metrics
        metrics::gauge!("tn_exex_num_exexs").set(num_exexs as f64);

        let manager = Self {
            exex_handles: handles,
            handle_rx,
            buffer: VecDeque::new(),
            max_capacity,
            current_capacity: Arc::clone(&current_capacity),
            is_ready: is_ready_tx,
            finished_height: finished_height_tx,
            next_notification_id: 0,
            exex_certificates_rx,
            exex_committed_sub_dags_rx,
        };

        let handle = TnExExManagerHandle {
            num_exexs,
            handle_tx: Some(handle_tx),
            current_capacity,
            is_ready: is_ready_rx,
            finished_height: finished_height_rx,
        };

        (manager, handle)
    }

    /// Handles incoming events from ExExes (progress updates).
    fn handle_exex_events(&mut self, cx: &mut Context<'_>) {
        for handle in &mut self.exex_handles {
            while let Poll::Ready(Some(event)) = handle.receiver.poll_recv(cx) {
                match event {
                    TnExExEvent::FinishedHeight(block) => {
                        handle.finished_height = Some(block);
                        tracing::debug!(
                            exex_id = %handle.id,
                            block_number = block.number,
                            block_hash = %block.hash,
                            "ExEx reported finished height"
                        );
                    }
                }
            }
        }

        // Update the global finished height (minimum across all ExExes)
        self.update_finished_height();
    }

    /// Updates the global finished height to the minimum across all ExExes.
    fn update_finished_height(&mut self) {
        let mut finished = FinishedTnExExHeight::default();
        
        for handle in &self.exex_handles {
            finished.update(handle.finished_height);
        }

        // Only send if changed
        if self.finished_height.borrow().height != finished.height {
            let _ = self.finished_height.send(finished);
        }
    }

    /// Receives new notifications from the execution engine and buffers them.
    fn receive_new_notifications(&mut self, cx: &mut Context<'_>) {
        // Drain notifications from the handle until we hit capacity or no more available
        while self.buffer.len() < self.max_capacity {
            match self.handle_rx.poll_recv(cx) {
                Poll::Ready(Some(notification)) => {
                    let id = self.next_notification_id;
                    self.next_notification_id += 1;
                    self.buffer.push_back((id, notification));
                    
                    metrics::gauge!("tn_exex_buffer_size").set(self.buffer.len() as f64);
                }
                Poll::Ready(None) => {
                    // Channel closed - engine shutting down
                    break;
                }
                Poll::Pending => break,
            }
        }

        // Update capacity
        let remaining = self.max_capacity.saturating_sub(self.buffer.len());
        self.current_capacity.store(remaining, Ordering::Relaxed);

        // Signal readiness
        let is_ready = remaining > 0;
        let _ = self.is_ready.send(is_ready);
    }

    /// Receives consensus-layer notifications from ConsensusBus broadcast channels.
    ///
    /// Polls the optional certificate and committed sub-DAG broadcast receivers,
    /// wraps received data in `TnExExNotification` variants, and pushes to the buffer.
    /// Handles `Lagged` errors by logging a warning (indicates ExEx is too slow to keep up).
    fn receive_consensus_notifications(&mut self) {
        // Drain certificate notifications
        if let Some(rx) = &mut self.exex_certificates_rx {
            loop {
                if self.buffer.len() >= self.max_capacity {
                    break;
                }
                match rx.try_recv() {
                    Ok(certificate) => {
                        let id = self.next_notification_id;
                        self.next_notification_id += 1;
                        self.buffer.push_back((
                            id,
                            TnExExNotification::CertificateCreated { certificate },
                        ));
                    }
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        tracing::warn!(
                            lagged = n,
                            "ExEx manager lagged behind on certificate notifications"
                        );
                    }
                    Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                        break;
                    }
                }
            }
        }

        // Drain committed sub-DAG notifications
        if let Some(rx) = &mut self.exex_committed_sub_dags_rx {
            loop {
                if self.buffer.len() >= self.max_capacity {
                    break;
                }
                match rx.try_recv() {
                    Ok(sub_dag) => {
                        let id = self.next_notification_id;
                        self.next_notification_id += 1;
                        self.buffer
                            .push_back((id, TnExExNotification::ConsensusCommitted { sub_dag }));
                    }
                    Err(broadcast::error::TryRecvError::Lagged(n)) => {
                        tracing::warn!(
                            lagged = n,
                            "ExEx manager lagged behind on committed sub-DAG notifications"
                        );
                    }
                    Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                        break;
                    }
                }
            }
        }
    }

    /// Sends buffered notifications to ExExes that are ready.
    fn send_notifications_to_exexes(&mut self, cx: &mut Context<'_>) {
        if self.buffer.is_empty() {
            return;
        }

        // Track which notifications have been fully sent to all ExExes
        let mut min_delivered_id = usize::MAX;

        for handle in &mut self.exex_handles {
            // Skip if this ExEx has already processed past the first buffered notification
            if let Some((_, first_notif)) = self.buffer.front() {
                if let Some(chain) = first_notif.committed_chain() {
                    // Only check the tip if the chain has blocks
                    if !chain.blocks().is_empty() {
                        let tip = chain.tip();
                        let tip_number = tip.number;
                        if let Some(finished) = handle.finished_height {
                            if finished.number >= tip_number {
                                // ExEx is already ahead, skip all buffered notifications
                                if let Some((last_id, _)) = self.buffer.back() {
                                    min_delivered_id = min_delivered_id.min(*last_id);
                                }
                                continue;
                            }
                        }
                    }
                }
            }

            // Try to send all buffered notifications to this ExEx
            let mut last_sent_id = None;
            for (id, notification) in &self.buffer {
                if *id < handle.next_notification_id {
                    // Already sent to this ExEx
                    last_sent_id = Some(*id);
                    continue;
                }

                match handle.sender.poll_reserve(cx) {
                    Poll::Ready(Ok(())) => {
                        match handle.sender.send_item(notification.clone()) {
                            Ok(()) => {
                                handle.next_notification_id = *id + 1;
                                last_sent_id = Some(*id);
                                
                                metrics::counter!("tn_exex_notifications_sent_total").increment(1);
                            }
                            Err(_) => {
                                // Channel closed - ExEx died
                                tracing::error!(exex_id = %handle.id, "ExEx notification channel closed");
                                break;
                            }
                        }
                    }
                    Poll::Ready(Err(_)) => {
                        // Channel closed
                        break;
                    }
                    Poll::Pending => {
                        // ExEx is backpressured, will try again next poll
                        break;
                    }
                }
            }

            if let Some(id) = last_sent_id {
                min_delivered_id = min_delivered_id.min(id);
            }
        }

        // Remove notifications that have been delivered to all ExExes
        if min_delivered_id != usize::MAX {
            self.buffer.retain(|(id, _)| *id > min_delivered_id);
            metrics::gauge!("tn_exex_buffer_size").set(self.buffer.len() as f64);

            // Update capacity
            let remaining = self.max_capacity.saturating_sub(self.buffer.len());
            self.current_capacity.store(remaining, Ordering::Relaxed);
            let is_ready = remaining > 0;
            let _ = self.is_ready.send(is_ready);
        }
    }
}

impl Future for TnExExManager {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // 1. Handle incoming events from ExExes
        this.handle_exex_events(cx);

        // 2. Receive new notifications from engine
        this.receive_new_notifications(cx);

        // 3. Receive consensus-layer notifications from ConsensusBus
        this.receive_consensus_notifications();

        // 4. Send buffered notifications to ExExes
        this.send_notifications_to_exexes(cx);

        // Manager runs indefinitely
        Poll::Pending
    }
}

impl fmt::Debug for TnExExManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TnExExManager")
            .field("num_exexs", &self.exex_handles.len())
            .field("buffer_size", &self.buffer.len())
            .field("max_capacity", &self.max_capacity)
            .field("current_capacity", &self.current_capacity.load(Ordering::Relaxed))
            .finish()
    }
}

/// Cloneable handle for sending notifications to the manager.
///
/// This handle is used by the execution engine to send notifications.
/// It provides backpressure awareness and can be cloned for use in multiple locations.
#[derive(Clone)]
pub struct TnExExManagerHandle {
    /// Number of ExExes being managed.
    num_exexs: usize,

    /// Channel for sending notifications to the manager.
    /// None if this is an empty handle.
    handle_tx: Option<mpsc::UnboundedSender<TnExExNotification>>,

    /// Shared capacity counter.
    current_capacity: Arc<AtomicUsize>,

    /// Watch receiver for readiness signal.
    is_ready: watch::Receiver<bool>,

    /// Watch receiver for the finished height across all ExExes.
    finished_height: watch::Receiver<FinishedTnExExHeight>,
}

impl TnExExManagerHandle {
    /// Creates an empty handle for when no ExExes are installed.
    ///
    /// This allows the engine to use the same code path regardless of whether
    /// ExExes are configured. All operations on an empty handle are no-ops.
    pub fn empty() -> Self {
        let (_, is_ready_rx) = watch::channel(true);
        let (_, finished_height_rx) = watch::channel(FinishedTnExExHeight::default());
        
        Self {
            num_exexs: 0,
            handle_tx: None,
            current_capacity: Arc::new(AtomicUsize::new(0)),
            is_ready: is_ready_rx,
            finished_height: finished_height_rx,
        }
    }

    /// Returns whether this handle has any ExExes registered.
    pub fn has_exexs(&self) -> bool {
        self.num_exexs > 0
    }

    /// Returns whether the manager has capacity to accept more notifications.
    pub fn has_capacity(&self) -> bool {
        self.current_capacity.load(Ordering::Relaxed) > 0
    }

    /// Sends a notification to all registered ExExes (synchronous, non-blocking).
    ///
    /// This will queue the notification for delivery but may fail if the internal
    /// channel is full or closed.
    pub fn send(&self, notification: TnExExNotification) {
        if let Some(tx) = &self.handle_tx {
            let _ = tx.send(notification);
        }
    }

    /// Sends a canonical state notification from Reth to all ExExes.
    ///
    /// Converts the Reth notification to TnExExNotification before sending.
    /// Reorg notifications are logged as errors and skipped — Bullshark consensus
    /// should never produce reorgs, but a bug elsewhere must not crash the node.
    pub fn send_canon_notification(&self, notification: reth_chain_state::CanonStateNotification) {
        match TnExExNotification::try_from_canon_state(notification) {
            Some(exex_notification) => self.send(exex_notification),
            None => {
                tracing::error!(
                    target: "exex",
                    "Received unexpected reorg notification — Bullshark consensus should never reorg. \
                     ExEx notification skipped. This may indicate a bug in the execution layer."
                );
            }
        }
    }

    /// Sends a notification to all registered ExExes (async with backpressure).
    ///
    /// This will wait until the manager has capacity before returning.
    pub async fn send_async(&self, notification: TnExExNotification) -> Result<(), ()> {
        if let Some(tx) = &self.handle_tx {
            // Wait for capacity if needed
            let mut is_ready = self.is_ready.clone();
            while !*is_ready.borrow_and_update() {
                is_ready.changed().await.map_err(|_| ())?;
            }

            tx.send(notification).map_err(|_| ())?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// Returns a watch receiver for the lowest finished height across all ExExes.
    pub fn finished_height(&self) -> watch::Receiver<FinishedTnExExHeight> {
        self.finished_height.clone()
    }
}

impl fmt::Debug for TnExExManagerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TnExExManagerHandle")
            .field("num_exexs", &self.num_exexs)
            .field("has_channel", &self.handle_tx.is_some())
            .field("current_capacity", &self.current_capacity.load(Ordering::Relaxed))
            .finish()
    }
}

/// Implement the minimal ExEx handle interface from tn-reth.
///
/// This allows tn-reth to use the handle without depending on the full tn-exex crate,
/// avoiding circular dependencies.
impl tn_reth::exex_handle::ExExNotificationHandle for TnExExManagerHandle {
    fn has_exexs(&self) -> bool {
        self.has_exexs()
    }

    fn send_canon_notification(&self, notification: reth_chain_state::CanonStateNotification) {
        self.send_canon_notification(notification)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_finished_height_update() {
        let mut height = FinishedTnExExHeight::default();
        assert_eq!(height.height, None);

        let block1 = BlockNumHash::new(100, Default::default());
        height.update(Some(block1));
        assert_eq!(height.height, Some(block1));

        let block2 = BlockNumHash::new(50, Default::default());
        height.update(Some(block2));
        assert_eq!(height.height, Some(block2)); // Should pick lower

        let block3 = BlockNumHash::new(150, Default::default());
        height.update(Some(block3));
        assert_eq!(height.height, Some(block2)); // Should keep lower
    }

    #[tokio::test]
    async fn test_empty_handle() {
        let handle = TnExExManagerHandle::empty();
        
        assert!(!handle.has_exexs());
        assert!(!handle.has_capacity());

        // Should be no-op
        handle.send(TnExExNotification::ChainCommitted {
            new: Arc::new(reth_provider::Chain::default()),
        });

        let result = handle.send_async(TnExExNotification::ChainCommitted {
            new: Arc::new(reth_provider::Chain::default()),
        }).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_manager_creation() {
        let node_head = BlockNumHash::new(0, Default::default());
        let (handle1, _event_tx1, _notif_rx1) = TnExExHandle::new("exex1".to_string(), node_head);
        let (handle2, _event_tx2, _notif_rx2) = TnExExHandle::new("exex2".to_string(), node_head);

        let (_manager, mgr_handle) = TnExExManager::new(vec![handle1, handle2], Some(10), None, None);

        assert!(mgr_handle.has_exexs());
        assert!(mgr_handle.has_capacity());
    }

    #[tokio::test]
    async fn test_notification_delivery() {
        let node_head = BlockNumHash::new(0, Default::default());
        let (handle, _event_tx, mut notif_rx) = TnExExHandle::new("test_exex".to_string(), node_head);

        let (manager, mgr_handle) = TnExExManager::new(vec![handle], Some(10), None, None);

        // Pin the manager for polling
        let mut manager = Box::pin(manager);

        // Spawn manager task
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    Poll::Ready(())
                }).await;
            }
        });

        // Send notification
        let notification = TnExExNotification::ChainCommitted {
            new: Arc::new(reth_provider::Chain::default()),
        };
        mgr_handle.send(notification.clone());

        // Give it time to process
        sleep(Duration::from_millis(100)).await;

        // ExEx should receive it
        let received = tokio::time::timeout(Duration::from_secs(1), notif_rx.recv()).await;
        assert!(received.is_ok());
        assert!(received.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_manager_receives_certificate_broadcast() {
        let node_head = BlockNumHash::new(0, Default::default());
        let (handle, _event_tx, mut notif_rx) =
            TnExExHandle::new("cert_exex".to_string(), node_head);

        // Create a broadcast channel for certificates
        let (cert_tx, cert_rx) = broadcast::channel::<Arc<Certificate>>(16);

        let (manager, _mgr_handle) =
            TnExExManager::new(vec![handle], Some(10), Some(cert_rx), None);

        let mut manager = Box::pin(manager);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    Poll::Ready(())
                })
                .await;
            }
        });

        // Send a certificate through the broadcast channel
        let cert = Arc::new(Certificate::default());
        cert_tx.send(cert.clone()).unwrap();

        // ExEx should receive the certificate as a notification
        let received = tokio::time::timeout(Duration::from_secs(2), notif_rx.recv()).await;
        assert!(received.is_ok(), "Should receive notification");
        let notification = received.unwrap().unwrap();
        assert!(
            notification.certificate().is_some(),
            "Should be a CertificateCreated notification"
        );
    }

    #[tokio::test]
    async fn test_manager_receives_committed_sub_dag_broadcast() {
        let node_head = BlockNumHash::new(0, Default::default());
        let (handle, _event_tx, mut notif_rx) =
            TnExExHandle::new("dag_exex".to_string(), node_head);

        // Create a broadcast channel for committed sub-DAGs
        let (dag_tx, dag_rx) = broadcast::channel::<Arc<CommittedSubDag>>(16);

        let (manager, _mgr_handle) =
            TnExExManager::new(vec![handle], Some(10), None, Some(dag_rx));

        let mut manager = Box::pin(manager);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    Poll::Ready(())
                })
                .await;
            }
        });

        // Send a committed sub-DAG through the broadcast channel
        let sub_dag = Arc::new(CommittedSubDag::default());
        dag_tx.send(sub_dag.clone()).unwrap();

        // ExEx should receive the sub-DAG as a notification
        let received = tokio::time::timeout(Duration::from_secs(2), notif_rx.recv()).await;
        assert!(received.is_ok(), "Should receive notification");
        let notification = received.unwrap().unwrap();
        assert!(
            notification.committed_sub_dag().is_some(),
            "Should be a ConsensusCommitted notification"
        );
    }

    #[tokio::test]
    async fn test_manager_no_consensus_receivers() {
        // When no consensus receivers are provided, manager should still work for chain notifications
        let node_head = BlockNumHash::new(0, Default::default());
        let (handle, _event_tx, mut notif_rx) =
            TnExExHandle::new("chain_only".to_string(), node_head);

        let (manager, mgr_handle) = TnExExManager::new(vec![handle], Some(10), None, None);

        let mut manager = Box::pin(manager);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                std::future::poll_fn(|cx| {
                    let _ = Pin::new(&mut manager).poll(cx);
                    Poll::Ready(())
                })
                .await;
            }
        });

        // Send a chain notification
        mgr_handle.send(TnExExNotification::ChainCommitted {
            new: Arc::new(reth_provider::Chain::default()),
        });

        let received = tokio::time::timeout(Duration::from_secs(1), notif_rx.recv()).await;
        assert!(received.is_ok());
        let notification = received.unwrap().unwrap();
        assert!(notification.committed_chain().is_some());
    }
}
