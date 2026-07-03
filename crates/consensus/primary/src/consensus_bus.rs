//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, metrics::PrimaryMetrics,
    proposer::OurDigestMessage, state_sync::CertificateManagerCommand, RecentBlocks,
};
use parking_lot::Mutex;
use std::{
    error::Error,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tn_config::Parameters;
use tn_network_libp2p::types::NetworkEvent;
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    deconstruct_nonce, BlockNumHash, Certificate, CommittedSubDag, ConsensusHeader,
    ConsensusHeaderDigest, ConsensusOutput, Epoch, EpochRecord, EpochVote, Header, Round,
    TnReceiver, TnSender, CHANNEL_CAPACITY,
};
use tokio::{
    sync::{
        broadcast, mpsc,
        watch::{self, error::RecvError},
    },
    time::error::Elapsed,
};

/// Capacity for the `sync_output` broadcast.
///
/// Items are full [`ConsensusOutput`]s (subdag + batches), so a deep buffer costs hundreds of
/// MB when a follower lags. The state-sync producer waits for execution to catch up before each
/// send and fails fast on a digest-chain mismatch, so a deep buffer buys nothing: 1_000 bounds
/// worst-case memory while still absorbing bursts.
const SYNC_OUTPUT_CHANNEL_CAPACITY: usize = 1_000;
/// Capacity for the `exex_certificates` broadcast.
///
/// Certificates are small but arrive every round forever; a lagging ExEx reconciles via its
/// native `Lagged` handling rather than needing a deep buffer.
const EXEX_CERTIFICATES_CHANNEL_CAPACITY: usize = 1_000;
/// Capacity for the `exex_consensus_output` broadcast.
///
/// Items are full [`ConsensusOutput`]s. ExEx handles `Lagged` natively (surfaced as
/// `TnExExNotification::Lagged` reconciliation), so match the engine's `consensus_output`
/// capacity instead of buffering hundreds of MB for a slow ExEx.
const EXEX_CONSENSUS_OUTPUT_CHANNEL_CAPACITY: usize = 100;

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
}

impl<T> QueChannel<T> {
    /// Create a new QueChannel.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        let subscribed = Arc::new(AtomicBool::new(false));
        Self { channel: tx, receiver, subscribed, always_subscribed: false }
    }

    /// Create a new QueChannel that will que messages even when no subscribers.
    pub fn new_always_subscribed() -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        let subscribed = Arc::new(AtomicBool::new(true));
        Self { channel: tx, receiver, subscribed, always_subscribed: true }
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
        }
    }
}

impl<T: Send + 'static> TnSender<T> for QueChannel<T> {
    async fn send(&self, value: T) -> Result<(), tn_types::SendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(());
        }
        Ok(self.channel.send(value).await?)
    }

    fn try_send(&self, value: T) -> Result<(), tn_types::TrySendError<T>> {
        if !self.subscribed.load(Ordering::Acquire) {
            return Ok(());
        }
        Ok(self.channel.try_send(value)?)
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
    /// Watch tracking the last gossipped consensus block number and hash.
    tx_last_published_consensus_num_hash: watch::Sender<(Epoch, u64, ConsensusHeaderDigest)>,

    /// Verified consensus OUTPUTs (header + batches) delivered to a following/catching-up
    /// subscriber for execution. Filled by the state-sync forward drain; used only by
    /// non-active nodes.
    sync_output: broadcast::Sender<ConsensusOutput>,
    /// Broadcast the latest output from consensus after committing to the subdag.
    /// Engine consumes and executes to extend canonical chain.
    consensus_output: broadcast::Sender<ConsensusOutput>,

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
    epoch_request_queue_tx: tokio::sync::mpsc::Sender<(EpochRecord, EpochRecord)>,
    /// Reciever for epoch records to download pack files for.
    epoch_request_queue_rx:
        Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(EpochRecord, EpochRecord)>>>,
    /// Channel to request consensus headers to cache.
    consensus_request_queue: QueChannel<(Epoch, u64, ConsensusHeaderDigest)>,
    /// Prometheus metrics for the primary's consensus pipeline.
    ///
    /// Lives on the app-lifetime bus because the bus already reaches every consensus
    /// component (proposer, certifier, certificate manager, consensus driver).
    metrics: PrimaryMetrics,
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

        let (tx_recent_blocks, _) = watch::channel(RecentBlocks::new(recent_blocks as usize));
        let (tx_sync_status, _) = watch::channel(NodeMode::default());

        let (sync_output, _rx_sync_output) = broadcast::channel(SYNC_OUTPUT_CHANNEL_CAPACITY);
        let (consensus_output, _rx_consensus_output) = broadcast::channel(100);

        let (exex_certificates, _) = broadcast::channel(EXEX_CERTIFICATES_CHANNEL_CAPACITY);
        let (exex_consensus_output, _) = broadcast::channel(EXEX_CONSENSUS_OUTPUT_CHANNEL_CAPACITY);

        let (tx_epoch_record, _) = watch::channel(None);

        let (epoch_request_queue_tx, epochs_rx) = tokio::sync::mpsc::channel(1024);
        let epoch_request_queue_rx = Arc::new(tokio::sync::Mutex::new(epochs_rx));
        Self {
            inner: Arc::new(ConsensusBusAppInner {
                tx_committed_round_updates,
                tx_requested_missing_epoch,

                tx_primary_round_updates,
                tx_recent_blocks,
                tx_last_consensus_header,
                tx_last_published_consensus_num_hash,
                sync_output,
                consensus_output,
                exex_certificates,
                exex_consensus_output,
                tx_sync_status,
                new_epoch_votes: QueChannel::new(),
                tx_epoch_record,
                primary_network_events: QueChannel::new_always_subscribed(),
                epoch_request_queue_tx,
                epoch_request_queue_rx,
                consensus_request_queue: QueChannel::new(),
                // new_with_labels (not Default): binds to the recorder active at bus
                // construction instead of caching the first-bound recorder process-wide
                metrics: PrimaryMetrics::new_with_labels(Vec::<metrics::Label>::new()),
            }),
        }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    pub fn reset_for_epoch(&self) {
        self.inner.tx_committed_round_updates.send_replace(Round::default());
        self.inner.tx_primary_round_updates.send_replace(0u32);
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

    /// Broadcast channel with consensus output (includes the consensus chain block).
    /// This also provides the ConsesusHeader, use this for block execution.
    pub fn consensus_output(&self) -> &impl TnSender<ConsensusOutput> {
        &self.inner.consensus_output
    }

    /// Broadcast channel delivering verified consensus OUTPUTs (header + batches) to a
    /// following/catching-up subscriber for execution. Used when not participating in consensus.
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
    pub fn new_epoch_votes(&self) -> &impl TnSender<EpochVote> {
        &self.inner.new_epoch_votes
    }

    /// Watch channel for the current epoch record.
    /// The epoch vote collector observes this to know when a new epoch starts.
    pub fn epoch_record_watch(&self) -> &watch::Sender<Option<EpochRecord>> {
        &self.inner.tx_epoch_record
    }

    /// Provide a subscriber (Receiver) for new_epoch_votes.
    pub fn subscribe_new_epoch_votes(&self) -> impl TnReceiver<EpochVote> {
        self.inner.new_epoch_votes.subscribe()
    }

    /// Provide a subscriber (Receiver) for primary network events.
    pub fn subscribe_primary_network_events(
        &self,
    ) -> impl TnReceiver<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner.primary_network_events.subscribe()
    }

    /// Provide a subscription (Receiver) for consensus output.
    pub fn subscribe_consensus_output(&self) -> impl TnReceiver<ConsensusOutput> {
        self.inner.consensus_output.subscribe()
    }

    /// Provide a subscription(Receiver) to verified sync consensus outputs.
    pub fn subscribe_sync_output(&self) -> impl TnReceiver<ConsensusOutput> {
        self.inner.sync_output.subscribe()
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
        } else {
            // Failed to find our block at it's number.
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
    pub async fn last_executed_consensus_block(
        &self,
        consensus_chain: &ConsensusChain,
    ) -> Option<ConsensusHeader> {
        let block = self.recent_blocks().borrow().latest_execution_block();
        let header = block.header();
        let (epoch, _) = deconstruct_nonce(header.nonce.into());
        let parent_beacon_block_root = header.parent_beacon_block_root;
        if let Some(consensus_hash) = parent_beacon_block_root {
            consensus_chain
                .consensus_header_by_digest(epoch, consensus_hash.into())
                .await
                .unwrap_or_default()
        } else {
            None
        }
    }

    /// Returns the ConsensusHeader that was processed.
    /// If we are not starting at genesis or a new epoch, then not finding this indicates a database
    /// issue.
    pub async fn last_consensus_block(
        &self,
        consensus_chain: &ConsensusChain,
    ) -> Option<ConsensusHeader> {
        let latest_consensus = self.recent_blocks().borrow().latest_consensus_block_num_hash();
        let epoch = consensus_chain.epochs().number_to_epoch(latest_consensus.number);
        consensus_chain
            .consensus_header_by_digest(epoch, latest_consensus.hash)
            .await
            .unwrap_or_default()
    }

    /// Send a request to download the epoch pack file for the provided EpochRecord.
    pub async fn request_epoch_pack_file(
        &self,
        previous_epoch_record: EpochRecord,
        epoch_record: EpochRecord,
    ) {
        let _ = self.inner.epoch_request_queue_tx.send((previous_epoch_record, epoch_record)).await;
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
        if let Some(previous_epoch_record) = maybe_previous {
            if let Some(epoch_record) =
                consensus_chain.epochs().record_by_epoch(current_epoch).await
            {
                let contains_final_header = consensus_chain.is_epoch_complete(&epoch_record).await;
                // If the pack file is missing or incomplete request it.
                // Note since we have an epoch record this is a past epoch
                // not the current epoch.
                if !contains_final_header {
                    self.request_epoch_pack_file(previous_epoch_record, epoch_record.clone()).await;
                }
            }
        }
    }

    /// Retrieve the next request to down load an epoch pack file.
    /// Will not resolve until a request is ready and will only ever
    /// provide each request once.  Returns None when the underlying channel closes.
    pub async fn get_next_epoch_pack_file_request(&self) -> Option<(EpochRecord, EpochRecord)> {
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
