//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, proposer::OurDigestMessage,
    state_sync::CertificateManagerCommand, RecentBlocks,
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
use tn_types::{
    BlockHash, BlockNumHash, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, Epoch,
    EpochRecord, EpochVote, Header, Round, TnReceiver, TnSender, CHANNEL_CAPACITY,
};
use tokio::{
    sync::{
        broadcast, mpsc,
        watch::{self, error::RecvError},
    },
    time::error::Elapsed,
};

/// Wrapper around a receiver and a subs count to make sure only one of these exists at a time.
/// Note this does NOT implement Clone on purpose, do not implement it else managing subscriptions
/// will break.
#[derive(Debug)]
struct QueChanReceiver<T> {
    receiver: Option<mpsc::Receiver<T>>,
    container: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
    /// Flag to signal the sender that this receiver has been dropped.
    subscribed: Arc<AtomicBool>,
}

/// Use the Drop to decrement subs and signal unsubscribed.
impl<T> Drop for QueChanReceiver<T> {
    fn drop(&mut self) {
        self.subscribed.store(false, Ordering::Release);
        (*self.container.lock()) = self.receiver.take();
    }
}

/// Wrapper around an mpsc channel.  It allows a channel to exist for application lifetime
/// even if used for epoch messages.  It tracks subscibers so that each epoch will be able to
/// "subscribe" to the channel (after the last epoch has dropped it's subscription).
#[derive(Debug)]
pub struct QueChannel<T> {
    channel: mpsc::Sender<T>,
    // Putting this in a lock is unfortunate but if want an mpsc under the hood is needed.
    receiver: Arc<Mutex<Option<mpsc::Receiver<T>>>>,
    /// Tracks whether a receiver is currently subscribed.
    /// When `false`, `send()` and `try_send()` become no-ops.
    subscribed: Arc<AtomicBool>,
}

impl<T> QueChannel<T> {
    /// Create a new QueChannel.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let receiver = Arc::new(Mutex::new(Some(rx)));
        let subscribed = Arc::new(AtomicBool::new(false));
        Self { channel: tx, receiver, subscribed }
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
#[derive(Clone, Debug)]
struct ConsensusBusAppInner {
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
    tx_last_published_consensus_num_hash: watch::Sender<(u64, BlockHash)>,

    /// Consensus header.  Note this can be used to create consensus output to execute for non
    /// validators.
    consensus_header: broadcast::Sender<ConsensusHeader>,
    /// Broadcast the latest output from consensus after committing to the subdag.
    /// Engine consumes and executes to extend canonical chain.
    consensus_output: broadcast::Sender<ConsensusOutput>,
    /// Status of sync?
    tx_sync_status: watch::Sender<NodeMode>,

    /// Produce new epoch certs as they are recieved.
    new_epoch_votes: QueChannel<EpochVote>,
    /// Watch channel to communicate the current epoch record to the vote collector.
    tx_epoch_record: watch::Sender<Option<EpochRecord>>,
    /// The que channel for primary network events.
    primary_network_events: QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>>,
}

impl ConsensusBusAppInner {
    fn new(recent_blocks: u32) -> Self {
        let (tx_committed_round_updates, _) = watch::channel(Round::default());

        let (tx_requested_missing_epoch, _) = watch::channel(Epoch::default());

        let (tx_primary_round_updates, _) = watch::channel(0u32);
        let (tx_last_consensus_header, _) = watch::channel(None);
        let (tx_last_published_consensus_num_hash, _) = watch::channel((0, BlockHash::default()));

        let (tx_recent_blocks, _) = watch::channel(RecentBlocks::new(recent_blocks as usize));
        let (tx_sync_status, _) = watch::channel(NodeMode::default());

        let (consensus_header, _rx_consensus_header) = broadcast::channel(CHANNEL_CAPACITY);
        let (consensus_output, _rx_consensus_output) = broadcast::channel(100);

        let (tx_epoch_record, _) = watch::channel(None);

        Self {
            tx_committed_round_updates,
            tx_requested_missing_epoch,

            tx_primary_round_updates,
            tx_recent_blocks,
            tx_last_consensus_header,
            tx_last_published_consensus_num_hash,
            consensus_header,
            consensus_output,
            tx_sync_status,
            new_epoch_votes: QueChannel::new(),
            tx_epoch_record,
            primary_network_events: QueChannel::new(),
        }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    fn reset_for_epoch(&self) {
        self.tx_committed_round_updates.send_replace(Round::default());
        self.tx_primary_round_updates.send_replace(0u32);
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
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    committed_certificates: QueChannel<(Round, Vec<Certificate>)>,

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
            committed_certificates: QueChannel::new(),
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
    inner_app: Arc<ConsensusBusAppInner>,
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
        let inner_app = Arc::new(ConsensusBusAppInner::new(recent_blocks));
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new());
        Self { inner_app, inner_epoch }
    }

    /// Reset for a new epoch.
    /// This is primarily so we can resubscribe to "one-time" subscription channels.
    pub fn reset_for_epoch(&mut self) {
        self.inner_app.reset_for_epoch();
        let inner_epoch = Arc::new(ConsensusBusEpochInner::new());
        self.inner_epoch = inner_epoch;
    }

    /// New certificates.
    ///
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    /// Can only be subscribed to once.
    pub fn new_certificates(&self) -> &impl TnSender<Certificate> {
        &self.inner_epoch.new_certificates
    }

    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    /// Can only be subscribed to once.
    pub fn committed_certificates(&self) -> &impl TnSender<(Round, Vec<Certificate>)> {
        &self.inner_epoch.committed_certificates
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

    /// Contains the highest committed round & corresponding gc_round for consensus.
    pub fn committed_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner_app.tx_committed_round_updates
    }

    /// Returns the current committed round value.
    pub fn committed_round(&self) -> Round {
        *self.inner_app.tx_committed_round_updates.borrow()
    }

    /// Contains the last requested epoch to retrieve a record.
    pub fn requested_missing_epoch(&self) -> &watch::Sender<Epoch> {
        &self.inner_app.tx_requested_missing_epoch
    }

    /// Signals a new round
    pub fn primary_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner_app.tx_primary_round_updates
    }

    /// Returns the current primary round value.
    pub fn primary_round(&self) -> Round {
        *self.inner_app.tx_primary_round_updates.borrow()
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

    /// Track recent blocks.
    pub fn recent_blocks(&self) -> &watch::Sender<RecentBlocks> {
        &self.inner_app.tx_recent_blocks
    }

    /// Returns the latest executed block's number and hash.
    pub fn latest_execution_block_num_hash(&self) -> BlockNumHash {
        self.inner_app.tx_recent_blocks.borrow().latest_execution_block_num_hash()
    }

    /// Returns the last consensus round processed by the engine.
    pub fn last_consensus_round(&self) -> Round {
        self.inner_app.tx_recent_blocks.borrow().last_consensus_round()
    }

    /// Returns the maximum number of recent blocks that can be held.
    pub fn recent_blocks_capacity(&self) -> u64 {
        self.inner_app.tx_recent_blocks.borrow().block_capacity()
    }

    /// Track the latest consensus header we have seen.
    /// Note, this should be a valid header (authenticated by it's epoch's committee).
    pub fn last_consensus_header(&self) -> &watch::Sender<Option<ConsensusHeader>> {
        &self.inner_app.tx_last_consensus_header
    }

    /// Track the latest published consensus header block number and hash seen on the gossip
    /// network. This value will have been verified and can be trusted to be the correct hash
    /// for block number.  DO NOT send unverified values to this watch.
    pub fn last_published_consensus_num_hash(&self) -> &watch::Sender<(u64, BlockHash)> {
        &self.inner_app.tx_last_published_consensus_num_hash
    }

    /// Returns the latest verified consensus block number and hash from gossip.
    pub fn published_consensus_num_hash(&self) -> (u64, BlockHash) {
        *self.inner_app.tx_last_published_consensus_num_hash.borrow()
    }

    /// Broadcast channel with consensus output (includes the consensus chain block).
    /// This also provides the ConsesusHeader, use this for block execution.
    pub fn consensus_output(&self) -> &impl TnSender<ConsensusOutput> {
        &self.inner_app.consensus_output
    }

    /// Broadcast channel with consensus header.
    /// This is useful pre-consensus output when not participating in consensus.
    pub fn consensus_header(&self) -> &impl TnSender<ConsensusHeader> {
        &self.inner_app.consensus_header
    }

    /// Status of initial sync operation.
    pub fn node_mode(&self) -> &watch::Sender<NodeMode> {
        &self.inner_app.tx_sync_status
    }

    /// Returns the current node mode.
    pub fn current_node_mode(&self) -> NodeMode {
        *self.inner_app.tx_sync_status.borrow()
    }

    /// Returns true if this node is a CVV (active or inactive).
    ///
    /// A CVV is a staked node that can participate in a committee,
    /// regardless of whether it's currently active or catching up.
    pub fn is_cvv(&self) -> bool {
        self.inner_app.tx_sync_status.borrow().is_cvv()
    }

    /// Returns true if this node is an active CVV (Committee Voting Validator).
    ///
    /// This is a helper method that borrows the node mode watch channel
    /// and checks if the node is actively participating in consensus.
    pub fn is_active_cvv(&self) -> bool {
        self.inner_app.tx_sync_status.borrow().is_active_cvv()
    }

    /// Returns true if this node is an inactive CVV.
    ///
    /// An inactive CVV is a staked node that is catching up to rejoin
    /// the committee after a failure during the epoch.
    pub fn is_cvv_inactive(&self) -> bool {
        self.inner_app.tx_sync_status.borrow().is_cvv_inactive()
    }

    /// Return the channel for primary network events.
    pub fn primary_network_events(
        &self,
    ) -> &impl TnSender<NetworkEvent<crate::network::Req, crate::network::Res>> {
        &self.inner_app.primary_network_events
    }

    /// Return the channel for primary network events.  Returns a concrete clone.
    pub fn primary_network_events_cloned(
        &self,
    ) -> QueChannel<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner_app.primary_network_events.clone()
    }

    /// New epoch certs as they are recieved.
    pub fn new_epoch_votes(&self) -> &impl TnSender<EpochVote> {
        &self.inner_app.new_epoch_votes
    }

    /// Watch channel for the current epoch record.
    /// The epoch vote collector observes this to know when a new epoch starts.
    pub fn epoch_record_watch(&self) -> &watch::Sender<Option<EpochRecord>> {
        &self.inner_app.tx_epoch_record
    }

    //
    //=== Channel subscription methods
    //
    // These must be called in synchronous spawn() methods, BEFORE spawning async tasks.
    // This ensures the channel is active before any messages are sent.
    //

    pub fn subscribe_new_certificates(&self) -> impl TnReceiver<Certificate> {
        self.inner_epoch.new_certificates.subscribe()
    }

    pub fn subscribe_committed_certificates(&self) -> impl TnReceiver<(Round, Vec<Certificate>)> {
        self.inner_epoch.committed_certificates.subscribe()
    }

    pub fn subscribe_certificate_fetcher(&self) -> impl TnReceiver<CertificateFetcherCommand> {
        self.inner_epoch.certificate_fetcher.subscribe()
    }

    pub fn subscribe_parents(&self) -> impl TnReceiver<(Vec<Certificate>, Round)> {
        self.inner_epoch.parents.subscribe()
    }

    pub fn subscribe_our_digests(&self) -> impl TnReceiver<OurDigestMessage> {
        self.inner_epoch.our_digests.subscribe()
    }

    pub fn subscribe_headers(&self) -> impl TnReceiver<Header> {
        self.inner_epoch.headers.subscribe()
    }

    pub fn subscribe_committed_own_headers(&self) -> impl TnReceiver<(Round, Vec<Round>)> {
        self.inner_epoch.committed_own_headers.subscribe()
    }

    pub fn subscribe_sequence(&self) -> impl TnReceiver<CommittedSubDag> {
        self.inner_epoch.sequence.subscribe()
    }

    pub(crate) fn subscribe_certificate_manager(
        &self,
    ) -> impl TnReceiver<CertificateManagerCommand> {
        self.inner_epoch.certificate_manager.subscribe()
    }

    pub fn subscribe_new_epoch_votes(&self) -> impl TnReceiver<EpochVote> {
        self.inner_app.new_epoch_votes.subscribe()
    }

    pub fn subscribe_primary_network_events(
        &self,
    ) -> impl TnReceiver<NetworkEvent<crate::network::Req, crate::network::Res>> {
        self.inner_app.primary_network_events.subscribe()
    }

    pub fn subscribe_consensus_output(&self) -> impl TnReceiver<ConsensusOutput> {
        self.inner_app.consensus_output.subscribe()
    }

    pub fn subscribe_consensus_header(&self) -> impl TnReceiver<ConsensusHeader> {
        self.inner_app.consensus_header.subscribe()
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
        hash: BlockHash,
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
    pub fn last_executed_consensus_block<DB: tn_types::Database>(
        &self,
        db: &DB,
    ) -> Option<ConsensusHeader> {
        use tn_storage::ConsensusStore as _;
        let last = self
            .recent_blocks()
            .borrow()
            .latest_execution_block()
            .header()
            .parent_beacon_block_root
            .and_then(|hash| db.get_consensus_by_hash(hash));

        last
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
