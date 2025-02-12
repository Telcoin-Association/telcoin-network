//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, consensus::ConsensusRound,
    proposer::OurDigestMessage, state_sync::CertificateManagerCommand, RecentBlocks,
};
use consensus_metrics::metered_channel::{self, channel_with_total_sender, MeteredMpscChannel};
use std::sync::{atomic::AtomicBool, Arc};
use tn_config::Parameters;
use tn_network_libp2p::GossipMessage;
use tn_primary_metrics::{ChannelMetrics, ConsensusMetrics, ExecutorMetrics, Metrics};
use tn_types::{
    Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput, Header, Round, TnSender,
    CHANNEL_CAPACITY,
};
use tokio::sync::{
    broadcast,
    watch::{self},
};

/// Has sync completed?
#[derive(Copy, Clone, Debug, Default)]
pub enum NodeMode {
    /// This is a full CVV that can participate in consensus.
    #[default]
    CvvActive,
    /// This node can only follow consensus via consensus output.
    /// It is staked and can be a CVV but is either currently not in the
    /// committee or is "catching up" to participate after a failure.
    CvvInactive,
    /// Node that is following consensus output but is not staked and will never
    /// join a committee.
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

    /// True if this node is only an obsever and will never participate in an committee.
    pub fn is_observer(&self) -> bool {
        matches!(self, NodeMode::Observer)
    }
}

#[derive(Debug)]
struct ConsensusBusInner {
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    new_certificates: MeteredMpscChannel<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    committed_certificates: MeteredMpscChannel<(Round, Vec<Certificate>)>,

    /// Outputs the highest committed round & corresponding gc_round in the consensus.
    tx_committed_round_updates: watch::Sender<Round>,
    /// Hold onto a receiver to keep it "open".
    _rx_committed_round_updates: watch::Receiver<Round>,

    /// Outputs the highest gc_round from the consensus.
    tx_gc_round_updates: watch::Sender<Round>,
    /// Hold onto a receiver to keep it "open".
    _rx_gc_round_updates: watch::Receiver<Round>,

    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    certificate_fetcher: MeteredMpscChannel<CertificateFetcherCommand>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    parents: MeteredMpscChannel<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    our_digests: MeteredMpscChannel<OurDigestMessage>,
    /// Sends newly created headers to the `Certifier`.
    headers: MeteredMpscChannel<Header>,
    /// Updates when headers were committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    committed_own_headers: MeteredMpscChannel<(Round, Vec<Round>)>,

    /// Outputs the sequence of ordered certificates to the application layer.
    sequence: MeteredMpscChannel<CommittedSubDag>,

    /// Messages to the Certificate Manager.
    certificate_manager: MeteredMpscChannel<CertificateManagerCommand>,

    /// Signals a new round
    tx_primary_round_updates: watch::Sender<Round>,
    /// Hold onto the primary metrics (allow early creation)
    _rx_primary_round_updates: watch::Receiver<Round>,

    /// Watch tracking most recent blocks
    tx_recent_blocks: watch::Sender<RecentBlocks>,
    /// Hold onto the recent blocks watch to keep it "open"
    _rx_recent_blocks: watch::Receiver<RecentBlocks>,

    /// Watch tracking most recently seen consensus header.
    tx_last_consensus_header: watch::Sender<ConsensusHeader>,
    /// Hold onto the consensus header watch to keep it "open"
    _rx_last_consensus_header: watch::Receiver<ConsensusHeader>,

    /// Consensus output with a consensus header.
    consensus_output: broadcast::Sender<ConsensusOutput>,
    /// Hold onto consensus output with a consensus header to keep it open.
    _rx_consensus_output: broadcast::Receiver<ConsensusOutput>,
    /// Consensus header.  Note this can be used to create consensus output to execute for non
    /// validators.
    consensus_header: broadcast::Sender<ConsensusHeader>,
    /// Hold onto consensus header to keep it open.
    _rx_consensus_header: broadcast::Receiver<ConsensusHeader>,

    /// Status of sync?
    tx_sync_status: watch::Sender<NodeMode>,
    /// Hold onto the recent sync_status to keep it "open"
    _rx_sync_status: watch::Receiver<NodeMode>,

    /// Hold onto the consensus_metrics (mostly for testing)
    consensus_metrics: Arc<ConsensusMetrics>,
    /// Hold onto the primary metrics (allow early creation)
    primary_metrics: Arc<Metrics>,
    /// Hold onto the channel metrics.
    channel_metrics: Arc<ChannelMetrics>,
    /// Hold onto the executor metrics.
    executor_metrics: Arc<ExecutorMetrics>,

    /// Flag to indicate a node should restart after a shutdown.
    restart: AtomicBool,
    /// Consensus output with a consensus header.
    consensus_network_gossip: broadcast::Sender<GossipMessage>,
    /// Hold onto consensus output with a consensus header to keep it open.
    _rx_consensus_network_gossip: broadcast::Receiver<GossipMessage>,
}

#[derive(Clone, Debug)]
pub struct ConsensusBus {
    inner: Arc<ConsensusBusInner>,
}

impl Default for ConsensusBus {
    fn default() -> Self {
        Self::new()
    }
}

/// This contains the shared consensus channels and the prometheus metrics
/// containers (used mostly to track consensus messages).
/// A new bus can be created with new() but there should only ever be one created (except for
/// tests). This allows us to not create and pass channels all over the place add-hoc.
/// It also allows makes it much easier to find where channels are fed and consumed.
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
        let consensus_metrics = Arc::new(ConsensusMetrics::default());
        let primary_metrics = Arc::new(Metrics::default()); // Initialize the metrics
        let channel_metrics = Arc::new(ChannelMetrics::default());
        let executor_metrics = Arc::new(ExecutorMetrics::default());
        let new_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_new_certificates,
        );

        let committed_certificates = metered_channel::channel_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_committed_certificates,
        );

        let (tx_committed_round_updates, _rx_committed_round_updates) =
            watch::channel(Round::default());

        let (tx_gc_round_updates, _rx_gc_round_updates) = watch::channel(Round::default());

        let our_digests = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_our_digests,
            &primary_metrics.primary_channel_metrics.tx_our_digests_total,
        );
        let parents = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_parents,
            &primary_metrics.primary_channel_metrics.tx_parents_total,
        );
        let headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_headers,
            &primary_metrics.primary_channel_metrics.tx_headers_total,
        );
        let certificate_fetcher = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_certificate_fetcher,
            &primary_metrics.primary_channel_metrics.tx_certificate_fetcher_total,
        );
        let committed_own_headers = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_committed_own_headers,
            &primary_metrics.primary_channel_metrics.tx_committed_own_headers_total,
        );

        let certificate_manager = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_metrics.primary_channel_metrics.tx_certificate_acceptor,
            &primary_metrics.primary_channel_metrics.tx_certificate_acceptor_total,
        );

        let (tx_primary_round_updates, _rx_primary_round_updates) = watch::channel(0u32);
        let (tx_last_consensus_header, _rx_last_consensus_header) =
            watch::channel(ConsensusHeader::default());

        let (tx_recent_blocks, _rx_recent_blocks) =
            watch::channel(RecentBlocks::new(recent_blocks as usize));
        let (tx_sync_status, _rx_sync_status) = watch::channel(NodeMode::default());

        let sequence =
            metered_channel::channel_sender(CHANNEL_CAPACITY, &channel_metrics.tx_sequence);

        let (consensus_output, _rx_consensus_output) = broadcast::channel(CHANNEL_CAPACITY);
        let (consensus_header, _rx_consensus_header) = broadcast::channel(CHANNEL_CAPACITY);

        let (consensus_network_gossip, _rx_consensus_network_gossip) =
            broadcast::channel(CHANNEL_CAPACITY);

        Self {
            inner: Arc::new(ConsensusBusInner {
                new_certificates,
                committed_certificates,
                tx_committed_round_updates,
                _rx_committed_round_updates,
                tx_gc_round_updates,
                _rx_gc_round_updates,
                certificate_fetcher,
                parents,
                our_digests,
                headers,
                committed_own_headers,
                sequence,
                certificate_manager,

                tx_primary_round_updates,
                _rx_primary_round_updates,
                tx_recent_blocks,
                _rx_recent_blocks,
                tx_last_consensus_header,
                _rx_last_consensus_header,
                consensus_output,
                _rx_consensus_output,
                consensus_header,
                _rx_consensus_header,
                tx_sync_status,
                _rx_sync_status,
                consensus_metrics,
                primary_metrics,
                channel_metrics,
                executor_metrics,
                restart: AtomicBool::new(false),
                consensus_network_gossip,
                _rx_consensus_network_gossip,
            }),
        }
    }

    /// New certificates.
    ///
    /// New certificates from the primary. The primary should send us new certificates
    /// only if it already sent us its whole history.
    /// Can only be subscribed to once.
    pub fn new_certificates(&self) -> &impl TnSender<Certificate> {
        &self.inner.new_certificates
    }

    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    /// Can only be subscribed to once.
    pub fn committed_certificates(&self) -> &impl TnSender<(Round, Vec<Certificate>)> {
        &self.inner.committed_certificates
    }

    /// Missing certificates.
    ///
    /// Sends missing certificates to the `CertificateFetcher`.
    /// Receives certificates with missing parents from the `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn certificate_fetcher(&self) -> &impl TnSender<CertificateFetcherCommand> {
        &self.inner.certificate_fetcher
    }

    /// Valid quorum of certificates' ids.
    ///
    /// Sends a valid quorum of certificates' ids to the `Proposer` (along with their round).
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    /// Can only be subscribed to once.
    pub fn parents(&self) -> &impl TnSender<(Vec<Certificate>, Round)> {
        &self.inner.parents
    }

    /// Contains the highest committed round & corresponding gc_round for consensus.
    pub fn committed_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner.tx_committed_round_updates
    }

    /// Contains the highest gc_round for consensus.
    pub fn gc_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner.tx_gc_round_updates
    }

    /// Signals a new round
    pub fn primary_round_updates(&self) -> &watch::Sender<Round> {
        &self.inner.tx_primary_round_updates
    }

    /// Batches' digests from our workers.
    /// Can only be subscribed to once.
    pub fn our_digests(&self) -> &impl TnSender<OurDigestMessage> {
        &self.inner.our_digests
    }

    /// Sends newly created headers to the `Certifier`.
    /// Can only be subscribed to once.
    pub fn headers(&self) -> &impl TnSender<Header> {
        &self.inner.headers
    }

    /// Updates when headers are committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    /// Can only be subscribed to once.
    pub fn committed_own_headers(&self) -> &impl TnSender<(Round, Vec<Round>)> {
        &self.inner.committed_own_headers
    }

    /// Outputs the sequence of ordered certificates from consensus.
    /// Can only be subscribed to once.
    pub fn sequence(&self) -> &impl TnSender<CommittedSubDag> {
        &self.inner.sequence
    }

    /// Channel for forwarding newly received certificates for verification.
    ///
    /// These channels are used to communicate with the long-running CertificateManager task.
    /// Can only be subscribed to once.
    pub(crate) fn certificate_manager(&self) -> &impl TnSender<CertificateManagerCommand> {
        &self.inner.certificate_manager
    }

    /// Track recent blocks.
    pub fn recent_blocks(&self) -> &watch::Sender<RecentBlocks> {
        &self.inner.tx_recent_blocks
    }

    /// Track the latest consensus header.
    pub fn last_consensus_header(&self) -> &watch::Sender<ConsensusHeader> {
        &self.inner.tx_last_consensus_header
    }

    /// Broadcast channel with consensus output (includes the consensus chain block).
    /// This also provides the ConsesusHeader, use this for block execution.
    pub fn consensus_output(&self) -> &impl TnSender<ConsensusOutput> {
        &self.inner.consensus_output
    }

    /// Broadcast subscriber with consensus output.
    /// This breaks the trait pattern in order to return a concrete receiver to pass to the
    /// execution module.
    pub fn subscribe_consensus_output(&self) -> broadcast::Receiver<ConsensusOutput> {
        self.inner.consensus_output.subscribe()
    }

    /// Broadcast channel with consensus header.
    /// This is useful pre-consensus output when not participating in consensus.
    pub fn consensus_header(&self) -> &impl TnSender<ConsensusHeader> {
        &self.inner.consensus_header
    }

    /// Status of initial sync operation.
    pub fn node_mode(&self) -> &watch::Sender<NodeMode> {
        &self.inner.tx_sync_status
    }

    /// Hold onto the consensus_metrics (mostly for testing)
    pub fn consensus_metrics(&self) -> Arc<ConsensusMetrics> {
        self.inner.consensus_metrics.clone()
    }

    /// Hold onto the primary metrics (allow early creation)
    pub fn primary_metrics(&self) -> Arc<Metrics> {
        self.inner.primary_metrics.clone()
    }

    /// Hold onto the channel metrics (metrics for the sequence channel).
    pub fn channel_metrics(&self) -> Arc<ChannelMetrics> {
        self.inner.channel_metrics.clone()
    }

    /// Hold onto the executor metrics
    pub fn executor_metrics(&self) -> &ExecutorMetrics {
        &self.inner.executor_metrics
    }

    /// Set the restart flag to indicate node restart after shutdown.
    pub fn set_restart(&self) {
        self.inner.restart.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Clear the restart flag to indicate node NOT restart after shutdown.
    pub fn clear_restart(&self) {
        self.inner.restart.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// True if the node should restart after shutdown.
    pub fn restart(&self) -> bool {
        self.inner.restart.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn consensus_network_gossip(&self) -> &impl TnSender<GossipMessage> {
        &self.inner.consensus_network_gossip
    }

    /// Update consensus round watch channels.
    ///
    /// This sends both the gc round and the committed round to the respective watch channels after
    /// consensus updates.
    pub fn update_consensus_rounds(&self, update: ConsensusRound) -> eyre::Result<()> {
        let ConsensusRound { committed_round, gc_round } = update;
        self.gc_round_updates().send(gc_round)?;
        self.committed_round_updates().send(committed_round)?;
        Ok(())
    }
}
