//! Implement a container for channels used internally by consensus.
//! This allows easier examination of message flow and avoids excessives channel passing as
//! arguments.

use crate::{
    certificate_fetcher::CertificateFetcherCommand, consensus::ConsensusRound,
    proposer::OurDigestMessage, RecentBlocks,
};
use consensus_metrics::metered_channel::{self, channel_with_total_sender, MeteredMpscChannel};
use std::sync::Arc;
use tn_primary_metrics::{ChannelMetrics, ConsensusMetrics, ExecutorMetrics, Metrics};
use tn_types::{
    Certificate, CommittedSubDag, ConsensusOutput, Header, Round, TnSender, CHANNEL_CAPACITY,
};
use tokio::sync::{broadcast, watch};

/// Has sync completed?
#[derive(Copy, Clone, Debug, Default)]
pub enum NodeMode {
    /// This is a full CVV that can participate in consensus.
    #[default]
    Cvv,
    /// This node can only follow consensus via consensus output.
    Nvv,
}

impl NodeMode {
    pub fn is_cvv(&self) -> bool {
        matches!(self, NodeMode::Cvv)
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
    tx_consensus_round_updates: watch::Sender<ConsensusRound>,
    /// Hold onto a receiver to keep it "open".
    _rx_consensus_round_updates: watch::Receiver<ConsensusRound>,

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

    /// Signals a new narwhal round
    tx_narwhal_round_updates: watch::Sender<Round>,
    /// Hold onto the primary metrics (allow early creation)
    _rx_narwhal_round_updates: watch::Receiver<Round>,

    /// Watch tracking most recent blocks
    tx_recent_blocks: watch::Sender<RecentBlocks>,
    /// Hold onto the recent blocks watch to keep it "open"
    _rx_recent_blocks: watch::Receiver<RecentBlocks>,

    /// Consensus output with a consensus header.
    consensus_output: broadcast::Sender<ConsensusOutput>,
    /// Hold onto consensus output with a consensus header to keep it open.
    _rx_consensus_output: broadcast::Receiver<ConsensusOutput>,

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
    pub fn new() -> Self {
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

        let (tx_consensus_round_updates, _rx_consensus_round_updates) =
            watch::channel(ConsensusRound::new(0, 0));

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

        let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);
        let (tx_recent_blocks, _rx_recent_blocks) = watch::channel(RecentBlocks::new(3));
        let (tx_sync_status, _rx_sync_status) = watch::channel(NodeMode::default());

        let sequence =
            metered_channel::channel_sender(CHANNEL_CAPACITY, &channel_metrics.tx_sequence);

        let (consensus_output, _rx_consensus_output) = broadcast::channel(CHANNEL_CAPACITY);

        Self {
            inner: Arc::new(ConsensusBusInner {
                new_certificates,
                committed_certificates,
                tx_consensus_round_updates,
                _rx_consensus_round_updates,
                certificate_fetcher,
                parents,
                our_digests,
                headers,
                committed_own_headers,
                sequence,

                tx_narwhal_round_updates,
                _rx_narwhal_round_updates,
                tx_recent_blocks,
                _rx_recent_blocks,
                consensus_output,
                _rx_consensus_output,
                tx_sync_status,
                _rx_sync_status,
                consensus_metrics,
                primary_metrics,
                channel_metrics,
                executor_metrics,
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
    pub fn consensus_round_updates(&self) -> &watch::Sender<ConsensusRound> {
        &self.inner.tx_consensus_round_updates
    }

    /// Signals a new narwhal round
    pub fn narwhal_round_updates(&self) -> &watch::Sender<u64> {
        &self.inner.tx_narwhal_round_updates
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

    /// Track recent blocks.
    pub fn recent_blocks(&self) -> &watch::Sender<RecentBlocks> {
        &self.inner.tx_recent_blocks
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
}
