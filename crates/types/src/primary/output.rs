//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::ConsensusHeader;
use crate::{
    crypto, encode, Address, Batch, BlockHash, BlsSignature, Certificate, Digest, Epoch, Hash,
    ReputationScores, Round, SealedHeader, TimestampSec, B256,
};
use alloy::primitives::keccak256;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::{error, warn};

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;

/// Notification sent by execution to consensus after processing one consensus output.
///
/// Tuple contents are:
/// - leader round from consensus
/// - consensus header hash
/// - latest canonical tip when execution produced a block (`None` when execution was skipped)
pub type EngineUpdate = (Round, B256, Option<SealedHeader>);

#[derive(Debug, Clone)]
/// Struct that contains all necessary information for executing a batch post-consensus.
pub struct CertifiedBatch {
    /// The ECDSA address of the authority that produced the batch. This address is used as the
    /// block beneficiary during execution. This may not be unique within a single
    /// [ConsensusOutput].
    pub address: Address,
    /// The collection of batches (in order) that reached consensus.
    pub batches: Vec<Batch>,
}

#[derive(Debug, Default)]
struct ConsensusOutputInner {
    /// The committed subdag that triggered this output.
    sub_dag: Arc<CommittedSubDag>,
    /// Matches certificates in the `sub_dag` one-to-one.
    ///
    /// This field is not included in [Self] digest. To validate,
    /// hash these batches and compare to [Self::batch_digests].
    batches: Vec<CertifiedBatch>,
    /// The ordered set of [BlockHash].
    ///
    /// This value is included in [Self] digest.
    batch_digests: VecDeque<BlockHash>,
    // These fields are used to construct the ConsensusHeader.
    /// The hash of the previous ConsesusHeader in the chain.
    parent_hash: B256,
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    number: u64,
    /// Temporary extra data field - currently unused.
    /// This is included for now for testnet purposes only.
    extra: B256,
}

/// The output of Consensus, which includes all the blocks for each certificate in the sub dag
/// It is sent to the the ExecutionState handle_consensus_transaction
#[derive(Clone, Debug, Default)]
pub struct ConsensusOutput {
    inner: Arc<ConsensusOutputInner>,
    /// Boolean indicating if this is the last output for the epoch.
    ///
    /// The engine should make a system call to consensus registry contract to close the epoch.
    pub close_epoch: bool,
}

impl ConsensusOutput {
    /// Create a
    pub fn new(
        sub_dag: Arc<CommittedSubDag>,
        parent_hash: BlockHash,
        number: u64,
        close_epoch: bool,
        batch_digests: VecDeque<BlockHash>,
        batches: Vec<CertifiedBatch>,
    ) -> Self {
        ConsensusOutput {
            inner: Arc::new(ConsensusOutputInner {
                sub_dag: sub_dag.clone(),
                parent_hash,
                number,
                batch_digests,
                batches,
                ..Default::default()
            }),
            close_epoch,
        }
    }
    pub fn new_with_subdag(
        sub_dag: Arc<CommittedSubDag>,
        parent_hash: BlockHash,
        number: u64,
    ) -> Self {
        Self::new(sub_dag, parent_hash, number, false, VecDeque::new(), Vec::new())
    }
    pub fn new_closed_with_subdag(
        sub_dag: Arc<CommittedSubDag>,
        parent_hash: BlockHash,
        number: u64,
    ) -> Self {
        Self::new(sub_dag, parent_hash, number, true, VecDeque::new(), Vec::new())
    }

    /// Reference the contained batches.
    pub fn batches(&self) -> &[CertifiedBatch] {
        &self.inner.batches
    }

    /// Return the a referance of contained Batch digests.
    pub fn batch_digests(&self) -> &VecDeque<BlockHash> {
        &self.inner.batch_digests
    }

    /// Return the consensus block number.
    pub fn number(&self) -> u64 {
        self.inner.number
    }

    /// Return the contained sub dag.
    pub fn sub_dag(&self) -> &CommittedSubDag {
        &self.inner.sub_dag
    }

    /// The leader for the round
    pub fn leader(&self) -> &Certificate {
        &self.inner.sub_dag.leader
    }

    /// The round for the [CommittedSubDag].
    pub fn leader_round(&self) -> Round {
        self.inner.sub_dag.leader_round()
    }

    /// Timestamp for when the subdag was committed.
    pub fn committed_at(&self) -> TimestampSec {
        self.inner.sub_dag.commit_timestamp()
    }

    /// The leader's `nonce`.
    pub fn nonce(&self) -> SequenceNumber {
        self.inner.sub_dag.leader.nonce()
    }

    /// Return the batch digest for index idx or None if not available.
    ///
    /// This method is used when executing [Self].
    pub fn get_batch_digest(&self, idx: usize) -> Option<BlockHash> {
        self.inner.batch_digests.get(idx).copied()
    }

    /// Create flat index mapping to retrieve certified batches during execution.
    /// The first `usize` is the index for the [CertifiedBatch] which is used
    /// to identify the authority that produced the batch. The second `usize`
    /// is the batch's index within the committed certificate.
    pub fn flatten_batches(&self) -> Vec<(usize, usize)> {
        self.inner
            .batches
            .iter()
            .enumerate()
            .flat_map(|(cert_idx, cert_batch)| {
                (0..cert_batch.batches.len()).map(move |batch_idx| (cert_idx, batch_idx))
            })
            .collect()
    }

    /// Build a new ConsensusHeader from this output.
    pub fn consensus_header(&self) -> ConsensusHeader {
        ConsensusHeader {
            parent_hash: self.inner.parent_hash,
            sub_dag: self.inner.sub_dag.clone(),
            number: self.inner.number,
            extra: self.inner.extra,
        }
    }

    /// Build a new ConsensusHeader from this output.
    pub fn into_consensus_header(self) -> ConsensusHeader {
        ConsensusHeader {
            parent_hash: self.inner.parent_hash,
            sub_dag: self.inner.sub_dag.clone(),
            number: self.inner.number,
            extra: self.inner.extra,
        }
    }

    /// Return the hash of the consensus header that matches this output.
    pub fn consensus_header_hash(&self) -> B256 {
        ConsensusHeader::digest_from_parts(
            self.inner.parent_hash,
            &self.inner.sub_dag,
            self.inner.number,
        )
    }

    /// Return a `bool` if this is the last batch (by index) of the last output for the epoch.
    ///
    /// This is used by the engine to apply system calls at the end of the epoch.
    /// Use index to deterine if on last Batch to apply system call on last processed batch.
    /// This logic also works for empty outputs with no batches.
    pub fn close_epoch_for_last_batch(&self, index: usize) -> Option<bool> {
        self.close_epoch.then_some(
            self.inner.batch_digests.is_empty() || (index + 1) >= self.inner.batch_digests.len(),
        )
    }

    /// Generate the source of randomness to shuffle future committees at the epoch boundary. The
    /// source of randomness comes from the keccak hash of the leader's aggregate signature.
    ///
    /// NOTE: this cannot fail - uses [BlsSignature::default] and is considered acceptable with
    /// permissioned validator set, but should never happen.
    pub fn keccak_leader_sigs(&self) -> B256 {
        let randomness = self.leader().aggregated_signature().unwrap_or_else(|| {
            error!(target: "engine", ?self, "BLS signature missing for leader - using default for closing epoch");
            BlsSignature::default()
        });
        keccak256(randomness.to_bytes())
    }

    /// The parent hash for this output.
    pub fn parent_hash(&self) -> BlockHash {
        self.inner.parent_hash
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for ConsensusOutput {
    type TypedDigest = ConsensusDigest;

    /// The digest of the corresponding [ConsensusHeader] that produced this output.
    fn digest(&self) -> ConsensusDigest {
        ConsensusDigest(Digest { digest: self.consensus_header_hash().into() })
    }
}

impl Display for ConsensusOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsensusOutput(epoch={:?}, round={:?}, timestamp={:?}, digest={:?})",
            self.inner.sub_dag.leader.epoch(),
            self.inner.sub_dag.leader.round(),
            self.inner.sub_dag.commit_timestamp(),
            self.digest()
        )
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
pub struct CommittedSubDag {
    /// The sequence of committed certificates.
    pub certificates: Vec<Certificate>,
    /// The leader certificate responsible for committing this sub-dag.
    pub leader: Certificate,
    /// The so far calculated reputation score for nodes
    pub reputation_score: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented. This is not necessarily the leader's timestamp. We compare the leader's
    /// timestamp with the previously committed sub dag timestamp and we always keep the max.
    /// Property is explicitly private so the method commit_timestamp() should be used instead
    /// which bears additional resolution logic.
    commit_timestamp: TimestampSec,
}

impl CommittedSubDag {
    pub fn new(
        certificates: Vec<Certificate>,
        leader: Certificate,
        sub_dag_index: SequenceNumber,
        reputation_score: ReputationScores,
        previous_sub_dag: Option<Arc<CommittedSubDag>>,
    ) -> Self {
        // Narwhal enforces some invariants on the header.created_at, so we can use it as a
        // timestamp.
        let previous_sub_dag_ts = previous_sub_dag.map(|s| s.commit_timestamp).unwrap_or_default();
        let commit_timestamp = previous_sub_dag_ts.max(*leader.header().created_at());

        if previous_sub_dag_ts > *leader.header().created_at() {
            warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to max {}.",
            leader.header().created_at(), previous_sub_dag_ts, commit_timestamp);
        }

        Self { certificates, leader, reputation_score, commit_timestamp }
    }

    pub fn len(&self) -> usize {
        self.certificates.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn num_primary_blocks(&self) -> usize {
        self.certificates.iter().map(|x| x.header().payload().len()).sum()
    }

    pub fn is_last(&self, output: &Certificate) -> bool {
        self.certificates.iter().last().map_or_else(|| false, |x| x == output)
    }

    /// The Certificate's round.
    pub fn leader_round(&self) -> Round {
        self.leader.round()
    }

    /// The Certificate's epoch.
    pub fn leader_epoch(&self) -> Epoch {
        self.leader.epoch()
    }

    /// Return the leaders commit timestamp.
    pub fn commit_timestamp(&self) -> TimestampSec {
        // If commit_timestamp is zero, then safely assume that this is an upgraded node that is
        // replaying this commit and field is never initialised. It's safe to fallback on leader's
        // timestamp.
        if self.commit_timestamp == 0 {
            return *self.leader.header().created_at();
        }
        self.commit_timestamp
    }

    /// Return the Certificates for this SubDag.
    pub fn certificates(&self) -> &[Certificate] {
        &self.certificates
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for CommittedSubDag {
    type TypedDigest = ConsensusDigest;

    fn digest(&self) -> ConsensusDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // Instead of hashing serialized CommittedSubDag, hash the certificate digests instead.
        // Signatures in the certificates are not part of the commitment.
        for cert in &self.certificates {
            hasher.update(cert.digest().as_ref());
        }
        hasher.update(self.leader.digest().as_ref());
        // skip reputation for stable hashes
        hasher.update(encode(&self.commit_timestamp).as_ref());
        ConsensusDigest(Digest { digest: hasher.finalize().into() })
    }
}

// Convenience function for casting `ConsensusDigest` into EL B256.
// note: these are both 32-bytes
impl From<ConsensusDigest> for B256 {
    fn from(value: ConsensusDigest) -> Self {
        B256::from_slice(value.as_ref())
    }
}

/// Shutdown token dropped when a task is properly shut down.
pub type ShutdownToken = mpsc::Sender<()>;

// Digest of ConsususOutput and CommittedSubDag
#[derive(
    Clone, Copy, Default, PartialEq, Eq, std::hash::Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct ConsensusDigest(Digest<{ crypto::DIGEST_LENGTH }>);

impl AsRef<[u8]> for ConsensusDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0.digest
    }
}

impl From<ConsensusDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(d: ConsensusDigest) -> Self {
        d.0
    }
}

impl fmt::Debug for ConsensusDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for ConsensusDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
    }
}

// See test_utils output_tests.rs for this modules tests.
