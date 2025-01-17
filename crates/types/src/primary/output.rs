//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::ConsensusHeader;
use crate::{
    crypto, encode, Certificate, CertificateDigest, ReputationScores, Round, TimestampSec,
    WorkerBlock,
};
use fastcrypto::hash::{Digest, Hash, HashFunction};
use reth_primitives::{Address, BlockHash, B256};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::warn;

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;

#[derive(Clone, Debug)]
/// The output of Consensus, which includes all the blocks for each certificate in the sub dag
/// It is sent to the the ExecutionState handle_consensus_transaction
pub struct ConsensusOutput {
    pub sub_dag: Arc<CommittedSubDag>,
    /// Matches certificates in the `sub_dag` one-to-one.
    ///
    /// This field is not included in [Self] digest. To validate,
    /// hash these blocks and compare to [Self::block_digests].
    pub blocks: Vec<Vec<WorkerBlock>>,
    /// The beneficiary for block rewards.
    pub beneficiary: Address,
    /// The ordered set of [BlockHash].
    ///
    /// This value is included in [Self] digest.
    pub block_digests: VecDeque<BlockHash>,

    // These fields are used to construct the ConsensusHeader.
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: B256,

    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    pub number: u64,
}

impl ConsensusOutput {
    /// The leader for the round
    ///
    /// TODO: need the address for the authority
    pub fn leader(&self) -> &Certificate {
        &self.sub_dag.leader
    }
    /// The round for the [CommittedSubDag].
    pub fn leader_round(&self) -> Round {
        self.sub_dag.leader_round()
    }
    /// Timestamp for when the subdag was committed.
    pub fn committed_at(&self) -> TimestampSec {
        self.sub_dag.commit_timestamp()
    }
    /// The subdag index (`SequenceNumber`).
    pub fn nonce(&self) -> SequenceNumber {
        self.sub_dag.sub_dag_index
    }
    /// Execution address of the leader for the round.
    ///
    /// The address is used in the executed block as the
    /// beneficiary for block rewards.
    pub fn beneficiary(&self) -> Address {
        self.beneficiary
    }
    /// Pop the next block digest.
    ///
    /// This method is used when executing [Self].
    pub fn next_block_digest(&mut self) -> Option<BlockHash> {
        self.block_digests.pop_front()
    }

    pub fn flatten_worker_blocks(&self) -> Vec<WorkerBlock> {
        self.blocks.iter().flat_map(|blocks| blocks.iter().cloned()).collect()
    }

    /// Build a new ConsensusHeader frome this output.
    pub fn consensus_header(&self) -> ConsensusHeader {
        ConsensusHeader {
            parent_hash: self.parent_hash,
            sub_dag: (*self.sub_dag).clone(),
            number: self.number,
        }
    }

    /// Return the hash of the consensus header that matches this output.
    pub fn consensus_header_hash(&self) -> B256 {
        ConsensusHeader::digest_from_parts(self.parent_hash, &self.sub_dag, self.number)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for ConsensusOutput {
    type TypedDigest = ConsensusOutputDigest;

    fn digest(&self) -> ConsensusOutputDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // hash subdag
        hasher.update(self.sub_dag.digest());
        // hash beneficiary
        hasher.update(self.beneficiary);
        // hash block digests in order
        self.block_digests.iter().for_each(|digest| {
            hasher.update(digest);
        });
        // finalize
        ConsensusOutputDigest(hasher.finalize().into())
    }
}

impl Display for ConsensusOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsensusOutput(round={:?}, sub_dag_index={:?}, timestamp={:?}, digest={:?})",
            self.sub_dag.leader_round(),
            self.sub_dag.sub_dag_index,
            self.sub_dag.commit_timestamp(),
            self.digest()
        )
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CommittedSubDag {
    /// The sequence of committed certificates.
    pub certificates: Vec<Certificate>,
    /// The leader certificate responsible of committing this sub-dag.
    pub leader: Certificate,
    /// The index associated with this CommittedSubDag
    pub sub_dag_index: SequenceNumber,
    /// The so far calculated reputation score for nodes
    pub reputation_score: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented. This is not necessarily the leader's timestamp. We compare the leader's
    /// timestamp with the previously committed sud dag timestamp and we always keep the max.
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
        previous_sub_dag: Option<&CommittedSubDag>,
    ) -> Self {
        // Narwhal enforces some invariants on the header.created_at, so we can use it as a
        // timestamp.
        let previous_sub_dag_ts = previous_sub_dag.map(|s| s.commit_timestamp).unwrap_or_default();
        let commit_timestamp = previous_sub_dag_ts.max(*leader.header().created_at());

        if previous_sub_dag_ts > *leader.header().created_at() {
            warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to max {}.",
            leader.header().created_at(), previous_sub_dag_ts, commit_timestamp);
        }

        Self { certificates, leader, sub_dag_index, reputation_score, commit_timestamp }
    }

    pub fn from_commit(
        commit: ConsensusCommit,
        certificates: Vec<Certificate>,
        leader: Certificate,
    ) -> Self {
        Self {
            certificates,
            leader,
            sub_dag_index: commit.sub_dag_index(),
            reputation_score: commit.reputation_score(),
            commit_timestamp: commit.commit_timestamp(),
        }
    }

    pub fn len(&self) -> usize {
        self.certificates.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn num_blocks(&self) -> usize {
        self.certificates.iter().map(|x| x.header().payload().len()).sum()
    }

    pub fn is_last(&self, output: &Certificate) -> bool {
        self.certificates.iter().last().map_or_else(|| false, |x| x == output)
    }

    /// The Certificate's round.
    pub fn leader_round(&self) -> Round {
        self.leader.round()
    }

    pub fn commit_timestamp(&self) -> TimestampSec {
        // If commit_timestamp is zero, then safely assume that this is an upgraded node that is
        // replaying this commit and field is never initialised. It's safe to fallback on leader's
        // timestamp.
        if self.commit_timestamp == 0 {
            return *self.leader.header().created_at();
        }
        self.commit_timestamp
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for CommittedSubDag {
    type TypedDigest = ConsensusOutputDigest;

    fn digest(&self) -> ConsensusOutputDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // Instead of hashing serialized CommittedSubDag, hash the certificate digests instead.
        // Signatures in the certificates are not part of the commitment.
        for cert in &self.certificates {
            hasher.update(cert.digest());
        }
        hasher.update(self.leader.digest());
        hasher.update(encode(&self.sub_dag_index));
        // skip reputation for stable hashes
        hasher.update(encode(&self.commit_timestamp));
        ConsensusOutputDigest(hasher.finalize().into())
    }
}

// Convenience function for casting `ConsensusOutputDigest` into EL B256.
// note: these are both 32-bytes
impl From<ConsensusOutputDigest> for B256 {
    fn from(value: ConsensusOutputDigest) -> Self {
        B256::from_slice(value.as_ref())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusCommit {
    /// The sequence of committed certificates' digests.
    pub certificates: Vec<CertificateDigest>,
    /// The leader certificate's digest responsible of committing this sub-dag.
    pub leader: CertificateDigest,
    /// The round of the leader
    pub leader_round: Round,
    /// Sequence number of the CommittedSubDag
    pub sub_dag_index: SequenceNumber,
    /// The so far calculated reputation score for nodes
    pub reputation_score: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented
    pub commit_timestamp: TimestampSec,
}

impl ConsensusCommit {
    pub fn from_sub_dag(sub_dag: &CommittedSubDag) -> Self {
        Self {
            certificates: sub_dag.certificates.iter().map(|x| x.digest()).collect(),
            leader: sub_dag.leader.digest(),
            leader_round: sub_dag.leader.round(),
            sub_dag_index: sub_dag.sub_dag_index,
            reputation_score: sub_dag.reputation_score.clone(),
            commit_timestamp: sub_dag.commit_timestamp,
        }
    }

    pub fn certificates(&self) -> Vec<CertificateDigest> {
        self.certificates.clone()
    }

    pub fn leader(&self) -> CertificateDigest {
        self.leader
    }

    pub fn leader_round(&self) -> Round {
        self.leader_round
    }

    pub fn sub_dag_index(&self) -> SequenceNumber {
        self.sub_dag_index
    }

    pub fn reputation_score(&self) -> ReputationScores {
        self.reputation_score.clone()
    }

    pub fn commit_timestamp(&self) -> TimestampSec {
        self.commit_timestamp
    }
}

/// Shutdown token dropped when a task is properly shut down.
pub type ShutdownToken = mpsc::Sender<()>;

// Digest of ConsususOutput and CommittedSubDag
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsensusOutputDigest([u8; crypto::DIGEST_LENGTH]);

impl AsRef<[u8]> for ConsensusOutputDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<ConsensusOutputDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(d: ConsensusOutputDigest) -> Self {
        Digest::new(d.0)
    }
}

impl fmt::Debug for ConsensusOutputDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.0))
    }
}

impl fmt::Display for ConsensusOutputDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.0)
                .get(0..16)
                .ok_or(fmt::Error)?
        )
    }
}

// See test_utils output_tests.rs for this modules tests.
