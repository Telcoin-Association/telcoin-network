//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::ConsensusHeader;
use crate::{
    crypto, encode, Address, Batch, BlockHash, Certificate, ReputationScores, Round, TimestampSec,
    B256,
};
use fastcrypto::hash::{Digest, Hash, HashFunction};
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

/// The output of Consensus, which includes all the blocks for each certificate in the sub dag
/// It is sent to the the ExecutionState handle_consensus_transaction
#[derive(Clone, Debug)]
pub struct ConsensusOutput {
    pub sub_dag: Arc<CommittedSubDag>,
    /// Matches certificates in the `sub_dag` one-to-one.
    ///
    /// This field is not included in [Self] digest. To validate,
    /// hash these batches and compare to [Self::batch_digests].
    pub batches: Vec<Vec<Batch>>,
    /// The beneficiary for block rewards.
    pub beneficiary: Address,
    /// The ordered set of [BlockHash].
    ///
    /// This value is included in [Self] digest.
    pub batch_digests: VecDeque<BlockHash>,
    // These fields are used to construct the ConsensusHeader.
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: B256,
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    pub number: u64,
    /// Temporary extra data field - currently unused.
    /// This is included for now for testnet purposes only.
    pub extra: B256,
}

impl ConsensusOutput {
    /// The leader for the round
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

    /// The leader's `nonce`.
    pub fn nonce(&self) -> SequenceNumber {
        self.sub_dag.leader.nonce()
    }

    /// Execution address of the leader for the round.
    ///
    /// The address is used in the executed block as the
    /// beneficiary for block rewards.
    pub fn beneficiary(&self) -> Address {
        self.beneficiary
    }

    /// Pop the next batch digest.
    ///
    /// This method is used when executing [Self].
    pub fn next_batch_digest(&mut self) -> Option<BlockHash> {
        self.batch_digests.pop_front()
    }

    /// Flatten sequenced batches.
    pub fn flatten_batches(&self) -> Vec<Batch> {
        self.batches.iter().flat_map(|batches| batches.iter().cloned()).collect()
    }

    /// Build a new ConsensusHeader frome this output.
    pub fn consensus_header(&self) -> ConsensusHeader {
        ConsensusHeader {
            parent_hash: self.parent_hash,
            sub_dag: (*self.sub_dag).clone(),
            number: self.number,
            extra: self.extra,
        }
    }

    /// Return the hash of the consensus header that matches this output.
    pub fn consensus_header_hash(&self) -> B256 {
        ConsensusHeader::digest_from_parts(self.parent_hash, &self.sub_dag, self.number)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for ConsensusOutput {
    type TypedDigest = ConsensusDigest;

    /// The digest of the corresponding [ConsensusHeader] that produced this output.
    fn digest(&self) -> ConsensusDigest {
        ConsensusDigest(self.consensus_header_hash().into())
    }
}

impl Display for ConsensusOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsensusOutput(epoch={:?}, round={:?}, timestamp={:?}, digest={:?})",
            self.sub_dag.leader.epoch(),
            self.sub_dag.leader.round(),
            self.sub_dag.commit_timestamp(),
            self.digest()
        )
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct CommittedSubDag {
    /// The sequence of committed certificates.
    pub certificates: Vec<Certificate>,
    /// The leader certificate responsible of committing this sub-dag.
    pub leader: Certificate,
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
    type TypedDigest = ConsensusDigest;

    fn digest(&self) -> ConsensusDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // Instead of hashing serialized CommittedSubDag, hash the certificate digests instead.
        // Signatures in the certificates are not part of the commitment.
        for cert in &self.certificates {
            hasher.update(cert.digest());
        }
        hasher.update(self.leader.digest());
        // skip reputation for stable hashes
        hasher.update(encode(&self.commit_timestamp));
        ConsensusDigest(hasher.finalize().into())
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
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConsensusDigest([u8; crypto::DIGEST_LENGTH]);

impl AsRef<[u8]> for ConsensusDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<ConsensusDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(d: ConsensusDigest) -> Self {
        Digest::new(d.0)
    }
}

impl fmt::Debug for ConsensusDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.0))
    }
}

impl fmt::Display for ConsensusDigest {
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
