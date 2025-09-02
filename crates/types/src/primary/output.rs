//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::{CertificateDigest, ConsensusHeader, SignatureVerificationState};
use crate::{
    crypto, encode,
    error::{CertificateError, CertificateResult},
    Address, Batch, BlockHash, Certificate, Committee, Digest, Epoch, Hash, ReputationScores,
    Round, TimestampSec, B256,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashSet, VecDeque},
    fmt::{self, Display, Formatter},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::warn;

/// A global sequence number assigned to every CommittedSubDag.
pub type SequenceNumber = u64;

/// The output of Consensus, which includes all the blocks for each certificate in the sub dag
/// It is sent to the the ExecutionState handle_consensus_transaction
#[derive(Clone, Debug, Default)]
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
    /// If true then finalize blocks as soon as they are executed.
    /// This is safe to do for a CVV (participating committe members) but otherwise should
    /// be false unless running a node with the potential to advertise a forked block or
    /// two before quitting.
    pub early_finalize: bool,
    /// Boolean indicating if this is the last output for the epoch.
    ///
    /// The engine should make a system call to consensus registry contract to close the epoch.
    pub close_epoch: bool,
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

    /// Build a new ConsensusHeader from this output.
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

    /// Return a `bool` if this is the last batch of the last output for the epoch.
    ///
    /// This is used by the engine to apply system calls at the end of the epoch.
    /// Batches are `popped` in `Self::next_batch_digest`, so check if batches
    /// are empty to apply system call on last processed batch. This logic also
    /// works for empty outputs with no batches.
    pub fn close_epoch_for_last_batch(&self) -> Option<bool> {
        self.close_epoch.then_some(self.batch_digests.is_empty())
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
            self.sub_dag.leader.epoch(),
            self.sub_dag.leader.round(),
            self.sub_dag.commit_timestamp(),
            self.digest()
        )
    }
}

#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
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

    /// The Certificate's epoch.
    pub fn leader_epoch(&self) -> Epoch {
        self.leader.epoch()
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

    /// Verify that all of the contained certificates are valid and signed by a quorum of committee.
    pub fn verify_certificates(self, committee: &Committee) -> CertificateResult<Self> {
        let Self { mut certificates, leader, reputation_score, commit_timestamp } = self;
        let leader = leader.verify_cert(committee)?;
        let mut verified_certs: HashSet<CertificateDigest> =
            leader.header.parents().iter().copied().collect();
        let mut new_certs = Vec::new();
        // Verify all the certs in the sub dag- we may directly apply these, save them or submit to
        // bullshark so check them all. The leader is directly verified against the committe
        // then any cert that is referenced from the leader is considered in-directly
        // verified.  We just go ahead and directly verify any cert not in the leader
        // sub dag to keep things simple (any of these certs will be remembered for indirect
        // verification as well).
        for mut cert in certificates.drain(..) {
            let digest = cert.digest();
            if verified_certs.contains(&digest) {
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedIndirectly(
                        cert.aggregated_signature()
                            .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
                    ),
                );
                new_certs.push(cert);
            } else {
                let cert = cert.verify_cert(committee)?;
                verified_certs.insert(cert.digest());
                new_certs.push(cert);
            }
        }
        Ok(Self { certificates: new_certs, leader, reputation_score, commit_timestamp })
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
