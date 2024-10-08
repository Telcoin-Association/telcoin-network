//! The ouput from consensus (bullshark)

use crate::{
    crypto, encode, Certificate, CertificateDigest, ReputationScores, Round, SequenceNumber,
    TimestampSec, WorkerBlock, WorkerBlockConversionError,
};
use fastcrypto::hash::{Digest, Hash, HashFunction};
use reth_primitives::{Address, BlockHash, Header, SealedBlockWithSenders, B256};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    fmt::{self, Display, Formatter},
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::warn;

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
    /// Ommers to use for the executed blocks.
    ///
    /// TODO: parallelize this when output contains enough blocks.
    pub fn ommers(&self) -> Vec<Header> {
        self.blocks
            .iter()
            .flat_map(|blocks| blocks.iter().map(|block| block.sealed_header().header().clone()))
            .collect()
    }
    /// Recover the sealed blocks with senders for all blocks in output.
    ///
    /// TODO: parallelize this when output contains enough blocks.
    pub fn sealed_blocks_from_blocks(
        &self,
    ) -> Result<Vec<SealedBlockWithSenders>, WorkerBlockConversionError> {
        self.blocks
            .iter()
            .flat_map(|blocks| {
                blocks.iter().map(|block| {
                    // create sealed block from block for execution this should never fail since
                    // blocks are validated
                    SealedBlockWithSenders::try_from(block)
                })
            })
            .collect()
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
        hasher.update(encode(&self.reputation_score));
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

#[cfg(test)]
mod tests {
    use crate::{
        test_utils::CommitteeFixture, AuthorityIdentifier, Certificate, CommittedSubDag,
        HeaderBuilder, ReputationScores,
    };
    use indexmap::IndexMap;
    use std::{collections::BTreeSet, num::NonZeroUsize};

    #[test]
    fn test_zero_timestamp_in_sub_dag() {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();

        let header_builder = HeaderBuilder::default();
        let header = header_builder
            .author(AuthorityIdentifier(1u16))
            .round(2)
            .epoch(0)
            .created_at(50)
            .payload(IndexMap::new())
            .parents(BTreeSet::new())
            .build()
            .unwrap();

        let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

        // AND we initialise the sub dag via the "restore" way
        let sub_dag_round = CommittedSubDag {
            certificates: vec![certificate.clone()],
            leader: certificate,
            sub_dag_index: 1,
            reputation_score: ReputationScores::default(),
            commit_timestamp: 0,
        };

        // AND commit timestamp is the leader's timestamp
        assert_eq!(sub_dag_round.commit_timestamp(), 50);
    }

    #[test]
    fn test_monotonically_incremented_commit_timestamps() {
        // Create a certificate (leader) of round 2 with a high timestamp
        let newer_timestamp = 100;
        let older_timestamp = 50;

        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();

        let header_builder = HeaderBuilder::default();
        let header = header_builder
            .author(AuthorityIdentifier(1u16))
            .round(2)
            .epoch(0)
            .created_at(newer_timestamp)
            .payload(IndexMap::new())
            .parents(BTreeSet::new())
            .build()
            .unwrap();

        let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

        // AND
        let sub_dag_round_2 = CommittedSubDag::new(
            vec![certificate.clone()],
            certificate,
            1,
            ReputationScores::default(),
            None,
        );

        // AND commit timestamp is the leader's timestamp
        assert_eq!(sub_dag_round_2.commit_timestamp, newer_timestamp);

        // Now create the leader of round 4 with the older timestamp
        let header_builder = HeaderBuilder::default();
        let header = header_builder
            .author(AuthorityIdentifier(1u16))
            .round(4)
            .epoch(0)
            .created_at(older_timestamp)
            .payload(IndexMap::new())
            .parents(BTreeSet::new())
            .build()
            .unwrap();

        let certificate = Certificate::new_unsigned(&committee, header, Vec::new()).unwrap();

        // WHEN create the sub dag based on the "previously committed" sub dag.
        let sub_dag_round_4 = CommittedSubDag::new(
            vec![certificate.clone()],
            certificate,
            2,
            ReputationScores::default(),
            Some(&sub_dag_round_2),
        );

        // THEN the latest sub dag should have the highest committed timestamp - basically the
        // same as the previous commit round
        assert_eq!(sub_dag_round_4.commit_timestamp, sub_dag_round_2.commit_timestamp);
    }

    #[test]
    fn test_authority_sorting_in_reputation_scores() {
        let fixture =
            CommitteeFixture::builder().committee_size(NonZeroUsize::new(10).unwrap()).build();
        let committee = fixture.committee();

        let mut scores = ReputationScores::new(&committee);

        let ids: Vec<AuthorityIdentifier> = fixture.authorities().map(|a| a.id()).collect();

        // adding some scores
        scores.add_score(ids[0], 0);
        scores.add_score(ids[1], 10);
        scores.add_score(ids[2], 10);
        scores.add_score(ids[3], 10);
        scores.add_score(ids[4], 10);
        scores.add_score(ids[5], 20);
        scores.add_score(ids[6], 30);
        scores.add_score(ids[7], 30);
        scores.add_score(ids[8], 40);
        scores.add_score(ids[9], 40);

        // the expected authorities
        let expected_authorities = vec![
            (ids[9], 40),
            (ids[8], 40),
            (ids[7], 30),
            (ids[6], 30),
            (ids[5], 20),
            (ids[4], 10),
            (ids[3], 10),
            (ids[2], 10),
            (ids[1], 10),
            (ids[0], 0),
        ];

        // sorting the authorities
        let sorted_authorities = scores.authorities_by_score_desc();
        assert_eq!(sorted_authorities, expected_authorities);
    }
}
