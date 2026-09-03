//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::ConsensusHeader;
use crate::{
    crypto, encode, Address, Batch, BlockHash, BlsSignature, Certificate, ConsensusHeaderDigest,
    ConsensusNumHash, Digest, Epoch, EpochSeedChainValue, Hash, Header, ReputationScores, Round,
    SealedHeader, TimestampSec, B256,
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
/// - consensus block number/hash
/// - latest canonical tip when execution produced a block (`None` when execution was skipped)
pub type EngineUpdate = (Round, ConsensusNumHash, Option<SealedHeader>);

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Struct that contains all necessary information for executing a batch post-consensus.
pub struct CertifiedBatch {
    /// The execution address of the sub-DAG header author that referenced these batches, resolved
    /// through the committee.
    ///
    /// Not the block beneficiary: each batch's priority fees are credited to the producer's own
    /// [`Batch::beneficiary`], which is covered by the batch digest, so a byzantine header cannot
    /// steal fees by copying another validator's batch digest (#1222). This address may not be
    /// unique within a single [ConsensusOutput].
    pub address: Address,
    /// The collection of batches (in order) that reached consensus.
    pub batches: Vec<Batch>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ConsensusOutputInner {
    /// The committed subdag that triggered this output.
    sub_dag: CommittedSubDag,
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
    parent_hash: ConsensusHeaderDigest,
    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    number: u64,
    /// Temporary extra data field - currently unused.
    /// This is included for now for testnet purposes only.
    extra: B256,
}

/// The output of Consensus, which includes all the blocks for each certificate in the sub dag
/// It is sent to the the ExecutionState handle_consensus_transaction
#[derive(Clone, Debug)]
pub struct ConsensusOutput {
    inner: Arc<ConsensusOutputInner>,
    /// Boolean indicating if this is the last output for the epoch.
    ///
    /// The engine should make a system call to consensus registry contract to close the epoch.
    close_epoch: bool,
    /// Cached digest of the consensus header for this output.
    consensus_header_hash_cache: ConsensusHeaderDigest,
}

// NOTE: only [Self::inner] is serialized. `close_epoch` is intentionally NOT part of the
// serialized form: it is a transient, locally-derived flag (not part of the consensus header
// digest) and is always recomputed from `committed_at() >= epoch_boundary`. Any consumer that
// deserializes a [ConsensusOutput] MUST recompute it via `EpochManager::process_output` before
// trusting [ConsensusOutput::close_epoch] — a deserialized output always reports `false`.
impl Serialize for ConsensusOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for ConsensusOutput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = ConsensusOutputInner::deserialize(deserializer)?;
        let consensus_header_hash_cache =
            ConsensusHeader::digest_from_parts(inner.parent_hash, &inner.sub_dag, inner.number);
        Ok(Self { inner: Arc::new(inner), close_epoch: false, consensus_header_hash_cache })
    }
}

impl ConsensusOutput {
    /// Create a
    pub fn new(
        sub_dag: CommittedSubDag,
        parent_hash: ConsensusHeaderDigest,
        number: u64,
        close_epoch: bool,
        batch_digests: VecDeque<BlockHash>,
        batches: Vec<CertifiedBatch>,
    ) -> Self {
        let inner = Arc::new(ConsensusOutputInner {
            sub_dag: sub_dag.clone(),
            parent_hash,
            number,
            batch_digests,
            batches,
            ..Default::default()
        });
        let consensus_header_hash_cache =
            ConsensusHeader::digest_from_parts(inner.parent_hash, &inner.sub_dag, inner.number);
        ConsensusOutput { inner, close_epoch, consensus_header_hash_cache }
    }
    pub fn new_with_subdag(
        sub_dag: CommittedSubDag,
        parent_hash: ConsensusHeaderDigest,
        number: u64,
    ) -> Self {
        Self::new(sub_dag, parent_hash, number, false, VecDeque::new(), Vec::new())
    }
    pub fn new_closed_with_subdag(
        sub_dag: CommittedSubDag,
        parent_hash: ConsensusHeaderDigest,
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
    pub fn leader(&self) -> &Header {
        self.inner.sub_dag.leader()
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
        self.inner.sub_dag.leader().nonce()
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
    pub fn consensus_header_hash(&self) -> ConsensusHeaderDigest {
        self.consensus_header_hash_cache
    }

    /// Return number/hash tuple for this consensus output.
    pub fn num_hash(&self) -> ConsensusNumHash {
        ConsensusNumHash::new(self.inner.number, self.consensus_header_hash())
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

    /// Set the close epoch field, this is the last consensus output for an epoch.
    pub fn set_epoch_close(&mut self) {
        self.close_epoch = true;
    }

    /// Boolean indicating if this is the last output for the epoch.
    ///
    /// The engine should make a system call to consensus registry contract to close the epoch.
    pub fn close_epoch(&self) -> bool {
        self.close_epoch
    }

    /// The source of randomness used to shuffle future committees at the epoch boundary: the
    /// epoch seed chain value as of this commit (see
    /// [`EpochSeedChainValue`](crate::EpochSeedChainValue)).
    ///
    /// The seed signature folded in at each step is part of that step's leader header, so it is
    /// covered by the header digest, the votes, and the certificate aggregate - every certificate
    /// for a leader's header carries the same seed contribution, making this value unforkable by
    /// the leader. Because it also folds every earlier commit of the epoch, no authority can
    /// compute it before the immediately preceding commit is published.
    ///
    /// Epochs where [`crate::forks::seed_signature_active`] is false use the legacy seed
    /// instead: keccak256 of the leader certificate's aggregate BLS signature, wire-identical
    /// to pre-fork releases; active epochs use the epoch seed chain described above.
    pub fn committee_shuffle_seed(&self) -> B256 {
        self.inner.sub_dag.inner.randomness
    }

    /// The parent hash for this output.
    pub fn parent_hash(&self) -> ConsensusHeaderDigest {
        self.inner.parent_hash
    }

    /// The executed block's `mix_hash` (EVM `PREVRANDAO`) for the payload at `batch_index`.
    ///
    /// Post-fork ([`crate::forks::prevrandao_seed_active`] for the committing leader's
    /// epoch), the value is `keccak256` over [`PREVRANDAO_DOMAIN`], the epoch seed chain
    /// value as of this commit ([`Self::committee_shuffle_seed`]), the consensus block
    /// number, and the batch index (integers little-endian). Every input is fixed by the
    /// committed order: the seed chain folds only digest-pinned deterministic BLS seed
    /// signatures, so transaction bytes, transaction ordering, and batch selection cannot
    /// vary the result. This closes the grinding channel of #1247, where both halves of the
    /// legacy XOR commit to transaction bytes and let a committing leader enumerate
    /// candidate `PREVRANDAO` values by re-cutting the payload it proposes.
    ///
    /// What this does NOT close is last-actor bias, the same residual
    /// [`EpochSeedChainValue`](crate::EpochSeedChainValue) documents and accepts. The seed
    /// chain value of a commit is computable by that commit's leader before it broadcasts,
    /// and the block number and batch index are known to it too, so the committing leader
    /// knows every `PREVRANDAO` its commit will produce and can withhold the proposal if it
    /// dislikes them, forfeiting the commit. What changes is the cost: the leader gets one
    /// propose-or-withhold coin flip per commit instead of unbounded free re-draws from
    /// re-cutting the payload. Contracts requiring unbiasable randomness MUST NOT use
    /// `PREVRANDAO` alone; use a commit-reveal or an external beacon.
    ///
    /// Pre-fork, the legacy derivation is preserved byte-identically for replay: the
    /// consensus header digest XOR `batch_digest`. The empty epoch-closing block passes
    /// [`B256::ZERO`], which reduces the XOR to the bare consensus header digest, exactly
    /// the value the engine used for that path.
    pub fn prev_randao(&self, batch_index: usize, batch_digest: B256) -> B256 {
        if crate::forks::prevrandao_seed_active(self.leader().epoch()) {
            seeded_prev_randao(self.committee_shuffle_seed(), self.number(), batch_index)
        } else {
            let output_digest: B256 = self.digest().into();
            output_digest ^ batch_digest
        }
    }
}

/// The post-fork `PREVRANDAO` derivation over the raw committed inputs (#1247).
///
/// A free function so the exact byte layout is pinnable by unit tests under every feature
/// set; [`ConsensusOutput::prev_randao`] owns the fork dispatch.
fn seeded_prev_randao(seed: B256, number: u64, batch_index: usize) -> B256 {
    let number = number.to_le_bytes();
    let index = (batch_index as u64).to_le_bytes();
    keccak256([PREVRANDAO_DOMAIN, seed.as_slice(), number.as_slice(), index.as_slice()].concat())
}

/// Domain tag for the post-fork `PREVRANDAO` derivation ([`ConsensusOutput::prev_randao`]).
///
/// Versioned like the seed-chain domains (`TN_EPOCH_SEED_*_V1`) so any future change to the
/// derivation can separate its outputs from this one's.
const PREVRANDAO_DOMAIN: &[u8] = b"TN_PREVRANDAO_V1";

impl Hash<{ crypto::DIGEST_LENGTH }> for ConsensusOutput {
    type TypedDigest = ConsensusHeaderDigest;

    /// The digest of the corresponding [ConsensusHeader] that produced this output.
    fn digest(&self) -> ConsensusHeaderDigest {
        self.consensus_header_hash()
    }
}

impl Display for ConsensusOutput {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConsensusOutput(epoch={:?}, round={:?}, timestamp={:?}, digest={:?})",
            self.inner.sub_dag.leader().epoch(),
            self.inner.sub_dag.leader().round(),
            self.inner.sub_dag.commit_timestamp(),
            self.digest()
        )
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug)]
struct CommittedSubDagInner {
    /// The sequence of committed certificates.
    /// Note the last element MUST be the leader.
    headers: Vec<Header>,
    /// The so far calculated reputation score for nodes
    reputation_scores: ReputationScores,
    /// The timestamp that should identify this commit. This is guaranteed to be monotonically
    /// incremented. This is not necessarily the leader's timestamp. We compare the leader's
    /// timestamp with the previously committed sub dag timestamp and we always keep the max.
    /// Property is explicitly private so the method commit_timestamp() should be used instead
    /// which bears additional resolution logic.
    commit_timestamp: TimestampSec,
    /// The epoch seed chain value as of this commit: the previous commit's value folded with this
    /// leader's round and its deterministic BLS seed signature over the canonical per-`(author,
    /// round)` [`EpochSeedMessage`](crate::EpochSeedMessage). See
    /// [`EpochSeedChainValue`](crate::EpochSeedChainValue).
    ///
    /// For epochs where [`crate::forks::seed_signature_active`] is false this holds the legacy
    /// seed instead - keccak256 of the leader certificate's aggregate BLS signature - so
    /// pre-fork commits stay wire-identical to origin/main; active epochs hold the epoch seed
    /// chain value.
    randomness: B256,
}

/// Contains the committed output from Bullshark consensus.
/// Note it stores Headers without certificates, all validation
/// should be complete.  Future validation can be done by verifying
/// the consensus chain against signed checkpoints (like epoch records).
#[derive(Clone, PartialEq, Debug)]
pub struct CommittedSubDag {
    inner: Arc<CommittedSubDagInner>,
}

impl Serialize for CommittedSubDag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for CommittedSubDag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = CommittedSubDagInner::deserialize(deserializer)?;
        Ok(Self { inner: Arc::new(inner) })
    }
}

impl Default for CommittedSubDag {
    fn default() -> Self {
        // The pinned genesis placeholder, used raw rather than folded.
        //
        // This is THE definition of the pre-genesis chain anchor: [`ConsensusHeader::default`]
        // builds its sub-dag from this one (state sync uses that header as the pre-genesis anchor),
        // so the two cannot drift apart into expressions that merely happen to agree. Using the
        // placeholder directly also keeps the anchor a value no node derives from local state.
        let randomness = EpochSeedChainValue::genesis_placeholder().into_inner();
        // Override default so we have one default header (the leader)
        // so a default value won't panic when used.
        let inner = Arc::new(CommittedSubDagInner {
            headers: vec![Header::default()],
            reputation_scores: Default::default(),
            commit_timestamp: Default::default(),
            randomness,
        });
        Self { inner }
    }
}

impl CommittedSubDag {
    /// Create a new CommittedSubDag.
    /// Note that leader MUST be the first element or certificates or this will panic.
    ///
    /// `previous_sub_dag` resolves the monotonic `commit_timestamp` only. `seed_chain` is the epoch
    /// seed chain value this commit folds into and is deliberately a separate, non-optional
    /// argument: the two must never be conflated, because an absent previous sub-dag is a normal
    /// first-commit condition while an absent chain anchor is unrepresentable (see
    /// [`EpochSeedChainValue`]).
    pub fn new(
        certificates: Vec<Certificate>,
        leader: Certificate,
        sub_dag_index: SequenceNumber,
        reputation_scores: ReputationScores,
        previous_sub_dag: Option<CommittedSubDag>,
        seed_chain: EpochSeedChainValue,
    ) -> Self {
        // Narwhal enforces some invariants on the header.created_at, so we can use it as a
        // timestamp.
        let previous_sub_dag_ts =
            previous_sub_dag.map(|s| s.inner.commit_timestamp).unwrap_or_default();
        let commit_timestamp = previous_sub_dag_ts.max(*leader.header().created_at());

        if previous_sub_dag_ts > *leader.header().created_at() {
            warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to max {}.",
                leader.header().created_at(), previous_sub_dag_ts, commit_timestamp);
        }
        // Make sure the leader is the LAST certificate.
        //
        assert_eq!(leader.digest(), certificates.last().map(|c| c.digest()).unwrap_or_default());
        // Gate the derivation on the leader's own epoch: `seed_signature()` is `Some` exactly
        // when [`crate::forks::seed_signature_active`] holds for that epoch (#1032, #1086).
        let randomness = leader.header().seed_signature().map_or_else(
            || {
                // Pre-fork arm, wire-identical to origin/main: keccak256 of the leader
                // certificate's aggregate BLS signature. The aggregate varies with the 2f+1
                // signer subset, so a Byzantine leader can fork the shuffle - that
                // forkable-by-aggregate weakness is the documented legacy defect the fork
                // fixes; pre-fork epochs deliberately retain it so the fleet can roll
                // gradually.
                let randomness = leader.aggregated_signature().unwrap_or_else(|| {
                    error!(target: "engine", "BLS signature missing for leader - using default for closing epoch");
                    BlsSignature::default()
                });
                keccak256(randomness.to_bytes())
            },
            // Post-fork arm: extend the epoch seed chain with this commit. Two properties hold
            // together:
            //
            // - The folded signature bytes are digest-pinned: for every seed-active epoch
            //   (from `crate::forks::SEED_SIGNATURE_FORK_EPOCH` onward under the `adiri`
            //   feature, from genesis in all other builds) `seed_signature` is mandatory on
            //   the wire and covered by the header digest, so every certificate for this
            //   leader header carries identical bytes - unlike the certificate's aggregate
            //   signature, which varies with the 2f+1 signer subset and would let a Byzantine
            //   leader fork the shuffle (#1032).
            // - The value is a fold over the epoch's committed prefix, not a per-leader constant:
            //   `seed_chain` is the previous commit's value (or the epoch root at the first
            //   commit), so no authority can compute this commit's seed before the preceding
            //   commit is published.
            |sig| seed_chain.fold(leader.round(), sig).into_inner(),
        );
        let headers = certificates.into_iter().map(|c| c.into_header()).collect();
        let inner = Arc::new(CommittedSubDagInner {
            headers,
            reputation_scores,
            commit_timestamp,
            randomness,
        });
        Self { inner }
    }

    /// Make a default with just headers for testing.
    pub fn new_with_headers_for_test(headers: Vec<Header>) -> Self {
        // Anchor the fold on the pinned genesis placeholder rather than a defaulted chain value:
        // `EpochSeedChainValue` has no `Default` precisely so no path can silently re-root the
        // chain, and test fixtures are explicitly allowed to use the placeholder.
        let randomness = headers.last().map_or_else(
            || EpochSeedChainValue::genesis_placeholder().into_inner(),
            |leader| {
                leader.seed_signature().map_or_else(
                    // Pre-fork epochs mirror origin/main's constructor exactly: headers carry
                    // no aggregate signature, so main left `randomness` at its default - keep
                    // that byte-for-byte.
                    B256::default,
                    |sig| {
                        EpochSeedChainValue::genesis_placeholder()
                            .fold(leader.round(), sig)
                            .into_inner()
                    },
                )
            },
        );
        // Override default so we have one default header (the leader)
        // so a default value won't panic when used.
        let inner = Arc::new(CommittedSubDagInner {
            headers,
            reputation_scores: Default::default(),
            commit_timestamp: Default::default(),
            randomness,
        });
        Self { inner }
    }

    /// How many consensus headers are in this sub dag (including the leader).
    pub fn len(&self) -> usize {
        self.inner.headers.len()
    }

    /// Is this empty (i.e. contains no headers).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of batches contained in the sub dag.
    pub fn num_primary_batches(&self) -> usize {
        self.inner.headers.iter().map(|x| x.payload().len()).sum()
    }

    /// The leader header responsible for committing this sub-dag.
    pub fn leader(&self) -> &Header {
        self.inner.headers.last().expect("sub dag MUST have a leader")
    }

    /// The Certificate's round.
    pub fn leader_round(&self) -> Round {
        self.leader().round()
    }

    /// The Certificate's epoch.
    pub fn leader_epoch(&self) -> Epoch {
        self.leader().epoch()
    }

    /// Return the leaders commit timestamp.
    pub fn commit_timestamp(&self) -> TimestampSec {
        // If commit_timestamp is zero, then safely assume that this is an upgraded node that is
        // replaying this commit and field is never initialised. It's safe to fallback on leader's
        // timestamp.
        if self.inner.commit_timestamp == 0 {
            return *self.leader().created_at();
        }
        self.inner.commit_timestamp
    }

    /// Return the Certificates for this SubDag.
    pub fn headers(&self) -> &[Header] {
        &self.inner.headers
    }

    /// The committee-shuffle randomness: the epoch seed chain value as of this commit.
    ///
    /// For epochs where [`crate::forks::seed_signature_active`] is false this is the legacy
    /// keccak256(leader certificate aggregate signature) seed; active epochs use the epoch
    /// seed chain.
    pub fn randomness(&self) -> B256 {
        self.inner.randomness
    }

    /// This commit's epoch seed chain value, to be folded by the next commit of the same epoch.
    ///
    /// Returned as an [`EpochSeedChainValue`] rather than a raw `B256` so the anchor threaded from
    /// one commit to the next can only come from a commit that actually happened.
    ///
    /// For epochs where [`crate::forks::seed_signature_active`] is false the wrapped value is
    /// the legacy keccak256(leader certificate aggregate signature) seed; only active-epoch
    /// commits fold it into the epoch seed chain.
    pub fn seed_chain_value(&self) -> EpochSeedChainValue {
        EpochSeedChainValue::from_committed(self.inner.randomness)
    }

    pub fn reputation_scores(&self) -> &ReputationScores {
        &self.inner.reputation_scores
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for CommittedSubDag {
    type TypedDigest = ConsensusDigest;

    fn digest(&self) -> ConsensusDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // Instead of hashing serialized CommittedSubDag, hash the certificate digests instead.
        // Signatures in the certificates are not part of the commitment.
        for cert in &self.inner.headers {
            hasher.update(cert.digest().as_ref());
        }
        hasher.update(encode(&self.inner.reputation_scores).as_ref());
        hasher.update(encode(&self.inner.commit_timestamp).as_ref());
        hasher.update(self.inner.randomness.as_ref());
        ConsensusDigest(Digest { digest: hasher.finalize().into() })
    }
}

/// Shutdown token dropped when a task is properly shut down.
pub type ShutdownToken = mpsc::Sender<()>;

crate::crypto::digest_newtype! {
    /// Digest of a [`ConsensusOutput`]/[`CommittedSubDag`].
    pub struct ConsensusDigest;
}

// See test_utils output_tests.rs for this modules tests.

#[cfg(test)]
mod tests {
    use super::*;

    /// Build an output over `digests` with a default single-header sub-dag, so two outputs
    /// differ only in their committed payload digests.
    fn output_with_digests(number: u64, digests: Vec<BlockHash>) -> ConsensusOutput {
        ConsensusOutput::new(
            CommittedSubDag::new_with_headers_for_test(vec![Header::default()]),
            ConsensusHeaderDigest::default(),
            number,
            false,
            digests.into(),
            Vec::new(),
        )
    }

    /// The keeper below derives both grid points from the constant, which only stays meaningful
    /// while an epoch below it exists.
    #[cfg(feature = "adiri")]
    const _: () = assert!(crate::forks::PREVRANDAO_FORK_EPOCH != 0);

    /// The keeper discriminates the PREVRANDAO gate only while a seed-ACTIVE epoch exists below
    /// the fork point. `forks`' own ordering assert permits equality, and at equality the
    /// keeper's `pre_fork` probe is seed-dormant: every assertion in it would then pass through
    /// the seed conjunct alone, so deleting the PREVRANDAO fork point from
    /// `prevrandao_seed_active` would leave the keeper green. Pin the strict inequality here
    /// rather than tightening `forks`, which states a rollout contract (`>=`) that is correct on
    /// its own terms; an arming PR that lands on equality fails to compile its tests and has to
    /// retarget the keeper deliberately.
    #[cfg(feature = "adiri")]
    const _: () = assert!(
        crate::forks::PREVRANDAO_FORK_EPOCH > crate::forks::SEED_SIGNATURE_FORK_EPOCH,
        "PREVRANDAO_FORK_EPOCH must be strictly above SEED_SIGNATURE_FORK_EPOCH for \
         prev_randao_switches_arms_at_the_prevrandao_fork_epoch to discriminate the arms",
    );

    /// Build an output whose leader header carries `epoch`, so the fork arm is selected by the
    /// committing leader's epoch rather than by this build's feature set.
    fn output_at_epoch(epoch: Epoch, number: u64, digests: Vec<BlockHash>) -> ConsensusOutput {
        let leader = crate::HeaderBuilder::default().epoch(epoch).build();
        ConsensusOutput::new(
            CommittedSubDag::new_with_headers_for_test(vec![leader]),
            ConsensusHeaderDigest::default(),
            number,
            false,
            digests.into(),
            Vec::new(),
        )
    }

    /// Pin the exact post-fork byte layout: keccak256 of the versioned domain tag, the seed
    /// chain value, then the consensus block number and batch index as little-endian u64s.
    #[test]
    fn seeded_prev_randao_pins_the_exact_derivation() {
        let seed = B256::repeat_byte(0xAB);
        let expected = keccak256(
            [
                b"TN_PREVRANDAO_V1".as_slice(),
                seed.as_slice(),
                7u64.to_le_bytes().as_slice(),
                3u64.to_le_bytes().as_slice(),
            ]
            .concat(),
        );
        assert_eq!(seeded_prev_randao(seed, 7, 3), expected);
    }

    /// Every derivation input must move the value, and distinct inputs must not collide.
    #[test]
    fn seeded_prev_randao_varies_with_every_input() {
        let base = seeded_prev_randao(B256::repeat_byte(1), 1, 1);
        let variants = [
            seeded_prev_randao(B256::repeat_byte(2), 1, 1),
            seeded_prev_randao(B256::repeat_byte(1), 2, 1),
            seeded_prev_randao(B256::repeat_byte(1), 1, 2),
        ];
        variants
            .iter()
            .for_each(|variant| assert_ne!(&base, variant, "each input must alter the value"));
        assert_ne!(variants[0], variants[1], "seed and number changes must not collide");
        assert_ne!(variants[0], variants[2], "seed and index changes must not collide");
        assert_ne!(variants[1], variants[2], "number and index changes must not collide");
    }

    /// The anti-grinding property of #1247: two outputs identical except for their committed
    /// payload digests. Post-fork the executed `PREVRANDAO` is identical across them, so
    /// re-cutting the payload yields no new candidate values; pre-fork the legacy XOR
    /// replays byte-identically (and does differ, which is exactly the grinding channel).
    #[test]
    fn prev_randao_ignores_payload_construction_post_fork() {
        let digest_a = BlockHash::repeat_byte(0x11);
        let digest_b = BlockHash::repeat_byte(0x22);
        let output_a = output_with_digests(5, vec![digest_a]);
        let output_b = output_with_digests(5, vec![digest_b]);
        let randao_a = output_a.prev_randao(0, digest_a);
        let randao_b = output_b.prev_randao(0, digest_b);
        if crate::forks::prevrandao_seed_active(output_a.leader().epoch()) {
            let expected = seeded_prev_randao(output_a.committee_shuffle_seed(), 5, 0);
            assert_eq!(randao_a, expected, "post-fork value must be the seeded derivation");
            assert_eq!(randao_b, expected, "payload-only changes must not move PREVRANDAO");
        } else {
            let header_a: B256 = output_a.digest().into();
            let header_b: B256 = output_b.digest().into();
            assert_eq!(randao_a, header_a ^ digest_a, "pre-fork arm must replay the XOR");
            assert_eq!(randao_b, header_b ^ digest_b, "pre-fork arm must replay the XOR");
            assert_ne!(randao_a, randao_b, "the legacy XOR is payload-dependent");
        }
    }

    /// Batch indices within one output must yield distinct values, and the empty
    /// epoch-close call shape (`batch_index` 0, zero batch digest) must reduce pre-fork to
    /// the bare consensus header digest, the value the engine historically used there.
    #[test]
    fn prev_randao_separates_batch_indices_and_replays_the_empty_block() {
        let digest_a = BlockHash::repeat_byte(0x33);
        let digest_b = BlockHash::repeat_byte(0x44);
        let output = output_with_digests(9, vec![digest_a, digest_b]);
        assert_ne!(
            output.prev_randao(0, digest_a),
            output.prev_randao(1, digest_b),
            "sibling blocks of one output must not share a PREVRANDAO",
        );
        let empty = output.prev_randao(0, B256::ZERO);
        if crate::forks::prevrandao_seed_active(output.leader().epoch()) {
            assert_eq!(
                empty,
                seeded_prev_randao(output.committee_shuffle_seed(), 9, 0),
                "the empty block must use the seeded derivation at index 0",
            );
        } else {
            let header: B256 = output.digest().into();
            assert_eq!(empty, header, "a zero batch digest must reduce to the header digest");
        }
    }

    /// THE boundary keeper for #1247: the arm switch must happen AT `PREVRANDAO_FORK_EPOCH`.
    /// Both epochs derive from the constant, so an arming PR retargets this with no edit here.
    #[cfg(feature = "adiri")]
    #[test]
    fn prev_randao_switches_arms_at_the_prevrandao_fork_epoch() {
        let post_fork = crate::forks::PREVRANDAO_FORK_EPOCH;
        let pre_fork = post_fork - 1;
        // anti-vacuity tripwire, mirroring `committee_sweep_tests.rs`: an ambient override or an
        // armed fork would make both sides land on the same arm and pass for the wrong reason
        assert!(
            !crate::forks::prevrandao_seed_active(pre_fork),
            "epoch {pre_fork} must be pre-fork for this keeper to mean anything; is \
             TN_PREVRANDAO_FORK_EPOCH set in the environment?"
        );
        assert!(
            crate::forks::prevrandao_seed_active(post_fork),
            "epoch {post_fork} must be post-fork; is TN_PREVRANDAO_FORK_EPOCH set, or has the \
             seed fork been ordered after the PREVRANDAO fork?"
        );

        let digest = BlockHash::repeat_byte(0x55);
        let legacy = output_at_epoch(pre_fork, 11, vec![digest]);
        let seeded = output_at_epoch(post_fork, 11, vec![digest]);

        let legacy_header: B256 = legacy.digest().into();
        assert_eq!(
            legacy.prev_randao(0, digest),
            legacy_header ^ digest,
            "PREVRANDAO_FORK_EPOCH - 1 must replay the legacy XOR byte-identically",
        );
        assert_eq!(
            seeded.prev_randao(0, digest),
            seeded_prev_randao(seeded.committee_shuffle_seed(), 11, 0),
            "the gate must fire from PREVRANDAO_FORK_EPOCH onward (`>=`, not `>`)",
        );
        // discriminate the arms on ONE output. Comparing `legacy` against `seeded` would pass
        // through their differing leader headers even if the gate never fired at all, so the
        // post-fork value is checked against the legacy recomposition of its own output.
        let seeded_as_legacy: B256 = B256::from(seeded.digest()) ^ digest;
        assert_ne!(
            seeded.prev_randao(0, digest),
            seeded_as_legacy,
            "at PREVRANDAO_FORK_EPOCH the seeded arm must not reproduce that output's legacy XOR",
        );
    }

    /// The always-active counterpart (non-adiri): no dormant epoch exists to switch from, so this
    /// states that every epoch takes the seeded arm rather than asserting a switch vacuously.
    ///
    /// The grid is genesis, two early epochs, a mid-range epoch and the ceiling. Nothing here
    /// mirrors an adiri fork constant: none of them exist in this build, so a literal epoch
    /// borrowed from that schedule could only go stale.
    #[cfg(not(feature = "adiri"))]
    #[test]
    fn prev_randao_takes_the_seeded_arm_at_every_epoch_without_adiri() {
        [0u32, 1, 2, Epoch::MAX / 2, Epoch::MAX].into_iter().for_each(|epoch| {
            let digest = BlockHash::repeat_byte(0x55);
            let output = output_at_epoch(epoch, 11, vec![digest]);
            assert!(
                crate::forks::prevrandao_seed_active(epoch),
                "non-adiri builds are active from genesis; epoch {epoch} must be post-fork. is \
                 TN_PREVRANDAO_FORK_EPOCH or TN_SEED_SIGNATURE_FORK_EPOCH set in the environment?"
            );
            assert_eq!(
                output.prev_randao(0, digest),
                seeded_prev_randao(output.committee_shuffle_seed(), 11, 0),
                "epoch {epoch} must use the seeded derivation",
            );
        });
    }
}
