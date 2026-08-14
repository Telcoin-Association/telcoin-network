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
use tracing::{debug, error, warn};

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
    /// The ECDSA address of the authority that produced the batch. This address is used as the
    /// block beneficiary during execution. This may not be unique within a single
    /// [ConsensusOutput].
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
}

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

/// Resolve the `commit_timestamp` a new [`CommittedSubDag`] records.
///
/// Pre-fork (`strict` false) this is the legacy clamp `max(previous, leader_created_at)`,
/// byte-identical to the historical chain: two consecutive commits may share one wall-clock
/// second, and a tied pair collides in the repurposed EIP-4788 beacon-roots ring buffer
/// (#1191). Post-fork (`strict` true, see [`crate::forks::strict_commit_timestamp_active`])
/// the floor moves to `previous + 1`: consecutive commits never tie, so every commit of an
/// epoch keys a unique buffer slot. The recorded time may then run ahead of wall clock while
/// commits arrive faster than one per second; the drift is bounded by the longest
/// faster-than-one-per-second commit run and shrinks again whenever a real second passes with
/// no commit, every downstream consumer reads this same chain (block timestamps,
/// epoch-boundary checks), and no consumer re-validates it against local wall clock, so the
/// drift is self-consistent.
///
/// A commit with no previous sub-dag has no floor in either arm and records the leader
/// timestamp as-is. That makes the strict guarantee intra-epoch only: the first commit of an
/// epoch starts from an empty per-epoch pack (`previous` is `None`), so it can still tie the
/// prior epoch's closing commit — a documented residual of #1191, not covered by this fork.
/// The `+ 1` saturates at `u64::MAX`, where strictness degrades to the legacy tie rather than
/// wrapping; that ceiling is unreachable for wall-clock seconds.
///
/// `previous` is the previous sub-dag's raw stored `commit_timestamp`, not the
/// [`CommittedSubDag::commit_timestamp`] accessor: the pre-fork arm must reproduce the
/// historical clamp byte-for-byte, and the accessor's zero-means-uninitialized fallback
/// cannot apply in the strict arm, because the previous sub-dag always belongs to the same
/// (post-fork) epoch and every sub-dag derived under this code records a nonzero timestamp.
fn resolved_commit_timestamp(
    strict: bool,
    previous: Option<TimestampSec>,
    leader_created_at: TimestampSec,
) -> TimestampSec {
    let floor = if strict {
        previous.map(|ts| ts.saturating_add(1)).unwrap_or_default()
    } else {
        previous.unwrap_or_default()
    };
    floor.max(leader_created_at)
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
        let previous_sub_dag_ts = previous_sub_dag.map(|s| s.inner.commit_timestamp);
        let commit_timestamp = resolved_commit_timestamp(
            crate::forks::strict_commit_timestamp_active(leader.epoch()),
            previous_sub_dag_ts,
            *leader.header().created_at(),
        );

        if previous_sub_dag_ts.unwrap_or_default() > *leader.header().created_at() {
            warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to {}.",
                leader.header().created_at(), previous_sub_dag_ts.unwrap_or_default(), commit_timestamp);
        } else if commit_timestamp != *leader.header().created_at() {
            // Only the strict arm reaches this branch: a tie with the previous commit was
            // bumped one second past the leader-reported time (#1191). Ties are routine while
            // commits arrive faster than one per second, so this is debug, not warn.
            debug!(sub_dag_index = ?sub_dag_index, "Tied leader timestamp {} bumped to {} to keep commit timestamps strictly increasing.",
                leader.header().created_at(), commit_timestamp);
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
//
// The tests below are the deliberate exception: `resolved_commit_timestamp` is private, so its
// arms are only reachable from inside the crate, and the fork-arm split must run under BOTH CI
// lanes (default features and `adiri`), which build tn-types but not the integration-test
// crates.
#[cfg(test)]
mod tests {
    use super::*;

    /// The legacy arm is byte-identical to the historical clamp `max(previous, created_at)`:
    /// ties and decreases both collapse onto the previous timestamp, which is the pre-fork
    /// wire behavior every already-committed sub-dag digest depends on.
    #[test]
    fn legacy_commit_timestamp_matches_history() {
        // No previous sub-dag: the leader timestamp is recorded as-is, including zero.
        assert_eq!(resolved_commit_timestamp(false, None, 0), 0);
        assert_eq!(resolved_commit_timestamp(false, None, 7), 7);
        // Advancing leader wins.
        assert_eq!(resolved_commit_timestamp(false, Some(10), 11), 11);
        // Tie collapses onto the shared second (the #1191 collision case).
        assert_eq!(resolved_commit_timestamp(false, Some(10), 10), 10);
        // Decrease clamps to the previous timestamp, again a tie.
        assert_eq!(resolved_commit_timestamp(false, Some(10), 9), 10);
    }

    /// The strict arm floors at `previous + 1`: no chained pair of commits can share a
    /// timestamp, so no two commits of an epoch key the same EIP-4788 ring-buffer slot
    /// (#1191).
    #[test]
    fn strict_commit_timestamp_never_ties_previous() {
        // No previous sub-dag: unchanged from the legacy arm, including zero.
        assert_eq!(resolved_commit_timestamp(true, None, 0), 0);
        assert_eq!(resolved_commit_timestamp(true, None, 7), 7);
        // Advancing leader wins exactly as before.
        assert_eq!(resolved_commit_timestamp(true, Some(10), 11), 11);
        // Tie bumps past the shared second instead of colliding.
        assert_eq!(resolved_commit_timestamp(true, Some(10), 10), 11);
        // Decrease also lands strictly past the previous commit.
        assert_eq!(resolved_commit_timestamp(true, Some(10), 9), 11);
        // The floor saturates at the ceiling instead of wrapping.
        assert_eq!(resolved_commit_timestamp(true, Some(u64::MAX), 0), u64::MAX);
    }

    /// `CommittedSubDag::new` wires the gate for this build: default-feature builds are
    /// strict from genesis, adiri builds stay on the legacy clamp while
    /// [`crate::forks::STRICT_COMMIT_TIMESTAMP_FORK_EPOCH`] is dormant. Both leaders carry
    /// epoch 0 and one shared NONZERO `created_at`, so the chained pair is a genuine tie and
    /// the first commit's recorded value is observable (a zero would also satisfy the
    /// accessor's uninitialized fallback, making the assertion vacuous).
    #[test]
    fn new_applies_this_builds_fork_arm_to_tied_leaders() {
        const TIED_SECOND: TimestampSec = 7;
        let mut leader = Certificate::default();
        leader.update_header_created_at_for_test(TIED_SECOND);
        let first = CommittedSubDag::new(
            vec![leader.clone()],
            leader.clone(),
            0,
            ReputationScores::default(),
            None,
            EpochSeedChainValue::genesis_placeholder(),
        );
        let second = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            Some(first.clone()),
            EpochSeedChainValue::genesis_placeholder(),
        );
        assert_eq!(
            first.inner.commit_timestamp, TIED_SECOND,
            "the first commit must record the leader second as-is",
        );
        #[cfg(not(feature = "adiri"))]
        assert_eq!(
            second.inner.commit_timestamp,
            TIED_SECOND + 1,
            "default-feature builds must bump a tied pair past the shared second",
        );
        #[cfg(feature = "adiri")]
        assert_eq!(
            second.inner.commit_timestamp, TIED_SECOND,
            "adiri must keep the legacy tie while the fork is dormant",
        );
    }
}
