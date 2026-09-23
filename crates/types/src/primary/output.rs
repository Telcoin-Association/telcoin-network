//! The ouput from consensus (bullshark)
//! See test_utils output_tests.rs for this modules tests.

use super::ConsensusHeader;
use crate::{
    codec::next_seq_field, crypto, encode, forks::subsecond_timestamp_active, Address, Batch,
    BlockHash, BlsSignature, Certificate, ConsensusHeaderDigest, ConsensusNumHash, Digest, Epoch,
    EpochSeedChainValue, Hash, Header, ReputationScores, Round, SealedHeader, TimestampMs,
    TimestampSec, B256,
};
use alloy::primitives::keccak256;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
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

    /// Timestamp for when the subdag was committed, in whole seconds.
    ///
    /// This is the EVM block `timestamp` source: [`CommittedSubDag::commit_timestamp`], the
    /// floor of [`Self::committed_at_ms`].
    pub fn committed_at(&self) -> TimestampSec {
        self.inner.sub_dag.commit_timestamp()
    }

    /// Timestamp for when the subdag was committed, in milliseconds.
    ///
    /// See [`CommittedSubDag::commit_timestamp_ms`]; the sub-second part is always 0 for
    /// leaders of epochs where [`crate::forks::subsecond_timestamp_active`] does not hold.
    pub fn committed_at_ms(&self) -> TimestampMs {
        self.inner.sub_dag.commit_timestamp_ms()
    }

    /// Whether this output's commit time has reached the epoch `boundary` (whole seconds).
    ///
    /// Delegates to [`CommittedSubDag::reaches_epoch_boundary`], the single seconds-based
    /// epoch-boundary predicate.
    pub fn reaches_epoch_boundary(&self, boundary: TimestampSec) -> bool {
        self.inner.sub_dag.reaches_epoch_boundary(boundary)
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

/// `CommittedSubDag` inner data.
///
/// Deliberately carries no serde derives: every encode and decode path (network wire, storage
/// packs) routes through [`CommittedSubDagRef`] and the hand-written [`CommittedSubDag`] impls,
/// so the epoch-gated `commit_timestamp_millis` field (gated by
/// [`crate::forks::subsecond_timestamp_active`] for the leader's epoch) cannot be bypassed.
#[derive(PartialEq, Debug)]
struct CommittedSubDagInner {
    /// The sequence of committed certificates.
    /// Note the last element MUST be the leader.
    headers: Vec<Header>,
    /// The so far calculated reputation score for nodes
    reputation_scores: ReputationScores,
    /// The timestamp that should identify this commit, in whole seconds. This is guaranteed to be
    /// monotonically incremented. This is not necessarily the leader's timestamp. We compare the
    /// leader's timestamp with the previously committed sub dag timestamp and we always keep the
    /// max. The sub-second part lives in `commit_timestamp_millis`.
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
    /// The sub-second part of `commit_timestamp` in milliseconds, always in `0..=999`.
    ///
    /// On the wire (after `randomness`) and in the digest only when
    /// [`crate::forks::subsecond_timestamp_active`] holds for the leader's epoch; always 0 when
    /// the gate is inactive, including for a sub-dag without headers.
    commit_timestamp_millis: u16,
}

/// Contains the committed output from Bullshark consensus.
/// Note it stores Headers without certificates, all validation
/// should be complete.  Future validation can be done by verifying
/// the consensus chain against signed checkpoints (like epoch records).
#[derive(Clone, PartialEq, Debug)]
pub struct CommittedSubDag {
    inner: Arc<CommittedSubDagInner>,
}

/// Number of `CommittedSubDagInner` wire fields when the sub-second timestamp fork is inactive
/// for the leader's epoch.
const SUB_DAG_FIELDS_LEGACY: usize = 4;
/// Number of `CommittedSubDagInner` wire fields once `commit_timestamp_millis` is active for the
/// leader's epoch.
const SUB_DAG_FIELDS_V2: usize = 5;
/// Field names for [`serde::Deserializer::deserialize_struct`], superset (latest) layout: the
/// legacy layout is a prefix of it.
const SUB_DAG_FIELD_NAMES: [&str; SUB_DAG_FIELDS_V2] =
    ["headers", "reputation_scores", "commit_timestamp", "randomness", "commit_timestamp_millis"];

/// Whether a sub-dag over `headers` carries a millisecond commit timestamp: the ONE gate
/// decision shared by serialization, deserialization, and the digest. The constructor makes the
/// same decision on the leader certificate's header before the headers are collected.
///
/// Keyed on the leader's own epoch (the last header), never node-local state, so historical
/// sub-dags keep their historical layout. A sub-dag without headers has no leader and keeps the
/// legacy layout.
fn leader_subsecond_active(headers: &[Header]) -> bool {
    headers.last().is_some_and(|leader| subsecond_timestamp_active(leader.epoch()))
}

/// Borrowed serialization view over [`CommittedSubDagInner`]: the ONE definition of sub-dag
/// wire bytes.
///
/// Writes the four legacy fields in declaration order, then `commit_timestamp_millis` only when
/// [`leader_subsecond_active`] holds. bcs emits no framing for structs, so the legacy layout is
/// byte-identical to the derive it replaces.
struct CommittedSubDagRef<'a>(&'a CommittedSubDagInner);

impl Serialize for CommittedSubDagRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let inner = self.0;
        let millis_active = leader_subsecond_active(&inner.headers);
        let fields = if millis_active { SUB_DAG_FIELDS_V2 } else { SUB_DAG_FIELDS_LEGACY };
        let mut state = serializer.serialize_struct("CommittedSubDagInner", fields)?;
        state.serialize_field("headers", &inner.headers)?;
        state.serialize_field("reputation_scores", &inner.reputation_scores)?;
        state.serialize_field("commit_timestamp", &inner.commit_timestamp)?;
        state.serialize_field("randomness", &inner.randomness)?;
        if millis_active {
            state.serialize_field("commit_timestamp_millis", &inner.commit_timestamp_millis)?;
        }
        state.end()
    }
}

impl Serialize for CommittedSubDag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        CommittedSubDagRef(&self.inner).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CommittedSubDag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Reads `headers` first (its last element is the leader, whose epoch selects the
        /// layout), then the remaining legacy fields, then `commit_timestamp_millis` only when
        /// [`leader_subsecond_active`] holds (earlier sub-dags fill 0).
        struct CommittedSubDagVisitor;

        impl<'de> serde::de::Visitor<'de> for CommittedSubDagVisitor {
            type Value = CommittedSubDag;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a CommittedSubDag: four legacy fields, plus commit_timestamp_millis when \
                     the sub-second timestamp fork is active for the leader's epoch",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let headers: Vec<Header> = next_seq_field(&mut seq, "headers")?;
                let reputation_scores = next_seq_field(&mut seq, "reputation_scores")?;
                let commit_timestamp = next_seq_field(&mut seq, "commit_timestamp")?;
                let randomness = next_seq_field(&mut seq, "randomness")?;
                // not read at all when inactive, so an extra trailing field on an earlier
                // epoch's sub-dag stays unconsumed and fails decode exactly as before the fork
                let commit_timestamp_millis = if leader_subsecond_active(&headers) {
                    let millis: u16 = next_seq_field(&mut seq, "commit_timestamp_millis")?;
                    if millis >= 1000 {
                        return Err(serde::de::Error::invalid_value(
                            serde::de::Unexpected::Unsigned(u64::from(millis)),
                            &"commit_timestamp_millis below 1000",
                        ));
                    }
                    millis
                } else {
                    0
                };
                let inner = CommittedSubDagInner {
                    headers,
                    reputation_scores,
                    commit_timestamp,
                    randomness,
                    commit_timestamp_millis,
                };
                Ok(CommittedSubDag { inner: Arc::new(inner) })
            }
        }

        deserializer.deserialize_struct(
            "CommittedSubDagInner",
            &SUB_DAG_FIELD_NAMES,
            CommittedSubDagVisitor,
        )
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
            commit_timestamp_millis: 0,
        });
        Self { inner }
    }
}

impl CommittedSubDag {
    /// Create a new CommittedSubDag.
    /// Note that leader MUST be the last element of certificates or this will panic.
    ///
    /// `previous_sub_dag` resolves the monotonic `commit_timestamp` only. `seed_chain` is the epoch
    /// seed chain value this commit folds into and is deliberately a separate, non-optional
    /// argument: the two must never be conflated, because an absent previous sub-dag is a normal
    /// first-commit condition while an absent chain anchor is unrepresentable (see
    /// [`EpochSeedChainValue`]).
    ///
    /// Equivalent to [`Self::new_with_commit_floor`] without an epoch commit floor.
    pub fn new(
        certificates: Vec<Certificate>,
        leader: Certificate,
        sub_dag_index: SequenceNumber,
        reputation_scores: ReputationScores,
        previous_sub_dag: Option<CommittedSubDag>,
        seed_chain: EpochSeedChainValue,
    ) -> Self {
        Self::new_with_commit_floor(
            certificates,
            leader,
            sub_dag_index,
            reputation_scores,
            previous_sub_dag.as_ref(),
            None,
            seed_chain,
        )
    }

    /// Create a new CommittedSubDag whose commit timestamp is also bounded below by an epoch
    /// commit floor.
    /// Note that leader MUST be the last element of certificates or this will panic.
    ///
    /// The commit timestamp depends on [`crate::forks::subsecond_timestamp_active`] for the
    /// leader's epoch:
    ///
    /// - Inactive: whole seconds only, `max(previous commit_timestamp, leader created_at)`, with
    ///   the previous sub-dag's raw stored seconds (0 when absent) and a sub-second part of 0.
    ///   `epoch_commit_floor` is ignored, so pre-fork commits replay byte-identically.
    /// - Active: milliseconds, strictly increasing within an epoch. The floor is the previous
    ///   sub-dag's [`Self::commit_timestamp_ms`] when there is one (a pre-fork previous sub-dag
    ///   resolves to whole seconds times 1000), else `epoch_commit_floor`. The commit timestamp is
    ///   the leader's [`Header::created_at_ms`] raised to at least 1 ms after the floor; without
    ///   any floor it is the leader's timestamp.
    ///
    /// `epoch_commit_floor` is the lower bound for the first commit of an epoch: the previous
    /// epoch's closing EVM block timestamp, whole seconds times 1000 (`None` for epoch 0). Every
    /// node must pass the same value for the same commit, so it has to come from committed
    /// history, never from local clocks or node progress.
    ///
    /// That floor drops the sub-second part of the previous epoch's last commit, so the first
    /// commit of an epoch lands after the closing second but can sit up to 998 ms below the
    /// previous epoch's last [`Self::commit_timestamp_ms`]. Across an epoch seam only the whole
    /// seconds, and with them the EVM `timestamp`, are guaranteed not to decrease.
    pub fn new_with_commit_floor(
        certificates: Vec<Certificate>,
        leader: Certificate,
        sub_dag_index: SequenceNumber,
        reputation_scores: ReputationScores,
        previous_sub_dag: Option<&CommittedSubDag>,
        epoch_commit_floor: Option<TimestampMs>,
        seed_chain: EpochSeedChainValue,
    ) -> Self {
        let millis_active = subsecond_timestamp_active(leader.header().epoch());
        let (commit_timestamp, commit_timestamp_millis) = if millis_active {
            let commit_ms = Self::strict_commit_timestamp_ms(
                leader.header(),
                sub_dag_index,
                previous_sub_dag,
                epoch_commit_floor,
            );
            (commit_ms.secs(), commit_ms.subsec_millis())
        } else {
            // Narwhal enforces some invariants on the header.created_at, so we can use it as a
            // timestamp.
            let previous_sub_dag_ts =
                previous_sub_dag.map(|s| s.inner.commit_timestamp).unwrap_or_default();
            let commit_timestamp = previous_sub_dag_ts.max(*leader.header().created_at());

            if previous_sub_dag_ts > *leader.header().created_at() {
                warn!(sub_dag_index = ?sub_dag_index, "Leader timestamp {} is older than previously committed sub dag timestamp {}. Auto-correcting to max {}.",
                    leader.header().created_at(), previous_sub_dag_ts, commit_timestamp);
            }
            (commit_timestamp, 0)
        };
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
            commit_timestamp_millis,
        });
        Self { inner }
    }

    /// The post-fork commit timestamp: the leader's millisecond timestamp raised to at least
    /// 1 ms after the floor (see [`Self::new_with_commit_floor`]).
    fn strict_commit_timestamp_ms(
        leader: &Header,
        sub_dag_index: SequenceNumber,
        previous_sub_dag: Option<&CommittedSubDag>,
        epoch_commit_floor: Option<TimestampMs>,
    ) -> TimestampMs {
        let leader_ms = leader.created_at_ms();
        let Some(floor) = previous_sub_dag.map(|s| s.commit_timestamp_ms()).or(epoch_commit_floor)
        else {
            return leader_ms;
        };
        let min_ms = floor.saturating_add_millis(1);
        if leader_ms >= min_ms {
            return leader_ms;
        }
        // the previous leader of the same epoch is an ancestor of this one and certified parents
        // are strictly older post-fork, so an in-epoch clamp needs an earlier clamp at the epoch
        // seam; it stays a warning as a tripwire. the first commit of an epoch is routinely
        // raised above the previous epoch's closing timestamp, which is expected
        if previous_sub_dag.is_some_and(|s| s.leader_epoch() == leader.epoch()) {
            warn!(
                target: "tn::consensus",
                sub_dag_index,
                %leader_ms,
                previous_commit_ms = %floor,
                commit_ms = %min_ms,
                "leader timestamp is not newer than the previously committed sub dag timestamp; auto-correcting to 1 ms after it",
            );
        } else {
            debug!(
                target: "tn::consensus",
                sub_dag_index,
                epoch = leader.epoch(),
                %leader_ms,
                floor_ms = %floor,
                commit_ms = %min_ms,
                "first commit of the epoch clamped to 1 ms after the commit floor",
            );
        }
        min_ms
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
            commit_timestamp_millis: 0,
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

    /// Return the commit timestamp in milliseconds: the single resolution point for this
    /// sub-dag's commit time.
    ///
    /// A stored timestamp of zero (both the seconds and the sub-second part) is the legacy
    /// uninitialised default and falls back to the leader's [`Header::created_at_ms`]. Otherwise
    /// this is the stored seconds and sub-second part; the sub-second part is always 0 for
    /// leaders of epochs where [`crate::forks::subsecond_timestamp_active`] does not hold, so a
    /// pre-fork sub-dag resolves to its whole seconds times 1000.
    pub fn commit_timestamp_ms(&self) -> TimestampMs {
        // If commit_timestamp is zero, then safely assume that this is an upgraded node that is
        // replaying this commit and field is never initialised. It's safe to fallback on leader's
        // timestamp. a non-zero sub-second part means the field was initialised.
        if self.inner.commit_timestamp == 0 && self.inner.commit_timestamp_millis == 0 {
            return self.leader().created_at_ms();
        }
        TimestampMs::from_parts(self.inner.commit_timestamp, self.inner.commit_timestamp_millis)
    }

    /// Return the commit timestamp in whole seconds, the floor of [`Self::commit_timestamp_ms`]
    /// (including its zero fallback to the leader's timestamp).
    pub fn commit_timestamp(&self) -> TimestampSec {
        self.commit_timestamp_ms().secs()
    }

    /// Whether this commit has reached the epoch `boundary` (whole seconds): the single
    /// seconds-based epoch-boundary predicate.
    ///
    /// Compares [`Self::commit_timestamp`], so it holds exactly when [`Self::commit_timestamp_ms`]
    /// is at least `1000 * boundary`: `floor(ms / 1000) >= boundary` is equivalent to
    /// `ms >= 1000 * boundary`. Sub-second commit times therefore never move the boundary
    /// decision off the seconds grid.
    pub fn reaches_epoch_boundary(&self, boundary: TimestampSec) -> bool {
        self.commit_timestamp() >= boundary
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
        // gated like the wire field, so sub-dags of inactive epochs keep their historical digest
        if leader_subsecond_active(&self.inner.headers) {
            hasher.update(encode(&self.inner.commit_timestamp_millis).as_ref());
        }
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
    use crate::{
        decode, try_decode, AuthorityIdentifier, BlockNumHash, DefaultHashFunction, HeaderBuilder,
        HeaderDigest,
    };
    use indexmap::IndexMap;
    use rand::SeedableRng as _;
    use std::collections::{BTreeMap, BTreeSet};

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
    /// its own terms. The adiri schedule clears it (seed fork 383, PREVRANDAO fork 574, so the
    /// `pre_fork` probe at 573 is seed-active); a retarget that lands on equality fails to
    /// compile its tests and has to rework the keeper deliberately.
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

    /// THE boundary keeper for #1247: the arm switch must happen AT `PREVRANDAO_FORK_EPOCH`
    /// (574 on adiri, so epoch 573 replays the legacy XOR and 574 takes the seeded derivation).
    /// Both epochs derive from the constant, so moving the fork epoch retargets this with no
    /// edit here.
    #[cfg(feature = "adiri")]
    #[test]
    fn prev_randao_switches_arms_at_the_prevrandao_fork_epoch() {
        let post_fork = crate::forks::PREVRANDAO_FORK_EPOCH;
        let pre_fork = post_fork - 1;
        // anti-vacuity tripwire, mirroring `committee_sweep_tests.rs`: the probes are 573 (legacy)
        // and 574 (seeded), and an ambient override (`TN_PREVRANDAO_FORK_EPOCH`, or a
        // `TN_SEED_SIGNATURE_FORK_EPOCH` above 574) would put both on the same arm and let the
        // test pass for the wrong reason
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

    /// An epoch whose leaders carry millisecond commit timestamps under the running cfg:
    /// `u32::MAX` under `adiri` (the placeholder `SUBSECOND_TIMESTAMP_FORK_EPOCH` itself,
    /// compared with `>=`), epoch zero elsewhere (non-adiri is active from genesis).
    fn v2_epoch() -> Epoch {
        let epoch = if cfg!(feature = "adiri") { u32::MAX } else { 0 };
        // anti-vacuity: every post-fork assertion below means nothing on an inactive epoch
        assert!(
            subsecond_timestamp_active(epoch),
            "epoch {epoch} must be sub-second-active; is TN_SUBSECOND_TIMESTAMP_FORK_EPOCH or \
             TN_SEED_SIGNATURE_FORK_EPOCH set in the environment?"
        );
        epoch
    }

    /// Adiri epochs below the sub-second fork: the legacy header layout (epoch 0) and the
    /// seed-signature layout (`SEED_SIGNATURE_FORK_EPOCH`), each checked to be inactive.
    #[cfg(feature = "adiri")]
    fn pre_fork_epochs() -> [Epoch; 2] {
        let epochs = [0, crate::forks::SEED_SIGNATURE_FORK_EPOCH];
        for epoch in epochs {
            assert!(
                !subsecond_timestamp_active(epoch),
                "epoch {epoch} must be pre-fork; is TN_SUBSECOND_TIMESTAMP_FORK_EPOCH set in the \
                 environment?"
            );
        }
        epochs
    }

    /// Shorthand for [`TimestampMs::from_parts`].
    fn ms(secs: TimestampSec, millis: u16) -> TimestampMs {
        TimestampMs::from_parts(secs, millis)
    }

    /// A leader certificate at `epoch` created at `created_at`, set through the certificate's
    /// test helpers (epoch first, so the sub-second part survives the builder's gate).
    fn leader_at(epoch: Epoch, created_at: TimestampMs) -> Certificate {
        let mut leader = Certificate::default();
        leader.update_header_epoch_for_test(epoch);
        leader.update_header_created_at_ms_for_test(created_at);
        leader
    }

    /// Commit a single-certificate sub-dag led by a header at `epoch` created at `created_at`.
    fn commit_at(
        epoch: Epoch,
        created_at: TimestampMs,
        previous: Option<&CommittedSubDag>,
        epoch_commit_floor: Option<TimestampMs>,
    ) -> CommittedSubDag {
        let leader = leader_at(epoch, created_at);
        CommittedSubDag::new_with_commit_floor(
            vec![leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            previous,
            epoch_commit_floor,
            EpochSeedChainValue::genesis_placeholder(),
        )
    }

    /// The stored `(seconds, millis)` pair, bypassing every accessor.
    fn stored(sub_dag: &CommittedSubDag) -> (TimestampSec, u16) {
        (sub_dag.inner.commit_timestamp, sub_dag.inner.commit_timestamp_millis)
    }

    /// Post-fork, every commit lands strictly after the previous one: a leader equal to or
    /// older than the previous commit is raised to 1 ms after it (carrying into the next whole
    /// second here), a newer leader keeps its own timestamp. `new` agrees with
    /// `new_with_commit_floor` without a floor.
    #[test]
    fn post_fork_commit_timestamps_strictly_increase() {
        let epoch = v2_epoch();
        let previous_ms = ms(1_700_000_000, 999);
        let previous = commit_at(epoch, previous_ms, None, None);
        assert_eq!(previous.commit_timestamp_ms(), previous_ms, "no floor keeps the leader time");
        let next = ms(1_700_000_001, 0);
        for (label, leader_ms, expected) in [
            ("equal", previous_ms, next),
            ("older", ms(1_699_999_999, 500), next),
            ("newer", ms(1_700_000_002, 250), ms(1_700_000_002, 250)),
        ] {
            let sub_dag = commit_at(epoch, leader_ms, Some(&previous), None);
            assert_eq!(sub_dag.commit_timestamp_ms(), expected, "{label} leader");
            assert_eq!(
                stored(&sub_dag),
                (expected.secs(), expected.subsec_millis()),
                "{label} leader stored the wrong parts"
            );
            let leader = leader_at(epoch, leader_ms);
            let via_new = CommittedSubDag::new(
                vec![leader.clone()],
                leader,
                1,
                ReputationScores::default(),
                Some(previous.clone()),
                EpochSeedChainValue::genesis_placeholder(),
            );
            assert_eq!(via_new, sub_dag, "{label} leader: `new` must delegate without a floor");
        }
    }

    /// Pre-fork (adiri), the commit timestamp is today's legacy seconds `max` over the previous
    /// sub-dag's RAW stored seconds (no zero fallback), the sub-second part stays 0, and the
    /// epoch commit floor is ignored.
    #[cfg(feature = "adiri")]
    #[test]
    fn pre_fork_commit_timestamp_is_the_legacy_seconds_max() {
        let previous_secs = 1_700_000_000;
        let floor = Some(ms(previous_secs + 100, 0));
        for epoch in pre_fork_epochs() {
            let previous = commit_at(epoch, ms(previous_secs, 0), None, None);
            for leader_secs in [previous_secs, previous_secs - 1, previous_secs + 2] {
                // the builder normalizes this sub-second part away for a pre-fork header
                let sub_dag = commit_at(epoch, ms(leader_secs, 750), Some(&previous), floor);
                let expected = previous_secs.max(leader_secs);
                assert_eq!(stored(&sub_dag), (expected, 0), "epoch {epoch}, leader {leader_secs}");
                assert_eq!(sub_dag.commit_timestamp(), expected, "epoch {epoch}");
                assert_eq!(sub_dag.commit_timestamp_ms(), ms(expected, 0), "epoch {epoch}");
            }
            let first = commit_at(epoch, ms(previous_secs, 0), None, floor);
            assert_eq!(stored(&first), (previous_secs, 0), "epoch {epoch}: floor must be ignored");
            // the legacy path reads the raw field: an uninitialised previous counts as 0 even
            // though its resolved timestamp falls back to a later leader time
            let uninitialised =
                CommittedSubDag::new_with_headers_for_test(vec![HeaderBuilder::default()
                    .epoch(epoch)
                    .created_at(previous_secs + 5)
                    .build()]);
            let sub_dag = commit_at(epoch, ms(previous_secs, 0), Some(&uninitialised), None);
            assert_eq!(stored(&sub_dag), (previous_secs, 0), "epoch {epoch}: raw field expected");
        }
    }

    /// Post-fork, the epoch commit floor bounds the first commit of an epoch (no previous
    /// sub-dag): an older or equal leader lands 1 ms after it, a newer leader keeps its own
    /// time. A previous sub-dag takes precedence over the floor.
    #[test]
    fn epoch_floor_seeds_the_first_commit() {
        let epoch = v2_epoch();
        let floor = ms(1_700_000_010, 0);
        let after = ms(1_700_000_010, 1);
        for (label, leader_ms, expected) in [
            ("older", ms(1_700_000_009, 400), after),
            ("equal", floor, after),
            ("newer", ms(1_700_000_010, 2), ms(1_700_000_010, 2)),
        ] {
            let sub_dag = commit_at(epoch, leader_ms, None, Some(floor));
            assert_eq!(sub_dag.commit_timestamp_ms(), expected, "{label} leader");
        }
        let unbounded = commit_at(epoch, ms(1_700_000_009, 400), None, None);
        assert_eq!(unbounded.commit_timestamp_ms(), ms(1_700_000_009, 400), "no floor, no clamp");

        let previous = commit_at(epoch, ms(1_700_000_005, 0), None, None);
        let sub_dag = commit_at(epoch, ms(1_700_000_004, 0), Some(&previous), Some(floor));
        assert_eq!(
            sub_dag.commit_timestamp_ms(),
            ms(1_700_000_005, 1),
            "the previous sub-dag, not the epoch floor, must bound the commit"
        );
    }

    /// The fork seam (adiri): a pre-fork previous sub-dag resolves to whole seconds times 1000,
    /// so the first post-fork commit lands 1 ms after that, including for a previous sub-dag
    /// whose stored timestamp is the uninitialised legacy default.
    #[cfg(feature = "adiri")]
    #[test]
    fn fork_seam_floors_on_the_pre_fork_whole_seconds() {
        let post_fork = v2_epoch();
        let secs = 1_700_000_020;
        for pre_fork in pre_fork_epochs() {
            let previous = commit_at(pre_fork, ms(secs, 0), None, None);
            assert_eq!(previous.commit_timestamp_ms(), ms(secs, 0), "pre-fork floor is secs*1000");
            for leader_ms in [ms(secs, 0), ms(secs - 1, 999)] {
                let sub_dag = commit_at(post_fork, leader_ms, Some(&previous), None);
                assert_eq!(sub_dag.commit_timestamp_ms(), ms(secs, 1), "leader {leader_ms}");
            }
            let uninitialised =
                CommittedSubDag::new_with_headers_for_test(vec![HeaderBuilder::default()
                    .epoch(pre_fork)
                    .created_at(secs)
                    .build()]);
            let sub_dag = commit_at(post_fork, ms(secs, 0), Some(&uninitialised), None);
            assert_eq!(sub_dag.commit_timestamp_ms(), ms(secs, 1), "zero fallback at the seam");
        }
    }

    /// `commit_timestamp` is the floor of `commit_timestamp_ms` for committed, clamped,
    /// defaulted and zero-fallback sub-dags, and the output accessors agree with the sub-dag.
    /// A zero seconds field with a non-zero sub-second part is an initialised value, not the
    /// legacy default.
    #[test]
    fn commit_timestamp_is_the_floor_of_commit_timestamp_ms() {
        let epoch = v2_epoch();
        let leader_ms = ms(1_700_000_030, 640);
        let fallback = CommittedSubDag::new_with_headers_for_test(vec![HeaderBuilder::default()
            .epoch(epoch)
            .created_at_ms(leader_ms)
            .build()]);
        assert_eq!(stored(&fallback), (0, 0), "test constructor stores the legacy default");
        assert_eq!(fallback.commit_timestamp_ms(), leader_ms, "zero fallback resolves the leader");
        assert_eq!(fallback.commit_timestamp(), leader_ms.secs(), "zero fallback in seconds");

        let committed = commit_at(epoch, leader_ms, None, None);
        let clamped = commit_at(epoch, ms(1_700_000_029, 0), Some(&committed), None);
        let near_zero = commit_at(epoch, ms(0, 0), None, Some(ms(0, 4)));
        assert_eq!(stored(&near_zero), (0, 5), "clamped into the first second");
        assert_eq!(near_zero.commit_timestamp_ms(), ms(0, 5), "a non-zero millis is not a default");
        for sub_dag in [&fallback, &committed, &clamped, &near_zero, &CommittedSubDag::default()] {
            assert_eq!(
                sub_dag.commit_timestamp_ms().secs(),
                sub_dag.commit_timestamp(),
                "seconds must floor the millisecond resolution"
            );
        }

        let output = ConsensusOutput::new_with_subdag(clamped.clone(), Default::default(), 1);
        assert_eq!(output.committed_at_ms(), clamped.commit_timestamp_ms());
        assert_eq!(output.committed_at(), clamped.commit_timestamp());
        assert_eq!(output.committed_at(), output.committed_at_ms().secs());
    }

    /// `reaches_epoch_boundary` agrees with `commit_timestamp() >= boundary` around a boundary,
    /// on the sub-dag and on the output: sub-second parts never move the decision.
    #[test]
    fn reaches_epoch_boundary_is_the_seconds_predicate() {
        let epoch = v2_epoch();
        let boundary = 1_700_000_100;
        for (commit_ms, expected) in [
            (ms(boundary - 1, 0), false),
            (ms(boundary - 1, 999), false),
            (ms(boundary, 0), true),
            (ms(boundary, 1), true),
            (ms(boundary, 999), true),
            (ms(boundary + 1, 0), true),
        ] {
            let sub_dag = commit_at(epoch, commit_ms, None, None);
            assert_eq!(sub_dag.commit_timestamp_ms(), commit_ms, "fixture must commit as given");
            assert_eq!(
                sub_dag.reaches_epoch_boundary(boundary),
                sub_dag.commit_timestamp() >= boundary,
                "predicate must be the seconds comparison at {commit_ms}"
            );
            assert_eq!(sub_dag.reaches_epoch_boundary(boundary), expected, "at {commit_ms}");
            let output = ConsensusOutput::new_with_subdag(sub_dag, Default::default(), 1);
            assert_eq!(output.reaches_epoch_boundary(boundary), expected, "output at {commit_ms}");
        }
    }

    /// Legacy shadow of the `CommittedSubDag` wire layout: the four historical fields with
    /// derived serde, byte-identical to the derive the hand-written impls replace.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct SubDagReprLegacy {
        headers: Vec<Header>,
        reputation_scores: ReputationScores,
        commit_timestamp: TimestampSec,
        randomness: B256,
    }

    /// Sub-second (V2) shadow of the `CommittedSubDag` wire layout: the legacy fields plus
    /// `commit_timestamp_millis`, with derived serde.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct SubDagReprV2 {
        headers: Vec<Header>,
        reputation_scores: ReputationScores,
        commit_timestamp: TimestampSec,
        randomness: B256,
        commit_timestamp_millis: u16,
    }

    /// Shadow of the `ConsensusHeader` wire layout over a V2 sub-dag shadow.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct ConsensusHeaderReprV2 {
        parent_hash: ConsensusHeaderDigest,
        sub_dag: SubDagReprV2,
        number: u64,
        extra: B256,
    }

    /// Shadow of the `ConsensusHeader` wire layout over a legacy sub-dag shadow: what a binary
    /// that predates the sub-second fork decodes.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct ConsensusHeaderReprLegacy {
        parent_hash: ConsensusHeaderDigest,
        sub_dag: SubDagReprLegacy,
        number: u64,
        extra: B256,
    }

    /// Independent restatement of the sub-dag digest preimage for a layout: header digests,
    /// reputation scores, seconds and randomness, then the sub-second part only when `v2`.
    fn restated_digest(sub_dag: &CommittedSubDag, v2: bool) -> ConsensusDigest {
        let mut hasher = DefaultHashFunction::new();
        sub_dag.headers().iter().for_each(|header| {
            hasher.update(header.digest().as_ref());
        });
        hasher.update(&encode(sub_dag.reputation_scores()));
        hasher.update(&encode(&sub_dag.inner.commit_timestamp));
        hasher.update(sub_dag.randomness().as_slice());
        if v2 {
            hasher.update(&encode(&sub_dag.inner.commit_timestamp_millis));
        }
        ConsensusDigest(Digest { digest: hasher.finalize().into() })
    }

    /// A post-fork sub-dag encodes as the V2 shadow, round-trips through both, and a
    /// four-field decoder (a binary that predates the fork) rejects its bytes, top-level as
    /// trailing input and nested inside a consensus header.
    #[test]
    fn v2_sub_dag_serde_round_trips() {
        let sub_dag = commit_at(v2_epoch(), ms(1_700_000_040, 437), None, None);
        let bytes = encode(&sub_dag);
        let repr: SubDagReprV2 = decode(&bytes);
        assert_eq!((repr.commit_timestamp, repr.commit_timestamp_millis), (1_700_000_040, 437));
        assert_eq!(encode(&repr), bytes, "V2 shadow bytes and sub-dag bytes diverged");
        let decoded: CommittedSubDag = decode(&bytes);
        assert_eq!(decoded, sub_dag, "V2 sub-dag did not round-trip");
        assert_eq!(decoded.digest(), sub_dag.digest(), "round trip moved the digest");
        assert!(
            matches!(try_decode::<SubDagReprLegacy>(&bytes), Err(bcs::Error::RemainingInput)),
            "a four-field decoder must reject V2 sub-dag bytes as trailing input"
        );

        let header = ConsensusHeader { sub_dag, number: 3, ..Default::default() };
        let header_bytes = encode(&header);
        assert_eq!(decode::<ConsensusHeader>(&header_bytes), header, "consensus header round trip");
        assert!(
            try_decode::<ConsensusHeaderReprLegacy>(&header_bytes).is_err(),
            "a pre-fork consensus header decoder must reject a V2 sub-dag"
        );
    }

    /// Decode rejects a sub-second part of 1000 or more, naming the field and the value, and
    /// accepts 999.
    #[test]
    fn decode_rejects_out_of_range_commit_millis() {
        let sub_dag = commit_at(v2_epoch(), ms(1_700_000_041, 0), None, None);
        let base: SubDagReprV2 = decode(&encode(&sub_dag));
        for millis in [1000, 1001, u16::MAX] {
            let bytes = encode(&SubDagReprV2 { commit_timestamp_millis: millis, ..base.clone() });
            let err = try_decode::<CommittedSubDag>(&bytes)
                .expect_err("commit_timestamp_millis of 1000 or more must not decode")
                .to_string();
            assert!(
                err.contains("commit_timestamp_millis") && err.contains(&format!("`{millis}`")),
                "decode error must name the field and the value {millis}, got: {err}"
            );
        }
        let bytes = encode(&SubDagReprV2 { commit_timestamp_millis: 999, ..base });
        let decoded: CommittedSubDag = decode(&bytes);
        assert_eq!(decoded.commit_timestamp_ms(), ms(1_700_000_041, 999), "999 must decode");
    }

    /// A sub-dag without headers has no leader and keeps the legacy layout and digest in every
    /// build; a fifth field there is left over as trailing input.
    #[test]
    fn empty_sub_dag_keeps_the_legacy_layout() {
        let sub_dag = CommittedSubDag::new_with_headers_for_test(Vec::new());
        let legacy = SubDagReprLegacy {
            headers: Vec::new(),
            reputation_scores: ReputationScores::default(),
            commit_timestamp: 0,
            randomness: EpochSeedChainValue::genesis_placeholder().into_inner(),
        };
        assert_eq!(encode(&sub_dag), encode(&legacy), "empty sub-dag must encode four fields");
        assert_eq!(decode::<CommittedSubDag>(&encode(&legacy)), sub_dag, "empty round trip");
        assert_eq!(sub_dag.digest(), restated_digest(&sub_dag, false), "legacy digest preimage");
        let v2 = SubDagReprV2 {
            headers: legacy.headers,
            reputation_scores: legacy.reputation_scores,
            commit_timestamp: legacy.commit_timestamp,
            randomness: legacy.randomness,
            commit_timestamp_millis: 0,
        };
        assert!(
            matches!(try_decode::<CommittedSubDag>(&encode(&v2)), Err(bcs::Error::RemainingInput)),
            "an empty sub-dag must not consume a commit_timestamp_millis field"
        );
    }

    /// Pre-fork (adiri), a sub-dag encodes as the legacy shadow and never reads a fifth field.
    #[cfg(feature = "adiri")]
    #[test]
    fn pre_fork_sub_dag_keeps_the_legacy_layout() {
        for epoch in pre_fork_epochs() {
            let sub_dag = commit_at(epoch, ms(1_700_000_050, 0), None, None);
            let bytes = encode(&sub_dag);
            let legacy: SubDagReprLegacy = decode(&bytes);
            assert_eq!(encode(&legacy), bytes, "epoch {epoch}: legacy shadow bytes diverged");
            assert_eq!(decode::<CommittedSubDag>(&bytes), sub_dag, "epoch {epoch}: round trip");
            let v2 = SubDagReprV2 {
                headers: legacy.headers,
                reputation_scores: legacy.reputation_scores,
                commit_timestamp: legacy.commit_timestamp,
                randomness: legacy.randomness,
                commit_timestamp_millis: 0,
            };
            assert!(
                matches!(
                    try_decode::<CommittedSubDag>(&encode(&v2)),
                    Err(bcs::Error::RemainingInput)
                ),
                "epoch {epoch}: a pre-fork sub-dag must not consume commit_timestamp_millis"
            );
        }
    }

    /// Post-fork, the digest covers the sub-second part: two sub-dags identical except for it
    /// have different digests, each equal to the independently restated V2 preimage.
    #[test]
    fn digest_covers_commit_millis_post_fork() {
        let epoch = v2_epoch();
        let leader_ms = ms(1_700_000_060, 0);
        let a = commit_at(epoch, leader_ms, None, Some(ms(1_700_000_060, 100)));
        let b = commit_at(epoch, leader_ms, None, Some(ms(1_700_000_060, 200)));
        assert_eq!(stored(&a), (1_700_000_060, 101));
        assert_eq!(stored(&b), (1_700_000_060, 201));
        assert_eq!(a.headers(), b.headers(), "fixtures must share their headers");
        assert_eq!(a.randomness(), b.randomness(), "fixtures must share their randomness");
        assert_ne!(a.digest(), b.digest(), "the digest must cover commit_timestamp_millis");
        assert_eq!(a.digest(), restated_digest(&a, true), "V2 digest preimage");
        assert_eq!(b.digest(), restated_digest(&b, true), "V2 digest preimage");
    }

    /// Pre-fork (adiri), the digest is exactly the legacy preimage.
    #[cfg(feature = "adiri")]
    #[test]
    fn digest_equals_the_legacy_preimage_pre_fork() {
        for epoch in pre_fork_epochs() {
            let sub_dag = commit_at(epoch, ms(1_700_000_070, 0), None, None);
            assert_eq!(sub_dag.digest(), restated_digest(&sub_dag, false), "epoch {epoch}");
        }
    }

    /// Epoch of the frozen V2 consensus header vector: `u32::MAX` is sub-second-active under
    /// BOTH cfgs (adiri's placeholder `SUBSECOND_TIMESTAMP_FORK_EPOCH` is `u32::MAX`, compared
    /// with `>=`; non-adiri everywhere), so one hex constant pins the bytes for every build.
    const GOLDEN_V2_EPOCH: Epoch = u32::MAX;

    /// Deterministic BLS signature from a seeded keypair (BLS signing is deterministic).
    fn seeded_signature(seed: u64, msg: &[u8]) -> BlsSignature {
        let keypair = crate::BlsKeypair::generate(&mut rand::rngs::StdRng::seed_from_u64(seed));
        crate::Signer::sign(&keypair, msg)
    }

    /// Fully deterministic V2 consensus header shadow over a leader at [`GOLDEN_V2_EPOCH`]
    /// with the largest valid sub-second part (999).
    fn golden_v2_repr() -> ConsensusHeaderReprV2 {
        let leader = HeaderBuilder::default()
            .author(AuthorityIdentifier::from_bytes([0x03; 32]))
            .round(6)
            .epoch(GOLDEN_V2_EPOCH)
            .created_at_ms(ms(1_700_000_077, 123))
            .payload(IndexMap::from([(BlockHash::repeat_byte(0x55), 4)]))
            .parents(BTreeSet::from([HeaderDigest::new([0xAC; 32])]))
            .latest_execution_block(BlockNumHash::new(17, BlockHash::repeat_byte(0xBD)))
            .seed_signature(seeded_signature(1032, b"golden-sub-dag-v2-seed"))
            .build();
        ConsensusHeaderReprV2 {
            parent_hash: B256::repeat_byte(0x1F).into(),
            sub_dag: SubDagReprV2 {
                headers: vec![leader],
                reputation_scores: ReputationScores {
                    scores_per_authority: BTreeMap::from([(
                        AuthorityIdentifier::from_bytes([0x07; 32]),
                        9,
                    )]),
                    final_of_schedule: true,
                },
                commit_timestamp: 1_700_000_078,
                randomness: B256::repeat_byte(0xCE),
                commit_timestamp_millis: 999,
            },
            number: 23,
            extra: B256::ZERO,
        }
    }

    /// FROZEN V2 consensus header vector: bcs wire bytes of [`golden_v2_repr`]. A change here is
    /// a consensus/storage wire-format change, NOT a constant to refresh.
    const GOLDEN_V2_CONSENSUS_HEADER_HEX: &str = "201f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f01030303030303030303030303030303030303030303030303030303030303030306000000ffffffff4df15365000000000120555555555555555555555555555555555555555555555555555555555555555504000120acacacacacacacacacacacacacacacacacacacacacacacacacacacacacacacac110000000000000020bdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbdbd3095ce4a09108c09d0d9dddeb4260c71d8460afe692070057e783b5718aa40c06af7d6fb1db275264b72b32cb7c3ae8d3b7b000107070707070707070707070707070707070707070707070707070707070707070900000000000000014ef153650000000020cecececececececececececececececececececececececececececececececee7031700000000000000200000000000000000000000000000000000000000000000000000000000000000";
    /// FROZEN digest of the consensus header decoded from [`GOLDEN_V2_CONSENSUS_HEADER_HEX`].
    /// Same warning as the wire pin.
    const GOLDEN_V2_CONSENSUS_HEADER_DIGEST_HEX: &str =
        "519eb2d4cfe4b7516cbf08b890a69be7a51d46f3b577047cca2346c5dd8545a2";

    /// PIN (all cfgs): the V2 consensus header wire bytes, from the shadow and through the
    /// hand-written sub-dag impls in both directions, and its digest, from the cache and from
    /// an independent restatement of the preimage.
    #[test]
    fn test_golden_v2_consensus_header_pinned() {
        let repr = golden_v2_repr();
        let bytes = encode(&repr);
        assert_eq!(
            hex::encode(&bytes),
            GOLDEN_V2_CONSENSUS_HEADER_HEX,
            "V2 consensus header shadow encode diverged from the frozen vector"
        );
        let header: ConsensusHeader = decode(&bytes);
        assert_eq!(
            hex::encode(encode(&header)),
            GOLDEN_V2_CONSENSUS_HEADER_HEX,
            "consensus header encode diverged from the frozen V2 vector"
        );
        assert_eq!(
            header.sub_dag.commit_timestamp_ms(),
            ms(1_700_000_078, 999),
            "decoded V2 vector lost its commit time"
        );
        assert_eq!(decode::<ConsensusHeaderReprV2>(&encode(&header)), repr, "shadow re-decode");
        assert_eq!(
            hex::encode(header.digest()),
            GOLDEN_V2_CONSENSUS_HEADER_DIGEST_HEX,
            "consensus header digest diverged from the frozen V2 digest"
        );
        let mut hasher = DefaultHashFunction::new();
        hasher.update(header.parent_hash.as_ref());
        hasher.update(restated_digest(&header.sub_dag, true).as_ref());
        hasher.update(&header.number.to_le_bytes());
        hasher.update(B256::ZERO.as_slice());
        assert_eq!(
            hex::encode(hasher.finalize().as_bytes()),
            GOLDEN_V2_CONSENSUS_HEADER_DIGEST_HEX,
            "restated V2 consensus header preimage diverged from the frozen digest"
        );
    }
}
