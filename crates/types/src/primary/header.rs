use crate::{
    crypto, encode,
    error::{HeaderError, HeaderResult},
    forks::{seed_signature_active, subsecond_timestamp_active},
    AuthorityIdentifier, Batch, BlockHash, BlockNumHash, BlsSignature, Committee, Digest, Epoch,
    Hash, Round, TimestampMs, TimestampSec, VoteDigest, WorkerId, MAX_HEADER_NUM_OF_BATCHES,
};
use indexmap::IndexMap;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, sync::Arc};

/// `Header` inner data type for consensus layer.
///
/// Deliberately carries no serde derives: every encode and decode path (network wire,
/// storage packs, and the digest preimage) routes through [`HeaderRef`] and the hand-written
/// [`Header`] impls, so the epoch-gated wire layouts for `seed_signature` (#1032, gated by
/// [`crate::forks::seed_signature_active`]) and `created_at_millis` (gated by
/// [`crate::forks::subsecond_timestamp_active`]) cannot be bypassed and the digest preimage is
/// byte-identical to the wire bytes by construction in every layout.
struct HeaderInner {
    /// Primary that created the header. Must be the same primary that broadcasted the header.
    author: AuthorityIdentifier,
    /// The round for this header
    round: Round,
    /// The epoch this Header was created in.
    epoch: Epoch,
    /// The timestamp for when the header was requested to be created, in whole seconds. The
    /// sub-second part lives in `created_at_millis`.
    created_at: TimestampSec,
    /// IndexMap of the [BatchDigest] to the [WorkerId]. Serialized in `serde_seq` shape (a
    /// length-prefixed sequence of pairs) via [`PayloadRef`]/[`PayloadOwned`]; `IndexMap`'s
    /// native map-shaped serde produces different bcs bytes.
    payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    parents: BTreeSet<HeaderDigest>,
    /// Hash and number of the latest known execution block when this Header was build.
    /// This may be our parent block or may not but it does include our latest
    /// execution result in a signed and validated structure which validates
    /// this execution block as well.
    latest_execution_block: BlockNumHash,
    /// The author's deterministic BLS signature over the canonical per-`(author, round)`
    /// [`EpochSeedMessage`](crate::EpochSeedMessage). The message binds this header's round, so
    /// the signature is constant for a given `(author, epoch, round)` and cannot exist before
    /// the author proposes at that round. Verified by voters before voting, and folded into the
    /// epoch seed chain ([`EpochSeedChainValue`](crate::EpochSeedChainValue)) when this header
    /// is a committing leader, which is what the epoch-close committee shuffle reads. Covered by
    /// the header digest (and so by votes and the certificate aggregate), which makes the
    /// shuffle seed unforkable.
    ///
    /// On the wire only for epochs where [`crate::forks::seed_signature_active`] holds;
    /// headers of earlier epochs do not carry the field and leave this at
    /// `BlsSignature::default()`, which [`Header::seed_signature`] surfaces as `None`.
    seed_signature: BlsSignature,
    /// The sub-second part of `created_at` in milliseconds, always in `0..=999`.
    ///
    /// On the wire, directly after `seed_signature`, only for epochs where
    /// [`crate::forks::subsecond_timestamp_active`] holds. Always 0 when the gate is inactive
    /// for `epoch`: constructors normalize it and decode never reads it, so a seconds-only
    /// header cannot carry an in-memory remainder that the wire and the digest would drop.
    created_at_millis: u16,
    /// The [HeaderDigest].
    /// This is cached to avoid calculating frequently (but not serialized).
    /// Note, this struct is private and this field MUST always be set on creation in this module.
    /// Failure to do so is undefined behaviour (not being set and not matching the struct is
    /// inexpressable outside of a bug in this module). Never serialized: [`HeaderRef`] does
    /// not write it and decode recomputes it.
    digest: HeaderDigest,
}

impl Default for HeaderInner {
    fn default() -> Self {
        // Override this so we can make sure to set the digest (avoid a future foot-gun).
        let mut inner = Self {
            author: Default::default(),
            round: Default::default(),
            epoch: Default::default(),
            created_at: Default::default(),
            payload: Default::default(),
            parents: Default::default(),
            latest_execution_block: Default::default(),
            seed_signature: Default::default(),
            created_at_millis: 0,
            digest: Default::default(),
        };

        let digest = Hash::digest(&inner);
        inner.digest = digest;
        inner
    }
}

/// `Header` type for consensus layer.
#[derive(Clone, Default)]
pub struct Header {
    inner: Arc<HeaderInner>,
}

impl Header {
    /// Initialize a new instance of [Header].
    ///
    /// `created_at` is split into the whole-second `created_at` field and the sub-second
    /// `created_at_millis` field. The sub-second part is kept only when
    /// [`subsecond_timestamp_active`] holds for `epoch`: earlier epochs have no millisecond
    /// field on the wire, so for them it is dropped and the header records only
    /// `created_at.secs()`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BlockHash, WorkerId>,
        parents: BTreeSet<HeaderDigest>,
        latest_execution_block: BlockNumHash,
        seed_signature: BlsSignature,
        created_at: TimestampMs,
    ) -> Self {
        let created_at_millis =
            if subsecond_timestamp_active(epoch) { created_at.subsec_millis() } else { 0 };
        let mut inner = HeaderInner {
            author,
            round,
            epoch,
            created_at: created_at.secs(),
            payload,
            parents,
            digest: HeaderDigest::default(),
            latest_execution_block,
            seed_signature,
            created_at_millis,
        };
        let digest = Hash::digest(&inner);
        inner.digest = digest;
        Self { inner: Arc::new(inner) }
    }

    /// Hashed digest for Header
    pub fn digest(&self) -> HeaderDigest {
        self.inner.digest
    }

    /// Ensure the header is valid based on the current committee and workercache.
    ///
    /// The digest is calculated with the sealed header, so the EL data is also verified.
    pub fn validate(&self, committee: &Committee) -> HeaderResult<()> {
        // Ensure the header is from the correct epoch.
        if self.inner.epoch != committee.epoch() {
            return Err(HeaderError::InvalidEpoch {
                theirs: self.inner.epoch,
                ours: committee.epoch(),
            });
        }

        // Ensure we don't have too many parents.
        if self.inner.parents.len() > committee.size() {
            return Err(HeaderError::TooManyParents(self.inner.parents.len(), committee.size()));
        }

        // Ensure the header does not reference more batches than the protocol permits.  The
        // proposer already caps its own headers at the configured
        // `max_header_num_of_batches` (validated to be <= MAX_HEADER_NUM_OF_BATCHES by
        // `Parameters::validate`); rejecting oversized inbound headers keeps the per-header
        // batch count a genuine consensus invariant, which bounds how many unique batches a
        // committed sub-DAG can reference and so keeps every committed output reconstructable
        // from pack storage.
        if self.inner.payload.len() > MAX_HEADER_NUM_OF_BATCHES {
            return Err(HeaderError::TooManyBatches(
                self.inner.payload.len(),
                MAX_HEADER_NUM_OF_BATCHES,
            ));
        }

        // every constructor and decode already keep this below 1000, so this only catches a bug
        // in this module; it sits here because the vote path validates every header it votes on
        if self.inner.created_at_millis >= 1000 {
            return Err(HeaderError::InvalidTimestampMillis(self.inner.created_at_millis));
        }

        // Note that self.digest() MUST be set correctly so no need to check.  We could add a panic
        // here but it is inexpressable outside of a bug in this module so no need to calc
        // the digest.  Use a debug assert just in case on debug builds.
        debug_assert_eq!(Hash::digest(&*self.inner), self.inner.digest);

        // Ensure authority is in the current committee.
        committee
            .authority(&self.inner.author)
            .ok_or(HeaderError::UnknownAuthority(self.inner.author.to_string()))?;

        // Ensure all worker ids are correct.
        for worker_id in self.inner.payload.values() {
            if usize::from(*worker_id) >= committee.number_of_workers() {
                return Err(HeaderError::UnkownWorkerId);
            }
        }

        Ok(())
    }

    /// The [AuthorityIdentifier] that produced the header.
    pub fn author(&self) -> &AuthorityIdentifier {
        &self.inner.author
    }
    /// The [Round] for the header.
    pub fn round(&self) -> Round {
        self.inner.round
    }
    /// The [Epoch] for the header.
    pub fn epoch(&self) -> Epoch {
        self.inner.epoch
    }
    /// The [TimestampSec] for the header: its creation time in whole seconds, rounded down.
    pub fn created_at(&self) -> &TimestampSec {
        &self.inner.created_at
    }
    /// The sub-second part of the header's creation time in milliseconds, always in `0..=999`.
    ///
    /// Always 0 for epochs where [`subsecond_timestamp_active`] does not hold.
    pub fn created_at_millis(&self) -> u16 {
        self.inner.created_at_millis
    }
    /// The header's creation time in milliseconds, combining [`Self::created_at`] and
    /// [`Self::created_at_millis`].
    ///
    /// For epochs where [`subsecond_timestamp_active`] does not hold this is the whole-second
    /// `created_at` times 1000.
    pub fn created_at_ms(&self) -> TimestampMs {
        TimestampMs::from_parts(self.inner.created_at, self.inner.created_at_millis)
    }
    /// The payload for the header.
    pub fn payload(&self) -> &IndexMap<BlockHash, WorkerId> {
        &self.inner.payload
    }
    /// The parents for the header.
    pub fn parents(&self) -> &BTreeSet<HeaderDigest> {
        &self.inner.parents
    }
    /// Return the latest executioin block for this header.
    pub fn latest_execution_block(&self) -> BlockNumHash {
        self.inner.latest_execution_block
    }

    /// The author's deterministic BLS signature over the canonical per-`(author, round)`
    /// [`EpochSeedMessage`](crate::EpochSeedMessage) - this header's contribution to the epoch
    /// seed chain that seeds the epoch-close committee shuffle.
    ///
    /// `None` for headers of epochs where the seed-signature fork
    /// ([`crate::forks::seed_signature_active`]) is not active: their wire format does not
    /// carry the field, so the legacy case is type-visible instead of a silently defaulted
    /// signature.
    pub fn seed_signature(&self) -> Option<&BlsSignature> {
        seed_signature_active(self.inner.epoch).then_some(&self.inner.seed_signature)
    }

    /// The nonce of this header used during execution.
    pub fn nonce(&self) -> u64 {
        ((self.inner.epoch as u64) << 32) | self.inner.round as u64
    }
}

/// Number of `HeaderInner` wire fields on the legacy (pre-`seed_signature`) layout.
const HEADER_FIELDS_LEGACY: usize = 7;
/// Number of `HeaderInner` wire fields once `seed_signature` is active for the header's
/// epoch.
const HEADER_FIELDS_V1: usize = 8;
/// Number of `HeaderInner` wire fields once `created_at_millis` is also active for the
/// header's epoch.
const HEADER_FIELDS_V2: usize = 9;
/// Field names for [`serde::Deserializer::deserialize_struct`], superset (latest) layout:
/// every earlier layout is a prefix of it.
const HEADER_FIELD_NAMES: [&str; HEADER_FIELDS_V2] = [
    "author",
    "round",
    "epoch",
    "created_at",
    "payload",
    "parents",
    "latest_execution_block",
    "seed_signature",
    "created_at_millis",
];

/// Borrowed serialization view over [`HeaderInner`]: the ONE definition of header wire
/// bytes.
///
/// `HeaderInner` has no serde derives, so every encode path (network wire, storage packs,
/// and the digest preimage in the [`Hash`] impl) is forced through this view. The
/// `seed_signature` field is written only when [`seed_signature_active`] holds, and the
/// `created_at_millis` field after it only when [`subsecond_timestamp_active`] holds, each
/// for the header's own `epoch` — never node-local state — which keeps mixed-epoch
/// containers (certificate vectors, sub-DAGs, pack records) correct at any nesting depth and
/// makes the digest preimage byte-identical to the wire bytes by construction in every
/// layout.
struct HeaderRef<'a>(&'a HeaderInner);

impl Serialize for HeaderRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let inner = self.0;
        let seed_active = seed_signature_active(inner.epoch);
        // implies `seed_active` (the gate conjoins it), so the layouts chain 7 -> 8 -> 9 fields
        let millis_active = subsecond_timestamp_active(inner.epoch);
        let fields = if millis_active {
            HEADER_FIELDS_V2
        } else if seed_active {
            HEADER_FIELDS_V1
        } else {
            HEADER_FIELDS_LEGACY
        };
        // serialize_struct mirrors the removed derive exactly: bcs emits no framing for
        // structs, so the wire bytes are the concatenated fields in declaration order.
        let mut state = serializer.serialize_struct("HeaderInner", fields)?;
        state.serialize_field("author", &inner.author)?;
        state.serialize_field("round", &inner.round)?;
        state.serialize_field("epoch", &inner.epoch)?;
        state.serialize_field("created_at", &inner.created_at)?;
        state.serialize_field("payload", &PayloadRef(&inner.payload))?;
        state.serialize_field("parents", &inner.parents)?;
        state.serialize_field("latest_execution_block", &inner.latest_execution_block)?;
        if seed_active {
            state.serialize_field("seed_signature", &inner.seed_signature)?;
        }
        if millis_active {
            state.serialize_field("created_at_millis", &inner.created_at_millis)?;
        }
        state.end()
    }
}

/// Zero-copy wrapper preserving the payload's `serde_seq` wire shape (a length-prefixed
/// sequence of `(BlockHash, WorkerId)` pairs) — exactly what the removed
/// `#[serde(with = "indexmap::map::serde_seq")]` derive attribute produced. `IndexMap`'s
/// native map-shaped `Serialize` yields different bcs bytes; do not "simplify" to it.
struct PayloadRef<'a>(&'a IndexMap<BlockHash, WorkerId>);

impl Serialize for PayloadRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        indexmap::map::serde_seq::serialize(self.0, serializer)
    }
}

/// Owned counterpart of [`PayloadRef`] for the decode path.
struct PayloadOwned(IndexMap<BlockHash, WorkerId>);

impl<'de> Deserialize<'de> for PayloadOwned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        indexmap::map::serde_seq::deserialize(deserializer).map(PayloadOwned)
    }
}

/// Extracts the next element of the header field sequence, converting an early end of input
/// into a field-labeled error (bcs would otherwise surface only a distal `Eof`).
fn next_header_field<'de, A, T>(seq: &mut A, field: &'static str) -> Result<T, A::Error>
where
    A: serde::de::SeqAccess<'de>,
    T: Deserialize<'de>,
{
    seq.next_element()?.ok_or_else(|| serde::de::Error::missing_field(field))
}

impl Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        HeaderRef(&self.inner).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        /// Reads the seven legacy fields, then `seed_signature` only when the just-decoded
        /// `epoch` has the seed-signature fork active (legacy headers fill
        /// `BlsSignature::default()`), then `created_at_millis` only when the sub-second
        /// timestamp fork is also active (earlier headers fill 0).
        struct HeaderVisitor;

        impl<'de> serde::de::Visitor<'de> for HeaderVisitor {
            type Value = Header;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a Header: seven legacy fields, plus seed_signature and then \
                     created_at_millis when their forks are active for the header's epoch",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let author = next_header_field(&mut seq, "author")?;
                let round = next_header_field(&mut seq, "round")?;
                let epoch: Epoch = next_header_field(&mut seq, "epoch")?;
                let created_at = next_header_field(&mut seq, "created_at")?;
                let PayloadOwned(payload) = next_header_field(&mut seq, "payload")?;
                let parents = next_header_field(&mut seq, "parents")?;
                let latest_execution_block = next_header_field(&mut seq, "latest_execution_block")?;
                let seed_signature = if seed_signature_active(epoch) {
                    next_header_field(&mut seq, "seed_signature")?
                } else {
                    BlsSignature::default()
                };
                // not read at all when inactive, so an extra trailing field on an earlier
                // epoch's header stays unconsumed and fails decode exactly as before the fork
                let created_at_millis = if subsecond_timestamp_active(epoch) {
                    let millis: u16 = next_header_field(&mut seq, "created_at_millis")?;
                    if millis >= 1000 {
                        return Err(serde::de::Error::invalid_value(
                            serde::de::Unexpected::Unsigned(u64::from(millis)),
                            &"created_at_millis below 1000",
                        ));
                    }
                    millis
                } else {
                    0
                };
                let mut inner = HeaderInner {
                    author,
                    round,
                    epoch,
                    created_at,
                    payload,
                    parents,
                    latest_execution_block,
                    seed_signature,
                    created_at_millis,
                    digest: HeaderDigest::default(),
                };
                inner.digest = Hash::digest(&inner);
                Ok(Header { inner: Arc::new(inner) })
            }
        }

        deserializer.deserialize_struct("HeaderInner", &HEADER_FIELD_NAMES, HeaderVisitor)
    }
}

/// Builder for `Header` data type for consensus layer.
#[derive(Default, Debug)]
pub struct HeaderBuilder {
    /// Primary that created the header. Must be the same primary that broadcasted the header.
    author: AuthorityIdentifier,
    /// The round for this header
    round: Round,
    /// The epoch this Header was created in.
    epoch: Epoch,
    /// The timestamp for when the header was requested to be created, in whole seconds.
    created_at: TimestampSec,
    /// The sub-second part of `created_at` in milliseconds. Kept by [`Self::build`] only when
    /// [`subsecond_timestamp_active`] holds for the builder's epoch.
    created_at_millis: u16,
    /// IndexMap of the [BatchDigest] to the [WorkerId]
    payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    parents: BTreeSet<HeaderDigest>,
    /// Hash and number of the latest known execution block when this Header was build.
    latest_execution_block: BlockNumHash,
    /// The author's deterministic BLS signature over the canonical per-`(author, round)`
    /// [`EpochSeedMessage`](crate::EpochSeedMessage).
    seed_signature: BlsSignature,
}

impl HeaderBuilder {
    /// Build a new builder using the values from header as the defaults.
    pub fn from_header(header: &Header) -> Self {
        Self {
            author: header.inner.author.clone(),
            round: header.inner.round,
            epoch: header.inner.epoch,
            created_at: header.inner.created_at,
            created_at_millis: header.inner.created_at_millis,
            payload: header.inner.payload.clone(),
            parents: header.inner.parents.clone(),
            latest_execution_block: header.inner.latest_execution_block,
            seed_signature: header.inner.seed_signature,
        }
    }

    /// "Build" the header by taking all fields and calculating the hash.
    /// This is used for tests, if used for "real" code then at least latest_execution_block will
    /// need to be visited.
    ///
    /// The sub-second part of `created_at` is normalized to 0 when
    /// [`subsecond_timestamp_active`] does not hold for the builder's epoch, matching
    /// [`Header::new`].
    pub fn build(self) -> Header {
        // gated here rather than in the setters because the epoch may be set after the
        // timestamp
        let created_at_millis =
            if subsecond_timestamp_active(self.epoch) { self.created_at_millis } else { 0 };
        let mut inner = HeaderInner {
            author: self.author,
            round: self.round,
            epoch: self.epoch,
            created_at: self.created_at,
            payload: self.payload,
            parents: self.parents,
            digest: HeaderDigest::default(),
            latest_execution_block: self.latest_execution_block,
            seed_signature: self.seed_signature,
            created_at_millis,
        };

        inner.digest = Hash::digest(&inner);

        Header { inner: Arc::new(inner) }
    }

    /// Set the author on the builder.
    pub fn author(mut self, author: AuthorityIdentifier) -> Self {
        self.author = author;
        self
    }
    /// Set the round on the builder.
    pub fn round(mut self, round: Round) -> Self {
        self.round = round;
        self
    }
    /// Set the epoch on the builder.
    pub fn epoch(mut self, epoch: Epoch) -> Self {
        self.epoch = epoch;
        self
    }
    /// Set the created_at on the builder, in whole seconds.
    ///
    /// Resets the sub-second part to 0. Between this and [`Self::created_at_ms`], the call
    /// made last wins.
    pub fn created_at(mut self, created_at: TimestampSec) -> Self {
        self.created_at = created_at;
        self.created_at_millis = 0;
        self
    }
    /// Set the creation time on the builder in milliseconds, covering both the whole-second
    /// `created_at` and its sub-second part.
    ///
    /// [`Self::build`] keeps the sub-second part only when [`subsecond_timestamp_active`] holds
    /// for the builder's epoch. Between this and [`Self::created_at`], the call made last wins.
    pub fn created_at_ms(mut self, created_at: TimestampMs) -> Self {
        self.created_at = created_at.secs();
        self.created_at_millis = created_at.subsec_millis();
        self
    }
    /// Set the payload on the builder.
    pub fn payload(mut self, payload: IndexMap<BlockHash, WorkerId>) -> Self {
        self.payload = payload;
        self
    }
    /// Set the parents on the builder.
    pub fn parents(mut self, parents: BTreeSet<HeaderDigest>) -> Self {
        self.parents = parents;
        self
    }
    /// Set the latest_execution_block on the builder.
    pub fn latest_execution_block(mut self, latest_execution_block: BlockNumHash) -> Self {
        self.latest_execution_block = latest_execution_block;
        self
    }
    /// Set the epoch-close seed signature on the builder.
    pub fn seed_signature(mut self, seed_signature: BlsSignature) -> Self {
        self.seed_signature = seed_signature;
        self
    }
    /// Helper method to directly set values of the payload
    pub fn with_payload_batch(mut self, batch: &Batch, worker_id: WorkerId) -> Self {
        self.payload.insert(batch.digest(), worker_id);
        self
    }
}

/// The slice of bytes for the header's digest.
#[derive(
    Clone, Copy, Default, PartialEq, Eq, std::hash::Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct HeaderDigest(Digest<{ crypto::DIGEST_LENGTH }>);

impl HeaderDigest {
    /// Create a new HeaderDigest based on the crate's `DIGEST_LENGTH` constant.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        HeaderDigest(Digest { digest })
    }
}

impl From<HeaderDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: HeaderDigest) -> Self {
        hd.0
    }
}

impl From<HeaderDigest> for [u8; crypto::DIGEST_LENGTH] {
    fn from(hd: HeaderDigest) -> Self {
        hd.0.digest
    }
}

impl AsRef<[u8]> for HeaderDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0.digest
    }
}

impl From<HeaderDigest> for VoteDigest {
    fn from(value: HeaderDigest) -> Self {
        Self::new(value.0.into())
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for HeaderInner {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        // The preimage is the wire bytes by construction: the same `HeaderRef` view that
        // serializes the header (epoch-gated `seed_signature` and `created_at_millis`
        // included) feeds the hash, in every layout.
        hasher.update(encode(&HeaderRef(self)).as_ref());
        HeaderDigest(Digest { digest: hasher.finalize().into() })
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}(v{}, e{}, {}wbs, exec: {:?})",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len(),
            self.latest_execution_block(),
        )
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "B{}({})", self.round(), self.author())
    }
}

impl PartialEq for Header {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use alloy::{eips::BlockNumHash, primitives::BlockHash};
    use indexmap::IndexMap;
    use rand::SeedableRng as _;
    use serde::{Deserialize, Serialize};

    use super::HeaderBuilder;
    #[cfg(feature = "adiri")]
    use crate::forks::SEED_SIGNATURE_FORK_EPOCH;
    use crate::{
        decode, encode, try_decode, AuthorityIdentifier, BlsSignature, DefaultHashFunction, Epoch,
        Header, HeaderDigest, Round, TimestampMs, TimestampSec, WorkerId,
    };

    /// Seed-signature (V1) shadow of the `Header` wire layout: all eight fields with derived
    /// serde, mirroring what a plain derive on `HeaderInner` emits once `seed_signature` is
    /// active for the epoch (#1032) and while `created_at_millis` is not. Keeps the
    /// hand-written `HeaderRef`/visitor honest and makes sure serde ignores the inner Arc.
    #[derive(Clone, Deserialize, Serialize, Default, Debug, PartialEq)]
    struct HeaderReprV1 {
        /// Primary that created the header. Must be the same primary that broadcasted the header.
        pub author: AuthorityIdentifier,
        /// The round for this header
        pub round: Round,
        /// The epoch this Header was created in.
        pub epoch: Epoch,
        /// The timestamp for when the header was requested to be created.
        pub created_at: TimestampSec,
        /// IndexMap of the [BatchDigest] to the [WorkerId]
        #[serde(with = "indexmap::map::serde_seq")]
        pub payload: IndexMap<BlockHash, WorkerId>,
        /// Parent certificates for this Header.
        pub parents: BTreeSet<HeaderDigest>,
        /// Hash and number of the latest known execution block when this Header was build.
        pub latest_execution_block: BlockNumHash,
        /// The author's deterministic BLS signature over the canonical round-bound seed message.
        /// Mirrors `HeaderInner::seed_signature` - a DELIBERATE wire-format change (#1032).
        pub seed_signature: BlsSignature,
    }

    /// Sub-second (V2) shadow of the `Header` wire layout: all nine fields with derived serde,
    /// mirroring what a plain derive on `HeaderInner` emits once `created_at_millis` is active
    /// for the epoch. Keeps the hand-written `HeaderRef`/visitor honest for the V2 layout.
    #[derive(Clone, Deserialize, Serialize, Default, Debug, PartialEq)]
    struct HeaderReprV2 {
        /// Primary that created the header. Must be the same primary that broadcasted the header.
        pub author: AuthorityIdentifier,
        /// The round for this header
        pub round: Round,
        /// The epoch this Header was created in.
        pub epoch: Epoch,
        /// The timestamp for when the header was requested to be created, in whole seconds.
        pub created_at: TimestampSec,
        /// IndexMap of the [BatchDigest] to the [WorkerId]
        #[serde(with = "indexmap::map::serde_seq")]
        pub payload: IndexMap<BlockHash, WorkerId>,
        /// Parent certificates for this Header.
        pub parents: BTreeSet<HeaderDigest>,
        /// Hash and number of the latest known execution block when this Header was build.
        pub latest_execution_block: BlockNumHash,
        /// The author's deterministic BLS signature over the canonical round-bound seed message.
        pub seed_signature: BlsSignature,
        /// The sub-second part of `created_at` in milliseconds. Mirrors
        /// `HeaderInner::created_at_millis` - a DELIBERATE wire-format change.
        pub created_at_millis: u16,
    }

    /// Pre-fork legacy shadow of the `Header` wire layout: the seven historical fields with
    /// derived serde — byte-identical to the `origin/main` `HeaderInner` derive output BY
    /// CONSTRUCTION (same field types, order, and serde attrs; no `seed_signature`). Anchors
    /// the legacy golden vector independently of the hand-written epoch gate.
    #[derive(Clone, Deserialize, Serialize, Default, Debug, PartialEq)]
    struct HeaderReprLegacy {
        /// Primary that created the header. Must be the same primary that broadcasted the header.
        pub author: AuthorityIdentifier,
        /// The round for this header
        pub round: Round,
        /// The epoch this Header was created in.
        pub epoch: Epoch,
        /// The timestamp for when the header was requested to be created.
        pub created_at: TimestampSec,
        /// IndexMap of the [BatchDigest] to the [WorkerId]
        #[serde(with = "indexmap::map::serde_seq")]
        pub payload: IndexMap<BlockHash, WorkerId>,
        /// Parent certificates for this Header.
        pub parents: BTreeSet<HeaderDigest>,
        /// Hash and number of the latest known execution block when this Header was build.
        pub latest_execution_block: BlockNumHash,
    }

    /// 32-byte fixture array with a controlled leading byte: `first` at index 0 (the
    /// 0x00/0x01 leading-byte misalignment trap right at the start of the wire) and `fill`
    /// everywhere else, so a field-order or length-prefix bug shifts bytes visibly.
    fn bytes32(first: u8, fill: u8) -> [u8; 32] {
        std::array::from_fn(|ix| if ix == 0 { first } else { fill })
    }

    /// Deterministic BLS signature: keypair from the seeded `StdRng` pattern of the original
    /// serde test, signing `msg`. BLS signing is deterministic, so the bytes are stable for
    /// the golden vectors (as stable as the locked `rand`/`blst` versions).
    fn seeded_signature(seed: u64, msg: &[u8]) -> BlsSignature {
        let keypair = crate::BlsKeypair::generate(&mut rand::rngs::StdRng::seed_from_u64(seed));
        crate::Signer::sign(&keypair, msg)
    }

    /// An epoch with the V2 (nine-field) wire layout under the running cfg: `u32::MAX` under
    /// `adiri` (the placeholder `SUBSECOND_TIMESTAMP_FORK_EPOCH` itself, compared with `>=`),
    /// epoch zero elsewhere (non-adiri is V2 from genesis).
    fn v2_epoch() -> Epoch {
        if cfg!(feature = "adiri") {
            u32::MAX
        } else {
            0
        }
    }

    /// Blake3 over raw wire bytes via the exact hasher usage of the `Hash` impl on
    /// `HeaderInner`: an independent restatement of the digest-preimage == wire-bytes
    /// contract.
    fn wire_digest(bytes: &[u8]) -> HeaderDigest {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(bytes);
        HeaderDigest::new(hasher.finalize().into())
    }

    /// Decode a test hex constant, failing loudly on a malformed constant.
    fn unhex(hex_str: &str) -> Vec<u8> {
        hex::decode(hex_str).expect("test hex constant must be valid hex")
    }

    /// Build a real [`Header`] carrying exactly the fields of a V1 shadow; adiri-only because
    /// only adiri builds have epochs that give a `Header` the V1 layout.
    #[cfg(feature = "adiri")]
    fn header_from_v1_repr(repr: HeaderReprV1) -> Header {
        HeaderBuilder::default()
            .author(repr.author)
            .round(repr.round)
            .epoch(repr.epoch)
            .created_at(repr.created_at)
            .payload(repr.payload)
            .parents(repr.parents)
            .latest_execution_block(repr.latest_execution_block)
            .seed_signature(repr.seed_signature)
            .build()
    }

    /// Build a real [`Header`] carrying exactly the fields of a V2 shadow.
    fn header_from_v2_repr(repr: HeaderReprV2) -> Header {
        HeaderBuilder::default()
            .author(repr.author)
            .round(repr.round)
            .epoch(repr.epoch)
            .created_at_ms(TimestampMs::from_parts(repr.created_at, repr.created_at_millis))
            .payload(repr.payload)
            .parents(repr.parents)
            .latest_execution_block(repr.latest_execution_block)
            .seed_signature(repr.seed_signature)
            .build()
    }

    /// Build a real [`Header`] carrying the fields of a legacy shadow plus an in-memory
    /// `seed_signature` that MUST NOT reach the wire (the epoch gate strips it pre-fork).
    #[cfg(feature = "adiri")]
    fn header_from_legacy_repr(repr: HeaderReprLegacy, inert_signature: BlsSignature) -> Header {
        HeaderBuilder::default()
            .author(repr.author)
            .round(repr.round)
            .epoch(repr.epoch)
            .created_at(repr.created_at)
            .payload(repr.payload)
            .parents(repr.parents)
            .latest_execution_block(repr.latest_execution_block)
            .seed_signature(inert_signature)
            .build()
    }

    /// A [`Header::new`] with empty contents at `epoch`, created at `created_at`.
    fn header_new_at(epoch: Epoch, created_at: TimestampMs) -> Header {
        Header::new(
            AuthorityIdentifier::default(),
            1,
            epoch,
            IndexMap::new(),
            BTreeSet::new(),
            BlockNumHash::default(),
            BlsSignature::default(),
            created_at,
        )
    }

    /// Epoch of the frozen shadow-only V1 golden vector. `u32::MAX` is seed-signature-active
    /// under BOTH cfgs (`adiri` activates at the concrete `SEED_SIGNATURE_FORK_EPOCH` (383),
    /// far below `u32::MAX`; non-adiri everywhere), so one hex constant pins the V1 wire bytes
    /// for every build.
    ///
    /// Trap: `u32::MAX` is also sub-second-active on BOTH builds (adiri's placeholder
    /// `SUBSECOND_TIMESTAMP_FORK_EPOCH` is `u32::MAX`, compared with `>=`), so a `Header` at this
    /// epoch encodes the V2 layout. This vector is anchored against [`HeaderReprV1`] only, never
    /// against `Header`; the `Header` side of the V1 layout is pinned at
    /// [`GOLDEN_V1_HEADER_EPOCH`] (adiri).
    const GOLDEN_V1_EPOCH: Epoch = u32::MAX;

    /// Epoch of the frozen adiri V1 `Header` vector: `SEED_SIGNATURE_FORK_EPOCH` (383), the
    /// first seed-signature epoch. The sub-second fork can only be armed above 383 (the live
    /// adiri chain is already past it), so a `Header` here keeps the eight-field layout on
    /// every adiri build, armed or not.
    #[cfg(feature = "adiri")]
    const GOLDEN_V1_HEADER_EPOCH: Epoch = SEED_SIGNATURE_FORK_EPOCH;

    /// Epoch of the frozen V2 golden vector: `u32::MAX` is sub-second-active under BOTH cfgs
    /// (adiri's placeholder `SUBSECOND_TIMESTAMP_FORK_EPOCH` is `u32::MAX`, compared with `>=`;
    /// non-adiri everywhere), so one hex constant pins the V2 wire bytes for every build.
    const GOLDEN_V2_EPOCH: Epoch = u32::MAX;

    /// Fully deterministic legacy-layout golden fixture (epoch 0: pre-fork under `adiri`).
    /// The author's leading byte is 0x00 (leading-byte trap, paired with the V1 fixture's
    /// 0x01).
    fn golden_legacy_repr() -> HeaderReprLegacy {
        HeaderReprLegacy {
            author: AuthorityIdentifier::from_bytes(bytes32(0x00, 0xB7)),
            round: 3,
            epoch: 0,
            created_at: 1_700_000_017,
            payload: IndexMap::from([(BlockHash::repeat_byte(0x22), 1)]),
            parents: BTreeSet::from([HeaderDigest::new([0x4D; 32])]),
            latest_execution_block: BlockNumHash::new(7, BlockHash::repeat_byte(0x5E)),
        }
    }

    /// Fully deterministic V1-layout golden fixture (epoch [`GOLDEN_V1_EPOCH`]). The
    /// author's leading byte is 0x01 (paired with the legacy fixture's 0x00).
    fn golden_v1_repr() -> HeaderReprV1 {
        HeaderReprV1 {
            author: AuthorityIdentifier::from_bytes(bytes32(0x01, 0xC3)),
            round: 4,
            epoch: GOLDEN_V1_EPOCH,
            created_at: 1_700_000_042,
            payload: IndexMap::from([(BlockHash::repeat_byte(0x33), 2)]),
            parents: BTreeSet::from([HeaderDigest::new([0x6A; 32])]),
            latest_execution_block: BlockNumHash::new(11, BlockHash::repeat_byte(0x7F)),
            seed_signature: seeded_signature(1032, b"golden-v1-seed"),
        }
    }

    /// [`golden_v1_repr`] moved to [`GOLDEN_V1_HEADER_EPOCH`], where a `Header` keeps the V1
    /// layout.
    #[cfg(feature = "adiri")]
    fn golden_v1_header_repr() -> HeaderReprV1 {
        HeaderReprV1 { epoch: GOLDEN_V1_HEADER_EPOCH, ..golden_v1_repr() }
    }

    /// [`golden_v1_header_repr`] built as a real [`Header`].
    #[cfg(feature = "adiri")]
    fn golden_v1_header() -> Header {
        header_from_v1_repr(golden_v1_header_repr())
    }

    /// Fully deterministic V2-layout golden fixture (epoch [`GOLDEN_V2_EPOCH`]) with the
    /// largest valid `created_at_millis` (999). The author's leading byte is 0x02 (continuing
    /// the legacy 0x00 / V1 0x01 pairing).
    fn golden_v2_repr() -> HeaderReprV2 {
        HeaderReprV2 {
            author: AuthorityIdentifier::from_bytes(bytes32(0x02, 0xD5)),
            round: 5,
            epoch: GOLDEN_V2_EPOCH,
            created_at: 1_700_000_063,
            payload: IndexMap::from([(BlockHash::repeat_byte(0x44), 3)]),
            parents: BTreeSet::from([HeaderDigest::new([0x8B; 32])]),
            latest_execution_block: BlockNumHash::new(13, BlockHash::repeat_byte(0x9C)),
            seed_signature: seeded_signature(1032, b"golden-v2-seed"),
            created_at_millis: 999,
        }
    }

    /// [`golden_v2_repr`] built as a real [`Header`].
    fn golden_v2_header() -> Header {
        header_from_v2_repr(golden_v2_repr())
    }

    /// [`golden_legacy_repr`] built as a real [`Header`]; adiri-only because only adiri
    /// builds give epoch 0 the legacy layout. Carries a REAL in-memory signature that the
    /// pre-fork wire (and therefore the digest preimage) must strip.
    #[cfg(feature = "adiri")]
    fn golden_legacy_header() -> Header {
        header_from_legacy_repr(golden_legacy_repr(), seeded_signature(1032, b"legacy-inert"))
    }

    /// FROZEN legacy golden vector: bcs wire bytes of [`golden_legacy_repr`], wire-identical
    /// to the `origin/main` derive output by construction of [`HeaderReprLegacy`]. If this
    /// pin breaks, the pre-fork wire format changed — a consensus/storage compatibility
    /// break, NOT a constant to refresh.
    const GOLDEN_LEGACY_HEADER_HEX: &str = "00b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7b7030000000000000011f153650000000001202222222222222222222222222222222222222222222222222222222222222222010001204d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d0700000000000000205e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";
    /// FROZEN blake3 digest of [`GOLDEN_LEGACY_HEADER_HEX`] (the digest preimage IS the wire
    /// bytes). Same warning as the wire pin.
    const GOLDEN_LEGACY_DIGEST_HEX: &str =
        "01b3ed3e245491fe3787c2cd844058459a062e290fa8fe1b56e79590ebb03c6e";
    /// FROZEN V1 golden vector (epoch [`GOLDEN_V1_EPOCH`], eight fields including the seeded
    /// deterministic `seed_signature`), anchored against the [`HeaderReprV1`] shadow only;
    /// identical under adiri and non-adiri cfgs. Same warning as the legacy wire pin.
    const GOLDEN_V1_HEADER_HEX: &str = "01c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c304000000ffffffff2af153650000000001203333333333333333333333333333333333333333333333333333333333333333020001206a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a0b00000000000000207f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f308b08cee4e14c2769831816cca5a767ef8a10d56da66bdd887601198d6bef7c6cae6b3155b84eac90b75912a22872b5f0";
    /// FROZEN blake3 digest of [`GOLDEN_V1_HEADER_HEX`]. Same warning as the wire pin.
    const GOLDEN_V1_DIGEST_HEX: &str =
        "a79a9d9d6bcc358587a20531bf4f3e5c232c3631d5276428cecda1ba41433db8";
    /// FROZEN adiri V1 `Header` vector: bcs wire bytes of [`golden_v1_header`] (epoch
    /// [`GOLDEN_V1_HEADER_EPOCH`]), the epoch-gated `Header` itself in the eight-field layout.
    /// Same warning as the legacy wire pin.
    #[cfg(feature = "adiri")]
    const GOLDEN_V1_HEADER_AT_FORK_HEX: &str = "01c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3040000007f0100002af153650000000001203333333333333333333333333333333333333333333333333333333333333333020001206a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a0b00000000000000207f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f308b08cee4e14c2769831816cca5a767ef8a10d56da66bdd887601198d6bef7c6cae6b3155b84eac90b75912a22872b5f0";
    /// FROZEN blake3 digest of [`GOLDEN_V1_HEADER_AT_FORK_HEX`]. Same warning as the wire pin.
    #[cfg(feature = "adiri")]
    const GOLDEN_V1_DIGEST_AT_FORK_HEX: &str =
        "defb0442a23f370e6504e75a47acce7c4b02e047e9320ec2227c5d54f6237b01";
    /// FROZEN V2 golden vector (epoch [`GOLDEN_V2_EPOCH`], nine fields ending in
    /// `created_at_millis` = 999); identical under adiri and non-adiri cfgs. A change here is a
    /// wire-format change. Same warning as the legacy wire pin.
    const GOLDEN_V2_HEADER_HEX: &str = "02d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d505000000ffffffff3ff153650000000001204444444444444444444444444444444444444444444444444444444444444444030001208b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b8b0d00000000000000209c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c9c3082484e0511729a0a44f2b05fb5474d89ade47042d8147e4fd7685bc22da11d9875d8b88591734569e5d4ee404f6c124ee703";
    /// FROZEN blake3 digest of [`GOLDEN_V2_HEADER_HEX`]. Same warning as the wire pin.
    const GOLDEN_V2_DIGEST_HEX: &str =
        "2e9d838de3571a207fa7f7c6208284e91e725507d1fc31e9e29394a7c91cc288";

    /// V1 layout (adiri: the seed-signature fork epoch stays V1 below the sub-second fork):
    /// shadow encode == Header encode, both decode directions, and the builder path lands on
    /// identical bytes. Successor of the original `test_header_serde` (the Arc-transparency
    /// check included).
    #[cfg(feature = "adiri")]
    #[test]
    fn test_header_serde_v1_layout() {
        let repr = golden_v1_header_repr();
        let enc_repr = encode(&repr);
        let header: Header = decode(&enc_repr);
        assert_eq!(
            Some(&repr.seed_signature),
            header.seed_signature(),
            "seed signature mismatch after decode"
        );
        assert_eq!(header.created_at_millis(), 0, "V1 header must carry no sub-second part");
        assert_eq!(enc_repr, encode(&header), "V1 shadow bytes and Header bytes diverged");
        let repr2: HeaderReprV1 = decode(&encode(&header));
        assert_eq!(repr, repr2, "Header mismatch");
        assert_eq!(
            header,
            header_from_v1_repr(repr),
            "builder-constructed header diverged from decoded header"
        );
    }

    /// V2 layout: shadow encode == Header encode, both decode directions, and the builder
    /// path lands on identical bytes. Epoch picked per cfg by [`v2_epoch`].
    #[test]
    fn test_header_serde_v2_layout() {
        let repr = HeaderReprV2 { epoch: v2_epoch(), ..golden_v2_repr() };
        let enc_repr = encode(&repr);
        let header: Header = decode(&enc_repr);
        assert_eq!(
            Some(&repr.seed_signature),
            header.seed_signature(),
            "seed signature mismatch after decode"
        );
        assert_eq!(
            repr.created_at_millis,
            header.created_at_millis(),
            "sub-second part mismatch after decode"
        );
        assert_eq!(enc_repr, encode(&header), "V2 shadow bytes and Header bytes diverged");
        let repr2: HeaderReprV2 = decode(&encode(&header));
        assert_eq!(repr, repr2, "Header mismatch");
        assert_eq!(
            header,
            header_from_v2_repr(repr),
            "builder-constructed header diverged from decoded header"
        );
    }

    /// Legacy layout (adiri: epoch 0 is pre-fork): shadow encode == Header encode, both
    /// decode directions, a populated in-memory signature stays OFF the wire, and the
    /// decoded accessor surfaces `None`.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_header_serde_legacy_layout() {
        let repr = golden_legacy_repr();
        let enc_repr = encode(&repr);
        let header: Header = decode(&enc_repr);
        assert!(
            header.seed_signature().is_none(),
            "pre-fork header must surface no seed signature"
        );
        // A populated in-memory signature must not leak into pre-fork wire bytes.
        let built = header_from_legacy_repr(repr.clone(), seeded_signature(1032, b"legacy-inert"));
        assert_eq!(enc_repr, encode(&built), "legacy wire bytes must exclude seed_signature");
        assert_eq!(enc_repr, encode(&header), "legacy shadow bytes and Header bytes diverged");
        let repr2: HeaderReprLegacy = decode(&encode(&header));
        assert_eq!(repr, repr2, "Header mismatch");
        assert_eq!(header, built, "builder-constructed header diverged from decoded header");
    }

    /// Digest preimage == wire bytes for the V1 layout (independent hasher run).
    #[cfg(feature = "adiri")]
    #[test]
    fn test_digest_is_wire_hash_v1_layout() {
        let header = golden_v1_header();
        assert_eq!(
            header.digest(),
            wire_digest(&encode(&header)),
            "V1 digest preimage must be exactly the wire bytes"
        );
    }

    /// Digest preimage == wire bytes for the V2 layout (independent hasher run).
    #[test]
    fn test_digest_is_wire_hash_v2_layout() {
        let header = header_from_v2_repr(HeaderReprV2 { epoch: v2_epoch(), ..golden_v2_repr() });
        assert_eq!(
            header.digest(),
            wire_digest(&encode(&header)),
            "V2 digest preimage must be exactly the wire bytes"
        );
    }

    /// Digest preimage == wire bytes for the legacy layout (adiri: epoch 0 pre-fork).
    #[cfg(feature = "adiri")]
    #[test]
    fn test_digest_is_wire_hash_legacy_layout() {
        let header = golden_legacy_header();
        assert_eq!(
            header.digest(),
            wire_digest(&encode(&header)),
            "legacy digest preimage must be exactly the wire bytes"
        );
    }

    /// PIN (all cfgs): V1 golden wire bytes anchored via the eight-field derive shadow, in
    /// both directions. Never checked against `Header`: see [`GOLDEN_V1_EPOCH`].
    #[test]
    fn test_golden_v1_wire_bytes_pinned() {
        assert_eq!(
            hex::encode(encode(&golden_v1_repr())),
            GOLDEN_V1_HEADER_HEX,
            "V1 shadow encode diverged from the frozen golden vector"
        );
        let decoded: HeaderReprV1 = decode(&unhex(GOLDEN_V1_HEADER_HEX));
        assert_eq!(decoded, golden_v1_repr(), "decode of the frozen V1 golden vector diverged");
    }

    /// PIN (all cfgs): V1 golden digest as blake3 over the pinned wire bytes (the digest
    /// preimage IS the wire bytes), independent of the `Header` code path.
    #[test]
    fn test_golden_v1_digest_pinned() {
        assert_eq!(
            hex::encode(wire_digest(&unhex(GOLDEN_V1_HEADER_HEX))),
            GOLDEN_V1_DIGEST_HEX,
            "blake3 of the pinned V1 wire bytes diverged from the frozen digest"
        );
    }

    /// PIN (adiri): the epoch-gated `Header` itself emits and re-reads the frozen V1 bytes at
    /// [`GOLDEN_V1_HEADER_EPOCH`], and the eight-field derive shadow agrees.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_golden_v1_header_wire_bytes_pinned() {
        assert_eq!(
            hex::encode(encode(&golden_v1_header_repr())),
            GOLDEN_V1_HEADER_AT_FORK_HEX,
            "V1 shadow encode diverged from the frozen adiri V1 Header vector"
        );
        assert_eq!(
            hex::encode(encode(&golden_v1_header())),
            GOLDEN_V1_HEADER_AT_FORK_HEX,
            "Header encode diverged from the frozen adiri V1 Header vector"
        );
        let decoded: Header = decode(&unhex(GOLDEN_V1_HEADER_AT_FORK_HEX));
        assert_eq!(decoded, golden_v1_header(), "decode of the frozen adiri V1 vector diverged");
        assert_eq!(
            Some(&golden_v1_repr().seed_signature),
            decoded.seed_signature(),
            "decoded adiri V1 vector lost its seed signature"
        );
        assert_eq!(
            decoded.created_at_millis(),
            0,
            "decoded V1 header must carry no sub-second part"
        );
    }

    /// PIN (adiri): the V1 `Header` digest at [`GOLDEN_V1_HEADER_EPOCH`], anchored two ways —
    /// the `Header::digest` cache and an independent blake3 over the pinned wire bytes.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_golden_v1_header_digest_pinned() {
        assert_eq!(
            hex::encode(golden_v1_header().digest()),
            GOLDEN_V1_DIGEST_AT_FORK_HEX,
            "Header digest diverged from the frozen adiri V1 digest"
        );
        assert_eq!(
            hex::encode(wire_digest(&unhex(GOLDEN_V1_HEADER_AT_FORK_HEX))),
            GOLDEN_V1_DIGEST_AT_FORK_HEX,
            "blake3 of the pinned adiri V1 wire bytes diverged from the frozen digest"
        );
    }

    /// PIN (all cfgs): V2 golden wire bytes — shadow encode, Header encode, and the decode
    /// direction for both.
    #[test]
    fn test_golden_v2_wire_bytes_pinned() {
        assert_eq!(
            hex::encode(encode(&golden_v2_repr())),
            GOLDEN_V2_HEADER_HEX,
            "V2 shadow encode diverged from the frozen golden vector"
        );
        assert_eq!(
            hex::encode(encode(&golden_v2_header())),
            GOLDEN_V2_HEADER_HEX,
            "Header encode diverged from the frozen V2 golden vector"
        );
        let decoded: Header = decode(&unhex(GOLDEN_V2_HEADER_HEX));
        assert_eq!(decoded, golden_v2_header(), "decode of the frozen V2 golden vector diverged");
        assert_eq!(
            Some(&golden_v2_repr().seed_signature),
            decoded.seed_signature(),
            "decoded V2 golden vector lost its seed signature"
        );
        assert_eq!(decoded.created_at_millis(), 999, "decoded V2 golden vector lost its millis");
        let decoded_repr: HeaderReprV2 = decode(&unhex(GOLDEN_V2_HEADER_HEX));
        assert_eq!(decoded_repr, golden_v2_repr(), "shadow decode of the V2 vector diverged");
    }

    /// PIN (all cfgs): V2 golden digest, anchored two ways — the `Header::digest` cache and an
    /// independent blake3 over the pinned wire bytes.
    #[test]
    fn test_golden_v2_digest_pinned() {
        assert_eq!(
            hex::encode(golden_v2_header().digest()),
            GOLDEN_V2_DIGEST_HEX,
            "Header digest diverged from the frozen V2 golden digest"
        );
        assert_eq!(
            hex::encode(wire_digest(&unhex(GOLDEN_V2_HEADER_HEX))),
            GOLDEN_V2_DIGEST_HEX,
            "blake3 of the pinned V2 wire bytes diverged from the frozen digest"
        );
    }

    /// PIN (all cfgs): legacy golden wire bytes anchored via the seven-field derive shadow —
    /// origin/main-identical by construction — so the pin holds even where the epoch gate
    /// gives epoch 0 a later layout (non-adiri builds).
    #[test]
    fn test_golden_legacy_wire_bytes_pinned() {
        assert_eq!(
            hex::encode(encode(&golden_legacy_repr())),
            GOLDEN_LEGACY_HEADER_HEX,
            "legacy shadow encode diverged from the frozen golden vector"
        );
        let decoded: HeaderReprLegacy = decode(&unhex(GOLDEN_LEGACY_HEADER_HEX));
        assert_eq!(decoded, golden_legacy_repr(), "decode of the frozen legacy vector diverged");
    }

    /// PIN (adiri): the epoch-gated `Header` itself emits and re-reads the frozen legacy
    /// bytes for a pre-fork epoch — the second, independent anchor of the same constant.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_golden_legacy_header_wire_bytes_pinned() {
        assert_eq!(
            hex::encode(encode(&golden_legacy_header())),
            GOLDEN_LEGACY_HEADER_HEX,
            "pre-fork Header encode diverged from the frozen legacy golden vector"
        );
        let decoded: Header = decode(&unhex(GOLDEN_LEGACY_HEADER_HEX));
        assert_eq!(
            decoded,
            golden_legacy_header(),
            "decode of the frozen legacy golden vector diverged"
        );
        assert!(
            decoded.seed_signature().is_none(),
            "decoded pre-fork golden header must surface no seed signature"
        );
    }

    /// PIN (all cfgs): legacy golden digest as blake3 over the pinned wire bytes (the
    /// digest preimage IS the wire bytes), independent of the `Header` code path.
    #[test]
    fn test_golden_legacy_digest_pinned() {
        assert_eq!(
            hex::encode(wire_digest(&unhex(GOLDEN_LEGACY_HEADER_HEX))),
            GOLDEN_LEGACY_DIGEST_HEX,
            "blake3 of the pinned legacy wire bytes diverged from the frozen digest"
        );
    }

    /// PIN (adiri): `Header::digest` of the pre-fork golden fixture equals the frozen
    /// legacy digest (ties the cached digest to the historical preimage).
    #[cfg(feature = "adiri")]
    #[test]
    fn test_golden_legacy_header_digest_pinned() {
        assert_eq!(
            hex::encode(golden_legacy_header().digest()),
            GOLDEN_LEGACY_DIGEST_HEX,
            "pre-fork Header digest diverged from the frozen legacy golden digest"
        );
    }

    /// Decode rejects a sub-second part of 1000 or more, naming the field and the value,
    /// instead of letting it carry into the seconds.
    #[test]
    fn test_decode_rejects_out_of_range_millis() {
        for millis in [1000, 1001, u16::MAX] {
            let bytes = encode(&HeaderReprV2 { created_at_millis: millis, ..golden_v2_repr() });
            let err = try_decode::<Header>(&bytes)
                .expect_err("created_at_millis of 1000 or more must not decode")
                .to_string();
            assert!(
                err.contains("created_at_millis") && err.contains(&format!("`{millis}`")),
                "decode error must name the field and the value {millis}, got: {err}"
            );
        }
    }

    /// An eight-field decoder, i.e. a binary that predates the sub-second fork, fails loudly
    /// on V2 bytes: `created_at_millis` is left over as trailing input rather than dropped.
    #[test]
    fn test_v1_decoder_rejects_v2_bytes() {
        let bytes = encode(&golden_v2_header());
        assert!(
            matches!(try_decode::<HeaderReprV1>(&bytes), Err(bcs::Error::RemainingInput)),
            "an eight-field decoder must reject V2 header bytes as trailing input"
        );
    }

    /// Eight-field bytes at a sub-second-active epoch do not decode as a `Header`: the
    /// decoder requires the ninth field there, which is also why [`GOLDEN_V1_HEADER_HEX`]
    /// cannot anchor `Header` at [`GOLDEN_V1_EPOCH`].
    #[test]
    fn test_v2_decoder_rejects_v1_bytes_at_active_epoch() {
        assert!(
            try_decode::<Header>(&unhex(GOLDEN_V1_HEADER_HEX)).is_err(),
            "a sub-second-active epoch must require created_at_millis on the wire"
        );
    }

    /// An inactive epoch never reads `created_at_millis` (adiri: the seed-signature fork
    /// epoch), so nine-field bytes there fail on the trailing field exactly like any other
    /// trailing input.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_pre_fork_decode_rejects_millis_field() {
        let repr = HeaderReprV2 { epoch: GOLDEN_V1_HEADER_EPOCH, ..golden_v2_repr() };
        assert!(
            matches!(try_decode::<Header>(&encode(&repr)), Err(bcs::Error::RemainingInput)),
            "a pre-fork header must not consume a created_at_millis field"
        );
    }

    /// Pre-fork epochs (adiri: the legacy epoch 0 and the V1 epoch [`GOLDEN_V1_HEADER_EPOCH`])
    /// keep `created_at_millis` at 0 through `Header::new` and the builder even when handed a
    /// sub-second part, and still record the floored seconds. The builder gates at `build`, so
    /// setting the epoch after the timestamp normalizes too.
    #[cfg(feature = "adiri")]
    #[test]
    fn test_pre_fork_normalizes_millis_to_zero() {
        let ms = TimestampMs::from_parts(1_700_000_101, 500);
        for epoch in [0, GOLDEN_V1_HEADER_EPOCH] {
            let headers = [
                ("Header::new", header_new_at(epoch, ms)),
                ("builder", HeaderBuilder::default().epoch(epoch).created_at_ms(ms).build()),
                (
                    "builder, epoch set last",
                    HeaderBuilder::default().created_at_ms(ms).epoch(epoch).build(),
                ),
            ];
            for (path, header) in headers {
                assert_eq!(header.created_at_millis(), 0, "{path} kept millis at epoch {epoch}");
                assert_eq!(
                    *header.created_at(),
                    ms.secs(),
                    "{path} seconds wrong at epoch {epoch}"
                );
            }
        }
    }

    /// Sub-second-active epochs keep the millisecond part through `Header::new` and the
    /// builder, with the floored seconds in `created_at`.
    #[test]
    fn test_post_fork_keeps_millis() {
        let ms = TimestampMs::from_parts(1_700_000_101, 500);
        let epoch = v2_epoch();
        let headers = [
            ("Header::new", header_new_at(epoch, ms)),
            ("builder", HeaderBuilder::default().epoch(epoch).created_at_ms(ms).build()),
        ];
        for (path, header) in headers {
            assert_eq!(header.created_at_millis(), 500, "{path} dropped the sub-second part");
            assert_eq!(*header.created_at(), ms.secs(), "{path} seconds must be floored");
            assert_eq!(header.created_at_ms(), ms, "{path} did not round-trip the timestamp");
        }
    }

    /// `created_at_ms` is exactly `from_parts(created_at, created_at_millis)`, on a decoded
    /// header and on headers built across the valid millisecond range.
    #[test]
    fn test_created_at_ms_round_trips() {
        let decoded: Header = decode(&encode(&golden_v2_repr()));
        assert_eq!(
            decoded.created_at_ms(),
            TimestampMs::from_parts(*decoded.created_at(), decoded.created_at_millis()),
            "created_at_ms must combine the two decoded fields"
        );
        assert_eq!(decoded.created_at_ms().as_millis(), 1_700_000_063_999);
        for millis in [0, 1, 500, 999] {
            let ms = TimestampMs::from_parts(1_700_000_101, millis);
            let header = header_new_at(v2_epoch(), ms);
            assert_eq!(
                header.created_at_ms(),
                TimestampMs::from_parts(*header.created_at(), header.created_at_millis()),
                "created_at_ms must combine the two fields for millis {millis}"
            );
            assert_eq!(header.created_at_ms(), ms, "created_at_ms lost millis {millis}");
        }
    }

    /// Between the builder's `created_at` and `created_at_ms` the last call wins, and
    /// `from_header` carries the sub-second part over.
    #[test]
    fn test_builder_created_at_precedence() {
        let ms = TimestampMs::from_parts(1_700_000_101, 500);
        let base = || HeaderBuilder::default().epoch(v2_epoch());
        let secs_last = base().created_at_ms(ms).created_at(ms.secs()).build();
        assert_eq!(secs_last.created_at_millis(), 0, "created_at(secs) must reset the millis");
        let ms_last = base().created_at(7).created_at_ms(ms).build();
        assert_eq!(ms_last.created_at_ms(), ms, "created_at_ms must override created_at");
        let copied = HeaderBuilder::from_header(&ms_last).build();
        assert_eq!(copied.created_at_millis(), 500, "from_header dropped the sub-second part");
        assert_eq!(copied, ms_last, "from_header round trip changed the header");
    }
}
