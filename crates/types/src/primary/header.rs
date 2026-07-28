use crate::{
    crypto, encode,
    error::{HeaderError, HeaderResult},
    now, AuthorityIdentifier, Batch, BlockHash, BlockNumHash, Committee, Digest, Epoch, Hash,
    Round, TimestampSec, VoteDigest, WorkerId, MAX_HEADER_NUM_OF_BATCHES,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt, sync::Arc};

/// `Header` inner data type for consensus layer.
#[derive(Deserialize, Serialize)]
struct HeaderInner {
    /// Primary that created the header. Must be the same primary that broadcasted the header.
    author: AuthorityIdentifier,
    /// The round for this header
    round: Round,
    /// The epoch this Header was created in.
    epoch: Epoch,
    /// The timestamp for when the header was requested to be created.
    created_at: TimestampSec,
    /// IndexMap of the [BatchDigest] to the [WorkerId]
    #[serde(with = "indexmap::map::serde_seq")]
    payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    parents: BTreeSet<HeaderDigest>,
    /// Hash and number of the latest known execution block when this Header was build.
    /// This may be our parent block or may not but it does include our latest
    /// execution result in a signed and validated structure which validates
    /// this execution block as well.
    latest_execution_block: BlockNumHash,
    /// The [HeaderDigest].
    /// This is cached to avoid calculating frequently (but not serialized).
    /// Note, this struct is private and this field MUST always be set on creation in this module.
    /// Failure to do so is undefined behaviour (not being set and not matching the struct is
    /// inexpressable outside of a bug in this module).
    #[serde(skip)]
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
    /// Initialize a new instance of [HeaderV1]
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BlockHash, WorkerId>,
        parents: BTreeSet<HeaderDigest>,
        latest_execution_block: BlockNumHash,
    ) -> Self {
        let mut inner = HeaderInner {
            author,
            round,
            epoch,
            created_at: now(),
            payload,
            parents,
            digest: HeaderDigest::default(),
            latest_execution_block,
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
            if *worker_id as usize >= committee.number_of_workers() {
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
    /// The [TimestampSec] for the header.
    pub fn created_at(&self) -> &TimestampSec {
        &self.inner.created_at
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

    /// The nonce of this header used during execution.
    pub fn nonce(&self) -> u64 {
        ((self.inner.epoch as u64) << 32) | self.inner.round as u64
    }
}

impl Serialize for Header {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let ok = self.inner.serialize(serializer)?;
        Ok(ok)
    }
}

impl<'de> Deserialize<'de> for Header {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mut inner = HeaderInner::deserialize(deserializer)?;
        let digest = Hash::digest(&inner);
        inner.digest = digest;
        Ok(Self { inner: Arc::new(inner) })
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
    /// The timestamp for when the header was requested to be created.
    created_at: TimestampSec,
    /// IndexMap of the [BatchDigest] to the [WorkerId]
    payload: IndexMap<BlockHash, WorkerId>,
    /// Parent certificates for this Header.
    parents: BTreeSet<HeaderDigest>,
    /// Hash and number of the latest known execution block when this Header was build.
    latest_execution_block: BlockNumHash,
}

impl HeaderBuilder {
    /// Build a new builder using the values from header as the defaults.
    pub fn from_header(header: &Header) -> Self {
        Self {
            author: header.inner.author.clone(),
            round: header.inner.round,
            epoch: header.inner.epoch,
            created_at: header.inner.created_at,
            payload: header.inner.payload.clone(),
            parents: header.inner.parents.clone(),
            latest_execution_block: header.inner.latest_execution_block,
        }
    }

    /// "Build" the header by taking all fields and calculating the hash.
    /// This is used for tests, if used for "real" code then at least latest_execution_block will
    /// need to be visited.
    pub fn build(self) -> Header {
        let mut inner = HeaderInner {
            author: self.author,
            round: self.round,
            epoch: self.epoch,
            created_at: self.created_at,
            payload: self.payload,
            parents: self.parents,
            digest: HeaderDigest::default(),
            latest_execution_block: self.latest_execution_block,
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
    /// Set the created_at on the builder.
    pub fn created_at(mut self, created_at: TimestampSec) -> Self {
        self.created_at = created_at;
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
        hasher.update(encode(&self).as_ref());
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
    use serde::{Deserialize, Serialize};

    use crate::{
        decode, encode, AuthorityIdentifier, Epoch, Header, HeaderDigest, Round, TimestampSec,
        WorkerId,
    };

    /// `Header` type for consensus layer- use this to make sure serde is ignoring the inner Arc....
    #[derive(Clone, Deserialize, Serialize, Default, Debug, PartialEq)]
    struct HeaderData {
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
        /// This may be our parent block or may not but it does include our latest
        /// execution result in a signed and validated structure which validates
        /// this execution block as well.
        pub latest_execution_block: BlockNumHash,
    }

    /// Simple test to make sure that moving to the inner Arc on Header will not
    /// change the serialized data.  If this test breaks it may indicate a fork
    /// was introduced.  This test may also outlive it's usefulness.
    #[test]
    fn test_header_serde() {
        let mut test_header = HeaderData::default();
        test_header.payload.insert(BlockHash::default(), 0);
        test_header.parents.insert(HeaderDigest::default());
        let test_enc = encode(&test_header);
        let header: Header = decode(&test_enc);
        let test_enc = encode(&header);
        let test_header2: HeaderData = decode(&test_enc);
        assert_eq!(test_header, test_header2, "Header mismatch");
    }
}
