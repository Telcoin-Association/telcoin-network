use crate::{
    crypto, encode,
    error::{HeaderError, HeaderResult},
    now, AuthorityIdentifier, Batch, BlockHash, BlockNumHash, BlockNumber, CertificateDigest,
    Committee, Epoch, Round, TimestampSec, VoteDigest, WorkerCache, WorkerId,
};
use base64::{engine::general_purpose, Engine};
use derive_builder::Builder;
use fastcrypto::hash::{Digest, Hash, HashFunction};
use fastcrypto_tbls::{tbls::ThresholdBls, types::ThresholdBls12381MinSig};
use indexmap::IndexMap;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

/// Messages generated internally by Narwhal that are included in headers for sequencing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Deserialize, Serialize)]
pub enum SystemMessage {
    // DKG is used to generate keys for use in the random beacon protocol.
    // `DkgMessage` is sent out at start-of-epoch to initiate the process.
    DkgMessage(
        fastcrypto_tbls::dkg::Message<
            <ThresholdBls12381MinSig as ThresholdBls>::Public,
            <ThresholdBls12381MinSig as ThresholdBls>::Public,
        >,
    ),
    // `DkgConfirmation` is the second DKG message, sent as soon as a threshold amount of
    // `DkgMessages` have been received locally, to complete the key generation process.
    DkgConfirmation(
        fastcrypto_tbls::dkg::Confirmation<<ThresholdBls12381MinSig as ThresholdBls>::Public>,
    ),
}

/// `Header` type for consensus layer.
#[derive(Builder, Clone, Deserialize, Serialize, Default)]
#[builder(pattern = "owned", build_fn(skip))]
pub struct Header {
    /// Primary that created the header. Must be the same primary that broadcasted the header.
    pub author: AuthorityIdentifier,
    /// The round for this header
    pub round: Round,
    /// The epoch this Header was created in.
    pub epoch: Epoch,
    /// The timestamp for when the header was requested to be created.
    pub created_at: TimestampSec,
    /// IndexMap of the [BatchDigest] to the [WorkerId] and [TimestampSec]
    #[serde(with = "indexmap::map::serde_seq")]
    pub payload: IndexMap<BlockHash, (WorkerId, TimestampSec)>,
    /// Collection of [SystemMessage]s.
    pub system_messages: Vec<SystemMessage>,
    /// Parent certificates for this Header.
    pub parents: BTreeSet<CertificateDigest>,
    /// Hash of the latest known execution block when this Header was build.
    /// This may be our parent block or may not but it does include our latest
    /// execution result in a signed and validates structure which validates
    /// this execution block as well.
    pub latest_execution_block: BlockHash,
    /// Number of the latest known execution block when this Header was build.
    pub latest_execution_block_num: BlockNumber,
    /// The [HeaderDigest].
    #[serde(skip)]
    pub digest: OnceCell<HeaderDigest>,
}

impl Header {
    /// Initialize a new instance of [HeaderV1]
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BlockHash, (WorkerId, TimestampSec)>,
        system_messages: Vec<SystemMessage>,
        parents: BTreeSet<CertificateDigest>,
        latest_execution_block: BlockNumHash,
    ) -> Self {
        let header = Self {
            author,
            round,
            epoch,
            created_at: now(),
            payload,
            system_messages,
            parents,
            digest: OnceCell::default(),
            latest_execution_block: latest_execution_block.hash,
            latest_execution_block_num: latest_execution_block.number,
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).expect("digest oncecell empty for new header");
        header
    }

    /// Hashed digest for Header
    pub fn digest(&self) -> HeaderDigest {
        *self.digest.get_or_init(|| Hash::digest(self))
    }

    /// Ensure the header is valid based on the current committee and workercache.
    ///
    /// The digest is calculated with the sealed header, so the EL data is also verified.
    pub fn validate(&self, committee: &Committee, worker_cache: &WorkerCache) -> HeaderResult<()> {
        // Ensure the header is from the correct epoch.
        if self.epoch != committee.epoch() {
            return Err(HeaderError::InvalidEpoch { theirs: self.epoch, ours: committee.epoch() });
        }

        // Ensure the header digest is well formed.
        if Hash::digest(self) != self.digest() {
            return Err(HeaderError::InvalidHeaderDigest);
        }

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake_by_id(self.author);
        if voting_rights == 0 {
            return Err(HeaderError::UnknownAuthority(self.author.to_string()));
        }

        // Ensure all worker ids are correct.
        for (worker_id, _) in self.payload.values() {
            worker_cache
                .worker(
                    committee
                        .authority(&self.author)
                        .ok_or(HeaderError::UnknownAuthority(self.author.to_string()))?
                        .protocol_key(),
                    worker_id,
                )
                .map_err(|_| HeaderError::UnkownWorkerId)?;
        }

        Ok(())
    }

    /// The [AuthorityIdentifier] that produced the header.
    pub fn author(&self) -> AuthorityIdentifier {
        self.author
    }
    /// The [Round] for the header.
    pub fn round(&self) -> Round {
        self.round
    }
    /// The [Epoch] for the header.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    /// The [TimestampSec] for the header.
    pub fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }
    /// The payload for the header.
    pub fn payload(&self) -> &IndexMap<BlockHash, (WorkerId, TimestampSec)> {
        &self.payload
    }
    /// The [SystemMessage]s included with the header.
    pub fn system_messages(&self) -> &[SystemMessage] {
        &self.system_messages
    }
    /// The parents for the header.
    pub fn parents(&self) -> &BTreeSet<CertificateDigest> {
        &self.parents
    }

    // Used for testing.

    /// Replace the header's payload with a new one.
    ///
    /// Only used for testing.
    pub fn update_payload_for_test(
        &mut self,
        new_payload: IndexMap<BlockHash, (WorkerId, TimestampSec)>,
    ) {
        self.payload = new_payload;
    }

    /// Replace the header's round with a new one.
    ///
    /// Only used for testing.
    pub fn update_round_for_test(&mut self, new_round: Round) {
        self.round = new_round;
    }

    /// Clear the header's parents.
    pub fn clear_parents_for_test(&mut self) {
        self.parents.clear();
    }

    /// Return the latest known block number/hash this header was built from.
    pub fn latest_execution_block_num_hash(&self) -> BlockNumHash {
        BlockNumHash { hash: self.latest_execution_block, number: self.latest_execution_block_num }
    }

    /// The nonce of this header used during execution.
    pub fn nonce(&self) -> u64 {
        ((self.epoch as u64) << 32) | self.round as u64
    }
}

impl From<Header> for CertificateDigest {
    fn from(value: Header) -> Self {
        Self::new(value.digest().0)
    }
}

impl HeaderBuilder {
    /// "Build" the header by taking all fields and calculating the hash.
    /// This is used for tests, if used for "real" code then at least latest_execution_block will
    /// need to be visited.
    pub fn build(self) -> Result<Header, fastcrypto::error::FastCryptoError> {
        let h = Header {
            author: self.author.expect("author set for header builder"),
            round: self.round.expect("round set for header builder"),
            epoch: self.epoch.expect("epoch set for header builder"),
            created_at: self.created_at.unwrap_or(0),
            payload: self.payload.unwrap_or_default(),
            system_messages: self.system_messages.unwrap_or_default(),
            parents: self.parents.expect("parents set for header builder"),
            digest: OnceCell::default(),
            latest_execution_block: self.latest_execution_block.unwrap_or_default(),
            latest_execution_block_num: self.latest_execution_block_num.unwrap_or_default(),
        };

        // TODO: return error here
        h.digest.set(Hash::digest(&h)).expect("digest oncecell empty for new header");

        Ok(h)
    }

    /// Helper method to directly set values of the payload
    pub fn with_payload_batch(
        mut self,
        batch: Batch,
        worker_id: WorkerId,
        created_at: TimestampSec,
    ) -> Self {
        if self.payload.is_none() {
            self.payload = Some(Default::default());
        }
        let payload = self.payload.as_mut().unwrap();

        payload.insert(batch.digest(), (worker_id, created_at));

        self
    }
}

/// The slice of bytes for the header's digest.
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HeaderDigest(pub [u8; crypto::DIGEST_LENGTH]);

impl HeaderDigest {
    /// Create a new HeaderDigest based on the crate's `DIGEST_LENGTH` constant.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        HeaderDigest(digest)
    }
}

impl From<HeaderDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: HeaderDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl AsRef<[u8]> for HeaderDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<HeaderDigest> for VoteDigest {
    fn from(value: HeaderDigest) -> Self {
        Self::new(value.0)
    }
}

impl fmt::Debug for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0))
    }
}

impl fmt::Display for HeaderDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(&self));
        HeaderDigest(hasher.finalize().into())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}(v{}, e{}, {}wbs, exec: {} - {})",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len(),
            self.latest_execution_block_num,
            self.latest_execution_block,
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
