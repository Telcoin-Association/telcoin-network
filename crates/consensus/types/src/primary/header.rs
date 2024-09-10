use alloy_rlp::MaxEncodedLenAssoc;
use base64::{engine::general_purpose, Engine};
use derive_builder::Builder;
use fastcrypto::hash::{Digest, Hash, HashFunction};
use fastcrypto_tbls::{tbls::ThresholdBls, types::ThresholdBls12381MinSig};
use indexmap::IndexMap;
use mem_utils::MallocSizeOf;
use once_cell::sync::OnceCell;
use reth_primitives::BlockHash;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

use crate::{
    config::{AuthorityIdentifier, Committee, Epoch, WorkerCache, WorkerId},
    crypto,
    error::{DagError, DagResult},
    now, CertificateDigest, Round, TimestampSec, VoteDigest, WorkerBlock,
};

/// Messages generated internally by Narwhal that are included in headers for sequencing.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Deserialize, MallocSizeOf, Serialize)]
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
    /// Validation is at: https://github.com/MystenLabs/sui/blob/f0b80d9eeef44edd9fbe606cee16717622b68651/narwhal/primary/src/primary.rs#L713-L719
    pub author: AuthorityIdentifier,
    /// The round for this header
    pub round: Round,
    /// The epoch this Header was created in.
    pub epoch: Epoch,
    /// The timestamp for when the header was requested to be created.
    pub created_at: TimestampSec,
    /// IndexMap of the [BatchDigest] to the [WorkerId] and [TimestampSec]
    #[serde(with = "indexmap::serde_seq")]
    pub payload: IndexMap<BlockHash, (WorkerId, TimestampSec)>,
    /// Collection of [SystemMessage]s.
    pub system_messages: Vec<SystemMessage>,
    /// Parent certificates for this Header.
    pub parents: BTreeSet<CertificateDigest>,
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
        };
        let digest = Hash::digest(&header);
        header.digest.set(digest).unwrap();
        header
    }

    /// Hashed digest for Header
    pub fn digest(&self) -> HeaderDigest {
        *self.digest.get_or_init(|| Hash::digest(self))
    }

    /// Ensure the header is valid based on the current committee and workercache.
    ///
    /// The digest is calculated with the sealed header, so the EL data is also verified.
    pub fn validate(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        // Ensure the header is from the correct epoch.
        ensure!(
            self.epoch == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: self.epoch }
        );

        // Ensure the header digest is well formed.
        ensure!(Hash::digest(self) == self.digest(), DagError::InvalidHeaderDigest);

        // Ensure the authority has voting rights.
        let voting_rights = committee.stake_by_id(self.author);
        ensure!(voting_rights > 0, DagError::UnknownAuthority(self.author.to_string()));

        // Ensure all worker ids are correct.
        for (worker_id, _) in self.payload.values() {
            worker_cache
                .worker(committee.authority(&self.author).unwrap().protocol_key(), worker_id)
                .map_err(|_| DagError::HeaderHasBadWorkerIds(self.digest()))?;
        }

        // Ensure system messages are valid.
        let mut has_dkg_message = false;
        let mut has_dkg_confirmation = false;
        for m in self.system_messages.iter() {
            match m {
                SystemMessage::DkgMessage(msg) => {
                    ensure!(msg.sender == self.author.0, DagError::InvalidSystemMessage);
                    // A header must have no more than one DkgMessage.
                    ensure!(!has_dkg_message, DagError::DuplicateSystemMessage);
                    has_dkg_message = true;
                }
                SystemMessage::DkgConfirmation(conf) => {
                    ensure!(conf.sender == self.author.0, DagError::InvalidSystemMessage);
                    // A header must have no more than one DkgConfirmation.
                    ensure!(!has_dkg_confirmation, DagError::DuplicateSystemMessage);
                    has_dkg_confirmation = true;
                }
            }
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
    #[cfg(any(test, feature = "test-utils"))]
    pub fn update_payload(&mut self, new_payload: IndexMap<BlockHash, (WorkerId, TimestampSec)>) {
        self.payload = new_payload;
    }

    /// Replace the header's round with a new one.
    ///
    /// Only used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn update_round(&mut self, new_round: Round) {
        self.round = new_round;
    }

    /// Clear the header's parents.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn clear_parents(&mut self) {
        self.parents.clear();
    }
}

impl From<Header> for CertificateDigest {
    fn from(value: Header) -> Self {
        Self::new(value.digest().0)
    }
}

impl HeaderBuilder {
    /// "Build" the header by taking all fields and calculating the hash.
    pub fn build(self) -> Result<Header, fastcrypto::error::FastCryptoError> {
        let h = Header {
            author: self.author.unwrap(),
            round: self.round.unwrap(),
            epoch: self.epoch.unwrap(),
            created_at: self.created_at.unwrap_or(0),
            payload: self.payload.unwrap(),
            system_messages: self.system_messages.unwrap_or_default(),
            parents: self.parents.unwrap(),
            digest: OnceCell::default(),
        };

        // TODO: return error here
        h.digest.set(Hash::digest(&h)).unwrap();

        Ok(h)
    }

    /// Helper method to directly set values of the payload
    pub fn with_payload_batch(
        mut self,
        worker_block: WorkerBlock,
        worker_id: WorkerId,
        created_at: TimestampSec,
    ) -> Self {
        if self.payload.is_none() {
            self.payload = Some(Default::default());
        }
        let payload = self.payload.as_mut().unwrap();

        payload.insert(worker_block.digest(), (worker_id, created_at));

        self
    }
}

/// The slice of bytes for the header's digest.
#[derive(
    Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord, MallocSizeOf,
)]
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
        hasher.update(bcs::to_bytes(&self).expect("Serialization should not fail"));
        HeaderDigest(hasher.finalize().into())
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: B{}({}, E{}, {}B)",
            self.digest(),
            self.round(),
            self.author(),
            self.epoch(),
            self.payload().len() * BlockHash::LEN,
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
