use base64::{engine::general_purpose, Engine};
use derive_builder::Builder;
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Digest, Hash, HashFunction};
use fastcrypto_tbls::{tbls::ThresholdBls, types::ThresholdBls12381MinSig};
use indexmap::IndexMap;
use mem_utils::MallocSizeOf;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fmt};

use crate::{
    config::{AuthorityIdentifier, Committee, Epoch, WorkerCache, WorkerId},
    crypto,
    error::{DagError, DagResult},
    now, Batch, BatchDigest, CertificateDigest, Round, TimestampSec, VoteDigest,
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

/// Versioned `Header` type for consensus layer.
#[derive(Clone, Deserialize, Serialize, MallocSizeOf)]
#[enum_dispatch(HeaderAPI)]
pub enum Header {
    /// Version 1 - based on sui's V2
    V1(HeaderV1),
}

// TODO: Revisit if we should not impl Default for Header and just use
// versioned header in Certificate?
impl Default for Header {
    fn default() -> Self {
        Self::V1(HeaderV1::default())
    }
}

impl Header {
    /// Hashed digest for Header
    pub fn digest(&self) -> HeaderDigest {
        match self {
            Header::V1(data) => data.digest(),
        }
    }

    /// Validate the [Header] based on the current [Committee] and [WorkerCache].
    pub fn validate(&self, committee: &Committee, worker_cache: &WorkerCache) -> DagResult<()> {
        match self {
            Header::V1(data) => data.validate(committee, worker_cache),
        }
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Header {
    type TypedDigest = HeaderDigest;

    fn digest(&self) -> HeaderDigest {
        match self {
            Header::V1(data) => data.digest(),
        }
    }
}

impl From<Header> for CertificateDigest {
    fn from(value: Header) -> Self {
        Self::new(value.digest().0)
    }
}

/// API for accessing fields for [Header] variants
#[enum_dispatch]
pub trait HeaderAPI {
    /// The [AuthorityIdentifier] that produced the header.
    fn author(&self) -> AuthorityIdentifier;
    /// The [Round] for the header.
    fn round(&self) -> Round;
    /// The [Epoch] for the header.
    fn epoch(&self) -> Epoch;
    /// The [TimestampSec] for the header.
    fn created_at(&self) -> &TimestampSec;
    /// The payload for the header.
    fn payload(&self) -> &IndexMap<BatchDigest, (WorkerId, TimestampSec)>;
    /// The [SystemMessage]s included with the header.
    fn system_messages(&self) -> &[SystemMessage];
    /// The parents for the header.
    fn parents(&self) -> &BTreeSet<CertificateDigest>;
    /// Replace the header's payload with a new one.
    ///
    /// Only used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn update_payload(&mut self, new_payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)>);
    /// Replace the header's round with a new one.
    ///
    /// Only used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn update_round(&mut self, new_round: Round);
    /// Clear the header's parents.
    ///
    /// Only used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn clear_parents(&mut self);
}

/// Header version 1
#[derive(Builder, Clone, Default, Deserialize, Serialize, MallocSizeOf)]
#[builder(pattern = "owned", build_fn(skip))]
pub struct HeaderV1 {
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
    pub payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)>,
    /// Collection of [SystemMessage]s.
    pub system_messages: Vec<SystemMessage>,
    /// Parent certificates for this Header.
    pub parents: BTreeSet<CertificateDigest>,
    /// The [HeaderDigest].
    #[serde(skip)]
    pub digest: OnceCell<HeaderDigest>,
}

impl HeaderAPI for HeaderV1 {
    fn author(&self) -> AuthorityIdentifier {
        self.author
    }
    fn round(&self) -> Round {
        self.round
    }
    fn epoch(&self) -> Epoch {
        self.epoch
    }
    fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }
    fn payload(&self) -> &IndexMap<BatchDigest, (WorkerId, TimestampSec)> {
        &self.payload
    }
    fn system_messages(&self) -> &[SystemMessage] {
        &self.system_messages
    }
    fn parents(&self) -> &BTreeSet<CertificateDigest> {
        &self.parents
    }

    // Used for testing.

    #[cfg(any(test, feature = "test-utils"))]
    fn update_payload(&mut self, new_payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)>) {
        self.payload = new_payload;
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn update_round(&mut self, new_round: Round) {
        self.round = new_round;
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn clear_parents(&mut self) {
        self.parents.clear();
    }
}

impl HeaderV1Builder {
    /// "Build" the header by taking all fields and calculating the hash.
    pub fn build(self) -> Result<HeaderV1, fastcrypto::error::FastCryptoError> {
        let h = HeaderV1 {
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

impl HeaderV1 {
    /// Initialize a new instance of [HeaderV1]
    pub fn new(
        author: AuthorityIdentifier,
        round: Round,
        epoch: Epoch,
        payload: IndexMap<BatchDigest, (WorkerId, TimestampSec)>,
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

    /// Return the digest of this header.
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

impl Hash<{ crypto::DIGEST_LENGTH }> for HeaderV1 {
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
            self.payload().keys().map(|x| Digest::from(*x).size()).sum::<usize>(),
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
        match self {
            Self::V1(data) => data.digest() == other.digest(),
        }
    }
}
