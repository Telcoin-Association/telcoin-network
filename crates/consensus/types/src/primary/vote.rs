//! Vote implementation for consensus
use base64::{engine::general_purpose, Engine};
use enum_dispatch::enum_dispatch;
use fastcrypto::{
    hash::{Digest, Hash},
    signature_service::SignatureService,
    traits::{Signer, VerifyingKey},
};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
    config::{AuthorityIdentifier, Epoch},
    crypto::{
        self, to_intent_message, BlsPublicKey, BlsSignature, IntentMessage, ValidatorSignature,
    },
    Header, HeaderAPI, HeaderDigest, Round,
};

/// A Vote on a Header is a claim by the voting authority that all payloads and the full history
/// of Certificates included in the Header are available.
#[derive(Clone, Serialize, Deserialize)]
#[enum_dispatch(VoteAPI)]
pub enum Vote {
    /// Version 1
    V1(VoteV1),
}

impl Vote {
    // TODO: Add version number and match on that
    /// Create a new instance of [Vote]
    pub async fn new(
        header: &Header,
        author: &AuthorityIdentifier,
        signature_service: &SignatureService<BlsSignature, { crypto::INTENT_MESSAGE_LENGTH }>,
    ) -> Self {
        Vote::V1(VoteV1::new(header, author, signature_service).await)
    }

    /// TODO: docs
    pub fn new_with_signer<S>(header: &Header, author: &AuthorityIdentifier, signer: &S) -> Self
    where
        S: Signer<BlsSignature>,
    {
        Vote::V1(VoteV1::new_with_signer(header, author, signer))
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Vote {
    type TypedDigest = VoteDigest;

    fn digest(&self) -> VoteDigest {
        match self {
            Vote::V1(data) => data.digest(),
        }
    }
}

#[enum_dispatch]
pub trait VoteAPI {
    /// TODO
    fn header_digest(&self) -> HeaderDigest;
    /// TODO
    fn round(&self) -> Round;
    /// TODO
    fn epoch(&self) -> Epoch;
    /// TODO
    fn origin(&self) -> AuthorityIdentifier;
    /// TODO
    fn author(&self) -> AuthorityIdentifier;
    /// TODO
    fn signature(&self) -> &<BlsPublicKey as VerifyingKey>::Sig;
}

/// VoteV1
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct VoteV1 {
    /// HeaderDigest, round, epoch and origin for the header being voted on.
    pub header_digest: HeaderDigest,
    /// Round for this vote.
    pub round: Round,
    /// Epoch for this vote.
    pub epoch: Epoch,
    /// TODO - doc
    pub origin: AuthorityIdentifier,
    /// Author of this vote.
    pub author: AuthorityIdentifier,
    /// Signature of the HeaderDigest.
    pub signature: <BlsPublicKey as VerifyingKey>::Sig,
}

impl VoteAPI for VoteV1 {
    fn header_digest(&self) -> HeaderDigest {
        self.header_digest
    }
    fn round(&self) -> Round {
        self.round
    }
    fn epoch(&self) -> Epoch {
        self.epoch
    }
    fn origin(&self) -> AuthorityIdentifier {
        self.origin
    }
    fn author(&self) -> AuthorityIdentifier {
        self.author
    }
    fn signature(&self) -> &<BlsPublicKey as VerifyingKey>::Sig {
        &self.signature
    }
}

impl VoteV1 {
    pub async fn new(
        header: &Header,
        author: &AuthorityIdentifier,
        signature_service: &SignatureService<BlsSignature, { crypto::INTENT_MESSAGE_LENGTH }>,
    ) -> Self {
        let vote = Self {
            header_digest: header.digest(),
            round: header.round(),
            epoch: header.epoch(),
            origin: header.author(),
            author: *author,
            signature: BlsSignature::default(),
        };
        let signature = signature_service.request_signature(vote.digest().into()).await;
        Self { signature, ..vote }
    }

    pub fn new_with_signer<S>(header: &Header, author: &AuthorityIdentifier, signer: &S) -> Self
    where
        S: Signer<BlsSignature>,
    {
        let vote = Self {
            header_digest: header.digest(),
            round: header.round(),
            epoch: header.epoch(),
            origin: header.author(),
            author: *author,
            signature: BlsSignature::default(),
        };

        let vote_digest: Digest<{ crypto::DIGEST_LENGTH }> = vote.digest().into();
        let signature = BlsSignature::new_secure(&to_intent_message(vote_digest), signer);

        Self { signature, ..vote }
    }
}

/// Hash a Vote based on the crate's `DIGEST_LENGTH`
#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Copy)]
pub struct VoteDigest([u8; crypto::DIGEST_LENGTH]);

impl VoteDigest {
    /// Create a VoteDigest
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        VoteDigest(digest)
    }
}

impl From<VoteDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: VoteDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl From<VoteDigest> for HeaderDigest {
    fn from(value: VoteDigest) -> Self {
        Self::new(value.0)
    }
}

impl From<VoteDigest> for Digest<{ crypto::INTENT_MESSAGE_LENGTH }> {
    fn from(digest: VoteDigest) -> Self {
        // let intent_message = to_intent_message(HeaderDigest(digest.0));
        let intent_message: IntentMessage<HeaderDigest> = to_intent_message(digest.into());
        Digest {
            digest: bcs::to_bytes(&intent_message)
                .expect("Serialization message should not fail")
                .try_into()
                .expect("INTENT_MESSAGE_LENGTH is correct"),
        }
    }
}

impl fmt::Debug for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0))
    }
}

impl fmt::Display for VoteDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for VoteV1 {
    type TypedDigest = VoteDigest;

    fn digest(&self) -> VoteDigest {
        // VoteDigest(self.header_digest().0)
        self.header_digest.into()
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: V{}({}, {}, E{})",
            self.digest(),
            self.round(),
            self.author(),
            self.origin(),
            self.epoch()
        )
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
#[enum_dispatch(VoteInfoAPI)]
pub enum VoteInfo {
    V1(VoteInfoV1),
}

#[enum_dispatch]
pub trait VoteInfoAPI {
    fn epoch(&self) -> Epoch;
    fn round(&self) -> Round;
    fn vote_digest(&self) -> VoteDigest;
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub struct VoteInfoV1 {
    /// The latest Epoch for which a vote was sent to given authority
    pub epoch: Epoch,
    /// The latest round for which a vote was sent to given authority
    pub round: Round,
    /// The hash of the vote used to ensure equality
    pub vote_digest: VoteDigest,
}

impl VoteInfoAPI for VoteInfoV1 {
    fn epoch(&self) -> Epoch {
        self.epoch
    }

    fn round(&self) -> Round {
        self.round
    }

    fn vote_digest(&self) -> VoteDigest {
        self.vote_digest
    }
}

impl From<&VoteV1> for VoteInfoV1 {
    fn from(vote: &VoteV1) -> Self {
        VoteInfoV1 { epoch: vote.epoch(), round: vote.round(), vote_digest: vote.digest() }
    }
}

impl From<&Vote> for VoteInfo {
    fn from(vote: &Vote) -> Self {
        match vote {
            Vote::V1(vote) => VoteInfo::V1(VoteInfoV1::from(vote)),
        }
    }
}
