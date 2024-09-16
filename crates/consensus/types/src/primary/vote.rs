//! Vote implementation for consensus
use base64::{engine::general_purpose, Engine};
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
    encode, Header, HeaderDigest, Round,
};

/// A Vote on a Header is a claim by the voting authority that all payloads and the full history
/// of Certificates included in the Header are available.
#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
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

impl Vote {
    // TODO: Add version number and match on that
    /// Create a new instance of [Vote]
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

    /// TODO: docs
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

    pub fn header_digest(&self) -> HeaderDigest {
        self.header_digest
    }
    pub fn round(&self) -> Round {
        self.round
    }
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    pub fn origin(&self) -> AuthorityIdentifier {
        self.origin
    }
    pub fn author(&self) -> AuthorityIdentifier {
        self.author
    }
    pub fn signature(&self) -> &<BlsPublicKey as VerifyingKey>::Sig {
        &self.signature
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
            digest: encode(&intent_message).try_into().expect("INTENT_MESSAGE_LENGTH is correct"),
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

impl Hash<{ crypto::DIGEST_LENGTH }> for Vote {
    type TypedDigest = VoteDigest;

    fn digest(&self) -> VoteDigest {
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
pub struct VoteInfo {
    /// The latest Epoch for which a vote was sent to given authority
    pub epoch: Epoch,
    /// The latest round for which a vote was sent to given authority
    pub round: Round,
    /// The hash of the vote used to ensure equality
    pub vote_digest: VoteDigest,
}

impl VoteInfo {
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn vote_digest(&self) -> VoteDigest {
        self.vote_digest
    }
}

impl From<&Vote> for VoteInfo {
    fn from(vote: &Vote) -> Self {
        VoteInfo { epoch: vote.epoch(), round: vote.round(), vote_digest: vote.digest() }
    }
}
