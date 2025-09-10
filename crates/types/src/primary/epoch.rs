//! Define the Epoch record, vote and certificate structs.
//!
//! These are used to form a signed "chain" of epoch records.  They are useful
//! for quickly determining a committee for a given epoch even if the executed
//! state is not available (i.e. when syncing).  They include a certificate so
//! can allow trustless syncing of meta-data (the consensus chain) in order to
//! execute with known correct consensus outputs.

use crate::{
    crypto, encode, serde::RoaringBitmapSerde, BlockHash, BlsAggregateSignature, BlsPublicKey,
    BlsSignature, BlsSigner, Epoch, Intent, IntentMessage, IntentScope,
    ValidatorAggregateSignature as _, B256,
};
use alloy::eips::BlockNumHash;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;

/// Record of an Epoch.  Will be created at epoch start for the previous epoch
/// and signed by that epochs committee members.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct EpochRecord {
    /// The epoch this record is for.
    pub epoch: Epoch,
    /// The active committee for this epoch.
    pub committee: Vec<BlsPublicKey>,
    /// The committee for the next epoch.
    /// This can be used for trustless syncing.
    pub next_committee: Vec<BlsPublicKey>,
    /// Hash of the previous EpochRecord.
    pub parent_hash: B256,
    /// The block number and hash of the last execution state of this epoch.
    /// Basically the execution genesis for the next epoch after this one.
    /// Also a signed checkpoint of execution state (with the certificate).
    pub parent_state: BlockNumHash,
    /// The hash of the last ['ConsensusHeader'] of this epoch.
    /// Can be used as a signed checkpoint for consensus (with the certificate).
    pub parent_consensus: B256,
}

impl EpochRecord {
    /// Return the digest for this ConsensusHeader.
    pub fn digest(&self) -> B256 {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(&encode(self));
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Use signer to generate an [`EpochVote`] for this EpochRecord.
    pub fn sign_vote<S: BlsSigner>(&self, signer: &S) -> EpochVote {
        let epoch_hash = self.digest();
        let intent =
            encode(&IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), epoch_hash));
        let signature = signer.request_signature_direct(&intent);

        EpochVote { epoch_hash, public_key: signer.public_key(), signature }
    }

    /// Return true if cert contains a quorum of committee signatures for this EpochRecord.
    pub fn verify_with_cert(&self, cert: &EpochCertificate) -> bool {
        if self.digest() != cert.epoch_hash {
            // Record and cert don't match.
            return false;
        }

        let auth_indexes = cert.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks: Vec<BlsPublicKey> = self
            .committee
            .iter()
            .enumerate()
            .filter(|(i, _authority)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == *i as u32 => {
                    auth_iter += 1;
                    true
                }
                _ => false,
            })
            .map(|(_, key)| *key)
            .collect();

        let aggregate_signature = BlsAggregateSignature::from_signature(&cert.signature);
        let intent =
            IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), cert.epoch_hash);
        if auth_iter < self.super_quorum() {
            false
        } else {
            aggregate_signature.verify_secure(&intent, &pks[..])
        }
    }

    /// Provide a super quorum, this is 2/3 of committee size plus one.
    /// With this many signers of an epoch record we are safe unless a
    /// super majority of validators are byzantine.
    pub fn super_quorum(&self) -> usize {
        ((self.committee.len() * 2) / 3) + 1
    }
}

/// Vote for an ['EpochRecord'].
/// Each committee member should gossip this on epoch start and other nodes
/// should collect them and aggregate signatures.
/// Note this is gossipped by the outgoing (previous committee).
#[derive(PartialEq, Serialize, Deserialize, Copy, Clone, Debug)]
pub struct EpochVote {
    /// The hash of the ['EpochRecord'].
    /// Store the hash not the record to keep gossip size down.
    /// Other nodes can request the record once vs recieving it many times.
    pub epoch_hash: B256,
    /// Public key of the committee member that signed this.
    /// This needs to be verified to be a committee member.
    pub public_key: BlsPublicKey,
    /// Signature of a committee member for the epoch.
    pub signature: BlsSignature,
}

impl EpochVote {
    /// Verify a single signature of the cert.
    /// Used when receiving published "single" signer certs for agregation.
    pub fn check_signature(&self) -> bool {
        let intent = encode(&IntentMessage::new(
            Intent::consensus(IntentScope::EpochBoundary),
            self.epoch_hash,
        ));
        self.signature.verify_raw(&intent, &self.public_key)
    }
}

/// Certificate of an ['EpochRecord'].
/// Each committee member should gossip this on epoch start and other nodes
/// should collect them and aggregate signatures.
#[serde_as]
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct EpochCertificate {
    /// The hash of the ['EpochRecord'].
    /// Store the hash not the record to keep gossip size down.
    /// Other nodes can request the record once vs recieving it many times.
    pub epoch_hash: B256,
    /// Signatures of a quorum of committee member for the epoch.
    pub signature: BlsSignature,
    /// Bitmap defining which committee members signed this certificate.
    #[serde_as(as = "RoaringBitmapSerde")]
    pub signed_authorities: roaring::RoaringBitmap,
}

impl EpochCertificate {
    /// Verify a groug of signatures against the cert.
    pub fn check_signatures(&self, signers: &[BlsPublicKey]) -> bool {
        let aggregate_signature = BlsAggregateSignature::from_signature(&self.signature);
        let intent =
            IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), self.epoch_hash);
        aggregate_signature.verify_secure(&intent, signers)
    }
}
