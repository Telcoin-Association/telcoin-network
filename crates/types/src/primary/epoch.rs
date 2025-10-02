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
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug, Default)]
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
#[derive(PartialEq, Serialize, Deserialize, Copy, Clone, Debug, Default)]
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use rand::{rngs::StdRng, CryptoRng, RngCore, SeedableRng as _};
    use roaring::RoaringBitmap;

    use crate::{BlsKeypair, Signer as _};

    use super::*;

    #[derive(Clone)]
    struct TestBlsKeypair(Arc<BlsKeypair>);

    impl TestBlsKeypair {
        fn new<R: CryptoRng + RngCore>(rng: &mut R) -> Self {
            Self(Arc::new(BlsKeypair::generate(rng)))
        }
    }

    impl BlsSigner for TestBlsKeypair {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    #[test]
    fn test_epoch_records() {
        let mut rng = StdRng::from_os_rng();
        let com1 = TestBlsKeypair::new(&mut rng);
        let com2 = TestBlsKeypair::new(&mut rng);
        let com3 = TestBlsKeypair::new(&mut rng);
        let record = EpochRecord {
            epoch: 0,
            committee: vec![com1.public_key(), com2.public_key(), com3.public_key()],
            next_committee: vec![com1.public_key(), com2.public_key(), com3.public_key()],
            parent_hash: B256::default(),
            parent_state: BlockNumHash::default(),
            parent_consensus: B256::default(),
        };
        let vote1 = record.sign_vote(&com1);
        let vote2 = record.sign_vote(&com2);
        let vote3 = record.sign_vote(&com3);
        assert_eq!(vote1.public_key, com1.public_key());
        assert_eq!(vote2.public_key, com2.public_key());
        assert_eq!(vote3.public_key, com3.public_key());
        assert!(vote1.check_signature(), "vote1 failed sig check");
        assert!(vote2.check_signature(), "vote2 failed sig check");
        assert!(vote3.check_signature(), "vote3 failed sig check");
        let sigs = vec![vote1.signature, vote2.signature, vote3.signature];
        match BlsAggregateSignature::aggregate(&sigs[..], true) {
            Ok(aggregated_signature) => {
                let signature: BlsSignature = aggregated_signature.to_signature();
                let mut signed_authorities = RoaringBitmap::new();
                signed_authorities.push(0);
                signed_authorities.push(1);
                signed_authorities.push(2);
                let cert =
                    EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
                assert!(record.verify_with_cert(&cert), "record failed to verify");
                // leave out a sig.
                let mut signed_authorities = RoaringBitmap::new();
                signed_authorities.push(0);
                signed_authorities.push(2);
                let cert =
                    EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
                assert!(!record.verify_with_cert(&cert), "record verified!");
            }
            Err(_) => {
                panic!("failed to aggregate epoch record signatures",);
            }
        }
    }
}

// If we have the record but not the cert then wait a beat for it to show up.
