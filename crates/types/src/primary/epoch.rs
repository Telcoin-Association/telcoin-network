//! Define the Epoch record, vote and certificate structs.
//!
//! These are used to form a signed "chain" of epoch records.  They are useful
//! for quickly determining a committee for a given epoch even if the executed
//! state is not available (i.e. when syncing).  They include a certificate so
//! can allow trustless syncing of meta-data (the consensus chain) in order to
//! execute with known correct consensus outputs.

use crate::{
    crypto, encode, serde::RoaringBitmapSerde, BlsAggregateSignature, BlsPublicKey, BlsSignature,
    BlsSigner, ConsensusNumHash, Epoch, Intent, IntentMessage, IntentScope,
    ValidatorAggregateSignature as _,
};
use alloy::eips::BlockNumHash;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::BTreeSet;

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
    pub parent_hash: EpochDigest,
    /// The block number and hash of the last execution state of this epoch.
    /// Basically the execution genesis for the next epoch after this one.
    /// Also a signed checkpoint of execution state (with the certificate).
    pub final_state: BlockNumHash,
    /// The hash and consensus block number of the last ['ConsensusHeader'] of this epoch.
    /// Can be used as a signed checkpoint for consensus (with the certificate).
    pub final_consensus: ConsensusNumHash,
}

impl EpochRecord {
    /// Return the digest for this ConsensusHeader.
    pub fn digest(&self) -> EpochDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(&encode(self));
        (*hasher.finalize().as_bytes()).into()
    }

    /// Use signer to generate an [`EpochVote`] for this EpochRecord.
    pub fn sign_vote<S: BlsSigner>(&self, signer: &S) -> EpochVote {
        let epoch_hash = self.digest();
        let intent =
            encode(&IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), epoch_hash));
        let signature = signer.request_signature_direct(&intent);

        EpochVote { epoch: self.epoch, epoch_hash, public_key: signer.public_key(), signature }
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

    /// Return true if this record's committee is an acceptable committee given
    /// `expected` (normally the previous epoch record's `next_committee`).
    ///
    /// This is the shared predicate used by both the epoch record producer and
    /// the sync-time verifier so they accept exactly the same shapes. The
    /// committees will usually be equal, but governance can forcibly eject a
    /// validator on-chain *after* the previous record sealed its
    /// `next_committee`, leaving this record's committee a strict subset of
    /// `expected`.
    ///
    /// Rules (`n = self.committee.len()`, `e = expected.len()`):
    /// - a committee with duplicate members is always invalid (duplicates would mask the real
    ///   signer count behind the certificate quorum)
    /// - `n > e`: invalid - a committee can never grow mid-epoch
    /// - `n == e`: the committees must be equal as sets
    /// - `n < e`: valid only if the shrunken committee keeps at least 4 members, keeps a BFT-safe
    ///   super majority of the expected committee (`n * 3 >= e * 2`, an integer-safe `n >=
    ///   ceil(2e/3)`), and every member was part of `expected`
    pub fn committee_compatible(&self, expected: &BTreeSet<BlsPublicKey>) -> bool {
        let committee: BTreeSet<BlsPublicKey> = self.committee.iter().copied().collect();
        if committee.len() != self.committee.len() {
            // Duplicate keys in a committee are always malformed.
            return false;
        }
        match self.committee.len().cmp(&expected.len()) {
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => &committee == expected,
            std::cmp::Ordering::Less => {
                // Make sure we still have a reasonable committee size, i.e. don't
                // let a bogus record with one signer through, etc.
                self.committee.len() >= 4
                    && self.committee.len() * 3 >= expected.len() * 2
                    && committee.is_subset(expected)
            }
        }
    }
}

/// Vote for an ['EpochRecord'].
/// Each committee member should gossip this on epoch start and other nodes
/// should collect them and aggregate signatures.
/// Note this is gossipped by the outgoing (previous committee).
#[derive(PartialEq, Serialize, Deserialize, Copy, Clone, Debug, Default)]
pub struct EpochVote {
    /// The epoch this is voting for.
    pub epoch: Epoch,
    /// The hash of the ['EpochRecord'].
    /// Store the hash not the record to keep gossip size down.
    /// Other nodes can request the record once vs recieving it many times.
    pub epoch_hash: EpochDigest,
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
    pub epoch_hash: EpochDigest,
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

crate::crypto::digest_newtype! {
    /// Digest of a [`EpochRecord`].
    pub struct EpochDigest;
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use rand::{rngs::StdRng, CryptoRng, RngCore, SeedableRng as _};
    use roaring::RoaringBitmap;

    use crate::{crypto, decode, encode, BlsKeypair, ConsensusNumHash, Signer as _};
    use alloy::primitives::B256;

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
            parent_hash: EpochDigest::default(),
            final_state: BlockNumHash::default(),
            final_consensus: ConsensusNumHash::default(),
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

    /// Deterministic BLS public key for committee-shape tests.
    fn test_bls_key(seed: u64) -> BlsPublicKey {
        let mut rng = StdRng::seed_from_u64(seed);
        *BlsKeypair::generate(&mut rng).public()
    }

    /// Build a record with `committee` and check compatibility against `expected`.
    fn compatible(expected: &[BlsPublicKey], committee: &[BlsPublicKey]) -> bool {
        let record = EpochRecord { committee: committee.to_vec(), ..Default::default() };
        let expected: BTreeSet<BlsPublicKey> = expected.iter().copied().collect();
        record.committee_compatible(&expected)
    }

    #[test]
    fn test_committee_compatible_equal_and_reordered() {
        let keys: Vec<_> = (0..5).map(test_bls_key).collect();
        // Same set, same order.
        assert!(compatible(&keys, &keys));
        // Same set, different order - compatibility is order-insensitive.
        let rotated = [keys[4], keys[2], keys[0], keys[3], keys[1]];
        assert!(compatible(&keys, &rotated));
    }

    #[test]
    fn test_committee_compatible_one_ejected_from_five() {
        let keys: Vec<_> = (0..5).map(test_bls_key).collect();
        // Swap-and-pop ejection of keys[2]: the last member moves into its slot.
        let ejected = [keys[0], keys[1], keys[4], keys[3]];
        assert!(compatible(&keys, &ejected));
        // Same size as the ejected shape but one member substituted with a
        // non-member: invalid.
        let substituted = [keys[0], keys[1], test_bls_key(9), keys[3]];
        assert!(!compatible(&keys, &substituted));
        // A duplicated member masking the true signer count: invalid.
        let duplicated = [keys[0], keys[1], keys[1], keys[3]];
        assert!(!compatible(&keys, &duplicated));
    }

    #[test]
    fn test_committee_compatible_floor_and_bound_table() {
        let keys: Vec<_> = (0..9).map(test_bls_key).collect();
        // (expected size, record size, compatible?)
        let table = [
            (4, 3, false), // below the 4-member floor
            (5, 4, true),
            (7, 5, true),
            (7, 4, false), // 4*3 < 7*2: below ceil(2/3) of expected
            (8, 6, true),
            (8, 5, false), // 5*3 < 8*2: below ceil(2/3) of expected
            (4, 5, false), // a record committee larger than expected is never valid
        ];
        for (expected_len, record_len, valid) in table {
            assert_eq!(
                compatible(&keys[..expected_len], &keys[..record_len]),
                valid,
                "expected len {expected_len}, record len {record_len}",
            );
        }
    }

    #[test]
    fn test_verify_with_cert_quorum_on_shrunken_committee() {
        // A 5-member committee that lost one member mid-epoch leaves a 4-member
        // record: the certificate quorum is 2/3+1 of the *shrunken* committee.
        let signers: Vec<_> =
            (0..4).map(|i| TestBlsKeypair::new(&mut StdRng::seed_from_u64(i))).collect();
        let committee: Vec<_> = signers.iter().map(|s| s.public_key()).collect();
        let record = EpochRecord { committee, ..Default::default() };
        assert_eq!(record.super_quorum(), 3);

        let cert = |count: usize| {
            let sigs: Vec<_> =
                signers.iter().take(count).map(|s| record.sign_vote(s).signature).collect();
            let signature = BlsAggregateSignature::aggregate(&sigs[..], true)
                .expect("aggregate")
                .to_signature();
            let mut signed_authorities = RoaringBitmap::new();
            for i in 0..count as u32 {
                signed_authorities.push(i);
            }
            EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities }
        };

        // 3 of 4 committee votes: quorum met.
        assert!(record.verify_with_cert(&cert(3)));
        // 2 of 4 committee votes: below quorum.
        assert!(!record.verify_with_cert(&cert(2)));
    }

    /// Verify that EpochDigest encodes/decodes to the same bytes as a B256/BlockHash.
    #[test]
    fn test_epoch_digest_serde() {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(b"test_epoch_digest_serde");
        let init_bytes = B256::from_slice(hasher.finalize().as_bytes());
        let edigest: EpochDigest = init_bytes.into();
        let enc = encode(&edigest);
        let b256: B256 = decode(&enc);
        assert_eq!(init_bytes, b256);
        let enc = encode(&b256);
        let edigest2: EpochDigest = decode(&enc);
        assert_eq!(edigest, edigest2);
    }
}
