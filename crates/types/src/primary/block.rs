//! Define the block header for the Telcoin "Consensus Chain"
//!
//! This is a very simple (data only) chain that records consesus output.
//! It can be used to validate the execution chain, catch up with consesus,
//! introduce a new validator to participate in consensus (either as a voter
//! or observer) or any task that requires realtime or historic consesus data
//! if not directly participating in consesus.

use super::{CommittedSubDag, ConsensusOutput};
use crate::{
    crypto, encode, error::CertificateResult, serde::RoaringBitmapSerde, BlockHash,
    BlsAggregateSignature, BlsPublicKey, BlsSignature, BlsSigner, Certificate, Committee, Epoch,
    Hash, Intent, IntentMessage, IntentScope, ValidatorAggregateSignature as _, B256,
};
use alloy::eips::BlockNumHash;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::sync::Arc;

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

    pub fn sign<S: BlsSigner>(&self, signer: &S) -> EpochCertificate {
        let epoch_hash = self.digest();
        let intent =
            encode(&IntentMessage::new(Intent::consensus(IntentScope::EpochRecord), epoch_hash));
        let signature = signer.request_signature_direct(&intent);
        let mut signed_authorities = RoaringBitmap::new();
        let me = signer.public_key();
        // Turn on our "bit" for our singleton cert.
        for (i, c) in self.committee.iter().enumerate() {
            if *c == me {
                signed_authorities.insert(i as u32);
                break;
            }
        }

        EpochCertificate { epoch_hash, signature, signed_authorities }
    }

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
            IntentMessage::new(Intent::consensus(IntentScope::EpochRecord), cert.epoch_hash);
        if auth_iter < self.simple_quorum() {
            false
        } else {
            aggregate_signature.verify_secure(&intent, &pks[..])
        }
    }

    /// Provide a simple quorum, this is 2/3 of committee size plus one.
    /// With this many signers of an epoch record we should be safe.
    pub fn simple_quorum(&self) -> usize {
        ((self.committee.len() * 2) / 3) + 1
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
    /// Verify a single signature of the cert.
    /// Used when receiving published "single" signer certs for agregation.
    pub fn check_single_signature(&self, signer: &BlsPublicKey) -> bool {
        let intent = encode(&IntentMessage::new(
            Intent::consensus(IntentScope::EpochRecord),
            self.epoch_hash,
        ));
        self.signature.verify_raw(&intent, signer)
    }

    /// Verify a groug of signatures against the cert.
    pub fn check_signatures(&self, signers: &[BlsPublicKey]) -> bool {
        let aggregate_signature = BlsAggregateSignature::from_signature(&self.signature);
        let intent =
            IntentMessage::new(Intent::consensus(IntentScope::EpochRecord), self.epoch_hash);
        aggregate_signature.verify_secure(&intent, signers)
    }
}

/// Header for the consensus chain.
///
/// The consensus chain records consensus output used to extend the execution chain.
/// All hashes are Keccak 256.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusHeader {
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: B256,

    /// This is the committed sub dag used to extend the execution chain.
    pub sub_dag: CommittedSubDag,

    /// A scalar value equal to the number of ancestor blocks. The genesis block has a number of
    /// zero.
    pub number: u64,

    /// Temp extra data field - currently unused.
    /// This is included for now for testnet purposes only.
    pub extra: B256,
}

impl ConsensusHeader {
    /// Return the digest for this ConsensusHeader.
    pub fn digest(&self) -> BlockHash {
        Self::digest_from_parts(self.parent_hash, &self.sub_dag, self.number)
    }

    /// Produce the digest that result from a ConsensusHeader with this data.
    /// This allows digesting in some cases with out cloning a CommittedSubDag.
    pub fn digest_from_parts(
        parent_hash: B256,
        sub_dag: &CommittedSubDag,
        number: u64,
    ) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(parent_hash.as_slice());
        hasher.update(sub_dag.digest().as_ref());
        hasher.update(number.to_le_bytes().as_ref());
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Verify that all of the contained certificates are valid and signed by a quorum of committee.
    pub fn verify_certificates(self, committee: &Committee) -> CertificateResult<Self> {
        let Self { parent_hash, sub_dag, number, extra } = self;
        let sub_dag = sub_dag.verify_certificates(committee)?;
        Ok(Self { parent_hash, sub_dag, number, extra })
    }
}

impl Default for ConsensusHeader {
    fn default() -> Self {
        let sub_dag = CommittedSubDag::new(
            vec![],
            Certificate::default(),
            0,
            crate::ReputationScores::default(),
            None,
        );
        Self { parent_hash: B256::default(), sub_dag, number: 0, extra: B256::default() }
    }
}

impl From<ConsensusOutput> for ConsensusHeader {
    fn from(value: ConsensusOutput) -> Self {
        Self {
            parent_hash: value.parent_hash,
            sub_dag: Arc::unwrap_or_clone(value.sub_dag),
            number: value.number,
            extra: value.extra,
        }
    }
}

impl From<&[u8]> for ConsensusHeader {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

impl From<&ConsensusHeader> for Vec<u8> {
    fn from(value: &ConsensusHeader) -> Self {
        crate::encode(value)
    }
}
