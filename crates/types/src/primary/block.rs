//! Define the block header for the Telcoin "Consensus Chain"
//!
//! This is a very simple (data only) chain that records consesus output.
//! It can be used to validate the execution chain, catch up with consesus,
//! introduce a new validator to participate in consensus (either as a voter
//! or observer) or any task that requires realtime or historic consesus data
//! if not directly participating in consesus.

use super::{CommittedSubDag, ConsensusOutput};
use crate::{crypto, BlsPublicKey, BlsSignature, Certificate, Digest, Epoch, Hash, Round, B256};
use serde::{Deserialize, Serialize};

/// Header for the consensus chain.
///
/// The consensus chain records consensus output used to extend the execution chain.
/// All hashes are Keccak 256.
#[derive(PartialEq, Serialize, Deserialize, Clone, Debug)]
pub struct ConsensusHeader {
    /// The hash of the previous ConsesusHeader in the chain.
    pub parent_hash: ConsensusHeaderDigest,

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
    pub fn digest(&self) -> ConsensusHeaderDigest {
        Self::digest_from_parts(self.parent_hash, &self.sub_dag, self.number)
    }

    /// Produce the digest that result from a ConsensusHeader with this data.
    /// This allows digesting in some cases with out cloning a CommittedSubDag.
    pub fn digest_from_parts(
        parent_hash: ConsensusHeaderDigest,
        sub_dag: &CommittedSubDag,
        number: u64,
    ) -> ConsensusHeaderDigest {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(parent_hash.as_ref());
        hasher.update(sub_dag.digest().as_ref());
        hasher.update(number.to_le_bytes().as_ref());
        // Include the extra field.
        // Not using this yet but include the default in the hash in prep when we do.
        hasher.update(B256::default().as_slice());
        ConsensusHeaderDigest(Digest { digest: hasher.finalize().into() })
    }
}

impl Default for ConsensusHeader {
    fn default() -> Self {
        let cert = Certificate::default();
        let sub_dag = CommittedSubDag::new(
            vec![cert.clone()],
            cert,
            0,
            crate::ReputationScores::default(),
            None,
        );
        Self {
            parent_hash: ConsensusHeaderDigest::default(),
            sub_dag,
            number: 0,
            extra: B256::default(),
        }
    }
}

impl From<ConsensusOutput> for ConsensusHeader {
    fn from(value: ConsensusOutput) -> Self {
        value.into_consensus_header()
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

crate::crypto::digest_newtype! {
    /// Digest of a [`ConsensusHeader`].
    pub struct ConsensusHeaderDigest;
}

/// A consensus header number and a hash.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, std::hash::Hash, Serialize, Deserialize)]
pub struct ConsensusNumHash {
    /// The number
    pub number: u64,
    /// The hash.
    pub hash: ConsensusHeaderDigest,
}

impl ConsensusNumHash {
    /// Creates a new `NumHash` from a number and hash.
    pub const fn new(number: u64, hash: ConsensusHeaderDigest) -> Self {
        Self { number, hash }
    }
}

/// Info that is published (via gossip) by validators once they reach consensus.
#[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct ConsensusResult {
    // epoch for this result (i.e. the current epoch)
    pub epoch: Epoch,
    // reound for epoch that consensus was reached on
    pub round: Round,
    /// the consensus header block number
    pub number: u64,
    /// hash of the consensus header that was reached
    pub hash: ConsensusHeaderDigest,
    /// the validator that produced this result
    pub validator: BlsPublicKey,
    /// the signature of the validator publishing this record
    /// see digest() below, this is a signature over the hash of the epoch, round, number and hash
    /// fields
    pub signature: BlsSignature,
}

impl ConsensusResult {
    /// Return the digest of the data fields (epoch, round, number and hash).
    /// This will be the same for all validadors and is what signature signs
    /// (verifying all the data fields not just the hash).
    pub fn digest(&self) -> ConsensusResultDigest {
        Self::digest_data(self.epoch, self.round, self.number, self.hash)
    }

    /// Return the digest of the data fields (epoch, round, number and hash).
    /// Used for generating the signature of the raw data.
    /// This will be the same for all validadors and is what signature signs
    /// (verifying all the data fields not just the hash).
    pub fn digest_data(
        epoch: Epoch,
        round: Round,
        number: u64,
        hash: ConsensusHeaderDigest,
    ) -> ConsensusResultDigest {
        let mut hasher = crate::DefaultHashFunction::new();
        hasher.update(&epoch.to_be_bytes());
        hasher.update(&round.to_be_bytes());
        hasher.update(&number.to_be_bytes());
        hasher.update(hash.as_ref());
        ConsensusResultDigest(Digest { digest: hasher.finalize().into() })
    }
}

crate::crypto::digest_newtype! {
    /// Digest of a [`ConsensusResult`].
    pub struct ConsensusResultDigest;
}

#[cfg(test)]
mod test {
    use alloy::{eips::NumHash, primitives::B256};

    use crate::{crypto, decode, encode, ConsensusHeaderDigest, ConsensusNumHash};

    /// Verify that ConsensusHeaderDigest encodes/decodes to the same bytes as a B256/BlockHash.
    #[test]
    fn test_consensus_digest_serde() {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(b"test_consensus_digest_serde");
        let init_bytes = B256::from_slice(hasher.finalize().as_bytes());
        let cdigest: ConsensusHeaderDigest = init_bytes.into();
        let enc = encode(&cdigest);
        let b256: B256 = decode(&enc);
        assert_eq!(init_bytes, b256);
        let enc = encode(&b256);
        let cdigest2: ConsensusHeaderDigest = decode(&enc);
        assert_eq!(cdigest, cdigest2);
    }

    /// Verify that ConsensusNumHash encodes/decodes to the same bytes as a NumHash.
    #[test]
    fn test_consensus_numhash_serde() {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(b"test_consensus_digest_serde");
        let init_bytes = B256::from_slice(hasher.finalize().as_bytes());
        let num_hash = NumHash { number: 3, hash: init_bytes };
        let consensus_num_hash = ConsensusNumHash { number: 3, hash: init_bytes.into() };
        let enc = encode(&consensus_num_hash);
        let num_hash2: NumHash = decode(&enc);
        assert_eq!(num_hash, num_hash2);
        let enc = encode(&num_hash2);
        let cnum_hash: ConsensusNumHash = decode(&enc);
        assert_eq!(consensus_num_hash, cnum_hash);
    }
}
