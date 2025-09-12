//! Define the block header for the Telcoin "Consensus Chain"
//!
//! This is a very simple (data only) chain that records consesus output.
//! It can be used to validate the execution chain, catch up with consesus,
//! introduce a new validator to participate in consensus (either as a voter
//! or observer) or any task that requires realtime or historic consesus data
//! if not directly participating in consesus.

use super::{CommittedSubDag, ConsensusOutput};
use crate::{crypto, error::CertificateResult, BlockHash, Certificate, Committee, Hash, B256};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
