//! Batch implementation for consensus.
//!
//! Batches hold transactions and other data. This type is used to represent worker proposals that
//! have reached quorum.

use crate::{
    crypto, encode, now, Address, BlockHash, ExecHeader, TimestampSec, MIN_PROTOCOL_BASE_FEE,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

use super::WorkerId;

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SealedBatch {
    /// The immutable batch fields.
    pub batch: Batch,
    /// The immutable digest of the batch.
    pub digest: BlockHash,
}

impl SealedBatch {
    /// Create a new instance of Self.
    ///
    /// WARNING: this does not verify the provided digest matches the provided batch.
    pub fn new(batch: Batch, digest: BlockHash) -> Self {
        Self { batch, digest }
    }

    /// Consume self to extract the batch so it can be modified.
    pub fn unseal(self) -> Batch {
        self.batch
    }

    /// Return the sealed batch fields.
    pub fn batch(&self) -> &Batch {
        &self.batch
    }

    /// Return the digest of the sealed batch.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// Split Self into separate parts.
    ///
    /// This is the inverse of [`Batch::seal_slow`].
    pub fn split(self) -> (Batch, BlockHash) {
        (self.batch, self.digest)
    }

    /// Size of the sealed batch.
    pub fn size(&self) -> usize {
        self.batch.size() + size_of::<BlockHash>()
    }
}

/// The batch for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The collection of transactions in this batch as bytes.
    pub transactions: Vec<Vec<u8>>,
    /// The Keccak 256-bit hash of the parent
    /// batch’s header, in its entirety; formally Hp.
    pub parent_hash: BlockHash,
    /// The 160-bit address to which all fees collected from the successful mining of this batch
    /// be transferred; formally Hc.
    pub beneficiary: Address,
    /// A scalar value equal to the reasonable output of Unix’s time() at this batch’s inception;
    /// formally Hs.
    pub timestamp: u64,
    /// A scalar representing EIP1559 base fee which can move up or down each batch according
    /// to a formula which is a function of gas used in parent batch and gas target
    /// (batch gas limit divided by elasticity multiplier) of parent batch.
    /// The algorithm results in the base fee per gas increasing when batchs are
    /// above the gas target, and decreasing when batchs are below the gas target. The base fee per
    /// gas is burned.
    pub base_fee_per_gas: Option<u64>,
    /// The worker id for the worker that orginated this batch.
    /// Worker ids will be consistent accross validators (i.e. worker 0 talks to othere worker 0s,
    /// etc). We can use this for tracking to support base fee calculations.
    /// Note: worker id 0 is the default.
    pub worker_id: WorkerId,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own batchs.
    #[serde(skip)]
    // This field changes often so don't serialize (i.e. don't use it in the digest)
    pub received_at: Option<TimestampSec>,
}

impl Batch {
    /// Create a new batch for testing only!
    ///
    /// This is NOT a valid batch for consensus.
    pub fn new_for_test(
        transactions: Vec<Vec<u8>>,
        header: ExecHeader,
        worker_id: WorkerId,
    ) -> Self {
        Self {
            transactions,
            parent_hash: header.parent_hash,
            beneficiary: header.beneficiary,
            timestamp: header.timestamp,
            base_fee_per_gas: header.base_fee_per_gas,
            worker_id,
            received_at: None,
        }
    }

    /// Size of the batch.
    pub fn size(&self) -> usize {
        size_of::<Self>()
    }

    /// Digest for this batch (the hash of the sealed header).
    ///
    /// NOTE: `Self::received_at` is skipped during serialization and is excluded from the digest.
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(self).as_ref());
        // finalize
        BlockHash::from_slice(hasher.finalize().as_bytes())
    }

    /// Timestamp of this batch header.
    pub fn created_at(&self) -> TimestampSec {
        self.timestamp
    }

    /// Pass a reference to a collection of transaction bytes;
    pub fn transactions(&self) -> &Vec<Vec<u8>> {
        &self.transactions
    }

    /// Returns a mutable reference to a collection of transaction bytes.
    pub fn transactions_mut(&mut self) -> &mut Vec<Vec<u8>> {
        &mut self.transactions
    }

    /// Returns the received at time if available.
    pub fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    /// Sets the recieved at field.
    pub fn set_received_at(&mut self, time: TimestampSec) {
        self.received_at = Some(time)
    }

    /// Seal the header with a known hash.
    ///
    /// WARNING: This method does not verify whether the hash is correct.
    pub fn seal(self, digest: BlockHash) -> SealedBatch {
        SealedBatch::new(self, digest)
    }

    /// Seal the batch.
    ///
    /// Calculate the hash and seal the batch so it can't be changed.
    ///
    /// NOTE: `Batch::received_at` is skipped during serialization and is excluded from the
    /// digest.
    pub fn seal_slow(self) -> SealedBatch {
        let digest = self.digest();
        self.seal(digest)
    }
}

impl Default for Batch {
    fn default() -> Self {
        Self {
            transactions: vec![],
            received_at: None,
            parent_hash: BlockHash::default(),
            beneficiary: Address::ZERO,
            timestamp: now(),
            worker_id: 0,
            base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
        }
    }
}

impl From<&SealedBatch> for Vec<u8> {
    fn from(value: &SealedBatch) -> Self {
        crate::encode(value)
    }
}

impl From<&[u8]> for SealedBatch {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

/// Return the max gas per batch in effect at timestamp.
/// Currently allways 30,000,000 but can change in the future at a fork.
pub fn max_batch_gas(_timestamp: u64) -> u64 {
    30_000_000
}

/// Max batch size in effect at a timestamp.  Measured in bytes.
/// Currently allways 1,000,000 but can change in the future at a fork.
pub fn max_batch_size(_timestamp: u64) -> usize {
    1_000_000
}

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator).
///
/// Invalid transactions will not receive further processing.
pub trait BatchValidation: Send + Sync + Debug {
    /// Determines if this batch can be voted on
    fn validate_batch(&self, b: SealedBatch) -> Result<(), BatchValidationError>;

    /// Submit a transaction (as bytes) for inclusion in a batch.
    /// Will only submit if the txn hash fits the provided committee slot.
    fn submit_txn_if_mine(&self, tx_bytes: &[u8], committee_size: u64, committee_slot: u64);
}

/// Block validation error types
#[derive(Error, Debug)]
pub enum BatchValidationError {
    /// The sealed batch hash does not match this worker's calculated digest.
    #[error("Invalid digest for sealed batch.")]
    InvalidDigest,
    /// Ensure proposed batch is after parent.
    #[error("Peer's header proposed before parent batch timestamp.")]
    TimestampIsInPast {
        /// The parent batch's timestamp.
        parent_timestamp: u64,
        /// The batch's timestamp.
        timestamp: u64,
    },
    /// Canonical chain header cannot be found.
    #[error("Canonical chain header {block_hash} can't be found for peer batch's parent")]
    CanonicalChain {
        /// The executed block hash of the missing canonical chain header.
        block_hash: BlockHash,
    },
    /// Empty batch.
    #[error("Batch contains no transactions")]
    EmptyBatch,
    /// Error when the max gas included in the header exceeds the batch's gas limit.
    #[error("Peer's batch total possible gas ({total_possible_gas}) is greater than batch's gas limit ({gas_limit})")]
    HeaderMaxGasExceedsGasLimit {
        /// The total possible gas used in the batch header measured by included transactions max
        /// gas.
        total_possible_gas: u64,
        /// The gas limit in the batch header.
        gas_limit: u64,
    },
    /// Error while calculating max possible gas from icluded transactions.
    #[error("Unable to reduce max possible gas limit for peer's batch")]
    CalculateMaxPossibleGas,
    /// Error when peer's transaction list exceeds the maximum bytes allowed.
    #[error("Peer's transactions exceed max byte size: {0}")]
    HeaderTransactionBytesExceedsMax(usize),
    /// Error trying to decode a transaction in a peer's batch.
    /// If any transaction fails to decode, the entire batch validation fails.
    #[error("Failed to decode transaction for batch {0}: {1}")]
    RecoverTransaction(BlockHash, String),
    /// Error, invalid base fee set.
    #[error("Invalid base fee, expected {expected_base_fee} got {base_fee}")]
    InvalidBaseFee { expected_base_fee: u64, base_fee: u64 },
    /// Error, wrong worker id.
    #[error("Invalid worker id, expected {expected_worker_id} got {worker_id}")]
    InvalidWorkerId { expected_worker_id: WorkerId, worker_id: WorkerId },
}
