// Copyright (c) Telcoin, LLC

//! Block implementation for consensus.
//!
//! Blocks hold transactions and other data. This type is used to represent worker proposals that
//! have reached quorum.

use crate::{adiri_chain_spec, crypto, encode, now, TimestampSec};
use fastcrypto::hash::HashFunction;
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, Address, BlockHash, Header, SealedBlock, TransactionSigned,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;

/// Type for sending ack back to EL once a block is sealed.
/// TODO: support propagating errors from the worker to the primary.
pub type WorkerBlockResponse = oneshot::Sender<BlockHash>;

/// Worker Block validation error types
#[derive(Error, Debug, Clone)]
pub enum WorkerBlockConversionError {
    /// Errors from BlockExecution
    #[error("Failed to recover signers for sealed block:\n{0:?}\n")]
    RecoverSigners(SealedBlock),
    /// Failed to decode transaction bytes
    #[error("RLP error decoding transaction: {0}")]
    DecodeTransaction(#[from] alloy_rlp::Error),
}

/// The block for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SealedWorkerBlock {
    /// The immutable worker block fields.
    pub block: WorkerBlock,
    /// The immutable digest of the block.
    pub digest: BlockHash,
}

impl SealedWorkerBlock {
    /// Create a new instance of Self.
    ///
    /// WARNING: this does not verify the provided digest matches the provided block.
    pub fn new(block: WorkerBlock, digest: BlockHash) -> Self {
        Self { block, digest }
    }

    /// Consume self to extract the worker block so it can be modified.
    pub fn unseal(self) -> WorkerBlock {
        self.block
    }

    /// Return the sealed worker block fields.
    pub fn block(&self) -> &WorkerBlock {
        &self.block
    }

    /// Return the digest of the sealed worker block.
    pub fn digest(&self) -> BlockHash {
        self.digest
    }

    /// Split Self into separate parts.
    ///
    /// This is the inverse of [`WorkerBlock::seal_slow`].
    pub fn split(self) -> (WorkerBlock, BlockHash) {
        (self.block, self.digest)
    }

    /// Size of the sealed block.
    pub fn size(&self) -> usize {
        self.block.size() + size_of::<BlockHash>()
    }
}

/// The block for workers to communicate for consensus.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct WorkerBlock {
    /// The collection of transactions executed in this block.
    pub transactions: Vec<TransactionSigned>,
    /// The Keccak 256-bit hash of the parent
    /// block’s header, in its entirety; formally Hp.
    pub parent_hash: BlockHash,
    /// The 160-bit address to which all fees collected from the successful mining of this block
    /// be transferred; formally Hc.
    pub beneficiary: Address,
    /// A scalar value equal to the reasonable output of Unix’s time() at this block’s inception;
    /// formally Hs.
    pub timestamp: u64,
    /// A scalar representing EIP1559 base fee which can move up or down each block according
    /// to a formula which is a function of gas used in parent block and gas target
    /// (block gas limit divided by elasticity multiplier) of parent block.
    /// The algorithm results in the base fee per gas increasing when blocks are
    /// above the gas target, and decreasing when blocks are below the gas target. The base fee per
    /// gas is burned.
    pub base_fee_per_gas: Option<u64>,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own blocks.
    #[serde(skip)]
    // This field changes often so don't serialize (i.e. don't use it in the digest)
    pub received_at: Option<TimestampSec>,
}

impl WorkerBlock {
    /// Create a new block for testing only!
    ///
    /// This is NOT a valid block for consensus.
    pub fn new_for_test(transactions: Vec<TransactionSigned>, header: Header) -> Self {
        Self {
            transactions,
            parent_hash: header.parent_hash,
            beneficiary: header.beneficiary,
            timestamp: header.timestamp,
            base_fee_per_gas: header.base_fee_per_gas,
            received_at: None,
        }
    }

    /// Size of the block.
    pub fn size(&self) -> usize {
        size_of::<Self>()
    }

    /// Digest for this block (the hash of the sealed header).
    ///
    /// NOTE: `Self::received_at` is skipped during serialization and is excluded from the digest.
    pub fn digest(&self) -> BlockHash {
        let mut hasher = crypto::DefaultHashFunction::new();
        hasher.update(encode(self));
        // finalize
        BlockHash::from_slice(&hasher.finalize().digest)
    }

    /// Timestamp of this block header.
    pub fn created_at(&self) -> TimestampSec {
        self.timestamp
    }

    /// Pass a reference to a Vec<Transaction>;
    pub fn transactions(&self) -> &Vec<TransactionSigned> {
        &self.transactions
    }

    /// Returns a mutable reference to a Vec<Transaction>.
    pub fn transactions_mut(&mut self) -> &mut Vec<TransactionSigned> {
        &mut self.transactions
    }

    /// Return the max possible gas the contained transactions could use.
    /// Does not execute transactions, just sums up there gas limit.
    pub fn total_possible_gas(&self) -> u64 {
        let mut total_possible_gas = 0;

        // begin loop through sorted "best" transactions in pending pool
        // and execute them to build the block
        for tx in &self.transactions {
            // txs are not executed, so use the gas_limit
            total_possible_gas += tx.gas_limit();
        }

        total_possible_gas
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
    pub fn seal(self, digest: BlockHash) -> SealedWorkerBlock {
        SealedWorkerBlock::new(self, digest)
    }

    /// Seal the worker block.
    ///
    /// Calculate the hash and seal the worker block so it can't be changed.
    ///
    /// NOTE: `WorkerBlock::received_at` is skipped during serialization and is excluded from the
    /// digest.
    pub fn seal_slow(self) -> SealedWorkerBlock {
        let digest = self.digest();
        self.seal(digest)
    }
}

impl Default for WorkerBlock {
    fn default() -> Self {
        Self {
            transactions: vec![],
            received_at: None,
            parent_hash: adiri_chain_spec().genesis_hash(),
            beneficiary: Address::ZERO,
            timestamp: now(),
            base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
        }
    }
}

impl From<&Vec<u8>> for SealedWorkerBlock {
    fn from(value: &Vec<u8>) -> Self {
        crate::decode(&value)
    }
}

/// Return the max gas per block in effect at timestamp.
/// Currently allways 30,000,000 but can change in the future at a fork.
pub fn max_worker_block_gas(_timestamp: u64) -> u64 {
    30_000_000
}

/// Max worker block size in effect at a timestamp.  Measured in bytes.
/// Currently allways 1,000,000 but can change in the future at a fork.
pub fn max_worker_block_size(_timestamp: u64) -> usize {
    1_000_000
}
