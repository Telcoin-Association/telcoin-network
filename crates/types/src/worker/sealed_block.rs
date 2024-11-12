//! Block implementation for consensus.
//!
//! Blocks hold transactions and other data. This type is used to represent worker proposals that
//! have reached quorum.
// Copyright (c) Telcoin, LLC

use crate::{crypto, encode, TimestampSec};
use fastcrypto::hash::HashFunction;
use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, proofs, Address, BlockHash, Bloom, Bytes, Header, SealedBlock,
    SealedHeader, TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, U256,
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
pub struct WorkerBlock {
    /// The collection of transactions executed in this block.
    pub transactions: Vec<TransactionSigned>,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own blocks.
    #[serde(skip)]
    // This field changes often so don't serialize (i.e. don't use it in the digest)
    pub received_at: Option<TimestampSec>,
    /// The Keccak 256-bit hash of the parent
    /// block’s header, in its entirety; formally Hp.
    pub parent_hash: B256,
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
}

impl WorkerBlock {
    /// Create a new block for testing only!
    ///
    /// This is NOT a valid block for consensus.
    pub fn new_for_test(transactions: Vec<TransactionSigned>, sealed_header: SealedHeader) -> Self {
        Self {
            transactions,
            parent_hash: sealed_header.parent_hash,
            beneficiary: sealed_header.beneficiary,
            timestamp: sealed_header.timestamp,
            base_fee_per_gas: sealed_header.base_fee_per_gas,
            received_at: None,
        }
    }

    /// Size of the block.
    pub fn size(&self) -> usize {
        size_of::<Self>()
    }

    /// Digest for this block (the hash of the sealed header).
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

    /// Returns a sealed header.
    /// This is a synthetic sealed header with a lot of default values.
    /// It is NOT an actual block on the chain and has limited utility.
    pub fn sealed_header(&self) -> SealedHeader {
        let transactions_root = proofs::calculate_transaction_root(&self.transactions);

        let total_possible_gas = self.total_possible_gas();

        // create header
        //
        // NOTE: workers do not execute transactions. Peers validate:
        // - calculated transaction root
        // - all other roots are defaults
        // - use ZERO for hashes
        let header = Header {
            parent_hash: self.parent_hash,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: self.beneficiary,
            state_root: B256::ZERO,
            transactions_root,
            receipts_root: B256::ZERO,
            withdrawals_root: Some(EMPTY_WITHDRAWALS),
            logs_bloom: Bloom::default(),
            timestamp: self.timestamp,
            mix_hash: B256::ZERO,
            nonce: 0,
            base_fee_per_gas: self.base_fee_per_gas,
            number: 1,
            gas_limit: max_worker_block_gas(self.timestamp), // gas limit in wei - just a default
            difficulty: U256::ZERO,
            gas_used: total_possible_gas,
            extra_data: Bytes::default(),
            parent_beacon_block_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_root: None,
        };
        header.seal_slow()
    }

    /// Returns the received at time if available.
    pub fn received_at(&self) -> Option<TimestampSec> {
        self.received_at
    }

    /// Sets the recieved at field.
    pub fn set_received_at(&mut self, time: TimestampSec) {
        self.received_at = Some(time)
    }
}

/// Return the max gas per block in effect at timestamp.
/// Currently allways 30,000,000 but can change in the future at a fork.
pub fn max_worker_block_gas(_timestamp: u64) -> u64 {
    30_000_000
}
