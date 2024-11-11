//! Block implementation for consensus.
//!
//! Blocks hold transactions and other data.
// Copyright (c) Telcoin, LLC

use reth_primitives::{
    BlockHash, SealedBlock, SealedBlockWithSenders, SealedHeader, TransactionSigned, Withdrawals,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;

use super::TimestampSec;

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
    /// The sealed header for this block.
    pub sealed_header: SealedHeader,
    /// Timestamp of when the entity was received by another node. This will help
    /// calculate latencies that are not affected by clock drift or network
    /// delays. This field is not set for own blocks.
    pub received_at: Option<TimestampSec>,
}

impl WorkerBlock {
    /// Create a new block for testing only!
    ///
    /// This is NOT a valid block for consensus.
    pub fn new(transactions: Vec<TransactionSigned>, sealed_header: SealedHeader) -> Self {
        Self { transactions, sealed_header, received_at: None }
    }

    /// Size of the block.
    pub fn size(&self) -> usize {
        size_of::<Self>()
    }

    /// Digest for this block (the hash of the sealed header).
    pub fn digest(&self) -> BlockHash {
        self.sealed_header.hash()
    }

    /// Replace the sealed header.
    pub fn update_header(&mut self, sealed_header: SealedHeader) {
        self.sealed_header = sealed_header;
    }

    /// Timestamp of this block header.
    pub fn created_at(&self) -> TimestampSec {
        self.sealed_header.header().timestamp
    }

    /// Pass a reference to a Vec<Transaction>;
    pub fn transactions(&self) -> &Vec<TransactionSigned> {
        &self.transactions
    }

    /// Returns a mutable reference to a Vec<Transaction>.
    pub fn transactions_mut(&mut self) -> &mut Vec<TransactionSigned> {
        &mut self.transactions
    }

    /// Returns the sealed header.
    pub fn sealed_header(&self) -> &SealedHeader {
        &self.sealed_header
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

impl TryFrom<&WorkerBlock> for SealedBlockWithSenders {
    type Error = WorkerBlockConversionError;

    fn try_from(block: &WorkerBlock) -> Result<Self, Self::Error> {
        let header = block.sealed_header.clone();
        // decode transactions
        let body = block.transactions.clone();
        // seal block
        let block = SealedBlock {
            header,
            body,
            ommers: vec![],
            withdrawals: Some(Withdrawals::new(vec![])),
            requests: None,
        };
        block.try_seal_with_senders().map_err(Self::Error::RecoverSigners)
    }
}
