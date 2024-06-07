//! Error types for building and executing output from consensus.

use thiserror::Error;

/// Executor error types
#[derive(Error, Debug)]
pub(crate) enum ExecutorError {
    /// Errors from BlockExecution
    #[error("Block execution error: {0}")]
    Execution(#[from] BlockExecutionError),
    /// Failed to decode transaction bytes
    #[error("RLP error decoding transaction within consensus output: {0}")]
    DecodeTransaction(#[from] alloy_rlp::Error),
    /// Failed to decode transaction bytes
    #[error("The number of transactions and signers recovered from batch don't match: {0} txs - {1} addresses")]
    RecoveredTransactionsLength(usize, usize),
}
