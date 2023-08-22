//! Error types emitted by types or implementations of this crate.

use revm_primitives::EVMError;
use tn_types::execution::H256;
use tokio::sync::oneshot;

/// Possible error variants during payload building.
#[derive(Debug, thiserror::Error)]
pub enum PayloadBuilderError {
    /// Thrown whe the parent block is missing.
    #[error("missing parent block {0:?}")]
    MissingParentBlock(H256),
    /// An oneshot channels has been closed.
    #[error("sender has been dropped")]
    ChannelClosed,
    /// Other internal error
    #[error(transparent)]
    Internal(#[from] execution_interfaces::Error),
    /// Unrecoverable error during evm execution.
    #[error("evm execution error: {0:?}")]
    EvmExecutionError(EVMError<execution_interfaces::Error>),
    /// Thrown if the payload requests withdrawals before Shanghai activation.
    #[error("withdrawals set before Shanghai activation")]
    WithdrawalsBeforeShanghai,
    /// Thrown if the batch payload builder can't find the finalized state
    #[error("missing finalized state to build next batch")]
    LatticeBatch,
    /// Thrown if the batch payload builder can't find the latest state (after genesis)
    #[error("missing genesis state for next batch")]
    LatticeBatchFromGenesis,
    /// Thrwon if the batch payload can't create a timestamp when initialized the BlockEnv.
    #[error("Failed to capture System Time.")]
    LatticeBatchSystemTime(#[from] std::time::SystemTimeError),
    /// Thrown if the receiver for the oneshot channel is dropped by the worker which requested the batch.
    #[error("Worker's receiver dropped before batch could be sent.")]
    LatticeBatchOneshotChannel,
}

impl From<oneshot::error::RecvError> for PayloadBuilderError {
    fn from(_: oneshot::error::RecvError) -> Self {
        PayloadBuilderError::ChannelClosed
    }
}
