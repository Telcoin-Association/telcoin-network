//! Error type to wrap various Reth errors.

use reth::rpc::{builder::error::RpcError, server_types::eth::EthApiError};
use reth_errors::BlockExecutionError;
use reth_provider::ProviderError;

/// Result alias for [`TNRethError`].
pub type TnRethResult<T> = Result<T, TnRethError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum TnRethError {
    /// Error retrieving data from Provider.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Error recovering transaction from bytes.
    #[error(transparent)]
    RecoverTransactionBytes(#[from] EthApiError),
    /// The block body and senders lengths don't match.
    #[error("Failed to seal block with senders - lengths don't match")]
    SealBlockWithSenders,
    /// The executed block failed.
    #[error("Block execution failed: {0}")]
    BlockExecution(#[from] BlockExecutionError),
    /// An RPC failed.
    #[error("RPC failed: {0}")]
    Rpc(#[from] RpcError),
    /// Error decoding alloy abi.
    #[error("Error encoding/decoding abi for sol type: {0}")]
    SolAbi(#[from] alloy::sol_types::Error),
    /// Error with EVM calls.
    #[error("{0}")]
    EVMCustom(String),
    /// Error forwarding executed block to tree.
    #[error("Failed to forward executed block to tree.")]
    TreeChannelClosed,
    /// Executed output must always contain at least one block.
    #[error("Empty execution output from engine.")]
    EmptyExecutionOutput,
}

impl From<TnRethError> for EthApiError {
    fn from(value: TnRethError) -> Self {
        if let TnRethError::RecoverTransactionBytes(e) = value {
            e
        } else {
            EthApiError::EvmCustom(value.to_string())
        }
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for TnRethError {
    fn from(_: std::sync::mpsc::SendError<T>) -> Self {
        Self::TreeChannelClosed
    }
}
