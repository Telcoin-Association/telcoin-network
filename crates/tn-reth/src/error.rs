//! Error type to wrap various Reth errors.

use reth::rpc::{builder::error::RpcError, server_types::eth::EthApiError};
use reth_blockchain_tree::error::InsertBlockError;
use reth_errors::{BlockExecutionError, CanonicalError};
use reth_provider::ProviderError;
use reth_revm::primitives::EVMError;

/// Result alias for [`TNRethError`].
pub type TnRethResult<T> = Result<T, TnRethError>;

/// Core error variants when executing the output from consensus and extending the canonical block.
#[derive(Debug, thiserror::Error)]
pub enum TnRethError {
    /// Error retrieving data from Provider.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// Error during EVM execution.
    #[error("evm execution error: {0}")]
    EvmExecution(#[from] EVMError<ProviderError>),
    /// Error recovering transaction from bytes.
    #[error(transparent)]
    RecoverTransactionBytes(#[from] EthApiError),
    /// The block could not be inserted into the tree.
    #[error(transparent)]
    InsertNextCanonicalBlock(#[from] InsertBlockError),
    /// The executed block failed to become part of the canonical chain.
    #[error("Blockchain tree failed to make_canonical: {0}")]
    Canonical(#[from] CanonicalError),
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
