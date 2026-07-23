//! Error type to wrap various Reth errors.

use alloy::primitives::Bytes;
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
    /// Error forwarding engine update to consensus.
    #[error("Failed to forward engine update to consensus.")]
    EngineUpdateChannelClosed,
    /// Executed output must always contain at least one block.
    #[error("Empty execution output from engine.")]
    EmptyExecutionOutput,
    /// Receipts are missing for a block that exists (DB inconsistency).
    ///
    /// Surfaced during ExEx replay: returning an empty receipt set would make a
    /// non-empty block look empty to a stateful indexer (silent corruption), so
    /// it is treated as an error instead.
    #[error("receipts not found for existing block {0} during replay")]
    ReplayReceiptsMissing(u64),
    /// Failed to recover a transaction's signer from its signature.
    #[error(transparent)]
    SignerRecovery(#[from] alloy::consensus::crypto::RecoveryError),
    /// The persisted finalized marker points past the persisted canonical tip.
    ///
    /// The marker only ever trails or equals the tip (it commits atomically with the
    /// blocks; pre-fix versions committed the blocks first), so a marker ahead of the
    /// tip means the execution database lost blocks this node already attested as
    /// final. Surfaced by the startup heal (`RethEnv::heal_finalized_to_persisted_tip`)
    /// to refuse startup instead of silently re-executing past attested state.
    #[error(
        "finalized marker {finalized} is ahead of the persisted canonical tip {tip}: \
         execution db is behind a marker this node already attested; refusing to start"
    )]
    FinalizedMarkerAheadOfTip {
        /// The persisted finalized-block marker.
        finalized: u64,
        /// The persisted canonical tip.
        tip: u64,
    },
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

/// Failure executing a read-only on-chain EVM call.
///
/// Distinguishes a user-triggerable on-chain revert (revert bytes surfaced to RPC
/// clients eth_call-style) from internal node failures (never leaked to clients).
#[derive(Debug, thiserror::Error)]
pub enum EvmReadError {
    /// The EVM call reverted on-chain.
    #[error("execution reverted{}", .reason.as_ref().map(|r| format!(": {r}")).unwrap_or_default())]
    Revert {
        /// Raw ABI-encoded revert bytes.
        output: Bytes,
        /// Decoded human-readable reason, if available.
        reason: Option<String>,
    },
    /// Non-revert failure: state provider/DB, EVM construction, Halt, ABI decode.
    #[error("{0}")]
    Internal(String),
}

/// Result of a read-only on-chain EVM call.
///
/// The error is always an [`EvmReadError`], so consumers (e.g. the RPC layer) can match
/// revert-vs-internal directly instead of recovering it via a runtime downcast. Every non-revert
/// failure — state provider/EVM setup, `Halt`, ABI decode — is an [`EvmReadError::Internal`].
pub type EvmReadResult<T> = Result<T, EvmReadError>;

/// Failure reading system-contract state at a pinned block, classified by whether the failure is
/// deterministic across the committee.
///
/// [`Provider`](Self::Provider) is a node-local storage/provider fault: the pinned header not
/// resolving in this node's database, state-provider construction failing, or a database error
/// surfaced while the EVM lazily reads accounts/storage during the call. Another committee member
/// issuing the same read at the same block may succeed, so consensus-critical callers must NOT
/// assume peers share the failure — proceeding on stale values there can diverge from the
/// committee (retry or halt instead).
///
/// [`ChainGlobal`](Self::ChainGlobal) is a deterministic product of the pinned chain state and the
/// node's own code: contract absent at the block, on-chain revert or halt, ABI decode failure,
/// arity mismatch, or EVM environment construction. Every node reading the same block observes the
/// identical failure, so a keep-current fail-open policy stays committee-consistent.
#[derive(Debug, thiserror::Error)]
pub enum StateReadError {
    /// Node-local storage/provider fault; NOT committee-deterministic.
    #[error("provider fault reading pinned chain state: {0}")]
    Provider(String),
    /// Deterministic product of the pinned chain state; identical on every node.
    #[error("{0}")]
    ChainGlobal(String),
}

/// Result of a committee-determinism-classified state read (see [`StateReadError`]).
pub type StateReadResult<T> = Result<T, StateReadError>;
