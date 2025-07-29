//! Error types for spawning a full node

use eyre::ErrReport;
use thiserror::Error;
use tn_types::WorkerId;

/// Error types when spawning the ExecutionNode
#[derive(Debug, Error)]
pub enum ExecutionError {
    /// Error creating temp db
    #[error(transparent)]
    Tempdb(#[from] std::io::Error),

    #[error(transparent)]
    Report(#[from] ErrReport),

    /// Worker id is not included in the execution node's known worker hashmap.
    #[error("Worker not found: {0:?}")]
    WorkerNotFound(WorkerId),
}
