//! This is an experimental approach to supporting pending blocks for workers.

use reth_primitives::Address;
use reth_provider::ExecutionOutcome;

/// The type that holds the worker's pending block.
///
/// The value is stored in memory and updated each time a worker's batch maker broadcasts a new batch.
pub struct PendingWorkerBlock {
    /// The state from the worker's newest batch.
    state: Option<ExecutionOutcome>,
}

impl PendingWorkerBlock {
    /// Create a new instance of [Self].
    pub fn new() -> Self {
        Self { state: None }
    }

    /// Return data for worker's current pending block.
    pub fn latest(&self) -> Option<ExecutionOutcome> {
        self.state.as_ref().map(|d| d.clone())
    }

    /// Return the account nonce if it exists.
    ///
    /// Based on reth's `StateProvider::account_nonce`.
    pub fn account_nonce(&self, address: &Address) -> Option<u64> {
        // check the execution output for a particular account's nonce
        self.state
            .as_ref()
            .map(|s| s.account(address))
            .unwrap_or_default()
            .unwrap_or_default()
            .map(|a| a.nonce)
    }
}
