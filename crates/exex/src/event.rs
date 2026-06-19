//! Events sent from ExExes back to the manager.

use tn_types::BlockNumber;

/// Event sent from an ExEx back to the manager.
///
/// ExExes use these events to report processing progress back to the node.
#[derive(Debug, Clone)]
pub enum TnExExEvent {
    /// Signal that the ExEx has durably processed all blocks up to this height.
    ///
    /// The manager aggregates these into a minimum finished height across all
    /// ExExes, exposed via
    /// [`TnExExManagerHandle::min_finished_height`](crate::TnExExManagerHandle::min_finished_height).
    ///
    /// This is the coordination point for future pruning support: a pruner would
    /// avoid removing data below this height so ExExes can still catch up via
    /// replay. **It is currently informational only** — TN runs in archive mode
    /// (pruning disabled), so nothing consumes it yet.
    FinishedHeight(BlockNumber),
}
