//! Events sent from ExExes back to the manager.

use tn_types::BlockNumber;

/// Event sent from an ExEx back to the manager.
///
/// ExExes use these events to communicate processing progress back to the
/// node, enabling coordination for data pruning.
#[derive(Debug, Clone)]
pub enum TnExExEvent {
    /// Signal that the ExEx has durably processed all blocks up to this height.
    ///
    /// Used for pruning coordination — the node won't prune data below the
    /// minimum finished height across all ExExes.
    FinishedHeight(BlockNumber),
}
