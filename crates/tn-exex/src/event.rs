// SPDX-License-Identifier: MIT OR Apache-2.0
//! Events sent from ExEx tasks back to the manager for coordination.

use tn_types::BlockNumHash;

/// Events that ExEx tasks send back to the manager.
///
/// These events are used for backpressure management and coordination between the execution
/// engine and ExEx tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TnExExEvent {
    /// The ExEx has finished processing up to and including this block.
    ///
    /// This event serves two purposes:
    ///
    /// 1. **Backpressure**: The manager uses this to track how far behind each ExEx is.
    ///    If an ExEx falls too far behind, the manager can apply backpressure to the
    ///    execution engine to prevent unbounded memory growth.
    ///
    /// 2. **Pruning Hints** (future): In a future version, this could inform the storage
    ///    layer that data before this height is no longer needed by this ExEx, enabling
    ///    safe pruning of historical state.
    ///
    /// # Note
    ///
    /// The block height reported here should be the last block that the ExEx has
    /// **completely processed**. Do not report a height until all processing for that
    /// block has been committed to persistent storage or completed in a way that
    /// guarantees durability.
    FinishedHeight(BlockNumHash),
}

impl TnExExEvent {
    /// Creates a `FinishedHeight` event for the given block.
    ///
    /// # Arguments
    ///
    /// * `block` - The block number and hash of the last fully processed block
    ///
    /// # Example
    ///
    /// ```no_run
    /// use tn_exex::TnExExEvent;
    /// use tn_types::BlockNumHash;
    ///
    /// let block = BlockNumHash::new(100, Default::default());
    /// let event = TnExExEvent::finished_height(block);
    /// ```
    pub fn finished_height(block: BlockNumHash) -> Self {
        Self::FinishedHeight(block)
    }

    /// Returns the block height from this event.
    pub fn block(&self) -> BlockNumHash {
        match self {
            Self::FinishedHeight(block) => *block,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::B256;

    #[test]
    fn test_finished_height_constructor() {
        let block = BlockNumHash::new(42, B256::from([1u8; 32]));
        let event = TnExExEvent::finished_height(block);

        assert_eq!(event, TnExExEvent::FinishedHeight(block));
    }

    #[test]
    fn test_block_accessor() {
        let block = BlockNumHash::new(100, B256::from([2u8; 32]));
        let event = TnExExEvent::FinishedHeight(block);

        assert_eq!(event.block(), block);
    }
}
