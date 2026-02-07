//! Track the most recent execution blocks for the consensus layer.

use std::collections::VecDeque;
use tn_types::{BlockHash, BlockNumHash, Round, SealedHeader, B256};

/// Tracks 'num_blocks' most recently executed block hashes and numbers.
#[derive(Clone, Debug)]
pub struct RecentBlocks {
    /// The last round of consensus processed by execution engine.
    last_consensus_round: Round,
    num_blocks: usize,
    executed_blocks: VecDeque<SealedHeader>,
    /// Recent consensus output hashes (both skipped and executed).
    /// Used by `contains_consensus` to resolve wait_for_consensus_execution.
    recent_consensus_hashes: VecDeque<B256>,
}

impl RecentBlocks {
    /// Create a RecentBlocks that will hold 'num_blocks' most recent blocks.
    pub fn new(num_blocks: usize) -> Self {
        Self {
            last_consensus_round: 0,
            num_blocks,
            executed_blocks: VecDeque::new(),
            recent_consensus_hashes: VecDeque::new(),
        }
    }

    /// Max number of blocks that can be held in RecentBlocks.
    pub fn block_capacity(&self) -> u64 {
        self.num_blocks as u64
    }

    /// Push the latest consensus output result onto RecentBlocks.
    ///
    /// Always tracks the consensus hash (for both skipped and executed rounds).
    /// Only pushes a block when `latest_canonical_tip` is `Some`.
    pub fn push_latest(
        &mut self,
        latest_consensus_round: Round,
        consensus_hash: B256,
        latest_canonical_tip: Option<SealedHeader>,
    ) {
        self.last_consensus_round = latest_consensus_round;

        // Always track the consensus hash (skipped or not)
        if self.recent_consensus_hashes.len() >= self.num_blocks {
            self.recent_consensus_hashes.pop_front();
        }
        self.recent_consensus_hashes.push_back(consensus_hash);

        if let Some(tip) = latest_canonical_tip {
            if self.executed_blocks.len() >= self.num_blocks {
                self.executed_blocks.pop_front();
            }
            self.executed_blocks.push_back(tip);
        }
    }

    /// The last consensus round processed by the engine.
    pub fn last_consensus_round(&self) -> Round {
        self.last_consensus_round
    }

    /// Return the hash and number of the last executed block.
    /// This will return a default BlockNumHash if recents blocks are empty.
    /// This should only happen on node startup before any execution has taken
    /// place.  Most callsites will be fine with this, call is_empty() if this
    /// matters to you.
    pub fn latest_block_num_hash(&self) -> BlockNumHash {
        self.executed_blocks.back().cloned().unwrap_or_else(Default::default).num_hash()
    }

    /// Return the hash and number of the last executed block.
    pub fn latest_block(&self) -> SealedHeader {
        self.executed_blocks.back().cloned().unwrap_or_else(Default::default)
    }

    /// Is hash a recent block we have executed?
    pub fn contains_hash(&self, hash: BlockHash) -> bool {
        for block in &self.executed_blocks {
            if block.hash() == hash {
                return true;
            }
        }
        false
    }

    /// Is hash (consensus output) in a recent block we have executed?
    pub fn contains_consensus(&self, hash: BlockHash) -> bool {
        // Check dedicated consensus hash tracker (covers both skipped and executed rounds)
        if self.recent_consensus_hashes.contains(&hash) {
            return true;
        }
        // Fallback: check block's parent_beacon_block_root
        for block in &self.executed_blocks {
            if block.parent_beacon_block_root == Some(hash) {
                return true;
            }
        }
        false
    }

    /// Number of blocks actually stored.
    pub fn len(&self) -> usize {
        self.executed_blocks.len()
    }

    /// Do we have any blocks?
    pub fn is_empty(&self) -> bool {
        self.executed_blocks.is_empty()
    }
}
