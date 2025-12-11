//! Track the most recent execution blocks for the consensus layer.

use std::collections::VecDeque;
use tn_types::{BlockHash, BlockNumHash, SealedHeader};

/// Tracks 'num_blocks' most recently executed block hashes and numbers.
#[derive(Clone, Debug)]
pub struct RecentBlocks {
    num_blocks: usize,
    blocks: VecDeque<SealedHeader>,
}

impl RecentBlocks {
    /// Create a RecentBlocks that will hold 'num_blocks' most recent blocks.
    pub fn new(num_blocks: usize) -> Self {
        Self { num_blocks, blocks: VecDeque::new() }
    }

    /// Max number of blocks that can be held in RecentBlocks.
    pub fn block_capacity(&self) -> u64 {
        self.num_blocks as u64
    }

    /// Push the latest block onto RecentBlocks, will remove the oldest if needed to make room.
    pub fn push_latest(&mut self, latest: SealedHeader) {
        if self.blocks.len() >= self.num_blocks {
            self.blocks.pop_front();
        }
        self.blocks.push_back(latest);
    }

    /// Return the hash and number of the last executed block.
    /// This will return a default BlockNumHash if recents blocks are empty.
    /// This should only happen on node startup before any execution has taken
    /// place.  Most callsites will be fine with this, call is_empty() if this
    /// matters to you.
    pub fn latest_block_num_hash(&self) -> BlockNumHash {
        self.blocks.back().cloned().unwrap_or_else(Default::default).num_hash()
    }

    /// Return the hash and number of the last executed block.
    pub fn latest_block(&self) -> SealedHeader {
        self.blocks.back().cloned().unwrap_or_else(Default::default)
    }

    /// Is hash a recent block we have executed?
    pub fn contains_hash(&self, hash: BlockHash) -> bool {
        for block in &self.blocks {
            if block.hash() == hash {
                return true;
            }
        }
        false
    }

    /// Is hash (consensus output) in a recent block we have executed?
    pub fn contains_consensus(&self, hash: BlockHash) -> bool {
        for block in &self.blocks {
            if block.parent_beacon_block_root == Some(hash) {
                return true;
            }
        }
        false
    }

    /// Number of blocks actually stored.
    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    /// Do we have any blocks?
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }
}
