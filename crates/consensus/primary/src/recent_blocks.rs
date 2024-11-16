// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Track the most recent execution blocks for the consensus layer.

use std::collections::VecDeque;

use reth_primitives::BlockNumHash;

#[derive(Clone, Debug)]
pub struct RecentBlocks {
    num_blocks: usize,
    blocks: VecDeque<BlockNumHash>,
}

impl RecentBlocks {
    pub fn new(num_blocks: usize) -> Self {
        Self { num_blocks, blocks: VecDeque::new() }
    }

    pub fn push_latest(&mut self, latest: BlockNumHash) {
        if self.blocks.len() >= self.num_blocks {
            self.blocks.pop_front();
        }
        self.blocks.push_back(latest);
    }

    pub fn latest_block(&self) -> BlockNumHash {
        self.blocks.back().cloned().unwrap_or_else(Default::default)
    }
}
