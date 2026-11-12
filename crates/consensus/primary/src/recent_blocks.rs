//! Track the most recent execution blocks for the consensus layer.

use std::collections::VecDeque;
use tn_types::{
    BlockNumHash, BlockNumber, CanonicalExecutionReader, ConsensusHeaderDigest, ConsensusNumHash,
    Round, SealedHeader,
};

/// Tracks 'num_blocks' most recently executed block hashes and numbers.
#[derive(Clone, Debug)]
pub struct RecentBlocks {
    /// The last round of consensus processed by execution engine.
    last_consensus_round: Round,
    num_blocks: usize,
    executed_blocks: VecDeque<SealedHeader>,
    /// Recent consensus output number/hash (both skipped and executed).
    /// Used by `contains_consensus` to resolve wait_for_consensus_execution.
    recent_consensus_num_hashes: VecDeque<ConsensusNumHash>,
}

impl RecentBlocks {
    /// Create a RecentBlocks that will hold 'num_blocks' most recent blocks.
    pub fn new(num_blocks: usize) -> Self {
        Self {
            last_consensus_round: 0,
            num_blocks,
            executed_blocks: VecDeque::new(),
            recent_consensus_num_hashes: VecDeque::new(),
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
        consensus_num_hash: ConsensusNumHash,
        latest_canonical_tip: Option<SealedHeader>,
    ) {
        self.last_consensus_round = latest_consensus_round;

        // Always track the consensus number/hash (skipped or not)
        if self.recent_consensus_num_hashes.len() >= self.num_blocks {
            self.recent_consensus_num_hashes.pop_front();
        }
        self.recent_consensus_num_hashes.push_back(consensus_num_hash);

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
    pub fn latest_execution_block_num_hash(&self) -> BlockNumHash {
        self.executed_blocks.back().cloned().unwrap_or_else(Default::default).num_hash()
    }

    /// Return the hash and number of the last executed block.
    pub fn latest_execution_block(&self) -> SealedHeader {
        self.executed_blocks.back().cloned().unwrap_or_else(Default::default)
    }

    /// Is `block` a recent block we have executed?
    ///
    /// The number and the hash must both agree. A header names its execution block by both, and a
    /// hash this node executed at another height is not that block: matching the hash alone lets
    /// a header claim block `n` with the hash of an older block `k` and pass on every node that
    /// still holds `k`.
    pub fn contains_execution_block(&self, block: BlockNumHash) -> bool {
        self.executed_blocks.iter().any(|executed| executed.num_hash() == block)
    }

    /// The ring's part of the execution check (see [`ExecutionCheck`]).
    ///
    /// `Some(NotYet)` while nothing is executed or the executed tip is below `block`'s number,
    /// `Some(Match)` when the ring holds `block`, and `None` for a miss at or below the tip. The
    /// ring keeps only the final block of each recent output, so only the canonical database can
    /// decide a miss (see [`ExecutionCheck::from_canonical`]).
    pub fn check_execution_block(&self, block: BlockNumHash) -> Option<ExecutionCheck> {
        if self.is_empty() || self.latest_execution_block_num_hash().number < block.number {
            Some(ExecutionCheck::NotYet)
        } else if self.contains_execution_block(block) {
            Some(ExecutionCheck::Match)
        } else {
            None
        }
    }

    /// Is hash (consensus output) in a recent block we have executed?
    pub fn contains_consensus(&self, hash: ConsensusHeaderDigest) -> bool {
        // Check dedicated consensus num/hash tracker (covers both skipped and executed rounds)
        if self.recent_consensus_num_hashes.iter().any(|num_hash| num_hash.hash == hash) {
            return true;
        }
        // Fallback: check block's parent_beacon_block_root
        for block in &self.executed_blocks {
            if block.parent_beacon_block_root == Some(hash.into()) {
                return true;
            }
        }
        false
    }

    /// Return the number/hash of the latest processed consensus output.
    pub fn latest_consensus_block_num_hash(&self) -> ConsensusNumHash {
        self.recent_consensus_num_hashes.back().copied().unwrap_or_else(Default::default)
    }

    /// Do we have any blocks?
    pub fn is_empty(&self) -> bool {
        self.executed_blocks.is_empty()
    }
}

/// The verdict of checking an execution block a header names against this node's execution.
///
/// One check serves every caller: the voter before it votes, the subscriber before it assembles a
/// committed output and state-sync before it hands a fetched output to execution. Only
/// [`Self::Fork`] is evidence of divergence. [`Self::NotYet`] and [`Self::Unconfirmed`] are asked
/// again later, and [`Self::BelowFloor`] cannot be decided on this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionCheck {
    /// This node executed the block: the number and the hash both agree.
    Match,
    /// Nothing is executed yet, or the executed tip is below the block's number: wait for
    /// execution.
    NotYet,
    /// The executed tip has reached the number, but the canonical database could not answer for
    /// it: the block is not persisted yet or the read failed. Not a fork; ask again.
    Unconfirmed,
    /// The number is below the earliest block this node holds. A snapshot-restored datadir has
    /// only scaffold headers under its restored-state floor, so it can neither confirm nor refute
    /// the block. Not a fork.
    BelowFloor {
        /// The node's restored-state floor.
        floor: BlockNumber,
    },
    /// The number is at or below the executed tip, and this node holds a different block at that
    /// number: execution has forked.
    Fork,
}

impl ExecutionCheck {
    /// The canonical database's part of the check, for a ring miss at or below the executed tip
    /// (see [`RecentBlocks::check_execution_block`]).
    ///
    /// The floor is the lower bound: a number below it is [`Self::BelowFloor`] and never reaches
    /// the hash comparison. `None` from the database (not persisted yet, or a read error) is
    /// [`Self::Unconfirmed`]. Without a reader (tests and tools that wire no engine) the ring is
    /// the only evidence, so a miss is a fork.
    pub fn from_canonical(
        block: BlockNumHash,
        reader: Option<&dyn CanonicalExecutionReader>,
    ) -> Self {
        let Some(reader) = reader else {
            return Self::Fork;
        };
        if let Some(floor) = reader.restored_state_floor().filter(|floor| block.number < *floor) {
            return Self::BelowFloor { floor };
        }
        match reader.canonical_execution_hash(block.number) {
            Some(hash) if hash == block.hash => Self::Match,
            Some(_) => Self::Fork,
            None => Self::Unconfirmed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExecutionCheck, RecentBlocks};
    use tn_types::{
        BlockHash, BlockNumHash, ConsensusHeaderDigest, ConsensusNumHash, ExecHeader, SealedHeader,
    };

    #[test]
    fn tracks_latest_consensus_num_hash_for_skipped_rounds() {
        let mut recent = RecentBlocks::new(4);
        let consensus_num_hash = ConsensusNumHash::new(10, ConsensusHeaderDigest::from([1u8; 32]));

        recent.push_latest(1, consensus_num_hash, None);

        assert_eq!(recent.latest_consensus_block_num_hash(), consensus_num_hash);
        assert!(recent.contains_consensus(consensus_num_hash.hash));
        assert_eq!(recent.latest_execution_block_num_hash().number, 0);
    }

    #[test]
    fn tracks_latest_execution_and_consensus_after_execution() {
        let mut recent = RecentBlocks::new(4);
        let consensus_num_hash = ConsensusNumHash::new(11, ConsensusHeaderDigest::from([2u8; 32]));
        let executed = SealedHeader::seal_slow(ExecHeader { number: 7, ..Default::default() });

        recent.push_latest(2, consensus_num_hash, Some(executed.clone()));

        assert_eq!(recent.latest_consensus_block_num_hash(), consensus_num_hash);
        assert_eq!(recent.latest_execution_block_num_hash(), executed.num_hash());
        assert!(recent.contains_execution_block(executed.num_hash()));
    }

    /// SEC-1: the ring holds a block by its number and its hash. The same hash at another number
    /// is not a match and is left to the canonical database (`None`); a number above the tip is
    /// not checkable yet.
    #[test]
    fn ring_lookup_compares_number_and_hash() {
        let mut recent = RecentBlocks::new(4);
        assert_eq!(
            recent.check_execution_block(BlockNumHash::new(0, BlockHash::ZERO)),
            Some(ExecutionCheck::NotYet),
            "nothing executed yet"
        );
        let consensus_num_hash = ConsensusNumHash::new(1, ConsensusHeaderDigest::from([3u8; 32]));
        let older = SealedHeader::seal_slow(ExecHeader { number: 7, ..Default::default() });
        let tip = SealedHeader::seal_slow(ExecHeader { number: 9, ..Default::default() });
        recent.push_latest(1, consensus_num_hash, Some(older.clone()));
        recent.push_latest(2, consensus_num_hash, Some(tip));

        assert!(recent.contains_execution_block(older.num_hash()));
        let same_hash_other_number = BlockNumHash::new(8, older.hash());
        assert!(!recent.contains_execution_block(same_hash_other_number));
        assert_eq!(recent.check_execution_block(older.num_hash()), Some(ExecutionCheck::Match));
        assert_eq!(recent.check_execution_block(same_hash_other_number), None);
        assert_eq!(
            recent.check_execution_block(BlockNumHash::new(10, older.hash())),
            Some(ExecutionCheck::NotYet)
        );
    }
}
