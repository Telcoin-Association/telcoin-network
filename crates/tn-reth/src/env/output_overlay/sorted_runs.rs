//! Chronological sorted runs compacted like carries in a binary counter.

use reth_trie::{updates::TrieUpdatesSorted, HashedPostStateSorted};
use std::sync::Arc;

/// The logarithm of the number of consecutive block deltas represented by a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RunLevel(usize);

/// Sorted deltas for a consecutive interval of blocks, with later writes winning.
#[derive(Debug)]
pub(super) struct TrieRun {
    /// Compaction level, independent of how many distinct keys survive overwrites.
    level: RunLevel,
    /// Hashed state, including account tombstones and storage wipes.
    state: Arc<HashedPostStateSorted>,
    /// Trie nodes, including removals and deleted storage tries.
    nodes: Arc<TrieUpdatesSorted>,
}

impl TrieRun {
    /// Read this run's state without flattening the other runs.
    pub(super) fn state(&self) -> &HashedPostStateSorted {
        &self.state
    }

    /// Read this run's trie nodes without flattening the other runs.
    pub(super) fn nodes(&self) -> &TrieUpdatesSorted {
        &self.nodes
    }

    /// Compact equal-level adjacent runs, retaining the newer run's precedence.
    fn merge(mut self, newer: Self) -> Self {
        Arc::make_mut(&mut self.state).extend_ref_and_sort(&newer.state);
        Arc::make_mut(&mut self.nodes).extend_ref_and_sort(&newer.nodes);
        self.level = RunLevel(self.level.0 + 1);
        self
    }
}

/// At most one run per level, ordered oldest to newest and largest to smallest.
///
/// Only equal-level adjacent runs merge. A block's entries participate in at most
/// `log2(N)` compactions across `N` blocks, bounding merge work by `O(N M log N)`
/// for deltas of at most `M` entries. Roots borrow the runs directly.
#[derive(Debug, Default)]
pub(super) struct SortedTrieRuns {
    /// Chronological runs with strictly decreasing compaction levels.
    runs: Vec<TrieRun>,
}

impl SortedTrieRuns {
    /// Borrow the runs in precedence order, oldest first.
    pub(super) fn iter(&self) -> impl Iterator<Item = &TrieRun> {
        self.runs.iter()
    }

    /// Incorporate one block, merging only the consecutive occupied lowest levels.
    ///
    /// The original block's `Arc`s remain immutable. A merge may copy its oldest
    /// input on first mutation, and Reth allocates a replacement vector for a
    /// general sorted merge. Geometric compaction bounds that cumulative work.
    pub(super) fn extend(
        &mut self,
        state: Arc<HashedPostStateSorted>,
        nodes: Arc<TrieUpdatesSorted>,
    ) {
        let carry = TrieRun { level: RunLevel(0), state, nodes };
        let merge_count = self
            .runs
            .iter()
            .rev()
            .enumerate()
            .take_while(|(level, run)| run.level == RunLevel(*level))
            .count();
        let retained = self.runs.len().saturating_sub(merge_count);
        let merged =
            self.runs.drain(retained..).rev().fold(carry, |newer, older| older.merge(newer));
        self.runs.push(merged);
    }
}
