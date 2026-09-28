//! Preserve Reth's flattened storage-emptiness predicate over the complete cursor stack.
//!
//! Seeking through layered cursors already respects newer zero values, but asking each
//! layer whether it contains non-zero storage does not. Inspect all overlay layers in
//! precedence order before consulting the raw database cursor.
//! The scan takes expected linear time in the account's overlay slots, with scratch
//! space for distinct slot keys, and stops at the first visible non-zero slot or wipe.

use super::sorted_runs::SortedTrieRuns;
use reth_db::DatabaseError;
use reth_trie::{
    hashed_cursor::{HashedCursor, HashedCursorFactory, HashedStorageCursor},
    HashedPostStateSorted,
};
use std::collections::HashSet;
use tn_types::{B256, U256};

/// Adapt the complete hashed-state stack, including the current block, to Reth's predicate.
#[derive(Clone)]
pub(super) struct OverlayCursorFactory<'a, CF, DF> {
    /// Complete cursor stack, used unchanged for account and storage traversal.
    layered: CF,
    /// Raw database factory, beneath every run and the current block.
    database: DF,
    /// Chronological runs whose slot values may be shadowed by newer layers.
    runs: &'a SortedTrieRuns,
    /// Current block's sorted deltas, with precedence over every accumulated run.
    current: &'a HashedPostStateSorted,
}

impl<'a, CF, DF> OverlayCursorFactory<'a, CF, DF> {
    /// Wrap the complete stack while retaining independent access to its raw database.
    pub(super) fn new(
        layered: CF,
        database: DF,
        runs: &'a SortedTrieRuns,
        current: &'a HashedPostStateSorted,
    ) -> Self {
        Self { layered, database, runs, current }
    }
}

impl<CF: HashedCursorFactory, DF: HashedCursorFactory> HashedCursorFactory
    for OverlayCursorFactory<'_, CF, DF>
{
    type AccountCursor<'a>
        = CF::AccountCursor<'a>
    where
        Self: 'a;
    type StorageCursor<'a>
        = OverlayStorageCursor<'a, CF::StorageCursor<'a>, DF::StorageCursor<'a>>
    where
        Self: 'a;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor<'_>, DatabaseError> {
        self.layered.hashed_account_cursor()
    }

    fn hashed_storage_cursor(
        &self,
        address: B256,
    ) -> Result<Self::StorageCursor<'_>, DatabaseError> {
        Ok(OverlayStorageCursor {
            layered: self.layered.hashed_storage_cursor(address)?,
            database: self.database.hashed_storage_cursor(address)?,
            runs: self.runs,
            current: self.current,
            address,
        })
    }
}

/// A layered storage cursor with an independent view of the raw database's emptiness.
pub(super) struct OverlayStorageCursor<'a, C, D> {
    /// Complete layered cursor, preserving the existing seek and next semantics.
    layered: C,
    /// Raw database cursor used only when no overlay value or wipe decides emptiness.
    database: D,
    /// Accumulated sorted runs, borrowed throughout the root computation.
    runs: &'a SortedTrieRuns,
    /// Current block's deltas, including zeros that shadow older non-zero slots.
    current: &'a HashedPostStateSorted,
    /// Account selected by construction or the most recent address switch.
    address: B256,
}

impl<C: HashedCursor<Value = U256>, D> HashedCursor for OverlayStorageCursor<'_, C, D> {
    type Value = U256;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, U256)>, DatabaseError> {
        self.layered.seek(key)
    }

    fn next(&mut self) -> Result<Option<(B256, U256)>, DatabaseError> {
        self.layered.next()
    }

    fn reset(&mut self) {
        self.layered.reset();
    }
}

impl<C: HashedStorageCursor<Value = U256>, D: HashedStorageCursor<Value = U256>> HashedStorageCursor
    for OverlayStorageCursor<'_, C, D>
{
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        let mut seen = HashSet::new();
        let overlay_empty = std::iter::once(self.current)
            .chain(self.runs.iter().rev().map(|run| run.state()))
            .filter_map(|state| state.storages.get(&self.address))
            .find_map(|storage| {
                // Record zeros too: the newest value must shadow every older value of a slot.
                if storage
                    .storage_slots
                    .iter()
                    .any(|(slot, value)| seen.insert(*slot) && !value.is_zero())
                {
                    Some(false)
                } else {
                    // A wipe cuts off older runs and the database, but keeps newer writes.
                    storage.wiped.then_some(true)
                }
            });

        // Reth reports non-empty for any raw DB row, even one zeroed by the overlay.
        // A seek on the composed cursor would therefore not be an equivalent predicate.
        overlay_empty.map_or_else(|| self.database.is_storage_empty(), Ok)
    }

    fn set_hashed_address(&mut self, address: B256) {
        self.layered.set_hashed_address(address);
        self.database.set_hashed_address(address);
        self.address = address;
    }
}
