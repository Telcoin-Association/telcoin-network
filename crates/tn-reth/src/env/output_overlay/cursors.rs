//! Statically dispatched cursor stacks over geometrically compacted runs.
//!
//! Each outer Reth cursor shadows older layers using Reth's existing deletion and
//! wipe semantics. Boxing the recursive enum keeps its type finite; the number
//! of layers is logarithmic in the number of blocks, without flattening runs.

use super::sorted_runs::SortedTrieRuns;
use reth_db::DatabaseError;
use reth_primitives_traits::Account;
use reth_trie::{
    hashed_cursor::{
        HashedCursor, HashedCursorFactory, HashedPostStateCursor, HashedPostStateCursorValue,
        HashedStorageCursor,
    },
    trie_cursor::{InMemoryTrieCursor, TrieCursor, TrieCursorFactory, TrieStorageCursor},
    BranchNodeCompact, Nibbles,
};
use tn_types::{B256, U256};

/// A database cursor factory with the output's chronological sorted runs above it.
#[derive(Clone)]
pub(super) struct RunCursorFactory<'a, CF> {
    /// Factory for the database layer at the output's anchor.
    inner: CF,
    /// Runs borrowed for the duration of one root computation.
    runs: &'a SortedTrieRuns,
}

impl<'a, CF> RunCursorFactory<'a, CF> {
    /// Layer `runs` over the database factory without merging their contents.
    pub(super) fn new(inner: CF, runs: &'a SortedTrieRuns) -> Self {
        Self { inner, runs }
    }
}

impl<CF: HashedCursorFactory> HashedCursorFactory for RunCursorFactory<'_, CF> {
    type AccountCursor<'a>
        = RunHashedCursor<'a, CF::AccountCursor<'a>, Option<Account>>
    where
        Self: 'a;
    type StorageCursor<'a>
        = RunHashedCursor<'a, CF::StorageCursor<'a>, U256>
    where
        Self: 'a;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor<'_>, DatabaseError> {
        let base = RunHashedCursor::Base(self.inner.hashed_account_cursor()?);
        Ok(self.runs.iter().fold(base, |cursor, run| {
            RunHashedCursor::Layer(Box::new(HashedPostStateCursor::new_account(
                cursor,
                run.state(),
            )))
        }))
    }

    fn hashed_storage_cursor(
        &self,
        address: B256,
    ) -> Result<Self::StorageCursor<'_>, DatabaseError> {
        let base = RunHashedCursor::Base(self.inner.hashed_storage_cursor(address)?);
        Ok(self.runs.iter().fold(base, |cursor, run| {
            RunHashedCursor::Layer(Box::new(HashedPostStateCursor::new_storage(
                cursor,
                run.state(),
                address,
            )))
        }))
    }
}

impl<CF: TrieCursorFactory> TrieCursorFactory for RunCursorFactory<'_, CF> {
    type AccountTrieCursor<'a>
        = RunTrieCursor<'a, CF::AccountTrieCursor<'a>>
    where
        Self: 'a;
    type StorageTrieCursor<'a>
        = RunTrieCursor<'a, CF::StorageTrieCursor<'a>>
    where
        Self: 'a;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
        let base = RunTrieCursor::Base(self.inner.account_trie_cursor()?);
        Ok(self.runs.iter().fold(base, |cursor, run| {
            RunTrieCursor::Layer(Box::new(InMemoryTrieCursor::new_account(cursor, run.nodes())))
        }))
    }

    fn storage_trie_cursor(
        &self,
        address: B256,
    ) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
        let base = RunTrieCursor::Base(self.inner.storage_trie_cursor(address)?);
        Ok(self.runs.iter().fold(base, |cursor, run| {
            RunTrieCursor::Layer(Box::new(InMemoryTrieCursor::new_storage(
                cursor,
                run.nodes(),
                address,
            )))
        }))
    }
}

/// A hashed-state cursor over either the database or one additional sorted run.
pub(super) enum RunHashedCursor<'a, C, V: HashedPostStateCursorValue> {
    /// Database cursor beneath every in-memory run.
    Base(C),
    /// Newer state overlay, delegating tombstones and wipes to Reth.
    Layer(Box<HashedPostStateCursor<'a, Self, V>>),
}

impl<C, V> HashedCursor for RunHashedCursor<'_, C, V>
where
    C: HashedCursor<Value = V::NonZero>,
    V: HashedPostStateCursorValue,
{
    type Value = V::NonZero;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.seek(key),
            Self::Layer(cursor) => cursor.seek(key),
        }
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.next(),
            Self::Layer(cursor) => cursor.next(),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Base(cursor) => cursor.reset(),
            Self::Layer(cursor) => cursor.reset(),
        }
    }
}

impl<C: HashedStorageCursor<Value = U256>> HashedStorageCursor for RunHashedCursor<'_, C, U256> {
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.is_storage_empty(),
            Self::Layer(cursor) => cursor.is_storage_empty(),
        }
    }

    fn set_hashed_address(&mut self, address: B256) {
        match self {
            Self::Base(cursor) => cursor.set_hashed_address(address),
            Self::Layer(cursor) => cursor.set_hashed_address(address),
        }
    }
}

/// A trie-node cursor over either the database or one additional sorted run.
pub(super) enum RunTrieCursor<'a, C> {
    /// Database cursor beneath every in-memory run.
    Base(C),
    /// Newer trie overlay, delegating removals and deleted tries to Reth.
    Layer(Box<InMemoryTrieCursor<'a, Self>>),
}

impl<C: TrieCursor> TrieCursor for RunTrieCursor<'_, C> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.seek_exact(key),
            Self::Layer(cursor) => cursor.seek_exact(key),
        }
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.seek(key),
            Self::Layer(cursor) => cursor.seek(key),
        }
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.next(),
            Self::Layer(cursor) => cursor.next(),
        }
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        match self {
            Self::Base(cursor) => cursor.current(),
            Self::Layer(cursor) => cursor.current(),
        }
    }

    fn reset(&mut self) {
        match self {
            Self::Base(cursor) => cursor.reset(),
            Self::Layer(cursor) => cursor.reset(),
        }
    }
}

impl<C: TrieStorageCursor> TrieStorageCursor for RunTrieCursor<'_, C> {
    fn set_hashed_address(&mut self, address: B256) {
        match self {
            Self::Base(cursor) => cursor.set_hashed_address(address),
            Self::Layer(cursor) => cursor.set_hashed_address(address),
        }
    }
}
