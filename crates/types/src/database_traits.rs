//! Database traits for compatibility.

use serde::{de::DeserializeOwned, Serialize};
use std::{borrow::Borrow, fmt::Debug, future::Future};

pub trait KeyT: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static {}
pub trait ValueT: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static {}

impl<K: Serialize + DeserializeOwned + Send + Sync + Ord + Clone + Debug + 'static> KeyT for K {}
impl<V: Serialize + DeserializeOwned + Send + Sync + Clone + Debug + 'static> ValueT for V {}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TableHint {
    Epoch,
    Kad,
    Cache,
}

pub trait Table: Send + Sync + Debug + 'static {
    type Key: KeyT;
    type Value: ValueT;

    const NAME: &'static str;
    const HINT: TableHint;
}

/// Interface to a DB read transaction.
pub trait DbTx {
    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        Ok(self.get::<T>(key)?.is_some())
    }
}

/// Interface to a DB write transaction.
pub trait DbTxMut: DbTx {
    /// Insert the given key/value into the table.
    /// If key already exists it should replace it.
    fn insert<T: Table>(&mut self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    fn remove<T: Table>(&mut self, key: &T::Key) -> eyre::Result<()>;

    /// Removes every key-value pair from the table.
    fn clear_table<T: Table>(&mut self) -> eyre::Result<()>;

    /// Commit data to durable storage.
    fn commit(self) -> eyre::Result<()>;
}

pub type DBIter<'i, T> = Box<dyn Iterator<Item = (<T as Table>::Key, <T as Table>::Value)> + 'i>;

pub trait Database: Send + Sync + Clone + Unpin + 'static {
    type TX<'txn>: DbTx + Send + Debug + 'txn
    where
        Self: 'txn;
    type TXMut<'txn>: DbTxMut + Send + Debug + 'txn
    where
        Self: 'txn;

    /// Open a new database table.
    fn open_table<T: Table>(&self) -> eyre::Result<()>;

    /// Return a read txn object.
    fn read_txn(&self) -> eyre::Result<Self::TX<'_>>;

    /// Return a write txn object.
    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>>;

    /// Returns true if the map contains a value for the specified key.
    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool>;

    /// Returns the value for the given key from the map, if it exists.
    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>>;

    /// Inserts the given key-value pair into the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// inserts.
    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()>;

    /// Removes the entry for the given key from the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// removes.
    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()>;

    /// Removes every key-value pair from the map.
    /// This will create and commit a TXN, useful for one-offs but use a transaction for multiple
    /// table clears.
    fn clear_table<T: Table>(&self) -> eyre::Result<()>;

    /// Returns true if the map is empty, otherwise false.
    fn is_empty<T: Table>(&self) -> bool;

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// If this is backed by storage an underlying error will most likely end the iterator early.
    fn iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>>;

    /// Iterates over all the keys in reverse.
    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T>;

    /// Returns the record prior to key if it exists or the first record that is sorted before if it
    /// does not exist.
    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)>;

    /// Returns the last (key, value) in the database.
    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)>;

    /// Returns a vector of values corresponding to the keys provided.
    fn multi_get<'a, T: Table>(
        &'a self,
        keys: impl IntoIterator<Item = &'a T::Key>,
    ) -> eyre::Result<Vec<Option<T::Value>>> {
        let tx = self.read_txn()?;
        keys.into_iter().map(|key| tx.get::<T>(key.borrow())).collect()
    }

    /// If the underlying DB needs to be manually compacted (looking at redb here) then this can be
    /// overwritten to allow this.  No-op for most backends.
    fn compact(&self) -> eyre::Result<()> {
        Ok(())
    }

    /// Await until this caller's background writes are durably committed to the physical store.
    ///
    /// Some backends persist in the background: an `insert` updates an in-memory layer and enqueues
    /// the disk write, returning before it is durable. This barrier resolves only once every write
    /// ordered before it is durably committed, including the subtle case where a concurrently-open
    /// write txn has *absorbed* this caller's bare insert, in which case the ack is deferred until
    /// that physical txn commits. Synchronously-durable backends (raw MDBX / redb, `MemDatabase`)
    /// are already durable on return, so the default no-op is correct for them. Uses `Table` as a
    /// hint so a composite backend only waits on the relevant physical store.
    ///
    /// This is the single durability barrier: await it at externalization points (the proposer's
    /// header broadcast, the vote handler's vote return, and the certifier's proposed-certificate
    /// externalization) where losing the record across a crash would make an honest node
    /// equivocate. See issues #934, #962, #963, and #975.
    ///
    /// Resolves to `Err` if the physical commit failed (e.g. disk full, `EIO`, a checksum error),
    /// so a caller can refuse to externalize an artifact whose anti-equivocation record never
    /// reached disk and fail-stop instead of silently equivocating on restart (issue #975).
    /// Synchronously-durable backends never fail here.
    fn persist<T: Table>(&self) -> impl Future<Output = eyre::Result<()>> + Send {
        std::future::ready(Ok::<(), eyre::Report>(()))
    }
    /// Synchronous, catch-up-only sibling of [`Database::persist`], for tests that cannot `await`.
    ///
    /// Unlike `persist` this deliberately does **not** provide the durability barrier: it must
    /// never defer past an open write txn, because a caller holding that txn open on the calling
    /// thread would self-deadlock (the txn it would wait on only commits after this returns). It
    /// merely lets the background writer catch up for read-your-writes ordering. Test-only;
    /// production code that needs durability awaits `persist`. The `Table` hint is unnecessary
    /// here.
    fn sync_persist(&self) {}
}
