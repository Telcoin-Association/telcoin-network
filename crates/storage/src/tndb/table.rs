//! A single tndb table — a [`Pack`] value log plus its sorted [`BtreeIndex`] — as a cheap `Clone`
//! [`TnTable`] handle.  Every op locks a shared [`Inner`] directly (no thread, no channel).  This
//! file is byte-oriented (byte-slice keys and values in, borrowed bytes out); the typed
//! `encode`/`decode` stays in `database.rs`.
//!
//! Point reads/writes take the `RwLock<Inner>` — a shared read lock for reads (`&self`:
//! `get`/`contains`/…), an exclusive write lock for writes (`&mut self`: `insert`/`remove`/…) — so
//! an uncontended op costs a lock, not a thread round-trip. (An earlier pure-actor version routed
//! every op through a channel, which was ~2–3× slower for point ops.)
//!
//! A scan ([`TableScan`]) is an owned read guard on the table plus a [`BtreeCursor`] detached from
//! the index (each step takes the index as an argument), so there is no self-reference to work
//! around: each step borrows the key from the mapped leaf and the value from the log.  A scan holds
//! its read lock until it is dropped — the same contract as `mem_db`'s iterators: concurrent reads
//! on other threads are fine, but a write to the *same* table waits until the scan is dropped, so a
//! caller must drop (or drain and drop) a scan before writing that table on the same thread.

use std::{ops::Bound, path::PathBuf, sync::Arc};

use parking_lot::{ArcRwLockReadGuard, RawRwLock, RwLock};

use crate::archive::{
    btree_index::{iter::BtreeCursor, BtreeIndex},
    error::fetch::FetchError,
    pack::{Pack, PackCompression},
};

/// Pack/index format version for tndb tables (matches `tndb::database`).
const PACK_VERSION: u16 = 1;

/// Which ordered scan to run over a table's index.
pub(crate) enum ScanKind {
    /// Every entry in ascending key order.
    Forward,
    /// Every entry in descending key order.
    Reverse,
    /// Ascending from the given key bytes (inclusive) to the end.
    From(Vec<u8>),
    /// Descending over the entries whose keys are strictly less than the given key bytes.
    RevFrom(Vec<u8>),
}

impl ScanKind {
    /// A cursor positioned for this scan over `index`.
    fn cursor(self, index: &BtreeIndex) -> Result<BtreeCursor, FetchError> {
        let (reverse, lower, upper) = match self {
            Self::Forward => (false, Bound::Unbounded, Bound::Unbounded),
            Self::Reverse => (true, Bound::Unbounded, Bound::Unbounded),
            Self::From(key) => (false, Bound::Included(key), Bound::Unbounded),
            Self::RevFrom(key) => (true, Bound::Unbounded, Bound::Excluded(key)),
        };
        BtreeCursor::new(index, reverse, lower, upper)
    }
}

/// Table state — the append-only value log plus its sorted key index (created lazily on the first
/// insert, once the encoded key length is known).  Shared behind an `RwLock`: reads (`&self`) take
/// a read lock, writes (`&mut self`) an exclusive write lock.
#[derive(Debug)]
struct Inner {
    /// Table directory holding the `data` log and the `btx/` index.
    dir: PathBuf,
    data: Pack<Vec<u8>>,
    idx: Option<BtreeIndex>,
}

impl Inner {
    /// The index, created (with key byte length `ksize`) on first use.  On a reopened directory
    /// this reopens the on-disk index, whose header records the same `ksize`.
    fn index_mut(&mut self, ksize: u16) -> eyre::Result<&mut BtreeIndex> {
        if self.idx.is_none() {
            let idx =
                BtreeIndex::open_btx_file(self.dir.join("btx"), self.data.header(), ksize, false)?;
            self.idx = Some(idx);
        }
        Ok(self.idx.as_mut().expect("index just created"))
    }

    fn insert(&mut self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
        let pos = self.data.append_raw(value)?;
        let ksize = key.len() as u16;
        self.index_mut(ksize)?.save(key, pos)?;
        Ok(())
    }

    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
        // Resolve the position under the index borrow, then decode the value straight from the
        // log's mmap (via `record_bytes`) while the caller's read lock is held -- no intermediate
        // `Vec`.
        let pos = match self.idx.as_ref() {
            Some(idx) => match idx.load(key) {
                Ok(pos) => pos,
                Err(FetchError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            None => return Ok(None),
        };
        Ok(Some(decode(self.data.record_bytes(pos)?)))
    }

    fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.idx.as_ref().is_some_and(|idx| idx.contains(key)))
    }

    fn remove(&mut self, key: &[u8]) -> eyre::Result<bool> {
        match self.idx.as_mut() {
            Some(idx) => Ok(idx.remove(key)?),
            None => Ok(false),
        }
    }

    fn clear(&mut self) -> eyre::Result<()> {
        if let Some(idx) = self.idx.as_mut() {
            idx.rebuild_from(std::iter::empty::<(Vec<u8>, u64)>())?;
        }
        Ok(())
    }

    /// Durably persist the value log (the WAL). The index is rebuildable from it and is synced by
    /// [`BtreeIndex`]'s `Drop` on clean close, matching today's barrier.
    fn flush(&self) -> eyre::Result<()> {
        self.data.commit()?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.idx.as_ref().is_none_or(|idx| idx.is_empty())
    }

    fn len(&self) -> usize {
        self.idx.as_ref().map_or(0, |idx| idx.len())
    }
}

/// A cheap `Clone` handle to a table.  Ops lock the shared [`Inner`] directly (a read lock for
/// reads and scans, a write lock for writes).  The table closes cleanly when the last handle — or
/// the last live [`TableScan`], whose guard shares `inner` — is dropped.
#[derive(Clone, Debug)]
pub(crate) struct TnTable {
    /// The table state, locked per operation.
    inner: Arc<RwLock<Inner>>,
}

impl TnTable {
    /// Open (creating if needed) the table rooted at `dir`.  `dir` holds the `data` log and (once
    /// populated) the `btx/` index.
    pub(crate) fn open(dir: PathBuf) -> eyre::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let data =
            Pack::<Vec<u8>>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)?;
        Ok(Self { inner: Arc::new(RwLock::new(Inner { dir, data, idx: None })) })
    }

    /// Insert (or overwrite) `key → value`.
    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
        self.inner.write().insert(key, value)
    }

    /// Read the value for `key` and map its bytes with `decode`, or `None` if absent. `decode` runs
    /// while the read lock is held, so it can borrow the value straight from the log's mmap (via
    /// [`Pack::record_bytes`]) without an intermediate `Vec`.
    pub(crate) fn get_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.inner.read().get_with(key, decode)
    }

    /// True if `key` is present.
    pub(crate) fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        self.inner.read().contains(key)
    }

    /// Remove `key`; returns whether it was present.
    pub(crate) fn remove(&self, key: &[u8]) -> eyre::Result<bool> {
        self.inner.write().remove(key)
    }

    /// Reset the table to empty (index rebuilt empty; log bytes orphaned until compaction).
    pub(crate) fn clear(&self) -> eyre::Result<()> {
        self.inner.write().clear()
    }

    /// Durably persist the value log. Takes the table's read lock: readers (and live scans) keep
    /// going during the sync; writers wait for it.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        // A commit is a pure barrier over bytes already appended (appends hold the write lock, so
        // they finished before this read lock was granted), so it shares the lock with readers
        // instead of stalling them for the whole sync; writers still wait.
        self.inner.read().flush()
    }

    /// True if the table has no entries.
    pub(crate) fn is_empty(&self) -> eyre::Result<bool> {
        Ok(self.inner.read().is_empty())
    }

    /// Number of entries. (Part of the table API; not currently used by `TnDatabase`.)
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> eyre::Result<usize> {
        Ok(self.inner.read().len())
    }

    /// A lazy, key-ordered scan over `(key_bytes, value_bytes)`.  It holds the table's read lock
    /// until dropped (see the module docs); a table with no index yet scans empty.
    pub(crate) fn scan(&self, kind: ScanKind) -> TableScan {
        let guard = self.inner.read_arc();
        let cursor = guard.idx.as_ref().and_then(|idx| kind.cursor(idx).ok());
        TableScan { guard, cursor }
    }

    /// Map the single `(key_bytes, value_bytes)` a scan of `kind` lands on first with `f`, or
    /// `None` if it lands on nothing — a direct seek under a short read lock.
    pub(crate) fn first_with<R>(
        &self,
        kind: ScanKind,
        f: impl FnOnce(&[u8], &[u8]) -> R,
    ) -> Option<R> {
        let inner = self.inner.read();
        let idx = inner.idx.as_ref()?;
        let (key, pos) = kind.cursor(idx).ok()?.next(idx)?.ok()?;
        Some(f(key, inner.data.record_bytes(pos).ok()?))
    }
}

/// A lazy, key-ordered scan of a table (see [`TnTable::scan`]): an owned read guard plus a cursor,
/// stepped with [`Self::next_with`].  Holds the table's read lock until dropped.
pub(crate) struct TableScan {
    guard: ArcRwLockReadGuard<RawRwLock, Inner>,
    /// `None` once the scan is exhausted or failed (or the table had no index).
    cursor: Option<BtreeCursor>,
}

impl TableScan {
    /// Map the next `(key_bytes, value_bytes)` with `f` — the key borrowed from the index leaf, the
    /// value from the log — or `None` when the scan is done.  A fetch failure ends the scan.
    pub(crate) fn next_with<R>(&mut self, f: impl FnOnce(&[u8], &[u8]) -> R) -> Option<R> {
        let Inner { data, idx, .. } = &*self.guard;
        let row = self.cursor.as_mut()?.next(idx.as_ref()?).and_then(|item| {
            let (key, pos) = item.ok()?;
            Some((key, data.record_bytes(pos).ok()?))
        });
        match row {
            Some((key, value)) => Some(f(key, value)),
            None => {
                // Exhausted, or a fetch failure ended the scan.
                self.cursor = None;
                None
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    /// 8-byte big-endian key (so byte order equals numeric order) and a small value.
    fn kv(i: u64) -> (Vec<u8>, Vec<u8>) {
        (i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
    }

    fn key_u64(key: &[u8]) -> u64 {
        u64::from_be_bytes(key.try_into().expect("8-byte key"))
    }

    fn keys_of(mut scan: TableScan) -> Vec<u64> {
        std::iter::from_fn(|| scan.next_with(|k, _| key_u64(k))).collect()
    }

    #[test]
    fn test_tntable_ops_and_scans() {
        let tmp = TempDir::with_prefix("tntable").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");

        assert!(table.is_empty().expect("is_empty"));
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        assert!(!table.is_empty().expect("is_empty"));
        assert_eq!(table.len().expect("len"), 100);

        // Point reads.
        for i in 0..100u64 {
            let (k, v) = kv(i);
            assert_eq!(table.get_with(&k, |b| b.to_vec()).expect("get"), Some(v));
            assert!(table.contains(&k).expect("contains"));
        }
        assert_eq!(table.get_with(&kv(999).0, |b| b.to_vec()).expect("get miss"), None);
        assert!(!table.contains(&kv(999).0).expect("contains miss"));

        // Ordered scans.
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), (0..100).collect::<Vec<_>>());
        assert_eq!(keys_of(table.scan(ScanKind::Reverse)), (0..100).rev().collect::<Vec<_>>());
        assert_eq!(
            keys_of(table.scan(ScanKind::From(50u64.to_be_bytes().to_vec()))),
            (50..100).collect::<Vec<_>>()
        );

        // Early-terminate a scan: take a few, drop the rest.
        let mut scan = table.scan(ScanKind::Forward);
        let head: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, _| key_u64(k))).take(3).collect();
        assert_eq!(head, vec![0, 1, 2]);
        drop(scan);

        // Remove + clear.
        assert!(table.remove(&kv(0).0).expect("remove"));
        assert!(!table.remove(&kv(0).0).expect("remove again"));
        assert_eq!(table.len().expect("len after remove"), 99);
    }

    #[test]
    fn test_tntable_persists_across_reopen() {
        let tmp = TempDir::with_prefix("tntable_persist").expect("temp dir");
        let dir = tmp.path().join("t");
        {
            let table = TnTable::open(dir.clone()).expect("open");
            for i in 0..50u64 {
                let (k, v) = kv(i);
                table.insert(&k, &v).expect("insert");
            }
            assert!(table.remove(&kv(7).0).expect("remove"));
            table.flush().expect("flush");
        } // drop -> actor thread joins, clean close (index synced, log sealed)

        // Reopen: the on-disk index reopens on the first same-width insert, then old+new are
        // visible.
        let table = TnTable::open(dir).expect("reopen");
        let (k100, v100) = kv(100);
        table.insert(&k100, &v100).expect("insert after reopen");
        assert_eq!(table.get_with(&k100, |b| b.to_vec()).expect("get new"), Some(v100));
        assert_eq!(
            table.get_with(&kv(20).0, |b| b.to_vec()).expect("get old"),
            Some(kv(20).1),
            "old value persisted"
        );
        assert_eq!(
            table.get_with(&kv(7).0, |b| b.to_vec()).expect("get removed"),
            None,
            "removal persisted"
        );
    }

    /// Dropping a scan releases the table's read lock, so the same thread can write the table
    /// afterwards (a scan held across the write would block it, as with `mem_db`).
    #[test]
    fn test_tntable_dropped_scan_releases_the_table() {
        let tmp = TempDir::with_prefix("tntable_scan_drop").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        for i in 0..3_000u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        let mut scan = table.scan(ScanKind::Forward);
        assert_eq!(scan.next_with(|k, v| (key_u64(k), v.to_vec())), Some((0, kv(0).1)));
        drop(scan); // an unconsumed scan, far more rows left than the old channel held
        let (k, v) = kv(9_999);
        table.insert(&k, &v).expect("write after the scan is dropped");
        assert_eq!(table.len().expect("len"), 3_001);
    }

    /// A live scan keeps the table open (its guard shares the state) even after the last handle
    /// is dropped; the table then closes cleanly when the scan ends.
    #[test]
    fn test_tntable_scan_outlives_its_handle() {
        let tmp = TempDir::with_prefix("tntable_scan_outlives").expect("temp dir");
        let dir = tmp.path().join("t");
        let table = TnTable::open(dir.clone()).expect("open");
        for i in 0..10u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        let scan = table.scan(ScanKind::Reverse);
        drop(table);
        assert_eq!(keys_of(scan), (0..10).rev().collect::<Vec<_>>());
        let table = TnTable::open(dir).expect("reopen after the scan closed the table");
        table.insert(&kv(10).0, &kv(10).1).expect("insert after reopen"); // reopens the index
        assert_eq!(table.len().expect("len"), 11);
    }

    /// `first_with` seeks directly: the last entry, the greatest entry below a key, and the first
    /// entry at or above a key.
    #[test]
    fn test_tntable_first_with_seeks() {
        let tmp = TempDir::with_prefix("tntable_first_with").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        assert_eq!(table.first_with(ScanKind::Reverse, |k, _| key_u64(k)), None, "no index yet");
        for i in (0..100u64).map(|i| i * 2) {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        let first = |kind| table.first_with(kind, |k, v| (key_u64(k), v.to_vec()));
        assert_eq!(first(ScanKind::Reverse), Some((198, kv(198).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(50).0)), Some((48, kv(48).1)), "strictly below");
        assert_eq!(first(ScanKind::RevFrom(kv(51).0)), Some((50, kv(50).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(0).0)), None, "nothing below the smallest key");
        assert_eq!(first(ScanKind::From(kv(51).0)), Some((52, kv(52).1)));
        assert_eq!(first(ScanKind::Forward), Some((0, kv(0).1)));
    }

    /// A flush is a barrier over already-appended bytes and takes only the read lock, so it
    /// completes while a scan of the same table is alive (a write-locked flush would wait for the
    /// scan, stalling every reader for the whole sync).
    #[test]
    fn test_tntable_flush_does_not_wait_for_readers() {
        use std::{sync::mpsc, time::Duration};

        let tmp = TempDir::with_prefix("tntable_flush_shared").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        let mut scan = table.scan(ScanKind::Forward);
        assert!(scan.next_with(|_, _| ()).is_some());

        // Flush on another thread so a regression fails the test instead of hanging it.
        let (done_tx, done_rx) = mpsc::channel();
        let flusher = table.clone();
        let flush = std::thread::spawn(move || {
            done_tx.send(flusher.flush().is_ok()).expect("report flush");
        });
        let flushed = done_rx.recv_timeout(Duration::from_secs(10));
        drop(scan);
        flush.join().expect("flush thread");
        assert_eq!(flushed, Ok(true), "a flush must not wait for a live scan of its table");
    }
}
