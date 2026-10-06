//! A single tndb table — a [`Pack`] value log plus its sorted [`BtreeIndex`] — as a cheap `Clone`
//! [`TnTable`] handle.  This file is byte-oriented (byte-slice keys and values in, borrowed bytes
//! out); the typed `encode`/`decode` stays in `database.rs`.
//!
//! Readers take no lock and write no shared memory. Writers serialize on a writer lock and change a
//! copy-on-write working tree (a published index page is never modified); [`TnTable::flush`] makes
//! the log durable and then installs a new [`Published`] snapshot — the index snapshot plus the
//! published extent of the log — in an `ArcSwap`. A reader loads the current snapshot through a
//! per-thread slot, looks the key up in its immutable pages and reads the value straight from the
//! log, which never changes once appended. Both files keep one mapping that does not move (a
//! reserved range; see [`MmapFileOptions::reserve`]), and a mapping replaced on the rare
//! reservation overflow stays mapped until the file closes, so a snapshot's pages stay readable.
//!
//! Writes are visible to readers from the next flush (snapshot isolation). The writer's own
//! uncommitted writes are read through [`TnTable::get_working_with`].
//!
//! A scan ([`TableScan`]) owns a snapshot and a [`BtreeCursor`] over it and holds no lock, so a
//! table can be written (and published) while it is being scanned, from any thread; the scan keeps
//! reading the snapshot it started with.

use std::{ops::Bound, path::PathBuf, sync::Arc};

use arc_swap::ArcSwap;
use parking_lot::Mutex;

use crate::archive::{
    btree_index::{
        index::IndexSnapshot,
        iter::{BtreeCursor, PageSource},
        BtreeIndex,
    },
    data_file::{MapView, MmapFileOptions},
    error::fetch::FetchError,
    pack::{Pack, PackCompression},
};

/// Pack/index format version for tndb tables (matches `tndb::database`).
const PACK_VERSION: u16 = 1;

/// Address space reserved for a table's value log mapping (see [`MmapFileOptions::reserve`]):
/// virtual only, sized so a table's log never outgrows it in practice.
const TNDB_MAP_RESERVE: u64 = crate::archive::btree_index::index::BTX_MAP_RESERVE;

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
    /// A cursor positioned for this scan over the tree `src` reads.
    fn cursor<S: PageSource + ?Sized>(self, src: &S) -> Result<BtreeCursor, FetchError> {
        let (reverse, lower, upper) = match self {
            Self::Forward => (false, Bound::Unbounded, Bound::Unbounded),
            Self::Reverse => (true, Bound::Unbounded, Bound::Unbounded),
            Self::From(key) => (false, Bound::Included(key), Bound::Unbounded),
            Self::RevFrom(key) => (true, Bound::Unbounded, Bound::Excluded(key)),
        };
        BtreeCursor::new(src, reverse, lower, upper)
    }
}

/// The writer's state: the value log and the working (copy-on-write) index, changed only under
/// the writer lock.
#[derive(Debug)]
struct Writer {
    /// Table directory holding the `data` log and the `btx/` index.
    dir: PathBuf,
    data: Pack<Vec<u8>>,
    /// Created lazily on the first insert, once the encoded key length is known.
    idx: Option<BtreeIndex>,
}

impl Writer {
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

    /// Read `key` from the working tree (including writes not yet published).
    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
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

    fn remove(&mut self, key: &[u8]) -> eyre::Result<bool> {
        match self.idx.as_mut() {
            Some(idx) => Ok(idx.remove(key)?),
            None => Ok(false),
        }
    }

    fn clear(&mut self) -> eyre::Result<()> {
        if let Some(idx) = self.idx.as_mut() {
            idx.clear()?;
        }
        Ok(())
    }

    /// Durably persist the value log (the WAL). The index is rebuildable from it and is synced by
    /// [`BtreeIndex`]'s `Drop` on clean close.
    fn flush(&self) -> eyre::Result<()> {
        self.data.commit()?;
        Ok(())
    }

    /// Make every write so far readable: publish the index (its new pages become immutable) and
    /// the log's current extent.
    fn publish(&mut self, data_view: &Arc<MapView>) -> Published {
        let index = self.idx.as_mut().map(BtreeIndex::publish);
        data_view.publish_len(self.data.file_len());
        Published { index, data_view: Arc::clone(data_view) }
    }
}

/// What readers see: the last published index snapshot and a view of the log (published up to
/// the log's extent at that publish). Immutable; replaced as a whole by each publish.
#[derive(Debug)]
struct Published {
    index: Option<IndexSnapshot>,
    data_view: Arc<MapView>,
}

impl Published {
    fn get_with<R>(&self, key: &[u8], decode: impl FnOnce(&[u8]) -> R) -> eyre::Result<Option<R>> {
        let Some(index) = &self.index else { return Ok(None) };
        let pos = match index.load(key) {
            Ok(pos) => pos,
            Err(FetchError::NotFound) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(Some(decode(Pack::<Vec<u8>>::record_bytes_in(&self.data_view, pos)?)))
    }

    fn contains(&self, key: &[u8]) -> bool {
        self.index.as_ref().is_some_and(|index| index.load(key).is_ok())
    }

    fn len(&self) -> usize {
        self.index.as_ref().map_or(0, IndexSnapshot::len)
    }
}

/// A table's state: the writer side and the published snapshot readers load. Field order makes
/// the writer (and so the files) drop before the snapshot that points into their mappings.
#[derive(Debug)]
struct Inner {
    writer: Mutex<Writer>,
    published: ArcSwap<Published>,
    data_view: Arc<MapView>,
}

/// A cheap `Clone` handle to a table.  Reads take no lock (they load the published snapshot);
/// writes take the writer lock and become readable at the next [`Self::flush`].  The table closes
/// cleanly when the last handle — or the last live [`TableScan`], which holds the table — is
/// dropped.
#[derive(Clone, Debug)]
pub(crate) struct TnTable {
    inner: Arc<Inner>,
}

impl TnTable {
    /// Open (creating if needed) the table rooted at `dir`.  `dir` holds the `data` log and (once
    /// populated) the `btx/` index.
    pub(crate) fn open(dir: PathBuf) -> eyre::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        // The value log keeps one reserved mapping for its whole open life (it never moves on
        // growth), as the B-tree index does, so lock-free readers can read it.
        let opts = MmapFileOptions { reserve: TNDB_MAP_RESERVE, ..Default::default() };
        let data = Pack::<Vec<u8>>::open_with(
            dir.join("data"),
            0,
            false,
            PackCompression::None,
            PACK_VERSION,
            opts,
        )?;
        let data_view = data.view();
        let mut writer = Writer { dir, data, idx: None };
        let published = writer.publish(&data_view);
        Ok(Self {
            inner: Arc::new(Inner {
                writer: Mutex::new(writer),
                published: ArcSwap::from_pointee(published),
                data_view,
            }),
        })
    }

    /// Insert (or overwrite) `key → value`; readable from the next flush.
    pub(crate) fn insert(&self, key: &[u8], value: &[u8]) -> eyre::Result<()> {
        self.inner.writer.lock().insert(key, value)
    }

    /// Remove `key`; returns whether it was present. Readable from the next flush.
    pub(crate) fn remove(&self, key: &[u8]) -> eyre::Result<bool> {
        self.inner.writer.lock().remove(key)
    }

    /// Reset the table to empty (a fresh index tree; log bytes orphaned until compaction). Readable
    /// from the next flush.
    pub(crate) fn clear(&self) -> eyre::Result<()> {
        self.inner.writer.lock().clear()
    }

    /// Durably persist the value log, then publish every write so far: install a new snapshot for
    /// readers. Readers are never blocked by it (they take no lock); writers wait for it.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        let mut writer = self.inner.writer.lock();
        writer.flush()?;
        // Stored under the writer lock, so snapshots are installed in publish order.
        self.inner.published.store(Arc::new(writer.publish(&self.inner.data_view)));
        Ok(())
    }

    /// Read the published value for `key` and map its bytes with `decode`, or `None` if absent. No
    /// lock: `decode` borrows the value straight from the log's mapping.
    pub(crate) fn get_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.inner.published.load().get_with(key, decode)
    }

    /// Read `key` from the working tree, including writes not yet flushed (a write transaction's
    /// own reads). Takes the writer lock.
    pub(crate) fn get_working_with<R>(
        &self,
        key: &[u8],
        decode: impl FnOnce(&[u8]) -> R,
    ) -> eyre::Result<Option<R>> {
        self.inner.writer.lock().get_with(key, decode)
    }

    /// True if `key` is present in the published snapshot.
    pub(crate) fn contains(&self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.inner.published.load().contains(key))
    }

    /// True if the published snapshot has no entries.
    pub(crate) fn is_empty(&self) -> eyre::Result<bool> {
        Ok(self.inner.published.load().len() == 0)
    }

    /// Number of entries in the published snapshot. (Part of the table API; not currently used by
    /// `TnDatabase`.)
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> eyre::Result<usize> {
        Ok(self.inner.published.load().len())
    }

    /// A lazy, key-ordered scan over `(key_bytes, value_bytes)` of the published snapshot. Takes no
    /// lock (see the module docs); a table with no index yet scans empty.
    pub(crate) fn scan(&self, kind: ScanKind) -> TableScan {
        let published = self.inner.published.load_full();
        let cursor = published.index.as_ref().and_then(|index| kind.cursor(index).ok());
        TableScan { published, cursor, _table: Arc::clone(&self.inner) }
    }

    /// Map the single `(key_bytes, value_bytes)` a scan of `kind` lands on first with `f`, or
    /// `None` if it lands on nothing — a direct seek in the published snapshot.
    pub(crate) fn first_with<R>(
        &self,
        kind: ScanKind,
        f: impl FnOnce(&[u8], &[u8]) -> R,
    ) -> Option<R> {
        let published = self.inner.published.load();
        let index = published.index.as_ref()?;
        let (key, pos) = kind.cursor(index).ok()?.next(index)?.ok()?;
        Some(f(key, Pack::<Vec<u8>>::record_bytes_in(&published.data_view, pos).ok()?))
    }
}

/// A lazy, key-ordered scan of a published snapshot (see [`TnTable::scan`]), stepped with
/// [`Self::next_with`]. Holds no lock; it keeps the table (and so the mappings the snapshot reads)
/// open until dropped.
pub(crate) struct TableScan {
    published: Arc<Published>,
    /// `None` once the scan is exhausted or failed (or the table had no index).
    cursor: Option<BtreeCursor>,
    /// Keeps the table's files open, so the snapshot's mappings stay valid. Declared last so it
    /// drops after the snapshot.
    _table: Arc<Inner>,
}

impl TableScan {
    /// Map the next `(key_bytes, value_bytes)` with `f` — the key borrowed from the index page, the
    /// value from the log — or `None` when the scan is done.  A fetch failure ends the scan.
    pub(crate) fn next_with<R>(&mut self, f: impl FnOnce(&[u8], &[u8]) -> R) -> Option<R> {
        let Published { index, data_view } = &*self.published;
        let index = index.as_ref()?;
        let row = self.cursor.as_mut()?.next(index).and_then(|item| {
            let (key, pos) = item.ok()?;
            Some((key, Pack::<Vec<u8>>::record_bytes_in(data_view, pos).ok()?))
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
        table.flush().expect("flush");
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
        table.flush().expect("flush");
        assert_eq!(table.len().expect("len after remove"), 99);
        table.clear().expect("clear");
        table.flush().expect("flush");
        assert!(table.is_empty().expect("is_empty after clear"));
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), Vec::<u64>::new());
    }

    /// Writes are invisible to readers until a flush publishes them, but visible to the writer's
    /// own working reads at once.
    #[test]
    fn test_tntable_writes_visible_at_flush() {
        let tmp = TempDir::with_prefix("tntable_visibility").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        let (k1, v1) = kv(1);
        table.insert(&k1, &v1).expect("insert");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), None, "not yet published");
        assert!(!table.contains(&k1).expect("contains"));
        assert_eq!(table.get_working_with(&k1, |b| b.to_vec()).expect("working"), Some(v1.clone()));
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1.clone()));

        // An unpublished overwrite and remove leave the published value in place.
        table.insert(&k1, b"new").expect("overwrite");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1.clone()));
        assert!(table.remove(&k1).expect("remove"));
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), Some(v1));
        assert_eq!(table.get_working_with(&k1, |b| b.to_vec()).expect("working"), None);
        table.flush().expect("flush");
        assert_eq!(table.get_with(&k1, |b| b.to_vec()).expect("get"), None);
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
        table.flush().expect("flush");
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

    /// A scan holds no lock: the same thread can write and flush the table mid-scan, and the scan
    /// keeps yielding the snapshot it started on (overwrites, removes and inserts made after it
    /// started are not seen) while a new scan sees the new state.
    #[test]
    fn test_tntable_write_during_scan_keeps_the_snapshot() {
        let tmp = TempDir::with_prefix("tntable_scan_write").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        for i in 0..3_000u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
        let mut scan = table.scan(ScanKind::Forward);
        assert_eq!(scan.next_with(|k, v| (key_u64(k), v.to_vec())), Some((0, kv(0).1)));

        for i in 0..3_000u64 {
            table.insert(&kv(i).0, b"changed").expect("overwrite during the scan");
        }
        for i in 0..1_000u64 {
            assert!(table.remove(&kv(i).0).expect("remove during the scan"));
        }
        table.insert(&kv(9_999).0, &kv(9_999).1).expect("insert during the scan");
        table.flush().expect("flush during the scan");

        let rest: Vec<_> =
            std::iter::from_fn(|| scan.next_with(|k, v| (key_u64(k), v.to_vec()))).collect();
        assert_eq!(rest, (1..3_000u64).map(|i| (i, kv(i).1)).collect::<Vec<_>>());
        assert_eq!(table.len().expect("len"), 2_001);
        assert_eq!(
            table.get_with(&kv(2_000).0, |b| b.to_vec()).expect("get"),
            Some(b"changed".to_vec())
        );
    }

    /// Readers on several threads against a writer that keeps overwriting and committing: every
    /// read sees a complete committed value (all its keys from one commit generation or later,
    /// never a torn or uncommitted value), across index and log growth.
    #[test]
    fn test_tntable_readers_against_committing_writer() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        const KEYS: u64 = 500;
        let value = |i: u64, generation: u64| -> Vec<u8> {
            let mut v = vec![(i ^ generation) as u8; 1_024];
            v[..8].copy_from_slice(&generation.to_le_bytes());
            v[8..16].copy_from_slice(&i.to_le_bytes());
            v
        };
        let tmp = TempDir::with_prefix("tntable_concurrent").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        for i in 0..KEYS {
            table.insert(&kv(i).0, &value(i, 0)).expect("insert");
        }
        table.flush().expect("flush");
        let committed = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let readers: Vec<_> = (0..4u64)
            .map(|t| {
                let (table, committed, stop) = (table.clone(), committed.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut x = t + 1;
                    let mut reads = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let floor = committed.load(Ordering::Acquire);
                        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                        let i = (x >> 33) % KEYS;
                        let v = table.get_with(&kv(i).0, |b| b.to_vec()).expect("get");
                        let v = v.expect("every key is always present");
                        let generation = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                        assert!(generation >= floor, "read generation {generation} < {floor}");
                        assert_eq!(v, value(i, generation), "a torn or foreign value");
                        if reads.is_multiple_of(64) {
                            // A whole scan sees one generation per key and every key.
                            let mut scan = table.scan(ScanKind::Forward);
                            let mut n = 0;
                            while let Some(ok) = scan.next_with(|k, v| {
                                let g = u64::from_le_bytes(v[..8].try_into().expect("8 bytes"));
                                v == value(key_u64(k), g).as_slice()
                            }) {
                                assert!(ok, "a torn value in a scan");
                                n += 1;
                            }
                            assert_eq!(n, KEYS);
                        }
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();

        for generation in 1..=40u64 {
            for i in 0..KEYS {
                table.insert(&kv(i).0, &value(i, generation)).expect("overwrite");
            }
            table.flush().expect("commit");
            committed.store(generation, Ordering::Release);
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            assert!(reader.join().expect("reader") > 0);
        }
    }

    /// A live scan keeps the table open (it shares the table's state) even after the last handle
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
        table.flush().expect("flush");
        let scan = table.scan(ScanKind::Reverse);
        drop(table);
        assert_eq!(keys_of(scan), (0..10).rev().collect::<Vec<_>>());
        let table = TnTable::open(dir).expect("reopen after the scan closed the table");
        table.insert(&kv(10).0, &kv(10).1).expect("insert after reopen"); // reopens the index
        table.flush().expect("flush");
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
        table.flush().expect("flush");
        let first = |kind| table.first_with(kind, |k, v| (key_u64(k), v.to_vec()));
        assert_eq!(first(ScanKind::Reverse), Some((198, kv(198).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(50).0)), Some((48, kv(48).1)), "strictly below");
        assert_eq!(first(ScanKind::RevFrom(kv(51).0)), Some((50, kv(50).1)));
        assert_eq!(first(ScanKind::RevFrom(kv(0).0)), None, "nothing below the smallest key");
        assert_eq!(first(ScanKind::From(kv(51).0)), Some((52, kv(52).1)));
        assert_eq!(first(ScanKind::Forward), Some((0, kv(0).1)));
    }

    /// A scan takes no lock, so a flush of the same table completes while the scan is alive.
    #[test]
    fn test_tntable_flush_does_not_wait_for_readers() {
        use std::{sync::mpsc, time::Duration};

        let tmp = TempDir::with_prefix("tntable_flush_shared").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(&k, &v).expect("insert");
        }
        table.flush().expect("flush");
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
