//! A single tndb table — a [`Pack`] value log plus its sorted [`BtreeIndex`] — as a cheap `Clone`
//! [`TnTable`] handle.  It is a **hybrid**: point ops lock a shared [`Inner`] directly (no
//! cross-thread hop), while ordered scans run on a dedicated thread.  This file is byte-oriented
//! (`Vec<u8>` keys and values); the typed `encode`/`decode` stays in `database.rs`.
//!
//! Point reads/writes take the `RwLock<Inner>` — a shared read lock for reads (`&self`:
//! `get`/`contains`/…), an exclusive write lock for writes (`&mut self`: `insert`/`remove`/…) — so
//! an uncontended op costs a lock, not a thread round-trip. (An earlier pure-actor version routed
//! every op through a channel, which was ~2–3× slower for point ops.)
//!
//! A scan needs a live `BtreeIter` borrowing the index, which can't be handed back as a
//! self-referential iterator; so scans run on a dedicated thread that takes a read lock and
//! **streams** the `(key, value)` pairs over a bounded channel with backpressure (dropping the
//! consumer stops the walk).  A scan therefore holds a read lock for its duration: concurrent point
//! reads are fine, but a point write to the *same* table waits until the scan is drained or dropped
//! — so a caller must not hold an unconsumed scan across a write of the same table on one thread.

use std::{
    path::PathBuf,
    sync::{
        mpsc::{self, SyncSender},
        Arc,
    },
    thread::JoinHandle,
};

use parking_lot::{Mutex, RwLock};

use crate::archive::{
    btree_index::BtreeIndex,
    error::fetch::FetchError,
    pack::{Pack, PackCompression},
};

/// Pack/index format version for tndb tables (matches `tndb::database`).
const PACK_VERSION: u16 = 1;

/// Bounded capacity of a scan's item channel — backpressure so a slow consumer bounds the actor's
/// look-ahead memory.
const SCAN_CHANNEL_CAP: usize = 1024;

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

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> eyre::Result<()> {
        let pos = self.data.append(&value)?;
        let ksize = key.len() as u16;
        self.index_mut(ksize)?.save(&key, pos)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> eyre::Result<Option<Vec<u8>>> {
        // Resolve the position under the index borrow, then read the value from the log.
        let pos = match self.idx.as_ref() {
            Some(idx) => match idx.load(key) {
                Ok(pos) => pos,
                Err(FetchError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            None => return Ok(None),
        };
        Ok(Some(self.data.fetch(pos)?))
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
    fn flush(&mut self) -> eyre::Result<()> {
        self.data.commit()?;
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.idx.as_ref().is_none_or(|idx| idx.is_empty())
    }

    fn len(&self) -> usize {
        self.idx.as_ref().map_or(0, |idx| idx.len())
    }

    /// Stream `(key_bytes, value_bytes)` over `out` in key order.  Split-borrowing `data`/`idx`
    /// keeps the live B+tree iterator (over `idx`) and the value fetch (from `data`) disjoint, so
    /// values stream lazily; a dropped receiver (`send` error) stops the walk early.
    fn scan(&self, kind: ScanKind, out: &SyncSender<(Vec<u8>, Vec<u8>)>) {
        let Inner { data, idx, .. } = self;
        let Some(idx) = idx.as_ref() else { return };
        let iter = match kind {
            ScanKind::Forward => idx.iter(),
            ScanKind::Reverse => idx.rev_iter(),
            ScanKind::From(from) => idx.range(from..),
            ScanKind::RevFrom(from) => idx.rev_range(..from),
        };
        let Ok(iter) = iter else { return };
        for item in iter {
            let Ok((key_bytes, pos)) = item else { break };
            let Ok(value_bytes) = data.fetch(pos) else { break };
            if out.send((key_bytes, value_bytes)).is_err() {
                break;
            }
        }
    }
}

/// A request sent to a table's scan thread (point ops bypass it and lock `Inner` directly).
enum ScanRequest {
    /// Stream the given scan's `(key, value)` pairs over `out`.
    Scan { kind: ScanKind, out: SyncSender<(Vec<u8>, Vec<u8>)> },
    /// Stop the scan thread (sent by the last handle's `Drop`).
    Shutdown,
}

/// A cheap `Clone` handle to a table.  Point ops lock the shared [`Inner`] directly (a read lock
/// for reads, a write lock for writes); ordered scans are streamed by a dedicated thread.
#[derive(Clone, Debug)]
pub(crate) struct TnTable {
    /// The table state, locked per operation — no cross-thread hop for point reads/writes.
    inner: Arc<RwLock<Inner>>,
    /// Requests to the scan thread (which shares `inner`).
    scan_tx: mpsc::Sender<ScanRequest>,
    /// The scan thread's join handle, taken by the last handle's `Drop` for a clean, durable
    /// close.
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl TnTable {
    /// Open (creating if needed) the table rooted at `dir` and spawn its actor thread.  `dir` holds
    /// the `data` log and (once populated) the `btx/` index.
    pub(crate) fn open(dir: PathBuf) -> eyre::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let data =
            Pack::<Vec<u8>>::open(dir.join("data"), 0, false, PackCompression::None, PACK_VERSION)?;
        let inner = Arc::new(RwLock::new(Inner { dir, data, idx: None }));
        let (scan_tx, scan_rx) = mpsc::channel();
        let scan_inner = Arc::clone(&inner);
        let join = std::thread::Builder::new()
            .name("tndb-table-scan".into())
            .spawn(move || run_scan_loop(scan_inner, scan_rx))?;
        Ok(Self { inner, scan_tx, join: Arc::new(Mutex::new(Some(join))) })
    }

    /// Insert (or overwrite) `key → value`.
    pub(crate) fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> eyre::Result<()> {
        self.inner.write().insert(key, value)
    }

    /// Read the value for `key`, or `None` if absent.
    pub(crate) fn get(&self, key: Vec<u8>) -> eyre::Result<Option<Vec<u8>>> {
        self.inner.read().get(&key)
    }

    /// True if `key` is present.
    pub(crate) fn contains(&self, key: Vec<u8>) -> eyre::Result<bool> {
        self.inner.read().contains(&key)
    }

    /// Remove `key`; returns whether it was present.
    pub(crate) fn remove(&self, key: Vec<u8>) -> eyre::Result<bool> {
        self.inner.write().remove(&key)
    }

    /// Reset the table to empty (index rebuilt empty; log bytes orphaned until compaction).
    pub(crate) fn clear(&self) -> eyre::Result<()> {
        self.inner.write().clear()
    }

    /// Durably persist the value log.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        self.inner.write().flush()
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

    /// A lazy, key-ordered iterator over `(key_bytes, value_bytes)`.  The scan thread streams items
    /// over a bounded channel (holding a read lock); dropping the returned iterator stops the scan.
    /// If the scan thread is gone the iterator is simply empty.
    pub(crate) fn scan(&self, kind: ScanKind) -> TnTableScan {
        let (out, rx) = mpsc::sync_channel(SCAN_CHANNEL_CAP);
        let _ = self.scan_tx.send(ScanRequest::Scan { kind, out });
        TnTableScan { rx }
    }
}

impl Drop for TnTable {
    fn drop(&mut self) {
        // The last live handle stops the scan thread and joins it, so the scan thread releases its
        // `inner` clone; this handle's `inner` is then the last ref, and dropping it clean-closes.
        if Arc::strong_count(&self.join) == 1 {
            if let Some(handle) = self.join.lock().take() {
                let _ = self.scan_tx.send(ScanRequest::Shutdown);
                let _ = handle.join();
            }
        }
    }
}

/// The iterator returned by [`TnTable::scan`]: it pulls streamed items from the scan thread.
pub(crate) struct TnTableScan {
    rx: mpsc::Receiver<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for TnTableScan {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // `Err` means the scan finished (the scan thread dropped its sender).
        self.rx.recv().ok()
    }
}

/// The scan thread: shares `inner` (via the `RwLock`) and streams scans until shutdown.  Point ops
/// never reach here — they lock `inner` on the caller's thread.
fn run_scan_loop(inner: Arc<RwLock<Inner>>, rx: mpsc::Receiver<ScanRequest>) {
    while let Ok(req) = rx.recv() {
        match req {
            // Hold a read lock only for the duration of the scan (concurrent reads OK; writes
            // wait).
            ScanRequest::Scan { kind, out } => inner.read().scan(kind, &out),
            ScanRequest::Shutdown => break,
        }
    }
    // Drop this thread's `inner` clone; the last handle's `Drop` then clean-closes the table.
    drop(inner);
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    /// 8-byte big-endian key (so byte order equals numeric order) and a small value.
    fn kv(i: u64) -> (Vec<u8>, Vec<u8>) {
        (i.to_be_bytes().to_vec(), format!("v{i}").into_bytes())
    }

    fn keys_of(scan: TnTableScan) -> Vec<u64> {
        scan.map(|(k, _)| u64::from_be_bytes(k.try_into().expect("8-byte key"))).collect()
    }

    #[test]
    fn test_tntable_actor_ops_and_scans() {
        let tmp = TempDir::with_prefix("tntable").expect("temp dir");
        let table = TnTable::open(tmp.path().join("t")).expect("open");

        assert!(table.is_empty().expect("is_empty"));
        for i in 0..100u64 {
            let (k, v) = kv(i);
            table.insert(k, v).expect("insert");
        }
        assert!(!table.is_empty().expect("is_empty"));
        assert_eq!(table.len().expect("len"), 100);

        // Point reads.
        for i in 0..100u64 {
            let (k, v) = kv(i);
            assert_eq!(table.get(k.clone()).expect("get"), Some(v));
            assert!(table.contains(k).expect("contains"));
        }
        assert_eq!(table.get(kv(999).0).expect("get miss"), None);
        assert!(!table.contains(kv(999).0).expect("contains miss"));

        // Ordered scans.
        assert_eq!(keys_of(table.scan(ScanKind::Forward)), (0..100).collect::<Vec<_>>());
        assert_eq!(keys_of(table.scan(ScanKind::Reverse)), (0..100).rev().collect::<Vec<_>>());
        assert_eq!(
            keys_of(table.scan(ScanKind::From(50u64.to_be_bytes().to_vec()))),
            (50..100).collect::<Vec<_>>()
        );

        // Early-terminate a scan: take a few, drop the rest — must not hang.
        let head: Vec<_> = table.scan(ScanKind::Forward).take(3).collect();
        assert_eq!(head.len(), 3);

        // Remove + clear.
        assert!(table.remove(kv(0).0).expect("remove"));
        assert!(!table.remove(kv(0).0).expect("remove again"));
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
                table.insert(k, v).expect("insert");
            }
            assert!(table.remove(kv(7).0).expect("remove"));
            table.flush().expect("flush");
        } // drop -> actor thread joins, clean close (index synced, log sealed)

        // Reopen: the on-disk index reopens on the first same-width insert, then old+new are
        // visible.
        let table = TnTable::open(dir).expect("reopen");
        let (k100, v100) = kv(100);
        table.insert(k100.clone(), v100.clone()).expect("insert after reopen");
        assert_eq!(table.get(k100).expect("get new"), Some(v100));
        assert_eq!(table.get(kv(20).0).expect("get old"), Some(kv(20).1), "old value persisted");
        assert_eq!(table.get(kv(7).0).expect("get removed"), None, "removal persisted");
    }
}
