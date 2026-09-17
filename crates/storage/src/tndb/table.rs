//! A single tndb table — a [`Pack`] value log plus its sorted [`BtreeIndex`] — encapsulated as an
//! **actor on its own dedicated thread**, mirroring [`crate::consensus_pack`]'s model.
//!
//! Callers hold a cheap `Clone` [`TnTable`] handle and interact by message passing, so a table is
//! accessed **without holding a lock**: a later step lets [`crate::tndb::TnDatabase`] store these
//! handles directly (replacing today's `Arc<Mutex<..>>` store). This file is byte-oriented
//! (`Vec<u8>` keys and values); the typed `encode`/`decode` stays in `database.rs`.
//!
//! Because the thread owns the `Pack` and `BtreeIndex` directly (no lock guard), a scan can
//! split-borrow the two fields and **stream** the B+tree iterator's values over a channel with
//! backpressure — no self-referential struct, and no eager position walk.
//!
//! Channels avoid a new dependency and work from both sync tests and the async node: commands use a
//! `tokio` unbounded MPSC (its `send` is synchronous and never panics in a runtime, and the sender
//! is `Send + Sync + Clone`), while each reply uses a `std::sync::mpsc` channel (its blocking
//! `recv` — unlike `tokio`'s — never panics inside a runtime). The actor thread is a plain
//! `std::thread`, so `blocking_recv` there is correct.

// Step one: the handle is not yet wired into `TnDatabase`, so its methods look unused in a non-test
// build. Remove once `database.rs` adopts it.
#![allow(dead_code)]

use std::{
    path::PathBuf,
    sync::{
        mpsc::{self, Receiver, Sender, SyncSender},
        Arc,
    },
    thread::JoinHandle,
};

use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
}

/// Thread-owned table state: the append-only value log plus its sorted key index (created lazily on
/// the first insert, once the encoded key length is known).
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

    fn get(&mut self, key: &[u8]) -> eyre::Result<Option<Vec<u8>>> {
        // Resolve the position under the index borrow, then read the value from the log.
        let pos = match self.idx.as_mut() {
            Some(idx) => match idx.load(key) {
                Ok(pos) => pos,
                Err(FetchError::NotFound) => return Ok(None),
                Err(e) => return Err(e.into()),
            },
            None => return Ok(None),
        };
        Ok(Some(self.data.fetch(pos)?))
    }

    fn contains(&mut self, key: &[u8]) -> eyre::Result<bool> {
        Ok(self.idx.as_mut().is_some_and(|idx| idx.contains(key)))
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
    fn scan(&mut self, kind: ScanKind, out: &SyncSender<(Vec<u8>, Vec<u8>)>) {
        let Inner { data, idx, .. } = self;
        let Some(idx) = idx.as_mut() else { return };
        let iter = match kind {
            ScanKind::Forward => idx.iter(),
            ScanKind::Reverse => idx.rev_iter(),
            ScanKind::From(from) => idx.range(from..),
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

/// A command sent to a table's actor thread; each request carries its reply channel.
enum TableMessage {
    Insert { key: Vec<u8>, value: Vec<u8>, reply: Sender<eyre::Result<()>> },
    Get { key: Vec<u8>, reply: Sender<eyre::Result<Option<Vec<u8>>>> },
    Contains { key: Vec<u8>, reply: Sender<eyre::Result<bool>> },
    Remove { key: Vec<u8>, reply: Sender<eyre::Result<bool>> },
    Clear { reply: Sender<eyre::Result<()>> },
    Flush { reply: Sender<eyre::Result<()>> },
    IsEmpty { reply: Sender<bool> },
    Len { reply: Sender<usize> },
    Scan { kind: ScanKind, out: SyncSender<(Vec<u8>, Vec<u8>)> },
    Shutdown,
}

/// A cheap `Clone` handle to a table actor.  Every method sends a command to the table's dedicated
/// thread and blocks on the reply — callers never hold a lock.
#[derive(Clone)]
pub(crate) struct TnTable {
    tx: UnboundedSender<TableMessage>,
    /// The actor thread's join handle, taken by the last handle's `Drop` for a clean, durable
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
        let inner = Inner { dir, data, idx: None };
        let (tx, rx) = unbounded_channel();
        let join = std::thread::Builder::new()
            .name("tndb-table".into())
            .spawn(move || run_table_loop(inner, rx))?;
        Ok(Self { tx, join: Arc::new(Mutex::new(Some(join))) })
    }

    /// Send a command with a fresh reply channel and block on the reply.
    fn send_recv<T>(&self, make: impl FnOnce(Sender<T>) -> TableMessage) -> eyre::Result<T> {
        let (reply, rx) = mpsc::channel();
        self.tx.send(make(reply)).map_err(|_| eyre::eyre!("tndb table actor stopped"))?;
        rx.recv().map_err(|_| eyre::eyre!("tndb table actor dropped the reply"))
    }

    /// Insert (or overwrite) `key → value`.
    pub(crate) fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> eyre::Result<()> {
        self.send_recv(|reply| TableMessage::Insert { key, value, reply })?
    }

    /// Read the value for `key`, or `None` if absent.
    pub(crate) fn get(&self, key: Vec<u8>) -> eyre::Result<Option<Vec<u8>>> {
        self.send_recv(|reply| TableMessage::Get { key, reply })?
    }

    /// True if `key` is present.
    pub(crate) fn contains(&self, key: Vec<u8>) -> eyre::Result<bool> {
        self.send_recv(|reply| TableMessage::Contains { key, reply })?
    }

    /// Remove `key`; returns whether it was present.
    pub(crate) fn remove(&self, key: Vec<u8>) -> eyre::Result<bool> {
        self.send_recv(|reply| TableMessage::Remove { key, reply })?
    }

    /// Reset the table to empty (index rebuilt empty; log bytes orphaned until compaction).
    pub(crate) fn clear(&self) -> eyre::Result<()> {
        self.send_recv(|reply| TableMessage::Clear { reply })?
    }

    /// Durably persist the value log.
    pub(crate) fn flush(&self) -> eyre::Result<()> {
        self.send_recv(|reply| TableMessage::Flush { reply })?
    }

    /// True if the table has no entries.
    pub(crate) fn is_empty(&self) -> eyre::Result<bool> {
        self.send_recv(|reply| TableMessage::IsEmpty { reply })
    }

    /// Number of entries.
    pub(crate) fn len(&self) -> eyre::Result<usize> {
        self.send_recv(|reply| TableMessage::Len { reply })
    }

    /// A lazy, key-ordered iterator over `(key_bytes, value_bytes)`.  The actor streams items over
    /// a bounded channel; dropping the returned iterator stops the scan.  If the actor is gone
    /// the iterator is simply empty.
    pub(crate) fn scan(&self, kind: ScanKind) -> TnTableScan {
        let (out, rx) = mpsc::sync_channel(SCAN_CHANNEL_CAP);
        let _ = self.tx.send(TableMessage::Scan { kind, out });
        TnTableScan { rx }
    }
}

impl Drop for TnTable {
    fn drop(&mut self) {
        // The last live handle shuts the actor down and waits for its clean, durable close.
        if Arc::strong_count(&self.join) == 1 {
            if let Some(handle) = self.join.lock().take() {
                let _ = self.tx.send(TableMessage::Shutdown);
                let _ = handle.join();
            }
        }
    }
}

/// The iterator returned by [`TnTable::scan`]: it pulls streamed items from the actor thread.
pub(crate) struct TnTableScan {
    rx: Receiver<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for TnTableScan {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // `Err` means the scan finished (the actor dropped its sender).
        self.rx.recv().ok()
    }
}

/// The actor loop: owns `inner` and serves commands until shutdown, then clean-closes.
fn run_table_loop(mut inner: Inner, mut rx: UnboundedReceiver<TableMessage>) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            TableMessage::Insert { key, value, reply } => {
                let _ = reply.send(inner.insert(key, value));
            }
            TableMessage::Get { key, reply } => {
                let _ = reply.send(inner.get(&key));
            }
            TableMessage::Contains { key, reply } => {
                let _ = reply.send(inner.contains(&key));
            }
            TableMessage::Remove { key, reply } => {
                let _ = reply.send(inner.remove(&key));
            }
            TableMessage::Clear { reply } => {
                let _ = reply.send(inner.clear());
            }
            TableMessage::Flush { reply } => {
                let _ = reply.send(inner.flush());
            }
            TableMessage::IsEmpty { reply } => {
                let _ = reply.send(inner.is_empty());
            }
            TableMessage::Len { reply } => {
                let _ = reply.send(inner.len());
            }
            TableMessage::Scan { kind, out } => inner.scan(kind, &out),
            TableMessage::Shutdown => break,
        }
    }
    // Dropping `inner` clean-closes: `Pack` seals the log and `BtreeIndex`'s `Drop` syncs the
    // index.
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
