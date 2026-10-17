//! Pack file for storing Certificates, indexed by certificate digest (header hash).

use std::{
    error::Error, fmt::Display, fs::File, hash::BuildHasherDefault, io, path::Path, sync::Arc,
    thread::JoinHandle,
};

use parking_lot::Mutex;
use tn_types::{Certificate, Epoch, Hash, HeaderDigest, B256};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tracing::{error, info, warn};

use crate::{
    archive::{
        digest_index::HdxIndex,
        error::{fetch::FetchError, open::OpenError},
        fxhasher::FxHasher,
        index::Index as _,
        pack::{Pack, PackCompression},
    },
    consensus_pack::PACK_VERSION,
    error_latch::latch_first_error,
};

enum PackMessage {
    Save(Certificate),
    Get(HeaderDigest, oneshot::Sender<Option<Certificate>>),
    Contains(HeaderDigest, oneshot::Sender<bool>),
    Persist(oneshot::Sender<Result<(), PackError>>),
    Shutdown,
    ShutdownAsync(oneshot::Sender<Result<(), PackError>>),
}

/// Manages a pack file of [`Certificate`] data, indexed by certificate digest.
/// Note, lack of Clone to make shutdown easier to manage.
///
/// Saves run on a background thread. A failed save latches into an error slot that
/// [`get_error`](Self::get_error) reads without clearing. [`persist`](Self::persist) and
/// [`shutdown`](Self::shutdown) are the durability barriers: a latched save failure reaches
/// their return value, even when that save was still queued at the time of the call. A dropped
/// pack (no `shutdown`) can only log its final flush's failure.
#[derive(Debug)]
pub struct CertificatePack {
    tx: Sender<PackMessage>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    error: watch::Receiver<Option<PackError>>,
    epoch: Epoch,
}

fn clear_pack_loop(mut rx: Receiver<PackMessage>) {
    rx.close();
    while let Ok(msg) = rx.try_recv() {
        drop(msg);
    }
}

fn run_pack_loop(
    mut inner: Inner,
    mut rx: Receiver<PackMessage>,
    tx_error: watch::Sender<Option<PackError>>,
    epoch: Epoch,
) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            PackMessage::Save(cert) => {
                // First-error-wins: a plain `send_replace` would overwrite the root cause
                // with a follow-on error before any reader could observe it (#1148). The
                // pack also replays the root cause of its failed state on each later save,
                // so this latch is defense in depth for failures that do not share one root
                // cause. The log keeps every failure visible, including the ones the latch
                // does not keep.
                inner.save(&cert).unwrap_or_else(|e| {
                    error!(target: "certificate_pack", %e, "failed to save certificate");
                    latch_first_error(&tx_error, e);
                });
            }
            PackMessage::Get(digest, tx) => {
                let _ = tx.send(inner.get(digest));
            }
            PackMessage::Contains(digest, tx) => {
                let _ = tx.send(inner.contains(digest));
            }
            PackMessage::Persist(tx) => {
                // Fold a save that failed while this persist was queued into the reply.
                // `persist()` samples the error slot before enqueueing, and saves are
                // fire-and-forget, so a save that fails after that sample but before this arm
                // would otherwise be acknowledged as a successful flush. The slot does not
                // clear on read, so a plain read is enough here.
                let pending = tx_error.borrow().clone();
                let flushed = inner.persist();
                let _ = tx.send(pending.map_or(flushed, Err));
            }
            PackMessage::Shutdown => {
                // The fire-and-forget close from `Drop`: nobody waits for a reply, so the log is
                // the only place a failed final flush shows.
                match inner.persist() {
                    Ok(()) => {
                        info!(target: "certificate_pack", "certificate pack for epoch {} persisted", epoch)
                    }
                    Err(e) => {
                        error!(target: "certificate_pack", %e, epoch, "certificate pack final persist failed")
                    }
                }
                break;
            }
            PackMessage::ShutdownAsync(tx) => {
                // Same fold as `Persist`: `shutdown()` never samples the error slot, so this
                // reply is the only place a save that failed earlier in the queue can surface.
                // Without it the final flush of an epoch reports `Ok(())` for a pack that
                // silently dropped a certificate.
                let pending = tx_error.borrow().clone();
                let flushed = inner.persist();
                let _ = tx.send(pending.map_or(flushed, Err));
                break;
            }
        }
    }
}

impl Drop for CertificatePack {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handle) == 1 {
            // Reaching this with a live handle means shutdown() was not used: a shutdown().await
            // already took the handle, so the block below is skipped. The node's epoch owner
            // (`run_epoch`) holds an `Arc` of the pack and seals it in `finish_epoch` with
            // shutdown().await, the path that reports a failed save or final flush
            // (`seal_certificate_pack`). This covers only a pack whose last handle drops
            // elsewhere (a test, or a task that outlived that seal). Mirrors ConsensusPack /
            // EpochRecordDb / LatestConsensus so all four actors seal consistently on a dropped
            // last reference. A reopen of this epoch waits on the epoch lock until this pack has
            // sealed, however the close is joined.
            if let Some(handle) = self.handle.lock().take() {
                warn!(target: "certificate_pack", "CertificatePack for epoch {} dropped without calling shutdown(); persisting as a fallback", self.epoch);
                if self.tx.try_send(PackMessage::Shutdown).is_err() {
                    // Full bounded channel -- detach. The actor still persists when the last Sender
                    // drops (Inner's Drop clean-closes); only the synchronous "persisted on return"
                    // wait is lost.
                    error!(target: "certificate_pack", "Failed to send shutdown message to CertificatePack; it closes when its channel drops");
                    return;
                }
                let epoch = self.epoch;
                let join = move || {
                    if let Err(e) = handle.join() {
                        error!(target: "certificate_pack", ?e, epoch, "Failed to join certificate pack thread");
                    }
                };
                // Never block a multi-threaded runtime worker on the persist/fsync: offload the
                // join to the blocking pool. On a current-thread runtime (nothing
                // else to starve) or no runtime, a synchronous join keeps
                // "persisted on return" for callers/tests that drop
                // then immediately reopen. `shutdown().await` reports the flush's result.
                match tokio::runtime::Handle::try_current() {
                    Ok(rt) if rt.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
                        rt.spawn_blocking(join);
                    }
                    _ => join(),
                }
            }
        }
    }
}

impl CertificatePack {
    /// Open (or create) a certificate pack at `path` for reading and writing.
    pub fn open<P: AsRef<Path>>(path: P, epoch: Epoch) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        let path = path.as_ref().join(format!("epoch-{epoch}"));
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || match Inner::open(path, false) {
            Ok(inner) => run_pack_loop(inner, rx, tx_error, epoch),
            Err(e) => {
                latch_first_error(&tx_error, e);
                clear_pack_loop(rx);
            }
        });
        Self { tx, handle: Arc::new(Mutex::new(Some(handle))), error, epoch }
    }

    /// Open an existing certificate pack at `path` in read-only mode.
    pub fn open_static<P: AsRef<Path>>(path: P, epoch: Epoch) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        let path = path.as_ref().join(format!("epoch-{epoch}"));
        let (tx_error, error) = watch::channel(None);
        let handle = std::thread::spawn(move || match Inner::open(path, true) {
            Ok(inner) => run_pack_loop(inner, rx, tx_error, epoch),
            Err(e) => {
                latch_first_error(&tx_error, e);
                clear_pack_loop(rx);
            }
        });
        Self { tx, handle: Arc::new(Mutex::new(Some(handle))), error, epoch }
    }

    /// Return any delayed error from a previous background operation.
    ///
    /// The slot does not clear on read. When more than one background operation fails, the
    /// slot keeps the first failure. In the poisoned-pack cascade (a write failure makes
    /// every queued save fail with a copy of that write failure) the first failure is the
    /// root cause. Every failure, kept or not, is logged by the pack loop.
    pub fn get_error(&self) -> Result<(), PackError> {
        match &*self.error.borrow() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Save a certificate into the pack file. The write is backgrounded; any error
    /// from this call (or a prior one) is surfaced via [`get_error`](Self::get_error).
    pub async fn save(&self, cert: Certificate) -> Result<(), PackError> {
        self.get_error()?;
        if self.tx.send(PackMessage::Save(cert)).await.is_err() {
            Err(PackError::SendFailed)
        } else {
            Ok(())
        }
    }

    /// Save a certificate into the pack file. The write is backgrounded; any error
    /// from this call (or a prior one) is surfaced via [`get_error`](Self::get_error).
    /// If the channel to send to the background thread is full will return an error.
    ///
    /// Only [`PackError::SendFull`] is transient: backpressure, this certificate was not queued.
    /// Any other error means the pack has failed for good, and it is this node's own storage
    /// failing: a write, sync or open failure the background thread latched (every later call,
    /// [`persist`](Self::persist) and [`shutdown`](Self::shutdown) return it too), or a thread
    /// that is gone (`SendFailed`). The owner must stop the node then, not carry on without the
    /// pack. A failure in the last saves before the owner stops calling this is reported by
    /// [`shutdown`](Self::shutdown).
    pub fn try_save(&self, cert: Certificate) -> Result<(), PackError> {
        self.get_error()?;
        if let Err(e) = self.tx.try_send(PackMessage::Save(cert)) {
            match e {
                mpsc::error::TrySendError::Full(_) => Err(PackError::SendFull),
                mpsc::error::TrySendError::Closed(_) => Err(PackError::SendFailed),
            }
        } else {
            Ok(())
        }
    }

    /// Return `true` if the pack contains a certificate with the given digest.
    pub async fn contains(&self, digest: HeaderDigest) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::Contains(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Load a certificate by its digest. Returns `None` if not found.
    pub async fn get(&self, digest: HeaderDigest) -> Option<Certificate> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(PackMessage::Get(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Flush the pack file and index to disk.
    ///
    /// Returns `Err` if any background save queued before this call failed. An error that is
    /// already latched when this is called short-circuits before the flush is enqueued (the
    /// slot does not clear on read). A save still queued at that sample is processed before
    /// the flush by the actor's single FIFO channel, and its failure is folded into the flush
    /// reply.
    pub async fn persist(&self) -> Result<(), PackError> {
        self.get_error()?;
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(PackMessage::Persist(tx)).await;
        rx.await.map_err(|_| self.dropped_reply_error())?
    }

    /// What a request whose reply the background thread dropped reports: the error it latched
    /// (the pack failed to open, or a write failed), else [`PackError::ReceiveFailed`].
    fn dropped_reply_error(&self) -> PackError {
        match &*self.error.borrow() {
            Some(e) => e.clone(),
            None => PackError::ReceiveFailed,
        }
    }

    /// Consume and shutdown the pack if this is the last instance (if not the last instance then is
    /// no-op). This is safer than relying on Drop.
    ///
    /// Returns `Err` if the final flush fails or if any earlier background save failed: the
    /// shutdown reply folds in the latched error slot, so a save failure from this epoch is
    /// reported here even though this method never samples the slot itself. A pack that failed to
    /// open sends no reply; this returns its latched open error, as [`Self::persist`] does.
    pub async fn shutdown(self) -> Result<(), PackError> {
        if Arc::strong_count(&self.handle) == 1 {
            let handle = self.handle.lock().take();
            if let Some(handle) = handle {
                let (tx, rx) = oneshot::channel();
                let _ = self.tx.send(PackMessage::ShutdownAsync(tx)).await;
                let reply = rx.await.unwrap_or_else(|_| Err(self.dropped_reply_error()));
                // Join even when the reply is an error: the thread still seals the pack after it
                // replies, and a caller that reopens this epoch next must find it sealed. The
                // thread should be over or ending after the ShutdownAsync but don't block tokio
                // just in case.
                let joined = match tokio::task::spawn_blocking(|| handle.join()).await {
                    Err(e) => {
                        error!(target: "certificate_pack", ?e, "Failed to join certificate pack thread (tokio");
                        Err(PackError::JoinFailed)
                    }
                    Ok(Err(e)) => {
                        error!(target: "certificate_pack", ?e, "Failed to join certificate pack thread");
                        Err(PackError::JoinFailed)
                    }
                    Ok(Ok(())) => Ok(()),
                };
                // The reply's error is the root cause; a join failure is reported only without one.
                return reply.and(joined);
            }
        }
        Ok(())
    }
}

/// The error for a read of the certificate log at `pos` that the disk failed (see
/// [`FetchError::is_read_failure`]): the log is left as it is, for a retry once the disk reads
/// again.
fn read_failure(pos: u64, source: FetchError) -> PackError {
    PackError::ReadError(format!(
        "the certificate log at offset {pos} (a read error, not a torn tail): {source}"
    ))
}

/// Base filename of the certificate pack's data file (`"cert_data"`) within an epoch directory.
pub const DATA_NAME: &str = Inner::DATA_NAME;
/// Directory name of the certificate pack's digest index (`"cert_hash"`) within an epoch directory.
pub const HASH_NAME: &str = Inner::HASH_NAME;

#[derive(Debug)]
struct Inner {
    data: Pack<Certificate>,
    digest_idx: HdxIndex,
    /// Set by a failed save, which invalidates the index's data-length marker; holds the marker
    /// at `0` for the rest of this handle's life, so the next open rebuilds the index from the
    /// log instead of trusting it.
    index_invalidated: bool,
    /// Test-only: when set, the next digest-index save fails right after the data append so tests
    /// can exercise the atomic rollback in [`Self::save`]. Consumed once. (Mirrors
    /// `EpochRecordDb`'s `fail_index_save_after_append`.)
    #[cfg(test)]
    fail_index_save_after_append: bool,
    /// The epoch directory's writer lock, held by a writable open for the handle's whole life
    /// (see [`Self::lock_epoch_dir`]); `None` on a read-only open. Declared LAST: fields drop in
    /// declaration order, so it is released only after `data` and `digest_idx` have sealed.
    _epoch_lock: Option<File>,
}

impl Inner {
    const DATA_NAME: &str = "cert_data";
    const HASH_NAME: &str = "cert_hash";
    const LOCK_NAME: &str = "cert_lock";

    /// Take the epoch directory's exclusive writer lock, waiting while a previous writable handle
    /// on this epoch still holds it.
    ///
    /// A same-epoch reopen can start while the previous handle's thread is still running its
    /// final persist and seal: that thread is detached when its owner is aborted during the
    /// epoch-exit grace, drops the pack (`Drop` joins on the blocking pool), or gets an error from
    /// `shutdown`. Opening the log then would map the old, still capacity-padded file; the old
    /// seal truncates it under that mapping, and the new handle's next append touches a page past
    /// EOF (SIGBUS). The old thread's last certs would also race the new handle's rebuild.
    ///
    /// The lock is `flock(LOCK_EX)` (what [`File::lock`] is on Linux and macOS) on a file of its
    /// own. It belongs to the open file description, so two opens in one process exclude each
    /// other, and the kernel drops it when the holder's file closes, crash included, so a stale
    /// lock file never blocks. The wait runs on the pack's own thread; meanwhile saves queue, and
    /// `try_save` reports `SendFull` once the channel fills.
    fn lock_epoch_dir(base_dir: &Path) -> Result<File, PackError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(base_dir.join(Self::LOCK_NAME))?;
        loop {
            match file.lock() {
                Ok(()) => return Ok(file),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// Open the certificate digest index. Factored out so [`Self::reset_index`] can reopen a fresh
    /// copy after wiping the sidecar directory.
    fn try_open_index(
        base_dir: &Path,
        data: &Pack<Certificate>,
        read_only: bool,
    ) -> Result<HdxIndex, PackError> {
        HdxIndex::open_hdx_file(
            base_dir.join(Self::HASH_NAME),
            data.header(),
            BuildHasherDefault::<FxHasher>::default(),
            read_only,
        )
        .map_err(OpenError::IndexFileOpen)
        .map_err(Into::into)
    }

    /// Discard the digest index sidecar directory and reopen a fresh (empty) copy for rebuild. A
    /// missing directory is tolerated. Mirrors `ConsensusPack`'s index reset
    /// (`open_indexes_for_append`).
    fn reset_index(base_dir: &Path, data: &Pack<Certificate>) -> Result<HdxIndex, PackError> {
        match std::fs::remove_dir_all(base_dir.join(Self::HASH_NAME)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Self::try_open_index(base_dir, data, false)
    }

    /// Rebuild the digest index by replaying the data log, the authoritative source (mirrors
    /// `ConsensusPack::recover_pack`). Capture `logical_position()` before each `next()` as the
    /// cert's stored offset, advance the consistent end only past a fully-decoded cert, and stop at
    /// the first torn/short cert so an incomplete tail is dropped; then roll the log back to that
    /// end, flush the zeros written over the cut, and reconcile the digest marker. Keyed by
    /// `Certificate::digest`, matching [`Self::save`]. A read the disk fails is an error, never a
    /// tear: the log is left as it is for a retry.
    fn rebuild_index(
        data: &mut Pack<Certificate>,
        digest_idx: &mut HdxIndex,
    ) -> Result<(), PackError> {
        // A cleanly-SEALED log is complete by construction (the clean-close sentinel is written
        // only after the tail is msync'd and truncated to `end`), so any tear in it is
        // at-rest corruption of committed certificates, not a truncatable unacked tail.
        // Only an unclean log can have a truncatable tail. (Mirrors the ConsensusPack /
        // EpochRecordDb sealed-log discipline.)
        let sealed = !data.opened_unclean();
        let mut iter = data.raw_iter().map_err(OpenError::DataFileOpen)?;
        let mut consistent_end = iter.logical_position();
        let mut torn = false;
        loop {
            let pos = iter.logical_position();
            match iter.next() {
                None => break,
                Some(Ok(cert)) => {
                    let digest = B256::from_slice(cert.digest().as_ref());
                    digest_idx
                        .save(digest, pos)
                        .map_err(|e| PackError::IndexAppend(e.to_string()))?;
                    consistent_end = iter.logical_position();
                }
                Some(Err(e)) if e.is_read_failure() => return Err(read_failure(pos, e)),
                Some(Err(_)) => {
                    torn = true;
                    break;
                }
            }
        }
        // `iter` owns a cloned file handle, but drop it before `rewind_to` for clarity.
        drop(iter);
        let file_len = data.file_len();
        if torn && consistent_end < file_len {
            let dropped = file_len - consistent_end;
            if sealed {
                // Fail closed rather than zeroing committed, durably-acked certificates (INV4).
                return Err(PackError::CorruptLog(format!(
                    "a record at offset {consistent_end} failed to decode in a cleanly-sealed \
                     certificate log; {dropped} committed byte(s) follow it. This is at-rest \
                     corruption, not a torn tail — refusing to truncate."
                )));
            }
            // An unclean log's torn tail is an unacked partial write: drop it, but say so (INV1).
            warn!(
                target: "cert-pack",
                consistent_end,
                file_len,
                dropped_bytes = dropped,
                "certificate log has a torn tail; truncating the unacked bytes and rebuilding the index"
            );
            if let Some(cut) = data.rewind_to(consistent_end) {
                // The unclean open's sync made the torn tail durable, and its zeros are only in
                // the mapping: flush them now, or a power loss before writeback could bring stale
                // certificates back behind the ones written next (mirrors tndb's `recover`).
                data.view().msync(cut.start, cut.end)?;
            }
        }
        digest_idx.set_data_file_length(data.file_len());
        Ok(())
    }

    /// Before an UNCLEAN log's index is discarded and rebuilt, refuse (`CorruptLog`, nothing
    /// changed) if the rebuild would drop an ACKED cert. The rebuild truncates at the first
    /// undecodable record, which is right for an unacked torn tail but zeroes acked certs when
    /// the damage lies below the acked end. Either of these proves a record past the tear was
    /// acked (mirrors `EpochRecordDb::refuse_dropping_acked` and `ConsensusPack`'s recovery):
    /// - a cert after the tear decodes and ends within `attested_end`, the index's durably synced
    ///   data length. The walk skips undecodable frames while it makes forward progress. Records
    ///   past `attested_end` were never acked (a crash can persist unacked ones out of order), so
    ///   they do not count;
    /// - the log's tail commit marker (stamped by [`Self::persist`]) lies past the end the rebuild
    ///   would keep. This holds even with nothing decodable after the damage and no readable index.
    ///
    /// `attested_end` must be read before the index is reset, so a retry refuses the same way. A
    /// `0` (invalidated or unreadable index) attests nothing; the commit marker still does. A read
    /// the disk fails is an error, never a tear: the walk cannot tell what lies behind it.
    fn refuse_dropping_acked(data: &Pack<Certificate>, attested_end: u64) -> Result<(), PackError> {
        let mut iter = data.raw_iter().map_err(OpenError::DataFileOpen)?;
        let mut tear: Option<u64> = None;
        let mut last_pos = iter.logical_position();
        let mut acked_after_tear = false;
        loop {
            match iter.next() {
                None => break,
                Some(Ok(_)) => {
                    let end = iter.logical_position();
                    if tear.is_some() && end <= attested_end {
                        acked_after_tear = true;
                        break;
                    }
                    last_pos = end;
                }
                Some(Err(e)) if e.is_read_failure() => {
                    return Err(read_failure(iter.logical_position(), e));
                }
                Some(Err(_)) => {
                    tear.get_or_insert(last_pos);
                    let pos = iter.logical_position();
                    if pos <= last_pos {
                        break; // no forward progress: the walk cannot see past here
                    }
                    last_pos = pos;
                }
            }
        }
        // The rebuild keeps the log up to its first undecodable record (or all of it).
        let kept_end = tear.unwrap_or(last_pos);
        let committed = data.committed_end();
        if acked_after_tear || committed.is_some_and(|committed| kept_end < committed) {
            return Err(PackError::CorruptLog(format!(
                "an unclean certificate log stops decoding at offset {kept_end}, below acked data \
                 (index attests {attested_end}, commit marker {committed:?}); at-rest corruption \
                 of acked certificates, refusing to truncate"
            )));
        }
        Ok(())
    }

    fn open<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, PackError> {
        let base_dir = path.as_ref();
        if !read_only {
            let _ = std::fs::create_dir_all(base_dir);
        }
        // Before touching any pack file: wait out a previous writer of this epoch.
        let epoch_lock = if read_only { None } else { Some(Self::lock_epoch_dir(base_dir)?) };
        let fresh = !base_dir.join(Self::DATA_NAME).exists();
        let mut data: Pack<Certificate> = Pack::open(
            base_dir.join(Self::DATA_NAME),
            0,
            read_only,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;

        // Open the digest index; on a writable open, if it is unreadable discard it and rebuild
        // from the data log below rather than latching (a corrupt sidecar index must not
        // silently disable cert archiving -- the data log is authoritative). A read-only
        // open surfaces the error as before (it cannot mutate to repair).
        let mut digest_idx = if fresh && !read_only {
            // A new log starts with a new index. A leftover `cert_hash/` (only the data file was
            // deleted) opens fine, as its header matches any cert log, but its entries point
            // into a log that no longer exists.
            Self::reset_index(base_dir, &data)?
        } else {
            match Self::try_open_index(base_dir, &data, read_only) {
                Ok(idx) => idx,
                Err(e) if !read_only => {
                    warn!(
                        target: "cert-pack",
                        "certificate index failed to open ({e}); discarding and rebuilding from \
                         the data log"
                    );
                    Self::reset_index(base_dir, &data)?
                }
                Err(e) => return Err(e),
            }
        };

        if !read_only {
            // On a brand-new pack, initialise the index's tracked length so the consistency check
            // below does not treat it as unclean. Gate on the data file being new, not on the
            // marker's value: a failed save deliberately leaves the marker at `0` to force a
            // rebuild on the next open, and that must not be mistaken for a fresh index.
            if fresh {
                digest_idx.set_data_file_length(data.file_len());
            }
            // If the pack or index was not cleanly sealed, or the index's tracked data length
            // disagrees with the log (a crash mid-write, or an hdx split whose new buckets reached
            // disk but whose header/`data_file_length` did not -- silently dropping the moved
            // keys), the index may be stale or torn. The data log is authoritative, so
            // discard the index and rebuild by replaying the log (mirrors
            // `ConsensusPack::recover_pack`).
            let unclean = data.opened_unclean()
                || digest_idx.opened_unclean()
                || digest_idx.data_file_length() != data.file_len();
            if unclean {
                // Validate before mutating: the index's attested length is gone once it is reset.
                if data.opened_unclean() {
                    Self::refuse_dropping_acked(&data, digest_idx.data_file_length())?;
                }
                digest_idx = Self::reset_index(base_dir, &data)?;
                Self::rebuild_index(&mut data, &mut digest_idx)?;
                // Recovery made the log + rebuilt index self-consistent; clear their unclean flags
                // so the clean `Drop` re-seals them and the next open skips this
                // rebuild (rather than rebuilding on every restart). A durability
                // failure still blocks the seal.
                data.mark_consistent();
                digest_idx.mark_consistent();
            }
        }
        Ok(Self {
            data,
            digest_idx,
            index_invalidated: false,
            #[cfg(test)]
            fail_index_save_after_append: false,
            _epoch_lock: epoch_lock,
        })
    }

    fn save(&mut self, cert: &Certificate) -> Result<(), PackError> {
        let digest = B256::from_slice(cert.digest().as_ref());
        // Idempotent: skip if already stored. `get` bounds the entry's offset by the log's end
        // and checks the stored cert's digest, so a stale entry (one whose append was rolled back
        // after its index write landed) does not turn a real save into a no-op.
        if self.get(cert.digest()).is_some() {
            return Ok(());
        }
        // Atomic append+index (same rollback discipline as `EpochRecordDb::save_cert_atomic`): if
        // the index write fails after the append lands, roll the log back to before the orphaned
        // append and invalidate the index's data-length marker (`0` is never a valid length -> the
        // next open's `data_file_length() != file_len()` check fails -> rebuild from the log).
        // Without this a post-append index failure would orphan the cert bytes and a retry
        // would append a duplicate (the index write never happened, so the idempotent check
        // above misses).
        let data_start = self.data.file_len();
        if let Err(e) = self.append_and_index(digest, cert) {
            Self::rewind_after_failed_save(&mut self.data, data_start);
            self.index_invalidated = true;
            self.digest_idx.set_data_file_length(0);
            return Err(e);
        }
        // Once invalidated, the marker stays at `0` until a reopen rebuilds the index: a later
        // successful save must not make a possibly damaged index look trusted again.
        if !self.index_invalidated {
            self.digest_idx.set_data_file_length(self.data.file_len());
        }
        Ok(())
    }

    /// Roll `data` back to `to` after a failed save (see [`Pack::rewind_to`]) and flush the zeros
    /// written over the abandoned bytes: they may already be on disk (the system's writeback), and
    /// a power loss could otherwise bring the failed save back. A failed flush poisons the log, so
    /// it refuses later saves and is not sealed: the next open recovers it.
    fn rewind_after_failed_save(data: &mut Pack<Certificate>, to: u64) {
        if let Some(cut) = data.rewind_to(to) {
            if let Err(e) = data.view().msync(cut.start, cut.end) {
                error!(target: "certificate_pack", %e, "failed to flush a failed save's rollback; the log refuses saves until reopened");
                data.poison();
            }
        }
    }

    /// Append `cert` to the data log and index it by `digest`. Split out from [`Self::save`] so the
    /// caller can roll back atomically on any failure.
    fn append_and_index(&mut self, digest: B256, cert: &Certificate) -> Result<(), PackError> {
        let position = self.data.append(cert).map_err(|e| PackError::Append(e.to_string()))?;
        // Test-only injection: fail the index save after the append to exercise the rollback.
        #[cfg(test)]
        if std::mem::take(&mut self.fail_index_save_after_append) {
            return Err(PackError::IndexAppend("injected post-append index failure".to_string()));
        }
        self.digest_idx
            .save(digest, position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        Ok(())
    }

    fn contains(&mut self, digest: HeaderDigest) -> bool {
        if let Ok(pos) = self.digest_idx.load(B256::from_slice(digest.as_ref())) {
            pos < self.data.file_len()
        } else {
            false
        }
    }

    fn get(&mut self, digest: HeaderDigest) -> Option<Certificate> {
        let b256 = B256::from_slice(digest.as_ref());
        let pos = self.digest_idx.load(b256).ok()?;
        if pos >= self.data.file_len() {
            return None;
        }
        let cert = self.data.fetch(pos).ok()?;
        // Verify digest to guard against the extremely unlikely case where a repaired file
        // wrote a different certificate to the same file offset as an old one.
        if cert.digest() != digest {
            return None;
        }
        Some(cert)
    }

    fn persist(&mut self) -> Result<(), PackError> {
        if !self.data.read_only() {
            self.data.commit().map_err(|e| PackError::PersistError(e.to_string()))?;
            // Stamp the log's tail commit marker AFTER its msync, so it can never point past
            // durable data. Best-effort and sync-free: an index-free record of the acked end that
            // a rebuild refuses to truncate below (see `refuse_dropping_acked`).
            self.data.stamp_commit_marker();
            self.digest_idx.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        }
        Ok(())
    }
}

/// Error type for [`CertificatePack`] operations, surfaced by the background pack loop.
#[derive(Debug, Clone)]
pub enum PackError {
    /// An underlying I/O error touching the pack's files.
    IO(Arc<io::Error>),
    /// Appending a certificate to the data file failed; holds the source error's text.
    Append(String),
    /// Recording a saved certificate's offset in the digest index failed; holds the source
    /// error's text.
    IndexAppend(String),
    /// Opening (or creating) the pack's data file or digest index failed.
    Open(Arc<OpenError>),
    /// Reading/fetching a certificate back from the data file failed; holds the source error's
    /// text.
    ReadError(String),
    /// The channel to the background pack thread is closed, so the message could not be sent.
    SendFailed,
    /// The bounded channel to the background pack thread is full
    /// ([`try_save`](CertificatePack::try_save) only).
    SendFull,
    /// The background pack thread's reply was dropped before it could be received.
    ReceiveFailed,
    /// Flushing the data file and/or syncing the index to disk failed; holds the source error's
    /// text.
    PersistError(String),
    /// The pack file was detected to be corrupt.
    CorruptPack,
    /// The data log stopped decoding partway through committed certificates: in a cleanly-sealed
    /// log, or in an unclean one below its acked end. At-rest corruption that must not be silently
    /// truncated (INV4). Holds an operator-facing description.
    CorruptLog(String),
    /// The background pack thread failed to join on shutdown.
    JoinFailed,
}

impl Error for PackError {}

impl Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::IO(e) => write!(f, "IO({e})"),
            PackError::Append(e) => write!(f, "Data Append Error ({e})"),
            PackError::IndexAppend(e) => write!(f, "Index Append Error ({e})"),
            PackError::Open(e) => write!(f, "Open Error {e}"),
            PackError::ReadError(e) => write!(f, "Read Error {e}"),
            PackError::SendFailed => write!(f, "Internal channel send failed"),
            PackError::SendFull => write!(f, "Internal channel send is full"),
            PackError::ReceiveFailed => write!(f, "Internal channel receive failed"),
            PackError::PersistError(e) => write!(f, "Failed to persist: {e}"),
            PackError::CorruptPack => write!(f, "Pack file is corrupt"),
            PackError::CorruptLog(e) => write!(f, "Certificate log corrupt: {e}"),
            PackError::JoinFailed => write!(f, "Pack file thread failed to join"),
        }
    }
}

impl From<OpenError> for PackError {
    fn from(value: OpenError) -> Self {
        Self::Open(Arc::new(value))
    }
}

impl From<FetchError> for PackError {
    fn from(value: FetchError) -> Self {
        Self::ReadError(value.to_string())
    }
}

impl From<io::Error> for PackError {
    fn from(value: io::Error) -> Self {
        Self::IO(Arc::new(value))
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use parking_lot::Mutex;
    use tempfile::TempDir;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{Certificate, Hash, HeaderDigest, B256};

    use crate::{
        archive::{error::insert::AppendError, index::Index as _},
        certificate_pack::CertificatePack,
        mem_db::MemDatabase,
    };

    fn make_test_cert(fixture: &CommitteeFixture<MemDatabase>, index: usize) -> Certificate {
        let mut cert = Certificate::default();
        cert.update_header_author_for_test(
            fixture.committee().authorities().get(index % 4).expect("authority").id(),
        );
        cert.update_header_epoch_for_test(fixture.committee().epoch());
        cert
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_certificate_pack_basic() {
        let temp_dir = TempDir::with_prefix("test_certificate_pack").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();

        let pack = CertificatePack::open(temp_dir.path(), 0);

        let num_certs = 100;
        let mut certs = Vec::new();
        for i in 0..num_certs {
            let cert = make_test_cert(&fixture, i);
            certs.push(cert.clone());
            pack.save(cert).await.expect("save cert");
        }

        pack.persist().await.expect("persist");

        for cert in &certs {
            let digest = cert.digest();
            assert!(pack.contains(digest).await, "should contain cert");
            let loaded = pack.get(digest).await.expect("should load cert");
            assert_eq!(loaded.digest(), digest, "loaded cert digest mismatch");
        }

        // Non-existent digest returns None.
        assert!(!pack.contains(HeaderDigest::default()).await);
        assert!(pack.get(HeaderDigest::default()).await.is_none());

        pack.shutdown().await.unwrap();

        // Reopen and verify certs are still there.
        let pack = CertificatePack::open(temp_dir.path(), 0);
        for cert in &certs {
            assert!(pack.contains(cert.digest()).await, "should still contain cert after reopen");
        }

        drop(pack);

        // Open read-only.
        let pack = CertificatePack::open_static(temp_dir.path(), 0);
        for cert in &certs {
            let digest = cert.digest();
            let loaded = pack.get(digest).await.expect("should load cert read-only");
            assert_eq!(loaded.digest(), digest);
        }
    }

    /// Like [`make_test_cert`] but varies the header round so each cert has a distinct digest
    /// (`make_test_cert` alone only cycles four authors -> four unique digests).
    fn make_unique_cert(fixture: &CommitteeFixture<MemDatabase>, i: usize) -> Certificate {
        let mut cert = make_test_cert(fixture, i);
        cert.update_header_round_for_test(i as u32 + 1);
        cert
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_certificate_index_open_failure_rebuilds() {
        // Regression for finding #8(b): an unreadable digest index must not latch and silently
        // disable cert archiving. The data log is authoritative, so a corrupt index is discarded
        // and rebuilt from it (was: `OpenError::IndexFileOpen` -> archiving disabled for
        // the epoch).
        let temp_dir = TempDir::with_prefix("cert_index_open_failure").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        // Populate and cleanly close (Drop commits + syncs the index).
        let mut certs = Vec::new();
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..32usize {
                let cert = make_unique_cert(&fixture, i);
                inner.save(&cert).expect("save cert");
                certs.push(cert);
            }
            inner.persist().expect("persist");
        }

        // Corrupt the digest index header so it fails to open (mirrors `break_index_file`).
        let hdx = dir.join(super::Inner::HASH_NAME).join("index.hdx");
        let f = std::fs::OpenOptions::new().write(true).open(&hdx).expect("open hdx to corrupt");
        f.set_len(4).expect("truncate hdx header");
        drop(f);

        // Reopen writable: the unreadable index is discarded and rebuilt from the data log, so the
        // open succeeds and every cert is reachable.
        let mut inner = super::Inner::open(&dir, false).expect("reopen must rebuild the index");
        for cert in &certs {
            assert!(inner.contains(cert.digest()), "cert missing after rebuild");
            let loaded = inner.get(cert.digest()).expect("cert should load after rebuild");
            assert_eq!(loaded.digest(), cert.digest());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_certificate_unclean_reopen_rebuilds() {
        // Regression for finding #8(a): after an unclean shutdown a digest index can be stale (an
        // hdx split whose new buckets reached disk but whose header did not), silently losing keys.
        // A writable reopen must detect the unclean state and rebuild the index from the
        // authoritative data log, so every persisted cert stays reachable and a re-save is
        // idempotent (no duplicate appended -- the #8 duplicate-append symptom).
        let temp_dir = TempDir::with_prefix("cert_unclean_rebuild").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        // Populate + persist (durable log + synced marker), then skip the clean close: models a
        // crash -- durable log, no sentinel.
        let mut certs = Vec::new();
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..32usize {
                let cert = make_unique_cert(&fixture, i);
                inner.save(&cert).expect("save cert");
                certs.push(cert);
            }
            inner.persist().expect("persist");
            simulate_crash(inner);
        }

        // Reopen: unclean (no sentinel) -> index rebuilt from the log; every cert reachable.
        let mut inner = super::Inner::open(&dir, false).expect("reopen after unclean exit");
        for cert in &certs {
            assert!(inner.contains(cert.digest()), "cert missing after rebuild");
        }

        // Re-saving an already-stored cert must be idempotent: the rebuilt index resolves the key,
        // so no duplicate is appended (the logical log length stays put).
        let len_before = inner.data.file_len();
        inner.save(&certs[0]).expect("idempotent re-save");
        assert_eq!(inner.data.file_len(), len_before, "re-save must not append a duplicate cert");
    }

    /// A post-append index-write failure must roll the data log back (no orphan) so a retry
    /// re-appends cleanly with no duplicate, and a reopen still finds every cert. Regression for
    /// the `CertificatePack::save` non-atomicity: pre-fix, the failed attempt left orphan bytes
    /// in the log (`file_len() > len_before`) and the retry appended a second copy.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cert_save_atomic_rollback_and_retry() {
        let temp_dir = TempDir::with_prefix("cert_save_atomic").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");
        let mut inner = super::Inner::open(&dir, false).expect("open pack");

        // Save a few certs cleanly.
        let mut certs = Vec::new();
        for i in 0..4usize {
            let cert = make_unique_cert(&fixture, i);
            inner.save(&cert).expect("save");
            certs.push(cert);
        }
        let len_before = inner.data.file_len();

        // Arm the injected post-append index failure and attempt one more cert.
        let extra = make_unique_cert(&fixture, 4);
        inner.fail_index_save_after_append = true;
        let err = inner.save(&extra).expect_err("index save must fail");
        assert!(matches!(err, super::PackError::IndexAppend(_)), "unexpected error: {err:?}");
        // Rolled back: no orphan bytes left in the log, and the failed cert is not resolvable.
        assert_eq!(inner.data.file_len(), len_before, "data log must be rewound (no orphan)");
        assert!(!inner.contains(extra.digest()), "failed cert must not be indexed");

        // Retry (the flag auto-cleared) re-appends at the same offset and succeeds, no duplicate.
        inner.save(&extra).expect("retry save");
        assert!(inner.contains(extra.digest()), "cert present after retry");
        certs.push(extra);
        inner.persist().expect("persist");
        drop(inner); // clean close (seals)

        // Reopen: every saved cert is present (and the rollback+retry left the log rebuildable).
        let mut inner = super::Inner::open(&dir, false).expect("reopen");
        for cert in &certs {
            assert!(inner.contains(cert.digest()), "cert missing after reopen");
        }
    }

    /// A cleanly-SEALED cert log that has a mid-log corrupt record must NOT be silently
    /// truncated on a rebuild — that would zero every committed cert after the first bad one.
    /// The rebuild fails closed (`CorruptLog`) and leaves the data file untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cert_rebuild_sealed_log_tear_fails_closed() {
        let temp_dir = TempDir::with_prefix("cert_sealed_tear").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        // Populate and cleanly close (normal drop seals the data file with the clean-close
        // sentinel).
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..8usize {
                inner.save(&make_unique_cert(&fixture, i)).expect("save cert");
            }
            inner.persist().expect("persist");
        }

        let data_path = dir.join(super::Inner::DATA_NAME);
        let len_before = std::fs::metadata(&data_path).expect("meta").len();

        // Corrupt a mid-log record (avoid the 28-byte header and the trailing 8-byte sentinel).
        {
            let mut bytes = std::fs::read(&data_path).expect("read data");
            let mid = bytes.len() / 2;
            bytes[mid] ^= 0xFF;
            std::fs::write(&data_path, &bytes).expect("write corrupted data");
        }
        // Break the digest index so the reopen is forced to rebuild from the (sealed) data log.
        let hdx = dir.join(super::Inner::HASH_NAME).join("index.hdx");
        let f = std::fs::OpenOptions::new().write(true).open(&hdx).expect("open hdx");
        f.set_len(4).expect("truncate hdx header");
        drop(f);

        // Rebuild hits the corrupt record in a SEALED log: fail closed, do not truncate committed
        // data.
        match super::Inner::open(&dir, false) {
            Err(super::PackError::CorruptLog(_)) => {}
            other => panic!("a sealed-log tear must be CorruptLog, got {other:?}"),
        }
        assert_eq!(
            std::fs::metadata(&data_path).expect("meta").len(),
            len_before,
            "a sealed cert log must not be truncated"
        );
    }

    /// An UNCLEAN cert log with a torn tail (an unacked partial write) is truncated back to
    /// the last complete cert on rebuild — the surviving certs stay, the torn one is dropped,
    /// and no error.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cert_rebuild_unclean_torn_tail_truncates() {
        let temp_dir = TempDir::with_prefix("cert_unclean_tear").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        let mut certs = Vec::new();
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..8usize {
                let cert = make_unique_cert(&fixture, i);
                inner.save(&cert).expect("save cert");
                certs.push(cert);
            }
            inner.persist().expect("persist");
            // Normal drop cleanly closes: truncates the mmap padding to the exact logical end and
            // writes the 8-byte clean-close sentinel. This leaves the file with NO padding, so the
            // strip below lands inside the last real record rather than in padding.
        }

        // Make it unclean with a torn final record: remove the sentinel (8 bytes) AND one more
        // byte, which falls inside the last cert's CRC so its read comes up short (a torn
        // unacked tail).
        let data_path = dir.join(super::Inner::DATA_NAME);
        {
            let f = std::fs::OpenOptions::new().write(true).open(&data_path).expect("open data");
            let len = f.metadata().expect("meta").len();
            f.set_len(len - crate::archive::data_file::SENTINEL_LEN - 1)
                .expect("strip sentinel + 1");
        }

        // Unclean + torn tail: rebuild truncates the incomplete last cert and keeps the rest (no
        // error).
        let mut inner = super::Inner::open(&dir, false).expect("reopen truncates the torn tail");
        assert!(inner.contains(certs[0].digest()), "surviving certs must remain");
        assert!(inner.contains(certs[6].digest()), "surviving certs must remain");
        assert!(
            !inner.contains(certs[7].digest()),
            "the torn final cert must be dropped by the tail truncation"
        );
    }

    /// Model a crash: skip `inner`'s clean close, so neither the log nor the index is sealed, but
    /// release the epoch lock as the dying process's closed descriptors would.
    fn simulate_crash(mut inner: super::Inner) {
        drop(inner._epoch_lock.take());
        std::mem::forget(inner);
    }

    /// An undecodable record below the acked end of an UNCLEAN log is at-rest damage, not a torn
    /// tail: the rebuild must refuse (`CorruptLog`) rather than truncate the acked certs after it,
    /// and must leave the log untouched so a retry sees the same evidence.
    ///
    /// Each piece of evidence must refuse on its own: the index's attested length (with the log's
    /// commit marker wiped), and the commit marker (with the index unreadable).
    #[tokio::test(flavor = "multi_thread")]
    async fn unclean_rebuild_refuses_to_drop_acked_certs() {
        use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

        /// Which evidence of the ack the reopen is left with.
        #[derive(Debug, Clone, Copy)]
        enum Evidence {
            Both,
            IndexOnly,
            MarkerOnly,
        }

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        for evidence in [Evidence::Both, Evidence::IndexOnly, Evidence::MarkerOnly] {
            let temp_dir = TempDir::with_prefix("cert_unclean_acked_damage").expect("temp dir");
            let dir = temp_dir.path().join("epoch-0");

            // Persist (ack) every cert, then crash before the clean close. Note where the 9th
            // cert's record lies, to damage its body (not its length prefix, so a walk past the
            // damage still finds the acked certs after it).
            let (damaged_from, damaged_to) = {
                let mut inner = super::Inner::open(&dir, false).expect("open pack");
                let mut positions = Vec::new();
                for i in 0..16usize {
                    let cert = make_unique_cert(&fixture, i);
                    inner.save(&cert).expect("save cert");
                    let digest = B256::from_slice(cert.digest().as_ref());
                    positions.push(inner.digest_idx.load(digest).expect("indexed"));
                }
                inner.persist().expect("persist");
                simulate_crash(inner);
                (positions[8], positions[9])
            };

            let data_path = dir.join(super::Inner::DATA_NAME);
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)
                .expect("open data");
            let at = SeekFrom::Start((damaged_from + damaged_to) / 2);
            let mut byte = [0_u8; 1];
            f.seek(at).expect("seek");
            f.read_exact(&mut byte).expect("read byte");
            f.seek(at).expect("seek");
            f.write_all(&[byte[0] ^ 0xFF]).expect("write damaged byte");
            match evidence {
                Evidence::Both => {}
                Evidence::IndexOnly => {
                    // The commit marker sits in the last bytes of the padded, unsealed file.
                    let len = f.metadata().expect("meta").len();
                    f.seek(SeekFrom::Start(len - crate::archive::data_file::COMMIT_MARKER_LEN))
                        .expect("seek");
                    f.write_all(&[0_u8; crate::archive::data_file::COMMIT_MARKER_LEN as usize])
                        .expect("wipe the commit marker");
                }
                Evidence::MarkerOnly => {
                    let hdx = dir.join(super::Inner::HASH_NAME).join("index.hdx");
                    let idx = std::fs::OpenOptions::new().write(true).open(&hdx).expect("hdx");
                    idx.set_len(4).expect("truncate the hdx header");
                }
            }
            drop(f);
            let before = std::fs::read(&data_path).expect("read data");

            match super::Inner::open(&dir, false) {
                Err(super::PackError::CorruptLog(_)) => {}
                other => {
                    panic!("{evidence:?}: damage below the acked end must refuse, got {other:?}")
                }
            }
            assert_eq!(
                std::fs::read(&data_path).expect("reread data"),
                before,
                "{evidence:?}: a refused rebuild must not touch the log"
            );
        }
    }

    /// A failed save invalidates the index's data-length marker, and a later successful save in
    /// the same session must not restore it: the index stays untrusted until a reopen rebuilds it.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_save_keeps_the_index_invalidated_until_reopen() {
        let temp_dir = TempDir::with_prefix("cert_sticky_invalidation").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        let mut certs = Vec::new();
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..4usize {
                let cert = make_unique_cert(&fixture, i);
                inner.save(&cert).expect("save");
                certs.push(cert);
            }
            inner.fail_index_save_after_append = true;
            inner.save(&make_unique_cert(&fixture, 4)).expect_err("index save must fail");
            // The next queued save succeeds.
            let cert = make_unique_cert(&fixture, 5);
            inner.save(&cert).expect("save after the failure");
            certs.push(cert);
            inner.persist().expect("persist");
            // Clean close.
        }

        // The index still carries the invalidated marker, so it is not trusted on reopen.
        let inner = super::Inner::open(&dir, true).expect("read-only open");
        assert_eq!(inner.digest_idx.data_file_length(), 0, "the index must stay invalidated");
        drop(inner);

        // A writable reopen rebuilds the index from the log.
        let mut inner = super::Inner::open(&dir, false).expect("reopen");
        for cert in &certs {
            assert!(inner.get(cert.digest()).is_some(), "cert missing after the rebuild");
        }
    }

    /// The idempotency check in `save` must not trust a bare index hit: an entry whose offset holds
    /// a different cert (or lies past the log's end) does not mean the cert is stored.
    #[tokio::test(flavor = "multi_thread")]
    async fn save_does_not_trust_a_stale_index_entry() {
        let temp_dir = TempDir::with_prefix("cert_stale_index_entry").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let mut inner =
            super::Inner::open(temp_dir.path().join("epoch-0"), false).expect("open pack");
        let first = make_unique_cert(&fixture, 0);
        inner.save(&first).expect("save");
        let first_pos = inner
            .digest_idx
            .load(B256::from_slice(first.digest().as_ref()))
            .expect("first cert indexed");

        // An entry for a cert the log does not hold, pointing at another cert's record.
        let cert = make_unique_cert(&fixture, 1);
        inner
            .digest_idx
            .save(B256::from_slice(cert.digest().as_ref()), first_pos)
            .expect("plant a stale entry");

        inner.save(&cert).expect("save");
        assert_eq!(
            inner.get(cert.digest()).map(|c| c.digest()),
            Some(cert.digest()),
            "a stale index entry must not make the save a no-op"
        );
    }

    /// A new data file paired with a leftover `cert_hash/` (only the data file was deleted) must
    /// not inherit the old index: its entries point into a log that no longer exists.
    #[tokio::test(flavor = "multi_thread")]
    async fn fresh_data_file_discards_a_leftover_index() {
        let temp_dir = TempDir::with_prefix("cert_leftover_index").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");

        // A populated pack, cleanly closed.
        {
            let mut inner = super::Inner::open(&dir, false).expect("open pack");
            for i in 0..8usize {
                inner.save(&make_unique_cert(&fixture, i)).expect("save cert");
            }
            inner.persist().expect("persist");
        }
        std::fs::remove_file(dir.join(super::Inner::DATA_NAME)).expect("remove the data file");

        // A new log, filled past where the old certs sat.
        let mut inner = super::Inner::open(&dir, false).expect("open over a leftover index");
        for i in 100..116usize {
            inner.save(&make_unique_cert(&fixture, i)).expect("save cert");
        }
        for i in 0..8usize {
            assert!(
                !inner.contains(make_unique_cert(&fixture, i).digest()),
                "cert {i} was never written to this log"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queued_saves_after_a_failed_write_keep_the_root_cause() {
        // Regression test for #1148: a real write failure poisons the pack, and every save
        // queued behind it fails on the poisoned-pack guard. Before the fix that guard
        // returned `AppendError::ReadOnly` and the loop latched last-write-wins: the
        // follow-on "read only" error overwrote the root cause before any reader could
        // observe it, and the latch then reported a read-only condition on a pack that was
        // opened read-write, forever. Now the guard replays the root cause and the latch
        // keeps the first error; both layers report the root cause here, so this test stays
        // green when either layer alone is reverted and fails only when both revert. The
        // per-layer kills live in the error_latch and epoch_records unit tests.
        let temp_dir = TempDir::with_prefix("queued_saves_root_cause").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();

        // Build the actor by hand so the test can arm the write-failure injector before the
        // loop takes ownership of the pack.
        let mut inner =
            super::Inner::open(temp_dir.path().join("epoch-0"), false).expect("open pack inner");
        inner.data.fail_next_append_for_test();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let (tx_error, error) = tokio::sync::watch::channel(None);
        let handle = std::thread::spawn(move || super::run_pack_loop(inner, rx, tx_error, 0));

        // Two saves back to back with no reader between them. The first fails with the
        // injected io write failure and poisons the pack; the second fails on the
        // poisoned-pack guard. A single consumer draining a FIFO channel guarantees the
        // order, so this is deterministic rather than a race.
        tx.send(super::PackMessage::Save(make_test_cert(&fixture, 0)))
            .await
            .expect("queue first failing save");
        tx.send(super::PackMessage::Save(make_test_cert(&fixture, 1)))
            .await
            .expect("queue second failing save");
        // A round-trip through the actor proves both saves were processed.
        let (tx_contains, rx_contains) = tokio::sync::oneshot::channel();
        tx.send(super::PackMessage::Contains(HeaderDigest::default(), tx_contains))
            .await
            .expect("queue contains barrier");
        let _ = rx_contains.await.expect("actor replied to the barrier");

        let latched = error.borrow().clone().expect("a failed save latched an error");
        assert!(
            matches!(latched, super::PackError::Append(_)),
            "latch holds the append failure, got: {latched:?}"
        );
        let text = latched.to_string();
        assert!(
            text.contains("injected write failure"),
            "latch must keep the root cause, got: {text}"
        );
        assert!(
            !text.contains("read only"),
            "latch must not report the follow-on read-only error, got: {text}"
        );
        // Positive control for the negative assertion above: the read-only guard error
        // really renders as "read only" today, so a later Display reword cannot make that
        // check pass vacuously without failing here.
        assert_eq!(AppendError::ReadOnly.to_string(), "read only");

        tx.send(super::PackMessage::Shutdown).await.expect("queue shutdown");
        tokio::task::spawn_blocking(move || handle.join())
            .await
            .expect("spawn_blocking join")
            .expect("actor thread exits cleanly");
    }

    /// Open a pack read-write once so the files exist, then reopen it read-only. On the
    /// read-only pack every background save fails deterministically (`AppendError::ReadOnly`),
    /// which is the failure injector for the queued-save regression tests below.
    async fn open_read_only_pack(temp_dir: &TempDir) -> CertificatePack {
        let pack = CertificatePack::open(temp_dir.path(), 0);
        pack.shutdown().await.expect("clean shutdown of the writable pack");
        CertificatePack::open_static(temp_dir.path(), 0)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_reports_a_save_that_failed_while_the_flush_was_queued() {
        // Regression test for #1138: `persist()` samples the error slot before it enqueues, and
        // saves are fire-and-forget, so a save that fails while the `Persist` message is still
        // queued behind it must be folded into the persist reply. Otherwise the caller treats
        // `Ok(())` as proof of durability for a certificate that never reached the pack.
        let temp_dir = TempDir::with_prefix("persist_queued_save_error").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let pack = open_read_only_pack(&temp_dir).await;

        // Queue a save the actor will reject and a persist behind it. Both go straight to the
        // channel: the handle-side `get_error` sample sees an empty slot at this point, and the
        // point of the test is the actor's ordering. A single consumer draining a FIFO channel
        // guarantees the save fails before the persist is dequeued, so this is deterministic
        // rather than a race.
        let cert = make_test_cert(&fixture, 0);
        pack.tx.send(super::PackMessage::Save(cert)).await.expect("queue failing save");
        let (tx, rx) = tokio::sync::oneshot::channel();
        pack.tx.send(super::PackMessage::Persist(tx)).await.expect("queue persist");

        let err = rx
            .await
            .expect("actor replied to the persist")
            .expect_err("persist must report the save that failed while it was queued");
        assert!(matches!(err, super::PackError::Append(_)), "unexpected error: {err:?}");

        // This slot does not clear on read, so the failure stays visible to later callers too.
        pack.get_error().expect_err("error stays latched after the persist reply");
    }

    /// Set by [`same_epoch_reopen_waits_for_the_old_seal`] in the child process it spawns; turns
    /// [`same_epoch_reopen_child`] from a no-op into the reopen body, run against the directory
    /// this names.
    const REOPEN_CHILD_DIR: &str = "TN_CERT_PACK_REOPEN_CHILD_DIR";

    /// A same-epoch reopen while the previous pack's thread is still alive (an epoch re-entered
    /// while the old consensus task's pack thread was detached mid persist/seal) must wait for
    /// the old seal. Otherwise the new handle maps the old pack's capacity-padded file, the old
    /// seal truncates it under that mapping, and the new handle's next append faults (SIGBUS).
    /// The body runs in a child process so that fault cannot kill the test runner.
    #[test]
    fn same_epoch_reopen_waits_for_the_old_seal() {
        let temp_dir = TempDir::with_prefix("cert_same_epoch_reopen").expect("temp dir");
        let exe = std::env::current_exe().expect("test binary path");
        let out = std::process::Command::new(exe)
            .args([
                "--exact",
                "certificate_pack::test::same_epoch_reopen_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(REOPEN_CHILD_DIR, temp_dir.path())
            .output()
            .expect("spawn the child test");
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            out.status.success() && stdout.contains("1 passed"),
            "the reopen child failed ({:?})\nstdout:\n{stdout}\nstderr:\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// The body of [`same_epoch_reopen_waits_for_the_old_seal`]; a no-op unless that test runs it
    /// in a child process.
    #[tokio::test(flavor = "multi_thread")]
    async fn same_epoch_reopen_child() {
        let Some(dir) = std::env::var_os(REOPEN_CHILD_DIR) else { return };
        let dir = std::path::PathBuf::from(dir);
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();

        // The old epoch's pack: flushed but not sealed, so its data file is still padded.
        let old = CertificatePack::open(&dir, 0);
        for i in 0..50 {
            old.save(make_unique_cert(&fixture, i)).await.expect("save to the old pack");
        }
        old.persist().await.expect("persist the old pack");

        // The same epoch reopened while the old pack's thread is still alive.
        let new = CertificatePack::open(&dir, 0);
        let probe = make_unique_cert(&fixture, 0).digest();
        let waited =
            tokio::time::timeout(Duration::from_millis(300), new.contains(probe)).await.is_err();

        // The old thread finishes: its final persist, then the seal truncates the file to its end.
        old.shutdown().await.expect("shut down the old pack");

        // The new pack keeps archiving into the same file.
        for i in 50..2_000 {
            new.save(make_unique_cert(&fixture, i)).await.expect("save to the new pack");
        }
        new.persist().await.expect("persist the new pack");
        for i in 0..2_000 {
            assert!(new.contains(make_unique_cert(&fixture, i).digest()).await, "cert {i} lost");
        }
        new.shutdown().await.expect("shut down the new pack");
        assert!(waited, "the reopen must wait for the old pack's seal");

        // The new pack sealed a consistent log that holds every cert.
        let mut inner = super::Inner::open(dir.join("epoch-0"), false).expect("reopen");
        assert!(!inner.data.opened_unclean(), "the new pack must seal the log");
        for i in 0..2_000 {
            assert!(inner.get(make_unique_cert(&fixture, i).digest()).is_some(), "cert {i} lost");
        }
    }

    /// `shutdown` joins the pack thread before it returns, even when the reply carries a save
    /// failure: a caller that reopens the epoch right after must not race the old seal.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_joins_the_thread_before_reporting_a_save_error() {
        let temp_dir = TempDir::with_prefix("cert_shutdown_join").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let mut inner =
            super::Inner::open(temp_dir.path().join("epoch-0"), false).expect("open pack inner");
        inner.data.fail_next_append_for_test();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let (tx_error, error) = tokio::sync::watch::channel(None);
        // Held by the thread until it has fully exited, after a delay standing in for a slow seal.
        let alive = Arc::new(());
        let thread_alive = alive.clone();
        let handle = std::thread::spawn(move || {
            super::run_pack_loop(inner, rx, tx_error, 0);
            std::thread::sleep(Duration::from_millis(300));
            drop(thread_alive);
        });
        let pack =
            CertificatePack { tx, handle: Arc::new(Mutex::new(Some(handle))), error, epoch: 0 };

        pack.tx
            .send(super::PackMessage::Save(make_test_cert(&fixture, 0)))
            .await
            .expect("queue failing save");
        let err = pack.shutdown().await.expect_err("shutdown reports the failed save");
        assert!(matches!(err, super::PackError::Append(_)), "unexpected error: {err:?}");
        assert_eq!(Arc::strong_count(&alive), 1, "shutdown returned before the thread exited");
    }

    /// A pack whose open failed reports that open's error from `shutdown`, as `persist` does,
    /// not a bare `ReceiveFailed`: the latched error is the only record of why the pack failed.
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_reports_the_open_failure() {
        let temp_dir = TempDir::with_prefix("cert_shutdown_open_failure").expect("temp dir");
        // A file where the epoch's directory belongs: every open fails.
        std::fs::write(temp_dir.path().join("epoch-0"), b"not a directory").expect("write");
        let opened = CertificatePack::open(temp_dir.path(), 0);
        let open_error = opened.persist().await.expect_err("persist reports the open failure");
        assert!(!matches!(open_error, super::PackError::ReceiveFailed), "{open_error:?}");

        let pack = CertificatePack::open(temp_dir.path(), 0);
        let err = pack.shutdown().await.expect_err("shutdown reports the open failure");
        assert_eq!(err.to_string(), open_error.to_string(), "the latched open error");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_reports_a_save_that_failed_while_the_shutdown_was_queued() {
        // Regression test for #1138, end-of-epoch path: `shutdown()` never samples the error
        // slot, so the `ShutdownAsync` reply is the only place a queued save failure can
        // surface. Before the fix this returned `Ok(())` for a pack that silently dropped a
        // certificate.
        let temp_dir = TempDir::with_prefix("shutdown_queued_save_error").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let pack = open_read_only_pack(&temp_dir).await;

        let cert = make_test_cert(&fixture, 0);
        pack.tx.send(super::PackMessage::Save(cert)).await.expect("queue failing save");

        let err = pack
            .shutdown()
            .await
            .expect_err("shutdown must report the save that failed while it was queued");
        assert!(matches!(err, super::PackError::Append(_)), "unexpected error: {err:?}");
    }

    /// A failed write reaches the owner through `try_save` as that write's error, never as the
    /// `SendFull` backpressure the owner may ride out, on every later call, and `shutdown`
    /// reports it too: the contract an owner that stops the node on a failed pack relies on.
    #[tokio::test(flavor = "multi_thread")]
    async fn try_save_reports_a_failed_write_as_a_failure() {
        let temp_dir = TempDir::with_prefix("cert_try_save_failure").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let mut inner =
            super::Inner::open(temp_dir.path().join("epoch-0"), false).expect("open pack inner");
        inner.data.fail_next_append_for_test();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let (tx_error, error) = tokio::sync::watch::channel(None);
        let handle = std::thread::spawn(move || super::run_pack_loop(inner, rx, tx_error, 0));
        let pack =
            CertificatePack { tx, handle: Arc::new(Mutex::new(Some(handle))), error, epoch: 0 };

        pack.try_save(make_test_cert(&fixture, 0)).expect("the failing save is queued");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pack.get_error().is_ok() {
            assert!(std::time::Instant::now() < deadline, "the failed write never latched");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for i in 1..3usize {
            let err =
                pack.try_save(make_test_cert(&fixture, i)).expect_err("a failed pack refuses");
            assert!(matches!(err, super::PackError::Append(_)), "the write's error: {err:?}");
        }
        let err = pack.shutdown().await.expect_err("shutdown reports the failed write");
        assert!(matches!(err, super::PackError::Append(_)), "the write's error: {err:?}");
    }

    /// Flip every bit of the byte at `at` in the file at `path`.
    fn toggle_byte(path: &std::path::Path, at: u64) {
        use std::os::unix::fs::FileExt as _;
        let file = std::fs::OpenOptions::new().read(true).write(true).open(path).expect("open");
        let mut byte = [0_u8];
        file.read_exact_at(&mut byte, at).expect("read a byte");
        file.write_all_at(&[byte[0] ^ 0xFF], at).expect("write a byte");
    }

    /// Whether the bytes `[start, end)` of the file at `path` all read zero.
    fn zeros_at(path: &std::path::Path, start: u64, end: u64) -> bool {
        use std::os::unix::fs::FileExt as _;
        let mut bytes = vec![0_u8; (end - start) as usize];
        std::fs::File::open(path).expect("open").read_exact_at(&mut bytes, start).expect("read");
        bytes.iter().all(|&b| b == 0)
    }

    /// A rebuild that cuts an unclean log's torn tail flushes the zeros it writes there. The
    /// unclean open's sync made the old tail durable (here a damaged cert and a whole one after
    /// it), and zeros only in the mapping would leave it on disk past the cut after a power loss,
    /// for a later rebuild to reach behind newer certificates.
    #[tokio::test(flavor = "multi_thread")]
    async fn rebuild_flushes_the_tail_it_cuts() {
        use crate::archive::data_file::disk_model;
        let temp_dir = TempDir::with_prefix("cert_rebuild_flush").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");
        let data_path = dir.join(super::Inner::DATA_NAME);
        let mut inner = super::Inner::open(&dir, false).expect("open pack");
        for i in 0..4usize {
            inner.save(&make_unique_cert(&fixture, i)).expect("save cert");
        }
        inner.persist().expect("persist");
        let cut = inner.data.file_len();
        for i in 4..6usize {
            inner.save(&make_unique_cert(&fixture, i)).expect("save cert, never persisted");
        }
        let stale_end = inner.data.file_len();
        simulate_crash(inner);
        // Damage the first unpersisted cert: the rebuild keeps the log up to `cut`.
        toggle_byte(&data_path, cut + 8);

        disk_model::track(&data_path);
        let inner = super::Inner::open(&dir, false).expect("the rebuild cuts the torn tail");
        assert_eq!(inner.data.file_len(), cut, "cut at the damaged cert");
        simulate_crash(inner); // crash again, before any writeback
        disk_model::power_loss(&data_path);
        assert!(zeros_at(&data_path, cut, stale_end), "the cut tail's zeros reached the disk");
        disk_model::untrack(&data_path);
    }

    /// A failed save whose bytes reached the disk before its rollback (the system's writeback)
    /// does not come back after a power loss: the rollback's zeros are flushed over it.
    #[tokio::test(flavor = "multi_thread")]
    async fn power_loss_never_brings_back_a_rolled_back_save() {
        use crate::archive::data_file::disk_model;
        let temp_dir = TempDir::with_prefix("cert_rollback_flush").expect("temp dir");
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let dir = temp_dir.path().join("epoch-0");
        let data_path = dir.join(super::Inner::DATA_NAME);
        let mut inner = super::Inner::open(&dir, false).expect("open pack");
        inner.save(&make_unique_cert(&fixture, 0)).expect("save cert");
        inner.persist().expect("persist");
        disk_model::track(&data_path);
        let start = inner.data.file_len();
        inner.data.append(&make_unique_cert(&fixture, 1)).expect("append");
        let end = inner.data.file_len();
        inner.data.view().msync(start, end).expect("writeback before the rollback");
        super::Inner::rewind_after_failed_save(&mut inner.data, start);
        simulate_crash(inner);
        disk_model::power_loss(&data_path);
        assert!(zeros_at(&data_path, start, end), "the rolled-back save stays gone");
        disk_model::untrack(&data_path);
    }

    /// A read the disk fails while an open walks the log is an error, never a torn tail: the open
    /// fails with the read error and cuts nothing, and once the disk reads again the next open
    /// keeps every certificate. In an unclean log the acked-damage walk reads first; in a clean
    /// log whose index will not open, the rebuild's replay does.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_error_during_the_rebuild_fails_the_open() {
        use crate::archive::pack_iter::FAIL_READ_AT;
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        for unclean in [true, false] {
            let temp_dir = TempDir::with_prefix("cert_read_error").expect("temp dir");
            let dir = temp_dir.path().join("epoch-0");
            let at = {
                let mut inner = super::Inner::open(&dir, false).expect("open pack");
                let mut positions = Vec::new();
                for i in 0..8usize {
                    let cert = make_unique_cert(&fixture, i);
                    inner.save(&cert).expect("save cert");
                    let digest = B256::from_slice(cert.digest().as_ref());
                    positions.push(inner.digest_idx.load(digest).expect("indexed"));
                }
                inner.persist().expect("persist");
                if unclean {
                    simulate_crash(inner);
                } else {
                    drop(inner);
                }
                positions[if unclean { 7 } else { 3 }]
            };
            if !unclean {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(dir.join(super::Inner::HASH_NAME).join("index.hdx"))
                    .expect("open the index")
                    .set_len(16)
                    .expect("tear the index header");
            }
            let data_path = dir.join(super::Inner::DATA_NAME);
            let before = std::fs::read(&data_path).expect("read the log");
            FAIL_READ_AT.with(|fail| fail.set(Some(at)));
            let opened = super::Inner::open(&dir, false);
            FAIL_READ_AT.with(|fail| fail.set(None));
            let Err(err) = opened else {
                panic!("(unclean: {unclean}) a read error must fail the open");
            };
            assert!(
                matches!(&err, super::PackError::ReadError(e) if e.contains("not a torn tail")),
                "(unclean: {unclean}) {err:?}"
            );
            assert!(std::fs::read(&data_path).expect("read") == before, "the failed open cut");

            let mut inner = super::Inner::open(&dir, false).expect("the disk reads again");
            for i in 0..8usize {
                let digest = make_unique_cert(&fixture, i).digest();
                assert!(inner.contains(digest), "(unclean: {unclean}) cert {i} kept");
            }
        }
    }
}
