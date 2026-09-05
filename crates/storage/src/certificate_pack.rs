//! Pack file for storing Certificates, indexed by certificate digest (header hash).

use std::{
    error::Error, fmt::Display, hash::BuildHasherDefault, io, path::Path, sync::Arc,
    thread::JoinHandle,
};

use parking_lot::Mutex;
use tn_types::{Certificate, Epoch, Hash, HeaderDigest, B256};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tracing::{error, info};

use crate::{
    archive::{
        digest_index::HdxIndex,
        error::{fetch::FetchError, open::OpenError},
        fxhasher::FxHasher,
        index::Index as _,
        pack::{Pack, PackCompression, DATA_HEADER_BYTES},
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
/// their return value, even when that save was still queued at the time of the call.
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
                let _ = inner.persist();
                info!(target: "certificate_pack", "certificate pack for epoch {} persisted", epoch);
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
            if let Some(_handle) = self.handle.lock().take() {
                error!(target: "certificate_pack", "DID NOT CALL SHUTDOWN on certificate pack for epoch {}", self.epoch);
                // Make an effort to shutdown anyway but this may not have time to run.
                // Ideally would wait on the handle to join but don't block the Drop or mess around
                // with an async runtime- not calling shutdown is the root problem.
                let _ = self.tx.try_send(PackMessage::Shutdown);
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
        rx.await.map_err(|_| match &*self.error.borrow() {
            Some(e) => e.clone(),
            None => PackError::ReceiveFailed,
        })?
    }

    /// Consume and shutdown the pack if this is the last instance (if not the last instance then is
    /// no-op). This is safer than relying on Drop.
    ///
    /// Returns `Err` if the final flush fails or if any earlier background save failed: the
    /// shutdown reply folds in the latched error slot, so a save failure from this epoch is
    /// reported here even though this method never samples the slot itself.
    pub async fn shutdown(self) -> Result<(), PackError> {
        if Arc::strong_count(&self.handle) == 1 {
            let handle = self.handle.lock().take();
            if let Some(handle) = handle {
                let (tx, rx) = oneshot::channel();
                let _ = self.tx.send(PackMessage::ShutdownAsync(tx)).await;
                rx.await.map_err(|_| PackError::ReceiveFailed)??;
                // The thread should be over or ending after the ShutdownAsync but don't block tokio
                // just in case.
                let join_result = tokio::task::spawn_blocking(|| handle.join()).await;
                match join_result {
                    Err(e) => {
                        error!(target: "certificate_pack", ?e, "Failed to join certificate pack thread (tokio");
                        return Err(PackError::JoinFailed);
                    }
                    Ok(Err(e)) => {
                        error!(target: "certificate_pack", ?e, "Failed to join certificate pack thread");
                        return Err(PackError::JoinFailed);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }
}

pub const DATA_NAME: &str = Inner::DATA_NAME;

#[derive(Debug)]
struct Inner {
    data: Pack<Certificate>,
    digest_idx: HdxIndex,
}

impl Inner {
    const DATA_NAME: &str = "cert_data";
    const HASH_NAME: &str = "cert_hash";

    fn open<P: AsRef<Path>>(path: P, read_only: bool) -> Result<Self, PackError> {
        let base_dir = path.as_ref();
        if !read_only {
            let _ = std::fs::create_dir_all(base_dir);
        }
        let mut data: Pack<Certificate> = Pack::open(
            base_dir.join(Self::DATA_NAME),
            0,
            read_only,
            PackCompression::ZStd,
            PACK_VERSION,
        )?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut digest_idx = HdxIndex::open_hdx_file(
            base_dir.join(Self::HASH_NAME),
            data.header(),
            builder,
            read_only,
        )
        .map_err(OpenError::IndexFileOpen)?;

        if !read_only {
            // Repair: if the pack was extended past what the index tracked (e.g. crash mid-write),
            // truncate back to the last known-good boundary.
            let pack_len = data.file_len();
            let idx_len = digest_idx.data_file_length();
            if pack_len > idx_len && idx_len >= DATA_HEADER_BYTES as u64 {
                data.truncate(idx_len)?;
            }
            // On a brand-new file the index's tracked length starts at DATA_HEADER_BYTES (the
            // pack header size). Sync it if it hasn't been set yet.
            if digest_idx.data_file_length() < DATA_HEADER_BYTES as u64 {
                digest_idx.set_data_file_length(data.file_len());
            }
        }
        Ok(Self { data, digest_idx })
    }

    fn save(&mut self, cert: &Certificate) -> Result<(), PackError> {
        let digest = B256::from_slice(cert.digest().as_ref());
        // Idempotent: skip if already present.
        if self.digest_idx.load(digest).is_ok() {
            return Ok(());
        }
        let position = self.data.append(cert).map_err(|e| PackError::Append(e.to_string()))?;
        self.digest_idx
            .save(digest, position)
            .map_err(|e| PackError::IndexAppend(e.to_string()))?;
        self.digest_idx.set_data_file_length(self.data.file_len());
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
            self.digest_idx.sync().map_err(|e| PackError::PersistError(e.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum PackError {
    IO(Arc<io::Error>),
    Append(String),
    IndexAppend(String),
    Open(Arc<OpenError>),
    ReadError(String),
    SendFailed,
    SendFull,
    ReceiveFailed,
    PersistError(String),
    CorruptPack,
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
    use tempfile::TempDir;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{Certificate, Hash, HeaderDigest};

    use crate::{
        archive::error::insert::AppendError, certificate_pack::CertificatePack, mem_db::MemDatabase,
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
}
