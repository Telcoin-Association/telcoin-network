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
        digest_index::index::HdxIndex,
        error::{fetch::FetchError, open::OpenError},
        fxhasher::FxHasher,
        index::Index as _,
        pack::{Pack, PackCompression, DATA_HEADER_BYTES},
    },
    consensus_pack::PACK_VERSION,
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
                if let Err(e) = inner.save(&cert) {
                    tx_error.send_replace(Some(e));
                }
            }
            PackMessage::Get(digest, tx) => {
                let _ = tx.send(inner.get(digest));
            }
            PackMessage::Contains(digest, tx) => {
                let _ = tx.send(inner.contains(digest));
            }
            PackMessage::Persist(tx) => {
                let _ = tx.send(inner.persist());
            }
            PackMessage::Shutdown => {
                let _ = inner.persist();
                info!(target: "certificate_pack", "certificate pack for epoch {} persisted", epoch);
                break;
            }
            PackMessage::ShutdownAsync(tx) => {
                let _ = tx.send(inner.persist());
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
                tx_error.send_replace(Some(e));
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
                tx_error.send_replace(Some(e));
                clear_pack_loop(rx);
            }
        });
        Self { tx, handle: Arc::new(Mutex::new(Some(handle))), error, epoch }
    }

    /// Return any delayed error from a previous background operation.
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

    use crate::{certificate_pack::CertificatePack, mem_db::MemDatabase};

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
}
