//! Database for storing [`EpochRecord`] and [`EpochCertificate`] data.
//!
//! Two log files are maintained:
//! - A records file containing [`EpochRecord`] entries, indexed by epoch number (position index)
//!   and by digest (hash index).
//! - A certs file containing [`EpochCertificate`] entries, indexed by digest only.

use std::{
    error::Error,
    fmt::Display,
    future::Future,
    hash::BuildHasherDefault,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    thread::JoinHandle,
    time::Duration,
};

use parking_lot::Mutex;
use tn_types::{BlsPublicKey, Epoch, EpochCertificate, EpochDigest, EpochRecord};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tracing::error;

use crate::{
    archive::{
        data_file::create_dir_synced,
        digest_index::index::HdxIndex,
        error::{fetch::FetchError, open::OpenError},
        fxhasher::FxHasher,
        index::Index as _,
        pack::{Pack, PackCompression, DATA_HEADER_BYTES},
        position_index::index::PositionIndex,
    },
    consensus_pack::fetch_error_is_absent,
};

/// Current version of the epoch pack file.
const EPOCH_PACK_VERSION: u16 = 0;

enum EpochDbMessage {
    /// Save a "dummy" epoch 0 [`EpochRecord`] without a certificate.
    SaveDummy0Record(EpochRecord),
    /// Save an [`EpochRecord`] without a certificate.
    SaveRecord(EpochRecord),
    /// Save an [`EpochRecord`] and its corresponding [`EpochCertificate`].
    /// If the record is already stored, only the certificate is saved.
    Save(EpochRecord, EpochCertificate),
    /// Save an [`EpochCertificate`] keyed by its record digest.
    SaveCertificate(EpochDigest, EpochCertificate),
    /// Retrieve an [`EpochRecord`] by epoch number.
    RecordByEpoch(Epoch, oneshot::Sender<Option<EpochRecord>>),
    /// Retrieve an [`EpochRecord`] by epoch number without collapsing storage failures into
    /// absence: `Ok(None)` only when the record is genuinely not stored.
    TryRecordByEpoch(Epoch, oneshot::Sender<Result<Option<EpochRecord>, FetchError>>),
    /// Retrieve an [`EpochRecord`] by its digest.
    RecordByDigest(EpochDigest, oneshot::Sender<Option<EpochRecord>>),
    /// Retrieve an [`EpochCertificate`] by its epoch_hash digest.
    CertByDigest(EpochDigest, oneshot::Sender<Option<EpochCertificate>>),
    /// Retrieve an [`EpochCertificate`] by its epoch_hash digest without collapsing storage
    /// failures into absence: `Ok(None)` only when no certificate is stored for the digest.
    TryCertByDigest(EpochDigest, oneshot::Sender<Result<Option<EpochCertificate>, FetchError>>),
    /// True if the database contains a record for the given epoch number.
    ContainsEpoch(Epoch, oneshot::Sender<bool>),
    /// True if the database contains a record with the given digest.
    ContainsRecordDigest(EpochDigest, oneshot::Sender<bool>),
    /// Return the latest (highest epoch) [`EpochRecord`] stored, if any.
    LatestRecord(oneshot::Sender<Option<EpochRecord>>),
    /// Flush all pending writes to disk.
    Persist(oneshot::Sender<Result<(), EpochDbError>>),
    Shutdown,
}

/// Handle to the epoch records database.
///
/// Operations are dispatched to a background thread that owns the file handles.
/// Errors from background writes are surfaced on the next call via [`get_error`], which clears
/// the slot as it reads, so exactly one subsequent caller observes a given failure. Use
/// [`peek_error`] to check without consuming. [`persist`] is the durability barrier: it reports
/// any earlier write failure even if that write was still queued when the flush was requested.
#[derive(Debug, Clone)]
pub struct EpochRecordDb {
    /// Channel to send commands to the background thread.
    tx: Sender<EpochDbMessage>,
    /// Join handle for the background thread running commands.
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Track any errors that happened in the background.
    error: watch::Sender<Option<EpochDbError>>,
    /// Vector to map epochs to the last consensus header number.
    /// Used for quickly deducing an epoch for a given consensus header number.
    final_numbers: Arc<Mutex<Vec<u64>>>,
}

fn run_db_loop(
    mut inner: Inner,
    mut rx: Receiver<EpochDbMessage>,
    tx_error: watch::Sender<Option<EpochDbError>>,
) {
    while let Some(msg) = rx.blocking_recv() {
        match msg {
            EpochDbMessage::SaveDummy0Record(record) => {
                if let Err(e) = inner.save_dummy_epoch0(record) {
                    error!(target: "epoch-db", %e, "failed to save dummy epoch 0 record");
                    tx_error.send_replace(Some(e));
                }
            }
            EpochDbMessage::SaveRecord(record) => {
                if let Err(e) = inner.save_record(record) {
                    error!(target: "epoch-db", %e, "failed to save epoch record");
                    tx_error.send_replace(Some(e));
                }
            }
            EpochDbMessage::Save(record, cert) => {
                if let Err(e) = inner.save(record, cert) {
                    error!(target: "epoch-db", %e, "failed to save epoch record and certificate");
                    tx_error.send_replace(Some(e));
                }
            }
            EpochDbMessage::SaveCertificate(digest, cert) => {
                if let Err(e) = inner.save_certificate(digest, cert) {
                    error!(target: "epoch-db", %e, "failed to save epoch certificate");
                    tx_error.send_replace(Some(e));
                }
            }
            EpochDbMessage::RecordByEpoch(epoch, tx) => {
                let _ = tx.send(inner.record_by_epoch(epoch));
            }
            EpochDbMessage::TryRecordByEpoch(epoch, tx) => {
                let _ = tx.send(inner.try_record_by_epoch(epoch));
            }
            EpochDbMessage::RecordByDigest(digest, tx) => {
                let _ = tx.send(inner.record_by_digest(digest));
            }
            EpochDbMessage::CertByDigest(digest, tx) => {
                let _ = tx.send(inner.cert_by_digest(digest));
            }
            EpochDbMessage::TryCertByDigest(digest, tx) => {
                let _ = tx.send(inner.try_cert_by_digest(digest));
            }
            EpochDbMessage::ContainsEpoch(epoch, tx) => {
                let _ = tx.send(inner.contains_epoch(epoch));
            }
            EpochDbMessage::ContainsRecordDigest(digest, tx) => {
                let _ = tx.send(inner.contains_record_digest(digest));
            }
            EpochDbMessage::LatestRecord(tx) => {
                let _ = tx.send(inner.latest_record());
            }
            EpochDbMessage::Persist(tx) => {
                // Fold a write that failed while this persist was queued into the reply.
                // `persist()` samples the error slot before enqueueing, and writes are
                // fire-and-forget, so a save that fails after that sample but before this arm
                // would otherwise be acknowledged as a successful flush.
                let pending = tx_error.send_replace(None);
                let flushed = inner.persist();
                let _ = tx.send(pending.map_or(flushed, Err));
            }
            EpochDbMessage::Shutdown => {
                let _ = inner.persist();
                break;
            }
        }
    }
}

impl Drop for EpochRecordDb {
    fn drop(&mut self) {
        if Arc::strong_count(&self.handle) == 1 {
            if let Some(handle) = self.handle.lock().take() {
                if self.tx.try_send(EpochDbMessage::Shutdown).is_ok() {
                    let _ = handle.join();
                }
            }
        }
    }
}

/// Outcome of validating a downloaded [`EpochRecord`] and [`EpochCertificate`] against the
/// locally-trusted committee for the requested epoch.
///
/// A downloaded record is never trusted on the strength of its own embedded committee: it must
/// be anchored to the committee the local node already trusts for that epoch. This is the single
/// result type shared by the state-sync ingest path and the failed-quorum recovery path so
/// neither can accept a record under weaker rules than the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochRecordValidation {
    /// The record is anchored to the locally-trusted committee, has the expected parent hash, and
    /// carries a super-quorum certificate from that committee.
    Valid,
    /// The record was checked against the trusted committee but failed one or more of the anchor
    /// checks. The booleans record which checks passed, for diagnostics. `epoch_matches` is false
    /// when the record is for a different epoch than the one that was requested.
    Invalid { epoch_matches: bool, parents_match: bool, committee_valid: bool, cert_valid: bool },
    /// No locally-trusted anchor is available for the record's epoch (the previous epoch record,
    /// or the genesis committee, is not stored locally), so the record cannot be validated.
    /// Callers should retry once the anchor is available rather than treat the record as invalid.
    NoAnchor,
}

impl EpochRecordValidation {
    /// True only for [`EpochRecordValidation::Valid`].
    pub fn is_valid(&self) -> bool {
        match self {
            EpochRecordValidation::Valid => true,
            EpochRecordValidation::Invalid { .. } | EpochRecordValidation::NoAnchor => false,
        }
    }
}

/// Return true if the record's committee is compatible with `committee` (the locally-trusted
/// committee, normally the previous epoch record's `next_committee`).
///
/// Delegates to the shared [`EpochRecord::committee_compatible`] predicate so this verifier and
/// the epoch record producer accept exactly the same committee shapes. The committees are usually
/// equal, but a validator can be ejected on-chain mid-epoch, leaving the record's committee a
/// sane-sized subset of the trusted committee.
fn epoch_committee_valid(
    epoch_rec: &EpochRecord,
    committee: &std::collections::BTreeSet<BlsPublicKey>,
) -> bool {
    epoch_rec.committee_compatible(committee)
}

/// Why a *certified* epoch record could not be resolved.
///
/// Returned by [`EpochRecordDb::certified_record_by_epoch`] and its timeout variant, for callers
/// (e.g. the epoch-close seed-anchor capture) that must never consume a record digest the
/// record's committee did not actually seal. The variants are distinguished because they heal
/// differently: a missing record or certificate can still arrive asynchronously (epoch record
/// collector, vote aggregation), while a stored-but-invalid certificate is permanent — the
/// certificate store is append-once per digest — and must surface loudly instead of being
/// retried. A storage-level read failure is likewise permanent from the poller's point of
/// view: re-reading corrupt bytes cannot repair them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertifiedRecordError {
    /// No [`EpochRecord`] is stored for the requested epoch.
    MissingRecord(Epoch),
    /// A record is stored for the epoch, but no [`EpochCertificate`] is stored under its digest
    /// (yet). Carries the record's digest for diagnostics.
    MissingCertificate(Epoch, EpochDigest),
    /// A certificate is stored under the record's digest but fails
    /// [`EpochRecord::verify_with_cert`]: it does not carry a verified super-quorum of the
    /// record's committee over that digest. Carries the record's digest for diagnostics.
    InvalidCertificate(Epoch, EpochDigest),
    /// Resolving the record or its certificate failed at the storage layer for a reason other
    /// than genuine absence (I/O error, CRC mismatch, decode failure, or an unreachable
    /// database thread — everything [`fetch_error_is_absent`] rejects). The underlying error is
    /// logged at `error!` at the classification site; only the epoch is carried so the enum
    /// stays `Copy`. Never retryable: polling corrupt bytes can only mask the corruption behind
    /// a misleading "missing" timeout.
    Storage(Epoch),
}

impl CertifiedRecordError {
    /// True when re-reading later could legitimately succeed.
    ///
    /// A missing record or certificate can still be supplied by the epoch record collector or
    /// the epoch-vote aggregation task; an invalid stored certificate can never be replaced
    /// (certificate writes are append-once per digest), and a storage-level failure cannot be
    /// repaired by re-reading, so retrying either would only mask the failure.
    pub fn is_retryable(&self) -> bool {
        match self {
            CertifiedRecordError::MissingRecord(_)
            | CertifiedRecordError::MissingCertificate(_, _) => true,
            CertifiedRecordError::InvalidCertificate(_, _) | CertifiedRecordError::Storage(_) => {
                false
            }
        }
    }
}

impl Error for CertifiedRecordError {}

impl Display for CertifiedRecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CertifiedRecordError::MissingRecord(epoch) => {
                write!(f, "no epoch record stored for epoch {epoch}")
            }
            CertifiedRecordError::MissingCertificate(epoch, digest) => {
                write!(f, "epoch record {digest} for epoch {epoch} has no stored certificate")
            }
            CertifiedRecordError::InvalidCertificate(epoch, digest) => {
                write!(
                    f,
                    "stored certificate for epoch record {digest} (epoch {epoch}) failed \
                     super-quorum verification"
                )
            }
            CertifiedRecordError::Storage(epoch) => {
                write!(
                    f,
                    "storage-level failure (not a miss) resolving the certified epoch record \
                     for epoch {epoch}; see the error log for the underlying error"
                )
            }
        }
    }
}

impl EpochRecordDb {
    /// Open (or create) the epoch records database at `path` for append.
    ///
    /// `start_epoch` is used when creating a brand-new database.  When reopening an
    /// existing database the start epoch is derived from the first stored record.
    pub fn open<P: Into<PathBuf>>(path: P) -> Result<Self, EpochDbError> {
        let (tx, rx) = mpsc::channel(1000);
        let path: PathBuf = path.into();
        let (error, _) = watch::channel(None);
        let inner = Inner::open_append(path, 0)?;
        let mut final_numbers = Vec::with_capacity(inner.epoch_idx.len());
        for epoch in inner.records.raw_iter().map_err(|_e| EpochDbError::CorruptDb)? {
            final_numbers.push(epoch?.final_consensus.number);
        }
        let tx_error = error.clone();
        let handle = std::thread::spawn(move || run_db_loop(inner, rx, tx_error));
        Ok(Self {
            tx,
            handle: Arc::new(Mutex::new(Some(handle))),
            error,
            final_numbers: Arc::new(Mutex::new(final_numbers)),
        })
    }

    /// Read every [`EpochRecord`] from a bare records pack file (e.g. an `epoch_records` file
    /// copied out of an export bundle) without needing its sidecar indexes. Records come back
    /// in stored (epoch-ascending) order.
    ///
    /// Used by `db load-state` to rebuild a fully-indexed records database from a data-only bundle:
    /// the returned records are re-saved through [`EpochRecordDb::save_record`] (which rebuilds the
    /// indexes) and also supply the previous-epoch / final-consensus-number context needed to
    /// stream-import the matching consensus pack.
    pub fn read_records_from_pack<P: Into<PathBuf>>(
        path: P,
    ) -> Result<Vec<EpochRecord>, EpochDbError> {
        let pack = Pack::<EpochRecord>::open(
            path.into(),
            Inner::PACK_EPOCH,
            true,
            PackCompression::ZStd,
            EPOCH_PACK_VERSION,
        )
        .map_err(|_| EpochDbError::CorruptDb)?;
        let mut records = Vec::new();
        for record in pack.raw_iter().map_err(|_| EpochDbError::CorruptDb)? {
            records.push(record.map_err(|_| EpochDbError::CorruptDb)?);
        }
        Ok(records)
    }

    /// Read every [`EpochCertificate`] from a bare certs pack file (e.g. an `epoch_certs` file
    /// copied out of an export bundle) without needing its sidecar index. Certificates come back in
    /// stored order; match one to its record with `cert.epoch_hash == record.digest()`.
    ///
    /// Used by `db load-state` to verify each restored epoch record against its certificate.
    pub fn read_certs_from_pack<P: Into<PathBuf>>(
        path: P,
    ) -> Result<Vec<EpochCertificate>, EpochDbError> {
        let pack = Pack::<EpochCertificate>::open(
            path.into(),
            Inner::CERT_PACK_EPOCH,
            true,
            PackCompression::ZStd,
            EPOCH_PACK_VERSION,
        )
        .map_err(|_| EpochDbError::CorruptDb)?;
        let mut certs = Vec::new();
        for cert in pack.raw_iter().map_err(|_| EpochDbError::CorruptDb)? {
            certs.push(cert.map_err(|_| EpochDbError::CorruptDb)?);
        }
        Ok(certs)
    }

    /// Return any delayed error recorded by the background thread.
    /// Also clears the error.
    pub fn get_error(&self) -> Result<(), EpochDbError> {
        match self.error.send_replace(None) {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Return any delayed error recorded by the background thread.
    /// Does not clear the error.
    pub fn peek_error(&self) -> Result<(), EpochDbError> {
        match &*self.error.borrow() {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Save an [`EpochRecord`] without a certificate.
    /// Returns `Ok(())` idempotently if the record is already stored.
    pub async fn save_dummy_epoch0(&self, record: EpochRecord) -> Result<(), EpochDbError> {
        self.get_error()?;
        self.tx
            .send(EpochDbMessage::SaveDummy0Record(record))
            .await
            .map_err(|_| EpochDbError::SendFailed)?;
        Ok(())
    }

    /// Update final_numbers with record data.
    fn update_finals(&self, record: &EpochRecord) -> Result<(), EpochDbError> {
        let epoch = record.epoch as usize;
        let number = record.final_consensus.number;
        let mut finals = self.final_numbers.lock();
        let finals_len = finals.len();
        if epoch > finals_len {
            return Err(EpochDbError::EpochOutOfOrder(finals_len as u32, epoch as u32));
        }
        if epoch < finals_len {
            finals[epoch] = number;
        } else {
            finals.push(number);
        }
        Ok(())
    }

    /// Save an [`EpochRecord`] without a certificate.
    /// Returns `Ok(())` idempotently if the record is already stored.
    pub async fn save_record(&self, record: EpochRecord) -> Result<(), EpochDbError> {
        self.get_error()?;
        self.update_finals(&record)?;
        self.tx
            .send(EpochDbMessage::SaveRecord(record))
            .await
            .map_err(|_| EpochDbError::SendFailed)?;
        Ok(())
    }

    /// Save an [`EpochRecord`] and its [`EpochCertificate`] to the database.
    /// If the record is already stored, only the certificate is saved.
    pub async fn save(
        &self,
        record: EpochRecord,
        cert: EpochCertificate,
    ) -> Result<(), EpochDbError> {
        self.get_error()?;
        self.update_finals(&record)?;
        self.tx
            .send(EpochDbMessage::Save(record, cert))
            .await
            .map_err(|_| EpochDbError::SendFailed)?;
        Ok(())
    }

    /// Save an [`EpochCertificate`] keyed by `digest` (the corresponding [`EpochRecord`]'s digest).
    /// Idempotent: returns `Ok(())` if a certificate for this digest is already stored.
    pub async fn save_certificate(
        &self,
        digest: EpochDigest,
        cert: EpochCertificate,
    ) -> Result<(), EpochDbError> {
        self.get_error()?;
        self.tx
            .send(EpochDbMessage::SaveCertificate(digest, cert))
            .await
            .map_err(|_| EpochDbError::SendFailed)?;
        Ok(())
    }

    /// Retrieve an [`EpochRecord`] by epoch number.
    pub async fn record_by_epoch(&self, epoch: Epoch) -> Option<EpochRecord> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::RecordByEpoch(epoch, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Retrieve an [`EpochRecord`] by epoch number.
    /// This version will wait up to timeout time for the record to show up if not available.
    pub async fn record_by_epoch_with_timeout(
        &self,
        epoch: Epoch,
        timeout: Duration,
    ) -> Option<EpochRecord> {
        let deadline = tokio::time::Instant::now() + timeout;
        // TODO issue 573, clean this up.
        loop {
            if let Some(rec) = self.record_by_epoch(epoch).await {
                return Some(rec);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
        }
    }

    /// Retrieve the [`EpochRecord`] for `epoch` only when a stored [`EpochCertificate`]
    /// cryptographically verifies against it.
    ///
    /// The verification reuses [`EpochRecord::verify_with_cert`] — the same super-quorum BLS
    /// aggregate check the vote-quorum and peer-recovery paths run before storing a certificate
    /// — so a record is only released when its own committee actually sealed its digest. This
    /// closes the gap left by [`Self::record_by_epoch`], which is a raw first-write-wins fetch:
    /// a record that no committee member ever certified (e.g. a divergent record built locally
    /// at an epoch close that failed quorum) is returned by the raw fetch but refused here, so
    /// consensus-critical consumers (the epoch-close seed anchor) can never silently adopt it.
    ///
    /// Threat model: both the record and the certificate come from the local, validated ingest
    /// paths (a record's embedded committee is anchored at download time by
    /// [`Self::validate_downloaded_record`], or derived from executed on-chain state at epoch
    /// close), so this check defends against *local divergence* — holding a digest the previous
    /// committee never certified — not against an attacker with arbitrary DB write access.
    ///
    /// Unlike the raw [`Self::record_by_epoch`] / [`Self::cert_by_digest`] fetches, the reads
    /// here do NOT collapse storage failures into absence: only a genuine miss (per the shared
    /// absence classification, [`fetch_error_is_absent`]) reports
    /// [`CertifiedRecordError::MissingRecord`] / [`CertifiedRecordError::MissingCertificate`];
    /// corruption or an unreachable db thread reports the non-retryable
    /// [`CertifiedRecordError::Storage`] so the timeout variant fails loudly at once instead of
    /// polling corrupt bytes until the deadline and mislabeling them "missing".
    pub async fn certified_record_by_epoch(
        &self,
        epoch: Epoch,
    ) -> Result<EpochRecord, CertifiedRecordError> {
        let record = self
            .certified_read_record(epoch)
            .await?
            .ok_or(CertifiedRecordError::MissingRecord(epoch))?;
        let digest = record.digest();
        let cert = self
            .certified_read_cert(epoch, digest)
            .await?
            .ok_or(CertifiedRecordError::MissingCertificate(epoch, digest))?;
        record
            .verify_with_cert(&cert)
            .then_some(record)
            .ok_or(CertifiedRecordError::InvalidCertificate(epoch, digest))
    }

    /// Non-collapsing record read for the certified path: `Ok(None)` only on genuine absence.
    ///
    /// This is the classification site for [`CertifiedRecordError::Storage`]: the underlying
    /// error (a [`FetchError`] from the background thread, or a dead command channel) is logged
    /// at `error!` here before being reduced to the `Copy` variant, so no detail is lost.
    async fn certified_read_record(
        &self,
        epoch: Epoch,
    ) -> Result<Option<EpochRecord>, CertifiedRecordError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(EpochDbMessage::TryRecordByEpoch(epoch, tx)).await.map_err(|_| {
            error!(target: "epoch-db", epoch, "epoch record read failed: db thread unreachable (send)");
            CertifiedRecordError::Storage(epoch)
        })?;
        rx.await
            .map_err(|_| {
                error!(target: "epoch-db", epoch, "epoch record read failed: db thread unreachable (recv)");
                CertifiedRecordError::Storage(epoch)
            })?
            .map_err(|e| {
                error!(target: "epoch-db", epoch, "epoch record read failed (not a miss): {e}");
                CertifiedRecordError::Storage(epoch)
            })
    }

    /// Non-collapsing certificate read for the certified path; same classification (and
    /// logging) as [`Self::certified_read_record`], keyed by the record's `digest`.
    async fn certified_read_cert(
        &self,
        epoch: Epoch,
        digest: EpochDigest,
    ) -> Result<Option<EpochCertificate>, CertifiedRecordError> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(EpochDbMessage::TryCertByDigest(digest, tx)).await.map_err(|_| {
            error!(target: "epoch-db", epoch, ?digest, "epoch certificate read failed: db thread unreachable (send)");
            CertifiedRecordError::Storage(epoch)
        })?;
        rx.await
            .map_err(|_| {
                error!(target: "epoch-db", epoch, ?digest, "epoch certificate read failed: db thread unreachable (recv)");
                CertifiedRecordError::Storage(epoch)
            })?
            .map_err(|e| {
                error!(target: "epoch-db", epoch, ?digest, "epoch certificate read failed (not a miss): {e}");
                CertifiedRecordError::Storage(epoch)
            })
    }

    /// Like [`Self::certified_record_by_epoch`], but waits up to `timeout` for the record and
    /// its certificate to arrive, polling every 200ms (matching
    /// [`Self::record_by_epoch_with_timeout`]).
    ///
    /// Only the retryable outcomes are waited on (see [`CertifiedRecordError::is_retryable`]):
    /// a record or certificate that has not arrived yet can still be supplied asynchronously,
    /// while a stored-but-invalid certificate fails immediately because certificate writes are
    /// append-once per digest, so re-reading can never observe a repaired one. A storage-level
    /// failure ([`CertifiedRecordError::Storage`]) likewise fails immediately: re-reading
    /// corrupt bytes for the full timeout would only relabel corruption as "missing".
    pub async fn certified_record_by_epoch_with_timeout(
        &self,
        epoch: Epoch,
        timeout: Duration,
    ) -> Result<EpochRecord, CertifiedRecordError> {
        let deadline = tokio::time::Instant::now() + timeout;
        self.certified_record_poll(epoch, deadline).await
    }

    /// Recursive polling body of [`Self::certified_record_by_epoch_with_timeout`].
    ///
    /// Boxed because async recursion needs an indirected future type. Always performs at least
    /// one check, then recurses only while the failure is retryable and `deadline` has not
    /// passed; the final (non-retryable or timed-out) error is returned to the caller intact.
    fn certified_record_poll(
        &self,
        epoch: Epoch,
        deadline: tokio::time::Instant,
    ) -> Pin<Box<dyn Future<Output = Result<EpochRecord, CertifiedRecordError>> + Send + '_>> {
        Box::pin(async move {
            let outcome = self.certified_record_by_epoch(epoch).await;
            let retry = outcome.as_ref().err().is_some_and(|e| e.is_retryable())
                && tokio::time::Instant::now() < deadline;
            if retry {
                tokio::time::sleep(Duration::from_millis(200)).await;
                self.certified_record_poll(epoch, deadline).await
            } else {
                outcome
            }
        })
    }

    /// Retrieve an [`EpochRecord`] by its digest.
    pub async fn record_by_digest(&self, digest: EpochDigest) -> Option<EpochRecord> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::RecordByDigest(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Retrieve an [`EpochCertificate`] by its `epoch_hash` digest.
    pub async fn cert_by_digest(&self, digest: EpochDigest) -> Option<EpochCertificate> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::CertByDigest(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Retrieve an [`EpochCertificate`] by its `epoch_hash` digest.
    /// This version will wait up to timeout time for the cert to show up if not available.
    ///
    /// A just-closed epoch's certificate is only aggregated at the next epoch's start, so a caller
    /// that needs epoch N's cert immediately after N closes (e.g. the state exporter) must give the
    /// collector a bounded window to produce it.
    pub async fn cert_by_digest_with_timeout(
        &self,
        digest: EpochDigest,
        timeout: Duration,
    ) -> Option<EpochCertificate> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(cert) = self.cert_by_digest(digest).await {
                return Some(cert);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
        }
    }

    /// True if the database contains a record for the given epoch number.
    pub async fn contains_epoch(&self, epoch: Epoch) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::ContainsEpoch(epoch, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// True if the database contains a record with the given digest.
    pub async fn contains_record_digest(&self, digest: EpochDigest) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::ContainsRecordDigest(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(false)
        } else {
            false
        }
    }

    /// Return the latest (highest epoch number) [`EpochRecord`] stored, if any.
    pub async fn latest_record(&self) -> Option<EpochRecord> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::LatestRecord(tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Flush all pending writes to disk.
    ///
    /// Returns `Err` if any background write queued before this call failed, including one that
    /// was still queued when this call sampled the error slot: the actor drains a single FIFO
    /// channel, so every earlier write is processed before the flush and its failure is folded
    /// into the reply. Callers that treat a successful `persist()` as proof of durability, such
    /// as the epoch-close path, depend on that guarantee.
    pub async fn persist(&self) -> Result<(), EpochDbError> {
        self.get_error()?;
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(EpochDbMessage::Persist(tx)).await;
        rx.await.map_err(|_| match &*self.error.borrow() {
            Some(e) => e.clone(),
            None => EpochDbError::ReceiveFailed,
        })?
    }

    /// Retrieve the committee keys for `epoch` if available.
    /// Tries the exact epoch first; falls back to the previous epoch's `next_committee`.
    /// Returns as a [`BTreeSet`] to enforce a stable order.
    pub async fn get_committee_keys(
        &self,
        epoch: Epoch,
    ) -> Option<std::collections::BTreeSet<BlsPublicKey>> {
        if let Some(record) = self.record_by_epoch(epoch).await {
            return Some(record.committee.into_iter().collect());
        }
        if epoch > 0 {
            if let Some(record) = self.record_by_epoch(epoch - 1).await {
                return Some(record.next_committee.into_iter().collect());
            }
        }
        None
    }

    /// Retrieve the epoch record and certificate (if available) by epoch number.
    pub async fn get_epoch_by_number(
        &self,
        epoch: Epoch,
    ) -> Option<(EpochRecord, Option<EpochCertificate>)> {
        let record = self.record_by_epoch(epoch).await?;
        let cert = self.cert_by_digest(record.digest()).await;
        Some((record, cert))
    }

    /// Scan the historical epochs `0..tip_epoch` and return the first whose certificate (or record)
    /// is not yet stored, or `None` if every one has a cert. Cheap: record + cert actor lookups, no
    /// state I/O — the same per-epoch queries
    /// [`export_bounded_bundle`](Self::export_bounded_bundle) already does, run before the
    /// export's plain-state walk so it can skip early when a required historical cert is
    /// permanently missing (e.g. a network-wide failed-quorum epoch no peer can supply) instead
    /// of walking the whole state and then failing.
    ///
    /// `tip_epoch` itself is EXCLUDED: the exported tip's own cert is only aggregated at the next
    /// epoch's start, so it is normally still pending at export time and is waited for separately.
    pub async fn first_missing_historical_cert(&self, tip_epoch: Epoch) -> Option<Epoch> {
        for epoch in 0..tip_epoch {
            if !matches!(self.get_epoch_by_number(epoch).await, Some((_, Some(_)))) {
                return Some(epoch);
            }
        }
        None
    }

    /// Retrieve the epoch record and certificate (if available) by record digest.
    pub async fn get_epoch_by_hash(
        &self,
        hash: EpochDigest,
    ) -> Option<(EpochRecord, Option<EpochCertificate>)> {
        let record = self.record_by_digest(hash).await?;
        let cert = self.cert_by_digest(record.digest()).await;
        Some((record, cert))
    }

    /// Write a bounded export bundle covering epochs `0..=through_epoch` into fresh records/certs
    /// pack files at `records_path` and `certs_path`.
    ///
    /// Unlike copying the live shared `epochs.pack` / `epoch_certs.pack`, this selects an explicit,
    /// bounded record set through the actor, so a later epoch appending to the live packs cannot
    /// race into the exported bundle. The written files are bare pack `data` streams (no sidecar
    /// indexes) that round-trip through [`read_records_from_pack`](Self::read_records_from_pack) /
    /// [`read_certs_from_pack`](Self::read_certs_from_pack).
    ///
    /// A certificate is required for every epoch, including epoch 0: the importer fully verifies
    /// each record against its cert — epoch 0 against the seeded genesis committee — so a missing
    /// cert would make the bundle unverifiable. A missing cert is therefore a hard error.
    pub async fn export_bounded_bundle(
        &self,
        through_epoch: Epoch,
        records_path: &Path,
        certs_path: &Path,
    ) -> Result<(), EpochDbError> {
        // Surface any pending background write error before reading.
        self.peek_error()?;

        // Collect the bounded record+cert set from the actor first, so the on-disk write below sees
        // a fixed snapshot even if a later epoch is appended concurrently to the live packs.
        let mut records = Vec::with_capacity(through_epoch as usize + 1);
        let mut certs = Vec::with_capacity(through_epoch as usize + 1);
        for epoch in 0..=through_epoch {
            let (record, cert) =
                self.get_epoch_by_number(epoch).await.ok_or(EpochDbError::MissingRecord(epoch))?;
            match cert {
                Some(cert) => certs.push(cert),
                None => return Err(EpochDbError::MissingCertificate(epoch)),
            }
            records.push(record);
        }

        let records_path = records_path.to_path_buf();
        let certs_path = certs_path.to_path_buf();
        // These are blocking calls that may take some time, so don't jamb up an async thread.
        tokio::task::spawn_blocking(move || -> Result<(), EpochDbError> {
            write_bounded_pack(&records_path, Inner::PACK_EPOCH, &records)?;
            write_bounded_pack(&certs_path, Inner::CERT_PACK_EPOCH, &certs)?;
            Ok(())
        })
        .await
        .map_err(|_| EpochDbError::JoinError)??;
        Ok(())
    }

    /// Find the epoch for a consensus header number.
    ///
    /// Uses binary search (`partition_point`) over `final_numbers` for O(log n)
    /// lookup. The vector is guaranteed sorted because [`update_finals`] enforces
    /// sequential epoch insertion. If `number` is beyond the last stored epoch,
    /// returns `last_epoch + 1` (the current in-progress epoch).
    pub fn number_to_epoch(&self, number: u64) -> Epoch {
        let finals = self.final_numbers.lock();
        finals.partition_point(|final_num| number > *final_num) as u32
    }

    /// Validate a downloaded [`EpochRecord`] and its [`EpochCertificate`] against the
    /// locally-trusted committee for `epoch`, the epoch slot that was requested from the peer.
    ///
    /// The trusted committee and expected parent hash are derived from local state only, never
    /// from the downloaded record itself: the genesis committee for epoch 0, otherwise the
    /// previous epoch record's `next_committee`. The record is accepted
    /// ([`EpochRecordValidation::Valid`]) only when it is actually for `epoch`, has the expected
    /// parent hash, its committee is anchored to that trusted committee, and its certificate
    /// carries a super-quorum of signatures from that committee. Anchoring against the requested
    /// `epoch` (rather than the record's self-declared epoch) prevents a peer from satisfying a
    /// request for one slot with a self-consistent record for a different slot.
    ///
    /// This is the single validation routine shared by the state-sync ingest path and the
    /// failed-quorum recovery path, so a downloaded record cannot be accepted under weaker rules
    /// on one path than the other.
    pub async fn validate_downloaded_record(
        &self,
        epoch: Epoch,
        record: &EpochRecord,
        cert: &EpochCertificate,
    ) -> EpochRecordValidation {
        let anchor: Option<(EpochDigest, std::collections::BTreeSet<BlsPublicKey>)> = if epoch == 0
        {
            self.get_committee_keys(0).await.map(|committee| (EpochDigest::default(), committee))
        } else {
            self.record_by_epoch(epoch - 1)
                .await
                .map(|prev| (prev.digest(), prev.next_committee.iter().copied().collect()))
        };
        anchor
            .map(|(parent_hash, committee)| {
                let epoch_matches = record.epoch == epoch;
                let parents_match = parent_hash == record.parent_hash;
                let committee_valid = epoch_committee_valid(record, &committee);
                let cert_valid = record.verify_with_cert(cert);
                if epoch_matches && parents_match && committee_valid && cert_valid {
                    EpochRecordValidation::Valid
                } else {
                    EpochRecordValidation::Invalid {
                        epoch_matches,
                        parents_match,
                        committee_valid,
                        cert_valid,
                    }
                }
            })
            .unwrap_or(EpochRecordValidation::NoAnchor)
    }
}

pub const RECORDS_NAME: &str = Inner::RECORDS_NAME;
pub const CERTS_NAME: &str = Inner::CERTS_NAME;

/// Lift a raw index/pack read into the non-collapsing shape: `Ok(Some(v))` on success,
/// `Ok(None)` when the error means the key is genuinely not present (per
/// [`fetch_error_is_absent`], the single absence classification shared with the consensus
/// pack), and `Err` for every real storage failure.
fn absent_to_none<T>(res: Result<T, FetchError>) -> Result<Option<T>, FetchError> {
    res.map(Some).or_else(|e| fetch_error_is_absent(&e).then_some(None).ok_or(e))
}

#[derive(Debug)]
struct Inner {
    /// Log file for [`EpochRecord`] entries.
    records: Pack<EpochRecord>,
    /// Log file for [`EpochCertificate`] entries.
    certs: Pack<EpochCertificate>,
    /// Position index: (epoch - start_epoch) → byte offset in `records`.
    epoch_idx: PositionIndex<u64>,
    /// Hash index: EpochRecord digest → byte offset in `records`.
    record_digests: HdxIndex,
    /// Hash index: EpochCertificate epoch_hash → byte offset in `certs`.
    cert_digests: HdxIndex,
    /// The first epoch stored in this database.
    start_epoch: Epoch,
    /// Store a dummy record for epoch 0 to allow chain to start.
    dummy_epoch0: Option<EpochRecord>,
}

impl Inner {
    const RECORDS_NAME: &str = "epochs.pack";
    const CERTS_NAME: &str = "epoch_certs.pack";
    const EPOCH_POS_NAME: &str = "epochs.idx";
    const RECORD_HASH_NAME: &str = "epochs.hash";
    const CERT_HASH_NAME: &str = "epoch_certs.hash";
    /// Sentinel pack-header tag for the records file.
    const PACK_EPOCH: u64 = 0;
    /// Sentinel pack-header tag for the certs file.
    const CERT_PACK_EPOCH: u64 = 1;

    /// Truncate records and its indexes back to a consistent state.
    fn heal_records(
        records: &mut Pack<EpochRecord>,
        epoch_idx: &mut PositionIndex<u64>,
        record_digests: &HdxIndex,
    ) -> Result<(), EpochDbError> {
        let records_len = records.file_len();
        let digest_final = record_digests.data_file_length();
        if records_len > digest_final && digest_final > DATA_HEADER_BYTES as u64 {
            records.truncate(digest_final)?;
        }
        let records_len = records.file_len();
        if !epoch_idx.is_empty() {
            let mut new_len = records_len;
            let start_idx = epoch_idx.len() as u64 - 1;
            let mut idx = start_idx;
            loop {
                if let Ok(last_record) = epoch_idx.load(idx) {
                    let size_res = records.record_size(last_record);
                    if size_res.is_ok() {
                        epoch_idx.truncate_to_index(idx)?;
                        new_len = last_record + size_res.unwrap_or_default() as u64;
                        break;
                    }
                }
                if idx == 0 {
                    epoch_idx.truncate_all()?;
                    break;
                }
                idx -= 1;
            }
            if new_len != records_len {
                records.truncate(new_len)?;
            }
        }
        Ok(())
    }

    /// Truncate the certs file back to a consistent state.
    fn heal_certs(
        certs: &mut Pack<EpochCertificate>,
        cert_digests: &HdxIndex,
    ) -> Result<(), EpochDbError> {
        let certs_len = certs.file_len();
        let digest_final = cert_digests.data_file_length();
        if certs_len > digest_final && digest_final > DATA_HEADER_BYTES as u64 {
            certs.truncate(digest_final)?;
        }
        Ok(())
    }

    fn open_append<P: AsRef<Path>>(path: P, start_epoch: Epoch) -> Result<Self, EpochDbError> {
        let base_dir = path.as_ref();
        let _ = create_dir_synced(base_dir);
        let have_records = std::fs::exists(base_dir.join(Self::RECORDS_NAME)).unwrap_or_default();

        let mut records = Pack::<EpochRecord>::open(
            base_dir.join(Self::RECORDS_NAME),
            Self::PACK_EPOCH,
            false,
            PackCompression::ZStd,
            EPOCH_PACK_VERSION,
        )?;
        let mut certs = Pack::<EpochCertificate>::open(
            base_dir.join(Self::CERTS_NAME),
            Self::CERT_PACK_EPOCH,
            false,
            PackCompression::ZStd,
            EPOCH_PACK_VERSION,
        )?;

        let mut epoch_idx: PositionIndex<u64> = PositionIndex::open_pdx_file(
            base_dir.join(Self::EPOCH_POS_NAME),
            records.header(),
            "index.pdx",
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut record_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::RECORD_HASH_NAME),
            records.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;
        let builder = BuildHasherDefault::<FxHasher>::default();
        let mut cert_digests = HdxIndex::open_hdx_file(
            base_dir.join(Self::CERT_HASH_NAME),
            certs.header(),
            builder,
            false,
        )
        .map_err(OpenError::IndexFileOpen)?;

        if !have_records {
            // Freshly created: initialise the stored data lengths in all indexes.
            record_digests.set_data_file_length(records.file_len());
            cert_digests.set_data_file_length(certs.file_len());
        }

        Self::heal_records(&mut records, &mut epoch_idx, &record_digests)?;
        Self::heal_certs(&mut certs, &cert_digests)?;

        // Derive start_epoch from the first stored record if present.
        let start_epoch = if !epoch_idx.is_empty() {
            let pos = epoch_idx.load(0).map_err(|e| EpochDbError::HeaderLoad(e.to_string()))?;
            records.fetch(pos).map_err(|e| EpochDbError::HeaderLoad(e.to_string()))?.epoch
        } else {
            start_epoch
        };

        Ok(Self {
            records,
            certs,
            epoch_idx,
            record_digests,
            cert_digests,
            start_epoch,
            dummy_epoch0: None,
        })
    }

    /// Save an [`EpochRecord`] without a certificate.
    /// Returns `Ok(())` idempotently if the record is already stored.
    /// This saves a tempary epoch 0 zero record to allow the chain to start.
    fn save_dummy_epoch0(&mut self, record: EpochRecord) -> Result<(), EpochDbError> {
        if record.epoch == 0 {
            self.dummy_epoch0 = Some(record);
            Ok(())
        } else {
            Err(EpochDbError::EpochOutOfOrder(record.epoch, 0))
        }
    }

    /// Save an [`EpochRecord`] without a certificate.
    /// Idempotent: returns `Ok(())` if the record is already stored.
    fn save_record(&mut self, record: EpochRecord) -> Result<(), EpochDbError> {
        let epoch = record.epoch;
        let idx = epoch.saturating_sub(self.start_epoch) as u64;

        if (idx as usize) < self.epoch_idx.len() {
            // Already stored — idempotent success.
            return Ok(());
        } else if idx as usize != self.epoch_idx.len() {
            return Err(EpochDbError::EpochOutOfOrder(
                self.start_epoch + self.epoch_idx.len() as Epoch,
                epoch,
            ));
        }

        let record_digest = record.digest();
        let record_pos =
            self.records.append(&record).map_err(|e| EpochDbError::Append(e.to_string()))?;
        self.record_digests
            .save(record_digest.into(), record_pos)
            .map_err(|e| EpochDbError::IndexAppend(format!("record digest: {e}")))?;
        self.epoch_idx
            .save(idx, record_pos)
            .map_err(|e| EpochDbError::IndexAppend(format!("epoch position: {e}")))?;
        self.record_digests.set_data_file_length(self.records.file_len());
        Ok(())
    }

    /// Save an [`EpochRecord`] paired with its [`EpochCertificate`].
    /// If the record is already stored, only the certificate is saved.
    /// The certificate save is idempotent: a duplicate cert is silently skipped.
    fn save(&mut self, record: EpochRecord, cert: EpochCertificate) -> Result<(), EpochDbError> {
        let record_digest = record.digest();

        // Save the record (idempotent).
        self.save_record(record)?;

        // Skip if the cert is already stored.
        if self.cert_digests.load(record_digest.into()).is_ok() {
            return Ok(());
        }

        let cert_pos = self.certs.append(&cert).map_err(|e| EpochDbError::Append(e.to_string()))?;
        self.cert_digests
            .save(record_digest.into(), cert_pos)
            .map_err(|e| EpochDbError::IndexAppend(format!("cert digest: {e}")))?;
        self.cert_digests.set_data_file_length(self.certs.file_len());
        Ok(())
    }

    /// Save an [`EpochCertificate`] keyed by `digest`. Idempotent.
    fn save_certificate(
        &mut self,
        digest: EpochDigest,
        cert: EpochCertificate,
    ) -> Result<(), EpochDbError> {
        if self.cert_digests.load(digest.into()).is_ok() {
            return Ok(());
        }
        let cert_pos = self.certs.append(&cert).map_err(|e| EpochDbError::Append(e.to_string()))?;
        self.cert_digests
            .save(digest.into(), cert_pos)
            .map_err(|e| EpochDbError::IndexAppend(format!("cert digest: {e}")))?;
        self.cert_digests.set_data_file_length(self.certs.file_len());
        Ok(())
    }

    /// Raw first-write-wins fetch of the record for `epoch`; collapses EVERY storage failure
    /// into `None`. Callers that must distinguish corruption from absence use
    /// [`Self::try_record_by_epoch`] instead.
    fn record_by_epoch(&mut self, epoch: Epoch) -> Option<EpochRecord> {
        self.try_record_by_epoch(epoch).ok().flatten()
    }

    /// Non-collapsing read of the record for `epoch`.
    ///
    /// `Ok(None)` only on genuine absence — an index or pack lookup failing with an error
    /// [`fetch_error_is_absent`] accepts. Every other storage failure (I/O, CRC mismatch,
    /// decode) surfaces as `Err` so the certified read path can classify it as
    /// [`CertifiedRecordError::Storage`] instead of a retryable "missing".
    fn try_record_by_epoch(&mut self, epoch: Epoch) -> Result<Option<EpochRecord>, FetchError> {
        if epoch < self.start_epoch {
            return Ok(None);
        }
        if epoch == 0 && self.epoch_idx.is_empty() {
            Ok(self.dummy_epoch0.clone())
        } else {
            absent_to_none(self.epoch_idx.load((epoch - self.start_epoch) as u64))?
                .map_or(Ok(None), |pos| absent_to_none(self.records.fetch(pos)))
        }
    }

    fn record_by_digest(&mut self, digest: EpochDigest) -> Option<EpochRecord> {
        let pos = self.record_digests.load(digest.into()).ok()?;
        self.records.fetch(pos).ok()
    }

    /// Raw fetch of the certificate stored under `digest`; collapses EVERY storage failure
    /// into `None`. Callers that must distinguish corruption from absence use
    /// [`Self::try_cert_by_digest`] instead.
    fn cert_by_digest(&mut self, digest: EpochDigest) -> Option<EpochCertificate> {
        self.try_cert_by_digest(digest).ok().flatten()
    }

    /// Non-collapsing read of the certificate stored under `digest`; same absence semantics as
    /// [`Self::try_record_by_epoch`].
    fn try_cert_by_digest(
        &mut self,
        digest: EpochDigest,
    ) -> Result<Option<EpochCertificate>, FetchError> {
        absent_to_none(self.cert_digests.load(digest.into()))?
            .map_or(Ok(None), |pos| absent_to_none(self.certs.fetch(pos)))
    }

    fn contains_epoch(&self, epoch: Epoch) -> bool {
        if epoch < self.start_epoch {
            return false;
        }
        if epoch == 0 && self.epoch_idx.is_empty() {
            self.dummy_epoch0.is_some()
        } else {
            ((epoch - self.start_epoch) as u64) < self.epoch_idx.len() as u64
        }
    }

    fn contains_record_digest(&mut self, digest: EpochDigest) -> bool {
        if let Ok(pos) = self.record_digests.load(digest.into()) {
            pos < self.records.file_len()
        } else {
            false
        }
    }

    fn latest_record(&mut self) -> Option<EpochRecord> {
        if self.epoch_idx.is_empty() {
            self.dummy_epoch0.clone()
        } else {
            let latest_epoch = self.start_epoch + self.epoch_idx.len() as Epoch - 1;
            self.record_by_epoch(latest_epoch)
        }
    }

    fn persist(&mut self) -> Result<(), EpochDbError> {
        if !self.records.read_only() {
            self.records.commit().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
            self.certs.commit().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
            self.epoch_idx.sync().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
            self.record_digests.sync().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
            self.cert_digests.sync().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum EpochDbError {
    IO(Arc<io::Error>),
    HeaderLoad(String),
    Append(String),
    IndexAppend(String),
    Open(Arc<OpenError>),
    EpochAlreadySaved,
    EpochOutOfOrder(Epoch, Epoch),
    MissingCertificate(Epoch),
    MissingRecord(Epoch),
    SendFailed,
    ReceiveFailed,
    PersistError(String),
    CorruptDb,
    JoinError,
}

impl Error for EpochDbError {}

impl Display for EpochDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochDbError::IO(e) => write!(f, "IO({e})"),
            EpochDbError::HeaderLoad(e) => write!(f, "Header load error ({e})"),
            EpochDbError::Append(e) => write!(f, "Data append error ({e})"),
            EpochDbError::IndexAppend(e) => write!(f, "Index append error ({e})"),
            EpochDbError::Open(e) => write!(f, "Open error: {e}"),
            EpochDbError::EpochAlreadySaved => write!(f, "Epoch record already saved"),
            EpochDbError::EpochOutOfOrder(expected, got) => {
                write!(f, "Epochs must be saved in order; expected {expected}, got {got}")
            }
            EpochDbError::MissingCertificate(epoch) => {
                write!(f, "Missing certificate for epoch {epoch}")
            }
            EpochDbError::MissingRecord(epoch) => {
                write!(f, "Missing record for epoch {epoch}")
            }
            EpochDbError::SendFailed => write!(f, "Internal channel send failed"),
            EpochDbError::ReceiveFailed => write!(f, "Internal channel receive failed"),
            EpochDbError::PersistError(e) => write!(f, "Failed to persist: {e}"),
            EpochDbError::CorruptDb => write!(f, "Epoch records database is corrupt"),
            EpochDbError::JoinError => write!(f, "Failed to join a background thread for DB"),
        }
    }
}

impl From<OpenError> for EpochDbError {
    fn from(value: OpenError) -> Self {
        Self::Open(Arc::new(value))
    }
}

impl From<FetchError> for EpochDbError {
    fn from(value: FetchError) -> Self {
        Self::HeaderLoad(value.to_string())
    }
}

impl From<io::Error> for EpochDbError {
    fn from(value: io::Error) -> Self {
        Self::IO(Arc::new(value))
    }
}

/// Write `values` into a fresh pack `data` stream at `path`, tagged with `uid_idx` and encoded
/// byte-compatibly with the read paths (`ZStd`, `EPOCH_PACK_VERSION`).
///
/// Removes any pre-existing file at `path` first, so a re-run overwrites rather than appends —
/// `Pack::open` opens read-write in append mode, so writing over a leftover file would otherwise
/// prepend prior-attempt records. A removal failure other than `NotFound` is surfaced as
/// [`EpochDbError::IO`] rather than ignored, since the stale file would otherwise survive and be
/// appended onto. Then opens the pack (which creates the file and writes its header), appends
/// every value in order, and commits so the file is complete on disk. Keeps the sentinel tags and
/// codec inside this crate so the export bundle stays readable by `read_records_from_pack` /
/// `read_certs_from_pack`.
fn write_bounded_pack<V>(path: &Path, uid_idx: u64, values: &[V]) -> Result<(), EpochDbError>
where
    V: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    // Enforce the "fresh" contract: a leftover file (e.g. from a prior failed export attempt) would
    // be appended to, not replaced. NotFound is the normal case; any other removal failure means
    // the stale file may survive, so surface it instead of appending a doubled pack.
    std::fs::remove_file(path)
        .or_else(|e| (e.kind() == io::ErrorKind::NotFound).then_some(()).ok_or(e))?;
    let mut pack =
        Pack::<V>::open(path, uid_idx, false, PackCompression::ZStd, EPOCH_PACK_VERSION)?;
    for value in values {
        pack.append(value).map_err(|e| EpochDbError::Append(e.to_string()))?;
    }
    pack.commit().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeSet,
        fs::OpenOptions,
        io::{Seek as _, SeekFrom},
        sync::Arc,
    };

    use rand::{rngs::StdRng, SeedableRng as _};
    use roaring::RoaringBitmap;
    use tempfile::TempDir;
    use tn_types::{
        BlsAggregateSignature, BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner,
        ConsensusHeaderDigest, ConsensusNumHash, Epoch, EpochCertificate, EpochDigest, EpochRecord,
        Signer as _,
    };

    use crate::{
        archive::pack::DATA_HEADER_BYTES,
        epoch_records::{
            epoch_committee_valid, CertifiedRecordError, EpochDbError, EpochRecordDb,
            EpochRecordValidation, CERTS_NAME, RECORDS_NAME,
        },
    };

    // Minimal BlsSigner wrapper around a BlsKeypair.
    #[derive(Clone)]
    struct TestSigner(Arc<BlsKeypair>);

    impl TestSigner {
        fn new(rng: &mut StdRng) -> Self {
            Self(Arc::new(BlsKeypair::generate(rng)))
        }
    }

    impl BlsSigner for TestSigner {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }

        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    /// Build an [`EpochRecord`] + [`EpochCertificate`] pair signed by all provided signers.
    fn make_test_pair(
        epoch: Epoch,
        signers: &[TestSigner],
        parent_hash: EpochDigest,
    ) -> (EpochRecord, EpochCertificate) {
        let next_committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        make_test_pair_with_next(epoch, signers, next_committee, parent_hash)
    }

    /// Like [`make_test_pair`], but with a `next_committee` that differs from the serving
    /// committee — the shape an epoch record takes when the validator set changes.
    fn make_test_pair_with_next(
        epoch: Epoch,
        signers: &[TestSigner],
        next_committee: Vec<BlsPublicKey>,
        parent_hash: EpochDigest,
    ) -> (EpochRecord, EpochCertificate) {
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        let record = EpochRecord {
            epoch,
            committee,
            next_committee,
            parent_hash,
            final_consensus: ConsensusNumHash::new(
                (epoch as u64 + 1) * 10,
                ConsensusHeaderDigest::default(),
            ),
            ..Default::default()
        };

        let votes: Vec<_> = signers.iter().map(|s| record.sign_vote(s)).collect();
        let sigs: Vec<BlsSignature> = votes.iter().map(|v| v.signature).collect();
        let aggregated =
            BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate signatures");
        let signature = aggregated.to_signature();
        let mut signed_authorities = RoaringBitmap::new();
        for i in 0..signers.len() as u32 {
            signed_authorities.push(i);
        }
        let cert = EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
        (record, cert)
    }

    #[tokio::test]
    async fn read_records_and_certs_from_pack_round_trips() {
        let temp_dir = TempDir::with_prefix("read_records_from_pack").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        // Write a handful of records with their certificates, flush, and close so the on-disk
        // `epochs.pack` / `epoch_certs.pack` are complete.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let mut records = Vec::new();
        let mut certs = Vec::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..6u32 {
            let (record, cert) = make_test_pair(epoch, &signers, parent);
            parent = record.digest();
            db.save(record.clone(), cert.clone()).await.expect("save record + cert");
            records.push(record);
            certs.push(cert);
        }
        db.persist().await.expect("persist");
        drop(db);

        // Reading the bare packs (no sidecar indexes) must return every record and cert in order.
        let got_records = EpochRecordDb::read_records_from_pack(temp_dir.path().join(RECORDS_NAME))
            .expect("read records from pack");
        assert_eq!(got_records.len(), records.len());
        for (got, want) in got_records.iter().zip(records.iter()) {
            assert_eq!(got.epoch, want.epoch);
            assert_eq!(got.digest(), want.digest());
        }

        let got_certs = EpochRecordDb::read_certs_from_pack(temp_dir.path().join(CERTS_NAME))
            .expect("read certs from pack");
        assert_eq!(got_certs.len(), certs.len());
        for (got, want) in got_certs.iter().zip(certs.iter()) {
            assert_eq!(got.epoch_hash, want.epoch_hash);
        }
    }

    #[tokio::test]
    async fn export_bundle_peeks_write_error_without_clearing_it() {
        // Regression test for finding #6: the read-only export must surface a pending background
        // write error WITHOUT clearing it, so the write path (the acknowledger) still learns of the
        // failure instead of it being silently consumed by an in-flight export.
        let temp_dir = TempDir::with_prefix("export_peek_error").expect("temp dir");
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        // Simulate a background write failure the actor recorded into the shared error slot.
        db.error.send_replace(Some(EpochDbError::CorruptDb));

        // The export surfaces the pending error (it returns at `peek_error()?` before any disk
        // work).
        let err = db
            .export_bounded_bundle(0, &temp_dir.path().join("recs"), &temp_dir.path().join("certs"))
            .await
            .expect_err("export must surface the pending write error");
        assert!(matches!(err, EpochDbError::CorruptDb), "unexpected error: {err:?}");

        // ...but must NOT clear it: the write path still learns of the failure (the #6 fix).
        let latched = db.get_error().expect_err("write path must still see the error");
        assert!(matches!(latched, EpochDbError::CorruptDb), "unexpected error: {latched:?}");

        // `get_error` is the acknowledger, so the slot is cleared only after it is read there.
        db.get_error().expect("slot cleared after acknowledgement");
    }

    #[tokio::test]
    async fn persist_reports_a_write_that_failed_while_the_flush_was_queued() {
        // Regression test for #1065: `persist()` samples the error slot before it enqueues, and
        // writes are fire-and-forget, so a save that fails while the `Persist` message is still
        // queued behind it must be folded into the persist reply. Otherwise the epoch-close path
        // treats `Ok(())` as proof of durability for a record that never reached disk.
        let temp_dir = TempDir::with_prefix("persist_queued_write_error").expect("temp dir");
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");

        // Queue a save the actor will reject — epoch 5 is out of order on an empty db — and a
        // persist behind it. Both go straight to the channel: the handle-side guards would reject
        // this record before it ever reached the actor, and the point of the test is the actor's
        // ordering. A single consumer draining a FIFO channel guarantees the save fails before
        // the persist is dequeued, so this is deterministic rather than a race.
        let record = EpochRecord { epoch: 5, ..Default::default() };
        db.tx.send(super::EpochDbMessage::SaveRecord(record)).await.expect("queue failing save");
        let (tx, rx) = tokio::sync::oneshot::channel();
        db.tx.send(super::EpochDbMessage::Persist(tx)).await.expect("queue persist");

        let err = rx
            .await
            .expect("actor replied to the persist")
            .expect_err("persist must report the write that failed while it was queued");
        assert!(matches!(err, EpochDbError::EpochOutOfOrder(0, 5)), "unexpected error: {err:?}");

        // The flush consumed the failure, so it is not left behind to be misattributed to an
        // unrelated later caller.
        db.get_error().expect("persist acknowledged the error");
    }

    #[test]
    fn write_bounded_pack_overwrites_stale_file() {
        // Regression test for finding #14: `write_bounded_pack` must produce a FRESH pack. A
        // leftover file (e.g. from a prior failed export attempt) must be overwritten, not
        // appended to — otherwise the bundle would carry prior-attempt records prepended to
        // this attempt's.
        let dir = TempDir::with_prefix("write_bounded_fresh").expect("temp dir");
        let path = dir.path().join("epoch_records");
        let first: Vec<EpochRecord> =
            (0..3).map(|epoch| EpochRecord { epoch, ..Default::default() }).collect();
        let second: Vec<EpochRecord> =
            (0..2).map(|epoch| EpochRecord { epoch, ..Default::default() }).collect();

        super::write_bounded_pack(&path, super::Inner::PACK_EPOCH, &first).expect("first write");
        super::write_bounded_pack(&path, super::Inner::PACK_EPOCH, &second).expect("second write");

        // Fresh, not append: reading back yields exactly the second write (2 records), not 3+2=5.
        let got = EpochRecordDb::read_records_from_pack(&path).expect("read records");
        assert_eq!(
            got.len(),
            2,
            "second write must overwrite, not append (got {} records)",
            got.len()
        );
        assert_eq!(got.iter().map(|r| r.epoch).collect::<Vec<_>>(), vec![0, 1]);
    }

    /// Issue #1080: a non-`NotFound` `remove_file` failure must surface as an error instead of
    /// being swallowed. A stale file surviving a failed removal would be appended onto, silently
    /// doubling the pack.
    #[test]
    #[cfg(unix)]
    fn write_bounded_pack_surfaces_remove_file_failure() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = TempDir::with_prefix("write_bounded_remove_err").expect("temp dir");
        let path = dir.path().join("epoch_records");
        let first: Vec<EpochRecord> =
            (0..3).map(|epoch| EpochRecord { epoch, ..Default::default() }).collect();
        super::write_bounded_pack(&path, super::Inner::PACK_EPOCH, &first).expect("first write");
        let stale_bytes = std::fs::read(&path).expect("read stale pack");

        // A probe file distinguishes root (directory permissions are bypassed, removal succeeds)
        // from a genuine `PermissionDenied` environment.
        let probe = dir.path().join("root-probe");
        std::fs::write(&probe, b"probe").expect("write probe");

        // Read-only directory: `remove_file` on its entries now fails with `PermissionDenied`.
        let writable = std::fs::metadata(dir.path()).expect("dir metadata").permissions();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o555))
            .expect("make dir read-only");
        if std::fs::remove_file(&probe).is_ok() {
            // Running as root (directory permissions are bypassed): the error branch is
            // unreachable, so the regression is untestable here. Say so loudly rather than
            // report a silently vacuous pass, then restore cleanup permissions.
            eprintln!(
                "write_bounded_pack_surfaces_remove_file_failure: SKIPPED (root bypasses \
                 directory permissions, so nothing was tested)"
            );
            std::fs::set_permissions(dir.path(), writable).expect("restore permissions");
        } else {
            let second: Vec<EpochRecord> =
                (0..2).map(|epoch| EpochRecord { epoch, ..Default::default() }).collect();
            let result = super::write_bounded_pack(&path, super::Inner::PACK_EPOCH, &second);

            // Restore before asserting so `TempDir` cleanup works even if an assertion fails.
            std::fs::set_permissions(dir.path(), writable).expect("restore permissions");

            let err = result.expect_err("removal failure must surface, not append");
            assert!(matches!(err, super::EpochDbError::IO(_)), "unexpected error: {err:?}");
            // The stale pack is byte-identical: nothing was appended.
            assert_eq!(std::fs::read(&path).expect("re-read pack"), stale_bytes);
        }
    }

    #[tokio::test]
    async fn first_missing_historical_cert_excludes_pending_tip() {
        // Finding #8: the export pre-check verifies historical certs `0..tip` are present,
        // EXCLUDING the tip epoch (whose cert is legitimately still pending at export
        // time).
        let dir = TempDir::with_prefix("first_missing_tip").expect("temp dir");
        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        // epochs 0..=2 each with a cert; epoch 3's record saved but its cert still pending.
        let mut parent = EpochDigest::default();
        for epoch in 0..=2u32 {
            let (record, cert) = make_test_pair(epoch, &signers, parent);
            parent = record.digest();
            db.save(record, cert).await.expect("save with cert");
        }
        let (record3, _cert3) = make_test_pair(3, &signers, parent);
        db.save_record(record3).await.expect("save record only");

        // tip = 3 scans 0..3 — all have certs — so nothing is missing (tip 3 excluded).
        assert_eq!(db.first_missing_historical_cert(3).await, None);
        // tip = 4 scans 0..4 — epoch 3 has no cert — so it is the first missing.
        assert_eq!(db.first_missing_historical_cert(4).await, Some(3));
    }

    #[tokio::test]
    async fn first_missing_historical_cert_detects_middle_gap() {
        // A historical cert missing in the middle (epoch 1) must be caught before the tip.
        let dir = TempDir::with_prefix("first_missing_gap").expect("temp dir");
        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let (r0, c0) = make_test_pair(0, &signers, EpochDigest::default());
        let (r1, _c1) = make_test_pair(1, &signers, r0.digest());
        let (r2, c2) = make_test_pair(2, &signers, r1.digest());
        db.save(r0, c0).await.expect("save 0 with cert");
        db.save_record(r1).await.expect("save 1 record only"); // cert never arrived
        db.save(r2, c2).await.expect("save 2 with cert");

        // tip = 3 scans 0..3 — epoch 1 has a record but no cert.
        assert_eq!(db.first_missing_historical_cert(3).await, Some(1));
    }

    #[test]
    fn missing_record_error_display() {
        // The record-absent case in `export_bounded_bundle` uses `MissingRecord`, not the
        // misleading `EpochOutOfOrder` ("Epochs must be saved in order...").
        assert_eq!(EpochDbError::MissingRecord(5).to_string(), "Missing record for epoch 5");
    }

    #[tokio::test]
    async fn test_epoch_record_db() {
        let temp_dir = TempDir::with_prefix("test_epoch_record_db").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        // Create and populate an initial database.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");

        let num_records: u32 = 20;
        let mut pairs = Vec::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..num_records {
            let (record, cert) = make_test_pair(epoch, &signers, parent);
            parent = record.digest();
            db.save(record.clone(), cert.clone()).await.expect("save");
            pairs.push((record, cert));
        }

        // Verify lookup by epoch and digest.
        for (record, cert) in &pairs {
            let by_epoch = db.record_by_epoch(record.epoch).await.expect("record by epoch");
            assert_eq!(by_epoch.digest(), record.digest());

            let by_digest = db.record_by_digest(record.digest()).await.expect("record by digest");
            assert_eq!(by_digest.digest(), record.digest());

            let cert_back = db.cert_by_digest(record.digest()).await.expect("cert by digest");
            assert_eq!(cert_back.epoch_hash, cert.epoch_hash);

            assert!(db.contains_epoch(record.epoch).await);
            assert!(db.contains_record_digest(record.digest()).await);
        }

        // Latest record should be the last one saved.
        let latest = db.latest_record().await.expect("latest record");
        assert_eq!(latest.epoch, num_records - 1);

        db.persist().await.expect("persist");
        drop(db);

        // Reopen in append mode and add more records.
        let db = EpochRecordDb::open(temp_dir.path()).expect("reopen db");
        for epoch in num_records..(num_records * 2) {
            let (record, cert) = make_test_pair(epoch, &signers, parent);
            parent = record.digest();
            db.save(record.clone(), cert.clone()).await.expect("save after reopen");
            pairs.push((record, cert));
        }
        for (record, _) in &pairs {
            let by_epoch = db.record_by_epoch(record.epoch).await.expect("record by epoch 2");
            assert_eq!(by_epoch.digest(), record.digest());
        }
        db.persist().await.expect("persist 2");
        drop(db);

        // Open and verify all records are still accessible.
        let db = EpochRecordDb::open(temp_dir.path()).expect("db open");
        for (record, cert) in pairs.iter() {
            let by_epoch = db.record_by_epoch(record.epoch).await.expect("static: record by epoch");
            assert_eq!(by_epoch.digest(), record.digest());

            let cert_back =
                db.cert_by_digest(record.digest()).await.expect("static: cert by digest");
            assert_eq!(cert_back.epoch_hash, cert.epoch_hash);
        }
        assert!(!db.contains_epoch(num_records * 2).await);
        for number in 0..pairs.len() * 10 {
            let epoch = (number.saturating_sub(1) / 10) as u32;
            assert_eq!(
                epoch,
                db.number_to_epoch(number as u64),
                "failed to get epoch for {number}"
            );
        }
        assert!(!db.contains_epoch(num_records * 2).await);
        drop(db);

        // --- Damage test: truncate the last byte of the records file. ---
        let records_path = temp_dir.path().join(RECORDS_NAME);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&records_path)
            .expect("open records file");
        let original_len = f.seek(SeekFrom::End(0)).expect("seek");
        f.set_len(original_len - 1).expect("truncate -1");
        drop(f);

        // Reopen should heal: last record is dropped, all others remain readable.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open after damage");
        for (record, _) in pairs.iter().take(pairs.len() - 1) {
            let by_epoch = db
                .record_by_epoch(record.epoch)
                .await
                .expect(&format!("damaged reopen: epoch {}", record.epoch));
            assert_eq!(by_epoch.digest(), record.digest());
        }
        // The damaged final record should be gone.
        assert!(db.record_by_epoch((num_records * 2) - 1).await.is_none());

        // Re-save the last record and confirm it round-trips.
        let (last_record, last_cert) = pairs.last().unwrap().clone();
        db.save(last_record.clone(), last_cert).await.expect("re-save last");
        let recovered = db.record_by_epoch(last_record.epoch).await.expect("recovered");
        assert_eq!(recovered.digest(), last_record.digest());
        db.persist().await.expect("persist after heal");
        drop(db);

        // File should be back to the original length after healing + re-save.
        let mut f = OpenOptions::new().read(true).open(&records_path).expect("open records file");
        let restored_len = f.seek(SeekFrom::End(0)).expect("seek");
        assert_eq!(original_len, restored_len, "file length should be restored after re-save");

        // --- Damage test: extend the file with 100 garbage bytes. ---
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&records_path)
            .expect("open records file");
        let extended_len = f.seek(SeekFrom::End(0)).expect("seek");
        f.set_len(extended_len + 100).expect("extend +100");
        drop(f);

        let db = EpochRecordDb::open(temp_dir.path()).expect("open after extend");
        for (record, _) in &pairs {
            let by_epoch = db
                .record_by_epoch(record.epoch)
                .await
                .expect(&format!("extended reopen: epoch {}", record.epoch));
            assert_eq!(by_epoch.digest(), record.digest());
        }
        drop(db);

        // Healing should have truncated the garbage back to the correct length.
        let mut f = OpenOptions::new().read(true).open(&records_path).expect("open records file");
        let healed_len = f.seek(SeekFrom::End(0)).expect("seek");
        assert_eq!(extended_len, healed_len, "garbage bytes should be removed on reopen");
    }

    /// Generate a deterministic test BLS public key from a seed.
    fn test_bls_key(seed: u8) -> BlsPublicKey {
        let mut rng = StdRng::from_seed([seed; 32]);
        *BlsKeypair::generate(&mut rng).public()
    }

    /// Create a test [`EpochRecord`] carrying the given committee.
    fn test_epoch_record(committee: Vec<BlsPublicKey>) -> EpochRecord {
        EpochRecord { epoch: 1, committee, ..Default::default() }
    }

    #[test]
    fn test_epoch_committee_valid_equal_committees() {
        // When committees are equal in size, they must be exactly equal
        let keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let epoch_rec = test_epoch_record(keys.clone());
        let committee: BTreeSet<_> = keys.into_iter().collect();

        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_equal_but_different() {
        // Same size but different members should fail
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let other_keys: Vec<_> = (10..14).map(test_bls_key).collect();

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = other_keys.into_iter().collect();

        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_committee_smaller_than_epoch() {
        // If committee is smaller than epoch_rec.committee, always invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let smaller_keys: Vec<_> = (0..3).map(test_bls_key).collect();

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = smaller_keys.into_iter().collect();

        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_committee_larger_valid() {
        // Committee larger but all epoch members present and epoch >= 4 and >= 2/3
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        larger_keys.push(test_bls_key(10)); // Add one more

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=4, committee_len=5, 2/3 of 5 = 3, 4 >= 3 so valid
        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_epoch_too_small() {
        // Epoch committee smaller than 4 is invalid (even if all present)
        let epoch_keys: Vec<_> = (0..3).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        larger_keys.push(test_bls_key(10));
        larger_keys.push(test_bls_key(11));

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=3 < 4, so invalid
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_epoch_less_than_two_thirds() {
        // Epoch committee less than 2/3 of committee is invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        // Add many more keys to committee so epoch is < 2/3
        let mut larger_keys = epoch_keys.clone();
        for i in 10..20 {
            larger_keys.push(test_bls_key(i));
        }

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=4, committee_len=14, 2/3 of 14 = 9, 4 < 9 so invalid
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_member_not_in_committee() {
        // Epoch has a member not in committee - invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let mut committee_keys: Vec<_> = (0..3).map(test_bls_key).collect();
        committee_keys.push(test_bls_key(10)); // Different key
        committee_keys.push(test_bls_key(11)); // Extra to make it larger

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = committee_keys.into_iter().collect();

        // epoch key 3 is not in committee
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_boundary_two_thirds() {
        // Test exactly at 2/3 boundary
        let epoch_keys: Vec<_> = (0..6).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        for i in 10..13 {
            larger_keys.push(test_bls_key(i));
        }

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=6, committee_len=9, 2/3 of 9 = 6, 6 >= 6 so valid
        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_accepts_anchored_committee() {
        // A downloaded record whose committee matches the previous epoch's next_committee, with
        // the expected parent hash and a super-quorum cert, is accepted.
        let temp_dir = TempDir::with_prefix("validate_accept").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &committee, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");

        let (rec1, cert1) = make_test_pair(1, &committee, rec0.digest());
        let validation = db.validate_downloaded_record(1, &rec1, &cert1).await;
        assert_eq!(validation, EpochRecordValidation::Valid);
        assert!(validation.is_valid());
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_rejects_unanchored_committee() {
        // Regression for the failed-quorum recovery path: an attacker-committee record with a
        // certificate self-signed by that same attacker committee is self-consistent (passes
        // verify_with_cert alone) but must be rejected because its committee is not anchored to
        // the locally-trusted committee from the previous epoch's next_committee.
        let temp_dir = TempDir::with_prefix("validate_reject").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let honest: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let attacker: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &honest, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");

        let (attacker_rec, attacker_cert) = make_test_pair(1, &attacker, rec0.digest());
        // The record is self-consistent, so the weak check the recovery path used to rely on
        // accepts it.
        assert!(attacker_rec.verify_with_cert(&attacker_cert));

        // The shared anchor rejects it: the committee is not the honest next_committee.
        let validation = db.validate_downloaded_record(1, &attacker_rec, &attacker_cert).await;
        assert!(!validation.is_valid());
        assert_eq!(
            validation,
            EpochRecordValidation::Invalid {
                epoch_matches: true,
                parents_match: true,
                committee_valid: false,
                cert_valid: true,
            }
        );
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_no_anchor_when_prev_missing() {
        // With no previous epoch record stored, there is no locally-trusted committee to anchor
        // against, so even a self-consistent record is not accepted.
        let temp_dir = TempDir::with_prefix("validate_no_anchor").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec1, cert1) = make_test_pair(1, &committee, EpochDigest::default());
        let validation = db.validate_downloaded_record(1, &rec1, &cert1).await;
        assert_eq!(validation, EpochRecordValidation::NoAnchor);
        assert!(!validation.is_valid());
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_rejects_wrong_parent_hash() {
        // A record with the correct anchored committee and a valid self-cert but the wrong parent
        // hash is rejected: parent-hash chaining is a required part of the anchor, independent of
        // the committee check.
        let temp_dir = TempDir::with_prefix("validate_parent").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &committee, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");

        // Correct committee and a valid cert, but the parent hash is the default digest rather
        // than epoch 0's digest, so only parents_match should fail.
        let (rec1, cert1) = make_test_pair(1, &committee, EpochDigest::default());
        let validation = db.validate_downloaded_record(1, &rec1, &cert1).await;
        assert!(!validation.is_valid());
        assert_eq!(
            validation,
            EpochRecordValidation::Invalid {
                epoch_matches: true,
                parents_match: false,
                committee_valid: true,
                cert_valid: true,
            }
        );
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_accepts_genesis_epoch() {
        // The epoch-0 branch anchors against the genesis committee (get_committee_keys(0)) with a
        // default parent hash. A genesis record whose committee is the stored genesis committee is
        // accepted.
        let temp_dir = TempDir::with_prefix("validate_genesis").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        // Store the genesis record so get_committee_keys(0) resolves the genesis committee.
        let (rec0, cert0) = make_test_pair(0, &committee, EpochDigest::default());
        db.save(rec0.clone(), cert0.clone()).await.expect("save epoch 0");

        let validation = db.validate_downloaded_record(0, &rec0, &cert0).await;
        assert_eq!(validation, EpochRecordValidation::Valid);
        assert!(validation.is_valid());
    }

    #[tokio::test]
    async fn validate_chain_against_seeded_genesis_dummy() {
        // Mirrors `db load-state`'s verification: seed a dummy epoch-0 carrying the genesis
        // committee, verify the real epoch-0 record against it *before* saving (so the dummy — not
        // the record itself — is the trust anchor), then verify epoch 1 against the saved epoch-0's
        // `next_committee`.
        let temp_dir = TempDir::with_prefix("validate_seeded_dummy").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let genesis_keys: Vec<BlsPublicKey> = committee.iter().map(|s| s.public_key()).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        db.save_dummy_epoch0(EpochRecord {
            epoch: 0,
            committee: genesis_keys.clone(),
            next_committee: genesis_keys,
            ..Default::default()
        })
        .await
        .expect("seed dummy");

        // Epoch 0 verifies against the seeded dummy (record index still empty), then is saved.
        let (rec0, cert0) = make_test_pair(0, &committee, EpochDigest::default());
        assert_eq!(
            db.validate_downloaded_record(0, &rec0, &cert0).await,
            EpochRecordValidation::Valid,
            "epoch 0 should verify against the seeded genesis committee"
        );
        db.save(rec0.clone(), cert0.clone()).await.expect("save epoch 0");

        // Epoch 1 chains from the saved epoch-0 record and verifies against its `next_committee`.
        let (rec1, cert1) = make_test_pair(1, &committee, rec0.digest());
        assert_eq!(
            db.validate_downloaded_record(1, &rec1, &cert1).await,
            EpochRecordValidation::Valid,
            "epoch 1 should verify against the saved epoch-0 next_committee"
        );

        // A record whose committee is unrelated to the trusted chain is rejected.
        let attacker: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let (bad1, bad_cert1) = make_test_pair(1, &attacker, rec0.digest());
        assert_ne!(
            db.validate_downloaded_record(1, &bad1, &bad_cert1).await,
            EpochRecordValidation::Valid,
            "a record signed by an unanchored committee must be rejected"
        );
    }

    #[tokio::test]
    async fn test_validate_downloaded_record_rejects_wrong_epoch() {
        // A peer answering a request for epoch N must not satisfy it with a self-consistent record
        // for a different epoch. Even a genuine, correctly-anchored historical record is rejected
        // when offered for the wrong slot, because the anchor is derived from the requested epoch,
        // not the record's self-declared epoch.
        let temp_dir = TempDir::with_prefix("validate_wrong_epoch").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let committee: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        // A legitimate chain: epoch 0 and epoch 1.
        let (rec0, cert0) = make_test_pair(0, &committee, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");
        let (rec1, cert1) = make_test_pair(1, &committee, rec0.digest());
        db.save(rec1.clone(), cert1.clone()).await.expect("save epoch 1");

        // rec1 is genuine and correctly anchored for epoch 1, but here it is offered as the answer
        // to a request for epoch 2. It must be rejected because it is not for the requested epoch.
        let validation = db.validate_downloaded_record(2, &rec1, &cert1).await;
        assert!(!validation.is_valid());
        assert_eq!(
            validation,
            EpochRecordValidation::Invalid {
                epoch_matches: false,
                parents_match: false,
                committee_valid: true,
                cert_valid: true,
            }
        );
    }

    /// Committee-key lookups across an epoch whose committee shrank mid-epoch because a
    /// validator was ejected on-chain (governance `burn` / slash-to-zero). The ejection
    /// epoch's record carries the shrunken, swap-and-popped committee; the epoch after it
    /// has no record yet and must be served from the ejection record's `next_committee`.
    #[tokio::test]
    async fn test_get_committee_keys_across_ejection_epoch() {
        let temp_dir = TempDir::with_prefix("test_get_committee_keys_across_ejection_epoch")
            .expect("temp dir");
        let mut rng = StdRng::seed_from_u64(0xE1EC7);
        let signers: Vec<TestSigner> = (0..6).map(|_| TestSigner::new(&mut rng)).collect();
        let five = &signers[..5];
        // A new validator activates during epoch 1 and joins the next committee.
        let incoming = signers[5].public_key();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");

        // Epoch 0: the full five-member committee.
        let (rec0, cert0) = make_test_pair(0, five, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save rec0");

        // Epoch 1: signers[2] was ejected mid-epoch. Its record carries the shrunken
        // committee in the on-chain swap-and-pop order, and a next committee that differs
        // from the serving one (survivors + the incoming validator).
        let ejected = signers[2].public_key();
        let survivors =
            vec![signers[0].clone(), signers[1].clone(), signers[4].clone(), signers[3].clone()];
        let mut next1: Vec<BlsPublicKey> = survivors.iter().map(|s| s.public_key()).collect();
        next1.push(incoming);
        let (rec1, cert1) = make_test_pair_with_next(1, &survivors, next1.clone(), rec0.digest());
        db.save(rec1, cert1).await.expect("save rec1");

        // Pre-ejection epoch reads the full committee.
        let keys0 = db.get_committee_keys(0).await.expect("keys for epoch 0");
        assert_eq!(keys0, five.iter().map(|s| s.public_key()).collect::<BTreeSet<_>>());

        // The ejection epoch reads exactly the shrunken set; the ejected key is gone.
        let keys1 = db.get_committee_keys(1).await.expect("keys for epoch 1");
        assert_eq!(keys1, survivors.iter().map(|s| s.public_key()).collect::<BTreeSet<_>>());
        assert!(!keys1.contains(&ejected));

        // Epoch 2 has no record yet: served from rec1.next_committee (not rec1.committee).
        let keys2 = db.get_committee_keys(2).await.expect("keys for epoch 2 fallback");
        assert_eq!(keys2, next1.iter().copied().collect::<BTreeSet<_>>());
        assert!(keys2.contains(&incoming));
        assert!(!keys2.contains(&ejected));

        // Beyond the one-epoch fallback horizon there is nothing to serve.
        assert!(db.get_committee_keys(3).await.is_none());
    }

    /// The dummy epoch-0 record is memory-only: `persist` never writes it, so a reopen loses
    /// it entirely — no record 0, no committee anchor for validating downloaded records. This
    /// is why every epoch-close path must overwrite the dummy with a real, persisted record
    /// before a restart can be survived: a real record saved and persisted comes back from
    /// reopen with its digest and committee anchor intact.
    #[tokio::test]
    async fn test_dummy_epoch0_lost_on_reopen_but_persisted_record_survives() {
        let temp_dir = TempDir::with_prefix("test_dummy_epoch0_lost_on_reopen").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();

        // Save the dummy record 0 and persist: the dummy lives only in memory, so even an
        // explicit persist leaves nothing durable behind.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let dummy = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.clone(),
            ..Default::default()
        };
        db.save_dummy_epoch0(dummy).await.expect("save dummy epoch 0");
        db.persist().await.expect("persist dummy");
        // The dummy serves reads while this handle is open.
        assert!(db.contains_epoch(0).await);
        drop(db);

        // Reopen: the dummy is gone — no record and no committee anchor.
        let db = EpochRecordDb::open(temp_dir.path()).expect("reopen after dummy");
        assert!(!db.contains_epoch(0).await, "dummy epoch 0 must not survive reopen");
        assert!(db.record_by_epoch(0).await.is_none(), "no durable record 0 after reopen");
        assert!(db.get_committee_keys(0).await.is_none(), "no committee anchor after reopen");

        // A real record 0, saved and persisted, survives the same reopen cycle.
        let (real0, _cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(real0.clone()).await.expect("save real record 0");
        db.persist().await.expect("persist real record 0");
        drop(db);

        let db = EpochRecordDb::open(temp_dir.path()).expect("reopen after real record");
        assert!(db.contains_epoch(0).await, "persisted record 0 must survive reopen");
        let survived = db.record_by_epoch(0).await.expect("record 0 after reopen");
        assert_eq!(survived.digest(), real0.digest(), "record survives with equal digest");
        let anchor = db.get_committee_keys(0).await.expect("committee anchor restored");
        assert_eq!(anchor, committee.iter().copied().collect::<BTreeSet<_>>());
    }

    /// The in-memory epoch-0 dummy and the real epoch-0 record are different records with
    /// different digests, and the dummy answers `record_by_epoch(0)` for as long as the index
    /// stays empty — which is exactly the window in which epoch 1 opens.
    ///
    /// #1032's `certified_prior_epoch_anchor` originally cross-checked the certified epoch-0
    /// digest against whatever this read returned and aborted the node on a mismatch. Both
    /// reads are legitimate; they simply do not agree until epoch 0's real record is
    /// persisted, so the mismatch was an ordinary startup race rather than the corruption the
    /// check assumed. Anything that needs an epoch-0 digest must take the certified record,
    /// never this one.
    #[tokio::test]
    async fn test_dummy_epoch0_digest_diverges_from_the_real_record() {
        let temp_dir = TempDir::with_prefix("test_dummy_epoch0_digest_diverges").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();

        // The genesis dummy `run_epoch.rs` installs at startup: real committee, everything
        // else defaulted.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let dummy = EpochRecord {
            epoch: 0,
            committee: committee.clone(),
            next_committee: committee.clone(),
            ..Default::default()
        };
        db.save_dummy_epoch0(dummy.clone()).await.expect("save dummy epoch 0");

        // While the index is empty, the dummy IS the answer for epoch 0.
        let served = db.record_by_epoch(0).await.expect("dummy must serve epoch-0 reads");
        assert_eq!(served.digest(), dummy.digest(), "the dummy is what an epoch-0 read returns");

        // But it is not the record the network certifies. Same epoch and same committee, and
        // still a different digest — the real record seals a final consensus block, the dummy
        // seals nothing.
        let (real0, cert0) = make_test_pair(0, &signers, EpochDigest::default());
        assert_eq!(real0.epoch, dummy.epoch, "same epoch");
        assert_eq!(real0.committee, dummy.committee, "same committee");
        assert_ne!(
            real0.digest(),
            dummy.digest(),
            "dummy and real epoch-0 record must be assumed to differ: a digest equality check \
             between them is a false invariant, not a corruption detector",
        );

        // Once the real record lands the read flips, with no signal to anyone still holding
        // the dummy's digest. That is what makes the window a race rather than a stable state.
        db.save(real0.clone(), cert0).await.expect("save real record 0 + cert");
        let after = db.record_by_epoch(0).await.expect("real record must now serve reads");
        assert_eq!(after.digest(), real0.digest(), "the epoch-0 read changed answer mid-flight");
    }

    #[tokio::test]
    async fn test_certified_record_by_epoch_accepts_certified_record() {
        // Positive control: a record saved with its quorum certificate is released, and the
        // released record is byte-identical (equal digest) to the raw fetch.
        let temp_dir = TempDir::with_prefix("certified_accept").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");

        let certified =
            db.certified_record_by_epoch(0).await.expect("certified record 0 must be released");
        assert_eq!(certified.digest(), rec0.digest());

        // The timeout variant takes the same fast path with no waiting.
        let start = tokio::time::Instant::now();
        let certified = db
            .certified_record_by_epoch_with_timeout(0, std::time::Duration::from_secs(30))
            .await
            .expect("certified record 0 must be released without waiting");
        assert_eq!(certified.digest(), rec0.digest());
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "an already-certified record must not wait on the timeout"
        );
    }

    #[tokio::test]
    async fn test_certified_record_by_epoch_refuses_uncertified_record() {
        // The core seed-anchor defect: a record saved without any certificate is served by the
        // raw record_by_epoch fetch but must be refused by the certified fetch.
        let temp_dir = TempDir::with_prefix("certified_missing_cert").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, _cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(rec0.clone()).await.expect("save record without cert");

        // Raw fetch serves the uncertified record — this is the gap being closed.
        assert!(db.record_by_epoch(0).await.is_some());
        let err = db
            .certified_record_by_epoch(0)
            .await
            .expect_err("an uncertified record must be refused");
        assert_eq!(err, CertifiedRecordError::MissingCertificate(0, rec0.digest()));
        assert!(err.is_retryable(), "a missing cert can still arrive and is retryable");
    }

    #[tokio::test]
    async fn test_certified_record_by_epoch_missing_record() {
        // No record stored at all: MissingRecord, retryable (the collector can still fetch it).
        let temp_dir = TempDir::with_prefix("certified_missing_rec").expect("temp dir");
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let err =
            db.certified_record_by_epoch(3).await.expect_err("a missing record must be refused");
        assert_eq!(err, CertifiedRecordError::MissingRecord(3));
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn test_certified_record_by_epoch_refuses_invalid_certificate() {
        // A certificate stored under the record's digest that does not verify against the
        // record (here: a valid cert for a DIFFERENT record, filed under this record's digest)
        // must be refused, and the failure is terminal — cert writes are append-once per
        // digest, so the timeout variant must fail fast instead of polling out the clock.
        let temp_dir = TempDir::with_prefix("certified_invalid_cert").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let others: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, _cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(rec0.clone()).await.expect("save record without cert");
        let (_other_rec, other_cert) = make_test_pair(0, &others, EpochDigest::default());
        db.save_certificate(rec0.digest(), other_cert).await.expect("file foreign cert");

        let err = db
            .certified_record_by_epoch(0)
            .await
            .expect_err("a non-verifying certificate must be refused");
        assert_eq!(err, CertifiedRecordError::InvalidCertificate(0, rec0.digest()));
        assert!(!err.is_retryable(), "an invalid stored cert can never be replaced");

        let start = tokio::time::Instant::now();
        let err = db
            .certified_record_by_epoch_with_timeout(0, std::time::Duration::from_secs(30))
            .await
            .expect_err("the timeout variant must also refuse the invalid certificate");
        assert_eq!(err, CertifiedRecordError::InvalidCertificate(0, rec0.digest()));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "an invalid certificate is terminal and must fail fast, not poll out the timeout"
        );
    }

    #[tokio::test]
    async fn test_certified_record_with_timeout_heals_when_cert_arrives() {
        // The wait path: the record is stored but its certificate arrives asynchronously
        // (mirroring vote aggregation finishing after the next epoch opens). The poll must
        // pick the certificate up and release the record within the timeout.
        let temp_dir = TempDir::with_prefix("certified_heal").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(rec0.clone()).await.expect("save record without cert");

        let db_writer = db.clone();
        let digest = rec0.digest();
        let writer = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            db_writer.save_certificate(digest, cert0).await.expect("late cert save");
        });

        let certified = db
            .certified_record_by_epoch_with_timeout(0, std::time::Duration::from_secs(10))
            .await
            .expect("the record must be released once its certificate arrives");
        assert_eq!(certified.digest(), rec0.digest());
        writer.await.expect("cert writer task");

        // The short-timeout counterpart: with no writer, the retryable wait expires and the
        // final retryable error is surfaced rather than swallowed.
        let temp_dir2 = TempDir::with_prefix("certified_timeout").expect("temp dir");
        let db2 = EpochRecordDb::open(temp_dir2.path()).expect("open db2");
        let (rec, _cert) = make_test_pair(0, &signers, EpochDigest::default());
        db2.save_record(rec.clone()).await.expect("save record without cert");
        let err = db2
            .certified_record_by_epoch_with_timeout(0, std::time::Duration::from_millis(500))
            .await
            .expect_err("with no certificate ever arriving, the wait must expire");
        assert_eq!(err, CertifiedRecordError::MissingCertificate(0, rec.digest()));
    }

    /// On-disk corruption under the certified read path must classify as the non-retryable
    /// [`CertifiedRecordError::Storage`] — never as a retryable "missing" outcome that the
    /// timeout variant would poll for the full budget before mislabeling the corruption.
    /// Modeled on `test_latest_consensus_recovers_from_corrupt_slot` in `consensus.rs`: write
    /// real data, flip one payload byte on disk to break the record's CRC, then read.
    #[tokio::test]
    async fn test_certified_record_by_epoch_reports_storage_corruption() {
        use std::io::{Read as _, Write as _};

        let temp_dir = TempDir::with_prefix("certified_storage_error").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &signers, EpochDigest::default());
        let (rec1, cert1) = make_test_pair(1, &signers, rec0.digest());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");
        db.save(rec1.clone(), cert1).await.expect("save epoch 1");
        db.persist().await.expect("persist");

        // Flip the first payload byte of the first stored record (epoch 0). The pack layout
        // is the fixed data header, then per record a 4-byte size prefix followed by the
        // payload the crc32 covers, so this breaks exactly that record's CRC while leaving
        // its neighbor intact. The db handle stays open: nothing was read through it yet, so
        // the next fetch hits the corrupted bytes on disk.
        let corrupt_at = (DATA_HEADER_BYTES + 4) as u64;
        let records_path = temp_dir.path().join(RECORDS_NAME);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&records_path)
            .expect("open records file");
        f.seek(SeekFrom::Start(corrupt_at)).expect("seek to payload");
        let mut byte = [0_u8; 1];
        f.read_exact(&mut byte).expect("read payload byte");
        f.seek(SeekFrom::Start(corrupt_at)).expect("seek back");
        f.write_all(&[!byte[0]]).expect("flip payload byte");
        f.sync_all().expect("sync corruption");
        drop(f);

        // The corrupt record must surface as Storage, and Storage must not be polled.
        let err =
            db.certified_record_by_epoch(0).await.expect_err("corrupt record must be refused");
        assert_eq!(err, CertifiedRecordError::Storage(0));
        assert!(!err.is_retryable(), "storage corruption cannot heal by re-reading");

        // The timeout variant must return the same error immediately, not after its budget.
        let start = tokio::time::Instant::now();
        let err = db
            .certified_record_by_epoch_with_timeout(0, std::time::Duration::from_secs(30))
            .await
            .expect_err("the timeout variant must also refuse the corrupt record");
        assert_eq!(err, CertifiedRecordError::Storage(0));
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "a storage error is terminal and must fail fast, not poll out the timeout"
        );

        // Positive control: the undamaged neighbor still resolves through the very same path,
        // so the Storage classification above is not a vacuous artifact of a broken db.
        let good = db.certified_record_by_epoch(1).await.expect("undamaged record must resolve");
        assert_eq!(good.digest(), rec1.digest());
    }

    #[tokio::test]
    async fn cert_by_digest_with_timeout_returns_saved_cert() {
        // A cert that is already stored is returned without hitting the deadline.
        let temp_dir = TempDir::with_prefix("cert_timeout_present").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (record, cert) = make_test_pair(0, &signers, EpochDigest::default());
        db.save(record.clone(), cert.clone()).await.expect("save record + cert");

        let got = db
            .cert_by_digest_with_timeout(record.digest(), std::time::Duration::from_secs(5))
            .await
            .expect("cert present within timeout");
        assert_eq!(got.epoch_hash, cert.epoch_hash);
    }

    #[tokio::test]
    async fn cert_by_digest_with_timeout_times_out_when_absent() {
        // With no cert stored for the digest, the poll loop returns None after the deadline rather
        // than hanging. A short timeout keeps the test fast.
        let temp_dir = TempDir::with_prefix("cert_timeout_absent").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        // Save the record only (no cert), so cert_by_digest never resolves.
        let (record, _cert) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(record.clone()).await.expect("save record");

        let start = std::time::Instant::now();
        let got = db
            .cert_by_digest_with_timeout(record.digest(), std::time::Duration::from_millis(300))
            .await;
        assert!(got.is_none(), "expected a timeout with no cert stored");
        // Poll interval is 200ms, so the loop must have waited at least one interval.
        assert!(start.elapsed() >= std::time::Duration::from_millis(200));
    }

    #[tokio::test]
    async fn export_bounded_bundle_writes_exactly_through_epoch() {
        // The bundle must hold records + certs for exactly 0..=N and round-trip through the bare
        // pack readers, even though the live db holds more epochs than N.
        let temp_dir = TempDir::with_prefix("export_bundle").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("export_bundle_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let mut records = Vec::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..6u32 {
            let (record, cert) = make_test_pair(epoch, &signers, parent);
            parent = record.digest();
            db.save(record.clone(), cert).await.expect("save record + cert");
            records.push(record);
        }

        // Export only through epoch 3 while the db holds through epoch 5.
        let records_path = bundle_dir.path().join("epoch_records");
        let certs_path = bundle_dir.path().join("epoch_certs");
        db.export_bounded_bundle(3, &records_path, &certs_path).await.expect("export bundle");

        let got_records =
            EpochRecordDb::read_records_from_pack(&records_path).expect("read records");
        assert_eq!(got_records.len(), 4, "bundle must hold exactly records 0..=3");
        for (got, want) in got_records.iter().zip(records.iter().take(4)) {
            assert_eq!(got.epoch, want.epoch);
            assert_eq!(got.digest(), want.digest());
        }

        let got_certs = EpochRecordDb::read_certs_from_pack(&certs_path).expect("read certs");
        assert_eq!(got_certs.len(), 4, "bundle must hold a cert for every record 0..=3");
        for (got, want) in got_certs.iter().zip(records.iter().take(4)) {
            assert_eq!(got.epoch_hash, want.digest());
        }
    }

    #[tokio::test]
    async fn export_bounded_bundle_errors_on_missing_required_cert() {
        // A record for epoch >= 1 with no cert makes the bundle unverifiable, so the export must
        // error rather than write a partial bundle.
        let temp_dir = TempDir::with_prefix("export_bundle_missing_cert").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("export_bundle_missing_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        let (rec0, cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save(rec0.clone(), cert0).await.expect("save epoch 0");
        // Epoch 1 saved without its cert (mirrors the tip epoch before aggregation).
        let (rec1, _cert1) = make_test_pair(1, &signers, rec0.digest());
        db.save_record(rec1).await.expect("save epoch 1 record");

        let err = db
            .export_bounded_bundle(
                1,
                &bundle_dir.path().join("epoch_records"),
                &bundle_dir.path().join("epoch_certs"),
            )
            .await
            .expect_err("export must fail when a required cert is missing");
        assert!(matches!(err, EpochDbError::MissingCertificate(1)), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn export_bounded_bundle_errors_on_missing_genesis_cert() {
        // Epoch 0 must carry a cert like every other epoch, so an exporter missing epoch 0's cert
        // must error rather than write a bundle the importer would reject.
        let temp_dir = TempDir::with_prefix("export_bundle_genesis").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("export_bundle_genesis_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        // Epoch 0 record only, no cert.
        let (rec0, _cert0) = make_test_pair(0, &signers, EpochDigest::default());
        db.save_record(rec0.clone()).await.expect("save epoch 0 record");
        let (rec1, cert1) = make_test_pair(1, &signers, rec0.digest());
        db.save(rec1, cert1).await.expect("save epoch 1");

        let err = db
            .export_bounded_bundle(
                1,
                &bundle_dir.path().join("epoch_records"),
                &bundle_dir.path().join("epoch_certs"),
            )
            .await
            .expect_err("export must fail when epoch 0's cert is missing");
        assert!(matches!(err, EpochDbError::MissingCertificate(0)), "unexpected error: {err}");
    }
}
