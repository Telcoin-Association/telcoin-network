//! Database for storing [`EpochRecord`] and [`EpochCertificate`] data.
//!
//! Two log files are maintained:
//! - A records file containing [`EpochRecord`] entries, indexed by epoch number (position index)
//!   and by digest (hash index).
//! - A certs file containing [`EpochCertificate`] entries, indexed by digest only.

use std::{
    error::Error,
    fmt::Display,
    hash::BuildHasherDefault,
    io,
    path::{Path, PathBuf},
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
use tracing::{debug, error};

use crate::archive::{
    data_file::create_dir_synced,
    digest_index::index::HdxIndex,
    error::{fetch::FetchError, open::OpenError},
    fxhasher::FxHasher,
    index::Index as _,
    pack::{Pack, PackCompression, DATA_HEADER_BYTES},
    position_index::index::PositionIndex,
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
    /// Retrieve an [`EpochRecord`] by its digest.
    RecordByDigest(EpochDigest, oneshot::Sender<Option<EpochRecord>>),
    /// Retrieve an [`EpochCertificate`] by its epoch_hash digest.
    CertByDigest(EpochDigest, oneshot::Sender<Option<EpochCertificate>>),
    /// True if the database contains a record for the given epoch number.
    ContainsEpoch(Epoch, oneshot::Sender<bool>),
    /// True if the database contains a record with the given digest.
    ContainsRecordDigest(EpochDigest, oneshot::Sender<bool>),
    /// Return the latest (highest epoch) [`EpochRecord`] stored, if any.
    LatestRecord(oneshot::Sender<Option<EpochRecord>>),
    /// Return the first epoch in `0..tip` whose record or certificate is not yet stored,
    /// resuming from (and advancing) the actor's contiguous-certified-prefix watermark.
    FirstMissingHistoricalCert(Epoch, oneshot::Sender<Option<Epoch>>),
    /// Flush all pending writes to disk.
    Persist(oneshot::Sender<Result<(), EpochDbError>>),
    Shutdown,
}

/// Handle to the epoch records database.
///
/// Operations are dispatched to a background thread that owns the file handles.
/// Errors from background writes are surfaced on the next call via [`get_error`].
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
            EpochDbMessage::RecordByDigest(digest, tx) => {
                let _ = tx.send(inner.record_by_digest(digest));
            }
            EpochDbMessage::CertByDigest(digest, tx) => {
                let _ = tx.send(inner.cert_by_digest(digest));
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
            EpochDbMessage::FirstMissingHistoricalCert(tip_epoch, tx) => {
                let _ = tx.send(inner.first_missing_historical_cert(tip_epoch));
            }
            EpochDbMessage::Persist(tx) => {
                let _ = tx.send(inner.persist());
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
    /// is not yet stored, or `None` if every one has a cert. Cheap: a single actor round-trip that
    /// resumes from the actor's contiguous-certified-prefix watermark, so per-boundary work is
    /// bounded by the epochs newly certified since the previous scan rather than by the chain's
    /// age. Run before the export's plain-state walk so it can skip early when a required
    /// historical cert is permanently missing (e.g. a network-wide failed-quorum epoch no peer can
    /// supply) instead of walking the whole state and then failing.
    ///
    /// `tip_epoch` itself is EXCLUDED: the exported tip's own cert is only aggregated at the next
    /// epoch's start, so it is normally still pending at export time and is waited for separately.
    pub async fn first_missing_historical_cert(&self, tip_epoch: Epoch) -> Option<Epoch> {
        let (tx, rx) = oneshot::channel();
        // On a dead or dying actor, report epoch 0 as unconfirmed (whenever any historical epoch
        // exists) so the caller skips the export rather than proceeding blind; the per-epoch
        // lookups this scan replaced degraded the same way.
        if self.tx.send(EpochDbMessage::FirstMissingHistoricalCert(tip_epoch, tx)).await.is_ok() {
            rx.await.unwrap_or_else(|_| (tip_epoch > 0).then_some(0))
        } else {
            (tip_epoch > 0).then_some(0)
        }
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

    /// Write the bounded export bundle covering `0..=through_epoch`, reusing the previous
    /// boundary's published bundle when one is supplied.
    ///
    /// The incremental path copies the previous bundle's records/certs packs to the destination
    /// paths, validates the copies against epoch `through_epoch`'s record (each pack must hold
    /// exactly `through_epoch` entries and end at epoch `through_epoch - 1`, anchored by digest),
    /// and appends only the new epoch's record and certificate. That removes the full rebuild's
    /// per-epoch actor round-trips, deserialization, and ZStd recompression; per-boundary work
    /// is still linear in the chain length (the copy itself, plus a validation walk of the
    /// copied packs' record-size chain that CRC-checks every copied entry), but as cheap
    /// sequential local file I/O off the DB actor's single thread. The produced bundle is the
    /// same self-contained `0..=through_epoch` bundle that round-trips through
    /// [`read_records_from_pack`](Self::read_records_from_pack) /
    /// [`read_certs_from_pack`](Self::read_certs_from_pack).
    ///
    /// Any failure on the incremental path falls back unconditionally to
    /// [`export_bounded_bundle`](Self::export_bounded_bundle), which remains the correctness
    /// baseline: the previous bundle can be legitimately absent (first exported epoch, a skipped
    /// or failed prior export, operator pruning) or fail validation. The previous bundle is only
    /// ever read, never appended to in place, so published bundles stay immutable and the
    /// caller's atomic tmp-then-rename publish contract is unchanged.
    pub async fn export_incremental_bundle(
        &self,
        through_epoch: Epoch,
        prev_bundle: Option<(PathBuf, PathBuf)>,
        records_path: &Path,
        certs_path: &Path,
    ) -> Result<(), EpochDbError> {
        // Surface any pending background write error before reading (without clearing it).
        self.peek_error()?;

        let incremental = self
            .try_append_previous_bundle(through_epoch, prev_bundle, records_path, certs_path)
            .await;
        if let Err(reason) = &incremental {
            debug!(
                target: "epoch-db", %reason, through_epoch,
                "incremental bundle append unavailable; rebuilding the full bundle"
            );
        }
        if incremental.is_ok() {
            incremental
        } else {
            self.export_bounded_bundle(through_epoch, records_path, certs_path).await
        }
    }

    /// Attempt the incremental copy + append against the previous bundle's packs.
    ///
    /// Every error (absent previous bundle, failed validation, IO) aborts before the destination
    /// holds a committed pack, so the caller can rebuild over the same paths. Fetches only epoch
    /// `through_epoch`'s record and certificate from the actor; the historical entries come from
    /// the copied files.
    async fn try_append_previous_bundle(
        &self,
        through_epoch: Epoch,
        prev_bundle: Option<(PathBuf, PathBuf)>,
        records_path: &Path,
        certs_path: &Path,
    ) -> Result<(), EpochDbError> {
        let (prev_records, prev_certs) = prev_bundle
            .filter(|_| through_epoch > 0)
            .ok_or_else(|| EpochDbError::BundleValidation("no previous bundle to extend".into()))?;

        // The only per-boundary actor traffic on the incremental path: the new epoch's own
        // record and certificate.
        let (record, cert) = self
            .get_epoch_by_number(through_epoch)
            .await
            .ok_or(EpochDbError::MissingRecord(through_epoch))?;
        let cert = cert.ok_or(EpochDbError::MissingCertificate(through_epoch))?;

        let records_dest = records_path.to_path_buf();
        let certs_dest = certs_path.to_path_buf();
        // Blocking file work (copy, validate, append, commit) off the async threads.
        tokio::task::spawn_blocking(move || {
            append_bundle_increment(
                &prev_records,
                &prev_certs,
                &records_dest,
                &certs_dest,
                &record,
                &cert,
            )
        })
        .await
        .map_err(|_| EpochDbError::JoinError)?
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
    /// Every epoch in `0..certified_watermark` has both a record and a stored certificate (a
    /// contiguous certified prefix, counted from absolute epoch 0 regardless of `start_epoch`).
    /// Never persisted: recomputed per process at open, because the heal step can truncate
    /// trailing records or certs after a crash. Advances only while the epoch at the watermark
    /// is certified, so a hole (a cert that arrives late via failed-quorum recovery or
    /// state-sync backfill) parks it until a later scan observes the backfill.
    certified_watermark: Epoch,
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

        let mut inner = Self {
            records,
            certs,
            epoch_idx,
            record_digests,
            cert_digests,
            start_epoch,
            dummy_epoch0: None,
            certified_watermark: 0,
        };
        inner.seed_certified_watermark();
        Ok(inner)
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

    fn record_by_epoch(&mut self, epoch: Epoch) -> Option<EpochRecord> {
        if epoch < self.start_epoch {
            return None;
        }
        if epoch == 0 && self.epoch_idx.is_empty() {
            self.dummy_epoch0.clone()
        } else {
            let pos = self.epoch_idx.load((epoch - self.start_epoch) as u64).ok()?;
            self.records.fetch(pos).ok()
        }
    }

    fn record_by_digest(&mut self, digest: EpochDigest) -> Option<EpochRecord> {
        let pos = self.record_digests.load(digest.into()).ok()?;
        self.records.fetch(pos).ok()
    }

    fn cert_by_digest(&mut self, digest: EpochDigest) -> Option<EpochCertificate> {
        let pos = self.cert_digests.load(digest.into()).ok()?;
        self.certs.fetch(pos).ok()
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

    /// True if `epoch` has both a stored record and a stored certificate for that record.
    ///
    /// The epoch-0 dummy record deliberately fails this check: it exists only to let the chain
    /// start and never has a certificate.
    fn epoch_certified(&mut self, epoch: Epoch) -> bool {
        self.record_by_epoch(epoch)
            .is_some_and(|record| self.cert_digests.load(record.digest().into()).is_ok())
    }

    /// Return the first epoch in `0..tip_epoch` without a stored record + certificate pair, or
    /// `None` when every one is certified, resuming from (and advancing) the
    /// contiguous-certified-prefix watermark.
    ///
    /// The watermark only advances while the epoch at the watermark is certified, never to a
    /// max-certified-epoch: certs for older epochs legitimately arrive after newer epochs are
    /// certified (failed-quorum recovery and state-sync backfill), so the scan waits at the hole
    /// and self-heals on the scan after the backfill lands. Epochs below the watermark are
    /// immutable within a process: record insertion is idempotent and gap-rejecting, certs are
    /// append-only, and no delete message exists.
    fn first_missing_historical_cert(&mut self, tip_epoch: Epoch) -> Option<Epoch> {
        let resume_from = self.certified_watermark;
        let first_missing = (resume_from..tip_epoch)
            .find(|epoch| !self.epoch_certified(*epoch))
            .unwrap_or(tip_epoch);
        self.certified_watermark = self.certified_watermark.max(first_missing);
        (first_missing < tip_epoch).then_some(first_missing)
    }

    /// Seed the certified-prefix watermark from on-disk state at open (after the heal step), so
    /// the process's first boundary scan resumes instead of rescanning from epoch 0. Bounded by
    /// the stored epochs: epochs at or beyond `start_epoch + len` have no record yet, and the
    /// scan stops at the first uncertified epoch anyway.
    fn seed_certified_watermark(&mut self) {
        let stored_end = self
            .start_epoch
            .saturating_add(Epoch::try_from(self.epoch_idx.len()).unwrap_or(Epoch::MAX));
        let _ = self.first_missing_historical_cert(stored_end);
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
    /// An export bundle failed validation on the incremental append path.
    BundleValidation(String),
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
            EpochDbError::BundleValidation(e) => {
                write!(f, "Export bundle validation failed: {e}")
            }
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
/// prepend prior-attempt records. Then opens the pack (which creates the file and writes its
/// header), appends every value in order, and commits so the file is complete on disk. Keeps the
/// sentinel tags and codec inside this crate so the export bundle stays readable by
/// `read_records_from_pack` / `read_certs_from_pack`.
fn write_bounded_pack<V>(path: &Path, uid_idx: u64, values: &[V]) -> Result<(), EpochDbError>
where
    V: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    // Enforce the "fresh" contract: a leftover file (e.g. from a prior failed export attempt) would
    // be appended to, not replaced. NotFound is the normal case and is ignored.
    let _ = std::fs::remove_file(path);
    let mut pack =
        Pack::<V>::open(path, uid_idx, false, PackCompression::ZStd, EPOCH_PACK_VERSION)?;
    for value in values {
        pack.append(value).map_err(|e| EpochDbError::Append(e.to_string()))?;
    }
    pack.commit().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
    Ok(())
}

/// Entry count and byte position of the final entry in `pack`, derived by walking the
/// record-size chain from the data header to the end of the file (bundle packs carry no sidecar
/// indexes, so the chain is the only structure available). Errors if the pack holds no entries
/// or the chain does not land exactly on the file length, so a truncated or torn pack can never
/// validate. Metadata-only: no entry is decompressed or deserialized.
fn pack_entry_chain<V>(pack: &mut Pack<V>) -> Result<(u64, u64), EpochDbError>
where
    V: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
{
    let file_len = pack.file_len();
    let start = u64::try_from(DATA_HEADER_BYTES).unwrap_or(u64::MAX);
    let positions = std::iter::successors((start < file_len).then_some(start), |&pos| {
        pack.record_size(pos)
            .ok()
            .map(|size| pos.saturating_add(u64::from(size)))
            .filter(|&next| next < file_len)
    });
    let (count, last) = positions.fold((0_u64, None), |(count, _), pos| (count + 1, Some(pos)));
    let last = last
        .ok_or_else(|| EpochDbError::BundleValidation("previous pack holds no entries".into()))?;
    let last_size = pack
        .record_size(last)
        .map_err(|e| EpochDbError::BundleValidation(format!("unreadable final entry: {e}")))?;
    (last.saturating_add(u64::from(last_size)) == file_len).then_some((count, last)).ok_or_else(
        || EpochDbError::BundleValidation("entry chain does not reach the file length".into()),
    )
}

/// Copy the previous bundle's pack at `prev` over `dest`, verify the copy holds exactly
/// `expected_entries` entries ending with an entry accepted by `last_entry_ok`, then append
/// `value` and commit.
///
/// Any error aborts before the append, leaving the caller to fall back to a full rebuild;
/// `dest` lives inside the export's temp dir, so a partial copy is discarded with it. `prev` is
/// opened only through `std::fs::copy`, never for writing.
fn append_to_copied_pack<V, F>(
    prev: &Path,
    dest: &Path,
    uid_idx: u64,
    expected_entries: u64,
    last_entry_ok: F,
    value: &V,
) -> Result<(), EpochDbError>
where
    V: std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned,
    F: FnOnce(&V) -> bool,
{
    // Enforce the "fresh" contract like `write_bounded_pack`: `std::fs::copy` truncates an
    // existing destination, but remove first so a failed copy cannot leave stale prior-attempt
    // bytes behind for a later step to append onto.
    let _ = std::fs::remove_file(dest);
    std::fs::copy(prev, dest)?;
    let mut pack =
        Pack::<V>::open(dest, uid_idx, false, PackCompression::ZStd, EPOCH_PACK_VERSION)?;
    let (entries, last_pos) = pack_entry_chain(&mut pack)?;
    let last: V = pack
        .fetch(last_pos)
        .map_err(|e| EpochDbError::BundleValidation(format!("unreadable final entry: {e}")))?;
    (entries == expected_entries && last_entry_ok(&last)).then_some(()).ok_or_else(|| {
        EpochDbError::BundleValidation(format!(
            "expected {expected_entries} entries ending at the previous epoch, found {entries}"
        ))
    })?;
    pack.append(value).map_err(|e| EpochDbError::Append(e.to_string()))?;
    pack.commit().map_err(|e| EpochDbError::PersistError(e.to_string()))?;
    Ok(())
}

/// Build the `0..=record.epoch` bundle at `records_dest` / `certs_dest` by copying the previous
/// boundary's published packs and appending only the new epoch's `record` and `cert`.
///
/// Validation anchors both copied packs to the new record before anything is appended: each pack
/// must hold exactly `record.epoch` entries (epochs `0..=record.epoch - 1`), the last copied
/// record must be the previous epoch's with the digest `record.parent_hash` names, and the last
/// copied certificate must certify that same digest. A previous bundle that is absent,
/// truncated, reordered, or from a different chain therefore fails closed and the caller
/// rebuilds the bundle in full.
fn append_bundle_increment(
    prev_records: &Path,
    prev_certs: &Path,
    records_dest: &Path,
    certs_dest: &Path,
    record: &EpochRecord,
    cert: &EpochCertificate,
) -> Result<(), EpochDbError> {
    let expected_entries = u64::from(record.epoch);
    let prev_epoch = record.epoch.saturating_sub(1);
    let parent = record.parent_hash;
    append_to_copied_pack(
        prev_records,
        records_dest,
        Inner::PACK_EPOCH,
        expected_entries,
        |last: &EpochRecord| last.epoch == prev_epoch && last.digest() == parent,
        record,
    )?;
    append_to_copied_pack(
        prev_certs,
        certs_dest,
        Inner::CERT_PACK_EPOCH,
        expected_entries,
        |last: &EpochCertificate| last.epoch_hash == parent,
        cert,
    )
}

#[cfg(test)]
mod test {
    use std::{
        collections::BTreeSet,
        fs::OpenOptions,
        io::{Seek as _, SeekFrom},
        path::Path,
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

    use crate::epoch_records::{
        epoch_committee_valid, EpochDbError, EpochRecordDb, EpochRecordValidation, CERTS_NAME,
        RECORDS_NAME,
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

    /// Build the four chained, fully-signed (record, cert) pairs for epochs 0..=3.
    fn make_chain4(signers: &[TestSigner]) -> [(EpochRecord, EpochCertificate); 4] {
        let (r0, c0) = make_test_pair(0, signers, EpochDigest::default());
        let (r1, c1) = make_test_pair(1, signers, r0.digest());
        let (r2, c2) = make_test_pair(2, signers, r1.digest());
        let (r3, c3) = make_test_pair(3, signers, r2.digest());
        [(r0, c0), (r1, c1), (r2, c2), (r3, c3)]
    }

    /// A clone of `record` tagged with a sentinel consensus number the live database never
    /// stores, so bundle bytes that came from a crafted "previous bundle" are distinguishable
    /// from bytes rebuilt out of the live database.
    fn sentinel_copy(record: &EpochRecord, number: u64) -> EpochRecord {
        EpochRecord {
            final_consensus: ConsensusNumHash::new(number, ConsensusHeaderDigest::default()),
            ..record.clone()
        }
    }

    #[test]
    fn certified_watermark_resumes_and_waits_at_hole() {
        // Issue #1078 fix 1: the scan resumes from the contiguous-certified-prefix watermark
        // instead of rescanning from epoch 0, and the watermark never advances past a hole, so a
        // late-arriving cert (failed-quorum recovery / state-sync backfill) is still requested.
        let dir = TempDir::with_prefix("watermark_hole").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let mut inner = super::Inner::open_append(dir.path(), 0).expect("open inner");

        let [(r0, c0), (r1, c1), (r2, c2), _] = make_chain4(&signers);
        let r1_digest = r1.digest();
        inner.save(r0, c0).expect("save 0");
        inner.save_record(r1).expect("save 1 record only"); // cert arrives later
        inner.save(r2, c2).expect("save 2");

        // First scan: epoch 1 is the hole; the watermark parks there, NOT at the max certified
        // epoch (2), so the backfill for epoch 1 keeps being requested.
        assert_eq!(inner.first_missing_historical_cert(3), Some(1));
        assert_eq!(inner.certified_watermark, 1, "watermark must wait at the hole");

        // Backfill epoch 1's cert (the failed-quorum recovery / state-sync path) and rescan:
        // the watermark self-heals past the hole.
        inner.save_certificate(r1_digest, c1).expect("backfill cert 1");
        assert_eq!(inner.first_missing_historical_cert(3), None);
        assert_eq!(inner.certified_watermark, 3, "watermark must pass the backfilled hole");

        // A smaller tip neither regresses the watermark nor reports a phantom hole.
        assert_eq!(inner.first_missing_historical_cert(1), None);
        assert_eq!(inner.certified_watermark, 3);
    }

    #[test]
    fn certified_watermark_seeded_at_open() {
        // Issue #1078 fix 1: the watermark is recomputed per process (never persisted, because
        // the open-time heal can truncate trailing records or certs). Reopening seeds it from
        // the on-disk records and certs, so the first boundary scan after a restart resumes
        // instead of walking from epoch 0.
        let dir = TempDir::with_prefix("watermark_seed").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        {
            let mut inner = super::Inner::open_append(dir.path(), 0).expect("open inner");
            let [(r0, c0), (r1, c1), (r2, _), _] = make_chain4(&signers);
            inner.save(r0, c0).expect("save 0");
            inner.save(r1, c1).expect("save 1");
            inner.save_record(r2).expect("save 2 record only");
            inner.persist().expect("persist");
        }
        let reopened = super::Inner::open_append(dir.path(), 0).expect("reopen inner");
        assert_eq!(
            reopened.certified_watermark, 2,
            "seed must stop at the first uncertified epoch"
        );
    }

    #[tokio::test]
    async fn export_incremental_bundle_appends_to_previous_bundle() {
        // Issue #1078 fix 2: the incremental path must actually REUSE the previous bundle's
        // bytes (copy + append), not silently rebuild from the live database. The crafted
        // previous bundle's historical records carry sentinel consensus numbers the live
        // database does not have, so sentinels surviving into the new bundle prove the
        // copy-and-append path ran.
        let temp_dir = TempDir::with_prefix("incr_append_db").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("incr_append_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let [(r0, c0), (r1, c1), (r2, c2), (r3, c3)] = make_chain4(&signers);

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        db.save(r0.clone(), c0.clone()).await.expect("save 0");
        db.save(r1.clone(), c1.clone()).await.expect("save 1");
        db.save(r2.clone(), c2.clone()).await.expect("save 2");
        db.save(r3.clone(), c3.clone()).await.expect("save 3");

        // Craft the "previous" 0..=2 bundle: sentinel copies for epochs 0..=1, but the REAL
        // record 2 (the validation anchor: its digest is record 3's parent_hash).
        let sentinel0 = sentinel_copy(&r0, 9_990);
        let sentinel1 = sentinel_copy(&r1, 9_991);
        let prev_records = bundle_dir.path().join("prev_records");
        let prev_certs = bundle_dir.path().join("prev_certs");
        super::write_bounded_pack(
            &prev_records,
            super::Inner::PACK_EPOCH,
            &[sentinel0.clone(), sentinel1.clone(), r2.clone()],
        )
        .expect("write prev records");
        super::write_bounded_pack(
            &prev_certs,
            super::Inner::CERT_PACK_EPOCH,
            &[c0.clone(), c1.clone(), c2.clone()],
        )
        .expect("write prev certs");

        let out_records = bundle_dir.path().join("epoch_records");
        let out_certs = bundle_dir.path().join("epoch_certs");
        db.export_incremental_bundle(3, Some((prev_records, prev_certs)), &out_records, &out_certs)
            .await
            .expect("incremental export");

        let got_records =
            EpochRecordDb::read_records_from_pack(&out_records).expect("read records");
        let [g0, g1, g2, g3]: [EpochRecord; 4] =
            got_records.try_into().expect("exactly four records");
        // Sentinels survived: the bundle was extended from the previous bundle's bytes.
        assert_eq!(g0.digest(), sentinel0.digest(), "epoch 0 must come from the copied bundle");
        assert_eq!(g1.digest(), sentinel1.digest(), "epoch 1 must come from the copied bundle");
        assert_eq!(g2.digest(), r2.digest());
        assert_eq!(g3.digest(), r3.digest(), "epoch 3 must be the appended new record");

        let got_certs = EpochRecordDb::read_certs_from_pack(&out_certs).expect("read certs");
        let [gc0, gc1, gc2, gc3]: [EpochCertificate; 4] =
            got_certs.try_into().expect("exactly four certs");
        assert_eq!(gc0.epoch_hash, c0.epoch_hash);
        assert_eq!(gc1.epoch_hash, c1.epoch_hash);
        assert_eq!(gc2.epoch_hash, r2.digest());
        assert_eq!(gc3.epoch_hash, r3.digest(), "epoch 3's cert must be the appended one");
    }

    #[tokio::test]
    async fn export_incremental_bundle_falls_back_without_previous_bundle() {
        // No previous bundle (first exported epoch, a skipped prior export, operator pruning):
        // the export must transparently rebuild the full 0..=N bundle from the live database,
        // whether the caller passes None or paths that do not exist.
        let temp_dir = TempDir::with_prefix("incr_fallback_db").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("incr_fallback_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let [(r0, c0), (r1, c1), (r2, c2), _] = make_chain4(&signers);

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        db.save(r0.clone(), c0).await.expect("save 0");
        db.save(r1.clone(), c1).await.expect("save 1");
        db.save(r2.clone(), c2).await.expect("save 2");

        let assert_full = |records_path: std::path::PathBuf, want: Vec<EpochDigest>| {
            let got = EpochRecordDb::read_records_from_pack(&records_path)
                .expect("read records")
                .iter()
                .map(|record| record.digest())
                .collect::<Vec<_>>();
            assert_eq!(got, want, "bundle must hold the full live-database record chain");
        };

        let none_records = bundle_dir.path().join("none_records");
        db.export_incremental_bundle(2, None, &none_records, &bundle_dir.path().join("none_certs"))
            .await
            .expect("export without previous bundle");
        assert_full(none_records, vec![r0.digest(), r1.digest(), r2.digest()]);

        let missing_records = bundle_dir.path().join("missing_records");
        db.export_incremental_bundle(
            2,
            Some((
                bundle_dir.path().join("no_such_records"),
                bundle_dir.path().join("no_such_certs"),
            )),
            &missing_records,
            &bundle_dir.path().join("missing_certs"),
        )
        .await
        .expect("export with absent previous bundle");
        assert_full(missing_records, vec![r0.digest(), r1.digest(), r2.digest()]);
    }

    #[tokio::test]
    async fn export_incremental_bundle_rejects_stale_or_padded_previous() {
        // A previous bundle that fails validation must be rejected in favor of the full rebuild,
        // and the rejected copy's sentinel bytes must never leak into the produced bundle. Three
        // rows, one per validation clause: stale (ends at N-2), padded (right final record,
        // wrong entry count), and a certs pack whose final cert does not certify epoch N-1.
        let temp_dir = TempDir::with_prefix("incr_reject_db").expect("temp dir");
        let bundle_dir = TempDir::with_prefix("incr_reject_out").expect("bundle dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();
        let [(r0, c0), (r1, c1), (r2, c2), (r3, c3)] = make_chain4(&signers);

        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");
        db.save(r0.clone(), c0.clone()).await.expect("save 0");
        db.save(r1.clone(), c1.clone()).await.expect("save 1");
        db.save(r2.clone(), c2.clone()).await.expect("save 2");
        db.save(r3.clone(), c3.clone()).await.expect("save 3");

        let sentinel0 = sentinel_copy(&r0, 9_990);
        let sentinel1 = sentinel_copy(&r1, 9_991);
        let real_chain = vec![r0.digest(), r1.digest(), r2.digest(), r3.digest()];
        let run_row = |name: &str,
                       prev_recs: Vec<EpochRecord>,
                       prev_certs_v: Vec<EpochCertificate>| {
            let prev_records = bundle_dir.path().join(format!("{name}_prev_records"));
            let prev_certs = bundle_dir.path().join(format!("{name}_prev_certs"));
            super::write_bounded_pack(&prev_records, super::Inner::PACK_EPOCH, &prev_recs)
                .expect("write prev records");
            super::write_bounded_pack(&prev_certs, super::Inner::CERT_PACK_EPOCH, &prev_certs_v)
                .expect("write prev certs");
            (prev_records, prev_certs)
        };

        // Row 1, stale: previous bundle ends at epoch 1 (N-2); the final-record check rejects it.
        let (stale_records, stale_certs) =
            run_row("stale", vec![sentinel0.clone(), r1.clone()], vec![c0.clone(), c1.clone()]);
        // Row 2, padded: four entries ending with the REAL record 2, so the final-record check
        // passes and only the entry-count check can reject it.
        let (padded_records, padded_certs) = run_row(
            "padded",
            vec![sentinel0.clone(), sentinel0.clone(), sentinel1.clone(), r2.clone()],
            vec![c0.clone(), c0.clone(), c1.clone(), c2.clone()],
        );
        // Row 3, bad certs: records pack valid, but the final cert certifies epoch 1, not 2.
        let (badcert_records, badcert_certs) = run_row(
            "badcert",
            vec![sentinel0.clone(), sentinel1.clone(), r2.clone()],
            vec![c0.clone(), c1.clone(), c1.clone()],
        );
        // Row 4, wrong final record with the RIGHT entry count and a VALID certs pack, so only
        // the final-record epoch/digest check can reject it.
        let (wronglast_records, wronglast_certs) = run_row(
            "wronglast",
            vec![sentinel0.clone(), sentinel1.clone(), r1.clone()],
            vec![c0.clone(), c1.clone(), c2.clone()],
        );

        /// Export with the given crafted previous bundle and assert the produced bundle is the
        /// real live-database chain (the fallback ran and no sentinel bytes leaked).
        async fn assert_falls_back(
            db: &EpochRecordDb,
            prev: (std::path::PathBuf, std::path::PathBuf),
            out_dir: &Path,
            real_chain: &[EpochDigest],
            name: &str,
        ) {
            let out_records = out_dir.join(format!("{name}_records"));
            let out_certs = out_dir.join(format!("{name}_certs"));
            db.export_incremental_bundle(3, Some(prev), &out_records, &out_certs)
                .await
                .expect("export must fall back, not error");
            let got = EpochRecordDb::read_records_from_pack(&out_records)
                .expect("read records")
                .iter()
                .map(|record| record.digest())
                .collect::<Vec<_>>();
            assert_eq!(got, real_chain, "{name}: fallback must rebuild the real chain");
            let got_certs = EpochRecordDb::read_certs_from_pack(&out_certs)
                .expect("read certs")
                .iter()
                .map(|cert| cert.epoch_hash)
                .collect::<Vec<_>>();
            assert_eq!(got_certs, real_chain, "{name}: fallback must rebuild the real cert chain");
        }

        let out_dir = bundle_dir.path();
        assert_falls_back(&db, (stale_records, stale_certs), out_dir, &real_chain, "stale").await;
        assert_falls_back(&db, (padded_records, padded_certs), out_dir, &real_chain, "padded")
            .await;
        assert_falls_back(&db, (badcert_records, badcert_certs), out_dir, &real_chain, "badcert")
            .await;
        assert_falls_back(
            &db,
            (wronglast_records, wronglast_certs),
            out_dir,
            &real_chain,
            "wronglast",
        )
        .await;
    }
}
