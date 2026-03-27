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
use tn_types::{BlsPublicKey, Epoch, EpochCertificate, EpochRecord, B256};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tracing::error;

use crate::archive::{
    digest_index::index::HdxIndex,
    error::{fetch::FetchError, open::OpenError},
    fxhasher::FxHasher,
    index::Index as _,
    pack::{Pack, DATA_HEADER_BYTES},
    position_index::index::PositionIndex,
};

enum EpochDbMessage {
    /// Save a "dummy" epoch 0 [`EpochRecord`] without a certificate.
    SaveDummy0Record(EpochRecord),
    /// Save an [`EpochRecord`] without a certificate.
    SaveRecord(EpochRecord),
    /// Save an [`EpochRecord`] and its corresponding [`EpochCertificate`].
    /// If the record is already stored, only the certificate is saved.
    Save(EpochRecord, EpochCertificate),
    /// Save an [`EpochCertificate`] keyed by its record digest.
    SaveCertificate(B256, EpochCertificate),
    /// Retrieve an [`EpochRecord`] by epoch number.
    RecordByEpoch(Epoch, oneshot::Sender<Option<EpochRecord>>),
    /// Retrieve an [`EpochRecord`] by its digest.
    RecordByDigest(B256, oneshot::Sender<Option<EpochRecord>>),
    /// Retrieve an [`EpochCertificate`] by its epoch_hash digest.
    CertByDigest(B256, oneshot::Sender<Option<EpochCertificate>>),
    /// True if the database contains a record for the given epoch number.
    ContainsEpoch(Epoch, oneshot::Sender<bool>),
    /// True if the database contains a record with the given digest.
    ContainsRecordDigest(B256, oneshot::Sender<bool>),
    /// Return the latest (highest epoch) [`EpochRecord`] stored, if any.
    LatestRecord(oneshot::Sender<Option<EpochRecord>>),
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

    /// Return any delayed error recorded by the background thread.
    /// Also clears the error.
    pub fn get_error(&self) -> Result<(), EpochDbError> {
        match self.error.send_replace(None) {
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
        digest: B256,
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
    pub async fn record_by_digest(&self, digest: B256) -> Option<EpochRecord> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::RecordByDigest(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
        }
    }

    /// Retrieve an [`EpochCertificate`] by its `epoch_hash` digest.
    pub async fn cert_by_digest(&self, digest: B256) -> Option<EpochCertificate> {
        let (tx, rx) = oneshot::channel();
        if self.tx.send(EpochDbMessage::CertByDigest(digest, tx)).await.is_ok() {
            rx.await.unwrap_or(None)
        } else {
            None
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
    pub async fn contains_record_digest(&self, digest: B256) -> bool {
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

    /// Retrieve the epoch record and certificate (if available) by record digest.
    pub async fn get_epoch_by_hash(
        &self,
        hash: B256,
    ) -> Option<(EpochRecord, Option<EpochCertificate>)> {
        let record = self.record_by_digest(hash).await?;
        let cert = self.cert_by_digest(record.digest()).await;
        Some((record, cert))
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
    epoch_idx: PositionIndex,
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
        epoch_idx: &mut PositionIndex,
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
        let _ = std::fs::create_dir_all(base_dir);
        let have_records = std::fs::exists(base_dir.join(Self::RECORDS_NAME)).unwrap_or_default();

        let mut records =
            Pack::<EpochRecord>::open(base_dir.join(Self::RECORDS_NAME), Self::PACK_EPOCH, false)?;
        let mut certs = Pack::<EpochCertificate>::open(
            base_dir.join(Self::CERTS_NAME),
            Self::CERT_PACK_EPOCH,
            false,
        )?;

        let mut epoch_idx = PositionIndex::open_pdx_file(
            base_dir.join(Self::EPOCH_POS_NAME),
            records.header(),
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
            .save(record_digest, record_pos)
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
        if self.cert_digests.load(record_digest).is_ok() {
            return Ok(());
        }

        let cert_pos = self.certs.append(&cert).map_err(|e| EpochDbError::Append(e.to_string()))?;
        self.cert_digests
            .save(record_digest, cert_pos)
            .map_err(|e| EpochDbError::IndexAppend(format!("cert digest: {e}")))?;
        self.cert_digests.set_data_file_length(self.certs.file_len());
        Ok(())
    }

    /// Save an [`EpochCertificate`] keyed by `digest`. Idempotent.
    fn save_certificate(
        &mut self,
        digest: B256,
        cert: EpochCertificate,
    ) -> Result<(), EpochDbError> {
        if self.cert_digests.load(digest).is_ok() {
            return Ok(());
        }
        let cert_pos = self.certs.append(&cert).map_err(|e| EpochDbError::Append(e.to_string()))?;
        self.cert_digests
            .save(digest, cert_pos)
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

    fn record_by_digest(&mut self, digest: B256) -> Option<EpochRecord> {
        let pos = self.record_digests.load(digest).ok()?;
        self.records.fetch(pos).ok()
    }

    fn cert_by_digest(&mut self, digest: B256) -> Option<EpochCertificate> {
        let pos = self.cert_digests.load(digest).ok()?;
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

    fn contains_record_digest(&mut self, digest: B256) -> bool {
        if let Ok(pos) = self.record_digests.load(digest) {
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
    SendFailed,
    ReceiveFailed,
    PersistError(String),
    CorruptDb,
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
            EpochDbError::SendFailed => write!(f, "Internal channel send failed"),
            EpochDbError::ReceiveFailed => write!(f, "Internal channel receive failed"),
            EpochDbError::PersistError(e) => write!(f, "Failed to persist: {e}"),
            EpochDbError::CorruptDb => write!(f, "Epoch records database is corrupt"),
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

#[cfg(test)]
mod test {
    use std::{
        fs::OpenOptions,
        io::{Seek as _, SeekFrom},
        sync::Arc,
    };

    use rand::{rngs::StdRng, SeedableRng as _};
    use roaring::RoaringBitmap;
    use tempfile::TempDir;
    use tn_types::{
        BlockNumHash, BlsAggregateSignature, BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner,
        Epoch, EpochCertificate, EpochRecord, Signer as _, B256,
    };

    use crate::epoch_records::{EpochRecordDb, RECORDS_NAME};

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
        parent_hash: B256,
    ) -> (EpochRecord, EpochCertificate) {
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        let record = EpochRecord {
            epoch,
            committee: committee.clone(),
            next_committee: committee,
            parent_hash,
            final_consensus: BlockNumHash::new((epoch as u64 + 1) * 10, B256::default()),
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
    async fn test_epoch_record_db() {
        let temp_dir = TempDir::with_prefix("test_epoch_record_db").expect("temp dir");
        let mut rng = StdRng::from_os_rng();
        let signers: Vec<TestSigner> = (0..4).map(|_| TestSigner::new(&mut rng)).collect();

        // Create and populate an initial database.
        let db = EpochRecordDb::open(temp_dir.path()).expect("open db");

        let num_records: u32 = 20;
        let mut pairs = Vec::new();
        let mut parent = B256::default();
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
}
