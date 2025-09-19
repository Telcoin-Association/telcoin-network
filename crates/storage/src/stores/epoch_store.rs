//! Trait and helpers for accessing Epoch data in the consensus DB.

use tn_types::{BlockHash, BlsPublicKey, Database, DbTxMut, Epoch, EpochCertificate, EpochRecord};

use crate::tables::{EpochCerts, EpochRecords, EpochRecordsIndex};

/// Helpers for Epoch DB access.
pub trait EpochStore {
    /// Retrieve the committee keys for epoch if available in the DB.
    fn get_committee_keys(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>>;
    /// Save epoch_rec to the DB.
    fn save_epoch_record(&self, epoch_rec: &EpochRecord);
    /// Save an epoch record with it's certificate.
    fn save_epoch_record_with_cert(&self, epoch_rec: &EpochRecord, cert: &EpochCertificate);
    /// Retrieve the consensus header by number.
    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)>;
    /// Retrieve the consensus header by hash
    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)>;
}

impl<DB: Database> EpochStore for DB {
    fn get_committee_keys(&self, epoch: Epoch) -> Option<Vec<BlsPublicKey>> {
        if let Ok(Some(epoch)) = self.get::<EpochRecords>(&epoch) {
            // Try the exact epoch first for freshest committee (only would matter in rare cases of
            // a validator being removed).
            Some(epoch.committee)
        } else if let Ok(Some(epoch)) = self.get::<EpochRecords>(&(epoch.saturating_sub(1))) {
            // If we don't have that epoch then try the previous and use it's next.
            // When using the latest epoch this could be triggered.
            Some(epoch.next_committee)
        } else {
            None
        }
    }

    fn save_epoch_record(&self, epoch_rec: &EpochRecord) {
        let epoch_hash = epoch_rec.digest();
        let epoch = epoch_rec.epoch;
        match self.write_txn() {
            Ok(mut tx) => {
                // Ignoring errors.  We won't get any unless the DB is broken and the app will be
                // dieing in short order.
                let _ = tx.insert::<EpochRecordsIndex>(&epoch_hash, &epoch);
                let _ = tx.insert::<EpochRecords>(&epoch, epoch_rec);
                let _ = tx.commit();
            }
            Err(e) => tracing::error!(target: "epoch-store", "error creating a write txn {e}"),
        }
    }

    fn save_epoch_record_with_cert(&self, epoch_rec: &EpochRecord, cert: &EpochCertificate) {
        let epoch_hash = epoch_rec.digest();
        let epoch = epoch_rec.epoch;
        match self.write_txn() {
            Ok(mut tx) => {
                // Ignoring errors.  We won't get any unless the DB is broken and the app will be
                // dieing in short order.
                let _ = tx.insert::<EpochRecordsIndex>(&epoch_hash, &epoch);
                let _ = tx.insert::<EpochRecords>(&epoch, epoch_rec);
                let _ = tx.insert::<EpochCerts>(&epoch_hash, cert);
                let _ = tx.commit();
            }
            Err(e) => tracing::error!(target: "epoch-store", "error creating a write txn {e}"),
        }
    }

    fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)> {
        let record = self.get::<EpochRecords>(&epoch).ok()??;
        let digest = record.digest();
        Some((record, self.get::<EpochCerts>(&digest).ok()??))
    }

    fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
        let epoch = self.get::<EpochRecordsIndex>(&hash).ok()??;
        let record = self.get::<EpochRecords>(&epoch).ok()??;
        let digest = record.digest();
        Some((record, self.get::<EpochCerts>(&digest).ok()??))
    }
}
