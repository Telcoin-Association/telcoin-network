//! Durable availability-vote reservations shared by all workers of one validator.
//!
//! TN's layered database permits overlapping transactions. One shared asynchronous lock
//! therefore covers the reservation check, insertion, and durability barrier. Callers must
//! clone the store created by their consensus configuration, rather than create one per worker.

use super::{
    BatchSlotAuthorization, BatchSlotError, BatchSlotId, BatchSlotVote, BatchSlotVoteKey,
    BatchSlots, SignedBatchSlotRecord,
};
use crate::{Database, DbTx, DbTxMut, Epoch, Table, TableHint};
use std::{fmt, sync::Arc};
use tokio::sync::{Mutex, OnceCell};

/// Epoch-scoped reservations, retaining one digest per slot, view, and vote namespace.
#[derive(Debug)]
pub struct BatchSlotVotes;

impl Table for BatchSlotVotes {
    type Key = BatchSlotVoteKey;
    type Value = BatchSlotVote;

    const NAME: &'static str = "batch-slot-votes";
    const HINT: TableHint = TableHint::Cache;
}

/// Canonical authorizations of closed slots, retained on disk through the epoch.
#[derive(Debug)]
pub struct BatchSlotAuthorizations;

impl Table for BatchSlotAuthorizations {
    type Key = BatchSlotId;
    type Value = BatchSlotAuthorization;

    const NAME: &'static str = "batch-slot-authorizations";
    const HINT: TableHint = TableHint::Cache;
}

/// Durable epoch marker guarding retirement of old slot metadata.
#[derive(Debug)]
pub struct BatchSlotStoreEpoch;

impl Table for BatchSlotStoreEpoch {
    type Key = ();
    type Value = Epoch;

    const NAME: &'static str = "batch-slot-store-epoch";
    const HINT: TableHint = TableHint::Cache;
}

/// Shared durable store for one validator's batch availability votes.
#[derive(Clone)]
pub struct BatchSlotVoteStore<DB> {
    /// Persistent consensus database, including its buffered-write durability barrier.
    database: DB,
    /// Epoch whose authorizations and votes this handle may use.
    epoch: Epoch,
    /// Serializes reservations across every worker and cache-validation path.
    gate: Arc<Mutex<()>>,
    /// Completes only after the epoch marker and any retirement are durable.
    initialized: Arc<OnceCell<()>>,
}

impl<DB> fmt::Debug for BatchSlotVoteStore<DB> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("BatchSlotVoteStore").finish_non_exhaustive()
    }
}

impl<DB: Database> BatchSlotVoteStore<DB> {
    /// Validate a fresh worker acknowledgement and persist its native reservation.
    ///
    /// Cache hits must use this same path before a new vote. No caller may hold a consensus
    /// database write transaction across the durability await.
    pub async fn validate_and_reserve(
        &self,
        control: &super::BatchSlotControl,
        validator: &(impl crate::BatchValidation + ?Sized),
        batch: crate::SealedBatch,
    ) -> Result<(), crate::BatchValidationError> {
        if let Some(record) = validator.validate_batch_for_vote(batch)? {
            self.initialize()
                .await
                .map_err(|error| crate::BatchValidationError::SlotAdmission(error.to_string()))?;
            let slots = control.snapshot().ok_or_else(|| {
                crate::BatchValidationError::SlotAdmission(
                    "native vote has no canonical slot snapshot".into(),
                )
            })?;
            let vote = self
                .authorize(&slots, &record)
                .map_err(|error| crate::BatchValidationError::SlotAdmission(error.to_string()))?;
            self.reserve(&vote)
                .await
                .map_err(|error| crate::BatchValidationError::SlotAdmission(error.to_string()))?;
            if let super::BatchSlotMessage::Timeout { position } = record.message() {
                control.observe_timeout(*position);
            }
        }
        Ok(())
    }

    /// Construct the shared handle without changing storage before protocol activation.
    pub fn new(database: DB, epoch: Epoch) -> Self {
        Self {
            database,
            epoch,
            gate: Arc::new(Mutex::new(())),
            initialized: Arc::new(OnceCell::new()),
        }
    }

    /// Open disk-backed tables and retire history only when the canonical epoch advances.
    ///
    /// The marker and retirement commit together. A same-epoch restart keeps every reservation;
    /// attempting to move the marker backward fails. Cache-class tables release buffered values
    /// after their physical write, keeping the epoch's growing history out of resident memory.
    pub async fn initialize(&self) -> Result<(), BatchSlotVoteStoreError> {
        self.initialized
            .get_or_try_init(|| async {
                let _guard = self.gate.lock().await;
                self.database
                    .open_table::<BatchSlotVotes>()
                    .and_then(|()| self.database.open_table::<BatchSlotAuthorizations>())
                    .and_then(|()| self.database.open_table::<BatchSlotStoreEpoch>())
                    .map_err(BatchSlotVoteStoreError::Database)?;
                let stored = self
                    .database
                    .get::<BatchSlotStoreEpoch>(&())
                    .map_err(BatchSlotVoteStoreError::Database)?;
                match () {
                    () if stored.is_some_and(|epoch| epoch > self.epoch) => {
                        Err(BatchSlotVoteStoreError::EpochRewind)
                    }
                    () if stored == Some(self.epoch) => self
                        .database
                        .persist::<BatchSlotStoreEpoch>()
                        .await
                        .map_err(BatchSlotVoteStoreError::Database),
                    () => {
                        let mut transaction =
                            self.database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
                        transaction
                            .clear_table::<BatchSlotVotes>()
                            .map_err(BatchSlotVoteStoreError::Database)?;
                        transaction
                            .clear_table::<BatchSlotAuthorizations>()
                            .map_err(BatchSlotVoteStoreError::Database)?;
                        transaction
                            .insert::<BatchSlotStoreEpoch>(&(), &self.epoch)
                            .map_err(BatchSlotVoteStoreError::Database)?;
                        transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
                        self.database
                            .persist::<BatchSlotStoreEpoch>()
                            .await
                            .map_err(BatchSlotVoteStoreError::Database)
                    }
                }
            })
            .await
            .map(|()| ())
    }

    /// Authenticate a current or delayed proposal against canonical slot history.
    ///
    /// Historical authorizations keep delayed honest headers certifiable after another proposal
    /// fills the slot. Their per-view reservations remain intact, so allowing late votes does
    /// not authorize conflicting proposals for a view that was already reserved.
    pub fn authorize(
        &self,
        slots: &BatchSlots,
        record: &SignedBatchSlotRecord,
    ) -> Result<BatchSlotVote, BatchSlotVoteStoreError> {
        match () {
            () if self.initialized.get().is_none() => Err(BatchSlotVoteStoreError::NotInitialized),
            () if slots.epoch() != self.epoch => {
                Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::WrongEpoch))
            }
            () => self
                .database
                .get::<BatchSlotAuthorizations>(&record.slot())
                .map_err(BatchSlotVoteStoreError::Database)?
                .map_or_else(
                    || slots.vote(record),
                    |authorization| slots.vote_for_authorization(record, &authorization),
                )
                .map_err(BatchSlotVoteStoreError::Protocol),
        }
    }

    /// Persist the authorizations of slots closed by a fully durable execution output.
    ///
    /// Callers capture these from the published state preceding the output, and publish its new
    /// slot snapshot only after this barrier succeeds. On restart, canonical replay reconstructs
    /// any history not yet persisted when the process stopped.
    pub async fn publish_authorizations(
        &self,
        authorizations: &[BatchSlotAuthorization],
    ) -> Result<(), BatchSlotVoteStoreError> {
        self.initialize().await?;
        let _guard = self.gate.lock().await;
        let mut transaction =
            self.database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
        authorizations.iter().try_for_each(|authorization| {
            if authorization.epoch() != self.epoch {
                Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::WrongEpoch))
            } else {
                let existing = transaction
                    .get::<BatchSlotAuthorizations>(&authorization.id())
                    .map_err(BatchSlotVoteStoreError::Database)?;
                if existing.as_ref().is_some_and(|previous| previous != authorization) {
                    Err(BatchSlotVoteStoreError::HistoryConflict(authorization.id()))
                } else {
                    transaction
                        .insert::<BatchSlotAuthorizations>(&authorization.id(), authorization)
                        .map_err(BatchSlotVoteStoreError::Database)
                }
            }
        })?;
        transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
        self.database
            .persist::<BatchSlotAuthorizations>()
            .await
            .map_err(BatchSlotVoteStoreError::Database)
    }

    /// Reserve an authenticated vote and wait until it is durable before returning success.
    ///
    /// The vote must have been obtained from canonical slot authorization after full body
    /// validation. A caller must not hold an open database write transaction across this await,
    /// because the durability barrier may need that transaction to finish.
    pub async fn reserve(&self, vote: &BatchSlotVote) -> Result<(), BatchSlotVoteStoreError> {
        self.initialize().await?;
        let _guard = self.gate.lock().await;
        if vote.epoch() != self.epoch {
            Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::WrongEpoch))
        } else {
            self.database
                .get::<BatchSlotVotes>(vote.key())
                .map_err(BatchSlotVoteStoreError::Database)?
                .map_or(Ok(()), |previous| previous.permits(vote))
                .map_err(BatchSlotVoteStoreError::Protocol)?;
            self.database
                .insert::<BatchSlotVotes>(vote.key(), vote)
                .map_err(BatchSlotVoteStoreError::Database)?;
            self.database
                .persist::<BatchSlotVotes>()
                .await
                .map_err(BatchSlotVoteStoreError::Database)
        }
    }
}

/// A reservation conflict or a database failure that forbids acknowledging the batch.
#[derive(Debug)]
pub enum BatchSlotVoteStoreError {
    /// The durable vote conflicts with this proposal or refers to a later slot.
    Protocol(BatchSlotError),
    /// Reading, storing, or durably committing the reservation failed.
    Database(eyre::Report),
    /// Canonical replay disagreed with an authorization already persisted for this slot.
    HistoryConflict(BatchSlotId),
    /// Slot authorization was requested before the epoch's storage initialization completed.
    NotInitialized,
    /// The execution state requested an epoch behind durable vote history.
    EpochRewind,
}

impl fmt::Display for BatchSlotVoteStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => write!(formatter, "batch-slot reservation rejected: {error}"),
            Self::Database(error) => {
                write!(formatter, "batch-slot reservation storage failed: {error}")
            }
            Self::HistoryConflict(slot) => {
                write!(formatter, "batch-slot authorization history conflicts for {slot:?}")
            }
            Self::NotInitialized => formatter.write_str("batch-slot storage is not initialized"),
            Self::EpochRewind => formatter.write_str("batch-slot storage cannot rewind its epoch"),
        }
    }
}

impl std::error::Error for BatchSlotVoteStoreError {}
