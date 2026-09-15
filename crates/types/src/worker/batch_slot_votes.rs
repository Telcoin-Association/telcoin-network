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
struct BatchSlotVotes;

impl Table for BatchSlotVotes {
    type Key = BatchSlotVoteKey;
    type Value = BatchSlotVote;

    const NAME: &'static str = "batch-slot-votes";
    const HINT: TableHint = TableHint::Cache;
}

/// Canonical authorizations of closed slots, retained on disk through the epoch.
#[derive(Debug)]
struct BatchSlotAuthorizations;

impl Table for BatchSlotAuthorizations {
    type Key = BatchSlotId;
    type Value = BatchSlotAuthorization;

    const NAME: &'static str = "batch-slot-authorizations";
    const HINT: TableHint = TableHint::Cache;
}

/// Durable epoch marker guarding retirement of old slot metadata.
#[derive(Debug)]
struct BatchSlotStoreEpoch;

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

#[cfg(test)]
mod tests {
    //! Reservations must survive reopening and cannot escape a pending durability barrier.

    use super::*;
    use crate::{
        Address, Batch, BatchSlotParent, BatchSlots, BlsKeypair, CommitteeBuilder, DbTxMut, B256,
    };
    use rand::{rngs::StdRng, SeedableRng};
    use tn_storage::{layered_db::LayeredDatabase, mem_db::MemDatabase, redb::database::ReDB};
    use tokio::sync::oneshot;

    /// Two authenticated, conflicting proposals for one producer's initial slot.
    fn conflicting_records(
    ) -> Result<(BatchSlots, SignedBatchSlotRecord, SignedBatchSlotRecord), BatchSlotVoteStoreError>
    {
        let key = BlsKeypair::generate(&mut StdRng::seed_from_u64(1377));
        let mut committee = CommitteeBuilder::new(7);
        committee.add_authority(*key.public(), Address::ZERO);
        let slots = BatchSlots::new(
            2017.into(),
            committee.build(),
            BatchSlotParent::new(B256::ZERO, B256::ZERO),
        )
        .map_err(BatchSlotVoteStoreError::Protocol)?;
        let bucket = slots.bucket(Address::ZERO);
        let proposal = |byte| {
            slots
                .sign_proposal(
                    bucket,
                    *key.public(),
                    Batch {
                        transactions: vec![vec![byte]],
                        epoch: slots.epoch(),
                        beneficiary: Address::ZERO,
                        base_fee_per_gas: 1,
                        worker_id: 0,
                        received_at: None,
                    },
                    &key,
                )
                .map_err(BatchSlotVoteStoreError::Protocol)
        };
        let first = proposal(1)?;
        proposal(2).map(|second| (slots, first, second))
    }

    /// Authenticate both records against the same canonical slot.
    fn conflicting_votes() -> Result<(BatchSlotVote, BatchSlotVote), BatchSlotVoteStoreError> {
        let (slots, first, second) = conflicting_records()?;
        let first = slots.vote(&first).map_err(BatchSlotVoteStoreError::Protocol)?;
        slots.vote(&second).map_err(BatchSlotVoteStoreError::Protocol).map(|second| (first, second))
    }

    #[tokio::test]
    async fn reopening_preserves_the_first_vote() -> Result<(), BatchSlotVoteStoreError> {
        let directory =
            tempfile::tempdir().map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        let path = directory.path().join("votes.redb");
        let (first, conflicting) = conflicting_votes()?;
        let database = ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?;
        let store = BatchSlotVoteStore::new(database, 7);
        store.reserve(&first).await?;
        drop(store);
        let reopened = BatchSlotVoteStore::new(
            ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?,
            7,
        );
        reopened.reserve(&first).await?;
        assert!(matches!(
            reopened.reserve(&conflicting).await,
            Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::ConflictingVote))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn workers_wait_for_the_shared_durability_barrier() -> Result<(), BatchSlotVoteStoreError>
    {
        let database = LayeredDatabase::open(MemDatabase::new(), false);
        let store = BatchSlotVoteStore::new(database.clone(), 7);
        store.initialize().await?;
        let (first, conflicting) = conflicting_votes()?;
        // The background writer absorbs bare inserts into this transaction. Its commit must
        // happen before either worker can finish its reservation.
        let transaction = database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
        let first_key = first.key().clone();
        let first_worker = store.clone();
        let (first_started, first_ready) = oneshot::channel();
        let first_task = tokio::spawn(async move {
            let _ = first_started.send(());
            first_worker.reserve(&first).await
        });
        first_ready.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        assert!(database
            .contains_key::<BatchSlotVotes>(&first_key)
            .map_err(BatchSlotVoteStoreError::Database)?);
        assert!(!first_task.is_finished(), "a vote escaped before its database commit");

        let second_worker = store.clone();
        let (second_started, second_ready) = oneshot::channel();
        let second_task = tokio::spawn(async move {
            let _ = second_started.send(());
            second_worker.reserve(&conflicting).await
        });
        second_ready.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        assert!(!second_task.is_finished(), "workers did not share the reservation lock");

        transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
        first_task.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))??;
        let second_result =
            second_task.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        assert!(matches!(
            second_result,
            Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::ConflictingVote))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reopening_recovers_closed_slot_authorizations() -> Result<(), BatchSlotVoteStoreError>
    {
        let directory =
            tempfile::tempdir().map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        let path = directory.path().join("history.redb");
        let (mut slots, original, conflicting) = conflicting_records()?;
        let bucket = slots.bucket(Address::ZERO);
        let authorization =
            slots.authorization(bucket).map_err(BatchSlotVoteStoreError::Protocol)?;
        let store = BatchSlotVoteStore::new(
            ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?,
            7,
        );
        store.initialize().await?;
        store.reserve(&store.authorize(&slots, &original)?).await?;
        slots.apply(&original, B256::repeat_byte(1)).map_err(BatchSlotVoteStoreError::Protocol)?;
        slots
            .finalize_openings(B256::repeat_byte(1), B256::repeat_byte(2))
            .map_err(BatchSlotVoteStoreError::Protocol)?;
        store.publish_authorizations(&[authorization]).await?;
        drop(store);
        let reopened = BatchSlotVoteStore::new(
            ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?,
            7,
        );
        reopened.initialize().await?;
        let late = reopened.authorize(&slots, &original);
        assert!(late.is_ok(), "closed-slot authorization was not recovered from disk");
        reopened.reserve(&late?).await?;
        assert!(matches!(
            reopened.reserve(&reopened.authorize(&slots, &conflicting)?).await,
            Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::ConflictingVote))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn history_publication_waits_for_durable_commit() -> Result<(), BatchSlotVoteStoreError> {
        let database = LayeredDatabase::open(MemDatabase::new(), false);
        let store = BatchSlotVoteStore::new(database.clone(), 7);
        store.initialize().await?;
        let (slots, original, _) = conflicting_records()?;
        let authorization = slots
            .authorization(slots.bucket(Address::ZERO))
            .map_err(BatchSlotVoteStoreError::Protocol)?;
        let transaction = database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
        let (started, ready) = oneshot::channel();
        let writer = tokio::spawn(async move {
            let _ = started.send(());
            store.publish_authorizations(&[authorization]).await
        });
        ready.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        assert!(!writer.is_finished(), "slot history escaped before the durable commit");
        transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
        writer.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))??;
        assert!(database
            .contains_key::<BatchSlotAuthorizations>(&original.slot())
            .map_err(BatchSlotVoteStoreError::Database)?);
        Ok(())
    }

    #[tokio::test]
    async fn epoch_advance_retires_history_and_refuses_rewind(
    ) -> Result<(), BatchSlotVoteStoreError> {
        let database = MemDatabase::new();
        let store = BatchSlotVoteStore::new(database.clone(), 7);
        let (first, _) = conflicting_votes()?;
        store.reserve(&first).await?;
        let next_epoch = BatchSlotVoteStore::new(database.clone(), 8);
        next_epoch.initialize().await?;
        assert!(!database
            .contains_key::<BatchSlotVotes>(first.key())
            .map_err(BatchSlotVoteStoreError::Database)?);
        assert!(matches!(
            next_epoch.reserve(&first).await,
            Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::WrongEpoch))
        ));
        let rewind = BatchSlotVoteStore::new(database, 7);
        assert!(matches!(rewind.initialize().await, Err(BatchSlotVoteStoreError::EpochRewind)));
        Ok(())
    }

    #[tokio::test]
    async fn epoch_initialization_waits_for_its_durable_marker(
    ) -> Result<(), BatchSlotVoteStoreError> {
        let database = LayeredDatabase::open(MemDatabase::new(), false);
        let store = BatchSlotVoteStore::new(database.clone(), 7);
        let transaction = database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
        let (started, ready) = oneshot::channel();
        let writer = tokio::spawn(async move {
            let _ = started.send(());
            store.initialize().await
        });
        ready.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
        assert!(!writer.is_finished(), "epoch initialization escaped before its durable marker");
        transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
        writer.await.map_err(|error| BatchSlotVoteStoreError::Database(error.into()))??;
        assert_eq!(
            database.get::<BatchSlotStoreEpoch>(&()).map_err(BatchSlotVoteStoreError::Database)?,
            Some(7)
        );
        Ok(())
    }
}
