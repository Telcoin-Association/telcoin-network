//! Reservations must survive reopening and cannot escape a pending durability barrier.

use rand::{rngs::StdRng, SeedableRng};
use tn_storage::{layered_db::LayeredDatabase, mem_db::MemDatabase, redb::database::ReDB};
use tn_types::{
    Address, Batch, BatchSlotAuthorizations, BatchSlotError, BatchSlotParent, BatchSlotStoreEpoch,
    BatchSlotVote, BatchSlotVoteStore, BatchSlotVoteStoreError, BatchSlotVotes, BatchSlots,
    BlsKeypair, CommitteeBuilder, Database, DbTxMut, SignedBatchSlotRecord, B256,
};
use tokio::sync::oneshot;

#[test]
fn canonical_envelopes_reject_unsigned_metadata() -> Result<(), BatchSlotVoteStoreError> {
    let (_, record, _) = conflicting_records()?;
    let envelope = record.envelope().map_err(BatchSlotVoteStoreError::Protocol)?;
    let decoded = SignedBatchSlotRecord::from_envelope(&envelope)
        .map_err(BatchSlotVoteStoreError::Protocol)?;
    assert_eq!(
        record.digest().map_err(BatchSlotVoteStoreError::Protocol)?,
        decoded.digest().map_err(BatchSlotVoteStoreError::Protocol)?
    );
    let mut extra = envelope.clone();
    extra.transactions.push(vec![1]);
    assert!(matches!(
        SignedBatchSlotRecord::from_envelope(&extra),
        Err(BatchSlotError::InvalidEnvelope)
    ));
    let mut unsigned = envelope;
    unsigned.worker_id = 1;
    assert!(matches!(
        SignedBatchSlotRecord::from_envelope(&unsigned),
        Err(BatchSlotError::InvalidEnvelope)
    ));
    Ok(())
}

#[test]
fn publication_rejects_an_unfinalized_execution_anchor() -> Result<(), BatchSlotVoteStoreError> {
    let (slots, record, _) = conflicting_records()?;
    let control = tn_types::BatchSlotControl::default();
    drop(control.install(slots, Some(*record.authority())));
    let mut output =
        control.prepare(B256::repeat_byte(9)).ok_or(BatchSlotVoteStoreError::NotInitialized)?;
    output.apply(&record).map_err(BatchSlotVoteStoreError::Protocol)?;
    assert!(matches!(
        control.commit_blocking(output),
        Err(tn_types::BatchSlotControlError::Protocol(BatchSlotError::OpeningNotFinalized))
    ));
    Ok(())
}

#[tokio::test]
async fn execution_publication_waits_for_durable_history() -> Result<(), BatchSlotVoteStoreError> {
    let database = LayeredDatabase::open(MemDatabase::new(), false);
    let store = BatchSlotVoteStore::new(database.clone(), 7);
    store.initialize().await?;
    let (slots, record, _) = conflicting_records()?;
    let bucket = slots.bucket(Address::ZERO);
    let control = tn_types::BatchSlotControl::default();
    let receiver = control.install(slots, Some(*record.authority()));
    let serving = control.clone();
    let server = tokio::spawn(async move { serving.serve(store, receiver).await });
    let mut output =
        control.prepare(B256::repeat_byte(9)).ok_or(BatchSlotVoteStoreError::NotInitialized)?;
    output.apply(&record).map_err(BatchSlotVoteStoreError::Protocol)?;
    output.finalize(B256::repeat_byte(10)).map_err(BatchSlotVoteStoreError::Protocol)?;
    let transaction = database.write_txn().map_err(BatchSlotVoteStoreError::Database)?;
    let publishing = control.clone();
    let publication = tokio::task::spawn_blocking(move || publishing.commit_blocking(output));
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        std::future::poll_fn(|context| {
            database.contains_key::<BatchSlotAuthorizations>(&record.slot()).map_or_else(
                |error| std::task::Poll::Ready(Err(error)),
                |present| {
                    if present {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                },
            )
        }),
    )
    .await
    .map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?
    .map_err(BatchSlotVoteStoreError::Database)?;
    assert!(!publication.is_finished(), "execution published before durable history");
    assert_eq!(
        control
            .snapshot()
            .ok_or(BatchSlotVoteStoreError::NotInitialized)?
            .position(bucket)
            .map_err(BatchSlotVoteStoreError::Protocol)?
            .sequence()
            .value(),
        0
    );
    transaction.commit().map_err(BatchSlotVoteStoreError::Database)?;
    publication
        .await
        .map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?
        .map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
    assert_eq!(
        control
            .snapshot()
            .ok_or(BatchSlotVoteStoreError::NotInitialized)?
            .position(bucket)
            .map_err(BatchSlotVoteStoreError::Protocol)?
            .sequence()
            .value(),
        1
    );
    server.abort();
    Ok(())
}

/// Two authenticated, conflicting proposals for one producer's initial slot.
fn conflicting_records(
) -> Result<(BatchSlots, SignedBatchSlotRecord, SignedBatchSlotRecord), BatchSlotVoteStoreError> {
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
    let reopened =
        BatchSlotVoteStore::new(ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?, 7);
    reopened.reserve(&first).await?;
    assert!(matches!(
        reopened.reserve(&conflicting).await,
        Err(BatchSlotVoteStoreError::Protocol(BatchSlotError::ConflictingVote))
    ));
    Ok(())
}

#[tokio::test]
async fn workers_wait_for_the_shared_durability_barrier() -> Result<(), BatchSlotVoteStoreError> {
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
async fn reopening_recovers_closed_slot_authorizations() -> Result<(), BatchSlotVoteStoreError> {
    let directory =
        tempfile::tempdir().map_err(|error| BatchSlotVoteStoreError::Database(error.into()))?;
    let path = directory.path().join("history.redb");
    let (mut slots, original, conflicting) = conflicting_records()?;
    let bucket = slots.bucket(Address::ZERO);
    let authorization = slots.authorization(bucket).map_err(BatchSlotVoteStoreError::Protocol)?;
    let store =
        BatchSlotVoteStore::new(ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?, 7);
    store.initialize().await?;
    store.reserve(&store.authorize(&slots, &original)?).await?;
    slots.apply(&original, B256::repeat_byte(1)).map_err(BatchSlotVoteStoreError::Protocol)?;
    slots
        .finalize_openings(B256::repeat_byte(1), B256::repeat_byte(2))
        .map_err(BatchSlotVoteStoreError::Protocol)?;
    store.publish_authorizations(&[authorization]).await?;
    drop(store);
    let reopened =
        BatchSlotVoteStore::new(ReDB::open(&path).map_err(BatchSlotVoteStoreError::Database)?, 7);
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
async fn epoch_advance_retires_history_and_refuses_rewind() -> Result<(), BatchSlotVoteStoreError> {
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
async fn epoch_initialization_waits_for_its_durable_marker() -> Result<(), BatchSlotVoteStoreError>
{
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
