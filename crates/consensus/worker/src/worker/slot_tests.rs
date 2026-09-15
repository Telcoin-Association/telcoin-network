//! Availability reservations must precede the worker's own quorum contribution.

use super::*;
use crate::quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait};
use futures::{StreamExt, TryStreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use tn_network_libp2p::types::NetworkHandle;
use tn_storage::{layered_db::LayeredDatabase, mem_db::MemDatabase};
use tn_test_utils::CommitteeFixture;
use tn_types::{
    Address, Batch, BatchBucket, BatchSlotControl, BatchSlotParent, BatchSlotVotes, BatchSlots,
    BatchValidationError, BlsSigner, DbTxMut, NoopTxnForwarder, SignedBatchSlotRecord, TaskManager,
    TaskSpawner, B256,
};
use tokio::sync::{mpsc, oneshot};

/// Decode real signed envelopes; transaction validity is exercised by the engine tests.
#[derive(Debug)]
struct NativeDecoder {
    /// The fixture's locally owned sender bucket.
    bucket: BatchBucket,
}

impl BatchValidation for NativeDecoder {
    fn slot_bucket(&self, _transaction: &[u8]) -> Result<BatchBucket, BatchValidationError> {
        Ok(self.bucket)
    }

    fn validate_batch(&self, batch: SealedBatch) -> Result<(), BatchValidationError> {
        self.validate_batch_for_vote(batch).map(|_| ())
    }

    fn validate_batch_for_vote(
        &self,
        batch: SealedBatch,
    ) -> Result<Option<SignedBatchSlotRecord>, BatchValidationError> {
        SignedBatchSlotRecord::from_envelope(&batch.batch)
            .map(Some)
            .map_err(BatchValidationError::SlotProtocol)
    }

    fn submit_txn_if_mine(&self, _bytes: &[u8], _size: u64, _slot: u64) {}
}

/// Count self-stake attempts and stop before a real network or primary is needed.
#[derive(Clone, Debug)]
struct RejectingQuorum {
    /// Shared by every clone of the worker under test.
    calls: Arc<AtomicUsize>,
}

impl QuorumWaiterTrait for RejectingQuorum {
    fn verify_batch(
        &self,
        _batch: SealedBatch,
        _timeout: Duration,
        _spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (send, receive) = oneshot::channel();
        let _ = send.send(Err(QuorumWaiterError::Timeout));
        receive
    }
}

#[tokio::test]
async fn self_stake_waits_for_durability_and_conflicting_clones_cannot_revote() -> eyre::Result<()>
{
    let fixture =
        CommitteeFixture::builder(|| LayeredDatabase::open(MemDatabase::new(), false)).build();
    let config = fixture
        .authorities()
        .next()
        .ok_or_else(|| eyre::eyre!("empty fixture"))?
        .consensus_config();
    let database = config.node_storage().clone();
    let author = config.key_config().public_key();
    let slots = BatchSlots::new(
        config.chain_id().into(),
        fixture.committee(),
        BatchSlotParent::new(B256::ZERO, B256::ZERO),
    )?;
    let bucket = slots
        .buckets()
        .find(|bucket| {
            slots
                .position(*bucket)
                .is_ok_and(|position| slots.producer(position).is_ok_and(|owner| owner == &author))
        })
        .ok_or_else(|| eyre::eyre!("fixture has no locally owned bucket"))?;
    let control = BatchSlotControl::default();
    let _publication = control.install(slots.clone(), Some(author));
    config.set_slot_control(control)?;
    config.slot_votes().initialize().await?;
    let batch = Batch {
        transactions: vec![vec![1]],
        epoch: slots.epoch(),
        beneficiary: Address::ZERO,
        base_fee_per_gas: 1,
        worker_id: 0,
        received_at: None,
    };
    let record = slots.sign_proposal(bucket, author, batch.clone(), config.key_config())?;
    let vote = slots.vote(&record)?;
    let calls = Arc::new(AtomicUsize::new(0));
    let tasks = TaskManager::default();
    let (network, _commands) = mpsc::channel(1);
    let worker = Worker::new(
        0,
        Some(RejectingQuorum { calls: calls.clone() }),
        LocalNetwork::new_with_empty_id(),
        database.clone(),
        Duration::from_secs(1),
        WorkerNetworkHandle::new(
            NetworkHandle::new(network),
            tasks.get_spawner(),
            0,
            slots.epoch(),
            config.chain_id(),
        ),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    )
    .with_slot_config(config, Arc::new(NativeDecoder { bucket }));
    let transaction = database.write_txn()?;
    let producer = worker.clone();
    let submitted = batch.clone();
    let pending = tokio::spawn(async move { producer.seal(submitted.seal_slow()).await });
    tokio::time::timeout(
        Duration::from_secs(10),
        futures::future::poll_fn(|cx| {
            let visible = database.contains_key::<BatchSlotVotes>(vote.key());
            if visible.as_ref().is_ok_and(|present| *present) || visible.is_err() {
                std::task::Poll::Ready(visible.map(|_| ()))
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }),
    )
    .await??;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "self-stake escaped the durable reservation barrier"
    );
    assert!(!pending.is_finished());
    transaction.commit()?;
    assert!(matches!(pending.await?, Err(BlockSealError::Timeout)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let mut conflicting = batch;
    conflicting.transactions.push(vec![2]);
    assert!(matches!(
        worker.seal(conflicting.seal_slow()).await,
        Err(BlockSealError::SlotAdmission(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the same slot and view received two self-votes");
    Ok(())
}

/// One accepted asynchronous forwarding job observed by the test.
#[derive(Debug)]
struct ForwardJob {
    /// Validator selected by the native worker's routing policy.
    target: BlsPublicKey,
    /// Original transactions, without a native consensus envelope.
    transactions: Vec<Vec<u8>>,
}

/// Accept every job, including jobs for an owner that never confirms execution.
#[derive(Clone, Debug, Default)]
struct AcceptingForwarder {
    /// Record jobs independently of the background RPC outcome.
    jobs: Arc<parking_lot::Mutex<Vec<ForwardJob>>>,
}

impl TxnForwarder for AcceptingForwarder {
    fn forward_txns(
        &self,
        transactions: Vec<Vec<u8>>,
        targets: Vec<BlsPublicKey>,
        _rpcs: Vec<(BlsPublicKey, tn_types::RpcInfo)>,
    ) -> bool {
        self.jobs.lock().extend(
            targets
                .into_iter()
                .map(|target| ForwardJob { target, transactions: transactions.clone() }),
        );
        true
    }
}

#[tokio::test]
async fn accepted_forward_jobs_still_visit_every_retry_witness() -> eyre::Result<()> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let config = fixture
        .authorities()
        .next()
        .ok_or_else(|| eyre::eyre!("empty fixture"))?
        .consensus_config();
    let committee = fixture.committee();
    let keys: Vec<_> =
        committee.authorities().iter().map(|authority| *authority.protocol_key()).collect();
    let slots = BatchSlots::new(
        config.chain_id().into(),
        committee,
        BatchSlotParent::new(B256::ZERO, B256::ZERO),
    )?;
    let bucket = slots.bucket(Address::ZERO);
    let control = BatchSlotControl::default();
    let _publication = control.install(slots.clone(), None);
    config.set_slot_control(control)?;
    let forwarder = AcceptingForwarder::default();
    let tasks = TaskManager::default();
    let (network, commands) = mpsc::channel(8);
    let worker = Worker::new(
        0,
        None::<RejectingQuorum>,
        LocalNetwork::new_with_empty_id(),
        config.node_storage().clone(),
        Duration::from_secs(1),
        WorkerNetworkHandle::new(
            NetworkHandle::new(network),
            tasks.get_spawner(),
            0,
            slots.epoch(),
            config.chain_id(),
        ),
        Arc::new(forwarder.clone()),
        keys.clone(),
    )
    .with_slot_config(config, Arc::new(NativeDecoder { bucket }));
    // Endpoint discovery is independent of the worker's routing decision. The test forwarder
    // models positive queue admission, which must not suppress the additional witness job.
    let discovery = tokio::spawn(async move {
        futures::stream::unfold(commands, |mut commands| async move {
            commands.recv().await.map(|command| (command, commands))
        })
        .for_each(|command| async move {
            if let tn_network_libp2p::types::NetworkCommand::GetAllValidatorRpcs { reply } = command
            {
                let _ = reply.send(Vec::new());
            }
        })
        .await;
    });
    let batch = Batch {
        transactions: vec![vec![1]],
        epoch: slots.epoch(),
        beneficiary: Address::ZERO,
        base_fee_per_gas: 1,
        worker_id: 0,
        received_at: None,
    };
    let forwards =
        futures::stream::iter(0..keys.len()).map(Ok::<_, BlockSealError>).try_for_each(|_| {
            let worker = worker.clone();
            let batch = batch.clone();
            async move { worker.seal(batch.seal_slow()).await }
        });
    tokio::time::timeout(Duration::from_secs(10), forwards).await??;
    let jobs = forwarder.jobs.lock();
    assert_eq!(
        jobs.len(),
        keys.len() * 2 - 1,
        "each retry queues at most the owner and one distinct witness"
    );
    let reached: std::collections::BTreeSet<_> = jobs.iter().map(|job| job.target).collect();
    assert_eq!(
        reached,
        keys.into_iter().collect(),
        "an accepting owner hid demand from an honest retry voter"
    );
    assert!(jobs.iter().all(|job| job.transactions == batch.transactions));
    discovery.abort();
    Ok(())
}
