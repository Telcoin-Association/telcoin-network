//! Unit tests for the worker's batch provider.
use futures::FutureExt as _;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tempfile::TempDir;
use tn_network_libp2p::types::NetworkHandle;
use tn_network_types::{local::LocalNetwork, MockWorkerToPrimary};
use tn_reth::test_utils::transaction;
use tn_storage::{
    mem_db::MemDatabase,
    open_db,
    tables::{NodeBatchesCache, OurNodeBatchesCache},
};
use tn_types::{
    error::BlockSealError, test_chain_spec_arc, Batch, Database, NoopTxnForwarder, SealedBatch,
    ShutdownNotifier, TaskManager, TaskSpawner,
};
use tn_worker::{
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    test_utils::TestMakeBlockQuorumWaiter,
    Worker, WorkerNetworkHandle,
};
use tokio::sync::{mpsc, oneshot};

/// Quorum waiter that records calls synchronously and immediately accepts each batch.
#[derive(Clone, Default)]
struct RecordingQuorumWaiter {
    /// Number of batches submitted for quorum verification.
    calls: Arc<AtomicUsize>,
}

impl QuorumWaiterTrait for RecordingQuorumWaiter {
    /// Record the call and resolve quorum without a spawned task or elapsed-time dependency.
    fn verify_batch(
        &self,
        _batch: SealedBatch,
        _timeout: Duration,
        _task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        rx
    }
}

/// Empty seals complete immediately without quorum, network, storage, or primary effects.
fn assert_empty_seal_is_noop(quorum_waiter: Option<RecordingQuorumWaiter>) {
    let store = MemDatabase::default();
    let task_manager = TaskManager::default();
    let (tx, mut rx) = mpsc::channel(1);
    let batch_provider = Worker::new(
        0,
        quorum_waiter.clone(),
        // Reporting without a registered primary handler would fail the seal.
        LocalNetwork::new_with_empty_id(),
        store.clone(),
        Duration::from_secs(5),
        WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner(), 0, 0, 0),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    );
    let empty_batch = Batch::default().seal_slow();
    let digest = empty_batch.digest();

    // A network command would remain pending because the receiver sends no response.
    assert!(matches!(batch_provider.seal(empty_batch).now_or_never(), Some(Ok(()))));
    assert!(quorum_waiter.is_none_or(|waiter| waiter.calls.load(Ordering::SeqCst) == 0));
    assert!(matches!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
    assert!(store.get::<OurNodeBatchesCache>(&digest).is_ok_and(|batch| batch.is_none()));
    assert!(store.get::<NodeBatchesCache>(&digest).is_ok_and(|batch| batch.is_none()));
}

/// A committee validator drops an empty batch before seeking quorum or broadcasting it.
#[tokio::test]
async fn validator_empty_seal_is_noop() {
    assert_empty_seal_is_noop(Some(RecordingQuorumWaiter::default()));
}

/// An observer also drops an empty batch without discovering forwarding endpoints.
#[tokio::test]
async fn observer_empty_seal_is_noop() {
    assert_empty_seal_is_noop(None);
}

/// A committee validator refuses to seal once this epoch's consensus shutdown has begun.
///
/// The refusal comes before quorum, so no peer is asked to vote and the batch cache stays empty.
#[tokio::test]
async fn validator_refuses_seal_after_consensus_shutdown() {
    let store = MemDatabase::default();
    let task_manager = TaskManager::default();
    let quorum_waiter = RecordingQuorumWaiter::default();
    let client = LocalNetwork::new_with_empty_id();
    client
        .set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary()))
        .expect("register mock primary handler");
    let shutdown = ShutdownNotifier::new();
    let batch_provider = Worker::new(
        0,
        Some(quorum_waiter.clone()),
        client,
        store.clone(),
        Duration::from_secs(5),
        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    )
    .with_consensus_shutdown(shutdown.clone());
    let tx = transaction(test_chain_spec_arc());

    // control: the same worker seals through quorum before shutdown
    let before = Batch { transactions: vec![tx.clone()], ..Default::default() }.seal_slow();
    batch_provider.seal(before).await.expect("seal before consensus shutdown");
    assert_eq!(quorum_waiter.calls.load(Ordering::SeqCst), 1);

    shutdown.notify();
    let after = Batch { transactions: vec![tx.clone(), tx], ..Default::default() }.seal_slow();
    let digest = after.digest();
    let res = batch_provider.seal(after).await;
    assert!(
        matches!(res, Err(BlockSealError::ConsensusShuttingDown)),
        "unexpected seal result: {res:?}"
    );
    assert_eq!(quorum_waiter.calls.load(Ordering::SeqCst), 1, "no quorum request after shutdown");
    assert!(store.get::<OurNodeBatchesCache>(&digest).is_ok_and(|batch| batch.is_none()));
}

#[tokio::test]
async fn make_batch() {
    let client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    client
        .set_worker_to_primary_local_handler(Arc::new(mock_server))
        .expect("register mock primary handler");

    // Spawn a `BatchProvider` instance.
    let id = 0;
    let qw = TestMakeBlockQuorumWaiter::new_test();
    let timeout = Duration::from_secs(5);
    let task_manager = TaskManager::default();
    let batch_provider = Worker::new(
        id,
        Some(qw.clone()),
        client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    );

    // Send enough transactions to seal a batch.
    let chain = test_chain_spec_arc();
    let tx = transaction(chain);
    let new_batch = Batch { transactions: vec![tx.clone(), tx.clone()], ..Default::default() };

    batch_provider.seal(new_batch.clone().seal_slow()).await.unwrap();

    // Ensure the batch is as expected.
    let expected_batch = Batch { transactions: vec![tx.clone(), tx.clone()], ..Default::default() };

    assert_eq!(
        new_batch.transactions(),
        qw.0.lock()
            .unwrap()
            .as_ref()
            .expect("batch not sent to Quorum Waiter!")
            .batch()
            .transactions()
    );

    // Ensure the batch is stored
    assert!(store.get::<NodeBatchesCache>(&expected_batch.digest()).unwrap().is_some());
}

/// An observer worker (no quorum waiter) must refuse a non-empty seal when the batch is not
/// admitted to a forward task. The test network handle discovers no validator RPC endpoints
/// and `NoopTxnForwarder` admits nothing, so `seal` returns `NotValidator` and never writes
/// the batch cache. An empty batch stays a success because there is nothing to forward.
///
/// The worker's consensus shutdown has already fired. The result is still `NotValidator`, not
/// `ConsensusShuttingDown`, because the shutdown refusal guards only the quorum path and must not
/// hold up forwarding.
#[tokio::test]
async fn observer_seal_without_admission_returns_not_validator() {
    let client = LocalNetwork::new_with_empty_id();
    let temp_dir = TempDir::new().unwrap();
    let store = open_db(temp_dir.path());

    // Mock the primary client to always succeed.
    let mock_server = MockWorkerToPrimary();
    client
        .set_worker_to_primary_local_handler(Arc::new(mock_server))
        .expect("register mock primary handler");

    // A `BatchProvider` instance without a quorum waiter: the observer path.
    let id = 0;
    let timeout = Duration::from_secs(5);
    let task_manager = TaskManager::default();
    let consensus_shutdown = ShutdownNotifier::new();
    consensus_shutdown.notify();
    let batch_provider = Worker::new(
        id,
        None::<TestMakeBlockQuorumWaiter>,
        client,
        store.clone(),
        timeout,
        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
        Arc::new(NoopTxnForwarder),
        Vec::new(),
    )
    .with_consensus_shutdown(consensus_shutdown);

    // Seal a batch with transactions.
    let chain = test_chain_spec_arc();
    let tx = transaction(chain);
    let new_batch = Batch { transactions: vec![tx.clone(), tx], ..Default::default() };
    let digest = new_batch.digest();

    let res = batch_provider.seal(new_batch.seal_slow()).await;
    assert!(matches!(res, Err(BlockSealError::NotValidator)), "unexpected seal result: {res:?}");

    // The observer path refuses before the batch cache write.
    assert!(store.get::<NodeBatchesCache>(&digest).unwrap().is_none());

    // An empty batch is still a success.
    let empty_batch = Batch { transactions: vec![], ..Default::default() };
    assert!(batch_provider.seal(empty_batch.seal_slow()).await.is_ok());
}

/// The batch metrics count a batch only once its report to the primary succeeds (issue #1444).
///
/// The first seal reaches quorum but no primary handler is registered, so the report fails
/// with `FailedToReport`, as it does on a node whose proposer is not running, so nothing accepts
/// the report. The batch builder keeps the transactions and re-seals the same batch, which
/// reaches quorum again and is reported this time. Recording at quorum would count both attempts.
#[tokio::test]
async fn seal_records_batch_metrics_only_after_report() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let client = LocalNetwork::new_with_empty_id();
    let store = MemDatabase::default();
    let task_manager = TaskManager::default();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // metric handles bind to the recorder active at construction, so seals awaited outside
    // this closure still record into the debugging recorder
    let batch_provider = metrics::with_local_recorder(&recorder, || {
        Worker::new(
            0,
            Some(RecordingQuorumWaiter::default()),
            client.clone(),
            store.clone(),
            Duration::from_secs(5),
            WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        )
    });
    let tx = transaction(test_chain_spec_arc());
    let sealed_batch =
        Batch { transactions: vec![tx.clone(), tx], ..Default::default() }.seal_slow();

    // the worker records `Batch::size()`, which excludes the sealed digest
    let expected_size = sealed_batch.batch().size() as f64;
    // each snapshot swaps the counters to zero and drains the histograms, so every call returns
    // only what the worker recorded since the previous call
    let drain = || {
        let (mut sealed, mut sizes, mut txs) = (None, None, None);
        for (key, _, _, value) in snapshotter.snapshot().into_vec() {
            if !key.key().labels().any(|l| l.key() == "worker" && l.value() == "0") {
                continue;
            }
            match (key.key().name(), value) {
                ("tn_worker.batches_sealed_total", DebugValue::Counter(n)) => sealed = Some(n),
                ("tn_worker.batch_size_bytes", DebugValue::Histogram(xs)) => {
                    sizes = Some(xs.into_iter().map(|x| x.0).collect::<Vec<f64>>())
                }
                ("tn_worker.batch_transactions", DebugValue::Histogram(xs)) => {
                    txs = Some(xs.into_iter().map(|x| x.0).collect::<Vec<f64>>())
                }
                _ => {}
            }
        }
        (
            sealed.expect("tn_worker.batches_sealed_total registered for worker 0"),
            sizes.expect("tn_worker.batch_size_bytes registered for worker 0"),
            txs.expect("tn_worker.batch_transactions registered for worker 0"),
        )
    };

    let res = batch_provider.seal(sealed_batch.clone()).await;
    assert!(matches!(res, Err(BlockSealError::FailedToReport)), "first seal: {res:?}");
    assert_eq!(drain(), (0, vec![], vec![]), "a seal whose report fails records nothing");

    client
        .set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary()))
        .expect("register mock primary handler");
    batch_provider.seal(sealed_batch).await.expect("re-seal reports to the primary");
    assert_eq!(drain(), (1, vec![expected_size], vec![2.0]), "the reported seal is recorded once");
}

/// One captured tracing event: its target and its fields, the message under `message`.
#[derive(Debug, Default)]
struct Captured {
    /// The event's target.
    target: String,
    /// Field names and values, strings without quotes.
    fields: Vec<(String, String)>,
}

impl Captured {
    /// The value of field `name`, if the event carries it.
    fn field(&self, name: &str) -> Option<&str> {
        self.fields.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

impl tracing::field::Visit for Captured {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields.push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.fields.push((field.name().to_string(), format!("{value:?}")));
    }
}

/// A subscriber that keeps every event and ignores spans.
#[derive(Clone, Default)]
struct Capture(Arc<std::sync::Mutex<Vec<Captured>>>);

impl Capture {
    /// Take every event captured since the last call.
    fn take(&self) -> Vec<Captured> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// The `consensus::metrics` events in `events` whose message is `message`, in order.
fn lines<'a>(events: &'a [Captured], message: &str) -> Vec<&'a Captured> {
    events
        .iter()
        .filter(|event| {
            event.target == "consensus::metrics" && event.field("message") == Some(message)
        })
        .collect()
}

impl tracing::Subscriber for Capture {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _attributes: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}
    fn enter(&self, _span: &tracing::span::Id) {}
    fn exit(&self, _span: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut captured =
            Captured { target: event.metadata().target().to_string(), ..Default::default() };
        event.record(&mut captured);
        self.0.lock().unwrap().push(captured);
    }
}

/// `batch sealed` carries the digest and quorum time on every quorum, `batch reported` follows
/// only a successful report with the digest and report time, and the report histogram times
/// both outcomes.
///
/// The seal runs on the test's thread (the quorum waiter resolves at once), so a thread-local
/// subscriber sees every line.
#[tokio::test]
async fn seal_lines_carry_digest_quorum_and_report_timing() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    let capture = Capture::default();
    let _guard = tracing::subscriber::set_default(capture.clone());
    let client = LocalNetwork::new_with_empty_id();
    let store = MemDatabase::default();
    let task_manager = TaskManager::default();
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let batch_provider = metrics::with_local_recorder(&recorder, || {
        Worker::new(
            0,
            Some(RecordingQuorumWaiter::default()),
            client.clone(),
            store.clone(),
            Duration::from_secs(5),
            WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        )
    });
    let tx = transaction(test_chain_spec_arc());
    let sealed_batch = Batch { transactions: vec![tx], ..Default::default() }.seal_slow();
    let digest = sealed_batch.digest().to_string();
    let number = |line: &Captured, field: &str| {
        let value = line.field(field).unwrap_or_else(|| panic!("{field} missing: {line:?}"));
        value.parse::<u64>().unwrap_or_else(|_| panic!("{field}={value} is not a number"))
    };
    let assert_sealed = |sealed: &[&Captured]| {
        assert_eq!(sealed.len(), 1, "one batch sealed line per quorum");
        assert_eq!(sealed[0].field("digest"), Some(digest.as_str()));
        assert_eq!(sealed[0].field("worker_id"), Some("0"));
        number(sealed[0], "quorum_ms");
    };

    // the report fails without a primary handler: sealed, but never reported
    let res = batch_provider.seal(sealed_batch.clone()).await;
    assert!(matches!(res, Err(BlockSealError::FailedToReport)), "first seal: {res:?}");
    let events = capture.take();
    assert_sealed(&lines(&events, "batch sealed"));
    assert!(lines(&events, "batch reported").is_empty(), "a failed report logs no reported line");

    client
        .set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary()))
        .expect("register mock primary handler");
    batch_provider.seal(sealed_batch.clone()).await.expect("re-seal reports to the primary");
    let events = capture.take();
    assert_sealed(&lines(&events, "batch sealed"));
    let reported = lines(&events, "batch reported");
    assert_eq!(reported.len(), 1, "one batch reported line per successful report");
    assert_eq!(reported[0].field("digest"), Some(digest.as_str()));
    number(reported[0], "report_us");
    assert_eq!(reported[0].field("report_ms"), None, "report time is logged in microseconds only");

    let report_samples =
        snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
            (key.key().name() == "tn_worker.batch_report_seconds").then_some(value)
        });
    assert!(
        matches!(report_samples, Some(DebugValue::Histogram(ref samples)) if samples.len() == 2),
        "the failed and the successful report are both timed: {report_samples:?}"
    );
}
