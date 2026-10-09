//! The receiving side of the execution layer's `BatchProvider`.
//!
//! Consensus `BatchProvider` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{
    batch_fetcher::BatchFetcher,
    metrics::{ForwardDropReason, WorkerMetrics},
    network::primary::PrimaryReceiverHandler,
    quorum_waiter::{QuorumWaiter, QuorumWaiterTrait},
    WorkerNetworkHandle,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, WorkerOwnBatchMessage, WorkerToPrimaryClient};
use tn_storage::{
    consensus::ConsensusChain,
    tables::{NodeBatchesCache, OurNodeBatchesCache},
};
use tn_types::{
    error::BlockSealError, BatchReceiver, BatchSender, BatchValidation, BlsPublicKey, Database,
    SealedBatch, ShutdownNotifier, TaskManager, TxnForwarder, WorkerId,
};
use tracing::{error, info, instrument, warn};

/// The default channel capacity for each channel of the worker.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// Spawn the worker.
///
/// Create an instance of `Self` and start all tasks to participate in consensus.
pub fn new_worker<DB: Database>(
    id: WorkerId,
    validator: Arc<dyn BatchValidation>,
    consensus_config: ConsensusConfig<DB>,
    network_handle: WorkerNetworkHandle,
    forwarder: Arc<dyn TxnForwarder>,
    consensus_chain: ConsensusChain,
) -> eyre::Result<Worker<DB, QuorumWaiter>> {
    info!(target: "worker::worker", "Boot worker node with id {} key {:?}", id, consensus_config.key_config().primary_public_key());

    let batch_fetcher = BatchFetcher::new(
        network_handle.clone(),
        consensus_config.node_storage().clone(),
        consensus_chain,
        WorkerMetrics::new_for_worker(id),
    );
    // This worker's own local network instance: a worker id outside the committee's worker
    // set is a wiring bug, surfaced as an error rather than a fallback onto another
    // worker's instance.
    let local_network = consensus_config
        .local_network(id)
        .cloned()
        .ok_or_else(|| eyre::eyre!("no local network instance for worker id {id}"))?;
    local_network.set_primary_to_worker_local_handler(Arc::new(PrimaryReceiverHandler {
        store: consensus_config.node_storage().clone(),
        network: Some(network_handle.clone()),
        batch_fetcher,
        validator,
    }))?;
    let batch_provider = new_worker_internal(
        id,
        &consensus_config,
        local_network,
        network_handle.clone(),
        forwarder,
    );

    // NOTE: This log entry is used to compute performance.
    info!(target: "worker::worker",
        "Worker {} successfully booted on {}",
        id,
        consensus_config.worker_address(id).map_or_else(|| "<no address>".to_string(), |addr| addr.to_string())
    );

    Ok(batch_provider)
}

#[cfg(test)]
mod local_recovery_tests {
    //! Required controls for bounded accepted batch ownership and retry.
    use super::*;
    use crate::quorum_waiter::QuorumWaiterError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tn_network_libp2p::types::NetworkHandle;
    use tn_storage::mem_db::MemDatabase;
    use tn_types::{Batch, NoopTxnForwarder, TaskManager, TaskSpawner};

    /// Observe whether cache refusal happens before any quorum acknowledgement is possible.
    #[derive(Clone, Default)]
    struct QuorumCalls(Arc<AtomicUsize>);

    /// A failed retry follows a quorum-accepted attempt without erasing its cache ownership.
    #[derive(Clone, Default)]
    struct RetryQuorumCalls(Arc<AtomicUsize>);

    /// Supply canonical resolution to the isolated cache iterator control.
    #[derive(Clone)]
    struct ResolvedLocalBatchRecovery;

    /// Exercise the production observer ACK callback with a real worker pool.
    #[derive(Clone)]
    struct ObserverLocalBatchRecovery(
        /// The real worker pool receiving the production observer ACK callback.
        tn_reth::WorkerTxPool,
    );

    impl LocalBatchRecovery for ObserverLocalBatchRecovery {
        fn cache_resolved(&self, batch: &tn_types::Batch) -> bool {
            self.0.local_batch_canonically_resolved(batch.transactions())
        }
        fn observer_accepted(&self, digest: tn_types::BlockHash) {
            self.0.mark_observer_seal(digest);
        }
    }

    /// Real observer ACK, failed forwarding and prune restore the identical digest reservation.
    #[tokio::test]
    async fn observer_ack_failed_forward_restores_same_digest_reservation() -> eyre::Result<()> {
        use tn_reth::{
            test_utils::TransactionFactory, ForwardTargetPolicy, RethChainSpec, RethEnv,
            WorkerRpcForwarder,
        };
        use tn_types::{
            gas_accumulator::BaseFeeContainer, BlsKeypair, Bytes, Encodable2718, GenesisAccount,
            RpcInfo, U256,
        };
        tokio::time::timeout(Duration::from_secs(45), async {
            let directory = tempfile::tempdir()?;
            let tasks = TaskManager::default();
            let mut factory = TransactionFactory::new();
            let chain: Arc<RethChainSpec> = Arc::new(
                tn_types::test_genesis()
                    .extend_accounts([(
                        factory.address(),
                        GenesisAccount::default().with_balance(U256::MAX),
                    )])
                    .into(),
            );
            let env = RethEnv::new_for_temp_chain(chain.clone(), directory.path(), &tasks, None)?;
            let pool = env.init_txn_pool(BaseFeeContainer::default())?;
            let transaction = factory.create_eip1559(
                chain.clone(),
                Some(21_000),
                7,
                Some(tn_types::Address::ZERO),
                U256::from(1),
                Bytes::new(),
            );
            let hash = *transaction.hash();
            factory.submit_tx_to_pool(transaction.clone(), pool.clone()).await;
            let batch =
                Batch { transactions: vec![transaction.encoded_2718()], ..Default::default() }
                    .seal_slow();
            let retention =
                pool.reserve_local_seal(batch.digest(), batch.batch().transactions())?;
            let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
            let address = closed.local_addr()?;
            drop(closed);
            let key = *BlsKeypair::from_bytes(&[7; 32]).expect("BLS fixture").public();
            let rpc = RpcInfo { http: format!("http://{address}").parse()?, ws: None };
            let (network, mut commands) = tokio::sync::mpsc::channel(2);
            let worker = Worker::new(
                0,
                None::<QuorumCalls>,
                LocalNetwork::new_with_empty_id(),
                MemDatabase::default(),
                Duration::from_secs(1),
                WorkerNetworkHandle::new(NetworkHandle::new(network), tasks.get_spawner(), 0, 0, 0),
                Arc::new(WorkerRpcForwarder::new(
                    tasks.get_spawner(),
                    ForwardTargetPolicy::AllowPrivate,
                    Some(pool.clone()),
                )),
                vec![key],
            )
            .with_local_recovery(
                Arc::new(Mutex::new(())),
                env.local_batch_seal_locks(),
                ObserverLocalBatchRecovery(pool.clone()),
            );
            let (ack, ()) = tokio::join!(worker.seal(batch.clone()), async {
                match commands.recv().await.expect("observer discovery request") {
                    tn_network_libp2p::types::NetworkCommand::GetAllValidatorRpcs { reply } => {
                        reply.send(vec![(key, rpc.clone())]).expect("observer discovery reply");
                    }
                    other => panic!("unexpected observer network request: {other:?}"),
                }
            });
            ack?;
            retention.accepted();
            assert!(pool.reserve_local_seal(batch.digest(), batch.batch().transactions()).is_err());
            pool.update_canonical_state(
                &chain.sealed_genesis_header(),
                Some(u128::MAX),
                vec![hash],
                vec![],
            )
            .await?;
            assert!(pool.get(&hash).is_none(), "ACK must be followed by actual optimistic prune");
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let next = std::future::poll_fn(|context| {
                if interval.poll_tick(context).is_ready() {
                    let ready = pool.get(&hash).and_then(|_| {
                        pool.reserve_local_seal(batch.digest(), batch.batch().transactions()).ok()
                    });
                    ready.map_or_else(
                        || {
                            context.waker().wake_by_ref();
                            std::task::Poll::Pending
                        },
                        std::task::Poll::Ready,
                    )
                } else {
                    std::task::Poll::Pending
                }
            })
            .await;
            assert!(pool.get(&hash).is_some(), "failed forwarding must recover the signed bytes");
            drop(next);
            assert!(pool.reserve_local_seal(batch.digest(), batch.batch().transactions()).is_ok());
            Ok::<(), eyre::Report>(())
        })
        .await?
    }

    impl LocalBatchRecovery for ResolvedLocalBatchRecovery {
        fn cache_resolved(&self, _batch: &tn_types::Batch) -> bool {
            true
        }
        fn observer_accepted(&self, _digest: tn_types::BlockHash) {}
    }

    impl QuorumWaiterTrait for RetryQuorumCalls {
        fn verify_batch(
            &self,
            _batch: SealedBatch,
            _timeout: Duration,
            _spawner: &TaskSpawner,
        ) -> tokio::sync::oneshot::Receiver<Result<(), QuorumWaiterError>> {
            let attempt = self.0.fetch_add(1, Ordering::SeqCst);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let result = if attempt == 1 { Err(QuorumWaiterError::Network) } else { Ok(()) };
            let _ = sender.send(result);
            receiver
        }
    }

    /// Same-digest retry preserves an earlier quorum acceptance through report and retry failure.
    #[tokio::test]
    async fn local_cache_same_digest_retry_preserves_accepted_bytes() -> eyre::Result<()> {
        let store = MemDatabase::default();
        let tasks = TaskManager::default();
        let client = LocalNetwork::new_with_empty_id();
        let calls = RetryQuorumCalls::default();
        let worker = Worker::new(
            0,
            Some(calls.clone()),
            client.clone(),
            store.clone(),
            Duration::from_secs(1),
            WorkerNetworkHandle::new_for_test(tasks.get_spawner()),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        );
        let batch = Batch { transactions: vec![vec![1]], ..Default::default() }.seal_slow();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), worker.seal(batch.clone())).await?,
            Err(BlockSealError::FailedToReport)
        ));
        assert_eq!(store.get::<OurNodeBatchesCache>(&batch.digest())?, Some(batch.batch().clone()));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), worker.seal(batch.clone())).await?,
            Err(BlockSealError::FailedQuorum)
        ));
        assert_eq!(calls.0.load(Ordering::SeqCst), 2, "cached digest must reach retry quorum");
        assert_eq!(store.get::<OurNodeBatchesCache>(&batch.digest())?, Some(batch.batch().clone()));
        client.set_worker_to_primary_local_handler(Arc::new(
            tn_network_types::MockWorkerToPrimary(),
        ))?;
        tokio::time::timeout(Duration::from_secs(5), worker.seal(batch.clone())).await??;
        assert_eq!(calls.0.load(Ordering::SeqCst), 3);
        assert_eq!(store.get::<OurNodeBatchesCache>(&batch.digest())?, Some(batch.batch().clone()));
        let mut substituted = batch.batch().clone();
        substituted.transactions = vec![vec![2]];
        assert!(matches!(
            worker.cache_local_batch(&batch.digest(), &substituted),
            Err(BlockSealError::FailedQuorum)
        ));
        assert_eq!(store.get::<OurNodeBatchesCache>(&batch.digest())?, Some(batch.batch().clone()));
        Ok(())
    }

    /// Isolate a blocked storage call so the regression fails without hanging runtime teardown.
    #[test]
    fn local_cache_resolved_cleanup_drops_iterator_before_write() -> eyre::Result<()> {
        if std::env::var_os("TN_LOCAL_CACHE_CLEANUP_CHILD").is_some() {
            local_cache_resolved_cleanup_child()
        } else {
            let module =
                module_path!().split_once("::").map(|(_, module)| module).unwrap_or(module_path!());
            let test =
                format!("{module}::local_cache_resolved_cleanup_drops_iterator_before_write");
            let directory = tempfile::tempdir()?;
            let marker = directory.path().join("resolved");
            let mut child = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", &test, "--nocapture"])
                .env("TN_LOCAL_CACHE_CLEANUP_CHILD", "1")
                .env("TN_LOCAL_CACHE_CLEANUP_MARKER", &marker)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()?;
            let status = (0..200)
                .find_map(|_| {
                    child.try_wait().transpose().or_else(|| {
                        std::thread::sleep(Duration::from_millis(25));
                        None
                    })
                })
                .transpose()?;
            if status.is_none() {
                child.kill()?;
                child.wait()?;
                panic!("resolved cache cleanup blocked while its iterator retained a read guard");
            }
            assert!(
                status.is_some_and(|status| status.success()),
                "isolated resolved cache fixture failed"
            );
            assert_eq!(std::fs::read(marker)?, b"resolved", "the exact child fixture must execute");
            Ok(())
        }
    }

    /// Run only in the regression's bounded child process.
    fn local_cache_resolved_cleanup_child() -> eyre::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        let _entered = runtime.enter();
        let store = MemDatabase::default();
        let retained = Batch { transactions: vec![vec![0]], ..Default::default() }.seal_slow();
        store.insert::<OurNodeBatchesCache>(&retained.digest(), retained.batch())?;
        let task_manager = TaskManager::default();
        let (network, _commands) = tokio::sync::mpsc::channel(1);
        let worker = Worker::new(
            0,
            Some(QuorumCalls::default()),
            LocalNetwork::new_with_empty_id(),
            store.clone(),
            Duration::from_secs(1),
            WorkerNetworkHandle::new(
                NetworkHandle::new(network),
                task_manager.get_spawner(),
                0,
                0,
                0,
            ),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        )
        .with_local_recovery(
            Arc::new(Mutex::new(())),
            Arc::new(tn_types::LocalBatchSealLocks::default()),
            ResolvedLocalBatchRecovery,
        );
        let next = Batch { transactions: vec![vec![1]], ..Default::default() }.seal_slow();
        worker.cache_local_batch(&next.digest(), next.batch())?;
        assert!(!store.contains_key::<OurNodeBatchesCache>(&retained.digest())?);
        assert_eq!(store.get::<OurNodeBatchesCache>(&next.digest())?, Some(next.batch().clone()));
        let marker = std::env::var_os("TN_LOCAL_CACHE_CLEANUP_MARKER")
            .ok_or_else(|| eyre::eyre!("isolated cache fixture requires its parent marker"))?;
        std::fs::write(marker, b"resolved")?;
        Ok(())
    }

    impl QuorumWaiterTrait for QuorumCalls {
        fn verify_batch(
            &self,
            _batch: SealedBatch,
            _timeout: Duration,
            _spawner: &TaskSpawner,
        ) -> tokio::sync::oneshot::Receiver<Result<(), QuorumWaiterError>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let _ = sender.send(Ok(()));
            receiver
        }
    }

    /// Durable cache exhaustion keeps accepted data and never starts quorum for new bytes.
    #[tokio::test]
    async fn local_cache_exhaustion_refuses_before_quorum() -> eyre::Result<()> {
        let store = MemDatabase::default();
        let retained =
            Batch { transactions: vec![vec![0]; 1024], ..Default::default() }.seal_slow();
        store.insert::<OurNodeBatchesCache>(&retained.digest(), retained.batch())?;
        let task_manager = TaskManager::default();
        let (network, mut commands) = tokio::sync::mpsc::channel(1);
        let calls = QuorumCalls::default();
        let worker = Worker::new(
            0,
            Some(calls.clone()),
            LocalNetwork::new_with_empty_id(),
            store.clone(),
            Duration::from_secs(1),
            WorkerNetworkHandle::new(
                NetworkHandle::new(network),
                task_manager.get_spawner(),
                0,
                0,
                0,
            ),
            Arc::new(NoopTxnForwarder),
            Vec::new(),
        );
        let refused = Batch { transactions: vec![vec![1]], ..Default::default() }.seal_slow();
        let refused_digest = refused.digest();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), worker.seal(refused)).await?,
            Err(BlockSealError::FailedQuorum)
        ));
        assert_eq!(calls.0.load(Ordering::SeqCst), 0);
        assert!(matches!(commands.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Empty)));
        assert_eq!(
            store.get::<OurNodeBatchesCache>(&retained.digest())?,
            Some(retained.batch().clone())
        );
        assert!(!store.contains_key::<OurNodeBatchesCache>(&refused_digest)?);
        Ok(())
    }
}

/// Builds a new batch provider responsible for handling client transactions.
fn new_worker_internal<DB: Database>(
    id: WorkerId,
    consensus_config: &ConsensusConfig<DB>,
    client: LocalNetwork,
    network_handle: WorkerNetworkHandle,
    forwarder: Arc<dyn TxnForwarder>,
) -> Worker<DB, QuorumWaiter> {
    info!(target: "worker::worker", "Starting handler for transactions");

    // The `QuorumWaiter` waits for 2f authorities to acknowledge receiving the batch
    // before forwarding the batch to the `Processor`
    // Only have a quorum waiter if we are an authority (validator).
    let quorum_waiter = consensus_config.authority().clone().map(|authority| {
        QuorumWaiter::new(
            authority,
            consensus_config.committee().clone(),
            network_handle.clone(),
            WorkerMetrics::new_for_worker(id),
        )
    });

    // Committee BLS keys in slot order (index == committee slot). A non-committee ("observer")
    // worker forwards each transaction it accepts to the JSON-RPC endpoint advertised by the
    // validator whose slot owns the sender, matching `submit_txn_if_mine` so nonce ordering is
    // preserved (issue #804).
    let committee_slots: Vec<BlsPublicKey> = consensus_config
        .committee()
        .authorities()
        .iter()
        .map(|authority| *authority.protocol_key())
        .collect();

    Worker::new(
        id,
        quorum_waiter,
        client,
        consensus_config.node_storage().clone(),
        consensus_config.parameters().batch_vote_timeout,
        network_handle,
        forwarder,
        committee_slots,
    )
    .with_consensus_shutdown(consensus_config.shutdown().clone())
}

/// Statically dispatched ownership hooks for accepted local and observer batches.
pub trait LocalBatchRecovery: Clone + Send + Sync + 'static {
    /// Canonical proof that every signed transaction in this batch has consumed its nonce.
    fn cache_resolved(&self, batch: &tn_types::Batch) -> bool;
    /// Mark the exact digest whose observer forwarding admission succeeded.
    fn observer_accepted(&self, digest: tn_types::BlockHash);
}

/// Existing worker callers have no accepted-byte recovery hook unless explicitly attached.
#[derive(Clone, Debug, Default)]
pub struct NoLocalBatchRecovery;

impl LocalBatchRecovery for NoLocalBatchRecovery {
    fn cache_resolved(&self, _batch: &tn_types::Batch) -> bool {
        false
    }
    fn observer_accepted(&self, _digest: tn_types::BlockHash) {}
}

/// Process batch from EL into sealed batches for CL.
pub struct Worker<DB, QW, R = NoLocalBatchRecovery> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to batches.
    quorum_waiter: Option<QW>,
    /// The network client to send our batches to the primary.
    client: LocalNetwork,
    /// The batch store to store our own batches.
    store: DB,
    /// Channel sender for alternate batch submision if not calling seal directly.
    tx_batches: BatchSender,
    /// Channel receiver for alternate batch submision if not calling seal directly.
    /// This will be "taken" on batch spawn and become None.
    rx_batches: Option<BatchReceiver>,
    /// The amount of time to wait on a reply from peer before timing out.
    timeout: Duration,
    /// Worker network handle.
    network_handle: WorkerNetworkHandle,
    /// Forwards transactions this node accepts to committee validators over their advertised
    /// JSON-RPC endpoints when this node is not a committee voting validator (issue #804).
    forwarder: Arc<dyn TxnForwarder>,
    /// Committee BLS keys in slot order (index == committee slot).
    ///
    /// Populated once per epoch at construction: a non-CVV worker forwards each transaction it
    /// accepts to the validator whose slot owns the sender, so nonce ordering is preserved.
    committee_slots: Vec<BlsPublicKey>,
    /// Prometheus metrics for this worker.
    metrics: WorkerMetrics,
    /// This epoch's consensus shutdown signal, the same notifier the primary's proposer exits on.
    ///
    /// Once it fires, a batch sealed by quorum could never be reported, so the seal is refused
    /// before any peer is asked to vote. `None` disables the check.
    consensus_shutdown: Option<ShutdownNotifier>,
    /// Shared node gate for bounded durable local batch ownership.
    local_cache_gate: Arc<Mutex<()>>,
    /// Canonical nonce proof required before deleting accepted batch bytes.
    local_recovery: R,
    /// Node-wide exact-digest serialization preserves accepted bytes through failed retries.
    local_seal_locks: Arc<tn_types::LocalBatchSealLocks>,
}

// Need to implement clone directly because of the rx_batches field.
// This field is a use once field when spawning the batch manager so this is fine.
// Code will panic quickly if this is messed up.
impl<DB: Clone, QW: Clone, R: Clone> Clone for Worker<DB, QW, R> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            quorum_waiter: self.quorum_waiter.clone(),
            client: self.client.clone(),
            store: self.store.clone(),
            tx_batches: self.tx_batches.clone(),
            rx_batches: None,
            timeout: self.timeout,
            network_handle: self.network_handle.clone(),
            forwarder: self.forwarder.clone(),
            committee_slots: self.committee_slots.clone(),
            metrics: self.metrics.clone(),
            consensus_shutdown: self.consensus_shutdown.clone(),
            local_cache_gate: self.local_cache_gate.clone(),
            local_recovery: self.local_recovery.clone(),
            local_seal_locks: self.local_seal_locks.clone(),
        }
    }
}

impl<DB, QW, R> std::fmt::Debug for Worker<DB, QW, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BatchProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> Worker<DB, QW> {
    /// Create an instance of `Self`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: WorkerId,
        quorum_waiter: Option<QW>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
        network_handle: WorkerNetworkHandle,
        forwarder: Arc<dyn TxnForwarder>,
        committee_slots: Vec<BlsPublicKey>,
    ) -> Self {
        let (tx_batches, rx_batches) = tokio::sync::mpsc::channel(1000);
        Self {
            id,
            quorum_waiter,
            client,
            store,
            tx_batches,
            rx_batches: Some(rx_batches),
            timeout,
            network_handle,
            forwarder,
            committee_slots,
            metrics: WorkerMetrics::new_for_worker(id),
            consensus_shutdown: None,
            local_cache_gate: Arc::new(Mutex::new(())),
            local_recovery: NoLocalBatchRecovery,
            local_seal_locks: Arc::new(tn_types::LocalBatchSealLocks::default()),
        }
    }
}

impl<DB: Database, QW: QuorumWaiterTrait, R: LocalBatchRecovery> Worker<DB, QW, R> {
    /// Refuse quorum seals once `shutdown` is notified.
    ///
    /// Pass this epoch's consensus shutdown notifier. After it fires the proposer is exiting and
    /// stops taking batch reports, so a quorum seal would make peers validate and store a batch
    /// whose report fails. Forwarding by a worker outside the committee is not affected.
    pub fn with_consensus_shutdown(mut self, shutdown: ShutdownNotifier) -> Self {
        self.consensus_shutdown = Some(shutdown);
        self
    }

    /// Attach one node's cache admission owner before starting worker batch tasks.
    pub fn with_local_recovery<H: LocalBatchRecovery>(
        self,
        gate: Arc<Mutex<()>>,
        locks: Arc<tn_types::LocalBatchSealLocks>,
        recovery: H,
    ) -> Worker<DB, QW, H> {
        Worker {
            id: self.id,
            quorum_waiter: self.quorum_waiter,
            client: self.client,
            store: self.store,
            tx_batches: self.tx_batches,
            rx_batches: self.rx_batches,
            timeout: self.timeout,
            network_handle: self.network_handle,
            forwarder: self.forwarder,
            committee_slots: self.committee_slots,
            metrics: self.metrics,
            consensus_shutdown: self.consensus_shutdown,
            local_cache_gate: gate,
            local_recovery: recovery,
            local_seal_locks: locks,
        }
    }

    /// Cache accepted bytes under a node-wide bound before starting quorum work.
    fn cache_local_batch(
        &self,
        digest: &tn_types::BlockHash,
        batch: &tn_types::Batch,
    ) -> Result<bool, tn_types::error::BlockSealError> {
        use tn_types::error::BlockSealError;
        /// Node-wide raw signed byte ceiling.
        const MAX_BYTES: usize = 64 * 1024 * 1024;
        /// Node-wide transaction and batch slot ceiling.
        const MAX_ENTRIES: usize = 1024;
        let _gate = self.local_cache_gate.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (bytes, entries, batches, visited, resolved) =
            self.store.iter::<OurNodeBatchesCache>().take(MAX_ENTRIES + 1).fold(
                (0usize, 0usize, 0usize, 0usize, Vec::new()),
                |(bytes, entries, batches, visited, mut resolved), (hash, cached)| {
                    if self.local_recovery.cache_resolved(&cached) {
                        resolved.push(hash);
                        (bytes, entries, batches, visited + 1, resolved)
                    } else {
                        let bytes = bytes.saturating_add(
                            cached.transactions().iter().map(Vec::len).sum::<usize>(),
                        );
                        let entries = entries.saturating_add(cached.transactions().len());
                        let batches = batches.saturating_add(1);
                        (bytes, entries, batches, visited + 1, resolved)
                    }
                },
            );
        // MemDatabase iterators hold a read guard. Release it before taking any write lock.
        resolved.into_iter().try_for_each(|hash| {
            self.store
                .remove::<OurNodeBatchesCache>(&hash)
                .map_err(|_| BlockSealError::FatalDBFailure)
        })?;
        // A legacy oversized cache drains in bounded attempts, without ignoring unseen rows.
        (visited <= MAX_ENTRIES && bytes <= MAX_BYTES && entries <= MAX_ENTRIES)
            .then_some(())
            .ok_or(BlockSealError::FailedQuorum)?;
        let cached = self
            .store
            .get::<OurNodeBatchesCache>(digest)
            .map_err(|_| BlockSealError::FatalDBFailure)?;
        cached
            .as_ref()
            .is_none_or(|cached| cached == batch)
            .then_some(())
            .ok_or(BlockSealError::FailedQuorum)?;
        let already_cached = cached.is_some();
        let new_bytes = batch.transactions().iter().map(Vec::len).sum::<usize>();
        if !already_cached
            && (bytes.saturating_add(new_bytes) > MAX_BYTES
                || entries.saturating_add(batch.transactions().len()) > MAX_ENTRIES
                || batches >= MAX_ENTRIES)
        {
            warn!(target: "worker::worker", bytes, entries, batches, "local batch recovery cache full; refusing before quorum");
            Err(BlockSealError::FailedQuorum)
        } else {
            self.store.insert::<OurNodeBatchesCache>(digest, batch).map(|()| !already_cached).map_err(|error| {
                error!(target: "worker::worker", ?error, "failed to retain local batch before quorum");
                BlockSealError::FatalDBFailure
            })
        }
    }

    /// Allows the engine to remain removed from the worker.
    /// Accept batches from the channel and seal them in this worker task.
    pub fn spawn_batch_builder(&mut self, prefix: &str, task_manager: &TaskManager) {
        let this_clone = self.clone();
        let mut rx_batches = self.rx_batches.take().expect("have batch receive");
        task_manager.spawn_critical_task(format!("{prefix} batch-builder"), async move {
            while let Some((batch, tx)) = rx_batches.recv().await {
                let res = this_clone.seal(batch).await;
                if tx.send(res).is_err() {
                    error!(target: "worker::batch_provider", "Error sending result to channel caller!  Channel closed.");
                }
            }
            Ok(())
        });
    }

    /// Return worker's ID.
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// True if this worker seals batches by collecting a committee quorum.
    ///
    /// A quorum-sealed batch must then be reported to this node's proposer, so the batch builder
    /// feeding this worker is only useful while that proposer runs. A worker without a quorum
    /// waiter forwards its transactions to committee validators instead.
    pub fn seals_via_quorum(&self) -> bool {
        self.quorum_waiter.is_some()
    }

    /// Return the network handle for this worker.
    pub fn network_handle(&self) -> WorkerNetworkHandle {
        self.network_handle.clone()
    }

    /// The sender end of the batch submit channel.
    pub fn batches_tx(&self) -> BatchSender {
        self.tx_batches.clone()
    }

    /// Forward all the txns in `sealed_batch` to committee validators so they can be included
    /// in blocks. Use this when not a CVV so that transactions you accept can be included.
    ///
    /// Replaces the previous gossip broadcast (issue #804): each transaction is forwarded to the
    /// JSON-RPC endpoint the owning validator advertised on its worker record, discovered over
    /// kademlia. Admission is decided here, synchronously, because the `Ok` this method returns
    /// is what lets the batch builder evict these transactions from its pool as mined: a batch
    /// that was never handed to a forward task must report [`BlockSealError::NotValidator`] so
    /// the builder keeps its transactions pending and retries on a later build. Delivery stays
    /// best-effort on a background task, so batch production is never stalled by a slow or
    /// unreachable validator; discovery is a snapshot of already-fetched kademlia records, not
    /// a blocking network round-trip.
    pub async fn disburse_txns(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let transactions = sealed_batch.batch.transactions;
        if transactions.is_empty() {
            return Ok(());
        }

        // Whole-batch count for the discovery dead ends below (issue #1133).
        let num_txns = transactions.len();
        let validator_rpcs = self
            .network_handle
            .get_all_validator_rpcs()
            .await
            .inspect_err(|err| {
                warn!(
                    target: "worker::batch_provider",
                    ?err,
                    "failed to discover validator JSON-RPC endpoints for transaction forwarding"
                );
                self.metrics.record_forward_dropped(ForwardDropReason::DiscoveryFailed, num_txns);
            })
            .inspect(|rpcs| {
                // Only the discovery-succeeded-but-empty case earns this message; a
                // discovery failure already warned above with the actual error.
                if rpcs.is_empty() {
                    warn!(
                        target: "worker::batch_provider",
                        "no committee validator has advertised a JSON-RPC endpoint; \
                         cannot forward accepted transactions"
                    );
                    self.metrics
                        .record_forward_dropped(ForwardDropReason::NoEndpointAdvertised, num_txns);
                }
            })
            .unwrap_or_default();

        let admitted = !validator_rpcs.is_empty()
            && self.forwarder.forward_txns(
                transactions,
                self.committee_slots.clone(),
                validator_rpcs,
            );
        admitted.then_some(()).ok_or(BlockSealError::NotValidator)
    }

    /// Seal and broadcast the current batch, treating empty batches as a successful no-op.
    #[instrument(level = "debug", skip_all, fields(batch_size = sealed_batch.size(), num_txs = sealed_batch.batch.transactions.len()))]
    pub async fn seal(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        if sealed_batch.batch.transactions.is_empty() {
            Ok(())
        } else {
            self.seal_non_empty(sealed_batch).await
        }
    }

    /// Forward or attest a batch after `seal` has confirmed it contains transactions.
    async fn seal_non_empty(&self, sealed_batch: SealedBatch) -> Result<(), BlockSealError> {
        let Some(quorum_waiter) = &self.quorum_waiter else {
            // We are not a validator so need to send any transactions out for a CVV to pickup.
            let digest = sealed_batch.digest();
            return self.disburse_txns(sealed_batch).await.inspect(|()| {
                self.local_recovery.observer_accepted(digest);
            });
        };

        // the proposer exits on this signal, so a batch sealed now could never be reported;
        // refuse before asking peers to validate and store it
        if self.consensus_shutdown.as_ref().is_some_and(ShutdownNotifier::is_notified) {
            return Err(BlockSealError::ConsensusShuttingDown);
        }

        let _seal_guard = self
            .local_seal_locks
            .acquire(sealed_batch.digest())
            .await
            .ok_or(BlockSealError::FailedQuorum)?;
        let worker = self.clone();
        let cached = sealed_batch.clone();
        let inserted = tokio::task::spawn_blocking(move || {
            worker.cache_local_batch(&cached.digest(), cached.batch())
        })
        .await
        .map_err(|_| BlockSealError::FatalDBFailure)??;
        self.store
            .persist::<OurNodeBatchesCache>()
            .await
            .map_err(|_| BlockSealError::FatalDBFailure)?;
        let batch_attest_handle = quorum_waiter.verify_batch(
            sealed_batch.clone(),
            self.timeout,
            self.network_handle.get_task_spawner(),
        );

        let (batch, digest) = sealed_batch.split();

        // Wait for our batch to reach quorum or fail to do so.
        match batch_attest_handle.await {
            Ok(res) => {
                match res {
                    Ok(()) => {
                        // batch reached quorum!
                        // logged for every seal that reaches quorum; the batch metrics wait for
                        // the report below, so a seal whose store or report fails is not counted
                        info!(
                            target: "consensus::metrics",
                            worker_id = self.id,
                            batch_size = batch.size(),
                            num_txs = batch.transactions.len(),
                            "batch sealed"
                        );
                        // Publish the digest for the nodes subscribed to this gossip, i.e. the
                        // committee validators that consume individual current-epoch batches.
                        // Note, ignore error- this should not
                        // happen and should not cause an issue (except the
                        // underlying p2p network may be in trouble but that will manifest quickly).
                        let _ = self.network_handle.publish_batch(digest).await;
                    }
                    Err(e) => {
                        // On error the batch builder should leave the transactions in the pool for
                        // a future batch. So go ahead and remove so we
                        // don't try to re-inject them later.
                        if inserted {
                            let _ = self.store.remove::<OurNodeBatchesCache>(&digest);
                        }
                        return Err(match e {
                            crate::quorum_waiter::QuorumWaiterError::QuorumRejected => {
                                BlockSealError::QuorumRejected
                            }
                            crate::quorum_waiter::QuorumWaiterError::AntiQuorum => {
                                BlockSealError::AntiQuorum
                            }
                            crate::quorum_waiter::QuorumWaiterError::Timeout => {
                                BlockSealError::Timeout
                            }
                            crate::quorum_waiter::QuorumWaiterError::Network
                            | crate::quorum_waiter::QuorumWaiterError::DroppedReceiver
                            | crate::quorum_waiter::QuorumWaiterError::Rpc(_) => {
                                BlockSealError::FailedQuorum
                            }
                        });
                    }
                }
            }
            Err(e) => {
                error!(target: "worker::batch_provider", "Join error attempting batch quorum! {e}");
                // See remove comment above.
                if inserted {
                    let _ = self.store.remove::<OurNodeBatchesCache>(&digest);
                }
                return Err(BlockSealError::FailedQuorum);
            }
        }

        // Save to live batch storage so other nodes can fetch it.
        // Keep OurNodeBatchesCache entry intact: if the epoch ends before this batch's cert is
        // committed, orphan_batches() will re-inject the transactions at the next epoch start.
        // Already-executed transactions are rejected by the pool (nonce too low), so re-injection
        // is always safe.
        if let Err(e) = self.store.insert::<NodeBatchesCache>(&digest, &batch) {
            error!(target: "worker::batch_provider", "Store failed with error: {:?}", e);
            return Err(BlockSealError::FatalDBFailure);
        }

        // Send the batch to the primary.
        let message = WorkerOwnBatchMessage::new(self.id, digest);
        if let Err(err) = self.client.report_own_batch(message).await {
            error!(target: "worker::batch_provider", "Failed to report our batch: {err:?}");
            Err(BlockSealError::FailedToReport)
        } else {
            // recorded only after a successful report: a failed report leaves the transactions
            // in the pool for the builder to seal again, and an earlier record would count them
            // once per attempt (issue #1444)
            self.metrics.record_batch_sealed(batch.size(), batch.transactions.len());
            Ok(())
        }
    }
}
