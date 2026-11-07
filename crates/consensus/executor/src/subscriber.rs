//! Subscriber gathers all information needed from consensus and forwards to the execution engine.

use crate::{errors::SubscriberResult, metrics::ExecutorMetrics, SubscriberError};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use metrics::Counter;
use state_sync::{last_consensus_parent, save_consensus, spawn_state_sync};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::ConsensusConfig;
use tn_network_types::PrimaryToWorkerClient;
use tn_primary::{
    network::PrimaryNetworkHandle, ConsensusBus, ConsensusBusApp, ExecutionCheckError, NodeMode,
};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    encode, to_intent_message, Address, AuthorityIdentifier, Batch, BlockHash, BlsSigner as _,
    CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusOutput, ConsensusResult, Database, Epoch, EpochRecord, Hash as _, Noticer, Round,
    TaskManager, TaskSpawner, Timestamp, TimestampSec, TnReceiver, TnSender, WorkerId,
};
use tracing::{debug, error, info, instrument, warn};

/// Interval between stall warnings while a worker's batch-fetch leg is outstanding.
///
/// The fetch itself is deliberately unbounded: the sub-dag is committed, so a quorum holds
/// its batches and `BatchFetcher::fetch_for_primary` retries until they arrive. Cancelling
/// on a deadline would fail-stop the node during a transient outage (a restart re-enters
/// the identical wait, a crash loop exactly when the node is furthest behind). The watchdog
/// keeps the unbounded wait but makes a stall loud instead of silent.
const FETCH_BATCHES_STALL_WARN_INTERVAL: Duration = Duration::from_secs(300);

/// Maximum age of the newest known consensus header for a catching-up CVV to treat it as the
/// network head and rejoin consensus.
///
/// Under load the committee commits often, so the newest gossiped header stays recent. A header
/// older than this is a stale record (for example one loaded at restart) and must not promote a
/// node that has not caught up.
const REJOIN_HEAD_MAX_AGE: Duration = Duration::from_secs(5);

/// Minimum interval between two `rejoin not ready` lines for the same [`RejoinBlocker`].
///
/// A catching-up CVV checks readiness after every output it handles, hundreds a second while it
/// replays, so the line is logged when the blocker changes and otherwise at most this often.
const REJOIN_NOT_READY_LOG_INTERVAL: Duration = Duration::from_secs(2);

/// Shortest wait for local execution that a committed sub-dag logs before its output is assembled.
///
/// Under load an output waits a few hundred milliseconds for the engine, so only a longer wait
/// (an execution backlog) is worth a line.
const EXECUTION_WAIT_LOG_THRESHOLD: Duration = Duration::from_secs(1);

/// Only an output from this subscriber's still-open epoch can establish rejoin readiness.
/// Future outputs are ignored by `handle_sync_output`, and finalized epochs must be replayed
/// through their boundary even if their final headers are recent. The synthetic epoch-zero
/// record has no final consensus block and does not mark that epoch as closed.
fn rejoin_epoch_is_open(
    current_epoch: Epoch,
    output_epoch: Epoch,
    latest_record: Option<&EpochRecord>,
) -> bool {
    output_epoch == current_epoch
        && latest_record.is_none_or(|record| {
            record.epoch < current_epoch
                || (record.epoch == 0 && record.final_consensus.number == 0)
        })
}

/// True when a catching-up CVV is close enough to the network head to rejoin consensus.
///
/// `handled_epoch` and `handled_round` belong to the consensus output the node just handled.
/// `newest_epoch`, `newest_round` and `newest_age` belong to the newest consensus header the node
/// knows about. The rule has three parts:
/// - the newest known header is younger than [`REJOIN_HEAD_MAX_AGE`]. Freshness is checked on the
///   newest known header, not on the handled output, so a stale record still blocks promotion;
/// - both are in the same epoch, because rounds only compare inside one epoch;
/// - the handled output trails the newest known header by at most `rejoin_window` rounds
///   ([`tn_types::rejoin_round_window`]). A handled output at or ahead of the newest known header
///   has a gap of zero.
///
/// The old rule required the handled output to be the newest known header. Under sustained load
/// a node that must fetch, verify and execute every output is always a few outputs behind, so
/// that rule never held until the load dropped.
fn rejoin_ready(
    handled_epoch: Epoch,
    handled_round: Round,
    newest_epoch: Epoch,
    newest_round: Round,
    newest_age: Duration,
    rejoin_window: Round,
) -> bool {
    newest_age < REJOIN_HEAD_MAX_AGE
        && handled_epoch == newest_epoch
        && newest_round.saturating_sub(handled_round) <= rejoin_window
}

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
#[derive(Clone, Debug)]
pub struct Subscriber<DB> {
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBusApp,
    /// Consensus configuration (contains the consensus DB)
    config: ConsensusConfig<DB>,
    /// The handle to the network.
    network_handle: PrimaryNetworkHandle,
    /// Inner state.
    inner: Arc<Inner>,
}

/// Inner subscriber type.
#[derive(Debug)]
struct Inner {
    /// The identifier for the authority.
    ///
    /// Used for logging, None if we are not a validator.
    authority_id: Option<AuthorityIdentifier>,
    /// The committee for the epoch.
    committee: Committee,
    /// Access to the consensus chain data.
    consensus_chain: ConsensusChain,
    /// Epoch boundary time.
    epoch_boundary: TimestampSec,
    /// Prometheus metrics for consensus output assembly.
    metrics: ExecutorMetrics,
}

/// Spawn the subscriber in the correct mode based on the validator status for the current epoch.
pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    task_manager: &TaskManager,
    network_handle: PrimaryNetworkHandle,
    consensus_chain: ConsensusChain,
    epoch_boundary: TimestampSec,
) {
    let authority_id = config.authority_id();
    let committee = config.committee().clone();
    let mode = consensus_bus.app().current_node_mode();
    info!(target: "tn::observer", node_mode = ?mode, "subscriber starting in mode");
    let subscriber = Subscriber {
        consensus_bus: consensus_bus.app().clone(),
        config,
        network_handle,
        inner: Arc::new(Inner {
            authority_id,
            committee,
            consensus_chain: consensus_chain.clone(),
            epoch_boundary,
            metrics: ExecutorMetrics::default(),
        }),
    };
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {
            // Subscribe before spawning so the channel is active before any messages are sent.
            let rx_sequence = consensus_bus.subscribe_sequence();
            task_manager.spawn_critical_task("subscriber consensus", async move {
                info!(target: "subscriber", "Starting subscriber: CVV");
                if let Err(e) = subscriber.run(rx_shutdown, rx_sequence, consensus_chain).await {
                    error!(target: "subscriber", "Error subscriber consensus: {e}");
                    Err(e.into())
                } else {
                    Ok(())
                }
            });
        }
        NodeMode::CvvInactive => {
            let clone = task_manager.get_spawner();
            // If we are not active but are a CVV then catch up and rejoin.
            task_manager.spawn_critical_task(
                "subscriber catch up and rejoin consensus",
                async move {
                    info!(target: "subscriber", "Starting subscriber: Catch up and rejoin");
                    if let Err(e) = subscriber.catch_up_rejoin_consensus(clone).await {
                        error!(target: "subscriber", "Error catching up consensus: {e}");
                        Err(e.into())
                    } else {
                        Ok(())
                    }
                },
            );
        }
        NodeMode::Observer => {
            let clone = task_manager.get_spawner();
            // If we are not active then just follow consensus.
            task_manager.spawn_critical_task("subscriber follow consensus", async move {
                info!(target: "subscriber", "Starting subscriber: Follower");
                if let Err(e) = subscriber.follow_consensus(clone).await {
                    error!(target: "subscriber", "Error following consensus: {e}");
                    Err(e.into())
                } else {
                    Ok(())
                }
            });
        }
    }
}

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Save a verified, fully-formed ConsensusOutput (delivered by the state-sync forward drain)
    /// and send it down the consensus_output channel for execution.
    ///
    /// The output arrives complete (header + batches) and already verified by state-sync, so there
    /// is no separate batch fetch here.
    #[instrument(level = "debug", skip_all, fields(number = consensus_output.number()))]
    async fn handle_sync_output(&self, consensus_output: ConsensusOutput) -> SubscriberResult<()> {
        if consensus_output.sub_dag().leader_epoch() > self.inner.committee.epoch() {
            // Do not process past our epoch.  Can just NO-OP here to avoid producing bogus output
            // before run_epoch() winds down.
            return Ok(());
        }
        let number = consensus_output.number();
        let latest = self.inner.consensus_chain.latest_consensus_number();
        match classify_sync_output(latest, number) {
            SyncContinuity::Stale => {
                debug!(target: "subscriber", number, latest, "skipping already saved sync output");
                return Ok(());
            }
            SyncContinuity::Gap => {
                error!(target: "subscriber", number, latest, "sync output does not follow the latest saved consensus output");
                return Err(SubscriberError::SyncOutputGap {
                    expected: latest + 1,
                    received: number,
                });
            }
            SyncContinuity::Next => {}
        }

        let mut consensus_chain = self.inner.consensus_chain.clone();
        // This save will essentially mark this consensus output as written in stone (added to the
        // consensus chain). This does NOT imply execution although it will be sent off for
        // execution.
        save_consensus(
            consensus_output.clone(),
            &mut consensus_chain,
            self.consensus_bus.metrics(),
        )
        .await?;

        // Once we've drained through the staged partial pack's final output, it has all been
        // written to the main pack in order — drop the staging dir. `clear_staging` is async so
        // sealing the staging pack (on the blocking pool) does not block this tokio worker.
        if self.inner.consensus_chain.staging_final() == Some(number) {
            self.inner.consensus_chain.clear_staging().await;
        }

        let last_round = consensus_output.leader_round();

        // We aren't doing consensus now but still need to update these watches before
        // we send the consensus output.
        self.consensus_bus.committed_round_updates().send_replace(last_round);
        self.consensus_bus.primary_round_updates().send_replace(last_round);

        // ExEx delivery runs on the consensus-following path only — Observer
        // (`follow_consensus`) and inactive CVV (`catch_up_rejoin_consensus`) both
        // reach this method, neither runs Bullshark. Hand the full reconstructed
        // output to any installed ExEx. The active-validator path
        // (`handle_consensus_output`) deliberately omits this so validators bear
        // no ExEx overhead.
        self.consensus_bus.notify_exex_consensus_output(&consensus_output);

        if let Err(e) = self.consensus_bus.consensus_output().send(consensus_output).await {
            error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
            return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
        }
        Ok(())
    }

    /// Catch up to current consensus and then try to rejoin as an active CVV.
    /// Keep replaying finalized epochs even when their final headers still count as fresh.
    async fn catch_up_rejoin_consensus(&self, tasks: TaskSpawner) -> SubscriberResult<()> {
        // Get a receiver and then stream any missing outputs so we don't miss them.
        let mut rx_sync_output = self.consensus_bus.subscribe_sync_output();
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            tasks,
            self.inner.consensus_chain.clone(),
        );
        // Derived from the same helper as the demotion window in `behind_consensus`, so a node
        // that rejoins is not demoted again at once.
        let rejoin_window = tn_types::rejoin_round_window(self.config.parameters().gc_depth);
        // readiness is checked after every handled output, so the counter handles are resolved
        // once here rather than per check
        let blocked_counters = rejoin_blocked_counters();
        let mut not_ready_log = RejoinLogThrottle::default();
        while let Some(output) = rx_sync_output.recv().await {
            let consensus_header_number = output.number();
            let handled_epoch = output.sub_dag().leader_epoch();
            let handled_round = output.leader_round();
            self.handle_sync_output(output).await?;
            // A recent header can belong to an epoch that has already closed. Activating there
            // stops replay before its boundary, leaving this node proposing to peers in the next
            // epoch until it is demoted again. Apply the output, but keep following consensus.
            let latest_record = self.inner.consensus_chain.epochs().latest_record().await;
            let epoch_is_open = rejoin_epoch_is_open(
                self.inner.committee.epoch(),
                handled_epoch,
                latest_record.as_ref(),
            );
            // freshness is measured in milliseconds so a sub-second commit time is not rounded
            // away. a pre-fork commit time is whole seconds, and there this equals comparing
            // whole seconds against the clock rounded down to the second
            let newest =
                self.consensus_bus.last_consensus_header().borrow().as_ref().map(|header| {
                    NewestHeader {
                        number: header.number,
                        epoch: header.sub_dag.leader_epoch(),
                        round: header.sub_dag.leader_round(),
                        age: header.sub_dag.commit_timestamp_ms().elapsed(),
                    }
                });
            if let Some(newest) = &newest {
                // Rejoin when the handled output is within the rejoin window of the newest known
                // header and that header is fresh. The freshness check stops a stale record from
                // promoting a node that has not caught up.
                if epoch_is_open
                    && rejoin_ready(
                        handled_epoch,
                        handled_round,
                        newest.epoch,
                        newest.round,
                        newest.age,
                        rejoin_window,
                    )
                {
                    let (exec_round, exec_number) = self.exec_progress();
                    // We are caught up enough so try to jump back into consensus
                    info!(
                        target: "subscriber",
                        consensus_header_number,
                        newest_number = newest.number,
                        round_gap = newest.round.saturating_sub(handled_round),
                        rejoin_window,
                        handled_round,
                        newest_round = newest.round,
                        exec_round,
                        exec_number,
                        "attempting to rejoin consensus"
                    );
                    let previous = self.consensus_bus.node_mode().send_replace(NodeMode::CvvActive);
                    self.consensus_bus.metrics().record_rejoin_transition(previous);
                    self.config.shutdown().notify();
                    return Ok(());
                }
            }
            let reason = RejoinBlocker::classify(epoch_is_open, newest.as_ref(), handled_epoch);
            blocked_counters[reason.index()].increment(1);
            if not_ready_log.should_log(reason, Instant::now()) {
                // a node with no known header logs zeros for the newest header's fields
                let (newest_epoch, newest_round, newest_age_ms) =
                    newest.as_ref().map_or((0, 0, 0), |newest| {
                        (
                            newest.epoch,
                            newest.round,
                            u64::try_from(newest.age.as_millis()).unwrap_or(u64::MAX),
                        )
                    });
                info!(
                    target: "subscriber",
                    consensus_header_number,
                    handled_epoch,
                    handled_round,
                    newest_epoch,
                    newest_round,
                    newest_age_ms,
                    rejoin_window,
                    exec_round = self.consensus_bus.last_consensus_round(),
                    reason = reason.as_str(),
                    "rejoin not ready"
                );
            }
        }
        Ok(())
    }

    /// The leader round and consensus number of the last output the engine processed, executed or
    /// skipped as empty.
    fn exec_progress(&self) -> (Round, u64) {
        let recent = self.consensus_bus.recent_blocks().borrow();
        (recent.last_consensus_round(), recent.latest_consensus_block_num_hash().number)
    }

    /// Follow along with consensus output but do not try to join consensus.
    async fn follow_consensus(&self, tasks: TaskSpawner) -> SubscriberResult<()> {
        // Get a receiver then stream any missing outputs so we don't miss them.
        let mut rx_sync_output = self.consensus_bus.subscribe_sync_output();
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            tasks,
            self.inner.consensus_chain.clone(),
        );
        let mut processed_count: u64 = 0;
        while let Some(output) = rx_sync_output.recv().await {
            let header_number = output.number();
            self.handle_sync_output(output).await?;
            processed_count += 1;

            // Periodically log observer progress (every 100 blocks)
            if processed_count.is_multiple_of(100) {
                let latest_processed_consensus = self
                    .consensus_bus
                    .recent_blocks()
                    .borrow()
                    .latest_consensus_block_num_hash()
                    .number;
                let (_latest_network_epoch, latest_network_consensus, _) =
                    self.consensus_bus.published_consensus_num_hash();
                let consensus_sync_distance =
                    latest_network_consensus.saturating_sub(latest_processed_consensus);
                info!(
                    target: "tn::observer",
                    processed_count,
                    header_number,
                    latest_processed_consensus,
                    latest_network_consensus,
                    consensus_sync_distance,
                    "observer follow progress"
                );
            }
        }
        warn!(target: "tn::observer", "consensus header channel closed - observer stopped following");
        Ok(())
    }

    /// Return the block hash and number of the last executed consensus output.
    ///
    /// This method is called on startup to retrieve the needed information to build the next
    /// `ConsensusHeader` off of this parent. A failed storage lookup propagates so startup
    /// fail-stops instead of numbering new output from a silently-defaulted parent at 0.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(ConsensusHeaderDigest, u64)> {
        let result = last_consensus_parent(&self.consensus_bus, &self.inner.consensus_chain)
            .await
            .map_err(|error| SubscriberError::ConsensusChainRead(format!("{error:#}")))?;

        info!(
            target: "subscriber",
            parent_hash = ?result.0,
            parent_number = result.1,
            "restoring last executed consensus for constructing the next ConsensusHeader:"
        );

        Ok(result)
    }

    /// Save consensus output and publish or signature.
    async fn handle_consensus_output(
        &self,
        consensus_chain: &mut ConsensusChain,
        output: ConsensusOutput,
    ) -> SubscriberResult<()> {
        debug!(target: "subscriber", output=?output.digest(), "saving next output");
        save_consensus(output.clone(), consensus_chain, self.consensus_bus.metrics()).await?;
        debug!(target: "subscriber", "broadcasting output...");
        // Publish the consensus result now that we are totally finished.
        let number = output.number();
        let this_digest = output.consensus_header_hash();

        // Record the latest ConsensusHeader, we probably don't need this in this mode but keep it
        // up to date anyway. Note we don't bother sending this to the consensus header
        // channel since not needed when an active CVV.
        self.consensus_bus.last_consensus_header().send_replace(Some(output.consensus_header()));
        let epoch = output.sub_dag().leader_epoch();
        let round = output.sub_dag().leader_round();
        let consensus_result_hash = ConsensusResult::digest_data(epoch, round, number, this_digest);
        let sig = self
            .config
            .key_config()
            .request_signature_direct(&encode(&to_intent_message(consensus_result_hash)));
        if let Err(e) = self
            .network_handle
            .publish_consensus(
                epoch,
                round,
                number,
                this_digest,
                self.config.key_config().public_key(),
                sig,
            )
            .await
        {
            error!(target: "subscriber", "error publishing latest consensus to network {:?}: {}", self.inner.authority_id, e);
        }
        if let Err(e) = self.consensus_bus.consensus_output().send(output).await {
            error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
            return Err(SubscriberError::ClosedChannel(
                "failed to broadcast consensus output".to_string(),
            ));
        }
        debug!(target: "subscriber", "output broadcast successfully");
        Ok(())
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    #[instrument(level = "info", skip_all, fields(authority = ?self.inner.authority_id))]
    async fn run(
        self,
        rx_shutdown: Noticer,
        mut rx_sequence: impl TnReceiver<CommittedSubDag>,
        mut consensus_chain: ConsensusChain,
    ) -> SubscriberResult<()> {
        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_sequence. So it doesn't
        // matter if we somehow managed to fetch the blocks from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        let (mut last_parent, mut last_number) = self.get_last_executed_consensus().await?;
        // running maximum of the received leaders' execution blocks (see `next_wait_floor`)
        let mut wait_floor = 0;

        let mut epoch_done = false;
        // rx_sequence is now passed as parameter to avoid race condition
        // Listen to sequenced consensus message and process them.
        loop {
            // Use biased select so shutdown is checked last. This ensures that if a
            // completed output and shutdown are both ready, the output is saved first,
            // preventing loss of committed consensus data during graceful shutdown.
            tokio::select! {
                biased;

                // Receive the ordered sequence of consensus messages from a consensus node.
                Some(sub_dag) = rx_sequence.recv(), if !epoch_done && waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                    // Once we cross epoch boundary then process this last output then we are done.
                    if sub_dag.reaches_epoch_boundary(self.inner.epoch_boundary) { epoch_done = true; }
                    debug!(target: "subscriber", subdag=?sub_dag.digest(), round=?sub_dag.leader_round(), "received committed subdag from consensus");
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    let parent_hash = last_parent;
                    let number = last_number + 1;
                    last_number += 1;
                    last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
                    wait_floor = next_wait_floor(wait_floor, &sub_dag);
                    waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number, wait_floor));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: `consensus_output` is a bounded queue, so while it is full the send in
                // `handle_consensus_output` waits, and this select, including the shutdown arm, is
                // not polled until that send completes or the epoch's tasks are aborted.
                Some(output) = waiting.next() => {
                    match output {
                        Ok(output) => self.handle_consensus_output(&mut consensus_chain, output).await?,
                        Err(SubscriberError::ExecutionFork {
                            number,
                            base_block,
                            base_hash,
                            leader,
                            leader_round,
                        }) => {
                            error!(
                                target: "subscriber",
                                number,
                                base_block,
                                %base_hash,
                                %leader,
                                leader_round,
                                "leader's execution block is not on the local chain; execution has forked, going inactive"
                            );
                            let previous =
                                self.consensus_bus.node_mode().send_replace(NodeMode::CvvInactive);
                            self.consensus_bus.metrics().record_execution_fork_transition(previous);
                            self.config.shutdown().notify();
                            // not an error (that would fail this critical task) and not through
                            // the shutdown drain (that would save outputs past the fork)
                            return Ok(());
                        }
                        Err(e) => {
                            error!(target: "subscriber", "error fetching batches: {e}");
                            // Failure to fetch batches is a fatal condition, return an error which will trigger node shutdown.
                            return Err(e);
                        }
                    }
                },

                _ = &rx_shutdown => {
                    // Drain any pending consensus outputs to prevent data loss.
                    // Without this, committed subdags whose batch downloads are in-flight
                    // would be lost on restart, causing consensus chain divergence.
                    drain_pending_on_shutdown(
                        &self.consensus_bus,
                        &mut consensus_chain,
                        waiting,
                        Duration::from_secs(3),
                    )
                    .await;
                    return Ok(())
                }

            }
        }
    }

    /// Helper function to obtain authority's execution address based on their
    /// `AuthorityIdentifier`. This address is used as the beneficiary during batch execution.
    /// A fatal error is returned if the authority is missing from the committee.
    fn authority_execution_address(
        &self,
        authority_id: &AuthorityIdentifier,
    ) -> SubscriberResult<Address> {
        self.inner
            .committee
            .authority(authority_id)
            .map(|a| a.execution_address())
            .ok_or(SubscriberError::UnexpectedAuthority(authority_id.clone()))
            .inspect_err(|_| {
                error!(target: "subscriber", ?authority_id, "Authority missing from committee");
            })
    }

    /// Turn a CommittedSubDag with consensus header info into ConsensusOutput.
    /// It will retrieve any missing Batches so the ConsensusOutput will be ready
    /// to execute.
    /// Note, an error here is BAD and will most likely cause node shutdown (clean).  Do
    /// not provide a bogus sub dag...
    ///
    /// Before anything else, empty sub-dags included, it waits for local execution to reach
    /// `wait_floor` and checks the leader's execution block (see
    /// [`Self::wait_for_leader_execution`]), so an output is only assembled, and later saved, once
    /// its leader passed the fork check.
    #[instrument(level = "debug", skip_all, fields(number))]
    async fn fetch_batches(
        &self,
        sub_dag: CommittedSubDag,
        parent_hash: ConsensusHeaderDigest,
        number: u64,
        wait_floor: u64,
    ) -> SubscriberResult<ConsensusOutput> {
        self.wait_for_leader_execution(&sub_dag, number, wait_floor).await?;

        let num_blocks = sub_dag.num_primary_batches();
        let num_certs = sub_dag.len();

        if num_blocks == 0 {
            debug!(target: "subscriber", "No blocks to fetch, payload is empty");
            return Ok(ConsensusOutput::new_with_subdag(sub_dag, parent_hash, number));
        }

        // Partition the payload per worker id so each digest is fetched through the local
        // network instance of the worker that owns it (dedup within a worker via the set).
        let mut batches_by_worker: BTreeMap<WorkerId, BTreeSet<BlockHash>> = BTreeMap::new();

        let mut batch_digests = VecDeque::with_capacity(num_certs);
        for header in sub_dag.headers() {
            for (digest, worker_id) in header.payload().iter() {
                batches_by_worker.entry(*worker_id).or_default().insert(*digest);
                batch_digests.push_back(*digest);
            }
        }

        // `MAX_GC_DEPTH` is the garbage-collection horizon, not the depth a commit reaches: a
        // sub-DAG is usually only a few rounds deep (`order_dag` descends only to `gc_round + 1`
        // and skips already-committed rounds), but because no certificate at or below
        // `gc_round` is ever committed, a committed sub-DAG spans at most `MAX_GC_DEPTH`
        // distinct rounds as a safe over-estimate.  With at most one certificate per
        // authority per round and at most `MAX_HEADER_NUM_OF_BATCHES` batches per header,
        // `batch_set` holds at most `committee.size() * MAX_GC_DEPTH *
        // MAX_HEADER_NUM_OF_BATCHES` unique digests.  The consensus-pack reader
        // (`max_batches_per_output`) uses that same bound, so every output built here can
        // later be reconstructed from pack storage.
        let mut fetched_batches = self.fetch_batches_from_peers(batches_by_worker).await?;

        let mut batches = Vec::with_capacity(num_certs);
        // map all fetched batches to their respective certificates for applying block rewards
        for header in sub_dag.headers() {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(header.payload().len());

            // retrieve fetched batch by digest
            for digest in header.payload().keys() {
                debug!(
                    target: "subscriber",
                    ?digest,
                    "fetching batch",
                );

                let batch = fetched_batches.remove(digest);
                if let Some(batch) = batch {
                    debug!(
                        target: "subscriber",
                        "Adding fetched batch {digest} from certificate {} to consensus output",
                        header.digest()
                    );

                    cert_batches.push(batch);
                } else {
                    // if the batch is a duplicate, the engine will ignore
                    if let Some(batch) = batches
                        .iter()
                        .flat_map(|cb: &CertifiedBatch| cb.batches.iter())
                        .chain(cert_batches.iter())
                        .find(|b| b.digest() == *digest)
                    {
                        warn!(target: "subscriber", ?digest, ?batch_digests, "failed to remove fetched batch - duplicate");
                        #[cfg(not(feature = "adiri"))]
                        cert_batches.push(batch.clone());

                        #[cfg(feature = "adiri")]
                        if sub_dag.leader_epoch() > tn_types::forks::ADIRI_DUP_BATCH_EPOCH {
                            // ADIRI BUG
                            // Epoch 74 and possibly other early epochs of adiri testnet had a bug
                            // with duplicate batches. We have to
                            // recreate it in order to sync testnet so we skip this push
                            // on adiri with early epochs.
                            cert_batches.push(batch.clone());
                        }
                    } else {
                        error!(target: "subscriber", ?digest, "[Protocol violation] Batch not found in fetched batches from workers of certificate signers");
                        self.inner.metrics.protocol_violations_total.increment(1);
                        return Err(SubscriberError::MissingFetchedBatch(*digest));
                    }
                }
            }

            // main collection for execution
            batches.push(CertifiedBatch {
                address: self.authority_execution_address(header.author())?,
                batches: cert_batches,
            });
        }
        // Count total transactions across all batches
        let total_txs: usize =
            batches.iter().flat_map(|cb| cb.batches.iter()).map(|b| b.transactions.len()).sum();

        // Metric: consensus_output_ready - tracks consensus output ready for execution
        info!(
            target: "consensus::metrics",
            number = number,
            leader_round = sub_dag.leader_round(),
            num_certs = num_certs,
            num_batches = batch_digests.len(),
            total_txs = total_txs,
            "consensus output ready"
        );
        self.inner.metrics.outputs_ready_total.increment(1);
        self.inner.metrics.output_transactions.record(total_txs as f64);
        self.inner.metrics.output_batches.record(batch_digests.len() as f64);

        debug!(target: "subscriber", "returning output to subscriber");
        Ok(ConsensusOutput::new(
            sub_dag.clone(),
            parent_hash,
            number,
            false,
            batch_digests,
            batches,
        ))
    }

    /// Wait until local execution reaches block `wait_floor`, then check that the leader's
    /// execution block is the block this node executed at that height.
    ///
    /// `wait_floor` is at least the leader's own block number: it is the running maximum over
    /// every leader received so far (see [`next_wait_floor`]), so the outputs pass this wait in
    /// consensus order and an output led by a node whose execution trails this one's does not
    /// pass early and sit fully assembled behind a slower predecessor. Every leader's block comes
    /// from an earlier output, so executing the outputs ahead of this one always reaches the
    /// floor. The check itself is [`ConsensusBusApp::wait_for_execution`], the one each vote
    /// makes.
    ///
    /// Returns [`SubscriberError::ExecutionFork`] only when this node holds a different block at
    /// the leader's number. A block below this node's restored-state floor cannot be checked here:
    /// it is logged at warn and the output goes on, because the voters that certified the leader
    /// checked it against their own chains.
    async fn wait_for_leader_execution(
        &self,
        sub_dag: &CommittedSubDag,
        number: u64,
        wait_floor: u64,
    ) -> SubscriberResult<()> {
        let started = Instant::now();
        let base = sub_dag.leader().latest_execution_block();
        let mut rx_recent_blocks = self.consensus_bus.recent_blocks().subscribe();
        rx_recent_blocks
            .wait_for(|blocks| blocks.latest_execution_block_num_hash().number >= wait_floor)
            .await
            .map(|_| ())
            .map_err(|_| SubscriberError::ClosedChannel("recent_blocks".to_string()))?;
        let checked = self.consensus_bus.wait_for_execution(base).await;
        let waited = started.elapsed();
        self.inner.metrics.execution_wait_seconds.record(waited.as_secs_f64());
        if waited > EXECUTION_WAIT_LOG_THRESHOLD {
            info!(
                target: "subscriber",
                number,
                leader_round = sub_dag.leader_round(),
                base_block = base.number,
                executed_block = self.consensus_bus.latest_execution_block_num_hash().number,
                waited_ms = u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                "waited for execution before assembling the output"
            );
        }
        match checked {
            Ok(()) => Ok(()),
            Err(ExecutionCheckError::BelowFloor { floor }) => {
                warn!(
                    target: "subscriber",
                    number,
                    leader_round = sub_dag.leader_round(),
                    base_block = base.number,
                    floor,
                    "leader's execution block is below the restored-state floor; not checked"
                );
                Ok(())
            }
            Err(ExecutionCheckError::Fork) => Err(SubscriberError::ExecutionFork {
                number,
                base_block: base.number,
                base_hash: base.hash,
                leader: sub_dag.leader().author().clone(),
                leader_round: sub_dag.leader_round(),
            }),
            Err(ExecutionCheckError::Closed) => {
                Err(SubscriberError::ClosedChannel("recent_blocks".to_string()))
            }
        }
    }

    /// Warn every [`FETCH_BATCHES_STALL_WARN_INTERVAL`] while a worker's fetch leg is
    /// outstanding.
    ///
    /// Never resolves; the caller races it against the fetch itself, so it only ever adds
    /// log lines and cannot cancel the fetch.
    async fn stall_watchdog(worker_id: WorkerId) -> std::convert::Infallible {
        futures::stream::unfold((), |()| async {
            tokio::time::sleep(FETCH_BATCHES_STALL_WARN_INTERVAL).await;
            Some(((), ()))
        })
        .for_each(|()| {
            warn!(
                target: "subscriber",
                worker_id,
                "batch fetch from worker still outstanding; waiting on local db or peers"
            );
            futures::future::ready(())
        })
        .await;
        // the unfold stream above is infinite, so `for_each` never completes
        futures::future::pending().await
    }

    /// Send message to relevant workers to fetch batches for execution.
    ///
    /// One request per worker id, over that worker's local network instance; the worker is
    /// responsible for retrieving the batch from its local DB or fetching from peers. The
    /// legs run concurrently, each raced against a stall watchdog that warns every
    /// [`FETCH_BATCHES_STALL_WARN_INTERVAL`] without cancelling, and any leg's real error
    /// fails the whole fetch (an incomplete output can never execute).
    async fn fetch_batches_from_peers(
        &self,
        batches_by_worker: BTreeMap<WorkerId, BTreeSet<BlockHash>>,
    ) -> SubscriberResult<HashMap<BlockHash, Batch>> {
        let num_digests: usize = batches_by_worker.values().map(BTreeSet::len).sum();
        debug!(target: "subscriber", "Attempting to fetch {num_digests} digests from workers");

        let legs = batches_by_worker.into_iter().map(|(worker_id, digests)| {
            // Sub-dag payloads passed `Header::validate`'s worker-id bounds check, so a
            // missing instance means the committee's worker set and this config disagree:
            // a protocol-level failure, surfaced with the variant that exists for it.
            let client = self.config.local_network(worker_id).cloned();
            async move {
                let client = client.ok_or(SubscriberError::UnexpectedWorkerId(worker_id))?;
                let fetch = std::pin::pin!(client.fetch_batches(digests));
                let watchdog = std::pin::pin!(Self::stall_watchdog(worker_id));
                match futures::future::select(fetch, watchdog).await {
                    futures::future::Either::Left((result, _)) => result.map_err(|e| {
                        error!(target: "subscriber", worker_id, "Failed to fetch batches from peers: {e:?}");
                        SubscriberError::ClientRequestsFailed
                    }),
                    futures::future::Either::Right((never, _)) => match never {},
                }
            }
        });
        futures::future::try_join_all(legs)
            .await
            .inspect_err(|_| self.inner.metrics.batch_fetch_failures_total.increment(1))
            .map(|fetched| {
                fetched
                    .into_iter()
                    .flatten()
                    .map(|(digest, block)| {
                        debug!(
                            target: "subscriber",
                            "Block {:?} took {:?} seconds since it was received to when it was fetched for execution",
                            digest,
                            block.received_at().map(|t| t.elapsed().as_secs_f64()),
                        );
                        (digest, block)
                    })
                    .collect()
            })
    }
}

/// The execution block number the next received sub-dag's output waits for: the running maximum
/// of the leaders' execution blocks, `wait_floor` so far raised to `sub_dag`'s leader's block.
///
/// Keeping the floor monotone makes the outputs pass their execution wait in consensus order: an
/// output led from an older block does not pass early and sit fully assembled behind a slower
/// predecessor. It does not bound how many outputs are assembled at once. Every output whose floor
/// local execution has passed is assembled concurrently, so under a uniform backlog the outputs
/// held in memory approach the outputs between the executed and the committed leader: up to about
/// 500 at the 1,000-round run-ahead bound ([`tn_types::MAX_COMMIT_RUN_AHEAD_ROUNDS`]), about
/// 0.5-0.6 GB at the 1.0-1.25 MB average output and 2.75 GB at the 5.5 MB maximum.
fn next_wait_floor(wait_floor: u64, sub_dag: &CommittedSubDag) -> u64 {
    wait_floor.max(sub_dag.leader().latest_execution_block().number)
}

/// Drain the batch-fetch futures still pending when graceful shutdown fires.
///
/// This is the shutdown counterpart of the steady-state arm in [`Subscriber::run`] and is
/// deliberately symmetric with it: a fetch `Err` is fail-stop, not skippable. `waiting` is a
/// [`FuturesOrdered`] that yields in consensus (push) order, so every `Ok` output ahead of a
/// failed fetch is the contiguous prefix. `try_fold` threads the consensus chain as its
/// accumulator and short-circuits at the first error, so the drain saves and sends that
/// contiguous prefix and then stops at the gap: nothing at or past the failed fetch is saved or
/// sent. In steady state the same fetch `Err` returns an error that shuts the node down;
/// here, already shutting down, the fold simply stops, leaving the drained prefix persisted for
/// the Phase-2 DB drain and normal restart. Stopping here makes the shutdown path self-evidently
/// fail-stop instead of relying on the downstream pack/load/replay guards to reject the
/// non-contiguous consensus number a swallowed fetch `Err` would otherwise let it march past.
///
/// `take_until` bounds only the wait for the next completed fetch, not an in-flight save: once
/// `deadline` passes the drain stops pulling new outputs, but an output already dequeued is always
/// saved and sent to completion. This preserves the original select-based drain's guarantee that a
/// committed output, once dequeued, is never dropped mid-save during graceful shutdown. The send
/// waits while the `consensus_output` queue is full; nothing reads the queue during teardown, so
/// the abort of the epoch's tasks ends that wait, and Phase 2 of the epoch manager's leftover
/// drain forwards whatever was saved.
async fn drain_pending_on_shutdown<Fut>(
    consensus_bus: &ConsensusBusApp,
    consensus_chain: &mut ConsensusChain,
    waiting: FuturesOrdered<Fut>,
    deadline: Duration,
) where
    Fut: std::future::Future<Output = SubscriberResult<ConsensusOutput>>,
{
    let stop_at = tokio::time::Instant::now() + deadline;
    let _ = waiting
        .take_until(tokio::time::sleep_until(stop_at))
        .map_err(|e| {
            // A fetch `Err` is the same fatal condition the steady-state arm fail-stops on;
            // short-circuiting the fold here stops the drain at the gap.
            error!(target: "subscriber", "error fetching batches during shutdown drain: {e}");
        })
        .try_fold(consensus_chain, |chain, output| async move {
            if let Err(e) = save_consensus(output.clone(), chain, consensus_bus.metrics()).await {
                // A storage failure here (a write that poisoned the pack, or a save rolled back
                // after one) makes every later persist fail, `finish_epoch`'s included, so the
                // epoch end stops the node. A save the pack refuses as invalid latches nothing;
                // the drain stops there all the same, as at a failed fetch, and nothing at or past
                // it is saved or sent.
                error!(target: "subscriber", "error saving consensus during shutdown: {e}");
                Err(())
            } else {
                // Best-effort forward: a no-op once the epoch manager has dropped its receiver,
                // and a wait while the queue is full, which the abort of the epoch's tasks ends.
                // The DB-aware drain (Phase 2) forwards anything saved but never received.
                let _ = consensus_bus.consensus_output().send(output).await;
                Ok(chain)
            }
        })
        .await;
    // A drain that stops because the deadline passed (rather than because `waiting` emptied) has
    // reached `stop_at`; surface that the way the original select-based drain did.
    if tokio::time::Instant::now() >= stop_at {
        warn!(target: "subscriber", "timed out draining pending consensus during shutdown");
    }
}

/// Where a sync output's consensus number falls relative to the latest saved number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncContinuity {
    /// At or below the latest saved number: already saved, skip it.
    Stale,
    /// Exactly the next number: save and forward it.
    Next,
    /// Past the next number: the outputs in between were lost.
    Gap,
}

/// Classify a sync output's consensus `number` against the `latest` saved consensus number.
///
/// State sync only sends contiguous runs (it stops at the first output it cannot resolve and
/// skips only a leading run at or below `latest`), and `latest` only moves when the subscriber
/// saves, so the first output of every run is `latest + 1`: on a fresh chain, at an epoch start,
/// after a mode change, through staging and through static packs. A larger number can only mean a
/// lost output.
///
/// Mirrors `check_output_continuity` in `tn-node`'s epoch manager, which applies the same rule to
/// outputs forwarded to the engine; it is not shared because `tn-node` depends on this crate.
/// Checking here, before the save, keeps a lost output from being saved past.
fn classify_sync_output(latest: u64, number: u64) -> SyncContinuity {
    if number <= latest {
        SyncContinuity::Stale
    } else if number == latest + 1 {
        SyncContinuity::Next
    } else {
        SyncContinuity::Gap
    }
}

/// The newest consensus header a catching-up CVV knows about, copied out of the watch for one
/// readiness check.
#[derive(Clone, Copy, Debug)]
struct NewestHeader {
    /// The header's consensus number.
    number: u64,
    /// The epoch of the header's leader.
    epoch: Epoch,
    /// The round of the header's leader.
    round: Round,
    /// Time since the header's sub-dag was committed.
    age: Duration,
}

/// Why a catching-up CVV did not rejoin consensus after handling an output: the `reason` of the
/// `rejoin not ready` line and the `reason` label of `tn_executor_rejoin_blocked_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RejoinBlocker {
    /// The handled output is not from this subscriber's still-open epoch (see
    /// `rejoin_epoch_is_open`), so the node keeps replaying.
    EpochClosed,
    /// No consensus header has been seen yet to measure the network head by.
    NoKnownHeader,
    /// The newest known header is at least [`REJOIN_HEAD_MAX_AGE`] old.
    StaleHead,
    /// The newest known header is from another epoch than the handled output.
    EpochMismatch,
    /// The handled output trails the newest known header by more than the rejoin window.
    RoundGap,
}

impl RejoinBlocker {
    /// The number of blockers.
    const COUNT: usize = 5;

    /// Every blocker, in the order [`Self::classify`] tests them. A blocker's position here is its
    /// [`Self::index`].
    const ALL: [Self; Self::COUNT] = [
        Self::EpochClosed,
        Self::NoKnownHeader,
        Self::StaleHead,
        Self::EpochMismatch,
        Self::RoundGap,
    ];

    /// The static label for this blocker.
    const fn as_str(self) -> &'static str {
        match self {
            Self::EpochClosed => "epoch_closed",
            Self::NoKnownHeader => "no_known_header",
            Self::StaleHead => "stale_head",
            Self::EpochMismatch => "epoch_mismatch",
            Self::RoundGap => "round_gap",
        }
    }

    /// The position of this blocker in [`Self::ALL`], for per-blocker arrays.
    const fn index(self) -> usize {
        self as usize
    }

    /// Classify a readiness check that failed: the first condition the check would refuse, in the
    /// order of [`Self::ALL`].
    ///
    /// Call only on the not-ready branch. The first four blockers mirror `rejoin_epoch_is_open`
    /// and the freshness and epoch parts of `rejoin_ready`, so a failed check that passes them
    /// failed on the round gap.
    fn classify(epoch_is_open: bool, newest: Option<&NewestHeader>, handled_epoch: Epoch) -> Self {
        if !epoch_is_open {
            return Self::EpochClosed;
        }
        let Some(newest) = newest else {
            return Self::NoKnownHeader;
        };
        if newest.age >= REJOIN_HEAD_MAX_AGE {
            Self::StaleHead
        } else if newest.epoch != handled_epoch {
            Self::EpochMismatch
        } else {
            Self::RoundGap
        }
    }
}

// compile-time check that `ALL` lists the blockers in declaration order, so every `index` in it is
// a valid position in the per-blocker arrays. a blocker inserted without updating `COUNT` and
// `ALL` fails the build here. stable rust cannot count variants, so a blocker appended after
// `RoundGap` still needs `COUNT` and `ALL` updated by hand.
const _: () = {
    let mut position = 0;
    while position < RejoinBlocker::COUNT {
        assert!(
            RejoinBlocker::ALL[position].index() == position,
            "RejoinBlocker::ALL out of order"
        );
        position += 1;
    }
};

/// Resolve the `tn_executor_rejoin_blocked_total{reason}` handles, indexed by
/// [`RejoinBlocker::index`].
fn rejoin_blocked_counters() -> [Counter; RejoinBlocker::COUNT] {
    RejoinBlocker::ALL.map(
        |reason| metrics::counter!("tn_executor.rejoin_blocked_total", "reason" => reason.as_str()),
    )
}

/// Rate limit for the `rejoin not ready` line: log when the blocker changes, and otherwise at most
/// once per [`REJOIN_NOT_READY_LOG_INTERVAL`].
///
/// Owned by one catch-up loop, so the stamp needs no synchronization.
#[derive(Debug, Default)]
struct RejoinLogThrottle {
    /// The blocker last logged and when.
    last: Option<(RejoinBlocker, Instant)>,
}

impl RejoinLogThrottle {
    /// True when a check blocked by `reason` at `now` should be logged. A `true` answer records
    /// `reason` and `now` as the last line logged.
    fn should_log(&mut self, reason: RejoinBlocker, now: Instant) -> bool {
        let due = self.last.is_none_or(|(last_reason, logged_at)| {
            last_reason != reason
                || now.saturating_duration_since(logged_at) >= REJOIN_NOT_READY_LOG_INTERVAL
        });
        if due {
            self.last = Some((reason, now));
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::{
        classify_sync_output, drain_pending_on_shutdown, save_consensus, Inner, Subscriber,
        SyncContinuity,
    };
    use crate::{metrics::ExecutorMetrics, SubscriberError};
    use futures::{future, stream::FuturesOrdered};
    use std::{collections::VecDeque, sync::Arc, time::Duration};
    use tempfile::TempDir;
    use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
    use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        Certificate, CommittedSubDag, Committee, ConsensusHeader, ConsensusHeaderDigest,
        ConsensusOutput, ReputationScores, TnReceiver as _, TnSender as _, B256,
    };
    use tokio::sync::mpsc;

    /// Build a valid `ConsensusOutput` at `number` (parent `parent`, sub-dag index `sub_dag_index`)
    /// from the fixture's certificates, returning it with its consensus-header digest so a caller
    /// can use that digest as the next output's parent.
    fn output_at(
        certificates: &[Certificate],
        committee: &Committee,
        number: u64,
        sub_dag_index: u64,
        parent: ConsensusHeaderDigest,
    ) -> (ConsensusOutput, ConsensusHeaderDigest) {
        let leader = certificates.last().cloned().unwrap();
        let sub_dag = CommittedSubDag::new(
            certificates.to_vec(),
            leader,
            sub_dag_index,
            ReputationScores::new(committee),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let digest = ConsensusHeader::digest_from_parts(parent, &sub_dag, number);
        let output = ConsensusOutput::new(sub_dag, parent, number, false, VecDeque::new(), vec![]);
        (output, digest)
    }

    /// Build `count` contiguous, digest-chained outputs numbered from 1, the run a fresh chain
    /// accepts in order.
    fn chained_outputs(
        fixture: &CommitteeFixture<MemDatabase>,
        count: u64,
    ) -> Vec<ConsensusOutput> {
        let committee = fixture.committee();
        // Round-1 headers with no payload: these outputs carry no batches, so their sub-dags may
        // declare none (a saved output carries every batch its sub-dag declares).
        let headers = fixture.headers();
        let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        let mut parent: ConsensusHeaderDigest = B256::ZERO.into();
        let mut outputs = Vec::new();
        for number in 1..=count {
            let (output, digest) = output_at(&certificates, &committee, number, number - 1, parent);
            outputs.push(output);
            parent = digest;
        }
        outputs
    }

    /// Build a subscriber for the fixture's first authority over `consensus_chain`, enough to
    /// drive `handle_sync_output`.
    fn sync_subscriber(
        fixture: &CommitteeFixture<MemDatabase>,
        consensus_chain: ConsensusChain,
        consensus_bus: &ConsensusBusApp,
    ) -> Subscriber<MemDatabase> {
        let config = fixture.authorities().next().unwrap().consensus_config().clone();
        let (network_tx, _network_rx) = mpsc::channel(1);
        Subscriber {
            consensus_bus: consensus_bus.clone(),
            config: config.clone(),
            network_handle: PrimaryNetworkHandle::new_for_test(network_tx),
            inner: Arc::new(Inner {
                authority_id: config.authority_id(),
                committee: config.committee().clone(),
                consensus_chain,
                epoch_boundary: u64::MAX,
                metrics: ExecutorMetrics::default(),
            }),
        }
    }

    /// A save error mid-drain must fail-stop the drain the same way a fetch `Err` does: the output
    /// whose save fails is not broadcast, and no later output is saved or broadcast. Reverting the
    /// save-error arm to swallow-and-continue lets the following contiguous output be saved and
    /// broadcast, which this test rejects.
    #[tokio::test]
    async fn shutdown_drain_fail_stops_at_save_error() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir = TempDir::new().unwrap();
        let mut consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();

        // Round-1 headers with no payload: the outputs carry no batches, so their sub-dags declare
        // none (a saved output carries every batch its sub-dag declares).
        let headers = fixture.headers();
        let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();

        let parent0: ConsensusHeaderDigest = B256::ZERO.into();
        // Output 1 saves fine (contiguous).
        let (out1, digest1) = output_at(&certificates, &committee, 1, 0, parent0);
        // A NON-contiguous number (3, skipping 2) makes save_consensus reject the write, forcing
        // the save-error short-circuit.
        let (out_bad, _) = output_at(&certificates, &committee, 3, 2, digest1);
        // Output 2 would save fine if the drain wrongly continued past the save failure.
        let (out_next, _) = output_at(&certificates, &committee, 2, 1, digest1);

        let mut waiting = FuturesOrdered::new();
        waiting.push_back(future::ready(Ok::<_, SubscriberError>(out1)));
        waiting.push_back(future::ready(Ok(out_bad)));
        waiting.push_back(future::ready(Ok(out_next)));

        let mut rx = consensus_bus.subscribe_consensus_output();

        drain_pending_on_shutdown(
            &consensus_bus,
            &mut consensus_chain,
            waiting,
            Duration::from_secs(3),
        )
        .await;

        // Only output 1 is broadcast: out_bad's save fails (never broadcast) and stops the drain,
        // so output 2 is never reached.
        assert!(rx.recv().await.is_some(), "the drained prefix (output 1) must be broadcast");
        assert!(rx.try_recv().is_err(), "no output at or past the save failure may be broadcast",);
        assert!(
            matches!(consensus_chain.consensus_header_by_number(1).await, Ok(Some(_))),
            "output 1 (the drained prefix) must be persisted",
        );
        assert!(
            !matches!(consensus_chain.consensus_header_by_number(2).await, Ok(Some(_))),
            "output 2 (past the save failure) must not be persisted",
        );
    }

    /// A fetch that never completes must not hang the drain: the deadline stops it. This pins the
    /// `take_until` cap added for graceful shutdown.
    #[tokio::test(start_paused = true)]
    async fn shutdown_drain_stops_on_deadline() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir = TempDir::new().unwrap();
        let mut consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();

        // A single fetch future that never resolves. With the tokio test clock paused and auto-
        // advanced, the drain must return when the deadline elapses rather than hang forever.
        let mut waiting = FuturesOrdered::new();
        waiting.push_back(future::pending::<Result<ConsensusOutput, SubscriberError>>());

        let mut rx = consensus_bus.subscribe_consensus_output();

        drain_pending_on_shutdown(
            &consensus_bus,
            &mut consensus_chain,
            waiting,
            Duration::from_secs(3),
        )
        .await;

        // Nothing was ever fetched, so nothing is saved or broadcast; the point is that the call
        // returned instead of hanging on the stalled fetch.
        assert!(rx.try_recv().is_err(), "a stalled fetch must not save or broadcast anything");
    }

    /// A fetch `Err` in the middle of the graceful-shutdown drain must fail-stop the drain: the
    /// contiguous prefix ahead of it is saved and broadcast, and nothing at or past the failed
    /// fetch is. Reverting `drain_pending_on_shutdown` to the old "swallow the `Err` and keep
    /// draining" shape makes the post-gap output get saved and broadcast, which this test rejects
    /// (both the broadcast and the persistence assertions flip), so the test genuinely pins the
    /// fail-stop semantics rather than passing vacuously.
    #[tokio::test]
    async fn shutdown_drain_fail_stops_at_fetch_error() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let temp_dir = TempDir::new().unwrap();
        let mut consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), committee.clone())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();

        // Two valid, CONTIGUOUS consensus outputs (numbers 1 and 2). Number 2 is contiguous so
        // that if the drain wrongly continued past the failed fetch, its save would succeed and
        // it would be broadcast -- making the swallow-and-continue bug observable rather than
        // masked by the pack rejecting a non-contiguous number.
        // Round-1 headers with no payload: the outputs carry no batches, so their sub-dags declare
        // none (a saved output carries every batch its sub-dag declares).
        let headers = fixture.headers();
        let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        let leader = certificates.last().cloned().unwrap();

        let parent0: ConsensusHeaderDigest = B256::ZERO.into();
        let sub_dag1 = CommittedSubDag::new(
            certificates.clone(),
            leader.clone(),
            0,
            ReputationScores::new(&committee),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let out1 =
            ConsensusOutput::new(sub_dag1.clone(), parent0, 1, false, VecDeque::new(), vec![]);

        let digest1 = ConsensusHeader::digest_from_parts(parent0, &sub_dag1, 1);
        let sub_dag2 = CommittedSubDag::new(
            certificates.clone(),
            leader,
            1,
            ReputationScores::new(&committee),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        let out2 = ConsensusOutput::new(sub_dag2, digest1, 2, false, VecDeque::new(), vec![]);

        // Pending fetch results in consensus (push) order: Ok(1), Err (failed fetch), Ok(2).
        let mut waiting = FuturesOrdered::new();
        waiting.push_back(future::ready(Ok(out1)));
        waiting.push_back(future::ready(Err(SubscriberError::ClientRequestsFailed)));
        waiting.push_back(future::ready(Ok(out2)));

        // Subscribe before draining so we observe exactly what is broadcast.
        let mut rx = consensus_bus.subscribe_consensus_output();

        drain_pending_on_shutdown(
            &consensus_bus,
            &mut consensus_chain,
            waiting,
            Duration::from_secs(3),
        )
        .await;

        // The contiguous prefix (output 1) is broadcast, and nothing past the failed fetch is:
        // output 2 was never broadcast.
        assert!(rx.recv().await.is_some(), "the drained prefix (output 1) must be broadcast");
        assert!(rx.try_recv().is_err(), "no output at or past the failed fetch may be broadcast",);

        // The prefix is persisted; the post-gap output is not. Querying a consensus number beyond
        // the chain head returns either `Ok(None)` or a "number too high" `Err`; either way the
        // output is absent, whereas a bug that saved output 2 would return `Ok(Some(_))`.
        assert!(
            matches!(consensus_chain.consensus_header_by_number(1).await, Ok(Some(_))),
            "output 1 (the drained prefix) must be persisted",
        );
        assert!(
            !matches!(consensus_chain.consensus_header_by_number(2).await, Ok(Some(_))),
            "output 2 (past the failed fetch) must not be persisted",
        );
    }

    /// A state-sync burst larger than any `sync_output` buffer must reach the subscriber intact.
    ///
    /// Replaying a local pack, `catch_up_consensus_from_to` waits for execution only before
    /// outputs that carry an execution block, so across a run of empty outputs it sends as fast as
    /// it reads while the subscriber saves (and fsyncs) each output before forwarding it. The
    /// producer here sends more outputs than the old `sync_output` broadcast held (1_000) without
    /// waiting on anything else, and the consumer drives the real `handle_sync_output`. Every
    /// output must arrive exactly once and in order, and every output forwarded on
    /// `consensus_output` must be the next number, the continuity
    /// `EpochManager::wait_for_epoch_boundary` enforces before the engine. A lagging broadcast
    /// drops the oldest outputs, so the subscriber sees a jump in numbers.
    #[tokio::test]
    async fn sync_output_burst_reaches_subscriber_without_gaps() {
        // more than the old broadcast capacity (1_000) and the bounded queue depth
        const OUTPUTS: u64 = 1_200;

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new().unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();
        let subscriber = sync_subscriber(&fixture, consensus_chain, &consensus_bus);
        let outputs = chained_outputs(&fixture, OUTPUTS);

        // subscribe both ends before the producer starts, as `follow_consensus` and the epoch
        // manager do
        let mut rx_sync_output = consensus_bus.subscribe_sync_output();
        let mut rx_consensus_output = consensus_bus.subscribe_consensus_output();
        let producer_bus = consensus_bus.clone();
        let producer = tokio::spawn(async move {
            for output in outputs {
                let sent = producer_bus.sync_output().send(output).await;
                assert!(sent.is_ok(), "sync_output send failed");
            }
        });

        let mut last_forwarded = 0;
        for expected in 1..=OUTPUTS {
            let output = tokio::time::timeout(Duration::from_secs(10), rx_sync_output.recv())
                .await
                .unwrap_or_else(|_| panic!("output {expected} never reached the subscriber"))
                .expect("sync_output stays open");
            assert_eq!(output.number(), expected, "sync_output lost or reordered outputs");
            subscriber.handle_sync_output(output).await.expect("a contiguous output saves");
            let forwarded =
                rx_consensus_output.try_recv().expect("every saved output is forwarded");
            assert_eq!(forwarded.number(), last_forwarded + 1, "consensus_output gap");
            last_forwarded = forwarded.number();
        }
        producer.await.expect("the producer finishes once everything is consumed");
        assert_eq!(subscriber.inner.consensus_chain.latest_consensus_number(), OUTPUTS);
    }

    /// A subscriber that runs more than a buffer ahead of the epoch manager must not lose outputs.
    ///
    /// The epoch manager stops reading `consensus_output` whenever the engine's bounded queue is
    /// full, while the subscriber keeps saving and publishing. The producer here drives the real
    /// `handle_consensus_output` for more outputs than the old 128-slot broadcast ring and the
    /// bounded queue hold, and nothing reads until it has either finished or stalled with more
    /// than a queue's worth saved. Every output must then reach the reader exactly once and in
    /// order, the continuity `check_output_continuity` enforces before the engine. A broadcast
    /// overwrites the oldest unread outputs, so the first receive is a jump past output 1.
    #[tokio::test]
    async fn consensus_output_backlog_reaches_epoch_manager_without_gaps() {
        // more than the old 128-slot broadcast ring and the bounded queue depth
        const OUTPUTS: u64 = 300;

        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new().unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();
        let subscriber = sync_subscriber(&fixture, consensus_chain.clone(), &consensus_bus);
        let outputs = chained_outputs(&fixture, OUTPUTS);

        // subscribe before the producer starts, as the epoch manager does
        let mut rx_consensus_output = consensus_bus.subscribe_consensus_output();
        let mut producer_chain = consensus_chain.clone();
        let producer = tokio::spawn(async move {
            for output in outputs {
                subscriber
                    .handle_consensus_output(&mut producer_chain, output)
                    .await
                    .expect("a contiguous output saves and sends");
            }
        });

        // hold off reading until the producer has finished or has stalled more than a queue ahead
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut last_saved = 0;
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let saved = consensus_chain.latest_consensus_number();
                if producer.is_finished() || (saved > 100 && saved == last_saved) {
                    break;
                }
                last_saved = saved;
            }
        })
        .await
        .expect("the producer either finishes or stalls on a full queue");

        let mut last_forwarded = 0;
        for _ in 0..OUTPUTS {
            let output = tokio::time::timeout(Duration::from_secs(10), rx_consensus_output.recv())
                .await
                .unwrap_or_else(|_| {
                    panic!("output {} never reached the reader", last_forwarded + 1)
                })
                .expect("consensus_output stays open");
            let number = output.number();
            assert_eq!(
                classify_sync_output(last_forwarded, number),
                SyncContinuity::Next,
                "consensus output gap: expected {} but received {}",
                last_forwarded + 1,
                number,
            );
            last_forwarded = number;
        }
        producer.await.expect("the producer finishes once everything is consumed");
        assert!(rx_consensus_output.try_recv().is_err(), "nothing past the last output is sent");
        assert_eq!(consensus_chain.latest_consensus_number(), OUTPUTS);
    }

    /// The sync-output guard's table: a number at or below the latest saved one is stale, exactly
    /// one past it is next, and anything further is a gap.
    #[test]
    fn classify_sync_output_table() {
        let cases = [
            (0, 1, SyncContinuity::Next),
            (5, 5, SyncContinuity::Stale),
            (5, 0, SyncContinuity::Stale),
            (5, 6, SyncContinuity::Next),
            (5, 7, SyncContinuity::Gap),
            (5, u64::MAX, SyncContinuity::Gap),
        ];
        for (latest, number, expected) in cases {
            assert_eq!(
                classify_sync_output(latest, number),
                expected,
                "latest {latest}, number {number}"
            );
        }
    }

    /// The guard agrees with what the consensus chain accepts: an output the chain already holds is
    /// stale (and saving it again fails), the next number is accepted, and one past it is a gap
    /// until the next number is saved.
    #[tokio::test]
    async fn sync_guard_matches_chain_state() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new().unwrap();
        let mut consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();
        let outputs = chained_outputs(&fixture, 5);
        for output in &outputs[..3] {
            save_consensus(output.clone(), &mut consensus_chain, consensus_bus.metrics())
                .await
                .expect("a contiguous output saves");
        }
        let latest = consensus_chain.latest_consensus_number();
        assert_eq!(latest, 3);

        assert_eq!(classify_sync_output(latest, outputs[1].number()), SyncContinuity::Stale);
        assert!(
            save_consensus(outputs[1].clone(), &mut consensus_chain, consensus_bus.metrics())
                .await
                .is_err(),
            "the chain refuses an output it already holds",
        );
        assert_eq!(consensus_chain.latest_consensus_number(), 3, "a refused save keeps latest");
        assert_eq!(classify_sync_output(latest, outputs[3].number()), SyncContinuity::Next);
        assert_eq!(classify_sync_output(latest, outputs[4].number()), SyncContinuity::Gap);

        save_consensus(outputs[3].clone(), &mut consensus_chain, consensus_bus.metrics())
            .await
            .expect("the next output saves");
        let latest = consensus_chain.latest_consensus_number();
        assert_eq!(latest, 4);
        assert_eq!(classify_sync_output(latest, outputs[4].number()), SyncContinuity::Next);
    }

    /// `handle_sync_output` applies the guard before it saves: an output the chain already holds
    /// is skipped without being saved again or forwarded, an output past the next number is
    /// refused with `SyncOutputGap` and leaves the chain and `consensus_output` untouched, and the
    /// next number still saves and forwards afterwards.
    #[tokio::test]
    async fn handle_sync_output_refuses_gap_and_skips_stale() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let temp_dir = TempDir::new().unwrap();
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .unwrap();
        let consensus_bus = ConsensusBusApp::new();
        let subscriber = sync_subscriber(&fixture, consensus_chain, &consensus_bus);
        let chain = &subscriber.inner.consensus_chain;
        let outputs = chained_outputs(&fixture, 5);
        let mut rx_consensus_output = consensus_bus.subscribe_consensus_output();

        for output in &outputs[..3] {
            subscriber.handle_sync_output(output.clone()).await.expect("a contiguous output saves");
            let forwarded = rx_consensus_output.try_recv().expect("a saved output is forwarded");
            assert_eq!(forwarded.number(), output.number());
        }
        assert_eq!(chain.latest_consensus_number(), 3);

        // a stale output would fail the save, so it must be skipped before it
        subscriber.handle_sync_output(outputs[1].clone()).await.expect("a stale output is skipped");
        assert_eq!(chain.latest_consensus_number(), 3, "a stale output is not saved");
        assert!(rx_consensus_output.try_recv().is_err(), "a stale output is not forwarded");

        let err = subscriber
            .handle_sync_output(outputs[4].clone())
            .await
            .expect_err("an output past the next number is refused");
        assert!(
            matches!(err, SubscriberError::SyncOutputGap { expected: 4, received: 5 }),
            "expected SyncOutputGap {{ expected: 4, received: 5 }}, got {err:?}"
        );
        assert_eq!(chain.latest_consensus_number(), 3, "a refused output is not saved");
        assert!(
            !matches!(chain.consensus_header_by_number(5).await, Ok(Some(_))),
            "a refused output is not persisted",
        );
        assert!(rx_consensus_output.try_recv().is_err(), "a refused output is not forwarded");

        subscriber.handle_sync_output(outputs[3].clone()).await.expect("the next output saves");
        assert_eq!(chain.latest_consensus_number(), 4, "the next output advances the chain");
        let forwarded = rx_consensus_output.try_recv().expect("the next output is forwarded");
        assert_eq!(forwarded.number(), 4);
    }

    mod f1_commit_lag {
        use super::*;
        use crate::subscriber::next_wait_floor;
        use tn_types::{
            Batch, BlockNumHash, ConsensusNumHash, EpochSeedChainValue, ExecHeader, Notifier,
            SealedHeader,
        };

        /// The hash this node's execution gives block `number` in these tests.
        fn local_hash(number: u64) -> B256 {
            B256::repeat_byte(u8::try_from(number).unwrap())
        }

        /// A block hash this node never executed: the forked leader's.
        fn forked_hash() -> B256 {
            B256::repeat_byte(0xff)
        }

        /// Record block `number` as executed, as the engine does after each output.
        fn execute(bus: &ConsensusBusApp, number: u64) {
            let header = ExecHeader { number, ..Default::default() };
            bus.recent_blocks().send_modify(|blocks| {
                blocks.push_latest(
                    0,
                    ConsensusNumHash::new(number, ConsensusHeaderDigest::default()),
                    Some(SealedHeader::new(header, local_hash(number))),
                )
            });
        }

        /// A one-certificate sub-dag whose leader references execution block `base`, with one
        /// batch in its payload when `with_payload`.
        fn led_at(
            fixture: &CommitteeFixture<MemDatabase>,
            base: BlockNumHash,
            with_payload: bool,
        ) -> CommittedSubDag {
            let mut builder = fixture.header_builder_last_authority().latest_execution_block(base);
            if with_payload {
                builder = builder.with_payload_batch(&Batch::default(), 0);
            }
            let leader = fixture.certificate(&builder.build());
            CommittedSubDag::new(
                vec![leader.clone()],
                leader,
                0,
                ReputationScores::new(&fixture.committee()),
                None,
                EpochSeedChainValue::genesis_placeholder(),
            )
        }

        /// A fixture, its consensus chain and a recent blocks ring holding block 1.
        async fn setup() -> (CommitteeFixture<MemDatabase>, TempDir, ConsensusChain, ConsensusBusApp)
        {
            let fixture = CommitteeFixture::builder(MemDatabase::default).build();
            let temp_dir = TempDir::new().unwrap();
            let chain =
                ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                    .await
                    .unwrap();
            let bus = ConsensusBusApp::new();
            execute(&bus, 1);
            (fixture, temp_dir, chain, bus)
        }

        /// A leader whose execution block this node executed differently fails the fork check
        /// before any batch is fetched: the payload's batch exists nowhere, so a fetch would never
        /// return.
        #[tokio::test]
        async fn fetch_batches_reports_execution_fork() {
            let (fixture, _temp_dir, chain, bus) = setup().await;
            let subscriber = sync_subscriber(&fixture, chain, &bus);
            let sub_dag = led_at(&fixture, BlockNumHash::new(1, forked_hash()), true);
            assert_ne!(sub_dag.num_primary_batches(), 0);
            let (expected_leader, expected_round) =
                (sub_dag.leader().author().clone(), sub_dag.leader_round());

            let result = tokio::time::timeout(
                Duration::from_secs(10),
                subscriber.fetch_batches(sub_dag, B256::ZERO.into(), 1, 1),
            )
            .await
            .expect("the fork check runs before the batch fetch");

            // the error names the leader that referenced the block (SEC-11)
            assert!(matches!(
                result,
                Err(SubscriberError::ExecutionFork {
                    number: 1,
                    base_block: 1,
                    base_hash,
                    leader,
                    leader_round,
                }) if base_hash == forked_hash()
                    && leader == expected_leader
                    && leader_round == expected_round
            ));
        }

        /// A leader block below a snapshot-restored node's floor cannot be checked there, so it
        /// is not a fork: the output is assembled, while the same mismatch at the floor still
        /// fails the check (SEC-6).
        #[tokio::test]
        async fn fetch_batches_assembles_below_the_restore_floor() {
            /// A restored execution database whose floor is block 3, attesting this node's
            /// blocks from there on.
            struct RestoredAt3;
            impl tn_types::CanonicalExecutionReader for RestoredAt3 {
                fn canonical_execution_hash(&self, number: u64) -> Option<B256> {
                    (number >= 3).then(|| local_hash(number))
                }
                fn restored_state_floor(&self) -> Option<u64> {
                    Some(3)
                }
            }

            let (fixture, _temp_dir, chain, bus) = setup().await;
            (2..=5).for_each(|number| execute(&bus, number));
            bus.set_canonical_reader(Arc::new(RestoredAt3));
            let subscriber = sync_subscriber(&fixture, chain, &bus);

            let below = led_at(&fixture, BlockNumHash::new(2, forked_hash()), false);
            let output = subscriber.fetch_batches(below, B256::ZERO.into(), 1, 2).await.unwrap();
            assert_eq!(output.number(), 1);

            let at_floor = led_at(&fixture, BlockNumHash::new(3, forked_hash()), false);
            let result = subscriber.fetch_batches(at_floor, B256::ZERO.into(), 2, 3).await;
            assert!(matches!(result, Err(SubscriberError::ExecutionFork { base_block: 3, .. })));
        }

        /// A sub-dag with no batches gets the same fork check: its output is not returned before
        /// the check, and the check passes for a leader on the local chain.
        #[tokio::test]
        async fn fetch_batches_checks_empty_subdag() {
            let (fixture, _temp_dir, chain, bus) = setup().await;
            let subscriber = sync_subscriber(&fixture, chain, &bus);

            let forked = led_at(&fixture, BlockNumHash::new(1, forked_hash()), false);
            assert_eq!(forked.num_primary_batches(), 0);
            let result = subscriber.fetch_batches(forked, B256::ZERO.into(), 1, 1).await;
            assert!(matches!(result, Err(SubscriberError::ExecutionFork { number: 1, .. })));

            let local = led_at(&fixture, BlockNumHash::new(1, local_hash(1)), false);
            let output = subscriber.fetch_batches(local, B256::ZERO.into(), 1, 1).await.unwrap();
            assert_eq!(output.number(), 1);
        }

        /// A later output led from an older execution block still waits for the floor an earlier
        /// output set: leaders at blocks 5 then 1, with block 1 executed, and the second output
        /// does not finish before execution reaches block 5.
        #[tokio::test]
        async fn wait_floor_is_monotone() {
            let (fixture, _temp_dir, chain, bus) = setup().await;
            let subscriber = sync_subscriber(&fixture, chain, &bus);
            let first = led_at(&fixture, BlockNumHash::new(5, local_hash(5)), false);
            let second = led_at(&fixture, BlockNumHash::new(1, local_hash(1)), false);
            let first_floor = next_wait_floor(0, &first);
            let second_floor = next_wait_floor(first_floor, &second);
            assert_eq!((first_floor, second_floor), (5, 5));

            let first_output =
                Box::pin(subscriber.fetch_batches(first, B256::ZERO.into(), 1, first_floor));
            let mut second_output =
                Box::pin(subscriber.fetch_batches(second, B256::ZERO.into(), 2, second_floor));
            assert!(
                tokio::time::timeout(Duration::from_millis(200), &mut second_output).await.is_err(),
                "the second output waits for the first leader's block"
            );

            (2..=5).for_each(|number| execute(&bus, number));
            let second_output = tokio::time::timeout(Duration::from_secs(10), second_output)
                .await
                .expect("execution reached the floor")
                .unwrap();
            let first_output = tokio::time::timeout(Duration::from_secs(10), first_output)
                .await
                .expect("execution reached the first leader's block")
                .unwrap();
            assert_eq!((first_output.number(), second_output.number()), (1, 2));
        }

        /// On a failed fork check `run` goes inactive, shuts the epoch down and returns `Ok`
        /// without saving the output: an `Err` would fail the critical task, and the shutdown
        /// drain would save outputs past the fork.
        #[tokio::test]
        async fn run_returns_ok_after_execution_fork() {
            let (fixture, _temp_dir, chain, bus) = setup().await;
            let subscriber = sync_subscriber(&fixture, chain.clone(), &bus);
            let epoch_shutdown =
                fixture.authorities().next().unwrap().consensus_config().shutdown().subscribe();
            let (tx_sequence, rx_sequence) = mpsc::channel(4);
            tx_sequence
                .send(led_at(&fixture, BlockNumHash::new(1, forked_hash()), false))
                .await
                .unwrap();
            let run_shutdown = Notifier::new();

            let result = tokio::time::timeout(
                Duration::from_secs(10),
                subscriber.run(run_shutdown.subscribe(), rx_sequence, chain.clone()),
            )
            .await
            .expect("run returns after the fork");

            assert!(result.is_ok(), "{result:?}");
            assert!(bus.is_cvv_inactive(), "the node goes inactive");
            assert!(epoch_shutdown.noticed(), "the epoch shuts down");
            assert_eq!(chain.latest_consensus_number(), 0, "the forked output is not saved");
        }
    }
}

#[cfg(test)]
mod worker_fanout_tests {
    use super::*;
    use std::num::NonZeroUsize;
    use tempfile::TempDir;
    use tn_network_types::MockPrimaryToWorkerClient;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::CommitteeFixture;
    use tokio::sync::mpsc;

    /// One fetch leg per worker id, merged into one result; an id with no local network
    /// instance is the protocol violation `UnexpectedWorkerId` (issue #556).
    #[tokio::test]
    async fn fetch_batches_from_peers_fans_out_per_worker() {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let primary = fixture.authorities().next().unwrap();
        let base = primary.consensus_config().clone();
        // a two-worker committee sizes the per-worker local networks
        let committee =
            base.committee().with_num_workers(NonZeroUsize::new(2).expect("2 is not 0"));
        let config = ConsensusConfig::new_with_committee_for_test(
            base.config().clone(),
            base.node_storage().clone(),
            base.key_config().clone(),
            committee,
            base.network_config().clone(),
        )
        .expect("two-worker consensus config");

        // each worker's instance serves a disjoint batch
        let digest_0 = BlockHash::random();
        let digest_1 = BlockHash::random();
        let batch = Batch::default();
        config
            .local_network(0)
            .expect("worker 0 local network")
            .set_primary_to_worker_local_handler(Arc::new(MockPrimaryToWorkerClient {
                batches: HashMap::from([(digest_0, batch.clone())]),
            }))
            .expect("register worker 0 mock");
        config
            .local_network(1)
            .expect("worker 1 local network")
            .set_primary_to_worker_local_handler(Arc::new(MockPrimaryToWorkerClient {
                batches: HashMap::from([(digest_1, batch)]),
            }))
            .expect("register worker 1 mock");

        let temp_dir = TempDir::new().expect("temp dir");
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), config.committee().clone())
                .await
                .expect("consensus chain");
        let (tx, _rx) = mpsc::channel(5);
        let consensus_bus = ConsensusBus::new();
        let subscriber = Subscriber {
            consensus_bus: consensus_bus.app().clone(),
            config: config.clone(),
            network_handle: PrimaryNetworkHandle::new_for_test(tx),
            inner: Arc::new(Inner {
                authority_id: config.authority_id(),
                committee: config.committee().clone(),
                consensus_chain,
                epoch_boundary: u64::MAX,
                metrics: ExecutorMetrics::default(),
            }),
        };

        // both legs fetch concurrently and the results merge
        let by_worker =
            BTreeMap::from([(0, BTreeSet::from([digest_0])), (1, BTreeSet::from([digest_1]))]);
        let fetched =
            subscriber.fetch_batches_from_peers(by_worker).await.expect("both worker legs succeed");
        assert!(fetched.contains_key(&digest_0), "worker 0's batch fetched");
        assert!(fetched.contains_key(&digest_1), "worker 1's batch fetched");

        // an id outside the committee's worker set is a protocol violation
        let unknown = BTreeMap::from([(7, BTreeSet::from([digest_0]))]);
        assert!(matches!(
            subscriber.fetch_batches_from_peers(unknown).await,
            Err(SubscriberError::UnexpectedWorkerId(7))
        ));
    }
}

#[cfg(test)]
mod rejoin_tests {
    use super::*;
    use tn_types::{gc_activity_window, rejoin_round_window, GC_ACTIVITY_BUFFER, MAX_GC_DEPTH};

    /// Epoch shared by the handled output and the newest known header in these cases.
    const EPOCH: Epoch = 3;
    /// Round of the newest known header in these cases.
    const NEWEST: Round = 400;
    /// Age of a newest known header that counts as fresh.
    const FRESH: Duration = Duration::from_millis(500);

    /// `rejoin_ready` for a handled output `gap` rounds behind a fresh newest header, with the
    /// rejoin window of the default `gc_depth`.
    fn ready_at_gap(gap: Round) -> bool {
        rejoin_ready(
            EPOCH,
            NEWEST.saturating_sub(gap),
            EPOCH,
            NEWEST,
            FRESH,
            rejoin_round_window(MAX_GC_DEPTH),
        )
    }

    /// The old exact-match case still rejoins.
    #[test]
    fn rejoin_on_exact_match() {
        assert!(ready_at_gap(0), "a node at the newest known header must rejoin");
    }

    /// A node a few rounds behind the head rejoins. The old exact-match rule refused this case,
    /// which kept a node under load inactive until the load dropped (issue #1513).
    #[test]
    fn rejoin_with_small_gap() {
        assert!(ready_at_gap(1), "a node one round behind must rejoin");
        assert!(ready_at_gap(2), "a node one leader commit behind must rejoin");
    }

    /// The rejoin window is inclusive, and one round past it blocks promotion.
    #[test]
    fn rejoin_window_edge() {
        let window = rejoin_round_window(MAX_GC_DEPTH);
        assert!(ready_at_gap(window), "a gap equal to the rejoin window must rejoin");
        assert!(!ready_at_gap(window + 1), "a gap past the rejoin window must not rejoin");
    }

    /// A node at the demotion edge does not rejoin, so it does not flip between active and
    /// inactive.
    #[test]
    fn no_rejoin_at_demotion_edge() {
        assert!(
            !ready_at_gap(gc_activity_window(MAX_GC_DEPTH)),
            "a gap at the demotion window must not rejoin"
        );
    }

    /// A stale newest header blocks promotion even at an exact match.
    #[test]
    fn no_rejoin_on_stale_newest_header() {
        let window = rejoin_round_window(MAX_GC_DEPTH);
        assert!(
            !rejoin_ready(EPOCH, NEWEST, EPOCH, NEWEST, REJOIN_HEAD_MAX_AGE, window),
            "a newest header at the age limit must not promote"
        );
        assert!(
            !rejoin_ready(EPOCH, NEWEST, EPOCH, NEWEST, Duration::from_secs(60), window),
            "an old newest header must not promote"
        );
    }

    /// Rounds do not compare across epochs, so an epoch mismatch blocks promotion.
    #[test]
    fn no_rejoin_across_epochs() {
        let window = rejoin_round_window(MAX_GC_DEPTH);
        assert!(
            !rejoin_ready(EPOCH, NEWEST, EPOCH + 1, NEWEST, FRESH, window),
            "a node in an older epoch must not rejoin"
        );
    }

    /// An ignored future output cannot promote the subscriber still closing an older epoch,
    /// even when that output matches the fresh network head exactly.
    #[test]
    fn no_rejoin_on_ignored_future_output() {
        assert!(
            !rejoin_epoch_is_open(EPOCH, EPOCH + 1, None),
            "an ignored future output must not activate the subscriber in its older epoch"
        );
    }

    /// A finalized record closes that epoch and older epochs, while genesis and the epoch after
    /// the finalized tip remain eligible for the normal freshness and round-gap checks.
    #[test]
    fn rejoin_epoch_respects_finalized_tip() {
        let finalized = EpochRecord {
            epoch: EPOCH,
            final_consensus: tn_types::ConsensusNumHash::new(1, Default::default()),
            ..Default::default()
        };
        assert!(
            !rejoin_epoch_is_open(EPOCH, EPOCH, Some(&finalized)),
            "a fresh header from a finalized epoch must not stop catch-up"
        );
        let older = EPOCH.saturating_sub(1);
        assert!(!rejoin_epoch_is_open(older, older, Some(&finalized)));
        assert!(rejoin_epoch_is_open(EPOCH + 1, EPOCH + 1, Some(&finalized)));
        assert!(rejoin_epoch_is_open(0, 0, None));
        assert!(rejoin_epoch_is_open(0, 0, Some(&EpochRecord::default())));
        let finalized_zero = EpochRecord { epoch: 0, ..finalized };
        assert!(!rejoin_epoch_is_open(0, 0, Some(&finalized_zero)));
    }

    /// A handled output ahead of the newest known header has a gap of zero.
    #[test]
    fn rejoin_when_ahead_of_newest_header() {
        let window = rejoin_round_window(MAX_GC_DEPTH);
        assert!(
            rejoin_ready(EPOCH, NEWEST + 2, EPOCH, NEWEST, FRESH, window),
            "a node past the newest known header must rejoin"
        );
    }

    /// For every production `gc_depth`, promotion sits strictly inside the demotion window.
    #[test]
    fn rejoin_window_inside_demotion_window() {
        assert!(
            ((GC_ACTIVITY_BUFFER + 1)..=MAX_GC_DEPTH)
                .all(|gc_depth| rejoin_round_window(gc_depth) < gc_activity_window(gc_depth)),
            "the rejoin window must be smaller than the demotion window"
        );
    }

    /// A newest known header in [`EPOCH`] at round [`NEWEST`] with the given age.
    fn newest_header(epoch: Epoch, age: Duration) -> NewestHeader {
        NewestHeader { number: 9, epoch, round: NEWEST, age }
    }

    /// Each blocker is reported for the first condition that refuses promotion.
    #[test]
    fn rejoin_blocker_names_first_failed_condition() {
        let fresh = newest_header(EPOCH, FRESH);
        let stale = newest_header(EPOCH, REJOIN_HEAD_MAX_AGE);
        let other_epoch = newest_header(EPOCH + 1, FRESH);
        let cases = [
            (false, Some(&fresh), RejoinBlocker::EpochClosed),
            (false, None, RejoinBlocker::EpochClosed),
            (true, None, RejoinBlocker::NoKnownHeader),
            (true, Some(&stale), RejoinBlocker::StaleHead),
            (true, Some(&newest_header(EPOCH + 1, REJOIN_HEAD_MAX_AGE)), RejoinBlocker::StaleHead),
            (true, Some(&other_epoch), RejoinBlocker::EpochMismatch),
            (true, Some(&fresh), RejoinBlocker::RoundGap),
        ];
        for (epoch_is_open, newest, expected) in cases {
            assert_eq!(
                RejoinBlocker::classify(epoch_is_open, newest, EPOCH),
                expected,
                "open={epoch_is_open} newest={newest:?}"
            );
        }
        let labels =
            ["epoch_closed", "no_known_header", "stale_head", "epoch_mismatch", "round_gap"];
        assert_eq!(RejoinBlocker::ALL.map(RejoinBlocker::as_str), labels);
        for (index, blocker) in RejoinBlocker::ALL.into_iter().enumerate() {
            assert_eq!(blocker.index(), index);
        }
    }

    /// Whenever the readiness check refuses an open epoch with a known header, the blocker names a
    /// condition that holds, and `round_gap` only when the gap really exceeds the window.
    #[test]
    fn rejoin_blocker_agrees_with_rejoin_ready() {
        let window = rejoin_round_window(MAX_GC_DEPTH);
        let ages = [Duration::ZERO, FRESH, REJOIN_HEAD_MAX_AGE, Duration::from_secs(60)];
        for age in ages {
            for newest_epoch in [EPOCH - 1, EPOCH, EPOCH + 1] {
                for handled_round in [0, NEWEST - window - 1, NEWEST - window, NEWEST, NEWEST + 1] {
                    let newest = newest_header(newest_epoch, age);
                    if rejoin_ready(EPOCH, handled_round, newest_epoch, NEWEST, age, window) {
                        continue;
                    }
                    let holds = match RejoinBlocker::classify(true, Some(&newest), EPOCH) {
                        RejoinBlocker::StaleHead => age >= REJOIN_HEAD_MAX_AGE,
                        RejoinBlocker::EpochMismatch => newest_epoch != EPOCH,
                        RejoinBlocker::RoundGap => NEWEST.saturating_sub(handled_round) > window,
                        RejoinBlocker::EpochClosed | RejoinBlocker::NoKnownHeader => false,
                    };
                    assert!(holds, "age={age:?} epoch={newest_epoch} round={handled_round}");
                }
            }
        }
    }

    /// The blocked counter registers one series per blocker under its `reason` label.
    #[test]
    fn rejoin_blocked_counters_register_labelled_series() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let counters = rejoin_blocked_counters();
            counters[RejoinBlocker::EpochClosed.index()].increment(3);
            counters[RejoinBlocker::RoundGap.index()].increment(1);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        for blocker in RejoinBlocker::ALL {
            let value = snapshot
                .iter()
                .find(|(key, ..)| {
                    let key = key.key();
                    key.name() == "tn_executor.rejoin_blocked_total"
                        && key
                            .labels()
                            .map(|label| (label.key(), label.value()))
                            .eq([("reason", blocker.as_str())])
                })
                .map(|(.., value)| value)
                .unwrap_or_else(|| panic!("reason={} not registered", blocker.as_str()));
            let expected = match blocker {
                RejoinBlocker::EpochClosed => 3,
                RejoinBlocker::RoundGap => 1,
                _ => 0,
            };
            assert_eq!(value, &DebugValue::Counter(expected), "reason={}", blocker.as_str());
        }
    }

    /// The not-ready line is logged on the first check, on every change of blocker and otherwise
    /// once the interval has passed since the last line.
    #[test]
    fn rejoin_log_throttle_logs_on_change_or_interval() {
        let start = Instant::now();
        let at = |millis: u64| start + Duration::from_millis(millis);
        let interval = u64::try_from(REJOIN_NOT_READY_LOG_INTERVAL.as_millis()).unwrap();
        let mut throttle = RejoinLogThrottle::default();

        assert!(throttle.should_log(RejoinBlocker::EpochClosed, at(0)), "first check logs");
        assert!(!throttle.should_log(RejoinBlocker::EpochClosed, at(10)), "repeat is throttled");
        assert!(throttle.should_log(RejoinBlocker::RoundGap, at(20)), "a new blocker logs");
        assert!(throttle.should_log(RejoinBlocker::EpochClosed, at(30)), "a change back logs");
        assert!(
            !throttle.should_log(RejoinBlocker::EpochClosed, at(30 + interval - 1)),
            "a repeat inside the interval is throttled"
        );
        assert!(
            throttle.should_log(RejoinBlocker::EpochClosed, at(30 + interval)),
            "a repeat once the interval has passed logs"
        );
        assert!(
            !throttle.should_log(RejoinBlocker::EpochClosed, at(30 + interval + 1)),
            "the interval restarts at the last line"
        );
        // an earlier stamp than the last line saturates to zero instead of panicking
        assert!(!throttle.should_log(RejoinBlocker::EpochClosed, start));
    }
}
