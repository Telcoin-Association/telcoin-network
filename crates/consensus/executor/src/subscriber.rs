//! Subscriber gathers all information needed from consensus and forwards to the execution engine.

use crate::{errors::SubscriberResult, metrics::ExecutorMetrics, SubscriberError};
use futures::{stream::FuturesOrdered, StreamExt, TryStreamExt};
use state_sync::{last_consensus_parent, save_consensus, spawn_state_sync};
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, PrimaryToWorkerClient};
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus, ConsensusBusApp, NodeMode};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    encode, to_intent_message, Address, AuthorityIdentifier, Batch, BlockHash, BlsSigner as _,
    CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusOutput, ConsensusResult, Database, Hash as _, Noticer, TaskManager, TaskSpawner,
    Timestamp, TimestampSec, TnReceiver, TnSender,
};
use tracing::{debug, error, info, instrument, warn};

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
    /// The client to request worker batches and build consensus output.
    client: LocalNetwork,
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
    let client = config.local_network().clone();
    let mode = consensus_bus.app().current_node_mode();
    info!(target: "tn::observer", node_mode = ?mode, "subscriber starting in mode");
    let subscriber = Subscriber {
        consensus_bus: consensus_bus.app().clone(),
        config,
        network_handle,
        inner: Arc::new(Inner {
            authority_id,
            committee,
            client,
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
        // written to the main pack in order — drop the staging dir.
        if self.inner.consensus_chain.staging_final() == Some(number) {
            self.inner.consensus_chain.clear_staging();
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
    async fn catch_up_rejoin_consensus(&self, tasks: TaskSpawner) -> SubscriberResult<()> {
        // Get a receiver and then stream any missing outputs so we don't miss them.
        let mut rx_sync_output = self.consensus_bus.subscribe_sync_output();
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            tasks,
            self.inner.consensus_chain.clone(),
        );
        while let Some(output) = rx_sync_output.recv().await {
            let consensus_header_number = output.number();
            self.handle_sync_output(output).await?;
            if let Some(last_consensus_header) =
                self.consensus_bus.last_consensus_header().borrow().as_ref()
            {
                // If we seem to be on the same number also make sure this is not a stale record.
                // If that happens during a catch up it will lead to premature cvv active when not
                // caught up.
                if consensus_header_number == last_consensus_header.number
                    && last_consensus_header.sub_dag.commit_timestamp().elapsed()
                        < Duration::from_secs(5)
                {
                    // We are caught up enough so try to jump back into consensus
                    info!(target: "subscriber", "attempting to rejoin consensus, consensus block height {consensus_header_number}");
                    self.consensus_bus.node_mode().send_replace(NodeMode::CvvActive);
                    self.config.shutdown().notify();
                    return Ok(());
                }
            }
        }
        Ok(())
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
    /// `ConsensusHeader` off of this parent.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(ConsensusHeaderDigest, u64)> {
        let result = last_consensus_parent(&self.consensus_bus, &self.inner.consensus_chain).await;

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
                    if sub_dag.commit_timestamp() >= self.inner.epoch_boundary { epoch_done = true; }
                    debug!(target: "subscriber", subdag=?sub_dag.digest(), round=?sub_dag.leader_round(), "received committed subdag from consensus");
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    let parent_hash = last_parent;
                    let number = last_number + 1;
                    last_number += 1;
                    last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
                    waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(output) = waiting.next() => {
                    match output {
                        Ok(output) => self.handle_consensus_output(&mut consensus_chain, output).await?,
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
    #[instrument(level = "debug", skip_all, fields(number))]
    async fn fetch_batches(
        &self,
        sub_dag: CommittedSubDag,
        parent_hash: ConsensusHeaderDigest,
        number: u64,
    ) -> SubscriberResult<ConsensusOutput> {
        let num_blocks = sub_dag.num_primary_batches();
        let num_certs = sub_dag.len();

        if num_blocks == 0 {
            debug!(target: "subscriber", "No blocks to fetch, payload is empty");
            return Ok(ConsensusOutput::new_with_subdag(sub_dag, parent_hash, number));
        }

        let mut batch_set: BTreeSet<BlockHash> = BTreeSet::new();

        let mut batch_digests = VecDeque::with_capacity(num_certs);
        for header in sub_dag.headers() {
            for (digest, _) in header.payload().iter() {
                batch_set.insert(*digest);
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
        let mut fetched_batches = self.fetch_batches_from_peers(batch_set).await?;

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

    /// Send message to relevant workers to fetch batches for execution.
    ///
    /// The worker is responsible for retrieving the batch from it's local DB or fetching from
    /// peers.
    async fn fetch_batches_from_peers(
        &self,
        batch_digests: BTreeSet<BlockHash>,
    ) -> SubscriberResult<HashMap<BlockHash, Batch>> {
        let mut fetched_blocks = HashMap::new();

        debug!(target: "subscriber", "Attempting to fetch {} digests from workers", batch_digests.len());
        let batches = match self.inner.client.fetch_batches(batch_digests).await {
            Ok(resp) => resp,
            Err(e) => {
                error!(target: "subscriber", "Failed to fetch batches from peers: {e:?}");
                self.inner.metrics.batch_fetch_failures_total.increment(1);
                return Err(SubscriberError::ClientRequestsFailed);
            }
        };

        for (digest, block) in batches.into_iter() {
            debug!(
                target: "subscriber",
                "Block {:?} took {:?} seconds since it was received to when it was fetched for execution",
                digest,
                block.received_at().map(|t| t.elapsed().as_secs_f64()),
            );

            fetched_blocks.insert(digest, block);
        }

        Ok(fetched_blocks)
    }
}

/// Drain the batch-fetch futures still pending when graceful shutdown fires.
///
/// This is the shutdown counterpart of the steady-state arm in [`Subscriber::run`] and is
/// deliberately symmetric with it: a fetch `Err` is fail-stop, not skippable. `waiting` is a
/// [`FuturesOrdered`] that yields in consensus (push) order, so every `Ok` output ahead of a
/// failed fetch is the contiguous prefix. `try_fold` threads the consensus chain as its
/// accumulator and short-circuits at the first error, so the drain saves and broadcasts that
/// contiguous prefix and then stops at the gap: nothing at or past the failed fetch is saved or
/// broadcast. In steady state the same fetch `Err` returns an error that shuts the node down;
/// here, already shutting down, the fold simply stops, leaving the drained prefix persisted for
/// the Phase-2 DB drain and normal restart. Stopping here makes the shutdown path self-evidently
/// fail-stop instead of relying on the downstream pack/load/replay guards to reject the
/// non-contiguous consensus number a swallowed fetch `Err` would otherwise let it march past.
///
/// `take_until` bounds only the wait for the next completed fetch, not an in-flight save: once
/// `deadline` passes the drain stops pulling new outputs, but an output already dequeued is always
/// saved and broadcast to completion. This preserves the original select-based drain's guarantee
/// that a committed output, once dequeued, is never dropped mid-save during graceful shutdown.
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
                warn!(target: "subscriber", "error saving consensus during shutdown: {e}");
                Err(())
            } else {
                // Best-effort broadcast: if epoch manager already exited, this is a no-op.
                // The DB-aware drain (Phase 2) handles the gap regardless.
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

#[cfg(test)]
mod tests {
    use super::drain_pending_on_shutdown;
    use crate::SubscriberError;
    use futures::{future, stream::FuturesOrdered};
    use std::{
        collections::{BTreeSet, VecDeque},
        time::Duration,
    };
    use tempfile::TempDir;
    use tn_primary::ConsensusBusApp;
    use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase};
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        Certificate, CommittedSubDag, Committee, ConsensusHeader, ConsensusHeaderDigest,
        ConsensusOutput, ReputationScores, TnReceiver as _, B256,
    };

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
        );
        let digest = ConsensusHeader::digest_from_parts(parent, &sub_dag, number);
        let output = ConsensusOutput::new(sub_dag, parent, number, false, VecDeque::new(), vec![]);
        (output, digest)
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

        let genesis: BTreeSet<_> = fixture.genesis().collect();
        let (_, headers) = fixture.headers_round(0, &genesis);
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
        let genesis: BTreeSet<_> = fixture.genesis().collect();
        let (_, headers) = fixture.headers_round(0, &genesis);
        let certificates: Vec<_> = headers.iter().map(|h| fixture.certificate(h)).collect();
        let leader = certificates.last().cloned().unwrap();

        let parent0: ConsensusHeaderDigest = B256::ZERO.into();
        let sub_dag1 = CommittedSubDag::new(
            certificates.clone(),
            leader.clone(),
            0,
            ReputationScores::new(&committee),
            None,
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
}
