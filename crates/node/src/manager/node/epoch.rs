//! The impl for node manager type's epoch functions.
//!
//! This oversees the tasks that run for each epoch. Some consensus-related
//! tasks run for one epoch. Other resources are shared across epochs.
//! This file defines the struct and the epoch scoped code.

use crate::{
    engine::ExecutionNode,
    manager::EpochManager,
    primary::PrimaryNode,
    worker::{worker_task_manager_name, WorkerNode},
    EngineToPrimaryRpc,
};
use eyre::{eyre, OptionExt};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, ConsensusConfig, NetworkConfig, TelcoinDirs};
use tn_network_libp2p::{error::NetworkError, types::NetworkHandle, TNMessage};
use tn_primary::{
    network::{PrimaryNetwork, PrimaryNetworkHandle},
    ConsensusBus, NodeMode, StateSynchronizer,
};
use tn_reth::{
    recover_raw_transaction,
    system_calls::{
        ConsensusRegistry::{self, EpochInfo},
        EpochState,
    },
};
use tn_storage::tables::{
    CertificateDigestByOrigin, CertificateDigestByRound, Certificates, LastProposed,
    NodeBatchesCache, Payload, Votes,
};
use tn_types::{
    gas_accumulator::{compute_next_base_fee_eip1559, GasAccumulator, WorkerFeeConfig},
    Batch, BatchValidation, BlockHash, BlockNumHash, BlsPublicKey, Committee, CommitteeBuilder,
    ConsensusOutput, Database as TNDatabase, Epoch, EpochRecord, Multiaddr, NetworkPublicKey,
    Notifier, P2pNode, TaskJoinError, TaskManager, TaskSpawner, TnReceiver, B256,
};
use tn_worker::{quorum_waiter::QuorumWaiterTrait, Worker, WorkerNetwork, WorkerNetworkHandle};
use tokio::sync::mpsc;
use tracing::{debug, error, info, info_span, warn, Instrument};

/// The epoch-specific task manager name.
const EPOCH_TASK_MANAGER: &str = "Epoch Task Manager";

/// Result from replaying missed consensus outputs.
struct ReplayResult {
    /// If the epoch boundary was crossed during replay, this is the hash to close the epoch with.
    epoch_close_hash: Option<BlockHash>,
    /// The hash of the last consensus output that was actually forwarded to the engine.
    last_replayed_hash: Option<BlockHash>,
}

/// Modes for an epoch.
#[derive(Debug, Copy, Clone)]
pub(crate) enum RunEpochMode {
    /// This is the initial epoch when the system starts, will need to get established.
    Initial,
    /// We re-ran an epoch as a result of a node change (CVV is to far behind or CVV has caught up
    /// for instance).
    ModeChange,
    /// This a fresh new epoch on an already running node.
    NewEpoch,
}

impl RunEpochMode {
    fn replay_consensus(&self) -> bool {
        match self {
            RunEpochMode::ModeChange => false,
            RunEpochMode::Initial | RunEpochMode::NewEpoch => true,
        }
    }

    fn initial_epoch(&self) -> bool {
        matches!(self, RunEpochMode::Initial)
    }
}

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Run a single epoch.
    pub(crate) async fn run_epoch(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        epoch_mode: RunEpochMode,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<RunEpochMode> {
        info!(target: "epoch-manager", "Starting epoch");

        // Create a new bus wrapping the application channels and adding the epoch specific
        // channels.
        let consensus_bus = ConsensusBus::new_with_app(self.consensus_bus.clone());
        self.last_consensus_header = None;
        self.last_forwarded_consensus_number = 0;
        // We have not created this epoch's primary yet (no committee) so get it from chain
        // ourselves... Note, any consensus output to replay should be in the same epoch...
        let (committee, epoch_info, epoch_start) =
            self.get_committee_with_epoch_start_info(engine).await?;
        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;

        // The task manager that resets every epoch and manages
        // short-running tasks for the lifetime of the epoch.
        let mut epoch_task_manager = TaskManager::new(EPOCH_TASK_MANAGER);
        // Do not wait long for tasks to exit, just drop them and move on to next epoch.
        epoch_task_manager.set_join_wait(200);

        self.open_epoch_pack(committee.clone(), epoch_task_manager.get_spawner()).await?;
        if epoch_mode.replay_consensus() {
            // If we are starting up then make sure that any consensus we previously validated goes
            // to the engine and is executed.  Otherwise we could miss consensus execution.
            gas_accumulator.rewards_counter().set_committee(committee.clone());
            let replay = self.replay_missed_consensus(committee, to_engine).await?;
            if let Some(target_hash) = replay.epoch_close_hash {
                // If things go down at exactly the wrong time we might have to replay the epoch end
                // so account for that.
                self.close_epoch(None, engine, &gas_accumulator, target_hash).await?;
                return Ok(RunEpochMode::NewEpoch);
            }
            // Only wait for consensus that was actually forwarded to the engine.
            // Waiting on DB-latest consensus could hang if it was saved but never sent.
            if let Some(last_hash) = replay.last_replayed_hash {
                info!(target: "epoch-manager", "Waiting for execution of replayed consensus {last_hash}");
                self.consensus_bus.wait_for_consensus_execution(last_hash).await?;
                info!(target: "epoch-manager", "Confirmed execution of replayed consensus {last_hash}");
            }
        }

        let node_ended = self.node_shutdown.subscribe();

        // subscribe to output early to prevent missed messages
        let mut consensus_output = self.consensus_bus.subscribe_consensus_output();

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(
                engine,
                &epoch_task_manager,
                network_config,
                epoch_mode.initial_epoch(),
                gas_accumulator.clone(),
                consensus_bus.clone(),
            )
            .await?;
        // consensus config for shutdown subscribers
        let consensus_shutdown = primary.shutdown_signal().await;
        let epoch_shutdown_rx = consensus_shutdown.subscribe();

        // This needs to be created early so required machinery for other tasks exists when needed.
        let mut worker = worker_node.new_worker().await?;
        let current_epoch = primary.current_committee().await.epoch();

        // Produce a "dummy" epoch 0 EpochRecord if missing.
        // This will let us use simple code to find any epoch including 0 at startup.
        if !self.consensus_chain.epochs().contains_epoch(0).await {
            if current_epoch != 0 {
                return Err(eyre::eyre!(
                    "We have epoch 0 in our database if we are past epoch 0, on {current_epoch}"
                ));
            }
            // No keys for epoch 0, fix that.
            // We are on epoch 0 so load up that committee in Db as well.
            let committee: Vec<BlsPublicKey> =
                primary.current_committee().await.bls_keys().iter().copied().collect();
            let next_committee = committee.clone();
            let epoch_rec =
                EpochRecord { epoch: 0, committee, next_committee, ..Default::default() };
            // Save the "dummy" record, should be overwritten once epoch 0 closes.
            // This will NOT be signed.
            self.consensus_chain.epochs().save_dummy_epoch0(epoch_rec).await?;
        }

        gas_accumulator.rewards_counter().set_committee(primary.current_committee().await);
        // start primary
        primary.start(&epoch_task_manager, self.consensus_chain.clone()).await?;

        let worker_task_manager_name = worker_task_manager_name(worker_node.id().await);
        // start batch builder
        worker.spawn_batch_builder(&worker_task_manager_name, &epoch_task_manager);

        let batch_builder_task_spawner = epoch_task_manager.get_spawner();
        engine
            .start_batch_builder(
                worker.id(),
                worker.batches_tx(),
                &batch_builder_task_spawner,
                gas_accumulator.base_fee(worker.id()).base_fee(),
                current_epoch,
            )
            .await?;

        self.orphan_batches(&epoch_task_manager, engine.clone(), worker.clone(), current_epoch)
            .await?;

        // update tasks
        epoch_task_manager.update_tasks();

        info!(target: "epoch-manager", tasks=?epoch_task_manager, "EPOCH TASKS\n");

        // await the epoch boundary or the epoch task manager exiting
        // this can also happen due to committee nodes re-syncing and errors
        let consensus_shutdown_clone = consensus_shutdown.clone();

        // indicate if the node is restarting to join the committe or if the epoch is changed and
        // tables should be cleared
        let mut clear_tables_for_next_epoch = false;

        let mut epoch_boundary_reached = false;
        tokio::select! {
            _ = node_ended => {
                info!(target: "epoch-manager", "node exiting, epoch ending");
            },
            // wait for epoch boundary to transition
            res = self.wait_for_epoch_boundary(to_engine, &mut consensus_output) => {
                // toggle bool to clear tables
                clear_tables_for_next_epoch = true;
                let target_hash = res.inspect_err(|e| {
                    error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                })?;
                self.close_epoch(Some(consensus_shutdown.clone()), engine, &gas_accumulator, target_hash)
                    .await?;

                // Write the epoch record to DB and save in manager for next epoch.
                self.write_epoch_record(&primary, engine).await?;

                info!(target: "epoch-manager", "epoch boundary success - clearing consensus db tables for next epoch");
                epoch_boundary_reached = true;
            },

            // return any errors
            res = epoch_task_manager.until_task_ends(consensus_shutdown_clone) => {
                match res {
                    Ok(()) => info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee"),
                    // There are times when the epoch task manager can exit with Ok...
                    Err(TaskJoinError::CriticalExitOk(task)) => {
                        // It is possible for the epoch to get a shutdown signal before the join.
                        // In that case it will not reconize the Ok task exit so we double check it here
                        // with a noticer that was aquired much earlier on epoch startup.
                        if epoch_shutdown_rx.noticed() {
                            info!(target: "epoch-manager", "epoch task manager exited - likely syncing with committee");
                        } else {
                            error!(target: "epoch-manager", ?task, "failed to reach epoch boundary");
                            return Err(TaskJoinError::CriticalExitOk(task).into());
                        }
                    }
                    Err(e) => {
                        error!(target: "epoch-manager", ?e, "failed to reach epoch boundary");
                        return Err(e.into());
                    }
                }
            },
        }

        let mut res = RunEpochMode::NewEpoch;
        // If the select exitted because of a join() then do not join() again- we are already
        // shutting down.
        consensus_shutdown.notify();
        // abort all epoch-related tasks
        epoch_task_manager.abort_all_tasks();
        // Expect complaints from join so swallow those errors...
        // If we timeout here something is not playing nice and shutting down so return the
        // timeout.
        tokio::time::timeout(
            Duration::from_millis(500),
            epoch_task_manager.wait_for_task_shutdown(),
        )
        .await?;
        if epoch_boundary_reached {
            // The epoch is over now and consensus should be shutdown.
            // Do a sanity clear of the consensus_output channel.
            // Note we probably do not need this anymore but not harmful.
            while let Ok(_output) = consensus_output.try_recv() {}
        } else if let Some(target_hash) =
            self.send_leftover_consensus_output_to_engine(&mut consensus_output, to_engine).await
        {
            // If things go down at exactly the wrong time we might have reached the epoch end
            // so account for that.
            self.close_epoch(None, engine, &gas_accumulator, target_hash).await?;
            res = RunEpochMode::NewEpoch;
            clear_tables_for_next_epoch = true;
        } else {
            res = RunEpochMode::ModeChange;
        }

        // clear tables
        if clear_tables_for_next_epoch {
            self.clear_consensus_db_for_next_epoch()?;
        }

        Ok(res)
    }

    /// Collect any batches that never got into consensus (at epoch change or node restart) and
    /// Re-introduce them into the mempool for inclusion in future batches.
    async fn orphan_batches<QuorumWaiter: QuorumWaiterTrait>(
        &mut self,
        epoch_task_manager: &TaskManager,
        engine: ExecutionNode,
        worker: Worker<DB, QuorumWaiter>,
        epoch: Epoch,
    ) -> eyre::Result<()> {
        // Collect any batches from this epoch that never made it to the consensus chain.
        let mut orphan_batches: Vec<(BlockHash, Batch)> = Vec::new();
        // We can not await while using the db iter so capture the digest and filter out the ones
        // that were processed.
        let digests: Vec<BlockHash> =
            self.consensus_db.iter::<NodeBatchesCache>().map(|(digest, _)| digest).collect();
        for digest in digests.into_iter() {
            if !self.consensus_chain.contains_current_batch(digest).await {
                if let Ok(Some(batch)) = self.consensus_db.get::<NodeBatchesCache>(&digest) {
                    orphan_batches.push((digest, batch));
                }
            }
        }
        // We have what we need so clear the Batch cache now.
        // Do this now vs at end of epoch so we keep the batches until we need them.
        self.consensus_db.clear_table::<NodeBatchesCache>()?;
        if !orphan_batches.is_empty() {
            let consensus_bus = self.consensus_bus.clone();
            let span =
                info_span!(target: "telcoin", "orphan-batches", epoch = tracing::field::Empty);
            span.record("epoch", epoch.to_string());
            epoch_task_manager.spawn_task("Orphaned Batches", async move {
                info!(target: "epoch-manager", "Re-introducing orphaned batchs {} transactions", orphan_batches.len());
                let pools = engine.get_all_worker_transaction_pools().await;
                let is_cvv = consensus_bus.is_active_cvv();
                for (digest, batch) in orphan_batches.drain(..) {
                    // Loop through any orphaned batches and resubmit it's transactions.
                    // This is most likely because of epoch changes but could be caused by a restart as
                    // well.
                    if is_cvv {
                        for tx_bytes in batch.transactions() {
                            // Put txn back into the mempool.
                            if let Ok(recovered) = recover_raw_transaction(tx_bytes) {
                                if let Some(pool) = pools.get(batch.worker_id as usize) {
                                    let _ = pool.add_recovered_transaction_external(recovered).await;
                                }
                            }
                        }
                    } else {
                        // If we are not a CVV then go ahead and disburse the txns from the batch directly.
                        let _ = worker.disburse_txns(batch.seal(digest)).await;
                    }
                }
            }.instrument(span));
        } else {
            info!(target: "epoch-manager", "No batches leftover");
        }
        Ok(())
    }

    /// If we have any consensus that made it into the consensus chain but was not executed
    /// then make sure we submit it to the engine for execution now.
    /// Note, this has to be called correctly or it can lead to double execution.
    async fn replay_missed_consensus(
        &mut self,
        committee: Committee,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) -> eyre::Result<ReplayResult> {
        let missing =
            state_sync::get_missing_consensus(&self.consensus_bus, &self.consensus_chain).await?;
        let mut last_replayed_hash = None;
        for consensus_header in missing.into_iter() {
            if consensus_header.sub_dag.leader_epoch() != committee.epoch() {
                error!(target: "epoch-manager", "Crossed epoch boundary with missing execution! expected epoch {} got {}",
                    committee.epoch(), consensus_header.sub_dag.leader_epoch());
                return Err(eyre::eyre!(
                    "Crossed epoch boundary with missing execution! expected epoch {} got {}",
                    committee.epoch(),
                    consensus_header.sub_dag.leader_epoch()
                ));
            }
            let consensus_output =
                self.consensus_chain.get_consensus_output_current(consensus_header.number).await?;
            let is_epoch_close = consensus_output.committed_at() >= self.epoch_boundary;
            let output_hash = consensus_output.consensus_header_hash();
            if let Err(e) = self.process_output(to_engine, consensus_output).await {
                error!(target: "epoch-manager", "error sending consensus output to engine: {}", e);
                return Err(e);
            }
            last_replayed_hash = Some(output_hash);
            if is_epoch_close {
                return Ok(ReplayResult {
                    epoch_close_hash: Some(output_hash),
                    last_replayed_hash,
                });
            }
        }
        Ok(ReplayResult { epoch_close_hash: None, last_replayed_hash })
    }

    /// Create the current committee from the current execution state.  Also return the epoch info
    /// and epoch start since this will be needed by some callers (avoid extra system calls).
    async fn get_committee_with_epoch_start_info(
        &self,
        engine: &ExecutionNode,
    ) -> eyre::Result<(Committee, EpochInfo, u64)> {
        let EpochState { epoch, epoch_info, validators, epoch_start } =
            engine.epoch_state_from_canonical_tip().await?;
        let validators = validators
            .iter()
            .map(|v| {
                let decoded_bls = BlsPublicKey::from_literal_bytes(v.blsPubkey.as_ref());
                decoded_bls.map(|decoded| (decoded, v))
            })
            .collect::<Result<HashMap<_, _>, _>>()
            .map_err(|err| eyre!("failed to create bls key from on-chain bytes: {err:?}"))?;

        Ok((self.create_committee_from_state(epoch, validators).await?, epoch_info, epoch_start))
    }

    /// Open/re-use if open the epoch pack files for the current epoch.
    async fn open_epoch_pack(
        &mut self,
        committee: Committee,
        task_spawner: TaskSpawner,
    ) -> eyre::Result<()> {
        let current_epoch = committee.epoch();
        let previous_epoch = current_epoch.saturating_sub(1);
        let previous_epoch_rec =
            self.consensus_chain.epochs().record_by_epoch(previous_epoch).await;
        let previous_epoch_rec = if let Some(rec) = previous_epoch_rec {
            // Even when the record is found, proactively trigger the epoch record
            // collector so it backfills any epoch certs that are missing (e.g. when
            // quorum failed AND the peer-fetch in manage_epoch_votes also failed because
            // the network channels had already closed after epoch shutdown).
            // Never decrease requested_missing_epoch: if the gossip handler already set it
            // to a higher epoch (e.g. 3 while we are opening epoch 3 with previous_epoch=2),
            // keep the higher value so the collector retries that epoch too.
            let current = *self.consensus_bus.requested_missing_epoch().borrow();
            self.consensus_bus.requested_missing_epoch().send_replace(current.max(previous_epoch));
            rec
        } else if previous_epoch == 0 {
            EpochRecord {
                // If we can't find the record then this we should be starting at epoch 0- use
                // this filler.
                epoch: 0,
                committee: committee.bls_keys().iter().copied().collect(),
                next_committee: committee.bls_keys().iter().copied().collect(),
                ..Default::default()
            }
        } else {
            // The previous epoch record is missing. This can happen when a node restarts while
            // catching up across multiple epoch boundaries - state sync feeds epoch-boundary
            // consensus to the engine faster than the epoch record collector fetches the records
            // from peers. Trigger the collector and wait up to 30 seconds for the record.
            // Never decrease requested_missing_epoch (same reasoning as the found-record branch).
            let current = *self.consensus_bus.requested_missing_epoch().borrow();
            self.consensus_bus.requested_missing_epoch().send_replace(current.max(previous_epoch));
            warn!(target: "epoch-manager", previous_epoch, current_epoch, "missing previous epoch record, waiting for epoch record collector");

            // Pre-dial committee peers before blocking so the epoch record collector can connect.
            // Without this we deadlock: open_epoch_pack blocks here waiting for the record, but
            // peer connections are only established in spawn_primary_network_for_epoch which runs
            // after open_epoch_pack returns.
            let primary_network_handle =
                self.primary_network_handle.as_ref().expect("primary network");
            if primary_network_handle.connected_peers_count().await.unwrap_or_default() == 0 {
                let committee_keys: HashSet<BlsPublicKey> =
                    committee.bls_keys().into_iter().collect();
                let _ = primary_network_handle.inner_handle().new_epoch(committee_keys).await;
                for bls_key in committee.bls_keys() {
                    self.dial_peer_bls(
                        primary_network_handle.inner_handle().clone(),
                        bls_key,
                        task_spawner.clone(),
                    );
                }
            }

            if let Some(rec) = self
                .consensus_chain
                .epochs()
                .record_by_epoch_with_timeout(previous_epoch, Duration::from_secs(30))
                .await
            {
                rec
            } else {
                return Err(eyre::eyre!(
                    "Missing previous epoch record for epoch {previous_epoch} after waiting"
                ));
            }
        };
        self.consensus_chain.new_epoch(previous_epoch_rec, committee).await?;
        Ok(())
    }

    async fn process_output(
        &mut self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        mut output: ConsensusOutput,
    ) -> eyre::Result<()> {
        let last_forwarded_consensus_number = output.number();
        if output.committed_at() >= self.epoch_boundary {
            // update output so engine closes epoch
            output.close_epoch = true;
        }
        // only forward the output to the engine
        to_engine.send(output).await?;
        // store number after successful send
        self.last_forwarded_consensus_number = last_forwarded_consensus_number;
        Ok(())
    }

    // We stopped waiting on the epoch boundary so lets make sure that the consensus queue
    // is sent to the engine. If we don't do this it is possible that a quick
    // exit could orphan output (for instance a CVV that is behind).
    // We need to go until all the consensus output in DB has been sent to the engine (if it was
    // saved it should have been sent).
    async fn send_leftover_consensus_output_to_engine(
        &mut self,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
        to_engine: &mpsc::Sender<ConsensusOutput>,
    ) -> Option<BlockHash> {
        // Phase 1: Drain broadcast channel (existing behavior)
        while let Ok(output) = consensus_output.try_recv() {
            let result = if output.committed_at() >= self.epoch_boundary {
                Some(output.consensus_header_hash())
            } else {
                None
            };
            // only forward the output to the engine
            let _ = self.process_output(to_engine, output).await;
            if result.is_some() {
                return result;
            }
        }

        // Phase 2: Check DB for outputs saved during subscriber shutdown drain.
        // During shutdown, the subscriber saves outputs to the pack file DB but may not
        // broadcast them through the channel. Scan for any gap between the last output
        // we forwarded and the DB latest, loading missing entries from the pack file.
        let latest_db = self.consensus_chain.latest_consensus_number();
        let last_sent = self.last_forwarded_consensus_number;
        if latest_db > last_sent {
            for number in (last_sent + 1)..=latest_db {
                match self.consensus_chain.get_consensus_output_current(number).await {
                    Ok(output) => {
                        let result = if output.committed_at() >= self.epoch_boundary {
                            Some(output.consensus_header_hash())
                        } else {
                            None
                        };
                        let _ = self.process_output(to_engine, output).await;
                        if result.is_some() {
                            return result;
                        }
                    }
                    Err(e) => {
                        warn!(target: "epoch-manager", number, ?e, "failed to load gap consensus from DB");
                        break;
                    }
                }
            }
        }

        None
    }

    /// Record the epoch record for just completed epoch in our DB.
    /// Also record this in the manager for posible signing/collection of signatures.
    async fn write_epoch_record(
        &mut self,
        primary: &PrimaryNode<DB>,
        engine: &ExecutionNode,
    ) -> eyre::Result<()> {
        let committee = primary.current_committee().await;
        let epoch = committee.epoch();
        if epoch == 0 {
            // Epoch 0 will have a "dummy" epoch record to make the initial committee avaliable to
            // code using these records. In this case there will not be a cert so we
            // want to overwrite this with the correct record. That is why we need to
            // use Some(_) (this means we have a certificate) instead of _ like in the general case.
            // Without this we never overwrite the dummy epoch 0 record with the proper record and
            // would break sync.
            if let Some((epoch_rec, Some(_))) =
                self.consensus_chain.epochs().get_epoch_by_number(epoch).await
            {
                // We already have this record...
                self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
                return Ok(());
            }
        } else if let Some((epoch_rec, _)) =
            self.consensus_chain.epochs().get_epoch_by_number(epoch).await
        {
            // We already have this record...
            self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
            return Ok(());
        }

        let committee_keys = engine.validators_for_epoch(epoch).await?;
        let next_committee_keys = engine.validators_for_epoch(epoch + 1).await?;
        let parent_hash = if epoch == 0 {
            B256::default()
        } else if let Some(prev) = self.consensus_chain.epochs().record_by_epoch(epoch - 1).await {
            if committee_keys != prev.next_committee {
                error!(
                    target: "epoch-manager",
                    "Last epochs next committee not equal to this epochs committee! previous {:?}, current {:?}",
                    prev.next_committee,
                    committee_keys
                );
                return Err(eyre!(
                    "Last epochs next committee not equal to this epochs committee!"
                ));
            }
            prev.digest()
        } else {
            error!(
                target: "epoch-manager",
                "failed to find previous epoch record when starting epoch",
            );
            return Err(eyre!("failed to find previous epoch record when starting epoch"));
        };
        let last_consensus_header = self
            .last_consensus_header
            .take()
            .expect("epoch was finished with last consensus header");
        let target_hash = last_consensus_header.digest();
        let parent_state = self.consensus_bus.latest_execution_block_num_hash();

        let epoch_rec = EpochRecord {
            epoch,
            committee: committee_keys,
            next_committee: next_committee_keys,
            parent_hash,
            final_state: parent_state,
            final_consensus: BlockNumHash::new(last_consensus_header.number, target_hash),
        };

        self.consensus_chain.epochs().save_record(epoch_rec.clone()).await?;
        self.consensus_bus.epoch_record_watch().send_replace(Some(epoch_rec));
        Ok(())
    }

    /// Monitor consensus output for the last block of the epoch.
    ///
    /// This method forwards all consensus output to the engine for execution.
    /// Once the epoch boundary is reached, the manager initiates the epoch transitions.
    async fn wait_for_epoch_boundary(
        &mut self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
    ) -> eyre::Result<B256> {
        // receive output from consensus and forward to engine
        while let Some(mut output) = consensus_output.recv().await {
            // observe epoch boundary to initiate epoch transition
            if output.committed_at() >= self.epoch_boundary {
                info!(
                    target: "epoch-manager",
                    epoch=?output.leader().epoch(),
                    commit=?output.committed_at(),
                    epoch_boundary=?self.epoch_boundary,
                    "epoch boundary detected",
                );
                // update output so engine closes epoch
                output.close_epoch = true;

                // obtain hash to monitor execution progress
                let target_hash = output.consensus_header_hash();

                self.last_consensus_header = Some(output.clone().into());
                // forward the output to the engine
                self.process_output(to_engine, output).await?;
                return Ok(target_hash);
            } else {
                // only forward the output to the engine
                self.process_output(to_engine, output).await?;
            }
        }
        Err(eyre::eyre!("invalid wait for epoch end"))
    }

    /// Use accumulated gas information to set each worker's base fee for the next epoch.
    ///
    /// Reads each worker's fee config from the [`WorkerConfigs`] contract and applies
    /// the appropriate strategy:
    /// - **EIP-1559**: adjusts up to ±12.5% based on gas utilization vs target.
    /// - **Static**: sets the fee to the governance-configured value.
    ///
    /// If the contract read fails, logs a warning and keeps current base fees unchanged.
    async fn adjust_base_fees(&self, engine: &ExecutionNode, gas_accumulator: &GasAccumulator) {
        let reth_env = engine.get_reth_env().await;
        let num_workers = gas_accumulator.num_workers();

        let configs = match reth_env.get_worker_fee_configs(num_workers) {
            Ok(c) => c,
            Err(e) => {
                error!(target: "epoch-manager", ?e, "failed to read worker fee configs, keeping current base fees");
                return; // noop
            }
        };

        for (worker_id, config) in configs.iter().enumerate() {
            let worker_id = worker_id as u16;
            let base_fee_container = gas_accumulator.base_fee(worker_id);
            let current = base_fee_container.base_fee();

            let new_fee = match config {
                WorkerFeeConfig::Eip1559 { target_gas } => {
                    let (_blocks, gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
                    compute_next_base_fee_eip1559(current, gas_used, *target_gas)
                }
                WorkerFeeConfig::Static { fee } => *fee,
            };

            base_fee_container.set_base_fee(new_fee);
            info!(target: "epoch-manager", worker_id, current, new_fee, "base fee adjusted");
        }
    }

    /// Close an epoch after wait_for_epoch_boundary returns.
    async fn close_epoch(
        &self,
        shutdown_consensus: Option<Notifier>,
        engine: &ExecutionNode,
        gas_accumulator: &GasAccumulator,
        target_hash: B256,
    ) -> eyre::Result<()> {
        // begin consensus shutdown while engine executes
        if let Some(s) = shutdown_consensus {
            s.notify()
        }
        self.consensus_bus.wait_for_consensus_execution(target_hash).await?;
        self.adjust_base_fees(engine, gas_accumulator).await;
        gas_accumulator.clear(); // Clear the accumlated values for next epoch.
        Ok(())
    }

    /// Helper method to create all consensus-related components for this epoch.
    ///
    /// Consensus components are short-lived and only relevant for the current epoch.
    async fn create_consensus(
        &mut self,
        engine: &ExecutionNode,
        epoch_task_manager: &TaskManager,
        network_config: &NetworkConfig,
        initial_epoch: bool,
        gas_accumulator: GasAccumulator,
        consensus_bus: ConsensusBus,
    ) -> eyre::Result<(PrimaryNode<DB>, WorkerNode<DB>)> {
        // create config for consensus
        let (consensus_config, preload_keys) =
            self.configure_consensus(engine, network_config).await?;
        let _mode = self.identify_node_mode(&consensus_config, &consensus_bus).await?;

        let consensus_bus_app = consensus_bus.app().clone();
        let primary = self
            .create_primary_node_components(
                &consensus_config,
                epoch_task_manager.get_spawner(),
                initial_epoch,
                consensus_bus,
            )
            .await?;

        let engine_to_primary =
            EngineToPrimaryRpc::new(consensus_bus_app, self.consensus_chain.clone());
        // only spawns one worker for now
        let worker = self
            .spawn_worker_node_components(
                &consensus_config,
                engine,
                epoch_task_manager.get_spawner(),
                initial_epoch,
                engine_to_primary,
                gas_accumulator,
            )
            .await?;

        let primary_handle = primary.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = primary_handle.inner_handle().find_authorities(prefetches).await;
        let worker_handle = worker.network_handle().await;
        let prefetches = preload_keys.clone();
        // Attempt to pre-load the next couple of committee's network info.
        let _ = worker_handle.inner_handle().find_authorities(prefetches).await;
        Ok((primary, worker))
    }

    /// Configure consensus for the current epoch.
    ///
    /// This method reads the canonical tip to read the epoch information needed
    /// to create the current committee and the consensus config.
    async fn configure_consensus(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
    ) -> eyre::Result<(ConsensusConfig<DB>, Vec<BlsPublicKey>)> {
        // retrieve epoch information from canonical tip
        let (committee, epoch_info, epoch_start) =
            self.get_committee_with_epoch_start_info(engine).await?;
        let validators = committee.bls_keys();

        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        debug!(target: "epoch-manager", new_epoch_boundary=self.epoch_boundary, "resetting epoch boundary");

        debug!(target: "epoch-manager", ?validators, "creating committee for validators");

        let mut next_vals: HashSet<BlsPublicKey> = HashSet::new();
        next_vals.extend(validators.iter());

        next_vals.extend(engine.validators_for_epoch(committee.epoch() + 1).await?);
        next_vals.extend(engine.validators_for_epoch(committee.epoch() + 2).await?);

        // create config for consensus
        let consensus_config = ConsensusConfig::new_for_epoch(
            self.builder.tn_config.clone(),
            self.consensus_db.clone(),
            self.key_config.clone(),
            committee,
            network_config.clone(),
        )?;

        Ok((consensus_config, next_vals.into_iter().collect()))
    }

    /// Create the [Committee] for the current epoch.
    ///
    /// This is the first step for configuring consensus.
    async fn create_committee_from_state(
        &self,
        epoch: Epoch,
        validators: HashMap<BlsPublicKey, &ConsensusRegistry::ValidatorInfo>,
    ) -> eyre::Result<Committee> {
        info!(target: "epoch-manager", "creating committee from state");

        // the network must be live
        let committee = if epoch == 0 {
            // read from fs for genesis
            Config::load_from_path_or_default::<Committee>(
                self.tn_datadir.committee_path(),
                ConfigFmt::YAML,
            )?
        } else {
            let mut committee_builder = CommitteeBuilder::new(epoch);

            for validator in validators {
                committee_builder.add_authority(validator.0, validator.1.validatorAddress);
            }
            committee_builder.build()
        };

        Ok(committee)
    }

    /// Create a [PrimaryNode].
    ///
    /// This also creates the [PrimaryNetwork].
    async fn create_primary_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
        consensus_bus: ConsensusBus,
    ) -> eyre::Result<PrimaryNode<DB>> {
        let state_sync = StateSynchronizer::new(
            consensus_config.clone(),
            consensus_bus.clone(),
            epoch_task_spawner.clone(),
        );
        let network_handle = self
            .primary_network_handle
            .as_ref()
            .ok_or_eyre("primary network handle missing from epoch manager")?
            .clone();

        // create the epoch-specific `PrimaryNetwork`
        self.spawn_primary_network_for_epoch(
            consensus_config,
            state_sync.clone(),
            epoch_task_spawner.clone(),
            &network_handle,
            initial_epoch,
            consensus_bus.clone(),
        )
        .await?;

        // spawn primary - create node and spawn network
        let primary = PrimaryNode::new(
            consensus_config.clone(),
            consensus_bus,
            network_handle,
            state_sync,
            self.epoch_boundary,
        );

        Ok(primary)
    }

    /// Create a [WorkerNode].
    async fn spawn_worker_node_components(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        engine: &ExecutionNode,
        epoch_task_spawner: TaskSpawner,
        initial_epoch: bool,
        engine_to_primary: EngineToPrimaryRpc,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<WorkerNode<DB>> {
        // only support one worker for now (with id 0) - otherwise, loop here
        let worker_id = 0;
        let base_fee = gas_accumulator.base_fee(worker_id);

        // update the network handle's task spawner for reporting batches in the epoch
        {
            let network_handle = self
                .worker_network_handle
                .as_mut()
                .ok_or_eyre("worker network handle missing from epoch manager")?;

            network_handle.update_task_spawner(epoch_task_spawner.clone());
            // initialize worker components on startup
            // This will use the new epoch_task_spawner on network_handle.
            // Also initialize if workers are empty: this happens when the first epoch returns
            // early from replay_missed_consensus (epoch boundary hit) before create_consensus
            // is reached, leaving workers uninitialized.
            if initial_epoch || !engine.are_workers_initialized().await {
                engine
                    .initialize_worker_components(
                        worker_id,
                        network_handle.clone(),
                        engine_to_primary,
                    )
                    .await?;
            } else {
                // We updated our epoch task spawner so make sure worker network tasks are
                // restarted.
                engine.respawn_worker_network_tasks(network_handle.clone()).await;
            }
        }

        let network_handle = self
            .worker_network_handle
            .as_ref()
            .ok_or_eyre("worker network handle missing from epoch manager")?
            .clone();

        let validator = engine
            .new_batch_validator(&worker_id, base_fee, consensus_config.committee().epoch())
            .await;
        self.spawn_worker_network_for_epoch(
            consensus_config,
            &worker_id,
            validator.clone(),
            epoch_task_spawner,
            &network_handle,
            initial_epoch,
        )
        .await?;

        let worker =
            WorkerNode::new(worker_id, consensus_config.clone(), network_handle.clone(), validator);

        Ok(worker)
    }

    /// Create the primary network for the specific epoch.
    ///
    /// This is not the swarm level, but the [PrimaryNetwork] interface.
    async fn spawn_primary_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        state_sync: StateSynchronizer<DB>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &PrimaryNetworkHandle,
        initial_epoch: bool,
        consensus_bus: ConsensusBus,
    ) -> eyre::Result<()> {
        // get event streams for the primary network handler
        let rx_event_stream = self.consensus_bus.subscribe_primary_network_events();

        // set committee for network to prevent banning
        debug!(target: "epoch-manager", auth=?consensus_config.authority_id(), "spawning primary network for epoch");
        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        let bootstrap_peers = consensus_config
            .committee()
            .bootstrap_servers()
            .iter()
            .map(|(k, v)| (*k, v.primary.clone()))
            .collect();
        Self::init_network_for_epoch(
            network_handle.inner_handle(),
            bootstrap_peers,
            committee_keys.clone(),
            initial_epoch,
        )
        .await?;

        // start listening if the network needs to be initialized
        if initial_epoch {
            let primary_address = Self::parse_listener_address_for_swarm(
                "PRIMARY_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.primary_address(),
            )?;
            info!(target: "epoch-manager", ?primary_address, "listening to {primary_address}");
            network_handle.inner_handle().start_listening(primary_address).await?;
        }

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::primary_topic(),
                committee_keys.into_iter().collect(),
            )
            .await?;

        if network_handle.connected_peers_count().await.unwrap_or(0) == 0
            || self.consensus_bus.is_cvv()
        {
            // always dial peers for the new epoch
            // do this if a CVV (may need to connect to the other CVVs) or if we don't have any
            // peers if we are not a committee member and have peers then do not pester
            // the committee
            for (_authority_id, bls_pubkey) in consensus_config
                .committee()
                .others_primaries_by_id(consensus_config.authority_id().as_ref())
            {
                self.dial_peer_bls(
                    network_handle.inner_handle().clone(),
                    bls_pubkey,
                    epoch_task_spawner.clone(),
                );
            }
        }

        Self::wait_for_network_peers(network_handle.inner_handle(), "primary network").await?;

        // spawn primary network
        PrimaryNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            consensus_bus.app().clone(),
            state_sync,
            epoch_task_spawner.clone(), // tasks should abort with epoch
            self.consensus_chain.clone(),
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Dial peer.
    fn dial_peer_bls<Req: TNMessage, Res: TNMessage>(
        &self,
        handle: NetworkHandle<Req, Res>,
        bls_pubkey: BlsPublicKey,
        node_task_spawner: TaskSpawner,
    ) {
        // spawn dials on long-running task manager
        let task_name = format!("DialPeer {bls_pubkey}");
        node_task_spawner.spawn_task(task_name, async move {
            let mut backoff = 1;
            let mut retries = 0;

            debug!(target: "epoch-manager", ?bls_pubkey, "dialing peer");
            while let Err(e) = handle.dial_by_bls(bls_pubkey).await {
                // ignore errors for peers that are already connected or being dialed
                if matches!(e, NetworkError::AlreadyConnected(_))
                    || matches!(e, NetworkError::AlreadyDialing(_))
                {
                    return;
                }
                retries += 1;

                warn!(target: "epoch-manager", "failed to dial {bls_pubkey}: {e}");
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 120 {
                    backoff += backoff;
                }
                let peers = handle.connected_peer_count().await.unwrap_or(0);
                // We have been trying for a while (at least two max backoffs at 120 secs), if we
                // have any other peers give up.
                if retries > 10 && peers > 0 {
                    error!(target = "dial_peer", "failed to reach peer {bls_pubkey}, giving up");
                    return;
                }
            }
        });
    }

    /// Create the worker network.
    async fn spawn_worker_network_for_epoch(
        &mut self,
        consensus_config: &ConsensusConfig<DB>,
        worker_id: &u16,
        validator: Arc<dyn BatchValidation>,
        epoch_task_spawner: TaskSpawner,
        network_handle: &WorkerNetworkHandle,
        initial_epoch: bool,
    ) -> eyre::Result<()> {
        // get event streams for the worker network handler
        let rx_event_stream = self.worker_event_stream.subscribe();
        debug!(target: "epoch-manager", "spawning worker network for epoch");

        let committee_keys: HashSet<BlsPublicKey> = consensus_config
            .committee()
            .authorities()
            .into_iter()
            .map(|a| *a.protocol_key())
            .collect();

        let bootstrap_peers = consensus_config
            .committee()
            .bootstrap_servers()
            .iter()
            .map(|(k, v)| (*k, v.worker.clone()))
            .collect();
        Self::init_network_for_epoch(
            network_handle.inner_handle(),
            bootstrap_peers,
            committee_keys.clone(),
            initial_epoch,
        )
        .await?;

        // start listening if the network needs to be initialized
        if initial_epoch {
            let worker_address = Self::parse_listener_address_for_swarm(
                "WORKER_LISTENER_MULTIADDR",
                consensus_config.primary_networkkey(),
                consensus_config.worker_address(),
            )?;
            network_handle.inner_handle().start_listening(worker_address).await?;
        }

        let worker_address = consensus_config.worker_address();

        // always attempt to dial peers for the new epoch
        // the network's peer manager will intercept dial attempts for peers that are already
        // connected
        debug!(target: "epoch-manager", ?worker_address, "spawning worker network for epoch");
        for (_, peer) in consensus_config
            .committee()
            .others_primaries_by_id(consensus_config.authority().as_ref().map(|a| a.id()).as_ref())
        {
            self.dial_peer_bls(
                network_handle.inner_handle().clone(),
                peer,
                epoch_task_spawner.clone(),
            );
        }

        Self::wait_for_network_peers(network_handle.inner_handle(), "worker network").await?;

        // update the authorized publishers for gossip every epoch
        network_handle
            .inner_handle()
            .subscribe(tn_config::LibP2pConfig::worker_txn_topic())
            .await?;
        // Get gossip from committee members about batches.
        // Useful for non-CVVs to prefetch and harmless for CVVs.
        network_handle
            .inner_handle()
            .subscribe_with_publishers(
                tn_config::LibP2pConfig::worker_batch_topic(),
                committee_keys.into_iter().collect(),
            )
            .await?;

        // spawn worker network
        WorkerNetwork::new(
            rx_event_stream,
            network_handle.clone(),
            consensus_config.clone(),
            *worker_id,
            validator,
            self.consensus_chain.clone(),
        )
        .spawn(&epoch_task_spawner);

        Ok(())
    }

    /// Helper method to identify the node's mode:
    /// - "Committee-voting Validator" (CVV)
    /// - "Committee-voting Validator Inactive" (CVVInactive - syncing to rejoin)
    /// - "Observer"
    ///
    /// This method also updates the `ConsensusBus::node_mode()`.
    async fn identify_node_mode(
        &self,
        consensus_config: &ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
    ) -> eyre::Result<NodeMode> {
        if self.consensus_bus.is_cvv_inactive() {
            // If we have an inactive mode then it was set so keep it for now.
            return Ok(NodeMode::CvvInactive);
        }
        debug!(target: "epoch-manager", authority_id=?consensus_config.authority_id(), "identifying node mode..." );
        let in_committee = consensus_config
            .authority_id()
            .map(|id| consensus_config.in_committee(&id))
            .unwrap_or(false);
        state_sync::prime_consensus(
            consensus_bus.app(),
            consensus_config,
            self.consensus_chain.clone(),
        )
        .await;
        let mode = if !in_committee || self.builder.tn_config.observer {
            NodeMode::Observer
        } else {
            // Assume we are caught up, will be demoted to inactive if this is not true...
            NodeMode::CvvActive
        };

        debug!(target: "epoch-manager", ?mode, "node mode identified");
        // update consensus bus
        self.consensus_bus.node_mode().send_modify(|v| *v = mode);

        Ok(mode)
    }

    /// Clear the epoch-related tables for consensus.
    ///
    /// These tables are epoch-specific. Complete historic data is stored
    /// in the `ConsensusBlocks` table.
    fn clear_consensus_db_for_next_epoch(&self) -> eyre::Result<()> {
        self.consensus_db.clear_table::<LastProposed>()?;
        self.consensus_db.clear_table::<Votes>()?;
        self.consensus_db.clear_table::<Certificates>()?;
        self.consensus_db.clear_table::<CertificateDigestByRound>()?;
        self.consensus_db.clear_table::<CertificateDigestByOrigin>()?;
        self.consensus_db.clear_table::<Payload>()?;
        Ok(())
    }

    /// Helper method for parsing provided env var with fallback [Multiaddr]. This is useful to
    /// override the primary/worker swarm listner address for cloud deployments.
    fn parse_listener_address_for_swarm(
        env_var: &str,
        network_pubkey: NetworkPublicKey,
        fallback: Multiaddr,
    ) -> eyre::Result<Multiaddr> {
        std::env::var(env_var)
            .map(|addr| {
                addr.parse()
                    .map_err(|e| {
                        eyre::eyre!(
                            "Failed to parse listener multiaddr from env {env_var} ({addr})\n{e}"
                        )
                    })
                    // add Protocol::P2p to multiaddr to maintain consistency with
                    // bin/telcoin-network/src/keytool/generate.rs
                    .and_then(|multi: Multiaddr| {
                        multi.with_p2p(network_pubkey.into()).map_err(|_| {
                            eyre::eyre!(
                                "{env_var} multiaddr contains a different P2P protocol {:?}",
                                std::env::var(env_var)
                            )
                        })
                    })
            })
            .unwrap_or(Ok(fallback))
    }

    /// Initialize a network handle for a new epoch: register bootstrap peers (on first epoch)
    /// then update the epoch committee.
    ///
    /// Bootstrap peers must be added BEFORE `new_epoch()` so that `known_peers` is populated
    /// when `new_epoch()` builds `current_committee` from it.
    async fn init_network_for_epoch<Req: TNMessage, Res: TNMessage>(
        handle: &NetworkHandle<Req, Res>,
        bootstrap_peers: BTreeMap<BlsPublicKey, P2pNode>,
        committee_keys: HashSet<BlsPublicKey>,
        initial_epoch: bool,
    ) -> eyre::Result<()> {
        if initial_epoch {
            handle.add_bootstrap_peers(bootstrap_peers).await?;
        }
        handle.new_epoch(committee_keys).await?;
        Ok(())
    }

    /// Block until the network has connected to at least one peer, with retries.
    async fn wait_for_network_peers<Req: TNMessage, Res: TNMessage>(
        handle: &NetworkHandle<Req, Res>,
        network_name: &str,
    ) -> eyre::Result<()> {
        let mut peers = handle.connected_peer_count().await.unwrap_or(0);
        let mut retries = 0;
        while peers == 0 {
            retries += 1;
            if retries > 240 {
                return Err(eyre::eyre!(
                    "{network_name} unable to join, cannot connect to any peers!"
                ));
            }
            if retries % 10 == 0 {
                error!(target: "epoch-manager", "failed to join the {network_name}!");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
            peers = handle.connected_peer_count().await.unwrap_or(0);
        }
        Ok(())
    }
}
