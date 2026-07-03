//! Per-epoch orchestration for the node manager.
//!
//! [`EpochManager`] itself lives in the sibling `node` module; this module is the per-epoch
//! orchestrator that drives one epoch's lifecycle from the manager's lifetime-scoped loop.
//!
//! `run_epoch` runs a single epoch start to finish: read the committee and epoch info from chain,
//! open the epoch's pack files, optionally replay consensus output missed before a restart,
//! configure consensus, create the primary and worker consensus components, run until the epoch
//! boundary or shutdown, close the epoch, and clear the epoch-scoped consensus DB tables so the
//! next epoch starts clean. Each epoch's short-running tasks live on their own [`TaskManager`] that
//! is torn down at the boundary; resources owned by the [`EpochManager`] (networks, DBs, channels)
//! persist across epochs.
//!
//! The epoch-start setup and epoch-teardown sequences are split into the child `start` and `close`
//! modules. This module also holds the helpers both of them call back into — `process_output`,
//! `parse_listener_address_for_swarm`, `wait_for_network_peers` — plus the [`RunEpochMode`] /
//! [`ReplayResult`] types that thread control flow through the loop.

use crate::{engine::ExecutionNode, manager::EpochManager, worker::worker_task_manager_name};
use std::{collections::HashSet, time::Duration};
use tn_config::{NetworkConfig, TelcoinDirs};
use tn_executor::subscriber::spawn_subscriber;
use tn_primary::ConsensusBus;
use tn_storage::{certificate_pack::CertificatePack, tables::OurNodeBatchesCache};
use tn_types::{
    gas_accumulator::{compute_next_base_fee_eip1559, GasAccumulator},
    BlsPublicKey, Committee, ConsensusHeaderDigest, ConsensusOutput, Database as TNDatabase,
    EpochRecord, Notifier, TaskJoinError, TaskManager, TaskSpawner, TnReceiver,
};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Name of the per-epoch [`TaskManager`], created fresh and torn down each epoch.
const EPOCH_TASK_MANAGER: &str = "Epoch Task Manager";

/// Why `run_epoch` is being entered, and on exit what kind of transition just happened.
///
/// The manager's loop (`run_epochs` in the `node` module) passes one in to start an epoch and gets
/// one back describing the boundary that was crossed, then feeds that returned mode into the next
/// iteration. Two behaviors gate on it: whether to replay missed consensus on entry
/// ([`RunEpochMode::replay_consensus`]) and whether this is the one-time process startup
/// ([`RunEpochMode::initial_epoch`]).
#[derive(Debug, Copy, Clone)]
pub(crate) enum RunEpochMode {
    /// First epoch after process start. Triggers the one-time network init and a consensus replay,
    /// since output validated before the previous shutdown may still need to reach the engine.
    Initial,
    /// The epoch was re-entered for the same committee because the node's role changed mid-epoch
    /// (e.g. a CVV that fell too far behind and must resync, or one that has caught back up)
    /// rather than because the boundary was crossed. No replay: live consensus state is still
    /// trusted.
    ModeChange,
    /// A fresh epoch on an already-running node, entered after the previous epoch closed cleanly
    /// at its boundary. Replays consensus as a safety net against a restart racing the
    /// boundary.
    NewEpoch,
}

impl RunEpochMode {
    /// Whether to re-forward consensus output persisted but possibly unexecuted before a restart.
    ///
    /// Skipped only for [`RunEpochMode::ModeChange`], where the node never went down and live state
    /// is authoritative; the startup ([`RunEpochMode::Initial`]) and boundary-crossing
    /// ([`RunEpochMode::NewEpoch`]) paths both replay because either could follow a crash.
    fn replay_consensus(&self) -> bool {
        match self {
            RunEpochMode::ModeChange => false,
            RunEpochMode::Initial | RunEpochMode::NewEpoch => true,
        }
    }

    /// Whether this is the process's first epoch. Used as one input to the network-first-init
    /// decision; the actual gate also accounts for the replay-and-close restart path, which can
    /// defer real network setup past the [`RunEpochMode::Initial`] iteration.
    fn initial_epoch(&self) -> bool {
        matches!(self, RunEpochMode::Initial)
    }
}

impl<P, DB> EpochManager<P, DB>
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    /// Drive one epoch from setup through teardown.
    ///
    /// Ordered phases:
    /// 1. Build an epoch-scoped [`ConsensusBus`] over the application channels and read the
    ///    committee plus epoch timing from chain (the epoch's primary does not exist yet). Derive
    ///    `self.epoch_boundary` and backfill a dummy epoch-0 [`EpochRecord`] if missing so later
    ///    lookups can treat epoch 0 like any other.
    /// 2. Create the per-epoch [`TaskManager`] and open the epoch pack files via `open_epoch_pack`.
    /// 3. If the mode calls for replay, re-forward any missed consensus output. If that replay
    ///    crosses the epoch boundary, close the epoch immediately, clear the consensus DB, and
    ///    return early as [`RunEpochMode::NewEpoch`] — consensus is never configured this
    ///    iteration. Otherwise, block until the engine has executed the last replayed output before
    ///    going live.
    /// 4. Subscribe to consensus output, configure consensus, and create the primary/worker
    ///    components. The one-time per-process network setup is gated on `network_first_init`,
    ///    which is driven by `self.network_initialized` (not by [`RunEpochMode::Initial`]) so the
    ///    replay-and-close return above can defer setup to a following iteration without skipping
    ///    it.
    /// 5. Start the primary (if this node is an active CVV), the subscriber, the worker batch
    ///    builder, and the engine batch builder; reattach any orphaned batches.
    /// 6. `tokio::select!` over three exits: node shutdown, the epoch boundary
    ///    (`wait_for_epoch_boundary`), and the epoch task manager ending early (a CVV resync or a
    ///    task error). Only the boundary arm closes the epoch and writes its [`EpochRecord`].
    /// 7. Notify consensus shutdown, abort and drain the epoch task manager, then resolve the
    ///    outcome. On a non-boundary exit, drain leftover output to the engine: if that drain hits
    ///    the boundary, close the epoch here too. Clear epoch-scoped DB tables when a boundary was
    ///    crossed.
    ///
    /// The returned [`RunEpochMode`] tells the caller (`run_epochs` in the `node` module) which
    /// transition occurred: [`RunEpochMode::NewEpoch`] when the boundary was crossed (advance the
    /// epoch), or [`RunEpochMode::ModeChange`] when the loop exited without crossing it (re-run the
    /// same epoch, typically a role/resync change).
    pub(super) async fn run_epoch(
        &mut self,
        engine: &ExecutionNode,
        network_config: &NetworkConfig,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        epoch_mode: RunEpochMode,
        gas_accumulator: GasAccumulator,
    ) -> eyre::Result<RunEpochMode> {
        info!(target: "epoch-manager", "Starting epoch");
        // counts epoch transitions AND mid-epoch restarts (recovery/flapping signal)
        self.metrics.record_epoch_run(&epoch_mode);

        // Create a new bus wrapping the application channels and adding the epoch specific
        // channels.
        let consensus_bus = ConsensusBus::new_with_app(self.consensus_bus.clone());
        self.last_consensus_header = None;
        // We have not created this epoch's primary yet (no committee) so get it from chain
        // ourselves... Note, any consensus output to replay should be in the same epoch...
        let (committee, epoch_info, epoch_start) =
            self.get_committee_with_epoch_start_info(engine).await?;
        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        self.metrics.current.set(committee.epoch() as f64);
        self.metrics.boundary_timestamp_seconds.set(self.epoch_boundary as f64);
        // Produce a "dummy" epoch 0 EpochRecord if missing.
        // This will let us use simple code to find any epoch including 0 at startup.
        if !self.consensus_chain.epochs().contains_epoch(0).await {
            if committee.epoch() != 0 {
                return Err(eyre::eyre!(
                    "We have epoch 0 in our database if we are past epoch 0, on {}",
                    committee.epoch()
                ));
            }
            // No keys for epoch 0, fix that.
            // We are on epoch 0 so load up that committee in Db as well.
            let committee: Vec<BlsPublicKey> = committee.bls_keys().iter().copied().collect();
            let next_committee = committee.clone();
            let epoch_rec =
                EpochRecord { epoch: 0, committee, next_committee, ..Default::default() };
            // Save the "dummy" record, should be overwritten once epoch 0 closes.
            // This will NOT be signed.
            self.consensus_chain.epochs().save_dummy_epoch0(epoch_rec).await?;
        }

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
            let mut replay = self.replay_missed_consensus(committee, to_engine).await?;
            if let Some(target_hash) = replay.take_epoch_close_hash() {
                // If things go down at exactly the wrong time we might have to replay the epoch end
                // so account for that.
                self.close_epoch(None, &gas_accumulator, target_hash).await?;
                self.clear_consensus_db_for_next_epoch()?;
                return Ok(RunEpochMode::NewEpoch);
            }
            // Only wait for consensus that was actually forwarded to the engine.
            // Waiting on DB-latest consensus could hang if it was saved but never sent.
            if let Some(last_hash) = replay.take_last_replayed_hash() {
                info!(target: "epoch-manager", "Waiting for execution of replayed consensus {last_hash}");
                self.consensus_bus.wait_for_consensus_execution(last_hash).await?;
                info!(target: "epoch-manager", "Confirmed execution of replayed consensus {last_hash}");
            }
        }

        let node_ended = self.node_shutdown.subscribe();

        // subscribe to output early to prevent missed messages
        let mut consensus_output = self.consensus_bus.subscribe_consensus_output();
        let consensus_config = self.configure_consensus(engine, network_config).await?;

        // The networks need their one-time, per-process setup (start listening, register bootstrap
        // peers) on the first iteration that actually reaches `create_consensus`. This is usually
        // the `Initial` epoch, but the replay above can return early before
        // `create_consensus` on a restart that replays-and-closes an epoch boundary, so the first
        // real setup then happens on a following `NewEpoch` iteration. Drive the decision off
        // whether the network has actually been set up yet (not off `RunEpochMode::Initial`) so the
        // setup is never skipped on that restart path. (Committee slots are set every epoch
        // regardless via `update_committees`.)
        let network_first_init = epoch_mode.initial_epoch() || !self.network_initialized;

        // create primary and worker nodes
        let (primary, worker_node) = self
            .create_consensus(
                engine,
                &epoch_task_manager,
                network_first_init,
                gas_accumulator.clone(),
                consensus_bus.clone(),
                consensus_config.clone(),
            )
            .await?;
        // Networks are now set up; subsequent epochs rotate committees instead of re-seeding.
        self.network_initialized = true;
        // consensus config for shutdown subscribers
        let consensus_shutdown = primary.shutdown_signal().await;
        let epoch_shutdown_rx = consensus_shutdown.subscribe();

        // This needs to be created early so required machinery for other tasks exists when needed.
        let mut worker = worker_node.new_worker().await?;
        let current_epoch = primary.current_committee().await.epoch();
        let (current_consensus_epoch, _, _) = self.consensus_bus.published_consensus_num_hash();
        if current_epoch < current_consensus_epoch {
            // If we are starting an epoch behind consensus then make sure we have requested this
            // pack file. The request will not do anything if we have the pack or it's
            // inprocess already. This should not be needed but should be harmless and
            // adds a small safety net.
            self.consensus_bus
                .request_epoch_pack_file_by_epoch(current_epoch, &self.consensus_chain)
                .await;
        }

        gas_accumulator.rewards_counter().set_committee(primary.current_committee().await);
        let certificate_pack = if consensus_bus.is_active_cvv() {
            Some(CertificatePack::open(self.tn_datadir.epochs_db_path(), current_epoch))
        } else {
            None
        };
        if self.consensus_bus.is_active_cvv() {
            // start primary
            primary
                .start(&epoch_task_manager, self.consensus_chain.clone(), certificate_pack)
                .await?;
        }
        // Spawn the subscriber.
        // This is mode sensitive and will start the correct tasks for current mode.
        spawn_subscriber(
            consensus_config.clone(),
            consensus_config.shutdown().subscribe(),
            consensus_bus.clone(),
            &epoch_task_manager,
            primary.network_handle().await,
            self.consensus_chain.clone(),
            self.epoch_boundary,
        );

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
                self.close_epoch(Some(consensus_shutdown.clone()), &gas_accumulator, target_hash)
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
            self.close_epoch(None, &gas_accumulator, target_hash).await?;
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

    /// Open (or reuse, if already open) the epoch pack files for the current epoch.
    ///
    /// Seeds the consensus chain for the new epoch, which requires the previous epoch's
    /// [`EpochRecord`]. Resolving that record is the awkward part: it may already be in the DB, it
    /// may be the synthetic epoch-0 filler, or it may be missing because a restart is catching up
    /// across multiple boundaries faster than the epoch record collector can fetch records. In the
    /// missing case this nudges `requested_missing_epoch` (only ever upward, never clobbering a
    /// higher value already set by the gossip handler), pre-dials committee peers so the collector
    /// has connections — without that pre-dial this blocks waiting for a record while the very task
    /// that would supply it has not started — then waits up to 30s, erroring if it still does not
    /// arrive.
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
                // If we can't find the record then we should be starting at epoch 0- use
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
                let _ = primary_network_handle
                    .inner_handle()
                    .prepare_committee_dial(committee_keys)
                    .await;
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

    /// Forward one consensus output to the engine and record progress.
    ///
    /// If the leader's commit timestamp has reached `self.epoch_boundary`, the output is flagged as
    /// the epoch's close so the engine finalizes the epoch on execution. The output's batches are
    /// evicted from [`OurNodeBatchesCache`] (they have reached execution, so we no longer need to
    /// rebroadcast them), then the output is sent. `last_forwarded_consensus_number` is updated
    /// only after the send succeeds, so the restart-replay and leftover-drain paths can rely on
    /// it marking what actually reached the engine rather than what was merely dequeued.
    pub(super) async fn process_output(
        &mut self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        mut output: ConsensusOutput,
    ) -> eyre::Result<()> {
        let last_forwarded_consensus_number = output.number();
        if output.committed_at() >= self.epoch_boundary {
            // update output so engine closes epoch
            output.set_epoch_close();
        }
        // Now that this output has made it to execution (or almost) clear any of
        // batches from our batches cache.
        for digest in output.batch_digests().iter() {
            if let Err(e) = self.consensus_db.remove::<OurNodeBatchesCache>(digest) {
                error!(target: "epoch-manager", "Remove from our batches cache failed with error: {:?}", e);
            }
        }
        // only forward the output to the engine
        to_engine.send(output).await?;
        // store number after successful send
        self.last_forwarded_consensus_number = last_forwarded_consensus_number;
        Ok(())
    }

    /// Forward live consensus output until the epoch's final commit, then return its hash.
    ///
    /// Each output is handed to `process_output` for execution. The first output whose commit
    /// timestamp reaches `self.epoch_boundary` is the epoch's last: it is flagged as the close,
    /// stashed in `self.last_consensus_header`, forwarded, and its [`ConsensusHeaderDigest`]
    /// returned so the caller can drive `close_epoch` and track execution to that point. Errors
    /// only if the output stream ends before any boundary output arrives.
    async fn wait_for_epoch_boundary(
        &mut self,
        to_engine: &mpsc::Sender<ConsensusOutput>,
        consensus_output: &mut impl TnReceiver<ConsensusOutput>,
    ) -> eyre::Result<ConsensusHeaderDigest> {
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
                output.set_epoch_close();

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

    /// Finalize an epoch once its boundary output has been identified.
    ///
    /// Begins shutting consensus down (when a [`Notifier`] is supplied) so it winds down in
    /// parallel while the engine finishes executing up to `target_hash`, then blocks until that
    /// execution is confirmed. Afterward it runs the (currently no-op) base fee adjustment and
    /// clears the [`GasAccumulator`] so the next epoch starts from zero. Called both on the
    /// normal boundary path and on the restart replay-and-close path, which passes `None`
    /// because there is no live consensus to stop.
    async fn close_epoch(
        &self,
        shutdown_consensus: Option<Notifier>,
        gas_accumulator: &GasAccumulator,
        target_hash: ConsensusHeaderDigest,
    ) -> eyre::Result<()> {
        // begin consensus shutdown while engine executes
        if let Some(s) = shutdown_consensus {
            s.notify()
        }
        self.consensus_bus.wait_for_consensus_execution(target_hash).await?;
        adjust_base_fees(gas_accumulator);
        gas_accumulator.clear(); // Clear the accumlated values for next epoch.
        Ok(())
    }
}

/// Recompute each worker's next-epoch base fee from the gas it accumulated this epoch.
///
/// Applies the EIP-1559-style adjustment ([`compute_next_base_fee_eip1559`]) per worker. The
/// target gas is currently hardcoded to `u64::MAX`, which keeps the adjustment inert: against an
/// unreachable target the fee can only ratchet *down*, and the formula floors every result at
/// `MIN_PROTOCOL_BASE_FEE`. Because workers start at `MIN`, the fee stays at `MIN` everywhere.
///
/// The real per-worker target (from the WorkerConfigs contract) is wired in later; this is the
/// deterministic seam every committee member runs identically at the epoch boundary.
fn adjust_base_fees(gas_accumulator: &GasAccumulator) {
    for worker_id in 0..gas_accumulator.num_workers() {
        let worker_id = worker_id as u16;
        let (_blocks, gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
        let base_fee = gas_accumulator.base_fee(worker_id);
        // u64::MAX target => inert: floors at MIN until governance targets are read.
        let next_base_fee = compute_next_base_fee_eip1559(base_fee.base_fee(), gas_used, u64::MAX);
        base_fee.set_base_fee(next_base_fee);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::MIN_PROTOCOL_BASE_FEE;

    #[test]
    fn adjust_base_fees_keeps_workers_at_min() {
        // Production starting point: every worker's base fee defaults to MIN. With the u64::MAX
        // target the fee can only ratchet down and is floored at MIN, so a worker that starts at
        // MIN stays at MIN regardless of how much gas it used.
        let acc = GasAccumulator::new(2);
        assert_eq!(acc.base_fee(0).base_fee(), MIN_PROTOCOL_BASE_FEE);
        assert_eq!(acc.base_fee(1).base_fee(), MIN_PROTOCOL_BASE_FEE);

        // worker 0 used a lot of gas; worker 1 used none
        acc.inc_block(0, 5_000_000, 30_000_000);

        adjust_base_fees(&acc);

        assert_eq!(acc.base_fee(0).base_fee(), MIN_PROTOCOL_BASE_FEE);
        assert_eq!(acc.base_fee(1).base_fee(), MIN_PROTOCOL_BASE_FEE);
    }

    #[test]
    fn adjust_base_fees_ratchets_non_min_fee_down_to_min() {
        // A worker that somehow holds a non-MIN fee is pulled back down toward MIN every epoch
        // (proving compute_next_base_fee_eip1559 is wired into adjust_base_fees) and is floored at
        // MIN. Zero gas used each epoch applies maximal downward pressure against the unreachable
        // u64::MAX target so the fee converges cleanly to the floor.
        let acc = GasAccumulator::new(1);
        acc.base_fee(0).set_base_fee(MIN_PROTOCOL_BASE_FEE * 1000);

        let mut prev = acc.base_fee(0).base_fee();
        let mut reached_min = false;
        for _ in 0..500 {
            adjust_base_fees(&acc);
            let now = acc.base_fee(0).base_fee();
            assert!(now <= prev, "fee is non-increasing with a u64::MAX target");
            assert!(now >= MIN_PROTOCOL_BASE_FEE, "fee never drops below MIN");
            if now == MIN_PROTOCOL_BASE_FEE {
                reached_min = true;
                break;
            }
            prev = now;
        }
        assert!(reached_min, "fee should ratchet all the way down to MIN");
    }
}
