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

use crate::{
    engine::ExecutionNode, manager::EpochManager, metrics::EpochMetrics,
    worker::worker_task_manager_name,
};
use std::{
    collections::HashSet,
    future::{ready, Future},
    time::Duration,
};
use tn_config::{NetworkConfig, TelcoinDirs};
use tn_executor::subscriber::spawn_subscriber;
use tn_primary::ConsensusBus;
use tn_reth::{error::StateReadError, RethEnv};
use tn_storage::{certificate_pack::CertificatePack, tables::OurNodeBatchesCache};
use tn_types::{
    gas_accumulator::{next_base_fee_for_config, GasAccumulator},
    BlsPublicKey, Committee, ConsensusHeaderDigest, ConsensusOutput, Database as TNDatabase,
    EpochRecord, Notifier, SealedHeader, TaskJoinError, TaskManager, TaskSpawner, TnReceiver,
};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

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
    ///    `self.epoch_boundary`, seed the [`GasAccumulator`]'s worker count and per-worker base
    ///    fees from the previous epoch's closing block (`read_base_fees_for_entered_epoch`; epoch 0
    ///    sizes from genesis state and keeps the MIN defaults), and backfill a dummy epoch-0
    ///    [`EpochRecord`] if missing so later lookups can treat epoch 0 like any other.
    /// 2. Create the per-epoch [`TaskManager`] and open the epoch pack files via `open_epoch_pack`.
    /// 3. If the mode calls for replay, re-forward any missed consensus output. If that replay
    ///    crosses the epoch boundary, close the epoch immediately, write its [`EpochRecord`], clear
    ///    the consensus DB, and return early as [`RunEpochMode::NewEpoch`] — consensus is never
    ///    configured this iteration. Otherwise, block until the engine has executed the last
    ///    replayed output before going live.
    /// 4. Subscribe to consensus output, configure consensus, and create the primary/worker
    ///    components. The previous and next committees' keys are resolved first in ONE batched read
    ///    pinned to the epoch-start header and threaded into both steps as parameters. The one-time
    ///    per-process network setup is gated on `network_first_init`, which is driven by
    ///    `self.network_initialized` (not by [`RunEpochMode::Initial`]) so the replay-and-close
    ///    return above can defer setup to a following iteration without skipping it.
    /// 5. Start the primary (if this node is an active CVV), the subscriber, the worker batch
    ///    builder, and the engine batch builder; reattach any orphaned batches.
    /// 6. `tokio::select!` over three exits: node shutdown, the epoch boundary
    ///    (`wait_for_epoch_boundary`), and the epoch task manager ending early (a CVV resync or a
    ///    task error). Of these, only the boundary arm closes the epoch and writes its
    ///    [`EpochRecord`]; the replay-and-close (step 3) and leftover-drain (step 7) recovery paths
    ///    write the record on their own closes.
    /// 7. Notify consensus shutdown, abort and drain the epoch task manager, then resolve the
    ///    outcome. On a non-boundary exit, drain leftover output to the engine: if that drain hits
    ///    the boundary, close the epoch and write its [`EpochRecord`] here too. Clear epoch-scoped
    ///    DB tables when a boundary was crossed.
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
        let (committee, epoch_info, epoch_start, epoch_start_header) =
            self.get_committee_with_epoch_start_info(engine).await?;
        self.epoch_boundary = epoch_start + epoch_info.epochDuration as u64;
        debug!(target: "epoch-manager", new_epoch_boundary=self.epoch_boundary, "resetting epoch boundary");
        self.metrics.current.set(committee.epoch() as f64);
        self.metrics.boundary_timestamp_seconds.set(self.epoch_boundary as f64);

        let reth_env = engine.get_reth_env().await;

        // ENTRY-READ INVARIANT: the previous epoch's closing block rules the entire epoch.
        // Every epoch-scoped entry read - committee membership, `epoch_info`, and the fee read
        // below - derives from the ONE pinned header returned by the atomic
        // `epoch_state_at_epoch_start` read above (via `get_committee_with_epoch_start_info`):
        // the previous epoch's closing block, `getEpochInfo(entered).blockHeight - 1` (genesis
        // for epoch 0). `concludeEpoch` writes an epoch's `blockHeight` exactly once at the
        // boundary, so the pin itself is re-derivable at ANY tip: a fresh boundary crossing, a
        // crash-restart replay, and a ModeChange re-entry all resolve the same header. The
        // consequence: a re-entry AFTER a mid-epoch governance `burn` (which swap-and-pops the
        // CURRENT epoch's stored committee arrays immediately) re-reads the IDENTICAL
        // epoch-start membership - RewardsCounter rows, quorum thresholds, leader schedule -
        // instead of the post-burn tip set, so entry timing can no longer change the node's
        // view of the epoch. The neighbor-committee hoist below (previous and next, batched in
        // one pinned EVM at the same header) is an entry read too and inherits the same
        // stability.
        //
        // The fees are pinned the same way, and more strongly than a re-derivation could be:
        // the closing block's own system call (`record_next_epoch_base_fees` in tn-reth) writes
        // every eip1559 worker's next-epoch fee into `WorkerConfigs` storage exactly once, as
        // part of the block this entry reads. A value already written into that block's state
        // cannot drift the way a recomputation over the epoch's header contents can, so the
        // entry reads the fee instead of re-deriving it.
        //
        // That value-stability is also the safety argument under concurrent execution -
        // `send_leftover_consensus_output_to_engine` forwards leftover output without waiting,
        // so the engine may still be executing (calling `inc_block`) while this entry runs:
        // `apply`'s resize no-ops and its fee writes rewrite the same values. The guard is
        // value-stability, NOT quiescence and NOT any refusal inside `set_num_workers` (it
        // truncates unconditionally): the pinned re-read yields the identical count so the resize
        // no-ops, and a shrink below an in-flight worker id would trip `inc_block`'s production
        // panic rather than pass silently. `GasAccumulator::set_num_workers`'s own doc states
        // that bound canonically - keep the two in sync. See the
        // `mode_change_reentry_is_idempotent` IT.
        //
        // Seed the accumulator's worker count and per-worker base fees for the entered epoch
        // from the pinned header (the previous epoch's closing block). This is the single seam
        // every entry shape converges on: a live producer that just crossed the boundary
        // (reading back the value `adjust_base_fees` recomputed at close), every
        // close_epoch(None) recovery shape (replay-and-close, crash-after-close,
        // leftover-drain), and a mid-epoch sync or restart all read the same values from the
        // same pinned state. Runs before replay drives `inc_block`, so replay operates on a
        // correctly sized accumulator.
        let entered = committee.epoch();
        if entered == 0 {
            // Epoch 0 has no prior epoch, so there is no closing block to read: fees stay at the
            // MIN defaults (epoch-0 blocks carry MIN by construction, and configured worker
            // fees only activate entering epoch 1 - the first epoch a closing block has priced).
            // Size the accumulator from the genesis `WorkerConfigs` state.
            super::sync_num_workers_from_chain(
                &reth_env,
                &gas_accumulator,
                epoch_info.blockHeight,
            )?;
        } else {
            // Read failure is a hard error: fees are exact-match consensus values, so producing
            // with an unverifiable fee is a safety failure while halting is only a single-node
            // liveness failure. One state read at the pinned block - O(1) in the epoch's length,
            // so even a mid-epoch (ModeChange) re-entry costs nothing.
            super::read_base_fees_for_entered_epoch(&reth_env, entered, &epoch_start_header)?
                .apply(&gas_accumulator);
        }
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
            let mut replay = self.replay_missed_consensus(committee.clone(), to_engine).await?;
            if let Some(target_hash) = replay.take_epoch_close_hash() {
                // If things go down at exactly the wrong time we might have to replay the epoch end
                // so account for that.
                self.close_epoch(None, &reth_env, &gas_accumulator, target_hash).await?;
                // write the record before clearing tables: a crash after an epoch-0 replay-close
                // that left no durable record 0 would trip the epoch-0 guard above on every
                // restart (peers cannot backfill epoch 0), bricking the node
                self.write_epoch_record(committee.epoch(), engine).await?;
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

        // Neighbor committees from on-chain state - canonical source of truth - resolve through
        // ONE pinned EVM at the held epoch-start header. The previous epoch's committee array
        // is frozen once this epoch starts (a mid-epoch burn mutates the current and future
        // epochs' arrays, never a past epoch's), and the registry at the pin already serves the
        // next epoch's committee, so a post-burn re-entry derives the same sets (see the
        // ENTRY-READ INVARIANT above).
        //
        // READ-FAILURE POLICY: this is a consensus input, so the failure is classified by
        // committee determinism (`StateReadError`) and BOTH classes halt - there is no fail-open
        // arm here, despite what `StateReadError::ChainGlobal`'s variant doc says about
        // keep-current staying committee-consistent. There is nothing to keep: the node is
        // ENTERING the epoch, so it holds no prior neighbor sets, and entering with an
        // unverifiable neighbor committee mis-scopes peer banning and next-committee
        // pre-resolution for the whole epoch. Halting is a single-node liveness failure.
        //
        // A Provider fault is node-local (peers reading the same block may succeed), so retry
        // briefly before halting; ChainGlobal returns from the first attempt. The pin is
        // `epoch_start_header`, a fixed `SealedHeader` captured before this retry, so every
        // attempt provably reads the same block. The `try_into` arity checks are `eyre`, not
        // `StateReadError`, so they stay OUTSIDE the retried closure.
        let epochs: Vec<_> =
            if entered == 0 { vec![entered + 1] } else { vec![entered - 1, entered + 1] };
        let sets = retry_provider_faults("neighbor committees at the epoch-start pin", || {
            engine.validators_for_epochs_at_header(&epochs, &epoch_start_header)
        })
        .await
        .map_err(|e| {
            eyre::eyre!(
                "failed neighbor-committee read at the epoch-start pin - halting rather than \
                 entering epoch {entered} with an unverifiable neighbor committee: {e}"
            )
        })?;
        let (previous_committee_keys, next_committee_keys): (HashSet<BlsPublicKey>, Vec<_>) =
            if entered == 0 {
                // epoch 0 has no previous committee
                let [next] = sets.try_into().map_err(|_| {
                    eyre::eyre!("neighbor-committee batch arity mismatch for epoch 0")
                })?;
                (HashSet::new(), next)
            } else {
                let [previous, next] = sets.try_into().map_err(|_| {
                    eyre::eyre!("neighbor-committee batch arity mismatch for epoch {entered}")
                })?;
                (previous.into_iter().collect(), next)
            };

        let consensus_config =
            self.configure_consensus(network_config, committee, next_committee_keys).await?;

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
                &epoch_start_header,
                previous_committee_keys,
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
                self.close_epoch(
                    Some(consensus_shutdown.clone()),
                    &reth_env,
                    &gas_accumulator,
                    target_hash,
                )
                .await?;

                // Write the epoch record to DB and save in manager for next epoch.
                self.write_epoch_record(current_epoch, engine).await?;

                // Export the epoch's final execution state (no-op unless --enable-state-export).
                self.export_epoch_state(&primary, &reth_env).await?;

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
            self.close_epoch(None, &reth_env, &gas_accumulator, target_hash).await?;
            // this arm closed the epoch, so it writes the record too — otherwise the epoch has
            // no durable record and the next live close fails as out-of-order
            self.write_epoch_record(current_epoch, engine).await?;
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
            // The engine executes exactly the sequence forwarded here, so enforce continuity
            // against the last number that actually reached it. A stale output (already
            // forwarded, e.g. replayed from the DB) would double-execute; a gap (e.g. the
            // broadcast lagged this receiver) would silently diverge execution from
            // consensus. Every output is saved to the consensus DB before it is broadcast,
            // so erroring here lets the restart path replay the gap from the DB.
            match check_output_continuity(self.last_forwarded_consensus_number, output.number()) {
                OutputContinuity::Stale => {
                    warn!(
                        target: "epoch-manager",
                        number=output.number(),
                        last_forwarded=self.last_forwarded_consensus_number,
                        "skipping already-forwarded consensus output",
                    );
                    continue;
                }
                OutputContinuity::Gap => {
                    return Err(eyre::eyre!(
                        "consensus output gap: expected {} but received {} - restarting to \
                        replay missed consensus from the DB",
                        self.last_forwarded_consensus_number + 1,
                        output.number(),
                    ));
                }
                OutputContinuity::Next => {}
            }
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
    /// execution is confirmed. On the live boundary path it then recomputes each worker's
    /// next-epoch base fee ([`adjust_base_fees`]); the restart replay-and-close and leftover-drain
    /// paths (which pass `None`) skip that forward computation. Neither shape can strand the next
    /// epoch on stale fees: the closing block itself recorded every worker's next-epoch fee
    /// on-chain, and the next `run_epoch` entry reads it back
    /// (`read_base_fees_for_entered_epoch`), hard-erroring (halting the node) when that state
    /// cannot be read, rather than running on fees the chain does not support. The live path's
    /// close-time computation is therefore redundant for seeding the next epoch, and is kept
    /// deliberately for two reasons: it carries the close-time identity check that proves the fees
    /// were priced off a genuine closing block, and it leaves the accumulator holding correct fees
    /// for the window between the close and the next entry. Finally it clears the
    /// [`GasAccumulator`] so the next epoch starts from zero.
    async fn close_epoch(
        &self,
        shutdown_consensus: Option<Notifier>,
        reth_env: &RethEnv,
        gas_accumulator: &GasAccumulator,
        target_hash: ConsensusHeaderDigest,
    ) -> eyre::Result<()> {
        // Only the live producer (Some(shutdown)) holds a complete accumulator for the epoch it
        // just closed and computes the next fee forward here. The None paths (replay-and-close,
        // leftover-drain) skip it: the next run_epoch entry reads the identical fees the closing
        // block recorded on-chain (read_base_fees_for_entered_epoch) and halts if it cannot.
        // FOLLOW-UP: once the on-chain record has soaked in production, this close-time
        // computation can be removed entirely; whoever does that must also decide where the
        // close-time identity check it carries (proof the fees were priced off a genuine
        // closing block) lives afterwards.
        let mut live_boundary = false;
        // begin consensus shutdown while engine executes
        if let Some(s) = shutdown_consensus {
            s.notify();
            live_boundary = true;
        }
        self.consensus_bus.wait_for_consensus_execution(target_hash).await?;
        // adjust basefees after final execution
        if live_boundary {
            adjust_base_fees(reth_env, gas_accumulator).await?;
        }
        gas_accumulator.clear(); // Clear the accumlated values for next epoch.
        Ok(())
    }
}

/// Recompute each worker's next-epoch base fee from the gas it accumulated this epoch and the
/// worker's fee strategy read from the `WorkerConfigs` contract.
///
/// Reads one [`WorkerFeeConfig`](tn_types::gas_accumulator::WorkerFeeConfig) per worker at the
/// canonical tip — which inside [`Self::close_epoch`] (after `wait_for_consensus_execution`) is
/// exactly the epoch's closing block — then applies the strategy via [`next_base_fee_for_config`]
/// and writes the result back to each worker's base-fee container. This is the deterministic seam
/// every committee member runs identically at the boundary.
///
/// The read's config count is the on-chain `numWorkers()` at the closing block, i.e. the worker
/// count for the NEXT epoch (a mid-epoch `setNumWorkers` only takes effect at the boundary by
/// design). The accumulator is resized to it before the per-worker loop so workers governance
/// just added get their configured fee (e.g. `Static`) computed here rather than defaulting to
/// MIN; epoch-entry sync then confirms the same count from the same block.
///
/// Fails on an identity violation (below) or when a node-local provider fault persists
/// through [`CLOSE_READ_ATTEMPTS`] tries of either chain read (such a fault is NOT
/// committee-deterministic, so producing on possibly-stale fees is a safety risk while halting is
/// only a single-node liveness failure). A CHAIN-GLOBAL read failure instead keeps the current
/// per-worker base fees and worker count unchanged rather than aborting the epoch close (see the
/// FAIL-OPEN note in the body).
///
/// Inert on existing chains: the genesis fee strategy is `Eip1559 { target_gas: u64::MAX }`, which
/// floors every worker at `MIN_PROTOCOL_BASE_FEE`. Fees only move once governance sets a real
/// per-worker target.
///
/// The identity check lives here and the per-worker fold in
/// [`apply_close_time_fee_updates`]; the closing block itself records the same values on-chain
/// (`record_next_epoch_base_fees` in `tn-reth`), computed from the same accumulator and the same
/// pinned block, so all three seams agree bit-for-bit.
async fn adjust_base_fees(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<()> {
    // One-header discipline: pin the canonical tip ONCE. Inside `Self::close_epoch` (after
    // `wait_for_consensus_execution`) this IS the epoch's closing block; the identity check
    // and the WorkerConfigs read below both resolve against this single header.
    let tip = reth_env.canonical_tip();

    // CLOSE-TIME IDENTITY: `concludeEpoch` stamps the newly-entered epoch's `blockHeight` as
    // `closing block + 1` (ConsensusRegistry), so this read (canonical tip = closing block) prices
    // fees for the epoch whose `blockHeight` is exactly `tip + 1`. That identity is otherwise
    // implicit - it silently depends on the contract's `+1` convention and on no block executing
    // between the close and this read. Assert it against the newly-recorded epoch info at the SAME
    // pinned tip before touching fees, so a future multi-block-boundary or contract change trips
    // loudly here instead of pricing fees off a non-closing block.
    //
    // READ-FAILURE POLICY: both this identity read and the config read below are consensus
    // inputs, so their failures are classified by committee determinism (`StateReadError`):
    // - ChainGlobal (contract absent, revert, decode, arity) is a deterministic product of the
    //   pinned chain state — every committee member hitting it lands on the same kept-current
    //   fees/count, so keep-current fail-open is committee-consistent.
    // - Provider (node-local storage/provider fault) is NOT committee-deterministic: peers may read
    //   successfully and advance their fees while this node would keep stale ones, and the
    //   exact-equality basefee validation would then reject every peer batch (and peers reject
    //   ours) for the entire epoch. Retry briefly, then HALT — never silently keep-current.
    // Only a proven identity VIOLATION or an exhausted provider fault halts.
    let (entered_epoch, epoch_info) = match retry_provider_faults(
        "close-time epoch info (identity check)",
        || ready(reth_env.get_current_epoch_info_at_header(&tip)),
    )
    .await
    {
        Ok(read) => read,
        Err(e @ StateReadError::Provider(_)) => {
            return Err(eyre::eyre!(
                "node-local provider fault reading epoch info at closing tip {} ({:?}) after \
                     {CLOSE_READ_ATTEMPTS} attempts - refusing to price base fees this node \
                     cannot verify: {e}",
                tip.number,
                tip.hash(),
            ));
        }
        Err(e @ StateReadError::ChainGlobal(_)) => {
            warn!(
                target: "epoch-manager",
                ?e,
                tip_number = tip.number,
                "chain-global failure reading epoch info at canonical tip for the close-time base-fee identity check - keeping current per-worker base fees and worker count (committee-deterministic)"
            );
            return Ok(());
        }
    };

    let block_height = epoch_info.blockHeight;
    if tip.number + 1 != block_height {
        error!(
            target: "epoch-manager",
            tip_number = tip.number,
            entered_epoch,
            block_height,
            "close-time base-fee identity: canonical tip + 1 != entered-epoch blockHeight"
        );
        return Err(eyre::eyre!(
            "close-time base-fee identity violated: canonical tip {} + 1 != entered-epoch \
             {entered_epoch} blockHeight {block_height} at tip {:?} - refusing to price base fees \
             off a non-closing block",
            tip.number,
            tip.hash(),
        ));
    }

    apply_close_time_fee_updates(reth_env, gas_accumulator, &tip).await
}

/// The post-identity-check half of [`adjust_base_fees`]: read the worker fee configs at the
/// closing block `tip` and fold each worker's next-epoch base fee into `gas_accumulator`.
///
/// Split out so the read-failure policy can be exercised at any pinned header without driving a
/// real epoch-closing block. The closing block itself now records the same fees on-chain (see
/// `record_next_epoch_base_fees` in `tn-reth`), which is fatal when the `WorkerConfigs` contract
/// is unreadable — so a test that strips the contract from genesis can no longer produce a
/// closing block to call the outer function against.
///
/// See [`adjust_base_fees`] for the identity check this assumes has already passed, and the
/// read-failure classification both halves share.
async fn apply_close_time_fee_updates(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
    tip: &SealedHeader,
) -> eyre::Result<()> {
    // FAIL-OPEN (CHAIN-GLOBAL FAILURES ONLY): a chain-global config-read failure must not abort
    // the epoch close. Keep the current per-worker base fees and worker count untouched -- both
    // are already consensus-consistent (seeded from the same chain at epoch entry, then held
    // deterministic within the epoch), and a chain-global failure is a deterministic product of
    // the pinned block, so EVERY committee member hits it and lands on the same state. The count
    // self-heals at the next epoch entry, which reads count and fees from the new closing block
    // (`read_base_fees_for_entered_epoch`) and halts if that state is unreadable.
    // A node-local provider fault is NOT committee-deterministic (peers may read fine
    // and move to the new fees), so it must never fail open: retry, then halt. Pinned to the SAME
    // `tip` the identity check validated (one-header discipline).
    match retry_provider_faults("close-time worker fee configs", || {
        ready(reth_env.get_worker_fee_configs_at_block(tip.hash()))
    })
    .await
    {
        Ok((_num_workers, entries)) => {
            gas_accumulator.set_num_workers(entries.len());
            for (worker_id, row) in entries.into_iter().enumerate() {
                let worker_id = worker_id as u16;
                let (_blocks, gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
                let base_fee = gas_accumulator.base_fee(worker_id);
                let next_base_fee =
                    next_base_fee_for_config(row.config, base_fee.base_fee(), gas_used);
                base_fee.set_base_fee(next_base_fee);
            }
        }
        Err(e @ StateReadError::Provider(_)) => {
            return Err(eyre::eyre!(
                "node-local provider fault reading worker fee configs at closing tip {} ({:?}) \
                 after {CLOSE_READ_ATTEMPTS} attempts - refusing to price base fees this node \
                 cannot verify: {e}",
                tip.number,
                tip.hash(),
            ));
        }
        Err(e @ StateReadError::ChainGlobal(_)) => {
            warn!(
                target: "epoch-manager",
                ?e,
                "chain-global failure reading worker fee configs at epoch close - keeping current per-worker base fees and worker count (committee-deterministic)"
            );
        }
    }
    Ok(())
}

/// Total attempts (first try + retries) for each classified pinned chain read at an epoch seam —
/// the close-time reads in [`adjust_base_fees`], the epoch-record committee reads, and the
/// epoch-entry reads — before a node-local provider fault escalates to the caller (a halt at every
/// current site).
const CLOSE_READ_ATTEMPTS: u32 = 3;

/// Pause between read retries in [`retry_provider_faults`].
const CLOSE_READ_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Run `read` up to [`CLOSE_READ_ATTEMPTS`] times, sleeping [`CLOSE_READ_RETRY_BACKOFF`] between
/// tries, retrying ONLY on [`StateReadError::Provider`].
///
/// Provider faults are node-local (a transient I/O error may clear on a re-read: every pinned
/// read constructs a fresh state provider per attempt), so a bounded retry preserves liveness
/// before the caller escalates to a halt. Chain-global failures are deterministic products of the
/// pinned block — re-reading cannot change them — so they return immediately for the caller's
/// fail-open-or-halt arm. Success passes straight through.
///
/// `read` returns a future so the async epoch-record and epoch-entry committee reads share this
/// policy with the synchronous close-time reads (which adapt with [`std::future::ready`]). Each
/// attempt calls `read` afresh, so every retry re-runs the whole read — which is why every caller
/// must fix its pin OUTSIDE the closure, or successive attempts could resolve different blocks.
///
/// Every retry is counted through [`EpochMetrics::record_provider_fault_retry`], labelled by
/// `what`. Once a provider fault at an epoch seam is survivable it becomes invisible until it is
/// not, and a `warn!` alone will not surface a node quietly retrying at every boundary.
pub(super) async fn retry_provider_faults<T, Fut>(
    what: &'static str,
    mut read: impl FnMut() -> Fut,
) -> Result<T, StateReadError>
where
    Fut: Future<Output = Result<T, StateReadError>>,
{
    let mut attempt = 1u32;
    loop {
        match read().await {
            Err(StateReadError::Provider(detail)) if attempt < CLOSE_READ_ATTEMPTS => {
                warn!(
                    target: "epoch-manager",
                    attempt,
                    max_attempts = CLOSE_READ_ATTEMPTS,
                    what,
                    detail,
                    "node-local provider fault on pinned chain read - retrying"
                );
                EpochMetrics::record_provider_fault_retry(what);
                attempt += 1;
                tokio::time::sleep(CLOSE_READ_RETRY_BACKOFF).await;
            }
            other => return other,
        }
    }
}

/// How the next consensus output's number relates to the last one forwarded to the engine.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum OutputContinuity {
    /// Already forwarded (`number <= last_forwarded`): skip as a duplicate.
    Stale,
    /// The expected next output (`last_forwarded + 1`): forward it.
    Next,
    /// At least one output was missed (`number > last_forwarded + 1`): forwarding would leave
    /// a silent execution gap.
    Gap,
}

/// Classify `number` against the last consensus number actually forwarded to the engine.
///
/// Used only on the live-forwarding path ([`EpochManager::wait_for_epoch_boundary`]); the
/// replay and leftover-drain paths legitimately re-forward numbers at or below
/// `last_forwarded` and must not be checked.
fn check_output_continuity(last_forwarded: u64, number: u64) -> OutputContinuity {
    if number <= last_forwarded {
        OutputContinuity::Stale
    } else if number == last_forwarded + 1 {
        OutputContinuity::Next
    } else {
        OutputContinuity::Gap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{derive_base_fees_for_entered_epoch, sync_num_workers_from_chain};
    use rand::{rngs::StdRng, SeedableRng as _};
    use std::{cell::Cell, sync::Arc};
    use tempfile::TempDir;
    use tn_config::WORKER_CONFIGS_ADDRESS;
    use tn_reth::{
        payload::TNPayload,
        system_calls::CONSENSUS_REGISTRY_ADDRESS,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            read_worker_config_entries_at, test_genesis_with_consensus_registry,
            test_genesis_with_consensus_registry_and_workers, TransactionFactory,
        },
        RethChainSpec,
    };
    use tn_types::{
        gas_accumulator::WorkerFeeConfig, Address, GenesisAccount, WorkerId, B256,
        MIN_PROTOCOL_BASE_FEE, U256,
    };

    #[tokio::test(start_paused = true)]
    async fn retry_provider_faults_halts_after_exhausting_attempts() {
        // Retry-then-halt: a persistent node-local provider fault is retried exactly
        // CLOSE_READ_ATTEMPTS times total, then surfaces as the Provider error for the caller
        // to escalate into a halt.
        let calls = Cell::new(0u32);
        let res: Result<(), StateReadError> = retry_provider_faults("test read", || {
            calls.set(calls.get() + 1);
            ready(Err(StateReadError::Provider("mdbx i/o fault".into())))
        })
        .await;

        assert!(matches!(res, Err(StateReadError::Provider(_))), "exhaustion keeps the class");
        assert_eq!(calls.get(), CLOSE_READ_ATTEMPTS, "reads exactly CLOSE_READ_ATTEMPTS times");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_provider_faults_recovers_from_transient_fault() {
        // A transient provider fault (fails once, then reads fine) must NOT halt the node: the
        // retry absorbs it and the successful value passes through.
        let calls = Cell::new(0u32);
        let res = retry_provider_faults("test read", || {
            calls.set(calls.get() + 1);
            ready(if calls.get() == 1 {
                Err(StateReadError::Provider("transient i/o fault".into()))
            } else {
                Ok(7u64)
            })
        })
        .await;

        assert_eq!(res.expect("transient fault recovers"), 7);
        assert_eq!(calls.get(), 2, "one retry after the transient fault");
    }

    #[tokio::test(start_paused = true)]
    async fn retry_provider_faults_does_not_retry_chain_global() {
        // Chain-global failures are deterministic products of the pinned block - re-reading
        // cannot change them, so they return immediately for the caller's fail-open arm.
        let calls = Cell::new(0u32);
        let res: Result<(), StateReadError> = retry_provider_faults("test read", || {
            calls.set(calls.get() + 1);
            ready(Err(StateReadError::ChainGlobal("contract absent".into())))
        })
        .await;

        assert!(matches!(res, Err(StateReadError::ChainGlobal(_))));
        assert_eq!(calls.get(), 1, "chain-global failures are never retried");
    }

    /// Keep-current arm of the close-time fail-open: a CHAIN-GLOBAL config-read failure
    /// (WorkerConfigs contract absent, the alloc-stripped-genesis trick) returns `Ok` and keeps
    /// the per-worker base fees, the worker count, and the accumulated gas untouched.
    ///
    /// Calls the inner [`apply_close_time_fee_updates`] at a genesis tip rather than the outer
    /// [`adjust_base_fees`] behind a real closing block: the closing block now records the same
    /// fees on-chain and is FATAL when `WorkerConfigs` is unreadable, so this fixture (which
    /// exists precisely to make that contract unreadable) can no longer produce one. Pinning the
    /// inner function keeps the coverage that matters — the fail-open arm still preserves fees
    /// and count — and it is also the shape a node whose tip predates this feature hits, where
    /// the closing block carries no recorded fees at all.
    #[tokio::test]
    async fn close_time_fee_updates_keep_fees_and_count_on_read_failure() -> eyre::Result<()> {
        // registry genesis WITHOUT the WorkerConfigs account: the config read is guaranteed to
        // fail chain-globally (call to codeless address succeeds with empty data -> decode fails)
        let mut genesis = test_genesis_with_consensus_registry(4);
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::with_prefix("adjust_fees_fail_open")?;
        let task_manager = TaskManager::new("adjust fees fail open");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
        let tip = reth_env.canonical_tip();

        // non-default fees, gas, and count on the accumulator
        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(4_242);
        acc.base_fee(1).set_base_fee(9_099);
        acc.inc_block(0, 1_000_000, 30_000_000);
        acc.inc_block(1, 2_000_000, 30_000_000);

        // chain-global failure -> keep-current fail-open: Ok, everything untouched
        apply_close_time_fee_updates(&reth_env, &acc, &tip).await?;

        assert_eq!(acc.num_workers(), 2, "worker count unchanged");
        assert_eq!(acc.base_fee(0).base_fee(), 4_242, "worker 0 fee unchanged");
        assert_eq!(acc.base_fee(1).base_fee(), 9_099, "worker 1 fee unchanged");
        assert_eq!(acc.get_values(0), (1, 1_000_000, 30_000_000), "worker 0 gas unchanged");
        assert_eq!(acc.get_values(1), (1, 2_000_000, 30_000_000), "worker 1 gas unchanged");

        Ok(())
    }

    /// The identity read's fail-open arm follows the same classification: a CHAIN-GLOBAL failure
    /// there (ConsensusRegistry absent too) keeps fees and count and returns `Ok` without ever
    /// reaching the config read.
    #[tokio::test]
    async fn adjust_base_fees_keeps_fees_when_identity_read_fails_chain_global() -> eyre::Result<()>
    {
        // strip BOTH contracts: the identity read itself now fails chain-globally at the
        // genesis tip (no closing block needed - the read never resolves an epoch to check)
        let mut genesis = test_genesis_with_consensus_registry(4);
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        genesis.alloc.remove(&CONSENSUS_REGISTRY_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::with_prefix("adjust_fees_identity_fail_open")?;
        let task_manager = TaskManager::new("adjust fees identity fail open");
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;

        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(4_242);
        acc.base_fee(1).set_base_fee(9_099);
        acc.inc_block(0, 1_000_000, 30_000_000);

        adjust_base_fees(&reth_env, &acc).await?;

        assert_eq!(acc.num_workers(), 2, "worker count unchanged");
        assert_eq!(acc.base_fee(0).base_fee(), 4_242, "worker 0 fee unchanged");
        assert_eq!(acc.base_fee(1).base_fee(), 9_099, "worker 1 fee unchanged");
        assert_eq!(acc.get_values(0), (1, 1_000_000, 30_000_000), "worker 0 gas unchanged");

        Ok(())
    }

    /// Identity-violation arm: with the registry present but NO closing block executed, the
    /// canonical tip is genesis and the identity read succeeds with epoch 0's record
    /// (`blockHeight` 0), so `tip + 1 != blockHeight` and the guard halts the close with the
    /// descriptive error instead of pricing base fees off a non-closing block.
    #[tokio::test]
    async fn adjust_base_fees_errors_when_tip_is_not_a_closing_block() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(4);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::with_prefix("adjust_fees_identity_violation")?;
        let task_manager = TaskManager::new("adjust fees identity violation");
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;

        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(4_242);

        let err = adjust_base_fees(&reth_env, &acc)
            .await
            .expect_err("non-closing tip must violate the close-time identity");
        assert!(
            err.to_string().contains("close-time base-fee identity violated"),
            "unexpected error: {err}"
        );

        // the guard trips before the config read, so fees stay untouched
        assert_eq!(acc.base_fee(0).base_fee(), 4_242, "worker 0 fee unchanged");

        Ok(())
    }

    /// Build one worker block on `parent` carrying `base_fee` and `txs`, execute it (running
    /// the epoch-closing system calls when `output` is flagged), commit it as the canonical +
    /// finalized tip, and mirror the engine's post-execution accounting by folding the executed
    /// header's gas into `acc`.
    ///
    /// The `inc_block` AFTER execution is the production ordering (`tn_engine`'s
    /// payload builder): for an epoch-closing block it means the closing system calls read the
    /// accumulator WITHOUT this block's own gas — the executor folds that gas in itself.
    fn execute_worker_block(
        reth_env: &RethEnv,
        acc: &GasAccumulator,
        parent: SealedHeader,
        output: &ConsensusOutput,
        base_fee: u64,
        worker_id: WorkerId,
        txs: Vec<Vec<u8>>,
    ) -> eyre::Result<SealedHeader> {
        let gas_limit = parent.gas_limit;
        let payload = TNPayload::new(
            parent,
            Address::random(),
            0,
            B256::random(),
            output,
            B256::ZERO,
            base_fee,
            gas_limit,
            B256::random(),
            worker_id,
        );
        let block = execute_payload_and_update_canonical_chain(reth_env, payload, txs)?;
        let header = block.recovered_block.clone_sealed_header();
        acc.inc_block(worker_id, header.gas_used, header.gas_limit);
        Ok(header)
    }

    /// CROSS-LAYER CAPSTONE: the three production computations of a worker's next-epoch base
    /// fee agree bit-identically at every epoch close:
    ///
    ///  (a) the on-chain `WorkerConfigs.data` word the closing block's 4th system call records
    ///      (`record_next_epoch_base_fees` in `tn-reth`), read back at the closing block's
    ///      state through the production decode seam;
    ///  (b) the epoch-entry derivation every node runs ([`derive_base_fees_for_entered_epoch`]);
    ///  (c) the live producer's close-time accumulator update ([`adjust_base_fees`]);
    ///
    /// each pinned against an independent [`next_base_fee_for_config`] oracle computed from raw
    /// header gas and the epoch's entry fee. This equality is a consensus invariant: the batch
    /// validator snapshots one entry fee per epoch and compares for EXACT equality, so a
    /// one-wei divergence between any two of these seams rejects every peer batch for a whole
    /// epoch — and replacing (b) with a read of (a) is only safe while all three agree.
    ///
    /// Drives TWO real epoch closes over one chain (worker 0 `Eip1559 { target_gas: 1M }`,
    /// worker 1 `Static` — a mixed strategy set), with genuine per-worker user-tx gas and real
    /// transfers in BOTH closing blocks so the closing block's own-gas fold-in is exercised at
    /// each boundary. Fixture guards pin that the eip1559 fee moves away from MIN, from the
    /// epoch's entry fee, AND from the value computed without the closing block's own gas — a
    /// wrong fee source, a dropped total, or an off-by-one-block gas fold all fail loudly.
    ///
    /// Epoch 0 runs at a preloaded `START_FEE` (accumulator slot and epoch-0 block headers
    /// carry the same value — the input-consistency production guarantees by pricing batches
    /// from the accumulator), standing in for any epoch N with a real fee so the ±12.5% moves
    /// are exercised at full scale rather than degenerating to MIN±1. The second boundary is
    /// fully organic: epoch 1 runs at the fee the FIRST close wrote on-chain, so boundary 2
    /// starts from a written fee, not genesis defaults.
    #[tokio::test]
    async fn close_record_adjust_and_entry_derivation_agree_across_boundaries() -> eyre::Result<()>
    {
        const TARGET_GAS: u64 = 1_000_000;
        const START_FEE: u64 = 1_000_000;
        const STATIC_FEE: u64 = 12_345;
        const TX_GAS_PRICE: u128 = 2_000_000;
        let cfg0 = WorkerFeeConfig::Eip1559 { target_gas: TARGET_GAS };

        // fund an EOA so every epoch (and both closing blocks) carries real user-tx gas
        let mut sender = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(913));
        let genesis = test_genesis_with_consensus_registry_and_workers(
            4,
            vec![(0u8, TARGET_GAS), (1u8, STATIC_FEE)],
        )
        .extend_accounts([(
            sender.address(),
            GenesisAccount::default().with_balance(U256::from(1_000_000_000_000_000_000_u64)),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::with_prefix("cross_layer_fee_agreement")?;
        let task_manager = TaskManager::new("cross layer fee agreement");
        let recipient = Address::repeat_byte(0xab);
        let mut transfer = |chain: &Arc<RethChainSpec>| {
            sender.create_eip1559_encoded(
                chain.clone(),
                None,
                TX_GAS_PRICE,
                Some(recipient),
                U256::from(1),
                Default::default(),
            )
        };

        // the LIVE accumulator, shared with the block executor (wired into the EVM config so
        // the closing block's record reads exactly this state)
        let acc = GasAccumulator::new(1);
        let reth_env = RethEnv::new_for_temp_chain(
            chain.clone(),
            tmp_dir.path(),
            &task_manager,
            Some(acc.clone()),
        )?;

        // epoch-0 entry: size the accumulator from genesis WorkerConfigs state (the production
        // epoch-0 entry seam), then preload worker 0's stand-in entry fee; worker 1 keeps MIN
        sync_num_workers_from_chain(&reth_env, &acc, 0)?;
        assert_eq!(acc.num_workers(), 2, "accumulator sized from the on-chain worker count");
        acc.base_fee(0).set_base_fee(START_FEE);

        // ceremony genesis deploys WorkerConfigs with every data word unwritten
        let (_, entries) =
            read_worker_config_entries_at(&reth_env, chain.sealed_genesis_header().hash())?;
        assert!(entries[0].data.is_zero(), "no close has recorded a fee yet");
        assert!(entries[1].data.is_zero(), "no close has recorded a fee yet");

        // ----- epoch 0: worker 0 produces (fee START_FEE), worker 1 produces (fee MIN) -----
        let out1 = consensus_output_for_tests(1, 0, 1, false);
        let h1 = execute_worker_block(
            &reth_env,
            &acc,
            chain.sealed_genesis_header(),
            &out1,
            START_FEE,
            0,
            vec![transfer(&chain), transfer(&chain)],
        )?;
        assert!(h1.gas_used > 0, "worker 0's epoch-0 block must carry real gas");

        let out2 = consensus_output_for_tests(2, 0, 2, false);
        let h2 = execute_worker_block(
            &reth_env,
            &acc,
            h1.clone(),
            &out2,
            MIN_PROTOCOL_BASE_FEE,
            1,
            vec![transfer(&chain)],
        )?;
        assert!(h2.gas_used > 0, "worker 1's epoch-0 block must carry real gas");

        // the closing block: worker 0, epoch-close flagged, with real transfers riding it
        let out3 = consensus_output_for_tests(3, 0, 3, true);
        let h3 = execute_worker_block(
            &reth_env,
            &acc,
            h2.clone(),
            &out3,
            START_FEE,
            0,
            vec![transfer(&chain), transfer(&chain)],
        )?;
        assert!(h3.gas_used > 0, "the closing block must carry its own user-tx gas");
        assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 1, "epoch 0 closed");

        // independent oracle for boundary 1: the ONE formula over the entry fee and the
        // epoch's total worker-0 gas INCLUDING the closing block's own
        let epoch0_gas_w0 = h1.gas_used + h3.gas_used;
        let oracle_1 = next_base_fee_for_config(cfg0, START_FEE, epoch0_gas_w0);
        // fixture guards: the value discriminates a MIN write, a no-op write, and a total
        // that dropped the closing block's own gas
        assert_ne!(oracle_1, MIN_PROTOCOL_BASE_FEE, "fixture: fee must move off MIN");
        assert_ne!(oracle_1, START_FEE, "fixture: fee must move off the entry fee");
        assert_ne!(
            oracle_1,
            next_base_fee_for_config(cfg0, START_FEE, h1.gas_used),
            "fixture: the closing block's own gas must change the priced fee",
        );

        // live totals at the close (before clear): also the scan-equivalence reference
        let (_, live_gas_w0, _) = acc.get_values(0);
        let (_, live_gas_w1, _) = acc.get_values(1);
        assert_eq!(live_gas_w0, epoch0_gas_w0, "inc_block total = header gas total");
        assert_eq!(live_gas_w1, h2.gas_used);

        // (c) the live producer's close-time update over the shared accumulator
        adjust_base_fees(&reth_env, &acc).await?;
        let close_time_w0 = acc.base_fee(0).base_fee();
        let close_time_w1 = acc.base_fee(1).base_fee();

        // (a) the on-chain record at the closing block's state
        let (num_workers, entries) = read_worker_config_entries_at(&reth_env, h3.hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(entries[0].config, cfg0, "strategy survives the data write");
        assert_eq!(entries[1].config, WorkerFeeConfig::Static { fee: STATIC_FEE });
        assert!(entries[1].data.is_zero(), "a static worker's data word is never written");
        let recorded_w0 = entries[0].data.to::<u64>();

        // (b) the epoch-entry derivation every node runs, pinned to the same closing block
        let derived_1 = derive_base_fees_for_entered_epoch(&reth_env, 1, &h3)?;
        assert_eq!(derived_1.num_workers, 2);
        // header scan ≡ the live accumulator's inc_block totals for the closed epoch
        assert_eq!(derived_1.gas_totals.get(&0).copied().unwrap_or_default(), live_gas_w0);
        assert_eq!(derived_1.gas_totals.get(&1).copied().unwrap_or_default(), live_gas_w1);

        // THE EQUALITY at boundary 1: (a) == (b) == (c) == oracle, per worker
        assert_eq!(recorded_w0, oracle_1, "(a) on-chain record != oracle");
        assert_eq!(derived_1.fees[0], Some(oracle_1), "(b) entry derivation != oracle");
        assert_eq!(close_time_w0, oracle_1, "(c) close-time accumulator != oracle");
        assert_eq!(derived_1.fees[1], Some(STATIC_FEE), "(b) static worker != configured fee");
        assert_eq!(close_time_w1, STATIC_FEE, "(c) static worker != configured fee");

        // enter epoch 1 exactly as production: clear the epoch's gas, then derive+apply —
        // which must rewrite the very fees the close-time update left in place
        acc.clear();
        derived_1.apply(&acc);
        assert_eq!(acc.base_fee(0).base_fee(), oracle_1, "entry apply rewrites the close value");
        assert_eq!(acc.base_fee(1).base_fee(), STATIC_FEE);

        // ----- epoch 1: runs at the fee the first close WROTE (not genesis defaults) -----
        let fee_epoch1 = acc.base_fee(0).base_fee();
        let out4 = consensus_output_for_tests(1, 1, 4, false);
        let h4 = execute_worker_block(
            &reth_env,
            &acc,
            h3.clone(),
            &out4,
            fee_epoch1,
            0,
            vec![transfer(&chain), transfer(&chain)],
        )?;
        assert!(h4.gas_used > 0);

        let out5 = consensus_output_for_tests(2, 1, 5, false);
        let h5 = execute_worker_block(
            &reth_env,
            &acc,
            h4.clone(),
            &out5,
            STATIC_FEE,
            1,
            vec![transfer(&chain)],
        )?;
        assert!(h5.gas_used > 0);

        // epoch 1's closing block, again carrying real user-tx gas of its own
        let out6 = consensus_output_for_tests(3, 1, 6, true);
        let h6 = execute_worker_block(
            &reth_env,
            &acc,
            h5.clone(),
            &out6,
            fee_epoch1,
            0,
            vec![transfer(&chain), transfer(&chain)],
        )?;
        assert!(h6.gas_used > 0, "the second closing block must carry its own user-tx gas");
        assert_eq!(reth_env.epoch_state_from_canonical_tip()?.epoch, 2, "epoch 1 closed");

        // independent oracle for boundary 2, folded from the WRITTEN boundary-1 fee
        let epoch1_gas_w0 = h4.gas_used + h6.gas_used;
        let oracle_2 = next_base_fee_for_config(cfg0, oracle_1, epoch1_gas_w0);
        assert_ne!(oracle_2, oracle_1, "fixture: the second boundary must move the fee again");
        assert_ne!(oracle_2, MIN_PROTOCOL_BASE_FEE);
        assert_ne!(
            oracle_2,
            next_base_fee_for_config(cfg0, oracle_1, h4.gas_used),
            "fixture: the second closing block's own gas must change the priced fee",
        );

        let (_, live_gas_w0_e1, _) = acc.get_values(0);
        let (_, live_gas_w1_e1, _) = acc.get_values(1);
        assert_eq!(live_gas_w0_e1, epoch1_gas_w0);
        assert_eq!(live_gas_w1_e1, h5.gas_used);

        // (c) at boundary 2
        adjust_base_fees(&reth_env, &acc).await?;

        // (a) at boundary 2
        let (num_workers, entries) = read_worker_config_entries_at(&reth_env, h6.hash())?;
        assert_eq!(num_workers, 2);
        assert!(entries[1].data.is_zero(), "static worker still never written");

        // (b) at boundary 2
        let derived_2 = derive_base_fees_for_entered_epoch(&reth_env, 2, &h6)?;
        assert_eq!(derived_2.num_workers, 2);
        assert_eq!(derived_2.gas_totals.get(&0).copied().unwrap_or_default(), epoch1_gas_w0);
        assert_eq!(derived_2.gas_totals.get(&1).copied().unwrap_or_default(), h5.gas_used);

        // THE EQUALITY at boundary 2 — starting from a written fee, not genesis defaults
        assert_eq!(entries[0].data.to::<u64>(), oracle_2, "(a) on-chain record != oracle");
        assert_eq!(derived_2.fees[0], Some(oracle_2), "(b) entry derivation != oracle");
        assert_eq!(acc.base_fee(0).base_fee(), oracle_2, "(c) close-time accumulator != oracle");
        assert_eq!(derived_2.fees[1], Some(STATIC_FEE));
        assert_eq!(acc.base_fee(1).base_fee(), STATIC_FEE);

        Ok(())
    }

    #[test]
    fn test_check_output_continuity() {
        // stale: anything at or below the last forwarded number
        assert_eq!(check_output_continuity(5, 0), OutputContinuity::Stale);
        assert_eq!(check_output_continuity(5, 4), OutputContinuity::Stale);
        assert_eq!(check_output_continuity(5, 5), OutputContinuity::Stale);
        // next: exactly one past
        assert_eq!(check_output_continuity(5, 6), OutputContinuity::Next);
        // genesis: nothing forwarded yet, first output is number 1
        assert_eq!(check_output_continuity(0, 1), OutputContinuity::Next);
        // gap: anything further ahead
        assert_eq!(check_output_continuity(5, 7), OutputContinuity::Gap);
        assert_eq!(check_output_continuity(5, u64::MAX), OutputContinuity::Gap);
        // overflow safety at the top of the range
        assert_eq!(check_output_continuity(u64::MAX, u64::MAX), OutputContinuity::Stale);
    }
}
