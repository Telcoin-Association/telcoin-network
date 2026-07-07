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
use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};
use tn_config::{NetworkConfig, TelcoinDirs};
use tn_executor::subscriber::spawn_subscriber;
use tn_primary::ConsensusBus;
use tn_reth::{error::StateReadError, RethEnv};
use tn_storage::{certificate_pack::CertificatePack, tables::OurNodeBatchesCache};
use tn_types::{
    gas_accumulator::{compute_next_base_fee_eip1559, GasAccumulator, WorkerFeeConfig},
    BlsPublicKey, Committee, ConsensusHeaderDigest, ConsensusOutput, Database as TNDatabase, Epoch,
    EpochRecord, Notifier, TaskJoinError, TaskManager, TaskSpawner, TnReceiver, WorkerId,
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

        let reth_env = engine.get_reth_env().await;

        // ENTRY-READ INVARIANT: the previous epoch's closing state rules the entire epoch,
        // so every worker-count/config read in this entry flow is pinned to the previous epoch's
        // closing block (or an earlier epoch's closing block via the walk-back) - never the live
        // mid-epoch tip - and the epoch-info/committee reads resolve values written once at the
        // boundary. The fees the adopt branch reads off mid-epoch blocks are likewise constants
        // within the epoch. A ModeChange re-entry therefore re-reads IDENTICAL values even though
        // `send_leftover_consensus_output_to_engine` forwards leftover output without waiting for
        // execution: the engine may still be executing (calling `inc_block`) while this entry
        // runs, and safety comes from that value-stability (the count resize no-ops, the fee
        // writes rewrite the same values) plus `set_num_workers`' no-shrink-below-in-flight-id
        // bound - NOT from quiescence. See `GasAccumulator::set_num_workers` and the
        // `mode_change_reentry_is_idempotent` IT.

        // Size the accumulator to the on-chain worker count for the epoch being entered (read at
        // the epoch's first block's parent - the previous epoch's closing block). Runs on every
        // entry mode, before fee seeding (which loops 0..num_workers) and before replay drives
        // inc_block, so both operate on a correctly sized accumulator.
        super::sync_num_workers_from_chain(&reth_env, &gas_accumulator, epoch_info.blockHeight);

        // Base-fee-from-chain at entry: classify the pinned finalized tip against the epoch
        // being entered.
        //  - tip already IN the entered epoch: a syncing/restarting node adopts each worker's
        //    latest on-chain base fee (the fee is already baked into this epoch's blocks);
        //    configured workers WITHOUT a block this epoch derive theirs from prior closing-block
        //    state (`fill_absent_worker_fees`) — the value otherwise lives only in live nodes'
        //    memory.
        //  - tip is the PREVIOUS epoch's closing block: derive the entered epoch's fees as a pure
        //    function of the prior epoch's chain state. This is the single seam every boundary
        //    shape converges on: the live producer that just crossed (recomputing the identical
        //    value adjust_base_fees produced at close) and every close_epoch(None) recovery shape
        //    (replay-and-close, crash-after-close, leftover-drain), which previously skipped both
        //    the close-time adjustment and the entry seeding and silently ran the epoch on default
        //    fees.
        //  - anything else: the node's chain view is inconsistent - halt rather than produce
        //    batches with unverifiable consensus-affecting fees.
        //  - no finalized tip at all (fresh genesis, no blocks executed): keep MIN defaults.
        {
            let entered = committee.epoch();
            if let Some(tip) = reth_env.finalized_header()? {
                let tip_epoch = RethEnv::extract_epoch_from_header(&tip);
                if tip_epoch == entered {
                    // The range START, range END, and the epoch classification above all derive
                    // from the ONE pinned finalized header: `tip_epoch` from its nonce, the
                    // epoch's start height from the epoch state read AT that header, and the end
                    // from its number. If finality ever lags the canonical tip the pair stays
                    // internally consistent instead of collapsing to a silently empty range that
                    // drops the fee restore (`catchup_accumulator` pins the same way). `tip` is
                    // sealed - number and hash from one read - so follow-up contract reads can
                    // also pin to `tip.hash()`.
                    let epoch_state = reth_env.epoch_state_at_header(&tip)?;
                    let blocks = reth_env
                        .blocks_for_range(epoch_state.epoch_info.blockHeight..=tip.number)?;
                    let chain_fees = super::latest_base_fee_per_worker(&blocks);
                    seed_base_fees_from_chain(entered, tip_epoch, &chain_fees, &gas_accumulator);
                    // Configured workers absent from the epoch's blocks have no fee to adopt:
                    // derive theirs from prior closing-block state (pinned to the same `tip`)
                    // so this node converges with the live committee for EVERY worker.
                    // Derivation failure is a hard error for the same reason as the boundary
                    // branch below.
                    super::fill_absent_worker_fees(
                        &reth_env,
                        entered,
                        &tip,
                        &chain_fees,
                        &gas_accumulator,
                    )?;
                } else if entered > 0 && tip_epoch == entered - 1 {
                    // The tip IS the previous epoch's closing block: its nonce still carries the
                    // old epoch while the registry state it holds already reports the new one.
                    // Derivation failure is a hard error: fees are exact-match consensus values,
                    // so producing with an unverifiable fee is a safety failure while halting is
                    // only a single-node liveness failure.
                    //
                    // ENTRY-TIME IDENTITY: the derivation trusts `tip` to be E-1's closing
                    // block, which holds only because `concludeEpoch` stamps the entered epoch's
                    // `blockHeight = closing block + 1` (ConsensusRegistry) - i.e. `tip` is the
                    // block right before the entered epoch's first block. That identity is
                    // otherwise implicit; assert `tip.number + 1 == blockHeight` (entered epoch's
                    // info read at THIS pinned tip, same header the derivation scans) before
                    // deriving, so a broken `+1` convention or a future multi-block-boundary
                    // change trips here instead of silently deriving fees off the wrong block.
                    let entered_info = reth_env.get_epoch_info_at_block(entered, tip.hash())?;
                    debug_assert_eq!(
                        tip.number + 1,
                        entered_info.blockHeight,
                        "epoch-entry base-fee identity: closing-block tip {} + 1 != entered-epoch \
                         {entered} blockHeight {}",
                        tip.number,
                        entered_info.blockHeight,
                    );
                    if tip.number + 1 != entered_info.blockHeight {
                        return Err(eyre::eyre!(
                            "epoch-entry closing-block identity violated: finalized tip {} + 1 != \
                             entered-epoch {entered} blockHeight {} at tip {:?} - refusing to \
                             derive base fees off a non-closing block",
                            tip.number,
                            entered_info.blockHeight,
                            tip.hash(),
                        ));
                    }
                    super::derive_base_fees_for_entered_epoch(&reth_env, entered, &tip)?
                        .apply(&gas_accumulator);
                } else {
                    // Unreachable by construction (`entered` is read from registry state at the
                    // canonical tip and finality tracks executed output), so reaching this means
                    // the node's finalized and canonical views genuinely disagree.
                    return Err(eyre::eyre!(
                        "epoch-entry base-fee seeding: finalized tip epoch {tip_epoch} is \
                         neither the entered epoch {entered} nor its predecessor"
                    ));
                }
            }
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
            let mut replay = self.replay_missed_consensus(committee, to_engine).await?;
            if let Some(target_hash) = replay.take_epoch_close_hash() {
                // If things go down at exactly the wrong time we might have to replay the epoch end
                // so account for that.
                self.close_epoch(None, &reth_env, &gas_accumulator, target_hash).await?;
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
                self.close_epoch(
                    Some(consensus_shutdown.clone()),
                    &reth_env,
                    &gas_accumulator,
                    target_hash,
                )
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
            self.close_epoch(None, &reth_env, &gas_accumulator, target_hash).await?;
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
    /// execution is confirmed. On the live boundary path it then recomputes each worker's
    /// next-epoch base fee ([`adjust_base_fees`]); the restart replay-and-close and leftover-drain
    /// paths (which pass `None`) skip that forward computation. Neither shape can strand the next
    /// epoch on stale fees: the next `run_epoch` entry re-derives every worker's fee from the
    /// closed epoch's chain state (`derive_base_fees_for_entered_epoch`) — the same pure function
    /// of the same inputs the live close applies, so both produce the identical value — and
    /// hard-errors (halting the node) when that state cannot be read or verified, rather than
    /// running on fees the chain does not support. The live path's close-time computation is
    /// therefore redundant with the entry derivation but kept so the accumulator holds correct
    /// fees for the window between close and the next entry. Finally it clears the
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
        // leftover-drain) skip it: the next run_epoch entry derives the identical fees from the
        // closed epoch's chain state (derive_base_fees_for_entered_epoch) and halts if it cannot.
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
/// Reads one [`WorkerFeeConfig`] per worker at the canonical tip — which inside
/// [`Self::close_epoch`] (after `wait_for_consensus_execution`) is exactly the epoch's closing
/// block — then applies the strategy via [`next_base_fee_for_config`] and writes the result back to
/// each worker's base-fee container. This is the deterministic seam every committee member runs
/// identically at the boundary.
///
/// The read's config count is the on-chain `numWorkers()` at the closing block, i.e. the worker
/// count for the NEXT epoch (a mid-epoch `setNumWorkers` only takes effect at the boundary by
/// design). The accumulator is resized to it before the per-worker loop so workers governance
/// just added get their configured fee (e.g. `Static`) computed here rather than defaulting to
/// MIN; epoch-entry sync then confirms the same count from the same block.
///
/// Fails on an F17 identity violation (below) or when a node-local provider fault persists
/// through [`CLOSE_READ_ATTEMPTS`] tries of either chain read (F4: such a fault is NOT
/// committee-deterministic, so producing on possibly-stale fees is a safety risk while halting is
/// only a single-node liveness failure). A CHAIN-GLOBAL read failure instead keeps the current
/// per-worker base fees and worker count unchanged rather than aborting the epoch close (see the
/// FAIL-OPEN note in the body).
///
/// Inert on existing chains: the genesis fee strategy is `Eip1559 { target_gas: u64::MAX }`, which
/// floors every worker at `MIN_PROTOCOL_BASE_FEE`. Fees only move once governance sets a real
/// per-worker target.
async fn adjust_base_fees(
    reth_env: &RethEnv,
    gas_accumulator: &GasAccumulator,
) -> eyre::Result<()> {
    // F16 one-header discipline: pin the canonical tip ONCE. Inside `Self::close_epoch` (after
    // `wait_for_consensus_execution`) this IS the epoch's closing block; the F17 identity check
    // and the WorkerConfigs read below both resolve against this single header.
    let tip = reth_env.canonical_tip();

    // F17 CLOSE-TIME IDENTITY: `concludeEpoch` stamps the newly-entered epoch's `blockHeight` as
    // `closing block + 1` (ConsensusRegistry), so this read (canonical tip = closing block) prices
    // fees for the epoch whose `blockHeight` is exactly `tip + 1`. That identity is otherwise
    // implicit - it silently depends on the contract's `+1` convention and on no block executing
    // between the close and this read. Assert it against the newly-recorded epoch info at the SAME
    // pinned tip before touching fees, so a future multi-block-boundary or contract change trips
    // loudly here instead of pricing fees off a non-closing block.
    //
    // READ-FAILURE POLICY (F4): both this identity read and the config read below are consensus
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
        || reth_env.get_current_epoch_info_at_header(&tip),
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
    debug_assert_eq!(
        tip.number + 1,
        block_height,
        "close-time base-fee identity: canonical tip {} + 1 != entered-epoch {entered_epoch} \
         blockHeight {block_height}",
        tip.number,
    );
    if tip.number + 1 != block_height {
        return Err(eyre::eyre!(
            "close-time base-fee identity violated: canonical tip {} + 1 != entered-epoch \
             {entered_epoch} blockHeight {block_height} at tip {:?} - refusing to price base fees \
             off a non-closing block",
            tip.number,
            tip.hash(),
        ));
    }

    // FAIL-OPEN (CHAIN-GLOBAL FAILURES ONLY): a chain-global config-read failure must not abort
    // the epoch close. Keep the current per-worker base fees and worker count untouched -- both
    // are already consensus-consistent (seeded from the same chain at epoch entry, then held
    // deterministic within the epoch), and a chain-global failure is a deterministic product of
    // the pinned block, so EVERY committee member hits it and lands on the same state. The count
    // self-heals at the next epoch entry via `sync_num_workers_from_chain`, which fails open the
    // same way. A node-local provider fault is NOT committee-deterministic (peers may read fine
    // and move to the new fees), so it must never fail open: retry, then halt. Pinned to the SAME
    // `tip` the identity check validated (F16 one-header discipline).
    match retry_provider_faults("close-time worker fee configs", || {
        reth_env.get_worker_fee_configs_at_block(tip.hash())
    })
    .await
    {
        Ok((_num_workers, configs)) => {
            gas_accumulator.set_num_workers(configs.len());
            for (worker_id, config) in configs.into_iter().enumerate() {
                let worker_id = worker_id as u16;
                let (_blocks, gas_used, _gas_limit) = gas_accumulator.get_values(worker_id);
                let base_fee = gas_accumulator.base_fee(worker_id);
                let next_base_fee = next_base_fee_for_config(config, base_fee.base_fee(), gas_used);
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

/// Total attempts (first try + retries) for each close-time chain read in [`adjust_base_fees`]
/// before a node-local provider fault halts the close.
const CLOSE_READ_ATTEMPTS: u32 = 3;

/// Pause between close-time read retries in [`retry_provider_faults`].
const CLOSE_READ_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Run `read` up to [`CLOSE_READ_ATTEMPTS`] times, sleeping [`CLOSE_READ_RETRY_BACKOFF`] between
/// tries, retrying ONLY on [`StateReadError::Provider`].
///
/// Provider faults are node-local (a transient I/O error may clear on a re-read), so a bounded
/// retry preserves liveness before the caller escalates to a halt. Chain-global failures are
/// deterministic products of the pinned block — re-reading cannot change them — so they return
/// immediately for the caller's fail-open arm. Success passes straight through.
async fn retry_provider_faults<T>(
    what: &'static str,
    mut read: impl FnMut() -> Result<T, StateReadError>,
) -> Result<T, StateReadError> {
    let mut attempt = 1u32;
    loop {
        match read() {
            Err(StateReadError::Provider(detail)) if attempt < CLOSE_READ_ATTEMPTS => {
                warn!(
                    target: "epoch-manager",
                    attempt,
                    max_attempts = CLOSE_READ_ATTEMPTS,
                    what,
                    detail,
                    "node-local provider fault on close-time read - retrying"
                );
                attempt += 1;
                tokio::time::sleep(CLOSE_READ_RETRY_BACKOFF).await;
            }
            other => return other,
        }
    }
}

/// Apply a worker's [`WorkerFeeConfig`] to compute its next-epoch base fee.
///
/// `Eip1559 { target_gas }` nudges the fee toward the gas target via
/// [`compute_next_base_fee_eip1559`] (floored at `MIN_PROTOCOL_BASE_FEE`); `Static { fee }` pins
/// the fee to the governance-set value, ignoring gas usage.
///
/// This is the ONE fee formula: [`adjust_base_fees`] applies it at a live epoch close and
/// `fold_next_epoch_base_fees` (in the parent module) applies it when deriving the entered
/// epoch's fees from the previous epoch's chain state, so both seams produce identical values
/// from identical inputs.
pub(crate) fn next_base_fee_for_config(
    config: WorkerFeeConfig,
    current_base_fee: u64,
    gas_used: u64,
) -> u64 {
    match config {
        WorkerFeeConfig::Eip1559 { target_gas } => {
            compute_next_base_fee_eip1559(current_base_fee, gas_used, target_gas)
        }
        WorkerFeeConfig::Static { fee } => fee,
    }
}

/// Seed per-worker base fees from the chain at epoch entry (the base-fee-from-chain invariant).
///
/// When `tip_epoch == entered_epoch` the canonical tip is already in the epoch being entered, so
/// the node is syncing/restarting and adopts each worker's latest on-chain base fee from
/// `chain_fees`. When the epochs differ the values already held are kept untouched. Workers
/// absent from `chain_fees` (no block produced this epoch) are left as-is BY THIS HELPER — the
/// `run_epoch` call site follows up with `fill_absent_worker_fees`, which derives those workers'
/// fees from prior closing-block state (their value is not observable from this epoch's blocks).
///
/// NOTE: `run_epoch` now only calls this on the `tip_epoch == entered_epoch` branch; a tip on
/// the previous epoch's closing block routes through `derive_base_fees_for_entered_epoch`
/// instead. The differing-epoch early return is kept as defense-in-depth for the helper's
/// contract (a caller must never have chain fees from one epoch overwrite another's).
fn seed_base_fees_from_chain(
    entered_epoch: Epoch,
    tip_epoch: Epoch,
    chain_fees: &HashMap<WorkerId, u64>,
    gas_accumulator: &GasAccumulator,
) {
    if tip_epoch != entered_epoch {
        // new epoch - calculate fees from accumulated epoch gas
        return;
    }
    for worker_id in 0..gas_accumulator.num_workers() as u16 {
        if let Some(&fee) = chain_fees.get(&worker_id) {
            gas_accumulator.base_fee(worker_id).set_base_fee(fee);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, sync::Arc};
    use tempfile::TempDir;
    use tn_config::WORKER_CONFIGS_ADDRESS;
    use tn_reth::{
        payload::TNPayload, system_calls::CONSENSUS_REGISTRY_ADDRESS,
        test_utils::test_genesis_with_consensus_registry, NewCanonicalChain, RethChainSpec,
    };
    use tn_types::{
        BlsSignature, Certificate, CommittedSubDag, ConsensusHeader, ReputationScores,
        SignatureVerificationState, MIN_PROTOCOL_BASE_FEE,
    };

    #[test]
    fn eip1559_config_with_max_target_is_inert_at_min() {
        // Genesis/default strategy: Eip1559 { target_gas: u64::MAX }. Against an unreachable target
        // the fee can only ratchet down and floors at MIN, so a worker at MIN stays at MIN
        // regardless of gas used -- the inert guarantee that keeps existing chains unchanged.
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: u64::MAX };
        assert_eq!(
            next_base_fee_for_config(cfg, MIN_PROTOCOL_BASE_FEE, 5_000_000),
            MIN_PROTOCOL_BASE_FEE
        );
        // a non-MIN fee ratchets down (and never below MIN)
        let down = next_base_fee_for_config(cfg, MIN_PROTOCOL_BASE_FEE * 1000, 0);
        assert!((MIN_PROTOCOL_BASE_FEE..MIN_PROTOCOL_BASE_FEE * 1000).contains(&down));
    }

    #[test]
    fn eip1559_config_moves_fee_with_gas_vs_target() {
        let target = 1_000_000u64;
        let current = 1_000_000u64;
        let cfg = WorkerFeeConfig::Eip1559 { target_gas: target };
        // gas above target -> fee increases; below -> decreases; at target -> unchanged.
        assert!(next_base_fee_for_config(cfg, current, 2_000_000) > current);
        assert!(next_base_fee_for_config(cfg, current, 0) < current);
        assert_eq!(next_base_fee_for_config(cfg, current, target), current);
    }

    #[test]
    fn static_config_pins_to_configured_fee() {
        // Static ignores gas usage and the current fee, always returning the governance-set value.
        assert_eq!(
            next_base_fee_for_config(
                WorkerFeeConfig::Static { fee: 12_345 },
                MIN_PROTOCOL_BASE_FEE,
                999_999
            ),
            12_345
        );
        assert_eq!(
            next_base_fee_for_config(WorkerFeeConfig::Static { fee: 500 }, 1_000_000, 0),
            500
        );
    }

    #[test]
    fn seed_base_fees_adopts_chain_fee_when_tip_in_entered_epoch() {
        // Syncing/restarting node: the tip is already in the epoch being entered, so each worker
        // adopts its latest on-chain base fee, overwriting any locally-held value.
        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(1000);
        acc.base_fee(1).set_base_fee(2000);
        let chain_fees = HashMap::from([(0u16, 111u64), (1u16, 222u64)]);

        seed_base_fees_from_chain(5, 5, &chain_fees, &acc);

        assert_eq!(acc.base_fee(0).base_fee(), 111);
        assert_eq!(acc.base_fee(1).base_fee(), 222);
    }

    #[test]
    fn seed_base_fees_keeps_computed_value_for_live_producer() {
        // Helper contract (defense-in-depth): when the tip epoch differs from the entered epoch,
        // held values (e.g. what adjust_base_fees computed at a live close) must not be
        // overwritten with another epoch's chain fees. run_epoch no longer reaches this helper on
        // that branch - a tip on the previous epoch's closing block routes through
        // derive_base_fees_for_entered_epoch instead.
        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(1000);
        acc.base_fee(1).set_base_fee(2000);
        let chain_fees = HashMap::from([(0u16, 111u64), (1u16, 222u64)]);

        seed_base_fees_from_chain(5, 4, &chain_fees, &acc);

        assert_eq!(acc.base_fee(0).base_fee(), 1000);
        assert_eq!(acc.base_fee(1).base_fee(), 2000);
    }

    #[test]
    fn seed_base_fees_leaves_workers_without_chain_blocks_untouched() {
        // A worker that produced no block this epoch is absent from chain_fees and keeps its value.
        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(1000);
        acc.base_fee(1).set_base_fee(2000);
        let chain_fees = HashMap::from([(0u16, 111u64)]); // only worker 0 produced a block

        seed_base_fees_from_chain(7, 7, &chain_fees, &acc);

        assert_eq!(acc.base_fee(0).base_fee(), 111); // seeded from chain
        assert_eq!(acc.base_fee(1).base_fee(), 2000); // untouched
    }

    #[tokio::test(start_paused = true)]
    async fn retry_provider_faults_halts_after_exhausting_attempts() {
        // F4 retry-then-halt: a persistent node-local provider fault is retried exactly
        // CLOSE_READ_ATTEMPTS times total, then surfaces as the Provider error for the caller
        // to escalate into a halt.
        let calls = Cell::new(0u32);
        let res: Result<(), StateReadError> = retry_provider_faults("test read", || {
            calls.set(calls.get() + 1);
            Err(StateReadError::Provider("mdbx i/o fault".into()))
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
            if calls.get() == 1 {
                Err(StateReadError::Provider("transient i/o fault".into()))
            } else {
                Ok(7u64)
            }
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
            Err(StateReadError::ChainGlobal("contract absent".into()))
        })
        .await;

        assert!(matches!(res, Err(StateReadError::ChainGlobal(_))));
        assert_eq!(calls.get(), 1, "chain-global failures are never retried");
    }

    /// Drive ONE epoch-closing block on `reth_env` (parent = genesis) outside the full engine:
    /// build the block from a boundary [`ConsensusOutput`] (its payload runs `concludeEpoch`),
    /// commit it as the canonical head, and finalize it. Afterwards the canonical tip IS an
    /// epoch-0 closing block, so the F17 close-time identity (`tip + 1 == entered blockHeight`)
    /// holds for `adjust_base_fees`. Mirrors tn-reth's `execute_payload_and_update_canonical_chain`
    /// test helper via the public [`RethEnv`] surface.
    fn execute_epoch_closing_block(
        reth_env: &RethEnv,
        chain: &Arc<RethChainSpec>,
    ) -> eyre::Result<()> {
        let mut leader = Certificate::default();
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.update_header_created_at_for_test(tn_types::now());
        leader.update_header_round_for_test(2);
        let sub_dag = CommittedSubDag::new(
            vec![Certificate::default(), leader.clone()],
            leader,
            1,
            ReputationScores::default(),
            None,
        );
        let output = ConsensusOutput::new_closed_with_subdag(
            sub_dag,
            ConsensusHeader::default().digest(),
            1,
        );

        let parent = chain.sealed_genesis_header();
        let payload = TNPayload::new_for_test(parent.clone(), &output);
        let block =
            reth_env.build_block_from_batch_payload(payload, &Vec::new(), parent.hash(), &[])?;
        let header = block.recovered_block.clone_sealed_header();
        let canonical_state = reth_env.canonical_in_memory_state();
        canonical_state.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
        canonical_state.set_canonical_head(header.clone());
        reth_env.finish_executing_output(vec![block], None)?;
        reth_env.finalize_block(header)?;
        Ok(())
    }

    /// F4 keep-current arm (review G1/P0-1 - first coverage of the close-time fail-open): a
    /// CHAIN-GLOBAL config-read failure (WorkerConfigs contract absent, the alloc-stripped-genesis
    /// trick) at a REAL closing block returns `Ok` and keeps the per-worker base fees, the worker
    /// count, and the accumulated gas untouched. The F17 identity read PASSES here (registry
    /// present, `concludeEpoch` ran in the tip), isolating the failure to the config read's
    /// fail-open arm.
    #[tokio::test]
    async fn adjust_base_fees_keeps_fees_and_count_on_read_failure() -> eyre::Result<()> {
        // registry genesis WITHOUT the WorkerConfigs account: the config read is guaranteed to
        // fail chain-globally (call to codeless address succeeds with empty data -> decode fails)
        let mut genesis = test_genesis_with_consensus_registry(4);
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::with_prefix("adjust_fees_fail_open")?;
        let task_manager = TaskManager::new("adjust fees fail open");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // close epoch 0 so the canonical tip is a closing block and the identity gate passes
        execute_epoch_closing_block(&reth_env, &chain)?;
        let tip = reth_env.canonical_tip();
        let (entered_epoch, epoch_info) = reth_env.get_current_epoch_info_at_header(&tip)?;
        assert_eq!(entered_epoch, 1, "registry state crossed to the entered epoch");
        assert_eq!(tip.number + 1, epoch_info.blockHeight, "F17 identity holds at the tip");

        // non-default fees, gas, and count on the accumulator
        let acc = GasAccumulator::new(2);
        acc.base_fee(0).set_base_fee(4_242);
        acc.base_fee(1).set_base_fee(9_099);
        acc.inc_block(0, 1_000_000, 30_000_000);
        acc.inc_block(1, 2_000_000, 30_000_000);

        // chain-global failure -> keep-current fail-open: Ok, everything untouched
        adjust_base_fees(&reth_env, &acc).await?;

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
}
