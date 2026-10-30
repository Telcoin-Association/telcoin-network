//! The busy-node execution bench: production-shaped load from `tn_reth::test_utils::load` run
//! through `execute_consensus_output`, the engine's per-output code.
//!
//! [`exec_bench`] is ignored; run it alone, in release, with `TMPDIR` on a real disk:
//!
//! ```text
//! TMPDIR=<dir on a real disk> cargo test --release -p tn-engine --test exec_bench -- \
//!     --ignored --exact exec_bench --nocapture
//! ```
//!
//! Add `--features tn-reth/tndb-b` to run the tndb-b backend. Run it with `--exact`: it installs
//! the process's global metrics recorder, and a test that opened a reth env first in the same
//! process would leave reth's metric handles bound to no recorder.
//!
//! **Phases.**
//!
//! 1. A [`CaptureRecorder`] is installed before any env opens. It keeps every sample of the
//!    per-block stages (`tn_reth.block_*_seconds`) and of reth's `save_blocks_*` split; with
//!    `METRICS=prom` everything also goes to tn-metrics' Prometheus recorder, as on a node run with
//!    `--metrics`.
//! 2. The base: [`build_base`] builds the prefilled genesis once per spec, backend and page size in
//!    `CACHE_DIR`, with `--db.page-size` set to `PAGE_SIZE`: both backends fix the page size when
//!    the file is created, so the base, the aged base and every clone share it.
//! 3. The aged base: `AGING` outputs executed on a clone of the base, closed and cached beside it
//!    under [`base_dir_name`] with an `extra` that keys the aging ([`Aging::extra`]). It is built
//!    in a `.building` dir, its meta ([`AgedMeta`]) synced, and becomes the aged base by one
//!    rename. An existing aged base whose meta does not match, or a dir without a meta, is an
//!    error, never rebuilt over. `AGING=0` runs on the base itself.
//! 4. The run: the aged base is cloned into a fresh dir under `TMPDIR` (refused on tmpfs, and when
//!    it is on another file system than the cache: the clone would copy every byte) and opened with
//!    [`slim_chain`] and the production `--db.*` flags for `SYNC` and `PAGE_SIZE`; a clone whose
//!    database has another page size ([`db_page_size`]) stops the run. Worker 0's transaction pool
//!    and HTTP RPC methods are built on it as the node builds them ([`BenchNode::serve`]), so the
//!    pool's maintenance follows every executed output, side load or not. `WARMUP` outputs execute
//!    untimed, then all `OUTPUTS` outputs are built and signed up front, then the timed loop
//!    executes them. Each output runs on a blocking task, as `ExecutorEngine` runs it, while a task
//!    drains the engine-update channel. Just before and just after each output's call, outside its
//!    wall time, the bench reads the database's own counters ([`db_counters`]) and the process's
//!    disk bytes written ([`disk_bytes_written`]), and after it the data file's length (one `stat`,
//!    [`db_file_bytes`]): the database commit completes inside the call on both backends
//!    (`persist_executed_output` commits in line, and neither backend defers its sync to another
//!    thread), and nothing else writes the database, so the differences are the output's.
//!
//!    With `EPOCH_EVERY`, every `EPOCH_EVERY`-th timed output closes its epoch
//!    ([`OutputFactory::next_output_closing`]) and is timed like any other. No output can be built
//!    after a close until the close has executed, so the timed outputs are built in segments
//!    ([`segments`]): the outputs up to a close and the closing output are built and signed, then
//!    executed. After each close the node's epoch boundary runs untimed: the accumulator is
//!    cleared, the factory enters the new epoch ([`OutputFactory::begin_epoch`]) and its committee
//!    is set on the accumulator. The time between segments counts neither in the pacing nor in the
//!    loop's wall clock ([`Timed`]); the side-load windows and the pacing slots follow the timed
//!    outputs' indices across segments. The warm-up and the aging never close an epoch.
//! 5. The side load, with `READERS` > 0 ([`SideLoad`], over `tn_reth::test_utils::load::readers`):
//!    the timed outputs go in windows of `WINDOW` outputs, the first with the side load off, the
//!    next on, and so on ([`side_load_on`]); the warm-up runs with it off. While it is on,
//!    `READERS` reader threads call the RPC methods back to back, a poller calls the pool's
//!    `get_account_balances` every 250 ms as the batch builder does, and just before each output
//!    executes `SUBMIT_FRACTION` of its raw transactions are queued for `eth_sendRawTransaction`,
//!    as user transactions arriving over RPC. At each window's start the readers' gate switches and
//!    their samples go to the window that ended. Each executed output's transaction hashes (the
//!    keccak of each raw transaction) feed the readers' receipt lookups. With `READERS=0` there are
//!    no windows, readers, poller or submitter.
//! 6. The export, with `EXPORT=1` ([`Export`]): just before the first timed output the clone's
//!    state is pinned at the tip (`RethEnv::pin_state_view`, checked with `verify_tip` as the node
//!    checks its pin), and a side thread writes it to an exec state pack in the run's temp dir with
//!    the arguments the node's snapshot export passes: the tip's state root and its headers
//!    ([`export_headers`]). The node exports only a tip that closed an epoch; the bench exports the
//!    tip it pinned. The database's sizes are taken at the pin and when the pack has finished,
//!    before the view drops: MDBX cannot reuse the pages a long reader pins, so its growth while
//!    pinned shows. A row is `export_active` when the export ran at any point during its output.
//!    The export ends before the env closes; a failed export is reported, then fails the bench.
//!
//! **Knobs** (environment variables, each refused when malformed, as is an unknown
//! `TN_EXEC_BENCH_*` name):
//!
//! - `TN_EXEC_BENCH_OUTPUTS` (300): timed outputs, at least 1.
//! - `TN_EXEC_BENCH_BATCHES` (8): batches, and so blocks, per output, at least 1.
//! - `TN_EXEC_BENCH_GAS` (15000000): each batch's target expected gas, at least 1.
//! - `TN_EXEC_BENCH_WARMUP` (20): untimed outputs on the clone before the timed loop.
//! - `TN_EXEC_BENCH_AGING` (200): outputs executed into the cached aged base; 0 runs on the base.
//! - `TN_EXEC_BENCH_PREFILL` (1000000): prefilled accounts, the existing recipients.
//! - `TN_EXEC_BENCH_SENDERS` (10000): keyed senders.
//! - `TN_EXEC_BENCH_POOLS` (64): swap router pools.
//! - `TN_EXEC_BENCH_SEED` (0): seeds the prefill and the aging's draws; the warm-up and timed
//!   outputs draw with `SEED + 1`, so they do not replay the aging's draws.
//! - `TN_EXEC_BENCH_REGISTRY` (1): `1` builds on the `ConsensusRegistry` genesis (the committee
//!   rotates leaders and producers), `0` on the plain test genesis.
//! - `TN_EXEC_BENCH_PACE_MS` (0): 0 runs the outputs back to back; otherwise output `i` starts at
//!   `t0 + i * PACE_MS`, and the report counts the outputs that started late.
//! - `TN_EXEC_BENCH_METRICS` (`off`): `off`, or `prom` for a `--metrics` node's recorder.
//! - `TN_EXEC_BENCH_SYNC` (`durable`): `durable` (`--db.sync-mode durable`) or `nosync`
//!   (`--db.sync-mode safe-no-sync`, MDBX only: the tndb-b build refuses that flag).
//! - `TN_EXEC_BENCH_PAGE_SIZE` (the backend's default, [`default_page_size`]: MDBX the OS page
//!   size, 16384 on Apple Silicon and 4096 on x86 Linux; tndb-b 4096): the database page size in
//!   bytes, a power of two from 4096 to 65536, passed as `--db.page-size` to the base's build and
//!   every open. Each page size has its own base and aged base.
//! - `TN_EXEC_BENCH_READERS` (0): reader threads, plus the balance poller when above 0; 0 runs no
//!   side load.
//! - `TN_EXEC_BENCH_SUBMIT_FRACTION` (0.25): the fraction, from 0 to 1, of each on-window output's
//!   raw transactions submitted through `eth_sendRawTransaction` before it executes ([`submitted`]
//!   spreads them evenly).
//! - `TN_EXEC_BENCH_WINDOW` (20): timed outputs per side-load window, at least 1.
//! - `TN_EXEC_BENCH_EPOCH_EVERY` (0): 0 never closes an epoch; otherwise every `EPOCH_EVERY`-th
//!   timed output closes one (phase 4). It needs `REGISTRY=1`.
//! - `TN_EXEC_BENCH_EXPORT` (0): `1` runs the snapshot export's long read beside the timed loop
//!   (phase 6).
//! - `TN_EXEC_BENCH_CACHE_DIR` (`<workspace>/target/exec-bench-cache`): where the base and the aged
//!   base are cached. It must be on `TMPDIR`'s file system.
//! - `TN_EXEC_BENCH_REPORT` (unset): a path to also write the report to as JSON ([`json_report`]).
//!   Its dir must exist; it is checked before the run.
//!
//! **Report.** A header with the backend, sync mode, metrics mode, pacing, every knob, the page
//! size (the open clone's, read with [`db_page_size`], and `PAGE_SIZE` as set or `default`), where
//! the disk bytes come from ([`DISK_BYTES_SOURCE`], with the first failed read's reason), the
//! host and the date (the base's `db` size is its data file's allocated bytes, then its length,
//! which MDBX preallocates), and `pool + RPC: on`; one row per stage with the mean, p50, p90, p99
//! (nearest rank) and max in milliseconds per output and its share of the total; then the
//! throughput. Then:
//!
//! - `thirds`: each stage's mean and p50 over the first and the last third of the timed outputs
//!   (`OUTPUTS / 3` each), and their ratios, last over first: how the stage changes as the history
//!   grows. None for fewer than 3 outputs.
//! - `sizes`: the clone's database data file and static files, allocated bytes and length, when the
//!   env has opened on it, before and after the timed loop (the env open), and once the env has
//!   closed; then the allocated bytes the timed loop added per output, from before to after; then
//!   the logical growth, the tables' total size added per executed output (warm-up and timed), from
//!   the clone before the env opened to the closed clone ([`Footprint`]). tndb-b's data file grows
//!   in preallocated steps, so over a short run its allocated growth reads 0.
//! - `database writes per output`, after the sizes: what each timed output's call wrote
//!   ([`DbWrites`]): each database counter that moved, as its mean per output (`_ns` counters in
//!   ms; `tndb_max_gate_lag`, a maximum since the open, as its largest, [`MAXIMA`]), leaving out
//!   the counters that never moved; the disk MiB per output; the data file's growth per output and
//!   over the run, from its length before the first timed output; then the deciles: the timed
//!   outputs in ten consecutive groups (group `k` from `k * n / 10` to `(k + 1) * n / 10`), each
//!   with its outputs, its mean `commit mdbx` ms and disk MiB per output, and the data file's
//!   length at its end. None for fewer than 10 outputs.
//! - `table`: the largest of reth's tables in the closed clone (`db_table_stats`), with rows and
//!   size, then the total over every table. When the table stats fail, the section says so with the
//!   error, the report and the JSON are still written, and the bench then fails.
//! - `side load`: with the side load on, the writer's main stages ([`SPLIT_KEYS`]) with it off and
//!   on (mean, p50, p99, and on over off); per reader kind the calls, calls per second over the on
//!   windows' wall time, p50 and p99 latency, and ok, empty and error counts with the first error;
//!   the submitter's accepted, `nonce too low`, `already known` and error counts with p50 and p99;
//!   the poller's polls with p50 and p99, and its degraded windows (a polled address without a
//!   balance, or a zero one, in a window's last poll); the reader and poller calls in off windows.
//!   Any reader or submitter error, degraded window or off-window call fails the bench after the
//!   report and the JSON are written; refused submissions of mined or pooled transactions are
//!   counted, not errors.
//! - `epoch closes`, after the throughput: with `EPOCH_EVERY`, the closing outputs' count and the
//!   writer's main stages ([`SPLIT_KEYS`]) over the normal and the closing outputs (mean, p50, p99,
//!   and close over normal); one line without.
//! - `export`, after the sizes: with `EXPORT=1`, the pinned block, the export's duration (from the
//!   pin to the pack's finish), its stats (accounts, storage slots, bytecodes) and the pack's
//!   length, the timed outputs it overlapped, the database's allocated bytes and length at the pin
//!   and at the pack's finish with the growth between them, and the writer's main stages with the
//!   export idle and active; one line without. A failed export is reported with its error, and
//!   fails the bench after the report and the JSON are written.
//!
//! The stages:
//!
//! - `total`: the wall time of one `execute_consensus_output` call.
//! - `execute`, `finish` and `overlay extend`: the output's blocks' `tn_reth.block_*_seconds`,
//!   summed. `execute` includes `signer recovery`; `finish` includes `hashed state` and `state
//!   root`.
//! - `save_blocks`: reth's `save_blocks_total`. `mdbx` is its database writes, which include
//!   `insert_block` through `update_history_indices`; `sf` is the static-file writes, which run
//!   beside them on another thread.
//! - `commit sf` and `commit mdbx`: the static files' commit and the database commit after
//!   `save_blocks`, outside `save_blocks_total`.
//! - `other`: `total` minus `execute`, `finish`, `overlay extend`, `save_blocks`, `commit sf` and
//!   `commit mdbx`: the engine's own work, announcing the output and finalizing.
//!
//! **JSON report.** One object, its names snake_case with the unit in the name, for scripts that
//! aggregate runs: `report_version` (4); `header` (backend, sync, `page_size_bytes` (the open
//! clone's), `disk_bytes_source` (`"proc_pid_rusage"`, `"/proc/self/io"` or null),
//! `disk_bytes_error` (the first failed read's reason, or null), `db_counter_names` (the database
//! counters' names in the build's order: JSON objects come out with their keys sorted), metrics,
//! pacing, host, date, base, aged, `pool_rpc`, and `knobs`, every knob, with `page_size` null for
//! the backend's default);
//! `rows`, one per timed output (`first_block`, `last_block`, `gas`, `txs`, `late`, `side_load`,
//! `epoch_close`, `export_active`, each stage as `<key>_ms`, `db_counters` (an object of each
//! database counter's name, `mdbx_*` or `tndb_*`, and its difference over the output),
//! `disk_bytes_written` (null when a read failed or the target has no
//! source) and `db_file_len_bytes` (after the output)); `stats`, per
//! stage key (`mean_ms`, `p50_ms`, `p90_ms`, `p99_ms`, `max_ms`, `share_pct`); `throughput`;
//! `thirds` (null for fewer than 3 outputs: `outputs_per_third` and per stage key `first_mean_ms`,
//! `first_p50_ms`, `last_mean_ms`, `last_p50_ms`, `mean_ratio`, `p50_ratio`); `sizes` (`open`,
//! `before_timed`, `after_timed` and `closed`, each `db_len_bytes`, `db_allocated_bytes`,
//! `static_files_len_bytes` and `static_files_allocated_bytes`, then
//! `db_growth_per_output_bytes`, `static_files_growth_per_output_bytes` and
//! `db_logical_growth_per_output_bytes`); `db_writes` (`counters_mean_per_output`, every counter's
//! mean per output but the maxima, `counters_max`, each of [`MAXIMA`]'s largest,
//! `disk_bytes_mean_per_output` (null when no output has it), `disk_bytes_outputs`,
//! `db_file_len_before_bytes`, `db_file_len_after_bytes`, `db_file_growth_bytes`,
//! `db_file_growth_per_output_bytes`, and `deciles`, null for fewer than 10 outputs, else ten of
//! `outputs`, `commit_mdbx_mean_ms`, `disk_bytes_mean` (or null), `db_file_len_mean_bytes` and
//! `db_file_len_end_bytes`); `tables` (`failed`, `error`, `baseline_total_size_bytes`,
//! `count`, `total_entries`, `total_size_bytes`, and `by_size`, every table largest first; the last
//! four null when `failed`); and `side_load`, null with `READERS=0`, else `off_outputs`,
//! `on_outputs`, `on_time_s`, `errors`, `writer` (per [`SPLIT_KEYS`] key `off` and `on`, each
//! `mean_ms`, `p50_ms` and `p99_ms` or null, and `mean_ratio`, `p50_ratio`, `p99_ratio`),
//! `readers` (per kind `method`, `ops`, `ops_per_s`, `p50_ms`, `p99_ms`, `ok`, `empty`, `errors`,
//! `first_error`), `submitter` (`accepted`, `nonce_too_low`, `already_known`, `errors`,
//! `first_error`, `p50_ms`, `p99_ms`), `poller` (`polls`, `p50_ms`, `p99_ms`, `degraded_windows`)
//! and `off_window_calls`; `epoch_closes`, null with `EPOCH_EVERY=0`, else `closes`,
//! `normal_outputs` and `writer` (per [`SPLIT_KEYS`] key `normal` and `closing`, each `mean_ms`,
//! `p50_ms` and `p99_ms` or null, and `mean_ratio`, `p50_ratio`, `p99_ratio`, closing over
//! normal); and `export`, null with `EXPORT=0`, else `failed`, `error`, `block`, `duration_s`,
//! `account_count`, `storage_slots`, `bytecodes`, `pack_len_bytes`, `pack_allocated_bytes`,
//! `at_pin` and `at_finish` (each as a `sizes` point), `db_allocated_growth_bytes` and
//! `db_len_growth_bytes` (from `block` on null when `failed`), `overlapped_outputs`, and `writer`
//! (per key `idle` and `active`, with the ratios active over idle). The stage keys are
//! [`STAGES`]'s. It is written to a temp file, synced and renamed into place.
//!
//! **Durable sync on macOS.** libmdbx's durable commit uses `F_FULLFSYNC` and tndb-b's a plain
//! fsync, so Mac durable commit times are not like-for-like between the backends.
//!
//! **Reading the database writes.** The disk bytes are only an approximation: Linux's
//! `write_bytes` counts a page as it is dirtied, so a page dirtied once and synced counts once, and
//! it includes the static files' (NippyJar) writes and anything else the process writes (the
//! export's pack, say); macOS counts the I/O the process issues and misses background writeback. A
//! "pages synced" counter is in database pages, not bytes: an OS page larger than the database page
//! writes more. MDBX's counters are per environment and count from its lock file's creation, so
//! only the per-output differences mean anything; `mdbx_wops` stays 0 (reth maps MDBX writable, so
//! its syncs show in `mdbx_msync`).

#![allow(unused_crate_dependencies)]

use jsonrpsee::{Methods, RpcModule};
use metrics::Recorder;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tn_engine::execute_consensus_output;
use tn_reth::{
    db_table_stats, default_page_size,
    payload::BuildArguments,
    snapshot::PinnedStateView,
    test_utils::load::{
        announce_temp_dir,
        base::{base_dir_name, BACKEND, META_FILE},
        build_base, clone_datadir, db_counters, db_file_bytes, db_page_size, dir_bytes,
        open_production_env_with_db, poll_addresses,
        readers::{KindSamples, PollSamples, POLL_INTERVAL},
        slim_chain,
        workload::WorkloadConfig,
        BaseMeta, CaptureRecorder, LoadOutput, OutputConfig, OutputFactory, PrefillSpec, ReadKind,
        ReadSamples, Readers, SubmitSamples, Submitter,
    },
    worker::WorkerNetwork,
    DbTableStat, RethChainSpec, RethDb, RethEnv, WorkerTxPool,
};
use tn_storage::{
    disk::{fsync_directory, fsync_file},
    exec_state_pack::{ExecStateStats, BLOCKHASH_ANCESTORS},
};
use tn_types::{
    gas_accumulator::GasAccumulator, keccak256, repack_monitor::RepackMonitor, Address,
    ConsensusHeader, ConsensusOutput, EngineUpdate, ExecHeader, NodeMode, SealedHeader,
    TaskManager, B256,
};
use tokio::{sync::mpsc, task::JoinHandle};

/// A reth env opened as the node opens it, with the gas accumulator the engine credits.
struct BenchNode {
    env: RethEnv,
    /// The env's database, for the reads the env does not offer (its page size, its counters).
    db: RethDb,
    /// The datadir's `db` dir, holding the database's data file ([`db_file_bytes`]).
    db_dir: PathBuf,
    gas_accumulator: GasAccumulator,
    _tasks: TaskManager,
    _tmp: Option<TempDir>,
}

impl BenchNode {
    /// Open `spec`'s genesis in a temp dir with one worker's accumulator.
    fn open(spec: &PrefillSpec) -> eyre::Result<Self> {
        let tmp = tempfile::tempdir()?;
        let datadir = tmp.path().to_path_buf();
        Self::open_at(&datadir, Arc::new(spec.genesis().into()), &[], Some(tmp))
    }

    /// Open the datadir `datadir` on `chain` with the `--db.*` flags `db_flags` and one worker's
    /// accumulator, keeping `tmp` (the dir holding `datadir`, if any) until the env closes.
    fn open_at(
        datadir: &Path,
        chain: Arc<RethChainSpec>,
        db_flags: &[&str],
        tmp: Option<TempDir>,
    ) -> eyre::Result<Self> {
        let tasks = TaskManager::default();
        let (env, db) = open_production_env_with_db(datadir, chain, db_flags, &tasks)?;
        Ok(Self {
            env,
            db,
            db_dir: datadir.join("db"),
            gas_accumulator: GasAccumulator::new(1),
            _tasks: tasks,
            _tmp: tmp,
        })
    }

    /// A factory after the env's last executed output, with its committee (if any) set on the
    /// accumulator as the node sets it at epoch start.
    fn factory(
        &self,
        spec: PrefillSpec,
        workload: WorkloadConfig,
        config: OutputConfig,
    ) -> eyre::Result<OutputFactory> {
        let factory = OutputFactory::new(&self.env, spec, workload, config)?;
        if let Some(committee) = factory.committee() {
            self.gas_accumulator.rewards_counter().set_committee(committee.clone());
        }
        Ok(factory)
    }

    /// Worker 0's transaction pool and HTTP RPC methods on the env, built as the node builds them
    /// at worker start: the pool on the accumulator's base fee, its block info at the finalized
    /// tip, and the RPC server on the accumulator's worker base fee. The pool's maintenance task
    /// follows the canonical-state notifications of every executed output, readers or not.
    ///
    /// The node fills the RPC server's last slot with its `tn` namespace
    /// (`TelcoinNetworkRpcExt::into_rpc`) and reads its node mode from it; the side load never
    /// calls `tn_*`, so an empty module and an active validator's mode stand in.
    fn serve(&self) -> eyre::Result<(WorkerTxPool, Methods)> {
        let base_fee = self.gas_accumulator.base_fee(0);
        let pool = self.env.init_txn_pool(base_fee.clone())?;
        let mut block_info = pool.block_info();
        block_info.pending_basefee = base_fee.base_fee();
        let last_seen = self.env.finalized_block_hash_number_for_startup()?;
        block_info.last_seen_block_hash = last_seen.hash;
        block_info.last_seen_block_number = last_seen.number;
        pool.set_block_info(block_info);
        let network =
            WorkerNetwork::new(self.env.chainspec(), env!("CARGO_PKG_VERSION"), self.env.clone());
        let methods = self
            .env
            .get_rpc_server(
                pool.clone(),
                network,
                self.gas_accumulator.worker_base_fee(0),
                || NodeMode::CvvActive,
                RpcModule::new(()),
            )?
            .http_methods(|_| true)
            .ok_or_else(|| eyre::eyre!("the production env serves HTTP"))?;
        Ok((pool, methods))
    }

    /// Close the env and wait until its database is closed: drop it and the database handle, then
    /// signal and abort the tasks spawned on its task manager (the pool's maintenance and
    /// transaction validation and the RPC's state cache hold the database) and wait for them; a
    /// task's join resolves once its future, and what it holds, is dropped. The validation
    /// service runs on the blocking pool, which only the shutdown signal ends, and dropping the
    /// manager would signal without waiting.
    async fn close(self) -> eyre::Result<()> {
        let Self { env, db, db_dir: _, gas_accumulator, _tasks: mut tasks, _tmp } = self;
        drop((env, db, gas_accumulator));
        tasks.update_tasks();
        tasks.signal_shutdown();
        tasks.abort();
        let grace = Duration::from_secs(10);
        eyre::ensure!(
            tasks.wait_for_exit(grace).await,
            "the env's tasks still ran after {grace:?}: {tasks}"
        );
        Ok(())
    }

    /// Run `load` through `execute_consensus_output` on a blocking task on the canonical tip, as
    /// the engine runs each output, and return the new tip with the engine updates it sent.
    async fn execute(&self, load: &LoadOutput) -> eyre::Result<(SealedHeader, Vec<EngineUpdate>)> {
        let (engine_update_tx, mut engine_update_rx) = mpsc::channel(ENGINE_UPDATE_SLOTS);
        let args =
            BuildArguments::new(self.env.clone(), load.output.clone(), self.env.canonical_tip());
        let gas_accumulator = self.gas_accumulator.clone();
        let tip = tokio::task::spawn_blocking(move || {
            execute_consensus_output(
                args,
                gas_accumulator,
                RepackMonitor::default(),
                engine_update_tx,
            )
        })
        .await??;
        let mut updates = Vec::new();
        while let Some(update) = engine_update_rx.recv().await {
            updates.push(update);
        }
        Ok((tip, updates))
    }

    /// Run `output` through `execute_consensus_output` on a blocking task on the canonical tip,
    /// sending its engine update on `engine_update_tx`, and return the new tip and the call's
    /// wall time.
    async fn execute_timed(
        &self,
        output: ConsensusOutput,
        engine_update_tx: &mpsc::Sender<EngineUpdate>,
    ) -> eyre::Result<(SealedHeader, Duration)> {
        let args = BuildArguments::new(self.env.clone(), output, self.env.canonical_tip());
        let gas_accumulator = self.gas_accumulator.clone();
        let engine_update_tx = engine_update_tx.clone();
        let (tip, wall) = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            let tip = execute_consensus_output(
                args,
                gas_accumulator,
                RepackMonitor::default(),
                engine_update_tx,
            );
            (tip, start.elapsed())
        })
        .await?;
        Ok((tip?, wall))
    }

    /// Check the blocks `load` executed from block `first` on top of `parent`: one per batch, each
    /// on the one before, with every batch transaction (by hash, as [`tx_hashes`] derives them
    /// from the raw transactions), exactly the batch's expected gas and the output's commit time.
    /// Returns the last block's hash.
    fn assert_blocks(&self, load: &LoadOutput, first: u64, mut parent: B256) -> eyre::Result<B256> {
        let mut expected_hashes = tx_hashes(load).into_iter();
        for (number, meta) in (first..).zip(&load.batches) {
            let block = self.env.sealed_block_by_number(number)?.expect("an executed block");
            assert_eq!(block.parent_hash, parent, "block {number} is on the block before");
            assert_eq!(block.body().transactions.len(), meta.len(), "block {number} skips no tx");
            for tx in &block.body().transactions {
                assert_eq!(Some(*tx.tx_hash()), expected_hashes.next(), "block {number}'s tx hash");
            }
            assert_eq!(block.gas_used, meta.expected_gas(), "block {number} uses the batch's gas");
            assert_eq!(
                block.timestamp,
                load.output.committed_at(),
                "block {number} is not clamped"
            );
            parent = block.hash();
        }
        assert_eq!(expected_hashes.next(), None, "every raw transaction executed");
        Ok(parent)
    }
}

/// Three outputs of three batches from an `OutputFactory` execute through
/// `execute_consensus_output` as nine chained, persisted and finalized blocks with every
/// transaction, the exact expected gas and unclamped timestamps. A second factory on the same env
/// continues the consensus chain after them.
#[tokio::test]
async fn test_load_outputs_chain_through_execute_consensus_output() -> eyre::Result<()> {
    let spec = PrefillSpec { accounts: 2_000, senders: 8, pools: 2, seed: 17, registry: true };
    let config = OutputConfig { batches: 3, batch_gas: 3_000_000, ..OutputConfig::default() };
    let node = BenchNode::open(&spec)?;
    let genesis = node.env.canonical_tip();

    let outputs = node.factory(spec, WorkloadConfig::default(), config)?.generate(3);
    let mut updates = 0;
    for load in &outputs {
        let (tip, sent) = node.execute(load).await?;
        let expected = (load.output.leader_round(), load.output.num_hash(), Some(tip));
        assert_eq!(sent, vec![expected], "one engine update per output");
        updates += sent.len();
    }
    assert_eq!(updates, 3);
    assert_eq!(node.env.canonical_tip().number, 9);
    assert_eq!(node.env.last_block_number()?, 9, "persisted");
    assert_eq!(node.env.last_finalized_block_number()?, 9, "finalized");
    let mut parent = genesis.hash();
    for (load, first) in outputs.iter().zip([1, 4, 7]) {
        parent = node.assert_blocks(load, first, parent)?;
    }

    // a second factory reads its starting point from the env
    let workload = WorkloadConfig { seed: 1, ..WorkloadConfig::default() };
    let mut resumed = node.factory(spec, workload, config)?;
    assert_eq!(resumed.last_number(), 3);
    let next = resumed.next_output();
    assert_eq!(next.output.number(), 4);
    assert_eq!(next.output.parent_hash(), outputs[2].output.consensus_header_hash());
    let (tip, sent) = node.execute(&next).await?;
    assert_eq!(sent.len(), 1);
    assert_eq!(tip.number, 12);
    node.assert_blocks(&next, 10, parent)?;

    // every leader was a committee member: the rewards counter credits each output's leader
    let leader_counts = node.gas_accumulator.rewards_counter().get_address_counts();
    assert_eq!(leader_counts.values().sum::<u32>(), 4);
    Ok(())
}

/// Three consecutive epochs close through `execute_consensus_output`, each after two outputs. Each
/// close advances the registry's epoch and pays the epoch's leaders; after the node's boundary
/// steps (clear the accumulator, `OutputFactory::begin_epoch`, set the new committee) the next
/// epoch's outputs carry that epoch and leaders from its committee. Every output executes as one
/// update and one block per batch, with no consensus number, block or transaction skipped across
/// the boundaries.
#[tokio::test]
async fn test_load_outputs_close_three_epochs_through_execute_consensus_output() -> eyre::Result<()>
{
    const CLOSES: u32 = 3;
    let spec = PrefillSpec { accounts: 2_000, senders: 8, pools: 2, seed: 19, registry: true };
    let config = OutputConfig { batches: 2, batch_gas: 3_000_000, ..OutputConfig::default() };
    let node = BenchNode::open(&spec)?;
    let mut factory = node.factory(spec, WorkloadConfig::default(), config)?;
    let mut parent = node.env.canonical_tip().hash();
    let mut digest = ConsensusHeader::default().digest();
    let (mut number, mut block) = (0, 1);

    // epochs 0 to 2 close after two outputs; the epoch after the last close runs one output
    for epoch in 0..=CLOSES {
        let closing = epoch < CLOSES;
        assert_eq!(factory.epoch(), epoch);
        assert_eq!(node.env.epoch_state_from_canonical_tip()?.epoch, epoch, "the registry's epoch");
        let committee = factory.committee().cloned().expect("a registry committee");
        assert_eq!(committee.epoch(), epoch);
        let mut outputs = factory.generate(if closing { 2 } else { 1 });
        if closing {
            outputs.push(factory.next_output_closing()?);
        }
        for load in &outputs {
            let output = &load.output;
            assert_eq!(output.number(), number + 1, "no consensus number skipped");
            assert_eq!(
                output.parent_hash(),
                digest,
                "output {} follows its parent",
                output.number()
            );
            assert_eq!(output.leader().epoch(), epoch);
            let leader = output.leader().author();
            assert!(committee.authority(leader).is_some(), "a leader of epoch {epoch}'s committee");
            let (tip, sent) = node.execute(load).await?;
            assert_eq!(sent, vec![(output.leader_round(), output.num_hash(), Some(tip))]);
            parent = node.assert_blocks(load, block, parent)?;
            block += load.batches.len() as u64;
            (number, digest) = (output.number(), output.consensus_header_hash());
        }
        // the accumulator credits only committee members
        let credited = node.gas_accumulator.rewards_counter().get_address_counts();
        assert_eq!(credited.values().sum::<u32>() as usize, outputs.len(), "every leader credited");
        if !closing {
            break;
        }

        // the close ran with the closing output's seed and entered the next epoch
        let seed = outputs[2].output.committee_shuffle_seed();
        assert_eq!(&node.env.canonical_tip().extra_data[..], seed.as_slice());
        assert_eq!(node.env.epoch_state_from_canonical_tip()?.epoch, epoch + 1, "epoch advanced");

        // the node's boundary: clear the accumulator, enter the epoch, set its committee
        node.gas_accumulator.clear();
        let committee = factory.begin_epoch(&node.env)?;
        node.gas_accumulator.rewards_counter().set_committee(committee.clone());
    }

    assert_eq!(number, 10);
    assert_eq!(node.env.canonical_tip().number, 20);
    assert_eq!(node.env.last_block_number()?, 20, "persisted");
    assert_eq!(node.env.last_finalized_block_number()?, 20, "finalized");
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// The bench driver
// ------------------------------------------------------------------------------------------------

/// The knobs' environment variable prefix.
const KNOB_PREFIX: &str = "TN_EXEC_BENCH_";

/// The knob names this build reads, after [`KNOB_PREFIX`].
const KNOB_NAMES: [&str; 21] = [
    "OUTPUTS",
    "BATCHES",
    "GAS",
    "WARMUP",
    "AGING",
    "PREFILL",
    "SENDERS",
    "POOLS",
    "SEED",
    "REGISTRY",
    "PACE_MS",
    "METRICS",
    "SYNC",
    "PAGE_SIZE",
    "READERS",
    "SUBMIT_FRACTION",
    "WINDOW",
    "EPOCH_EVERY",
    "EXPORT",
    "CACHE_DIR",
    "REPORT",
];

/// The engine-update channel's capacity.
const ENGINE_UPDATE_SLOTS: usize = 64;

/// The aging outputs built and signed at a time, which bounds the aging's memory.
const AGING_CHUNK: usize = 50;

/// How the database syncs its commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SyncMode {
    /// `--db.sync-mode durable`.
    Durable,
    /// `--db.sync-mode safe-no-sync` (MDBX only).
    NoSync,
}

impl SyncMode {
    /// The `--db.*` flags that select this mode.
    fn db_flags(self) -> [&'static str; 2] {
        match self {
            Self::Durable => ["--db.sync-mode", "durable"],
            Self::NoSync => ["--db.sync-mode", "safe-no-sync"],
        }
    }

    /// The mode's knob value and report label.
    fn label(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::NoSync => "nosync",
        }
    }
}

/// The smallest and the largest `PAGE_SIZE`, in bytes: the range both backends accept.
const PAGE_SIZES: std::ops::RangeInclusive<u32> = 4096..=65_536;

/// The `--db.*` flags that open a datadir with `sync` and, for a new database file, `page_size`
/// (a number of bytes) pages; an existing file keeps its own page size.
fn db_flags(sync: SyncMode, page_size: &str) -> [&str; 4] {
    let [mode_flag, mode] = sync.db_flags();
    [mode_flag, mode, "--db.page-size", page_size]
}

/// Where metrics go besides the capture recorder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MetricsMode {
    /// Nowhere, as on a node run without `--metrics`.
    Off,
    /// tn-metrics' Prometheus recorder, as on a node run with `--metrics`.
    Prom,
}

impl MetricsMode {
    /// The mode's knob value and report label.
    fn label(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Prom => "prom",
        }
    }
}

/// The bench's knobs (see the module docs).
#[derive(Clone, Debug, PartialEq)]
struct Knobs {
    outputs: usize,
    batches: usize,
    gas: u64,
    warmup: usize,
    aging: usize,
    prefill: usize,
    senders: usize,
    pools: usize,
    seed: u64,
    registry: bool,
    pace_ms: u64,
    metrics: MetricsMode,
    sync: SyncMode,
    /// The requested page size in bytes; `None` for the backend's default ([`Knobs::page_size`]).
    page_size: Option<u32>,
    readers: usize,
    submit_fraction: f64,
    window: usize,
    epoch_every: usize,
    export: bool,
    cache_dir: PathBuf,
    report: Option<PathBuf>,
}

impl Knobs {
    /// The knobs from this process's environment, for the database backend `backend`.
    fn from_env(backend: &str) -> eyre::Result<Self> {
        let mut vars = Vec::new();
        for (name, value) in std::env::vars_os() {
            let Some(name) = name.to_str().filter(|name| name.starts_with(KNOB_PREFIX)) else {
                continue;
            };
            let value = value
                .into_string()
                .map_err(|value| eyre::eyre!("{name} is not valid UTF-8: {value:?}"))?;
            vars.push((name.to_owned(), value));
        }
        Self::parse(vars, backend)
    }

    /// The knobs from the `TN_EXEC_BENCH_*` variables in `vars` (others are ignored), the
    /// defaults for those absent, for the database backend `backend`. Refuses an unknown knob, a
    /// malformed value, a `PAGE_SIZE` that is not a power of two from 4096 to 65536,
    /// `SYNC=nosync` on tndb-b, and `EPOCH_EVERY` without `REGISTRY=1`.
    fn parse(
        vars: impl IntoIterator<Item = (String, String)>,
        backend: &str,
    ) -> eyre::Result<Self> {
        let mut set = BTreeMap::new();
        for (name, value) in vars {
            let Some(knob) = name.strip_prefix(KNOB_PREFIX) else { continue };
            eyre::ensure!(
                KNOB_NAMES.contains(&knob),
                "unknown knob {name}: this build reads {KNOB_PREFIX}{{{}}}",
                KNOB_NAMES.join(",")
            );
            set.insert(knob.to_owned(), value);
        }
        let get = |knob: &str| set.get(knob).map(String::as_str);
        let number = |knob: &str, default: u64, min: u64| -> eyre::Result<u64> {
            let Some(value) = get(knob) else { return Ok(default) };
            let parsed: u64 = value.parse().map_err(|e| {
                eyre::eyre!("{KNOB_PREFIX}{knob}={value:?} is not a whole number: {e}")
            })?;
            eyre::ensure!(parsed >= min, "{KNOB_PREFIX}{knob}={parsed} is below {min}");
            Ok(parsed)
        };
        let count = |knob: &str, default: usize, min: usize| -> eyre::Result<usize> {
            Ok(usize::try_from(number(knob, default as u64, min as u64)?)?)
        };
        let choice = |knob: &str, choices: &[&'static str]| -> eyre::Result<&'static str> {
            let Some(value) = get(knob) else { return Ok(choices[0]) };
            choices.iter().copied().find(|choice| *choice == value).ok_or_else(|| {
                eyre::eyre!("{KNOB_PREFIX}{knob}={value:?} is not one of {}", choices.join(", "))
            })
        };

        let registry = choice("REGISTRY", &["1", "0"])? == "1";
        let metrics = if choice("METRICS", &["off", "prom"])? == "off" {
            MetricsMode::Off
        } else {
            MetricsMode::Prom
        };
        let sync = if choice("SYNC", &["durable", "nosync"])? == "durable" {
            SyncMode::Durable
        } else {
            SyncMode::NoSync
        };
        eyre::ensure!(
            !(sync == SyncMode::NoSync && backend == "tndb-b"),
            "{KNOB_PREFIX}SYNC=nosync is --db.sync-mode safe-no-sync, which the tndb-b build \
             refuses: run tndb-b with SYNC=durable"
        );
        let page_size = match get("PAGE_SIZE") {
            None => None,
            Some(value) => {
                let refuse = || {
                    eyre::eyre!(
                        "{KNOB_PREFIX}PAGE_SIZE={value:?} is not a page size: a power of two from \
                         {} to {} bytes",
                        PAGE_SIZES.start(),
                        PAGE_SIZES.end()
                    )
                };
                let parsed: u32 = value.parse().map_err(|_| refuse())?;
                eyre::ensure!(parsed.is_power_of_two() && PAGE_SIZES.contains(&parsed), refuse());
                Some(parsed)
            }
        };
        let submit_fraction = match get("SUBMIT_FRACTION") {
            None => 0.25,
            Some(value) => {
                let parsed: f64 = value.parse().map_err(|e| {
                    eyre::eyre!("{KNOB_PREFIX}SUBMIT_FRACTION={value:?} is not a number: {e}")
                })?;
                eyre::ensure!(
                    (0.0..=1.0).contains(&parsed),
                    "{KNOB_PREFIX}SUBMIT_FRACTION={value:?} is not between 0 and 1"
                );
                parsed
            }
        };
        let epoch_every = count("EPOCH_EVERY", 0, 0)?;
        eyre::ensure!(
            epoch_every == 0 || registry,
            "{KNOB_PREFIX}EPOCH_EVERY={epoch_every} closes epochs through the ConsensusRegistry: \
             it needs {KNOB_PREFIX}REGISTRY=1"
        );
        let export = choice("EXPORT", &["0", "1"])? == "1";
        let cache_dir = match get("CACHE_DIR") {
            Some("") => eyre::bail!("{KNOB_PREFIX}CACHE_DIR is empty"),
            Some(dir) => PathBuf::from(dir),
            None => default_cache_dir(),
        };
        let report = match get("REPORT") {
            Some("") => eyre::bail!("{KNOB_PREFIX}REPORT is empty"),
            report => report.map(PathBuf::from),
        };
        Ok(Self {
            outputs: count("OUTPUTS", 300, 1)?,
            batches: count("BATCHES", 8, 1)?,
            gas: number("GAS", 15_000_000, 1)?,
            warmup: count("WARMUP", 20, 0)?,
            aging: count("AGING", 200, 0)?,
            prefill: count("PREFILL", 1_000_000, 0)?,
            senders: count("SENDERS", 10_000, 1)?,
            pools: count("POOLS", 64, 0)?,
            seed: number("SEED", 0, 0)?,
            registry,
            pace_ms: number("PACE_MS", 0, 0)?,
            metrics,
            sync,
            page_size,
            readers: count("READERS", 0, 0)?,
            submit_fraction,
            window: count("WINDOW", 20, 1)?,
            epoch_every,
            export,
            cache_dir,
            report,
        })
    }

    /// The prefilled genesis the knobs describe.
    fn spec(&self) -> PrefillSpec {
        PrefillSpec {
            accounts: self.prefill,
            senders: self.senders,
            pools: self.pools,
            seed: self.seed,
            registry: self.registry,
        }
    }

    /// How each output is shaped.
    fn output_config(&self) -> OutputConfig {
        OutputConfig { batches: self.batches, batch_gas: self.gas, ..OutputConfig::default() }
    }

    /// How the aged base draws its transactions.
    fn aging(&self) -> Aging {
        Aging {
            outputs: self.aging,
            output_config: self.output_config(),
            workload: WorkloadConfig { seed: self.seed, ..WorkloadConfig::default() },
        }
    }

    /// How the warm-up and timed outputs draw their transactions.
    fn run_workload(&self) -> WorkloadConfig {
        WorkloadConfig { seed: self.seed.wrapping_add(1), ..WorkloadConfig::default() }
    }

    /// The page size in bytes of the bases and the run's database: `PAGE_SIZE`, or else the
    /// backend's default ([`default_page_size`], the one the database opener uses).
    fn page_size(&self) -> u32 {
        self.page_size.unwrap_or_else(default_page_size)
    }

    /// `PAGE_SIZE` as the knob line and the report show it: the bytes, or `default`.
    fn page_size_label(&self) -> String {
        self.page_size.map_or_else(|| "default".to_owned(), |bytes| bytes.to_string())
    }

    /// The pause between paced output starts, or `None` back to back.
    fn pace(&self) -> Option<Duration> {
        (self.pace_ms > 0).then(|| Duration::from_millis(self.pace_ms))
    }

    /// The pacing's report label.
    fn pacing(&self) -> String {
        match self.pace() {
            Some(pace) => format!("paced every {} ms", pace.as_millis()),
            None => "back to back".to_owned(),
        }
    }

    /// The knobs as one line of `name=value` words.
    fn line(&self) -> String {
        format!(
            "outputs={} batches={} gas={} warmup={} aging={} prefill={} senders={} pools={} \
             seed={} registry={} pace_ms={} metrics={} sync={} page_size={} readers={} \
             submit_fraction={} window={} epoch_every={} export={} cache_dir={} report={}",
            self.outputs,
            self.batches,
            self.gas,
            self.warmup,
            self.aging,
            self.prefill,
            self.senders,
            self.pools,
            self.seed,
            u8::from(self.registry),
            self.pace_ms,
            self.metrics.label(),
            self.sync.label(),
            self.page_size_label(),
            self.readers,
            self.submit_fraction,
            self.window,
            self.epoch_every,
            u8::from(self.export),
            self.cache_dir.display(),
            self.report
                .as_ref()
                .map_or_else(|| "none".to_owned(), |path| path.display().to_string())
        )
    }
}

/// `<workspace>/target/exec-bench-cache`.
fn default_cache_dir() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest.ancestors().nth(2).unwrap_or(manifest).join("target").join("exec-bench-cache")
}

// ------------------------------------------------------------------------------------------------
// The aged base
// ------------------------------------------------------------------------------------------------

/// The version of the aged base's layout. Bump it when an aged base built by earlier code no
/// longer matches what this code would build from the same base and [`Aging`].
const AGED_VERSION: u32 = 1;

/// The aged base's meta file inside its datadir.
const AGED_META_FILE: &str = "bench-aged.json";

/// What shapes an aged base's state beyond its base: `outputs` outputs of `output_config` drawn
/// by `workload`.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Aging {
    outputs: usize,
    output_config: OutputConfig,
    workload: WorkloadConfig,
}

impl Aging {
    /// The `extra` component of the aged base's dir name ([`base_dir_name`]):
    /// `aged<outputs>-<key>`, with `<key>` the first 8 bytes of `keccak256` over the JSON of
    /// [`AGED_VERSION`], `outputs` and the `Debug` text of the output config and the workload,
    /// in hex. The base's own name before it keys the spec and the backend.
    fn extra(&self) -> eyre::Result<String> {
        let keyed = (AGED_VERSION, self.outputs, self.output_text(), self.workload_text());
        let hash = keccak256(serde_json::to_vec(&keyed)?);
        let key: String = hash[..8].iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(format!("aged{}-{key}", self.outputs))
    }

    /// The output config as the meta records it.
    fn output_text(&self) -> String {
        format!("{:?}", self.output_config)
    }

    /// The workload as the meta records it.
    fn workload_text(&self) -> String {
        format!("{:?}", self.workload)
    }

    /// Refuse `meta`, read from the aged base `dir`, unless it describes this aging of the base
    /// `base`.
    fn check(&self, meta: &AgedMeta, base: &BaseMeta, dir: &Path) -> eyre::Result<()> {
        let refuse = |what: String| {
            eyre::eyre!(
                "the cached aged base {} does not match: {what}. Remove the dir to rebuild it",
                dir.display()
            )
        };
        if meta.aged_version != AGED_VERSION {
            return Err(refuse(format!(
                "aged version {} (this code: {AGED_VERSION})",
                meta.aged_version
            )));
        }
        if meta.base != *base {
            return Err(refuse(format!(
                "it was aged from another base build (built at {}, the base's meta says {})",
                meta.base.built_at, base.built_at
            )));
        }
        if meta.aging != self.outputs {
            return Err(refuse(format!("aging {} (requested: {})", meta.aging, self.outputs)));
        }
        if meta.output_config != self.output_text() {
            return Err(refuse(format!(
                "output config {} (requested: {})",
                meta.output_config,
                self.output_text()
            )));
        }
        if meta.workload != self.workload_text() {
            return Err(refuse(format!(
                "workload {} (requested: {})",
                meta.workload,
                self.workload_text()
            )));
        }
        let blocks = (self.outputs * self.output_config.batches) as u64;
        if meta.tip_number != base.genesis_header.number + blocks {
            return Err(refuse(format!(
                "tip block {} after {blocks} blocks on genesis {}",
                meta.tip_number, base.genesis_header.number
            )));
        }
        Ok(())
    }
}

/// What a cached aged base holds, written as [`AGED_META_FILE`] in its datadir.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct AgedMeta {
    /// [`AGED_VERSION`] of the code that built the aged base.
    aged_version: u32,
    /// The meta of the base it was aged from.
    base: BaseMeta,
    /// The outputs executed into it.
    aging: usize,
    /// The `Debug` text of the outputs' [`OutputConfig`].
    output_config: String,
    /// The `Debug` text of the aging's [`WorkloadConfig`].
    workload: String,
    /// The canonical tip after the aging.
    tip_number: u64,
    /// The hash of the canonical tip after the aging.
    tip_hash: B256,
    /// When the build finished, in seconds since the Unix epoch.
    built_at: u64,
    /// The whole build, in milliseconds.
    build_ms: u64,
    /// The length of the database data file ([`db_file_bytes`]).
    db_len: u64,
    /// The allocated bytes of the database data file.
    db_allocated: u64,
    /// The total length of the files under `static_files` ([`dir_bytes`]).
    static_files_len: u64,
    /// The allocated bytes of the files under `static_files`.
    static_files_allocated: u64,
}

/// A closed datadir a run clones: the base or an aged base, with the tip it holds.
struct Source {
    dir: PathBuf,
    tip_number: u64,
    tip_hash: B256,
    /// Whether this run built it.
    built: bool,
    /// How long building it took, in milliseconds.
    build_ms: u64,
}

/// Refuse to clone `src` into `dst_dir` across file systems: [`clone_datadir`] shares blocks only
/// within one, and would otherwise copy every byte of the datadir.
fn ensure_same_file_system(src: &Path, dst_dir: &Path) -> eyre::Result<()> {
    use std::os::unix::fs::MetadataExt as _;
    let (src_dev, dst_dev) = (std::fs::metadata(src)?.dev(), std::fs::metadata(dst_dir)?.dev());
    eyre::ensure!(
        src_dev == dst_dev,
        "{} and {} are on different file systems, so the clone would copy the whole datadir: put \
         {KNOB_PREFIX}CACHE_DIR on TMPDIR's file system",
        src.display(),
        dst_dir.display()
    );
    Ok(())
}

/// Write `value` as `dir`'s file `name`: a temp file, synced, renamed into place, and the dir
/// synced.
fn write_json(dir: &Path, name: &str, value: &impl Serialize) -> eyre::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    fsync_file(&file)?;
    drop(file);
    std::fs::rename(&tmp, dir.join(name))?;
    fsync_directory(dir)?;
    Ok(())
}

/// The aged base of `aging` from the base `base_dir` (meta `base`) in `cache_dir`, built first
/// when absent (see the module docs). An existing aged base is returned when its
/// [`AGED_META_FILE`] matches ([`Aging::check`]); otherwise this is an error naming the mismatch,
/// and the dir is left as it is.
async fn aged_base(
    cache_dir: &Path,
    base_dir: &Path,
    base: &BaseMeta,
    aging: &Aging,
    recorder: &CaptureRecorder,
) -> eyre::Result<Source> {
    let name = base_dir_name(&base.spec, base.page_size, &aging.extra()?)?;
    let dir = cache_dir.join(&name);
    if dir.symlink_metadata().is_ok() {
        let meta_path = dir.join(AGED_META_FILE);
        let json = std::fs::read(&meta_path).map_err(|e| {
            eyre::eyre!(
                "the cached aged base {} has no readable {AGED_META_FILE} ({e}). Remove the dir \
                 to rebuild it",
                dir.display()
            )
        })?;
        let meta: AgedMeta = serde_json::from_slice(&json).map_err(|e| {
            eyre::eyre!("the cached aged base meta {} does not parse: {e}", meta_path.display())
        })?;
        aging.check(&meta, base, &dir)?;
        return Ok(Source {
            dir,
            tip_number: meta.tip_number,
            tip_hash: meta.tip_hash,
            built: false,
            build_ms: meta.build_ms,
        });
    }

    let building = cache_dir.join(format!("{name}.building"));
    if building.symlink_metadata().is_ok() {
        std::fs::remove_dir_all(&building)?;
    }
    let start = Instant::now();
    clone_datadir(base_dir, &building)?;
    // the copied base meta would make the aged dir look like a base; the aged meta embeds it
    std::fs::remove_file(building.join(META_FILE))?;
    let tip = {
        let page_text = base.page_size.to_string();
        let flags = db_flags(SyncMode::Durable, &page_text);
        let node = BenchNode::open_at(&building, slim_chain(base), &flags, None)?;
        let mut factory = node.factory(base.spec, aging.workload, aging.output_config)?;
        let updates = EngineUpdates::spawn();
        let mut left = aging.outputs;
        while left > 0 {
            let chunk = left.min(AGING_CHUNK);
            let outputs = factory.generate(chunk);
            run_outputs(&node, &updates, recorder, outputs, &mut Timed::default()).await?;
            left -= chunk;
        }
        let sent = updates.close().await?;
        eyre::ensure!(sent == aging.outputs, "{sent} engine updates for {} outputs", aging.outputs);
        node.env.canonical_tip()
        // the env, then its tasks, drop here: the database closes before it is measured or moved
    };
    let blocks = (aging.outputs * aging.output_config.batches) as u64;
    eyre::ensure!(
        tip.number == base.genesis_header.number + blocks,
        "the aged base's tip is block {}, expected {blocks} blocks on genesis",
        tip.number
    );

    let (db_len, db_allocated) = db_file_bytes(&building.join("db"))?;
    let (static_files_len, static_files_allocated) = dir_bytes(&building.join("static_files"))?;
    let meta = AgedMeta {
        aged_version: AGED_VERSION,
        base: base.clone(),
        aging: aging.outputs,
        output_config: aging.output_text(),
        workload: aging.workload_text(),
        tip_number: tip.number,
        tip_hash: tip.hash(),
        built_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        build_ms: start.elapsed().as_millis() as u64,
        db_len,
        db_allocated,
        static_files_len,
        static_files_allocated,
    };
    write_json(&building, AGED_META_FILE, &meta)?;
    std::fs::rename(&building, &dir)?;
    fsync_directory(cache_dir)?;
    Ok(Source {
        dir,
        tip_number: tip.number,
        tip_hash: tip.hash(),
        built: true,
        build_ms: meta.build_ms,
    })
}

// ------------------------------------------------------------------------------------------------
// Executing outputs and capturing their stages
// ------------------------------------------------------------------------------------------------

/// How many samples a captured histogram records per output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Per {
    /// One per block: `build_block_from_batch_payload`'s stages.
    Block,
    /// One per output: `persist_executed_output`'s `save_blocks` and commit.
    Output,
}

/// The captured histograms, by raw name (before tn-metrics' prefix), and their samples per
/// output. Every value is in seconds.
const CAPTURED: [(&str, Per); 16] = [
    (EXECUTE, Per::Block),
    ("tn_reth.block_signer_recovery_seconds", Per::Block),
    (FINISH, Per::Block),
    ("tn_reth.block_hashed_state_seconds", Per::Block),
    ("tn_reth.block_state_root_seconds", Per::Block),
    (OVERLAY_EXTEND, Per::Block),
    (SAVE_BLOCKS, Per::Output),
    ("storage.providers.database.save_blocks_mdbx", Per::Output),
    ("storage.providers.database.save_blocks_sf", Per::Output),
    ("storage.providers.database.save_blocks_insert_block", Per::Output),
    ("storage.providers.database.save_blocks_write_state", Per::Output),
    ("storage.providers.database.save_blocks_write_hashed_state", Per::Output),
    ("storage.providers.database.save_blocks_write_trie_updates", Per::Output),
    ("storage.providers.database.save_blocks_update_history_indices", Per::Output),
    (COMMIT_SF, Per::Output),
    (COMMIT_MDBX, Per::Output),
];

/// A block's EVM execution, signer recovery included.
const EXECUTE: &str = "tn_reth.block_execute_seconds";
/// A block's `builder.finish`, hashed state and state root included.
const FINISH: &str = "tn_reth.block_finish_seconds";
/// Extending the output's trie overlay with a block.
const OVERLAY_EXTEND: &str = "tn_reth.block_overlay_extend_seconds";
/// reth's `save_blocks`, its commit excluded.
const SAVE_BLOCKS: &str = "storage.providers.database.save_blocks_total";
/// The static files' commit after `save_blocks`.
const COMMIT_SF: &str = "storage.providers.database.save_blocks_commit_sf";
/// The database commit after the static files'.
const COMMIT_MDBX: &str = "storage.providers.database.save_blocks_commit_mdbx";

/// The captured stages that run one after another within `total`; `other` is what they leave.
const SEQUENTIAL: [&str; 6] =
    [EXECUTE, FINISH, OVERLAY_EXTEND, SAVE_BLOCKS, COMMIT_SF, COMMIT_MDBX];

/// One executed output: its wall time, each captured stage's samples summed, and what it
/// executed.
#[derive(Clone, Debug, Default, PartialEq)]
struct OutputRow {
    /// The output's first block.
    first_block: u64,
    /// The output's last block.
    last_block: u64,
    /// The wall time of `execute_consensus_output`, in seconds.
    total: f64,
    /// Each [`CAPTURED`] histogram's samples for this output, summed, in seconds, in its order.
    stages: [f64; CAPTURED.len()],
    /// The gas the output's blocks used.
    gas: u64,
    /// The transactions in the output's blocks.
    txs: usize,
    /// Whether a paced output started after its slot.
    late: bool,
    /// Whether the output ran in a window with the side load on ([`side_load_on`]).
    side_load: bool,
    /// Whether the output closed its epoch.
    epoch_close: bool,
    /// Whether the export ran at any point during the output ([`Export::active_during`]).
    export_active: bool,
    /// What the database's own counters moved by over the output's call ([`DbCounters::since`]
    /// of [`db_counters`] read just before and just after it), named, in
    /// [`DbCounters::entries`]'s order.
    ///
    /// [`DbCounters::since`]: tn_reth::test_utils::load::DbCounters::since
    /// [`DbCounters::entries`]: tn_reth::test_utils::load::DbCounters::entries
    db_counters: Vec<(&'static str, u64)>,
    /// The bytes the process wrote to disk over the output's call ([`disk_bytes_written`] just
    /// before and just after it), or `None` when a read failed or the target has no source.
    disk_bytes: Option<u64>,
    /// The database data file's length after the output, in bytes ([`db_file_bytes`]).
    db_file_len: u64,
}

impl OutputRow {
    /// The summed samples of the captured histogram `name`.
    fn stage(&self, name: &str) -> f64 {
        let index = CAPTURED
            .iter()
            .position(|(captured, _)| *captured == name)
            .unwrap_or_else(|| panic!("{name} is not captured"));
        self.stages[index]
    }

    /// `total` minus the [`SEQUENTIAL`] stages.
    fn other(&self) -> f64 {
        SEQUENTIAL.iter().fold(self.total, |rest, name| rest - self.stage(name))
    }
}

/// Each [`CAPTURED`] histogram's samples for one output of `blocks` blocks, summed, taking them
/// with `drain`. Refuses a histogram whose sample count is not one per block or one per output
/// ([`Per`]).
fn stage_sums(
    mut drain: impl FnMut(&str) -> Vec<f64>,
    blocks: usize,
) -> eyre::Result<[f64; CAPTURED.len()]> {
    let mut sums = [0.0; CAPTURED.len()];
    for (sum, (name, per)) in sums.iter_mut().zip(CAPTURED) {
        let samples = drain(name);
        let expected = match per {
            Per::Block => blocks,
            Per::Output => 1,
        };
        let hint = if samples.is_empty() {
            " (a metric handle that registered before the capture recorder records nothing: run \
             exec_bench alone, with --exact)"
        } else {
            ""
        };
        eyre::ensure!(
            samples.len() == expected,
            "{name} recorded {} samples for an output of {blocks} blocks, expected {expected}{hint}",
            samples.len()
        );
        *sum = samples.iter().sum();
    }
    Ok(sums)
}

/// The engine-update channel each output's update goes to, drained by a task meanwhile, as the
/// node's consumer drains `ExecutorEngine`'s.
struct EngineUpdates {
    tx: mpsc::Sender<EngineUpdate>,
    drainer: JoinHandle<usize>,
}

impl EngineUpdates {
    /// Open the channel and start its drainer.
    fn spawn() -> Self {
        let (tx, mut rx) = mpsc::channel(ENGINE_UPDATE_SLOTS);
        let drainer = tokio::spawn(async move {
            let mut count = 0;
            while rx.recv().await.is_some() {
                count += 1;
            }
            count
        });
        Self { tx, drainer }
    }

    /// Close the channel and return how many updates the drainer took.
    async fn close(self) -> eyre::Result<usize> {
        drop(self.tx);
        Ok(self.drainer.await?)
    }
}

/// The table stats of the closed database in `db`. The pool's maintenance hands reloads to
/// `spawn_blocking`, and such a closure outlives its task by the moment it takes to finish,
/// holding the database meanwhile: a refused open is retried for a few seconds.
async fn closed_table_stats(db: &Path) -> eyre::Result<Vec<DbTableStat>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match db_table_stats(db) {
            Ok(stats) => return Ok(stats),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(e) => return Err(e),
        }
    }
}

/// Whether the timed output at `index` runs with the side load on: the outputs go in windows of
/// `window`, the first off, the next on, and so on.
fn side_load_on(index: usize, window: usize) -> bool {
    (index / window) % 2 == 1
}

/// The raw transactions of `load`'s batches, in execution order.
fn raw_txs(load: &LoadOutput) -> impl Iterator<Item = &Vec<u8>> {
    (0..load.batches.len()).flat_map(move |index| load.raw_txs(index))
}

/// The hashes of `load`'s transactions, in execution order: the keccak of each EIP-2718 encoding,
/// which is the transaction's hash.
fn tx_hashes(load: &LoadOutput) -> Vec<B256> {
    raw_txs(load).map(keccak256).collect()
}

/// The indexes `fraction` picks of `count` items, spread evenly: item `i` when `(i + 1) *
/// fraction` reaches a whole number that `i * fraction` has not, `floor(count * fraction)` in all.
fn submitted(count: usize, fraction: f64) -> impl Iterator<Item = usize> {
    (0..count).filter(move |&i| ((i + 1) as f64 * fraction).floor() > (i as f64 * fraction).floor())
}

/// What the side load recorded over the timed loop.
#[derive(Clone, Debug, Default)]
struct SideSamples {
    /// Each reader kind's calls in the on windows, in [`ReadKind::ALL`] order.
    kinds: [KindSamples; 5],
    /// The balance poller's latencies in the on windows.
    polls: Vec<Duration>,
    /// On windows whose last poll had no balance, or a zero one, for a polled address: the pool
    /// degrades a failed state read to zero.
    degraded_polls: u64,
    /// Reader or poller calls recorded in an off window, which the gate should have parked.
    off_calls: u64,
    /// The on windows' summed wall time.
    on_time: Duration,
    /// The submitter's calls.
    submits: SubmitSamples,
}

impl SideSamples {
    /// Attribute one window's samples, `kinds` in [`ReadKind::ALL`] order and the poller's
    /// `polls`, to the window that just ended: `on` or not, open for `elapsed`, polling `polled`.
    fn add_window(
        &mut self,
        on: bool,
        elapsed: Duration,
        kinds: [&KindSamples; 5],
        polls: &PollSamples,
        polled: &[Address],
    ) {
        if !on {
            let calls: u64 = kinds.iter().map(|kind| kind.ok + kind.empty + kind.errors).sum();
            self.off_calls += calls + polls.latencies.len() as u64;
            return;
        }
        self.on_time += elapsed;
        for (total, window) in self.kinds.iter_mut().zip(kinds) {
            total.latencies.extend_from_slice(&window.latencies);
            total.ok += window.ok;
            total.empty += window.empty;
            total.errors += window.errors;
            if total.first_error.is_none() {
                total.first_error.clone_from(&window.first_error);
            }
        }
        self.polls.extend_from_slice(&polls.latencies);
        let degraded = |address: &Address| polls.last.get(address).is_none_or(|b| b.is_zero());
        if !polls.latencies.is_empty() && polled.iter().any(degraded) {
            self.degraded_polls += 1;
        }
    }

    /// Attribute a drain of the readers to the window that just ended (see [`Self::add_window`]).
    fn add(&mut self, window: &ReadSamples, on: bool, elapsed: Duration, polled: &[Address]) {
        let kinds = ReadKind::ALL.map(|kind| window.kind(kind));
        self.add_window(on, elapsed, kinds, &window.polls, polled);
    }

    /// The reader errors, the degraded polls, the calls in off windows and the submitter's
    /// errors: any makes the bench fail.
    fn errors(&self) -> u64 {
        self.kinds.iter().map(|kind| kind.errors).sum::<u64>()
            + self.degraded_polls
            + self.off_calls
            + self.submits.errors
    }
}

/// The timed loop's side load (see the module docs): the readers and the balance poller behind
/// one gate, the submitter, and the samples so far.
struct SideLoad {
    readers: Readers,
    submitter: Submitter,
    submit_fraction: f64,
    window: usize,
    /// The addresses the poller polls.
    polled: Vec<Address>,
    samples: SideSamples,
    /// When the current window opened, and whether it is on.
    current: Option<(Instant, bool)>,
}

impl SideLoad {
    /// Start `knobs.readers` readers and the poller (gate off) and the submitter over `methods`
    /// and `pool`, drawing accounts of `spec`.
    fn start(
        methods: Methods,
        pool: WorkerTxPool,
        spec: PrefillSpec,
        knobs: &Knobs,
    ) -> eyre::Result<Self> {
        let mut readers =
            Readers::start(methods.clone(), spec, knobs.readers, knobs.seed.wrapping_add(2))?;
        let polled = poll_addresses(&spec, knobs.seed)?;
        readers.poll_balances(pool, polled.clone(), POLL_INTERVAL)?;
        Ok(Self {
            readers,
            submitter: Submitter::start(methods)?,
            submit_fraction: knobs.submit_fraction,
            window: knobs.window,
            polled,
            samples: SideSamples::default(),
            current: None,
        })
    }

    /// End the current window, attributing what the readers recorded to it, and open the next,
    /// `on` or not. Closing the gate returns once the calls in flight are recorded, so the drain
    /// holds the ended window's samples; an opening gate drains first.
    fn switch(&mut self, on: bool) {
        tokio::task::block_in_place(|| {
            if !on {
                self.readers.set_on(false);
            }
            let window = self.readers.drain();
            if let Some((opened, was_on)) = self.current {
                self.samples.add(&window, was_on, opened.elapsed(), &self.polled);
            }
            if on {
                self.readers.set_on(true);
            }
        });
        self.current = Some((Instant::now(), on));
    }

    /// Before the timed output at `index`, `load`: switch at a window's start, and in an on
    /// window queue `submit_fraction` of its raw transactions for `eth_sendRawTransaction`, as
    /// user transactions arriving over RPC. Returns whether the side load is on.
    fn before(&mut self, index: usize, load: &LoadOutput) -> eyre::Result<bool> {
        let on = side_load_on(index, self.window);
        if index.is_multiple_of(self.window) {
            self.switch(on);
        }
        if on {
            let raw: Vec<&Vec<u8>> = raw_txs(load).collect();
            let txs = submitted(raw.len(), self.submit_fraction).map(|i| raw[i].clone()).collect();
            self.submitter.submit(txs)?;
        }
        Ok(on)
    }

    /// Close the last window, stop and join every thread, and return what they recorded.
    fn finish(mut self) -> eyre::Result<SideSamples> {
        self.switch(false);
        let (readers, submitter) = (self.readers, self.submitter);
        let (rest, submits) =
            tokio::task::block_in_place(|| eyre::Ok((readers.stop()?, submitter.stop()?)))?;
        self.samples.add(&rest, false, Duration::ZERO, &self.polled);
        self.samples.submits = submits;
        Ok(self.samples)
    }
}

/// The snapshot export's long read beside the timed loop, with `EXPORT=1` (see the module docs):
/// the state pinned at the tip and written to an exec state pack on a side thread.
struct Export {
    /// When the view was pinned.
    started: Instant,
    /// When the export ended, the pack finished or failed, and its view dropped.
    ended: Arc<OnceLock<Instant>>,
    thread: std::thread::JoinHandle<eyre::Result<ExportRun>>,
}

impl Export {
    /// Pin `env`'s state at its canonical tip, checked as the node checks its pin, take the
    /// sizes in `datadir`, and start the thread that writes the pack into `out_dir`.
    fn start(env: &RethEnv, datadir: &Path, out_dir: PathBuf) -> eyre::Result<Self> {
        let started = Instant::now();
        let view = env.pin_state_view()?;
        let tip = env.canonical_tip();
        eyre::ensure!(
            view.verify_tip(tip.num_hash())?,
            "the pinned view is not at the canonical tip, block {} {}",
            tip.number,
            tip.hash()
        );
        let at_pin = Sizes::of(datadir)?;
        let ended = Arc::new(OnceLock::new());
        let (env, datadir, end) = (env.clone(), datadir.to_owned(), Arc::clone(&ended));
        let thread =
            std::thread::Builder::new().name("exec-bench-export".to_owned()).spawn(move || {
                let run = write_pack(&env, view, &tip, &datadir, &out_dir, started, at_pin);
                // the view dropped in `write_pack`
                let _ = end.set(Instant::now());
                run
            })?;
        Ok(Self { started, ended, thread })
    }

    /// Whether the export ran at any point from `start` to `end`.
    fn active_during(&self, start: Instant, end: Instant) -> bool {
        overlaps((start, end), self.started, self.ended.get().copied())
    }

    /// Wait for the export to end and return what it wrote.
    fn finish(self) -> eyre::Result<ExportRun> {
        tokio::task::block_in_place(|| self.thread.join())
            .map_err(|_| eyre::eyre!("the export thread panicked"))?
    }
}

/// Whether a run from `run_start` to `run_end` (`None` while it runs) overlaps the span from
/// `start` to `end`.
fn overlaps(
    (start, end): (Instant, Instant),
    run_start: Instant,
    run_end: Option<Instant>,
) -> bool {
    run_start < end && run_end.is_none_or(|run_end| run_end > start)
}

/// Write the state `view` pins at `tip` to an exec state pack in `out_dir` with the arguments the
/// node's snapshot export passes: the tip's state root and [`export_headers`]. The sizes in
/// `datadir` are taken when the pack has finished, before the view drops; `started` is when it
/// was pinned, and `at_pin` the sizes then.
fn write_pack(
    env: &RethEnv,
    view: PinnedStateView,
    tip: &SealedHeader,
    datadir: &Path,
    out_dir: &Path,
    started: Instant,
    at_pin: Sizes,
) -> eyre::Result<ExportRun> {
    let headers = export_headers(env, tip)?;
    let stats = view
        .export_state_pack(tip.state_root, &headers, out_dir)
        .map_err(|e| eyre::eyre!("state export failed: {e}"))?;
    let duration = started.elapsed();
    let at_finish = Sizes::of(datadir)?;
    drop(view);
    let (pack_len_bytes, pack_allocated_bytes) = dir_bytes(out_dir)?;
    Ok(ExportRun {
        block: tip.number,
        duration,
        stats,
        pack_len_bytes,
        pack_allocated_bytes,
        at_pin,
        at_finish,
    })
}

/// The headers the node's snapshot export embeds for `anchor` (`gather_headers` in tn-node's
/// `export_exec`): the anchor, then its ancestors newest first, at most [`BLOCKHASH_ANCESTORS`]
/// and none below block 1.
fn export_headers(env: &RethEnv, anchor: &SealedHeader) -> eyre::Result<Vec<ExecHeader>> {
    let lowest = anchor.number.saturating_sub(BLOCKHASH_ANCESTORS).max(1);
    let mut headers = Vec::with_capacity(BLOCKHASH_ANCESTORS as usize + 1);
    headers.push(anchor.header().clone());
    for number in (lowest..anchor.number).rev() {
        let header = env
            .sealed_header_by_number(number)?
            .ok_or_else(|| eyre::eyre!("no header at block {number}"))?;
        headers.push(header.header().clone());
    }
    Ok(headers)
}

/// What the export wrote, and the clone's sizes around it.
#[derive(Clone, Debug, PartialEq)]
struct ExportRun {
    /// The pinned tip's block number.
    block: u64,
    /// From the pin to the pack's finish.
    duration: Duration,
    stats: ExecStateStats,
    /// The pack dir's length and allocated bytes.
    pack_len_bytes: u64,
    pack_allocated_bytes: u64,
    /// The clone's sizes at the pin.
    at_pin: Sizes,
    /// The clone's sizes when the pack finished, the view still pinned.
    at_finish: Sizes,
}

impl ExportRun {
    /// The database data file's allocated bytes and length added while the view was pinned.
    fn db_growth(&self) -> (i64, i64) {
        let grew = |end: u64, start: u64| end as i64 - start as i64;
        (
            grew(self.at_finish.db_allocated_bytes, self.at_pin.db_allocated_bytes),
            grew(self.at_finish.db_len_bytes, self.at_pin.db_len_bytes),
        )
    }
}

/// The timed loop's segments for `outputs` timed outputs that close an epoch every `epoch_every`
/// (0 never): each segment's output count and whether its last output closes the epoch. No output
/// can be built after a close until the close has executed, so each segment is built once the one
/// before it has run.
fn segments(outputs: usize, epoch_every: usize) -> Vec<(usize, bool)> {
    if epoch_every == 0 {
        return vec![(outputs, false)];
    }
    let mut plan = vec![(epoch_every, true); outputs / epoch_every];
    if !outputs.is_multiple_of(epoch_every) {
        plan.push((outputs % epoch_every, false));
    }
    plan
}

/// Where [`disk_bytes_written`] reads on this target, as the report names it; `None` on a target
/// without a source.
const DISK_BYTES_SOURCE: Option<&str> = if cfg!(target_os = "macos") {
    Some("proc_pid_rusage")
} else if cfg!(target_os = "linux") {
    Some("/proc/self/io")
} else {
    None
};

/// The bytes this process has written to disk since it started, from [`DISK_BYTES_SOURCE`]:
/// `proc_pid_rusage`'s `ri_diskio_byteswritten` on macOS. Only a difference of two reads means
/// anything, and only approximately (the module docs' caveats). The error says why the read
/// failed.
#[cfg(target_os = "macos")]
fn disk_bytes_written() -> Result<u64, String> {
    let mut info = std::mem::MaybeUninit::<libc::rusage_info_v2>::zeroed();
    // SAFETY: `info` is a writable `rusage_info_v2`, the struct the `RUSAGE_INFO_V2` flavor fills,
    // and it outlives the call; `proc_pid_rusage` takes it as a `rusage_info_t *`, so the pointer
    // is cast to that. `getpid` has no preconditions.
    let status = unsafe {
        libc::proc_pid_rusage(libc::getpid(), libc::RUSAGE_INFO_V2, info.as_mut_ptr().cast())
    };
    if status != 0 {
        return Err(format!("proc_pid_rusage failed: {}", std::io::Error::last_os_error()));
    }
    // SAFETY: every field of `rusage_info_v2` is an integer or an array of them, so the zeroed
    // buffer was already a valid value, and the call succeeded in filling it.
    Ok(unsafe { info.assume_init() }.ri_diskio_byteswritten)
}

/// The bytes this process has written to disk since it started, from [`DISK_BYTES_SOURCE`]:
/// `/proc/self/io`'s `write_bytes` on Linux. Only a difference of two reads means anything, and
/// only approximately (the module docs' caveats). The error says why the read failed.
#[cfg(target_os = "linux")]
fn disk_bytes_written() -> Result<u64, String> {
    let io = std::fs::read_to_string("/proc/self/io")
        .map_err(|e| format!("reading /proc/self/io failed: {e}"))?;
    proc_io_write_bytes(&io)
}

/// No disk-bytes source on this target ([`DISK_BYTES_SOURCE`] is `None`): always an error.
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn disk_bytes_written() -> Result<u64, String> {
    Err("this target has no disk-bytes source".to_owned())
}

/// The `write_bytes:` field of a `/proc/<pid>/io` text.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn proc_io_write_bytes(io: &str) -> Result<u64, String> {
    let value = io
        .lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .ok_or_else(|| "/proc/self/io has no write_bytes line".to_owned())?;
    value.trim().parse().map_err(|e| format!("/proc/self/io write_bytes {value:?}: {e}"))
}

/// What the timed loop carries across its segments' [`run_outputs`] calls: the next output's
/// index, the pacing, the side load, the export and the first disk-bytes read failure. The warm-up
/// runs with the default: no pacing, side load or export.
#[derive(Default)]
struct Timed<'a> {
    pace: Option<Duration>,
    side: Option<&'a mut SideLoad>,
    export: Option<Export>,
    /// The next timed output's index.
    next: usize,
    /// When the first output started, moved on by the time between segments: output `i`'s slot
    /// is `t0 + i * pace`.
    t0: Option<Instant>,
    /// When the last segment's last output ended.
    paused: Option<Instant>,
    /// Why a [`disk_bytes_written`] read failed, the first time one did.
    disk_error: Option<String>,
}

impl Timed<'_> {
    /// [`disk_bytes_written`] now, or `None` when the read fails; the first failure's reason is
    /// kept for the report's header.
    fn disk_bytes(&mut self) -> Option<u64> {
        disk_bytes_written()
            .map_err(|e| {
                self.disk_error.get_or_insert(e);
            })
            .ok()
    }

    /// Start a segment: move `t0` on by the time since the last segment ended, so that neither the
    /// pacing nor the wall clock counts the time between segments (building the next, entering an
    /// epoch).
    fn resume(&mut self) {
        if let (Some(t0), Some(paused)) = (self.t0.as_mut(), self.paused.take()) {
            *t0 += paused.elapsed();
        }
    }

    /// The loop's wall clock: from the first output's start to the last's end, without the time
    /// between segments.
    fn elapsed(&self) -> Duration {
        match (self.t0, self.paused) {
            (Some(t0), Some(paused)) => paused.saturating_duration_since(t0),
            _ => Duration::ZERO,
        }
    }
}

/// Execute `outputs` in order through `execute_consensus_output`, each on the canonical tip, and
/// return one row per output, its stages drained from `recorder`. `timed` numbers the outputs on
/// from its next index and keeps the loop's wall clock ([`Timed::elapsed`]).
///
/// With `timed.pace`, output `i` starts at `t0 + i * pace` (`t0` when the first output starts,
/// moved on by the time between calls, [`Timed::resume`]); an output whose slot has passed starts
/// at once and is marked late. After the loop, each output's blocks are read back: one per batch,
/// with every transaction and exactly the expected gas.
///
/// With `timed.side`, each output first goes through [`SideLoad::before`] (outside its wall time),
/// and its transaction hashes go to the readers' receipt ring once it has executed. With
/// `timed.export`, each row says whether the export ran during its output.
async fn run_outputs(
    node: &BenchNode,
    updates: &EngineUpdates,
    recorder: &CaptureRecorder,
    outputs: Vec<LoadOutput>,
    timed: &mut Timed<'_>,
) -> eyre::Result<Vec<OutputRow>> {
    let mut rows = Vec::with_capacity(outputs.len());
    let mut expected = Vec::with_capacity(outputs.len());
    let mut parent = node.env.canonical_tip().number;
    timed.resume();
    for load in outputs {
        let index = timed.next;
        timed.next += 1;
        let t0 = *timed.t0.get_or_insert_with(Instant::now);
        let mut late = false;
        if let Some(pace) = timed.pace {
            let slot = t0 + pace * u32::try_from(index)?;
            let now = Instant::now();
            if now < slot {
                tokio::time::sleep_until(slot.into()).await;
            } else {
                late = index > 0 && now > slot;
            }
        }
        let blocks = load.batches.len();
        let epoch_close = load.output.close_epoch();
        expected.push((load.expected_gas(), load.tx_count()));
        let side_load = match timed.side.as_deref_mut() {
            Some(side) => side.before(index, &load)?,
            None => false,
        };
        let hashes = timed.side.is_some().then(|| tx_hashes(&load));
        // the commit histograms also record the env's other commits (opening it, say): start each
        // output with no samples
        for (name, _) in CAPTURED {
            recorder.drain(name);
        }
        // the database's and the process's write counters around the call, outside its wall
        // time: the commit completes inside the call, so the difference is this output's
        let counters_before = db_counters(&node.db)?;
        let disk_before = timed.disk_bytes();
        let started = Instant::now();
        let (tip, wall) = node.execute_timed(load.output, &updates.tx).await?;
        let export_active = timed
            .export
            .as_ref()
            .is_some_and(|export| export.active_during(started, Instant::now()));
        let disk_after = timed.disk_bytes();
        let counters = db_counters(&node.db)?.since(&counters_before);
        let (db_file_len, _) = db_file_bytes(&node.db_dir)?;
        let stages = stage_sums(|name| recorder.drain(name), blocks)?;
        if let (Some(side), Some(hashes)) = (timed.side.as_deref_mut(), hashes) {
            side.readers.feed_tx_hashes(hashes);
        }
        eyre::ensure!(
            tip.number == parent + blocks as u64,
            "an output of {blocks} batches moved the tip from block {parent} to {}",
            tip.number
        );
        rows.push(OutputRow {
            first_block: parent + 1,
            last_block: tip.number,
            total: wall.as_secs_f64(),
            stages,
            late,
            side_load,
            epoch_close,
            export_active,
            db_counters: counters.entries(),
            disk_bytes: disk_before
                .zip(disk_after)
                .map(|(before, after)| after.saturating_sub(before)),
            db_file_len,
            ..OutputRow::default()
        });
        parent = tip.number;
    }
    timed.paused = Some(Instant::now());
    for (row, (expected_gas, expected_txs)) in rows.iter_mut().zip(expected) {
        for number in row.first_block..=row.last_block {
            let block = node
                .env
                .sealed_block_by_number(number)?
                .ok_or_else(|| eyre::eyre!("block {number} was not persisted"))?;
            row.gas += block.gas_used;
            row.txs += block.body().transactions.len();
        }
        eyre::ensure!(
            (row.gas, row.txs) == (expected_gas, expected_txs),
            "blocks {}..={} used {} gas in {} txs, the output expected {expected_gas} in \
             {expected_txs}",
            row.first_block,
            row.last_block,
            row.gas,
            row.txs
        );
    }
    Ok(rows)
}

// ------------------------------------------------------------------------------------------------
// The report
// ------------------------------------------------------------------------------------------------

/// A sample's mean, nearest-rank percentiles and max.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Stats {
    mean: f64,
    p50: f64,
    p90: f64,
    p99: f64,
    max: f64,
}

impl Stats {
    /// The stats of `values`, which must not be empty.
    fn of(values: &[f64]) -> Self {
        assert!(!values.is_empty(), "stats of no values");
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let rank = |p: f64| {
            let index = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
            sorted[index]
        };
        Self {
            mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
            p50: rank(0.50),
            p90: rank(0.90),
            p99: rank(0.99),
            max: sorted[sorted.len() - 1],
        }
    }
}

/// A report row's stage.
#[derive(Clone, Copy, Debug)]
enum Stage {
    /// The whole `execute_consensus_output` call.
    Total,
    /// A captured histogram.
    Captured(&'static str),
    /// What the sequential stages leave of the total ([`OutputRow::other`]).
    Other,
}

/// The report's stage rows, indented under the stage that contains them, each with its JSON
/// report key.
const STAGES: [(&str, &str, Stage); 18] = [
    ("total", "total", Stage::Total),
    ("  execute", "execute", Stage::Captured(EXECUTE)),
    (
        "    signer recovery",
        "signer_recovery",
        Stage::Captured("tn_reth.block_signer_recovery_seconds"),
    ),
    ("  finish", "finish", Stage::Captured(FINISH)),
    ("    hashed state", "hashed_state", Stage::Captured("tn_reth.block_hashed_state_seconds")),
    ("    state root", "state_root", Stage::Captured("tn_reth.block_state_root_seconds")),
    ("  overlay extend", "overlay_extend", Stage::Captured(OVERLAY_EXTEND)),
    ("  save_blocks", "save_blocks", Stage::Captured(SAVE_BLOCKS)),
    (
        "    mdbx",
        "save_blocks_mdbx",
        Stage::Captured("storage.providers.database.save_blocks_mdbx"),
    ),
    (
        "      insert_block",
        "insert_block",
        Stage::Captured("storage.providers.database.save_blocks_insert_block"),
    ),
    (
        "      write_state",
        "write_state",
        Stage::Captured("storage.providers.database.save_blocks_write_state"),
    ),
    (
        "      write_hashed_state",
        "write_hashed_state",
        Stage::Captured("storage.providers.database.save_blocks_write_hashed_state"),
    ),
    (
        "      write_trie_updates",
        "write_trie_updates",
        Stage::Captured("storage.providers.database.save_blocks_write_trie_updates"),
    ),
    (
        "      update_history_idx",
        "update_history_indices",
        Stage::Captured("storage.providers.database.save_blocks_update_history_indices"),
    ),
    ("    sf", "save_blocks_sf", Stage::Captured("storage.providers.database.save_blocks_sf")),
    ("  commit sf", "commit_sf", Stage::Captured(COMMIT_SF)),
    ("  commit mdbx", "commit_mdbx", Stage::Captured(COMMIT_MDBX)),
    ("  other", "other", Stage::Other),
];

impl Stage {
    /// This stage's seconds in `row`.
    fn of(self, row: &OutputRow) -> f64 {
        match self {
            Self::Total => row.total,
            Self::Captured(name) => row.stage(name),
            Self::Other => row.other(),
        }
    }
}

/// The timed loop's throughput: gas and transactions over the summed output time.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Throughput {
    /// The summed `execute_consensus_output` time, in seconds.
    seconds: f64,
    gas: u64,
    txs: usize,
    blocks: u64,
    /// Total gas over `seconds`, in millions per second.
    mgas_per_s: f64,
    /// Total transactions over `seconds`.
    tx_per_s: f64,
    /// The median of each output's gas over its own time, in millions per second.
    p50_output_mgas_per_s: f64,
}

impl Throughput {
    /// The throughput of `rows`, which must not be empty.
    fn of(rows: &[OutputRow]) -> Self {
        let seconds: f64 = rows.iter().map(|row| row.total).sum();
        let gas: u64 = rows.iter().map(|row| row.gas).sum();
        let txs: usize = rows.iter().map(|row| row.txs).sum();
        let blocks = rows.iter().map(|row| row.last_block + 1 - row.first_block).sum();
        let per_output: Vec<f64> =
            rows.iter().map(|row| row.gas as f64 / row.total / 1e6).collect();
        Self {
            seconds,
            gas,
            txs,
            blocks,
            mgas_per_s: gas as f64 / seconds / 1e6,
            tx_per_s: txs as f64 / seconds,
            p50_output_mgas_per_s: Stats::of(&per_output).p50,
        }
    }
}

/// One stage's mean and p50 over the first and the last third of the timed outputs, in
/// milliseconds per output, and their ratios, last over first: how the stage changes as the
/// history grows.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
struct Thirds {
    first_mean_ms: f64,
    first_p50_ms: f64,
    last_mean_ms: f64,
    last_p50_ms: f64,
    /// `last_mean_ms` over `first_mean_ms`, or `None` when the first is not positive.
    mean_ratio: Option<f64>,
    /// `last_p50_ms` over `first_p50_ms`, or `None` when the first is not positive.
    p50_ratio: Option<f64>,
}

impl Thirds {
    /// The thirds of `values_ms`, in output order. The first and the last third each hold
    /// `len / 3` values, the middle the rest, so there are none for fewer than three values.
    fn of(values_ms: &[f64]) -> Option<Self> {
        let third = values_ms.len() / 3;
        if third == 0 {
            return None;
        }
        let first = Stats::of(&values_ms[..third]);
        let last = Stats::of(&values_ms[values_ms.len() - third..]);
        let ratio = |last: f64, first: f64| (first > 0.0).then(|| last / first);
        Some(Self {
            first_mean_ms: first.mean,
            first_p50_ms: first.p50,
            last_mean_ms: last.mean,
            last_p50_ms: last.p50,
            mean_ratio: ratio(last.mean, first.mean),
            p50_ratio: ratio(last.p50, first.p50),
        })
    }
}

/// One [`STAGES`] row's figures over the timed outputs.
#[derive(Clone, Copy, Debug, PartialEq)]
struct StageSummary {
    label: &'static str,
    key: &'static str,
    /// Its milliseconds per output.
    stats: Stats,
    /// Its share of the summed `total`, in percent.
    share_pct: f64,
    thirds: Option<Thirds>,
}

/// Each [`STAGES`] row's figures over `rows`, which must not be empty.
fn summarize(rows: &[OutputRow]) -> Vec<StageSummary> {
    let total: f64 = rows.iter().map(|row| row.total).sum();
    STAGES
        .iter()
        .map(|&(label, key, stage)| {
            let values_ms: Vec<f64> = rows.iter().map(|row| stage.of(row) * 1e3).collect();
            StageSummary {
                label,
                key,
                stats: Stats::of(&values_ms),
                share_pct: values_ms.iter().sum::<f64>() / 1e3 / total * 100.0,
                thirds: Thirds::of(&values_ms),
            }
        })
        .collect()
}

/// The length and allocated bytes of a datadir's database data file ([`db_file_bytes`]) and of
/// its static files ([`dir_bytes`]) at one point of the run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
struct Sizes {
    db_len_bytes: u64,
    db_allocated_bytes: u64,
    static_files_len_bytes: u64,
    static_files_allocated_bytes: u64,
}

impl Sizes {
    /// The sizes in `datadir` now.
    fn of(datadir: &Path) -> eyre::Result<Self> {
        let (db_len_bytes, db_allocated_bytes) = db_file_bytes(&datadir.join("db"))?;
        let (static_files_len_bytes, static_files_allocated_bytes) =
            dir_bytes(&datadir.join("static_files"))?;
        Ok(Self {
            db_len_bytes,
            db_allocated_bytes,
            static_files_len_bytes,
            static_files_allocated_bytes,
        })
    }
}

/// The clone's sizes through the run.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct RunSizes {
    /// Once the env opened on the clone.
    open: Sizes,
    /// After the warm-up, before the timed loop.
    before_timed: Sizes,
    /// After the timed loop, the env still open.
    after_timed: Sizes,
    /// After the env closed.
    closed: Sizes,
}

impl RunSizes {
    /// The points, by report label and JSON key.
    fn points(&self) -> [(&'static str, &'static str, Sizes); 4] {
        [
            ("open", "open", self.open),
            ("before timed", "before_timed", self.before_timed),
            ("after timed", "after_timed", self.after_timed),
            ("closed", "closed", self.closed),
        ]
    }

    /// The database's and the static files' allocated bytes the timed loop added per output,
    /// from `before_timed` to `after_timed` (both with the env open), over `outputs`.
    fn growth_per_output(&self, outputs: usize) -> (f64, f64) {
        let growth = |before: u64, after: u64| (after as f64 - before as f64) / outputs as f64;
        let (before, after) = (self.before_timed, self.after_timed);
        (
            growth(before.db_allocated_bytes, after.db_allocated_bytes),
            growth(before.static_files_allocated_bytes, after.static_files_allocated_bytes),
        )
    }
}

/// One of reth's tables in the closed clone ([`DbTableStat`]).
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
struct TableRow {
    name: &'static str,
    entries: u64,
    branch_pages: u64,
    leaf_pages: u64,
    overflow_pages: u64,
    /// The bytes its pages take ([`DbTableStat::total_size`]).
    size_bytes: u64,
}

/// `stats` as rows, largest first, equal sizes by name.
fn tables_by_size(stats: Vec<DbTableStat>) -> Vec<TableRow> {
    let mut tables: Vec<TableRow> = stats
        .into_iter()
        .map(|stat| TableRow {
            name: stat.name,
            entries: stat.entries,
            branch_pages: stat.branch_pages,
            leaf_pages: stat.leaf_pages,
            overflow_pages: stat.overflow_pages,
            size_bytes: stat.total_size,
        })
        .collect();
    tables.sort_by(|a, b| b.size_bytes.cmp(&a.size_bytes).then_with(|| a.name.cmp(b.name)));
    tables
}

/// The tables the text report lists; the JSON report lists them all.
const TOP_TABLES: usize = 10;

/// The clone's footprint: its sizes through the run and its tables once closed.
struct Footprint {
    sizes: RunSizes,
    /// The tables' total size in the clone before the env opened it.
    baseline_table_bytes: u64,
    /// The outputs executed on the clone: the warm-up and the timed outputs.
    executed: usize,
    /// Every table once closed, largest first ([`tables_by_size`]), or the error reading them.
    tables: Result<Vec<TableRow>, String>,
}

impl Footprint {
    /// The bytes the tables' total size grew per executed output, from the baseline to the
    /// closed clone, or `None` when the closed tables could not be read. Unlike the allocated
    /// growth it does not depend on how the data file grows: tndb-b's grows in preallocated
    /// steps, so over a short run its allocated growth reads 0.
    fn logical_growth_per_output(&self) -> Option<f64> {
        let tables = self.tables.as_ref().ok()?;
        let total: u64 = tables.iter().map(|table| table.size_bytes).sum();
        Some((total as f64 - self.baseline_table_bytes as f64) / self.executed as f64)
    }
}

/// The database counters whose [`OutputRow::db_counters`] value is not a difference but a maximum
/// since the open (`DbCounters::since` keeps the later read's): their summary is the largest, not
/// the mean.
const MAXIMA: [&str; 1] = ["tndb_max_gate_lag"];

/// How many equal consecutive groups [`DbWrites::of`] splits the timed outputs into.
const DECILES: usize = 10;

/// One database counter over the timed outputs ([`DbWrites::counters`]).
#[derive(Clone, Copy, Debug, PartialEq)]
struct CounterSummary {
    name: &'static str,
    /// Its mean per output, or for one of the [`MAXIMA`] its largest value.
    value: f64,
    /// Whether one of [`MAXIMA`].
    maximum: bool,
    /// Whether it moved in any output.
    moved: bool,
}

/// A tenth of the timed outputs, consecutive ([`DbWrites::of`]).
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
struct Decile {
    outputs: usize,
    /// The database commit's (`commit mdbx`) mean, in milliseconds per output.
    commit_mdbx_mean_ms: f64,
    /// The disk bytes' mean per output over its outputs that have them, or `None` when none has.
    disk_bytes_mean: Option<f64>,
    /// The database data file's mean length after its outputs.
    db_file_len_mean_bytes: f64,
    /// The database data file's length after its last output.
    db_file_len_end_bytes: u64,
}

/// What the database and the process wrote over the timed outputs ([`OutputRow::db_counters`],
/// [`OutputRow::disk_bytes`], [`OutputRow::db_file_len`]).
#[derive(Clone, Debug, PartialEq)]
struct DbWrites {
    /// Each counter, in the rows' order.
    counters: Vec<CounterSummary>,
    /// The disk bytes' mean per output over the outputs that have them, or `None` when none has.
    disk_bytes_mean: Option<f64>,
    /// The outputs that have disk bytes.
    disk_outputs: usize,
    /// The data file's length before the first timed output.
    db_file_len_before: u64,
    /// The data file's length after the last timed output.
    db_file_len_after: u64,
    /// The outputs' deciles: [`DECILES`] groups, group `k` the outputs from `k * n / 10` up to
    /// `(k + 1) * n / 10` (sizes differ by at most one); `None` for fewer than [`DECILES`].
    deciles: Option<Vec<Decile>>,
}

/// The mean of `values`, or `None` for none.
fn mean_of(values: impl Iterator<Item = f64>) -> Option<f64> {
    let (sum, count) = values.fold((0.0, 0_usize), |(sum, count), value| (sum + value, count + 1));
    (count > 0).then(|| sum / count as f64)
}

impl DbWrites {
    /// The writes over `rows`, which must not be empty, whose data file was `db_file_len_before`
    /// bytes long before the first.
    fn of(rows: &[OutputRow], db_file_len_before: u64) -> Self {
        let count = |row: &OutputRow, name: &str| {
            row.db_counters.iter().find(|(entry, _)| *entry == name).map_or(0, |(_, value)| *value)
        };
        let counters = rows[0]
            .db_counters
            .iter()
            .map(|&(name, _)| {
                let maximum = MAXIMA.contains(&name);
                let values = rows.iter().map(|row| count(row, name));
                let value = if maximum {
                    values.max().unwrap_or(0) as f64
                } else {
                    values.sum::<u64>() as f64 / rows.len() as f64
                };
                let moved = rows.iter().any(|row| count(row, name) > 0);
                CounterSummary { name, value, maximum, moved }
            })
            .collect();
        let disk_mean = |rows: &[OutputRow]| {
            mean_of(rows.iter().filter_map(|row| row.disk_bytes).map(|b| b as f64))
        };
        let deciles = (rows.len() >= DECILES).then(|| {
            (0..DECILES)
                .map(|k| {
                    let group = &rows[k * rows.len() / DECILES..(k + 1) * rows.len() / DECILES];
                    Decile {
                        outputs: group.len(),
                        commit_mdbx_mean_ms: mean_of(
                            group.iter().map(|row| row.stage(COMMIT_MDBX) * 1e3),
                        )
                        .unwrap_or_default(),
                        disk_bytes_mean: disk_mean(group),
                        db_file_len_mean_bytes: mean_of(
                            group.iter().map(|row| row.db_file_len as f64),
                        )
                        .unwrap_or_default(),
                        db_file_len_end_bytes: group.last().map_or(0, |row| row.db_file_len),
                    }
                })
                .collect()
        });
        Self {
            counters,
            disk_bytes_mean: disk_mean(rows),
            disk_outputs: rows.iter().filter(|row| row.disk_bytes.is_some()).count(),
            db_file_len_before,
            db_file_len_after: rows[rows.len() - 1].db_file_len,
            deciles,
        }
    }

    /// The data file's growth over the timed outputs, negative when it shrank.
    fn db_file_growth(&self) -> i64 {
        self.db_file_len_after as i64 - self.db_file_len_before as i64
    }
}

/// The report's line naming where the disk bytes come from ([`DISK_BYTES_SOURCE`]), with the
/// first failed read's `error`, if any.
fn disk_bytes_line(source: Option<&str>, error: Option<&str>) -> String {
    match (source, error) {
        (None, _) => "  disk bytes: none (this target has no source)\n".to_owned(),
        (Some(source), None) => {
            format!("  disk bytes: {source} (the process's bytes written, approximate)\n")
        }
        (Some(source), Some(e)) => {
            format!("  disk bytes: {source}, a read FAILED (its outputs have none): {e}\n")
        }
    }
}

/// The report's database-writes section: each counter's mean per output (a maximum's largest),
/// leaving out those that never moved, `_ns` counters in milliseconds; the disk bytes and the
/// data file's growth per output; then the deciles.
fn db_writes_report(writes: &DbWrites, outputs: usize) -> String {
    let mut out = "database writes per output: the database's own counters over each timed \
                   output's call (mean per output; counters that never moved left out)\n"
        .to_owned();
    for counter in writes.counters.iter().filter(|counter| counter.moved) {
        let value = if counter.maximum {
            format!("{:.0} (max)", counter.value)
        } else if counter.name.ends_with("_ns") {
            format!("{:.3} ms", counter.value / 1e6)
        } else {
            format!("{:.3}", counter.value)
        };
        out += &format!("  {:<28} {value:>16}\n", counter.name);
    }
    out += &match writes.disk_bytes_mean {
        Some(bytes) => format!(
            "  {:<28} {:>16} per output ({} outputs read)\n",
            "disk written",
            format!("{:.3} MiB", bytes / f64::from(1 << 20)),
            writes.disk_outputs
        ),
        None => format!("  {:<28} {:>16}\n", "disk written", "-"),
    };
    let growth = writes.db_file_growth() as f64 / f64::from(1 << 20);
    out += &format!(
        "  {:<28} {:>16} per output, {growth:.1} MiB over {outputs} outputs\n",
        "db file growth",
        format!("{:.3} MiB", growth / outputs as f64)
    );
    let Some(deciles) = &writes.deciles else {
        out += &format!("deciles: none for fewer than {DECILES} timed outputs\n");
        return out;
    };
    out += &format!(
        "{:<8} {:>8} {:>12} {:>10} {:>12}   (means per output; file at the decile's end)\n",
        "decile", "outputs", "commit ms", "disk MiB", "file MiB"
    );
    for (k, decile) in deciles.iter().enumerate() {
        let disk = decile
            .disk_bytes_mean
            .map_or_else(|| "-".to_owned(), |bytes| format!("{:.3}", bytes / f64::from(1 << 20)));
        out += &format!(
            "  {:<6} {:>8} {:>12.3} {disk:>10} {:>12.1}\n",
            k + 1,
            decile.outputs,
            decile.commit_mdbx_mean_ms,
            mib(decile.db_file_len_end_bytes)
        );
    }
    out
}

/// The JSON report's `db_writes`: per counter its mean per output (`counters_mean_per_output`,
/// the differences) or its largest (`counters_max`, the [`MAXIMA`]), in the rows' order; the disk
/// bytes' mean and how many outputs have them; the data file's length before and after the timed
/// outputs and its growth; and the deciles.
fn db_writes_json(writes: &DbWrites, outputs: usize) -> serde_json::Value {
    use serde_json::{json, Map, Value};

    let counters = |maximum: bool| -> Map<String, Value> {
        writes
            .counters
            .iter()
            .filter(|counter| counter.maximum == maximum)
            .map(|counter| (counter.name.to_owned(), counter.value.into()))
            .collect()
    };
    json!({
        "counters_mean_per_output": counters(false),
        "counters_max": counters(true),
        "disk_bytes_mean_per_output": writes.disk_bytes_mean,
        "disk_bytes_outputs": writes.disk_outputs,
        "db_file_len_before_bytes": writes.db_file_len_before,
        "db_file_len_after_bytes": writes.db_file_len_after,
        "db_file_growth_bytes": writes.db_file_growth(),
        "db_file_growth_per_output_bytes": writes.db_file_growth() as f64 / outputs as f64,
        "deciles": writes.deciles,
    })
}

/// The main stages the side-load writer split compares, by [`STAGES`] key.
const SPLIT_KEYS: [&str; 8] = [
    "total",
    "execute",
    "finish",
    "state_root",
    "save_blocks",
    "commit_sf",
    "commit_mdbx",
    "other",
];

/// One [`SPLIT_KEYS`] stage's milliseconds per output over the timed outputs a split puts off and
/// on ([`split_by`]): the side load off and on, say; `None` for a side with no outputs.
#[derive(Clone, Copy, Debug, PartialEq)]
struct SplitRow {
    key: &'static str,
    label: &'static str,
    off: Option<Stats>,
    on: Option<Stats>,
}

impl SplitRow {
    /// On over off of the mean, the p50 and the p99; `None` without both sides or for a zero off.
    fn ratios(&self) -> [Option<f64>; 3] {
        let ratio = |pick: fn(&Stats) -> f64| {
            let (off, on) = (pick(self.off.as_ref()?), pick(self.on.as_ref()?));
            (off > 0.0).then(|| on / off)
        };
        [ratio(|stats| stats.mean), ratio(|stats| stats.p50), ratio(|stats| stats.p99)]
    }
}

/// Each [`SPLIT_KEYS`] stage's figures over `rows` with the side load off and on.
fn writer_split(rows: &[OutputRow]) -> Vec<SplitRow> {
    split_by(rows, |row| row.side_load)
}

/// Each [`SPLIT_KEYS`] stage's figures over the `rows` that `on` refuses (off) and accepts (on).
fn split_by(rows: &[OutputRow], on: impl Fn(&OutputRow) -> bool) -> Vec<SplitRow> {
    SPLIT_KEYS
        .iter()
        .map(|&key| {
            let &(label, _, stage) = STAGES
                .iter()
                .find(|(_, stage_key, _)| *stage_key == key)
                .unwrap_or_else(|| panic!("{key} is a stage key"));
            let side = |side: bool| {
                let values_ms: Vec<f64> = rows
                    .iter()
                    .filter(|row| on(row) == side)
                    .map(|row| stage.of(row) * 1e3)
                    .collect();
                (!values_ms.is_empty()).then(|| Stats::of(&values_ms))
            };
            SplitRow { key, label: label.trim(), off: side(false), on: side(true) }
        })
        .collect()
}

/// The stats of `latencies` in milliseconds, or `None` for none.
fn latency_ms(latencies: &[Duration]) -> Option<Stats> {
    let values: Vec<f64> = latencies.iter().map(|latency| latency.as_secs_f64() * 1e3).collect();
    (!values.is_empty()).then(|| Stats::of(&values))
}

/// A reader kind's report label and JSON key.
fn kind_key(kind: ReadKind) -> &'static str {
    match kind {
        ReadKind::Balance => "balance",
        ReadKind::TokenBalance => "token_balance",
        ReadKind::Receipt => "receipt",
        ReadKind::Logs => "logs",
        ReadKind::Block => "block",
    }
}

/// `stats`' p50 and p99 to three places, or `-` for none.
fn p50_p99_text(stats: Option<Stats>) -> (String, String) {
    stats.map_or_else(
        || ("-".to_owned(), "-".to_owned()),
        |stats| (format!("{:.3}", stats.p50), format!("{:.3}", stats.p99)),
    )
}

/// What the report's header says besides the knobs.
struct Header {
    knobs: Knobs,
    /// The run's database page size in bytes, read from the open clone ([`db_page_size`]).
    page_size: u32,
    host: String,
    date: String,
    base: String,
    aged: String,
    /// The timed loop's wall clock, from the first output's start to the last's end, without the
    /// time between segments ([`Timed::elapsed`]).
    elapsed: Duration,
    /// With `EXPORT=1`, what the export wrote, or its error.
    export: Option<Result<ExportRun, String>>,
    /// Why a timed output's [`disk_bytes_written`] read failed, the first time one did.
    disk_error: Option<String>,
}

/// `bytes` in MiB.
fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

/// A ratio to three places, or `-` for none.
fn ratio_text(ratio: Option<f64>) -> String {
    ratio.map_or_else(|| "-".to_owned(), |ratio| format!("{ratio:.3}"))
}

/// The report of the timed `rows`, the clone's `footprint` and the `side` load, if it ran (see
/// the module docs).
fn report(
    header: &Header,
    rows: &[OutputRow],
    footprint: &Footprint,
    side: Option<&SideSamples>,
) -> String {
    let knobs = &header.knobs;
    let mut out = format!(
        "exec_bench backend={BACKEND} sync={} metrics={} pacing={}\n",
        knobs.sync.label(),
        knobs.metrics.label(),
        knobs.pacing()
    );
    out += &format!("  knobs: {}\n", knobs.line());
    out += &format!(
        "  page size: {} bytes (PAGE_SIZE={})\n",
        header.page_size,
        knobs.page_size_label()
    );
    out += &disk_bytes_line(DISK_BYTES_SOURCE, header.disk_error.as_deref());
    out += &format!("  host: {}  date: {}\n", header.host, header.date);
    if cfg!(target_os = "macos") && knobs.sync == SyncMode::Durable {
        out += "  note: on macOS libmdbx's durable commit uses F_FULLFSYNC and tndb-b's a plain \
                fsync: durable commit times are not like-for-like between the backends\n";
    }
    out += &format!("  base: {}\n  aged: {}\n", header.base, header.aged);
    out += "  pool + RPC: on (worker 0's transaction pool and HTTP RPC methods, as on a node)\n";
    out += &format!(
        "{:<26} {:>9} {:>9} {:>9} {:>9} {:>9} {:>7}   (ms per output)\n",
        "stage", "mean", "p50", "p90", "p99", "max", "share"
    );
    let summary = summarize(rows);
    for stage in &summary {
        let stats = stage.stats;
        out += &format!(
            "{:<26} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>9.3} {:>6.1}%\n",
            stage.label, stats.mean, stats.p50, stats.p90, stats.p99, stats.max, stage.share_pct
        );
    }
    let throughput = Throughput::of(rows);
    out += &format!(
        "throughput: outputs={} blocks={} txs={} gas={} busy_s={:.3} elapsed_s={:.3} \
         mgas_per_s={:.2} tx_per_s={:.0} p50_output_mgas_per_s={:.2}\n",
        rows.len(),
        throughput.blocks,
        throughput.txs,
        throughput.gas,
        throughput.seconds,
        header.elapsed.as_secs_f64(),
        throughput.mgas_per_s,
        throughput.tx_per_s,
        throughput.p50_output_mgas_per_s
    );
    if knobs.pace().is_some() {
        let late = rows.iter().filter(|row| row.late).count();
        out += &format!("late starts: {late} of {}\n", rows.len());
    }
    out += &close_report(knobs, rows);

    let third = rows.len() / 3;
    if third == 0 {
        out += "thirds: none for fewer than 3 timed outputs\n";
    } else {
        out += &format!(
            "thirds: the first {third} and the last {third} timed outputs (ms per output; ratio = \
             last / first)\n"
        );
        out += &format!(
            "{:<26} {:>10} {:>10} {:>7} {:>10} {:>10} {:>7}\n",
            "stage", "first mean", "last mean", "ratio", "first p50", "last p50", "ratio"
        );
        for stage in &summary {
            let Some(thirds) = stage.thirds else { continue };
            out += &format!(
                "{:<26} {:>10.3} {:>10.3} {:>7} {:>10.3} {:>10.3} {:>7}\n",
                stage.label,
                thirds.first_mean_ms,
                thirds.last_mean_ms,
                ratio_text(thirds.mean_ratio),
                thirds.first_p50_ms,
                thirds.last_p50_ms,
                ratio_text(thirds.p50_ratio)
            );
        }
    }

    let sizes = &footprint.sizes;
    out += &format!(
        "{:<26} {:>10} {:>10} {:>10} {:>10}   (MiB)\n",
        "sizes", "db alloc", "db len", "sf alloc", "sf len"
    );
    for (label, _, point) in sizes.points() {
        out += &format!(
            "  {label:<24} {:>10.1} {:>10.1} {:>10.1} {:>10.1}\n",
            mib(point.db_allocated_bytes),
            mib(point.db_len_bytes),
            mib(point.static_files_allocated_bytes),
            mib(point.static_files_len_bytes)
        );
    }
    let (db_growth, static_files_growth) = sizes.growth_per_output(rows.len());
    out += &format!(
        "growth per timed output (allocated): db {:.1} KiB, static files {:.1} KiB\n",
        db_growth / 1024.0,
        static_files_growth / 1024.0
    );
    out += &match footprint.logical_growth_per_output() {
        Some(growth) => format!(
            "growth per executed output (logical, table bytes): db {:.1} KiB over {} outputs \
             (warm-up and timed)\n",
            growth / 1024.0,
            footprint.executed
        ),
        None => "growth per executed output (logical, table bytes): unknown, the table stats \
                 failed\n"
            .to_owned(),
    };
    out += &db_writes_report(&DbWrites::of(rows, sizes.before_timed.db_len_bytes), rows.len());
    out += &export_report(header.export.as_ref(), rows);

    let tables = match &footprint.tables {
        Ok(tables) => tables,
        Err(e) => {
            out += &format!("table: FAILED reading the closed clone's table stats: {e}\n");
            out += &side_report(rows, side);
            return out;
        }
    };
    out += &format!(
        "{:<26} {:>14} {:>10}   (the {} largest of {} tables, closed clone)\n",
        "table",
        "entries",
        "MiB",
        tables.len().min(TOP_TABLES),
        tables.len()
    );
    for table in tables.iter().take(TOP_TABLES) {
        out += &format!(
            "  {:<24} {:>14} {:>10.1}\n",
            table.name,
            table.entries,
            mib(table.size_bytes)
        );
    }
    out += &format!(
        "  {:<24} {:>14} {:>10.1}\n",
        "total",
        tables.iter().map(|table| table.entries).sum::<u64>(),
        mib(tables.iter().map(|table| table.size_bytes).sum())
    );
    out += &side_report(rows, side);
    out
}

/// The report's side-load section: the writer's stages with the side load off and on, then
/// what the readers, the submitter and the poller recorded; one line when it did not run.
fn side_report(rows: &[OutputRow], side: Option<&SideSamples>) -> String {
    let Some(side) = side else {
        return "side load: off (READERS=0)\n".to_owned();
    };
    let on = rows.iter().filter(|row| row.side_load).count();
    let mut out = format!(
        "side load: {} outputs off, {on} on, on for {:.3} s; errors {}\n",
        rows.len() - on,
        side.on_time.as_secs_f64(),
        side.errors()
    );
    out += &split_text("off", "on", &writer_split(rows));
    out += &format!(
        "{:<12} {:>9} {:>9} {:>9} {:>9} {:>9} {:>7} {:>7}   (on windows; ms)\n",
        "readers", "ops", "ops/s", "p50", "p99", "ok", "empty", "errors"
    );
    let on_s = side.on_time.as_secs_f64();
    for (kind, samples) in ReadKind::ALL.iter().zip(&side.kinds) {
        let ops = samples.ok + samples.empty + samples.errors;
        let ops_per_s = if on_s > 0.0 { ops as f64 / on_s } else { 0.0 };
        let (p50, p99) = p50_p99_text(latency_ms(&samples.latencies));
        out += &format!(
            "  {:<10} {ops:>9} {ops_per_s:>9.1} {p50:>9} {p99:>9} {:>9} {:>7} {:>7}\n",
            kind_key(*kind),
            samples.ok,
            samples.empty,
            samples.errors
        );
        if let Some(e) = &samples.first_error {
            out += &format!("    first error ({}): {e}\n", kind.method());
        }
    }
    let submits = &side.submits;
    let (p50, p99) = p50_p99_text(latency_ms(&submits.latencies));
    out += &format!(
        "submitter: accepted={} nonce_too_low={} already_known={} errors={} p50_ms={p50} \
         p99_ms={p99}\n",
        submits.accepted, submits.nonce_too_low, submits.already_known, submits.errors
    );
    if let Some(e) = &submits.first_error {
        out += &format!("  first error: {e}\n");
    }
    let (p50, p99) = p50_p99_text(latency_ms(&side.polls));
    out += &format!(
        "poller: polls={} p50_ms={p50} p99_ms={p99} degraded_windows={}\n",
        side.polls.len(),
        side.degraded_polls
    );
    out += &format!("calls in off windows: {}\n", side.off_calls);
    out
}

/// A writer split's report lines: a heading naming its sides `off` and `on`, then per stage the
/// mean, p50 and p99 of both sides and the ratios on over off.
fn split_text(off: &str, on: &str, splits: &[SplitRow]) -> String {
    let width = (off.len().max(on.len()) + 5).max(9);
    let mut out = format!(
        "{:<12} {:>width$} {:>width$} {:>width$} {:>width$} {:>width$} {:>width$} {:>7} {:>7} \
         {:>7}   (ms per output; ratio = {on} / {off})\n",
        "writer",
        format!("{off} mean"),
        format!("{off} p50"),
        format!("{off} p99"),
        format!("{on} mean"),
        format!("{on} p50"),
        format!("{on} p99"),
        "mean",
        "p50",
        "p99"
    );
    let figure = |stats: Option<Stats>, pick: fn(&Stats) -> f64| {
        stats.map_or_else(|| "-".to_owned(), |stats| format!("{:.3}", pick(&stats)))
    };
    for split in splits {
        let [mean, p50, p99] = split.ratios().map(ratio_text);
        out += &format!(
            "  {:<10} {:>width$} {:>width$} {:>width$} {:>width$} {:>width$} {:>width$} {:>7} \
             {:>7} {:>7}\n",
            split.label,
            figure(split.off, |stats| stats.mean),
            figure(split.off, |stats| stats.p50),
            figure(split.off, |stats| stats.p99),
            figure(split.on, |stats| stats.mean),
            figure(split.on, |stats| stats.p50),
            figure(split.on, |stats| stats.p99),
            mean,
            p50,
            p99
        );
    }
    out
}

/// The report's epoch-close section: the closing outputs' count and the writer's main stages over
/// the normal and the closing outputs; one line without `EPOCH_EVERY`.
fn close_report(knobs: &Knobs, rows: &[OutputRow]) -> String {
    if knobs.epoch_every == 0 {
        return "epoch closes: none (EPOCH_EVERY=0)\n".to_owned();
    }
    let closes = rows.iter().filter(|row| row.epoch_close).count();
    let mut out = format!(
        "epoch closes: {closes} of {} timed outputs, every {} (each boundary after them untimed)\n",
        rows.len(),
        knobs.epoch_every
    );
    out += &split_text("normal", "close", &split_by(rows, |row| row.epoch_close));
    out
}

/// The report's export section: the pinned block, how long the export took and what it wrote,
/// the timed outputs it overlapped, the database's growth while it was pinned, and the writer's
/// main stages with it idle and active; one line without `EXPORT`.
fn export_report(export: Option<&Result<ExportRun, String>>, rows: &[OutputRow]) -> String {
    let Some(export) = export else {
        return "export: off (EXPORT=0)\n".to_owned();
    };
    let overlapped = rows.iter().filter(|row| row.export_active).count();
    let mut out = match export {
        Ok(run) => {
            let (allocated, len) = run.db_growth();
            let signed_mib = |bytes: i64| bytes as f64 / f64::from(1 << 20);
            let mut out = format!(
                "export: block {} in {:.3} s (pin to pack finish), accounts={} storage_slots={} \
                 bytecodes={}, pack {:.1} MiB; {overlapped} of {} timed outputs overlapped\n",
                run.block,
                run.duration.as_secs_f64(),
                run.stats.account_count,
                run.stats.storage_slots,
                run.stats.bytecodes,
                mib(run.pack_len_bytes),
                rows.len()
            );
            out += &format!(
                "  db while pinned (MiB): allocated {:.1} -> {:.1} ({:+.1}), len {:.1} -> {:.1} \
                 ({:+.1})\n",
                mib(run.at_pin.db_allocated_bytes),
                mib(run.at_finish.db_allocated_bytes),
                signed_mib(allocated),
                mib(run.at_pin.db_len_bytes),
                mib(run.at_finish.db_len_bytes),
                signed_mib(len)
            );
            out
        }
        Err(e) => {
            format!(
                "export: FAILED: {e}; {overlapped} of {} timed outputs overlapped\n",
                rows.len()
            )
        }
    };
    out += &split_text("idle", "active", &split_by(rows, |row| row.export_active));
    out
}

/// The JSON report's layout version, bumped when a field changes meaning or goes away.
const JSON_REPORT_VERSION: u32 = 4;

/// The JSON report of the timed `rows`, the clone's `footprint` and the `side` load, if it ran
/// (see the module docs). Its field names stay stable for the scripts that aggregate runs:
/// snake_case, with the unit in the name.
fn json_report(
    header: &Header,
    rows: &[OutputRow],
    footprint: &Footprint,
    side: Option<&SideSamples>,
) -> serde_json::Value {
    use serde_json::{json, Map, Value};

    let knobs = &header.knobs;
    let summary = summarize(rows);
    let rows_json: Vec<Value> = rows
        .iter()
        .map(|row| {
            let mut fields = Map::new();
            fields.insert("first_block".to_owned(), row.first_block.into());
            fields.insert("last_block".to_owned(), row.last_block.into());
            fields.insert("gas".to_owned(), row.gas.into());
            fields.insert("txs".to_owned(), row.txs.into());
            fields.insert("late".to_owned(), row.late.into());
            fields.insert("side_load".to_owned(), row.side_load.into());
            fields.insert("epoch_close".to_owned(), row.epoch_close.into());
            fields.insert("export_active".to_owned(), row.export_active.into());
            for (_, key, stage) in STAGES {
                fields.insert(format!("{key}_ms"), (stage.of(row) * 1e3).into());
            }
            let counters: Map<String, Value> = row
                .db_counters
                .iter()
                .map(|&(name, value)| (name.to_owned(), value.into()))
                .collect();
            fields.insert("db_counters".to_owned(), counters.into());
            fields.insert("disk_bytes_written".to_owned(), json!(row.disk_bytes));
            fields.insert("db_file_len_bytes".to_owned(), row.db_file_len.into());
            Value::Object(fields)
        })
        .collect();
    let stats: Map<String, Value> = summary
        .iter()
        .map(|stage| {
            let stats = stage.stats;
            let value = json!({
                "mean_ms": stats.mean,
                "p50_ms": stats.p50,
                "p90_ms": stats.p90,
                "p99_ms": stats.p99,
                "max_ms": stats.max,
                "share_pct": stage.share_pct,
            });
            (stage.key.to_owned(), value)
        })
        .collect();
    let thirds = if rows.len() < 3 {
        Value::Null
    } else {
        let stages: Map<String, Value> =
            summary.iter().map(|stage| (stage.key.to_owned(), json!(stage.thirds))).collect();
        json!({ "outputs_per_third": rows.len() / 3, "stages": stages })
    };
    let throughput = Throughput::of(rows);
    let sizes = &footprint.sizes;
    let mut sizes_json: Map<String, Value> =
        sizes.points().into_iter().map(|(_, key, point)| (key.to_owned(), json!(point))).collect();
    let (db_growth, static_files_growth) = sizes.growth_per_output(rows.len());
    sizes_json.insert("db_growth_per_output_bytes".to_owned(), db_growth.into());
    sizes_json
        .insert("static_files_growth_per_output_bytes".to_owned(), static_files_growth.into());
    sizes_json.insert(
        "db_logical_growth_per_output_bytes".to_owned(),
        json!(footprint.logical_growth_per_output()),
    );
    let tables = match &footprint.tables {
        Ok(tables) => json!({
            "failed": false,
            "error": null,
            "baseline_total_size_bytes": footprint.baseline_table_bytes,
            "count": tables.len(),
            "total_entries": tables.iter().map(|table| table.entries).sum::<u64>(),
            "total_size_bytes": tables.iter().map(|table| table.size_bytes).sum::<u64>(),
            "by_size": tables,
        }),
        Err(e) => json!({
            "failed": true,
            "error": e,
            "baseline_total_size_bytes": footprint.baseline_table_bytes,
            "count": null,
            "total_entries": null,
            "total_size_bytes": null,
            "by_size": null,
        }),
    };

    let epoch_closes = (knobs.epoch_every > 0).then(|| {
        let closes = rows.iter().filter(|row| row.epoch_close).count();
        let writer = split_json(split_by(rows, |row| row.epoch_close), "normal", "closing");
        json!({ "closes": closes, "normal_outputs": rows.len() - closes, "writer": writer })
    });

    let db_writes =
        db_writes_json(&DbWrites::of(rows, sizes.before_timed.db_len_bytes), rows.len());
    let counter_names: Vec<&str> = rows[0].db_counters.iter().map(|&(name, _)| name).collect();
    json!({
        "report_version": JSON_REPORT_VERSION,
        "header": {
            "backend": BACKEND,
            "sync": knobs.sync.label(),
            "page_size_bytes": header.page_size,
            "disk_bytes_source": DISK_BYTES_SOURCE,
            "disk_bytes_error": header.disk_error,
            "db_counter_names": counter_names,
            "metrics": knobs.metrics.label(),
            "pacing": knobs.pacing(),
            "host": header.host,
            "date": header.date,
            "base": header.base,
            "aged": header.aged,
            "pool_rpc": "on",
            "knobs": {
                "outputs": knobs.outputs,
                "batches": knobs.batches,
                "gas": knobs.gas,
                "warmup": knobs.warmup,
                "aging": knobs.aging,
                "prefill": knobs.prefill,
                "senders": knobs.senders,
                "pools": knobs.pools,
                "seed": knobs.seed,
                "registry": knobs.registry,
                "pace_ms": knobs.pace_ms,
                "metrics": knobs.metrics.label(),
                "sync": knobs.sync.label(),
                "page_size": knobs.page_size,
                "readers": knobs.readers,
                "submit_fraction": knobs.submit_fraction,
                "window": knobs.window,
                "epoch_every": knobs.epoch_every,
                "export": knobs.export,
                "cache_dir": knobs.cache_dir.display().to_string(),
                "report": knobs.report.as_ref().map(|path| path.display().to_string()),
            },
        },
        "rows": rows_json,
        "stats": stats,
        "throughput": {
            "outputs": rows.len(),
            "blocks": throughput.blocks,
            "txs": throughput.txs,
            "gas": throughput.gas,
            "busy_s": throughput.seconds,
            "elapsed_s": header.elapsed.as_secs_f64(),
            "mgas_per_s": throughput.mgas_per_s,
            "tx_per_s": throughput.tx_per_s,
            "p50_output_mgas_per_s": throughput.p50_output_mgas_per_s,
            "late_starts": rows.iter().filter(|row| row.late).count(),
        },
        "thirds": thirds,
        "sizes": sizes_json,
        "db_writes": db_writes,
        "tables": tables,
        "side_load": side.map(|side| side_json(rows, side)),
        "epoch_closes": epoch_closes,
        "export": header.export.as_ref().map(|export| export_json(export, rows)),
    })
}

/// A writer split as JSON: per [`SPLIT_KEYS`] key each side's `mean_ms`, `p50_ms` and `p99_ms`
/// (null for a side with no outputs) under the names `off` and `on`, and the ratios on over off.
fn split_json(splits: Vec<SplitRow>, off: &str, on: &str) -> serde_json::Value {
    use serde_json::{json, Map, Value};

    let stats = |stats: Option<Stats>| {
        json!(stats.map(
            |stats| json!({ "mean_ms": stats.mean, "p50_ms": stats.p50, "p99_ms": stats.p99 })
        ))
    };
    let writer: Map<String, Value> = splits
        .into_iter()
        .map(|split| {
            let [mean_ratio, p50_ratio, p99_ratio] = split.ratios();
            let mut value = Map::new();
            value.insert(off.to_owned(), stats(split.off));
            value.insert(on.to_owned(), stats(split.on));
            value.insert("mean_ratio".to_owned(), json!(mean_ratio));
            value.insert("p50_ratio".to_owned(), json!(p50_ratio));
            value.insert("p99_ratio".to_owned(), json!(p99_ratio));
            (split.key.to_owned(), Value::Object(value))
        })
        .collect();
    Value::Object(writer)
}

/// The JSON report's `export` object (see the module docs).
fn export_json(export: &Result<ExportRun, String>, rows: &[OutputRow]) -> serde_json::Value {
    use serde_json::json;

    let overlapped = rows.iter().filter(|row| row.export_active).count();
    let writer = split_json(split_by(rows, |row| row.export_active), "idle", "active");
    match export {
        Ok(run) => {
            let (allocated, len) = run.db_growth();
            json!({
                "failed": false,
                "error": null,
                "block": run.block,
                "duration_s": run.duration.as_secs_f64(),
                "account_count": run.stats.account_count,
                "storage_slots": run.stats.storage_slots,
                "bytecodes": run.stats.bytecodes,
                "pack_len_bytes": run.pack_len_bytes,
                "pack_allocated_bytes": run.pack_allocated_bytes,
                "at_pin": run.at_pin,
                "at_finish": run.at_finish,
                "db_allocated_growth_bytes": allocated,
                "db_len_growth_bytes": len,
                "overlapped_outputs": overlapped,
                "writer": writer,
            })
        }
        Err(e) => json!({
            "failed": true,
            "error": e,
            "block": null,
            "duration_s": null,
            "account_count": null,
            "storage_slots": null,
            "bytecodes": null,
            "pack_len_bytes": null,
            "pack_allocated_bytes": null,
            "at_pin": null,
            "at_finish": null,
            "db_allocated_growth_bytes": null,
            "db_len_growth_bytes": null,
            "overlapped_outputs": overlapped,
            "writer": writer,
        }),
    }
}

/// The JSON report's `side_load` object (see the module docs).
fn side_json(rows: &[OutputRow], side: &SideSamples) -> serde_json::Value {
    use serde_json::{json, Map, Value};

    let writer = split_json(writer_split(rows), "off", "on");
    let on_s = side.on_time.as_secs_f64();
    let readers: Map<String, Value> = ReadKind::ALL
        .iter()
        .zip(&side.kinds)
        .map(|(kind, samples)| {
            let ops = samples.ok + samples.empty + samples.errors;
            let ops_per_s = if on_s > 0.0 { ops as f64 / on_s } else { 0.0 };
            let latency = latency_ms(&samples.latencies);
            let value = json!({
                "method": kind.method(),
                "ops": ops,
                "ops_per_s": ops_per_s,
                "p50_ms": latency.map(|stats| stats.p50),
                "p99_ms": latency.map(|stats| stats.p99),
                "ok": samples.ok,
                "empty": samples.empty,
                "errors": samples.errors,
                "first_error": samples.first_error,
            });
            (kind_key(*kind).to_owned(), value)
        })
        .collect();
    let submits = &side.submits;
    let submit_latency = latency_ms(&submits.latencies);
    let poll_latency = latency_ms(&side.polls);
    let on = rows.iter().filter(|row| row.side_load).count();
    json!({
        "off_outputs": rows.len() - on,
        "on_outputs": on,
        "on_time_s": on_s,
        "errors": side.errors(),
        "writer": writer,
        "readers": readers,
        "submitter": {
            "accepted": submits.accepted,
            "nonce_too_low": submits.nonce_too_low,
            "already_known": submits.already_known,
            "errors": submits.errors,
            "first_error": submits.first_error,
            "p50_ms": submit_latency.map(|stats| stats.p50),
            "p99_ms": submit_latency.map(|stats| stats.p99),
        },
        "poller": {
            "polls": side.polls.len(),
            "p50_ms": poll_latency.map(|stats| stats.p50),
            "p99_ms": poll_latency.map(|stats| stats.p99),
            "degraded_windows": side.degraded_polls,
        },
        "off_window_calls": side.off_calls,
    })
}

/// The dir and file name of the JSON report path `path`, refused unless the dir exists.
fn report_target(path: &Path) -> eyre::Result<(PathBuf, String)> {
    let name = path.file_name().and_then(|name| name.to_str()).ok_or_else(|| {
        eyre::eyre!("{KNOB_PREFIX}REPORT={} names no UTF-8 file name", path.display())
    })?;
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
        _ => PathBuf::from("."),
    };
    eyre::ensure!(
        dir.is_dir(),
        "{KNOB_PREFIX}REPORT={}: {} is not a directory",
        path.display(),
        dir.display()
    );
    Ok((dir, name.to_owned()))
}

/// The first line `program args` prints, trimmed, or `None` when it fails.
fn command_line(program: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(program).args(args).output().ok()?;
    let text = String::from_utf8(output.stdout).ok()?;
    output.status.success().then(|| text.lines().next().unwrap_or_default().trim().to_owned())
}

/// The host name and CPU model.
fn host() -> String {
    let name = command_line("hostname", &[]).unwrap_or_else(|| "unknown".to_owned());
    let cpu = if cfg!(target_os = "macos") {
        command_line("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        std::fs::read_to_string("/proc/cpuinfo").ok().and_then(|info| {
            info.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split(':').nth(1))
                .map(|model| model.trim().to_owned())
        })
    };
    format!("{name} ({})", cpu.unwrap_or_else(|| "unknown cpu".to_owned()))
}

/// The busy-node execution bench (see the module docs).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a long bench: run it alone, in release (see the module docs)"]
async fn exec_bench() -> eyre::Result<()> {
    let knobs = Knobs::from_env(BACKEND)?;
    // refuse a report path that cannot be written before the run, not after it
    let report_target = knobs.report.as_deref().map(report_target).transpose()?;

    // the recorder goes first: reth's metric handles bind to the recorder current at their first
    // registration, which the first env open does
    let (inner, prometheus) = match knobs.metrics {
        MetricsMode::Off => (None, None),
        MetricsMode::Prom => {
            let (recorder, handle) = tn_metrics::build_recorder()?;
            (Some(Box::new(recorder) as Box<dyn Recorder + Send + Sync>), Some(handle))
        }
    };
    let names: Vec<&'static str> = CAPTURED.iter().map(|(name, _)| *name).collect();
    let recorder = CaptureRecorder::new(&names, inner).install()?;
    // registry upkeep every 5s, as the node's metrics server runs it
    let upkeep = prometheus.map(|handle| {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                handle.run_upkeep();
            }
        })
    });

    let tmp_root = std::env::temp_dir();
    announce_temp_dir(&tmp_root);
    let spec = knobs.spec();
    let page_size = knobs.page_size();
    let base_cached = knobs.cache_dir.join(base_dir_name(&spec, page_size, "")?).exists();
    let (base_dir, base) = build_base(&spec, page_size, &knobs.cache_dir)?;
    let base_line = format!(
        "{} ({}, {} accounts, db {} MiB allocated of {} MiB)",
        base_dir.display(),
        if base_cached { "cached".to_owned() } else { format!("built in {} ms", base.build_ms) },
        spec.accounts,
        base.db_allocated >> 20,
        base.db_len >> 20
    );
    let aging = knobs.aging();
    let source = if aging.outputs == 0 {
        Source {
            dir: base_dir.clone(),
            tip_number: base.genesis_header.number,
            tip_hash: base.genesis_hash,
            built: false,
            build_ms: base.build_ms,
        }
    } else {
        aged_base(&knobs.cache_dir, &base_dir, &base, &aging, &recorder).await?
    };
    let aged_line = if aging.outputs == 0 {
        "none (AGING=0)".to_owned()
    } else {
        format!(
            "{} ({}, tip block {})",
            source.dir.display(),
            if source.built {
                format!("built in {} ms", source.build_ms)
            } else {
                "cached".to_owned()
            },
            source.tip_number
        )
    };

    // the run: a clone of the source, opened as the node opens it
    let tmp = tempfile::Builder::new().prefix("exec-bench-").tempdir_in(&tmp_root)?;
    ensure_same_file_system(&source.dir, tmp.path())?;
    let datadir = tmp.path().join("datadir");
    clone_datadir(&source.dir, &datadir)?;
    // the clone's tables before the env opens it, while the database is closed
    let baseline_table_bytes =
        db_table_stats(&datadir.join("db"))?.iter().map(|stat| stat.total_size).sum();
    // the driver keeps `tmp`: the clone outlives the env, to be measured closed
    let page_text = page_size.to_string();
    let node =
        BenchNode::open_at(&datadir, slim_chain(&base), &db_flags(knobs.sync, &page_text), None)?;
    let effective_page_size = db_page_size(&node.db)?;
    eyre::ensure!(
        effective_page_size == page_size,
        "the clone's database has {effective_page_size}-byte pages, the run asked for {page_size}"
    );
    let (pool, methods) = node.serve()?;
    let mut sizes = RunSizes { open: Sizes::of(&datadir)?, ..RunSizes::default() };
    let tip = node.env.canonical_tip();
    eyre::ensure!(
        (tip.number, tip.hash()) == (source.tip_number, source.tip_hash),
        "the clone opened at block {} {}, its source holds block {} {}",
        tip.number,
        tip.hash(),
        source.tip_number,
        source.tip_hash
    );
    let mut factory = node.factory(spec, knobs.run_workload(), knobs.output_config())?;
    let updates = EngineUpdates::spawn();
    let warmup = factory.generate(knobs.warmup);
    run_outputs(&node, &updates, &recorder, warmup, &mut Timed::default()).await?;
    let mut side_load = (knobs.readers > 0)
        .then(|| SideLoad::start(methods.clone(), pool.clone(), spec, &knobs))
        .transpose()?;
    let mut timed = Timed { pace: knobs.pace(), side: side_load.as_mut(), ..Timed::default() };
    let mut rows = Vec::with_capacity(knobs.outputs);
    for (count, closes) in segments(knobs.outputs, knobs.epoch_every) {
        let mut outputs = factory.generate(count - usize::from(closes));
        if closes {
            outputs.push(factory.next_output_closing()?);
        }
        if timed.next == 0 {
            sizes.before_timed = Sizes::of(&datadir)?;
            if knobs.export {
                timed.export = Some(Export::start(&node.env, &datadir, tmp.path().join("export"))?);
            }
        }
        rows.extend(run_outputs(&node, &updates, &recorder, outputs, &mut timed).await?);
        if closes {
            // the node's epoch boundary, untimed: clear the accumulator, enter the epoch, set its
            // committee
            node.gas_accumulator.clear();
            let committee = factory.begin_epoch(&node.env)?;
            node.gas_accumulator.rewards_counter().set_committee(committee.clone());
        }
    }
    let elapsed = timed.elapsed();
    let export = timed.export.take();
    let disk_error = timed.disk_error.take();
    drop(timed);
    let side = side_load.map(SideLoad::finish).transpose()?;
    sizes.after_timed = Sizes::of(&datadir)?;
    // the export holds a read on the database: it ends before the env closes. A failure is
    // reported, then returned
    let (export, export_error) = match export.map(Export::finish) {
        None => (None, None),
        Some(Ok(run)) => (Some(Ok(run)), None),
        Some(Err(e)) => (Some(Err(format!("{e:#}"))), Some(e)),
    };
    let sent = updates.close().await?;
    eyre::ensure!(
        sent == knobs.warmup + knobs.outputs,
        "{sent} engine updates for {} outputs",
        knobs.warmup + knobs.outputs
    );
    // the pool and the RPC methods hold the database too
    drop((methods, pool));
    let closed = node.close().await;
    if let Some(upkeep) = upkeep {
        upkeep.abort();
    }
    sizes.closed = Sizes::of(&datadir)?;
    // the database is closed: the tndb-b build's table stats refuse an open one. A failure is
    // reported, then returned, so the run's figures are not lost
    let closed_stats = match closed {
        Ok(()) => closed_table_stats(&datadir.join("db")).await,
        Err(e) => Err(e),
    };
    let (tables, tables_error) = match closed_stats {
        Ok(stats) => (Ok(tables_by_size(stats)), None),
        Err(e) => (Err(format!("{e:#}")), Some(e)),
    };
    drop(tmp);
    let footprint =
        Footprint { sizes, baseline_table_bytes, executed: knobs.warmup + knobs.outputs, tables };

    let date = command_line("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"]).unwrap_or_default();
    let header = Header {
        knobs,
        page_size: effective_page_size,
        host: host(),
        date,
        base: base_line,
        aged: aged_line,
        elapsed,
        export,
        disk_error,
    };
    print!("{}", report(&header, &rows, &footprint, side.as_ref()));
    if let Some((dir, name)) = report_target {
        write_json(&dir, &name, &json_report(&header, &rows, &footprint, side.as_ref()))?;
        println!("json report: {}", dir.join(name).display());
    }
    if let Some(e) = tables_error {
        return Err(e.wrap_err("closing the clone and reading its table stats"));
    }
    if let Some(side) = &side {
        eyre::ensure!(
            side.errors() == 0,
            "the side load had {} errors (reader and submitter errors, degraded polls, calls in \
             off windows): see the report",
            side.errors()
        );
    }
    if let Some(e) = export_error {
        return Err(e.wrap_err("the export beside the timed loop"));
    }
    Ok(())
}

mod tests {
    use super::*;
    use std::collections::HashSet;
    use tn_reth::test_utils::load::workload::{LoadKind, Mix};
    use tn_types::{ExecHeader, U256};

    /// The knob variables for `pairs` of knob name (after the prefix) and value.
    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(knob, value)| (format!("{KNOB_PREFIX}{knob}"), value.to_string()))
            .collect()
    }

    /// The defaults.
    fn defaults() -> Knobs {
        Knobs::parse(Vec::new(), "mdbx").expect("the defaults parse")
    }

    /// With no knob set, every knob takes its documented default; variables without the prefix
    /// are ignored.
    #[test]
    fn test_knobs_default() {
        let knobs = Knobs::parse(vec![("PATH".to_owned(), "/bin".to_owned())], "mdbx").unwrap();
        let expected = Knobs {
            outputs: 300,
            batches: 8,
            gas: 15_000_000,
            warmup: 20,
            aging: 200,
            prefill: 1_000_000,
            senders: 10_000,
            pools: 64,
            seed: 0,
            registry: true,
            pace_ms: 0,
            metrics: MetricsMode::Off,
            sync: SyncMode::Durable,
            page_size: None,
            readers: 0,
            submit_fraction: 0.25,
            window: 20,
            epoch_every: 0,
            export: false,
            cache_dir: default_cache_dir(),
            report: None,
        };
        assert_eq!(knobs, expected);
        assert!(knobs.cache_dir.ends_with("target/exec-bench-cache"));
        let workspace = knobs.cache_dir.ancestors().nth(2).unwrap();
        assert!(workspace.join("crates/engine/Cargo.toml").exists(), "{}", workspace.display());
        assert_eq!(knobs.pace(), None);
        assert_eq!(knobs.sync.db_flags(), ["--db.sync-mode", "durable"]);
        assert_eq!(knobs.page_size(), default_page_size(), "the opener's default page size");
        assert!(knobs.line().contains(" sync=durable page_size=default "), "{}", knobs.line());
    }

    /// `PAGE_SIZE` takes a power of two from 4096 to 65536 bytes and refuses anything else by
    /// name, with the range; the flags carry it to the database as `--db.page-size`.
    #[test]
    fn test_knobs_page_size() {
        for bytes in [4096, 16_384, 65_536] {
            let knobs = Knobs::parse(vars(&[("PAGE_SIZE", &bytes.to_string())]), "mdbx").unwrap();
            assert_eq!((knobs.page_size, knobs.page_size()), (Some(bytes), bytes));
            assert!(knobs.line().contains(&format!(" page_size={bytes} ")), "{}", knobs.line());
        }
        for value in ["2048", "131072", "12288", "16k", "", "-4096", "0"] {
            let err =
                Knobs::parse(vars(&[("PAGE_SIZE", value)]), "tndb-b").unwrap_err().to_string();
            let needle = format!(
                "TN_EXEC_BENCH_PAGE_SIZE={value:?} is not a page size: a power of two from 4096 \
                 to 65536 bytes"
            );
            assert!(err.contains(&needle), "{value:?}: {err}");
        }
        assert_eq!(
            db_flags(SyncMode::Durable, "16384"),
            ["--db.sync-mode", "durable", "--db.page-size", "16384"]
        );
        assert_eq!(
            db_flags(SyncMode::NoSync, "4096"),
            ["--db.sync-mode", "safe-no-sync", "--db.page-size", "4096"]
        );
    }

    /// Every knob is read from its variable and shapes the spec, the outputs and the workloads.
    #[test]
    fn test_knobs_set() {
        let knobs = Knobs::parse(
            vars(&[
                ("OUTPUTS", "5"),
                ("BATCHES", "2"),
                ("GAS", "3000000"),
                ("WARMUP", "0"),
                ("AGING", "0"),
                ("PREFILL", "10000"),
                ("SENDERS", "100"),
                ("POOLS", "4"),
                ("SEED", "9"),
                ("REGISTRY", "0"),
                ("PACE_MS", "250"),
                ("METRICS", "prom"),
                ("SYNC", "nosync"),
                ("READERS", "3"),
                ("SUBMIT_FRACTION", "0.5"),
                ("WINDOW", "7"),
                ("CACHE_DIR", "/some/cache"),
                ("REPORT", "/some/report.json"),
            ]),
            "mdbx",
        )
        .unwrap();
        assert_eq!((knobs.outputs, knobs.warmup, knobs.aging), (5, 0, 0));
        assert_eq!(
            knobs.spec(),
            PrefillSpec { accounts: 10_000, senders: 100, pools: 4, seed: 9, registry: false }
        );
        assert_eq!(
            knobs.output_config(),
            OutputConfig { batches: 2, batch_gas: 3_000_000, ..OutputConfig::default() }
        );
        assert_eq!(knobs.aging().workload.seed, 9);
        assert_eq!(knobs.run_workload().seed, 10, "the run does not replay the aging's draws");
        assert_eq!(knobs.pace(), Some(Duration::from_millis(250)));
        assert_eq!(knobs.metrics, MetricsMode::Prom);
        assert_eq!(knobs.sync.db_flags(), ["--db.sync-mode", "safe-no-sync"]);
        assert_eq!(knobs.cache_dir, PathBuf::from("/some/cache"));
        assert_eq!(knobs.report, Some(PathBuf::from("/some/report.json")));
        assert_eq!((knobs.readers, knobs.submit_fraction, knobs.window), (3, 0.5, 7));
        assert!(
            knobs.line().contains(" readers=3 submit_fraction=0.5 window=7 "),
            "{}",
            knobs.line()
        );
        assert!(knobs.line().ends_with(" report=/some/report.json"), "{}", knobs.line());
        let edges =
            Knobs::parse(vars(&[("SUBMIT_FRACTION", "0"), ("WINDOW", "1")]), "mdbx").unwrap();
        assert_eq!((edges.submit_fraction, edges.window), (0.0, 1));
        assert_eq!(
            Knobs::parse(vars(&[("SUBMIT_FRACTION", "1")]), "mdbx").unwrap().submit_fraction,
            1.0
        );
    }

    /// A malformed value, an unknown knob and `SYNC=nosync` on tndb-b are refused, each by name.
    #[test]
    fn test_knobs_refused() {
        let cases: [(&str, &str, &str, &str); 20] = [
            ("OUTPUTS", "many", "mdbx", "OUTPUTS=\"many\" is not a whole number"),
            ("OUTPUTS", "0", "mdbx", "OUTPUTS=0 is below 1"),
            ("BATCHES", "0", "mdbx", "BATCHES=0 is below 1"),
            ("GAS", "-1", "mdbx", "GAS=\"-1\" is not a whole number"),
            ("PREFILL", "1e6", "mdbx", "PREFILL=\"1e6\""),
            ("REGISTRY", "true", "mdbx", "REGISTRY=\"true\" is not one of 1, 0"),
            ("METRICS", "on", "mdbx", "METRICS=\"on\" is not one of off, prom"),
            ("SYNC", "fast", "mdbx", "SYNC=\"fast\" is not one of durable, nosync"),
            ("SYNC", "nosync", "tndb-b", "the tndb-b build refuses"),
            ("CACHE_DIR", "", "mdbx", "CACHE_DIR is empty"),
            ("REPORT", "", "mdbx", "REPORT is empty"),
            ("READERS", "-1", "mdbx", "READERS=\"-1\" is not a whole number"),
            ("SUBMIT_FRACTION", "a lot", "mdbx", "SUBMIT_FRACTION=\"a lot\" is not a number"),
            ("SUBMIT_FRACTION", "1.5", "mdbx", "SUBMIT_FRACTION=\"1.5\" is not between 0 and 1"),
            ("SUBMIT_FRACTION", "-0.1", "mdbx", "SUBMIT_FRACTION=\"-0.1\" is not between 0 and 1"),
            ("SUBMIT_FRACTION", "NaN", "mdbx", "SUBMIT_FRACTION=\"NaN\" is not between 0 and 1"),
            ("WINDOW", "0", "mdbx", "WINDOW=0 is below 1"),
            ("EPOCH_EVERY", "often", "mdbx", "EPOCH_EVERY=\"often\" is not a whole number"),
            ("EXPORT", "2", "mdbx", "EXPORT=\"2\" is not one of 0, 1"),
            ("READER", "1", "mdbx", "unknown knob TN_EXEC_BENCH_READER"),
        ];
        for (knob, value, backend, needle) in cases {
            let err = Knobs::parse(vars(&[(knob, value)]), backend).unwrap_err().to_string();
            assert!(err.contains(needle), "{knob}={value:?} on {backend}: {err}");
        }
        assert_eq!(Knobs::parse(vars(&[("SYNC", "durable")]), "tndb-b").unwrap(), defaults());
    }

    /// `EPOCH_EVERY` and `EXPORT` are read and shown in the knob line; `EPOCH_EVERY` above 0 needs
    /// the registry.
    #[test]
    fn test_knobs_epoch_every_and_export() {
        let knobs = Knobs::parse(vars(&[("EPOCH_EVERY", "3"), ("EXPORT", "1")]), "tndb-b").unwrap();
        assert_eq!((knobs.epoch_every, knobs.export), (3, true));
        assert!(
            knobs.line().contains(" window=20 epoch_every=3 export=1 cache_dir="),
            "{}",
            knobs.line()
        );
        let err = Knobs::parse(vars(&[("EPOCH_EVERY", "2"), ("REGISTRY", "0")]), "mdbx")
            .unwrap_err()
            .to_string();
        let needle = "EPOCH_EVERY=2 closes epochs through the ConsensusRegistry: it needs \
                      TN_EXEC_BENCH_REGISTRY=1";
        assert!(err.contains(needle), "{err}");
        let plain = Knobs::parse(vars(&[("EPOCH_EVERY", "0"), ("REGISTRY", "0")]), "mdbx").unwrap();
        assert_eq!((plain.epoch_every, plain.registry), (0, false));
        assert_eq!(Knobs::parse(vars(&[("EXPORT", "0")]), "mdbx").unwrap(), defaults());
    }

    /// The segments hold every timed output once, and each closes on its `EPOCH_EVERY`-th output
    /// but a last shorter one; without `EPOCH_EVERY` one segment holds them all.
    #[test]
    fn test_segments() {
        assert_eq!(segments(8, 3), [(3, true), (3, true), (2, false)]);
        assert_eq!(segments(6, 3), [(3, true), (3, true)]);
        assert_eq!(segments(3, 1), [(1, true); 3]);
        assert_eq!(segments(2, 5), [(2, false)]);
        assert_eq!(segments(5, 0), [(5, false)]);
        // the timed indices that close
        let closing = |outputs: usize, every: usize| {
            let (mut next, mut closing) = (0, Vec::new());
            for (count, closes) in segments(outputs, every) {
                next += count;
                if closes {
                    closing.push(next - 1);
                }
            }
            assert_eq!(next, outputs, "{outputs} every {every}: each output once");
            closing
        };
        assert_eq!(closing(8, 3), [2, 5]);
        assert_eq!(closing(300, 100), [99, 199, 299]);
        assert_eq!(closing(7, 0), Vec::<usize>::new());
        for outputs in 1..20 {
            for every in 1..8 {
                let closing = closing(outputs, every);
                assert_eq!(closing.len(), outputs / every, "{outputs} every {every}");
                assert!(closing.iter().all(|index| (index + 1) % every == 0), "{closing:?}");
            }
        }
    }

    /// The timed loop's clock leaves out the time between segments: resuming moves its origin on
    /// by the pause, and the first segment has none.
    #[test]
    fn test_timed_clock() {
        let now = Instant::now();
        let ago = |secs| now.checked_sub(Duration::from_secs(secs)).unwrap();
        let mut timed = Timed { t0: Some(ago(10)), paused: Some(ago(4)), ..Timed::default() };
        assert_eq!(timed.elapsed(), Duration::from_secs(6));
        timed.resume();
        assert_eq!(timed.paused, None);
        assert_eq!(timed.elapsed(), Duration::ZERO, "a segment runs");
        timed.paused = Some(Instant::now());
        let elapsed = timed.elapsed();
        assert!(elapsed >= Duration::from_secs(6), "{elapsed:?}");
        assert!(elapsed < Duration::from_secs(7), "{elapsed:?}");
        let mut first = Timed::default();
        first.resume();
        assert_eq!((first.t0, first.paused), (None, None));
    }

    /// The export overlaps an output it ran at any point of: it started before the output ended
    /// and did not end before the output started; while it runs, every later output.
    #[test]
    fn test_export_overlaps() {
        let t = Instant::now();
        let at = |ms| t + Duration::from_millis(ms);
        let span = (at(10), at(20));
        assert!(overlaps(span, at(0), None), "running");
        assert!(overlaps(span, at(0), Some(at(15))), "ended during it");
        assert!(overlaps(span, at(15), Some(at(30))), "started during it");
        assert!(overlaps(span, at(12), Some(at(18))), "within it");
        assert!(overlaps(span, at(0), Some(at(30))), "around it");
        assert!(!overlaps(span, at(0), Some(at(10))), "ended as it started");
        assert!(!overlaps(span, at(0), Some(at(5))), "ended before it");
        assert!(!overlaps(span, at(20), None), "started as it ended");
        assert!(!overlaps(span, at(25), None), "started after it");
    }

    /// An export marks the outputs it ran during, by its start and, once set, its end; `finish`
    /// returns its thread's error.
    #[test]
    fn test_export_active_during() {
        let started = Instant::now();
        let export = Export {
            started,
            ended: Arc::new(OnceLock::new()),
            thread: std::thread::spawn(|| Err(eyre::eyre!("no pack"))),
        };
        let at = |ms| started + Duration::from_millis(ms);
        assert!(export.active_during(at(5), at(6)), "running");
        assert!(export.active_during(at(50), at(60)), "running");
        export.ended.set(at(7)).unwrap();
        assert!(export.active_during(at(5), at(6)), "ended after it");
        assert!(export.active_during(at(6), at(8)), "ended during it");
        assert!(!export.active_during(at(50), at(60)), "ended before it");
        assert_eq!(export.finish().unwrap_err().to_string(), "no pack");
    }

    /// Rows of 100, 300 and 500 ms, the last closing an epoch, the second with the side load on.
    fn close_rows() -> [OutputRow; 3] {
        let mut rows = [
            row(1, 8, 0.1, 1, 1, false),
            row(9, 16, 0.3, 1, 1, false),
            row(17, 24, 0.5, 1, 1, false),
        ];
        rows[2].epoch_close = true;
        rows[1].side_load = true;
        rows
    }

    /// The words of `report`'s first line that starts with `prefix`.
    fn words<'a>(report: &'a str, prefix: &str) -> Vec<&'a str> {
        let line = report.lines().find(|line| line.starts_with(prefix));
        line.unwrap_or_else(|| panic!("no {prefix:?} in {report}")).split_whitespace().collect()
    }

    /// The close section counts the closing outputs and splits the writer's stages over the normal
    /// and the closing outputs; one line without `EPOCH_EVERY`.
    #[test]
    fn test_close_report() {
        let rows = close_rows();
        let knobs = Knobs { epoch_every: 3, ..defaults() };
        let report = close_report(&knobs, &rows);
        let first =
            "epoch closes: 1 of 3 timed outputs, every 3 (each boundary after them untimed)\n";
        assert!(report.starts_with(first), "{report}");
        assert!(report.contains(" normal mean ") && report.contains(" close p99 "), "{report}");
        assert!(report.contains("(ms per output; ratio = close / normal)"), "{report}");
        let total = [
            "total", "200.000", "100.000", "300.000", "500.000", "500.000", "500.000", "2.500",
            "5.000", "1.667",
        ];
        assert_eq!(words(&report, "  total "), total);
        for key in SPLIT_KEYS {
            let label = STAGES.iter().find(|(_, stage, _)| *stage == key).unwrap().0.trim();
            assert!(report.contains(&format!("\n  {label:<10} ")), "{key}\n{report}");
        }
        assert_eq!(close_report(&defaults(), &rows), "epoch closes: none (EPOCH_EVERY=0)\n");
        let full = report_text(&header(knobs), &rows);
        assert!(full.contains(&format!("\n{first}")), "{full}");
    }

    /// An export: block 24 pinned for 1.5 s, the database 2 MiB larger allocated and 1 MiB longer
    /// when the pack finished.
    fn export_run() -> ExportRun {
        let at = |db_allocated_bytes: u64, db_len_bytes: u64| Sizes {
            db_allocated_bytes,
            db_len_bytes,
            ..Sizes::default()
        };
        ExportRun {
            block: 24,
            duration: Duration::from_millis(1500),
            stats: ExecStateStats { account_count: 10, storage_slots: 20, bytecodes: 3 },
            pack_len_bytes: 3 << 20,
            pack_allocated_bytes: 4 << 20,
            at_pin: at(8 << 20, 16 << 20),
            at_finish: at(10 << 20, 17 << 20),
        }
    }

    /// `close_rows` with the export active during the first two.
    fn export_rows() -> [OutputRow; 3] {
        let mut rows = close_rows();
        rows[0].export_active = true;
        rows[1].export_active = true;
        rows
    }

    /// The export section gives the block, the duration, the stats, the pack, the outputs it
    /// overlapped and the database's growth while pinned, then the writer idle and active; a
    /// failed export gives its error; one line without `EXPORT`.
    #[test]
    fn test_export_report() {
        let rows = export_rows();
        let report = export_report(Some(&Ok(export_run())), &rows);
        let first =
            "export: block 24 in 1.500 s (pin to pack finish), accounts=10 storage_slots=20 \
                     bytecodes=3, pack 3.0 MiB; 2 of 3 timed outputs overlapped\n";
        assert!(report.starts_with(first), "{report}");
        let pinned =
            "\n  db while pinned (MiB): allocated 8.0 -> 10.0 (+2.0), len 16.0 -> 17.0 (+1.0)\n";
        assert!(report.contains(pinned), "{report}");
        assert!(report.contains("(ms per output; ratio = active / idle)"), "{report}");
        let total = [
            "total", "500.000", "500.000", "500.000", "200.000", "100.000", "300.000", "0.400",
            "0.200", "0.600",
        ];
        assert_eq!(words(&report, "  total "), total);
        let failed = export_report(Some(&Err("no pack".to_owned())), &rows);
        let first = "export: FAILED: no pack; 2 of 3 timed outputs overlapped\n";
        assert!(failed.starts_with(first), "{failed}");
        assert_eq!(words(&failed, "  total "), total);
        assert_eq!(export_report(None, &rows), "export: off (EXPORT=0)\n");
        assert_eq!(export_run().db_growth(), (2 << 20, 1 << 20));
        let shrank = ExportRun {
            at_pin: export_run().at_finish,
            at_finish: export_run().at_pin,
            ..export_run()
        };
        assert_eq!(shrank.db_growth(), (-(2 << 20), -(1 << 20)));
        let header = Header { export: Some(Ok(export_run())), ..header(defaults()) };
        let full = report_text(&header, &rows);
        assert!(full.contains(&format!("\n{}", &report)), "{full}");
    }

    /// The text report of `rows` under `header`, with the test footprint and no side load.
    fn report_text(header: &Header, rows: &[OutputRow]) -> String {
        report(header, rows, &footprint(), None)
    }

    /// Report version 3: each row says whether it closed an epoch and whether the export was
    /// active; `epoch_closes` and `export` are null without their knobs, and otherwise hold the
    /// close count and split, and the export's figures and split, null but for the error, the
    /// overlap and the split when it failed.
    #[test]
    fn test_json_closes_and_export() {
        use serde_json::Value;

        let rows = export_rows();
        let plain = json_report(&header(defaults()), &rows, &footprint(), None);
        assert_eq!(plain["report_version"], 4);
        assert_eq!(plain["epoch_closes"], Value::Null);
        assert_eq!(plain["export"], Value::Null);
        assert_eq!(plain["header"]["knobs"]["epoch_every"], 0);
        assert_eq!(plain["header"]["knobs"]["export"], false);
        let flags = |json: &Value, key: &str| -> Vec<Value> {
            json["rows"].as_array().unwrap().iter().map(|row| row[key].clone()).collect()
        };
        assert_eq!(flags(&plain, "epoch_close"), [false, false, true]);
        assert_eq!(flags(&plain, "export_active"), [true, true, false]);

        let knobs = Knobs { epoch_every: 3, export: true, ..defaults() };
        let header = Header { export: Some(Ok(export_run())), ..header(knobs.clone()) };
        let text = serde_json::to_string(&json_report(&header, &rows, &footprint(), None)).unwrap();
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["header"]["knobs"]["epoch_every"], 3);
        assert_eq!(json["header"]["knobs"]["export"], true);
        let closes = &json["epoch_closes"];
        assert_eq!(closes["closes"], 1);
        assert_eq!(closes["normal_outputs"], 2);
        assert_eq!(closes["writer"]["total"]["normal"]["mean_ms"], 200.0);
        assert_eq!(closes["writer"]["total"]["closing"]["p99_ms"], 500.0);
        assert_eq!(closes["writer"]["total"]["mean_ratio"], 2.5);
        for key in SPLIT_KEYS {
            assert!(closes["writer"][key]["normal"].is_object(), "{key}");
            assert!(json["export"]["writer"][key]["idle"].is_object(), "{key}");
        }
        let export = &json["export"];
        assert_eq!(export["failed"], false);
        assert_eq!(export["error"], Value::Null);
        assert_eq!(export["block"], 24);
        assert_eq!(export["duration_s"], 1.5);
        assert_eq!(export["account_count"], 10);
        assert_eq!(export["storage_slots"], 20);
        assert_eq!(export["bytecodes"], 3);
        assert_eq!(export["pack_len_bytes"], 3 << 20);
        assert_eq!(export["pack_allocated_bytes"], 4 << 20);
        assert_eq!(export["at_pin"]["db_allocated_bytes"], 8 << 20);
        assert_eq!(export["at_finish"]["db_len_bytes"], 17 << 20);
        assert_eq!(export["db_allocated_growth_bytes"], 2 << 20);
        assert_eq!(export["db_len_growth_bytes"], 1 << 20);
        assert_eq!(export["overlapped_outputs"], 2);
        assert_eq!(export["writer"]["total"]["active"]["mean_ms"], 200.0);
        assert_eq!(export["writer"]["total"]["idle"]["mean_ms"], 500.0);
        assert_eq!(export["writer"]["total"]["mean_ratio"], 0.4);

        let failed = Header { export: Some(Err("no pack".to_owned())), ..self::header(knobs) };
        let json = json_report(&failed, &rows, &footprint(), None);
        assert_eq!(json["export"]["failed"], true);
        assert_eq!(json["export"]["error"], "no pack");
        assert_eq!(json["export"]["overlapped_outputs"], 2);
        assert!(json["export"]["writer"]["total"]["active"].is_object());
        for key in [
            "block",
            "duration_s",
            "account_count",
            "storage_slots",
            "bytecodes",
            "pack_len_bytes",
            "pack_allocated_bytes",
            "at_pin",
            "at_finish",
            "db_allocated_growth_bytes",
            "db_len_growth_bytes",
        ] {
            assert_eq!(json["export"][key], Value::Null, "{key}");
        }
    }

    /// Nearest-rank percentiles, the mean and the max, whatever the input order.
    #[test]
    fn test_stats() {
        let mut hundred: Vec<f64> = (1..=100).map(f64::from).collect();
        hundred.reverse();
        let expected = Stats { mean: 50.5, p50: 50.0, p90: 90.0, p99: 99.0, max: 100.0 };
        assert_eq!(Stats::of(&hundred), expected);
        let ten = [7.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 9.0, 10.0];
        let expected = Stats { mean: 5.5, p50: 5.0, p90: 9.0, p99: 10.0, max: 10.0 };
        assert_eq!(Stats::of(&ten), expected);
        let one = Stats { mean: 3.0, p50: 3.0, p90: 3.0, p99: 3.0, max: 3.0 };
        assert_eq!(Stats::of(&[3.0]), one);
    }

    /// The aged base's name changes with everything that shapes its state, and only with that.
    #[test]
    fn test_aged_extra_keys_the_aging() {
        let aging = defaults().aging();
        let extra = aging.extra().unwrap();
        assert!(extra.starts_with("aged200-"), "{extra}");
        assert_eq!(extra.len(), "aged200-".len() + 16);
        assert_eq!(Knobs { pace_ms: 5, warmup: 1, ..defaults() }.aging().extra().unwrap(), extra);

        let config = aging.output_config;
        let workload = aging.workload;
        let variants = [
            Aging { outputs: 201, ..aging },
            Aging { output_config: OutputConfig { batches: 7, ..config }, ..aging },
            Aging { output_config: OutputConfig { batch_gas: 1, ..config }, ..aging },
            Aging { output_config: OutputConfig { interval_ms: 999, ..config }, ..aging },
            Aging { output_config: OutputConfig { base_fee: 8, ..config }, ..aging },
            Aging { workload: WorkloadConfig { seed: 1, ..workload }, ..aging },
            Aging {
                workload: WorkloadConfig { mix: Mix::only(LoadKind::Native), ..workload },
                ..aging
            },
            Aging { workload: WorkloadConfig { zipf_exponent: 1.5, ..workload }, ..aging },
        ];
        let mut seen = HashSet::from([extra]);
        for variant in variants {
            assert!(seen.insert(variant.extra().unwrap()), "{variant:?} keys like another");
        }
    }

    /// A base meta for the aged-base checks.
    fn base_meta() -> BaseMeta {
        BaseMeta {
            layout_version: 1,
            backend: BACKEND.to_owned(),
            page_size: 4096,
            spec: defaults().spec(),
            slim_genesis_hash: B256::ZERO,
            genesis_hash: B256::ZERO,
            genesis_header: ExecHeader::default(),
            built_at: 7,
            build_ms: 1,
            genesis_ms: 1,
            init_ms: 1,
            db_len: 1,
            db_allocated: 1,
            static_files_len: 1,
            static_files_allocated: 1,
        }
    }

    /// An aged meta matches only the base build and the aging it records; each mismatch is
    /// refused by name and the dir is left for the caller to remove.
    #[test]
    fn test_aged_meta_check() {
        let aging = defaults().aging();
        let base = base_meta();
        let meta = AgedMeta {
            aged_version: AGED_VERSION,
            base: base.clone(),
            aging: 200,
            output_config: aging.output_text(),
            workload: aging.workload_text(),
            tip_number: 1_600,
            tip_hash: B256::ZERO,
            built_at: 9,
            build_ms: 1,
            db_len: 1,
            db_allocated: 1,
            static_files_len: 1,
            static_files_allocated: 1,
        };
        let dir = Path::new("/cache/aged");
        aging.check(&meta, &base, dir).unwrap();
        let rebuilt = BaseMeta { built_at: 8, ..base.clone() };
        let cases = [
            (AgedMeta { aged_version: AGED_VERSION + 1, ..meta.clone() }, "aged version"),
            (AgedMeta { base: rebuilt, ..meta.clone() }, "another base build"),
            (AgedMeta { aging: 199, ..meta.clone() }, "aging 199"),
            (AgedMeta { output_config: "other".to_owned(), ..meta.clone() }, "output config other"),
            (AgedMeta { workload: "other".to_owned(), ..meta.clone() }, "workload other"),
            (AgedMeta { tip_number: 1_599, ..meta.clone() }, "tip block 1599"),
        ];
        for (bad, needle) in cases {
            let err = aging.check(&bad, &base, dir).unwrap_err().to_string();
            assert!(err.contains(needle) && err.contains("Remove the dir"), "{err}");
        }
    }

    /// Distinct samples for each captured histogram: `index` milliseconds per block, or
    /// `index` centiseconds once per output.
    fn samples(name: &str, blocks: usize) -> Vec<f64> {
        let index = CAPTURED.iter().position(|(captured, _)| *captured == name).unwrap() + 1;
        match CAPTURED[index - 1].1 {
            Per::Block => vec![index as f64 * 1e-3; blocks],
            Per::Output => vec![index as f64 * 1e-2],
        }
    }

    /// One output's samples are summed per histogram, a histogram with the wrong sample count is
    /// refused by name, and `other` is the total less the six sequential stages.
    #[test]
    fn test_stage_sums_and_other() {
        let stages = stage_sums(|name| samples(name, 3), 3).unwrap();
        let row = OutputRow { total: 2.0, stages, ..OutputRow::default() };
        assert!((row.stage(EXECUTE) - 0.003).abs() < 1e-12);
        assert!((row.stage("tn_reth.block_state_root_seconds") - 0.015).abs() < 1e-12);
        assert!((row.stage(SAVE_BLOCKS) - 0.07).abs() < 1e-12);
        assert!((row.stage(COMMIT_MDBX) - 0.16).abs() < 1e-12);
        // execute 1, finish 3 and overlay extend 6 ms per block; save_blocks 7, commit sf 15 and
        // commit mdbx 16 centiseconds per output
        let expected = 2.0 - 3.0 * (0.001 + 0.003 + 0.006) - (0.07 + 0.15 + 0.16);
        assert!((row.other() - expected).abs() < 1e-12, "{} != {expected}", row.other());

        let short = |name: &str| {
            let mut samples = samples(name, 3);
            if name == FINISH {
                samples.pop();
            }
            samples
        };
        let err = stage_sums(short, 3).unwrap_err().to_string();
        assert!(err.contains(&format!("{FINISH} recorded 2 samples")), "{err}");
        let twice = |name: &str| {
            let mut samples = samples(name, 3);
            if name == COMMIT_SF {
                samples.push(0.0);
            }
            samples
        };
        let err = stage_sums(twice, 3).unwrap_err().to_string();
        assert!(err.contains(&format!("{COMMIT_SF} recorded 2 samples")), "{err}");
    }

    /// A timed row of blocks `first..=last`.
    fn row(first: u64, last: u64, total: f64, gas: u64, txs: usize, late: bool) -> OutputRow {
        OutputRow {
            first_block: first,
            last_block: last,
            total,
            gas,
            txs,
            late,
            ..OutputRow::default()
        }
    }

    /// Throughput is the total gas and transactions over the summed output time, with the median
    /// of each output's own gas rate.
    #[test]
    fn test_throughput() {
        let rows = [
            row(1, 8, 0.5, 50_000_000, 1_000, false),
            row(9, 16, 1.0, 50_000_000, 1_000, false),
            row(17, 24, 0.25, 25_000_000, 500, false),
        ];
        let throughput = Throughput::of(&rows);
        assert_eq!((throughput.gas, throughput.txs, throughput.blocks), (125_000_000, 2_500, 24));
        assert!((throughput.seconds - 1.75).abs() < 1e-12);
        assert!((throughput.mgas_per_s - 125.0 / 1.75).abs() < 1e-9);
        assert!((throughput.tx_per_s - 2_500.0 / 1.75).abs() < 1e-9);
        // per output 100, 50 and 100 Mgas/s
        assert!((throughput.p50_output_mgas_per_s - 100.0).abs() < 1e-9);
    }

    /// The first and the last third each take `len / 3` values in output order, whatever the
    /// middle holds; their ratios are last over first, with none for a first that is zero, and
    /// there are no thirds for fewer than three values.
    #[test]
    fn test_thirds() {
        let thirds = Thirds::of(&[1.0, 3.0, 100.0, 1_000.0, 100.0, 6.0, 8.0]).unwrap();
        let expected = Thirds {
            first_mean_ms: 2.0,
            first_p50_ms: 1.0,
            last_mean_ms: 7.0,
            last_p50_ms: 6.0,
            mean_ratio: Some(3.5),
            p50_ratio: Some(6.0),
        };
        assert_eq!(thirds, expected);
        let zero = Thirds::of(&[0.0, 9.0, 2.0]).unwrap();
        assert_eq!((zero.first_mean_ms, zero.last_mean_ms), (0.0, 2.0));
        assert_eq!((zero.mean_ratio, zero.p50_ratio), (None, None));
        assert_eq!(Thirds::of(&[1.0, 2.0]), None);
    }

    /// The sizes are the data file's and the static files' length and allocated bytes, each in
    /// its field; growth per output is the allocated bytes the timed loop added, over the outputs,
    /// negative when it shrank.
    #[test]
    fn test_sizes_and_growth() {
        let tmp = tempfile::tempdir().unwrap();
        let db = tmp.path().join("db");
        let static_files = tmp.path().join("static_files");
        std::fs::create_dir_all(static_files.join("sub")).unwrap();
        std::fs::create_dir(&db).unwrap();
        // the data file under both backends' names, so either build measures it
        for name in ["mdbx.dat", "tndb.dat"] {
            std::fs::write(db.join(name), vec![1; 10_000]).unwrap();
        }
        std::fs::write(static_files.join("headers"), vec![1; 3_000]).unwrap();
        std::fs::write(static_files.join("sub").join("receipts"), vec![1; 70_000]).unwrap();
        let sizes = Sizes::of(tmp.path()).unwrap();
        let (db_len, db_allocated) = db_file_bytes(&db).unwrap();
        let (static_files_len, static_files_allocated) = dir_bytes(&static_files).unwrap();
        assert_eq!((db_len, static_files_len), (10_000, 73_000));
        let expected = Sizes {
            db_len_bytes: db_len,
            db_allocated_bytes: db_allocated,
            static_files_len_bytes: static_files_len,
            static_files_allocated_bytes: static_files_allocated,
        };
        assert_eq!(sizes, expected);

        let at = |db_allocated_bytes, static_files_allocated_bytes| Sizes {
            db_allocated_bytes,
            static_files_allocated_bytes,
            ..Sizes::default()
        };
        let run = RunSizes {
            open: at(1, 1),
            before_timed: at(1_000, 500),
            after_timed: at(4_000, 200),
            closed: at(9, 9),
        };
        assert_eq!(run.growth_per_output(3), (1_000.0, -100.0));
        let keys: Vec<_> = run.points().iter().map(|(_, key, point)| (*key, *point)).collect();
        assert_eq!(
            keys,
            [
                ("open", run.open),
                ("before_timed", run.before_timed),
                ("after_timed", run.after_timed),
                ("closed", run.closed)
            ]
        );
    }

    /// A table stat for `name` of `entries` rows taking `size` bytes.
    fn table_stat(name: &'static str, entries: u64, size: u64) -> DbTableStat {
        DbTableStat {
            name,
            entries,
            branch_pages: 1,
            leaf_pages: 2,
            overflow_pages: 3,
            total_size: size,
        }
    }

    /// Tables are listed largest first, equal sizes by name, each with its own figures.
    #[test]
    fn test_tables_by_size() {
        let tables = tables_by_size(vec![
            table_stat("Bodies", 5, 100),
            table_stat("Storage", 7, 300),
            table_stat("Accounts", 6, 100),
            table_stat("Empty", 0, 0),
        ]);
        let order: Vec<_> = tables.iter().map(|table| (table.name, table.size_bytes)).collect();
        assert_eq!(order, [("Storage", 300), ("Accounts", 100), ("Bodies", 100), ("Empty", 0)]);
        let expected = TableRow {
            name: "Storage",
            entries: 7,
            branch_pages: 1,
            leaf_pages: 2,
            overflow_pages: 3,
            size_bytes: 300,
        };
        assert_eq!(tables[0], expected);
    }

    /// A report header for `knobs`.
    fn header(knobs: Knobs) -> Header {
        Header {
            knobs,
            page_size: 16_384,
            host: "host (cpu)".to_owned(),
            date: "today".to_owned(),
            base: "base".to_owned(),
            aged: "aged".to_owned(),
            elapsed: Duration::from_secs(1),
            export: None,
            disk_error: None,
        }
    }

    /// A footprint whose timed loop grew the database by 3 MiB and the static files by 3 KiB,
    /// with two tables 3 MiB larger in all than before 3 executed outputs.
    fn footprint() -> Footprint {
        let at = |db_allocated_bytes, static_files_allocated_bytes| Sizes {
            db_len_bytes: 4 << 30,
            db_allocated_bytes,
            static_files_len_bytes: static_files_allocated_bytes,
            static_files_allocated_bytes,
        };
        Footprint {
            sizes: RunSizes {
                open: at(10 << 20, 1 << 20),
                before_timed: at(11 << 20, 2 << 20),
                after_timed: at(14 << 20, (2 << 20) + 3 * 1024),
                closed: at(14 << 20, (2 << 20) + 3 * 1024),
            },
            baseline_table_bytes: 4 << 20,
            executed: 3,
            tables: Ok(tables_by_size(vec![
                table_stat("PlainStorageState", 40, 6 << 20),
                table_stat("Headers", 24, 1 << 20),
            ])),
        }
    }

    /// The logical growth is the tables' total size from the baseline to the closed clone over
    /// every executed output, negative when the tables shrank, and unknown when the closed tables
    /// could not be read.
    #[test]
    fn test_logical_growth() {
        assert_eq!(footprint().logical_growth_per_output(), Some(f64::from(1 << 20)));
        let shrunk = Footprint { baseline_table_bytes: 10 << 20, executed: 2, ..footprint() };
        assert_eq!(shrunk.logical_growth_per_output(), Some(-1.5 * f64::from(1 << 20)));
        let failed = Footprint { tables: Err("no stats".to_owned()), ..footprint() };
        assert_eq!(failed.logical_growth_per_output(), None);
    }

    /// The side load is off for the first window of timed outputs, on for the next, and so on;
    /// the submitted transactions are `floor(count * fraction)`, spread evenly.
    #[test]
    fn test_side_load_windows() {
        let on = |outputs: usize, window: usize| -> Vec<bool> {
            (0..outputs).map(|index| side_load_on(index, window)).collect()
        };
        assert_eq!(on(7, 2), [false, false, true, true, false, false, true]);
        assert_eq!(on(4, 1), [false, true, false, true]);
        assert_eq!(on(3, 20), [false; 3]);
        assert_eq!(on(60, 20).iter().filter(|on| **on).count(), 20);
        let edges = [19, 20, 39, 40].map(|index| side_load_on(index, 20));
        assert_eq!(edges, [false, true, true, false]);

        let picked =
            |count: usize, fraction: f64| -> Vec<usize> { submitted(count, fraction).collect() };
        assert_eq!(picked(8, 0.25), [3, 7]);
        assert_eq!(picked(5, 0.5), [1, 3]);
        assert_eq!(picked(3, 1.0), [0, 1, 2]);
        assert_eq!(picked(3, 0.0), Vec::<usize>::new());
        assert_eq!(submitted(1_000, 0.25).count(), 250);
        assert_eq!(submitted(999, 0.1).count(), 99);
    }

    /// A kind's samples: `answered` latencies of `ms` each, split into ok and empty, with
    /// `errors` errors, the first `first`.
    fn kind(ms: u64, ok: u64, empty: u64, errors: u64, first: Option<&str>) -> KindSamples {
        KindSamples {
            latencies: vec![Duration::from_millis(ms); (ok + empty) as usize],
            ok,
            empty,
            errors,
            first_error: first.map(str::to_owned),
        }
    }

    /// A window's samples go to the on totals when it was on, kinds merged with the first error
    /// kept; calls in an off window are counted apart; a last poll missing or zeroing a polled
    /// address degrades its window; every one of those counts as an error, with the
    /// submitter's.
    #[test]
    fn test_side_samples() {
        let (a, b) = (Address::repeat_byte(1), Address::repeat_byte(2));
        let polls = |latencies: usize, balances: &[(Address, u64)]| PollSamples {
            latencies: vec![Duration::from_millis(3); latencies],
            last: balances.iter().map(|(address, b)| (*address, U256::from(*b))).collect(),
        };
        let empty = KindSamples::default();
        let mut side = SideSamples::default();
        let first = kind(2, 3, 1, 1, Some("first"));
        let window = [&first, &empty, &empty, &empty, &kind(5, 1, 0, 0, None)];
        side.add_window(
            true,
            Duration::from_secs(2),
            window,
            &polls(4, &[(a, 1), (b, 9)]),
            &[a, b],
        );
        let second = kind(4, 1, 0, 2, Some("second"));
        let window = [&second, &empty, &empty, &empty, &empty];
        side.add_window(true, Duration::from_secs(1), window, &polls(2, &[(a, 1)]), &[a, b]);
        assert_eq!(side.on_time, Duration::from_secs(3));
        let balance = &side.kinds[0];
        assert_eq!((balance.ok, balance.empty, balance.errors), (4, 1, 3));
        assert_eq!(balance.latencies.len(), 5);
        assert_eq!(balance.first_error.as_deref(), Some("first"));
        assert_eq!(side.kinds[4].latencies, [Duration::from_millis(5)]);
        assert_eq!(side.polls.len(), 6);
        assert_eq!((side.degraded_polls, side.off_calls), (1, 0), "b had no balance");
        assert_eq!(side.errors(), 3 + 1);

        // a zero balance degrades a window too; a window without polls is not judged
        side.add_window(true, Duration::ZERO, [&empty; 5], &polls(1, &[(a, 0), (b, 1)]), &[a, b]);
        side.add_window(true, Duration::ZERO, [&empty; 5], &polls(0, &[]), &[a, b]);
        assert_eq!(side.degraded_polls, 2);

        // an off window adds no samples: its calls only count against the gate
        let off = kind(1, 2, 1, 1, Some("off"));
        side.add_window(false, Duration::from_secs(5), [&off; 5], &polls(3, &[]), &[a, b]);
        assert_eq!(side.off_calls, 5 * 4 + 3);
        assert_eq!(side.on_time, Duration::from_secs(3));
        assert_eq!(side.kinds[0].ok, 4);
        side.submits.errors = 7;
        assert_eq!(side.errors(), 3 + 2 + 23 + 7);
    }

    /// Rows split by `side_load`: each split stage's stats over its side, on over off of the
    /// mean, p50 and p99, and no stats or ratios for a side without outputs.
    #[test]
    fn test_writer_split() {
        let mut rows = [
            row(1, 8, 0.1, 1, 1, false),
            row(9, 16, 0.3, 1, 1, false),
            row(17, 24, 0.4, 1, 1, false),
            row(25, 32, 0.8, 1, 1, false),
        ];
        rows[2].side_load = true;
        rows[3].side_load = true;
        let split = writer_split(&rows);
        let keys: Vec<_> = split.iter().map(|split| split.key).collect();
        assert_eq!(keys, SPLIT_KEYS);
        let labels: Vec<_> = split.iter().map(|split| split.label).collect();
        assert_eq!(labels[..4], ["total", "execute", "finish", "state root"]);
        let total = split[0];
        assert_eq!(total.off.map(|stats| stats.mean), Some(200.0));
        assert_eq!(
            total.on.map(|stats| (stats.mean, stats.p50, stats.p99)),
            Some((600.0, 400.0, 800.0))
        );
        assert_eq!(total.ratios(), [Some(3.0), Some(4.0), Some(800.0 / 300.0)]);
        assert_eq!(split[1].ratios(), [None; 3], "a zero off stage has no ratio");

        let off_only = writer_split(&rows[..2]);
        assert_eq!(off_only[0].on, None);
        assert_eq!(off_only[0].ratios(), [None; 3]);
    }

    /// The report labels the backend, sync, metrics and pacing, prints every stage row, counts
    /// late starts on a paced run, and adds the thirds, the sizes with growth per output, and the
    /// largest tables with a total.
    #[test]
    fn test_report_labels() {
        let header = header(Knobs { pace_ms: 250, ..defaults() });
        let rows = [
            row(1, 8, 0.5, 1, 1, false),
            row(9, 16, 0.5, 1, 1, true),
            row(17, 24, 0.5, 1, 1, true),
        ];
        let report = report(&header, &rows, &footprint(), None);
        let first = format!(
            "exec_bench backend={BACKEND} sync=durable metrics=off pacing=paced every 250 ms"
        );
        assert!(report.starts_with(&first), "{report}");
        assert!(report.contains("\n  page size: 16384 bytes (PAGE_SIZE=default)\n"), "{report}");
        for (label, _, _) in STAGES {
            let rows = report.lines().filter(|line| line.starts_with(&format!("{label} ")));
            assert_eq!(rows.count(), 2, "{label}: a stats row and a thirds row\n{report}");
        }
        assert!(report.contains("late starts: 2 of 3"), "{report}");
        assert!(report.contains("thirds: the first 1 and the last 1 timed outputs"), "{report}");
        let thirds_total = "total                         500.000    500.000   1.000    500.000";
        assert!(report.contains(thirds_total), "{report}");
        let before = "  before timed                   11.0     4096.0        2.0        2.0";
        assert!(report.contains(before), "{report}");
        assert!(report.contains("db 1024.0 KiB, static files 1.0 KiB"), "{report}");
        assert!(report.contains("\n  pool + RPC: on ("), "{report}");
        let logical =
            "growth per executed output (logical, table bytes): db 1024.0 KiB over 3 outputs";
        assert!(report.contains(logical), "{report}");
        assert!(report.ends_with("side load: off (READERS=0)\n"), "{report}");
        assert!(report.contains("\nepoch closes: none (EPOCH_EVERY=0)\n"), "{report}");
        assert!(report.contains("\nexport: off (EXPORT=0)\n"), "{report}");
        assert!(report.contains("(the 2 largest of 2 tables, closed clone)"), "{report}");
        assert!(
            report.contains("  PlainStorageState                    40        6.0"),
            "{report}"
        );
        assert!(
            report.contains("  total                                64        7.0"),
            "{report}"
        );
        let short = super::report(&header, &rows[..2], &footprint(), None);
        assert!(short.contains("thirds: none for fewer than 3 timed outputs"), "{short}");

        // a failed table read is reported, with the side load after it
        let failed = Footprint { tables: Err("the db is open".to_owned()), ..footprint() };
        let report = super::report(&header, &rows, &failed, Some(&side()));
        assert!(report
            .contains("table: FAILED reading the closed clone's table stats: the db is open\n"));
        assert!(
            report.contains("(logical, table bytes): unknown, the table stats failed"),
            "{report}"
        );
        assert!(!report.contains("PlainStorageState"), "{report}");
        assert!(report.contains("\nside load: 3 outputs off, 0 on"), "{report}");
    }

    /// Side-load samples: 2 s on, 4 balance reads (one an error), a submitter and a poller.
    fn side() -> SideSamples {
        let mut side = SideSamples { on_time: Duration::from_secs(2), ..SideSamples::default() };
        side.kinds[0] = kind(2, 2, 1, 1, Some("bad balance"));
        side.polls = vec![Duration::from_millis(1), Duration::from_millis(3)];
        side.submits = SubmitSamples {
            latencies: vec![Duration::from_millis(4); 5],
            accepted: 3,
            nonce_too_low: 1,
            already_known: 1,
            errors: 0,
            first_error: None,
        };
        side
    }

    /// Rows of 100 ms off, then 300 and 500 ms on.
    fn side_rows() -> [OutputRow; 3] {
        let mut rows = [
            row(1, 8, 0.1, 1, 1, false),
            row(9, 16, 0.3, 1, 1, false),
            row(17, 24, 0.5, 1, 1, false),
        ];
        rows[1].side_load = true;
        rows[2].side_load = true;
        rows
    }

    /// The side-load section splits the writer's stages off and on, then lists each reader kind,
    /// the submitter, the poller and the calls in off windows.
    #[test]
    fn test_side_report() {
        let report = side_report(&side_rows(), Some(&side()));
        assert!(report.starts_with("side load: 1 outputs off, 2 on, on for 2.000 s; errors 1\n"));
        let total = "  total        100.000   100.000   100.000   400.000   300.000   500.000   4.000   3.000   5.000";
        assert!(report.contains(total), "{report}");
        for key in SPLIT_KEYS {
            let label = STAGES.iter().find(|(_, stage, _)| *stage == key).unwrap().0.trim();
            assert!(report.contains(&format!("\n  {label:<10} ")), "{key}\n{report}");
        }
        let balance =
            "  balance            4       2.0     2.000     2.000         2       1       1";
        assert!(report.contains(balance), "{report}");
        assert!(report.contains("    first error (eth_getBalance): bad balance\n"), "{report}");
        let logs = "  logs               0       0.0         -         -         0       0       0";
        assert!(report.contains(logs), "{report}");
        let submitter =
            "submitter: accepted=3 nonce_too_low=1 already_known=1 errors=0 p50_ms=4.000";
        assert!(report.contains(submitter), "{report}");
        assert!(report.contains("poller: polls=2 p50_ms=1.000 p99_ms=3.000 degraded_windows=0"));
        assert!(report.ends_with("calls in off windows: 0\n"), "{report}");
    }

    /// The JSON report holds the header with every knob, one row per output with every stage in
    /// milliseconds, the stats, throughput and thirds per stage, the sizes with growth, and the
    /// tables, under stable snake_case names.
    #[test]
    fn test_json_report_shape() {
        let header = header(Knobs { report: Some(PathBuf::from("/r.json")), ..defaults() });
        let mut rows = [
            row(1, 8, 0.5, 40, 4, false),
            row(9, 16, 0.25, 40, 4, false),
            row(17, 24, 1.0, 40, 4, false),
        ];
        for (row, execute) in rows.iter_mut().zip([0.1, 0.2, 0.3]) {
            row.stages[0] = execute;
        }
        for (i, row) in (0_u64..).zip(rows.iter_mut()) {
            row.db_counters = vec![("tndb_pages_synced", 10 * i), ("tndb_commits", 1)];
            row.disk_bytes = (i != 2).then_some(4096 * i);
            row.db_file_len = (4 << 30) + (1 << 20) * i;
        }
        let text = serde_json::to_string(&json_report(&header, &rows, &footprint(), None)).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let at = |pointer: &str| {
            json.pointer(pointer).unwrap_or_else(|| panic!("no {pointer} in {json:#}")).clone()
        };
        assert_eq!(at("/report_version"), 4);
        assert_eq!(at("/header/backend"), BACKEND);
        assert_eq!(at("/header/sync"), "durable");
        assert_eq!(at("/header/page_size_bytes"), 16_384);
        assert_eq!(at("/header/disk_bytes_source"), serde_json::json!(DISK_BYTES_SOURCE));
        assert_eq!(at("/header/disk_bytes_error"), serde_json::Value::Null);
        let names = serde_json::json!(["tndb_pages_synced", "tndb_commits"]);
        assert_eq!(at("/header/db_counter_names"), names, "in the rows' order");
        let failed = Header { disk_error: Some("no io".to_owned()), ..self::header(defaults()) };
        let failed = json_report(&failed, &rows, &footprint(), None);
        assert_eq!(failed["header"]["disk_bytes_error"], "no io");
        assert_eq!(at("/header/knobs/page_size"), serde_json::Value::Null, "the default");
        assert_eq!(at("/header/pacing"), "back to back");
        assert_eq!(at("/header/host"), "host (cpu)");
        assert_eq!(at("/header/knobs/outputs"), 300);
        assert_eq!(at("/header/knobs/registry"), true);
        assert_eq!(at("/header/knobs/report"), "/r.json");
        assert_eq!(at("/header/pool_rpc"), "on");
        assert_eq!(at("/header/knobs/readers"), 0);
        assert_eq!(at("/header/knobs/submit_fraction"), 0.25);
        assert_eq!(at("/header/knobs/window"), 20);
        assert_eq!(at("/rows/1/side_load"), false);
        assert_eq!(at("/rows/1/epoch_close"), false);
        assert_eq!(at("/rows/1/export_active"), false);
        assert_eq!(at("/header/knobs/epoch_every"), 0);
        assert_eq!(at("/header/knobs/export"), false);
        assert_eq!(at("/rows").as_array().unwrap().len(), 3);
        assert_eq!(at("/rows/1/first_block"), 9);
        assert_eq!(at("/rows/1/total_ms"), 250.0);
        assert_eq!(at("/rows/1/execute_ms"), 200.0);
        assert_eq!(at("/rows/1/gas"), 40);
        assert_eq!(at("/rows/1/late"), false);
        assert_eq!(at("/rows/1/db_counters/tndb_commits"), 1);
        assert_eq!(at("/rows/1/db_counters/tndb_pages_synced"), 10);
        assert_eq!(at("/rows/1/db_counters").as_object().unwrap().len(), 2);
        assert_eq!(at("/rows/1/disk_bytes_written"), 4096);
        assert_eq!(at("/rows/2/disk_bytes_written"), serde_json::Value::Null, "a failed read");
        assert_eq!(at("/rows/2/db_file_len_bytes"), (4_u64 << 30) + (2 << 20));
        assert_eq!(at("/db_writes/counters_mean_per_output/tndb_commits"), 1.0);
        assert_eq!(at("/db_writes/counters_mean_per_output/tndb_pages_synced"), 10.0);
        assert_eq!(at("/db_writes/counters_max"), serde_json::json!({}));
        assert_eq!(at("/db_writes/disk_bytes_mean_per_output"), 2048.0);
        assert_eq!(at("/db_writes/disk_bytes_outputs"), 2);
        assert_eq!(at("/db_writes/db_file_len_before_bytes"), 4_u64 << 30, "before_timed's");
        assert_eq!(at("/db_writes/db_file_growth_bytes"), 2 << 20);
        assert_eq!(at("/db_writes/db_file_growth_per_output_bytes"), f64::from(2 << 20) / 3.0);
        assert_eq!(at("/db_writes/deciles"), serde_json::Value::Null, "fewer than 10 outputs");
        for (_, key, _) in STAGES {
            assert!(json["rows"][0][format!("{key}_ms")].is_f64(), "{key}_ms");
            assert!(json["stats"][key]["p99_ms"].is_f64(), "stats {key}");
            assert!(json["thirds"]["stages"][key].is_object(), "thirds {key}");
        }
        assert_eq!(at("/stats/total/mean_ms"), 1750.0 / 3.0);
        assert_eq!(at("/stats/total/max_ms"), 1000.0);
        assert_eq!(at("/stats/total/share_pct"), 100.0);
        assert_eq!(at("/throughput/gas"), 120);
        assert_eq!(at("/throughput/busy_s"), 1.75);
        assert_eq!(at("/throughput/late_starts"), 0);
        assert_eq!(at("/thirds/outputs_per_third"), 1);
        assert_eq!(at("/thirds/stages/execute/first_mean_ms"), 100.0);
        assert_eq!(at("/thirds/stages/execute/last_mean_ms"), 300.0);
        assert_eq!(at("/thirds/stages/total/mean_ratio"), 2.0);
        assert_eq!(at("/sizes/before_timed/db_allocated_bytes"), 11 << 20);
        assert_eq!(at("/sizes/closed/db_len_bytes"), 4_u64 << 30);
        assert_eq!(at("/sizes/db_growth_per_output_bytes"), f64::from(1 << 20));
        assert_eq!(at("/sizes/static_files_growth_per_output_bytes"), 1024.0);
        assert_eq!(at("/tables/count"), 2);
        assert_eq!(at("/tables/total_entries"), 64);
        assert_eq!(at("/tables/total_size_bytes"), 7 << 20);
        assert_eq!(at("/tables/by_size/0/name"), "PlainStorageState");
        assert_eq!(at("/tables/by_size/1/size_bytes"), 1 << 20);
        assert_eq!(at("/tables/failed"), false);
        assert_eq!(at("/tables/error"), serde_json::Value::Null);
        assert_eq!(at("/tables/baseline_total_size_bytes"), 4 << 20);
        assert_eq!(at("/sizes/db_logical_growth_per_output_bytes"), f64::from(1 << 20));
        assert_eq!(at("/side_load"), serde_json::Value::Null);
        assert_eq!(at("/epoch_closes"), serde_json::Value::Null);
        assert_eq!(at("/export"), serde_json::Value::Null);

        let two = json_report(&header, &rows[..2], &footprint(), None);
        assert_eq!(two["thirds"], serde_json::Value::Null);
        let set = self::header(Knobs { page_size: Some(4096), ..defaults() });
        let set = json_report(&set, &rows, &footprint(), None);
        assert_eq!(set["header"]["knobs"]["page_size"], 4096, "a set PAGE_SIZE is its bytes");

        // a failed table read keeps the section's keys, null, with the error
        let failed = Footprint { tables: Err("the db is open".to_owned()), ..footprint() };
        let json = json_report(&header, &rows, &failed, None);
        assert_eq!(json["tables"]["failed"], true);
        assert_eq!(json["tables"]["error"], "the db is open");
        assert_eq!(json["tables"]["baseline_total_size_bytes"], 4 << 20);
        for key in ["count", "total_entries", "total_size_bytes", "by_size"] {
            assert_eq!(json["tables"][key], serde_json::Value::Null, "{key}");
        }
        assert_eq!(json["sizes"]["db_logical_growth_per_output_bytes"], serde_json::Value::Null);
    }

    /// The JSON `side_load` object holds the writer split per split stage, each reader kind, the
    /// submitter, the poller and the error total, and each row says whether its side load was on.
    #[test]
    fn test_json_side_load_shape() {
        let header = header(Knobs { readers: 2, window: 1, ..defaults() });
        let json = json_report(&header, &side_rows(), &footprint(), Some(&side()));
        let at = |pointer: &str| {
            json.pointer(pointer).unwrap_or_else(|| panic!("no {pointer} in {json:#}")).clone()
        };
        assert_eq!(at("/header/knobs/readers"), 2);
        assert_eq!(at("/rows/0/side_load"), false);
        assert_eq!(at("/rows/2/side_load"), true);
        assert_eq!(at("/side_load/off_outputs"), 1);
        assert_eq!(at("/side_load/on_outputs"), 2);
        assert_eq!(at("/side_load/on_time_s"), 2.0);
        assert_eq!(at("/side_load/errors"), 1);
        for key in SPLIT_KEYS {
            for field in ["off", "on", "mean_ratio", "p50_ratio", "p99_ratio"] {
                assert!(json["side_load"]["writer"][key].get(field).is_some(), "{key} {field}");
            }
        }
        assert_eq!(at("/side_load/writer/total/off/mean_ms"), 100.0);
        assert_eq!(at("/side_load/writer/total/on/p99_ms"), 500.0);
        assert_eq!(at("/side_load/writer/total/mean_ratio"), 4.0);
        assert_eq!(at("/side_load/writer/execute/p50_ratio"), serde_json::Value::Null);
        for kind in ReadKind::ALL {
            let fields = &json["side_load"]["readers"][kind_key(kind)];
            assert_eq!(fields["method"], kind.method());
            for field in ["ops", "ops_per_s", "p50_ms", "p99_ms", "ok", "empty", "errors"] {
                assert!(fields.get(field).is_some(), "{kind:?} {field}");
            }
        }
        assert_eq!(at("/side_load/readers/balance/ops"), 4);
        assert_eq!(at("/side_load/readers/balance/ops_per_s"), 2.0);
        assert_eq!(at("/side_load/readers/balance/p99_ms"), 2.0);
        assert_eq!(at("/side_load/readers/balance/first_error"), "bad balance");
        assert_eq!(at("/side_load/readers/logs/p50_ms"), serde_json::Value::Null);
        assert_eq!(at("/side_load/submitter/accepted"), 3);
        assert_eq!(at("/side_load/submitter/nonce_too_low"), 1);
        assert_eq!(at("/side_load/submitter/already_known"), 1);
        assert_eq!(at("/side_load/submitter/errors"), 0);
        assert_eq!(at("/side_load/submitter/p50_ms"), 4.0);
        assert_eq!(at("/side_load/poller/polls"), 2);
        assert_eq!(at("/side_load/poller/p99_ms"), 3.0);
        assert_eq!(at("/side_load/poller/degraded_windows"), 0);
        assert_eq!(at("/side_load/off_window_calls"), 0);
    }

    /// A timed output for the database-writes summary: `counters`, the disk bytes, the data
    /// file's length after it, and its database commit in milliseconds.
    fn db_row(
        counters: Vec<(&'static str, u64)>,
        disk_bytes: Option<u64>,
        db_file_len: u64,
        commit_ms: f64,
    ) -> OutputRow {
        let mut row = OutputRow {
            db_counters: counters,
            disk_bytes,
            db_file_len,
            ..row(1, 8, 0.5, 1, 1, false)
        };
        let commit = CAPTURED.iter().position(|(name, _)| *name == COMMIT_MDBX).unwrap();
        row.stages[commit] = commit_ms / 1e3;
        row
    }

    /// The database-writes summary: each counter's mean over every output (a maximum's largest),
    /// whether it moved, the disk bytes' mean over the outputs that have them, the data file's
    /// growth from before the first output, and ten consecutive deciles whose sizes differ by at
    /// most one; none for fewer than ten outputs.
    #[test]
    fn test_db_writes_summary() {
        let rows: Vec<OutputRow> = (0_u64..25)
            .map(|i| {
                db_row(
                    vec![("pages", i + 1), ("tndb_max_gate_lag", i % 7), ("never", 0)],
                    (i != 3).then_some((i + 1) * 1000),
                    1000 + 10 * i,
                    (i + 1) as f64,
                )
            })
            .collect();
        let writes = DbWrites::of(&rows, 900);
        let summary =
            |name: &str| *writes.counters.iter().find(|counter| counter.name == name).unwrap();
        assert_eq!(writes.counters.len(), 3, "every counter, moved or not");
        let pages = summary("pages");
        assert_eq!((pages.value, pages.maximum, pages.moved), (13.0, false, true), "325 / 25");
        let lag = summary("tndb_max_gate_lag");
        assert_eq!((lag.value, lag.maximum, lag.moved), (6.0, true, true), "the largest");
        let never = summary("never");
        assert_eq!((never.value, never.moved), (0.0, false));
        assert_eq!(writes.disk_bytes_mean, Some(321_000.0 / 24.0), "over the 24 that have it");
        assert_eq!(writes.disk_outputs, 24);
        assert_eq!((writes.db_file_len_before, writes.db_file_len_after), (900, 1240));
        assert_eq!(writes.db_file_growth(), 340);

        let deciles = writes.deciles.as_ref().expect("25 outputs have deciles");
        let sizes: Vec<usize> = deciles.iter().map(|decile| decile.outputs).collect();
        assert_eq!(sizes, [2, 3, 2, 3, 2, 3, 2, 3, 2, 3], "boundaries at k * 25 / 10");
        let first = Decile {
            outputs: 2,
            commit_mdbx_mean_ms: 1.5,
            disk_bytes_mean: Some(1500.0),
            db_file_len_mean_bytes: 1005.0,
            db_file_len_end_bytes: 1010,
        };
        assert_eq!(deciles[0], first);
        let second = Decile {
            outputs: 3,
            commit_mdbx_mean_ms: 4.0,
            disk_bytes_mean: Some(4000.0),
            db_file_len_mean_bytes: 1030.0,
            db_file_len_end_bytes: 1040,
        };
        assert_eq!(deciles[1], second, "the output without disk bytes is left out of the mean");
        let last = Decile {
            outputs: 3,
            commit_mdbx_mean_ms: 24.0,
            disk_bytes_mean: Some(24_000.0),
            db_file_len_mean_bytes: 1230.0,
            db_file_len_end_bytes: 1240,
        };
        assert_eq!(deciles[9], last);

        assert_eq!(DbWrites::of(&rows[..10], 900).deciles.unwrap().len(), 10, "one output each");
        assert_eq!(DbWrites::of(&rows[..9], 900).deciles, None);
        let blind: Vec<OutputRow> =
            rows.iter().map(|row| OutputRow { disk_bytes: None, ..row.clone() }).collect();
        let blind = DbWrites::of(&blind, 900);
        assert_eq!((blind.disk_bytes_mean, blind.disk_outputs), (None, 0));
        assert_eq!(blind.deciles.unwrap()[0].disk_bytes_mean, None);
    }

    /// The text section prints the counters that moved, `_ns` ones in milliseconds and a maximum
    /// marked, leaves out the counters that never moved, and gives the disk bytes and the data
    /// file's growth; the deciles table needs ten outputs. The header names the disk bytes' source
    /// and the first failed read.
    #[test]
    fn test_db_writes_report() {
        let counters = |commits| {
            vec![
                ("tndb_commits", commits),
                ("tndb_save_ns", 2_500_000),
                ("tndb_verifies", 0),
                ("tndb_max_gate_lag", 3),
            ]
        };
        let rows: Vec<OutputRow> = (0_u64..3)
            .map(|i| db_row(counters(i), Some(1 << 20), (1 << 30) + (1 << 20) * i, 2.0))
            .collect();
        let text = db_writes_report(&DbWrites::of(&rows, (1 << 30) - (1 << 20)), rows.len());
        assert!(text.starts_with("database writes per output: "), "{text}");
        let line = |name: &str| text.lines().find(|line| line.trim_start().starts_with(name));
        assert!(line("tndb_commits").unwrap().ends_with(" 1.000"), "{text}");
        assert!(line("tndb_save_ns").unwrap().ends_with(" 2.500 ms"), "{text}");
        assert!(line("tndb_max_gate_lag").unwrap().ends_with(" 3 (max)"), "{text}");
        assert_eq!(line("tndb_verifies"), None, "never moved\n{text}");
        assert!(
            line("disk written").unwrap().contains(" 1.000 MiB per output (3 outputs read)"),
            "{text}"
        );
        let growth = line("db file growth").unwrap();
        assert!(growth.contains(" 1.000 MiB per output, 3.0 MiB over 3 outputs"), "{text}");
        assert!(text.ends_with("deciles: none for fewer than 10 timed outputs\n"), "{text}");

        let ten: Vec<OutputRow> =
            (0_u64..10).map(|i| db_row(counters(1), None, (1 << 20) * (i + 1), 2.0)).collect();
        let text = db_writes_report(&DbWrites::of(&ten, 0), ten.len());
        assert!(line_of(&text, "disk written").ends_with(" -"), "{text}");
        assert!(text.contains("\ndecile "), "{text}");
        let deciles: Vec<&str> =
            text.lines().skip_while(|line| !line.starts_with("decile ")).skip(1).collect();
        assert_eq!(deciles.len(), 10, "{text}");
        let last: Vec<&str> = deciles[9].split_whitespace().collect();
        assert_eq!(last, ["10", "1", "2.000", "-", "10.0"], "{text}");

        assert_eq!(disk_bytes_line(None, None), "  disk bytes: none (this target has no source)\n");
        assert!(disk_bytes_line(Some("/proc/self/io"), None)
            .starts_with("  disk bytes: /proc/self/io ("));
        let failed = disk_bytes_line(Some("proc_pid_rusage"), Some("EPERM"));
        assert!(failed.contains("a read FAILED") && failed.ends_with(": EPERM\n"), "{failed}");
        let header = header(defaults());
        let report = report(&header, &rows, &footprint(), None);
        assert!(report.contains(&disk_bytes_line(DISK_BYTES_SOURCE, None)), "{report}");
        assert!(report.contains("\ndatabase writes per output: "), "{report}");
    }

    /// The first line of `text` that starts, after its indent, with `name`.
    fn line_of<'a>(text: &'a str, name: &str) -> &'a str {
        text.lines()
            .find(|line| line.trim_start().starts_with(name))
            .unwrap_or_else(|| panic!("no {name} line in {text}"))
    }

    /// The process's disk bytes written read on macOS and Linux, and do not go down across a
    /// few MiB written and synced (no exact amount: macOS may attribute the writes later); a
    /// target without a source reads none. `/proc/<pid>/io`'s `write_bytes` field parses.
    #[test]
    fn test_disk_bytes_written() {
        let Some(_) = DISK_BYTES_SOURCE else {
            assert!(disk_bytes_written().is_err());
            return;
        };
        let before = disk_bytes_written().expect("this target has a source");
        let dir = tempfile::tempdir().unwrap();
        let mut file = File::create(dir.path().join("written")).unwrap();
        let chunk = vec![0xa5_u8; 1 << 20];
        for _ in 0..4 {
            file.write_all(&chunk).unwrap();
        }
        fsync_file(&file).unwrap();
        let after = disk_bytes_written().expect("a second read");
        assert!(after >= before, "{after} bytes written after {before}");

        let io = "rchar: 3239\nwchar: 3229\nsyscr: 63\nsyscw: 62\nread_bytes: 4096\n\
                  write_bytes: 323932160\ncancelled_write_bytes: 0\n";
        assert_eq!(proc_io_write_bytes(io), Ok(323_932_160));
        assert!(proc_io_write_bytes("read_bytes: 1\n").is_err());
        assert!(proc_io_write_bytes("write_bytes: lots\n").is_err());
    }

    /// The report path splits into an existing dir and a file name; a missing dir or no file
    /// name is refused.
    #[test]
    fn test_report_target() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("run.json");
        assert_eq!(
            report_target(&path).unwrap(),
            (tmp.path().to_path_buf(), "run.json".to_owned())
        );
        assert_eq!(
            report_target(Path::new("run.json")).unwrap(),
            (PathBuf::from("."), "run.json".to_owned())
        );
        let err = report_target(&tmp.path().join("missing/run.json")).unwrap_err().to_string();
        assert!(err.contains("is not a directory"), "{err}");
        let err = report_target(Path::new("/")).unwrap_err().to_string();
        assert!(err.contains("names no UTF-8 file name"), "{err}");
    }
}
