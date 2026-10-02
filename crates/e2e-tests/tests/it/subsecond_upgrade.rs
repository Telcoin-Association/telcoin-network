//! In-place node binary upgrades made before the sub-second timestamp fork epoch arrives.
//!
//! Validators reach the fork the way production will: they run a binary that predates the
//! sub-second change, restart on the new binary over their existing datadirs some epochs before
//! the fork epoch, and the network then crosses the fork on the new binary with history the old
//! one wrote. The older binary comes from `TN_BIN_PATH_PREV`
//! ([`e2e_tests::get_previous_telcoin_network_binary`]):
//!
//! | `TN_BIN_PATH_PREV` | result |
//! |---|---|
//! | unset | skip, with a `warn!` and an `eprintln!` naming `make test-e2e-subsecond-fork` |
//! | set, no file there | panic in `get_previous_telcoin_network_binary` |
//! | set | every assertion is hard |

use super::{
    common::{
        assert_blocks_match_consensus, assert_consensus_commit_times, assert_epoch_records_verify,
        assert_nodes_agree_on_commit_times, current_epoch, drive_light_tx_load,
        fetch_verified_epoch_record, kill_child, loop_epochs, pin_fork_epoch,
        pin_fork_epoch_override, read_consensus_headers, scrape_metric_value,
        start_validator_with_args, wait_for_epoch_at_least, wait_for_rpc, walk_block_commit_times,
        BlockCommitTime, ProcessGuard, EVM_TIMESTAMP_CLAMPED_SERIES,
        LEADER_SEEDED_ORDERING_FORK_ENV, MULTI_WORKERS_FORK_ENV, RPC_REQUEST_TIMEOUT,
        SEED_SIGNATURE_FORK_ENV, SUBSECOND_TIMESTAMP_FORK_ENV,
    },
    restarts::{wait_for_node_mode, wait_for_restart_catch_up},
};
use alloy::{
    primitives::utils::parse_ether,
    providers::{DynProvider, Provider, ProviderBuilder},
};
use e2e_tests::{
    config_local_testnet_with_epoch_duration, get_previous_telcoin_network_binary,
    get_telcoin_network_binary, NodeEndpoints, TestBinary,
};
use rand::{rngs::StdRng, SeedableRng as _};
use serde_json::Value;
use std::{
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
use tn_test_utils::wait_until;
use tn_types::{
    forks::{
        leader_seeded_ordering_fork_epoch_override, multi_workers_fork_active,
        seed_signature_active, subsecond_timestamp_fork_epoch_override,
    },
    get_available_tcp_port, Epoch, Genesis, GenesisAccount, NodeMode, U256,
};
use tokio::time::{sleep, timeout, Instant};
use tracing::{info, warn};

/// Epoch length of these runs, the 5 s cadence of the other fork tests in `epochs.rs`.
const EPOCH_DURATION: u64 = 5;

/// How many epochs past the one open when the fork is pinned the sub-second fork arms.
///
/// The fork has to arrive after the last in-place restart, and how many epochs the restarts take
/// is only known at run time, so the fork epoch is the open epoch plus this window. One restart
/// is a SIGTERM and exit (at most 6 s, see `kill_child`), the new process opening the datadir and
/// answering RPC, and getting back to its peer's head and `CvvActive`. Measured on a developer
/// machine that takes about 3 s, so the four restarts finish within 12 s, under three 5 s epochs.
/// Eight epochs (40 s) give the restarts over three times that, and leave the network a few
/// pre-fork epochs under load on the new binary alone before the fork arrives. A slower machine
/// that overruns the window fails naming it rather than crossing the fork mid-upgrade.
const UPGRADE_WINDOW_EPOCHS: Epoch = 8;

/// How far an upgraded node's head may trail a peer's before the next validator goes down.
const MAX_HEAD_LAG_BLOCKS: u64 = 3;

/// Start of the warning the new binary logs when `network-config` holds the drift tolerance as a
/// bare integer, the format only the older binary writes (`secs_or_humantime` in
/// `crates/config/src/network.rs`).
const BARE_DRIFT_TOLERANCE_WARNING: &str =
    "max_header_time_drift_tolerance is a bare integer of seconds";

/// The line the older binary writes into `network-config` for its whole-second drift tolerance;
/// the new binary writes a humantime value such as `250ms` instead.
const BARE_DRIFT_TOLERANCE_LINE: &str = "max_header_time_drift_tolerance: 1";

/// JSON-RPC "method not found", which the older binary answers for `tn_getBlockTimestampMillis`.
const METHOD_NOT_FOUND: i64 = -32601;

/// Start of the ERROR the new binary logs, once per epoch table, when it opens an epoch database
/// holding a header it cannot decode (`discard_undecodable_header_tables` in
/// `crates/storage/src/epoch_db_recovery.rs`).
const UNDECODABLE_EPOCH_TABLE_ERROR: &str = "epoch table holds headers this binary cannot decode";

/// Start of the WARN the new binary logs once it has cleared those tables.
const DISCARDED_EPOCH_STATE_WARNING: &str = "discarded undecodable epoch state";

/// Where a node panics when it decodes a header row it cannot read (`decode` in
/// `crates/types/src/codec.rs`), which is how the new binary failed on the older binary's epoch
/// state before it discarded that state.
const UNDECODABLE_ROW_PANIC: &str = "codec.rs:67";

/// The older binary when the lane provides one, or `None` after saying the test was skipped.
fn previous_binary_or_skip(test: &str) -> Option<&'static TestBinary> {
    let previous = get_previous_telcoin_network_binary();
    if previous.is_none() {
        let skipped = format!(
            "SKIPPING {test}: TN_BIN_PATH_PREV is unset, so there is no older node binary to \
             upgrade from. Run `make test-e2e-subsecond-fork`, which builds one with `make \
             build-e2e-bin-prev` and exports it."
        );
        warn!(target: "subsecond-upgrade-test", "{skipped}");
        // also unconditionally: `init_test_tracing` drops `warn!` when RUST_LOG is unset, which is
        // how the default lanes this message is for run
        eprintln!("{skipped}");
    }
    previous
}

/// Pin the multi-workers, seed-signature and leader-seeded-ordering forks for this process and
/// every node it spawns, on both binaries.
///
/// The same pins as `pin_fork_epochs(None, Some(0), None, _)` minus the sub-second one, which
/// [`UpgradeNetwork::pin_subsecond_fork`] sets once the run knows its fork epoch. The seed fork is
/// forced to 0 because the sub-second gate conjoins it fail-closed; the other two follow the lane,
/// defaulting to what `TestBinary::command` would forward anyway.
fn pin_forks_before_subsecond() {
    let lane = |var: &str, default: Epoch| -> Epoch {
        std::env::var(var).ok().and_then(|raw| raw.trim().parse().ok()).unwrap_or(default)
    };
    pin_fork_epoch(
        MULTI_WORKERS_FORK_ENV,
        lane(MULTI_WORKERS_FORK_ENV, u32::MAX),
        multi_workers_fork_active,
    );
    pin_fork_epoch(SEED_SIGNATURE_FORK_ENV, 0, seed_signature_active);
    pin_fork_epoch_override(
        LEADER_SEEDED_ORDERING_FORK_ENV,
        lane(LEADER_SEEDED_ORDERING_FORK_ENV, 0),
        leader_seeded_ordering_fork_epoch_override,
    );
}

/// Run `work` while [`drive_light_tx_load`] sends transfers through `providers`, and stop the load
/// when `work` finishes.
async fn with_light_load<T>(
    providers: &[DynProvider],
    senders: &mut [TransactionFactory],
    chain: Arc<RethChainSpec>,
    work: impl Future<Output = eyre::Result<T>>,
) -> eyre::Result<T> {
    tokio::select! {
        out = work => out,
        never = drive_light_tx_load(providers, senders, chain) => match never {},
    }
}

/// `text` without its ANSI colour escapes (`ESC [ parameters final-byte`), so node log lines read
/// as plain `name=value` fields.
fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            plain.push(c);
            continue;
        }
        if chars.next() == Some('[') {
            // parameter and intermediate bytes, up to and including the final byte
            for c in chars.by_ref() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
    }
    plain
}

/// Four validators started on the older binary, upgraded one at a time.
struct UpgradeNetwork {
    /// Temp-dir prefix and `test_logs` directory of the run.
    test: &'static str,
    /// The node processes, indexed by validator instance. Declared before the temp dir so that
    /// on an early return the nodes are stopped before their datadirs are deleted (fields drop in
    /// declaration order), rather than failing on a vanished datadir in their last seconds.
    guard: ProcessGuard,
    /// Holds the validators' datadirs for the length of the run.
    _temp_dir: tempfile::TempDir,
    /// Where the datadirs live (`validator-1` ..= `validator-4`).
    temp_path: PathBuf,
    /// Each validator's HTTP RPC port, kept across its restart.
    rpc_ports: Vec<u16>,
    /// Each validator's HTTP RPC URL.
    rpc_urls: Vec<String>,
    /// Each validator's RPC client.
    providers: Vec<DynProvider>,
    /// Each validator's metrics address once it runs the new binary.
    metrics_addrs: Vec<Option<String>>,
    /// The run index of each validator's current process (0 old binary, 1 after its upgrade).
    runs: Vec<u32>,
    /// One funded sender per validator for the light load.
    senders: Vec<TransactionFactory>,
    /// The genesis chain spec the senders sign for.
    chain: Arc<RethChainSpec>,
}

impl UpgradeNetwork {
    /// Configure a four-validator network with 5 s epochs and one funded sender per validator,
    /// start every validator on `old`, and wait for their RPC.
    async fn start(old: &'static TestBinary, test: &'static str) -> eyre::Result<Self> {
        // short on purpose: node IPC socket paths are built under the temp dir
        let temp_dir = tempfile::TempDir::with_prefix(test)?;
        let temp_path = temp_dir.path().to_path_buf();

        let senders: Vec<TransactionFactory> = (0..4u64)
            .map(|i| {
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(0x5e50 + i))
            })
            .collect();
        let funding = U256::from(parse_ether("1_000")?);
        let accounts = senders
            .iter()
            .map(|sender| (sender.address(), GenesisAccount::default().with_balance(funding)))
            .collect();
        config_local_testnet_with_epoch_duration(
            &temp_path,
            Some("restart_test".to_string()),
            Some(accounts),
            Some(EPOCH_DURATION as u32),
        )?;
        let genesis: Genesis = Config::load_from_path(
            temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
            ConfigFmt::YAML,
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let mut guard = ProcessGuard::empty();
        let mut rpc_ports = Vec::new();
        let mut rpc_urls = Vec::new();
        for instance in 0..4 {
            let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
            guard.push(start_validator_with_args(
                instance,
                old,
                &temp_path,
                rpc_port,
                test,
                0,
                &[],
            ));
            rpc_ports.push(rpc_port);
            rpc_urls.push(format!("http://127.0.0.1:{rpc_port}"));
        }
        let providers = rpc_urls
            .iter()
            .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?).erased()))
            .collect::<eyre::Result<Vec<_>>>()?;
        futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;

        Ok(Self {
            test,
            guard,
            _temp_dir: temp_dir,
            temp_path,
            rpc_ports,
            rpc_urls,
            providers,
            metrics_addrs: vec![None; 4],
            runs: vec![0; 4],
            senders,
            chain,
        })
    }

    /// Run `work` under the light load, sending through validators `0..nodes`.
    async fn under_load<T>(
        &mut self,
        nodes: usize,
        work: impl Future<Output = eyre::Result<T>>,
    ) -> eyre::Result<T> {
        with_light_load(
            &self.providers[..nodes],
            &mut self.senders[..nodes],
            self.chain.clone(),
            work,
        )
        .await
    }

    /// The epoch the network has open: the highest any validator reads from its registry, since a
    /// node a block behind still reports the epoch before. Every validator must answer.
    async fn open_epoch(&self) -> eyre::Result<Epoch> {
        let mut open = 0;
        for provider in &self.providers {
            open = open.max(current_epoch(provider).await?.epoch_id);
        }
        Ok(open)
    }

    /// `instance`'s execution head.
    async fn head(&self, instance: usize) -> eyre::Result<u64> {
        let url = &self.rpc_urls[instance];
        timeout(RPC_REQUEST_TIMEOUT, self.providers[instance].get_block_number())
            .await
            .map_err(|_| eyre::eyre!("{url} did not answer eth_blockNumber"))?
            .map_err(|e| eyre::eyre!("{url} eth_blockNumber: {e}"))
    }

    /// Choose the sub-second fork epoch, [`UPGRADE_WINDOW_EPOCHS`] past the open one, and pin it.
    async fn pin_subsecond_fork(&self) -> eyre::Result<Epoch> {
        let fork = self.open_epoch().await? + UPGRADE_WINDOW_EPOCHS;
        pin_fork_epoch_override(
            SUBSECOND_TIMESTAMP_FORK_ENV,
            fork,
            subsecond_timestamp_fork_epoch_override,
        );
        Ok(fork)
    }

    /// Prove which binary serves `instance`'s RPC: the new binary answers
    /// `tn_getBlockTimestampMillis` for genesis, the older one has no such method.
    async fn assert_runs_new_binary(&self, instance: usize, upgraded: bool) -> eyre::Result<()> {
        let url = &self.rpc_urls[instance];
        let answer = timeout(
            RPC_REQUEST_TIMEOUT,
            self.providers[instance]
                .raw_request::<_, Option<Value>>("tn_getBlockTimestampMillis".into(), ("0x0",)),
        )
        .await
        .map_err(|_| eyre::eyre!("{url} did not answer tn_getBlockTimestampMillis"))?;
        match (upgraded, answer) {
            (true, Ok(Some(_))) => Ok(()),
            (false, Err(e)) if e.as_error_resp().is_some_and(|r| r.code == METHOD_NOT_FOUND) => {
                Ok(())
            }
            (true, other) => Err(eyre::eyre!(
                "{url} should run the new binary, which serves tn_getBlockTimestampMillis for \
                 genesis, but the call answered {other:?}"
            )),
            (false, other) => Err(eyre::eyre!(
                "{url} should still run the older binary, which has no tn_getBlockTimestampMillis \
                 (method not found, {METHOD_NOT_FOUND}), but the call answered {other:?}"
            )),
        }
    }

    /// Run [`Self::assert_runs_new_binary`] on every validator, the first `upgraded` of them
    /// expected on the new binary.
    async fn assert_binaries(&self, upgraded: &[bool; 4]) -> eyre::Result<()> {
        for (instance, upgraded) in upgraded.iter().enumerate() {
            self.assert_runs_new_binary(instance, *upgraded).await?;
        }
        Ok(())
    }

    /// The `test_logs` file of `instance`'s current process, stdout or stderr.
    fn log_path(&self, instance: usize, stderr: bool) -> PathBuf {
        let suffix = if stderr { ".stderr" } else { "" };
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_logs")
            .join(self.test)
            .join(format!("node{instance}-run{}{suffix}.log", self.runs[instance]))
    }

    /// How `instance`'s current process ended, with the tail of its stderr, or `None` while it
    /// still runs.
    fn exit_report(&mut self, instance: usize) -> Option<String> {
        let status = self.guard.get_mut(instance)?.try_wait().ok()??;
        let path = self.log_path(instance, true);
        let stderr = std::fs::read_to_string(&path).unwrap_or_default();
        let lines: Vec<&str> = stderr.lines().collect();
        let tail = lines[lines.len().saturating_sub(40)..].join("\n");
        Some(format!(
            "validator-{} exited ({status}); last lines of {}:\n{tail}",
            instance + 1,
            path.display()
        ))
    }

    /// Append [`Self::exit_report`] to `error` when `instance`'s process has exited.
    fn explain(&mut self, instance: usize, error: eyre::Report) -> eyre::Report {
        match self.exit_report(instance) {
            Some(report) => error.wrap_err(report),
            None => error,
        }
    }

    /// Stop `instance` and start it again on `bin` over the same datadir and RPC port, with
    /// `--metrics` on a fresh port, then wait for its RPC, failing with its stderr tail if the
    /// process exits first.
    ///
    /// Before the restart its `network-config` must still hold the bare-integer drift tolerance,
    /// which only the older binary writes, so the datadir the new binary opens is the older
    /// binary's.
    async fn restart_on(&mut self, instance: usize, bin: &'static TestBinary) -> eyre::Result<()> {
        let mut old = self
            .guard
            .take(instance)
            .ok_or_else(|| eyre::eyre!("validator-{} is not running", instance + 1))?;
        tokio::task::block_in_place(|| kill_child(&mut old));

        let datadir = self.temp_path.join(format!("validator-{}", instance + 1));
        let network_config = std::fs::read_to_string(datadir.join("network-config"))?;
        eyre::ensure!(
            network_config.lines().any(|line| line.trim() == BARE_DRIFT_TOLERANCE_LINE),
            "validator-{}'s network-config lacks `{BARE_DRIFT_TOLERANCE_LINE}`, so the older binary \
             did not write this datadir:\n{network_config}",
            instance + 1
        );

        let metrics_port = get_available_tcp_port("127.0.0.1").expect("metrics port assigned");
        let metrics_addr = format!("127.0.0.1:{metrics_port}");
        self.runs[instance] += 1;
        let child = start_validator_with_args(
            instance,
            bin,
            &self.temp_path,
            self.rpc_ports[instance],
            self.test,
            self.runs[instance],
            &["--metrics", &metrics_addr],
        );
        eyre::ensure!(
            self.guard.replace(instance, child).is_none(),
            "validator-{} had a second process tracked",
            instance + 1
        );
        self.metrics_addrs[instance] = Some(metrics_addr);

        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            if let Some(report) = self.exit_report(instance) {
                return Err(eyre::eyre!("restarted on the new binary, {report}"));
            }
            if self.providers[instance].get_chain_id().await.is_ok() {
                return Ok(());
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "validator-{} did not answer RPC within 45 s of its restart",
                instance + 1
            );
            sleep(Duration::from_millis(250)).await;
        }
    }

    /// Require `instance`'s stdout log to carry the new binary's warning about the bare-integer
    /// drift tolerance it read from the older binary's `network-config`.
    fn assert_warned_bare_drift_tolerance(&self, instance: usize) -> eyre::Result<()> {
        let path = self.log_path(instance, false);
        let log = std::fs::read_to_string(&path)?;
        eyre::ensure!(
            log.contains(BARE_DRIFT_TOLERANCE_WARNING),
            "{} lacks \"{BARE_DRIFT_TOLERANCE_WARNING}\": the upgraded node did not read the older \
             binary's network-config",
            path.display()
        );
        Ok(())
    }

    /// Require `instance`'s current process to have discarded the epoch-`fork` proposal the older
    /// binary left in its epoch database instead of failing on it: stdout carries the ERROR naming
    /// the `last_proposed` table and epoch `fork`, then the WARN that the epoch state was
    /// discarded, and stderr carries no panic.
    fn assert_discarded_stale_proposal(&self, instance: usize, fork: Epoch) -> eyre::Result<()> {
        let node = format!("validator-{}", instance + 1);
        let stderr_path = self.log_path(instance, true);
        let stderr = std::fs::read_to_string(&stderr_path)?;
        if let Some(line) = stderr
            .lines()
            .find(|line| line.contains(UNDECODABLE_ROW_PANIC) || line.contains("panicked"))
        {
            eyre::bail!(
                "{} reports a panic: {node} panicked on the new binary instead of discarding the \
                 epoch state the older binary left (a panic at {UNDECODABLE_ROW_PANIC} is the \
                 decode of that state): {line}",
                stderr_path.display()
            );
        }

        let path = self.log_path(instance, false);
        let stdout = strip_ansi(&std::fs::read_to_string(&path)?);
        let at_level = |line: &str, level: &str| line.split_whitespace().nth(1) == Some(level);
        let epoch_field = format!("epoch=Some({fork})");
        let error = stdout
            .lines()
            .position(|line| {
                at_level(line, "ERROR")
                    && line.contains(UNDECODABLE_EPOCH_TABLE_ERROR)
                    && line.contains("table=last_proposed")
                    && line.contains(&epoch_field)
            })
            .ok_or_else(|| {
                eyre::eyre!(
                    "{} lacks an ERROR \"{UNDECODABLE_EPOCH_TABLE_ERROR}\" with \
                     table=last_proposed and {epoch_field}: {node} did not take the recovery path \
                     for the epoch-{fork} proposal the older binary wrote in the legacy header \
                     layout",
                    path.display()
                )
            })?;
        eyre::ensure!(
            stdout.lines().skip(error + 1).any(|line| {
                at_level(line, "WARN") && line.contains(DISCARDED_EPOCH_STATE_WARNING)
            }),
            "{} lacks a WARN \"{DISCARDED_EPOCH_STATE_WARNING}\" after the ERROR: {node} found the \
             undecodable epoch-{fork} proposal but did not discard it",
            path.display()
        );
        Ok(())
    }

    /// Upgrade validator `instance` in place to `head_bin` before the fork epoch `fork`, and
    /// return the epoch it was upgraded in.
    ///
    /// The open epoch must be below `fork` when the validator goes down. After the restart the
    /// node has to come back within [`MAX_HEAD_LAG_BLOCKS`] of `peer`'s head and
    /// to `CvvActive` before the caller takes the next validator down, so the network never loses
    /// two validators at once.
    async fn upgrade_before_fork(
        &mut self,
        instance: usize,
        head_bin: &'static TestBinary,
        fork: Epoch,
        peer: usize,
    ) -> eyre::Result<Epoch> {
        let epoch = self.open_epoch().await?;
        eyre::ensure!(
            epoch < fork,
            "validator-{} is due for its upgrade in epoch {epoch}, at or past the sub-second fork \
             epoch {fork}: the rolling upgrade overran its {UPGRADE_WINDOW_EPOCHS}-epoch window, so \
             this run cannot prove an in-place upgrade before the fork",
            instance + 1
        );
        let started = Instant::now();
        self.restart_on(instance, head_bin).await?;

        let (node, peer_node) = (&self.providers[instance], &self.providers[peer]);
        let peer_url = &self.rpc_urls[peer];
        let caught_up = wait_until(
            Duration::from_secs(EPOCH_DURATION * 6),
            &format!("validator-{} head within {MAX_HEAD_LAG_BLOCKS} of {peer_url}", instance + 1),
            || async {
                let (Ok(mine), Ok(theirs)) =
                    (node.get_block_number().await, peer_node.get_block_number().await)
                else {
                    return Ok(false);
                };
                Ok(mine + MAX_HEAD_LAG_BLOCKS >= theirs)
            },
        )
        .await;
        let url = self.rpc_urls[instance].clone();
        let active = caught_up.and_then(|()| {
            tokio::task::block_in_place(|| wait_for_node_mode(&url, NodeMode::CvvActive))
        });
        active.map_err(|e| self.explain(instance, e))?;
        self.assert_warned_bare_drift_tolerance(instance)?;
        info!(
            target: "subsecond-upgrade-test",
            validator = instance + 1,
            epoch,
            elapsed = ?started.elapsed(),
            "validator upgraded in place"
        );
        Ok(epoch)
    }

    /// Walk every block `instance` serves, genesis to its head.
    async fn walk(&self, instance: usize) -> eyre::Result<Vec<BlockCommitTime>> {
        let head = self.head(instance).await?;
        walk_block_commit_times(&self.providers[instance], &self.rpc_urls[instance], 0..=head).await
    }

    /// The final block of each epoch in `0..=last`, from `instance`'s certified epoch records.
    async fn epoch_final_blocks(&self, instance: usize, last: Epoch) -> eyre::Result<Vec<u64>> {
        let mut finals = Vec::new();
        for epoch in 0..=last {
            let record = fetch_verified_epoch_record(
                &self.rpc_urls[instance],
                epoch,
                (EPOCH_DURATION * 6).max(60),
            )
            .await?;
            finals.push(record.final_state.number);
        }
        Ok(finals)
    }

    /// The node endpoints [`assert_epoch_records_verify`] reads (`http_url` only) for `nodes`.
    fn endpoints(&self, nodes: &[usize]) -> Vec<NodeEndpoints> {
        nodes
            .iter()
            .map(|&instance| NodeEndpoints {
                http_url: self.rpc_urls[instance].clone(),
                ws_url: String::new(),
                ipc_path: String::new(),
            })
            .collect()
    }

    /// Require the EVM timestamp clamp counter of `instance`'s current process to read 0.
    fn assert_never_clamped(&self, instance: usize) -> eyre::Result<()> {
        let url = &self.rpc_urls[instance];
        let addr = self.metrics_addrs[instance]
            .as_ref()
            .ok_or_else(|| eyre::eyre!("{url} runs without --metrics"))?;
        let clamped = tokio::task::block_in_place(|| {
            scrape_metric_value(addr, EVM_TIMESTAMP_CLAMPED_SERIES)
        })?;
        eyre::ensure!(
            clamped == 0.0,
            "{url} clamped {clamped} EVM timestamps up to their parent's since its upgrade: \
             consensus let commit time go backwards after the fork (node logs under \
             test_logs/{}/ carry the \"evm timestamp clamped to parent\" warnings)",
            self.test
        );
        Ok(())
    }
}

/// Check that `served`, one node's block walk, crossed the sub-second fork at `fork` where the
/// epoch records put it, and return the first post-fork block.
///
/// `finals[e]` is epoch `e`'s final block from a certified record, for every epoch through at
/// least `fork + 1`. Each of those blocks must be in the walk, close its epoch, and report
/// `subSecond` exactly when `e >= fork` (the node derives the flag from the commit's leader
/// epoch, which for a closing block is the epoch it closes). Every other block must agree with
/// the epoch it sits in: whole-second commit times with `subSecond` false up to epoch `fork - 1`'s
/// final block, `subSecond` true after it, and at least one post-fork commit with a non-zero
/// millisecond part.
fn assert_crossed_fork_at(
    served: &[BlockCommitTime],
    finals: &[u64],
    fork: Epoch,
    node: &str,
) -> eyre::Result<u64> {
    let fork_index = fork as usize;
    eyre::ensure!(
        fork_index >= 1 && finals.len() > fork_index + 1,
        "{node}: epoch records end at epoch {}, short of the fork epoch {fork} + 1",
        finals.len().saturating_sub(1)
    );
    for (epoch, &number) in finals.iter().enumerate() {
        let block = served.iter().find(|block| block.block_number == number).ok_or_else(|| {
            eyre::eyre!("{node}: epoch {epoch}'s final block {number} is not in the walk")
        })?;
        eyre::ensure!(
            block.closes_epoch,
            "{node}: epoch {epoch}'s final block {number} does not close an epoch: {block:?}"
        );
        eyre::ensure!(
            block.sub_second == (epoch >= fork_index),
            "{node}: epoch {epoch}'s final block {number} reports subSecond {} with the fork at \
             epoch {fork}: {block:?}",
            block.sub_second
        );
    }
    let last_pre_fork = finals[fork_index - 1];
    let mut sub_second_millis = 0usize;
    let mut first_post_fork = None;
    for block in served {
        let post_fork = block.block_number > last_pre_fork;
        eyre::ensure!(
            block.sub_second == post_fork,
            "{node}: block {} reports subSecond {} but epoch {}'s final block is {last_pre_fork}: \
             {block:?}",
            block.block_number,
            block.sub_second,
            fork - 1
        );
        if post_fork {
            first_post_fork.get_or_insert(block.block_number);
            if block.timestamp_millis % 1000 != 0 {
                sub_second_millis += 1;
            }
        } else {
            eyre::ensure!(
                block.timestamp_millis % 1000 == 0,
                "{node}: pre-fork block {} has a sub-second commit time: {block:?}",
                block.block_number
            );
        }
    }
    eyre::ensure!(
        sub_second_millis > 0,
        "{node}: no post-fork block has a commit time with a non-zero millisecond part"
    );
    first_post_fork.ok_or_else(|| eyre::eyre!("{node}: the walk holds no post-fork block"))
}

/// Upgrade all four validators in place from the older binary to the new one before the
/// sub-second fork epoch, then cross the fork on the new binary over history the older one wrote.
///
/// This is the production path: the older binary seals epochs 0 and 1 under a light transfer
/// load, then one validator at a time is stopped and restarted on the new binary over the same
/// datadir and RPC port, each back to `CvvActive` and its peer's head before the next goes down.
/// After every restart `tn_getBlockTimestampMillis` answers on the upgraded validators and is
/// "method not found" on the rest, which shows which binary runs where; the upgraded validator's
/// log carries the new binary's warning about the bare-integer drift tolerance in
/// `network-config`, which shows the datadir it opened was the older binary's. With all four
/// upgraded and the fork still ahead, the network runs under load to epoch `F + 2`.
///
/// The fork epoch `F` is chosen at run time, [`UPGRADE_WINDOW_EPOCHS`] past the epoch open after
/// the older binary sealed epoch 1, because how many 5 s epochs four restarts take is only known
/// then. The multi-workers, seed-signature (forced to 0, which the sub-second gate conjoins) and
/// leader-seeded-ordering forks are pinned before the first spawn and apply to both binaries. The
/// sub-second fork is pinned once `F` is chosen, before the first new-binary node spawns and never
/// again, so every new-binary node is spawned with the same `F`; the older binary has no
/// sub-second fork and ignores the variable it was spawned with. The pin also latches `F` in this
/// process, which the on-disk walk at the end decodes headers under.
///
/// Every node must then show the crossing: all four serve the same blocks and commit times from
/// genesis to their heads; each serves a certified record with a matching final block for every
/// epoch through `F + 1`; each epoch's final block reports `subSecond` exactly from `F` on; every
/// block before epoch `F` has a whole-second commit time and at least one after it does not; and
/// no node's engine clamped an EVM timestamp since its upgrade. The pre-fork blocks of epochs 0
/// and 1 live in packs the older binary wrote, so serving their commit times shows the new binary
/// reads those packs. Validator-1 is then stopped and its consensus chain walked on disk
/// ([`assert_consensus_commit_times`]), so the commit-time formula is checked across packs the
/// older binary wrote and packs the new one wrote, and every block it served is matched to the
/// commit its header holds there.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "only run independently from all other it tests"]
async fn test_epoch_upgrade_in_place_before_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let Some(old_bin) =
        previous_binary_or_skip("test_epoch_upgrade_in_place_before_subsecond_fork")
    else {
        return Ok(());
    };
    pin_forks_before_subsecond();

    let mut net = UpgradeNetwork::start(old_bin, "ss_upgrade").await?;
    // the older binary seals epochs 0 and 1
    let provider = net.providers[0].clone();
    net.under_load(4, wait_for_epoch_at_least(&provider, 2)).await?;
    net.assert_binaries(&[false; 4]).await?;

    let fork = net.pin_subsecond_fork().await?;
    let head_bin = get_telcoin_network_binary();
    let mut upgrade_epochs = Vec::new();
    for instance in 0..4 {
        upgrade_epochs
            .push(net.upgrade_before_fork(instance, head_bin, fork, (instance + 1) % 4).await?);
        net.assert_binaries(&std::array::from_fn(|other| other <= instance)).await?;
    }
    let all_upgraded = net.open_epoch().await?;
    eyre::ensure!(
        all_upgraded < fork,
        "the last upgrade finished in epoch {all_upgraded}, at or past the fork epoch {fork}"
    );

    let url = net.rpc_urls[0].clone();
    let boundaries = fork + 2 - all_upgraded;
    let reached =
        net.under_load(4, loop_epochs(all_upgraded, boundaries, &url, EPOCH_DURATION)).await?;
    eyre::ensure!(reached >= fork + 2, "network stopped at epoch {reached}, short of {}", fork + 2);

    let mut served = Vec::new();
    for instance in 0..4 {
        served.push(net.walk(instance).await?);
    }
    assert_nodes_agree_on_commit_times(&served, &net.rpc_urls)?;
    assert_epoch_records_verify(&net.endpoints(&[0, 1, 2, 3]), 0..=fork + 1, 60).await?;
    let finals = net.epoch_final_blocks(0, fork + 1).await?;
    let mut first_post_fork = 0;
    for (instance, walk) in served.iter().enumerate() {
        first_post_fork = assert_crossed_fork_at(walk, &finals, fork, &net.rpc_urls[instance])?;
        net.assert_never_clamped(instance)?;
    }

    // stop validator-1 so its consensus chain can be opened from this process
    let mut validator_1 =
        net.guard.take(0).ok_or_else(|| eyre::eyre!("validator-1 is not running"))?;
    tokio::task::block_in_place(|| kill_child(&mut validator_1));
    eyre::ensure!(
        net.providers[0].get_chain_id().await.is_err(),
        "validator-1 still answers RPC after being stopped"
    );
    let headers = read_consensus_headers(&net.temp_path.join("validator-1")).await?;
    let commits = assert_consensus_commit_times(&headers, fork)?;
    assert_blocks_match_consensus(&served[0], &commits, fork, 0..=fork + 1)?;

    let summary = format!(
        "in-place upgrade run: fork epoch {fork}, validators upgraded in epochs \
         {upgrade_epochs:?}, all upgraded by epoch {all_upgraded}, first post-fork block \
         {first_post_fork}, epoch final blocks {finals:?}"
    );
    info!(target: "subsecond-upgrade-test", "{summary}");
    eprintln!("{summary}");
    net.guard.kill_all();
    Ok(())
}

/// Upgrade three validators in place before the sub-second fork and leave validator-4 on the older
/// binary through it, then upgrade validator-4 late, over its datadir, after the fork.
///
/// The three upgraded validators are a quorum on their own, so the network must keep committing
/// through epoch `F + 1` and those three must agree on every block and commit time. Validator-4
/// cannot read the new layout, so its head must stop inside epoch `F - 1` (after epoch `F - 2`'s
/// final block, at most epoch `F - 1`'s) and stay there across two reads 15 s apart. Restarted on
/// the new binary with its datadir intact, it must come back, apply headers from its peers, return
/// to `CvvActive`, catch up with validator-1, cross the fork where the epoch records put it, and
/// agree with validator-1 on every block and commit time from genesis to its head.
///
/// The datadir the late upgrade opens holds what the older binary wrote for epoch `F` before it
/// stalled, including its own epoch-`F` proposal in the legacy layout, which the new binary cannot
/// decode under the post-fork layout. The new binary discards that stale header when it opens the
/// datadir and logs it: validator-4's stdout must carry the ERROR naming the `last_proposed`
/// table and epoch `F`, then the WARN that the epoch state was discarded, and its stderr no panic.
/// It then fetches epoch `F` from its peers and catches up, which the return to `CvvActive` and the
/// agreement with validator-1 above prove.
///
/// Fork pins as in [`test_epoch_upgrade_in_place_before_subsecond_fork`]; this test decodes no
/// consensus data in-process, so the sub-second pin's latch here only backs its own read-back.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "only run independently from all other it tests"]
async fn test_epoch_late_upgrader_after_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let Some(old_bin) = previous_binary_or_skip("test_epoch_late_upgrader_after_subsecond_fork")
    else {
        return Ok(());
    };
    pin_forks_before_subsecond();

    let mut net = UpgradeNetwork::start(old_bin, "ss_late_upg").await?;
    let provider = net.providers[0].clone();
    net.under_load(4, wait_for_epoch_at_least(&provider, 2)).await?;
    net.assert_binaries(&[false; 4]).await?;

    let fork = net.pin_subsecond_fork().await?;
    let head_bin = get_telcoin_network_binary();
    let mut upgrade_epochs = Vec::new();
    for instance in 0..3 {
        upgrade_epochs
            .push(net.upgrade_before_fork(instance, head_bin, fork, (instance + 1) % 4).await?);
        net.assert_binaries(&std::array::from_fn(|other| other <= instance)).await?;
    }
    let three_upgraded = net.open_epoch().await?;
    eyre::ensure!(
        three_upgraded < fork,
        "the third upgrade finished in epoch {three_upgraded}, at or past the fork epoch {fork}"
    );

    // the three upgraded validators carry the network through epoch F + 1 on their own
    let url = net.rpc_urls[0].clone();
    let boundaries = fork + 2 - three_upgraded;
    let reached =
        net.under_load(3, loop_epochs(three_upgraded, boundaries, &url, EPOCH_DURATION)).await?;
    eyre::ensure!(reached >= fork + 2, "network stopped at epoch {reached}, short of {}", fork + 2);
    let finals = net.epoch_final_blocks(0, fork + 1).await?;

    // validator-4, still on the older binary, stops inside epoch F - 1
    let stalled = net.head(3).await.map_err(|e| net.explain(3, e))?;
    sleep(Duration::from_secs(15)).await;
    let still = net.head(3).await.map_err(|e| net.explain(3, e))?;
    let (before_last_pre_fork, last_pre_fork) =
        (finals[fork as usize - 2], finals[fork as usize - 1]);
    eyre::ensure!(
        stalled == still,
        "validator-4 (older binary) kept executing after the fork: head {stalled} then {still}"
    );
    eyre::ensure!(
        before_last_pre_fork < still && still <= last_pre_fork,
        "validator-4 (older binary) stalled at block {still}, outside epoch {} (blocks {} ..= \
         {last_pre_fork})",
        fork - 1,
        before_last_pre_fork + 1
    );

    let mut served = Vec::new();
    for instance in 0..3 {
        served.push(net.walk(instance).await?);
    }
    assert_nodes_agree_on_commit_times(&served, &net.rpc_urls[..3])?;
    assert_epoch_records_verify(&net.endpoints(&[0, 1, 2]), 0..=fork + 1, 60).await?;
    for (instance, walk) in served.iter().enumerate() {
        assert_crossed_fork_at(walk, &finals, fork, &net.rpc_urls[instance])?;
        net.assert_never_clamped(instance)?;
    }

    // the late upgrade, over the datadir the older binary stalled on
    let late_epoch = net.open_epoch().await?;
    net.restart_on(3, head_bin).await?;
    let late_url = net.rpc_urls[3].clone();
    let metrics = net.metrics_addrs[3].clone().unwrap_or_default();
    tokio::task::block_in_place(|| wait_for_restart_catch_up(&late_url, &metrics))
        .map_err(|e| net.explain(3, e))?;
    let target = net.head(0).await?;
    let late = &net.providers[3];
    wait_until(
        Duration::from_secs(EPOCH_DURATION * 6),
        &format!("late upgrader {late_url} to reach validator-1's head {target}"),
        || async { Ok(late.get_block_number().await.is_ok_and(|head| head >= target)) },
    )
    .await
    .map_err(|e| net.explain(3, e))?;
    net.assert_runs_new_binary(3, true).await?;
    net.assert_warned_bare_drift_tolerance(3)?;
    net.assert_discarded_stale_proposal(3, fork)?;

    let reference = net.walk(0).await?;
    let late_walk = net.walk(3).await?;
    assert_crossed_fork_at(&late_walk, &finals, fork, &late_url)?;
    assert_nodes_agree_on_commit_times(
        &[reference, late_walk],
        &[net.rpc_urls[0].clone(), late_url],
    )?;
    assert_epoch_records_verify(&net.endpoints(&[3]), 0..=fork + 1, 60).await?;
    net.assert_never_clamped(3)?;

    let summary = format!(
        "late-upgrader run: fork epoch {fork}, validators 1-3 upgraded in epochs \
         {upgrade_epochs:?}, validator-4 stalled at block {still} (epoch {} final block \
         {last_pre_fork}) and was upgraded in epoch {late_epoch}",
        fork - 1
    );
    info!(target: "subsecond-upgrade-test", "{summary}");
    eprintln!("{summary}");
    net.guard.kill_all();
    Ok(())
}
