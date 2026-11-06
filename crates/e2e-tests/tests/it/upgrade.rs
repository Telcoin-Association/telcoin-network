//! Mixed-binary (rolling upgrade) test: a live network upgraded in place, one validator at a time.
//!
//! Four validators start on the previous node binary (`TN_BIN_PATH_PREV`). Each is then stopped
//! and started again on the current binary (`TN_BIN_PATH`) over the same datadir, the way an
//! operator upgrades, while transactions keep flowing and epochs keep closing. During the rollout
//! old and new binaries execute the same consensus output side by side, so any difference in how
//! they execute it shows up as two nodes disagreeing on a block: a fork. The new binary also has to
//! open the execution and consensus databases the old one wrote.
//!
//! The run, in order:
//!
//! 1. All four validators on PREV: deploy a token contract, send transfers and token calls, close
//!    an epoch.
//! 2. Upgrade validator-1 to validator-4 to CURRENT, one at a time, each over its own datadir.
//!    After each upgrade the validator must rejoin and catch up, a round of transactions goes
//!    through it, and an epoch closes, so epochs close under committees of 3+1, 2+2 and 1+3.
//! 3. Close one more epoch with all four on CURRENT.
//! 4. Start two observers with empty datadirs, one on each binary, and wait until each has executed
//!    the chain from genesis.
//! 5. Compare every block from genesis across all six nodes.
//! 6. Roll validator-1 back to PREV over the datadir CURRENT wrote; it must rejoin and close an
//!    epoch with the others.
//! 7. Stop the load and compare every block again, the rolled-back validator included.
//!
//! A light transfer load ([`drive_light_tx_load`]) runs on its own task through all four validators
//! for the whole run, restarts included. Every closed epoch's certified record is checked against
//! the final block of that epoch on every running node. Every node's `web3_clientVersion` is
//! checked against the commit its binary's `--version` reports, so a slot cannot silently run the
//! other binary.
//!
//! Without `TN_BIN_PATH_PREV` the test skips with a warning. `make test-upgrade` runs it.

use crate::common::{
    acquire_test_permit, assert_epoch_reached, assert_epoch_records_verify, current_epoch,
    drive_light_tx_load, epoch_seconds_remaining, get_block_number, kill_child, node_log_path,
    start_named_observer, start_validator, strip_ansi, wait_for_head_at_least, wait_for_node_mode,
    ProcessGuard, RPC_REQUEST_TIMEOUT, WEI_PER_TEL,
};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    rpc::types::{TransactionReceipt, TransactionRequest},
    sol,
    sol_types::{SolCall as _, SolConstructor as _},
};
use e2e_tests::{
    config_local_testnet_with_epoch_duration, config_observer, NodeEndpoints, TestBinary,
};
use eyre::WrapErr as _;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    fmt,
    ops::RangeInclusive,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::{fetch_file_content_relative_to_manifest, Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{test_utils::TransactionFactory, RethChainSpec, RethEnv};
use tn_types::{
    get_available_tcp_port, hex, keccak256, Address, Genesis, GenesisAccount, NodeMode, B256, U256,
};
use tokio::{task::JoinHandle, time::timeout};
use tracing::{info, warn};

/// Names the temp dir and the `test_logs/` directory. Kept short: node IPC socket paths are built
/// under the temp dir.
const TEST: &str = "upgrade";

/// Number of validators in the committee.
const VALIDATORS: usize = 4;

/// Epoch duration in seconds. Long enough that a validator stopped early in an epoch is back
/// before the epoch closes (see [`Network::wait_for_restart_window`]), short enough that the run
/// closes seven epochs in a few minutes.
const EPOCH_SECS: u64 = 20;

/// A validator is stopped only once its epoch is at least this many seconds old, so the previous
/// boundary's epoch record has been voted on.
const RESTART_MIN_ELAPSED_SECS: u64 = 2;

/// A validator is stopped only while its epoch has at least this many seconds left, so the restart
/// does not straddle a boundary.
const RESTART_MIN_REMAINING_SECS: u64 = 12;

/// BLS passphrase the keys are written with; `start_validator` and `start_named_observer` hand the
/// same one to every node.
const PASSPHRASE: &str = "restart_test";

/// Datadir of the observer that runs PREV. The CURRENT observer uses the harness's `observer`.
const PREV_OBSERVER: &str = "observer-prev";

/// The validator rolled back to PREV at the end: validator-1, the one that ran CURRENT longest.
const ROLLBACK: usize = 0;

/// Bound on a started process answering RPC.
const READY_TIMEOUT: Duration = Duration::from_secs(90);

/// Bound, in seconds, on a restarted validator reaching the head the others had when it came back.
const CATCH_UP_SECS: u64 = 90;

/// Bound, in seconds, on an observer with an empty datadir executing the chain from genesis.
const OBSERVER_SYNC_SECS: u64 = 300;

/// Bound, in seconds, on each node serving the certified record of a closed epoch.
const EPOCH_RECORD_TIMEOUT_SECS: u64 = 60;

/// Bound on one transaction's receipt.
const RECEIPT_TIMEOUT: Duration = Duration::from_secs(60);

/// Max fee per gas of every transaction the test signs. These networks keep the base fee at the
/// protocol minimum.
const GAS_PRICE: u128 = 100;

/// Gas limit of the two contract creations.
const DEPLOY_GAS: u64 = 5_000_000;

/// Seed of the first light-load sender; sender `i` uses `LOAD_SEED + i`.
const LOAD_SEED: u64 = 0x0b_0000;

/// Seed of the first account in the transfer ring; account `i` uses `RING_SEED + i`.
const RING_SEED: u64 = 0x0b_1000;

/// Seed of the account that deploys and drives the token.
const DEPLOYER_SEED: u64 = 0x0b_2000;

/// Accounts in the transfer ring.
const RING_ACCOUNTS: u64 = 3;

sol! {
    /// The calls this test makes on `tn-contracts/artifacts/Stablecoin.json`.
    contract Stablecoin {
        function initialize(string memory name_, string memory symbol_, uint8 decimals_) external;
        function grantRole(bytes32 role, address account) external;
        function mintTo(address account, uint256 value) external;
        function approve(address spender, uint256 value) external returns (bool);
        function balanceOf(address account) external view returns (uint256);
    }

    /// The constructor of `tn-contracts/artifacts/ERC1967Proxy.json`.
    contract ERC1967Proxy {
        constructor(address implementation, bytes memory _data);
    }
}

/// Which of the two node binaries a process runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Release {
    /// `TN_BIN_PATH_PREV`, the binary validators upgrade from.
    Prev,
    /// `TN_BIN_PATH`, the binary validators upgrade to.
    Current,
}

impl fmt::Display for Release {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Prev => "PREV",
            Self::Current => "CURRENT",
        })
    }
}

/// The two binaries under test and the commit each one reports.
#[derive(Debug)]
struct Binaries {
    prev: &'static TestBinary,
    current: &'static TestBinary,
    prev_sha: String,
    current_sha: String,
}

impl Binaries {
    /// Resolve both commits, refusing two binaries that report the same one: the run would then
    /// compare a binary with itself and prove nothing.
    fn new(prev: &'static TestBinary, current: &'static TestBinary) -> eyre::Result<Self> {
        let prev_sha = commit_sha(prev).wrap_err("reading the TN_BIN_PATH_PREV binary's commit")?;
        let current_sha =
            commit_sha(current).wrap_err("reading the TN_BIN_PATH binary's commit")?;
        eyre::ensure!(
            prev_sha != current_sha,
            "TN_BIN_PATH and TN_BIN_PATH_PREV both report commit {prev_sha}, so the run would \
             compare one binary with itself"
        );
        Ok(Self { prev, current, prev_sha, current_sha })
    }

    fn bin(&self, release: Release) -> &'static TestBinary {
        match release {
            Release::Prev => self.prev,
            Release::Current => self.current,
        }
    }

    fn sha(&self, release: Release) -> &str {
        match release {
            Release::Prev => &self.prev_sha,
            Release::Current => &self.current_sha,
        }
    }
}

/// The `Commit SHA:` line of `bin --version`.
fn commit_sha(bin: &TestBinary) -> eyre::Result<String> {
    let output = bin.command().arg("--version").output()?;
    eyre::ensure!(output.status.success(), "--version exited with {}", output.status);
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix("Commit SHA:"))
        .map(|sha| sha.trim().to_string())
        .filter(|sha| !sha.is_empty())
        .ok_or_else(|| eyre::eyre!("--version printed no commit: {stdout}"))
}

/// A node process the test started.
#[derive(Debug)]
struct Node {
    /// Datadir name, e.g. `validator-1`.
    name: String,
    /// HTTP RPC endpoint.
    url: String,
    /// Port of `url`, reused when the node restarts.
    rpc_port: u16,
    /// The binary the running process was started from.
    release: Release,
    /// Log run number of the running process (`node<slot>-run<run>.log`).
    run: u32,
}

/// The nodes of the run and the processes behind them. A node's index in `nodes` is its
/// [`ProcessGuard`] slot and its log instance number.
struct Network<'a> {
    temp_path: &'a Path,
    binaries: &'a Binaries,
    guard: &'a mut ProcessGuard,
    nodes: Vec<Node>,
}

impl Network<'_> {
    /// Start the next validator (validators are started first, in order) on `release`.
    fn start_validator(&mut self, release: Release) -> eyre::Result<usize> {
        let idx = self.nodes.len();
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no rpc port available for validator-{}", idx + 1))?;
        let child =
            start_validator(idx, self.binaries.bin(release), self.temp_path, rpc_port, TEST, 0);
        let slot = self.guard.push(child);
        eyre::ensure!(slot == idx, "validator-{} landed in guard slot {slot}", idx + 1);
        self.nodes.push(Node {
            name: format!("validator-{}", idx + 1),
            url: format!("http://127.0.0.1:{rpc_port}"),
            rpc_port,
            release,
            run: 0,
        });
        Ok(idx)
    }

    /// Start an observer on `release` from the datadir `name`.
    fn start_observer(&mut self, name: &str, release: Release) -> eyre::Result<usize> {
        let idx = self.nodes.len();
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no rpc port available for {name}"))?;
        let child = start_named_observer(
            idx,
            self.binaries.bin(release),
            self.temp_path,
            name,
            rpc_port,
            TEST,
            0,
            &[],
        );
        let slot = self.guard.push(child);
        eyre::ensure!(slot == idx, "{name} landed in guard slot {slot}");
        self.nodes.push(Node {
            name: name.to_string(),
            url: format!("http://127.0.0.1:{rpc_port}"),
            rpc_port,
            release,
            run: 0,
        });
        Ok(idx)
    }

    /// `name (RELEASE sha)`, for messages.
    fn label(&self, idx: usize) -> String {
        let node = &self.nodes[idx];
        format!("{} ({} {})", node.name, node.release, self.binaries.sha(node.release))
    }

    fn validators(&self) -> &[Node] {
        &self.nodes[..VALIDATORS]
    }

    /// The committee by binary, e.g. `2 PREV + 2 CURRENT`.
    fn committee(&self) -> String {
        let prev = self.validators().iter().filter(|node| node.release == Release::Prev).count();
        format!("{prev} PREV + {} CURRENT", VALIDATORS - prev)
    }

    fn endpoints(&self) -> Vec<NodeEndpoints> {
        self.nodes
            .iter()
            .map(|node| NodeEndpoints {
                http_url: node.url.clone(),
                ws_url: String::new(),
                ipc_path: String::new(),
            })
            .collect()
    }

    /// Highest execution head among the validators other than `except`.
    fn validator_head(&self, except: Option<usize>) -> eyre::Result<u64> {
        let mut head = 0;
        for (idx, node) in self.validators().iter().enumerate() {
            if Some(idx) != except {
                head = head.max(get_block_number(&node.url)?);
            }
        }
        Ok(head)
    }

    /// Wait until the process in slot `idx` answers RPC, then check it runs the binary the slot
    /// was given. A process that exits first fails with the tail of its logs: for a restart over
    /// an existing datadir this is how a binary that cannot open the other's datadir shows up.
    async fn wait_ready(&mut self, idx: usize) -> eyre::Result<()> {
        let label = self.label(idx);
        let node = &self.nodes[idx];
        let provider = ProviderBuilder::new().connect_http(node.url.parse()?);
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            let child = self
                .guard
                .get_mut(idx)
                .ok_or_else(|| eyre::eyre!("no process in guard slot {idx} for {label}"))?;
            if let Some(status) = child.try_wait()? {
                eyre::bail!(
                    "{label} exited ({status}) before its RPC came up; datadir {}; log tail:\n{}",
                    self.temp_path.join(&node.name).display(),
                    log_tail(idx, node.run),
                );
            }
            if provider.get_block_number().await.is_ok() {
                break;
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "{label} did not answer RPC within {READY_TIMEOUT:?}; log tail:\n{}",
                log_tail(idx, node.run),
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        let version = provider.get_client_version().await?;
        let sha = self.binaries.sha(node.release);
        eyre::ensure!(
            version.contains(sha),
            "{label} reports web3_clientVersion {version:?}, not the {} binary's commit {sha}",
            node.release,
        );
        info!(target: "upgrade-test", node = %label, version, "node is up on the expected binary");
        Ok(())
    }

    /// Wait until the current epoch is old enough that its opening boundary is settled and young
    /// enough that a stop and start fit before it closes, measured on validator `peer`.
    async fn wait_for_restart_window(&self, peer: usize) -> eyre::Result<()> {
        let url = &self.nodes[peer].url;
        let provider = ProviderBuilder::new().connect_http(url.parse()?);
        let deadline = Instant::now() + Duration::from_secs(EPOCH_SECS * 3);
        loop {
            let snap = current_epoch(&provider).await?;
            let remaining = epoch_seconds_remaining(url, &snap)?;
            let elapsed = snap.epoch_duration.saturating_sub(remaining);
            if elapsed >= RESTART_MIN_ELAPSED_SECS && remaining >= RESTART_MIN_REMAINING_SECS {
                return Ok(());
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "no restart window within {}s: epoch {} at {elapsed}s of {}s",
                EPOCH_SECS * 3,
                snap.epoch_id,
                snap.epoch_duration,
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop validator `idx` and start it on `release` over the same datadir, then wait until it is
    /// an active committee member again and has caught up with the others.
    async fn restart_validator(
        &mut self,
        idx: usize,
        release: Release,
        phase: &str,
    ) -> eyre::Result<()> {
        let peer = (idx + 1) % VALIDATORS;
        self.wait_for_restart_window(peer).await.wrap_err_with(|| phase.to_string())?;
        let before = self.label(idx);
        let url = self.nodes[idx].url.clone();
        let head_before = get_block_number(&url)?;
        // the engine moves `finalized` only after it has committed the output to the database,
        // so every block up to it is on disk when the process stops, however it stops
        let persisted = ProviderBuilder::new()
            .connect_http(url.parse()?)
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await?
            .map(|block| block.header.number)
            .ok_or_else(|| eyre::eyre!("{phase}: {before} serves no finalized block"))?;

        let mut child = self
            .guard
            .take(idx)
            .ok_or_else(|| eyre::eyre!("{phase}: no process in guard slot {idx}"))?;
        // SIGTERM, the way a service manager stops a node, with SIGKILL after a grace period
        let graceful = tokio::task::block_in_place(|| kill_child(&mut child));

        let run = self.nodes[idx].run + 1;
        let child = start_validator(
            idx,
            self.binaries.bin(release),
            self.temp_path,
            self.nodes[idx].rpc_port,
            TEST,
            run,
        );
        self.guard.replace(idx, child);
        self.nodes[idx].release = release;
        self.nodes[idx].run = run;
        let after = self.label(idx);

        self.wait_ready(idx).await.wrap_err_with(|| {
            format!("{phase}: {after} did not start over the datadir {before} wrote")
        })?;
        let head_restarted = get_block_number(&url)?;
        // below the persisted block, the new binary did not use all of the datadir and executed
        // those blocks again, which the block comparison alone would not show
        eyre::ensure!(
            head_restarted >= persisted,
            "{phase}: {after} came up at block {head_restarted}, below block {persisted} that \
             {before} had finalized on disk before it stopped"
        );
        tokio::task::block_in_place(|| wait_for_node_mode(&url, NodeMode::CvvActive))
            .wrap_err_with(|| format!("{phase}: {after} did not rejoin the committee"))?;
        let target = self.validator_head(Some(idx))?;
        wait_for_head_at_least(&url, target, CATCH_UP_SECS)
            .await
            .wrap_err_with(|| format!("{phase}: {after} did not catch up"))?;
        let stop = if graceful { "exited on SIGTERM" } else { "needed SIGKILL" };
        progress(format!(
            "{phase}: {before} {stop} at block {head_before} (finalized {persisted}); {after} \
             came up over the same datadir at block {head_restarted}, is CvvActive and reached \
             block {target}; committee is now {}",
            self.committee()
        ));
        Ok(())
    }

    /// Wait for the epoch open now to close, then check that every running node serves its
    /// certified record and has executed the record's final block with the hash the record
    /// commits to. A quorum of the committee signed that record, so a mixed committee closing an
    /// epoch means both binaries agreed on its final block.
    async fn cross_epoch_boundary(&self, phase: &str) -> eyre::Result<u32> {
        let reference = &self.nodes[1].url;
        let provider = ProviderBuilder::new().connect_http(reference.parse()?);
        let closing = current_epoch(&provider).await?.epoch_id;
        let committee = self.committee();
        assert_epoch_reached(reference, closing + 1, phase).await?;
        let endpoints = self.endpoints();
        assert_epoch_records_verify(&endpoints, closing..=closing, EPOCH_RECORD_TIMEOUT_SECS)
            .await
            .wrap_err_with(|| format!("{phase}: epoch {closing} record check"))?;
        progress(format!(
            "{phase}: epoch {closing} closed under a committee of {committee}; all {} nodes \
             executed the final block its certified record commits to",
            endpoints.len()
        ));
        Ok(closing)
    }

    /// Wait until every node has reached the highest head any of them reports, and return it.
    async fn wait_for_common_head(&self) -> eyre::Result<u64> {
        let mut head = 0;
        for node in &self.nodes {
            head = head.max(get_block_number(&node.url)?);
        }
        for node in &self.nodes {
            wait_for_head_at_least(&node.url, head, CATCH_UP_SECS).await?;
        }
        Ok(head)
    }

    /// Require every block from genesis to the lowest head among the nodes to have the same
    /// `hash`, `stateRoot` and `receiptsRoot` on every node, and return the range compared.
    ///
    /// The first node is the reference. The first block a node disagrees on fails the check with
    /// both nodes' values.
    async fn assert_nodes_agree_on_blocks(&self) -> eyre::Result<RangeInclusive<u64>> {
        let labels: Vec<String> = (0..self.nodes.len()).map(|idx| self.label(idx)).collect();
        let providers = self
            .nodes
            .iter()
            .map(|node| Ok(ProviderBuilder::new().connect_http(node.url.parse()?)))
            .collect::<eyre::Result<Vec<_>>>()?;
        let mut tip = u64::MAX;
        for (provider, label) in providers.iter().zip(&labels) {
            let head = timeout(RPC_REQUEST_TIMEOUT, provider.get_block_number())
                .await
                .map_err(|_| eyre::eyre!("{label} did not answer eth_blockNumber"))??;
            tip = tip.min(head);
        }
        for number in 0..=tip {
            let roots = futures::future::try_join_all(
                providers
                    .iter()
                    .zip(&labels)
                    .map(|(provider, label)| block_roots(provider, label, number)),
            )
            .await?;
            let Some((reference, others)) = roots.split_first() else { break };
            for (other, label) in others.iter().zip(&labels[1..]) {
                if other != reference {
                    eyre::bail!(
                        "fork at block {number} ({} differ): {label} has {other}; {} has \
                         {reference}",
                        reference.differing_fields(other),
                        labels[0],
                    );
                }
            }
        }
        Ok(0..=tip)
    }
}

/// The header fields compared across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockRoots {
    hash: B256,
    state_root: B256,
    receipts_root: B256,
}

impl BlockRoots {
    /// Names of the fields that differ between `self` and `other`.
    fn differing_fields(&self, other: &Self) -> String {
        [
            ("hash", self.hash != other.hash),
            ("stateRoot", self.state_root != other.state_root),
            ("receiptsRoot", self.receipts_root != other.receipts_root),
        ]
        .into_iter()
        .filter_map(|(name, differs)| differs.then_some(name))
        .collect::<Vec<_>>()
        .join(", ")
    }
}

impl fmt::Display for BlockRoots {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hash {}, stateRoot {}, receiptsRoot {}",
            self.hash, self.state_root, self.receipts_root
        )
    }
}

/// [`BlockRoots`] of block `number` on the node behind `provider`.
async fn block_roots<P: Provider>(
    provider: &P,
    label: &str,
    number: u64,
) -> eyre::Result<BlockRoots> {
    let block = timeout(
        RPC_REQUEST_TIMEOUT,
        provider.get_block_by_number(BlockNumberOrTag::Number(number)),
    )
    .await
    .map_err(|_| eyre::eyre!("{label} did not answer eth_getBlockByNumber for block {number}"))??
    .ok_or_else(|| eyre::eyre!("{label} has no block {number}"))?;
    Ok(BlockRoots {
        hash: block.header.hash,
        state_root: block.header.state_root,
        receipts_root: block.header.receipts_root,
    })
}

/// The last lines of the stdout and stderr logs of process `run` in slot `idx`.
fn log_tail(idx: usize, run: u32) -> String {
    const LINES: usize = 30;
    [false, true]
        .into_iter()
        .map(|stderr| {
            let path = node_log_path(TEST, idx, run, stderr);
            match std::fs::read_to_string(&path) {
                Ok(text) => {
                    let text = strip_ansi(&text);
                    let lines: Vec<&str> = text.lines().collect();
                    let tail = lines[lines.len().saturating_sub(LINES)..].join("\n");
                    format!("--- {} ---\n{tail}", path.display())
                }
                Err(e) => format!("--- {}: {e} ---", path.display()),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Log a phase result and print it to stderr, which `--no-capture` shows live.
fn progress(message: String) {
    info!(target: "upgrade-test", "{message}");
    eprintln!("upgrade: {message}");
}

/// The light transfer load, running on its own task so the blocking waits of the test task do not
/// pause it. Dropping it stops the load.
struct LightLoad(JoinHandle<()>);

impl LightLoad {
    /// Send one transfer per round through each of `urls`, each from its own sender in
    /// [`light_load_senders`].
    fn start(urls: &[String], chain: Arc<RethChainSpec>) -> eyre::Result<Self> {
        let providers = urls
            .iter()
            .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
            .collect::<eyre::Result<Vec<_>>>()?;
        let mut senders = light_load_senders();
        Ok(Self(tokio::spawn(async move {
            match drive_light_tx_load(&providers, &mut senders, chain).await {}
        })))
    }
}

impl Drop for LightLoad {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One light-load sender per validator.
fn light_load_senders() -> Vec<TransactionFactory> {
    (0..VALIDATORS as u64)
        .map(|i| {
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(LOAD_SEED + i))
        })
        .collect()
}

/// The foreground workload: value transfers around a ring of accounts, and a token contract that
/// is deployed once and then called every round.
struct Workload {
    /// Each account sends to the next one.
    ring: Vec<TransactionFactory>,
    /// Deploys the token and holds its admin and minter roles.
    deployer: TransactionFactory,
    /// Token recipients and spenders; they never send transactions.
    holders: Vec<Address>,
    /// Token balance each holder should have.
    minted: Vec<U256>,
    /// The token proxy, once deployed.
    token: Option<Address>,
    /// Rounds sent so far.
    rounds: u64,
}

impl Workload {
    fn new() -> Self {
        let ring = (0..RING_ACCOUNTS)
            .map(|i| {
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(RING_SEED + i))
            })
            .collect();
        let deployer =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(DEPLOYER_SEED));
        let holders: Vec<Address> = (0xa1..=0xa3).map(|byte| Address::from([byte; 20])).collect();
        let minted = vec![U256::ZERO; holders.len()];
        Self { ring, deployer, holders, minted, token: None, rounds: 0 }
    }

    /// The accounts that pay gas, to be funded at genesis.
    fn senders(&self) -> impl Iterator<Item = Address> + '_ {
        self.ring.iter().map(TransactionFactory::address).chain([self.deployer.address()])
    }

    fn token(&self) -> eyre::Result<Address> {
        self.token.ok_or_else(|| eyre::eyre!("the token is not deployed yet"))
    }

    /// Deploy `Stablecoin` behind an `ERC1967Proxy` through `node` and make the deployer a minter.
    /// The proxy's constructor runs `initialize` through a delegate call, which writes storage and
    /// emits events, as does the role grant.
    async fn deploy_token(&mut self, node: &str, chain: &Arc<RethChainSpec>) -> eyre::Result<()> {
        // the deployer has sent nothing before, so its first two transactions use nonces 0 and 1
        let deployer = self.deployer.address();
        let implementation = deployer.create(0);
        let proxy = deployer.create(1);

        let initialize = Stablecoin::initializeCall {
            name_: "Upgrade".to_string(),
            symbol_: "UPG".to_string(),
            decimals_: 18,
        }
        .abi_encode();
        let constructor =
            ERC1967Proxy::constructorCall { implementation, _data: initialize.into() }.abi_encode();
        let proxy_create = [artifact_bytecode("ERC1967Proxy.json")?, constructor].concat();
        let grant = Stablecoin::grantRoleCall { role: keccak256("MINTER_ROLE"), account: deployer }
            .abi_encode();

        let txs = vec![
            (
                "Stablecoin implementation deployment".to_string(),
                self.deployer.create_eip1559_encoded(
                    chain.clone(),
                    Some(DEPLOY_GAS),
                    GAS_PRICE,
                    None,
                    U256::ZERO,
                    artifact_bytecode("Stablecoin.json")?.into(),
                ),
            ),
            (
                "ERC1967Proxy deployment".to_string(),
                self.deployer.create_eip1559_encoded(
                    chain.clone(),
                    Some(DEPLOY_GAS),
                    GAS_PRICE,
                    None,
                    U256::ZERO,
                    proxy_create.into(),
                ),
            ),
            (
                "MINTER_ROLE grant".to_string(),
                self.deployer.create_eip1559_encoded(
                    chain.clone(),
                    None,
                    GAS_PRICE,
                    Some(proxy),
                    U256::ZERO,
                    grant.into(),
                ),
            ),
        ];
        let receipts = submit_and_confirm(node, txs).await?;
        eyre::ensure!(
            receipts[0].contract_address == Some(implementation)
                && receipts[1].contract_address == Some(proxy),
            "contracts landed at {:?} and {:?}, expected {implementation} and {proxy}",
            receipts[0].contract_address,
            receipts[1].contract_address,
        );
        for receipt in &receipts[1..] {
            require_token_logs(receipt, proxy)?;
        }
        let provider = ProviderBuilder::new().connect_http(node.parse()?);
        eyre::ensure!(
            !provider.get_code_at(proxy).await?.is_empty(),
            "no code at the token proxy {proxy}"
        );
        self.token = Some(proxy);
        progress(format!(
            "token deployed at {proxy} (implementation {implementation}) in blocks {:?}",
            receipts.iter().map(|receipt| receipt.block_number).collect::<Vec<_>>()
        ));
        Ok(())
    }

    /// Send one round through `node` and wait for every transaction in it: a transfer from each
    /// ring account to the next, a mint to one holder and an approval for another. Then read the
    /// minted holder's balance back through `eth_call`.
    async fn round(&mut self, node: &str, chain: &Arc<RethChainSpec>) -> eyre::Result<()> {
        let token = self.token()?;
        let round = self.rounds;
        self.rounds += 1;

        let mut txs = Vec::new();
        let ring_len = self.ring.len();
        for k in 0..ring_len {
            let to = self.ring[(k + 1) % ring_len].address();
            // distinct amounts, so every transfer moves state differently
            let value = U256::from(WEI_PER_TEL) + U256::from(round * 100 + k as u64);
            txs.push((
                format!("round {round} transfer {k} -> {}", (k + 1) % ring_len),
                self.ring[k].create_eip1559_encoded(
                    chain.clone(),
                    None,
                    GAS_PRICE,
                    Some(to),
                    value,
                    Bytes::new(),
                ),
            ));
        }

        let minted_holder = round as usize % self.holders.len();
        let approved_holder = (minted_holder + 1) % self.holders.len();
        let amount = U256::from(1_000 + round);
        let mint = Stablecoin::mintToCall { account: self.holders[minted_holder], value: amount }
            .abi_encode();
        let approve =
            Stablecoin::approveCall { spender: self.holders[approved_holder], value: amount }
                .abi_encode();
        for (label, calldata) in [("mintTo", mint), ("approve", approve)] {
            txs.push((
                format!("round {round} {label}"),
                self.deployer.create_eip1559_encoded(
                    chain.clone(),
                    None,
                    GAS_PRICE,
                    Some(token),
                    U256::ZERO,
                    calldata.into(),
                ),
            ));
        }

        let receipts = submit_and_confirm(node, txs).await?;
        for receipt in &receipts[ring_len..] {
            require_token_logs(receipt, token)?;
        }
        self.minted[minted_holder] += amount;

        let provider = ProviderBuilder::new().connect_http(node.parse()?);
        let holder = self.holders[minted_holder];
        let call = TransactionRequest::default()
            .to(token)
            .input(Bytes::from(Stablecoin::balanceOfCall { account: holder }.abi_encode()).into());
        let balance = Stablecoin::balanceOfCall::abi_decode_returns(&provider.call(call).await?)?;
        eyre::ensure!(
            balance == self.minted[minted_holder],
            "round {round}: {node} reports a token balance of {balance} for {holder}, expected {}",
            self.minted[minted_holder],
        );
        let blocks: Vec<_> = receipts.iter().filter_map(|receipt| receipt.block_number).collect();
        info!(target: "upgrade-test", round, node, ?blocks, "workload round landed");
        Ok(())
    }
}

/// Creation bytecode of a `tn-contracts` artifact.
fn artifact_bytecode(artifact: &str) -> eyre::Result<Vec<u8>> {
    let json =
        fetch_file_content_relative_to_manifest(format!("../../tn-contracts/artifacts/{artifact}"));
    let code = RethEnv::fetch_value_from_json_str(&json, Some("bytecode.object"))?;
    let code =
        code.as_str().ok_or_else(|| eyre::eyre!("{artifact}: bytecode.object is not a string"))?;
    Ok(hex::decode(code)?)
}

/// Require `receipt` to carry at least one log, all emitted by `token`.
fn require_token_logs(receipt: &TransactionReceipt, token: Address) -> eyre::Result<()> {
    let logs = receipt.inner.logs();
    eyre::ensure!(
        !logs.is_empty() && logs.iter().all(|log| log.address() == token),
        "transaction {} emitted {} logs, expected at least one and all from the token {token}: \
         {logs:?}",
        receipt.transaction_hash,
        logs.len(),
    );
    Ok(())
}

/// Send each labelled raw transaction through `node`, then wait for every receipt and require
/// each one to have succeeded. Returns the receipts in the order given.
async fn submit_and_confirm(
    node: &str,
    txs: Vec<(String, Vec<u8>)>,
) -> eyre::Result<Vec<TransactionReceipt>> {
    let provider = ProviderBuilder::new().connect_http(node.parse()?);
    let mut sent = Vec::with_capacity(txs.len());
    for (label, raw) in txs {
        let pending = provider
            .send_raw_transaction(&raw)
            .await
            .wrap_err_with(|| format!("{node} refused the {label}"))?;
        sent.push((label, *pending.tx_hash()));
    }

    let mut receipts = Vec::with_capacity(sent.len());
    for (label, hash) in sent {
        let deadline = Instant::now() + RECEIPT_TIMEOUT;
        let receipt = loop {
            if let Some(receipt) = provider.get_transaction_receipt(hash).await? {
                break receipt;
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "the {label} ({hash}) sent through {node} has no receipt after {RECEIPT_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        };
        eyre::ensure!(
            receipt.status(),
            "the {label} ({hash}) sent through {node} reverted in block {:?}",
            receipt.block_number
        );
        receipts.push(receipt);
    }
    Ok(receipts)
}

/// Upgrade a live four-validator network from `TN_BIN_PATH_PREV` to `TN_BIN_PATH` in place, one
/// validator at a time, and require every node, observers on both binaries included, to agree on
/// every block. See the module docs for the phases.
#[ignore = "mixed-binary e2e test: needs TN_BIN_PATH_PREV, run with make test-upgrade"]
#[tokio::test(flavor = "multi_thread")]
async fn test_upgrade_rolling_in_place() -> eyre::Result<()> {
    let Some(prev) = e2e_tests::get_previous_telcoin_network_binary() else {
        warn!(target: "upgrade-test", "TN_BIN_PATH_PREV is not set; skipping the rolling upgrade test");
        eprintln!(
            "upgrade: SKIPPED test_upgrade_rolling_in_place: TN_BIN_PATH_PREV is not set. Build \
             the previous binary with `make build-e2e-bin-prev` and export the path it prints, or \
             run `make test-upgrade`."
        );
        return Ok(());
    };
    let _permit = acquire_test_permit();
    let binaries = Binaries::new(prev, e2e_tests::get_telcoin_network_binary())?;
    progress(format!(
        "PREV is commit {}, CURRENT is commit {}",
        binaries.prev_sha, binaries.current_sha
    ));

    let temp_dir = tempfile::TempDir::with_prefix(TEST)?;
    let mut guard = ProcessGuard::empty();
    let started = Instant::now();
    let result = run_rolling_upgrade(temp_dir.path(), &binaries, &mut guard).await;
    match result {
        Ok(()) => {
            guard.finish();
            progress(format!("passed in {:.0?}", started.elapsed()));
            Ok(())
        }
        Err(error) => {
            let log_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("test_logs").join(TEST);
            let kept = temp_dir.keep();
            Err(error.wrap_err(format!(
                "rolling upgrade failed after {:.0?}; datadirs kept at {}; node logs in {}",
                started.elapsed(),
                kept.display(),
                log_dir.display(),
            )))
        }
    }
}

/// The phases of [`test_upgrade_rolling_in_place`], on a network configured under `temp_path`.
async fn run_rolling_upgrade(
    temp_path: &Path,
    binaries: &Binaries,
    guard: &mut ProcessGuard,
) -> eyre::Result<()> {
    let mut workload = Workload::new();
    let funding = U256::from(parse_ether("10_000")?);
    let accounts = workload
        .senders()
        .chain(light_load_senders().iter().map(TransactionFactory::address))
        .map(|address| (address, GenesisAccount::default().with_balance(funding)))
        .collect();
    config_local_testnet_with_epoch_duration(
        temp_path,
        Some(PASSPHRASE.to_string()),
        Some(accounts),
        Some(EPOCH_SECS as u32),
    )?;
    // the harness configures one observer; the PREV observer needs an identity of its own
    config_observer(temp_path, PREV_OBSERVER, Some(PASSPHRASE.to_string()), 1)?;
    let genesis: Genesis = Config::load_from_path(
        temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let mut net = Network { temp_path, binaries, guard, nodes: Vec::new() };
    // execution head of validator-2 at the end of each phase, for the summary
    let mut marks: Vec<(String, u64)> = Vec::new();

    // phase 1: all four validators on PREV
    for _ in 0..VALIDATORS {
        net.start_validator(Release::Prev)?;
    }
    for idx in 0..VALIDATORS {
        net.wait_ready(idx).await?;
    }
    for node in net.validators() {
        tokio::task::block_in_place(|| wait_for_node_mode(&node.url, NodeMode::CvvActive))?;
    }
    let urls: Vec<String> = net.validators().iter().map(|node| node.url.clone()).collect();
    let load = LightLoad::start(&urls, chain.clone())?;
    let phase = "phase 1 (all PREV)";
    workload.deploy_token(&net.nodes[0].url, &chain).await.wrap_err(phase)?;
    workload.round(&net.nodes[1].url, &chain).await.wrap_err(phase)?;
    net.cross_epoch_boundary(phase).await?;
    workload.round(&net.nodes[2].url, &chain).await.wrap_err(phase)?;
    marks.push((phase.to_string(), get_block_number(&net.nodes[1].url)?));

    // phase 2: upgrade one validator at a time; an epoch closes after each upgrade
    for idx in 0..VALIDATORS {
        let phase = format!("phase 2 (upgrade validator-{})", idx + 1);
        net.restart_validator(idx, Release::Current, &phase).await?;
        // the transactions enter through the upgraded validator's worker, so they land only if it
        // seals batches and its primary's headers are certified
        workload.round(&net.nodes[idx].url, &chain).await.wrap_err_with(|| phase.clone())?;
        net.cross_epoch_boundary(&phase).await?;
        workload
            .round(&net.nodes[(idx + 2) % VALIDATORS].url, &chain)
            .await
            .wrap_err_with(|| phase.clone())?;
        marks.push((
            format!("{phase}, committee {}", net.committee()),
            get_block_number(&net.nodes[1].url)?,
        ));
    }

    // phase 3: all four on CURRENT
    let phase = "phase 3 (all CURRENT)";
    workload.round(&net.nodes[3].url, &chain).await.wrap_err(phase)?;
    let last_closed = net.cross_epoch_boundary(phase).await?;
    workload.round(&net.nodes[0].url, &chain).await.wrap_err(phase)?;
    marks.push((phase.to_string(), get_block_number(&net.nodes[1].url)?));

    // phase 4: observers with empty datadirs on both binaries execute the chain from genesis,
    // the CURRENT one through blocks executed while the committee ran PREV and the PREV one
    // through blocks executed while it ran CURRENT
    let phase = "phase 4 (observers from genesis)";
    let target = net.validator_head(None)?;
    let observers_started = Instant::now();
    let observers = [
        net.start_observer("observer", Release::Current)?,
        net.start_observer(PREV_OBSERVER, Release::Prev)?,
    ];
    for idx in observers {
        net.wait_ready(idx).await.wrap_err(phase)?;
        let url = net.nodes[idx].url.clone();
        tokio::task::block_in_place(|| wait_for_node_mode(&url, NodeMode::Observer))
            .wrap_err(phase)?;
        wait_for_head_at_least(&url, target, OBSERVER_SYNC_SECS).await.wrap_err_with(|| {
            format!("{phase}: {} did not execute up to block {target}", net.label(idx))
        })?;
        // both observers started together, so this bounds each one's sync time
        progress(format!(
            "{phase}: {} executed blocks 1..={target} from an empty datadir within {:.0?} of \
             starting",
            net.label(idx),
            observers_started.elapsed()
        ));
    }
    let observer_endpoints: Vec<NodeEndpoints> =
        net.endpoints().into_iter().skip(VALIDATORS).collect();
    assert_epoch_records_verify(&observer_endpoints, 0..=last_closed, EPOCH_RECORD_TIMEOUT_SECS)
        .await
        .wrap_err(phase)?;

    // phase 5: every block so far, on all six nodes
    let phase = "phase 5 (compare before rollback)";
    let compared = net.assert_nodes_agree_on_blocks().await.wrap_err(phase)?;
    progress(format!(
        "{phase}: {} nodes agree on hash, stateRoot and receiptsRoot of blocks {compared:?}",
        net.nodes.len()
    ));

    // phase 6: roll validator-1 back to PREV over the datadir CURRENT wrote
    let phase = "phase 6 (rollback validator-1 to PREV)";
    net.restart_validator(ROLLBACK, Release::Prev, phase).await?;
    workload.round(&net.nodes[ROLLBACK].url, &chain).await.wrap_err(phase)?;
    net.cross_epoch_boundary(phase).await?;
    workload.round(&net.nodes[ROLLBACK].url, &chain).await.wrap_err(phase)?;
    marks.push((
        format!("{phase}, committee {}", net.committee()),
        get_block_number(&net.nodes[1].url)?,
    ));

    // phase 7: stop the load, let every node reach the same head, compare everything again
    let phase = "phase 7 (final compare)";
    drop(load);
    let head = net.wait_for_common_head().await.wrap_err(phase)?;
    let compared = net.assert_nodes_agree_on_blocks().await.wrap_err(phase)?;
    eyre::ensure!(
        *compared.end() >= head,
        "{phase}: compared only up to block {}, below the common head {head}",
        compared.end()
    );
    progress(format!(
        "{phase}: {} nodes agree on hash, stateRoot and receiptsRoot of blocks {compared:?} ({} \
         workload rounds)",
        net.nodes.len(),
        workload.rounds,
    ));
    for (idx, _) in net.nodes.iter().enumerate() {
        progress(format!("node {idx}: {}", net.label(idx)));
    }
    for (phase, head) in &marks {
        progress(format!("head at the end of {phase}: block {head}"));
    }
    Ok(())
}
