//! Captured SIGKILL reproductions for issue #1510, with restart across an epoch boundary.

mod process;

use alloy::providers::{Provider, ProviderBuilder};
use std::{
    fs::{File, OpenOptions},
    io::Write,
    os::unix::process::ExitStatusExt,
    path::Path,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tn_test_utils::wait_until;
use tn_types::{get_available_tcp_port, Address, NodeMode, MIN_PROTOCOL_BASE_FEE};

use super::common::{
    address_from_word, current_epoch, epoch_seconds_remaining, get_block, get_block_number,
    get_key, get_latest_consensus_header_number, get_node_mode, get_tx_receipt_block,
    scrape_metrics, send_tel, start_validator_with_args, wait_for_rpc, ProcessGuard,
};

/// Where to interrupt the validator relative to its measured epoch schedule.
#[derive(Clone, Copy)]
enum KillPhase {
    /// Send SIGKILL shortly before the close.
    BeforeClose,
    /// Send SIGKILL halfway through the epoch.
    MidEpoch,
}

/// Timing overrides for a short local run or the fleet's 20-minute epochs.
struct Timing {
    /// Epoch duration configured in genesis.
    epoch: Duration,
    /// Remaining time at which the late-kill case sends SIGKILL.
    before_close: Duration,
    /// Minimum downtime, with a separate assertion that a boundary was missed.
    downtime: Duration,
    /// Maximum catch-up time after restart.
    recovery: Duration,
}

impl Timing {
    /// Reject malformed settings and kill offsets outside the configured epoch.
    fn from_env() -> eyre::Result<Self> {
        let timing = Self {
            epoch: Duration::from_secs(setting("TN_SIGKILL_EPOCH_SECS", 30)?),
            before_close: Duration::from_secs(setting("TN_SIGKILL_BEFORE_CLOSE_SECS", 2)?),
            downtime: Duration::from_secs(setting("TN_SIGKILL_DOWNTIME_SECS", 45)?),
            recovery: Duration::from_secs(setting("TN_SIGKILL_RECOVERY_SECS", 180)?),
        };
        eyre::ensure!(timing.epoch.as_secs() >= 5, "epoch must be at least five seconds");
        eyre::ensure!(
            !timing.before_close.is_zero() && timing.before_close < timing.epoch,
            "kill offset must be strictly inside the epoch"
        );
        eyre::ensure!(!timing.recovery.is_zero(), "recovery timeout must be positive");
        Ok(timing)
    }
}

/// Parse an optional override without silently accepting a malformed value.
fn setting(name: &str, default: u64) -> eyre::Result<u64> {
    std::env::var_os(name).map_or(Ok(default), |value| {
        value
            .into_string()
            .map_err(|_| eyre::eyre!("{name} is not UTF-8"))
            .and_then(|text| text.parse().map_err(Into::into))
    })
}

/// RPC address of a local validator.
struct RpcUrl(String);

/// Prometheus address of a local validator.
struct MetricsAddr(String);

/// Addresses retained unchanged across the child's restart.
struct Endpoint {
    /// RPC listen port.
    port: u16,
    /// HTTP endpoint for execution and registry queries.
    rpc: RpcUrl,
    /// Independent Prometheus listen address.
    metrics: MetricsAddr,
}

impl Endpoint {
    /// Allocate independent ephemeral RPC and metrics ports.
    fn allocate() -> eyre::Result<Self> {
        get_available_tcp_port("127.0.0.1")
            .ok_or_else(|| eyre::eyre!("no available RPC port"))
            .and_then(|port| {
                get_available_tcp_port("127.0.0.1")
                    .ok_or_else(|| eyre::eyre!("no available metrics port"))
                    .map(|metrics| Self {
                        port,
                        rpc: RpcUrl(format!("http://127.0.0.1:{port}")),
                        metrics: MetricsAddr(format!("127.0.0.1:{metrics}")),
                    })
            })
    }
}

/// Nonce-ordered traffic submitted on each bounded observation poll.
struct Traffic {
    /// Next nonce, advanced only when the peer accepts a transaction.
    nonce: Mutex<u128>,
    /// Genesis-funded signing key.
    key: String,
    /// Recipient shared by the load and the post-restart transaction.
    recipient: Address,
}

impl Traffic {
    /// Keep the surviving committee processing transactions while observing recovery.
    fn submit(&self, rpc: &RpcUrl) -> eyre::Result<String> {
        let mut nonce =
            self.nonce.lock().map_err(|_| eyre::eyre!("traffic nonce lock poisoned"))?;
        let hash = send_tel(
            &rpc.0,
            &self.key,
            self.recipient,
            1,
            u128::from(MIN_PROTOCOL_BASE_FEE).saturating_mul(100),
            21_000,
            *nonce,
        )?;
        *nonce += 1;
        Ok(hash)
    }
}

/// Walk metadata only, recording full timestamps and sizes without opening live databases.
fn write_inventory(path: &Path, file: &mut File) -> std::io::Result<()> {
    std::fs::read_dir(path)?
        .try_for_each(|entry| {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                write_inventory(&entry.path(), file)
            } else if kind.is_file() {
                let metadata = entry.metadata()?;
                let modified = metadata
                    .modified()?
                    .duration_since(UNIX_EPOCH)
                    .map_err(std::io::Error::other)?
                    .as_millis();
                writeln!(file, "{modified}\t{}\t{}", metadata.len(), entry.path().display())
            } else {
                Ok(())
            }
        })
        // Import directories can disappear while they are sampled. Preserve that observation.
        .or_else(|error| writeln!(file, "unavailable\t{}\t{error}", path.display()))
}

/// Capture execution progress, all Prometheus series, and the killed validator's pack inventory.
async fn snapshot<P: Provider>(
    logs: &Path,
    data: &Path,
    phase: &str,
    provider: &P,
    endpoint: &Endpoint,
) -> eyre::Result<()> {
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let block = get_block_number(&endpoint.rpc.0).map_or(String::new(), |v| v.to_string());
    let epoch = current_epoch(provider).await.map_or(String::new(), |v| v.epoch_id.to_string());
    let consensus = get_latest_consensus_header_number(&endpoint.rpc.0)
        .map_or(String::new(), |v| v.to_string());
    let mode = get_node_mode(&endpoint.rpc.0).map_or(String::new(), |v| format!("{v:?}"));
    let mut csv = OpenOptions::new().append(true).open(logs.join("samples.csv"))?;
    writeln!(csv, "{timestamp},{phase},{block},{epoch},{consensus},{mode}")?;
    let metrics = scrape_metrics(&endpoint.metrics.0).unwrap_or_else(|e| e.to_string());
    std::fs::write(logs.join(format!("{timestamp}-{phase}.metrics.txt")), metrics)?;
    let mut inventory = File::create(logs.join(format!("{timestamp}-{phase}.files.tsv")))?;
    write_inventory(&data.join("validator-3"), &mut inventory).map_err(Into::into)
}

/// Start a validator with debug logs for the import and follower targets.
fn start(
    index: usize,
    endpoint: &Endpoint,
    data: &Path,
    test: &str,
    run: u32,
) -> std::process::Child {
    start_validator_with_args(
        index,
        e2e_tests::get_telcoin_network_binary(),
        data,
        endpoint.port,
        test,
        run,
        &[
            "--metrics",
            &endpoint.metrics.0,
            "--log.stdout.filter",
            "info,state-sync=debug,consensus-chain=debug,tn::observer=debug",
        ],
    )
}

/// Retain the failed node's data after the process guard has shut down the network.
async fn run_case(phase: KillPhase, test: &str) -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let timing = Timing::from_env()?;
    let temporary = tempfile::TempDir::with_prefix(test)?;
    let logs = Path::new(env!("CARGO_MANIFEST_DIR")).join("test_logs").join(test);
    std::fs::create_dir_all(&logs)?;
    std::fs::write(logs.join("samples.csv"), "unix_ms,phase,block,epoch,consensus,mode\n")?;
    let result = run_network(phase, test, &timing, temporary.path(), &logs).await;
    if result.is_err() {
        let retained = temporary.keep();
        std::fs::write(logs.join("retained-datadir.txt"), retained.to_string_lossy().as_bytes())?;
        tracing::error!(?retained, ?logs, "SIGKILL reproduction failed; data and logs retained");
    }
    result
}

/// Kill under load, prove the committee crossed the missed boundary, and verify local recovery.
async fn run_network(
    phase: KillPhase,
    test: &str,
    timing: &Timing,
    data: &Path,
    logs: &Path,
) -> eyre::Result<()> {
    e2e_tests::config_local_testnet_with_worker_fee_configs(
        data,
        Some("restart_test".to_string()),
        None,
        Some(u32::try_from(timing.epoch.as_secs())?),
        &[],
    )?;
    let endpoints = (0..4).map(|_| Endpoint::allocate()).collect::<eyre::Result<Vec<_>>>()?;
    let mut guard = ProcessGuard::new(
        endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| start(index, endpoint, data, test, 0))
            .collect(),
    );
    let peer = endpoints.first().ok_or_else(|| eyre::eyre!("missing peer endpoint"))?;
    let victim = endpoints.get(2).ok_or_else(|| eyre::eyre!("missing victim endpoint"))?;
    let peer_provider = ProviderBuilder::new().connect_http(peer.rpc.0.parse()?);
    let victim_provider = ProviderBuilder::new().connect_http(victim.rpc.0.parse()?);
    wait_for_rpc(&peer_provider).await?;
    wait_for_rpc(&victim_provider).await?;
    wait_until(timing.epoch.saturating_mul(3), "first non-genesis epoch", || async {
        current_epoch(&peer_provider).await.map(|s| s.epoch_id >= 1)
    })
    .await?;
    snapshot(logs, data, "before-kill", &victim_provider, victim).await?;
    let kill_epoch = current_epoch(&peer_provider).await?.epoch_id.saturating_add(1);
    let remaining = match phase {
        KillPhase::BeforeClose => timing.before_close.as_secs(),
        KillPhase::MidEpoch => timing.epoch.as_secs() / 2,
    };
    let traffic = Traffic {
        nonce: Mutex::new(0),
        key: get_key("test-source"),
        recipient: address_from_word(test),
    };
    wait_until(timing.epoch.saturating_mul(3), "measured SIGKILL phase under load", || async {
        traffic.submit(&peer.rpc)?;
        let at = current_epoch(&victim_provider).await?;
        eyre::ensure!(at.epoch_id <= kill_epoch, "missed the configured kill epoch");
        epoch_seconds_remaining(&victim.rpc.0, &at)
            .map(|seconds| at.epoch_id == kill_epoch && seconds > 0 && seconds <= remaining)
    })
    .await?;
    let killed_height = get_block_number(&victim.rpc.0)?;
    let at_kill = current_epoch(&victim_provider).await?;
    eyre::ensure!(at_kill.epoch_id == kill_epoch, "validator crossed the boundary before SIGKILL");
    eyre::ensure!(
        get_node_mode(&victim.rpc.0)? == NodeMode::CvvActive,
        "victim must be active before the crash"
    );
    let mut child = guard.take(2).ok_or_else(|| eyre::eyre!("missing victim child"))?;
    let pid = child.id();
    let killed_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let status = process::kill_and_reap(&mut child).inspect_err(|_| {
        guard.replace(2, child);
    })?;
    eyre::ensure!(status.signal() == Some(9), "expected SIGKILL, observed {status:?}");
    let down_since = Instant::now();
    std::fs::write(
        logs.join("kill.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "pid": pid, "signal": status.signal(), "kill_unix_ms": killed_at,
            "epoch": kill_epoch, "block": killed_height, "configured_seconds_before_close": remaining,
            "epoch_duration_secs": timing.epoch.as_secs(), "minimum_downtime_secs": timing.downtime.as_secs(),
        }))?,
    )?;
    wait_until(timing.epoch.saturating_mul(2), "committee closes the killed epoch", || async {
        traffic.submit(&peer.rpc)?;
        current_epoch(&peer_provider).await.map(|s| s.epoch_id > kill_epoch)
    })
    .await?;
    let closed = current_epoch(&peer_provider).await?;
    let closing_height = closed.block_height.saturating_sub(1);
    eyre::ensure!(closing_height > killed_height, "SIGKILL must miss the epoch-closing block");
    std::fs::write(
        logs.join("boundary.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "observed_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "peer_epoch": closed.epoch_id, "closing_block": closing_height,
            "block": get_block(&peer.rpc.0, Some(closing_height))?,
        }))?,
    )?;
    wait_until(
        timing.downtime.saturating_add(timing.epoch),
        "minimum downtime across boundary",
        || async {
            traffic.submit(&peer.rpc)?;
            Ok(down_since.elapsed() >= timing.downtime)
        },
    )
    .await?;
    let target_epoch = current_epoch(&peer_provider).await?.epoch_id;
    let target_height = get_block_number(&peer.rpc.0)?;
    guard.replace(2, start(2, victim, data, test, 1));
    std::fs::write(
        logs.join("restart.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "restart_unix_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "downtime_ms": down_since.elapsed().as_millis(), "target_epoch": target_epoch,
            "target_block": target_height,
        }))?,
    )?;
    let sampled = Mutex::new(Instant::now());
    let recovered = wait_until(
        timing.recovery,
        "restarted validator catches up and becomes active",
        || async {
            traffic.submit(&peer.rpc)?;
            let due = {
                let mut sampled =
                    sampled.lock().map_err(|_| eyre::eyre!("sample clock lock poisoned"))?;
                if sampled.elapsed() >= Duration::from_secs(60) {
                    *sampled = Instant::now();
                    true
                } else {
                    false
                }
            };
            if due {
                snapshot(logs, data, "recovering", &victim_provider, victim).await?;
            }
            let epoch = current_epoch(&victim_provider).await;
            Ok(epoch.is_ok_and(|s| s.epoch_id >= target_epoch)
                && get_block_number(&victim.rpc.0).is_ok_and(|height| height >= target_height)
                && get_node_mode(&victim.rpc.0).is_ok_and(|mode| mode == NodeMode::CvvActive))
        },
    )
    .await;
    snapshot(logs, data, "recovery-result", &victim_provider, victim).await?;
    recovered?;
    let expected = get_block(&peer.rpc.0, Some(target_height))?;
    let actual = get_block(&victim.rpc.0, Some(target_height))?;
    let expected_hash =
        expected.get("hash").ok_or_else(|| eyre::eyre!("peer block has no hash"))?;
    let actual_hash =
        actual.get("hash").ok_or_else(|| eyre::eyre!("recovered block has no hash"))?;
    eyre::ensure!(
        actual_hash == expected_hash,
        "recovered block hash differs from the committee's chain"
    );
    let transaction = traffic.submit(&victim.rpc)?;
    wait_until(
        timing.recovery,
        "transaction submitted through recovered validator executes",
        || async { Ok(get_tx_receipt_block(&victim.rpc.0, &transaction).is_ok()) },
    )
    .await?;
    snapshot(logs, data, "recovered", &victim_provider, victim).await
}

/// Exercise the late-kill window with the validator down across the committee's close.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "four-node SIGKILL reproduction; run the dedicated crash-recovery lane"]
async fn test_sigkill_before_epoch_close_recovers() -> eyre::Result<()> {
    run_case(KillPhase::BeforeClose, "sigkill_before_close").await
}

/// Exercise an earlier kill followed by a restart in a later epoch.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "four-node SIGKILL reproduction; run the dedicated crash-recovery lane"]
async fn test_sigkill_mid_epoch_recovers() -> eyre::Result<()> {
    run_case(KillPhase::MidEpoch, "sigkill_mid_epoch").await
}
