//! Governance-driven qualification of a validator joining through an open hub.

use super::*;
use futures::{StreamExt as _, TryStreamExt as _};
use std::{net::TcpListener, process::Child, time::Instant};
use tn_config::{AdmissionConfig, AdmissionMode};
use tn_types::AuthorityIdentifier;

/// Swarms required by this fixture's on-chain worker fee configuration.
const REQUIRED_SWARMS: [&str; 3] = ["primary", "worker-0", "worker-1"];
/// Publication includes the periodic record refresh after governance notice.
const PUBLICATION_BUDGET: Duration = Duration::from_secs(120);
/// Maximum time to resolve a record after the hub has authenticated it.
const RESOLUTION_BUDGET: Duration = Duration::from_secs(60);
/// Maximum time to establish direct authenticated validator connections.
const CONNECTION_BUDGET: Duration = Duration::from_secs(60);
/// Maximum time to observe the joining primary committing a consensus leader.
const CONSENSUS_BUDGET: Duration = Duration::from_secs(120);

/// A full node with independently advertised worker RPCs and per-swarm metrics.
struct QualificationNode {
    /// Child process, terminated on every exit path.
    process: Child,
    /// Worker RPC URLs in on-chain worker-id order.
    rpcs: Vec<String>,
    /// Prometheus endpoint used to observe each swarm separately.
    metrics: String,
    /// Governance and transport bindings provisioned for this node.
    info: NodeInfo,
}

impl Drop for QualificationNode {
    fn drop(&mut self) {
        let _termination = self.process.kill();
        let _reaped = self.process.wait();
    }
}

impl QualificationNode {
    /// Stop the hub without stopping any validator.
    fn stop(&mut self) -> eyre::Result<()> {
        self.process.kill()?;
        self.process.wait().map(|_| ()).map_err(Into::into)
    }

    /// Return worker zero's RPC endpoint, also serving the consensus namespace.
    fn rpc(&self) -> eyre::Result<&str> {
        self.rpcs.first().map(String::as_str).ok_or_else(|| eyre::eyre!("worker zero RPC"))
    }

    /// Require an independently observed metric for primary and every configured worker.
    fn all_swarms(&self, metric: &str, minimum: f64) -> eyre::Result<bool> {
        let body = super::super::common::scrape_metrics(&self.metrics)?;
        REQUIRED_SWARMS.into_iter().try_fold(true, |ready, network| {
            let prefix = format!("{metric}{{");
            let label = format!("network=\"{network}\"");
            body.lines()
                .find(|line| line.starts_with(&prefix) && line.contains(&label))
                .and_then(|line| line.split_ascii_whitespace().last())
                .ok_or_else(|| eyre::eyre!("missing {metric} for {network}"))
                .and_then(|value| value.parse::<f64>().map_err(Into::into))
                .map(|value| ready && value >= minimum)
        })
    }
}

/// Configure and start one full node with an explicit bootstrap map and rollout mode.
fn start_qualification_node(
    base: &Path,
    name: &str,
    mode: AdmissionMode,
    bootstraps: &BTreeMap<tn_types::BlsPublicKey, BootstrapServer>,
) -> eyre::Result<QualificationNode> {
    let dir = base.join(name);
    let mut info: NodeInfo =
        serde_yaml::from_str(&std::fs::read_to_string(dir.join("node-info.yaml"))?)?;
    let first = TcpListener::bind("127.0.0.1:0")?;
    let second = TcpListener::bind("127.0.0.1:0")?;
    let metric = TcpListener::bind("127.0.0.1:0")?;
    let first_port = first.local_addr()?.port();
    let second_port = second.local_addr()?.port();
    let metrics = metric.local_addr()?.to_string();
    let rpcs = [first_port, second_port]
        .into_iter()
        .map(|port| format!("http://127.0.0.1:{port}"))
        .collect::<Vec<_>>();
    info.p2p_info.workers.iter_mut().zip(&rpcs).try_for_each(
        |(worker, url)| -> eyre::Result<()> {
            worker.rpc = Some(tn_types::RpcInfo { http: url.parse()?, ws: None });
            Ok(())
        },
    )?;
    Config::write_to_path(dir.join("node-info.yaml"), &info, ConfigFmt::YAML)?;
    let admission = AdmissionConfig::new(mode, Duration::from_secs(300))
        .with_transition_grace(Duration::from_secs(1));
    let mut settings = serde_json::to_value(NetworkConfig::default())?;
    settings
        .as_object_mut()
        .ok_or_else(|| eyre::eyre!("network configuration object"))?
        .insert("admission".to_owned(), serde_json::to_value(admission)?);
    let settings: NetworkConfig = serde_json::from_value(settings)?;
    Config::write_to_path(dir.join("network-config"), &settings, ConfigFmt::YAML)?;
    let bootstrap_json = serde_json::to_string(bootstraps)?;
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut command = bin.command();
    command
        .env("TN_BLS_PASSPHRASE", NODE_PASSWORD)
        .arg("--bls-passphrase-source")
        .arg("env")
        .arg("node")
        .arg("--datadir")
        .arg(&dir)
        .arg("--http")
        .arg("--http.port")
        .arg(first_port.to_string())
        .arg("--ipcdisable")
        .arg("--metrics")
        .arg(&metrics)
        .arg("--bootstrap-peers")
        .arg(bootstrap_json);
    e2e_tests::setup_log_dir(&mut command, name, "hub_join", 1);
    drop((first, second, metric));
    command
        .spawn()
        .map(|process| QualificationNode { process, rpcs, metrics, info })
        .map_err(Into::into)
}

/// Read a committed consensus header through the running node's typed serialization.
fn qualification_header(rpc: &str) -> eyre::Result<ConsensusHeader> {
    super::super::common::get_latest_consensus_header(rpc).and_then(|header| {
        serde_json::to_value(header).and_then(serde_json::from_value).map_err(Into::into)
    })
}

/// Exercise governance admission, record publication, all swarms, and actual consensus readiness.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the built full-node binary; run in the hub-join qualification lane"]
async fn hub_join_governance_two_workers() -> eyre::Result<()> {
    let _permit = super::super::common::acquire_test_permit();
    pin_fork_epochs(Some(0), None, None, None);
    let temp = tempfile::TempDir::with_prefix("hub_join_governance")?;
    let base = temp.path();
    let mut new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
    ];
    let genesis = super::super::common::create_genesis_for_test_with_workers(
        base,
        (NEW_VALIDATOR, new_validator.address()),
        governance.address(),
        &committee,
        EPOCH_DURATION,
        &["0:1:7", "1:1:7"],
    )?;
    let hub_dir = base.join("open-hub");
    e2e_tests::create_validator_info_with_workers(
        &hub_dir,
        &Address::from_slice(&[0x99; 20]).to_string(),
        Some(NODE_PASSWORD.to_owned()),
        2,
    )?;
    std::fs::create_dir_all(hub_dir.join("genesis"))?;
    ["genesis/committee.yaml", "genesis/genesis.yaml", "parameters.yaml"]
        .into_iter()
        .try_for_each(|path| {
            std::fs::copy(base.join("shared-genesis").join(path), hub_dir.join(path))
                .map(|_| ())
                .map_err(eyre::Report::from)
        })?;
    let mut hub =
        start_qualification_node(base, "open-hub", AdmissionMode::Open, &BTreeMap::new())?;
    let bootstrap = BTreeMap::from([(
        hub.info.bls_public_key,
        BootstrapServer {
            primary: hub.info.p2p_info.primary.clone(),
            workers: hub.info.p2p_info.workers.clone(),
        },
    )]);
    let nodes = committee
        .iter()
        .map(|(name, _)| start_qualification_node(base, name, AdmissionMode::Closed, &bootstrap))
        .collect::<eyre::Result<Vec<_>>>()?;
    let joining = start_qualification_node(base, NEW_VALIDATOR, AdmissionMode::Closed, &bootstrap)?;
    let existing = nodes.first().ok_or_else(|| eyre::eyre!("existing committee"))?;
    let provider = ProviderBuilder::new().connect_http(existing.rpc()?.parse()?);
    wait_until(PUBLICATION_BUDGET, "initial direct committee RPC", || async {
        Ok(provider.get_chain_id().await.is_ok())
    })
    .await?;
    let txs = generate_new_validator_txs(
        base,
        Arc::<RethChainSpec>::new(genesis.into()),
        &mut new_validator,
        &mut governance,
    )?;
    futures::stream::iter(txs)
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|tx| {
            let provider = provider.clone();
            async move {
                let pending = provider.send_raw_transaction(&tx).await?;
                timeout(PUBLICATION_BUDGET, pending.watch()).await??;
                Ok(())
            }
        })
        .await?;
    let started = Instant::now();
    wait_until(
        PUBLICATION_BUDGET,
        "governance-noticed record publication on every hub swarm",
        || async { hub.all_swarms("tn_network_admission_resolved_window", 5.0).or(Ok(false)) },
    )
    .await?;
    let published = started.elapsed();
    wait_until(RESOLUTION_BUDGET, "cold committee record resolution on every swarm", || async {
        joining.all_swarms("tn_network_admission_resolved_window", 5.0).or(Ok(false))
    })
    .await?;
    let resolved = started.elapsed();
    wait_until(
        CONNECTION_BUDGET,
        "direct current-validator connections on every swarm",
        || async {
            joining.all_swarms("tn_network_admission_connected_current", 1.0).or(Ok(false))
        },
    )
    .await?;
    let connected = started.elapsed();
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());
    wait_until(CONSENSUS_BUDGET, "on-chain current-committee activation", || async {
        registry
            .getCurrentEpochInfo()
            .call()
            .await
            .map(|info| info.committee.contains(&new_validator.address()))
            .map_err(Into::into)
    })
    .await?;
    let author = AuthorityIdentifier::from(joining.info.bls_public_key);
    wait_until(CONSENSUS_BUDGET, "new validator commits its own consensus leader", || async {
        qualification_header(joining.rpc()?)
            .map(|header| header.sub_dag.leader().author() == &author)
            .or(Ok(false))
    })
    .await?;
    let ready = started.elapsed();
    let header = qualification_header(joining.rpc()?)?;
    let epoch = header.sub_dag.leader().epoch();
    fetch_verified_epoch_record(joining.rpc()?, epoch, 120).await?;
    let before_hub_loss = provider.get_block_number().await?;
    hub.stop()?;
    wait_until(CONNECTION_BUDGET, "consensus advances after open hub loss", || async {
        provider.get_block_number().await.map(|number| number > before_hub_loss).map_err(Into::into)
    })
    .await?;
    wait_until(CONNECTION_BUDGET, "all required direct swarms survive hub loss", || async {
        joining.all_swarms("tn_network_admission_connected_current", 1.0).or(Ok(false))
    })
    .await?;
    let revision = std::process::Command::new("git").args(["rev-parse", "HEAD"]).output()?;
    eyre::ensure!(revision.status.success(), "git revision probe failed");
    let revision = String::from_utf8(revision.stdout)?;
    println!(
        "hub_join_governance_evidence={}",
        serde_json::json!({
            "revision": revision.trim(), "topology": {
                "existing_validators": 4, "joining_validators": 1, "open_hubs": 1,
                "swarms": REQUIRED_SWARMS,
            }, "conditions": {
                "transport": "loopback QUIC", "worker_fees": [7, 7],
                "transition_grace_seconds": 1, "snapshot_lease_seconds": 300,
            }, "publication_ms": published.as_millis(),
            "publication_to_resolution_ms": resolved.saturating_sub(published).as_millis(),
            "resolution_to_connection_ms": connected.saturating_sub(resolved).as_millis(),
            "connection_to_consensus_readiness_ms": ready.saturating_sub(connected).as_millis(),
            "consensus_leader_epoch": epoch, "hub_loss_preserved_consensus": true,
        })
    );
    Ok(())
}
