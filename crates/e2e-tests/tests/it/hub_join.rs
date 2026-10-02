//! Governance-driven qualification of a validator joining through an open hub.

use super::*;
use futures::{StreamExt as _, TryStreamExt as _};
use std::{
    net::TcpListener,
    os::unix::fs::{MetadataExt as _, PermissionsExt as _},
    process::Child,
    time::Instant,
};
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
/// Disposable numeric identity used only by the joining child on the Linux runner.
const JOIN_UID: &str = "59599";

/// State of the fixture's UDP provider ACL, restored before the runner is reused.
enum PathAclState {
    /// Only the open hub can receive UDP from the joining validator's process identity.
    Restricted {
        /// Unique chain owned by this fixture.
        chain: String,
    },
    /// The normal direct-validator path has been restored.
    Released,
}

/// Prevent genesis dial hints from bypassing the hub while records are being resolved.
struct PathAcl {
    /// Current ownership of the temporary packet rules.
    state: PathAclState,
}

impl Drop for PathAcl {
    fn drop(&mut self) {
        let _restored = self.release();
    }
}

impl PathAcl {
    /// Execute a narrowly scoped rule operation on the disposable Linux qualification runner.
    fn iptables(args: &[&str]) -> eyre::Result<()> {
        let output =
            std::process::Command::new("sudo").args(["-n", "iptables"]).args(args).output()?;
        eyre::ensure!(
            output.status.success(),
            "iptables {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    /// Extract the already validated QUIC endpoint's UDP port.
    fn ports(info: &BootstrapServer) -> eyre::Result<Vec<u16>> {
        std::iter::once(&info.primary)
            .chain(&info.workers)
            .map(|node| {
                node.network_address
                    .to_string()
                    .split("/udp/")
                    .nth(1)
                    .and_then(|tail| tail.split('/').next())
                    .ok_or_else(|| eyre::eyre!("QUIC UDP port"))
                    .and_then(|port| port.parse().map_err(Into::into))
            })
            .collect()
    }

    /// Restrict this validator alone to hub UDP ports until authenticated resolution completes.
    fn install(hub: &BootstrapServer) -> eyre::Result<Self> {
        let chain = format!("TN_JOIN_{}", std::process::id());
        let guard = Self { state: PathAclState::Restricted { chain: chain.clone() } };
        Self::iptables(&["-N", &chain])?;
        Self::ports(hub)?.into_iter().try_for_each(|port| {
            Self::iptables(&[
                "-A",
                &chain,
                "-p",
                "udp",
                "--dport",
                &port.to_string(),
                "-j",
                "ACCEPT",
            ])
        })?;
        Self::iptables(&["-A", &chain, "-j", "DROP"])?;
        Self::iptables(&[
            "-I",
            "OUTPUT",
            "-p",
            "udp",
            "-m",
            "owner",
            "--uid-owner",
            JOIN_UID,
            "-j",
            &chain,
        ])?;
        Ok(guard)
    }

    /// Restore every source-port rule, including when qualification fails.
    fn release(&mut self) -> eyre::Result<()> {
        match std::mem::replace(&mut self.state, PathAclState::Released) {
            PathAclState::Released => Ok(()),
            PathAclState::Restricted { chain } => {
                // Evaluate every cleanup operation before returning any failure.
                let removed = Self::iptables(&[
                    "-D",
                    "OUTPUT",
                    "-p",
                    "udp",
                    "-m",
                    "owner",
                    "--uid-owner",
                    JOIN_UID,
                    "-j",
                    &chain,
                ]);
                let flushed = Self::iptables(&["-F", &chain]);
                let deleted = Self::iptables(&["-X", &chain]);
                [removed, flushed, deleted]
                    .into_iter()
                    .collect::<eyre::Result<Vec<_>>>()
                    .map(|_| ())
            }
        }
    }
}

/// Restore the joining fixture's temporary directory ownership after its child exits.
struct NodeOwnership {
    /// Directory delegated to the unprivileged child.
    dir: PathBuf,
    /// Directory permissions to restore after the unprivileged child stops.
    dir_permissions: std::fs::Permissions,
    /// Fixture parent whose traversal permission was temporarily opened.
    parent: PathBuf,
    /// Parent permissions to restore on cleanup.
    parent_permissions: std::fs::Permissions,
    /// Original fixture owner's numeric user id.
    uid: u32,
    /// Original fixture owner's numeric group id.
    gid: u32,
}

impl Drop for NodeOwnership {
    fn drop(&mut self) {
        let _restored = std::process::Command::new("sudo")
            .args(["-n", "chown", "-R", &format!("{}:{}", self.uid, self.gid)])
            .arg(&self.dir)
            .status();
        let _directory_permissions =
            std::fs::set_permissions(&self.dir, self.dir_permissions.clone());
        let _permissions = std::fs::set_permissions(&self.parent, self.parent_permissions.clone());
    }
}

impl NodeOwnership {
    /// Delegate only this temporary node directory to the disposable numeric child identity.
    fn acquire(parent: &Path, dir: &Path) -> eyre::Result<Self> {
        let metadata = std::fs::metadata(dir)?;
        let guard = Self {
            dir: dir.to_owned(),
            dir_permissions: metadata.permissions(),
            parent: parent.to_owned(),
            parent_permissions: std::fs::metadata(parent)?.permissions(),
            uid: metadata.uid(),
            gid: metadata.gid(),
        };
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o711))?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o711))?;
        let changed = std::process::Command::new("sudo")
            .args(["-n", "chown", "-R", &format!("{JOIN_UID}:{JOIN_UID}")])
            .arg(dir)
            .output()?;
        eyre::ensure!(
            changed.status.success(),
            "fixture chown: {}",
            String::from_utf8_lossy(&changed.stderr)
        );
        Ok(guard)
    }
}

/// Preserve explicit fork overrides while launching the child without root privileges.
fn restricted_command(
    original: std::process::Command,
    pid_file: &Path,
    executable: &Path,
) -> std::process::Command {
    let mut command = std::process::Command::new("sudo");
    command.args([
        "-n",
        "setpriv",
        &format!("--reuid={JOIN_UID}"),
        &format!("--regid={JOIN_UID}"),
        "--clear-groups",
        "env",
    ]);
    command.args(
        original
            .get_envs()
            .filter(|(_, value)| value.is_none())
            .flat_map(|(key, _)| [std::ffi::OsString::from("-u"), key.to_os_string()]),
    );
    command.args(original.get_envs().filter_map(|(key, value)| {
        value.map(|value| {
            let mut assignment = key.to_os_string();
            assignment.push("=");
            assignment.push(value);
            assignment
        })
    }));
    command
        .args(["sh", "-c", "umask 022; echo $$ > \"$1\"; shift; exec \"$@\"", "hub-join"])
        .arg(pid_file)
        .arg(executable)
        .args(original.get_args());
    original.get_current_dir().into_iter().for_each(|dir| {
        command.current_dir(dir);
    });
    command
}

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
    /// Restore the temporary ownership after this child has stopped.
    _ownership: Option<NodeOwnership>,
    /// Sole-hub restriction until this validator's authenticated record resolution completes.
    path_acl: Option<PathAcl>,
    /// Actual unprivileged node PID, distinct from a possible sudo monitor process.
    restricted_pid: Option<PathBuf>,
}

impl Drop for QualificationNode {
    fn drop(&mut self) {
        self.restricted_pid
            .as_ref()
            .and_then(|path| std::fs::read_to_string(path).ok())
            .and_then(|pid| pid.trim().parse::<u32>().ok())
            .filter(|pid| {
                std::fs::read_to_string(format!("/proc/{pid}/status")).is_ok_and(|status| {
                    status.lines().any(|line| {
                        line.starts_with("Uid:") && line.split_whitespace().nth(1) == Some(JOIN_UID)
                    })
                })
            })
            .into_iter()
            .for_each(|pid| {
                let _terminated = std::process::Command::new("sudo")
                    .args(["-n", "kill", "-KILL", "--", &pid.to_string()])
                    .status();
            });
        let _termination = self.process.kill();
        let _reaped = self.process.wait();
    }
}

impl QualificationNode {
    /// Permit direct validator UDP traffic after the sole-hub resolution proof.
    fn release_paths(&mut self) -> eyre::Result<()> {
        self.path_acl.as_mut().map(PathAcl::release).transpose().map(|_| ())
    }
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
    let (ownership, path_acl, restricted_pid) = if name == NEW_VALIDATOR {
        // The runner's home directory is private, so execute an identical copy under /tmp.
        let executable = dir.join("qualification-node");
        std::fs::copy(command.get_program(), &executable)?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))?;
        let ownership = NodeOwnership::acquire(base, &dir)?;
        let hub = bootstraps.values().next().ok_or_else(|| eyre::eyre!("sole open hub"))?;
        let path_acl = PathAcl::install(hub)?;
        let pid_file = dir.join("qualification.pid");
        command = restricted_command(command, &pid_file, &executable);
        (Some(ownership), Some(path_acl), Some(pid_file))
    } else {
        (None, None, None)
    };
    let attempt = std::env::var("HUB_JOIN_QUALIFICATION_ATTEMPT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    e2e_tests::setup_log_dir(&mut command, name, "hub_join", attempt);
    drop((first, second, metric));
    command
        .spawn()
        .map(|process| QualificationNode {
            process,
            rpcs,
            metrics,
            info,
            _ownership: ownership,
            path_acl,
            restricted_pid,
        })
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
    let mut joining =
        start_qualification_node(base, NEW_VALIDATOR, AdmissionMode::Closed, &bootstrap)?;
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
        let fresh =
            joining.all_swarms("tn_network_admission_resolved_window", 5.0).unwrap_or(false);
        let current = nodes.iter().all(|node| {
            node.all_swarms("tn_network_admission_resolved_window", 5.0).unwrap_or(false)
        });
        Ok(fresh && current)
    })
    .await?;
    let resolved = started.elapsed();
    joining.release_paths()?;
    wait_until(
        CONNECTION_BUDGET,
        "direct current-validator connections on every swarm",
        || async {
            joining.all_swarms("tn_network_admission_connected_current", 3.0).or(Ok(false))
        },
    )
    .await?;
    let connected = started.elapsed();
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider.clone());
    let author = AuthorityIdentifier::from(joining.info.bls_public_key);
    wait_until(
        CONSENSUS_BUDGET,
        "current activation, closed swarms, and own consensus leader",
        || async {
            let current = registry
                .getCurrentEpochInfo()
                .call()
                .await
                .is_ok_and(|info| info.committee.contains(&new_validator.address()));
            let closed = joining.all_swarms("tn_network_admission_mode", 2.0).unwrap_or(false);
            let leads = qualification_header(joining.rpc()?)
                .is_ok_and(|header| header.sub_dag.leader().author() == &author);
            Ok(current && closed && leads)
        },
    )
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
        joining.all_swarms("tn_network_admission_connected_current", 3.0).or(Ok(false))
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
            "provider_acl": "joining process UDP restricted to open hub until authenticated resolution",
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
