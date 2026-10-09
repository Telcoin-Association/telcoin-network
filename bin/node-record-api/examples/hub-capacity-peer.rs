//! Persistent qualification peers using the production QUIC, DHT, and bulk-sync implementation.
//!
//! This example uses deterministic test identities on an isolated qualification network. Its
//! control endpoint binds the address explicitly selected in the input file. The public ready
//! file records the BLS identity and each swarm identity, permitting topology and NAT review.

use axum::{
    extract::{DefaultBodyLimit, State},
    routing::post,
    Json, Router,
};
use clap::Parser;
use eyre::{eyre, Result, WrapErr};
use futures::{StreamExt, TryStreamExt};
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tn_config::{KeyConfig, LibP2pConfig, NetworkConfig};
use tn_kad_client::{BlsPublicKey, Multiaddr, NetworkType, PeerId};
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{NetworkEvent, NetworkHandle},
    write_frame, ConsensusNetwork, PeerExchangeMap, PrimarySyncRequest, SyncFrame, TNMessage,
    WorkerSyncRequest,
};
use tn_storage::mem_db::MemDatabase;
use tn_types::{try_decode, Batch, BlsKeypair, Epoch, TaskManager, B256};
use tokio::sync::{mpsc, watch, Mutex, Semaphore};

// Standalone examples acknowledge the package's other dependencies without relaxing its lints.
use humantime as _;
use hyper as _;
use hyper_util as _;
use metrics as _;
use reqwest as _;
use serde_yaml as _;
#[cfg(any(target_os = "android", target_os = "linux"))]
use socket2 as _;
use tempfile as _;
use thiserror as _;
use tn_metrics as _;
use tn_node_record as _;
use tn_node_record_api as _;
use tn_test_utils as _;
use tower as _;
use tower_http as _;
use tracing as _;
use url as _;

/// Maximum Unicode characters retained from a native dial rejection.
const MAX_DIAL_ERROR_CHARS: usize = 256;

/// Command-line input paths, frozen into the workload manifest.
#[derive(Parser)]
struct Args {
    /// Export a public test identity or start a persistent protocol peer.
    #[command(subcommand)]
    mode: Mode,
}

/// Public identity generation is separate from deployment, allowing the DAO set to be frozen first.
#[derive(clap::Subcommand)]
enum Mode {
    /// Print only public identities derived from a deterministic qualification seed.
    Identity {
        /// Public deterministic seed, never an operational key.
        #[arg(long)]
        seed: u64,
    },
    /// Start three persistent production swarms and the bounded private control server.
    Run(RunArgs),
}

/// Exact deployment inputs for one isolated qualification peer.
#[derive(clap::Args)]
struct RunArgs {
    /// Peer configuration on the isolated network.
    #[arg(long)]
    config: PathBuf,
    /// Exclusively created public identity and listener report.
    #[arg(long)]
    ready: PathBuf,
}

/// Deterministic identity and deployment bindings for one ordinary or DAO peer.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// Public deterministic test seed, never an operational key.
    seed: u64,
    /// Chain and bootstrap configuration shared by its three swarms.
    network: NetworkConfig,
    /// Genesis chain ID, stamped after the network configuration is deserialized.
    chain_id: u64,
    /// Primary, worker-0, and worker-1 QUIC listeners, each with a fixed nonzero port.
    listen: Vec<Multiaddr>,
    /// Private control address reachable by the local qualification coordinator.
    control: SocketAddr,
    /// Hub identities whose live connectivity must be checked.
    required_hubs: Vec<BlsPublicKey>,
    /// Hub whose signed records and complete epoch pack are requested.
    target: BlsPublicKey,
    /// A completed epoch available before measurement begins.
    sync_epoch: Epoch,
}

impl Config {
    /// Load bounded deployment inputs and apply the genesis-derived protocol domain.
    fn read(path: &std::path::Path) -> Result<Self> {
        if std::fs::metadata(path)?.len() > 128 * 1024 {
            Err(eyre!("peer configuration exceeds 128 KiB"))?;
        }
        let mut config: Self = serde_json::from_reader(File::open(path)?)?;
        config.network.set_chain_id(config.chain_id);
        Ok(config)
    }
}

/// Peer-exchange compatibility; these read-only peers never issue consensus RPC messages.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Message {
    /// Legacy fallback, with current peers using the separate peer-exchange protocol.
    PeerExchange(PeerExchangeMap),
}

impl TNMessage for Message {
    fn peer_exchange_msg(&self) -> Option<PeerExchangeMap> {
        match self {
            Self::PeerExchange(peers) => Some(peers.clone()),
        }
    }
}

impl From<PeerExchangeMap> for Message {
    fn from(peers: PeerExchangeMap) -> Self {
        Self::PeerExchange(peers)
    }
}

/// Cloneable command handle for a persistent swarm.
type Handle = NetworkHandle<Message, Message>;
/// Production network behavior with a finite in-memory store and drained event channel.
type Network =
    ConsensusNetwork<Message, Message, MemDatabase, mpsc::Sender<NetworkEvent<Message, Message>>>;

/// Stable telemetry names, shared with the frozen three-swarm bindings.
fn role_name(role: NetworkType) -> String {
    match role {
        NetworkType::Primary => "primary".to_owned(),
        NetworkType::Worker(id) => format!("worker-{id}"),
    }
}

/// Required workload classes; unsupported classes fail explicitly until their adapters exist.
#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Scenario {
    /// A peer joins the public hub with its persistent identity.
    PublicJoin,
    /// The same peer reconnects through its declared shared NAT.
    SharedNatReconnect,
    /// Delivery through distinct authenticated forwarding identities.
    GossipTwoHops,
    /// Signature-validated record queries on all three DHTs.
    RecordLookup,
    /// Signature-validated worker RPC endpoint resolution.
    SubmitUrlLookup,
    /// A completed epoch is transferred through the production stream protocol.
    ConcurrentSync,
    /// Concurrent committee request completion.
    CommitteeProgress,
    /// Both hub connections remain established on every swarm.
    DaoConnectivity,
}

/// Nonce-bound operation request from the workload coordinator.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    /// Frozen workload operation identifier.
    operation_id: String,
    /// Required workload class.
    scenario: Scenario,
    /// The measurement window's system-clock origin, excluding warmup observations.
    not_before_unix_us: u128,
    /// Executed epoch batch digests, supplied by the coordinator's retained RPC observations.
    #[serde(default)]
    batch_digests: BTreeSet<B256>,
    /// Retained source epoch for each of the selected executed batch digests.
    #[serde(default)]
    batch_epochs: BTreeMap<B256, Epoch>,
    /// Completed epoch selected by the coordinator's declared executed-batch selection rule.
    sync_epoch: Option<Epoch>,
}

/// One persistent peer, with separate swarm handles and bounded control concurrency.
struct Peer {
    /// Actual public identity bound to successful workload acknowledgements.
    identity: String,
    /// Exact deployment and target bindings.
    config: Config,
    /// Primary and both worker command handles.
    handles: Vec<(NetworkType, Handle)>,
    /// No waiting tasks are admitted by the control handler.
    slots: Arc<Semaphore>,
    /// Latest accepted two-hop receipt, with one retained value and one serialized consumer.
    gossip: Mutex<watch::Receiver<Option<Value>>>,
}

/// Wait for the existing peer manager to report the authenticated target as connected.
async fn connected(handle: &Handle, target: BlsPublicKey, expected: bool) -> Result<()> {
    let polls = futures::stream::unfold(
        (handle.clone(), tokio::time::interval(Duration::from_millis(20))),
        |(handle, mut interval)| async move {
            interval.tick().await;
            let peers = handle.connected_peers().await;
            Some((peers, (handle, interval)))
        },
    )
    .try_filter(move |peers| futures::future::ready(peers.contains(&target) == expected));
    let mut polls = Box::pin(polls);
    tokio::time::timeout(Duration::from_secs(8), polls.try_next())
        .await??
        .ok_or_else(|| eyre!("peer observation stream ended"))?;
    Ok(())
}

/// Wait for both authenticated peer absence and completion of the physical connection close.
async fn disconnected(handle: &Handle, target: BlsPublicKey, peer: PeerId) -> Result<()> {
    let polls = futures::stream::unfold(
        (handle.clone(), tokio::time::interval(Duration::from_millis(20))),
        move |(handle, mut interval)| async move {
            interval.tick().await;
            let closed =
                futures::future::try_join(handle.connected_peers(), handle.is_peer_connected(peer))
                    .await
                    .map(|(peers, physically_connected)| {
                        !peers.contains(&target) && !physically_connected
                    });
            Some((closed, (handle, interval)))
        },
    )
    .try_filter(|closed| futures::future::ready(*closed));
    let mut polls = Box::pin(polls);
    tokio::time::timeout(Duration::from_secs(8), polls.try_next())
        .await??
        .ok_or_else(|| eyre!("peer observation stream ended"))?;
    Ok(())
}

/// Re-dial the validated peer only after its previous physical connection has closed.
async fn disconnect_and_redial(
    handle: &Handle,
    role: NetworkType,
    target: BlsPublicKey,
    peer: PeerId,
) -> Result<()> {
    handle.disconnect_peer(peer).await?;
    disconnected(handle, target, peer).await?;
    dial_and_confirm(handle, role, target, handle.dial_by_bls(target)).await
}

/// Retain native dial detail that the existing error's Display deliberately omits.
fn bounded_dial_cause(error: &NetworkError) -> String {
    if let NetworkError::Dial(detail) = error {
        detail.chars().take(MAX_DIAL_ERROR_CHARS).collect()
    } else {
        error.to_string().chars().take(MAX_DIAL_ERROR_CHARS).collect()
    }
}

/// Preserve typed error propagation while exposing bounded detail at the fixture boundary.
fn bounded_report_cause(error: &eyre::Report) -> String {
    error
        .downcast_ref::<NetworkError>()
        .map(bounded_dial_cause)
        .unwrap_or_else(|| error.to_string().chars().take(MAX_DIAL_ERROR_CHARS).collect())
}

/// Confirm the authenticated target after a successful or potentially raced dial.
async fn dial_and_confirm<F>(
    handle: &Handle,
    role: NetworkType,
    target: BlsPublicKey,
    dial: F,
) -> Result<()>
where
    F: Future<Output = std::result::Result<(), NetworkError>>,
{
    let outcome = dial.await.inspect_err(|error| {
        tracing::warn!(target: "hub_capacity::peer", event = "peer_dial_rejection",
            phase = "confirmation", role = %role_name(role), peer = %target, outcome = "rejected",
            cause = %bounded_dial_cause(error),
            "native dial rejected");
    });
    if outcome.as_ref().is_err_and(|error| {
        !matches!(
            error,
            NetworkError::AlreadyDialing(_)
                | NetworkError::AlreadyConnected(_)
                | NetworkError::Dial(_)
        )
    }) {
        outcome
            .inspect_err(|error| {
                tracing::error!(target: "hub_capacity::peer", event = "peer_dial_confirmation",
                    phase = "confirmation", role = %role_name(role), peer = %target,
                    confirmation = "not_attempted",
                    cause = %bounded_dial_cause(error),
                    "native dial failure prevents authenticated confirmation");
            })
            .map_err(eyre::Report::from)
    } else {
        connected(handle, target, true)
            .await
            .inspect_err(|error| {
                tracing::error!(target: "hub_capacity::peer", event = "peer_dial_confirmation",
                    phase = "confirmation", role = %role_name(role), peer = %target, confirmation = "failed",
                    cause = %error.to_string().chars().take(MAX_DIAL_ERROR_CHARS).collect::<String>(),
                    native_cause = %outcome.as_ref().err().map(bounded_dial_cause).unwrap_or_default(),
                    "authenticated target confirmation failed");
            })
            .map_err(|confirmation_error| {
                outcome.err().map(eyre::Report::from).unwrap_or(confirmation_error)
            })
    }
}

/// Accept queued startup dials and recover opaque dial failures with one bounded same-target retry.
/// Race rejections still require authenticated target observation.
async fn startup_dial<F>(
    handle: &Handle,
    role: NetworkType,
    target: BlsPublicKey,
    dial: F,
) -> Result<()>
where
    F: Future<Output = std::result::Result<(), NetworkError>>,
{
    let outcome = dial.await;
    let initial_dial_cause = outcome.as_ref().err().map(bounded_dial_cause);
    let retry_count = if outcome.as_ref().is_err_and(|error| matches!(error, NetworkError::Dial(_)))
    {
        1
    } else {
        0
    };
    let needs_confirmation = outcome.as_ref().is_err_and(|error| {
        matches!(
            error,
            NetworkError::Dial(_)
                | NetworkError::AlreadyDialing(_)
                | NetworkError::AlreadyConnected(_)
        )
    });
    let outcome = outcome.inspect_err(|error| {
        tracing::warn!(target: "hub_capacity::peer", event = "peer_dial_rejection",
            phase = "startup", role = %role_name(role), peer = %target, outcome = "rejected", retry_count,
            cause = %bounded_dial_cause(error),
            "native dial rejected");
    });
    let result = if outcome.is_ok() {
        Ok(())
    } else if outcome.as_ref().is_err_and(|error| matches!(error, NetworkError::Dial(_))) {
        // The retry and authenticated observation share the existing recovery window.
        tokio::time::timeout(
            Duration::from_secs(8),
            dial_and_confirm(handle, role, target, handle.dial_by_bls(target)),
        )
        .await
        .map_err(|error| {
            tracing::error!(target: "hub_capacity::peer", event = "peer_startup_recovery",
                role = %role_name(role), peer = %target, retry_count,
                confirmation = "deadline",
                native_cause = initial_dial_cause.as_deref().unwrap_or_default(),
                "startup recovery window elapsed");
            eyre::Report::from(error)
        })
        .and_then(std::convert::identity)
        .or_else(|_| outcome.map_err(eyre::Report::from))
    } else {
        dial_and_confirm(handle, role, target, futures::future::ready(outcome)).await
    };
    if needs_confirmation {
        tracing::warn!(target: "hub_capacity::peer", event = "peer_startup_recovery",
            role = %role_name(role), peer = %target, retry_count,
            confirmation = if result.is_ok() { "confirmed" } else { "failed" },
            "startup authenticated recovery completed");
    }
    result.inspect_err(|error| {
        tracing::error!(target: "hub_capacity::peer", event = "peer_startup_failure",
            phase = "startup", role = %role_name(role), peer = %target, outcome = "rejected", retry_count,
            confirmation = if needs_confirmation { "failed" } else { "not_attempted" },
            cause = %bounded_report_cause(error),
            "startup dial failed");
    })
}

/// Finish every started reconnect transition before propagating a native target error.
async fn settle_reconnects<F>(transitions: impl IntoIterator<Item = F>) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    futures::future::join_all(transitions).await.into_iter().collect::<Result<Vec<_>>>().map(|_| ())
}

impl Peer {
    /// Observe the next accepted delivery through a distinct authenticated forwarding peer.
    async fn gossip(&self, not_before_unix_us: u128) -> Result<Value> {
        let mut receiver = self.gossip.lock().await;
        receiver.changed().await?;
        let observation = receiver
            .wait_for(|observation| {
                observation.as_ref().is_some_and(|observation| {
                    observation
                        .pointer("/receipt/received_unix_us")
                        .and_then(Value::as_u64)
                        .is_some_and(|received| u128::from(received) >= not_before_unix_us)
                })
            })
            .await?
            .clone();
        observation.ok_or_else(|| eyre!("gossip receipt missing"))
    }
    /// Obtain fresh, signature-validated records instead of cached RPC metadata.
    async fn records(&self, require_rpc: bool) -> Result<Value> {
        let target = self.config.target;
        let records = futures::future::try_join_all(self.handles.clone().into_iter().map(
            |(role, handle)| async move {
                let record = handle.get_node_record(target).await.wrap_err_with(|| {
                    format!("records get_node_record swarm={} target={target:?}", role_name(role))
                })?;
                if require_rpc && matches!(role, NetworkType::Worker(_)) {
                    record
                        .info
                        .rpc
                        .as_ref()
                        .ok_or_else(|| eyre!("worker record has no submit URL"))?
                        .validate()?;
                }
                Ok::<_, eyre::Report>(json!({"swarm": role_name(role), "record": record}))
            },
        ))
        .await?;
        Ok(json!({"signed_records": records}))
    }

    /// Disconnect and re-dial the same validated identities, preserving this peer's keys.
    async fn reconnect(&self, require_existing: bool) -> Result<Value> {
        let targets = self
            .handles
            .iter()
            .flat_map(|(role, handle)| {
                self.config.required_hubs.iter().map(move |key| (*role, handle.clone(), *key))
            })
            .collect::<Vec<_>>();
        settle_reconnects(targets.into_iter().map(|(role, handle, key)| async move {
            let peers = handle.connected_peers().await?;
            if peers.contains(&key) {
                let record = handle.get_node_record(key).await.wrap_err_with(|| {
                    format!("reconnect get_node_record swarm={} target={key:?}", role_name(role))
                })?;
                let peer: PeerId = record.info.pubkey.into();
                disconnect_and_redial(&handle, role, key, peer).await
            } else if require_existing {
                Err(eyre!("shared-NAT restart requires an existing hub connection"))
            } else {
                dial_and_confirm(&handle, role, key, handle.dial_by_bls(key)).await
            }
        }))
        .await?;
        self.connectivity().await
    }

    /// Observe both required hub identities, independently on every live swarm.
    async fn connectivity(&self) -> Result<Value> {
        let required_hubs = self.config.required_hubs.clone();
        let observations = futures::future::try_join_all(self.handles.clone().into_iter().map(
            |(role, handle)| {
                let required_hubs = required_hubs.clone();
                async move {
                    let peers = handle.connected_peers().await?;
                    if !required_hubs.iter().all(|key| peers.contains(key)) {
                        Err(eyre!("required hub identity disconnected on {role:?}"))
                    } else {
                        Ok(json!({"swarm": role_name(role), "connected": peers}))
                    }
                }
            },
        ))
        .await?;
        Ok(json!({"connections": observations}))
    }

    /// Transfer a completed primary epoch and verify the same real batches on both worker swarms.
    async fn sync(
        &self,
        batch_digests: &BTreeSet<B256>,
        batch_epochs: &BTreeMap<B256, Epoch>,
        epoch: Epoch,
    ) -> Result<Value> {
        let batch_requests = worker_batch_requests(batch_digests, batch_epochs, epoch)?;
        let target = self.config.target;
        let limit = self.config.network.libp2p_config().max_rpc_message_size;
        let transfers = futures::future::try_join_all(self.handles.clone().into_iter().map(
            |(role, handle)| {
                let batch_digests = batch_digests.clone();
                let batch_epochs = batch_epochs.clone();
                let batch_requests = batch_requests.clone();
                async move {
                    match role {
                        NetworkType::Primary => {
                            let frames = transfer(handle, target, PrimarySyncRequest::EpochPack { epoch }, limit)
                                .await
                                .wrap_err_with(|| format!("sync transfer swarm={} source_epoch={epoch}", role_name(role)))?;
                            Ok::<_, eyre::Report>(json!({"swarm": role_name(role), "bytes": frames.iter().map(Vec::len).sum::<usize>(), "completed": true}))
                        }
                        NetworkType::Worker(_) => {
                            let (bytes, frames) = futures::stream::iter(batch_requests.into_iter().map(Ok::<_, eyre::Report>))
                                .try_fold((0usize, Vec::new()), |(total, mut frames), (source_epoch, requested)| {
                                    let handle = handle.clone();
                                    let batch_epochs = batch_epochs.clone();
                                    async move {
                                        let data = transfer(handle, target, WorkerSyncRequest::Batches { batch_digests: requested.clone(), epoch: source_epoch }, limit)
                                            .await
                                            .wrap_err_with(|| format!("sync transfer swarm={} source_epoch={source_epoch}", role_name(role)))?;
                                        verify_worker_batches(&data, &requested, &batch_epochs)?;
                                        total.checked_add(data.iter().map(Vec::len).sum::<usize>())
                                            .filter(|bytes| *bytes <= 64 * 1024 * 1024)
                                            .ok_or_else(|| eyre!("sync exceeded its 64 MiB or 1024-frame transfer bound"))
                                            .map(|total| {
                                                frames.extend(data);
                                                (total, frames)
                                            })
                                    }
                                }).await?;
                            verify_worker_batches(&frames, &batch_digests, &batch_epochs).map(|received|
                                json!({"swarm": role_name(role), "bytes": bytes, "batch_digests": received, "completed": true}))
                        }
                    }
                }
            },
        )).await?;
        Ok(json!({"epoch": epoch, "target": target, "transfers": transfers, "completed": true}))
    }
}

/// Validate retained provenance and group the unchanged selected digests by source epoch.
fn worker_batch_requests(
    batch_digests: &BTreeSet<B256>,
    batch_epochs: &BTreeMap<B256, Epoch>,
    epoch: Epoch,
) -> Result<BTreeMap<Epoch, BTreeSet<B256>>> {
    if batch_digests.len() != 4 {
        Err(eyre!("bulk qualification requires four executed batch digests"))?;
    }
    if batch_epochs.keys().copied().collect::<BTreeSet<_>>() != *batch_digests
        || batch_epochs.values().any(|source_epoch| *source_epoch < epoch)
        || !batch_epochs.values().any(|source_epoch| *source_epoch == epoch)
    {
        Err(eyre!("bulk batch source epochs do not match the selected executed batches"))?;
    }
    Ok(batch_epochs.iter().fold(BTreeMap::new(), |mut requests, (digest, source_epoch)| {
        requests.entry(*source_epoch).or_insert_with(BTreeSet::new).insert(*digest);
        requests
    }))
}

/// Every source-epoch transfer and the aggregate must contain exactly the requested real batches.
fn verify_worker_batches(
    frames: &[Vec<u8>],
    batch_digests: &BTreeSet<B256>,
    batch_epochs: &BTreeMap<B256, Epoch>,
) -> Result<BTreeSet<B256>> {
    frames
        .iter()
        .map(|data| {
            try_decode::<Batch>(data).map_err(eyre::Report::from).and_then(|batch| {
                let digest = batch.digest();
                batch_epochs
                    .get(&digest)
                    .filter(|source_epoch| **source_epoch == batch.epoch)
                    .map(|_| digest)
                    .ok_or_else(|| eyre!("worker batch did not match its retained source epoch"))
            })
        })
        .collect::<Result<BTreeSet<_>, _>>()
        .and_then(|received| {
            if received != *batch_digests || frames.len() != batch_digests.len() {
                Err(eyre!("worker transfer did not return the requested batches"))
            } else {
                Ok(received)
            }
        })
}

/// Read a finite nonempty production sync exchange, rejecting invalid ordering and oversized data.
async fn transfer<Request>(
    handle: Handle,
    target: BlsPublicKey,
    request: Request,
    limit: usize,
) -> Result<Vec<Vec<u8>>>
where
    Request: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    let mut stream = handle
        .open_stream(target)
        .await
        .wrap_err("sync stream open outer channel")?
        .wrap_err("sync stream open inner stream")?;
    let request = SyncFrame::Req(request);
    write_frame(&mut stream, &request, &mut Vec::new(), &mut Vec::new(), limit)
        .await
        .wrap_err("sync request write")?;
    let frames = futures::stream::try_unfold(
        (stream, Vec::new(), Vec::new(), false, false),
        move |(mut stream, mut plain, mut compressed, admitted, ended)| async move {
            if ended {
                Ok(None)
            } else {
                let frame: SyncFrame<Request> =
                    read_frame(&mut stream, &mut plain, &mut compressed, limit)
                        .await
                        .wrap_err("sync response read")?;
                match frame {
                    SyncFrame::Ack if !admitted => {
                        Ok(Some((None, (stream, plain, compressed, true, false))))
                    }
                    SyncFrame::Data(data) if admitted => {
                        Ok(Some((Some(data), (stream, plain, compressed, true, false))))
                    }
                    SyncFrame::End if admitted => {
                        Ok(Some((None, (stream, plain, compressed, true, true))))
                    }
                    SyncFrame::Deny(reason) => Err(eyre!("sync denied: {reason:?}")),
                    SyncFrame::Err(reason) => Err(eyre!("sync aborted: {reason:?}")),
                    SyncFrame::Req(_) | SyncFrame::Ack | SyncFrame::Data(_) | SyncFrame::End => {
                        Err(eyre!("invalid sync frame order"))
                    }
                }
            }
        },
    );
    let (total, data) = frames
        .try_fold((0usize, Vec::new()), |(total, mut data), frame| async move {
            let count = frame.as_ref().map_or(0, Vec::len);
            let total = total
                .checked_add(count)
                .filter(|bytes| *bytes <= 64 * 1024 * 1024 && data.len() < 1024)
                .ok_or_else(|| eyre!("sync exceeded its 64 MiB or 1024-frame transfer bound"))?;
            data.extend(frame);
            Ok((total, data))
        })
        .await?;
    if total == 0 {
        Err(eyre!("empty sync transfer"))
    } else {
        Ok(data)
    }
}

/// Execute a bounded command and retain protocol failures in its nonce-bound acknowledgement.
async fn command(State(peer): State<Arc<Peer>>, Json(request): Json<Command>) -> Json<Value> {
    let result = tokio::time::timeout(Duration::from_secs(29), async {
        let _permit = peer.slots.clone().try_acquire_owned()?;
        if request.operation_id.is_empty() || request.operation_id.len() > 128 {
            Err(eyre!("operation identifier must contain 1 through 128 bytes"))
        } else {
            match request.scenario {
                Scenario::PublicJoin => peer.reconnect(false).await,
                Scenario::SharedNatReconnect => peer.reconnect(true).await,
                Scenario::RecordLookup => peer.records(false).await,
                Scenario::SubmitUrlLookup => peer.records(true).await,
                Scenario::ConcurrentSync => {
                    peer.sync(
                        &request.batch_digests,
                        &request.batch_epochs,
                        request.sync_epoch.unwrap_or(peer.config.sync_epoch),
                    )
                    .await
                }
                Scenario::DaoConnectivity => peer.connectivity().await,
                Scenario::GossipTwoHops => peer.gossip(request.not_before_unix_us).await,
                Scenario::CommitteeProgress => {
                    Err(eyre!("protocol adapter for this scenario is not implemented"))
                }
            }
        }
    })
    .await
    .map_err(eyre::Report::from)
    .and_then(|result| result);
    let response = result.map_or_else(
        |error| json!({"operation_id": request.operation_id, "scenario": request.scenario, "identity": peer.identity, "success": false, "rejection_reason": format!("{error:#}")}),
        |trace| json!({"operation_id": request.operation_id, "scenario": request.scenario, "identity": peer.identity, "success": true, "route": trace.get("route"), "trace": trace}),
    );
    Json(response)
}

/// Start three persistent production swarms and the bounded private control server.
async fn run_peer(args: RunArgs) -> Result<()> {
    let config = Config::read(&args.config)?;
    if config.listen.len() != 3
        || config.required_hubs.len() != 2
        || config
            .required_hubs
            .first()
            .zip(config.required_hubs.get(1))
            .is_none_or(|(first, second)| first == second)
        || !config.required_hubs.contains(&config.target)
        || config.listen.iter().any(|address| address.to_string().contains("/udp/0/"))
    {
        Err(eyre!("one primary, two workers, and two required hub identities must be declared"))?;
    }
    let keys = KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(
        config.seed,
    )));
    let manager = TaskManager::default();
    let (gossip, gossip_rx) = watch::channel(None);
    let roles = [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)];
    let networks = futures::future::try_join_all(roles.into_iter().zip(config.listen.iter()).map(
        |(role, address)| {
            let keys = keys.clone();
            let network_config = &config.network;
            let required_hubs = &config.required_hubs;
            let manager = &manager;
            let gossip = gossip.clone();
            async move {
                let (events, received) = mpsc::channel(100);
                let network = match role {
                    NetworkType::Primary => Network::new_for_primary(
                        network_config,
                        events,
                        keys.clone(),
                        MemDatabase::default(),
                        manager.get_spawner(),
                        address.clone(),
                    )?,
                    NetworkType::Worker(id) => Network::new_for_worker(
                        id,
                        network_config,
                        events,
                        keys.clone(),
                        MemDatabase::default(),
                        manager.get_spawner(),
                        address.clone(),
                        None,
                    )?,
                };
                let handle = network.network_handle();
                let task = tokio::spawn(network.run());
                let receiver_id = PeerId::from(match role {
                    NetworkType::Primary => keys.primary_network_public_key(),
                    NetworkType::Worker(id) => keys.worker_network_public_key(id),
                });
                let drain = tokio::spawn(
                    futures::stream::unfold(received, |mut receiver| async {
                        receiver.recv().await.map(|event| (event, receiver))
                    })
                    .for_each(move |event| {
                        match event {
                            NetworkEvent::Gossip(payload) => {
                                payload.receipt.zip(payload.message.source).into_iter()
                                    .filter(|(receipt, source)| *source != receipt.propagation_source
                                        && *source != receiver_id && receipt.propagation_source != receiver_id)
                                    .for_each(|(receipt, source)| {
                                        let _ = gossip.send_replace(Some(json!({
                                            "receipt": receipt, "swarm": role_name(role),
                                            "route": [source.to_string(), receipt.propagation_source.to_string(), receiver_id.to_string()],
                                            "author": payload.author, "relayer": payload.relayer,
                                        })));
                                    });
                            }
                            NetworkEvent::Request { .. } | NetworkEvent::Error(..)
                            | NetworkEvent::InboundStream { .. } => {}
                        }
                        futures::future::ready(())
                    }),
                );
                handle.start_listening(address.clone()).await?;
                let chain = network_config.libp2p_config().chain_id;
                let topic = match role {
                    NetworkType::Primary => LibP2pConfig::primary_topic(chain),
                    NetworkType::Worker(id) => LibP2pConfig::worker_batch_topic(chain, id),
                };
                handle.subscribe(topic).await?;
                let gateways = network_config
                    .bootstrap_peers()
                    .iter()
                    .filter(|(key, _)| required_hubs.contains(key))
                    .filter_map(|(key, server)| {
                        match role {
                            NetworkType::Primary => Some(server.primary.clone()),
                            NetworkType::Worker(id) => server.worker(id).cloned(),
                        }
                        .map(|peer| (*key, peer))
                    })
                    .collect::<Vec<_>>();
                handle.add_bootstrap_peers(gateways.iter().cloned().collect()).await?;
                // Pin the declared gateways in the client swarm. The measured hubs still
                // classify each client using their unchanged public or DAO profile.
                futures::future::try_join_all(gateways.into_iter().map(|(key, gateway)| {
                    let handle = handle.clone();
                    async move {
                        startup_dial(
                            &handle,
                            role,
                            key,
                            handle.add_trusted_peer_and_dial(
                                key,
                                gateway.network_key,
                                gateway.network_address,
                            ),
                        )
                        .await
                    }
                }))
                .await?;
                Ok::<_, eyre::Report>((role, handle, task, drain))
            }
        },
    ))
    .await?;
    let (handles, tasks): (Vec<_>, Vec<_>) = networks
        .into_iter()
        .map(|(role, handle, task, drain)| ((role, handle), (task, drain)))
        .unzip();
    let peer = Arc::new(Peer {
        identity: hex::encode(tn_types::encode(&keys.primary_public_key())),
        config,
        handles,
        slots: Arc::new(Semaphore::new(16)),
        gossip: Mutex::new(gossip_rx),
    });
    let startup =
        futures::future::try_join_all(peer.handles.iter().map(|(role, handle)| async {
            futures::future::try_join_all(peer.config.required_hubs.iter().map(|key| async {
                startup_dial(handle, *role, *key, handle.dial_by_bls(*key)).await
            }))
            .await
        }))
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());
    let listener = tokio::net::TcpListener::bind(peer.config.control).await?;
    let public = json!({"identity": peer.identity, "bls_key": keys.primary_public_key(), "control": listener.local_addr()?, "startup_error": startup.err(), "swarms": roles.into_iter().zip(peer.config.listen.iter()).map(|(role, address)| {
        let key = match role { NetworkType::Primary => keys.primary_network_public_key(), NetworkType::Worker(id) => keys.worker_network_public_key(id) };
        json!({"swarm": role_name(role), "network_key": key, "peer_id": PeerId::from(key.clone()), "listen": address})
    }).collect::<Vec<_>>()});
    let mut ready = OpenOptions::new().create_new(true).write(true).open(args.ready)?;
    ready.write_all(&serde_json::to_vec(&public)?)?;
    let app = Router::new()
        .route("/", post(command))
        .layer(DefaultBodyLimit::max(16 * 1024))
        .with_state(peer);
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    tasks.into_iter().for_each(|(network, drain)| {
        network.abort();
        drain.abort();
    });
    served?;
    Ok(())
}

/// Export public qualification identities or run the peer with its frozen deployment file.
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    match Args::parse().mode {
        Mode::Identity { seed } => {
            let keys = KeyConfig::new_with_testing_key(BlsKeypair::generate(
                &mut StdRng::seed_from_u64(seed),
            ));
            println!(
                "{}",
                serde_json::to_string(&json!({
                    "identity": hex::encode(tn_types::encode(&keys.primary_public_key())),
                    "bls_key": keys.primary_public_key(),
                "swarms": ([NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)]
                        .into_iter().map(|role| {
                            let key = match role {
                                NetworkType::Primary => keys.primary_network_public_key(),
                                NetworkType::Worker(id) => keys.worker_network_public_key(id),
                            };
                        json!({"swarm": role_name(role), "network_key": key, "peer_id": PeerId::from(key.clone())})
                    }).collect::<Vec<_>>()),
                }))?
            );
            Ok(())
        }
        Mode::Run(args) => {
            let filter = tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("network::identity=debug".parse()?);
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .init();
            run_peer(args).await
        }
    }
}

#[cfg(test)]
mod tests {
    //! Deployment domain and authenticated dial confirmation regressions.

    use super::*;
    use tn_network_libp2p::types::NetworkCommand;

    async fn accept_disconnect(
        receiver: &mut mpsc::Receiver<NetworkCommand<Message, Message>>,
        peer: PeerId,
    ) -> Result<()> {
        let command = receiver.recv().await.ok_or_else(|| eyre!("commands closed"))?;
        if let NetworkCommand::DisconnectPeer { peer_id, reply } = command {
            assert_eq!(peer_id, peer);
            reply.send(Ok(())).map_err(|_| eyre!("disconnect acknowledgement canceled"))
        } else {
            Err(eyre!("expected disconnect before close observations"))
        }
    }

    async fn reply_disconnect_poll(
        receiver: &mut mpsc::Receiver<NetworkCommand<Message, Message>>,
        peer: PeerId,
        logical_peers: Vec<BlsPublicKey>,
        physically_connected: bool,
    ) -> Result<()> {
        let command = receiver.recv().await.ok_or_else(|| eyre!("commands closed"))?;
        if let NetworkCommand::ConnectedPeers { reply } = command {
            reply.send(logical_peers).map_err(|_| eyre!("logical observation canceled"))
        } else {
            Err(eyre!("expected logical observation before redial"))
        }?;
        let command = receiver.recv().await.ok_or_else(|| eyre!("commands closed"))?;
        if let NetworkCommand::IsPeerConnected { peer_id, reply } = command {
            assert_eq!(peer_id, peer);
            reply.send(physically_connected).map_err(|_| eyre!("physical observation canceled"))
        } else {
            Err(eyre!("expected physical close observation before redial"))
        }
    }

    /// Neither an absent logical identity nor physical absence alone permits a redial.
    #[tokio::test]
    async fn reconnect_waits_for_physical_close_before_redial() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(7376)));
        let target = keys.primary_public_key();
        let peer = PeerId::random();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let commands = async {
            accept_disconnect(&mut receiver, peer).await?;
            reply_disconnect_poll(&mut receiver, peer, Vec::new(), true).await?;
            reply_disconnect_poll(&mut receiver, peer, vec![target], false).await?;
            reply_disconnect_poll(&mut receiver, peer, Vec::new(), false).await?;
            let command = receiver.recv().await.ok_or_else(|| eyre!("commands closed"))?;
            if let NetworkCommand::DialBls { bls_key, reply } = command {
                assert_eq!(bls_key, target);
                reply.send(Ok(())).map_err(|_| eyre!("redial canceled"))
            } else {
                Err(eyre!("expected one redial after physical close"))
            }?;
            let command = receiver.recv().await.ok_or_else(|| eyre!("commands closed"))?;
            if let NetworkCommand::ConnectedPeers { reply } = command {
                reply.send(vec![target]).map_err(|_| eyre!("confirmation canceled"))
            } else {
                Err(eyre!("expected authenticated confirmation after redial"))
            }
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                disconnect_and_redial(&handle, NetworkType::Primary, target, peer),
                commands
            )
        })
        .await?;
        commands?;
        result?;
        assert!(receiver.try_recv().is_err(), "reconnect must not queue another dial");
        Ok(())
    }

    async fn hold_physical_close(
        receiver: &mut mpsc::Receiver<NetworkCommand<Message, Message>>,
        peer: PeerId,
    ) -> Result<()> {
        accept_disconnect(receiver, peer).await?;
        futures::stream::try_unfold(receiver, move |receiver| async move {
            reply_disconnect_poll(receiver, peer, Vec::new(), true).await?;
            Ok::<_, eyre::Report>(Some(((), receiver)))
        })
        .try_for_each(|()| futures::future::ready(Ok(())))
        .await
    }

    /// A stuck physical close exhausts the original single eight-second deadline without a dial.
    #[tokio::test]
    async fn reconnect_stuck_physical_close_times_out_without_redial() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(8376)));
        let target = keys.primary_public_key();
        let peer = PeerId::random();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(9), async {
            tokio::select! {
                biased;
                result = disconnect_and_redial(&handle, NetworkType::Primary, target, peer) => result,
                result = hold_physical_close(&mut receiver, peer) => {
                    result?;
                    Err(eyre!("pending close responder ended"))
                },
            }
        })
        .await?;
        let error = result.expect_err("a stuck physical close must time out");
        assert!(error.downcast_ref::<tokio::time::error::Elapsed>().is_some());
        assert!(started.elapsed() >= Duration::from_secs(8));
        assert!(
            std::iter::from_fn(|| receiver.try_recv().ok())
                .all(|command| !matches!(command, NetworkCommand::DialBls { .. })),
            "a timed out close must not queue a dial"
        );
        Ok(())
    }

    /// The four selected digests retain their individual source epochs and the primary epoch.
    #[test]
    fn mixed_epoch_worker_requests_preserve_selected_batches() -> Result<()> {
        let digests = [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]].map(B256::from);
        let batch_epochs =
            BTreeMap::from([(digests[0], 5), (digests[1], 5), (digests[2], 6), (digests[3], 7)]);
        let request: Command = serde_json::from_value(json!({
            "operation_id": "nonce", "scenario": "concurrent_sync", "not_before_unix_us": 0,
            "sync_epoch": 5, "batch_digests": digests, "batch_epochs": batch_epochs,
        }))?;
        let requests = worker_batch_requests(&request.batch_digests, &request.batch_epochs, 5)?;
        assert_eq!(request.sync_epoch, Some(5));
        assert_eq!(
            requests,
            BTreeMap::from([
                (5, BTreeSet::from([digests[0], digests[1]])),
                (6, BTreeSet::from([digests[2]])),
                (7, BTreeSet::from([digests[3]])),
            ])
        );
        assert_eq!(
            requests.into_values().flatten().collect::<BTreeSet<_>>(),
            request.batch_digests
        );
        Ok(())
    }

    /// No worker transfer may substitute absent, mismatched, or stale retained provenance.
    #[test]
    fn worker_requests_reject_invalid_epoch_maps() -> Result<()> {
        let digests = [[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]].map(B256::from);
        let selected = BTreeSet::from(digests);
        let valid =
            BTreeMap::from([(digests[0], 5), (digests[1], 5), (digests[2], 6), (digests[3], 6)]);
        let missing = valid.iter().skip(1).map(|(digest, epoch)| (*digest, *epoch)).collect();
        let extra = valid
            .iter()
            .map(|(digest, epoch)| (*digest, *epoch))
            .chain([(B256::ZERO, 5)])
            .collect();
        let stale =
            BTreeMap::from([(digests[0], 4), (digests[1], 5), (digests[2], 6), (digests[3], 6)]);
        let wrong_primary = digests.into_iter().map(|digest| (digest, 6)).collect();
        [BTreeMap::new(), missing, extra, stale, wrong_primary].into_iter().for_each(|epochs| {
            assert!(worker_batch_requests(&selected, &epochs, 5).is_err());
        });
        assert!(worker_batch_requests(
            &BTreeSet::from([digests[0], digests[1], digests[2]]),
            &valid,
            5
        )
        .is_err());
        let payload = json!({"operation_id": "nonce", "scenario": "concurrent_sync",
            "not_before_unix_us": 0, "sync_epoch": 5, "batch_digests": digests});
        let missing: Command = serde_json::from_value(payload.clone())?;
        assert!(worker_batch_requests(&missing.batch_digests, &missing.batch_epochs, 5).is_err());
        [
            json!([]),
            json!({"invalid-digest": 5}),
            json!({digests[0].to_string(): "5"}),
            json!({digests[0].to_string(): -1}),
        ]
        .into_iter()
        .for_each(|epochs| {
            let mut malformed = payload.clone();
            malformed["batch_epochs"] = epochs;
            assert!(serde_json::from_value::<Command>(malformed).is_err());
        });
        Ok(())
    }

    /// Completed worker witnesses aggregate real mixed-epoch batches and reject substitutions.
    #[test]
    fn mixed_epoch_worker_witness_requires_exact_digests_and_source_epochs() -> Result<()> {
        let batches = (0u8..4)
            .map(|index| Batch {
                transactions: vec![vec![index + 1; 32 * 1024]],
                epoch: 5 + Epoch::from(index / 2),
                ..Batch::default()
            })
            .collect::<Vec<_>>();
        let selected = batches.iter().map(Batch::digest).collect::<BTreeSet<_>>();
        let epochs =
            batches.iter().map(|batch| (batch.digest(), batch.epoch)).collect::<BTreeMap<_, _>>();
        let frames = batches.iter().map(tn_types::encode).collect::<Vec<_>>();
        let requests = worker_batch_requests(&selected, &epochs, 5)?;
        requests.into_iter().try_for_each(|(epoch, requested)| {
            let data = batches
                .iter()
                .filter(|batch| batch.epoch == epoch)
                .map(tn_types::encode)
                .collect::<Vec<_>>();
            verify_worker_batches(&data, &requested, &epochs)
                .map(|received| assert_eq!(received, requested))
        })?;
        assert_eq!(verify_worker_batches(&frames, &selected, &epochs)?, selected);
        assert!(frames.iter().map(Vec::len).sum::<usize>() >= 128 * 1024);
        assert!(verify_worker_batches(&frames[..3], &selected, &epochs).is_err());
        assert!(verify_worker_batches(&vec![frames[0].clone(); 4], &selected, &epochs).is_err());
        let duplicate = frames.iter().cloned().chain([frames[0].clone()]).collect::<Vec<_>>();
        assert!(verify_worker_batches(&duplicate, &selected, &epochs).is_err());
        assert!(verify_worker_batches(&[vec![0xff]], &selected, &epochs).is_err());
        let stale = epochs.into_iter().map(|(digest, epoch)| (digest, epoch + 1)).collect();
        assert!(verify_worker_batches(&frames, &selected, &stale).is_err());
        Ok(())
    }

    /// Native payloads are clipped at Unicode scalar boundaries without altering the error.
    #[test]
    fn dial_cause_keeps_unicode_boundaries_and_original_payload() {
        [0, 255, 256, 257].into_iter().for_each(|length| {
            let original = "🙂".repeat(length);
            let error = NetworkError::Dial(original.clone());
            let cause = bounded_dial_cause(&error);
            assert_eq!(cause, "🙂".repeat(length.min(MAX_DIAL_ERROR_CHARS)));
            assert!(matches!(error, NetworkError::Dial(payload) if payload == original));
        });
        let error = NetworkError::ProtocolError("protocol detail".to_owned());
        assert_eq!(bounded_dial_cause(&error), error.to_string());
    }

    /// Default ERROR logging retains native detail when exact target confirmation fails.
    #[tokio::test]
    async fn failed_confirmation_retains_native_dial_detail_and_typed_error() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(6376)));
        let target = keys.primary_public_key();
        let trace = tempfile::NamedTempFile::new()?;
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                    .parse("")?,
            )
            .without_time()
            .with_ansi(false)
            .with_writer(trace.as_file().try_clone()?)
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let handle = Handle::new(sender);
        let detail = "界".repeat(300);
        let error = dial_and_confirm(
            &handle,
            NetworkType::Worker(0),
            target,
            futures::future::ready(Err(NetworkError::Dial(detail.clone()))),
        )
        .await
        .expect_err("a native rejection without authenticated confirmation remains a failure");
        assert!(
            matches!(error.downcast_ref::<NetworkError>(), Some(NetworkError::Dial(original)) if original == &detail)
        );
        let retained = std::fs::read_to_string(trace.path())?;
        assert!(!retained.contains("peer_dial_rejection"));
        assert!(retained.contains("peer_dial_confirmation"));
        assert!(retained.contains("confirmation=\"failed\""));
        assert!(retained.contains("role=worker-0"));
        assert!(retained.contains(&format!("peer={target}")));
        assert!(retained.contains("native_cause="));
        assert_eq!(retained.matches('界').count(), MAX_DIAL_ERROR_CHARS);
        assert!(!retained.contains(&detail));
        Ok(())
    }

    /// The production default ERROR filter retains bounded fatal detail and the original error.
    #[tokio::test]
    async fn startup_dial_trace_bounds_native_detail_and_keeps_error() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(5376)));
        let target = keys.primary_public_key();
        let trace = tempfile::NamedTempFile::new()?;
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing_subscriber::filter::LevelFilter::ERROR.into())
                    .parse("")?,
            )
            .without_time()
            .with_ansi(false)
            .with_writer(trace.as_file().try_clone()?)
            .finish();
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let detail = "界".repeat(300);
        let retry_detail = "路".repeat(300);
        let commands = async {
            let reply = retry_command(&mut receiver, target).await?;
            reply
                .send(Err(NetworkError::ProtocolError(retry_detail.clone())))
                .map_err(|_| eyre!("the original startup recovery was canceled"))
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                startup_dial(
                    &handle,
                    NetworkType::Worker(1),
                    target,
                    futures::future::ready(Err(NetworkError::Dial(detail.clone())))
                ),
                commands
            )
        })
        .await?;
        commands?;
        let error = result.expect_err("failed recovery must preserve the initial Dial error");
        assert!(
            matches!(error.downcast_ref::<NetworkError>(), Some(NetworkError::Dial(original)) if original == &detail)
        );
        let retained = std::fs::read_to_string(trace.path())?;
        assert!(!retained.contains("peer_dial_rejection"));
        assert!(retained.contains("peer_startup_failure"));
        assert!(retained.contains("peer_dial_confirmation"));
        assert!(retained.contains("phase=\"startup\""));
        assert!(retained.contains("role=worker-1"));
        assert!(retained.contains("outcome=\"rejected\""));
        assert!(retained.contains("retry_count=1"));
        assert!(retained.contains("confirmation=\"failed\""));
        assert!(retained.contains(&format!("peer={target}")));
        assert_eq!(retained.matches('界').count(), MAX_DIAL_ERROR_CHARS);
        assert_eq!(
            retained.matches('路').count(),
            MAX_DIAL_ERROR_CHARS.saturating_sub(
                NetworkError::ProtocolError(String::new()).to_string().chars().count()
            )
        );
        assert!(!retained.contains(&detail));
        assert!(!retained.contains(&retry_detail));
        Ok(())
    }

    /// Model a sibling that has disconnected and must remain alive until redial completes.
    async fn held_reconnect(
        started: tokio::sync::oneshot::Sender<()>,
        release: tokio::sync::oneshot::Receiver<()>,
        completed: tokio::sync::oneshot::Sender<()>,
    ) -> Result<()> {
        started.send(()).map_err(|_| eyre!("started observer dropped"))?;
        release.await?;
        completed.send(()).map_err(|_| eyre!("completion observer dropped"))
    }

    /// A native error cannot cancel another already-started reconnect transition.
    #[tokio::test]
    async fn reconnect_failure_waits_for_started_sibling() -> Result<()> {
        let (started, mut started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let (completed, mut completed_rx) = tokio::sync::oneshot::channel();
        let transitions = [
            futures::future::Either::Left(held_reconnect(started, release_rx, completed)),
            futures::future::Either::Right(futures::future::ready(Err(eyre::Report::from(
                NetworkError::Dial("original reconnect failure".to_owned()),
            )))),
        ];
        let mut batch = Box::pin(settle_reconnects(transitions));
        assert!(futures::poll!(batch.as_mut()).is_pending());
        started_rx.try_recv()?;
        assert!(!release.is_closed(), "the started sibling must remain owned");
        assert!(matches!(
            completed_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        release.send(()).map_err(|_| eyre!("the started reconnect was canceled"))?;
        let error = batch.await.expect_err("the original native failure must propagate");
        completed_rx.try_recv()?;
        assert!(matches!(
            error.downcast_ref::<NetworkError>(),
            Some(NetworkError::Dial(detail)) if detail == "original reconnect failure"
        ));
        Ok(())
    }

    /// The former aggregation cancels a sibling after it has started its transition.
    #[tokio::test]
    async fn old_reconnect_join_cancels_started_sibling() -> Result<()> {
        let (started, mut started_rx) = tokio::sync::oneshot::channel();
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let (completed, mut completed_rx) = tokio::sync::oneshot::channel();
        let transitions = [
            futures::future::Either::Left(held_reconnect(started, release_rx, completed)),
            futures::future::Either::Right(futures::future::ready(Err(eyre::Report::from(
                NetworkError::Dial("original reconnect failure".to_owned()),
            )))),
        ];
        let mut old_batch = Box::pin(futures::future::try_join_all(transitions));
        let error = match futures::poll!(old_batch.as_mut()) {
            std::task::Poll::Ready(Err(error)) => error,
            std::task::Poll::Ready(Ok(_)) => panic!("the failing transition must fail"),
            std::task::Poll::Pending => panic!("the old join must return its immediate error"),
        };
        started_rx.try_recv()?;
        assert!(release.is_closed(), "the old join canceled the started sibling");
        assert!(matches!(
            completed_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed)
        ));
        assert!(matches!(
            error.downcast_ref::<NetworkError>(),
            Some(NetworkError::Dial(detail)) if detail == "original reconnect failure"
        ));
        Ok(())
    }

    /// All six successful hub/swarm transitions finish before the aggregate succeeds.
    #[tokio::test]
    async fn reconnect_success_finishes_all_six_transitions() -> Result<()> {
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        settle_reconnects((0..6).map(|_| {
            let completed = Arc::clone(&completed);
            async move {
                completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }))
        .await?;
        assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 6);
        Ok(())
    }

    /// Supply one authenticated peer observation through the real handle command channel.
    async fn observe_peer(
        receiver: &mut mpsc::Receiver<NetworkCommand<Message, Message>>,
        peer: BlsPublicKey,
    ) -> Result<()> {
        let command = receiver.recv().await.ok_or_else(|| eyre!("peer observation missing"))?;
        if let NetworkCommand::ConnectedPeers { reply } = command {
            reply.send(vec![peer]).map_err(|_| eyre!("peer observation canceled"))
        } else {
            Err(eyre!("expected authenticated peer observation"))
        }
    }

    /// Receive a native BLS dial command and verify its declared target.
    async fn retry_command(
        receiver: &mut mpsc::Receiver<NetworkCommand<Message, Message>>,
        target: BlsPublicKey,
    ) -> Result<tokio::sync::oneshot::Sender<std::result::Result<(), NetworkError>>> {
        let command = receiver.recv().await.ok_or_else(|| eyre!("same-target retry missing"))?;
        if let NetworkCommand::DialBls { bls_key, reply } = command {
            assert_eq!(bls_key, target);
            Ok(reply)
        } else {
            Err(eyre!("expected one same-target BLS retry"))
        }
    }

    /// Successful native startup commands do not await connection observation.
    #[tokio::test]
    async fn queued_startup_dials_do_not_wait_for_peer_observation() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let target = keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let startup = async {
            startup_dial(
                &handle,
                NetworkType::Primary,
                target,
                handle.add_trusted_peer_and_dial(
                    target,
                    keys.primary_network_public_key(),
                    "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?,
                ),
            )
            .await?;
            startup_dial(&handle, NetworkType::Primary, target, handle.dial_by_bls(target)).await
        };
        let commands = async {
            let command =
                receiver.recv().await.ok_or_else(|| eyre!("trusted startup dial missing"))?;
            let reply =
                if let NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, reply, .. } = command {
                    assert_eq!(bls_pubkey, target);
                    Ok(reply)
                } else {
                    Err(eyre!("expected trusted startup dial without peer observation"))
                }?;
            reply.send(Ok(())).map_err(|_| eyre!("trusted startup dial canceled"))?;
            let command = receiver.recv().await.ok_or_else(|| eyre!("BLS startup dial missing"))?;
            let reply = if let NetworkCommand::DialBls { bls_key, reply } = command {
                assert_eq!(bls_key, target);
                Ok(reply)
            } else {
                Err(eyre!("expected BLS startup dial without peer observation"))
            }?;
            reply.send(Ok(())).map_err(|_| eyre!("BLS startup dial canceled"))
        };
        let (result, commands) =
            tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(startup, commands) })
                .await?;
        commands?;
        result?;
        assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        Ok(())
    }

    /// Successful reconnect dials still wait for the exact authenticated target.
    #[tokio::test]
    async fn successful_reconnect_waits_for_authenticated_target() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let wrong_keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4477)));
        let target = keys.primary_public_key();
        let wrong = wrong_keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let commands = async {
            let command = receiver.recv().await.ok_or_else(|| eyre!("reconnect dial missing"))?;
            let reply = if let NetworkCommand::DialBls { bls_key, reply } = command {
                assert_eq!(bls_key, target);
                Ok(reply)
            } else {
                Err(eyre!("expected reconnect BLS dial"))
            }?;
            reply.send(Ok(())).map_err(|_| eyre!("reconnect dial canceled"))?;
            observe_peer(&mut receiver, wrong).await?;
            observe_peer(&mut receiver, target).await
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                dial_and_confirm(&handle, NetworkType::Primary, target, handle.dial_by_bls(target)),
                commands
            )
        })
        .await?;
        commands?;
        result
    }

    /// A rejected trusted dial becomes ready only after the target identity is established.
    #[tokio::test]
    async fn rejected_trusted_dial_waits_for_authenticated_target() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let wrong_keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4477)));
        let target = keys.primary_public_key();
        let wrong = wrong_keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let dial = handle.add_trusted_peer_and_dial(
            target,
            keys.primary_network_public_key(),
            "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?,
        );
        let commands = async {
            let command =
                receiver.recv().await.ok_or_else(|| eyre!("trusted dial command missing"))?;
            let reply =
                if let NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, reply, .. } = command {
                    assert_eq!(bls_pubkey, target);
                    Ok(reply)
                } else {
                    Err(eyre!("expected trusted dial command"))
                }?;
            reply
                .send(Err(NetworkError::Dial("opaque pending dial rejection".to_owned())))
                .map_err(|_| eyre!("trusted dial reply canceled"))?;
            retry_command(&mut receiver, target)
                .await?
                .send(Ok(()))
                .map_err(|_| eyre!("same-target retry canceled"))?;
            observe_peer(&mut receiver, wrong).await?;
            observe_peer(&mut receiver, target).await
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(startup_dial(&handle, NetworkType::Primary, target, dial), commands)
        })
        .await?;
        commands?;
        result
    }

    /// Failed authenticated observation preserves the original opaque dial error.
    #[tokio::test]
    async fn rejected_bls_dial_keeps_error_when_observation_fails() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let target = keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let original = "original opaque dial rejection";
        let commands = async {
            let command = receiver.recv().await.ok_or_else(|| eyre!("BLS dial command missing"))?;
            let reply = if let NetworkCommand::DialBls { bls_key, reply } = command {
                assert_eq!(bls_key, target);
                Ok(reply)
            } else {
                Err(eyre!("expected BLS dial command"))
            }?;
            reply
                .send(Err(NetworkError::Dial(original.to_owned())))
                .map_err(|_| eyre!("BLS dial reply canceled"))?;
            retry_command(&mut receiver, target)
                .await?
                .send(Err(NetworkError::Dial("retry also failed".to_owned())))
                .map_err(|_| eyre!("failed retry reply canceled"))?;
            let command = receiver.recv().await.ok_or_else(|| eyre!("peer observation missing"))?;
            if let NetworkCommand::ConnectedPeers { reply } = command {
                drop(reply);
                Ok::<_, eyre::Report>(())
            } else {
                Err(eyre!("expected authenticated peer observation"))
            }
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(
                startup_dial(&handle, NetworkType::Primary, target, handle.dial_by_bls(target)),
                commands
            )
        })
        .await?;
        commands?;
        let error = result.expect_err("missing observation must fail readiness");
        assert!(
            matches!(error.downcast_ref::<NetworkError>(), Some(NetworkError::Dial(message)) if message == original)
        );
        Ok(())
    }

    /// A held retry expires within the shared recovery window and preserves the first error.
    #[tokio::test]
    async fn held_startup_retry_keeps_original_error_at_recovery_deadline() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let target = keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let original = "original opaque startup failure";
        let commands = async {
            retry_command(&mut receiver, target)
                .await?
                .send(Err(NetworkError::Dial(original.to_owned())))
                .map_err(|_| eyre!("initial startup reply canceled"))?;
            let mut held = retry_command(&mut receiver, target).await?;
            held.closed().await;
            assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
            Ok::<_, eyre::Report>(())
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(9), async {
            tokio::join!(
                startup_dial(&handle, NetworkType::Primary, target, handle.dial_by_bls(target)),
                commands
            )
        })
        .await?;
        commands?;
        let error = result.expect_err("held retry must exhaust the existing recovery window");
        assert!(
            matches!(error.downcast_ref::<NetworkError>(), Some(NetworkError::Dial(message)) if message == original)
        );
        Ok(())
    }

    /// Already-dialing and already-connected races require the exact target without another dial.
    #[tokio::test]
    async fn raced_startup_rejection_observes_target_without_transport_retry() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let wrong_keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4477)));
        let target = keys.primary_public_key();
        let wrong = wrong_keys.primary_public_key();
        futures::future::try_join_all(
            [
                NetworkError::AlreadyDialing("opaque dialing race".to_owned()),
                NetworkError::AlreadyConnected("opaque connected race".to_owned()),
            ]
            .into_iter()
            .map(|native| async move {
                let (sender, mut receiver) = mpsc::channel(4);
                let handle = Handle::new(sender);
                let commands = async {
                    retry_command(&mut receiver, target)
                        .await?
                        .send(Err(native))
                        .map_err(|_| eyre!("raced startup reply canceled"))?;
                    observe_peer(&mut receiver, wrong).await?;
                    observe_peer(&mut receiver, target).await
                };
                let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
                    tokio::join!(
                        startup_dial(
                            &handle,
                            NetworkType::Primary,
                            target,
                            handle.dial_by_bls(target)
                        ),
                        commands
                    )
                })
                .await?;
                commands?;
                result?;
                assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
                Ok::<_, eyre::Report>(())
            }),
        )
        .await?;
        Ok(())
    }

    /// An unauthenticated target preserves its original already-connected cause without retrying.
    #[tokio::test]
    async fn already_connected_startup_keeps_error_when_target_is_absent() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(5776)));
        let wrong_keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(5777)));
        let target = keys.primary_public_key();
        let wrong = wrong_keys.primary_public_key();
        let (sender, mut receiver) = mpsc::channel(4);
        let handle = Handle::new(sender);
        let original = "native disconnected-condition rejection for the declared transport";
        let commands = async {
            let command =
                receiver.recv().await.ok_or_else(|| eyre!("trusted dial command missing"))?;
            let reply =
                if let NetworkCommand::AddTrustedPeerAndDial { bls_pubkey, reply, .. } = command {
                    assert_eq!(bls_pubkey, target);
                    Ok(reply)
                } else {
                    Err(eyre!("expected the declared trusted target"))
                }?;
            reply
                .send(Err(NetworkError::AlreadyConnected(original.to_owned())))
                .map_err(|_| eyre!("trusted dial reply canceled"))?;
            observe_peer(&mut receiver, wrong).await?;
            let command = receiver.recv().await.ok_or_else(|| eyre!("peer observation missing"))?;
            if let NetworkCommand::ConnectedPeers { reply } = command {
                drop(reply);
                Ok::<_, eyre::Report>(())
            } else {
                Err(eyre!("expected exact authenticated confirmation without retry"))
            }
        };
        let dial = handle.add_trusted_peer_and_dial(
            target,
            keys.primary_network_public_key(),
            "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?,
        );
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(startup_dial(&handle, NetworkType::Worker(1), target, dial), commands)
        })
        .await?;
        commands?;
        let error =
            result.err().ok_or_else(|| eyre!("an absent authenticated target cannot be ready"))?;
        assert!(matches!(error.downcast_ref::<NetworkError>(),
            Some(NetworkError::AlreadyConnected(cause)) if cause == original));
        assert!(matches!(receiver.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
        Ok(())
    }

    #[test]
    fn deployment_chain_overrides_non_persisted_network_domain() -> Result<()> {
        let keys =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(4476)));
        let mut fixture = tempfile::NamedTempFile::new()?;
        serde_json::to_writer(
            fixture.as_file_mut(),
            &json!({
                "seed": 4476, "chain_id": 4476,
                "network": {"libp2p_config": {"chain_id": 999}},
                "listen": [], "control": "127.0.0.1:9500",
                "required_hubs": [], "target": keys.primary_public_key(), "sync_epoch": 0,
            }),
        )?;
        let config = Config::read(fixture.path())?;
        assert_eq!(config.network.libp2p_config().chain_id, 4476);
        assert_eq!(
            LibP2pConfig::primary_topic(config.network.libp2p_config().chain_id),
            "tn-primary-4476"
        );
        Ok(())
    }
}
