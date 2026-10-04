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
use eyre::{eyre, Result};
use futures::{StreamExt, TryStreamExt};
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
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
use tracing_subscriber as _;
use url as _;

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

/// Confirm the authenticated target after a successful or potentially raced dial.
async fn dial_and_confirm<F>(handle: &Handle, target: BlsPublicKey, dial: F) -> Result<()>
where
    F: Future<Output = std::result::Result<(), NetworkError>>,
{
    let outcome = dial.await;
    if outcome.as_ref().is_err_and(|error| {
        !matches!(
            error,
            NetworkError::AlreadyDialing(_)
                | NetworkError::AlreadyConnected(_)
                | NetworkError::Dial(_)
        )
    }) {
        outcome.map_err(eyre::Report::from)
    } else {
        connected(handle, target, true).await.map_err(|confirmation_error| {
            outcome.err().map(eyre::Report::from).unwrap_or(confirmation_error)
        })
    }
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
                let record = handle.get_node_record(target).await?;
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
            .flat_map(|(_, handle)| {
                self.config.required_hubs.iter().map(move |key| (handle.clone(), *key))
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(targets.into_iter().map(|(handle, key)| async move {
            let peers = handle.connected_peers().await?;
            if peers.contains(&key) {
                let record = handle.get_node_record(key).await?;
                let peer: PeerId = record.info.pubkey.into();
                handle.disconnect_peer(peer).await?;
                connected(&handle, key, false).await?;
            } else if require_existing {
                Err(eyre!("shared-NAT restart requires an existing hub connection"))?;
            }
            dial_and_confirm(&handle, key, handle.dial_by_bls(key)).await
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
    async fn sync(&self, batch_digests: &BTreeSet<B256>, epoch: Epoch) -> Result<Value> {
        if batch_digests.len() != 4 {
            Err(eyre!("bulk qualification requires four executed batch digests"))?;
        }
        let target = self.config.target;
        let limit = self.config.network.libp2p_config().max_rpc_message_size;
        let transfers = futures::future::try_join_all(self.handles.clone().into_iter().map(
            |(role, handle)| {
                let batch_digests = batch_digests.clone();
                async move {
                    match role {
                        NetworkType::Primary => {
                            let frames = transfer(handle, target, PrimarySyncRequest::EpochPack { epoch }, limit).await?;
                            Ok::<_, eyre::Report>(json!({"swarm": role_name(role), "bytes": frames.iter().map(Vec::len).sum::<usize>(), "completed": true}))
                        }
                        NetworkType::Worker(_) => {
                            let frames = transfer(handle, target, WorkerSyncRequest::Batches { batch_digests: batch_digests.clone(), epoch }, limit).await?;
                            let received = frames.iter().map(|data| try_decode::<Batch>(data).map(|batch| batch.digest())).collect::<Result<BTreeSet<_>, _>>()?;
                            if received != batch_digests || frames.len() != batch_digests.len() {
                                Err(eyre!("worker transfer did not return the requested batches"))?;
                            }
                            Ok(json!({"swarm": role_name(role), "bytes": frames.iter().map(Vec::len).sum::<usize>(), "batch_digests": received, "completed": true}))
                        }
                    }
                }
            },
        )).await?;
        Ok(json!({"epoch": epoch, "target": target, "transfers": transfers, "completed": true}))
    }
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
    let mut stream = handle.open_stream(target).await??;
    let request = SyncFrame::Req(request);
    write_frame(&mut stream, &request, &mut Vec::new(), &mut Vec::new(), limit).await?;
    let frames = futures::stream::try_unfold(
        (stream, Vec::new(), Vec::new(), false, false),
        move |(mut stream, mut plain, mut compressed, admitted, ended)| async move {
            if ended {
                Ok(None)
            } else {
                let frame: SyncFrame<Request> =
                    read_frame(&mut stream, &mut plain, &mut compressed, limit).await?;
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
                        dial_and_confirm(
                            &handle,
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
        futures::future::try_join_all(peer.handles.iter().map(|(_, handle)| async {
            futures::future::try_join_all(peer.config.required_hubs.iter().map(|key| async {
                dial_and_confirm(handle, *key, handle.dial_by_bls(*key)).await
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
        Mode::Run(args) => run_peer(args).await,
    }
}

#[cfg(test)]
mod tests {
    //! Deployment domain and authenticated dial confirmation regressions.

    use super::*;
    use tn_network_libp2p::types::NetworkCommand;

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
            observe_peer(&mut receiver, wrong).await?;
            observe_peer(&mut receiver, target).await
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(dial_and_confirm(&handle, target, dial), commands)
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
            let command = receiver.recv().await.ok_or_else(|| eyre!("peer observation missing"))?;
            if let NetworkCommand::ConnectedPeers { reply } = command {
                drop(reply);
                Ok::<_, eyre::Report>(())
            } else {
                Err(eyre!("expected authenticated peer observation"))
            }
        };
        let (result, commands) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(dial_and_confirm(&handle, target, handle.dial_by_bls(target)), commands)
        })
        .await?;
        commands?;
        let error = result.expect_err("missing observation must fail readiness");
        assert!(
            matches!(error.downcast_ref::<NetworkError>(), Some(NetworkError::Dial(message)) if message == original)
        );
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
