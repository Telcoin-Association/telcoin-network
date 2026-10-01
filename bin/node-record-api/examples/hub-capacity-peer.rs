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
    fs::{File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tn_config::{KeyConfig, LibP2pConfig, NetworkConfig};
use tn_kad_client::{BlsPublicKey, Multiaddr, NetworkType, PeerId};
use tn_network_libp2p::{
    read_frame,
    types::{NetworkEvent, NetworkHandle},
    write_frame, ConsensusNetwork, PeerExchangeMap, PrimarySyncRequest, SyncFrame, TNMessage,
};
use tn_storage::mem_db::MemDatabase;
use tn_types::{BlsKeypair, Epoch, TaskManager};
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
            handle.dial_by_bls(key).await?;
            connected(&handle, key, true).await
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

    /// Transfer a nonempty completed epoch, accepting only the production ACK/DATA/END sequence.
    async fn sync(&self) -> Result<Value> {
        let handle = self
            .handles
            .iter()
            .find(|(role, _)| *role == NetworkType::Primary)
            .map(|(_, handle)| handle)
            .ok_or_else(|| eyre!("primary swarm missing"))?;
        let mut stream = handle.open_stream(self.config.target).await??;
        let limit = self.config.network.libp2p_config().max_rpc_message_size;
        let request =
            SyncFrame::Req(PrimarySyncRequest::EpochPack { epoch: self.config.sync_epoch });
        write_frame(&mut stream, &request, &mut Vec::new(), &mut Vec::new(), limit).await?;
        let frames = futures::stream::try_unfold(
            (stream, Vec::new(), Vec::new(), false, false),
            move |(mut stream, mut plain, mut compressed, admitted, ended)| async move {
                if ended {
                    Ok(None)
                } else {
                    let frame: SyncFrame<PrimarySyncRequest> =
                        read_frame(&mut stream, &mut plain, &mut compressed, limit).await?;
                    match frame {
                        SyncFrame::Ack if !admitted => {
                            Ok(Some((0, (stream, plain, compressed, true, false))))
                        }
                        SyncFrame::Data(data) if admitted => {
                            Ok(Some((data.len(), (stream, plain, compressed, true, false))))
                        }
                        SyncFrame::End if admitted => {
                            Ok(Some((0, (stream, plain, compressed, true, true))))
                        }
                        SyncFrame::Deny(reason) => Err(eyre!("sync denied: {reason:?}")),
                        SyncFrame::Err(reason) => Err(eyre!("sync aborted: {reason:?}")),
                        SyncFrame::Req(_)
                        | SyncFrame::Ack
                        | SyncFrame::Data(_)
                        | SyncFrame::End => Err(eyre!("invalid sync frame order")),
                    }
                }
            },
        );
        let total = frames
            .try_fold(0usize, |total, count| async move {
                total
                    .checked_add(count)
                    .filter(|bytes| *bytes <= 64 * 1024 * 1024)
                    .ok_or_else(|| eyre!("sync exceeded the declared 64 MiB transfer bound"))
            })
            .await?;
        if total == 0 {
            Err(eyre!("empty epoch transfer"))
        } else {
            Ok(
                json!({"epoch": self.config.sync_epoch, "bytes": total, "target": self.config.target, "completed": true}),
            )
        }
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
                Scenario::ConcurrentSync => peer.sync().await,
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
        |error| json!({"operation_id": request.operation_id, "scenario": request.scenario, "identity": peer.identity, "success": false, "rejection_reason": error.to_string()}),
        |trace| json!({"operation_id": request.operation_id, "scenario": request.scenario, "identity": peer.identity, "success": true, "route": trace.get("route"), "trace": trace}),
    );
    Json(response)
}

/// Start three persistent production swarms and the bounded private control server.
async fn run_peer(args: RunArgs) -> Result<()> {
    if std::fs::metadata(&args.config)?.len() > 128 * 1024 {
        Err(eyre!("peer configuration exceeds 128 KiB"))?;
    }
    let config: Config = serde_json::from_reader(File::open(&args.config)?)?;
    if config.listen.len() != 3
        || config.required_hubs.len() != 2
        || !config
            .required_hubs
            .first()
            .zip(config.required_hubs.get(1))
            .is_some_and(|(first, second)| first != second)
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
    let startup = futures::future::try_join_all(peer.handles.iter().map(|(_, handle)| async {
        futures::future::try_join_all(peer.config.required_hubs.iter().map(|key| async {
            handle.dial_by_bls(*key).await?;
            connected(handle, *key, true).await
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
#[tokio::main]
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
