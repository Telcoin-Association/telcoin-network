//! Swarm regressions for inbound request occupancy and the shared application queue.
//!
//! The swarm counts each forwarded request as pending until it ends. A server swarm with a small
//! application queue receives catch-up requests that the application does not answer. The
//! unanswered requests fill the queue and the swarm sheds the next vote request. The swarm does
//! not reserve queue space by class; the metrics only make this pressure visible.

use super::*;
use crate::common::{TestPrimaryRequest, TestPrimaryResponse, TEST_HEARTBEAT_INTERVAL};
use eyre::eyre;
use tn_config::{ConsensusConfig, NetworkConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::{Header, TaskManager};
use tokio::{sync::mpsc, time::timeout};

/// The generous bound for each network step. The tests wait for events and never sleep.
const STEP: Duration = Duration::from_secs(10);

/// The application queue capacity of the server swarm.
const SERVER_QUEUE: usize = 2;

/// The application events of a test swarm.
type TestEvents = mpsc::Receiver<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>;

/// The handle of a test swarm.
type TestHandle = NetworkHandle<TestPrimaryRequest, TestPrimaryResponse>;

/// The pending reply to one request that the client sent.
type PendingReply = oneshot::Receiver<NetworkResult<NetworkResponseMessage<TestPrimaryResponse>>>;

/// A running client swarm that is connected to a running server swarm.
struct Pair {
    /// The client handle that sends requests.
    client: TestHandle,
    /// The server handle. Kept so that the server command channel stays open and the swarm runs.
    _server: TestHandle,
    /// The server application queue. The test is the server application.
    events: TestEvents,
    /// The server BLS key, the destination of every request.
    server_key: BlsPublicKey,
    /// The client application queue. Kept open so that the client can forward its events.
    _client_events: TestEvents,
    /// The owned task manager, to prevent drop.
    _task_manager: TaskManager,
}

/// The network config for both swarms, with a short peer heartbeat.
fn test_config() -> NetworkConfig {
    let mut config = NetworkConfig::default();
    config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    config
}

/// Create and run one primary swarm for `config`, and return its handle.
fn start(
    config: &ConsensusConfig<MemDatabase>,
    events: mpsc::Sender<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>,
    task_manager: &TaskManager,
) -> eyre::Result<TestHandle> {
    let network = ConsensusNetwork::<TestPrimaryRequest, TestPrimaryResponse, MemDatabase, _>::new(
        config.network_config(),
        events,
        config.key_config().clone(),
        config.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        config.primary_address(),
        None,
    )?;
    let handle = network.network_handle();
    tokio::spawn(async move { network.run().await });
    Ok(handle)
}

/// Start a client and a server swarm with `config` and connect them.
///
/// The function returns when the server knows the BLS key of the client, because the server
/// forwards requests only from known peers.
async fn connected_pair(config: NetworkConfig) -> eyre::Result<Pair> {
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(config).build();
    let mut authorities = fixture.authorities();
    let client_config =
        authorities.next().ok_or_else(|| eyre!("no client authority"))?.consensus_config();
    let server_config =
        authorities.next().ok_or_else(|| eyre!("no server authority"))?.consensus_config();
    let task_manager = TaskManager::default();
    let (client_tx, client_events) = mpsc::channel(10);
    let (server_tx, events) = mpsc::channel(SERVER_QUEUE);
    let client = start(&client_config, client_tx, &task_manager)?;
    let server = start(&server_config, server_tx, &task_manager)?;

    client.start_listening(client_config.primary_address()).await?;
    server.start_listening(server_config.primary_address()).await?;
    let server_key = server_config.key_config().primary_public_key();
    client
        .add_explicit_peer(
            server_key,
            server_config.primary_networkkey(),
            server_config.primary_address(),
        )
        .await?;
    client.dial_by_bls(server_key).await?;

    let client_key = client_config.key_config().primary_public_key();
    let known = &server;
    wait_until(STEP, "server learns the client BLS key", move || async move {
        Ok(known.connected_peers().await?.contains(&client_key))
    })
    .await?;

    Ok(Pair {
        client,
        _server: server,
        events,
        server_key,
        _client_events: client_events,
        _task_manager: task_manager,
    })
}

/// A certificate catch-up request.
fn catch_up() -> TestPrimaryRequest {
    TestPrimaryRequest::MissingCertificates(Vec::new())
}

/// A vote request.
fn vote() -> TestPrimaryRequest {
    TestPrimaryRequest::Vote { header: Header::default(), parents: Vec::new() }
}

/// Send `request` from the client to the server.
async fn send(pair: &Pair, request: TestPrimaryRequest) -> eyre::Result<PendingReply> {
    pair.client.send_request(request, pair.server_key).await.map_err(Into::into)
}

/// Wait until the server application queue holds exactly `count` events.
async fn queued(events: &TestEvents, count: usize) -> eyre::Result<()> {
    wait_until(
        STEP,
        "server application queue length",
        move || async move { Ok(events.len() == count) },
    )
    .await
}

/// Return true if the client receives a failure for `reply`, which means that the server shed
/// the request without a response.
async fn failed(reply: PendingReply) -> eyre::Result<bool> {
    timeout(STEP, reply)
        .await
        .map_err(eyre::Report::from)
        .and_then(|received| received.map(|reply| reply.is_err()).map_err(Into::into))
}

/// Every close releases the occupancy that its add counted and notifies the request handler.
#[tokio::test]
async fn close_releases_the_pending_occupancy() -> eyre::Result<()> {
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).with_network_config(test_config()).build();
    let config =
        fixture.authorities().next().ok_or_else(|| eyre!("no authority"))?.consensus_config();
    let task_manager = TaskManager::default();
    let (events, _events) = mpsc::channel(SERVER_QUEUE);
    let mut network =
        ConsensusNetwork::<TestPrimaryRequest, TestPrimaryResponse, MemDatabase, _>::new(
            config.network_config(),
            events,
            config.key_config().clone(),
            config.key_config().primary_network_keypair().clone(),
            MemDatabase::default(),
            task_manager.get_spawner(),
            NetworkType::Primary,
            config.primary_address(),
            None,
        )?;
    let class = ServiceClass::CertificateSync;
    network.add_inbound(class);
    network.add_inbound(class);
    let (notify, mut cancel) = oneshot::channel();
    network.close_inbound(PendingInbound { notify, class, received: Instant::now() });
    assert_eq!(network.inbound_pending.pending(class), 1);
    assert_eq!(cancel.try_recv(), Ok(()));
    Ok(())
}

/// Unanswered catch-up requests fill the application queue and the swarm sheds the next vote
/// request. The swarm reserves no queue space by class, so this is the expected behavior.
#[tokio::test]
async fn unanswered_catch_up_fills_the_queue_and_sheds_votes() -> eyre::Result<()> {
    let pair = connected_pair(test_config()).await?;

    let _first = send(&pair, catch_up()).await?;
    queued(&pair.events, 1).await?;
    let _second = send(&pair, catch_up()).await?;
    queued(&pair.events, SERVER_QUEUE).await?;

    let vote_reply = send(&pair, vote()).await?;
    assert!(failed(vote_reply).await?, "a full queue must shed the vote request");
    assert_eq!(pair.events.len(), SERVER_QUEUE);
    Ok(())
}
