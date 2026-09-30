//! Measurement harness for peer-local QUIC traffic contention (advisory GHSA-5pxp-f3g9-vcwx).
//!
//! # What this measures
//!
//! Every libp2p-quic listener drives its `quinn::Endpoint` from a single background task whose
//! `EndpointDriver::poll` takes the per-endpoint state `Mutex` and holds it across `drive_recv`
//! plus `handle_events` (quinn 0.11.9 `endpoint.rs:367-391`). Inbound connection acceptance takes
//! the *same* `Mutex`: `EndpointInner::accept` locks the state around
//! `quinn_proto::Endpoint::accept` (`endpoint.rs:414-424`), which decrypts the Initial, runs the
//! rustls first flight and produces the server `CertificateVerify` signature before the lock is
//! released. libp2p-quic 0.14.0 calls `incoming.accept()` synchronously from the listener poll
//! (`transport.rs:590-595`) and exposes no `retry` / `refuse` / `max_incoming` knob, so a node
//! cannot move that work off the shared lock without an upstream change.
//!
//! This harness measures established request/response latency under five workloads:
//!
//! * `baseline` . . . one measured peer talking to the target, no other load.
//! * `same-endpoint-load` . . . extra peers send requests to the target.
//! * `sibling-endpoint-load` . . . those peers send requests to a separate listening peer in the
//!   same process. The comparison bounds total peer-local contention, including the socket,
//!   endpoint driver, swarm, event and command channels, and responder. The mutex is one
//!   unseparated component; this is not a measurement of its wait or hold time.
//! * `same-endpoint-udp` and `sibling-endpoint-udp` . . . invalid datagrams sent to the respective
//!   listener sockets exercise receive/parse work without reaching the swarm or responder. These
//!   controls still include kernel, socket, and runtime scheduling costs, not just the mutex.
//!
//! # Scope and honesty
//!
//! Accept-path hold time is not exercised: every connection exists before the first arm, and the
//! invalid datagrams cannot initiate a handshake. Measuring acceptance needs a QUIC ClientHello
//! generator or direct instrumentation of quinn's lock wait and hold times. Neither is implemented
//! here. This harness alone cannot justify an acceptance-path mitigation or a no-change decision.
//! Established packets are routed under the endpoint lock; their decryption runs off that lock.
//!
//! Each arm discards a warm-up window, then samples for a fixed duration. Four repetitions
//! alternate forward and reverse arm order. Request loaders keep only one request in flight each.
//! Between arms, load tasks are aborted and joined and pending requests must drain to zero. UDP
//! senders use bounded bursts without timer catch-up; warm-up also discards transients from prior
//! socket traffic. Responders handle requests concurrently, with a finite bound matching the number
//! of requesters.
//!
//! The absolute numbers are host-dependent and mean nothing on a busy or shared machine. Treat the
//! distributions as observations, not causal proof, and run on a quiet, appropriately provisioned
//! host. Percentiles use nearest rank; p99 is omitted when fewer than 100 samples were collected.
//!
//! # Running
//!
//! ```text
//! cargo test -p tn-network-libp2p --lib -- --ignored --nocapture --test-threads=1 admission_contention_report
//! ```
//!
//! The test is `#[ignore]`, so the normal suite compiles it (guarding against rot) but never runs
//! it. Every measured and loader round trip checks its response payload and propagates transport
//! errors immediately. That payload check is the correctness guard, not a sample-count assertion.
//! Empty measurement windows are errors. All spawned harness tasks are owned and aborted on drop.

use super::*;
use crate::common::{TestWorkerRequest, TestWorkerResponse};
use futures::{stream, TryStreamExt};
use std::{net::SocketAddr, time::Instant};
use tn_config::ConsensusConfig;
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::TaskManager;
use tokio::{net::UdpSocket, sync::mpsc, task::JoinSet, time::timeout};

/// Number of load-generating peers or datagram senders.
const LOADERS: usize = 5;
/// Warm-up traffic discarded before each measurement window.
const WARM_UP: Duration = Duration::from_secs(1);
/// Window in which new measured round trips may start.
const SAMPLE_WINDOW: Duration = Duration::from_secs(2);
/// Repetitions, alternating forward and reverse arm order.
const REPETITIONS: usize = 4;

/// Concrete request type exercised by the harness.
type Req = TestWorkerRequest;
/// Concrete response type exercised by the harness.
type Res = TestWorkerResponse;
/// Event stream type the network reports back on.
type Events = mpsc::Sender<NetworkEvent<Req, Res>>;
/// Fully applied network type spawned for every harness peer.
type Net = ConsensusNetwork<Req, Res, MemDatabase, Events>;
/// Fully applied handle type used to drive a harness peer.
type Handle = NetworkHandle<Req, Res>;

/// A running harness peer: its config (for addresses and keys), a handle, and the event receiver
/// until it is taken by a responder or drain task.
struct HarnessPeer {
    /// Consensus config carrying this peer's keys and listen address.
    config: ConsensusConfig<MemDatabase>,
    /// Handle used to dial, request and query this peer's swarm.
    handle: Handle,
    /// Inbound event receiver, taken once by a responder or drain task.
    events: Option<mpsc::Receiver<NetworkEvent<Req, Res>>>,
    /// Swarm and event-handler tasks, aborted automatically if the peer is dropped.
    tasks: JoinSet<()>,
}

/// Build a peer's network from a committee authority config and spawn its swarm task.
async fn spawn_peer(
    config: ConsensusConfig<MemDatabase>,
    task_manager: &TaskManager,
) -> NetworkResult<HarnessPeer> {
    let (tx, rx) = mpsc::channel(2048);
    let network_key = config.key_config().primary_network_keypair().clone();
    let db = MemDatabase::default();
    let net = Net::new(
        config.network_config(),
        tx,
        config.key_config().clone(),
        network_key,
        db,
        task_manager.get_spawner(),
        NetworkType::Primary,
        config.primary_address(),
        None,
    )?;
    let handle = net.network_handle();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        net.run().await.unwrap_or_else(|err| {
            tracing::error!(target: "admission-harness", ?err, "harness network task ended with error");
        });
    });
    // Register the server endpoint before any dial, including outbound dials from this peer.
    handle.start_listening(config.primary_address()).await?;
    Ok(HarnessPeer { config, handle, events: Some(rx), tasks })
}

/// Continuously answer inbound requests with a fixed response. Runs until the channel closes.
fn spawn_responder(peer: &mut HarnessPeer, response: Res) -> eyre::Result<()> {
    let events = peer.events.take().ok_or_else(|| eyre::eyre!("peer events already taken"))?;
    let handle = peer.handle.clone();
    peer.tasks.spawn(async move {
        stream::unfold(events, |mut events| async move { events.recv().await.map(|event| (event, events)) })
            .for_each_concurrent(LOADERS + 1, |event| {
                let handle = handle.clone();
                let response = response.clone();
                async move {
                    if let NetworkEvent::Request { channel, .. } = event {
                        handle.send_response(response, channel).await.unwrap_or_else(|err| {
                            tracing::warn!(target: "admission-harness", ?err, "responder send_response failed");
                        });
                    }
                }
            })
            .await;
    });
    Ok(())
}

/// Drain and discard a peer's inbound events so the network never blocks on a full channel.
fn spawn_drain(peer: &mut HarnessPeer) -> eyre::Result<()> {
    let events = peer.events.take().ok_or_else(|| eyre::eyre!("peer events already taken"))?;
    peer.tasks.spawn(async move {
        stream::unfold(events, |mut events| async move {
            events.recv().await.map(|_event| ((), events))
        })
        .for_each(|()| async {})
        .await;
    });
    Ok(())
}

/// Add a peer as explicit and dial it, establishing an outbound connection.
async fn connect(
    from: &Handle,
    bls: BlsPublicKey,
    network_key: NetworkPublicKey,
    addr: Multiaddr,
) -> eyre::Result<()> {
    from.add_explicit_peer(bls, network_key, addr).await?;
    from.dial_by_bls(bls).await?;
    Ok(())
}

/// Complete and validate one request, bounding command submission and response waiting together.
async fn round_trip(
    from: &Handle,
    to: BlsPublicKey,
    request: &Req,
    expected: &Res,
    max_wait: Duration,
) -> eyre::Result<Duration> {
    let start = Instant::now();
    timeout(max_wait, async {
        let response = from.send_request(request.clone(), to).await?;
        let message =
            response.await.map_err(|_| eyre::eyre!("response channel closed before a reply"))??;
        eyre::ensure!(message.result == *expected, "unexpected response payload");
        Ok::<_, eyre::Report>(())
    })
    .await
    .map_err(|_| eyre::eyre!("request timed out awaiting response"))??;
    Ok(start.elapsed())
}

/// Sample sequential round trips for a fixed window, stopping on the first error or bad payload.
async fn sample_rtt(
    from: &Handle,
    to: BlsPublicKey,
    request: &Req,
    expected: &Res,
    window: Duration,
    max_wait: Duration,
) -> eyre::Result<Vec<Duration>> {
    let samples: Vec<Duration> = stream::unfold(Instant::now(), |start| async move {
        (start.elapsed() < window).then_some(((), start))
    })
    .then(|()| round_trip(from, to, request, expected, max_wait))
    .try_collect()
    .await?;
    eyre::ensure!(!samples.is_empty(), "measurement window produced no samples");
    Ok(samples)
}

/// Load applied while the measured connection keeps requesting from the target.
#[derive(Clone, Copy)]
enum Load<'a> {
    /// No extra traffic.
    Idle,
    /// One in-flight request per loader to this peer.
    Requests(&'a BlsPublicKey),
    /// Invalid UDP datagrams addressed to this listener socket.
    Datagrams(SocketAddr),
}

/// One named workload in the repeated measurement schedule.
#[derive(Clone, Copy)]
struct Arm<'a> {
    /// Label printed with the arm's distribution.
    label: &'static str,
    /// Traffic generated while sampling.
    load: Load<'a>,
}

/// Send bounded bursts of invalid QUIC packets without generating swarm requests or handshakes.
async fn flood_datagrams(target: SocketAddr) -> eyre::Result<()> {
    let bind_addr = if target.is_ipv4() { "127.0.0.1:0" } else { "[::1]:0" };
    let socket = UdpSocket::bind(bind_addr).await?;
    let mut ticks = tokio::time::interval(Duration::from_millis(1));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stream::unfold(ticks, |mut ticks| async move {
        ticks.tick().await;
        Some(((), ticks))
    })
    .map(Ok::<_, eyre::Report>)
    .try_for_each(|()| async {
        stream::iter(0..16)
            .map(Ok::<_, eyre::Report>)
            .try_for_each(|_| async {
                // A zero first byte has neither a long header nor the QUIC fixed bit.
                socket.send_to(&[0; 64], target).await?;
                Ok(())
            })
            .await
    })
    .await
}

/// Extract the fixture's literal IP and UDP port for the datagram-only control.
fn listener_socket(address: &Multiaddr) -> eyre::Result<SocketAddr> {
    use libp2p::multiaddr::Protocol;
    let ip = address
        .iter()
        .find_map(|protocol| {
            if let Protocol::Ip4(ip) = protocol {
                Some(std::net::IpAddr::V4(ip))
            } else if let Protocol::Ip6(ip) = protocol {
                Some(std::net::IpAddr::V6(ip))
            } else {
                None
            }
        })
        .ok_or_else(|| eyre::eyre!("listener address has no literal IP"))?;
    let port = address
        .iter()
        .find_map(|protocol| if let Protocol::Udp(port) = protocol { Some(port) } else { None })
        .ok_or_else(|| eyre::eyre!("listener address has no UDP port"))?;
    eyre::ensure!(ip.is_loopback(), "harness listener must be on loopback");
    Ok(SocketAddr::new(ip, port))
}

/// The measured request and its validation settings, shared by every arm.
struct Probe<'a> {
    /// Handle of the peer issuing measured requests.
    measured: &'a Handle,
    /// Peer receiving measured requests.
    target: BlsPublicKey,
    /// Request payload sent by measured and load-generating peers.
    request: &'a Req,
    /// Expected response payload for every completed round trip.
    expected: &'a Res,
    /// Timeout for an individual round trip, including command submission.
    max_wait: Duration,
}

/// Warm up and sample one workload, then join its senders and drain pending requests before return.
async fn run_arm(
    loaders: &[HarnessPeer],
    load: Load<'_>,
    probe: &Probe<'_>,
) -> eyre::Result<Vec<Duration>> {
    let mut floods = JoinSet::new();
    loaders.iter().for_each(|loader| match load {
        Load::Idle => {}
        Load::Requests(flood_target) => {
            let flood_target = *flood_target;
            let handle = loader.handle.clone();
            let request = probe.request.clone();
            let expected = probe.expected.clone();
            let max_wait = probe.max_wait;
            floods.spawn(async move {
                stream::repeat_with(|| ())
                    .then(|()| round_trip(&handle, flood_target, &request, &expected, max_wait))
                    .try_for_each(|_| futures::future::ready(Ok(())))
                    .await
            });
        }
        Load::Datagrams(socket) => {
            floods.spawn(flood_datagrams(socket));
        }
    });
    let measure = async {
        sample_rtt(
            probe.measured,
            probe.target,
            probe.request,
            probe.expected,
            WARM_UP,
            probe.max_wait,
        )
        .await?;
        sample_rtt(
            probe.measured,
            probe.target,
            probe.request,
            probe.expected,
            SAMPLE_WINDOW,
            probe.max_wait,
        )
        .await
    };
    let result = tokio::select! {
        result = measure => result,
        stopped = floods.join_next(), if !floods.is_empty() => {
            stopped
                .ok_or_else(|| eyre::eyre!("missing load task"))
                .and_then(|joined| joined.map_err(eyre::Report::from))
                .and_then(|loaded| loaded)
                .and_then(|()| Err(eyre::eyre!("load task stopped before sampling finished")))
        }
    };
    floods.shutdown().await;
    // The command count includes submitted requests until their response or failure is processed.
    let drained = stream::iter(loaders.iter().map(|loader| &loader.handle).chain([probe.measured]))
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|handle| async move {
            wait_until(probe.max_wait, "arm pending requests drain", || async {
                Ok(handle.get_pending_request_count().await? == 0)
            })
            .await
        })
        .await;
    // Preserve a sampling or load failure even if its cleanup also fails.
    let samples = result?;
    drained?;
    Ok(samples)
}

/// Nearest-rank percentile (in milliseconds) over pre-sorted samples.
fn percentile_ms(sorted: &[Duration], pct: usize) -> eyre::Result<f64> {
    eyre::ensure!((1..=100).contains(&pct), "percentile must be between 1 and 100");
    let idx = sorted.len().saturating_mul(pct).div_ceil(100).saturating_sub(1);
    sorted
        .get(idx)
        .map(|sample| sample.as_secs_f64() * 1000.0)
        .ok_or_else(|| eyre::eyre!("cannot report an empty sample set"))
}

/// Print one arm's latency distribution.
fn report(repetition: usize, label: &str, samples: &[Duration]) -> eyre::Result<()> {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let max_ms = percentile_ms(&sorted, 100)?;
    let p99 = if sorted.len() >= 100 {
        format!("{:.3}ms", percentile_ms(&sorted, 99)?)
    } else {
        "n/a (n<100)".to_owned()
    };
    println!(
        "[admission-harness] repeat={repetition} {label:<22} n={:>3}  p50={:>9.3}ms  p90={:>9.3}ms  p99={p99}  max={max_ms:.3}ms",
        sorted.len(),
        percentile_ms(&sorted, 50)?,
        percentile_ms(&sorted, 90)?,
    );
    Ok(())
}

/// Nearest rank includes the last of 40 samples at p99 and refuses invalid inputs.
#[test]
fn nearest_rank_percentiles() -> eyre::Result<()> {
    let samples = (1..=40).map(Duration::from_millis).collect::<Vec<_>>();
    assert_eq!(percentile_ms(&samples, 50)?, 20.0);
    assert_eq!(percentile_ms(&samples, 90)?, 36.0);
    assert_eq!(percentile_ms(&samples, 99)?, 40.0);
    assert!(percentile_ms(&[], 50).is_err());
    assert!(percentile_ms(&samples, 0).is_err());
    assert!(percentile_ms(&samples, 101).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement harness for GHSA-5pxp QUIC endpoint contention; run manually on a quiet, \
            provisioned host with --ignored --nocapture"]
/// Report repeated workload distributions without asserting a host-dependent latency gap.
async fn admission_contention_report() -> eyre::Result<()> {
    /// Committee size: target, measured, sibling, plus the loaders.
    const COMMITTEE: usize = 3 + LOADERS;

    let setup_timeout = Duration::from_secs(60);
    let request_timeout = Duration::from_secs(10);

    // Build one committee so every peer accepts the others, and spawn each network.
    let committee_size = NonZeroUsize::new(COMMITTEE)
        .ok_or_else(|| eyre::eyre!("committee size must be non-zero"))?;
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).committee_size(committee_size).build();
    let task_manager = TaskManager::default();
    let peers = stream::iter(fixture.authorities())
        .then(|authority| spawn_peer(authority.consensus_config(), &task_manager))
        .try_collect::<Vec<HarnessPeer>>()
        .await?;

    // Assign roles: [0] target, [1] measured, [2] sibling, [3..] loaders.
    let mut roles = peers.into_iter();
    let mut target = roles.next().ok_or_else(|| eyre::eyre!("missing target peer"))?;
    let mut measured = roles.next().ok_or_else(|| eyre::eyre!("missing measured peer"))?;
    let mut sibling = roles.next().ok_or_else(|| eyre::eyre!("missing sibling peer"))?;
    let mut loaders = roles.collect::<Vec<HarnessPeer>>();

    // Payload: reuse the worker missing-batch round trip.
    let missing_block = fixture_batch_with_transactions(3).seal_slow();
    let request = TestWorkerRequest::MissingBatches(vec![missing_block.digest()]);
    let response = TestWorkerResponse::MissingBatches { batches: vec![missing_block] };

    // The target and sibling answer every request; everyone else just drains events.
    spawn_responder(&mut target, response.clone())?;
    spawn_responder(&mut sibling, response.clone())?;
    spawn_drain(&mut measured)?;
    loaders.iter_mut().try_for_each(spawn_drain)?;

    // Connection endpoints for the peers that receive dials.
    let target_bls = target.config.key_config().primary_public_key();
    let target_key = target.config.primary_networkkey();
    let target_addr = target.config.primary_address();
    let sibling_bls = sibling.config.key_config().primary_public_key();
    let sibling_key = sibling.config.primary_networkkey();
    let sibling_addr = sibling.config.primary_address();

    // Establish the measured connection and every loader connection to both target and sibling.
    connect(&measured.handle, target_bls, target_key.clone(), target_addr.clone()).await?;
    stream::iter(loaders.iter())
        .then(|loader| {
            let target_key = target_key.clone();
            let target_addr = target_addr.clone();
            let sibling_key = sibling_key.clone();
            let sibling_addr = sibling_addr.clone();
            async move {
                connect(&loader.handle, target_bls, target_key, target_addr).await?;
                connect(&loader.handle, sibling_bls, sibling_key, sibling_addr).await?;
                Ok::<(), eyre::Report>(())
            }
        })
        .try_collect::<Vec<()>>()
        .await?;

    // Wait for the connections to establish before sampling.
    let target_handle = target.handle.clone();
    wait_until(setup_timeout, "target establishes measured + loaders", move || {
        let target_handle = target_handle.clone();
        async move { Ok(target_handle.established_peer_count().await? > LOADERS) }
    })
    .await?;
    let sibling_handle = sibling.handle.clone();
    wait_until(setup_timeout, "sibling establishes loaders", move || {
        let sibling_handle = sibling_handle.clone();
        async move { Ok(sibling_handle.established_peer_count().await? >= LOADERS) }
    })
    .await?;

    let arms = [
        Arm { label: "baseline", load: Load::Idle },
        Arm { label: "same-endpoint-load", load: Load::Requests(&target_bls) },
        Arm { label: "sibling-endpoint-load", load: Load::Requests(&sibling_bls) },
        Arm { label: "same-endpoint-udp", load: Load::Datagrams(listener_socket(&target_addr)?) },
        Arm {
            label: "sibling-endpoint-udp",
            load: Load::Datagrams(listener_socket(&sibling_addr)?),
        },
    ];
    let probe = Probe {
        measured: &measured.handle,
        target: target_bls,
        request: &request,
        expected: &response,
        max_wait: request_timeout,
    };
    println!(
        "[admission-harness] committee={COMMITTEE} loaders={LOADERS} repetitions={REPETITIONS} \
         warm_up={WARM_UP:?} window={SAMPLE_WINDOW:?} max_in_flight_per_loader=1"
    );
    let schedule = (0..REPETITIONS).flat_map(|repetition| {
        let mut ordered = arms;
        if repetition % 2 == 1 {
            ordered.reverse();
        }
        ordered.into_iter().map(move |arm| (repetition + 1, arm))
    });
    let result = stream::iter(schedule)
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|(repetition, arm)| {
            let loaders = &loaders;
            let probe = &probe;
            async move {
                let samples = run_arm(loaders, arm.load, probe).await?;
                report(repetition, arm.label, &samples)
            }
        })
        .await;
    // Join normal shutdown; JoinSet also aborts all owned tasks on any earlier return.
    stream::iter([&mut target, &mut measured, &mut sibling].into_iter().chain(loaders.iter_mut()))
        .for_each(|peer| async { peer.tasks.shutdown().await })
        .await;
    result
}
