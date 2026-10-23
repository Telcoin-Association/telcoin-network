//! Measurement harness for vote latency and per-peer requests while a validator catches up
//! (issue #1742).
//!
//! # What this measures
//!
//! One validator catches up from a recorded height gap while the committee runs at its normal
//! rate. Every peer is a real `ConsensusNetwork` swarm on loopback QUIC. Catch-up uses the sync
//! stream protocol (`Req`, `Ack`, `Data` and `End` frames). Votes and epoch records use the
//! request-response protocol and carry the `vote` and `epoch_record` service classes. The
//! harness records:
//!
//! * vote round-trip latency from committee peer 0 to the catching-up peer, whose connection
//!   carries the sync streams, and to committee peer 1, whose connection carries none;
//! * the honest peak of in-flight requests per target peer, with its distribution;
//! * the peak of concurrent open sync streams per connection;
//! * configured receive credit and resident bytes, reported separately;
//! * process RSS, sampled every 100 ms; and
//! * catch-up completion time.
//!
//! The `baseline` arm runs the build defaults. The `regression` arm derives per-peer limits from
//! the baseline peaks, applies them through `process_budget`, adds hostile committee identities
//! that push each ceiling, and repeats the same workload. Both arms read one `Workload` value.
//!
//! # Scope and honesty
//!
//! One swarm per peer: repeat the run for each swarm role (primary, then each worker). The
//! harness serves the sync streams itself, so the node's own sync responder limits are not
//! exercised. Hostile loaders are cooperative Rust tasks, not a packet generator. RSS is the
//! test process, which holds every peer. Percentiles use nearest rank: p99 needs 100 samples
//! and p99.9 needs 1000 samples, otherwise the record holds `null` instead of a number. The
//! swarm's own protocol streams (identify, gossip, kademlia, request-response) are not counted;
//! the derived stream limit adds a declared allowance for them. Swarm metrics are aggregated
//! over every peer in the process because all peers share one metrics label set. Missing data
//! is unknown, never zero. The numbers are host dependent. A lower constant without a passing
//! regression run closes nothing.
//!
//! # Running
//!
//! ```text
//! cargo test -p tn-network-libp2p --lib -- --ignored --nocapture --test-threads=1 catch_up_contention_report
//! ```
//!
//! Each arm writes one evidence record to `$TN_M3_EVIDENCE_DIR`, or to `target/m3-evidence/`
//! under the workspace root. `TN_M3_VOTE_P99_MS` and `TN_M3_CATCH_UP_FACTOR` override the
//! declared bounds. `catch_up_contention_smoke` runs the same code at a small size in the normal
//! suite and asserts structure only.

use super::*;
use crate::{
    common::{TestPrimaryRequest, TestPrimaryResponse},
    sync::{read_frame, write_frame, SyncFrame},
};
use futures::{future, stream, StreamExt, TryStreamExt};
use libp2p::Stream;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use serde::{Deserialize, Serialize};
use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tn_config::{ConsensusConfig, NetworkConfig, NetworkProcessBudget};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::{Header, TaskManager};
use tokio::{sync::mpsc, task::JoinSet, time::timeout};

/// Headroom of the derivation rule, in percent of the observed peak.
const HEADROOM_PERCENT: u64 = 150;
/// Declared stream allowance for the swarm's own protocols on one connection.
const PROTOCOL_STREAM_ALLOWANCE: u32 = 8;
/// Smallest per-connection receive credit the derivation proposes, in bytes.
const MIN_RECEIVE_CREDIT: u64 = 1024 * 1024;
/// Default vote p99 bound on the sync-carrying connection, in milliseconds.
const DEFAULT_VOTE_P99_MS: f64 = 250.0;
/// Default catch-up completion factor against the baseline arm.
const DEFAULT_CATCH_UP_FACTOR: f64 = 3.0;
/// Largest encoded sync frame the harness reads or writes.
const MAX_FRAME: usize = 1024 * 1024;
/// RSS and swarm metric sampling interval.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// Longest catch-up the harness waits for after the probe windows.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(600);
/// Longest wait for one sync stream to open.
const STREAM_OPEN_WAIT: Duration = Duration::from_secs(20);
/// Concurrent inbound events one responder serves.
const RESPONDER_CONCURRENCY: usize = 4096;
/// Buckets of the in-flight histogram; the last bucket holds every larger count.
const HISTOGRAM_BUCKETS: usize = 64;
/// Owner of the declared bounds.
const BOUND_OWNER: &str = "Onyeka Obi (MavenRain)";
/// Date the bounds were declared, before the first regression run.
const BOUND_DATE: &str = "2026-10-23";

/// Concrete request type exercised by the harness.
type Req = TestPrimaryRequest;
/// Concrete response type exercised by the harness.
type Res = TestPrimaryResponse;
/// Event stream type the network reports back on.
type Events = mpsc::Sender<NetworkEvent<Req, Res>>;
/// Fully applied network type spawned for every harness peer.
type Net = ConsensusNetwork<Req, Res, MemDatabase, Events>;
/// Fully applied handle type used to drive a harness peer.
type Handle = NetworkHandle<Req, Res>;

/// The honest workload. Both arms read the same value, so they cannot drift.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct Workload {
    /// Committee peers. Peer 0 is the probe, peer 1 is the no-sync comparison target.
    committee: usize,
    /// Hostile committee identities. Idle in the baseline arm, loaded in the regression arm.
    hostile: usize,
    /// Concurrent sync streams the catching-up peer opens on each committee connection.
    streams_per_connection: u32,
    /// Sync exchanges per committee peer, the recorded height gap.
    exchanges_per_peer: u32,
    /// Data frames per exchange.
    frames_per_exchange: u32,
    /// Payload bytes per data frame.
    frame_bytes: usize,
    /// Vote requests each committee peer keeps in flight to every other peer.
    votes_in_flight: usize,
    /// Interval between epoch record requests from each committee peer to every other peer.
    epoch_record_interval: Duration,
    /// Warm-up discarded before the first sample window.
    warm_up: Duration,
    /// Probe sample window.
    sample_window: Duration,
    /// Sample windows per arm.
    repetitions: usize,
    /// Timeout for one vote or epoch record round trip.
    request_timeout: Duration,
}

impl Workload {
    /// The reported workload.
    const REPORT: Self = Self {
        committee: 4,
        hostile: 2,
        streams_per_connection: 4,
        exchanges_per_peer: 32,
        frames_per_exchange: 16,
        frame_bytes: 64 * 1024,
        votes_in_flight: 1,
        epoch_record_interval: Duration::from_millis(250),
        warm_up: Duration::from_secs(1),
        sample_window: Duration::from_secs(2),
        repetitions: 4,
        request_timeout: Duration::from_secs(10),
    };

    /// The smoke workload: the same code path at a size the normal suite can afford.
    const SMOKE: Self = Self {
        committee: 3,
        hostile: 1,
        streams_per_connection: 2,
        exchanges_per_peer: 2,
        frames_per_exchange: 4,
        frame_bytes: 4096,
        votes_in_flight: 1,
        epoch_record_interval: Duration::from_millis(50),
        warm_up: Duration::from_millis(100),
        sample_window: Duration::from_millis(200),
        repetitions: 1,
        request_timeout: Duration::from_secs(10),
    };

    /// Every peer: the committee, the catching-up peer, then the hostile identities.
    fn peers(self) -> usize {
        self.committee.saturating_add(1).saturating_add(self.hostile)
    }

    /// Index of the catching-up peer.
    fn catch_up_index(self) -> usize {
        self.committee
    }

    /// Sync exchanges each stream worker performs on one connection.
    fn exchanges_per_worker(self) -> u32 {
        self.exchanges_per_peer.div_ceil(self.streams_per_connection.max(1))
    }

    /// Bytes the catching-up peer receives when every exchange completes.
    fn expected_catch_up_bytes(self) -> u64 {
        let frames = u64::from(self.streams_per_connection)
            .saturating_mul(u64::from(self.exchanges_per_worker()))
            .saturating_mul(u64::from(self.frames_per_exchange));
        u64::try_from(self.committee)
            .unwrap_or(u64::MAX)
            .saturating_mul(frames)
            .saturating_mul(u64::try_from(self.frame_bytes).unwrap_or(u64::MAX))
    }
}

/// The opening request of one harness sync exchange.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CatchUpRequest {
    /// Data frames the responder sends.
    frames: u32,
    /// Payload bytes per data frame.
    frame_bytes: u32,
}

/// A peer identity with its role index.
#[derive(Clone, Copy)]
struct PeerRef {
    /// Role index in the topology.
    index: usize,
    /// Consensus key used to address the peer.
    bls: BlsPublicKey,
}

/// A current count with its peak.
#[derive(Default)]
struct PeakGauge {
    /// Current count.
    current: AtomicU32,
    /// Highest count observed.
    peak: AtomicU32,
}

impl PeakGauge {
    /// Add one unit and return a guard that releases it on drop.
    fn occupy(&self) -> Occupancy<'_> {
        let level = self.current.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        self.peak.fetch_max(level, Ordering::SeqCst);
        Occupancy { gauge: self, level }
    }

    /// Highest count observed.
    fn peak(&self) -> u32 {
        self.peak.load(Ordering::SeqCst)
    }
}

/// One unit of a gauge, released on drop.
struct Occupancy<'a> {
    /// The gauge that holds the unit.
    gauge: &'a PeakGauge,
    /// The count right after this unit was added.
    level: u32,
}

impl Drop for Occupancy<'_> {
    fn drop(&mut self) {
        self.gauge.current.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Observations of the in-flight count at each request start, by count.
struct InFlightHistogram {
    /// Observation count per in-flight level; the last bucket holds every larger level.
    buckets: Vec<AtomicU64>,
}

impl InFlightHistogram {
    /// An empty histogram.
    fn new() -> Self {
        Self { buckets: (0..HISTOGRAM_BUCKETS).map(|_| AtomicU64::new(0)).collect() }
    }

    /// Record one request start at `level` in-flight requests.
    fn observe(&self, level: u32) {
        let index =
            usize::try_from(level).unwrap_or(usize::MAX).min(HISTOGRAM_BUCKETS.saturating_sub(1));
        if let Some(bucket) = self.buckets.get(index) {
            bucket.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Non-empty buckets.
    fn buckets(&self) -> Vec<HistogramBucket> {
        self.buckets
            .iter()
            .enumerate()
            .map(|(level, bucket)| HistogramBucket {
                in_flight: u32::try_from(level).unwrap_or(u32::MAX),
                observations: bucket.load(Ordering::SeqCst),
            })
            .filter(|bucket| bucket.observations > 0)
            .collect()
    }
}

/// One histogram bucket.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct HistogramBucket {
    /// In-flight requests at the request start.
    in_flight: u32,
    /// Request starts that observed this level.
    observations: u64,
}

/// Counters shared by every loader task of one arm.
struct Telemetry {
    /// Honest in-flight requests per target peer index.
    in_flight: Vec<PeakGauge>,
    /// Honest in-flight distribution per target peer index.
    histograms: Vec<InFlightHistogram>,
    /// Open sync streams per (opener, target) connection, flat index `opener * peers + target`.
    streams: Vec<PeakGauge>,
    /// Peer count, for the flat connection index.
    peers: usize,
    /// Whether catch-up is still running.
    catch_up_active: AtomicBool,
    /// Epoch record requests answered with the expected payload in time.
    epoch_records_served: AtomicU64,
    /// Epoch record requests that failed or timed out.
    epoch_records_failed: AtomicU64,
    /// Hostile sync streams that opened and sent their request.
    hostile_streams_opened: AtomicU64,
    /// Hostile sync stream opens that failed or timed out.
    hostile_streams_refused: AtomicU64,
    /// Hostile requests answered with the expected payload.
    hostile_requests_served: AtomicU64,
    /// Hostile requests that failed or timed out.
    hostile_requests_failed: AtomicU64,
}

impl Telemetry {
    /// Counters for `peers` peers.
    fn new(peers: usize) -> Self {
        Self {
            in_flight: (0..peers).map(|_| PeakGauge::default()).collect(),
            histograms: (0..peers).map(|_| InFlightHistogram::new()).collect(),
            streams: (0..peers.saturating_mul(peers)).map(|_| PeakGauge::default()).collect(),
            peers,
            catch_up_active: AtomicBool::new(true),
            epoch_records_served: AtomicU64::new(0),
            epoch_records_failed: AtomicU64::new(0),
            hostile_streams_opened: AtomicU64::new(0),
            hostile_streams_refused: AtomicU64::new(0),
            hostile_requests_served: AtomicU64::new(0),
            hostile_requests_failed: AtomicU64::new(0),
        }
    }

    /// The stream gauge of the connection `opener` holds with `target`.
    fn connection(&self, opener: usize, target: usize) -> eyre::Result<&PeakGauge> {
        self.streams
            .get(opener.saturating_mul(self.peers).saturating_add(target))
            .ok_or_else(|| eyre::eyre!("no stream gauge for connection {opener} -> {target}"))
    }

    /// Count one outcome into `served` or `failed`.
    fn count(served: &AtomicU64, failed: &AtomicU64, outcome: &eyre::Result<Duration>) {
        if outcome.is_ok() {
            served.fetch_add(1, Ordering::SeqCst);
        } else {
            failed.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// A running harness peer: its config (for addresses and keys), a handle, and the event
/// receiver until a responder or drain task takes it.
struct HarnessPeer {
    /// Role index in the topology.
    index: usize,
    /// Consensus config carrying this peer's keys and listen address.
    config: ConsensusConfig<MemDatabase>,
    /// Handle used to dial, request, open streams and query this peer's swarm.
    handle: Handle,
    /// Inbound event receiver, taken once by a responder or drain task.
    events: Option<mpsc::Receiver<NetworkEvent<Req, Res>>>,
    /// Swarm and event-handler tasks, aborted automatically if the peer is dropped.
    tasks: JoinSet<()>,
}

impl HarnessPeer {
    /// This peer's identity and role index.
    fn peer_ref(&self) -> PeerRef {
        PeerRef { index: self.index, bls: self.config.key_config().primary_public_key() }
    }
}

/// Return `base` with `budget` as its `process_budget`, through the config's own serde form.
fn with_budget(base: &NetworkConfig, budget: &NetworkProcessBudget) -> eyre::Result<NetworkConfig> {
    let mut value = serde_json::to_value(base)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| eyre::eyre!("network config did not serialize as an object"))?;
    object.insert("process_budget".to_owned(), serde_json::to_value(budget)?);
    serde_json::from_value(value).map_err(Into::into)
}

/// Build a peer's network from a committee authority config and spawn its swarm task.
async fn spawn_peer(
    index: usize,
    config: ConsensusConfig<MemDatabase>,
    task_manager: &TaskManager,
    budget: Option<&NetworkProcessBudget>,
) -> eyre::Result<HarnessPeer> {
    let budgeted = budget.map(|budget| with_budget(config.network_config(), budget)).transpose()?;
    let network_config = budgeted.as_ref().unwrap_or_else(|| config.network_config());
    let (tx, rx) = mpsc::channel(2048);
    let network_key = config.key_config().primary_network_keypair().clone();
    let net = Net::new(
        network_config,
        tx,
        config.key_config().clone(),
        network_key,
        MemDatabase::default(),
        task_manager.get_spawner(),
        NetworkType::Primary,
        config.primary_address(),
        None,
    )?;
    let handle = net.network_handle();
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        net.run().await.unwrap_or_else(|err| {
            tracing::error!(target: "catch-up-harness", ?err, "harness network task ended with error");
        });
    });
    handle.start_listening(config.primary_address()).await?;
    Ok(HarnessPeer { index, config, handle, events: Some(rx), tasks })
}

/// The fixed answer to one request. The class comes from the request, not from the answer.
fn answer(request: &Req) -> Res {
    match request {
        TestPrimaryRequest::Vote { .. } => TestPrimaryResponse::MissingParents(Vec::new()),
        TestPrimaryRequest::EpochRecord { epoch } => TestPrimaryResponse::EpochRecord(*epoch),
        TestPrimaryRequest::NewCertificate { .. } | TestPrimaryRequest::MissingCertificates(_) => {
            TestPrimaryResponse::MissingCertificates(Vec::new())
        }
    }
}

/// Serve one inbound sync exchange: read the request, acknowledge, send the data frames, end.
async fn serve_stream(stream: Stream) -> eyre::Result<()> {
    let mut dec = Vec::new();
    let mut comp = Vec::new();
    let mut enc = Vec::new();
    let mut stream = stream;
    let opening: SyncFrame<CatchUpRequest> =
        read_frame(&mut stream, &mut dec, &mut comp, MAX_FRAME).await?;
    let SyncFrame::Req(request) = opening else {
        eyre::bail!("sync exchange did not open with a request frame");
    };
    write_frame(&mut stream, &SyncFrame::<CatchUpRequest>::Ack, &mut enc, &mut comp, MAX_FRAME)
        .await?;
    let payload = vec![0xA5_u8; usize::try_from(request.frame_bytes)?];
    let (mut stream, mut enc, mut comp) = stream::iter(0..request.frames)
        .map(Ok::<_, eyre::Report>)
        .try_fold((stream, enc, comp), |(mut stream, mut enc, mut comp), _| {
            let payload = payload.clone();
            async move {
                write_frame(
                    &mut stream,
                    &SyncFrame::<CatchUpRequest>::Data(payload),
                    &mut enc,
                    &mut comp,
                    MAX_FRAME,
                )
                .await?;
                Ok((stream, enc, comp))
            }
        })
        .await?;
    write_frame(&mut stream, &SyncFrame::<CatchUpRequest>::End, &mut enc, &mut comp, MAX_FRAME)
        .await?;
    Ok(())
}

/// Answer inbound requests and serve inbound sync streams until the channel closes.
fn spawn_responder(peer: &mut HarnessPeer) -> eyre::Result<()> {
    let events = peer.events.take().ok_or_else(|| eyre::eyre!("peer events already taken"))?;
    let handle = peer.handle.clone();
    peer.tasks.spawn(async move {
        stream::unfold(events, |mut events| async move {
            events.recv().await.map(|event| (event, events))
        })
        .for_each_concurrent(RESPONDER_CONCURRENCY, |event| {
            let handle = handle.clone();
            async move {
                if let NetworkEvent::Request { request, channel, .. } = event {
                    handle.send_response(answer(&request), channel).await.unwrap_or_else(|err| {
                        tracing::warn!(target: "catch-up-harness", ?err, "send_response failed");
                    });
                } else if let NetworkEvent::InboundStream { stream, .. } = event {
                    serve_stream(stream).await.unwrap_or_else(|err| {
                        tracing::debug!(target: "catch-up-harness", ?err, "sync exchange ended");
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
async fn connect(from: &Handle, to: &HarnessPeer) -> eyre::Result<()> {
    let bls = to.config.key_config().primary_public_key();
    from.add_explicit_peer(bls, to.config.primary_networkkey(), to.config.primary_address())
        .await?;
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

/// One honest round trip, counted in the target's in-flight gauge and histogram.
async fn honest_round_trip(
    from: &Handle,
    to: PeerRef,
    telemetry: &Telemetry,
    request: &Req,
    expected: &Res,
    max_wait: Duration,
) -> eyre::Result<Duration> {
    let gauge = telemetry
        .in_flight
        .get(to.index)
        .ok_or_else(|| eyre::eyre!("no in-flight gauge for peer {}", to.index))?;
    let occupancy = gauge.occupy();
    if let Some(histogram) = telemetry.histograms.get(to.index) {
        histogram.observe(occupancy.level);
    }
    round_trip(from, to.bls, request, expected, max_wait).await
}

/// Open one sync stream, run one exchange to its end and return the payload bytes received.
async fn exchange(
    from: &Handle,
    to: BlsPublicKey,
    gauge: &PeakGauge,
    request: CatchUpRequest,
) -> eyre::Result<u64> {
    let mut stream = timeout(STREAM_OPEN_WAIT, from.open_stream(to))
        .await
        .map_err(|_| eyre::eyre!("sync stream open timed out"))???;
    let _occupancy = gauge.occupy();
    let mut enc = Vec::new();
    let mut comp = Vec::new();
    let mut dec = Vec::new();
    write_frame(&mut stream, &SyncFrame::Req(request), &mut enc, &mut comp, MAX_FRAME).await?;
    let ack: SyncFrame<CatchUpRequest> =
        read_frame(&mut stream, &mut dec, &mut comp, MAX_FRAME).await?;
    eyre::ensure!(ack == SyncFrame::Ack, "sync request was not acknowledged: {ack:?}");
    stream::try_unfold((stream, dec, comp), |(mut stream, mut dec, mut comp)| async move {
        let frame: SyncFrame<CatchUpRequest> =
            read_frame(&mut stream, &mut dec, &mut comp, MAX_FRAME).await?;
        match frame {
            SyncFrame::Data(chunk) => Ok(Some((u64::try_from(chunk.len())?, (stream, dec, comp)))),
            SyncFrame::End => Ok(None),
            SyncFrame::Deny(reason) => Err(eyre::eyre!("sync request denied: {reason:?}")),
            SyncFrame::Err(error) => Err(eyre::eyre!("sync exchange aborted: {error:?}")),
            SyncFrame::Req(_) | SyncFrame::Ack => {
                Err(eyre::eyre!("unexpected frame in the data phase"))
            }
        }
    })
    .try_fold(0_u64, |total, len| future::ready(Ok(total.saturating_add(len))))
    .await
}

/// The catching-up peer's completed workload.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct CatchUpSummary {
    /// Wall time from the first stream open to the last `End` frame.
    completion: Duration,
    /// Payload bytes received.
    bytes: u64,
    /// Exchanges completed.
    exchanges: u64,
}

/// Run the whole catch-up workload from `opener` against `targets`.
async fn catch_up(
    handle: Handle,
    opener: usize,
    targets: Vec<PeerRef>,
    telemetry: Arc<Telemetry>,
    workload: Workload,
) -> eyre::Result<CatchUpSummary> {
    let start = Instant::now();
    let request = CatchUpRequest {
        frames: workload.frames_per_exchange,
        frame_bytes: u32::try_from(workload.frame_bytes)?,
    };
    let per_worker = workload.exchanges_per_worker();
    let jobs = targets
        .iter()
        .flat_map(|target| (0..workload.streams_per_connection).map(move |_| *target));
    let handle = &handle;
    let telemetry = &telemetry;
    let result = future::try_join_all(jobs.map(|target| async move {
        let gauge = telemetry.connection(opener, target.index)?;
        stream::iter(0..per_worker)
            .map(Ok::<_, eyre::Report>)
            .try_fold(0_u64, |total, _| async move {
                Ok(total.saturating_add(exchange(handle, target.bls, gauge, request).await?))
            })
            .await
    }))
    .await;
    telemetry.catch_up_active.store(false, Ordering::SeqCst);
    let bytes = result?.into_iter().fold(0_u64, u64::saturating_add);
    let exchanges = u64::try_from(targets.len())
        .unwrap_or(u64::MAX)
        .saturating_mul(u64::from(workload.streams_per_connection))
        .saturating_mul(u64::from(per_worker));
    Ok(CatchUpSummary { completion: start.elapsed(), bytes, exchanges })
}

/// Open a sync stream, send its request and hold it open without reading, forever.
async fn hold_stream(
    from: &Handle,
    to: BlsPublicKey,
    telemetry: &Telemetry,
    request: CatchUpRequest,
) {
    let opened = timeout(STREAM_OPEN_WAIT, from.open_stream(to))
        .await
        .ok()
        .and_then(Result::ok)
        .and_then(Result::ok);
    if let Some(mut stream) = opened {
        let mut enc = Vec::new();
        let mut comp = Vec::new();
        let sent =
            write_frame(&mut stream, &SyncFrame::Req(request), &mut enc, &mut comp, MAX_FRAME)
                .await;
        if sent.is_ok() {
            telemetry.hostile_streams_opened.fetch_add(1, Ordering::SeqCst);
            future::pending::<()>().await;
        } else {
            telemetry.hostile_streams_refused.fetch_add(1, Ordering::SeqCst);
        }
    } else {
        telemetry.hostile_streams_refused.fetch_add(1, Ordering::SeqCst);
    }
}

/// One probe sample.
#[derive(Clone, Copy)]
struct ProbeSample {
    /// Round-trip time of the vote.
    rtt: Duration,
    /// Whether catch-up was still running when the vote was sent.
    during_catch_up: bool,
}

/// The measured vote and its two targets.
struct Probe<'a> {
    /// Handle of committee peer 0.
    measured: &'a Handle,
    /// The catching-up peer, whose connection with peer 0 carries sync streams.
    sync_target: PeerRef,
    /// Committee peer 1, whose connection with peer 0 carries no sync streams.
    plain_target: PeerRef,
    /// Shared counters.
    telemetry: &'a Telemetry,
    /// Vote request.
    request: &'a Req,
    /// Expected vote answer.
    expected: &'a Res,
    /// Timeout for one round trip.
    max_wait: Duration,
}

impl Probe<'_> {
    /// One sample to `target`.
    async fn sample(&self, target: PeerRef) -> eyre::Result<ProbeSample> {
        let during_catch_up = self.telemetry.catch_up_active.load(Ordering::SeqCst);
        let rtt = honest_round_trip(
            self.measured,
            target,
            self.telemetry,
            self.request,
            self.expected,
            self.max_wait,
        )
        .await?;
        Ok(ProbeSample { rtt, during_catch_up })
    }

    /// Sample both targets alternately for `window`; `sync_first` sets the order in each pair.
    async fn window(
        &self,
        window: Duration,
        sync_first: bool,
    ) -> eyre::Result<Vec<(ProbeSample, ProbeSample)>> {
        let samples: Vec<(ProbeSample, ProbeSample)> =
            stream::unfold(Instant::now(), |start| async move {
                (start.elapsed() < window).then_some(((), start))
            })
            .then(|()| async move {
                if sync_first {
                    let sync = self.sample(self.sync_target).await?;
                    let plain = self.sample(self.plain_target).await?;
                    Ok::<_, eyre::Report>((sync, plain))
                } else {
                    let plain = self.sample(self.plain_target).await?;
                    let sync = self.sample(self.sync_target).await?;
                    Ok((sync, plain))
                }
            })
            .try_collect()
            .await?;
        eyre::ensure!(!samples.is_empty(), "measurement window produced no samples");
        Ok(samples)
    }
}

/// Nearest-rank percentile in milliseconds over sorted samples; `tenths` is the rank in tenths
/// of a percent (500 is p50, 990 is p99, 999 is p99.9, 1000 is the maximum).
fn percentile_ms(sorted: &[Duration], tenths: usize) -> Option<f64> {
    let valid = (1..=1000).contains(&tenths) && !sorted.is_empty();
    let index = sorted.len().saturating_mul(tenths).div_ceil(1000).saturating_sub(1);
    valid.then(|| sorted.get(index)).flatten().map(|sample| sample.as_secs_f64() * 1000.0)
}

/// Percentiles of one series, with `null` where the sample count is too small.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LatencySummary {
    /// Series name.
    label: String,
    /// Sample count.
    samples: usize,
    /// p50 in milliseconds.
    p50_ms: Option<f64>,
    /// p99 in milliseconds; `null` below 100 samples.
    p99_ms: Option<f64>,
    /// p99.9 in milliseconds; `null` below 1000 samples.
    p999_ms: Option<f64>,
    /// Maximum in milliseconds.
    max_ms: Option<f64>,
}

/// Summarize one series.
fn summarize(label: &str, samples: &[Duration]) -> LatencySummary {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    LatencySummary {
        label: label.to_owned(),
        samples: sorted.len(),
        p50_ms: percentile_ms(&sorted, 500),
        p99_ms: (sorted.len() >= 100).then(|| percentile_ms(&sorted, 990)).flatten(),
        p999_ms: (sorted.len() >= 1000).then(|| percentile_ms(&sorted, 999)).flatten(),
        max_ms: percentile_ms(&sorted, 1000),
    }
}

/// Resident set size of this process in kibibytes, from a Linux `/proc/self/status` text.
fn parse_vm_rss_kib(status: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
}

/// Resident set size in kibibytes, from `ps -o rss=` output.
fn parse_ps_rss_kib(output: &str) -> Option<u64> {
    output.split_whitespace().next().and_then(|kib| kib.parse().ok())
}

/// Resident set size of this process in bytes, or `None` when the host cannot report it.
fn resident_bytes() -> Option<u64> {
    match () {
        () if cfg!(target_os = "linux") => std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| parse_vm_rss_kib(&status))
            .map(|kib| kib.saturating_mul(1024)),
        () if cfg!(target_os = "macos") => Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .and_then(|text| parse_ps_rss_kib(&text))
            .map(|kib| kib.saturating_mul(1024)),
        () => None,
    }
}

/// The process-wide debugging recorder, when this test installed it.
struct MetricsSource {
    /// Snapshot access; `None` when another recorder owns the process.
    snapshotter: Option<Snapshotter>,
}

impl MetricsSource {
    /// Install the debugging recorder once; keep going without metrics if one is installed.
    fn install() -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        Self { snapshotter: recorder.install().ok().map(|()| snapshotter) }
    }

    /// Current value of the gauge `name` for `class`, if recorded.
    fn class_gauge(&self, name: &str, class: &str) -> Option<f64> {
        self.snapshotter.as_ref().and_then(|snapshotter| {
            snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
                let matches = key.key().name() == name
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "class" && label.value() == class);
                match value {
                    DebugValue::Gauge(gauge) if matches => Some(gauge.into_inner()),
                    DebugValue::Gauge(_) | DebugValue::Counter(_) | DebugValue::Histogram(_) => {
                        None
                    }
                }
            })
        })
    }

    /// Sum of every counter series named `name` for `class`, if recorded.
    fn class_counter(&self, name: &str, class: &str) -> Option<u64> {
        self.snapshotter.as_ref().map(|snapshotter| {
            snapshotter.snapshot().into_vec().into_iter().fold(
                0_u64,
                |total, (key, _, _, value)| {
                    let matches = key.key().name() == name
                        && key
                            .key()
                            .labels()
                            .any(|label| label.key() == "class" && label.value() == class);
                    match value {
                        DebugValue::Counter(count) if matches => total.saturating_add(count),
                        DebugValue::Counter(_)
                        | DebugValue::Gauge(_)
                        | DebugValue::Histogram(_) => total,
                    }
                },
            )
        })
    }
}

/// Peaks and last values sampled every `SAMPLE_INTERVAL`.
struct SampleStore {
    /// Highest RSS seen, in bytes.
    rss_peak: AtomicU64,
    /// Last RSS seen, in bytes.
    rss_last: AtomicU64,
    /// Whether any RSS sample succeeded.
    rss_seen: AtomicBool,
    /// Highest sampled `vote` pending gauge, as `f64` bits.
    pending_vote_bits: AtomicU64,
    /// Highest sampled `epoch_record` pending gauge, as `f64` bits.
    pending_epoch_bits: AtomicU64,
    /// Whether any pending gauge sample succeeded.
    pending_seen: AtomicBool,
}

impl SampleStore {
    /// An empty store.
    fn new() -> Self {
        Self {
            rss_peak: AtomicU64::new(0),
            rss_last: AtomicU64::new(0),
            rss_seen: AtomicBool::new(false),
            pending_vote_bits: AtomicU64::new(0),
            pending_epoch_bits: AtomicU64::new(0),
            pending_seen: AtomicBool::new(false),
        }
    }

    /// Record one sample.
    fn record(&self, rss: Option<u64>, pending_vote: Option<f64>, pending_epoch: Option<f64>) {
        if let Some(bytes) = rss {
            self.rss_peak.fetch_max(bytes, Ordering::SeqCst);
            self.rss_last.store(bytes, Ordering::SeqCst);
            self.rss_seen.store(true, Ordering::SeqCst);
        }
        if let Some(value) = pending_vote {
            self.pending_vote_bits.fetch_max(value.max(0.0).to_bits(), Ordering::SeqCst);
            self.pending_seen.store(true, Ordering::SeqCst);
        }
        if let Some(value) = pending_epoch {
            self.pending_epoch_bits.fetch_max(value.max(0.0).to_bits(), Ordering::SeqCst);
        }
    }

    /// RSS peak and final value, or `None` when the host never reported one.
    fn rss(&self) -> RssSummary {
        let seen = self.rss_seen.load(Ordering::SeqCst);
        RssSummary {
            peak_bytes: seen.then(|| self.rss_peak.load(Ordering::SeqCst)),
            final_bytes: seen.then(|| self.rss_last.load(Ordering::SeqCst)),
        }
    }

    /// Sampled peaks of the pending gauges for `vote` and `epoch_record`.
    fn pending_peaks(&self) -> (Option<f64>, Option<f64>) {
        let seen = self.pending_seen.load(Ordering::SeqCst);
        (
            seen.then(|| f64::from_bits(self.pending_vote_bits.load(Ordering::SeqCst))),
            seen.then(|| f64::from_bits(self.pending_epoch_bits.load(Ordering::SeqCst))),
        )
    }
}

/// Process RSS summary.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct RssSummary {
    /// Highest sampled RSS in bytes.
    peak_bytes: Option<u64>,
    /// Last sampled RSS in bytes.
    final_bytes: Option<u64>,
}

/// Sample RSS and the pending gauges every `SAMPLE_INTERVAL` until aborted.
async fn sample_loop(source: Arc<MetricsSource>, store: Arc<SampleStore>) -> eyre::Result<()> {
    let mut ticks = tokio::time::interval(SAMPLE_INTERVAL);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stream::unfold(ticks, |mut ticks| async move {
        ticks.tick().await;
        Some(((), ticks))
    })
    .for_each(|()| {
        let source = source.clone();
        let store = store.clone();
        async move {
            store.record(
                resident_bytes(),
                source.class_gauge("tn_network.inbound_requests_pending", "vote"),
                source.class_gauge("tn_network.inbound_requests_pending", "epoch_record"),
            );
        }
    })
    .await;
    Ok(())
}

/// Honest in-flight peak and distribution for one target peer.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PeerPeak {
    /// Role index.
    peer: usize,
    /// Role name.
    role: String,
    /// Highest honest in-flight request count.
    peak: u32,
    /// In-flight count observed at each request start.
    distribution: Vec<HistogramBucket>,
}

/// Peak concurrent sync streams on one connection.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct ConnectionPeak {
    /// Peer that opened the streams.
    opener: usize,
    /// Peer that served them.
    target: usize,
    /// Highest concurrent open streams.
    peak: u32,
}

/// Served and failed counts of one request kind.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct ServiceCount {
    /// Requests answered with the expected payload in time.
    served: u64,
    /// Requests that failed or timed out.
    failed: u64,
}

/// What the hostile loaders observed.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
struct HostileSummary {
    /// Sync streams that opened and sent their request.
    streams_opened: u64,
    /// Sync stream opens that failed or timed out.
    streams_refused: u64,
    /// Requests answered.
    requests_served: u64,
    /// Requests that failed or timed out.
    requests_failed: u64,
}

/// Process-aggregated swarm metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SwarmMetricsSummary {
    /// Whether this test owned the process recorder.
    available: bool,
    /// Sampled peak of the `vote` pending gauge; last writer wins across peers.
    pending_vote_peak_sampled: Option<f64>,
    /// Sampled peak of the `epoch_record` pending gauge; last writer wins across peers.
    pending_epoch_record_peak_sampled: Option<f64>,
    /// Final `vote` shed counter, summed over reasons and peers.
    vote_shed_total: Option<u64>,
    /// Final `epoch_record` shed counter, summed over reasons and peers.
    epoch_record_shed_total: Option<u64>,
    /// Final `vote` failed counter, summed over outcomes and peers.
    vote_failed_total: Option<u64>,
    /// Final `epoch_record` failed counter, summed over outcomes and peers.
    epoch_record_failed_total: Option<u64>,
}

/// Everything one arm observed.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArmOutcome {
    /// Arm name.
    arm: String,
    /// Votes to the catching-up peer while catch-up ran.
    vote_sync_connection: LatencySummary,
    /// Votes to the catching-up peer after catch-up completed.
    vote_sync_connection_after_catch_up: LatencySummary,
    /// Votes to committee peer 1.
    vote_plain_connection: LatencySummary,
    /// Honest in-flight peak per target peer.
    in_flight_peak_per_peer: Vec<PeerPeak>,
    /// Peak sync streams per honest connection.
    sync_streams_peak_per_connection: Vec<ConnectionPeak>,
    /// Peak sync streams per hostile connection.
    hostile_streams_peak_per_connection: Vec<ConnectionPeak>,
    /// Established connections of the catching-up peer after catch-up.
    established_connections_catch_up_peer: u64,
    /// Configured receive credit per connection, in bytes.
    receive_credit_per_connection_bytes: u64,
    /// Configured receive credit times established connections of the catching-up peer.
    receive_credit_total_bytes: u64,
    /// Harness buffers at the peak: two frames per open sync stream on the catching-up peer.
    resident_harness_bytes_peak: u64,
    /// Process RSS.
    rss: RssSummary,
    /// Process-aggregated swarm metrics.
    swarm_metrics: SwarmMetricsSummary,
    /// Catch-up completion.
    catch_up: CatchUpSummary,
    /// Epoch record service.
    epoch_records: ServiceCount,
    /// Hostile observations.
    hostile: HostileSummary,
}

/// Run one arm: spawn the topology, load it, probe it, wait for catch-up, then tear it down.
async fn run_arm(
    arm: &str,
    workload: Workload,
    budget: Option<&NetworkProcessBudget>,
    hostile: Option<DerivedLimits>,
    source: &Arc<MetricsSource>,
) -> eyre::Result<ArmOutcome> {
    let setup_timeout = Duration::from_secs(60);
    let committee_size = NonZeroUsize::new(workload.peers())
        .ok_or_else(|| eyre::eyre!("committee size must be non-zero"))?;
    let fixture =
        CommitteeFixture::builder(MemDatabase::default).committee_size(committee_size).build();
    let task_manager = TaskManager::default();
    let mut peers = stream::iter(fixture.authorities().enumerate())
        .then(|(index, authority)| {
            spawn_peer(index, authority.consensus_config(), &task_manager, budget)
        })
        .try_collect::<Vec<HarnessPeer>>()
        .await?;
    let catch_up_index = workload.catch_up_index();
    peers.iter_mut().try_for_each(|peer| {
        if peer.index <= catch_up_index {
            spawn_responder(peer)
        } else {
            spawn_drain(peer)
        }
    })?;

    // Committee peers dial every higher committee peer; the catching-up and hostile peers dial
    // every committee peer. Each pair shares one connection in both directions.
    let committee = peers.iter().take(workload.committee).collect::<Vec<_>>();
    stream::iter(peers.iter())
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|from| {
            let committee = &committee;
            async move {
                stream::iter(committee.iter().filter(|to| {
                    from.index < workload.committee && to.index > from.index
                        || from.index >= workload.committee
                }))
                .map(Ok::<_, eyre::Report>)
                .try_for_each(|to| connect(&from.handle, to))
                .await
            }
        })
        .await?;
    stream::iter(peers.iter())
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|peer| async move {
            let expected = if peer.index < workload.committee {
                workload.peers() - 1
            } else {
                workload.committee
            };
            let handle = peer.handle.clone();
            wait_until(setup_timeout, "peer establishes its connections", move || {
                let handle = handle.clone();
                async move { Ok(handle.established_peer_count().await? >= expected) }
            })
            .await
        })
        .await?;

    let telemetry = Arc::new(Telemetry::new(workload.peers()));
    let store = Arc::new(SampleStore::new());
    let vote = TestPrimaryRequest::Vote { header: Header::default(), parents: Vec::new() };
    let vote_reply = answer(&vote);
    let refs = peers.iter().map(HarnessPeer::peer_ref).collect::<Vec<PeerRef>>();
    let honest = refs.iter().copied().take(workload.committee + 1).collect::<Vec<PeerRef>>();
    let committee_refs = refs.iter().copied().take(workload.committee).collect::<Vec<PeerRef>>();
    let catch_up_ref =
        *refs.get(catch_up_index).ok_or_else(|| eyre::eyre!("missing catching-up peer"))?;
    let plain_ref = *refs.get(1).ok_or_else(|| eyre::eyre!("missing committee peer 1"))?;
    let probe_handle =
        &peers.first().ok_or_else(|| eyre::eyre!("missing committee peer 0"))?.handle;

    let mut loaders: JoinSet<eyre::Result<()>> = JoinSet::new();
    loaders.spawn(sample_loop(source.clone(), store.clone()));
    // Vote loaders: every committee peer keeps votes in flight to every other honest peer. Peer 0
    // reaches its two probe targets through the probe itself.
    peers.iter().take(workload.committee).for_each(|from| {
        honest
            .iter()
            .filter(|to| to.index != from.index)
            .filter(|to| !(from.index == 0 && (to.index == catch_up_index || to.index == 1)))
            .for_each(|to| {
                (0..workload.votes_in_flight).for_each(|_| {
                    let handle = from.handle.clone();
                    let to = *to;
                    let telemetry = telemetry.clone();
                    let vote = vote.clone();
                    let vote_reply = vote_reply.clone();
                    loaders.spawn(async move {
                        stream::repeat_with(|| ())
                            .then(|()| {
                                honest_round_trip(
                                    &handle,
                                    to,
                                    &telemetry,
                                    &vote,
                                    &vote_reply,
                                    workload.request_timeout,
                                )
                            })
                            .try_for_each(|_| future::ready(Ok(())))
                            .await
                    });
                });
            });
    });
    // Epoch record tickers: every committee peer asks every other honest peer each interval.
    peers.iter().take(workload.committee).for_each(|from| {
        let handle = from.handle.clone();
        let telemetry = telemetry.clone();
        let targets =
            honest.iter().copied().filter(|to| to.index != from.index).collect::<Vec<PeerRef>>();
        loaders.spawn(async move {
            let mut ticks = tokio::time::interval(workload.epoch_record_interval);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            stream::unfold((ticks, 0_u64), |(mut ticks, epoch)| async move {
                ticks.tick().await;
                Some((epoch, (ticks, epoch.saturating_add(1))))
            })
            .for_each(|epoch| {
                let handle = &handle;
                let telemetry = &telemetry;
                let targets = &targets;
                async move {
                    future::join_all(targets.iter().map(|to| async move {
                        let request = TestPrimaryRequest::EpochRecord { epoch };
                        let expected = TestPrimaryResponse::EpochRecord(epoch);
                        let outcome = honest_round_trip(
                            handle,
                            *to,
                            telemetry,
                            &request,
                            &expected,
                            workload.request_timeout,
                        )
                        .await;
                        Telemetry::count(
                            &telemetry.epoch_records_served,
                            &telemetry.epoch_records_failed,
                            &outcome,
                        );
                    }))
                    .await;
                }
            })
            .await;
            Ok(())
        });
    });
    // Hostile loaders, regression arm only: hold streams past the ceiling and flood requests.
    if let Some(limits) = hostile {
        let hold_request = CatchUpRequest {
            frames: workload.frames_per_exchange,
            frame_bytes: u32::try_from(workload.frame_bytes)?,
        };
        peers.iter().skip(catch_up_index + 1).for_each(|from| {
            committee_refs.iter().for_each(|to| {
                let to = *to;
                (0..limits.streams_per_connection.saturating_mul(2)).for_each(|_| {
                    let handle = from.handle.clone();
                    let telemetry = telemetry.clone();
                    loaders.spawn(async move {
                        hold_stream(&handle, to.bls, &telemetry, hold_request).await;
                        Ok(())
                    });
                });
                (0..limits.in_flight_per_peer.saturating_mul(2)).for_each(|_| {
                    let handle = from.handle.clone();
                    let telemetry = telemetry.clone();
                    let vote = vote.clone();
                    let vote_reply = vote_reply.clone();
                    loaders.spawn(async move {
                        stream::repeat_with(|| ())
                            .for_each(|()| async {
                                let outcome = round_trip(
                                    &handle,
                                    to.bls,
                                    &vote,
                                    &vote_reply,
                                    workload.request_timeout,
                                )
                                .await;
                                Telemetry::count(
                                    &telemetry.hostile_requests_served,
                                    &telemetry.hostile_requests_failed,
                                    &outcome,
                                );
                            })
                            .await;
                        Ok(())
                    });
                });
            });
        });
    }

    // Catch-up starts with the loaders already running.
    let catch_up_handle = peers
        .get(catch_up_index)
        .ok_or_else(|| eyre::eyre!("missing catching-up peer"))?
        .handle
        .clone();
    let mut catch_up_task: JoinSet<eyre::Result<CatchUpSummary>> = JoinSet::new();
    catch_up_task.spawn(catch_up(
        catch_up_handle,
        catch_up_index,
        committee_refs.clone(),
        telemetry.clone(),
        workload,
    ));

    let probe = Probe {
        measured: probe_handle,
        sync_target: catch_up_ref,
        plain_target: plain_ref,
        telemetry: &telemetry,
        request: &vote,
        expected: &vote_reply,
        max_wait: workload.request_timeout,
    };
    let measure = async {
        probe.window(workload.warm_up, true).await?;
        stream::iter(0..workload.repetitions)
            .map(Ok::<_, eyre::Report>)
            .and_then(|repetition| probe.window(workload.sample_window, repetition % 2 == 0))
            .try_concat()
            .await
    };
    let pairs = tokio::select! {
        result = measure => result,
        stopped = loaders.join_next(), if !loaders.is_empty() => {
            stopped
                .ok_or_else(|| eyre::eyre!("missing load task"))
                .and_then(|joined| joined.map_err(eyre::Report::from))
                .and_then(|loaded| loaded)
                .and_then(|()| Err(eyre::eyre!("load task stopped before sampling finished")))
        }
    };
    let catch_up_summary = timeout(CATCH_UP_TIMEOUT, catch_up_task.join_next())
        .await
        .map_err(|_| eyre::eyre!("catch-up did not complete within {CATCH_UP_TIMEOUT:?}"))
        .and_then(|joined| joined.ok_or_else(|| eyre::eyre!("missing catch-up task")))
        .and_then(|joined| joined.map_err(eyre::Report::from))
        .and_then(|outcome| outcome);
    let established = peers
        .get(catch_up_index)
        .ok_or_else(|| eyre::eyre!("missing catching-up peer"))?
        .handle
        .established_peer_count()
        .await;
    loaders.shutdown().await;
    let drained = stream::iter(peers.iter().take(workload.committee + 1))
        .map(Ok::<_, eyre::Report>)
        .try_for_each(|peer| async move {
            wait_until(workload.request_timeout, "arm pending requests drain", || async {
                Ok(peer.handle.get_pending_request_count().await? == 0)
            })
            .await
        })
        .await;
    let quic_credit = peers
        .get(catch_up_index)
        .map(|peer| u64::from(peer.config.network_config().quic_config().max_connection_data))
        .unwrap_or(0);
    stream::iter(peers.iter_mut()).for_each(|peer| async { peer.tasks.shutdown().await }).await;

    // Preserve a sampling or load failure even if the cleanup also fails.
    let pairs = pairs?;
    let catch_up_summary = catch_up_summary?;
    let established = u64::try_from(established?)?;
    drained?;

    let during = pairs
        .iter()
        .filter(|(sync, _)| sync.during_catch_up)
        .map(|(sync, _)| sync.rtt)
        .collect::<Vec<Duration>>();
    let after = pairs
        .iter()
        .filter(|(sync, _)| !sync.during_catch_up)
        .map(|(sync, _)| sync.rtt)
        .collect::<Vec<Duration>>();
    let plain = pairs.iter().map(|(_, plain)| plain.rtt).collect::<Vec<Duration>>();
    let role = |index: usize| {
        if index < workload.committee {
            format!("committee-{index}")
        } else if index == catch_up_index {
            "catch-up".to_owned()
        } else {
            format!("hostile-{}", index.saturating_sub(catch_up_index + 1))
        }
    };
    let in_flight_peak_per_peer = telemetry
        .in_flight
        .iter()
        .zip(telemetry.histograms.iter())
        .enumerate()
        .take(workload.committee + 1)
        .map(|(peer, (gauge, histogram))| PeerPeak {
            peer,
            role: role(peer),
            peak: gauge.peak(),
            distribution: histogram.buckets(),
        })
        .collect::<Vec<PeerPeak>>();
    let connection_peaks = |opener: usize| {
        committee_refs
            .iter()
            .filter_map(|to| {
                telemetry.connection(opener, to.index).ok().map(|gauge| ConnectionPeak {
                    opener,
                    target: to.index,
                    peak: gauge.peak(),
                })
            })
            .collect::<Vec<ConnectionPeak>>()
    };
    let sync_streams_peak_per_connection = connection_peaks(catch_up_index);
    let hostile_streams_peak_per_connection = (catch_up_index + 1..workload.peers())
        .flat_map(connection_peaks)
        .collect::<Vec<ConnectionPeak>>();
    let peak_streams_catch_up = sync_streams_peak_per_connection
        .iter()
        .fold(0_u64, |total, peak| total.saturating_add(u64::from(peak.peak)));
    let receive_credit_per_connection_bytes = budget
        .and_then(|budget| budget.allocate().ok())
        .map(|allocated| u64::from(allocated.receive_credit_per_connection()).min(quic_credit))
        .unwrap_or(quic_credit);
    let (pending_vote, pending_epoch) = store.pending_peaks();
    Ok(ArmOutcome {
        arm: arm.to_owned(),
        vote_sync_connection: summarize("vote p0 -> catch-up (sync connection)", &during),
        vote_sync_connection_after_catch_up: summarize(
            "vote p0 -> catch-up after catch-up",
            &after,
        ),
        vote_plain_connection: summarize("vote p0 -> committee-1 (plain connection)", &plain),
        in_flight_peak_per_peer,
        sync_streams_peak_per_connection,
        hostile_streams_peak_per_connection,
        established_connections_catch_up_peer: established,
        receive_credit_per_connection_bytes,
        receive_credit_total_bytes: receive_credit_per_connection_bytes.saturating_mul(established),
        resident_harness_bytes_peak: peak_streams_catch_up
            .saturating_mul(2)
            .saturating_mul(u64::try_from(workload.frame_bytes).unwrap_or(u64::MAX)),
        rss: store.rss(),
        swarm_metrics: SwarmMetricsSummary {
            available: source.snapshotter.is_some(),
            pending_vote_peak_sampled: pending_vote,
            pending_epoch_record_peak_sampled: pending_epoch,
            vote_shed_total: source.class_counter("tn_network.inbound_requests_shed_total", "vote"),
            epoch_record_shed_total: source
                .class_counter("tn_network.inbound_requests_shed_total", "epoch_record"),
            vote_failed_total: source
                .class_counter("tn_network.inbound_requests_failed_total", "vote"),
            epoch_record_failed_total: source
                .class_counter("tn_network.inbound_requests_failed_total", "epoch_record"),
        },
        catch_up: catch_up_summary,
        epoch_records: ServiceCount {
            served: telemetry.epoch_records_served.load(Ordering::SeqCst),
            failed: telemetry.epoch_records_failed.load(Ordering::SeqCst),
        },
        hostile: HostileSummary {
            streams_opened: telemetry.hostile_streams_opened.load(Ordering::SeqCst),
            streams_refused: telemetry.hostile_streams_refused.load(Ordering::SeqCst),
            requests_served: telemetry.hostile_requests_served.load(Ordering::SeqCst),
            requests_failed: telemetry.hostile_requests_failed.load(Ordering::SeqCst),
        },
    })
}

/// Per-peer limits derived from the baseline peaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DerivedLimits {
    /// Incoming streams per connection: sync peak with headroom, plus the protocol allowance,
    /// plus the in-flight request limit.
    streams_per_connection: u32,
    /// Receive credit per connection in bytes.
    receive_credit_per_connection: u64,
    /// In-flight requests per peer. No config field exists yet; recorded as a proposed constant.
    in_flight_per_peer: u32,
    /// Established connections per peer. Unchanged.
    connections_per_peer: u32,
}

/// `ceil(peak * 1.5)`, at least 1.
fn headroom(peak: u64) -> u64 {
    peak.saturating_mul(HEADROOM_PERCENT).div_ceil(100).max(1)
}

/// Apply the derivation rule to the baseline peaks.
fn derive_limits(baseline: &ArmOutcome, frame_bytes: u64) -> DerivedLimits {
    let peak_in_flight =
        baseline.in_flight_peak_per_peer.iter().map(|peak| u64::from(peak.peak)).max().unwrap_or(0);
    let peak_streams = baseline
        .sync_streams_peak_per_connection
        .iter()
        .map(|peak| u64::from(peak.peak))
        .max()
        .unwrap_or(0);
    let in_flight_per_peer = u32::try_from(headroom(peak_in_flight)).unwrap_or(u32::MAX);
    let streams_per_connection = u32::try_from(headroom(peak_streams))
        .unwrap_or(u32::MAX)
        .saturating_add(PROTOCOL_STREAM_ALLOWANCE)
        .saturating_add(in_flight_per_peer);
    let receive_credit_per_connection = headroom(baseline.resident_harness_bytes_peak)
        .max(u64::from(streams_per_connection).saturating_mul(2).saturating_mul(frame_bytes))
        .max(MIN_RECEIVE_CREDIT);
    DerivedLimits {
        streams_per_connection,
        receive_credit_per_connection,
        in_flight_per_peer,
        connections_per_peer: MAX_ESTABLISHED_CONNECTIONS_PER_PEER,
    }
}

/// The `process_budget` that allocates exactly `limits` to every connection of a one-swarm peer.
fn budget_for(limits: DerivedLimits, workload: Workload) -> eyre::Result<NetworkProcessBudget> {
    let connections = u32::try_from(workload.peers())?.saturating_mul(2);
    serde_json::from_value(serde_json::json!({
        "swarm_count": 1,
        "max_established_connections": connections,
        "max_established_connections_per_peer": limits.connections_per_peer,
        "max_inbound_streams": u64::from(limits.streams_per_connection).saturating_mul(u64::from(connections)),
        "max_receive_credit_bytes": limits.receive_credit_per_connection.saturating_mul(u64::from(connections)),
    }))
    .map_err(Into::into)
}

/// Pass, fail or unknown for one declared bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Verdict {
    /// The observation is inside the bound.
    Pass,
    /// The observation is outside the bound.
    Fail,
    /// The observation is missing or has too few samples.
    Unknown,
    /// The bound does not apply to this arm.
    NotApplicable,
}

/// One declared bound with its result.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Threshold {
    /// Bound name.
    name: String,
    /// Declared value.
    bound: f64,
    /// Who declared it.
    owner: String,
    /// When it was declared.
    date: String,
    /// Review status of the bound.
    status: String,
    /// Observed value, when known.
    observed: Option<f64>,
    /// Result.
    result: Verdict,
}

/// The declared bounds, with their environment overrides.
#[derive(Clone, Copy, Debug)]
struct Bounds {
    /// Vote p99 on the sync-carrying connection, in milliseconds.
    vote_p99_ms: f64,
    /// Catch-up completion factor against the baseline.
    catch_up_factor: f64,
}

impl Bounds {
    /// Defaults, overridden by `TN_M3_VOTE_P99_MS` and `TN_M3_CATCH_UP_FACTOR`.
    fn from_env() -> Self {
        let parse = |name: &str, default: f64| {
            std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
        };
        Self {
            vote_p99_ms: parse("TN_M3_VOTE_P99_MS", DEFAULT_VOTE_P99_MS),
            catch_up_factor: parse("TN_M3_CATCH_UP_FACTOR", DEFAULT_CATCH_UP_FACTOR),
        }
    }
}

/// Build one threshold row; `None` observed is `Unknown`.
fn threshold(name: &str, bound: f64, observed: Option<f64>, within: bool) -> Threshold {
    let result =
        observed.map_or(Verdict::Unknown, |_| if within { Verdict::Pass } else { Verdict::Fail });
    Threshold {
        name: name.to_owned(),
        bound,
        owner: BOUND_OWNER.to_owned(),
        date: BOUND_DATE.to_owned(),
        status: "proposed; accepted when the docs page merges".to_owned(),
        observed,
        result,
    }
}

/// Evaluate the declared bounds for one arm.
fn evaluate(
    outcome: &ArmOutcome,
    bounds: Bounds,
    baseline_catch_up: Option<Duration>,
    limits: Option<DerivedLimits>,
) -> Vec<Threshold> {
    let p99 = outcome.vote_sync_connection.p99_ms;
    let vote = threshold(
        "vote_p99_sync_connection_ms",
        bounds.vote_p99_ms,
        p99,
        p99.is_some_and(|value| value <= bounds.vote_p99_ms),
    );
    let total = outcome.epoch_records.served.saturating_add(outcome.epoch_records.failed);
    let served_percent = (total > 0).then(|| {
        u32::try_from(outcome.epoch_records.served.saturating_mul(100) / total)
            .map(f64::from)
            .unwrap_or(0.0)
    });
    let epoch = threshold(
        "epoch_record_service_percent",
        100.0,
        served_percent,
        outcome.epoch_records.failed == 0 && outcome.epoch_records.served > 0,
    );
    let factor = baseline_catch_up.map(|baseline| {
        outcome.catch_up.completion.as_secs_f64() / baseline.as_secs_f64().max(f64::EPSILON)
    });
    let mut catch_up = threshold(
        "catch_up_completion_factor",
        bounds.catch_up_factor,
        factor,
        factor.is_some_and(|value| value <= bounds.catch_up_factor),
    );
    if baseline_catch_up.is_none() {
        catch_up.result = Verdict::NotApplicable;
    }
    let honest_peak =
        outcome.sync_streams_peak_per_connection.iter().map(|peak| peak.peak).max().unwrap_or(0);
    let mut streams = threshold(
        "honest_sync_streams_within_derived_ceiling",
        limits.map_or(0.0, |limits| f64::from(limits.streams_per_connection)),
        Some(f64::from(honest_peak)),
        limits.is_some_and(|limits| honest_peak <= limits.streams_per_connection),
    );
    if limits.is_none() {
        streams.result = Verdict::NotApplicable;
    }
    vec![vote, epoch, catch_up, streams]
}

/// The evidence record of one arm, field for field as issue #1742 lists it.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EvidenceRecord {
    /// Arm name.
    arm: String,
    /// `TN_NODE_REVISION`, else `git rev-parse HEAD`, else `null`.
    node_revision: Option<String>,
    /// `git hash-object Cargo.lock`, else `null`.
    cargo_lock_hash: Option<String>,
    /// Last commit that touched `patches/libp2p-quic`, else `null`.
    quic_patch_revision: Option<String>,
    /// Build features that matter.
    features: Vec<String>,
    /// Harness configuration.
    configuration: Configuration,
    /// Topology.
    topology: Topology,
    /// Host.
    host: Host,
    /// Metric definitions, sampling, and results.
    metrics: MetricsRecord,
    /// Declared bounds with owner, date and result.
    thresholds: Vec<Threshold>,
    /// Open limitations.
    limitations: Vec<String>,
}

/// Harness configuration of one arm.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Configuration {
    /// The shared workload.
    workload: Workload,
    /// The applied process budget, regression arm only.
    process_budget: Option<NetworkProcessBudget>,
    /// The derived limits, regression arm only.
    derived_limits: Option<DerivedLimits>,
    /// Derivation rule.
    derivation_rule: String,
}

/// Topology of one arm.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Topology {
    /// Roles by index.
    roles: Vec<String>,
    /// Swarms per peer.
    swarms_per_peer: u32,
    /// Workers per peer.
    workers_per_peer: u32,
    /// Honest peers.
    honest_peers: usize,
    /// Hostile identities loaded.
    hostile_peers_loaded: usize,
    /// Shared addresses.
    shared_addresses: String,
    /// Admission policy state.
    admission_policy: String,
}

/// Host facts.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Host {
    /// Operating system.
    os: String,
    /// CPU architecture.
    arch: String,
    /// Available parallelism.
    available_parallelism: Option<usize>,
    /// Network path.
    network: String,
    /// Firewall.
    firewall: String,
    /// Traffic generator limits.
    generator_limits: String,
}

/// Metric definitions, sampling and results.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MetricsRecord {
    /// Definitions.
    definitions: Vec<String>,
    /// RSS and gauge sampling interval.
    sampling_interval: Duration,
    /// Sample windows.
    trial_count: usize,
    /// Baseline identity.
    baseline: String,
    /// Instrumentation overhead statement.
    instrumentation_overhead: String,
    /// Results.
    outcome: ArmOutcome,
}

/// Workspace root, two levels above this crate.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

/// Trimmed stdout of a successful git command at the workspace root, else `None`.
fn git_output(args: &[&str]) -> Option<String> {
    Command::new("git")
        .arg("-C")
        .arg(workspace_root())
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
}

/// Assemble the evidence record of one arm.
fn evidence(
    workload: Workload,
    outcome: &ArmOutcome,
    budget: Option<&NetworkProcessBudget>,
    limits: Option<DerivedLimits>,
    thresholds: Vec<Threshold>,
    baseline: &str,
) -> EvidenceRecord {
    let catch_up_index = workload.catch_up_index();
    let roles = (0..workload.peers())
        .map(|index| {
            if index < workload.committee {
                format!("{index}: committee")
            } else if index == catch_up_index {
                format!("{index}: catch-up")
            } else {
                format!("{index}: hostile")
            }
        })
        .collect();
    EvidenceRecord {
        arm: outcome.arm.clone(),
        node_revision: std::env::var("TN_NODE_REVISION")
            .ok()
            .or_else(|| git_output(&["rev-parse", "HEAD"])),
        cargo_lock_hash: git_output(&["hash-object", "Cargo.lock"]),
        quic_patch_revision: git_output(&["log", "-1", "--format=%H", "--", "patches/libp2p-quic"]),
        features: vec![
            format!("debug_assertions={}", cfg!(debug_assertions)),
            format!("target_os={}", std::env::consts::OS),
            format!("target_arch={}", std::env::consts::ARCH),
            "crate test configuration".to_owned(),
        ],
        configuration: Configuration {
            workload,
            process_budget: budget.cloned(),
            derived_limits: limits,
            derivation_rule: format!(
                "limit = ceil(peak * {HEADROOM_PERCENT} / 100), at least 1; streams add \
                 {PROTOCOL_STREAM_ALLOWANCE} protocol streams and the in-flight limit; credit is \
                 at least two frames per stream and {MIN_RECEIVE_CREDIT} bytes"
            ),
        },
        topology: Topology {
            roles,
            swarms_per_peer: 1,
            workers_per_peer: 0,
            honest_peers: workload.committee + 1,
            hostile_peers_loaded: if limits.is_some() { workload.hostile } else { 0 },
            shared_addresses: "all peers on loopback, one UDP port each".to_owned(),
            admission_policy: "test defaults; every peer is a committee identity".to_owned(),
        },
        host: Host {
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            available_parallelism: std::thread::available_parallelism().ok().map(NonZeroUsize::get),
            network: "loopback, no NIC".to_owned(),
            firewall: "none".to_owned(),
            generator_limits: "cooperative in-process tasks, no packet generator".to_owned(),
        },
        metrics: MetricsRecord {
            definitions: vec![
                "vote latency: send_request to validated response, nearest-rank percentiles"
                    .to_owned(),
                "in-flight per peer: honest requests outstanding to the target, all requesters"
                    .to_owned(),
                "sync streams per connection: open harness sync streams on one connection"
                    .to_owned(),
                "receive credit: configured quic max_connection_data or budget allocation"
                    .to_owned(),
                "resident harness bytes: two frames per open sync stream on the catching-up peer"
                    .to_owned(),
                "rss: process resident set, Linux /proc or macOS ps".to_owned(),
                "swarm metrics: tn_network.inbound_requests_* aggregated over every peer"
                    .to_owned(),
            ],
            sampling_interval: SAMPLE_INTERVAL,
            trial_count: workload.repetitions,
            baseline: baseline.to_owned(),
            instrumentation_overhead: "atomic counters per request and stream; RSS sample runs \
                                       ps on macOS every 100 ms; not separately measured"
                .to_owned(),
            outcome: outcome.clone(),
        },
        thresholds,
        limitations: vec![
            "loopback only; no NIC, firewall or generator limits".to_owned(),
            "one swarm per peer; repeat per swarm role".to_owned(),
            "the harness serves sync streams; node responder limits are not exercised".to_owned(),
            "RSS is the whole test process, every peer included".to_owned(),
            "p99 needs 100 samples and p99.9 needs 1000; null otherwise".to_owned(),
            "hostile loaders are cooperative tasks with committee identities".to_owned(),
            "swarm protocol streams are not counted; the stream allowance is declared".to_owned(),
            "pending gauges are shared by every peer; last writer wins".to_owned(),
            "a lower constant without a passing regression run closes nothing".to_owned(),
        ],
    }
}

/// Write one evidence record and return its path.
fn write_evidence(record: &EvidenceRecord) -> eyre::Result<PathBuf> {
    let dir = std::env::var_os("TN_M3_EVIDENCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target").join("m3-evidence"));
    std::fs::create_dir_all(&dir)?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let path = dir.join(format!("{}-{stamp}.json", record.arm));
    std::fs::write(&path, serde_json::to_vec_pretty(record)?)?;
    Ok(path)
}

/// Print one arm in the admission harness style.
fn report(outcome: &ArmOutcome, thresholds: &[Threshold]) {
    let line = |summary: &LatencySummary| {
        format!(
            "{:<44} n={:>5} p50={:?} p99={:?} p99.9={:?} max={:?}",
            summary.label,
            summary.samples,
            summary.p50_ms,
            summary.p99_ms,
            summary.p999_ms,
            summary.max_ms
        )
    };
    println!("[catch-up-harness] arm={}", outcome.arm);
    println!("[catch-up-harness] {}", line(&outcome.vote_sync_connection));
    println!("[catch-up-harness] {}", line(&outcome.vote_sync_connection_after_catch_up));
    println!("[catch-up-harness] {}", line(&outcome.vote_plain_connection));
    outcome.in_flight_peak_per_peer.iter().for_each(|peak| {
        println!("[catch-up-harness] in-flight peak {:<12} = {}", peak.role, peak.peak);
    });
    outcome.sync_streams_peak_per_connection.iter().for_each(|peak| {
        println!(
            "[catch-up-harness] sync streams peak {} -> {} = {}",
            peak.opener, peak.target, peak.peak
        );
    });
    println!(
        "[catch-up-harness] credit/connection={} credit total={} resident harness={} rss={:?}",
        outcome.receive_credit_per_connection_bytes,
        outcome.receive_credit_total_bytes,
        outcome.resident_harness_bytes_peak,
        outcome.rss
    );
    println!(
        "[catch-up-harness] catch-up {:?} bytes={} epoch records served={} failed={} hostile={:?}",
        outcome.catch_up.completion,
        outcome.catch_up.bytes,
        outcome.epoch_records.served,
        outcome.epoch_records.failed,
        outcome.hostile
    );
    thresholds.iter().for_each(|threshold| {
        println!(
            "[catch-up-harness] bound {:<44} bound={} observed={:?} result={:?}",
            threshold.name, threshold.bound, threshold.observed, threshold.result
        );
    });
}

/// Run both arms, write both evidence records and return the regression thresholds.
async fn run_both_arms(
    workload: Workload,
) -> eyre::Result<(Vec<PathBuf>, Vec<Threshold>, DerivedLimits)> {
    let source = Arc::new(MetricsSource::install());
    let bounds = Bounds::from_env();
    let baseline = run_arm("baseline", workload, None, None, &source).await?;
    let baseline_thresholds = evaluate(&baseline, bounds, None, None);
    report(&baseline, &baseline_thresholds);
    let baseline_path = write_evidence(&evidence(
        workload,
        &baseline,
        None,
        None,
        baseline_thresholds,
        "this run",
    ))?;
    let limits = derive_limits(&baseline, u64::try_from(workload.frame_bytes)?);
    let budget = budget_for(limits, workload)?;
    let regression = run_arm("regression", workload, Some(&budget), Some(limits), &source).await?;
    let thresholds =
        evaluate(&regression, bounds, Some(baseline.catch_up.completion), Some(limits));
    report(&regression, &thresholds);
    let regression_path = write_evidence(&evidence(
        workload,
        &regression,
        Some(&budget),
        Some(limits),
        thresholds.clone(),
        &baseline_path.display().to_string(),
    ))?;
    Ok((vec![baseline_path, regression_path], thresholds, limits))
}

/// Nearest rank over tenths of a percent, with `None` for empty input and invalid ranks.
#[test]
fn nearest_rank_percentiles() {
    let samples = (1..=40).map(Duration::from_millis).collect::<Vec<_>>();
    assert_eq!(percentile_ms(&samples, 500), Some(20.0));
    assert_eq!(percentile_ms(&samples, 990), Some(40.0));
    assert_eq!(percentile_ms(&samples, 1000), Some(40.0));
    assert_eq!(percentile_ms(&[], 500), None);
    assert_eq!(percentile_ms(&samples, 0), None);
    assert_eq!(percentile_ms(&samples, 1001), None);
    let summary = summarize("short", &samples);
    assert_eq!(summary.p99_ms, None);
    assert_eq!(summary.p999_ms, None);
}

/// The derivation rule rounds up, floors at one and adds the declared allowances.
#[test]
fn derivation_rule_adds_headroom_and_allowances() {
    assert_eq!(headroom(0), 1);
    assert_eq!(headroom(1), 2);
    assert_eq!(headroom(2), 3);
    assert_eq!(headroom(4), 6);
    let outcome = ArmOutcome {
        arm: "baseline".to_owned(),
        vote_sync_connection: summarize("a", &[]),
        vote_sync_connection_after_catch_up: summarize("b", &[]),
        vote_plain_connection: summarize("c", &[]),
        in_flight_peak_per_peer: vec![PeerPeak {
            peer: 0,
            role: "committee-0".to_owned(),
            peak: 4,
            distribution: Vec::new(),
        }],
        sync_streams_peak_per_connection: vec![ConnectionPeak { opener: 4, target: 0, peak: 4 }],
        hostile_streams_peak_per_connection: Vec::new(),
        established_connections_catch_up_peer: 4,
        receive_credit_per_connection_bytes: 0,
        receive_credit_total_bytes: 0,
        resident_harness_bytes_peak: 2 * 4 * 65_536,
        rss: RssSummary { peak_bytes: None, final_bytes: None },
        swarm_metrics: SwarmMetricsSummary {
            available: false,
            pending_vote_peak_sampled: None,
            pending_epoch_record_peak_sampled: None,
            vote_shed_total: None,
            epoch_record_shed_total: None,
            vote_failed_total: None,
            epoch_record_failed_total: None,
        },
        catch_up: CatchUpSummary { completion: Duration::from_secs(1), bytes: 0, exchanges: 0 },
        epoch_records: ServiceCount { served: 0, failed: 0 },
        hostile: HostileSummary {
            streams_opened: 0,
            streams_refused: 0,
            requests_served: 0,
            requests_failed: 0,
        },
    };
    let limits = derive_limits(&outcome, 65_536);
    assert_eq!(limits.in_flight_per_peer, 6);
    assert_eq!(limits.streams_per_connection, 6 + PROTOCOL_STREAM_ALLOWANCE + 6);
    assert_eq!(limits.receive_credit_per_connection, 20 * 2 * 65_536);
    assert_eq!(limits.connections_per_peer, MAX_ESTABLISHED_CONNECTIONS_PER_PEER);
}

/// RSS parsers read the kibibyte figure and refuse malformed text.
#[test]
fn rss_parsers_read_kibibytes() {
    assert_eq!(parse_vm_rss_kib("VmPeak:\t  10 kB\nVmRSS:\t   1234 kB\n"), Some(1234));
    assert_eq!(parse_vm_rss_kib("VmSize: 10 kB\n"), None);
    assert_eq!(parse_vm_rss_kib("VmRSS: lots kB\n"), None);
    assert_eq!(parse_ps_rss_kib("  5678\n"), Some(5678));
    assert_eq!(parse_ps_rss_kib("\n"), None);
}

/// The budget allocates exactly the derived per-connection limits to a one-swarm peer.
#[test]
fn budget_allocates_derived_limits() -> eyre::Result<()> {
    let limits = DerivedLimits {
        streams_per_connection: 20,
        receive_credit_per_connection: 4 * 1024 * 1024,
        in_flight_per_peer: 6,
        connections_per_peer: 8,
    };
    let allocated = budget_for(limits, Workload::SMOKE)?.allocate()?;
    assert_eq!(allocated.streams_per_connection(), 20);
    assert_eq!(u64::from(allocated.receive_credit_per_connection()), 4 * 1024 * 1024);
    assert_eq!(allocated.connections_per_peer(), 8);
    Ok(())
}

/// The same code path at a small size: structure only, no latency assertions.
#[tokio::test(flavor = "multi_thread")]
async fn catch_up_contention_smoke() -> eyre::Result<()> {
    let (paths, thresholds, limits) = run_both_arms(Workload::SMOKE).await?;
    assert_eq!(paths.len(), 2);
    let records = paths
        .iter()
        .map(|path| {
            std::fs::read(path).map_err(eyre::Report::from).and_then(|bytes| {
                serde_json::from_slice::<EvidenceRecord>(&bytes).map_err(Into::into)
            })
        })
        .collect::<eyre::Result<Vec<EvidenceRecord>>>()?;
    records.iter().for_each(|record| {
        assert_eq!(
            record.metrics.outcome.catch_up.bytes,
            Workload::SMOKE.expected_catch_up_bytes()
        );
        assert!(record.metrics.outcome.epoch_records.served > 0);
        assert!(record.metrics.outcome.vote_plain_connection.samples > 0);
    });
    assert!(limits.streams_per_connection >= 1);
    assert!(limits.in_flight_per_peer >= 1);
    assert!(limits.receive_credit_per_connection >= MIN_RECEIVE_CREDIT);
    assert_eq!(thresholds.len(), 4);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement harness for issue #1742; run manually on a quiet, provisioned host with \
            --ignored --nocapture"]
/// Baseline, derivation, regression, two evidence records, then the declared bounds.
async fn catch_up_contention_report() -> eyre::Result<()> {
    let (paths, thresholds, limits) = run_both_arms(Workload::REPORT).await?;
    println!("[catch-up-harness] evidence={paths:?} derived={limits:?}");
    let failed = thresholds
        .iter()
        .filter(|threshold| !matches!(threshold.result, Verdict::Pass | Verdict::NotApplicable))
        .map(|threshold| {
            format!(
                "{} ({:?}, observed {:?})",
                threshold.name, threshold.result, threshold.observed
            )
        })
        .collect::<Vec<String>>();
    eyre::ensure!(failed.is_empty(), "regression bounds not met: {}", failed.join(", "));
    Ok(())
}
