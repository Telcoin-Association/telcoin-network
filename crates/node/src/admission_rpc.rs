//! Admission snapshots of the node's swarms for the `tn_admissionSnapshot` RPC method.
//!
//! A swarm answers the snapshot command on the same bounded command channel that carries its
//! certificate and batch publishes, so the RPC path must not be able to load that channel.
//! [`AdmissionSnapshotSource`] runs one fetch at a time, serves the result for `SNAPSHOT_TTL`,
//! starts fetches at least `SNAPSHOT_TTL` apart and bounds each swarm's answer by
//! `SWARM_SNAPSHOT_TIMEOUT`.

use eyre::eyre;
use futures::future::join_all;
use std::{
    future::Future,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tn_config::AdmissionMode;
use tn_network_libp2p::{
    types::{NetworkHandle, NetworkResult},
    AdmissionFallback, AdmissionStatus, EntrySource, SwarmAdmissionSnapshot, TrackedEntry,
    TrackedSlot,
};
use tn_primary::network::{PrimaryRequest, PrimaryResponse};
use tn_rpc::{
    RpcAdmissionEntry, RpcAdmissionFallback, RpcAdmissionMode, RpcAdmissionSlot,
    RpcAdmissionSnapshot, RpcAdmissionStatus, RpcEntrySource, RpcSwarmAdmission,
};
use tn_types::{Bytes, WorkerId};
use tn_worker::{WorkerRequest, WorkerResponse};
use tokio::{sync::Mutex, time::Instant};
use tracing::warn;

/// How long a fetched snapshot is served, and the least time between two fetch starts.
const SNAPSHOT_TTL: Duration = Duration::from_secs(1);

/// Bound on one swarm's answer, from queueing the snapshot command to receiving its reply.
const SWARM_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(2);

/// The primary swarm's metrics label.
const PRIMARY_NETWORK: &str = "primary";

/// Source of the node's [`RpcAdmissionSnapshot`], shared by every RPC server the node binds.
///
/// It holds nothing until [`Self::set`] installs the swarm handles after the networks spawn, and
/// [`Self::snapshot`] returns `None` until then. Clones share the handles and the cached snapshot,
/// so an RPC server built before the networks start serves snapshots once they are set.
///
/// # Command channel load
///
/// The fetch state mutex is the source's only lock, and only [`Self::snapshot`] takes it. The
/// caller that finds no fresh snapshot holds it for the whole fetch, so concurrent callers wait
/// for that fetch and serve its result instead of sending commands of their own. A fetch sends
/// each swarm one snapshot command and waits at most 2 seconds for each answer. Fetch starts are
/// at least 1 second apart, also when a caller is cancelled mid-fetch. No network path takes the
/// lock, so a publish never waits on it, and the source adds at most one command per swarm per
/// second to the channel it shares with publishes.
#[derive(Clone, Debug, Default)]
pub struct AdmissionSnapshotSource {
    /// Handles of every swarm, installed once when the networks spawn.
    handles: Arc<OnceLock<SwarmHandles>>,
    /// Fetch spacing and the latest snapshot.
    ///
    /// A tokio mutex because the fetching caller holds it across the swarm requests.
    fetch: Arc<Mutex<FetchState>>,
}

impl AdmissionSnapshotSource {
    /// Install the handles of the primary swarm and of every configured worker swarm.
    ///
    /// Worker swarms outside the epoch's active worker range belong here too: they listen for the
    /// whole process, so a firewall built from the snapshot must keep their peers reachable.
    /// Workers are reported in worker id order.
    ///
    /// # Errors
    ///
    /// Returns an error, and keeps the installed handles, when the handles are already set.
    pub fn set(
        &self,
        primary: NetworkHandle<PrimaryRequest, PrimaryResponse>,
        mut workers: Vec<(WorkerId, NetworkHandle<WorkerRequest, WorkerResponse>)>,
    ) -> eyre::Result<()> {
        workers.sort_by_key(|(worker_id, _)| *worker_id);
        self.handles
            .set(SwarmHandles { primary, workers })
            .map_err(|_| eyre!("admission snapshot handles are already set"))
    }

    /// Return the admission view of every swarm, or `None` before [`Self::set`].
    ///
    /// A snapshot under 1 second old is served as is. Otherwise the caller waits for any running
    /// fetch and then fetches a new snapshot from every swarm concurrently, starting no sooner than
    /// 1 second after the previous fetch started. A swarm that errors or does not answer within 2
    /// seconds is reported with `available` false, no tracked epoch, no admission status and no
    /// entries. The snapshot's epoch is the primary swarm's accepted admission epoch, `None` when
    /// the primary is unavailable or has accepted no window.
    pub async fn snapshot(&self) -> Option<RpcAdmissionSnapshot> {
        let handles = self.handles.get()?;
        let mut state = self.fetch.lock().await;
        if let Some((_, snapshot)) =
            state.latest.as_ref().filter(|(finished, _)| finished.elapsed() < SNAPSHOT_TTL)
        {
            return Some(snapshot.clone());
        }
        // a cancelled caller leaves no snapshot behind; waiting out the spacing keeps requests
        // that disconnect mid-fetch from sending commands faster than completed fetches do
        if let Some(started) = state.started {
            tokio::time::sleep_until(started + SNAPSHOT_TTL).await;
        }
        state.started = Some(Instant::now());
        let snapshot = handles.fetch().await;
        state.latest = Some((Instant::now(), snapshot.clone()));
        Some(snapshot)
    }
}

/// Handles of every swarm the node runs.
#[derive(Debug)]
struct SwarmHandles {
    /// The primary swarm.
    primary: NetworkHandle<PrimaryRequest, PrimaryResponse>,
    /// Every configured worker swarm, active in this epoch or not, in worker id order.
    workers: Vec<(WorkerId, NetworkHandle<WorkerRequest, WorkerResponse>)>,
}

impl SwarmHandles {
    /// Ask every swarm for its snapshot concurrently and assemble the node's report, the primary
    /// first and then the workers in id order.
    async fn fetch(&self) -> RpcAdmissionSnapshot {
        let workers = join_all(self.workers.iter().map(|(worker_id, handle)| {
            swarm_admission(
                worker_network(*worker_id),
                Some(*worker_id),
                handle.admission_snapshot(),
            )
        }));
        let primary =
            swarm_admission(PRIMARY_NETWORK.to_owned(), None, self.primary.admission_snapshot());
        let (primary, workers) = tokio::join!(primary, workers);
        let epoch = primary.admission.as_ref().and_then(|status| status.epoch);
        RpcAdmissionSnapshot::new(epoch, std::iter::once(primary).chain(workers).collect())
    }
}

/// Fetch spacing and the latest snapshot of an [`AdmissionSnapshotSource`].
#[derive(Debug, Default)]
struct FetchState {
    /// When the latest fetch started, whether it completed or its caller was cancelled.
    started: Option<Instant>,
    /// The latest completed snapshot and when its fetch finished.
    latest: Option<(Instant, RpcAdmissionSnapshot)>,
}

/// Await one swarm's snapshot for at most `SWARM_SNAPSHOT_TIMEOUT` and convert the answer.
///
/// A swarm that errors or does not answer in time is reported unavailable under `network`. On a
/// timeout the request is dropped: a command still waiting for channel capacity is never queued,
/// and a queued one is answered into a closed reply channel.
async fn swarm_admission(
    network: String,
    worker_id: Option<WorkerId>,
    request: impl Future<Output = NetworkResult<SwarmAdmissionSnapshot>>,
) -> RpcSwarmAdmission {
    match tokio::time::timeout(SWARM_SNAPSHOT_TIMEOUT, request).await {
        Ok(Ok(snapshot)) => available_swarm(snapshot, worker_id),
        Ok(Err(error)) => {
            warn!(target: "tn::network", %network, %error, "swarm admission snapshot failed");
            unavailable_swarm(network, worker_id)
        }
        Err(_) => {
            warn!(target: "tn::network", %network, "swarm admission snapshot timed out");
            unavailable_swarm(network, worker_id)
        }
    }
}

/// Convert the snapshot of a swarm that answered into its RPC report.
///
/// The label is the one the swarm reports, its metrics label.
fn available_swarm(
    snapshot: SwarmAdmissionSnapshot,
    worker_id: Option<WorkerId>,
) -> RpcSwarmAdmission {
    let SwarmAdmissionSnapshot { network, tracked_epoch, status, entries } = snapshot;
    RpcSwarmAdmission {
        network,
        worker_id,
        available: true,
        tracked_epoch,
        admission: Some(rpc_status(&status)),
        entries: entries.into_iter().map(rpc_entry).collect(),
    }
}

/// Report a swarm that did not answer.
///
/// It has no status to report, so `admission` is `None` exactly when `available` is false, and
/// its empty entry list is not an empty tracked set.
fn unavailable_swarm(network: String, worker_id: Option<WorkerId>) -> RpcSwarmAdmission {
    RpcSwarmAdmission {
        network,
        worker_id,
        available: false,
        tracked_epoch: None,
        admission: None,
        entries: Vec::new(),
    }
}

/// Convert a swarm's admission status.
fn rpc_status(status: &AdmissionStatus) -> RpcAdmissionStatus {
    RpcAdmissionStatus {
        configured: rpc_mode(status.configured()),
        effective: rpc_mode(status.effective()),
        epoch: status.epoch(),
        fallback: status.fallback().map(rpc_fallback),
        resolved_current: rpc_count(status.resolved_current()),
        required_current: rpc_count(status.required_current()),
        connected_current: rpc_count(status.connected_current()),
        contradicted: rpc_count(status.contradicted()),
    }
}

/// Convert one tracked key, keeping its slot order and the swarm's admission decision.
fn rpc_entry(entry: TrackedEntry) -> RpcAdmissionEntry {
    let TrackedEntry {
        bls_pubkey,
        slots,
        peer_id,
        network_pubkey,
        multiaddrs,
        record_multiaddrs,
        record_timestamp,
        source,
        admitted,
    } = entry;
    RpcAdmissionEntry {
        bls_pubkey: Bytes::copy_from_slice(bls_pubkey.as_ref()),
        slots: slots.into_iter().map(rpc_slot).collect(),
        peer_id: peer_id.map(|peer_id| peer_id.to_base58()),
        network_pubkey,
        multiaddrs: multiaddrs.iter().map(ToString::to_string).collect(),
        record_multiaddrs: record_multiaddrs
            .map(|addrs| addrs.iter().map(ToString::to_string).collect()),
        record_timestamp,
        source: rpc_source(source),
        // copied, never derived from peer_id: a reported peer id can come from a tracked record
        // that admission does not resolve
        admitted,
    }
}

/// Convert an admission mode.
fn rpc_mode(mode: AdmissionMode) -> RpcAdmissionMode {
    match mode {
        AdmissionMode::Open => RpcAdmissionMode::Open,
        AdmissionMode::Grace => RpcAdmissionMode::Grace,
        AdmissionMode::Closed => RpcAdmissionMode::Closed,
    }
}

/// Convert an admission fallback cause.
fn rpc_fallback(fallback: AdmissionFallback) -> RpcAdmissionFallback {
    match fallback {
        AdmissionFallback::Missing => RpcAdmissionFallback::Missing,
        AdmissionFallback::Stale => RpcAdmissionFallback::Stale,
        AdmissionFallback::Contradictory => RpcAdmissionFallback::Contradictory,
        AdmissionFallback::Unresolved => RpcAdmissionFallback::Unresolved,
    }
}

/// Convert a tracked key's slot.
fn rpc_slot(slot: TrackedSlot) -> RpcAdmissionSlot {
    match slot {
        TrackedSlot::Previous => RpcAdmissionSlot::Previous,
        TrackedSlot::Current => RpcAdmissionSlot::Current,
        TrackedSlot::Next => RpcAdmissionSlot::Next,
        TrackedSlot::Scheduled => RpcAdmissionSlot::Scheduled,
        TrackedSlot::Eligible => RpcAdmissionSlot::Eligible,
        TrackedSlot::Trusted => RpcAdmissionSlot::Trusted,
        TrackedSlot::Bootstrap => RpcAdmissionSlot::Bootstrap,
    }
}

/// Convert where a tracked key's addresses came from.
fn rpc_source(source: EntrySource) -> RpcEntrySource {
    match source {
        EntrySource::Record => RpcEntrySource::Record,
        EntrySource::Config => RpcEntrySource::Config,
        EntrySource::Stub => RpcEntrySource::Stub,
        EntrySource::Unresolved => RpcEntrySource::Unresolved,
    }
}

/// Widen a count for the wire; a `usize` never exceeds `u64` on supported targets.
fn rpc_count(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// The metrics label of worker `worker_id`'s swarm, used for a worker that did not answer.
///
/// Matches the label a worker swarm reports for itself.
fn worker_network(worker_id: WorkerId) -> String {
    format!("worker-{worker_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_network_libp2p::{types::NetworkCommand, TNMessage};
    use tn_types::{BlsKeypair, BlsPublicKey, NetworkKeypair, NetworkPublicKey};
    use tokio::sync::mpsc;

    /// When each snapshot command reached a test swarm, in arrival order.
    type Commands = Arc<parking_lot::Mutex<Vec<Instant>>>;

    /// The primary record's address in the schema example.
    const CURRENT_ADDRESS: &str = "/ip4/203.0.113.10/udp/49590/quic-v1";

    /// The eligible record's address in the schema example.
    const ELIGIBLE_ADDRESS: &str = "/ip4/198.51.100.7/udp/49590/quic-v1";

    /// A swarm that answers every snapshot command with `snapshot` after `delay` and records when
    /// each command arrived.
    fn answering_swarm<Req, Res>(
        snapshot: SwarmAdmissionSnapshot,
        delay: Duration,
    ) -> (NetworkHandle<Req, Res>, Commands)
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        let (sender, mut queue) = mpsc::channel(16);
        let commands = Commands::default();
        let arrivals = commands.clone();
        tokio::spawn(async move {
            while let Some(command) = queue.recv().await {
                let NetworkCommand::AdmissionSnapshot { reply } = command else {
                    panic!("the source sends a swarm nothing but snapshot commands");
                };
                arrivals.lock().push(Instant::now());
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                // the caller may be gone by now; a swarm answers into a closed channel then
                let _ = reply.send(snapshot.clone());
            }
        });
        (NetworkHandle::new(sender), commands)
    }

    /// A swarm whose command loop has stalled: commands queue on the returned receiver and are
    /// never answered.
    fn silent_swarm<Req, Res>(
    ) -> (NetworkHandle<Req, Res>, mpsc::Receiver<NetworkCommand<Req, Res>>)
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        let (sender, queue) = mpsc::channel(16);
        (NetworkHandle::new(sender), queue)
    }

    /// A swarm whose network has stopped: its command channel is closed.
    fn stopped_swarm<Req, Res>() -> NetworkHandle<Req, Res>
    where
        Req: TNMessage,
        Res: TNMessage,
    {
        let (sender, _) = mpsc::channel(1);
        NetworkHandle::new(sender)
    }

    /// A source with `primary` and `workers` installed.
    fn source_with(
        primary: NetworkHandle<PrimaryRequest, PrimaryResponse>,
        workers: Vec<(WorkerId, NetworkHandle<WorkerRequest, WorkerResponse>)>,
    ) -> AdmissionSnapshotSource {
        let source = AdmissionSnapshotSource::default();
        source.set(primary, workers).expect("the first set installs the handles");
        source
    }

    /// The BLS keypair of the fixed scalar `[seed; 32]`, the key tn-rpc's schema tests use.
    fn bls_keypair(seed: u8) -> BlsKeypair {
        BlsKeypair::from_bytes(&[seed; 32])
            .expect("a small repeated byte is a valid bls private key scalar")
    }

    /// The BLS public key of [`bls_keypair`].
    fn bls_key(seed: u8) -> BlsPublicKey {
        *bls_keypair(seed).public()
    }

    /// The ed25519 network key of the fixed seed `[seed; 32]`.
    fn network_key(seed: u8) -> NetworkPublicKey {
        NetworkKeypair::ed25519_from_bytes([seed; 32])
            .expect("a 32-byte array is a valid ed25519 secret seed")
            .public()
            .into()
    }

    /// An entry the swarm resolved from the signed record of the validator with key seed `seed`,
    /// published at `record_timestamp` with the single address `address`.
    fn record_entry(
        seed: u8,
        slots: &[TrackedSlot],
        address: &str,
        record_timestamp: u64,
        admitted: bool,
    ) -> TrackedEntry {
        let network_pubkey = network_key(seed);
        TrackedEntry {
            peer_id: Some(network_pubkey.to_peer_id()),
            network_pubkey: Some(network_pubkey),
            multiaddrs: vec![address.parse().expect("a valid multiaddr")],
            record_timestamp: Some(record_timestamp),
            source: EntrySource::Record,
            admitted,
            ..TrackedEntry::new_for_test(bls_key(seed), slots.to_vec())
        }
    }

    /// The report of [`record_entry`]'s entry, built the way tn-rpc's schema tests build it.
    fn rpc_record_entry(
        seed: u8,
        slots: &[RpcAdmissionSlot],
        address: &str,
        record_timestamp: u64,
        admitted: bool,
    ) -> RpcAdmissionEntry {
        let network_pubkey = network_key(seed);
        RpcAdmissionEntry {
            bls_pubkey: Bytes::from(bls_keypair(seed).public().to_bytes().to_vec()),
            slots: slots.to_vec(),
            peer_id: Some(network_pubkey.to_peer_id().to_string()),
            network_pubkey: Some(network_pubkey),
            multiaddrs: vec![address.to_string()],
            record_multiaddrs: None,
            record_timestamp: Some(record_timestamp),
            source: RpcEntrySource::Record,
            admitted,
        }
    }

    /// The schema example's primary swarm: Closed at epoch 42, tracking a current committee
    /// member it admits and an eligible validator it does not, in key order.
    fn schema_primary() -> SwarmAdmissionSnapshot {
        SwarmAdmissionSnapshot::new_for_test(
            PRIMARY_NETWORK,
            Some(42),
            AdmissionStatus::new_for_test(
                AdmissionMode::Closed,
                AdmissionMode::Closed,
                Some(42),
                None,
                4,
                3,
                3,
                0,
            ),
            vec![
                record_entry(
                    7,
                    &[TrackedSlot::Current, TrackedSlot::Eligible],
                    CURRENT_ADDRESS,
                    1_793_750_400,
                    true,
                ),
                record_entry(9, &[TrackedSlot::Eligible], ELIGIBLE_ADDRESS, 1_793_736_000, false),
            ],
        )
    }

    /// The report tn-rpc's schema tests expect for [`schema_primary`].
    fn rpc_schema_primary() -> RpcSwarmAdmission {
        RpcSwarmAdmission {
            network: PRIMARY_NETWORK.to_string(),
            worker_id: None,
            available: true,
            tracked_epoch: Some(42),
            admission: Some(RpcAdmissionStatus {
                configured: RpcAdmissionMode::Closed,
                effective: RpcAdmissionMode::Closed,
                epoch: Some(42),
                fallback: None,
                resolved_current: 4,
                required_current: 3,
                connected_current: 3,
                contradicted: 0,
            }),
            entries: vec![
                rpc_record_entry(
                    7,
                    &[RpcAdmissionSlot::Current, RpcAdmissionSlot::Eligible],
                    CURRENT_ADDRESS,
                    1_793_750_400,
                    true,
                ),
                rpc_record_entry(
                    9,
                    &[RpcAdmissionSlot::Eligible],
                    ELIGIBLE_ADDRESS,
                    1_793_736_000,
                    false,
                ),
            ],
        }
    }

    /// A worker swarm one epoch behind the primary that fell back to Grace for unresolved current
    /// records, tracking the current committee member only.
    fn grace_worker(worker_id: WorkerId) -> SwarmAdmissionSnapshot {
        SwarmAdmissionSnapshot::new_for_test(
            worker_network(worker_id),
            Some(41),
            AdmissionStatus::new_for_test(
                AdmissionMode::Closed,
                AdmissionMode::Grace,
                Some(41),
                Some(AdmissionFallback::Unresolved),
                1,
                3,
                1,
                0,
            ),
            vec![record_entry(7, &[TrackedSlot::Current], CURRENT_ADDRESS, 1_793_750_400, true)],
        )
    }

    /// The report of [`grace_worker`].
    fn rpc_grace_worker(worker_id: WorkerId) -> RpcSwarmAdmission {
        RpcSwarmAdmission {
            network: format!("worker-{worker_id}"),
            worker_id: Some(worker_id),
            available: true,
            tracked_epoch: Some(41),
            admission: Some(RpcAdmissionStatus {
                configured: RpcAdmissionMode::Closed,
                effective: RpcAdmissionMode::Grace,
                epoch: Some(41),
                fallback: Some(RpcAdmissionFallback::Unresolved),
                resolved_current: 1,
                required_current: 3,
                connected_current: 1,
                contradicted: 0,
            }),
            entries: vec![rpc_record_entry(
                7,
                &[RpcAdmissionSlot::Current],
                CURRENT_ADDRESS,
                1_793_750_400,
                true,
            )],
        }
    }

    /// The report of a swarm that did not answer.
    fn rpc_unavailable(network: &str, worker_id: Option<WorkerId>) -> RpcSwarmAdmission {
        RpcSwarmAdmission {
            network: network.to_string(),
            worker_id,
            available: false,
            tracked_epoch: None,
            admission: None,
            entries: Vec::new(),
        }
    }

    /// The source has nothing to serve before the networks start, and a clone taken then, as an
    /// rpc server built before the swarms spawn holds one, serves snapshots once they are set.
    #[tokio::test(start_paused = true)]
    async fn snapshot_is_none_until_the_handles_are_set() {
        let source = AdmissionSnapshotSource::default();
        let rpc_clone = source.clone();
        assert_eq!(source.snapshot().await, None);
        assert_eq!(rpc_clone.snapshot().await, None);

        let (primary, commands) = answering_swarm(schema_primary(), Duration::ZERO);
        source.set(primary, Vec::new()).expect("the first set installs the handles");

        assert_eq!(
            rpc_clone.snapshot().await,
            Some(RpcAdmissionSnapshot::new(Some(42), vec![rpc_schema_primary()]))
        );
        assert_eq!(commands.lock().len(), 1);
    }

    /// The node reports the primary first and then each worker with its own status and entries,
    /// under the epoch the primary accepted.
    #[tokio::test(start_paused = true)]
    async fn snapshot_reports_the_primary_and_a_worker() {
        let (primary, primary_commands) = answering_swarm(schema_primary(), Duration::ZERO);
        let (worker, worker_commands) = answering_swarm(grace_worker(1), Duration::ZERO);
        let source = source_with(primary, vec![(1, worker)]);

        let snapshot = source.snapshot().await.expect("the handles are set");

        assert_eq!(
            snapshot,
            RpcAdmissionSnapshot::new(Some(42), vec![rpc_schema_primary(), rpc_grace_worker(1)])
        );
        assert_eq!(primary_commands.lock().len(), 1);
        assert_eq!(worker_commands.lock().len(), 1);
    }

    /// A worker that never answers is reported unavailable, with a null admission and no
    /// entries, once its 2 second bound passes; the primary's report is complete, and the
    /// abandoned command waits in the worker's queue with its reply channel closed.
    #[tokio::test(start_paused = true)]
    async fn a_silent_worker_times_out_while_the_primary_reports() {
        let (primary, _) = answering_swarm(schema_primary(), Duration::ZERO);
        let (worker, mut worker_queue) = silent_swarm();
        let source = source_with(primary, vec![(1, worker)]);
        let start = Instant::now();

        let snapshot = source.snapshot().await.expect("the handles are set");

        assert_eq!(start.elapsed().as_millis(), SWARM_SNAPSHOT_TIMEOUT.as_millis());
        assert_eq!(
            snapshot,
            RpcAdmissionSnapshot::new(
                Some(42),
                vec![rpc_schema_primary(), rpc_unavailable("worker-1", Some(1))]
            )
        );
        let worker_json = serde_json::to_value(&snapshot.swarms[1]).expect("a json value");
        assert_eq!(worker_json["available"], serde_json::Value::Bool(false));
        assert_eq!(worker_json.get("admission"), Some(&serde_json::Value::Null));

        let Ok(NetworkCommand::AdmissionSnapshot { reply }) = worker_queue.try_recv() else {
            panic!("the timed-out snapshot command waits in the worker's queue");
        };
        assert!(reply.is_closed(), "the source dropped the reply it stopped waiting for");
        assert!(worker_queue.try_recv().is_err(), "the source sent the worker one command");
    }

    /// A swarm whose network stopped is reported unavailable at once, without waiting out the
    /// bound; with the primary unavailable the snapshot has no epoch.
    #[tokio::test(start_paused = true)]
    async fn a_stopped_swarm_reports_unavailable_without_waiting() {
        let (worker, _) = answering_swarm(grace_worker(0), Duration::ZERO);
        let source = source_with(stopped_swarm(), vec![(0, worker)]);
        let start = Instant::now();

        let snapshot = source.snapshot().await.expect("the handles are set");

        assert_eq!(start.elapsed(), Duration::ZERO);
        assert_eq!(
            snapshot,
            RpcAdmissionSnapshot::new(
                None,
                vec![rpc_unavailable(PRIMARY_NETWORK, None), rpc_grace_worker(0)]
            )
        );
    }

    /// Fifty concurrent callers share one fetch: every swarm receives one snapshot command per
    /// TTL window, the callers waiting on the fetch in flight all return when it does, and
    /// workers installed out of order are reported in id order.
    #[tokio::test(start_paused = true)]
    async fn concurrent_callers_share_one_fetch_per_ttl_window() {
        let fetch_time = Duration::from_millis(300);
        let (primary, primary_commands) = answering_swarm(schema_primary(), fetch_time);
        let (worker_one, worker_one_commands) = answering_swarm(grace_worker(1), fetch_time);
        let (worker_two, worker_two_commands) = answering_swarm(grace_worker(2), fetch_time);
        let source = source_with(primary, vec![(2, worker_two), (1, worker_one)]);
        let expected = RpcAdmissionSnapshot::new(
            Some(42),
            vec![rpc_schema_primary(), rpc_grace_worker(1), rpc_grace_worker(2)],
        );
        let command_counts = || {
            [&primary_commands, &worker_one_commands, &worker_two_commands]
                .map(|commands| commands.lock().len())
        };

        for window in 1..=2 {
            let start = Instant::now();
            let callers: Vec<_> = (0..50)
                .map(|_| {
                    let source = source.clone();
                    tokio::spawn(async move { source.snapshot().await })
                })
                .collect();

            for answer in join_all(callers).await {
                assert_eq!(answer.expect("a caller task completes"), Some(expected.clone()));
            }
            assert_eq!(start.elapsed(), fetch_time, "every caller returned with the one fetch");
            assert_eq!(command_counts(), [window; 3]);

            tokio::time::advance(SNAPSHOT_TTL).await;
        }
    }

    /// A snapshot is served for 1 second after its fetch finished: a call inside the window sends
    /// no command, a call once it has passed fetches anew.
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_is_served_for_one_ttl() {
        let (primary, primary_commands) = answering_swarm(schema_primary(), Duration::ZERO);
        let (worker, worker_commands) = answering_swarm(grace_worker(0), Duration::ZERO);
        let source = source_with(primary, vec![(0, worker)]);
        let command_counts =
            || [&primary_commands, &worker_commands].map(|commands| commands.lock().len());

        let first = source.snapshot().await.expect("the handles are set");
        assert_eq!(command_counts(), [1, 1]);

        tokio::time::advance(SNAPSHOT_TTL - Duration::from_millis(1)).await;
        assert_eq!(source.snapshot().await, Some(first.clone()));
        assert_eq!(command_counts(), [1, 1], "a call inside the window sends no command");

        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(source.snapshot().await, Some(first));
        assert_eq!(command_counts(), [2, 2], "a call once the window passed fetches anew");
    }

    /// A caller cancelled mid-fetch, as by a client that disconnects, leaves no snapshot behind,
    /// and the next caller still waits for the spacing from the cancelled fetch's start before
    /// it sends commands.
    #[tokio::test(start_paused = true)]
    async fn a_cancelled_fetch_still_spaces_the_next_one() {
        let fetch_time = Duration::from_millis(500);
        let (primary, primary_commands) = answering_swarm(schema_primary(), fetch_time);
        let (worker, worker_commands) = answering_swarm(grace_worker(0), fetch_time);
        let source = source_with(primary, vec![(0, worker)]);

        let cancelled = tokio::time::timeout(Duration::from_millis(100), source.snapshot()).await;
        assert!(cancelled.is_err(), "the caller gave up before the swarms answered");

        let snapshot = source.snapshot().await;

        assert_eq!(
            snapshot,
            Some(RpcAdmissionSnapshot::new(
                Some(42),
                vec![rpc_schema_primary(), rpc_grace_worker(0)]
            ))
        );
        for commands in [primary_commands, worker_commands] {
            let arrivals = commands.lock();
            assert_eq!(arrivals.len(), 2);
            assert!(
                arrivals[1].duration_since(arrivals[0]) >= SNAPSHOT_TTL,
                "the next fetch started a full spacing after the cancelled one"
            );
        }
    }

    /// The handles are installed once: a second set is an error and the first handles keep
    /// serving.
    #[tokio::test(start_paused = true)]
    async fn a_second_set_is_an_error_and_keeps_the_first_handles() {
        let (first, first_commands) = answering_swarm(schema_primary(), Duration::ZERO);
        // a different answer, so the snapshot shows which handles served it
        let (second, second_commands) = answering_swarm(grace_worker(0), Duration::ZERO);
        let source = source_with(first, Vec::new());

        assert!(source.set(second, Vec::new()).is_err());

        assert_eq!(
            source.snapshot().await,
            Some(RpcAdmissionSnapshot::new(Some(42), vec![rpc_schema_primary()]))
        );
        assert_eq!(first_commands.lock().len(), 1);
        assert_eq!(second_commands.lock().len(), 0);
    }

    /// A swarm's answer reaches the RPC report field for field: the schema example the tn-rpc
    /// tests serialize comes out of the swarm types holding the same values, and a launch
    /// address that replaces a record's addresses keeps the record's own list.
    #[tokio::test(start_paused = true)]
    async fn conversion_keeps_every_field_of_the_schema_example() {
        let (primary, _) = answering_swarm(schema_primary(), Duration::ZERO);
        let source = source_with(primary, Vec::new());

        assert_eq!(
            source.snapshot().await,
            Some(RpcAdmissionSnapshot::new(Some(42), vec![rpc_schema_primary()]))
        );

        let launch = "/ip4/192.0.2.1/udp/49590/quic-v1";
        let stub = TrackedEntry {
            multiaddrs: vec![launch.parse().expect("a valid multiaddr")],
            record_multiaddrs: Some(vec![CURRENT_ADDRESS.parse().expect("a valid multiaddr")]),
            source: EntrySource::Stub,
            ..record_entry(7, &[TrackedSlot::Current], CURRENT_ADDRESS, 1_793_750_400, true)
        };
        assert_eq!(
            rpc_entry(stub),
            RpcAdmissionEntry {
                multiaddrs: vec![launch.to_string()],
                record_multiaddrs: Some(vec![CURRENT_ADDRESS.to_string()]),
                source: RpcEntrySource::Stub,
                ..rpc_record_entry(
                    7,
                    &[RpcAdmissionSlot::Current],
                    CURRENT_ADDRESS,
                    1_793_750_400,
                    true
                )
            }
        );
    }

    /// Every swarm-side variant maps to the RPC variant of the same name.
    #[test]
    fn every_variant_maps_to_its_namesake() {
        let modes = [AdmissionMode::Open, AdmissionMode::Grace, AdmissionMode::Closed];
        assert_eq!(
            modes.map(rpc_mode),
            [RpcAdmissionMode::Open, RpcAdmissionMode::Grace, RpcAdmissionMode::Closed]
        );
        let fallbacks = [
            AdmissionFallback::Missing,
            AdmissionFallback::Stale,
            AdmissionFallback::Contradictory,
            AdmissionFallback::Unresolved,
        ];
        assert_eq!(
            fallbacks.map(rpc_fallback),
            [
                RpcAdmissionFallback::Missing,
                RpcAdmissionFallback::Stale,
                RpcAdmissionFallback::Contradictory,
                RpcAdmissionFallback::Unresolved,
            ]
        );
        let slots = [
            TrackedSlot::Previous,
            TrackedSlot::Current,
            TrackedSlot::Next,
            TrackedSlot::Scheduled,
            TrackedSlot::Eligible,
            TrackedSlot::Trusted,
            TrackedSlot::Bootstrap,
        ];
        assert_eq!(
            slots.map(rpc_slot),
            [
                RpcAdmissionSlot::Previous,
                RpcAdmissionSlot::Current,
                RpcAdmissionSlot::Next,
                RpcAdmissionSlot::Scheduled,
                RpcAdmissionSlot::Eligible,
                RpcAdmissionSlot::Trusted,
                RpcAdmissionSlot::Bootstrap,
            ]
        );
        let sources =
            [EntrySource::Record, EntrySource::Config, EntrySource::Stub, EntrySource::Unresolved];
        assert_eq!(
            sources.map(rpc_source),
            [
                RpcEntrySource::Record,
                RpcEntrySource::Config,
                RpcEntrySource::Stub,
                RpcEntrySource::Unresolved,
            ]
        );
    }
}
