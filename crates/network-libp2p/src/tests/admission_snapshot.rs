//! A Closed committee tracks an eligible validator that only a trusted hub can reach.
//!
//! The committee's admission snapshot exports the validator's signed record without admitting
//! it, and the kad tracked class keeps that record reachable for the committee after the
//! validator leaves the hub.

use super::*;
use crate::{
    common::{TestPrimaryRequest, TestPrimaryResponse, TEST_HEARTBEAT_INTERVAL},
    AdmissionFallback, AdmissionStatus, EntrySource, TrackedEntry, TrackedSlot,
};
use assert_matches::assert_matches;
use eyre::eyre;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use std::{cell::RefCell, collections::BTreeMap};
use tn_config::{AdmissionMode, ConsensusConfig};
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{wait_until, CommitteeFixture};
use tn_types::{NetworkPublicKey, P2pNode, TaskManager};
use tokio::{sync::mpsc, task::JoinHandle};

/// The chain id of the e2e harness.
const CHAIN_ID: u64 = 911_329;

/// Bound for each convergence step, generous for a loaded CI host.
const CONVERGE: Duration = Duration::from_secs(60);

/// The shortest window in which a lookup that finds no record must leave the key unresolved.
const NEGATIVE_WINDOW: Duration = Duration::from_secs(5);

/// The swarm under test.
type TestNetwork = ConsensusNetwork<
    TestPrimaryRequest,
    TestPrimaryResponse,
    MemDatabase,
    mpsc::Sender<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>,
>;

/// The events a [`TestNetwork`] emits.
type TestEvents = mpsc::Receiver<NetworkEvent<TestPrimaryRequest, TestPrimaryResponse>>;

/// The handle of a [`TestNetwork`].
type TestHandle = NetworkHandle<TestPrimaryRequest, TestPrimaryResponse>;

/// A running primary swarm.
struct Node {
    /// BLS identity.
    bls: BlsPublicKey,
    /// Transport identity.
    peer_id: PeerId,
    /// Listen address, which is also the address the node's record advertises.
    address: Multiaddr,
    /// Network key, for trusted-hub endpoints.
    network_key: NetworkPublicKey,
    /// Command handle.
    handle: TestHandle,
    /// The swarm task.
    task: JoinHandle<NetworkResult<()>>,
    /// Held so the swarm's event channel stays open.
    _events: TestEvents,
}

/// Network settings shared by every swarm: the test heartbeat, the e2e chain id, and the
/// requested admission mode with a lease that outlives the test.
fn network_config(mode: AdmissionMode) -> eyre::Result<NetworkConfig> {
    let mut config: NetworkConfig = serde_json::from_value(serde_json::json!({
        "admission": { "mode": mode, "snapshot_max_age_secs": 600 }
    }))?;
    config.peer_config_mut().heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    config.set_chain_id(CHAIN_ID);
    Ok(config)
}

/// Build an authority's primary swarm without running it.
fn build(
    config: &ConsensusConfig<MemDatabase>,
    network_config: &NetworkConfig,
    tasks: &TaskManager,
) -> NetworkResult<(TestNetwork, TestEvents)> {
    let (events, network_events) = mpsc::channel(10);
    let network = TestNetwork::new(
        network_config,
        events,
        config.key_config().clone(),
        config.key_config().primary_network_keypair().clone(),
        MemDatabase::default(),
        tasks.get_spawner(),
        NetworkType::Primary,
        config.primary_address(),
        None,
    )?;
    Ok((network, network_events))
}

/// Run a built swarm and listen on the authority's address.
async fn start(
    config: &ConsensusConfig<MemDatabase>,
    network: TestNetwork,
    events: TestEvents,
) -> eyre::Result<Node> {
    let handle = network.network_handle();
    let task = tokio::spawn(network.run());
    let address = config.primary_address();
    handle.start_listening(address.clone()).await?;
    let network_key = config.primary_networkkey();
    Ok(Node {
        bls: config.key_config().primary_public_key(),
        peer_id: PeerId::from(network_key.clone()),
        address,
        network_key,
        handle,
        task,
        _events: events,
    })
}

/// Build and run an authority's primary swarm.
async fn spawn(
    config: &ConsensusConfig<MemDatabase>,
    network_config: &NetworkConfig,
    tasks: &TaskManager,
) -> eyre::Result<Node> {
    let (network, events) = build(config, network_config, tasks)?;
    start(config, network, events).await
}

/// The observations of an admission status as one comparable value.
type Observed = (AdmissionMode, AdmissionMode, Option<u64>, Option<AdmissionFallback>, [usize; 4]);

/// Every observation of `status`.
fn observed(status: &AdmissionStatus) -> Observed {
    (
        status.configured(),
        status.effective(),
        status.epoch(),
        status.fallback(),
        [
            status.resolved_current(),
            status.required_current(),
            status.connected_current(),
            status.contradicted(),
        ],
    )
}

/// The snapshot entry of `key`, if the swarm lists it.
async fn entry(handle: &TestHandle, key: BlsPublicKey) -> eyre::Result<Option<TrackedEntry>> {
    Ok(handle.admission_snapshot().await?.entries.into_iter().find(|entry| entry.bls_pubkey == key))
}

/// Whether the swarm lists `key` with a verified record.
async fn resolved(handle: &TestHandle, key: BlsPublicKey) -> eyre::Result<bool> {
    Ok(entry(handle, key).await?.is_some_and(|entry| entry.source == EntrySource::Record))
}

/// Assert that `entry` exports `record` for an eligible-only key and does not admit it.
fn assert_eligible_record(entry: &TrackedEntry, record: &NodeRecord, peer_id: PeerId) {
    assert_eq!(entry.slots, vec![TrackedSlot::Eligible]);
    assert_eq!(entry.source, EntrySource::Record);
    assert_eq!(entry.peer_id, Some(peer_id));
    assert_eq!(entry.network_pubkey.as_ref(), Some(&record.info.pubkey));
    assert_eq!(entry.multiaddrs, record.info.multiaddrs);
    assert_eq!(entry.record_multiaddrs, None);
    assert_eq!(entry.record_timestamp, Some(record.info.timestamp));
    assert!(!entry.admitted, "an eligible-only key is never admitted");
}

/// Running counter totals of one debugging recorder, whose snapshots drain every counter.
struct CounterTotals {
    /// Reads and drains the recorder.
    snapshotter: Snapshotter,
    /// Every counter's total across all snapshots so far.
    totals: RefCell<HashMap<metrics::Key, u64>>,
}

impl CounterTotals {
    /// Totals for the recorder `snapshotter` reads.
    fn new(snapshotter: Snapshotter) -> Self {
        Self { snapshotter, totals: RefCell::default() }
    }

    /// The total of the counters named `name` whose labels include `label`.
    fn total(&self, name: &str, label: (&str, &str)) -> u64 {
        let mut totals = self.totals.borrow_mut();
        self.snapshotter.snapshot().into_vec().into_iter().for_each(|(key, _, _, value)| {
            if let DebugValue::Counter(count) = value {
                let total = totals.entry(key.key().clone()).or_default();
                *total = total.saturating_add(count);
            }
        });
        totals
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && key.labels().any(|l| l.key() == label.0 && l.value() == label.1)
            })
            .map(|(_, count)| *count)
            .sum()
    }
}

/// Four Closed committee swarms C1-C4 and an eligible validator E reach each other only through
/// a trusted Open hub H.
///
/// C1 tracks E as eligible: its snapshot exports E's signed record, addresses and timestamp
/// without admitting E, and leaves the admission status unchanged. E's inbound connection is
/// refused under Closed admission, and so is C1's own dial to E.
///
/// Then E leaves the hub. C2 has not tracked E, so it holds no copy; its lookup succeeds from the
/// row C1's kad store retains for the tracked key, the only copy left. As a negative control,
/// C1 and C2 stop tracking E, which releases both rows, so no reachable node holds E's record;
/// C2 then tracks E again and still lists it unresolved after at least twice the time its
/// successful lookup took. The control proves the release; it does not observe that a lookup ran
/// meanwhile.
#[tokio::test]
async fn closed_committee_exports_an_eligible_validator_it_does_not_admit() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let counters = CounterTotals::new(recorder.snapshotter());
    let open = network_config(AdmissionMode::Open)?;
    let closed = network_config(AdmissionMode::Closed)?;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .committee_size(NonZeroUsize::new(6).ok_or_else(|| eyre!("six authorities"))?)
        .randomize_ports(true)
        .with_network_config(open.clone())
        .build();
    let configs: Vec<_> =
        fixture.authorities().map(|authority| authority.consensus_config()).collect();
    let [c1, c2, c3, c4, h, e] = configs.as_slice() else {
        return Err(eyre!("the fixture yields six authorities"));
    };
    let tasks = TaskManager::default();

    // the hub listens first, so every trusted dial finds it
    let hub = spawn(h, &open, &tasks).await?;
    let (network, events) = build(e, &open, &tasks)?;
    // the record E publishes at startup; it is refreshed only after a publication interval
    let record = network.node_record.clone();
    let eligible = start(e, network, events).await?;
    // C1's peer-manager counters resolve against the recorder when the swarm is built
    let (network, events) = metrics::with_local_recorder(&recorder, || build(c1, &closed, &tasks))?;
    let mut committee = vec![start(c1, network, events).await?];
    for config in [c2, c3, c4] {
        committee.push(spawn(config, &closed, &tasks).await?);
    }

    let hub_endpoint = BTreeMap::from([(
        hub.bls,
        P2pNode {
            network_address: hub.address.clone(),
            network_key: hub.network_key.clone(),
            rpc: None,
        },
    )]);
    for node in committee.iter().chain([&eligible]) {
        node.handle.add_trusted_peers(hub_endpoint.clone()).await?;
    }
    wait_until(CONVERGE, "the hub serves the eligible validator's record", || async {
        Ok(hub.handle.kad_store_get(eligible.bls).await?.is_some())
    })
    .await?;

    // the committee resolves its members through the hub and closes admission
    let window: HashSet<_> = committee.iter().map(|node| node.bls).collect();
    for node in &committee {
        node.handle.update_committees_at(1, HashSet::new(), window.clone(), HashSet::new()).await?;
    }
    for node in &committee {
        wait_until(CONVERGE, "a committee member enforces Closed admission", || async {
            if node.handle.admission_status().await?.effective() == AdmissionMode::Closed {
                return Ok(true);
            }
            // a lookup that ran before the hub held every record is retried
            node.handle.find_authorities(window.iter().copied().collect()).await?;
            Ok(false)
        })
        .await?;
    }
    for node in &committee {
        for peer in committee.iter().filter(|peer| peer.bls != node.bls) {
            wait_until(CONVERGE, "committee members connect", || async {
                if node.handle.connected_peer_ids().await?.contains(&peer.peer_id) {
                    return Ok(true);
                }
                // a concurrent dial from the other side may win; poll again either way
                let _ = node.handle.dial_by_bls(peer.bls).await;
                Ok(false)
            })
            .await?;
        }
        wait_until(CONVERGE, "Closed admission drops the eligible validator", || async {
            Ok(!node.handle.connected_peer_ids().await?.contains(&eligible.peer_id))
        })
        .await?;
    }

    // C1 tracks E as eligible
    let c1 = &committee[0];
    let before = c1.handle.admission_status().await?;
    assert_eq!(
        observed(&before),
        (AdmissionMode::Closed, AdmissionMode::Closed, Some(1), None, [4, 3, 3, 0])
    );
    c1.handle.update_tracked_keys(1, HashSet::new(), HashSet::from([eligible.bls])).await?;
    wait_until(CONVERGE, "C1 resolves the eligible record through the hub", || async {
        resolved(&c1.handle, eligible.bls).await
    })
    .await?;
    // C1 lists the hub from its configured stub until the hub's own signed record arrives
    wait_until(CONVERGE, "C1 holds the hub's signed record", || async {
        resolved(&c1.handle, hub.bls).await
    })
    .await?;
    let snapshot = c1.handle.admission_snapshot().await?;
    assert_eq!(snapshot.network, "primary");
    assert_eq!(snapshot.tracked_epoch, Some(1));
    assert_eq!(observed(&snapshot.status), observed(&before));
    assert_eq!(observed(&c1.handle.admission_status().await?), observed(&before));
    let listed = snapshot
        .entries
        .iter()
        .find(|entry| entry.bls_pubkey == eligible.bls)
        .ok_or_else(|| eyre!("C1 lists the eligible validator"))?;
    assert_eligible_record(listed, &record, eligible.peer_id);
    // every other listed key is a member or the hub, and admitted
    let others: BTreeMap<_, _> = snapshot
        .entries
        .iter()
        .filter(|entry| entry.bls_pubkey != eligible.bls)
        .map(|entry| (entry.bls_pubkey, (entry.slots.clone(), entry.source, entry.admitted)))
        .collect();
    let expected: BTreeMap<_, _> = committee[1..]
        .iter()
        .map(|node| (node.bls, (vec![TrackedSlot::Current], EntrySource::Record, true)))
        .chain([(hub.bls, (vec![TrackedSlot::Trusted], EntrySource::Record, true))])
        .collect();
    assert_eq!(others, expected);
    // a tracked record binds no identity and feeds no dial
    assert!(!c1.handle.connected_peers().await?.contains(&eligible.bls));
    assert_matches!(c1.handle.dial_by_bls(eligible.bls).await, Err(NetworkError::PeerMissing));

    // E dials C1, which refuses the identity after the handshake
    let closed_denied = |direction| {
        counters.total("tn_network.closed_admission_denied_total", ("direction", direction))
    };
    let refused_listens = || {
        counters
            .total("tn_network.listen_failures_total", ("reason", "peer_manager_closed_admission"))
    };
    let (inbound, outbound, listens) =
        (closed_denied("in"), closed_denied("out"), refused_listens());
    // E's own result is not the refusal: its side of the handshake completes before C1 checks the
    // authenticated identity, and an automatic redial of E's may already be in flight
    let _ = eligible.handle.dial(c1.peer_id, c1.address.clone()).await;
    wait_until(CONVERGE, "C1 refuses the eligible validator's connection", || async {
        Ok(closed_denied("in") > inbound && refused_listens() > listens)
    })
    .await?;
    assert!(!c1.handle.connected_peer_ids().await?.contains(&eligible.peer_id));
    // C1's own dial to E is refused before any transport work
    let refused = c1.handle.dial(eligible.peer_id, eligible.address.clone()).await;
    assert_matches!(refused, Err(NetworkError::Dial(_)));
    assert!(closed_denied("out") > outbound, "a Closed outbound refusal is counted");

    // E leaves the hub, which releases E's connection-scoped row
    eligible.task.abort();
    wait_until(CONVERGE, "the hub drops the departed validator's record", || async {
        Ok(!hub.handle.connected_peer_ids().await?.contains(&eligible.peer_id)
            && hub.handle.kad_store_get(eligible.bls).await?.is_none())
    })
    .await?;
    // C1's tracked row is the only copy left, and C2 has not tracked E yet
    assert!(c1.handle.kad_store_get(eligible.bls).await?.is_some(), "C1 retains the tracked row");
    for node in &committee[1..] {
        assert!(node.handle.kad_store_get(eligible.bls).await?.is_none());
    }
    let c2 = &committee[1];
    assert!(entry(&c2.handle, eligible.bls).await?.is_none());
    assert!(c2.handle.connected_peer_ids().await?.contains(&c1.peer_id));

    // C2's lookup succeeds from C1's tracked row
    let started = tokio::time::Instant::now();
    c2.handle.update_tracked_keys(1, HashSet::new(), HashSet::from([eligible.bls])).await?;
    wait_until(CONVERGE, "C2 resolves the record from C1's tracked row", || async {
        resolved(&c2.handle, eligible.bls).await
    })
    .await?;
    let lookup = started.elapsed();
    let listed = entry(&c2.handle, eligible.bls)
        .await?
        .ok_or_else(|| eyre!("C2 lists the eligible validator"))?;
    assert_eligible_record(&listed, &record, eligible.peer_id);
    assert!(c2.handle.kad_store_get(eligible.bls).await?.is_some(), "C2 retains the tracked row");

    // negative control: without a tracked row on any reachable node the same lookup fails
    for node in [c1, c2] {
        node.handle.update_tracked_keys(2, HashSet::new(), HashSet::new()).await?;
    }
    wait_until(CONVERGE, "C1 and C2 release the tracked rows", || async {
        Ok(c1.handle.kad_store_get(eligible.bls).await?.is_none()
            && c2.handle.kad_store_get(eligible.bls).await?.is_none()
            && entry(&c2.handle, eligible.bls).await?.is_none())
    })
    .await?;
    for node in committee.iter().chain([&hub]) {
        assert!(node.handle.kad_store_get(eligible.bls).await?.is_none());
    }
    c2.handle.update_tracked_keys(3, HashSet::new(), HashSet::from([eligible.bls])).await?;
    // no reachable node retains the row, so E cannot resolve whether or not a lookup runs; the
    // wait, at least twice the successful lookup's time, normally spans one, but nothing checks
    tokio::time::sleep(lookup.saturating_mul(2).max(NEGATIVE_WINDOW)).await;
    let unresolved = entry(&c2.handle, eligible.bls)
        .await?
        .ok_or_else(|| eyre!("C2 lists the eligible validator again"))?;
    assert_eq!(unresolved.slots, vec![TrackedSlot::Eligible]);
    assert_eq!(unresolved.source, EntrySource::Unresolved);
    assert!(unresolved.multiaddrs.is_empty());
    assert_eq!(unresolved.record_timestamp, None);
    assert!(!unresolved.admitted);

    for node in committee.into_iter().chain([hub]) {
        node.task.abort();
    }
    Ok(())
}
