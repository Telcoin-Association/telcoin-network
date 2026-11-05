//! Scheduled and eligible key tracking: ingest, isolation from admission and pruning, demotion,
//! freshness, the lookup scheduler, and the key cap.

use super::*;
use crate::{
    common::create_multiaddr, diagnostics::emission_tests::Capture, metrics::ConnectionDirection,
    types::NetworkType, CommitteeUpdate,
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::BTreeSet, task::Waker};
use tn_types::{BlsKeypair, NetworkKeypair, TimestampSec};

/// A deterministic BLS key for `seed`.
fn bls(seed: u8) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
}

/// `count` distinct BLS keys drawn from one seeded generator.
fn keys(seed: u8, count: usize) -> Vec<BlsPublicKey> {
    let mut rng = StdRng::from_seed([seed; 32]);
    (0..count).map(|_| *BlsKeypair::generate(&mut rng).public()).collect()
}

/// A record with a fresh transport identity, signed at `timestamp`.
fn network_info_at(timestamp: TimestampSec) -> NetworkInfo {
    NetworkInfo {
        pubkey: NetworkKeypair::generate_ed25519().public().into(),
        multiaddrs: vec![create_multiaddr(None)],
        timestamp,
        rpc: None,
    }
}

/// A record with a fresh transport identity, signed now.
fn network_info() -> NetworkInfo {
    network_info_at(now())
}

/// A record that also advertises an RPC endpoint.
fn network_info_with_rpc() -> NetworkInfo {
    NetworkInfo {
        rpc: Some(RpcInfo {
            http: "https://validator.example.com:8545/".parse().expect("http url"),
            ws: None,
        }),
        ..network_info()
    }
}

/// The transport identity a record advertises.
fn peer_id(info: &NetworkInfo) -> PeerId {
    info.pubkey.clone().into()
}

/// A configured BLS identity with an independent transport key.
fn node(seed: u8) -> (BlsPublicKey, P2pNode) {
    let node = P2pNode {
        network_key: NetworkKeypair::generate_ed25519().public().into(),
        network_address: create_multiaddr(None),
        rpc: None,
    };
    (bls(seed), node)
}

/// The record a configured identity would sign.
fn info_of(node: &P2pNode) -> NetworkInfo {
    NetworkInfo {
        pubkey: node.network_key.clone(),
        multiaddrs: vec![node.network_address.clone()],
        timestamp: now(),
        rpc: None,
    }
}

/// A primary-swarm manager with the given local transport identity and peer settings.
fn manager_with(local: PeerId, config: &PeerConfig) -> PeerManager {
    PeerManager::new(local, config, PeerManagerMetrics::new_for(&NetworkType::Primary))
}

/// A primary-swarm manager with default peer settings.
fn manager() -> PeerManager {
    manager_with(PeerId::random(), &PeerConfig::default())
}

/// Whether `held` is `expected` with its RPC endpoint dropped.
fn holds(held: Option<&NetworkInfo>, expected: &NetworkInfo) -> bool {
    held.is_some_and(|held| {
        held.pubkey == expected.pubkey
            && held.multiaddrs == expected.multiaddrs
            && held.timestamp == expected.timestamp
            && held.rpc.is_none()
    })
}

/// Keys of the committee cache, in key order.
fn known_keys(manager: &PeerManager) -> BTreeSet<BlsPublicKey> {
    manager.known_peers.keys().copied().collect()
}

/// Keys of every queued committee-class lookup.
fn committee_lookups(manager: &PeerManager) -> HashSet<BlsPublicKey> {
    manager
        .events
        .iter()
        .filter_map(|event| match event {
            PeerEvent::MissingAuthorities(keys)
            | PeerEvent::MissingRecords(keys, LookupClass::Committee) => Some(keys),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

/// Advance one heartbeat period, poll the heartbeat, and return each tracked lookup it queued.
async fn tracked_tick(manager: &mut PeerManager) -> Vec<Vec<BlsPublicKey>> {
    manager.events.clear();
    tokio::time::advance(manager.heartbeat.period()).await;
    assert!(manager.heartbeat_ready(&mut Context::from_waker(Waker::noop())));
    manager
        .events
        .iter()
        .filter_map(|event| match event {
            PeerEvent::MissingRecords(keys, LookupClass::Tracked) => Some(keys.clone()),
            _ => None,
        })
        .collect()
}

/// Pick the due tracked lookups at `at` with the production budget and backoff.
fn due(
    tracked: &mut TrackedKeys,
    wanted: &HashSet<BlsPublicKey>,
    at: Instant,
) -> Vec<BlsPublicKey> {
    tracked.due_lookups(
        wanted,
        at,
        TRACKED_LOOKUPS_PER_HEARTBEAT,
        TRACKED_LOOKUP_INITIAL,
        TRACKED_LOOKUP_MAX,
    )
}

/// A Closed admission window whose current committee is fully resolved.
struct ClosedWindow {
    /// Transport identity of the swarm under test.
    local_peer: PeerId,
    /// BLS identity of the swarm under test, a current member.
    local: BlsPublicKey,
    /// Remote current members with their cached records.
    members: Vec<(BlsPublicKey, NetworkInfo)>,
}

impl ClosedWindow {
    /// Three remote members plus the local identity.
    fn new() -> Self {
        Self {
            local_peer: PeerId::random(),
            local: bls(20),
            members: (21..24).map(|seed| (bls(seed), network_info())).collect(),
        }
    }

    /// A manager that has accepted the window at epoch 7 and enforces Closed.
    fn manager(&self) -> PeerManager {
        let mut manager = manager_with(self.local_peer, &PeerConfig::default());
        manager.configure_admission(
            AdmissionConfig::new(AdmissionMode::Closed, Duration::from_secs(300)),
            self.local,
        );
        self.members.iter().for_each(|(key, info)| manager.cache_known_peer(*key, info.clone()));
        let current = self.members.iter().map(|(key, _)| *key).chain([self.local]).collect();
        assert!(manager.update_committees_at(
            CommitteeUpdate::EpochStart,
            7,
            HashSet::new(),
            current,
            HashSet::new(),
        ));
        manager
    }
}

/// A tracked record is retained from a query result and from a pushed record, own or relayed,
/// while an untracked key and the local key are still dropped at both sites.
#[tokio::test]
async fn tracked_record_retained_at_both_ingest_sites() {
    let mut manager = manager();
    let local = bls(1);
    manager.configure_admission(
        AdmissionConfig::new(AdmissionMode::Open, Duration::from_secs(300)),
        local,
    );
    let (scheduled, eligible, relayed, untracked) = (bls(2), bls(3), bls(4), bls(5));
    manager.update_tracked_keys(
        1,
        HashSet::from([scheduled, local]),
        HashSet::from([eligible, relayed]),
    );

    let discovered = network_info_with_rpc();
    manager.add_discovered_peer(scheduled, discovered.clone());
    let own = network_info_with_rpc();
    manager.add_self_advertised_peer(peer_id(&own), eligible, own.clone());
    let pushed = network_info();
    manager.add_self_advertised_peer(PeerId::random(), relayed, pushed.clone());
    manager.add_discovered_peer(untracked, network_info());
    let untracked_own = network_info();
    manager.add_self_advertised_peer(peer_id(&untracked_own), untracked, untracked_own);
    manager.add_discovered_peer(local, network_info());
    let local_own = network_info();
    manager.add_self_advertised_peer(peer_id(&local_own), local, local_own);

    assert!(holds(manager.tracked.record(&scheduled), &discovered), "query result retained");
    assert!(holds(manager.tracked.record(&eligible), &own), "own pushed record retained");
    assert!(holds(manager.tracked.record(&relayed), &pushed), "relayed pushed record retained");
    assert!(manager.tracked.record(&untracked).is_none(), "untracked key dropped");
    assert!(!manager.tracked.contains(&untracked));
    assert!(manager.tracked.record(&local).is_none(), "the local key is never tracked");
    assert!(manager.known_peers.is_empty(), "tracking caches nothing in the committee cache");
    assert!(manager.known_timestamps.is_empty());
}

/// A tracked record that contradicts a configured hub or the launch inventory, in either
/// direction, is refused as on the committee cache path; a pinned key keeps its committee cache
/// entry instead of a tracked record.
#[tokio::test]
async fn tracked_record_contradicting_configured_binding_is_refused() -> eyre::Result<()> {
    let mut manager = manager();
    let (hub, hub_node) = node(10);
    let (launch, launch_node) = node(11);
    manager.add_trusted_peers(BTreeMap::from([(hub, hub_node.clone())]));
    manager.seed_committee_peers(BTreeMap::from([(launch, launch_node.clone())]))?;
    let (claims_hub, claims_launch, honest) = (bls(12), bls(13), bls(14));
    manager.update_tracked_keys(
        1,
        HashSet::new(),
        HashSet::from([claims_hub, claims_launch, honest, hub, launch]),
    );

    let hub_claim = NetworkInfo { pubkey: hub_node.network_key.clone(), ..network_info() };
    manager.add_discovered_peer(claims_hub, hub_claim);
    let launch_claim = NetworkInfo { pubkey: launch_node.network_key.clone(), ..network_info() };
    manager.add_self_advertised_peer(peer_id(&launch_claim), claims_launch, launch_claim);
    let honest_info = network_info();
    manager.add_discovered_peer(honest, honest_info.clone());
    let hub_record = info_of(&hub_node);
    manager.add_discovered_peer(hub, hub_record);

    assert!(manager.tracked.record(&claims_hub).is_none(), "record claiming a hub key refused");
    assert!(
        manager.tracked.record(&claims_launch).is_none(),
        "record claiming a launch key refused"
    );
    assert!(holds(manager.tracked.record(&honest), &honest_info), "control record retained");
    assert!(manager.tracked.record(&hub).is_none(), "a pinned key is not tracked");
    assert!(manager.known_peers.contains_key(&hub) && !manager.stub_records.contains(&hub));
    assert!(!manager.known_peers.contains_key(&claims_hub));
    Ok(())
}

/// Tracked records, including a forged one claiming a window member's transport key, leave the
/// admission status, identity bindings, validator status and peer policy exactly as an otherwise
/// identical manager that tracks nothing.
#[tokio::test]
async fn tracked_records_leave_admission_and_identity_unchanged() {
    let window = ClosedWindow::new();
    let mut plain = window.manager();
    let mut tracking = window.manager();
    let (scheduled, eligible, forger) = (bls(30), bls(31), bls(32));
    tracking.update_tracked_keys(7, HashSet::from([scheduled]), HashSet::from([eligible, forger]));
    let (member, member_info) = window.members[0].clone();
    let scheduled_info = network_info();
    let eligible_info = network_info();
    let forged = NetworkInfo { pubkey: member_info.pubkey.clone(), ..network_info() };

    [&mut plain, &mut tracking].into_iter().for_each(|manager| {
        manager.add_discovered_peer(scheduled, scheduled_info.clone());
        manager.add_self_advertised_peer(peer_id(&eligible_info), eligible, eligible_info.clone());
        manager.add_discovered_peer(forger, forged.clone());
    });

    assert!(holds(tracking.tracked.record(&scheduled), &scheduled_info));
    assert!(holds(tracking.tracked.record(&eligible), &eligible_info));
    assert!(holds(tracking.tracked.record(&forger), &forged), "forged record held for export");
    assert!(plain.tracked.record(&forger).is_none());
    let status = tracking.admission_status();
    assert_eq!(format!("{:?}", plain.admission_status()), format!("{status:?}"));
    assert_eq!(status.effective(), AdmissionMode::Closed);
    assert_eq!(status.contradicted(), 0, "the forged claim contradicts nothing");
    assert_eq!(known_keys(&plain), known_keys(&tracking));

    let member_id = peer_id(&member_info);
    let ids = [member_id, peer_id(&scheduled_info), peer_id(&eligible_info), PeerId::random()];
    ids.iter().for_each(|id| {
        assert_eq!(plain.peer_to_bls(id), tracking.peer_to_bls(id), "{id}: peer_to_bls");
        assert_eq!(plain.is_peer_validator(id), tracking.is_peer_validator(id), "{id}: validator");
        assert_eq!(plain.peer_policy(id), tracking.peer_policy(id), "{id}: policy");
        assert_eq!(
            plain.check_admission(Some(id), ConnectionDirection::Inbound).is_ok(),
            tracking.check_admission(Some(id), ConnectionDirection::Inbound).is_ok(),
            "{id}: admission"
        );
    });
    [member, scheduled, eligible, forger].into_iter().for_each(|key| {
        assert_eq!(plain.auth_to_peer(key), tracking.auth_to_peer(key));
    });
    assert_eq!(tracking.peer_to_bls(&member_id), Some(member), "member keeps its binding");
    assert!(tracking.auth_to_peer(forger).is_none(), "a tracked record binds nothing");
    assert!(tracking
        .check_admission(Some(&peer_id(&scheduled_info)), ConnectionDirection::Inbound)
        .is_err());
}

/// Rotation prunes the committee cache exactly as without tracking; a learned record of a key
/// that is still tracked moves into the tracked map without its RPC endpoint, and is never
/// promoted back when the key returns to a slot.
#[tokio::test]
async fn rotation_prunes_as_before_and_demotes_tracked_records() {
    let (kept, forgotten, successor) = (bls(40), bls(41), bls(42));
    let kept_info = network_info_with_rpc();
    let forgotten_info = network_info();
    let mut plain = manager();
    let mut tracking = manager();
    tracking.update_tracked_keys(1, HashSet::new(), HashSet::from([kept]));

    [&mut plain, &mut tracking].into_iter().for_each(|manager| {
        manager.update_committees(HashSet::new(), HashSet::from([kept, forgotten]), HashSet::new());
        manager.add_discovered_peer(kept, kept_info.clone());
        manager.add_discovered_peer(forgotten, forgotten_info.clone());
    });
    assert!(tracking.known_peers.get(&kept).is_some_and(|info| info.rpc.is_some()));
    assert!(tracking.tracked.record(&kept).is_none(), "a slot member is cached, not tracked");

    [&mut plain, &mut tracking].into_iter().for_each(|manager| {
        manager.update_committees(HashSet::new(), HashSet::from([successor]), HashSet::new());
    });
    assert_eq!(known_keys(&plain), known_keys(&tracking), "pruning unchanged");
    assert!(
        !tracking.known_peers.contains_key(&kept) && !tracking.known_peers.contains_key(&forgotten)
    );
    assert_eq!(plain.stub_records, tracking.stub_records);
    assert_eq!(
        plain.known_timestamps.keys().collect::<BTreeSet<_>>(),
        tracking.known_timestamps.keys().collect::<BTreeSet<_>>()
    );
    [peer_id(&kept_info), peer_id(&forgotten_info)].iter().for_each(|id| {
        assert_eq!(plain.peer_to_bls(id), tracking.peer_to_bls(id));
        assert_eq!(plain.is_peer_validator(id), tracking.is_peer_validator(id));
        assert_eq!(plain.peer_policy(id), tracking.peer_policy(id));
    });
    assert!(holds(tracking.tracked.record(&kept), &kept_info), "demoted without its RPC");
    assert!(tracking.tracked.record(&forgotten).is_none(), "an untracked key is forgotten");

    tracking.events.clear();
    tracking.update_committees(HashSet::new(), HashSet::from([kept]), HashSet::new());
    assert!(!tracking.known_peers.contains_key(&kept), "no promotion into the committee cache");
    assert!(committee_lookups(&tracking).contains(&kept), "the returning member is looked up");
    assert!(holds(tracking.tracked.record(&kept), &kept_info));
}

/// A tracked record grants no exemption from disconnected-peer pruning: the oldest
/// disconnected peer is evicted whether or not its key is tracked.
#[tokio::test]
async fn tracked_record_grants_no_pruning_exemption() {
    let config = PeerConfig { max_disconnected_peers: 1, ..PeerConfig::default() };
    let local = PeerId::random();
    let mut plain = manager_with(local, &config);
    let mut tracking = manager_with(local, &config);
    let key = bls(45);
    let info = network_info();
    let tracked_id = peer_id(&info);
    let other_id = PeerId::random();
    tracking.update_tracked_keys(1, HashSet::new(), HashSet::from([key]));

    [&mut plain, &mut tracking].into_iter().for_each(|manager| {
        manager.add_discovered_peer(key, info.clone());
        let multiaddr = create_multiaddr(None);
        assert!(manager.register_peer_connection(
            &tracked_id,
            ConnectionType::IncomingConnection { multiaddr }
        ));
        manager.add_self_advertised_peer(tracked_id, key, info.clone());
        manager.register_disconnected(&tracked_id);
    });
    // disconnect instants come from the monotonic clock, so order the two disconnects
    tokio::time::sleep(Duration::from_millis(5)).await;
    [&mut plain, &mut tracking].into_iter().for_each(|manager| {
        let multiaddr = create_multiaddr(None);
        assert!(manager
            .register_peer_connection(&other_id, ConnectionType::IncomingConnection { multiaddr }));
        manager.register_disconnected(&other_id);
    });

    assert!(holds(tracking.tracked.record(&key), &info));
    [&plain, &tracking].into_iter().for_each(|manager| {
        assert!(manager.peers.get_peer(&tracked_id).is_none(), "oldest disconnected peer pruned");
        assert!(manager.peers.get_peer(&other_id).is_some());
    });
}

/// An older, an equally old, or a demoted older record never replaces the held tracked record;
/// a newer one replaces it at either ingest site.
#[tokio::test]
async fn tracked_record_freshness_is_monotonic() {
    let mut manager = manager();
    let key = bls(50);
    manager.update_tracked_keys(1, HashSet::new(), HashSet::from([key]));
    let base = now();
    let newer = network_info_at(base + 20);
    let older = network_info_at(base + 10);

    manager.add_discovered_peer(key, newer.clone());
    manager.add_discovered_peer(key, older.clone());
    manager.add_self_advertised_peer(peer_id(&older), key, older.clone());
    manager.add_discovered_peer(key, network_info_at(base + 20));
    assert!(holds(manager.tracked.record(&key), &newer), "older and equal records ignored");

    // the committee cache learns an older record while the key sits in a slot; demotion at
    // rotation follows the same freshness rule
    manager.update_committees(HashSet::new(), HashSet::from([key]), HashSet::new());
    manager.add_discovered_peer(key, older.clone());
    assert!(manager.known_peers.get(&key).is_some_and(|info| info.timestamp == older.timestamp));
    manager.update_committees(HashSet::new(), HashSet::from([bls(51)]), HashSet::new());
    assert!(holds(manager.tracked.record(&key), &newer), "demoted older record ignored");

    let newest = network_info_at(base + 30);
    manager.add_self_advertised_peer(PeerId::random(), key, newest.clone());
    assert!(holds(manager.tracked.record(&key), &newest), "newer pushed record replaces");
    let latest = network_info_at(base + 40);
    manager.add_discovered_peer(key, latest.clone());
    assert!(holds(manager.tracked.record(&key), &latest), "newer query result replaces");
}

/// An update for an older epoch changes nothing; an applied update drops the records of keys it
/// no longer lists, and later records for them are dropped too.
#[tokio::test]
async fn older_epoch_is_ignored_and_dropped_keys_lose_records() {
    let mut manager = manager();
    let (kept, dropped) = (bls(60), bls(61));
    let (kept_info, dropped_info) = (network_info(), network_info());
    manager.update_tracked_keys(5, HashSet::from([kept]), HashSet::from([dropped]));
    manager.add_discovered_peer(kept, kept_info.clone());
    manager.add_discovered_peer(dropped, dropped_info.clone());

    manager.update_tracked_keys(4, HashSet::new(), HashSet::new());
    assert!(holds(manager.tracked.record(&kept), &kept_info), "older epoch ignored");
    assert!(holds(manager.tracked.record(&dropped), &dropped_info));

    // a repeat of the held epoch replaces it
    manager.update_tracked_keys(5, HashSet::from([kept]), HashSet::new());
    assert!(holds(manager.tracked.record(&kept), &kept_info));
    assert!(!manager.tracked.contains(&dropped));
    assert!(manager.tracked.record(&dropped).is_none(), "dropped key loses its record");
    manager.add_discovered_peer(dropped, network_info());
    assert!(manager.tracked.record(&dropped).is_none(), "an untracked record is not retained");

    manager.update_tracked_keys(4, HashSet::new(), HashSet::from([dropped]));
    assert!(!manager.tracked.contains(&dropped), "still older than the held epoch");
}

/// A key the update no longer lists loses its lookup schedule, so tracking it again starts a
/// fresh schedule instead of inheriting the old backoff.
#[tokio::test]
async fn dropped_key_loses_its_lookup_schedule() {
    let mut tracked = TrackedKeys::default();
    let key = bls(62);
    let wanted = HashSet::from([key]);
    let start = Instant::now();
    assert_eq!(tracked.update(1, HashSet::new(), wanted.clone()), Some(0));
    assert_eq!(due(&mut tracked, &wanted, start), [key]);
    assert!(due(&mut tracked, &wanted, start + Duration::from_secs(1)).is_empty(), "backing off");

    assert_eq!(tracked.update(2, HashSet::new(), HashSet::new()), Some(0));
    assert!(due(&mut tracked, &wanted, start + Duration::from_secs(1)).is_empty(), "untracked");
    assert_eq!(tracked.update(1, HashSet::new(), wanted.clone()), None, "older epoch");
    assert_eq!(tracked.update(3, HashSet::new(), wanted.clone()), Some(0));
    assert_eq!(due(&mut tracked, &wanted, start + Duration::from_secs(1)), [key], "fresh schedule");
}

/// The scheduler picks at most the per-tick budget, never-looked-up keys first, and backs one
/// unresolved key off from 30 s, doubling to the 30 min cap; a key that stops needing a lookup
/// starts over.
#[tokio::test]
async fn tracked_lookup_budget_and_backoff() {
    assert_eq!(TRACKED_LOOKUPS_PER_HEARTBEAT, 16);
    assert_eq!(TRACKED_LOOKUP_INITIAL, Duration::from_secs(30));
    assert_eq!(TRACKED_LOOKUP_MAX, Duration::from_secs(30 * 60));

    let pool = keys(70, 40);
    let wanted: HashSet<_> = pool.iter().copied().collect();
    let mut tracked = TrackedKeys::default();
    tracked.update(1, HashSet::new(), wanted.clone());
    let start = Instant::now();
    let batches: Vec<Vec<BlsPublicKey>> =
        (0..4).map(|_| due(&mut tracked, &wanted, start)).collect();
    assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [16, 16, 8, 0]);
    let covered: HashSet<_> = batches.iter().flatten().copied().collect();
    assert_eq!(covered, wanted, "the budget covers the set across ticks without repeats");

    let key = bls(71);
    let single = HashSet::from([key]);
    let mut tracked = TrackedKeys::default();
    tracked.update(1, HashSet::new(), single.clone());
    let mut at = start;
    assert_eq!(due(&mut tracked, &single, at), [key]);
    [30, 60, 120, 240, 480, 960, 1800, 1800].into_iter().map(Duration::from_secs).for_each(
        |delay| {
            let early = at + delay - Duration::from_secs(1);
            assert!(due(&mut tracked, &single, early).is_empty(), "inside the {delay:?} backoff");
            at += delay;
            assert_eq!(due(&mut tracked, &single, at), [key], "due after {delay:?}");
        },
    );

    // a key outside the wanted set loses its schedule and starts over from the initial delay
    assert!(due(&mut tracked, &HashSet::new(), at).is_empty());
    assert_eq!(due(&mut tracked, &single, at), [key]);
    assert!(
        due(&mut tracked, &single, at + TRACKED_LOOKUP_INITIAL - Duration::from_secs(1)).is_empty()
    );
    assert_eq!(due(&mut tracked, &single, at + TRACKED_LOOKUP_INITIAL), [key]);
}

/// Each heartbeat queues one tracked lookup of at most the budget, covering every unresolved
/// tracked key across ticks, and never queries a slot member, a pinned key, or a key whose
/// retained record is still within the age limit.
#[tokio::test(start_paused = true)]
async fn heartbeat_spends_the_tracked_lookup_budget() {
    let mut manager = manager();
    manager.configure_record_max_age(Duration::from_secs(600));
    let pool = keys(80, 40);
    let (member, pinned, fresh, outdated) = (bls(81), bls(82), bls(83), bls(84));
    manager.update_committees(HashSet::new(), HashSet::from([member]), HashSet::new());
    manager.add_bootstrap_peer(pinned, network_info());
    let eligible: HashSet<_> = pool.iter().copied().chain([fresh, outdated]).collect();
    manager.update_tracked_keys(1, HashSet::from([member, pinned]), eligible);
    manager.add_discovered_peer(fresh, network_info());
    manager.add_discovered_peer(outdated, network_info_at(now() - 3_600));
    let unresolved: HashSet<_> = pool.iter().copied().chain([outdated]).collect();

    let mut ticks = Vec::new();
    for _ in 0..3 {
        let lookups = tracked_tick(&mut manager).await;
        assert_eq!(lookups.len(), 1, "one tracked lookup event per heartbeat");
        assert!(committee_lookups(&manager).is_disjoint(&unresolved));
        ticks.extend(lookups);
    }
    assert_eq!(ticks.iter().map(Vec::len).collect::<Vec<_>>(), [16, 16, 16]);
    let first: HashSet<_> = ticks[0].iter().copied().collect();
    assert!(ticks[1].iter().all(|key| !first.contains(key)), "new keys go before repeats");
    let covered: HashSet<_> = ticks.iter().flatten().copied().collect();
    assert_eq!(covered, unresolved);
}

/// One unresolved key is looked up on the heartbeat with a doubling delay, and no longer once
/// its record is retained.
#[tokio::test(start_paused = true)]
async fn heartbeat_backs_off_and_stops_once_resolved() {
    let mut manager = manager();
    assert_eq!(manager.heartbeat.period(), TRACKED_LOOKUP_INITIAL);
    let key = bls(90);
    manager.update_tracked_keys(1, HashSet::new(), HashSet::from([key]));
    let mut pattern = Vec::new();
    for _ in 0..8 {
        pattern
            .push(tracked_tick(&mut manager).await.iter().flatten().any(|looked| *looked == key));
    }
    assert_eq!(pattern, [true, true, false, true, false, false, false, true]);

    manager.add_discovered_peer(key, network_info());
    for _ in 0..4 {
        assert!(tracked_tick(&mut manager).await.is_empty(), "a resolved key is not looked up");
    }
}

/// Configured bootstrap, hub and launch entries are stubs: they are pinned in the committee
/// cache, never copied or demoted into the tracked map, and never queried as tracked keys.
#[tokio::test(start_paused = true)]
async fn configured_stub_is_never_a_tracked_record() -> eyre::Result<()> {
    let mut manager = manager();
    let (boot, boot_node) = node(130);
    let (hub, hub_node) = node(131);
    let (launch, launch_node) = node(132);
    manager.add_bootstrap_peer(boot, info_of(&boot_node));
    manager.add_trusted_peers(BTreeMap::from([(hub, hub_node)]));
    manager.seed_committee_peers(BTreeMap::from([(launch, launch_node)]))?;
    let stubs = HashSet::from([boot, hub, launch]);
    assert!(stubs.iter().all(|key| manager.stub_records.contains(key)));
    manager.update_tracked_keys(1, HashSet::new(), stubs.clone());

    manager.update_committees(HashSet::new(), stubs.clone(), HashSet::new());
    manager.update_committees(HashSet::new(), HashSet::from([bls(133)]), HashSet::new());
    assert!(stubs.iter().all(|key| {
        manager.stub_records.contains(key) && manager.tracked.record(key).is_none()
    }));
    for _ in 0..2 {
        assert!(tracked_tick(&mut manager).await.is_empty(), "stubs are not tracked lookups");
    }
    Ok(())
}

/// Past the cap, scheduled keys are kept first and eligible keys fill the room in key order,
/// independent of hash order; an oversized scheduled set keeps its lowest keys.
#[tokio::test]
async fn cap_keeps_scheduled_first_then_eligible_in_key_order() {
    let mut pool = keys(110, MAX_TRACKED_KEYS + 76);
    pool.sort_unstable();
    // the largest keys are scheduled, so key order alone would leave them out
    let (rest, largest) = pool.split_at(pool.len() - 10);
    let scheduled: HashSet<_> = largest.iter().copied().collect();
    let eligible: HashSet<_> = pool.iter().copied().collect();
    let mut tracked = TrackedKeys::default();
    assert_eq!(tracked.update(1, scheduled.clone(), eligible.clone()), Some(76));
    assert_eq!(tracked.keys().count(), MAX_TRACKED_KEYS);
    assert!(scheduled.iter().all(|key| tracked.contains(key)));
    let (kept, left_out) = rest.split_at(MAX_TRACKED_KEYS - scheduled.len());
    assert!(kept.iter().all(|key| tracked.contains(key)));
    assert!(left_out.iter().all(|key| !tracked.contains(key)));

    let mut reordered = TrackedKeys::default();
    let reversed = || pool.iter().rev().copied();
    reordered.update(1, reversed().take(10).collect(), reversed().collect());
    assert_eq!(
        tracked.keys().collect::<BTreeSet<_>>(),
        reordered.keys().collect::<BTreeSet<_>>(),
        "every node keeps the same subset of the same pool"
    );

    assert_eq!(tracked.update(2, eligible, HashSet::new()), Some(76));
    assert!(pool[..MAX_TRACKED_KEYS].iter().all(|key| tracked.contains(key)));
    assert!(pool[MAX_TRACKED_KEYS..].iter().all(|key| !tracked.contains(key)));
}

/// An oversized publication warns once and leaves the excess out: its records are dropped and a
/// later publication that fits replaces it without another warning.
#[tokio::test]
async fn tracked_key_cap_warns_and_drops_the_excess() {
    let capture = Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.clone());
    let mut manager = manager();
    let mut pool = keys(120, MAX_TRACKED_KEYS + 8);
    pool.sort_unstable();
    let (kept, left_out) = pool.split_at(MAX_TRACKED_KEYS);

    manager.update_tracked_keys(1, HashSet::new(), pool.iter().copied().collect());
    assert_eq!(capture.message_count("tracked key limit reached"), 1);
    assert!(kept.iter().all(|key| manager.tracked.contains(key)));
    assert!(left_out.iter().all(|key| !manager.tracked.contains(key)));
    manager.add_discovered_peer(left_out[0], network_info());
    assert!(manager.tracked.record(&left_out[0]).is_none(), "a left-out key is not retained");
    let kept_info = network_info();
    manager.add_discovered_peer(kept[0], kept_info.clone());
    assert!(holds(manager.tracked.record(&kept[0]), &kept_info));

    manager.update_tracked_keys(2, HashSet::new(), left_out.iter().copied().collect());
    assert_eq!(capture.message_count("tracked key limit reached"), 1, "a fitting set is quiet");
    assert!(left_out.iter().all(|key| manager.tracked.contains(key)));
    assert!(manager.tracked.record(&kept[0]).is_none(), "the replaced set's records are dropped");
}
