//! The swarm's kad store follows the peer manager's tracked keys, not the raw command: an update
//! the peer manager ignores for an older epoch leaves the store untouched, a key the peer
//! manager's cap leaves out gets no store row, and the keys past the store's headroom are left out
//! in the cap's priority order and counted in one warning, without failing the command or the
//! peer manager's update. Across a restart, a persisted tracked row binds its peer id only until
//! the first committee update.
//!
//! The command handler is private to the swarm, so these tests sit beside it. The store is probed
//! through its record API: `put` lands a row only for a key some owner retains, and `get` serves
//! only retained rows.

use super::*;
use crate::{
    common::{TestWorkerRequest, TestWorkerResponse},
    EntrySource, TrackedSlot,
};
use eyre::eyre;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
};
use tn_config::ConsensusConfig;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{BlsKeypair, TaskManager};
use tracing::{
    field::{Field, Visit},
    span, Metadata, Subscriber,
};

/// The fixed text of the handler's warning about keys past the store's headroom.
const HEADROOM_WARNING: &str = "kad store headroom reached";

/// The events a test swarm reports to the application.
type TestEvent = NetworkEvent<TestWorkerRequest, TestWorkerResponse>;

/// The swarm under test.
type TestNetwork =
    ConsensusNetwork<TestWorkerRequest, TestWorkerResponse, MemDatabase, Sender<TestEvent>>;

/// `count` deterministic BLS keys drawn from `seed`.
fn keys(seed: u8, count: usize) -> Vec<BlsPublicKey> {
    let mut rng = StdRng::from_seed([seed; 32]);
    (0..count).map(|_| *BlsKeypair::generate(&mut rng).public()).collect()
}

/// A row under `key`'s discovery record key; the store does not verify record values.
fn row(key: &BlsPublicKey) -> kad::Record {
    kad::Record { key: node_record_key(key), value: vec![7, 8, 9], publisher: None, expires: None }
}

/// The consensus config of the first authority of a fresh fixture committee.
fn authority_config() -> eyre::Result<ConsensusConfig<MemDatabase>> {
    let committee = CommitteeFixture::builder(MemDatabase::default).build();
    let authority =
        committee.authorities().next().ok_or_else(|| eyre!("fixture has an authority"))?;
    Ok(authority.consensus_config())
}

/// A primary swarm, with the event receiver and task manager it needs kept alive.
struct Fixture {
    /// The swarm under test.
    network: TestNetwork,
    /// Keeps the application event channel open.
    _events: Receiver<TestEvent>,
    /// Owns the tasks the swarm spawned.
    _tasks: TaskManager,
}

impl Fixture {
    /// Build the first fixture authority's primary swarm.
    fn new() -> eyre::Result<Self> {
        Self::start(&authority_config()?, MemDatabase::default())
    }

    /// Build `config`'s primary swarm over `db`, as a node start does.
    fn start(config: &ConsensusConfig<MemDatabase>, db: MemDatabase) -> eyre::Result<Self> {
        let (sender, events) = tokio::sync::mpsc::channel(10);
        let tasks = TaskManager::default();
        let network = ConsensusNetwork::new(
            config.network_config(),
            sender,
            config.key_config().clone(),
            config.key_config().primary_network_keypair().clone(),
            db,
            tasks.get_spawner(),
            NetworkType::Primary,
            config.primary_address(),
            None,
        )?;
        Ok(Self { network, _events: events, _tasks: tasks })
    }

    /// Hand one tracked-keys command to the swarm handler.
    fn update(
        &mut self,
        epoch: u64,
        scheduled: &[BlsPublicKey],
        eligible: &[BlsPublicKey],
    ) -> NetworkResult<()> {
        self.network.process_command(NetworkCommand::UpdateTrackedKeys {
            epoch,
            scheduled: scheduled.iter().copied().collect(),
            eligible: eligible.iter().copied().collect(),
        })
    }

    /// The tracked keys the peer manager accepted.
    fn tracked(&self) -> HashSet<BlsPublicKey> {
        self.network.swarm.behaviour().peer_manager.tracked_keys().copied().collect()
    }

    /// The swarm's kad store.
    fn store(&mut self) -> &mut KadStore<MemDatabase> {
        self.network.swarm.behaviour_mut().kademlia.store_mut()
    }

    /// Whether the store admits a row for `key`, which it does only while an owner retains it.
    fn admits(&mut self, key: &BlsPublicKey) -> bool {
        self.store().put(row(key)).is_ok()
    }

    /// Whether the store serves a row for `key`.
    fn serves(&mut self, key: &BlsPublicKey) -> bool {
        self.store().get(&node_record_key(key)).is_some()
    }

    /// Whether the peer manager's known-peer cache holds `key`.
    fn known(&self, key: BlsPublicKey) -> bool {
        self.network.swarm.behaviour().peer_manager.auth_to_peer(key).is_some()
    }

    /// The BLS key the peer store binds `peer_id` to.
    fn bound(&self, peer_id: &PeerId) -> Option<BlsPublicKey> {
        self.network.swarm.behaviour().peer_manager.peer_to_bls(peer_id)
    }

    /// Install a committee window whose only member is `member`.
    fn seat(&mut self, member: BlsPublicKey) -> NetworkResult<()> {
        self.network.process_command(NetworkCommand::UpdateCommittees {
            previous: HashSet::new(),
            current: HashSet::from([member]),
            next: HashSet::new(),
        })
    }
}

/// A validator outside the node's committee, with the signed record it publishes.
struct Eligible {
    /// The validator's BLS key.
    bls: BlsPublicKey,
    /// The peer id of the validator's primary network key.
    peer_id: PeerId,
    /// The validator's signed primary record, as a kad row.
    row: kad::Record,
}

impl Eligible {
    /// A validator from another fixture committee, with its record signed for `config`'s chain.
    fn new(config: &ConsensusConfig<MemDatabase>) -> eyre::Result<Self> {
        let validator = authority_config()?;
        let keys = validator.key_config();
        let bls = keys.primary_public_key();
        let peer_id = PeerId::from(keys.primary_network_public_key());
        let chain_id = config.network_config().libp2p_config().chain_id;
        let record = NodeRecord::build(
            RecordDomain::new(chain_id, NetworkType::Primary),
            &bls,
            keys.primary_network_keypair(),
            validator.primary_address(),
            None,
            |data| keys.request_signature_direct(data),
        )?;
        let row = kad::Record {
            key: node_record_key(&bls),
            value: encode(&record),
            publisher: Some(peer_id),
            expires: None,
        };
        Ok(Self { bls, peer_id, row })
    }
}

/// Run a node that tracks `eligible` and stores the row a relay puts for it, then start the node
/// again over the same database, before any committee or tracked update.
fn restart_with_a_tracked_row(
    config: &ConsensusConfig<MemDatabase>,
    eligible: &Eligible,
) -> eyre::Result<Fixture> {
    let db = MemDatabase::default();
    let mut previous_run = Fixture::start(config, db.clone())?;
    previous_run.update(1, &[], &[eligible.bls])?;
    previous_run.network.process_kad_put_request(PeerId::random(), eligible.row.clone())?;
    assert!(previous_run.serves(&eligible.bls), "the tracked row is stored");
    assert!(!previous_run.known(eligible.bls), "the live path keeps it out of the cache");
    assert_eq!(previous_run.bound(&eligible.peer_id), None, "and binds nothing");
    drop(previous_run);
    Fixture::start(config, db)
}

/// An update the peer manager ignores for an older epoch leaves the store's tracked rows as they
/// were, and a repeat of the held epoch replaces them in both.
#[tokio::test]
async fn older_tracked_update_leaves_the_store_untouched() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let pool = keys(1, 2);
    let (held, stale) = (pool[0], pool[1]);
    fixture.update(5, &[], &[held])?;
    assert!(fixture.admits(&held), "an accepted update reaches the store");

    fixture.update(4, &[stale], &[])?;
    assert_eq!(fixture.tracked(), HashSet::from([held]), "the peer manager ignores it");
    assert!(fixture.serves(&held), "the ignored update prunes no store row");
    assert!(!fixture.admits(&stale), "the ignored update tracks no store key");

    fixture.update(5, &[], &[stale])?;
    assert_eq!(fixture.tracked(), HashSet::from([stale]));
    assert!(!fixture.serves(&held), "a repeat of the held epoch releases the replaced key");
    assert!(fixture.admits(&stale));
    Ok(())
}

/// The store tracks exactly the keys the peer manager kept under its tracked key cap, so a key
/// the cap left out gets no store row.
#[tokio::test]
async fn store_tracks_only_the_keys_the_peer_manager_kept() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    // an isolated allowance above the cap, so only the peer manager's cap can leave keys out
    fixture.store().set_retention_budget_for_test(4096)?;
    // more keys than the peer manager tracks
    let pool = keys(2, 1100);
    fixture.update(1, &[], &pool)?;

    let kept = fixture.tracked();
    let left_out: Vec<_> = pool.iter().filter(|key| !kept.contains(key)).collect();
    assert!(!left_out.is_empty(), "the pool exceeds the tracked key cap");
    assert!(left_out.iter().all(|key| !fixture.admits(key)), "a key past the cap is not retained");
    assert!(kept.iter().all(|key| fixture.admits(key)), "every kept key is retained");
    Ok(())
}

/// Keys past the store's headroom are left out in the peer manager's cap order, scheduled keys
/// first and then eligible keys in key order, and one warning names how many. Neither the command
/// nor the peer manager's update fails, and a later update that fits logs nothing.
#[tokio::test]
async fn tracked_store_keeps_the_leading_keys_and_logs_the_count_left_out() -> eyre::Result<()> {
    let warnings = HeadroomWarnings::default();
    let _subscriber = tracing::subscriber::set_default(warnings.clone());
    let mut fixture = Fixture::new()?;
    // the own key holds one of eight slots, which leaves seven for tracked keys
    fixture.store().set_retention_budget_for_test(8)?;
    let mut pool = keys(3, 12);
    pool.sort_unstable();
    // the highest key is the scheduled one, so key order alone would leave it out
    let (eligible, scheduled) = pool.split_at(11);
    fixture.update(1, scheduled, eligible)?;

    assert_eq!(fixture.tracked().len(), 12, "the peer manager keeps every key");
    let (kept, left_out) = eligible.split_at(6);
    assert!(fixture.admits(&scheduled[0]), "the scheduled key is kept first");
    assert!(kept.iter().all(|key| fixture.admits(key)), "then the lowest eligible keys");
    assert!(left_out.iter().all(|key| !fixture.admits(key)), "the rest is left out");
    assert_eq!(warnings.left_out(), ["5"], "one warning names the count left out");

    fixture.update(2, &[], kept)?;
    assert_eq!(warnings.left_out(), ["5"], "an update that fits logs nothing");
    Ok(())
}

/// The startup restore caches a persisted tracked row and binds its peer id like any restored
/// row, before any committee is known. The first committee update that leaves the key in no slot
/// drops the cache entry and the binding together, and tracking the key again binds nothing.
#[tokio::test]
async fn restored_tracked_row_is_unbound_by_the_first_committee_update() -> eyre::Result<()> {
    let config = authority_config()?;
    let eligible = Eligible::new(&config)?;
    let mut fixture = restart_with_a_tracked_row(&config, &eligible)?;
    assert!(fixture.known(eligible.bls), "the restore caches the row");
    assert_eq!(fixture.bound(&eligible.peer_id), Some(eligible.bls), "and binds its peer id");

    fixture.seat(config.key_config().primary_public_key())?;
    assert!(!fixture.known(eligible.bls), "the committee update drops the cache entry");
    assert_eq!(fixture.bound(&eligible.peer_id), None, "and the binding with it");

    fixture.update(2, &[], &[eligible.bls])?;
    assert_eq!(fixture.tracked(), HashSet::from([eligible.bls]));
    assert_eq!(fixture.bound(&eligible.peer_id), None, "tracking the key again binds nothing");
    Ok(())
}

/// A tracked update that lists a restored key before the first committee update keeps its row
/// and, at that update, its record in the tracked map for export, while the binding goes.
#[tokio::test]
async fn restored_row_tracked_before_the_first_committee_update_is_exported_unbound(
) -> eyre::Result<()> {
    let config = authority_config()?;
    let eligible = Eligible::new(&config)?;
    let mut fixture = restart_with_a_tracked_row(&config, &eligible)?;
    fixture.update(2, &[], &[eligible.bls])?;
    assert_eq!(fixture.bound(&eligible.peer_id), Some(eligible.bls), "bound until the update");

    fixture.seat(config.key_config().primary_public_key())?;
    assert!(!fixture.known(eligible.bls));
    assert_eq!(fixture.bound(&eligible.peer_id), None, "the committee update unbinds it");
    assert!(fixture.serves(&eligible.bls), "the tracked row stays served");
    let snapshot = fixture.network.swarm.behaviour().peer_manager.admission_snapshot();
    let listed = snapshot
        .entries
        .iter()
        .find(|entry| entry.bls_pubkey == eligible.bls)
        .ok_or_else(|| eyre!("the snapshot lists the tracked key"))?;
    assert_eq!(listed.slots, vec![TrackedSlot::Eligible]);
    assert_eq!(listed.source, EntrySource::Record, "the record moved into the tracked map");
    assert_eq!(listed.peer_id, Some(eligible.peer_id));
    assert!(!listed.admitted);
    Ok(())
}

/// A tracing sink that keeps the `left_out` field of each headroom warning.
#[derive(Clone, Debug, Default)]
struct HeadroomWarnings(Arc<Mutex<Vec<String>>>);

impl HeadroomWarnings {
    /// The `left_out` value of every headroom warning, in emission order.
    fn left_out(&self) -> Vec<String> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl Subscriber for HeadroomWarnings {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _attributes: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }
    fn record(&self, _span: &span::Id, _values: &span::Record<'_>) {}
    fn record_follows_from(&self, _span: &span::Id, _follows: &span::Id) {}
    fn enter(&self, _span: &span::Id) {}
    fn exit(&self, _span: &span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = WarningFields::default();
        event.record(&mut fields);
        if fields.message.contains(HEADROOM_WARNING) {
            let left_out = fields.left_out.unwrap_or_default();
            self.0.lock().unwrap_or_else(PoisonError::into_inner).push(left_out);
        }
    }
}

/// The message and `left_out` fields of one event.
#[derive(Debug, Default)]
struct WarningFields {
    /// The event's message text.
    message: String,
    /// The `left_out` field, if the event has one.
    left_out: Option<String>,
}

impl Visit for WarningFields {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "left_out" => self.left_out = Some(format!("{value:?}")),
            _ => {}
        }
    }
}
