//! Module with kademlia specific extensions, like a persistant store.

use crate::{consensus::MAX_ADVERTISED_MULTIADDRS, types::NetworkType};
use libp2p::{
    kad::{
        store::{Error, MemoryStoreConfig, RecordStore},
        ProviderRecord, Record, RecordKey,
    },
    Multiaddr, PeerId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use std::{
    borrow::Cow,
    fmt, iter,
    time::{Duration, Instant, SystemTime},
};
use tn_config::KeyConfig;
use tn_storage::tables::{
    KadProviderRecords, KadRecords, KadWorkerProviderRecords, KadWorkerRecords,
};
use tn_types::{encode, try_decode, BlockHash, Database, DefaultHashFunction};
use tracing::{error, warn};

/// A record stored in the DHT.
/// This is a "shadow" struct for a kad Record so we can serialize/deserialize
/// for peristant storage.
#[serde_as]
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct KadRecord {
    /// Key of the record.
    #[serde_as(as = "RecordKeySerde")]
    key: RecordKey,
    /// Value of the record.
    value: Vec<u8>,
    /// The (original) publisher of the record.
    publisher: Option<PeerId>,
    /// The expiration time as measured by the system clock.
    /// The original kad Record uses an Instant here but that can not
    /// be serialized or deserialized so we use SystemTime here which
    /// should be "good enough" even if lacking in precision.
    expires: Option<SystemTime>,
}

impl fmt::Debug for KadRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = bs58::encode(&self.key).into_string();
        let value = bs58::encode(&self.value).into_string();
        write!(
            f,
            "KadRecord {{ key: {key}, value: {value}, publisher: {:?}, expires: {:?} }}",
            self.publisher, self.expires
        )
    }
}

impl KadRecord {
    /// Returns true if the record carries an expiry that has already passed.
    fn is_expired(&self, now: SystemTime) -> bool {
        matches!(self.expires, Some(exp) if exp <= now)
    }
}

impl KadProviderRecord {
    /// Returns true if the provider record carries an expiry that has already passed.
    fn is_expired(&self, now: SystemTime) -> bool {
        matches!(self.expires, Some(exp) if exp <= now)
    }
}

impl From<Record> for KadRecord {
    fn from(value: Record) -> Self {
        let expires = instant_to_system(&value.expires);

        Self { key: value.key, value: value.value, publisher: value.publisher, expires }
    }
}

impl From<KadRecord> for Record {
    fn from(value: KadRecord) -> Self {
        let expires = system_to_instant(&value.expires);
        Self { key: value.key, value: value.value, publisher: value.publisher, expires }
    }
}

/// A record stored in the DHT whose value is the ID of a peer
/// who can provide the value on-demand.
/// This is a "shadow" struct for a kad ProviderRecord so we can serialize/deserialize
/// for peristant storage.
#[serde_as]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KadProviderRecord {
    /// The key whose value is provided by the provider.
    #[serde_as(as = "RecordKeySerde")]
    key: RecordKey,
    /// The provider of the value for the key.
    provider: PeerId,
    /// The expiration time as measured by the system clock.
    /// The original kad Record uses an Instant here but that can not
    /// be serialized or deserialized so we use SystemTime here which
    /// should be "good enough" even if lacking in precision.
    expires: Option<SystemTime>,
    /// The known addresses that the provider may be listening on.
    addresses: Vec<Multiaddr>,
}

impl From<ProviderRecord> for KadProviderRecord {
    fn from(value: ProviderRecord) -> Self {
        let expires = instant_to_system(&value.expires);

        Self { key: value.key, provider: value.provider, expires, addresses: value.addresses }
    }
}

impl From<KadProviderRecord> for ProviderRecord {
    fn from(value: KadProviderRecord) -> Self {
        let expires = system_to_instant(&value.expires);
        Self { key: value.key, provider: value.provider, expires, addresses: value.addresses }
    }
}

/// Have to crudely convert back from a SystemTime to Instant to create a Record.
fn system_to_instant(expires: &Option<SystemTime>) -> Option<Instant> {
    if let Some(expires) = expires {
        let (system_now, now) = (SystemTime::now(), Instant::now());
        // This is sloppy and imprecise to work around a raw Instant being in a kad Record
        // so just ignore an error.
        let expires = *expires;
        if expires > system_now {
            if let Ok(duration) = expires.duration_since(system_now) {
                Some(now + duration)
            } else {
                None
            }
        } else if let Ok(duration) = system_now.duration_since(expires) {
            Some(now - duration)
        } else {
            None
        }
    } else {
        None
    }
}

/// The kad Record contains an Instant which can not be serialized or deserialized.
/// We crudely convert to a SystemTime which can.  Note this can be inacurate with
/// time change, clock drift, etc but it probably the best we can do to store a record
/// given it contains an Instant...
fn instant_to_system(expires: &Option<Instant>) -> Option<SystemTime> {
    if let Some(expires) = expires {
        let (system_now, now) = (SystemTime::now(), Instant::now());
        let expires = *expires;
        if expires > now {
            Some(system_now + (expires - now))
        } else {
            Some(system_now - (now - expires))
        }
    } else {
        None
    }
}

/// Decode a stored provider-record blob, tolerating bytes that no longer decode.
///
/// Provider envelopes contain separately encoded `Vec<KadProviderRecord>` values. A schema skew
/// across a restart, or on-disk corruption, can leave a row whose bytes the current
/// software can no longer decode. The panicking [`tn_types::decode`] would turn that single bad
/// row into a crash of the whole `ConsensusNetwork` task on the first provider read
/// after restart, and because the row is never purged the crash recurs on every
/// restart. This returns `None` (logging a warning) instead, so the caller can skip
/// the row on a read-only path or purge it on a mutating one. This is the provider-table
/// analogue of the tolerant startup load the `KadRecords` table already gets in
/// `consensus.rs` (issue #999).
fn decode_providers(key: &BlockHash, raw: &[u8]) -> Option<Vec<KadProviderRecord>> {
    try_decode::<Vec<KadProviderRecord>>(raw)
        .inspect_err(|error| {
            warn!(target: "network-kad", ?error, ?key, "skipping undecodable provider record");
        })
        .ok()
}

/// Provider row ownership stays readable when its separately encoded payload is corrupt.
///
/// The v2 column families discard the old unscoped format. Keeping the discovery key outside
/// the provider vector lets a worker count and scrub its own malformed or empty payloads without
/// touching siblings. An undecodable envelope has unknown ownership and is left untouched by
/// scans; a direct operation on its namespaced key can replace or remove it.
#[serde_as]
#[derive(Serialize, Deserialize)]
struct KadProviderRow {
    /// Discovery key used to rederive this row's namespaced database hash.
    #[serde_as(as = "RecordKeySerde")]
    key: RecordKey,
    /// Separately encoded provider vector, validated after ownership is established.
    records: Vec<u8>,
}

impl KadProviderRow {
    /// Encode a provider set while preserving independently decodable row ownership.
    fn encode(key: RecordKey, records: &[KadProviderRecord]) -> Vec<u8> {
        encode(&Self { key, records: encode(&records) })
    }

    /// Decode a provider set only when every member belongs to this row's discovery key.
    fn decode_records(&self, hash: &BlockHash) -> Option<Vec<KadProviderRecord>> {
        decode_providers(hash, &self.records)
            .filter(|records| records.iter().all(|record| record.key == self.key))
    }
}

/// Minimum spacing between saturated-table provider eviction scans.
///
/// Provider records carry a 48h TTL, so once `num_providers` reaches
/// `max_provided_keys` nothing expires and each new-key `add_provider` would
/// otherwise trigger a full O(`max_provided_keys`) decode scan that frees
/// nothing (GHSA-5475-xf29-3rv8). Throttling the scan to at most once per this
/// interval bounds that cost without losing any eviction a 48h TTL could yield.
const PROVIDER_EVICT_INTERVAL: Duration = Duration::from_secs(60);

/// Provide a persistant store for kademlia data.
/// Wraps around the consensus DB.
#[derive(Clone, Debug)]
pub struct KadStore<DB> {
    /// Shared database containing all swarm namespaces.
    db: DB,
    /// Discovery key under which this node publishes its provider record.
    node_key: RecordKey,
    /// This node's libp2p peer id.
    ///
    /// Used by [`RecordStore::provided`] to enumerate only the provider records
    /// the node itself authored (via `start_providing`), mirroring upstream
    /// `MemoryStore`'s `local_id`. Provider records supplied by inbound peers are
    /// never re-announced as our own. See issue #1001.
    local_peer_id: PeerId,
    /// Provide some sanity defaults for store sizing.
    /// Not bothering to expose these as knobs currenty since they are
    /// basically just here to prevent or mitigate attacks on the Kad store.
    /// Use the same settings as a Kad Memery store.
    config: MemoryStoreConfig,
    /// Number of persisted discovery records owned by this swarm, including expired rows.
    num_records: usize,
    /// Number of provider rows with a readable ownership envelope belonging to this swarm.
    num_providers: usize,
    /// Last time the saturated-table provider eviction scan ran, so a full table
    /// cannot be turned into a full-table decode scan per inbound `AddProvider`.
    /// `None` until the first scan. See [`PROVIDER_EVICT_INTERVAL`].
    last_provider_evict: Option<Instant>,
    /// Index used for database retrieval with multiple KAD tables.
    kad_type: NetworkType,
}

impl<DB: Database> KadStore<DB> {
    /// Create a new KadStore backed by db.
    ///
    /// `local_peer_id` is this node's libp2p peer id (the identity the swarm is
    /// built with); it must match the id libp2p stamps on records the node
    /// publishes via `start_providing`, so [`RecordStore::provided`] re-announces
    /// only the node's own provider records.
    pub fn new(
        db: DB,
        local_peer_id: PeerId,
        key_config: &KeyConfig,
        kad_type: NetworkType,
    ) -> Self {
        let node_key = RecordKey::new(&encode(&key_config.primary_public_key()));
        // Defaults for sanity.
        let config = MemoryStoreConfig::default();
        let mut store = Self {
            db,
            node_key,
            local_peer_id,
            config,
            num_records: 0,
            num_providers: 0,
            last_provider_evict: None,
            kad_type,
        };
        store.num_records = store.owned_records().count();
        store.num_providers = store.owned_provider_rows().count();
        metrics::describe_counter!(
            "tn_network.kad_provider_write_failures_total",
            metrics::Unit::Count,
            "Database insert failures while storing Kademlia provider records"
        );
        store.update_records_gauge();
        store
    }

    /// Mirror `num_records` into the prometheus gauge.
    fn update_records_gauge(&self) {
        metrics::gauge!(
            "tn_network.kad_records",
            "network" => crate::metrics::network_label(&self.kad_type),
        )
        .set(self.num_records as f64);
    }

    /// Count a failed provider-table insert using the bounded network label.
    fn record_provider_write_failure(&self) {
        metrics::counter!(
            "tn_network.kad_provider_write_failures_total",
            "network" => crate::metrics::network_label(&self.kad_type),
        )
        .increment(1);
    }

    /// Namespace shared discovery tables by the same role and worker id as `RecordDomain`.
    fn key_to_hash(&self, key: &RecordKey) -> BlockHash {
        let (role, worker_id): (u8, tn_types::WorkerId) = match self.kad_type {
            NetworkType::Primary => (0, 0),
            NetworkType::Worker(id) => (1, id),
        };
        let mut h = DefaultHashFunction::new();
        h.update(&[role]);
        h.update(&worker_id.to_le_bytes());
        h.update(encode(key).as_ref());
        BlockHash::from_slice(h.finalize().as_bytes())
    }

    /// Whether a persisted discovery key rederives this store's row hash.
    fn owns(&self, key: &RecordKey, hash: &BlockHash) -> bool {
        self.key_to_hash(key) == *hash
    }

    /// Decode a discovery row only when its persisted key matches this store's namespace.
    fn decode_record(&self, hash: &BlockHash, raw: &[u8]) -> Option<KadRecord> {
        try_decode::<KadRecord>(raw).ok().filter(|record| self.owns(&record.key, hash))
    }

    /// Enumerate this swarm's rows, including expired records needed for accounting.
    fn owned_records(&self) -> impl Iterator<Item = (BlockHash, KadRecord)> + '_ {
        let rows = match self.kad_type {
            NetworkType::Primary => self.db.iter::<KadRecords>(),
            NetworkType::Worker(_) => self.db.iter::<KadWorkerRecords>(),
        };
        rows.filter_map(move |(hash, raw)| {
            self.decode_record(&hash, &raw).map(|record| (hash, record))
        })
    }

    /// Read row ownership before inspecting provider payloads from a shared table.
    fn decode_provider_row(&self, hash: &BlockHash, raw: &[u8]) -> Option<KadProviderRow> {
        try_decode::<KadProviderRow>(raw).ok().filter(|row| self.owns(&row.key, hash))
    }

    /// Enumerate only this swarm's provider rows, even when their payloads are malformed.
    fn owned_provider_rows(&self) -> impl Iterator<Item = (BlockHash, KadProviderRow)> + '_ {
        let rows = match self.kad_type {
            NetworkType::Primary => self.db.iter::<KadProviderRecords>(),
            NetworkType::Worker(_) => self.db.iter::<KadWorkerProviderRecords>(),
        };
        rows.filter_map(move |(hash, raw)| {
            self.decode_provider_row(&hash, &raw).map(|row| (hash, row))
        })
    }

    /// Scan the records table and remove any rows whose expiry has passed.
    /// Returns the number of rows actually removed and updates `num_records`.
    fn evict_expired_records(&mut self) -> usize {
        let now = SystemTime::now();
        let expired_keys: Vec<BlockHash> = self
            .owned_records()
            .filter_map(|(hash, record)| record.is_expired(now).then_some(hash))
            .collect();
        let evicted = expired_keys
            .iter()
            .filter(|k| match self.kad_type {
                NetworkType::Primary => self.db.remove::<KadRecords>(k).is_ok(),
                NetworkType::Worker(_) => self.db.remove::<KadWorkerRecords>(k).is_ok(),
            })
            .count();
        self.num_records = self.num_records.saturating_sub(evicted);
        self.update_records_gauge();
        evicted
    }

    /// Scan provider records and drop any key whose entire `Vec<KadProviderRecord>` has expired.
    /// Returns the number of keys removed and updates `num_providers`.
    fn evict_expired_providers(&mut self) -> usize {
        let now = SystemTime::now();
        let drop_keys: Vec<BlockHash> = self
            .owned_provider_rows()
            .filter_map(|(hash, row)| {
                // Ownership is known even for empty or malformed provider payloads.
                row.decode_records(&hash)
                    .is_none_or(|records| records.iter().all(|record| record.is_expired(now)))
                    .then_some(hash)
            })
            .collect();
        let evicted = drop_keys
            .iter()
            .filter(|k| match self.kad_type {
                NetworkType::Primary => self.db.remove::<KadProviderRecords>(k).is_ok(),
                NetworkType::Worker(_) => self.db.remove::<KadWorkerProviderRecords>(k).is_ok(),
            })
            .count();
        self.num_providers = self.num_providers.saturating_sub(evicted);
        evicted
    }

    /// Purge provider-table rows whose stored bytes no longer decode, giving the provider
    /// tables the tolerant startup load the `KadRecords` table already gets in
    /// `consensus.rs`. Without this, a schema/version skew or on-disk corruption leaves a
    /// row that panics the whole `ConsensusNetwork` task on the first provider read after
    /// restart, and because the row is never purged the panic recurs on every restart.
    /// Only rows with a readable ownership envelope are considered; unknown ownership is never
    /// guessed. Returns the number of rows removed and keeps `num_providers` in step (issue #999).
    pub fn scrub_corrupt_providers(&mut self) -> usize {
        let corrupt: Vec<BlockHash> = self
            .owned_provider_rows()
            .filter_map(|(hash, row)| row.decode_records(&hash).is_none().then_some(hash))
            .collect();
        let evicted = corrupt
            .iter()
            .filter(|k| match self.kad_type {
                NetworkType::Primary => self.db.remove::<KadProviderRecords>(k).is_ok(),
                NetworkType::Worker(_) => self.db.remove::<KadWorkerProviderRecords>(k).is_ok(),
            })
            .count();
        self.num_providers = self.num_providers.saturating_sub(evicted);
        evicted
    }

    /// Merge a new provider record into an existing, already-decoded set for one key:
    /// replace the entry that shares the provider peer if present, otherwise prune expired
    /// entries, enforce the per-key cap, and append. Keyword-free analogue of the in-place
    /// update loop, factored out so the caller can decide what to do when the stored row
    /// does not decode.
    fn merge_provider(
        &self,
        existing: Vec<KadProviderRecord>,
        kr: KadProviderRecord,
    ) -> libp2p::kad::store::Result<Vec<KadProviderRecord>> {
        let found = existing.iter().any(|r| r.provider == kr.provider);
        if found {
            Ok(existing
                .into_iter()
                .map(|r| if r.provider == kr.provider { kr.clone() } else { r })
                .collect())
        } else {
            let now = SystemTime::now();
            let mut pruned: Vec<KadProviderRecord> =
                existing.into_iter().filter(|r| !r.is_expired(now)).collect();
            (pruned.len() < self.config.max_providers_per_key)
                .then_some(())
                .ok_or(Error::MaxProvidedKeys)?;
            pruned.push(kr);
            Ok(pruned)
        }
    }
}

/// Iterator of KAD records.
pub struct RecordIter<'a> {
    /// Decoded rows whose hashes match the originating store's namespace.
    iter: Box<dyn Iterator<Item = (BlockHash, KadRecord)> + 'a>,
}

impl<'a> std::fmt::Debug for RecordIter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Record Iterator")
    }
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Cow<'a, Record>;

    fn next(&mut self) -> Option<Self::Item> {
        let now = SystemTime::now();
        self.iter
            .find(|(_, record)| !record.is_expired(now))
            .map(|(_, record)| Cow::Owned(record.into()))
    }
}

impl<DB: Database> RecordStore for KadStore<DB> {
    type RecordsIter<'a> = RecordIter<'a>;

    type ProvidedIter<'a> = iter::Map<
        std::vec::IntoIter<ProviderRecord>,
        fn(ProviderRecord) -> Cow<'a, ProviderRecord>,
    >;

    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        let key = self.key_to_hash(k);
        let record = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadRecords>(&key),
            NetworkType::Worker(_) => self.db.get::<KadWorkerRecords>(&key),
        }
        .inspect_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to read Kademlia record");
        })
        .ok()?;
        let raw = record?;
        try_decode::<KadRecord>(&raw)
            .ok()
            .filter(|record| record.key == *k && !record.is_expired(SystemTime::now()))
            .map(|record| Cow::Owned(record.into()))
    }

    fn put(&mut self, r: Record) -> libp2p::kad::store::Result<()> {
        if r.value.len() >= self.config.max_value_bytes {
            return Err(Error::ValueTooLarge);
        }

        let key = self.key_to_hash(&r.key);
        let kr: KadRecord = r.into();
        let stored = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadRecords>(&key),
            NetworkType::Worker(_) => self.db.get::<KadWorkerRecords>(&key),
        }
        .map_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to read Kademlia record before insert");
            Error::ValueTooLarge
        })?;
        // Startup excludes unreadable records, so repairing one is an insertion for capacity
        // accounting. Replacing a readable owned row keeps the existing count.
        let new_record = stored.as_deref().and_then(|raw| self.decode_record(&key, raw)).is_none();
        if new_record && self.num_records >= self.config.max_records {
            // Try to free a slot by evicting any records whose TTL has passed.
            self.evict_expired_records();
            if self.num_records >= self.config.max_records {
                return Err(Error::MaxRecords);
            }
        }
        match self.kad_type {
            NetworkType::Primary => self.db.insert::<KadRecords>(&key, &encode(&kr)),
            NetworkType::Worker(_) => self.db.insert::<KadWorkerRecords>(&key, &encode(&kr)),
        }
        .map_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to insert Kademlia record");
            Error::ValueTooLarge
        })?;
        if new_record {
            self.num_records += 1;
            self.update_records_gauge();
        }
        Ok(())
    }

    fn remove(&mut self, k: &RecordKey) {
        let key = self.key_to_hash(k);
        let row_counted = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadRecords>(&key),
            NetworkType::Worker(_) => self.db.get::<KadWorkerRecords>(&key),
        }
        .ok()
        .flatten()
        .and_then(|raw| self.decode_record(&key, &raw))
        .is_some();
        if match self.kad_type {
            NetworkType::Primary => self.db.remove::<KadRecords>(&key),
            NetworkType::Worker(_) => self.db.remove::<KadWorkerRecords>(&key),
        }
        .is_ok()
            && row_counted
        {
            // Only readable owned rows contribute to startup accounting. An absent or
            // malformed row cannot uncount another row even when MDBX removal returns Ok.
            // Saturation also tolerates a preexisting stale count without wrapping capacity.
            self.num_records = self.num_records.saturating_sub(1);
            self.update_records_gauge();
        }
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        RecordIter { iter: Box::new(self.owned_records()) }
    }

    fn add_provider(&mut self, record: ProviderRecord) -> libp2p::kad::store::Result<()> {
        // Reject a record that advertises an implausible number of addresses. A legitimate
        // provider carries at most a few addresses, so a large list is only an attempt to
        // write attacker-controlled bytes into the consensus database (issue #1185). This
        // mirrors the `MAX_ADVERTISED_MULTIADDRS` cap on the signed `NodeRecord` path; the
        // sibling caps here (`max_providers_per_key`, `max_provided_keys`) bound different
        // axes and do not limit the address list inside one record.
        (record.addresses.len() <= MAX_ADVERTISED_MULTIADDRS).then_some(()).ok_or_else(|| {
            warn!(
                target: "network-kad",
                count = record.addresses.len(),
                max = MAX_ADVERTISED_MULTIADDRS,
                "provider record rejected: address count exceeds cap"
            );
            Error::ValueTooLarge
        })?;

        let record_key = record.key.clone();
        let key = self.key_to_hash(&record_key);
        let kr: KadProviderRecord = record.into();
        let stored = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadProviderRecords>(&key),
            NetworkType::Worker(_) => self.db.get::<KadWorkerProviderRecords>(&key),
        }
        .map_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to read Kademlia provider records before insert");
            Error::ValueTooLarge
        })?;

        // A readable ownership envelope is counted even when the provider payload is corrupt.
        // An unreadable envelope was excluded by startup accounting, so its replacement must
        // pass the capacity check and increment the count like a new key.
        let stored_row = stored.as_deref().and_then(|raw| self.decode_provider_row(&key, raw));
        let row_exists = stored_row.is_some();

        // The capacity check applies only to a brand-new key, mirroring `put`'s
        // `new_record` gate: an overwrite of an existing, already-counted row cannot
        // grow the table, so a refresh of a key this node already stores (including
        // its own self-provide) must succeed even when byzantine peers have
        // saturated the table with other keys (issue #1195).
        if !row_exists {
            // Try to free a slot by evicting fully-expired provider key groups, but
            // at most once per `PROVIDER_EVICT_INTERVAL`: with a 48h provider TTL a
            // saturated table expires nothing, so scanning per message would be a
            // full-table decode scan that frees nothing (GHSA-5475-xf29-3rv8). The
            // timestamp is stamped before the scan so the throttle also covers a
            // scan that does free rows.
            if self.num_providers >= self.config.max_provided_keys
                && self.last_provider_evict.is_none_or(|t| t.elapsed() >= PROVIDER_EVICT_INTERVAL)
            {
                self.last_provider_evict = Some(Instant::now());
                self.evict_expired_providers();
            }
            // Re-check after the (possibly throttled) eviction attempt; still full is
            // a hard error.
            (self.num_providers < self.config.max_provided_keys)
                .then_some(())
                .ok_or(Error::MaxProvidedKeys)?;
        }

        let merged = stored_row
            .and_then(|row| row.decode_records(&key))
            .map(|existing| self.merge_provider(existing, kr.clone()))
            .transpose()?;
        let records: Vec<KadProviderRecord> = merged.unwrap_or_else(|| vec![kr]);
        let encoded = KadProviderRow::encode(record_key, &records);

        match self.kad_type {
            NetworkType::Primary => self.db.insert::<KadProviderRecords>(&key, &encoded),
            NetworkType::Worker(_) => self.db.insert::<KadWorkerProviderRecords>(&key, &encoded),
        }
        .map_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to insert Kademlia provider records");
            self.record_provider_write_failure();
            Error::ValueTooLarge
        })?;
        if !row_exists {
            // A brand-new key: bump the counter. An overwrite (including of a purged
            // corrupt row) leaves the count unchanged.
            self.num_providers += 1;
        }
        Ok(())
    }

    fn providers(&self, key: &RecordKey) -> Vec<ProviderRecord> {
        let hash = self.key_to_hash(key);
        let stored = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadProviderRecords>(&hash),
            NetworkType::Worker(_) => self.db.get::<KadWorkerProviderRecords>(&hash),
        };
        let now = SystemTime::now();
        // Read-only path: an undecodable row is skipped (returns nothing) rather than
        // panicking. The startup scrub and the mutating paths do the actual purge.
        stored
            .inspect_err(|error| {
                error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to read Kademlia provider records");
            })
            .ok()
            .flatten()
            .and_then(|raw| self.decode_provider_row(&hash, &raw))
            .and_then(|row| row.decode_records(&hash))
            .map(|records| {
                records.into_iter().filter(|r| !r.is_expired(now)).map(Into::into).collect()
            })
            .unwrap_or_default()
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        // Enumerate only the provider records this node itself authored. Upstream
        // `MemoryStore::provided` filters on `provider == local_id`; restore that
        // guard here so the periodic republish job never re-announces a provider
        // record supplied by an inbound peer. Paired with the ban gate on inbound
        // `AddProvider` (`process_kad_add_provider` in consensus.rs), this closes
        // the divergence described in issue #1001. The `Vec` is filtered before
        // `into_iter().map(..)` so the concrete `ProvidedIter` type is preserved.
        let local_peer_id = self.local_peer_id;
        let provided: Vec<ProviderRecord> = self
            .providers(&self.node_key)
            .into_iter()
            .filter(|record| record.provider == local_peer_id)
            .collect();

        provided.into_iter().map(Cow::Owned)
    }

    fn remove_provider(&mut self, key: &RecordKey, p: &PeerId) {
        let hash = self.key_to_hash(key);
        let stored = match self.kad_type {
            NetworkType::Primary => self.db.get::<KadProviderRecords>(&hash),
            NetworkType::Worker(_) => self.db.get::<KadWorkerProviderRecords>(&hash),
        }
        .inspect_err(|error| {
            error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to read Kademlia provider records before removal");
        })
        .ok()
        .flatten();

        if stored.is_some() {
            let row = stored.as_deref().and_then(|raw| self.decode_provider_row(&hash, raw));
            let row_counted = row.is_some();
            // Direct lookup establishes the namespace even if the envelope itself is corrupt.
            // Preserve the count when removing an unreadable envelope excluded at startup.
            let remaining: Vec<KadProviderRecord> = row
                .and_then(|row| row.decode_records(&hash))
                .map(|records| records.into_iter().filter(|r| r.provider != *p).collect())
                .unwrap_or_default();
            if remaining.is_empty() {
                let removed = match self.kad_type {
                    NetworkType::Primary => self.db.remove::<KadProviderRecords>(&hash),
                    NetworkType::Worker(_) => self.db.remove::<KadWorkerProviderRecords>(&hash),
                }
                .is_ok();
                if removed && row_counted {
                    // The key holds no providers now (all filtered out, or the row was
                    // purged): drop the count once, saturating to avoid an underflow panic.
                    self.num_providers = self.num_providers.saturating_sub(1);
                }
            } else {
                let encoded = KadProviderRow::encode(key.clone(), &remaining);
                let _ = match self.kad_type {
                    NetworkType::Primary => self.db.insert::<KadProviderRecords>(&hash, &encoded),
                    NetworkType::Worker(_) => {
                        self.db.insert::<KadWorkerProviderRecords>(&hash, &encoded)
                    }
                }
                .inspect_err(|error| {
                    error!(target: "network-kad", ?error, kad_type = ?self.kad_type, "failed to update Kademlia provider records after removal");
                    self.record_provider_write_failure();
                });
            }
        }
    }
}

struct RecordKeySerde;

impl SerializeAs<RecordKey> for RecordKeySerde {
    fn serialize_as<S>(source: &RecordKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = source.to_vec();

        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&bytes).into_string())
        } else {
            serializer.serialize_bytes(&bytes)
        }
    }
}

impl<'de> DeserializeAs<'de, RecordKey> for RecordKeySerde {
    fn deserialize_as<D>(deserializer: D) -> Result<RecordKey, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::*;

        struct RKVisitor;

        impl Visitor<'_> for RKVisitor {
            type Value = RecordKey;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "valid bytes")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(RecordKey::new(&v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                let bytes = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;
                self.visit_bytes(&bytes)
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(RKVisitor)
        } else {
            deserializer.deserialize_bytes(RKVisitor)
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_config::KeyConfig;
    use tn_storage::open_db;
    use tn_types::{decode, encode, BlsKeypair};

    use super::*;

    fn test_record(expire_past: bool) -> Record {
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let value: Vec<u8> = vec![0, 1, 2, 3];
        let peer_id = PeerId::random();
        let expires = if expire_past {
            Instant::now().checked_sub(Duration::from_secs(60)) // Already expired
        } else {
            Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)) // one day
        };
        Record { key, value: value.clone(), publisher: Some(peer_id), expires }
    }

    fn test_provider_record() -> ProviderRecord {
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        ProviderRecord { key, provider, expires, addresses: vec![] }
    }

    #[test]
    fn test_kad_record_future() {
        let rec = test_record(false);
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec2.expires.unwrap().duration_since(now).as_secs()
        );

        // Now try an already past expiration.
        let rec = test_record(true);
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec2.expires.unwrap().duration_since(now).as_secs()
        );

        // Now try no expiration.
        let rec =
            Record { key: rec.key, value: rec.value, publisher: rec.publisher, expires: None };
        let krec: KadRecord = rec.clone().into();
        let bytes = encode(&krec);
        let krec2: KadRecord = decode(bytes.as_ref());
        let rec2: Record = krec2.into();
        assert_eq!(rec.key, rec2.key);
        assert_eq!(rec.value, rec2.value);
        assert_eq!(rec.publisher, rec2.publisher);
        assert!(rec.expires.is_none());
        assert!(rec2.expires.is_none());
    }

    fn test_rec<DB: Database>(rec: &Record, kad_store: &KadStore<DB>) {
        let rec_get = kad_store.get(&rec.key).expect("record");
        assert_eq!(rec.key, rec_get.key);
        assert_eq!(rec.value, rec_get.value);
        assert_eq!(rec.publisher, rec_get.publisher);
        let now = Instant::now();
        assert_eq!(
            rec.expires.unwrap().duration_since(now).as_secs(),
            rec_get.expires.unwrap().duration_since(now).as_secs()
        );
    }

    /// Sibling workers retain separate records, provider sets, startup counts and removals.
    #[test]
    fn test_kad_worker_store_isolation() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        // Sharing even the local peer id must not allow `provided()` to cross namespaces.
        let local_peer_id = PeerId::random();
        let mut worker_0 =
            KadStore::new(db.clone(), local_peer_id, &key_config, NetworkType::Worker(0));
        let mut worker_1 =
            KadStore::new(db.clone(), local_peer_id, &key_config, NetworkType::Worker(1));
        let record_0 = Record {
            key: RecordKey::new(&b"shared-worker-record"),
            value: vec![0],
            publisher: None,
            expires: None,
        };
        let record_1 = Record { value: vec![1], ..record_0.clone() };
        worker_0.put(record_0.clone())?;
        assert!(worker_1.get(&record_0.key).is_none());
        worker_1.put(record_1.clone())?;
        assert_eq!(worker_0.get(&record_0.key).map(|record| record.value.clone()), Some(vec![0]));
        assert_eq!(worker_1.get(&record_1.key).map(|record| record.value.clone()), Some(vec![1]));
        assert_eq!(worker_0.records().count(), 1);
        assert_eq!(worker_1.records().count(), 1);

        let provider_0 = ProviderRecord {
            key: worker_0.node_key.clone(),
            provider: local_peer_id,
            expires: None,
            addresses: vec!["/ip4/127.0.0.1/tcp/1000".parse()?],
        };
        let provider_1 = ProviderRecord {
            addresses: vec!["/ip4/127.0.0.1/tcp/1001".parse()?],
            ..provider_0.clone()
        };
        worker_0.add_provider(provider_0.clone())?;
        assert!(worker_1.providers(&provider_0.key).is_empty());
        worker_1.add_provider(provider_1.clone())?;
        assert_eq!(worker_0.providers(&provider_0.key), vec![provider_0.clone()]);
        assert_eq!(worker_1.providers(&provider_1.key), vec![provider_1.clone()]);
        assert_eq!(
            worker_0.provided().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![provider_0.clone()]
        );
        assert_eq!(
            worker_1.provided().map(Cow::into_owned).collect::<Vec<_>>(),
            vec![provider_1.clone()]
        );

        let restarted_0 =
            KadStore::new(db.clone(), local_peer_id, &key_config, NetworkType::Worker(0));
        let restarted_1 = KadStore::new(db, local_peer_id, &key_config, NetworkType::Worker(1));
        assert_eq!((restarted_0.num_records, restarted_0.num_providers), (1, 1));
        assert_eq!((restarted_1.num_records, restarted_1.num_providers), (1, 1));

        // Persist both namespaces so the deletion checks also exercise the disk fallback.
        worker_0.db.sync_persist();
        worker_0.remove(&record_0.key);
        worker_0.remove_provider(&provider_0.key, &local_peer_id);
        // Layered storage requires a persistence barrier before reading a deleted key.
        worker_0.db.sync_persist();
        assert!(worker_0.get(&record_0.key).is_none());
        assert!(worker_0.providers(&provider_0.key).is_empty());
        assert_eq!(worker_1.get(&record_1.key).map(|record| record.value.clone()), Some(vec![1]));
        assert_eq!(worker_1.providers(&provider_1.key), vec![provider_1]);
        Ok(())
    }

    /// Repairing or removing a row excluded at startup must preserve record capacity accounting.
    #[test]
    fn test_kad_worker_corrupt_record_accounting() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut seed =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        let good_record = test_record(false);
        seed.put(good_record.clone())?;
        let corrupt_record = test_record(false);
        let corrupt_hash = seed.key_to_hash(&corrupt_record.key);
        db.insert::<KadWorkerRecords>(&corrupt_hash, &vec![0xff])?;

        let mut worker_0 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        worker_0.config.max_records = 1;
        assert_eq!(worker_0.num_records, 1);
        assert!(matches!(worker_0.put(corrupt_record.clone()), Err(Error::MaxRecords)));
        assert_eq!(worker_0.records().count(), 1);
        assert_eq!(worker_0.num_records, 1);
        worker_0.remove(&corrupt_record.key);
        assert_eq!(
            worker_0.num_records, 1,
            "removing unreadable data must not uncount a valid row"
        );
        assert!(worker_0.get(&good_record.key).is_some());
        worker_0.remove(&good_record.key);
        assert_eq!(worker_0.num_records, 0);
        db.insert::<KadWorkerRecords>(&corrupt_hash, &vec![0xff])?;
        worker_0.put(corrupt_record.clone())?;
        assert_eq!(worker_0.num_records, 1, "repairing unreadable data consumes capacity");
        assert!(worker_0.get(&corrupt_record.key).is_some());
        assert_eq!(db.iter::<KadWorkerRecords>().count(), 1);
        Ok(())
    }

    /// A sibling's eviction must leave the owner's expired rows and counters in step.
    #[test]
    fn test_kad_worker_sibling_eviction_preserves_capacity() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut worker_0 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        let mut worker_1 = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(1));
        worker_0.config.max_records = 1;
        worker_0.config.max_provided_keys = 1;
        worker_0.put(test_record(true))?;
        worker_0.add_provider(expired_provider_under(&fresh_record_key()))?;

        assert_eq!(worker_1.evict_expired_records(), 0);
        assert_eq!(worker_1.evict_expired_providers(), 0);
        assert_eq!(worker_0.db.iter::<KadWorkerRecords>().count(), 1);
        assert_eq!(worker_0.db.iter::<KadWorkerProviderRecords>().count(), 1);
        let fresh = test_record(false);
        worker_0.put(fresh.clone())?;
        worker_0.add_provider(live_provider_under(&fresh.key))?;
        assert_eq!((worker_0.num_records, worker_0.num_providers), (1, 1));
        assert!(worker_0.get(&fresh.key).is_some());
        assert_eq!(worker_0.providers(&fresh.key).len(), 1);
        assert_eq!((worker_1.num_records, worker_1.num_providers), (0, 0));
        Ok(())
    }

    #[test]
    fn test_kad_store() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        let mut kad_store_worker =
            KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(0));

        let rec = test_record(false);
        let rec2 = test_record(false);
        let rec3 = test_record(false);
        kad_store.db.sync_persist();

        assert!(kad_store.get(&rec.key).is_none());
        assert_eq!(kad_store.records().count(), 0);
        kad_store.put(rec.clone()).expect("put record");
        kad_store_worker.put(rec.clone()).expect("put record");
        kad_store.db.sync_persist();
        //sleep(Duration::from_secs(3));
        test_rec(&rec, &kad_store);
        test_rec(&rec, &kad_store_worker);
        assert_eq!(kad_store.num_records, 1);
        assert_eq!(kad_store.records().count(), 1);
        assert_eq!(kad_store_worker.num_records, 1);
        assert_eq!(kad_store_worker.records().count(), 1);

        kad_store.remove(&rec.key);
        kad_store.db.sync_persist();
        test_rec(&rec, &kad_store_worker);
        assert_eq!(kad_store.records().count(), 0);
        assert_eq!(kad_store.num_records, 0);
        assert_eq!(kad_store_worker.records().count(), 1);
        assert_eq!(kad_store_worker.num_records, 1);
        kad_store_worker.remove(&rec.key);
        kad_store.db.sync_persist();
        assert!(kad_store.get(&rec.key).is_none());
        assert!(kad_store_worker.get(&rec.key).is_none());
        assert_eq!(kad_store.records().count(), 0);
        assert_eq!(kad_store.num_records, 0);
        assert_eq!(kad_store_worker.records().count(), 0);
        assert_eq!(kad_store_worker.num_records, 0);

        kad_store.put(rec.clone()).expect("put record");
        kad_store_worker.put(rec.clone()).expect("put record");
        kad_store.put(rec2.clone()).expect("put record");
        kad_store.put(rec3.clone()).expect("put record");
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_records, 3);
        assert_eq!(kad_store_worker.num_records, 1);
        assert_eq!(kad_store.records().count(), 3);
        assert_eq!(kad_store_worker.records().count(), 1);
        test_rec(&rec, &kad_store);
        test_rec(&rec, &kad_store_worker);
        test_rec(&rec2, &kad_store);
        test_rec(&rec3, &kad_store);

        let key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24));
        // Make manually to use our node key as key.
        let provider_rec1 = ProviderRecord { key, provider, expires, addresses: vec![] };
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        let provider_rec1_1 =
            ProviderRecord { key: provider_rec1.key.clone(), provider, expires, addresses: vec![] };
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24)); // one day
        let provider_rec1_2 =
            ProviderRecord { key: provider_rec1.key.clone(), provider, expires, addresses: vec![] };
        let provider_rec2 = test_provider_record();
        let provider_rec3 = test_provider_record();
        kad_store.db.sync_persist();
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 0);
        kad_store.add_provider(provider_rec1.clone()).expect("add provider");
        kad_store.add_provider(provider_rec2.clone()).expect("add provider");
        kad_store.add_provider(provider_rec3.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 1);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.add_provider(provider_rec1_2.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);
        assert_eq!(kad_store.providers(&provider_rec2.key).len(), 1);
        assert_eq!(kad_store.providers(&provider_rec3.key).len(), 1);

        assert_eq!(kad_store_worker.num_providers, 0);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 0);
        kad_store_worker.add_provider(provider_rec1.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec2.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec3.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 1);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store_worker.add_provider(provider_rec1_2.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);
        assert_eq!(kad_store_worker.providers(&provider_rec2.key).len(), 1);
        assert_eq!(kad_store_worker.providers(&provider_rec3.key).len(), 1);

        let recs_1 = kad_store.providers(&provider_rec1.key);
        assert_eq!(recs_1.len(), 3);
        assert_eq!(recs_1[0], provider_rec1);
        assert_eq!(recs_1[1], provider_rec1_1);
        assert_eq!(recs_1[2], provider_rec1_2);

        kad_store.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 2);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);
        kad_store.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 3);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 3);

        kad_store_worker.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 2);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);
        kad_store_worker.add_provider(provider_rec1_1.clone()).expect("add provider");
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 3);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 3);

        kad_store.remove_provider(&provider_rec1.key, &provider_rec1.provider);
        assert_eq!(kad_store.num_providers, 3);
        kad_store.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store.num_providers, 3);
        kad_store.remove_provider(&provider_rec1_2.key, &provider_rec1_2.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 2);
        assert_eq!(kad_store.providers(&provider_rec1.key).len(), 0);
        kad_store.remove_provider(&provider_rec2.key, &provider_rec2.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store.num_providers, 1);
        assert_eq!(kad_store.providers(&provider_rec2.key).len(), 0);

        kad_store_worker.remove_provider(&provider_rec1.key, &provider_rec1.provider);
        assert_eq!(kad_store_worker.num_providers, 3);
        kad_store_worker.remove_provider(&provider_rec1_1.key, &provider_rec1_1.provider);
        assert_eq!(kad_store_worker.num_providers, 3);
        kad_store_worker.remove_provider(&provider_rec1_2.key, &provider_rec1_2.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 2);
        assert_eq!(kad_store_worker.providers(&provider_rec1.key).len(), 0);
        kad_store_worker.remove_provider(&provider_rec2.key, &provider_rec2.provider);
        kad_store.db.sync_persist();
        assert_eq!(kad_store_worker.num_providers, 1);
        assert_eq!(kad_store_worker.providers(&provider_rec2.key).len(), 0);

        // Bogus remove, mismatches key and provider.
        kad_store.remove_provider(&provider_rec3.key, &provider_rec2.provider);
        assert_eq!(kad_store.num_providers, 1);
        kad_store.remove_provider(&provider_rec3.key, &provider_rec3.provider);
        assert_eq!(kad_store.num_providers, 0);
        kad_store_worker.remove_provider(&provider_rec3.key, &provider_rec2.provider);
        assert_eq!(kad_store_worker.num_providers, 1);
        kad_store_worker.remove_provider(&provider_rec3.key, &provider_rec3.provider);
        assert_eq!(kad_store_worker.num_providers, 0);
    }

    /// `provided()` enumerates only the provider records the node itself authored.
    ///
    /// The `node_key`-injection guard from issue #1001: an attacker can address an
    /// `AddProvider` at this node's own key (`node_key` is derived from the public
    /// primary key, which is not secret), landing a record in the exact slot
    /// `provided()` reads. Both records are stored under `node_key`, but the
    /// republish job must re-announce only the record whose `provider` is this
    /// node. Removing the `provider == local_peer_id` filter in `provided()` makes
    /// this test fail (the attacker record is enumerated), pinning the guard.
    #[test]
    fn test_provided_enumerates_only_self_authored_records() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));

        // The store's identity: records authored by this peer id are "ours".
        let local_peer_id = PeerId::random();
        let mut kad_store = KadStore::new(db, local_peer_id, &key_config, NetworkType::Primary);

        // Both records key on `node_key`, the slot `provided()` reads.
        let node_key = RecordKey::new(&encode(&key_config.primary_public_key()));
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24));
        let ours = ProviderRecord {
            key: node_key.clone(),
            provider: local_peer_id,
            expires,
            addresses: vec![],
        };
        let attacker = ProviderRecord {
            key: node_key.clone(),
            provider: PeerId::random(),
            expires,
            addresses: vec![],
        };

        kad_store.add_provider(ours.clone()).expect("add our provider record");
        kad_store.add_provider(attacker.clone()).expect("add attacker provider record");
        kad_store.db.sync_persist();

        // Both are physically stored under `node_key`...
        assert_eq!(kad_store.providers(&node_key).len(), 2, "both records stored under node_key");

        // ...but `provided()` re-announces only the record this node authored.
        let provided: Vec<ProviderRecord> =
            kad_store.provided().map(|record| record.into_owned()).collect();
        assert_eq!(provided.len(), 1, "provided() enumerates only self-authored records");
        assert_eq!(provided[0].provider, local_peer_id, "the enumerated record is ours");
    }

    /// Expired records must be filtered from `get()` and `records()` even though
    /// they remain on disk until `put()` triggers eviction.
    #[test]
    fn test_kad_expired_records_filtered_on_read() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let expired = test_record(true);
        kad_store.put(expired.clone()).expect("put expired record");
        assert_eq!(kad_store.num_records, 1, "row was written to disk");

        assert!(kad_store.get(&expired.key).is_none(), "get filters expired");
        assert_eq!(kad_store.records().count(), 0, "records() filters expired");
    }

    /// When the store is full of expired records, a new put should evict them and succeed.
    #[test]
    fn test_kad_put_evicts_expired_when_full() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let config = MemoryStoreConfig::default();

        // Saturate the store with already-expired records.
        for _ in 0..config.max_records {
            let rec = test_record(true);
            kad_store.put(rec).expect("put expired record");
        }
        assert_eq!(kad_store.num_records, config.max_records);

        // A live record under a brand-new key must succeed: the put path evicts expired rows.
        let fresh = test_record(false);
        kad_store.put(fresh.clone()).expect("eviction must make room");
        assert_eq!(kad_store.num_records, 1, "only the fresh row should remain");
        assert!(kad_store.get(&fresh.key).is_some(), "fresh record retained");
    }

    /// Replacing a record refreshes expiry without consuming another slot, even at capacity.
    #[test]
    fn test_kad_put_refreshes_expiry_at_capacity() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)]
            .into_iter()
            .try_for_each(|network_type| -> eyre::Result<()> {
                let mut store =
                    KadStore::new(db.clone(), PeerId::random(), &key_config, network_type);
                store.config.max_records = 1;
                let instant = Instant::now();
                let mut record = test_record(false);
                record.expires = Some(instant + Duration::from_secs(60));
                store.put(record.clone())?;
                let first_expiry = store
                    .get(&record.key)
                    .and_then(|stored| stored.expires)
                    .ok_or_else(|| eyre::eyre!("initial expiry"))?;
                record.expires = Some(instant + Duration::from_secs(120));
                store.put(record.clone())?;
                let stored =
                    store.get(&record.key).ok_or_else(|| eyre::eyre!("refreshed record"))?;
                assert_eq!(stored.value, record.value);
                assert_eq!(stored.publisher, record.publisher);
                assert!(stored
                    .expires
                    .is_some_and(|expiry| expiry > first_expiry + Duration::from_secs(30)));
                assert_eq!(store.num_records, 1);
                assert_eq!(store.records().count(), 1);
                Ok(())
            })
    }

    /// Test that we do not count duplicate puts against our max records.
    #[test]
    fn test_kad_put_limit() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        let mut kad_store_worker =
            KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(0));

        let config = MemoryStoreConfig::default();

        // Almost fill up the stores.
        for _ in 0..config.max_records - 1 {
            let rec = test_record(false);
            kad_store.put(rec.clone()).expect("put record");
            kad_store_worker.put(rec).expect("put record");
        }
        let rec = test_record(false);
        // These should all work because they are overwrites not new records.
        for _ in 0..10 {
            kad_store.put(rec.clone()).expect("put record");
            kad_store_worker.put(rec.clone()).expect("put record");
        }
        let rec = test_record(false);
        // Should be full now so get max errors.
        assert!(matches!(kad_store.put(rec.clone()), Err(Error::MaxRecords)));
        assert!(matches!(kad_store_worker.put(rec.clone()), Err(Error::MaxRecords)));
    }

    // ---- issue #1195: the provider capacity check gates only brand-new keys ----

    /// A refresh of an existing provider key is an overwrite, not a new row: it must
    /// succeed at capacity and leave the count unchanged, while a brand-new key is
    /// still rejected. Mirrors `test_kad_put_limit`, which locks the same gating for
    /// `put`.
    #[test]
    fn test_kad_add_provider_refresh_allowed_at_capacity() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut kad_store =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        let mut kad_store_worker =
            KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(0));
        // A small cap keeps the test fast; the gating logic does not depend on the
        // cap's value.
        kad_store.config.max_provided_keys = 4;
        kad_store_worker.config.max_provided_keys = 4;

        // Fill both tables to capacity with distinct live keys, keeping one record
        // to refresh later.
        let refresh_rec = live_provider_under(&fresh_record_key());
        kad_store.add_provider(refresh_rec.clone()).expect("add provider");
        kad_store_worker.add_provider(refresh_rec.clone()).expect("add provider");
        (1..4).for_each(|_| {
            let rec = live_provider_under(&fresh_record_key());
            kad_store.add_provider(rec.clone()).expect("add provider");
            kad_store_worker.add_provider(rec).expect("add provider");
        });
        assert_eq!(kad_store.num_providers, 4);
        assert_eq!(kad_store_worker.num_providers, 4);

        // The refresh of an existing key must succeed at capacity (issue #1195: the
        // node keeps its own provider records live this way).
        (0..10).for_each(|_| {
            kad_store.add_provider(refresh_rec.clone()).expect("refresh at capacity");
            kad_store_worker.add_provider(refresh_rec.clone()).expect("refresh at capacity");
        });
        assert_eq!(kad_store.num_providers, 4, "refresh must not change the count");
        assert_eq!(kad_store_worker.num_providers, 4, "refresh must not change the count");

        // Positive control: a brand-new key is still rejected while full.
        let new_rec = live_provider_under(&fresh_record_key());
        assert!(matches!(kad_store.add_provider(new_rec.clone()), Err(Error::MaxProvidedKeys)));
        assert!(matches!(kad_store_worker.add_provider(new_rec), Err(Error::MaxProvidedKeys)));
    }

    /// A brand-new key at capacity still triggers the expired-group eviction attempt
    /// before the hard error: moving the capacity check behind the new-key gate
    /// (issue #1195) must not lose the eviction. Mirrors
    /// `test_kad_put_evicts_expired_when_full`.
    #[test]
    fn test_kad_add_provider_evicts_expired_when_full() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);
        kad_store.config.max_provided_keys = 4;

        // Saturate the table with already-expired provider groups.
        (0..4).for_each(|_| {
            kad_store
                .add_provider(expired_provider_under(&fresh_record_key()))
                .expect("add expired provider");
        });
        assert_eq!(kad_store.num_providers, 4);

        // A live record under a brand-new key must succeed: eviction makes room.
        let fresh_key = fresh_record_key();
        kad_store.add_provider(live_provider_under(&fresh_key)).expect("eviction must make room");
        assert_eq!(kad_store.num_providers, 1, "only the fresh row should remain");
        assert_eq!(kad_store.providers(&fresh_key).len(), 1, "fresh provider retained");
    }

    /// A saturated provider table must not run the full-table eviction scan more
    /// than once per `PROVIDER_EVICT_INTERVAL`. Provider records carry a 48h TTL,
    /// so a full table expires nothing and an unthrottled scan would run on every
    /// inbound `AddProvider` for a new key (GHSA-5475-xf29-3rv8). The throttle is
    /// driven directly through `last_provider_evict`.
    #[test]
    fn test_kad_add_provider_throttles_eviction_scan() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);
        kad_store.config.max_provided_keys = 4;

        // Saturate the table with already-expired provider groups.
        (0..4).for_each(|_| {
            kad_store
                .add_provider(expired_provider_under(&fresh_record_key()))
                .expect("add expired provider");
        });
        assert_eq!(kad_store.num_providers, 4);

        // Simulate a scan that just ran: a brand-new key within the interval must
        // NOT trigger a second scan, so the expired rows are not reclaimed and the
        // insert is rejected.
        kad_store.last_provider_evict = Some(Instant::now());
        assert!(matches!(
            kad_store.add_provider(live_provider_under(&fresh_record_key())),
            Err(Error::MaxProvidedKeys)
        ));
        assert_eq!(
            kad_store.num_providers, 4,
            "a throttled scan must not reclaim the expired rows"
        );

        // Once the interval has elapsed the scan runs, reclaims the expired groups,
        // and the new key is admitted.
        kad_store.last_provider_evict =
            Instant::now().checked_sub(PROVIDER_EVICT_INTERVAL + Duration::from_secs(1));
        let fresh_key = fresh_record_key();
        kad_store.add_provider(live_provider_under(&fresh_key)).expect("scan must make room");
        assert_eq!(kad_store.num_providers, 1, "only the fresh row remains after the scan");
        assert_eq!(kad_store.providers(&fresh_key).len(), 1, "fresh provider retained");
    }

    /// Lock the per-role, chain-namespaced wire-protocol names. These strings are a
    /// peer-compatibility contract: a silent change would prevent peers from
    /// negotiating sessions, and the chain id keeps different chains from ever
    /// negotiating with each other (issue #765).
    #[test]
    fn test_network_type_protocol_names() -> crate::types::NetworkResult<()> {
        assert_eq!(NetworkType::Primary.req_res_protocol(2017)?.as_ref(), "/tn-primary-2017/0.0.2");
        assert_eq!(NetworkType::Primary.kad_protocol(2017)?.as_ref(), "/tn-primary-kad-2017/0.0.1");
        assert_eq!(
            NetworkType::Worker(0).req_res_protocol(2017)?.as_ref(),
            "/tn-worker-0-2017/0.0.2"
        );
        assert_eq!(
            NetworkType::Worker(0).kad_protocol(2017)?.as_ref(),
            "/tn-worker-0-kad-2017/0.0.1"
        );
        // worker id and chain id are both interpolated, not literal
        assert_eq!(NetworkType::Worker(3).req_res_protocol(7)?.as_ref(), "/tn-worker-3-7/0.0.2");
        assert_eq!(NetworkType::Worker(3).kad_protocol(7)?.as_ref(), "/tn-worker-3-kad-7/0.0.1");
        // the per-role sync protocol is chain-namespaced as well
        assert_eq!(
            NetworkType::Primary.sync_protocol(2017)?.as_ref(),
            "/tn-primary-sync-2017/0.0.1"
        );
        assert_eq!(NetworkType::Worker(3).sync_protocol(7)?.as_ref(), "/tn-worker-3-sync-7/0.0.1");
        // the per-role peer-exchange goodbye protocol is chain-namespaced as well
        assert_eq!(
            NetworkType::Primary.peer_exchange_protocol(2017)?.as_ref(),
            "/tn-primary-peer-exchange-2017/0.0.1"
        );
        assert_eq!(
            NetworkType::Worker(3).peer_exchange_protocol(7)?.as_ref(),
            "/tn-worker-3-peer-exchange-7/0.0.1"
        );
        // a stream node advertises exactly the per-role sync protocol (chain-namespaced)
        let sync = crate::types::stream_protocol(NetworkType::Worker(3), 2017)?;
        assert_eq!(sync.as_ref(), "/tn-worker-3-sync-2017/0.0.1");
        Ok(())
    }

    /// Lock the chain-namespaced gossipsub protocol-id prefix. Gossip negotiates
    /// its own `/meshsub` protocol (not the req-res/kad/stream names), so this
    /// prefix is what keeps two chains from ever sharing a gossip substream. A
    /// silent change is a peer-compatibility break, and dropping the leading `/`
    /// would make `gossipsub::ConfigBuilder::build` reject it (issue #765).
    #[test]
    fn test_gossip_protocol_id_prefix_is_chain_namespaced() -> crate::types::NetworkResult<()> {
        use crate::types::gossip_protocol_id_prefix;

        // the chain id is interpolated, not a literal
        assert_eq!(gossip_protocol_id_prefix(2017), "/tn-meshsub-2017");
        assert_eq!(gossip_protocol_id_prefix(0), "/tn-meshsub-0");
        // different chains get different gossip protocol ids
        assert_ne!(gossip_protocol_id_prefix(1), gossip_protocol_id_prefix(2));

        // the prefix is a valid `/meshsub`-style protocol id: feeding it to a real
        // gossipsub ConfigBuilder must build. `protocol_id_prefix` appends `/1.1.0`
        // and `/1.0.0` and does not prepend a `/`, so a prefix without the leading
        // slash would be a malformed StreamProtocol and `build` would error here.
        libp2p::gossipsub::ConfigBuilder::default()
            .protocol_id_prefix(gossip_protocol_id_prefix(2017))
            .build()
            .map(|_| ())
            .map_err(Into::into)
    }

    /// `remove` for a key that was never `put` must not drive `num_records` below zero.
    /// On MDBX `db.remove` reports `Ok` even for an absent key, so the `is_ok()` gate in
    /// `remove` fires and reaches the decrement. Before the `saturating_sub` fix this
    /// wrapped the `usize` to `usize::MAX` (debug: panicked), permanently satisfying
    /// `num_records >= max_records` and wedging every future `put` (issue #1005).
    #[test]
    fn test_kad_remove_uncounted_record_saturates() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let rec = test_record(false);
        assert_eq!(kad_store.num_records, 0);
        kad_store.remove(&rec.key);
        assert_eq!(kad_store.num_records, 0, "removing an uncounted record must clamp at zero");

        // The counter is still sound: a subsequent put is accepted, not wedged.
        kad_store.put(rec.clone()).expect("put must still succeed after a no-op remove");
        assert_eq!(kad_store.num_records, 1);
    }

    /// Removing the same record twice must not underflow `num_records`. The first `remove`
    /// deletes the row and decrements to zero; the second finds nothing on disk yet MDBX
    /// still reports `Ok`, so the decrement runs again. `saturating_sub` keeps it at zero
    /// (issue #1005).
    #[test]
    fn test_kad_double_remove_record_saturates() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let rec = test_record(false);
        kad_store.put(rec.clone()).expect("put record");
        assert_eq!(kad_store.num_records, 1);
        kad_store.remove(&rec.key);
        kad_store.remove(&rec.key);
        assert_eq!(kad_store.num_records, 0, "double remove must clamp at zero, not wrap");

        // Not wedged: the cap still admits fresh records.
        let fresh = test_record(false);
        kad_store.put(fresh).expect("put must still succeed");
        assert_eq!(kad_store.num_records, 1);
    }

    /// `remove_provider`'s decrement is currently guarded by the `db.get(..) == Some`
    /// precondition, so it is latent rather than live like the records path above. This
    /// test drives the guarded decrement directly by simulating the counter drifting below
    /// the true on-disk provider-key count, and asserts the emptying removal clamps at zero
    /// instead of wrapping to `usize::MAX` and wedging `add_provider` (issue #1005).
    #[test]
    fn test_kad_remove_provider_saturates_on_counter_drift() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config =
            KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()));
        let mut kad_store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let pr = test_provider_record();
        kad_store.add_provider(pr.clone()).expect("add provider");
        assert_eq!(kad_store.num_providers, 1);

        // Simulate a counter that has drifted below the on-disk count while the provider
        // row still exists; the emptying removal must saturate, not underflow.
        kad_store.num_providers = 0;
        kad_store.remove_provider(&pr.key, &pr.provider);
        assert_eq!(kad_store.num_providers, 0, "emptying removal must clamp at zero");

        // Not wedged: add_provider still admits a fresh key.
        let pr2 = test_provider_record();
        kad_store.add_provider(pr2).expect("add_provider must still succeed");
        assert_eq!(kad_store.num_providers, 1);
    }

    // ---- issue #999: provider tables tolerate undecodable rows instead of panicking ----

    /// Bytes that are not a valid BCS `Vec<KadProviderRecord>`: the leading ULEB128 length
    /// overflows a `u32`, so decoding always errors. This stands in for a row written by an
    /// incompatible schema/version or corrupted on disk. Every test below relies on the
    /// tolerant `decode_providers` helper: swap it back to the panicking `decode` and each
    /// one fails (a panic), which is the guard-reversal check the issue asks for.
    const CORRUPT_PROVIDER_BYTES: [u8; 5] = [0xff, 0xff, 0xff, 0xff, 0xff];

    fn test_key_config() -> KeyConfig {
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::from_os_rng()))
    }

    /// A random, distinct provider key.
    fn fresh_record_key() -> RecordKey {
        RecordKey::new(&encode(&test_key_config().primary_public_key()))
    }

    /// Corrupt a primary provider payload while keeping its ownership envelope readable.
    fn inject_corrupt_primary_provider<DB: Database>(store: &KadStore<DB>, key: &RecordKey) {
        let hash = store.key_to_hash(key);
        let row = KadProviderRow { key: key.clone(), records: CORRUPT_PROVIDER_BYTES.to_vec() };
        store
            .db
            .insert::<KadProviderRecords>(&hash, &encode(&row))
            .expect("inject corrupt provider row");
    }

    /// Corrupt a worker provider payload while keeping its ownership envelope readable.
    fn inject_corrupt_worker_provider<DB: Database>(store: &KadStore<DB>, key: &RecordKey) {
        let hash = store.key_to_hash(key);
        let row = KadProviderRow { key: key.clone(), records: CORRUPT_PROVIDER_BYTES.to_vec() };
        store
            .db
            .insert::<KadWorkerProviderRecords>(&hash, &encode(&row))
            .expect("inject corrupt worker provider row");
    }

    fn live_provider_under(key: &RecordKey) -> ProviderRecord {
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24));
        ProviderRecord { key: key.clone(), provider, expires, addresses: vec![] }
    }

    /// A provider record under `key` whose expiry is already in the past.
    fn expired_provider_under(key: &RecordKey) -> ProviderRecord {
        let provider = PeerId::random();
        let expires = Instant::now().checked_sub(Duration::from_secs(60));
        ProviderRecord { key: key.clone(), provider, expires, addresses: vec![] }
    }

    /// `providers()` is a read-only path: an undecodable row must be skipped (empty result)
    /// rather than panic the caller, and neighbouring good rows must be unaffected.
    #[test]
    fn test_kad_providers_skips_corrupt_row() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let good_key = fresh_record_key();
        store.add_provider(live_provider_under(&good_key)).expect("add good provider");

        let corrupt_key = fresh_record_key();
        inject_corrupt_primary_provider(&store, &corrupt_key);

        // The corrupt key yields nothing; the good key is untouched.
        assert!(store.providers(&corrupt_key).is_empty(), "corrupt row skipped, not panicked");
        assert_eq!(store.providers(&good_key).len(), 1, "good row still readable");
    }

    /// `remove_provider()` is a mutating path: an undecodable row is purged wholesale and the
    /// provider count is decremented, modelling a restart where `new()` counted the bad row.
    #[test]
    fn test_kad_remove_provider_purges_corrupt_row() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();

        let corrupt_key = fresh_record_key();
        // Seed then "restart" so `new()` counts the injected row exactly as it would on disk.
        let seed = KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        inject_corrupt_primary_provider(&seed, &corrupt_key);
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);
        assert_eq!(store.num_providers, 1, "corrupt row counted at startup");

        store.remove_provider(&corrupt_key, &PeerId::random());
        assert!(store.providers(&corrupt_key).is_empty(), "corrupt row purged");
        assert_eq!(store.num_providers, 0, "count decremented on purge");
    }

    /// `add_provider()` over a corrupt existing row overwrites (purges) it without panicking,
    /// and must not double-count: the row was already counted at startup.
    #[test]
    fn test_kad_add_provider_over_corrupt_row_overwrites() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();

        let key = fresh_record_key();
        let seed = KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        inject_corrupt_primary_provider(&seed, &key);
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);
        assert_eq!(store.num_providers, 1, "corrupt row counted at startup");

        let rec = live_provider_under(&key);
        store.add_provider(rec.clone()).expect("add over corrupt row must not panic");

        assert_eq!(store.num_providers, 1, "overwrite of a corrupt row keeps the count stable");
        let got = store.providers(&key);
        assert_eq!(got.len(), 1, "corrupt row replaced by the new provider");
        assert_eq!(got[0].provider, rec.provider);
    }

    /// The startup scrub purges every undecodable provider row (keeping the count in step) and
    /// leaves decodable rows intact; a second pass is a no-op. This is the parity with the
    /// `KadRecords` tolerant startup load that the issue asks for.
    #[test]
    fn test_kad_scrub_corrupt_providers_purges_bad_keeps_good() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();

        let good_key = fresh_record_key();
        let mut seed =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Primary);
        seed.add_provider(live_provider_under(&good_key)).expect("add good provider");
        let corrupt_key = fresh_record_key();
        inject_corrupt_primary_provider(&seed, &corrupt_key);

        // "restart": both rows are counted without being decoded.
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);
        assert_eq!(store.num_providers, 2, "good and corrupt rows both counted at startup");

        let purged = store.scrub_corrupt_providers();
        assert_eq!(purged, 1, "exactly the corrupt row is purged");
        assert_eq!(store.num_providers, 1, "count reflects the purge");
        assert_eq!(store.providers(&good_key).len(), 1, "good row survives the scrub");
        assert!(store.providers(&corrupt_key).is_empty(), "corrupt row gone after scrub");

        assert_eq!(store.scrub_corrupt_providers(), 0, "scrub is idempotent once clean");
    }

    /// The worker provider table gets the same tolerance as the primary one.
    #[test]
    fn test_kad_worker_provider_table_tolerates_corrupt_row() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();

        let corrupt_key = fresh_record_key();
        let seed = KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        inject_corrupt_worker_provider(&seed, &corrupt_key);
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(0));
        assert_eq!(store.num_providers, 1, "worker corrupt row counted at startup");

        // read path does not panic, scrub purges it
        assert!(store.providers(&corrupt_key).is_empty(), "worker read skips corrupt row");
        assert_eq!(store.scrub_corrupt_providers(), 1, "worker scrub purges corrupt row");
        assert_eq!(store.num_providers, 0, "worker count reflects the purge");
    }

    /// Provider scrubbing and expiry respect ownership even for malformed or empty payloads.
    #[test]
    fn test_kad_worker_provider_corruption_isolation() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut seed_0 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        let mut seed_1 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(1));
        let good_key = fresh_record_key();
        seed_0.add_provider(live_provider_under(&good_key))?;
        seed_1.add_provider(live_provider_under(&good_key))?;
        let corrupt_key = fresh_record_key();
        inject_corrupt_worker_provider(&seed_1, &corrupt_key);
        let empty_key = fresh_record_key();
        db.insert::<KadWorkerProviderRecords>(
            &seed_1.key_to_hash(&empty_key),
            &KadProviderRow::encode(empty_key.clone(), &[]),
        )?;
        let mixed_key = fresh_record_key();
        db.insert::<KadWorkerProviderRecords>(
            &seed_1.key_to_hash(&mixed_key),
            &KadProviderRow::encode(mixed_key.clone(), &[live_provider_under(&good_key).into()]),
        )?;
        let unknown_key = fresh_record_key();
        let unknown_hash = seed_1.key_to_hash(&unknown_key);
        db.insert::<KadWorkerProviderRecords>(&unknown_hash, &CORRUPT_PROVIDER_BYTES.to_vec())?;

        let mut worker_0 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(0));
        let mut worker_1 =
            KadStore::new(db.clone(), PeerId::random(), &key_config, NetworkType::Worker(1));
        assert_eq!(worker_0.num_providers, 1);
        assert_eq!(worker_1.num_providers, 4, "unreadable envelopes have no assumed owner");
        assert!(worker_0.providers(&corrupt_key).is_empty());
        assert!(worker_1.providers(&mixed_key).is_empty(), "mismatched payload key is rejected");
        assert!(worker_1.providers(&unknown_key).is_empty());
        assert_eq!(worker_0.scrub_corrupt_providers(), 0);
        assert_eq!(worker_0.evict_expired_providers(), 0);
        assert_eq!(db.iter::<KadWorkerProviderRecords>().count(), 6);

        assert_eq!(worker_1.scrub_corrupt_providers(), 2);
        assert_eq!(worker_1.evict_expired_providers(), 1, "empty owned payload frees its slot");
        assert_eq!(worker_1.num_providers, 1);
        assert_eq!(worker_0.num_providers, 1);
        assert_eq!(worker_0.providers(&good_key).len(), 1);
        assert_eq!(worker_1.providers(&good_key).len(), 1);
        assert!(db.get::<KadWorkerProviderRecords>(&unknown_hash)?.is_some());

        worker_1.config.max_provided_keys = 1;
        assert!(
            matches!(
                worker_1.add_provider(live_provider_under(&unknown_key)),
                Err(Error::MaxProvidedKeys)
            ),
            "replacing an uncounted envelope must still enforce capacity"
        );
        // Exercise deletion of an on-disk row, then wait for the queued removal to persist.
        db.sync_persist();
        worker_1.remove_provider(&unknown_key, &PeerId::random());
        db.sync_persist();
        assert_eq!(worker_1.num_providers, 1, "removing an uncounted envelope preserves the count");
        assert!(db.get::<KadWorkerProviderRecords>(&unknown_hash)?.is_none());
        Ok(())
    }

    // ---- issue #1185: a provider record's address list is capped ----

    /// A live provider record under `key` that carries `count` distinct addresses.
    fn provider_with_addresses(key: &RecordKey, count: usize) -> ProviderRecord {
        let provider = PeerId::random();
        let expires = Instant::now().checked_add(Duration::from_secs(60 * 60 * 24));
        let addresses = (0..count)
            .map(|i| format!("/ip4/127.0.0.1/tcp/{}", 1000 + i).parse().expect("valid multiaddr"))
            .collect();
        ProviderRecord { key: key.clone(), provider, expires, addresses }
    }

    /// One address over the cap: `add_provider` must reject the record with
    /// `ValueTooLarge` and leave the store untouched. Without the cap, each
    /// `AddProvider` request could write an attacker-sized address list into the
    /// consensus database.
    #[test]
    fn test_kad_add_provider_rejects_oversized_address_list() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let key = fresh_record_key();
        let rec = provider_with_addresses(&key, MAX_ADVERTISED_MULTIADDRS + 1);
        assert!(matches!(store.add_provider(rec), Err(Error::ValueTooLarge)));
        assert_eq!(store.num_providers, 0, "rejected record must not bump the provider count");
        assert!(store.providers(&key).is_empty(), "rejected record must not be stored");
    }

    /// A record at exactly the cap is legitimate and must be admitted with its full
    /// address list intact (positive control for the rejection test).
    #[test]
    fn test_kad_add_provider_admits_address_list_at_cap() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Primary);

        let key = fresh_record_key();
        let rec = provider_with_addresses(&key, MAX_ADVERTISED_MULTIADDRS);
        store.add_provider(rec.clone()).expect("record at the cap is admitted");
        assert_eq!(store.num_providers, 1);
        let got = store.providers(&key);
        assert_eq!(got.len(), 1, "record at the cap is stored");
        assert_eq!(got[0].addresses.len(), MAX_ADVERTISED_MULTIADDRS, "address list kept intact");
        assert_eq!(got[0].provider, rec.provider);
    }

    /// The cap guards the shared entry point, so the worker table is covered by the
    /// same check: an oversized record is rejected there too.
    #[test]
    fn test_kad_add_provider_worker_rejects_oversized_address_list() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let db = open_db(tmp_dir.path());
        let key_config = test_key_config();
        let mut store = KadStore::new(db, PeerId::random(), &key_config, NetworkType::Worker(0));

        let key = fresh_record_key();
        let rec = provider_with_addresses(&key, MAX_ADVERTISED_MULTIADDRS + 1);
        assert!(matches!(store.add_provider(rec), Err(Error::ValueTooLarge)));
        assert_eq!(store.num_providers, 0, "rejected record must not bump the provider count");
        assert!(store.providers(&key).is_empty(), "rejected record must not be stored");
    }
}
