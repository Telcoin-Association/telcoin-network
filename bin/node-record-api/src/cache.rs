//! The in-memory record cache the HTTP API serves from.
//!
//! One refresh cycle produces, for every key in the tracked set, either a verified record, a
//! clean miss, or a lookup error; [`RecordCache::apply_cycle`] folds that into the cache under
//! three rules:
//!
//! 1. **Newest wins.** A fetched record replaces the cached one only when its signed timestamp is
//!    not older. Lookups are answered by whichever peers hold a copy, and a lagging peer can serve
//!    a stale copy; it must not roll the cache back to it.
//! 2. **Failures keep the entry.** A miss or error leaves the cached record in place and does not
//!    advance its `fetched_at`, so it ages into `stale` and, eventually, out of the cache.
//! 3. **Two evictions.** An entry not refreshed for `record_ttl` is dropped, as is an entry whose
//!    key has been absent from the tracked set for `absent_cycles_before_evict` consecutive cycles
//!    (a validator that left the committee and the configured floor).
//!
//! The cache is pure: every method takes the current time as a parameter, so the rules are unit
//! tested against an injected clock.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use tn_kad_client::{BlsPublicKey, KadClientError, NodeRecord, VerifiedRecord};

/// A record whose last successful fetch is older than this many refresh intervals is reported as
/// `stale`. Two intervals tolerate exactly one failed cycle before a record is flagged, so a
/// single transient lookup failure does not flap the site's display.
pub const STALE_AFTER_INTERVALS: u32 = 2;

/// Eviction and staleness thresholds.
#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    /// Drop a record not successfully refreshed for this long.
    pub record_ttl: Duration,
    /// Drop a record whose key has been absent from the tracked set for this many cycles.
    pub absent_cycles_before_evict: u32,
    /// The refresh cadence; scales the staleness threshold (see [`STALE_AFTER_INTERVALS`]).
    pub refresh_interval: Duration,
}

impl CacheConfig {
    /// Age past which a record is reported as `stale`.
    fn stale_after(&self) -> Duration {
        self.refresh_interval.saturating_mul(STALE_AFTER_INTERVALS)
    }
}

/// One cached record and its bookkeeping.
#[derive(Debug, Clone)]
pub struct CachedRecord {
    /// The verified record.
    pub record: NodeRecord,
    /// Unix time of the last cycle that fetched this record.
    pub fetched_at: u64,
    /// The last cycle number whose tracked key set contained this key.
    pub last_seen_in_key_set: u64,
    /// How many valid copies the last successful lookup returned.
    pub copies_seen: usize,
}

/// A read-only view of one cached record with its ages resolved against a point in time.
#[derive(Debug, Clone)]
pub struct RecordView {
    /// The BLS key the record is published under.
    pub key: BlsPublicKey,
    /// The cached entry.
    pub cached: CachedRecord,
    /// Whether the last successful fetch is older than the staleness threshold.
    pub stale: bool,
}

/// What one cycle did to the cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CycleStats {
    /// Keys whose lookup returned a verified record (inserted or refreshed).
    pub found: usize,
    /// Keys whose lookup returned a clean miss.
    pub not_found: usize,
    /// Keys whose lookup failed.
    pub failed: usize,
    /// Entries dropped for exceeding the record TTL.
    pub evicted_ttl: usize,
    /// Entries dropped for being absent from the tracked set too long.
    pub evicted_absent: usize,
}

/// The outcome of a single key's lookup, as the cache sees it.
pub type LookupOutcome = Result<Option<VerifiedRecord>, KadClientError>;

/// The cache proper.
#[derive(Debug)]
pub struct RecordCache {
    /// Cached records keyed by BLS public key.
    entries: BTreeMap<BlsPublicKey, CachedRecord>,
    /// Eviction and staleness thresholds.
    config: CacheConfig,
    /// How many cycles have been applied.
    cycles_completed: u64,
    /// The size of the tracked key set at the last cycle.
    keys_tracked: usize,
    /// Unix time of the last cycle that fetched at least one record.
    last_refresh_at: Option<u64>,
}

impl RecordCache {
    /// An empty cache.
    pub fn new(config: CacheConfig) -> Self {
        Self {
            entries: BTreeMap::new(),
            config,
            cycles_completed: 0,
            keys_tracked: 0,
            last_refresh_at: None,
        }
    }

    /// Fold one refresh cycle's results into the cache (see the module docs for the rules).
    ///
    /// `key_set` is the full tracked set this cycle; `results` is what the DHT returned for it.
    /// A fetched key counts as seen this cycle whether or not it is in `key_set` (it was looked
    /// up, so the caller tracks it); on later cycles only membership in `key_set` counts.
    pub fn apply_cycle(
        &mut self,
        now: u64,
        cycle_no: u64,
        key_set: &BTreeSet<BlsPublicKey>,
        results: Vec<(BlsPublicKey, LookupOutcome)>,
    ) -> CycleStats {
        let mut stats = CycleStats::default();

        for (key, outcome) in results {
            match outcome {
                Ok(Some(verified)) => {
                    stats.found += 1;
                    self.insert_newest(key, verified, now, cycle_no);
                }
                // a miss or a failure keeps the existing entry and lets it age
                Ok(None) => stats.not_found += 1,
                Err(_) => stats.failed += 1,
            }
        }

        // every key in the tracked set was seen this cycle, fetched or not
        for (key, entry) in &mut self.entries {
            if key_set.contains(key) {
                entry.last_seen_in_key_set = cycle_no;
            }
        }

        let ttl = self.config.record_ttl.as_secs();
        let absent_limit = u64::from(self.config.absent_cycles_before_evict);
        self.entries.retain(|_, entry| {
            if now.saturating_sub(entry.fetched_at) > ttl {
                stats.evicted_ttl += 1;
                return false;
            }
            if cycle_no.saturating_sub(entry.last_seen_in_key_set) >= absent_limit {
                stats.evicted_absent += 1;
                return false;
            }
            true
        });

        self.cycles_completed += 1;
        self.keys_tracked = key_set.len();
        if stats.found > 0 {
            self.last_refresh_at = Some(now);
        }
        stats
    }

    /// Insert `verified` under `key` unless the cached record carries a newer signed timestamp.
    fn insert_newest(&mut self, key: BlsPublicKey, verified: VerifiedRecord, now: u64, cycle: u64) {
        let incoming = verified.record.info.timestamp;
        let newer_cached = self
            .entries
            .get(&key)
            .is_some_and(|existing| existing.record.info.timestamp > incoming);
        if newer_cached {
            // a lagging peer served an older copy; keep what we have but note it was fetched
            if let Some(existing) = self.entries.get_mut(&key) {
                existing.fetched_at = now;
                existing.last_seen_in_key_set = cycle;
            }
            return;
        }
        self.entries.insert(
            key,
            CachedRecord {
                record: verified.record,
                fetched_at: now,
                last_seen_in_key_set: cycle,
                copies_seen: verified.copies_seen,
            },
        );
    }

    /// Every cached record, keyed order, with staleness resolved at `now`.
    pub fn snapshot(&self, now: u64) -> Vec<RecordView> {
        self.entries.iter().map(|(key, cached)| self.view(*key, cached, now)).collect()
    }

    /// One cached record, if present, with staleness resolved at `now`.
    pub fn get(&self, key: &BlsPublicKey, now: u64) -> Option<RecordView> {
        self.entries.get(key).map(|cached| self.view(*key, cached, now))
    }

    /// Resolve one entry into a [`RecordView`] at `now`.
    fn view(&self, key: BlsPublicKey, cached: &CachedRecord, now: u64) -> RecordView {
        RecordView { key, cached: cached.clone(), stale: self.is_stale(cached, now) }
    }

    /// Whether `cached`'s last successful fetch is older than the staleness threshold at `now`.
    fn is_stale(&self, cached: &CachedRecord, now: u64) -> bool {
        now.saturating_sub(cached.fetched_at) > self.config.stale_after().as_secs()
    }

    /// How many cycles have been applied.
    pub fn cycles_completed(&self) -> u64 {
        self.cycles_completed
    }

    /// The size of the tracked key set at the last cycle.
    pub fn keys_tracked(&self) -> usize {
        self.keys_tracked
    }

    /// Unix time of the last cycle that fetched at least one record, if any.
    pub fn last_refresh_at(&self) -> Option<u64> {
        self.last_refresh_at
    }

    /// How many records are cached.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache holds no records.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether at least one cached record is not stale at `now`.
    pub fn any_fresh(&self, now: u64) -> bool {
        self.entries.values().any(|cached| !self.is_stale(cached, now))
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Record fixtures for the cache and API tests: real BLS keys (parsed from the testnet
    //! committee, so no signing key is needed) carrying an arbitrary signature, since the cache
    //! never verifies one.

    use super::*;
    use tn_kad_client::{Multiaddr, NetworkInfo, RpcInfo};
    use tn_node_record::parse_bls_pubkey;
    use tn_types::{BlsSignature, NetworkKeypair};

    /// Two distinct, valid BLS keys (the first two testnet authorities).
    pub(crate) const KEY_A: &str = "pDmpE29YEhr93MPVPCGkWx3BsCcow4oExmxv5viJz2GTjsaXwc3YwKxE1CFdVuTVSudsFDutFSVmtgF98Abs56JZPfGQs6GzbqXDFkGA1eZx2edkwfP2Q6eRLo8coLhvTNj";
    pub(crate) const KEY_B: &str = "rZdhyVGJAtz7kyFtFARWVVcw5DB3o9E4GvPN6zuKfUhUBGFRvEK2PgEV44CJt9wu9GMyssFQBFXRknrenuhJixo9KCADrZzHdtcvAwAYDmcLc7Ugh9epGc5LRZC5TK2hnYA";

    pub(crate) fn key(base58: &str) -> BlsPublicKey {
        parse_bls_pubkey(base58).expect("fixture key parses")
    }

    pub(crate) fn rpc() -> RpcInfo {
        RpcInfo {
            http: "https://validator.example:8545/".parse().expect("http url"),
            ws: Some("wss://validator.example:8546/".parse().expect("ws url")),
        }
    }

    /// A record signed-in-name-only with `timestamp`, with or without an RPC advertisement.
    pub(crate) fn record(timestamp: u64, rpc: Option<RpcInfo>) -> NodeRecord {
        let multiaddr: Multiaddr =
            "/ip4/127.0.0.1/udp/49594/quic-v1".parse().expect("static multiaddr parses");
        NodeRecord {
            info: NetworkInfo {
                pubkey: NetworkKeypair::generate_ed25519().public().into(),
                multiaddrs: vec![multiaddr],
                timestamp,
                rpc,
            },
            signature: BlsSignature::default(),
        }
    }

    pub(crate) fn verified(
        key: BlsPublicKey,
        timestamp: u64,
        rpc: Option<RpcInfo>,
    ) -> VerifiedRecord {
        VerifiedRecord { key, record: record(timestamp, rpc), copies_seen: 1 }
    }

    pub(crate) fn config() -> CacheConfig {
        CacheConfig {
            record_ttl: Duration::from_secs(24 * 3_600),
            absent_cycles_before_evict: 3,
            refresh_interval: Duration::from_secs(300),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{test_support::*, *};

    fn keys(list: &[BlsPublicKey]) -> BTreeSet<BlsPublicKey> {
        list.iter().copied().collect()
    }

    #[test]
    fn newest_wins_across_cycles() {
        let a = key(KEY_A);
        let mut cache = RecordCache::new(config());
        let set = keys(&[a]);

        // cycle 1 fetches a record stamped 1_000
        let stats =
            cache.apply_cycle(10_000, 1, &set, vec![(a, Ok(Some(verified(a, 1_000, None))))]);
        assert_eq!(stats.found, 1);
        assert_eq!(cache.get(&a, 10_000).expect("cached").cached.record.info.timestamp, 1_000);

        // cycle 2: a lagging peer serves an older copy; the cache must not roll back, but the
        // fetch is still a successful fetch so the entry does not age
        cache.apply_cycle(10_300, 2, &set, vec![(a, Ok(Some(verified(a, 900, Some(rpc())))))]);
        let view = cache.get(&a, 10_300).expect("cached");
        assert_eq!(view.cached.record.info.timestamp, 1_000);
        assert!(view.cached.record.info.rpc.is_none(), "older copy must not replace");
        assert_eq!(view.cached.fetched_at, 10_300);

        // cycle 3: a newer copy replaces; an equal timestamp also replaces (>=)
        cache.apply_cycle(10_600, 3, &set, vec![(a, Ok(Some(verified(a, 1_500, Some(rpc())))))]);
        let view = cache.get(&a, 10_600).expect("cached");
        assert_eq!(view.cached.record.info.timestamp, 1_500);
        assert!(view.cached.record.info.rpc.is_some());
        assert_eq!(cache.last_refresh_at(), Some(10_600));
    }

    #[test]
    fn lookup_failure_keeps_entry_and_ages_it() {
        let a = key(KEY_A);
        let mut cache = RecordCache::new(config());
        let set = keys(&[a]);
        cache.apply_cycle(10_000, 1, &set, vec![(a, Ok(Some(verified(a, 1_000, None))))]);

        // a miss and then an error: the entry stays, fetched_at does not move
        let stats = cache.apply_cycle(10_300, 2, &set, vec![(a, Ok(None))]);
        assert_eq!(stats.not_found, 1);
        let view = cache.get(&a, 10_300).expect("still cached");
        assert_eq!(view.cached.fetched_at, 10_000);
        assert!(!view.stale, "one failed cycle is inside the two-interval allowance");
        let stats = cache.apply_cycle(10_700, 3, &set, vec![(a, Err(KadClientError::Timeout))]);
        assert_eq!(stats.failed, 1);
        let view = cache.get(&a, 10_700).expect("still cached");
        assert_eq!(view.cached.fetched_at, 10_000);
        // 700s old with a 300s interval: past two intervals, so stale
        assert!(view.stale);
        assert!(!cache.any_fresh(10_700));
        // the threshold is strict: exactly two intervals old is still fresh
        assert!(!cache.get(&a, 10_000 + 600).expect("cached").stale);
        assert!(cache.get(&a, 10_000 + 601).expect("cached").stale);
        // `last_refresh_at` reflects the last cycle that actually fetched something
        assert_eq!(cache.last_refresh_at(), Some(10_000));
    }

    #[test]
    fn ttl_eviction_drops_records_not_refreshed_in_time() {
        let a = key(KEY_A);
        let mut cache = RecordCache::new(config());
        let set = keys(&[a]);
        cache.apply_cycle(10_000, 1, &set, vec![(a, Ok(Some(verified(a, 1_000, None))))]);

        // exactly at the ttl the record survives; one second past it is dropped
        let ttl = config().record_ttl.as_secs();
        let stats = cache.apply_cycle(10_000 + ttl, 2, &set, vec![(a, Ok(None))]);
        assert_eq!(stats.evicted_ttl, 0);
        assert_eq!(cache.len(), 1);
        let stats = cache.apply_cycle(10_000 + ttl + 1, 3, &set, vec![(a, Ok(None))]);
        assert_eq!(stats.evicted_ttl, 1);
        assert!(cache.is_empty());
    }

    #[test]
    fn absent_from_key_set_eviction() {
        let a = key(KEY_A);
        let b = key(KEY_B);
        let mut cache = RecordCache::new(config());
        cache.apply_cycle(
            10_000,
            1,
            &keys(&[a, b]),
            vec![(a, Ok(Some(verified(a, 1, None)))), (b, Ok(Some(verified(b, 1, None))))],
        );
        assert_eq!(cache.len(), 2);

        // `b` leaves the tracked set; with the limit at 3 it survives two absent cycles and is
        // dropped on the third, while `a` (still tracked, still fetched) is untouched
        let only_a = keys(&[a]);
        for cycle in 2..=3 {
            let stats = cache.apply_cycle(
                10_000 + cycle * 300,
                cycle,
                &only_a,
                vec![(a, Ok(Some(verified(a, 1, None))))],
            );
            assert_eq!(stats.evicted_absent, 0, "cycle {cycle}");
            assert_eq!(cache.len(), 2, "cycle {cycle}");
        }
        let stats =
            cache.apply_cycle(11_200, 4, &only_a, vec![(a, Ok(Some(verified(a, 1, None))))]);
        assert_eq!(stats.evicted_absent, 1);
        assert!(cache.get(&b, 11_200).is_none());
        assert!(cache.get(&a, 11_200).is_some());
        assert_eq!(cache.keys_tracked(), 1);
    }

    #[test]
    fn partial_failure_keeps_serving_the_rest() {
        let a = key(KEY_A);
        let b = key(KEY_B);
        let mut cache = RecordCache::new(config());
        let set = keys(&[a, b]);
        cache.apply_cycle(
            10_000,
            1,
            &set,
            vec![(a, Ok(Some(verified(a, 1, Some(rpc()))))), (b, Ok(Some(verified(b, 1, None))))],
        );

        // `a` fails, `b` refreshes: both are still served, `a` from its earlier fetch
        let stats = cache.apply_cycle(
            10_300,
            2,
            &set,
            vec![
                (a, Err(KadClientError::NoBootstrapPeerReachable)),
                (b, Ok(Some(verified(b, 2, None)))),
            ],
        );
        assert_eq!(stats, CycleStats { found: 1, failed: 1, ..CycleStats::default() });
        let snapshot = cache.snapshot(10_300);
        assert_eq!(snapshot.len(), 2);
        let a_view = cache.get(&a, 10_300).expect("a served");
        assert_eq!(a_view.cached.fetched_at, 10_000);
        assert!(!a_view.stale, "one failed cycle is inside the staleness allowance");
        assert!(a_view.cached.record.info.rpc.is_some());
        assert_eq!(cache.get(&b, 10_300).expect("b served").cached.fetched_at, 10_300);
        assert!(cache.any_fresh(10_300));
        assert_eq!(cache.cycles_completed(), 2);
    }
}
