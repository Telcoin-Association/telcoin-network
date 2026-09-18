//! The refresh loop: resolve the key set, fetch every key's record from the DHT, fold the results
//! into the cache, and publish metrics.
//!
//! # One `KadClient` per cycle
//!
//! Every cycle spawns a fresh [`KadClient`] and shuts it down at the end, rather than keeping one
//! alive for the process lifetime. The reason is on the node side, in
//! `crates/network-libp2p/src/peers/manager.rs`: when a node sits at or over its
//! `target_num_peers`, its heartbeat's `PeerManager::prune_connected_peers` disconnects the
//! excess, choosing non-validator, non-kad-routable peers first, which is exactly what this
//! client is. The disconnect goes through `PeerManager::disconnect_peer` and then
//! `PeerManager::temporarily_ban`, a bounded reconnection-timeout cache **keyed by `PeerId`**
//! (the IP is not banned). A long-lived client would be pruned and then refused for the ban
//! duration on every subsequent cycle; `KadClient::spawn` generates a fresh ephemeral ed25519
//! identity each time, so a per-cycle client is never the peer that was banned. It also removes
//! any need for a "restart the DHT after N failed cycles" supervisor: there is no long-lived DHT
//! state to supervise. The cost is one QUIC dial per bootstrap peer per cycle, negligible at a
//! minutes-scale interval.
//!
//! # Scheduling
//!
//! Cycles run on a plain interval (missed ticks are skipped, never bursted) and, when the live
//! key source is configured, additionally at each epoch boundary plus `epoch_grace` so the new
//! committee's records are picked up promptly. A change in the observed epoch id is logged as
//! the authoritative rollover; it needs no special handling beyond the new committee being in the
//! key set from that cycle on. Each distinct boundary value is armed once: the boundary is the
//! registry's *scheduled* close, and the actual close lands on the first block after it, so a
//! cycle that runs at boundary-plus-grace can still see the old epoch. Re-arming that same, now
//! past, boundary would fire immediately and spin cycles until the close landed. The loop selects
//! on shutdown, so teardown is bounded by one in-flight cycle.

use std::{
    num::NonZeroUsize,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use tn_kad_client::{BlsPublicKey, KadClient, KadClientConfig, Multiaddr, NetworkType};
use tn_types::{now, Noticer, TaskError};
use tracing::{info, warn};

use crate::{
    cache::{CycleStats, LookupOutcome, RecordCache},
    epoch::{EpochInfoSummary, RefreshScheduler, Wake},
    keys::KeySet,
    telemetry::{self, CycleOutcome},
};

/// Everything a cycle needs to reach the DHT.
#[derive(Debug, Clone)]
pub struct RefreshConfig {
    /// The chain whose worker DHT is read.
    pub chain_id: u64,
    /// The worker network to read; only worker records carry RPC info.
    pub network_type: NetworkType,
    /// Worker DHT bootstrap peers, each with `/p2p/<peer-id>`.
    pub bootstrap: Vec<Multiaddr>,
    /// Per-lookup deadline (also bounds the bootstrap dial).
    pub query_timeout: Duration,
    /// Lookups in flight at once.
    pub lookup_concurrency: NonZeroUsize,
}

impl RefreshConfig {
    /// The client config for one cycle.
    fn client_config(&self) -> KadClientConfig {
        KadClientConfig::new(self.chain_id, self.network_type, self.bootstrap.clone())
            .with_query_timeout(self.query_timeout)
    }
}

/// The shared cache handle the refresh loop writes and the API reads.
///
/// A `std` lock (not `tokio`): every critical section is a synchronous fold or snapshot, never
/// held across an `.await`.
pub type SharedCache = Arc<RwLock<RecordCache>>;

/// What one cycle produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleReport {
    /// The cycle's sequence number.
    pub cycle_no: u64,
    /// How many keys were tracked.
    pub keys: usize,
    /// The cache's accounting of the results.
    pub stats: CycleStats,
    /// The epoch summary from the live source, if it refreshed this cycle.
    pub epoch: Option<EpochInfoSummary>,
    /// The `outcome` label.
    pub outcome: CycleOutcome,
}

/// Run one refresh cycle: resolve keys, spawn a client, look every key up, shut the client down,
/// fold the results into `cache`, and record metrics.
///
/// Never fails: every failure mode is folded into the cache (entries age rather than vanish) and
/// reported through the returned [`CycleReport`] and the metrics.
pub async fn run_cycle(
    config: &RefreshConfig,
    keys: &mut KeySet,
    cache: &SharedCache,
    cycle_no: u64,
) -> CycleReport {
    let started = Instant::now();
    let resolved = keys.resolve().await;
    for source in &resolved.failed_sources {
        telemetry::record_key_source_failure(source);
    }
    let key_set = resolved.keys;
    let key_list: Vec<_> = key_set.iter().copied().collect();

    let results =
        if key_list.is_empty() { Vec::new() } else { lookup_all(config, &key_list).await };
    for (_, outcome) in &results {
        match outcome {
            Ok(Some(_)) => {}
            Ok(None) => telemetry::record_lookup_failure(telemetry::NOT_FOUND_REASON),
            Err(err) => telemetry::record_lookup_failure(telemetry::lookup_failure_reason(err)),
        }
    }

    let now = now();
    let (stats, gauges) = {
        // the fold and the gauge snapshot are synchronous; the lock is never held across an await
        let mut cache = cache.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        let stats = cache.apply_cycle(now, cycle_no, &key_set, results);
        (stats, CacheGauges::from_cache(&cache, now))
    };

    let outcome = match (stats.found, key_list.len()) {
        (0, _) => CycleOutcome::Failed,
        (found, keys) if found == keys => CycleOutcome::Ok,
        _ => CycleOutcome::Partial,
    };
    telemetry::record_cycle(outcome, started.elapsed());
    telemetry::set_cache_gauges(gauges.keys_tracked, gauges.cached, gauges.stale, gauges.with_rpc);
    if let Some(last) = gauges.last_refresh_at {
        telemetry::set_last_successful_refresh(last);
    }

    let report =
        CycleReport { cycle_no, keys: key_list.len(), stats, epoch: resolved.epoch, outcome };
    log_cycle(&report, started.elapsed(), gauges);
    report
}

/// Spawn a client, look every key up, and shut the client down. A spawn failure is reported as
/// that error for every key, so the cache treats it exactly like a per-key lookup failure.
async fn lookup_all(
    config: &RefreshConfig,
    keys: &[BlsPublicKey],
) -> Vec<(BlsPublicKey, LookupOutcome)> {
    let client = match KadClient::spawn(config.client_config()).await {
        Ok(client) => client,
        Err(err) => return keys.iter().map(|key| (*key, Err(err.clone()))).collect(),
    };
    let results = client.get_node_records(keys, config.lookup_concurrency).await;
    client.shutdown().await;
    results
}

/// The gauge inputs read from the cache right after a fold.
#[derive(Debug, Clone, Copy)]
struct CacheGauges {
    /// Tracked keys.
    keys_tracked: usize,
    /// Cached records.
    cached: usize,
    /// Stale records.
    stale: usize,
    /// Records advertising RPC.
    with_rpc: usize,
    /// Last successful refresh.
    last_refresh_at: Option<u64>,
}

impl CacheGauges {
    fn from_cache(cache: &RecordCache, now: u64) -> Self {
        let snapshot = cache.snapshot(now);
        Self {
            keys_tracked: cache.keys_tracked(),
            cached: snapshot.len(),
            stale: snapshot.iter().filter(|view| view.stale).count(),
            with_rpc: snapshot.iter().filter(|view| view.cached.record.info.rpc.is_some()).count(),
            last_refresh_at: cache.last_refresh_at(),
        }
    }
}

/// One operator-visible line per cycle; failures at `warn`, everything else at `info`.
fn log_cycle(report: &CycleReport, elapsed: Duration, gauges: CacheGauges) {
    let CycleStats { found, not_found, failed, evicted_ttl, evicted_absent } = report.stats;
    if report.outcome == CycleOutcome::Failed {
        warn!(
            target: "tn::node_record_api",
            cycle = report.cycle_no,
            keys = report.keys,
            failed,
            not_found,
            cached = gauges.cached,
            stale = gauges.stale,
            elapsed = ?elapsed,
            "refresh cycle fetched no records"
        );
    } else {
        info!(
            target: "tn::node_record_api",
            cycle = report.cycle_no,
            outcome = report.outcome.label(),
            keys = report.keys,
            found,
            not_found,
            failed,
            evicted_ttl,
            evicted_absent,
            cached = gauges.cached,
            stale = gauges.stale,
            with_rpc = gauges.with_rpc,
            elapsed = ?elapsed,
            "refresh cycle complete"
        );
    }
}

/// Run cycles until `shutdown` fires: one immediately, then on the interval and at each known
/// epoch boundary plus `epoch_grace`.
pub async fn run_loop(
    config: RefreshConfig,
    mut keys: KeySet,
    cache: SharedCache,
    refresh_interval: Duration,
    epoch_grace: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let mut scheduler = RefreshScheduler::new(refresh_interval, epoch_grace);
    let mut cycle_no: u64 = 0;
    let mut last_epoch: Option<u32> = None;
    let mut armed_boundary: Option<u64> = None;
    loop {
        cycle_no += 1;
        let report = tokio::select! {
            () = &shutdown => break,
            report = run_cycle(&config, &mut keys, &cache, cycle_no) => report,
        };
        if let Some(epoch) = report.epoch {
            if last_epoch.is_some_and(|previous| previous != epoch.epoch_id) {
                info!(
                    target: "tn::node_record_api",
                    previous = last_epoch,
                    epoch = epoch.epoch_id,
                    next_boundary = epoch.next_boundary_unix,
                    "epoch rollover observed; new committee is now tracked"
                );
            }
            last_epoch = Some(epoch.epoch_id);
            // arm each boundary value once; see the module docs for why a repeat is not re-armed
            if armed_boundary != Some(epoch.next_boundary_unix) {
                armed_boundary = Some(epoch.next_boundary_unix);
                scheduler.arm_boundary(now(), Some(epoch.next_boundary_unix));
            }
        }
        let wake = tokio::select! {
            () = &shutdown => break,
            wake = scheduler.wait() => wake,
        };
        if wake == Wake::EpochBoundary {
            info!(target: "tn::node_record_api", "epoch boundary reached; refreshing early");
        }
    }
    info!(target: "tn::node_record_api", "refresh loop stopped");
    Ok(())
}
