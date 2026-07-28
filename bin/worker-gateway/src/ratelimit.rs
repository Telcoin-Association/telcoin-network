//! Per-IP and global request rate limiting.
//!
//! Two token-bucket limiters shed load before a request reaches the proxy or
//! has its body buffered: a gateway-wide bucket caps aggregate throughput to
//! roughly what the upstream workers can absorb, and a per-client-IP bucket
//! stops any single source monopolizing that budget. An over-limit request
//! receives the gateway's JSON-RPC "rate limit exceeded" envelope (HTTP `429`),
//! never a bare connection reset.
//!
//! The client identity is the immediate TCP peer address (`ConnectInfo`,
//! injected per connection by the accept loop). The gateway is meant to run
//! edge-facing; behind an untrusted L7 proxy the peer is that proxy, so the
//! per-IP bucket would meter the proxy rather than the real client. Terminate
//! such a proxy's client identity upstream, or run the gateway at the edge (see
//! the crate README).

use std::{
    collections::HashMap,
    net::{IpAddr, SocketAddr},
    num::NonZeroU32,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use axum::{
    extract::{ConnectInfo, Request, State},
    middleware::Next,
    response::Response,
};
use tn_types::{Noticer, TaskError};
use tokio::time::MissedTickBehavior;

use crate::{
    error::{error_response, GatewayError},
    server::{HEALTH_PATH, READY_PATH},
};

/// How often the background sweep reclaims idle per-IP buckets.
pub(crate) const GC_INTERVAL: Duration = Duration::from_secs(60);

/// Default ceiling on tracked per-IP buckets. Bounds the per-IP map so a wide
/// spread of source IPs cannot grow it without limit; ~10 MB at the cap, and
/// idle buckets are reclaimed between sweeps by [`RateLimiters::gc`].
pub(crate) const DEFAULT_MAX_PER_IP_ENTRIES: usize = 100_000;

/// A source of monotonic time, injectable so the limiters can be driven
/// deterministically in tests.
pub(crate) trait Clock: Send + Sync + 'static {
    fn now(&self) -> Instant;
}

/// The production clock: the process monotonic clock.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A resolved rate limit: a sustained `rate` (requests per second) with a
/// `burst` ceiling (the token-bucket capacity).
#[derive(Debug, Clone, Copy)]
pub(crate) struct RateLimit {
    rate: NonZeroU32,
    burst: NonZeroU32,
}

impl RateLimit {
    pub(crate) fn new(rate: NonZeroU32, burst: NonZeroU32) -> Self {
        Self { rate, burst }
    }

    #[cfg(test)]
    pub(crate) fn rate(&self) -> NonZeroU32 {
        self.rate
    }

    #[cfg(test)]
    pub(crate) fn burst(&self) -> NonZeroU32 {
        self.burst
    }

    /// Sustained refill rate in tokens per second.
    fn tokens_per_sec(&self) -> f64 {
        f64::from(self.rate.get())
    }

    /// Bucket capacity in tokens.
    fn capacity(&self) -> f64 {
        f64::from(self.burst.get())
    }
}

/// A refilling token bucket. It starts full and refills at `rate` tokens per
/// second up to `capacity`; each admitted request spends one token.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn full(now: Instant, capacity: f64) -> Self {
        Self { tokens: capacity, last_refill: now }
    }

    /// The token count after refilling to `now`, clamped to `capacity`, without
    /// mutating the bucket.
    fn replenished(&self, now: Instant, rate: f64, capacity: f64) -> f64 {
        let elapsed = now.saturating_duration_since(self.last_refill).as_secs_f64();
        (self.tokens + elapsed * rate).min(capacity)
    }

    /// Refill for the elapsed time, then spend one token if one is available.
    /// Returns whether the request is admitted.
    fn try_admit(&mut self, now: Instant, rate: f64, capacity: f64) -> bool {
        self.tokens = self.replenished(now, rate, capacity);
        self.last_refill = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Whether the bucket has refilled to capacity by `now`. An idle bucket
    /// carries no state a freshly-created one would not, so the GC sweep can
    /// drop it.
    fn is_idle(&self, now: Instant, rate: f64, capacity: f64) -> bool {
        self.replenished(now, rate, capacity) >= capacity
    }
}

/// The single gateway-wide bucket.
#[derive(Debug)]
struct GlobalLimiter {
    limit: RateLimit,
    bucket: Mutex<Bucket>,
}

/// Per-client-IP buckets, bounded in cardinality; idle buckets are reclaimed by
/// [`RateLimiters::gc`].
#[derive(Debug)]
struct PerIpLimiter {
    limit: RateLimit,
    max_entries: usize,
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
}

impl PerIpLimiter {
    /// Admit or reject a request from `ip`, creating its bucket on first sight.
    fn admit(&self, now: Instant, ip: IpAddr) -> bool {
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        let mut buckets = lock(&self.buckets);
        // Bind the existing-bucket outcome first so its borrow of `buckets` ends
        // before the new-IP path takes `&mut buckets`.
        let existing = buckets.get_mut(&ip).map(|bucket| bucket.try_admit(now, rate, capacity));
        existing.unwrap_or_else(|| {
            admit_new_ip(&mut buckets, self.max_entries, now, ip, rate, capacity)
        })
    }
}

/// Admit a first-seen `ip`, tracking it unless the table is at capacity.
///
/// A new IP with the table already full is admitted *untracked* rather than
/// evicting a live bucket or rejecting a fresh client: the global limit still
/// bounds aggregate load, and the GC sweep keeps the table from staying full.
/// Bounded memory is chosen over perfect per-IP fairness under a very wide
/// source-IP spread.
fn admit_new_ip(
    buckets: &mut HashMap<IpAddr, Bucket>,
    max_entries: usize,
    now: Instant,
    ip: IpAddr,
    rate: f64,
    capacity: f64,
) -> bool {
    if buckets.len() < max_entries {
        let mut bucket = Bucket::full(now, capacity);
        let admitted = bucket.try_admit(now, rate, capacity);
        buckets.insert(ip, bucket);
        admitted
    } else {
        // Table full: admit untracked (bounded memory over per-IP fairness).
        true
    }
}

/// The gateway's rate limiters. Either limiter may be disabled (`None`).
pub(crate) struct RateLimiters<C: Clock = SystemClock> {
    clock: C,
    global: Option<GlobalLimiter>,
    per_ip: Option<PerIpLimiter>,
}

impl RateLimiters<SystemClock> {
    /// Build the limiters from resolved settings using the system clock, or
    /// `None` when both limiters are disabled (so no layer need be installed).
    pub(crate) fn new(
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_per_ip_entries: usize,
    ) -> Option<Arc<Self>> {
        Self::with_clock(SystemClock, per_ip, global, max_per_ip_entries).map(Arc::new)
    }
}

impl<C: Clock> RateLimiters<C> {
    fn with_clock(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_per_ip_entries: usize,
    ) -> Option<Self> {
        if per_ip.is_none() && global.is_none() {
            return None;
        }
        let now = clock.now();
        let global = global.map(|limit| GlobalLimiter {
            limit,
            bucket: Mutex::new(Bucket::full(now, limit.capacity())),
        });
        let per_ip = per_ip.map(|limit| PerIpLimiter {
            limit,
            max_entries: max_per_ip_entries.max(1),
            buckets: Mutex::new(HashMap::new()),
        });
        Some(Self { clock, global, per_ip })
    }

    /// Admit or reject a request from `peer`. A `None` peer skips the per-IP
    /// bucket; the global bucket still applies.
    ///
    /// The global bucket is evaluated first and short-circuits (`&&`), so a
    /// request rejected by the global limit does not spend a per-IP token. This
    /// keeps the aggregate cap authoritative and can only reduce, never inflate,
    /// admitted load.
    pub(crate) fn check(&self, peer: Option<IpAddr>) -> Result<(), GatewayError> {
        let now = self.clock.now();
        let global_ok = self.global.as_ref().is_none_or(|global| {
            lock(&global.bucket).try_admit(
                now,
                global.limit.tokens_per_sec(),
                global.limit.capacity(),
            )
        });
        let allowed = global_ok
            && self.per_ip.as_ref().zip(peer).is_none_or(|(per_ip, ip)| per_ip.admit(now, ip));
        allowed.then_some(()).ok_or(GatewayError::RateLimited)
    }

    /// Drop idle (fully-refilled) per-IP buckets to bound memory. Called
    /// periodically from a background task.
    pub(crate) fn gc(&self) {
        if let Some(per_ip) = &self.per_ip {
            let now = self.clock.now();
            let rate = per_ip.limit.tokens_per_sec();
            let capacity = per_ip.limit.capacity();
            let mut buckets = lock(&per_ip.buckets);
            buckets.retain(|_, bucket| !bucket.is_idle(now, rate, capacity));
        }
    }
}

/// Lock a mutex, recovering the guard if a previous holder panicked. The
/// limiter state is a best-effort counter, so a poisoned lock is safe to reuse.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Periodically reclaim idle per-IP buckets until `shutdown` fires. Runs as a
/// managed task alongside the server; a missed interval is skipped rather than
/// firing catch-up sweeps back to back.
pub(crate) async fn run_gc(
    limiters: Arc<RateLimiters>,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let mut ticker = tokio::time::interval(GC_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Consume the immediate first tick so the first real sweep is one interval
    // out, not at startup against an empty map.
    ticker.tick().await;
    loop {
        tokio::select! {
            () = &shutdown => break,
            _ = ticker.tick() => limiters.gc(),
        }
    }
    Ok(())
}

/// Axum middleware: rate-limit by peer IP and globally, rejecting an over-limit
/// request with the gateway's JSON-RPC `429` envelope before its body is read.
pub(crate) async fn rate_limit(
    State(limiters): State<Arc<RateLimiters>>,
    request: Request,
    next: Next,
) -> Response {
    // Orchestration probes are never rate-limited: a liveness/readiness check
    // failing under load would take the gateway down (the orchestrator kills or
    // depools the pod) exactly when it is meant to be absorbing a flood.
    let path = request.uri().path();
    let exempt = path == HEALTH_PATH || path == READY_PATH;
    let peer = request.extensions().get::<ConnectInfo<SocketAddr>>().map(|info| info.0.ip());
    let rejection = (!exempt).then(|| limiters.check(peer).err()).flatten();
    // The final dispatch stays a `match`: one arm awaits `next`, which a
    // combinator closure cannot do.
    match rejection {
        Some(err) => error_response(&err, b""),
        None => next.run(request).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hand-advanced clock so token refills are deterministic without sleeps.
    #[derive(Clone)]
    struct ManualClock {
        now: Arc<Mutex<Instant>>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self { now: Arc::new(Mutex::new(Instant::now())) }
        }

        fn advance(&self, by: Duration) {
            *lock(&self.now) += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            *lock(&self.now)
        }
    }

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("nonzero")
    }

    fn limit(rate: u32, burst: u32) -> RateLimit {
        RateLimit::new(nz(rate), nz(burst))
    }

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([10, 0, 0, last])
    }

    impl<C: Clock> RateLimiters<C> {
        fn per_ip_len(&self) -> usize {
            self.per_ip.as_ref().map(|per_ip| lock(&per_ip.buckets).len()).unwrap_or(0)
        }
    }

    fn limiters<C: Clock>(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_entries: usize,
    ) -> RateLimiters<C> {
        RateLimiters::with_clock(clock, per_ip, global, max_entries).expect("some limiter enabled")
    }

    #[test]
    fn global_bucket_admits_burst_then_rejects() {
        let limiters = limiters(ManualClock::new(), None, Some(limit(1, 3)), 16);
        // The burst of 3 is admitted; the 4th is rejected (no time advances, so
        // nothing refills).
        assert!(limiters.check(None).is_ok());
        assert!(limiters.check(None).is_ok());
        assert!(limiters.check(None).is_ok());
        assert!(matches!(limiters.check(None), Err(GatewayError::RateLimited)));
    }

    #[test]
    fn global_bucket_refills_over_time() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), None, Some(limit(10, 1)), 16);
        assert!(limiters.check(None).is_ok()); // spend the one token
        assert!(limiters.check(None).is_err()); // empty
        clock.advance(Duration::from_millis(100)); // 10/s => 1 token in 100ms
        assert!(limiters.check(None).is_ok());
    }

    #[test]
    fn per_ip_buckets_are_isolated() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 2)), None, 16);
        // IP 1 exhausts its burst.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(1))).is_err());
        // IP 2 is unaffected.
        assert!(limiters.check(Some(ip(2))).is_ok());
    }

    #[test]
    fn missing_peer_skips_per_ip_but_global_still_applies() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), Some(limit(1, 2)), 16);
        // No peer => the per-IP bucket is skipped; the global burst of 2 caps it.
        assert!(limiters.check(None).is_ok());
        assert!(limiters.check(None).is_ok());
        assert!(limiters.check(None).is_err());
    }

    #[test]
    fn gc_reclaims_idle_buckets() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), Some(limit(10, 2)), None, 16);
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
        // Advance well past a full refill, then sweep.
        clock.advance(Duration::from_secs(1));
        limiters.gc();
        assert_eq!(limiters.per_ip_len(), 0);
    }

    #[test]
    fn busy_bucket_survives_gc() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), Some(limit(1, 5)), None, 16);
        // Drain the bucket so it is not idle, then immediately sweep.
        for _ in 0..5 {
            assert!(limiters.check(Some(ip(1))).is_ok());
        }
        limiters.gc();
        assert_eq!(limiters.per_ip_len(), 1, "a non-idle bucket must not be reclaimed");
    }

    #[test]
    fn full_table_admits_untracked_new_ip() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), None, 1);
        // The first IP is tracked and exhausted.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(1))).is_err());
        // The table is full (cap 1): a new IP is admitted untracked, not rejected.
        assert!(limiters.check(Some(ip(2))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
    }

    #[test]
    fn both_disabled_yields_no_limiters() {
        assert!(RateLimiters::with_clock(SystemClock, None, None, 16).is_none());
    }
}
