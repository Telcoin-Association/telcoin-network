//! Per-IP and global request rate limiting.
//!
//! Two token-bucket limiters shed load before a request reaches the proxy or
//! has its body buffered: a gateway-wide bucket caps aggregate throughput to
//! roughly what the upstream workers can absorb, and a per-client bucket keyed
//! on the client's *network prefix* caps what one source can take of that
//! budget. An over-limit request receives the gateway's JSON-RPC "rate limit
//! exceeded" envelope (HTTP `429`), never a bare connection reset.
//!
//! The per-client key is the peer address masked to a configurable prefix
//! ([`PrefixPolicy`]), not the bare address. Keyed on the bare address, a client
//! that rotates its source address gets a fresh, full bucket per address and so
//! never accumulates spent budget, which a single IPv6 `/64` makes trivial;
//! masking collapses one allocation onto one bucket. The defaults are `/64` for
//! IPv6 and `/32` for IPv4, so IPv4 behaviour is unchanged and unrelated
//! customers behind one carrier NAT are never grouped. This bounds rotation
//! *within* an allocation, not across them: a client holding many distinct
//! allocations still earns a bucket per allocation, and only the gateway-wide
//! bucket caps their total.
//!
//! The client identity is the immediate TCP peer address (`ConnectInfo`,
//! injected per connection by the accept loop). The gateway is meant to run
//! edge-facing; behind an untrusted L7 proxy the peer is that proxy, so the
//! per-IP bucket would meter the proxy rather than the real client. Terminate
//! such a proxy's client identity upstream, or run the gateway at the edge (see
//! the crate README).

use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
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

/// Width of an IPv4 address, in bits.
const V4_BITS: u8 = 32;

/// Width of an IPv6 address, in bits.
const V6_BITS: u8 = 128;

/// Default IPv6 prefix the client address is masked to before it keys a bucket.
/// A `/64` is the smallest subnet routed to a single customer in practice, so it
/// is the smallest unit a rotating client cannot escape by picking another
/// address.
pub(crate) const DEFAULT_V6_PREFIX: u8 = 64;

/// Default IPv4 prefix the client address is masked to before it keys a bucket.
/// A `/32` is a single address, so it preserves the gateway's historical
/// per-address behaviour exactly and cannot group unrelated customers that share
/// one carrier-grade NAT onto a single bucket. IPv4 addresses are scarce enough
/// that rotation inside one allocation is not the cheap attack it is on IPv6.
pub(crate) const DEFAULT_V4_PREFIX: u8 = 32;

/// Why a prefix length was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrefixLenError {
    /// The requested length is wider than its address family allows.
    TooLong {
        /// The length that was requested, in bits.
        requested: u8,
        /// The widest length the family permits, in bits.
        max: u8,
    },
}

impl fmt::Display for PrefixLenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong { requested, max } => {
                write!(f, "prefix length /{requested} exceeds the maximum /{max} for this family")
            }
        }
    }
}

impl std::error::Error for PrefixLenError {}

/// A network prefix length in bits, validated against its address family so it
/// can never exceed the address width. Only [`PrefixLen::v4`] and
/// [`PrefixLen::v6`] construct one, so masking can never shift past the width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrefixLen(u8);

impl PrefixLen {
    /// An IPv4 prefix length, rejecting anything wider than `/32`.
    pub(crate) fn v4(bits: u8) -> Result<Self, PrefixLenError> {
        Self::checked(bits, V4_BITS)
    }

    /// An IPv6 prefix length, rejecting anything wider than `/128`.
    pub(crate) fn v6(bits: u8) -> Result<Self, PrefixLenError> {
        Self::checked(bits, V6_BITS)
    }

    /// The shared range check: `bits` must fit within the family's `max` width.
    fn checked(bits: u8, max: u8) -> Result<Self, PrefixLenError> {
        (bits <= max).then_some(Self(bits)).ok_or(PrefixLenError::TooLong { requested: bits, max })
    }

    /// The prefix width, in bits.
    fn bits(&self) -> u8 {
        self.0
    }
}

/// The network prefix each address family is masked to before it keys a per-IP
/// bucket, so a client rotating addresses inside one allocation shares a single
/// bucket instead of minting a fresh one per address.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PrefixPolicy {
    /// Prefix applied to IPv4 client addresses.
    v4: PrefixLen,
    /// Prefix applied to IPv6 client addresses.
    v6: PrefixLen,
}

impl PrefixPolicy {
    /// Build a policy from a validated length per address family.
    pub(crate) fn new(v4: PrefixLen, v6: PrefixLen) -> Self {
        Self { v4, v6 }
    }

    /// The IPv4 prefix width, in bits.
    #[cfg(test)]
    pub(crate) fn v4_bits(&self) -> u8 {
        self.v4.bits()
    }

    /// The IPv6 prefix width, in bits.
    #[cfg(test)]
    pub(crate) fn v6_bits(&self) -> u8 {
        self.v6.bits()
    }

    /// The bucket key for `ip`: the address with its host bits cleared, per this
    /// policy's prefix for the address's family.
    ///
    /// A dual-stack listener reports an IPv4 peer as the mapped `::ffff:a.b.c.d`
    /// form. Every such address shares the same fixed top 96 bits, so masking
    /// them as IPv6 would collapse *all* IPv4 clients onto one bucket; they are
    /// unmapped first and keyed by the IPv4 prefix, exactly as an IPv4-only
    /// listener would key them.
    fn key(&self, ip: IpAddr) -> IpAddr {
        match ip {
            IpAddr::V4(addr) => IpAddr::V4(mask_v4(addr, self.v4)),
            IpAddr::V6(addr) => addr.to_ipv4_mapped().map_or_else(
                || IpAddr::V6(mask_v6(addr, self.v6)),
                |unmapped| IpAddr::V4(mask_v4(unmapped, self.v4)),
            ),
        }
    }
}

impl Default for PrefixPolicy {
    /// The shipped defaults: [`DEFAULT_V4_PREFIX`] and [`DEFAULT_V6_PREFIX`].
    fn default() -> Self {
        // Both constants are within their family's width, so neither checked
        // constructor can fail; fall back to the widest (identity) prefix rather
        // than panic if that ever stops holding.
        Self {
            v4: PrefixLen::v4(DEFAULT_V4_PREFIX).unwrap_or(PrefixLen(V4_BITS)),
            v6: PrefixLen::v6(DEFAULT_V6_PREFIX).unwrap_or(PrefixLen(V6_BITS)),
        }
    }
}

/// Clear the host bits of an IPv4 address below `prefix`.
///
/// The host-bit count is `32 - prefix`, which is a full-width shift at
/// `prefix == 0` and so an arithmetic overflow (a panic in debug builds);
/// `checked_shl` returns `None` there and the `unwrap_or(0)` yields the all-zero
/// mask that a `/0` means. At `/32` the shift is zero and the mask is all-ones,
/// i.e. the identity.
fn mask_v4(addr: Ipv4Addr, prefix: PrefixLen) -> Ipv4Addr {
    let host_bits = u32::from(V4_BITS.saturating_sub(prefix.bits()));
    let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
    Ipv4Addr::from(u32::from(addr) & mask)
}

/// Clear the host bits of an IPv6 address below `prefix`. Same full-width shift
/// guard as [`mask_v4`]: `/0` masks to `::`, `/128` is the identity.
fn mask_v6(addr: Ipv6Addr, prefix: PrefixLen) -> Ipv6Addr {
    let host_bits = u32::from(V6_BITS.saturating_sub(prefix.bits()));
    let mask = u128::MAX.checked_shl(host_bits).unwrap_or(0);
    Ipv6Addr::from(u128::from(addr) & mask)
}

/// Per-client-prefix buckets, bounded in cardinality; idle buckets are reclaimed
/// by [`RateLimiters::gc`]. The map is keyed on the *masked* client address (see
/// [`PrefixPolicy`]), so address rotation inside one allocation shares a bucket.
#[derive(Debug)]
struct PerIpLimiter {
    /// The rate and burst every per-client bucket is built with.
    limit: RateLimit,
    /// Ceiling on tracked buckets; beyond it new clients are admitted untracked.
    max_entries: usize,
    /// Live buckets, keyed on the prefix-masked client address.
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
    /// How a client address is masked down to its bucket key.
    prefix: PrefixPolicy,
}

impl PerIpLimiter {
    /// Admit or reject a request from `ip`, creating the bucket for its network
    /// prefix on first sight.
    fn admit(&self, now: Instant, ip: IpAddr) -> bool {
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        // Every address in one allocation collapses onto this key, so a rotating
        // client keeps spending the same budget.
        let key = self.prefix.key(ip);
        let mut buckets = lock(&self.buckets);
        // Bind the existing-bucket outcome first so its borrow of `buckets` ends
        // before the new-IP path takes `&mut buckets`.
        let existing = buckets.get_mut(&key).map(|bucket| bucket.try_admit(now, rate, capacity));
        existing.unwrap_or_else(|| {
            admit_new_ip(&mut buckets, self.max_entries, now, key, rate, capacity)
        })
    }
}

/// Admit a first-seen `ip` (already masked to its network prefix by the
/// caller), tracking it unless the table is at capacity.
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
        prefix: PrefixPolicy,
    ) -> Option<Arc<Self>> {
        Self::with_clock(SystemClock, per_ip, global, max_per_ip_entries, prefix).map(Arc::new)
    }
}

impl<C: Clock> RateLimiters<C> {
    /// Build the limiters over an injected clock. `prefix` decides how a client
    /// address is masked before it keys a per-IP bucket; it is inert when the
    /// per-IP limiter is disabled.
    fn with_clock(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_per_ip_entries: usize,
        prefix: PrefixPolicy,
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
            prefix,
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

    /// An address in `2001:db8::/32`: `subnet` picks the `/64`, `host` the
    /// address within it.
    fn ip6(subnet: u16, host: u16) -> IpAddr {
        IpAddr::from([0x2001, 0x0db8, 0, subnet, 0, 0, 0, host])
    }

    /// A `PrefixPolicy` from raw v4/v6 prefix lengths, which must be in range.
    fn prefixes(v4: u8, v6: u8) -> PrefixPolicy {
        PrefixPolicy::new(
            PrefixLen::v4(v4).expect("v4 prefix in range"),
            PrefixLen::v6(v6).expect("v6 prefix in range"),
        )
    }

    impl<C: Clock> RateLimiters<C> {
        fn per_ip_len(&self) -> usize {
            self.per_ip.as_ref().map(|per_ip| lock(&per_ip.buckets).len()).unwrap_or(0)
        }
    }

    /// Limiters under the shipped prefix policy (`/32` v4, `/64` v6).
    fn limiters<C: Clock>(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_entries: usize,
    ) -> RateLimiters<C> {
        limiters_with_prefix(clock, per_ip, global, max_entries, PrefixPolicy::default())
    }

    /// Limiters under an explicit prefix policy.
    fn limiters_with_prefix<C: Clock>(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        max_entries: usize,
        prefix: PrefixPolicy,
    ) -> RateLimiters<C> {
        RateLimiters::with_clock(clock, per_ip, global, max_entries, prefix)
            .expect("some limiter enabled")
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
        assert!(RateLimiters::with_clock(SystemClock, None, None, 16, PrefixPolicy::default())
            .is_none());
    }

    #[test]
    fn rotation_inside_one_v6_prefix_shares_a_bucket() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 2)), None, 16);
        // Three requests, each from a different address inside one /64. Keyed on
        // the bare address they would be three fresh, full buckets and all three
        // would be admitted; keyed on the /64 they share one burst of 2.
        assert!(limiters.check(Some(ip6(1, 1))).is_ok());
        assert!(limiters.check(Some(ip6(1, 2))).is_ok());
        assert!(
            matches!(limiters.check(Some(ip6(1, 3))), Err(GatewayError::RateLimited)),
            "address rotation inside one /64 must not mint a fresh bucket"
        );
        assert_eq!(limiters.per_ip_len(), 1, "one /64 is one bucket");
    }

    #[test]
    fn rotation_across_v6_prefixes_uses_separate_buckets() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 2)), None, 16);
        // Exhaust the first /64.
        assert!(limiters.check(Some(ip6(1, 1))).is_ok());
        assert!(limiters.check(Some(ip6(1, 2))).is_ok());
        assert!(limiters.check(Some(ip6(1, 3))).is_err());
        // A different /64 is a different customer, so it gets its own bucket.
        // This is the residual limit: prefix keying bounds rotation within an
        // allocation, not across allocations.
        assert!(limiters.check(Some(ip6(2, 1))).is_ok());
        assert_eq!(limiters.per_ip_len(), 2);
    }

    #[test]
    fn ipv4_default_prefix_preserves_per_address_buckets() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), None, 16);
        // The default IPv4 prefix is /32, so neighbouring addresses stay
        // independent exactly as before prefix keying.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(1))).is_err());
        assert!(limiters.check(Some(ip(2))).is_ok());
        assert_eq!(limiters.per_ip_len(), 2);
    }

    #[test]
    fn ipv4_mapped_peers_key_as_ipv4() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), None, 16);
        let mapped = |last: u8| IpAddr::V6(Ipv4Addr::new(10, 0, 0, last).to_ipv6_mapped());
        // A dual-stack listener sees IPv4 peers as `::ffff:a.b.c.d`, which all
        // share their top 96 bits: masked as IPv6 under the /64 default they
        // would become one bucket for every IPv4 client on earth.
        assert!(limiters.check(Some(mapped(1))).is_ok());
        assert!(limiters.check(Some(mapped(2))).is_ok(), "mapped peers must not share a bucket");
        // The mapped form keys to the same bucket as the bare IPv4 address,
        // whose single token the first request already spent.
        assert!(limiters.check(Some(ip(1))).is_err());
        assert_eq!(limiters.per_ip_len(), 2);
    }

    #[test]
    fn prefix_zero_collapses_every_address_to_one_bucket() {
        let limiters =
            limiters_with_prefix(ManualClock::new(), Some(limit(1, 1)), None, 16, prefixes(0, 0));
        // A /0 is the degenerate policy: every address in a family shares one
        // bucket, and the full-width shift it implies must not panic.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_err(), "a /0 keys all of IPv4 to one bucket");
        assert!(limiters.check(Some(ip6(1, 1))).is_ok());
        assert!(limiters.check(Some(ip6(2, 9))).is_err(), "a /0 keys all of IPv6 to one bucket");
        assert_eq!(limiters.per_ip_len(), 2, "one bucket per address family");
    }

    #[test]
    fn prefix_zero_masks_to_the_unspecified_address() {
        let v4 = PrefixLen::v4(0).expect("/0 is in range");
        let v6 = PrefixLen::v6(0).expect("/0 is in range");
        assert_eq!(mask_v4(Ipv4Addr::new(203, 0, 113, 7), v4), Ipv4Addr::UNSPECIFIED);
        assert_eq!(
            mask_v6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 9), v6),
            Ipv6Addr::UNSPECIFIED
        );
    }

    #[test]
    fn max_prefix_masking_is_identity() {
        let v4 = PrefixLen::v4(32).expect("/32 is in range");
        let v6 = PrefixLen::v6(128).expect("/128 is in range");
        let addr4 = Ipv4Addr::new(203, 0, 113, 7);
        let addr6 = Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 9);
        assert_eq!(mask_v4(addr4, v4), addr4);
        assert_eq!(mask_v6(addr6, v6), addr6);
    }

    #[test]
    fn masking_clears_only_the_host_bits() {
        let v4 = PrefixLen::v4(24).expect("/24 is in range");
        let v6 = PrefixLen::v6(64).expect("/64 is in range");
        assert_eq!(mask_v4(Ipv4Addr::new(203, 0, 113, 7), v4), Ipv4Addr::new(203, 0, 113, 0));
        assert_eq!(
            mask_v6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0xdead, 0xbeef, 0, 9), v6),
            Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 0)
        );
    }

    #[test]
    fn out_of_range_prefix_is_rejected() {
        assert_eq!(
            PrefixLen::v4(33),
            Err(PrefixLenError::TooLong { requested: 33, max: 32 }),
            "a /33 is not an IPv4 prefix"
        );
        assert_eq!(
            PrefixLen::v6(129),
            Err(PrefixLenError::TooLong { requested: 129, max: 128 }),
            "a /129 is not an IPv6 prefix"
        );
        assert!(PrefixLen::v4(32).is_ok());
        assert!(PrefixLen::v6(128).is_ok());
        assert!(PrefixLen::v4(0).is_ok());
        assert!(PrefixLen::v6(0).is_ok());
    }
}
