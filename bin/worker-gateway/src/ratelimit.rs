//! Per-IP and global request rate limiting.
//!
//! Two token-bucket limiters shed load before a request reaches the proxy or
//! has its body buffered: a gateway-wide bucket caps aggregate throughput to
//! roughly what the upstream workers can absorb, and a per-client bucket keyed
//! on the client's *network prefix* caps what one source can take of that
//! budget. An over-limit request receives the gateway's JSON-RPC "rate limit
//! exceeded" envelope (HTTP `429`), never a bare connection reset.
//!
//! A JSON-RPC batch costs one token per call. The edge layer charges the first
//! before the body is read; the proxy handler charges the rest once it has
//! read the body and counted the calls (see [`RateLimiters::charge_batch`]).
//!
//! Every charge runs the per-client bucket first and the global bucket only
//! for a request the per-client bucket admits, and a global refusal refunds
//! the per-client tokens, so neither limit spends the other's budget on a
//! request it refused. One helper, `RateLimiters::try_admit_then`, holds that
//! rule for the edge check and the batch charge alike.
//!
//! The per-client key is the client address masked to a configurable prefix
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
//! The client identity is the request's [`ClientAddr`], resolved by the
//! identity middleware, which runs before this one: outermost first, the
//! router's layers are `set_nosniff`, `identify`, the access log, CORS, this
//! rate limit, the byte budget, the request deadline and the body limit (see
//! `crate::server::router`). It is the immediate TCP peer, or, when that peer
//! is in `--trusted-proxies`, the client it forwarded the request for (see
//! [`crate::identity`]). Behind a proxy that is not listed the peer is that
//! proxy, so the per-IP bucket meters the proxy rather than the real client.
//!
//! A third bucket, the submission budget (`--rate-limit-submissions`), keeps
//! reads from spending the rate that submissions need. With it set, the edge
//! runs the per-IP check first and still refuses an over-limit client there,
//! but no longer refuses a request the global bucket turns down: it hands the
//! verdict to the proxy as a [`SubmissionBudget`] extension, and the proxy
//! settles it once the request is classified. A submission is charged to the
//! submission bucket whatever the global bucket said, and any other request is
//! refused when the global bucket refused it. Without the flag the edge check
//! is [`RateLimiters::check`].

use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::NonZeroU32,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
};
use tn_types::{Noticer, TaskError};
use tokio::time::MissedTickBehavior;

use crate::{
    error::{error_response, GatewayError},
    identity::ClientAddr,
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

    /// Bucket capacity: the most tokens one request can be charged at once.
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

    /// Refill for the elapsed time, then spend `cost` tokens if that many are
    /// available; nothing is spent otherwise. Returns whether the request is
    /// admitted.
    fn try_admit(&mut self, now: Instant, rate: f64, capacity: f64, cost: f64) -> bool {
        self.tokens = self.replenished(now, rate, capacity);
        self.last_refill = now;
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Spend `cost` tokens as [`Bucket::try_admit`] does, then ask `next`
    /// whether the request may proceed. `next` runs only when this bucket
    /// admits, and a refusal from it refunds the tokens, so a request is
    /// charged here only when both admit it.
    fn try_admit_then(
        &mut self,
        now: Instant,
        rate: f64,
        capacity: f64,
        cost: f64,
        next: impl FnOnce() -> bool,
    ) -> bool {
        if !self.try_admit(now, rate, capacity, cost) {
            return false;
        }
        let admitted = next();
        if !admitted {
            self.refund(cost, capacity);
        }
        admitted
    }

    /// Give back `cost` tokens spent on a request that a later check then
    /// refused, never past `capacity`, so that refusal costs this bucket
    /// nothing.
    fn refund(&mut self, cost: f64, capacity: f64) {
        self.tokens = (self.tokens + cost).min(capacity);
    }

    /// Whether the bucket has refilled to capacity by `now`. An idle bucket
    /// carries no state a freshly-created one would not, so the GC sweep can
    /// drop it.
    fn is_idle(&self, now: Instant, rate: f64, capacity: f64) -> bool {
        self.replenished(now, rate, capacity) >= capacity
    }
}

/// A single gateway-wide bucket: the global limit, or the submission budget.
#[derive(Debug)]
struct GlobalLimiter {
    limit: RateLimit,
    bucket: Mutex<Bucket>,
}

impl GlobalLimiter {
    /// Refill to `now`, then spend `cost` tokens if that many are available.
    fn admit(&self, now: Instant, cost: f64) -> bool {
        lock(&self.bucket).try_admit(now, self.limit.tokens_per_sec(), self.limit.capacity(), cost)
    }
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
    pub(crate) fn bits(&self) -> u8 {
        self.0
    }
}

/// The network prefix each address family is masked to before it keys a per-IP
/// bucket and the accept loop's per-client connection count, so a client
/// rotating addresses inside one allocation shares one bucket and one count
/// instead of minting a fresh one per address.
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

    /// The client key for `ip`: the address with its host bits cleared, per this
    /// policy's prefix for the address's family. It keys both the per-IP bucket
    /// and the accept loop's per-client connection cap (see [`crate::server`]).
    ///
    /// A dual-stack listener reports an IPv4 peer as the mapped `::ffff:a.b.c.d`
    /// form. Every such address shares the same fixed top 96 bits, so masking
    /// them as IPv6 would collapse *all* IPv4 clients onto one bucket; they are
    /// unmapped first and keyed by the IPv4 prefix, exactly as an IPv4-only
    /// listener would key them.
    pub(crate) fn key(&self, ip: IpAddr) -> IpAddr {
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
pub(crate) fn mask_v4(addr: Ipv4Addr, prefix: PrefixLen) -> Ipv4Addr {
    let host_bits = u32::from(V4_BITS.saturating_sub(prefix.bits()));
    let mask = u32::MAX.checked_shl(host_bits).unwrap_or(0);
    Ipv4Addr::from(u32::from(addr) & mask)
}

/// Clear the host bits of an IPv6 address below `prefix`. Same full-width shift
/// guard as [`mask_v4`]: `/0` masks to `::`, `/128` is the identity.
pub(crate) fn mask_v6(addr: Ipv6Addr, prefix: PrefixLen) -> Ipv6Addr {
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
    /// Ceiling on tracked buckets; a first-seen client beyond it spends from
    /// `overflow` instead of getting a bucket of its own.
    max_entries: usize,
    /// Live buckets, keyed on the prefix-masked client address.
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
    /// One bucket, built with `limit`, shared by every client the full table
    /// cannot track, so untracked clients together get one client's budget.
    overflow: Mutex<Bucket>,
    /// How a client address is masked down to its bucket key.
    prefix: PrefixPolicy,
}

impl PerIpLimiter {
    /// Admit or reject a request from `ip` that costs `cost` tokens. A
    /// first-seen network prefix gets a bucket of its own only when the table
    /// has room and the request is admitted; with the table full it spends
    /// from the shared overflow bucket (see [`Self::admit_new_ip`]). `then` is
    /// asked only once the per-client bucket has admitted the request, and a
    /// refusal from it refunds those tokens to the bucket that was charged,
    /// the overflow bucket included.
    ///
    /// The map lock is held until `then` has answered, so the refund lands on
    /// the bucket this request charged even if a sweep runs concurrently. Lock
    /// order is always this map, then the overflow bucket, then the global
    /// bucket (the only lock a `then` takes); [`RateLimiters::gc`] takes only
    /// this map.
    fn admit(&self, now: Instant, ip: IpAddr, cost: f64, then: impl FnOnce() -> bool) -> bool {
        // Every address in one allocation collapses onto this key, so a rotating
        // client keeps spending the same budget.
        let key = self.prefix.key(ip);
        let mut buckets = lock(&self.buckets);
        match buckets.get_mut(&key) {
            Some(bucket) => bucket.try_admit_then(
                now,
                self.limit.tokens_per_sec(),
                self.limit.capacity(),
                cost,
                then,
            ),
            None => self.admit_new_ip(&mut buckets, now, key, cost, then),
        }
    }

    /// Admit a first-seen `ip` (already masked to its network prefix by the
    /// caller), tracking it unless the table is at capacity.
    ///
    /// The fresh bucket is inserted only when `then` admits the request too: a
    /// refused first request leaves no entry, exactly as if its refunded (full)
    /// bucket had been swept, so a flood the global limit refuses cannot fill the
    /// table.
    ///
    /// A new IP with the table already full is not tracked and spends from the
    /// shared overflow bucket instead, rather than evicting a live bucket or
    /// going unmetered: every client the table cannot track shares that one
    /// bucket, so together they get one client's budget however many prefixes
    /// they spread over. A `/48` holds 65,536 `/64`s, so one still fits with a
    /// bucket per `/64` and the overflow meters only the prefixes beyond the cap.
    /// Bounded memory is still chosen over perfect per-IP fairness: an honest
    /// client first seen while the table is full shares that budget until the
    /// GC sweep frees a slot.
    fn admit_new_ip(
        &self,
        buckets: &mut HashMap<IpAddr, Bucket>,
        now: Instant,
        ip: IpAddr,
        cost: f64,
        then: impl FnOnce() -> bool,
    ) -> bool {
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        if buckets.len() < self.max_entries {
            let mut bucket = Bucket::full(now, capacity);
            let admitted = bucket.try_admit_then(now, rate, capacity, cost, then);
            if admitted {
                buckets.insert(ip, bucket);
            }
            admitted
        } else {
            // Table full: share the overflow bucket (bounded memory over per-IP
            // fairness).
            lock(&self.overflow).try_admit_then(now, rate, capacity, cost, then)
        }
    }
}

/// The gateway's rate limiters. Any limiter may be disabled (`None`).
pub(crate) struct RateLimiters<C: Clock = SystemClock> {
    clock: C,
    global: Option<GlobalLimiter>,
    per_ip: Option<PerIpLimiter>,
    /// The submission budget (`--rate-limit-submissions`), charged by the
    /// proxy once a request is classified; see [`SubmissionBudget`].
    submissions: Option<GlobalLimiter>,
}

impl RateLimiters<SystemClock> {
    /// Build the limiters from resolved settings using the system clock, or
    /// `None` when every limiter is disabled (so no layer need be installed).
    pub(crate) fn new(
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        submissions: Option<RateLimit>,
        max_per_ip_entries: usize,
        prefix: PrefixPolicy,
    ) -> Option<Arc<Self>> {
        Self::with_clock(SystemClock, per_ip, global, submissions, max_per_ip_entries, prefix)
            .map(Arc::new)
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
        submissions: Option<RateLimit>,
        max_per_ip_entries: usize,
        prefix: PrefixPolicy,
    ) -> Option<Self> {
        if per_ip.is_none() && global.is_none() && submissions.is_none() {
            return None;
        }
        let now = clock.now();
        let bucket = |limit: RateLimit| GlobalLimiter {
            limit,
            bucket: Mutex::new(Bucket::full(now, limit.capacity())),
        };
        let global = global.map(bucket);
        let submissions = submissions.map(bucket);
        let per_ip = per_ip.map(|limit| PerIpLimiter {
            limit,
            max_entries: max_per_ip_entries.max(1),
            buckets: Mutex::new(HashMap::new()),
            overflow: Mutex::new(Bucket::full(now, limit.capacity())),
            prefix,
        });
        Some(Self { clock, global, per_ip, submissions })
    }

    /// The one admission rule every charge goes through: `cost` tokens from
    /// `client`'s per-IP bucket first (its own, or the shared overflow bucket
    /// when the table is full), then `then` only for a request that bucket
    /// admits; a refusal from `then` refunds the per-IP tokens, so a request is
    /// charged only when both admit it. A `None` client, or no per-IP limit,
    /// goes straight to `then`.
    ///
    /// [`Self::check`] and [`Self::charge_batch`] pass the global bucket as
    /// `then`; [`Self::check_edge`] passes one that records the global verdict
    /// for the proxy and admits.
    fn try_admit_then(
        &self,
        now: Instant,
        client: Option<IpAddr>,
        cost: f64,
        then: impl FnOnce() -> bool,
    ) -> bool {
        match self.per_ip.as_ref().zip(client) {
            Some((per_ip, ip)) => per_ip.admit(now, ip, cost, then),
            None => then(),
        }
    }

    /// Spend `cost` tokens from the global bucket; always admitted when there
    /// is none.
    fn admit_global(&self, now: Instant, cost: f64) -> bool {
        self.global.as_ref().is_none_or(|global| global.admit(now, cost))
    }

    /// Admit or reject a request from `client` (its [`ClientAddr`]). A `None`
    /// client skips the per-IP bucket; the global bucket still applies.
    ///
    /// The per-IP bucket is evaluated first and the global bucket only for a
    /// request it admits, so a request the per-IP limit refuses spends no global
    /// token: one source over its own limit cannot drain the shared budget every
    /// other client depends on. A request the global limit then refuses has its
    /// per-IP token refunded, so a global refusal spends no per-IP token either.
    /// Each bucket is charged only for requests both admit (see
    /// [`Self::try_admit_then`]).
    pub(crate) fn check(&self, client: Option<IpAddr>) -> Result<(), GatewayError> {
        let now = self.clock.now();
        self.try_admit_then(now, client, 1.0, || self.admit_global(now, 1.0))
            .then_some(())
            .ok_or(GatewayError::RateLimited)
    }

    /// Charge `tokens` more to a request from `client` (its [`ClientAddr`])
    /// that the edge layer already admitted: the calls its batch carries
    /// beyond the first.
    ///
    /// The same rule as [`Self::check`], through [`Self::try_admit_then`]: the
    /// per-IP bucket is charged first and the global bucket only when the
    /// per-IP bucket admits the charge, so a client over its own limit never
    /// spends the shared budget; when the global bucket then refuses, the
    /// per-IP tokens are given back. Each bucket is charged all or nothing: one
    /// without `tokens` left keeps what it has. A `None` client skips the
    /// per-IP bucket. Charging nothing touches no bucket, so a single call
    /// costs only its edge token.
    ///
    /// The extra calls are charged to the global bucket whatever the request's
    /// class, also with a submission budget configured: the budget settles
    /// only the edge's global verdict (see [`SubmissionBudget`]), so every
    /// token past the edge follows one rule and a batch never needs more than
    /// the per-IP or global burst, the bound the batch-length cap is clamped
    /// to.
    pub(crate) fn charge_batch(
        &self,
        client: Option<IpAddr>,
        tokens: usize,
    ) -> Result<(), GatewayError> {
        if tokens == 0 {
            return Ok(());
        }
        let cost = f64::from(u32::try_from(tokens).unwrap_or(u32::MAX));
        let now = self.clock.now();
        self.try_admit_then(now, client, cost, || self.admit_global(now, cost))
            .then_some(())
            .ok_or(GatewayError::RateLimited)
    }

    /// Charge one submission-class request to the submission budget; always
    /// admitted when there is none.
    fn charge_submission(&self) -> Result<(), GatewayError> {
        let now = self.clock.now();
        self.submissions
            .as_ref()
            .is_none_or(|submissions| submissions.admit(now, 1.0))
            .then_some(())
            .ok_or(GatewayError::RateLimited)
    }

    /// The edge verdict for a request from `client` (its [`ClientAddr`]),
    /// before its body is read.
    ///
    /// Without a submission budget this is [`Self::check`] and nothing reaches
    /// the proxy. With one, the per-IP bucket runs first and an over-limit
    /// client is refused here, without spending a global token; a request the
    /// global bucket refuses is let through, and the returned
    /// [`SubmissionBudget`] tells the proxy so. Both go through
    /// [`Self::try_admit_then`]; here the global verdict is recorded rather
    /// than enforced, so it never refunds the per-IP token.
    fn check_edge(
        self: &Arc<Self>,
        client: Option<IpAddr>,
    ) -> Result<Option<SubmissionBudget<C>>, GatewayError> {
        if self.submissions.is_none() {
            return self.check(client).map(|()| None);
        }
        let now = self.clock.now();
        let mut global_exhausted = false;
        let admitted = self.try_admit_then(now, client, 1.0, || {
            global_exhausted = !self.admit_global(now, 1.0);
            true
        });
        if !admitted {
            return Err(GatewayError::RateLimited);
        }
        Ok(Some(SubmissionBudget { limiters: Arc::clone(self), global_exhausted }))
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

/// The global bucket's verdict on one request, handed by the edge layer to the
/// proxy when a submission budget is configured.
///
/// The edge cannot tell a submission from a read (the body is not read yet), so
/// it does not refuse a request the global bucket turns down; the proxy settles
/// the verdict with [`Self::charge`] once it has classified the request. This
/// keeps a read flood that drains the global bucket from refusing submissions.
#[derive(Clone)]
pub(crate) struct SubmissionBudget<C: Clock = SystemClock> {
    /// The limiters holding the submission bucket.
    limiters: Arc<RateLimiters<C>>,
    /// Whether the global bucket refused this request at the edge.
    global_exhausted: bool,
}

impl<C: Clock> SubmissionBudget<C> {
    /// Settle the request's rate budget by class: a submission is charged to
    /// the submission bucket whatever the global bucket said, and any other
    /// request is refused when the global bucket refused it.
    pub(crate) fn charge(&self, submission: bool) -> Result<(), GatewayError> {
        if submission {
            self.limiters.charge_submission()
        } else if self.global_exhausted {
            Err(GatewayError::RateLimited)
        } else {
            Ok(())
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

/// Axum middleware: rate-limit by client IP ([`ClientAddr`]) and globally,
/// rejecting an over-limit request with the gateway's JSON-RPC `429` envelope
/// before its body is read. With a submission budget configured, the global
/// verdict travels to the proxy as a [`SubmissionBudget`] extension instead
/// (see [`RateLimiters::check_edge`]).
///
/// An admitted request carries the limiters on to the proxy handler as a
/// request extension, so the handler can charge a batch for its calls beyond
/// the first once it has counted them (see [`RateLimiters::charge_batch`]).
pub(crate) async fn rate_limit(
    State(limiters): State<Arc<RateLimiters>>,
    mut request: Request,
    next: Next,
) -> Response {
    // Orchestration probes are never rate-limited: a liveness/readiness check
    // failing under load would take the gateway down (the orchestrator kills or
    // depools the pod) exactly when it is meant to be absorbing a flood.
    let path = request.uri().path();
    let exempt = path == HEALTH_PATH || path == READY_PATH;
    let client = request.extensions().get::<ClientAddr>().map(|client| client.0);
    let verdict = if exempt { Ok(None) } else { limiters.check_edge(client) };
    // The final dispatch stays a `match`: one arm awaits `next`, which a
    // combinator closure cannot do.
    match verdict {
        Err(err) => error_response(&err, b""),
        Ok(budget) => {
            if let Some(budget) = budget {
                request.extensions_mut().insert(budget);
            }
            request.extensions_mut().insert(limiters);
            next.run(request).await
        }
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

        /// The tokens left in the bucket for `ip`'s prefix, if it is tracked.
        fn per_ip_tokens(&self, ip: IpAddr) -> Option<f64> {
            let per_ip = self.per_ip.as_ref()?;
            lock(&per_ip.buckets).get(&per_ip.prefix.key(ip)).map(|bucket| bucket.tokens)
        }

        /// The tokens left in the global bucket, if it is enabled.
        fn global_tokens(&self) -> Option<f64> {
            self.global.as_ref().map(|global| lock(&global.bucket).tokens)
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
        RateLimiters::with_clock(clock, per_ip, global, None, max_entries, prefix)
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
    fn per_ip_rejection_does_not_spend_global_token() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), Some(limit(1, 3)), 16);
        // A spends its one per-IP token; its next two requests are refused by its
        // own bucket and must leave the shared global burst of 3 untouched.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(matches!(limiters.check(Some(ip(1))), Err(GatewayError::RateLimited)));
        assert!(matches!(limiters.check(Some(ip(1))), Err(GatewayError::RateLimited)));
        // Only A's admitted request took a global token, so two remain for others.
        assert!(
            limiters.check(Some(ip(2))).is_ok(),
            "A's refused requests drained the global bucket"
        );
        assert!(limiters.check(Some(ip(3))).is_ok());
        assert!(matches!(limiters.check(Some(ip(4))), Err(GatewayError::RateLimited)));
    }

    #[test]
    fn global_rejection_does_not_spend_per_ip_token() {
        let clock = ManualClock::new();
        // The global bucket refills ten times faster than the per-IP one, so the
        // 100 ms advance below restores a global token but only 0.1 of a per-IP
        // token: A's last request can only pass on a refunded per-IP token.
        let limiters = limiters(clock.clone(), Some(limit(1, 2)), Some(limit(10, 1)), 16);
        assert!(limiters.check(Some(ip(1))).is_ok());
        // A's per-IP bucket admits, the empty global bucket refuses.
        assert!(matches!(limiters.check(Some(ip(1))), Err(GatewayError::RateLimited)));
        clock.advance(Duration::from_millis(100));
        assert!(limiters.check(Some(ip(1))).is_ok(), "the global refusal spent A's per-IP token");
    }

    #[test]
    fn global_rejection_leaves_no_entry_for_a_new_client() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 1)), Some(limit(1, 1)), 16);
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
        // B's first request passes its fresh per-IP bucket but the global bucket is
        // empty; the refused request must not leave a table entry behind.
        assert!(matches!(limiters.check(Some(ip(2))), Err(GatewayError::RateLimited)));
        assert_eq!(limiters.per_ip_len(), 1, "a globally refused new client must not be tracked");
    }

    #[test]
    fn refund_never_exceeds_capacity() {
        let now = ManualClock::new().now();
        let (rate, capacity) = (1.0, 2.0);
        let mut bucket = Bucket::full(now, capacity);
        bucket.refund(1.0, capacity);
        assert!(bucket.tokens <= capacity, "a refund lifted a full bucket past its capacity");
        // Still exactly `capacity` tokens: two admissions, then a refusal (the
        // clock does not move, so nothing refills).
        assert!(bucket.try_admit(now, rate, capacity, 1.0));
        assert!(bucket.try_admit(now, rate, capacity, 1.0));
        assert!(!bucket.try_admit(now, rate, capacity, 1.0));
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
        // The table is full (cap 1): a new IP is admitted untracked, spending from
        // the shared overflow bucket instead of getting an entry of its own.
        assert!(limiters.check(Some(ip(2))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
    }

    /// A single call is charged only the edge token `check` took: charging it
    /// nothing spends nothing and creates no bucket.
    #[test]
    fn single_request_charges_no_extra_token() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 2)), Some(limit(1, 2)), 16);
        assert!(limiters.charge_batch(Some(ip(1)), 0).is_ok());
        assert_eq!(limiters.per_ip_len(), 0, "charging nothing creates no bucket");
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.charge_batch(Some(ip(1)), 0).is_ok());
        assert!(limiters.check(Some(ip(1))).is_ok(), "a zero charge spends no token");
        assert!(limiters.check(Some(ip(1))).is_err());
    }

    #[test]
    fn charge_spends_all_or_nothing() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 4)), None, 16);
        assert!(limiters.check(Some(ip(1))).is_ok()); // 3 left
                                                      // a charge the bucket cannot cover in full spends nothing
        assert!(matches!(limiters.charge_batch(Some(ip(1)), 4), Err(GatewayError::RateLimited)));
        assert!(limiters.charge_batch(Some(ip(1)), 3).is_ok()); // 0 left
        assert!(limiters.check(Some(ip(1))).is_err());
        // a charge without a peer skips the per-IP bucket
        assert!(limiters.charge_batch(None, 100).is_ok());
    }

    #[test]
    fn charge_runs_per_ip_first_and_refunds_on_a_global_refusal() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 4)), Some(limit(1, 3)), 16);
        assert!(limiters.check(Some(ip(1))).is_ok()); // per-ip 3, global 2
                                                      // over its own limit: refused before the global bucket is touched
        assert!(limiters.charge_batch(Some(ip(1)), 4).is_err());
        assert_eq!(
            (limiters.per_ip_tokens(ip(1)), limiters.global_tokens()),
            (Some(3.0), Some(2.0))
        );
        // refused by the global bucket: the per-IP tokens come back
        assert!(limiters.charge_batch(Some(ip(1)), 3).is_err());
        assert_eq!(
            (limiters.per_ip_tokens(ip(1)), limiters.global_tokens()),
            (Some(3.0), Some(2.0))
        );
        assert!(limiters.charge_batch(Some(ip(1)), 2).is_ok());
        assert_eq!(
            (limiters.per_ip_tokens(ip(1)), limiters.global_tokens()),
            (Some(1.0), Some(0.0))
        );
    }

    /// Limiters with a submission budget, shared the way the edge layer holds
    /// them.
    fn limiters_with_submissions<C: Clock>(
        clock: C,
        per_ip: Option<RateLimit>,
        global: Option<RateLimit>,
        submissions: RateLimit,
    ) -> Arc<RateLimiters<C>> {
        let limiters = RateLimiters::with_clock(
            clock,
            per_ip,
            global,
            Some(submissions),
            16,
            PrefixPolicy::default(),
        );
        Arc::new(limiters.expect("the submission budget is enabled"))
    }

    #[test]
    fn submission_budget_admits_when_global_is_exhausted() {
        let clock = ManualClock::new();
        let limiters =
            limiters_with_submissions(clock.clone(), None, Some(limit(1, 1)), limit(1, 1));

        // a read spends the global bucket's only token
        let read = limiters.check_edge(None).expect("edge").expect("a budget verdict");
        assert!(read.charge(false).is_ok());

        // the edge lets the next request through with the global bucket empty
        let submission = limiters.check_edge(None).expect("edge").expect("a budget verdict");
        assert!(submission.global_exhausted);
        // a submission is admitted from its own bucket
        assert!(submission.charge(true).is_ok());

        // a second read is refused, and so is a submission once its own bucket
        // is spent
        let second_read = limiters.check_edge(None).expect("edge").expect("a budget verdict");
        assert!(matches!(second_read.charge(false), Err(GatewayError::RateLimited)));
        assert!(matches!(second_read.charge(true), Err(GatewayError::RateLimited)));

        // both buckets refill over time
        clock.advance(Duration::from_secs(1));
        let refilled = limiters.check_edge(None).expect("edge").expect("a budget verdict");
        assert!(!refilled.global_exhausted);
        assert!(refilled.charge(true).is_ok());
    }

    #[test]
    fn with_a_submission_budget_the_per_ip_check_runs_first_and_still_refuses() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 1)),
            Some(limit(1, 2)),
            limit(1, 1),
        );
        assert!(limiters.check_edge(Some(ip(1))).is_ok());
        // the client's own bucket is empty: refused at the edge, without
        // spending the global token another client can still use
        assert!(matches!(limiters.check_edge(Some(ip(1))), Err(GatewayError::RateLimited)));
        let other = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget verdict");
        assert!(!other.global_exhausted);
    }

    #[test]
    fn without_a_submission_budget_the_edge_verdict_is_check() {
        let limiters = Arc::new(limiters(ManualClock::new(), None, Some(limit(1, 1)), 16));
        assert!(matches!(limiters.check_edge(None), Ok(None)));
        assert!(matches!(limiters.check_edge(None), Err(GatewayError::RateLimited)));
    }

    #[test]
    fn full_table_shares_one_bucket_for_untracked_clients() {
        let limiters = limiters(ManualClock::new(), Some(limit(1, 2)), None, 2);
        // Two tracked clients fill the table.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_ok());
        assert_eq!(limiters.per_ip_len(), 2);
        // A third and a fourth client cannot get entries, so they share one
        // overflow bucket: one client's burst of 2 between them, then 429.
        assert!(limiters.check(Some(ip(3))).is_ok());
        assert!(limiters.check(Some(ip(4))).is_ok());
        assert!(matches!(limiters.check(Some(ip(3))), Err(GatewayError::RateLimited)));
        assert!(matches!(limiters.check(Some(ip(4))), Err(GatewayError::RateLimited)));
        assert_eq!(limiters.per_ip_len(), 2, "untracked clients must not add entries");
        // A tracked client keeps its own bucket, which still holds a token.
        assert!(limiters.check(Some(ip(1))).is_ok());
    }

    #[test]
    fn gc_frees_entries_so_new_clients_get_their_own_bucket() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), Some(limit(1, 1)), None, 1);
        // ip(1) fills the one-entry table; ip(2) spends the overflow token, so
        // ip(3) is refused.
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_ok());
        assert!(matches!(limiters.check(Some(ip(3))), Err(GatewayError::RateLimited)));
        // ip(1)'s bucket refills and goes idle, and the sweep reclaims it.
        clock.advance(Duration::from_secs(1));
        limiters.gc();
        assert_eq!(limiters.per_ip_len(), 0);
        // The freed slot tracks the next new client.
        assert!(limiters.check(Some(ip(3))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1, "a freed slot must track the new client");
        // ip(3) spent its own bucket, not the overflow one, which still has the
        // token it refilled for the next untracked client.
        assert!(limiters.check(Some(ip(4))).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
    }

    #[test]
    fn global_rejection_refunds_the_overflow_bucket() {
        let clock = ManualClock::new();
        // As in `global_rejection_does_not_spend_per_ip_token`, the global bucket
        // refills ten times faster than the per-client one; the one-entry table
        // sends every client after ip(1) to the overflow bucket.
        let limiters = limiters(clock.clone(), Some(limit(1, 1)), Some(limit(10, 1)), 1);
        assert!(limiters.check(Some(ip(1))).is_ok());
        // The overflow bucket admits ip(2), the empty global bucket refuses it.
        assert!(matches!(limiters.check(Some(ip(2))), Err(GatewayError::RateLimited)));
        clock.advance(Duration::from_millis(100));
        // A global token is back but the overflow bucket regained only 0.1, so
        // ip(3) passes only on the token refunded after ip(2)'s refusal.
        assert!(limiters.check(Some(ip(3))).is_ok(), "the global refusal spent the overflow token");
        assert_eq!(limiters.per_ip_len(), 1);
    }

    /// A batch's extra calls go through the same admission helper as `check`,
    /// so a global refusal of the batch refunds the per-IP tokens to the bucket
    /// that was charged, the shared overflow bucket included.
    #[test]
    fn batch_refund_reaches_the_overflow_bucket() {
        let clock = ManualClock::new();
        // per-IP burst 4 at 1/s, global burst 3 at 10/s, and a one-entry table,
        // so ip(2) spends from the overflow bucket
        let limiters = limiters(clock.clone(), Some(limit(1, 4)), Some(limit(10, 3)), 1);
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_ok());
        // the overflow bucket has the 3 tokens, the global bucket only 1
        assert!(matches!(limiters.charge_batch(Some(ip(2)), 3), Err(GatewayError::RateLimited)));
        assert_eq!(limiters.per_ip_len(), 1);
        // 200 ms refill the global bucket to 3 but give the overflow bucket only
        // 0.2, so the same batch passes only on the refunded overflow tokens
        clock.advance(Duration::from_millis(200));
        assert!(
            limiters.charge_batch(Some(ip(2)), 3).is_ok(),
            "the global refusal spent the overflow tokens"
        );
    }

    #[test]
    fn both_disabled_yields_no_limiters() {
        assert!(RateLimiters::with_clock(
            SystemClock,
            None,
            None,
            None,
            16,
            PrefixPolicy::default()
        )
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
    fn v6_prefix_keys_the_cap() {
        // The per-client connection cap counts by this key, so every address a
        // client can pick inside its own /64 must land on one key, while another
        // /64 is another client.
        let policy = PrefixPolicy::default();
        assert_eq!(policy.key(ip6(1, 1)), policy.key(ip6(1, 2)), "one /64 is one client");
        assert_eq!(policy.key(ip6(1, 1)), ip6(1, 0), "the key is the masked /64");
        assert_ne!(policy.key(ip6(1, 1)), policy.key(ip6(2, 1)), "another /64 is another client");
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
