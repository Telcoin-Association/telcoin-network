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
//! rule for the edge check and the batch charge alike. The exceptions are the
//! submission budget's charges, made after the body is read (see below): a
//! refusal by the class charge, and a global refusal of a request answered
//! before it was classified, keep the edge's per-IP token spent.
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
//! reads and submissions from spending each other's rate. With it set, the
//! edge runs only the per-IP check, which still refuses an over-limit client
//! before its body is read, and hands the proxy a [`SubmissionBudget`] naming
//! the per-IP bucket it charged. Once the request is classified, the proxy
//! charges its first call to the global bucket for a query or to the
//! submission bucket for a submission. A refusal there spends no other shared
//! bucket, so a refused submission spends no global token and a refused query
//! no submission token, but it keeps the edge's per-IP token spent: the body
//! has been read and screened by then, and that token pays for the work, so a
//! client whose requests keep being refused there is refused at the edge,
//! before its body is read, once it has spent its per-IP burst. A batch whose
//! calls beyond the first the later per-call charge refuses gets its class
//! token back; its per-IP token stays spent. Without the flag there is no
//! class charge to refund: the edge check is [`RateLimiters::check`], whose
//! global token was spent before the body was read and stays spent, with its
//! per-IP token, when the batch charge refuses, so the same refused batch
//! costs a global token without the flag and none with it.
//!
//! With the flag, a first-seen client's per-IP bucket is inserted at its
//! request's class charge, whatever the class bucket's verdict, or, for a
//! request answered before it is classified (a `405`, an unreadable or
//! over-length body, a denied method, a loop), when its [`SubmissionBudget`]
//! is dropped, whatever the global bucket's verdict. So a table entry costs the
//! per-IP token that paid for reading the request, and a flood the shared
//! buckets refuse can add entries, up to `--rate-limit-per-ip-max-entries` and
//! subject to the idle eviction, each of which then holds its client to the
//! per-IP limit. With the global limit off (`--rate-limit-global 0`), the
//! global charges here and in [`RateLimiters::check`] always admit, so a
//! first-seen client's entry then costs only its per-IP token in either mode.

use std::{
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    num::{NonZeroU32, NonZeroUsize},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{Request, State},
    http::Method,
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

/// How often the background sweep reclaims idle per-IP buckets: every second,
/// so a client whose bucket has refilled frees its slot within a second. A
/// sweep walks the whole table under the map lock (about 1 ms over the default
/// 100,000 entries, linear in the `--rate-limit-per-ip-max-entries` cap).
pub(crate) const GC_INTERVAL: Duration = Duration::from_secs(1);

/// Least time between two sweeps of a full per-IP table made inline for a
/// first-seen client (see [`PerIpLimiter::make_room`]). A sweep walks the whole
/// table under the map lock (about 1 ms over the default 100,000 entries,
/// linear in the cap), so this bounds its cost under a stream of new clients.
const INLINE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Default ceiling on tracked per-IP buckets (`--rate-limit-per-ip-max-entries`).
/// Bounds the per-IP map so a wide spread of source IPs cannot grow it without
/// limit; ~10 MB at the default cap, growing linearly with the flag, and idle
/// buckets are reclaimed between sweeps by [`RateLimiters::gc`].
pub(crate) const DEFAULT_MAX_PER_IP_ENTRIES: NonZeroUsize =
    NonZeroUsize::new(100_000).expect("the default per-IP table size is not zero");

/// Largest `--rate-limit-per-ip-max-entries` startup accepts without a
/// warning, ten times the default: memory (about 100 bytes an entry) and each
/// idle-bucket sweep, which walks the whole table under the map lock every
/// per-IP admission takes (about 1 ms per 100,000 entries), grow linearly with
/// the cap.
pub(crate) const MAX_QUIET_PER_IP_ENTRIES: usize = 1_000_000;

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
/// second up to `capacity`; each admitted request spends one token;
/// [`Bucket::spend`] can take it below zero.
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
        // never move the refill clock backwards: a stale `now` must not credit
        // an interval twice
        self.last_refill = self.last_refill.max(now);
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }

    /// Refill for the elapsed time, then spend `cost` tokens whether or not
    /// that many are left: a spend already admitted elsewhere, recorded as
    /// debt down to `-capacity`, so the bucket refills past it before it
    /// admits again. [`Self::replenished`], [`Self::is_idle`] and
    /// [`Self::refund`] handle a negative balance.
    fn spend(&mut self, now: Instant, rate: f64, capacity: f64, cost: f64) {
        self.tokens = (self.replenished(now, rate, capacity) - cost).max(-capacity);
        self.last_refill = self.last_refill.max(now);
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
    /// Refill to the time `clock` reads, then spend `cost` tokens if that many
    /// are available. The clock is read under the lock, so admissions refill
    /// in lock order and a stale reading never reaches the bucket.
    fn admit(&self, clock: &impl Clock, cost: f64) -> bool {
        let mut bucket = lock(&self.bucket);
        let now = clock.now();
        bucket.try_admit(now, self.limit.tokens_per_sec(), self.limit.capacity(), cost)
    }

    /// Give back `cost` tokens spent on a request a later check refused, never
    /// past capacity.
    fn refund(&self, cost: f64) {
        lock(&self.bucket).refund(cost, self.limit.capacity());
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
    pub(crate) fn v4_bits(&self) -> u8 {
        self.v4.bits()
    }

    /// The IPv6 prefix width, in bits.
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
/// by [`RateLimiters::gc`], and from a full table by a first-seen client (see
/// [`PerIpLimiter::make_room`]). The map is keyed on the *masked* client
/// address (see [`PrefixPolicy`]), so address rotation inside one allocation
/// shares a bucket.
#[derive(Debug)]
struct PerIpLimiter {
    /// The rate and burst every per-client bucket is built with.
    limit: RateLimit,
    /// Ceiling on tracked buckets; a first-seen client that meets a full table
    /// with no idle entry to free spends from `overflow` instead of getting a
    /// bucket of its own.
    max_entries: usize,
    /// When a first-seen client last swept the full table of its idle entries
    /// (see [`Self::make_room`]). Taken only while the map lock is held.
    last_inline_sweep: Mutex<Instant>,
    /// Live buckets, keyed on the prefix-masked client address.
    buckets: Mutex<HashMap<IpAddr, Bucket>>,
    /// One bucket, built with `limit`, shared by every client the full table
    /// cannot track, so untracked clients together get one client's budget.
    overflow: Mutex<Bucket>,
    /// How a client address is masked down to its bucket key.
    prefix: PrefixPolicy,
}

/// The per-IP bucket an admitted request was charged to. The edge check under
/// a submission budget hands it to the proxy, whose class charge inserts a
/// first-seen client's bucket ([`PerIpReceipt::Fresh`]) whatever its verdict
/// (see [`RateLimiters::charge_class`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PerIpReceipt {
    /// No per-IP bucket was charged: the per-IP limit is off, or the request
    /// has no client address.
    NotCharged,
    /// The client's own bucket.
    Own,
    /// A first-seen prefix admitted on a fresh bucket that is not in the table
    /// yet: the edge check under a submission budget defers the insertion to
    /// the request's class charge, or, for a request answered unclassified, to
    /// its settlement when its budget is dropped, and either inserts it
    /// whatever the verdict (see [`PerIpLimiter::insert_fresh`]). `at` is when
    /// the edge charged it, so the bucket rebuilt at insertion refills from
    /// then.
    Fresh { key: IpAddr, at: Instant },
    /// The overflow bucket every client the full table cannot track shares.
    Overflow,
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
    /// order is always this map, then the inline-sweep clock
    /// (`last_inline_sweep`), then the overflow bucket, then the global bucket
    /// (the only lock a `then` takes); [`RateLimiters::gc`] takes only this
    /// map.
    ///
    /// Returns the bucket that was charged when the request is admitted (see
    /// [`PerIpReceipt`]).
    ///
    /// With `defer`, a first-seen prefix the table has room for is charged on
    /// a fresh bucket that is not inserted: the receipt is
    /// [`PerIpReceipt::Fresh`], and the caller inserts the bucket with
    /// [`Self::insert_fresh`] at the request's class charge, or at the
    /// settlement of a request answered unclassified, whatever the verdict.
    ///
    /// The clock is read under the map lock, so admissions refill in lock
    /// order; the overflow bucket is touched only with the map lock held, so
    /// the same reading serves it.
    fn admit(
        &self,
        clock: &impl Clock,
        ip: IpAddr,
        cost: f64,
        defer: bool,
        then: impl FnOnce() -> bool,
    ) -> Option<PerIpReceipt> {
        // Every address in one allocation collapses onto this key, so a rotating
        // client keeps spending the same budget.
        let key = self.prefix.key(ip);
        let mut buckets = lock(&self.buckets);
        let now = clock.now();
        match buckets.get_mut(&key) {
            Some(bucket) => bucket
                .try_admit_then(now, self.limit.tokens_per_sec(), self.limit.capacity(), cost, then)
                .then_some(PerIpReceipt::Own),
            None => self.admit_new_ip(&mut buckets, now, key, cost, defer, then),
        }
    }

    /// Admit a first-seen `ip` (already masked to its network prefix by the
    /// caller), tracking it unless the table is at capacity.
    ///
    /// The fresh bucket is inserted only when `then` admits the request too: a
    /// refused first request leaves no entry, exactly as if its refunded (full)
    /// bucket had been swept, so a flood the global limit refuses cannot fill the
    /// table when a global limit is set (with it off, every first request the
    /// per-IP bucket admits is inserted, up to the cap). With `defer` (the edge
    /// check under a submission budget, whose `then` admits everything) it is
    /// not inserted here at all: the receipt is [`PerIpReceipt::Fresh`], and
    /// the bucket is inserted at the request's class charge, or, for a request
    /// answered unclassified, at its settlement, whatever the verdict (see
    /// [`RateLimiters::charge_class`]); there a refused request can add an
    /// entry, paid for by its per-IP token.
    ///
    /// A new IP that meets a full table first sweeps the table's idle entries
    /// (see [`Self::make_room`]), at most once per [`INLINE_SWEEP_INTERVAL`],
    /// and gets a bucket of its own if that frees a slot. An idle entry carries
    /// no state a fresh bucket would not; a busy one is never evicted. An
    /// entry goes idle once it has refilled, within `burst / rate` seconds of
    /// its last request, so holding the table full against new clients costs
    /// at least one request per tracked prefix per sweep.
    ///
    /// When the sweep frees nothing, or ran less than an interval ago, the new
    /// IP is not tracked and spends from the shared overflow bucket instead,
    /// rather than evicting a busy bucket or going unmetered: every client the
    /// table cannot track shares that one bucket, so together they get one
    /// client's budget however many prefixes they spread over. A `/48` holds
    /// 65,536 `/64`s, so one still fits with a bucket per `/64` and the overflow
    /// meters only the prefixes beyond the cap. Bounded memory is still chosen
    /// over perfect per-IP fairness: an honest client first seen while every
    /// tracked entry is busy shares that budget until a sweep frees a slot.
    /// Every charge metered there, admitted or not, counts in
    /// `tn_worker_gateway_rate_limit_overflow_total`.
    fn admit_new_ip(
        &self,
        buckets: &mut HashMap<IpAddr, Bucket>,
        now: Instant,
        ip: IpAddr,
        cost: f64,
        defer: bool,
        then: impl FnOnce() -> bool,
    ) -> Option<PerIpReceipt> {
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        if self.make_room(buckets, now) {
            let mut bucket = Bucket::full(now, capacity);
            if !bucket.try_admit_then(now, rate, capacity, cost, then) {
                return None;
            }
            if defer {
                return Some(PerIpReceipt::Fresh { key: ip, at: now });
            }
            buckets.insert(ip, bucket);
            Some(PerIpReceipt::Own)
        } else {
            // Table full of busy entries: share the overflow bucket (bounded
            // memory over per-IP fairness).
            let admitted = lock(&self.overflow).try_admit_then(now, rate, capacity, cost, then);
            crate::telemetry::record_rate_limit_overflow();
            admitted.then_some(PerIpReceipt::Overflow)
        }
    }

    /// Track the first-seen prefix `key` that the edge admitted at `at` on a
    /// fresh bucket charged `cost` tokens (a [`PerIpReceipt::Fresh`]), at its
    /// request's class charge or at the settlement of a request answered
    /// unclassified, whatever the verdict.
    ///
    /// If the prefix has a bucket by now, a concurrent first request from it
    /// was inserted first, and this request's `cost` is spent from that bucket
    /// instead, even past its tokens: the spend is recorded as debt, down to
    /// minus one burst ([`Bucket::spend`]), so concurrent first requests cost
    /// the prefix its sustained per-IP rate. Its first burst can still reach
    /// the number of requests it has in flight before the first insertion,
    /// which `--max-connections-per-ip` bounds for a direct client; keep that
    /// at or below the per-IP burst (32 and 200 at the defaults). A client
    /// named by a trusted `X-Forwarded-For` front is not under that cap.
    ///
    /// If the table is full by now and [`Self::make_room`] frees no slot,
    /// nothing is inserted and the prefix stays untracked until its next
    /// request.
    fn insert_fresh(&self, clock: &impl Clock, key: IpAddr, at: Instant, cost: f64) {
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        let mut buckets = lock(&self.buckets);
        let now = clock.now();
        if let Some(bucket) = buckets.get_mut(&key) {
            // the request is already past the edge: record what it spent, as
            // debt if the bucket is short
            bucket.spend(now, rate, capacity, cost);
        } else if self.make_room(&mut buckets, now) {
            // the bucket the edge charged, refilling from when it was charged
            buckets.insert(key, Bucket { tokens: capacity - cost, last_refill: at });
        }
    }

    /// Whether `buckets` (the locked map) has room for a first-seen client.
    /// A full table is first swept of its idle entries, the same sweep as
    /// [`RateLimiters::gc`], unless an inline sweep ran less than
    /// [`INLINE_SWEEP_INTERVAL`] ago; a busy entry is never evicted.
    fn make_room(&self, buckets: &mut HashMap<IpAddr, Bucket>, now: Instant) -> bool {
        if buckets.len() < self.max_entries {
            return true;
        }
        {
            let mut last_sweep = lock(&self.last_inline_sweep);
            if now.saturating_duration_since(*last_sweep) < INLINE_SWEEP_INTERVAL {
                return false;
            }
            *last_sweep = now;
        }
        let rate = self.limit.tokens_per_sec();
        let capacity = self.limit.capacity();
        buckets.retain(|_, bucket| !bucket.is_idle(now, rate, capacity));
        buckets.len() < self.max_entries
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
            last_inline_sweep: Mutex::new(now),
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
    /// An admitted request gets back the [`PerIpReceipt`] naming the per-IP
    /// bucket that was charged. [`Self::check`] and [`Self::charge_batch`]
    /// pass the global bucket as `then` and need no receipt. With a submission
    /// budget, [`Self::check_edge`] charges the per-IP bucket alone, with a
    /// first-seen client's insertion deferred (see [`PerIpLimiter::admit`]),
    /// and the proxy keeps that receipt so its class charge can insert a
    /// first-seen client's bucket, whatever its verdict (see
    /// [`Self::charge_class`]).
    fn try_admit_then(
        &self,
        client: Option<IpAddr>,
        cost: f64,
        then: impl FnOnce() -> bool,
    ) -> Option<PerIpReceipt> {
        match self.per_ip.as_ref().zip(client) {
            Some((per_ip, ip)) => per_ip.admit(&self.clock, ip, cost, false, then),
            None => then().then_some(PerIpReceipt::NotCharged),
        }
    }

    /// Insert the per-IP bucket a [`PerIpReceipt::Fresh`] names, at its
    /// request's class charge or at the settlement of a request answered
    /// unclassified, whatever the verdict; every other receipt names a bucket
    /// that is tracked already, or none (see [`PerIpLimiter::insert_fresh`]).
    fn track_fresh(&self, receipt: PerIpReceipt) {
        if let (Some(per_ip), PerIpReceipt::Fresh { key, at }) = (&self.per_ip, receipt) {
            per_ip.insert_fresh(&self.clock, key, at, 1.0);
        }
    }

    /// Spend `cost` tokens from the global bucket; always admitted when there
    /// is none.
    fn admit_global(&self, cost: f64) -> bool {
        self.global.as_ref().is_none_or(|global| global.admit(&self.clock, cost))
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
        self.try_admit_then(client, 1.0, || self.admit_global(1.0))
            .map(drop)
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
    /// only the first call (see [`SubmissionBudget`]), so every token past the
    /// first follows one rule and a batch never needs more than the per-IP or
    /// global burst, the bound the batch-length cap is clamped to.
    pub(crate) fn charge_batch(
        &self,
        client: Option<IpAddr>,
        tokens: usize,
    ) -> Result<(), GatewayError> {
        if tokens == 0 {
            return Ok(());
        }
        let cost = f64::from(u32::try_from(tokens).unwrap_or(u32::MAX));
        self.try_admit_then(client, cost, || self.admit_global(cost))
            .map(drop)
            .ok_or(GatewayError::RateLimited)
    }

    /// Charge the first call of a classified request that the edge admitted on
    /// its per-IP check alone: a submission to the submission budget, any
    /// other request to the global bucket (either always admits when it is
    /// off). A refusal spends no other shared bucket, so neither is spent on a
    /// request the other refused, but the per-IP token `edge` names stays
    /// spent: the body was read and screened before this verdict, and that
    /// token pays for the work. A batch whose calls beyond the first
    /// [`Self::charge_batch`] then refuses gets its class token back through
    /// [`Self::refund_class`]; its per-IP token stays spent.
    ///
    /// A first-seen client's bucket ([`PerIpReceipt::Fresh`]) is inserted here
    /// whatever the verdict (see [`PerIpLimiter::insert_fresh`]), so a client
    /// whose requests keep being refused meets its per-IP limit at the edge.
    fn charge_class(&self, edge: PerIpReceipt, submission: bool) -> Result<(), GatewayError> {
        let admitted = if submission {
            self.submissions.as_ref().is_none_or(|submissions| submissions.admit(&self.clock, 1.0))
        } else {
            self.admit_global(1.0)
        };
        // the body was read and screened before this verdict, and the edge's
        // per-IP token pays for that work: it stays spent, and a first-seen
        // client is tracked whatever the class bucket says
        self.track_fresh(edge);
        if admitted {
            Ok(())
        } else {
            Err(GatewayError::RateLimited)
        }
    }

    /// Give back the class token [`Self::charge_class`] took for a request
    /// that a later charge refused: to the submission bucket for a submission,
    /// to the global bucket otherwise. Either is a no-op when that bucket is
    /// off. Only the class charge is refunded: without a submission budget
    /// there is none, and the global token [`Self::check`] took at the edge
    /// stays spent with the per-IP token, as the per-IP token does here.
    fn refund_class(&self, submission: bool) {
        let bucket = if submission { &self.submissions } else { &self.global };
        if let Some(bucket) = bucket {
            bucket.refund(1.0);
        }
    }

    /// The edge verdict for a request from `client` (its [`ClientAddr`]),
    /// before its body is read.
    ///
    /// Without a submission budget this is [`Self::check`] and nothing reaches
    /// the proxy. With one, only the per-IP bucket runs here, so an over-limit
    /// client is still refused before its body is read, and no shared bucket
    /// is touched: the edge cannot tell a query from a submission yet. The
    /// returned [`SubmissionBudget`] carries the per-IP receipt to the proxy,
    /// which charges the class bucket once it has classified the request.
    ///
    /// A first-seen client is charged on a fresh bucket that is not inserted
    /// yet ([`PerIpReceipt::Fresh`]): the class charge inserts it whatever its
    /// verdict, and so does a budget dropped uncharged, whatever the global
    /// bucket says of the request as a query (see [`SubmissionBudget`]), so
    /// the edge alone never adds a table entry.
    fn check_edge(
        self: &Arc<Self>,
        client: Option<IpAddr>,
    ) -> Result<Option<SubmissionBudget<C>>, GatewayError> {
        if self.submissions.is_none() {
            return self.check(client).map(|()| None);
        }
        let edge = match self.per_ip.as_ref().zip(client) {
            Some((per_ip, ip)) => per_ip.admit(&self.clock, ip, 1.0, true, || true),
            None => Some(PerIpReceipt::NotCharged),
        }
        .ok_or(GatewayError::RateLimited)?;
        let inner =
            BudgetInner { limiters: Arc::clone(self), edge, charged: AtomicBool::new(false) };
        Ok(Some(SubmissionBudget { inner: Arc::new(inner) }))
    }

    /// Settle a request the edge admitted under a submission budget but that
    /// was answered without a class charge (a `405`, an unreadable or
    /// over-length body, a denied method, a loop): charge its first call to
    /// the global bucket as a query (nothing when the global limit is off), as
    /// [`Self::check`] would have, and insert a first-seen client's bucket
    /// whatever that bucket's verdict, as [`Self::charge_class`] does. Unlike
    /// `check`, a global refusal keeps the per-IP token the edge spent: that
    /// token paid for reading the request, so a client whose requests are all
    /// answered unclassified meets its per-IP limit at the edge however the
    /// global bucket answers them.
    fn settle_unclassified(&self, edge: PerIpReceipt) {
        // the request was already answered, so the global verdict decides
        // nothing: the per-IP token the edge spent stays spent either way
        self.admit_global(1.0);
        self.track_fresh(edge);
    }

    /// Drop idle (fully-refilled) per-IP buckets to bound memory. Called
    /// periodically from a background task.
    pub(crate) fn gc(&self) {
        if let Some(per_ip) = &self.per_ip {
            let rate = per_ip.limit.tokens_per_sec();
            let capacity = per_ip.limit.capacity();
            let mut buckets = lock(&per_ip.buckets);
            // read under the lock, like every admission's
            let now = self.clock.now();
            buckets.retain(|_, bucket| !bucket.is_idle(now, rate, capacity));
        }
    }
}

/// A request the edge layer admitted on its per-IP check alone, handed to the
/// proxy when a submission budget is configured.
///
/// The edge cannot tell a submission from a read (the body is not read yet), so
/// it charges no shared bucket; the proxy charges the request's class with
/// [`Self::charge`] once it has classified it. This keeps a read flood that
/// drains the global bucket from refusing single submissions, and a submission
/// *request* over its own budget from draining the global bucket. The calls of
/// a submission batch beyond its first are still charged to the global bucket
/// (see [`RateLimiters::charge_batch`]), so the budget bounds submission
/// requests, not submitted transactions.
///
/// A request answered before it is classified (a `405`, an unreadable or
/// over-length body, a denied method, a loop) never reaches [`Self::charge`].
/// When the last clone of its budget is dropped, it is charged to the global
/// bucket as a query, and a first-seen client's per-IP bucket is inserted
/// whatever that bucket's verdict, as the class charge inserts it. Unlike
/// [`RateLimiters::check`], a global refusal here does not refund the edge's
/// per-IP token: the request has been answered already, and that token paid
/// for reading it, so a client whose requests are all answered unclassified
/// still meets its per-IP limit at the edge while the global bucket is empty.
/// With the global limit off the drop charge always admits. Dropping a budget
/// that was charged costs nothing more.
#[derive(Clone)]
pub(crate) struct SubmissionBudget<C: Clock = SystemClock> {
    /// Shared by every clone (the request extension and the handler's copy),
    /// so the request is settled once, when the last clone is dropped.
    inner: Arc<BudgetInner<C>>,
}

impl<C: Clock> SubmissionBudget<C> {
    /// Charge the request's first call by class: a submission to the
    /// submission bucket, any other request to the global bucket. A refusal
    /// keeps the edge's per-IP token spent, and either verdict tracks a
    /// first-seen client (see [`RateLimiters::charge_class`]).
    pub(crate) fn charge(&self, submission: bool) -> Result<(), GatewayError> {
        self.inner.charged.store(true, Ordering::Relaxed);
        self.inner.limiters.charge_class(self.inner.edge, submission)
    }

    /// Give back the class token [`Self::charge`] took, for a batch whose
    /// calls beyond the first [`RateLimiters::charge_batch`] then refused;
    /// `submission` is the class it was charged as. The per-IP token the edge
    /// spent stays spent.
    pub(crate) fn refund(&self, submission: bool) {
        self.inner.limiters.refund_class(submission);
    }
}

/// The state every clone of one [`SubmissionBudget`] shares.
struct BudgetInner<C: Clock> {
    /// The limiters holding the global and submission buckets.
    limiters: Arc<RateLimiters<C>>,
    /// The per-IP bucket the edge charged; a first-seen client's is inserted
    /// by the class charge, or by the drop charge, whatever the verdict.
    edge: PerIpReceipt,
    /// Whether [`SubmissionBudget::charge`] has run, whatever its verdict.
    charged: AtomicBool,
}

impl<C: Clock> Drop for BudgetInner<C> {
    fn drop(&mut self) {
        if !*self.charged.get_mut() {
            self.limiters.settle_unclassified(self.edge);
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
pub(crate) async fn run_gc<C: Clock>(
    limiters: Arc<RateLimiters<C>>,
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
/// before its body is read. With a submission budget configured, only the
/// per-IP check runs here and the proxy charges the request's class later,
/// through the [`SubmissionBudget`] extension (see
/// [`RateLimiters::check_edge`]).
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
    // depools the pod) exactly when it is meant to be absorbing a flood. Only a
    // `GET` or `HEAD` there is a probe; any other method on a probe path is
    // limited like any request before it gets its `405`.
    let exempt = is_exempt(request.method(), request.uri().path());
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

/// Whether a request skips the rate limiter: a `GET` or `HEAD` on a probe
/// path (`/health`, `/ready`). A `GET` or `HEAD` on `/ready/any` never
/// reaches this layer; its other methods are routed through it and limited
/// like any request.
fn is_exempt(method: &Method, path: &str) -> bool {
    (path == HEALTH_PATH || path == READY_PATH) && matches!(*method, Method::GET | Method::HEAD)
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

        /// The tokens left in the submission bucket, if it is enabled.
        fn submission_tokens(&self) -> Option<f64> {
            self.submissions.as_ref().map(|submissions| lock(&submissions.bucket).tokens)
        }

        /// When the global bucket last refilled, if it is enabled.
        fn global_last_refill(&self) -> Option<Instant> {
            self.global.as_ref().map(|global| lock(&global.bucket).last_refill)
        }
    }

    /// The background task sweeps once a second: an entry idle by the
    /// limiter's clock survives until the second has passed, then goes.
    #[tokio::test(start_paused = true)]
    async fn run_gc_sweeps_once_per_interval() {
        let clock = ManualClock::new();
        let limiters = Arc::new(limiters(clock.clone(), Some(limit(10, 2)), None, 16));
        assert!(limiters.check(Some(ip(1))).is_ok());
        clock.advance(Duration::from_secs(1));
        let shutdown = tn_types::Notifier::new();
        let task = tokio::spawn(run_gc(Arc::clone(&limiters), shutdown.subscribe()));
        // the task starts its ticker and takes the immediate first tick
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(999)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(limiters.per_ip_len(), 1, "swept before the interval had passed");
        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        assert_eq!(limiters.per_ip_len(), 0, "the idle entry survived the sweep");
        shutdown.notify();
        assert!(matches!(task.await, Ok(Ok(()))), "the sweep task did not stop cleanly");
    }

    /// A [`ManualClock`] whose first reading once `armed` is set stalls: it
    /// takes the time, says so on `entered`, then waits up to 500 ms for a
    /// message on `release` before returning that time, stale by then.
    #[derive(Clone)]
    struct GatedClock {
        inner: ManualClock,
        gate: Arc<Gate>,
    }

    struct Gate {
        armed: AtomicBool,
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }

    impl Clock for GatedClock {
        fn now(&self) -> Instant {
            let now = self.inner.now();
            if self.gate.armed.swap(false, Ordering::SeqCst) {
                // the test waits on `entered`; a send fails only if it gave up
                let _ = self.gate.entered.send(());
                // a timeout is the expected outcome when the reader holds the
                // lock the releasing thread waits on
                let _ = lock(&self.gate.release).recv_timeout(Duration::from_millis(500));
            }
            now
        }
    }

    /// Thread A reads the clock and stalls in that reading while the clock
    /// moves on and thread B admits a request. The clock is read under the
    /// bucket lock, so A holds the bucket while it stalls (asserted with
    /// `try_lock`) and B refills after it. The rewind itself is also prevented
    /// by [`Bucket::try_admit`] never moving `last_refill` backwards, so the
    /// lock assertion is what pins the order.
    #[test]
    fn stale_clock_read_cannot_rewind_a_bucket() {
        let (entered, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release) = std::sync::mpsc::channel();
        let gate =
            Arc::new(Gate { armed: AtomicBool::new(false), entered, release: Mutex::new(release) });
        let clock = GatedClock { inner: ManualClock::new(), gate: Arc::clone(&gate) };
        let limiters = Arc::new(limiters(clock.clone(), None, Some(limit(1, 10)), 16));
        gate.armed.store(true, Ordering::SeqCst);
        let thread_a = {
            let limiters = Arc::clone(&limiters);
            std::thread::spawn(move || limiters.check(None))
        };
        entered_rx.recv().expect("thread A read the clock");
        let global = limiters.global.as_ref().expect("global limit on");
        assert!(
            matches!(global.bucket.try_lock(), Err(std::sync::TryLockError::WouldBlock)),
            "thread A read the clock without holding the bucket lock"
        );
        clock.inner.advance(Duration::from_millis(100));
        let t2 = clock.inner.now();
        // thread B is this one
        assert!(limiters.check(None).is_ok());
        // with the fix, A has stopped waiting already and nobody receives this
        let _ = release_tx.send(());
        assert!(thread_a.join().expect("thread A finished").is_ok());
        assert_eq!(limiters.global_last_refill(), Some(t2), "a stale reading rewound the bucket");
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

    /// A stale reading neither refills the bucket nor moves its refill clock
    /// back, so the next refill credits only the time since the latest one.
    #[test]
    fn bucket_never_rewinds_its_refill_clock() {
        let t1 = ManualClock::new().now();
        let t2 = t1 + Duration::from_millis(100);
        let t3 = t2 + Duration::from_millis(500);
        let (rate, capacity) = (10.0, 10.0);
        let mut bucket = Bucket::full(t1, capacity);
        assert!(bucket.try_admit(t2, rate, capacity, capacity));
        assert!(!bucket.try_admit(t1, rate, capacity, 1.0), "a stale reading refilled the bucket");
        assert_eq!(bucket.last_refill, t2, "a stale reading moved the refill clock back");
        // (t3 - t2) x rate = 5 tokens, not the 6 that t1..t3 would credit
        assert_eq!(bucket.replenished(t3, rate, capacity), 5.0);
    }

    #[test]
    fn only_get_and_head_on_probe_paths_are_exempt() {
        for path in [HEALTH_PATH, READY_PATH] {
            assert!(is_exempt(&Method::GET, path), "GET {path}");
            assert!(is_exempt(&Method::HEAD, path), "HEAD {path}");
            for method in [Method::POST, Method::PUT, Method::OPTIONS] {
                assert!(!is_exempt(&method, path), "{method} {path} was exempt");
            }
        }
        assert!(!is_exempt(&Method::GET, "/"), "GET / was exempt");
        assert!(!is_exempt(&Method::GET, "/health/"), "a path near a probe's was exempt");
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
        assert_eq!(limiters.global_tokens(), Some(0.0));
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
        assert!(refilled.charge(false).is_ok());
        let refilled = limiters.check_edge(None).expect("edge").expect("a budget verdict");
        assert!(refilled.charge(true).is_ok());
    }

    /// The reviewer's case: a submission flood over its own budget is refused
    /// without spending a global token, so reads keep their whole budget, and
    /// each refusal keeps the client's per-IP token spent.
    #[test]
    fn submission_flood_over_its_budget_leaves_the_read_budget_intact() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 100)),
            Some(limit(1, 2)),
            limit(1, 1),
        );
        let mut admitted = 0;
        for _ in 0..10 {
            let budget = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
            admitted += usize::from(budget.charge(true).is_ok());
        }
        assert_eq!(admitted, 1, "the submission bucket holds one token");
        assert_eq!(limiters.global_tokens(), Some(2.0), "a submission spent a global token");
        assert_eq!(
            limiters.per_ip_tokens(ip(1)),
            Some(90.0),
            "a refused submission got its per-IP token back"
        );
        // the read budget is whole: two reads are admitted, the third refused
        for _ in 0..2 {
            let read = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
            assert!(read.charge(false).is_ok());
        }
        let read = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
        assert!(matches!(read.charge(false), Err(GatewayError::RateLimited)));
    }

    /// The other direction, through the overflow bucket: a read flood over the
    /// global budget spends no submission token, and each refused read keeps
    /// the overflow token the edge charged spent, so the overflow bucket
    /// refuses at the edge once the refused reads have spent its burst.
    #[test]
    fn refused_read_spends_no_submission_token_and_keeps_the_overflow_token_spent() {
        // per-IP burst 2 at 1/s with a one-entry table, so every client after
        // ip(1) spends from the overflow bucket; global and submission burst 1
        let limiters = Arc::new(
            RateLimiters::with_clock(
                ManualClock::new(),
                Some(limit(1, 2)),
                Some(limit(1, 1)),
                Some(limit(1, 1)),
                1,
                PrefixPolicy::default(),
            )
            .expect("limiters"),
        );
        let read = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok(), "the global bucket's only token");
        // ip(2) and ip(3) share the overflow bucket's two tokens; their reads
        // are refused by the empty global bucket and keep those tokens spent
        for client in [ip(2), ip(3)] {
            let read = limiters.check_edge(Some(client)).expect("edge").expect("a budget");
            assert!(matches!(read.charge(false), Err(GatewayError::RateLimited)));
        }
        assert!(
            matches!(limiters.check_edge(Some(ip(2))), Err(GatewayError::RateLimited)),
            "a refused read got its overflow token back"
        );
        assert_eq!(limiters.per_ip_len(), 1);
        assert_eq!(
            limiters.submission_tokens(),
            Some(1.0),
            "a refused read spent the submission token"
        );
        // ip(1) has one token of its own left, and the submission bucket its one
        let submission = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(submission.charge(true).is_ok());
    }

    /// A submission batch the global bucket refuses for its extra calls gets
    /// its submission token back, never past the burst; its per-IP token
    /// stays spent.
    #[test]
    fn batch_refusal_refunds_the_class_token() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 10)),
            Some(limit(1, 1)),
            limit(1, 2),
        );
        let read = limiters.check_edge(None).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok(), "the global bucket's only token");
        let batch = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(batch.charge(true).is_ok());
        assert_eq!(limiters.submission_tokens(), Some(1.0));
        assert!(matches!(limiters.charge_batch(Some(ip(1)), 2), Err(GatewayError::RateLimited)));
        batch.refund(true);
        assert_eq!(limiters.submission_tokens(), Some(2.0), "the refused batch kept its token");
        batch.refund(true);
        assert_eq!(
            limiters.submission_tokens(),
            Some(2.0),
            "a refund lifted the bucket past its burst"
        );
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(9.0), "the edge token stays spent");
        assert_eq!(limiters.global_tokens(), Some(0.0));
    }

    /// The same for a query batch, refused by its own per-IP bucket: the
    /// global token its first call took comes back.
    #[test]
    fn query_batch_refusal_refunds_the_global_token() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 2)),
            Some(limit(1, 4)),
            limit(1, 1),
        );
        let batch = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(batch.charge(false).is_ok());
        assert_eq!(limiters.global_tokens(), Some(3.0));
        // one per-IP token left, two needed: refused before the global bucket
        assert!(matches!(limiters.charge_batch(Some(ip(1)), 2), Err(GatewayError::RateLimited)));
        batch.refund(false);
        assert_eq!(limiters.global_tokens(), Some(4.0), "the refused batch kept its global token");
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(1.0), "the edge token stays spent");
        assert_eq!(limiters.submission_tokens(), Some(1.0));
    }

    /// The README's bound: with the submission budget on, every call that
    /// gets past the proxy's charges spent a global or a submission token, so
    /// at no refill a gateway admits at most the two bursts together, whatever
    /// mix of reads, submissions and batches it is sent.
    #[test]
    fn admitted_calls_stay_within_global_plus_submissions() {
        let limiters =
            limiters_with_submissions(ManualClock::new(), None, Some(limit(1, 4)), limit(1, 3));
        let mut admitted = 0;
        for round in 0..20 {
            let submission = round % 2 == 0;
            let extra = round % 3;
            let budget = limiters.check_edge(None).expect("edge").expect("a budget");
            if budget.charge(submission).is_ok() {
                if limiters.charge_batch(None, extra).is_ok() {
                    admitted += 1 + extra;
                } else {
                    // as the handler does (proxy.rs, the batch-charge refusal arm)
                    budget.refund(submission);
                }
            }
        }
        assert!(admitted <= 4 + 3, "{admitted} calls admitted over two bursts of 4 and 3");
        assert!(admitted >= 3, "the submission budget admitted nothing");
    }

    #[test]
    fn with_a_submission_budget_the_per_ip_check_runs_first_and_still_refuses() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 1)),
            Some(limit(1, 2)),
            limit(1, 1),
        );
        let first = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget verdict");
        assert!(first.charge(false).is_ok());
        // the client's own bucket is empty: refused at the edge, without
        // spending the global token another client can still use
        assert!(matches!(limiters.check_edge(Some(ip(1))), Err(GatewayError::RateLimited)));
        let other = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget verdict");
        // the edge charges no shared bucket at all: the class is not known yet
        assert_eq!(limiters.global_tokens(), Some(1.0));
        assert!(other.charge(false).is_ok());
    }

    /// With a submission budget, the edge's per-IP check alone adds no table
    /// entry: a first-seen client is tracked at its class charge, whatever the
    /// verdict, with the edge's token spent. Without the budget the edge check
    /// inserts the entry itself, and only once the global bucket admits the
    /// request, as `global_rejection_leaves_no_entry_for_a_new_client` pins.
    #[test]
    fn check_edge_inserts_no_entry_until_the_class_charge() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 2)),
            Some(limit(1, 2)),
            limit(1, 1),
        );
        let budget = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert_eq!(limiters.per_ip_len(), 0, "the edge tracked a client no shared bucket admitted");
        assert!(budget.charge(false).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(1.0), "the edge token was not kept");
        assert_eq!(limiters.global_tokens(), Some(1.0));
    }

    /// A request answered before it is classified (a `GET` refused with `405`)
    /// drops its budget uncharged: it pays a global token as a query, and the
    /// client is tracked with its edge token spent, as `check` would leave it.
    #[test]
    fn edge_admission_without_a_class_charge_costs_a_global_token() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 2)),
            Some(limit(1, 2)),
            limit(1, 1),
        );
        let budget = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert_eq!(limiters.global_tokens(), Some(2.0), "the edge charged a shared bucket");
        drop(budget);
        assert_eq!(limiters.global_tokens(), Some(1.0), "an unclassified request was free");
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(1.0));
    }

    /// The same with the global bucket empty: the unclassified request is
    /// refused as a query, and the client is tracked anyway with its edge token
    /// spent, as a class refusal leaves it, so a source whose requests are all
    /// answered unclassified meets its per-IP limit at the edge while the
    /// global limit refuses them.
    #[test]
    fn edge_admission_without_a_class_charge_tracks_the_client_when_global_refuses() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 1)),
            Some(limit(1, 1)),
            limit(1, 1),
        );
        let read = limiters.check_edge(None).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok(), "the global bucket's only token");
        drop(limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget"));
        assert_eq!(limiters.global_tokens(), Some(0.0));
        assert_eq!(limiters.per_ip_len(), 1, "a globally refused new client was not tracked");
        assert_eq!(
            limiters.per_ip_tokens(ip(1)),
            Some(0.0),
            "the unclassified request's per-IP token was not kept"
        );
        assert!(
            matches!(limiters.check_edge(Some(ip(1))), Err(GatewayError::RateLimited)),
            "a client whose requests are all answered unclassified was not limited per IP"
        );
        assert!(limiters.check_edge(Some(ip(2))).is_ok(), "another client was refused at the edge");
    }

    /// A budget is settled once: a charged one costs nothing more when it is
    /// dropped, and a clone dropped before the charge settles nothing.
    #[test]
    fn dropping_a_charged_budget_charges_nothing_more() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 4)),
            Some(limit(1, 4)),
            limit(1, 2),
        );
        let read = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok());
        drop(read);
        assert_eq!(limiters.global_tokens(), Some(3.0), "a charged read paid twice");

        // the request extension's copy goes before the handler charges its own
        let submission = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        drop(submission.clone());
        assert_eq!(limiters.global_tokens(), Some(3.0), "a live budget was settled");
        assert!(submission.charge(true).is_ok());
        drop(submission);
        assert_eq!(limiters.global_tokens(), Some(3.0), "a charged submission paid a global token");
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(2.0));
    }

    /// A first-seen client whose request its class bucket refuses is tracked
    /// anyway, with the edge's per-IP token spent: that token paid for reading
    /// the body.
    #[test]
    fn class_refusal_tracks_a_first_seen_client() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 2)),
            Some(limit(1, 1)),
            limit(1, 1),
        );
        let read = limiters.check_edge(None).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok(), "the global bucket's only token");
        let refused = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(matches!(refused.charge(false), Err(GatewayError::RateLimited)));
        drop(refused);
        assert_eq!(limiters.per_ip_len(), 1, "a client its class bucket refused was not tracked");
        assert_eq!(
            limiters.per_ip_tokens(ip(1)),
            Some(1.0),
            "the refused request's per-IP token was not kept"
        );
    }

    /// Requests the class charge keeps refusing spend the client's per-IP
    /// burst, so once it is gone the edge refuses them before their bodies are
    /// read, and no shared bucket is spent on them.
    #[test]
    fn class_refused_requests_spend_the_per_ip_burst() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 3)),
            Some(limit(1, 1)),
            limit(1, 1),
        );
        let read = limiters.check_edge(None).expect("edge").expect("a budget");
        assert!(read.charge(false).is_ok(), "the global bucket's only token");
        for _ in 0..3 {
            let refused = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
            assert!(matches!(refused.charge(false), Err(GatewayError::RateLimited)));
        }
        assert!(
            matches!(limiters.check_edge(Some(ip(1))), Err(GatewayError::RateLimited)),
            "refused requests were read at no per-IP cost"
        );
        assert_eq!(limiters.global_tokens(), Some(0.0));
        assert_eq!(limiters.submission_tokens(), Some(1.0));
    }

    /// Two first requests from one new prefix both pass the edge on fresh
    /// buckets; once both are charged the prefix has one entry that paid for
    /// both.
    #[test]
    fn concurrent_first_requests_from_one_prefix_share_one_bucket() {
        let limiters = limiters_with_submissions(
            ManualClock::new(),
            Some(limit(1, 4)),
            Some(limit(1, 4)),
            limit(1, 2),
        );
        let first = limiters.check_edge(Some(ip6(1, 1))).expect("edge").expect("a budget");
        let second = limiters.check_edge(Some(ip6(1, 2))).expect("edge").expect("a budget");
        assert!(first.charge(false).is_ok());
        assert!(second.charge(true).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
        assert_eq!(limiters.per_ip_tokens(ip6(1, 3)), Some(2.0));
    }

    /// Limiters with a one-entry per-IP table (burst 4) under a submission
    /// budget, for the deferred-insertion tests.
    fn one_entry_limiters(clock: ManualClock) -> Arc<RateLimiters<ManualClock>> {
        let limiters = RateLimiters::with_clock(
            clock,
            Some(limit(1, 4)),
            Some(limit(1, 10)),
            Some(limit(1, 10)),
            1,
            PrefixPolicy::default(),
        );
        Arc::new(limiters.expect("limiters"))
    }

    /// Two first-seen clients pass the edge against an empty one-entry table.
    /// The first one charged takes the slot; the second, charged while the
    /// table is full and less than an interval after the last inline sweep,
    /// is admitted untracked rather than growing the table past its cap.
    #[test]
    fn deferred_insertion_never_grows_a_full_table() {
        let limiters = one_entry_limiters(ManualClock::new());
        let first = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        let second = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
        assert!(first.charge(false).is_ok());
        assert!(second.charge(false).is_ok());
        assert_eq!(limiters.per_ip_len(), 1, "insert_fresh grew the table past its cap");
        assert_eq!(limiters.per_ip_tokens(ip(2)), None);
    }

    /// A deferred first-seen client that meets a full table once an inline
    /// sweep is due takes the slot an idle entry frees, on the bucket the
    /// edge charged.
    #[test]
    fn deferred_insertion_takes_a_slot_an_idle_entry_frees() {
        let clock = ManualClock::new();
        let limiters = one_entry_limiters(clock.clone());
        let first = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        let second = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
        assert!(first.charge(false).is_ok());
        // ip(1) refills to its burst of 4 and is idle
        clock.advance(INLINE_SWEEP_INTERVAL);
        assert!(second.charge(false).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(2)), Some(3.0));
        assert_eq!(limiters.per_ip_len(), 1);
    }

    /// A deferred first-seen client that meets a full table whose only entry
    /// is still busy at the inline sweep is admitted untracked, and the busy
    /// entry keeps its slot.
    #[test]
    fn deferred_insertion_never_evicts_a_busy_entry() {
        let clock = ManualClock::new();
        let limiters = one_entry_limiters(clock.clone());
        let first = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        let second = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
        assert!(first.charge(false).is_ok());
        // three more requests take ip(1) from 3 tokens to 0
        for _ in 0..3 {
            let budget = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
            assert!(budget.charge(false).is_ok());
        }
        assert_eq!(limiters.per_ip_tokens(ip(1)), Some(0.0));
        // ip(1) refills to 1 of 4: busy at the sweep
        clock.advance(INLINE_SWEEP_INTERVAL);
        assert!(second.charge(false).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(2)), None);
        assert!(limiters.per_ip_tokens(ip(1)).is_some(), "a busy entry was evicted");
        assert_eq!(limiters.per_ip_len(), 1);
    }

    /// More concurrent first requests from one new prefix than its burst all
    /// pass the edge on fresh buckets; what they spend past the burst is
    /// recorded against the entry as debt, down to minus one burst, so the
    /// prefix is refused until the bucket has refilled past it.
    #[test]
    fn first_requests_beyond_the_burst_are_recorded_as_debt() {
        let clock = ManualClock::new();
        let limiters = limiters_with_submissions(
            clock.clone(),
            Some(limit(1, 2)),
            Some(limit(1, 10)),
            limit(1, 10),
        );
        // five first requests from one /64, all admitted before any is tracked
        let budgets: Vec<_> = (1..=5)
            .map(|host| limiters.check_edge(Some(ip6(1, host))).expect("edge").expect("a budget"))
            .collect();
        assert_eq!(limiters.per_ip_len(), 0);
        for budget in &budgets[..4] {
            assert!(budget.charge(false).is_ok());
        }
        // the first inserts the bucket with one token left, the next three
        // take it to -2
        assert_eq!(
            limiters.per_ip_tokens(ip6(1, 9)),
            Some(-2.0),
            "the spends past the burst were not recorded"
        );
        assert!(budgets[4].charge(false).is_ok());
        assert_eq!(
            limiters.per_ip_tokens(ip6(1, 9)),
            Some(-2.0),
            "the debt went past minus one burst"
        );
        assert!(matches!(limiters.check_edge(Some(ip6(1, 9))), Err(GatewayError::RateLimited)));
        // two seconds bring the bucket back to zero, not to one token
        clock.advance(Duration::from_secs(2));
        assert!(
            matches!(limiters.check_edge(Some(ip6(1, 9))), Err(GatewayError::RateLimited)),
            "the debt was forgiven"
        );
        clock.advance(Duration::from_secs(1));
        assert!(limiters.check_edge(Some(ip6(1, 9))).is_ok());
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

    /// A charge metered on the overflow bucket is counted, so an operator can
    /// see first-seen clients sharing one client's budget; a tracked client's
    /// request, charged to its own bucket, is not.
    #[test]
    fn overflow_charges_are_counted() {
        const OVERFLOW: &str = "tn_worker_gateway_rate_limit_overflow_total";
        let mut metrics = crate::telemetry::test_utils::CapturedMetrics::install();
        let limiters = one_entry_limiters(ManualClock::new());
        let tracked = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(tracked.charge(false).is_ok());
        assert_eq!(limiters.per_ip_len(), 1);
        assert_eq!(
            metrics.value(OVERFLOW, &[]),
            0.0,
            "a client the table had room for was counted"
        );
        // the table is full of a busy entry and was swept less than an interval
        // ago, so the second client is metered on the overflow bucket
        let untracked = limiters.check_edge(Some(ip(2))).expect("edge").expect("a budget");
        assert!(untracked.charge(false).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(2)), None, "the full table tracked a second client");
        assert_eq!(metrics.value(OVERFLOW, &[]), 1.0);
        let again = limiters.check_edge(Some(ip(1))).expect("edge").expect("a budget");
        assert!(again.charge(false).is_ok());
        assert_eq!(metrics.value(OVERFLOW, &[]), 1.0, "a tracked client's own bucket was counted");
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
    fn full_table_evicts_an_idle_entry_for_a_new_client() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), Some(limit(1, 2)), None, 2);
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_ok());
        // both buckets refill and go idle; a sweep may run inline again
        clock.advance(INLINE_SWEEP_INTERVAL);
        assert!(limiters.check(Some(ip(3))).is_ok());
        // the sweep freed every idle entry, so the new client has its own bucket
        assert_eq!(limiters.per_ip_tokens(ip(3)), Some(1.0), "the new client was not tracked");
        assert_eq!(limiters.per_ip_len(), 1);
        // and the next new client a slot of its own too, within the cap
        assert!(limiters.check(Some(ip(4))).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(4)), Some(1.0));
        assert_eq!(limiters.per_ip_len(), 2);
    }

    #[test]
    fn full_table_never_evicts_a_busy_entry() {
        let clock = ManualClock::new();
        let limiters = limiters(clock.clone(), Some(limit(1, 2)), None, 2);
        for client in [ip(1), ip(1), ip(2), ip(2)] {
            assert!(limiters.check(Some(client)).is_ok());
        }
        // a sweep may run, but after a second each bucket holds 1 of its 2
        // tokens: both are busy
        clock.advance(INLINE_SWEEP_INTERVAL);
        assert!(limiters.check(Some(ip(3))).is_ok(), "the overflow bucket refused");
        assert_eq!(limiters.per_ip_tokens(ip(3)), None, "a busy entry was evicted");
        assert_eq!(limiters.per_ip_len(), 2);
        assert!(limiters.per_ip_tokens(ip(1)).is_some() && limiters.per_ip_tokens(ip(2)).is_some());
    }

    #[test]
    fn inline_sweep_runs_at_most_once_per_interval() {
        let clock = ManualClock::new();
        // 2 tokens/s with a burst of 1: a spent bucket is idle 500 ms later
        let limiters = limiters(clock.clone(), Some(limit(2, 1)), None, 2);
        clock.advance(INLINE_SWEEP_INTERVAL);
        assert!(limiters.check(Some(ip(1))).is_ok());
        assert!(limiters.check(Some(ip(2))).is_ok());
        // this sweep finds both entries busy, frees nothing, and starts the
        // interval; ip(3) spends the overflow bucket
        assert!(limiters.check(Some(ip(3))).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(3)), None);
        // both entries go idle, but within the interval no sweep runs again
        clock.advance(Duration::from_millis(600));
        assert!(limiters.check(Some(ip(4))).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(4)), None, "a sweep ran within the interval");
        assert_eq!(limiters.per_ip_len(), 2);
        // once the interval has passed, the next new client sweeps them
        clock.advance(Duration::from_millis(400));
        assert!(limiters.check(Some(ip(5))).is_ok());
        assert_eq!(limiters.per_ip_tokens(ip(5)), Some(0.0), "the idle entries were not swept");
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
