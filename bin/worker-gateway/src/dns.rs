//! Cached, concurrency-capped name resolution for the gateway's upstream
//! clients.
//!
//! reqwest's default resolver runs `getaddrinfo` on tokio's blocking pool, and
//! a lookup cannot be cancelled once it has started: when the connect timeout
//! drops the resolve future, the lookup keeps its thread until the system
//! resolver gives up. Under steady traffic to an upstream whose name resolves
//! slowly, every new connection starts another lookup, the blocking pool (512
//! threads) fills, and every other lookup in the process, readiness polls
//! included, queues behind them.
//!
//! [`CachingResolver`] bounds that:
//!
//! - An answer is cached per host for the TTL (`--dns-cache-ttl`), so steady traffic to an upstream
//!   does not resolve its name per connection. A TTL of zero stores nothing.
//! - At most one lookup per host runs at a time. A connection to a host whose lookup is in flight
//!   waits for that lookup instead of starting another.
//! - At most `--max-concurrent-dns-lookups` lookups run at once. Each runs in its own task, which
//!   holds a semaphore permit until the lookup returns, so a caller that gives up (the connect
//!   timeout) never frees a permit early, and the lookups and the threads they occupy stay bounded.
//! - When every permit is taken, a host with no usable cached answer fails at once. reqwest reports
//!   that as a connect error, which the proxy answers with `UpstreamUnreachable`.
//! - An expired answer is still served while one background lookup refreshes it, and keeps being
//!   served when that refresh fails or cannot start, for at most [`MAX_STALE`] past its expiry.
//!
//! Resolve outcomes are counted in `tn_worker_gateway_dns_lookups_total{result}`
//! (see [`crate::telemetry::record_dns_lookup`]): a resolve that joins a lookup
//! in flight adds nothing, and a stale answer that needs a refresh counts as a
//! `hit` and as that refresh's `miss` or `rejected`.
//!
//! The names the resolver sees come from the gateway's own configuration: the
//! worker RPC and readiness URLs and `--redirect-queries`. reqwest never calls a
//! resolver for an IP literal, and nothing in a client request becomes part of
//! an upstream URL. The readiness client follows the node's redirects, so a
//! readiness endpoint could name another host; the cache is capped at
//! [`MAX_ENTRIES`] hosts as a backstop.

use std::{
    collections::HashMap,
    fmt, io,
    net::SocketAddr,
    num::NonZeroUsize,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::{Duration, Instant},
};

use futures::{
    future::{BoxFuture, Shared},
    FutureExt as _,
};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

/// How long past its TTL an answer may still be served while every refresh
/// fails or cannot start: long enough to ride out a resolver outage, short
/// enough that a name that stays unresolvable is eventually forgotten. Unlike
/// RFC 8767, which ends serve-stale on an authoritative NXDOMAIN, every failure
/// counts here, because the system resolver's errors do not say which kind it
/// was; a deleted name is served for up to this long too.
const MAX_STALE: Duration = Duration::from_secs(24 * 60 * 60);

/// Most hosts the cache holds. The gateway resolves a handful of configured
/// upstream names, so this is only a backstop: when a new host would exceed
/// it, every entry with no lookup in flight is dropped first.
const MAX_ENTRIES: usize = 256;

/// A [`Resolve`] implementation that caches each host's addresses and caps
/// how many lookups run at once (see the module docs).
///
/// Each client gets its own instance, so two clients never share a cache or a
/// lookup cap.
pub(crate) struct CachingResolver {
    /// State shared with every lookup task, which writes its answer back here.
    cache: Arc<Cache>,
}

impl CachingResolver {
    /// A resolver over `lookup` (the system resolver, [`SystemLookup`],
    /// outside tests).
    ///
    /// An answer stays fresh for `ttl`; a zero `ttl` disables caching, so every
    /// resolve runs a lookup (still one per host at a time, and capped). At most
    /// `max_lookups` lookups run at once.
    pub(crate) fn new(
        lookup: Arc<dyn LookupHost>,
        ttl: Duration,
        max_lookups: NonZeroUsize,
    ) -> Self {
        Self::with_stale_limit(lookup, ttl, max_lookups, MAX_STALE)
    }

    /// [`Self::new`] with an explicit stale limit.
    fn with_stale_limit(
        lookup: Arc<dyn LookupHost>,
        ttl: Duration,
        max_lookups: NonZeroUsize,
        max_stale: Duration,
    ) -> Self {
        // `Semaphore::new` panics above `MAX_PERMITS`; a cap that large is no
        // cap at all, so clamp it instead of failing startup
        let permits = max_lookups.get().min(Semaphore::MAX_PERMITS);
        Self {
            cache: Arc::new(Cache {
                lookup,
                ttl,
                max_stale,
                permits: Arc::new(Semaphore::new(permits)),
                entries: Mutex::new(HashMap::new()),
                #[cfg(test)]
                counts: tests::Counts::default(),
            }),
        }
    }
}

impl Resolve for CachingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let cache = Arc::clone(&self.cache);
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addrs = cache.resolve(&host).await.map_err(BoxError::from)?;
            // reqwest wants a `'static` iterator, so it owns the shared answer
            // and copies one address at a time
            let len = addrs.len();
            Ok::<Addrs, BoxError>(Box::new((0..len).filter_map(move |i| addrs.get(i).copied())))
        })
    }
}

/// The lookup a [`CachingResolver`] runs when the cache cannot answer.
pub(crate) trait LookupHost: Send + Sync + 'static {
    /// Resolve `host` to its addresses. Any port is discarded: the cache
    /// stores port 0, which the client replaces with the URL's port.
    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>>;
}

/// The system resolver: `getaddrinfo` on tokio's blocking pool, as reqwest's
/// default resolver does.
#[derive(Debug)]
pub(crate) struct SystemLookup;

impl LookupHost for SystemLookup {
    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        async move { Ok(tokio::net::lookup_host((host.as_str(), 0)).await?.collect()) }.boxed()
    }
}

/// State shared by a [`CachingResolver`] and its lookup tasks.
struct Cache {
    /// The lookup run on a miss.
    lookup: Arc<dyn LookupHost>,
    /// How long an answer is fresh; zero disables caching.
    ttl: Duration,
    /// How long past `ttl` an answer may still be served.
    max_stale: Duration,
    /// One permit per lookup allowed to run at once.
    permits: Arc<Semaphore>,
    /// Per-host answers and lookups in flight. Locked only for short,
    /// non-blocking updates, never across an `.await`.
    entries: Mutex<HashMap<String, Entry>>,
    /// Per-result counts the tests read, since no metrics recorder is installed
    /// in unit tests.
    #[cfg(test)]
    counts: tests::Counts,
}

impl Cache {
    /// Resolve `host`: from the cache, by joining its lookup in flight, or by
    /// starting a lookup.
    async fn resolve(self: Arc<Self>, host: &str) -> LookupResult {
        match self.step(host) {
            Step::Answer(addrs) => Ok(addrs),
            Step::Wait(lookup) => lookup.await,
            Step::Reject => Err(LookupError::Saturated),
        }
    }

    /// Decide how to answer `host`, starting a lookup when one is needed and a
    /// permit is free.
    fn step(self: &Arc<Self>, host: &str) -> Step {
        let now = Instant::now();
        let mut entries = self.entries();
        if let Some(entry) = entries.get_mut(host) {
            if let Some(answer) = &entry.answer {
                let age = now.saturating_duration_since(answer.resolved_at);
                if age < self.ttl {
                    self.record(Outcome::Hit);
                    return Step::Answer(Arc::clone(&answer.addrs));
                }
                if age < self.ttl.saturating_add(self.max_stale) {
                    // stale: answer now and refresh in the background; when no
                    // permit is free the stale answer is all there is
                    let addrs = Arc::clone(&answer.addrs);
                    self.record(Outcome::Hit);
                    if entry.inflight.is_none() {
                        entry.inflight = self.start(host);
                    }
                    return Step::Answer(addrs);
                }
                // past the stale limit: resolve as if the host were uncached
                entry.answer = None;
            }
            if let Some(lookup) = &entry.inflight {
                return Step::Wait(lookup.clone());
            }
        }

        // no usable answer and no lookup in flight
        let Some(lookup) = self.start(host) else {
            // any entry left for the host is empty
            entries.remove(host);
            return Step::Reject;
        };
        if entries.len() >= MAX_ENTRIES && !entries.contains_key(host) {
            entries.retain(|_, entry| entry.inflight.is_some());
        }
        entries.entry(host.to_owned()).or_insert(Entry { answer: None, inflight: None }).inflight =
            Some(lookup.clone());
        Step::Wait(lookup)
    }

    /// Start a lookup for `host` in its own task, or return `None` when every
    /// permit is taken.
    ///
    /// The task, not the caller, owns the permit and writes the answer back
    /// (through a [`Completion`], so a task that panics or is cancelled still
    /// clears the in-flight marker): reqwest drops a resolve future at the
    /// connect timeout, but the system lookup underneath keeps its blocking
    /// thread until it returns, so the permit must outlive every caller to
    /// bound those threads. The task is short-lived (it ends when the lookup
    /// returns) and holds no gateway state beyond this cache, so it is spawned
    /// directly rather than through the task manager.
    fn start(self: &Arc<Self>, host: &str) -> Option<PendingLookup> {
        let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
            self.record(Outcome::Rejected);
            debug!(target: "gateway", host, "dns lookup rejected: every lookup slot is busy");
            return None;
        };
        self.record(Outcome::Miss);
        let cache = Arc::clone(self);
        let host = host.to_owned();
        let task = tokio::spawn(async move {
            // built inside the task, not before the spawn: `tokio::spawn` drops
            // the task at once on a runtime that is shutting down, while `step`
            // still holds the entries lock, and writing back there would
            // deadlock
            let mut completion = Completion { cache, host, result: None, _permit: permit };
            let result = completion.cache.run(&completion.host).await;
            completion.result = Some(result.clone());
            // writes the answer back, then releases the permit
            drop(completion);
            result
        });
        Some(task.map(|joined| joined.unwrap_or(Err(LookupError::Aborted))).boxed().shared())
    }

    /// Run the lookup for `host`, turning an empty answer or a panic into an
    /// error.
    async fn run(&self, host: &str) -> LookupResult {
        let lookup = AssertUnwindSafe(async { self.lookup.lookup(host.to_owned()).await })
            .catch_unwind()
            .await;
        let result = match lookup {
            Ok(Ok(addrs)) if addrs.is_empty() => Err(LookupError::NoAddresses),
            // the client sets the URL's port only on port-0 addresses (a
            // non-zero port is kept when the URL has none), so clear any port
            // a lookup returned
            Ok(Ok(addrs)) => {
                Ok(addrs.into_iter().map(|addr| SocketAddr::new(addr.ip(), 0)).collect())
            }
            Ok(Err(err)) => Err(LookupError::Failed(Arc::new(err))),
            Err(_panic) => Err(LookupError::Aborted),
        };
        if let Err(err) = &result {
            self.record(Outcome::Error);
            debug!(target: "gateway", host, ?err, "dns lookup failed");
        }
        result
    }

    /// Record a finished lookup: store a successful answer (unless caching is
    /// off) and clear the in-flight marker. A failure keeps any earlier answer,
    /// which goes on being served until the stale limit.
    ///
    /// Called from [`Completion`]'s `Drop`, possibly while a panic unwinds, so
    /// it only takes the entries lock and updates the map, and never panics.
    fn complete(&self, host: &str, result: &LookupResult) {
        let mut entries = self.entries();
        // an entry with a lookup in flight is never removed, so this always
        // finds the one `step` inserted
        let Some(entry) = entries.get_mut(host) else {
            return;
        };
        entry.inflight = None;
        if let Ok(addrs) = result {
            if !self.ttl.is_zero() {
                entry.answer =
                    Some(Answer { addrs: Arc::clone(addrs), resolved_at: Instant::now() });
            }
        }
        if entry.answer.is_none() {
            entries.remove(host);
        }
    }

    /// Lock the entries.
    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        // every update under the lock is a plain field store, so a panic while
        // it was held cannot leave an entry half-written: a poisoned lock is
        // safe to keep using
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Count one resolve outcome.
    fn record(&self, outcome: Outcome) {
        crate::telemetry::record_dns_lookup(outcome.label());
        #[cfg(test)]
        self.counts.bump(outcome);
    }
}

/// What the cache holds for one host. An entry always has an answer, a lookup
/// in flight, or both.
struct Entry {
    /// The last successful answer.
    answer: Option<Answer>,
    /// The lookup in flight for this host, shared by every caller waiting on
    /// it.
    inflight: Option<PendingLookup>,
}

/// One successful lookup.
struct Answer {
    /// The addresses, all with port 0.
    addrs: Arc<[SocketAddr]>,
    /// When the lookup returned.
    resolved_at: Instant,
}

/// Writes a lookup task's result back to the cache when dropped, however the
/// task ends: normally, by a panic outside the lookup (in a metric or a log
/// call), or cancelled mid-lookup by runtime shutdown. Without it such a task
/// would leave the host's in-flight marker set, so no refresh would ever
/// start and, past the stale limit, every resolve of the host would join the
/// finished lookup and fail.
struct Completion {
    /// The cache to write back to.
    cache: Arc<Cache>,
    /// The host the lookup is for.
    host: String,
    /// The lookup's result, set once it returns; `None` writes back
    /// [`LookupError::Aborted`].
    result: Option<LookupResult>,
    /// The task's lookup permit, released after the write-back (fields drop
    /// after `Drop::drop` runs).
    _permit: OwnedSemaphorePermit,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let result = self.result.take().unwrap_or(Err(LookupError::Aborted));
        self.cache.complete(&self.host, &result);
    }
}

/// How a resolve proceeds after consulting the cache.
enum Step {
    /// Answer from the cache, fresh or stale.
    Answer(Arc<[SocketAddr]>),
    /// Wait for the host's lookup in flight.
    Wait(PendingLookup),
    /// Fail at once: no usable answer and no permit free.
    Reject,
}

/// The `result` label of one resolve outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// Answered from the cache, fresh or stale, without waiting.
    Hit,
    /// A lookup was started (a cold miss, a stale refresh, or any lookup with
    /// caching off).
    Miss,
    /// A started lookup failed or returned no addresses.
    Error,
    /// A lookup was needed but no permit was free.
    Rejected,
}

impl Outcome {
    /// The metric label.
    fn label(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Error => "error",
            Self::Rejected => "rejected",
        }
    }
}

/// Why a resolve failed. Cloneable, because every caller waiting on one lookup
/// gets the same result.
#[derive(Clone, Debug)]
enum LookupError {
    /// No usable cached answer and every lookup permit was taken.
    Saturated,
    /// The lookup returned an error.
    Failed(Arc<io::Error>),
    /// The lookup returned no addresses.
    NoAddresses,
    /// The lookup panicked, or its task was cancelled by runtime shutdown.
    Aborted,
}

impl fmt::Display for LookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Saturated => "dns lookup not started: every lookup slot is busy",
            Self::Failed(_) => "dns lookup failed",
            Self::NoAddresses => "dns lookup returned no addresses",
            Self::Aborted => "dns lookup aborted",
        })
    }
}

impl std::error::Error for LookupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Failed(err) => Some(&**err),
            Self::Saturated | Self::NoAddresses | Self::Aborted => None,
        }
    }
}

/// The result every caller waiting on one lookup receives.
type LookupResult = Result<Arc<[SocketAddr]>, LookupError>;

/// A lookup in flight, awaitable by any number of callers.
type PendingLookup = Shared<BoxFuture<'static, LookupResult>>;

/// The error type reqwest's resolver interface carries.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Test lookups are plain closures.
#[cfg(test)]
impl<F> LookupHost for F
where
    F: Fn(String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> + Send + Sync + 'static,
{
    fn lookup(&self, host: String) -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        self(host)
    }
}

/// Wrap an async closure as a [`LookupHost`].
#[cfg(test)]
pub(crate) fn lookup_fn<F, Fut>(lookup: F) -> Arc<dyn LookupHost>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = io::Result<Vec<SocketAddr>>> + Send + 'static,
{
    Arc::new(move |host: String| -> BoxFuture<'static, io::Result<Vec<SocketAddr>>> {
        lookup(host).boxed()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    /// Per-outcome resolve counts, bumped beside the metric.
    #[derive(Debug, Default)]
    pub(super) struct Counts {
        hit: AtomicU64,
        miss: AtomicU64,
        error: AtomicU64,
        rejected: AtomicU64,
    }

    impl Counts {
        pub(super) fn bump(&self, outcome: Outcome) {
            let count = match outcome {
                Outcome::Hit => &self.hit,
                Outcome::Miss => &self.miss,
                Outcome::Error => &self.error,
                Outcome::Rejected => &self.rejected,
            };
            count.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// A resolver's counts as `[hit, miss, error, rejected]`.
    fn counts(resolver: &CachingResolver) -> [u64; 4] {
        let counts = &resolver.cache.counts;
        [&counts.hit, &counts.miss, &counts.error, &counts.rejected]
            .map(|count| count.load(Ordering::SeqCst))
    }

    fn resolver(lookup: Arc<dyn LookupHost>, ttl: Duration, max_lookups: usize) -> CachingResolver {
        CachingResolver::new(lookup, ttl, NonZeroUsize::new(max_lookups).expect("nonzero"))
    }

    /// Resolve `host` through the reqwest-facing entry point.
    async fn resolve(resolver: &CachingResolver, host: &str) -> Result<Vec<SocketAddr>, BoxError> {
        let name: Name = host.parse().expect("name");
        Ok(resolver.resolve(name).await?.collect())
    }

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, last], 0))
    }

    /// A lookup that answers its `n`th call (counting from 1) with
    /// `answer(n)`, and the shared call count.
    fn scripted(
        answer: impl Fn(usize) -> io::Result<Vec<SocketAddr>> + Send + Sync + 'static,
    ) -> (Arc<dyn LookupHost>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let lookup = lookup_fn(move |_host| {
            let result = answer(counter.fetch_add(1, Ordering::SeqCst) + 1);
            async move { result }
        });
        (lookup, calls)
    }

    /// A lookup that holds every call for `blocked` until `gate` is closed and
    /// answers any other host at once with `addr(2)`; `blocked` gets `addr(1)`.
    fn gated(
        blocked: &'static str,
        gate: Arc<Semaphore>,
    ) -> (Arc<dyn LookupHost>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let lookup = lookup_fn(move |host| {
            counter.fetch_add(1, Ordering::SeqCst);
            let gate = Arc::clone(&gate);
            async move {
                if host == blocked {
                    // closing the gate wakes every waiter with an error
                    let _closed = gate.acquire().await;
                    Ok(vec![addr(1)])
                } else {
                    Ok(vec![addr(2)])
                }
            }
        });
        (lookup, calls)
    }

    /// Poll `condition` every few milliseconds for up to five seconds.
    async fn eventually(mut condition: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if condition() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        condition()
    }

    #[tokio::test]
    async fn cached_addresses_are_reused_within_ttl() {
        let (lookup, calls) = scripted(|_| Ok(vec![addr(1)]));
        let resolver = resolver(lookup, Duration::from_secs(60), 8);

        assert_eq!(resolve(&resolver, "a.test").await.expect("first"), vec![addr(1)]);
        assert_eq!(resolve(&resolver, "a.test").await.expect("second"), vec![addr(1)]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts(&resolver), [1, 1, 0, 0]);
    }

    /// The client keeps a non-zero resolved port when the URL names none, so
    /// the resolver hands out port 0 whatever the lookup returned.
    #[tokio::test]
    async fn resolved_ports_are_cleared() {
        let (lookup, _calls) = scripted(|_| Ok(vec![SocketAddr::from(([10, 0, 0, 1], 9_999))]));
        let resolver = resolver(lookup, Duration::from_secs(60), 8);

        assert_eq!(resolve(&resolver, "a.test").await.expect("lookup"), vec![addr(1)]);
        assert_eq!(resolve(&resolver, "a.test").await.expect("cached"), vec![addr(1)]);
    }

    #[tokio::test]
    async fn expired_entry_is_refreshed() {
        let (lookup, _calls) = scripted(|n| Ok(vec![addr(if n == 1 { 1 } else { 2 })]));
        let resolver = resolver(lookup, Duration::from_millis(1), 8);

        assert_eq!(resolve(&resolver, "a.test").await.expect("first"), vec![addr(1)]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        // the expired answer comes back at once while a refresh runs behind it
        assert_eq!(resolve(&resolver, "a.test").await.expect("stale"), vec![addr(1)]);
        assert_eq!(counts(&resolver), [1, 2, 0, 0], "the stale hit started one refresh");

        let mut refreshed = false;
        for _ in 0..1_000 {
            if resolve(&resolver, "a.test").await.expect("resolve") == vec![addr(2)] {
                refreshed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(refreshed, "the refreshed answer replaces the expired one");
        let [hit, miss, error, rejected] = counts(&resolver);
        assert!(hit >= 2 && miss >= 2, "hit {hit}, miss {miss}");
        assert_eq!((error, rejected), (0, 0));
    }

    #[tokio::test]
    async fn stale_entry_is_served_when_the_refresh_fails() {
        let (lookup, calls) = scripted(|n| {
            if n == 1 {
                Ok(vec![addr(1)])
            } else {
                Err(io::Error::other("resolver down"))
            }
        });
        let resolver = resolver(lookup, Duration::from_millis(1), 8);

        assert_eq!(resolve(&resolver, "a.test").await.expect("first"), vec![addr(1)]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(resolve(&resolver, "a.test").await.expect("stale"), vec![addr(1)]);
        assert!(
            eventually(|| counts(&resolver)[2] >= 1).await,
            "the background refresh should have failed"
        );
        // the failed refresh did not evict the answer
        assert_eq!(resolve(&resolver, "a.test").await.expect("still stale"), vec![addr(1)]);
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[tokio::test]
    async fn answer_past_the_stale_limit_is_not_served() {
        let (lookup, _calls) = scripted(|n| {
            if n == 1 {
                Ok(vec![addr(1)])
            } else {
                Err(io::Error::other("resolver down"))
            }
        });
        let resolver = CachingResolver::with_stale_limit(
            lookup,
            Duration::from_millis(1),
            NonZeroUsize::new(8).expect("nonzero"),
            Duration::from_millis(1),
        );

        assert_eq!(resolve(&resolver, "a.test").await.expect("first"), vec![addr(1)]);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(resolve(&resolver, "a.test").await.is_err(), "too old to serve");
        assert_eq!(counts(&resolver), [0, 2, 1, 0]);
        assert!(resolver.cache.entries().is_empty());
    }

    /// The load-bearing test: with every permit taken, a host the cache cannot
    /// answer fails at once instead of queueing another lookup.
    #[tokio::test]
    async fn lookups_over_the_cap_fail_fast() {
        let gate = Arc::new(Semaphore::new(0));
        let (lookup, calls) = gated("a.test", Arc::clone(&gate));
        let resolver = Arc::new(resolver(lookup, Duration::from_secs(60), 1));

        let first = tokio::spawn({
            let resolver = Arc::clone(&resolver);
            async move { resolve(&resolver, "a.test").await.map_err(|err| err.to_string()) }
        });
        assert!(eventually(|| calls.load(Ordering::SeqCst) == 1).await, "a.test lookup started");

        let started = Instant::now();
        let err = resolve(&resolver, "b.test").await.expect_err("every permit is taken");
        assert!(started.elapsed() < Duration::from_secs(1), "rejection must not wait");
        assert!(err.to_string().contains("every lookup slot is busy"), "{err}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "b.test never reached the lookup");
        assert_eq!(counts(&resolver), [0, 1, 0, 1]);

        gate.close();
        assert_eq!(first.await.expect("join"), Ok(vec![addr(1)]));
        // the permit is back once the lookup has returned
        assert_eq!(resolve(&resolver, "b.test").await.expect("b.test"), vec![addr(2)]);
    }

    #[tokio::test]
    async fn one_lookup_runs_per_host() {
        let gate = Arc::new(Semaphore::new(0));
        let (lookup, calls) = gated("a.test", Arc::clone(&gate));
        let resolver = Arc::new(resolver(lookup, Duration::from_secs(60), 1));

        let waiters: Vec<_> = (0..5)
            .map(|_| {
                let resolver = Arc::clone(&resolver);
                tokio::spawn(async move {
                    resolve(&resolver, "a.test").await.map_err(|err| err.to_string())
                })
            })
            .collect();
        assert!(eventually(|| calls.load(Ordering::SeqCst) == 1).await);
        tokio::time::sleep(Duration::from_millis(20)).await;
        // the later callers joined the first lookup instead of being rejected
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(counts(&resolver), [0, 1, 0, 0]);

        gate.close();
        for waiter in waiters {
            assert_eq!(waiter.await.expect("join"), Ok(vec![addr(1)]));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn abandoned_caller_keeps_the_permit_until_the_lookup_returns() {
        let gate = Arc::new(Semaphore::new(0));
        let (lookup, calls) = gated("a.test", Arc::clone(&gate));
        let resolver = resolver(lookup, Duration::from_secs(60), 1);

        // the caller gives up, as reqwest does at its connect timeout
        let abandoned =
            tokio::time::timeout(Duration::from_millis(20), resolve(&resolver, "a.test")).await;
        assert!(abandoned.is_err(), "the lookup is still blocked");

        // the lookup it started still holds the only permit
        assert!(resolve(&resolver, "b.test").await.is_err());
        assert_eq!(counts(&resolver)[3], 1);

        gate.close();
        assert!(
            eventually(|| resolver
                .cache
                .entries()
                .get("a.test")
                .is_some_and(|entry| { entry.inflight.is_none() && entry.answer.is_some() }))
            .await,
            "the abandoned lookup still completes and caches its answer"
        );
        assert_eq!(resolve(&resolver, "a.test").await.expect("cached"), vec![addr(1)]);
        assert_eq!(resolve(&resolver, "b.test").await.expect("permit free"), vec![addr(2)]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn zero_ttl_disables_the_cache() {
        let (lookup, calls) = scripted(|_| Ok(vec![addr(1)]));
        let resolver = resolver(lookup, Duration::ZERO, 8);

        assert_eq!(resolve(&resolver, "a.test").await.expect("first"), vec![addr(1)]);
        assert_eq!(resolve(&resolver, "a.test").await.expect("second"), vec![addr(1)]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(counts(&resolver), [0, 2, 0, 0]);
        assert!(resolver.cache.entries().is_empty(), "nothing is stored");
    }

    #[tokio::test]
    async fn cache_holds_a_bounded_number_of_hosts() {
        let (lookup, _calls) = scripted(|_| Ok(vec![addr(1)]));
        let resolver = resolver(lookup, Duration::from_secs(60), 8);

        for n in 0..MAX_ENTRIES + 10 {
            resolve(&resolver, &format!("host-{n}.test")).await.expect("resolve");
        }
        let entries = resolver.cache.entries().len();
        assert!(entries <= MAX_ENTRIES, "{entries} entries");
    }

    /// A lookup task cancelled mid-lookup (here by dropping its runtime) still
    /// clears the host's in-flight marker, so a later resolve starts a fresh
    /// lookup instead of joining the cancelled one.
    #[test]
    fn cancelled_lookup_does_not_wedge_the_host() {
        let gate = Arc::new(Semaphore::new(0));
        let (lookup, calls) = gated("a.test", Arc::clone(&gate));
        let resolver = Arc::new(resolver(lookup, Duration::from_secs(60), 1));
        let runtime =
            || tokio::runtime::Builder::new_current_thread().enable_all().build().expect("runtime");

        let first = runtime();
        first.block_on(async {
            let resolver = Arc::clone(&resolver);
            drop(tokio::spawn(async move { resolve(&resolver, "a.test").await.is_ok() }));
            assert!(
                eventually(|| calls.load(Ordering::SeqCst) == 1).await,
                "a.test lookup started"
            );
        });
        // dropping the runtime cancels the lookup task while it waits on the gate
        drop(first);
        assert!(resolver.cache.entries().is_empty(), "the in-flight marker was cleared");
        assert_eq!(resolver.cache.permits.available_permits(), 1, "the permit was released");

        gate.close();
        runtime().block_on(async {
            assert_eq!(resolve(&resolver, "a.test").await.expect("fresh lookup"), vec![addr(1)]);
        });
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(counts(&resolver), [0, 2, 0, 0]);
    }

    #[tokio::test]
    async fn panicking_lookup_does_not_wedge_the_host() {
        let (lookup, _calls) = scripted(|n| {
            assert_ne!(n, 1, "the first lookup panics");
            Ok(vec![addr(1)])
        });
        let resolver = resolver(lookup, Duration::from_secs(60), 1);

        assert!(resolve(&resolver, "a.test").await.is_err());
        // the permit and the in-flight marker were both released
        assert_eq!(resolve(&resolver, "a.test").await.expect("second"), vec![addr(1)]);
        assert_eq!(counts(&resolver), [0, 2, 1, 0]);
    }
}
