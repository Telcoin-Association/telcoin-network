//! The gateway's HTTP surface: the JSON-RPC proxy plus liveness / readiness.
//!
//! A single server serves all three on one address: `GET` (or `HEAD`) on
//! `/health`, `/ready` and `/ready/any` are the orchestration probes, a `POST`
//! to any other path is JSON-RPC and goes to the proxy, and every other
//! request gets a `405` JSON-RPC error envelope without reaching an upstream.
//! Every response the service produces carries
//! `X-Content-Type-Options: nosniff`.
//! With `--probe-addr` set, a second listener serves the probes alone, outside
//! the connection cap and the rate limits, so a flood that holds every client
//! connection cannot make them time out.
//!
//! The accept loop is hand-rolled over hyper's HTTP/1 connection builder
//! rather than `axum::serve`: `axum::serve` never installs a hyper timer, so
//! hyper's header read timeout is silently disabled and a slow-loris client
//! could hold connections open forever. Here every connection gets a header
//! read deadline, a whole-request deadline, `TCP_NODELAY`, a bounded read
//! buffer, global and per-client connection caps, and two write-path guards
//! for the response-side slow loris (a client that stops or trickles its reads
//! while a response body streams to it): a transport-stall deadline
//! (`TCP_USER_TIMEOUT`) and a hard cap on total connection lifetime.
//!
//! The router also budgets the request-body bytes held across all in-flight
//! requests (see [`reserve_request_bytes`]), so the memory buffered bodies
//! take is bounded by one number rather than by the connection count.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    pin::pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, MutexGuard, PoisonError,
    },
    time::{Duration, Instant},
};

use axum::{
    body::HttpBody as _,
    extract::{ConnectInfo, DefaultBodyLimit, Request, State},
    handler::Handler as _,
    http::{header, HeaderValue, Method, StatusCode},
    middleware::{from_fn, from_fn_with_state, map_response, Next},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use futures::future::{self, Either};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::graceful::GracefulShutdown,
    service::TowerToHyperService,
};
use reqwest::Client;
use serde::Serialize;
use tn_types::{Noticer, TaskError, B256};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    error::{error_response, GatewayError},
    identity::{
        identify, read_proxy_header, ClientAddr, LocalConnection, ProxyHeader, TrustedProxies,
    },
    proxy::{proxy, AccessRecord, ForwardingStarted, QueryHeader, OVERSIZED_BODY_WARNINGS},
    ratelimit::{rate_limit, PrefixPolicy, RateLimiters},
    readiness::GatewayReadiness,
    telemetry::{self, Throttle},
};

/// Pause before re-polling `accept()` after it fails with an error that is
/// not about the one connection being accepted (see [`is_connection_error`]),
/// so a persistent accept error (e.g. fd exhaustion) cannot spin the loop hot.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Linux `ENONET`: the peer's machine is not on the network. With the four
/// below, one of the errors `accept(2)` documents as already pending on the
/// new socket, so it is about that connection alone; `std::io::ErrorKind` has
/// no kind for these and `libc` is not a dependency, hence the raw values.
const ENONET: i32 = 64;

/// Linux `EPROTO`: protocol error on the connection being accepted.
const EPROTO: i32 = 71;

/// Linux `ENOPROTOOPT`: protocol option not available on the new socket.
const ENOPROTOOPT: i32 = 92;

/// Linux `EOPNOTSUPP`: operation not supported on the new socket.
const EOPNOTSUPP: i32 = 95;

/// Linux `EHOSTDOWN`: the peer's host is down.
const EHOSTDOWN: i32 = 112;

/// Liveness probe path. Its `GET` and `HEAD` are exempt from rate limiting (see
/// [`crate::ratelimit`]); any other method is limited and then refused with a
/// `405`.
pub(crate) const HEALTH_PATH: &str = "/health";

/// Readiness probe path. Its `GET` and `HEAD` are exempt from rate limiting
/// (see [`crate::ratelimit`]); any other method is limited and then refused
/// with a `405`.
pub(crate) const READY_PATH: &str = "/ready";

/// Read-availability probe path. Its `GET` and `HEAD` are never rate limited:
/// [`router`] adds it after every layer but the `nosniff` one. Any other method
/// passes the identity and rate-limit layers before its `405`.
pub(crate) const READY_ANY_PATH: &str = "/ready/any";

/// Default budget of request-body bytes held across all in-flight requests
/// (`--max-inflight-request-bytes`), 512 MiB.
pub(crate) const MAX_INFLIGHT_REQUEST_BYTES: u32 = 512 * 1024 * 1024;

/// Default cap on hyper's per-connection read buffer (`--http1-max-buf-size`),
/// 64 KiB, against hyper's own default of about 408 KiB.
pub(crate) const HTTP1_MAX_BUF_SIZE: usize = 64 * 1024;

/// The smallest read buffer hyper accepts: `http1::Builder::max_buf_size`
/// panics below it.
pub(crate) const HTTP1_MIN_BUF_SIZE: usize = 8 * 1024;

/// Most submission hashes a [`RecentSubmissions`] set remembers at once; past
/// it the oldest is forgotten first. An entry costs a 32-byte hash and an
/// `Instant` in the queue plus the hash again in the index, so a full set
/// stays around a megabyte.
pub(crate) const RECENT_SUBMISSIONS_CAPACITY: usize = 10_000;

/// Default `--max-connections-per-ip`: room for a busy client's connection
/// pool, far below the default `--max-connections` (500), so one client
/// cannot hold every slot.
pub(crate) const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 32;

/// `reason` label on `tn_worker_gateway_connections_refused_total` for a
/// connection closed because its client prefix was at its cap.
const REFUSED_PER_IP_CAP: &str = "per_ip_cap";

/// `reason` label on `tn_worker_gateway_connections_refused_total` for a
/// connection from a `--proxy-protocol` peer closed because its PROXY header
/// was missing, malformed or late.
const REFUSED_PROXY_HEADER: &str = "proxy_header";

/// Throttle for the warning on a connection closed for its PROXY header. A
/// misconfigured front has every connection closed this way, so the warning
/// shows at the default log level, once per window.
pub(crate) static PROXY_HEADER_WARNINGS: Throttle = Throttle::new("proxy_header");

/// Throttle for the warning on a failed `accept()` on the client listener. A
/// persistent failure such as fd exhaustion repeats every
/// [`ACCEPT_RETRY_DELAY`], so the warning shows once per window, carrying the
/// count it skipped.
pub(crate) static ACCEPT_ERROR_WARNINGS: Throttle = Throttle::new("accept");

/// Throttle for the warning on a failed `accept()` on the `--probe-addr`
/// listener, kept apart from the client listener's so each shows.
pub(crate) static PROBE_ACCEPT_ERROR_WARNINGS: Throttle = Throttle::new("probe_accept");

/// How long a browser may cache the gateway's answer to a CORS preflight
/// (`Access-Control-Max-Age`).
const CORS_MAX_AGE: Duration = Duration::from_secs(60 * 60);

/// The access log's `status` for a sampled request dropped before it had a
/// response (the client closed the connection, the connection lifetime cap,
/// shutdown): nginx's "client closed request".
const CLIENT_CLOSED_REQUEST: u16 = 499;

/// Shared state handed to every request handler.
#[derive(Clone, Debug)]
pub(crate) struct AppState {
    /// Live readiness view of the configured upstream workers.
    pub(crate) readiness: Arc<GatewayReadiness>,
    /// Client for the worker route, and for the query route when `query_http`
    /// is `None` (built by [`crate::proxy::route_clients`] outside tests).
    pub(crate) http: Client,
    /// Endpoint serving every call the worker does not (`--redirect-queries`),
    /// or `None` when every call goes to the workers. It is not
    /// readiness-gated.
    pub(crate) query_upstream: Option<Url>,
    /// Header sent on every request to the query upstream and never to a
    /// worker (`--redirect-queries-header`), or `None`. Its `Debug` prints
    /// `<redacted>`, so this state can be debug-printed without leaking it.
    pub(crate) query_header: Option<QueryHeader>,
    /// Hashes of the submissions recently forwarded to a worker, whose lookups
    /// follow them there under `--redirect-queries`, or `None` when that
    /// routing is off (no redirect, or `--recent-submission-ttl 0`).
    pub(crate) recent_submissions: Option<Arc<RecentSubmissions>>,
    /// Method-name prefixes refused before forwarding when `query_upstream` is
    /// `None` (`--denied-method-prefixes`); empty allows every method.
    pub(crate) denied_method_prefixes: Arc<[String]>,
    /// Maximum calls in one JSON-RPC batch (`--max-batch-len`), or `None` when
    /// unlimited.
    pub(crate) max_batch_len: Option<NonZeroUsize>,
    /// Client for the query route, with `--query-request-timeout` as its read
    /// timeout and sharing `http`'s resolver, or `None`, which sends the query
    /// route through `http` (see [`crate::proxy::route_clients`]).
    pub(crate) query_http: Option<Client>,
    /// The deadline, from send, for reading a buffered submission answer whole
    /// (`--upstream-request-timeout`; see [`crate::proxy`]).
    pub(crate) upstream_request_timeout: Duration,
    /// In-flight cap for requests made only of submissions
    /// (`--max-inflight-submissions`); see [`inflight_slots`].
    pub(crate) submission_slots: Arc<Semaphore>,
    /// In-flight cap for every other request (`--max-inflight-queries`); see
    /// [`inflight_slots`].
    pub(crate) query_slots: Arc<Semaphore>,
    /// Cap on concurrent requests forwarded to a worker
    /// (`--max-upstream-inflight`); see [`inflight_slots`].
    pub(crate) upstream_slots: Arc<Semaphore>,
    /// Cap on the reads kept on the worker under `--redirect-queries`, a fifth
    /// of the worker cap, so they cannot hold the worker slots submissions
    /// need (see `crate::cli::Settings::max_worker_pool_reads`); taken in
    /// addition to an `upstream_slots` slot, and only with a redirect. See
    /// [`inflight_slots`].
    pub(crate) pool_read_slots: Arc<Semaphore>,
    /// Set by both accept loops once the shutdown notice arrives, so `/ready`
    /// and `/ready/any` answer `503` with `"draining": true` during
    /// `--shutdown-delay` while the listeners still accept and serve.
    pub(crate) draining: Arc<AtomicBool>,
}

/// The semaphore behind an in-flight cap of `cap` requests, where `0` means
/// unlimited (the semaphore's maximum, which no gateway can reach). Requests
/// take a permit without waiting (see [`crate::proxy`]), so the cap is a
/// fail-fast limit, never a queue.
pub(crate) fn inflight_slots(cap: usize) -> Arc<Semaphore> {
    let permits = if cap == 0 { Semaphore::MAX_PERMITS } else { cap.min(Semaphore::MAX_PERMITS) };
    Arc::new(Semaphore::new(permits))
}

/// Inbound connection limits enforced by the accept loop and router (derived
/// from the CLI flags; see [`crate::cli::Cli`]).
#[derive(Clone, Debug)]
pub(crate) struct ServerLimits {
    /// How long a new connection may take to send the complete request headers
    /// before it is closed (slow-loris guard).
    pub(crate) header_read_timeout: Duration,
    /// Deadline for a whole request: body read plus upstream response headers.
    /// A body trickled in below the size limit must still finish inside this.
    /// It is also the grace an exchange in flight gets to finish once its
    /// connection reaches the lifetime cap.
    pub(crate) request_deadline: Duration,
    /// Maximum concurrently-open inbound connections; further connections wait
    /// in the OS accept backlog.
    pub(crate) max_connections: NonZeroUsize,
    /// Maximum concurrently-open inbound connections per client prefix, or
    /// `None` when uncapped. A client is the connection's resolved address
    /// (a PROXY header's source, else the peer), and a trusted proxy's
    /// connection is not counted. A connection over the cap is closed before
    /// it is served, without a response, and its global slot is returned at
    /// once.
    pub(crate) max_connections_per_ip: Option<NonZeroUsize>,
    /// How a client address is masked to the key the per-client cap counts by
    /// (the rate limiter's prefix policy).
    pub(crate) client_prefix: PrefixPolicy,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`) armed on every accepted
    /// connection, or `None` when disabled. Closes a connection whose peer
    /// leaves written data unacknowledged (or its receive window closed) this
    /// long; Linux-family kernels only, best-effort elsewhere.
    pub(crate) tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single connection's total lifetime (keep-alive sessions
    /// included), or `None` when uncapped. Enforced by the runtime independent
    /// of connection progress, so it fires even when hyper's write path is
    /// backpressured by a slow-reading client and no future the connection
    /// owns is being polled forward. At the cap the connection stops taking
    /// new requests, and an exchange still in flight gets
    /// [`Self::request_deadline`] more to finish before the connection is
    /// closed.
    pub(crate) max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub(crate) max_request_bytes: usize,
    /// The peers whose connections must open with a PROXY protocol v2 header
    /// (`--proxy-protocol-from`, by default the `--trusted-proxies` ranges),
    /// or `None` when PROXY protocol (`--proxy-protocol`) is off. Every other
    /// connection is served as plain HTTP.
    pub(crate) proxy_protocol: Option<Arc<TrustedProxies>>,
    /// Budget of request-body bytes held across all in-flight requests, or
    /// `None` when unlimited (see [`reserve_request_bytes`]).
    pub(crate) max_inflight_request_bytes: Option<NonZeroU32>,
    /// Cap on each connection's read buffer, in bytes; it also caps the size
    /// of a request head. Values below [`HTTP1_MIN_BUF_SIZE`] are raised to it.
    pub(crate) http1_max_buf_size: usize,
    /// How long the listeners keep accepting and serving after the shutdown
    /// notice, with `/ready` and `/ready/any` reporting draining, before they
    /// close and the drain starts (`--shutdown-delay`); zero closes them at
    /// once.
    pub(crate) shutdown_delay: Duration,
}

/// Hashes of the submissions this gateway forwarded to a worker within the
/// last `ttl`, so that with `--redirect-queries` a lookup of one goes to the
/// worker whose pool holds the transaction instead of the query upstream,
/// which has not seen it (see [`crate::proxy`]).
///
/// The set is per process and best effort: a lookup that lands on another
/// gateway replica, or arrives after its hash was forgotten, goes to the query
/// upstream as it would without the set. It is bounded by age and by count, so
/// a flood of submissions can only push older hashes out early, never grow
/// memory past [`RECENT_SUBMISSIONS_CAPACITY`] entries.
#[derive(Debug)]
pub(crate) struct RecentSubmissions {
    /// How long a hash is remembered after its submission was forwarded.
    ttl: Duration,
    /// Most hashes remembered at once.
    capacity: usize,
    /// The remembered hashes, behind one lock so the queue and its index never
    /// disagree.
    entries: Mutex<RecentEntries>,
}

impl RecentSubmissions {
    /// An empty set that remembers each hash for `ttl`, and at most `capacity`
    /// hashes at once.
    pub(crate) fn new(ttl: Duration, capacity: usize) -> Self {
        Self { ttl, capacity, entries: Mutex::new(RecentEntries::default()) }
    }

    /// Remember `hash` as forwarded now, forgetting the oldest hash when the
    /// set is full. A hash already remembered keeps its first time, so
    /// resubmitting a transaction does not extend how long it is remembered.
    pub(crate) fn record(&self, hash: B256) {
        let mut entries = self.lock();
        // read the clock under the lock so the queue stays in time order
        let now = Instant::now();
        entries.forget_expired(now, self.ttl);
        if entries.index.insert(hash) {
            entries.order.push_back((hash, now));
            if entries.order.len() > self.capacity {
                entries.forget_oldest();
            }
        }
    }

    /// Whether `hash` was forwarded within the last `ttl`.
    pub(crate) fn contains(&self, hash: &B256) -> bool {
        let mut entries = self.lock();
        entries.forget_expired(Instant::now(), self.ttl);
        entries.index.contains(hash)
    }

    /// Lock the entries, recovering the guard if a previous holder panicked:
    /// the set only steers routing, so a poisoned lock is safe to reuse.
    fn lock(&self) -> MutexGuard<'_, RecentEntries> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The queue and index behind [`RecentSubmissions`]: every hash in `index` has
/// exactly one entry in `order`, and the other way round.
#[derive(Debug, Default)]
struct RecentEntries {
    /// Remembered hashes with the time each was forwarded, oldest first.
    order: VecDeque<(B256, Instant)>,
    /// The hashes in `order`, for constant-time lookup.
    index: HashSet<B256>,
}

impl RecentEntries {
    /// Forget every hash remembered for `ttl` or longer as of `now`.
    fn forget_expired(&mut self, now: Instant, ttl: Duration) {
        while self.order.front().is_some_and(|(_, at)| now.saturating_duration_since(*at) >= ttl) {
            self.forget_oldest();
        }
    }

    /// Forget the oldest remembered hash, if any.
    fn forget_oldest(&mut self) {
        if let Some((hash, _)) = self.order.pop_front() {
            self.index.remove(&hash);
        }
    }
}

/// Browser origins allowed to call the gateway cross-origin
/// (`--cors-allowed-origins`, validated in [`crate::cli`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CorsOrigins {
    /// Any origin (`*`).
    Any,
    /// Exactly these origins, each written as a browser sends it in `Origin`
    /// (`scheme://host[:port]`, lowercase, no default port). Never `*`.
    List(Vec<HeaderValue>),
}

/// The router's optional layers, each off unless configured, passed to
/// [`router`] and [`serve`] as one argument. `Default` installs none of them.
#[derive(Clone, Default)]
pub(crate) struct RouterLayers {
    /// Per-IP and global rate limiters (`--rate-limit-*`), installed inside
    /// the CORS layer; `None` when every limit is off.
    pub(crate) rate_limiters: Option<Arc<RateLimiters>>,
    /// Origins allowed to call the gateway cross-origin
    /// (`--cors-allowed-origins`); `None` leaves CORS off.
    pub(crate) cors: Option<CorsOrigins>,
    /// The sampled access log (`--access-log-sample`), installed outside the
    /// CORS layer; `None` when the sample rate is `0`.
    pub(crate) access_log: Option<Arc<AccessLog>>,
}

/// The sampled access log (`--access-log-sample`): one `gateway::access` line
/// for each sampled request (see [`log_access`]), written when the response
/// head is ready or, when the request is cancelled first, as it is dropped.
///
/// Sampling is deterministic and needs no random source: the `n`th counted
/// request, counting from one, is logged when `floor(n * rate)` exceeds
/// `floor((n - 1) * rate)`, so exactly `floor(n * rate)` of the first `n`
/// requests are logged, evenly spread. A rate of `1` logs every request. The
/// probes are not counted.
///
/// A sampled request is then held to the line ceiling
/// (`--access-log-max-lines-per-second`): a fixed one-second window counts the
/// lines written, and a sampled request over the ceiling writes no line and is
/// counted in `tn_worker_gateway_access_log_suppressed_total` instead. The
/// window is fixed, not sliding, so a burst across one window edge can write
/// about twice the ceiling within a second at most.
#[derive(Debug)]
pub(crate) struct AccessLog {
    /// Fraction of requests logged, in `(0, 1]`.
    rate: f64,
    /// Requests counted so far.
    requests: AtomicU64,
    /// Most lines written per second, after sampling; `None` is unlimited.
    max_lines_per_second: Option<NonZeroU32>,
    /// When the log was built; the ceiling's windows are whole seconds since.
    started: Instant,
    /// The ceiling's current window.
    window: Mutex<LineWindow>,
    /// Reduces a client address to the rate limiter's key for it (its network
    /// prefix), so a line names the client the way the limiter meters it.
    prefix: PrefixPolicy,
}

impl AccessLog {
    /// Build the access log for a sample `rate` already validated to `0..=1`,
    /// writing at most `max_lines_per_second` lines a second (`None` is
    /// unlimited), or `None` at a `0` rate, where the log is off and no layer
    /// is installed.
    pub(crate) fn new(
        rate: f64,
        max_lines_per_second: Option<NonZeroU32>,
        prefix: PrefixPolicy,
    ) -> Option<Arc<Self>> {
        (rate > 0.0).then(|| {
            Arc::new(Self {
                rate,
                requests: AtomicU64::new(0),
                max_lines_per_second,
                started: Instant::now(),
                window: Mutex::default(),
                prefix,
            })
        })
    }

    /// Count one request and report whether it is logged.
    fn sample(&self) -> bool {
        self.sample_at(self.started.elapsed().as_secs())
    }

    /// [`Self::sample`] for a request arriving in window `second` (whole
    /// seconds since the log was built).
    fn sample_at(&self, second: u64) -> bool {
        // the counter only hands each request a distinct position, which
        // `fetch_add` does under any ordering
        let before = self.requests.fetch_add(1, Ordering::Relaxed);
        // exact while the count stays below 2^53, which takes centuries at any
        // rate the gateway can serve
        let sampled = (before.saturating_add(1) as f64 * self.rate).floor()
            > (before as f64 * self.rate).floor();
        sampled && self.admit_line(second)
    }

    /// Whether a sampled request in window `second` still fits under the line
    /// ceiling; one that does not is counted as suppressed.
    fn admit_line(&self, second: u64) -> bool {
        let Some(max) = self.max_lines_per_second else { return true };
        // a poisoned lock only means a panic while holding it; the two plain
        // fields are still consistent enough to cap a log line
        let mut window = self.window.lock().unwrap_or_else(PoisonError::into_inner);
        // a request that read the clock just before a newer one took the lock
        // counts against the newer window rather than reopening its own
        if second > window.second {
            *window = LineWindow { second, lines: 0 };
        }
        if window.lines < max.get() {
            window.lines += 1;
            true
        } else {
            telemetry::record_access_log_suppressed();
            false
        }
    }
}

/// One window of the access log's line ceiling.
#[derive(Debug, Default)]
struct LineWindow {
    /// The window's second, counted from when the log was built.
    second: u64,
    /// Lines written in the window so far.
    lines: u32,
}

/// Request extension marking a request the access log samples. The proxy
/// notes the request's class and route in the [`AccessRecord`] as it learns
/// them, so the line names them even when the request's future is dropped
/// before a response exists; a request the log skips carries no slot and is
/// not classified for it.
#[derive(Clone, Debug)]
pub(crate) struct AccessSampled(pub(crate) Arc<AccessRecord>);

/// One sampled request's access line, written exactly once as the guard
/// drops: after [`log_access`] has the response head, or with the request's
/// future when the request is cancelled first.
#[derive(Debug)]
struct AccessEntry {
    /// The resolved client's network prefix, as the rate limiter keys it.
    client: String,
    /// When the access log took the request.
    started: Instant,
    /// The declared `Content-Length` (`0` when there is none), logged as
    /// `bytes_in` when the proxy never read the body.
    declared_bytes: u64,
    /// What the proxy noted about the request.
    record: Arc<AccessRecord>,
    /// The response status, or `None` while the request has no response.
    status: Option<u16>,
}

impl Drop for AccessEntry {
    fn drop(&mut self) {
        let (class, bytes_in) = self.record.class().unwrap_or(("unknown", self.declared_bytes));
        info!(
            target: "gateway::access",
            client = %self.client,
            class,
            route = self.record.route(),
            status = self.status.unwrap_or(CLIENT_CLOSED_REQUEST),
            latency_ms = u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            bytes_in,
            "request"
        );
    }
}

/// JSON body of the gateway's `/ready` response.
#[derive(Debug, Serialize)]
struct ReadyBody {
    /// Whether at least one upstream worker is currently ready and the gateway
    /// is not shutting down.
    ready: bool,
    /// Whether the gateway is shutting down; serialized only when true, so the
    /// body is unchanged while no shutdown is in progress.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    draining: bool,
}

/// JSON body of the gateway's `/ready/any` response.
#[derive(Debug, Serialize)]
struct ReadyAnyBody {
    /// Whether the gateway can serve any route and is not shutting down.
    ready: bool,
    /// Whether at least one upstream worker is ready (what `/ready` reports
    /// outside shutdown).
    submissions: bool,
    /// Whether the query upstream (`--redirect-queries`) is up by the
    /// poller's probes; always `false` without a redirect.
    queries: bool,
    /// Whether the gateway is shutting down; serialized only when true, like
    /// `/ready`'s.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    draining: bool,
}

/// The in-flight request-byte budget (`--max-inflight-request-bytes`): byte
/// reservations taken before a request body is read and held until the
/// request has been forwarded or rejected.
#[derive(Debug)]
struct RequestByteBudget {
    /// One permit per byte, or `None` when the budget is unlimited.
    permits: Option<Arc<Semaphore>>,
    /// The per-request body cap (`--max-request-bytes`), reserved whole for a
    /// body whose length is not declared up front (chunked).
    max_request_bytes: usize,
}

impl RequestByteBudget {
    fn new(max_inflight_request_bytes: Option<NonZeroU32>, max_request_bytes: usize) -> Self {
        // `MAX_PERMITS` is far above `u32::MAX` on 64-bit targets; the clamp
        // only matters where `usize` is narrower.
        let permits = max_inflight_request_bytes.map(|budget| {
            let budget = usize::try_from(budget.get()).unwrap_or(usize::MAX);
            Arc::new(Semaphore::new(budget.min(Semaphore::MAX_PERMITS)))
        });
        Self { permits, max_request_bytes }
    }

    /// Reserve what a request body may take, before any of it is read.
    ///
    /// `declared_len` is hyper's decoded `Content-Length`, or `None` for a
    /// chunked body, which reserves the whole per-request cap. A declared
    /// length over the cap is refused as too large outright. A reservation
    /// that does not fit in what is left of the budget is refused at once
    /// rather than queued: a queued request would hold its connection open
    /// while it waits. Returns `None` when the budget is unlimited.
    fn reserve(&self, declared_len: Option<u64>) -> Result<Option<HeldRequestBytes>, GatewayError> {
        let cap = u64::try_from(self.max_request_bytes).unwrap_or(u64::MAX);
        let reservation = match declared_len {
            Some(len) if len > cap => return Err(GatewayError::RequestTooLarge),
            Some(len) => len,
            None => cap,
        };
        let Some(permits) = &self.permits else { return Ok(None) };
        // a reservation past `u32::MAX` can never fit a budget that is a `u32`
        u32::try_from(reservation)
            .ok()
            .and_then(|bytes| {
                let permit = Arc::clone(permits).try_acquire_many_owned(bytes).ok()?;
                Some(HeldRequestBytes {
                    _permit: permit,
                    _gauge: telemetry::RequestBytesHeld::enter(bytes),
                })
            })
            .map(Some)
            .ok_or(GatewayError::InflightBytesExhausted)
    }
}

/// The bytes one in-flight request holds against the budget; dropping it
/// returns them.
#[derive(Debug)]
struct HeldRequestBytes {
    /// Keeps the held-bytes gauge in step with the permits; declared first so
    /// it is lowered before the permits are returned, and the gauge never reads
    /// above the bytes actually held.
    _gauge: telemetry::RequestBytesHeld,
    /// One permit per reserved byte.
    _permit: OwnedSemaphorePermit,
}

/// Open inbound connections per client prefix, enforcing
/// `--max-connections-per-ip` in the accept loop (which decides what a
/// connection's client address is, and which connections are not counted).
///
/// A key's entry is removed when its count returns to zero, so the map holds
/// at most one entry per open connection and never grows with the number of
/// distinct clients seen over time.
#[derive(Debug)]
struct ClientConnections {
    /// Most connections one client prefix may hold open at once.
    max_per_client: NonZeroUsize,
    /// How a client address is masked to its key.
    prefix: PrefixPolicy,
    /// Open connections per client key.
    open: Mutex<HashMap<IpAddr, usize>>,
}

impl ClientConnections {
    fn new(max_per_client: NonZeroUsize, prefix: PrefixPolicy) -> Self {
        Self { max_per_client, prefix, open: Mutex::new(HashMap::new()) }
    }

    /// Take one of `client`'s slots, or return its client key when that
    /// prefix already holds `max_per_client` connections.
    fn try_acquire(self: &Arc<Self>, client: IpAddr) -> Result<ClientSlot, IpAddr> {
        let key = self.prefix.key(client);
        let mut open = self.lock();
        let count = open.entry(key).or_insert(0);
        // The cap is at least one, so a refused key always has a live entry
        // and a refusal never leaves a zero count behind.
        if *count >= self.max_per_client.get() {
            return Err(key);
        }
        *count += 1;
        Ok(ClientSlot { connections: Arc::clone(self), key })
    }

    /// Return one of `key`'s slots, dropping its entry once none are left.
    fn release(&self, key: IpAddr) {
        let mut open = self.lock();
        if let Some(count) = open.get_mut(&key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                open.remove(&key);
            }
        }
    }

    /// Lock the map, recovering it if a previous holder panicked: neither
    /// critical section can leave a count half-updated.
    fn lock(&self) -> MutexGuard<'_, HashMap<IpAddr, usize>> {
        self.open.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One open connection's hold on a slot of its client prefix. Dropping it,
/// however the connection task ends, returns the slot.
#[derive(Debug)]
struct ClientSlot {
    /// The table the slot was taken from.
    connections: Arc<ClientConnections>,
    /// The client key the slot counts against.
    key: IpAddr,
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        self.connections.release(self.key);
    }
}

/// Build the gateway router: health/readiness routes plus the proxy fallback.
///
/// `request_deadline` bounds each whole request, and an overrun is answered
/// with the gateway's own JSON-RPC error envelope, a `408` before the request
/// was forwarded and a `504` after (see [`enforce_request_deadline`]); a `408`
/// from an upstream passes through this layer unchanged, relayed like any other
/// upstream error status. Streamed *response* bodies are written after the
/// handler returns, outside this deadline; they are bounded by the accept loop's
/// transport-stall deadline and connection-lifetime cap (the upstream
/// client's read timeout is only checked when the body is polled, which a
/// slow-reading client can prevent; see [`accept_loop`]). `max_request_bytes`
/// caps the buffered request body.
///
/// `max_inflight_request_bytes` budgets the body bytes held across all
/// in-flight requests (see [`reserve_request_bytes`]); `None` leaves the total
/// unbounded, though a declared `Content-Length` over `max_request_bytes` is
/// still refused before the body is read.
///
/// The probe paths answer `GET` and `HEAD`; any other method on them, `POST`
/// included, gets the same `405` envelope as a non-`POST` request anywhere
/// else, with axum's `Allow: GET,HEAD` in place of the proxy's `Allow: POST`.
/// A `POST` to a probe path is not proxied: the path belongs to the probe, and
/// its answer must not depend on the upstream.
///
/// `layers` carries the optional layers (see [`RouterLayers`]). When it holds
/// rate limiters, the rate-limit layer is installed inside up to four layers
/// and outside every other one, so an over-limit request is shed with a
/// JSON-RPC `429` before its body is buffered or forwarded, and before it
/// reserves any of the byte budget. Outside it are, innermost first, the CORS
/// layer when `layers` holds allowed origins, the access log ([`log_access`])
/// when it holds one, the cancellation counter ([`count_cancelled`]), the
/// identity middleware ([`identify`]), which resolves
/// the client address the rate limiter keys on, the access log names and the
/// proxy forwards, believing `X-Forwarded-For` only from a peer in
/// `trusted_proxies`, and, outermost, the `nosniff` layer ([`set_nosniff`]),
/// so every response the service produces carries it, that `429` included.
///
/// The access log sits outside every layer that can answer a request itself,
/// so a request answered there is logged like a proxied one: a `429`, a
/// byte-budget `413` or `503`, a `405`, the deadline's `408` or `504` and, with
/// CORS on, a preflight.
/// `/health` and `/ready` are not logged, whatever their method.
///
/// The CORS layer answers every `OPTIONS` itself as a preflight, so a
/// preflight is never forwarded, never refused by the proxy's `POST`-only
/// rule and spends no rate-limit token, and it adds
/// `Access-Control-Allow-Origin` for an allowed `Origin` to every other
/// response, a `429` included. Without allowed origins no CORS layer is
/// installed and no CORS header is ever added; an `OPTIONS` then gets the
/// same `405` as any other method but `POST`.
///
/// `/ready/any` is added after every layer but the `nosniff` one, the identity,
/// access-log and CORS layers included, so its `GET` and `HEAD` answer from
/// memory with none of them applied: they are never rate limited, metered,
/// logged, charged against the byte budget or deadline-bound, like `/health`
/// and `/ready`, and never carry a CORS header. Any other method there (an
/// `OPTIONS` included) passes the identity and rate-limit layers on its own
/// before it gets the probes' `405`, so, as on the other probe paths, it
/// spends the client's tokens and is refused with a `429` once they run out.
/// `/health` and `/ready` sit inside the CORS layer, which answers an
/// `OPTIONS` to them and adds `Access-Control-Allow-Origin` to their answers
/// for an allowed `Origin`.
pub(crate) fn router(
    state: AppState,
    request_deadline: Duration,
    max_request_bytes: usize,
    max_inflight_request_bytes: Option<NonZeroU32>,
    layers: RouterLayers,
    trusted_proxies: Arc<TrustedProxies>,
) -> Router {
    let byte_budget =
        Arc::new(RequestByteBudget::new(max_inflight_request_bytes, max_request_bytes));
    let router = Router::new()
        .route(HEALTH_PATH, get(liveness))
        .route(READY_PATH, get(readiness))
        .method_not_allowed_fallback(probe_method_not_allowed)
        .fallback(proxy)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(from_fn_with_state(request_deadline, enforce_request_deadline))
        // inside the rate limit, so a rate-limited request costs no budget, and
        // outside the body read and the request deadline, so a refused request
        // is answered without reading its body
        .layer(from_fn_with_state(byte_budget, reserve_request_bytes));
    // Add the rate-limit layer after the body, deadline and byte-budget layers
    // so it runs ahead of the reservation and the body read.
    let router = match &layers.rate_limiters {
        Some(limiters) => router.layer(from_fn_with_state(Arc::clone(limiters), rate_limit)),
        None => router,
    };
    // Add CORS after the rate limiter so it runs before it: a preflight is
    // answered here, ahead of the proxy's `POST`-only rule and without spending
    // a token, and a `429` still carries the CORS headers a browser needs to
    // let the page read it.
    let router = match layers.cors {
        Some(origins) => router.layer(cors_layer(origins)),
        None => router,
    };
    // Add the access log after CORS so it runs before it and logs every
    // answer the layers inside give (a preflight, a `429`, a `503`), and
    // before the identity layer so it names the resolved client.
    let router = match layers.access_log {
        Some(log) => router.layer(from_fn_with_state(log, log_access)),
        None => router,
    };
    // Add the cancellation counter after every layer that can answer a
    // request itself, so a request dropped anywhere inside counts as
    // cancelled while an answer from any of them (the deadline's included,
    // counted `rejected`) is not counted twice.
    let router = router.layer(from_fn(count_cancelled));
    // Add the identity layer after the access-log, CORS and rate-limit layers
    // so it runs before them: they read the client address this layer stores.
    let router = router.layer(from_fn_with_state(Arc::clone(&trusted_proxies), identify));
    // a layer wraps only the routes added before it, so this probe's `GET` and
    // `HEAD`, like those on `/health` and `/ready` (exempt in
    // `crate::ratelimit`), are never rate limited; it stays the last route
    // added, outside every layer but the `nosniff` one, and refuses other
    // methods like the two probes above, through the same rate limiter, which
    // needs the identity layer's client address
    let ready_any_route = match layers.rate_limiters {
        Some(limiters) => get(ready_any).fallback(
            probe_method_not_allowed
                .layer(from_fn_with_state(limiters, rate_limit))
                .layer(from_fn_with_state(trusted_proxies, identify)),
        ),
        None => get(ready_any).fallback(probe_method_not_allowed),
    };
    router
        .route(READY_ANY_PATH, ready_any_route)
        // Outermost, so every response the service produces carries it, the
        // identity, CORS and rate-limit layers' included.
        .layer(map_response(set_nosniff))
        .with_state(state)
}

/// Mark a response `X-Content-Type-Options: nosniff`, so a browser takes its
/// `Content-Type` as given and never sniffs a body served on the validator's
/// origin into HTML or script.
async fn set_nosniff(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

/// Axum middleware: count a request dropped before it was answered as
/// `outcome="cancelled"` in `tn_worker_gateway_requests_total`.
///
/// A request's future is dropped mid-flight when its connection closes: a
/// client disconnect, the connection-lifetime cap or shutdown. Every answer
/// records its own outcome where it is built, so only this drop needs a
/// guard, and the guard is disarmed once any response comes back. The probes
/// `/health` and `/ready` are skipped (their `GET` and `HEAD` record no request
/// metric; another method's `405` or `429` records its own `rejected` outcome
/// where it is built); `/ready/any` is added after this layer and never
/// reaches it.
async fn count_cancelled(request: Request, next: Next) -> Response {
    let path = request.uri().path();
    if path == HEALTH_PATH || path == READY_PATH {
        return next.run(request).await;
    }
    let mut guard = OutcomeGuard { answered: false };
    let response = next.run(request).await;
    guard.answered = true;
    response
}

/// Drop guard behind [`count_cancelled`]: records the `cancelled` outcome
/// unless the request it covers was answered.
struct OutcomeGuard {
    /// Set once the inner service returned a response, whose outcome the
    /// code that built it recorded.
    answered: bool,
}

impl Drop for OutcomeGuard {
    fn drop(&mut self) {
        if !self.answered {
            telemetry::record_cancelled();
        }
    }
}

/// Answer a probe path requested with a method other than `GET` or `HEAD`.
/// axum adds `Allow: GET,HEAD` to the response, and the message names the
/// same methods.
async fn probe_method_not_allowed(method: Method) -> Response {
    debug!(target: "gateway::server", %method, "rejecting non-GET request to a probe path");
    error_response(&GatewayError::ProbeNonGetMethod, b"")
}

/// Build the CORS layer for `origins`: a JSON-RPC client only ever needs
/// `POST` with a `Content-Type` header, and a browser may cache the preflight
/// answer for [`CORS_MAX_AGE`]. `Retry-After` is exposed so a page can read
/// how long a capacity `503` asks it to wait; a browser hides every response
/// header outside the CORS-safelisted few unless it is exposed. Origins are
/// matched exactly, byte for byte, against the request's `Origin`.
fn cors_layer(origins: CorsOrigins) -> CorsLayer {
    let allow_origin = match origins {
        CorsOrigins::Any => AllowOrigin::any(),
        // `AllowOrigin::list` panics on a `*` entry; `CorsOrigins::List` never
        // holds one (see `cli::resolve_cors_origins`)
        CorsOrigins::List(origins) => AllowOrigin::list(origins),
    };
    CorsLayer::new()
        .allow_origin(allow_origin)
        .allow_methods([Method::POST])
        .allow_headers([header::CONTENT_TYPE])
        .expose_headers([header::RETRY_AFTER])
        .max_age(CORS_MAX_AGE)
}

/// Axum middleware: write one `gateway::access` line for each request the
/// [`AccessLog`] samples. The probes `/health` and `/ready` are neither
/// counted nor logged (`/ready/any` and the probe listener never reach it).
///
/// An [`AccessEntry`] guard writes the line exactly once: when the response
/// head is ready, or, when the request's future is dropped before it has a
/// response (the client closed the connection while the request was being
/// forwarded, the connection lifetime cap, shutdown), with status
/// [`CLIENT_CLOSED_REQUEST`].
///
/// The line's fields are `client` (the resolved client's [`ClientAddr`]
/// masked to its network prefix, as the rate limiter keys it, so behind a
/// trusted front it names the forwarded client, or the front itself when the
/// front forwarded no usable client: no, an empty or an unparsable
/// `X-Forwarded-For`, a chain of only trusted addresses, or a PROXY `LOCAL`
/// connection), `class`
/// and `route` (from the [`AccessRecord`] the proxy keeps in the request's
/// [`AccessSampled`] slot, or `unknown` and `rejected` for a request answered
/// or dropped before the proxy read its body: a rate-limit `429`, a byte
/// budget refusal, a `405`, a request timeout before the forward started),
/// `status`, `latency_ms` (until the response head is ready or the request is
/// dropped; a streamed body is still being written) and `bytes_in` (the
/// buffered body's length, or the declared `Content-Length` when the body was
/// never read). It carries no URL, header value, body, params or method name.
async fn log_access(
    State(log): State<Arc<AccessLog>>,
    mut request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path == HEALTH_PATH || path == READY_PATH || !log.sample() {
        return next.run(request).await;
    }
    let started = Instant::now();
    let client = request.extensions().get::<ClientAddr>().map_or_else(
        || String::from("unknown"),
        |ClientAddr(client)| log.prefix.key(*client).to_string(),
    );
    let declared_bytes = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let record = Arc::new(AccessRecord::default());
    request.extensions_mut().insert(AccessSampled(Arc::clone(&record)));
    // the guard writes the line as it drops: on return, once the response head
    // is ready, or with this future if the request is cancelled first
    let mut entry = AccessEntry { client, started, declared_bytes, record, status: None };
    let response = next.run(request).await;
    entry.status = Some(response.status().as_u16());
    response
}

/// Bound a whole request by `deadline`, answering an overrun with the
/// gateway's JSON-RPC error envelope: a `408` while the request has not been
/// forwarded, and a `504` once it has. A `408` says the request never fully
/// arrived, and client stacks such as OkHttp and Chromium resend one on their
/// own, so a request that may have reached the upstream is never answered
/// with it.
///
/// Only an overrun of this deadline produces the envelope; the response of a
/// request that finishes in time, an upstream's own `408` included, passes
/// through this layer unchanged. The request `id` is not recovered (the body may never
/// have finished arriving), so it echoes as `null`, per spec. The message
/// says whether the request may have reached an upstream, from the
/// [`ForwardingStarted`] marker the proxy sets just before it forwards.
///
/// A request whose upstream has already answered when the deadline passes is
/// not cut: the proxy is only passing that answer on, and cutting it would
/// report an answer the upstream already gave, a transaction it accepted
/// included, as a timeout. A streamed answer leaves this layer once its head
/// is in. The one answer still read here, a single submission the gateway
/// remembers (see `relay_submission_answer`), must be read within
/// `--upstream-request-timeout` of the request being sent, so the handler can
/// run up to that long past this deadline. It finishes and the proxy counts
/// it, once.
async fn enforce_request_deadline(
    State(deadline): State<Duration>,
    mut request: Request,
    next: Next,
) -> Response {
    let forwarding = ForwardingStarted::default();
    request.extensions_mut().insert(forwarding.clone());
    let mut response = pin!(next.run(request));
    match tokio::time::timeout(deadline, response.as_mut()).await {
        Ok(response) => response,
        Err(_elapsed) if forwarding.is_answered() => response.await,
        Err(_elapsed) => error_response(
            &GatewayError::RequestTimeout { forwarding_started: forwarding.is_marked() },
            b"",
        ),
    }
}

/// Reserve a request's body bytes against the in-flight budget before the body
/// is read, and hold them until the inner service has answered.
///
/// A refused request is answered from its head alone, without reading its
/// body: a declared `Content-Length` over the per-request cap gets the "request
/// too large" error, and a reservation the budget cannot cover gets the
/// "in-flight bytes exhausted" error at once instead of waiting. Neither can
/// recover the request `id`, which sits in the unread body, so it echoes as
/// `null`. The reservation is held across the inner call, which returns once
/// the proxy has the upstream's response head, or, for a single submission,
/// its whole answer; the buffered body is dropped by then. It cannot ride in
/// the request extensions instead: the proxy drops the request's parts before
/// it reads the body. Requests to `/health` and `/ready` are exempt whatever
/// their method (their `GET`/`HEAD` hold no body, and any other method is
/// refused with a `405` without reading one), and so is a request that is not
/// a `POST`: it is refused with the `405` before its body is read (see
/// [`crate::proxy::PostOnly`]), so it holds no bytes, and its `405` is not
/// masked by a `413` or a `503`.
async fn reserve_request_bytes(
    State(budget): State<Arc<RequestByteBudget>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if request.method() != Method::POST || path == HEALTH_PATH || path == READY_PATH {
        return next.run(request).await;
    }
    let held = match budget.reserve(request.body().size_hint().exact()) {
        Ok(held) => held,
        Err(err) => {
            if matches!(err, GatewayError::RequestTooLarge) {
                // the same event as the body limit tripping in the proxy, so it
                // shares that warning's throttle
                if let Some(suppressed) = OVERSIZED_BODY_WARNINGS.admit() {
                    warn!(
                        target: "gateway::server",
                        suppressed,
                        "rejecting oversized request body before reading it"
                    );
                }
            } else {
                debug!(
                    target: "gateway::server",
                    "in-flight request byte budget exhausted; refusing request"
                );
            }
            return error_response(&err, b"");
        }
    };
    let response = next.run(request).await;
    drop(held);
    response
}

/// Liveness probe: always `200 OK` while the process is running.
async fn liveness() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness probe: `200` when at least one upstream worker is ready, else
/// `503`. Once shutdown begins it is `503` with `"draining": true`, whatever
/// the workers report, so fronts that probe it stop routing here during
/// `--shutdown-delay` while the listener still serves.
///
/// It means "this gateway can take submissions". With `--redirect-queries`
/// set, reads other than the few kept on the worker keep working while it
/// reports `503`, and the query upstream's state does not change it;
/// [`ready_any`] reports that.
async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let draining = state.draining.load(Ordering::Relaxed);
    let ready = !draining && state.readiness.any_ready();
    let status = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(ReadyBody { ready, draining }))
}

/// Read-availability probe: `200` when the gateway can serve any route, that
/// is when `/ready` would, or when `--redirect-queries` is set and the query
/// upstream is up by the poller's probes (see
/// [`GatewayReadiness::query_ready`]); else `503`. Without a redirect it is
/// exactly `/ready`. Once shutdown begins it is `503` with `"draining": true`,
/// like `/ready`, whatever the upstreams report.
///
/// Every gateway in front of a validator polls the same worker, so a front
/// that drops gateways on `/ready` drops all of them during a worker outage,
/// and the reads they could still serve go down too. This probe keeps them in
/// rotation while the query upstream answers.
async fn ready_any(State(state): State<AppState>) -> impl IntoResponse {
    let draining = state.draining.load(Ordering::Relaxed);
    let ready = !draining && state.readiness.any_route_ready();
    let status = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    let submissions = state.readiness.any_ready();
    let queries = state.readiness.query_ready();
    (status, Json(ReadyAnyBody { ready, submissions, queries, draining }))
}

/// Bind `listen_addr` and serve until `shutdown` fires. Then report draining
/// on `/ready` and `/ready/any` while still accepting and serving for
/// `limits.shutdown_delay`, stop accepting, and drain in-flight requests until
/// they finish or `graceful_timeout` elapses, whichever comes first.
pub(crate) async fn serve(
    listen_addr: SocketAddr,
    state: AppState,
    limits: ServerLimits,
    layers: RouterLayers,
    trusted_proxies: Arc<TrustedProxies>,
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "gateway::server", %local_addr, "worker gateway listening");

    let draining = Arc::clone(&state.draining);
    let app = router(
        state,
        limits.request_deadline,
        limits.max_request_bytes,
        limits.max_inflight_request_bytes,
        layers,
        Arc::clone(&trusted_proxies),
    );
    accept_loop(listener, app, limits, trusted_proxies, draining, graceful_timeout, shutdown).await
}

/// Accept connections until `shutdown` fires, serving each on its own task
/// with the configured header deadline, `TCP_NODELAY`, transport-stall
/// deadline, lifetime cap, and global and per-client connection caps. On the
/// notice, set `draining` (the flag `/ready` and `/ready/any` report) and keep
/// accepting and serving for `limits.shutdown_delay`; then close the listener
/// and drain within `graceful_timeout`.
///
/// The two write-path guards close the response-side slow loris: the
/// whole-request deadline stops covering a response once its head is produced,
/// and the upstream client's total timeout is only observed when the streamed
/// body is polled; under downstream backpressure hyper stops polling the body
/// (its write loop parks on a full write buffer), so that timeout never fires.
/// `TCP_USER_TIMEOUT` fires in the kernel when the peer stops acknowledging
/// written data outright, and the connection-lifetime cap is a runtime timer
/// polled independent of connection progress, so it fires even against a
/// client trickling one byte per interval to keep the transport alive.
///
/// With `--proxy-protocol`, a connection from a `--proxy-protocol-from` peer
/// is first read for its PROXY protocol v2 header, on the connection's own task (a slow
/// front never stalls `accept()`) and within the header read timeout; the
/// source address it names becomes the connection's `ConnectInfo`. A `LOCAL`
/// header keeps the front's address and marks the connection
/// ([`LocalConnection`]) so its forwarding headers are ignored. A missing, late
/// or malformed header closes the connection.
///
/// The per-client cap counts a connection against its resolved client, the
/// address the identity middleware starts from: the PROXY header's source, or
/// else the peer. It is taken once that address is known, so behind an L4
/// front each client counts on its own. A connection whose resolved address is
/// in `trusted_proxies` is not counted: `X-Forwarded-For` attributes its
/// requests to clients one at a time, and counting the front would put every
/// client behind it under one cap.
///
/// Neither the cap nor shutdown cuts a request off mid-exchange: either one
/// asks the connection to take no further requests and close once the
/// exchange in flight (if any) finishes, and only drops it if that takes
/// longer than a grace of `request_deadline` after the cap or
/// `graceful_timeout` at shutdown.
async fn accept_loop(
    listener: TcpListener,
    app: Router,
    limits: ServerLimits,
    trusted_proxies: Arc<TrustedProxies>,
    draining: Arc<AtomicBool>,
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    // hyper's header read timeout only arms when a timer is installed; without
    // one the timeout is silently disabled (the exact `axum::serve` gap this
    // loop exists to close).
    let mut connection_builder = hyper::server::conn::http1::Builder::new();
    connection_builder.timer(TokioTimer::new()).header_read_timeout(limits.header_read_timeout);
    // hyper's read buffer grows up to this cap and stays allocated while the
    // connection lives, so a connection holding a buffered body costs the body
    // plus this buffer; at hyper's default (about 408 KiB) a held 1 MiB body
    // cost about 1.42 MiB. hyper panics below its minimum, which startup
    // validation enforces; the clamp keeps that panic unreachable.
    connection_builder.max_buf_size(limits.http1_max_buf_size.max(HTTP1_MIN_BUF_SIZE));

    // Shutdown fan-out: every connection task holds a receiver, watches it
    // for the drain signal, and drops it when its connection ends, so the
    // sender's `closed()` resolves once every connection has finished.
    let (drain, _) = watch::channel(());
    let request_deadline = limits.request_deadline;
    let limiter =
        Arc::new(Semaphore::new(limits.max_connections.get().min(Semaphore::MAX_PERMITS)));
    let client_connections = limits
        .max_connections_per_ip
        .map(|max| Arc::new(ClientConnections::new(max, limits.client_prefix)));

    // Draining is reported from the notice on, and the listener closes once
    // the shutdown delay has run out (see [`until_listener_closes`]).
    let stop_accepting =
        until_listener_closes(&shutdown, &draining, limits.shutdown_delay, "client");
    tokio::pin!(stop_accepting);

    // Both selects are `biased` toward `stop_accepting`, so the notice is seen
    // (and draining set) before another connection is accepted.
    loop {
        // Backpressure: once `max_connections` are open, leave new connections
        // in the OS accept backlog instead of accepting without bound.
        let permit = tokio::select! {
            biased;
            () = &mut stop_accepting => break,
            permit = Arc::clone(&limiter).acquire_owned() => permit,
        };
        // The semaphore is never closed, so acquisition cannot fail; bail out
        // defensively rather than panic if that invariant ever changes.
        let Ok(permit) = permit else { break };

        let accepted = tokio::select! {
            biased;
            () = &mut stop_accepting => break,
            accepted = listener.accept() => accepted,
        };
        let (mut stream, peer_addr) = match accepted {
            Ok(accepted) => accepted,
            // the next connection is unaffected, so accept it at once; the
            // permit is released and taken again
            Err(err) if is_connection_error(&err) => {
                debug!(target: "gateway::server", %err, "accept: connection error, retrying");
                continue;
            }
            Err(err) => {
                if let Some(suppressed) = ACCEPT_ERROR_WARNINGS.admit() {
                    warn!(target: "gateway::server", %err, suppressed, "failed to accept connection");
                }
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        // counted from here, so a failed accept never shows as an open
        // connection
        let connection_slot = telemetry::ConnectionSlot::open(permit);

        // Nagle + delayed-ACK can add ~40ms to small JSON-RPC responses; the
        // upstream hop (reqwest) already disables it. Best-effort: a failure
        // only costs latency.
        if let Err(err) = stream.set_nodelay(true) {
            debug!(target: "gateway::server", %err, "failed to set TCP_NODELAY");
        }

        // Best-effort: on an unsupported platform (or a setsockopt failure)
        // the lifetime cap below still bounds a stalled reader.
        if let Some(Err(err)) =
            limits.tcp_user_timeout.map(|timeout| set_tcp_user_timeout(&stream, timeout))
        {
            debug!(target: "gateway::server", %err, "failed to set TCP_USER_TIMEOUT");
        }

        // Only a `--proxy-protocol-from` peer's connection opens with a PROXY
        // header; every other connection is plain HTTP.
        let proxy_header_deadline = limits
            .proxy_protocol
            .as_ref()
            .filter(|senders| senders.contains(peer_addr.ip()))
            .map(|_| limits.header_read_timeout);
        let app = app.clone();
        let connection_builder = connection_builder.clone();
        let client_connections = client_connections.clone();
        let trusted_proxies = Arc::clone(&trusted_proxies);
        // The lifetime cap is a runtime timer, deliberately NOT a timeout on
        // any body future: the runtime polls it regardless of whether hyper's
        // backpressured write path ever polls the connection forward again.
        // Constructed here (not inside the task) so the deadline counts from
        // accept even if the spawned task's first poll is delayed. `None` caps
        // nothing (a never-ready future).
        let lifetime_cap = limits.max_connection_duration.map_or_else(
            || Either::Left(future::pending::<()>()),
            |cap| Either::Right(tokio::time::sleep(cap)),
        );
        // Taken at accept, so a drain that starts (after the shutdown delay)
        // while a PROXY header is still arriving waits for this connection
        // too. The drain does not cut the header read, which the header read
        // timeout and the lifetime cap already bound; once the connection is
        // built it sees the signal at once and closes after the exchange in
        // flight. The future below owns the receiver and drops it when the
        // connection ends.
        let mut drain_signal = drain.subscribe();
        let connection = async move {
            let mut lifetime_cap = pin!(lifetime_cap);
            let (client_addr, local) = match proxy_header_deadline {
                Some(deadline) => {
                    let header = tokio::select! {
                        biased;
                        header = proxy_header(&mut stream, peer_addr, deadline) => header,
                        () = lifetime_cap.as_mut() => {
                            debug!(
                                target: "gateway::server",
                                %peer_addr,
                                "connection exceeded max lifetime; closing"
                            );
                            None
                        }
                    };
                    match header {
                        Some(ProxyHeader::Proxy(source)) => (source, false),
                        // the front's own connection: no client to name
                        Some(ProxyHeader::Local) => (peer_addr, true),
                        None => return,
                    }
                }
                None => (peer_addr, false),
            };
            // Per-client cap, keyed on the resolved client and taken before
            // the connection is served: a client prefix already at its cap
            // has this connection closed at once, and the global permit goes
            // back when the task ends, so one client cannot take every slot.
            // A connection-level refusal cannot answer JSON-RPC, so the close
            // carries no response. A trusted proxy's connection is not counted
            // (see above). The slot lives as long as this future, which the
            // task drops before it returns the global permit, so the
            // connection that permit lets in never finds this one still
            // counted against its prefix.
            let _client_slot = match client_connections
                .as_ref()
                .filter(|_| !trusted_proxies.contains(client_addr.ip()))
                .map(|connections| connections.try_acquire(client_addr.ip()))
                .transpose()
            {
                Ok(slot) => slot,
                Err(key) => {
                    telemetry::record_connection_refused(REFUSED_PER_IP_CAP);
                    debug!(
                        target: "gateway::server",
                        %peer_addr,
                        %client_addr,
                        %key,
                        "client prefix at its connection cap; closing"
                    );
                    return;
                }
            };
            // Hand the identity middleware the client's address
            // (`ConnectInfo`): the peer, or the source its PROXY header named.
            // It resolves the client the rate limiter and the proxy use, and
            // believes no forwarding header on a `LOCAL` connection.
            let app = app.layer(Extension(ConnectInfo(client_addr)));
            let app = if local { app.layer(Extension(LocalConnection)) } else { app };
            let service = TowerToHyperService::new(app);
            let mut connection =
                pin!(connection_builder.serve_connection(TokioIo::new(stream), service));
            // `biased` so a connection that finishes in the same poll as the
            // cap expires is reported as what it was (completion or its real
            // error), never mislabeled as cap-killed.
            let grace = tokio::select! {
                biased;
                result = connection.as_mut() => {
                    if let Err(err) = result {
                        debug!(target: "gateway::server", %err, "connection error");
                    }
                    None
                }
                () = lifetime_cap => {
                    debug!(
                        target: "gateway::server",
                        %peer_addr,
                        %client_addr,
                        "connection exceeded max lifetime; closing"
                    );
                    Some(request_deadline)
                }
                // an error means the accept loop has returned, which it only
                // does after shutdown, so both outcomes mean "drain"
                _ = drain_signal.changed() => Some(graceful_timeout),
            };
            // Take no further requests: hyper closes an idle connection at
            // once and lets the exchange in flight finish first. The grace
            // bounds that exchange, so a client that stops reading cannot
            // hold the connection past it.
            if let Some(grace) = grace {
                connection.as_mut().graceful_shutdown();
                match tokio::time::timeout(grace, connection).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => debug!(target: "gateway::server", %err, "connection error"),
                    Err(_elapsed) => debug!(
                        target: "gateway::server",
                        %peer_addr,
                        ?grace,
                        "connection did not finish within its grace; closing"
                    ),
                }
            }
        };
        tokio::spawn(async move {
            // the connection future drops its client slot and its drain
            // receiver as it ends, before the global permit goes back
            connection.await;
            drop(connection_slot);
        });
    }

    // Stop accepting (drop the listener), ask every connection to finish its
    // current exchange and close, then wait until they all have or the
    // graceful deadline elapses.
    drop(listener);
    info!(target: "gateway::server", "shutdown signal received; draining in-flight requests");
    drain.send_replace(());
    tokio::select! {
        () = drain.closed() => {
            info!(target: "gateway::server", "in-flight requests drained");
        }
        () = tokio::time::sleep(graceful_timeout) => {
            warn!(
                target: "gateway::server",
                timeout = ?graceful_timeout,
                "graceful shutdown deadline exceeded; forcing close"
            );
        }
    }
    Ok(())
}

/// Resolve when a listener should stop accepting: `shutdown_delay` after the
/// shutdown notice. `draining` is set at the notice, so `/ready` and
/// `/ready/any` answer `503` from then on, on either listener, while new
/// connections are still accepted and served: fronts that probe them (DNS
/// checks, external load balancers) stop routing here before the listener
/// closes. Both accept loops race their `accept` against this, `biased` toward
/// it, so the notice is seen (and draining set) before another connection is
/// accepted, and none is accepted once it resolves.
async fn until_listener_closes(
    shutdown: &Noticer,
    draining: &AtomicBool,
    shutdown_delay: Duration,
    listener: &'static str,
) {
    shutdown.await;
    draining.store(true, Ordering::Relaxed);
    if !shutdown_delay.is_zero() {
        info!(
            target: "gateway::server",
            listener,
            delay = ?shutdown_delay,
            "shutdown signal received; reporting draining before closing the listener"
        );
        tokio::time::sleep(shutdown_delay).await;
    }
}

/// Read the PROXY protocol v2 header a trusted front opens `stream` with,
/// within `deadline`. `None` means the connection is to be closed without a
/// response: the header was missing, late or malformed, which is counted and
/// logged as a (throttled) warning naming the peer, or the peer went away
/// before sending a byte (a TCP connect health check, say), which is not.
async fn proxy_header(
    stream: &mut TcpStream,
    peer_addr: SocketAddr,
    deadline: Duration,
) -> Option<ProxyHeader> {
    let read = async {
        if matches!(stream.peek(&mut [0_u8; 1]).await, Ok(0) | Err(_)) {
            return Ok(None);
        }
        read_proxy_header(stream).await.map(Some)
    };
    let reason = match tokio::time::timeout(deadline, read).await {
        Ok(Ok(Some(header))) => return Some(header),
        Ok(Ok(None)) => {
            debug!(
                target: "gateway::server",
                %peer_addr,
                "connection closed before its PROXY protocol header"
            );
            return None;
        }
        Ok(Err(err)) => err.to_string(),
        Err(_elapsed) => String::from("not complete within the header read timeout"),
    };
    telemetry::record_connection_refused(REFUSED_PROXY_HEADER);
    if let Some(suppressed) = PROXY_HEADER_WARNINGS.admit() {
        warn!(
            target: "gateway::server",
            %peer_addr,
            %reason,
            suppressed,
            "missing, malformed or late PROXY protocol header from a peer that must send one; \
             closing connection"
        );
    }
    None
}

/// Build the probe-only router served on `--probe-addr`: the health, readiness
/// and read-availability routes and nothing else (any other path is a `404`,
/// another method on a probe path a `405`), with no rate limit, body limit or
/// request deadline, since the handlers answer from memory. Every response
/// carries `X-Content-Type-Options: nosniff`, as on the client listener.
fn probe_router(state: AppState) -> Router {
    Router::new()
        .route(HEALTH_PATH, get(liveness))
        .route(READY_PATH, get(readiness))
        .route(READY_ANY_PATH, get(ready_any))
        .layer(map_response(set_nosniff))
        .with_state(state)
}

/// Bind `probe_addr` and serve the probe-only router until `shutdown` fires
/// and `shutdown_delay` has run out (see [`probe_accept_loop`]).
pub(crate) async fn serve_probes(
    probe_addr: SocketAddr,
    state: AppState,
    header_read_timeout: Duration,
    shutdown_delay: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(probe_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "gateway::server", %local_addr, "probe listener listening");
    let draining = Arc::clone(&state.draining);
    probe_accept_loop(
        listener,
        probe_router(state),
        header_read_timeout,
        draining,
        shutdown_delay,
        shutdown,
    )
    .await
}

/// Accept probe connections until `shutdown` fires and `shutdown_delay` has
/// run out, serving each on its own task, then stop accepting and let
/// in-flight probes finish.
///
/// The shutdown delay applies here as on the client listener: on the notice
/// `draining` is set and this listener keeps answering, so the probes that
/// target it see `/ready` and `/ready/any` turn `503` before either listener
/// closes; the two close together.
///
/// There is deliberately no connection cap: a cap is the thing a flood
/// exhausts, and this listener exists so the probes still answer when the
/// client listener's cap is full. Each connection must therefore be cheap and
/// short-lived instead: it serves one request (keep-alive off), and its
/// request head may be at most 8 KiB (hyper's smallest buffer; a larger head
/// gets a `431`). A probe needs neither more. The header deadline bounds every
/// connection that never completes a request. No connection here opens with a
/// PROXY protocol header: `--proxy-protocol` applies to the client listener
/// only, and nothing on this one keys on the client's address.
async fn probe_accept_loop(
    listener: TcpListener,
    app: Router,
    header_read_timeout: Duration,
    draining: Arc<AtomicBool>,
    shutdown_delay: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let mut connection_builder = hyper::server::conn::http1::Builder::new();
    connection_builder
        .timer(TokioTimer::new())
        .header_read_timeout(header_read_timeout)
        .keep_alive(false)
        .max_buf_size(8 * 1024);
    let graceful = GracefulShutdown::new();
    let stop_accepting = until_listener_closes(&shutdown, &draining, shutdown_delay, "probe");
    tokio::pin!(stop_accepting);

    loop {
        let accepted = tokio::select! {
            biased;
            () = &mut stop_accepting => break,
            accepted = listener.accept() => accepted,
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(err) if is_connection_error(&err) => {
                debug!(target: "gateway::server", %err, "probe accept: connection error, retrying");
                continue;
            }
            Err(err) => {
                if let Some(suppressed) = PROBE_ACCEPT_ERROR_WARNINGS.admit() {
                    warn!(
                        target: "gateway::server",
                        %err,
                        suppressed,
                        "failed to accept probe connection"
                    );
                }
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        let connection = graceful.watch(
            connection_builder
                .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app.clone())),
        );
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                debug!(target: "gateway::server", %err, "probe connection error");
            }
        });
    }

    // each connection serves one probe, answered from memory, so only one
    // still sending its head (at most 8 KiB) can outlast its exchange, and
    // the header deadline closes that one anyway
    drop(listener);
    if tokio::time::timeout(header_read_timeout, graceful.shutdown()).await.is_err() {
        debug!(target: "gateway::server", "probe connections still open at shutdown");
    }
    Ok(())
}

/// Whether an `accept()` error is about the one connection being accepted
/// (its peer refused, aborted or reset it, or the network to the peer failed)
/// rather than about the listener or the process (fd exhaustion, out of
/// memory). Such an error says nothing about the next connection, so the
/// accept loops retry at once instead of backing off. The raw `errno` values
/// are Linux's and are only consulted there.
fn is_connection_error(err: &std::io::Error) -> bool {
    use std::io::ErrorKind;
    matches!(
        err.kind(),
        ErrorKind::ConnectionRefused
            | ErrorKind::ConnectionAborted
            | ErrorKind::ConnectionReset
            | ErrorKind::NetworkDown
            | ErrorKind::NetworkUnreachable
            | ErrorKind::HostUnreachable
    ) || (cfg!(any(target_os = "android", target_os = "linux"))
        && matches!(
            err.raw_os_error(),
            Some(ENONET | EPROTO | ENOPROTOOPT | EOPNOTSUPP | EHOSTDOWN)
        ))
}

/// Arm `TCP_USER_TIMEOUT` on an accepted connection: the kernel forcibly
/// closes the connection when transmitted data stays unacknowledged, or
/// buffered data stays untransmittable behind a closed receive window, longer
/// than `timeout`, freeing the slot a fully stalled reader would otherwise
/// hold.
#[cfg(any(target_os = "android", target_os = "linux"))]
fn set_tcp_user_timeout(stream: &TcpStream, timeout: Duration) -> std::io::Result<()> {
    socket2::SockRef::from(stream).set_tcp_user_timeout(Some(timeout))
}

/// `TCP_USER_TIMEOUT` is Linux-family only; elsewhere report it unsupported so
/// the caller's debug log tells the truth. Production gateways deploy on Linux
/// (see the crate's `Dockerfile`); on other hosts the connection-lifetime cap
/// still bounds a stalled reader.
#[cfg(not(any(target_os = "android", target_os = "linux")))]
fn set_tcp_user_timeout(_stream: &TcpStream, _timeout: Duration) -> std::io::Result<()> {
    Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "TCP_USER_TIMEOUT requires Linux"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app::{check_chain_ids, log_chain_id_check, ChainIdCheck, UpstreamClients},
        cli::{LogFormat, Settings, DEFAULT_DENIED_METHOD_PREFIXES},
        config::UpstreamWorker,
        dns::{self, lookup_fn, SystemLookup},
        identity::proxy_v2_header,
        proxy::{
            client_builder, proxy_client, UpstreamClientBuilder, DEFAULT_MAX_BATCH_LEN,
            MAX_REQUEST_BYTES, SUBMISSION_ANSWER_SLACK,
        },
        ratelimit::RateLimit,
        readiness::{run_poller, QueryProbe, ReadinessThresholds},
        telemetry::test_utils::{process_per_test, CapturedMetrics},
    };
    use axum::{
        body::Bytes,
        http::{header, HeaderMap},
        routing::{any, post},
    };
    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, KeyPair,
        KeyUsagePurpose,
    };
    use reqwest::redirect::Policy;
    use std::{
        collections::BTreeMap,
        fmt,
        io::Write as _,
        sync::atomic::{AtomicUsize, Ordering},
        time::Instant,
    };
    use tempfile::NamedTempFile;
    use tn_types::Notifier;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
        sync::Notify,
        task::JoinHandle,
    };
    use tokio_rustls::{
        rustls::{
            crypto::ring::default_provider, pki_types::PrivatePkcs8KeyDer,
            server::WebPkiClientVerifier, RootCertStore, ServerConfig,
        },
        TlsAcceptor,
    };
    use tracing::{
        field::{Field, Visit},
        subscriber::DefaultGuard,
        Event, Subscriber,
    };
    use tracing_subscriber::{
        layer::{Context, SubscriberExt as _},
        Layer,
    };

    /// Graceful drain timeout of the accept loops the helpers below spawn.
    const GRACEFUL_TIMEOUT: Duration = Duration::from_secs(1);

    /// Generous limits so only the behavior under test can trip.
    fn test_limits() -> ServerLimits {
        ServerLimits {
            header_read_timeout: Duration::from_secs(5),
            request_deadline: Duration::from_secs(5),
            max_connections: NonZeroUsize::new(64).expect("nonzero"),
            max_connections_per_ip: None,
            client_prefix: PrefixPolicy::default(),
            tcp_user_timeout: None,
            max_connection_duration: None,
            max_request_bytes: MAX_REQUEST_BYTES,
            proxy_protocol: None,
            max_inflight_request_bytes: None,
            http1_max_buf_size: HTTP1_MAX_BUF_SIZE,
            shutdown_delay: Duration::ZERO,
        }
    }

    fn test_state(upstreams: &[UpstreamWorker]) -> AppState {
        test_state_with_client(upstreams, Client::builder().build().expect("build client"))
    }

    /// The CLI's default readiness thresholds.
    fn test_thresholds() -> ReadinessThresholds {
        ReadinessThresholds { failure: nz(3), success: nz(2), rpc_failure: Some(nz(3)) }
    }

    fn test_state_with_client(upstreams: &[UpstreamWorker], client: Client) -> AppState {
        AppState {
            readiness: Arc::new(GatewayReadiness::new(upstreams, test_thresholds())),
            http: client,
            query_upstream: None,
            query_header: None,
            recent_submissions: None,
            denied_method_prefixes: default_denied(),
            max_batch_len: NonZeroUsize::new(DEFAULT_MAX_BATCH_LEN),
            query_http: None,
            upstream_request_timeout: Duration::from_secs(5),
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
            pool_read_slots: inflight_slots(0),
            draining: Arc::default(),
        }
    }

    /// The shipped `--denied-method-prefixes` default, as the CLI resolves it.
    fn default_denied() -> Arc<[String]> {
        DEFAULT_DENIED_METHOD_PREFIXES.split(',').map(String::from).collect()
    }

    /// Serve `app` through the real accept loop (header timeout, nodelay,
    /// connection cap, `ConnectInfo` injection) on an ephemeral port. The
    /// returned `Notifier` keeps the server alive for the test's duration.
    async fn spawn_with_limits(app: Router, limits: ServerLimits) -> (SocketAddr, Notifier) {
        spawn_trusting(app, limits, Arc::default()).await
    }

    /// [`spawn_with_limits`], with `trusted` as the accept loop's
    /// `--trusted-proxies` (whose connections the per-client cap does not
    /// count); `app` should trust the same ranges, as in [`serve`].
    async fn spawn_trusting(
        app: Router,
        limits: ServerLimits,
        trusted: Arc<TrustedProxies>,
    ) -> (SocketAddr, Notifier) {
        let (addr, shutdown, _accept) =
            spawn_accept_loop(app, limits, trusted, Arc::default()).await;
        (addr, shutdown)
    }

    /// [`spawn_with_limits`] with a shutdown delay, setting `draining` (pass
    /// the flag of the state `app` was built from) on the notice. Notifying
    /// the returned `Notifier` starts the shutdown; the returned handle
    /// resolves when the accept loop's drain ends.
    async fn spawn_draining(
        app: Router,
        limits: ServerLimits,
        draining: Arc<AtomicBool>,
        shutdown_delay: Duration,
    ) -> (SocketAddr, Notifier, JoinHandle<Result<(), TaskError>>) {
        let limits = ServerLimits { shutdown_delay, ..limits };
        spawn_accept_loop(app, limits, Arc::default(), draining).await
    }

    /// Run the real accept loop for `app` on an ephemeral port with `trusted`
    /// as its `--trusted-proxies` and `draining` as the flag it sets on the
    /// notice, returning the address, the shutdown `Notifier` and the loop's
    /// handle.
    async fn spawn_accept_loop(
        app: Router,
        limits: ServerLimits,
        trusted: Arc<TrustedProxies>,
        draining: Arc<AtomicBool>,
    ) -> (SocketAddr, Notifier, JoinHandle<Result<(), TaskError>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = Notifier::new();
        let accept = tokio::spawn(accept_loop(
            listener,
            app,
            limits,
            trusted,
            draining,
            GRACEFUL_TIMEOUT,
            shutdown.subscribe(),
        ));
        (addr, shutdown, accept)
    }

    async fn spawn(app: Router) -> (SocketAddr, Notifier) {
        spawn_with_limits(app, test_limits()).await
    }

    /// Serve the probe-only router for `state` through the real probe accept
    /// loop on a second ephemeral port, as `--probe-addr` does. The returned
    /// `Notifier` keeps the listener alive for the test's duration.
    async fn spawn_probes(state: AppState) -> (SocketAddr, Notifier) {
        let shutdown = Notifier::new();
        let addr = spawn_probes_until(state, Duration::ZERO, shutdown.subscribe()).await;
        (addr, shutdown)
    }

    /// [`spawn_probes`] with a shutdown delay, stopping on `shutdown` (which
    /// may also stop a client listener, as in `app::run`) and setting the
    /// state's `draining` flag on the notice.
    async fn spawn_probes_until(
        state: AppState,
        shutdown_delay: Duration,
        shutdown: Noticer,
    ) -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let draining = Arc::clone(&state.draining);
        tokio::spawn(probe_accept_loop(
            listener,
            probe_router(state),
            test_limits().header_read_timeout,
            draining,
            shutdown_delay,
            shutdown,
        ));
        addr
    }

    fn upstream(addr: SocketAddr) -> UpstreamWorker {
        UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse(&format!("http://{addr}/")).expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}/health/workers")).expect("ready url"),
        }
    }

    fn test_router(state: AppState) -> Router {
        router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES),
            RouterLayers::default(),
            Arc::default(),
        )
    }

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("nonzero")
    }

    #[tokio::test]
    async fn health_is_always_ok() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (addr, _shutdown) = spawn(test_router(state)).await;

        let response =
            Client::new().get(format!("http://{addr}/health")).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_reflects_upstream_state() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let readiness = Arc::clone(&state.readiness);
        let (addr, _shutdown) = spawn(test_router(state)).await;
        let client = Client::new();

        let not_ready = client.get(format!("http://{addr}/ready")).send().await.expect("send");
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

        readiness.set_ready(0, true);
        let ready = client.get(format!("http://{addr}/ready")).send().await.expect("send");
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn proxies_to_ready_upstream() {
        // Mock upstream worker: echoes a canned JSON-RPC result on POST.
        let mock = Router::new()
            .route("/", post(|| async { r#"{"jsonrpc":"2.0","result":"0x1","id":1}"# }));
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        // the gateway does not forward the path, so the mock's `/` route
        // answers a POST to any path
        for path in ["/", "/anything"] {
            let response = Client::new()
                .post(format!("http://{gateway_addr}{path}"))
                .header("content-type", "application/json")
                .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":1}"#)
                .send()
                .await
                .expect("send");

            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.text().await.expect("text"),
                r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#,
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn forwards_client_identity_and_hop_marker() {
        // Mock upstream that echoes the identity headers it received.
        let mock = Router::new().route(
            "/",
            post(|headers: HeaderMap| async move {
                let get = |name: &str| {
                    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
                };
                format!(
                    "{}|{}|{}",
                    get("x-forwarded-for"),
                    get("x-forwarded-proto"),
                    get("x-tn-gateway")
                )
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .body("{}")
            .send()
            .await
            .expect("send");
        assert_eq!(response.text().await.expect("text"), "127.0.0.1|http|1");
    }

    #[tokio::test]
    async fn rejects_with_jsonrpc_error_when_no_upstream_ready() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":42}"#)
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32000);
        assert_eq!(body["id"], 42);
    }

    #[tokio::test]
    async fn unreachable_upstream_maps_to_bad_gateway() {
        // Upstream marked ready but nothing listens on its rpc_url.
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":7}"#)
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32001);
        assert_eq!(body["id"], 7);
    }

    #[tokio::test]
    async fn slow_upstream_maps_to_gateway_timeout() {
        // Mock upstream that answers slower than the proxy client's deadline.
        let mock = Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;

        // the production client, whose read timeout also bounds the wait for
        // the response head
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let state = test_state_with_client(&[upstream(upstream_addr)], client);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":8}"#)
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32002);
        assert_eq!(body["id"], 8);
    }

    #[tokio::test]
    async fn looped_request_is_rejected() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .header("x-tn-gateway", "1")
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":5}"#)
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::LOOP_DETECTED);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32004);
        assert_eq!(body["id"], 5);
    }

    #[tokio::test]
    async fn screened_transaction_is_rejected_with_the_request_id() {
        // The screening reject answers from the id its own parse produced; end
        // to end, the client must still get its id echoed back. The upstream is
        // marked ready and points at a dead port, so a submission that reached
        // forwarding would surface as a 502 rather than this 400.
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        let (gateway_addr, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .body(
                r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":"tx-77"}"#,
            )
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32007);
        assert_eq!(body["id"], "tx-77");
    }

    #[tokio::test]
    async fn oversized_body_gets_jsonrpc_error() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        // A tiny configured body limit so a small request trips the size guard
        // through the real router path (`--max-request-bytes` is configurable).
        let (addr, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            8,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let response = Client::new()
            .post(format!("http://{addr}/"))
            .body("this body is definitely longer than eight bytes")
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
            Some(&HeaderValue::from_static("nosniff"))
        );
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32003);
    }

    #[tokio::test]
    async fn over_limit_request_gets_jsonrpc_429() {
        let mock = Router::new()
            .route("/", post(|| async { r#"{"jsonrpc":"2.0","result":"0x1","id":1}"# }));
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        // Global limit of one request with no burst headroom: the first request
        // passes, the second (same instant, no refill) is rejected with 429.
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;

        let client = Client::new();
        let first = client
            .post(format!("http://{addr}/"))
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":1}"#)
            .send()
            .await
            .expect("send");
        assert_eq!(first.status(), StatusCode::OK);

        let second = client
            .post(format!("http://{addr}/"))
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":2}"#)
            .send()
            .await
            .expect("send");
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        let body: serde_json::Value = second.json().await.expect("json");
        assert_eq!(body["error"]["code"], -32006);
    }

    #[tokio::test]
    async fn health_probe_bypasses_rate_limit() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        // A maximally strict global limit (burst 1). If probes were rate-limited,
        // the second `/health` hit would be 429; they must stay 200 so an
        // orchestrator does not kill the pod under load.
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;

        let client = Client::new();
        for _ in 0..5 {
            let response = client.get(format!("http://{addr}/health")).send().await.expect("send");
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn slow_headers_are_disconnected() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let limits =
            ServerLimits { header_read_timeout: Duration::from_millis(300), ..test_limits() };
        let (addr, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        // Slow-loris probe: send a partial request line, then stall. The server
        // must close the connection once the header deadline passes, rather
        // than hold it open indefinitely (the pre-fix behavior).
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(b"POST / HTTP/1.1\r\nHost: gateway\r\n").await.expect("write");
        let mut buf = [0_u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .expect("connection should be closed by the header read timeout");
        assert_eq!(read.expect("read"), 0, "expected EOF from the server");
    }

    #[tokio::test]
    async fn gateway_408_before_forwarding_says_not_delivered() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        // Short whole-request deadline; generous header timeout so only the
        // body trickle trips.
        let (addr, _shutdown) = spawn_with_limits(
            router(
                state,
                Duration::from_millis(300),
                MAX_REQUEST_BYTES,
                None,
                RouterLayers::default(),
                Arc::default(),
            ),
            test_limits(),
        )
        .await;

        // Complete headers, then stall mid-body below the size limit.
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(
                b"POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
                  Content-Length: 100\r\n\r\n{\"id\":",
            )
            .await
            .expect("write");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("request should be timed out by the request deadline")
            .expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 408"), "expected 408, got: {response}");
        let (head, body) = response.split_once("\r\n\r\n").expect("response head and body");
        assert!(
            head.to_ascii_lowercase().contains("\r\nx-content-type-options: nosniff"),
            "gateway 408 must carry nosniff: {head}"
        );
        let body: serde_json::Value = serde_json::from_str(body).expect("json error body");
        assert_eq!(body["error"]["code"], -32005);
        // the body never finished arriving, so nothing was forwarded and the
        // message must not hedge about delivery
        assert_eq!(
            body["error"]["message"],
            "request did not complete within the gateway's deadline"
        );
        assert_eq!(body["id"], serde_json::Value::Null);
    }

    /// The deadline fired after the request went upstream is a `504`, not a
    /// `408` a client stack would resend on its own; its code, `-32005`, tells
    /// it from the upstream timeout's `504` (`-32002`). The deadline before
    /// forwarding keeps its `408` (see
    /// `gateway_408_before_forwarding_says_not_delivered`).
    #[tokio::test]
    async fn deadline_after_forwarding_is_a_504() {
        // A worker slower than the whole-request deadline: the request reaches
        // it, then the deadline fires while the gateway waits for the answer.
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let mock = Router::new().route(
            "/",
            post(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;

        // the forwarding client has no timeout of its own, so only the
        // gateway's deadline can end the request
        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_millis(300),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 3)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        let body: serde_json::Value = serde_json::from_str(&text).expect("json error body");
        assert_eq!(body["error"]["code"], -32005);
        assert_eq!(
            body["error"]["message"],
            "request did not complete within the gateway's deadline; the request may have \
             reached the upstream"
        );
        assert_eq!(body["id"], serde_json::Value::Null);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the request reached the upstream");
    }

    #[tokio::test]
    async fn upstream_408_passes_through_unchanged() {
        // Only the gateway's own deadline produces its 408 envelope; a 408
        // from an upstream is relayed like any other upstream status.
        const UPSTREAM_BODY: &str =
            r#"{"jsonrpc":"2.0","error":{"code":-1,"message":"upstream timeout"},"id":1}"#;
        let mock = Router::new().route(
            "/",
            post(|| async {
                (
                    StatusCode::REQUEST_TIMEOUT,
                    [(header::CONTENT_TYPE, "application/json")],
                    UPSTREAM_BODY,
                )
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::REQUEST_TIMEOUT, UPSTREAM_BODY));
    }

    #[tokio::test]
    async fn connection_cap_releases_permits() {
        let mock = Router::new()
            .route("/", post(|| async { r#"{"jsonrpc":"2.0","result":"0x1","id":1}"# }));
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(1).expect("nonzero"),
            ..test_limits()
        };
        let (gateway_addr, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        // Two sequential requests over connections that close after each
        // response: the second only succeeds if the first connection's permit
        // is released, so a permit leak would hang (and time out) this test.
        for id in [1, 2] {
            let response = Client::new()
                .post(format!("http://{gateway_addr}/"))
                .header("connection", "close")
                .body(format!(r#"{{"jsonrpc":"2.0","method":"eth_chainId","id":{id}}}"#))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    /// `tn_worker_gateway_open_connections` counts the client connections
    /// holding a `--max-connections` slot: it rises as connections are
    /// accepted and falls as each one closes.
    #[tokio::test]
    async fn open_connections_gauge_tracks_held_connections() {
        const OPEN: &str = "tn_worker_gateway_open_connections";
        let mut metrics = CapturedMetrics::install();
        let app = Router::new().route(HEALTH_PATH, get(|| async { "ok" }));
        // idle connections send nothing, so the header read timeout is what
        // would close them; keep it far past the test
        let limits = ServerLimits { header_read_timeout: Duration::from_secs(60), ..test_limits() };
        let (gateway, _shutdown) = spawn_with_limits(app, limits).await;

        let mut held = Vec::new();
        for _ in 0..3 {
            held.push(TcpStream::connect(gateway).await.expect("connect"));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.value(OPEN, &[]) != 3.0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("three accepted connections should read 3");

        drop(held.pop());
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.value(OPEN, &[]) != 2.0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a closed connection should give its slot back");
    }

    /// Upstream response body sized well above the sum of every buffer
    /// between the mock upstream and the stalled client (gateway channel,
    /// socket send/receive buffers, even generously tuned `tcp_wmem`/
    /// `tcp_rmem` sysctls), so a client that stops reading provably parks the
    /// stream mid-body.
    const STALL_BODY_BYTES: usize = 32 * 1024 * 1024;

    #[tokio::test]
    async fn slow_reader_is_closed_at_connection_lifetime_cap() {
        // The issue #958 scenario: response streaming to a client that stops
        // reading. The whole-request deadline no longer covers the response
        // body, and the upstream client's total timeout is only observed when
        // the body is polled (which the stall prevents), so only the lifetime
        // cap bounds this connection.
        let mock = Router::new().route("/", post(|| async { vec![0_u8; STALL_BODY_BYTES] }));
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        // The cap is generous enough that the response head always arrives
        // inside it, even on a loaded CI host where the whole 3-hop round
        // trip shares one test runtime. The exchange in flight gets the
        // limits' request deadline as its grace past the cap, kept short here
        // so the test can wait out both.
        let limits = ServerLimits {
            max_connection_duration: Some(Duration::from_secs(2)),
            request_deadline: Duration::from_millis(500),
            ..test_limits()
        };
        let (gateway_addr, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        let mut stream = TcpStream::connect(gateway_addr).await.expect("connect");
        stream
            .write_all(
                b"POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
                  Content-Length: 47\r\n\r\n{\"jsonrpc\":\"2.0\",\"method\":\"eth_getLogs\",\"id\":1}",
            )
            .await
            .expect("write");

        // Read one chunk (the response head plus some body), then stall past
        // the lifetime cap and its grace without reading further.
        let mut first = [0_u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut first))
            .await
            .expect("response head should arrive well inside the lifetime cap")
            .expect("first read");
        assert!(read > 0, "expected the response head to arrive");
        tokio::time::sleep(Duration::from_secs(3)).await;

        // Drain what the socket still holds. The grace ran out with the body
        // still stalled, so the connection was closed mid-body and the drain
        // must end (EOF or reset both count) well short of the full body;
        // without the cap the stream resumes here and delivers all of it.
        let mut rest = Vec::new();
        let drained = tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut rest))
            .await
            .expect("connection should be closed by the lifetime cap");
        drop(drained); // EOF is Ok, an RST is Err; either way `rest` holds what arrived.
        assert!(
            read + rest.len() < STALL_BODY_BYTES,
            "expected a truncated body, got all {} bytes",
            read + rest.len(),
        );
    }

    #[tokio::test]
    async fn lifetime_cap_lets_the_inflight_request_finish() {
        // A worker that answers 400ms after it is asked, and counts the asks.
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let mock = Router::new().route(
            "/",
            post(move || async move {
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(400)).await;
                r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;

        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);
        let limits = ServerLimits {
            max_connection_duration: Some(Duration::from_millis(300)),
            ..test_limits()
        };
        let (gateway, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        let body = call("eth_chainId", 1);
        let request = format!(
            "POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );

        // The request is written at once, well inside the 300ms cap, and its
        // answer comes at about 400ms, past it: the cap always fires while the
        // exchange is in flight (both deadlines share the runtime clock).
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        stream.write_all(request.as_bytes()).await.expect("write");

        // The exchange in flight finishes, then the connection closes.
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the connection should close once the exchange in flight finishes")
            .expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");
        assert!(response.contains(r#""result":"0x1""#), "expected the worker's answer: {response}");

        // The next request on that connection fails: nothing answers it and
        // it never reaches the worker.
        let written = stream.write_all(request.as_bytes()).await;
        let mut rest = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut rest))
            .await
            .expect("a closed connection answers at once");
        assert!(
            written.is_err() || read.is_err() || rest.is_empty(),
            "a second request was answered: {}",
            String::from_utf8_lossy(&rest)
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shutdown_drains_the_inflight_request() {
        let mock = Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#
            }),
        );
        let (upstream_addr, _mock) = spawn(mock).await;
        let state = test_state(&[upstream(upstream_addr)]);
        state.readiness.set_ready(0, true);

        // A drain deadline far past the test's bounds, so the accept loop can
        // only return in time by seeing every connection finish.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = Notifier::new();
        let server = tokio::spawn(accept_loop(
            listener,
            test_router(state),
            test_limits(),
            Arc::default(),
            Arc::default(),
            Duration::from_secs(30),
            shutdown.subscribe(),
        ));

        let request = tokio::spawn(
            Client::new().post(format!("http://{addr}/")).body(call("eth_chainId", 1)).send(),
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        shutdown.notify();

        // The request in flight when shutdown began still gets its answer.
        let response = request.await.expect("join").expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.text().await.expect("text"),
            r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#
        );

        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the drain should end as soon as the last connection closes")
            .expect("join")
            .expect("accept loop");
        assert!(TcpStream::connect(addr).await.is_err(), "the listener should be closed");
    }

    #[tokio::test]
    async fn lifetime_cap_closes_idle_connection_and_releases_permit() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        // One connection slot total, capped at 300ms; the header read deadline
        // (5s) stays out of the way of both asserts below.
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(1).expect("nonzero"),
            max_connection_duration: Some(Duration::from_millis(300)),
            ..test_limits()
        };
        let (addr, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        // A connection that never sends a byte must be closed by the lifetime
        // cap (well before the 5s header deadline, hence the 2s bound).
        let mut stream = TcpStream::connect(addr).await.expect("connect");

        // The probe waits in the accept backlog while the only slot is held.
        // Its request is written now, so its bytes are already buffered when
        // it is accepted and its own 300ms cap cannot close it before they
        // are read.
        let mut probe = TcpStream::connect(addr).await.expect("connect probe");
        probe
            .write_all(b"GET /health HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
            .await
            .expect("write probe");

        let mut buf = [0_u8; 16];
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("connection should be closed by the lifetime cap");
        assert_eq!(read.expect("read"), 0, "expected EOF from the server");

        // The capped connection's permit must be released: with a single slot,
        // the probe only gets accepted (and answered) if it was.
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), probe.read_to_end(&mut response))
            .await
            .expect("permit should be released after the cap fires")
            .expect("read probe");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");
    }

    /// The transport-stall guard actually arms the socket option (readable
    /// back via `SO_TCP_USER_TIMEOUT`). Linux-family only, like the option.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    #[tokio::test]
    async fn tcp_user_timeout_is_armed() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let (_client, accepted) =
            tokio::join!(async { TcpStream::connect(addr).await.expect("connect") }, async {
                listener.accept().await.expect("accept").0
            });
        set_tcp_user_timeout(&accepted, Duration::from_secs(7)).expect("set TCP_USER_TIMEOUT");
        let armed = socket2::SockRef::from(&accepted).tcp_user_timeout().expect("read back");
        assert_eq!(armed, Some(Duration::from_secs(7)));
    }

    /// What a mock upstream has received.
    #[derive(Clone, Debug, Default)]
    struct Seen {
        /// Requests received.
        hits: Arc<AtomicUsize>,
        /// `X-TN-Gateway` values received: one per request that carries the
        /// marker once, so a duplicated marker shows.
        hop_marker: Arc<AtomicUsize>,
        /// `X-TN-Gateway-Redirect` values received, counted the same way.
        redirect_marker: Arc<AtomicUsize>,
        /// Requests carrying the client's identity (`X-Forwarded-For`,
        /// `X-Forwarded-Proto`) and the gateway's user agent.
        identified: Arc<AtomicUsize>,
        /// Every [`API_KEY_HEADER`] value received, in order.
        api_keys: Arc<Mutex<Vec<String>>>,
    }

    impl Seen {
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }

        fn hop_marker(&self) -> usize {
            self.hop_marker.load(Ordering::SeqCst)
        }

        fn redirect_marker(&self) -> usize {
            self.redirect_marker.load(Ordering::SeqCst)
        }

        fn identified(&self) -> usize {
            self.identified.load(Ordering::SeqCst)
        }

        fn api_keys(&self) -> Vec<String> {
            self.api_keys.lock().expect("api keys lock").clone()
        }
    }

    /// A worker's answer to a submission it admitted: a JSON-RPC `result`
    /// holding the transaction hash, whose value the gateway never reads.
    const ACCEPTED: &str = r#"{"jsonrpc":"2.0","result":"0x1111111111111111111111111111111111111111111111111111111111111111","id":1}"#;

    /// A mock upstream that answers every request, whatever its method, with
    /// `name` as the body and counts what it receives. The `Notifier` keeps it
    /// alive.
    async fn named_mock(name: &'static str) -> (SocketAddr, Seen, Notifier) {
        counting_mock(name, false).await
    }

    /// A worker mock like [`named_mock`] that admits every single
    /// `eth_sendRawTransaction`, answering it with [`ACCEPTED`] so the gateway
    /// remembers its hash. Every other call, a batch of submissions included,
    /// gets `worker`.
    async fn accepting_worker_mock() -> (SocketAddr, Seen, Notifier) {
        counting_mock("worker", true).await
    }

    /// The mock behind [`named_mock`] and [`accepting_worker_mock`].
    async fn counting_mock(
        name: &'static str,
        accept_submissions: bool,
    ) -> (SocketAddr, Seen, Notifier) {
        let seen = Seen::default();
        let counters = seen.clone();
        let mock = Router::new().route(
            "/",
            any(move |headers: HeaderMap, body: Bytes| {
                let counters = counters.clone();
                async move {
                    let get = |name: &str| {
                        headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
                    };
                    counters.hits.fetch_add(1, Ordering::SeqCst);
                    let markers = |name: &str| headers.get_all(name).iter().count();
                    counters.hop_marker.fetch_add(markers("x-tn-gateway"), Ordering::SeqCst);
                    counters
                        .redirect_marker
                        .fetch_add(markers("x-tn-gateway-redirect"), Ordering::SeqCst);
                    if get("x-forwarded-for") == "127.0.0.1"
                        && get("x-forwarded-proto") == "http"
                        && get("user-agent").starts_with("tn-worker-gateway/")
                    {
                        counters.identified.fetch_add(1, Ordering::SeqCst);
                    }
                    counters.api_keys.lock().expect("api keys lock").extend(
                        headers.get_all(API_KEY_HEADER).iter().map(|value| {
                            value.to_str().unwrap_or("<not visible ascii>").to_string()
                        }),
                    );
                    // a batch is an array, whose `method` reads as null
                    let admitted = accept_submissions
                        && serde_json::from_slice::<serde_json::Value>(&body)
                            .is_ok_and(|call| call["method"] == "eth_sendRawTransaction");
                    // labelled json, as a real upstream's answer is: a query
                    // upstream's 2xx without a json content type gets the
                    // -32012 error in its place
                    let answer = if admitted { ACCEPTED } else { name };
                    ([(header::CONTENT_TYPE, "application/json")], answer)
                }
            }),
        );
        let (addr, shutdown) = spawn(mock).await;
        (addr, seen, shutdown)
    }

    /// Gateway state with one worker at `worker` and, when given,
    /// `--redirect-queries` pointing at `query`, using the production proxy
    /// client and the default 60 s recent-submission set. The worker starts
    /// not ready.
    fn redirect_state(worker: SocketAddr, query: Option<SocketAddr>) -> AppState {
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(5),
        )
        .expect("client");
        redirect_state_with_client(worker, query, client)
    }

    fn redirect_state_with_client(
        worker: SocketAddr,
        query: Option<SocketAddr>,
        client: Client,
    ) -> AppState {
        AppState {
            readiness: Arc::new(GatewayReadiness::new(&[upstream(worker)], test_thresholds())),
            http: client,
            query_upstream: query
                .map(|addr| Url::parse(&format!("http://{addr}/")).expect("query url")),
            query_header: None,
            recent_submissions: Some(Arc::new(RecentSubmissions::new(
                Duration::from_secs(60),
                RECENT_SUBMISSIONS_CAPACITY,
            ))),
            denied_method_prefixes: default_denied(),
            max_batch_len: NonZeroUsize::new(DEFAULT_MAX_BATCH_LEN),
            query_http: None,
            upstream_request_timeout: Duration::from_secs(5),
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
            pool_read_slots: inflight_slots(0),
            draining: Arc::default(),
        }
    }

    /// A JSON-RPC call to `method` with empty params and the given id.
    fn call(method: &str, id: u64) -> String {
        format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":[],"id":{id}}}"#)
    }

    /// POST `body` to the gateway, with an extra request header when given.
    async fn post_rpc(
        gateway: SocketAddr,
        extra_header: Option<&'static str>,
        body: String,
    ) -> (StatusCode, String) {
        let request = Client::new().post(format!("http://{gateway}/")).body(body);
        let request = match extra_header {
            Some(name) => request.header(name, "1"),
            None => request,
        };
        let response = request.send().await.expect("send");
        (response.status(), response.text().await.expect("text"))
    }

    /// The JSON-RPC error code and id of a gateway error body.
    fn error_code_and_id(body: &str) -> (i64, serde_json::Value) {
        let body: serde_json::Value = serde_json::from_str(body).expect("json error body");
        (body["error"]["code"].as_i64().expect("error code"), body["id"].clone())
    }

    /// A batch of `len` calls to `method`, with ids `1..=len`.
    fn batch(method: &str, len: u64) -> String {
        format!("[{}]", (1..=len).map(|id| call(method, id)).collect::<Vec<_>>().join(","))
    }

    /// An all-submission batch is screened element by element; the first bad
    /// element answers for the batch with its own id, and nothing reaches an
    /// upstream. A mixed batch is not screened: it goes whole to the query
    /// upstream, which validates it.
    #[tokio::test]
    async fn bad_element_inside_an_all_submission_batch_is_refused_with_its_id() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // a submission without a raw transaction is left to the worker
        let unscreened = call("eth_sendRawTransaction", 1);
        let bad = |method: &str, id: &str| {
            format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":["0xdeadbeef"],"id":{id}}}"#)
        };
        let body = format!(
            "[{unscreened},{},{}]",
            bad("eth_sendRawTransactionSync", r#""tx-2""#),
            bad("eth_sendRawTransaction", "3")
        );
        let (status, text) = post_rpc(gateway, None, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error_code_and_id(&text), (-32007, serde_json::json!("tx-2")));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 0));

        let (status, text) = post_rpc(gateway, None, format!("[{unscreened},{unscreened}]")).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        let mixed = format!("[{},{}]", bad("eth_sendRawTransaction", "4"), call("eth_chainId", 5));
        let (status, text) = post_rpc(gateway, None, mixed).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, 1));
    }

    /// A batch of K calls costs K rate-limit tokens: one at the edge, before
    /// the body is read, and K - 1 once the handler has counted the calls. A
    /// per-IP burst of K - 1 refuses it; a burst of K admits it.
    #[tokio::test]
    async fn batch_of_k_elements_costs_k_tokens() {
        const K: u32 = 5;
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        for (burst, expected) in [(K - 1, StatusCode::TOO_MANY_REQUESTS), (K, StatusCode::OK)] {
            let state = redirect_state(worker, None);
            state.readiness.set_ready(0, true);
            let limiters = RateLimiters::new(
                Some(RateLimit::new(nz(1), nz(burst))),
                None,
                None,
                16,
                PrefixPolicy::default(),
            )
            .expect("limiters");
            let (gateway, _shutdown) = spawn(router(
                state,
                Duration::from_secs(5),
                MAX_REQUEST_BYTES,
                None,
                RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
                Arc::default(),
            ))
            .await;

            let (status, text) =
                post_rpc(gateway, None, batch("eth_getBalance", u64::from(K))).await;
            assert_eq!(status, expected, "per-IP burst {burst}");
            if expected == StatusCode::TOO_MANY_REQUESTS {
                assert_eq!(error_code_and_id(&text), (-32006, serde_json::Value::Null));
            }
        }
        assert_eq!(worker_seen.hits(), 1, "only the batch within the burst is forwarded");
    }

    #[tokio::test]
    async fn over_length_batch_gets_413_batch_too_long() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        state.max_batch_len = NonZeroUsize::new(3);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // at the cap, a batch routes as usual
        for (method, expected) in
            [("eth_getBalance", "query"), ("eth_sendRawTransaction", "worker")]
        {
            let (status, text) = post_rpc(gateway, None, batch(method, 3)).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, expected), "{method}");
        }

        // one past the cap, on either route, is refused whole at the gateway
        for body in [
            batch("eth_getBalance", 4),
            batch("eth_sendRawTransaction", 4),
            batch("eth_getBalance", 1_000),
            format!("[{},1,2,3]", call("eth_sendRawTransaction", 1)),
            // the worker drops leading ascii whitespace, form feed included
            format!("\u{c}{}", batch("eth_getBalance", 4)),
        ] {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
            assert_eq!(error_code_and_id(&text), (-32003, serde_json::Value::Null), "{body}");
            let error: serde_json::Value = serde_json::from_str(&text).expect("json");
            assert_eq!(error["error"]["message"], "batch too long");
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, 1));
    }

    /// A batch the gateway cannot read to its end is refused whole, with or
    /// without the redirect: the worker would skip the element serde's typed
    /// readers refuse and run every other call, so the gateway cannot know
    /// the batch's length, its cost or whether it is all submissions.
    #[tokio::test]
    async fn unreadable_batch_gets_400_invalid_request() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let reads = (1..=1_000).map(|id| call("eth_getBalance", id)).collect::<Vec<_>>().join(",");
        for redirect in [Some(query), None] {
            let state = redirect_state(worker, redirect);
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;
            for lead in ["1e400", r#"{"\udc00":1}"#] {
                let (status, text) = post_rpc(gateway, None, format!("[{lead},{reads}]")).await;
                assert_eq!(status, StatusCode::BAD_REQUEST, "{lead}");
                assert_eq!(error_code_and_id(&text), (-32600, serde_json::Value::Null), "{lead}");
            }
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 0));
    }

    #[tokio::test]
    async fn redirect_sends_each_method_to_its_upstream() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let table = [
            ("eth_sendRawTransaction", "worker"),
            ("eth_sendRawTransactionSync", "worker"),
            ("eth_sendTransaction", "query"),
            ("eth_call", "query"),
            ("eth_chainId", "query"),
            ("eth_getLogs", "query"),
            ("eth_getTransactionCount", "query"),
            ("tn_info", "query"),
            ("debug_traceTransaction", "query"),
        ];
        for (method, expected) in table {
            let (status, text) = post_rpc(gateway, None, call(method, 1)).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, expected), "{method}");
        }
        let (status, text) =
            post_rpc(gateway, None, "not json, but eth_sendRawTransaction".to_string()).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!(worker_seen.hits(), 2);
        assert_eq!(query_seen.hits(), table.len() - 1);

        // the screen still runs before routing: a junk submission is refused
        // at the gateway and reaches neither upstream
        let junk =
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":3}"#;
        let (status, text) = post_rpc(gateway, None, junk.to_string()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(error_code_and_id(&text), (-32007, serde_json::json!(3)));
        assert_eq!(worker_seen.hits() + query_seen.hits(), table.len() + 1);
    }

    #[tokio::test]
    async fn redirect_sends_only_all_submission_batches_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let submission = call("eth_sendRawTransaction", 1);
        let sync = call("eth_sendRawTransactionSync", 2);
        let read = call("eth_getLogs", 3);
        for (body, expected) in [
            (format!("[{submission},{sync}]"), "worker"),
            (format!("[{submission},{read}]"), "query"),
            (format!("[{read},{submission}]"), "query"),
            ("[]".to_string(), "query"),
        ] {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, expected), "{body}");
        }
        assert_eq!(worker_seen.hits(), 1);
        assert_eq!(query_seen.hits(), 3);
    }

    /// A call with more than one `method` member never reaches the worker on
    /// the strength of either name: with the redirect, alone it goes to the
    /// query upstream like any body the gateway cannot read, and a batch
    /// holding one is refused whole, with or without the redirect.
    #[tokio::test]
    async fn duplicated_method_never_routes_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let duplicates = [
            r#"{"jsonrpc":"2.0","method":"eth_call","method":"eth_sendRawTransaction","params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","method":"eth_call","params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_call","m\u0065thod":"eth_sendRawTransactionSync","params":[],"id":1}"#,
        ];
        let submission = call("eth_sendRawTransaction", 2);
        for redirect in [Some(query), None] {
            let state = redirect_state(worker, redirect);
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;
            for duplicate in duplicates {
                if redirect.is_some() {
                    let (status, text) = post_rpc(gateway, None, duplicate.to_string()).await;
                    assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{duplicate}");
                }
                for body in [format!("[{duplicate}]"), format!("[{submission},{duplicate}]")] {
                    let (status, text) = post_rpc(gateway, None, body.clone()).await;
                    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
                    assert_eq!(
                        error_code_and_id(&text),
                        (-32600, serde_json::Value::Null),
                        "{body}"
                    );
                }
            }
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, duplicates.len()));
    }

    /// A `method` that is an object or an array names no method, so the call
    /// routes like one the gateway cannot read: to the query upstream, never
    /// to the worker, however a submission's name sits inside it. In a batch
    /// it is another call, so a batch beside a submission is mixed.
    #[tokio::test]
    async fn container_valued_method_never_routes_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let containers = [
            r#"{"jsonrpc":"2.0","method":["eth_sendRawTransaction"],"params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":[["eth_sendRawTransactionSync"]],"params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":{"method":"eth_sendRawTransaction"},"params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":{},"params":["0xdeadbeef"],"id":1}"#,
        ];
        let submission = call("eth_sendRawTransaction", 2);
        let mut sent = 0;
        for container in containers {
            for body in [
                container.to_string(),
                format!("[{container}]"),
                format!("[{submission},{container}]"),
                format!("[{container},{submission}]"),
            ] {
                let (status, text) = post_rpc(gateway, None, body.clone()).await;
                assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{body}");
                sent += 1;
            }
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, sent));
    }

    #[tokio::test]
    async fn worker_down_serves_queries_and_refuses_submissions() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        // the worker is never marked ready
        let (gateway, _shutdown) = spawn(test_router(redirect_state(worker, Some(query)))).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));

        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 2)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text), (-32000, serde_json::json!(2)));

        let ready =
            Client::new().get(format!("http://{gateway}/ready")).send().await.expect("send");
        assert_eq!(ready.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(worker_seen.hits(), 0);
        assert_eq!(query_seen.hits(), 1);
    }

    #[tokio::test]
    async fn query_upstream_down_never_falls_back_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        // nothing listens on port 1
        let state = redirect_state(worker, Some("127.0.0.1:1".parse().expect("addr")));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 9)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(9)));
        assert_eq!(worker_seen.hits(), 0, "a failed query must not fall back to the worker");

        // submissions and readiness are unaffected
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        let ready =
            Client::new().get(format!("http://{gateway}/ready")).send().await.expect("send");
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn slow_query_upstream_times_out_without_falling_back() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let slow = Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let (query, _query) = spawn(slow).await;
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let state = redirect_state_with_client(worker, Some(query), client);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_getLogs", 4)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error_code_and_id(&text), (-32002, serde_json::json!(4)));
        assert_eq!(worker_seen.hits(), 0);
    }

    /// A mock upstream that answers every request at once with a `200` head
    /// and then sends `body` in pieces of `piece` bytes, one every `every`,
    /// and then, when `stall` is set, holds the body open without sending
    /// anything more.
    async fn trickling_mock(
        body: &'static str,
        piece: usize,
        every: Duration,
        stall: bool,
    ) -> (SocketAddr, Notifier) {
        let mock = Router::new().route(
            "/",
            post(move || async move {
                let pieces = futures::stream::unfold(body.as_bytes(), move |rest| async move {
                    if rest.is_empty() {
                        return None;
                    }
                    tokio::time::sleep(every).await;
                    let (now, later) = rest.split_at(piece.min(rest.len()));
                    Some((Ok::<_, std::io::Error>(Bytes::from_static(now)), later))
                });
                let tail = futures::StreamExt::flatten(futures::stream::iter(
                    stall.then(futures::stream::pending),
                ));
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    axum::body::Body::from_stream(futures::StreamExt::chain(pieces, tail)),
                )
            }),
        );
        spawn(mock).await
    }

    /// The query route's request timeout bounds each gap in a streamed answer,
    /// not the whole transfer: an answer that keeps arriving runs well past it
    /// and reaches the client whole.
    #[tokio::test]
    async fn progressing_query_answer_outlives_the_route_deadline() {
        let mut metrics = CapturedMetrics::install();
        let answer = "0123456789".repeat(15);
        let answer: &'static str = answer.leak();
        // 15 pieces 100 ms apart, about 1.5 s in all
        let (query, _query) = trickling_mock(answer, 10, Duration::from_millis(100), false).await;
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let deadline = Duration::from_millis(400);
        let mut state = redirect_state(worker, Some(query));
        state.query_http = Some(
            proxy_client(UpstreamClientBuilder::for_test(Duration::from_secs(2)), deadline)
                .expect("client"),
        );
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let started = tokio::time::Instant::now();
        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.bytes().await.expect("the whole body");
        let elapsed = started.elapsed();
        assert_eq!(body.as_ref(), answer.as_bytes());
        assert!(elapsed > 3 * deadline, "the answer took {elapsed:?}, past the deadline");

        let routed = "tn_worker_gateway_routed_requests_total";
        assert_eq!(metrics.value(routed, &[("route", "query"), ("result", "body_failed")]), 0.0);
        assert_eq!(metrics.value(routed, &[("route", "query"), ("result", "forwarded")]), 1.0);
    }

    /// A query answer that stops arriving still fails once its route's request
    /// timeout passes without data: the client gets the `200` head and then a
    /// body that cannot be read to the end, and the failure is counted.
    #[tokio::test]
    async fn stalled_query_body_fails_after_the_idle_window() {
        let mut metrics = CapturedMetrics::install();
        // one piece, then nothing
        let start = r#"{"jsonrpc":"2.0","#;
        let (query, _query) =
            trickling_mock(start, start.len(), Duration::from_millis(10), true).await;
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let mut state = redirect_state(worker, Some(query));
        state.query_http = Some(
            proxy_client(
                UpstreamClientBuilder::for_test(Duration::from_secs(2)),
                Duration::from_millis(400),
            )
            .expect("client"),
        );
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let started = tokio::time::Instant::now();
        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        let body = tokio::time::timeout(Duration::from_secs(10), response.bytes())
            .await
            .expect("the stalled body must end");
        let elapsed = started.elapsed();
        assert!(body.is_err(), "a stalled body must not read as complete");
        assert!(elapsed < Duration::from_secs(3), "the stalled body ended after {elapsed:?}");

        let routed = "tn_worker_gateway_routed_requests_total";
        assert_eq!(metrics.value(routed, &[("route", "query"), ("result", "body_failed")]), 1.0);
    }

    /// The worker's answer to a remembered submission is read whole, so it
    /// stays bounded by `--upstream-request-timeout` from send even while each
    /// piece arrives inside the read timeout: a worker that trickles an
    /// accepting answer past the deadline loses it, and the hash is not
    /// remembered.
    #[tokio::test]
    async fn buffered_submission_answer_keeps_its_deadline() {
        // the accepting answer in 26 pieces 100 ms apart, about 2.6 s in all,
        // each gap well inside the 400 ms read timeout
        let (worker, _worker) =
            trickling_mock(ACCEPTED, 4, Duration::from_millis(100), false).await;
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(400),
        )
        .expect("client");
        let mut state = redirect_state_with_client(worker, None, client);
        state.upstream_request_timeout = Duration::from_millis(400);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let started = tokio::time::Instant::now();
        let (status, text) = post_rpc(gateway, None, submission(7)).await;
        let elapsed = started.elapsed();
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error_code_and_id(&text), (-32002, serde_json::json!(7)));
        assert!(elapsed < Duration::from_secs(2), "the answer was given up after {elapsed:?}");
        assert!(!recent.contains(&submitted_hash()));
    }

    /// The worker and query clients resolve upstream names through one cache:
    /// a host the worker client has resolved is not looked up again when the
    /// query client connects to it.
    #[tokio::test]
    async fn query_and_worker_clients_share_one_resolver() {
        let (upstream, _upstream_seen, _upstream) = named_mock("upstream").await;
        let lookups = Arc::new(AtomicUsize::new(0));
        let lookup = {
            let lookups = Arc::clone(&lookups);
            lookup_fn(move |_host| {
                lookups.fetch_add(1, Ordering::SeqCst);
                async move { Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))]) }
            })
        };
        let url = named_url("shared.test", upstream, "/");
        let settings =
            worker_settings("http", upstream, upstream, &[format!("--redirect-queries={url}")]);
        let clients = crate::proxy::route_clients(&settings, lookup).expect("clients");
        let query = clients.query.expect("a redirect builds a query client");

        for client in [&clients.worker, &query] {
            let response = client.post(url.clone()).send().await.expect("send");
            assert_eq!(response.text().await.expect("text"), "upstream");
        }
        assert_eq!(lookups.load(Ordering::SeqCst), 1, "the query client hits the shared cache");
    }

    #[tokio::test]
    async fn markers_follow_the_route() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, call("eth_call", 1)).await;
        assert_eq!(text, "query");
        let (_, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 2)).await;
        assert_eq!(text, "worker");

        // the query hop carries the redirect marker and never the hop marker,
        // which a public rpc behind its own gateway would reject as a loop
        assert_eq!(
            (query_seen.hits(), query_seen.redirect_marker(), query_seen.hop_marker()),
            (1, 1, 0)
        );
        assert_eq!(
            (worker_seen.hits(), worker_seen.hop_marker(), worker_seen.redirect_marker()),
            (1, 1, 0)
        );
        // both hops carry the client identity and the gateway's user agent
        assert_eq!((query_seen.identified(), worker_seen.identified()), (1, 1));
    }

    #[tokio::test]
    async fn only_a_redirecting_gateway_rejects_the_redirect_marker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;

        let redirecting = redirect_state(worker, Some(query));
        redirecting.readiness.set_ready(0, true);
        let (redirecting, _redirecting) = spawn(test_router(redirecting)).await;
        for method in ["eth_call", "eth_sendRawTransaction"] {
            let (status, text) =
                post_rpc(redirecting, Some("x-tn-gateway-redirect"), call(method, 5)).await;
            assert_eq!(status, StatusCode::LOOP_DETECTED, "{method}");
            assert_eq!(error_code_and_id(&text), (-32004, serde_json::json!(5)));
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 0));

        // a gateway without a redirect (one fronting the public rpc, say)
        // forwards a redirected request normally
        let plain = redirect_state(worker, None);
        plain.readiness.set_ready(0, true);
        let (plain, _plain) = spawn(test_router(plain)).await;
        let (status, text) =
            post_rpc(plain, Some("x-tn-gateway-redirect"), call("eth_call", 6)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_seen.hits(), 1);
    }

    /// The redirect marker cannot tell a loop from a chain: a redirecting
    /// gateway behind another one refuses every query the first redirects to
    /// it, while a gateway without the flag on the same path forwards them.
    #[tokio::test]
    async fn chained_redirecting_gateways_answer_508_at_the_second() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let second = redirect_state(worker, Some(query));
        second.readiness.set_ready(0, true);
        let (second, _second) = spawn(test_router(second)).await;
        let first = redirect_state(worker, Some(second));
        first.readiness.set_ready(0, true);
        let (first, _first) = spawn(test_router(first)).await;

        let (status, text) = post_rpc(first, None, call("eth_call", 8)).await;
        assert_eq!(status, StatusCode::LOOP_DETECTED);
        assert_eq!(error_code_and_id(&text), (-32004, serde_json::json!(8)));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 0));

        // a gateway without the flag in the middle forwards the query
        let plain = redirect_state(query, None);
        plain.readiness.set_ready(0, true);
        let (plain, _plain) = spawn(test_router(plain)).await;
        let behind_plain = redirect_state(worker, Some(plain));
        behind_plain.readiness.set_ready(0, true);
        let (behind_plain, _behind_plain) = spawn(test_router(behind_plain)).await;
        let (status, text) = post_rpc(behind_plain, None, call("eth_call", 9)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 1));
    }

    #[tokio::test]
    async fn hop_marker_is_still_rejected_with_the_redirect_on() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        for method in ["eth_call", "eth_sendRawTransaction"] {
            let (status, text) = post_rpc(gateway, Some("x-tn-gateway"), call(method, 7)).await;
            assert_eq!(status, StatusCode::LOOP_DETECTED, "{method}");
            assert_eq!(error_code_and_id(&text), (-32004, serde_json::json!(7)));
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, 0));
    }

    #[tokio::test]
    async fn without_a_redirect_everything_goes_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let bodies = [
            call("eth_chainId", 1),
            call("eth_getLogs", 2),
            call("net_version", 3),
            call("eth_sendRawTransaction", 4),
            call("eth_sendRawTransactionSync", 5),
            format!("[{},{}]", call("eth_sendRawTransaction", 6), call("eth_call", 7)),
        ];
        for body in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        // a request some redirecting gateway sent on (this one fronting the
        // public rpc, say) is forwarded too, and the client's marker is not
        let (status, text) =
            post_rpc(gateway, Some("x-tn-gateway-redirect"), call("eth_call", 8)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        // each forward carries exactly one hop marker and no redirect marker
        let forwards = bodies.len() + 1;
        assert_eq!(
            (worker_seen.hits(), worker_seen.hop_marker(), worker_seen.redirect_marker()),
            (forwards, forwards, 0)
        );
    }

    #[tokio::test]
    async fn denied_prefixes_are_refused_before_forwarding() {
        // the worker is ready, so a call that got past the policy would be
        // answered `200 worker` instead of refused
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let methods = ["tn_getStuff", "debug_traceTransaction", "trace_block", "admin_peers"];
        for (id, method) in (1u64..).zip(methods) {
            let (status, text) = post_rpc(gateway, None, call(method, id)).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{method}");
            assert_eq!(error_code_and_id(&text), (-32011, serde_json::json!(id)), "{method}");
        }
        assert_eq!(worker_seen.hits(), 0, "a denied call must never reach the worker");
    }

    #[tokio::test]
    async fn eth_calls_are_forwarded_without_a_redirect() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let bodies = [
            call("eth_blockNumber", 1),
            call("eth_call", 2),
            call("eth_getLogs", 3),
            call("eth_sendRawTransaction", 4),
            format!("[{},{}]", call("eth_chainId", 5), call("eth_getBalance", 6)),
        ];
        for body in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        assert_eq!(worker_seen.hits(), bodies.len());
    }

    #[tokio::test]
    async fn batch_with_a_denied_method_is_refused_whole() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let batch = format!("[{},{}]", call("eth_blockNumber", 1), call("tn_x", 2));
        let (status, text) = post_rpc(gateway, None, batch).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        // one error object for the whole batch, not an array of per-call answers
        let body: serde_json::Value = serde_json::from_str(&text).expect("json error body");
        assert!(body.is_object(), "expected one error object: {text}");
        assert_eq!(error_code_and_id(&text), (-32011, serde_json::Value::Null));
        assert_eq!(worker_seen.hits(), 0, "no part of the batch may reach the worker");
    }

    #[tokio::test]
    async fn unreadable_bodies_do_not_carry_a_denied_call_to_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let denied = call("tn_getStuff", 1);
        // a single call behind a form feed is read as the worker reads it and
        // refused by the policy with its own id; a batch the gateway cannot
        // read to its end is refused whole as unreadable, which comes first
        let invalid_batch = (StatusCode::BAD_REQUEST, -32600, serde_json::Value::Null);
        for (body, expected) in [
            (format!("\x0C{denied}"), (StatusCode::FORBIDDEN, -32011, serde_json::json!(1))),
            (format!(r#"["\ud800",{denied}]"#), invalid_batch.clone()),
            (format!(r#"[{denied},{{"\ud800":1}}]"#), invalid_batch.clone()),
            (format!("[1e999,{denied}]"), invalid_batch.clone()),
        ] {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            let (code, id) = error_code_and_id(&text);
            assert_eq!((status, code, id), expected, "{body:?}");
        }
        assert_eq!(worker_seen.hits(), 0, "a denied call must never reach the worker");
    }

    /// The scan's two shortcuts, the single-call fast path and the skip of a
    /// mixed batch's remaining elements, would hide a call from the method
    /// policy, so a deny list in force turns both off: a denied call after a
    /// batch's first elements have made it mixed, a single denied call naming
    /// no method the scan routes, and a single call the scan cannot read are
    /// all refused. With the list empty the same bodies reach the worker.
    #[tokio::test]
    async fn denied_calls_behind_the_scan_shortcuts_are_refused() {
        let mixed = format!(
            r#"[{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":1}},{},{}]"#,
            call("eth_call", 2),
            call("tn_getStuff", 3),
        );
        let unreadable = r#"{"jsonrpc":"2.0","method":"eth_call","params":[1e999],"id":5}"#;
        let bodies = [
            (mixed, serde_json::Value::Null),
            (call("tn_getStuff", 4), serde_json::json!(4)),
            (unreadable.to_string(), serde_json::json!(5)),
        ];

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        for (body, id) in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(error_code_and_id(&text), (-32011, id.clone()), "{body}");
        }
        assert_eq!(worker_seen.hits(), 0, "a denied call must never reach the worker");

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let mut state = redirect_state(worker, None);
        state.denied_method_prefixes = Arc::from([]);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        for (body, _) in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        assert_eq!(worker_seen.hits(), bodies.len());
    }

    /// A 2 MiB array `id` is never echoed (#1595) on any path that recovers an
    /// id from the body: the hop marker, a denied call, a too-long batch, the
    /// screen on a single call and on a batch element, a mixed batch holding a
    /// denied call, and the upstream-error envelope all answer `id: null`.
    #[tokio::test]
    async fn oversized_ids_echo_null_on_every_reject_path() {
        let huge_id = format!("[{}0]", "0,".repeat(1024 * 1024));
        let with_id = |method: &str, params: &str| {
            format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{huge_id}}}"#)
        };
        let too_long = format!(
            "[{},{}]",
            with_id("eth_chainId", "[]"),
            (1..=DEFAULT_MAX_BATCH_LEN as u64)
                .map(|id| call("eth_chainId", id))
                .collect::<Vec<_>>()
                .join(",")
        );
        let bad_element = format!(
            r#"[{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{SIGNED_TX}"],"id":1}},{}]"#,
            with_id("eth_sendRawTransaction", r#"["0xdeadbeef"]"#)
        );
        let mixed = format!(
            "[{},{},{}]",
            call("eth_sendRawTransaction", 1),
            with_id("eth_call", "[]"),
            call("tn_getStuff", 3)
        );
        let cases = [
            (Some("x-tn-gateway"), with_id("eth_chainId", "[]"), StatusCode::LOOP_DETECTED, -32004),
            (None, with_id("tn_getStuff", "[]"), StatusCode::FORBIDDEN, -32011),
            (None, too_long, StatusCode::PAYLOAD_TOO_LARGE, -32003),
            (
                None,
                with_id("eth_sendRawTransaction", r#"["0xdeadbeef"]"#),
                StatusCode::BAD_REQUEST,
                -32007,
            ),
            (None, bad_element, StatusCode::BAD_REQUEST, -32007),
            (None, mixed, StatusCode::FORBIDDEN, -32011),
        ];
        let large_router = |state| {
            router(
                state,
                Duration::from_secs(5),
                4 * MAX_REQUEST_BYTES,
                NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES),
                RouterLayers::default(),
                Arc::default(),
            )
        };

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(large_router(state)).await;
        for (header, body, status, code) in cases {
            let (got, text) = post_rpc(gateway, header, body).await;
            assert_eq!((got, error_code_and_id(&text)), (status, (code, serde_json::Value::Null)));
        }
        assert_eq!(worker_seen.hits(), 0, "every case is refused before a forward");

        let (plain, _plain) = answering_mock(StatusCode::BAD_GATEWAY, "text/plain", "down").await;
        let state = redirect_state(plain, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(large_router(state)).await;
        let (status, text) = post_rpc(gateway, None, with_id("eth_chainId", "[]")).await;
        assert_eq!(
            (status, error_code_and_id(&text)),
            (StatusCode::BAD_GATEWAY, (-32012, serde_json::Value::Null))
        );
    }

    #[tokio::test]
    async fn redirect_leaves_routing_unchanged() {
        // the default list is configured, but a redirect turns the policy off
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let bodies = [
            call("tn_x", 1),
            call("debug_traceTransaction", 2),
            format!("[{},{}]", call("eth_blockNumber", 3), call("tn_x", 4)),
        ];
        for body in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{body}");
        }
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 5)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!((query_seen.hits(), worker_seen.hits()), (bodies.len(), 1));
    }

    #[tokio::test]
    async fn empty_denied_list_allows_everything() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let mut state = redirect_state(worker, None);
        state.denied_method_prefixes = Arc::from([]);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let bodies = [
            call("tn_getStuff", 1),
            call("debug_traceTransaction", 2),
            call("trace_block", 3),
            call("admin_peers", 4),
            format!("[{},{}]", call("eth_blockNumber", 5), call("tn_x", 6)),
        ];
        for body in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        assert_eq!(worker_seen.hits(), bodies.len());
    }

    /// The proxy client follows no redirect. A query upstream answering `307`
    /// or `308` (which reqwest would otherwise follow, replaying the POST
    /// body) must not bounce a read onto the worker; the status passes through
    /// to the client like any other, without the `Location` header and, the
    /// body not being JSON, with the gateway's JSON-RPC error in its place.
    #[tokio::test]
    async fn query_upstream_redirects_are_not_followed() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let location = format!("http://{worker}/");
        for status in [StatusCode::TEMPORARY_REDIRECT, StatusCode::PERMANENT_REDIRECT] {
            let location = location.clone();
            let bouncer = Router::new().route(
                "/",
                post(move || async move { (status, [(header::LOCATION, location)], "moved") }),
            );
            let (query, _query) = spawn(bouncer).await;
            let state = redirect_state(worker, Some(query));
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;

            // a client that follows nothing either, so any worker hit could
            // only have come from the gateway
            let client = Client::builder().redirect(Policy::none()).build().expect("client");
            let response = client
                .post(format!("http://{gateway}/"))
                .body(call("eth_call", 1))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), status);
            assert!(response.headers().get(header::LOCATION).is_none());
            let text = response.text().await.expect("text");
            assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(1)));
        }
        assert_eq!(worker_seen.hits(), 0, "a redirect must never reach the worker");
    }

    /// A `3xx` with a JSON body passes through as the upstream's own answer,
    /// without its `Location`, and is logged as a warning on either route,
    /// naming the upstream by origin, like a `3xx` without one: the gateway
    /// follows no redirect, so a worker behind a front that answers `307` must
    /// not go unnoticed.
    #[tokio::test]
    async fn json_redirect_from_an_upstream_is_logged() {
        if !process_per_test() {
            eprintln!("skipped: {THROTTLED_WARNING_SKIP}");
            return;
        }
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let answer = r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#;
        let location = format!("http://{worker}/elsewhere");
        let bouncer = Router::new().route(
            "/",
            post(move || async move {
                (
                    StatusCode::TEMPORARY_REDIRECT,
                    [
                        (header::LOCATION, location),
                        (header::CONTENT_TYPE, "application/json".into()),
                    ],
                    answer,
                )
            }),
        );
        let (bouncer, _bouncer) = spawn(bouncer).await;
        let client = Client::builder().redirect(Policy::none()).build().expect("client");
        for (route, state) in [
            ("worker", redirect_state(bouncer, None)),
            ("query", redirect_state(worker, Some(bouncer))),
        ] {
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;
            let response = client
                .post(format!("http://{gateway}/"))
                .body(call("eth_call", 1))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT, "{route}");
            assert!(response.headers().get(header::LOCATION).is_none(), "{route}");
            assert_eq!(response.text().await.expect("text"), answer, "{route}");
        }
        assert_eq!(worker_seen.hits(), 0, "a redirect must never be followed");

        let logs = logs.contents();
        for route in ["worker", "query"] {
            let line = logs
                .lines()
                .find(|line| {
                    line.contains("upstream answered a redirect")
                        && line.contains(&format!("route=\"{route}\""))
                })
                .unwrap_or_else(|| panic!("no redirect warning for the {route} route: {logs}"));
            assert!(line.contains(" WARN "), "{line}");
            assert!(line.contains("status=307"), "{line}");
            assert!(line.contains(&format!("upstream=http://{bouncer}")), "{line}");
        }
        assert!(!logs.contains("elsewhere"), "the Location must not be logged: {logs}");
    }

    /// A per-IP limit of one request per second with no burst headroom, and no
    /// global limit. A test's exchanges see no refill while they take under
    /// 1s in all, which loopback exchanges do (1 token/s is the slowest rate
    /// the limit can express).
    fn one_request_per_ip() -> Option<Arc<RateLimiters>> {
        RateLimiters::new(
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            None,
            16,
            PrefixPolicy::default(),
        )
    }

    /// The gateway router with `limiters`, trusting the proxies in `list`.
    fn identity_router(state: AppState, limiters: Option<Arc<RateLimiters>>, list: &str) -> Router {
        let trusted = Arc::new(list.parse::<TrustedProxies>().expect("trusted proxies"));
        router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: limiters, ..RouterLayers::default() },
            trusted,
        )
    }

    /// POST a JSON-RPC call to the gateway carrying `headers`.
    async fn post_with(gateway: SocketAddr, headers: &[(&str, &str)]) -> (StatusCode, String) {
        let request = headers.iter().fold(
            Client::new().post(format!("http://{gateway}/")).body(call("eth_chainId", 1)),
            |request, (name, value)| request.header(*name, *value),
        );
        let response = request.send().await.expect("send");
        (response.status(), response.text().await.expect("text"))
    }

    /// A mock upstream that answers every POST with the `X-Forwarded-For` and
    /// `X-Forwarded-Proto` it received, joined by `|`.
    async fn forwarding_echo() -> (SocketAddr, Notifier) {
        let mock = Router::new().route(
            "/",
            post(|headers: HeaderMap| async move {
                let get = |name: &str| {
                    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
                };
                format!("{}|{}", get("x-forwarded-for"), get("x-forwarded-proto"))
            }),
        );
        spawn(mock).await
    }

    #[tokio::test]
    async fn spoofed_x_forwarded_for_from_an_untrusted_peer_is_ignored() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // some proxies are trusted, but not the loopback peer the test connects from
        let app = identity_router(state, one_request_per_ip(), "10.0.0.0/8, 2001:db8::/32");
        let (gateway, _shutdown) = spawn(app).await;

        let (status, _) = post_with(gateway, &[("x-forwarded-for", "198.51.100.1")]).await;
        assert_eq!(status, StatusCode::OK);
        // claiming another client from the same peer spends the same bucket
        let (status, text) = post_with(gateway, &[("x-forwarded-for", "198.51.100.2")]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&text).0, -32006);
        assert_eq!(worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn trusted_peer_requests_are_keyed_by_the_forwarded_client() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let app = identity_router(state, one_request_per_ip(), "127.0.0.1/32");
        let (gateway, _shutdown) = spawn(app).await;

        // two clients behind one trusted peer get a bucket each
        for client in ["198.51.100.1", "198.51.100.2"] {
            let (status, text) = post_with(gateway, &[("x-forwarded-for", client)]).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{client}");
        }
        // the bucket is the forwarded client's: its second request is limited,
        // whatever the client wrote to the left of the trusted peer's entry
        let chain = "203.0.113.9, 198.51.100.1";
        let (status, _) = post_with(gateway, &[("x-forwarded-for", chain)]).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(worker_seen.hits(), 2);
    }

    /// The calls a batch carries beyond its first are charged to the client
    /// its edge token was: the forwarded client behind a trusted peer, never
    /// the peer itself.
    #[tokio::test]
    async fn batch_charge_keys_on_the_forwarded_client() {
        const K: u32 = 3;
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let limiters = RateLimiters::new(
            Some(RateLimit::new(nz(1), nz(K))),
            None,
            None,
            16,
            PrefixPolicy::default(),
        );
        let (gateway, _shutdown) = spawn(identity_router(state, limiters, "127.0.0.1/32")).await;
        let post = |client: &'static str, body: String| async move {
            let response = Client::new()
                .post(format!("http://{gateway}/"))
                .header("x-forwarded-for", client)
                .body(body)
                .send()
                .await
                .expect("send");
            response.status()
        };

        // each client behind the one trusted peer pays for its K-call batch
        // from its own burst; charged to the peer, the second batch would find
        // the peer's bucket spent by the first
        for client in ["198.51.100.1", "198.51.100.2"] {
            let status = post(client, batch("eth_getBalance", u64::from(K))).await;
            assert_eq!(status, StatusCode::OK, "{client}");
        }
        // the first client's bucket is the one its batch emptied
        let status = post("198.51.100.1", call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(worker_seen.hits(), 2);
    }

    #[tokio::test]
    async fn untrusted_chain_is_replaced_toward_the_upstream() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(identity_router(state, None, "10.0.0.0/8")).await;

        let spoofed = [("x-forwarded-for", "6.6.6.6, 7.7.7.7"), ("x-forwarded-proto", "https")];
        let (status, text) = post_with(gateway, &spoofed).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "127.0.0.1|http"));
    }

    #[tokio::test]
    async fn trusted_chain_is_extended_toward_the_upstream() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(identity_router(state, None, "127.0.0.1/32")).await;

        // the resolved client, then the trusted peer that vouched for it
        let forwarded = [("x-forwarded-for", "6.6.6.6, 7.7.7.7"), ("x-forwarded-proto", "https")];
        let (status, text) = post_with(gateway, &forwarded).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "7.7.7.7, 127.0.0.1|https"));
        // nothing forwarded: the peer alone
        let (status, text) = post_with(gateway, &[]).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "127.0.0.1|http"));
    }

    /// Serve the gateway router with `limiters` and `--proxy-protocol` on,
    /// trusting the proxies in `list`, which are also the peers that must
    /// send a PROXY header (`--proxy-protocol-from` unset).
    async fn spawn_proxy_protocol(
        state: AppState,
        limiters: Option<Arc<RateLimiters>>,
        list: &str,
        header_read_timeout: Duration,
    ) -> (SocketAddr, Notifier) {
        spawn_proxy_protocol_from(state, limiters, list, list, header_read_timeout).await
    }

    /// [`spawn_proxy_protocol`] trusting the proxies in `trusted` and reading
    /// a PROXY header from the peers in `senders` (`--proxy-protocol-from`).
    async fn spawn_proxy_protocol_from(
        state: AppState,
        limiters: Option<Arc<RateLimiters>>,
        trusted: &str,
        senders: &str,
        header_read_timeout: Duration,
    ) -> (SocketAddr, Notifier) {
        let trusted = Arc::new(trusted.parse::<TrustedProxies>().expect("trusted proxies"));
        let senders = Arc::new(senders.parse::<TrustedProxies>().expect("proxy protocol senders"));
        let app = router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: limiters, ..RouterLayers::default() },
            Arc::clone(&trusted),
        );
        let limits =
            ServerLimits { header_read_timeout, proxy_protocol: Some(senders), ..test_limits() };
        spawn_trusting(app, limits, trusted).await
    }

    /// Send `preamble` and then a JSON-RPC POST carrying the raw header lines
    /// `headers`, in one write on a fresh connection, and return everything
    /// the gateway sends before it closes the connection.
    async fn raw_exchange(gateway: SocketAddr, preamble: &[u8], headers: &str) -> String {
        let body = call("eth_chainId", 1);
        let request = format!(
            "POST / HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n{headers}\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        stream.write_all(&[preamble, request.as_bytes()].concat()).await.expect("write");
        let mut response = Vec::new();
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the gateway should close the connection");
        drop(read); // EOF is Ok, a reset is Err; either way `response` holds what arrived
        String::from_utf8_lossy(&response).into_owned()
    }

    fn socket(addr: &str) -> SocketAddr {
        addr.parse().expect("socket addr")
    }

    #[tokio::test]
    async fn proxy_protocol_v2_header_is_parsed_from_a_trusted_peer() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn_proxy_protocol(
            state,
            one_request_per_ip(),
            "127.0.0.1/32",
            Duration::from_secs(5),
        )
        .await;

        let first = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        // an ipv6 client, with a noop tlv after the address block
        let second = proxy_v2_header(socket("[2001:db8::7]:4711"), &[0x04, 0x00, 0x01, 0x00]);
        let response = raw_exchange(gateway, &first, "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("198.51.100.1|http"), "{response}");
        let response = raw_exchange(gateway, &second, "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("2001:db8::7|http"), "{response}");
        // the rate-limit key is the header's source: the first client is now
        // limited, though every connection came from the same peer
        let response = raw_exchange(gateway, &first, "").await;
        assert!(response.starts_with("HTTP/1.1 429"), "{response}");
        // a local header keeps the peer, which has a bucket of its own
        let local = [&b"\r\n\r\n\0\r\nQUIT\n"[..], &[0x20, 0x00, 0x00, 0x00]].concat();
        let response = raw_exchange(gateway, &local, "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("127.0.0.1|http"), "{response}");
    }

    #[tokio::test]
    async fn forwarded_headers_count_only_from_a_trusted_proxy_header_source() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // the loopback front and an l7 proxy range behind it are trusted
        let (gateway, _shutdown) =
            spawn_proxy_protocol(state, None, "127.0.0.1/32, 192.0.2.0/24", Duration::from_secs(5))
                .await;
        let forwarded = "X-Forwarded-For: 6.6.6.6\r\nX-Forwarded-Proto: https\r\n";

        // the front is trusted, but the client its header names is not
        let header = proxy_v2_header(socket("203.0.113.5:4711"), &[]);
        let response = raw_exchange(gateway, &header, forwarded).await;
        assert!(response.contains("203.0.113.5|http"), "{response}");
        assert!(!response.contains("6.6.6.6"), "{response}");
        // a trusted proxy behind the front vouches for its client
        let header = proxy_v2_header(socket("192.0.2.10:4711"), &[]);
        let response = raw_exchange(gateway, &header, forwarded).await;
        assert!(response.contains("6.6.6.6, 192.0.2.10|https"), "{response}");
    }

    #[tokio::test]
    async fn local_proxy_header_ignores_forwarded_headers() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn_proxy_protocol(
            state,
            one_request_per_ip(),
            "127.0.0.1/32",
            Duration::from_secs(5),
        )
        .await;
        let local = [&b"\r\n\r\n\0\r\nQUIT\n"[..], &[0x20, 0x00, 0x00, 0x00]].concat();
        let forwarded =
            |client: &str| format!("X-Forwarded-For: {client}\r\nX-Forwarded-Proto: https\r\n");

        // a `PROXY` header naming the trusted front itself still lets it vouch
        // for a client, which then has a bucket of its own
        let header = proxy_v2_header(socket("127.0.0.1:4711"), &[]);
        let response = raw_exchange(gateway, &header, &forwarded("6.6.6.6")).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("6.6.6.6, 127.0.0.1|https"), "{response}");
        // under `LOCAL` the same front vouches for no one: the request is the
        // front's own, upstream and in the rate limiter
        let response = raw_exchange(gateway, &local, &forwarded("7.7.7.7")).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("127.0.0.1|http"), "{response}");
        assert!(!response.contains("7.7.7.7"), "{response}");
        // so another forwarded address cannot pick a fresh bucket: the front's
        // one token is spent
        let response = raw_exchange(gateway, &local, &forwarded("8.8.8.8")).await;
        assert!(response.starts_with("HTTP/1.1 429"), "{response}");
    }

    #[tokio::test]
    async fn proxy_protocol_header_from_an_untrusted_peer_is_not_parsed() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // some proxies are trusted, but not the loopback peer the test connects from
        let (gateway, _shutdown) =
            spawn_proxy_protocol(state, None, "10.0.0.0/8", Duration::from_secs(5)).await;

        // the header is not read, so hyper takes it for a broken request line
        let header = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let response = raw_exchange(gateway, &header, "").await;
        assert!(response.is_empty() || response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(!response.contains("198.51.100.1"), "{response}");
        // the same peer's plain HTTP is served under its own address
        let response = raw_exchange(gateway, b"", "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("127.0.0.1|http"), "{response}");
    }

    #[tokio::test]
    async fn proxy_protocol_from_peer_must_send_a_header_without_being_trusted() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // the loopback peer must send a PROXY header, but only an l7 range
        // behind it is a trusted proxy
        let (gateway, _shutdown) = spawn_proxy_protocol_from(
            state,
            None,
            "192.0.2.0/24",
            "127.0.0.1/32",
            Duration::from_secs(5),
        )
        .await;
        let forwarded = "X-Forwarded-For: 6.6.6.6\r\nX-Forwarded-Proto: https\r\n";

        // without a header the connection is closed without a response
        assert_eq!(raw_exchange(gateway, b"", forwarded).await, "");
        // with one, the source it names is resolved as before: an untrusted
        // source is the client whatever it forwards
        let header = proxy_v2_header(socket("203.0.113.5:4711"), &[]);
        let response = raw_exchange(gateway, &header, forwarded).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("203.0.113.5|http"), "{response}");
        assert!(!response.contains("6.6.6.6"), "{response}");
        // and a trusted source vouches for its client
        let header = proxy_v2_header(socket("192.0.2.10:4711"), &[]);
        let response = raw_exchange(gateway, &header, forwarded).await;
        assert!(response.contains("6.6.6.6, 192.0.2.10|https"), "{response}");
    }

    #[tokio::test]
    async fn trusted_proxy_outside_proxy_protocol_from_is_served_as_http() {
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // the loopback peer is a trusted proxy, but not one that sends PROXY
        // headers
        let (gateway, _shutdown) = spawn_proxy_protocol_from(
            state,
            None,
            "127.0.0.1/32",
            "10.0.0.0/8",
            Duration::from_secs(5),
        )
        .await;

        // served without a header, and its `X-Forwarded-For` is believed
        let forwarded = "X-Forwarded-For: 6.6.6.6\r\nX-Forwarded-Proto: https\r\n";
        let response = raw_exchange(gateway, b"", forwarded).await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("6.6.6.6, 127.0.0.1|https"), "{response}");
    }

    #[tokio::test]
    async fn malformed_proxy_header_closes_the_connection() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) =
            spawn_proxy_protocol(state, None, "127.0.0.1/32", Duration::from_secs(5)).await;

        let valid = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let mut bad_version = valid.clone();
        bad_version[12] = 0x11;
        let mut udp = valid.clone();
        udp[13] = 0x12;
        let mut short_block = valid.clone();
        short_block[15] = 4;
        for (case, preamble) in [
            ("missing", Vec::new()),
            ("v1 text header", b"PROXY TCP4 198.51.100.1 127.0.0.1 4711 8545\r\n".to_vec()),
            ("version 1", bad_version),
            ("udp", udp),
            ("short address block", short_block),
        ] {
            // closed without a response
            assert_eq!(raw_exchange(gateway, &preamble, "").await, "", "{case}");
        }
        assert_eq!(worker_seen.hits(), 0);
        // a well-formed header on the same listener is served
        let response = raw_exchange(gateway, &valid, "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn bad_proxy_headers_are_counted_and_warned_once_per_window() {
        const WARNING: &str = "missing, malformed or late PROXY protocol header";
        if !process_per_test() {
            eprintln!("skipped: {THROTTLED_WARNING_SKIP}");
            return;
        }
        let mut metrics = CapturedMetrics::install();
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) =
            spawn_proxy_protocol(state, None, "127.0.0.1/32", Duration::from_millis(300)).await;
        let mut refused = || {
            metrics.value(
                "tn_worker_gateway_connections_refused_total",
                &[("reason", REFUSED_PROXY_HEADER)],
            )
        };

        // a missing header (plain http), a malformed one, and a late one (half
        // a header, then nothing until the header read timeout)
        assert_eq!(raw_exchange(gateway, b"", "").await, "");
        let v1 = b"PROXY TCP4 198.51.100.1 127.0.0.1 4711 8545\r\n";
        assert_eq!(raw_exchange(gateway, v1, "").await, "");
        let header = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let mut late = TcpStream::connect(gateway).await.expect("connect");
        late.write_all(&header[..8]).await.expect("write");
        let read = tokio::time::timeout(Duration::from_secs(5), late.read(&mut [0_u8; 1]))
            .await
            .expect("closed at the header read timeout");
        assert_eq!(read.expect("read"), 0, "expected EOF");
        assert_eq!(refused(), 3.0);
        // the throttle lets one warning through per window, naming the peer
        // and why
        let text = logs.contents();
        assert_eq!(text.matches(WARNING).count(), 1, "{text}");
        let line = text.lines().find(|line| line.contains(WARNING)).expect("the warning");
        assert!(line.contains("WARN"), "{line}");
        assert!(line.contains("peer_addr=127.0.0.1:"), "{line}");
        assert!(line.contains("missing the PROXY protocol v2 signature"), "{line}");
        assert!(line.contains("suppressed=0"), "{line}");

        // a peer that connects and closes without a byte (a tcp connect check)
        // is neither counted nor warned about
        drop(TcpStream::connect(gateway).await.expect("connect"));
        // a good header is served, and the closed connection was seen by then
        let response = raw_exchange(gateway, &header, "").await;
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert_eq!(refused(), 3.0);
        let text = logs.contents();
        assert_eq!(text.matches(WARNING).count(), 1, "{text}");
        assert!(text.contains("connection closed before its PROXY protocol header"), "{text}");
    }

    #[tokio::test]
    async fn slow_proxy_header_neither_stalls_accept_nor_outlives_the_header_timeout() {
        let header_read_timeout = Duration::from_secs(2);
        let (worker, _worker) = forwarding_echo().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) =
            spawn_proxy_protocol(state, None, "127.0.0.1/32", header_read_timeout).await;

        // a trusted peer sends half a header and stalls; the clock starts
        // just before it connects, because the header read timeout is armed
        // when the connection's task first runs, which can come before the
        // write returns
        let header = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let started = tokio::time::Instant::now();
        let mut stalled = TcpStream::connect(gateway).await.expect("connect");
        stalled.write_all(&header[..8]).await.expect("write");
        // the header is read on the stalled connection's own task, so another
        // connection is served well before the stalled one times out
        let response =
            tokio::time::timeout(Duration::from_secs(1), raw_exchange(gateway, &header, ""))
                .await
                .expect("accept must not wait for the stalled header");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");

        // and the stalled connection is closed at the header read timeout, not
        // before it and not at some later bound. Once the test has no other
        // socket to wait on, the clock is paused (see
        // [`drain_during_proxy_headers`]) and stepped to just before the
        // timeout and to a margin past it; the timeout was armed after
        // `started`, so it fires in between. The socket is checked without
        // waiting on it: a paused clock skips ahead to the next pending timer
        // whenever the runtime parks, so a read that waits could be answered
        // by a later bound. `yield_now` lets the runtime take in the socket's
        // events without moving the clock.
        tokio::time::sleep(SETTLE).await;
        tokio::time::pause();
        tokio::time::sleep_until(started + header_read_timeout - Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        let before = stalled.try_read(&mut [0_u8; 1]);
        assert!(
            matches!(&before, Err(err) if err.kind() == std::io::ErrorKind::WouldBlock),
            "closed before the header read timeout: {before:?}"
        );
        tokio::time::sleep_until(started + header_read_timeout + Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        let after = stalled.try_read(&mut [0_u8; 1]);
        assert!(matches!(after, Ok(0)), "still open 500ms past the header read timeout: {after:?}");
    }

    /// The accept loop for the probe-only `/health` router with PROXY protocol
    /// read from the loopback peer, `header_read_timeout` for the header,
    /// lifetime cap `cap` and drain deadline `graceful_timeout`, on a fresh
    /// ephemeral port: the address, the shutdown `Notifier` and the loop's
    /// handle.
    async fn spawn_proxy_header_loop(
        header_read_timeout: Duration,
        cap: Duration,
        graceful_timeout: Duration,
    ) -> (SocketAddr, Notifier, JoinHandle<Result<(), TaskError>>) {
        let trusted = Arc::new("127.0.0.1/32".parse::<TrustedProxies>().expect("trusted"));
        let limits = ServerLimits {
            header_read_timeout,
            max_connection_duration: Some(cap),
            proxy_protocol: Some(Arc::clone(&trusted)),
            ..test_limits()
        };
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = Notifier::new();
        let accept = tokio::spawn(accept_loop(
            listener,
            probe_router(test_state(&[])),
            limits,
            trusted,
            Arc::default(),
            graceful_timeout,
            shutdown.subscribe(),
        ));
        (addr, shutdown, accept)
    }

    /// How long the real-clock part of a PROXY header test waits for the
    /// gateway to act on what the test just sent.
    const SETTLE: Duration = Duration::from_millis(50);

    /// Run a shutdown while two connections are halfway through their PROXY
    /// header: one completes it after the notice and sends a request, the
    /// other stalls. Returns how long after the connections opened the accept
    /// loop's drain ended, having checked that the completed one was served
    /// and that the drain waited for the stalled one.
    ///
    /// The clock is paused only once the test has no socket of its own left
    /// to wait on. A paused clock jumps to the next pending timer whenever the
    /// runtime parks, and waiting on a socket parks it, so pausing before the
    /// exchange would fire the stalled header's timeout while the test's own
    /// bytes were still in flight. After the pause, the first of the loop's
    /// timers to run out fires at its exact deadline.
    async fn drain_during_proxy_headers(header_read_timeout: Duration, cap: Duration) -> Duration {
        // far longer than either bound, so only they can end the drain
        const GRACE: Duration = Duration::from_secs(600);
        let (addr, shutdown, accept) =
            spawn_proxy_header_loop(header_read_timeout, cap, GRACE).await;
        let header = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let started = tokio::time::Instant::now();
        let mut stalled = TcpStream::connect(addr).await.expect("connect");
        stalled.write_all(&header[..8]).await.expect("write");
        let mut completing = TcpStream::connect(addr).await.expect("connect");
        completing.write_all(&header[..8]).await.expect("write");
        // the loop accepts both and starts reading their headers
        tokio::time::sleep(SETTLE).await;

        shutdown.notify();
        tokio::time::sleep(SETTLE).await;
        assert!(!accept.is_finished(), "the drain must wait for the header reads");

        // a header completed after the notice is served, request and all
        let request = b"GET /health HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n";
        completing.write_all(&[&header[8..], &request[..]].concat()).await.expect("write");
        let mut response = Vec::new();
        completing.read_to_end(&mut response).await.expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(!accept.is_finished(), "the stalled header still holds the drain");

        // the stalled one is closed by the first bound to run out, and the
        // drain ends with it
        tokio::time::pause();
        let read = stalled.read(&mut [0_u8; 1]).await.expect("read");
        assert_eq!(read, 0, "expected EOF from the server");
        accept.await.expect("join").expect("accept loop");
        let ended = started.elapsed();
        tokio::time::resume();
        ended
    }

    /// The drain takes a connection still reading its PROXY header into
    /// account (its receiver is taken at accept) but never cuts the read: it
    /// ends when the header read timeout does, or the lifetime cap if that
    /// comes first. Both bound the read from accept, so with the clock paused
    /// for the wait the drain ends at the bound to within the connect time.
    #[tokio::test]
    async fn shutdown_waits_for_a_proxy_header_until_its_bound() {
        let margin = Duration::from_millis(500);
        let header_read = Duration::from_secs(5);
        let ended = drain_during_proxy_headers(header_read, Duration::from_secs(30)).await;
        assert!(ended >= header_read && ended < header_read + margin, "{ended:?}");
        // a lifetime cap shorter than the header read timeout ends it sooner
        // (startup validation rules that out; the loop does not rely on it)
        let cap = Duration::from_secs(3);
        let ended = drain_during_proxy_headers(Duration::from_secs(30), cap).await;
        assert!(ended >= cap && ended < cap + margin, "{ended:?}");
    }

    /// The clock is paused once the half header has been read, as in
    /// [`drain_during_proxy_headers`], so the cap fires at its exact deadline.
    #[tokio::test]
    async fn lifetime_cap_closes_a_connection_still_reading_its_proxy_header() {
        const CAP_LINE: &str = "connection exceeded max lifetime; closing";
        let mut metrics = CapturedMetrics::install();
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let cap = Duration::from_secs(5);
        let (addr, _shutdown, _accept) =
            spawn_proxy_header_loop(Duration::from_secs(60), cap, GRACEFUL_TIMEOUT).await;

        let header = proxy_v2_header(socket("198.51.100.1:4711"), &[]);
        let started = tokio::time::Instant::now();
        let mut stalled = TcpStream::connect(addr).await.expect("connect");
        stalled.write_all(&header[..8]).await.expect("write");
        tokio::time::sleep(SETTLE).await;
        tokio::time::pause();
        let read = stalled.read(&mut [0_u8; 1]).await.expect("read");
        assert_eq!(read, 0, "expected EOF from the server");
        let elapsed = started.elapsed();
        assert!(elapsed >= cap && elapsed < cap + Duration::from_millis(500), "{elapsed:?}");

        // the cap's own debug line, naming the peer only: no client is known
        // before the header is read
        let text = logs.contents();
        let line = text.lines().find(|line| line.contains(CAP_LINE)).expect("the cap line");
        assert!(line.contains("DEBUG"), "{line}");
        assert!(line.contains("peer_addr=127.0.0.1:"), "{line}");
        assert!(!line.contains("client_addr"), "{line}");
        // the cap is not a header failure
        assert!(!text.contains("PROXY protocol header from a peer"), "{text}");
        let refused = metrics.value(
            "tn_worker_gateway_connections_refused_total",
            &[("reason", REFUSED_PROXY_HEADER)],
        );
        assert_eq!(refused, 0.0);
    }

    /// Gateway state over `upstreams`, every one marked ready, using the
    /// production proxy client and passive health at `rpc_failure` failed
    /// forwards in a row.
    fn passive_health_state(upstreams: &[UpstreamWorker], rpc_failure: u32) -> AppState {
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(5),
        )
        .expect("client");
        let thresholds =
            ReadinessThresholds { rpc_failure: Some(nz(rpc_failure)), ..test_thresholds() };
        let readiness = Arc::new(GatewayReadiness::new(upstreams, thresholds));
        upstreams.iter().for_each(|upstream| readiness.set_ready(upstream.worker_id, true));
        AppState { readiness, ..test_state_with_client(upstreams, client) }
    }

    /// Worker 0 with a closed RPC port (nothing listens on port 1), then
    /// worker 1 at `second`.
    fn dead_then_live(second: SocketAddr) -> [UpstreamWorker; 2] {
        [
            upstream("127.0.0.1:1".parse().expect("addr")),
            UpstreamWorker { worker_id: 1, ..upstream(second) },
        ]
    }

    async fn ready_status(gateway: SocketAddr) -> StatusCode {
        Client::new().get(format!("http://{gateway}/ready")).send().await.expect("send").status()
    }

    #[tokio::test]
    async fn rpc_connection_failures_mark_the_upstream_not_ready() {
        // nothing listens on port 1, so every forward fails to connect while
        // the upstream stays marked ready by its (unpolled) health listener
        let state = passive_health_state(&[upstream("127.0.0.1:1".parse().expect("addr"))], 2);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(1)));
        assert_eq!(
            ready_status(gateway).await,
            StatusCode::OK,
            "one failure is below the threshold"
        );

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(2)));
        assert_eq!(ready_status(gateway).await, StatusCode::SERVICE_UNAVAILABLE);

        // with no ready upstream left, the next request is refused unsent
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 3)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text), (-32000, serde_json::json!(3)));
    }

    #[tokio::test]
    async fn requests_reach_the_second_upstream_after_the_threshold() {
        let (second, second_seen, _second) = named_mock("second").await;
        let state = passive_health_state(&dead_then_live(second), 2);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // configured order sends the first two requests to the dead worker
        for id in 1..=2 {
            let (status, _) = post_rpc(gateway, None, call("eth_chainId", id)).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "request {id}");
        }

        for id in 3..=4 {
            let (status, text) = post_rpc(gateway, None, call("eth_chainId", id)).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "second"), "request {id}");
        }
        assert_eq!(second_seen.hits(), 2);
        assert_eq!(ready_status(gateway).await, StatusCode::OK, "the second upstream is ready");
    }

    #[tokio::test]
    async fn a_failed_request_is_not_retried_on_another_upstream() {
        let (second, second_seen, _second) = named_mock("second").await;
        let state = passive_health_state(&dead_then_live(second), 2);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // each failing request, including the one that reaches the threshold,
        // is answered with its own error and never resent to the live worker
        for id in 1..=2 {
            let (status, text) = post_rpc(gateway, None, call("eth_chainId", id)).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "request {id}");
            assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(id)));
            assert_eq!(second_seen.hits(), 0, "request {id} was retried on the second upstream");
        }

        // only a request sent after the mark goes to the second upstream
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 3)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "second"));
        assert_eq!(second_seen.hits(), 1);
    }

    #[tokio::test]
    async fn slow_worker_responses_do_not_mark_the_upstream() {
        // the worker accepts every request but answers after the proxy
        // client's deadline, as it does for eth_sendRawTransactionSync while
        // a transaction waits for inclusion
        let slow = Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let (worker, _worker) = spawn(slow).await;
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let readiness = Arc::new(GatewayReadiness::new(
            &[upstream(worker)],
            ReadinessThresholds { rpc_failure: Some(nz(1)), ..test_thresholds() },
        ));
        readiness.set_ready(0, true);
        let state = AppState { readiness, ..test_state_with_client(&[upstream(worker)], client) };
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        for id in 1..=3 {
            let (status, _) = post_rpc(gateway, None, call("eth_sendRawTransactionSync", id)).await;
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "request {id}");
        }
        assert_eq!(ready_status(gateway).await, StatusCode::OK, "a timeout must not mark");
    }

    #[tokio::test]
    async fn a_forward_that_gets_a_response_restarts_the_failure_run() {
        // reserve a loopback port, then free it so forwards to it are refused
        let addr = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind")
            .local_addr()
            .expect("local addr");
        // no idle connections, so every forward opens a new one and the third
        // is refused rather than sent on the second's connection
        let client = Client::builder()
            .pool_max_idle_per_host(0)
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client");
        let readiness = Arc::new(GatewayReadiness::new(
            &[upstream(addr)],
            ReadinessThresholds { rpc_failure: Some(nz(2)), ..test_thresholds() },
        ));
        readiness.set_ready(0, true);
        let state = AppState { readiness, ..test_state_with_client(&[upstream(addr)], client) };
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, _) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "nothing should listen on the freed port {addr}; another process may have taken it"
        );

        // the worker answers once on the same port, then goes away again
        let listener = TcpListener::bind(addr).await.unwrap_or_else(|err| {
            panic!("rebinding the freed port {addr} failed, another process took it: {err}")
        });
        let worker = tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", post(|| async { "ok" }))).await
        });
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "ok"));
        worker.abort();
        let _ = worker.await;

        // one refused forward after the response: the run restarted, so the
        // count is 1, below the threshold of 2
        let (status, _) = post_rpc(gateway, None, call("eth_chainId", 3)).await;
        assert_eq!(
            status,
            StatusCode::BAD_GATEWAY,
            "nothing should listen on the freed port {addr} again; another process may have \
             taken it"
        );
        assert_eq!(ready_status(gateway).await, StatusCode::OK, "the response reset the count");
    }

    /// A redirecting gateway whose query route is capped and saturated: every
    /// query slot is held by a read that the query upstream accepted and will
    /// never answer. The worker is ready and answers `worker`.
    struct SaturatedQueries {
        gateway: SocketAddr,
        worker_seen: Seen,
        /// Reads the never-answering query upstream has received.
        query_hits: Arc<AtomicUsize>,
        /// Keeps the gateway and both mocks alive.
        _servers: [Notifier; 3],
    }

    /// Start a [`SaturatedQueries`] with a query cap of `cap` and wait until
    /// `cap` stalled reads hold every slot.
    async fn saturated_query_route(cap: usize) -> SaturatedQueries {
        let (worker, worker_seen, worker_server) = named_mock("worker").await;
        let query_hits = Arc::new(AtomicUsize::new(0));
        let hits = Arc::clone(&query_hits);
        let stalled = Router::new().route(
            "/",
            post(move || {
                let hits = Arc::clone(&hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    future::pending::<()>().await;
                    "never"
                }
            }),
        );
        let (query, query_server) = spawn(stalled).await;

        let mut state = redirect_state(worker, Some(query));
        state.query_slots = inflight_slots(cap);
        state.readiness.set_ready(0, true);
        let query_slots = Arc::clone(&state.query_slots);
        let (gateway, gateway_server) = spawn(test_router(state)).await;

        for (_, id) in (0..cap).zip(100_u64..) {
            tokio::spawn(post_rpc(gateway, None, call("eth_getLogs", id)));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while query_slots.available_permits() > 0 || query_hits.load(Ordering::SeqCst) < cap {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled reads should take every query slot and reach the query upstream");

        SaturatedQueries {
            gateway,
            worker_seen,
            query_hits,
            _servers: [worker_server, query_server, gateway_server],
        }
    }

    /// The issue's scenario: a submission is answered while stalled reads hold
    /// every query slot. It passes on a gateway without class caps too, since
    /// four stalled reads never fill the connection cap;
    /// `submissions_succeed_while_stalled_reads_exceed_the_connection_cap` is
    /// the test that proves the isolation.
    #[tokio::test]
    async fn submissions_succeed_while_query_route_is_saturated() {
        let saturated = saturated_query_route(4).await;

        // the submission has its own slots, so the four stalled reads cannot
        // hold it back
        let (status, text) = tokio::time::timeout(
            Duration::from_secs(1),
            post_rpc(saturated.gateway, None, call("eth_sendRawTransaction", 1)),
        )
        .await
        .expect("a submission must not wait behind a saturated query route");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(saturated.worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn submissions_succeed_while_stalled_reads_exceed_the_connection_cap() {
        let (worker, worker_seen, _worker_server) = named_mock("worker").await;
        let (query, _query_server) =
            spawn(Router::new().route("/", post(future::pending::<&'static str>))).await;
        let mut state = redirect_state(worker, Some(query));
        state.query_slots = inflight_slots(4);
        state.readiness.set_ready(0, true);
        // six connections: stalled reads could hold all of them, so only the
        // query cap keeps two free for the submission
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(6).expect("nonzero"),
            ..test_limits()
        };
        let (gateway, _gateway_server) = spawn_with_limits(test_router(state), limits).await;

        // twelve reads: four take the query slots and stall, and the other
        // eight must be refused rather than wait for a slot (or a connection)
        let (done, mut refused) = tokio::sync::mpsc::unbounded_channel();
        for id in 100_u64..112 {
            let done = done.clone();
            tokio::spawn(async move {
                let outcome = post_rpc(gateway, None, call("eth_getLogs", id)).await;
                let _ = done.send(outcome);
            });
        }
        for _ in 0..8 {
            let (status, text) = tokio::time::timeout(Duration::from_secs(5), refused.recv())
                .await
                .expect("over-cap reads must be refused, not queued")
                .expect("outcome");
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
            assert_eq!(error_code_and_id(&text).0, -32009);
        }

        let (status, text) = tokio::time::timeout(
            Duration::from_secs(1),
            post_rpc(gateway, None, call("eth_sendRawTransaction", 1)),
        )
        .await
        .expect("a submission must not wait behind stalled reads");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn query_slot_is_held_until_the_response_body_ends() {
        use futures::StreamExt as _;

        let (worker, _worker_seen, _worker_server) = named_mock("worker").await;
        // a query upstream that sends its head and a first chunk, then stalls
        let (query, _query_server) = spawn(Router::new().route(
            "/",
            post(|| async {
                let first = futures::stream::iter([Ok::<_, std::convert::Infallible>("partial")]);
                let body = axum::body::Body::from_stream(first.chain(futures::stream::pending()));
                ([(header::CONTENT_TYPE, "application/json")], body)
            }),
        ))
        .await;
        // the proxy client's 3s deadline is what ends the stalled body
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(3),
        )
        .expect("client");
        let mut state = redirect_state_with_client(worker, Some(query), client);
        state.query_slots = inflight_slots(1);
        let query_slots = Arc::clone(&state.query_slots);
        let (gateway, _gateway_server) = spawn(test_router(state)).await;

        let mut response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        let first = response.chunk().await.expect("first chunk");
        assert_eq!(first.as_deref(), Some(&b"partial"[..]));

        // the handler has returned, but the body is still streaming, so the
        // read keeps its slot and a second read is refused
        assert_eq!(query_slots.available_permits(), 0);
        let (status, text) = post_rpc(gateway, None, call("eth_getLogs", 2)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text), (-32009, serde_json::json!(2)));

        // the deadline cuts the stalled body off, which frees the slot
        let rest = tokio::time::timeout(Duration::from_secs(10), response.chunk())
            .await
            .expect("the upstream deadline should end the body");
        assert!(rest.is_err(), "a body cut off by the deadline must not end cleanly");
        tokio::time::timeout(Duration::from_secs(5), async {
            while query_slots.available_permits() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the slot should be freed once the body ends");
    }

    #[tokio::test]
    async fn over_cap_queries_get_503_overloaded_without_waiting() {
        let saturated = saturated_query_route(4).await;

        // a read over the cap is answered at once; waiting for a slot would
        // take until the upstream request timeout (5s) frees one
        let client = Client::new();
        let started = tokio::time::Instant::now();
        let response = client
            .post(format!("http://{}/", saturated.gateway))
            .body(call("eth_getLogs", 5))
            .send()
            .await
            .expect("send");
        let elapsed = started.elapsed();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let text = response.text().await.expect("text");
        assert_eq!(error_code_and_id(&text), (-32009, serde_json::json!(5)));
        assert!(elapsed < Duration::from_secs(1), "the over-cap read waited {elapsed:?}");

        // the refused read never reached either upstream
        assert_eq!(saturated.query_hits.load(Ordering::SeqCst), 4);
        assert_eq!(saturated.worker_seen.hits(), 0);
    }

    /// A worker mock that holds every request until the gate opens, tracking
    /// how many it holds at once and the most it ever held.
    struct GatedWorker {
        addr: SocketAddr,
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        /// Send `true` to answer every held request (and any later one).
        gate: tokio::sync::watch::Sender<bool>,
        /// Keeps the mock alive.
        _server: Notifier,
    }

    async fn gated_worker() -> GatedWorker {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (gate, opened) = tokio::sync::watch::channel(false);
        let (current, most) = (Arc::clone(&in_flight), Arc::clone(&peak));
        let mock = Router::new().route(
            "/",
            post(move || {
                let (current, most, mut opened) =
                    (Arc::clone(&current), Arc::clone(&most), opened.clone());
                async move {
                    let held = current.fetch_add(1, Ordering::SeqCst) + 1;
                    most.fetch_max(held, Ordering::SeqCst);
                    // a dropped sender also lets the request through
                    let _ = opened.wait_for(|open| *open).await;
                    current.fetch_sub(1, Ordering::SeqCst);
                    "worker"
                }
            }),
        );
        let (addr, server) = spawn(mock).await;
        GatedWorker { addr, in_flight, peak, gate, _server: server }
    }

    #[tokio::test]
    async fn submissions_keep_worker_slots_without_a_redirect() {
        // a worker that answers submissions and never answers anything else
        let mock = Router::new().route(
            "/",
            post(|body: String| async move {
                if !body.contains("eth_sendRawTransaction") {
                    future::pending::<()>().await;
                }
                "worker"
            }),
        );
        let (worker, _worker_server) = spawn(mock).await;
        let mut state = redirect_state(worker, None);
        // a worker cap of 5 without a redirect gives reads a cap of 4 (see
        // `effective_query_cap` in cli.rs)
        state.upstream_slots = inflight_slots(5);
        state.query_slots = inflight_slots(4);
        state.readiness.set_ready(0, true);
        let upstream_slots = Arc::clone(&state.upstream_slots);
        let (gateway, _gateway_server) = spawn(test_router(state)).await;

        for id in 100_u64..104 {
            tokio::spawn(post_rpc(gateway, None, call("eth_getLogs", id)));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while upstream_slots.available_permits() > 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the four stalled reads should hold four worker slots");

        // a fifth read is over the query cap and is refused at once
        let (status, text) = tokio::time::timeout(
            Duration::from_secs(1),
            post_rpc(gateway, None, call("eth_getLogs", 5)),
        )
        .await
        .expect("an over-cap read must not wait");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text), (-32009, serde_json::json!(5)));

        // reads cannot reach the last worker slot, so a submission gets it
        let (status, text) = tokio::time::timeout(
            Duration::from_secs(1),
            post_rpc(gateway, None, call("eth_sendRawTransaction", 1)),
        )
        .await
        .expect("a submission must not wait behind stalled reads");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
    }

    #[tokio::test]
    async fn worker_never_sees_more_than_max_upstream_inflight() {
        let worker = gated_worker().await;
        let mut state = redirect_state(worker.addr, None);
        state.upstream_slots = inflight_slots(2);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // ten concurrent submissions, each reporting its outcome as it lands
        let (done, mut outcomes) = tokio::sync::mpsc::unbounded_channel();
        let client = Client::new();
        for id in 1..=10_u64 {
            let request =
                client.post(format!("http://{gateway}/")).body(call("eth_sendRawTransaction", id));
            let done = done.clone();
            tokio::spawn(async move {
                let response = request.send().await.expect("send");
                let status = response.status();
                let _ = done.send((status, response.text().await.expect("text"), id));
            });
        }

        // the worker answers nothing until the gate opens, so the eight
        // submissions over the cap can only land now if they did not wait
        // for a slot
        for _ in 0..8 {
            let (status, text, id) = tokio::time::timeout(Duration::from_secs(5), outcomes.recv())
                .await
                .expect("an over-cap submission must not wait for a worker slot")
                .expect("outcome");
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
            assert_eq!(error_code_and_id(&text), (-32009, serde_json::json!(id)));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker.in_flight.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the two admitted submissions should reach the worker");
        assert_eq!(worker.peak.load(Ordering::SeqCst), 2);

        // opening the gate answers the two that held a slot
        worker.gate.send_replace(true);
        for _ in 0..2 {
            let (status, text, _) = tokio::time::timeout(Duration::from_secs(5), outcomes.recv())
                .await
                .expect("the admitted submissions should be answered")
                .expect("outcome");
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        }
        assert!(worker.peak.load(Ordering::SeqCst) <= 2);
    }

    /// A mock upstream that answers `name` after `delay`.
    async fn slow_mock(name: &'static str, delay: Duration) -> (SocketAddr, Notifier) {
        spawn(Router::new().route(
            "/",
            post(move || async move {
                tokio::time::sleep(delay).await;
                name
            }),
        ))
        .await
    }

    #[tokio::test]
    async fn query_route_times_out_at_query_request_timeout() {
        // both upstreams take 1s; the client's own deadline is 5s
        let (worker, _worker) = slow_mock("worker", Duration::from_secs(1)).await;
        let (query, _query) = slow_mock("query", Duration::from_secs(1)).await;
        let mut state = redirect_state(worker, Some(query));
        state.query_http = Some(
            proxy_client(
                UpstreamClientBuilder::for_test(Duration::from_secs(2)),
                Duration::from_millis(200),
            )
            .expect("client"),
        );
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // without the query deadline the read would get the late answer after
        // 1s; with it the read times out after about 200ms
        let client = Client::new();
        let started = tokio::time::Instant::now();
        let response = client
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 4))
            .send()
            .await
            .expect("send");
        let elapsed = started.elapsed();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let text = response.text().await.expect("text");
        assert_eq!(error_code_and_id(&text), (-32002, serde_json::json!(4)));
        assert!(
            elapsed >= Duration::from_millis(200) && elapsed < Duration::from_secs(1),
            "the read timed out after {elapsed:?}"
        );

        // the worker route keeps the client's 5s deadline, so the slow
        // submission still gets its answer
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 5)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
    }

    /// A gateway with a ready worker answering `worker` and a global limit of
    /// one request (no refill within the test), plus a submission budget when
    /// `submissions` is given.
    async fn one_token_gateway(
        submissions: Option<RateLimit>,
    ) -> (SocketAddr, Seen, [Notifier; 2]) {
        let (worker, worker_seen, worker_server) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            submissions,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (gateway, gateway_server) = spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;
        (gateway, worker_seen, [worker_server, gateway_server])
    }

    #[tokio::test]
    async fn submission_budget_admits_when_global_is_exhausted() {
        let (gateway, worker_seen, _servers) =
            one_token_gateway(Some(RateLimit::new(nz(1), nz(1)))).await;

        // a read spends the global bucket's only token
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        // a submission still gets through, on its own budget
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 2)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        // a second read is refused, with its id
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 3)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&text), (-32006, serde_json::json!(3)));
        assert_eq!(worker_seen.hits(), 2);
    }

    /// The reverse of the test above: submissions over their own budget are
    /// refused, with their ids, without spending the global token a read
    /// still gets.
    #[tokio::test]
    async fn submission_flood_spends_no_read_budget() {
        let (gateway, worker_seen, _servers) =
            one_token_gateway(Some(RateLimit::new(nz(1), nz(1)))).await;

        // the first submission spends the submission bucket's only token
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        // the rest of the flood is refused once classified, so the id is echoed
        for id in 2..=4 {
            let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", id)).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(error_code_and_id(&text), (-32006, serde_json::json!(id)));
        }

        // the global bucket still holds its token for a read
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 5)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_seen.hits(), 2);
    }

    /// A submission batch whose extra call the empty global bucket refuses
    /// gets its submission token back in the handler, so the submission
    /// budget is whole for the next submissions.
    #[tokio::test]
    async fn refused_submission_batch_returns_its_submission_token() {
        let (gateway, worker_seen, _servers) =
            one_token_gateway(Some(RateLimit::new(nz(1), nz(2)))).await;

        // a read spends the global bucket's only token
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        // a two-call submission batch: the first call takes a submission
        // token, the second needs a global token and is refused
        let (status, _) = post_rpc(gateway, None, batch("eth_sendRawTransaction", 2)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        // both of the budget's tokens are there for single submissions
        for id in [3, 4] {
            let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", id)).await;
            assert_eq!(
                (status, text.as_str()),
                (StatusCode::OK, "worker"),
                "the refused batch kept a submission token"
            );
        }
        assert_eq!(worker_seen.hits(), 3);
    }

    #[tokio::test]
    async fn without_a_submission_budget_the_edge_check_is_unchanged() {
        let (gateway, worker_seen, _servers) = one_token_gateway(None).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        // the edge refuses whatever the request holds, before reading its body
        // (so the id is not echoed), as `over_limit_request_gets_jsonrpc_429`
        // pins for a read
        for body in [call("eth_sendRawTransaction", 2), call("eth_chainId", 3)] {
            let (status, text) = post_rpc(gateway, None, body).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(error_code_and_id(&text), (-32006, serde_json::Value::Null));
        }
        assert_eq!(worker_seen.hits(), 1);
    }

    /// A mock upstream that answers every POST with `status`, `content_type`
    /// and `body`. The `Notifier` keeps it alive.
    async fn answering_mock(
        status: StatusCode,
        content_type: &'static str,
        body: &'static str,
    ) -> (SocketAddr, Notifier) {
        let mock = Router::new().route(
            "/",
            post(move || async move { (status, [(header::CONTENT_TYPE, content_type)], body) }),
        );
        spawn(mock).await
    }

    /// The content type of a gateway response, as a string.
    fn content_type(response: &reqwest::Response) -> Option<String> {
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    }

    /// A worker's jsonrpsee answers `403`, `415` and `429` with text/plain
    /// bodies; the client gets the status with a JSON-RPC error carrying its id.
    #[tokio::test]
    async fn text_plain_429_from_upstream_becomes_a_jsonrpc_error_with_the_id() {
        let (worker, _worker) = answering_mock(
            StatusCode::TOO_MANY_REQUESTS,
            "text/plain; charset=utf-8",
            "Too many connections",
        )
        .await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_chainId", 21))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(content_type(&response).as_deref(), Some("application/json"));
        let text = response.text().await.expect("text");
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(21)));
        assert!(text.contains("upstream returned 429 without a JSON-RPC body"), "{text}");
    }

    #[tokio::test]
    async fn json_error_body_from_upstream_passes_through_unchanged() {
        let cases = [
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "application/json",
                r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"internal"},"id":1}"#,
            ),
            (
                StatusCode::BAD_REQUEST,
                "Application/Problem+JSON; charset=utf-8",
                r#"{"title":"bad request"}"#,
            ),
        ];
        for (status, upstream_content_type, body) in cases {
            let (worker, _worker) = answering_mock(status, upstream_content_type, body).await;
            let state = redirect_state(worker, None);
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;

            let response = Client::new()
                .post(format!("http://{gateway}/"))
                .body(call("eth_chainId", 1))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), status);
            assert_eq!(content_type(&response).as_deref(), Some(upstream_content_type));
            assert_eq!(response.text().await.expect("text"), body);
        }
    }

    /// An upstream's own `408` is not the gateway's request deadline: a
    /// text/plain one gets the `-32012` envelope with the request's id, a JSON
    /// one passes through unchanged, and each is counted once.
    #[tokio::test]
    async fn upstream_408_is_not_rewritten_as_a_request_timeout() {
        let mut metrics = CapturedMetrics::install();

        let (plain, _plain) =
            answering_mock(StatusCode::REQUEST_TIMEOUT, "text/plain", "late").await;
        let state = redirect_state(plain, None);
        state.readiness.set_ready(0, true);
        let (first, _first) = spawn(test_router(state)).await;
        let (status, text) = post_rpc(first, None, call("eth_chainId", 7)).await;
        assert_eq!(status, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(7)));

        let json_body = r#"{"jsonrpc":"2.0","error":{"code":-32000,"message":"slow"},"id":8}"#;
        let (json, _json) =
            answering_mock(StatusCode::REQUEST_TIMEOUT, "application/json", json_body).await;
        let state = redirect_state(json, None);
        state.readiness.set_ready(0, true);
        let (second, _second) = spawn(test_router(state)).await;
        let (status, text) = post_rpc(second, None, call("eth_chainId", 8)).await;
        assert_eq!((status, text.as_str()), (StatusCode::REQUEST_TIMEOUT, json_body));

        let requests = "tn_worker_gateway_requests_total";
        assert_eq!(metrics.value(requests, &[("outcome", "upstream_error")]), 1.0);
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 1.0);
        assert_eq!(metrics.value(requests, &[("outcome", "rejected")]), 0.0);
        assert_eq!(
            metrics.value("tn_worker_gateway_rejections_total", &[("reason", "request_timeout")]),
            0.0
        );
    }

    #[tokio::test]
    async fn upstream_error_is_counted_under_its_own_outcome() {
        let mut metrics = CapturedMetrics::install();
        let (worker, _worker) =
            answering_mock(StatusCode::TOO_MANY_REQUESTS, "text/plain", "Too many connections")
                .await;
        let (query, _query) = answering_mock(StatusCode::FORBIDDEN, "text/plain", "denied").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(1)));
        let (status, text) = post_rpc(gateway, None, call("eth_call", 2)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(2)));

        let requests = "tn_worker_gateway_requests_total";
        assert_eq!(metrics.value(requests, &[("outcome", "upstream_error")]), 2.0);
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 0.0);
        assert_eq!(metrics.value(requests, &[("outcome", "rejected")]), 0.0);
        assert_eq!(
            metrics.value("tn_worker_gateway_rejections_total", &[("reason", "upstream_error")]),
            2.0
        );
        let routed = "tn_worker_gateway_routed_requests_total";
        for route in ["worker", "query"] {
            assert_eq!(
                metrics.value(routed, &[("route", route), ("result", "upstream_error")]),
                1.0,
                "{route}"
            );
            assert_eq!(metrics.value(routed, &[("route", route), ("result", "forwarded")]), 0.0);
        }
    }

    /// Read one HTTP/1.1 request, its head plus a `Content-Length` body, off
    /// `stream`, so that closing the stream afterwards sends a clean FIN rather
    /// than a reset over unread bytes.
    async fn read_request(stream: &mut TcpStream) {
        let mut request = Vec::new();
        let mut buf = [0_u8; 4096];
        loop {
            let read = stream.read(&mut buf).await.expect("read");
            assert!(read > 0, "the gateway closed before its request ended");
            request.extend_from_slice(&buf[..read]);
            let text = String::from_utf8_lossy(&request).to_ascii_lowercase();
            if let Some(head_end) = text.find("\r\n\r\n") {
                let length = text[..head_end]
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .and_then(|length| length.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if request.len() >= head_end + 4 + length {
                    return;
                }
            }
        }
    }

    /// A mock upstream that answers one request with `200` and a
    /// `Content-Length` of 1000, sends 34 bytes of body and closes.
    async fn truncating_mock() -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            read_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                      Content-Length: 1000\r\n\r\n{\"jsonrpc\":\"2.0\",\"result\":\"0x1\",\"i",
                )
                .await
                .expect("write");
        });
        addr
    }

    #[tokio::test]
    async fn mid_stream_upstream_failure_is_counted() {
        let mut metrics = CapturedMetrics::install();
        let state = redirect_state(truncating_mock().await, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_chainId", 1))
            .send()
            .await
            .expect("send");
        // the status went out before the body broke off, so the client gets a
        // 200 whose body cannot be read to the end
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.bytes().await.is_err(), "a truncated body must not read as complete");

        let routed = "tn_worker_gateway_routed_requests_total";
        assert_eq!(metrics.value(routed, &[("route", "worker"), ("result", "body_failed")]), 1.0);
        assert_eq!(metrics.value(routed, &[("route", "worker"), ("result", "forwarded")]), 1.0);
    }

    /// hyper's server sends a `304` without a body, so an upstream's `304`
    /// could carry no JSON-RPC answer whatever its content type: the client
    /// gets `502` and the `-32012` error with its id instead.
    #[tokio::test]
    async fn upstream_not_modified_becomes_a_bad_gateway_error() {
        for content_type in ["text/plain", "application/json"] {
            let (worker, _worker) = answering_mock(
                StatusCode::NOT_MODIFIED,
                content_type,
                r#"{"jsonrpc":"2.0","result":"0x1","id":5}"#,
            )
            .await;
            let state = redirect_state(worker, None);
            state.readiness.set_ready(0, true);
            let (gateway, _shutdown) = spawn(test_router(state)).await;

            let (status, text) = post_rpc(gateway, None, call("eth_chainId", 5)).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "{content_type}");
            assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(5)), "{content_type}");
        }
    }

    /// An upstream's `101` would have hyper's server send the client a bare
    /// protocol switch and close the connection; the client gets `502` and the
    /// `-32012` error with its id instead.
    #[tokio::test]
    async fn upstream_switching_protocols_becomes_a_bad_gateway_error() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let worker = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            read_request(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\n\
                      Upgrade: websocket\r\n\r\n",
                )
                .await
                .expect("write");
            // hold the connection until the gateway drops it
            let _ = stream.read(&mut [0_u8; 1]).await;
        });
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 6)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(6)));
    }

    /// A streamed answer keeps the upstream's `Content-Length`, so the client
    /// can tell a complete answer by its length rather than by a final chunk.
    #[tokio::test]
    async fn streamed_answer_keeps_the_upstream_content_length() {
        let answer = "a".repeat(100 * 1024);
        let sized = answer.clone();
        let mock = Router::new().route(
            "/",
            post(move || async move { ([(header::CONTENT_TYPE, "application/json")], sized) }),
        );
        let (worker, _worker) = spawn(mock).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.content_length(), Some(102_400));
        assert_eq!(response.text().await.expect("text"), answer);
    }

    /// An upstream answer without a `Content-Length` still goes out chunked.
    #[tokio::test]
    async fn chunked_upstream_answer_stays_chunked() {
        let mock = Router::new().route(
            "/",
            post(|| async {
                let pieces = futures::stream::iter(
                    [r#"{"jsonrpc":"2.0","#, r#""result":"0x1","id":1}"#]
                        .map(|piece| Ok::<_, std::io::Error>(axum::body::Bytes::from(piece))),
                );
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    axum::body::Body::from_stream(pieces),
                )
            }),
        );
        let (worker, _worker) = spawn(mock).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_getLogs", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.content_length(), None);
        assert_eq!(
            response.headers().get(header::TRANSFER_ENCODING).map(HeaderValue::as_bytes),
            Some(b"chunked".as_slice())
        );
        assert_eq!(
            response.text().await.expect("text"),
            r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#
        );
    }

    /// A relayed response stays in the in-flight gauge until its body has
    /// finished streaming, not just until the handler returns its head.
    #[tokio::test]
    async fn inflight_gauge_drops_after_the_body_streams() {
        let mut metrics = CapturedMetrics::install();
        // a mock that sends the head and a first chunk, then holds the rest of
        // the body until released
        let release = Arc::new(tokio::sync::Notify::new());
        let held = Arc::clone(&release);
        let mock = Router::new().route(
            "/",
            post(move || {
                let held = Arc::clone(&held);
                async move {
                    let head = futures::stream::once(async {
                        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(
                            br#"{"jsonrpc":"2.0","#,
                        ))
                    });
                    let tail = futures::stream::once(async move {
                        held.notified().await;
                        Ok(axum::body::Bytes::from_static(br#""result":"0x1","id":1}"#))
                    });
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        axum::body::Body::from_stream(futures::StreamExt::chain(head, tail)),
                    )
                }
            }),
        );
        let (worker, _worker) = spawn(mock).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let mut response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_chainId", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.chunk().await.expect("chunk").expect("first chunk").to_vec();

        // the handler has long returned the head, but the body still streams
        let inflight = "tn_worker_gateway_inflight_requests";
        let duration = "tn_worker_gateway_request_duration_seconds";
        assert_eq!(metrics.value(inflight, &[]), 1.0);
        assert_eq!(metrics.value(duration, &[]), 0.0, "no duration before the body ends");

        release.notify_one();
        while let Some(chunk) = response.chunk().await.expect("chunk") {
            body.extend_from_slice(&chunk);
        }
        assert_eq!(body, br#"{"jsonrpc":"2.0","result":"0x1","id":1}"#);
        // the gateway drops the body after its last frame; allow for the
        // client reading the end first
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.value(inflight, &[]) > 0.5 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the in-flight gauge drops once the body has streamed");
        assert_eq!(metrics.value(inflight, &[]), 0.0);
        assert_eq!(metrics.value(duration, &[]), 1.0, "one duration sample, at the body's end");
    }

    /// The worker slot is released at the response head on the buffered path
    /// too: a worker that sends the head of a submission's answer and then
    /// stalls holds no `--max-upstream-inflight` slot while the gateway waits
    /// for the rest, though the request is still in flight and keeps its
    /// class slot.
    #[tokio::test]
    async fn worker_slot_is_released_at_the_head_of_a_buffered_answer() {
        let mut metrics = CapturedMetrics::install();
        // a worker that sends the head and the start of an accepting answer,
        // then holds the rest until released
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (seen, held) = (Arc::clone(&entered), Arc::clone(&release));
        let mock = Router::new().route(
            "/",
            post(move || {
                let (seen, held) = (Arc::clone(&seen), Arc::clone(&held));
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    let (start, rest) = ACCEPTED.split_at(20);
                    let head = futures::stream::once(async move {
                        Ok::<_, std::io::Error>(axum::body::Bytes::from_static(start.as_bytes()))
                    });
                    let tail = futures::stream::once(async move {
                        held.notified().await;
                        Ok(axum::body::Bytes::from_static(rest.as_bytes()))
                    });
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        axum::body::Body::from_stream(futures::StreamExt::chain(head, tail)),
                    )
                }
            }),
        );
        let (worker, _worker) = spawn(mock).await;
        let (query, _query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        state.upstream_slots = inflight_slots(1);
        state.readiness.set_ready(0, true);
        let upstream_slots = Arc::clone(&state.upstream_slots);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let answer = tokio::spawn(post_rpc(gateway, None, submission(1)));
        let upstream = "tn_worker_gateway_upstream_inflight";
        tokio::time::timeout(Duration::from_secs(5), async {
            while entered.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the submission should reach the worker");
        // the head is in, the rest of the answer is not; the wait stays well
        // inside the proxy client's 5 s deadline, which would free the slot
        // by failing the request
        tokio::time::timeout(Duration::from_secs(2), async {
            while metrics.value(upstream, &[]) > 0.5 || upstream_slots.available_permits() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the worker slot must be released at the response head");
        assert!(!answer.is_finished(), "the answer is still being read whole");
        assert_eq!(metrics.value("tn_worker_gateway_inflight_requests", &[]), 1.0);
        let class = [("route", "submission")];
        assert_eq!(metrics.value("tn_worker_gateway_route_inflight", &class), 1.0);

        release.notify_one();
        let (status, text) = tokio::time::timeout(Duration::from_secs(5), answer)
            .await
            .expect("the answer completes once released")
            .expect("client task");
        assert_eq!((status, text.as_str()), (StatusCode::OK, ACCEPTED));
    }

    /// Send a request head promising a 100-byte body, and the first bytes of
    /// that body, on a raw connection; then stall or stop sending as `then`
    /// says, and return everything the gateway answers before it closes.
    async fn partial_request(gateway: SocketAddr, then: Abort) -> String {
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        stream
            .write_all(
                b"POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
                  Content-Length: 100\r\n\r\n{\"id\":",
            )
            .await
            .expect("write");
        if then == Abort::CloseWrites {
            stream.shutdown().await.expect("shutdown");
        }
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the gateway answers within the test's deadline")
            .expect("read");
        String::from_utf8_lossy(&response).into_owned()
    }

    /// What a client does after sending part of a request body.
    #[derive(Clone, Copy, PartialEq)]
    enum Abort {
        /// Stall, leaving the request deadline to fire.
        Stall,
        /// Stop sending: the body ends early.
        CloseWrites,
    }

    /// Every kind of rejection, including the ones that log nothing per
    /// request (`429`, `408` and an aborted body), is counted under its reason.
    #[tokio::test]
    async fn every_reject_kind_is_counted() {
        let mut metrics = CapturedMetrics::install();
        let (worker, _worker_seen, _worker) = named_mock("worker").await;

        // 429: a global limit of one request with no burst headroom
        let limited = redirect_state(worker, None);
        limited.readiness.set_ready(0, true);
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (limited, _limited) = spawn(router(
            limited,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;
        let (status, _) = post_rpc(limited, None, call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = post_rpc(limited, None, call("eth_chainId", 2)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

        // 408: a body that stalls past a short request deadline
        let slow = redirect_state(worker, None);
        slow.readiness.set_ready(0, true);
        let (slow, _slow) = spawn(router(
            slow,
            Duration::from_millis(300),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;
        let response = partial_request(slow, Abort::Stall).await;
        assert!(response.starts_with("HTTP/1.1 408"), "{response}");

        // the rest on a gateway whose only worker is not ready
        let (gateway, _shutdown) = spawn(test_router(redirect_state(worker, None))).await;
        // an aborted body: the client stops sending part-way through
        let response = partial_request(gateway, Abort::CloseWrites).await;
        assert!(response.starts_with("HTTP/1.1 400"), "{response}");
        assert!(response.contains("-32600"), "{response}");
        // a loop
        let (status, _) = post_rpc(gateway, Some("x-tn-gateway"), call("eth_chainId", 3)).await;
        assert_eq!(status, StatusCode::LOOP_DETECTED);
        // a redirect loop, at a gateway that redirects
        let (query, _query_seen, _query) = named_mock("query").await;
        let (redirecting, _redirecting) =
            spawn(test_router(redirect_state(worker, Some(query)))).await;
        let (status, _) =
            post_rpc(redirecting, Some("x-tn-gateway-redirect"), call("eth_chainId", 6)).await;
        assert_eq!(status, StatusCode::LOOP_DETECTED);
        // a screened transaction
        let junk =
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":4}"#;
        let (status, _) = post_rpc(gateway, None, junk.to_string()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // no upstream ready
        let (status, _) = post_rpc(gateway, None, call("eth_chainId", 5)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

        let reasons = [
            "rate_limited",
            "request_timeout",
            "unreadable_body",
            "loop_detected",
            "redirect_loop_detected",
            "invalid_transaction",
            "no_upstream_ready",
        ];
        for reason in reasons {
            assert_eq!(
                metrics.value("tn_worker_gateway_rejections_total", &[("reason", reason)]),
                1.0,
                "{reason}"
            );
        }
        let requests = "tn_worker_gateway_requests_total";
        assert_eq!(metrics.value(requests, &[("outcome", "rejected")]), reasons.len() as f64);
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 1.0);
    }

    /// `routed_requests_total{route, result}` counts every forward attempt
    /// under its route and result, and `mixed_batches_total` counts each batch
    /// sent to the query upstream only because it mixed submissions with reads.
    #[tokio::test]
    async fn routed_and_mixed_batch_counters_are_recorded() {
        let mut metrics = CapturedMetrics::install();
        let submission = call("eth_sendRawTransaction", 1);
        let read = call("eth_getLogs", 2);

        // forwarded on both routes, two of the batches mixed
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let (query, _query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        for (body, expected) in [
            (submission.clone(), "worker"),
            (format!("[{submission},{submission}]"), "worker"),
            (read.clone(), "query"),
            (format!("[{submission},{read}]"), "query"),
            (format!("[{read},{submission}]"), "query"),
        ] {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, expected), "{body}");
        }

        // unreachable on both routes: nothing listens on port 1
        let dead: SocketAddr = "127.0.0.1:1".parse().expect("addr");
        let state = redirect_state(dead, Some(dead));
        state.readiness.set_ready(0, true);
        let (unreachable, _unreachable) = spawn(test_router(state)).await;
        for body in [submission.clone(), read.clone()] {
            let (status, _) = post_rpc(unreachable, None, body).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY);
        }

        // timed out on both routes
        let slow = Router::new().route(
            "/",
            post(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let (slow, _slow) = spawn(slow).await;
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_millis(200),
        )
        .expect("client");
        let state = redirect_state_with_client(slow, Some(slow), client);
        state.readiness.set_ready(0, true);
        let (timing_out, _timing_out) = spawn(test_router(state)).await;
        for body in [submission.clone(), read.clone()] {
            let (status, _) = post_rpc(timing_out, None, body).await;
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        }

        let series = |route: &str, result: &str| {
            vec![
                ("result".to_string(), result.to_string()),
                ("route".to_string(), route.to_string()),
            ]
        };
        let mut expected = vec![
            (series("worker", "forwarded"), 2.0),
            (series("query", "forwarded"), 3.0),
            (series("worker", "unreachable"), 1.0),
            (series("query", "unreachable"), 1.0),
            (series("worker", "timeout"), 1.0),
            (series("query", "timeout"), 1.0),
        ];
        expected.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(metrics.series("tn_worker_gateway_routed_requests_total"), expected);
        assert_eq!(metrics.value("tn_worker_gateway_mixed_batches_total", &[]), 2.0);
    }

    #[tokio::test]
    async fn probes_answer_while_client_connections_hold_every_permit() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(4).expect("nonzero"),
            ..test_limits()
        };
        let (gateway_addr, _shutdown) = spawn_with_limits(test_router(state.clone()), limits).await;
        let (probe_addr, _probes) = spawn_probes(state).await;

        // four client connections that send nothing hold every permit until
        // the 5s header deadline
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(TcpStream::connect(gateway_addr).await.expect("connect"));
        }
        let client = Client::builder().timeout(Duration::from_secs(1)).build().expect("client");

        // the client listener's cap is full, so a probe sent there waits in
        // the accept backlog
        let blocked = client.get(format!("http://{gateway_addr}{HEALTH_PATH}")).send().await;
        assert!(blocked.is_err(), "the held connections should hold every permit");

        for path in [HEALTH_PATH, READY_PATH] {
            let response = client
                .get(format!("http://{probe_addr}{path}"))
                .send()
                .await
                .unwrap_or_else(|err| panic!("{path} on the probe listener: {err}"));
            assert_eq!(response.status(), StatusCode::OK, "{path}");
        }
        drop(held);
    }

    /// The probe listener marks its responses `nosniff` like the client
    /// listener, a probe's answer and a `404` alike.
    #[tokio::test]
    async fn probe_listener_responses_carry_nosniff() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (probe_addr, _probes) = spawn_probes(state).await;

        for path in [READY_PATH, "/not-a-probe"] {
            let response =
                Client::new().get(format!("http://{probe_addr}{path}")).send().await.expect("send");
            assert_eq!(
                response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
                Some(&HeaderValue::from_static("nosniff")),
                "{path} answered {}",
                response.status()
            );
        }
    }

    #[tokio::test]
    async fn probe_connection_serves_one_request_with_a_small_head() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (probe_addr, _probes) = spawn_probes(state).await;

        // a keep-alive request still gets one response and then EOF, long
        // before the 5s header deadline would close an idle connection
        let mut stream = TcpStream::connect(probe_addr).await.expect("connect");
        stream
            .write_all(format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: probe\r\n\r\n").as_bytes())
            .await
            .expect("write");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .expect("the probe connection should close after one response")
            .expect("read");
        let response = String::from_utf8_lossy(&response);
        assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");

        // a head well over 8 KiB (one long header value) is refused, never
        // served
        let mut stream = TcpStream::connect(probe_addr).await.expect("connect");
        let padding = "a".repeat(32 * 1024);
        let head =
            format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: probe\r\nX-Padding: {padding}\r\n\r\n");
        // the server may refuse the head and close before all of it is written
        let _ = stream.write_all(head.as_bytes()).await;
        let mut response = Vec::new();
        // a reset after the refusal is as good as a clean close; the bytes read
        // before it stay in `response`
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
            .await
            .expect("the oversized head should end the connection");
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.is_empty() || response.starts_with("HTTP/1.1 431"),
            "expected 431 or a closed connection, got: {response}"
        );
    }

    #[tokio::test]
    async fn probe_listener_is_not_rate_limited() {
        let (worker, _seen, _worker) = named_mock("worker").await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        // one request per second per IP and in total, no burst headroom
        let limiters = RateLimiters::new(
            Some(RateLimit::new(nz(1), nz(1))),
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (gateway_addr, _shutdown) = spawn(router(
            state.clone(),
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;
        let (probe_addr, _probes) = spawn_probes(state).await;

        // a client request spends the bucket; the next one is refused
        let (first, _) = post_rpc(gateway_addr, None, call("eth_chainId", 1)).await;
        assert_eq!(first, StatusCode::OK);
        let (second, body) = post_rpc(gateway_addr, None, call("eth_chainId", 2)).await;
        assert_eq!(second, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&body).0, -32006);

        // the probe listener, from the same address, still answers every probe
        // no limiter by construction; the global bucket and /ready/any would catch one being added
        let client = Client::new();
        for _ in 0..3 {
            for path in [HEALTH_PATH, READY_PATH, READY_ANY_PATH] {
                let response =
                    client.get(format!("http://{probe_addr}{path}")).send().await.expect("send");
                assert_eq!(response.status(), StatusCode::OK, "{path}");
            }
        }
    }

    #[tokio::test]
    async fn probe_listener_serves_no_rpc() {
        let (worker, seen, _worker) = named_mock("worker").await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        let (probe_addr, _probes) = spawn_probes(state).await;

        let (status, _) = post_rpc(probe_addr, None, call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(seen.hits(), 0, "the probe listener must never reach an upstream");
    }

    /// A query upstream's answer to the poller's `eth_chainId` probe.
    const CHAIN_ID_REPLY: &str = r#"{"jsonrpc":"2.0","id":1,"result":"0x7e1"}"#;

    /// Run the real readiness poller over `state` every 50ms, probing its query
    /// upstream (if any) with its proxy client, as `app::run` wires it. The
    /// returned `Notifier` keeps the poller running.
    fn spawn_poller(state: &AppState) -> Notifier {
        let shutdown = Notifier::new();
        let query_probe = state.query_upstream.clone().map(|url| QueryProbe {
            url,
            client: state.http.clone(),
            query_header: state.query_header.clone(),
        });
        tokio::spawn(run_poller(
            Arc::clone(&state.readiness),
            Client::new(),
            query_probe,
            Duration::from_millis(50),
            Duration::from_secs(1),
            shutdown.subscribe(),
        ));
        shutdown
    }

    /// GET a probe path, returning its status and JSON body.
    async fn get_probe(addr: SocketAddr, path: &str) -> (StatusCode, serde_json::Value) {
        let response =
            Client::new().get(format!("http://{addr}{path}")).send().await.expect("send");
        (response.status(), response.json().await.expect("json body"))
    }

    /// GET `path` until it answers `status`, returning that body; fails after
    /// five seconds.
    async fn await_probe_status(
        addr: SocketAddr,
        path: &str,
        status: StatusCode,
    ) -> serde_json::Value {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let (got, body) = get_probe(addr, path).await;
                if got == status {
                    break body;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{path} never answered {status}"))
    }

    #[tokio::test]
    async fn ready_any_is_200_when_only_the_query_upstream_is_up() {
        // the worker mock serves no readiness route, so its poll fails
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let (query, _query_seen, _query) = named_mock(CHAIN_ID_REPLY).await;
        let state = redirect_state(worker, Some(query));
        let _poller = spawn_poller(&state);
        let (gateway_addr, _shutdown) = spawn(test_router(state.clone())).await;
        let (probe_addr, _probes) = spawn_probes(state).await;

        let body = await_probe_status(probe_addr, READY_ANY_PATH, StatusCode::OK).await;
        assert_eq!(body, serde_json::json!({"ready": true, "submissions": false, "queries": true}));
        for addr in [gateway_addr, probe_addr] {
            assert_eq!(get_probe(addr, READY_ANY_PATH).await.0, StatusCode::OK);
            let (status, body) = get_probe(addr, READY_PATH).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(body, serde_json::json!({"ready": false}));
        }
    }

    #[tokio::test]
    async fn ready_any_is_503_when_neither_is_up() {
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        // the query upstream answers, but with an error rather than a result
        let (query, query_seen, _query) = named_mock(
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"internal error"}}"#,
        )
        .await;
        let state = redirect_state(worker, Some(query));
        let _poller = spawn_poller(&state);
        let (gateway_addr, _shutdown) = spawn(test_router(state.clone())).await;
        let (probe_addr, _probes) = spawn_probes(state).await;

        // the poller probes one cycle at a time, so a second probe means the
        // first one's answer has been recorded
        tokio::time::timeout(Duration::from_secs(5), async {
            while query_seen.hits() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the poller should probe the query upstream");

        for addr in [gateway_addr, probe_addr] {
            let (status, body) = get_probe(addr, READY_ANY_PATH).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(
                body,
                serde_json::json!({"ready": false, "submissions": false, "queries": false})
            );
            assert_eq!(get_probe(addr, READY_PATH).await.0, StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[tokio::test]
    async fn ready_any_matches_ready_without_a_redirect() {
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        let readiness = Arc::clone(&state.readiness);
        let (gateway_addr, _shutdown) = spawn(test_router(state.clone())).await;
        let (probe_addr, _probes) = spawn_probes(state).await;

        for worker_ready in [false, true, false] {
            readiness.set_ready(0, worker_ready);
            let expected =
                if worker_ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
            for addr in [gateway_addr, probe_addr] {
                let (ready_status, ready_body) = get_probe(addr, READY_PATH).await;
                let (any_status, any_body) = get_probe(addr, READY_ANY_PATH).await;
                assert_eq!(ready_status, expected);
                assert_eq!(any_status, ready_status);
                assert_eq!(any_body["ready"], ready_body["ready"]);
                assert_eq!(
                    any_body,
                    serde_json::json!({
                        "ready": worker_ready,
                        "submissions": worker_ready,
                        "queries": false
                    })
                );
            }
        }
    }

    #[tokio::test]
    async fn query_probe_sends_the_redirect_marker() {
        // a query upstream that records the marker headers and the call of
        // every request it receives
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = Arc::clone(&received);
        let mock = Router::new().route(
            "/",
            post(move |headers: HeaderMap, body: String| {
                let record = Arc::clone(&record);
                async move {
                    let header = |name: &str| {
                        headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
                    };
                    let seen = (header("x-tn-gateway-redirect"), header("x-tn-gateway"), body);
                    record.lock().expect("record lock").push(seen);
                    CHAIN_ID_REPLY
                }
            }),
        );
        let (query, _query) = spawn(mock).await;
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, Some(query));
        let _poller = spawn_poller(&state);
        let (probe_addr, _probes) = spawn_probes(state.clone()).await;

        await_probe_status(probe_addr, READY_ANY_PATH, StatusCode::OK).await;
        let probes = received.lock().expect("record lock").clone();
        assert!(!probes.is_empty());
        for (redirect_marker, hop_marker, body) in &probes {
            assert_eq!(redirect_marker.as_deref(), Some("1"));
            assert_eq!(*hop_marker, None, "the query probe must not carry the worker hop marker");
            let call: serde_json::Value = serde_json::from_str(body).expect("json call");
            assert_eq!(call["method"], "eth_chainId");
        }
        assert_eq!(worker_seen.hits(), 0, "the query probe must never reach the worker");

        // a read the proxy forwards to the same upstream carries exactly the
        // probe's markers, so the probe travels like any query-route request
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        let (status, _) = post_rpc(gateway, None, call("eth_call", 2)).await;
        assert_eq!(status, StatusCode::OK);
        let reads: Vec<_> = received
            .lock()
            .expect("record lock")
            .iter()
            .filter(|(_, _, body)| {
                let call: serde_json::Value = serde_json::from_str(body).expect("json call");
                call["method"] == "eth_call"
            })
            .map(|(redirect_marker, hop_marker, _)| (redirect_marker.clone(), hop_marker.clone()))
            .collect();
        let [read] = reads.as_slice() else { panic!("expected one proxied read, got {reads:?}") };
        for (redirect_marker, hop_marker, _) in &probes {
            assert_eq!(
                (redirect_marker, hop_marker),
                (&read.0, &read.1),
                "the query probe's markers must match a proxied read's"
            );
        }
        assert_eq!(worker_seen.hits(), 0, "a redirected read must never reach the worker");
    }

    #[tokio::test]
    async fn ready_any_bypasses_rate_limit() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        // a maximally strict global limit: if this probe were rate limited, the
        // second hit would be a 429
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;

        for _ in 0..5 {
            assert_eq!(get_probe(addr, READY_ANY_PATH).await.0, StatusCode::OK);
        }
    }

    /// A method other than `GET` or `HEAD` on `/ready/any` is charged against
    /// the client's bucket like any request before it gets its `405`, and a
    /// `429` once the bucket is empty; `GET` and `HEAD` are never charged.
    #[tokio::test]
    async fn ready_any_charges_other_methods_against_the_rate_limit() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let app = identity_router(state, one_request_per_ip(), "127.0.0.1/32");
        let (gateway, _shutdown) = spawn(app).await;
        let url = format!("http://{gateway}{READY_ANY_PATH}");
        let client = Client::new();
        let status = |method: Method| {
            let request = client.request(method, &url).body(call("eth_chainId", 1));
            async move { request.send().await.expect("send").status() }
        };

        // probes spend nothing, so the only token is still there for the POST
        for _ in 0..3 {
            assert_eq!(status(Method::GET).await, StatusCode::OK);
            assert_eq!(status(Method::HEAD).await, StatusCode::OK);
        }
        assert_eq!(status(Method::POST).await, StatusCode::METHOD_NOT_ALLOWED);
        // the POST took the client's only token
        assert_eq!(status(Method::POST).await, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(status(Method::PUT).await, StatusCode::TOO_MANY_REQUESTS);
        let (status_rpc, _) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!(status_rpc, StatusCode::TOO_MANY_REQUESTS, "the bucket is the client's");
        // and the probes still answer with the bucket empty
        assert_eq!(status(Method::GET).await, StatusCode::OK);
        assert_eq!(status(Method::HEAD).await, StatusCode::OK);
        assert_eq!(worker_seen.hits(), 0, "a probe path must never reach the upstream");
    }

    /// A throwaway certificate authority for the TLS upstream tests.
    struct TestCa {
        cert: rcgen::Certificate,
        key: KeyPair,
    }

    impl TestCa {
        fn new(name: &str) -> Self {
            let key = KeyPair::generate().expect("ca key");
            let mut params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            params.distinguished_name.push(DnType::CommonName, name);
            params.key_usages =
                vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
            let cert = params.self_signed(&key).expect("self-signed ca");
            Self { cert, key }
        }

        /// A leaf certificate for `127.0.0.1` (an IP SAN) signed by this CA,
        /// with its key.
        fn leaf(&self, usage: ExtendedKeyUsagePurpose) -> (rcgen::Certificate, KeyPair) {
            self.leaf_for("127.0.0.1", usage)
        }

        /// A leaf certificate for `name` (an IP SAN when it parses as an
        /// address, a DNS SAN otherwise) signed by this CA, with its key.
        fn leaf_for(
            &self,
            name: &str,
            usage: ExtendedKeyUsagePurpose,
        ) -> (rcgen::Certificate, KeyPair) {
            let key = KeyPair::generate().expect("leaf key");
            let mut params = CertificateParams::new(vec![name.to_string()]).expect("leaf params");
            params.distinguished_name.push(DnType::CommonName, name);
            params.extended_key_usages = vec![usage];
            let cert = params.signed_by(&key, &self.cert, &self.key).expect("signed leaf");
            (cert, key)
        }

        /// The CA certificate as a PEM file, as `--upstream-ca-cert` takes it.
        fn pem_file(&self) -> NamedTempFile {
            pem_file(&self.cert.pem())
        }
    }

    /// Write `pem` to a temporary file that lives as long as the handle.
    fn pem_file(pem: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().expect("temp file");
        file.write_all(pem.as_bytes()).expect("write pem");
        file
    }

    /// A rustls server configuration presenting a `127.0.0.1` leaf signed by
    /// `ca`, requiring a client certificate signed by `client_ca` when given.
    fn tls_server_config(ca: &TestCa, client_ca: Option<&TestCa>) -> Arc<ServerConfig> {
        tls_server_config_for(ca, "127.0.0.1", client_ca)
    }

    /// [`tls_server_config`] with a leaf issued to `name` instead. The
    /// provider is explicit so no process-wide default is needed.
    fn tls_server_config_for(
        ca: &TestCa,
        name: &str,
        client_ca: Option<&TestCa>,
    ) -> Arc<ServerConfig> {
        let provider = Arc::new(default_provider());
        let (leaf, key) = ca.leaf_for(name, ExtendedKeyUsagePurpose::ServerAuth);
        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("protocol versions");
        let builder = match client_ca {
            Some(client_ca) => {
                let mut roots = RootCertStore::empty();
                roots.add(client_ca.cert.der().clone()).expect("client ca root");
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()
                        .expect("client verifier");
                builder.with_client_cert_verifier(verifier)
            }
            None => builder.with_no_client_auth(),
        };
        let config = builder
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .expect("server certificate");
        Arc::new(config)
    }

    /// Serve `app` over TLS with `config` on an ephemeral port, mirroring
    /// [`accept_loop`]'s per-connection hyper service without its limits.
    /// The counter counts finished TLS handshakes, failed ones included, so a
    /// test can wait for a poll to have been refused. The `Notifier` keeps the
    /// server alive.
    async fn spawn_tls(
        app: Router,
        config: Arc<ServerConfig>,
    ) -> (SocketAddr, Arc<AtomicUsize>, Notifier) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let acceptor = TlsAcceptor::from(config);
        let handshakes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&handshakes);
        let shutdown = Notifier::new();
        let noticer = shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = &noticer => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, peer_addr)) = accepted else { continue };
                let (acceptor, app, counter) =
                    (acceptor.clone(), app.clone(), Arc::clone(&counter));
                tokio::spawn(async move {
                    let tls = acceptor.accept(stream).await;
                    counter.fetch_add(1, Ordering::SeqCst);
                    // a refused handshake (unknown CA, missing client
                    // certificate) just drops the connection
                    let Ok(tls) = tls else { return };
                    let service =
                        TowerToHyperService::new(app.layer(Extension(ConnectInfo(peer_addr))));
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                });
            }
        });
        (addr, handshakes, shutdown)
    }

    /// A worker RPC mock (`POST /` answers `worker`) and a readiness mock
    /// (`GET /health/workers` reports worker 0 accepting), each on its own port.
    struct WorkerMocks {
        rpc: SocketAddr,
        rpc_hits: Arc<AtomicUsize>,
        readiness: SocketAddr,
        readiness_handshakes: Arc<AtomicUsize>,
        _servers: [Notifier; 2],
    }

    fn worker_mock_routers() -> (Router, Router, Arc<AtomicUsize>) {
        let rpc_hits = Arc::new(AtomicUsize::new(0));
        let hits = Arc::clone(&rpc_hits);
        let rpc = Router::new().route(
            "/",
            post(move || {
                let hits = Arc::clone(&hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    "worker"
                }
            }),
        );
        let readiness = Router::new().route(
            "/health/workers",
            get(|| async {
                r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#
            }),
        );
        (rpc, readiness, rpc_hits)
    }

    /// Both worker mocks over TLS with `config`.
    async fn tls_worker_mocks(config: Arc<ServerConfig>) -> WorkerMocks {
        let (rpc, readiness, rpc_hits) = worker_mock_routers();
        let (rpc, _, rpc_server) = spawn_tls(rpc, Arc::clone(&config)).await;
        let (readiness, readiness_handshakes, readiness_server) =
            spawn_tls(readiness, config).await;
        WorkerMocks {
            rpc,
            rpc_hits,
            readiness,
            readiness_handshakes,
            _servers: [rpc_server, readiness_server],
        }
    }

    /// Resolve the gateway's settings through the CLI, as `main` does, for one
    /// worker whose RPC and readiness mocks listen on `rpc` and `readiness`
    /// and are reached over `scheme`, plus `flags`.
    fn worker_settings(
        scheme: &str,
        rpc: SocketAddr,
        readiness: SocketAddr,
        flags: &[String],
    ) -> Settings {
        let mut argv = vec![
            "worker-gateway".to_string(),
            format!("--upstream-rpc-url={scheme}://{rpc}/"),
            format!("--upstream-readiness-url={scheme}://{readiness}/health/workers"),
        ];
        argv.extend(flags.iter().cloned());
        crate::cli::try_parse_test_argv(argv).expect("parse").into_settings().expect("settings")
    }

    /// The gateway state with the production upstream client built from
    /// `settings`, as `app::run` builds it, with every worker not ready.
    fn production_state(settings: &Settings) -> AppState {
        let http = proxy_client(
            client_builder(settings, Arc::new(SystemLookup)),
            settings.upstream_request_timeout,
        )
        .expect("proxy client");
        test_state_with_client(&settings.upstreams, http)
    }

    /// Serve a gateway for `settings` with a live readiness poller using the
    /// production clients, as `app::run` wires them. The two `Notifier`s keep
    /// the server and the poller alive.
    async fn spawn_polled_gateway(settings: &Settings) -> (SocketAddr, Notifier, Notifier) {
        let UpstreamClients { proxy, query, readiness: readiness_client } =
            UpstreamClients::new(settings, Arc::new(SystemLookup)).expect("upstream clients");
        let mut state = test_state_with_client(&settings.upstreams, proxy);
        let query_probe =
            settings.query_upstream.clone().zip(query.clone()).map(|(url, client)| QueryProbe {
                url,
                client,
                query_header: settings.query_header.clone(),
            });
        state.query_http = query;
        let poller = Notifier::new();
        tokio::spawn(run_poller(
            Arc::clone(&state.readiness),
            readiness_client,
            query_probe,
            Duration::from_millis(50),
            Duration::from_secs(2),
            poller.subscribe(),
        ));
        let (gateway, shutdown) = spawn(test_router(state)).await;
        (gateway, shutdown, poller)
    }

    /// Wait up to five seconds for the gateway's `/ready` to answer `200`.
    async fn wait_until_ready(gateway: SocketAddr) {
        for _ in 0..100 {
            if ready_status(gateway).await == StatusCode::OK {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("the upstream never became ready");
    }

    /// Wait up to five seconds for `counter` to reach `target`.
    async fn wait_for_count(counter: &AtomicUsize, target: usize) {
        for _ in 0..100 {
            if counter.load(Ordering::SeqCst) >= target {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("count never reached {target}");
    }

    #[tokio::test]
    async fn https_worker_with_a_custom_ca_forwards_and_polls() {
        let ca = TestCa::new("worker test ca");
        let mocks = tls_worker_mocks(tls_server_config(&ca, None)).await;
        let ca_file = ca.pem_file();
        let settings = worker_settings(
            "https",
            mocks.rpc,
            mocks.readiness,
            &[format!("--upstream-ca-cert={}", ca_file.path().display())],
        );
        let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;

        // the poller reached the readiness mock over tls and believed it
        wait_until_ready(gateway).await;
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(mocks.rpc_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn wrong_ca_makes_the_upstream_not_ready() {
        let worker_ca = TestCa::new("worker test ca");
        let other_ca = TestCa::new("unrelated test ca");
        let mocks = tls_worker_mocks(tls_server_config(&worker_ca, None)).await;
        let ca_file = other_ca.pem_file();
        let settings = worker_settings(
            "https",
            mocks.rpc,
            mocks.readiness,
            &[format!("--upstream-ca-cert={}", ca_file.path().display())],
        );
        let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;

        // two refused handshakes: the first poll has finished and been recorded
        wait_for_count(&mocks.readiness_handshakes, 2).await;
        assert_eq!(ready_status(gateway).await, StatusCode::SERVICE_UNAVAILABLE);
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 2)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text), (-32000, serde_json::json!(2)));

        // forced ready (no poller), the forward itself still refuses the
        // certificate and never reaches the worker's handler
        let state = production_state(&settings);
        state.readiness.set_ready(0, true);
        let (forced, _forced) = spawn(test_router(state)).await;
        let (status, text) = post_rpc(forced, None, call("eth_sendRawTransaction", 3)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(3)));
        assert_eq!(mocks.rpc_hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn certificate_for_another_host_makes_the_upstream_not_ready() {
        let ca = TestCa::new("worker test ca");
        let ca_file = ca.pem_file();
        // signed by the trusted CA, but for a DNS name and for another
        // address, while the URLs name 127.0.0.1
        for name in ["worker.test", "127.0.0.2"] {
            let mocks = tls_worker_mocks(tls_server_config_for(&ca, name, None)).await;
            let settings = worker_settings(
                "https",
                mocks.rpc,
                mocks.readiness,
                &[format!("--upstream-ca-cert={}", ca_file.path().display())],
            );
            let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;
            wait_for_count(&mocks.readiness_handshakes, 2).await;
            assert_eq!(ready_status(gateway).await, StatusCode::SERVICE_UNAVAILABLE, "{name}");

            // forced ready, the forward itself refuses the name and never
            // reaches the worker's handler
            let state = production_state(&settings);
            state.readiness.set_ready(0, true);
            let (forced, _forced) = spawn(test_router(state)).await;
            let (status, text) = post_rpc(forced, None, call("eth_sendRawTransaction", 3)).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "{name}");
            assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(3)), "{name}");
            assert_eq!(mocks.rpc_hits.load(Ordering::SeqCst), 0, "{name}");
        }
    }

    #[tokio::test]
    async fn http_worker_urls_still_work() {
        // plain http mocks, with an extra CA configured: the rustls client
        // built for https upstreams still polls and forwards over http
        let (rpc, readiness, rpc_hits) = worker_mock_routers();
        let (rpc, _rpc) = spawn(rpc).await;
        let (readiness, _readiness) = spawn(readiness).await;
        let ca_file = TestCa::new("unused test ca").pem_file();
        let settings = worker_settings(
            "http",
            rpc,
            readiness,
            &[format!("--upstream-ca-cert={}", ca_file.path().display())],
        );
        let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;

        wait_until_ready(gateway).await;
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(rpc_hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn client_certificate_is_presented() {
        let server_ca = TestCa::new("worker test ca");
        let client_ca = TestCa::new("gateway client test ca");
        let config = tls_server_config(&server_ca, Some(&client_ca));
        let ca_file = server_ca.pem_file();
        let ca_flag = format!("--upstream-ca-cert={}", ca_file.path().display());

        // without a client certificate the mocks refuse the handshake
        let refused = tls_worker_mocks(Arc::clone(&config)).await;
        let settings = worker_settings(
            "https",
            refused.rpc,
            refused.readiness,
            std::slice::from_ref(&ca_flag),
        );
        let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;
        wait_for_count(&refused.readiness_handshakes, 2).await;
        assert_eq!(ready_status(gateway).await, StatusCode::SERVICE_UNAVAILABLE);

        // with the certificate and key, the same configuration accepts it
        let (cert, key) = client_ca.leaf(ExtendedKeyUsagePurpose::ClientAuth);
        let cert_file = pem_file(&cert.pem());
        let key_file = pem_file(&key.serialize_pem());
        let accepted = tls_worker_mocks(config).await;
        let settings = worker_settings(
            "https",
            accepted.rpc,
            accepted.readiness,
            &[
                ca_flag,
                format!("--upstream-client-cert={}", cert_file.path().display()),
                format!("--upstream-client-key={}", key_file.path().display()),
            ],
        );
        let (gateway, _shutdown, _poller) = spawn_polled_gateway(&settings).await;
        wait_until_ready(gateway).await;
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(accepted.rpc_hits.load(Ordering::SeqCst), 1);
        assert_eq!(refused.rpc_hits.load(Ordering::SeqCst), 0);
    }

    /// reqwest connects to an IP literal without calling its resolver, so a
    /// literal upstream never takes a lookup permit or a cache entry.
    #[tokio::test]
    async fn ip_literal_upstreams_never_reach_the_resolver() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let lookups = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&lookups);
        let lookup = lookup_fn(move |_host| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Err(std::io::Error::other("no name should be looked up")) }
        });
        let settings =
            worker_settings("http", worker, worker, &["--max-concurrent-dns-lookups=1".into()]);
        let client =
            proxy_client(client_builder(&settings, lookup), settings.upstream_request_timeout)
                .expect("client");
        let state = redirect_state_with_client(worker, Some(query), client);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, 1));
        assert_eq!(lookups.load(Ordering::SeqCst), 0, "an IP literal is never looked up");
    }

    /// The `Threads:` count from `/proc/self/status`.
    fn thread_count() -> usize {
        std::fs::read_to_string("/proc/self/status")
            .expect("read /proc/self/status")
            .lines()
            .find_map(|line| line.strip_prefix("Threads:"))
            .and_then(|count| count.trim().parse().ok())
            .expect("Threads: line")
    }

    /// A node health endpoint reporting worker 0 ready.
    async fn ready_node() -> (SocketAddr, Notifier) {
        let node = Router::new().route(
            "/health/workers",
            get(|| async {
                r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#
            }),
        );
        spawn(node).await
    }

    /// `http://{host}:{port of addr}{path}`.
    fn named_url(host: &str, addr: SocketAddr, path: &str) -> Url {
        Url::parse(&format!("http://{host}:{}{path}", addr.port())).expect("url")
    }

    /// The #1614 stall: the query host's name takes 10 s to resolve, and each
    /// lookup holds a blocking thread the way `getaddrinfo` does, while 100
    /// reads per second go to that host. Readiness polls and submissions,
    /// which resolve the worker's name, must keep working, and the lookups
    /// must not pile up threads. Measures the process's thread count, so it is
    /// ignored by default; run it by hand with `--run-ignored only
    /// --no-capture` to see the numbers.
    ///
    /// The thread bound checks one lookup per host (single flight) together
    /// with the lookup cap, not the cap alone: only `query.test` is slow, so
    /// single flight already keeps it to one blocking thread, and the bound
    /// would hold with the cap removed. The cap itself is asserted by the
    /// `dns` unit tests, and
    /// `held_query_host_lookup_leaves_ready_and_submissions_at_200` runs the
    /// `/ready` and submission assertions without the thread count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "measures the process thread count; run by hand"]
    async fn slow_query_host_resolution_does_not_stall_readiness_or_submissions() {
        const MAX_LOOKUPS: usize = 8;
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let (node, _node) = ready_node().await;

        let query_lookups = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&query_lookups);
        let lookup = lookup_fn(move |host| {
            let slow = host == "query.test";
            if slow {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            async move {
                if slow {
                    tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_secs(10)))
                        .await
                        .map_err(std::io::Error::other)?;
                }
                Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))])
            }
        });
        // the production wiring: separate resolvers for the two clients, and
        // the query probe on the proxy client
        let settings = worker_settings(
            "http",
            worker,
            node,
            &[
                format!("--max-concurrent-dns-lookups={MAX_LOOKUPS}"),
                "--upstream-connect-timeout=2s".into(),
                "--upstream-request-timeout=5s".into(),
            ],
        );
        let clients = UpstreamClients::new(&settings, lookup).expect("clients");

        let query_url = named_url("query.test", query, "/");
        let query_probe = QueryProbe {
            url: query_url.clone(),
            client: clients.proxy.clone(),
            query_header: None,
        };
        let state = AppState {
            query_upstream: Some(query_url),
            ..test_state_with_client(
                &[UpstreamWorker {
                    worker_id: 0,
                    rpc_url: named_url("worker.test", worker, "/"),
                    readiness_url: named_url("worker.test", node, "/health/workers"),
                }],
                clients.proxy,
            )
        };
        let poller = Notifier::new();
        tokio::spawn(run_poller(
            Arc::clone(&state.readiness),
            clients.readiness,
            Some(query_probe),
            Duration::from_millis(100),
            Duration::from_secs(1),
            poller.subscribe(),
        ));
        // room for every read in flight, so the connection cap is not what
        // stalls the probes
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(4_096).expect("nonzero"),
            ..test_limits()
        };
        let (gateway, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        let client = Client::new();
        let mut ready = false;
        for _ in 0..50 {
            let response =
                client.get(format!("http://{gateway}/ready")).send().await.expect("ready");
            if response.status() == StatusCode::OK {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(ready, "the poller should mark the worker ready");
        let start = thread_count();

        let reader = Client::new();
        let reads = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(10));
            let mut sent = Vec::with_capacity(300);
            for id in 0..300_u64 {
                ticker.tick().await;
                let read = reader.post(format!("http://{gateway}/")).body(call("eth_chainId", id));
                sent.push(tokio::spawn(async move { read.send().await.map(|r| r.status()) }));
            }
            sent
        });

        let mut peak = start;
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        for id in 0..30_u64 {
            ticker.tick().await;
            let response = client
                .post(format!("http://{gateway}/"))
                .body(call("eth_sendRawTransaction", id))
                .send()
                .await
                .expect("submission");
            assert_eq!(response.status(), StatusCode::OK, "submission {id}");
            assert_eq!(response.text().await.expect("text"), "worker");
            let response =
                client.get(format!("http://{gateway}/ready")).send().await.expect("ready");
            assert_eq!(response.status(), StatusCode::OK, "/ready at tick {id}");
            peak = peak.max(thread_count());
        }
        let end = thread_count();
        println!("threads: start {start}, peak {peak}, end {end}");

        let bound = start + MAX_LOOKUPS + 4;
        assert!(peak < bound && end < bound, "threads grew from {start} to {peak} (bound {bound})");
        assert_eq!(query_lookups.load(Ordering::SeqCst), 1, "every read shares one lookup");
        for read in reads.await.expect("reads") {
            let status = read.await.expect("join").expect("a gateway response");
            assert!(
                matches!(status, StatusCode::GATEWAY_TIMEOUT | StatusCode::BAD_GATEWAY),
                "{status}"
            );
        }
        assert_eq!(query_seen.hits(), 0);
        assert!(worker_seen.hits() >= 30);
    }

    /// The #1614 acceptance assertions without the thread count, so they run
    /// by default: while the query host's lookup is held, reads to it fail at
    /// the connect timeout, and submissions and `/ready`, which resolve the
    /// worker's name, keep answering `200`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn held_query_host_lookup_leaves_ready_and_submissions_at_200() {
        const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let (node, _node) = ready_node().await;

        // the query host's lookup waits on the gate; any other host resolves
        // at once
        let gate = Arc::new(Semaphore::new(0));
        let query_lookups = Arc::new(AtomicUsize::new(0));
        let lookup = {
            let gate = Arc::clone(&gate);
            let query_lookups = Arc::clone(&query_lookups);
            lookup_fn(move |host| {
                let held = host == "query.test";
                if held {
                    query_lookups.fetch_add(1, Ordering::SeqCst);
                }
                let gate = Arc::clone(&gate);
                async move {
                    if held {
                        // closing the gate releases the lookup
                        let _closed = gate.acquire().await;
                    }
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))])
                }
            })
        };
        // the production wiring: separate resolvers for the two clients, and
        // the query probe on the proxy client
        let settings = worker_settings(
            "http",
            worker,
            node,
            &[
                "--max-concurrent-dns-lookups=8".into(),
                format!("--upstream-connect-timeout={}ms", CONNECT_TIMEOUT.as_millis()),
                "--upstream-request-timeout=5s".into(),
            ],
        );
        let clients = UpstreamClients::new(&settings, lookup).expect("clients");
        let query_url = named_url("query.test", query, "/");
        let query_probe = QueryProbe {
            url: query_url.clone(),
            client: clients.proxy.clone(),
            query_header: None,
        };
        let state = AppState {
            query_upstream: Some(query_url),
            ..test_state_with_client(
                &[UpstreamWorker {
                    worker_id: 0,
                    rpc_url: named_url("worker.test", worker, "/"),
                    readiness_url: named_url("worker.test", node, "/health/workers"),
                }],
                clients.proxy,
            )
        };
        let poller = Notifier::new();
        tokio::spawn(run_poller(
            Arc::clone(&state.readiness),
            clients.readiness,
            Some(query_probe),
            Duration::from_millis(50),
            Duration::from_secs(1),
            poller.subscribe(),
        ));
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let client = Client::new();
        let mut ready = false;
        for _ in 0..100 {
            let response =
                client.get(format!("http://{gateway}/ready")).send().await.expect("ready");
            if response.status() == StatusCode::OK {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(ready, "the poller should mark the worker ready");

        // a read starts the held lookup and gives up at the connect timeout,
        // well before the 5 s request timeout
        let started = tokio::time::Instant::now();
        let (status, _) = post_rpc(gateway, None, call("eth_chainId", 0)).await;
        let elapsed = started.elapsed();
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(elapsed >= CONNECT_TIMEOUT && elapsed < Duration::from_secs(2), "{elapsed:?}");

        // reads keep arriving and join the held lookup, while submissions and
        // `/ready` keep answering
        let mut reads = Vec::with_capacity(10);
        for id in 1..=10_u64 {
            reads.push(tokio::spawn(post_rpc(gateway, None, call("eth_chainId", id))));
            let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", id)).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "submission {id}");
            let response =
                client.get(format!("http://{gateway}/ready")).send().await.expect("ready");
            assert_eq!(response.status(), StatusCode::OK, "/ready after submission {id}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        for read in reads {
            let (status, _) = read.await.expect("join");
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        }
        assert_eq!(query_lookups.load(Ordering::SeqCst), 1, "every read joined the held lookup");
        assert_eq!(query_seen.hits(), 0);
        assert_eq!(worker_seen.hits(), 10);
        gate.close();
    }

    /// Exhausting the proxy client's lookup permits leaves the readiness poll
    /// resolving: [`UpstreamClients`] gives each client its own resolver.
    #[tokio::test]
    async fn readiness_resolver_is_independent_of_the_proxy_resolver() {
        let (node, _node) = ready_node().await;
        let gate = Arc::new(Semaphore::new(0));
        let lookups = Arc::new(AtomicUsize::new(0));
        let lookup = {
            let gate = Arc::clone(&gate);
            let lookups = Arc::clone(&lookups);
            lookup_fn(move |host| {
                lookups.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                async move {
                    if host == "slow.test" {
                        // closing the gate releases the lookup
                        let _closed = gate.acquire().await;
                    }
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))])
                }
            })
        };
        let settings = worker_settings(
            "http",
            node,
            node,
            &[
                "--max-concurrent-dns-lookups=1".into(),
                "--upstream-connect-timeout=5s".into(),
                "--upstream-request-timeout=10s".into(),
            ],
        );
        let clients = UpstreamClients::new(&settings, lookup).expect("clients");

        // a read to a slow host takes the proxy client's only lookup permit
        let proxy = clients.proxy.clone();
        let slow_url = named_url("slow.test", node, "/");
        let slow = tokio::spawn(async move { proxy.post(slow_url).send().await.map(|_| ()) });
        let mut started = false;
        for _ in 0..500 {
            if lookups.load(Ordering::SeqCst) == 1 {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started, "the slow lookup should have started");

        // the proxy client is out of permits: another host fails at once
        let refused = tokio::time::Instant::now();
        let err = clients
            .proxy
            .post(named_url("other.test", node, "/"))
            .send()
            .await
            .expect_err("no lookup permit is free");
        assert!(err.is_connect(), "{err:?}");
        assert!(refused.elapsed() < Duration::from_secs(1), "the refusal must not wait");
        assert_eq!(lookups.load(Ordering::SeqCst), 1);

        // the readiness poller still resolves the worker's host
        let readiness = Arc::new(GatewayReadiness::new(
            &[UpstreamWorker {
                worker_id: 0,
                rpc_url: named_url("worker.test", node, "/"),
                readiness_url: named_url("worker.test", node, "/health/workers"),
            }],
            test_thresholds(),
        ));
        let poller = Notifier::new();
        tokio::spawn(run_poller(
            Arc::clone(&readiness),
            clients.readiness,
            None,
            Duration::from_millis(100),
            Duration::from_secs(1),
            poller.subscribe(),
        ));
        let mut ready = false;
        for _ in 0..500 {
            if readiness.any_ready() {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(ready, "the readiness poll must resolve while the proxy's permits are taken");
        assert_eq!(lookups.load(Ordering::SeqCst), 2, "slow.test and worker.test only");

        gate.close();
        assert!(slow.await.expect("join").is_ok(), "the slow read completes once released");
    }

    /// A lookup the proxy's resolver refuses because every lookup slot is busy
    /// is the gateway's own back-pressure, not a sign that the worker is down:
    /// the submission fails as unreachable, but passive health does not count
    /// it, so the worker stays ready however many submissions are refused.
    #[tokio::test]
    async fn saturated_dns_lookups_do_not_count_against_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;

        // the query host's lookup waits on the gate, holding the only slot
        let gate = Arc::new(Semaphore::new(0));
        let query_lookups = Arc::new(AtomicUsize::new(0));
        let lookup = {
            let gate = Arc::clone(&gate);
            let query_lookups = Arc::clone(&query_lookups);
            lookup_fn(move |host| {
                let held = host == "query.test";
                if held {
                    query_lookups.fetch_add(1, Ordering::SeqCst);
                }
                let gate = Arc::clone(&gate);
                async move {
                    if held {
                        // closing the gate releases the lookup
                        let _closed = gate.acquire().await;
                    }
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], 0))])
                }
            })
        };
        // a connect timeout the test never reaches, so the held read waits for
        // its lookup however slowly the submissions below run
        let settings = worker_settings(
            "http",
            worker,
            worker,
            &["--max-concurrent-dns-lookups=1".into(), "--upstream-connect-timeout=30s".into()],
        );
        let UpstreamClients { proxy, .. } =
            UpstreamClients::new(&settings, lookup).expect("clients");
        let state = AppState {
            query_upstream: Some(named_url("query.test", query, "/")),
            ..test_state_with_client(
                &[UpstreamWorker {
                    worker_id: 0,
                    rpc_url: named_url("worker.test", worker, "/"),
                    readiness_url: named_url("worker.test", worker, "/health/workers"),
                }],
                proxy.clone(),
            )
        };
        state.readiness.set_ready(0, true);
        let readiness = Arc::clone(&state.readiness);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // a read starts the query host's lookup, which takes the only slot
        let read = tokio::spawn(post_rpc(gateway, None, call("eth_chainId", 0)));
        let mut started = false;
        for _ in 0..500 {
            if query_lookups.load(Ordering::SeqCst) == 1 {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started, "the query host's lookup should have started");

        // the refusal is a connect error that carries the resolver's marker;
        // a closed port is a connect error without it
        let refused = proxy
            .post(named_url("other.test", worker, "/"))
            .send()
            .await
            .expect_err("no lookup slot is free");
        assert!(refused.is_connect() && dns::is_saturated(&refused), "{refused:?}");
        let closed = proxy.post("http://127.0.0.1:1/").send().await.expect_err("closed port");
        assert!(closed.is_connect() && !dns::is_saturated(&closed), "{closed:?}");

        // the worker's name has no cached answer and no slot is free, so every
        // submission fails at once, well past `--upstream-failure-threshold`
        let threshold = test_thresholds().rpc_failure.expect("passive health on").get();
        for id in 1..=u64::from(threshold) + 2 {
            let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", id)).await;
            assert_eq!(status, StatusCode::BAD_GATEWAY, "submission {id}");
            assert_eq!(error_code_and_id(&text), (-32001, serde_json::json!(id)));
        }
        assert!(readiness.any_ready(), "the gateway's own lookup cap must not mark the worker");
        assert_eq!(worker_seen.hits(), 0);

        // once the held lookup returns, the read completes, the worker's name
        // resolves and the still-ready worker takes the next submission
        gate.close();
        let (status, text) = read.await.expect("join");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 99)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, 1));
    }

    /// A worker whose connection setup stalls (here its name never resolves)
    /// fails each forward at `--upstream-connect-timeout`, which startup keeps
    /// below `--upstream-request-timeout`. reqwest reports that as a connect
    /// error, so passive health counts it and `/ready` turns `503` at
    /// `--upstream-failure-threshold`, and the forwarding warning's cause
    /// names the connect stage.
    #[tokio::test]
    async fn stalled_connection_setup_counts_against_the_worker() {
        if !process_per_test() {
            eprintln!("skipped: {THROTTLED_WARNING_SKIP}");
            return;
        }
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let lookups = Arc::new(AtomicUsize::new(0));
        let lookup = {
            let lookups = Arc::clone(&lookups);
            lookup_fn(move |_host| {
                lookups.fetch_add(1, Ordering::SeqCst);
                future::pending::<std::io::Result<Vec<SocketAddr>>>()
            })
        };
        let settings = worker_settings(
            "http",
            worker,
            worker,
            &["--upstream-connect-timeout=200ms".into(), "--upstream-request-timeout=1s".into()],
        );
        let UpstreamClients { proxy, .. } =
            UpstreamClients::new(&settings, lookup).expect("clients");
        let state = test_state_with_client(
            &[UpstreamWorker {
                worker_id: 0,
                rpc_url: named_url("worker.test", worker, "/"),
                readiness_url: named_url("worker.test", worker, "/health/workers"),
            }],
            proxy,
        );
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let threshold = test_thresholds().rpc_failure.expect("passive health on").get();
        for id in 1..=u64::from(threshold) {
            assert_eq!(ready_status(gateway).await, StatusCode::OK, "before submission {id}");
            let (status, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", id)).await;
            assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "submission {id}");
            assert_eq!(error_code_and_id(&text).1, serde_json::json!(id));
        }
        assert_eq!(
            ready_status(gateway).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "a stalled connection setup must count toward --upstream-failure-threshold"
        );
        assert_eq!(worker_seen.hits(), 0);
        assert_eq!(lookups.load(Ordering::SeqCst), 1, "each forward waited on the one lookup");

        // the first failure is logged with its whole cause chain and the
        // upstream's origin, no path
        let text = logs.contents();
        let line = text
            .lines()
            .find(|line| line.contains("forwarding to upstream failed"))
            .unwrap_or_else(|| panic!("no forwarding warning: {text}"));
        assert!(
            line.contains(&format!("upstream=http://worker.test:{} ", worker.port())),
            "{line}"
        );
        assert!(
            line.contains("cause=error sending request: client error (Connect): ")
                && line.contains("timed out"),
            "{line}"
        );
    }

    /// A mock upstream that counts each request it receives and never answers
    /// it, so the gateway holds the forwarded request (and its byte
    /// reservation) for as long as the client stays connected. The `Notifier`
    /// keeps it alive.
    async fn stalled_mock() -> (SocketAddr, Arc<AtomicUsize>, Notifier) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let mock = Router::new().route(
            "/",
            post(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                future::pending::<&'static str>()
            }),
        );
        let (addr, shutdown) = spawn(mock).await;
        (addr, hits, shutdown)
    }

    /// An `eth_chainId` call padded with trailing whitespace (which JSON
    /// allows) to exactly `len` bytes.
    fn padded_call(id: u64, len: usize) -> String {
        let mut body = call("eth_chainId", id);
        body.push_str(&" ".repeat(len.saturating_sub(body.len())));
        body
    }

    /// Wait until an upstream has seen `n` requests, so every request held
    /// against it has taken its byte reservation.
    async fn wait_for_hits(hits: &AtomicUsize, n: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while hits.load(Ordering::SeqCst) < n {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the held request should reach the upstream");
    }

    /// Send a complete `POST /` with a `len`-byte body (a JSON-RPC call padded
    /// with whitespace) on a fresh connection without waiting for the answer.
    /// Against a stalled upstream the gateway holds the request, and its byte
    /// reservation, until the returned stream is dropped.
    async fn send_held_request(addr: SocketAddr, len: usize) -> TcpStream {
        let body = padded_call(1, len);
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream
            .write_all(
                format!("POST / HTTP/1.1\r\nHost: gateway\r\nContent-Length: {len}\r\n\r\n{body}")
                    .as_bytes(),
            )
            .await
            .expect("write");
        stream
    }

    /// Send the request line and headers in `head`, then `body`, on a fresh
    /// connection that asks to be closed after the response, and read the whole
    /// response. Returns how long the answer took from the first byte sent,
    /// its status and its body.
    async fn timed_raw_exchange(
        addr: SocketAddr,
        head: &str,
        body: &[u8],
    ) -> (Duration, u16, String) {
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let started = Instant::now();
        let head = format!("{head}\r\nHost: gateway\r\nConnection: close\r\n\r\n");
        stream.write_all(&[head.as_bytes(), body].concat()).await.expect("write");
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .expect("the gateway should answer and close")
            .expect("read");
        let elapsed = started.elapsed();
        let response = String::from_utf8_lossy(&response).into_owned();
        let status =
            response.split(' ').nth(1).and_then(|status| status.parse().ok()).expect("status line");
        let (head, body) = response.split_once("\r\n\r\n").expect("response body");
        let chunked = head.to_ascii_lowercase().contains("transfer-encoding: chunked");
        (elapsed, status, if chunked { dechunk(body) } else { body.to_string() })
    }

    /// Decode a chunked response body (a proxied response streams back
    /// chunked).
    fn dechunk(mut rest: &str) -> String {
        let mut body = String::new();
        while let Some((size, tail)) = rest.split_once("\r\n") {
            let size = usize::from_str_radix(size.trim(), 16).expect("chunk size");
            if size == 0 {
                break;
            }
            body.push_str(tail.get(..size).expect("chunk data"));
            rest = tail.get(size + 2..).expect("chunk terminator");
        }
        body
    }

    /// A gateway with one ready worker at `worker`, the given byte budget and
    /// the default per-request cap.
    async fn spawn_with_budget(worker: SocketAddr, budget: u32) -> (SocketAddr, Notifier) {
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            Some(nz(budget)),
            RouterLayers::default(),
            Arc::default(),
        ))
        .await
    }

    /// The issue's acceptance criterion: a request that would exceed the
    /// remaining budget gets an envelope at once instead of stalling. The
    /// refused request is answered from its head, before its body is read or
    /// even sent, so its `id` is unknown and echoes as `null`.
    #[tokio::test]
    async fn request_over_remaining_byte_budget_gets_envelope_immediately() {
        let (worker, hits, _worker) = stalled_mock().await;
        let (gateway, _shutdown) = spawn_with_budget(worker, 1024 * 1024).await;

        // 900 KiB held against an upstream that never answers leaves 124 KiB
        let _held = send_held_request(gateway, 900 * 1024).await;
        wait_for_hits(&hits, 1).await;

        let (elapsed, status, body) =
            timed_raw_exchange(gateway, "POST / HTTP/1.1\r\nContent-Length: 204800", b"").await;
        assert_eq!(status, 503);
        assert_eq!(error_code_and_id(&body), (-32010, serde_json::Value::Null));
        // a queued reservation, or one that read the body first, would wait
        // out the 5s request deadline; 1s separates the two with room to spare
        // on a loaded runner, like the other "must not wait" checks
        assert!(elapsed < Duration::from_secs(1), "answered after {elapsed:?}");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the refused request must not be forwarded");
    }

    #[tokio::test]
    async fn byte_budget_is_released_after_forward() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (gateway, _shutdown) = spawn_with_budget(worker, 1024 * 1024).await;

        // each request reserves 900 KiB of the 1 MiB budget, so the second
        // only fits if the first returned its reservation once forwarded
        for id in [1, 2] {
            let body = padded_call(id, 900 * 1024);
            let (status, text) = post_rpc(gateway, None, body).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "request {id}");
        }
        assert_eq!(worker_seen.hits(), 2);
    }

    #[tokio::test]
    async fn chunked_body_reserves_the_request_cap() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let request = call("eth_chainId", 1);
        let chunked_head = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked";
        let chunked_body = format!("{:x}\r\n{request}\r\n0\r\n\r\n", request.len());
        let cap = u32::try_from(MAX_REQUEST_BYTES).expect("the cap fits a u32");

        // a budget of exactly the cap admits a chunked request
        let (gateway, _exact) = spawn_with_budget(worker, cap).await;
        let (_, status, body) =
            timed_raw_exchange(gateway, chunked_head, chunked_body.as_bytes()).await;
        assert_eq!((status, body.as_str()), (200, "worker"));

        // one byte less cannot cover the cap, so the same call sent chunked is
        // refused from its head, while sent with its length it still fits
        let (gateway, _short) = spawn_with_budget(worker, cap - 1).await;
        let (_, status, body) = timed_raw_exchange(gateway, chunked_head, b"").await;
        assert_eq!(status, 503);
        assert_eq!(error_code_and_id(&body).0, -32010);
        let sized_head = format!("POST / HTTP/1.1\r\nContent-Length: {}", request.len());
        let (_, status, body) = timed_raw_exchange(gateway, &sized_head, request.as_bytes()).await;
        assert_eq!((status, body.as_str()), (200, "worker"));
        assert_eq!(worker_seen.hits(), 2);

        // startup refuses a budget below the cap, so the case a deployment can
        // reach is a budget of exactly the cap with a small request held
        // against a stalled upstream: what is left is below the cap, so a
        // chunked head is refused
        let (stalled, stalled_hits, _stalled) = stalled_mock().await;
        let (gateway, _full) = spawn_with_budget(stalled, cap).await;
        let _held = send_held_request(gateway, 1024).await;
        wait_for_hits(&stalled_hits, 1).await;
        let (_, status, body) = timed_raw_exchange(gateway, chunked_head, b"").await;
        assert_eq!(status, 503);
        assert_eq!(error_code_and_id(&body).0, -32010);
        assert_eq!(stalled_hits.load(Ordering::SeqCst), 1, "the refused head is not forwarded");
    }

    #[tokio::test]
    async fn content_length_over_the_request_cap_is_rejected_before_reading() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        // with the budget on and off: the length check does not need it
        for budget in [NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES), None] {
            let state = test_state(&[upstream(worker)]);
            state.readiness.set_ready(0, true);
            let app = router(
                state,
                Duration::from_secs(5),
                MAX_REQUEST_BYTES,
                budget,
                RouterLayers::default(),
                Arc::default(),
            );
            let (gateway, _shutdown) = spawn(app).await;

            // only the head is sent, so an answer proves nothing was read
            let head = format!("POST / HTTP/1.1\r\nContent-Length: {}", 2 * MAX_REQUEST_BYTES);
            let (_, status, body) = timed_raw_exchange(gateway, &head, b"").await;
            assert_eq!(status, 413, "budget {budget:?}");
            assert_eq!(error_code_and_id(&body).0, -32003, "budget {budget:?}");
        }
        assert_eq!(worker_seen.hits(), 0);
    }

    /// A body over the cap whose length is not declared is still refused
    /// while it is read: the early length check only sees declared lengths.
    #[tokio::test]
    async fn oversized_chunked_body_is_rejected_while_read() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            8,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let request = call("eth_chainId", 1);
        let chunked_body = format!("{:x}\r\n{request}\r\n0\r\n\r\n", request.len());
        let (_, status, body) = timed_raw_exchange(
            gateway,
            "POST / HTTP/1.1\r\nTransfer-Encoding: chunked",
            chunked_body.as_bytes(),
        )
        .await;
        assert_eq!(status, 413);
        assert_eq!(error_code_and_id(&body).0, -32003);
        assert_eq!(worker_seen.hits(), 0);
    }

    #[tokio::test]
    async fn rate_limited_request_costs_no_budget() {
        let (worker, hits, _worker) = stalled_mock().await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        // two requests of burst, refilled at one per second
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(2))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let budget = 64 * 1024;
        let app = router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            Some(nz(budget)),
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        );
        let (gateway, _shutdown) = spawn(app).await;
        let budget = usize::try_from(budget).expect("usize");

        // the first token: a request holding the whole budget
        let held = send_held_request(gateway, budget).await;
        wait_for_hits(&hits, 1).await;
        // the second token: admitted by the rate limit, refused by the budget
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text).0, -32010);
        // out of tokens: the rate limit answers before the budget is consulted
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 3)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&text).0, -32006);

        // drop the held request (the gateway cancels it on the disconnect) and
        // wait for a token: a request needing the whole budget then fits, so
        // the refused and rate-limited requests left nothing held
        drop(held);
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        let _held = send_held_request(gateway, budget).await;
        wait_for_hits(&hits, 2).await;
    }

    #[tokio::test]
    async fn probes_bypass_the_byte_budget() {
        let (worker, hits, _worker) = stalled_mock().await;
        let budget = 64 * 1024;
        let (gateway, _shutdown) = spawn_with_budget(worker, budget).await;
        let _held = send_held_request(gateway, usize::try_from(budget).expect("usize")).await;
        wait_for_hits(&hits, 1).await;

        // the budget is spent
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_code_and_id(&text).0, -32010);

        // the probes still answer, even carrying a body that would not fit;
        // `/ready/any` is added after every layer, so it never reserves bytes
        for path in [HEALTH_PATH, READY_PATH, READY_ANY_PATH] {
            for body in [Vec::new(), vec![b' '; 16]] {
                let response = Client::new()
                    .get(format!("http://{gateway}{path}"))
                    .body(body)
                    .send()
                    .await
                    .expect("send");
                assert_eq!(response.status(), StatusCode::OK, "{path}");
            }
        }
    }

    /// Byte `i` of a recognizable test body: the alphabet, over and over.
    fn pattern_byte(i: usize) -> u8 {
        b'a' + u8::try_from(i % 26).expect("below 26")
    }

    #[tokio::test]
    async fn large_body_still_forwards_with_a_small_read_buffer() {
        // the upstream answers with the length it received and whether every
        // byte is where the client put it
        let mock = Router::new().route(
            "/",
            post(|body: axum::body::Bytes| async move {
                let intact = body.iter().enumerate().all(|(i, byte)| *byte == pattern_byte(i));
                format!("{} {intact}", body.len())
            }),
        );
        let (worker, _worker) = spawn(mock).await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        // hyper's minimum read buffer, 128 times smaller than the body
        let limits = ServerLimits { http1_max_buf_size: HTTP1_MIN_BUF_SIZE, ..test_limits() };
        let (gateway, _shutdown) = spawn_with_limits(test_router(state), limits).await;

        let body: String = (0..MAX_REQUEST_BYTES).map(|i| char::from(pattern_byte(i))).collect();
        let (status, text) = post_rpc(gateway, None, body).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "1048576 true"));
    }

    /// Peak resident memory of this process so far, in kB (`VmHWM`).
    #[cfg(any(target_os = "android", target_os = "linux"))]
    fn peak_rss_kb() -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
        status
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|value| value.trim().strip_suffix("kB"))
            .and_then(|value| value.trim().parse().ok())
            .expect("VmHWM line")
    }

    /// The issue's acceptance criterion: peak RSS stays near the budget for
    /// many parallel maximum-size bodies held against a slow upstream.
    /// Everything (client, gateway, upstream) shares this process, so the
    /// client sends one shared allocation and the upstream drains each body
    /// without keeping it. The peak is measured above the idle process and
    /// bounded in proportion to the budget, so a body reader that holds about
    /// twice what it reserved fails. The bound relies on the read-buffer cap
    /// both servers run with: at hyper's default every connection keeps up to
    /// 408 KiB. Run by hand; the printed lines go in the PR body.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    #[ignore = "measures peak RSS; run alone with --run-ignored ignored-only --no-capture"]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn peak_rss_stays_near_the_budget_with_parallel_max_bodies() {
        const BUDGET: u32 = 64 * 1024 * 1024;
        const PARALLEL: usize = 128;
        let slow = Router::new().route(
            "/",
            post(|body: axum::body::Body| async move {
                let mut chunks = body.into_data_stream();
                while futures::StreamExt::next(&mut chunks).await.is_some() {}
                tokio::time::sleep(Duration::from_secs(3)).await;
                "slow"
            }),
        );
        let wide = ServerLimits {
            max_connections: NonZeroUsize::new(2 * PARALLEL).expect("nonzero"),
            request_deadline: Duration::from_secs(30),
            ..test_limits()
        };
        let (worker, _worker) = spawn_with_limits(slow, wide.clone()).await;
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(30),
        )
        .expect("client");
        let state = test_state_with_client(&[upstream(worker)], client);
        state.readiness.set_ready(0, true);
        let app = router(
            state,
            Duration::from_secs(30),
            MAX_REQUEST_BYTES,
            Some(nz(BUDGET)),
            RouterLayers::default(),
            Arc::default(),
        );
        let (gateway, _shutdown) = spawn_with_limits(app, wide).await;

        let body = axum::body::Bytes::from(padded_call(1, MAX_REQUEST_BYTES));
        let client = Client::new();
        let before = peak_rss_kb();
        let requests = (0..PARALLEL).map(|_| {
            let request = client.post(format!("http://{gateway}/")).body(body.clone()).send();
            async move { request.await.map(|response| response.status()) }
        });
        let outcomes = futures::future::join_all(requests).await;
        let peak = peak_rss_kb();

        let forwarded =
            outcomes.iter().filter(|outcome| matches!(outcome, Ok(StatusCode::OK))).count();
        let refused = outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(StatusCode::SERVICE_UNAVAILABLE)))
            .count();
        let failed = PARALLEL - forwarded - refused;
        // the budget with a tenth for allocator slack, a full read buffer for
        // every connection on both servers, and a fixed allowance; collecting
        // the chunks and then joining them costs about 1.7 times the budget
        // and does not fit
        let above_kb = peak.saturating_sub(before);
        let budget_kb = u64::from(BUDGET / 1024);
        let buffers_kb = u64::try_from(PARALLEL * 2 * HTTP1_MAX_BUF_SIZE / 1024).expect("fits");
        let bound_kb = budget_kb * 11 / 10 + buffers_kb + 16 * 1024;
        println!(
            "VmHWM: {peak} kB ({before} kB before the flood) for {PARALLEL} parallel \
             {MAX_REQUEST_BYTES}-byte bodies against a {BUDGET}-byte budget: {forwarded} \
             forwarded, {refused} refused with -32010, {failed} other"
        );
        println!("{above_kb} kB above the idle process, bound {bound_kb} kB");
        assert!((1..=64).contains(&forwarded), "{forwarded} forwarded");
        assert!(above_kb < bound_kb, "{above_kb} kB above the idle process, bound {bound_kb} kB");
    }

    /// The canonical EIP-155 example transaction (a signed legacy transfer):
    /// the screen decodes it and forwards it, handing back its hash.
    const SIGNED_TX: &str = "0xf86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83";

    /// The account the nonce reads below ask about.
    const ADDRESS: &str = "0x3535353535353535353535353535353535353535";

    /// A JSON-RPC call to `method` with the given params and id.
    fn call_with(method: &str, params: &str, id: u64) -> String {
        format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{id}}}"#)
    }

    /// A single `eth_sendRawTransaction` of [`SIGNED_TX`].
    fn submission(id: u64) -> String {
        call_with("eth_sendRawTransaction", &format!(r#"["{SIGNED_TX}"]"#), id)
    }

    /// The hash of [`SIGNED_TX`].
    fn submitted_hash() -> B256 {
        let raw = tn_types::hex::decode(&SIGNED_TX[2..]).expect("fixture hex");
        tn_types::keccak256(raw)
    }

    /// `eth_getTransactionCount` for [`ADDRESS`] at `block`.
    fn nonce_read(block: &str, id: u64) -> String {
        call_with("eth_getTransactionCount", &format!(r#"["{ADDRESS}","{block}"]"#), id)
    }

    /// A lookup by hash (`eth_getTransactionByHash` or
    /// `eth_getTransactionReceipt`) of `hash`.
    fn lookup(method: &str, hash: &str, id: u64) -> String {
        call_with(method, &format!(r#"["{hash}"]"#), id)
    }

    /// The issue's acceptance case: a client that submits and then asks for
    /// its pending nonce must reach the worker that took the submission, whose
    /// pool counts it, not the query upstream, which has not seen it.
    #[tokio::test]
    async fn pending_nonce_read_goes_to_the_worker_under_redirect() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        let (status, text) = post_rpc(gateway, None, nonce_read("pending", 2)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        // the tag matches in any ASCII case, as the worker reads it
        let (status, text) = post_rpc(gateway, None, nonce_read("Pending", 3)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));

        assert_eq!((worker_seen.hits(), worker_seen.hop_marker()), (3, 3));
        assert_eq!(query_seen.hits(), 0);
    }

    #[tokio::test]
    async fn latest_nonce_read_goes_to_the_query_upstream() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, "worker");
        let reads = [
            nonce_read("latest", 2),
            nonce_read("0x10", 3),
            // the block parameter defaults to latest
            call_with("eth_getTransactionCount", &format!(r#"["{ADDRESS}"]"#), 4),
        ];
        for body in &reads {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{body}");
        }
        assert_eq!(worker_seen.hits(), 1);
        assert_eq!(query_seen.hits(), reads.len());
    }

    #[tokio::test]
    async fn other_pending_reads_stay_on_the_query_upstream() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let target = format!(r#"{{"to":"{ADDRESS}"}}"#);
        let reads = [
            call_with("eth_call", &format!(r#"[{target},"pending"]"#), 1),
            call_with("eth_getBalance", &format!(r#"["{ADDRESS}","pending"]"#), 2),
            call_with("eth_estimateGas", &format!(r#"[{target},"pending"]"#), 3),
        ];
        for body in &reads {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{body}");
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (0, reads.len()));
    }

    #[tokio::test]
    async fn recently_forwarded_hash_lookup_goes_to_the_worker() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        let hash = submitted_hash().to_string();
        // the hash is matched by value, so its hex case does not matter
        let upper = format!("0x{}", hash[2..].to_uppercase());
        let lookups = [
            lookup("eth_getTransactionByHash", &hash, 2),
            lookup("eth_getTransactionReceipt", &hash, 3),
            lookup("eth_getTransactionReceipt", &upper, 4),
        ];
        for body in &lookups {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        assert_eq!(worker_seen.hits(), 1 + lookups.len());
        assert_eq!(query_seen.hits(), 0);
    }

    #[tokio::test]
    async fn unknown_hash_lookup_goes_to_the_query_upstream() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        let unknown = B256::repeat_byte(0x22).to_string();
        let lookups = [
            lookup("eth_getTransactionByHash", &unknown, 2),
            lookup("eth_getTransactionReceipt", &unknown, 3),
            // not a hash at all
            lookup("eth_getTransactionByHash", "0x1234", 4),
        ];
        for body in &lookups {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "{body}");
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, lookups.len()));
    }

    #[tokio::test]
    async fn recent_hash_expires_after_ttl() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        // short enough to wait out, long enough that the first lookup, one
        // local round trip after the submission, lands well inside it
        state.recent_submissions = Some(Arc::new(RecentSubmissions::new(
            Duration::from_secs(1),
            RECENT_SUBMISSIONS_CAPACITY,
        )));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 2);
        let (_, text) = post_rpc(gateway, None, receipt.clone()).await;
        assert_eq!(text, "worker", "a lookup inside the ttl follows the submission");

        tokio::time::sleep(Duration::from_millis(1_200)).await;
        let (status, text) = post_rpc(gateway, None, receipt).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"), "the hash has expired");
        assert_eq!((worker_seen.hits(), query_seen.hits()), (2, 1));
    }

    /// `--recent-submission-ttl 0` turns off only the hash routing.
    #[tokio::test]
    async fn without_recent_submissions_only_the_pending_nonce_read_stays_on_the_worker() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        state.recent_submissions = None;
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 2);
        let (_, text) = post_rpc(gateway, None, receipt).await;
        assert_eq!(text, "query");
        let (_, text) = post_rpc(gateway, None, nonce_read("pending", 3)).await;
        assert_eq!(text, "worker");
        assert_eq!((worker_seen.hits(), query_seen.hits()), (2, 1));
    }

    #[tokio::test]
    async fn rejected_submission_is_not_recorded() {
        // a worker that refuses with a 4xx, as one over its connection limit
        // answers every call
        let refusing =
            Router::new().route("/", post(|| async { (StatusCode::TOO_MANY_REQUESTS, "worker") }));
        let (worker, _worker) = spawn(refusing).await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // the text answer reaches the client as the gateway's envelope for an
        // error status without a json-rpc body, keeping the status
        let (status, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error_code_and_id(&text), (-32012, serde_json::json!(1)));
        assert!(!recent.contains(&submitted_hash()));

        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 2);
        let (status, text) = post_rpc(gateway, None, receipt).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!(query_seen.hits(), 1);
    }

    /// jsonrpsee answers a transaction the worker refuses (unfunded, nonce too
    /// low, an already-mined transaction replayed) with a `200` and a JSON-RPC
    /// `error`, so the status alone cannot tell a refusal from an admission.
    #[tokio::test]
    async fn submission_rejected_inside_a_200_is_not_recorded() {
        const REFUSED: &str =
            r#"{"jsonrpc":"2.0","error":{"code":-32003,"message":"insufficient funds"},"id":1}"#;
        let rejecting = Router::new()
            .route("/", post(|| async { ([(header::CONTENT_TYPE, "application/json")], REFUSED) }));
        let (worker, _worker) = spawn(rejecting).await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, REFUSED));
        assert!(!recent.contains(&submitted_hash()));

        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 2);
        let (status, text) = post_rpc(gateway, None, receipt).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!(query_seen.hits(), 1);
    }

    /// Reading the answer whole to look for a `result` must not change what
    /// the client gets: the worker's status, content type and bytes.
    #[tokio::test]
    async fn accepted_submission_is_recorded_and_its_answer_passed_through() {
        const CONTENT_TYPE: &str = "application/json; charset=utf-8";
        let accepting = Router::new().route(
            "/",
            post(|| async {
                (StatusCode::ACCEPTED, [(header::CONTENT_TYPE, CONTENT_TYPE)], ACCEPTED)
            }),
        );
        let (worker, _worker) = spawn(accepting).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(submission(1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()),
            Some(CONTENT_TYPE)
        );
        assert_eq!(response.bytes().await.expect("bytes").as_ref(), ACCEPTED.as_bytes());
        assert!(recent.contains(&submitted_hash()));
    }

    /// The answer echoes the client's `id`, so the read cap grows with the
    /// request: a submission whose id alone is past the fixed slack is still
    /// remembered, and its answer reaches the client whole.
    #[tokio::test]
    async fn submission_with_a_long_id_is_recorded() {
        let echoing = Router::new().route(
            "/",
            post(|body: Bytes| async move {
                let call: serde_json::Value = serde_json::from_slice(&body).expect("json call");
                format!(r#"{{"jsonrpc":"2.0","result":"0x11","id":{}}}"#, call["id"])
            }),
        );
        let (worker, _worker) = spawn(echoing).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let id = "i".repeat(4 * SUBMISSION_ANSWER_SLACK);
        let body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{SIGNED_TX}"],"id":"{id}"}}"#
        );
        let (status, text) = post_rpc(gateway, None, body).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(text, format!(r#"{{"jsonrpc":"2.0","result":"0x11","id":"{id}"}}"#));
        assert!(recent.contains(&submitted_hash()));
    }

    /// An answer past the request's length plus the slack is not one a healthy
    /// worker sends; the gateway stops reading, so the answer is lost and the
    /// client gets the lost-answer error, which says the transaction may have
    /// been accepted, rather than a truncated body.
    #[tokio::test]
    async fn submission_answer_past_the_cap_is_lost() {
        let padding = "p".repeat(64 * 1024);
        let bloated = Router::new().route(
            "/",
            post(move || async move {
                format!(r#"{{"jsonrpc":"2.0","result":"0x11","padding":"{padding}","id":1}}"#)
            }),
        );
        let (worker, _worker) = spawn(bloated).await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, submission(7)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32013, serde_json::json!(7)));
        assert!(text.contains("may have been accepted"), "{text}");
        assert!(!recent.contains(&submitted_hash()));
    }

    /// A submission's answer read whole counts like a streamed one: its head
    /// as `forwarded` on `routed_requests_total`, and a lost answer as
    /// `body_failed` after it, never as `unreachable`. `requests_total`
    /// counts the answer passed on as `forwarded` and the lost one, once, as
    /// `rejected`.
    #[tokio::test]
    async fn submission_answers_count_like_streamed_answers() {
        let mut metrics = CapturedMetrics::install();
        let (worker, _worker_seen, _worker) = accepting_worker_mock().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        let (status, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, ACCEPTED));
        let series = |result: &str| {
            vec![
                ("result".to_string(), result.to_string()),
                ("route".to_string(), "worker".to_string()),
            ]
        };
        let routed = "tn_worker_gateway_routed_requests_total";
        let requests = "tn_worker_gateway_requests_total";
        assert_eq!(metrics.series(routed), vec![(series("forwarded"), 1.0)]);
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 1.0);

        // a worker whose answer breaks off mid-body
        let state = redirect_state(truncating_mock().await, None);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (truncating, _truncating) = spawn(test_router(state)).await;
        let (status, text) = post_rpc(truncating, None, submission(7)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(error_code_and_id(&text), (-32013, serde_json::json!(7)));
        assert!(!recent.contains(&submitted_hash()));

        // the lost answer's head counts as forwarded, then its failure; on
        // requests_total it counts only as rejected
        assert_eq!(
            metrics.series(routed),
            vec![(series("body_failed"), 1.0), (series("forwarded"), 2.0)]
        );
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 1.0);
        assert_eq!(metrics.value(requests, &[("outcome", "rejected")]), 1.0);
        let rejections = "tn_worker_gateway_rejections_total";
        assert_eq!(metrics.value(rejections, &[("reason", "upstream_answer_lost")]), 1.0);
    }

    /// The worker's answer to a remembered submission is read whole before
    /// `proxy()` returns and then waits in the response body for the client:
    /// until the client has taken it, the request stays in the in-flight gauge
    /// and keeps its submission slot, as a streamed answer does.
    #[tokio::test]
    async fn buffered_submission_answer_holds_its_guards_until_taken() {
        let mut metrics = CapturedMetrics::install();
        let (worker, _worker_seen, _worker) = accepting_worker_mock().await;
        let mut state = redirect_state(worker, None);
        state.submission_slots = inflight_slots(1);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let slots = Arc::clone(&state.submission_slots);
        let forwarding = TrustedProxies::default()
            .resolve("127.0.0.1".parse().expect("peer"), &HeaderMap::new());

        let response = proxy(
            State(state),
            Extension(forwarding),
            crate::proxy::PostOnly,
            None,
            None,
            None,
            Request::new(axum::body::Body::from(submission(1))),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        // the handler has returned, but nothing has taken the answer yet
        let inflight = "tn_worker_gateway_inflight_requests";
        let class = [("route", "submission")];
        assert_eq!(slots.available_permits(), 0, "the submission slot is still held");
        assert_eq!(metrics.value(inflight, &[]), 1.0);
        assert_eq!(metrics.value("tn_worker_gateway_route_inflight", &class), 1.0);

        let answer =
            axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("the answer");
        assert_eq!(answer.as_ref(), ACCEPTED.as_bytes());
        assert_eq!(slots.available_permits(), 1, "taking the answer frees the slot");
        assert_eq!(metrics.value(inflight, &[]), 0.0);
        assert_eq!(metrics.value("tn_worker_gateway_route_inflight", &class), 0.0);
        assert!(recent.contains(&submitted_hash()));
    }

    /// The held answer keeps an exact size, so the client gets it with a
    /// `Content-Length`, as it did when the answer went out in one piece.
    #[tokio::test]
    async fn buffered_submission_answer_keeps_its_content_length() {
        let (worker, _worker_seen, _worker) = accepting_worker_mock().await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let response = Client::new()
            .post(format!("http://{gateway}/"))
            .body(submission(1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        let length = u64::try_from(ACCEPTED.len()).expect("fits");
        assert_eq!(response.content_length(), Some(length));
        assert_eq!(response.bytes().await.expect("bytes").as_ref(), ACCEPTED.as_bytes());
    }

    #[tokio::test]
    async fn batches_keep_the_existing_classification() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // remember the submission, so only the batch shape decides below
        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        let pending = nonce_read("pending", 2);
        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 3);
        let batches = [
            (format!("[{pending}]"), "query"),
            (format!("[{receipt}]"), "query"),
            (format!("[{pending},{receipt}]"), "query"),
            (format!("[{},{pending}]", submission(4)), "query"),
            (format!("[{}]", submission(5)), "worker"),
        ];
        for (body, expected) in &batches {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, *expected), "{body}");
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (2, 4));
    }

    #[tokio::test]
    async fn worker_bound_reads_are_readiness_gated() {
        let (worker, worker_seen, _worker) = accepting_worker_mock().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        let readiness = Arc::clone(&state.readiness);
        readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // remember a submission while the worker is up, then lose the worker
        let (_, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!(text, ACCEPTED);
        readiness.set_ready(0, false);

        let receipt = lookup("eth_getTransactionReceipt", &submitted_hash().to_string(), 3);
        for (body, id) in [(nonce_read("pending", 2), 2), (receipt, 3)] {
            let (status, text) = post_rpc(gateway, None, body).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(error_code_and_id(&text), (-32000, serde_json::json!(id)));
        }
        // with no fallback to the query upstream, which has not seen the pool;
        // every other read keeps working there
        let (status, text) = post_rpc(gateway, None, nonce_read("latest", 4)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "query"));
        assert_eq!((worker_seen.hits(), query_seen.hits()), (1, 1));
    }

    /// A worker that answers every submission with `worker` and never answers
    /// anything else, counting the requests it receives.
    async fn worker_stalling_reads() -> (SocketAddr, Arc<AtomicUsize>, Notifier) {
        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&hits);
        let mock = Router::new().route(
            "/",
            post(move |body: String| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    if !body.contains("eth_sendRawTransaction") {
                        future::pending::<()>().await;
                    }
                    "worker"
                }
            }),
        );
        let (addr, server) = spawn(mock).await;
        (addr, hits, server)
    }

    /// Send `count` pending nonce reads (ids from 100) that a
    /// [`worker_stalling_reads`] mock holds unanswered, and wait until it has
    /// received them all.
    async fn hold_pending_nonce_reads(
        gateway: SocketAddr,
        worker_hits: &AtomicUsize,
        count: usize,
    ) {
        for (_, id) in (0..count).zip(100_u64..) {
            tokio::spawn(post_rpc(gateway, None, nonce_read("pending", id)));
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker_hits.load(Ordering::SeqCst) < count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled pending nonce reads should reach the worker");
    }

    /// With a redirect, a pending nonce read stays on the worker as a query,
    /// so while the worker is slow such reads alone could take every worker
    /// slot and leave submissions a `503`. They hold at most a fifth of the
    /// worker slots instead.
    #[tokio::test]
    async fn submissions_succeed_while_pool_reads_hold_their_worker_share() {
        let (worker, worker_hits, _worker) = worker_stalling_reads().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        // a worker cap of 10 gives the reads kept on the worker 2 of its slots
        // (see `worker_pool_read_cap` in cli.rs)
        state.upstream_slots = inflight_slots(10);
        state.pool_read_slots = inflight_slots(2);
        state.readiness.set_ready(0, true);
        let upstream_slots = Arc::clone(&state.upstream_slots);
        let (gateway, _gateway) = spawn(test_router(state)).await;

        hold_pending_nonce_reads(gateway, &worker_hits, 2).await;
        assert_eq!(upstream_slots.available_permits(), 8);

        // a third is over the share and is refused at once, though eight
        // worker slots are free
        let (status, text) = tokio::time::timeout(
            Duration::from_secs(1),
            post_rpc(gateway, None, nonce_read("pending", 3)),
        )
        .await
        .expect("a pool read over its share must be refused, not forwarded");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{text}");
        assert_eq!(error_code_and_id(&text), (-32009, serde_json::json!(3)));

        // the submission does not count against that share
        let (status, text) =
            tokio::time::timeout(Duration::from_secs(1), post_rpc(gateway, None, submission(1)))
                .await
                .expect("a submission must not wait behind stalled pool reads");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!((worker_hits.load(Ordering::SeqCst), query_seen.hits()), (3, 0));
    }

    #[tokio::test]
    async fn pending_nonce_reads_are_ordinary_queries_without_a_redirect() {
        let (worker, worker_hits, _worker) = worker_stalling_reads().await;
        let mut state = redirect_state(worker, None);
        state.upstream_slots = inflight_slots(10);
        state.pool_read_slots = inflight_slots(2);
        state.readiness.set_ready(0, true);
        let (gateway, _gateway) = spawn(test_router(state)).await;

        // without a redirect every read is a worker forward bounded by the
        // query cap alone, so the pool-read share does not apply
        hold_pending_nonce_reads(gateway, &worker_hits, 3).await;
        let (status, text) =
            tokio::time::timeout(Duration::from_secs(1), post_rpc(gateway, None, submission(1)))
                .await
                .expect("a submission must not wait behind stalled reads");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_hits.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn pool_reads_are_unlimited_when_the_worker_cap_is() {
        let (worker, worker_hits, _worker) = worker_stalling_reads().await;
        let (query, query_seen, _query) = named_mock("query").await;
        let mut state = redirect_state(worker, Some(query));
        // `--max-upstream-inflight 0` leaves the pool-read share unlimited too
        // (see `worker_pool_read_cap` in cli.rs)
        state.upstream_slots = inflight_slots(0);
        state.pool_read_slots = inflight_slots(0);
        state.readiness.set_ready(0, true);
        let (gateway, _gateway) = spawn(test_router(state)).await;

        // more than the default share of 20
        hold_pending_nonce_reads(gateway, &worker_hits, 25).await;
        let (status, text) =
            tokio::time::timeout(Duration::from_secs(1), post_rpc(gateway, None, submission(1)))
                .await
                .expect("a submission must not wait behind stalled reads");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!((worker_hits.load(Ordering::SeqCst), query_seen.hits()), (26, 0));
    }

    #[test]
    fn recent_submissions_forget_the_oldest_past_capacity() {
        let recent = RecentSubmissions::new(Duration::from_secs(60), 2);
        let [first, second, third] = [1, 2, 3].map(B256::repeat_byte);
        recent.record(first);
        recent.record(second);
        // already remembered: neither queued twice nor refreshed
        recent.record(first);
        recent.record(third);
        assert!(!recent.contains(&first));
        assert!(recent.contains(&second));
        assert!(recent.contains(&third));
    }

    /// The expiry boundary, driven with synthetic instants instead of a sleep:
    /// the hash is recorded the way the proxy records it, its forward time is
    /// read back from the queue, and the entries are then aged by hand.
    #[test]
    fn recent_submissions_forget_a_hash_at_its_ttl() {
        let ttl = Duration::from_secs(60);
        let recent = RecentSubmissions::new(ttl, RECENT_SUBMISSIONS_CAPACITY);
        let hash = B256::repeat_byte(1);
        recent.record(hash);

        let mut entries = recent.lock();
        let (_, start) = *entries.order.front().expect("hash recorded");
        entries.forget_expired(start + ttl - Duration::from_nanos(1), ttl);
        assert!(entries.index.contains(&hash), "kept a nanosecond short of the ttl");
        entries.forget_expired(start + ttl, ttl);
        assert!(!entries.index.contains(&hash), "forgotten at exactly the ttl");
        assert!(entries.order.is_empty(), "the queue and the index agree");
    }

    /// The per-client table counts by client key, not by bare peer address,
    /// and forgets a key once its last slot is returned. Network-free, so it
    /// covers the IPv6 and IPv4-mapped peers the loopback tests below cannot
    /// produce.
    #[test]
    fn client_cap_counts_by_prefix_key_and_forgets_idle_keys() {
        let cap = NonZeroUsize::new(1).expect("nonzero");
        let connections = Arc::new(ClientConnections::new(cap, PrefixPolicy::default()));

        // Two addresses in one /64 are one client: the second is refused with
        // the /64 key.
        let first: IpAddr = "2001:db8:0:1::1".parse().expect("addr");
        let rotated: IpAddr = "2001:db8:0:1::2".parse().expect("addr");
        let v6_slot = connections.try_acquire(first).expect("first connection of the /64");
        let refused = connections.try_acquire(rotated).expect_err("the /64 is at its cap");
        assert_eq!(refused, "2001:db8:0:1::".parse::<IpAddr>().expect("addr"));

        // A dual-stack listener's mapped IPv4 peer counts against the bare
        // IPv4 key.
        let bare = IpAddr::from([10, 0, 0, 1]);
        let mapped = IpAddr::V6(std::net::Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped());
        let v4_slot = connections.try_acquire(mapped).expect("first connection of 10.0.0.1");
        let refused = connections.try_acquire(bare).expect_err("10.0.0.1 is at its cap");
        assert_eq!(refused, bare);

        // Returning every slot leaves no entry behind.
        drop(v6_slot);
        drop(v4_slot);
        assert!(connections.lock().is_empty(), "a zero count must not keep its entry");
    }

    /// The per-client connection cap. Each test plays several clients by
    /// binding distinct loopback source addresses, which Linux routes to
    /// loopback without configuration (other platforms only configure
    /// `127.0.0.1`), hence the gate.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    mod per_client_cap {
        use super::*;
        use tokio::net::TcpSocket;

        /// Three clients, one prefix each under the default `/32` IPv4 policy.
        const CLIENT_A: [u8; 4] = [127, 0, 0, 2];
        const CLIENT_B: [u8; 4] = [127, 0, 0, 3];
        const CLIENT_C: [u8; 4] = [127, 0, 0, 4];

        /// Long enough for any of these tests to run on a loaded host, short
        /// enough that a client stuck in the accept backlog fails the test
        /// long before the header read deadline would free a slot for it.
        const SERVED_WITHIN: Duration = Duration::from_secs(2);

        /// `test_limits` with a per-client cap (`0` disables it) and a global
        /// cap, and a header read deadline far beyond any test so idle
        /// sockets keep holding their slots.
        fn cap_limits(per_client: usize, max_connections: usize) -> ServerLimits {
            ServerLimits {
                header_read_timeout: Duration::from_secs(60),
                max_connections: NonZeroUsize::new(max_connections).expect("nonzero"),
                max_connections_per_ip: NonZeroUsize::new(per_client),
                ..test_limits()
            }
        }

        /// A gateway whose only route under test is `/health`.
        fn health_router() -> Router {
            test_router(test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]))
        }

        /// Connect to `server` from the loopback address `source`.
        async fn connect_from(source: [u8; 4], server: SocketAddr) -> TcpStream {
            let socket = TcpSocket::new_v4().expect("socket");
            socket.bind(SocketAddr::from((source, 0))).expect("bind the source address");
            socket.connect(server).await.expect("connect")
        }

        /// `GET /health` from `source` on a fresh connection; the raw
        /// response, empty when the gateway closed the connection unanswered.
        async fn health_from(source: [u8; 4], server: SocketAddr) -> std::io::Result<String> {
            let mut stream = connect_from(source, server).await;
            stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: gateway\r\nConnection: close\r\n\r\n")
                .await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok(String::from_utf8_lossy(&response).into_owned())
        }

        /// `health_from`, required to answer `200` within [`SERVED_WITHIN`].
        async fn assert_served(source: [u8; 4], server: SocketAddr, why: &str) {
            let response = tokio::time::timeout(SERVED_WITHIN, health_from(source, server))
                .await
                .unwrap_or_else(|_| panic!("{why}: no response within {SERVED_WITHIN:?}"))
                .expect("read the response");
            assert!(response.starts_with("HTTP/1.1 200"), "{why}: got {response:?}");
        }

        /// Assert the gateway closes `stream` within one second without a
        /// single response byte: a read sees EOF, or a reset.
        async fn assert_closed_at_once(stream: &mut TcpStream) {
            let mut buf = [0_u8; 16];
            let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut buf))
                .await
                .expect("a connection over the cap must be closed at once");
            if let Ok(read) = read {
                assert_eq!(read, 0, "a connection over the cap must get no response");
            }
        }

        #[tokio::test]
        async fn one_prefix_at_the_cap_does_not_block_a_second_prefix() {
            let (addr, _shutdown) = spawn_with_limits(health_router(), cap_limits(2, 8)).await;

            // The issue's flood: one client opens as many idle sockets as the
            // gateway has slots. The cap keeps two and closes the rest, so the
            // global slots they would have held stay free; without it all
            // eight are held until the header read deadline.
            let mut sockets = Vec::new();
            for _ in 0..8 {
                sockets.push(connect_from(CLIENT_A, addr).await);
            }

            assert_served(CLIENT_B, addr, "a second prefix must be served").await;
        }

        #[tokio::test]
        async fn excess_connection_from_one_prefix_is_closed_at_once() {
            // One global slot beyond the per-client cap: if the refused
            // connection kept its permit, the last client could not get in.
            let (addr, _shutdown) = spawn_with_limits(health_router(), cap_limits(2, 3)).await;

            let _held = [connect_from(CLIENT_A, addr).await, connect_from(CLIENT_A, addr).await];
            let mut excess = connect_from(CLIENT_A, addr).await;
            assert_closed_at_once(&mut excess).await;

            assert_served(CLIENT_C, addr, "the refused connection's global slot must be free")
                .await;
        }

        #[tokio::test]
        async fn closing_a_connection_frees_its_slot() {
            let (addr, _shutdown) = spawn_with_limits(health_router(), cap_limits(1, 64)).await;

            let held = connect_from(CLIENT_A, addr).await;
            let mut excess = connect_from(CLIENT_A, addr).await;
            assert_closed_at_once(&mut excess).await;

            // The gateway sees the client's close on its own schedule, so
            // retry until the prefix is served again; a slot that is never
            // returned keeps refusing until the timeout fails the test.
            drop(held);
            let served = tokio::time::timeout(SERVED_WITHIN, async {
                loop {
                    match health_from(CLIENT_A, addr).await {
                        Ok(response) if response.starts_with("HTTP/1.1 200") => break,
                        _ => tokio::time::sleep(Duration::from_millis(10)).await,
                    }
                }
            })
            .await;
            assert!(served.is_ok(), "closing the held connection must free its slot");
        }

        #[tokio::test]
        async fn refused_connections_are_counted() {
            // `#[tokio::test]` runs every task on this thread, so a
            // thread-local recorder sees the accept loop's metrics.
            let mut metrics = CapturedMetrics::install();
            let (addr, _shutdown) = spawn_with_limits(health_router(), cap_limits(1, 64)).await;

            let _held = connect_from(CLIENT_A, addr).await;
            for _ in 0..3 {
                let mut excess = connect_from(CLIENT_A, addr).await;
                assert_closed_at_once(&mut excess).await;
            }
            // A connection under its prefix's cap is served and not counted.
            assert_served(CLIENT_B, addr, "a second prefix must be served").await;

            assert_eq!(
                metrics.value(
                    "tn_worker_gateway_connections_refused_total",
                    &[("reason", "per_ip_cap")]
                ),
                3.0
            );
        }

        #[tokio::test]
        async fn zero_disables_the_cap() {
            let (addr, _shutdown) = spawn_with_limits(health_router(), cap_limits(0, 64)).await;

            // More idle sockets than the default cap, all from one prefix.
            let mut sockets = Vec::new();
            for _ in 0..=DEFAULT_MAX_CONNECTIONS_PER_IP {
                sockets.push(connect_from(CLIENT_A, addr).await);
            }

            assert_served(CLIENT_A, addr, "an uncapped prefix must still be served").await;
        }

        /// A trusted front's connections are not counted: its requests are
        /// attributed by `X-Forwarded-For` one at a time, so counting the
        /// front would put every client behind it under one cap.
        #[tokio::test]
        async fn trusted_front_is_not_held_to_the_per_client_cap() {
            let front = "127.0.0.2/32";
            let trusted = Arc::new(front.parse::<TrustedProxies>().expect("trusted proxies"));
            let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
            let app = identity_router(state, None, front);
            let (addr, _shutdown) = spawn_trusting(app, cap_limits(2, 8), trusted).await;

            // the front holds twice the cap and is still served
            let mut sockets = Vec::new();
            for _ in 0..4 {
                sockets.push(connect_from(CLIENT_A, addr).await);
            }
            assert_served(CLIENT_A, addr, "a trusted front must not be capped").await;

            // a client of the same gateway is still held to the cap
            sockets.push(connect_from(CLIENT_B, addr).await);
            sockets.push(connect_from(CLIENT_B, addr).await);
            let mut excess = connect_from(CLIENT_B, addr).await;
            assert_closed_at_once(&mut excess).await;
        }

        /// Open a connection from the loopback front with a PROXY header
        /// naming `source`, and hold it open once a keep-alive `GET /health`
        /// on it is answered, by which point it holds its client's slot.
        async fn hold_proxied(gateway: SocketAddr, source: &str) -> TcpStream {
            let mut stream = TcpStream::connect(gateway).await.expect("connect");
            let head = b"GET /health HTTP/1.1\r\nHost: gateway\r\n\r\n";
            let preamble = proxy_v2_header(socket(source), &[]);
            stream.write_all(&[&preamble[..], &head[..]].concat()).await.expect("write");
            let mut response = Vec::new();
            let mut buf = [0_u8; 512];
            while !response.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = tokio::time::timeout(SERVED_WITHIN, stream.read(&mut buf))
                    .await
                    .expect("the held connection must be answered")
                    .expect("read");
                assert_ne!(read, 0, "the held connection must stay open");
                response.extend_from_slice(&buf[..read]);
            }
            assert!(
                response.starts_with(b"HTTP/1.1 200"),
                "{}",
                String::from_utf8_lossy(&response)
            );
            stream
        }

        /// Behind an L4 front sending PROXY headers every connection comes from
        /// the front, and the cap counts each one against the source its
        /// header names.
        #[tokio::test]
        async fn proxy_sourced_client_is_capped_on_its_proxy_source() {
            let (worker, _worker) = forwarding_echo().await;
            let state = redirect_state(worker, None);
            state.readiness.set_ready(0, true);
            let trusted = Arc::new("127.0.0.1/32".parse::<TrustedProxies>().expect("trusted"));
            let app = router(
                state,
                Duration::from_secs(5),
                MAX_REQUEST_BYTES,
                None,
                RouterLayers::default(),
                Arc::clone(&trusted),
            );
            let limits =
                ServerLimits { proxy_protocol: Some(Arc::clone(&trusted)), ..cap_limits(1, 64) };
            let (gateway, _shutdown) = spawn_trusting(app, limits, trusted).await;

            let _held = hold_proxied(gateway, "198.51.100.1:4711").await;
            // that client's next connection is over its cap: closed unanswered
            let same = proxy_v2_header(socket("198.51.100.1:4712"), &[]);
            let response = raw_exchange(gateway, &same, "").await;
            assert!(response.is_empty(), "the source is at its cap: {response}");
            // another client behind the same front has a slot of its own
            let other = proxy_v2_header(socket("198.51.100.2:4711"), &[]);
            let response = raw_exchange(gateway, &other, "").await;
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            assert!(response.contains("198.51.100.2|http"), "{response}");
        }
    }

    #[tokio::test]
    async fn non_post_requests_get_a_405_envelope() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // Every body below is over this 8-byte limit, so a 405 rather than a
        // 413 shows the method is checked before the body is read.
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            8,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let client = Client::new();
        // a POST to a probe path is refused too: the probe routes refuse it
        // before the proxy
        const NON_POST: &str = "only POST is accepted";
        const NON_GET: &str = "only GET and HEAD are accepted on this path";
        for (method, path, allow, message) in [
            (Method::GET, "/", "POST", NON_POST),
            (Method::PUT, "/", "POST", NON_POST),
            (Method::DELETE, "/x", "POST", NON_POST),
            (Method::PUT, "/health", "GET,HEAD", NON_GET),
            (Method::POST, "/ready", "GET,HEAD", NON_GET),
        ] {
            let response = client
                .request(method.clone(), format!("http://{gateway}{path}"))
                .body(call("eth_chainId", 1))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
            assert_eq!(
                response.headers().get(header::ALLOW),
                Some(&HeaderValue::from_static(allow)),
                "{method} {path}"
            );
            let body: serde_json::Value = response.json().await.expect("json error body");
            assert_eq!(body["error"]["code"], -32600, "{method} {path}");
            assert_eq!(body["error"]["message"], message, "{method} {path}");
            assert_eq!(body["id"], serde_json::Value::Null, "{method} {path}");
        }
        assert_eq!(worker_seen.hits(), 0, "a non-POST request must never reach the upstream");

        // the probes still answer GET
        let response = client.get(format!("http://{gateway}/health")).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Every probe path refuses a method other than `GET` or `HEAD` with a
    /// message that agrees with its `Allow: GET,HEAD`, not the proxy's "only
    /// POST is accepted", and never forwards it.
    #[tokio::test]
    async fn probe_paths_refuse_other_methods_with_a_get_head_message() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            1024,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let client = Client::new();
        for path in [HEALTH_PATH, READY_PATH, READY_ANY_PATH] {
            for method in [Method::POST, Method::DELETE] {
                let response = client
                    .request(method.clone(), format!("http://{gateway}{path}"))
                    .body(call("eth_chainId", 1))
                    .send()
                    .await
                    .expect("send");
                assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{method} {path}");
                assert_eq!(
                    response.headers().get(header::ALLOW),
                    Some(&HeaderValue::from_static("GET,HEAD")),
                    "{method} {path}"
                );
                let body: serde_json::Value = response.json().await.expect("json error body");
                assert_eq!(body["error"]["code"], -32600, "{method} {path}");
                assert_eq!(
                    body["error"]["message"], "only GET and HEAD are accepted on this path",
                    "{method} {path}"
                );
                assert_eq!(body["id"], serde_json::Value::Null, "{method} {path}");
            }
        }
        assert_eq!(worker_seen.hits(), 0, "a probe-path request must never reach the upstream");
    }

    /// An accept error about the one connection (its peer aborted it, or a
    /// network error already pending on it) is retried at once; one about the
    /// process (fd exhaustion, out of memory) takes the back-off.
    #[test]
    fn accept_connection_errors_are_retried_at_once() {
        use std::io::{Error, ErrorKind};
        assert!(is_connection_error(&Error::from(ErrorKind::ConnectionAborted)));
        assert!(is_connection_error(&Error::from(ErrorKind::ConnectionReset)));
        // `EPROTO`, a pending network error `accept(2)` returns on Linux
        assert_eq!(
            is_connection_error(&Error::from_raw_os_error(71)),
            cfg!(any(target_os = "android", target_os = "linux"))
        );
        // `EMFILE`
        assert!(!is_connection_error(&Error::from_raw_os_error(24)));
        assert!(!is_connection_error(&Error::from(ErrorKind::OutOfMemory)));
    }

    /// A query upstream that answers every POST with an HTML page labelled
    /// `content_type` (no `Content-Type` at all when `None`).
    async fn labelled_query_mock(content_type: Option<&'static str>) -> (SocketAddr, Notifier) {
        let mock = Router::new().route(
            "/",
            post(move || async move {
                let mut response =
                    Response::new(axum::body::Body::from("<html><script>alert(1)</script></html>"));
                if let Some(content_type) = content_type {
                    response
                        .headers_mut()
                        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
                }
                response
            }),
        );
        spawn(mock).await
    }

    /// POST a query through a gateway whose `--redirect-queries` URL answers
    /// with `content_type`, and return the response the client gets.
    async fn query_through_labelled_upstream(
        content_type: Option<&'static str>,
    ) -> reqwest::Response {
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let (query, _query) = labelled_query_mock(content_type).await;
        let (gateway, _shutdown) = spawn(test_router(redirect_state(worker, Some(query)))).await;
        Client::new()
            .post(format!("http://{gateway}/"))
            .body(call("eth_call", 1))
            .send()
            .await
            .expect("send")
    }

    /// A query upstream's `2xx` without a JSON content type (a CDN challenge
    /// or a captive page, say) carries no JSON-RPC answer: the client gets the
    /// upstream's status with the `-32012` error, labelled JSON, in its place.
    #[tokio::test]
    async fn query_route_non_json_content_type_is_replaced() {
        for content_type in [
            Some("text/html; charset=utf-8"),
            Some("text/plain"),
            Some("image/svg+xml"),
            Some("application/javascript"),
            Some("application/jsonp"),
            Some("application/json-seq"),
            Some("text/foo+json"),
            Some("application/json;a=b,text/html"),
            Some("application/json;,text/html"),
            Some("application/json, text/html"),
            None,
        ] {
            let response = query_through_labelled_upstream(content_type).await;
            assert_eq!(response.status(), StatusCode::OK, "{content_type:?}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json")),
                "{content_type:?}"
            );
            assert_eq!(
                response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
                Some(&HeaderValue::from_static("nosniff")),
                "{content_type:?}"
            );
            let text = response.text().await.expect("text");
            assert_eq!(
                error_code_and_id(&text),
                (-32012, serde_json::json!(1)),
                "{content_type:?}"
            );
            assert!(text.contains("upstream returned 200"), "{content_type:?}: {text}");
        }
    }

    #[tokio::test]
    async fn query_route_json_content_type_passes_through() {
        for content_type in [
            "application/json",
            "application/json; charset=utf-8",
            "Application/JSON",
            "application/problem+json",
        ] {
            let response = query_through_labelled_upstream(Some(content_type)).await;
            assert_eq!(response.status(), StatusCode::OK, "{content_type}");
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static(content_type)),
            );
        }
    }

    #[tokio::test]
    async fn every_response_carries_nosniff() {
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // one request a second with a burst of two: the probe-path PUT (no
        // longer exempt, only GET and HEAD are) and the first POST take the two
        // tokens, so the second POST is shed by the rate limiter, the layer just
        // inside the nosniff one; the GET probes are exempt
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(2))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers { rate_limiters: Some(limiters), ..RouterLayers::default() },
            Arc::default(),
        ))
        .await;

        let client = Client::new();
        let url = |path: &str| format!("http://{gateway}{path}");
        let cases = [
            ("method refused", client.put(url("/health")), StatusCode::METHOD_NOT_ALLOWED),
            ("proxied", client.post(url("/")).body(call("eth_chainId", 1)), StatusCode::OK),
            (
                "rate limited",
                client.post(url("/")).body(call("eth_chainId", 2)),
                StatusCode::TOO_MANY_REQUESTS,
            ),
            ("liveness", client.get(url("/health")), StatusCode::OK),
            ("readiness", client.get(url("/ready")), StatusCode::OK),
        ];
        for (case, request, status) in cases {
            let response = request.send().await.expect("send");
            assert_eq!(response.status(), status, "{case}");
            assert_eq!(
                response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
                Some(&HeaderValue::from_static("nosniff")),
                "{case}"
            );
        }
    }

    /// `/ready/any` sits outside the router's other layers but answers like
    /// the other probes: `nosniff` on every answer, and the `405` envelope
    /// with `Allow: GET,HEAD` for any method but `GET` and `HEAD`, `POST`
    /// included, which never reaches an upstream.
    #[tokio::test]
    async fn ready_any_refuses_other_methods_like_the_probes() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        let url = format!("http://{gateway}{READY_ANY_PATH}");
        let nosniff = |response: &reqwest::Response| {
            response.headers().get(header::X_CONTENT_TYPE_OPTIONS)
                == Some(&HeaderValue::from_static("nosniff"))
        };

        let response = Client::new().get(&url).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(nosniff(&response), "GET");

        for method in [Method::POST, Method::PUT] {
            let response = Client::new()
                .request(method.clone(), &url)
                .body(call("eth_chainId", 1))
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{method}");
            assert_eq!(
                response.headers().get(header::ALLOW),
                Some(&HeaderValue::from_static("GET,HEAD")),
                "{method}"
            );
            assert!(nosniff(&response), "{method}");
            let body: serde_json::Value = response.json().await.expect("json error body");
            assert_eq!(body["error"]["code"], -32600, "{method}");
            assert_eq!(body["id"], serde_json::Value::Null, "{method}");
        }
        assert_eq!(worker_seen.hits(), 0, "a probe path must never reach the upstream");
    }

    /// A worker that sends each answer's head at once and its body, `ACCEPTED`,
    /// `delay` later.
    async fn slow_body_mock(delay: Duration) -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.expect("accept");
                tokio::spawn(async move {
                    read_request(&mut stream).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                         Content-Length: {}\r\n\r\n",
                        ACCEPTED.len()
                    );
                    stream.write_all(head.as_bytes()).await.expect("write head");
                    tokio::time::sleep(delay).await;
                    stream.write_all(ACCEPTED.as_bytes()).await.expect("write body");
                });
            }
        });
        addr
    }

    /// The request deadline cuts a request only until its upstream answers:
    /// once the head is in, the answer reaches the client whether it streams
    /// through or is read whole first (a submission whose hash is remembered),
    /// and the forward counts once, as `forwarded`, with no `request_timeout`.
    #[tokio::test]
    async fn deadline_after_the_answer_head_keeps_the_answer() {
        let mut metrics = CapturedMetrics::install();
        let state = redirect_state(slow_body_mock(Duration::from_millis(600)).await, None);
        state.readiness.set_ready(0, true);
        let recent = state.recent_submissions.clone().expect("hash routing on");
        let (gateway, _shutdown) = spawn(router(
            state,
            Duration::from_millis(300),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        // read whole inside the handler, past the deadline
        let (status, text) = post_rpc(gateway, None, submission(1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, ACCEPTED));
        assert!(recent.contains(&submitted_hash()), "the accepted submission is remembered");
        // streamed after the handler returned
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 2)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, ACCEPTED));

        let forwarded = vec![
            ("result".to_string(), "forwarded".to_string()),
            ("route".to_string(), "worker".to_string()),
        ];
        assert_eq!(
            metrics.series("tn_worker_gateway_routed_requests_total"),
            vec![(forwarded, 2.0)]
        );
        let requests = "tn_worker_gateway_requests_total";
        assert_eq!(metrics.value(requests, &[("outcome", "forwarded")]), 2.0);
        assert_eq!(metrics.value(requests, &[("outcome", "rejected")]), 0.0);
        assert_eq!(
            metrics.value("tn_worker_gateway_rejections_total", &[("reason", "request_timeout")]),
            0.0
        );
    }

    /// The header name the query-header tests configure.
    const API_KEY_HEADER: &str = "x-api-key";

    /// The secret value the query-header tests configure.
    const API_KEY: &str = "s3cr3t-k3y-1609";

    /// The `--redirect-queries-header` line the query-header tests configure.
    fn api_key_line() -> String {
        format!("X-Api-Key: {API_KEY}")
    }

    /// `redirect_state` with the test API key as the query header.
    fn keyed_redirect_state(worker: SocketAddr, query: SocketAddr) -> AppState {
        let query_header = QueryHeader::parse(&api_key_line()).expect("valid header");
        AppState { query_header: Some(query_header), ..redirect_state(worker, Some(query)) }
    }

    #[tokio::test]
    async fn header_reaches_the_query_mock_and_never_the_worker() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = keyed_redirect_state(worker, query);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let submission = call("eth_sendRawTransaction", 1);
        let sync = call("eth_sendRawTransactionSync", 2);
        let read = call("eth_getLogs", 3);
        let bodies = [
            (call("eth_call", 4), "query"),
            (submission.clone(), "worker"),
            (sync.clone(), "worker"),
            (format!("[{submission},{sync}]"), "worker"),
            (format!("[{submission},{read}]"), "query"),
            ("not json".to_string(), "query"),
        ];
        for (body, expected) in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, *expected), "{body}");
        }
        assert_eq!((worker_seen.hits(), query_seen.hits()), (3, 3));
        // every query hop carries the header exactly once, with its value,
        // and no worker hop carries it at all
        assert_eq!(query_seen.api_keys(), vec![API_KEY.to_string(); 3]);
        assert_eq!(worker_seen.api_keys(), Vec::<String>::new());
        // the markers are unchanged by the header
        assert_eq!((query_seen.redirect_marker(), query_seen.hop_marker()), (3, 0));
        assert_eq!((worker_seen.hop_marker(), worker_seen.redirect_marker()), (3, 0));
    }

    /// Why a test that asserts a throttled warning skips outside nextest: the
    /// throttles are process-wide statics, so under `cargo test` another test
    /// may already have used up the window and the warning never appears.
    const THROTTLED_WARNING_SKIP: &str = "reads a warning a process-wide throttle admits once per \
                                          10s, so it needs nextest's process-per-test model";

    /// A `MakeWriter` that appends every formatted log line to a shared
    /// buffer.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log buffer lock")).into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log buffer lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Captures every log line, at every level, from resolving the settings
    /// and running the startup chain-id check through a forward that succeeds
    /// and one that fails, with the state and settings debug-printed into the
    /// log as well. The runtime is current-thread, so the spawned servers log
    /// through the same thread-local subscriber.
    #[tokio::test]
    async fn header_value_never_appears_in_logs() {
        if !process_per_test() {
            eprintln!("skipped: {THROTTLED_WARNING_SKIP}");
            return;
        }
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;

        // startup: resolve both header forms; the public plain-http redirect
        // also takes the plaintext warning path
        let header = format!("--redirect-queries-header={}", api_key_line());
        let worker_flag = format!("--upstream-rpc-url=http://{worker}/");
        for redirect in [format!("http://{query}/"), "http://203.0.113.5:8545/".to_string()] {
            let settings = crate::cli::try_parse_test_argv([
                "worker-gateway",
                worker_flag.as_str(),
                "--upstream-readiness-url=http://127.0.0.1:1/health/workers",
                format!("--redirect-queries={redirect}").as_str(),
                header.as_str(),
            ])
            .expect("parse")
            .into_settings()
            .expect("settings resolve");
            assert!(settings.query_header.is_some());
            info!(target: "gateway", ?settings, "resolved settings");
        }

        // startup's chain-id check, against the mocks (whose answers hold no
        // chain id) and against a query upstream that is down
        let header = QueryHeader::parse(&api_key_line()).expect("valid header");
        let client = proxy_client(
            UpstreamClientBuilder::for_test(Duration::from_secs(2)),
            Duration::from_secs(5),
        )
        .expect("client");
        let worker_url = Url::parse(&format!("http://{worker}/")).expect("worker url");
        for query_url in [format!("http://{query}/"), "http://127.0.0.1:1/".to_string()] {
            let query_url = Url::parse(&query_url).expect("query url");
            let check = check_chain_ids(&client, &worker_url, &query_url, Some(&header)).await;
            log_chain_id_check(&check, &worker_url, &query_url);
        }

        // a forward to each route, then a query whose upstream is down
        let state = keyed_redirect_state(worker, query);
        info!(target: "gateway", ?state, "gateway state");
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;
        let (_, text) = post_rpc(gateway, None, call("eth_call", 1)).await;
        assert_eq!(text, "query");
        let (_, text) = post_rpc(gateway, None, call("eth_sendRawTransaction", 2)).await;
        assert_eq!(text, "worker");
        // nothing listens on port 1
        let down = keyed_redirect_state(worker, "127.0.0.1:1".parse().expect("addr"));
        down.readiness.set_ready(0, true);
        let (down, _down) = spawn(test_router(down)).await;
        let (status, _) = post_rpc(down, None, call("eth_call", 3)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);

        let captured = logs.contents();
        // the capture saw the gateway's own lines, and the header was in play
        for line in [
            "plain http",
            "resolved settings",
            "could not compare the chain ids",
            "gateway state",
            "forwarding to upstream failed",
        ] {
            assert!(captured.contains(line), "missing {line:?} in: {captured}");
        }
        assert!(captured.contains("<redacted>"), "the header should print redacted: {captured}");
        // one chain-id call and one forwarded read reached the query mock
        assert_eq!(query_seen.api_keys(), vec![API_KEY.to_string(); 2]);
        assert_eq!(worker_seen.api_keys(), Vec::<String>::new());
        assert!(!captured.contains(API_KEY), "the header value leaked into the logs: {captured}");
    }

    /// A mismatch is a warning carrying both chain ids; a match, and a check
    /// that could not compare, are logged at info.
    #[test]
    fn chain_id_mismatch_is_logged_as_a_warning_with_both_ids() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let worker = Url::parse("http://127.0.0.1:8545/").expect("worker url");
        let query = Url::parse("https://rpc.example/").expect("query url");

        log_chain_id_check(&ChainIdCheck::Mismatch { worker: 2017, query: 1 }, &worker, &query);
        log_chain_id_check(&ChainIdCheck::Match(2017), &worker, &query);
        let unchecked =
            ChainIdCheck::Unchecked { worker: None, query: Some("connection refused".to_string()) };
        log_chain_id_check(&unchecked, &worker, &query);

        let captured = logs.contents();
        let lines: Vec<&str> = captured.lines().collect();
        assert_eq!(lines.len(), 3, "{captured}");
        assert!(lines[0].contains(" WARN "), "{}", lines[0]);
        assert!(
            lines[0].contains("worker_chain_id=2017") && lines[0].contains("query_chain_id=1"),
            "{}",
            lines[0]
        );
        assert!(lines[1].contains(" INFO ") && lines[1].contains(" chain_id=2017"), "{}", lines[1]);
        assert!(
            lines[2].contains(" INFO ") && lines[2].contains("could not compare the chain ids"),
            "{}",
            lines[2]
        );
    }

    /// A mock upstream whose POST handler signals `received`, then holds the
    /// request until `release` is notified and answers `"slow"`. Both are
    /// `Notify` permits, so neither signal is lost to ordering.
    async fn gated_mock() -> (SocketAddr, Arc<Notify>, Arc<Notify>, Notifier) {
        let (received, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let (signal, gate) = (Arc::clone(&received), Arc::clone(&release));
        let mock = Router::new().route(
            "/",
            post(move || {
                let (signal, gate) = (Arc::clone(&signal), Arc::clone(&gate));
                async move {
                    signal.notify_one();
                    gate.notified().await;
                    "slow"
                }
            }),
        );
        let (addr, shutdown) = spawn(mock).await;
        (addr, received, release, shutdown)
    }

    /// GET `/ready` over a fresh connection, returning the status and the JSON
    /// body.
    async fn get_ready(gateway: SocketAddr) -> (StatusCode, serde_json::Value) {
        get_probe(gateway, READY_PATH).await
    }

    /// Open fresh connections to `addr` until one is refused (or reset, when
    /// the listener closes mid-handshake) and return how long after `since`
    /// that was. A connection still accepted is dropped and retried; panics if
    /// the listener is still open after 10s.
    async fn first_refusal(addr: SocketAddr, since: Instant) -> Duration {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match TcpStream::connect(addr).await {
                    Ok(stream) => drop(stream),
                    Err(err) => {
                        assert!(
                            matches!(
                                err.kind(),
                                std::io::ErrorKind::ConnectionRefused
                                    | std::io::ErrorKind::ConnectionReset
                            ),
                            "{err}"
                        );
                        return since.elapsed();
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the listener should close after the shutdown notice")
    }

    /// A gateway with one ready worker at `worker`, serving through the real
    /// accept loop with `shutdown_delay`, and that loop's handle.
    async fn draining_gateway(
        worker: SocketAddr,
        shutdown_delay: Duration,
    ) -> (SocketAddr, Notifier, JoinHandle<Result<(), TaskError>>) {
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        let draining = Arc::clone(&state.draining);
        spawn_draining(test_router(state), test_limits(), draining, shutdown_delay).await
    }

    /// Start a POST that the gated mock holds and return once it has reached
    /// the mock.
    async fn held_post(gateway: SocketAddr, received: &Notify) -> JoinHandle<(StatusCode, String)> {
        let in_flight = tokio::spawn(post_rpc(gateway, None, call("eth_chainId", 1)));
        tokio::time::timeout(Duration::from_secs(5), received.notified())
            .await
            .expect("the POST should reach the mock");
        in_flight
    }

    /// Keep a POST held at the gated mock across the shutdown notice, wait for
    /// the gateway's listener to refuse a new connection, then release the
    /// POST and check that the drain ends before the graceful deadline and the
    /// POST still completes. With a delay the POST is accepted during it;
    /// without one the listener closes at the notice, so the POST is accepted
    /// before the notice. Returns how long after the notice the listener
    /// closed.
    async fn listener_close_with_a_held_post(shutdown_delay: Duration) -> Duration {
        let (worker, received, release, _worker) = gated_mock().await;
        let (gateway, shutdown, accept) = draining_gateway(worker, shutdown_delay).await;

        let notify = || {
            let notice = Instant::now();
            shutdown.notify();
            notice
        };
        let (notice, in_flight) = if shutdown_delay.is_zero() {
            let in_flight = held_post(gateway, &received).await;
            (notify(), in_flight)
        } else {
            let notice = notify();
            (notice, held_post(gateway, &received).await)
        };

        let closed_after = first_refusal(gateway, notice).await;
        assert!(!in_flight.is_finished(), "the held POST must still be in flight");
        assert!(!accept.is_finished(), "the drain must wait for the held POST");

        release.notify_one();
        tokio::time::timeout(GRACEFUL_TIMEOUT + Duration::from_millis(500), accept)
            .await
            .expect("the drain should end within the graceful timeout")
            .expect("join")
            .expect("accept loop result");
        // the graceful deadline is armed when the listener closes, so it
        // cannot fire before `shutdown_delay + GRACEFUL_TIMEOUT` after the
        // notice; a drain that ended sooner waited for the held POST
        assert!(
            notice.elapsed() < shutdown_delay + GRACEFUL_TIMEOUT,
            "the drain ended at the graceful deadline, not when the held POST finished"
        );
        let (status, text) = tokio::time::timeout(Duration::from_secs(5), in_flight)
            .await
            .expect("the in-flight POST should finish during the drain")
            .expect("join");
        assert_eq!((status, text.as_str()), (StatusCode::OK, "slow"));
        closed_after
    }

    #[tokio::test]
    async fn ready_reports_503_during_the_shutdown_delay_while_posts_succeed() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        // the test never waits the delay out, so a long one costs nothing
        // and leaves the exchanges below no window to miss
        let (gateway, shutdown, _accept) = draining_gateway(worker, Duration::from_secs(5)).await;
        assert_eq!(get_ready(gateway).await, (StatusCode::OK, serde_json::json!({"ready": true})));

        shutdown.notify();

        // every request below opens a new connection, so each one is accepted
        // after the notice
        assert_eq!(
            get_ready(gateway).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({"ready": false, "draining": true})
            )
        );
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn after_the_delay_new_connections_are_refused_and_inflight_finish() {
        // long enough for the POST started after the notice to be accepted
        // before the listener closes
        let shutdown_delay = Duration::from_secs(1);
        let closed_after = listener_close_with_a_held_post(shutdown_delay).await;
        // timers never fire early, so a listener closed before the delay ran
        // out would show up here
        assert!(
            closed_after >= shutdown_delay,
            "the listener closed {closed_after:?} after the notice, inside the {shutdown_delay:?} delay"
        );
    }

    #[tokio::test]
    async fn zero_delay_keeps_the_old_behaviour() {
        // no shutdown in progress: the `/ready` body is the old one, with no
        // `draining` key
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let (gateway, _shutdown, _accept) = draining_gateway(worker, Duration::ZERO).await;
        let (status, body) = get_ready(gateway).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.to_string(), r#"{"ready":true}"#);

        // the listener closes on the notice and the in-flight POST still
        // finishes
        listener_close_with_a_held_post(Duration::ZERO).await;
    }

    #[tokio::test]
    async fn ready_any_reports_503_during_the_shutdown_delay_while_posts_succeed() {
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        // the test never waits the delay out, so a long one costs nothing
        // and leaves the exchanges below no window to miss
        let (gateway, shutdown, _accept) = draining_gateway(worker, Duration::from_secs(5)).await;
        assert_eq!(
            get_probe(gateway, READY_ANY_PATH).await,
            (
                StatusCode::OK,
                serde_json::json!({"ready": true, "submissions": true, "queries": false})
            )
        );

        shutdown.notify();

        // every request below opens a new connection, so each one is accepted
        // after the notice; the worker is still ready, only draining turns
        // the probe
        assert_eq!(
            get_probe(gateway, READY_ANY_PATH).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                serde_json::json!({
                    "ready": false,
                    "submissions": true,
                    "queries": false,
                    "draining": true,
                })
            )
        );
        let (status, text) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"));
        assert_eq!(worker_seen.hits(), 1);
    }

    #[tokio::test]
    async fn probe_listener_reports_draining_through_the_shutdown_delay() {
        let (worker, _worker_seen, _worker) = named_mock("worker").await;
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        let shutdown_delay = Duration::from_secs(1);
        let shutdown = Notifier::new();
        let probes = spawn_probes_until(state, shutdown_delay, shutdown.subscribe()).await;
        assert_eq!(get_probe(probes, READY_PATH).await.0, StatusCode::OK);

        let notice = Instant::now();
        shutdown.notify();

        // the probe listener keeps answering through the delay, and both
        // readiness probes report the drain on it
        let draining = |status: StatusCode, body: serde_json::Value| {
            status == StatusCode::SERVICE_UNAVAILABLE && body["draining"] == true
        };
        let (status, body) = get_probe(probes, READY_PATH).await;
        assert!(draining(status, body.clone()), "{status} {body}");
        let (status, body) = get_probe(probes, READY_ANY_PATH).await;
        assert!(draining(status, body.clone()), "{status} {body}");
        let health =
            Client::new().get(format!("http://{probes}{HEALTH_PATH}")).send().await.expect("send");
        assert_eq!(health.status(), StatusCode::OK);
        assert!(notice.elapsed() < shutdown_delay, "the probes above must run inside the delay");

        // then it closes, not before the delay ran out (timers never fire
        // early)
        let closed_after = first_refusal(probes, notice).await;
        assert!(
            closed_after >= shutdown_delay,
            "the probe listener closed {closed_after:?} after the notice, inside the \
             {shutdown_delay:?} delay"
        );
    }

    /// A gateway with one ready worker (`named_mock("worker")`) and the given
    /// `--cors-allowed-origins`. The `Notifier`s keep both servers alive.
    async fn cors_gateway(origins: Option<CorsOrigins>) -> (SocketAddr, Seen, [Notifier; 2]) {
        let (worker, seen, worker_shutdown) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        let app = router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES),
            RouterLayers { cors: origins, ..RouterLayers::default() },
            Arc::default(),
        );
        let (gateway, shutdown) = spawn(app).await;
        (gateway, seen, [worker_shutdown, shutdown])
    }

    /// `--cors-allowed-origins` listing exactly `origins`.
    fn cors_list(origins: &[&'static str]) -> Option<CorsOrigins> {
        Some(CorsOrigins::List(origins.iter().copied().map(HeaderValue::from_static).collect()))
    }

    /// The browser's preflight for a JSON-RPC `POST` from `origin`.
    async fn preflight(gateway: SocketAddr, origin: &str) -> reqwest::Response {
        Client::new()
            .request(Method::OPTIONS, format!("http://{gateway}/"))
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
            .send()
            .await
            .expect("send")
    }

    /// A JSON-RPC `POST` from `origin`, as a browser sends it after the
    /// preflight.
    async fn post_from(gateway: SocketAddr, origin: &str) -> reqwest::Response {
        Client::new()
            .post(format!("http://{gateway}/"))
            .header(header::ORIGIN, origin)
            .header(header::CONTENT_TYPE, "application/json")
            .body(call("eth_chainId", 1))
            .send()
            .await
            .expect("send")
    }

    /// The names of the `Access-Control-*` headers on `response`.
    fn cors_headers(response: &reqwest::Response) -> Vec<String> {
        response
            .headers()
            .keys()
            .map(|name| name.as_str().to_string())
            .filter(|name| name.starts_with("access-control-"))
            .collect()
    }

    /// The value of `name` on `response`, or `""` when absent.
    fn header_value(response: &reqwest::Response, name: header::HeaderName) -> &str {
        response.headers().get(name).and_then(|value| value.to_str().ok()).unwrap_or("")
    }

    /// The flag's core promise: the gateway answers a preflight itself, with
    /// the allow headers a browser needs, and never forwards it. The mock
    /// counts every method, so a forwarded `OPTIONS` would show up as a hit.
    #[tokio::test]
    async fn preflight_is_answered_locally_with_the_flag() {
        let origin = "https://app.example";
        let (gateway, worker_seen, _guards) = cors_gateway(cors_list(&[origin])).await;

        let response = preflight(gateway, origin).await;
        assert!(response.status().is_success(), "preflight answered {}", response.status());
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_METHODS), "POST");
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_HEADERS), "content-type");
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_MAX_AGE), "3600");
        assert_eq!(worker_seen.hits(), 0, "a preflight must never reach the worker");
    }

    #[tokio::test]
    async fn allowed_origin_gets_access_control_allow_origin() {
        let (gateway, worker_seen, _guards) =
            cors_gateway(cors_list(&["https://one.example", "http://localhost:3000"])).await;

        for origin in ["https://one.example", "http://localhost:3000"] {
            let response = post_from(gateway, origin).await;
            assert_eq!(response.status(), StatusCode::OK, "{origin}");
            // the allowed origin is echoed, and caches are told the answer
            // depends on it
            assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);
            assert!(header_value(&response, header::VARY).contains("origin"), "{origin}");
            assert_eq!(response.text().await.expect("text"), "worker", "{origin}");
        }
        assert_eq!(worker_seen.hits(), 2);
    }

    /// A disallowed origin is still served, as a non-browser client would be,
    /// but neither its `POST` nor its preflight gets
    /// `Access-Control-Allow-Origin`, so a browser withholds the response from
    /// the page. The `POST` carries only the `Access-Control-Expose-Headers`
    /// the layer adds to every answer but a preflight, which grants nothing
    /// without the origin. Matching is exact: scheme, host and port must all
    /// agree.
    #[tokio::test]
    async fn disallowed_origin_gets_no_cors_headers() {
        let (gateway, worker_seen, _guards) =
            cors_gateway(cors_list(&["https://app.example"])).await;

        let disallowed = [
            "https://evil.example",
            "http://app.example",
            "https://app.example:8443",
            "https://app.example.evil",
            "https://sub.app.example",
        ];
        for origin in disallowed {
            let response = post_from(gateway, origin).await;
            assert_eq!(response.status(), StatusCode::OK, "{origin}");
            assert_eq!(cors_headers(&response), ["access-control-expose-headers"], "{origin}");
            assert_eq!(response.text().await.expect("text"), "worker", "{origin}");

            // the preflight is still answered locally, without allowing the
            // origin: only `Access-Control-Allow-Origin` is withheld
            let response = preflight(gateway, origin).await;
            let mut names = cors_headers(&response);
            names.sort();
            assert_eq!(
                names,
                [
                    "access-control-allow-headers",
                    "access-control-allow-methods",
                    "access-control-max-age",
                ],
                "{origin}"
            );
        }
        assert_eq!(worker_seen.hits(), disallowed.len(), "only the posts are forwarded");
    }

    /// An allowed page can read `Retry-After`: the browser hides a response
    /// header from the page unless the CORS answer exposes it.
    #[tokio::test]
    async fn cors_response_exposes_retry_after() {
        let origin = "https://app.example";
        let (gateway, _worker_seen, _guards) = cors_gateway(cors_list(&[origin])).await;

        let response = post_from(gateway, origin).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);
        let exposed = header_value(&response, header::ACCESS_CONTROL_EXPOSE_HEADERS);
        assert!(
            exposed.split(',').any(|name| name.trim().eq_ignore_ascii_case("retry-after")),
            "exposed: {exposed:?}"
        );
    }

    #[tokio::test]
    async fn wildcard_allows_any_origin() {
        let (gateway, worker_seen, _guards) = cors_gateway(Some(CorsOrigins::Any)).await;

        for origin in ["https://anything.example", "http://127.0.0.1:5173"] {
            let response = preflight(gateway, origin).await;
            assert!(response.status().is_success(), "{origin}");
            assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), "*");

            let response = post_from(gateway, origin).await;
            assert_eq!(response.status(), StatusCode::OK, "{origin}");
            assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), "*");
            assert_eq!(response.text().await.expect("text"), "worker", "{origin}");
        }
        assert_eq!(worker_seen.hits(), 2, "only the posts are forwarded");
    }

    /// Without the flag the gateway's responses are unchanged: no CORS header,
    /// whatever the `Origin`. (How a bare `OPTIONS` is routed without the flag
    /// is outside this test.)
    #[tokio::test]
    async fn without_the_flag_no_cors_headers_are_added() {
        let (gateway, worker_seen, _guards) = cors_gateway(None).await;

        let response = post_from(gateway, "https://app.example").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(cors_headers(&response), Vec::<String>::new());
        assert!(response.headers().get(header::VARY).is_none());
        assert_eq!(response.text().await.expect("text"), "worker");
        assert_eq!(worker_seen.hits(), 1);
    }

    /// The CORS layer sits outside the rate limiter: a preflight spends no
    /// token, and a `429` still carries the CORS header a browser needs to let
    /// the page read it.
    #[tokio::test]
    async fn cors_wraps_the_rate_limiter() {
        let origin = "https://app.example";
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let state = redirect_state(worker, None);
        state.readiness.set_ready(0, true);
        // one request, no refill within the test
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let app = router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES),
            RouterLayers {
                rate_limiters: Some(limiters),
                cors: cors_list(&[origin]),
                ..RouterLayers::default()
            },
            Arc::default(),
        );
        let (gateway, _shutdown) = spawn(app).await;

        for _ in 0..3 {
            let response = preflight(gateway, origin).await;
            assert!(response.status().is_success(), "preflight answered {}", response.status());
        }
        let first = post_from(gateway, origin).await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(header_value(&first, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);

        let second = post_from(gateway, origin).await;
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(header_value(&second, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);
        assert_eq!(worker_seen.hits(), 1);
    }

    /// Without `--cors-allowed-origins` an `OPTIONS`, a browser's preflight
    /// included, is a non-`POST` request like any other: the proxy's `405`
    /// envelope with `Allow: POST`, no CORS header, and nothing forwarded.
    #[tokio::test]
    async fn options_without_cors_gets_the_405_envelope() {
        let (gateway, worker_seen, _guards) = cors_gateway(None).await;

        let response = preflight(gateway, "https://app.example").await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(header_value(&response, header::ALLOW), "POST");
        assert_eq!(cors_headers(&response), Vec::<String>::new());
        let body: serde_json::Value = response.json().await.expect("json error body");
        assert_eq!(body["error"]["code"], -32600);
        assert_eq!(body["error"]["message"], "only POST is accepted");
        assert_eq!(body["id"], serde_json::Value::Null);
        assert_eq!(worker_seen.hits(), 0, "an OPTIONS must never reach the worker");
    }

    /// With `--cors-allowed-origins` the CORS layer answers every `OPTIONS`
    /// before the proxy's `POST`-only rule sees it, so a preflight gets its
    /// `2xx` rather than the `405`, on the proxied paths and on `/health` and
    /// `/ready`, which sit inside the layer (axum adds their `Allow: GET,HEAD`
    /// to that answer). Every other method but `POST` is
    /// still refused with the `405` envelope, which now carries the CORS
    /// header for an allowed origin. `/ready/any` is added outside the layer:
    /// it never carries a CORS header and refuses an `OPTIONS` like any method
    /// but `GET` and `HEAD`.
    #[tokio::test]
    async fn options_with_cors_is_answered_before_the_post_only_rule() {
        let origin = "https://app.example";
        let (gateway, worker_seen, _guards) = cors_gateway(cors_list(&[origin])).await;
        let client = Client::new();

        // the proxy's refusal would say `Allow: POST`; on the probe paths axum
        // adds the routes' own `Allow: GET,HEAD` to the preflight answer
        for (path, allow) in
            [("/", ""), ("/x", ""), (HEALTH_PATH, "GET,HEAD"), (READY_PATH, "GET,HEAD")]
        {
            // a bare `OPTIONS` too: the layer takes every `OPTIONS` as a
            // preflight
            for request_method in [Some("POST"), None] {
                let request = client
                    .request(Method::OPTIONS, format!("http://{gateway}{path}"))
                    .header(header::ORIGIN, origin);
                let request = match request_method {
                    Some(method) => request.header(header::ACCESS_CONTROL_REQUEST_METHOD, method),
                    None => request,
                };
                let response = request.send().await.expect("send");
                assert!(response.status().is_success(), "{path}: {}", response.status());
                assert_eq!(
                    header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN),
                    origin,
                    "{path}"
                );
                assert_eq!(
                    header_value(&response, header::ACCESS_CONTROL_ALLOW_METHODS),
                    "POST",
                    "{path}"
                );
                assert_eq!(header_value(&response, header::ALLOW), allow, "{path}");
            }
        }

        let response = client
            .put(format!("http://{gateway}/"))
            .header(header::ORIGIN, origin)
            .body(call("eth_chainId", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(header_value(&response, header::ALLOW), "POST");
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);
        let body: serde_json::Value = response.json().await.expect("json error body");
        assert_eq!(body["error"]["code"], -32600);
        assert_eq!(worker_seen.hits(), 0, "no OPTIONS or PUT reaches the worker");

        let response = client
            .get(format!("http://{gateway}{HEALTH_PATH}"))
            .header(header::ORIGIN, origin)
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(header_value(&response, header::ACCESS_CONTROL_ALLOW_ORIGIN), origin);

        let response = client
            .get(format!("http://{gateway}{READY_ANY_PATH}"))
            .header(header::ORIGIN, origin)
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(cors_headers(&response), Vec::<String>::new());
        let response = client
            .request(Method::OPTIONS, format!("http://{gateway}{READY_ANY_PATH}"))
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(header_value(&response, header::ALLOW), "GET,HEAD");
        assert_eq!(cors_headers(&response), Vec::<String>::new());
    }

    /// The fields every access-log line carries, besides its message.
    const ACCESS_FIELDS: [&str; 6] =
        ["client", "class", "route", "status", "latency_ms", "bytes_in"];

    /// One captured `gateway::access` line: each field by name, the message
    /// included, with its value as the log renders it.
    type AccessLine = BTreeMap<String, String>;

    /// A tracing layer that keeps every `gateway::access` event.
    #[derive(Clone, Debug, Default)]
    struct AccessLines(Arc<Mutex<Vec<AccessLine>>>);

    impl AccessLines {
        /// Capture this thread's `gateway::access` lines until the guard
        /// drops. `#[tokio::test]` runs every task on the test's thread, the
        /// gateway's connections included, so their lines land here.
        fn capture() -> (Self, DefaultGuard) {
            let lines = Self::default();
            let subscriber = tracing_subscriber::registry().with(lines.clone());
            (lines, tracing::subscriber::set_default(subscriber))
        }

        /// The lines captured so far, oldest first.
        fn lines(&self) -> Vec<AccessLine> {
            self.0.lock().expect("capture lock").clone()
        }
    }

    impl<S: Subscriber> Layer<S> for AccessLines {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            if event.metadata().target() == "gateway::access" {
                let mut line = AccessLine::new();
                event.record(&mut LineFields(&mut line));
                self.0.lock().expect("capture lock").push(line);
            }
        }
    }

    /// Collects an event's fields into an [`AccessLine`].
    struct LineFields<'a>(&'a mut AccessLine);

    impl Visit for LineFields<'_> {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    /// The gateway router with the access log at `rate` (and, when given,
    /// rate limiters), keying clients by the default prefixes and trusting no
    /// proxy.
    fn access_router(state: AppState, rate: f64, limiters: Option<Arc<RateLimiters>>) -> Router {
        access_router_with(
            state,
            rate,
            RouterLayers { rate_limiters: limiters, ..RouterLayers::default() },
            Arc::default(),
        )
    }

    /// [`access_router`] with other optional `layers` and `trusted` proxies;
    /// the access log in `layers` is replaced by one at `rate`.
    fn access_router_with(
        state: AppState,
        rate: f64,
        layers: RouterLayers,
        trusted: Arc<TrustedProxies>,
    ) -> Router {
        let access_log = AccessLog::new(rate, None, PrefixPolicy::default());
        router(
            state,
            Duration::from_secs(5),
            MAX_REQUEST_BYTES,
            NonZeroU32::new(MAX_INFLIGHT_REQUEST_BYTES),
            RouterLayers { access_log, ..layers },
            trusted,
        )
    }

    /// A gateway state whose one worker is the ready mock at `worker`.
    fn ready_state(worker: SocketAddr) -> AppState {
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        state
    }

    /// A mock upstream that counts each POST as it arrives and answers it only
    /// after `delay` ([`slow_mock`] with a hit count). The `Notifier` keeps it
    /// alive.
    async fn counted_slow_mock(delay: Duration) -> (SocketAddr, Seen, Notifier) {
        let seen = Seen::default();
        let counters = seen.clone();
        let mock = Router::new().route(
            "/",
            post(move || {
                let counters = counters.clone();
                async move {
                    counters.hits.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    "late"
                }
            }),
        );
        let (addr, shutdown) = spawn(mock).await;
        (addr, seen, shutdown)
    }

    #[tokio::test]
    async fn sample_rate_one_logs_every_request_including_429s() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, _seen, _worker) = named_mock("worker").await;
        // two requests of burst and a refill too slow to matter inside the
        // test, so the third request is shed with a `429`
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(2))),
            None,
            16,
            PrefixPolicy::default(),
        );
        let (gateway, _shutdown) = spawn(access_router(ready_state(worker), 1.0, limiters)).await;

        let body = call("eth_chainId", 1);
        let mut statuses = Vec::new();
        for _ in 0..3 {
            statuses.push(post_rpc(gateway, None, body.clone()).await.0);
        }
        assert_eq!(statuses, [StatusCode::OK, StatusCode::OK, StatusCode::TOO_MANY_REQUESTS]);

        let lines = lines.lines();
        assert_eq!(lines.len(), 3, "one line per request: {lines:?}");
        for line in &lines {
            let mut fields: Vec<&str> = line.keys().map(String::as_str).collect();
            fields.retain(|field| *field != "message");
            let mut expected = ACCESS_FIELDS.to_vec();
            expected.sort_unstable();
            assert_eq!(fields, expected, "{line:?}");
            assert_eq!(line["client"], "127.0.0.1");
            line["latency_ms"].parse::<u64>().expect("latency_ms is whole milliseconds");
        }
        let seen: Vec<[&str; 4]> = lines
            .iter()
            .map(|line| {
                [&line["status"], &line["class"], &line["route"], &line["bytes_in"]]
                    .map(String::as_str)
            })
            .collect();
        // the shed request never reached the proxy: no class, no route, and
        // its size is the declared `Content-Length`
        let size = body.len().to_string();
        assert_eq!(
            seen,
            [
                ["200", "query", "worker", size.as_str()],
                ["200", "query", "worker", size.as_str()],
                ["429", "unknown", "rejected", size.as_str()],
            ]
        );
    }

    /// The line ceiling caps the lines the sampler admits within one second
    /// and counts the rest; the next second opens a fresh window.
    #[test]
    fn access_log_ceiling_suppresses_and_counts_lines_over_the_limit() {
        let mut metrics = CapturedMetrics::install();
        let log = AccessLog::new(1.0, NonZeroU32::new(2), PrefixPolicy::default())
            .expect("a non-zero rate installs the log");
        let second_zero: Vec<bool> = (0..3).map(|_| log.sample_at(0)).collect();
        assert_eq!(second_zero, [true, true, false]);
        assert!(log.sample_at(1), "a new second opens a new window");
        assert_eq!(metrics.value("tn_worker_gateway_access_log_suppressed_total", &[]), 1.0);
    }

    /// A `0` ceiling (`None` once resolved) never suppresses a sampled line.
    #[test]
    fn access_log_ceiling_zero_is_unlimited() {
        let mut metrics = CapturedMetrics::install();
        let log = AccessLog::new(1.0, NonZeroU32::new(0), PrefixPolicy::default())
            .expect("a non-zero rate installs the log");
        assert!((0..10_000).all(|_| log.sample_at(0)), "every sampled line is written");
        assert_eq!(metrics.value("tn_worker_gateway_access_log_suppressed_total", &[]), 0.0);
    }

    #[tokio::test]
    async fn sample_rate_zero_logs_nothing() {
        let (lines, _capture) = AccessLines::capture();
        assert!(
            AccessLog::new(0.0, None, PrefixPolicy::default()).is_none(),
            "a zero rate installs no access log"
        );
        let (worker, _seen, _worker) = named_mock("worker").await;
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            None,
            16,
            PrefixPolicy::default(),
        );
        let (gateway, _shutdown) = spawn(access_router(ready_state(worker), 0.0, limiters)).await;

        let mut statuses = Vec::new();
        for id in 1..=3 {
            statuses.push(post_rpc(gateway, None, call("eth_chainId", id)).await.0);
        }
        assert_eq!(
            statuses,
            [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS, StatusCode::TOO_MANY_REQUESTS]
        );
        assert_eq!(lines.lines(), Vec::<AccessLine>::new());
    }

    #[tokio::test]
    async fn sample_rate_half_logs_every_other_request() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, _seen, _worker) = named_mock("worker").await;
        let (gateway, _shutdown) = spawn(access_router(ready_state(worker), 0.5, None)).await;

        // ids of different widths give every request its own body length, so
        // a line's `bytes_in` names the request it logged
        let bodies = [1, 22, 333, 4_444, 55_555, 666_666].map(|id| call("eth_chainId", id));
        for body in &bodies {
            assert_eq!(post_rpc(gateway, None, body.clone()).await.0, StatusCode::OK);
        }

        let logged: Vec<String> =
            lines.lines().iter().map(|line| line["bytes_in"].clone()).collect();
        let every_other: Vec<String> =
            bodies.iter().skip(1).step_by(2).map(|body| body.len().to_string()).collect();
        assert_eq!(logged, every_other, "the 2nd, 4th and 6th requests are logged");
    }

    #[tokio::test]
    async fn access_log_carries_no_url_body_or_params() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (query, query_seen, _query) = named_mock("query").await;
        let state = redirect_state(worker, Some(query));
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(access_router(state, 1.0, None)).await;

        // a read for the query upstream and a submission for the worker, each
        // with a path, a query string, a header value and params that must
        // stay out of the log
        let read = r#"{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x5ec2e75ec2e75ec2e75ec2e75ec2e75ec2e75ec2","latest"],"id":"read-id"}"#;
        // a transaction the screen passes: it decodes both submission methods
        let submission = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransactionSync","params":["{SIGNED_TX}"],"id":"submission-id"}}"#
        );
        let client = Client::new();
        for body in [read.to_string(), submission.clone()] {
            let response = client
                .post(format!("http://{gateway}/private/path?api_key=SECRET"))
                .header("authorization", "Bearer SECRET-TOKEN")
                .body(body)
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!((query_seen.hits(), worker_seen.hits()), (1, 1));

        let lines = lines.lines();
        let routes: Vec<(&str, &str)> =
            lines.iter().map(|line| (line["class"].as_str(), line["route"].as_str())).collect();
        assert_eq!(routes, [("query", "query"), ("submission", "worker")]);
        let rendered = format!("{lines:?}");
        let secrets = [
            query.to_string(),
            worker.to_string(),
            gateway.to_string(),
            read.to_string(),
            submission.clone(),
            SIGNED_TX[..18].to_string(),
            "eth_getBalance".to_string(),
            "eth_sendRawTransactionSync".to_string(),
            "0x5ec2e7".to_string(),
            "latest".to_string(),
            "read-id".to_string(),
            "submission-id".to_string(),
            "private".to_string(),
            "api_key".to_string(),
            "SECRET".to_string(),
        ];
        for secret in secrets {
            assert!(!rendered.contains(&secret), "the access log leaks {secret:?}: {rendered}");
        }
    }

    #[tokio::test]
    async fn probes_are_not_logged() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, _seen, _worker) = named_mock("worker").await;
        // at 0.5 a probe that took a sample position would shift which
        // proxied request is logged
        let (gateway, _shutdown) = spawn(access_router(ready_state(worker), 0.5, None)).await;

        let client = Client::new();
        for path in [HEALTH_PATH, READY_PATH, READY_ANY_PATH, HEALTH_PATH] {
            let response =
                client.get(format!("http://{gateway}{path}")).send().await.expect("send");
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert_eq!(lines.lines(), Vec::<AccessLine>::new(), "a probe was logged");

        // bodies of different lengths name the request each line logged
        let bodies = [call("eth_chainId", 1), call("eth_chainId", 22)];
        for body in &bodies {
            assert_eq!(post_rpc(gateway, None, body.clone()).await.0, StatusCode::OK);
        }
        let logged: Vec<String> =
            lines.lines().iter().map(|line| line["bytes_in"].clone()).collect();
        assert_eq!(logged, [bodies[1].len().to_string()], "probes took sample positions");
    }

    #[tokio::test]
    async fn oversized_body_is_logged_unknown_rejected() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, _seen, _worker) = named_mock("worker").await;
        let access_log = AccessLog::new(1.0, None, PrefixPolicy::default());
        let layers = RouterLayers { access_log, ..RouterLayers::default() };
        let app =
            router(ready_state(worker), Duration::from_secs(5), 64, None, layers, Arc::default());
        let (gateway, _shutdown) = spawn(app).await;

        let body = call("eth_chainId", 1).repeat(4);
        assert_eq!(post_rpc(gateway, None, body.clone()).await.0, StatusCode::PAYLOAD_TOO_LARGE);
        let lines = lines.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let size = body.len().to_string();
        assert_eq!(
            [&lines[0]["class"], &lines[0]["route"], &lines[0]["status"], &lines[0]["bytes_in"]]
                .map(String::as_str),
            ["unknown", "rejected", "413", size.as_str()]
        );
    }

    #[tokio::test]
    async fn cancelled_request_is_logged_with_status_499() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, worker_seen, _worker) = counted_slow_mock(Duration::from_secs(2)).await;
        let (gateway, _shutdown) = spawn(access_router(ready_state(worker), 1.0, None)).await;

        // a complete request, then a close while the gateway awaits the worker
        let body = call("eth_chainId", 1);
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        let request = format!(
            "POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.expect("write");
        for _ in 0..60 {
            if worker_seen.hits() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(worker_seen.hits(), 1, "the request never reached the worker");
        drop(stream);

        for _ in 0..60 {
            if !lines.lines().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let lines = lines.lines();
        assert_eq!(lines.len(), 1, "one line for the cancelled request: {lines:?}");
        let mut fields: Vec<&str> = lines[0].keys().map(String::as_str).collect();
        fields.retain(|field| *field != "message");
        let mut expected = ACCESS_FIELDS.to_vec();
        expected.sort_unstable();
        assert_eq!(fields, expected, "{lines:?}");
        let size = body.len().to_string();
        assert_eq!(
            [&lines[0]["class"], &lines[0]["route"], &lines[0]["status"], &lines[0]["bytes_in"]]
                .map(String::as_str),
            ["query", "worker", "499", size.as_str()]
        );
    }

    /// A request whose client closes the connection while the gateway awaits
    /// the worker is dropped without an answer; it counts as
    /// `outcome="cancelled"`, so the outcomes still add up to every request.
    #[tokio::test]
    async fn cancelled_request_counts_outcome_cancelled() {
        const REQUESTS: &str = "tn_worker_gateway_requests_total";
        let mut metrics = CapturedMetrics::install();
        let (worker, worker_seen, _worker) = counted_slow_mock(Duration::from_secs(10)).await;
        let (gateway, _shutdown) = spawn(router(
            ready_state(worker),
            Duration::from_secs(30),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        // a complete request, then a close while the gateway awaits the worker
        let body = call("eth_chainId", 1);
        let mut stream = TcpStream::connect(gateway).await.expect("connect");
        let request = format!(
            "POST / HTTP/1.1\r\nHost: gateway\r\nContent-Type: application/json\r\n\
             Content-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.expect("write");
        tokio::time::timeout(Duration::from_secs(5), async {
            while worker_seen.hits() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the request never reached the worker");
        drop(stream);

        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.value(REQUESTS, &[("outcome", "cancelled")]) < 1.0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the dropped request should count as cancelled");
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "cancelled")]), 1.0);
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "forwarded")]), 0.0);
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "rejected")]), 0.0);
    }

    /// The deadline's answer records `rejected` where it is built; the
    /// cancellation counter sits outside the deadline layer and sees that
    /// answer, so the request is not counted again as cancelled.
    #[tokio::test]
    async fn deadline_is_not_counted_as_cancelled() {
        const REQUESTS: &str = "tn_worker_gateway_requests_total";
        let mut metrics = CapturedMetrics::install();
        let (worker, worker_seen, _worker) = counted_slow_mock(Duration::from_secs(10)).await;
        let (gateway, _shutdown) = spawn(router(
            ready_state(worker),
            Duration::from_millis(300),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        ))
        .await;

        let (status, _) = post_rpc(gateway, None, call("eth_chainId", 1)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(worker_seen.hits(), 1, "the request was forwarded");
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "rejected")]), 1.0);
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "cancelled")]), 0.0);
        assert_eq!(metrics.value(REQUESTS, &[("outcome", "forwarded")]), 0.0);
    }

    #[tokio::test]
    async fn request_timeout_after_forward_keeps_class_and_route() {
        let (lines, _capture) = AccessLines::capture();
        // the worker answers after the whole-request deadline but well inside
        // the proxy client's own timeout, so the deadline fires mid-forward
        let (worker, worker_seen, _worker) = counted_slow_mock(Duration::from_secs(1)).await;
        let proxy_client =
            Client::builder().timeout(Duration::from_secs(5)).build().expect("build client");
        let state = test_state_with_client(&[upstream(worker)], proxy_client);
        state.readiness.set_ready(0, true);
        let access_log = AccessLog::new(1.0, None, PrefixPolicy::default());
        let layers = RouterLayers { access_log, ..RouterLayers::default() };
        let app = router(
            state,
            Duration::from_millis(300),
            MAX_REQUEST_BYTES,
            None,
            layers,
            Arc::default(),
        );
        let (gateway, _shutdown) = spawn(app).await;

        let body = call("eth_chainId", 1);
        assert_eq!(post_rpc(gateway, None, body.clone()).await.0, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(worker_seen.hits(), 1, "the request was forwarded");
        let lines = lines.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let size = body.len().to_string();
        assert_eq!(
            [&lines[0]["class"], &lines[0]["route"], &lines[0]["status"], &lines[0]["bytes_in"]]
                .map(String::as_str),
            ["query", "worker", "504", size.as_str()]
        );
    }

    /// Behind a trusted front the line names the client the front forwarded
    /// for, masked to the rate limiter's prefix, as the limiter keys it, not
    /// the front's own address (which it names only when the front forwarded
    /// no usable client).
    #[tokio::test]
    async fn access_log_names_the_forwarded_client_behind_a_trusted_proxy() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, _seen, _worker) = named_mock("worker").await;
        let trusted = Arc::new("127.0.0.1/32".parse::<TrustedProxies>().expect("trusted proxies"));
        let layers =
            RouterLayers { rate_limiters: one_request_per_ip(), ..RouterLayers::default() };
        let (gateway, _shutdown) =
            spawn(access_router_with(ready_state(worker), 1.0, layers, trusted)).await;

        // the second request from the first client is shed by its own bucket,
        // and logged against it
        let mut statuses = Vec::new();
        for client in ["198.51.100.1", "198.51.100.1", "2001:db8::1:2:3:4"] {
            statuses.push(post_with(gateway, &[("x-forwarded-for", client)]).await.0);
        }
        assert_eq!(statuses, [StatusCode::OK, StatusCode::TOO_MANY_REQUESTS, StatusCode::OK]);
        let logged: Vec<[String; 2]> = lines
            .lines()
            .iter()
            .map(|line| [line["client"].clone(), line["status"].clone()])
            .collect();
        // an IPv6 client is masked to the default /64
        assert_eq!(
            logged,
            [["198.51.100.1", "200"], ["198.51.100.1", "429"], ["2001:db8::", "200"]]
                .map(|line| line.map(String::from))
        );
    }

    /// The layers that answer a request themselves sit inside the access log,
    /// so a CORS preflight and a non-`POST` request are logged (`unknown`,
    /// `rejected`, their own status) and take sample positions like any other.
    #[tokio::test]
    async fn edge_answers_are_logged_unknown_rejected() {
        let (lines, _capture) = AccessLines::capture();
        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let origin = "https://app.example";
        let layers = RouterLayers { cors: cors_list(&[origin]), ..RouterLayers::default() };
        let (gateway, _shutdown) =
            spawn(access_router_with(ready_state(worker), 1.0, layers, Arc::default())).await;

        let preflight_status = preflight(gateway, origin).await.status();
        assert!(preflight_status.is_success(), "{preflight_status}");
        let put = Client::new().put(format!("http://{gateway}/")).send().await.expect("send");
        assert_eq!(put.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(worker_seen.hits(), 0);

        let logged: Vec<[String; 3]> = lines
            .lines()
            .iter()
            .map(|line| [line["class"].clone(), line["route"].clone(), line["status"].clone()])
            .collect();
        assert_eq!(
            logged,
            [["unknown", "rejected", preflight_status.as_str()], ["unknown", "rejected", "405"],]
                .map(|line| line.map(String::from))
        );
    }

    /// The access line carries the same six fields under `--log-format json`
    /// as under `text`, and no others.
    #[test]
    fn access_line_has_the_same_fields_in_json_and_text() -> eyre::Result<()> {
        let line = |format| {
            let logs = CapturedLogs::default();
            let scoped = crate::subscriber("info", format, false, logs.clone());
            tracing::subscriber::with_default(scoped, || {
                drop(AccessEntry {
                    client: String::from("198.51.100.0"),
                    started: Instant::now(),
                    declared_bytes: 7,
                    record: Arc::default(),
                    status: Some(429),
                });
            });
            logs.contents()
        };
        let expected = [
            ("client", "198.51.100.0"),
            ("class", "unknown"),
            ("route", "rejected"),
            ("status", "429"),
            ("bytes_in", "7"),
        ];

        let json = line(LogFormat::Json);
        let json: serde_json::Value = serde_json::from_str(json.trim())?;
        assert_eq!(json["target"], "gateway::access");
        assert_eq!(json["level"], "INFO");
        let fields = json["fields"].as_object().expect("fields object");
        let mut names: Vec<&str> = fields.keys().map(String::as_str).collect();
        names.sort_unstable();
        let mut want = ACCESS_FIELDS.to_vec();
        want.push("message");
        want.sort_unstable();
        assert_eq!(names, want, "{json}");
        for (name, value) in expected {
            assert_eq!(fields[name].to_string().trim_matches('"'), value, "{name}");
        }
        assert!(fields["latency_ms"].is_u64(), "{json}");

        let text = line(LogFormat::Text);
        assert_eq!(text.lines().count(), 1, "{text:?}");
        assert!(text.contains("gateway::access"), "{text:?}");
        // the text format quotes a string field, as it renders every `&str`
        for (name, value) in expected {
            let plain = format!("{name}={value}");
            let quoted = format!("{name}=\"{value}\"");
            assert!(text.contains(&plain) || text.contains(&quoted), "{name}: {text:?}");
        }
        assert!(text.contains("latency_ms="), "{text:?}");
        Ok(())
    }

    /// Worker `worker_id` with its RPC and readiness endpoints on `addr`.
    fn upstream_at(worker_id: u16, addr: SocketAddr) -> UpstreamWorker {
        UpstreamWorker { worker_id, ..upstream(addr) }
    }

    /// A mock node whose `GET /health/workers` reports workers 0 and 1 both
    /// accepting transactions. Returns the number of polls it has answered;
    /// the `Notifier` keeps it alive.
    async fn counted_ready_node() -> (SocketAddr, Arc<AtomicUsize>, Notifier) {
        let polls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&polls);
        let node = Router::new().route(
            "/health/workers",
            get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async {
                    r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true},{"worker_id":1,"accepting_transactions":true}]}"#
                }
            }),
        );
        let (addr, shutdown) = spawn(node).await;
        (addr, polls, shutdown)
    }

    /// Run the readiness poller over `readiness` with `client` and no query
    /// probe, polling every 50 ms. The `Notifier` stops it.
    fn spawn_readiness_poller(readiness: Arc<GatewayReadiness>, client: Client) -> Notifier {
        let shutdown = Notifier::new();
        tokio::spawn(run_poller(
            readiness,
            client,
            None,
            Duration::from_millis(50),
            Duration::from_secs(2),
            shutdown.subscribe(),
        ));
        shutdown
    }

    /// Wait up to five seconds for `done` to hold, and report whether it did.
    async fn wait_until(done: impl Fn() -> bool) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !done() {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        true
    }

    /// The readiness client follows no redirect. Worker 0's readiness endpoint
    /// answers `302` toward a node that reports it ready, with a body that
    /// reports it ready too; the poll must leave worker 0 not ready, and that
    /// node must never be polled. Worker 1 is polled directly at an identical
    /// node, so the client and the poller are shown to work in the same run.
    /// The client is the one `app::run` builds (see [`UpstreamClients`]).
    #[tokio::test]
    async fn readiness_client_does_not_follow_redirects() {
        let (target, target_polls, _target) = counted_ready_node().await;
        let (control, _control_polls, _control) = counted_ready_node().await;
        let location = format!("http://{target}/health/workers");
        let bounces = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&bounces);
        let bouncer = Router::new().route(
            "/health/workers",
            get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                let location = location.clone();
                async move {
                    let ready = r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#;
                    (StatusCode::FOUND, [(header::LOCATION, location)], ready)
                }
            }),
        );
        let (bouncer, _bouncer) = spawn(bouncer).await;

        let redirected = upstream_at(0, bouncer);
        let direct = upstream_at(1, control);
        // thresholds of one, so a single poll decides each worker
        let thresholds = ReadinessThresholds { failure: nz(1), success: nz(1), rpc_failure: None };
        let readiness =
            Arc::new(GatewayReadiness::new(&[redirected.clone(), direct.clone()], thresholds));
        let settings = worker_settings("http", bouncer, bouncer, &[]);
        let UpstreamClients { readiness: client, .. } =
            UpstreamClients::new(&settings, Arc::new(SystemLookup)).expect("clients");
        let _poller = spawn_readiness_poller(Arc::clone(&readiness), client);

        // poll cycles run one after another, so a second bounce means the
        // whole first cycle has been recorded
        assert!(
            wait_until(|| bounces.load(Ordering::SeqCst) >= 2).await,
            "the poller never reached the redirecting endpoint twice"
        );
        assert_eq!(
            readiness.first_ready_rpc_url(),
            Some(direct.rpc_url),
            "worker 1 must be ready and worker 0 not, at {}",
            redirected.rpc_url
        );
        assert_eq!(target_polls.load(Ordering::SeqCst), 0, "the redirect target was polled");
    }

    /// Both upstream clients ignore proxy variables. Every proxy variable
    /// points at a port nothing listens on, with no `NO_PROXY` exemption, yet
    /// a poll through the production readiness client still marks the worker
    /// ready and a call through the production proxy client still reaches it.
    /// Both clients are the ones `app::run` builds (see [`UpstreamClients`]).
    ///
    /// The variables set here are process-wide, so the body runs only under
    /// nextest's process-per-test model, where they reach no other test; any
    /// other runner skips it.
    #[tokio::test]
    async fn clients_ignore_proxy_environment() {
        if !process_per_test() {
            eprintln!(
                "skipped: clients_ignore_proxy_environment needs nextest's process-per-test model"
            );
            return;
        }
        for name in
            ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"]
        {
            std::env::set_var(name, "http://127.0.0.1:9");
        }
        // `REQUEST_METHOD` makes reqwest skip `HTTP_PROXY` (a CGI guard), so it
        // goes too, along with any loopback exemption the environment carries
        for name in ["NO_PROXY", "no_proxy", "REQUEST_METHOD"] {
            std::env::remove_var(name);
        }

        let (worker, worker_seen, _worker) = named_mock("worker").await;
        let (node, node_polls, _node) = counted_ready_node().await;
        let settings = worker_settings("http", worker, node, &[]);
        let UpstreamClients { proxy, readiness: readiness_client, .. } =
            UpstreamClients::new(&settings, Arc::new(SystemLookup)).expect("clients");
        let state = test_state_with_client(&settings.upstreams, proxy);
        let readiness = Arc::clone(&state.readiness);
        let _poller = spawn_readiness_poller(Arc::clone(&readiness), readiness_client);
        assert!(
            wait_until(|| readiness.any_ready()).await,
            "the readiness poll must reach the node directly"
        );
        assert!(node_polls.load(Ordering::SeqCst) >= 1);

        let (gateway, _shutdown) = spawn(test_router(state)).await;

        // the test's own client must not use the proxy either
        let client = Client::builder().no_proxy().build().expect("client");
        let response = client
            .post(format!("http://{gateway}/"))
            .body(call("eth_chainId", 1))
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.expect("text"), "worker");
        assert_eq!(worker_seen.hits(), 1);
    }
}
