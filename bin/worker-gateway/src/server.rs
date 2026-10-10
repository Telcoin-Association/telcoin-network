//! The gateway's HTTP surface: the JSON-RPC proxy plus liveness / readiness.
//!
//! A single server serves all three on one address: any request that is not
//! `GET /health` or `GET /ready` falls through to the proxy, so JSON-RPC
//! (`POST /`) is forwarded while orchestration probes hit the health routes.
//!
//! The accept loop is hand-rolled over hyper's HTTP/1 connection builder
//! rather than `axum::serve`: `axum::serve` never installs a hyper timer, so
//! hyper's header read timeout is silently disabled and a slow-loris client
//! could hold connections open forever. Here every connection gets a header
//! read deadline, a whole-request deadline, `TCP_NODELAY`, a global
//! concurrent-connection cap, and two write-path guards for the response-side
//! slow loris (a client that stops or trickles its reads while a response
//! body streams to it): a transport-stall deadline (`TCP_USER_TIMEOUT`) and a
//! hard cap on total connection lifetime.

use std::{
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, Request, State},
    http::{header::CONTENT_LENGTH, StatusCode},
    middleware::{from_fn_with_state, map_response, Next},
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
use tn_types::{Noticer, TaskError};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, info, warn};
use url::Url;

use crate::{
    error::{error_response, GatewayError},
    proxy::{proxy, AccessRecord, REJECTED_ROUTE},
    ratelimit::{rate_limit, PrefixPolicy, RateLimiters},
    readiness::GatewayReadiness,
};

/// Pause before re-polling `accept()` after it fails, so a persistent accept
/// error (e.g. fd exhaustion) cannot spin the loop hot.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Liveness probe path. Exempt from rate limiting (see [`crate::ratelimit`]).
pub(crate) const HEALTH_PATH: &str = "/health";

/// Readiness probe path. Exempt from rate limiting (see [`crate::ratelimit`]).
pub(crate) const READY_PATH: &str = "/ready";

/// The access log's `status` for a sampled request dropped before it had a
/// response (the client closed the connection, the connection lifetime cap,
/// shutdown): nginx's "client closed request".
const CLIENT_CLOSED_REQUEST: u16 = 499;

/// Shared state handed to every request handler.
#[derive(Clone, Debug)]
pub(crate) struct AppState {
    /// Live readiness view of the configured upstream workers.
    pub(crate) readiness: Arc<GatewayReadiness>,
    /// Client used to forward requests on both routes (built by
    /// [`crate::proxy::proxy_client`] outside tests).
    pub(crate) http: Client,
    /// Endpoint serving every non-submission call (`--redirect-queries`), or
    /// `None` when every call goes to the workers. It is not readiness-gated.
    pub(crate) query_upstream: Option<Url>,
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
    pub(crate) request_deadline: Duration,
    /// Maximum concurrently-open inbound connections; further connections wait
    /// in the OS accept backlog.
    pub(crate) max_connections: NonZeroUsize,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`) armed on every accepted
    /// connection, or `None` when disabled. Closes a connection whose peer
    /// leaves written data unacknowledged (or its receive window closed) this
    /// long; Linux-family kernels only, best-effort elsewhere.
    pub(crate) tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single connection's total lifetime (keep-alive sessions
    /// included), or `None` when uncapped. Enforced by the runtime independent
    /// of connection progress, so it fires even when hyper's write path is
    /// backpressured by a slow-reading client and no future the connection
    /// owns is being polled forward. The close is abrupt: an exchange still
    /// in flight when a keep-alive session hits the cap is cut off mid-stream.
    pub(crate) max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub(crate) max_request_bytes: usize,
}

/// The sampled access log (`--access-log-sample`): one `gateway::access` line
/// for each sampled proxied request (see [`log_access`]), written when the
/// response head is ready or, when the request is cancelled first, as it is
/// dropped.
///
/// Sampling is deterministic and needs no random source: the `n`th proxied
/// request, counting from one, is logged when `floor(n * rate)` exceeds
/// `floor((n - 1) * rate)`, so exactly `floor(n * rate)` of the first `n`
/// requests are logged, evenly spread. A rate of `1` logs every request. The
/// probes are not counted.
#[derive(Debug)]
pub(crate) struct AccessLog {
    /// Fraction of proxied requests logged, in `(0, 1]`.
    rate: f64,
    /// Proxied requests counted so far.
    requests: AtomicU64,
    /// Reduces a peer address to the rate limiter's key for it (its network
    /// prefix), so a line names the client the way the limiter meters it.
    prefix: PrefixPolicy,
}

impl AccessLog {
    /// Build the access log for a sample `rate` already validated to `0..=1`,
    /// or `None` at `0`, where the log is off and no layer is installed.
    pub(crate) fn new(rate: f64, prefix: PrefixPolicy) -> Option<Arc<Self>> {
        (rate > 0.0).then(|| Arc::new(Self { rate, requests: AtomicU64::new(0), prefix }))
    }

    /// Count one proxied request and report whether it is logged.
    fn sample(&self) -> bool {
        // the counter only hands each request a distinct position, which
        // `fetch_add` does under any ordering
        let before = self.requests.fetch_add(1, Ordering::Relaxed);
        // exact while the count stays below 2^53, which takes centuries at any
        // rate the gateway can serve
        (before.saturating_add(1) as f64 * self.rate).floor() > (before as f64 * self.rate).floor()
    }
}

/// Request extension marking a request the access log samples. The proxy
/// fills the slot with the request's [`AccessRecord`] before it answers or
/// forwards, so the line names the class and route even when the request's
/// future is dropped before a response exists; a request the log skips
/// carries no slot and is not classified for it.
#[derive(Clone, Debug)]
pub(crate) struct AccessSampled(pub(crate) Arc<OnceLock<AccessRecord>>);

/// One sampled request's access line, written exactly once as the guard
/// drops: after [`log_access`] has the response head, or with the request's
/// future when the request is cancelled first.
#[derive(Debug)]
struct AccessEntry {
    /// The peer's network prefix, as the rate limiter keys it.
    client: String,
    /// When the access log took the request.
    started: Instant,
    /// The declared `Content-Length` (`0` when there is none), logged as
    /// `bytes_in` when the proxy never read the body.
    declared_bytes: u64,
    /// The proxy's record for the request, empty until the proxy answers or
    /// forwards it.
    record: Arc<OnceLock<AccessRecord>>,
    /// The response status, or `None` while the request has no response.
    status: Option<u16>,
}

impl Drop for AccessEntry {
    fn drop(&mut self) {
        let (class, route, bytes_in) =
            self.record.get().map_or(("unknown", REJECTED_ROUTE, self.declared_bytes), |record| {
                (record.class, record.route, record.bytes_in)
            });
        info!(
            target: "gateway::access",
            client = %self.client,
            class,
            route,
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
    /// Whether at least one upstream worker is currently ready.
    ready: bool,
}

/// Build the gateway router: health/readiness routes plus the proxy fallback.
///
/// `request_deadline` bounds each whole request; the bare `408` the timeout
/// layer produces is rewritten into the gateway's JSON-RPC error envelope so
/// the "always a well-formed JSON-RPC error" contract holds. Streamed
/// *response* bodies are written after the handler returns, outside this
/// deadline; they are bounded by the accept loop's transport-stall deadline
/// and connection-lifetime cap (the upstream client's total timeout is only
/// checked when the body is polled, which a slow-reading client can prevent;
/// see [`accept_loop`]). `max_request_bytes` caps the buffered request body.
///
/// When `rate_limiters` is present it is installed outside every other layer
/// but the access log, so an over-limit request is shed with a JSON-RPC `429`
/// before its body is buffered or forwarded. When `access_log` is present it
/// is the outermost layer, so the requests the rate limiter sheds are logged
/// too.
pub(crate) fn router(
    state: AppState,
    request_deadline: Duration,
    max_request_bytes: usize,
    rate_limiters: Option<Arc<RateLimiters>>,
    access_log: Option<Arc<AccessLog>>,
) -> Router {
    let router = Router::new()
        .route(HEALTH_PATH, get(liveness))
        .route(READY_PATH, get(readiness))
        .fallback(proxy)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_deadline))
        .layer(map_response(envelope_request_timeout));
    // Add the rate-limit layer after the others so it runs ahead of the body
    // read.
    let router = match rate_limiters {
        Some(limiters) => router.layer(from_fn_with_state(limiters, rate_limit)),
        None => router,
    };
    // the access log goes outside the rate limiter so that a `429` is logged
    let router = match access_log {
        Some(log) => router.layer(from_fn_with_state(log, log_access)),
        None => router,
    };
    router.with_state(state)
}

/// Rewrite the timeout layer's bare `408` into the gateway's JSON-RPC error
/// envelope. The request `id` is unrecoverable here (the body never finished
/// arriving), so it echoes as `null`, per spec. Workers do not emit `408` for
/// JSON-RPC, so this cannot clobber a real upstream response in practice.
async fn envelope_request_timeout(response: Response) -> Response {
    if response.status() == StatusCode::REQUEST_TIMEOUT {
        return error_response(&GatewayError::RequestTimeout, b"");
    }
    response
}

/// Axum middleware: write one `gateway::access` line for each proxied request
/// the [`AccessLog`] samples. The probes are neither counted nor logged.
///
/// An [`AccessEntry`] guard writes the line exactly once: when the response
/// head is ready, or, when the request's future is dropped before it has a
/// response (the client closed the connection while the request was being
/// forwarded, the connection lifetime cap, shutdown), with status
/// [`CLIENT_CLOSED_REQUEST`].
///
/// The line's fields are `client` (the peer's network prefix, as the rate
/// limiter keys it), `class` and `route` (from the [`AccessRecord`] the proxy
/// puts in the request's [`AccessSampled`] slot before it answers or
/// forwards, or `unknown` and `rejected` for a request answered or dropped
/// before the proxy read its body: a rate-limit `429`, an oversized body, a
/// request timeout before the forward started), `status`, `latency_ms` (until
/// the response head is ready or the request is dropped; a streamed body is
/// still being written) and `bytes_in` (the buffered body's length, or the
/// declared `Content-Length` when the body was never read). It carries no URL,
/// header value, body, params or method name.
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
    let client = request.extensions().get::<ConnectInfo<SocketAddr>>().map_or_else(
        || String::from("unknown"),
        |ConnectInfo(peer)| log.prefix.key(peer.ip()).to_string(),
    );
    let declared_bytes = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let record = Arc::new(OnceLock::new());
    request.extensions_mut().insert(AccessSampled(Arc::clone(&record)));
    // the guard writes the line as it drops: on return, once the response head
    // is ready, or with this future if the request is cancelled first
    let mut entry = AccessEntry { client, started, declared_bytes, record, status: None };
    let response = next.run(request).await;
    entry.status = Some(response.status().as_u16());
    response
}

/// Liveness probe: always `200 OK` while the process is running.
async fn liveness() -> impl IntoResponse {
    StatusCode::OK
}

/// Readiness probe: `200` when at least one upstream worker is ready, else
/// `503`.
///
/// It means "this gateway can take submissions". With `--redirect-queries`
/// set, reads keep working while it reports `503`, and a failing query
/// upstream does not change it: that upstream is never probed.
async fn readiness(State(state): State<AppState>) -> impl IntoResponse {
    let ready = state.readiness.any_ready();
    let status = if ready { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(ReadyBody { ready }))
}

/// Bind `listen_addr` and serve until `shutdown` fires, then stop accepting and
/// drain in-flight requests until they finish or `graceful_timeout` elapses,
/// whichever comes first.
pub(crate) async fn serve(
    listen_addr: SocketAddr,
    state: AppState,
    limits: ServerLimits,
    rate_limiters: Option<Arc<RateLimiters>>,
    access_log: Option<Arc<AccessLog>>,
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "gateway::server", %local_addr, "worker gateway listening");

    let app =
        router(state, limits.request_deadline, limits.max_request_bytes, rate_limiters, access_log);
    accept_loop(listener, app, limits, graceful_timeout, shutdown).await
}

/// Accept connections until `shutdown` fires, serving each on its own task
/// with the configured header deadline, `TCP_NODELAY`, transport-stall
/// deadline, lifetime cap, and connection cap, then drain within
/// `graceful_timeout`.
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
async fn accept_loop(
    listener: TcpListener,
    app: Router,
    limits: ServerLimits,
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    // hyper's header read timeout only arms when a timer is installed; without
    // one the timeout is silently disabled (the exact `axum::serve` gap this
    // loop exists to close).
    let mut connection_builder = hyper::server::conn::http1::Builder::new();
    connection_builder.timer(TokioTimer::new()).header_read_timeout(limits.header_read_timeout);

    let graceful = GracefulShutdown::new();
    let limiter =
        Arc::new(Semaphore::new(limits.max_connections.get().min(Semaphore::MAX_PERMITS)));

    loop {
        // Backpressure: once `max_connections` are open, leave new connections
        // in the OS accept backlog instead of accepting without bound.
        let permit = tokio::select! {
            () = &shutdown => break,
            permit = Arc::clone(&limiter).acquire_owned() => permit,
        };
        // The semaphore is never closed, so acquisition cannot fail; bail out
        // defensively rather than panic if that invariant ever changes.
        let Ok(permit) = permit else { break };

        let accepted = tokio::select! {
            () = &shutdown => break,
            accepted = listener.accept() => accepted,
        };
        let Ok((stream, peer_addr)) = accepted.inspect_err(|err| {
            warn!(target: "gateway::server", %err, "failed to accept connection");
        }) else {
            tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            continue;
        };

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

        // Hand handlers the real client address (`ConnectInfo`, consumed by the
        // proxy's `X-Forwarded-For`).
        let service =
            TowerToHyperService::new(app.clone().layer(Extension(ConnectInfo(peer_addr))));
        let connection =
            graceful.watch(connection_builder.serve_connection(TokioIo::new(stream), service));
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
        tokio::spawn(async move {
            // `biased` so a connection that finishes in the same poll as the
            // cap expires is reported as what it was (completion or its real
            // error), never mislabeled as cap-killed.
            tokio::select! {
                biased;
                result = connection => {
                    if let Err(err) = result {
                        debug!(target: "gateway::server", %err, "connection error");
                    }
                }
                () = lifetime_cap => {
                    debug!(
                        target: "gateway::server",
                        %peer_addr,
                        "connection exceeded max lifetime; closing"
                    );
                }
            }
            drop(permit);
        });
    }

    // Stop accepting (drop the listener), then drain in-flight connections
    // until they finish or the graceful deadline elapses.
    drop(listener);
    info!(target: "gateway::server", "shutdown signal received; draining in-flight requests");
    tokio::select! {
        () = graceful.shutdown() => {
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
        config::UpstreamWorker,
        proxy::{proxy_client, MAX_REQUEST_BYTES},
        ratelimit::{PrefixPolicy, RateLimit},
    };
    use axum::{
        http::{header, HeaderMap},
        routing::post,
    };
    use reqwest::redirect::Policy;
    use std::{
        collections::BTreeMap,
        fmt,
        num::NonZeroU32,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Mutex,
        },
    };
    use tn_types::Notifier;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
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

    /// Generous limits so only the behavior under test can trip.
    fn test_limits() -> ServerLimits {
        ServerLimits {
            header_read_timeout: Duration::from_secs(5),
            request_deadline: Duration::from_secs(5),
            max_connections: NonZeroUsize::new(64).expect("nonzero"),
            tcp_user_timeout: None,
            max_connection_duration: None,
            max_request_bytes: MAX_REQUEST_BYTES,
        }
    }

    fn test_state(upstreams: &[UpstreamWorker]) -> AppState {
        test_state_with_client(upstreams, Client::builder().build().expect("build client"))
    }

    fn test_state_with_client(upstreams: &[UpstreamWorker], client: Client) -> AppState {
        AppState {
            readiness: Arc::new(GatewayReadiness::new(upstreams)),
            http: client,
            query_upstream: None,
        }
    }

    /// Serve `app` through the real accept loop (header timeout, nodelay,
    /// connection cap, `ConnectInfo` injection) on an ephemeral port. The
    /// returned `Notifier` keeps the server alive for the test's duration.
    async fn spawn_with_limits(app: Router, limits: ServerLimits) -> (SocketAddr, Notifier) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let shutdown = Notifier::new();
        let noticer = shutdown.subscribe();
        tokio::spawn(async move {
            let _ = accept_loop(listener, app, limits, Duration::from_secs(1), noticer).await;
        });
        (addr, shutdown)
    }

    async fn spawn(app: Router) -> (SocketAddr, Notifier) {
        spawn_with_limits(app, test_limits()).await
    }

    fn upstream(addr: SocketAddr) -> UpstreamWorker {
        UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse(&format!("http://{addr}/")).expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}/health/workers")).expect("ready url"),
        }
    }

    fn test_router(state: AppState) -> Router {
        router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, None, None)
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

        let response = Client::new()
            .post(format!("http://{gateway_addr}/"))
            .header("content-type", "application/json")
            .body(r#"{"jsonrpc":"2.0","method":"eth_chainId","id":1}"#)
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.text().await.expect("text"),
            r#"{"jsonrpc":"2.0","result":"0x1","id":1}"#
        );
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

        let proxy_client =
            Client::builder().timeout(Duration::from_millis(200)).build().expect("build client");
        let state = test_state_with_client(&[upstream(upstream_addr)], proxy_client);
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
        let (addr, _shutdown) = spawn(router(state, Duration::from_secs(5), 8, None, None)).await;

        let response = Client::new()
            .post(format!("http://{addr}/"))
            .body("this body is definitely longer than eight bytes")
            .send()
            .await
            .expect("send");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
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
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) =
            spawn(router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, Some(limiters), None))
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
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) =
            spawn(router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, Some(limiters), None))
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
    async fn slow_body_gets_enveloped_timeout() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        state.readiness.set_ready(0, true);
        // Short whole-request deadline; generous header timeout so only the
        // body trickle trips.
        let (addr, _shutdown) = spawn_with_limits(
            router(state, Duration::from_millis(300), MAX_REQUEST_BYTES, None, None),
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
        assert!(response.contains("-32005"), "expected enveloped timeout code, got: {response}");
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
        // trip shares one test runtime.
        let limits =
            ServerLimits { max_connection_duration: Some(Duration::from_secs(2)), ..test_limits() };
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
        // the lifetime cap without reading further.
        let mut first = [0_u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut first))
            .await
            .expect("response head should arrive well inside the lifetime cap")
            .expect("first read");
        assert!(read > 0, "expected the response head to arrive");
        tokio::time::sleep(Duration::from_millis(2_500)).await;

        // Drain what the socket still holds. The cap closed the connection
        // mid-body, so the drain must end (EOF or reset both count) well short
        // of the full body; pre-fix the stream resumes here and delivers all
        // of it.
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
        let mut buf = [0_u8; 16];
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("connection should be closed by the lifetime cap");
        assert_eq!(read.expect("read"), 0, "expected EOF from the server");

        // The capped connection's permit must be released: with a single slot,
        // this probe only gets accepted (and answered) if it was.
        let response = tokio::time::timeout(
            Duration::from_secs(5),
            Client::new().get(format!("http://{addr}/health")).send(),
        )
        .await
        .expect("permit should be released after the cap fires")
        .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
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
        /// Requests carrying `X-TN-Gateway`.
        hop_marker: Arc<AtomicUsize>,
        /// Requests carrying `X-TN-Gateway-Redirect`.
        redirect_marker: Arc<AtomicUsize>,
        /// Requests carrying the client's identity (`X-Forwarded-For`,
        /// `X-Forwarded-Proto`) and the gateway's user agent.
        identified: Arc<AtomicUsize>,
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
    }

    /// A mock upstream that answers every POST with `name` as the body and
    /// counts what it receives. The `Notifier` keeps it alive.
    async fn named_mock(name: &'static str) -> (SocketAddr, Seen, Notifier) {
        let seen = Seen::default();
        let counters = seen.clone();
        let mock = Router::new().route(
            "/",
            post(move |headers: HeaderMap| {
                let counters = counters.clone();
                async move {
                    let get = |name: &str| {
                        headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
                    };
                    counters.hits.fetch_add(1, Ordering::SeqCst);
                    if headers.contains_key("x-tn-gateway") {
                        counters.hop_marker.fetch_add(1, Ordering::SeqCst);
                    }
                    if headers.contains_key("x-tn-gateway-redirect") {
                        counters.redirect_marker.fetch_add(1, Ordering::SeqCst);
                    }
                    if get("x-forwarded-for") == "127.0.0.1"
                        && get("x-forwarded-proto") == "http"
                        && get("user-agent").starts_with("tn-worker-gateway/")
                    {
                        counters.identified.fetch_add(1, Ordering::SeqCst);
                    }
                    name
                }
            }),
        );
        let (addr, shutdown) = spawn(mock).await;
        (addr, seen, shutdown)
    }

    /// Gateway state with one worker at `worker` and, when given,
    /// `--redirect-queries` pointing at `query`, using the production proxy
    /// client. The worker starts not ready.
    fn redirect_state(worker: SocketAddr, query: Option<SocketAddr>) -> AppState {
        let client = proxy_client(Duration::from_secs(2), Duration::from_secs(5)).expect("client");
        redirect_state_with_client(worker, query, client)
    }

    fn redirect_state_with_client(
        worker: SocketAddr,
        query: Option<SocketAddr>,
        client: Client,
    ) -> AppState {
        AppState {
            readiness: Arc::new(GatewayReadiness::new(&[upstream(worker)])),
            http: client,
            query_upstream: query
                .map(|addr| Url::parse(&format!("http://{addr}/")).expect("query url")),
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
        let client =
            proxy_client(Duration::from_secs(2), Duration::from_millis(200)).expect("client");
        let state = redirect_state_with_client(worker, Some(query), client);
        state.readiness.set_ready(0, true);
        let (gateway, _shutdown) = spawn(test_router(state)).await;

        let (status, text) = post_rpc(gateway, None, call("eth_getLogs", 4)).await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error_code_and_id(&text), (-32002, serde_json::json!(4)));
        assert_eq!(worker_seen.hits(), 0);
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
            call("tn_info", 3),
            call("eth_sendRawTransaction", 4),
            call("eth_sendRawTransactionSync", 5),
            format!("[{},{}]", call("eth_sendRawTransaction", 6), call("eth_call", 7)),
        ];
        for body in &bodies {
            let (status, text) = post_rpc(gateway, None, body.clone()).await;
            assert_eq!((status, text.as_str()), (StatusCode::OK, "worker"), "{body}");
        }
        assert_eq!(worker_seen.hits(), bodies.len());
        assert_eq!(worker_seen.hop_marker(), bodies.len());
        assert_eq!(worker_seen.redirect_marker(), 0);
    }

    /// The proxy client follows no redirect. A query upstream answering `307`
    /// or `308` (which reqwest would otherwise follow, replaying the POST
    /// body) must not bounce a read onto the worker; the status passes through
    /// to the client like any other, without the `Location` header.
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
            assert_eq!(response.text().await.expect("text"), "moved");
        }
        assert_eq!(worker_seen.hits(), 0, "a redirect must never reach the worker");
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
    /// rate limiters), keying clients by the default prefixes.
    fn access_router(state: AppState, rate: f64, limiters: Option<Arc<RateLimiters>>) -> Router {
        let access_log = AccessLog::new(rate, PrefixPolicy::default());
        router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, limiters, access_log)
    }

    /// A gateway state whose one worker is the ready mock at `worker`.
    fn ready_state(worker: SocketAddr) -> AppState {
        let state = test_state(&[upstream(worker)]);
        state.readiness.set_ready(0, true);
        state
    }

    /// A mock upstream that counts each POST as it arrives and answers it only
    /// after `delay`. The `Notifier` keeps it alive.
    async fn slow_mock(delay: Duration) -> (SocketAddr, Seen, Notifier) {
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

    #[tokio::test]
    async fn sample_rate_zero_logs_nothing() {
        let (lines, _capture) = AccessLines::capture();
        assert!(
            AccessLog::new(0.0, PrefixPolicy::default()).is_none(),
            "a zero rate installs no access log"
        );
        let (worker, _seen, _worker) = named_mock("worker").await;
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
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
        let submission = r#"{"jsonrpc":"2.0","method":"eth_sendRawTransactionSync","params":["0x5ec2e7deadbeef"],"id":"submission-id"}"#;
        let client = Client::new();
        for body in [read, submission] {
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
            submission.to_string(),
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
        for path in [HEALTH_PATH, READY_PATH, HEALTH_PATH] {
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
        let access_log = AccessLog::new(1.0, PrefixPolicy::default());
        let app = router(ready_state(worker), Duration::from_secs(5), 64, None, access_log);
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
        let (worker, worker_seen, _worker) = slow_mock(Duration::from_secs(2)).await;
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

    #[tokio::test]
    async fn request_timeout_after_forward_keeps_class_and_route() {
        let (lines, _capture) = AccessLines::capture();
        // the worker answers after the whole-request deadline but well inside
        // the proxy client's own timeout, so the deadline fires mid-forward
        let (worker, worker_seen, _worker) = slow_mock(Duration::from_secs(1)).await;
        let proxy_client =
            Client::builder().timeout(Duration::from_secs(5)).build().expect("build client");
        let state = test_state_with_client(&[upstream(worker)], proxy_client);
        state.readiness.set_ready(0, true);
        let access_log = AccessLog::new(1.0, PrefixPolicy::default());
        let app = router(state, Duration::from_millis(300), MAX_REQUEST_BYTES, None, access_log);
        let (gateway, _shutdown) = spawn(app).await;

        let body = call("eth_chainId", 1);
        assert_eq!(post_rpc(gateway, None, body.clone()).await.0, StatusCode::REQUEST_TIMEOUT);
        assert_eq!(worker_seen.hits(), 1, "the request was forwarded");
        let lines = lines.lines();
        assert_eq!(lines.len(), 1, "{lines:?}");
        let size = body.len().to_string();
        assert_eq!(
            [&lines[0]["class"], &lines[0]["route"], &lines[0]["status"], &lines[0]["bytes_in"]]
                .map(String::as_str),
            ["query", "worker", "408", size.as_str()]
        );
    }
}
