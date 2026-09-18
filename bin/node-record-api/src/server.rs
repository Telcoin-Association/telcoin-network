//! The daemon's HTTP edge: the accept loop and the layers around the routes.
//!
//! Copied from `bin/worker-gateway/src/server.rs`; keep the two in sync until a shared
//! `crates/tn-http-edge` is extracted (follow-up). The JSON-RPC proxy specifics are dropped; a
//! CORS layer is added because this API backs a public website.
//!
//! The accept loop is hand-rolled over hyper's HTTP/1 connection builder rather than
//! `axum::serve`: `axum::serve` never installs a hyper timer, so hyper's header read timeout is
//! silently disabled and a slow-loris client could hold connections open forever. Here every
//! connection gets a header read deadline, a whole-request deadline, `TCP_NODELAY`, a global
//! concurrent-connection cap, and two write-path guards for the response-side slow loris (a
//! client that stops or trickles its reads while a response body streams to it): a
//! transport-stall deadline (`TCP_USER_TIMEOUT`) and a hard cap on total connection lifetime.
//!
//! Layer order on the router is load-bearing: `rate_limit → CorsLayer → TimeoutLayer →
//! DefaultBodyLimit → routes`. Rate limiting runs first so an over-limit request is shed before
//! anything else is spent on it; CORS sits outside the timeout so a `408` still carries the
//! browser's headers.

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit},
    http::{Method, StatusCode},
    middleware::{from_fn_with_state, map_response},
    response::Response,
    Extension, Router,
};
use futures::future::{self, Either};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::graceful::GracefulShutdown,
    service::TowerToHyperService,
};
use tn_types::{Noticer, TaskError};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tower_http::{
    cors::{Any, CorsLayer},
    timeout::TimeoutLayer,
};
use tracing::{debug, info, warn};

use crate::{
    error::{error_response, ApiError},
    ratelimit::{rate_limit, RateLimiters},
};

/// Pause before re-polling `accept()` after it fails, so a persistent accept
/// error (e.g. fd exhaustion) cannot spin the loop hot.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Inbound connection limits enforced by the accept loop and router (derived
/// from the CLI flags; see [`crate::cli::Cli`]).
#[derive(Clone, Debug)]
pub struct ServerLimits {
    /// How long a new connection may take to send the complete request headers
    /// before it is closed (slow-loris guard).
    pub header_read_timeout: Duration,
    /// Deadline for a whole request once its headers are in.
    pub request_deadline: Duration,
    /// Maximum concurrently-open inbound connections; further connections wait
    /// in the OS accept backlog.
    pub max_connections: NonZeroUsize,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`) armed on every accepted
    /// connection, or `None` when disabled. Closes a connection whose peer
    /// leaves written data unacknowledged (or its receive window closed) this
    /// long; Linux-family kernels only, best-effort elsewhere.
    pub tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single connection's total lifetime (keep-alive sessions
    /// included), or `None` when uncapped. Enforced by the runtime independent
    /// of connection progress, so it fires even when hyper's write path is
    /// backpressured by a slow-reading client and no future the connection
    /// owns is being polled forward. The close is abrupt: an exchange still
    /// in flight when a keep-alive session hits the cap is cut off mid-stream.
    pub max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub max_request_bytes: usize,
}

/// Wrap `routes` in the edge layers, in the order the module docs describe.
///
/// `request_deadline` bounds each whole request; the bare `408` the timeout layer produces is
/// rewritten into the daemon's JSON error so the "always JSON" contract holds.
/// `max_request_bytes` caps the buffered request body (the API is `GET`-only, so this is small).
/// When `rate_limiters` is present it is installed as the outermost layer.
pub fn router(
    routes: Router,
    request_deadline: Duration,
    max_request_bytes: usize,
    rate_limiters: Option<Arc<RateLimiters>>,
) -> Router {
    let router = routes
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_deadline))
        .layer(map_response(envelope_request_timeout))
        .layer(cors_layer());
    // Add the rate-limit layer last so it runs first.
    match rate_limiters {
        Some(limiters) => router.layer(from_fn_with_state(limiters, rate_limit)),
        None => router,
    }
}

/// CORS for a public, read-only API: any origin, the safe methods only, and never credentials
/// (the layer sends no `Access-Control-Allow-Credentials`, and `Any` origin is incompatible with
/// it by construction).
fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_headers(Any)
        .max_age(Duration::from_secs(3_600))
}

/// Rewrite the timeout layer's bare `408` into the daemon's JSON error.
async fn envelope_request_timeout(response: Response) -> Response {
    if response.status() == StatusCode::REQUEST_TIMEOUT {
        return error_response(ApiError::RequestTimeout);
    }
    response
}

/// Bind `listen_addr` and serve `app` until `shutdown` fires, then stop accepting and drain
/// in-flight requests until they finish or `graceful_timeout` elapses, whichever comes first.
pub async fn serve(
    listen_addr: SocketAddr,
    app: Router,
    limits: ServerLimits,
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "tn::node_record_api::server", %local_addr, "node record api listening");
    accept_loop(listener, app, limits, graceful_timeout, shutdown).await
}

/// Accept connections until `shutdown` fires, serving each on its own task
/// with the configured header deadline, `TCP_NODELAY`, transport-stall
/// deadline, lifetime cap, and connection cap, then drain within
/// `graceful_timeout`.
///
/// The two write-path guards close the response-side slow loris: the
/// whole-request deadline stops covering a response once its head is produced,
/// and under downstream backpressure hyper stops polling the body (its write
/// loop parks on a full write buffer). `TCP_USER_TIMEOUT` fires in the kernel
/// when the peer stops acknowledging written data outright, and the
/// connection-lifetime cap is a runtime timer polled independent of connection
/// progress, so it fires even against a client trickling one byte per interval
/// to keep the transport alive.
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
            warn!(target: "tn::node_record_api::server", %err, "failed to accept connection");
        }) else {
            tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            continue;
        };

        // Nagle + delayed-ACK can add ~40ms to small JSON responses.
        // Best-effort: a failure only costs latency.
        if let Err(err) = stream.set_nodelay(true) {
            debug!(target: "tn::node_record_api::server", %err, "failed to set TCP_NODELAY");
        }

        // Best-effort: on an unsupported platform (or a setsockopt failure)
        // the lifetime cap below still bounds a stalled reader.
        if let Some(Err(err)) =
            limits.tcp_user_timeout.map(|timeout| set_tcp_user_timeout(&stream, timeout))
        {
            debug!(target: "tn::node_record_api::server", %err, "failed to set TCP_USER_TIMEOUT");
        }

        // Hand handlers the real client address (`ConnectInfo`, consumed by the
        // per-IP rate limiter).
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
                        debug!(target: "tn::node_record_api::server", %err, "connection error");
                    }
                }
                () = lifetime_cap => {
                    debug!(
                        target: "tn::node_record_api::server",
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
    info!(
        target: "tn::node_record_api::server",
        "shutdown signal received; draining in-flight requests"
    );
    tokio::select! {
        () = graceful.shutdown() => {
            info!(target: "tn::node_record_api::server", "in-flight requests drained");
        }
        () = tokio::time::sleep(graceful_timeout) => {
            warn!(
                target: "tn::node_record_api::server",
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
/// the caller's debug log tells the truth. Production deployments run on Linux
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
        api::{routes, ApiState, HEALTH_PATH},
        cache::{test_support::config, RecordCache},
        ratelimit::{PrefixPolicy, RateLimit},
    };
    use axum::routing::get;
    use reqwest::Client;
    use std::{num::NonZeroU32, sync::RwLock};
    use tn_kad_client::NetworkType;
    use tn_types::Notifier;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
    };

    /// Generous limits so only the behavior under test can trip.
    fn test_limits() -> ServerLimits {
        ServerLimits {
            header_read_timeout: Duration::from_secs(5),
            request_deadline: Duration::from_secs(5),
            max_connections: NonZeroUsize::new(64).expect("nonzero"),
            tcp_user_timeout: None,
            max_connection_duration: None,
            max_request_bytes: 16 * 1024,
        }
    }

    fn test_routes() -> Router {
        let cache = Arc::new(RwLock::new(RecordCache::new(config())));
        routes(ApiState::new(cache, 2017, NetworkType::Worker(0)))
    }

    fn test_app(rate_limiters: Option<Arc<RateLimiters>>) -> Router {
        router(test_routes(), Duration::from_secs(5), 16 * 1024, rate_limiters)
    }

    /// Serve `app` through the real accept loop on an ephemeral port. The returned `Notifier`
    /// keeps the server alive for the test's duration.
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

    fn nz(n: u32) -> NonZeroU32 {
        NonZeroU32::new(n).expect("nonzero")
    }

    #[tokio::test]
    async fn health_is_served_through_the_edge() {
        let (addr, _shutdown) = spawn(test_app(None)).await;
        let response =
            Client::new().get(format!("http://{addr}{HEALTH_PATH}")).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cors_allows_any_origin_without_credentials() {
        let (addr, _shutdown) = spawn(test_app(None)).await;
        let client = Client::new();

        // preflight
        let preflight = client
            .request(Method::OPTIONS, format!("http://{addr}/v1/rpcs"))
            .header("origin", "https://site.example")
            .header("access-control-request-method", "GET")
            .send()
            .await
            .expect("send");
        assert!(preflight.status().is_success(), "{}", preflight.status());
        let headers = preflight.headers();
        assert_eq!(headers.get("access-control-allow-origin").expect("allow-origin"), "*");
        let methods = headers.get("access-control-allow-methods").expect("methods");
        assert!(methods.to_str().expect("ascii").contains("GET"));
        assert!(headers.get("access-control-allow-credentials").is_none());

        // actual request
        let response = client
            .get(format!("http://{addr}/v1/rpcs"))
            .header("origin", "https://site.example")
            .send()
            .await
            .expect("send");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("access-control-allow-origin").expect("allow-origin"),
            "*"
        );
        assert!(response.headers().get("access-control-allow-credentials").is_none());
    }

    #[tokio::test]
    async fn over_limit_request_gets_json_429() {
        // Global limit of one request with no burst headroom: the first request
        // passes, the second (same instant, no refill) is rejected with 429.
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) = spawn(test_app(Some(limiters))).await;

        let client = Client::new();
        let first = client.get(format!("http://{addr}/v1/records")).send().await.expect("send");
        assert_eq!(first.status(), StatusCode::OK);
        let second = client.get(format!("http://{addr}/v1/records")).send().await.expect("send");
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
        let body: serde_json::Value = second.json().await.expect("json");
        assert_eq!(body["error"], "rate_limited");
    }

    #[tokio::test]
    async fn probes_bypass_rate_limit() {
        // A maximally strict global limit (burst 1). If probes were rate-limited,
        // the second hit would be 429; they must stay answered so an
        // orchestrator does not kill the pod under load.
        let limiters = RateLimiters::new(
            None,
            Some(RateLimit::new(nz(1), nz(1))),
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) = spawn(test_app(Some(limiters))).await;

        let client = Client::new();
        for path in ["/healthz", "/health", "/readyz", "/ready", "/healthz"] {
            let response = client.get(format!("http://{addr}{path}")).send().await.expect("send");
            assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS, "{path}");
        }
    }

    #[tokio::test]
    async fn request_deadline_is_enveloped_as_json() {
        // A route slower than the deadline: the timeout layer's 408 must come back as JSON.
        let slow = Router::new().route(
            "/slow",
            get(|| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                "late"
            }),
        );
        let app = router(slow, Duration::from_millis(200), 16 * 1024, None);
        let (addr, _shutdown) = spawn(app).await;

        let response = Client::new().get(format!("http://{addr}/slow")).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        let body: serde_json::Value = response.json().await.expect("json");
        assert_eq!(body["error"], "request_timeout");
    }

    #[tokio::test]
    async fn slow_headers_are_disconnected() {
        let limits =
            ServerLimits { header_read_timeout: Duration::from_millis(300), ..test_limits() };
        let (addr, _shutdown) = spawn_with_limits(test_app(None), limits).await;

        // Slow-loris probe: send a partial request line, then stall. The server
        // must close the connection once the header deadline passes, rather
        // than hold it open indefinitely.
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        stream.write_all(b"GET /v1/rpcs HTTP/1.1\r\nHost: api\r\n").await.expect("write");
        let mut buf = [0_u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
            .await
            .expect("connection should be closed by the header read timeout");
        assert_eq!(read.expect("read"), 0, "expected EOF from the server");
    }

    #[tokio::test]
    async fn connection_cap_releases_permits() {
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(1).expect("nonzero"),
            ..test_limits()
        };
        let (addr, _shutdown) = spawn_with_limits(test_app(None), limits).await;

        // Two sequential requests over connections that close after each
        // response: the second only succeeds if the first connection's permit
        // is released, so a permit leak would hang (and time out) this test.
        for _ in 0..2 {
            let response = Client::new()
                .get(format!("http://{addr}/v1/records"))
                .header("connection", "close")
                .send()
                .await
                .expect("send");
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn lifetime_cap_closes_idle_connection_and_releases_permit() {
        // One connection slot total, capped at 300ms; the header read deadline
        // (5s) stays out of the way of both asserts below.
        let limits = ServerLimits {
            max_connections: NonZeroUsize::new(1).expect("nonzero"),
            max_connection_duration: Some(Duration::from_millis(300)),
            ..test_limits()
        };
        let (addr, _shutdown) = spawn_with_limits(test_app(None), limits).await;

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
            Client::new().get(format!("http://{addr}{HEALTH_PATH}")).send(),
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
}
