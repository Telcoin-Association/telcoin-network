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

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::StatusCode,
    middleware::{from_fn_with_state, map_response},
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
    proxy::proxy,
    ratelimit::{rate_limit, RateLimiters},
    readiness::GatewayReadiness,
};

/// Pause before re-polling `accept()` after it fails, so a persistent accept
/// error (e.g. fd exhaustion) cannot spin the loop hot.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(100);

/// Liveness probe path. Exempt from rate limiting (see [`crate::ratelimit`]).
pub(crate) const HEALTH_PATH: &str = "/health";

/// Readiness probe path. Exempt from rate limiting (see [`crate::ratelimit`]).
pub(crate) const READY_PATH: &str = "/ready";

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
    /// Per-request deadline on the query route (`--query-request-timeout`), or
    /// `None` when it follows the client's own (`--upstream-request-timeout`).
    pub(crate) query_request_timeout: Option<Duration>,
    /// In-flight cap for requests made only of submissions
    /// (`--max-inflight-submissions`); see [`inflight_slots`].
    pub(crate) submission_slots: Arc<Semaphore>,
    /// In-flight cap for every other request (`--max-inflight-queries`); see
    /// [`inflight_slots`].
    pub(crate) query_slots: Arc<Semaphore>,
    /// Cap on concurrent requests forwarded to a worker
    /// (`--max-upstream-inflight`); see [`inflight_slots`].
    pub(crate) upstream_slots: Arc<Semaphore>,
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
/// When `rate_limiters` is present it is installed as the outermost layer, so
/// an over-limit request is shed with a JSON-RPC `429` before its body is
/// buffered or forwarded.
pub(crate) fn router(
    state: AppState,
    request_deadline: Duration,
    max_request_bytes: usize,
    rate_limiters: Option<Arc<RateLimiters>>,
) -> Router {
    let router = Router::new()
        .route(HEALTH_PATH, get(liveness))
        .route(READY_PATH, get(readiness))
        .fallback(proxy)
        .layer(DefaultBodyLimit::max(max_request_bytes))
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_deadline))
        .layer(map_response(envelope_request_timeout));
    // Add the rate-limit layer last so it runs first, ahead of the body read.
    let router = match rate_limiters {
        Some(limiters) => router.layer(from_fn_with_state(limiters, rate_limit)),
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
    graceful_timeout: Duration,
    shutdown: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "gateway::server", %local_addr, "worker gateway listening");

    let app = router(state, limits.request_deadline, limits.max_request_bytes, rate_limiters);
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
        num::NonZeroU32,
        sync::atomic::{AtomicUsize, Ordering},
    };
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
            query_request_timeout: None,
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
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
        router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, None)
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
        let (addr, _shutdown) = spawn(router(state, Duration::from_secs(5), 8, None)).await;

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
            None,
            16,
            PrefixPolicy::default(),
        )
        .expect("limiters");
        let (addr, _shutdown) =
            spawn(router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, Some(limiters))).await;

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
        let (addr, _shutdown) =
            spawn(router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, Some(limiters))).await;

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
            router(state, Duration::from_millis(300), MAX_REQUEST_BYTES, None),
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
            query_request_timeout: None,
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
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
                axum::body::Body::from_stream(first.chain(futures::stream::pending()))
            }),
        ))
        .await;
        // the proxy client's 3s deadline is what ends the stalled body
        let client = proxy_client(Duration::from_secs(2), Duration::from_secs(3)).expect("client");
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
        state.query_request_timeout = Some(Duration::from_millis(200));
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
        let (gateway, gateway_server) =
            spawn(router(state, Duration::from_secs(5), MAX_REQUEST_BYTES, Some(limiters))).await;
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
}
