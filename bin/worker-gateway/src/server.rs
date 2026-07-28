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
//! read deadline, a whole-request deadline, `TCP_NODELAY`, and a global
//! concurrent-connection cap.

use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use axum::{
    extract::{ConnectInfo, DefaultBodyLimit, State},
    http::StatusCode,
    middleware::{from_fn_with_state, map_response},
    response::{IntoResponse, Response},
    routing::get,
    Extension, Json, Router,
};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::graceful::GracefulShutdown,
    service::TowerToHyperService,
};
use reqwest::Client;
use serde::Serialize;
use tn_types::{Noticer, TaskError};
use tokio::{net::TcpListener, sync::Semaphore};
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, info, warn};

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
    /// Client used to forward requests to upstream workers.
    pub(crate) http: Client,
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
/// *response* bodies are written after the handler returns and are bounded by
/// the upstream client's own total timeout instead. `max_request_bytes` caps
/// the buffered request body.
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
/// with the configured header deadline, `TCP_NODELAY`, and connection cap,
/// then drain within `graceful_timeout`.
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

        // Hand handlers the real client address (`ConnectInfo`, consumed by the
        // proxy's `X-Forwarded-For`).
        let service =
            TowerToHyperService::new(app.clone().layer(Extension(ConnectInfo(peer_addr))));
        let connection =
            graceful.watch(connection_builder.serve_connection(TokioIo::new(stream), service));
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                debug!(target: "gateway::server", %err, "connection error");
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::UpstreamWorker, proxy::MAX_REQUEST_BYTES, ratelimit::RateLimit};
    use axum::{http::HeaderMap, routing::post};
    use std::num::NonZeroU32;
    use tn_types::Notifier;
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpStream,
    };
    use url::Url;

    /// Generous limits so only the behavior under test can trip.
    fn test_limits() -> ServerLimits {
        ServerLimits {
            header_read_timeout: Duration::from_secs(5),
            request_deadline: Duration::from_secs(5),
            max_connections: NonZeroUsize::new(64).expect("nonzero"),
            max_request_bytes: MAX_REQUEST_BYTES,
        }
    }

    fn test_state(upstreams: &[UpstreamWorker]) -> AppState {
        test_state_with_client(upstreams, Client::builder().build().expect("build client"))
    }

    fn test_state_with_client(upstreams: &[UpstreamWorker], client: Client) -> AppState {
        AppState { readiness: Arc::new(GatewayReadiness::new(upstreams)), http: client }
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
        let limiters =
            RateLimiters::new(None, Some(RateLimit::new(nz(1), nz(1))), 16).expect("limiters");
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
        let limiters =
            RateLimiters::new(None, Some(RateLimit::new(nz(1), nz(1))), 16).expect("limiters");
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
}
