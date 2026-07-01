//! The gateway's HTTP surface: the JSON-RPC proxy plus liveness / readiness.
//!
//! A single axum server serves all three on one address: any request that is
//! not `GET /health` or `GET /ready` falls through to the proxy, so JSON-RPC
//! (`POST /`) is forwarded while orchestration probes hit the health routes.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use reqwest::Client;
use serde::Serialize;
use tn_types::{Noticer, TaskError};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::{
    proxy::{proxy, MAX_REQUEST_BYTES},
    readiness::GatewayReadiness,
};

/// Shared state handed to every request handler.
#[derive(Clone, Debug)]
pub(crate) struct AppState {
    /// Live readiness view of the configured upstream workers.
    pub(crate) readiness: Arc<GatewayReadiness>,
    /// Client used to forward requests to upstream workers.
    pub(crate) http: Client,
}

/// JSON body of the gateway's `/ready` response.
#[derive(Debug, Serialize)]
struct ReadyBody {
    /// Whether at least one upstream worker is currently ready.
    ready: bool,
}

/// Build the gateway router: health/readiness routes plus the proxy fallback.
pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(liveness))
        .route("/ready", get(readiness))
        .fallback(proxy)
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BYTES))
        .with_state(state)
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

/// Bind `listen_addr` and serve until `graceful` fires, then stop accepting and
/// drain in-flight requests until they finish or `graceful_timeout` elapses
/// after the signal (tracked by `deadline`), whichever comes first.
pub(crate) async fn serve(
    listen_addr: SocketAddr,
    state: AppState,
    graceful_timeout: Duration,
    graceful: Noticer,
    deadline: Noticer,
) -> Result<(), TaskError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(target: "gateway::server", %local_addr, "worker gateway listening");

    let server = axum::serve(listener, router(state)).with_graceful_shutdown(async move {
        graceful.await;
        info!(target: "gateway::server", "shutdown signal received; draining in-flight requests");
    });

    tokio::select! {
        result = server => result.map_err(TaskError::from),
        () = drain_deadline(deadline, graceful_timeout) => {
            warn!(
                target: "gateway::server",
                timeout = ?graceful_timeout,
                "graceful shutdown deadline exceeded; forcing close"
            );
            Ok(())
        }
    }
}

/// Resolve once the shutdown signal has fired and `graceful_timeout` has then
/// elapsed, bounding how long the drain may take.
async fn drain_deadline(deadline: Noticer, graceful_timeout: Duration) {
    deadline.await;
    tokio::time::sleep(graceful_timeout).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::UpstreamWorker;
    use axum::routing::post;
    use tokio::task::JoinHandle;
    use url::Url;

    fn test_state(upstreams: &[UpstreamWorker]) -> AppState {
        AppState {
            readiness: Arc::new(GatewayReadiness::new(upstreams)),
            http: Client::builder().build().expect("build client"),
        }
    }

    async fn spawn(app: Router) -> (SocketAddr, JoinHandle<()>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (addr, handle)
    }

    fn upstream(addr: SocketAddr) -> UpstreamWorker {
        UpstreamWorker {
            worker_id: 0,
            rpc_url: Url::parse(&format!("http://{addr}/")).expect("rpc url"),
            readiness_url: Url::parse(&format!("http://{addr}/health/workers")).expect("ready url"),
        }
    }

    #[tokio::test]
    async fn health_is_always_ok() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (addr, _server) = spawn(router(state)).await;

        let response =
            Client::new().get(format!("http://{addr}/health")).send().await.expect("send");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_reflects_upstream_state() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let readiness = Arc::clone(&state.readiness);
        let (addr, _server) = spawn(router(state)).await;
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
        let (gateway_addr, _server) = spawn(router(state)).await;

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
    async fn rejects_with_jsonrpc_error_when_no_upstream_ready() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        let (gateway_addr, _server) = spawn(router(state)).await;

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
    async fn oversized_body_gets_jsonrpc_error() {
        let state = test_state(&[upstream("127.0.0.1:1".parse().expect("addr"))]);
        // Tiny body limit so a small request trips the DefaultBodyLimit path.
        let app = Router::new().fallback(proxy).layer(DefaultBodyLimit::max(8)).with_state(state);
        let (addr, _server) = spawn(app).await;

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
}
