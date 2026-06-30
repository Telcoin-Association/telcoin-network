//! Simple TCP health/readiness endpoints for monitoring service availability.
//!
//! Implements a minimal HTTP/1.1 server with two routes on a single port:
//! - any path except `/health/workers` -> liveness: a fixed `200 OK` (the process is up), matching
//!   the original unconditional behavior.
//! - `GET /health/workers` -> readiness: a `200 OK` carrying a JSON envelope that reports, per
//!   worker, whether the worker is accepting transactions.
//!
//! The readiness route is the contract a stateless worker gateway polls to
//! decide whether to forward RPC traffic to this node (see issue #712). The
//! endpoint always answers `200`; the JSON body is the machine-readable signal,
//! so the gateway (not the node) is responsible for translating "not accepting"
//! into a client-facing `503`.

use std::{future::Future, net::SocketAddr, time::Duration};

use serde::Serialize;
use tn_types::{TaskSpawner, WorkerId, DEFAULT_WORKER_ID};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
use tracing::info;

/// Request path that serves the per-worker readiness envelope.
const WORKERS_PATH: &str = "/health/workers";

/// Version of the `/health/workers` payload envelope. Bump when the shape
/// changes so the gateway parser can stay forward-compatible.
const READINESS_VERSION: u32 = 1;

/// Bound on bytes read while parsing the request line. The HTTP request-target
/// always fits well within this, so a single read suffices to route.
const REQUEST_READ_BUF: usize = 1024;

/// Upper bound on waiting for a client's request before falling back to the
/// liveness response, so a slow or idle client cannot stall the synchronous
/// accept loop and starve other probes.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Upper bound on the readiness probe so a contended engine lock (e.g. held by
/// a writer during an epoch transition) cannot stall the accept loop. On
/// timeout the worker is reported not-ready (fail-closed).
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Fixed liveness response: the process is up. Returned for every path other
/// than the readiness route (and for empty or malformed requests).
const LIVENESS_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

/// Fallback body used only if the readiness payload (impossibly) fails to
/// serialize: report no ready workers so the gateway fails closed.
const READINESS_FALLBACK: &str = r#"{"version":1,"workers":[]}"#;

/// Readiness of a single worker, as reported by `GET /health/workers`.
#[derive(Debug, Serialize)]
struct WorkerReadiness {
    /// The worker's id (its index in the node's worker set; v1 is always `0`).
    worker_id: WorkerId,
    /// Whether the worker is up and accepting transactions.
    ///
    /// True once the worker's RPC server + transaction pool are initialized.
    /// These are created on the node's first epoch and are not torn down across
    /// epoch transitions, so this stays true for the life of the process once
    /// the node has started. A node that is down answers no request at all,
    /// which the polling gateway treats as not-ready (fail-closed).
    accepting_transactions: bool,
}

/// Versioned envelope served at `GET /health/workers`.
///
/// The `workers` array maps onto the node's worker set (indexed by worker id)
/// and ships with exactly one entry (worker `0`) today; the method-aware
/// routing follow-up can extend the per-worker fields without breaking the
/// envelope shape.
#[derive(Debug, Serialize)]
struct NodeReadiness {
    /// Envelope version; see [`READINESS_VERSION`].
    version: u32,
    /// Readiness for each known worker.
    workers: Vec<WorkerReadiness>,
}

impl NodeReadiness {
    /// Build the v1 single-worker envelope.
    fn single_worker(worker_id: WorkerId, accepting_transactions: bool) -> Self {
        Self {
            version: READINESS_VERSION,
            workers: vec![WorkerReadiness { worker_id, accepting_transactions }],
        }
    }
}

/// Serialize the v1 readiness body for worker `0` given its accepting state.
fn readiness_json(accepting_transactions: bool) -> String {
    let readiness = NodeReadiness::single_worker(DEFAULT_WORKER_ID, accepting_transactions);
    serde_json::to_string(&readiness).unwrap_or_else(|_| READINESS_FALLBACK.to_string())
}

/// Minimal HTTP health/readiness responder for service monitoring.
///
/// Binds to a TCP port and serves the liveness and `/health/workers` readiness
/// routes. Uses raw TCP sockets for minimal overhead and dependencies.
///
/// # Security Considerations
///
/// This endpoint accepts connections from any source. Liveness responds
/// unconditionally; readiness reports only worker-id and an accepting flag (no
/// sensitive internals).
///
/// Node operators must ensure the endpoint is protected by a firewall.
/// This service is off by default, but can be enabled through the CLI node
/// command. Each connection is handled synchronously in the accept loop, with a
/// read timeout so a slow client cannot stall the loop. No connection limits or
/// rate limiting are implemented. Connections are closed after the response.
///
/// To enable on node startup, use `telcoin-network node --healthcheck <PORT>`.
/// See `telcoin-network-cli::node` for more info.
#[derive(Debug)]
pub(crate) struct HealthcheckServer;

impl HealthcheckServer {
    /// Spawns the health check server task and returns the bound address.
    ///
    /// Binds to the given `port` (or lets the OS assign one if `0`).
    ///
    /// `worker_ready` is polled per readiness request to learn whether worker
    /// `0` is accepting transactions. It is a closure (rather than a concrete
    /// node handle) so the server stays decoupled from the engine and unit
    /// testable; the production call site captures the [`ExecutionNode`] handle.
    ///
    /// [`ExecutionNode`]: crate::engine::ExecutionNode
    ///
    /// # Network Binding
    ///
    /// Binds to 0.0.0.0 (all interfaces) to allow external health checkers.
    /// This makes the service accessible on all network interfaces including
    /// public IPs.
    ///
    /// # Protocol
    ///
    /// Implements minimal HTTP/1.1 with two routes:
    /// - liveness (any other path): `200 OK`, body `"OK"`.
    /// - `GET /health/workers`: `200 OK`, `application/json` readiness envelope.
    pub(crate) async fn spawn<F, Fut>(
        task_spawner: TaskSpawner,
        port: u16,
        worker_ready: F,
    ) -> eyre::Result<SocketAddr>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: Future<Output = bool> + Send,
    {
        // IMPORTANT: use firewall to protect this endpoint
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr).await?;
        let listen_on = listener.local_addr()?;
        info!(target: "epoch-manager", ?listen_on, "healthcheck listening");

        task_spawner.spawn_critical_task("healthcheck", async move {
            // accept loop - no connection limits; bounded read timeout per conn
            while let Ok((mut socket, _)) = listener.accept().await {
                // read the request with a bounded timeout so a slow or idle
                // client cannot stall the loop; treat a timeout/error/empty
                // read as "no path" and fall through to the liveness response.
                let mut buf = [0u8; REQUEST_READ_BUF];
                let n = timeout(REQUEST_READ_TIMEOUT, socket.read(&mut buf))
                    .await
                    .ok()
                    .and_then(Result::ok)
                    .unwrap_or(0);

                // route on the request-line path; readiness for the workers
                // path, liveness for everything else (preserves prior behavior)
                if request_path(&buf[..n]).is_some_and(|path| path == WORKERS_PATH) {
                    // bound the readiness probe too: if it cannot resolve
                    // quickly (e.g. the engine lock is held during an epoch
                    // transition) report not-ready rather than stalling the loop
                    let accepting =
                        timeout(READINESS_PROBE_TIMEOUT, worker_ready()).await.unwrap_or(false);
                    let body = readiness_json(accepting);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body,
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                } else {
                    // write liveness response, ignore errors (client disconnect)
                    let _ = socket.write_all(LIVENESS_RESPONSE).await;
                }
            }
            Ok(())
        });

        Ok(listen_on)
    }
}

/// Extract the request-target (path) from the first line of an HTTP request.
///
/// Returns `None` when `request` is not valid UTF-8 or has no well-formed
/// request line (`METHOD SP PATH SP VERSION`).
fn request_path(request: &[u8]) -> Option<&str> {
    let line = std::str::from_utf8(request).ok()?.lines().next()?;
    let target = line.split_whitespace().nth(1)?;
    // ignore any query string so e.g. `/health/workers?probe=1` still routes
    Some(target.split_once('?').map_or(target, |(path, _query)| path))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::{readiness_json, request_path, HealthcheckServer};
    use tn_types::{get_available_tcp_port, TaskManager};

    /// Send `request` to the spawned server at `addr` and return the response.
    async fn roundtrip(addr: std::net::SocketAddr, request: &[u8]) -> eyre::Result<String> {
        tokio::time::timeout(Duration::from_millis(500), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(request).await?;
            let mut response = vec![0u8; 1024];
            let n = stream.read(&mut response).await?;
            response.truncate(n);
            Ok::<String, eyre::Error>(String::from_utf8_lossy(&response).into_owned())
        })
        .await
        .expect("response received")
    }

    #[tokio::test]
    async fn test_tcp_healthcheck() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        // liveness path never polls the readiness probe
        let addr = HealthcheckServer::spawn(task_spawner.clone(), port, || async { false }).await?;

        // give server time to start listening
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = roundtrip(addr, b"GET / HTTP/1.1\r\n\r\n").await?;

        // verify http status line
        assert!(response.starts_with("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response);
        // verify body
        assert!(response.ends_with("OK"), "Expected body 'OK', got: {}", response);
        // verify content-length header
        assert!(
            response.contains("Content-Length: 2"),
            "Missing or incorrect Content-Length header"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_health_workers_reports_not_ready() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr = HealthcheckServer::spawn(task_spawner.clone(), port, || async { false }).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = roundtrip(addr, b"GET /health/workers HTTP/1.1\r\n\r\n").await?;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response);
        assert!(
            response.contains("Content-Type: application/json"),
            "Expected json content type, got: {}",
            response
        );
        let body = response.split("\r\n\r\n").nth(1).expect("response has a body");
        assert_eq!(
            body,
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false}]}"#
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_health_workers_reports_ready() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr = HealthcheckServer::spawn(task_spawner.clone(), port, || async { true }).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;

        let response = roundtrip(addr, b"GET /health/workers HTTP/1.1\r\n\r\n").await?;

        let body = response.split("\r\n\r\n").nth(1).expect("response has a body");
        assert_eq!(
            body,
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#
        );

        Ok(())
    }

    #[test]
    fn test_readiness_payload_contract() {
        // lock the exact wire contract: snake_case keys, version 1, one worker
        assert_eq!(
            readiness_json(true),
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#,
        );
        assert_eq!(
            readiness_json(false),
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false}]}"#,
        );
    }

    #[test]
    fn test_request_path_parsing() {
        assert_eq!(request_path(b"GET /health/workers HTTP/1.1\r\n\r\n"), Some("/health/workers"));
        assert_eq!(request_path(b"POST /health/workers HTTP/1.1\r\n\r\n"), Some("/health/workers"));
        assert_eq!(
            request_path(b"GET /health/workers?probe=1 HTTP/1.1\r\n\r\n"),
            Some("/health/workers")
        );
        assert_eq!(request_path(b"GET / HTTP/1.1\r\n\r\n"), Some("/"));
        assert_eq!(request_path(b""), None);
        assert_eq!(request_path(b"garbage"), None);
    }
}
