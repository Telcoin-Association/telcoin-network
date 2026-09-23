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

use futures::{Stream, StreamExt};
use serde::Serialize;
use tn_types::{TaskSpawner, WorkerId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{debug, info};

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
/// timeout an empty worker list is reported (fail-closed).
const READINESS_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

/// Fixed liveness response: the process is up. Returned for every path other
/// than the readiness route (and for empty or malformed requests).
const LIVENESS_RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";

/// Fallback body used only if the readiness payload (impossibly) fails to
/// serialize: report no ready workers so the gateway fails closed.
const READINESS_FALLBACK: &str = r#"{"version":1,"workers":[]}"#;

/// Readiness of a single worker, as reported by `GET /health/workers`.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(crate) struct WorkerReadiness {
    /// The worker's id, independent of its position in the response array.
    worker_id: WorkerId,
    /// Whether the worker is up and accepting transactions.
    ///
    /// True only when the worker's RPC server and pool are initialized, its id is
    /// in the current committee's worker range, and that epoch has not shut down.
    /// Persistent components alone do not imply current readiness.
    accepting_transactions: bool,
}

impl WorkerReadiness {
    /// Construct one worker's readiness entry without exposing mutable fields.
    pub(crate) fn new(worker_id: WorkerId, accepting_transactions: bool) -> Self {
        Self { worker_id, accepting_transactions }
    }
}

/// Versioned envelope served at `GET /health/workers`.
///
/// The `workers` array reports every initialized worker by id. Workers outside
/// the active epoch remain in the list with a false accepting flag. An empty
/// array indicates that no workers are initialized or the probe timed out.
#[derive(Debug, Serialize)]
struct NodeReadiness {
    /// Envelope version; see [`READINESS_VERSION`].
    version: u32,
    /// Readiness for each known worker.
    workers: Vec<WorkerReadiness>,
}

impl NodeReadiness {
    /// Build the v1 envelope for a snapshot of all initialized workers.
    fn new(workers: Vec<WorkerReadiness>) -> Self {
        Self { version: READINESS_VERSION, workers }
    }
}

/// Serialize the v1 readiness body for all workers in the probe snapshot.
fn readiness_json(workers: Vec<WorkerReadiness>) -> String {
    let readiness = NodeReadiness::new(workers);
    serde_json::to_string(&readiness).unwrap_or_else(|_| READINESS_FALLBACK.to_string())
}

/// Bound readiness probing and fail closed if the snapshot cannot be acquired in time.
async fn probe_readiness<Fut>(probe: Fut) -> Vec<WorkerReadiness>
where
    Fut: Future<Output = Vec<WorkerReadiness>>,
{
    timeout(READINESS_PROBE_TIMEOUT, probe).await.unwrap_or_default()
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
    /// `worker_ready` is polled per readiness request for all initialized workers'
    /// current accepting states. It is a closure (rather than a concrete
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
        Fut: Future<Output = Vec<WorkerReadiness>> + Send,
    {
        // IMPORTANT: use firewall to protect this endpoint
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr).await?;
        let listen_on = listener.local_addr()?;
        info!(target: "epoch-manager", ?listen_on, "healthcheck listening");

        // wrap the listener in a stream so the accept loop is a testable seam
        // (`serve` below): production feeds it the real `TcpListenerStream`, and
        // tests feed it a stream that injects a failing accept.
        task_spawner.spawn_critical_task("healthcheck", async move {
            serve(TcpListenerStream::new(listener), worker_ready).await;
            Ok(())
        });

        Ok(listen_on)
    }
}

/// Drive the accept loop over a stream of accepted connections.
///
/// Serves each connection synchronously (bounded per-connection read timeout),
/// routing the workers path to readiness and everything else to liveness.
///
/// A transient `accept()` error (fd exhaustion `EMFILE`/`ENFILE`,
/// `ECONNABORTED`, `EINTR`, `ENOBUFS`) is logged and skipped: the loop must
/// keep serving. The caller spawns this as a *critical* task, so ending the
/// loop would resolve the task `Ok` and notify a whole-node shutdown - exactly
/// the outage this endpoint is supposed to warn about. This mirrors the metrics
/// server (`tn_metrics::server`), whose accept loop swallows the same errors.
async fn serve<S, F, Fut>(mut incoming: S, worker_ready: F)
where
    S: Stream<Item = std::io::Result<TcpStream>> + Unpin,
    F: Fn() -> Fut,
    Fut: Future<Output = Vec<WorkerReadiness>>,
{
    // the loop survives a transient accept error because the `Some(Err(..))`
    // arm below logs and skips instead of breaking. `worker_ready` is called
    // inline in the loop body (rather than from a per-connection `for_each`
    // closure) for a separate reason: it keeps a `Sync` bound off the public
    // `spawn` signature.
    loop {
        match incoming.next().await {
            Some(Ok(mut socket)) => {
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
                    let body = readiness_json(probe_readiness(worker_ready()).await);
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
            // transient accept errors (e.g. fd exhaustion) must not kill the node
            Some(Err(e)) => debug!(target: "epoch-manager", ?e, "healthcheck accept error"),
            // the production `TcpListenerStream` is infinite; a finite test
            // stream ends here once drained.
            None => break,
        }
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

    use super::{
        probe_readiness, readiness_json, request_path, serve, HealthcheckServer, WorkerReadiness,
    };
    use futures::StreamExt;
    use tn_types::TaskManager;

    /// Send `request` to the spawned server at `addr` and return the response.
    async fn roundtrip(addr: std::net::SocketAddr, request: &[u8]) -> eyre::Result<String> {
        tokio::time::timeout(Duration::from_millis(500), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(request).await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<String, eyre::Error>(String::from_utf8_lossy(&response).into_owned())
        })
        .await?
    }

    #[tokio::test]
    async fn test_tcp_healthcheck() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        // liveness path never polls the readiness probe
        let addr =
            HealthcheckServer::spawn(task_spawner.clone(), 0, futures::future::pending).await?;

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

        let addr = HealthcheckServer::spawn(task_spawner.clone(), 0, || async {
            vec![WorkerReadiness::new(0, false)]
        })
        .await?;

        let response = roundtrip(addr, b"GET /health/workers HTTP/1.1\r\n\r\n").await?;

        assert!(response.starts_with("HTTP/1.1 200 OK"), "Expected 200 OK, got: {}", response);
        assert!(
            response.contains("Content-Type: application/json"),
            "Expected json content type, got: {}",
            response
        );
        let body =
            response.split_once("\r\n\r\n").ok_or_else(|| eyre::eyre!("response has no body"))?.1;
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

        let addr = HealthcheckServer::spawn(task_spawner.clone(), 0, || async {
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, false)]
        })
        .await?;

        let response = roundtrip(addr, b"GET /health/workers HTTP/1.1\r\n\r\n").await?;

        let body =
            response.split_once("\r\n\r\n").ok_or_else(|| eyre::eyre!("response has no body"))?.1;
        assert_eq!(
            body,
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true},{"worker_id":1,"accepting_transactions":false}]}"#
        );

        Ok(())
    }

    /// Regression for #943: a failed `accept()` must not end the loop.
    ///
    /// Before the fix the accept loop was `while let Ok(..) = accept()`, so the
    /// first `Err` fell out of the loop, the critical task resolved `Ok`, and
    /// the whole node was shut down. Inject a failing accept ahead of a real
    /// listener and assert the endpoint still serves the next connection.
    #[tokio::test]
    async fn test_accept_error_does_not_end_loop() -> eyre::Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        // a transient accept() error (fd exhaustion, ECONNABORTED, ...) followed
        // by the real listener stream. `Box::pin` makes the chained stream Unpin.
        let incoming = Box::pin(
            futures::stream::once(async {
                Err::<tokio::net::TcpStream, _>(std::io::Error::other("simulated accept error"))
            })
            .chain(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let handle = tokio::spawn(async move { serve(incoming, || async { Vec::new() }).await });

        // the loop logged+skipped the injected error and kept serving
        let response = roundtrip(addr, b"GET / HTTP/1.1\r\n\r\n").await?;
        assert!(
            response.starts_with("HTTP/1.1 200 OK"),
            "endpoint should still serve after a failed accept, got: {response}"
        );
        assert!(response.ends_with("OK"), "expected liveness body 'OK', got: {response}");

        // the serve loop is still running (it did not resolve on the error)
        assert!(!handle.is_finished(), "accept error must not end the serve loop");
        handle.abort();

        Ok(())
    }

    #[test]
    fn test_readiness_payload_contract() {
        // Lock the exact wire contract: snake_case keys, version 1, per-worker entries.
        assert_eq!(
            readiness_json(vec![WorkerReadiness::new(0, true)]),
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true}]}"#,
        );
        assert_eq!(
            readiness_json(vec![WorkerReadiness::new(0, false)]),
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":false}]}"#,
        );
        assert_eq!(
            readiness_json(vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]),
            r#"{"version":1,"workers":[{"worker_id":0,"accepting_transactions":true},{"worker_id":1,"accepting_transactions":true}]}"#,
        );
        assert_eq!(readiness_json(Vec::new()), r#"{"version":1,"workers":[]}"#);
    }

    /// A stalled snapshot must not advertise any worker as accepting transactions.
    #[tokio::test(start_paused = true)]
    async fn test_readiness_probe_timeout_fails_closed() {
        let workers = probe_readiness(futures::future::pending()).await;
        assert!(workers.is_empty());
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
