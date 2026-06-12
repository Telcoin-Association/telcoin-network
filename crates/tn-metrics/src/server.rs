//! Minimal HTTP `/metrics` endpoint for Prometheus scrapes.
//!
//! Mirrors the raw-TCP pattern of the node healthcheck server: no request parsing, no
//! connection limits, every connection receives the full rendered registry. Node operators
//! must protect the endpoint with a firewall.

use std::{fmt, net::SocketAddr, time::Duration};

use tn_types::TaskSpawner;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time,
};
use tracing::{debug, info};

use crate::recorder::install_recorder;

/// Callbacks executed before each scrape renders the registry.
///
/// Used for metrics that are sampled on demand rather than recorded on events,
/// e.g. process stats and database table sizes.
pub struct MetricsHooks {
    /// The hooks to run, in registration order.
    hooks: Vec<Box<dyn Fn() + Send + Sync>>,
}

impl Default for MetricsHooks {
    /// The default hook set samples process metrics (cpu, memory, fds) on every scrape.
    fn default() -> Self {
        let collector = metrics_process::Collector::default();
        // register HELP text once; requires the global recorder to be installed already
        collector.describe();
        Self { hooks: vec![Box::new(move || collector.collect())] }
    }
}

impl fmt::Debug for MetricsHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsHooks").field("hooks", &self.hooks.len()).finish()
    }
}

impl MetricsHooks {
    /// Create an empty hook set (no process metrics).
    pub fn empty() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Add a hook to run before each scrape.
    pub fn with_hook(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.hooks.push(Box::new(hook));
        self
    }

    /// Run all hooks in registration order.
    fn run(&self) {
        for hook in &self.hooks {
            hook();
        }
    }
}

/// Start the Prometheus scrape endpoint and return the bound address.
///
/// Installs the global recorder if it isn't installed yet (idempotent - the node CLI
/// installs it much earlier, before reth components are constructed). Registers a static
/// `tn_info{version=...} 1` gauge for dashboards, then spawns a critical task that serves
/// scrapes and runs registry upkeep every 5s (drains stale histogram samples).
///
/// # Security Considerations
///
/// Like the healthcheck endpoint, this accepts connections from any source and responds
/// unconditionally, without parsing the request. Operators must firewall the port.
/// Binding to a loopback address (e.g. `127.0.0.1:9001`) with a local scraper relaying
/// to remote storage is the recommended deployment.
pub async fn start_metrics_server(
    addr: SocketAddr,
    task_spawner: &TaskSpawner,
    version: &'static str,
    hooks: MetricsHooks,
) -> eyre::Result<SocketAddr> {
    let handle = install_recorder()?;

    // IMPORTANT: use firewall to protect this endpoint
    let listener = TcpListener::bind(addr).await?;
    let listen_on = listener.local_addr()?;

    // static info series so dashboards can surface the running version
    metrics::gauge!("tn_info", "version" => version).set(1.0);

    info!(target: "tn::metrics", ?listen_on, "prometheus metrics endpoint listening");

    task_spawner.spawn_critical_task("metrics", async move {
        let mut upkeep = time::interval(Duration::from_secs(5));
        upkeep.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = upkeep.tick() => handle.run_upkeep(),
                accepted = listener.accept() => {
                    match accepted {
                        Ok((mut socket, _)) => {
                            // drain the request before responding - closing a socket with
                            // unread bytes in the kernel buffer sends RST instead of FIN,
                            // resetting the response mid-flight on the scraper's side. the
                            // timeout bounds idle probes that connect but never send.
                            let mut buf = [0u8; 1024];
                            let _ = time::timeout(
                                Duration::from_secs(2),
                                socket.read(&mut buf),
                            )
                            .await;
                            hooks.run();
                            let body = handle.render();
                            let response = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body,
                            );
                            // write response, ignore errors (client disconnect, etc.)
                            // then drop connection. the timeout bounds a client that
                            // connects but never drains the body - without it, TCP
                            // backpressure would block write_all indefinitely and freeze
                            // the whole metrics task, including histogram upkeep.
                            let _ = time::timeout(Duration::from_secs(5), async {
                                let _ = socket.write_all(response.as_bytes()).await;
                                let _ = socket.shutdown().await;
                            })
                            .await;
                        }
                        // transient accept errors (e.g. fd exhaustion) must not kill the node
                        Err(e) => debug!(target: "tn::metrics", ?e, "metrics endpoint accept error"),
                    }
                }
            }
        }
    });

    Ok(listen_on)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tn_types::TaskManager;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// End-to-end scrape test. This is the ONLY test suite allowed to install the global
    /// recorder - instrumented crates must use `metrics::with_local_recorder` instead.
    #[tokio::test]
    async fn test_metrics_endpoint_serves_tn_and_reth_metrics() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        // port 0: let the OS assign
        let addr = start_metrics_server(
            "127.0.0.1:0".parse()?,
            &task_spawner,
            "test-version",
            MetricsHooks::default(),
        )
        .await?;

        // record one tn metric and one fake reth-internal metric
        metrics::counter!("tn_test_counter").increment(1);
        metrics::counter!("db.fake").increment(1);

        let response_str = tokio::time::timeout(Duration::from_secs(5), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;

            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<_, eyre::Error>(String::from_utf8_lossy(&response).to_string())
        })
        .await
        .expect("response received")?;

        assert!(response_str.starts_with("HTTP/1.1 200 OK"), "{response_str}");
        assert!(response_str.contains("Content-Type: text/plain; version=0.0.4"), "{response_str}");
        // tn metrics pass through untouched
        assert!(response_str.contains("tn_test_counter 1"), "{response_str}");
        // non-tn metrics render with the reth prefix
        assert!(response_str.contains("reth_db_fake 1"), "{response_str}");
        // the version info series is registered by the server
        assert!(response_str.contains("tn_info{version=\"test-version\"} 1"), "{response_str}");
        // the default hook samples process metrics on scrape
        assert!(response_str.contains("reth_process_cpu_seconds_total"), "{response_str}");

        Ok(())
    }
}
