//! Sync-aware TCP healthcheck endpoint for monitoring service availability.
//!
//! Implements a minimal HTTP/1.1 server that responds with sync status information.
//! - Body "OK" when the node is fully synced (sync distance == 0)
//! - Body "behind N" when the node is catching up (N = blocks behind)
//! - Body "syncing" when no consensus data is available yet
//!
//! HTTP status is always 200 for load balancer compatibility (GCP, AWS, etc.).
//! Inspired by Solana's `GET /health` pattern.

use std::net::SocketAddr;

use tn_primary::ConsensusBus;
use tn_types::TaskSpawner;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tracing::info;

/// Sync-aware HTTP health check responder for service monitoring.
///
/// Binds to a TCP port and responds with HTTP 200 and a body indicating sync status.
/// Uses raw TCP sockets for minimal overhead and dependencies.
///
/// # Response Body
///
/// - `"OK"` when node is synced (execution caught up with consensus)
/// - `"behind N"` when node is N consensus blocks behind
/// - `"syncing"` when consensus data is not yet available
///
/// # Security Considerations
///
/// This endpoint accepts connections from any source and responds unconditionally.
///
/// Node operators must ensure the endpoint is protected by a firewall.
/// This service is off by default, but can be enabled through the CLI node command.
/// Each connection is handled synchronously in the main accept loop.
/// No connection limits or rate limiting are implemented.
/// Connections are immediately closed after sending response.
///
/// To enable on node startup, use `telcoin-network node --enable-healthcheck`.
/// See `telcoin-network-cli::node` for more info.
#[derive(Debug)]
pub(crate) struct HealthcheckServer;

impl HealthcheckServer {
    /// Spawns the health check server task and returns the bound address.
    ///
    /// Binds to port specified by `HEALTHCHECK_TCP_PORT` environment variable,
    /// or lets the OS assign a port if unset or set to 0.
    ///
    /// # Network Binding
    ///
    /// Binds to 0.0.0.0 (all interfaces) to allow external health checkers.
    /// This makes the service accessible on all network interfaces including public IPs.
    ///
    /// # Protocol
    ///
    /// Implements minimal HTTP/1.1 with a sync-aware response:
    /// - Status: 200 OK (always, for load balancer compatibility)
    /// - Body: "OK", "behind N", or "syncing"
    /// - No request parsing or validation
    /// - No custom headers to avoid information disclosure
    pub(crate) async fn spawn(
        task_spawner: TaskSpawner,
        port: u16,
        consensus_bus: ConsensusBus,
    ) -> eyre::Result<SocketAddr> {
        // IMPORTANT: use firewall to protect this endpoint
        let addr: SocketAddr = ([0, 0, 0, 0], port).into();
        let listener = TcpListener::bind(addr).await?;
        let listen_on = listener.local_addr()?;
        info!(target: "epoch-manager", ?listen_on, "healthcheck listening");

        task_spawner.spawn_critical_task("healthcheck", async move {
            // accept loop - no connection limits or timeouts
            while let Ok((mut socket, _)) = listener.accept().await {
                let body = Self::sync_status_body(&consensus_bus);
                let response =
                    format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}", body.len());
                // write response, ignore errors (client disconnect, etc.)
                // then drop connection
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });

        Ok(listen_on)
    }

    /// Compute the sync status body string from the consensus bus state.
    fn sync_status_body(consensus_bus: &ConsensusBus) -> String {
        let latest_executed = consensus_bus.latest_block_num_hash().number;
        let (latest_network, _) = *consensus_bus.last_published_consensus_num_hash().borrow();

        if latest_network == 0 && latest_executed == 0 {
            // No consensus data yet
            return "syncing".to_string();
        }

        let sync_distance = latest_network.saturating_sub(latest_executed);
        if sync_distance == 0 {
            "OK".to_string()
        } else {
            format!("behind {sync_distance}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use crate::health::HealthcheckServer;
    use tn_primary::ConsensusBus;
    use tn_types::{get_available_tcp_port, TaskManager};

    #[tokio::test]
    async fn test_tcp_healthcheck_syncing() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();
        let consensus_bus = ConsensusBus::new();

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr =
            HealthcheckServer::spawn(task_spawner.clone(), port, consensus_bus.clone()).await?;

        tokio::time::sleep(Duration::from_millis(10)).await;

        tokio::time::timeout(Duration::from_millis(500), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await?;

            let mut response = vec![0u8; 1024];
            let n = stream.read(&mut response).await?;
            response.truncate(n);
            let response_str = String::from_utf8_lossy(&response);

            assert!(
                response_str.starts_with("HTTP/1.1 200 OK"),
                "Expected 200 OK, got: {}",
                response_str
            );

            // No consensus data yet, body should be "syncing"
            assert!(
                response_str.ends_with("syncing"),
                "Expected body 'syncing', got: {}",
                response_str
            );

            Ok::<(), eyre::Error>(())
        })
        .await
        .expect("response received")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_tcp_healthcheck_behind() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();
        let consensus_bus = ConsensusBus::new();

        // Simulate: network at block 100, but no execution yet (block 0)
        consensus_bus.last_published_consensus_num_hash().send_replace((100, Default::default()));

        let port = get_available_tcp_port("127.0.0.1").expect("tcp port assigned by host");
        let addr =
            HealthcheckServer::spawn(task_spawner.clone(), port, consensus_bus.clone()).await?;

        tokio::time::sleep(Duration::from_millis(10)).await;

        tokio::time::timeout(Duration::from_millis(500), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"GET / HTTP/1.1\r\n\r\n").await?;

            let mut response = vec![0u8; 1024];
            let n = stream.read(&mut response).await?;
            response.truncate(n);
            let response_str = String::from_utf8_lossy(&response);

            assert!(
                response_str.starts_with("HTTP/1.1 200 OK"),
                "Expected 200 OK, got: {}",
                response_str
            );

            assert!(
                response_str.ends_with("behind 100"),
                "Expected body 'behind 100', got: {}",
                response_str
            );

            Ok::<(), eyre::Error>(())
        })
        .await
        .expect("response received")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_status_body() {
        let bus = ConsensusBus::new();

        // Initial state: no data
        assert_eq!(HealthcheckServer::sync_status_body(&bus), "syncing");

        // Network at 50, executed at 0
        bus.last_published_consensus_num_hash().send_replace((50, Default::default()));
        assert_eq!(HealthcheckServer::sync_status_body(&bus), "behind 50");
    }
}
