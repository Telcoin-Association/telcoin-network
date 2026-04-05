// SPDX-License-Identifier: MIT OR Apache-2.0
//! Context provided to ExEx tasks at startup.

use crate::{TnExExEvent, TnExExNotification};
use reth_provider::providers::BlockchainProvider;
use tn_config::Config;
use tn_reth::traits::TelcoinNode;
use tn_types::{BlockNumHash, TaskSpawner};
use tokio::sync::mpsc;

/// Context provided to an ExEx task when it is launched.
///
/// This context gives the ExEx everything it needs to:
/// - Receive notifications about new blocks via `notifications`
/// - Report progress back to the manager via `events`
/// - Access chain state and historical data via `provider`
/// - Spawn background tasks via `task_executor`
/// - Access node configuration via `config`
///
/// # Lifetime
///
/// The context is provided once at ExEx startup and remains valid for the lifetime
/// of the ExEx task. The notification receiver will be closed when the node shuts down
/// or when the ExEx manager is dropped.
#[derive(Debug)]
pub struct TnExExContext {
    /// The current head of the chain when this ExEx was started.
    ///
    /// ExExes should begin processing from this point forward. Historical backfill
    /// is not currently supported but could be added in a future version.
    pub head: BlockNumHash,

    /// The node's configuration.
    ///
    /// This provides access to network parameters, genesis configuration, and other
    /// node-wide settings that the ExEx might need.
    pub config: Config,

    /// Channel for sending events back to the ExEx manager.
    ///
    /// Use this to report progress via `TnExExEvent::FinishedHeight` notifications.
    /// The manager uses these events for backpressure management.
    pub events: mpsc::UnboundedSender<TnExExEvent>,

    /// Channel for receiving notifications about committed blocks.
    ///
    /// The ExEx should continuously read from this receiver in its main loop.
    /// When the receiver is closed, the ExEx should gracefully shut down.
    pub notifications: mpsc::Receiver<TnExExNotification>,

    /// Provider for accessing blockchain data.
    ///
    /// This gives read-only access to:
    /// - Block headers, bodies, and receipts
    /// - Account state and storage
    /// - Transaction data
    /// - Chain metadata
    ///
    /// The provider is backed by the node's database and is safe to use concurrently.
    pub provider: BlockchainProvider<TelcoinNode>,

    /// Task executor for spawning background tasks.
    ///
    /// Use this to spawn long-running background work that should be tracked by
    /// the node's task manager. Tasks spawned via this executor will be properly
    /// shut down when the node exits.
    pub task_executor: TaskSpawner,
}

impl TnExExContext {
    /// Creates a new ExEx context.
    ///
    /// This is typically called by the ExEx launcher during node startup.
    ///
    /// # Arguments
    ///
    /// * `head` - Current chain head
    /// * `config` - Node configuration
    /// * `events` - Channel for sending events to the manager
    /// * `notifications` - Channel for receiving block notifications
    /// * `provider` - Blockchain data provider
    /// * `task_executor` - Task spawner for background work
    pub fn new(
        head: BlockNumHash,
        config: Config,
        events: mpsc::UnboundedSender<TnExExEvent>,
        notifications: mpsc::Receiver<TnExExNotification>,
        provider: BlockchainProvider<TelcoinNode>,
        task_executor: TaskSpawner,
    ) -> Self {
        Self { head, config, events, notifications, provider, task_executor }
    }
}

// Safety: TnExExContext contains channels and a provider that are all Send + Sync
// The Send bound is required so contexts can be passed across thread boundaries
// The Sync bound is not strictly required but is good practice
#[allow(dead_code)]
fn assert_context_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<TnExExContext>();
    assert_sync::<TnExExContext>();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_is_send_and_sync() {
        // This test ensures TnExExContext satisfies the Send + Sync bounds
        // required for passing contexts to ExEx tasks across threads
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<TnExExContext>();
        assert_sync::<TnExExContext>();
    }
}
