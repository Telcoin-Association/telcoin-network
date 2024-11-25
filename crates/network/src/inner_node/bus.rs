//! Local implementation for single process, inner-node network.

use consensus_metrics::metered_channel::MeteredMpscChannel;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct InnerNodeNetworkBus {
    /// Inner type for inner-node network.
    inner: Arc<InnerNodeNetworkBus>,
}

/// The local network for inner-node messaging.
pub struct InnerNodeNetworkBusInner {
    /// Primary to worker.
    primary_to_worker: MeteredMpscChannel<()>,
    /// Primary to engine.
    primary_to_engine: MeteredMpscChannel<()>,
    /// Worker to primary.
    worker_to_primary: MeteredMpscChannel<()>,
    /// Engine to primary.
    engine_to_primary: MeteredMpscChannel<()>,
}

impl Default for InnerNodeNetworkBus {
    fn default() -> Self {
        Self::new()
    }
}

impl InnerNodeNetworkBus {
    /// Create a new instance of Self.
    pub fn new() -> Self {
        todo!()
    }
}
