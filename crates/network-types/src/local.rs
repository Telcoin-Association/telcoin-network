//! Client implementation for local network messages between primary and worker.
use crate::{
    FetchBatchResponse, PrimaryToWorkerClient, WorkerOthersBatchMessage, WorkerOwnBatchMessage,
    WorkerSynchronizeMessage, WorkerToPrimaryClient,
};
use libp2p::PeerId;
use parking_lot::RwLock;
use std::{collections::HashSet, sync::Arc};
use tn_types::{network_public_key_to_libp2p, BlockHash, NetworkKeypair, NetworkPublicKey};

/// LocalNetwork provides the interface to send requests to other nodes, and call other components
/// directly if they live in the same process. It is used by both primary and worker(s).
///
/// Currently this only supports local direct calls, and it will be extended to support remote
/// network calls.
#[derive(Debug, Clone)]
pub struct LocalNetwork {
    inner: Arc<RwLock<Inner>>,
}

struct Inner {
    /// The primary's peer id.
    primary_peer_id: PeerId,
    /// The type that holds logic for worker to primary requests.
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimaryClient>>,
    /// The type that holds logic for primary to worker requests.
    primary_to_worker_handler: Option<Arc<dyn PrimaryToWorkerClient>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalNetwork::Inner for {}", self.primary_peer_id)
    }
}

impl LocalNetwork {
    /// Create a new instance of [Self].
    pub fn new(primary_peer_id: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_peer_id,
                worker_to_primary_handler: None,
                primary_to_worker_handler: None,
            })),
        }
    }

    /// Create a new instance of [Self] using a primary network keypair.
    pub fn new_from_keypair(primary_network_keypair: &NetworkKeypair) -> Self {
        Self::new(primary_network_keypair.public().to_peer_id())
    }

    /// Create a new [LocalNetwork] from the primary's network key.
    pub fn new_from_public_key(primary_network_public_key: &NetworkPublicKey) -> Self {
        Self::new(network_public_key_to_libp2p(primary_network_public_key))
    }

    /// Create a new instance of [Self] with a randomly generated ed25519 key.
    pub fn new_with_empty_id() -> Self {
        Self::new(network_public_key_to_libp2p(&NetworkKeypair::generate_ed25519().public().into()))
    }

    /// Set the handler for worker to primary messages.
    pub fn set_worker_to_primary_local_handler(&self, handler: Arc<dyn WorkerToPrimaryClient>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    /// Set the handler for primary to worker messages.
    pub fn set_primary_to_worker_local_handler(&self, handler: Arc<dyn PrimaryToWorkerClient>) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler = Some(handler);
    }

    /// Get the handler for worker to primary messages.
    async fn get_primary_to_worker_handler(&self) -> Option<Arc<dyn PrimaryToWorkerClient>> {
        let inner = self.inner.read();
        inner.primary_to_worker_handler.clone()
    }

    /// Get the handler for primary to worker messages.
    async fn get_worker_to_primary_handler(&self) -> Option<Arc<dyn WorkerToPrimaryClient>> {
        let inner = self.inner.read();
        inner.worker_to_primary_handler.clone()
    }
}

#[async_trait::async_trait]
impl PrimaryToWorkerClient for LocalNetwork {
    async fn synchronize(&self, request: WorkerSynchronizeMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_primary_to_worker_handler().await {
            c.synchronize(request).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }

    async fn fetch_batches(&self, digests: HashSet<BlockHash>) -> eyre::Result<FetchBatchResponse> {
        if let Some(c) = self.get_primary_to_worker_handler().await {
            c.fetch_batches(digests).await
        } else {
            tracing::warn!(target = "local_network", "primary to worker handler not set yet!");
            Err(eyre::eyre!("primary to worker not set yet"))
        }
    }
}

#[async_trait::async_trait]
impl WorkerToPrimaryClient for LocalNetwork {
    async fn report_own_batch(&self, request: WorkerOwnBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_own_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_others_batch(request).await?;
        } else {
            tracing::warn!(target = "local_network", "working to primary handler not set yet!");
        }
        Ok(())
    }
}
