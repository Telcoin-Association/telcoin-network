//! Client implementation for local network messages between primary and worker.
use crate::{
    PrimaryToWorkerClient, WorkerOthersBatchMessage, WorkerOwnBatchMessage,
    WorkerSynchronizeMessage, WorkerToPrimaryClient,
};
use parking_lot::RwLock;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};
use tn_types::{Batch, BlockHash, BlsPublicKey};

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
    /// The primary's BLS public key.
    primary_bls_key: BlsPublicKey,
    /// The type that holds logic for worker to primary requests.
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimaryClient>>,
    /// The type that holds logic for primary to worker requests.
    primary_to_worker_handler: Option<Arc<dyn PrimaryToWorkerClient>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalNetwork::Inner for {}", self.primary_bls_key)
    }
}

impl LocalNetwork {
    /// Create a new instance of [Self].
    pub fn new(primary_bls_key: BlsPublicKey) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_bls_key,
                worker_to_primary_handler: None,
                primary_to_worker_handler: None,
            })),
        }
    }

    /// Create a new instance of [Self] with a randomly generated ed25519 key.
    pub fn new_with_empty_id() -> Self {
        Self::new(BlsPublicKey::default())
    }

    /// Set the handler for worker to primary messages.
    ///
    /// A handler binds exactly once: a second registration is a wiring bug (two components
    /// claiming the same seam would otherwise alias silently, last writer winning), so it
    /// returns an error instead of overwriting.
    pub fn set_worker_to_primary_local_handler(
        &self,
        handler: Arc<dyn WorkerToPrimaryClient>,
    ) -> eyre::Result<()> {
        let mut inner = self.inner.write();
        if inner.worker_to_primary_handler.is_some() {
            Err(eyre::eyre!("worker to primary handler already set"))
        } else {
            inner.worker_to_primary_handler = Some(handler);
            Ok(())
        }
    }

    /// Set the handler for primary to worker messages.
    ///
    /// A handler binds exactly once: a second registration is a wiring bug (two components
    /// claiming the same seam would otherwise alias silently, last writer winning), so it
    /// returns an error instead of overwriting.
    pub fn set_primary_to_worker_local_handler(
        &self,
        handler: Arc<dyn PrimaryToWorkerClient>,
    ) -> eyre::Result<()> {
        let mut inner = self.inner.write();
        if inner.primary_to_worker_handler.is_some() {
            Err(eyre::eyre!("primary to worker handler already set"))
        } else {
            inner.primary_to_worker_handler = Some(handler);
            Ok(())
        }
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

    async fn fetch_batches(
        &self,
        digests: BTreeSet<BlockHash>,
    ) -> eyre::Result<HashMap<BlockHash, Batch>> {
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
            c.report_own_batch(request).await
        } else {
            // A missing handler must surface as an error, symmetric with `synchronize` /
            // `fetch_batches` below: swallowing it would let `Worker::seal` report success
            // while the digest never reached the proposer, and the batch builder would evict
            // the batch's transactions from the pool as mined.
            tracing::warn!(target = "local_network", "worker to primary handler not set yet!");
            Err(eyre::eyre!("worker to primary handler not set yet"))
        }
    }

    async fn report_others_batch(&self, request: WorkerOthersBatchMessage) -> eyre::Result<()> {
        if let Some(c) = self.get_worker_to_primary_handler().await {
            c.report_others_batch(request).await
        } else {
            // See `report_own_batch`: a missing handler is an error, never a silent drop.
            tracing::warn!(target = "local_network", "worker to primary handler not set yet!");
            Err(eyre::eyre!("worker to primary handler not set yet"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockPrimaryToWorkerClient, MockWorkerToPrimary};

    #[tokio::test]
    async fn report_paths_surface_a_missing_handler() {
        let local = LocalNetwork::new_with_empty_id();

        // no handler: both report paths error instead of swallowing the message
        let own = WorkerOwnBatchMessage::new(0, BlockHash::default());
        assert!(local.report_own_batch(own.clone()).await.is_err());
        let others = WorkerOthersBatchMessage::new(BlockHash::default(), 0);
        assert!(local.report_others_batch(others.clone()).await.is_err());

        // registered handler: both paths succeed
        local
            .set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary()))
            .expect("first registration");
        assert!(local.report_own_batch(own).await.is_ok());
        assert!(local.report_others_batch(others).await.is_ok());
    }

    #[tokio::test]
    async fn handlers_bind_exactly_once() {
        let local = LocalNetwork::new_with_empty_id();

        local
            .set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary()))
            .expect("first registration");
        assert!(
            local.set_worker_to_primary_local_handler(Arc::new(MockWorkerToPrimary())).is_err(),
            "a second registration is a wiring bug"
        );

        local
            .set_primary_to_worker_local_handler(Arc::new(MockPrimaryToWorkerClient::default()))
            .expect("first registration");
        assert!(
            local
                .set_primary_to_worker_local_handler(Arc::new(MockPrimaryToWorkerClient::default()))
                .is_err(),
            "a second registration is a wiring bug"
        );
    }
}
