//! Client implementation for local network messages between primary and worker.
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    error::LocalClientError,
    traits::{PrimaryToWorkerClient, WorkerToPrimaryClient},
};
use anemo::{Network, PeerId, Request};
use consensus_network_types::{
    FetchBlocksRequest, FetchBlocksResponse, PrimaryToWorker, WorkerOthersBlockMessage,
    WorkerOwnBlockMessage, WorkerSynchronizeMessage, WorkerToPrimary,
};
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tn_types::{traits::KeyPair, NetworkKeypair, NetworkPublicKey};
use tn_utils::sync::notify_once::NotifyOnce;
use tokio::{select, time::sleep};
use tracing::error;

// // //
//
// TODO: replace the `LocalNetwork` with worker/primary implementations and add engine.
// and get rid of this stupid loop crap with retry attempts. the only reason this is here
// is because LocalNetwork does too much. code doesn't know if/when primary/worker start
// so there's options and confusing logic. Just create the clients with the config on node startup.
//
// This is not so simple.
//
// // //

/// LocalNetwork provides the interface to send requests to other nodes, and call other components
/// directly if they live in the same process. It is used by both primary and worker(s).
///
/// Currently this only supports local direct calls, and it will be extended to support remote
/// network calls.
///
/// TODO: investigate splitting this into Primary and Worker specific clients.
#[derive(Debug, Clone)]
pub struct LocalNetwork {
    inner: Arc<RwLock<Inner>>,
    shutdown_notify: Arc<NotifyOnce>,
}

struct Inner {
    /// The primary's peer id.
    primary_peer_id: PeerId,
    /// The type that holds logic for worker to primary requests.
    worker_to_primary_handler: Option<Arc<dyn WorkerToPrimary>>,
    /// The type that holds logic for primary to worker requests.
    primary_to_worker_handler: BTreeMap<PeerId, Arc<dyn PrimaryToWorker>>,
    /// Shutdown status.
    shutdown: bool,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LocalNetwork::Inner for {}", self.primary_peer_id)
    }
}

impl LocalNetwork {
    const GET_CLIENT_RETRIES: usize = 50;
    const GET_CLIENT_INTERVAL: Duration = Duration::from_millis(300);

    pub fn new(primary_peer_id: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                primary_peer_id,
                worker_to_primary_handler: None,
                primary_to_worker_handler: BTreeMap::new(),
                shutdown: false,
            })),
            shutdown_notify: Arc::new(NotifyOnce::new()),
        }
    }

    pub fn new_from_keypair(primary_network_keypair: &NetworkKeypair) -> Self {
        Self::new(PeerId(primary_network_keypair.public().0.into()))
    }

    /// Create a new [LocalNetwork] from the primary's network key.
    pub fn new_from_public_key(primary_network_public_key: &NetworkPublicKey) -> Self {
        Self::new(PeerId(primary_network_public_key.0.into()))
    }

    pub fn new_with_empty_id() -> Self {
        // ED25519_PUBLIC_KEY_LENGTH is 32 bytes.
        Self::new(PeerId([0u8; 32]))
    }

    pub fn set_worker_to_primary_local_handler(&self, handler: Arc<dyn WorkerToPrimary>) {
        let mut inner = self.inner.write();
        inner.worker_to_primary_handler = Some(handler);
    }

    pub fn set_primary_to_worker_local_handler(
        &self,
        worker_id: PeerId,
        handler: Arc<dyn PrimaryToWorker>,
    ) {
        let mut inner = self.inner.write();
        inner.primary_to_worker_handler.insert(worker_id, handler);
    }

    pub fn shutdown(&self) {
        let mut inner = self.inner.write();
        if inner.shutdown {
            return;
        }
        inner.worker_to_primary_handler = None;
        inner.primary_to_worker_handler = BTreeMap::new();
        inner.shutdown = true;
        let _ = self.shutdown_notify.notify();
    }

    async fn get_primary_to_worker_handler(
        &self,
        peer_id: PeerId,
    ) -> Result<Arc<dyn PrimaryToWorker>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown);
                }
                if let Some(handler) = inner.primary_to_worker_handler.get(&peer_id) {
                    return Ok(handler.clone());
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::WorkerNotStarted(peer_id))
    }

    async fn get_worker_to_primary_handler(
        &self,
    ) -> Result<Arc<dyn WorkerToPrimary>, LocalClientError> {
        for _ in 0..Self::GET_CLIENT_RETRIES {
            {
                let inner = self.inner.read();
                if inner.shutdown {
                    return Err(LocalClientError::ShuttingDown);
                }
                if let Some(handler) = &inner.worker_to_primary_handler {
                    return Ok(handler.clone());
                }
            }
            sleep(Self::GET_CLIENT_INTERVAL).await;
        }
        Err(LocalClientError::PrimaryNotStarted(self.inner.read().primary_peer_id))
    }
}

// TODO: extract common logic for cancelling on shutdown.

impl PrimaryToWorkerClient for LocalNetwork {
    async fn synchronize(
        &self,
        worker_name: NetworkPublicKey,
        request: WorkerSynchronizeMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_primary_to_worker_handler(PeerId(worker_name.0.into())).await?;
        select! {
            resp = c.synchronize(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn fetch_blocks(
        &self,
        worker_name: NetworkPublicKey,
        request: FetchBlocksRequest,
    ) -> Result<FetchBlocksResponse, LocalClientError> {
        let c = self.get_primary_to_worker_handler(PeerId(worker_name.0.into())).await?;
        select! {
            resp = c.fetch_blocks(Request::new(request)) => {
                Ok(resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?.into_inner())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}

impl WorkerToPrimaryClient for LocalNetwork {
    async fn report_own_block(
        &self,
        request: WorkerOwnBlockMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_primary_handler().await?;
        select! {
            resp = c.report_own_block(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }

    async fn report_others_block(
        &self,
        request: WorkerOthersBlockMessage,
    ) -> Result<(), LocalClientError> {
        let c = self.get_worker_to_primary_handler().await?;
        select! {
            resp = c.report_others_block(Request::new(request)) => {
                resp.map_err(|e| LocalClientError::Internal(format!("{e:?}")))?;
                Ok(())
            },
            () = self.shutdown_notify.wait() => {
                Err(LocalClientError::ShuttingDown)
            },
        }
    }
}
