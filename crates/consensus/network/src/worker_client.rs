//! Client implementations for local network messages.
//!
//! The clients are written from the perspective of the client.
//!
//! A "PrimaryClient" is a client for primaries to use.
//!
//! A "WorkerClient" is a client for workers to use.
//!
//! NOTE: This implementation only supports one worker. The primary can have multiple
//! workers. This code is a temporary solution, so moving forward with one worker per primary.
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

/// The worker's client to send messages to the primary.
#[derive(Debug, Clone)]
pub struct WorkerClient<H> {
    inner: Arc<RwLock<WorkerClientInner<H>>>,
    shutdown_notify: Arc<NotifyOnce>,
}

/// The inner type for [WorkerClient].
///
/// TODO: this only supports one worker.
struct WorkerClientInner {
    // TODO: is this needed for worker client?
    primary_peer_id: PeerId,
    // worker_network: BTreeMap<u16, Network>,
    /// The handler for processing network messages.
    // TODO: better name
    handler: Arc<H>,
    // handler: BTreeMap<PeerId, Arc<H>>,
    shutdown: bool,
}

impl<H> std::fmt::Debug for WorkerClientInner<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "worker network client")
    }
}

// // //
//
// TODO: replace the `PrimaryClient` with worker/primary implementations and add engine.
// and get rid of this stupid loop crap with retry attempts. the only reason this is here
// is because PrimaryClient does too much. code doesn't know if/when primary/worker start
// so there's options and confusing logic. Just create the clients with the config on node startup.
//
// // //

impl<H> WorkerClient<H>
where
    H: PrimaryToWorker + 'static,
{
    /// Create a new instance of `Self`.
    pub fn new(primary_peer_id: PeerId, handler: Arc<H>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(WorkerClientInner {
                primary_peer_id,
                handler,
                shutdown: false,
            })),
            shutdown_notify: Arc::new(NotifyOnce::new()),
        }
    }

    /// Create a new [PrimaryClient] from the primary's network key.
    pub fn new_from_public_key(
        primary_network_public_key: &NetworkPublicKey,
        handler: Arc<H>,
    ) -> Self {
        Self::new(PeerId(primary_network_public_key.0.into()), handler)
    }

    /// Create a new [Self] using [0; 32] as the network public key.
    pub fn new_with_empty_id(handler: Arc<H>) -> Self {
        // ED25519_PUBLIC_KEY_LENGTH is 32 bytes.
        Self::new(PeerId([0u8; 32]), handler)
    }

    // get_client?
    // primary_client
    // client_for_primary
    // pub async fn wan_handle(&self) -> Result<Network, anemo::rpc::Status> {
    //     let inner = self.inner.read();
    //     if inner.shutdown {
    //         return Err(anemo::rpc::Status::internal("This node has shutdown"));
    //     }
    //     Ok(inner.wan_handle.clone())
    // }

    pub fn shutdown(&self) {
        let mut inner = self.inner.write();
        if inner.shutdown {
            return;
        }
        // inner.worker_to_primary_handler = None;
        inner.shutdown = true;
        let _ = self.shutdown_notify.notify();
    }
}

impl WorkerToPrimaryClient for WorkerClient {
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
