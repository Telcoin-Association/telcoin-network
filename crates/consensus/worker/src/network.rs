//! The network interface for WAN worker communication and LAN communication with this worker's
//! primary.

use crate::batch_fetcher::BatchFetcher;
use anemo::{types::response::StatusCode, Network};
use async_trait::async_trait;
use eyre::Result;
use itertools::Itertools;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tn_network::{local::LocalNetwork, WorkerToPrimaryClient as _};
use tn_network_types::{
    BatchMessage, FetchBatchResponse, FetchBatchesRequest, PrimaryToWorker, RequestBatchesRequest,
    RequestBatchesResponse, WorkerOthersBatchMessage, WorkerSynchronizeMessage, WorkerToWorker,
    WorkerToWorkerClient,
};
use tn_storage::{
    tables::Batches,
    traits::{Database, DbTxMut},
};
use tn_types::{now, BatchValidation, Committee, SealedBatch, WorkerCache, WorkerId};
use tracing::{debug, trace};

#[cfg(test)]
#[path = "tests/handlers_tests.rs"]
pub mod handlers_tests;

/// Defines how the network receiver handles incoming workers messages.
#[derive(Clone)]
pub struct WorkerReceiverHandler<DB> {
    /// This worker's id.
    pub id: WorkerId,
    /// The interface for communicating with this worker's primary.
    pub client: LocalNetwork,
    /// Database for storing batches received from peers.
    pub store: DB,
    /// The type that validates batches received from peers.
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait]
impl<DB: Database> WorkerToWorker for WorkerReceiverHandler<DB> {
    async fn report_batch(
        &self,
        request: anemo::Request<BatchMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        // own peer id for error handling
        let peer_id = request.peer_id().copied();
        let message = request.into_body();
        let BatchMessage { sealed_batch } = message;
        // validate batch - log error if invalid
        if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                format!(
                    "Invalid batch from peer {:?}: {err}\nsealed_\nbad batch:\n{:?}",
                    peer_id, sealed_batch,
                ),
            ));
        }

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        self.store.insert::<Batches>(&digest, &batch).map_err(|e| {
            anemo::rpc::Status::internal(format!("failed to write to batch store: {e:?}"))
        })?;

        // notify primary for payload store
        self.client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| anemo::rpc::Status::internal(e.to_string()))?;

        Ok(anemo::Response::new(()))
    }

    async fn request_batches(
        &self,
        request: anemo::Request<RequestBatchesRequest>,
    ) -> Result<anemo::Response<RequestBatchesResponse>, anemo::rpc::Status> {
        const MAX_REQUEST_BATCHES_RESPONSE_SIZE: usize = 6_000_000;
        const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

        let digests_to_fetch = request.into_body().batch_digests;
        let digests_chunks = digests_to_fetch
            .chunks(BATCH_DIGESTS_READ_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect_vec();
        let mut batches = Vec::new();
        let mut total_size = 0;
        let mut is_size_limit_reached = false;

        for digests_chunks in digests_chunks {
            let stored_batches =
                self.store.multi_get::<Batches>(digests_chunks.iter()).map_err(|e| {
                    anemo::rpc::Status::internal(format!("failed to read from batch store: {e:?}"))
                })?;

            for stored_batch in stored_batches.into_iter().flatten() {
                let batch_size = stored_batch.size();
                if total_size + batch_size <= MAX_REQUEST_BATCHES_RESPONSE_SIZE {
                    batches.push(stored_batch);
                    total_size += batch_size;
                } else {
                    is_size_limit_reached = true;
                    break;
                }
            }
        }

        Ok(anemo::Response::new(RequestBatchesResponse { batches, is_size_limit_reached }))
    }
}

/// Defines how the network receiver handles incoming primary messages.
pub struct PrimaryReceiverHandler<DB> {
    /// The id of this worker.
    pub id: WorkerId,
    /// The committee information.
    pub committee: Committee,
    /// The worker information cache.
    pub worker_cache: WorkerCache,
    /// The batch store
    pub store: DB,
    /// Timeout on RequestBatches RPC.
    pub request_batches_timeout: Duration,
    /// Synchronize header payloads from other workers.
    pub network: Option<Network>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher<DB>>,
    /// Validate incoming batches
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait]
impl<DB: Database> PrimaryToWorker for PrimaryReceiverHandler<DB> {
    async fn synchronize(
        &self,
        request: anemo::Request<WorkerSynchronizeMessage>,
    ) -> Result<anemo::Response<()>, anemo::rpc::Status> {
        let Some(network) = self.network.as_ref() else {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead",
            ));
        };
        let message = request.body();
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get::<Batches>(digest) {
                Ok(None) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(Some(_)) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(anemo::rpc::Status::internal(format!(
                        "failed to read from batch store: {e:?}"
                    )));
                }
            };
        }
        if missing.is_empty() {
            return Ok(anemo::Response::new(()));
        }

        let worker_name = match self
            .worker_cache
            .worker(self.committee.authority(&message.target).unwrap().protocol_key(), &self.id)
        {
            Ok(worker_info) => worker_info.name,
            Err(e) => {
                return Err(anemo::rpc::Status::internal(format!(
                    "The primary asked worker to sync with an unknown node: {e}"
                )));
            }
        };
        let Some(peer) = network.peer(anemo::PeerId(worker_name.0.to_bytes())) else {
            return Err(anemo::rpc::Status::internal(format!(
                "Not connected with worker peer {worker_name}"
            )));
        };
        let mut client = WorkerToWorkerClient::new(peer.clone());

        // Attempt to retrieve missing batches.
        // Retried at a higher level in Synchronizer::sync_batches_internal().
        let request = RequestBatchesRequest { batch_digests: missing.iter().cloned().collect() };
        debug!("Sending RequestBatchesRequest to {worker_name}: {request:?}");

        let response = client
            .request_batches(
                anemo::Request::new(request).with_timeout(self.request_batches_timeout),
            )
            .await?
            .into_inner();

        let sealed_batches_from_response: Vec<SealedBatch> = missing
            .iter()
            .cloned()
            .zip(response.batches)
            .map(|(digest, batch)| SealedBatch::new(batch, digest))
            .collect();

        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
                    return Err(anemo::rpc::Status::new_with_message(
                        StatusCode::BadRequest,
                        format!("Invalid batch: {err}"),
                    ));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());
                let mut tx = self.store.write_txn().map_err(|e| {
                    anemo::rpc::Status::internal(format!(
                        "failed to create batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.insert::<Batches>(&digest, &batch).map_err(|e| {
                    anemo::rpc::Status::internal(format!(
                        "failed to batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.commit().map_err(|e| {
                    anemo::rpc::Status::internal(format!("failed to commit batch: {e:?}"))
                })?;
            }
        }

        if missing.is_empty() {
            return Ok(anemo::Response::new(()));
        }
        Err(anemo::rpc::Status::internal("failed to synchronize batches!"))
    }

    async fn fetch_batches(
        &self,
        request: anemo::Request<FetchBatchesRequest>,
    ) -> Result<anemo::Response<FetchBatchResponse>, anemo::rpc::Status> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(anemo::rpc::Status::new_with_message(
                StatusCode::BadRequest,
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead",
            ));
        };
        let request = request.into_body();
        let batches = batch_fetcher.fetch(request.digests, request.known_workers).await;
        Ok(anemo::Response::new(FetchBatchResponse { batches }))
    }
}
