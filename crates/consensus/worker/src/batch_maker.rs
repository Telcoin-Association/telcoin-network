// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
//! The receiving side of the execution layer's `BatchMaker`.
//!
//! Consensus `BatchMaker` takes a batch from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.
use crate::metrics::WorkerMetrics;
use consensus_metrics::{
    metered_channel::{Receiver, Sender},
    monitored_scope, spawn_logged_monitored_task,
};
use fastcrypto::hash::Hash;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use narwhal_network::{client::NetworkClient, WorkerToPrimaryClient};
use narwhal_typed_store::{rocks::DBMap, Map};
use std::sync::Arc;
use tn_types::{NewBatch, WorkerId};

use narwhal_network_types::WorkerOwnBatchMessage;
use tn_types::{
    error::DagError, now, Batch, BatchAPI, BatchDigest, ConditionalBroadcastReceiver, MetadataAPI,
};
use tokio::{
    task::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{error, warn};

#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};

// The number of batches to store / transmit in parallel.
pub const MAX_PARALLEL_BATCH: usize = 100;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Process batches from EL into sealed batches for CL.
pub struct BatchMaker {
    /// Our worker's id.
    id: WorkerId,
    /// TODO: remove this
    ///
    /// The preferred batch size (in bytes).
    _batch_size_limit: usize,
    /// TODO: remove this
    ///
    /// The maximum delay after which to seal the batch.
    _max_batch_delay: Duration,
    /// Receiver for shutdown.
    rx_shutdown: ConditionalBroadcastReceiver,
    /// Channel to receive transactions from the network.
    rx_batch_maker: Receiver<NewBatch>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_quorum_waiter: Sender<(Batch, tokio::sync::oneshot::Sender<()>)>,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The timestamp of the batch creation.
    /// Average resident time in the batch would be ~ (batch seal time - creation time) / 2
    batch_start_timestamp: Instant,
    /// The network client to send our batches to the primary.
    client: NetworkClient,
    /// The batch store to store our own batches.
    store: DBMap<BatchDigest, Batch>,
}

impl BatchMaker {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn spawn(
        id: WorkerId,
        _batch_size_limit: usize,
        _max_batch_delay: Duration,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_batch_maker: Receiver<NewBatch>,
        tx_quorum_waiter: Sender<(Batch, tokio::sync::oneshot::Sender<()>)>,
        node_metrics: Arc<WorkerMetrics>,
        client: NetworkClient,
        store: DBMap<BatchDigest, Batch>,
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                Self {
                    id,
                    _batch_size_limit,
                    _max_batch_delay,
                    rx_shutdown,
                    rx_batch_maker,
                    tx_quorum_waiter,
                    batch_start_timestamp: Instant::now(),
                    node_metrics,
                    client,
                    store,
                }
                .run()
                .await;
            },
            "BatchMakerTask"
        )
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        // collection of future sealed batches
        let mut batch_pipeline = FuturesUnordered::new();

        loop {
            tokio::select! {
                // Wait for the next batch from the EL.
                //
                // Note that transactions are only consumed when the number of batches
                // 'in-flight' are below a certain number (MAX_PARALLEL_BATCH). This
                // condition will be met eventually if the store and network are functioning.
                Some(new_batch) = self.rx_batch_maker.recv(), if batch_pipeline.len() < MAX_PARALLEL_BATCH => {
                    let _scope = monitored_scope("BatchMaker::recv");
                    // try seal the batch
                    if let Some(seal) = self.seal(new_batch).await {
                        batch_pipeline.push(seal);
                    }

                    self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);
                    self.batch_start_timestamp = Instant::now();

                    // Yield once per size threshold to allow other tasks to run.
                    tokio::task::yield_now().await;
                },

                _ = self.rx_shutdown.receiver.recv() => {
                    return
                }

                // Process the pipeline of batches, this consumes items in the `batch_pipeline`
                // list, and ensures the main loop in run will always be able to make progress
                // by lowering it until condition batch_pipeline.len() < MAX_PARALLEL_BATCH is met.
                _ = batch_pipeline.next(), if !batch_pipeline.is_empty() => {
                    self.node_metrics.parallel_worker_batches.set(batch_pipeline.len() as i64);
                }

            }
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal<'a>(
        &self,
        // timeout: bool,
        new_batch: NewBatch,
        // size: usize,
        // response: BatchResponse,
    ) -> Option<BoxFuture<'a, ()>> {
        #[cfg(feature = "benchmark")]
        {
            let digest = new_batch.batch.digest();

            // Look for sample txs (they all start with 0) and gather their txs id (the next 8
            // bytes).
            let tx_ids: Vec<_> = new_batch
                .batch
                .transactions()
                .iter()
                .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
                .filter_map(|tx| tx[1..9].try_into().ok())
                .collect();

            let size = tx_ids.len();

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                tracing::info!("Batch {:?} contains sample tx {}", digest, u64::from_be_bytes(id));
            }

            #[cfg(feature = "trace_transaction")]
            {
                // The first 8 bytes of each transaction message is reserved for an identifier
                // that's useful for debugging and tracking the lifetime of messages between
                // Narwhal and clients.
                let tracking_ids: Vec<_> = new_batch
                    .batch
                    .transactions()
                    .iter()
                    .map(|tx| {
                        let len = tx.len();
                        if len >= 8 {
                            (&tx[0..8]).read_u64::<BigEndian>().unwrap_or_default()
                        } else {
                            0
                        }
                    })
                    .collect();
                tracing::debug!(
                    "Tracking IDs of transactions in the Batch {:?}: {:?}",
                    digest,
                    tracking_ids
                );
            }

            // NOTE: This log entry is used to compute performance.
            tracing::info!("Batch {:?} contains {} B", digest, size);
        }

        let NewBatch { mut batch, ack } = new_batch;
        let size = batch.size();

        // TODO: include timeout vs size_reached in `NewBatch`
        // let reason = if timeout { "timeout" } else { "size_reached" };
        let reason = "timeout";

        self.node_metrics.created_batch_size.with_label_values(&[reason]).observe(size as f64);

        // Send the batch through the deliver channel for further processing.
        let (notify_done, broadcasted_to_quorum) = tokio::sync::oneshot::channel();
        if self.tx_quorum_waiter.send((batch.clone(), notify_done)).await.is_err() {
            tracing::debug!("{}", DagError::ShuttingDown);
            return None;
        }

        let batch_creation_duration = self.batch_start_timestamp.elapsed().as_secs_f64();

        tracing::debug!(
            "Batch {:?} took {} seconds to create due to {}",
            batch.digest(),
            batch_creation_duration,
            reason
        );

        // we are deliberately measuring this after the sending to the downstream
        // channel tx_quorum_waiter as the operation is blocking and affects any further
        // batch creation.
        self.node_metrics
            .created_batch_latency
            .with_label_values(&[reason])
            .observe(batch_creation_duration);

        // Clone things to not capture self
        let client = self.client.clone();
        let store = self.store.clone();
        let worker_id = self.id;

        // The batch has been sealed so we can officially set its creation time
        // for latency calculations.
        batch.versioned_metadata_mut().set_created_at(now());
        let metadata = batch.versioned_metadata().clone();

        Some(Box::pin(async move {
            // Also wait quorum broadcast here.
            //
            // Error can only happen when the worker is shutting down.
            // All other errors, e.g. timeouts, failure responses from individual peers,
            // are retried indefinitely underneath.
            if broadcasted_to_quorum.await.is_err() {
                // Drop all response handlers to signal error.
                return;
            }

            // Now save it to disk
            let digest = batch.digest();

            if let Err(e) = store.insert(&digest, &batch) {
                error!("Store failed with error: {:?}", e);
                return;
            }

            // Send the batch to the primary.
            let message = WorkerOwnBatchMessage { digest, worker_id, metadata };
            if let Err(e) = client.report_own_batch(message).await {
                warn!("Failed to report our batch: {}", e);
                // Drop all response handlers to signal error, since we
                // cannot ensure the primary has actually signaled the
                // batch will eventually be sent.
                // The transaction submitter will see the error and retry.
                return;
            }

            // We now signal back to the execution layer's batch maker
            // that the batch is sealed.
            let _ = ack.send(digest);
        }))
    }
}
