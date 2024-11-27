// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::metrics::WorkerMetrics;
use anemo::types::response::StatusCode;
use consensus_metrics::monitored_future;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tn_network::{CancelOnDropHandler, ReliableNetwork};
use tn_network_types::WorkerBlockMessage;
use tn_types::{Authority, Committee, SealedWorkerBlock, Stake, WorkerCache, WorkerId};
use tokio::task::JoinHandle;

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// Interface to QuorumWaiter, exists primarily for tests.
pub trait QuorumWaiterTrait: Send + Sync + Clone + Unpin + 'static {
    /// Send a block to committee peers in an attempt to get quorum on it's validity.
    ///
    /// Returns a JoinHandle to a future that will timeout.  Each peer attempt can:
    /// - Accept the block and it's stake to quorum
    /// - Reject the block explicitly in which case it's stake will never be added to quorum (can
    ///   cause total block rejection)
    /// - Have an error of some type stopping it's stake from adding to quorum but possibly not
    ///   forever
    ///
    /// If the future resolves to Ok then the block has reached quorum other wise examine the error.
    /// An error of QuorumWaiterError::QuorumRejected indicates the block will never be accepted
    /// otherwise it might be possible if the network improves.
    fn verify_block(
        &self,
        block: SealedWorkerBlock,
        timeout: Duration,
    ) -> JoinHandle<Result<(), QuorumWaiterError>>;
}

/// Basically BoxFuture but without the unneeded lifetime.
type QMBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

struct QuorumWaiterInner {
    /// This authority.
    authority: Authority,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The worker information cache.
    worker_cache: WorkerCache,
    /// A network sender to broadcast the batches to the other workers.
    network: anemo::Network,
    /// Record metrics for quorum waiter.
    metrics: Arc<WorkerMetrics>,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
#[derive(Clone)]
pub struct QuorumWaiter {
    inner: Arc<QuorumWaiterInner>,
}

impl QuorumWaiter {
    /// Create a new QuorumWaiter.
    pub fn new(
        authority: Authority,
        id: WorkerId,
        committee: Committee,
        worker_cache: WorkerCache,
        network: anemo::Network,
        metrics: Arc<WorkerMetrics>,
    ) -> Self {
        Self {
            inner: Arc::new(QuorumWaiterInner {
                authority,
                id,
                committee,
                worker_cache,
                network,
                metrics,
            }),
        }
    }

    /// Helper function. It waits for a future to complete and then delivers a value.
    async fn waiter(
        wait_for: CancelOnDropHandler<eyre::Result<anemo::Response<()>>>,
        deliver: Stake,
    ) -> Result<Stake, WaiterError> {
        match wait_for.await {
            Ok(r) => {
                let status = r.status();
                match status {
                    StatusCode::Success => Ok(deliver),
                    StatusCode::BadRequest => Err(WaiterError::Rejected(deliver)),
                    // Non-exhaustive enum...
                    _ => Err(WaiterError::Rpc(status, deliver)),
                }
            }
            Err(_) => Err(WaiterError::Network(deliver)),
        }
    }
}

impl QuorumWaiterTrait for QuorumWaiter {
    fn verify_block(
        &self,
        sealed_worker_block: SealedWorkerBlock,
        timeout: Duration,
    ) -> JoinHandle<Result<(), QuorumWaiterError>> {
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let timeout_res = tokio::time::timeout(timeout, async move {
                let start_time = Instant::now();
                // Broadcast the batch to the other workers.
                let workers: Vec<_> = inner
                    .worker_cache
                    .others_workers_by_id(inner.authority.protocol_key(), &inner.id)
                    .into_iter()
                    .map(|(name, info)| (name, info.name))
                    .collect();
                let (primary_names, worker_names): (Vec<_>, _) = workers.into_iter().unzip();
                let message = WorkerBlockMessage { sealed_worker_block };
                let handlers = inner.network.broadcast(worker_names, &message);
                let _timer = inner.metrics.block_broadcast_quorum_latency.start_timer();

                // Collect all the handlers to receive acknowledgements.
                let mut wait_for_quorum: FuturesUnordered<QMBoxFuture<Result<Stake, WaiterError>>> =
                    FuturesUnordered::new();
                // Total stake available for the entire committee.
                // Can use this to determine anti-quorum more quickly.
                let mut available_stake = 0;
                // Stake from a committee member that has rejected this block.
                let mut rejected_stake = 0;
                primary_names
                    .into_iter()
                    .zip(handlers.into_iter())
                    .map(|(name, handler)| {
                        let stake = inner.committee.stake(&name);
                        available_stake += stake;
                        Box::pin(monitored_future!(Self::waiter(handler, stake)))
                    })
                    .for_each(|f| wait_for_quorum.push(f));

                // Wait for the first 2f nodes to send back an Ack. Then we consider the block
                // delivered and we send its digest to the primary (that will include it into
                // the dag). This should reduce the amount of syncing.
                let threshold = inner.committee.quorum_threshold();
                let mut total_stake = inner.authority.stake();
                // If more stake than this is rejected then the block will never be accepted.
                let max_rejected_stake = available_stake - threshold;

                // Wait on the peer responses and produce an Ok(()) for quorum (2/3 stake confirmed
                // block) or Error if quorum not reached.
                loop {
                    if let Some(res) = wait_for_quorum.next().await {
                        match res {
                            Ok(stake) => {
                                total_stake += stake;
                                if total_stake >= threshold {
                                    let remaining_time =
                                        start_time.elapsed().saturating_sub(timeout);
                                    if !wait_for_quorum.is_empty() && !remaining_time.is_zero() {
                                        // Let the remaining waiters have a chance for the remaining
                                        // time.
                                        // These are fire and forget, they will timeout soon so no
                                        // big deal.
                                        tokio::spawn(async move {
                                            let _ =
                                                tokio::time::timeout(remaining_time, async move {
                                                    while (wait_for_quorum.next().await).is_some() {
                                                        // do nothing
                                                    }
                                                })
                                                .await;
                                        });
                                    }
                                    break Ok(());
                                }
                            }
                            Err(WaiterError::Rejected(stake)) => {
                                rejected_stake -= stake;
                                available_stake -= stake;
                            }
                            Err(WaiterError::Network(stake)) => {
                                available_stake -= stake;
                            }
                            Err(WaiterError::Rpc(_, stake)) => {
                                available_stake -= stake;
                            }
                        }
                    } else {
                        // Ran out of Peers and did not reach quorum...
                        break Err(QuorumWaiterError::AntiQuorum);
                    }
                    if rejected_stake > max_rejected_stake {
                        // Can no longer reach quorum because our block was explicitly rejected by
                        // to much stack.
                        break Err(QuorumWaiterError::QuorumRejected);
                    }
                    if total_stake + available_stake < threshold {
                        // It is no longer possible to reach quorum...
                        // This is likely because of network/rpc errors and may not be permanent.
                        break Err(QuorumWaiterError::AntiQuorum);
                    }
                }
            })
            .await;
            match timeout_res {
                Ok(res) => match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(e),
                },
                Err(_elapsed) => Err(QuorumWaiterError::Timeout),
            }
        })
    }
}

#[derive(Clone, Debug, Error)]
pub enum QuorumWaiterError {
    #[error("Block was rejected by enough peers to never reach quorum")]
    QuorumRejected,
    #[error("Anti quorum reached for block (note this may not be permanent)")]
    AntiQuorum,
    #[error("Timed out waiting for quorum")]
    Timeout,
    #[error("Network Error")]
    Network,
    #[error("RPC Status Error {0}")]
    Rpc(StatusCode),
}

#[derive(Clone, Debug, Error)]
enum WaiterError {
    #[error("Block was rejected by peer")]
    Rejected(Stake),
    #[error("Network Error")]
    Network(Stake),
    #[error("RPC Status Error {0}")]
    Rpc(StatusCode, Stake),
}
