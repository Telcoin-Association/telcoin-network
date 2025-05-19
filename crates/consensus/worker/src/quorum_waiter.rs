//! Wait for a quorum of acks from workers before sharing with the primary.

use crate::{metrics::WorkerMetrics, network::WorkerNetworkHandle};
use consensus_metrics::monitored_future;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tn_network_libp2p::error::NetworkError;
use tn_types::{
    network_public_key_to_libp2p, Authority, Committee, SealedBatch, TaskSpawner, VotingPower,
    WorkerCache, WorkerId,
};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/quorum_waiter_tests.rs"]
pub mod quorum_waiter_tests;

/// Interface to QuorumWaiter, exists primarily for tests.
pub trait QuorumWaiterTrait: Send + Sync + Clone + Unpin + 'static {
    /// Send a batch to committee peers in an attempt to get quorum on it's validity.
    ///
    /// Returns a JoinHandle to a future that will timeout.  Each peer attempt can:
    /// - Accept the batch and it's stake to quorum
    /// - Reject the batch explicitly in which case it's stake will never be added to quorum (can
    ///   cause total batch rejection)
    /// - Have an error of some type stopping it's stake from adding to quorum but possibly not
    ///   forever
    ///
    /// If the future resolves to Ok then the batch has reached quorum other wise examine the error.
    /// An error of QuorumWaiterError::QuorumRejected indicates the batch will never be accepted
    /// otherwise it might be possible if the network improves.
    fn verify_batch(
        &self,
        batch: SealedBatch,
        timeout: Duration,
        task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>>;
}

/// Basically BoxFuture but without the unneeded lifetime.
type QMBoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

struct QuorumWaiterInner {
    /// This authority- if None we are not a validator and this won't do much...
    authority: Authority,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The worker information cache.
    worker_cache: WorkerCache,
    /// A network sender to broadcast the batches to the other workers.
    network: WorkerNetworkHandle,
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
        network: WorkerNetworkHandle,
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
        wait_for: oneshot::Receiver<Result<(), NetworkError>>,
        deliver: VotingPower,
    ) -> Result<VotingPower, WaiterError> {
        match wait_for.await {
            Ok(r) => {
                match r {
                    Ok(_) => Ok(deliver),
                    Err(NetworkError::RPCError(msg)) => {
                        tracing::error!(target = "worker::quorum_waiter", "RPCError: {msg}");
                        Err(WaiterError::Rejected(deliver))
                    }
                    // Non-exhaustive enum...
                    Err(err) => {
                        tracing::error!(target = "worker::quorum_waiter", "Network error: {err}");
                        Err(WaiterError::Network(deliver))
                    }
                }
            }
            Err(_) => Err(WaiterError::Network(deliver)),
        }
    }
}

impl QuorumWaiterTrait for QuorumWaiter {
    fn verify_batch(
        &self,
        sealed_batch: SealedBatch,
        timeout: Duration,
        task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
        let inner = self.inner.clone();
        let task_name = format!("verifying-batch-{}", sealed_batch.digest());
        let (tx, rx) = oneshot::channel();
        let spawner_clone = task_spawner.clone();
        task_spawner.spawn_task(task_name, async move {
            let timeout_res = tokio::time::timeout(timeout, async move {
                let start_time = Instant::now();
                // Broadcast the batch to the other workers.
                let workers: Vec<_> = inner
                    .worker_cache
                    .others_workers_by_id(inner.authority.protocol_key(), &inner.id)
                    .into_iter()
                    .map(|(name, info)| (name, info.name))
                    .collect();
                let (primary_names, worker_names): (Vec<_>, Vec<_>) = workers.into_iter().unzip();

                let handlers = inner.network.report_batch_to_peers(
                    worker_names.iter().map(network_public_key_to_libp2p).collect(),
                    sealed_batch,
                );
                let _timer = inner.metrics.batch_broadcast_quorum_latency.start_timer();

                // Collect all the handlers to receive acknowledgements.
                let mut wait_for_quorum: FuturesUnordered<
                    QMBoxFuture<Result<VotingPower, WaiterError>>,
                > = FuturesUnordered::new();
                // Total stake available for the entire committee.
                // Can use this to determine anti-quorum more quickly.
                let mut available_stake = 0;
                // Stake from a committee member that has rejected this batch.
                let mut rejected_stake = 0;
                primary_names
                    .into_iter()
                    .zip(handlers.into_iter())
                    .map(|(name, handler)| {
                        let stake = inner.committee.voting_power(&name);
                        available_stake += stake;
                        Box::pin(monitored_future!(Self::waiter(handler, stake)))
                    })
                    .for_each(|f| wait_for_quorum.push(f));

                // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
                // delivered and we send its digest to the primary (that will include it into
                // the dag). This should reduce the amount of syncing.
                let threshold = inner.committee.quorum_threshold();
                let mut total_stake = inner.authority.voting_power();
                // If more stake than this is rejected then the batch will never be accepted.
                let max_rejected_stake = available_stake - threshold;

                // Wait on the peer responses and produce an Ok(()) for quorum (2/3 stake confirmed
                // batch) or Error if quorum not reached.
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
                                        spawner_clone.spawn_task("quorum-remainder", async move {
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
                        }
                    } else {
                        // Ran out of Peers and did not reach quorum...
                        break Err(QuorumWaiterError::AntiQuorum);
                    }
                    if rejected_stake > max_rejected_stake {
                        // Can no longer reach quorum because our batch was explicitly rejected by
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

            let res = match timeout_res {
                Ok(res) => match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(e),
                },
                Err(_elapsed) => Err(QuorumWaiterError::Timeout),
            };

            // forward result
            tx.send(res)
        });
        rx
    }
}

#[derive(Clone, Debug, Error)]
pub enum QuorumWaiterError {
    #[error("Block was rejected by enough peers to never reach quorum")]
    QuorumRejected,
    #[error("Anti quorum reached for batch (note this may not be permanent)")]
    AntiQuorum,
    #[error("Timed out waiting for quorum")]
    Timeout,
    #[error("Network Error")]
    Network,
    #[error("RPC Status Error {0}")]
    Rpc(String),
}

#[derive(Clone, Debug, Error)]
enum WaiterError {
    #[error("Block was rejected by peer")]
    Rejected(VotingPower),
    #[error("Network Error")]
    Network(VotingPower),
}
