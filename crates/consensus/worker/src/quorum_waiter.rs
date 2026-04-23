//! Wait for a quorum of acks from workers before sharing with the primary.

use crate::network::WorkerNetworkHandle;
use futures::stream::{futures_unordered::FuturesUnordered, StreamExt as _};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tn_network_libp2p::error::NetworkError;
use tn_types::{Authority, BlsPublicKey, Committee, SealedBatch, TaskSpawner, VotingPower};
use tokio::sync::oneshot;
use tracing::debug;

/// Maximum number of retry attempts for reporting a batch to a single peer.
const MAX_BATCH_REPORT_ATTEMPTS: u32 = 3;

/// Base backoff duration for retrying batch reports (doubles each attempt).
const BATCH_REPORT_RETRY_BACKOFF: Duration = Duration::from_millis(200);

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

#[derive(Debug)]
struct QuorumWaiterInner {
    /// This authority.
    authority: Authority,
    /// The committee information.
    committee: Committee,
    /// A network sender to broadcast the batches to the other workers.
    network: WorkerNetworkHandle,
}

/// The QuorumWaiter waits for 2f authorities to acknowledge reception of a batch.
#[derive(Clone, Debug)]
pub struct QuorumWaiter {
    inner: Arc<QuorumWaiterInner>,
}

impl QuorumWaiter {
    /// Create a new QuorumWaiter.
    pub fn new(authority: Authority, committee: Committee, network: WorkerNetworkHandle) -> Self {
        Self { inner: Arc::new(QuorumWaiterInner { authority, committee, network }) }
    }

    /// Report a batch to a single peer with retry-and-backoff for transient network errors.
    ///
    /// - `Ok(deliver)` — peer accepted the batch
    /// - `Err(Rejected)` — peer explicitly rejected (RPCError), no retry
    /// - `Err(Network)` — all retry attempts exhausted
    async fn waiter(
        bls: BlsPublicKey,
        network: WorkerNetworkHandle,
        sealed_batch: SealedBatch,
        deliver: VotingPower,
    ) -> Result<VotingPower, WaiterError> {
        for attempt in 0..MAX_BATCH_REPORT_ATTEMPTS {
            match network.report_batch(bls, sealed_batch.clone()).await {
                Ok(()) => return Ok(deliver),
                Err(NetworkError::RPCError(msg)) => {
                    tracing::error!(
                        target = "worker::quorum_waiter",
                        "RPCError, peer {bls}: {msg}"
                    );
                    return Err(WaiterError::Rejected(deliver));
                }
                Err(err) => {
                    tracing::warn!(
                        target = "worker::quorum_waiter",
                        attempt = attempt + 1,
                        max_attempts = MAX_BATCH_REPORT_ATTEMPTS,
                        "Network error, peer {bls}: {err:?}, retrying..."
                    );
                    if attempt + 1 < MAX_BATCH_REPORT_ATTEMPTS {
                        tokio::time::sleep(BATCH_REPORT_RETRY_BACKOFF * 2u32.pow(attempt)).await;
                    }
                }
            }
        }
        Err(WaiterError::Network(deliver))
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
                let peers: Vec<_> =
                    inner.committee.others_keys_except(inner.authority.protocol_key());

                // Spawn per-peer retry tasks that report the batch with backoff.
                let mut wait_for_quorum: FuturesUnordered<
                    oneshot::Receiver<Result<VotingPower, WaiterError>>,
                > = FuturesUnordered::new();
                // Total stake available for the entire committee.
                // Can use this to determine anti-quorum more quickly.
                let mut available_stake = 0;
                // Stake from a committee member that has rejected this batch.
                let mut rejected_stake = 0;
                for (i, name) in peers.into_iter().enumerate() {
                    let stake = inner.committee.voting_power(&name);
                    available_stake += stake;
                    let (tx, rx) = oneshot::channel();
                    let network = inner.network.clone();
                    let batch = sealed_batch.clone();
                    spawner_clone.spawn_task(format!("qw-peer-{i}"), async move {
                        let res = Self::waiter(name, network, batch, stake).await;
                        let _ = tx.send(res);
                        Ok(())
                    });
                    wait_for_quorum.push(rx);
                }

                // Wait for the first 2f nodes to send back an Ack. Then we consider the batch
                // delivered and we send its digest to the primary (that will include it into
                // the dag). This should reduce the amount of syncing.
                let threshold = inner.committee.quorum_threshold();
                let mut total_stake = inner.authority.voting_power();
                // If more stake than this is rejected then the batch will never be accepted,
                // and account for this node's vote.
                let max_rejected_stake = (available_stake + total_stake) - threshold;

                debug!(
                    target: "quorum-waiter",
                    ?available_stake,
                    ?rejected_stake,
                    ?threshold,
                    ?total_stake,
                    ?max_rejected_stake,
                    "begin loop"
                );

                // Wait on the peer responses and produce an Ok(()) for quorum (2/3 stake confirmed
                // batch) or Error if quorum not reached.
                loop {
                    if let Some(res) = wait_for_quorum.next().await {
                        match res? {
                            Ok(stake) => {
                                total_stake += stake;
                                available_stake -= stake;
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
                                            Ok(())
                                        });
                                    }
                                    break Ok(());
                                }
                            }
                            Err(WaiterError::Rejected(stake)) => {
                                rejected_stake += stake;
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

                    debug!(
                        target: "quorum-waiter",
                        ?total_stake,
                        ?available_stake,
                        ?threshold,
                        ?rejected_stake,
                        ?max_rejected_stake,
                        "begin loop"
                    );

                    // check if quorum is impossible
                    if rejected_stake > max_rejected_stake {
                        // Can no longer reach quorum because our batch was explicitly rejected by
                        // to much stake.
                        break Err(QuorumWaiterError::QuorumRejected);
                    }
                    if total_stake + available_stake < threshold {
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
            let _ = tx.send(res.clone());
            res.map_err(|e| eyre::eyre!({ e }))
        });
        rx
    }
}

#[derive(Clone, Debug, Error)]
/// Error variants for proposing batches.
pub enum QuorumWaiterError {
    /// Block was rejected by enough peers to never reach quorum
    #[error("Block was rejected by enough peers to never reach quorum")]
    QuorumRejected,
    /// Anti quorum reached for batch (note this may not be permanent)
    #[error("Anti quorum reached for batch (note this may not be permanent)")]
    AntiQuorum,
    /// Timed out waiting for quorum
    #[error("Timed out waiting for quorum")]
    Timeout,
    /// Network-related error
    #[error("Network Error")]
    Network,
    /// RPC error (application-specific)
    #[error("RPC Status Error {0}")]
    Rpc(String),
    /// Oneshot receiver dropped.
    #[error("Oneshot receiver dropped.")]
    DroppedReceiver,
}

impl From<oneshot::error::RecvError> for QuorumWaiterError {
    fn from(_: oneshot::error::RecvError) -> Self {
        Self::DroppedReceiver
    }
}

#[derive(Clone, Debug, Error)]
enum WaiterError {
    /// Batch was rejected by peer.
    #[error("Batch was rejected by peer")]
    Rejected(VotingPower),
    /// Network-related error.
    #[error("Network Error")]
    Network(VotingPower),
}
