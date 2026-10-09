//! Process-lifetime reachability, independent of consensus, sync and RPC readiness.

use std::{future::Future, time::Duration};

use futures::{future::join_all, StreamExt as _};
use serde::Serialize;
use tn_types::WorkerId;
use tokio::{
    sync::watch,
    time::{interval, timeout, MissedTickBehavior},
};
use tokio_stream::wrappers::IntervalStream;

/// Bound on sending a network command and receiving its acknowledgement.
pub(crate) const NETWORK_COMMAND_TIMEOUT: Duration = Duration::from_secs(1);

/// Retry cadence for reachability snapshots, including unresponsive swarms.
const NETWORK_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// Established connectivity of one swarm. Pending dials never imply reachability.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(crate) enum SwarmReadiness {
    /// The swarm has not yet been sampled.
    Checking,
    /// At least one established peer is available for requests.
    Reachable {
        /// Number of established peers at the time of the probe.
        established_peers: usize,
    },
    /// The network task answered, but no peer is established.
    Disconnected,
    /// The network task could not answer the peer-count command.
    Unavailable,
    /// Sending the command or awaiting its reply exceeded the command bound.
    TimedOut,
}

impl SwarmReadiness {
    /// Whether this snapshot has at least one established peer.
    pub(crate) fn is_reachable(&self) -> bool {
        matches!(self, Self::Reachable { .. })
    }
}

/// Reachability of every configured swarm, for every node role.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum NetworkStatus {
    /// Every configured swarm has an established peer.
    Reachable,
    /// At least one configured swarm has no confirmed established peer.
    NotReady,
}

/// A worker's connectivity, preserving its configured identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct WorkerNetworkReadiness {
    /// Configured worker identifier, including nonzero workers.
    worker_id: WorkerId,
    /// Established connectivity of this worker's swarm.
    connectivity: SwarmReadiness,
}

/// Cached network-only readiness envelope served by `/health/network`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct NetworkReadiness {
    /// Schema version for the network readiness endpoint.
    version: u32,
    /// Aggregate reachability across the primary and every worker swarm.
    status: NetworkStatus,
    /// Established connectivity of the primary swarm.
    primary: SwarmReadiness,
    /// Established connectivity of each configured worker swarm.
    workers: Vec<WorkerNetworkReadiness>,
}

impl NetworkReadiness {
    /// Fail closed while process startup has not yet prepared the swarms.
    pub(crate) fn pending() -> Self {
        Self::new(SwarmReadiness::Checking, Vec::new())
    }

    /// Derive aggregate reachability without a worker-zero shortcut.
    fn new(primary: SwarmReadiness, workers: Vec<WorkerNetworkReadiness>) -> Self {
        let status = if primary.is_reachable()
            && workers.iter().all(|worker| worker.connectivity.is_reachable())
        {
            NetworkStatus::Reachable
        } else {
            NetworkStatus::NotReady
        };
        Self { version: 1, status, primary, workers }
    }

    /// Reachability does not certify consensus participation, sync or RPC acceptance.
    pub(crate) fn is_reachable(&self) -> bool {
        self.status == NetworkStatus::Reachable
    }
}

/// Sample a peer-count request with one bound covering queue admission and its reply.
pub(crate) async fn probe<F, E>(peer_count: F) -> SwarmReadiness
where
    F: Future<Output = Result<usize, E>>,
{
    timeout(NETWORK_COMMAND_TIMEOUT, peer_count)
        .await
        .map(|result| {
            result
                .map(|established_peers| {
                    if established_peers == 0 {
                        SwarmReadiness::Disconnected
                    } else {
                        SwarmReadiness::Reachable { established_peers }
                    }
                })
                .unwrap_or(SwarmReadiness::Unavailable)
        })
        .unwrap_or(SwarmReadiness::TimedOut)
}

/// Continuously sample all swarms concurrently and publish recoverable snapshots.
///
/// Run on the node's task spawner: epoch turnover leaves monitoring intact, and
/// task-manager shutdown drops pending peer-count requests and the retry timer.
pub(crate) async fn monitor<P, W, PF, WF, PE, WE>(
    publisher: watch::Sender<NetworkReadiness>,
    primary_count: P,
    worker_counts: W,
) where
    P: Fn() -> PF,
    W: Fn() -> Vec<(WorkerId, WF)>,
    PF: Future<Output = Result<usize, PE>>,
    WF: Future<Output = Result<usize, WE>>,
{
    let mut retry = interval(NETWORK_PROBE_INTERVAL);
    retry.set_missed_tick_behavior(MissedTickBehavior::Skip);
    IntervalStream::new(retry)
        .for_each(|_| async {
            let workers = worker_counts().into_iter().map(|(worker_id, count)| async move {
                WorkerNetworkReadiness { worker_id, connectivity: probe(count).await }
            });
            let (primary, workers) = tokio::join!(probe(primary_count()), join_all(workers));
            let readiness = NetworkReadiness::new(primary, workers);
            publisher.send_if_modified(|current| {
                if *current == readiness {
                    false
                } else {
                    tracing::info!(target: "epoch-manager", ?readiness, "network reachability changed");
                    *current = readiness;
                    true
                }
            });
        })
        .await;
}

#[cfg(test)]
mod tests {
    use super::{monitor, probe, NetworkReadiness, SwarmReadiness, NETWORK_PROBE_INTERVAL};
    use std::{
        future::{pending, ready},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::watch;

    /// A disconnected nonzero worker stays visible beyond the old startup wait and recovers.
    #[tokio::test(start_paused = true)]
    async fn recovery_after_startup_window_includes_every_worker(
    ) -> Result<(), watch::error::RecvError> {
        let primary = Arc::new(AtomicUsize::new(1));
        let workers = Arc::new(vec![(0, AtomicUsize::new(1)), (2, AtomicUsize::new(0))]);
        let (publisher, mut readiness) = watch::channel(NetworkReadiness::pending());
        let primary_probe = primary.clone();
        let worker_probes = workers.clone();
        let task = tokio::spawn(monitor(
            publisher,
            move || ready(Ok::<_, ()>(primary_probe.load(Ordering::Relaxed))),
            move || {
                worker_probes
                    .iter()
                    .map(|(id, peers)| (*id, ready(Ok::<_, ()>(peers.load(Ordering::Relaxed)))))
                    .collect()
            },
        ));
        readiness.changed().await?;
        assert!(!readiness.borrow().is_reachable());
        assert!(readiness.borrow().workers.iter().any(|worker| {
            worker.worker_id == 2 && worker.connectivity == SwarmReadiness::Disconnected
        }));

        tokio::time::advance(Duration::from_secs(180)).await;
        assert!(!readiness.borrow().is_reachable());
        workers
            .iter()
            .filter(|(id, _)| *id == 2)
            .for_each(|(_, peers)| peers.store(1, Ordering::Relaxed));
        tokio::time::advance(NETWORK_PROBE_INTERVAL).await;
        readiness.changed().await?;
        assert!(readiness.borrow().is_reachable());

        primary.store(0, Ordering::Relaxed);
        tokio::time::advance(NETWORK_PROBE_INTERVAL).await;
        readiness.changed().await?;
        assert!(!readiness.borrow().is_reachable());
        assert_eq!(readiness.borrow().primary, SwarmReadiness::Disconnected);
        task.abort();
        assert!(task.await.is_err());
        Ok(())
    }

    /// Unresponsive requests are bounded concurrently, with each affected worker identified.
    #[tokio::test(start_paused = true)]
    async fn stalled_swarms_are_bounded_and_cancellable() -> Result<(), watch::error::RecvError> {
        let (publisher, mut readiness) = watch::channel(NetworkReadiness::pending());
        let task = tokio::spawn(monitor(publisher, pending::<Result<usize, ()>>, || {
            vec![(0, pending::<Result<usize, ()>>()), (3, pending::<Result<usize, ()>>())]
        }));
        readiness.changed().await?;
        assert_eq!(readiness.borrow().primary, SwarmReadiness::TimedOut);
        assert!(readiness
            .borrow()
            .workers
            .iter()
            .all(|worker| { worker.connectivity == SwarmReadiness::TimedOut }));
        assert!(readiness.borrow().workers.iter().any(|worker| worker.worker_id == 3));
        assert_eq!(readiness.borrow().workers.len(), 2);
        tokio::time::advance(NETWORK_PROBE_INTERVAL).await;
        tokio::task::yield_now().await;
        task.abort();
        assert!(task.await.is_err());
        Ok(())
    }

    /// Channel failures and empty established-peer queues both fail closed.
    #[tokio::test]
    async fn peer_count_errors_and_zero_peers_are_not_ready() {
        assert_eq!(probe(ready(Err::<usize, _>(()))).await, SwarmReadiness::Unavailable);
        assert_eq!(probe(ready(Ok::<_, ()>(0))).await, SwarmReadiness::Disconnected);
        assert!(!NetworkReadiness::pending().is_reachable());
    }

    /// A command-channel failure after a disconnected sample stays not-ready and can recover.
    #[tokio::test(start_paused = true)]
    async fn channel_failure_on_retry_recovers_on_later_probe() -> eyre::Result<()> {
        let (responses, primary_response) = watch::channel(Ok::<usize, ()>(0));
        let worker_response = primary_response.clone();
        let (publisher, mut readiness) = watch::channel(NetworkReadiness::pending());
        let monitor = tokio::spawn(monitor(
            publisher,
            move || ready(*primary_response.borrow()),
            move || vec![(1, ready(*worker_response.borrow()))],
        ));

        readiness.changed().await?;
        let disconnected = readiness.borrow_and_update().clone();
        assert_eq!(disconnected.primary, SwarmReadiness::Disconnected);
        assert!(!disconnected.is_reachable());

        responses.send(Err(()))?;
        readiness.changed().await?;
        let unavailable = readiness.borrow_and_update().clone();
        assert_eq!(unavailable.primary, SwarmReadiness::Unavailable);
        assert!(unavailable
            .workers
            .iter()
            .all(|worker| { worker.connectivity == SwarmReadiness::Unavailable }));
        assert!(!unavailable.is_reachable());

        responses.send(Ok(1))?;
        readiness.changed().await?;
        assert!(readiness.borrow_and_update().is_reachable());
        monitor.abort();
        Ok(())
    }

    /// Epoch-task shutdown preserves the monitor, while node shutdown cancels stalled probes.
    #[tokio::test(start_paused = true)]
    async fn epoch_turnover_preserves_monitor_and_node_shutdown_cancels_it() -> eyre::Result<()> {
        use tn_types::{ShutdownNotifier, TaskManager};
        use tokio::sync::Notify;

        let node_tasks = TaskManager::new("readiness-node");
        let mut epoch_tasks = TaskManager::new("readiness-epoch");
        let (publisher, mut readiness) = watch::channel(NetworkReadiness::pending());
        let primary_started = Arc::new(Notify::new());
        let worker_started = Arc::new(Notify::new());
        let primary_probe_started = primary_started.clone();
        let worker_probe_started = worker_started.clone();
        node_tasks.get_spawner().spawn_task("network-readiness", async move {
            monitor(
                publisher,
                move || {
                    let started = primary_probe_started.clone();
                    async move {
                        started.notify_one();
                        pending::<Result<usize, ()>>().await
                    }
                },
                move || {
                    let started = worker_probe_started.clone();
                    vec![(4, async move {
                        started.notify_one();
                        pending::<Result<usize, ()>>().await
                    })]
                },
            )
            .await;
            Ok(())
        });
        primary_started.notified().await;
        worker_started.notified().await;
        readiness.changed().await?;
        let epoch_shutdown = ShutdownNotifier::default();
        epoch_shutdown.notify();
        epoch_tasks.join(epoch_shutdown).await?;
        drop(epoch_tasks);
        assert!(!readiness.has_changed()?);
        assert_eq!(readiness.borrow().primary, SwarmReadiness::TimedOut);
        tokio::time::advance(NETWORK_PROBE_INTERVAL).await;
        primary_started.notified().await;
        worker_started.notified().await;
        let _ = readiness.borrow_and_update();
        // Manager drop cancels the stalled monitor and drops its sole readiness publisher.
        drop(node_tasks);
        assert!(tokio::time::timeout(Duration::from_secs(1), readiness.changed()).await?.is_err());
        Ok(())
    }
}
