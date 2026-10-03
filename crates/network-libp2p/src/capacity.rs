//! Serve admission and occupancy measurements tied to the lifetime of concurrency permits.

use crate::{metrics::network_label, types::NetworkType};
use metrics::Gauge;
use std::{fmt, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// A separately bounded class of network work.
#[derive(Clone, Copy, Debug)]
pub enum ServeClass {
    /// Primary epoch and consensus-output streams.
    EpochStream,
    /// Primary epoch-record request-response serves.
    EpochRecord,
    /// Primary denial writes for streams refused at capacity.
    PrimaryShed,
    /// Worker batch streams.
    BatchStream,
    /// Worker denial writes for streams refused at capacity.
    WorkerShed,
    /// Gossip-triggered worker batch prefetches.
    Prefetch,
}

impl ServeClass {
    /// Stable qualification and Prometheus label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::EpochStream => "epoch_stream",
            Self::EpochRecord => "epoch_record",
            Self::PrimaryShed => "primary_shed",
            Self::BatchStream => "batch_stream",
            Self::WorkerShed => "worker_shed",
            Self::Prefetch => "prefetch",
        }
    }
}

/// A bounded-cardinality reason for refusing network work.
#[derive(Clone, Copy, Debug)]
pub enum ServeRejection {
    /// The global concurrency budget has no free permit.
    GlobalLimit,
    /// The semaphore was closed during shutdown.
    Closed,
    /// The peer already holds its per-peer concurrency allocation.
    PeerLimit,
    /// A worker already has a prefetch for the same batch digest.
    Duplicate,
}

impl ServeRejection {
    const fn label(self) -> &'static str {
        match self {
            Self::GlobalLimit => "global_limit",
            Self::Closed => "closed",
            Self::PeerLimit => "peer_limit",
            Self::Duplicate => "duplicate",
        }
    }
}

/// A concurrency budget whose measured permits include queued and active tasks.
///
/// The underlying Tokio semaphore still decides admission. Measurements add no waiting,
/// queue, or capacity exemption. A permit updates occupancy when reserved, before a task
/// can be spawned, and when dropped, including cancellation before the first poll.
pub struct CapacitySemaphore {
    /// Admission authority shared by this serve actor's tasks.
    semaphore: Arc<Semaphore>,
    /// Optional process recorder bindings, absent in isolated admission tests.
    metrics: Option<CapacityMetrics>,
}

/// Bounded labels and the gauge owned by measured permits.
struct CapacityMetrics {
    /// Reserved slots, including tasks that have not yet been polled.
    active: Gauge,
    /// Primary or worker swarm label.
    network: String,
    /// Independent serve class label.
    class: ServeClass,
}

impl CapacitySemaphore {
    /// Construct an unregistered budget for isolated admission tests.
    pub fn new(permits: usize) -> Self {
        Self { semaphore: Arc::new(Semaphore::new(permits)), metrics: None }
    }

    /// Construct a budget with per-network, per-class occupancy and rejection measurements.
    pub fn new_for(permits: usize, class: ServeClass, network: &NetworkType) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            metrics: Some(Self::register(permits, class, network)),
        }
    }

    fn register(permits: usize, class: ServeClass, network: &NetworkType) -> CapacityMetrics {
        let network = network_label(network);
        let active = metrics::gauge!(
            "tn_network.serve_tasks_active", "network" => network.clone(), "class" => class.label()
        );
        active.increment(0.0);
        metrics::gauge!(
            "tn_network.serve_tasks_limit", "network" => network.clone(), "class" => class.label()
        )
        .set(permits.to_string().parse::<f64>().unwrap_or(f64::MAX));
        CapacityMetrics { active, network, class }
    }

    /// Register allocations even when this swarm has no active consensus serve actor.
    pub(crate) fn initialize(network: &NetworkType, limits: &tn_config::NetworkServeConfig) {
        let allocations = match network {
            NetworkType::Primary => [
                (ServeClass::EpochStream, limits.epoch_stream()),
                (ServeClass::EpochRecord, limits.epoch_record()),
                (ServeClass::PrimaryShed, limits.primary_shed()),
            ],
            NetworkType::Worker(_) => [
                (ServeClass::BatchStream, limits.batch_stream()),
                (ServeClass::WorkerShed, limits.worker_shed()),
                (ServeClass::Prefetch, limits.prefetch()),
            ],
        };
        allocations.into_iter().for_each(|(class, limit)| {
            Self::register(limit, class, network);
        });
    }

    /// Attempt admission without waiting or allocating a waiting task.
    pub fn try_acquire_owned(&self) -> Result<CapacityPermit, TryAcquireError> {
        self.semaphore
            .clone()
            .try_acquire_owned()
            .map(|permit| {
                let active = self.metrics.as_ref().map(|metrics| {
                    metrics.active.increment(1.0);
                    metrics.active.clone()
                });
                CapacityPermit { _permit: permit, active }
            })
            .inspect_err(|error| {
                self.record_rejection(match error {
                    TryAcquireError::NoPermits => ServeRejection::GlobalLimit,
                    TryAcquireError::Closed => ServeRejection::Closed,
                });
            })
    }

    /// Current number of unreserved permits.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Record a refusal by an additional per-peer or deduplication admission guard.
    pub fn record_rejection(&self, reason: ServeRejection) {
        self.metrics.iter().for_each(|metrics| {
            metrics::counter!(
                "tn_network.serve_rejections_total",
                "network" => metrics.network.clone(),
                "class" => metrics.class.label(),
                "reason" => reason.label()
            )
            .increment(1);
        });
    }
}

impl fmt::Debug for CapacitySemaphore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapacitySemaphore")
            .field("available_permits", &self.available_permits())
            .finish_non_exhaustive()
    }
}

/// An admitted task's permit, releasing capacity and occupancy on every drop path.
pub struct CapacityPermit {
    /// Released after the drop implementation decrements occupancy.
    _permit: OwnedSemaphorePermit,
    /// Gauge retained independently of the serve actor's lifetime.
    active: Option<Gauge>,
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        self.active.iter().for_each(|active| {
            // Decrement before the semaphore permit is released so a concurrent admission
            // cannot transiently count both the released task and its replacement.
            active.decrement(1.0);
        });
    }
}

impl fmt::Debug for CapacityPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CapacityPermit").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    fn gauge(recorder: &DebuggingRecorder, network: &str, name: &str) -> Option<f64> {
        recorder.snapshotter().snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
            (key.key().name() == name
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "network" && label.value() == network))
            .then_some(value)
            .and_then(
                |value| {
                    if let DebugValue::Gauge(value) = value {
                        Some(value.0)
                    } else {
                        None
                    }
                },
            )
        })
    }

    /// Cancellation before a task's first poll releases both the slot and its measurement.
    #[test]
    fn cancellation_releases_reserved_occupancy() -> Result<(), TryAcquireError> {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let budget =
                CapacitySemaphore::new_for(1, ServeClass::BatchStream, &NetworkType::Worker(0));
            let permit = budget.try_acquire_owned()?;
            let unpolled = async move {
                let _permit = permit;
                std::future::pending::<()>().await;
            };
            assert_eq!(budget.available_permits(), 0);
            assert!(budget.try_acquire_owned().is_err());
            drop(unpolled);
            assert_eq!(budget.available_permits(), 1);
            let snapshot = recorder.snapshotter().snapshot().into_vec();
            assert!(snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == "tn_network.serve_tasks_active"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "network" && label.value() == "worker-0")
                    && matches!(value, DebugValue::Gauge(value) if value.0 == 0.0)
            }));
            assert!(snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == "tn_network.serve_rejections_total"
                    && key
                        .key()
                        .labels()
                        .any(|label| label.key() == "reason" && label.value() == "global_limit")
                    && matches!(value, DebugValue::Counter(1))
            }));
            let _replacement = budget.try_acquire_owned()?;
            assert_eq!(gauge(&recorder, "worker-0", "tn_network.serve_tasks_active"), Some(1.0));
            Ok(())
        })
    }

    /// Distinct worker budgets remain separately observable at zero and under load.
    #[test]
    fn workers_have_independent_series() -> Result<(), TryAcquireError> {
        let recorder = DebuggingRecorder::new();
        metrics::with_local_recorder(&recorder, || {
            let first =
                CapacitySemaphore::new_for(5, ServeClass::BatchStream, &NetworkType::Worker(0));
            let second =
                CapacitySemaphore::new_for(5, ServeClass::BatchStream, &NetworkType::Worker(1));
            let _permit = first.try_acquire_owned()?;
            assert_eq!(gauge(&recorder, "worker-0", "tn_network.serve_tasks_active"), Some(1.0));
            assert_eq!(gauge(&recorder, "worker-1", "tn_network.serve_tasks_active"), Some(0.0));
            assert_eq!(second.available_permits(), 5);
            first.record_rejection(ServeRejection::PeerLimit);
            second.record_rejection(ServeRejection::Duplicate);
            let snapshot = recorder.snapshotter().snapshot().into_vec();
            [("worker-0", "peer_limit"), ("worker-1", "duplicate")].into_iter().for_each(
                |(network, reason)| {
                    assert!(snapshot.iter().any(|(key, _, _, value)| {
                        key.key().name() == "tn_network.serve_rejections_total"
                            && key
                                .key()
                                .labels()
                                .any(|label| label.key() == "network" && label.value() == network)
                            && key
                                .key()
                                .labels()
                                .any(|label| label.key() == "reason" && label.value() == reason)
                            && matches!(value, DebugValue::Counter(1))
                    }));
                },
            );
            Ok(())
        })
    }
}
