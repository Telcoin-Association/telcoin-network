//! Current-epoch readiness for persistent worker RPC servers and transaction pools.

use crate::health::WorkerReadiness;
use tn_types::{Noticer, WorkerId};

/// The worker range and shutdown signal belonging to one active epoch.
#[derive(Debug)]
struct WorkerEpoch {
    /// Number of workers in the epoch's committee, starting at worker zero.
    worker_count: usize,
    /// Becomes noticed when consensus stops driving this epoch's workers.
    shutdown: Noticer,
}

/// Tracks epoch membership separately from process-lifetime worker initialization.
#[derive(Debug, Default)]
pub(super) struct WorkerReadinessState {
    /// Absent until the epoch manager starts worker initialization.
    epoch: Option<WorkerEpoch>,
}

impl WorkerReadinessState {
    /// Replace the active range and shutdown signal before initializing an epoch's workers.
    pub(super) fn start_epoch(&mut self, worker_count: usize, shutdown: Noticer) {
        self.epoch = Some(WorkerEpoch { worker_count, shutdown });
    }

    /// Report every initialized worker, accepting only while its current epoch is active.
    pub(super) fn snapshot(&self, initialized_workers: usize) -> Vec<WorkerReadiness> {
        let active_workers = self
            .epoch
            .as_ref()
            .filter(|epoch| !epoch.shutdown.noticed())
            .map_or(0, |epoch| epoch.worker_count);
        (0..=WorkerId::MAX)
            .take(initialized_workers)
            .map(|worker_id| {
                WorkerReadiness::new(worker_id, usize::from(worker_id) < active_workers)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::WorkerReadinessState;
    use crate::health::WorkerReadiness;
    use tn_types::ShutdownNotifier;

    /// Initialization, membership changes and shutdown constrain readiness independently.
    #[test]
    fn readiness_follows_initialization_and_epoch_membership() {
        let mut state = WorkerReadinessState::default();
        assert!(state.snapshot(0).is_empty());
        assert_eq!(state.snapshot(1), vec![WorkerReadiness::new(0, false)]);

        let first_epoch = ShutdownNotifier::new();
        state.start_epoch(2, first_epoch.subscribe());
        assert!(state.snapshot(0).is_empty());
        assert_eq!(state.snapshot(1), vec![WorkerReadiness::new(0, true)]);
        assert_eq!(
            state.snapshot(2),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]
        );

        first_epoch.notify();
        assert_eq!(
            state.snapshot(2),
            vec![WorkerReadiness::new(0, false), WorkerReadiness::new(1, false)]
        );

        let smaller_epoch = ShutdownNotifier::new();
        state.start_epoch(1, smaller_epoch.subscribe());
        assert_eq!(
            state.snapshot(2),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, false)]
        );

        let larger_epoch = ShutdownNotifier::new();
        state.start_epoch(2, larger_epoch.subscribe());
        smaller_epoch.notify();
        assert_eq!(
            state.snapshot(2),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]
        );
    }
}
