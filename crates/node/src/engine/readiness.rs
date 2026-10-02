//! Current-epoch readiness for worker RPC listeners and persistent transaction pools.

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

    /// Report every initialized worker, accepting only with a running RPC and active epoch.
    pub(super) fn snapshot(
        &self,
        running_workers: impl IntoIterator<Item = bool>,
    ) -> Vec<WorkerReadiness> {
        let active_workers = self
            .epoch
            .as_ref()
            .filter(|epoch| !epoch.shutdown.noticed())
            .map_or(0, |epoch| epoch.worker_count);
        (0..=WorkerId::MAX)
            .zip(running_workers)
            .map(|(worker_id, rpc_running)| {
                WorkerReadiness::new(
                    worker_id,
                    rpc_running && usize::from(worker_id) < active_workers,
                )
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
        assert!(state.snapshot([]).is_empty());
        assert_eq!(state.snapshot([true]), vec![WorkerReadiness::new(0, false)]);

        let first_epoch = ShutdownNotifier::new();
        state.start_epoch(2, first_epoch.subscribe());
        assert!(state.snapshot([]).is_empty());
        assert_eq!(state.snapshot([true]), vec![WorkerReadiness::new(0, true)]);
        assert_eq!(
            state.snapshot([true, true]),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]
        );

        first_epoch.notify();
        assert_eq!(
            state.snapshot([true, true]),
            vec![WorkerReadiness::new(0, false), WorkerReadiness::new(1, false)]
        );

        let smaller_epoch = ShutdownNotifier::new();
        state.start_epoch(1, smaller_epoch.subscribe());
        assert_eq!(
            state.snapshot([true, true]),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, false)]
        );

        let larger_epoch = ShutdownNotifier::new();
        state.start_epoch(2, larger_epoch.subscribe());
        smaller_epoch.notify();
        assert_eq!(
            state.snapshot([true, true]),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]
        );
    }

    /// A retained worker stays unavailable on regrowth until its RPC listeners restart.
    #[test]
    fn readiness_waits_for_rpc_restart() {
        let mut state = WorkerReadinessState::default();
        let epoch = ShutdownNotifier::new();
        state.start_epoch(2, epoch.subscribe());
        assert_eq!(
            state.snapshot([true, false]),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, false)]
        );
        assert_eq!(
            state.snapshot([true, true]),
            vec![WorkerReadiness::new(0, true), WorkerReadiness::new(1, true)]
        );
    }
}
