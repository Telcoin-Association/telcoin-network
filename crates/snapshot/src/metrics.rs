//! Prometheus metrics for the snapshot subsystem.
//!
//! All series are emitted under the `tn_snapshot` scope: the `tn_snapshot.jobs_total` counter
//! is labeled by job `outcome`, while the `tn_snapshot.last_uploaded_epoch` and
//! `tn_snapshot.last_restored_epoch` gauges track the most recent successful upload and
//! restore.

// TODO(snapshot): remove once service/export/restore wire these handles (C6/C7)
#![allow(dead_code)]

use reth_metrics::{metrics::Gauge, Metrics};

/// Metrics for the snapshot subsystem, owned by the snapshot service.
///
/// The gauges are set directly by their owner; the labeled `jobs_total` counter is recorded
/// through [`SnapshotMetrics::record_job`] because its `outcome` label varies per call and so
/// cannot be a derive-backed field.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_snapshot")]
pub(crate) struct SnapshotMetrics {
    /// Epoch number of the most recently uploaded snapshot.
    pub(crate) last_uploaded_epoch: Gauge,
    /// Epoch number of the most recently restored snapshot.
    pub(crate) last_restored_epoch: Gauge,
}

impl SnapshotMetrics {
    /// Record a completed snapshot job with the given `outcome`.
    pub(crate) fn record_job(&self, outcome: JobOutcome) {
        metrics::counter!("tn_snapshot.jobs_total", "outcome" => outcome.as_str()).increment(1);
    }
}

/// Terminal outcome of a snapshot job, used as the `outcome` label on the
/// `tn_snapshot.jobs_total` counter.
#[derive(Clone, Copy, Debug)]
pub(crate) enum JobOutcome {
    /// The snapshot was exported and uploaded successfully.
    Uploaded,
    /// Skipped because the target epoch overlapped an existing remote snapshot.
    SkippedOverlap,
    /// Skipped because the upload-and-certify deadline elapsed.
    SkippedCertTimeout,
    /// The job failed with an error.
    Failed,
}

impl JobOutcome {
    /// Return the metric label value for this outcome.
    fn as_str(self) -> &'static str {
        match self {
            Self::Uploaded => "uploaded",
            Self::SkippedOverlap => "skipped_overlap",
            Self::SkippedCertTimeout => "skipped_cert_timeout",
            Self::Failed => "failed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // new_with_labels binds handles to the local recorder; Default::default() would
            // cache the first-bound recorder process-wide
            let metrics = SnapshotMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.last_uploaded_epoch.set(7.0);
            metrics.last_restored_epoch.set(4.0);
            metrics.record_job(JobOutcome::Uploaded);
            metrics.record_job(JobOutcome::SkippedOverlap);
            metrics.record_job(JobOutcome::SkippedCertTimeout);
            metrics.record_job(JobOutcome::Failed);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_snapshot.last_uploaded_epoch");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 7.0));
        let (_, _, _, value) = find("tn_snapshot.last_restored_epoch");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 4.0));

        // jobs_total is one series per outcome label value
        let (key, _, _, _) = find("tn_snapshot.jobs_total");
        assert!(key.key().labels().any(|l| l.key() == "outcome"));
    }
}
