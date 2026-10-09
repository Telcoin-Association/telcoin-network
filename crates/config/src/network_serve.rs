//! Independent, finite concurrency limits for each network serve class.

use serde::{Deserialize, Serialize};
use std::num::NonZeroU16;

/// Operator-selected serve limits, applied independently by each primary and worker.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkServeConfig {
    /// Primary stream slots, bounded below Tokio's semaphore maximum on all targets.
    epoch_stream: NonZeroU16,
    /// Primary record slots, independent of long-lived streams.
    epoch_record: NonZeroU16,
    /// Primary denial-write slots.
    primary_shed: NonZeroU16,
    /// Batch-stream slots on each worker.
    batch_stream: NonZeroU16,
    /// Denial-write slots on each worker.
    worker_shed: NonZeroU16,
    /// Batch-prefetch slots on each worker.
    prefetch: NonZeroU16,
}

impl Default for NetworkServeConfig {
    fn default() -> Self {
        let five = NonZeroU16::MIN.saturating_add(4);
        let eight = NonZeroU16::MIN.saturating_add(7);
        Self {
            epoch_stream: five,
            epoch_record: five,
            primary_shed: eight,
            batch_stream: five,
            worker_shed: eight,
            prefetch: eight,
        }
    }
}

impl NetworkServeConfig {
    /// Epoch-pack and consensus-output stream slots on the primary.
    pub fn epoch_stream(&self) -> usize {
        usize::from(self.epoch_stream.get())
    }
    /// Separate epoch-record serve slots on the primary.
    pub fn epoch_record(&self) -> usize {
        usize::from(self.epoch_record.get())
    }
    /// Denial-write slots on the primary.
    pub fn primary_shed(&self) -> usize {
        usize::from(self.primary_shed.get())
    }
    /// Batch-stream slots on each worker.
    pub fn batch_stream(&self) -> usize {
        usize::from(self.batch_stream.get())
    }
    /// Denial-write slots on each worker.
    pub fn worker_shed(&self) -> usize {
        usize::from(self.worker_shed.get())
    }
    /// Gossip-prefetch slots on each worker.
    pub fn prefetch(&self) -> usize {
        usize::from(self.prefetch.get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Operator values reach all six independent admission classes; zero is rejected.
    #[test]
    fn operator_limits_are_finite_and_independent() -> Result<(), serde_json::Error> {
        let config: NetworkServeConfig = serde_json::from_str(
            r#"{"epoch_stream":1,"epoch_record":2,"primary_shed":3,"batch_stream":4,"worker_shed":5,"prefetch":6}"#,
        )?;
        assert_eq!(
            [
                config.epoch_stream(),
                config.epoch_record(),
                config.primary_shed(),
                config.batch_stream(),
                config.worker_shed(),
                config.prefetch()
            ],
            [1, 2, 3, 4, 5, 6]
        );
        ["epoch_stream", "epoch_record", "primary_shed", "batch_stream", "worker_shed", "prefetch"]
            .into_iter()
            .for_each(|field| {
                let invalid = format!(r#"{{"{field}":0}}"#);
                assert!(serde_json::from_str::<NetworkServeConfig>(&invalid).is_err());
                let oversized = format!(r#"{{"{field}":65536}}"#);
                assert!(serde_json::from_str::<NetworkServeConfig>(&oversized).is_err());
            });
        Ok(())
    }
}
