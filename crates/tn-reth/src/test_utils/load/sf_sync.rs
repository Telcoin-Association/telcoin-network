//! The static files' sync timing per segment, for the exec bench's per-output rows:
//! [`SfSyncStats`], and [`SfSyncSource`], what reads (and resets) it from an env.
//!
//! A persist syncs each static-file segment it wrote, in the segment's own thread inside
//! `save_blocks` (vendor/reth CHANGES.md TN-0118): the data file, its offsets (`.off`), a
//! changeset segment's `.csoff` sidecar, then the `.conf` freeze (temp file, `fsync`, rename and
//! the directory's `fsync`). The commit then only updates the readers' index. reth's metrics time
//! the static-file arm and its commit whole (`save_blocks_sf`, `save_blocks_commit_sf`); these
//! split them by segment and phase, from the static-file provider's own timers
//! (`StaticFileProvider::take_sync_stats`, TN-0119, through
//! [`RethEnv::take_static_file_sync_stats`]).

use crate::RethEnv;
use std::collections::BTreeMap;

/// The phases a segment's sync splits into, by their labels: the data file, the offsets file, the
/// changeset offsets sidecar, the config file's freeze (its directory `fsync` included), and the
/// readers' index update at the commit.
pub const PHASES: [&str; 5] = ["data", "off", "csoff", "conf", "index"];

/// One phase of one segment's syncs over a span (an output, say).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PhaseTiming {
    /// How many syncs.
    pub calls: u64,
    /// Their summed time in nanoseconds.
    pub total_ns: u64,
    /// The longest one in nanoseconds.
    pub max_ns: u64,
}

/// The static files' syncs over a span, per (segment label, phase label): the segment as reth's
/// `StaticFileSegment::as_str` names it, the phase one of [`PHASES`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SfSyncStats {
    /// Each timed (segment, phase).
    pub phases: BTreeMap<(&'static str, &'static str), PhaseTiming>,
}

impl SfSyncStats {
    /// Add `calls` syncs of `phase` on `segment` taking `total_ns` in all, the longest `max_ns`.
    pub fn add(
        &mut self,
        segment: &'static str,
        phase: &'static str,
        calls: u64,
        total_ns: u64,
        max_ns: u64,
    ) {
        let timing = self.phases.entry((segment, phase)).or_default();
        timing.calls += calls;
        timing.total_ns += total_ns;
        timing.max_ns = timing.max_ns.max(max_ns);
    }
}

/// What measures the static files' syncs.
pub trait SfSyncSource {
    /// The syncs since the last take, resetting them; `None` when nothing measures them.
    fn take_sf_sync_stats(&self) -> Option<SfSyncStats>;
}

impl SfSyncSource for RethEnv {
    /// The static-file provider's timers since the last take, one entry per segment and phase
    /// that ran; `Some` even when nothing synced (an empty span).
    fn take_sf_sync_stats(&self) -> Option<SfSyncStats> {
        let mut stats = SfSyncStats::default();
        for (segment, phases) in self.take_static_file_sync_stats().iter() {
            let label = segment.as_str();
            for (phase, timing) in [
                (PHASES[0], phases.data_sync),
                (PHASES[1], phases.offsets_sync),
                (PHASES[2], phases.csoff_sync),
                (PHASES[3], phases.conf_freeze),
                (PHASES[4], phases.update_index),
            ] {
                if timing.calls > 0 {
                    stats.add(
                        label,
                        phase,
                        timing.calls,
                        u64::try_from(timing.total.as_nanos()).unwrap_or(u64::MAX),
                        u64::try_from(timing.max.as_nanos()).unwrap_or(u64::MAX),
                    );
                }
            }
        }
        Some(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{consensus_output_for_tests, execute_payload_and_update_canonical_chain},
        RethChainSpec,
    };
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_types::{test_genesis, TaskManager};

    /// An env's source reads the provider's timers: after one persist (past the first, whose
    /// writer opens update the index too) each v1 segment reports one data, offsets, config and
    /// index call under its label, no `.csoff` (v1 has no changeset segment), and the take
    /// drains them.
    #[tokio::test]
    async fn test_env_reports_the_providers_sync_timers() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("sf sync source");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
        let output1 = consensus_output_for_tests(2, 0, 1, false);
        let payload1 = TNPayload::new_for_test(chain.sealed_genesis_header(), &output1);
        let block1 = execute_payload_and_update_canonical_chain(&env, payload1, vec![])?;
        env.take_sf_sync_stats();
        let output2 = consensus_output_for_tests(2, 0, 2, false);
        let payload2 =
            TNPayload::new_for_test(block1.recovered_block.clone_sealed_header(), &output2);
        execute_payload_and_update_canonical_chain(&env, payload2, vec![])?;
        let stats = env.take_sf_sync_stats().expect("the provider measures its syncs");
        for segment in ["headers", "transactions", "receipts"] {
            for phase in ["data", "off", "conf", "index"] {
                let timing = stats.phases.get(&(segment, phase)).copied().unwrap_or_default();
                assert_eq!(timing.calls, 1, "{segment} {phase}");
                assert!(timing.max_ns <= timing.total_ns, "{segment} {phase}");
            }
            assert!(!stats.phases.contains_key(&(segment, "csoff")), "{segment} csoff");
        }
        let drained = env.take_sf_sync_stats().expect("still measured");
        assert!(drained.phases.is_empty(), "a take drains the timers");
        Ok(())
    }

    /// Adding sums the calls and the time and keeps the longest call, per segment and phase.
    #[test]
    fn test_sf_sync_stats_add() {
        let mut stats = SfSyncStats::default();
        stats.add("headers", "data", 1, 300, 300);
        stats.add("headers", "data", 2, 500, 400);
        stats.add("receipts", "conf", 1, 90, 90);
        assert_eq!(
            stats.phases.get(&("headers", "data")),
            Some(&PhaseTiming { calls: 3, total_ns: 800, max_ns: 400 })
        );
        assert_eq!(stats.phases.len(), 2);
    }
}
