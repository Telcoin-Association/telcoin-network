//! The static files' sync timing per segment, for the exec bench's per-output rows:
//! [`SfSyncStats`], and [`SfSyncSource`], what reads (and resets) it from an env.
//!
//! A persist syncs each static-file segment it wrote: the data file, its offsets (`.off`), a
//! changeset segment's `.csoff` sidecar, the `.conf` written at the commit, and the directory.
//! reth's metrics time the static-file arm and its commit whole (`save_blocks_sf`,
//! `save_blocks_commit_sf`); these split them by segment and phase.
//!
//! The provider measures none of it yet, so [`SfSyncSource::take_sf_sync_stats`] answers `None`
//! for a [`RethEnv`] and the bench reports the section as not measured. Wiring a provider that
//! keeps such timers (a take-and-reset call on the static-file provider) is its body alone: one
//! [`SfSyncStats::add`] per segment and phase the provider timed.

use crate::RethEnv;
use std::collections::BTreeMap;

/// The phases a segment's sync splits into, by their labels: the data file, the offsets file, the
/// changeset offsets sidecar, the config file and the directory.
pub const PHASES: [&str; 5] = ["data", "off", "csoff", "conf", "dir"];

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
    /// `None`: the static-file provider keeps no per-segment sync timers yet (see the module
    /// docs).
    fn take_sf_sync_stats(&self) -> Option<SfSyncStats> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
