//! Per-segment timings of the static-file syncs.
//!
//! TN(TN-0119): TN-only, for benches. Each [`StaticFileProvider`](super::StaticFileProvider)
//! accumulates its own, and [`StaticFileProvider::take_sync_stats`] drains them, so a bench reads
//! one persist's split after its commit. Two providers never share them.
//!
//! [`StaticFileProvider::take_sync_stats`]: super::StaticFileProvider::take_sync_stats

use reth_static_file_types::{StaticFileMap, StaticFileSegment};
use std::time::Duration;

/// The calls of one sync phase since the last take.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncPhaseStats {
    /// Number of calls.
    pub calls: u64,
    /// Total time of the calls.
    pub total: Duration,
    /// Longest call.
    pub max: Duration,
}

impl SyncPhaseStats {
    fn record(&mut self, elapsed: Duration) {
        self.calls += 1;
        self.total += elapsed;
        self.max = self.max.max(elapsed);
    }
}

/// One segment's sync phases. `save_blocks` runs the first four in the segment's thread, beside
/// the database writes; `DatabaseProvider::commit` runs `update_index`, before the database
/// commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SegmentSyncStats {
    /// The data file: writing out the buffered rows, then its `fsync`.
    pub data_sync: SyncPhaseStats,
    /// The offsets file: appending the new offsets, then its `fsync`.
    pub offsets_sync: SyncPhaseStats,
    /// The `.csoff` sidecar's `fsync` (changeset segments only).
    pub csoff_sync: SyncPhaseStats,
    /// The `.conf` freeze: temp file write and `fsync`, rename, directory `fsync`.
    pub conf_freeze: SyncPhaseStats,
    /// The reader index update, which reloads the jar's configuration and maps its files.
    pub update_index: SyncPhaseStats,
}

/// One sync phase of [`SegmentSyncStats`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncPhase {
    /// [`SegmentSyncStats::data_sync`].
    Data,
    /// [`SegmentSyncStats::offsets_sync`].
    Offsets,
    /// [`SegmentSyncStats::csoff_sync`].
    Csoff,
    /// [`SegmentSyncStats::conf_freeze`].
    ConfFreeze,
    /// [`SegmentSyncStats::update_index`].
    UpdateIndex,
}

/// [`SegmentSyncStats`] per segment, as [`StaticFileProvider::take_sync_stats`] returns them.
///
/// [`StaticFileProvider::take_sync_stats`]: super::StaticFileProvider::take_sync_stats
#[derive(Debug, Clone, Default)]
pub struct StaticFileSyncStats {
    segments: StaticFileMap<SegmentSyncStats>,
}

impl StaticFileSyncStats {
    /// The stats of `segment` (all zero if it recorded nothing).
    pub fn segment(&self, segment: StaticFileSegment) -> SegmentSyncStats {
        self.segments.get(segment).copied().unwrap_or_default()
    }

    /// The segments that recorded anything, with their stats, in segment order.
    pub fn iter(&self) -> impl Iterator<Item = (StaticFileSegment, SegmentSyncStats)> + '_ {
        self.segments.iter().map(|(segment, stats)| (segment, *stats))
    }

    /// Whether nothing was recorded.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub(crate) fn record(
        &mut self,
        segment: StaticFileSegment,
        phase: SyncPhase,
        elapsed: Duration,
    ) {
        let stats = match self.segments.get_mut(segment) {
            Some(stats) => stats,
            None => {
                self.segments.insert(segment, SegmentSyncStats::default());
                self.segments.get_mut(segment).expect("just inserted")
            }
        };
        let phase = match phase {
            SyncPhase::Data => &mut stats.data_sync,
            SyncPhase::Offsets => &mut stats.offsets_sync,
            SyncPhase::Csoff => &mut stats.csoff_sync,
            SyncPhase::ConfFreeze => &mut stats.conf_freeze,
            SyncPhase::UpdateIndex => &mut stats.update_index,
        };
        phase.record(elapsed);
    }
}
