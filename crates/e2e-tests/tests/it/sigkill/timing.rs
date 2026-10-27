//! Conservative runtime admission for the dedicated SIGKILL nextest profile.
//!
//! The short-run defaults live here, next to the restart window they must satisfy, so the unit
//! tests below read the same numbers `Timing::from_env` uses.

/// Per-case hard timeout, matching `profile.sigkill` in `.config/nextest.toml`.
pub(crate) const TEST_TIMEOUT_SECS: u64 = 18_000;

/// Short-run epoch duration in seconds. Every wait in a case is a multiple of it.
///
/// The node needs no particular epoch length: the other e2e cases run 5-second epochs and the
/// case's own floor is five seconds. Ten seconds keeps one steady-state epoch under load before
/// the kill, while the three epochs before the kill take 30 seconds instead of 90.
pub(crate) const SHORT_RUN_EPOCH_SECS: u64 = 10;

/// Short-run late-kill offset before the boundary, in seconds.
pub(crate) const SHORT_RUN_BEFORE_CLOSE_SECS: u64 = 2;

/// Short-run minimum downtime in seconds, inside the window of
/// [`restarts_land_two_epochs_after_kill`].
///
/// The before-close restart lands 16 seconds after the killed epoch's close and the mid-epoch
/// restart 13 seconds after it. Both are inside the second epoch after the kill, which spans 10
/// to 20 seconds after that close.
pub(crate) const SHORT_RUN_DOWNTIME_SECS: u64 = 18;

/// Short-run catch-up deadline after restart, in seconds.
pub(crate) const SHORT_RUN_RECOVERY_SECS: u64 = 180;

/// Cover all bounded waits plus RPC startup and capture overhead.
///
/// Epoch waits consume at most 3 + 3 + 2 + 1 epochs. Downtime adds its own deadline,
/// and recovery, transaction receipts, and subsequent chain progress each get one recovery window.
pub(crate) fn required_secs(epoch: u64, downtime: u64, recovery: u64) -> Option<u64> {
    epoch
        .checked_mul(9)?
        .checked_add(downtime)?
        .checked_add(recovery.checked_mul(3)?)?
        .checked_add(300)
}

/// Whether both cases restart inside the second epoch after the killed one.
///
/// Let `T` be the killed epoch's close, `E` the epoch, `b` the late-kill offset and `D` the
/// downtime. The before-close case restarts at `T - b + D` and the mid-epoch case at
/// `T - E / 2 + D`. The second epoch after the kill spans `(T + E, T + 2E)`. A restart inside it
/// finds an epoch record newer than the node's local epoch, so `node_mode_from_epoch_record`
/// starts the node as `CvvInactive` and it rejoins through catch-up. That is the recovery path the
/// short run exercises. Both restarts fall inside the span exactly when `E + b < D < 2E + b` and
/// `1.5E < D < 2.5E`; with `b < E / 2` that is `1.5E < D < 2E + b`. The arithmetic is doubled to
/// stay in integers. `None` on overflow.
///
/// The check is advisory. Fleet B (`1200 / 59 / 300`) restarts in the epoch right after the kill,
/// where the newest epoch record matches the local epoch and the node restarts as `CvvActive`.
/// `Timing::from_env` logs which path the settings select and rejects neither.
pub(crate) fn restarts_land_two_epochs_after_kill(
    epoch: u64,
    before_close: u64,
    downtime: u64,
) -> Option<bool> {
    let doubled_downtime = downtime.checked_mul(2)?;
    let doubled_offset = before_close.checked_mul(2)?;
    let lower = epoch.checked_mul(3)?.max(epoch.checked_mul(2)?.checked_add(doubled_offset)?);
    let upper = epoch.checked_mul(5)?.min(epoch.checked_mul(4)?.checked_add(doubled_offset)?);
    Some(lower < doubled_downtime && doubled_downtime < upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The short-run defaults use a small fraction of the watchdog budget.
    #[test]
    fn short_run_defaults_fit_the_sigkill_profile() {
        let required =
            required_secs(SHORT_RUN_EPOCH_SECS, SHORT_RUN_DOWNTIME_SECS, SHORT_RUN_RECOVERY_SECS);
        assert_eq!(required, Some(948));
        assert!(required.is_some_and(|secs| secs < TEST_TIMEOUT_SECS));
    }

    /// The short-run defaults restart both cases inside the second epoch after the kill.
    #[test]
    fn short_run_defaults_restart_two_epochs_after_the_kill() {
        assert!(SHORT_RUN_BEFORE_CLOSE_SECS < SHORT_RUN_EPOCH_SECS / 2);
        assert_eq!(
            restarts_land_two_epochs_after_kill(
                SHORT_RUN_EPOCH_SECS,
                SHORT_RUN_BEFORE_CLOSE_SECS,
                SHORT_RUN_DOWNTIME_SECS
            ),
            Some(true)
        );
    }

    /// A downtime on either edge of the window lands a restart on an epoch close.
    #[test]
    fn window_edges_are_excluded() {
        // 1.5E: the mid-epoch restart lands exactly on the next close. The previous 30 / 2 / 45
        // defaults and the mutation steps' earlier 10 / 2 / 15 settings both sat here.
        assert_eq!(restarts_land_two_epochs_after_kill(30, 2, 45), Some(false));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 2, 15), Some(false));
        // 2E + b: the before-close restart lands exactly on the close two epochs later.
        assert_eq!(restarts_land_two_epochs_after_kill(10, 2, 22), Some(false));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 2, 16), Some(true));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 2, 21), Some(true));
    }

    /// An offset of half an epoch or more moves both edges: `E + b` below and `2.5E` above.
    #[test]
    fn large_offset_moves_both_edges() {
        assert_eq!(restarts_land_two_epochs_after_kill(10, 8, 16), Some(false));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 8, 19), Some(true));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 8, 24), Some(true));
        assert_eq!(restarts_land_two_epochs_after_kill(10, 8, 25), Some(false));
    }

    /// Fleet B restarts in the epoch right after the kill, the optimistic `CvvActive` path.
    #[test]
    fn fleet_timing_restarts_in_the_next_epoch() {
        assert_eq!(restarts_land_two_epochs_after_kill(1200, 59, 300), Some(false));
    }

    /// Fleet B must fit even with the documented 30-minute recovery window.
    #[test]
    fn fleet_timing_fits_the_sigkill_profile() {
        assert_eq!(required_secs(1200, 300, 1800), Some(16_800));
        assert!(required_secs(1200, 300, 1800).is_some_and(|secs| secs < TEST_TIMEOUT_SECS));
        assert!(required_secs(1200, 300, 180).is_some_and(|secs| secs > 2160));
    }

    /// Settings beyond the watchdog budget and arithmetic overflow cannot be admitted.
    #[test]
    fn oversized_and_overflowing_settings_do_not_fit() {
        assert!(required_secs(2000, 300, 180).is_some_and(|secs| secs >= TEST_TIMEOUT_SECS));
        assert_eq!(required_secs(u64::MAX, 300, 180), None);
        assert_eq!(required_secs(30, u64::MAX, 180), None);
        assert_eq!(required_secs(30, 45, u64::MAX), None);
        assert_eq!(restarts_land_two_epochs_after_kill(u64::MAX, 2, 18), None);
        assert_eq!(restarts_land_two_epochs_after_kill(10, 2, u64::MAX), None);
    }
}
