//! Epoch-boundary derivation and the refresh scheduler.
//!
//! The committee changes at every epoch boundary, so a refresh that runs shortly after the
//! boundary picks up the new committee's records as soon as they are published. The boundary is
//! derived exactly as the node derives it (`RethEnv::epoch_state_at_header` in
//! `crates/tn-reth/src/env/epoch.rs`): the epoch started at the timestamp of the previous
//! epoch's closing block, `blockHeight - 1`, and ends `epochDuration` seconds later.

use std::{pin::Pin, time::Duration};

use tokio::time::{interval, Instant, Interval, MissedTickBehavior, Sleep};

/// A far-future delay used to park the boundary timer when no boundary is known. Long enough
/// that no deployment reaches it; `sleep_until` saturates rather than overflows if it ever did.
const PARKED: Duration = Duration::from_secs(60 * 60 * 24 * 365);

/// What the live key source learned about the current epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochInfoSummary {
    /// The current epoch id from the `ConsensusRegistry`.
    pub epoch_id: u32,
    /// When the current epoch ends, as a unix timestamp in seconds.
    pub next_boundary_unix: u64,
}

/// The unix timestamp at which an epoch that started at `epoch_start_ts` and lasts
/// `epoch_duration_secs` ends. Saturates rather than wraps on absurd inputs.
///
/// `epoch_start_ts` is the timestamp of the previous epoch's closing block
/// (`EpochInfo.blockHeight - 1`), which is what the node uses as the epoch's start.
pub fn next_boundary(epoch_start_ts: u64, epoch_duration_secs: u64) -> u64 {
    epoch_start_ts.saturating_add(epoch_duration_secs)
}

/// How long to wait from `now_unix` before refreshing for the boundary at `boundary_unix`: the
/// boundary plus `grace` (records are re-published after the node's own boundary handling, so
/// refreshing at the boundary itself would race that). A boundary already in the past clamps to
/// zero, so the refresh runs immediately rather than never.
pub fn boundary_wake_delay(now_unix: u64, boundary_unix: u64, grace: Duration) -> Duration {
    let wake_at = boundary_unix.saturating_add(grace.as_secs());
    Duration::from_secs(wake_at.saturating_sub(now_unix))
}

/// Why the scheduler woke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wake {
    /// The regular refresh interval elapsed.
    Interval,
    /// The armed epoch boundary (plus grace) was reached.
    EpochBoundary,
}

/// Decides when the next refresh cycle runs: a plain interval, plus a one-shot timer armed at the
/// next epoch boundary (plus grace) whenever the live key source reports one.
///
/// The interval skips missed ticks rather than bursting: a cycle that overruns the interval must
/// not be followed by back-to-back catch-up cycles, each of which dials every bootstrap peer.
/// The boundary timer is parked far in the future until [`Self::arm_boundary`] sets it and is
/// re-parked once it fires, so a single boundary wakes the loop exactly once.
#[derive(Debug)]
pub struct RefreshScheduler {
    /// The regular refresh cadence.
    ticker: Interval,
    /// The one-shot boundary timer, parked when no boundary is armed.
    boundary: Pin<Box<Sleep>>,
    /// Added to the boundary before waking.
    grace: Duration,
}

impl RefreshScheduler {
    /// Build a scheduler whose interval first fires one `refresh_interval` from now (the caller
    /// runs the first cycle itself before waiting).
    pub fn new(refresh_interval: Duration, grace: Duration) -> Self {
        let mut ticker = interval(refresh_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // consume the immediate first tick so the first wait is one interval out
        ticker.reset();
        Self { ticker, boundary: Box::pin(tokio::time::sleep(PARKED)), grace }
    }

    /// Arm the boundary timer for `boundary_unix` (as of `now_unix`), or park it when `None`. A
    /// boundary already in the past fires on the next wait.
    pub fn arm_boundary(&mut self, now_unix: u64, boundary_unix: Option<u64>) {
        let delay = boundary_unix
            .map_or(PARKED, |boundary| boundary_wake_delay(now_unix, boundary, self.grace));
        self.boundary.as_mut().reset(Instant::now() + delay);
    }

    /// Wait for the sooner of the interval tick and the armed boundary.
    pub async fn wait(&mut self) -> Wake {
        tokio::select! {
            _ = self.ticker.tick() => Wake::Interval,
            () = self.boundary.as_mut() => {
                // one boundary wakes the loop once; the next cycle re-arms from fresh epoch info
                self.boundary.as_mut().reset(Instant::now() + PARKED);
                Wake::EpochBoundary
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_is_start_plus_duration() {
        assert_eq!(next_boundary(1_700_000_000, 28_800), 1_700_028_800);
        assert_eq!(next_boundary(u64::MAX, 1), u64::MAX, "saturates instead of wrapping");
    }

    #[test]
    fn wake_delay_is_boundary_plus_grace_from_now() {
        let grace = Duration::from_secs(30);
        assert_eq!(boundary_wake_delay(1_000, 1_100, grace), Duration::from_secs(130));
        assert_eq!(boundary_wake_delay(1_000, 1_000, grace), grace);
    }

    #[test]
    fn past_boundary_clamps_to_now() {
        let grace = Duration::from_secs(30);
        // boundary plus grace already elapsed: refresh immediately, not never
        assert_eq!(boundary_wake_delay(5_000, 1_000, grace), Duration::ZERO);
        assert_eq!(boundary_wake_delay(1_031, 1_000, grace), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn interval_fires_when_no_boundary_is_armed() {
        let mut scheduler =
            RefreshScheduler::new(Duration::from_secs(300), Duration::from_secs(30));
        let start = Instant::now();
        assert_eq!(scheduler.wait().await, Wake::Interval);
        assert_eq!(start.elapsed(), Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn sooner_boundary_wins_over_interval() {
        let mut scheduler =
            RefreshScheduler::new(Duration::from_secs(300), Duration::from_secs(30));
        // boundary 60s out plus 30s grace: wakes at 90s, well before the 300s tick
        scheduler.arm_boundary(1_000, Some(1_060));
        let start = Instant::now();
        assert_eq!(scheduler.wait().await, Wake::EpochBoundary);
        assert_eq!(start.elapsed(), Duration::from_secs(90));
        // the boundary is consumed: the next wake is the interval, at 300s from construction
        assert_eq!(scheduler.wait().await, Wake::Interval);
        assert_eq!(start.elapsed(), Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn later_boundary_loses_to_interval() {
        let mut scheduler =
            RefreshScheduler::new(Duration::from_secs(300), Duration::from_secs(30));
        scheduler.arm_boundary(1_000, Some(1_000 + 3_600));
        let start = Instant::now();
        assert_eq!(scheduler.wait().await, Wake::Interval);
        assert_eq!(start.elapsed(), Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn past_boundary_wakes_immediately() {
        let mut scheduler =
            RefreshScheduler::new(Duration::from_secs(300), Duration::from_secs(30));
        scheduler.arm_boundary(9_000, Some(1_000));
        let start = Instant::now();
        assert_eq!(scheduler.wait().await, Wake::EpochBoundary);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn disarming_parks_the_boundary_timer() {
        let mut scheduler =
            RefreshScheduler::new(Duration::from_secs(300), Duration::from_secs(30));
        scheduler.arm_boundary(1_000, Some(1_010));
        scheduler.arm_boundary(1_000, None);
        assert_eq!(scheduler.wait().await, Wake::Interval);
    }
}
