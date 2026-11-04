//! Fixed-size diagnostic windows, with time supplied by the caller for deterministic tests.

use std::time::{Duration, Instant};

/// Maximum detailed diagnostics per class in one reporting window.
pub(super) const BURST: u64 = 4;
/// Reporting interval shared by every fixed event class.
pub(crate) const INTERVAL: Duration = Duration::from_secs(60);

/// Counts for a completed observation window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Summary {
    /// Events whose detailed diagnostic was emitted.
    emitted: u64,
    /// Events whose detailed diagnostic was skipped.
    suppressed: u64,
    /// Actual elapsed observation time, including any scheduler delay.
    elapsed: Duration,
}

impl Summary {
    /// Number of detailed diagnostics admitted in the completed window.
    pub(super) const fn emitted(self) -> u64 {
        self.emitted
    }

    /// Number of detailed diagnostics suppressed in the completed window.
    pub(super) const fn suppressed(self) -> u64 {
        self.suppressed
    }

    /// Actual duration of the completed observation window.
    pub(super) const fn elapsed(self) -> Duration {
        self.elapsed
    }
}

/// Constant storage for one event class, independent of event payloads and identities.
#[derive(Debug, Default)]
pub(super) struct Window {
    /// Start of the active window, absent when idle.
    start: Option<Instant>,
    /// Detailed diagnostics admitted in this window.
    emitted: u64,
    /// Detailed diagnostics suppressed in this window.
    suppressed: u64,
}

impl Window {
    /// Construct an idle window for static initialization.
    pub(super) const fn new() -> Self {
        Self { start: None, emitted: 0, suppressed: 0 }
    }

    /// Finish an elapsed window, reporting suppression even if no further events arrive.
    pub(super) fn flush(&mut self, now: Instant) -> Option<Summary> {
        self.start.filter(|start| now.saturating_duration_since(*start) >= INTERVAL).and_then(
            |start| {
                let previous = std::mem::take(self);
                (previous.suppressed > 0).then_some(Summary {
                    emitted: previous.emitted,
                    suppressed: previous.suppressed,
                    elapsed: now.saturating_duration_since(start),
                })
            },
        )
    }

    /// Count an event and decide whether to evaluate its diagnostic fields.
    ///
    /// The current event belongs to the new window when it lands exactly on the boundary.
    pub(super) fn observe(&mut self, now: Instant) -> (bool, Option<Summary>) {
        let summary = self.flush(now);
        self.start.get_or_insert(now);
        let emit = self.emitted < BURST;
        if emit {
            self.emitted += 1;
        } else {
            self.suppressed = self.suppressed.saturating_add(1);
        }
        (emit, summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A burst preserves details, then suppresses until the exact reporting boundary.
    #[test]
    fn burst_suppression_summary_and_recovery() {
        let start = Instant::now();
        let mut window = Window::new();
        (0..BURST).for_each(|_| assert_eq!(window.observe(start), (true, None)));
        (0..1_000).for_each(|_| assert_eq!(window.observe(start), (false, None)));
        assert_eq!(window.flush(start + INTERVAL - Duration::from_nanos(1)), None);
        assert_eq!(
            window.observe(start + INTERVAL),
            (true, Some(Summary { emitted: BURST, suppressed: 1_000, elapsed: INTERVAL }))
        );
        assert_eq!(window.emitted, 1);
        assert_eq!(window.suppressed, 0);
        assert_eq!(window.observe(start + INTERVAL), (true, None));
    }

    /// A quiet stream retains each diagnostic and an idle tick drains a suppressed tail once.
    #[test]
    fn idle_summary_and_low_rate_diagnostics() {
        let start = Instant::now();
        let mut window = Window::new();
        (0..10).for_each(|step| {
            assert_eq!(window.observe(start + INTERVAL * step), (true, None));
        });
        (0..BURST).for_each(|_| {
            window.observe(start + INTERVAL * 9);
        });
        assert_eq!(
            window.flush(start + INTERVAL * 11),
            Some(Summary { emitted: BURST, suppressed: 1, elapsed: INTERVAL * 2 })
        );
        assert_eq!(window.flush(start + INTERVAL * 11), None);
        assert_eq!(window.observe(start + INTERVAL * 11), (true, None));
    }

    /// A stalled or backward clock never panics, underflows, or refills a live burst.
    #[test]
    fn clock_and_counter_boundaries() {
        let start = Instant::now();
        let mut window = Window::new();
        (0..BURST).for_each(|_| {
            window.observe(start);
        });
        window.suppressed = u64::MAX;
        assert_eq!(window.observe(start - Duration::from_secs(1)), (false, None));
        assert_eq!(window.suppressed, u64::MAX);
        assert_eq!(
            window.flush(start + INTERVAL).map(|summary| summary.suppressed),
            Some(u64::MAX)
        );
    }
}
