//! Conservative runtime admission for the dedicated SIGKILL nextest profile.

/// Per-case hard timeout, matching `profile.sigkill` in `.config/nextest.toml`.
pub(crate) const TEST_TIMEOUT_SECS: u64 = 18_000;

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

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
