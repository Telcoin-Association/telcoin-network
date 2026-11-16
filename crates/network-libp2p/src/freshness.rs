//! Local ordering of authenticated node records, independent of their signed bytes.
//!
//! Timestamps at most five minutes ahead of the local Unix clock retain signed ordering.
//! Larger values are usable, but their local comparison ceiling is fixed at admission.
//! A corrected timestamp within the tolerance repairs a clamped entry immediately. If both
//! records are clamped, a lower signed timestamp can replace it once that ceiling passes.
//! Identical signed timestamps never advance freshness, including repeated future records.

use serde::{Deserialize, Serialize};
use tn_types::TimestampSec;

/// Maximum future skew used for local comparison, in seconds.
const FUTURE_SKEW: TimestampSec = 5 * 60;

/// Unsigned, local freshness metadata stored alongside an unchanged signed record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RecordTimestamp {
    /// Original timestamp covered by the record's signature.
    signed: TimestampSec,
    /// Comparison ceiling fixed at admission, never renewed by a replay or restart.
    effective: TimestampSec,
}

impl RecordTimestamp {
    /// Admit a timestamp using the local Unix clock without changing the signed payload.
    pub(crate) fn admit(signed: TimestampSec, observed: TimestampSec) -> Self {
        Self { signed, effective: signed.min(observed.saturating_add(FUTURE_SKEW)) }
    }

    /// Recover a legacy row without admission metadata.
    ///
    /// An implausible legacy value has no trustworthy local admission time. Giving it a zero
    /// comparison ceiling makes the next distinct authenticated record able to repair it.
    pub(crate) fn legacy(signed: TimestampSec, observed: TimestampSec) -> Self {
        let admitted = Self::admit(signed, observed);
        if admitted.is_clamped() {
            Self { signed, effective: 0 }
        } else {
            admitted
        }
    }

    /// Whether admission replaced the signed timestamp with a bounded local ceiling.
    fn is_clamped(self) -> bool {
        self.effective < self.signed
    }

    /// Whether this metadata belongs to the specified signed timestamp.
    pub(crate) fn matches(self, signed: TimestampSec) -> bool {
        self.signed == signed
    }

    /// Decide whether this candidate supersedes the retained record at `observed`.
    ///
    /// Ordinary records stay strictly timestamp-monotonic. Repair deliberately resets ordering
    /// for an out-of-policy cached value, since its signed clock is no longer a freshness oracle.
    pub(crate) fn supersedes(self, cached: Self, observed: TimestampSec) -> bool {
        let ceiling = observed.saturating_add(FUTURE_SKEW);
        let cached = if cached.signed <= ceiling {
            // Restore strict signed ordering when the local clock catches up.
            Self { signed: cached.signed, effective: cached.signed }
        } else if cached.effective > ceiling {
            // A backward clock adjustment can invalidate any retained local ceiling.
            Self::legacy(cached.signed, observed)
        } else {
            cached
        };
        self.signed != cached.signed
            && if !cached.is_clamped() {
                self.signed > cached.signed
            } else if self.signed <= ceiling {
                true
            } else {
                self.signed > cached.signed || observed >= cached.effective
            }
    }
}

#[cfg(test)]
mod tests {
    use super::RecordTimestamp;

    /// Ordinary updates advance, while older and equal signed timestamps do not.
    #[test]
    fn ordinary_ordering_rejects_replays() {
        let cached = RecordTimestamp::admit(1_000, 1_000);
        assert!(cached.matches(1_000));
        assert!(!cached.matches(999));
        assert!(RecordTimestamp::admit(1_001, 1_000).supersedes(cached, 1_000));
        assert!(!RecordTimestamp::admit(999, 1_000).supersedes(cached, 1_000));
        assert!(!RecordTimestamp::admit(1_000, 2_000).supersedes(cached, 2_000));
    }

    /// A properly clocked correction immediately repairs an extreme signed value.
    #[test]
    fn corrected_record_repairs_future_timestamp() {
        let cached = RecordTimestamp::admit(u64::MAX, 1_000);
        assert!(RecordTimestamp::admit(1_001, 1_001).supersedes(cached, 1_001));
        assert!(!RecordTimestamp::admit(u64::MAX, 2_000).supersedes(cached, 2_000));
    }

    /// A lagging clock admits honest future records and bounds recovery of a lower correction.
    #[test]
    fn lagging_clock_has_bounded_recovery() {
        let cached = RecordTimestamp::admit(u64::MAX, 1_000);
        let correction = RecordTimestamp::admit(10_000, 1_299);
        assert!(!correction.supersedes(cached, 1_299));
        assert!(correction.supersedes(cached, 1_300));
        assert!(RecordTimestamp::admit(10_001, 1_300).supersedes(correction, 1_300));
    }

    /// The tolerance boundary is inclusive and its arithmetic cannot wrap.
    #[test]
    fn skew_boundary_and_overflow() {
        assert!(!RecordTimestamp::admit(1_300, 1_000).is_clamped());
        assert!(RecordTimestamp::admit(1_301, 1_000).is_clamped());
        assert!(!RecordTimestamp::admit(u64::MAX, u64::MAX).is_clamped());
    }

    /// Legacy poison and a backward local clock adjustment both have a repair path.
    #[test]
    fn legacy_and_clock_rollback_repair() {
        let correction = RecordTimestamp::admit(10_000, 1_000);
        assert!(correction.supersedes(RecordTimestamp::legacy(u64::MAX, 1_000), 1_000));
        assert!(correction.supersedes(RecordTimestamp::admit(20_000, 20_000), 1_000));
        assert!(correction.supersedes(RecordTimestamp::admit(20_000, 5_000), 1_000));
    }

    /// Once a retained signed timestamp is plausible, ordinary replay protection resumes.
    #[test]
    fn clock_catchup_restores_strict_ordering() {
        let cached = RecordTimestamp::admit(10_000, 1_000);
        assert!(!RecordTimestamp::admit(9_999, 10_000).supersedes(cached, 10_000));
        assert!(RecordTimestamp::admit(10_001, 10_000).supersedes(cached, 10_000));
    }
}
