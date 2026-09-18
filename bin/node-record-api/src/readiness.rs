//! The readiness rule behind `GET /readyz`.
//!
//! The daemon is ready once it has completed its first refresh cycle **and** holds at least one
//! record that is not stale. Both halves matter: before the first cycle the cache is empty by
//! construction (an orchestrator must not route traffic to a replica that would answer with
//! nothing), and a cache whose every record has aged past the staleness threshold means the DHT
//! has been unreachable for at least two intervals, which the site should not present as
//! current. Liveness (`/healthz`) is separate and is simply "the process is running".

use std::fmt;

use crate::cache::RecordCache;

/// Why the daemon is not ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReady {
    /// No refresh cycle has completed yet.
    AwaitingFirstCycle,
    /// Cycles have run but no cached record is fresh; carries how many are cached at all.
    NoFreshRecords {
        /// Records in the cache, all stale (or none).
        cached: usize,
    },
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AwaitingFirstCycle => f.write_str("first refresh cycle has not completed"),
            Self::NoFreshRecords { cached: 0 } => f.write_str("no records cached"),
            Self::NoFreshRecords { cached } => {
                write!(f, "no fresh records: all {cached} cached record(s) are stale")
            }
        }
    }
}

/// A ready daemon's summary, for the `/readyz` body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ready {
    /// Refresh cycles completed so far.
    pub cycles_completed: u64,
    /// Records in the cache.
    pub records_cached: usize,
}

/// Apply the readiness rule to `cache` at `now`.
pub fn check(cache: &RecordCache, now: u64) -> Result<Ready, NotReady> {
    if cache.cycles_completed() == 0 {
        return Err(NotReady::AwaitingFirstCycle);
    }
    if !cache.any_fresh(now) {
        return Err(NotReady::NoFreshRecords { cached: cache.len() });
    }
    Ok(Ready { cycles_completed: cache.cycles_completed(), records_cached: cache.len() })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{test_support::*, CacheConfig, RecordCache};
    use std::{collections::BTreeSet, time::Duration};

    #[test]
    fn readiness_requires_a_cycle_and_a_fresh_record() {
        let a = key(KEY_A);
        let config = CacheConfig {
            record_ttl: Duration::from_secs(3_600),
            absent_cycles_before_evict: 3,
            refresh_interval: Duration::from_secs(300),
        };
        let mut cache = RecordCache::new(config);
        assert_eq!(check(&cache, 10_000), Err(NotReady::AwaitingFirstCycle));

        // a completed cycle that fetched nothing is still not ready
        let set: BTreeSet<_> = [a].into_iter().collect();
        cache.apply_cycle(10_000, 1, &set, vec![(a, Ok(None))]);
        assert_eq!(check(&cache, 10_000), Err(NotReady::NoFreshRecords { cached: 0 }));

        // a fresh record makes it ready; the same record aged past two intervals does not
        cache.apply_cycle(10_300, 2, &set, vec![(a, Ok(Some(verified(a, 1, None))))]);
        assert_eq!(check(&cache, 10_300), Ok(Ready { cycles_completed: 2, records_cached: 1 }));
        assert_eq!(check(&cache, 10_300 + 601), Err(NotReady::NoFreshRecords { cached: 1 }));
        assert_eq!(
            NotReady::NoFreshRecords { cached: 1 }.to_string(),
            "no fresh records: all 1 cached record(s) are stale"
        );
    }
}
