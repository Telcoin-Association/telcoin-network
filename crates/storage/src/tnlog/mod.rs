// SPDX-License-Identifier: MIT or Apache-2.0

//! tnlog: a `Database` whose tables share one log (work in progress, built beside the per-table
//! [`crate::tndb`] to compare the two).
//!
//! Every table's records go to one CRC-framed log per database ([`record`]); each table keeps its
//! own B+tree index of positions in it; a manifest ([`manifest`]) names the tables. All writes take
//! one write lock and land as whole batches (a bare write, or a whole transaction, atomic across
//! tables), so every batch end is a commit point and the commit thread ([`commit`]) syncs the log
//! up to the latest without taking any lock a writer takes: one sync per round, however many
//! tables and writers it covers. Readers take no lock. A compaction thread ([`compact`])
//! rewrites the log's live rows into a new generation in the background, once the log has doubled
//! since the last one.

mod commit;
mod compact;
pub mod database;
mod manifest;
mod record;
pub mod split;
mod store;

pub use database::{TnLogDatabase, TnLogOptions};
pub use split::{SplitDatabase, CACHE_AUTO_MIN_BYTES};

/// A table's id in the log's records (never reused).
pub(crate) type TableId = u16;

/// Test-only: what the writers and the commit thread did (process-wide), for the benches.
#[cfg(test)]
pub(crate) mod stats {
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };

    pub(crate) static SYNCS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static SYNC_NS: AtomicU64 = AtomicU64::new(0);
    /// The write lock: acquisitions (waits) and holds, by writers.
    pub(crate) static LOCK_WAITS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static LOCK_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static LOCK_HOLDS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static LOCK_HOLD_NS: AtomicU64 = AtomicU64::new(0);

    /// Compactions switched to, and how long each switch held the write lock.
    pub(crate) static COMPACTIONS: AtomicU64 = AtomicU64::new(0);
    pub(crate) static SWITCH_HOLD_NS: AtomicU64 = AtomicU64::new(0);
    static SWITCH_HOLD_MAX_NS: AtomicU64 = AtomicU64::new(0);

    /// Count one compaction's switch, which held the write lock for `took`.
    pub(crate) fn switch(took: Duration) {
        add(&COMPACTIONS, &SWITCH_HOLD_NS, took);
        SWITCH_HOLD_MAX_NS.fetch_max(took.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Write-lock holds by power-of-two nanosecond bucket, and the longest.
    static HOLD_HIST: [AtomicU64; 40] = [const { AtomicU64::new(0) }; 40];
    static HOLD_MAX_NS: AtomicU64 = AtomicU64::new(0);

    /// Count one event of `count` taking `took` (added to `ns`).
    pub(crate) fn add(count: &AtomicU64, ns: &AtomicU64, took: Duration) {
        count.fetch_add(1, Ordering::Relaxed);
        ns.fetch_add(took.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Count one hold of the write lock.
    pub(crate) fn hold(took: Duration) {
        let ns = took.as_nanos() as u64;
        add(&LOCK_HOLDS, &LOCK_HOLD_NS, took);
        HOLD_HIST[(64 - ns.leading_zeros() as usize).min(39)].fetch_add(1, Ordering::Relaxed);
        HOLD_MAX_NS.fetch_max(ns, Ordering::Relaxed);
    }

    /// What happened since the last take (counts reset).
    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct Stats {
        pub(crate) syncs: u64,
        pub(crate) sync_ns: u64,
        pub(crate) lock_waits: u64,
        pub(crate) lock_wait_ns: u64,
        pub(crate) holds: u64,
        pub(crate) hold_ns: u64,
        /// Upper bounds (ns) of the buckets holding the median and the 99th percentile hold.
        pub(crate) hold_p50_ns: u64,
        pub(crate) hold_p99_ns: u64,
        pub(crate) hold_max_ns: u64,
        pub(crate) compactions: u64,
        pub(crate) switch_hold_ns: u64,
        pub(crate) switch_hold_max_ns: u64,
    }

    pub(crate) fn take() -> Stats {
        let take = |c: &AtomicU64| c.swap(0, Ordering::Relaxed);
        let hist: Vec<u64> = HOLD_HIST.iter().map(take).collect();
        let total: u64 = hist.iter().sum();
        let quantile = |q: f64| {
            let want = (total as f64 * q).ceil() as u64;
            let mut seen = 0;
            for (bucket, n) in hist.iter().enumerate() {
                seen += n;
                if seen >= want.max(1) {
                    return 1_u64 << bucket;
                }
            }
            0
        };
        Stats {
            syncs: take(&SYNCS),
            sync_ns: take(&SYNC_NS),
            lock_waits: take(&LOCK_WAITS),
            lock_wait_ns: take(&LOCK_WAIT_NS),
            holds: take(&LOCK_HOLDS),
            hold_ns: take(&LOCK_HOLD_NS),
            hold_p50_ns: quantile(0.50),
            hold_p99_ns: quantile(0.99),
            hold_max_ns: take(&HOLD_MAX_NS),
            compactions: take(&COMPACTIONS),
            switch_hold_ns: take(&SWITCH_HOLD_NS),
            switch_hold_max_ns: take(&SWITCH_HOLD_MAX_NS),
        }
    }
}

#[cfg(test)]
mod test;
