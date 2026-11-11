//! The writer's I/O and allocation counters, [`IoStats`], which [`crate::Env::io_stats`] reads.
//!
//! The writer keeps running totals in plain fields of its own state as it works (one add next
//! to each event, a clock read only around a whole commit step, a published page's check and a
//! free-list refill), and publishes a copy whenever its state comes back from a transaction or
//! a sync: at each commit, each abort and each [`crate::Env::sync`]. A reader of the counters
//! takes one short lock of that copy and never waits for the writer. They live in memory only,
//! from zero at each open; a read-only environment, which has no writer, always reads zeros.

use std::time::Instant;

/// Cumulative counters of the writer's I/O and page allocation since the environment opened
/// (in memory only: not persisted, zero at each open, and always zero in an environment opened
/// read-only). Times are in nanoseconds.
///
/// The writer publishes them whenever its state comes back to the environment: at the end of
/// every commit (after step 7, see the README's commit protocol), every abort, and every
/// [`crate::Env::sync`]. Between those points the values do not move, even while a write
/// transaction runs. An aborted transaction's work counts: it was done. Take two reads and
/// [`IoStats::since`] for what happened between them, such as one commit's I/O.
///
/// The commit protocol's steps: 1 writes the changed tables' roots into the catalog and saves the
/// free list, 3 stamps the CRCs, 4 syncs the data pages, 5 syncs the file's size if it grew, 6
/// writes and syncs the meta. A [`crate::SyncMode::NoSync`] commit runs steps 1 and 3 (and
/// counts in `save_ns`, `stamp_ns` and `free_list_save_rounds`), then copies its meta as a weak
/// record, unsynced and untimed; the sync that makes its state
/// durable runs steps 4 to 6: an [`crate::Env::sync`], or a later `NoSync` commit that commits
/// durably ([`IoStats::auto_syncs`]), which runs all seven steps itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoStats {
    /// States made durable: each run of steps 4 to 6, by a [`crate::SyncMode::Durable`] commit,
    /// by a [`crate::SyncMode::NoSync`] commit that commits durably ([`Self::auto_syncs`]), or by
    /// an [`crate::Env::sync`] that found a state no steady meta holds (a `NoSync` commit's, or a
    /// weak head the open trusted) to make durable (a sync with nothing pending counts nothing).
    pub commits: u64,
    /// [`crate::SyncMode::NoSync`] commits that deferred their sync: steps 1, 3 and 7, and their
    /// weak meta's copy (no sync).
    pub nosync_commits: u64,
    /// [`crate::SyncMode::NoSync`] commits that committed durably: past
    /// [`crate::EnvOptions::sync_period`] or [`crate::EnvOptions::sync_bytes`], the first that
    /// changes something after an open, or one more than 2^20 txns past the last sync
    /// ([`crate::SyncMode::NoSync`]). Steps 1 to 7, over their own pages and those of the `NoSync`
    /// commits since the last sync. Each also counts in [`Self::commits`], never in
    /// [`Self::nosync_commits`].
    pub auto_syncs: u64,
    /// Of [`Self::commits`], those for which the file had grown since the last durable state,
    /// so its size needed a sync too (by step 5, or by step 4's `fdatasync`).
    pub grown_commits: u64,
    /// Pages step 4 made durable: every page the commits wrote (for a sync, the
    /// [`crate::SyncMode::NoSync`] commits since the last sync; for one of [`Self::auto_syncs`],
    /// those and its own), each overflow run's pages included. Pages, not bytes: an OS page larger
    /// than the page size makes the writes larger.
    pub pages_synced: u64,
    /// Step 4s done by one `msync` of the one range the dirty pages form.
    pub msync_syncs: u64,
    /// Step 4s done by one `fdatasync` of the file (dirty pages in more than one range, or in too
    /// many runs; on macOS these take an `msync` of each range instead, counted here too).
    pub fdatasync_syncs: u64,
    /// The pages the `msync`s of [`Self::msync_syncs`] spanned, from each range's first dirty page
    /// to its last, the clean pages between included.
    pub msync_span_pages: u64,
    /// Runs of adjacent pages among the pages step 4 made durable ([`Self::pages_synced`]),
    /// summed over the runs of step 4, before any gap fill: on Linux about the writes the disk
    /// took for them (a run longer than the device's largest write is split), and with
    /// [`crate::EnvOptions::gap_fill`] on, `sync_runs - gaps_filled`.
    pub sync_runs: u64,
    /// Gaps of clean pages between two runs of [`Self::sync_runs`] that step 4 rewrote with their
    /// own bytes before its sync ([`crate::EnvOptions::gap_fill`]): each joins two runs into one.
    pub gaps_filled: u64,
    /// The pages the gaps of [`Self::gaps_filled`] held: the extra pages step 4 wrote.
    pub gap_pages: u64,
    /// Gaps the gap fill skipped because the page cache did not hold them whole (Linux only: on
    /// macOS every gap is read).
    pub gap_misses: u64,
    /// Commits whose gap fill met a kernel or file system that refuses reads that do not wait for
    /// the disk (Linux's `RWF_NOWAIT`; tmpfs is one) and so turned
    /// [`crate::EnvOptions::gap_fill`] off for the environment, a fixed width and
    /// [`crate::GapFill::Auto`] alike, with a warning: 0 or 1 per open (always 0 on macOS, and
    /// while gap fill is off).
    pub gap_fill_refusals: u64,
    /// Scattered durable steps (more than 16 runs) whose width [`crate::GapFill::Auto`] chose:
    /// the steps it learns from. A step of fewer runs fills nothing under auto.
    pub gap_auto_steps: u64,
    /// Of [`Self::gap_auto_steps`], those at another width than the model's choice: the warm-up's
    /// and the probes' (README, "Automatic width").
    pub gap_auto_probes: u64,
    /// Times the automatic width found the disk's costs changed (steps in a row off its
    /// prediction the same way) and started its warm-up again.
    pub gap_auto_readapts: u64,
    /// The widths of [`Self::gap_auto_steps`], summed: over an interval ([`IoStats::since`]),
    /// `gap_auto_width_sum / gap_auto_steps` is the mean width.
    pub gap_auto_width_sum: u64,
    /// A gauge, not a counter ([`IoStats::since`] keeps the later value): the width the last of
    /// [`Self::gap_auto_steps`] used, in pages (0: no fill).
    pub gap_auto_width: u64,
    /// A gauge: the automatic width's fitted cost per run written (per write the disk takes), in
    /// nanoseconds, at the last of [`Self::gap_auto_steps`] (0 before a fit).
    pub gap_auto_run_ns: u64,
    /// A gauge: its fitted cost per page written, in nanoseconds.
    pub gap_auto_page_ns: u64,
    /// A gauge: its running cost of the fill per gap tried (the fill's timed reads and writes),
    /// in nanoseconds.
    pub gap_auto_gap_ns: u64,
    /// A gauge: 1 when the fit combines the run and page costs by their maximum (a disk capped
    /// on writes and on bytes per second, whichever binds: [`Self::gap_auto_run_ns`] is then the
    /// time per write at the write cap, [`Self::gap_auto_page_ns`] per page at the byte cap), 0
    /// when by their sum (README, "Automatic width").
    pub gap_auto_capped: u64,
    /// Bytes the file grew by: its physical length, which grows geometrically, with real blocks,
    /// ahead of the pages in use ([`Self::pages_from_end`] counts those). A growth that first cut
    /// a tail the file had at open counts from the cut length.
    pub bytes_grown: u64,
    /// Write rounds of the free list's fixed-point save (step 1), summed over the commits: 0 for a
    /// commit that freed nothing and took nothing from the list; a save settles in at most 3
    /// (more is a broken invariant, which fails the commit).
    pub free_list_save_rounds: u64,
    /// Time in step 1: the changed tables' roots into the catalog, then the free list's save.
    pub save_ns: u64,
    /// Time in step 3: the CRCs of every page written, one per tree page and one per run.
    pub stamp_ns: u64,
    /// Time in step 4: the one `msync` or the one `fdatasync` of the data pages (on macOS the
    /// `msync`s that take the `fdatasync`'s place).
    pub data_sync_ns: u64,
    /// Time in step 4's gap fill ([`crate::EnvOptions::gap_fill`]), before its sync: the gaps'
    /// reads and writes, and under [`crate::GapFill::Auto`] the choice of a scattered step's
    /// width.
    pub gap_fill_ns: u64,
    /// Time in step 5: the file's size sync (`fdatasync`) when it grew and step 4 did not already
    /// sync it; a check of a flag otherwise.
    pub size_sync_ns: u64,
    /// Time in step 6: the meta written into its slot and that page's `msync`.
    pub meta_sync_ns: u64,
    /// Copy-on-write copies: a published page copied into a private one on the first change a
    /// transaction makes to it.
    pub cow_copies: u64,
    /// Checks of a published page (its CRC and structure) before its first change, or before a
    /// merge, a collapse or a table's clear or drop trusts its contents. A page this writer
    /// stamped at one of its own commits is not checked again, so this counts only pages it did
    /// not write since the open.
    pub verifies: u64,
    /// Time in the checks of [`Self::verifies`].
    pub verify_ns: u64,
    /// Pages allocated from the pool of reusable pages, single pages and overflow run pages.
    pub pages_from_pool: u64,
    /// Pages allocated at the end of the file (past the transaction's page count), single pages
    /// and overflow run pages; the free list had none to give.
    pub pages_from_end: u64,
    /// Free-list records loaded into the pool, by refills and by overflow runs looking for a fit.
    pub records_loaded: u64,
    /// Time loading free-list records ([`Self::records_loaded`]), each load's reads of the free
    /// tree included.
    pub refill_ns: u64,
    /// Overflow runs allocated, by the step of the run policy that served each: 0 a fit in the
    /// pool, 1 a fit after loading more free records, 2 tail reuse (pool pages that end the file,
    /// plus growth), 3 new pages at the end of the file.
    pub runs_by_step: [u64; 4],
    /// The reuse gate's lag, summed over [`Self::gate_lag_samples`]: at each load of free records,
    /// the writing transaction's id minus the reuse gate (the newest txn whose freed pages may be
    /// reused). In [`crate::SyncMode::Durable`] with no reader on an old snapshot it is 2 (a page
    /// freed by txn N comes back at txn N + 2), 1 only for a new file's first commit (the txn ids
    /// a writer open skips are not counted: a gate from before the open lags by the commits since
    /// it, not by the ids); a reader
    /// held on an old snapshot, or
    /// [`crate::SyncMode::NoSync`] commits since the last sync, raise it. `gate_lag_sum /
    /// gate_lag_samples` of a difference ([`IoStats::since`]) is the mean lag over that interval.
    pub gate_lag_sum: u64,
    /// The loads of free records [`Self::gate_lag_sum`] sampled.
    pub gate_lag_samples: u64,
    /// The largest lag one of [`Self::gate_lag_samples`] saw since the open.
    pub max_gate_lag: u64,
}

impl IoStats {
    /// What happened between `earlier` and `self`, two reads of one environment's counters
    /// (`earlier` first): each counter's difference (saturating, so a pair from different
    /// environments gives no wrap), except [`Self::max_gate_lag`], a maximum since the open, and
    /// the gauges ([`Self::gap_auto_width`], [`Self::gap_auto_run_ns`],
    /// [`Self::gap_auto_page_ns`], [`Self::gap_auto_gap_ns`], [`Self::gap_auto_capped`]), which are
    /// `self`'s.
    pub fn since(&self, earlier: &IoStats) -> IoStats {
        let d = |now: u64, then: u64| now.saturating_sub(then);
        let IoStats {
            commits,
            nosync_commits,
            auto_syncs,
            grown_commits,
            pages_synced,
            msync_syncs,
            fdatasync_syncs,
            msync_span_pages,
            sync_runs,
            gaps_filled,
            gap_pages,
            gap_misses,
            gap_fill_refusals,
            gap_auto_steps,
            gap_auto_probes,
            gap_auto_readapts,
            gap_auto_width_sum,
            gap_auto_width,
            gap_auto_run_ns,
            gap_auto_page_ns,
            gap_auto_gap_ns,
            gap_auto_capped,
            bytes_grown,
            free_list_save_rounds,
            save_ns,
            stamp_ns,
            data_sync_ns,
            gap_fill_ns,
            size_sync_ns,
            meta_sync_ns,
            cow_copies,
            verifies,
            verify_ns,
            pages_from_pool,
            pages_from_end,
            records_loaded,
            refill_ns,
            runs_by_step,
            gate_lag_sum,
            gate_lag_samples,
            max_gate_lag,
        } = *self;
        IoStats {
            commits: d(commits, earlier.commits),
            nosync_commits: d(nosync_commits, earlier.nosync_commits),
            auto_syncs: d(auto_syncs, earlier.auto_syncs),
            grown_commits: d(grown_commits, earlier.grown_commits),
            pages_synced: d(pages_synced, earlier.pages_synced),
            msync_syncs: d(msync_syncs, earlier.msync_syncs),
            fdatasync_syncs: d(fdatasync_syncs, earlier.fdatasync_syncs),
            msync_span_pages: d(msync_span_pages, earlier.msync_span_pages),
            sync_runs: d(sync_runs, earlier.sync_runs),
            gaps_filled: d(gaps_filled, earlier.gaps_filled),
            gap_pages: d(gap_pages, earlier.gap_pages),
            gap_misses: d(gap_misses, earlier.gap_misses),
            gap_fill_refusals: d(gap_fill_refusals, earlier.gap_fill_refusals),
            gap_auto_steps: d(gap_auto_steps, earlier.gap_auto_steps),
            gap_auto_probes: d(gap_auto_probes, earlier.gap_auto_probes),
            gap_auto_readapts: d(gap_auto_readapts, earlier.gap_auto_readapts),
            gap_auto_width_sum: d(gap_auto_width_sum, earlier.gap_auto_width_sum),
            gap_auto_width,
            gap_auto_run_ns,
            gap_auto_page_ns,
            gap_auto_gap_ns,
            gap_auto_capped,
            bytes_grown: d(bytes_grown, earlier.bytes_grown),
            free_list_save_rounds: d(free_list_save_rounds, earlier.free_list_save_rounds),
            save_ns: d(save_ns, earlier.save_ns),
            stamp_ns: d(stamp_ns, earlier.stamp_ns),
            data_sync_ns: d(data_sync_ns, earlier.data_sync_ns),
            gap_fill_ns: d(gap_fill_ns, earlier.gap_fill_ns),
            size_sync_ns: d(size_sync_ns, earlier.size_sync_ns),
            meta_sync_ns: d(meta_sync_ns, earlier.meta_sync_ns),
            cow_copies: d(cow_copies, earlier.cow_copies),
            verifies: d(verifies, earlier.verifies),
            verify_ns: d(verify_ns, earlier.verify_ns),
            pages_from_pool: d(pages_from_pool, earlier.pages_from_pool),
            pages_from_end: d(pages_from_end, earlier.pages_from_end),
            records_loaded: d(records_loaded, earlier.records_loaded),
            refill_ns: d(refill_ns, earlier.refill_ns),
            runs_by_step: std::array::from_fn(|i| d(runs_by_step[i], earlier.runs_by_step[i])),
            gate_lag_sum: d(gate_lag_sum, earlier.gate_lag_sum),
            gate_lag_samples: d(gate_lag_samples, earlier.gate_lag_samples),
            max_gate_lag,
        }
    }
}

/// The nanoseconds since `started`, for the counters' times.
#[inline]
pub(crate) fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

/// The nanoseconds since `*since`, which then moves to now: one clock read per step of a
/// sequence of steps timed back to back.
#[inline]
pub(crate) fn lap(since: &mut Instant) -> u64 {
    let now = Instant::now();
    let ns = u64::try_from(now.duration_since(*since).as_nanos()).unwrap_or(u64::MAX);
    *since = now;
    ns
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `since` subtracts every counter but keeps the later maximum and the later gauges.
    #[test]
    fn since_subtracts_every_counter_but_the_maximum() {
        let earlier = IoStats {
            commits: 1,
            auto_syncs: 1,
            pages_synced: 10,
            runs_by_step: [1, 2, 3, 4],
            max_gate_lag: 7,
            gap_auto_steps: 2,
            gap_auto_width_sum: 8,
            gap_auto_width: 4,
            gap_auto_run_ns: 50,
            ..IoStats::default()
        };
        let later = IoStats {
            commits: 3,
            auto_syncs: 2,
            pages_synced: 25,
            runs_by_step: [2, 2, 5, 9],
            max_gate_lag: 9,
            verify_ns: 4,
            gap_auto_steps: 5,
            gap_auto_width_sum: 20,
            gap_auto_width: 2,
            gap_auto_run_ns: 40,
            gap_auto_page_ns: 3,
            gap_auto_gap_ns: 9,
            gap_auto_capped: 1,
            ..IoStats::default()
        };
        let d = later.since(&earlier);
        assert_eq!(
            d,
            IoStats {
                commits: 2,
                auto_syncs: 1,
                pages_synced: 15,
                runs_by_step: [1, 0, 2, 5],
                max_gate_lag: 9,
                verify_ns: 4,
                gap_auto_steps: 3,
                gap_auto_width_sum: 12,
                gap_auto_width: 2,
                gap_auto_run_ns: 40,
                gap_auto_page_ns: 3,
                gap_auto_gap_ns: 9,
                gap_auto_capped: 1,
                ..IoStats::default()
            }
        );
        assert_eq!(earlier.since(&later).commits, 0, "saturating");
    }
}
