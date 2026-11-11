//! The open reth database's own counters, read the same way on either backend: [`db_counters`]
//! reads the backend's cumulative page and write counters as [`DbCounters`], and [`db_page_size`]
//! the page size the database file was created with. Callers name no backend; the split between
//! the builds lives here.
//!
//! - MDBX: libmdbx's page-operation statistics (`Info::page_ops`, from `mdbx_env_info_ex`), named
//!   `mdbx_<field>`: `newly` (pages added), `cow` (pages copied to change them), `clone`, `split`,
//!   `merge`, `spill`, `unspill`, `wops` (write calls to the disk, not pages), `msync`, `fsync`,
//!   `prefault` and `mincore`. They live in the lock file, counting from its creation (the first
//!   open), and move while a write transaction works, not only at its commit. One read is one
//!   `mdbx_env_info_ex`: the meta pages, the lock file's counters and its reader table, with no
//!   lock and no transaction.
//! - tn-lmdb: the engine's I/O and allocation counters (`tn_lmdb_reth::IoStats`), named after its
//!   fields with a `tn_lmdb_` prefix (`tn_lmdb_commits`, `tn_lmdb_pages_synced`,
//!   `tn_lmdb_cow_copies`, ...), `runs_by_step` as `tn_lmdb_runs_by_step_0` to
//!   `tn_lmdb_runs_by_step_3`, the times in nanoseconds (`_ns`). They count from zero at each open
//!   and move only at a commit, an abort or a sync. One read is one short lock of the writer's
//!   published copy, never a wait for an open write transaction.
//!
//! Either read is cheap enough to take before and after every output. [`db_page_size`] costs
//! more on MDBX (`mdbx_env_stat_ex`, a read transaction over the table catalog): read it once per
//! open.

use crate::RethDb;

/// MDBX's counter names, in the order of [`DbCounters`]'s values: libmdbx's `PageOps` fields.
#[cfg(feature = "mdbx")]
const MDBX_NAMES: [&str; 12] = [
    "mdbx_newly",
    "mdbx_cow",
    "mdbx_clone",
    "mdbx_split",
    "mdbx_merge",
    "mdbx_spill",
    "mdbx_unspill",
    "mdbx_wops",
    "mdbx_msync",
    "mdbx_fsync",
    "mdbx_prefault",
    "mdbx_mincore",
];

/// One read of the open database's cumulative counters ([`db_counters`]; the module docs list
/// them per backend). Two reads and [`Self::since`] give what happened between them, such as one
/// output's writes; [`Self::entries`] names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbCounters {
    /// libmdbx's page-operation statistics, in [`MDBX_NAMES`]'s order.
    #[cfg(feature = "mdbx")]
    values: [u64; MDBX_NAMES.len()],
    /// tn-lmdb's I/O and allocation counters.
    #[cfg(not(feature = "mdbx"))]
    io: tn_lmdb_reth::IoStats,
}

impl DbCounters {
    /// What happened between `earlier` and `self`, two reads of one database (`earlier` first):
    /// each counter's difference, saturating, so a pair from two databases does not wrap.
    #[cfg(feature = "mdbx")]
    pub fn since(&self, earlier: &Self) -> Self {
        Self { values: std::array::from_fn(|i| self.values[i].saturating_sub(earlier.values[i])) }
    }

    /// What happened between `earlier` and `self`, two reads of one database (`earlier` first):
    /// tn-lmdb's `IoStats::since`, each counter's saturating difference except those of
    /// [`NOT_DIFFERENCES`] (`tn_lmdb_max_gate_lag`, a maximum since the open, and the automatic
    /// gap fill's gauges), which are `self`'s.
    #[cfg(not(feature = "mdbx"))]
    pub fn since(&self, earlier: &Self) -> Self {
        Self { io: self.io.since(&earlier.io) }
    }

    /// The counters as `(name, value)`, in this build's fixed order (see the module docs).
    #[cfg(feature = "mdbx")]
    pub fn entries(&self) -> Vec<(&'static str, u64)> {
        MDBX_NAMES.into_iter().zip(self.values).collect()
    }

    /// The counters as `(name, value)`, in this build's fixed order (see the module docs).
    #[cfg(not(feature = "mdbx"))]
    pub fn entries(&self) -> Vec<(&'static str, u64)> {
        // Destructured whole, so a counter the engine adds fails to compile here until it is named.
        let tn_lmdb_reth::IoStats {
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
            runs_by_step: [runs_0, runs_1, runs_2, runs_3],
            gate_lag_sum,
            gate_lag_samples,
            max_gate_lag,
        } = self.io;
        vec![
            ("tn_lmdb_commits", commits),
            ("tn_lmdb_nosync_commits", nosync_commits),
            ("tn_lmdb_auto_syncs", auto_syncs),
            ("tn_lmdb_grown_commits", grown_commits),
            ("tn_lmdb_pages_synced", pages_synced),
            ("tn_lmdb_msync_syncs", msync_syncs),
            ("tn_lmdb_fdatasync_syncs", fdatasync_syncs),
            ("tn_lmdb_msync_span_pages", msync_span_pages),
            ("tn_lmdb_sync_runs", sync_runs),
            ("tn_lmdb_gaps_filled", gaps_filled),
            ("tn_lmdb_gap_pages", gap_pages),
            ("tn_lmdb_gap_misses", gap_misses),
            ("tn_lmdb_gap_auto_steps", gap_auto_steps),
            ("tn_lmdb_gap_auto_probes", gap_auto_probes),
            ("tn_lmdb_gap_auto_readapts", gap_auto_readapts),
            ("tn_lmdb_gap_auto_width_sum", gap_auto_width_sum),
            ("tn_lmdb_gap_auto_width", gap_auto_width),
            ("tn_lmdb_gap_auto_run_ns", gap_auto_run_ns),
            ("tn_lmdb_gap_auto_page_ns", gap_auto_page_ns),
            ("tn_lmdb_gap_auto_gap_ns", gap_auto_gap_ns),
            ("tn_lmdb_gap_auto_capped", gap_auto_capped),
            ("tn_lmdb_bytes_grown", bytes_grown),
            ("tn_lmdb_free_list_save_rounds", free_list_save_rounds),
            ("tn_lmdb_save_ns", save_ns),
            ("tn_lmdb_stamp_ns", stamp_ns),
            ("tn_lmdb_data_sync_ns", data_sync_ns),
            ("tn_lmdb_gap_fill_ns", gap_fill_ns),
            ("tn_lmdb_size_sync_ns", size_sync_ns),
            ("tn_lmdb_meta_sync_ns", meta_sync_ns),
            ("tn_lmdb_cow_copies", cow_copies),
            ("tn_lmdb_verifies", verifies),
            ("tn_lmdb_verify_ns", verify_ns),
            ("tn_lmdb_pages_from_pool", pages_from_pool),
            ("tn_lmdb_pages_from_end", pages_from_end),
            ("tn_lmdb_records_loaded", records_loaded),
            ("tn_lmdb_refill_ns", refill_ns),
            ("tn_lmdb_runs_by_step_0", runs_0),
            ("tn_lmdb_runs_by_step_1", runs_1),
            ("tn_lmdb_runs_by_step_2", runs_2),
            ("tn_lmdb_runs_by_step_3", runs_3),
            ("tn_lmdb_gate_lag_sum", gate_lag_sum),
            ("tn_lmdb_gate_lag_samples", gate_lag_samples),
            ("tn_lmdb_max_gate_lag", max_gate_lag),
        ]
    }

    /// The counter `name`, one of [`Self::entries`]'s names; `None` for a name this build does not
    /// count (the other backend's, say).
    pub fn get(&self, name: &str) -> Option<u64> {
        self.entries().into_iter().find_map(|(entry, value)| (entry == name).then_some(value))
    }
}

/// The tn-lmdb counters whose [`DbCounters::since`] is the later read's value, not a difference:
/// `tn_lmdb_max_gate_lag`, a maximum since the open, and the automatic gap fill's gauges (its last
/// width and its fit).
pub const NOT_DIFFERENCES: [&str; 6] = [
    "tn_lmdb_max_gate_lag",
    "tn_lmdb_gap_auto_width",
    "tn_lmdb_gap_auto_run_ns",
    "tn_lmdb_gap_auto_page_ns",
    "tn_lmdb_gap_auto_gap_ns",
    "tn_lmdb_gap_auto_capped",
];

/// The open database `db`'s cumulative counters (see the module docs for what they count and what
/// a read costs).
#[cfg(feature = "mdbx")]
pub fn db_counters(db: &RethDb) -> eyre::Result<DbCounters> {
    let ops = db.info()?.page_ops();
    Ok(DbCounters {
        values: [
            ops.newly,
            ops.cow,
            ops.clone,
            ops.split,
            ops.merge,
            ops.spill,
            ops.unspill,
            ops.wops,
            ops.msync,
            ops.fsync,
            ops.prefault,
            ops.mincore,
        ],
    })
}

/// The open database `db`'s cumulative counters (see the module docs for what they count and what
/// a read costs). Never fails on tn-lmdb.
#[cfg(not(feature = "mdbx"))]
pub fn db_counters(db: &RethDb) -> eyre::Result<DbCounters> {
    Ok(DbCounters { io: db.io_stats() })
}

/// The page size, in bytes, of the open database `db`: the one its file was created with, which
/// a later `--db.page-size` does not change. On MDBX one environment stat, a read transaction
/// over the table catalog: read it once per open, not per output.
#[cfg(feature = "mdbx")]
pub fn db_page_size(db: &RethDb) -> eyre::Result<u32> {
    Ok(db.stat()?.page_size())
}

/// The page size, in bytes, of the open database `db`: the one its file was created with, which
/// a later `--db.page-size` does not change.
#[cfg(not(feature = "mdbx"))]
pub fn db_page_size(db: &RethDb) -> eyre::Result<u32> {
    Ok(db.page_size())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{default_page_size, test_utils::load::open_production_env_with_db, RethChainSpec};
    use reth_db::{
        tables,
        transaction::{DbTx as _, DbTxMut as _},
        Database as _,
    };
    use std::{path::Path, sync::Arc};
    use tn_types::{test_genesis, TaskManager, B256};

    /// Open a reth env of `chain` on `datadir` with the `--db.*` flags `flags` and return the
    /// database's page size, the env closed again.
    fn page_size_opened_with(
        datadir: &Path,
        chain: &Arc<RethChainSpec>,
        flags: &[&str],
    ) -> eyre::Result<u32> {
        let chain = chain.clone();
        let tasks = TaskManager::default();
        let (_env, db) = open_production_env_with_db(datadir, chain, flags, &tasks)?;
        db_page_size(&db)
    }

    /// A write commit moves the counters, and `since` is each counter's difference.
    #[tokio::test]
    async fn test_db_counters_move_after_a_commit() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tasks = TaskManager::default();
        let flags = ["--db.sync-mode", "durable"];
        let (_env, db) = open_production_env_with_db(tmp.path(), chain, &flags, &tasks)?;
        let before = db_counters(&db)?;
        let tx = db.tx_mut()?;
        for number in 1_000..1_500u64 {
            tx.put::<tables::CanonicalHeaders>(number, B256::with_last_byte(number as u8))?;
        }
        tx.commit()?;
        let after = db_counters(&db)?;
        let delta = after.since(&before);

        // pages were added and reached the disk: reth maps MDBX writable, so its durable commit
        // syncs the map (`msync`) or the file (`fsync`) rather than writing (`wops`)
        let count = |name: &str| delta.get(name).expect("a counter of this build");
        #[cfg(feature = "mdbx")]
        let moved = [
            ("pages added", count("mdbx_newly")),
            ("syncs", count("mdbx_msync") + count("mdbx_fsync")),
        ];
        #[cfg(not(feature = "mdbx"))]
        let moved = [
            ("commits", count("tn_lmdb_commits")),
            ("pages synced", count("tn_lmdb_pages_synced")),
        ];
        for (what, value) in moved {
            assert!(value > 0, "the commit moved the {what}: {delta:?}");
        }
        assert_eq!(delta.get("no_such_counter"), None);
        // the open's genesis commit counted, so a `since` that did not subtract would differ
        assert!(before.entries().iter().any(|(_, value)| *value > 0), "{before:?}");
        let rows = before.entries().into_iter().zip(after.entries()).zip(delta.entries());
        for (((name, earlier), (_, later)), (delta_name, value)) in rows {
            assert_eq!(delta_name, name, "one order");
            // a maximum since the open, not a difference
            let expected = if NOT_DIFFERENCES.contains(&name) { later } else { later - earlier };
            assert_eq!(value, expected, "{name}");
        }
        let unmoved = before.since(&after).entries();
        assert!(
            unmoved.iter().all(|(name, value)| *value == 0 || NOT_DIFFERENCES.contains(name)),
            "a pair in the wrong order saturates at zero: {unmoved:?}"
        );
        Ok(())
    }

    /// A new database takes `--db.page-size`, or [`default_page_size`] without it, and keeps its
    /// page size when reopened with another.
    #[tokio::test]
    async fn test_db_page_size_of_a_new_database() -> eyre::Result<()> {
        // one chain for every open: the test genesis takes the current time
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let cases: [(&[&str], u32); 3] = [
            (&["--db.page-size", "16384"], 16_384),
            (&["--db.page-size", "4096"], 4_096),
            (&[], default_page_size()),
        ];
        for (flags, expected) in cases {
            let tmp = tempfile::tempdir()?;
            assert_eq!(page_size_opened_with(tmp.path(), &chain, flags)?, expected, "{flags:?}");
        }

        let tmp = tempfile::tempdir()?;
        let first = page_size_opened_with(tmp.path(), &chain, &["--db.page-size", "16384"])?;
        assert_eq!(first, 16_384);
        let reopened = page_size_opened_with(tmp.path(), &chain, &["--db.page-size", "4096"])?;
        assert_eq!(reopened, 16_384, "an existing file keeps its page size");
        Ok(())
    }
}
