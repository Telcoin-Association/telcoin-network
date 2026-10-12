// SPDX-License-Identifier: MIT or Apache-2.0

//! Pack-file-backed `Database` implementation keyed by the sorted B+tree index (work in progress).

mod commit;
pub mod database;
pub(crate) mod layout;
mod table;

#[cfg(test)]
pub(crate) use commit::stats as group_stats;
pub use database::{CommitMode, TnDatabase, TnDbOptions};
pub use table::CompactionConfig;
pub(crate) use table::{
    is_corruption, AUTO_BACKOFF_MAX, AUTO_BACKOFF_MIN, BATCH_BYTES, CATCHUP_BYTES, DISK_HEADROOM,
    MAX_CATCHUP_ROUNDS, PACE_SLICE, REQUESTED_MIN_BYTES,
};
