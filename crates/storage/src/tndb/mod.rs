// SPDX-License-Identifier: MIT or Apache-2.0

//! Pack-file-backed `Database` implementation keyed by the sorted B+tree index (work in progress).

mod commit;
pub mod database;
mod layout;
mod table;

pub use database::{CommitMode, TnDatabase, TnDbOptions};
pub use table::CompactionConfig;
