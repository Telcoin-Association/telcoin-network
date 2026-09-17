// SPDX-License-Identifier: MIT or Apache-2.0

//! Pack-file-backed `Database` implementation keyed by the sorted B+tree index (work in progress).

pub mod database;
mod table;

pub use database::TnDatabase;
