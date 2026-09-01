#![deny(missing_docs)]

//! Implement a simple DB to store archival (previous epoch) data.
//! Will compress data and manage it in chunks (by epoch for instance) to also work well with sync.

pub mod btree_index;
pub(crate) mod crc;
pub mod data_file;
pub mod digest_index;
pub mod error;
pub mod fxhasher;
pub mod index;
/// On-demand benchmark comparing the archive point-lookup indexes (see `index_bench.rs`).
#[cfg(test)]
mod index_bench;
pub mod pack;
pub mod pack_iter;
pub mod position_index;
