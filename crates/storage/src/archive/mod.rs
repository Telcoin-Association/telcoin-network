#![deny(missing_docs)]

//! Implement a simple DB to store archival (previous epoch) data.
//! Will compress data and manage it in chunks (by epoch for instance) to also work well with sync.

pub(crate) mod crc;
pub mod data_file;
pub mod digest_index;
pub mod error;
pub mod fxhasher;
pub mod pack;
pub mod pack_iter;
