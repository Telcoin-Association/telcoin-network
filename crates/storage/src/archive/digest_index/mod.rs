#![deny(missing_docs)]

//! Module to maintain an index of digests for a pack file.
//!
//! The digest index is the memory-mapped, cache-free [`HdxIndex`]: a 256-bit digest -> u64
//! record-position hash index over an `index.hdx` bucket file plus an `index.odx` overflow log.

pub mod bloom;
pub mod index;
pub mod odx_header;

pub use index::{BucketCrcReport, HdxIndex};
