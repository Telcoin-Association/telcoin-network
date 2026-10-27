// SPDX-License-Identifier: MIT or Apache-2.0
//! tndb-b: an embedded, transactional key-value engine.
//!
//! One page file holds every table as a copy-on-write B+tree with the values in its leaves, or,
//! for a value too large for its leaf, in an overflow run of contiguous pages (up to 16 MiB). A
//! single writer builds the next version of the trees in pages no reader can reach, while any
//! number of readers see consistent snapshots. A commit makes the new version durable by writing
//! one of two fixed meta pages (atomic commit, O(1) recovery: open picks the valid meta with the
//! highest transaction id), and pages freed by a commit are reused only once no durable meta and no
//! live reader can reach them, through a free list persisted in the file itself. There is no log:
//! the meta pages and the gated page reuse carry all crash safety.

// The page file is mmap-backed, synced with `msync`/`fsync`, preallocated with
// `fallocate`/`F_PREALLOCATE` and locked with `flock`: this crate builds for unix targets only.
#[cfg(not(unix))]
compile_error!("tndb-b requires a unix target (mmap-backed page file, msync, flock)");

mod alloc;
#[cfg(test)]
mod bench;
mod catalog;
mod check;
mod crc;
#[cfg(test)]
mod disk_model;
mod env;
mod error;
mod file;
mod freelist;
mod latch;
mod meta;
mod overflow;
mod page;
mod page_set;
mod sys;
#[cfg(test)]
mod tests;
mod tree;
mod txn;

pub use check::{CheckReport, TableCheck};
pub use env::{Env, EnvOptions, EnvStat, SyncMode};
pub use error::{Error, Result};
pub use tree::cursor::{Cursor, KeyTail};
pub use txn::{ReadTxn, RoCursor, RoTxn, RwCursor, RwTxn, Table, TableStat, TxnCursor};
