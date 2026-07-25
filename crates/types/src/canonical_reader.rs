// SPDX-License-Identifier: Apache-2.0
//! A narrow read-only capability for confirming that an execution block is persisted as
//! canonical in the local execution database.
//!
//! Consensus components track recently executed blocks in a small in-memory ring bounded by
//! `gc_depth`. That ring is authoritative for "has execution caught up", but it is not
//! authoritative for "does this block exist": a block evicted from the ring can still be fully
//! persisted and canonical in the execution database. [`CanonicalExecutionReader`] lets a
//! consensus component consult the database on a ring miss instead of assuming a fork.

use crate::{BlockHash, BlockNumber};

/// Reads canonical execution block hashes from the persisted local execution database.
///
/// Implemented by the execution environment (`tn_reth::RethEnv`) and installed into the consensus
/// bus once the engine is built. Kept deliberately minimal so the consensus crates depend on this
/// capability alone, not on the execution environment itself.
pub trait CanonicalExecutionReader: Send + Sync {
    /// Return the canonical execution block hash persisted at `number`.
    ///
    /// Returns `None` when `number` is above the persisted canonical tip (not yet executed or
    /// persisted) or when the underlying read fails. The returned hash is always the canonical
    /// one at `number`, so a caller comparing it against an expected hash can treat a mismatch as
    /// a genuine fork and `None` as "not confirmed".
    fn canonical_execution_hash(&self, number: BlockNumber) -> Option<BlockHash>;
}
