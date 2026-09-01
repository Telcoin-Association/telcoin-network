//! On-disk, paged B+tree "sortable index": a durable `key → u64` index over pack files supporting
//! sorted point lookup plus range/prefix/forward/reverse iteration.  It complements the hash-based
//! [`digest_index`](crate::archive::digest_index), which offers point lookups only.
//!
//! Keys are fixed `KSIZE`-byte byte strings compared lexicographically; values are `u64` byte
//! offsets into a pack file.  The tree is a B+tree with doubly-linked leaves, stored in fixed
//! 4 KiB pages (each protected by a trailing CRC32) in a single `index.btx` file.  See
//! [`index::BtreeIndex`].

pub(crate) mod header;
pub(crate) mod page;

pub mod index;
pub mod iter;

pub use index::BtreeIndex;
pub use iter::BtreeIter;
