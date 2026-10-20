//! Copy-on-write B+trees over slotted pages ([`crate::page`]).
//!
//! A tree is named by a [`TreeRoot`]: its root page, the low 32 bits of the id of the
//! transaction that wrote that page, its height and its entry count. The tree code reads pages
//! through a [`PageRead`] store, which does the checks every fetch makes ([`read::checked_page`]),
//! and the writer changes trees through a [`PageWrite`] store, which hands out pages private to the
//! writing transaction. A page reachable from a published tree is never changed: the writer copies
//! it first ([`write::put`]), so a reader of an older snapshot is unaffected.
//!
//! Every descent checks that a child page is the copy its parent names: its header page number is
//! the cell's child and its header txn (low 32 bits) is the cell's `child_txn`, which catches a
//! pointer redirected to a stale copy that still has a valid CRC.

use crate::{
    error::{Error, Result},
    page::Geometry,
};

pub(crate) mod cursor;
pub(crate) mod read;
#[cfg(test)]
pub(crate) mod test_harness;
pub(crate) mod write;

/// The page number of no page: the root of an empty tree.
pub(crate) const NULL_PGNO: u32 = u32::MAX;

/// The tallest tree accepted (a tree of 4 KiB pages holding every page number is far lower).
pub(crate) const MAX_HEIGHT: u32 = 48;

/// The handle of one B+tree: where its root is and what it holds. An empty tree costs no pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TreeRoot {
    /// The root page, or [`NULL_PGNO`] for an empty tree.
    pub(crate) root: u32,
    /// The low 32 bits of the root page's header txn (zero for an empty tree).
    pub(crate) root_txn: u32,
    /// The number of levels (1: the root is a leaf; 0: empty).
    pub(crate) height: u32,
    /// The number of entries.
    pub(crate) entries: u64,
}

impl TreeRoot {
    /// The empty tree.
    pub(crate) const EMPTY: Self = Self { root: NULL_PGNO, root_txn: 0, height: 0, entries: 0 };

    /// The size of the encoding: `root u32 | root_txn u32 | height u32 | reserved u32 | entries
    /// u64`, little-endian.
    pub(crate) const ENCODED_LEN: usize = 24;

    /// True for the empty tree.
    pub(crate) fn is_empty(&self) -> bool {
        self.root == NULL_PGNO
    }

    /// Check the handle's shape: the empty tree is exactly [`Self::EMPTY`], and a non-empty one
    /// has a height in `1..=MAX_HEIGHT`. Else [`Error::Corrupt`].
    pub(crate) fn check(&self) -> Result<()> {
        let ok = if self.is_empty() {
            *self == Self::EMPTY
        } else {
            (1..=MAX_HEIGHT).contains(&self.height)
        };
        if ok {
            Ok(())
        } else {
            Err(Error::Corrupt(format!("impossible tree root {self:?}")))
        }
    }

    /// The 24-byte encoding.
    pub(crate) fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0; Self::ENCODED_LEN];
        out[0..4].copy_from_slice(&self.root.to_le_bytes());
        out[4..8].copy_from_slice(&self.root_txn.to_le_bytes());
        out[8..12].copy_from_slice(&self.height.to_le_bytes());
        out[16..24].copy_from_slice(&self.entries.to_le_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes, checking the reserved word is zero and the shape
    /// ([`Self::check`]); else [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8; Self::ENCODED_LEN]) -> Result<Self> {
        let u32_at = |off: usize| {
            u32::from_le_bytes(bytes[off..off + 4].try_into().expect("a 4-byte field"))
        };
        if u32_at(12) != 0 {
            return Err(Error::Corrupt("tree root reserved word is not zero".to_string()));
        }
        let root = Self {
            root: u32_at(0),
            root_txn: u32_at(4),
            height: u32_at(8),
            entries: u64::from_le_bytes(bytes[16..24].try_into().expect("an 8-byte field")),
        };
        root.check()?;
        Ok(root)
    }
}

/// A source of checked tree pages: a reader's snapshot, or the writer (which also sees its own
/// private pages).
pub(crate) trait PageRead {
    /// The page size and the limits derived from it.
    fn geometry(&self) -> Geometry;

    /// The snapshot's page count: pages `0..page_count` are allocated. A walk that frees pages
    /// through their parents' pointers without reading them checks each against it.
    fn page_count(&self) -> u32;

    /// Page `pgno`, after the checks of [`read::checked_page`]: in range, not a meta page, a
    /// non-zero CRC trailer unless the page is private to the writer, the header's page number
    /// `pgno`, and the header's txn no later than the reading snapshot. Damage is
    /// [`Error::Corrupt`].
    fn page(&self, pgno: u32) -> Result<&[u8]>;

    /// Page `pgno` reached through a pointer that names it with `want_txn` (a branch cell's
    /// `child_txn`, or a root's `root_txn`): the checks of [`Self::page`], then the identity check
    /// of [`read::checked_child`]. A store that can read the header once for both overrides it.
    fn child(&self, pgno: u32, want_txn: u32) -> Result<&[u8]> {
        let page = self.page(pgno)?;
        read::check_identity(pgno, crate::page::txn(page), want_txn)?;
        Ok(page)
    }
}

/// The writer's page store: pages private to the writing transaction (created by it, reachable
/// by no published tree) change in place; every other page is copied first.
pub(crate) trait PageWrite: PageRead {
    /// The id of the writing transaction (the txn stamped into the pages it writes).
    fn txn(&self) -> u64;

    /// True if this transaction created page `pgno` (no reader can reach it).
    fn is_private(&self, pgno: u32) -> bool;

    /// True if this writer stamped page `pgno`'s CRC itself at an earlier commit, so its bytes
    /// were built (and checked) here: copy-on-write does not re-verify it.
    fn is_stamped(&self, pgno: u32) -> bool;

    /// Private page `pgno`'s bytes for a trusted descent (see [`write`]): the caller checked
    /// [`Self::is_private`], so the page is one this transaction allocated, inside the file; only
    /// the mapping is checked ([`Error::Corrupt`] if it fails), none of [`read::checked_page`]'s
    /// checks.
    fn private_page(&self, pgno: u32) -> Result<&[u8]>;

    /// The allocator's epoch (`Allocator::epoch`): unchanged exactly while no page was
    /// allocated or freed, no transaction began or aborted and no commit stamped the private
    /// pages, so while every tree keeps its shape and separators.
    fn epoch(&self) -> u64;

    /// A new private page, all zeros. A failure (e.g. the file cannot grow) changes nothing.
    fn alloc(&mut self) -> Result<u32>;

    /// The bytes of private page `pgno`, to change in place. A page that is not private is
    /// refused: it must be copied first.
    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]>;

    /// Page `pgno` left the tree: a published page is freed once no reader or durable meta can
    /// reach it; a private one is reusable at once.
    fn free(&mut self, pgno: u32);

    /// The writer's reused buffers ([`write::Scratch`]), taken for one write that copies or splits
    /// pages and handed back with [`Self::put_scratch`].
    fn take_scratch(&mut self) -> write::Scratch;

    /// Hand back the buffers [`Self::take_scratch`] took.
    fn put_scratch(&mut self, scratch: write::Scratch);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 24-byte encoding round-trips, and a non-zero reserved word or an impossible shape is
    /// `Corrupt`.
    #[test]
    fn tree_root_encoding_and_shape() {
        let root = TreeRoot { root: 7, root_txn: 0xDEAD_BEEF, height: 3, entries: 1 << 40 };
        let bytes = root.encode();
        assert_eq!(TreeRoot::decode(&bytes).expect("decode"), root);
        assert_eq!(TreeRoot::decode(&TreeRoot::EMPTY.encode()).expect("empty"), TreeRoot::EMPTY);

        let mut reserved = bytes;
        reserved[13] = 1;
        assert!(matches!(TreeRoot::decode(&reserved), Err(Error::Corrupt(_))));
        for bad in [
            TreeRoot { height: 1, ..TreeRoot::EMPTY },
            TreeRoot { entries: 1, ..TreeRoot::EMPTY },
            TreeRoot { root_txn: 1, ..TreeRoot::EMPTY },
            TreeRoot { height: 0, ..root },
            TreeRoot { height: MAX_HEIGHT + 1, ..root },
        ] {
            assert!(matches!(TreeRoot::decode(&bad.encode()), Err(Error::Corrupt(_))), "{bad:?}");
        }
        assert!(TreeRoot { height: MAX_HEIGHT, ..root }.check().is_ok());
    }
}
