//! Copy-on-write B+trees over slotted pages ([`crate::page`]).
//!
//! A tree is named by a [`TreeRoot`]: its root page, the low 32 bits of the id of the
//! transaction that wrote that page, its height, its entry count and its pages by kind. The tree
//! code reads pages through a [`PageRead`] store, which does the checks every fetch makes
//! ([`read::checked_page`]), and the writer changes trees through a [`PageWrite`] store, which
//! hands out pages private to the writing transaction. A page reachable from a published tree is
//! never changed: the writer copies it first ([`write::put`]), so a reader of an older snapshot is
//! unaffected.
//!
//! Every descent checks that a child page is the copy its parent names: its header page number is
//! the cell's child and its header txn (low 32 bits) is the cell's `child_txn`, which catches a
//! pointer redirected to a stale copy that still has a valid CRC.

use crate::{
    error::{Error, Result},
    overflow::{self, Ovf, RunCrc},
    page::{Geometry, Kind, Val},
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

/// The most pages one tree can hold: every page number but the two meta pages and [`NULL_PGNO`].
pub(crate) const MAX_TREE_PAGES: u64 = NULL_PGNO as u64 - 2;

/// The handle of one B+tree: where its root is and what it holds. An empty tree costs no pages.
///
/// Encoding (32 bytes, little-endian; [`Self::encode`]):
///
/// | off | size | field |
/// |---|---|---|
/// | 0 | 4 | `root` |
/// | 4 | 4 | `root_txn` |
/// | 8 | 4 | `height` |
/// | 12 | 4 | `overflow_pages` |
/// | 16 | 8 | `entries` |
/// | 24 | 4 | `branch_pages` |
/// | 28 | 4 | `leaf_pages` |
///
/// The page counters keep a table's size an O(1) read. The tree code keeps them exact: a split
/// adds its new page, a new root adds a branch, a delete subtracts the pages it drops, and a
/// clear resets them; a copy-on-write copy replaces a page and changes no count. `Env::check`
/// compares them with the pages its walk finds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TreeRoot {
    /// The root page, or [`NULL_PGNO`] for an empty tree.
    pub(crate) root: u32,
    /// The low 32 bits of the root page's header txn (zero for an empty tree).
    pub(crate) root_txn: u32,
    /// The number of levels (1: the root is a leaf; 0: empty).
    pub(crate) height: u32,
    /// The pages of the overflow runs holding the tree's large values ([`crate::overflow`]).
    /// [`Self::check`] asks only that an empty tree has none, since a run may hang from a leaf at
    /// any height; the walks that count pages (`Env::check`, the tests' `walk_tree`) compare it
    /// with the runs the leaves name.
    pub(crate) overflow_pages: u32,
    /// The number of entries.
    pub(crate) entries: u64,
    /// The branch pages: every level above the leaves.
    pub(crate) branch_pages: u32,
    /// The leaf pages.
    pub(crate) leaf_pages: u32,
}

impl TreeRoot {
    /// The empty tree.
    pub(crate) const EMPTY: Self = Self {
        root: NULL_PGNO,
        root_txn: 0,
        height: 0,
        overflow_pages: 0,
        entries: 0,
        branch_pages: 0,
        leaf_pages: 0,
    };

    /// The size of the encoding (see the type's docs).
    pub(crate) const ENCODED_LEN: usize = 32;

    /// True for the empty tree.
    pub(crate) fn is_empty(&self) -> bool {
        self.root == NULL_PGNO
    }

    /// Check the handle's shape, else [`Error::Corrupt`]:
    /// - the empty tree is exactly [`Self::EMPTY`], every counter zero;
    /// - a non-empty one has a height in `1..=MAX_HEIGHT` and at most [`MAX_TREE_PAGES`] pages;
    /// - at height 1 the root is the only page: no branch and one leaf;
    /// - at height `h >= 2` each level above the leaves holds a branch (`branch_pages >= h - 1`)
    ///   and the root branch has two children or more, each over a leaf of its own (`leaf_pages >=
    ///   2`).
    pub(crate) fn check(&self) -> Result<()> {
        let ok = if self.is_empty() {
            *self == Self::EMPTY
        } else {
            (1..=MAX_HEIGHT).contains(&self.height)
                && self.pages() <= MAX_TREE_PAGES
                && if self.height == 1 {
                    self.branch_pages == 0 && self.leaf_pages == 1
                } else {
                    self.branch_pages >= self.height - 1 && self.leaf_pages >= 2
                }
        };
        if ok {
            Ok(())
        } else {
            Err(Error::Corrupt(format!("impossible tree root {self:?}")))
        }
    }

    /// The tree's pages: branches, leaves and overflow pages.
    pub(crate) fn pages(&self) -> u64 {
        u64::from(self.branch_pages) + u64::from(self.leaf_pages) + u64::from(self.overflow_pages)
    }

    /// Count one new page of `kind` (a split's new page, a new root). Only a damaged handle can
    /// count past [`MAX_TREE_PAGES`]: [`Error::Corrupt`], the handle unchanged.
    pub(crate) fn add_page(&mut self, kind: Kind) -> Result<()> {
        if self.pages() >= MAX_TREE_PAGES {
            return Err(Error::Corrupt(format!(
                "tree root {self:?} counts more pages than a tree holds"
            )));
        }
        // Below the limit every counter is below `u32::MAX`.
        match kind {
            Kind::Branch => self.branch_pages += 1,
            Kind::Leaf => self.leaf_pages += 1,
        }
        Ok(())
    }

    /// Count a new overflow run of `n` pages (a put's). Only a damaged handle can count past
    /// [`MAX_TREE_PAGES`]: [`Error::Corrupt`], the handle unchanged.
    pub(crate) fn add_run(&mut self, n: u32) -> Result<()> {
        if self.pages() + u64::from(n) > MAX_TREE_PAGES {
            return Err(Error::Corrupt(format!(
                "tree root {self:?} counts more pages than a tree holds with a run of {n} more"
            )));
        }
        // Within the limit every counter is below `u32::MAX`.
        self.overflow_pages += n;
        Ok(())
    }

    /// Stop counting an overflow run of `n` pages (one that leaves the tree). A handle that does
    /// not count that many is damaged: [`Error::Corrupt`], the handle unchanged.
    pub(crate) fn remove_run(&mut self, n: u32) -> Result<()> {
        self.overflow_pages = self.overflow_pages.checked_sub(n).ok_or_else(|| {
            Error::Corrupt(format!("tree root {self:?} does not count a run of {n} pages"))
        })?;
        Ok(())
    }

    /// The 32-byte encoding.
    pub(crate) fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0; Self::ENCODED_LEN];
        out[0..4].copy_from_slice(&self.root.to_le_bytes());
        out[4..8].copy_from_slice(&self.root_txn.to_le_bytes());
        out[8..12].copy_from_slice(&self.height.to_le_bytes());
        out[12..16].copy_from_slice(&self.overflow_pages.to_le_bytes());
        out[16..24].copy_from_slice(&self.entries.to_le_bytes());
        out[24..28].copy_from_slice(&self.branch_pages.to_le_bytes());
        out[28..32].copy_from_slice(&self.leaf_pages.to_le_bytes());
        out
    }

    /// Decode [`Self::encode`]'s bytes, checking the shape ([`Self::check`]); else
    /// [`Error::Corrupt`].
    pub(crate) fn decode(bytes: &[u8; Self::ENCODED_LEN]) -> Result<Self> {
        let u32_at = |off: usize| {
            u32::from_le_bytes(bytes[off..off + 4].try_into().expect("a 4-byte field"))
        };
        let root = Self {
            root: u32_at(0),
            root_txn: u32_at(4),
            height: u32_at(8),
            overflow_pages: u32_at(12),
            entries: u64::from_le_bytes(bytes[16..24].try_into().expect("an 8-byte field")),
            branch_pages: u32_at(24),
            leaf_pages: u32_at(28),
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

    /// The overflow run `ovf` names (all its pages), after the checks of
    /// [`overflow::checked_run`] against this snapshot; `verify` says when to add the full CRC of
    /// a run that is not private to the writer. Damage is [`Error::Corrupt`].
    fn run(&self, ovf: Ovf, verify: RunCrc) -> Result<&[u8]>;

    /// The value in overflow run `ovf`: [`Self::run`] with the CRC of a run of more than one page
    /// verified ([`RunCrc::MultiPage`]).
    #[inline]
    fn overflow(&self, ovf: Ovf) -> Result<&[u8]> {
        Ok(overflow::value(self.run(ovf, RunCrc::MultiPage)?, ovf))
    }

    /// A leaf value as a reader sees it: the inline bytes, or the value its run holds
    /// ([`Self::overflow`]).
    #[inline]
    fn value<'s>(&'s self, val: Val<'s>) -> Result<&'s [u8]> {
        match val {
            Val::Inline(v) => Ok(v),
            Val::Overflow(ovf) => self.overflow(ovf),
        }
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

    /// Copy page `from`, fetched with exactly [`PageRead::page`]'s checks (whose
    /// [`Error::Corrupt`] it returns as is), into private page `to`, and return `to`'s bytes:
    /// copy-on-write in one copy, the store lending the source and the destination at once. A
    /// private `from` is refused ([`Error::Failed`]), so the two never overlap; so is a `to` that
    /// is not private, as [`Self::page_mut`] refuses it.
    fn copy_published(&mut self, from: u32, to: u32) -> Result<&mut [u8]>;

    /// Page `pgno` left the tree: a published page is freed once no reader or durable meta can
    /// reach it; a private one is reusable at once.
    fn free(&mut self, pgno: u32);

    /// How the overflow run of `npages` pages from `pgno` stands to the writing transaction
    /// (`Allocator::run_privacy`): `Some(true)` for a run it created, `Some(false)` for one that
    /// touches no private page (published), `None` for one that overlaps private pages otherwise.
    fn run_privacy(&self, pgno: u32, npages: u32) -> Option<bool>;

    /// A new private overflow run of `npages` contiguous pages, returning its first page. Its
    /// bytes are not zeroed: the caller writes all of them ([`crate::overflow::encode_run`]). A
    /// failure changes nothing.
    fn alloc_run(&mut self, npages: u32) -> Result<u32>;

    /// The bytes of private run `pgno` of `npages` pages, to write once before a leaf names it. A
    /// run that is not exactly a private one is refused: a published run is immutable.
    fn run_mut(&mut self, pgno: u32, npages: u32) -> Result<&mut [u8]>;

    /// The run of `npages` pages from `pgno` left the tree, as `npages` single pages
    /// ([`Self::free`]). The caller checked [`Self::run_privacy`].
    fn free_run(&mut self, pgno: u32, npages: u32);

    /// The writer's reused buffers ([`write::Scratch`]), taken for one write that copies or splits
    /// pages and handed back with [`Self::put_scratch`].
    fn take_scratch(&mut self) -> write::Scratch;

    /// Hand back the buffers [`Self::take_scratch`] took.
    fn put_scratch(&mut self, scratch: write::Scratch);

    /// Test-only: true when deletes must not merge under-full pages ([`write::delete`]), so a
    /// test can build the shapes a tree without merging has.
    #[cfg(test)]
    fn no_merge(&self) -> bool {
        false
    }

    /// Test-only: a delete applied the merges `report` describes ([`write::delete`]); called only
    /// by a delete that merged or read a sibling that did not fit.
    #[cfg(test)]
    fn merges_applied(&mut self, _report: write::MergeReport) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A three-level tree's handle.
    const THREE: TreeRoot = TreeRoot {
        root: 7,
        root_txn: 0xDEAD_BEEF,
        height: 3,
        overflow_pages: 0x0102_0304,
        entries: 1 << 40,
        branch_pages: 0x0506_0708,
        leaf_pages: 0x090A_0B0C,
    };

    /// The 32-byte encoding round-trips with every field at its offset, and an impossible shape
    /// is `Corrupt`.
    #[test]
    fn tree_root_encoding_and_shape() {
        let bytes = THREE.encode();
        assert_eq!(TreeRoot::decode(&bytes).expect("decode"), THREE);
        assert_eq!(TreeRoot::decode(&TreeRoot::EMPTY.encode()).expect("empty"), TreeRoot::EMPTY);
        let mut expect = Vec::new();
        expect.extend_from_slice(&7_u32.to_le_bytes());
        expect.extend_from_slice(&0xDEAD_BEEF_u32.to_le_bytes());
        expect.extend_from_slice(&3_u32.to_le_bytes());
        expect.extend_from_slice(&0x0102_0304_u32.to_le_bytes());
        expect.extend_from_slice(&(1_u64 << 40).to_le_bytes());
        expect.extend_from_slice(&0x0506_0708_u32.to_le_bytes());
        expect.extend_from_slice(&0x090A_0B0C_u32.to_le_bytes());
        assert_eq!(bytes.as_slice(), expect, "the field offsets");

        for bad in [
            TreeRoot { height: 1, ..TreeRoot::EMPTY },
            TreeRoot { entries: 1, ..TreeRoot::EMPTY },
            TreeRoot { root_txn: 1, ..TreeRoot::EMPTY },
            TreeRoot { height: 0, ..THREE },
            TreeRoot { height: MAX_HEIGHT + 1, ..THREE },
        ] {
            assert!(matches!(TreeRoot::decode(&bad.encode()), Err(Error::Corrupt(_))), "{bad:?}");
        }
        let tall = TreeRoot { height: MAX_HEIGHT, branch_pages: MAX_HEIGHT - 1, ..THREE };
        assert!(tall.check().is_ok());
    }

    /// The page counters must fit the height: none in an empty tree, exactly one leaf at height
    /// 1, a branch per level above the leaves and two leaves or more at height 2 and up, and no
    /// more pages than a file can give one tree.
    #[test]
    fn tree_root_counters_fit_the_shape() {
        let one = TreeRoot {
            root: 9,
            root_txn: 1,
            height: 1,
            entries: 5,
            leaf_pages: 1,
            ..TreeRoot::EMPTY
        };
        let shape = |h: u32, branch_pages: u32, leaf_pages: u32| TreeRoot {
            height: h,
            branch_pages,
            leaf_pages,
            overflow_pages: 0,
            ..THREE
        };
        let good = [
            one,
            TreeRoot { overflow_pages: 3, ..one },
            shape(2, 1, 2),
            shape(2, 1, 400),
            shape(3, 2, 2),
            shape(3, 9, 70),
            TreeRoot { overflow_pages: 12, ..shape(4, 3, 2) },
            shape(MAX_HEIGHT, MAX_HEIGHT - 1, 2),
            shape(2, 1, (MAX_TREE_PAGES - 1) as u32),
            TreeRoot { overflow_pages: (MAX_TREE_PAGES - 3) as u32, ..shape(2, 1, 2) },
        ];
        for root in good {
            assert!(TreeRoot::decode(&root.encode()).is_ok(), "{root:?}");
        }
        let bad = [
            TreeRoot { branch_pages: 1, ..TreeRoot::EMPTY },
            TreeRoot { leaf_pages: 1, ..TreeRoot::EMPTY },
            TreeRoot { overflow_pages: 1, ..TreeRoot::EMPTY },
            TreeRoot { leaf_pages: 0, ..one },
            TreeRoot { leaf_pages: 2, ..one },
            TreeRoot { branch_pages: 1, ..one },
            shape(2, 0, 2),
            shape(2, 1, 1),
            shape(2, 1, 0),
            shape(3, 1, 9),
            shape(MAX_HEIGHT, MAX_HEIGHT - 2, 9),
            shape(2, 1, MAX_TREE_PAGES as u32),
            TreeRoot { overflow_pages: (MAX_TREE_PAGES - 2) as u32, ..shape(2, 1, 2) },
            shape(2, u32::MAX, u32::MAX),
        ];
        for root in bad {
            assert!(matches!(TreeRoot::decode(&root.encode()), Err(Error::Corrupt(_))), "{root:?}");
        }
    }

    /// A new page counts once by kind, and a counter at the limit is `Corrupt`, unchanged.
    #[test]
    fn add_page_counts_by_kind_up_to_the_limit() {
        let mut root = TreeRoot { overflow_pages: 0, ..THREE };
        let before = root;
        root.add_page(Kind::Branch).expect("branch");
        root.add_page(Kind::Leaf).expect("leaf");
        root.add_page(Kind::Leaf).expect("leaf");
        assert_eq!(
            root,
            TreeRoot {
                branch_pages: before.branch_pages + 1,
                leaf_pages: before.leaf_pages + 2,
                ..before
            }
        );
        let max = MAX_TREE_PAGES as u32;
        for kind in [Kind::Branch, Kind::Leaf] {
            let mut room =
                TreeRoot { branch_pages: 2, leaf_pages: max - 3, overflow_pages: 0, ..THREE };
            room.add_page(kind).expect("the last page a tree can hold");
            assert_eq!(room.pages(), MAX_TREE_PAGES);
            for (branch_pages, leaf_pages, overflow_pages) in
                [(2, max - 2, 0), (max - 2, 2, 0), (1, 2, max - 3), (u32::MAX, u32::MAX, 0)]
            {
                let full = TreeRoot { branch_pages, leaf_pages, overflow_pages, ..THREE };
                let mut root = full;
                assert!(matches!(root.add_page(kind), Err(Error::Corrupt(_))), "{kind:?} {full:?}");
                assert_eq!(root, full, "a refused count changes nothing");
            }
        }
    }
}
