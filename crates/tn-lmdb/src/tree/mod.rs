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
//! pointer redirected to a stale copy that still has a valid CRC; and that it belongs to the tree
//! being read: its header tag is the tree's ([`TreeRoot::tag`]), which catches a pointer
//! redirected to a page of another tree, even one the same commit wrote.

use crate::{
    error::{Error, Result},
    overflow::{self, Ovf, RunCrc},
    page::{Geometry, Kind, Val},
};

pub(crate) mod cursor;
pub(crate) mod dup_cursor;
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

/// The tag of the catalog's tree ([`TreeRoot::tag`]).
pub(crate) const CATALOG_TAG: u32 = u32::MAX;

/// The tag of the free tree ([`TreeRoot::tag`]).
pub(crate) const FREE_TAG: u32 = u32::MAX - 1;

/// The tag of the tree of table `id` ([`TreeRoot::tag`]): the id itself, which is unique over
/// the file's life (`meta::FIRST_TABLE_ID` on) and never reaches the two fixed tags.
#[inline]
pub(crate) const fn table_tag(id: u32) -> u32 {
    id
}

/// The tag of the tree nested in the outer cell of key `key` of the dup table tagged `table`
/// ([`TreeRoot::tag`]): the CRC-32 of the table's tag and the key, so a nested root redirected
/// to another key's nested tree, of this table or another, is refused like any other tree.
#[inline]
pub(crate) fn nested_tag(table: u32, key: &[u8]) -> u32 {
    crate::crc::crc32_update(crate::crc::crc32(&table.to_le_bytes()), key)
}

/// The byte debug builds fill every page [`PageWrite::alloc`] and [`PageWrite::alloc_run`] hand
/// out with ([`poison`]): release builds leave a new page's old bytes, so no layout may rely on
/// zeros, and the debug test suite runs every layout over this garbage to show none does.
#[cfg(any(test, debug_assertions))]
pub(crate) const POISON: u8 = 0xA5;

/// Fill `bytes`, a page or run just allocated, with [`POISON`] (debug builds only).
#[cfg(debug_assertions)]
pub(crate) fn poison(bytes: &mut [u8]) {
    bytes.fill(POISON);
}

/// The handle of one B+tree: where its root is and what it holds. An empty tree costs no pages.
///
/// The tag names the tree in its pages' headers and is not part of the encoding: a handle is
/// decoded with the tag its holder gives it ([`Self::decode`]): the catalog's and the free tree's
/// are fixed ([`CATALOG_TAG`], [`FREE_TAG`]), a table's is its id ([`table_tag`]) and a nested
/// tree's is derived from its table's and its outer key ([`nested_tag`]).
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
#[derive(Clone, Copy, Debug)]
pub(crate) struct TreeRoot {
    /// The tag every page of the tree carries in its header (`page::tag`), compared on every
    /// fetch; not encoded, and not compared by `==`, which compares the encoded fields: two
    /// handles are equal when the file would store them alike.
    pub(crate) tag: u32,
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

impl PartialEq for TreeRoot {
    fn eq(&self, other: &Self) -> bool {
        self.encode() == other.encode()
    }
}

impl Eq for TreeRoot {}

impl TreeRoot {
    /// The empty tree of the tests, tagged 0.
    #[cfg(test)]
    pub(crate) const EMPTY: Self = Self::empty(0);

    /// The empty tree tagged `tag`.
    pub(crate) const fn empty(tag: u32) -> Self {
        Self {
            tag,
            root: NULL_PGNO,
            root_txn: 0,
            height: 0,
            overflow_pages: 0,
            entries: 0,
            branch_pages: 0,
            leaf_pages: 0,
        }
    }

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
            *self == Self::empty(self.tag)
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

    /// Count one more entry (a put's insert). Only a damaged handle counts `u64::MAX` entries:
    /// [`Error::Corrupt`] naming it, the handle unchanged.
    #[inline]
    pub(crate) fn add_entry(&mut self) -> Result<()> {
        match self.entries.checked_add(1) {
            Some(entries) => {
                self.entries = entries;
                Ok(())
            }
            None => Err(self.entries_error("counts more entries than a tree holds")),
        }
    }

    /// Count one entry fewer (a delete's). A handle that counts none is damaged:
    /// [`Error::Corrupt`] naming it, the handle unchanged.
    #[inline]
    pub(crate) fn remove_entry(&mut self) -> Result<()> {
        match self.entries.checked_sub(1) {
            Some(entries) => {
                self.entries = entries;
                Ok(())
            }
            None => Err(self.entries_error("counts no entry for a delete to remove")),
        }
    }

    /// [`Self::add_entry`]'s and [`Self::remove_entry`]'s refusal, built out of line.
    #[cold]
    fn entries_error(&self, what: &str) -> Error {
        Error::Corrupt(format!("tree root {self:?} {what}"))
    }

    /// Count a new overflow run of `n` pages (a put's), the handle unchanged on an error. A count
    /// past [`MAX_TREE_PAGES`] is [`Error::Full`] when the tree's pages lie within the
    /// `file_pages` the file holds (a sound tree filling the file), else [`Error::Corrupt`] (only
    /// a damaged handle counts more pages than the file).
    pub(crate) fn add_run(&mut self, n: u32, file_pages: u32) -> Result<()> {
        if self.pages() + u64::from(n) > MAX_TREE_PAGES {
            return Err(room_error(self, file_pages, format_args!("a run of {n} more")));
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
    pub(crate) fn decode(bytes: &[u8; Self::ENCODED_LEN], tag: u32) -> Result<Self> {
        let u32_at = |off: usize| {
            u32::from_le_bytes(bytes[off..off + 4].try_into().expect("a 4-byte field"))
        };
        let root = Self {
            tag,
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

    /// Page `pgno` of the tree tagged `tag`, after the checks of [`read::checked_page`]: in
    /// range, not a meta page, a non-zero CRC trailer unless the page is private to the writer,
    /// the header's page number `pgno`, its tag `tag`, and its txn no later than the reading
    /// snapshot. Damage is [`Error::Corrupt`].
    fn page(&self, pgno: u32, tag: u32) -> Result<&[u8]>;

    /// Page `pgno` of the tree tagged `tag`, reached through a pointer that names it with
    /// `want_txn` (a branch cell's `child_txn`, or a root's `root_txn`): the checks of
    /// [`Self::page`], then the identity check of [`read::checked_child`]. A store that can read
    /// the header once for both overrides it.
    fn child(&self, pgno: u32, want_txn: u32, tag: u32) -> Result<&[u8]> {
        let page = self.page(pgno, tag)?;
        read::check_identity(pgno, crate::page::txn(page), want_txn)?;
        Ok(page)
    }

    /// Test-only: [`Self::page`] with the tag the page itself carries, for a test that inspects
    /// a page by number outside any tree.
    #[cfg(test)]
    fn page_any(&self, pgno: u32) -> Result<&[u8]>;

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

    /// A leaf value as a reader sees it ([`read_value`]). No store overrides it: the plain
    /// cursors call [`read_value`] itself, so a get and a cursor read each value alike.
    #[inline]
    fn value<'s>(&'s self, val: Val<'s>) -> Result<&'s [u8]> {
        read_value(self, val)
    }
}

/// A leaf value as a reader sees it, read through `store`: the inline bytes, or the value its run
/// holds ([`PageRead::overflow`]). A nested root is no value: [`Error::Corrupt`]
/// ([`crate::page::nested_error`]). The body of [`PageRead::value`] and of the plain cursors'
/// read ([`cursor::Values`]), forced inline and generic over the store, so that on a trait-object
/// store (the borrowed [`cursor::Cursor`]'s) only a run's read is an indirect call: an inline
/// value is a tag test.
#[inline(always)]
pub(crate) fn read_value<'s, S: PageRead + ?Sized>(store: &'s S, val: Val<'s>) -> Result<&'s [u8]> {
    match val {
        Val::Inline(v) => Ok(v),
        Val::Overflow(ovf) => store.overflow(ovf),
        Val::Nested(root) => Err(crate::page::nested_error(root)),
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

    /// The page count of the state this transaction began from. Every page from it up to
    /// [`PageRead::page_count`] is one this transaction allocated, so no published page names
    /// one, and one that is not private is loose: allocated and freed again, no tree's page.
    fn base_page_count(&self) -> u32;

    /// Private page `pgno`'s bytes for a trusted descent (see [`write`]): the caller checked
    /// [`Self::is_private`], so the page is one this transaction allocated, inside the file; only
    /// the mapping is checked ([`Error::Corrupt`] if it fails), none of [`read::checked_page`]'s
    /// checks.
    fn private_page(&self, pgno: u32) -> Result<&[u8]>;

    /// The allocator's epoch (`Allocator::epoch`): unchanged exactly while no page was
    /// allocated or freed, no transaction began or aborted and no commit stamped the private
    /// pages, so while every tree keeps its shape and separators.
    fn epoch(&self) -> u64;

    /// A new private page. Its bytes are not zeroed (debug builds fill it with [`POISON`]): the
    /// caller copies a page into it ([`Self::copy_published`]) or lays it out as a fresh page
    /// (`page::init_header` and the cells after it), and either writes every byte but the free
    /// gap of the page it copies or lays out, bytes that nothing reads. A failure (e.g. the file
    /// cannot grow) changes nothing.
    fn alloc(&mut self) -> Result<u32>;

    /// The bytes of private page `pgno`, to change in place. A page that is not private is
    /// refused: it must be copied first.
    fn page_mut(&mut self, pgno: u32) -> Result<&mut [u8]>;

    /// Copy page `from`, fetched with exactly [`PageRead::page`]'s checks (whose
    /// [`Error::Corrupt`] it returns as is), into private page `to`, and return `to`'s bytes:
    /// copy-on-write in one copy ([`write::copy_page`]: every byte but the free gap), the store
    /// lending the source and the destination at once. A private `from` is refused
    /// ([`Error::Failed`]), so the two never overlap; so is a `to` that is not private, as
    /// [`Self::page_mut`] refuses it.
    #[inline]
    fn copy_published(&mut self, from: u32, to: u32) -> Result<&mut [u8]> {
        self.copy_published_with(from, to, |src, dst| write::copy_page(src, to, dst))
    }

    /// [`Self::copy_published`] with `f(src, dst)` in place of [`write::copy_page`]: every check
    /// in its order, then `f` given the source page and `to`'s bytes, which it writes from the
    /// source alone (the copy of a leaf with its change made, `page::copy_insert` and its
    /// siblings), never reading `dst`. `f`'s error is returned as is, and `dst` then holds no
    /// copy. A source past the bytes the store lends in place is first copied into a buffer,
    /// which `f` then reads.
    fn copy_published_with<F>(&mut self, from: u32, to: u32, f: F) -> Result<&mut [u8]>
    where
        F: FnOnce(&[u8], &mut [u8]) -> Result<()>;

    /// Page `pgno` left the tree: a published page is freed once no reader or durable meta can
    /// reach it; a private one is reusable at once.
    fn free(&mut self, pgno: u32);

    /// How the overflow run of `npages` pages from `pgno` stands to the writing transaction
    /// (`Allocator::run_privacy`): `Some(true)` for a run it created, `Some(false)` for one that
    /// touches no private page (published), `None` for one that overlaps private pages otherwise.
    fn run_privacy(&self, pgno: u32, npages: u32) -> Option<bool>;

    /// A new private overflow run of `npages` contiguous pages, returning its first page. Its
    /// bytes are not zeroed (debug builds fill them with [`POISON`]): the caller writes all of
    /// them ([`crate::overflow::encode_run`]). A failure changes nothing.
    fn alloc_run(&mut self, npages: u32) -> Result<u32>;

    /// The bytes of private run `pgno` of `npages` pages, to write once before a leaf names it. A
    /// run that is not exactly a private one is refused: a published run is immutable.
    fn run_mut(&mut self, pgno: u32, npages: u32) -> Result<&mut [u8]>;

    /// The run of `npages` pages from `pgno` left the tree, as `npages` single pages
    /// ([`Self::free`]). The caller checked [`Self::run_privacy`].
    fn free_run(&mut self, pgno: u32, npages: u32);

    /// The writer's reused buffers ([`write::Scratch`]), taken for one write that copies or splits
    /// pages and handed back with [`Self::put_scratch`]. Boxed, so the hand-off moves one pointer:
    /// moved whole, the buffers went through a stack copy whose 16-byte loads spanned the 8-byte
    /// length stores the write had just made, which store forwarding cannot serve, so each load
    /// waited for every older store, a page copy's among them.
    fn take_scratch(&mut self) -> Box<write::Scratch>;

    /// Hand back the buffers [`Self::take_scratch`] took.
    fn put_scratch(&mut self, scratch: Box<write::Scratch>);

    /// Count one check of a published page ([`write::verify_published`]) that took `ns`
    /// nanoseconds, for the writer's I/O counters (`IoStats::verifies`, `IoStats::verify_ns`).
    /// The checks run where the store is borrowed shared. A store with no counters ignores it.
    fn count_verify(&self, _ns: u64) {}

    /// Test-only: true when deletes must not merge under-full pages ([`write::delete`]), so a
    /// test can build the shapes a tree without merging has.
    #[cfg(test)]
    fn no_merge(&self) -> bool {
        false
    }

    /// Test-only: true when copy-on-write of a leaf for a put's or a delete's change takes the
    /// two-step path, the copy ([`write::copy_page`]) and then the change in the copy, in place of
    /// one pass from the source ([`Self::copy_published_with`]): the twin a test compares with.
    #[cfg(test)]
    fn no_fused_copy(&self) -> bool {
        false
    }

    /// Test-only: a delete applied the merges `report` describes ([`write::delete`]); called only
    /// by a delete that merged or read a sibling that did not fit.
    #[cfg(test)]
    fn merges_applied(&mut self, _report: write::MergeReport) {}
}

/// The error of a write whose tree `root` would count more than [`MAX_TREE_PAGES`] pages with
/// `more`: [`Error::Full`] when its pages lie within the `file_pages` the file holds (a sound tree
/// counts at most the file's pages, so it is filling the file's page numbers), else
/// [`Error::Corrupt`] (a damaged count).
#[cold]
fn room_error(root: &TreeRoot, file_pages: u32, more: std::fmt::Arguments<'_>) -> Error {
    if root.pages() < u64::from(file_pages) {
        return Error::Full(format!(
            "tree root {root:?} has no room in the file's page numbers for {more}"
        ));
    }
    Error::Corrupt(format!("tree root {root:?} counts more pages than a tree holds with {more}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A three-level tree's handle.
    const THREE: TreeRoot = TreeRoot {
        tag: 0,
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
        assert_eq!(TreeRoot::decode(&bytes, 0).expect("decode"), THREE);
        assert_eq!(TreeRoot::decode(&TreeRoot::EMPTY.encode(), 0).expect("empty"), TreeRoot::EMPTY);
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
            assert!(
                matches!(TreeRoot::decode(&bad.encode(), 0), Err(Error::Corrupt(_))),
                "{bad:?}"
            );
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
            assert!(TreeRoot::decode(&root.encode(), 0).is_ok(), "{root:?}");
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
            assert!(
                matches!(TreeRoot::decode(&root.encode(), 0), Err(Error::Corrupt(_))),
                "{root:?}"
            );
        }
    }

    /// A run past the page bound is `Full` for a tree within the file's pages (one filling the
    /// file) and `Corrupt` for one counting more pages than the file holds, the handle unchanged
    /// either way; a run up to the bound counts.
    #[test]
    fn a_run_past_the_page_bound_is_full_or_corrupt_by_the_file() {
        let near = TreeRoot {
            branch_pages: 2,
            leaf_pages: 3,
            overflow_pages: MAX_TREE_PAGES as u32 - 10,
            ..THREE
        };
        let room = MAX_TREE_PAGES - near.pages();
        let mut root = near;
        root.add_run(room as u32, NULL_PGNO).expect("the last pages a tree can hold");
        assert_eq!(root.pages(), MAX_TREE_PAGES);
        let counted = near.pages() as u32;
        for (file_pages, full) in
            [(NULL_PGNO, true), (counted + 1, true), (counted, false), (1000, false)]
        {
            let mut root = near;
            let done = root.add_run(room as u32 + 1, file_pages);
            match done {
                Err(Error::Full(_)) if full => {}
                Err(Error::Corrupt(_)) if !full => {}
                other => panic!("a file of {file_pages} pages: {other:?}"),
            }
            assert_eq!(root, near, "a refused run changes nothing");
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
