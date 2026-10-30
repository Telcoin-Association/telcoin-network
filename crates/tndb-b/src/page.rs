//! Slotted page layout and byte-level codec for the B+tree pages.
//!
//! Every tree page is one page of the file ([`Geometry::page_size`] bytes, a power of two in
//! 4 KiB..=64 KiB) holding variable-size cells addressed through a slot array kept in key order:
//!
//! ```text
//! header (24):  pgno u32 | kind u8 (1 = branch, 2 = leaf) | flags u8 | nkeys u16 | txn u64 | upper u16 | reserved [6]
//! slots:        nkeys × u16 cell offsets from byte 24; slot i addresses cell i (key order)
//! free space:   from the end of the slot array up to `upper`
//! cells:        packed from `upper` up to P − 4, in no particular order
//! trailer (4):  CRC32, stamped at commit (all zeros = dirty); this codec only zeroes it
//!
//! leaf cell:    vlen u32 | klen u16 | flags u16 | key | value
//! branch cell:  child u32 | child_txn u32 | klen u16 | key
//! ```
//!
//! A leaf cell's flags are 0 for a value held in the cell, 1 for an overflow cell, whose
//! 12-byte value names the overflow run holding the value ([`Val`], [`crate::overflow`]), or 2 for
//! a nested cell, whose 32-byte value is an encoded [`TreeRoot`] ([`Val::Nested`]); any other flag
//! is [`Error::Corrupt`]. Only a dup table's outer tree holds nested cells, one per outer key;
//! every reader and writer of any other tree refuses one as damage ([`nested_error`]).
//!
//! **Packing.** The cells tile `upper..P − 4` exactly. A new cell goes just below `upper`;
//! deleting or shrinking a cell closes its gap at once (the cells below it move up and their slots
//! follow), so a page never fragments and its free space is the single gap
//! `upper − (24 + 2·nkeys)`. With `used` = the bytes of every cell plus its slot,
//! `free + used == U` always holds, where `U = P − 28` (header and trailer). Nothing reads the
//! gap, so it need not be zero: [`init_header`] lays out a page over whatever bytes it holds,
//! writing the header and zeroing the trailer (dirty), and the gap keeps the old bytes.
//!
//! **Branches** hold one cell per child (each a key and a child): cell i's key is the lower bound
//! of child i's key range, and cell 0's key is empty and stands for −∞, so the child for a key is
//! the last cell whose key is `<=` it.
//!
//! **Limits.** An entry (cell plus slot) is at most `MAX_ENTRY = U / 4` bytes:
//! `max_key = MAX_ENTRY − 12` (so any key fits a branch cell as a separator) and
//! `max_value(klen) = MAX_ENTRY − 10 − klen` (0 for a key over `max_key`, which [`validate`]
//! refuses in a leaf too), the inline limit. A table's larger value goes to an overflow run
//! ([`Geometry::place`]), up to 16 MiB for a key that leaves room for the 12-byte overflow cell
//! ([`Geometry::max_table_value`]); the cell itself is an ordinary entry within the limits. A page
//! that overflows holds at most `U + MAX_ENTRY` bytes counting the pending entry, so cutting it
//! at the first entry where the left part reaches half leaves a left part under
//! `half + MAX_ENTRY ≤ 5U/8 + U/4` and a right part of at most half: one split always suffices
//! ([`split_leaf`], [`split_branch`]). A delete's merge of two siblings ([`SplitBuf::merge`]) is
//! sized to fit before it is planned: the two pages' cells and slots ([`View::used`]), plus for
//! branches the parent's key coming down as the right page's cell-0 key, within `U`.
//!
//! **Checking.** Pages read from the file may hold any bytes. [`validate`] checks a whole page
//! (header, every cell in bounds, cells tiling the cell area, keys strictly ascending, entry
//! limits). Without it, every accessor still bounds-checks what it touches (the header in O(1),
//! then the cells it reads) and returns [`Error::Corrupt`] instead of indexing out of the page, so
//! readers can use pages lazily. The mutators stay in bounds on any page but keep a page valid only
//! if it was valid, so a page read from the file must pass [`validate`] before its first change.
//!
//! **Views.** [`View`], [`LeafView`] and [`BranchView`] hold a page whose header passed the O(1)
//! check once, for a caller that reads several cells of one page; each cell they read still gets
//! the per-cell checks. The free readers ([`leaf_find`], [`branch_route`], [`keys_in_range`], ...)
//! are the views' methods behind a fresh header check.

use std::{cmp::Ordering, fmt::Display};

use crate::{
    error::{Error, Result},
    overflow::{self, Ovf, MAX_VALUE, OVF_CELL},
    tree::TreeRoot,
};

/// Smallest supported page size.
pub(crate) const MIN_PAGE_SIZE: usize = 4096;
/// Largest supported page size (the largest whose cell offsets fit a `u16` slot).
pub(crate) const MAX_PAGE_SIZE: usize = 65536;
/// Size of the page header.
pub(crate) const HEADER: usize = 24;
/// Size of the CRC trailer at the end of every page.
pub(crate) const TRAILER: usize = 4;
/// Size of one slot (a `u16` cell offset).
pub(crate) const SLOT: usize = 2;
/// Fixed part of a leaf cell: `vlen u32 | klen u16 | flags u16`.
pub(crate) const LEAF_CELL_HEADER: usize = 8;
/// Fixed part of a branch cell: `child u32 | child_txn u32 | klen u16`.
pub(crate) const BRANCH_CELL_HEADER: usize = 10;

// Header field offsets.
const PGNO_OFF: usize = 0;
const KIND_OFF: usize = 4;
const FLAGS_OFF: usize = 5;
const NKEYS_OFF: usize = 6;
const TXN_OFF: usize = 8;
const UPPER_OFF: usize = 16;
const RESERVED_OFF: usize = 18;

const KIND_BRANCH: u8 = 1;
const KIND_LEAF: u8 = 2;

/// A leaf cell's flag: the value is an overflow reference ([`Ovf`], [`OVF_CELL`] bytes).
const CELL_OVERFLOW: usize = 0x0001;

/// A leaf cell's flag: the value is a nested tree's root ([`Val::Nested`], [`NESTED_CELL`] bytes).
const CELL_NESTED: usize = 0x0002;

/// The value bytes of a nested cell: an encoded [`TreeRoot`].
pub(crate) const NESTED_CELL: usize = TreeRoot::ENCODED_LEN;

/// The two kinds of tree page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// An interior page: one `(child, child_txn, lower-bound key)` cell per child.
    Branch,
    /// A leaf page: one `(key, value)` cell per entry.
    Leaf,
}

impl Kind {
    /// The header tag byte.
    fn tag(self) -> u8 {
        match self {
            Self::Branch => KIND_BRANCH,
            Self::Leaf => KIND_LEAF,
        }
    }

    /// The fixed part of a cell of this kind (its smallest possible size).
    fn cell_header(self) -> usize {
        match self {
            Self::Branch => BRANCH_CELL_HEADER,
            Self::Leaf => LEAF_CELL_HEADER,
        }
    }
}

/// Outcome of a leaf binary search.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Search {
    /// The key is at this cell index.
    Found(usize),
    /// The key is absent; this is the cell index it would be inserted at.
    NotFound(usize),
}

/// A leaf entry's value as its cell holds it: the bytes themselves, a reference to the
/// overflow run holding them, or a nested tree's root. A put passes the same form to the leaf
/// mutators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Val<'a> {
    /// The value, in the cell.
    Inline(&'a [u8]),
    /// The value's overflow run ([`crate::overflow`]).
    Overflow(Ovf),
    /// A nested tree's root as the cell holds it: the encoded [`TreeRoot`], undecoded and
    /// unchecked, since a root read from a cell is untrusted until its use checks it. No table
    /// holds one yet: every use refuses it ([`nested_error`]).
    Nested(&'a [u8; NESTED_CELL]),
}

// `Val` rides in every entry read and in `Find::Found`: the nested form must not grow it.
const _: () = assert!(std::mem::size_of::<Val<'static>>() <= 24);

impl<'a> Val<'a> {
    /// The bytes of the value in the cell: the value, [`OVF_CELL`] for a reference, or
    /// [`NESTED_CELL`] for a nested root.
    #[inline]
    pub(crate) fn cell_len(&self) -> usize {
        match self {
            Self::Inline(v) => v.len(),
            Self::Overflow(_) => OVF_CELL,
            Self::Nested(_) => NESTED_CELL,
        }
    }

    /// The value of a tree that holds only inline values (the catalog, the free list): an
    /// overflow reference or a nested root there is [`Error::Corrupt`].
    #[inline]
    pub(crate) fn inline(self) -> Result<&'a [u8]> {
        match self {
            Self::Inline(v) => Ok(v),
            Self::Overflow(ovf) => Err(inline_error(ovf)),
            Self::Nested(root) => Err(nested_error(root)),
        }
    }
}

/// A value as the leaf mutators ([`LeafMut::insert`], [`LeafMut::replace_value`]) write it into
/// a cell: inline bytes (`&[u8]`), or either form ([`Val`]). The mutators are generic over it so
/// that a put of inline bytes, the hot path, compiles to code that never sees the overflow form:
/// a [`Val`] passed to them through memory and matched there slowed every inline put by a few
/// percent.
pub(crate) trait CellVal: Copy {
    /// The bytes of the value in the cell ([`Val::cell_len`]).
    fn cell_len(&self) -> usize;

    /// Write the cell's value length, flags and value, the value at `v` (past the key).
    fn write(self, cell: &mut [u8], v: usize);
}

impl CellVal for &[u8] {
    #[inline(always)]
    fn cell_len(&self) -> usize {
        self.len()
    }

    #[inline(always)]
    fn write(self, cell: &mut [u8], v: usize) {
        put_u32(cell, 0, self.len() as u32);
        put_u16(cell, 6, 0);
        copy_short(&mut cell[v..v + self.len()], self);
    }
}

impl CellVal for Val<'_> {
    #[inline]
    fn cell_len(&self) -> usize {
        Val::cell_len(self)
    }

    #[inline]
    fn write(self, cell: &mut [u8], v: usize) {
        match self {
            Val::Inline(bytes) => bytes.write(cell, v),
            Val::Overflow(ovf) => {
                put_u32(cell, 0, OVF_CELL as u32);
                put_u16(cell, 6, CELL_OVERFLOW);
                cell[v..v + OVF_CELL].copy_from_slice(&ovf.encode());
            }
            Val::Nested(root) => {
                put_u32(cell, 0, NESTED_CELL as u32);
                put_u16(cell, 6, CELL_NESTED);
                cell[v..v + NESTED_CELL].copy_from_slice(root);
            }
        }
    }
}

/// [`Val::inline`]'s error.
#[cold]
#[inline(never)]
fn inline_error(ovf: Ovf) -> Error {
    Error::Corrupt(format!("an overflow value ({ovf:?}) in a tree of inline values"))
}

/// The [`Error::Corrupt`] of a nested cell met outside a dup table's outer tree: in a plain
/// table, the catalog, the free tree or a nested tree a nested cell is damage wherever a read or
/// a write meets it. The one message of every such use.
#[cold]
#[inline(never)]
pub(crate) fn nested_error(root: &[u8; NESTED_CELL]) -> Error {
    Error::Corrupt(format!("a nested cell in a tree that holds none (root bytes {root:02x?})"))
}

/// Where a put stores a value ([`Geometry::place`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
    /// In its leaf cell.
    Inline,
    /// In an overflow run of `npages` pages, named by a 12-byte cell.
    Overflow {
        /// The run's pages ([`overflow::run_pages`]).
        npages: usize,
    },
}

/// Outcome of [`leaf_find`]: a [`Search`] with what the search already read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Find<'a> {
    /// The key is at cell `index`, whose value is `value`.
    Found {
        /// The cell index.
        index: usize,
        /// The entry's value.
        value: Val<'a>,
    },
    /// The key is absent and would be inserted at cell `index`, between `before` (the key of cell
    /// `index − 1`; `None` when `index` is 0) and `after` (the key of cell `index`; `None` past
    /// the last cell).
    NotFound {
        /// The insertion index.
        index: usize,
        /// The key below the insertion point.
        before: Option<&'a [u8]>,
        /// The key above the insertion point.
        after: Option<&'a [u8]>,
    },
}

/// The way down a branch to one child ([`branch_route`], [`branch_route_at`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Route<'a> {
    /// The child's cell index.
    pub(crate) index: usize,
    /// The child's lower bound in this branch, the key of cell `index` (`None` for cell 0: −∞).
    pub(crate) lo: Option<&'a [u8]>,
    /// The child's upper bound in this branch, the key of cell `index + 1` (`None` for the last
    /// child).
    pub(crate) hi: Option<&'a [u8]>,
    /// The child's page number.
    pub(crate) child: u32,
    /// The low 32 bits of the child's header txn (see [`branch_child`]).
    pub(crate) child_txn: u32,
}

/// Outcome of an operation that adds bytes to a page.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fit {
    /// The page was changed.
    Done,
    /// The bytes do not fit the page's free space; the page is unchanged (byte for byte).
    Full,
}

/// Page size and the limits derived from it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Geometry {
    page_size: usize,
    usable: usize,
    max_entry: usize,
}

impl Geometry {
    /// The geometry of `page_size`-byte pages: a power of two in 4096..=65536, else
    /// [`Error::InvalidPageSize`].
    pub(crate) fn new(page_size: u32) -> Result<Self> {
        let p = page_size as usize;
        if !p.is_power_of_two() || !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&p) {
            return Err(Error::InvalidPageSize(page_size));
        }
        let usable = p - HEADER - TRAILER;
        Ok(Self { page_size: p, usable, max_entry: usable / 4 })
    }

    /// The page size P in bytes.
    pub(crate) fn page_size(&self) -> usize {
        self.page_size
    }

    /// `U = P − 28`: the bytes a page has for cells and slots.
    pub(crate) fn usable(&self) -> usize {
        self.usable
    }

    /// `U / 4`: the largest entry (cell plus slot) a page accepts.
    #[cfg(test)]
    pub(crate) fn max_entry(&self) -> usize {
        self.max_entry
    }

    /// The longest key: one whose branch cell (as a separator) is a maximal entry.
    pub(crate) fn max_key(&self) -> usize {
        self.max_entry - BRANCH_CELL_HEADER - SLOT
    }

    /// The widest outer key a dup table takes: the largest `K` whose outer cell, the key with a
    /// [`NESTED_CELL`]-byte nested root, is an inline entry (`max_value(K) >= NESTED_CELL`). It is
    /// below [`Self::max_key`] by more than a byte, so a logical key `k ‖ s` with a 1-byte subkey
    /// is within the key limit.
    pub(crate) fn max_dup_key_len(&self) -> usize {
        self.max_entry - LEAF_CELL_HEADER - SLOT - NESTED_CELL
    }

    /// The longest value stored with a `klen`-byte key, or 0 for a key over [`max_key`], which
    /// is refused with any value. A key within the limit always takes at least 2 bytes, so 0 is
    /// never a limit for an accepted key.
    ///
    /// [`max_key`]: Self::max_key
    pub(crate) fn max_value(&self, klen: usize) -> usize {
        if klen > self.max_key() {
            return 0;
        }
        self.max_entry - LEAF_CELL_HEADER - SLOT - klen
    }

    /// The longest value a table stores with a `klen`-byte key: [`MAX_VALUE`] when the key leaves
    /// room for an overflow cell (`klen <= max_key − 10`), else [`Self::max_value`] (0 for a key
    /// over [`Self::max_key`]).
    pub(crate) fn max_table_value(&self, klen: usize) -> usize {
        if self.overflow_fits(klen) {
            MAX_VALUE
        } else {
            self.max_value(klen)
        }
    }

    /// True if a `klen`-byte key leaves room for an overflow cell's 12 bytes in a maximal entry.
    fn overflow_fits(&self, klen: usize) -> bool {
        klen <= self.max_key() && OVF_CELL <= self.max_value(klen)
    }

    /// Where a table stores a `vlen`-byte value under a `klen`-byte key: inline when it fits
    /// [`Self::max_value`] (so every inline layout is unchanged by overflow support), else in an
    /// overflow run when the key leaves room for the 12-byte cell and `vlen <=` [`MAX_VALUE`]; else
    /// [`Error::KeyTooLarge`] or [`Error::ValueTooLarge`], before anything is changed.
    #[inline]
    pub(crate) fn place(&self, klen: usize, vlen: usize) -> Result<Place> {
        let max = self.max_key();
        if klen > max {
            return Err(Error::KeyTooLarge { len: klen, max });
        }
        if vlen <= self.max_value(klen) {
            return Ok(Place::Inline);
        }
        let max = self.max_table_value(klen);
        if vlen > max {
            return Err(Error::ValueTooLarge { len: vlen, max });
        }
        Ok(Place::Overflow { npages: overflow::run_pages(vlen, self.page_size) })
    }

    /// Check an entry's sizes against the inline limits: [`Error::KeyTooLarge`] or
    /// [`Error::ValueTooLarge`], before anything is changed. The catalog and the free list, which
    /// hold only inline values, write through it; a table's put asks [`Self::place`].
    pub(crate) fn check_entry(&self, klen: usize, vlen: usize) -> Result<()> {
        let max = self.max_key();
        if klen > max {
            return Err(Error::KeyTooLarge { len: klen, max });
        }
        let max = self.max_value(klen);
        if vlen > max {
            return Err(Error::ValueTooLarge { len: vlen, max });
        }
        Ok(())
    }
}

// ---- little-endian scalar helpers ----
//
// The `get_*` readers are for header fields of a buffer of any length; the `read_*` readers and
// the writers index directly and are only used at offsets the page checks have proven in bounds.

fn get_u16(buf: &[u8], off: usize) -> Option<u16> {
    Some(u16::from_le_bytes(buf.get(off..off + 2)?.try_into().ok()?))
}

fn get_u32(buf: &[u8], off: usize) -> Option<u32> {
    Some(u32::from_le_bytes(buf.get(off..off + 4)?.try_into().ok()?))
}

fn get_u64(buf: &[u8], off: usize) -> Option<u64> {
    Some(u64::from_le_bytes(buf.get(off..off + 8)?.try_into().ok()?))
}

fn read_u16(buf: &[u8], off: usize) -> usize {
    u16::from_le_bytes([buf[off], buf[off + 1]]) as usize
}

fn read_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]])
}

fn put_u16(buf: &mut [u8], off: usize, v: usize) {
    debug_assert!(v <= u16::MAX as usize);
    buf[off..off + 2].copy_from_slice(&(v as u16).to_le_bytes());
}

fn put_u32(buf: &mut [u8], off: usize, v: u32) {
    buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

fn put_u64(buf: &mut [u8], off: usize, v: u64) {
    buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

/// `dst.copy_from_slice(src)`, panicking as it does when the lengths differ, for the short
/// copies a put makes on every row (a key, an inline value, a cursor's saved key). Up to 64 bytes
/// it copies the slice's first and last bytes in two fixed-size chunks that overlap
/// ([`copy_ends`]), so the compiler emits plain loads and stores where `copy_from_slice` of a
/// length it cannot see calls `memcpy`. Longer slices go to `copy_from_slice`.
///
/// `#[inline(always)]`: the reth adapter instantiates the generic write path ([`LeafMut::insert`])
/// in its own crate, where a call to this helper would cost more than the `memcpy` it replaces.
#[inline(always)]
pub(crate) fn copy_short(dst: &mut [u8], src: &[u8]) {
    let n = src.len();
    if dst.len() != n || n > 64 {
        // `copy_from_slice` panics on the length mismatch.
        dst.copy_from_slice(src);
    } else if n > 32 {
        copy_ends::<32>(dst, src);
    } else if n > 16 {
        copy_ends::<16>(dst, src);
    } else if n >= 8 {
        copy_ends::<8>(dst, src);
    } else if n >= 4 {
        copy_ends::<4>(dst, src);
    } else if n > 0 {
        // 1..=3 bytes: the first, the middle and the last cover them all.
        dst[0] = src[0];
        dst[n / 2] = src[n / 2];
        dst[n - 1] = src[n - 1];
    }
}

/// [`copy_short`] of `K..=2K` bytes between slices of equal length: the first `K` bytes and the
/// last `K`, which overlap unless the length is `2K`.
#[inline(always)]
fn copy_ends<const K: usize>(dst: &mut [u8], src: &[u8]) {
    let tail = src.len() - K;
    dst[..K].copy_from_slice(&src[..K]);
    dst[tail..tail + K].copy_from_slice(&src[tail..tail + K]);
}

/// A [`Error::Corrupt`] naming the page.
#[cold]
fn corrupt(page: &[u8], what: impl Display) -> Error {
    Error::Corrupt(format!("page {}: {what}", pgno(page)))
}

// ---- header ----
//
// The raw header accessors read a buffer of any length (a buffer too short for a header reads as
// zeros); they do not check the page.

/// The page number recorded in the header.
pub(crate) fn pgno(page: &[u8]) -> u32 {
    get_u32(page, PGNO_OFF).unwrap_or(0)
}

/// The page kind, or [`Error::Corrupt`] for an unknown tag.
pub(crate) fn kind(page: &[u8]) -> Result<Kind> {
    match page.get(KIND_OFF) {
        Some(&KIND_BRANCH) => Ok(Kind::Branch),
        Some(&KIND_LEAF) => Ok(Kind::Leaf),
        tag => Err(corrupt(page, format_args!("unknown page kind {tag:?}"))),
    }
}

/// The header flags (zero in this format version).
pub(crate) fn flags(page: &[u8]) -> u8 {
    page.get(FLAGS_OFF).copied().unwrap_or(0)
}

/// The number of cells (entries of a leaf, children of a branch).
#[inline]
pub(crate) fn nkeys(page: &[u8]) -> usize {
    get_u16(page, NKEYS_OFF).map_or(0, usize::from)
}

/// The id of the transaction that wrote the page.
pub(crate) fn txn(page: &[u8]) -> u64 {
    get_u64(page, TXN_OFF).unwrap_or(0)
}

/// The offset of the lowest cell (the page size minus the trailer when the page is empty).
#[inline]
pub(crate) fn upper(page: &[u8]) -> usize {
    get_u16(page, UPPER_OFF).map_or(0, usize::from)
}

fn init(page: &mut [u8], kind: Kind, pgno: u32, txn: u64) {
    page.fill(0);
    init_header(page, kind, pgno, txn);
}

/// [`init`] without the fill: lay out an empty page over whatever bytes `page` holds, such as a
/// page the writer just allocated (`PageWrite::alloc`, whose bytes are not zeroed). It writes
/// every byte a reader of an empty page interprets, the whole header (its flags and reserved
/// bytes zero) and the trailer (zero: dirty), and leaves the rest, the empty page's free gap, as
/// it is: nothing reads the gap, and the CRC covers it as it is.
fn init_header(page: &mut [u8], kind: Kind, pgno: u32, txn: u64) {
    debug_assert!(valid_len(page.len()), "page buffer of {} bytes", page.len());
    let trailer = page.len() - TRAILER;
    page[..HEADER].fill(0);
    page[trailer..].fill(0);
    put_u32(page, PGNO_OFF, pgno);
    page[KIND_OFF] = kind.tag();
    put_u64(page, TXN_OFF, txn);
    put_u16(page, UPPER_OFF, page.len() - TRAILER);
}

/// Initialise `page` (all of it, trailer zeroed = dirty) as an empty leaf.
#[cfg(test)]
pub(crate) fn init_leaf(page: &mut [u8], pgno: u32, txn: u64) {
    init(page, Kind::Leaf, pgno, txn);
}

/// Lay out `page`, whatever its bytes (a page just allocated), as an empty leaf: only the header
/// and the trailer are written ([`init_header`]).
pub(crate) fn init_leaf_header(page: &mut [u8], pgno: u32, txn: u64) {
    init_header(page, Kind::Leaf, pgno, txn);
}

/// Initialise `page` (all of it, trailer zeroed = dirty) as a branch with no children yet: add
/// child 0 with an empty key ([`branch_insert`]), or use [`init_root`].
#[cfg(test)]
pub(crate) fn init_branch(page: &mut [u8], pgno: u32, txn: u64) {
    init(page, Kind::Branch, pgno, txn);
}

/// Re-stamp a page's identity: a copy-on-write copy takes its new page number and the writing
/// transaction's id.
pub(crate) fn set_pgno_txn(page: &mut [u8], pgno: u32, txn: u64) {
    put_u32(page, PGNO_OFF, pgno);
    put_u64(page, TXN_OFF, txn);
}

#[inline]
fn valid_len(len: usize) -> bool {
    len.is_power_of_two() && (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&len)
}

/// A page header that passed the O(1) checks of [`hdr`].
#[derive(Clone, Copy, Debug)]
struct Hdr {
    kind: Kind,
    nkeys: usize,
    upper: usize,
    /// `P − TRAILER`: the end of the cell area.
    end: usize,
}

impl Hdr {
    /// The end of the slot array.
    fn slots_end(&self) -> usize {
        HEADER + SLOT * self.nkeys
    }

    /// The free gap between the slot array and the cells.
    fn free(&self) -> usize {
        self.upper - self.slots_end()
    }

    /// Whether a `cell_size`-byte cell and its slot fit the free gap.
    fn fits(&self, cell_size: usize) -> bool {
        cell_size.saturating_add(SLOT) <= self.free()
    }

    /// The bytes of every cell plus its slot: `U − free`.
    fn used(&self) -> usize {
        self.end - self.upper + SLOT * self.nkeys
    }
}

// The hot accessors below keep their error messages out of line, in `#[cold]` functions that
// are never inlined: a header or cell check is a few compares on the fast path.

/// The cheap per-access check: a supported buffer length, a known kind, flags zero and the slot
/// array ending at or below `upper`, which is at or below the trailer. Every slot read is then in
/// bounds.
#[inline]
fn hdr(page: &[u8]) -> Result<Hdr> {
    let len = page.len();
    let kind = match page.get(KIND_OFF) {
        Some(&KIND_LEAF) => Kind::Leaf,
        Some(&KIND_BRANCH) => Kind::Branch,
        _ => return Err(hdr_error(page)),
    };
    let (nkeys, upper) = (nkeys(page), upper(page));
    if !valid_len(len) || flags(page) != 0 || upper > len - TRAILER || HEADER + SLOT * nkeys > upper
    {
        return Err(hdr_error(page));
    }
    Ok(Hdr { kind, nkeys, upper, end: len - TRAILER })
}

/// The [`Error::Corrupt`] of a header [`hdr`] refused, naming the first of its checks in order
/// that fails.
#[cold]
#[inline(never)]
fn hdr_error(page: &[u8]) -> Error {
    let len = page.len();
    if !valid_len(len) {
        return corrupt(page, format_args!("page buffer of {len} bytes"));
    }
    if let Err(e) = kind(page) {
        return e;
    }
    if flags(page) != 0 {
        return corrupt(page, format_args!("page flags {:#x}", flags(page)));
    }
    let (nkeys, upper) = (nkeys(page), upper(page));
    corrupt(page, format_args!("{nkeys} slots and upper {upper} do not fit"))
}

/// [`hdr`] for a page that must be of kind `want`.
#[inline]
fn hdr_of(page: &[u8], want: Kind) -> Result<Hdr> {
    let h = hdr(page)?;
    if h.kind != want {
        return Err(kind_error(page, want, h.kind));
    }
    Ok(h)
}

/// [`hdr_of`]'s [`Error::Corrupt`] for a page of the wrong kind.
#[cold]
#[inline(never)]
fn kind_error(page: &[u8], want: Kind, found: Kind) -> Error {
    corrupt(page, format_args!("expected a {want:?} page, found a {found:?}"))
}

// ---- key order ----

/// The byte-wise lexicographic order of keys, a shorter key first when it is a prefix of the
/// longer: the order of `<[u8]>::cmp`, compared inline eight bytes at a time as big-endian words.
/// Every key comparison of a search goes through it.
#[inline]
pub(crate) fn cmp_keys(a: &[u8], b: &[u8]) -> Ordering {
    let n = a.len().min(b.len());
    let (a_words, a_tail) = a[..n].as_chunks::<8>();
    let (b_words, b_tail) = b[..n].as_chunks::<8>();
    for (x, y) in a_words.iter().zip(b_words) {
        if x != y {
            return u64::from_be_bytes(*x).cmp(&u64::from_be_bytes(*y));
        }
    }
    for (x, y) in a_tail.iter().zip(b_tail) {
        if x != y {
            return x.cmp(y);
        }
    }
    a.len().cmp(&b.len())
}

// ---- cells ----

/// Slot `i` (in bounds for `i < nkeys` once [`hdr`] passed).
fn slot(page: &[u8], i: usize) -> usize {
    read_u16(page, HEADER + SLOT * i)
}

/// Start loading the cell slot `i` names into the CPU cache, ahead of a binary search probe that
/// may read it: a search's probes are dependent cache misses, and this overlaps the next probe's
/// miss with the current one. Only a hint: it reads slot `i` (in bounds for `i < nkeys` once
/// [`hdr`] passed), touches nothing past the page, and never changes what a search returns.
#[inline(always)]
fn prefetch_cell(page: &[u8], i: usize) {
    if let Some(cell) = page.get(slot(page, i)) {
        prefetch(cell);
    }
}

/// [`prefetch_cell`] for both cells the probe after `mid` of a binary search over `[lo, hi)` may
/// read (`lo <= mid < hi <= nkeys`): the midpoint of `[lo, mid)` and of `[mid + 1, hi)`.
#[inline(always)]
fn prefetch_next_probes(page: &[u8], lo: usize, mid: usize, hi: usize) {
    if lo < mid {
        prefetch_cell(page, lo + (mid - lo) / 2);
    }
    if mid + 1 < hi {
        prefetch_cell(page, mid + 1 + (hi - mid - 1) / 2);
    }
}

/// A prefetch hint for the cache line holding `byte` (a no-op on other targets).
#[inline(always)]
fn prefetch(byte: &u8) {
    let _addr: *const u8 = byte;
    // SAFETY: a prefetch is a hint: it never faults, changes no memory, register or flag, and
    // its address is a live byte of the page.
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!(
            "prfm pldl1keep, [{0}]",
            in(reg) _addr,
            options(nostack, preserves_flags, readonly)
        );
    }
    // SAFETY: as above.
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::asm!(
            "prefetcht0 [{0}]",
            in(reg) _addr,
            options(nostack, preserves_flags, readonly)
        );
    }
}

/// The offset of cell `i`, checked to lie in the cell area with room for its fixed part.
#[inline]
fn cell_off(page: &[u8], h: &Hdr, i: usize) -> Result<usize> {
    if i >= h.nkeys {
        return Err(cell_error(page, h, i));
    }
    let off = slot(page, i);
    if off < h.upper || off > h.end - h.kind.cell_header() {
        return Err(cell_error(page, h, i));
    }
    Ok(off)
}

/// [`cell_off`]'s [`Error::Corrupt`] for cell `i`.
#[cold]
#[inline(never)]
fn cell_error(page: &[u8], h: &Hdr, i: usize) -> Error {
    if i >= h.nkeys {
        return corrupt(page, format_args!("cell {i} of {}", h.nkeys));
    }
    let off = slot(page, i);
    corrupt(page, format_args!("cell {i} at {off} outside the cell area {}..{}", h.upper, h.end))
}

/// Leaf cell `i` as `(offset, klen, vlen)`, checked to end inside the cell area and to carry
/// flags 0 (an inline value), [`CELL_OVERFLOW`] with a 12-byte value (an overflow reference) or
/// [`CELL_NESTED`] with a 32-byte value (a nested root, [`leaf_val`]); every other flag is
/// refused. Always inlined: every leaf read runs it, and with this many callers a plain
/// `#[inline]` left it a call in hot paths such as [`crate::RoCursor::current_with_key_tail`] as
/// the adapter crate compiles it. A nested cell fails the inline and overflow tests and is
/// accepted on the cold path ([`leaf_cell_other`]), so an inline or overflow cell runs only the
/// compares it ran before the nested form existed.
#[inline(always)]
fn leaf_cell(page: &[u8], h: &Hdr, i: usize) -> Result<(usize, usize, usize)> {
    let off = cell_off(page, h, i)?;
    let vlen = read_u32(page, off) as usize;
    let klen = read_u16(page, off + 4);
    let cell_flags = read_u16(page, off + 6);
    let room = h.end - off - LEAF_CELL_HEADER;
    if (cell_flags != 0 && (cell_flags != CELL_OVERFLOW || vlen != OVF_CELL))
        || vlen > room
        || klen > room - vlen
    {
        return leaf_cell_other(page, i, off, room, (klen, vlen, cell_flags));
    }
    Ok((off, klen, vlen))
}

/// [`leaf_cell`]'s cold path for cell `i` at `off`, read as `(klen, vlen, flags)` with `room`
/// bytes after its header: a nested cell ([`nested_shape`]) that ends inside the cell area is
/// `(off, klen, vlen)`; any other cell is [`leaf_cell_error`]'s [`Error::Corrupt`].
#[cold]
#[inline(never)]
fn leaf_cell_other(
    page: &[u8],
    i: usize,
    off: usize,
    room: usize,
    (klen, vlen, flags): (usize, usize, usize),
) -> Result<(usize, usize, usize)> {
    if nested_shape(flags, vlen) && vlen <= room && klen <= room - vlen {
        return Ok((off, klen, vlen));
    }
    Err(leaf_cell_error(page, i, off, (klen, vlen, flags)))
}

/// Whether a cell of flags `flags` (neither 0 nor a 12-byte overflow cell's) with a `vlen`-byte
/// value is a nested cell: [`CELL_NESTED`] with [`NESTED_CELL`] bytes. Asked only on the cold
/// paths ([`leaf_cell_other`], [`leaf_cell_error`]): only a dup table's outer leaves hold
/// nested cells, and no hot read path reads one.
#[inline]
fn nested_shape(flags: usize, vlen: usize) -> bool {
    flags == CELL_NESTED && vlen == NESTED_CELL
}

/// The value of the leaf cell at `off` with a `klen`-byte key and a `vlen`-byte value, as
/// [`leaf_cell`] checked it: the bytes, the overflow reference its 12 bytes hold, or the nested
/// root its 32 bytes hold.
#[inline]
fn leaf_val(page: &[u8], off: usize, klen: usize, vlen: usize) -> Val<'_> {
    let v = off + LEAF_CELL_HEADER + klen;
    match read_u16(page, off + 6) {
        0 => Val::Inline(&page[v..v + vlen]),
        CELL_OVERFLOW => {
            Val::Overflow(Ovf::decode(page[v..v + OVF_CELL].try_into().expect("12 bytes")))
        }
        _ => nested_val(page, v),
    }
}

/// [`leaf_val`] of a nested cell whose value starts at `v` ([`leaf_cell`] checked its shape).
#[cold]
#[inline(never)]
fn nested_val(page: &[u8], v: usize) -> Val<'_> {
    Val::Nested(page[v..v + NESTED_CELL].try_into().expect("32 bytes"))
}

/// [`leaf_cell`]'s [`Error::Corrupt`] for cell `i` at `off`, read as `(klen, vlen, flags)`.
#[cold]
#[inline(never)]
fn leaf_cell_error(
    page: &[u8],
    i: usize,
    off: usize,
    (klen, vlen, flags): (usize, usize, usize),
) -> Error {
    if flags > CELL_OVERFLOW && !nested_shape(flags, vlen) {
        return corrupt(page, format_args!("cell {i} has flags {flags:#x}"));
    }
    if flags == CELL_OVERFLOW && vlen != OVF_CELL {
        return corrupt(page, format_args!("overflow cell {i} has a {vlen}-byte value"));
    }
    corrupt(page, format_args!("cell {i} at {off} ({klen}+{vlen} bytes) overruns"))
}

/// Branch cell `i` as `(offset, klen)`, checked to end inside the cell area.
#[inline]
fn branch_cell(page: &[u8], h: &Hdr, i: usize) -> Result<(usize, usize)> {
    let off = cell_off(page, h, i)?;
    let klen = read_u16(page, off + 8);
    if klen > h.end - off - BRANCH_CELL_HEADER {
        return Err(branch_cell_error(page, i, off, klen));
    }
    Ok((off, klen))
}

/// [`branch_cell`]'s [`Error::Corrupt`] for cell `i` at `off` with a `klen`-byte key.
#[cold]
#[inline(never)]
fn branch_cell_error(page: &[u8], i: usize, off: usize, klen: usize) -> Error {
    corrupt(page, format_args!("cell {i} at {off} ({klen} key bytes) overruns"))
}

/// Cell `i` of either kind as `(offset, size)`.
fn cell_extent(page: &[u8], h: &Hdr, i: usize) -> Result<(usize, usize)> {
    match h.kind {
        Kind::Leaf => {
            let (off, klen, vlen) = leaf_cell(page, h, i)?;
            Ok((off, LEAF_CELL_HEADER + klen + vlen))
        }
        Kind::Branch => {
            let (off, klen) = branch_cell(page, h, i)?;
            Ok((off, BRANCH_CELL_HEADER + klen))
        }
    }
}

/// The key of cell `i` of either kind (empty for a branch's cell 0).
fn key_at<'a>(page: &'a [u8], h: &Hdr, i: usize) -> Result<&'a [u8]> {
    let (k, klen) = match h.kind {
        Kind::Leaf => {
            let (off, klen, _) = leaf_cell(page, h, i)?;
            (off + LEAF_CELL_HEADER, klen)
        }
        Kind::Branch => {
            let (off, klen) = branch_cell(page, h, i)?;
            (off + BRANCH_CELL_HEADER, klen)
        }
    };
    Ok(&page[k..k + klen])
}

/// Make room for a `size`-byte cell at index `i`: the slots from `i` move up one, slot `i` takes
/// the new cell's offset (returned; its bytes are for the caller to write). `None`, with the page
/// untouched, if the cell and its slot do not fit the free space.
#[inline]
fn reserve(page: &mut [u8], h: &Hdr, i: usize, size: usize) -> Result<Option<usize>> {
    if i > h.nkeys {
        return Err(reserve_error(page, h, i));
    }
    if size.saturating_add(SLOT) > h.free() {
        return Ok(None);
    }
    let off = h.upper - size;
    let at = HEADER + SLOT * i;
    // A cell at the end (an append) moves no slot: skip the empty move's `memmove` call.
    if i < h.nkeys {
        page.copy_within(at..h.slots_end(), at + SLOT);
    }
    put_u16(page, at, off);
    put_u16(page, NKEYS_OFF, h.nkeys + 1);
    put_u16(page, UPPER_OFF, off);
    Ok(Some(off))
}

/// [`reserve`]'s [`Error::Corrupt`] for an index past the cells.
#[cold]
#[inline(never)]
fn reserve_error(page: &[u8], h: &Hdr, i: usize) -> Error {
    corrupt(page, format_args!("insert at {i} of {}", h.nkeys))
}

/// Remove cell `i` and compact: the cells below it move up over its bytes, their slots follow,
/// and slot `i` leaves the array.
fn remove_cell(page: &mut [u8], h: &Hdr, i: usize) -> Result<()> {
    let (off, size) = cell_extent(page, h, i)?;
    page.copy_within(h.upper..off, h.upper + size);
    for j in 0..h.nkeys {
        let o = slot(page, j);
        if h.upper <= o && o < off {
            put_u16(page, HEADER + SLOT * j, o + size);
        }
    }
    let at = HEADER + SLOT * i;
    page.copy_within(at + SLOT..h.slots_end(), at);
    put_u16(page, NKEYS_OFF, h.nkeys - 1);
    put_u16(page, UPPER_OFF, h.upper + size);
    Ok(())
}

/// Resize the cell at `off` by moving the cells below it, together with its own first `keep`
/// bytes, so that the cell area starts at `new_upper`: the cell grows (`new_upper < upper`) or
/// shrinks at byte `keep` while its tail stays in place, and every slot addressing a cell at or
/// below `off` follows. The caller has checked the free space for growth, and that a shrink is no
/// larger than the cell's bytes past `keep`.
fn shift_front(page: &mut [u8], h: &Hdr, off: usize, keep: usize, new_upper: usize) {
    page.copy_within(h.upper..off + keep, new_upper);
    for j in 0..h.nkeys {
        let o = slot(page, j);
        if h.upper <= o && o <= off {
            put_u16(page, HEADER + SLOT * j, new_upper + (o - h.upper));
        }
    }
    put_u16(page, UPPER_OFF, new_upper);
}

/// Bytes a leaf cell takes (without its slot).
pub(crate) const fn cell_size_leaf(klen: usize, vlen: usize) -> usize {
    LEAF_CELL_HEADER.saturating_add(klen).saturating_add(vlen)
}

/// Bytes a branch cell takes (without its slot).
pub(crate) const fn cell_size_branch(klen: usize) -> usize {
    BRANCH_CELL_HEADER.saturating_add(klen)
}

/// The free gap of a page: `upper − (24 + 2·nkeys)` ([`View::free_space`]).
#[cfg(test)]
pub(crate) fn free_space(page: &[u8]) -> Result<usize> {
    Ok(View::parsed(page, hdr(page)?).free_space())
}

/// Whether a `cell_size`-byte cell and its slot fit the page's free space ([`View::fits`]).
pub(crate) fn fits(page: &[u8], cell_size: usize) -> Result<bool> {
    Ok(View::parsed(page, hdr(page)?).fits(cell_size))
}

// ---- parsed views ----
//
// A view holds a page whose header passed `hdr` once, so a caller that reads several cells of
// one page (a search, the range check, an entry per cursor step) parses the header once instead
// of once per call. Every cell a view reads still gets the per-cell checks of `cell_off`,
// `leaf_cell` and `branch_cell`. The free readers (`leaf_find`, `branch_route`, `keys_in_range`,
// ...) are their view's method behind a fresh parse, so both give the same results and errors.
// They build the view from their own `hdr` call rather than through the `new` constructors: a
// one-shot read through a returned `Result<view>` measured a few percent slower (a cursor step is
// one `leaf_entry`), and the constructors are for callers that read several cells of one page.

/// A page of either kind behind one header parse.
#[derive(Clone, Copy, Debug)]
pub(crate) enum View<'a> {
    /// A leaf page.
    Leaf(LeafView<'a>),
    /// A branch page.
    Branch(BranchView<'a>),
}

impl<'a> View<'a> {
    /// Parse `page`'s header: [`Error::Corrupt`] unless it passes the O(1) checks of [`hdr`].
    #[inline]
    pub(crate) fn new(page: &'a [u8]) -> Result<Self> {
        Ok(Self::parsed(page, hdr(page)?))
    }

    /// The view of `page` with the header `h` that [`hdr`] parsed from it.
    #[inline]
    fn parsed(page: &'a [u8], h: Hdr) -> Self {
        match h.kind {
            Kind::Leaf => Self::Leaf(LeafView { page, h }),
            Kind::Branch => Self::Branch(BranchView { page, h }),
        }
    }

    /// The free gap: `upper − (24 + 2·nkeys)`.
    #[cfg(test)]
    pub(crate) fn free_space(&self) -> usize {
        match self {
            Self::Leaf(v) => v.free_space(),
            Self::Branch(v) => v.free_space(),
        }
    }

    /// Whether a `cell_size`-byte cell and its slot fit the free gap.
    #[inline]
    pub(crate) fn fits(&self, cell_size: usize) -> bool {
        match self {
            Self::Leaf(v) => v.fits(cell_size),
            Self::Branch(v) => v.fits(cell_size),
        }
    }

    /// The bytes of every cell plus its slot (`U − free`): what a merge lays out
    /// ([`SplitBuf::merge`]).
    #[inline]
    pub(crate) fn used(&self) -> usize {
        match self {
            Self::Leaf(v) => v.h.used(),
            Self::Branch(v) => v.h.used(),
        }
    }

    /// Whether every key lies in `[lo, hi)` ([`LeafView::keys_in_range`],
    /// [`BranchView::keys_in_range`]).
    #[inline]
    pub(crate) fn keys_in_range(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<bool> {
        match self {
            Self::Leaf(v) => v.keys_in_range(lo, hi),
            Self::Branch(v) => v.keys_in_range(lo, hi),
        }
    }
}

/// A leaf page whose header passed the O(1) checks of [`hdr`] once (see [`View`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct LeafView<'a> {
    page: &'a [u8],
    h: Hdr,
}

impl<'a> LeafView<'a> {
    /// Parse `page`'s header: [`Error::Corrupt`] unless it passes [`hdr`]'s checks and is a
    /// leaf's. A descent builds its views with [`View::new`]; this is for a leaf the caller
    /// entered before and holds again (a cursor's seek from its leaf).
    #[inline]
    pub(crate) fn new(page: &'a [u8]) -> Result<Self> {
        Ok(Self { page, h: hdr_of(page, Kind::Leaf)? })
    }

    /// The page's bytes.
    #[inline]
    pub(crate) fn page(&self) -> &'a [u8] {
        self.page
    }

    /// The number of cells.
    #[inline]
    pub(crate) fn nkeys(&self) -> usize {
        self.h.nkeys
    }

    /// The free gap: `upper − (24 + 2·nkeys)`.
    #[inline]
    pub(crate) fn free_space(&self) -> usize {
        self.h.free()
    }

    /// The bytes of every cell plus its slot ([`View::used`]).
    #[inline]
    pub(crate) fn used(&self) -> usize {
        self.h.used()
    }

    /// Whether a `cell_size`-byte cell and its slot fit the free gap.
    #[inline]
    pub(crate) fn fits(&self, cell_size: usize) -> bool {
        self.h.fits(cell_size)
    }

    /// Whether every key lies in `[lo, hi)` (`None`: unbounded): the first and the last key
    /// decide it, each read only when its bound is given.
    #[inline]
    pub(crate) fn keys_in_range(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<bool> {
        keys_within(self.page, &self.h, lo, hi)
    }

    /// The key of entry `i` (the leaf arm of `key_at`).
    #[inline]
    pub(crate) fn key(&self, i: usize) -> Result<&'a [u8]> {
        let (off, klen, _) = leaf_cell(self.page, &self.h, i)?;
        let k = off + LEAF_CELL_HEADER;
        Ok(&self.page[k..k + klen])
    }

    /// Entry `i` as `(key, value)`.
    #[inline(always)]
    pub(crate) fn entry(&self, i: usize) -> Result<(&'a [u8], Val<'a>)> {
        let page = self.page;
        let (off, klen, vlen) = leaf_cell(page, &self.h, i)?;
        let k = off + LEAF_CELL_HEADER;
        Ok((&page[k..k + klen], leaf_val(page, off, klen, vlen)))
    }

    /// Entry `i` as its key and, for an inline value, the key's last `t` bytes followed by the
    /// value: `key[klen − t..] ‖ value`, one slice of the cell, since a cell holds its value right
    /// after its key. An overflow value is its reference, as [`Self::entry`] gives it. A `t` past
    /// the key's length is the key's length (the slice is then `key ‖ value`): the slice never
    /// reaches into the cell's fixed part.
    #[inline(always)]
    pub(crate) fn entry_with_key_tail(&self, i: usize, t: usize) -> Result<(&'a [u8], Val<'a>)> {
        let page = self.page;
        let (off, klen, vlen) = leaf_cell(page, &self.h, i)?;
        let k = off + LEAF_CELL_HEADER;
        let v = k + klen;
        let val = match leaf_val(page, off, klen, vlen) {
            Val::Inline(_) => Val::Inline(&page[v - t.min(klen)..v + vlen]),
            other @ (Val::Overflow(_) | Val::Nested(_)) => other,
        };
        Ok((&page[k..v], val))
    }

    /// Binary-search for `key`.
    #[inline]
    pub(crate) fn search(&self, key: &[u8]) -> Result<Search> {
        let (mut lo, mut hi) = (0, self.h.nkeys);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            prefetch_next_probes(self.page, lo, mid, hi);
            match cmp_keys(self.key(mid)?, key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(Search::Found(mid)),
            }
        }
        Ok(Search::NotFound(lo))
    }

    /// [`LeafView::search`] that also returns what the search read: the value of the entry
    /// found, or the keys either side of the insertion point. Every cell it reads gets the checks
    /// of [`LeafView::entry`].
    #[inline]
    pub(crate) fn find(&self, key: &[u8]) -> Result<Find<'a>> {
        let page = self.page;
        let (mut lo, mut hi) = (0, self.h.nkeys);
        // The keys of the last probes either way: cell `lo − 1` once `lo` moved, cell `hi` once
        // `hi` moved (the final `lo` and `hi` are the last values assigned).
        let (mut before, mut after) = (None, None);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            prefetch_next_probes(page, lo, mid, hi);
            let (off, klen, vlen) = leaf_cell(page, &self.h, mid)?;
            let k = off + LEAF_CELL_HEADER;
            let probe = &page[k..k + klen];
            match cmp_keys(probe, key) {
                Ordering::Less => (lo, before) = (mid + 1, Some(probe)),
                Ordering::Greater => (hi, after) = (mid, Some(probe)),
                Ordering::Equal => {
                    return Ok(Find::Found { index: mid, value: leaf_val(page, off, klen, vlen) })
                }
            }
        }
        Ok(Find::NotFound { index: lo, before, after })
    }
}

/// A branch page whose header passed the O(1) checks of [`hdr`] once (see [`View`]).
#[derive(Clone, Copy, Debug)]
pub(crate) struct BranchView<'a> {
    page: &'a [u8],
    h: Hdr,
}

impl<'a> BranchView<'a> {
    /// Parse `page`'s header: [`Error::Corrupt`] unless it passes [`hdr`]'s checks and is a
    /// branch's.
    #[inline]
    pub(crate) fn new(page: &'a [u8]) -> Result<Self> {
        Ok(Self { page, h: hdr_of(page, Kind::Branch)? })
    }

    /// The number of cells (children).
    #[inline]
    pub(crate) fn nkeys(&self) -> usize {
        self.h.nkeys
    }

    /// The free gap: `upper − (24 + 2·nkeys)`.
    #[cfg(test)]
    pub(crate) fn free_space(&self) -> usize {
        self.h.free()
    }

    /// The bytes of every cell plus its slot ([`View::used`]).
    #[inline]
    pub(crate) fn used(&self) -> usize {
        self.h.used()
    }

    /// Whether a `cell_size`-byte cell and its slot fit the free gap.
    #[inline]
    pub(crate) fn fits(&self, cell_size: usize) -> bool {
        self.h.fits(cell_size)
    }

    /// Whether every key lies in `[lo, hi)` (`None`: unbounded): the key of cell 1 and the last
    /// key decide it, each read only when its bound is given; cell 0 (key −∞) is skipped, its
    /// lower bound being `lo` itself.
    #[inline]
    pub(crate) fn keys_in_range(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<bool> {
        keys_within(self.page, &self.h, lo, hi)
    }

    /// The key of cell `i`: the lower bound of child `i`'s range (empty for cell 0: −∞).
    #[inline]
    pub(crate) fn key(&self, i: usize) -> Result<&'a [u8]> {
        key_at(self.page, &self.h, i)
    }

    /// Child `i` as `(child_pgno, child_txn)`; `child_txn` is the low 32 bits of the child's
    /// header txn, checked on every descent.
    #[inline]
    pub(crate) fn child(&self, i: usize) -> Result<(u32, u32)> {
        let (off, _) = branch_cell(self.page, &self.h, i)?;
        Ok((read_u32(self.page, off), read_u32(self.page, off + 4)))
    }

    /// The child to descend into for `key` (the largest `i` with `key_i <= key`, where
    /// `key_0 = −∞`) with its bounds and pointer ([`Route`]: what [`BranchView::key`] and
    /// [`BranchView::child`] give), from the cells the binary search read; every cell it reads
    /// gets the checks of [`BranchView::child`]. An empty branch is [`Error::Corrupt`].
    #[inline]
    pub(crate) fn route(&self, key: &[u8]) -> Result<Route<'a>> {
        let (page, h) = (self.page, &self.h);
        if h.nkeys == 0 {
            return Err(corrupt(page, "branch page without children"));
        }
        let (mut lo, mut hi) = (1, h.nkeys);
        // The cells of the last probes either way: cell `lo − 1` (offset and key) once `lo`
        // moved, the key of cell `hi` once `hi` moved (the final `lo` and `hi` are the last
        // values assigned).
        let (mut below, mut above) = (None, None);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            prefetch_next_probes(page, lo, mid, hi);
            let (off, klen) = branch_cell(page, h, mid)?;
            let k = off + BRANCH_CELL_HEADER;
            let probe = &page[k..k + klen];
            if cmp_keys(probe, key).is_le() {
                (lo, below) = (mid + 1, Some((off, probe)));
            } else {
                (hi, above) = (mid, Some(probe));
            }
        }
        let off = match below {
            Some((off, _)) => off,
            None => branch_cell(page, h, 0)?.0,
        };
        Ok(Route {
            index: lo - 1,
            lo: below.map(|(_, k)| k),
            hi: above,
            child: read_u32(page, off),
            child_txn: read_u32(page, off + 4),
        })
    }

    /// The [`Route`] to child `i`.
    #[inline]
    pub(crate) fn route_at(&self, i: usize) -> Result<Route<'a>> {
        let (page, h) = (self.page, &self.h);
        let (off, klen) = branch_cell(page, h, i)?;
        let k = off + BRANCH_CELL_HEADER;
        let hi = if i + 1 < h.nkeys { Some(key_at(page, h, i + 1)?) } else { None };
        Ok(Route {
            index: i,
            lo: (i > 0).then(|| &page[k..k + klen]),
            hi,
            child: read_u32(page, off),
            child_txn: read_u32(page, off + 4),
        })
    }
}

// ---- whole-page check ----

/// Check every structural rule of a page, never panicking on any bytes: a buffer of the
/// geometry's page size; a known kind, flags and reserved header bytes zero, the slot array below
/// `upper` and `upper` below the trailer; every slot addressing a cell that lies in the cell area
/// with lengths inside the page; a leaf cell's flags 0, or 1 for an overflow cell, whose value is
/// 12 bytes naming a length past the inline limit ([`Geometry::max_value`]) and within
/// [`MAX_VALUE`], or 2 for a nested cell, whose 32 bytes decode to a non-empty root that passes
/// [`TreeRoot::check`] (its pages are its use's to check); the cells tiling `upper..P − 4` exactly
/// (no overlap, no gap); every entry within the geometry's maximal entry (`U / 4`) and every key
/// within its `max_key` (for a branch cell the entry limit is that, for a leaf cell it is not);
/// keys strictly ascending; and for a branch at least one child, with cell 0's key empty. The page
/// number, transaction id and CRC are the caller's to check.
pub(crate) fn validate(page: &[u8], geometry: &Geometry) -> Result<()> {
    if page.len() != geometry.page_size {
        return Err(corrupt(
            page,
            format_args!("{} bytes, expected {}", page.len(), geometry.page_size),
        ));
    }
    let h = hdr(page)?;
    if page[RESERVED_OFF..HEADER].iter().any(|&b| b != 0) {
        return Err(corrupt(page, "reserved header bytes are not zero"));
    }
    if h.nkeys * (h.kind.cell_header() + SLOT) > geometry.usable {
        return Err(corrupt(page, format_args!("{} cells cannot fit", h.nkeys)));
    }
    if h.kind == Kind::Branch && h.nkeys == 0 {
        return Err(corrupt(page, "branch page without children"));
    }
    let mut extents = Vec::with_capacity(h.nkeys);
    let mut prev: Option<&[u8]> = None;
    for i in 0..h.nkeys {
        let (off, size) = cell_extent(page, &h, i)?;
        if size + SLOT > geometry.max_entry {
            return Err(corrupt(page, format_args!("cell {i} of {size} bytes exceeds the limit")));
        }
        let key = key_at(page, &h, i)?;
        if key.len() > geometry.max_key() {
            return Err(corrupt(
                page,
                format_args!("cell {i} has a key of {} bytes over the limit", key.len()),
            ));
        }
        if h.kind == Kind::Branch && i == 0 && !key.is_empty() {
            return Err(corrupt(page, "branch cell 0 has a key"));
        }
        if h.kind == Kind::Leaf {
            let (off, klen, vlen) = leaf_cell(page, &h, i)?;
            match leaf_val(page, off, klen, vlen) {
                Val::Inline(_) => {}
                Val::Overflow(ovf) => {
                    let len = ovf.len as usize;
                    if len <= geometry.max_value(klen) || len > MAX_VALUE {
                        return Err(corrupt(
                            page,
                            format_args!("overflow cell {i} holds a value of {len} bytes"),
                        ));
                    }
                }
                Val::Nested(root) => {
                    if !TreeRoot::decode(root).is_ok_and(|r| !r.is_empty()) {
                        return Err(corrupt(
                            page,
                            format_args!("nested cell {i} holds an impossible or empty tree root"),
                        ));
                    }
                }
            }
        }
        if prev.is_some_and(|p| p >= key) {
            return Err(corrupt(page, format_args!("cell {i} is out of key order")));
        }
        prev = Some(key);
        extents.push((off, size));
    }
    extents.sort_unstable();
    let mut at = h.upper;
    for (off, size) in extents {
        if off != at {
            return Err(corrupt(page, format_args!("cell at {off} where {at} was expected")));
        }
        at = off + size;
    }
    if at != h.end {
        return Err(corrupt(page, format_args!("cells end at {at}, not {}", h.end)));
    }
    Ok(())
}

/// Whether every key of the page lies in `[lo, hi)`, the range the separators on the way down
/// give it (`None`: unbounded). Keys are sorted, so the first and the last decide it; a branch's
/// cell 0 (key −∞) is skipped, its lower bound being `lo` itself.
pub(crate) fn keys_in_range(page: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<bool> {
    View::parsed(page, hdr(page)?).keys_in_range(lo, hi)
}

/// The range check of [`keys_in_range`] on a parsed header.
fn keys_within(page: &[u8], h: &Hdr, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<bool> {
    let first = usize::from(h.kind == Kind::Branch);
    if h.nkeys <= first {
        return Ok(true);
    }
    let lo_ok = match lo {
        Some(lo) => cmp_keys(key_at(page, h, first)?, lo).is_ge(),
        None => true,
    };
    let hi_ok = match hi {
        Some(hi) => cmp_keys(key_at(page, h, h.nkeys - 1)?, hi).is_lt(),
        None => true,
    };
    Ok(lo_ok && hi_ok)
}

// ---- leaf ----

#[inline]
fn write_leaf_cell(cell: &mut [u8], key: &[u8], value: impl CellVal) {
    put_u16(cell, 4, key.len());
    let k = LEAF_CELL_HEADER + key.len();
    copy_short(&mut cell[LEAF_CELL_HEADER..k], key);
    value.write(cell, k);
}

/// The key of leaf entry `i` ([`LeafView::key`]).
pub(crate) fn leaf_key(page: &[u8], i: usize) -> Result<&[u8]> {
    LeafView { page, h: hdr_of(page, Kind::Leaf)? }.key(i)
}

/// Test-only: leaf entry `i` of a tree of inline values (the catalog, the free list): an
/// overflow reference is [`Error::Corrupt`] ([`Val::inline`]).
#[cfg(test)]
pub(crate) fn leaf_inline_entry(page: &[u8], i: usize) -> Result<(&[u8], &[u8])> {
    let (k, v) = leaf_entry(page, i)?;
    Ok((k, v.inline()?))
}

/// Leaf entry `i` as `(key, value)` ([`LeafView::entry`]).
pub(crate) fn leaf_entry(page: &[u8], i: usize) -> Result<(&[u8], Val<'_>)> {
    LeafView { page, h: hdr_of(page, Kind::Leaf)? }.entry(i)
}

/// Leaf entry `i` as its key and its value joined to the key's last `t` bytes
/// ([`LeafView::entry_with_key_tail`]).
#[inline]
pub(crate) fn leaf_entry_with_key_tail(
    page: &[u8],
    i: usize,
    t: usize,
) -> Result<(&[u8], Val<'_>)> {
    LeafView { page, h: hdr_of(page, Kind::Leaf)? }.entry_with_key_tail(i, t)
}

/// Binary-search a leaf for `key` ([`LeafView::search`]).
pub(crate) fn leaf_search(page: &[u8], key: &[u8]) -> Result<Search> {
    LeafView { page, h: hdr_of(page, Kind::Leaf)? }.search(key)
}

/// [`leaf_search`] that also returns what the search read: the value of the entry found, or the
/// keys either side of the insertion point ([`LeafView::find`]). One header check; every cell it
/// reads gets the checks of [`leaf_entry`].
pub(crate) fn leaf_find<'a>(page: &'a [u8], key: &[u8]) -> Result<Find<'a>> {
    LeafView { page, h: hdr_of(page, Kind::Leaf)? }.find(key)
}

/// Insert `(key, value)` as entry `i` (the sorted position: [`Search::NotFound`]). The caller has
/// checked the sizes with [`Geometry::check_entry`]. [`Fit::Full`] leaves the page unchanged.
/// Test-only: the engine changes a leaf through [`LeafMut`].
#[cfg(test)]
pub(crate) fn leaf_insert(page: &mut [u8], i: usize, key: &[u8], value: &[u8]) -> Result<Fit> {
    LeafMut::new(page)?.insert(i, key, value)
}

/// Replace the value of leaf entry `i`: in place when the size is unchanged, otherwise by
/// resizing the cell in place (the cells below it shift by the difference). [`Fit::Full`] (a
/// larger value than the free space allows) leaves the page unchanged. Test-only, like
/// [`leaf_insert`].
#[cfg(test)]
pub(crate) fn leaf_replace_value(page: &mut [u8], i: usize, value: &[u8]) -> Result<Fit> {
    LeafMut::new(page)?.replace_value(i, value)
}

/// A leaf page open for one change behind one header parse: read it through [`LeafMut::view`],
/// then change it with [`LeafMut::insert`] or [`LeafMut::replace_value`], which take the parsed
/// header instead of parsing it again. A put in a leaf reads and changes the page this way.
#[derive(Debug)]
pub(crate) struct LeafMut<'a> {
    page: &'a mut [u8],
    h: Hdr,
}

impl<'a> LeafMut<'a> {
    /// Parse `page`'s header: [`Error::Corrupt`] unless it passes [`hdr`]'s checks and is a
    /// leaf's.
    #[inline]
    pub(crate) fn new(page: &'a mut [u8]) -> Result<Self> {
        let h = hdr_of(page, Kind::Leaf)?;
        Ok(Self { page, h })
    }

    /// The page read through its parsed header.
    #[inline]
    pub(crate) fn view(&self) -> LeafView<'_> {
        LeafView { page: self.page, h: self.h }
    }

    /// [`leaf_insert`] of an inline value or an overflow reference ([`CellVal`]).
    #[inline]
    pub(crate) fn insert(self, i: usize, key: &[u8], value: impl CellVal) -> Result<Fit> {
        let Self { page, h } = self;
        let size = cell_size_leaf(key.len(), value.cell_len());
        let Some(off) = reserve(page, &h, i, size)? else {
            return Ok(Fit::Full);
        };
        write_leaf_cell(&mut page[off..off + size], key, value);
        Ok(Fit::Done)
    }

    /// [`leaf_replace_value`] with an inline value or an overflow reference (either may replace
    /// either, [`CellVal`]).
    #[inline]
    pub(crate) fn replace_value(self, i: usize, value: impl CellVal) -> Result<Fit> {
        let Self { page, h } = self;
        let (off, klen, vlen) = leaf_cell(page, &h, i)?;
        let keep = LEAF_CELL_HEADER + klen;
        let new_len = value.cell_len();
        let off = match new_len.cmp(&vlen) {
            Ordering::Equal => off,
            Ordering::Greater => {
                let grow = new_len - vlen;
                if grow > h.free() {
                    return Ok(Fit::Full);
                }
                shift_front(page, &h, off, keep, h.upper - grow);
                off - grow
            }
            Ordering::Less => {
                let shrink = vlen - new_len;
                shift_front(page, &h, off, keep, h.upper + shrink);
                off + shrink
            }
        };
        value.write(&mut page[off..off + keep + new_len], keep);
        Ok(Fit::Done)
    }

    /// [`leaf_delete`] of entry `i` through the parsed header.
    #[inline]
    pub(crate) fn delete(self, i: usize) -> Result<()> {
        let Self { page, h } = self;
        remove_cell(page, &h, i)
    }
}

/// Delete leaf entry `i`, compacting the cell area.
pub(crate) fn leaf_delete(page: &mut [u8], i: usize) -> Result<()> {
    let h = hdr_of(page, Kind::Leaf)?;
    remove_cell(page, &h, i)
}

// ---- branch ----

fn write_branch_cell(cell: &mut [u8], key: &[u8], child: u32, child_txn: u32) {
    put_u32(cell, 0, child);
    put_u32(cell, 4, child_txn);
    put_u16(cell, 8, key.len());
    copy_short(&mut cell[BRANCH_CELL_HEADER..BRANCH_CELL_HEADER + key.len()], key);
}

/// Child `i` of a branch as `(child_pgno, child_txn)`; `child_txn` is the low 32 bits of the
/// child's header txn, checked on every descent ([`BranchView::child`]).
pub(crate) fn branch_child(page: &[u8], i: usize) -> Result<(u32, u32)> {
    BranchView { page, h: hdr_of(page, Kind::Branch)? }.child(i)
}

/// The key of branch cell `i`: the lower bound of child `i`'s range (empty for cell 0: −∞)
/// ([`BranchView::key`]).
pub(crate) fn branch_key(page: &[u8], i: usize) -> Result<&[u8]> {
    BranchView { page, h: hdr_of(page, Kind::Branch)? }.key(i)
}

/// The child index to descend into for `key`: the largest `i` with `key_i <= key`, where
/// `key_0 = −∞`. An empty branch is [`Error::Corrupt`]. The tree routes with [`branch_route`];
/// this is its model in the tests.
#[cfg(test)]
pub(crate) fn branch_search(page: &[u8], key: &[u8]) -> Result<usize> {
    let h = hdr_of(page, Kind::Branch)?;
    if h.nkeys == 0 {
        return Err(corrupt(page, "branch page without children"));
    }
    // Invariant: key_(lo−1) <= key, and key_hi > key when hi < nkeys.
    let (mut lo, mut hi) = (1, h.nkeys);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if cmp_keys(key_at(page, &h, mid)?, key).is_le() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo - 1)
}

/// The child to descend into for `key` (the largest `i` with `key_i <= key`, where `key_0 = −∞`)
/// with its bounds and pointer ([`Route`]: what [`branch_key`] and [`branch_child`] give), from
/// one header check and the cells the binary search read ([`BranchView::route`]); every cell it
/// reads gets the checks of [`branch_child`]. An empty branch is [`Error::Corrupt`].
pub(crate) fn branch_route<'a>(page: &'a [u8], key: &[u8]) -> Result<Route<'a>> {
    BranchView { page, h: hdr_of(page, Kind::Branch)? }.route(key)
}

/// The [`Route`] to child `i` of a branch, from one header check ([`BranchView::route_at`]).
pub(crate) fn branch_route_at(page: &[u8], i: usize) -> Result<Route<'_>> {
    BranchView { page, h: hdr_of(page, Kind::Branch)? }.route_at(i)
}

/// Insert a child as cell `i` with lower-bound `key`: `i >= 1` with a non-empty key above
/// `key_(i−1)` and below `key_i`, or `i == 0` with an empty key into an empty branch.
/// [`Fit::Full`] leaves the page unchanged.
pub(crate) fn branch_insert(
    page: &mut [u8],
    i: usize,
    key: &[u8],
    child: u32,
    child_txn: u32,
) -> Result<Fit> {
    let h = hdr_of(page, Kind::Branch)?;
    debug_assert!(
        if i == 0 { key.is_empty() && h.nkeys == 0 } else { !key.is_empty() },
        "branch cell {i} with a {}-byte key in a branch of {}",
        key.len(),
        h.nkeys
    );
    let size = cell_size_branch(key.len());
    let Some(off) = reserve(page, &h, i, size)? else {
        return Ok(Fit::Full);
    };
    write_branch_cell(&mut page[off..off + size], key, child, child_txn);
    Ok(Fit::Done)
}

/// Point branch cell `i` at another child (a copy-on-write copy of it), in place.
pub(crate) fn branch_set_child(
    page: &mut [u8],
    i: usize,
    child: u32,
    child_txn: u32,
) -> Result<()> {
    let h = hdr_of(page, Kind::Branch)?;
    let (off, _) = branch_cell(page, &h, i)?;
    put_u32(page, off, child);
    put_u32(page, off + 4, child_txn);
    Ok(())
}

/// Remove child `i` of a branch, compacting the cell area; its left neighbour takes over its key
/// range. Removing child 0 makes child 1 the new cell 0, whose key is dropped (it now covers the
/// range from −∞).
pub(crate) fn branch_delete(page: &mut [u8], i: usize) -> Result<()> {
    let h = hdr_of(page, Kind::Branch)?;
    remove_cell(page, &h, i)?;
    if i == 0 && h.nkeys > 1 {
        let h = hdr_of(page, Kind::Branch)?;
        let (off, klen) = branch_cell(page, &h, 0)?;
        shift_front(page, &h, off, BRANCH_CELL_HEADER, h.upper + klen);
        put_u16(page, off + klen + 8, 0);
    }
    Ok(())
}

/// Lay out `page`, whatever its bytes (a page just allocated), as a new root over two children:
/// `left` (cell 0, from −∞) and `right` (from `sep`), each `(child_pgno, child_txn)`
/// ([`init_header`], then the two cells).
pub(crate) fn init_root(
    page: &mut [u8],
    pgno: u32,
    txn: u64,
    left: (u32, u32),
    sep: &[u8],
    right: (u32, u32),
) -> Result<()> {
    init_header(page, Kind::Branch, pgno, txn);
    if branch_insert(page, 0, &[], left.0, left.1)? == Fit::Full
        || branch_insert(page, 1, sep, right.0, right.1)? == Fit::Full
    {
        return Err(corrupt(page, format_args!("a {}-byte separator overflows a root", sep.len())));
    }
    Ok(())
}

// ---- splits ----

/// The buffers of a split, reused from one split to the next (the writer owns one), so a split
/// allocates nothing once they have grown. A split never borrows two pages at once, and checks
/// everything before it changes a page:
/// 1. [`Self::split_leaf`] or [`Self::split_branch`] plans it from the page that splits (`src`): it
///    copies `src`, encodes the pending cell after the copy, picks the cut and checks that both
///    halves fit. It changes no page, so an error leaves both pages unchanged.
/// 2. [`Self::write_right`] writes the right half into its new page, one just allocated (its bytes
///    not zeroed: every byte but the free gap is written).
/// 3. [`Self::write_left`] rewrites `src` as the left half, keeping its page number and txn.
///
/// [`Self::sep`] is then the separator, which goes up to the parent. Each half's bytes are those
/// of an empty page ([`init`]) given its cells in order by [`reserve`]: the first cell at the top
/// of the cell area, each next one below it.
///
/// A delete's merge of two siblings reuses the buffers: [`Self::merge`] plans it from both pages,
/// [`Self::write_merged`] lays the cells out the same way over the survivor.
#[derive(Debug, Default)]
pub(crate) struct SplitBuf {
    /// The size of the page that splits.
    page_size: usize,
    /// A copy of `src` with the pending cell encoded after it; for a leaf's append split, which
    /// does not rewrite `src`, the pending cell alone.
    copy: Vec<u8>,
    /// Every cell of both halves in order, as `(offset, size)` in `copy`.
    cells: Vec<(usize, usize)>,
    /// The number of cells the left half keeps (`m`): `cells[m]` is the right half's first.
    left: usize,
    /// Whether `src` is rewritten (false for a leaf's append split, which keeps it as it is).
    rewrite: bool,
    /// The kind of page that splits.
    kind: Option<Kind>,
    /// The separator; before [`Self::split_branch`], the key of the cell it inserts.
    sep: Vec<u8>,
}

impl SplitBuf {
    /// The separator of the planned split: the first key of the right half; every key left in
    /// `src` is below it. Before a branch split, this is the key of the cell it inserts.
    pub(crate) fn sep(&self) -> &[u8] {
        &self.sep
    }

    /// Plan the split of the leaf `src` into `src` and a new right leaf, inserting `(key, value)`
    /// at sorted position `at` on whichever side it lands (see [`SplitBuf`] for the steps).
    ///
    /// The cut is by bytes, not entries: `src` keeps the entries before the first one at which
    /// its share (cells plus slots) reaches half of the total, so both halves end within `U` and
    /// neither is empty. With `append` and `at == nkeys` (the key goes past the end of the tree's
    /// rightmost leaf), `src` keeps every old entry and the right leaf gets only the new one, so
    /// ascending inserts fill leaves instead of leaving each half empty behind them; otherwise
    /// `append` is ignored.
    pub(crate) fn split_leaf(
        &mut self,
        src: &[u8],
        at: usize,
        key: &[u8],
        value: Val<'_>,
        append: bool,
    ) -> Result<()> {
        let size = cell_size_leaf(key.len(), value.cell_len());
        check_split_entry(src, size)?;
        let h = hdr_of(src, Kind::Leaf)?;
        check_split_at(src, &h, at)?;
        if append && at == h.nkeys {
            // A leaf's append split keeps every old cell (`m = n` below): `src` is left as it
            // is, byte for byte, and the right leaf takes the new cell, whose key is the
            // separator.
            self.copy.clear();
            self.copy.resize(size, 0);
            write_leaf_cell(&mut self.copy, key, value);
            self.cells.clear();
            self.cells.push((0, size));
            self.sep.clear();
            self.sep.extend_from_slice(key);
            (self.page_size, self.left, self.rewrite, self.kind) =
                (src.len(), 0, false, Some(Kind::Leaf));
            return Ok(());
        }
        self.copy_with_cell(src, size);
        write_leaf_cell(&mut self.copy[src.len()..], key, value);
        self.plan(&h, at, size, append)
    }

    /// Plan the split of the branch `src` into `src` and a new right branch, inserting the child
    /// `(self.sep(), child, child_txn)` as cell `at` (`at >= 1`): the separator of the split below
    /// climbs. The new separator moves up to the parent: `src` keeps the cells below it, and the
    /// right branch's cell 0 is the separator's own child with an empty key.
    ///
    /// The cut is by bytes as in [`Self::split_leaf`]; both halves end within `U` with at least
    /// one child each. With `append` and `at == nkeys` (the child goes last, on the tree's
    /// rightmost path), `src` keeps all but its last old child, which the right branch takes
    /// with the new one; otherwise `append` is ignored.
    pub(crate) fn split_branch(
        &mut self,
        src: &[u8],
        at: usize,
        child: u32,
        child_txn: u32,
        append: bool,
    ) -> Result<()> {
        let size = cell_size_branch(self.sep.len());
        check_split_entry(src, size)?;
        let h = hdr_of(src, Kind::Branch)?;
        check_split_at(src, &h, at)?;
        self.copy_with_cell(src, size);
        write_branch_cell(&mut self.copy[src.len()..], &self.sep, child, child_txn);
        self.plan(&h, at, size, append)
    }

    /// `copy` = `src` followed by `size` bytes for the pending cell.
    fn copy_with_cell(&mut self, src: &[u8], size: usize) {
        self.copy.clear();
        self.copy.extend_from_slice(src);
        self.copy.resize(src.len() + size, 0);
    }

    /// The split shared by both kinds, past the leaf's append split: splice the pending cell (the
    /// last `size` bytes of `copy`) into the copied cells at `at`, pick the cut, check that both
    /// halves fit and take the separator.
    fn plan(&mut self, h: &Hdr, at: usize, size: usize, append: bool) -> Result<()> {
        let len = self.copy.len() - size;
        let old = &self.copy[..len];
        let n = h.nkeys;
        self.cells.clear();
        for i in 0..n {
            if i == at {
                self.cells.push((len, size));
            }
            self.cells.push(cell_extent(old, h, i)?);
        }
        if at == n {
            self.cells.push((len, size));
        }
        let bytes = |cells: &[(usize, usize)]| cells.iter().map(|c| c.1 + SLOT).sum::<usize>();
        let total = bytes(&self.cells);

        // `m` = the number of cells `src` keeps (1..=n); cell `m` is the first of the right half.
        let m = if append && at == n {
            match h.kind {
                Kind::Leaf => n,
                Kind::Branch => n - 1,
            }
        } else {
            let mut cum = 0;
            self.cells
                .iter()
                .position(|c| {
                    let reached = 2 * cum >= total;
                    cum += c.1 + SLOT;
                    reached
                })
                .unwrap_or(n)
        }
        .clamp(1, n);

        let first = self.cells[m].0;
        let (key_off, klen) = match h.kind {
            Kind::Leaf => (LEAF_CELL_HEADER, read_u16(&self.copy, first + 4)),
            Kind::Branch => (BRANCH_CELL_HEADER, read_u16(&self.copy, first + 8)),
        };
        let left = bytes(&self.cells[..m]);
        // A branch's separator moves up: its cell stays in the right half without the key.
        let right = total - left - if h.kind == Kind::Branch { klen } else { 0 };
        let usable = len - HEADER - TRAILER;
        if left > usable || right > usable {
            return Err(corrupt(
                old,
                format_args!("split halves of {left} and {right} bytes exceed {usable}"),
            ));
        }
        self.sep.clear();
        self.sep.extend_from_slice(&self.copy[first + key_off..first + key_off + klen]);
        (self.page_size, self.left, self.rewrite, self.kind) = (len, m, true, Some(h.kind));
        Ok(())
    }

    /// Write the planned right half into `dst`, page `pgno` just allocated (whatever its bytes),
    /// as a page of transaction `txn`: the header and trailer ([`init_header`]) and every cell
    /// and slot ([`place_cells`]), so only the free gap keeps `dst`'s old bytes.
    pub(crate) fn write_right(&self, dst: &mut [u8], pgno: u32, txn: u64) -> Result<()> {
        let Some(kind) = self.kind.filter(|_| dst.len() == self.page_size) else {
            return Err(corrupt(dst, "split target is not a page of the planned source's size"));
        };
        init_header(dst, kind, pgno, txn);
        place_cells(dst, &self.copy, &self.cells[self.left..], kind == Kind::Branch);
        Ok(())
    }

    /// Rewrite the planned page `src` as the left half (a leaf's append split leaves it as it is).
    pub(crate) fn write_left(&self, src: &mut [u8]) -> Result<()> {
        if !self.rewrite {
            return Ok(());
        }
        let old = &self.copy[..self.page_size];
        let Some(kind) = self.kind.filter(|_| src.len() == old.len() && pgno(src) == pgno(old))
        else {
            return Err(corrupt(src, "split source is not the planned page"));
        };
        init(src, kind, pgno(old), txn(old));
        place_cells(src, &self.copy, &self.cells[..self.left], false);
        Ok(())
    }

    /// Plan the merge of two adjacent pages under one parent, `left` then `right`, into one:
    /// `left`'s cells followed by `right`'s. For branches `sep` is the parent's key for `right`,
    /// which comes down as the key of `right`'s cell 0 (empty until then), so the merged page's
    /// size is `used(left) + used(right) + sep.len()` ([`View::used`]); for leaves it is ignored.
    /// It changes no page: [`Self::write_merged`] writes the result over the survivor. Pages of
    /// different kinds or sizes, or a result over `U`, are [`Error::Corrupt`] (the caller sized the
    /// merge to fit, from the same bytes [`place_cells`] lays out). The caller also checked the key
    /// order across the seam (`left`'s keys below `sep`, `sep` below `right`'s key 1).
    pub(crate) fn merge(&mut self, left: &[u8], right: &[u8], sep: &[u8]) -> Result<()> {
        let (hl, hr) = (hdr(left)?, hdr(right)?);
        if hl.kind != hr.kind || left.len() != right.len() {
            return Err(corrupt(right, "merging pages of different kinds or sizes"));
        }
        let branch = hl.kind == Kind::Branch;
        self.copy.clear();
        self.copy.extend_from_slice(left);
        self.copy.extend_from_slice(right);
        self.cells.clear();
        for i in 0..hl.nkeys {
            self.cells.push(cell_extent(left, &hl, i)?);
        }
        let base = left.len();
        for i in 0..hr.nkeys {
            let (off, size) = cell_extent(right, &hr, i)?;
            if i == 0 && branch {
                // Right's cell 0 with the separator for its key.
                let at = self.copy.len();
                let size = cell_size_branch(sep.len());
                self.copy.resize(at + size, 0);
                let (child, child_txn) = (read_u32(right, off), read_u32(right, off + 4));
                write_branch_cell(&mut self.copy[at..], sep, child, child_txn);
                self.cells.push((at, size));
            } else {
                self.cells.push((base + off, size));
            }
        }
        let total = self.cells.iter().map(|c| c.1 + SLOT).sum::<usize>();
        let usable = left.len() - HEADER - TRAILER;
        if total > usable {
            return Err(corrupt(right, format_args!("a merge of {total} bytes exceeds {usable}")));
        }
        (self.page_size, self.left, self.rewrite, self.kind) =
            (left.len(), self.cells.len(), true, Some(hl.kind));
        Ok(())
    }

    /// Write the planned merge over `dst`, the survivor (one of the two pages merged, private to
    /// the writer), keeping its page number and txn. The other page is the caller's to free.
    pub(crate) fn write_merged(&self, dst: &mut [u8]) -> Result<()> {
        let Some(kind) = self.kind.filter(|_| dst.len() == self.page_size) else {
            return Err(corrupt(dst, "merge target is not a page of the planned size"));
        };
        let (p, t) = (pgno(dst), txn(dst));
        init(dst, kind, p, t);
        place_cells(dst, &self.copy, &self.cells[..self.left], false);
        Ok(())
    }
}

/// The pending entry of a split must respect the entry limit (the split guarantee rests on it).
fn check_split_entry(src: &[u8], cell_size: usize) -> Result<()> {
    let max_entry = src.len().saturating_sub(HEADER + TRAILER) / 4;
    if cell_size.saturating_add(SLOT) > max_entry {
        return Err(corrupt(src, format_args!("split entry of {cell_size} bytes over the limit")));
    }
    Ok(())
}

/// A split inserts cell `at` into a page with cells: `0..=n` for a leaf, `1..=n` for a branch.
fn check_split_at(src: &[u8], h: &Hdr, at: usize) -> Result<()> {
    let n = h.nkeys;
    let first = usize::from(h.kind == Kind::Branch);
    if n == 0 || at < first || at > n {
        return Err(corrupt(src, format_args!("split inserting cell {at} into {n}")));
    }
    Ok(())
}

/// Lay `cells` (extents in `from`) out in `page`, an empty page, as [`reserve`] would one after
/// another: each cell below the last, its slot next. Every byte of each cell and slot is
/// written, so only the free gap keeps the page's old bytes. With `drop_first_key`, the first is
/// a branch's new cell 0: its child, with an empty key. The split sized the cells to fit.
fn place_cells(page: &mut [u8], from: &[u8], cells: &[(usize, usize)], drop_first_key: bool) {
    let mut upper = page.len() - TRAILER;
    for (j, &(off, size)) in cells.iter().enumerate() {
        if j == 0 && drop_first_key {
            // The child's 8 bytes, then a zero key length.
            upper -= BRANCH_CELL_HEADER;
            page[upper..upper + 8].copy_from_slice(&from[off..off + 8]);
            put_u16(page, upper + 8, 0);
        } else {
            upper -= size;
            page[upper..upper + size].copy_from_slice(&from[off..off + size]);
        }
        put_u16(page, HEADER + SLOT * j, upper);
    }
    put_u16(page, NKEYS_OFF, cells.len());
    put_u16(page, UPPER_OFF, upper);
}

/// [`SplitBuf::split_leaf`] into `dst`, an empty leaf ([`init_leaf`]) of `src`'s size: the old
/// call shape, for tests. Returns the separator.
#[cfg(test)]
pub(crate) fn split_leaf(
    src: &mut [u8],
    dst: &mut [u8],
    at: usize,
    key: &[u8],
    value: &[u8],
    append: bool,
) -> Result<Vec<u8>> {
    let mut buf = SplitBuf::default();
    buf.split_leaf(src, at, key, Val::Inline(value), append)?;
    split_into(&buf, src, dst, Kind::Leaf)
}

/// [`SplitBuf::split_branch`] of the child `(key, child, child_txn)` into `dst`, an empty branch
/// ([`init_branch`]) of `src`'s size: the old call shape, for tests. Returns the separator.
#[cfg(test)]
pub(crate) fn split_branch(
    src: &mut [u8],
    dst: &mut [u8],
    at: usize,
    key: &[u8],
    child: u32,
    child_txn: u32,
    append: bool,
) -> Result<Vec<u8>> {
    let mut buf = SplitBuf { sep: key.to_vec(), ..SplitBuf::default() };
    buf.split_branch(src, at, child, child_txn, append)?;
    split_into(&buf, src, dst, Kind::Branch)
}

/// Write a planned split's halves into `src` and `dst` (an empty `kind` page, filled with
/// [`crate::tree::POISON`], as a page just allocated in a debug build, and re-initialised with
/// its page number and txn).
#[cfg(test)]
fn split_into(buf: &SplitBuf, src: &mut [u8], dst: &mut [u8], kind: Kind) -> Result<Vec<u8>> {
    if dst.len() != src.len() || hdr_of(dst, kind)?.nkeys != 0 {
        return Err(corrupt(dst, "split target is not an empty page of the source's size"));
    }
    let (p, t) = (pgno(dst), txn(dst));
    dst.fill(crate::tree::POISON);
    buf.write_right(dst, p, t)?;
    buf.write_left(src)?;
    Ok(buf.sep.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{collection::vec, prelude::*, sample::select};

    /// The split as it was built before [`SplitBuf`]: the right half in a separate empty page, the
    /// left half rebuilt from a copy of the page by appending its cells one by one. The reference
    /// for `splits_match_the_reference_split`.
    mod reference {
        use super::super::*;

        pub(super) fn split_leaf(
            src: &mut [u8],
            dst: &mut [u8],
            at: usize,
            key: &[u8],
            value: &[u8],
            append: bool,
        ) -> Result<Vec<u8>> {
            let size = cell_size_leaf(key.len(), value.len());
            check_split_entry(src, size)?;
            let mut cell = vec![0; size];
            write_leaf_cell(&mut cell, key, Val::Inline(value));
            split(src, dst, Kind::Leaf, at, &cell, append)
        }

        pub(super) fn split_branch(
            src: &mut [u8],
            dst: &mut [u8],
            at: usize,
            key: &[u8],
            child: u32,
            child_txn: u32,
            append: bool,
        ) -> Result<Vec<u8>> {
            let size = cell_size_branch(key.len());
            check_split_entry(src, size)?;
            let mut cell = vec![0; size];
            write_branch_cell(&mut cell, key, child, child_txn);
            split(src, dst, Kind::Branch, at, &cell, append)
        }

        fn check_split_entry(src: &[u8], cell_size: usize) -> Result<()> {
            let max_entry = src.len().saturating_sub(HEADER + TRAILER) / 4;
            if cell_size.saturating_add(SLOT) > max_entry {
                return Err(corrupt(
                    src,
                    format_args!("split entry of {cell_size} bytes over the limit"),
                ));
            }
            Ok(())
        }

        fn split(
            src: &mut [u8],
            dst: &mut [u8],
            kind: Kind,
            at: usize,
            new_cell: &[u8],
            append: bool,
        ) -> Result<Vec<u8>> {
            let h = hdr_of(src, kind)?;
            if dst.len() != src.len() || hdr_of(dst, kind)?.nkeys != 0 {
                return Err(corrupt(dst, "split target is not an empty page of the source's size"));
            }
            let n = h.nkeys;
            let first = usize::from(kind == Kind::Branch);
            if n == 0 || at < first || at > n {
                return Err(corrupt(src, format_args!("split inserting cell {at} into {n}")));
            }
            // A leaf's append split keeps every old cell (`m = n` below): `src` is left as it is,
            // byte for byte, and `dst` takes the new cell, whose key is the separator.
            if append && at == n && kind == Kind::Leaf {
                let klen = read_u16(new_cell, 4);
                let sep = new_cell[LEAF_CELL_HEADER..LEAF_CELL_HEADER + klen].to_vec();
                append_cell(dst, new_cell)?;
                return Ok(sep);
            }

            // The old cells with the new one spliced in at `at`, borrowed from a copy of `src`.
            let old = src.to_vec();
            let mut cells: Vec<&[u8]> = Vec::with_capacity(n + 1);
            for i in 0..n {
                if i == at {
                    cells.push(new_cell);
                }
                let (off, size) = cell_extent(&old, &h, i)?;
                cells.push(&old[off..off + size]);
            }
            if at == n {
                cells.push(new_cell);
            }
            let bytes = |cells: &[&[u8]]| cells.iter().map(|c| c.len() + SLOT).sum::<usize>();
            let total = bytes(&cells);

            // `m` = the number of cells `src` keeps (1..=n); cell `m` is the first of `dst`.
            let m = if append && at == n {
                match kind {
                    Kind::Leaf => n,
                    Kind::Branch => n - 1,
                }
            } else {
                let mut cum = 0;
                cells
                    .iter()
                    .position(|c| {
                        let reached = 2 * cum >= total;
                        cum += c.len() + SLOT;
                        reached
                    })
                    .unwrap_or(n)
            }
            .clamp(1, n);

            let (key_off, klen) = match kind {
                Kind::Leaf => (LEAF_CELL_HEADER, read_u16(cells[m], 4)),
                Kind::Branch => (BRANCH_CELL_HEADER, read_u16(cells[m], 8)),
            };
            let left = bytes(&cells[..m]);
            // A branch's separator moves up: its cell stays in `dst` without the key.
            let right = total - left - if kind == Kind::Branch { klen } else { 0 };
            let usable = src.len() - HEADER - TRAILER;
            if left > usable || right > usable {
                return Err(corrupt(
                    src,
                    format_args!("split halves of {left} and {right} bytes exceed {usable}"),
                ));
            }
            let sep = cells[m][key_off..key_off + klen].to_vec();

            init(src, kind, pgno(&old), txn(&old));
            for (j, cell) in cells.iter().enumerate() {
                if j < m {
                    append_cell(src, cell)?;
                } else if j == m && kind == Kind::Branch {
                    let mut first = [0; BRANCH_CELL_HEADER];
                    first[..8].copy_from_slice(&cell[..8]);
                    append_cell(dst, &first)?;
                } else {
                    append_cell(dst, cell)?;
                }
            }
            Ok(sep)
        }

        fn append_cell(page: &mut [u8], cell: &[u8]) -> Result<()> {
            let h = hdr(page)?;
            let Some(off) = reserve(page, &h, h.nkeys, cell.len())? else {
                return Err(corrupt(page, "split half overflowed"));
            };
            page[off..off + cell.len()].copy_from_slice(cell);
            Ok(())
        }
    }

    const SIZES: [u32; 5] = [4096, 8192, 16384, 32768, 65536];

    type Entry = (Vec<u8>, Vec<u8>);
    type Cell = (Vec<u8>, u32, u32);

    fn geometry(size: u32) -> Geometry {
        Geometry::new(size).unwrap()
    }

    /// A page's bytes without its free gap (from the end of the slot array to `upper`, by its
    /// header): every byte a layout writes, for comparing pages whose gaps hold different bytes.
    fn outside_gap(page: &[u8]) -> Vec<u8> {
        let slots_end = HEADER + SLOT * nkeys(page);
        [&page[..slots_end], &page[upper(page)..]].concat()
    }

    fn fresh(g: &Geometry, kind: Kind) -> Vec<u8> {
        let mut page = vec![0; g.page_size()];
        init(&mut page, kind, 7, 3);
        page
    }

    fn entries(page: &[u8]) -> Vec<Entry> {
        (0..nkeys(page))
            .map(|i| {
                let (k, v) = leaf_entry(page, i).unwrap();
                (k.to_vec(), v.inline().unwrap().to_vec())
            })
            .collect()
    }

    fn cells(page: &[u8]) -> Vec<Cell> {
        (0..nkeys(page))
            .map(|i| {
                let (child, txn) = branch_child(page, i).unwrap();
                (branch_key(page, i).unwrap().to_vec(), child, txn)
            })
            .collect()
    }

    fn leaf_used(model: &[Entry]) -> usize {
        model.iter().map(|(k, v)| cell_size_leaf(k.len(), v.len()) + SLOT).sum()
    }

    fn branch_used(model: &[Cell]) -> usize {
        model.iter().map(|(k, ..)| cell_size_branch(k.len()) + SLOT).sum()
    }

    /// A key that sorts by `rank` (a big-endian prefix), padded to `klen` (at least 4) bytes.
    fn ranked_key(rank: u32, klen: usize) -> Vec<u8> {
        let mut key = rank.to_be_bytes().to_vec();
        key.resize(klen.max(4), 0xab);
        key
    }

    /// Random entry sizes scaled to the page: a quarter at the limit, a quarter anywhere up to
    /// it, half small.
    #[derive(Clone, Copy, Debug)]
    struct Spec {
        ksel: u8,
        klen: u16,
        kseed: u8,
        vsel: u8,
        vlen: u16,
        vfill: u8,
    }

    fn spec() -> impl Strategy<Value = Spec> {
        any::<(u8, u16, u8, u8, u16, u8)>().prop_map(|(ksel, klen, kseed, vsel, vlen, vfill)| {
            Spec { ksel, klen, kseed, vsel, vlen, vfill }
        })
    }

    impl Spec {
        fn klen(&self, g: &Geometry) -> usize {
            match self.ksel % 4 {
                0 => g.max_key(),
                1 => usize::from(self.klen) % (g.max_key() + 1),
                _ => usize::from(self.klen) % 3,
            }
        }

        /// Short keys come from a tiny alphabet so they repeat; long ones are spread out.
        fn key(&self, g: &Geometry) -> Vec<u8> {
            let klen = self.klen(g);
            let base = if klen < 3 { self.kseed % 3 } else { self.kseed };
            (0..klen).map(|k| base.wrapping_add((k as u8).wrapping_mul(7))).collect()
        }

        fn value(&self, g: &Geometry, klen: usize) -> Vec<u8> {
            let max = g.max_value(klen);
            let vlen = match self.vsel % 4 {
                0 => max,
                1 => usize::from(self.vlen) % (max + 1),
                _ => usize::from(self.vlen) % 17.min(max + 1),
            };
            vec![self.vfill; vlen]
        }
    }

    #[derive(Clone, Debug)]
    enum LeafOp {
        Put(Spec),
        Delete(usize),
    }

    fn leaf_op() -> impl Strategy<Value = LeafOp> {
        prop_oneof![
            3 => spec().prop_map(LeafOp::Put),
            1 => any::<usize>().prop_map(LeafOp::Delete),
        ]
    }

    #[derive(Clone, Debug)]
    enum BranchOp {
        Insert(Spec, u32, u32),
        Delete(usize),
        SetChild(usize, u32, u32),
        Search(Spec),
        SearchExisting(usize),
    }

    fn branch_op() -> impl Strategy<Value = BranchOp> {
        prop_oneof![
            4 => (spec(), any::<u32>(), any::<u32>()).prop_map(|(s, c, t)| BranchOp::Insert(s, c, t)),
            2 => any::<usize>().prop_map(BranchOp::Delete),
            1 => (any::<usize>(), any::<u32>(), any::<u32>())
                .prop_map(|(i, c, t)| BranchOp::SetChild(i, c, t)),
            1 => spec().prop_map(BranchOp::Search),
            1 => any::<usize>().prop_map(BranchOp::SearchExisting),
        ]
    }

    /// Fill a leaf in ascending order (ranks 1, 3, 5, ...) from `specs`, cycling, until an entry
    /// does not fit. Returns the entries and the size of the one that did not fit.
    fn fill_leaf(g: &Geometry, page: &mut [u8], specs: &[Spec]) -> (Vec<Entry>, usize, Vec<u8>) {
        let mut model = Vec::new();
        for s in specs.iter().cycle() {
            let klen = s.klen(g).max(4);
            let value = s.value(g, klen);
            let key = ranked_key(2 * model.len() as u32 + 1, klen);
            match leaf_insert(page, model.len(), &key, &value).unwrap() {
                Fit::Done => model.push((key, value)),
                Fit::Full => return (model, klen, value),
            }
        }
        unreachable!("specs is never empty")
    }

    /// Fill a branch (cell 0, then ranks 3, 5, ...) from `specs`, cycling, until a cell does not
    /// fit. Returns the cells and the key length of the one that did not fit.
    fn fill_branch(g: &Geometry, page: &mut [u8], specs: &[Spec]) -> (Vec<Cell>, usize) {
        assert_eq!(branch_insert(page, 0, &[], 0, 0).unwrap(), Fit::Done);
        let mut model = vec![(Vec::new(), 0, 0)];
        for s in specs.iter().cycle() {
            let n = model.len();
            let key = ranked_key(2 * n as u32 + 1, s.klen(g));
            match branch_insert(page, n, &key, n as u32, 3 * n as u32).unwrap() {
                Fit::Done => model.push((key, n as u32, 3 * n as u32)),
                Fit::Full => return (model, key.len()),
            }
        }
        unreachable!("specs is never empty")
    }

    #[test]
    fn entry_limits_are_exact() {
        for size in SIZES {
            let g = geometry(size);
            let u = size as usize - HEADER - TRAILER;
            assert_eq!((g.page_size(), g.usable(), g.max_entry()), (size as usize, u, u / 4));
            let mk = g.max_key();
            assert!(g.check_entry(mk, 0).is_ok());
            assert!(matches!(
                g.check_entry(mk + 1, 0),
                Err(Error::KeyTooLarge { len, max }) if len == mk + 1 && max == mk
            ));
            // A maximal key is a maximal separator entry.
            assert_eq!(cell_size_branch(mk) + SLOT, g.max_entry());
            for klen in [0, 1, 32, mk] {
                let mv = g.max_value(klen);
                assert!(g.check_entry(klen, mv).is_ok());
                assert!(matches!(
                    g.check_entry(klen, mv + 1),
                    Err(Error::ValueTooLarge { len, max }) if len == mv + 1 && max == mv
                ));
                assert_eq!(cell_size_leaf(klen, mv) + SLOT, g.max_entry());
            }
            // Four maximal entries fill a leaf without overflowing it.
            let mut page = fresh(&g, Kind::Leaf);
            for i in 0..4_u8 {
                let value = vec![i; g.max_value(32)];
                let fit = leaf_insert(&mut page, i.into(), &[i; 32], &value).unwrap();
                assert_eq!(fit, Fit::Done);
            }
            assert!(free_space(&page).unwrap() < g.max_entry());
            validate(&page, &g).unwrap();
        }
        for size in SIZES {
            // The widest dup outer key: its cell inline, a 1-byte subkey within the key limit.
            let g = geometry(size);
            let k = g.max_dup_key_len();
            assert!(k < g.max_key() && g.max_value(k) >= NESTED_CELL, "{size}");
            assert!(g.max_value(k + 1) < NESTED_CELL, "{size}: the largest");
        }
        let g = geometry(4096);
        assert_eq!((g.max_key(), g.max_value(32)), (1005, 975));
        assert_eq!(g.max_dup_key_len(), 975);
        // A refused key has no value limit, not the 1 or 0 bytes its entry would leave.
        assert_eq!((g.max_value(1005), g.max_value(1006), g.max_value(1007)), (2, 0, 0));
        assert_eq!(g.max_value(usize::MAX), 0);
        for bad in [0, 2048, 4095, 4097, 6144, 131_072, u32::MAX] {
            assert!(matches!(Geometry::new(bad), Err(Error::InvalidPageSize(b)) if b == bad));
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn leaf_ops_match_a_sorted_vec_model(
            size in select(&SIZES[..]),
            ops in vec(leaf_op(), 1..120),
        ) {
            let g = geometry(size);
            let mut page = fresh(&g, Kind::Leaf);
            let mut model: Vec<Entry> = Vec::new();
            for op in ops {
                let before = page.clone();
                match op {
                    LeafOp::Put(s) => {
                        let key = s.key(&g);
                        let value = s.value(&g, key.len());
                        prop_assert!(g.check_entry(key.len(), value.len()).is_ok());
                        let (fit, need) = match leaf_search(&page, &key).unwrap() {
                            Search::Found(i) => {
                                prop_assert_eq!(&model[i].0, &key);
                                let fit = leaf_replace_value(&mut page, i, &value).unwrap();
                                let need = value.len().saturating_sub(model[i].1.len());
                                if fit == Fit::Done {
                                    model[i].1 = value;
                                }
                                (fit, need)
                            }
                            Search::NotFound(i) => {
                                prop_assert_eq!(i, model.partition_point(|(k, _)| *k < key));
                                let need = cell_size_leaf(key.len(), value.len()) + SLOT;
                                let fit = leaf_insert(&mut page, i, &key, &value).unwrap();
                                if fit == Fit::Done {
                                    model.insert(i, (key, value));
                                }
                                (fit, need)
                            }
                        };
                        if fit == Fit::Full {
                            prop_assert!(free_space(&before).unwrap() < need);
                            prop_assert_eq!(&page, &before);
                        }
                    }
                    LeafOp::Delete(sel) => {
                        if !model.is_empty() {
                            let i = sel % model.len();
                            leaf_delete(&mut page, i).unwrap();
                            model.remove(i);
                        }
                    }
                }
                prop_assert!(validate(&page, &g).is_ok(), "{:?}", validate(&page, &g));
                prop_assert_eq!(entries(&page), model.clone());
                prop_assert_eq!(free_space(&page).unwrap() + leaf_used(&model), g.usable());
            }
        }

        #[test]
        fn branch_ops_match_a_sorted_vec_model(
            size in select(&SIZES[..]),
            ops in vec(branch_op(), 1..120),
        ) {
            let g = geometry(size);
            let mut page = fresh(&g, Kind::Branch);
            prop_assert_eq!(branch_insert(&mut page, 0, &[], 100, 200).unwrap(), Fit::Done);
            let mut model: Vec<Cell> = vec![(Vec::new(), 100, 200)];
            for op in ops {
                let before = page.clone();
                match op {
                    BranchOp::Insert(s, child, txn) => {
                        let key = s.key(&g);
                        let i = model.partition_point(|(k, ..)| *k < key);
                        if !key.is_empty() && model.get(i).is_none_or(|(k, ..)| *k != key) {
                            match branch_insert(&mut page, i, &key, child, txn).unwrap() {
                                Fit::Done => model.insert(i, (key, child, txn)),
                                Fit::Full => {
                                    let need = cell_size_branch(key.len()) + SLOT;
                                    prop_assert!(free_space(&before).unwrap() < need);
                                    prop_assert_eq!(&page, &before);
                                }
                            }
                        }
                    }
                    BranchOp::Delete(sel) => {
                        if model.len() > 1 {
                            let i = sel % model.len();
                            branch_delete(&mut page, i).unwrap();
                            model.remove(i);
                            model[0].0.clear();
                        }
                    }
                    BranchOp::SetChild(sel, child, txn) => {
                        let i = sel % model.len();
                        branch_set_child(&mut page, i, child, txn).unwrap();
                        (model[i].1, model[i].2) = (child, txn);
                    }
                    BranchOp::Search(s) => {
                        let key = s.key(&g);
                        let want = model.iter().rposition(|(k, ..)| *k <= key).unwrap();
                        prop_assert_eq!(branch_search(&page, &key).unwrap(), want);
                    }
                    BranchOp::SearchExisting(sel) => {
                        let i = sel % model.len();
                        prop_assert_eq!(branch_search(&page, &model[i].0.clone()).unwrap(), i);
                    }
                }
                prop_assert!(validate(&page, &g).is_ok(), "{:?}", validate(&page, &g));
                prop_assert_eq!(cells(&page), model.clone());
                prop_assert_eq!(free_space(&page).unwrap() + branch_used(&model), g.usable());
            }
        }

        #[test]
        fn leaf_splits_fit_and_keep_every_entry(
            specs in vec(spec(), 1..40),
            at_sel in any::<usize>(),
            append in any::<bool>(),
        ) {
            for size in SIZES {
                let g = geometry(size);
                let mut src = fresh(&g, Kind::Leaf);
                let (mut model, klen, value) = fill_leaf(&g, &mut src, &specs);
                let n = model.len();
                let at = at_sel % (n + 1);
                let key = ranked_key(2 * at as u32, klen);
                model.insert(at, (key.clone(), value.clone()));
                let mut dst = fresh(&g, Kind::Leaf);
                let sep = split_leaf(&mut src, &mut dst, at, &key, &value, append);
                prop_assert!(sep.is_ok(), "{size}: {sep:?}");
                prop_assert!(validate(&src, &g).is_ok(), "{:?}", validate(&src, &g));
                prop_assert!(validate(&dst, &g).is_ok(), "{:?}", validate(&dst, &g));
                let (left, right) = (entries(&src), entries(&dst));
                prop_assert!(!left.is_empty() && !right.is_empty());
                prop_assert_eq!(sep.unwrap(), right[0].0.clone());
                prop_assert_eq!(&[left.clone(), right].concat(), &model);
                prop_assert_eq!((pgno(&src), txn(&src)), (7, 3));
                let left_used = g.usable() - free_space(&src).unwrap();
                let right_used = g.usable() - free_space(&dst).unwrap();
                let total = leaf_used(&model);
                prop_assert_eq!(left_used + right_used, total);
                if append && at == n {
                    prop_assert_eq!(left.len(), n);
                } else {
                    // Cut by bytes: the left reaches half, overshooting by less than one entry.
                    prop_assert!(2 * right_used <= total, "{left_used} | {right_used}");
                    prop_assert!(2 * left_used < total + 2 * g.max_entry());
                }
            }
        }

        #[test]
        fn branch_splits_fit_and_keep_every_child(
            specs in vec(spec(), 1..40),
            at_sel in any::<usize>(),
            append in any::<bool>(),
        ) {
            for size in SIZES {
                let g = geometry(size);
                let mut src = fresh(&g, Kind::Branch);
                let (mut model, klen) = fill_branch(&g, &mut src, &specs);
                let n = model.len();
                let at = 1 + at_sel % n;
                let key = ranked_key(2 * at as u32, klen);
                model.insert(at, (key.clone(), 1000, 77));
                let mut dst = fresh(&g, Kind::Branch);
                let sep = split_branch(&mut src, &mut dst, at, &key, 1000, 77, append);
                prop_assert!(sep.is_ok(), "{size}: {sep:?}");
                prop_assert!(validate(&src, &g).is_ok(), "{:?}", validate(&src, &g));
                prop_assert!(validate(&dst, &g).is_ok(), "{:?}", validate(&dst, &g));
                let (left, mut right) = (cells(&src), cells(&dst));
                // The separator moved up: it is the lower bound of dst's cell 0.
                right[0].0 = sep.unwrap();
                prop_assert_eq!([left.clone(), right].concat(), model);
                if append && at == n && n >= 2 {
                    prop_assert_eq!(left.len(), n - 1);
                }
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn cmp_keys_is_the_slice_order(
            prefix in vec(any::<u8>(), 0..40),
            a in vec(select(&[0_u8, 1, 0x7f, 0x80, 0xfe, 0xff][..]), 0..20),
            b in vec(select(&[0_u8, 1, 0x7f, 0x80, 0xfe, 0xff][..]), 0..20),
            cut in any::<usize>(),
        ) {
            let a = [&prefix[..], &a].concat();
            let b = [&prefix[..], &b].concat();
            let cut = &a[..cut % (a.len() + 1)];
            for (x, y) in [(&a[..], &b[..]), (&a, &a), (cut, &a), (&[][..], &b), (&[][..], &[][..])] {
                prop_assert_eq!(cmp_keys(x, y), x.cmp(y), "{:?} vs {:?}", x, y);
                prop_assert_eq!(cmp_keys(y, x), y.cmp(x), "{:?} vs {:?}", y, x);
            }
        }

        #[test]
        fn one_pass_searches_match_the_slow_accessors(
            size in select(&SIZES[..]),
            specs in vec(spec(), 1..8),
            leaf in any::<bool>(),
        ) {
            let g = geometry(size);
            let mut page = fresh(&g, if leaf { Kind::Leaf } else { Kind::Branch });
            let keys: Vec<Vec<u8>> = if leaf {
                fill_leaf(&g, &mut page, &specs).0.into_iter().map(|(k, _)| k).collect()
            } else {
                fill_branch(&g, &mut page, &specs).0.into_iter().map(|(k, _, _)| k).collect()
            };
            // Each key, a prefix and an extension of it, and a rank between it and the one below.
            let mut probes = vec![Vec::new(), vec![0xff; 6]];
            for (r, k) in keys.iter().enumerate().step_by(keys.len().div_ceil(48)) {
                probes.push(k.clone());
                probes.push(k[..k.len().saturating_sub(1)].to_vec());
                probes.push([&k[..], &[0]].concat());
                probes.push(ranked_key(2 * r as u32, 4));
            }
            let (mut found, mut missed) = (0, 0);
            for (i, key) in probes.iter().enumerate() {
                assert_one_pass_matches(&page, key, i % (keys.len() + 1));
                if leaf {
                    match leaf_find(&page, key).unwrap() {
                        Find::Found { .. } => found += 1,
                        Find::NotFound { .. } => missed += 1,
                    }
                } else {
                    prop_assert!(branch_route(&page, key).is_ok());
                }
            }
            prop_assert!(!leaf || (found > 0 && missed > 0), "{found} hits, {missed} misses");
        }
    }

    /// [`copy_short`] is `copy_from_slice` for every length through its fallback past 64 bytes:
    /// random bytes into a slice of a larger buffer, whose bytes either side stay as they were.
    #[test]
    fn copy_short_is_copy_from_slice() {
        let mut s = 0x5eed_u64;
        for n in 0..=80 {
            for at in 0..4 {
                for _ in 0..8 {
                    let src: Vec<u8> = (0..n).map(|_| splitmix(&mut s) as u8).collect();
                    let mut buf: Vec<u8> = (0..n + 8).map(|_| splitmix(&mut s) as u8).collect();
                    let mut want = buf.clone();
                    want[at..at + n].copy_from_slice(&src);
                    copy_short(&mut buf[at..at + n], &src);
                    assert_eq!(buf, want, "{n} bytes at {at}");
                }
            }
        }
    }

    /// [`copy_short`] panics where `copy_from_slice` does, with its message.
    #[test]
    fn copy_short_panics_as_copy_from_slice_on_a_length_mismatch() {
        fn panic_message(copy: fn(&mut [u8], &[u8]), dst: usize, src: usize) -> String {
            let (mut d, s) = (vec![0_u8; dst], vec![1_u8; src]);
            let caught =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| copy(&mut d, &s)));
            let payload = caught.expect_err("a length mismatch must panic");
            match payload.downcast::<String>() {
                Ok(message) => *message,
                Err(payload) => payload.downcast::<&str>().map(|m| m.to_string()).unwrap(),
            }
        }
        for (dst, src) in [(0, 1), (1, 0), (2, 3), (4, 3), (8, 7), (7, 8), (17, 16), (64, 65)] {
            assert_eq!(
                panic_message(copy_short, dst, src),
                panic_message(|d, s| d.copy_from_slice(s), dst, src),
                "dst {dst}, src {src}"
            );
        }
    }

    /// An insert at every index of leaves of 0..=10 entries, the end (`i == nkeys`, which moves
    /// no slot in [`reserve`]) included, against a sorted model. Key lengths vary so the key
    /// copies take every [`copy_short`] size class. The entry inserted at an odd index is a
    /// nested cell, read back as [`Val::Nested`].
    #[test]
    fn inserts_at_every_index_match_a_model() {
        let g = geometry(4096);
        let klen = |rank: u32| 4 + (rank as usize * 7) % 67;
        for n in 0..=10_u32 {
            for i in 0..=n {
                let mut page = fresh(&g, Kind::Leaf);
                let mut model: Vec<Entry> = Vec::new();
                for j in 0..n {
                    let rank = 2 * (j + 1);
                    let (key, value) = (ranked_key(rank, klen(rank)), vec![j as u8; j as usize]);
                    assert_eq!(
                        leaf_insert(&mut page, j as usize, &key, &value).unwrap(),
                        Fit::Done
                    );
                    model.push((key, value));
                }
                let rank = 2 * i + 1;
                let key = ranked_key(rank, klen(rank));
                let nested = i % 2 == 1;
                let root = TreeRoot {
                    root: 9 + i,
                    root_txn: 7,
                    height: 1,
                    entries: 3,
                    leaf_pages: 1,
                    ..TreeRoot::EMPTY
                }
                .encode();
                let fit = if nested {
                    LeafMut::new(&mut page).unwrap().insert(i as usize, &key, Val::Nested(&root))
                } else {
                    leaf_insert(&mut page, i as usize, &key, &[0xee; 8])
                };
                assert_eq!(fit.unwrap(), Fit::Done);
                let value = if nested { root.to_vec() } else { vec![0xee; 8] };
                model.insert(i as usize, (key, value));
                validate(&page, &g).unwrap();
                let got: Vec<_> =
                    (0..nkeys(&page)).map(|j| leaf_entry(&page, j).unwrap()).collect();
                let want: Vec<_> = model
                    .iter()
                    .enumerate()
                    .map(|(j, (k, v))| match v.as_slice().try_into() {
                        Ok(root) if nested && j == i as usize => (k.as_slice(), Val::Nested(root)),
                        _ => (k.as_slice(), Val::Inline(v)),
                    })
                    .collect();
                assert_eq!(got, want, "{n} entries, insert at {i}");
            }
        }
    }

    #[test]
    fn append_splits_fill_the_left_leaf() {
        for size in SIZES {
            let g = geometry(size);
            let mut page = fresh(&g, Kind::Leaf);
            let (mut rank, mut splits) = (0_u32, 0);
            while splits < 3 {
                let (key, value) = (ranked_key(rank, 16), [rank as u8; 40]);
                rank += 1;
                let n = nkeys(&page);
                if leaf_insert(&mut page, n, &key, &value).unwrap() == Fit::Full {
                    let mut dst = fresh(&g, Kind::Leaf);
                    assert_eq!(
                        split_leaf(&mut page, &mut dst, n, &key, &value, true).unwrap(),
                        key
                    );
                    let used = g.usable() - free_space(&page).unwrap();
                    assert!(
                        used * 100 > g.usable() * 95,
                        "{size}: left leaf {used}/{}",
                        g.usable()
                    );
                    assert_eq!(nkeys(&dst), 1);
                    page = dst;
                    splits += 1;
                }
            }
        }
    }

    /// A leaf's append split leaves the left leaf byte for byte as it was, even one built in
    /// random insertion order, whose cells a rebuild would rewrite in key order: only the new
    /// cell moves, to the right leaf, and its key is the separator.
    #[test]
    fn an_append_split_leaves_a_random_leaf_byte_identical() {
        for size in SIZES {
            let g = geometry(size);
            let mut state = u64::from(size);
            let mut leaf = fresh(&g, Kind::Leaf);
            let mut keys: Vec<Vec<u8>> = Vec::new();
            loop {
                let key = ranked_key((splitmix(&mut state) >> 33) as u32, 16);
                let Err(at) = keys.binary_search(&key) else { continue };
                if leaf_insert(&mut leaf, at, &key, &[7; 40]).unwrap() == Fit::Full {
                    break;
                }
                keys.insert(at, key);
            }
            let before = leaf.clone();
            let (n, key) = (nkeys(&leaf), ranked_key(u32::MAX, 16));
            let mut dst = fresh(&g, Kind::Leaf);
            assert_eq!(split_leaf(&mut leaf, &mut dst, n, &key, &[8; 40], true).unwrap(), key);
            assert_eq!(leaf, before, "{size}: the left leaf is unchanged");
            assert_eq!(nkeys(&dst), 1, "{size}");
            assert_eq!(leaf_key(&dst, 0).unwrap(), &key[..], "{size}");
            validate(&dst, &g).unwrap();
        }
    }

    #[test]
    fn a_full_page_is_left_byte_identical() {
        let g = geometry(4096);
        let mut leaf = fresh(&g, Kind::Leaf);
        let mut n = 0;
        while leaf_insert(&mut leaf, n, &ranked_key(n as u32, 8), &[1; 100]).unwrap() == Fit::Done {
            n += 1;
        }
        let free = free_space(&leaf).unwrap();
        let before = leaf.clone();
        for at in [0, n / 2, n] {
            let fit = leaf_insert(&mut leaf, at, &ranked_key(u32::MAX, 8), &[2; 100]).unwrap();
            assert_eq!(fit, Fit::Full);
            assert_eq!(leaf, before);
        }
        let fit = leaf_replace_value(&mut leaf, n / 2, &vec![3; 100 + free + 1]).unwrap();
        assert_eq!(fit, Fit::Full);
        assert_eq!(leaf, before);
        // Growing by exactly the free space fits and uses the page to the last byte.
        let fit = leaf_replace_value(&mut leaf, n / 2, &vec![3; 100 + free]).unwrap();
        assert_eq!(fit, Fit::Done);
        assert_eq!(free_space(&leaf).unwrap(), 0);
        validate(&leaf, &g).unwrap();
        // So does an insert of exactly the free space.
        let mut leaf = before;
        let value = vec![4; free - SLOT - cell_size_leaf(8, 0)];
        assert_eq!(leaf_insert(&mut leaf, n, &ranked_key(u32::MAX, 8), &value).unwrap(), Fit::Done);
        assert_eq!(free_space(&leaf).unwrap(), 0);
        validate(&leaf, &g).unwrap();

        let mut branch = fresh(&g, Kind::Branch);
        assert_eq!(branch_insert(&mut branch, 0, &[], 1, 1).unwrap(), Fit::Done);
        let mut n = 1;
        while branch_insert(&mut branch, n, &ranked_key(n as u32, 40), 1, 1).unwrap() == Fit::Done {
            n += 1;
        }
        let before = branch.clone();
        for (at, rank) in [(1, 0), (n, u32::MAX)] {
            let fit = branch_insert(&mut branch, at, &ranked_key(rank, 40), 2, 2).unwrap();
            assert_eq!(fit, Fit::Full);
            assert_eq!(branch, before);
        }
    }

    /// A small valid leaf for the damage cases: five 8-byte keys with 20-byte values.
    fn damage_base(g: &Geometry) -> Vec<u8> {
        let mut page = fresh(g, Kind::Leaf);
        for i in 0..5 {
            assert_eq!(
                leaf_insert(&mut page, i, &ranked_key(i as u32, 8), &[9; 20]).unwrap(),
                Fit::Done
            );
        }
        validate(&page, g).unwrap();
        page
    }

    #[test]
    fn validate_rejects_a_leaf_key_over_max_key() {
        for size in SIZES {
            let g = geometry(size);
            let mk = g.max_key();
            // `leaf_insert` does not enforce the limits. Both entries are exactly `max_entry`, so
            // only the key limit refuses them: a branch separator must hold any leaf key.
            for (klen, value) in [(mk + 1, &b"v"[..]), (mk + 2, &b""[..])] {
                assert_eq!(cell_size_leaf(klen, value.len()) + SLOT, g.max_entry());
                let mut page = fresh(&g, Kind::Leaf);
                assert_eq!(leaf_insert(&mut page, 0, &vec![7; klen], value).unwrap(), Fit::Done);
                let err = validate(&page, &g).unwrap_err();
                assert!(err.to_string().contains("over the limit"), "{size}/{klen}: {err}");
            }
            // The longest accepted key passes.
            let mut page = fresh(&g, Kind::Leaf);
            let value = vec![1; g.max_value(mk)];
            assert_eq!(leaf_insert(&mut page, 0, &vec![7; mk], &value).unwrap(), Fit::Done);
            validate(&page, &g).unwrap();
        }
    }

    /// A named way to damage a valid page.
    type Damage = (&'static str, fn(&mut Vec<u8>));

    #[test]
    fn validate_rejects_each_kind_of_damage() {
        let g = geometry(4096);
        let end = g.page_size() - TRAILER;
        let damages: [Damage; 13] = [
            ("unknown kind", |p| p[KIND_OFF] = 3),
            ("page flags", |p| p[FLAGS_OFF] = 1),
            ("reserved header byte", |p| p[RESERVED_OFF + 5] = 1),
            ("one slot too many", |p| put_u16(p, NKEYS_OFF, 6)),
            ("upper past the trailer", |p| put_u16(p, UPPER_OFF, 4096 - TRAILER + 1)),
            ("keys out of order", |p| {
                let (a, b) = (slot(p, 1), slot(p, 2));
                put_u16(p, HEADER + SLOT, b);
                put_u16(p, HEADER + 2 * SLOT, a);
            }),
            ("gap before the first cell", |p| {
                let u = upper(p);
                put_u16(p, UPPER_OFF, u - 1);
            }),
            ("cell overlapping its neighbour", |p| {
                let off = upper(p);
                put_u32(p, off, 21);
            }),
            ("cells ending before the trailer", |p| {
                let off = slot(p, 0);
                put_u32(p, off, 19);
            }),
            ("cell flags", |p| {
                let off = slot(p, 0);
                put_u16(p, off + 6, 1);
            }),
            ("cell overrunning the page", |p| {
                let off = slot(p, 0);
                put_u32(p, off, u32::MAX);
            }),
            ("wrong kind for the cells", |p| p[KIND_OFF] = KIND_BRANCH),
            ("short buffer", |p| p.truncate(2048)),
        ];
        for (what, damage) in damages {
            let mut page = damage_base(&g);
            damage(&mut page);
            assert!(validate(&page, &g).is_err(), "{what} passed validate");
        }
        assert_eq!(slot(&damage_base(&g), 0), end - cell_size_leaf(8, 20));

        // An entry over the limit (the codec itself does not check limits).
        let mut page = fresh(&g, Kind::Leaf);
        let fit = leaf_insert(&mut page, 0, &[1; 8], &vec![0; g.max_value(8) + 1]).unwrap();
        assert_eq!(fit, Fit::Done);
        assert!(validate(&page, &g).is_err());

        // A branch needs a child, and its cell 0 has no key.
        let page = fresh(&g, Kind::Branch);
        assert!(validate(&page, &g).is_err());
        let mut page = fresh(&g, Kind::Branch);
        for (i, key) in [&b"a"[..], b"b"].into_iter().enumerate() {
            let h = hdr(&page).unwrap();
            let off = reserve(&mut page, &h, i, cell_size_branch(1)).unwrap().unwrap();
            write_branch_cell(&mut page[off..off + cell_size_branch(1)], key, 1, 1);
        }
        assert!(validate(&page, &g).is_err());
        branch_delete(&mut page, 0).unwrap();
        validate(&page, &g).unwrap();
        assert_eq!(cells(&page), vec![(Vec::new(), 1, 1)]);
    }

    #[test]
    fn root_and_range_helpers() {
        let g = geometry(4096);
        let mut root = vec![0; g.page_size()];
        init_root(&mut root, 5, 9, (10, 1), b"m", (11, 2)).unwrap();
        validate(&root, &g).unwrap();
        assert_eq!((pgno(&root), txn(&root), kind(&root).unwrap()), (5, 9, Kind::Branch));
        assert_eq!(cells(&root), vec![(Vec::new(), 10, 1), (b"m".to_vec(), 11, 2)]);
        assert_eq!(branch_search(&root, b"a").unwrap(), 0);
        assert_eq!(branch_search(&root, b"m").unwrap(), 1);
        assert!(keys_in_range(&root, Some(b"m"), Some(b"n")).unwrap());
        assert!(!keys_in_range(&root, Some(b"n"), None).unwrap());
        assert!(!keys_in_range(&root, None, Some(b"m")).unwrap());
        set_pgno_txn(&mut root, 6, 10);
        assert_eq!((pgno(&root), txn(&root)), (6, 10));

        let leaf = damage_base(&g);
        let (first, last) = (ranked_key(0, 8), ranked_key(4, 8));
        assert!(keys_in_range(&leaf, Some(&first), Some(&ranked_key(5, 8))).unwrap());
        assert!(!keys_in_range(&leaf, Some(&ranked_key(1, 8)), None).unwrap());
        assert!(!keys_in_range(&leaf, None, Some(&last)).unwrap());
        assert!(keys_in_range(&fresh(&g, Kind::Leaf), Some(b"z"), Some(b"a")).unwrap());
        assert_eq!(leaf_search(&leaf, &last).unwrap(), Search::Found(4));
        assert_eq!(leaf_search(&leaf, b"").unwrap(), Search::NotFound(0));
        assert!(fits(&leaf, free_space(&leaf).unwrap() - SLOT).unwrap());
        assert!(!fits(&leaf, free_space(&leaf).unwrap() - SLOT + 1).unwrap());
        assert!(leaf_search(&root, b"a").is_err());
        assert!(branch_search(&leaf, b"a").is_err());
    }

    /// SplitMix64, for cheap reproducible page bytes.
    fn splitmix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Page bytes from a seed: plain noise, or noise shaped to pass the header and per-cell
    /// checks (a known kind, flags zero, slots into the cell area, small cell lengths) so the
    /// deeper code runs on overlapping, out-of-order cells.
    fn noise_page(seed: u64, size: usize, shaped: bool) -> Vec<u8> {
        let mut s = seed;
        let mut page = vec![0_u8; size];
        for chunk in page.chunks_mut(8) {
            chunk.copy_from_slice(&splitmix(&mut s).to_le_bytes()[..chunk.len()]);
        }
        if shaped {
            let r = splitmix(&mut s);
            let end = size - TRAILER;
            page[KIND_OFF] = if r & 1 == 0 { KIND_BRANCH } else { KIND_LEAF };
            page[FLAGS_OFF] = 0;
            let nkeys = (r >> 8) as usize % 200;
            let slots_end = HEADER + SLOT * nkeys;
            let upper = slots_end + (r >> 32) as usize % (end - slots_end + 1);
            put_u16(&mut page, NKEYS_OFF, nkeys);
            put_u16(&mut page, UPPER_OFF, upper);
            for i in 0..nkeys {
                let r = splitmix(&mut s);
                let off = upper + r as usize % (end - upper).max(1);
                put_u16(&mut page, HEADER + SLOT * i, off);
                if off + BRANCH_CELL_HEADER <= end {
                    put_u32(&mut page, off, (r >> 16) as u32 % 64);
                    put_u16(&mut page, off + 4, (r >> 24) as usize % 32);
                    put_u16(&mut page, off + 6, 0);
                    put_u16(&mut page, off + 8, (r >> 32) as usize % 32);
                }
            }
        }
        page
    }

    /// A valid leaf or branch of random content with a few random bytes overwritten, mostly in
    /// the header and slot array. A leaf's cells are inline, overflow (a random reference) or
    /// nested (a valid root).
    fn damaged_page(seed: u64, size: u32, flips: usize) -> Vec<u8> {
        let mut s = seed;
        let g = geometry(size);
        let leaf = splitmix(&mut s) & 1 == 0;
        let mut page = fresh(&g, if leaf { Kind::Leaf } else { Kind::Branch });
        if !leaf {
            assert_eq!(branch_insert(&mut page, 0, &[], 1, 1).unwrap(), Fit::Done);
        }
        for i in 1.. {
            let r = splitmix(&mut s);
            let key = ranked_key(i, (r % 40) as usize);
            let fit = if leaf && (r >> 56) & 3 == 0 {
                // An overflow cell: a 12-byte reference with flags 1.
                let ovf = Ovf { pgno: i, txn: (r >> 16) as u32, len: (r >> 32) as u32 };
                LeafMut::new(&mut page)
                    .and_then(|m| m.insert(i as usize - 1, &key, Val::Overflow(ovf)))
            } else if leaf && (r >> 56) & 3 == 1 {
                // A nested cell: a valid tree root's 32 bytes with flags 2.
                let root = TreeRoot {
                    root: i,
                    root_txn: (r >> 16) as u32,
                    height: 1,
                    entries: 1 + (r >> 32) % 9,
                    leaf_pages: 1,
                    ..TreeRoot::EMPTY
                }
                .encode();
                LeafMut::new(&mut page)
                    .and_then(|m| m.insert(i as usize - 1, &key, Val::Nested(&root)))
            } else if leaf {
                leaf_insert(&mut page, i as usize - 1, &key, &vec![7; (r >> 8) as usize % 200])
            } else {
                branch_insert(&mut page, i as usize, &key, i, i)
            };
            if fit.unwrap() == Fit::Full || r >> 60 == 0 {
                break;
            }
        }
        let hot = HEADER + SLOT * nkeys(&page) + 16;
        for _ in 0..flips {
            let r = splitmix(&mut s);
            let pos =
                if r & 1 == 0 { (r >> 8) as usize % hot } else { (r >> 8) as usize % page.len() };
            page[pos] = (r >> 40) as u8;
        }
        page
    }

    /// What [`branch_route`] and [`branch_route_at`] give, from the slow accessors.
    fn route_model(page: &[u8], ci: Result<usize>) -> Option<Route<'_>> {
        let ci = ci.ok()?;
        let (child, child_txn) = branch_child(page, ci).ok()?;
        let lo = if ci > 0 { Some(branch_key(page, ci).ok()?) } else { None };
        let hi = if ci + 1 < nkeys(page) { Some(branch_key(page, ci + 1).ok()?) } else { None };
        Some(Route { index: ci, lo, hi, child, child_txn })
    }

    /// What [`leaf_find`] gives, from the slow accessors.
    fn find_model<'a>(page: &'a [u8], key: &[u8]) -> Option<Find<'a>> {
        Some(match leaf_search(page, key).ok()? {
            Search::Found(index) => Find::Found { index, value: leaf_entry(page, index).ok()?.1 },
            Search::NotFound(index) => Find::NotFound {
                index,
                before: if index > 0 { Some(leaf_key(page, index - 1).ok()?) } else { None },
                after: if index < nkeys(page) { Some(leaf_key(page, index).ok()?) } else { None },
            },
        })
    }

    /// The one-pass searches agree with the slow accessors on any page, damaged or not: the same
    /// result, and an error exactly when one of those accessors errs (they check the same cells).
    fn assert_one_pass_matches(page: &[u8], key: &[u8], i: usize) {
        assert_eq!(leaf_find(page, key).ok(), find_model(page, key), "leaf_find {key:?}");
        let route = route_model(page, branch_search(page, key));
        assert_eq!(branch_route(page, key).ok(), route, "branch_route {key:?}");
        assert_eq!(branch_route_at(page, i).ok(), route_model(page, Ok(i)), "branch_route_at {i}");
    }

    /// Call every reader and mutator on `page` (mutators on copies); none may panic. The one-pass
    /// searches must match the slow accessors ([`assert_one_pass_matches`]).
    /// [`leaf_entry_with_key_tail`] against [`leaf_entry`] on any page: both refuse, or both give
    /// the key, and for an inline value the key's last `t` bytes (all of them for a `t` past its
    /// length) and the value as one slice of the page, starting inside the key; an overflow
    /// reference or a nested root is the same one.
    fn assert_key_tail_matches(page: &[u8], i: usize, t: usize) {
        let (got, want) = (leaf_entry_with_key_tail(page, i, t), leaf_entry(page, i));
        let ((key, tail), (want_key, want_val)) = match (got, want) {
            (Ok(got), Ok(want)) => (got, want),
            (Err(_), Err(_)) => return,
            (got, want) => panic!("cell {i}, t {t}: {got:?} where leaf_entry gives {want:?}"),
        };
        assert_eq!(key, want_key, "cell {i}, t {t}: key");
        match (tail, want_val) {
            (Val::Inline(joined), Val::Inline(value)) => {
                let start = key.len() - t.min(key.len());
                assert_eq!(joined, [&key[start..], value].concat(), "cell {i}, t {t}");
                // One slice of the page, from inside the key: no copy.
                assert_eq!(joined.as_ptr(), key[start..].as_ptr(), "cell {i}, t {t}");
            }
            (tail, value) => assert_eq!(tail, value, "cell {i}, t {t}: a reference or a root"),
        }
    }

    fn exercise(page: &[u8], key: &[u8], sel: usize) {
        let g = geometry(page.len() as u32);
        let _ = validate(page, &g);
        let _ = (pgno(page), kind(page), flags(page), nkeys(page), txn(page), upper(page));
        let _ = (free_space(page), fits(page, 100));
        let _ = (leaf_search(page, key), branch_search(page, key));
        let _ = keys_in_range(page, Some(key), Some(&[0xff; 4]));
        let n = nkeys(page);
        for i in 0..=n.min(300) {
            let _ = (
                leaf_entry(page, i),
                leaf_key(page, i),
                branch_child(page, i),
                branch_key(page, i),
            );
            assert_eq!(branch_route_at(page, i).ok(), route_model(page, Ok(i)), "route_at {i}");
            assert_key_tail_matches(page, i, sel % 70);
        }
        let i = sel % (n + 2);
        assert_one_pass_matches(page, key, i);
        assert_one_pass_matches(page, &[], i);
        let sep = [&[1][..], key].concat();
        let empty = |kind| {
            let mut page = vec![0; g.page_size()];
            init(&mut page, kind, 1, 1);
            page
        };
        let _ = leaf_insert(&mut page.to_vec(), i, key, b"value");
        let _ = leaf_replace_value(&mut page.to_vec(), i, &[7; 300]);
        let _ = leaf_replace_value(&mut page.to_vec(), i, &[]);
        let _ = leaf_delete(&mut page.to_vec(), i);
        let _ = branch_insert(&mut page.to_vec(), i.max(1), &sep, 1, 2);
        let _ = branch_set_child(&mut page.to_vec(), i, 1, 2);
        let _ = branch_delete(&mut page.to_vec(), i);
        let _ = branch_delete(&mut page.to_vec(), 0);
        let append = sel.is_multiple_of(3);
        let _ = split_leaf(&mut page.to_vec(), &mut empty(Kind::Leaf), i, key, b"v", append);
        let _ = split_branch(
            &mut page.to_vec(),
            &mut empty(Kind::Branch),
            i.max(1),
            &sep,
            1,
            2,
            append,
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn arbitrary_bytes_never_panic(
            seed in any::<u64>(),
            size in select(&SIZES[..]),
            shaped in any::<bool>(),
            key in vec(any::<u8>(), 0..24),
            sel in any::<usize>(),
        ) {
            exercise(&noise_page(seed, size as usize, shaped), &key, sel);
        }

        #[test]
        fn damaged_valid_pages_never_panic(
            seed in any::<u64>(),
            size in select(&SIZES[..]),
            flips in 1_usize..6,
            key in vec(any::<u8>(), 0..24),
            sel in any::<usize>(),
        ) {
            exercise(&damaged_page(seed, size, flips), &key, sel);
        }
    }

    /// An independent decoding of the page format for the view tests: the header and each cell
    /// read straight from the bytes, sharing no helper with the codec, with `None` wherever the
    /// codec must refuse (the checks of `hdr`, `cell_off`, `leaf_cell` and `branch_cell`,
    /// restated), and the searches restated on top with the codec's probe order, since which cells
    /// a search reads decides whether a damaged cell makes it fail.
    struct Model<'a> {
        page: &'a [u8],
        leaf: bool,
        n: usize,
        upper: usize,
        end: usize,
    }

    impl<'a> Model<'a> {
        fn new(page: &'a [u8]) -> Option<Self> {
            let len = page.len();
            if !len.is_power_of_two() || !(4096..=65536).contains(&len) {
                return None;
            }
            let leaf = match page[4] {
                1 => false,
                2 => true,
                _ => return None,
            };
            let at = |i: usize| usize::from(u16::from_le_bytes([page[i], page[i + 1]]));
            let (n, upper, end) = (at(6), at(16), len - 4);
            (page[5] == 0 && upper <= end && 24 + 2 * n <= upper).then_some(Self {
                page,
                leaf,
                n,
                upper,
                end,
            })
        }

        fn u16_at(&self, i: usize) -> usize {
            usize::from(u16::from_le_bytes([self.page[i], self.page[i + 1]]))
        }

        fn u32_at(&self, i: usize) -> u32 {
            u32::from_le_bytes(self.page[i..i + 4].try_into().unwrap())
        }

        fn free(&self) -> usize {
            self.upper - (24 + 2 * self.n)
        }

        fn fits(&self, cell_size: usize) -> bool {
            cell_size.saturating_add(2) <= self.free()
        }

        /// Cell `i` as `(key, rest)`: a leaf's value, or a branch's 8 pointer bytes.
        fn cell(&self, i: usize) -> Option<(&'a [u8], &'a [u8])> {
            if i >= self.n {
                return None;
            }
            let off = self.u16_at(24 + 2 * i);
            let fixed = if self.leaf { 8 } else { 10 };
            if off < self.upper || off + fixed > self.end {
                return None;
            }
            let page = self.page;
            if self.leaf {
                let (vlen, klen) = (self.u32_at(off) as usize, self.u16_at(off + 4));
                let flags = self.u16_at(off + 6);
                let shaped = flags == 0 || (flags == 1 && vlen == 12) || (flags == 2 && vlen == 32);
                if !shaped || off + 8 + klen + vlen > self.end {
                    return None;
                }
                let k = off + 8;
                Some((&page[k..k + klen], &page[k + klen..k + klen + vlen]))
            } else {
                let klen = self.u16_at(off + 8);
                if off + 10 + klen > self.end {
                    return None;
                }
                Some((&page[off + 10..off + 10 + klen], &page[off..off + 8]))
            }
        }

        /// Leaf cell `i` as `(key, value)`: flags 1 makes its 12 value bytes an overflow
        /// reference, flags 2 its 32 value bytes a nested root.
        fn entry(&self, i: usize) -> Option<(&'a [u8], Val<'a>)> {
            let (k, v) = self.cell(i)?;
            let off = self.u16_at(24 + 2 * i);
            Some(match self.u16_at(off + 6) {
                1 => (k, Val::Overflow(Ovf::decode(v.try_into().unwrap()))),
                2 => (k, Val::Nested(v.try_into().unwrap())),
                _ => (k, Val::Inline(v)),
            })
        }

        fn child(&self, i: usize) -> Option<(u32, u32)> {
            let (_, ptr) = self.cell(i)?;
            let word = |at: usize| u32::from_le_bytes(ptr[at..at + 4].try_into().unwrap());
            Some((word(0), word(4)))
        }

        fn find(&self, key: &[u8]) -> Option<Find<'a>> {
            let (mut lo, mut hi, mut before, mut after) = (0, self.n, None, None);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let (k, v) = self.entry(mid)?;
                match k.cmp(key) {
                    Ordering::Less => (lo, before) = (mid + 1, Some(k)),
                    Ordering::Greater => (hi, after) = (mid, Some(k)),
                    Ordering::Equal => return Some(Find::Found { index: mid, value: v }),
                }
            }
            Some(Find::NotFound { index: lo, before, after })
        }

        fn search(&self, key: &[u8]) -> Option<Search> {
            Some(match self.find(key)? {
                Find::Found { index, .. } => Search::Found(index),
                Find::NotFound { index, .. } => Search::NotFound(index),
            })
        }

        fn route(&self, key: &[u8]) -> Option<Route<'a>> {
            if self.n == 0 {
                return None;
            }
            let (mut lo, mut hi, mut below, mut above) = (1, self.n, None, None);
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let (k, _) = self.cell(mid)?;
                if k <= key {
                    (lo, below) = (mid + 1, Some(k));
                } else {
                    (hi, above) = (mid, Some(k));
                }
            }
            let (child, child_txn) = self.child(lo - 1)?;
            Some(Route { index: lo - 1, lo: below, hi: above, child, child_txn })
        }

        fn route_at(&self, i: usize) -> Option<Route<'a>> {
            let (k, _) = self.cell(i)?;
            let (child, child_txn) = self.child(i)?;
            let hi = if i + 1 < self.n { Some(self.cell(i + 1)?.0) } else { None };
            Some(Route { index: i, lo: (i > 0).then_some(k), hi, child, child_txn })
        }

        fn keys_in_range(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<bool> {
            let first = usize::from(!self.leaf);
            if self.n <= first {
                return Some(true);
            }
            let lo_ok = match lo {
                Some(lo) => self.cell(first)?.0 >= lo,
                None => true,
            };
            let hi_ok = match hi {
                Some(hi) => self.cell(self.n - 1)?.0 < hi,
                None => true,
            };
            Some(lo_ok && hi_ok)
        }
    }

    /// Every page reader against [`Model`] on one page, damaged or not: each view method on one
    /// parse ([`View`], [`LeafView`], [`BranchView`]) and the free reader over it give the
    /// model's result, and an error exactly where the model refuses.
    fn assert_readers_match_the_model(page: &[u8], key: &[u8], sel: usize) {
        let model = Model::new(page);
        let leaf = model.as_ref().filter(|m| m.leaf);
        let branch = model.as_ref().filter(|m| !m.leaf);
        let n = model.as_ref().map_or(0, |m| m.n);
        let (view, lv, bv) =
            (View::new(page).ok(), LeafView::new(page).ok(), BranchView::new(page).ok());
        assert_eq!(
            (view.is_some(), lv.is_some(), bv.is_some()),
            (model.is_some(), leaf.is_some(), branch.is_some())
        );
        match view {
            Some(View::Leaf(_)) => assert!(leaf.is_some(), "View::new parsed a leaf"),
            Some(View::Branch(_)) => assert!(branch.is_some(), "View::new parsed a branch"),
            None => {}
        }
        // The given key, the extremes, and a key of the page with keys either side of it.
        let mut probes = vec![key.to_vec(), Vec::new(), vec![0xff; 8]];
        if let Some((k, _)) = model.as_ref().and_then(|m| m.cell(sel % n.max(1))) {
            probes.extend([
                k.to_vec(),
                [k, &[0]].concat(),
                k[..k.len().saturating_sub(1)].to_vec(),
            ]);
        }
        for p in &probes {
            let want = leaf.and_then(|m| m.find(p));
            assert_eq!(lv.and_then(|v| v.find(p).ok()), want, "LeafView::find {p:?}");
            assert_eq!(leaf_find(page, p).ok(), want, "leaf_find {p:?}");
            let want = leaf.and_then(|m| m.search(p));
            assert_eq!(lv.and_then(|v| v.search(p).ok()), want, "LeafView::search {p:?}");
            assert_eq!(leaf_search(page, p).ok(), want, "leaf_search {p:?}");
            let want = branch.and_then(|m| m.route(p));
            assert_eq!(bv.and_then(|v| v.route(p).ok()), want, "BranchView::route {p:?}");
            assert_eq!(branch_route(page, p).ok(), want, "branch_route {p:?}");
        }
        for i in (0..n.min(64)).chain([n.saturating_sub(1), n, n + 1, sel % (n + 2)]) {
            let want = leaf.and_then(|m| m.entry(i));
            assert_eq!(lv.and_then(|v| v.entry(i).ok()), want, "LeafView::entry {i}");
            assert_eq!(leaf_entry(page, i).ok(), want, "leaf_entry {i}");
            assert_eq!(lv.and_then(|v| v.key(i).ok()), want.map(|c| c.0), "LeafView::key {i}");
            assert_eq!(leaf_key(page, i).ok(), want.map(|c| c.0), "leaf_key {i}");
            let want = branch.and_then(|m| m.cell(i)).map(|c| c.0);
            assert_eq!(bv.and_then(|v| v.key(i).ok()), want, "BranchView::key {i}");
            assert_eq!(branch_key(page, i).ok(), want, "branch_key {i}");
            let want = branch.and_then(|m| m.child(i));
            assert_eq!(bv.and_then(|v| v.child(i).ok()), want, "BranchView::child {i}");
            assert_eq!(branch_child(page, i).ok(), want, "branch_child {i}");
            let want = branch.and_then(|m| m.route_at(i));
            assert_eq!(bv.and_then(|v| v.route_at(i).ok()), want, "BranchView::route_at {i}");
            assert_eq!(branch_route_at(page, i).ok(), want, "branch_route_at {i}");
        }
        let bounds = [None, Some(&probes[0][..]), Some(&probes[probes.len() - 1][..])];
        for lo in bounds {
            for hi in bounds.into_iter().chain([Some(&[0xff; 8][..])]) {
                let want = model.as_ref().and_then(|m| m.keys_in_range(lo, hi));
                let got = [
                    view.and_then(|v| v.keys_in_range(lo, hi).ok()),
                    lv.and_then(|v| v.keys_in_range(lo, hi).ok()),
                    bv.and_then(|v| v.keys_in_range(lo, hi).ok()),
                ];
                assert_eq!(got[0], want, "View::keys_in_range {lo:?} {hi:?}");
                assert_eq!(got[1].or(got[2]), want, "kind view keys_in_range {lo:?} {hi:?}");
                assert_eq!(keys_in_range(page, lo, hi).ok(), want, "keys_in_range {lo:?} {hi:?}");
            }
        }
        let free = model.as_ref().map(Model::free);
        assert_eq!(view.map(|v| v.free_space()), free, "View::free_space");
        assert_eq!(lv.map(|v| v.free_space()).or(bv.map(|v| v.free_space())), free, "free_space");
        assert_eq!(free_space(page).ok(), free, "free_space");
        let near = free.unwrap_or(0);
        for size in [0, 1, near.saturating_sub(3), near.saturating_sub(2), near, sel, usize::MAX] {
            let want = model.as_ref().map(|m| m.fits(size));
            assert_eq!(view.map(|v| v.fits(size)), want, "View::fits {size}");
            assert_eq!(lv.map(|v| v.fits(size)).or(bv.map(|v| v.fits(size))), want, "fits {size}");
            assert_eq!(fits(page, size).ok(), want, "fits {size}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn views_match_an_independent_model(
            seed in any::<u64>(),
            size in select(&SIZES[..]),
            source in 0_u8..3,
            flips in 0_usize..6,
            key in vec(any::<u8>(), 0..24),
            sel in any::<usize>(),
        ) {
            let page = match source {
                0 => noise_page(seed, size as usize, false),
                1 => noise_page(seed, size as usize, true),
                _ => damaged_page(seed, size, flips),
            };
            assert_readers_match_the_model(&page, &key, sel);
        }
    }

    #[test]
    fn short_buffers_are_rejected_not_indexed() {
        let g = geometry(4096);
        for len in [0, 3, 23, 24, 100, 4095, 4097, 8000] {
            // An empty leaf's header, cut to `len` bytes where it is shorter.
            let mut page = vec![0; len.max(HEADER)];
            page[KIND_OFF] = KIND_LEAF;
            put_u16(&mut page, UPPER_OFF, len.saturating_sub(TRAILER).min(u16::MAX.into()));
            page.truncate(len);
            let _ = (pgno(&page), flags(&page), nkeys(&page), txn(&page), upper(&page));
            assert!(free_space(&page).is_err());
            assert!(leaf_search(&page, b"k").is_err());
            assert!(leaf_entry(&page, 0).is_err());
            assert!(branch_search(&page, b"k").is_err());
            assert!(validate(&page, &g).is_err());
        }
    }

    /// A full `kind` page (page 7, txn 3, as [`fresh`]) whose keys came in the random order of
    /// `ranks` (each made odd, duplicates skipped), each at its sorted place, so its cells lie in
    /// the cell area out of slot order; sizes cycle through `specs`. Stops at the first entry that
    /// does not fit and returns the page with that entry, re-keyed to sort at the cell index `at`
    /// picked by `at_sel`: `(page, at, key, value)` (a branch's value is empty). One `at_sel` in
    /// four picks the place past the last cell (`at == nkeys`, where an append split cuts), the
    /// others any place a split can insert at.
    fn scrambled_full_page(
        g: &Geometry,
        kind: Kind,
        specs: &[Spec],
        ranks: &[u32],
        at_sel: usize,
    ) -> (Vec<u8>, usize, Vec<u8>, Vec<u8>) {
        let mut page = fresh(g, kind);
        let first = usize::from(kind == Kind::Branch);
        if kind == Kind::Branch {
            assert_eq!(branch_insert(&mut page, 0, &[], 0, 0).unwrap(), Fit::Done);
        }
        let mut sorted: Vec<u32> = Vec::new();
        let mut ranks = ranks.iter().map(|r| r % (u32::MAX / 2) * 2 + 1).cycle();
        for (round, s) in specs.iter().cycle().enumerate() {
            let klen = s.klen(g).max(4);
            let value = if kind == Kind::Leaf { s.value(g, klen) } else { Vec::new() };
            // Past the given ranks, fresh ones above them keep the keys distinct.
            let mut rank = ranks.next().unwrap();
            if round >= 400 || sorted.binary_search(&rank).is_ok() {
                rank = sorted.last().map_or(1, |r| r + 2);
            }
            let pos = sorted.binary_search(&rank).unwrap_err();
            let key = ranked_key(rank, klen);
            let fit = match kind {
                Kind::Leaf => leaf_insert(&mut page, pos, &key, &value).unwrap(),
                Kind::Branch => branch_insert(&mut page, pos + 1, &key, rank, !rank).unwrap(),
            };
            if fit == Fit::Full {
                // `n` keys after a branch's cell 0: cells `0..n + first`.
                let n = sorted.len();
                let at =
                    if at_sel.is_multiple_of(4) { n + first } else { first + at_sel / 4 % (n + 1) };
                // An even rank between the neighbours of cell `at` (ranks are odd and distinct).
                let below = at.checked_sub(first + 1).map_or(0, |i| sorted[i] + 1);
                return (page, at, ranked_key(below, klen), value);
            }
            sorted.insert(pos, rank);
        }
        unreachable!("specs is never empty")
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Splits through one reused [`SplitBuf`], the right half written into a page of
        /// garbage ([`crate::tree::POISON`]) as the writer's fresh allocation may be, give exactly
        /// the bytes of the reference split (the right half built in an empty page, the left one
        /// re-appended from a copy), the right half's free gap aside ([`outside_gap`]): both
        /// halves and the separator, for leaves and branches at every page size, with the pending
        /// cell anywhere, with and without `append`.
        #[test]
        fn splits_match_the_reference_split(
            specs in vec(spec(), 1..40),
            ranks in vec(any::<u32>(), 1..400),
            at_sel in any::<usize>(),
            append in any::<bool>(),
        ) {
            let mut buf = SplitBuf::default();
            for size in SIZES {
                let g = geometry(size);
                for kind in [Kind::Leaf, Kind::Branch] {
                    let (src, at, key, value) =
                        scrambled_full_page(&g, kind, &specs, &ranks, at_sel);
                    let (mut want_src, mut want_dst) = (src.clone(), fresh(&g, kind));
                    let (mut got_src, mut got_dst) =
                        (src.clone(), vec![crate::tree::POISON; g.page_size()]);
                    let want = match kind {
                        Kind::Leaf => {
                            buf.split_leaf(&got_src, at, &key, Val::Inline(&value), append).unwrap();
                            reference::split_leaf(
                                &mut want_src, &mut want_dst, at, &key, &value, append,
                            )
                        }
                        Kind::Branch => {
                            buf.sep.clone_from(&key);
                            buf.split_branch(&got_src, at, 1000, 77, append).unwrap();
                            reference::split_branch(
                                &mut want_src, &mut want_dst, at, &key, 1000, 77, append,
                            )
                        }
                    }
                    .unwrap();
                    buf.write_right(&mut got_dst, 7, 3).unwrap();
                    buf.write_left(&mut got_src).unwrap();
                    prop_assert_eq!(buf.sep(), &want[..], "{} {:?}: separator", size, kind);
                    prop_assert!(got_src == want_src, "{size} {kind:?} at {at}: left halves differ");
                    prop_assert!(
                        outside_gap(&got_dst) == outside_gap(&want_dst),
                        "{size} {kind:?} at {at}: right halves differ"
                    );
                    validate(&got_src, &g).unwrap();
                    validate(&got_dst, &g).unwrap();
                }
            }
        }
    }

    /// [`Geometry::place`]: a value up to [`Geometry::max_value`] stays inline (M1's rule, so
    /// every inline layout is unchanged); a larger one goes to a run of `ceil((28 + len) / P)`
    /// pages when its key leaves room for the 12-byte cell and it is at most [`MAX_VALUE`];
    /// anything else is refused with the error and limit `Env::max_value_len` reports.
    #[test]
    fn place_keeps_m1_values_inline_and_sends_larger_ones_to_runs() {
        for size in SIZES {
            let g = geometry(size);
            let (p, mk) = (g.page_size(), g.max_key());
            // `Error` has no `PartialEq`: compare the debug forms.
            let want = |r: Result<Place>| format!("{r:?}");
            let place = |klen: usize, vlen: usize| want(g.place(klen, vlen));
            let runs = |len: usize| want(Ok(Place::Overflow { npages: (28 + len).div_ceil(p) }));
            for klen in [0, 1, 24, mk - OVF_CELL + 2, mk - OVF_CELL + 3] {
                let max = g.max_value(klen);
                assert_eq!(place(klen, 0), want(Ok(Place::Inline)), "{size}: klen {klen}");
                assert_eq!(place(klen, max), want(Ok(Place::Inline)), "{size}: klen {klen}");
                if klen + OVF_CELL <= mk + 2 {
                    assert_eq!(g.max_table_value(klen), MAX_VALUE);
                    assert_eq!(place(klen, max + 1), runs(max + 1), "{size}: klen {klen}");
                    assert_eq!(place(klen, p - 28), runs(p - 28), "one full page");
                    assert_eq!(place(klen, p - 27), runs(p - 27), "two pages");
                    assert_eq!(place(klen, MAX_VALUE), runs(MAX_VALUE), "the largest value");
                    assert_eq!(
                        place(klen, MAX_VALUE + 1),
                        want(Err(Error::ValueTooLarge { len: MAX_VALUE + 1, max: MAX_VALUE }))
                    );
                } else {
                    // The 12-byte cell would not fit a maximal entry: inline values only.
                    assert_eq!(g.max_table_value(klen), max);
                    assert_eq!(
                        place(klen, max + 1),
                        want(Err(Error::ValueTooLarge { len: max + 1, max })),
                        "{size}: klen {klen}"
                    );
                }
            }
            assert_eq!(place(mk + 1, 0), want(Err(Error::KeyTooLarge { len: mk + 1, max: mk })));
            // The longest key with a run: its overflow cell is exactly a maximal entry.
            let klen = mk - OVF_CELL + 2;
            assert_eq!(cell_size_leaf(klen, OVF_CELL) + SLOT, g.max_entry());
        }
    }

    /// An overflow cell (flags 1, 12 value bytes) reads back as [`Val::Overflow`] through every
    /// reader, replaces and is replaced by inline values in place, and is canonical: [`validate`]
    /// refuses flags other than 0 and 1, an overflow cell whose value is not 12 bytes, and a run
    /// length that would fit inline or exceeds [`MAX_VALUE`]. The catalog's and the free list's
    /// readers ([`Val::inline`]) refuse it.
    #[test]
    fn overflow_cells_are_canonical_references() {
        for size in SIZES {
            let g = geometry(size);
            let key = b"key".as_slice();
            let ovf = Ovf { pgno: 9, txn: 0xDEAD_BEEF, len: (g.max_value(key.len()) + 1) as u32 };
            let mut page = fresh(&g, Kind::Leaf);
            assert_eq!(leaf_insert(&mut page, 0, b"a", b"inline").unwrap(), Fit::Done);
            let fit = LeafMut::new(&mut page).unwrap().insert(1, key, Val::Overflow(ovf)).unwrap();
            assert_eq!(fit, Fit::Done);
            validate(&page, &g).expect("a canonical overflow cell");
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Overflow(ovf)));
            assert_eq!(leaf_entry(&page, 0).unwrap(), (b"a".as_slice(), Val::Inline(b"inline")));
            assert_eq!(
                leaf_find(&page, key).unwrap(),
                Find::Found { index: 1, value: Val::Overflow(ovf) }
            );
            assert!(matches!(Val::Overflow(ovf).inline(), Err(Error::Corrupt(_))));
            assert!(matches!(leaf_inline_entry(&page, 1), Err(Error::Corrupt(_))));
            assert_eq!(
                free_space(&page).unwrap() + 2 * (SLOT + 8) + 1 + 6 + 3 + OVF_CELL,
                g.usable()
            );

            // Replace in place: overflow -> inline -> overflow, the free space following.
            let free = free_space(&page).unwrap();
            let fit = LeafMut::new(&mut page).unwrap().replace_value(1, Val::Inline(&[5; 40]));
            assert_eq!(fit.unwrap(), Fit::Done);
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Inline(&[5; 40])));
            assert_eq!(free_space(&page).unwrap(), free - 28);
            validate(&page, &g).expect("inline again");
            let other = Ovf { len: MAX_VALUE as u32, ..ovf };
            let fit = LeafMut::new(&mut page).unwrap().replace_value(1, Val::Overflow(other));
            assert_eq!(fit.unwrap(), Fit::Done);
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Overflow(other)));
            assert_eq!(free_space(&page).unwrap(), free);
            validate(&page, &g).expect("the largest value");

            let off = slot(&page, 1);
            let damaged = |change: &dyn Fn(&mut [u8])| {
                let mut p = page.clone();
                change(&mut p);
                p
            };
            for (what, bad) in [
                ("flags 2", damaged(&|p| put_u16(p, off + 6, 2))),
                ("flags 3", damaged(&|p| put_u16(p, off + 6, 3))),
                ("flags 0x100", damaged(&|p| put_u16(p, off + 6, 0x100))),
            ] {
                assert!(matches!(validate(&bad, &g), Err(Error::Corrupt(_))), "{size}: {what}");
                assert!(leaf_entry(&bad, 1).is_err(), "{size}: {what}");
                assert!(leaf_find(&bad, key).is_err(), "{size}: {what}");
            }
            // Flags 1 on an inline cell whose value is not 12 bytes.
            let bad = damaged(&|p| put_u16(p, slot(p, 0) + 6, 1));
            assert!(matches!(validate(&bad, &g), Err(Error::Corrupt(_))), "{size}: flags 1");
            assert!(leaf_entry(&bad, 0).is_err(), "{size}: flags 1 on a 6-byte value");
            // Run lengths that are not canonical: one that fits inline, one past the limit.
            let v = off + LEAF_CELL_HEADER + key.len();
            for len in [0, g.max_value(key.len()), MAX_VALUE + 1, u32::MAX as usize] {
                let bad = damaged(&|p| put_u32(p, v + 8, len as u32));
                assert!(matches!(validate(&bad, &g), Err(Error::Corrupt(_))), "{size}: len {len}");
            }
        }
    }

    /// Whether `r` is [`Error::Corrupt`] with a message containing `what`.
    fn corrupt_with<T>(r: Result<T>, what: &str) -> bool {
        matches!(&r, Err(Error::Corrupt(m)) if m.contains(what))
    }

    /// A nested cell (flags 2, 32 value bytes: an encoded [`TreeRoot`]) reads back as
    /// [`Val::Nested`] through every reader, the key-tail one included, replaces and is replaced
    /// by inline values in place, and is canonical: [`validate`] refuses one whose bytes are an
    /// impossible or an empty root, which the cell readers pass on unchecked. The cell checks
    /// refuse flags 3, flags 1 and flags 0x102 on it, flags 2 on a value of another length (with
    /// the message they gave before nested cells existed), and an overrunning nested cell as an
    /// overrun. The inline readers ([`Val::inline`]) refuse it ([`nested_error`]).
    #[test]
    fn nested_cells_are_canonical_roots() {
        const NONE: &str = "a nested cell in a tree that holds none";
        const NOT_A_TREE: &str = "holds an impossible or empty tree root";
        for size in SIZES {
            let g = geometry(size);
            let key = b"key".as_slice();
            let tree = TreeRoot {
                root: 9,
                root_txn: 7,
                height: 1,
                entries: 3,
                leaf_pages: 1,
                ..TreeRoot::EMPTY
            };
            let root = tree.encode();
            let mut page = fresh(&g, Kind::Leaf);
            assert_eq!(leaf_insert(&mut page, 0, b"a", b"inline").unwrap(), Fit::Done);
            let fit = LeafMut::new(&mut page).unwrap().insert(1, key, Val::Nested(&root)).unwrap();
            assert_eq!(fit, Fit::Done);
            validate(&page, &g).expect("a canonical nested cell");
            let off = slot(&page, 1);
            assert_eq!(read_u32(&page, off) as usize, NESTED_CELL, "{size}: vlen");
            assert_eq!(read_u16(&page, off + 6), CELL_NESTED, "{size}: flags");
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Nested(&root)));
            assert_eq!(leaf_entry(&page, 0).unwrap(), (b"a".as_slice(), Val::Inline(b"inline")));
            assert_eq!(
                leaf_find(&page, key).unwrap(),
                Find::Found { index: 1, value: Val::Nested(&root) }
            );
            for t in [0, 1, key.len(), key.len() + 5] {
                let got = leaf_entry_with_key_tail(&page, 1, t).unwrap();
                assert_eq!(got, (key, Val::Nested(&root)), "{size}: t {t}");
            }
            assert_eq!(Val::Nested(&root).cell_len(), NESTED_CELL);
            assert!(corrupt_with(Val::Nested(&root).inline(), NONE), "{size}: Val::inline");
            assert!(corrupt_with(leaf_inline_entry(&page, 1), NONE), "{size}: leaf_inline_entry");
            assert_eq!(
                free_space(&page).unwrap() + 2 * (SLOT + 8) + 1 + 6 + 3 + NESTED_CELL,
                g.usable()
            );

            // Replace in place: nested -> inline -> nested, the free space following.
            let free = free_space(&page).unwrap();
            let fit = LeafMut::new(&mut page).unwrap().replace_value(1, Val::Inline(&[5; 40]));
            assert_eq!(fit.unwrap(), Fit::Done);
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Inline(&[5; 40])));
            assert_eq!(free_space(&page).unwrap(), free - 8);
            validate(&page, &g).expect("inline again");
            let other = TreeRoot {
                root: 11,
                root_txn: 8,
                height: 2,
                overflow_pages: 3,
                entries: 900,
                branch_pages: 1,
                leaf_pages: 4,
            }
            .encode();
            let fit = LeafMut::new(&mut page).unwrap().replace_value(1, Val::Nested(&other));
            assert_eq!(fit.unwrap(), Fit::Done);
            assert_eq!(leaf_entry(&page, 1).unwrap(), (key, Val::Nested(&other)));
            assert_eq!(free_space(&page).unwrap(), free);
            validate(&page, &g).expect("another nested root");

            let off = slot(&page, 1);
            let damaged = |change: &dyn Fn(&mut [u8])| {
                let mut p = page.clone();
                change(&mut p);
                p
            };
            // Cell shapes the cell checks refuse, at cell `i`, each with its message.
            for (what, i, bad, msg) in [
                ("flags 3", 1, damaged(&|p| put_u16(p, off + 6, 3)), "cell 1 has flags 0x3"),
                (
                    "flags 1",
                    1,
                    damaged(&|p| put_u16(p, off + 6, CELL_OVERFLOW)),
                    "overflow cell 1 has a 32-byte value",
                ),
                ("flags 0x102", 1, damaged(&|p| put_u16(p, off + 6, 0x102)), "has flags 0x102"),
                (
                    "flags 2 on 6 bytes",
                    0,
                    damaged(&|p| put_u16(p, slot(p, 0) + 6, CELL_NESTED)),
                    "cell 0 has flags 0x2",
                ),
                ("overrun", 1, damaged(&|p| put_u16(p, off + 4, 0xffff)), "overruns"),
            ] {
                let entry = format!("{:?}", leaf_entry(&bad, i));
                assert!(corrupt_with(leaf_entry(&bad, i), msg), "{size}: {what}: {entry}");
                assert!(corrupt_with(validate(&bad, &g), msg), "{size}: {what}: validate");
                assert!(leaf_entry_with_key_tail(&bad, i, 2).is_err(), "{size}: {what}: tail");
                if i == 1 {
                    assert!(leaf_find(&bad, key).is_err(), "{size}: {what}: leaf_find");
                }
            }
            // Root bytes that are no tree: the readers pass them on (a root read from a cell is
            // checked where it is used), `validate` refuses them.
            let v = off + LEAF_CELL_HEADER + key.len();
            for (what, bytes) in [
                ("empty", TreeRoot::EMPTY.encode()),
                ("zeros", [0; NESTED_CELL]),
                ("ones", [0xff; NESTED_CELL]),
                ("impossible", TreeRoot { leaf_pages: 2, ..tree }.encode()),
            ] {
                let bad = damaged(&|p| p[v..v + NESTED_CELL].copy_from_slice(&bytes));
                assert_eq!(
                    leaf_entry(&bad, 1).unwrap(),
                    (key, Val::Nested(&bytes)),
                    "{size}: {what}"
                );
                let refused = validate(&bad, &g);
                assert!(corrupt_with(refused, "nested cell 1 holds"), "{size}: {what}");
            }
            // Flags 2 on an inline value of 32 bytes makes it a nested cell of whatever root its
            // bytes hold.
            let mut flagged = page.clone();
            let zeros = [0; NESTED_CELL];
            assert_eq!(leaf_insert(&mut flagged, 2, b"z", &zeros).unwrap(), Fit::Done);
            validate(&flagged, &g).expect("an inline value of 32 bytes");
            let z = slot(&flagged, 2);
            put_u16(&mut flagged, z + 6, CELL_NESTED);
            assert_eq!(leaf_entry(&flagged, 2).unwrap(), (b"z".as_slice(), Val::Nested(&zeros)));
            assert!(corrupt_with(validate(&flagged, &g), NOT_A_TREE), "{size}: flags 2 on zeros");
            let z = z + LEAF_CELL_HEADER + 1;
            flagged[z..z + NESTED_CELL].copy_from_slice(&root);
            validate(&flagged, &g).expect("flags 2 on the 32 bytes of a valid root");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// `leaf_entry_with_key_tail` on random valid leaves (an empty first key now and then,
        /// keys up to `max_key`, values up to the inline limit, overflow and nested cells among
        /// them): the key, and for an inline value `key[klen − t..] ‖ value` for `t` of 0, the
        /// key's length, past it (clamped: `key ‖ value`) and in between; an overflow cell's
        /// reference; a nested cell's root bytes.
        #[test]
        fn key_tail_joins_the_keys_last_bytes_to_an_inline_value(
            size in select(&SIZES[..]),
            empty_first in any::<bool>(),
            specs in vec((spec(), any::<u8>()), 1..80),
            tsel in any::<usize>(),
        ) {
            let g = geometry(size);
            let mut page = fresh(&g, Kind::Leaf);
            // Each cell's key, its overflow reference or `None`, whether it is nested, and the
            // bytes of its inline value or nested root.
            type ModelCell = (Vec<u8>, Option<Ovf>, bool, Vec<u8>);
            let mut model: Vec<ModelCell> = Vec::new();
            for (n, (s, ovf_sel)) in specs.iter().enumerate() {
                let key = if n == 0 && empty_first {
                    Vec::new()
                } else {
                    ranked_key(2 * n as u32 + 1, s.klen(&g))
                };
                let value = s.value(&g, key.len());
                let ovf = (ovf_sel % 4 == 0 && g.overflow_fits(key.len())).then(|| Ovf {
                    pgno: n as u32 + 9,
                    txn: 0xC0FF_EE00 | n as u32,
                    len: (g.max_value(key.len()) + 1) as u32,
                });
                let nested = ovf_sel % 4 == 1 && g.max_value(key.len()) >= NESTED_CELL;
                let root = TreeRoot {
                    root: n as u32 + 9,
                    root_txn: 0xC0DE_0000 | n as u32,
                    height: 1,
                    entries: n as u64 + 1,
                    leaf_pages: 1,
                    ..TreeRoot::EMPTY
                }
                .encode();
                let leaf = LeafMut::new(&mut page).unwrap();
                let fit = match ovf {
                    Some(ovf) => leaf.insert(model.len(), &key, Val::Overflow(ovf)),
                    None if nested => leaf.insert(model.len(), &key, Val::Nested(&root)),
                    None => leaf.insert(model.len(), &key, value.as_slice()),
                };
                if fit.unwrap() == Fit::Full {
                    break;
                }
                let bytes = if nested { root.to_vec() } else { value };
                model.push((key, ovf, nested, bytes));
            }
            validate(&page, &g).unwrap();
            for (i, (key, ovf, nested, value)) in model.iter().enumerate() {
                let klen = key.len();
                let want = match (ovf, nested) {
                    (Some(ovf), _) => Some(Val::Overflow(*ovf)),
                    (None, true) => Some(Val::Nested(value.as_slice().try_into().unwrap())),
                    (None, false) => None,
                };
                for t in [0, klen, klen + 1 + tsel % 50, tsel % (klen + 1)] {
                    let (got_key, tail) = leaf_entry_with_key_tail(&page, i, t).unwrap();
                    prop_assert_eq!(got_key, key.as_slice());
                    match (tail, want) {
                        (Val::Inline(joined), None) => {
                            let want = [&key[klen - t.min(klen)..], value.as_slice()].concat();
                            prop_assert_eq!(joined, want.as_slice(), "cell {}, t {}", i, t);
                        }
                        (tail, want) => prop_assert_eq!(Some(tail), want, "cell {}, t {}", i, t),
                    }
                    assert_key_tail_matches(&page, i, t);
                }
            }
        }
    }
}
