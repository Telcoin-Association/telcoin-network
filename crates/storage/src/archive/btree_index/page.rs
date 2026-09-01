//! Fixed-size 4 KiB page layout and byte-level codec for the on-disk B+tree index.
//!
//! Every page (header, internal node, leaf node) is exactly [`PAGE_SIZE`] bytes and ends with a
//! 4-byte CRC32 over the preceding bytes (see [`crate::archive::crc`]).  Keys are fixed `KSIZE`
//! bytes and values are fixed 8-byte little-endian `u64` file offsets, so every array on a page
//! has a fixed stride and can be binary-searched in place.
//!
//! Page byte layout (`KSIZE` = key length, `V` = 8):
//!
//! ```text
//! common tag:  page_type u8 | flags u8 | entry_count u16
//! internal:    tag | children[(MAX_INTERNAL_KEYS+1) u32] | keys[MAX_INTERNAL_KEYS * KSIZE] | .. | crc u32
//! leaf:        tag | prev u32 | next u32 | keys[MAX_LEAF_KEYS * KSIZE] | values[MAX_LEAF_KEYS * V] | .. | crc u32
//! ```

use std::cmp::Ordering;

use crate::archive::crc::{add_crc32, check_crc};

/// Size of every page in the index file, in bytes.
pub(crate) const PAGE_SIZE: usize = 4096;

/// Sentinel page number meaning "no page" (e.g. a leaf with no sibling, or an empty header slot).
pub(crate) const NULL_PAGE: u32 = u32::MAX;

/// Page type tag for an internal (branch) node.
pub(crate) const PAGE_TYPE_INTERNAL: u8 = 1;
/// Page type tag for a leaf node.
pub(crate) const PAGE_TYPE_LEAF: u8 = 2;

/// Common page tag: `page_type(1) + flags(1) + entry_count(2)`.
const TAG: usize = 4;
/// Trailing CRC32 size.
const CRC: usize = 4;
/// Leaf sibling links: `prev(4) + next(4)`.
const LINKS: usize = 8;
/// On-disk value size (a `u64` file offset).
const VALUE_SIZE: usize = 8;

/// Page layout and byte-level codec for a B+tree holding fixed `KSIZE`-byte keys.
///
/// Zero-sized helper; every method operates directly on a [`PAGE_SIZE`]-byte page buffer.
pub(crate) struct Node<const KSIZE: usize>;

impl<const KSIZE: usize> Node<KSIZE> {
    /// Max separator keys in an internal node (it also holds `MAX_INTERNAL_KEYS + 1` children).
    pub(crate) const MAX_INTERNAL_KEYS: usize = (PAGE_SIZE - TAG - CRC - 4) / (KSIZE + 4);
    /// Max key/value pairs in a leaf node.
    pub(crate) const MAX_LEAF_KEYS: usize = (PAGE_SIZE - TAG - LINKS - CRC) / (KSIZE + VALUE_SIZE);

    /// Compile-time feasibility guard: a node must hold at least two keys for splits to make
    /// progress.  Referenced from `BtreeIndex::open_btx_file` so that a `KSIZE` too large for a
    /// page fails to compile (post-monomorphization) rather than misbehaving at runtime.
    pub(crate) const GEOMETRY_OK: () = assert!(
        KSIZE >= 1 && Self::MAX_INTERNAL_KEYS >= 2 && Self::MAX_LEAF_KEYS >= 2,
        "KSIZE is too large for PAGE_SIZE: a node must hold at least 2 keys",
    );

    const INTERNAL_CHILDREN_OFF: usize = TAG;
    const INTERNAL_KEYS_OFF: usize = TAG + (Self::MAX_INTERNAL_KEYS + 1) * 4;
    const LEAF_PREV_OFF: usize = TAG;
    const LEAF_NEXT_OFF: usize = TAG + 4;
    const LEAF_KEYS_OFF: usize = TAG + LINKS;
    const LEAF_VALUES_OFF: usize = TAG + LINKS + Self::MAX_LEAF_KEYS * KSIZE;

    // ---- common tag ----

    /// True if the page is a leaf.
    pub(crate) fn is_leaf(buf: &[u8]) -> bool {
        buf[0] == PAGE_TYPE_LEAF
    }

    /// Number of live entries (keys) on the page.
    pub(crate) fn entry_count(buf: &[u8]) -> usize {
        u16::from_le_bytes([buf[2], buf[3]]) as usize
    }

    fn set_entry_count(buf: &mut [u8], n: usize) {
        buf[2..4].copy_from_slice(&(n as u16).to_le_bytes());
    }

    /// Stamp the CRC32 over the page (overwrites the last 4 bytes).  Call before writing to disk.
    pub(crate) fn finalize(buf: &mut [u8]) {
        add_crc32(buf);
    }

    /// Verify a page's trailing CRC32 after reading from disk.
    pub(crate) fn verify(buf: &[u8]) -> bool {
        check_crc(buf)
    }

    // ---- little-endian scalar helpers ----

    fn read_u32(buf: &[u8], off: usize) -> u32 {
        u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
    }

    fn write_u32(buf: &mut [u8], off: usize, v: u32) {
        buf[off..off + 4].copy_from_slice(&v.to_le_bytes());
    }

    fn read_u64(buf: &[u8], off: usize) -> u64 {
        u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
    }

    fn write_u64(buf: &mut [u8], off: usize, v: u64) {
        buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }

    // ---- leaf accessors ----

    /// Initialize `buf` as an empty leaf with the given sibling links.
    pub(crate) fn init_leaf(buf: &mut [u8], prev: u32, next: u32) {
        buf.fill(0);
        buf[0] = PAGE_TYPE_LEAF;
        Self::set_entry_count(buf, 0);
        Self::write_u32(buf, Self::LEAF_PREV_OFF, prev);
        Self::write_u32(buf, Self::LEAF_NEXT_OFF, next);
    }

    /// Previous-leaf page pointer (or [`NULL_PAGE`]).
    pub(crate) fn leaf_prev(buf: &[u8]) -> u32 {
        Self::read_u32(buf, Self::LEAF_PREV_OFF)
    }

    /// Next-leaf page pointer (or [`NULL_PAGE`]).
    pub(crate) fn leaf_next(buf: &[u8]) -> u32 {
        Self::read_u32(buf, Self::LEAF_NEXT_OFF)
    }

    /// Set the previous-leaf page pointer.
    pub(crate) fn set_leaf_prev(buf: &mut [u8], p: u32) {
        Self::write_u32(buf, Self::LEAF_PREV_OFF, p);
    }

    /// Set the next-leaf page pointer.
    pub(crate) fn set_leaf_next(buf: &mut [u8], p: u32) {
        Self::write_u32(buf, Self::LEAF_NEXT_OFF, p);
    }

    /// The key at leaf slot `i` (a `KSIZE`-byte slice).
    pub(crate) fn leaf_key(buf: &[u8], i: usize) -> &[u8] {
        let off = Self::LEAF_KEYS_OFF + i * KSIZE;
        &buf[off..off + KSIZE]
    }

    fn set_leaf_key(buf: &mut [u8], i: usize, key: &[u8]) {
        let off = Self::LEAF_KEYS_OFF + i * KSIZE;
        buf[off..off + KSIZE].copy_from_slice(key);
    }

    /// The value at leaf slot `i`.
    pub(crate) fn leaf_value(buf: &[u8], i: usize) -> u64 {
        Self::read_u64(buf, Self::LEAF_VALUES_OFF + i * VALUE_SIZE)
    }

    /// Overwrite the value at an existing leaf slot `i` (used for duplicate-key updates).
    pub(crate) fn set_leaf_value(buf: &mut [u8], i: usize, v: u64) {
        Self::write_u64(buf, Self::LEAF_VALUES_OFF + i * VALUE_SIZE, v);
    }

    /// Binary-search a leaf for `key`.  `Ok(i)` if present at slot `i`; `Err(i)` is the sorted
    /// insertion point otherwise.
    pub(crate) fn leaf_search(buf: &[u8], key: &[u8]) -> Result<usize, usize> {
        let n = Self::entry_count(buf);
        let mut lo = 0;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = Self::LEAF_KEYS_OFF + mid * KSIZE;
            match buf[off..off + KSIZE].cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Insert `(key, val)` at sorted slot `at` in a leaf that has room (caller must ensure
    /// `entry_count < MAX_LEAF_KEYS`).
    pub(crate) fn leaf_insert(buf: &mut [u8], at: usize, key: &[u8], val: u64) {
        let n = Self::entry_count(buf);
        let ks = Self::LEAF_KEYS_OFF;
        buf.copy_within(ks + at * KSIZE..ks + n * KSIZE, ks + (at + 1) * KSIZE);
        let vs = Self::LEAF_VALUES_OFF;
        buf.copy_within(vs + at * VALUE_SIZE..vs + n * VALUE_SIZE, vs + (at + 1) * VALUE_SIZE);
        Self::set_leaf_key(buf, at, key);
        Self::set_leaf_value(buf, at, val);
        Self::set_entry_count(buf, n + 1);
    }

    /// Split a full leaf into `left` (rewritten in place) and `right` (fully written, minus the
    /// prev/next links which the caller fixes), inserting `(key, val)` at sorted slot `at`.
    /// Returns the separator key = right leaf's first key.  `right` may be any scratch buffer.
    pub(crate) fn leaf_split(
        left: &mut [u8],
        right: &mut [u8],
        at: usize,
        key: &[u8],
        val: u64,
    ) -> [u8; KSIZE] {
        let n = Self::entry_count(left);
        let total = n + 1;
        let mut keys: Vec<[u8; KSIZE]> = Vec::with_capacity(total);
        let mut vals: Vec<u64> = Vec::with_capacity(total);
        for i in 0..n {
            let mut k = [0u8; KSIZE];
            k.copy_from_slice(Self::leaf_key(left, i));
            keys.push(k);
            vals.push(Self::leaf_value(left, i));
        }
        let mut nk = [0u8; KSIZE];
        nk.copy_from_slice(key);
        keys.insert(at, nk);
        vals.insert(at, val);

        let left_count = total.div_ceil(2);
        Self::set_entry_count(left, left_count);
        for i in 0..left_count {
            Self::set_leaf_key(left, i, &keys[i]);
            Self::set_leaf_value(left, i, vals[i]);
        }

        let right_count = total - left_count;
        right[0] = PAGE_TYPE_LEAF;
        right[1] = 0;
        Self::set_entry_count(right, right_count);
        for i in 0..right_count {
            Self::set_leaf_key(right, i, &keys[left_count + i]);
            Self::set_leaf_value(right, i, vals[left_count + i]);
        }
        keys[left_count]
    }

    // ---- internal accessors ----

    /// Initialize `buf` as an internal node with a single child pointer `left_child` and no keys.
    pub(crate) fn init_internal(buf: &mut [u8], left_child: u32) {
        buf.fill(0);
        buf[0] = PAGE_TYPE_INTERNAL;
        Self::set_entry_count(buf, 0);
        Self::write_u32(buf, Self::INTERNAL_CHILDREN_OFF, left_child);
    }

    /// The child page pointer at child slot `i` (there are `entry_count + 1` children).
    pub(crate) fn internal_child(buf: &[u8], i: usize) -> u32 {
        Self::read_u32(buf, Self::INTERNAL_CHILDREN_OFF + i * 4)
    }

    fn set_internal_child(buf: &mut [u8], i: usize, v: u32) {
        Self::write_u32(buf, Self::INTERNAL_CHILDREN_OFF + i * 4, v);
    }

    fn internal_key(buf: &[u8], i: usize) -> &[u8] {
        let off = Self::INTERNAL_KEYS_OFF + i * KSIZE;
        &buf[off..off + KSIZE]
    }

    fn set_internal_key(buf: &mut [u8], i: usize, key: &[u8]) {
        let off = Self::INTERNAL_KEYS_OFF + i * KSIZE;
        buf[off..off + KSIZE].copy_from_slice(key);
    }

    /// Index of the child to descend into for `key` (the count of separators `<= key`).
    pub(crate) fn internal_child_index(buf: &[u8], key: &[u8]) -> usize {
        let n = Self::entry_count(buf);
        let mut lo = 0;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = Self::INTERNAL_KEYS_OFF + mid * KSIZE;
            match buf[off..off + KSIZE].cmp(key) {
                Ordering::Greater => hi = mid,
                // separator <= key: descend to the right of it
                Ordering::Less | Ordering::Equal => lo = mid + 1,
            }
        }
        lo
    }

    /// Insert separator `sep` at key slot `at` and right child `rc` at child slot `at + 1` in an
    /// internal node that has room (caller must ensure `entry_count < MAX_INTERNAL_KEYS`).
    pub(crate) fn internal_insert(buf: &mut [u8], at: usize, sep: &[u8], rc: u32) {
        let n = Self::entry_count(buf);
        let ks = Self::INTERNAL_KEYS_OFF;
        buf.copy_within(ks + at * KSIZE..ks + n * KSIZE, ks + (at + 1) * KSIZE);
        let cs = Self::INTERNAL_CHILDREN_OFF;
        buf.copy_within(cs + (at + 1) * 4..cs + (n + 1) * 4, cs + (at + 2) * 4);
        Self::set_internal_key(buf, at, sep);
        Self::set_internal_child(buf, at + 1, rc);
        Self::set_entry_count(buf, n + 1);
    }

    /// Split a full internal node into `left` (rewritten in place) and `right` (fully written),
    /// inserting separator `sep`/right-child `rc` at key slot `at`.  Returns the median key that
    /// the caller lifts into the parent.  `right` may be any scratch buffer.
    pub(crate) fn internal_split(
        left: &mut [u8],
        right: &mut [u8],
        at: usize,
        sep: &[u8],
        rc: u32,
    ) -> [u8; KSIZE] {
        let n = Self::entry_count(left);
        let mut keys: Vec<[u8; KSIZE]> = Vec::with_capacity(n + 1);
        let mut kids: Vec<u32> = Vec::with_capacity(n + 2);
        for i in 0..n {
            let mut k = [0u8; KSIZE];
            k.copy_from_slice(Self::internal_key(left, i));
            keys.push(k);
        }
        for i in 0..=n {
            kids.push(Self::internal_child(left, i));
        }
        let mut sk = [0u8; KSIZE];
        sk.copy_from_slice(sep);
        keys.insert(at, sk);
        kids.insert(at + 1, rc);

        let total = keys.len(); // n + 1 keys, n + 2 children
        let mid = total / 2;
        let median = keys[mid];

        // Left keeps keys[0..mid] and children[0..=mid], rewritten in place.
        Self::set_entry_count(left, mid);
        for (i, k) in keys.iter().take(mid).enumerate() {
            Self::set_internal_key(left, i, k);
        }
        for (i, c) in kids.iter().take(mid + 1).enumerate() {
            Self::set_internal_child(left, i, *c);
        }

        // Right takes keys[mid+1..] and children[mid+1..].
        let right_keys = total - mid - 1;
        right[0] = PAGE_TYPE_INTERNAL;
        right[1] = 0;
        Self::set_entry_count(right, right_keys);
        for i in 0..right_keys {
            Self::set_internal_key(right, i, &keys[mid + 1 + i]);
        }
        for i in 0..=right_keys {
            Self::set_internal_child(right, i, kids[mid + 1 + i]);
        }
        median
    }
}
