//! Fixed-size 4 KiB page layout and byte-level codec for the on-disk B+tree index.
//!
//! Every page (header, internal node, leaf node) is exactly [`PAGE_SIZE`] bytes and ends with a
//! 4-byte CRC32 over the preceding bytes (see [`crate::archive::crc`]).  Keys are a fixed `ksize`
//! bytes (chosen at index creation, recorded in the header) and values are fixed 8-byte
//! little-endian `u64` file offsets, so every array on a page has a fixed stride and can be
//! binary-searched in place.
//!
//! Page byte layout (`ksize` = key length, `V` = 8):
//!
//! ```text
//! common tag:  page_type u8 | flags u8 | entry_count u16
//! internal:    tag | children[(max_internal_keys+1) u32] | keys[max_internal_keys * ksize] | .. | crc u32
//! leaf:        tag | prev u32 | next u32 | keys[max_leaf_keys * ksize] | values[max_leaf_keys * V] | .. | crc u32
//! ```

use std::cmp::Ordering;

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

// Fixed (key-size-independent) byte offsets within a page.
const INTERNAL_CHILDREN_OFF: usize = TAG;
const LEAF_PREV_OFF: usize = TAG;
const LEAF_NEXT_OFF: usize = TAG + 4;
const LEAF_KEYS_OFF: usize = TAG + LINKS;

// ---- little-endian scalar helpers (pure byte codec, key-size independent) ----

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

/// Page layout and byte-level codec for a B+tree holding fixed `ksize`-byte keys.
///
/// A small value carrying the runtime key length and the page geometry derived from it (built once
/// by [`Node::new`]); every method operates directly on a [`PAGE_SIZE`]-byte page buffer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Node {
    ksize: usize,
    max_internal_keys: usize,
    max_leaf_keys: usize,
    internal_keys_off: usize,
    leaf_values_off: usize,
}

impl Node {
    /// Build the page geometry for a `ksize`-byte key.  Validate feasibility with
    /// [`Node::geometry_ok`] before use.
    pub(crate) fn new(ksize: usize) -> Self {
        let max_internal_keys = (PAGE_SIZE - TAG - CRC - 4) / (ksize + 4);
        let max_leaf_keys = (PAGE_SIZE - TAG - LINKS - CRC) / (ksize + VALUE_SIZE);
        Self {
            ksize,
            max_internal_keys,
            max_leaf_keys,
            internal_keys_off: TAG + (max_internal_keys + 1) * 4,
            leaf_values_off: TAG + LINKS + max_leaf_keys * ksize,
        }
    }

    /// The key length in bytes.
    pub(crate) fn ksize(&self) -> usize {
        self.ksize
    }

    /// Max separator keys in an internal node (it also holds `max_internal_keys() + 1` children).
    pub(crate) fn max_internal_keys(&self) -> usize {
        self.max_internal_keys
    }

    /// Max key/value pairs in a leaf node.
    pub(crate) fn max_leaf_keys(&self) -> usize {
        self.max_leaf_keys
    }

    /// Feasibility guard (was the compile-time `GEOMETRY_OK` assert on the const generic): a node
    /// must hold at least two keys for splits to make progress, so a `ksize` too large for a page
    /// fails here.  Checked at index open/create.
    pub(crate) fn geometry_ok(&self) -> bool {
        self.ksize >= 1 && self.max_internal_keys >= 2 && self.max_leaf_keys >= 2
    }

    // ---- common tag ----

    /// True if the page is a leaf.
    pub(crate) fn is_leaf(&self, buf: &[u8]) -> bool {
        buf[0] == PAGE_TYPE_LEAF
    }

    /// Number of live entries (keys) on the page.
    pub(crate) fn entry_count(&self, buf: &[u8]) -> usize {
        u16::from_le_bytes([buf[2], buf[3]]) as usize
    }

    fn set_entry_count(&self, buf: &mut [u8], n: usize) {
        buf[2..4].copy_from_slice(&(n as u16).to_le_bytes());
    }

    // ---- leaf accessors ----

    /// Initialize `buf` as an empty leaf with the given sibling links.
    pub(crate) fn init_leaf(&self, buf: &mut [u8], prev: u32, next: u32) {
        buf.fill(0);
        buf[0] = PAGE_TYPE_LEAF;
        self.set_entry_count(buf, 0);
        write_u32(buf, LEAF_PREV_OFF, prev);
        write_u32(buf, LEAF_NEXT_OFF, next);
    }

    /// Previous-leaf page pointer (or [`NULL_PAGE`]).
    pub(crate) fn leaf_prev(&self, buf: &[u8]) -> u32 {
        read_u32(buf, LEAF_PREV_OFF)
    }

    /// Next-leaf page pointer (or [`NULL_PAGE`]).
    pub(crate) fn leaf_next(&self, buf: &[u8]) -> u32 {
        read_u32(buf, LEAF_NEXT_OFF)
    }

    /// Set the previous-leaf page pointer.
    pub(crate) fn set_leaf_prev(&self, buf: &mut [u8], p: u32) {
        write_u32(buf, LEAF_PREV_OFF, p);
    }

    /// Set the next-leaf page pointer.
    pub(crate) fn set_leaf_next(&self, buf: &mut [u8], p: u32) {
        write_u32(buf, LEAF_NEXT_OFF, p);
    }

    /// The key at leaf slot `i` (a `ksize`-byte slice).
    pub(crate) fn leaf_key<'a>(&self, buf: &'a [u8], i: usize) -> &'a [u8] {
        let off = LEAF_KEYS_OFF + i * self.ksize;
        &buf[off..off + self.ksize]
    }

    fn set_leaf_key(&self, buf: &mut [u8], i: usize, key: &[u8]) {
        let off = LEAF_KEYS_OFF + i * self.ksize;
        buf[off..off + self.ksize].copy_from_slice(key);
    }

    /// The value at leaf slot `i`.
    pub(crate) fn leaf_value(&self, buf: &[u8], i: usize) -> u64 {
        read_u64(buf, self.leaf_values_off + i * VALUE_SIZE)
    }

    /// Overwrite the value at an existing leaf slot `i` (used for duplicate-key updates).
    pub(crate) fn set_leaf_value(&self, buf: &mut [u8], i: usize, v: u64) {
        write_u64(buf, self.leaf_values_off + i * VALUE_SIZE, v);
    }

    /// Binary-search a leaf for `key`.  `Ok(i)` if present at slot `i`; `Err(i)` is the sorted
    /// insertion point otherwise.
    pub(crate) fn leaf_search(&self, buf: &[u8], key: &[u8]) -> Result<usize, usize> {
        let n = self.entry_count(buf);
        let mut lo = 0;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = LEAF_KEYS_OFF + mid * self.ksize;
            match buf[off..off + self.ksize].cmp(key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Insert `(key, val)` at sorted slot `at` in a leaf that has room (caller must ensure
    /// `entry_count < max_leaf_keys()`).
    pub(crate) fn leaf_insert(&self, buf: &mut [u8], at: usize, key: &[u8], val: u64) {
        let n = self.entry_count(buf);
        let z = self.ksize;
        let ks = LEAF_KEYS_OFF;
        buf.copy_within(ks + at * z..ks + n * z, ks + (at + 1) * z);
        let vs = self.leaf_values_off;
        buf.copy_within(vs + at * VALUE_SIZE..vs + n * VALUE_SIZE, vs + (at + 1) * VALUE_SIZE);
        self.set_leaf_key(buf, at, key);
        self.set_leaf_value(buf, at, val);
        self.set_entry_count(buf, n + 1);
    }

    /// Remove the entry at leaf slot `i` by shifting the tail one position left.
    pub(crate) fn leaf_delete(&self, buf: &mut [u8], i: usize) {
        let n = self.entry_count(buf);
        let z = self.ksize;
        let ks = LEAF_KEYS_OFF;
        buf.copy_within(ks + (i + 1) * z..ks + n * z, ks + i * z);
        let vs = self.leaf_values_off;
        buf.copy_within(vs + (i + 1) * VALUE_SIZE..vs + n * VALUE_SIZE, vs + i * VALUE_SIZE);
        self.set_entry_count(buf, n - 1);
    }

    /// Split a full leaf into `left` (rewritten in place) and `right` (fully written, minus the
    /// prev/next links which the caller fixes), inserting `(key, val)` at sorted slot `at`.
    /// Returns the separator key = right leaf's first key.  `right` may be any scratch buffer.
    pub(crate) fn leaf_split(
        &self,
        left: &mut [u8],
        right: &mut [u8],
        at: usize,
        key: &[u8],
        val: u64,
    ) -> Vec<u8> {
        let n = self.entry_count(left);
        let total = n + 1;
        let z = self.ksize;
        // Materialize all keys with the new key spliced in at `at` into one flat scratch buffer
        // (single allocation); values likewise into one `Vec`.
        let mut keys = vec![0_u8; total * z];
        for i in 0..at {
            keys[i * z..(i + 1) * z].copy_from_slice(self.leaf_key(left, i));
        }
        keys[at * z..(at + 1) * z].copy_from_slice(key);
        for i in at..n {
            keys[(i + 1) * z..(i + 2) * z].copy_from_slice(self.leaf_key(left, i));
        }
        let mut vals: Vec<u64> = Vec::with_capacity(total);
        for i in 0..n {
            vals.push(self.leaf_value(left, i));
        }
        vals.insert(at, val);

        let left_count = total.div_ceil(2);
        self.set_entry_count(left, left_count);
        for i in 0..left_count {
            self.set_leaf_key(left, i, &keys[i * z..(i + 1) * z]);
            self.set_leaf_value(left, i, vals[i]);
        }

        let right_count = total - left_count;
        right[0] = PAGE_TYPE_LEAF;
        right[1] = 0;
        self.set_entry_count(right, right_count);
        for i in 0..right_count {
            let src = left_count + i;
            self.set_leaf_key(right, i, &keys[src * z..(src + 1) * z]);
            self.set_leaf_value(right, i, vals[src]);
        }
        keys[left_count * z..(left_count + 1) * z].to_vec()
    }

    // ---- internal accessors ----

    /// Initialize `buf` as an internal node with a single child pointer `left_child` and no keys.
    pub(crate) fn init_internal(&self, buf: &mut [u8], left_child: u32) {
        buf.fill(0);
        buf[0] = PAGE_TYPE_INTERNAL;
        self.set_entry_count(buf, 0);
        write_u32(buf, INTERNAL_CHILDREN_OFF, left_child);
    }

    /// The child page pointer at child slot `i` (there are `entry_count + 1` children).
    pub(crate) fn internal_child(&self, buf: &[u8], i: usize) -> u32 {
        read_u32(buf, INTERNAL_CHILDREN_OFF + i * 4)
    }

    fn set_internal_child(&self, buf: &mut [u8], i: usize, v: u32) {
        write_u32(buf, INTERNAL_CHILDREN_OFF + i * 4, v);
    }

    fn internal_key<'a>(&self, buf: &'a [u8], i: usize) -> &'a [u8] {
        let off = self.internal_keys_off + i * self.ksize;
        &buf[off..off + self.ksize]
    }

    fn set_internal_key(&self, buf: &mut [u8], i: usize, key: &[u8]) {
        let off = self.internal_keys_off + i * self.ksize;
        buf[off..off + self.ksize].copy_from_slice(key);
    }

    /// Index of the child to descend into for `key` (the count of separators `<= key`).
    pub(crate) fn internal_child_index(&self, buf: &[u8], key: &[u8]) -> usize {
        let n = self.entry_count(buf);
        let mut lo = 0;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let off = self.internal_keys_off + mid * self.ksize;
            match buf[off..off + self.ksize].cmp(key) {
                Ordering::Greater => hi = mid,
                // separator <= key: descend to the right of it
                Ordering::Less | Ordering::Equal => lo = mid + 1,
            }
        }
        lo
    }

    /// Insert separator `sep` at key slot `at` and right child `rc` at child slot `at + 1` in an
    /// internal node that has room (caller must ensure `entry_count < max_internal_keys()`).
    pub(crate) fn internal_insert(&self, buf: &mut [u8], at: usize, sep: &[u8], rc: u32) {
        let n = self.entry_count(buf);
        let z = self.ksize;
        let ks = self.internal_keys_off;
        buf.copy_within(ks + at * z..ks + n * z, ks + (at + 1) * z);
        let cs = INTERNAL_CHILDREN_OFF;
        buf.copy_within(cs + (at + 1) * 4..cs + (n + 1) * 4, cs + (at + 2) * 4);
        self.set_internal_key(buf, at, sep);
        self.set_internal_child(buf, at + 1, rc);
        self.set_entry_count(buf, n + 1);
    }

    /// Split a full internal node into `left` (rewritten in place) and `right` (fully written),
    /// inserting separator `sep`/right-child `rc` at key slot `at`.  Returns the median key that
    /// the caller lifts into the parent.  `right` may be any scratch buffer.
    pub(crate) fn internal_split(
        &self,
        left: &mut [u8],
        right: &mut [u8],
        at: usize,
        sep: &[u8],
        rc: u32,
    ) -> Vec<u8> {
        let n = self.entry_count(left);
        let z = self.ksize;
        // Materialize all keys with `sep` spliced in at `at` into one flat scratch buffer, and the
        // children (n + 2 of them) into one `Vec` with `rc` inserted at `at + 1`.
        let total = n + 1; // keys; children = total + 1
        let mut keys = vec![0_u8; total * z];
        for i in 0..at {
            keys[i * z..(i + 1) * z].copy_from_slice(self.internal_key(left, i));
        }
        keys[at * z..(at + 1) * z].copy_from_slice(sep);
        for i in at..n {
            keys[(i + 1) * z..(i + 2) * z].copy_from_slice(self.internal_key(left, i));
        }
        let mut kids: Vec<u32> = Vec::with_capacity(n + 2);
        for i in 0..=n {
            kids.push(self.internal_child(left, i));
        }
        kids.insert(at + 1, rc);

        let mid = total / 2;
        let median = keys[mid * z..(mid + 1) * z].to_vec();

        // Left keeps keys[0..mid] and children[0..=mid], rewritten in place.
        self.set_entry_count(left, mid);
        for i in 0..mid {
            self.set_internal_key(left, i, &keys[i * z..(i + 1) * z]);
        }
        for (i, c) in kids.iter().take(mid + 1).enumerate() {
            self.set_internal_child(left, i, *c);
        }

        // Right takes keys[mid+1..] and children[mid+1..].
        let right_keys = total - mid - 1;
        right[0] = PAGE_TYPE_INTERNAL;
        right[1] = 0;
        self.set_entry_count(right, right_keys);
        for i in 0..right_keys {
            let src = mid + 1 + i;
            self.set_internal_key(right, i, &keys[src * z..(src + 1) * z]);
        }
        for i in 0..=right_keys {
            self.set_internal_child(right, i, kids[mid + 1 + i]);
        }
        median
    }
}
