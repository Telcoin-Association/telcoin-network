# `archive::btree_index` — the on-disk B+tree index

A durable, paged, memory-mapped **B+tree** that maps fixed-size byte keys to `u64` file positions
(byte offsets of records in a pack file). It supports sorted point lookup plus forward, reverse,
bounded-range and prefix iteration. It complements the hash-based
[`digest_index`](../digest_index) (`HdxIndex`), which offers point lookups only.

The tree is **copy-on-write**: a page that has been published to readers is never modified, so
published snapshots are read with **no lock** from any thread while a single writer keeps working.

Like every index in `archive/`, the B+tree is **derived data**: it is never the source of truth.
Its owner (today, [`tndb`](../../tndb/README.md)) rebuilds it from the owning data log whenever it
cannot be trusted (see [Crash consistency](#crash-consistency-and-the-owners-contract)).

| file | contents |
|---|---|
| `mod.rs` | module docs and re-exports (`BtreeIndex`, `PageCrcReport`, `BtreeIter`) |
| `index.rs` | `BtreeIndex`: open/create, point ops, copy-on-write, snapshots, page reuse, sync, rebuild |
| `page.rs` | the 4 KiB page layout and the byte-level node codec (`Node`) |
| `header.rs` | the page-0 header (`BtreeHeader`) |
| `iter.rs` | sorted iteration: the detached `BtreeCursor`, `BtreeIter`, and the `PageSource` trait |

Users: `tndb::table` (each tndb table's index), the `pack-btree` column of
`pack_kv_bench::pack_vs_mdbx_bench`, and `archive::index_bench`.

---

## Contents

1. [Concepts at a glance](#concepts-at-a-glance)
2. [File format](#file-format)
3. [Tree operations](#tree-operations)
4. [Copy-on-write, snapshots and page reuse](#copy-on-write-snapshots-and-page-reuse)
5. [Iteration](#iteration)
6. [CRC regime and integrity checks](#crc-regime-and-integrity-checks)
7. [Durability](#durability)
8. [Opening an index](#opening-an-index)
9. [Crash consistency and the owner's contract](#crash-consistency-and-the-owners-contract)
10. [Concurrency rules](#concurrency-rules)
11. [API reference](#api-reference)
12. [Errors](#errors)
13. [Limits and non-goals](#limits-and-non-goals)
14. [Tests and benchmarks](#tests-and-benchmarks)

---

## Concepts at a glance

- **One file**, `<dir>/index.btx`, of fixed **4 KiB pages**. Page 0 is the header; every other page
  is a leaf or an internal node. Pages are addressed by `u32` page number.
- **Fixed-size keys.** The key length `ksize` (1 to 2032 bytes) is chosen when the index is created
  and recorded in the header. Keys compare lexicographically as byte strings, so a caller that wants
  numeric order must encode big-endian (tndb's `encode_key` does).
- **Fixed-size values:** a little-endian `u64`, by convention a record position in the data log.
- **Memory-mapped, no page cache of its own.** The file is mapped once (in a 1 GiB reserved range,
  so the mapping does not move as it grows) and worked on in place; the OS page cache is the cache.
- **Copy-on-write.** The writer copies any published page before changing it. `publish()` makes the
  writer's tree visible as an immutable `IndexSnapshot`.
- **Lazy CRC.** Every page ends with a CRC32 trailer, but writes leave it **zeroed** (a "dirty"
  marker) and it is stamped once, at publish/sync, instead of on every write.
- **Derived, rebuildable.** It is synced only at an explicit `sync()` or a clean close. After a crash
  its owner rebuilds it from the data log (`rebuild_from`) and calls `mark_consistent()`.
- **No sibling links; empty pages unlinked, no merges.** Scans move between leaves through their
  parents. A removal that empties a leaf unlinks it (and any parent it empties, collapsing the root);
  under-full leaves are not merged.
- **Splits cannot fail half-way.** A split reserves every page it can need before changing anything,
  and ascending inserts use an append split that keeps pages full.

---

## File format

### Page geometry

Every page is exactly `PAGE_SIZE` = 4096 bytes and ends with a 4-byte CRC32 (little-endian) over
the preceding 4092 bytes. A page-type tag starts every node page.

```text
common tag (4 bytes):  page_type u8 | flags u8 | entry_count u16 (LE)

internal page:  tag | children[(max_internal_keys + 1) × u32] | keys[max_internal_keys × ksize] | … | crc u32
leaf page:      tag | prev u32 | next u32 | keys[max_leaf_keys × ksize] | values[max_leaf_keys × u64] | … | crc u32
```

- `page_type`: `1` = internal, `2` = leaf. `flags` is unused (0).
- Keys (and values) are fixed-stride arrays, binary-searched in place.
- A leaf's `prev`/`next` links keep their bytes in the layout but are always written `NULL_PAGE`
  (`u32::MAX`): a copy-on-write tree cannot keep sibling links current (copying a leaf would force
  copying its neighbours), so iteration walks through parents instead.
- An internal page with `n` keys has `n + 1` children; child `i` holds keys `k` with
  `sep[i-1] <= k < sep[i]` (the descent picks the count of separators `<= key`).

Capacity per page (`Node::new`):

```text
max_internal_keys = (4096 − 4 tag − 4 crc − 4) / (ksize + 4)
max_leaf_keys     = (4096 − 4 tag − 8 links − 4 crc) / (ksize + 8)
```

| `ksize` | example | keys per leaf | keys per internal (children) |
|---:|---|---:|---:|
| 8 | `u64` (big-endian) | 255 | 340 (341) |
| 32 | raw 32-byte digest (`B256` adapters) | 102 | 113 (114) |
| 36 | `(u32, B256)` encoded key | 92 | 102 (103) |
| 40 | a `B256` through `encode_key` (8-byte length prefix + 32) | 85 | 92 (93) |
| 2032 | largest accepted | 2 | 2 (3) |

`Node::geometry_ok` requires `ksize >= 1` and at least two keys in both page kinds (so splits make
progress); a larger `ksize` is refused at open with `InvalidIndexGeometry`. With 32-byte keys a
height-3 tree holds on the order of 10⁵–10⁶ keys depending on fill.

### Page 0: the header

| offset | size | field | meaning |
|---:|---:|---|---|
| 0 | 8 | `type_id` | `b"telcoinb"` |
| 8 | 2 | `version` | the paired data log's version (cross-checked on open) |
| 10 | 8 | `uid` | the paired data log's uid (cross-checked on open) |
| 18 | 4 | `appnum` | the paired data log's appnum (cross-checked on open) |
| 22 | 4 | `page_size` | 4096 (geometry check) |
| 26 | 2 | `ksize` | key length in bytes (geometry check) |
| 28 | 2 | `value_size` | 8 (geometry check) |
| 30 | 4 | `root_page` | page number of the root |
| 34 | 4 | `height` | tree height (1 = a single leaf) |
| 38 | 4 | `page_count` | pages in the tree's address space, including page 0 and free pages |
| 42 | 8 | `values` | number of keys |
| 50 | 4 | `first_leaf` | not maintained (layout only) |
| 54 | 4 | `last_leaf` | not maintained (layout only) |
| 58 | 8 | `data_file_length` | the data log length this index covers (set by the owner) |
| 66 | 8 | `owner_value` | any value the owner keeps with the index (tndb: dead puts); 0 if never set |
| 74 | … | — | zero |
| 4092 | 4 | CRC32 | over bytes `0..4092` (always a real CRC: the header is the commit marker) |

All integers are little-endian. A fresh index is `page_count = 2`: the header and one empty root
leaf on page 1.

### CRC trailers

- **Node pages** are stamped with `add_crc32_nonzero`: a computed CRC of 0 is written as a fixed
  non-zero sentinel, so an all-zero trailer always means "dirty" (`zero_crc`), never a real CRC.
  `crc_state` classifies a page as `Valid`, `Dirty` (all-zero trailer) or `Corrupt` (non-zero,
  mismatched).
- **The header** is stamped with a plain CRC (`add_crc32`) and checked with `check_crc` when parsed.

---

## Tree operations

All writes take `&mut self` (one writer). Each write first makes the root-to-leaf path writable
(copy-on-write, below), then changes private pages in place.

| operation | algorithm |
|---|---|
| **lookup** (`load`, `contains`) | Descend from the root: in each internal page binary-search the separators (`internal_child_index`), follow the child; binary-search the leaf. Bounded by `MAX_DEPTH` (48) descents. |
| **insert** (`save`) | `writable_path(key)`, then: duplicate key → overwrite the value in place (count unchanged); room in the leaf → shift and insert; full leaf → split. |
| **split reservation** | Before a split changes anything, `reserve_split_pages` allocates every page it can need: the right leaf, one per full internal page above it, and a new root if the root splits. If an allocation fails (e.g. a full disk), the pages go back to the free list and the tree is unchanged; with the reservation made, the split cannot fail part-way. |
| **leaf split** | The left keeps `ceil((n+1)/2)` entries, the right the rest; the separator is the right leaf's first key. The separator and right child go up into the parent (`insert_into_parent`). **Append split:** when the key goes past the end of the tree's rightmost leaf (ascending inserts, sorted rebuilds), the left keeps all its entries and the new key starts the right leaf, so pages fill instead of being left half empty. |
| **internal split** | When the parent is full: split it around the median (left keeps keys `[0, mid)`, right `[mid+1, …)`), lift the median, repeat upward; on the rightmost path (append) the split is just before the last key. A split of the root grows a new root (`height += 1`). |
| **remove** | Look the key up first, so a miss copies nothing; otherwise make the path writable and delete from the leaf. If that **empties the leaf**, it is unlinked: removed from its parent with the separator beside it (`Node::internal_remove_child`), cascading while a parent loses its only child (an emptied root becomes an empty leaf), then the root collapses while it has a single child (`height -= 1`). Unlinked pages are private copies, so they are free at once. Under-full leaves are **not merged**; the tree stays balanced in height. Without the unlinking, ascending keys with old ones removed (a sliding window) would leave a growing trail of empty leaves that every scan from the start walks. |
| **clear** | Copy-on-write: a fresh empty root leaf becomes the working root; the old tree's pages are retired (reused once no snapshot can reach them; immediately if never published). |
| **rebuild_from** | `reset_empty` (truncate the file to a fresh empty tree, synced; refused while any snapshot is alive), then insert every `(key, position)`. Feeding keys in sorted order gives a sequential build. |

Page allocation (`allocate_page`) reuses a free page if one is available (zero-filling it), and
otherwise grows the file by one page (`ensure_len`). Fresh and reused pages start all-zero, so their
zero CRC trailer marks them private-dirty.

---

## Copy-on-write, snapshots and page reuse

The writer keeps four sets of pages:

| set | meaning |
|---|---|
| `private` (`PageSet` bitset) | pages this handle created (allocated or copied) since the last publish: the **only** pages it modifies in place. No reader can see them. |
| `superseded` | published pages that were replaced by a copy since the last publish. Still reachable from the latest published state. |
| `retiring` (queue) | per published state, the pages it replaced, waiting until no snapshot of that state (or any older one) is alive. |
| `free` | pages nothing can reach: reused before the file grows. |

**`make_writable(p)`**: if `p` is private it is returned as is; otherwise `p` is copied to a newly
allocated page (CRC zeroed), `p` goes to `superseded`, and the copy is returned. `writable_path`
does this top-down from the root, re-pointing each parent at its child's copy, so a write changes a
fresh root-to-leaf path while every published page stays untouched.

**`publish()`** (crate-internal) makes the working tree visible:

1. CRC-stamp every private page (they become immutable: later writes copy them).
2. Extend the readers' `MapView` over all pages (`publish_len(page_count × 4096)`).
3. Move `superseded` into `retiring`, tagged with the pin of the previous published state.
4. Return an `IndexSnapshot` (root, page count, key count, the shared `MapView`) holding a new
   `Arc<SnapshotPin>`; the index keeps only a `Weak` to it.

**Reuse.** `reclaim` pops retiring states **from the front only** while no snapshot of them is
alive (`Weak::strong_count() == 0`); a state's replaced pages may be reachable from any older state
too, and an older entry is popped only once its own snapshots are gone. After freeing, an
`Acquire` fence pairs with the `Release` of each snapshot's final `Arc` drop, so a reader's last
reads of a page happen before the writer reuses it. A long-lived snapshot therefore lets the file
grow until it is dropped.

**Reopen.** The free list lives in memory. On a clean, writable reopen `unreachable_pages` walks
the tree from the root (checking each internal page's CRC, child bounds and that no page is reached
twice) and frees every page it does not reach. Any walk failure frees nothing: a page that might be
live is never reused, and the damage surfaces on the lookups that reach it.

---

## Iteration

A scan's state is a **`BtreeCursor`**: the path from the root to the current leaf as a fixed stack
of `(page, slot)` entries (at most `MAX_DEPTH` = 48), **detached from the tree**. Each step takes a
`PageSource` (`node()`, `root()`, `page(p)`), so the same cursor runs over:

- the writer's working tree (`BtreeIndex: PageSource`, including unpublished writes), or
- a published `IndexSnapshot` (lock-free; used by tndb's `TableScan`).

Because leaves are not linked, moving to the next leaf (`next_leaf`) walks up to the nearest
ancestor with another child in scan order and descends its leftmost (forward) or rightmost
(reverse) edge.

Bounds (`std::ops::Bound` over key bytes): a forward scan starts at the first key `>=` (`Included`)
or `>` (`Excluded`) the lower bound and stops past the upper; a reverse scan starts at the last key
`<=`/`<` the upper bound and stops before the lower. A fetch or CRC failure ends the scan with a
terminal `Err`.

Public iterators (`BtreeIter`, yielding `Result<(Vec<u8>, u64), FetchError>` over the writer's tree):

| method | order and range |
|---|---|
| `iter()` / `rev_iter()` | every entry, ascending / descending |
| `range(bounds)` / `rev_range(bounds)` | entries within any `RangeBounds<T: AsRef<[u8]>>` (a bare `..` needs the element type spelled out, e.g. `range::<[u8; 32], _>(..)`) |
| `prefix(p)` | ascending over keys starting with `p` (truncated to `ksize`): from `p` zero-padded, up to the next prefix (all-`0xFF` prefixes are unbounded above) |

---

## CRC regime and integrity checks

The B+tree does not pay a CRC per operation: it is derived and rebuildable, so it uses the same
**lazy CRC** regime as the digest index.

- **Writes** zero the page's CRC trailer (`zero_crc`) as a dirty marker and record the page as
  private.
- **Publish/sync** stamps exactly the private pages (O(pages written), never a full scan). A page
  zeroed *at rest* is never re-stamped as valid, because it is not in the private set.
- **Reads do not recompute the full CRC**, but two cheap checks turn damage into
  `FetchError::CorruptIndex` instead of a wrong answer:
  - the page number must be inside the tree (`1 ≤ p < page_count`), and
  - an all-zero trailer is only legal on a page this handle wrote since its last publish. On a
    published snapshot every page must be stamped.
- **Writes into a damaged page are refused** (`page_mut` runs the same checks), so a sync never
  stamps at-rest damage as valid.
- **`page_crc_scan()`** is the off-path full check: it classifies every data page as valid, dirty
  (unsynced writes: rebuild from the log) or corrupt (genuine on-disk corruption), in a
  `PageCrcReport { dirty, corrupt }`.
- **Open** checks the header CRC, and on a cleanly sealed file the root page's full CRC.

---

## Durability

`sync()` (also run by `Drop` when the index has unsynced changes) commits **pages first, header
last**:

1. `publish_pages()`: CRC-stamp the private pages.
2. `msync` the whole file (`MmapDataFile::sync_all`).
3. Rewrite page 0 from the in-memory header (root, height, page count, key count,
   `data_file_length`, `owner_value`), then `msync` just that page (`sync_range(0, 4096)`).

So a durable header never names pages that did not reach disk with it.

The index file is opened with `derived: true`: its barriers skip the deferred size `fsync`, so a
growth's new size is made durable only by the **clean-close seal** (or an explicit full sync of the
data file). After a crash the file may be shorter than its header claims; that is safe because an
index that was not sealed is never trusted (next section).

`set_data_file_length(len)` records, in memory, the data log length the index covers; it becomes
durable with the header at the next `sync()`. Owners compare it with the log's length on open to
detect an index that lags its log. `set_owner_value(v)` does the same for one `u64` the owner keeps
with the index (tndb keeps its dead-put count there); it reads 0 until first set.

`set_remove_on_drop()` deletes the file when the handle drops, skipping the sync (used to discard a
cleared or abandoned index).

---

## Opening an index

```rust
BtreeIndex::open_btx_file(dir, data_header: &DataHeader, ksize: u16, read_only: bool)
    -> Result<BtreeIndex, LoadHeaderError>
```

1. Build the page geometry for `ksize` and refuse an infeasible one (`InvalidIndexGeometry`) before
   touching the filesystem.
2. Create `dir` if writable and absent (fsyncing its parent). A read-only open never creates it.
3. Map `dir/index.btx` with `WriteMode::Random`, `MmapAccess::Random`, `derived: true` and a 1 GiB
   reservation (`BTX_MAP_RESERVE`).
4. **New (empty) file**: refused if read-only (`ReadOnlyEmpty`); otherwise write the header
   (identity from `data_header`) and an empty root leaf, `msync`, fsync the directory.
5. **Existing file**, checked in order:
   - the header CRC and type (`CrcFailed`, `InvalidType`);
   - `version`, `appnum`, `uid` against `data_header` (`InvalidIndexVersion`, `InvalidIndexAppNum`,
     `InvalidIndexUID`);
   - `page_size`, `ksize`, `value_size` against this binary (`InvalidIndexGeometry`);
   - plausibility: `page_count >= 2`, the root inside the tree, `1 <= height <= 48`
     (`InvalidIndexGeometry`);
   - the file holds every page the header names, i.e. no lost size extension
     (`InvalidIndexGeometry`);
   - when writable, trailing bytes past `page_count` are trimmed (not recovery: an unclean index
     stays unclean);
   - on a cleanly sealed file, the root page's CRC must be valid (`CrcFailed`).
6. Publish the tree as opened to the reader view; on a clean writable reopen, recover the free list.

A pack wrapper treats any rejection (except an `IO` error, which it surfaces) as "rebuild from the
log".

---

## Crash consistency and the owner's contract

The B+tree is not synced on the hot path, and its pages are reused without waiting for a sync. So
between syncs the on-disk tree is **not** self-consistent, and that is by design. Safety comes from
this contract with the owner:

1. **Never trust an unclean index.** `opened_unclean()` is true when the file had no valid
   clean-close sentinel. Such an index may lag its log or hold unstamped pages.
2. **Rebuild from the log.** Replay the data log to the live `(key, position)` set and call
   `rebuild_from(entries)` (or discard the directory and create a fresh index).
3. **Record coverage, sync and mark consistent:** `set_data_file_length(log_len)`, `sync()`, then
   `mark_consistent()`, so the next clean close seals the file and the next open skips the rebuild.
   An index that is never marked consistent rebuilds on every open.
4. **Also rebuild** when the index fails to open, or when `data_file_length()` differs from the log's
   length (the index lags the log).

`tndb::table::TnTable::open` / `Writer::recover` is the reference implementation of this contract.

---

## Concurrency rules

- **One writer.** All mutation takes `&mut BtreeIndex`; owners serialize writers (tndb holds a
  per-table writer mutex).
- **Lock-free readers via snapshots.** An `IndexSnapshot` is `Clone + Send + Sync` and reads
  published, immutable, CRC-stamped pages through the shared `MapView` with no lock and no shared
  write. Readers never block the writer, and the writer never blocks readers.
- **The owner keeps the file open while snapshots live.** A `MapView` does not own the mapping
  (`data_file::MapView`). A mapping replaced on a rare reservation overflow is retired, not
  unmapped, until the file closes; dropping the `BtreeIndex` while a snapshot is in use is a bug.
  (tndb keeps a cleared generation's files alive through its snapshots for exactly this reason.)
- **`reset_empty` / `rebuild_from` refuse to run while any snapshot is alive** (they truncate the
  file); in practice they run at open, before anything is published.
- A `BtreeIter` borrows the index (`&BtreeIndex`), so the tree cannot change under it.

---

## API reference

Public (`pub`) unless marked crate-internal.

| item | description |
|---|---|
| `BtreeIndex::open_btx_file(dir, &DataHeader, ksize, read_only)` | open or create `dir/index.btx` |
| `save(&mut, key, pos)` / `load(&, key)` / `contains(&, key)` / `remove(&mut, key) -> bool` | point ops on `&[u8]` keys of length `ksize()` (another length is a `KeySize` error) |
| `save_digest` / `load_digest` / `remove_digest` | `B256` adapters (32-byte index) |
| `impl Index<[u8; 32], u64>` | the generic point-index trait (`save`/`load`/`sync`/`contains`) for 32-byte keys |
| `iter` / `rev_iter` / `range` / `rev_range` / `prefix` | sorted iteration over the writer's tree (`BtreeIter`) |
| `clear(&mut)` | copy-on-write reset to an empty tree |
| `rebuild_from(&mut, entries)` | discard and rebuild from `(key, pos)` pairs |
| `sync(&mut)` | stamp, msync pages, then the header |
| `len` / `is_empty` / `height` / `ksize` | tree stats |
| `opened_unclean` / `mark_consistent` | the crash-consistency handshake |
| `set_data_file_length` / `data_file_length` | the covered log length (owner bookkeeping) |
| `set_owner_value` / `owner_value` | one `u64` the owner keeps with the index, durable at `sync` |
| `set_remove_on_drop` | delete on drop, skipping the sync |
| `page_crc_scan -> PageCrcReport` | off-path full CRC classification |
| `publish(&mut) -> IndexSnapshot` *(crate)* | publish the working tree for lock-free readers |
| `IndexSnapshot::{load, len}` + `PageSource` *(crate)* | snapshot reads and cursor source |
| `iter::BtreeCursor` *(crate)* | the detached cursor (`new`, `next`) |
| `BTX_MAP_RESERVE` *(crate)* | the 1 GiB mapping reservation (also used by tndb's data logs) |

---

## Errors

| type | from | notable variants |
|---|---|---|
| `LoadHeaderError` | `open_btx_file` | `IO`, `CrcFailed`, `InvalidType`, `InvalidIndexVersion`, `InvalidIndexUID`, `InvalidIndexAppNum`, `InvalidIndexGeometry` (geometry mismatch, implausible header, or a short file), `ReadOnlyEmpty` |
| `FetchError` | reads | `NotFound` (a miss, not an error condition), `KeySize { expected, got }` (a key that is not `ksize` bytes), `CorruptIndex(msg)` (out-of-tree page, zero-CRC page not written by this handle, depth exceeded, bad walk), `IO`, `CrcFailed` |
| `AppendError` | writes | `ReadOnly`, `KeySize { expected, got }`, `CorruptIndex`, `CrcError`, `WriteDataError(io)` (a failed growth also poisons the file, so it is never sealed; a failed split changes nothing) |
| `CommitError` | `sync` | `ReadOnly`, `IndexFileSync(io)` |

---

## Limits and non-goals

- **Fixed-size keys only** (1 to 2032 bytes), **`u64` values only**.
- **No node merging** on removal: emptied leaves are unlinked, but under-full ones stay sparse (a
  random-delete workload can leave pages part full). Pages are reused and owners rebuild, so there
  is no compaction pass.
- **No sibling links:** leaf-to-leaf movement goes through parents (O(height) per leaf boundary,
  amortized O(1) per entry).
- **Page size is a compile-time constant** (4096) and is checked against the header.
- **The free list is not persisted**; a clean reopen recovers it by walking the tree, and an unclean
  index is rebuilt anyway.
- **Not crash-safe on its own** by design: it relies on the owner's rebuild-from-log contract.
- Height is capped at 48 (`MAX_DEPTH`), a corruption tripwire far above any real height.

---

## Tests and benchmarks

Tests live in `index.rs` (`test_archive_btx_*`) and `iter.rs`:

- basic operations and reopen, a million keys with splits, custom key sizes;
- geometry and uid mismatch rejection, implausible headers, short files (lost size extension), and a
  corrupt root page on a clean open;
- torn tails stay unclean until rebuilt; a zeroed page is neither laundered by a sync nor
  answered silently, and writes into it are refused;
- `data_file_length` and `owner_value` become durable only at `sync`; remove-on-drop skips the
  sync; a read-only open does not create the directory;
- copy-on-write matches a model under random operations; published snapshots are immutable;
- reclaim never reuses a page a live snapshot can reach, bounds file growth, and handles snapshots
  released out of order; `clear` retires the old tree; reopen recovers free pages; reset is refused
  with a live snapshot;
- `rebuild_from`, `remove`, the CRC regime and rebuild;
- an allocation failure at every point of deep split cascades leaves the tree unchanged;
- a sliding window of ascending keys keeps the tree the size of the window (emptied leaves
  unlinked, pages reused), across a clean reopen; removing every key collapses the tree to one leaf,
  with earlier snapshots intact, and it refills;
- ascending inserts and sorted rebuilds fill leaves above 95%, while random inserts keep the usual
  split; the model test also runs with mostly removals over few keys;
- a wrong-size key is a `KeySize` error from every point op;
- iteration: sorted forward and reverse order, ranges, prefixes, and an empty tree.

```text
cargo test -p tn-storage btree
cargo test --release -p tn-storage index_bench -- --ignored --nocapture --test-threads 1      # B+tree vs digest index
cargo test --release -p tn-storage pack_vs_mdbx_bench -- --ignored --nocapture --test-threads 1  # pack-btree column
```
