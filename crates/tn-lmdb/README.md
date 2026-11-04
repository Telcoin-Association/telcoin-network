# `tn-lmdb`: a copy-on-write B+tree key-value engine

An embedded, memory-mapped key-value engine. One page file holds every table as a
copy-on-write B+tree with the values in its leaves, or, for a value too large for its leaf, in an
overflow run of contiguous pages (up to 16 MiB). A single writer builds the next version of
the trees in pages no reader can reach, while any number of readers see consistent snapshots
without locks. A commit becomes durable by writing one of two fixed meta pages: commits are
atomic, and recovery is O(1) because open picks the valid meta with the highest transaction id.
Pages a commit frees are reused only once no durable meta and no live reader can reach them.
The free list that tracks them is persisted in the file itself.

There is **no log**. The meta pages, the gated page reuse and the persisted free list carry all
crash safety. This is the difference from `tn-storage`'s
[`btree_index`](../storage/src/archive/btree_index/README.md), the tree this crate's code is copied
from. `btree_index` reuses pages before a sync, and its owner rebuilds it from a log after a crash.

The design is LMDB-style: copy-on-write B+trees in one memory-mapped file, two meta pages, a
single writer and snapshot readers. It is not LMDB and not a binding to it. It has its
own on-disk format (the meta records start with the magic `tn-lmdb\0`) and reads neither LMDB nor
MDBX files: such a file opens as `NotTnLmdb`.

The crate is self-contained: it copies its primitives (CRC, page set, sync wrappers, mapping) and
does not depend on `tn-storage` or reth. reth's database API on top of it is a separate crate,
[`tn-lmdb-reth`](../tn-lmdb-reth/README.md).

## Contents

1. [Concepts at a glance](#concepts-at-a-glance)
2. [On-disk format](#on-disk-format)
3. [Commit protocol](#commit-protocol)
4. [Free list and page reuse](#free-list-and-page-reuse)
5. [Readers and snapshots](#readers-and-snapshots)
6. [Open and recovery](#open-and-recovery), [read-only open](#read-only-open) and [close](#close)
7. [Failure latch](#failure-latch)
8. [Concurrency rules](#concurrency-rules)
9. [Deletes and merging](#deletes-and-merging)
10. [Dup tables](#dup-tables)
11. [Public API reference](#public-api-reference)
12. [Errors](#errors)
13. [Platform notes](#platform-notes)
14. [Limits and non-goals](#limits-and-non-goals)
15. [Intentional design decisions: please do not flag these](#intentional-design-decisions-please-do-not-flag-these)
16. [Source files](#source-files)
17. [Tests and benchmarks](#tests-and-benchmarks)
18. [Milestones](#milestones)

---

## Concepts at a glance

- **One file of fixed-size pages.** The page size P is a power of two from 4 KiB to 64 KiB. It is
  chosen when the file is created (default 4096) and kept for the file's life. Pages 0 and 1 are
  the metas, and every other page is a branch or a leaf of some tree, a page of an overflow run,
  or free. Pages are numbered by `u32`.
- **Tables are named B+trees** with byte-string keys and values stored in the leaves. A value too
  large for its leaf goes to an **overflow run** of contiguous pages, which its leaf cell names.
  A **catalog** tree maps each table name to the table's root. A **free tree** lists the pages
  committed transactions freed. The meta names both.
- **Copy-on-write.** The writer never changes a page a published tree can reach. It copies the
  page into a page private to the transaction. Readers therefore read published pages with no
  lock. A put or delete that changes only a published leaf copies it with the change made in
  the same pass, reading only the source (`page::copy_insert`, `copy_replace`, `copy_delete`):
  the source holds the copy's bytes outside the free gap, so every check sees the same bytes,
  and nothing in the write reads the copy, whose stores are still draining when a change made
  after the copy would first load them. Branches, splits and merges copy, then change the copy.
- **Single writer, many readers.** One `RwTxn` at a time (others wait). `RoTxn`s clone the
  current snapshot and see one consistent state of every table for as long as they live.
- **Atomic commit through two metas.** A commit syncs its pages, then writes its meta over the
  slot holding the lower transaction id and syncs that one page. The other slot always names the
  previous state.
- **Gated page reuse.** A page freed by txn N is reused only when neither meta slot nor any live
  reader can still reach it. In `Durable` mode that is from txn N+2 on.
- **Memory-mapped, no page cache of its own.** The file is mapped inside a reserved address range,
  so the base does not move while the file grows within it (past it the mapping moves, and the
  old one is kept for its readers). The OS page cache is the cache.
- **Fail-stop.** Any I/O or commit error, and any damage a read finds, latches the environment.
  From then on writes and commits return `Failed` and reads continue. A reopen recovers from the
  metas.
- **Deletes merge.** A page a delete leaves under a quarter full absorbs same-parent siblings
  while they fit, up the tree. A merge allocates nothing and never splits. See
  [Deletes and merging](#deletes-and-merging).
- **Dup tables.** A dup table (`RwTxn::create_dup_table`) keys every row `k ‖ s`, an outer key
  of a fixed width and a subkey. Its outer tree holds one cell per outer key: the key's few rows
  packed in the cell (a packed set), or the root of the key's own nested B+tree of subkeys and
  values once its rows outgrow the table's bound. The point operations take the logical key `k ‖ s`;
  `delete_prefix(k)` removes a key's rows in one walk. Its cursors (`DupRoCursor`,
  `DupTxnCursor`) walk the logical keys as a plain cursor walks a table keyed `k ‖ s`, plus
  seeks bounded to one outer key. See [Dup tables](#dup-tables).

**Not supported:** borrowed cursors over a dup table, and, by design, multi-process access
alongside a writer. See
[Limits and non-goals](#limits-and-non-goals).

---

## On-disk format

All integers are little-endian unless marked BE.

### Meta pages (pages 0 and 1)

Each meta page holds a 128-byte record at offset 0, and the rest of the page is zero (`meta.rs`):

| off | size | field |
|---|---|---|
| 0 | 8 | magic `b"tn-lmdb\0"` |
| 8 | 4 | format version = 5 |
| 12 | 4 | page size P |
| 16 | 8 | txn id |
| 24 | 4 | page count (pages `0..page_count` are allocated) |
| 28 | 4 | next table id (first table: 1) |
| 32 | 32 | catalog `TreeRoot` |
| 64 | 32 | free-tree `TreeRoot` |
| 96 | 28 | reserved (zero) |
| 124 | 4 | CRC32 over bytes 0..124 |

Decoding (`Meta::decode`) runs its checks in this order:

1. The magic must match, else `NotTnLmdb`.
2. The version must be 5, else `UnsupportedVersion`. This runs before the CRC check, because
   another version may lay the record out differently. The format is pre-release: a file of
   version 1 to 4 is refused (`UnsupportedVersion(1)` to `UnsupportedVersion(4)`), with no
   migration and never as damage. Version 3 added the nested cell (leaf cell flags 2) and the
   96-byte catalog record. Version 4 changed the tree page's CRC to skip the page's free gap
   ([Tree pages](#tree-pages-pagers)), so a version-3 page's whole-page CRC would not check.
   Version 5 raised the leaf entry limit from `U / 4` to `U / 2` ([Limits](#limits)), so values
   that version 4 kept in overflow runs are inline: a version-4 overflow cell of such a value
   would fail `page::validate`'s canonical-length rule. The meta record's layout is unchanged.
3. The CRC must match, else `Corrupt`.
4. The remaining fields must be possible, else `Corrupt`: a valid page size, no non-zero reserved
   byte, well-formed tree roots, a `page_count` of at least 2, and a next table id of at least 1.

`page_count` in the meta is the only authoritative length. The file may be longer (growth slack).

### `TreeRoot` (32 bytes)

| off | size | field |
|---|---|---|
| 0 | 4 | `root`: the root page |
| 4 | 4 | `root_txn`: the low 32 bits of the root page's header txn |
| 8 | 4 | `height` (1: the root is a leaf) |
| 12 | 4 | `overflow_pages` |
| 16 | 8 | `entries` |
| 24 | 4 | `branch_pages` |
| 28 | 4 | `leaf_pages` |

The fields of version 1 keep their offsets. `overflow_pages` takes the old reserved word, and
`entries` stays 8-byte aligned.

The page counters make `table_stat` an O(1) read. The tree code keeps them exact: a split adds
its new page, a new root adds a branch, a delete subtracts the pages it drops (the emptied pages,
the siblings its merges absorb and a root that gives way), and a clear resets them. A put that
stores its value in a run adds the run's pages to `overflow_pages`, and a replace or delete that
frees a run subtracts them (`TreeRoot::add_run`, `remove_run`, both checked). A copy-on-write
copy replaces a page and shares its runs, so it changes no count. `Env::check` compares every root's counters with the pages its walk finds, by kind, and
`overflow_pages` with the pages of the runs its leaves name.

`TreeRoot::check` refuses an impossible shape with `Corrupt`:
- The empty tree is exactly `root = u32::MAX` with every other field 0, and costs no pages.
- A non-empty tree has a height in `1..=48` (`MAX_HEIGHT`) and at most `2^32 − 3` pages (every
  page number but the metas and `u32::MAX`).
- At height 1 the root is the only page: 0 branch pages and 1 leaf page.
- At height h ≥ 2 there are at least h − 1 branch pages (one per level above the leaves) and at
  least 2 leaf pages (the root branch has two children or more).
- `overflow_pages` has no rule beyond the empty tree, because a value's overflow run can hang from
  a leaf at any height.

### Tree pages (`page.rs`)

```text
header (24):  pgno u32 | kind u8 (1 = branch, 2 = leaf) | flags u8 (0) | nkeys u16 | txn u64 | upper u16 | reserved [6]
slots:        nkeys × u16 cell offsets from byte 24; slot i addresses cell i (key order)
free space:   from the end of the slot array up to `upper`
cells:        packed from `upper` up to P − 4, in no particular order
trailer (4):  CRC32 over bytes 0..24+2·nkeys, then upper..P−4 (the page outside its free gap)
```

- **Cells tile `upper..P−4` exactly.** Deleting or shrinking a cell closes its gap at once, so a
  page never fragments. Free space is `upper − (24 + 2·nkeys)`, and `free + used == U` always
  holds.
- **The free gap need not be zero.** `PageWrite::alloc` does not zero a new page, and a page
  laid out there (`page::init_header`, then its cells and slots) writes every byte but the gap,
  which keeps the page's previous bytes (debug builds fill a new page with `tree::POISON`). A
  delete or a shrinking replace leaves bytes in the gap too: the moved cells' old bytes and the
  slot array's old last slot (`remove_cell`, `shift_front`). Copy-on-write
  (`tree::write::copy_page`) writes the source's header and slots, then its cells and trailer, so
  the copy's gap keeps the destination's old bytes as well. Nothing ever reads them, and the CRC
  skips them (do-not-flag items 17 and 28).
  - The gap's bytes are the same for the same history of operations: two files driven
    through the same writes stay byte-identical.
  - Why: zero-filling cost a page-sized `memset` per allocation, about 4.7% of a busy output in
    a macOS profile. A fresh layout writes every byte it relies on itself (`page::init_header`
    writes the header and the trailer, then the cells and slots follow; `overflow::encode_run`
    writes a run whole). Copying the gap too cost 3.4% and 1.8% per call on two Linux rewrite
    benchmarks (history-shaped and dup-shaped rewrites of existing pages).
  - The copy's source passed the fetch checks and, unless this writer stamped it, its CRC and
    `page::validate`, so its slots end at or below `upper` and `upper` at or below the trailer
    (`page::hdr`'s check); a header outside those bounds, which no caller passes, is copied
    whole, so the checks after the copy see the source's bytes. The leaf and branch mutators
    that grow into the gap (`reserve`, `shift_front`) write every byte they hand out before it
    is read.
  - Debug builds fill every page and run that `PageWrite::alloc` and `alloc_run` hand out with
    `tree::POISON` (0xA5), so the whole test suite runs every layout, and every copy (both
    stores copy through `copy_page`), over garbage, and a reliance on zeros or on a copied gap
    fails it.
  - A page taken from the file end still gets one byte written when it is allocated, so pages
    are first written in allocation order (see the macOS note in
    [Platform notes](#platform-notes)). That store is not a fill and promises nothing.
  - Deleted values can stay on disk in gaps until those pages are rewritten, as they already
    could in free pages and in the gap a delete leaves. The engine makes no secure-delete
    promise.
- **Leaf cell:** `vlen u32 | klen u16 | flags u16 | key | value`. The flags say what the value
  bytes are, and each flag has one exact shape (`page::leaf_cell`):

  | flags | the value bytes | shape |
  |---|---|---|
  | 0 | the value, inline | any `vlen` that fits the cell |
  | 1 (`CELL_OVERFLOW`) | a reference to an overflow run ([below](#overflow-runs-overflowrs)) | `vlen = 12` |
  | 2 (`CELL_NESTED`) | a nested tree's root: an encoded [`TreeRoot`](#treeroot-32-bytes) (`Val::Nested`) | `vlen = 32` |

  Any other flags, and flags 1 or 2 with another `vlen`, are `Corrupt`. A nested cell is
  canonical (`page::validate`) when its 32 bytes decode to a non-empty root that passes
  `TreeRoot::check`; the root's pages are its user's to check. Only a dup table's outer tree
  holds nested cells, one per outer key; one anywhere else is damage
  ([Nested cells and dup records](#nested-cells-and-dup-records)). In the outer tree of a dup
  table that packs, a flags-0 value is a packed set ([Packed sets](#packed-sets)); the page codec
  cannot tell one from any other inline value and never parses it.
- **Branch cell:** `child u32 | child_txn u32 | klen u16 | key`. There is one cell per child, with
  no separate rightmost-child pointer. Cell i's key is the lower bound of child i, and cell 0's key is empty and stands for
  −∞. `child_txn` is the low 32 bits of the child page's header txn.
- **CRC trailer.** It holds the CRC-32 of the page outside its free gap: of `page[..24 + 2·nkeys]`
  (the header and the slots) followed by `page[upper..P − 4]` (the cells), every byte a reader
  interprets (`page::crc`). The span comes from the header (`page::free_gap`), the one rule both
  the CRC and copy-on-write (`tree::write::copy_page`) follow. A header whose slots pass `upper`,
  or whose `upper` passes the trailer, gives no gap: its page is hashed whole, `page[..P − 4]`.
  `nkeys` and `upper` are covered bytes, so a header damaged after the stamp fails the check
  either way. The trailer is stamped at commit (`page::stamp`), and a computed CRC of 0 maps to
  the all-ones value, as `add_crc32_nonzero` maps it for a run. **An all-zero trailer means
  "dirty"**: a page being built by the writer. A published page with a zero trailer is
  `Corrupt`.
  Skipping the gap is deliberate (do-not-flag item 28): the stamp (commit step 3) and every check
  of a published page's CRC (the copy-on-write verify, open's root check, `Env::check`) read the
  bytes they hash, and on a long-running writer's pages the gap is about a third of a page, so
  hashing it pulled stale bytes from memory only to hash them (see the stamp note in
  [Platform notes](#platform-notes)). Overflow runs keep their whole-run CRC, and meta pages their
  own codec.

### Overflow runs (`overflow.rs`)

A value too large for its leaf lives in a run of `n` contiguous pages, and its leaf cell holds a
12-byte reference to it:

```text
overflow cell:  vlen u32 = 12 | klen u16 | flags u16 = 1 | key | ovf_pgno u32 | ovf_txn u32 | len u32

run (n·P bytes, n = ceil((28 + len) / P)):
  @0  pgno u32 | @4 kind u8 = 3 | @5 flags u8 = 0 | @6 reserved u16 = 0 | @8 txn u64
  @16 len u32  | @20 npages u32 | @24 value (len bytes) | zero pad | CRC32 u32 at n·P − 4
```

- **The cell is canonical.** `ovf_pgno` is the run's first page, `ovf_txn` the low 32 bits of the
  txn that wrote it (the identity check, as a branch cell's `child_txn`), and `len` the value's
  length. A flags-1 cell must have `vlen = 12`, and `page::validate` requires
  `max_value(klen) < len <= MAX_VALUE`: a value that fits its leaf is always inline, so every
  value has exactly one encoding.
- **One header, one CRC per run.** The header is on the first page only; pages 2..n are raw value
  bytes. The trailer at `n·P − 4` is a CRC32 over `[0, n·P − 4)`, stamped at commit with
  `add_crc32_nonzero`; all zeros means dirty, as for a tree page. The pad between the value and
  the trailer is zero. Kind 3 is no tree page's kind, so a tree pointer that lands on a run is
  `Corrupt`.
- **Runs are immutable.** A run is written in full before any leaf names it, and never changed.
  A copy of the leaf copies the 12 bytes and shares the run; a replace or a delete frees it.
- **`MAX_VALUE` = 16 MiB** (`16 << 20`), the largest `len` a run holds and a table accepts.

### Limits

`U = P − 28` (the header plus the trailer). An entry is a cell plus its slot, and each page kind
has its own entry limit (`page::entry_limit`): a leaf entry is at most `U / 2` bytes and a branch
entry at most `U / 4`. The derived limits are `max_key = U / 4 − 12`, so any key fits a branch
cell as a separator, and the inline limit `max_value(klen) = U / 2 − 10 − klen` for
`klen <= max_key` (0 for a longer key, which is refused with any value). `page::validate` holds
each page's entries to its kind's limit and every key to `max_key`: a leaf entry within `U / 2`
can carry a key far over `max_key`, so its key is checked too.

Some inline records keep the quarter-page bound `max_entry_value(klen) = U / 4 − 10 − klen`: the
catalog's and the free list's records (`Geometry::check_entry`), and a dup table's packed sets
(`packed_max` at most `max_entry_value(key_len)`) and outer cells (`max_dup_key_len`, the widest
key whose cell with a 32-byte nested root stays within `U / 4`).

**Where a table value goes** (`Geometry::place`): inline when it fits `max_value(klen)`, else in
an overflow run: every key up to `max_key` leaves room for the 12-byte overflow cell in a maximal
leaf entry, so a table takes values up to `MAX_VALUE` = 16 MiB with any key it accepts
(`Geometry::max_table_value`). The catalog and the free tree hold inline values only
(`Geometry::check_entry`); an overflow or nested cell in either is `Corrupt` (`Val::inline`).

| P | U | leaf entry (U/2) | branch entry (U/4) | max key | inline value (8-byte key) | inline value (32-byte key) | quarter-page value (32-byte key) | overflow value | a 1-page run holds | a 16 MiB value takes |
|---|---|---|---|---|---|---|---|---|---|---|
| 4 KiB | 4068 | 2034 | 1017 | 1005 | 2016 | 1992 | 975 | 16 MiB | 4068 B | 4097 pages |
| 16 KiB | 16356 | 8178 | 4089 | 4077 | 8160 | 8136 | 4047 | 16 MiB | 16356 B | 1025 pages |
| 64 KiB | 65508 | 32754 | 16377 | 16365 | 32736 | 32712 | 16335 | 16 MiB | 65508 B | 257 pages |

**One split always suffices.** An overflowing page holds at most `U` bytes plus the pending
entry. A split cuts it at the first entry where the left part reaches half of the total. A
branch's entries are at most `U / 4`, so both parts are then within `U`. A leaf's entries reach
`U / 2`, so that left part may pass `U`, or leave a right part under `U / 4`, merge-due at once:
then the leaf keeps one entry less (`SplitBuf::plan`), and both leaves end within `U` and at least
`U / 4` (the proof is in `SplitBuf::split_leaf`'s doc). With every entry at most `U / 4` the cut
never moves. An overflow cell is an ordinary inline cell of `8 + klen + 12` bytes, so this holds
with runs too.

**Append splits.** A leaf split usually cuts in the middle. When the overflowing leaf is the
rightmost of its tree and the change is at its end, the split is an append instead: the leaf keeps
every other cell, and the entry alone starts the new right leaf, its key the separator, so an
ascending load leaves full leaves behind it. That holds for an insert past the leaf's last entry
(an `append`, or a put of a new last key) and, in every tree, for a replace that grows the leaf's
last value past its free space (`write::plan`): a dup key's packed set, rewritten as its rows
arrive in order, is such a value. The path cache then holds the new right leaf (do-not-flag item
12). Every other split cuts in the middle; a middle cut always fits, and an append split's two
leaves are the old leaf less one cell and a leaf of one entry, each within U.

Larger entries are refused up front with `KeyTooLarge` or `ValueTooLarge`, before anything
changes. `Env::max_key_len` and `Env::max_value_len` report the limits; `max_value_len` reports
the table limit (16 MiB for every key up to `max_key`).

### Catalog

The catalog is a B+tree keyed by table name: UTF-8, 1..=255 bytes. Each value is a 96-byte
`TableRecord` (`catalog.rs`, `RECORD_LEN`):

| off | size | field |
|---|---|---|
| 0 | 4 | `id` |
| 4 | 4 | flags: bit 0 `FLAG_DUP` (a dup table), every other bit zero |
| 8 | 32 | `root`: the table's `TreeRoot` (a dup table's outer tree, which counts its own pages only) |
| 40 | 2 | `key_len` (dup: the outer key width; plain: 0) |
| 42 | 2 | `packed_max` (dup: the largest packed set in bytes of its encoding, as resolved at create; 0 = never packed; plain: 0) |
| 44 | 4 | `nested_trees` (dup: one per nested outer cell; plain: 0) |
| 48 | 8 | `nested_entries` (dup: the rows in nested trees; plain: 0) |
| 56 | 8 | `packed_rows` (dup: the rows in packed sets; plain: 0) |
| 64 | 4 | `nested_branch_pages` (dup; plain: 0) |
| 68 | 4 | `nested_leaf_pages` (dup; plain: 0) |
| 72 | 4 | `nested_overflow_pages` (dup; plain: 0) |
| 76 | 4 | reserved (zero) |
| 80 | 8 | `created_txn` |
| 88 | 8 | reserved (zero) |

The record ends as version 2's 56-byte record did, `created_txn` and then 8 reserved bytes, so a
byte at a given distance from a record's end means what it meant there.

`TableRecord::decode` takes exactly 96 bytes and refuses with `Corrupt`, in this order: a root
`TreeRoot::decode` refuses; a flag bit other than bit 0; a non-zero reserved byte; for a plain
record (flags 0), any non-zero field from offset 40 to 76; for a dup record (`DupRecord`), a
broken rule:

- `key_len` is at least 1;
- `nested_trees` is at most `root.entries` (one nested tree per outer entry at most);
- with no nested tree, `nested_entries` and the three nested page counts are 0;
- `nested_leaf_pages` and `nested_entries` are each at least `nested_trees` (a nested tree is
  non-empty);
- with `packed_max` 0, `packed_rows` is 0 and `nested_trees` equals `root.entries` (every outer
  cell is nested);
- with `packed_max` above 0, it is at least `PACKED_MIN` (7, the smallest set: "a packed-set
  limit below the smallest set"), and `packed_rows` is at least the packed sets,
  `root.entries − nested_trees` (a packed set is non-empty);
- `packed_rows` is 0 when there is no packed set ("packed rows without a packed set"), and at most
  the packed sets times `(packed_max − 2) / 5` ("more packed rows than the packed sets can hold": a
  row takes at least a 4-byte slot and a 1-byte subkey);
- the outer tree's pages plus the three nested page counts are at most `MAX_TREE_PAGES`, so the
  page totals of `TableStat` cannot overflow;
- `nested_entries + packed_rows` does not overflow a `u64` (the table's row count).

A record that decodes is then held to this build's limits at every use (`TableRecord::usable`,
behind `catalog::get`, the engine's one read of a record, and `Env::check`'s catalog walk): a dup
record whose `key_len` is past this page size's `max_dup_key_len` (below) is `Corrupt`, and so is
one whose `packed_max` is neither 0 nor in `7..=max_entry_value(key_len)` ("packed_max {n}
outside 7..={max} for this page size": no build writes one, as `create_dup_table` refuses a larger
`packed_max`). A plain record always passes.

Table ids are unique over the file's life, so a dropped and re-created table gets a new id.

### Nested cells and dup records

A dup table is a catalog record with flag bit 0 and an outer tree with one leaf cell per outer
key: a nested cell (leaf cell flags 2) naming the key's nested tree, or, in a table whose record's
`packed_max` is above 0, a packed set (flags 0, [below](#packed-sets)) holding the key's rows. The
outer-cell rule, which every read, write, free walk and `Env::check` holds an outer tree to:

- every outer key is exactly `key_len` bytes;
- every outer value is a nested cell, or a packed set when `packed_max` is above 0: an inline
  outer cell with `packed_max` 0 is `Corrupt` ("the outer cell of key .. holds no nested tree"),
  and an overflow cell is `Corrupt` in every dup table (a packed set is at most
  `max_entry_value(key_len)` bytes, so it is always inline);
- a nested cell is legal in both modes and at any row count (a nested key never demotes);
- a packed set is held to its codec's checks (below), at the first use of the rows it touches;
- a nested cell decodes to a non-empty root (an emptied nested tree leaves with its cell);
- a nested tree holds subkeys of 1 to `max_key − key_len` bytes and ordinary values (inline or in
  overflow runs), never a nested cell;
- no page is reached twice: a root named by two cells, or a page of two trees, is `Corrupt`.

Anywhere else a nested cell, and a dup record where a plain table is expected, are damage:
`Corrupt`, which latches as any damage does.

- **A nested cell outside a dup table's outer tree** (in a plain table, the catalog, the free
  tree or a nested tree) is refused by every read and write that meets it, with one message
  (`page::nested_error`, "a nested cell in a tree that holds none"): a value read
  (`PageRead::value`: gets, cursor moves and seeks), a cursor's `current_with_key_tail` and
  `seek_with_key_tail`, a put or delete of its key (the path cache's included), copy-on-write of
  a published leaf holding it and a merge absorbing that leaf (`check_cells_naming_this_txn`),
  a free walk that reads its leaf (`tree_pages`, behind `drop_table` and `clear_table`) and
  `Env::check`. `page::validate` holds it to the canonical shape above. A free walk reads every
  leaf only of a tree with overflow pages; in another tree a nested cell in a leaf the walk does
  not read is not met there, as other damage inside such a leaf is not.
- **A dup record** is read by `catalog::get` like any record and must pass
  `TableRecord::usable` (above), else it is `Corrupt` at every use: a table's resolution,
  `open_table`, `create_table`, `create_dup_table`, `drop_table`, `clear_table`, every read or
  write through a handle, and `Env::check`. A call that needs the other kind of table
  (`create_table` or a plain cursor on a dup table, `delete_prefix` on a plain one) is
  `TableKind`, a validation error that changes nothing and does not latch.

### Packed sets

A packed set (`packed.rs`) is the whole value of a flags-0 outer cell: one outer key's rows,
little-endian, in subkey order.

| off | size | field |
|---|---|---|
| 0 | 2 | `n`: the rows, at least 1 |
| 2 + 4i | 2 | `end[i]`: the offset from the set's start one past row i's bytes |
| 4 + 4i | 2 | `slen[i]`: row i's subkey length |
| 2 + 4n | ... | the rows' bytes, row after row: `s_i ‖ v_i` |

Row i spans `start_i..end[i]`, with `start_0 = 2 + 4n` and `start_i = end[i−1]`; its subkey is the
first `slen[i]` bytes and its value the rest (possibly empty, always inline). A seek is a binary
search over the slot pairs, and `s ‖ v` is contiguous, as a nested leaf's cell is. A set costs 2
bytes plus 4 per row (a nested leaf spends 10 per row). The smallest set, `PACKED_MIN`, is 7 bytes
(one row, a 1-byte subkey, an empty value); the largest is the table's `packed_max`, at most
`max_entry_value(key_len)`, so every offset fits a `u16`.

The checks, and when each runs:

| check | `PackedView::new`, every decode (O(1)) | `PackedView::row`, each row touched | `packed::validate` (whole, O(n)) |
|---|---|---|---|
| `n >= 1`, at least `PACKED_MIN` bytes | yes | | yes |
| at most `packed_max` bytes | yes | | yes |
| the slot table fits; the rows end at the set's end (`end[n−1]` is its length) | yes | | yes |
| the row in bounds, after the slot table, `start_i <= end[i]` | | yes | yes (every row) |
| a subkey of 1 to `max_key − key_len` bytes, within its row | | yes | yes |
| subkeys strictly ascending | | a hit is exact; a miss's two neighbours bracket the subkey | yes |

A read (`get`) runs `new` and the rows its binary search probes, a cursor `new` when it enters the
key and `row` plus an order compare with the previous subkey at each step, so a whole walk of a key
checks its whole order. Every write that decodes a set (put, insert, append, delete,
`delete_prefix`, promotion), the free walks of `drop_table` and `clear_table`, and `Env::check`
run `validate` first, before anything changes. A failure is `Corrupt` ("dup table {name}: the
packed set of key {hex}: ..."), and latches as any damage does. A set's bytes are not checked at
page validation or copy-on-write (do-not-flag item 25).

Checking only the rows a read touches is deliberate (do-not-flag item 23): damage is `Corrupt` at
first use, at the granularity of what the read touches, the rule pages and the subkey width checks
follow. The whole check on every get would cost several times the lazy one on a full set
(estimated ~150-250 ns against ~30-40 ns at a 2 KB set), up to half again a packed get's outer
descent. The exposure: a misorder off a get's probe path passes that get
(`the_lazy_checks_refuse_what_they_touch` pins it); a write to the key, a cursor's walk of it or
`Env::check` refuses it.

### Free-list records

- **Key** (12 bytes): `txn u64 BE | seq u32 BE`. The key holds a transaction and a sequence
  number, so records sort by txn. The txn is the reuse gate the pages wait for: the committing
  transaction for the pages it freed, or the key of a record it took for the pages it took and
  left unused (see [Free list and page reuse](#free-list-and-page-reuse)).
- **Value:** page numbers as `u32` LE, strictly ascending. A value holds at most
  `record_capacity = max_entry_value(12) / 4` page numbers (248 at 4 KiB). Empty values are allowed.

---

## Commit protocol

**Copy-on-write in one copy.** A commit publishes private pages: the first change a write
transaction makes to a published page copies it into a page reserved for the write
(`write::writable`), stamps the copy with its new page number and the transaction's id, zeroes its
trailer (dirty) and frees the original. The copy is one copy (`PageWrite::copy_published`). The
writer borrows the published source through its own clone of the readers' `MapView` (`Writer::view`:
the one view the file updates in place when its mapping moves, so it is never refreshed by hand),
with exactly `PageRead::page`'s checks (`checked_page`), and writes it into the private page through
`PageFile::page_mut`. The two slices coexist: each is formed from the mapping's raw pointer
(`MmapRaw`) and covers exactly its own page, and they are never the same page, because
`copy_published` refuses a private source and the destination must be private
(do-not-flag item 2). A source past the published bytes, where the view ends, is no page of a
committed tree, only one this transaction freed, and only damage names it: the cold
`copy_unpublished` copies it through a buffer of its own, so its checks and messages stay
`PageRead::page`'s. The test store (`MemStore`) copies with one `copy_within`: it has no readers.

`RwTxn::commit` with `SyncMode::Durable` (`txn.rs` `Work::commit`, `env.rs`
`Writer::make_durable`) runs these steps:

| step | what |
|---|---|
| 0 | Latched: return `Failed`. A transaction that changed nothing returns its base txn id with no I/O. |
| 1 | Write each changed table's root into the catalog (in name order), then save the free list (see below). |
| 2 | Build meta M in memory: txn L+1, both roots, the page count. |
| 3 | Stamp the CRC of every page the transaction created (they become "stamped by this writer"). A tree page's CRC skips its free gap (`page::stamp`, [Tree pages](#tree-pages-pagers)); an overflow run gets one CRC over all its pages, never one per page (`stamp_private`, `PrivatePages::units`, `stamp::Unit`), and its pages never count as stamped. Below 2 MiB of units the writer stamps each unit in turn (`stamp::stamp`). From 2 MiB (`stamp::threads`: a second thread at 2 MiB, one more per MiB, at most 8 and the CPUs the process may use) it computes the CRCs on several threads first (`Writer::unit_crcs`, `stamp::crcs`): each thread reads a contiguous share of the units, balanced by the units' bytes (not by the bytes hashed), through `PageFile::pages` slices while the writer holds the file shared, so no page changes and no `unsafe` is added; then the writer stores each CRC in unit order through `PageFile::pages_mut` (`stamp::put_crc`). The bytes, their order and the errors are the serial loop's, and the crash points are unchanged. |
| 4 | Sync the dirty pages (`data_sync`), every page of every new run included. Runs separated by at most 64 clean pages (`MERGE_GAP`) merge into one range. If the pages form exactly one range, in at most 16 runs of adjacent pages (`MAX_RUNS`), one `msync(MS_SYNC)` of that range; otherwise one `fdatasync` of the file (why: below and [Step 4](#step-4-one-msync-or-one-fdatasync); do-not-flag item 10). |
| 5 | If the file grew since the last sync, run `fdatasync_file` so its size is durable. After step 4's `fdatasync` this has nothing left to do: that call made the size durable too. |
| 6 | Copy meta M into the slot holding the lower txn (an invalid slot first; on a tie, slot 1), msync that one page, then record M's txn as the slot's. |
| 7 | Publish: swap in the new snapshot, push `(M, Weak<Snapshot>)` on the reader queue. |

**Step 4's one sync.** Why step 4 is one call, and the numbers behind its choice, are in
[Step 4](#step-4-one-msync-or-one-fdatasync) (do-not-flag item 10). The `fdatasync` also writes any
other dirty page of the file. At a commit that can only be a page of an aborted transaction, which
is a free page, so writing it is harmless. A crash test cannot stop a commit between data ranges,
because there is one sync; partial writeback of the dirty pages is still modelled by
`power_loss_random`, and the crash matrix runs every point on both paths (`Writer::data_by_msync`,
`Writer::data_by_fdatasync`).

Crash outcomes (L = the last durable state, M = this commit):

| crash during or after | Durable recovers | NoSync recovers |
|---|---|---|
| steps 1–3 (memory only) | L | the last synced state |
| step 4 or 5 | L. M's meta was never written, and L's pages are untouched, because pages reachable from either slot are never reused. | not applicable |
| step 6, before the meta's msync completes | M if its record reached the disk with a valid CRC, else L | not applicable |
| after step 6 | M | not applicable |
| any time after a NoSync commit, before `Env::sync` | not applicable | the last synced state. A process crash also rolls back. |

**NoSync** runs steps 1–3 and 7 only. The stamped pages accumulate in an `unsynced` page set, and
no meta is written, so the slots stay put. `Env::sync()` runs steps 4–6 for the newest state.
Dropping the environment calls it unless the environment is latched. Because the reuse gate uses
the slots' txns, the durable state's pages are never overwritten, and the file stays consistent
whatever is lost.

**Abort** means dropping the `RwTxn` (or calling `abort`). The free tree was never touched, the
pool and `next_pgno` return to the transaction's base, and its private pages and runs are simply
unreferenced. There is no I/O.

**An `Err` from commit** latches the environment (`fail_damaged` for `Corrupt`, otherwise `fail`),
and whether the commit survives is unknown until a reopen. Validation errors (`KeyTooLarge`,
`KeyTooShort`, `ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, table errors, `TableKind`,
`InvalidDupOptions`, `ReadOnly`, `WrongTxn`) happen in `put` and the other calls, before anything
changes, and do not latch.

---

## Free list and page reuse

**During a transaction** (`alloc.rs`):

- A published page that leaves a tree goes to `freed`.
- A page the transaction itself created that leaves a tree ("loose") goes straight back to
  `pool`.
- An allocation takes a page from `pool`, else the next page number at the end of the file,
  growing the file.
- An overflow run of `n` pages (`Writer::alloc_run`, `allocate_run`) tries, in order:
  1. the first fit in `pool`: the lowest `n` consecutive page numbers it holds (`first_fit`, which
     sorts the pool; `n = 1` takes a page as an allocation does);
  2. more free records that pass the gate, loaded until the pool fits or 8 records
     (`RUN_LOAD_RECORDS`) are loaded, then the first fit again;
  3. tail reuse: pool pages that end the file start the run, and the file grows by the rest;
  4. the next `n` page numbers at the end of the file, in one growth.

  The budget counts records, not pages: a budget of `8 · n` pages stopped after the first record,
  which lists far more, and left runs growing the file without bound under churn while later
  records held fits (`run_churn_keeps_the_file_bounded_heavy`). Its pages are private, and the
  allocator also records the run (`private_runs`) for the commit's one CRC.
- A run that leaves a tree goes back as `n` single pages (`free_run`): to `freed` if published,
  to `pool` if this transaction created it. Free records list single pages, so a freed run's
  pages come back one by one, as tree pages or as parts of a later run.
- `clear_table` and `drop_table` collect every page before freeing any (`tree_pages`). Each branch
  is read with a descent's checks and verified unless private or stamped, and its child pointers
  follow the provenance rules of a descent (do-not-flag item 11): a page private to the
  transaction named by a page it did not write, or by an untrusted root handle, is `Corrupt`, and
  a private child must be the page and txn its parent names. A freed private page goes back to
  the pool at once, so a forged pointer to a page this transaction allocated for another tree
  must not free it. A tree whose `overflow_pages` is 0 reads its branches, its first leaf and its
  private leaves (in memory) only. A tree with runs reads every leaf, checks each run's header,
  identity and provenance (`leaving_run`, without the CRC: the value is not read), and requires
  the run pages to add up to `overflow_pages`. Damage refuses the free with `Corrupt`, before any
  page is freed.
- When `pool` is empty, the writer refills it (`Writer::refill`). It takes free records smallest
  first, and only while their txn is at most the **reuse gate**:

  ```text
  gate = min( txn of each valid meta slot , txn of the oldest snapshot a reader may hold )
  ```

  `Writer::reuse_gate` computes it. The oldest reader comes from the reader queue, followed by
  `fence(Acquire)`, which pairs with the `Release` drop of each snapshot's last `Arc`.

Before a record's pages are handed out, `next_record` and `decode_pages` check them:

- every page on the way to the record passes its full CRC and structure checks, within its
  ancestors' separators;
- each page number is in `[2, page_count)`;
- the page numbers are strictly ascending;
- no page was already taken by this transaction.

Damage is `Corrupt`, and none of the record's pages is used. The free tree itself is not changed
while the transaction runs.

A record that lists a page a tree still holds passes these checks: telling would take a walk of
every tree. A write that hands the page out overwrites it, and a read through the old pointer
then fails its checks (a run of more than one page fails its CRC; a tree page or a 1-page run its
header or txn identity), so the damage is met as `Corrupt`, never as wrong data. A write that
instead takes the page out of its tree while the page waits in the pool finds it at commit both
free and freed (`Allocator::reusable`), which is `Corrupt` too. A `clear_table` or `drop_table`
of the tree that holds the page, after a write in the same transaction handed it out, finds it
private under a page the transaction did not write (`tree_pages`), `Corrupt`. In a later
transaction, the page now published, a free of a tree without runs cannot tell (it does not read
the leaf) and frees the page again; once a write reuses it, a read through either pointer fails
its txn identity, `Corrupt`. A forged nested root named by two outer cells of a dup table, each
cell written in its own transaction, is the same case. A page freed twice in one transaction (two
pointers naming it, such as two cells naming one run) is `Corrupt` at commit
(`Allocator::reusable`). `Env::check` reports the page as used twice.

**Reuse timing.** A record's txn is the reuse gate its pages wait for: once the gate reaches it,
no durable meta and no live reader reaches them. A page freed by txn N is listed under N, since
N's base state still holds it. In `Durable` mode, after commit N the slots hold N−1 and N. So a
page freed by txn N becomes reusable at txn **N+2**, once N+1 has committed and the slots hold N
and N+1. In `NoSync` mode the slots move only at a sync, so a freed page comes back once two syncs
have made states from N on durable in both slots.

**The save at commit** (`freelist::save`) runs with loading stopped, so pages come only from the
pool or the end of the file:

1. Delete the records the transaction consumed.
2. Write the pool, sorted and chunked by `record_capacity`, under the keys of the records
   consumed, in order. Every pool page is free in each state a durable meta or a reader holds (a
   page taken from a record that passed the gate, or one this transaction added to the file), and
   every key consumed has passed the gate, so the next transaction may take these pages at once.
3. Write the freed pages, plus any pool pages past what the keys consumed hold (only pages this
   transaction added to the file and freed again), as records `(M, 0..k)` of the committing txn
   M.
4. Writing can take pool pages and free pages of the free tree, which changes both lists. So
   write again until the records list exactly them: a fixed point.

Record keys are never deleted inside the loop. A record no longer needed is written empty, so a
round never frees a page a later round must list. Three rounds suffice. More than 8 is a broken
invariant, which returns `Failed` and latches.

Each round takes the two lists from `Allocator::reusable`. A peak commit's lists hold about as
many pages as it wrote, so the round sorts each list once, with `alloc::sort_pages` (an LSD radix
sort, 11 bits a pass, which skips a pass whose digit every page shares). One linear pass over the
two sorted lists (`first_duplicate`) then finds the smallest page listed twice: twice freed
(`Corrupt`), a loaded pool page also freed (`Corrupt`), or any other repeat (`Failed`). The pool
pages that spill to step 3 merge into the sorted freed pages (`merge_sorted`) instead of a sort.
The records are those a combined sort of both lists wrote, byte for byte, with the same errors:
`reusable_matches_the_reference` (and its `_heavy` twin, on a peak commit's list sizes) holds
`reusable` to that combined sort, kept test-only as `Allocator::reusable_reference`;
`the_save_writes_the_records_the_reference_writes` compares whole commits' free trees, and
`a_spill_below_the_freed_pages_is_merged_into_them` a commit whose spilled pool pages fall below
pages it freed (a record size cut below the one the record taken was written with).

**Leftover pages are reusable at once.** When a transaction consumes a record but uses only some
of its pages, the rest stay in `pool`, and step 2 lists them again under a consumed key. No two
records list one page: the records consumed are deleted in step 1, and their keys are free for
step 2. Listing them under M instead made them wait two more commits, longer
under a held reader: each commit then took more records and left more over, and under churn of a
tenth of the pages per commit with readers held the file reached 2.5 times its live pages, against
1.6 now (`run_churn_with_readers_keeps_the_file_bounded`).
A leftover page can then sit under a key older than the record that listed it (do-not-flag item 7).
That is sound: a key's txn only has to be one the gate has passed, every key consumed had, and the
gate never moves back, so no state a durable meta or a reader can hold reaches the page.

**The accounting invariant** that `Env::check` enforces after every commit in the model tests is:

```text
(pages reachable from the catalog, every table and the free tree)  ⊎  (page numbers in free records)  =  [2, page_count)
```

A table's reachable pages include every page of the overflow runs its leaves name. The union is
disjoint: every page is used by exactly one tree, once, or listed exactly once. Any leak or double
use is `Corrupt` naming the pages, and it latches.

---

## Readers and snapshots

A `Snapshot` holds `{ meta, geometry, view: Arc<MapView>, tables: TableRoots }`. It is published
as an `Arc` under a `parking_lot::RwLock`. `begin_ro` clones the current one, so a reader sees one
state of every table. The environment's own `Arc` to the current snapshot keeps the reader queue
non-empty.

**A long reader shows, at no cost to readers.** A reader holding a superseded snapshot keeps every
page freed after it from reuse (the gate in [Free list and page reuse](#free-list-and-page-reuse)),
so the file grows while it lives; the pages become reusable once it ends.
`Env::oldest_reader_age() -> Option<Duration>` says how long the oldest live reader has held a
superseded snapshot. Each entry of the writer's reader queue records when the next commit
superseded its snapshot (one monotonic clock read per commit, in `Writer::publish`), and each
commit, after pruning the queue, stores the oldest live entry's time in an atomic of the shared
state. The call loads it and subtracts it from now, with no lock; `begin_ro` and a reader's drop
do nothing for it. A reader of the current snapshot does not count, as it holds back no page a
commit freed. The value is refreshed only at commits: between them, a reader that has ended still
counts until the next commit. A read-only environment never commits, so it always returns `None`.

**A snapshot resolves each table handle once.** `TableRoots` remembers the tree roots its
readers resolved, in 32 slots (`OnceLock<(Table, TreeRoot)>`): a handle's slot is the first one
from `id % 32` on, wrapping, that no other handle holds, and a hit is `Table::is`, the id and
then the name (a handle of another file, or one of a table an aborted transaction created, whose
id the next table created takes, may carry the same id). Every `RoTxn` on the snapshot shares
them, so a table's catalog lookup runs once per snapshot, not once per transaction, and an
`RoTxn` is two `Arc`s (`{ inner, snapshot }`). `RoTxn::root`, inline, checks the handle's home
slot; `resolve_root`, out of line, probes on, then reads the catalog and remembers the root it
resolved in the first free slot. Only a resolved handle is remembered: `StaleTable` and
`TableNotFound` are checked again at each use. A slot is never emptied, and once every slot holds
another handle a handle resolves through the catalog at each use, uncached. Each commit
publishes a new snapshot with no table resolved; a read-only open has one snapshot for its life,
so it resolves each handle once. `open_table` reads the catalog every time. The cache is safe (do-not-flag
item 15): a snapshot's catalog never changes while an `Arc` of it lives (the reuse gate holds its
pages), so a second lookup would read the same bytes, pass the same checks and give the same root.
Damage in the catalog is met by the first resolve in each snapshot and by every `open_table`.

**Every page fetch** (`tree::read::checked_page`) checks:

- `2 <= pgno < page_count` (the snapshot's), and the page is mapped;
- a non-zero CRC trailer, unless the page is private to the writer;
- the header's `pgno` equals the requested one;
- the header's `txn` is no later than the reading snapshot's txn, so a page reused too early
  surfaces as `Corrupt` instead of wrong data.

**Every descent** (`read::descend`, the cursor's `descend`) also checks:

- each child is the copy its parent names: header `pgno == child` and `txn as u32 == child_txn`.
  This catches a pointer redirected to a stale copy that still has a valid CRC. The fetch reads
  the header's `pgno` and `txn` once for these and the per-fetch checks
  (`PageRead::child`, `tree::read::checked_child`).
- a branch at every level above the leaves and a leaf at the leaf level;
- only the root leaf may be empty;
- keys lie within the intersection of every ancestor's separator range. A cursor's descent along
  an edge (to the first or last entry, and every crossing) checks each page's first and last keys
  against it; a cursor's seek, as a point read, checks only the keys its search compared, and the
  leaf it lands in gets the full check before any read past its landing entry (below).

**A seek checks only the keys its search compared** (do-not-flag item 29; `TreeCursor::descend`
for a key: `seek`, `seek_exact`, `seek_with_key_tail`, and the dup walker's seeks and finds, in
outer and nested trees). At a branch it checks the route's bounds, the separators either side of the child taken, against the range the levels
above give the page (`route.lo >= lo`, `route.hi < hi`); at the leaf a hit needs nothing (the
sought key itself, which the routes kept inside every range), and a miss checks the keys
either side of the insertion point, `read::check_miss`'s rule, which point reads
(`read::get`) have always followed. The check of every key, the page's first and last against
the range, is deferred: the leaf a seek lands in stays unchecked (`TreeCursor::leaf_checked`)
until a read of it past the landing entry (a step, a prefix step, a seek from the leaf) gives it
that check first, once (`TreeCursor::check_leaf`: the path's branches fetched again for the
range, the descent's message). That range is read from two levels only
(`TreeCursor::seek_bounds`): the descent checked each route against the range above it
(`route.lo >= lo`, `route.hi < hi`), so the deepest route with a lower bound gives the
intersection's `lo`, and the deepest with an upper bound its `hi`. The slots name those two
levels with no cell read (a route has a lower bound exactly when its slot is not the branch's
first, an upper one when it is not the last; `TreeCursor::bound_levels`), so the descent
records nothing, and the check reads only their routes, every branch still fetched again with
the per-fetch checks and parsed as a branch, in the path's order, before any route (the routes
of the other levels are cells the descent's search read with the same cell checks). Debug
builds assert that range equals, by content, the one built from every level's route
(`TreeCursor::bounds`; `a_seeks_leaf_check_reads_the_range_of_its_deepest_bounded_levels`).
A dup write reads its key's cell in a held outer leaf that is still unchecked only at the
cursor's slot, its landing entry, and finds any other cell from the root (`held_cell`). A
descent along an edge (to the first or last entry, every crossing) keeps the full check of
every page.
- What is given up: a seek that lands and moves no further accepts a page that only partly
  overlaps its range (a damaged separator or pointer whose page straddles a bound), its landing
  entry, which the search compared, inside the range. A page wholly outside its range is still
  refused at the seek with the same message (every damage sweep's expectation holds
  unchanged), and every entry a cursor returns past its landing comes from a leaf checked in
  full (`a_seek_lands_in_a_leaf_partly_outside_its_range_and_reading_on_refuses_it`,
  `a_seek_into_a_page_wholly_outside_its_range_is_corrupt_at_that_page`).
- What it adds: the deferred check fetches the path's branches again, so a seek inside a leaf a
  seek entered meets damage on the path that the cursor's landing did not
  (`a_seek_from_the_leaf_meets_damage_as_a_descent_does`).
- Why: the full check reads the first and last keys of every page a seek enters, cells its
  search mostly never touches, on every seek, though most seeks never read past their landing
  (a point read through a cursor, an exact seek). In TN's reth exec bench on Linux it cost about
  16 ms of a ~1.5 s peak output, 11.7 ms of it in the state root, nearly all in the cursor's
  descent. A seek that steps on pays the full check once, for its leaf alone.

The cursor's descent (`TreeCursor::descend`) parses each page's header once (`page::View::new`),
after the fetch, identity and kind checks; the other checks and the search then read through that
one view: along an edge the separator-range check, the empty-page checks and the move to slot 0 or
the last slot, in that order; for a key the empty-page checks, the search, then the range check of
the keys it compared. Right after the parse, the view prefetches the cells the level reads first:
along an edge the first and the last key's, which the range check reads, and the slot the move
takes (`View::prefetch_level`); for a key the cell of the search's first probe alone
(`View::prefetch_probe` of `View::first_probe`), since its range check reads only cells the search
compared. Those cache misses then overlap instead of following one another; a prefetch is only a
hint, so every check, its order and its message stay. The leaf's view goes to the move that
settles there (`Reached`), which reads its entry through it: `seek_exact`'s hit is the search's own
`Find::Found` value, not a second read of the cell. A crossing to the next or previous leaf climbs the path to the nearest branch with a
child beyond it (`climb`) and descends from that child (`land_beyond`).

**A seek starts from the leaf the cursor is at** when the cursor is at an entry and its path is
still the tree's: always for `Cursor` and `RoCursor` (a transaction's or a snapshot's tree does
not change under them), and for a `TxnCursor` while its table is unchanged since its last call
(`TreeCursor::seek` with the leaf's page; `TreeCursor::near` decides): a key between the leaf's
first and last keys is searched in that leaf alone. Any other key, even one next to the leaf, and
any seek of a cursor not at an entry, descend from the root, as before. The leaf's parent is not
read to place a key just outside the leaf: a seek that misses the leaf would pay that branch fetch
and its compares before descending anyway, and a run of forward seeks mostly lands in the leaf it
is at, so that tier bought forward seeks nothing measurable and cost random ones.

`seek`, `seek_exact` and `seek_with_key_tail` all start this way; an exact seek's key in a gap of
the leaf is a miss, the cursor unset. These are the entries a descent from the root finds once
the leaf's keys are known to lie inside the range all its ancestors' separators give it (the
argument of do-not-flag items 11 and 12): a key between its first and last keys then lies there
too, and its descent takes the cursor's own path. A leaf a seek's descent entered has had only the
checks of the keys its search compared (above), so the seek from it first gives it the full
range check, as a step does before reading past the landing entry (`TreeCursor::check_leaf`); a
leaf already
checked in full, or entered along an edge, is not checked again. A key outside the leaf trusts
nothing of it and needs no check. The leaf is the page `Cursor` holds, `RoCursor`'s
`Snapshot::page_again` (unchecked, do-not-flag item 14) and a `TxnCursor`'s `Writer::page` (checked). A seek
that descends meets damage with the descent's exact message; damage in the leaf the cursor
already holds, or on its path, is met only by those re-fetches and that one full check, as for a
step (do-not-flag item 14). `RwCursor`'s seeks, and the re-seeks a write cursor makes after a change to its
tree, descend from the root.

**The writer skips them on pages it built.** Its descent (`write::descend_write`) trusts a page
this transaction's writer built from checked pages and still holds private: the root when its
holder's flag says the writer wrote it, and below a trusted page every private child. A trusted
page gets only the identity, kind and non-empty-leaf checks, which read the header line that is
loaded anyway. The per-fetch checks and the separator-range check run on the untrusted
(published) pages, the only ones damage at rest or a redirected pointer can reach, and so does
`verify_published` unless this writer stamped the page. On such a page the descent first
prefetches the range check's two cells and the cell of the level's first probe (the hinted slot,
else the search's), as the cursor's descent does. The rule is
`trusted(child) = trusted(parent) && is_private(child)`. `is_private` alone is not enough, and the
rules that close the gap are deliberate (do-not-flag item 11):

- an untrusted parent (or root handle) naming a private page is `Corrupt`: a sound tree never
  has that edge. The descents (`descend_write`, `collapse_to`), a merge's sibling reads
  (`sibling_view`, `check_sibling_chain`) and the free walk of `clear` and `drop`
  (`tree_pages`, which also checks a private child's identity) all apply it;
- when the writer copies a published branch it did not stamp (where `verify_published` runs),
  every cell whose `child_txn` equals this txn's low 32 bits must name a page whose header
  matches the cell and whose full txn is below this one (`check_cells_naming_this_txn`).
  Otherwise a forged cell naming a free page that this txn later allocates for another tree
  would pass the copy and let a later put write into that tree's page. A child whose txn
  wrapped 2^32 back passes; a free page's stale bytes and a page already private fail.
- overflow runs follow the same rules. A run private to this transaction may leave a tree
  only through a leaf this transaction wrote: a private run named by a leaf that is not
  private is `Corrupt`, as is a run that overlaps private pages without being one of this
  transaction's runs (`leaving_run`, which a replace, a delete and `tree_pages` call before
  freeing it). The copy-time rule covers overflow cells: when the writer copies a published
  leaf it did not stamp, every overflow cell whose `ovf_txn` equals this txn's low 32 bits
  must name a run whose header matches the cell and whose full txn is below this one
  (`check_cells_naming_this_txn`). Otherwise a forged cell naming a run this txn wrote for
  another entry would let a replace or delete through the copy free that run.

The range check's bounds are lazy: the descent recomputes the trusted ancestors' separators
with `BranchView::route_at` from its path when it first reaches an untrusted page, which gives
exactly the bounds `route` gave on the way down. A descent routed by a cursor's hint
([Detached cursors](#detached-cursors-txncursor))
applies these rules unchanged on every page it enters.

**Every overflow value read** (`PageRead::overflow`, `overflow::checked_run`) checks the run
against its cell before returning a byte:

- the run's `n` pages lie in `2..page_count` (a checked add) and are mapped;
- its header names it: `pgno`, kind 3, flags 0 and zero reserved bytes;
- its header `txn` is no later than the reading snapshot's, and its low 32 bits are the cell's
  `ovf_txn`, so a redirected or stale reference is `Corrupt`;
- its `len` and `npages` are the ones the cell implies;
- unless the run is private to the writer, a non-zero trailer and, **for a run of more than one
  page, the full CRC** (`RunCrc::MultiPage`). Pages 2..n carry no header, so only the run's CRC
  ties them to it: without it a reused or redirected middle page would return wrong data.

A 1-page run is all header-checked page, so a read checks it by its header, as it checks a leaf;
damage at rest inside it is `Env::check`'s to find, as inside a leaf. `Env::check` verifies every
run's full CRC and its zero pad (`RunCrc::Always`, `claim_run`). The walks that only free a run
or check its txn (`leaving_run`, `tree_pages`, the copy-time rule) check its header and identity
and skip the CRC (`RunCrc::Skip`), since they never read the value. The multi-page rule is never wrong
data, so the CRC stays (do-not-flag item 13).
It costs a pass over the value. In the comparison benchmark's `big` rows
([`engine-bench.md`](../tn-lmdb-reth/engine-bench.md#large-values-overflow-runs); macOS,
medians of three, each value size in a fresh environment) a get takes about 1.1 us at 8 KiB
and 2.1 us at 32 KiB at 4 KiB pages (`t4d`, runs of 3 and 9 pages), and 1.4 and 2.6 us at
16 KiB pages (`t16d`). With the CRC skipped in an experiment, the same `t4d` and `t16d` gets took 0.34–0.40 us. A 1-page run
has no page without a header, so its read skips the CRC as a leaf's does (do-not-flag item 3): that took
a `t4d` 2 KiB get from 724 to 302 ns and a `t16d` 8 KiB get from 1.30 us to 271 ns.

**A nested cell is `Corrupt` at every value read** (`PageRead::value`) and every key-tail read
(the cursors' `key_tail`, behind `current_with_key_tail` and `seek_with_key_tail`): no value a
read returns is one. Only a dup table's outer tree holds nested cells, and its reads match the
cell itself (see [Dup tables](#dup-tables)); a cursor over a dup table is `TableKind`.

**CRCs are not recomputed on every read** of a tree page. That would cost a pass over the page
each time. The CRC is verified in these places instead:

- by copy-on-write, before a page is copied (unless this writer stamped it itself), together with
  a full `page::validate`;
- for the tree roots at open;
- for free-list leaves before their pages are reused;
- for an overflow run of more than one page, on every read of its value (above);
- by `Env::check`, for every page and every run.

The public `Cursor` remembers the leaf it is at. A step inside that leaf skips the fetch: the page
passed the per-fetch checks when the cursor entered it, and the transaction's pages cannot change
while the cursor borrows it. Entering a leaf always runs the descent's checks, and the first read
past a seek's landing entry the full range check (above).

The owned `RoCursor` cannot hold a borrowed page between calls, so each `next` and `prev` fetches
the page of the leaf it stands on again, by page number, through `Snapshot::page_again`: a plain
slice of the mapping without the per-fetch checks, which that page passed when the cursor entered
it (a snapshot's pages do not change while it lives). A seek from an entry starts from that
leaf, fetched the same way (above). Every other move, and every leaf it enters, runs the same
checks as `Cursor`'s.

---

## Open and recovery

`Env::open` (`env.rs`) runs these steps:

1. **Validate the options.** `options.page_size` must be a valid page size, else
   `InvalidPageSize`, and `options.max_growth` at least `MIN_GROWTH` (1 MiB), else
   `InvalidMaxGrowth`, even for an existing file. Nothing is created or opened before.
2. **Create the file if it is missing or empty.** Both metas (txn 0, page count 2, empty trees)
   are written with `pwrite` to `<path>.creating`, which is locked and truncated first. Then
   `fdatasync_file`, rename over the path, and `fsync_directory`. If another creator holds
   `<path>.creating` locked, the open fails with `Locked` at once, without waiting: that creator
   holds the database. If another handle created the file meanwhile, its file is kept, and the
   unused `<path>.creating` is removed when the name still holds the empty file this open locked.
3. **Lock.** `flock(LOCK_EX | LOCK_NB)`, else `Locked`. A second open in the same process fails
   too. The locked file must be the one the path names, and non-empty. A creator may rename its
   new file over the path between this open and its lock, so the open is retried up to three
   times, then fails with `Locked`.
4. **Choose a meta** (`choose_meta`):
   - Meta 0 is read at offset 0. If it is valid, meta 1 is read at its page size.
   - If meta 0 is invalid, each P in 4K..64K is probed for meta 1 at offset P, accepted only if its
     own page size is P. A meta 1 valid at two page sizes is `Corrupt`.
   - The valid meta with the higher txn wins. Both valid requires equal page sizes, and equal txns
     must be byte-identical.
   - An `UnsupportedVersion` record in either slot refuses the file.
   - No valid meta is `Corrupt` if any slot carries the magic, else `NotTnLmdb`.
5. **Txn check.** A meta txn of `u64::MAX` (no next txn) is `Corrupt`.
6. **Length check.** A file shorter than `page_count × P` is `Corrupt`. Open never falls back to
   the older meta.
7. **`fsync_file` once.** A crash may have left a meta in the page cache whose msync never
   finished. What open reads must be durable before anything is built on it.
8. **Map** inside a reservation of `max(map_reserve, file length)`, advised `MADV_RANDOM` unless
   `readahead` is set ([Platform notes](#platform-notes); do-not-flag item 16); a remap past the reservation is advised the same way.
9. **Check both tree roots** (`check_root`). Each root must be a tree page in range that passes
   its full CRC and `page::validate`. Its header must name it, with `txn as u32 == root_txn` and
   txn no later than the meta's. It must be a leaf exactly when the height is 1. The height is
   already capped at 48 by the decode.

**Open never writes to an existing file.** Every failure above leaves the file unchanged.
Recovery is choosing a meta: there is no repair, no reinitialisation of a non-empty file, and no
fallback past a short file. A torn or invalid slot counts as the lowest and is the first one a
later commit overwrites.

**Known limit: open cannot see a meta writeback error reported earlier.** If a meta msync fails
(say EIO), the environment latches. On Linux the page cache may keep the new meta in a page now
marked clean while the disk holds the slot's old bytes, and the error is reported once, to the
file already open, not to one opened later (the "fsyncgate" pattern, common to engines that
rely on the page cache for writeback). A reopen
in the same boot then reads the cached meta, and step 7's `fsync_file` succeeds without writing
it, so later commits can build on a meta the disk does not hold. A read-only open
(`Env::open_read_only`) in the same boot reads the same cached meta, so it can report a state the
disk does not hold. After a write error the node stops; do not reopen the file in the same boot,
for writing or for reading. Reboot first, or copy the file to a new one. `Env::check` reads the
same cache and cannot tell.

### Read-only open

`Env::open_read_only(path, options)` opens a file no writer has open, for a process that only
reads it (a tool run while the node is stopped). It shares the steps of `Env::open` that read,
in the same code (`choose_state`, `checked_snapshot`), and skips every one that writes:

1. **Validate the page size**, as `Env::open` does. Of the other fields of `EnvOptions` only
   `readahead` applies (step 8).
2. **Never create.** A missing file is `Io(NotFound)`. An empty file is `NotTnLmdb` (step 4 finds
   no meta in it). No `<path>.creating` file is made.
3. **Lock shared** (`open_shared`): `O_RDONLY`, then `flock(LOCK_SH | LOCK_NB)`
   (`sys::flock_shared`), else `Locked`. Read-only opens coexist; a writer's `LOCK_EX` excludes
   them, and they exclude it (its open is `Locked`). The locked file must be the one the path
   names, retried up to three times as in step 3 above.
4. **Choose a meta**, 5. **txn check**, 6. **length check**: exactly as above. A damaged newest
   meta falls back to the other valid one.
7. **No fsync.** Nothing is built on what this open reads. A meta that a crash left in the page
   cache unsynced is the state it reads, and a power loss afterwards can still lose that state;
   the next `Env::open` then makes durable whichever meta it reads.
8. **Map read-only:** one `PROT_READ` shared mapping of the file's length (`ReadOnlyFile`), with
   no reservation and no growth, advised as in `Env::open`'s step 8. Readers reach it through the
   same `MapView`.
9. **Check both tree roots**, as above.

The environment has no writer. `begin_rw` and `try_begin_rw` return `ReadOnly`, which does not
latch, whether or not the environment has latched. `sync` returns `Ok(())`, and the drop does
nothing. Every reader sees the state opened at. Gets, cursors, overflow values, `table_stat`,
`Env::check` and `stat` work as in any environment. Damage a read or `check` finds latches the
environment as usual (`failure()` reports it). The file is never written or synced: its bytes
and its modification time are unchanged after the handle drops, and a mode-0444 file opens.

The file must keep its length while it is open. A foreign process truncating it (outside the
crate's single-process model) makes the next read past the new end a `SIGBUS`, under the
read-only mapping as under a writer's. A truncation before the mapping is safe: reads are bounded
by both the published page count and the mapped length (`MapView::slice`), so they find
`Corrupt`.

### Close

An environment closes when its last `Env` clone and transaction drop (`EnvInner::drop`), in
this order:

1. **A crash stops here** (the test-only `Env::crash`): no sync, no trim.
2. **A latched environment stops here:** its newest state may not be durable and its view of the
   meta slots may not match the disk, so it neither syncs nor trims.
3. **A read-only environment stops here:** it has no writer, and never writes its file.
4. **`NoSync`: the final sync** makes the newest state durable (`Env::sync`'s steps). A failure
   is logged (`tracing::error!`) and latches, and the close stops: no trim.
5. **The trim** (`Writer::trim_tail`): the file is truncated to the most pages any state a meta
   slot holds names, never fewer than the newest state's, times P, rounded up to the OS page,
   when that is shorter than the file (`PageFile::trim_to`, a `set_len`). Each valid slot's
   record is read back and must decode to the txn the writer holds for it, else the trim is
   skipped; a torn or invalid slot names no state and is left out. The page count never shrinks
   from one state to the next, so the newest state's is the largest. No sync follows: the pages
   kept are durable already, and a power loss that undoes the truncation brings back the longer
   file, which opens as before (step 6 of the open accepts any length of at least the meta's
   pages). A failure is logged (`tracing::warn!`, target `tn_lmdb`) and does not latch: the file
   only keeps its tail. The mapping stays until the file drops: every snapshot reads at most its
   own `page_count × P` bytes, all below the new length.

---

## Failure latch

`latch.rs` keeps the first cause and never clears it, and only the first cause is logged
(`tracing::error!`, target `tn_lmdb`).

| latches (later writes and commits return `Failed`) | does not latch |
|---|---|
| any error inside `commit` or `sync` (`fail`; `fail_damaged` for `Corrupt`) | `KeyTooLarge`, `KeyTooShort`, `ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, `ReadOnly` |
| an I/O or growth error in a write (`fail`); a failed msync also poisons the file, which is never retried | `TableNotFound`, `StaleTable`, `TableKind`, `InvalidTableName`, `InvalidDupOptions`, `WrongTxn` |
| a panic part way through a write, commit or sync (`UnwindLatch` guard) | a miss (`Ok(None)`) |
| `Corrupt` from any read, get or cursor move (`fail_damaged`) | errors from `Env::open` (there is no environment yet) |
| a failed `Env::check` | |

After a latch, `begin_rw` and `try_begin_rw` return `Failed` (`ReadOnly` in a read-only
environment), and reads continue on the published snapshots. `Env::failure()` reports the cause.
A latched environment skips its close: no final sync (NoSync) and no trim ([Close](#close)).

---

## Concurrency rules

| type | thread safety |
|---|---|
| `Env` | `Clone + Send + Sync`. Clones share one environment, which closes when the last clone and the last transaction drop. |
| `RoTxn` | `Send + Sync`. It holds an `Arc` of the environment and of its snapshot, so the mapping outlives it. |
| `RwTxn` | `Send + Sync`. It owns the boxed writer state, taken through a `Mutex` + `Condvar` hand-off. |
| `Cursor<'t>`, `RwCursor<'t>` | borrow their transaction (`Cursor` is `Send + Sync`). |
| `RoCursor` | `Send + Sync`, no lifetime. It owns an `Arc<RoTxn>`, so the snapshot stays readable, and its pages unreused, until the last cursor and handle of that transaction drop. |
| `TxnCursor` | `Send + Sync`, no lifetime, and no borrow of its `RwTxn`: each call takes the transaction (`&RwTxn` for a move, `&mut RwTxn` for a write), so any number of cursors share one write transaction. A caller that shares the transaction behind handles whose methods all take `&self` wraps the `RwTxn` in a `Mutex` and holds the guard for each call; the entries a move returns borrow the guard. |

- `begin_rw` blocks while another write transaction is open, from any thread. `try_begin_rw`
  returns `None` instead. The writer returns to the environment when the `RwTxn` drops (commit
  consumes it).
- **Deadlock note:** a thread that holds an `RwTxn` and calls `begin_rw`, or **`Env::sync`**,
  waits forever. `sync` needs the writer state, as `begin_rw` does. Commit or drop the transaction
  first.
- Readers never block the writer and the writer never blocks readers. A long-lived reader only
  holds back page reuse, so the file grows instead.
- Multi-process access alongside a writer is out of scope. The `flock` makes a second process's
  open fail with `Locked`. Read-only opens (`Env::open_read_only`) take the lock shared, so any
  number of processes may read a file no writer holds.

---

## Deletes and merging

A delete (`write::delete`) runs as a put does, in three steps: `plan_delete` descends with the
writer's checks and decides the whole change without writing, `reserve` allocates the copies of
the published pages on the path that stay in the tree, and `apply_delete` makes the change. A
failed allocation (a full disk, say) therefore leaves the tree exactly as it was: the same root
handle and every reachable page byte for byte.

The delete descends once. Its descent (`descend_write` with `Verify::Later`) makes every check of a
put's except the verification of the published pages it passes (`verify_published` and
`check_cells_naming_this_txn`). A key that is not there ends there, after the check a lookup
makes of a miss (`read::check_miss`: the keys either side of where it would go lie inside the
leaf's separators), so it copies, verifies, allocates and frees nothing, and leaves the
allocator's epoch as it was. A hit then verifies the published pages on the path (`verify_path`)
before it reads them further, as a put's descent verifies them as it goes.

**Through the path cache.** A table's delete (`RwTxn::delete`, both cursors' `delete_current`)
goes through the table's path cache (`write::delete_cached`, the twin of `put_cached`; do-not-flag
item 12). While the table's root is trusted and the allocator's epoch is the one the cache was
filled in, a key inside the cached leaf's range `[lo, hi)` is deleted in that leaf with no descent
(`delete_in_cached_leaf`) when the descent's change would be that leaf's alone: the value is
inline, the leaf keeps another entry, and no merge is due (`merge_due`, below). That is exactly the
descent's `Leaf` shape with no merge and no copy (every page on a cached path is private), so the
bytes, the handle and the epoch are the descent's; the cell leaves behind the one header parse
(`page::LeafMut::delete`). A key inside the range that the leaf does not hold is a miss there, with
no descent: no other leaf can hold it. Every other delete descends (`delete_from_root`): a value in
an overflow run (the run is freed with the tree's other changes), the leaf's last entry (an unlink,
a collapse or an empty tree), a merge due, a key outside the range, a moved epoch or an untrusted
root. A delete through a positioned detached cursor that misses the cache descends along the
cursor's own path, with every check of the descent kept ([Detached cursors](#detached-cursors-txncursor),
do-not-flag item 27). The descent records its path straight into the cache's buffer (`PathCache::lend_path`, which
empties the cache) and refills the cache after a `Leaf` delete that merged nothing, with the leaf
and the path to it, private by then, and the epoch its copies left. A miss refills it only when
every page on its path is this transaction's own, from a trusted root: nothing was copied or
verified, and the epoch did not move. An unlink, a collapse, an emptied tree and any merge free
pages and leave the cache empty. So does a miss over published pages, since the descent borrowed
the cache's buffer: the next put or delete in the table descends once and refills it, which costs
speed only, and rarely. The delete reports the deleted entry's old slot in the cached leaf, now its
follower's (`write::Removed::slot`), which a `TxnCursor` places itself from
([Detached cursors](#detached-cursors-txncursor)).

**Shapes.** The entry leaves its leaf, and the leaf's fate picks the shape (`DeleteShape`):

| shape | when | what leaves the tree |
|---|---|---|
| `Leaf` | the leaf keeps other entries | nothing: only the leaf changes |
| `Unlink(d)` | the leaf empties | the leaf and the single-child branches above it, up to path step `d`, the deepest branch with another child, which loses the child the path took |
| `Collapse` | the leaf empties and the root is left with one child | the root, the branches on the path and the single-child branches below the remaining child (`collapse_to`); the first page below them that is a leaf or has two children or more becomes the root |
| `Empty` | the leaf held the tree's last entry | every page: the handle becomes `TreeRoot::EMPTY` |

Emptied pages are never copied. They leave the tree as they are: a published one is freed, and a
private one is reusable at once. A deleted value's overflow run leaves the tree with them, checked
as a free checks a run (`leaving_run`: its header, identity and provenance, without the CRC, since
the value is not read), as `clear_table` and `drop_table` check theirs.

**Merging.** After a `Leaf` or an `Unlink` change, `plan_merges` looks at the page the change
left: the leaf without the entry, or the branch without the child. While that page is not the root
and holds under a quarter of `U` (`4·used < U`, where `used` is the bytes of its cells and slots,
`View::used`), it absorbs a same-parent sibling that fits: left neighbours first, while they fit,
then right ones. A leaf pair fits when `used + used(S) <= U`. A branch pair also takes the
parent's key for the right page of the pair, which comes down as that page's cell-0 key (empty
until then), so it fits when `used + used(S) + len(key) <= U`. The page keeps absorbing until it
reaches a quarter or no neighbour fits. A page a delete changes is therefore never left under a
quarter beside a sibling it would fit into: it is **merge-closed**. The test that starts the
search, not the root and under a quarter, is `merge_due`, the one merge rule of both delete paths:
`plan_merges` asks it of each page up the path, and the cached delete asks it of the leaf it would
leave (its bytes without the entry, `leaf_used_after_delete`, which `plan_delete` uses too) and
descends when it holds. So a delete in the cached leaf never skips a merge the descent would have
made, even where no sibling turns out to fit: a small rightmost leaf after an ascending load
descends on every delete.

- **Cascade.** Each absorption takes the right page's cell out of the parent (when the sibling was
  on the left, the parent's left cell of the pair is first pointed at the survivor,
  `branch_set_child`). The parent, having lost cells, gets the same test one level up.
- **The root gives way.** A root branch the merges leave with one child gives way to it: that
  child, the page on the path, becomes the root, and the height drops by one. The old root leaves
  the tree as it is, not copied (`DeletePlan::merged_root`).
- **Merge only.** Siblings never borrow entries from each other, and nothing is redistributed
  (do-not-flag item 9). Pages no delete changes are not revisited, and pages are reused through
  the free list, so there is no compaction pass either. Why:
  - **A merge never grows a separator, so it never splits.** It only takes cells out of parents
    (the right page's cell of each pair, after the left cell is pointed at the survivor when the
    sibling was on the left) and brings an existing separator down as the right page's cell-0
    key, whose own key was empty and unused. No new or longer key enters any parent, and the
    merged page is sized to fit before anything changes. Borrowing would move a different key up
    as the separator, which can be longer than the one it replaces, so a delete could overflow a
    parent and split, and its reservation would need a split cascade's worst case. Merge-only
    keeps it to the path's copies.
  - **A quarter is the merge threshold.** It leaves a wide gap below the half-full pages a split
    makes, so a page does not swing between splitting and merging, and it is enough in practice: after
    90% of a random fill is deleted at random, the leaves stay 0.40–0.47 full, against about 0.08
    without merging (`deleting_90_percent_leaves_no_page_merge_open`).
  - **Single-child branches are still possible**, as they were before merging. A branch that an
    unlink or a merge leaves with one child stays so when no sibling fits into it; it is a valid
    page (`page::validate` asks a branch for one child or more) and reads route through it. Only
    the root gives way to a single child: `Collapse` when an unlink leaves it so, and
    `DeletePlan::merged_root` when merges do, so `Env::check` refuses a root branch with one
    child.
- **A merge allocates nothing.** The survivor is always the page on the path, which copy-on-write
  copies anyway (`make_path_writable`). The sibling's cells move into it (`SplitBuf::merge` plans
  the merged page from both, `SplitBuf::write_merged` lays it out over the survivor), and the
  sibling leaves the tree as it is, never copied. So the reservation is the path's copies alone,
  and a failed allocation leaves the tree identical with merges as without.
- **Overflow cells move as cells.** A leaf merge moves a 12-byte overflow cell like any other. Its
  run stays where it is, and no run changes hands or counters.

**Checks on siblings.** A sibling's cells enter a page this transaction trusts, so the sibling is
checked as an untrusted page on a descent is:

- Every neighbour read for a fit test, absorbed or not, first gets the cheap checks a descent makes
  of every page (`sibling_view`): a page private to this transaction under a parent it did not
  write is `Corrupt`, the header must name the page and the txn the parent's cell names, the page
  must be a leaf exactly at the leaf level, and it must not be empty. These find damage even in a
  neighbour the fit test then refuses.
- The sibling a merge absorbs also gets the rest (`check_sibling`): unless this writer stamped it,
  `verify_published` (the CRC and `page::validate`) and the copy-time rule
  `check_cells_naming_this_txn`, which covers its child cells and its overflow cells; and its keys
  must lie within the intersection of its ancestors' separators (`keys_in_range`). A branch pair
  also gets the seam check: the right page's key 1 must sort strictly above the key coming down.
  The range check passes a key 1 equal to its lower bound, and the merged page would then hold that
  key twice.
- An absorbed branch sibling also has its child-0 chain descended to the leaf level
  (`check_sibling_chain`), each page with a descent's checks: not private (a published page names
  none), the page and txn its parent names, a branch above the leaf level and a leaf at it, not
  empty, verified unless this writer stamped it, and its keys within the bounds tightened at each
  step. A branch's own keys bound its children but say nothing of a single child's subtree, and
  `sibling_view` tells a leaf from a branch, not one level from another, so without the descent a
  cell redirected to a single-child branch from elsewhere, or to a branch of another level, would
  pass and the merge would leak a subtree or mix levels. Two cells naming one sibling cannot both
  pass (their ranges are disjoint and the chain ends at a non-empty leaf), so no sibling is
  absorbed, and freed, twice. Branch merges are rare, so the chain's few page reads cost little.
- A refused neighbour is read no further than its header, so damage only its CRC finds (a flipped
  value byte, say) is met at its first real use, as a reader's lazy reads meet it.

All of this runs in `plan_delete`, before anything changes: damage is `Corrupt` with the tree as it
was, and the environment latches.

**Counters.** `handle_after` computes the handle a delete leaves before anything changes: one
entry fewer, the deleted value's run uncounted, the emptied pages uncounted and, for the merges
(`Merged`), one leaf or branch fewer per absorbed sibling and one branch and one level fewer for a
root that gave way. Each subtraction is checked, and a handle whose counters cannot allow the change
is `Corrupt`.

**What else stays true.** A merge never lengthens a separator or adds one to any page, so it never
splits (do-not-flag item 9). The path cache stays sound because every merge frees a page, which
moves the allocator's `epoch` (do-not-flag item 12), and a merging delete leaves the cache empty.
`RwCursor::delete_current` finds its place again by key, so after merges it still lands on the
deleted entry's follower. `TxnCursor::delete_current` lands there too: from the cached path after a
delete that merged nothing, else by key.

**Fill.** In `deleting_90_percent_leaves_no_page_merge_open` (4 KiB pages), the pages are about
0.69 full after a random fill, and the leaves 0.40–0.47 full after 90% of the keys are deleted at
random. Without merging, the same deletes leave the leaves about 0.08 full, in about six times the
pages.

---

## Dup tables

A dup table holds rows keyed `k ‖ s`: an outer key `k` of exactly `key_len` bytes, then a subkey
`s` of at least one byte. Its outer tree holds one entry per outer key: a packed set holding the
key's rows in the cell itself while they encode within the table's bound (`DupOptions::packed_max`,
in bytes), else a nested cell naming the key's nested tree, which holds the subkeys and their
values (inline or in overflow runs, as any table's). A table with `packed_max` 0 never packs:
every outer key holds a nested tree of at least one page, however few rows it has.

**Kinds.** A table is plain or dup for its whole life, by its catalog record; a `Table` handle is
`{id, name}` for both, and the kind comes from the record at each resolution, so a stale or
re-created table is refused by id, never by a cached kind. A call that needs the other kind is
`TableKind`, checked before anything changes, with no latch:

| call | on a plain table | on a dup table |
|---|---|---|
| `create_table(name)` | its handle | `TableKind` |
| `create_dup_table(name, options)` | `TableKind` | its handle when the options are the table's, else `TableKind` |
| `get`, `put`, `insert`, `append`, `delete`, `table_stat`, `drop_table`, `clear_table` | the plain operation | the dup operation (below) |
| `delete_prefix(k)` | `TableKind` | the dup operation (below) |
| `cursor`, `cursor_owned`, `cursor_mut`, `cursor_detached` | a cursor | `TableKind` |
| `dup_cursor_owned`, `dup_cursor_detached` | `TableKind` | a dup cursor ([Dup cursors](#dup-cursors)) |

**Options and limits.** `create_dup_table` refuses a `key_len` of 0 or past
`max_dup_key_len = U / 4 − 42` with `InvalidDupOptions`: the outer cell, the key and a
32-byte nested root, must fit a quarter-page entry (975 bytes at 4 KiB pages, 4047 at 16 KiB,
16335 at 64 KiB). `packed_max` is 0 (never pack), or 7 (`PACKED_MIN`, the smallest set) to
`max_entry_value(key_len)`, the longest value a quarter-page entry of the key takes (987 bytes at
a 20-byte key and 4 KiB pages), or `DupOptions::PACKED_DEFAULT` (`u16::MAX`, never a limit itself), which
asks for that most; anything else is `InvalidDupOptions`, with nothing changed and no latch.
`DupOptions::packed(key_len)` asks for the default. `create_dup_table` records the resolved number
(`DupOptions::resolved(page_size)` computes it without a file), and `table_stat`'s `dup` reports
it, so asking `PACKED_DEFAULT` or its resolved number of an existing table gives the same handle
and another number is `TableKind`. A point operation's logical key `k ‖ s` of `L` bytes first passes the plain limits
(`Geometry::place(L, value.len())`: `KeyTooLarge`, `ValueTooLarge`), so a dup table refuses exactly
the entries a plain table keyed `k ‖ s` refuses; then `L <= key_len` (no subkey) is `KeyTooShort`,
for `get` and `delete` too (a get of `k` alone is a caller's mistake, not "the first row of `k`").
A `get` or `delete` of a key over `max_key` misses, as on a plain table. The split is positional:
`k` is the key's first `key_len` bytes, so a wrong outer width cannot be expressed.
`delete_prefix` takes `k` alone: shorter than `key_len` is `KeyTooShort`, longer `KeyTooLarge`.
A write's checks run in this order, all before anything changes: the size limits (checked before
the table resolves, as for a plain table), the table's resolution (`TableNotFound`, `StaleTable`),
the kind where the call takes one kind only (`TableKind`), `KeyTooShort`, then the operation's own
refusals (`KeyExists`, `AppendOutOfOrder`).

**Point operations.** `get(k ‖ s)` finds `k`'s outer cell, then `s` in its nested tree, and
borrows the value from the mapping as a plain get does. `put`, `insert` and `delete` write the
key's nested tree through the copy-on-write code every table uses; `insert`'s `KeyExists` is the
nested tree's. `append` orders by the logical key: an outer key before the table's last is
`AppendOutOfOrder`; the last outer key appends to its nested tree (whose own rule refuses a subkey
not past its last); a later one starts a new nested tree. A put of a new outer key builds its
nested tree first, then writes its outer cell; a delete that empties a nested tree removes its
outer cell, and the tree's last leaf is freed. `delete_prefix(k)` frees `k`'s whole nested tree in
one walk, which collects every page and run before it frees any (damage is refused first), removes
the cell and returns the rows removed (0, and no change, when `k` holds none). On a packed key the
same calls work on its set, below.

**Packed sets.** In a table whose `packed_max` is above 0, a new outer key whose first row encodes
within the bound (`2 + 4 + |s| + |v|` bytes, [Packed sets](#packed-sets)) is created as a packed
set in its outer cell, and a key past the bound gets a nested tree as in a table that never packs.
Every write that changes a packed set (a put, insert or append of a row, an upsert of one, a
delete) validates the whole set first, rebuilds it with the change and rewrites the key's outer
cell at once (an ordinary leaf replace, split or merge of the outer tree). A get binary-searches
the set and borrows the value from the mapping.

- **Promotion.** A write whose set would exceed `packed_max` bytes (a new row, or an upsert that
  grows a value) moves the key's rows, the change included, to a new nested tree built by appends
  in subkey order, and replaces the outer cell with the tree's root. The tree is the
  transaction's own (trusted, in the map), as a nested create's is.
- **Never demote.** A nested key that shrinks stays nested at any row count (do-not-flag item 24).
  Deleting its last row removes the cell; a later put creates the key afresh, packed if its first
  row fits.
- **Deletes.** A delete of a set's only row removes the cell; of another row, rewrites the set
  without it. `delete_prefix` of a packed key removes the cell and returns its rows; it frees no
  page (a set owns none).
- **No map entry (invariant P).** A packed key has no entry in the working map (below): its cell is
  rewritten at each write, so the cell is the truth for it, and only nested keys wait for the
  flush (do-not-flag item 22).
  Debug builds assert it in `txn::dup_entry`, and a free walk's hit count enforces it in every
  build.
- **The write stamp.** Every packed change (a create, rewrite, row delete, cell removal or
  promotion) moves the table's write stamp, even an in-place rewrite that leaves the outer root's
  bytes as they were: it is the only sign a dup cursor gets of a change to a key with no map
  entry. A miss, a refused write and a get move nothing.

**A packed write goes to the cell at once** (do-not-flag item 22):

- Why: the copy is the same either way. A set is immutable and at most `packed_max` bytes,
  so a set held in the map would still be rebuilt on every write; writing it at once adds
  only the outer cell's replace, which after the transaction's first write to that leaf is an
  in-place replace in a private leaf. Nested roots are deferred (the working map, below) because their cell
  changes on every nested write of a big key; a packed key has few rows by definition.
- Correctness: a set held in the map would leave a stale but valid-looking cell until the
  flush, and nothing would catch a reader that forgot the map (a stale nested root names
  private pages, a stale set names none). Written at once, the cell is the truth for every
  packed key, so reads, the dup cursors, the free walks, `delete_prefix` and `Env::check`
  need no packed overlay, and the commit's flush stays nested-only.
- The cost: each write to a packed key copies its set (at most ~1 KB at 4 KiB pages), so a
  key's whole packed phase in a bulk append copies at most ~17 KB before it promotes.

**The working map and the flush at commit.** A write transaction keeps, per dup table, a map from
each outer key whose nested tree it has loaded or written to that tree's working root, the root's
provenance (written by this transaction or not), whether the outer cell still holds an older root,
and the tree's own path cache. A nested write changes only the nested tree and its map entry; the
outer cell is rewritten once, at commit, before step 1 (the catalog's roots): every changed entry,
in key order, replaces its cell in place (a 32-byte value over a 32-byte value never splits), so a
key written many times in a transaction rewrites its outer path once. Creating or removing a key
changes the outer tree at once (an insert or delete of its cell). Until commit the map, not the
outer cell, is the truth for its key: every key whose tree the transaction created or changed has
an entry from that write until commit or until the key is removed, entries are never evicted, and
every reader of an outer cell inside the transaction (`get`, the free walks) looks in the map
first. A map hit is always a nested key, by invariant P. An abort drops the map with the transaction; the flush does no I/O, so a process death
anywhere before the meta's msync recovers to the previous state, as always. A flush error (damage
on an outer path) fails the commit and latches.
Debug builds assert after the flush that every entry's cell holds its root.

**Trust.** The writer's provenance trust (do-not-flag item 11) extends to nested
trees with these rules:

- A nested root is trusted only from its map entry, and only after this transaction wrote it. A
  root read from an outer cell is untrusted: its pages get every published-page check.
- An outer cell that names a page private to this transaction, with no map entry for its key, is
  `Corrupt`: only a forged cell can name one (the transaction's own trees are all in its map).
- Copy-on-write of a published outer leaf, and a merge absorbing one, checks its nested cells as
  it checks branch pointers: a cell whose root carries this transaction's id must name a live
  page published by an earlier transaction, else it is `Corrupt`.
- A free walk (`drop_table`, `clear_table`) takes each nested root from the map, else from the
  cell, untrusted. Every map entry must be met by the walk (else `Failed`, a broken invariant).
- Splits and merges of the outer tree move nested cells as raw cells: the map is keyed by outer
  key, not by position, so a moved cell keeps its entry. A cell the transaction wrote may name a
  superseded root until the flush; that is expected (do-not-flag item 18).
- A packed set carries no trust and needs none: it names no page. Its bytes are unchecked until
  each use runs the codec's checks; copy-on-write and merges move it raw and skip it
  (`Cells::Packing`, do-not-flag item 25); a free walk validates it whole and counts it.

**Totals and `TableStat`.** The catalog record keeps a dup table's totals: `nested_trees`,
`nested_entries`, `packed_rows` (the rows in packed sets) and the nested trees' branch, leaf and
overflow pages; the outer `root` counts the outer tree's own pages and entries, so the packed sets
are `root.entries − nested_trees` (no field of their own). A set owns no page: its bytes are in the
outer leaves the outer counters already count. Every write updates them in memory at once,
with checked arithmetic (a total that would underflow or overflow is `Corrupt`: only a damaged
record gets there), so a write transaction's `table_stat` includes changes not yet flushed.
`TableStat` is O(1) for both kinds:

| field | plain | dup |
|---|---|---|
| `entries` | the tree's entries | `nested_entries + packed_rows`: every row `k ‖ s` |
| `height` | the tree's height | the outer tree's height |
| `branch_pages`, `leaf_pages`, `overflow_pages` | the tree's | the outer tree's plus the nested trees' |
| `nested_trees` | 0 | one per nested outer key (a packed key counts none) |
| `dup` | `None` | `Some(DupOptions)`, `packed_max` resolved: the O(1) way to learn a table's kind |

**Drop and clear.** `drop_table` and `clear_table` of a dup table walk the outer tree and every
nested tree in one walk sharing one page set, collecting every page and run before any is freed:
every outer leaf is read and held to the outer-cell rule, each packed set is validated whole
(`write::PackedLimits`) and its rows counted, a page or root reached twice is `Corrupt`, and the
trees and sets walked must sum to the record's totals (`packed_rows`, and nested trees plus packed
sets equal to the outer entries; else `Corrupt`, naming the field), so a damaged table frees
nothing. A dropped table's published pages are reused past the
reader gate as any freed page is; a held `RoTxn` keeps reading them until it drops. `clear_table`
leaves an empty dup table of the same options (`packed_max` kept, `packed_rows` 0).

**What `Env::check` verifies** for a dup table, beyond a plain table's checks of its outer tree:
the record passes `usable`; every outer key is `key_len` bytes; every outer cell is a nested cell
or, with `packed_max` above 0, a packed set that passes `packed::validate` (an overflow outer cell
is `Corrupt`, and so is an inline one with `packed_max` 0); each nested root decodes and its tree passes a
plain table's whole walk (identity, CRC, validation, kinds, ranges, its counters against its pages,
runs with their full CRC), with subkeys of 1 to `max_key − key_len` bytes and no nested cell; one
owner claims the outer and nested pages, so a root or page named twice is a double claim; and the
walked trees' counts, entries and pages equal the record's totals, the packed sets' rows equal
`packed_rows`, and the nested trees plus the packed sets equal the outer tree's entries. A table's `TableCheck` reports its rows, its outer and nested pages (runs
included) and its outer height, so `entries` equals `table_stat`'s for both kinds.

**A subkey of another length** (0 bytes, or past `max_key − key_len`) is only ever forged: a get
that misses beside one (`read::find_nested`) and a dup cursor that lands on one are `Corrupt` at
first use, but a write beside one is left to `Env::check` (an empty key is legal in a plain table,
so `page::validate` holds no leaf key to a minimum). In a packed set the same first-use rule holds
at the granularity of the rows a read touches: a get checks the set's header and the rows its
binary search probes (a miss reads both neighbours), a cursor each row it steps to and its order,
so a bad row off a get's path is met by the next write to the key, a walk of it or `Env::check`,
each of which validates the whole set (do-not-flag item 23).

### Dup cursors

`RoTxn::dup_cursor_owned(self: &Arc<RoTxn>, &Table) -> DupRoCursor` and
`RwTxn::dup_cursor_detached(&self, &Table) -> DupTxnCursor` are the dup tables' cursors, the
owned and detached shapes of `RoCursor` and `TxnCursor`; a plain table is `TableKind` (as the plain
constructors refuse a dup table), and there are no borrowed dup cursors. Both walk the rows as
the logical keys `k ‖ s` in order, the rows of the composite twin (a plain table keyed `k ‖ s`
holding the same rows), and every call gives the result and the state the same call of a
`RoCursor` or `TxnCursor` gives on the twin: `first`, `last`, `next`, `prev`, `current`, `seek`
(first row `>= x`, `x` of any length) and the ends, a step crossing from a key's last row to the
next key's first as a plain step crosses a leaf. `seek_exact(x)` with `x.len() <= K` (`K` the
key width, `key_len`) always misses, with no read: no logical key is that short. What the twin's
cursor has no exact counterpart for:

- **Prefix steps by level.** `next_in_prefix(n)` and `prev_in_prefix(n)` keep the plain rules on
  logical keys. A step with `n < K` may cross to the neighbouring outer key; `n >= K` never leaves
  the key, since no other outer key shares its `K` bytes, so `n = K` walks one key's subkeys
  (the adapter's `next_dup`) and returns `None` at the key's edge.
- **Bounded seeks.** `seek_key(k)` goes to `k`'s first row, `seek_dup(k, s)` to its first row
  `k ‖ s'` with `s' >= s` (an empty `s` is `seek_key`), and `seek_dup_with_key_tail(k, s, t)` is
  `seek_dup` giving the row as `current_with_key_tail(t)` gives it (the adapter's
  `seek_by_key_subkey`). A `k` of
  another width is `KeyTooShort` or `KeyTooLarge` and changes nothing. A miss returns `None` and
  leaves the cursor unset, as `seek_exact`'s miss does, never on the next key's row, and reads no
  other key's nested tree (only the outer key its outer seek lands beside, for its width). On the
  twin a bounded seek is `seek(k ‖ s)` kept only if the row starts with `k`.
- **Key tails.** `current_with_key_tail(t)` and `seek_with_key_tail(x, t)` give the logical key
  and, for `t <= |s|` and an inline value, `KeyTail::Joined(s[|s|−t..] ‖ v)`, one slice of the
  nested leaf (the adapter's `t = |s|` is the stored row `s ‖ rest`, with no copy); for a value
  in a run, or `t > |s|`, `KeyTail::Apart(v)`, the caller joining the key's last `t` bytes. Both
  forms keep `KeyTail`'s meaning, so the joined bytes are the twin's. Every move has a tail
  twin that gives its row this way: `first_with_key_tail(t)`, `last_with_key_tail(t)`,
  `next_with_key_tail(t)`, `prev_with_key_tail(t)`, `next_in_prefix_with_key_tail(n, t)`,
  `prev_in_prefix_with_key_tail(n, t)` (`None` when the step does not move),
  `seek_with_key_tail(x, t)` and `seek_dup_with_key_tail(k, s, t)` (a `DupTxnCursor`'s take the
  transaction first). Each is its value move's code with the row's tail taken from the move's own
  read of the leaf, so a row is read once (the nested cell or the packed row read as `s ‖ v`, the
  value and every tail a slice of it); `current_with_key_tail(t)` reads the row the cursor is at
  again.
- **Borrows.** No page holds `k ‖ s` in one piece, so the key a call returns is the cursor's copy
  (rewritten past `k` on a move inside a key, its buffer reused); the value borrows the snapshot
  or the transaction. A `DupRoCursor`'s results borrow the cursor, as `RoCursor`'s do; a
  `DupTxnCursor`'s borrow the cursor and the transaction, where a `TxnCursor`'s borrow the
  transaction alone.

**Packed keys.** The walker (`tree::dup_cursor::DupWalk`) has a packed mode: at a packed key's row
it holds the outer path and the row's index in the set (`packed_rows`, `packed_at`, two `u16`s in
the walker's slack, so its size is unchanged) and no nested path. Each move re-reads the set from
the held outer leaf and decodes its header (O(1), `PackedView::new`); a step inside the key reads
the next row and compares its subkey with the one before, so a whole walk of a key checks its whole
order; a step past the set's edge crosses to the next key as from a nested key's edge. A seek that
stays in the key binary-searches the set, with no outer move and no descent; `seek_dup` past the
set's last subkey misses and unsets, as on a nested key. A packed row is contiguous `s ‖ v`, so
`current_with_key_tail` gives `Joined` from the set itself. A cell that is no longer a packed set,
or a set of another row count, under a cursor at one is `Corrupt` ("the packed set of key .. changed
under a cursor"): a snapshot cannot change, and a write transaction's change moves the stamp, which
makes the cursor reseek before any move. The nested key's path pays one predicted compare
(`packed_rows != 0`) per move.

**Seeks from the held leaves.** A seek from a row (a `DupTxnCursor`'s only while its table is
unchanged) starts from the leaves the cursor is at. One that stays in the cursor's outer key
(`seek`, `seek_exact` or `seek_with_key_tail` of a key starting with `k`, `seek_key(k)`,
`seek_dup(k, s)`) never reads the outer root: the nested seek runs in the held nested leaf when
the target is inside it, else from the nested root (`seek_key` always from the nested root), and a
`seek` past `k`'s last subkey steps the outer path on from its leaf. One to another key starts the
outer seek from the held outer leaf (inside it, the leaf alone; else the outer root), then
descends the new key's nested root. A cursor not at a row, and a stale one, descends both roots.
`ROOT_DESCENTS` counts both levels in debug test builds, so
`dup_seeks_from_random_positions_match_a_fresh_seek` pins each class: inside the held nested leaf
0, elsewhere in the key 1, another key inside the held outer leaf 1, outside it 2, as from the
roots. Landing on a packed key costs no nested descent: in the same key 0, another key inside the
held outer leaf 0, outside it or from the roots 1.

**The write cursor.** A `DupTxnCursor` is a `TxnCursor` on logical keys (its staleness, deleted
state, writes and refusals are [Detached cursors](#detached-cursors-txncursor)'s), with the
table's write stamp as its one staleness signal: every dup write moves it, a nested-only write
included, so any change to the table makes each of its dup cursors find its logical key again in
one reseek of both trees at its next call (back on its row, else on the first row after it in the
deleted state). A key's nested root comes from the working map while the map holds the key, never
from a cell the map supersedes, and from the cell otherwise.

- `delete_current` leaves the cursor on the follower (the key's next row, else the next key's
  first row, else after the end) in the deleted state, the deleted logical key in `deleted_key`;
  it places itself from the nested path cache when the key's tree kept the leaf, from the outer
  path cache when the delete emptied the key, else by key from both roots.
- An emptied key, a `delete_prefix` or a `clear_table` under a cursor leaves it, after the reseek,
  where the adapter's wipe loop (`seek(k)`, then `delete_current` or `next_in_prefix(K)` until
  `None`) leaves the twin's cursor: on the next key's first row (or after the end), deleted. The
  deleted keys differ (the loop's last row, the cursor's own row), but no call tells them apart:
  a prefix question with `n <= K` compares `k` either way, and with `n > K` no other key shares
  the bytes.
- A `put`, `insert` or `append` through the cursor into the key it is at, up to date, that
  creates no key keeps the outer path and places the nested path from the key's path cache; any
  other write finds the row again from both roots.
- Both levels of a write at the key the cursor is at descend along the cursor's paths
  (`DupWalk::hints`), as a `TxnCursor`'s write does (every check kept, a refusal per level): a
  `put`, `insert` or `append` through an up-to-date cursor at a row of the written key, and
  `delete_current`, pass the outer path's slots to every write of the key's outer cell and, at a
  nested key, the nested path's slots to the nested write. The nested root is still the working
  map's while the map holds the key; the cursor's nested slots only route, and are refused per
  level where the map's tree differs. The key's outer cell, when the map does not hold the key,
  is read from the outer leaf the cursor holds (`held_cell`: the page fetched with the per-fetch
  checks, the slot's key first, else, once the leaf has had the full range check, a search of the
  leaf when the key lies within its first and last keys; do-not-flag item 29), and from the outer
  root (`read::find_outer`) when that leaf lacks it.
- A write to a packed key, a promotion included, rewrites the outer cell, so the cursor finds its
  row again by key (the write reports no slot, `DupPlaced::slot` `None`); `delete_current` of a
  set's row places by key on the follower, and of a set's only row (the cell removed) from the
  outer path cache, as for an emptied nested key.

**Design choices** (do-not-flag item 21):

- A bounded seek's miss (`seek_key`, `seek_dup`, `seek_dup_with_key_tail`) leaves the cursor
  unset, as a `seek_exact` miss does, not after the end or on the next key's first row: the
  call asks about one key, landing on the next would read that key's rows.
- Any change to the table's write stamp, a nested-only write to another key included, makes
  every dup cursor of the table reseek, as every `TxnCursor` of a changed table does. A
  separate outer stamp (a reseek of the nested level alone when the outer tree did not
  move) waits for a profile that shows the reseeks.
- A `DupTxnCursor`'s results borrow the cursor as well as the transaction: the logical key
  is the cursor's copy (no page holds `k ‖ s` in one piece).
- `current_with_key_tail(t)` with `t` past the subkey gives the value `Apart`, not joined
  in a buffer: that would copy the value, and `Apart` keeps `KeyTail`'s
  meaning.
- There are no borrowed dup cursors (analogs of `ReadTxn::cursor` and `RwTxn::cursor_mut`)
  and no `delete_prefix` on the cursor: no caller needs them, and the transaction's
  `delete_prefix` plus the reseek leaves the wipe loop's state.
- `DupTxnCursor` shares `TxnCursor`'s checks (`Tracker`) but keeps its own `sync`,
  `refresh`, `retarget` and `seek_call`: they move the level-specific position, and a trait
  over both positions would sit on the plain cursor's hot path. Oracle 2b
  (`dup_txn_cursors_and_direct_writes_match_the_composite_twin`) holds the two copies to the
  same behaviour.

**Errors.** Damage a move meets (an outer key of another width, an outer cell that is not a nested
cell or, in a table that packs, a packed set, an empty or damaged nested root, a packed set that
fails a check, a subkey of another length, any check of a descent) is
`Corrupt`, latches, and leaves the cursor unset (a `DupTxnCursor` out of the deleted state too).
`WrongTxn`, `StaleTable`, the width refusals and refused writes change nothing and do not latch.

---

## Public API reference

| item | description |
|---|---|
| `Env::open(path, EnvOptions) -> Result<Env>` | open or create, then recover (see above) |
| `Env::open_read_only(path, EnvOptions) -> Result<Env>` | open an existing file no writer holds, read-only (see [Read-only open](#read-only-open)): a shared lock, the same meta choice and root checks, a read-only mapping; never creates, writes or syncs; `begin_rw`/`try_begin_rw` return `ReadOnly` |
| `EnvOptions { page_size, map_reserve, sync, readahead, max_growth }` | defaults: 4096, 1 GiB, `SyncMode::Durable`, `false` on Linux and `true` elsewhere, 1 GiB. `page_size` applies at create; an existing file keeps its own. `readahead: false` advises every mapping of the file `MADV_RANDOM`; `true` leaves the kernel's readahead (do-not-flag item 16). `max_growth` is the largest growth step: the file grows by doubling up to it, then linearly by it, each step preallocated ([Platform notes](#platform-notes)); under `MIN_GROWTH` (1 MiB, public) `Env::open` refuses it with `InvalidMaxGrowth`, before touching the file, and `open_read_only` ignores it. |
| `MIN_GROWTH` | the smallest growth step, 1 MiB, and the least `EnvOptions::max_growth` an open accepts, for a caller that checks its own setting first (the reth adapter re-exports it) |
| `SyncMode::{Durable, NoSync}` | durable at commit, or at the next `Env::sync` or drop |
| `Env::begin_ro() / begin_rw() / try_begin_rw()` | start transactions: an `RoTxn` (one committed state of every table, kept intact while it lives), an `RwTxn` (`ReadOnly` in a read-only environment) |
| `Env::oldest_reader_age() -> Option<Duration>` | how long the oldest live reader has held a superseded snapshot (pages freed after it wait for it to end, so the file grows meanwhile); `None` when every reader holds the current snapshot, and in a read-only environment. Lock-free; refreshed at each commit (see [Readers and snapshots](#readers-and-snapshots)) |
| `Env::sync()` | make NoSync commits durable (a no-op when nothing is pending, and in a read-only environment) |
| `Env::check() -> CheckReport` | full structural check plus exact page accounting, each root's entry count and page counters compared with its tree. `CheckReport` gives the state's txn id and page count, the catalog, table, free-tree and free pages, the free records, and a `TableCheck { entries, pages, height }` per table. |
| `Env::stat() -> EnvStat` | `page_size`, `txn_id`, `page_count`, `tables` |
| `Env::free_pages() -> u64` | the pages the newest committed state's free list holds (space the file can reuse before it grows; the reth adapter reports it as reth's `db.freelist` gauge): every free record read with the allocator's checks, so damage is `Corrupt` and latches |
| `Env::io_stats() -> IoStats` | the writer's I/O and allocation counters (see [I/O counters](#io-counters) below): in memory only, cumulative since this open, all zero in a read-only environment; published at the end of every commit, abort and `Env::sync`, read with one short lock of that copy (never a wait for an open write transaction) |
| `IoStats::since(&earlier) -> IoStats` | the difference of two reads: each counter's, saturating, but `max_gate_lag`, a maximum since the open, which is the later read's |
| `Env::failure()` | the latched cause, if any |
| `Env::max_key_len() / max_value_len(klen)` | the entry limits of this page size. `max_value_len` is the table limit: 16 MiB for every key up to `max_key_len`, else 0 (such a key is refused). A value over the inline limit is stored in an overflow run transparently: `put`, `get` and the cursors take and return its bytes as for any value. |
| `Table::name() / id()` | a handle's name and id. A committed table's id is never another table's over the file's life, so the id tells apart the tables whose handles open cursors in one transaction; it alone does not name a table (an aborted creation's id goes to the next table created) |
| `ReadTxn` (both txn kinds) | `open_table(name) -> Option<Table>` (either kind), `get` (a dup table's by the logical key `k ‖ s`), `cursor` (`TableKind` on a dup table), `txn_id`, `table_stat -> TableStat { entries, height, branch_pages, leaf_pages, overflow_pages, nested_trees, dup }` (O(1), from the table's record; a dup table's figures in [Dup tables](#dup-tables)) |
| `RwTxn::create_table(name)` | open or create a plain table (`TableKind` on a dup table) |
| `RwTxn::create_dup_table(name, DupOptions { key_len, packed_max })` | open or create a dup table: rows keyed `k ‖ s`, `k` of exactly `key_len` bytes (1 to `max_dup_key_len`, 975 at 4 KiB pages), `packed_max` 0 (never pack), 7 to `max_entry_value(key_len)` bytes, or `DupOptions::PACKED_DEFAULT` for that most (`DupOptions::packed(key_len)`), else `InvalidDupOptions`; the table records the resolved number; an existing table of the other kind or other resolved options is `TableKind` |
| `RwTxn::drop_table(Table) / clear_table(&Table)` | free the table's pages, a dup table's nested trees included (both are copy-on-write: readers keep the old tree) |
| `RwTxn::put / delete -> bool` | point writes; on a dup table by the logical key `k ‖ s` (a key with no subkey is `KeyTooShort`) |
| `RwTxn::delete_prefix(&Table, k) -> u64` | a dup table's rows under the outer key `k` (exactly `key_len` bytes) removed, its nested tree, if it has one, freed in one walk (a packed key's cell owns no page); returns the rows removed (`TableKind` on a plain table) |
| `RwTxn::insert(table, key, value)` | put only a key the table does not hold: a present key is `KeyExists`, found by the put's own search (in the table's cached leaf, else by the descent's plan) and refused before anything changes, with no latch; a new key is stored exactly as `put` stores it |
| `RwTxn::append(table, key, value)` | put a new last entry: `key` must sort strictly after the table's last key (an empty table takes any key), else `AppendOutOfOrder`, refused before anything changes. An append into the table's cached rightmost leaf neither descends nor searches; the cache holds the leaf of the table's last put or append in this transaction (after an append split, the new right leaf) until a page is allocated or freed, and any other append descends. |
| `RwTxn::cursor_mut -> RwCursor` | cursor moves and prefix steps plus `put`, `append` (as `RwTxn::append`), `insert` (as `RwTxn::insert`) and `delete_current`. A put, append or insert leaves the cursor at its entry: placed from the path it wrote (after an append split, from the new path the split refills the table's path cache with), or re-sought by key after any other split; a refused one leaves the cursor where it was. A delete re-seeks by key, so after any merges the cursor is on the deleted entry's follower in a deleted state, so a walk that deletes as it goes skips nothing: `next` returns the follower without stepping past it, `current` returns it too, `prev` returns the entry before it, and another `delete_current` deletes it. Right after a delete the prefix steps compare with the deleted key: the follower (`next_in_prefix`) or the entry before (`prev_in_prefix`) if it shares the prefix, else `None` with the cursor still on the follower in the deleted state. With no follower the cursor is after the end: `next`, `current` and `delete_current` find nothing and `prev` returns the new last entry. |
| `RwTxn::cursor_detached(&self, &Table) -> TxnCursor` | a write cursor that holds no borrow of the transaction: its moves (`first`, `last`, `next`, `prev`, `current`, `seek`, `seek_exact`, the prefix steps) take `&RwTxn` and return entries borrowed from it; its writes (`put`, `insert`, `append`, `delete_current`) take `&mut RwTxn`; after a delete, `follower_shares_prefix(n)` (whether the follower shares the deleted key's first `n` bytes, from the keys the cursor kept, without moving it) takes `&RwTxn`. The states, the writes' placement and the deleted state are `RwCursor`'s. After `last` (or `prev` from unset) the cursor is on that entry as after any move: `next` and `next_in_prefix` return an entry another writer has put past it since, else `None` (the cursor after the end). Several cursors, over one table or several, and the transaction's own writes interleave, each seeing the others' writes; see [Detached cursors](#detached-cursors-txncursor) below. |
| `RwTxn::commit() -> txn id / abort()` | finish |
| `Cursor` | `first`, `last`, `next`, `prev`, `current`, `seek` (first `>= key`), `seek_exact`, and the prefix steps `next_in_prefix(n)`, `prev_in_prefix(n)` (below) |
| `RoTxn::cursor_owned(self: &Arc<RoTxn>, &Table) -> RoCursor` | a read cursor that owns a share of its transaction: no lifetime, `Send + Sync`, the same moves, states and prefix steps as `Cursor`; the entries it returns, overflow values included, borrow the cursor until its next call. Each step fetches its leaf again by page number (see [Readers and snapshots](#readers-and-snapshots)), so a scan costs about 4–8% more per entry than `Cursor`'s (macOS). |
| `current_with_key_tail(t) -> (key, KeyTail)` on `Cursor`, `RoCursor` and `TxnCursor` | the current entry with its value joined to its key's last `t` bytes (`t` clamped to the key's length): `KeyTail::Joined` for an inline value, the key's tail then the value as one slice of the page (a leaf cell holds its value right after its key), so with no copy; `KeyTail::Apart` for a value in an overflow run, the value alone. The reth adapter reads a dup row (subkey, then the rest) this way. |
| `seek_with_key_tail(key, t) -> (key, KeyTail)` on `RoCursor` and `TxnCursor` | `seek(key)` then `current_with_key_tail(t)` from one leaf access: the entry is read through the leaf the descent fetched and parsed, an overflow value once; `None` (after the end) when no key is `>= key`. The cursor is left exactly as `seek` leaves it (a `TxnCursor` saves the landed key for its reseek). The reth adapter's `seek_by_key_subkey` reads its row this way. |
| `RoTxn::dup_cursor_owned(self: &Arc<RoTxn>, &Table) -> DupRoCursor` | a read cursor over a dup table, owned as `RoCursor` is (`TableKind` on a plain table): `RoCursor`'s moves, prefix steps and key-tail calls on the logical keys `k ‖ s`, each giving what `RoCursor` gives on the composite twin, plus the bounded seeks `seek_key(k)`, `seek_dup(k, s)` and `seek_dup_with_key_tail(k, s, t)` (a miss unset, no other key's tree read), and each move's tail twin `*_with_key_tail(.., t)` (`first`, `last`, `next`, `prev`, `seek`, `seek_dup`, `next_in_prefix`, `prev_in_prefix`: the move, its row as `current_with_key_tail(t)` gives it, from the move's own leaf access); see [Dup cursors](#dup-cursors) |
| `reset()` on `RoCursor` and `DupRoCursor` | forget the position: the cursor then behaves exactly as a new one from `cursor_owned` / `dup_cursor_owned` on the same snapshot and table (unset), and keeps its allocations (a dup cursor's logical-key buffer), so a pool of cursors reuses one with no descent and no allocation |
| `RwTxn::dup_cursor_detached(&self, &Table) -> DupTxnCursor` | a detached write cursor over a dup table (`TableKind` on a plain table): `TxnCursor`'s moves, writes (`put`, `insert`, `append` by the logical key, `delete_current`), deleted state, `follower_shares_prefix` and staleness on logical keys, plus `DupRoCursor`'s bounded seeks and tail twins (the transaction first); its results borrow the cursor and the transaction; see [Dup cursors](#dup-cursors) |
| `Table` (`name()`) | a handle holding the table's id and name, checked against the catalog of the state it is used in: by a read transaction once per snapshot (the snapshot's `TableRoots` remembers the root it resolved; see [Readers and snapshots](#readers-and-snapshots)), by a write transaction on every use (against its tables' working records, then its catalog). A dropped or re-created table is `StaleTable`, and one created after the reading state is `TableNotFound` |
| `Error`, `Result<T>` | see [Errors](#errors) |

**Cursor states:** Unset, At, AfterEnd, BeforeStart.

- `next` from Unset or BeforeStart goes to the first entry.
- `prev` from Unset or AfterEnd goes to the last entry.
- A `seek` miss leaves the cursor AfterEnd.
- A `seek_exact` miss leaves it Unset.
- On an empty table, `first` leaves it AfterEnd and `last` BeforeStart.
- An error leaves it Unset.

A write transaction's read cursor sees its own uncommitted changes.

**Prefix steps** (`next_in_prefix(n)`, `prev_in_prefix(n)`, on `Cursor`, `RoCursor`, `RwCursor`
and `TxnCursor`, and on the dup cursors over logical keys) move to the adjacent entry only if its key shares the first `n` bytes of the current
entry's key, and return it; otherwise they return `None` and the cursor stays where it was. For
keys built as `key ‖ subkey` they walk one key's subkeys (the reth adapter's `next_dup` and
`prev_dup`).

- Not at an entry (Unset, AfterEnd, BeforeStart): `None`.
- `n` longer than the current key: `None` (no other key has that prefix).
- `n = 0`: `next` (`prev`), except that at the last (first) entry it returns `None` and stays.
- At a leaf's end the step peeks into the adjacent leaf with a copy of the cursor and keeps its
  position unless the entry there shares the prefix.

### I/O counters

`Env::io_stats` returns an `IoStats`, a `Copy` struct of counters the writer keeps in plain fields of
its state as it works: an add beside each event, and a clock read only around a whole commit step,
a published page's check (`verify_published`) and a refill of the pool. Whenever the writer's
state comes back to the environment (`EnvInner::return_writer`: the end of a commit, an abort,
and an `Env::sync`), it copies them into one `Mutex<IoStats>`, which `io_stats` copies out: a
reader sees one consistent set as of the last of those points, and the values do not move while a
write transaction runs. An aborted transaction's work counts (it was done). Times are in
nanoseconds. A benchmark takes two reads and `IoStats::since` for one commit's or one interval's
work.

| group | counters |
|---|---|
| commits | `commits` (each run of steps 4–6: a `Durable` commit, or an `Env::sync` that had a `NoSync` state to make durable), `nosync_commits`, `grown_commits` (the file grew since the last durable state) |
| step 4 | `pages_synced` (pages, not bytes), `msync_syncs` and `fdatasync_syncs` (which one step 4 took), `msync_span_pages` (the one range's pages, clean ones included) |
| growth | `bytes_grown` (the physical length, which grows geometrically ahead of the pages in use) |
| step times | `save_ns` (step 1: the catalog's roots and the free list's save), `stamp_ns` (step 3), `data_sync_ns` (4), `size_sync_ns` (5), `meta_sync_ns` (6); `free_list_save_rounds` (write rounds of the fixed-point save) |
| a transaction's pages | `cow_copies`, `verifies` and `verify_ns` (checks of published pages this writer did not stamp), `pages_from_pool`, `pages_from_end` (single and run pages) |
| free list | `records_loaded`, `refill_ns`; `runs_by_step` (the run policy's four steps); `gate_lag_sum`, `gate_lag_samples` and `max_gate_lag` (at each load of records, the writing txn minus the reuse gate: 2 in `Durable` with no reader on an old snapshot, more under a held reader or `NoSync` commits not yet synced) |

### The path cache

A table put or delete can skip the descent (`write::PathCache`, `write::put_cached`,
`write::delete_cached`; do-not-flag item 12). Each table's working record caches the leaf its last put or delete
reached on a trusted path, the path above it, and the leaf's key range `[lo, hi)` as owned
copies. A put that did not split fills it from its own descent; an append split refills it with the new path: from the split itself when the leaf's
parent took the separator (the usual case: the descent's path with the parent's step at its new
last cell, then the new right leaf, which holds only the entry, its range from the key, the
separator, with no upper bound; debug builds check it against a real descent,
`assert_found_again`), else (a branch split or a new root) by a second, trusted descent; any
other split leaves it empty. While the table's root is trusted and the allocator's `epoch` is
unchanged, a put whose key lies in the range and whose entry fits goes straight into that leaf
(through `page_mut`, so the private and poison checks stay, plus the identity check), with the
descent's exact change and bookkeeping. A delete uses it the same way: a key in the range
whose delete changes only that leaf (an inline value, another entry left, no merge due by
`merge_due`, the rule `plan_merges` follows) leaves the leaf in place, and a key in the range
that the leaf lacks is a miss there. Any other delete descends, and refills the cache after a
delete that only took the entry out of its leaf, or after a miss whose path held only this
transaction's pages; an unlink, a collapse, a merge or an emptied tree leaves it empty, and so
does a miss over published pages (the descent borrows the cache's buffer), which costs the
next write one descent and nothing else ([Deletes and merging](#deletes-and-merging)). One
writer-wide epoch is enough because every change to
a tree's shape or separators allocates or frees a page: a copy allocates and frees, a split
allocates, an unlink, merge, collapse, clear or drop frees; and the epoch also moves at commit
(`stamp_private`), `begin_txn` and abort. An in-place insert, replace or delete (one that does
not merge) changes only its leaf, never a separator, so it leaves the cache valid. A freed page
is reusable at once by another table in the same txn, which is exactly the move the epoch
catches. The range is the nearest separator either side on the path, which equals the
intersection of all of them on any path the descent accepts (debug builds assert it). The
catalog and the free list never use a cache. A put whose value goes to an overflow run, a
replace of a value held in one, and a delete of one, always descend: the run is allocated, or
the old one freed, with the tree's other pages.

### Detached cursors (`TxnCursor`)

A `TxnCursor` keeps its position as a detached path plus a copy of its current entry's key (one
`memcpy` per landing, into a reused buffer). The write transaction keeps three counters for it,
none of them public: a per-table **write stamp** (`Working::writes`, set from a transaction-wide
sequence by `Work::set_record`, which every change to a table's tree goes through: put, append,
insert, delete, clear), a **schema counter** that moves when a table is created or dropped, and a
process-unique **serial** per write transaction. Every call first compares them with the values
the cursor saw at its last call, inline:

- **another transaction** (the serial differs: a later transaction, even one that reuses the txn
  id of the aborted one the cursor came from, or another environment's) is `WrongTxn`;
- **a table created or dropped** (the schema counter moved): the cursor's table is looked up again,
  and a dropped one (perhaps re-created since) is `StaleTable`;
- **its table changed** (its write stamp moved): the cursor takes the table's new tree and seeks
  its saved key. If the key is still there the cursor stays on it, with its new value if it
  changed. If not, the cursor is left as if it had deleted the entry itself, so it behaves the
  same whichever writer deleted its entry: on the first entry after the key (the
  follower) in the deleted state, so `next` and `current` return the follower, `prev` the entry
  before it, `delete_current` deletes it, and the prefix steps compare with the gone key. Its
  saved key becomes the follower's (the gone key is kept apart, `deleted_key`, for the prefix
  steps). A cursor already in that state stays in it, on its follower, or on the first entry
  after the follower if that is gone too. An unset cursor, or one after the end or before the
  start, keeps its state over the new tree.
- **a seek** places the cursor anew, so after a change to its table it takes the new tree and
  descends once from its root, without seeking its saved key first; the cursor ends as that
  reseek and the seek would leave it (out of the deleted state, the landed key saved). While its
  table is unchanged, a seek starts from the leaf the cursor is at
  ([Readers and snapshots](#readers-and-snapshots)).

The reseek is lazy: it sees the table as it is at the cursor's next call. If, before that call,
another writer deleted the cursor's entry and put it back, or put an entry between it and its
follower, the cursor is on that entry: it reseeks the key it saved, not the delete it never
saw. Past either end the cursor has left the entries: after stepping past the end entry, after a
`seek` past the end or after deleting the last entry it is after the end or before the start
(`current` finds nothing, `delete_current` returns `false`), and a step the other way returns
the end entry.

A write to another table (each table has its own stamp) and the cursor's own writes (it records
the stamp its write left) cause no reseek. A `put`, `insert` or `append` through the cursor checks
the serial and the table, not the stamp: it places the cursor anew at the entry it wrote, from the
path the table's path cache holds (or by key after a split), as `RwCursor` does; a refused one
changes nothing, the cursor included, so a stale cursor is brought up to date at its next call.
`delete_current` brings the cursor up to date first, then deletes the entry it is at, through the
table's path cache ([Deletes and merging](#deletes-and-merging)), and lands on the follower in the
deleted state. When the delete left the cache naming the entry's leaf (it took the entry out of
that leaf and merged nothing), the cursor places itself from the cached path
(`TreeCursor::place_after_delete`): the leaf's entry at the deleted entry's old slot, or past the
leaf's end the next leaf's first entry or after the end, as a seek lands. Separators are
unchanged, so that is the path and insertion point a seek of the deleted key finds. After any
other delete (a merge, an unlink, a collapse, an emptied tree) it finds its place by key
(`find_again`), as `RwCursor` does. Either way it ends where a seek of the deleted key lands.
`WrongTxn` and `StaleTable` change nothing and do not latch.

**A write descends along the cursor's path.** A `put`, `insert` or `append` through a cursor that
is up to date and at an entry, of any key, and every `delete_current` (which brings the cursor up
to date first; in the deleted state it is at the follower, so it hints the follower's path), hand
the table's write the slots of the cursor's path as a routing hint (`write::Hint`,
`TreeCursor::hint`): slots only, root first, the leaf's last, no page number. The table's path
cache still comes first, and a write that misses it descends from the write's own root as every
write does (`write::descend_write`). At each branch the descent reads the hinted cell
(`BranchView::route_at`) instead of searching (`BranchView::route`), and follows it only if the
key lies in that child's range `[lo, hi)`; at the leaf, a hinted slot holding the key answers the
lookup with one compare (`write::find_at`). A slot past the cells, a cell that does not read, a
key outside the range or another key at the slot refuses the hint at that level, and that level
and every one below it search; a hint of another length than the tree's height is refused whole.
Every page the descent enters gets the search's checks in the search's order: the provenance rule,
the per-fetch checks of an untrusted page, identity, kind, a non-root leaf not empty,
`verify_published` and `check_cells_naming_this_txn` for a put (for a delete, `verify_path` on a
hit), and the separator-range check. So on any tree the writer accepts, the hinted descent returns
the leaf, path, range and copies the search returns, or the search's error; the write leaves the
same bytes, handle, epoch moves and path cache; and the cursor places itself as before, where a
fresh seek lands. A put through a stale cursor, through an unset one or one past either end, and
the transaction's own writes (`RwTxn`) pass no hint. The hint changes what a level costs, never
what a write does; one message differs: a delete's descent defers verification (`Verify::Later`),
so on a damaged published branch the hinted descent reads two of its cells where the search reads
several, and the damage is met by `verify_path` on the hit, `Corrupt` before any change as the
search's is, with that check's message.
The hint is only routing (do-not-flag item 27). The writer does not reuse the cursor's checks of the pages it entered (the cursor read them
under the table stamp it saw): each write still pays the per-level checks, and the hint
saves only each level's search (one `route_at` and two key compares in its place).

In the deleted state the cursor holds both keys a prefix question needs: the follower's (its
saved key) and the deleted one (`deleted_key`). `follower_shares_prefix(n)` answers from them
whether the follower starts with the deleted key's first `n` bytes, so whether
`next_in_prefix(n)` would return it, without a step and, while the cursor is up to date, without
a page read. It is `false` after the end (no follower), for an `n` past the deleted key's length,
and outside the deleted state (no delete since the cursor's last move); the cursor stays where it
is, still in the deleted state. Like every call it first brings the cursor up to date, so after
another writer's delete of the follower it answers for the entry after it. A caller that deletes
an entry and needs to know whether it was the last of the entries sharing its first `n` bytes
asks it after the delete, instead of stepping ahead and back before it.

**Cost** (Apple M1 Max, macOS, 2026-10-21, 1M entries): a put or append through a `TxnCursor`
costs about 4 ns more than through an `RwCursor` (7–9%), and a full scan 4–5 ns more per entry (4 KiB pages 44.8 → 48.6 ns,
16 KiB 30.5 → 35.6 ns). The key copy is about 2 ns of it; the rest is the inline check, within the
±3 ns that code layout moves these loops. A reseek is one descent (it reads the value of the entry
it lands on, a multi-page overflow value in full).

---

## Errors

`Error` (`error.rs`, `thiserror`) has these variants:

| variant | when |
|---|---|
| `Io(io::Error)` | an I/O failure (latches inside a write, commit or sync) |
| `Corrupt(String)` | damage: a failed CRC, structure, pointer, range or accounting check, or an unopenable file (latches when found by a read) |
| `Failed(String)` | the environment is latched (the first cause), or a broken internal invariant |
| `KeyTooLarge { len, max }` / `ValueTooLarge { len, max }` | an entry over the page size's limits, or an outer key passed to `delete_prefix` longer than its dup table's `key_len` (no latch) |
| `KeyTooShort { len, min }` | a dup table's point operation with no subkey (`len <= key_len`, `min = key_len + 1`), or a `delete_prefix` outer key shorter than `key_len` (no latch) |
| `AppendOutOfOrder` | an append whose key does not sort strictly after the table's last key (no latch) |
| `KeyExists` | an insert (`RwTxn::insert`, `RwCursor::insert`, `TxnCursor::insert`) of a key the table holds (no latch) |
| `TableNotFound(String)` / `StaleTable(String)` / `InvalidTableName` | table errors (no latch) |
| `TableKind(String)` | a call that needs the other kind of table: `create_table` on a dup table, `create_dup_table` on a plain table or a dup table of other options, a plain cursor on a dup table, `delete_prefix` on a plain table (no latch) |
| `InvalidDupOptions(String)` | `create_dup_table` with a `packed_max` neither 0, `PACKED_DEFAULT` nor in `7..=max_entry_value(key_len)`, or a `key_len` of 0 or past `max_dup_key_len` (no latch) |
| `NotTnLmdb` | a non-empty file without the magic in either meta slot |
| `UnsupportedVersion(u32)` | another format version in a meta slot |
| `InvalidPageSize(u32)` | `EnvOptions::page_size` is not a power of two in 4K..=64K |
| `InvalidMaxGrowth(u64)` | `EnvOptions::max_growth` is under `MIN_GROWTH` (1 MiB); refused by `Env::open` before it touches the file |
| `Locked` | another handle (in this process or another) holds the file: any handle for `Env::open`, a writer's for `Env::open_read_only` |
| `ReadOnly` | `begin_rw` or `try_begin_rw` in an environment opened read-only (no latch) |
| `WrongTxn` | a `TxnCursor` used with a write transaction other than the one that made it (no latch) |

---

## Platform notes

- **Unix only** (`compile_error!` elsewhere): the engine uses mmap, msync, flock, and
  fallocate or `F_PREALLOCATE`.
- **macOS: msync and fsync are not power-loss barriers.** Neither flushes the drive's write cache.
  Only `F_FULLFSYNC` does, at 10–20 ms each, and the crate deliberately never issues it. Never
  call `File::sync_all`; use `sys::fsync_file` and `fdatasync_file`. A process or kernel crash
  loses nothing committed durably, but a power loss can, as with any engine that syncs without
  `F_FULLFSYNC` on macOS. Production durability is a Linux property, where `msync(MS_SYNC)` and `fdatasync` each
  take the file system's fsync path, drive-cache flush included.
- **A page size below the OS page puts both metas in one OS page** (4 KiB pages on Apple
  Silicon's 16 KiB OS pages, or on a 64 KiB-page arm64 or ppc64 Linux kernel). A mapped write
  dirties the whole OS page and an msync rounds down to it, so every meta msync also writes back
  the kept slot's bytes, unchanged. On a drive whose sector writes are atomic that is harmless.
  A drive that can garble a whole sector it was writing when power fails could lose both metas
  in one commit, and the file would no longer open. The crash model (`power_loss_random`) mixes
  old and new per 512-byte sector, so it cannot show this: unchanged bytes stay unchanged. Where
  that matters, create the file with `page_size` at least the OS page size, which puts each meta
  in its own OS page.
- **Linux: growth uses `fallocate`** (mode 0, which allocates real blocks and extends the size).
  macOS uses `F_PREALLOCATE` plus `set_len`, and other unix targets use a plain `set_len`.
  Preallocation makes a full disk an `io::Error` at growth, not a `SIGBUS` on a mapped write.
  That holds only where preallocation works: on other unix targets, and on a file system without
  it (`fallocate` failing with `EOPNOTSUPP` or `ENOSYS`, as on tmpfs and some network file
  systems, or `F_PREALLOCATE` with `ENOTSUP`), growth falls back to a sparse `set_len`, and a
  full disk can then be a `SIGBUS` on a mapped write.
  Growth is geometric: at least `clamp(len, 1 MiB, max_growth)` more (`EnvOptions::max_growth`,
  default 1 GiB, at least `MIN_GROWTH`, 1 MiB), rounded to the OS page, so a running file holds a
  preallocated tail of up to one step past its pages. A clean close truncates the file to its
  pages, rounded up to the OS page (`PageFile::trim_to`, see [Close](#close)), so the resting
  file holds no preallocated tail; a failed or skipped trim (a crash, a latched close) leaves the
  slack. Past the reservation the mapping moves to a new reservation of at least twice the size.
  Retired mappings are kept until close while a reader may hold a view of them.
- **Linux: every mapping of the file is advised `MADV_RANDOM`** (`file::advise_access`;
  do-not-flag item 16), unless `EnvOptions::readahead` is set: the writer's mapping at open and
  after each remap past the reservation, and a read-only environment's. Elsewhere `readahead`
  defaults to `true`, so no advice is given unless it is cleared. Why:
  - A B+tree's faults are random. A descent touches one page per level, far from its
    neighbours, so readahead mostly reads pages nothing asks for.
  - On Linux, readahead sizes what a commit writes. The page cache holds a file in folios, and
    readahead builds large ones; a page dirtied through a shared mapping writes back its whole
    folio. Measured on Linux 7.1, ext4, NVMe, as disk KiB written per 4 KiB page dirtied through
    a shared mapping and synced (2000 random pages): 439 for a file written by `write(2)` and
    still cached (what a copy leaves), 115 after a cold full sequential read with the default
    readahead, 4.2 after cold random reads with it, 4.0 with `MADV_RANDOM`.
  - The advice cannot help a file already cached in large folios by buffered writes (a copy, a
    restore written with `write(2)`). Evict such a file from the page cache before opening it
    (sync it, then `posix_fadvise(POSIX_FADV_DONTNEED)`): a commit on a copy left cached grew
    from 82 to 583 ms over 300 commits in one measured workload, all in its one
    `fdatasync`, as the disk wrote 199 to 532 MiB per commit.
  - A failed `madvise` is not an error: the advice is a hint, so it is logged (`tracing::warn!`)
    and the mapping is used without it. Mappings are made at open and at a growth past the
    reservation, so the log is rare.
  - The cost is a cold scan: with no readahead, each page of a sequential scan of uncached
    pages is its own fault and read. Set `readahead` for a workload that scans whole tables
    cold.
  - Why not on macOS: a page dirtied through a shared mapping there writes back its whole VM page
    (16 KiB on Apple silicon) with or without the advice, so it saves nothing at commit, and on
    an Apple M1 Max it made first writes into new file space slower (the comparison benchmark,
    A B B A: large-value puts 1.13–1.32x, sequential put loops 1.05–1.07x, small-transaction
    p99 1.5–1.7x; reads, scans and commits within the spread). Clear `readahead` to get the
    advice there.
- **macOS (APFS): first writes into preallocated space after an `fsync` are slow.** Writing
  through the mapping into file space reserved with `F_PREALLOCATE` before an `fsync` takes slow
  first-touch page faults: 6–24 us per 12 KiB in a standalone C test, against 1.3 us when the
  space was preallocated after the `fsync`, when an `msync(MS_SYNC)` replaced the `fsync`, or when
  the file grew sparse. Growth leaves slack past the pages in use, and a durable commit that grew
  the file fsyncs it (the size sync, or the `fdatasync` data sync), so the puts of a long-lived
  environment into slack grown before a durable commit pay this, on overflow runs and tree
  pages alike. When the comparison benchmark's `big` rows
  ([`engine-bench.md`](../tn-lmdb-reth/engine-bench.md#large-values-overflow-runs)) ran in one
  shared environment, a `t4d` put of an 8 KiB value took 9.5 us, against 2.0 us in a fresh
  environment; most of it was the run's first-touch faults. A file grown sparse does not pay
  this, but tn-lmdb keeps growing with real blocks by design: a full disk must be an `io::Error`, not a `SIGBUS`. The
  cause is specific to macOS; Linux is not measured.
  - The order of those first writes matters too. A put reserves its pages before it writes them,
    so it can write a page after pages past it. `Writer::alloc` therefore writes one byte of
    each page it takes from the file end, which makes each page's first write follow allocation
    order. Without that store, the adapter's sequential-write row of about 1.2 KiB values
    (one-page overflow runs between tree pages, durable) took 4.40 us per put against 3.80 us
    with it, and the comparison benchmark's 32 KiB puts at 16 KiB pages 1.45x as long, on an
    Apple M1 Max. With the store, both matched the old zero-filled pages. The store moves the
    page's first fault earlier and adds none, as the layout writes the page anyway.
- **A large commit stamps its CRCs on up to 8 threads** (commit step 3, `stamp::threads`). The
  cap is also the CPUs the process may use (`std::thread::available_parallelism`, which on Linux
  reads the cgroup quota; read once, at the first large commit). On an Apple M1 Max, 400 MiB of
  4 KiB units took 16.9 ms on one thread, 5.9 ms on 4 and 4.5 ms on 8, and 2 MiB 84 us on one
  against 66 us on 2, the threshold. Whole commits of ~100k tree pages (the stamp probe, medians)
  stamped in 17.5 ms on one thread, 8.4 ms on 4, 6.7 ms on 6 and on 8, and 6.8 ms on 12: no
  gain past about 6 to 8 threads there. On an Intel i7-1360P under Linux (4 performance cores
  with two threads each and 8 efficiency cores), the same commits took 22.6 ms on one thread,
  15.8 ms on 4, 12.0 ms on 8 and 10.9 ms on 12, and one thread's CRC ran at 39.9 GB/s over a
  cache-resident buffer against 20.8 GB/s over a DRAM-resident one: bound by memory. The commit
  shares the machine with the node's other work, so it takes no more.
- **A tree page's CRC skips its free gap** (do-not-flag item 28), so the stamp hashes fewer bytes
  on pages with room left. The stamp probe's table (c) has leaves 34.9% free; there, on the
  Apple M1 Max (medians, two runs each), one thread stamped a commit in 14.2 ms against 15.9 ms
  with the whole-page CRC, and 8 threads in 5.4 ms either way. The full leaves of table (a), with
  no gap, took 16.8 ms against 15.7 ms on one thread (likely the two CRC calls a page and the
  header read) and 5.4 ms against 5.2 ms on 8. One thread's CRC there runs at 25.8 GB/s over
  cache-resident bytes and 25.6 GB/s over DRAM-resident ones, so it is bound by the CPU, and 8
  threads meet a floor near 5.3 ms that does not follow the bytes hashed. Where one thread's CRC
  is bound by memory, as on the i7-1360P above, the stamp's time follows the bytes it reads; that
  machine's numbers for table (c) are not measured yet.

---

## Limits and non-goals

The entry limits are in [Limits](#limits): a table takes values up to 16 MiB, in overflow runs.
These limits and non-goals are known:

- **Dup tables have owned and detached cursors only, and pack only what fits a quarter-page entry.**
  A dup table's cursors are `DupRoCursor` and `DupTxnCursor`, and the borrowed `ReadTxn::cursor`
  and `RwTxn::cursor_mut` are `TableKind` on it ([Dup tables](#dup-tables)). A packed set is at
  most `max_entry_value(key_len)` bytes, so a key past that holds a nested tree of at least one page
  (do-not-flag item 26), and one in a table with `packed_max` 0 always does. Leaf cell flags other than 0, 1 (overflow) and 2 (nested) are
  refused.
  - At 20-byte keys, 32-byte subkeys and 4 KiB pages, keys of 18 to ~34 rows are therefore
    left nested (do-not-flag item 26).
  - A packed set is held to a quarter-page entry (`packed_max` at most
    `max_entry_value(key_len)`, 987 bytes at a 20-byte key), and an outer tree holds no
    overflow cell. A key past ~17 such rows
    therefore nests and spends a leaf page, where a composite table (rows keyed `k ‖ s` in a
    plain table) spends 0.52-0.99 pages: up to ~2x the composite file for that band.
  - Closing it would need a larger bound. Sets up to the inline limit (`max_value(key_len)`,
    about 2 KB at 4 KiB pages) would stay inline, but each row change would rebuild up to that
    many bytes, a trade not yet measured; past it they would need overflow runs in an outer
    tree. Over a whole state the band costs little: a random state of 1 to 20 rows per contract takes 0.286
    pages per contract at the default bound against composite's 0.298
    (`packed_sets_file_size_by_bound`, an ignored measurement).
- **The inline limit is about half a page** (a leaf entry of `U / 2`): about 2 KB at 4 KiB
  pages. A value of 2–4 KB at 4 KiB pages therefore takes a run page of its own. Raising it
  further needs a new split proof (one split keeps both halves within `U` only for entries of at
  most `U / 2`) and a format change.
- **No multi-process access** alongside a live writer: there is no shared reader table. A
  read-only open takes the lock shared, so it and a writer exclude each other.
- Merging is local and merge-only. A delete merges only the pages it changes, siblings never
  borrow entries, and a page stays under a quarter full when no neighbour fits into it. Pages no
  delete changes are not revisited, so there is no fill bound for the whole tree: only the pages
  a delete changes are merge-closed.
- No sibling links. A cursor crosses leaves through their parents (copy-on-write cannot keep
  sibling links).
- A `TxnCursor` reseeks lazily, at its next call, so it does not see a delete and re-put of its
  entry between two calls; and a cursor stepped past either end rests after the end or before
  the start, not on the end entry (see [Detached cursors](#detached-cursors-txncursor)).
- The height is capped at 48, a corruption tripwire far above any real height.
- Table names are 1..=255 bytes of UTF-8.

---

## Intentional design decisions: please do not flag these

Each item is a deliberate choice: the decision, the reason in brief, and the section that holds the
argument and the measurements. Code and other docs cite the items by number, so the numbers stay.

1. **No `F_FULLFSYNC` on macOS.** The commit path is msync, which is no stronger, and a full drive
   flush per sync would cost 10–20 ms and buy nothing the commits keep. See
   [Platform notes](#platform-notes).
2. **`unsafe` in five places**, each block with a `SAFETY:` comment:
   - `file.rs`: a read-only open's mapping (`map`), `msync`, and the `from_raw_parts` page slices.
     The writer's reservation is an `MmapRaw` from the safe `map_raw`, so every page slice, the
     writer's and the readers', is formed from the base pointer over exactly its own bytes, and no
     reference to the whole mapping exists. Sound under the single-writer model: the base is
     stable inside the reservation, readers see only published, immutable pages, retired mappings
     outlive every view of them, and a read-only open's shared lock keeps every writer out.
     Copy-on-write adds no `unsafe` ([Commit protocol](#commit-protocol)).
   - `sys.rs`: the libc calls `fsync`, `fallocate`, `fcntl(F_PREALLOCATE)`, `sysconf` and `flock`,
     each on a live file descriptor (`sysconf` takes none).
   - `crc.rs`: the aarch64 PMULL kernel (`pmull::update`, a copy of `tn-storage`'s), called only
     after the CPU feature is detected at runtime.
   - `page::prefetch`: a one-instruction prefetch hint (`prfm` or `prefetcht0`) at a byte of the
     page, for a search's next probes and a level's first cells; a hint never faults or changes
     memory.
   - the test-only `disk_model.rs`: a copy out of a live mapping into the model's own buffer.
3. **Reads do not recompute a tree page's CRC, nor a 1-page overflow run's.** The per-fetch checks
   (and a 1-page run's header checks) catch misdirected and reused pages; the CRC is checked before
   copy-on-write, on the roots at open, on free-list leaves before reuse, and in `Env::check`. A
   run of more than one page is the exception (item 13). See
   [Readers and snapshots](#readers-and-snapshots).
4. **`child_txn` and `root_txn` hold only the low 32 bits of a txn.** They are a damage detector: a
   sound tree never points at a reused page (the reuse gate guarantees it), and a pointer
   redirected to a stale copy goes unnoticed only if that copy's txn matches modulo 2^32. See
   [Tree pages](#tree-pages-pagers).
5. **The file can be longer than `page_count × P`.** That is growth slack; the meta's page count is
   authoritative. A clean close trims it; a crash, a latched close, a failed final sync or a failed
   trim keeps it, because the longer file is always valid while a trim whose length rests on state
   the close cannot trust could cut a page a meta names. In doubt, no trim. See [Close](#close).
6. **A torn or invalid meta slot is not repaired at open.** Open never writes; the slot counts as
   the lowest and is the next one a commit overwrites. See [Open and recovery](#open-and-recovery).
7. **Leftover pages go back under the consumed keys in page order, not each under its own record's
   key.** A key's txn only has to be one the gate has passed, and the gate never moves back. Pool
   pages past what the consumed keys hold wait under the committing txn, as freed pages do. See
   [Free list and page reuse](#free-list-and-page-reuse).
8. **`Env::sync` blocks while a write transaction is open.** It needs the writer state, so calling
   it while holding an `RwTxn` deadlocks. This is documented, not guarded. See
   [Concurrency rules](#concurrency-rules).
9. **Merging is merge-only, at a quarter, and local.** Nothing is borrowed or redistributed, pages
   no delete changes are not revisited, and there is no compaction pass. A merge never grows a
   separator, so a delete never splits and reserves only its path's copies; a quarter leaves a wide
   gap below a split's half-full pages. See [Deletes and merging](#deletes-and-merging).
10. **Step 4 is one sync:** one `msync` when the dirty pages merge into one range of at most 16
    runs, else one `fdatasync` of the file. On Linux every msync takes the fsync path, drive-cache
    flush included, so syncing k ranges one by one paid k flushes. See
    [Commit protocol](#commit-protocol) and [Step 4](#step-4-one-msync-or-one-fdatasync).
11. **The writer trusts its own pages by provenance** (`write::descend_write`): a trusted page skips
    the per-fetch checks, `verify_published` and the separator-range check. `is_private` alone is
    not enough; the rules that close the gap (an untrusted parent naming a private page is
    `Corrupt`, the copy-time `check_cells_naming_this_txn`, the same for overflow runs) and the lazy
    range bounds are in [Readers and snapshots](#readers-and-snapshots).
12. **A table put or delete can skip the descent** (`write::PathCache`): each table's working record
    caches the leaf its last write reached. One writer-wide epoch guards it, because every change to
    a tree's shape or separators allocates or frees a page. See [The path cache](#the-path-cache).
13. **Every read of a value in a run of more than one page verifies the run's full CRC**
    (`RunCrc::MultiPage`). Pages 2..n carry no header, so only the CRC catches a reused or
    redirected middle page, and the rule is never wrong data. The cost per read is in
    [Readers and snapshots](#readers-and-snapshots).
14. **An owned cursor's step and seek re-fetch its leaf without the per-fetch checks**
    (`Snapshot::page_again`). The cursor's descent entered the page with them, and a snapshot's
    published pages never change while an `Arc` of it lives, so the checks would pass again. See
    [Readers and snapshots](#readers-and-snapshots).
15. **A snapshot resolves each table handle once** (`TableRoots`). Its catalog cannot change while
    it lives, so a second lookup would give the same root; only a resolved handle is remembered, and
    a hit compares the name as well as the id. See [Readers and snapshots](#readers-and-snapshots).
16. **On Linux every mapping of the file is advised `MADV_RANDOM`** (`file::advise_access`) unless
    `EnvOptions::readahead` is set; elsewhere `readahead` defaults to `true`. A B+tree's faults are
    random, and on Linux readahead's large folios multiply what a commit writes back. A failed
    `madvise` is logged, not an error. See [Platform notes](#platform-notes).
17. **A new page is not zero-filled, and copy-on-write copies a page outside its free gap, so the
    gap holds old bytes** (`PageWrite::alloc`, `tree::write::copy_page`). Nothing reads the gap and
    the CRC skips it (item 28); zero-filling cost a page-sized `memset` per allocation. Debug builds
    poison new pages, so a reliance on zeros fails the tests. There is no secure-delete promise. See
    [Tree pages](#tree-pages-pagers).
18. **Inside a write transaction a dup table's outer cell can name a superseded nested root**
    (`Work::flush_nested`). The outer cell is rewritten once per changed key, at commit, not per
    row; until then the working map is the truth for its key, and every reader of an outer cell in
    the transaction looks there first. Published outer pages never hold a stale cell. See
    [Dup tables](#dup-tables).
19. **A dup table created with `packed_max` 0 holds every outer key in a nested tree of at least one
    page.** 0 asks for no packing, and such a table runs exactly the code and the messages it ran
    before packed sets existed (`Cells::Outer`). `DupOptions::packed` gives the bound that packs
    most. See [Dup tables](#dup-tables).
20. **A write transaction keeps one path cache per outer key it writes** (the boxed
    `write::PathCache` of each map entry, about 1 KiB), so keys written in turn do not evict each
    other's path, and map entries are never evicted (item 18). The memory is bounded by the
    transaction's own work: every such key already owns a private page, and the map drops at commit
    or abort. A packed key has no entry and no path cache of its own (item 22).
21. **The dup cursors' shape:** a bounded seek's miss leaves the cursor unset; any change to the
    table's write stamp reseeks every dup cursor of it; a `DupTxnCursor`'s results borrow the cursor
    too; a key tail past the subkey is `Apart`; there are no borrowed dup cursors and no cursor
    `delete_prefix`; `DupTxnCursor` keeps its own sync and reseek code beside `TxnCursor`'s. The
    reasons are in [Dup cursors](#dup-cursors).
22. **A packed write rewrites the key's outer cell at once; packed sets never enter the
    transaction's map** (`txn::dup_entry`, `write::put_outer_cell` with `OuterCell::Packed`). The
    copy is the same either way, and written at once the cell is the truth for every packed key
    (invariant P), so nothing needs a packed overlay. See [Dup tables](#dup-tables).
23. **A read checks only the packed rows it touches.** Every write that decodes a set, the free
    walks and `Env::check` validate it whole. Damage is `Corrupt` at first use, as for pages, and
    the whole check on every get would cost several times the lazy one. See
    [Packed sets](#packed-sets).
24. **A nested key never demotes** (`a_nested_key_never_demotes`). A key crossing the bound in turn
    would rebuild its rows each time, a nested key with few rows costs one leaf, and a demotion
    would turn a map entry back into a cell, against invariant P (item 22). A key emptied and put
    again is a new key, packed if its first row fits
    (`a_nested_key_emptied_then_recreated_is_packed_in_one_txn`). See [Dup tables](#dup-tables).
25. **A packed set is not checked at page validation or copy-on-write.** `page::validate` cannot
    tell an outer leaf from any other, and `check_cells_naming_this_txn` skips a set under
    `Cells::Packing`: it names no page, its bytes are under the CRC `verify_published` has just
    checked, and the page mutators never parse it. Checking there would make copy-on-write the
    first use of keys the writer never touched; each set is checked at its own first use (item 23).
    See [Packed sets](#packed-sets).
26. **Keys of 18 to ~34 rows are left nested** (at 20-byte keys, 32-byte subkeys and 4 KiB pages). A
    packed set is held to a quarter-page entry and an outer tree holds no overflow cell, so such a
    key spends a leaf page where a composite table spends part of one; over a whole state the band
    costs little. See [Limits and non-goals](#limits-and-non-goals).
27. **A write through a positioned cursor routes by the cursor's slots** (`write::Hint`). The hint
    is only routing: it carries no page number, a hinted cell is followed only when the key lies in
    that child's range, and every check of the write descent runs on every page it enters, in the
    same order. See [Detached cursors](#detached-cursors-txncursor) and [Dup cursors](#dup-cursors).
28. **A tree page's free gap is not covered by its CRC** (`page::crc`, `page::free_gap`). Nothing
    reads the gap, every byte a reader interprets is covered (`nkeys` and `upper`, which bound the
    gap, included), and hashing the gap pulled stale bytes from memory only to hash them. See
    [Tree pages](#tree-pages-pagers).
29. **A cursor seek checks only the keys its search compared** (`TreeCursor::descend` for a key).
    The leaf a seek lands in gets the full range check before any read past its landing entry, so
    every entry returned past the landing comes from a leaf checked in full; most seeks never read
    past their landing, and the full check on every seek was a measurable share of the read path.
    See [Readers and snapshots](#readers-and-snapshots).

---

## Source files

One row per file of `src/`. `CLAUDE.md`'s crate map gives each file's entry points and the
invariants a change must keep.

| file | contents |
|---|---|
| `lib.rs` | crate docs, `compile_error!` off unix, re-exports |
| `error.rs` | `Error`, `Result` |
| `stats.rs` | `IoStats`, the writer's I/O and allocation counters (`Env::io_stats`) |
| `env.rs` | `Env`, `EnvOptions`, `SyncMode`, `EnvStat`: open and recovery (and the read-only open), the writer hand-off, snapshots, the reader queue, the durable steps of a commit, `sync` |
| `txn.rs` | `RoTxn`, `RwTxn`, `ReadTxn`, `Table`, `TableStat`, `RoCursor` (the owned read cursor), `RwCursor`, `TxnCursor` (the detached write cursor, with the transaction's write stamps, schema counter and serial); dup tables' point ops, `delete_prefix` and the per-transaction map of their nested trees, flushed at commit; commit steps 1–3 and 7 |
| `meta.rs` | the 128-byte meta record, `choose_meta`, `slot_to_write` |
| `page.rs` | the slotted page codec (inline, overflow and nested leaf cells, `Val`), `Geometry` (limits, `place`: inline or overflow), `validate` |
| `packed.rs` | the packed-set codec of a dup table's outer cell: `PackedView` (the O(1) header and per-row checks, `find`), `validate` (the whole set), the rebuilds (`build_one`, `build_with`, `build_without`), `PACKED_MIN` |
| `overflow.rs` | overflow runs: the 12-byte cell reference (`Ovf`), the run codec, `checked_run` (every check a run read makes), `MAX_VALUE` |
| `tree/` | `TreeRoot`, the `PageRead`/`PageWrite` stores, checked descent and lookup (`read.rs`), copy-on-write put/delete/split/merge/free (`write.rs`), the bidirectional cursor (`cursor.rs`), the dup cursors' two-level walker (`dup_cursor.rs`), the test-only `MemStore` and tree walkers (`test_harness.rs`) |
| `alloc.rs` | the writer's page allocator (`private`, `stamped`, `pool`, `freed`, `next_pgno`) and its overflow runs (`allocate_run`, `free_run`) |
| `stamp.rs` | commit step 3's CRCs on several threads for a large commit (`threads`, `crcs`, `put_crc`) |
| `freelist.rs` | free-record codec, gated loading, the fixed-point save |
| `catalog.rs` | the table catalog (`TableRecord`, the 96-byte record of a plain or a dup table; `TableRecord::usable`, the limits of this build; `DupOptions`) |
| `check.rs` | `Env::check`: the full structural check (every overflow run's CRC included, a dup table's nested trees, packed sets and totals) and exact page accounting |
| `latch.rs` | the failure latch |
| `file.rs` | `PageFile` (reserved mapping, preallocated growth capped by `EnvOptions::max_growth`, the close's tail trim `trim_to`, msync, poison), `ReadOnlyFile` (a read-only open's `PROT_READ` mapping) and `MapView` (the lock-free reader view of either) |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `os_page_size`, `flock_exclusive`, `flock_shared` |
| `crc.rs`, `page_set.rs` | copied from `crates/storage/src/archive/` (`page_set.rs` adds a touched-word list, so `clear` and `drain` cost the words touched, not the file) |
| `disk_model.rs` | test-only page-cache and power-loss model |
| `tests/`, `*/tests.rs` | crash, damage (nested cells and dup records included), dup tables against a composite-key model, multi-table model, merging, path cache, append, detached cursor, key tail, concurrency and read-only suites, per-module tests |
| `bench.rs` | the on-demand benchmark of step 4's data sync and of merging, each against its test-only switch, and the on-demand probe of step 3's thread count (`stamp_probe`) |

---

## Tests and benchmarks

The default tests are small and fast, because CI runs nextest in debug. The heavy variants are
`#[ignore]`d `_heavy` twins of small tests and run with `--release`.

- **Per-module tests** (a module's `mod tests`, or its `*/tests.rs`) hold each codec and component
  to a model or a reference: the page, packed-set, overflow, meta and catalog codecs, the
  allocator, the free list, the stamp, and the tree's copy-on-write, merging and cursors on the
  in-memory store (`tree/test_harness.rs`'s `MemStore`); `env/tests.rs` and `env/io_stats_tests.rs`
  cover open, commit, close, the latch and the I/O counters.
- **Whole-engine suites** (`src/tests/`): the crash matrices (every `CrashAt` point under a
  process crash, a strict power loss and random early writeback, through `disk_model`), the damage
  sweeps (each damage refused at its first use, the environment latched), multi-table models with
  crashes, dup tables and their cursors against a composite-key twin, the path cache, append,
  detached cursors, key tails, concurrency and the read-only open.
- **On-demand measurements**, `#[ignore]`d and skipped by the heavy run: the benchmark and the stamp
  probe below.

`CLAUDE.md`'s "Where tests live" maps each kind of change to the tests that cover it, test by test,
and its "Adding a crash test" gives the crash-test recipe.

```text
cargo nextest run -p tn-lmdb                                    # default suite (debug)
cargo test --release -p tn-lmdb -- --ignored --skip tn_lmdb_sync_and_merge --skip stamp_probe  # heavy variants (about 10 min)
cargo test --release -p tn-lmdb tn_lmdb_sync_and_merge -- --ignored --nocapture --test-threads 1  # benchmark
cargo test --release -p tn-lmdb stamp_probe -- --ignored --nocapture --test-threads 1  # step 3's probe
```

### Benchmark: step 4 and merging

`bench.rs` (`tn_lmdb_sync_and_merge`) times two engine choices against their test-only switches,
in fresh environments on seeded streams (32-byte random keys, 100-byte values):

- **small:** 2000 transactions of 3 puts, each committed and timed, with step 4's own choice
  against step 4 forced to one `fdatasync` for every commit (`Writer::data_by_fdatasync`, the
  "fdatasync" rows). The two modes alternate in blocks of 100 transactions (A B B A order) on the
  same table, so neither gains from running second. They differ only on one-range commits, which
  step 4 syncs by one `msync`.
- **churn:** ten rounds over a table of N/4 random keys, each a fill and a delete of 90% of the
  keys at random, timed as a whole; then the table's pages, the file's used span and its size. It
  runs with merging and without (`Writer::no_merge`, the "churn (no merge)" rows), to show what
  merging saves.

```text
cargo test --release -p tn-lmdb tn_lmdb_sync_and_merge -- --ignored --nocapture --test-threads 1
```

`TN_LMDB_BENCH_N` sets N (default 1,000,000), `TN_LMDB_BENCH_ENGINES` the columns (`t4d` and `t16d`,
`Durable` with 4 and 16 KiB pages; `t4n`, 4 KiB `NoSync`, which has no step 4 and runs only the
first mode), and `TN_LMDB_BENCH_ROWS` the groups (`small`, `churn`). Put the temp dir on a real disk
(`TMPDIR`, scoped to the one command): on a tmpfs syncs cost nothing, and on Linux the run stops.

**The stamp probe** (`stamp_probe`, also `#[ignore]`d) prices commit step 3 against its thread
count. A durable environment of 4 KiB pages holds 400k keys with 999-byte values, four to a leaf;
each measured commit replaces every value, so step 3 stamps about 100k private tree pages (~393
MiB), written just before by copy-on-write. Each commit's `IoStats::stamp_ns` is read at 1, 2, 3,
4, 6, 8 and 12 threads (`Writer::stamp_threads`), every count once a round in an order rotated
each round, over 7 rounds: the median and least time per count, and GB/s. A second table gives one
thread's `crc::crc32` rate over a 1 MiB buffer, repeated (cache-resident), and over a 400 MiB one
(DRAM-resident): whether one thread's CRC is bound by its compute or by memory.

**Merging** (Apple M1 Max, macOS, 2026-10-21, N = 300k, medians of three runs, at 4 KiB pages; code
before the owned and detached cursors): after the churn the table took 2.3 MiB of pages, against
13.1 MiB without merging, and the churn took 729.2 ms against 742.5 ms. The delete rows of the
same runs stayed within a few percent of merging off, and the other rows were unchanged.

Comparison benchmarks live in the reth adapter crate:
[`tn-lmdb-reth/engine-bench.md`](../tn-lmdb-reth/engine-bench.md) runs this engine through its own
API, and the adapter's [README](../tn-lmdb-reth/README.md) runs reth's database benches.

#### Step 4: one msync or one fdatasync

Step 4 syncs a commit's dirty pages with one call: one `msync` when they merge into one range of
at most 16 runs, and one `fdatasync` of the file otherwise (do-not-flag item 10). The numbers
behind the rule:

- **Linux, small commits.** Each `msync(MS_SYNC)` runs the file system's fsync path, drive-cache
  flush included, so syncing range by range paid one flush per range. In the A/B rows of three
  runs, one `fdatasync` beat the per-range msyncs in all 18 cells: median 904–920 → 515–516 us
  at 4K, 703–931 → 532–568 us at 16K, with lower p99s and totals.
- **macOS, small commits.** One `fdatasync` was about 35% cheaper than the per-range msyncs
  (75–84 us vs 114–130 us median). Neither is a drive-cache flush on macOS, so this measures only
  the kernel paths.
- **macOS, bulk commits.** A 1M-entry load dirties one range. One `fdatasync` of it cost 15–26%
  more than one `msync` of the range plus the growth sync: sequential commit 34.9 → 43.9 ms,
  random 87.9 → 106.1 ms at 4K, confirmed by a repeat. So one range keeps the `msync`, which is
  also a single flush on Linux.
- **macOS, scattered commits.** An msync writes its range's dirty pages one cluster at a time, so
  its cost follows the dirty pages, not the span. In the mixed row at 200k entries, a 1000-update
  commit dirtied about 835 pages at 16K, in about 620 runs, which the 64-page merge gap folded
  into 2 ranges spanning the file. Their msyncs took about 14.6 ms, and one `fdatasync` 5.3 ms
  (16.5 vs 3.7 ms at 4K). Syncing the runs one by one, or a smaller merge gap, cost the same as
  the merged span. So past 16 runs even one range takes the `fdatasync`.

The Linux numbers are the A/B rows of three runs on a Linux x86_64 laptop (i7-1360P, NVMe, ext4),
2026-10-20; the macOS ones come from an Apple M1 Max (internal SSD, APFS), 2026-10-20, in targeted
runs on the same code. They were taken when these rows ran in the comparison bench, whose runs are
in [`engine-bench.md`](../tn-lmdb-reth/engine-bench.md).

**After the change**, on the Mac (medians of three targeted runs, 2026-10-20), the
small-commit median is 73.2 us at 4K and 90.2 us at 16K (it was 114 and 130 us; the "fdatasync"
rows read 72 and 81 us in the same runs), and the bulk commits, the mixed row and `clear_table`
are level (sequential commit 34.5 vs 34.9 ms, random 85.5 vs 87.9 ms at 4K). On Linux the small
commits are not yet measured under the new rule (see
[`engine-bench.md`](../tn-lmdb-reth/engine-bench.md#linux-run)).

**Open:** at 16K on the Mac, a one-range small commit synced by `msync` is still slower than by
`fdatasync` (median 89–90 vs 81–82 us, p99 158–165 vs 96–97 us). At 4K the two match.

---

## Milestones

- **M1, the crash-safe core:** copy-on-write B+trees in one page file, two metas, gated page
  reuse, the persisted free list, the failure latch, `Env::check`, and the crash and damage
  suites. Its performance work added provenance trust, the path cache, `append` and cursor puts;
  step 4 then became one sync (an `msync` of one range, else one `fdatasync`).
- **M2, what reth's tables need:** format v2 with page counters in every `TreeRoot`; overflow runs
  for values up to 16 MiB, with run allocation (first fit, gated records loaded for a fit, tail
  reuse) and a record's leftover pages listed again under the consumed keys; merging on delete;
  a delete in one descent; the read-only open; owned (`RoCursor`) and detached (`TxnCursor`)
  cursors; `insert` (`KeyExists`); prefix steps; a cursor that rests on the follower after a
  delete, so a walk that deletes as it goes skips nothing; `KeyTail`; and the reth adapter, [`tn-lmdb-reth`](../tn-lmdb-reth/README.md).
- **M3, TN's reth database:** TN runs its reth database on tn-lmdb behind the `tn-lmdb` feature,
  migrating an MDBX one at its first open (`crates/tn-reth`'s README); dup tables as nested
  trees with packed small sets ([Dup tables](#dup-tables)); leaf values inline up to half a page;
  a growth cap (`EnvOptions::max_growth`) and a clean close that gives back the preallocated
  tail ([Close](#close)). On 2026-11-04 TN's reth execution bench on Linux (six load profiles,
  A B B A, commits judged where both engines' drive syncs ran fast) put tn-lmdb within 2% of MDBX
  at peak and busy load and within 5% on the other four profiles, with its tables within 1–3% of
  MDBX's size. Of the remaining peak cost, most is step 3's page CRCs, which MDBX does not have.
- **Next:** a faster page checksum for step 3 (the checks stay); the reth storage changes that
  move the changesets out of the database; and a bound on `NoSync`'s growth, which the reth
  integration needs before it accepts `--db.sync-mode safe-no-sync`.
