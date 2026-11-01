# `tndb-b`: a copy-on-write B+tree key-value engine

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

The crate is self-contained: it copies its primitives (CRC, page set, sync wrappers, mapping) and
does not depend on `tn-storage` or reth. reth's database API on top of it is a separate crate,
[`tndb-b-reth`](../tndb-b-reth/README.md).

| file | contents |
|---|---|
| `lib.rs` | crate docs, `compile_error!` off unix, re-exports |
| `error.rs` | `Error`, `Result` |
| `env.rs` | `Env`, `EnvOptions`, `SyncMode`, `EnvStat`: open and recovery (and the read-only open), the writer hand-off, snapshots, the reader queue, the durable steps of a commit, `sync` |
| `txn.rs` | `RoTxn`, `RwTxn`, `ReadTxn`, `Table`, `TableStat`, `RoCursor` (the owned read cursor), `RwCursor`, `TxnCursor` (the detached write cursor, with the transaction's write stamps, schema counter and serial); dup tables' point ops, `delete_prefix` and the per-transaction map of their nested trees, flushed at commit; commit steps 1–3 and 7 |
| `meta.rs` | the 128-byte meta record, `choose_meta`, `slot_to_write` |
| `page.rs` | the slotted page codec (inline, overflow and nested leaf cells, `Val`), `Geometry` (limits, `place`: inline or overflow), `validate` |
| `packed.rs` | the packed-set codec of a dup table's outer cell: `PackedView` (the O(1) header and per-row checks, `find`), `validate` (the whole set), the rebuilds (`build_one`, `build_with`, `build_without`), `PACKED_MIN` |
| `overflow.rs` | overflow runs: the 12-byte cell reference (`Ovf`), the run codec, `checked_run` (every check a run read makes), `MAX_VALUE` |
| `tree/` | `TreeRoot`, the `PageRead`/`PageWrite` stores, checked descent and lookup (`read.rs`), copy-on-write put/delete/split/merge/free (`write.rs`), the bidirectional cursor (`cursor.rs`) |
| `alloc.rs` | the writer's page allocator (`private`, `stamped`, `pool`, `freed`, `next_pgno`) and its overflow runs (`allocate_run`, `free_run`) |
| `stamp.rs` | commit step 3's CRCs on several threads for a large commit (`threads`, `crcs`, `put_crc`) |
| `freelist.rs` | free-record codec, gated loading, the fixed-point save |
| `catalog.rs` | the table catalog (`TableRecord`, the 96-byte record of a plain or a dup table; `TableRecord::usable`, the limits of this build; `DupOptions`) |
| `check.rs` | `Env::check`: the full structural check (every overflow run's CRC included, a dup table's nested trees, packed sets and totals) and exact page accounting |
| `latch.rs` | the failure latch |
| `file.rs` | `PageFile` (reserved mapping, preallocated growth, msync, poison), `ReadOnlyFile` (a read-only open's `PROT_READ` mapping) and `MapView` (the lock-free reader view of either) |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `os_page_size`, `flock_exclusive`, `flock_shared` |
| `crc.rs`, `page_set.rs` | copied from `crates/storage/src/archive/` (`page_set.rs` adds a touched-word list, so `clear` and `drain` cost the words touched, not the file) |
| `disk_model.rs` | test-only page-cache and power-loss model |
| `tests/`, `*/tests.rs` | crash, damage (nested cells and dup records included), dup tables against a composite-key model, multi-table model, merging, path cache, append, detached cursor, key tail, concurrency and read-only suites, per-module tests |
| `bench.rs` | the on-demand benchmark of step 4's data sync and of merging, each against its test-only switch |

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
  delete, so a walk that deletes as it goes skips nothing; `KeyTail`; and the reth adapter, [`tndb-b-reth`](../tndb-b-reth/README.md).
- **Next:** a fresh Linux run of the engine's benches (its Linux tables, in the adapter crate's
  [`engine-bench.md`](../tndb-b-reth/engine-bench.md), predate M2; the adapter's benches passed
  their Linux gate on 2026-10-26, see its README); TN integration behind a feature; and the open
  performance items: the descent of a small random read, dup-table reads, the commit of overflow
  runs, the one-range `msync` at 16 KiB on macOS, and 1–4 KB values taking a run page at 4 KiB
  pages (see [`engine-bench.md`](../tndb-b-reth/engine-bench.md),
  [Step 4](#step-4-one-msync-or-one-fdatasync) and the adapter's README).

---

## Contents

1. [Concepts at a glance](#concepts-at-a-glance)
2. [On-disk format](#on-disk-format)
3. [Deletes and merging](#deletes-and-merging)
4. [Dup tables](#dup-tables)
5. [Commit protocol](#commit-protocol)
6. [Free list and page reuse](#free-list-and-page-reuse)
7. [Readers and snapshots](#readers-and-snapshots)
8. [Open and recovery](#open-and-recovery)
9. [Failure latch](#failure-latch)
10. [Concurrency rules](#concurrency-rules)
11. [Public API reference](#public-api-reference)
12. [Errors](#errors)
13. [Platform notes](#platform-notes)
14. [Limits and non-goals](#limits-and-non-goals)
15. [Intentional design decisions: please do not flag these](#intentional-design-decisions-please-do-not-flag-these)
16. [Tests and benchmarks](#tests-and-benchmarks)

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
  lock.
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
| 0 | 8 | magic `b"tndb-b\0\0"` |
| 8 | 4 | format version = 3 |
| 12 | 4 | page size P |
| 16 | 8 | txn id |
| 24 | 4 | page count (pages `0..page_count` are allocated) |
| 28 | 4 | next table id (first table: 1) |
| 32 | 32 | catalog `TreeRoot` |
| 64 | 32 | free-tree `TreeRoot` |
| 96 | 28 | reserved (zero) |
| 124 | 4 | CRC32 over bytes 0..124 |

Decoding (`Meta::decode`) runs its checks in this order:

1. The magic must match, else `NotTndbB`.
2. The version must be 3, else `UnsupportedVersion`. This runs before the CRC check, because
   another version may lay the record out differently. The format is pre-release: a file of
   version 1 or 2 is refused (`UnsupportedVersion(1)`, `UnsupportedVersion(2)`), with no
   migration. Version 3 added the nested cell (leaf cell flags 2) and the 96-byte catalog record;
   the meta record's layout is unchanged.
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
trailer (4):  CRC32 over bytes 0..P−4
```

- **Cells tile `upper..P−4` exactly.** Deleting or shrinking a cell closes its gap at once, so a
  page never fragments. Free space is `upper − (24 + 2·nkeys)`, and `free + used == U` always
  holds.
- **The free gap need not be zero.** `PageWrite::alloc` does not zero a new page, and a page
  laid out there (`page::init_header`, then its cells and slots) writes every byte but the gap,
  which keeps the page's previous bytes (debug builds fill a new page with `tree::POISON`). A
  delete or a shrinking replace leaves bytes in the gap too: the moved cells' old bytes and the
  slot array's old last slot (`remove_cell`, `shift_front`). The CRC covers them as it covers
  every byte of the page, and nothing ever reads them (do-not-flag item 17).
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
- **CRC trailer.** It is stamped at commit with `add_crc32_nonzero`, which maps a computed CRC of 0
  to a fixed non-zero value. **An all-zero trailer means "dirty"**: a page being built by the
  writer. A published page with a zero trailer is `Corrupt`.

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

`U = P − 28` (the header plus the trailer), and `MAX_ENTRY = U / 4` (a cell plus its slot). The
derived limits are `max_key = MAX_ENTRY − 12`, so any key fits a branch cell as a separator, and
the inline limit `max_value(klen) = MAX_ENTRY − 10 − klen` for `klen <= max_key` (0 for a longer
key, which is refused with any value). `page::validate` holds every stored page to both: a leaf
entry within `MAX_ENTRY` can still carry a key up to 2 bytes over `max_key`, so its key is
checked too.

**Where a table value goes** (`Geometry::place`): inline when it fits `max_value(klen)`, so every
inline layout is the one it was before overflow runs; else in an overflow run, when the key leaves
room for the 12-byte overflow cell in a maximal entry, which every key up to `max_key − 10` bytes
does. A table therefore takes values up to `MAX_VALUE` = 16 MiB with such a key
(`Geometry::max_table_value`). A key of `max_key − 9` to `max_key` bytes keeps the inline limit.
The catalog and the free tree hold inline values only (`Geometry::check_entry`); an overflow or
nested cell in either is `Corrupt` (`Val::inline`).

| P | U | MAX_ENTRY | max key | inline value (8-byte key) | inline value (32-byte key) | overflow value (key up to) | a 1-page run holds | a 16 MiB value takes |
|---|---|---|---|---|---|---|---|---|
| 4 KiB | 4068 | 1017 | 1005 | 999 | 975 | 16 MiB (995 B) | 4068 B | 4097 pages |
| 16 KiB | 16356 | 4089 | 4077 | 4071 | 4047 | 16 MiB (4067 B) | 16356 B | 1025 pages |
| 64 KiB | 65508 | 16377 | 16365 | 16359 | 16335 | 16 MiB (16355 B) | 65508 B | 257 pages |

**One split always suffices.** An overflowing page holds at most `U + MAX_ENTRY` bytes. Cutting
it at the first entry where the left part reaches half leaves both parts within U. An overflow
cell is an ordinary inline cell of `8 + klen + 12` bytes, so this holds with runs too.

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
the table limit (16 MiB for a key that qualifies).

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
one whose `packed_max` is neither 0 nor in `7..=max_value(key_len)` ("packed_max {n} outside
7..={max} for this page size": no build writes one, since a set past that would not be an inline
entry). A plain record always passes.

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
  `max_value(key_len)` bytes, so it is always inline);
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
`max_value(key_len)`, so every offset fits a `u16`.

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

### Free-list records

- **Key** (12 bytes): `txn u64 BE | seq u32 BE`. The key holds a transaction and a sequence
  number, so records sort by txn. The txn is the reuse gate the pages wait for: the committing
  transaction for the pages it freed, or the key of a record it took for the pages it took and
  left unused (see [Free list and page reuse](#free-list-and-page-reuse)).
- **Value:** page numbers as `u32` LE, strictly ascending. A value holds at most
  `record_capacity = max_value(12) / 4` page numbers (248 at 4 KiB). Empty values are allowed.

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
  (do-not-flag item 9 says why).
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
`max_dup_key_len = MAX_ENTRY − 42` with `InvalidDupOptions`: the outer cell, the key and a
32-byte nested root, must be an inline entry (975 bytes at 4 KiB pages, 4047 at 16 KiB, 16335 at
64 KiB). `packed_max` is 0 (never pack), or 7 (`PACKED_MIN`, the smallest set) to
`max_value(key_len)`, the longest value an inline entry of the key takes (987 bytes at a 20-byte
key and 4 KiB pages), or `DupOptions::PACKED_DEFAULT` (`u16::MAX`, never a limit itself), which
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
- **The write stamp.** Every packed change (a create, rewrite, row delete, cell removal or
  promotion) moves the table's write stamp, even an in-place rewrite that leaves the outer root's
  bytes as they were: it is the only sign a dup cursor gets of a change to a key with no map
  entry. A miss, a refused write and a get move nothing.

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
  `seek_dup` then `current_with_key_tail(t)` (the adapter's `seek_by_key_subkey`). A `k` of
  another width is `KeyTooShort` or `KeyTooLarge` and changes nothing. A miss returns `None` and
  leaves the cursor unset, as `seek_exact`'s miss does, never on the next key's row, and reads no
  other key's nested tree (only the outer key its outer seek lands beside, for its width). On the
  twin a bounded seek is `seek(k ‖ s)` kept only if the row starts with `k`.
- **Key tails.** `current_with_key_tail(t)` and `seek_with_key_tail(x, t)` give the logical key
  and, for `t <= |s|` and an inline value, `KeyTail::Joined(s[|s|−t..] ‖ v)`, one slice of the
  nested leaf (the adapter's `t = |s|` is the stored row `s ‖ rest`, with no copy); for a value
  in a run, or `t > |s|`, `KeyTail::Apart(v)`, the caller joining the key's last `t` bytes. Both
  forms keep `KeyTail`'s meaning, so the joined bytes are the twin's.
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
  checks, the slot's key first, else a search of the leaf when the key lies within its first and
  last keys), and from the outer root (`read::find_outer`) when that leaf lacks it.
- A write to a packed key, a promotion included, rewrites the outer cell, so the cursor finds its
  row again by key (the write reports no slot, `DupPlaced::slot` `None`); `delete_current` of a
  set's row places by key on the follower, and of a set's only row (the cell removed) from the
  outer path cache, as for an emptied nested key.

**Errors.** Damage a move meets (an outer key of another width, an outer cell that is not a nested
cell or, in a table that packs, a packed set, an empty or damaged nested root, a packed set that
fails a check, a subkey of another length, any check of a descent) is
`Corrupt`, latches, and leaves the cursor unset (a `DupTxnCursor` out of the deleted state too).
`WrongTxn`, `StaleTable`, the width refusals and refused writes change nothing and do not latch.

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
| 3 | Stamp the CRC of every page the transaction created (they become "stamped by this writer"). An overflow run gets one CRC over all its pages, never one per page (`stamp_private`, `PrivatePages::units`), and its pages never count as stamped. Below 2 MiB of units the writer stamps each unit in turn. From 2 MiB (`stamp::threads`: a second thread at 2 MiB, one more per MiB, at most 4 and the CPUs the process may use) it computes the CRCs on several threads first (`Writer::unit_crcs`, `stamp::crcs`): each thread reads a contiguous share of the units through `PageFile::pages` slices while the writer holds the file shared, so no page changes and no `unsafe` is added; then the writer stores each CRC in unit order through `PageFile::pages_mut` (`stamp::put_crc`). The bytes, their order and the errors are the serial loop's, and the crash points are unchanged. |
| 4 | Sync the dirty pages (`data_sync`), every page of every new run included. Runs separated by at most 64 clean pages (`MERGE_GAP`) merge into one range. If the pages form exactly one range, in at most 16 runs of adjacent pages (`MAX_RUNS`), one `msync(MS_SYNC)` of that range; otherwise one `fdatasync` of the file (do-not-flag item 10 says why). |
| 5 | If the file grew since the last sync, run `fdatasync_file` so its size is durable. After step 4's `fdatasync` this has nothing left to do: that call made the size durable too. |
| 6 | Copy meta M into the slot holding the lower txn (an invalid slot first; on a tie, slot 1), msync that one page, then record M's txn as the slot's. |
| 7 | Publish: swap in the new snapshot, push `(M, Weak<Snapshot>)` on the reader queue. |

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
`ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, table errors, `ReadOnly`, `WrongTxn`) happen in
`put` and the other calls, before anything changes, and do not latch.

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

**Leftover pages are reusable at once.** When a transaction consumes a record but uses only some
of its pages, the rest stay in `pool`, and step 2 lists them again under a consumed key. No two
records list one page: the records consumed are deleted in step 1, and their keys are free for
step 2. Listing them under M instead made them wait two more commits, longer
under a held reader: each commit then took more records and left more over, and under churn of a
tenth of the pages per commit with readers held the file reached 2.5 times its live pages, against
1.6 now (`run_churn_with_readers_keeps_the_file_bounded`).

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
so it resolves each handle once. `open_table` reads the catalog every time. Do-not-flag item 15
says why the cache is safe.

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
- a branch sits above the tree's height and a leaf at it;
- only the root leaf may be empty;
- keys lie within the intersection of every ancestor's separator range (the cursor checks each
  page entered; a point miss checks the leaf's neighbours of the key).

The cursor's descent (`TreeCursor::descend`) parses each page's header once (`page::View::new`),
after the fetch, identity and kind checks; the separator-range check, the empty-page checks and
the search then read through that one view, in that order. The leaf's view goes to the move that
settles there (`Reached`), which reads its entry through it: `seek_exact`'s hit is the search's
own `Find::Found` value, not a second read of the cell. A crossing to the next or previous leaf
climbs the path to the nearest branch with a child beyond it (`climb`) and descends from that
child (`land_beyond`).

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
the leaf is a miss, the cursor unset. These are the entries a descent from the root finds: the
cursor entered the leaf with a descent's checks, so its keys lie inside the range all its
ancestors' separators give it (the argument of do-not-flag items 11 and 12), and so does a key
between its first and last keys, whose descent takes the cursor's own path. The leaf is the page
`Cursor` holds, `RoCursor`'s `Snapshot::page_again` (unchecked, item 14) and a `TxnCursor`'s
`Writer::page` (checked). A seek that descends meets damage with the descent's exact message;
damage in the leaf the cursor already holds is met only by those re-fetches, as for a step (item
14). `RwCursor`'s seeks, and the re-seeks a write cursor makes after a change to its tree, descend
from the root.

**The writer skips them on pages it built.** Its descent (`write::descend_write`) trusts a page
this transaction's writer built from checked pages and still holds private: the root when its
holder's flag says the writer wrote it, and below a trusted page every private child. A trusted
page gets only the identity, kind and non-empty-leaf checks, which read the header line that is
loaded anyway. The per-fetch checks and the separator-range check run on the untrusted
(published) pages, the only ones damage at rest or a redirected pointer can reach, and so does
`verify_published` unless this writer stamped the page. See
item 11 of the [do-not-flag list](#intentional-design-decisions-please-do-not-flag-these).

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
and skip the CRC (`RunCrc::Skip`), since they never read the value. Do-not-flag item 13 gives the
cost.

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
passed every check when the cursor entered it, and the transaction's pages cannot change while
the cursor borrows it. Entering a leaf always runs the full descent checks.

The owned `RoCursor` cannot hold a borrowed page between calls, so each `next` and `prev` fetches
the page of the leaf it stands on again, by page number, through `Snapshot::page_again`: a plain
slice of the mapping without the per-fetch checks, which that page passed when the cursor entered
it (a snapshot's pages do not change while it lives). A seek from an entry starts from that
leaf, fetched the same way (above). Every other move, and every leaf it enters, runs the same
checks as `Cursor`'s.

---

## Open and recovery

`Env::open` (`env.rs`) runs these steps:

1. **Validate the page size.** `options.page_size` must be a valid page size, else
   `InvalidPageSize`, even for an existing file.
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
   - No valid meta is `Corrupt` if any slot carries the magic, else `NotTndbB`.
5. **Txn check.** A meta txn of `u64::MAX` (no next txn) is `Corrupt`.
6. **Length check.** A file shorter than `page_count × P` is `Corrupt`. Open never falls back to
   the older meta.
7. **`fsync_file` once.** A crash may have left a meta in the page cache whose msync never
   finished. What open reads must be durable before anything is built on it.
8. **Map** inside a reservation of `max(map_reserve, file length)`, advised `MADV_RANDOM` unless
   `readahead` is set (do-not-flag item 16); a remap past the reservation is advised the same way.
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
2. **Never create.** A missing file is `Io(NotFound)`. An empty file is `NotTndbB` (step 4 finds
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

---

## Failure latch

`latch.rs` keeps the first cause and never clears it, and only the first cause is logged
(`tracing::error!`, target `tndb_b`).

| latches (later writes and commits return `Failed`) | does not latch |
|---|---|
| any error inside `commit` or `sync` (`fail`; `fail_damaged` for `Corrupt`) | `KeyTooLarge`, `ValueTooLarge`, `AppendOutOfOrder`, `KeyExists`, `ReadOnly` |
| an I/O or growth error in a write (`fail`); a failed msync also poisons the file, which is never retried | `TableNotFound`, `StaleTable`, `InvalidTableName`, `WrongTxn` |
| a panic part way through a write, commit or sync (`UnwindLatch` guard) | a miss (`Ok(None)`) |
| `Corrupt` from any read, get or cursor move (`fail_damaged`) | errors from `Env::open` (there is no environment yet) |
| a failed `Env::check` | |

After a latch, `begin_rw` and `try_begin_rw` return `Failed` (`ReadOnly` in a read-only
environment), and reads continue on the published snapshots. `Env::failure()` reports the cause.
A latched NoSync environment skips its final sync on drop.

---

## Concurrency rules

| type | thread safety |
|---|---|
| `Env` | `Clone + Send + Sync`. Clones share one environment, which closes when the last clone and the last transaction drop. |
| `RoTxn` | `Send + Sync`. It holds an `Arc` of the environment and of its snapshot, so the mapping outlives it. |
| `RwTxn` | `Send + Sync`. It owns the boxed writer state, taken through a `Mutex` + `Condvar` hand-off. |
| `Cursor<'t>`, `RwCursor<'t>` | borrow their transaction (`Cursor` is `Send + Sync`). |
| `RoCursor` | `Send + Sync`, no lifetime. It owns an `Arc<RoTxn>`, so the snapshot stays readable, and its pages unreused, until the last cursor and handle of that transaction drop. |
| `TxnCursor` | `Send + Sync`, no lifetime, and no borrow of its `RwTxn`: each call takes the transaction (`&RwTxn` for a move, `&mut RwTxn` for a write), so any number of cursors share one write transaction. A caller that shares the transaction between handles (reth's `DbTxMut`, whose methods take `&self`) wraps the `RwTxn` in a `Mutex` and holds the guard for each call; the entries a move returns borrow the guard. |

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

## Public API reference

| item | description |
|---|---|
| `Env::open(path, EnvOptions) -> Result<Env>` | open or create, then recover (see above) |
| `Env::open_read_only(path, EnvOptions) -> Result<Env>` | open an existing file no writer holds, read-only (see [Read-only open](#read-only-open)): a shared lock, the same meta choice and root checks, a read-only mapping; never creates, writes or syncs; `begin_rw`/`try_begin_rw` return `ReadOnly` |
| `EnvOptions { page_size, map_reserve, sync, readahead }` | defaults: 4096, 1 GiB, `SyncMode::Durable`, `false` on Linux and `true` elsewhere. `page_size` applies at create; an existing file keeps its own. `readahead: false` advises every mapping of the file `MADV_RANDOM`; `true` leaves the kernel's readahead (do-not-flag item 16). |
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
| `Env::max_key_len() / max_value_len(klen)` | the entry limits of this page size. `max_value_len` is the table limit: 16 MiB for a key up to `max_key_len − 10` bytes, else the inline limit. A value over the inline limit is stored in an overflow run transparently: `put`, `get` and the cursors take and return its bytes as for any value. |
| `Table::name() / id()` | a handle's name and id. A committed table's id is never another table's over the file's life, so the id tells apart the tables whose handles open cursors in one transaction; it alone does not name a table (an aborted creation's id goes to the next table created) |
| `ReadTxn` (both txn kinds) | `open_table(name) -> Option<Table>` (either kind), `get` (a dup table's by the logical key `k ‖ s`), `cursor` (`TableKind` on a dup table), `txn_id`, `table_stat -> TableStat { entries, height, branch_pages, leaf_pages, overflow_pages, nested_trees, dup }` (O(1), from the table's record; a dup table's figures in [Dup tables](#dup-tables)) |
| `RwTxn::create_table(name)` | open or create a plain table (`TableKind` on a dup table) |
| `RwTxn::create_dup_table(name, DupOptions { key_len, packed_max })` | open or create a dup table: rows keyed `k ‖ s`, `k` of exactly `key_len` bytes (1 to `max_dup_key_len`, 975 at 4 KiB pages), `packed_max` 0 (never pack), 7 to `max_value(key_len)` bytes, or `DupOptions::PACKED_DEFAULT` for that most (`DupOptions::packed(key_len)`), else `InvalidDupOptions`; the table records the resolved number; an existing table of the other kind or other resolved options is `TableKind` |
| `RwTxn::drop_table(Table) / clear_table(&Table)` | free the table's pages, a dup table's nested trees included (both are copy-on-write: readers keep the old tree) |
| `RwTxn::put / delete -> bool` | point writes; on a dup table by the logical key `k ‖ s` (a key with no subkey is `KeyTooShort`) |
| `RwTxn::delete_prefix(&Table, k) -> u64` | a dup table's rows under the outer key `k` (exactly `key_len` bytes) removed, its nested tree freed in one walk; returns the rows removed (`TableKind` on a plain table) |
| `RwTxn::insert(table, key, value)` | put only a key the table does not hold: a present key is `KeyExists`, found by the put's own search (in the table's cached leaf, else by the descent's plan) and refused before anything changes, with no latch; a new key is stored exactly as `put` stores it |
| `RwTxn::append(table, key, value)` | put a new last entry: `key` must sort strictly after the table's last key (an empty table takes any key), else `AppendOutOfOrder`, refused before anything changes. An append into the table's cached rightmost leaf neither descends nor searches; the cache holds the leaf of the table's last put or append in this transaction (after an append split, the new right leaf) until a page is allocated or freed, and any other append descends. |
| `RwTxn::cursor_mut -> RwCursor` | cursor moves and prefix steps plus `put`, `append` (as `RwTxn::append`), `insert` (as `RwTxn::insert`) and `delete_current`. A put, append or insert leaves the cursor at its entry: placed from the path it wrote (after an append split, from the new path the split refills the table's path cache with), or re-sought by key after any other split; a refused one leaves the cursor where it was. A delete re-seeks by key, so after any merges the cursor is on the deleted entry's follower in a deleted state, so a walk that deletes as it goes skips nothing: `next` returns the follower without stepping past it, `current` returns it too, `prev` returns the entry before it, and another `delete_current` deletes it. Right after a delete the prefix steps compare with the deleted key: the follower (`next_in_prefix`) or the entry before (`prev_in_prefix`) if it shares the prefix, else `None` with the cursor still on the follower in the deleted state. With no follower the cursor is after the end: `next`, `current` and `delete_current` find nothing and `prev` returns the new last entry. |
| `RwTxn::cursor_detached(&self, &Table) -> TxnCursor` | a write cursor that holds no borrow of the transaction: its moves (`first`, `last`, `next`, `prev`, `current`, `seek`, `seek_exact`, the prefix steps) take `&RwTxn` and return entries borrowed from it; its writes (`put`, `insert`, `append`, `delete_current`) take `&mut RwTxn`; after a delete, `follower_shares_prefix(n)` (whether the follower shares the deleted key's first `n` bytes, from the keys the cursor kept, without moving it) takes `&RwTxn`. The states, the writes' placement and the deleted state are `RwCursor`'s. After `last` (or `prev` from unset) the cursor is on that entry as after any move: `next` and `next_in_prefix` return an entry another writer has put past it since, else `None` (the cursor after the end). Several cursors, over one table or several, and the transaction's own writes interleave, each seeing the others' writes; see [Detached cursors](#detached-cursors-txncursor) below. |
| `RwTxn::commit() -> txn id / abort()` | finish |
| `Cursor` | `first`, `last`, `next`, `prev`, `current`, `seek` (first `>= key`), `seek_exact`, and the prefix steps `next_in_prefix(n)`, `prev_in_prefix(n)` (below) |
| `RoTxn::cursor_owned(self: &Arc<RoTxn>, &Table) -> RoCursor` | a read cursor that owns a share of its transaction: no lifetime, `Send + Sync`, the same moves, states and prefix steps as `Cursor`; the entries it returns, overflow values included, borrow the cursor until its next call. Each step fetches its leaf again by page number (see [Readers and snapshots](#readers-and-snapshots)), so a scan costs about 4–8% more per entry than `Cursor`'s (macOS). |
| `current_with_key_tail(t) -> (key, KeyTail)` on `Cursor`, `RoCursor` and `TxnCursor` | the current entry with its value joined to its key's last `t` bytes (`t` clamped to the key's length): `KeyTail::Joined` for an inline value, the key's tail then the value as one slice of the page (a leaf cell holds its value right after its key), so with no copy; `KeyTail::Apart` for a value in an overflow run, the value alone. The reth adapter reads a dup row (subkey, then the rest) this way. |
| `seek_with_key_tail(key, t) -> (key, KeyTail)` on `RoCursor` and `TxnCursor` | `seek(key)` then `current_with_key_tail(t)` from one leaf access: the entry is read through the leaf the descent fetched and parsed, an overflow value once; `None` (after the end) when no key is `>= key`. The cursor is left exactly as `seek` leaves it (a `TxnCursor` saves the landed key for its reseek). The reth adapter's `seek_by_key_subkey` reads its row this way. |
| `RoTxn::dup_cursor_owned(self: &Arc<RoTxn>, &Table) -> DupRoCursor` | a read cursor over a dup table, owned as `RoCursor` is (`TableKind` on a plain table): `RoCursor`'s moves, prefix steps and key-tail calls on the logical keys `k ‖ s`, each giving what `RoCursor` gives on the composite twin, plus the bounded seeks `seek_key(k)`, `seek_dup(k, s)` and `seek_dup_with_key_tail(k, s, t)` (a miss unset, no other key's tree read); see [Dup cursors](#dup-cursors) |
| `reset()` on `RoCursor` and `DupRoCursor` | forget the position: the cursor then behaves exactly as a new one from `cursor_owned` / `dup_cursor_owned` on the same snapshot and table (unset), and keeps its allocations (a dup cursor's logical-key buffer), so a pool of cursors reuses one with no descent and no allocation |
| `RwTxn::dup_cursor_detached(&self, &Table) -> DupTxnCursor` | a detached write cursor over a dup table (`TableKind` on a plain table): `TxnCursor`'s moves, writes (`put`, `insert`, `append` by the logical key, `delete_current`), deleted state, `follower_shares_prefix` and staleness on logical keys, plus `DupRoCursor`'s bounded seeks; its results borrow the cursor and the transaction; see [Dup cursors](#dup-cursors) |
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
| `InvalidDupOptions(String)` | `create_dup_table` with a `packed_max` neither 0, `PACKED_DEFAULT` nor in `7..=max_value(key_len)`, or a `key_len` of 0 or past `max_dup_key_len` (no latch) |
| `NotTndbB` | a non-empty file without the magic in either meta slot |
| `UnsupportedVersion(u32)` | another format version in a meta slot |
| `InvalidPageSize(u32)` | `EnvOptions::page_size` is not a power of two in 4K..=64K |
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
  Growth is geometric: at least `clamp(len, 1 MiB, 1 GiB)` more, rounded to the OS page. Past
  the reservation the mapping moves to a new reservation of at least twice the size. Retired
  mappings are kept until close while a reader may hold a view of them.
- **macOS (APFS): first writes into preallocated space after an `fsync` are slow.** Writing
  through the mapping into file space reserved with `F_PREALLOCATE` before an `fsync` takes slow
  first-touch page faults: 6–24 us per 12 KiB in a standalone C test, against 1.3 us when the
  space was preallocated after the `fsync`, when an `msync(MS_SYNC)` replaced the `fsync`, or when
  the file grew sparse. Growth leaves slack past the pages in use, and a durable commit that grew
  the file fsyncs it (the size sync, or the `fdatasync` data sync), so the puts of a long-lived
  environment into slack grown before a durable commit pay this, on overflow runs and tree
  pages alike. When the comparison benchmark's `big` rows
  ([`engine-bench.md`](../tndb-b-reth/engine-bench.md#large-values-overflow-runs)) ran in one
  shared environment, a `t4d` put of an 8 KiB value took 9.5 us, against 2.0 us in a fresh
  environment; most of it was the run's first-touch faults. A file grown sparse does not pay
  this, but tndb-b keeps growing with real blocks by design: a full disk must be an `io::Error`, not a `SIGBUS`. The
  cause is specific to macOS; Linux is not measured.
  - The order of those first writes matters too. A put reserves its pages before it writes them,
    so it can write a page after pages past it. `Writer::alloc` therefore writes one byte of
    each page it takes from the file end, which makes each page's first write follow allocation
    order. Without that store, the adapter's sequential-write row of about 1.2 KiB values
    (one-page overflow runs between tree pages, durable) took 4.40 us per put against 3.80 us
    with it, and the comparison benchmark's 32 KiB puts at 16 KiB pages 1.45x as long, on an
    Apple M1 Max. With the store, both matched the old zero-filled pages. The store moves the
    page's first fault earlier and adds none, as the layout writes the page anyway.
- **A large commit stamps its CRCs on up to 4 threads** (commit step 3, `stamp::threads`). The
  cap is also the CPUs the process may use (`std::thread::available_parallelism`, which on Linux
  reads the cgroup quota; read once, at the first large commit). The CRC is memory-bound past
  about 4 threads: on an Apple M1 Max, 400 MiB of 4 KiB units took 16.9 ms on one thread, 5.9 ms
  on 4 and 4.5 ms on 8, and 2 MiB 84 us on one against 66 us on 2, the threshold. The commit
  shares the machine with the node's other work, so it takes no more.

---

## Limits and non-goals

The entry limits are in [Limits](#limits): a table takes values up to 16 MiB, in overflow runs.
These limits and non-goals are known:

- **Dup tables have owned and detached cursors only, and pack only what fits an inline entry.**
  A dup table's cursors are `DupRoCursor` and `DupTxnCursor`, and the borrowed `ReadTxn::cursor`
  and `RwTxn::cursor_mut` are `TableKind` on it ([Dup tables](#dup-tables)). A packed set is at
  most `max_value(key_len)` bytes, so a key past that holds a nested tree of at least one page
  (do-not-flag item 26), and one in a table with `packed_max` 0 always does. Leaf cell flags other than 0, 1 (overflow) and 2 (nested) are
  refused.
- **The inline limit is about a quarter of a page** (`MAX_ENTRY = U / 4`, kept for its one-split
  proof): about 1 KB at 4 KiB pages. A value of 1–4 KB at 4 KiB pages
  therefore takes a run page of its own. Raising it needs a new split proof and a format change.
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

1. **No `F_FULLFSYNC` on macOS** (see [Platform notes](#platform-notes)). The commit path is
   msync, which is no stronger. A full drive flush per sync would cost 10–20 ms and buy nothing
   the commits keep.
2. **`unsafe` in five places.** Each block carries a `SAFETY:` comment.
   - `file.rs`: a read-only open's mapping (`map`), `msync`, and the `from_raw_parts` page
     slices. The writer's reservation is mapped by the safe `map_raw` as an `MmapRaw`, which
     hands out only raw pointers: every page slice, the writer's and the readers', is formed from
     the mapping's base pointer over exactly its own bytes, so no reference to the whole mapping
     ever exists. These are sound under the single-writer model: the base is stable inside the
     reservation, readers see only published, immutable pages, retired mappings outlive every
     view of them, and a read-only open's shared lock keeps every writer out.
     Copy-on-write follows the readers' pattern (`Writer::copy_published`): the writer borrows
     the published source through its clone of the readers' view (`MapView::slice`) while it
     writes the private destination through `page_mut`. Each slice is exactly its own page, and
     the two are never the same page, because a private source is refused. It adds no `unsafe`.
   - `sys.rs`: the libc calls `fsync`, `fallocate`, `fcntl(F_PREALLOCATE)`, `sysconf` and
     `flock`, each on a live file descriptor (`sysconf` takes none).
   - `crc.rs`: the aarch64 PMULL kernel (`pmull::update`, a copy of `tn-storage`'s), called only
     after the CPU feature is detected at runtime.
   - `page::prefetch`: a one-instruction prefetch hint (`prfm` or `prefetcht0`) that the binary
     searches issue for the cells of their next probe. A hint never faults or changes memory, and
     its address is a byte of the page.
   - the test-only `disk_model.rs`: a copy out of a live mapping into the model's own buffer.
3. **Reads do not recompute tree pages' CRCs, nor a 1-page overflow run's.** The CRC is checked
   where it matters: before copy-on-write, on the roots at open, on free-list leaves before reuse,
   and in `Env::check`, which verifies every page and every run. The per-fetch checks (range,
   non-zero trailer, header pgno, txn) catch misdirected and reused pages, and a 1-page run's
   header checks (pgno, kind, txn identity, length) do the same for it. The exception is a run of
   more than one page, whose full CRC every read verifies (item 13).
4. **`child_txn` and `root_txn` hold only the low 32 bits of a txn.** They are a damage detector:
   a sound tree never points at a reused page (the reuse gate guarantees it), and a pointer
   redirected to a stale copy goes unnoticed only if that copy's txn matches modulo 2^32.
5. **The file is longer than `page_count × P`.** That is growth slack and harmless; the meta's
   page count is authoritative.
6. **A torn or invalid meta slot is not repaired at open.** Open never writes. The slot counts as
   the lowest and is the next one a commit overwrites.
7. **Leftover pages go back under the consumed keys in page order, not each under its own
   record's key.** A page can then sit under a key older than the record that listed it. That is
   sound: a key's txn only has to be one the gate has passed, every key consumed had, and the gate
   never moves back, so no state a durable meta or a reader can hold reaches the page. Pool pages
   past what the consumed keys hold (pages the transaction added to the file and freed again) wait
   two commits under the committing txn, as freed pages do.
8. **`Env::sync` blocks while a write transaction is open.** It needs the writer state, so calling
   it while holding an `RwTxn` deadlocks. This is documented, not guarded.
9. **Merging is merge-only, at a quarter, and local** (see
   [Deletes and merging](#deletes-and-merging)). A page a delete changes absorbs a same-parent
   sibling only while it holds under a quarter of `U`, and only when the pair fits one page.
   Nothing is borrowed or redistributed between siblings, and pages no delete changes are not
   revisited. Pages are reused through the free list, so there is no compaction pass either. Why:
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
10. **Step 4 is one sync: an `msync` of one range, or one `fdatasync` of the file.** Dirty pages
    that merge into one range (gaps of at most 64 clean pages, at most 16 runs) take one `msync`
    of it; any other set takes one `fdatasync`. Why (the numbers are in
    [Step 4](#step-4-one-msync-or-one-fdatasync)):
    - On Linux every `msync(MS_SYNC)` runs the file system's fsync path, drive-cache flush
      included, so a commit of k ranges synced range by range pays k flushes, and one `fdatasync`
      pays one. One `fdatasync` beat the per-range msyncs in every small-commit cell measured.
    - A one-range bulk commit keeps the `msync`: on macOS one `fdatasync` of it cost 15–26% more,
      and on Linux the one `msync` is a single flush too.
    - Past 16 runs, even runs that merge into one range take the `fdatasync`: on macOS an msync
      of a scattered range is about three times slower.

    `fdatasync` also writes any other dirty page of the file. At a commit, that can only be a page
    of an aborted transaction, which is a free page, so writing it is harmless. A crash test can no
    longer stop a commit between data ranges, because there is one sync; partial writeback of the
    dirty pages is still modelled by `power_loss_random`, and the crash matrix runs every point
    on both paths (`Writer::data_by_msync`).
11. **The writer trusts its own pages by provenance** (`write::descend_write`):
    `trusted(child) = trusted(parent) && is_private(child)`, and the root is trusted only when its
    holder's flag says this txn's writer wrote it. Trusted pages skip the per-fetch checks,
    `verify_published` and the separator-range check. `is_private` alone is not enough, and the
    rules that close the gap are deliberate:
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
    exactly the bounds `route` gave on the way down. A descent routed by a cursor's hint (item 27)
    applies these rules unchanged on every page it enters.
12. **A table put or delete can skip the descent** (`write::PathCache`, `write::put_cached`,
    `write::delete_cached`). Each table's working record caches the leaf its last put or delete
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
13. **Every read of an overflow value held in a run of more than one page verifies the run's full
    CRC** (`RunCrc::MultiPage`), so a get of such a value reads all its bytes. Pages 2..n of a run
    carry no header: a middle page reused or redirected by damage passes every header check of
    the first page, and only the CRC catches it. The rule is never wrong data, so the CRC stays.
    It costs a pass over the value. In the comparison benchmark's `big` rows
    ([`engine-bench.md`](../tndb-b-reth/engine-bench.md#large-values-overflow-runs); macOS,
    medians of three, each value size in a fresh environment) a get takes about 1.1 us at 8 KiB
    and 2.1 us at 32 KiB at 4 KiB pages (`t4d`, runs of 3 and 9 pages), and 1.4 and 2.6 us at
    16 KiB pages (`t16d`). With the CRC skipped in an experiment, the same `t4d` and `t16d` gets took 0.34–0.40 us. A 1-page run
    has no page without a header, so its read skips the CRC as a leaf's does (item 3): that took
    a `t4d` 2 KiB get from 724 to 302 ns and a `t16d` 8 KiB get from 1.30 us to 271 ns.
14. **An owned cursor's step and seek re-fetch its leaf without the per-fetch checks**
    (`Snapshot::page_again`, used by `RoCursor::next`, `prev`, `current_with_key_tail`, `seek`,
    `seek_exact` and `seek_with_key_tail`). The page number is the one the
    cursor's descent entered with every check, and a snapshot's published pages never change while
    an `Arc` of it lives (the reuse gate holds them), so the checks would see the same bytes and
    pass again. `Cursor` relies on the same fact when it keeps the page slice itself, for its steps
    and its seeks. A seek settles in that leaf only for a key between its first and last keys;
    any other seek descends from the root with every check, so damage in the leaf the cursor holds
    meets a later seek only if the seek descends ([Readers and snapshots](#readers-and-snapshots)).
    Leaves the cursor enters, and every other move, run the full checks. A safe slice re-derived from the mapping
    replaces a cached raw pointer, so the owned cursor adds no `unsafe`.
15. **A snapshot resolves each table handle once** (`TableRoots`, see [Readers and
    snapshots](#readers-and-snapshots)). Later transactions on the snapshot take the remembered root
    without reading the catalog again. A snapshot's catalog never changes while an `Arc` of it lives
    (the reuse gate holds its pages), so a second lookup would read the same bytes, pass the same
    checks and give the same root. Only a resolved handle is remembered, so a refused one
    (`StaleTable`, `TableNotFound`) is checked again at each use, and a hit compares the name as
    well as the id (`Table::is`), so a handle with the same id and another name misses: one of
    another file, or one of a table an aborted transaction created (the next table created takes its
    id). Each commit publishes a new snapshot with nothing remembered, so every handle is checked
    against each new catalog. Damage in the catalog is met by the first resolve in each snapshot and
    by every `open_table`, which never uses the cache.
16. **On Linux every mapping of the file is advised `MADV_RANDOM`** (`file::advise_access`),
    unless `EnvOptions::readahead` is set: the writer's mapping at open and after each remap past
    the reservation, and a read-only environment's. Elsewhere `readahead` defaults to `true`, so no
    advice is given unless it is cleared. Why:
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
17. **A new page is not zero-filled, and copy-on-write copies a page outside its free gap, so
    the gap holds old bytes** (`PageWrite::alloc`, `tree::write::copy_page`,
    [Tree pages](#tree-pages-pagers)). A free gap, and an empty page's unwritten bytes (all of it
    past the header up to the trailer), hold whatever the page held before. That can be bytes of
    a freed page, including data since deleted. A copy-on-write copy writes the source's header
    and slots, then its cells and trailer; the gap between them keeps the destination's old
    bytes.
    - The bytes are covered by the page CRC, are the same for the same history of operations (two
      files driven through the same writes stay byte-identical), and are never read: no read,
      `page::validate` or `Env::check` looks between the slot array and `upper`.
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
18. **Inside a write transaction a dup table's outer cell can name a superseded nested root**
    (`Work::flush_nested`, see [Dup tables](#dup-tables)).
    - Why: a nested write changes only its nested tree and the transaction's map entry for the
      key; the outer cell is rewritten once per changed key, at commit. Rewriting it on every
      write would copy and rewrite the outer path per row.
    - The map is the truth for its key until commit: every key whose tree the transaction
      created or changed keeps its entry until commit or the key's removal, entries are never
      evicted, and every reader of an outer cell inside the transaction looks in the map first.
      A stale cell's root may even name pages since freed and reused; nothing follows it.
    - A packed set is never stale: a packed key has no map entry and its cell is rewritten at
      each write (item 22).
    - Published outer pages never hold a stale cell: the flush runs before step 1, and a cell on
      a published page is committed. Debug builds assert after the flush that every entry's cell
      holds its root.
19. **A dup table created with `packed_max` 0 holds every outer key in a nested tree of at least
    one page.**
    - Why: 0 asks for no packing, and such a table takes exactly the code and the messages it
      took before packed sets existed (`Cells::Outer`, an inline outer cell `Corrupt`). A key
      with one row costs a leaf page of its own. `DupOptions::packed` gives the bound that packs
      most; the reth adapter creates every engine dup table with it.
20. **A write transaction keeps one path cache per outer key it writes** (the boxed
    `write::PathCache` of each map entry, about 1 KiB).
    - Why: keys written in turn each keep their own cached leaf, so interleaved keys do not
      evict each other's path; the map's entries are never evicted (item 18).
    - The memory is bounded by the transaction's own work: every such key already owns at
      least one private page (a new tree's leaf, or the copy of a committed tree's leaf), so the
      map stays a fraction of the transaction's dirty pages, and it drops at commit or abort. A
      packed key has no entry and no path cache of its own (item 22).
21. **The dup cursors' shape** (`DupRoCursor`, `DupTxnCursor`, see [Dup cursors](#dup-cursors)).
    - A bounded seek's miss (`seek_key`, `seek_dup`, `seek_dup_with_key_tail`) leaves the cursor
      unset, as a `seek_exact` miss does, not after the end or on the next key's first row: the
      call asks about one key, landing on the next would read that key's rows, and the reth
      adapter treats a missed `seek_by_key_subkey` as a miss whatever the cursor's state.
    - Any change to the table's write stamp, a nested-only write to another key included, makes
      every dup cursor of the table reseek, as every `TxnCursor` of a changed table does. A
      separate outer stamp (a reseek of the nested level alone when the outer tree did not
      move) waits for a profile that shows the reseeks.
    - A `DupTxnCursor`'s results borrow the cursor as well as the transaction: the logical key
      is the cursor's copy (no page holds `k ‖ s` in one piece). The adapter's write cursor
      reborrows both for each call.
    - `current_with_key_tail(t)` with `t` past the subkey gives the value `Apart`, not joined
      in a buffer: that would copy the value, `Apart` keeps `KeyTail`'s meaning, and the
      adapter asks only `t = |s|`.
    - There are no borrowed dup cursors (analogs of `ReadTxn::cursor` and `RwTxn::cursor_mut`)
      and no `delete_prefix` on the cursor: no caller needs them, and the transaction's
      `delete_prefix` plus the reseek leaves the wipe loop's state.
    - `DupTxnCursor` shares `TxnCursor`'s checks (`Tracker`) but keeps its own `sync`,
      `refresh`, `retarget` and `seek_call`: they move the level-specific position, and a trait
      over both positions would sit on the plain cursor's hot path. Oracle 2b
      (`dup_txn_cursors_and_direct_writes_match_the_composite_twin`) holds the two copies to the
      same behaviour.
22. **A packed write rewrites the key's outer cell at once; packed sets never enter the
    transaction's map** (`txn::dup_entry`, `write::put_outer_cell` with `OuterCell::Packed`).
    - Why: the copy is the same either way. A set is immutable and at most `packed_max` bytes,
      so a set held in the map would still be rebuilt on every write; writing it at once adds
      only the outer cell's replace, which after the transaction's first write to that leaf is an
      in-place replace in a private leaf. Nested roots are deferred (item 18) because their cell
      changes on every nested write of a big key; a packed key has few rows by definition.
    - Correctness: a set held in the map would leave a stale but valid-looking cell until the
      flush, and nothing would catch a reader that forgot the map (a stale nested root names
      private pages, a stale set names none). Written at once, the cell is the truth for every
      packed key, so reads, the dup cursors, the free walks, `delete_prefix` and `Env::check`
      need no packed overlay, and the commit's flush stays nested-only.
    - Invariant P: a key whose outer cell is a packed set has no map entry. Entries come only
      from a nested cell, a nested create and a promotion, and leave with their cell. Debug
      builds assert it in `txn::dup_entry`; a free walk's hit count enforces it in every build.
    - The cost: each write to a packed key copies its set (at most ~1 KB at 4 KiB pages), so a
      key's whole packed phase in a bulk append copies at most ~17 KB before it promotes.
23. **A read checks only the packed rows it touches** (see [Packed sets](#packed-sets)).
    - `get` runs `PackedView::new` (O(1)) and the rows its binary search probes; a cursor checks
      each row it steps to, and its order against the row before. Every write that decodes a
      set, the free walks and `Env::check` validate the whole set first.
    - Why: damage is `Corrupt` at first use, at the granularity of what the read touches, the
      rule pages and the subkey width checks follow. The whole check on every get would cost
      several times the lazy one on a full set (estimated ~150-250 ns against ~30-40 ns at a
      2 KB set), up to half again a packed get's outer descent.
    - The exposure: a misorder off a get's probe path passes that get
      (`the_lazy_checks_refuse_what_they_touch` pins it); a write to the key, a cursor's walk of
      it or `Env::check` refuses it.
24. **A nested key never demotes.** A key past its table's bound moves its rows to a nested tree
    and stays nested however few rows it keeps, one included (`a_nested_key_never_demotes`).
    - Why: a key that shrinks under the bound and grows past it in turn would rebuild its rows
      each time; a nested key with few rows costs one leaf, as every key did before packed sets.
      A demotion would also need a path that turns a map entry back into a cell, against
      invariant P (item 22).
    - A key whose last row is deleted leaves with its cell; a later put creates it afresh,
      packed if its first row fits, in the same transaction too
      (`a_nested_key_emptied_then_recreated_is_packed_in_one_txn`). That is a new key, not a
      demotion.
25. **A packed set is not checked at page validation or copy-on-write.**
    - `page::validate` cannot tell a dup table's outer leaf from any other leaf, so a flags-0
      value is an ordinary inline value there. `check_cells_naming_this_txn` (copy-on-write and a
      merge of a published outer leaf) skips a set under `Cells::Packing`: it names no page, so
      the provenance check has nothing to look at, its bytes are under the page CRC
      `verify_published` checks just before, and the page mutators never parse it, so a bad set
      cannot make them write a bad page.
    - Checking every set of a copied leaf would be cheap, but would make copy-on-write the first
      use of keys the writer never touched, unlike every other cell kind. Each set is checked at
      its own first use (item 23).
26. **Keys of 18 to ~34 rows are left nested** (PlainStorageState's shape: 20-byte keys, 32-byte
    subkeys, 4 KiB pages).
    - A packed set must be an inline entry (`packed_max` at most `max_value(key_len)`, 987 bytes
      at a 20-byte key), since an outer tree holds no overflow cell. A key past ~17 such rows
      therefore nests and spends a leaf page, where a composite table (rows keyed `k ‖ s` in a
      plain table) spends 0.52-0.99 pages: up to ~2x the composite file for that band.
    - Closing it would need sets past the inline limit, so overflow runs in an outer tree. Over a
      whole state the band costs little: a random state of 1 to 20 rows per contract takes 0.286
      pages per contract at the default bound against composite's 0.298
      (`packed_sets_file_size_by_bound`, an ignored measurement).
27. **A write through a positioned cursor routes by the cursor's slots** (`write::Hint`,
    `write::descend_write`, `TxnCursor`, `DupTxnCursor`; see [Detached
    cursors](#detached-cursors-txncursor) and [Dup cursors](#dup-cursors)). A reader's path is
    used by a writer, and that is sound because the hint is only routing:
    - it carries slots, no page number: every page is reached from the write's own root through
      cells the descent read, so a hint from another tree, a stale one or a forged one can name no
      page;
    - a hinted cell is followed only when the key lies in that child's range (its cell's key and
      the next cell's), the range a search's route gives on a branch whose keys ascend, and a
      leaf's hinted slot only when it holds the key; anything else is a refusal, and the search
      takes over from that level down;
    - every check of the write descent runs on every page it enters, in the same order (the
      provenance rule of item 11, the per-fetch checks, identity, kind, `verify_published`,
      `check_cells_naming_this_txn`, `keys_in_range`), so the hint never trusts a page the
      search would check. A hinted delete's damage on an unverified published branch is met by
      `verify_path` on the hit, with that check's message, `Corrupt` before any change as the
      search's is.
    The writer does not reuse the cursor's checks of the pages it entered (the cursor read them
    under the table stamp it saw): each write still pays the per-level checks, and the hint
    saves only each level's search (one `route_at` and two key compares in its place).

---

## Tests and benchmarks

The default tests are small and fast, because CI runs nextest in debug. The heavy variants are
`#[ignore]`d and run with `--release`.

| suite | covers |
|---|---|
| `page.rs` tests | leaf and branch codecs against proptest models; an insert at every index, the end included, against a model (`inserts_at_every_index_match_a_model`); `copy_short` against `copy_from_slice` at every length to 80, and its length-mismatch panic; the parsed views (`View`, `LeafView`, `BranchView`) against an independent model; splits at all five page sizes; `SplitBuf` splits byte-identical to a reference split (leaves and branches, the pending cell anywhere, a quarter of the cases at the append place); append fill; exact limits; a full page left byte-identical; 13 damage kinds; never panicking on fuzzed bytes; overflow cells are canonical references (flags 1 with a 12-byte value, a length past the inline limit and within 16 MiB), and the views model and damaged-page generator include them; nested cells are canonical roots (flags 2 with a 32-byte value that decodes to a non-empty, possible root; every other shape refused, by message: `nested_cells_are_canonical_roots`), and the views model, the damaged-page generator, the key-tail model and the insert-at-every-index model include them; `Geometry::place` keeps every value that fits inline and sends larger ones to runs of the right length (`place_keeps_m1_values_inline_and_sends_larger_ones_to_runs`) |
| `overflow.rs` | the run codec: a run reads back its value; each kind of damage to a run is `Corrupt` under every `RunCrc` (each header field re-stamped, a zero trailer, a flipped middle page, a range past the end or over a meta page, a page-number overflow, an unmapped run, a txn after the snapshot or not the cell's, a length that disagrees with the run or is over 16 MiB), private runs too; the test store reads runs against its snapshot |
| `tree/write.rs`, `tree/read.rs` | copy-on-write put and delete against a `BTreeMap` model over random batches (with a coverage gate on split and delete shapes), which also put large values (inline, 1-page and 2–10-page runs) and check the overflow counter and the run pages; a failed allocation leaves the tree identical, also when the put takes a run; a put into a run or a replace of one takes the descent, not the path cache; a published leaf naming a private run, and a copied leaf naming a run this txn allocates later, are refused; a delete refuses a handle miscounting its pages; after every put of appends, inserts and replaces through each split shape (a leaf split its parent takes, a branch split, a new root), the path cache equals a fresh descent (`an_append_split_caches_the_path_a_descent_finds`); a replace that grows the rightmost leaf's last value past its free space splits as an append, the leaf's other cells left byte for byte, in a table's tree and an outer tree that packs, while a grown key before the last or in another leaf splits in the middle (`a_growing_last_value_in_the_rightmost_leaf_splits_as_an_append`); a put or delete through a cache whose leaf was unlinked since, by deletes that bypassed it, descends and lands where the key lives (`a_path_cache_of_another_epoch_is_never_used`); with merging off, cached deletes leave a leaf's last entry to the descent, which unlinks the leaf (`a_cached_delete_leaves_a_leafs_last_entry_to_the_descent`); a delete's miss over a published leaf leaves the cache empty, so a put there copies the leaf (`a_delete_miss_over_a_published_leaf_leaves_the_path_cache_empty`); both against the same writes without the cache; split reservation for long separators; append fill; collapse and unlink; `clear` and the free walk refuse damaged or stale pointers, and a page private to the transaction for another tree (its leaf or a run's page) named by a published cell, an untrusted root handle or a forged cell a private copy kept (`the_free_walk_refuses_a_page_private_to_another_tree`); fetch checks, stale copies, redirected children and misrouted pointers are `Corrupt`; copy-on-write's one copy gives the two-step copy's bytes, on `MemStore` and the environment's writer (`copy_on_write_is_the_two_step_copy`), reads a page published before a remap from the new mapping (`copy_on_write_reads_a_page_published_before_a_remap`), and fails exactly as `PageRead::page` fails, a page past the published bytes included (`copy_on_write_fails_as_the_page_fetch_fails`); provenance trust: a trusted path skips the range checks, a private page named by a page this writer did not write (a stamped forged cell, an untrusted root handle, a collapse) is refused, as is a copied cell naming a page this txn allocates later, while a child whose txn wrapped stays live; a nested cell in a private leaf is `Corrupt` at a put, a cached put, a cached delete, a delete, `clear` and `free_subtree`, the handle unchanged (`a_nested_cell_in_a_private_leaf_is_corrupt_at_every_write`); an outer tree of nested cells against a model through every write shape, and the refusals of each cell policy (`outer_trees_of_nested_cells_match_a_model`); a published outer cell naming this txn's id refused at copy (`a_published_outer_cell_naming_this_txn_is_refused_at_copy`); `read::last_key` against a model (`last_key_is_the_trees_last_key`); an outer tree of `Cells::Packing` cells (packed sets and nested roots) through `put_outer_cell` and `delete_outer_cell` with splits and merges against a model, `Cells::Outer` still refusing an inline cell and `check_cells_naming_this_txn` skipping sets (`outer_trees_of_packing_cells_match_a_model`) A write descent routed by a cursor's hint against the search: every key of seeded trees of height 1 to 4, private and published pages, with its own path, a neighbour's, random slots, a hint of another length and another tree's, gives the search's leaf, path, range, copies or error (`a_hinted_descent_matches_the_search`, heavy twin), and a hint along a damaged path meets the damage as the search does, a put with the same `Corrupt`, a delete before any change (`a_hinted_write_meets_damage_as_a_descent_does`). |
| `tree/write.rs` merging | the random batches with merging on, against the same model (`random_batches_with_merging_match_a_btreemap_model`): odd rounds put and delete through a `PathCache` (`put_cached`, `delete_cached`; debug builds require cached deletes among them), and after every delete, cached or not, the pages it changed are merge-closed and the accounting holds, and the coverage gate adds leaves merged from the left and from the right, branch merges, a cascade over two levels or more, a root giving way and a refused merge; the fill bound (`deleting_90_percent_leaves_no_page_merge_open`: after 90% of a random fill is deleted at random, no page under a quarter fits with a neighbour, and the leaves stay 0.40–0.47 full against about 0.08 without merging); a merge refuses a damaged sibling from either side before anything changes (`a_merge_refuses_a_damaged_leaf_sibling`, `a_merge_refuses_a_damaged_branch_sibling`: a flipped byte under a stale CRC, a stale txn in the parent's cell, an older retired copy, a redirect outside the separators, a private sibling, a cell or overflow cell naming this txn, and for branches a key out of order across the seam); a refused neighbour gets only the cheap checks, and damage only its CRC finds waits for its first use (`a_refused_neighbour_gets_only_the_cheap_checks`); a failed allocation during a merging delete leaves the tree identical (`a_failed_allocation_during_a_merging_delete_leaves_the_tree_identical`); an absorbed branch sibling's child-0 chain is descended to the leaves, so a single-child branch outside the cell's separators, a branch of another level and one branch named by two cells are refused (`a_merge_refuses_a_single_child_branch_outside_the_cell`, `a_merge_refuses_a_branch_sibling_of_another_level`, `a_merge_refuses_a_branch_named_by_two_cells`). `a_collapse_checks_the_page_it_promotes` and `a_delete_refuses_a_handle_miscounting_its_pages` turn merging off (`no_merge`) to reach their shapes |
| `tree/cursor/tests.rs` | random moves and prefix steps against a sorted model on the detached cursor and the public `Cursor`, over many tree shapes (grouped keys among them) and page sizes, committed and uncommitted; the ends of empty and single-entry trees; leaf crossings; seeks; prefix steps from every entry of a grouped tree, across leaf boundaries both ways (`prefix_steps_from_every_entry_match_a_model`); every key and every gap of many tree shapes through `seek`, `seek_exact` and `seek_with_key_tail`, each followed by a few steps, against the model on both cursors (`seeks_of_every_key_and_gap_match_a_model`, `_heavy`); `seek_with_key_tail` meets damage (a misrouted cell, an emptied leaf, a root of the wrong height, a flipped run byte) with the error a `seek` then `current_with_key_tail` meets (`a_seek_with_key_tail_meets_damage_as_a_seek_does`); two damages on one page met in the descent's order, the error the first check's exact message: the kind check before the header parse and before the range check (`the_descent_names_the_first_check_a_page_fails`); the path stays 6 bytes a level (`tree_cursor_stays_small`); seeks from random positions (after seeks, steps, `first` and `last`, past either end, after a `seek_exact` miss) equal a new cursor's seek and the steps after it, on the detached and public cursors and on `RoCursor` and `TxnCursor` (from the deleted state, and stale after another writer's put or delete), over every key and gap, every separator and the keys around it (`seeks_from_random_positions_match_a_fresh_seek`, `_heavy`); a seek inside the cursor's leaf (its keys and gaps, `seek_exact` misses there) makes no root descent and any other one does, even next to the leaf, a stale `TxnCursor`'s seek one descent and no reseek, and one after only a table created or dropped none (`a_seek_near_the_cursor_starts_from_its_leaf`, `owned_and_txn_cursors_seek_from_their_leaf`, `a_txn_cursor_seeks_from_its_leaf_after_a_schema_change`, debug builds); damage in the next leaf, sought from the leaf's end, is met with the descent's message, damage on the held path is not met again (`a_seek_from_the_leaf_meets_damage_as_a_descent_does`); damage under a cursor latches; `RwCursor` writes and prefix steps against a model (`current` and a second delete right after `delete_current` act on the follower), and prefix steps after `delete_current`; the owned `RoCursor` against the model and `Cursor` on one snapshot with overflow values (the random moves include `seek_with_key_tail`), outliving its transaction handle on another thread while commits free its pages, and latching damage |
| `meta.rs`, `catalog.rs`, `latch.rs`, `alloc.rs`, `file.rs`, `sys.rs`, `crc.rs`, `page_set.rs`, `disk_model.rs` | codecs (the 96-byte catalog record: its layout, a plain record's zero dup fields and each damaged byte, `table_record_encoding`; a dup record's round trip and each broken rule, the page and row-sum rules included, `dup_records_round_trip_and_refuse_damage`; a dup record held to this build's limits, a `packed_max` out of range and a key past `max_dup_key_len`, `dup_records_are_usable_within_this_builds_limits`; a version-2 meta refused, `a_version_2_record_is_refused`), slot choice, the latch's first-cause rule (`ReadOnly` does not latch), allocator abort, overflow runs in the allocator (the first fit in the pool, else the file end; one CRC per run, its pages never trusted as stamped; a freed run comes back as single pages), growth and remap, a read-only mapping of a mode-0444 file that maps nothing once dropped (`a_read_only_file_maps_its_bytes_until_dropped`), sync wrappers, the exclusive and shared locks (`flock_shared_coexists_and_excludes_an_exclusive_lock`), the power-loss model |
| `stamp.rs`, `env/tests.rs` (the stamp) | the parallel stamp against the serial loop (`crc::add_crc32_nonzero` on each unit), byte for byte: tree pages and runs of up to 9 pages, from no unit to more units than threads, on 1 to 9 threads (`crcs_stamp_the_bytes_of_the_serial_loop`); a payload whose CRC is 0 stamped with the all-ones sentinel (`a_zero_crc_payload_gets_the_serial_loops_sentinel`); the shares cover every unit once, in order, in balanced parts (`shares_cover_the_units_in_order_in_balanced_parts`); the thread count's threshold and cap (`threads_grow_with_the_bytes_to_the_cap`); whole commits stamped on 2, 3 and 5 threads leave the serial stamp's file, which reopens, checks and reads back (`a_parallel_stamp_writes_the_serial_stamps_bytes`), as does a 3 MiB commit of runs on `stamp::threads`' own choice (`a_commit_past_the_stamp_threshold_writes_the_serial_stamps_bytes`) |
| `env/tests.rs` | open steps (create at every page size, leftovers, the lock, a damaged newest meta, damaged roots, short and foreign files, the open fsync), commit and NoSync durability under power loss, a commit of no change, abort, the writer hand-off, readers across commits, step 4's sync choice (`data_sync`: one range or one `fdatasync`), a scattered commit and a two-range commit under power loss, the latch (failed msync, damage, oversized entries do not latch), table handles, a multi-page value read back after a commit and a reopen, `max_value_len` at every page size; the per-snapshot table roots: one catalog resolve per handle per snapshot, whichever read uses it (`a_snapshot_resolves_each_handle_once`), a dropped table's handle stale only in later snapshots (`a_dropped_tables_handle_is_stale_only_in_later_snapshots`), a hit needing the handle's name as well as its id, for another file's handle with the same id and for an aborted create's handle, whose id the next table created takes (`a_remembered_root_needs_the_handles_name_too`), and 40 tables, past the 32 slots, each resolving to its own table (`more_tables_than_a_snapshot_remembers_still_resolve`) |
| `freelist/tests.rs` | the record codec; reuse after two durable commits or two NoSync syncs, held back by a live reader; damaged records never handed out; abort returns the records taken; dropped and cleared tables' pages reused; steady churn keeps the file bounded; the save settles within three rounds, including a record count that shrinks mid-save; `check` reports damage, leaks and double uses, and compares each root's page counters, and refuses a root branch with one child (`check_refuses_a_root_branch_with_one_child`); exact accounting in a random workload. Overflow runs at the environment level: the runs of cleared and dropped tables are freed and reused; a damaged run (header, leaf, counter) refuses `clear_table` and `drop_table` and changes nothing, the file included; `check` reports a damaged run (a CRC flip, a header or pad byte, a wrong counter) and a run page also on the free list; `check` finds a flip inside a 1-page run that a get does not; a get refuses a 1-page run reused as another page and a flip in a middle page of a run. Run allocation: a run takes a fit from records loaded for it, loads at most `RUN_LOAD_RECORDS` records, and an abort returns the records it loaded; leftover pages of a consumed record are listed under the keys consumed and reused by the next transaction; churn of 4–20 KB values keeps the file bounded (`run_churn_keeps_the_file_bounded`; with held readers, `run_churn_with_readers_keeps_the_file_bounded`, at most 1.75 times the live pages, and both heavy twins, 1.5 times over 50K commits and 1.75 over 20K). Merges free the siblings they absorb, which the save lists like any freed page, and it still settles within three rounds (`merges_feed_the_save_which_still_settles`) |
| `tests/crash.rs` | every crash point (`CrashAt`) × {process crash, strict power loss, random power loss with three seeds} × {`Durable`, `NoSync`} × {step 4 by `fdatasync`, by one `msync`}: the reopen recovers exactly the allowed state, which passes `check`, reads back through gets and both cursor scans, and survives a crash right after the reopen; a commit that grew the file survives a power loss by either data sync; the older state stays recoverable until its meta is overwritten. The dup matrix runs the same over a dup table beside the plain ones (`every_crash_point_recovers_a_dup_table`, `_heavy`): a crash commit of nested-only changes (outer cells rewritten by the flush), a new outer key, a key emptied row by row and a `delete_prefix` reopens to exactly `L` or `M`, every row read back by a get and by a walk of every tree, with the totals and `check`'s figures; the grown-file and older-state cases with that commit (`a_commit_that_grew_the_file_survives_a_power_loss_with_a_dup_table`, `the_older_state_with_a_dup_table_stays_recoverable_until_its_meta_is_overwritten`) |
| `tests/crash_runs.rs` | the same matrix (two seeds; the heavy twin 40 at 4 and 16 KiB pages) on a workload of overflow runs: puts of 1–10-page runs, replaces inline to run, run to inline, run to a larger and a smaller run, deletes, a drop of a table with runs; the commit does not grow the file and ends with a run at the top of its pages, so the one `msync` of step 4 alone must cover the run's interior pages (a growth's size sync is an `fsync` of every page), and the dry run asserts that layout: the last run ends at the state's page count (`last_run_ends_the_file`); a commit that grows the file through a run allocation survives a power loss by either data sync |
| `tests/damage.rs` | each kind of damage at rest fails the open closed (the file unchanged) or, once opened, is met by reads, copy-on-write, free-list loads and `check` as `Corrupt`, which latches; nothing panics. A free record listing a live leaf of a tree without runs: a `drop_table` or `clear_table` of that tree after a write handed the leaf out (as a tree page or a 1-page run) is `Corrupt`, frees nothing and leaves the other table readable (`a_free_of_a_tree_refuses_a_leaf_this_transaction_holds_for_another`); after the write committed, the drop frees the leaf again and, once it is reused, the other table's gets are `Corrupt` by txn identity (`a_live_leaf_freed_by_a_later_drop_is_corrupt_once_reused`) |
| `tests/damage_runs.rs` | a table with inline values and runs of 1, 3 and 9 pages, damaged at rest: `Corrupt` at a get for a flipped byte or a zeroed page in the middle of a run, a zeroed last page (zero trailer), a cell redirected to another run of the same txn (of the same pages: only the length tells) or to its key's stale run of the same length (only the txn identity tells), a cell's length changed, a run's kind changed, a 1-page run's page number or kind changed; at `check` for a flipped byte inside a 1-page run and an overflow counter one too high, which `drop_table` and `clear_table` also refuse; a free record listing a live run's middle page is `Corrupt` at `check`, at the get after a write reused the page (every other value reads back), or at the commit of a write that freed the run while the page sat in the pool; two cells naming one run, both deleted in one transaction, are `Corrupt` at the commit, which latches as damage (`two_cells_naming_one_run_freed_together_are_corrupt_at_commit`) |
| `tests/damage_merge.rs` | a file whose deletes merge leaves (table `l`) and, after an unlink, branches (table `b`), checked undamaged (`the_fixture_deletes_merge`); damage at rest to the sibling a merge absorbs, from either side (`a_merge_refuses_a_damaged_leaf_sibling`, `a_merge_refuses_a_damaged_branch_sibling`): a flipped byte (the CRC finds it), a stale txn in the parent's cell, a redirect outside the cell's separators, a cell naming a page or run by this transaction's id past the file's end, and for branches the parent's key coming down equal to the right page's key 1 (the seam check). Each is `Corrupt` at the delete, the environment latches, and the file is byte for byte as it was |
| `tests/nested_damage.rs` | a nested cell forged into a committed table's published leaf, its CRC restamped so only the nested rule finds it: `Corrupt`, latched, at a get, a `Cursor` seek and a step onto it, a `RoCursor` seek, `seek_with_key_tail` on `RoCursor` and `TxnCursor`, a put over it, a put beside it (copy-on-write of its leaf), its delete, `drop_table`, `clear_table` and `Env::check` (`a_forged_nested_cell_is_corrupt_and_latches_at_every_use`); a dup record written over a plain table's inline cells: it opens, its stat and a same-options `create_dup_table` answer, `create_table` of its name is `TableKind` with no latch, a get of an absent outer key misses; then, each on a fresh copy, `Corrupt` and latched at a get (read and write transactions), a put, a delete, `delete_prefix`, a put of a new outer key (copy-on-write of the leaf), `drop_table`, `clear_table` and `Env::check` (`a_dup_record_over_inline_cells_is_corrupt_at_first_use`) |
| `tests/dup.rs` | dup tables: the kinds and options (`table_kinds_are_checked`, `dup_options_are_validated` at 4 and 16 KiB, `a_dup_record_with_packed_max_out_of_range_is_refused`, `every_bound_resolves_to_its_limit`, `an_empty_dup_tables_stat_is_its_shape`), the logical key's limits (`dup_keys_are_validated`), append by the logical key (`dup_append_orders_by_the_logical_key`), the working map and the flush (`reads_inside_the_txn_see_unflushed_roots`, `the_flush_writes_every_changed_root`, `nested_writes_move_the_table_stamp`, round trips, abort), the totals after every change against `table_stat` and a walk (`dup_totals_follow_every_change`), `delete_prefix`, drop and clear freeing every page with a reader held (`delete_prefix_frees_the_tree_and_its_cell`, `dropping_or_clearing_a_dup_table_frees_every_page`); forged damage, each `Corrupt` at first use: a cell naming a private page with no working root, a published outer cell naming this txn's id at copy-on-write, a nested cell inside a nested tree, a shared nested root (a double claim; reached twice at drop, nothing freed), totals off by one, an outer key of another width, an inline or overflow outer cell (`check` and the free walks); and Oracle 1, `dup_tables_match_a_composite_model` (a dup table at each of the three bounds, `Bound::Unpacked` (`packed_max` 0), `Bound::Small` (`PACKED_SMALL`, 96 bytes: most keys created packed and promoted mid-script) and `Bound::Default` (cold keys packed, hot ones promoted), with coverage gates proving each bound made its forms, and a plain table keyed `k ‖ s` against one `BTreeMap` model: every op's result, reads, readers, commits, aborts, reopens, clears and drops, `check` after every commit, coverage gates on outer and nested height 2; its `_heavy` twin runs 8 seeds of 40k steps at 4 and 16 KiB) |
| `tests/dup_cursor.rs` | `DupRoCursor` against the composite twin (a dup table of key width 4 and a plain table keyed `k ‖ s` with the same rows): Oracle 2a, `dup_cursor_moves_match_the_composite_twin`, random calls on a `DupRoCursor` and a `RoCursor` side by side, every result (tails compared as joined bytes) and state, with coverage gates on each prefix class (`n < K` across keys and inside one, `n = K` at a key's edge, `K < n <= L`, `n > L`), seek targets shorter than, as long as and longer than `K`, bounded-seek hits and misses, key crossings both ways and `Apart` tails of both causes; its `_heavy` twin (release) on 3000 keys, two of 40k rows, nested height 3; seeks from random positions against a fresh cursor's seek and two steps after, on both dup cursors, a `DupTxnCursor` stale after the transaction's writes near its row and in the deleted state among them, each seek's `ROOT_DESCENTS` at both levels its class's exactly in debug builds, every class met by both kinds (`dup_seeks_from_random_positions_match_a_fresh_seek`, `_heavy`); the bounded seeks read only their own key, the neighbours' nested roots garbled (`bounded_seeks_never_read_another_key`); the key tail joined inside the subkey, apart past it and for a run (`current_with_key_tail_joins_inside_the_subkey`); every landing checks the outer width (`a_dup_cursor_meets_an_outer_key_of_another_width_as_corrupt`); an empty subkey `Corrupt` at first use; `dup_cursors_on_a_plain_table_are_table_kind`; Oracle 2a and the random-position seeks run at the three bounds (the plain name the default, `_unpacked`, `_packed_small`), and the packed mode has its own cases: a walk across packed, nested and packed keys, prefix steps at a packed key's edges, seeks inside, between and past packed keys, `seek_dup` past a set's last row unset, key tails of a packed row and packed landings without a nested descent (`packed_landings_skip_the_nested_descent`) |
| `tests/dup_txn_cursor.rs` | `DupTxnCursor` against the twin in one transaction: Oracle 2b, `dup_txn_cursors_and_direct_writes_match_the_composite_twin`, several dup cursors on the dup table and as many `TxnCursor`s on the twin in lockstep, with cursor writes and the transaction's own `put`, `insert`, `append`, `delete`, `delete_prefix` (on the twin the adapter's wipe loop, every call compared) and `clear_table` between the moves; each step's results, states and deleted flags, and `follower_shares_prefix` at `K − 1`, `K`, `K + 1`; rounds commit (then `check`) or abort; coverage gates on reseeks, each landing of a cursor's delete, another writer's deletes under a cursor (its row, its follower, emptying its key, `delete_prefix` of it), clears, deletes into the next key and of the table's last row, refused and key-creating cursor writes, and every prefix class both ways; its `_heavy` twin (release). Then one by one: staleness after another writer's put, delete, emptied key, `delete_prefix` and `clear_table` (`a_dup_txn_cursor_finds_its_row_again_after_another_writers_change`), the wipe loop's state (`delete_prefix_under_a_cursor_leaves_the_wipe_loops_state`), a delete of a key's last row and one that empties a key, from the path caches and by key (`a_delete_of_a_keys_last_row_lands_on_the_next_key`, `a_delete_that_empties_a_key_lands_on_the_next_key`), the working map before the cell (`a_cursor_reads_the_map_root_not_the_cell`), a private cell with no map entry (`a_private_cell_with_no_map_entry_is_corrupt_under_a_dup_cursor`), `WrongTxn` and `StaleTable` (`dup_txn_cursor_wrong_txn_and_stale_table`), refused writes and width refusals (`refused_writes_through_a_dup_cursor_change_nothing`), a cursor put after another writer moved the outer path (`a_cursor_put_after_another_writers_change_finds_its_row`); and `damage_under_a_dup_cursor_is_corrupt`: forged nested pages, CRC-stamped again (a nested cell in a nested leaf, an outer key of another width, an empty subkey, a misrouting nested separator), each met by a write cursor's seek, its step and a `delete_current` beside it: `Corrupt`, the cursor unset and out of the deleted state, latched, on a fresh copy each; Oracle 2b runs at the three bounds as Oracle 2a does; a cursor on a packed row sees an in-place rewrite, a promotion and a removal under it (`a_dup_txn_cursor_sees_an_in_place_packed_rewrite`, `a_txn_cursor_follows_its_packed_key_rewritten_promoted_and_emptied`, `another_writers_packed_rewrite_reseeks_the_cursor`), and packed damage under a cursor is `Corrupt` (`packed_damage_under_a_dup_cursor_is_corrupt`) Oracle 2b alternates rounds with the cursors' hints on and off, gated (debug) on levels routed by a hint and hints refused. A dup cursor's delete and insert of its row route both levels by its paths, the outer cell read from the leaf it holds (`a_dup_write_at_the_cursor_routes_both_levels`), and a key whose map root supersedes its cell is written in the map's tree, never the cell's (`a_held_outer_leaf_never_overrides_the_map`). |
| `tests/dup_sweep.rs` | `damage.rs`'s and `read_only.rs`'s sweeps over a dup table (`damage_sweep_on_a_dup_table`, `read_only_sweep_on_a_dup_table`): every kind of damage to its cells, trees and record `Corrupt` at the first use that meets it, latched; the scan stage scans the dup table with a `DupRoCursor` (`first`, `next_in_prefix(K)` inside a key, `next` across keys, `seek_key` past a key it must not read), then the plain table; two outer cells naming one nested root, `Corrupt` at the commit that writes both and found by `check` across two (`a_shared_nested_root_written_once_per_cell_in_one_transaction_is_corrupt`, `a_shared_nested_root_across_transactions_is_found_by_check`); `an_empty_subkey_is_corrupt_at_first_use` The write stage rewrites every fifth row through detached cursors first (`rewrite_through_cursors`: a dup row's seek, `delete_current` and `insert`, a plain row's `seek_exact` and put), so its damage meets writes routed by a cursor's paths; a byte of a nested leaf and of a plain leaf (CRC only) are met there. |
| `packed.rs` tests | the packed-set codec: random sequences of `build_one`, `build_with` and `build_without` against a `BTreeMap`, every set validated and decoded to the model, `find` on present, absent, before-first and past-last subkeys (`packed_sets_built_by_random_edits_match_a_model`, `_heavy` 64 seeds); every single-byte change refused or another valid set, never a panic (`every_single_byte_change_is_refused_or_another_valid_set`); each O(1) rule of `new` and `row`, and a misorder off the probe path passing `find` and failing `validate` (`the_lazy_checks_refuse_what_they_touch`); `PACKED_MIN` and `packed_max` exactly and one past, and the largest set at 64 KiB pages (`packed_codec_limits`) |
| `tests/dup_packed.rs` | packed sets through the engine: `packed_dup_options_resolve` (0, the range, `PACKED_DEFAULT` at 4 and 16 KiB, the same handle twice, the resolved number reported after a reopen); a set exactly at the bound stays packed and one byte more promotes, by create, insert and upsert (`a_set_at_the_bound_stays_packed_and_one_byte_more_promotes`); an upsert that grows a value past the bound promotes (`an_upsert_that_grows_a_value_past_the_bound_promotes`); `a_nested_key_never_demotes`; `a_nested_key_emptied_then_recreated_is_packed_in_one_txn`; a bulk append crossing the bound inside a key larger than a commit (`a_bulk_append_crosses_the_bound_inside_a_key_larger_than_a_commit`); `drop_clear_and_delete_prefix_with_packed_and_nested_keys` (no page freed or leaked by a packed key); `packed_writes_move_the_table_stamp`; `a_free_walk_refuses_packed_totals_that_miss_its_sets` (nothing freed); `check_counts_packed_sets_and_refuses_their_damage`; and `packed_sets_file_size_by_bound`, an ignored measurement of the file per bound against composite |
| `tests/packed_damage.rs` | `damage.rs`'s and `read_only.rs`'s sweeps over a packed dup table (`damage_sweep_on_a_packed_dup_table`, `read_only_sweep_on_a_packed_dup_table`): each kind of damage to a set `Corrupt` at the first use that touches it, under the lazy read rule; damage refusing drop and clear before any free (`packed_damage_refuses_drop_and_clear_before_any_free`) and met by a `DupRoCursor` (`packed_damage_met_by_a_dup_read_cursor_is_corrupt`) |
| `tests/merge.rs` | environment-level merging: deletes that merge leaves, a branch and the root, committed, reopened and read back against the model, with `Env::check` and a twin that does not merge (`deletes_that_merge_commit_and_reopen_to_the_model`); `RwCursor::delete_current` sweeps across leaves, every other key and down to one leaf, `next` returning each follower (`delete_current_sweeps_return_each_follower_while_pages_merge`); a put after a merge that freed or kept the cached leaf descends instead of using the stale cache (`a_put_after_a_merge_never_uses_the_cache_of_a_freed_page`); the crash matrix on a commit whose deletes merge and make the root give way (`every_crash_point_of_a_merging_commit_recovers_the_state_the_protocol_allows`, `_heavy`) |
| `tests/multi_model.rs` | a random multi-table workload with crashes against a model; its large-value variant spreads values over inline and 1–10-page runs, with drops and clears of tables holding runs; a draining variant (stretches of mostly deletes, `a_draining_multi_table_workload_with_crashes_matches_the_model`) makes pages merge, and every variant's coverage gate counts merging deletes; the dup variant (`a_multi_table_workload_with_dup_tables_and_crashes_matches_the_model`, `_heavy`) runs two dup tables, of 4- and 20-byte outer keys, beside the plain ones under the same crashes (puts, inserts, appends, deletes, `delete_prefix`, gets, clears and drops), its gate counting per dup table outer and nested trees of two levels, deletes that emptied a key, `delete_prefix` hits, a clear and a drop |
| `tests/path_cache.rs` | random multi-table workloads (puts, deletes, a detached cursor's `delete_current` walks and its seek, delete and put of one key, clears, drops, commits, aborts, readers held) on a cached and an uncached environment (the test-only `no_path_cache`, which also drops a delete's slot, so a `TxnCursor` there finds its place by key after a delete): the same outcomes, the cursor's after each delete included, and byte-identical files after every commit, with puts and deletes in the cached leaf counted in debug builds (`CACHED_PUTS`, `CACHED_DELETES`); over 3000 sorted keys, a seek, `delete_current` and cursor put of each key make no root descent inside `delete_current` and delete in the cached leaf but for each leaf's first delete and the deletes in a leaf under a quarter, the cursor on each follower (`slot_rewrites_over_sorted_keys_delete_in_the_cached_leaf`, counted in debug builds); a cached leaf reused by another table in the same txn is not written; an aborted transaction's cache never reaches the next Beside them, a cursor's write hint against the search: seeded scripts of two detached cursors' seeks, slot rewrites, puts of any key and delete runs, with the transaction's own puts and deletes, on three-level trees with splits, runs and merges, give the same outcomes, cursor positions and file bytes as under `Writer::no_write_hint` (`hinted_writes_leave_the_bytes_of_unhinted_ones`, heavy twin). |
| `tests/append.rs` | `RwTxn::append`, `RwCursor::put`/`append` and both inserts against `RwTxn::put`: byte-identical files and a model across splits, aborts, deletes and stale path caches; an append out of order, and an insert of a present key (in the cached leaf, on the descent, over an overflow value), is refused without a latch, a change or a moved epoch; a cursor put or insert lands on its entry (`current`, `next`, `prev`) |
| `tests/txn_cursor.rs` | detached cursors sharing one write transaction: three `TxnCursor`s over two tables and the transaction's own puts, deletes and a `clear_table`, interleaved at random, against a model of each cursor (its entry, or the follower and the deleted key after a delete, over the table's rows) in every result and state, with writes next to where the cursors sit (before and after their slot, deleting their entry and its neighbours), overflow values and refused inserts and appends; after every `delete_current` that deleted, the cursor's position (the test-only `TxnCursor::position`: state, path, root, saved key, up to date) equals a new cursor's after a seek of the deleted key; each round commits and checks, a cursor of an earlier round is `WrongTxn`, and the file reopens to the model (`txn_cursors_and_direct_writes_match_a_model`, `_heavy`); another table's writes, a table created and the cursor's own writes cause no reseek, a write to its table one (`a_write_to_another_table_causes_no_reseek`); a dropped table is `StaleTable` for every call, touched in the transaction or not, and after a re-create (`drop_table_under_a_live_cursor_is_stale_table`); a later transaction, the successor of an aborted one with its txn id, and another environment's are `WrongTxn`, changing nothing, without a latch (`a_cursor_used_with_another_transaction_is_wrong_txn`); a refused insert changes nothing and keeps the cursor and its saved key, and after a delete a reseek finds the follower by its key (`a_refused_insert_through_a_txn_cursor_changes_nothing`); after `last` (or `prev` from unset) `next` and `next_in_prefix` move to an entry another writer put past it, and with none go past the end (`next_after_last_finds_entries_put_past_it`); prefix steps after another cursor's insert and delete and through a group split under the cursor (`prefix_steps_through_txn_cursors_follow_other_writes`); a `delete_current` that copies the published root places the cursor in the new tree, its `position` equal to a new cursor's seek before any step (`a_txn_cursor_delete_that_copies_the_root_places_the_cursor_in_the_new_tree`); `follower_shares_prefix` against the model after every delete at six prefix lengths (a follower in the next leaf and a delete of the table's last entry counted among them) and as a random call in any position, stale included (in `txn_cursors_and_direct_writes_match_a_model`), and in each state by hand: before a delete, after a move, past the key's length, a follower in the next leaf, after the end, after another writer's delete of the follower (`follower_shares_prefix_answers_for_the_deleted_entrys_follower`) Every other round of the model test writes with the cursors' hints off (`Writer::no_write_hint`), the others gated (debug) on levels routed by a hint and hints refused. A write at the cursor of a three-level tree routes every level by its path with no search, then places as a fresh seek (`a_write_at_the_cursor_routes_by_its_path`); a stale cursor, one after a `seek_exact` miss and one after the end pass no hint (`a_stale_or_unplaced_cursor_passes_no_hint`). |
| `tests/key_tail.rs` | `current_with_key_tail` on `Cursor`, `RoCursor` and `TxnCursor` against `current`: inline, empty and overflow values, over walks, seeks, prefix steps, a reseek and a delete; `seek_with_key_tail` on `RoCursor` and `TxnCursor` against `seek` then `current_with_key_tail` (hits, misses, both ends, an empty table, and the moves after: `seek_with_key_tail_is_a_seek_then_current_with_key_tail`), and a `TxnCursor` taken out of the end that `last` (or `prev` from unset) sets, and out of the deleted state, as `seek` takes it (`seek_with_key_tail_clears_a_txn_cursors_end_and_deleted_states`); the page codec's span is tested in `page.rs` (`key_tail_joins_the_keys_last_bytes_to_an_inline_value`, and on damaged pages) |
| `tests/concurrency.rs` | reader threads rescan pinned snapshots and take fresh ones while a writer replaces, deletes and clears (pages freed and reused): no read sees damage or a torn state |
| `tests/read_only.rs` | `Env::open_read_only` on a file of several commits (inline values, runs of 1–18 pages, a dropped table): every key reads back through gets and both scans, `check` passes, and the file is byte-identical with its mtime unchanged after the open, the reads, the check, a sync and the drop; a mode-0444 file opens (`a_read_only_open_reads_every_table_and_changes_nothing`); an unsynced meta in the page cache is read and still lost to a power loss: the open synced nothing (`a_read_only_open_syncs_nothing`); read-only opens coexist and exclude a writer both ways (`read_only_opens_share_the_lock_and_exclude_a_writer`); `begin_rw` and `try_begin_rw` are `ReadOnly` without a latch; a missing, empty or foreign file is refused with nothing created; a damaged newest meta falls back one txn, unrepaired; a read-only damage sweep (`damage_at_rest_is_met_by_read_only_reads_and_check`, on `tests/damage.rs`'s fixture): damage to a leaf, a branch, the catalog or the free list is `Corrupt` from the first get or scan that reaches it, else from `check`, latched by that operation, and a damaged tree root fails the open closed, the file unchanged |

```text
cargo nextest run -p tndb-b                                    # default suite (debug)
cargo test --release -p tndb-b -- --ignored --skip tndb_b_sync_and_merge  # heavy variants (about 4–5 min)
cargo test --release -p tndb-b tndb_b_sync_and_merge -- --ignored --nocapture --test-threads 1  # benchmark
```

### Benchmark: step 4 and merging

`bench.rs` (`tndb_b_sync_and_merge`) times two engine choices against their test-only switches,
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
cargo test --release -p tndb-b tndb_b_sync_and_merge -- --ignored --nocapture --test-threads 1
```

`TNDB_B_BENCH_N` sets N (default 1,000,000), `TNDB_B_BENCH_ENGINES` the columns (`t4d` and `t16d`,
`Durable` with 4 and 16 KiB pages; `t4n`, 4 KiB `NoSync`, which has no step 4 and runs only the
first mode), and `TNDB_B_BENCH_ROWS` the groups (`small`, `churn`). Put the temp dir on a real disk
(`TMPDIR`, scoped to the one command): on a tmpfs syncs cost nothing, and on Linux the run stops.

**Merging** (Apple M1 Max, macOS, 2026-10-21, N = 300k, medians of three runs, at 4 KiB pages; code
before the owned and detached cursors): after the churn the table took 2.3 MiB of pages, against
13.1 MiB without merging, and the churn took 729.2 ms against 742.5 ms. The delete rows of the
same runs stayed within a few percent of merging off, and the other rows were unchanged.

Comparison benchmarks live in the reth adapter crate:
[`tndb-b-reth/engine-bench.md`](../tndb-b-reth/engine-bench.md) runs this engine through its own
API, and the adapter's [README](../tndb-b-reth/README.md) runs reth's database benches.

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
in [`engine-bench.md`](../tndb-b-reth/engine-bench.md).

**After the change**, on the Mac (medians of three targeted runs, 2026-10-20, before M2), the
small-commit median is 73.2 us at 4K and 90.2 us at 16K (it was 114 and 130 us; the "fdatasync"
rows read 72 and 81 us in the same runs), and the bulk commits, the mixed row and `clear_table`
are level (sequential commit 34.5 vs 34.9 ms, random 85.5 vs 87.9 ms at 4K). On Linux the small
commits are not yet measured under the new rule (see
[`engine-bench.md`](../tndb-b-reth/engine-bench.md#linux-run)).

**Open:** at 16K on the Mac, a one-range small commit synced by `msync` is still slower than by
`fdatasync` (median 89–90 vs 81–82 us, p99 158–165 vs 96–97 us). At 4K the two match.
