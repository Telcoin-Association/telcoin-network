# `tndb-b`: a copy-on-write B+tree key-value engine

An embedded key-value engine in the LMDB/MDBX mould. One page file holds every table as a
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
| `txn.rs` | `RoTxn`, `RwTxn`, `ReadTxn`, `Table`, `TableStat`, `RoCursor` (the owned read cursor), `RwCursor`, `TxnCursor` (the detached write cursor, with the transaction's write stamps, schema counter and serial); commit steps 1–3 and 7 |
| `meta.rs` | the 128-byte meta record, `choose_meta`, `slot_to_write` |
| `page.rs` | the slotted page codec, `Geometry` (limits, `place`: inline or overflow), `validate` |
| `overflow.rs` | overflow runs: the 12-byte cell reference (`Ovf`), the run codec, `checked_run` (every check a run read makes), `MAX_VALUE` |
| `tree/` | `TreeRoot`, the `PageRead`/`PageWrite` stores, checked descent and lookup (`read.rs`), copy-on-write put/delete/split/merge/free (`write.rs`), the bidirectional cursor (`cursor.rs`) |
| `alloc.rs` | the writer's page allocator (`private`, `stamped`, `pool`, `freed`, `next_pgno`) and its overflow runs (`allocate_run`, `free_run`) |
| `freelist.rs` | free-record codec, gated loading, the fixed-point save |
| `catalog.rs` | the table catalog (`TableRecord`) |
| `check.rs` | `Env::check`: the full structural check (every overflow run's CRC included) and exact page accounting |
| `latch.rs` | the failure latch |
| `file.rs` | `PageFile` (reserved mapping, preallocated growth, msync, poison), `ReadOnlyFile` (a read-only open's `PROT_READ` mapping) and `MapView` (the lock-free reader view of either) |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `os_page_size`, `flock_exclusive`, `flock_shared` |
| `crc.rs`, `page_set.rs` | copied from `crates/storage/src/archive/` (`page_set.rs` adds a touched-word list, so `clear` and `drain` cost the words touched, not the file) |
| `disk_model.rs` | test-only page-cache and power-loss model |
| `tests/`, `*/tests.rs` | crash, damage, multi-table model, merging, path cache, append, detached cursor, key tail, concurrency and read-only suites, per-module tests |
| `bench.rs` | the on-demand benchmark against MDBX |

## Milestones

- **M1, the crash-safe core:** copy-on-write B+trees in one page file, two metas, gated page
  reuse, the persisted free list, the failure latch, `Env::check`, and the crash and damage
  suites. Its performance work added provenance trust, the path cache, `append` and cursor puts;
  step 4 then became one sync (an `msync` of one range, else one `fdatasync`).
- **M2, what reth's tables need:** format v2 with page counters in every `TreeRoot`; overflow runs
  for values up to 16 MiB, with run allocation (first fit, gated records loaded for a fit, tail
  reuse) and a record's leftover pages listed again under the consumed keys; merging on delete;
  a delete in one descent; the read-only open; owned (`RoCursor`) and detached (`TxnCursor`)
  cursors; `insert` (`KeyExists`); prefix steps; MDBX's cursor state after a delete (on the
  follower); `KeyTail`; and the reth adapter, [`tndb-b-reth`](../tndb-b-reth/README.md).
- **Next:** the Linux gate run (the adapter's benches, each at most 1.10× MDBX with at most one
  explained row up to 1.25×, plus a fresh run of this crate's: the Linux tables below predate M2);
  TN integration behind a feature; and the open performance items: the descent of a small random
  read, dup-table reads, the commit of overflow runs, the one-range `msync` at 16 KiB on macOS,
  and 1–4 KB values taking a run page at 4 KiB pages (see [Benchmark](#benchmark-tndb-b-vs-mdbx)
  and the adapter's README).

---

## Contents

1. [Concepts at a glance](#concepts-at-a-glance)
2. [On-disk format](#on-disk-format)
3. [Deletes and merging](#deletes-and-merging)
4. [Commit protocol](#commit-protocol)
5. [Free list and page reuse](#free-list-and-page-reuse)
6. [Readers and snapshots](#readers-and-snapshots)
7. [Open and recovery](#open-and-recovery)
8. [Failure latch](#failure-latch)
9. [Concurrency rules](#concurrency-rules)
10. [Public API reference](#public-api-reference)
11. [Errors](#errors)
12. [Platform notes](#platform-notes)
13. [Limits and non-goals](#limits-and-non-goals)
14. [Intentional design decisions: please do not flag these](#intentional-design-decisions-please-do-not-flag-these)
15. [Tests and benchmarks](#tests-and-benchmarks)

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

**Out of scope, by design:** dup-sort tables (the reth adapter stores their rows as ordinary keys
`key ‖ subkey`) and multi-process access alongside a writer. See
[Limits and non-goals](#limits-and-non-goals).

---

## On-disk format

All integers are little-endian unless marked BE.

### Meta pages (pages 0 and 1)

Each meta page holds a 128-byte record at offset 0, and the rest of the page is zero (`meta.rs`):

| off | size | field |
|---|---|---|
| 0 | 8 | magic `b"tndb-b\0\0"` |
| 8 | 4 | format version = 2 |
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
2. The version must be 2, else `UnsupportedVersion`. This runs before the CRC check, because
   another version may lay the record out differently. The format is pre-release: a version-1
   file is refused, with no migration.
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
- **The free gap is zero only as built.** The writer zero-fills each page it allocates
  (`PageWrite::alloc`), but a delete or a shrinking replace leaves bytes in the gap: the moved
  cells' old bytes and the slot array's old last slot (`remove_cell`, `shift_front`). The CRC
  covers them as it covers every byte of the page, and nothing ever reads them.
- **Leaf cell:** `vlen u32 | klen u16 | flags u16 | key | value`. Flags 0 holds the value
  inline; flags 1 makes the cell an overflow cell (below). Any other flag is `Corrupt`.
- **Branch cell:** `child u32 | child_txn u32 | klen u16 | key`. There is one cell per child, in
  LMDB style. Cell i's key is the lower bound of child i, and cell 0's key is empty and stands for
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
The catalog and the free tree hold inline values only (`Geometry::check_entry`); an overflow cell
in either is `Corrupt`.

| P | U | MAX_ENTRY | max key | inline value (8-byte key) | inline value (32-byte key) | overflow value (key up to) | a 1-page run holds | a 16 MiB value takes |
|---|---|---|---|---|---|---|---|---|
| 4 KiB | 4068 | 1017 | 1005 | 999 | 975 | 16 MiB (995 B) | 4068 B | 4097 pages |
| 16 KiB | 16356 | 4089 | 4077 | 4071 | 4047 | 16 MiB (4067 B) | 16356 B | 1025 pages |
| 64 KiB | 65508 | 16377 | 16365 | 16359 | 16335 | 16 MiB (16355 B) | 65508 B | 257 pages |

**One split always suffices.** An overflowing page holds at most `U + MAX_ENTRY` bytes. Cutting
it at the first entry where the left part reaches half leaves both parts within U. An overflow
cell is an ordinary inline cell of `8 + klen + 12` bytes, so this holds with runs too.

Larger entries are refused up front with `KeyTooLarge` or `ValueTooLarge`, before anything
changes. `Env::max_key_len` and `Env::max_value_len` report the limits; `max_value_len` reports
the table limit (16 MiB for a key that qualifies).

### Catalog

The catalog is a B+tree keyed by table name: UTF-8, 1..=255 bytes. Each value is a 56-byte
`TableRecord`:

`id u32 | flags u32 (0) | root TreeRoot (32) | created_txn u64 | reserved 8`.

Table ids are unique over the file's life, so a dropped and re-created table gets a new id.

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
allocator's epoch, so the path cache, as it was. A hit then verifies the published pages on the
path (`verify_path`) before it reads them further, as a put's descent verifies them as it goes.

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
quarter beside a sibling it would fit into: it is **merge-closed**.

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
moves the allocator's `epoch` (do-not-flag item 12). `RwCursor::delete_current` finds its place
again by key, so after merges it still lands on the deleted entry's follower.

**Fill.** In `deleting_90_percent_leaves_no_page_merge_open` (4 KiB pages), the pages are about
0.69 full after a random fill, and the leaves 0.40–0.47 full after 90% of the keys are deleted at
random. Without merging, the same deletes leave the leaves about 0.08 full, in about six times the
pages.

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
| 3 | Stamp the CRC of every page the transaction created (they become "stamped by this writer"). An overflow run gets one CRC over all its pages, never one per page (`stamp_private`, `PrivatePages::units`), and its pages never count as stamped. |
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
its txn identity, `Corrupt`. A page freed twice in one transaction (two pointers naming it, such
as two cells naming one run) is `Corrupt` at commit (`Allocator::reusable`). `Env::check` reports
the page as used twice.

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
step 2. LMDB does the same (`mdb_freelist_save` writes the reclaimed pages under keys up to the
last record it consumed), and so does MDBX (`gc_update` stores them under reclaimed ids, or ids
below its oldest record). Listing them under M instead made them wait two more commits, longer
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
it (a snapshot's pages do not change while it lives). Every other move, and every leaf it
enters, runs the same checks as `Cursor`'s.

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
8. **Map** inside a reservation of `max(map_reserve, file length)`.
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
file already open, not to one opened later (the "fsyncgate" pattern; LMDB shares it). A reopen
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

1. **Validate the page size**, as `Env::open` does. No other field of `EnvOptions` applies.
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
   no reservation and no growth. Readers reach it through the same `MapView`.
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
| `EnvOptions { page_size, map_reserve, sync }` | defaults: 4096, 1 GiB, `SyncMode::Durable`. `page_size` applies at create; an existing file keeps its own. |
| `SyncMode::{Durable, NoSync}` | durable at commit, or at the next `Env::sync` or drop |
| `Env::begin_ro() / begin_rw() / try_begin_rw()` | start transactions: an `RoTxn` (one committed state of every table, kept intact while it lives), an `RwTxn` (`ReadOnly` in a read-only environment) |
| `Env::sync()` | make NoSync commits durable (a no-op when nothing is pending, and in a read-only environment) |
| `Env::check() -> CheckReport` | full structural check plus exact page accounting, each root's entry count and page counters compared with its tree. `CheckReport` gives the state's txn id and page count, the catalog, table, free-tree and free pages, the free records, and a `TableCheck { entries, pages, height }` per table. |
| `Env::stat() -> EnvStat` | `page_size`, `txn_id`, `page_count`, `tables` |
| `Env::free_pages() -> u64` | the pages the newest committed state's free list holds (MDBX's `freelist`, for reth's `db.freelist` gauge): every free record read with the allocator's checks, so damage is `Corrupt` and latches |
| `Env::failure()` | the latched cause, if any |
| `Env::max_key_len() / max_value_len(klen)` | the entry limits of this page size. `max_value_len` is the table limit: 16 MiB for a key up to `max_key_len − 10` bytes, else the inline limit. A value over the inline limit is stored in an overflow run transparently: `put`, `get` and the cursors take and return its bytes as for any value. |
| `ReadTxn` (both txn kinds) | `open_table(name) -> Option<Table>`, `get`, `cursor`, `txn_id`, `table_stat -> TableStat { entries, height, branch_pages, leaf_pages, overflow_pages }` (O(1), from the root record) |
| `RwTxn::create_table(name)` | open or create |
| `RwTxn::drop_table(Table) / clear_table(&Table)` | free the tree's pages (both are copy-on-write: readers keep the old tree) |
| `RwTxn::put / delete -> bool` | point writes |
| `RwTxn::insert(table, key, value)` | put only a key the table does not hold: a present key is `KeyExists` (like `MDBX_NOOVERWRITE`), found by the put's own search (in the table's cached leaf, else by the descent's plan) and refused before anything changes, with no latch; a new key is stored exactly as `put` stores it |
| `RwTxn::append(table, key, value)` | put a new last entry: `key` must sort strictly after the table's last key (an empty table takes any key), else `AppendOutOfOrder`, refused before anything changes (like `MDBX_APPEND`). An append into the table's cached rightmost leaf neither descends nor searches; the cache holds the leaf of the table's last put or append in this transaction (after an append split, the new right leaf) until a page is allocated or freed, and any other append descends. |
| `RwTxn::cursor_mut -> RwCursor` | cursor moves and prefix steps plus `put`, `append` (as `RwTxn::append`), `insert` (as `RwTxn::insert`) and `delete_current`. A put, append or insert leaves the cursor at its entry: placed from the path it wrote (after an append split, from a second descent that refills the table's path cache), or re-sought by key after any other split; a refused one leaves the cursor where it was. A delete re-seeks by key, so after any merges the cursor is on the deleted entry's follower in a deleted state, as MDBX's cursor is: `next` returns the follower without stepping past it, `current` returns it too, `prev` returns the entry before it, and another `delete_current` deletes it. Right after a delete the prefix steps compare with the deleted key: the follower (`next_in_prefix`) or the entry before (`prev_in_prefix`) if it shares the prefix, else `None` with the cursor still on the follower in the deleted state. With no follower the cursor is after the end: `next`, `current` and `delete_current` find nothing and `prev` returns the new last entry (MDBX's `GET_CURRENT` returns that entry, as after a `seek` past the end). |
| `RwTxn::cursor_detached(&self, &Table) -> TxnCursor` | a write cursor that holds no borrow of the transaction: its moves (`first`, `last`, `next`, `prev`, `current`, `seek`, `seek_exact`, the prefix steps) take `&RwTxn` and return entries borrowed from it; its writes (`put`, `insert`, `append`, `delete_current`) take `&mut RwTxn`. The states, the writes' placement and the deleted state are `RwCursor`'s. As MDBX's cursor after `last`, a `TxnCursor` whose `last` (or `prev` from unset) landed on an entry ends the table: `next` and `next_in_prefix` return `None` and stay on the entry, even after another writer appends past it, until another move or write. Several cursors, over one table or several, and the transaction's own writes interleave, each seeing the others' writes; see [Detached cursors](#detached-cursors-txncursor) below. |
| `RwTxn::commit() -> txn id / abort()` | finish |
| `Cursor` | `first`, `last`, `next`, `prev`, `current`, `seek` (first `>= key`), `seek_exact`, and the prefix steps `next_in_prefix(n)`, `prev_in_prefix(n)` (below) |
| `RoTxn::cursor_owned(self: &Arc<RoTxn>, &Table) -> RoCursor` | a read cursor that owns a share of its transaction: no lifetime, `Send + Sync`, the same moves, states and prefix steps as `Cursor`; the entries it returns, overflow values included, borrow the cursor until its next call. Each step fetches its leaf again by page number (see [Readers and snapshots](#readers-and-snapshots)), so a scan costs about 4–8% more per entry than `Cursor`'s (macOS). |
| `current_with_key_tail(t) -> (key, KeyTail)` on `Cursor`, `RoCursor` and `TxnCursor` | the current entry with its value joined to its key's last `t` bytes (`t` clamped to the key's length): `KeyTail::Joined` for an inline value, the key's tail then the value as one slice of the page (a leaf cell holds its value right after its key), so with no copy; `KeyTail::Apart` for a value in an overflow run, the value alone. The reth adapter reads a dup row (subkey, then the rest) this way. |
| `seek_with_key_tail(key, t) -> (key, KeyTail)` on `RoCursor` and `TxnCursor` | `seek(key)` then `current_with_key_tail(t)` from one leaf access: the entry is read through the leaf the descent fetched and parsed, an overflow value once; `None` (after the end) when no key is `>= key`. The cursor is left exactly as `seek` leaves it (a `TxnCursor` saves the landed key for its reseek). The reth adapter's `seek_by_key_subkey` reads its row this way. |
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
and `TxnCursor`) move to the adjacent entry only if its key shares the first `n` bytes of the current
entry's key, and return it; otherwise they return `None` and the cursor stays where it was. For
keys built as `key ‖ subkey` they walk one key's subkeys (the reth adapter's `next_dup` and
`prev_dup`).

- Not at an entry (Unset, AfterEnd, BeforeStart): `None`.
- `n` longer than the current key: `None` (no other key has that prefix).
- `n = 0`: `next` (`prev`), except that at the last (first) entry it returns `None` and stays.
- At a leaf's end the step peeks into the adjacent leaf with a copy of the cursor and keeps its
  position unless the entry there shares the prefix.

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
  changed. If not, the cursor is left as if it had deleted the entry itself, which is what an MDBX
  cursor does when another cursor deletes its entry: on the first entry after the key (the
  follower) in the deleted state, so `next` and `current` return the follower, `prev` the entry
  before it, `delete_current` deletes it, and the prefix steps compare with the gone key. Its
  saved key becomes the follower's (the gone key is kept apart, `deleted_key`, for the prefix
  steps). A cursor already in that state stays in it, on its follower, or on the first entry
  after the follower if that is gone too. An unset cursor, or one after the end or before the
  start, keeps its state over the new tree. A reseek that finds the entry keeps the end `last`
  sets (`soft_end`); one that does not clears it.

The reseek is lazy: it sees the table as it is at the cursor's next call. If, before that call,
another writer deleted the cursor's entry and put it back, or put an entry between it and its
follower, the cursor is on that entry, where MDBX's cursor, moved at the delete, is on the
follower. The other place the cursors differ from MDBX's is past either end: MDBX's cursor stays
on the end entry after stepping past it, after a `seek` past the end or after deleting the last
entry (`current` returns it, `delete_current` deletes it), where tndb-b's is after the end or
before the start (`current` finds nothing, `delete_current` returns `false`).

A write to another table (each table has its own stamp) and the cursor's own writes (it records
the stamp its write left) cause no reseek. A `put`, `insert` or `append` through the cursor checks
the serial and the table, not the stamp: it places the cursor anew at the entry it wrote, from the
path the table's path cache holds (or by key after a split), as `RwCursor` does; a refused one
changes nothing, the cursor included, so a stale cursor is brought up to date at its next call.
`delete_current` brings the cursor up to date first, then deletes the entry it is at.
`WrongTxn` and `StaleTable` change nothing and do not latch.

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
| `KeyTooLarge { len, max }` / `ValueTooLarge { len, max }` | an entry over the page size's limits (no latch) |
| `AppendOutOfOrder` | an append whose key does not sort strictly after the table's last key (no latch) |
| `KeyExists` | an insert (`RwTxn::insert`, `RwCursor::insert`, `TxnCursor::insert`) of a key the table holds (no latch) |
| `TableNotFound(String)` / `StaleTable(String)` / `InvalidTableName` | table errors (no latch) |
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
  loses nothing committed durably, but a power loss can, exactly as with MDBX's `Durable` mode on
  macOS. Production durability is a Linux property, where `msync(MS_SYNC)` and `fdatasync` each
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
  pages alike. In the `big` benchmark rows, run in one shared environment, a `t4d` put of an
  8 KiB value took 9.5 us, against 2.0 us in a fresh environment; most of it was the run's
  first-touch faults. MDBX grows the file sparse on macOS, so it does not pay this. tndb-b keeps
  growing with real blocks by design: a full disk must be an `io::Error`, not a `SIGBUS`. The
  cause is specific to macOS; Linux is not measured.

---

## Limits and non-goals

The entry limits are in [Limits](#limits): a table takes values up to 16 MiB, in overflow runs.
These limits and non-goals are known:

- **No dup-sort tables, by design.** The reth adapter ([`tndb-b-reth`](../tndb-b-reth/README.md))
  stores a dup table's rows as ordinary keys `key ‖ subkey`. Leaf cell flags other than 0 and 1
  (overflow) are refused.
- **The inline limit is about a quarter of a page** (`MAX_ENTRY = U / 4`, kept for its one-split
  proof): about 1 KB at 4 KiB pages, against MDBX's about 2 KB. A value of 1–4 KB at 4 KiB pages
  therefore takes a run page of its own. Raising it needs a new split proof and a format change.
- **No multi-process access** alongside a live writer: there is no shared reader table. A
  read-only open takes the lock shared, so it and a writer exclude each other.
- Merging is local and merge-only. A delete merges only the pages it changes, siblings never
  borrow entries, and a page stays under a quarter full when no neighbour fits into it. Pages no
  delete changes are not revisited, so there is no fill bound for the whole tree: only the pages
  a delete changes are merge-closed.
- No sibling links. A cursor crosses leaves through their parents (copy-on-write cannot keep
  sibling links).
- A `TxnCursor` reseeks lazily, and the engine's cursors end past either end where MDBX's stay on
  the end entry: the two places its cursors still differ from MDBX's (see
  [Detached cursors](#detached-cursors-txncursor)).
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
   - **A quarter is LMDB's and MDBX's default merge threshold.** It is enough in practice: after
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
    exactly the bounds `route` gave on the way down.
12. **A table put can skip the descent** (`write::PathCache`, `write::put_cached`). Each table's
    working record caches the leaf its last put reached on a trusted path, the path above it, and
    the leaf's key range `[lo, hi)` as owned copies. A put that did not split fills it from its
    own descent; an append split refills it by a second, trusted descent of the new path; any
    other split leaves it empty. While the table's root is
    trusted and the allocator's `epoch` is unchanged, a put whose key lies in the range and whose
    entry fits goes straight into that leaf (through `page_mut`, so the private and poison checks
    stay, plus the identity check), with the descent's exact change and bookkeeping. One
    writer-wide epoch is enough because every change to a tree's shape or separators allocates
    or frees a page: a copy allocates and frees, a split allocates, an unlink, merge, collapse,
    clear or drop frees; and the epoch also moves at commit (`stamp_private`), `begin_txn` and
    abort. An in-place insert, replace or delete (one that does not merge) changes only its leaf,
    never a separator, so it leaves the cache valid. A freed page is reusable at once by another
    table in the same txn, which is exactly the move the epoch catches. The range is the nearest separator either side on the
    path, which equals the intersection of all of them on any path the descent accepts (debug
    builds assert it). The catalog and the free list never use a cache. A put whose value goes to
    an overflow run, and a replace of a value held in one, always descend: the run is allocated,
    or the old one freed, with the tree's other pages.
13. **Every read of an overflow value held in a run of more than one page verifies the run's full
    CRC** (`RunCrc::MultiPage`), so a get of such a value reads all its bytes. Pages 2..n of a run
    carry no header: a middle page reused or redirected by damage passes every header check of
    the first page, and only the CRC catches it. The rule is never wrong data, so the CRC stays.
    It costs a pass over the value. In the `big` benchmark rows (macOS, medians of three, each
    value size in a fresh environment) a get takes about 1.1 us at 8 KiB and 2.1 us at 32 KiB at
    4 KiB pages (`t4d`, runs of 3 and 9 pages), and 1.4 and 2.6 us at 16 KiB pages (`t16d`),
    against MDBX's about 0.3 us, which returns a pointer without reading the value. With the CRC
    skipped in an experiment, the same `t4d` and `t16d` gets took 0.34–0.40 us. A 1-page run
    has no page without a header, so its read skips the CRC as a leaf's does (item 3): that took
    a `t4d` 2 KiB get from 724 to 302 ns and a `t16d` 8 KiB get from 1.30 us to 271 ns.
14. **An owned cursor's step re-fetches its leaf without the per-fetch checks**
    (`Snapshot::page_again`, used by `RoCursor::next`, `prev` and `current_with_key_tail`). The
    page number is the one the
    cursor's descent entered with every check, and a snapshot's published pages never change while
    an `Arc` of it lives (the reuse gate holds them), so the checks would see the same bytes and
    pass again. `Cursor` relies on the same fact when it keeps the page slice itself. Leaves the
    cursor enters, and every other move, run the full checks. A safe slice re-derived from the
    mapping replaces a cached raw pointer, so the owned cursor adds no `unsafe`.
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

---

## Tests and benchmarks

The default tests are small and fast, because CI runs nextest in debug. The heavy variants are
`#[ignore]`d and run with `--release`.

| suite | covers |
|---|---|
| `page.rs` tests | leaf and branch codecs against proptest models; the parsed views (`View`, `LeafView`, `BranchView`) against an independent model; splits at all five page sizes; `SplitBuf` splits byte-identical to a reference split (leaves and branches, the pending cell anywhere, a quarter of the cases at the append place); append fill; exact limits; a full page left byte-identical; 13 damage kinds; never panicking on fuzzed bytes; overflow cells are canonical references (flags 1 with a 12-byte value, a length past the inline limit and within 16 MiB), and the views model and damaged-page generator include them; `Geometry::place` keeps every value that fits inline and sends larger ones to runs of the right length (`place_keeps_m1_values_inline_and_sends_larger_ones_to_runs`) |
| `overflow.rs` | the run codec: a run reads back its value; each kind of damage to a run is `Corrupt` under every `RunCrc` (each header field re-stamped, a zero trailer, a flipped middle page, a range past the end or over a meta page, a page-number overflow, an unmapped run, a txn after the snapshot or not the cell's, a length that disagrees with the run or is over 16 MiB), private runs too; the test store reads runs against its snapshot |
| `tree/write.rs`, `tree/read.rs` | copy-on-write put and delete against a `BTreeMap` model over random batches (with a coverage gate on split and delete shapes), which also put large values (inline, 1-page and 2–10-page runs) and check the overflow counter and the run pages; a failed allocation leaves the tree identical, also when the put takes a run; a put into a run or a replace of one takes the descent, not the path cache; a published leaf naming a private run, and a copied leaf naming a run this txn allocates later, are refused; a delete refuses a handle miscounting its pages; split reservation for long separators; append fill; collapse and unlink; `clear` and the free walk refuse damaged or stale pointers, and a page private to the transaction for another tree (its leaf or a run's page) named by a published cell, an untrusted root handle or a forged cell a private copy kept (`the_free_walk_refuses_a_page_private_to_another_tree`); fetch checks, stale copies, redirected children and misrouted pointers are `Corrupt`; copy-on-write's one copy gives the two-step copy's bytes, on `MemStore` and the environment's writer (`copy_on_write_is_the_two_step_copy`), reads a page published before a remap from the new mapping (`copy_on_write_reads_a_page_published_before_a_remap`), and fails exactly as `PageRead::page` fails, a page past the published bytes included (`copy_on_write_fails_as_the_page_fetch_fails`); provenance trust: a trusted path skips the range checks, a private page named by a page this writer did not write (a stamped forged cell, an untrusted root handle, a collapse) is refused, as is a copied cell naming a page this txn allocates later, while a child whose txn wrapped stays live |
| `tree/write.rs` merging | the random batches with merging on, against the same model (`random_batches_with_merging_match_a_btreemap_model`): after every delete the pages it changed are merge-closed, and the coverage gate adds leaves merged from the left and from the right, branch merges, a cascade over two levels or more, a root giving way and a refused merge; the fill bound (`deleting_90_percent_leaves_no_page_merge_open`: after 90% of a random fill is deleted at random, no page under a quarter fits with a neighbour, and the leaves stay 0.40–0.47 full against about 0.08 without merging); a merge refuses a damaged sibling from either side before anything changes (`a_merge_refuses_a_damaged_leaf_sibling`, `a_merge_refuses_a_damaged_branch_sibling`: a flipped byte under a stale CRC, a stale txn in the parent's cell, an older retired copy, a redirect outside the separators, a private sibling, a cell or overflow cell naming this txn, and for branches a key out of order across the seam); a refused neighbour gets only the cheap checks, and damage only its CRC finds waits for its first use (`a_refused_neighbour_gets_only_the_cheap_checks`); a failed allocation during a merging delete leaves the tree identical (`a_failed_allocation_during_a_merging_delete_leaves_the_tree_identical`); an absorbed branch sibling's child-0 chain is descended to the leaves, so a single-child branch outside the cell's separators, a branch of another level and one branch named by two cells are refused (`a_merge_refuses_a_single_child_branch_outside_the_cell`, `a_merge_refuses_a_branch_sibling_of_another_level`, `a_merge_refuses_a_branch_named_by_two_cells`). `a_collapse_checks_the_page_it_promotes` and `a_delete_refuses_a_handle_miscounting_its_pages` turn merging off (`no_merge`) to reach their shapes |
| `tree/cursor/tests.rs` | random moves and prefix steps against a sorted model on the detached cursor and the public `Cursor`, over many tree shapes (grouped keys among them) and page sizes, committed and uncommitted; the ends of empty and single-entry trees; leaf crossings; seeks; prefix steps from every entry of a grouped tree, across leaf boundaries both ways (`prefix_steps_from_every_entry_match_a_model`); every key and every gap of many tree shapes through `seek`, `seek_exact` and `seek_with_key_tail`, each followed by a few steps, against the model on both cursors (`seeks_of_every_key_and_gap_match_a_model`, `_heavy`); `seek_with_key_tail` meets damage (a misrouted cell, an emptied leaf, a root of the wrong height, a flipped run byte) with the error a `seek` then `current_with_key_tail` meets (`a_seek_with_key_tail_meets_damage_as_a_seek_does`); two damages on one page met in the descent's order, the error the first check's exact message: the kind check before the header parse and before the range check (`the_descent_names_the_first_check_a_page_fails`); the path stays 6 bytes a level (`tree_cursor_stays_small`); damage under a cursor latches; `RwCursor` writes and prefix steps against a model (`current` and a second delete right after `delete_current` act on the follower), and prefix steps after `delete_current`; the owned `RoCursor` against the model and `Cursor` on one snapshot with overflow values (the random moves include `seek_with_key_tail`), outliving its transaction handle on another thread while commits free its pages, and latching damage |
| `meta.rs`, `catalog.rs`, `latch.rs`, `alloc.rs`, `file.rs`, `sys.rs`, `crc.rs`, `page_set.rs`, `disk_model.rs` | codecs, slot choice, the latch's first-cause rule (`ReadOnly` does not latch), allocator abort, overflow runs in the allocator (the first fit in the pool, else the file end; one CRC per run, its pages never trusted as stamped; a freed run comes back as single pages), growth and remap, a read-only mapping of a mode-0444 file that maps nothing once dropped (`a_read_only_file_maps_its_bytes_until_dropped`), sync wrappers, the exclusive and shared locks (`flock_shared_coexists_and_excludes_an_exclusive_lock`), the power-loss model |
| `env/tests.rs` | open steps (create at every page size, leftovers, the lock, a damaged newest meta, damaged roots, short and foreign files, the open fsync), commit and NoSync durability under power loss, a commit of no change, abort, the writer hand-off, readers across commits, step 4's sync choice (`data_sync`: one range or one `fdatasync`), a scattered commit and a two-range commit under power loss, the latch (failed msync, damage, oversized entries do not latch), table handles, a multi-page value read back after a commit and a reopen, `max_value_len` at every page size; the per-snapshot table roots: one catalog resolve per handle per snapshot, whichever read uses it (`a_snapshot_resolves_each_handle_once`), a dropped table's handle stale only in later snapshots (`a_dropped_tables_handle_is_stale_only_in_later_snapshots`), a hit needing the handle's name as well as its id, for another file's handle with the same id and for an aborted create's handle, whose id the next table created takes (`a_remembered_root_needs_the_handles_name_too`), and 40 tables, past the 32 slots, each resolving to its own table (`more_tables_than_a_snapshot_remembers_still_resolve`) |
| `freelist/tests.rs` | the record codec; reuse after two durable commits or two NoSync syncs, held back by a live reader; damaged records never handed out; abort returns the records taken; dropped and cleared tables' pages reused; steady churn keeps the file bounded; the save settles within three rounds, including a record count that shrinks mid-save; `check` reports damage, leaks and double uses, and compares each root's page counters, and refuses a root branch with one child (`check_refuses_a_root_branch_with_one_child`); exact accounting in a random workload. Overflow runs at the environment level: the runs of cleared and dropped tables are freed and reused; a damaged run (header, leaf, counter) refuses `clear_table` and `drop_table` and changes nothing, the file included; `check` reports a damaged run (a CRC flip, a header or pad byte, a wrong counter) and a run page also on the free list; `check` finds a flip inside a 1-page run that a get does not; a get refuses a 1-page run reused as another page and a flip in a middle page of a run. Run allocation: a run takes a fit from records loaded for it, loads at most `RUN_LOAD_RECORDS` records, and an abort returns the records it loaded; leftover pages of a consumed record are listed under the keys consumed and reused by the next transaction; churn of 4–20 KB values keeps the file bounded (`run_churn_keeps_the_file_bounded`; with held readers, `run_churn_with_readers_keeps_the_file_bounded`, at most 1.75 times the live pages, and both heavy twins, 1.5 times over 50K commits and 1.75 over 20K). Merges free the siblings they absorb, which the save lists like any freed page, and it still settles within three rounds (`merges_feed_the_save_which_still_settles`) |
| `tests/crash.rs` | every crash point (`CrashAt`) × {process crash, strict power loss, random power loss with three seeds} × {`Durable`, `NoSync`} × {step 4 by `fdatasync`, by one `msync`}: the reopen recovers exactly the allowed state, which passes `check`, reads back through gets and both cursor scans, and survives a crash right after the reopen; a commit that grew the file survives a power loss by either data sync; the older state stays recoverable until its meta is overwritten |
| `tests/crash_runs.rs` | the same matrix (two seeds; the heavy twin 40 at 4 and 16 KiB pages) on a workload of overflow runs: puts of 1–10-page runs, replaces inline to run, run to inline, run to a larger and a smaller run, deletes, a drop of a table with runs; the commit does not grow the file and ends with a run at the top of its pages, so the one `msync` of step 4 alone must cover the run's interior pages (a growth's size sync is an `fsync` of every page), and the dry run asserts that layout: the last run ends at the state's page count (`last_run_ends_the_file`); a commit that grows the file through a run allocation survives a power loss by either data sync |
| `tests/damage.rs` | each kind of damage at rest fails the open closed (the file unchanged) or, once opened, is met by reads, copy-on-write, free-list loads and `check` as `Corrupt`, which latches; nothing panics. A free record listing a live leaf of a tree without runs: a `drop_table` or `clear_table` of that tree after a write handed the leaf out (as a tree page or a 1-page run) is `Corrupt`, frees nothing and leaves the other table readable (`a_free_of_a_tree_refuses_a_leaf_this_transaction_holds_for_another`); after the write committed, the drop frees the leaf again and, once it is reused, the other table's gets are `Corrupt` by txn identity (`a_live_leaf_freed_by_a_later_drop_is_corrupt_once_reused`) |
| `tests/damage_runs.rs` | a table with inline values and runs of 1, 3 and 9 pages, damaged at rest: `Corrupt` at a get for a flipped byte or a zeroed page in the middle of a run, a zeroed last page (zero trailer), a cell redirected to another run of the same txn (of the same pages: only the length tells) or to its key's stale run of the same length (only the txn identity tells), a cell's length changed, a run's kind changed, a 1-page run's page number or kind changed; at `check` for a flipped byte inside a 1-page run and an overflow counter one too high, which `drop_table` and `clear_table` also refuse; a free record listing a live run's middle page is `Corrupt` at `check`, at the get after a write reused the page (every other value reads back), or at the commit of a write that freed the run while the page sat in the pool; two cells naming one run, both deleted in one transaction, are `Corrupt` at the commit, which latches as damage (`two_cells_naming_one_run_freed_together_are_corrupt_at_commit`) |
| `tests/damage_merge.rs` | a file whose deletes merge leaves (table `l`) and, after an unlink, branches (table `b`), checked undamaged (`the_fixture_deletes_merge`); damage at rest to the sibling a merge absorbs, from either side (`a_merge_refuses_a_damaged_leaf_sibling`, `a_merge_refuses_a_damaged_branch_sibling`): a flipped byte (the CRC finds it), a stale txn in the parent's cell, a redirect outside the cell's separators, a cell naming a page or run by this transaction's id past the file's end, and for branches the parent's key coming down equal to the right page's key 1 (the seam check). Each is `Corrupt` at the delete, the environment latches, and the file is byte for byte as it was |
| `tests/merge.rs` | environment-level merging: deletes that merge leaves, a branch and the root, committed, reopened and read back against the model, with `Env::check` and a twin that does not merge (`deletes_that_merge_commit_and_reopen_to_the_model`); `RwCursor::delete_current` sweeps across leaves, every other key and down to one leaf, `next` returning each follower (`delete_current_sweeps_return_each_follower_while_pages_merge`); a put after a merge that freed or kept the cached leaf descends instead of using the stale cache (`a_put_after_a_merge_never_uses_the_cache_of_a_freed_page`); the crash matrix on a commit whose deletes merge and make the root give way (`every_crash_point_of_a_merging_commit_recovers_the_state_the_protocol_allows`, `_heavy`) |
| `tests/multi_model.rs` | a random multi-table workload with crashes against a model; its large-value variant spreads values over inline and 1–10-page runs, with drops and clears of tables holding runs; a draining variant (stretches of mostly deletes, `a_draining_multi_table_workload_with_crashes_matches_the_model`) makes pages merge, and every variant's coverage gate counts merging deletes |
| `tests/path_cache.rs` | random multi-table workloads (puts, deletes, clears, drops, commits, aborts, readers held) on a cached and an uncached environment: the same outcomes and byte-identical files after every commit; a cached leaf reused by another table in the same txn is not written; an aborted transaction's cache never reaches the next |
| `tests/append.rs` | `RwTxn::append`, `RwCursor::put`/`append` and both inserts against `RwTxn::put`: byte-identical files and a model across splits, aborts, deletes and stale path caches; an append out of order, and an insert of a present key (in the cached leaf, on the descent, over an overflow value), is refused without a latch, a change or a moved epoch; a cursor put or insert lands on its entry (`current`, `next`, `prev`) |
| `tests/txn_cursor.rs` | detached cursors sharing one write transaction: three `TxnCursor`s over two tables and the transaction's own puts, deletes and a `clear_table`, interleaved at random, against a model of each cursor (its entry, or the follower and the deleted key after a delete, and the end `last` sets, over the table's rows) in every result and state, with writes next to where the cursors sit (before and after their slot, deleting their entry and its neighbours), overflow values and refused inserts and appends; each round commits and checks, a cursor of an earlier round is `WrongTxn`, and the file reopens to the model (`txn_cursors_and_direct_writes_match_a_model`, `_heavy`); another table's writes, a table created and the cursor's own writes cause no reseek, a write to its table one (`a_write_to_another_table_causes_no_reseek`); a dropped table is `StaleTable` for every call, touched in the transaction or not, and after a re-create (`drop_table_under_a_live_cursor_is_stale_table`); a later transaction, the successor of an aborted one with its txn id, and another environment's are `WrongTxn`, changing nothing, without a latch (`a_cursor_used_with_another_transaction_is_wrong_txn`); a refused insert changes nothing and keeps the cursor and its saved key, and after a delete a reseek finds the follower by its key (`a_refused_insert_through_a_txn_cursor_changes_nothing`); `last` ends the table for `next` and `next_in_prefix` past another writer's append, until another move (`last_ends_the_table_for_next_until_another_move`); prefix steps after another cursor's insert and delete and through a group split under the cursor (`prefix_steps_through_txn_cursors_follow_other_writes`) |
| `tests/key_tail.rs` | `current_with_key_tail` on `Cursor`, `RoCursor` and `TxnCursor` against `current`: inline, empty and overflow values, over walks, seeks, prefix steps, a reseek and a delete; `seek_with_key_tail` on `RoCursor` and `TxnCursor` against `seek` then `current_with_key_tail` (hits, misses, both ends, an empty table, and the moves after: `seek_with_key_tail_is_a_seek_then_current_with_key_tail`), and a `TxnCursor` taken out of the end that `last` (or `prev` from unset) sets, and out of the deleted state, as `seek` takes it (`seek_with_key_tail_clears_a_txn_cursors_end_and_deleted_states`); the page codec's span is tested in `page.rs` (`key_tail_joins_the_keys_last_bytes_to_an_inline_value`, and on damaged pages) |
| `tests/concurrency.rs` | reader threads rescan pinned snapshots and take fresh ones while a writer replaces, deletes and clears (pages freed and reused): no read sees damage or a torn state |
| `tests/read_only.rs` | `Env::open_read_only` on a file of several commits (inline values, runs of 1–18 pages, a dropped table): every key reads back through gets and both scans, `check` passes, and the file is byte-identical with its mtime unchanged after the open, the reads, the check, a sync and the drop; a mode-0444 file opens (`a_read_only_open_reads_every_table_and_changes_nothing`); an unsynced meta in the page cache is read and still lost to a power loss: the open synced nothing (`a_read_only_open_syncs_nothing`); read-only opens coexist and exclude a writer both ways (`read_only_opens_share_the_lock_and_exclude_a_writer`); `begin_rw` and `try_begin_rw` are `ReadOnly` without a latch; a missing, empty or foreign file is refused with nothing created; a damaged newest meta falls back one txn, unrepaired; a read-only damage sweep (`damage_at_rest_is_met_by_read_only_reads_and_check`, on `tests/damage.rs`'s fixture): damage to a leaf, a branch, the catalog or the free list is `Corrupt` from the first get or scan that reaches it, else from `check`, latched by that operation, and a damaged tree root fails the open closed, the file unchanged |

```text
cargo nextest run -p tndb-b                                    # default suite (debug)
cargo test --release -p tndb-b -- --ignored --skip tndb_b_vs_mdbx  # heavy variants (about 4–5 min)
cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1  # benchmark
```

### Benchmark: tndb-b vs MDBX

`bench.rs` (`tndb_b_vs_mdbx`) runs each column in a fresh temp dir on the same seeded streams:
1M entries per bulk table, 8-byte BE sequential keys and 32-byte random keys, 100-byte values.

```text
cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

**Run tip (Linux):** pin the run to performance cores, and put the temp dir on a real disk (never
a tmpfs: on Linux the run stops if it is one), scoped to the one command. On a CPU with P and E
cores (`lscpu -e` shows which CPUs are which), for example:

```text
TMPDIR=/path/on/disk taskset -c 0-7 cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

Environment variables select what runs (the `bench.rs` module docs have the details):

- `TNDB_B_BENCH_N`: entries per bulk table (default 1,000,000).
- `TNDB_B_BENCH_ENGINES`: the columns, comma-separated (default all): `t4d`, `t16d`, `t4n`,
  `md`, `md4`, `mn`.
- `TNDB_B_BENCH_ROWS`: the row groups, comma-separated (default all): `seq`, `rand`, `get-hit`,
  `get-miss`, `scan-fwd`, `scan-rev`, `seek`, `small`, `mixed`, `clear`, `append` (the API rows),
  `big` (the large-value rows, see [Large values](#large-values-overflow-runs)), `delete` (the
  delete rows, see [Deletes](#deletes)), and the aliases `bulk`, `get` and `scan`.
- `TNDB_B_BENCH_REPEAT=k`: each selected read row runs k times and reports the median.

The columns:

- `t4d` and `t16d`: tndb-b `Durable` with 4 KiB and 16 KiB pages; `t4n`: 4 KiB `NoSync`.
- `md`: MDBX (`reth-libmdbx`) `Durable`, opened as `tn-storage` opens it: a `write_map`
  environment with the OS page size and a generous geometry. `md4`: the same with 4 KiB pages.
  `mn`: MDBX `SafeNoSync`. On x86_64 the OS page is 4 KiB, so `md` and `md4` are the same
  configuration there.

**Like-for-like pairs:** `t4d` with `md4` everywhere, and `t16d` with `md` on Apple Silicon,
where the OS page is 16 KiB. On x86_64 `t16d` has no MDBX twin. `t4n` has none either: a
`SafeNoSync` commit costs far more than a tndb-b NoSync commit on both platforms (see
[NoSync against SafeNoSync](#nosync-against-safenosync)).

A bulk row's "put loop" runs from `begin` to just before `commit`, and its "commit" is the rest.
The API rows time the put loop into a fresh environment: tndb-b's `RwTxn::append`,
`RwCursor::put` and `RwCursor::append` against MDBX's `put` with `WriteFlags::APPEND`,
`Cursor::put`, and `Cursor::put` with `WriteFlags::APPEND`. The "txn cursor" rows run the same
puts through `TxnCursor::put` and `TxnCursor::append` (tndb-b only): the cost of a cursor that
shares its transaction, against the `RwCursor` rows.

The "fdatasync" rows rerun the small-commit workload with commit step 4 forced to one
`fdatasync` of the whole file for every commit (a test-only switch, `Writer::data_by_fdatasync`).
Step 4 picks the `fdatasync` itself whenever the dirty pages form two or more ranges or more
than 16 runs, so the two modes differ only on one-range commits, which step 4 syncs by one
`msync`. The two modes alternate in blocks of 100 transactions (A B B A order) on the same
table, so neither gains from running second.

**Which code the tables measured.** The macOS and Linux tables below were both taken on
2026-10-20 with M1 and its performance work, before step 4 became one sync: each dirty range was
synced by its own `msync` (one `fdatasync` past 16 runs), and there the "fdatasync" rows replaced
those msyncs on every commit. They also predate every M2 change (format v2 and its page counters,
overflow runs, merging, the one-descent delete, the owned and detached cursors). The large-value
and delete tables further down were taken during M2, each with its code state given. None has
been rerun on the final M2 code; the Linux gate run will refresh them.

#### macOS run

**Run:** Apple M1 Max, 64 GiB RAM, internal SSD (APFS), macOS (Darwin 24.6), 2026-10-20; code: M1
with its performance work, per-range msyncs at step 4, before M2. Each cell is the median of
three full runs (each about 45 s). The Mac had background load, and the
MDBX cells moved by up to 20% between runs (`md` random insert: 779–894 ms), so a difference
under about 10% is noise. How step 4 syncs matters mainly to the small-commit rows: a bulk
commit is one range, synced by one `msync` before and after, and nearly every mixed-row commit
already took the `fdatasync` past 16 runs. Since step 4 became one sync, the Mac's small-commit
median is 73 us at 4K and 90 us at 16K, against 114 and 130 us here (see
[Step 4](#step-4-one-msync-or-one-fdatasync)).

| | `t4d` | `t16d` | `t4n` | `md` | `md4` | `mn` |
|---|--:|--:|--:|--:|--:|--:|
| seq insert 1M, 1 txn | 96.7 ms | 81.8 ms | 62.1 ms | 140.4 ms | 147.3 ms | 127.2 ms |
| seq: put loop | 59.8 ms | 47.3 ms | 57.0 ms | 110.5 ms | 115.4 ms | 109.6 ms |
| seq: commit | 36.9 ms | 34.5 ms | 5.1 ms | 29.9 ms | 31.5 ms | 16.1 ms |
| random insert 1M, 1 txn | 892.1 ms | 829.9 ms | 801.6 ms | 820.3 ms | 773.0 ms | 830.7 ms |
| random: put loop | 800.9 ms | 741.4 ms | 792.9 ms | 754.0 ms | 683.4 ms | 795.1 ms |
| random: commit | 91.2 ms | 91.1 ms | 8.6 ms | 66.3 ms | 61.1 ms | 30.8 ms |
| file size (st_size) | 520.0 MiB | 528.0 MiB | 520.0 MiB | 512.0 MiB | 512.0 MiB | 512.0 MiB |
| file used (pages × P) | 318.1 MiB | 311.0 MiB | 318.1 MiB | 312.3 MiB | 324.6 MiB | 312.3 MiB |
| get hit (per op) | 717 ns | 668 ns | 724 ns | 696 ns | 681 ns | 713 ns |
| get miss (per op) | 752 ns | 746 ns | 698 ns | 742 ns | 712 ns | 768 ns |
| scan forward 1M | 46.6 ms | 27.1 ms | 43.0 ms | 30.2 ms | 55.8 ms | 31.7 ms |
| scan reverse 1M | 38.8 ms | 24.9 ms | 36.6 ms | 30.0 ms | 37.1 ms | 29.2 ms |
| seek + 10 entries (per seek) | 1.2 us | 1.1 us | 1.1 us | 1.1 us | 1.1 us | 1.1 us |
| small txn (3 puts) median | 114.4 us | 129.9 us | 5.9 us | 1.3 ms | 1.3 ms | 2.3 ms |
| small txn p99 | 189.5 us | 192.0 us | 14.1 us | 3.8 ms | 3.8 ms | 6.6 ms |
| small txns total (2000) | 234.7 ms | 262.7 ms | 14.0 ms | 2.98 s | 2.95 s | 5.42 s |
| fdatasync: median | 75.3 us | 83.9 us | - | - | - | - |
| fdatasync: p99 | 125.8 us | 117.2 us | - | - | - | - |
| fdatasync: total | 155.7 ms | 171.9 ms | - | - | - | - |
| mixed 100k upd/del (1000/txn) | 3.34 s | 2.63 s | 256.5 ms | 3.27 s | 4.26 s | 1.62 s |
| clear_table 1M + commit | 1.4 ms | 569.0 us | 1.4 ms | 2.2 ms | 2.7 ms | 3.6 ms |
| append put 1M (put loop) | 53.9 ms | 48.3 ms | 59.4 ms | 64.6 ms | 69.2 ms | 67.7 ms |
| cursor put 1M (put loop) | 58.5 ms | 52.3 ms | 61.4 ms | 50.1 ms | 51.2 ms | 50.7 ms |
| cursor put APPEND 1M (put loop) | 57.8 ms | 51.5 ms | 60.2 ms | 57.9 ms | 61.6 ms | 59.2 ms |

**What M1's performance work changed** (this table replaced a run of the first M1 code). Every
guarantee and damage check is kept; the checks moved to where they are needed.

- **Provenance trust.** The writer's descent skips the per-fetch and separator-range checks on
  pages this transaction built, and computes the separator bounds only when it first meets a
  published page (item 11 of the
  [do-not-flag list](#intentional-design-decisions-please-do-not-flag-these)).
- **The path cache.** A table put that lands in the leaf of the table's last put goes straight
  into it, with no descent (item 12).
- **Append and cursor put.** `RwTxn::append`, and `RwCursor::put`/`append` placed from the path
  the write took. An append split leaves the left leaf as it is and refills the path cache.
- **One header parse per page, cold errors.** The parsed views (`View`, `LeafView`,
  `BranchView`, `LeafMut`) check a page's header once for many cell reads. The hot accessors are
  `#[inline]` and build their errors in `#[cold]` functions.
- **Search prefetch.** Each binary-search probe prefetches the two cells the next probe may read.
- **Cheaper splits.** `SplitBuf` plans a split in reused buffers and writes the right half
  straight into the zeroed new page, with no heap allocation per split.

At 16K, against the run this table replaces: the sequential insert went from 391 to 82 ms (its
put loop from about 367 to 47 ms), the random insert from 1097 to 830 ms, the forward scan from
38.8 to 27.1 ms, and a seek from 1.3 to 1.1 us. Small commits, the mixed workload and
`clear_table` stayed level in back-to-back comparisons.

#### Linux run

**Run:** Linux x86_64 laptop (i7-1360P, NVMe, ext4), 2026-10-20, three full runs; code: M1 with
its performance work, per-range msyncs at step 4 (before the one-range rule), before M2. Each run
took 132–163 s.

**CPU-bound rows.** Each cell is the median of the three runs. tndb-b's cells repeated within a
few percent, and the median drops a few one-run outliers in `mn`. The NoSync bulk totals are
shown for `t4n` and `mn` only; the durable totals follow the drive (below).

| | `t4d` | `t16d` | `t4n` | `md` | `md4` | `mn` |
|---|--:|--:|--:|--:|--:|--:|
| seq: put loop | 53.5 ms | 46.0 ms | 53.6 ms | 115.7 ms | 115.8 ms | 115.8 ms |
| random: put loop | 512.9 ms | 557.8 ms | 517.4 ms | 544.2 ms | 546.8 ms | 563.3 ms |
| seq insert 1M, 1 txn (NoSync) | - | - | 60.3 ms | - | - | 116.1 ms |
| random insert 1M, 1 txn (NoSync) | - | - | 531.3 ms | - | - | 563.8 ms |
| file size (st_size) | 516.0 MiB | 528.0 MiB | 516.0 MiB | 512.0 MiB | 512.0 MiB | 512.0 MiB |
| file used (pages × P) | 318.1 MiB | 311.0 MiB | 318.1 MiB | 324.6 MiB | 324.6 MiB | 324.6 MiB |
| get hit (per op) | 497 ns | 545 ns | 494 ns | 587 ns | 602 ns | 603 ns |
| get miss (per op) | 505 ns | 569 ns | 504 ns | 619 ns | 626 ns | 632 ns |
| scan forward 1M | 31.9 ms | 20.8 ms | 31.9 ms | 42.7 ms | 42.5 ms | 35.2 ms |
| scan reverse 1M | 32.6 ms | 21.2 ms | 32.6 ms | 30.2 ms | 30.3 ms | 30.4 ms |
| seek + 10 entries (per seek) | 1.1 us | 855 ns | 1.1 us | 1.0 us | 1.0 us | 1.1 us |
| append put 1M (put loop) | 52.2 ms | 46.0 ms | 52.8 ms | 56.2 ms | 56.2 ms | 55.6 ms |
| cursor put 1M (put loop) | 55.4 ms | 48.6 ms | 55.7 ms | 48.3 ms | 47.5 ms | 47.2 ms |
| cursor put APPEND 1M (put loop) | 54.4 ms | 48.2 ms | 54.8 ms | 52.6 ms | 52.9 ms | 52.1 ms |

The MDBX forward-scan medians hide a split: each MDBX column had runs of about 32 ms and runs of
about 42.7 ms, while tndb-b's scans moved by at most 0.2 ms. The cause is unconfirmed; the core
type the run landed on is one guess (see the run tip).

**Durable rows.** Each cell is the range (min–max) of the three runs.

| | `t4d` | `t16d` | `md` | `md4` |
|---|--:|--:|--:|--:|
| seq: commit | 32.7–119.3 ms | 28.6–127.2 ms | 33.0–143.4 ms | 104.0–170.9 ms |
| random: commit | 51.9–214.6 ms | 63.0–223.8 ms | 50.2–406.3 ms | 191.7–280.6 ms |
| small txn (3 puts) median | 904.3–920.2 us | 703.2–931.3 us | 543.6 us–3.5 ms | 3.4 ms (all three) |
| small txn p99 | 4.9–16.9 ms | 5.6–7.1 ms | 848.6 us–7.0 ms | 3.9–7.3 ms |
| small txns total (2000) | 2.15–5.45 s | 1.85–4.29 s | 1.13–8.19 s | 5.72–6.48 s |
| fdatasync: median | 514.5–515.9 us | 531.8–568.1 us | - | - |
| fdatasync: p99 | 3.4–7.2 ms | 3.5–3.9 ms | - | - |
| fdatasync: total | 1.21–3.17 s | 1.54–3.38 s | - | - |
| mixed 100k upd/del (1000/txn) | 7.90–57.80 s | 3.11–10.62 s | 33.33–43.74 s | 3.47–27.54 s |
| clear_table 1M + commit | 4.3–9.1 ms | 900.8 us–4.0 ms | 1.6–11.6 ms | 1.6–7.2 ms |

**These rows measure the drive's state, not the engine.** On x86_64 `md` and `md4` are the same
configuration, yet within one run they differed by up to 12.6× (mixed 43.7 s vs 3.5 s;
sequential commit 33 vs 171 ms). By estimate, each column writes 1–3 GB (two bulk loads, the
mixed commits and three fresh 1M-entry files), so a later column meets a busier drive. The
fastest runs are level:
sequential commit 32.7 ms (`t4d`) vs 33.0 ms (`md`), random commit 51.9 vs 50.2 ms.

**The small-commit A/B rows do not drift with the drive,** because the two modes alternate on one
table. They are why step 4 changed (see [Step 4](#step-4-one-msync-or-one-fdatasync)). Under the
one-range rule, about 3 in 4 small commits have two or more ranges and take the `fdatasync` path,
so the next Linux run should show a small-commit median near the "fdatasync" rows. That is not
yet measured.

#### Where tndb-b wins and loses

From the macOS and Linux tables above, so for the code before M2.

**On Apple Silicon, where tndb-b loses, and why** (the macOS table):

- **The commit of a bulk load costs more:** 91 ms against MDBX's 61–66 ms after the random
  insert, and 35–37 ms against 30–32 ms after the sequential one. A tndb-b commit stamps a CRC on
  every page it wrote, msyncs the dirty pages (one range for a bulk load), and fsyncs the file's
  growth; in a profile of the sequential commit, msync took about 77%, CRCs 15% and the growth
  fsync 8%.
- **Random inserts at 4K:** the put loop takes 801 ms against `md4`'s 683 ms (+17%). Random keys
  rarely land in the cached leaf, so each put descends. MDBX's put loop gets faster at 4K than at
  16K while tndb-b's gets slower, which points at tndb-b's per-page costs: each level of the
  descent, and each split (about four times as frequent per entry at 4K). At 16K the put loop is
  level with MDBX (741 vs 754 ms).
- **A plain cursor put at 4K** takes 58.5 ms against `md4`'s 51.2 ms (+14%); at 16K it is level
  (52.3 vs 50.1 ms).
- **Point reads at 4K** are about 5% slower than `md4` (717 vs 681 ns hit, 752 vs 712 ns miss),
  which is at the noise level. A fetch from the published file keeps its range, trailer, page
  number and txn checks, and a miss checks the leaf against its separators. At 16K reads are level
  with MDBX.

**On Apple Silicon, where tndb-b wins:**

- **Sequential loads:** the put loop is 2.3× faster than MDBX's at 16K (47 vs 111 ms) and 1.9×
  at 4K (60 vs 115 ms), because almost every put lands in the cached leaf. A plain put even beats
  MDBX's `APPEND` put (65–69 ms). `RwTxn::append` takes 48–54 ms, and `RwCursor::append` is a
  little faster than MDBX's cursor `APPEND` (51.5 vs 57.9 ms at 16K, 57.8 vs 61.6 ms at 4K).
- **Small durable commits are about 10× cheaper** in the table (130 us vs 1.3 ms median at 16K,
  114 us vs 1.3 ms at 4K), and more since step 4 became one sync (90 and 73 us; see
  [Step 4](#step-4-one-msync-or-one-fdatasync)). tndb-b syncs only the pages the commit dirtied,
  by one `msync` or one `fdatasync`, plus one meta page. MDBX msyncs the whole used span of the
  map on every commit (about 312 MB here), and on macOS that walk is the cost.
- **Scans** are 10–17% faster than MDBX's at 16K (27.1 vs 30.2 ms forward, 24.9 vs 30.0 ms
  reverse). At 4K the forward scan beats `md4` (46.6 vs 55.8 ms), and the reverse one is level.
- **`clear_table`** frees a 1M-entry tree 2–4× faster (569 us vs 2.2 ms at 16K).
- **The durable mixed workload** matches MDBX or beats it (2.63 vs 3.27 s at 16K, 3.34 vs 4.26 s
  at 4K). MDBX's own cell moved most between runs here (2.72–3.33 s at 16K), so read this row as
  parity.
- **File sizes match:** the used pages within 2%, the file lengths within 3%.

**On Linux x86_64** (the Linux tables; "MDBX" is the median of the six `md` and `md4` runs, the
same configuration there):

- **At 4K, tndb-b wins** the sequential put loop by 2.2× (53.5 vs 115.7 ms), the random put loop
  by 6% (512.9 vs 545.5 ms), `RwTxn::append` by 7% (52.2 vs 56.2 ms), and gets by 16–19% (497 vs
  589 ns hit, 505 vs 622 ns miss). Small durable commits took 0.90–0.92 ms with per-range
  msyncs, against 3.4–3.5 ms in 5 of the 6 MDBX runs; the sixth, an `md` run, took 0.54 ms.
- **At 4K, level:** the forward scan against MDBX's fast runs (31.8–31.9 ms both; its slow runs
  took about 42.7 ms), and the bulk commits at the fastest runs (above).
- **At 4K, tndb-b loses** on a plain cursor put (+15%: 55.4 vs 48.2 ms), the reverse scan (+8%:
  32.6 vs 30.3 ms) and the cursor `APPEND` (+3%: 54.4 vs 52.6 ms). A seek is about level (1.0–1.1
  vs 1.0 us, at the table's 0.1 us resolution).
- **At 16K, tndb-b beats MDBX on every CPU row but two:** the random put loop (557.8 vs 545.5 ms,
  +2%) and the plain cursor put, which is level (48.6 vs 48.2 ms).
- **4K against 16K:** 16K wins the scans by 35%, a seek by about 20% and the sequential puts by
  11–14%. 4K wins gets by 9–11% and random puts by 8%.
- **The Mac's 4K losses do not occur here.** The random put loop (+17% on the Mac) and point
  reads (+5%) both beat MDBX at 4K, and tndb-b's random put loop is faster at 4K than at 16K, so
  the per-page explanation above is an Apple Silicon finding.

#### NoSync against SafeNoSync

**On macOS**, MDBX under `SafeNoSync` still issues an `MS_ASYNC` msync of the whole span every
commit (`MDBX_MMAP_NEEDS_JOLT` is set off Linux). That makes its small commits slower than its
own Durable mode (2.3 vs 1.3 ms median), and it is most of why tndb-b's NoSync mixed workload is
about 6× faster (256.5 ms vs 1.62 s).

**On Linux**, `mn` took a median of 0.58–3.5 ms per small commit (`t4n` 3.8 us) and 4.3–5.4 s on
the mixed row (`t4n` 0.25 s). The cause was not investigated.

On both platforms, a tndb-b NoSync commit does no I/O, and its deferred `Env::sync` is not timed.

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

The Linux numbers are the A/B rows of the Linux run above; the macOS ones come from the Mac of the
macOS run, 2026-10-20, in targeted runs on the same code.

**After the change**, on the Mac (medians of three targeted runs, 2026-10-20, before M2), the
small-commit median is 73.2 us at 4K and 90.2 us at 16K (it was 114 and 130 us; the "fdatasync"
rows read 72 and 81 us in the same runs), and the bulk commits, the mixed row and `clear_table`
are level (sequential commit 34.5 vs 34.9 ms, random 85.5 vs 87.9 ms at 4K). On Linux the small
commits are not yet measured under the new rule (see the Linux run).

**Open:** at 16K on the Mac, a one-range small commit synced by `msync` is still slower than by
`fdatasync` (median 89–90 vs 81–82 us, p99 158–165 vs 96–97 us). At 4K the two match.

#### Large values (overflow runs)

The `big` rows (`TNDB_B_BENCH_ROWS=big`) put values of 2, 8 and 32 KiB: 20k, 10k and 4k entries
under 8-byte keys in a scrambled order, in one transaction. They time the put loop (per put),
its commit, and then a get of every key in another scrambled order (per get). At 4 KiB pages
the values take runs of 1, 3 and 9 pages; at 16 KiB the 2 KiB value is inline and the others take
runs of 1 and 3 pages. Each value size runs in its own fresh environment, so no row writes into
file space another row grew and synced (see the macOS note in
[Platform notes](#platform-notes)). `TNDB_B_BENCH_N` does not scale these rows.

**Run:** the Mac above, 2026-10-20, medians of three runs; code: M2 with overflow runs and the
one-sync step 4, before run allocation loaded records for a fit, merging and the later M2
changes. Not rerun since.

| | `t4d` | `t16d` | `md` | `md4` |
|---|--:|--:|--:|--:|
| put 2 KiB (per op) | 918 ns | 1.60 us | 1.30 us | 671 ns |
| commit | 29.9 ms | 19.0 ms | 16.6 ms | 22.9 ms |
| get 2 KiB (per op) | 302 ns ¹ | 419 ns | 361 ns | 293 ns |
| put 8 KiB (per op) | 2.00 us | 2.30 us | 2.00 us | 1.70 us |
| commit | 36.5 ms | 61.3 ms | 38.0 ms | 32.4 ms |
| get 8 KiB (per op) | 1.10 us | 271 ns ¹ | 286 ns | 286 ns |
| put 32 KiB (per op) | 4.30 us | 8.00 us | 4.90 us | 4.20 us |
| commit | 57.9 ms | 65.0 ms | 46.3 ms | 36.5 ms |
| get 32 KiB (per op) | 2.20 us | 2.60 us | 282 ns | 274 ns |

¹ A 1-page run, read with its header checks only (do-not-flag item 13). These two cells come from
a later set of three interleaved runs, after that change; before it the same gets took 724 ns and
1.30 us, and the multi-page gets did not move.

- **Puts** of 2 and 8 KiB at 4 KiB pages are level with MDBX or within about 0.3 us of `md4`, and
  32 KiB puts are level with `md4` (4.3 vs 4.2 us). The `t16d` 32 KiB put varied most (5.4–8.0 us
  across the three runs).
- **Gets** of a multi-page run cost the CRC pass over the value (item 13): 1.1 and 2.2 us at 4 KiB
  pages against MDBX's 0.3 us, which hands back a pointer. A 1-page run reads like an inline
  value.
- **Commits** stamp one CRC per run and sync every run page, so they grow with the bytes written,
  as MDBX's do, but faster: at 32 KiB, 57.9 ms for `t4d` against 36.5 ms for `md4`. The cause is
  not investigated.

#### Deletes

The `delete` rows (`TNDB_B_BENCH_ROWS=delete`) time deletes, which merge pages in tndb-b (see
[Deletes and merging](#deletes-and-merging)) and rebalance them in MDBX. Each runs in a fresh environment, so it
changes no table another row reads:

- **delete 90% + commit:** a random table of N entries is filled (untimed), then 90% of its keys
  are deleted in a random order in one transaction, timed as a whole and split into the delete
  loop and the commit, as a bulk insert is.
- **scan after:** a forward scan of the tenth left, which shows how well the deletes left the
  pages filled.
- **mixed 50/50:** operations over a random table of N entries in transactions of a fixed size:
  half delete a random present key, and half put, re-inserting the key deleted longest ago. The
  table stays within a few hundred entries of N, so the row measures deletes and inserts at a
  steady size.
- **churn file size (merge on/off):** ten rounds over a table of N/4 random keys, each a fill and a
  delete of 90% of the keys at random, timed as a whole; then the file's used span (pages times
  the page size) and its size. tndb-b runs the churn twice, with merging and without (the "churn
  (no merge)" rows, through the test-only `Writer::no_merge`), to show what merging saves.

**Run:** Apple M1 Max, macOS, 2026-10-21, N = 300k, medians of three runs; code: M2 with merging
and the one-descent delete, before the owned and detached cursors, the MDBX-aligned state after a
delete and the adapter. Not rerun since. The "merging off" column reruns tndb-b's rows with
merging turned off; MDBX's two columns are `md` (16 KiB pages here) and `md4`.

| | `t4d` | `t16d` | `t4d`, merging off | `md` | `md4` |
|---|--:|--:|--:|--:|--:|
| delete 90% + commit | 232.0 ms | 268.8 ms | 235.7 ms | 220.3 ms | 213.9 ms |
| delete loop | 207.2 ms | 245.7 ms | 210.6 ms | 204.6 ms | 197.4 ms |
| commit | 24.5 ms | 22.8 ms | 25.9 ms | 16.3 ms | 16.9 ms |
| scan of the tenth left | 2.5 ms | 1.0 ms | 7.6 ms | 0.9 ms | 1.8 ms |
| mixed 50/50, 100k ops | 923.3 ms | 1009.5 ms | 940.8 ms | 1291.6 ms | 1603.9 ms |
| churn, 10 rounds of 75k | 729.2 ms | 761.4 ms | 742.5 ms | 811.4 ms | 743.2 ms |
| churn: table pages × P | 2.3 MiB | 2.4 MiB | 13.1 MiB | 3.4 MiB | 3.3 MiB |
| churn: file used | 45.6 MiB | 44.9 MiB | 45.7 MiB | 30.2 MiB | 31.1 MiB |

A single run at N = 1M agrees: the delete loop takes 892 ms (`t4d`) and 1046 ms (`t16d`) against
MDBX's 908 ms (`md`) and 1021 ms (`md4`), and after the churn tndb-b's table holds 7.8 MiB
(43.8 MiB without merging) against MDBX's 11.2 MiB, in a used span of 152 MiB against
101–104 MiB.

What the rows show:
- **Merging works.** After the churn the table takes 5–6 times fewer pages than without merging
  (2.3 against 13.1 MiB at 4K), and is smaller than MDBX's. After deleting 90% of a table, the scan
  of what is left is three times faster at 4K.
- **Merging costs almost nothing.** Delete, mixed and churn times are within a few percent of
  merging off. The other rows (puts, small commits, mixed updates, `clear_table`) are unchanged.
- **Random deletes are level with MDBX's.** At 300k the 4K delete loop is within 5% of `md4`
  (207 vs 197 ms, the noise level here), and at 1M it is level or faster (892 vs 908–1021 ms).
  They were 44% slower while `write::delete` descended twice, once to look the key up, so a miss
  changed nothing, and again to plan the change (304 vs 200 ms in the same session). Now one
  descent does both: it defers the verification of the published pages it passes to a hit
  (`Verify::Later`, `verify_path`), so a miss still changes and verifies nothing. That cut the
  delete loop by 32% at 4K and 30% at 16K, and the churn by 21–25%. What is left of the delete
  loop is that one descent and the leaf search (about 60% of it in a profile), the cell's removal
  (12%), and the copies of the path (6% zero-filling the page `alloc` returns, then the copy).
- **The mixed 50/50 workload is faster than MDBX's** at both page sizes.
- **After the churn, the file's used span is about 1.5× MDBX's,** even though tndb-b's tree is
  smaller. Each round is a fill transaction followed by a delete transaction, and a page freed by
  one transaction is reused only from the second transaction after it, once no meta slot names a
  state that reaches it (the reuse gate). So each fill grows the file instead of reusing the pages
  the delete just freed. The gate is deliberate: it keeps the previous state recoverable if the
  newest meta is lost.
