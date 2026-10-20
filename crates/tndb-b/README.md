# `tndb-b`: a copy-on-write B+tree key-value engine

An embedded key-value engine in the LMDB/MDBX mould. One page file holds every table as a
copy-on-write B+tree with the values in its pages. A single writer builds the next version of
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
does not depend on `tn-storage`.

| file | contents |
|---|---|
| `lib.rs` | crate docs, `compile_error!` off unix, re-exports |
| `env.rs` | `Env`, `EnvOptions`, `SyncMode`, `EnvStat`: open and recovery, the writer hand-off, snapshots, the reader queue, the durable steps of a commit, `sync` |
| `txn.rs` | `RoTxn`, `RwTxn`, `ReadTxn`, `Table`, `TableStat`, `RwCursor`; commit steps 1–3 and 7 |
| `meta.rs` | the 128-byte meta record, `choose_meta`, `slot_to_write` |
| `page.rs` | the slotted page codec, `Geometry` (limits), `validate` |
| `tree/` | `TreeRoot`, the `PageRead`/`PageWrite` stores, checked descent and lookup (`read.rs`), copy-on-write put/delete/split/free (`write.rs`), the bidirectional cursor (`cursor.rs`) |
| `alloc.rs` | the writer's page allocator (`private`, `stamped`, `pool`, `freed`, `next_pgno`) |
| `freelist.rs` | free-record codec, gated loading, the fixed-point save |
| `catalog.rs` | the table catalog (`TableRecord`) |
| `check.rs` | `Env::check`: the full structural check and exact page accounting |
| `latch.rs` | the failure latch |
| `file.rs` | `PageFile` (reserved mapping, preallocated growth, msync, poison) and `MapView` (the lock-free reader view) |
| `sys.rs` | `fsync_file`, `fdatasync_file`, `fsync_directory`, `allocate_range`, `os_page_size`, `flock_exclusive` |
| `crc.rs`, `page_set.rs` | copied from `crates/storage/src/archive/` (`page_set.rs` adds a touched-word list, so `clear` and `drain` cost the words touched, not the file) |
| `disk_model.rs` | test-only page-cache and power-loss model |
| `tests/`, `*/tests.rs` | crash, damage, multi-table model and concurrency suites, per-module tests |
| `bench.rs` | the on-demand benchmark against MDBX |

---

## Contents

1. [Concepts at a glance](#concepts-at-a-glance)
2. [On-disk format](#on-disk-format)
3. [Commit protocol](#commit-protocol)
4. [Free list and page reuse](#free-list-and-page-reuse)
5. [Readers and snapshots](#readers-and-snapshots)
6. [Open and recovery](#open-and-recovery)
7. [Failure latch](#failure-latch)
8. [Concurrency rules](#concurrency-rules)
9. [Public API reference](#public-api-reference)
10. [Errors](#errors)
11. [Platform notes](#platform-notes)
12. [Limits and non-goals](#limits-and-non-goals)
13. [Intentional design decisions: please do not flag these](#intentional-design-decisions-please-do-not-flag-these)
14. [Tests and benchmarks](#tests-and-benchmarks)

---

## Concepts at a glance

- **One file of fixed-size pages.** The page size P is a power of two from 4 KiB to 64 KiB. It is
  chosen when the file is created (default 4096) and kept for the file's life. Pages 0 and 1 are
  the metas, and every other page is a branch or a leaf of some tree. Pages are numbered by `u32`.
- **Tables are named B+trees** with byte-string keys and values stored in the leaves. A
  **catalog** tree maps each table name to the table's root. A **free tree** lists the pages
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

**Not yet (M1 scope).** No overflow pages (an entry is capped at a quarter of a page), no leaf
merging, no dup-sort tables, no multi-process or read-only open, no reth `db-api` adapter. See
[Limits and non-goals](#limits-and-non-goals).

---

## On-disk format

All integers are little-endian unless marked BE.

### Meta pages (pages 0 and 1)

Each meta page holds a 128-byte record at offset 0, and the rest of the page is zero (`meta.rs`):

| off | size | field |
|---|---|---|
| 0 | 8 | magic `b"tndb-b\0\0"` |
| 8 | 4 | format version = 1 |
| 12 | 4 | page size P |
| 16 | 8 | txn id |
| 24 | 4 | page count (pages `0..page_count` are allocated) |
| 28 | 4 | next table id (first table: 1) |
| 32 | 24 | catalog `TreeRoot` |
| 56 | 24 | free-tree `TreeRoot` |
| 80 | 44 | reserved (zero) |
| 124 | 4 | CRC32 over bytes 0..124 |

Decoding (`Meta::decode`) runs its checks in this order:

1. The magic must match, else `NotTndbB`.
2. The version must be 1, else `UnsupportedVersion`. This runs before the CRC check, because
   another version may lay the record out differently.
3. The CRC must match, else `Corrupt`.
4. The remaining fields must be possible, else `Corrupt`: a valid page size, no non-zero reserved
   byte, well-formed tree roots, a `page_count` of at least 2, and a next table id of at least 1.

`page_count` in the meta is the only authoritative length. The file may be longer (growth slack).

### `TreeRoot` (24 bytes)

`root u32 | root_txn u32 | height u32 | reserved u32 | entries u64`.

- `root_txn` is the low 32 bits of the root page's header txn.
- The empty tree is exactly `root = u32::MAX, root_txn = 0, height = 0, entries = 0`, and costs no
  pages.
- A non-empty tree has a height in `1..=48` (`MAX_HEIGHT`).
- A non-zero reserved word, or any other shape, is `Corrupt`.

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
- **Leaf cell:** `vlen u32 | klen u16 | flags u16 | key | value`. M1 refuses non-zero cell flags,
  which are reserved for overflow and dup-sort.
- **Branch cell:** `child u32 | child_txn u32 | klen u16 | key`. There is one cell per child, in
  LMDB style. Cell i's key is the lower bound of child i, and cell 0's key is empty and stands for
  −∞. `child_txn` is the low 32 bits of the child page's header txn.
- **CRC trailer.** It is stamped at commit with `add_crc32_nonzero`, which maps a computed CRC of 0
  to a fixed non-zero value. **An all-zero trailer means "dirty"**: a page being built by the
  writer. A published page with a zero trailer is `Corrupt`.

### Limits

`U = P − 28` (the header plus the trailer), and `MAX_ENTRY = U / 4` (a cell plus its slot). The
derived limits are `max_key = MAX_ENTRY − 12`, so any key fits a branch cell as a separator, and
`max_value(klen) = MAX_ENTRY − 10 − klen` for `klen <= max_key` (0 for a longer key, which is
refused with any value). `page::validate` holds every stored page to both: a leaf entry within
`MAX_ENTRY` can still carry a key up to 2 bytes over `max_key`, so its key is checked too.

| P | U | MAX_ENTRY | max key | max value (8-byte key) | max value (32-byte key) |
|---|---|---|---|---|---|
| 4 KiB | 4068 | 1017 | 1005 | 999 | 975 |
| 16 KiB | 16356 | 4089 | 4077 | 4071 | 4047 |
| 64 KiB | 65508 | 16377 | 16365 | 16359 | 16335 |

**One split always suffices.** An overflowing page holds at most `U + MAX_ENTRY` bytes. Cutting
it at the first entry where the left part reaches half leaves both parts within U.

Larger entries are refused up front with `KeyTooLarge` or `ValueTooLarge`, before anything
changes. `Env::max_key_len` and `Env::max_value_len` report the limits.

### Catalog

The catalog is a B+tree keyed by table name: UTF-8, 1..=255 bytes. Each value is a 48-byte
`TableRecord`:

`id u32 | flags u32 (0) | root TreeRoot (24) | created_txn u64 | reserved 8`.

Table ids are unique over the file's life, so a dropped and re-created table gets a new id.

### Free-list records

- **Key** (12 bytes): `txn u64 BE | seq u32 BE`. The key holds the committing transaction and a
  sequence number, so records sort by txn.
- **Value:** page numbers as `u32` LE, strictly ascending. A value holds at most
  `record_capacity = max_value(12) / 4` page numbers (248 at 4 KiB). Empty values are allowed.

---

## Commit protocol

`RwTxn::commit` with `SyncMode::Durable` (`txn.rs` `Work::commit`, `env.rs`
`Writer::make_durable`) runs these steps:

| step | what |
|---|---|
| 0 | Latched: return `Failed`. A transaction that changed nothing returns its base txn id with no I/O. |
| 1 | Write each changed table's root into the catalog (in name order), then save the free list (see below). |
| 2 | Build meta M in memory: txn L+1, both roots, the page count. |
| 3 | Stamp the CRC of every page the transaction created (they become "stamped by this writer"). |
| 4 | `msync(MS_SYNC)` the dirty pages. Runs separated by at most 64 clean pages (`MERGE_GAP`) are coalesced, and each range is one msync call. If the pages form more than 16 runs of adjacent pages (`MAX_RANGES`), one `fdatasync` of the file replaces the msyncs (`data_sync`). |
| 5 | If the file grew since the last sync, run `fdatasync_file` so its size is durable. |
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
pool and `next_pgno` return to the transaction's base, and its private pages are simply
unreferenced. There is no I/O.

**An `Err` from commit** latches the environment (`fail_damaged` for `Corrupt`, otherwise `fail`),
and whether the commit survives is unknown until a reopen. Validation errors (`KeyTooLarge`,
`ValueTooLarge`, `AppendOutOfOrder`, table errors) happen in `put` and the other calls, before
anything changes, and do not latch.

---

## Free list and page reuse

**During a transaction** (`alloc.rs`):

- A published page that leaves a tree goes to `freed`.
- A page the transaction itself created that leaves a tree ("loose") goes straight back to
  `pool`.
- An allocation takes a page from `pool`, else the next page number at the end of the file,
  growing the file.
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

**Reuse timing.** Every page a record of txn M lists is unreachable from the states M onwards. In
`Durable` mode, after commit N the slots hold N−1 and N. So a page freed by txn N becomes
reusable at txn **N+2**, once N+1 has committed and the slots hold N and N+1. In `NoSync` mode the
slots move only at a sync, so a freed page comes back once two syncs have made states from N on
durable in both slots.

**The save at commit** (`freelist::save`) runs with loading stopped, so pages come only from the
pool or the end of the file:

1. Delete the records the transaction consumed.
2. Write `want = sort(freed ∪ pool)` as records `(M, 0..k)` of the committing txn M, chunked
   deterministically by `record_capacity`.
3. Writing can take pool pages and free pages of the free tree, which changes `want`. So write
   again until the records list exactly `want`: a fixed point.

Record keys are never deleted inside the loop. A record no longer needed is written empty, so a
round never frees a page a later round must list. Three rounds suffice. More than 8 is a broken
invariant, which returns `Failed` and latches.

**Leftover pages are re-listed, as in LMDB.** When a transaction consumes a record but uses only
some of its pages, the rest stay in `pool`. They are saved again under the new txn M, so they wait
two more commits before reuse.

**The accounting invariant** that `Env::check` enforces after every commit in the model tests is:

```text
(pages reachable from the catalog, every table and the free tree)  ⊎  (page numbers in free records)  =  [2, page_count)
```

The union is disjoint: every page is used by exactly one tree, once, or listed exactly once. Any
leak or double use is `Corrupt` naming the pages, and it latches.

---

## Readers and snapshots

A `Snapshot` holds `{ meta, geometry, view: Arc<MapView> }`. It is published as an `Arc` under a
`parking_lot::RwLock`. `begin_ro` clones the current one, so a reader sees one state of every
table. The environment's own `Arc` to the current snapshot keeps the reader queue non-empty.

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

**The writer skips them on pages it built.** Its descent (`write::descend_write`) trusts a page
this transaction's writer built from checked pages and still holds private: the root when its
holder's flag says the writer wrote it, and below a trusted page every private child. A trusted
page gets only the identity, kind and non-empty-leaf checks, which read the header line that is
loaded anyway. The per-fetch checks and the separator-range check run on the untrusted
(published) pages, the only ones damage at rest or a redirected pointer can reach, and so does
`verify_published` unless this writer stamped the page. See
item 11 of the [do-not-flag list](#intentional-design-decisions-please-do-not-flag-these).

**CRCs are not recomputed on every read.** That would cost a pass over the page each time. The CRC
is verified in these places instead:

- by copy-on-write, before a page is copied (unless this writer stamped it itself), together with
  a full `page::validate`;
- for the tree roots at open;
- for free-list leaves before their pages are reused;
- by `Env::check`.

The public `Cursor` remembers the leaf it is at. A step inside that leaf skips the fetch: the page
passed every check when the cursor entered it, and the transaction's pages cannot change while
the cursor borrows it. Entering a leaf always runs the full descent checks.

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
it, so later commits can build on a meta the disk does not hold. After a write error the node
stops; do not reopen the file in the same boot. Reboot first, or copy the file to a new one.
`Env::check` reads the same cache and cannot tell.

---

## Failure latch

`latch.rs` keeps the first cause and never clears it, and only the first cause is logged
(`tracing::error!`, target `tndb_b`).

| latches (later writes and commits return `Failed`) | does not latch |
|---|---|
| any error inside `commit` or `sync` (`fail`; `fail_damaged` for `Corrupt`) | `KeyTooLarge`, `ValueTooLarge` |
| an I/O or growth error in a write (`fail`); a failed msync also poisons the file, which is never retried | `TableNotFound`, `StaleTable`, `InvalidTableName` |
| a panic part way through a write, commit or sync (`UnwindLatch` guard) | a miss (`Ok(None)`) |
| `Corrupt` from any read, get or cursor move (`fail_damaged`) | errors from `Env::open` (there is no environment yet) |
| a failed `Env::check` | |

After a latch, `begin_rw` and `try_begin_rw` return `Failed`, and reads continue on the published
snapshots. `Env::failure()` reports the cause. A latched NoSync environment skips its final sync
on drop.

---

## Concurrency rules

| type | thread safety |
|---|---|
| `Env` | `Clone + Send + Sync`. Clones share one environment, which closes when the last clone and the last transaction drop. |
| `RoTxn` | `Send + Sync`. It holds an `Arc` of the environment and of its snapshot, so the mapping outlives it. |
| `RwTxn` | `Send + Sync`. It owns the boxed writer state, taken through a `Mutex` + `Condvar` hand-off. |
| `Cursor<'t>`, `RwCursor<'t>` | borrow their transaction (`Cursor` is `Send + Sync`). |

- `begin_rw` blocks while another write transaction is open, from any thread. `try_begin_rw`
  returns `None` instead. The writer returns to the environment when the `RwTxn` drops (commit
  consumes it).
- **Deadlock note:** a thread that holds an `RwTxn` and calls `begin_rw`, or **`Env::sync`**,
  waits forever. `sync` needs the writer state, as `begin_rw` does. Commit or drop the transaction
  first.
- Readers never block the writer and the writer never blocks readers. A long-lived reader only
  holds back page reuse, so the file grows instead.
- Multi-process access is out of scope. The `flock` makes a second process's open fail with
  `Locked`.

---

## Public API reference

| item | description |
|---|---|
| `Env::open(path, EnvOptions) -> Result<Env>` | open or create, then recover (see above) |
| `EnvOptions { page_size, map_reserve, sync }` | defaults: 4096, 1 GiB, `SyncMode::Durable`. `page_size` applies at create; an existing file keeps its own. |
| `SyncMode::{Durable, NoSync}` | durable at commit, or at the next `Env::sync` or drop |
| `Env::begin_ro() / begin_rw() / try_begin_rw()` | start transactions |
| `Env::sync()` | make NoSync commits durable (a no-op when nothing is pending) |
| `Env::check() -> CheckReport` | full structural check plus exact page accounting (`CheckReport`, `TableCheck`) |
| `Env::stat() -> EnvStat` | `page_size`, `txn_id`, `page_count`, `tables` |
| `Env::failure()` | the latched cause, if any |
| `Env::max_key_len() / max_value_len(klen)` | the entry limits of this page size |
| `ReadTxn` (both txn kinds) | `open_table(name) -> Option<Table>`, `get`, `cursor`, `txn_id`, `table_stat -> TableStat { entries, height }` |
| `RwTxn::create_table(name)` | open or create |
| `RwTxn::drop_table(Table) / clear_table(&Table)` | free the tree's pages (both are copy-on-write: readers keep the old tree) |
| `RwTxn::put / delete -> bool` | point writes |
| `RwTxn::append(table, key, value)` | put a new last entry: `key` must sort strictly after the table's last key (an empty table takes any key), else `AppendOutOfOrder`, refused before anything changes (like `MDBX_APPEND`). An append into the table's cached rightmost leaf neither descends nor searches; the cache holds the leaf of the table's last put or append in this transaction (after an append split, the new right leaf) until a page is allocated or freed, and any other append descends. |
| `RwTxn::cursor_mut -> RwCursor` | cursor moves plus `put`, `append` (as `RwTxn::append`) and `delete_current`. A put or append leaves the cursor at its entry: placed from the path it wrote (after an append split, from a second descent that refills the table's path cache), or re-sought by key after any other split. A delete re-seeks by key, and `next` then returns the follower (MDBX behaviour). |
| `RwTxn::commit() -> txn id / abort()` | finish |
| `Cursor` | `first`, `last`, `next`, `prev`, `current`, `seek` (first `>= key`), `seek_exact` |
| `Table` (`name()`) | a handle holding the table's id and name, checked against the catalog on every use: a dropped or re-created table is `StaleTable`, and one created after the reading state is `TableNotFound` |

**Cursor states:** Unset, At, AfterEnd, BeforeStart.

- `next` from Unset or BeforeStart goes to the first entry.
- `prev` from Unset or AfterEnd goes to the last entry.
- A `seek` miss leaves the cursor AfterEnd.
- A `seek_exact` miss leaves it Unset.
- On an empty table, `first` leaves it AfterEnd and `last` BeforeStart.
- An error leaves it Unset.

A write transaction's read cursor sees its own uncommitted changes.

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
| `TableNotFound(String)` / `StaleTable(String)` / `InvalidTableName` | table errors (no latch) |
| `NotTndbB` | a non-empty file without the magic in either meta slot |
| `UnsupportedVersion(u32)` | another format version in a meta slot |
| `InvalidPageSize(u32)` | `EnvOptions::page_size` is not a power of two in 4K..=64K |
| `Locked` | another handle (in this process or another) holds the file |

---

## Platform notes

- **Unix only** (`compile_error!` elsewhere): the engine uses mmap, msync, flock, and
  fallocate or `F_PREALLOCATE`.
- **macOS: msync and fsync are not power-loss barriers.** Neither flushes the drive's write cache.
  Only `F_FULLFSYNC` does, at 10–20 ms each, and the crate deliberately never issues it. Never
  call `File::sync_all`; use `sys::fsync_file` and `fdatasync_file`. A process or kernel crash
  loses nothing committed durably, but a power loss can, exactly as with MDBX's `Durable` mode on
  macOS. Production durability is a Linux property, where `msync(MS_SYNC)` takes the file
  system's fsync path.
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

---

## Limits and non-goals

M1 is the crash-safe core. These limits are known; the first four are M2 items:

- **No overflow pages:** an entry (cell plus slot) is at most `U / 4`, about 1 KiB at 4 KiB pages.
  Larger values are `ValueTooLarge`.
- **No leaf merging:** a delete that empties a leaf unlinks it (collapsing emptied parents and the
  root), but under-full leaves are not merged. Random deletes can leave pages part full.
- **No dup-sort tables:** leaf cell flags are reserved for this.
- **No multi-process access or read-only open.**
- **No reth `db-api` adapter.** That is planned as a separate crate on top of this native byte
  API.
- No sibling links. A cursor crosses leaves through their parents (copy-on-write cannot keep
  sibling links).
- The height is capped at 48, a corruption tripwire far above any real height.
- Table names are 1..=255 bytes of UTF-8.

---

## Intentional design decisions: please do not flag these

1. **No `F_FULLFSYNC` on macOS** (see [Platform notes](#platform-notes)). The commit path is
   msync, which is no stronger. A full drive flush per sync would cost 10–20 ms and buy nothing
   the commits keep.
2. **`unsafe` in five places.** Each block carries a `SAFETY:` comment.
   - `file.rs`: the mapping (`map_mut`), `msync`, and the `from_raw_parts` page slices. These are
     sound under the single-writer model: the base is stable inside the reservation, readers see
     only published, immutable pages, and retired mappings outlive every view of them.
   - `sys.rs`: the libc calls `fsync`, `fallocate`, `fcntl(F_PREALLOCATE)`, `sysconf` and
     `flock`, each on a live file descriptor (`sysconf` takes none).
   - `crc.rs`: the aarch64 PMULL kernel (`pmull::update`, a copy of `tn-storage`'s), called only
     after the CPU feature is detected at runtime.
   - `page::prefetch`: a one-instruction prefetch hint (`prfm` or `prefetcht0`) that the binary
     searches issue for the cells of their next probe. A hint never faults or changes memory, and
     its address is a byte of the page.
   - the test-only `disk_model.rs`: a copy out of a live mapping into the model's own buffer.
3. **Reads do not recompute page CRCs.** The CRC is checked where it matters: before
   copy-on-write, on the roots at open, on free-list leaves before reuse, and in `Env::check`.
   The per-fetch checks (range, non-zero trailer, header pgno, txn) catch misdirected and reused
   pages.
4. **`child_txn` and `root_txn` hold only the low 32 bits of a txn.** They are a damage detector:
   a sound tree never points at a reused page (the reuse gate guarantees it), and a pointer
   redirected to a stale copy goes unnoticed only if that copy's txn matches modulo 2^32.
5. **The file is longer than `page_count × P`.** That is growth slack and harmless; the meta's
   page count is authoritative.
6. **A torn or invalid meta slot is not repaired at open.** Open never writes. The slot counts as
   the lowest and is the next one a commit overwrites.
7. **Leftover pages of a consumed free record wait two more commits.** They are re-listed under
   the new txn, as in LMDB, instead of tracking per-page ages.
8. **`Env::sync` blocks while a write transaction is open.** It needs the writer state, so calling
   it while holding an `RwTxn` deadlocks. This is documented, not guarded.
9. **No leaf merging in M1** (see above). Pages are reused through the free list, so there is no
   compaction pass.
10. **Each dirty range is its own msync**, so a crash test can stop the commit between ranges.
    Past 16 runs of adjacent pages, one `fdatasync` of the file replaces them. That bounds the
    syscalls, and it is much cheaper for a commit scattered over the file (see the benchmark).
    `fdatasync` also writes any other dirty page of the file. At a commit, that can only be a page
    of an aborted transaction, which is a free page, so writing it is harmless.
11. **The writer trusts its own pages by provenance** (`write::descend_write`):
    `trusted(child) = trusted(parent) && is_private(child)`, and the root is trusted only when its
    holder's flag says this txn's writer wrote it. Trusted pages skip the per-fetch checks,
    `verify_published` and the separator-range check. `is_private` alone is not enough, and the
    rules that close the gap are deliberate:
    - an untrusted parent (or root handle) naming a private page is `Corrupt`: a sound tree never
      has that edge;
    - when the writer copies a published branch it did not stamp (where `verify_published` runs),
      every cell whose `child_txn` equals this txn's low 32 bits must name a page whose header
      matches the cell and whose full txn is below this one (`check_cells_naming_this_txn`).
      Otherwise a forged cell naming a free page that this txn later allocates for another tree
      would pass the copy and let a later put write into that tree's page. A child whose txn
      wrapped 2^32 back passes; a free page's stale bytes and a page already private fail.

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
    or frees a page: a copy allocates and frees, a split allocates, an unlink, collapse, clear
    or drop frees; and the epoch also moves at commit (`stamp_private`), `begin_txn` and abort.
    An in-place insert, replace or delete changes only its leaf, never a separator, so it leaves
    the cache valid. A freed page is reusable at once by another table in the same txn, which is
    exactly the move the epoch catches. The range is the nearest separator either side on the
    path, which equals the intersection of all of them on any path the descent accepts (debug
    builds assert it). The catalog and the free list never use a cache.

---

## Tests and benchmarks

The default tests are small and fast, because CI runs nextest in debug. The heavy variants are
`#[ignore]`d and run with `--release`.

| suite | covers |
|---|---|
| `page.rs` tests | leaf and branch codecs against proptest models; the parsed views (`View`, `LeafView`, `BranchView`) against an independent model; splits at all five page sizes; `SplitBuf` splits byte-identical to a reference split (leaves and branches, the pending cell anywhere, a quarter of the cases at the append place); append fill; exact limits; a full page left byte-identical; 13 damage kinds; never panicking on fuzzed bytes |
| `tree/write.rs`, `tree/read.rs` | copy-on-write put and delete against a `BTreeMap` model over random batches (with a coverage gate on split and delete shapes); a failed allocation leaves the tree identical; split reservation for long separators; append fill; collapse and unlink; `clear` and the free walk refuse damaged or stale pointers; fetch checks, stale copies, redirected children and misrouted pointers are `Corrupt`; provenance trust: a trusted path skips the range checks, a private page named by a page this writer did not write (a stamped forged cell, an untrusted root handle, a collapse) is refused, as is a copied cell naming a page this txn allocates later, while a child whose txn wrapped stays live |
| `tree/cursor/tests.rs` | random moves against a sorted model on the detached cursor and the public `Cursor`, over many tree shapes and page sizes, committed and uncommitted; the ends of empty and single-entry trees; leaf crossings; seeks; damage under a cursor latches; `RwCursor` writes against a model |
| `meta.rs`, `catalog.rs`, `latch.rs`, `alloc.rs`, `file.rs`, `sys.rs`, `crc.rs`, `page_set.rs`, `disk_model.rs` | codecs, slot choice, the latch's first-cause rule, allocator abort, growth and remap, sync wrappers, the power-loss model |
| `env/tests.rs` | open steps (create at every page size, leftovers, the lock, a damaged newest meta, damaged roots, short and foreign files, the open fsync), commit and NoSync durability under power loss, a commit of no change, abort, the writer hand-off, readers across commits, range coalescing, the latch (failed msync, damage, oversized entries do not latch), table handles |
| `freelist/tests.rs` | the record codec; reuse after two durable commits or two NoSync syncs, held back by a live reader; damaged records never handed out; abort returns the records taken; dropped and cleared tables' pages reused; steady churn keeps the file bounded; the save settles within three rounds, including a record count that shrinks mid-save; `check` reports damage, leaks and double uses; exact accounting in a random workload |
| `tests/crash.rs` | every crash point (`CrashAt`) × {process crash, strict power loss, random power loss with three seeds} × {`Durable`, `NoSync`}: the reopen recovers exactly the allowed state, which passes `check` and survives a crash right after the reopen; a commit that grew the file survives a power loss; the older state stays recoverable until its meta is overwritten |
| `tests/damage.rs` | each kind of damage at rest fails the open closed (the file unchanged) or, once opened, is met by reads, copy-on-write, free-list loads and `check` as `Corrupt`, which latches; nothing panics |
| `tests/multi_model.rs` | a random multi-table workload with crashes against a model |
| `tests/path_cache.rs` | random multi-table workloads (puts, deletes, clears, drops, commits, aborts, readers held) on a cached and an uncached environment: the same outcomes and byte-identical files after every commit; a cached leaf reused by another table in the same txn is not written; an aborted transaction's cache never reaches the next |
| `tests/append.rs` | `RwTxn::append` and `RwCursor::put`/`append` against `RwTxn::put`: byte-identical files and a model across splits, aborts, deletes and stale path caches; an append out of order is refused without a latch or a change; a cursor put lands on its entry (`current`, `next`, `prev`) |
| `tests/concurrency.rs` | reader threads rescan pinned snapshots and take fresh ones while a writer replaces, deletes and clears (pages freed and reused): no read sees damage or a torn state |

```text
cargo nextest run -p tndb-b                                    # default suite (debug)
cargo test --release -p tndb-b -- --ignored --skip tndb_b_vs_mdbx  # heavy variants (~3 min)
cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1  # benchmark
```

### Benchmark: tndb-b vs MDBX

`bench.rs` (`tndb_b_vs_mdbx`) runs each column in a fresh temp dir on the same seeded streams:
1M entries per bulk table, 8-byte BE sequential keys and 32-byte random keys, 100-byte values.

```text
cargo test --release -p tndb-b tndb_b_vs_mdbx -- --ignored --nocapture --test-threads 1
```

Environment variables select what runs (the `bench.rs` module docs have the details):

- `TNDB_B_BENCH_N`: entries per bulk table (default 1,000,000).
- `TNDB_B_BENCH_ENGINES`: the columns, comma-separated (default all): `t4d`, `t16d`, `t4n`,
  `md`, `md4`, `mn`.
- `TNDB_B_BENCH_ROWS`: the row groups, comma-separated (default all): `seq`, `rand`, `get-hit`,
  `get-miss`, `scan-fwd`, `scan-rev`, `seek`, `small`, `mixed`, `clear`, `append` (the API rows),
  and the aliases `bulk`, `get` and `scan`.
- `TNDB_B_BENCH_REPEAT=k`: each selected read row runs k times and reports the median.

The columns:

- `t4d` and `t16d`: tndb-b `Durable` with 4 KiB and 16 KiB pages; `t4n`: 4 KiB `NoSync`.
- `md`: MDBX (`reth-libmdbx`) `Durable`, opened as `tn-storage` opens it: a `write_map`
  environment with the OS page size and a generous geometry. `md4`: the same with 4 KiB pages.
  `mn`: MDBX `SafeNoSync`. On x86_64 the OS page is 4 KiB, so `md` and `md4` are the same
  configuration there.

**Like-for-like pairs:** `t4d` with `md4` everywhere, and `t16d` with `md` on Apple Silicon,
where the OS page is 16 KiB. `t4n` has no MDBX twin: on macOS `SafeNoSync` still msyncs every
commit (see below).

A bulk row's "put loop" runs from `begin` to just before `commit`, and its "commit" is the rest.
The API rows time the put loop into a fresh environment: tndb-b's `RwTxn::append`,
`RwCursor::put` and `RwCursor::append` against MDBX's `put` with `WriteFlags::APPEND`,
`Cursor::put`, and `Cursor::put` with `WriteFlags::APPEND`.

The "fdatasync" rows rerun the small-commit workload with commit step 4 done by one `fdatasync`
of the whole file instead of one msync per range. A test-only switch (`Writer::data_by_fdatasync`)
selects this for every commit. Production takes the fdatasync path only past 16 runs of adjacent
dirty pages, which a 3-put commit never reaches. The two modes alternate in blocks of 100
transactions (A B B A order) on the same table, so neither gains from running second.

**Run:** Apple M1 Max, 64 GiB RAM, internal SSD (APFS), macOS (Darwin 24.6), 2026-10-20. Each
cell is the median of three full runs (each about 45 s). The Mac had background load, and the
MDBX cells moved by up to 20% between runs (`md` random insert: 779–894 ms), so a difference
under about 10% is noise. Linux numbers are still to come.

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

**What changed in the latest round.** Every guarantee and damage check is kept; the checks moved
to where they are needed.

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

**Where tndb-b loses, and why:**

- **The commit of a bulk load costs more:** 91 ms against MDBX's 61–66 ms after the random
  insert, and 35–37 ms against 30–32 ms after the sequential one. A tndb-b commit stamps a CRC on
  every page it wrote, msyncs each dirty range, and fsyncs the file's growth; in a profile of the
  sequential commit, msync took about 77%, CRCs 15% and the growth fsync 8%.
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

**Where tndb-b wins:**

- **Sequential loads:** the put loop is 2.3× faster than MDBX's at 16K (47 vs 111 ms) and 1.9×
  at 4K (60 vs 115 ms), because almost every put lands in the cached leaf. A plain put even beats
  MDBX's `APPEND` put (65–69 ms). `RwTxn::append` takes 48–54 ms, and `RwCursor::append` is a
  little faster than MDBX's cursor `APPEND` (51.5 vs 57.9 ms at 16K, 57.8 vs 61.6 ms at 4K).
- **Small durable commits are about 10× cheaper** (130 us vs 1.3 ms median at 16K, 114 us vs 1.3
  ms at 4K). tndb-b msyncs only the dirty ranges plus one meta page. MDBX msyncs the whole used
  span of the map on every commit (about 312 MB here), and on macOS that walk is the cost.
- Under `SafeNoSync`, MDBX on macOS still issues an `MS_ASYNC` msync of the whole span every commit
  (`MDBX_MMAP_NEEDS_JOLT` is set off Linux). That makes it slower than its own Durable mode here.
- **Scans** are 10–17% faster than MDBX's at 16K (27.1 vs 30.2 ms forward, 24.9 vs 30.0 ms
  reverse). At 4K the forward scan beats `md4` (46.6 vs 55.8 ms), and the reverse one is level.
- **`clear_table`** frees a 1M-entry tree 2–4× faster (569 us vs 2.2 ms at 16K).
- **The durable mixed workload** matches MDBX or beats it (2.63 vs 3.27 s at 16K, 3.34 vs 4.26 s
  at 4K). MDBX's own cell moved most between runs here (2.72–3.33 s at 16K), so read this row as
  parity.
- **The mixed workload without syncs** is about 6× faster than MDBX SafeNoSync, on macOS only,
  and mostly for the reason above: MDBX msyncs the whole span asynchronously on every commit,
  while a tndb-b NoSync commit does no I/O and its deferred `Env::sync` is not timed. On Linux
  SafeNoSync does not msync per commit, so this is not an engine win.
- **File sizes match:** the used pages within 2%, the file lengths within 3%.

**msync per range vs one fdatasync.** On macOS one fdatasync of the whole file is about 35%
cheaper per small commit than the per-range msyncs (75–84 us vs 114–130 us median). Neither is a
drive-cache flush on macOS, so this measures only the kernel paths.

For a large scattered commit the gap is far wider. On macOS an msync writes its range's dirty
pages one cluster at a time, so its cost follows the dirty pages, not the span. In the mixed row
at 200k entries, a 1000-update commit dirtied about 835 pages at 16K, in about 620 runs of
adjacent pages. The 64-page merge gap folded them into 2 ranges spanning the whole file. Their
msyncs took about 14.6 ms, and one fdatasync took 5.3 ms. Syncing the runs one by one, or with a
smaller merge gap, cost the same as the merged span. At 4K it was 16.5 ms against 3.7 ms. This is
why step 4 switches to one `fdatasync` past 16 runs of adjacent pages.

On Linux the difference should matter more. Each `msync(MS_SYNC)` range runs the file system's
fsync path, which includes a device cache flush, so a commit with k dirty ranges pays up to k+1
flushes (the ranges plus the meta). An `fdatasync` of the file plus the meta msync pays two.
MDBX also pays about two (one whole-span msync plus the meta). A scattered commit already takes
the fdatasync path.

**Linux numbers are still to be taken.** They decide whether the 16-run threshold should drop
(every commit of more than one range syncing by `fdatasync`), or whether step 4 should use
`sync_file_range` plus one `fdatasync`. Run the same command on the target hardware, with a
non-tmpfs `TMPDIR` scoped to that command.
