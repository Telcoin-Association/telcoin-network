# `tndb` — the pack-file key/value database

`tndb` is a `Database` (the `tn-types` key/value trait used by the node's consensus stores) built
on this crate's own storage primitives instead of MDBX or redb:

- each **table** is an append-only **data log** (an `archive::pack::Pack`, the write-ahead log and
  the source of truth),
- plus a copy-on-write **B+tree index** over it ([`archive::btree_index`](../archive/btree_index/README.md))
  mapping each key to its record's position,
- read through published, immutable **snapshots** with **no lock**: readers never block writers,
  and writers never block readers.

On top of that it adds crash recovery (rebuild the index by replaying the logs to the last commit),
physical clears (a cleared table's data leaves the disk), background compaction (overwritten and
removed records leave the disk too), and two key modes (store the key, or derive it from the
value).

> **Status.** tndb is not yet the node's production database: `open_db` still builds
> `CompositeDatabase<MdbxDatabase>`. tndb is complete enough to be benchmarked against MDBX through
> the same `Database` trait (raw and behind `LayeredDatabase`; see [Benchmarks](#benchmarks)), and
> is the intended replacement.

| file | contents |
|---|---|
| `mod.rs` | module root; re-exports `TnDatabase` and `CompactionConfig` |
| `database.rs` | the typed layer: `TnDatabase`, `TnDbTx`, `TnDbTxMut`, the `Database` impl, key modes |
| `table.rs` | one table (`TnTable`): logs, index, snapshots, commits, recovery, generations, clear |
| `table/compact.rs` | compaction: the background rewrite into the next generation, and its catch-up |
| `layout.rs` | the on-disk layout: the `meta` file, generation, spare and compaction directories, `fsync` helpers |

---

## Contents

1. [Quick start](#quick-start)
2. [Architecture](#architecture)
3. [On-disk layout](#on-disk-layout)
4. [Key modes](#key-modes)
5. [Reads, writes and transactions](#reads-writes-and-transactions)
6. [Commits and durability](#commits-and-durability)
7. [Commit modes: sync and group](#commit-modes-sync-and-group)
8. [Crash recovery](#crash-recovery)
9. [Clearing a table: generations and spares](#clearing-a-table-generations-and-spares)
10. [Compaction](#compaction)
11. [Concurrency and memory safety](#concurrency-and-memory-safety)
12. [API reference](#api-reference)
13. [Limitations and future work](#limitations-and-future-work)
14. [Tests](#tests)
15. [Benchmarks](#benchmarks)

---

## Quick start

```rust
use tn_storage::tndb::TnDatabase;
use tn_types::{Database as _, DbTxMut as _};

// `Votes` and `Certificates` stand for any `Table` types.
let db = TnDatabase::open("/path/to/tndb")?;

// A keyed table: every row's key is stored in its log (the default).
db.open_table::<Votes>()?;

// A derived-key table: the log stores only values; on a rebuild each key is recomputed from its
// value. Every row's key must equal `key_of(&value)`.
db.open_table_with_key::<Certificates>(|cert| cert.digest())?;

db.insert::<Votes>(&author, &vote)?;            // autocommit: durable and visible on return
let vote = db.get::<Votes>(&author)?;

let mut txn = db.write_txn()?;                  // several writes, one commit per table
txn.insert::<Certificates>(&digest, &cert)?;
txn.remove::<Votes>(&old_author)?;
txn.commit()?;                                  // durable, then visible to readers

for (k, v) in db.iter::<Certificates>() { /* lazy, key-ordered, lock-free */ }
db.clear_table::<Votes>()?;                     // empty, and the data is deleted from disk
```

Wrapping it in the in-memory layer works as with any backend:
`LayeredDatabase::open(TnDatabase::open(path)?, true)`. Open any derived-key table with
`open_table_with_key` on the `TnDatabase` *before* the wrapper opens it; the wrapper's later
`open_table` keeps the table as opened.

---

## Architecture

```text
TnDatabase (database.rs)                    typed layer: encode keys/values, route by table name
  │  ArcSwap<HashMap<&str, TableStore>>     table map, read lock-free (read-copy-update on open)
  ▼
TnTable (table.rs)                          one table; a cheap Clone handle over Arc<Inner>
  ├─ Mutex<Writer>                          the single writer: current generation's files
  │    ├─ data log      Pack<Vec<u8>>       puts + commit records (the WAL, source of truth)
  │    ├─ removal log   Pack<Vec<u8>>       removed keys
  │    ├─ BtreeIndex                         key → data-log position (copy-on-write, derived)
  │    ├─ spare thread                       the next generation, prepared in the background
  │    └─ compaction thread                  the live rows, rewritten into the next generation
  └─ ArcSwap<Published>                      what readers see: an IndexSnapshot + the log's MapView
```

- **Encoding.** Keys go through `encode_key` (binary-sortable, so byte order is key order) and
  values through `encode` (BCS). A table's key size is the encoded key length
  (`encode_key(key).len()`), fixed per table: `size_of::<T::Key>()` would be wrong (for example
  `AuthorityIdentifier` is an `Arc<[u8; 32]>`, 8 bytes in memory but 32 encoded).
- **No allocation per op.** Writes encode into the table's reusable `EncodeBufs` (behind a
  per-table mutex that only another writer of the same table could contend). Point reads encode
  their key into a per-thread buffer and decode straight from the mapped log.
- **Snapshot isolation.** A write changes the table's working state at once, but readers see it only
  after the next flush (commit) publishes a new `Published` snapshot. A write transaction's own
  `get` reads the working state.

---

## On-disk layout

```text
<root>/                                 TnDatabase::open(root)
  <TableName>/                          one directory per table (Table::NAME)
    LOCK                                flock'd by the table's one open writer
    meta                                key mode + encoded key size (16 bytes, CRC'd)
    gen-<N>/                            the current generation (exactly one after a clean open)
      data                              data log (Pack, uncompressed) ── + clean-close seal
      removed                           removal log (Pack, uncompressed) ── + clean-close seal
      btx/index.btx                     B+tree index (derived; created at the first insert)
    spare-<N+1>-<pid>-<id>/             the next generation, prepared in the background
    compact-<N+1>-<pid>-<id>/           a compaction's new generation, being built
```

### `meta`

Written atomically (temp file, `fsync`, rename, directory `fsync`) when the table is created, and
once more when its first row fixes the key size.

| bytes | field |
|---|---|
| `0..8` | magic `b"TNDBMETA"` |
| `8` | format version (`1`) |
| `9` | key mode: `0` keyed, `1` derived |
| `10..12` | encoded key size, `u16` LE (`0` until the first insert) |
| `12..16` | CRC32 (LE) of bytes `0..12` |

A corrupt `meta` fails the open. A table with generation data but no `meta` also fails the open.

### Data log records

Every record sits in the pack's frame `[u32 len LE | payload | u32 crc32 LE]` (the CRC covers
`len ‖ payload`) after the pack's 28-byte header. The payload carries no type tag; what it holds is
fixed per table by its key mode:

| payload | meaning |
|---|---|
| `key ‖ value` | a put in a **keyed** table (`key` is exactly the table's key size) |
| `value` | a put in a **derived-key** table (a derived table never stores an empty value) |
| *empty* | a **commit record**: every put before it (since the previous commit) is committed |

Overwrites append a new put; the index points at the newest. Reads find the value at
`payload[key_size..]` (keyed) or `payload` (derived).

### Removal log records

`key ‖ data_len (u64 LE)`, where `data_len` is the data log's length when the key was removed: every
put of that key **below** `data_len` is gone. A removal is logged only when the key was present (a
miss costs nothing). The removal log is read only by recovery and by compaction's catch-up.

### Files and options

| file | mapping | initial size | notes |
|---|---|---|---|
| `data` | 1 GiB reserved range (never moves while open) | 1 MiB, geometric growth | lock-free readers read it in place |
| `removed` | default | 64 KiB | never read on the hot path |
| `btx/index.btx` | 1 GiB reserved range | 2 pages | see the btree_index README |

---

## Key modes

| | keyed (default) | derived-key |
|---|---|---|
| open with | `Database::open_table::<T>()` | `TnDatabase::open_table_with_key::<T>(key_of)` |
| data record | `key ‖ value` | `value` |
| rebuild gets keys from | the record | `encode_key(key_of(&decode(value)))` |
| use when | any table | the key is a function of the value (e.g. a digest of it) |

- **The mode is fixed when the table is created** (in `meta`). Opening it in the other mode is an
  error.
- **`key_of` must hold for every row:** the key passed to `insert` must equal `key_of(&value)`, or a
  rebuild would file the row under a different key. A rebuild is not the only reader of `key_of`:
  every compaction derives the keys of the rows written while it ran, so a violation re-keys rows
  at an ordinary switch, not only after a crash. Debug builds check it on every insert
  (`debug_assert`); release builds do not, to keep the hot path free.
- **Opens are idempotent.** Re-opening an open table keeps it. `open_table` after
  `open_table_with_key` keeps the derived table (so a wrapper such as `LayeredDatabase` can open
  it again); `open_table_with_key` on a table already open as keyed is an error. Opens are
  serialized, so a table is never opened twice.

---

## Reads, writes and transactions

| `Database` method | behaviour |
|---|---|
| `get`, `contains_key` | published snapshot, lock-free; `get` decodes straight from the mapped log; a damaged index is an error, never "absent" |
| `iter`, `reverse_iter`, `skip_to` | lazy `DBIter` owning a snapshot and a B+tree cursor; holds no lock, so the table can be written (and committed) during a scan; the scan keeps its starting snapshot. `DBIter` cannot carry an error, so a scan ended by damage logs `tracing::error!` before it stops |
| `record_prior_to`, `last_record` | a single seek in the snapshot (an error ends it the same way, logged) |
| `is_empty` | snapshot key count (an absent table reads as not empty) |
| `insert`, `remove` | **autocommit**: write, then commit (durable) and publish before returning |
| `clear_table` | switch to an empty generation (durable on return), then publish |
| `read_txn` → `TnDbTx` | reads go straight to the published snapshots (`multi_get` uses the trait default) |
| `write_txn` → `TnDbTxMut` | see below |
| `persist` | `CommitMode::Sync`: nothing to wait for (every commit is durable when it returns). `CommitMode::Group`: waits until every earlier write, in any table, is committed; an error once any write or commit has failed |
| `sync_persist` | trait default (no-op): writes are readable as soon as they return, in both modes |
| `compact` | starts a background compaction of every table with dead records (see [Compaction](#compaction)); returns at once |

Every op that returns a `Result` on a table that was never opened is an **error** ("table … is not
open"), as with MDBX: a write is never silently dropped. Each `insert` and `remove` is all or
nothing: if its index step fails, its log record is taken back out, so a later commit cannot make
durable a write the caller saw fail.

**Write transactions are loose**, as in `MemDatabase`:

- writes apply to each table's working state immediately;
- the transaction's own `get` sees them;
- `commit` commits (durably) and publishes each table it wrote, one table after another;
- there is **no rollback**: a dropped transaction's writes stay in the working state and are
  committed by the table's next commit, or by a clean close;
- a table has one working state, so concurrent write transactions on the same table see, and
  commit, each other's writes;
- there is **no cross-table atomicity**: a crash during `commit` of a multi-table transaction can
  keep one table's part and not another's.

---

## Commits and durability

A commit (`TnTable::flush`, run by `commit` and by every autocommit op) is skipped when the table
has nothing uncommitted; otherwise:

1. If the transaction removed keys, `msync` the **removal log**. Removals must be durable before
   the commit record that makes them count.
2. Append the **commit record** (an empty record) to the data log.
3. `msync` the **data log** (plus a one-time `fsync` after a growth, so the new size is durable).
4. Stamp both logs' **commit markers** (best-effort, unsynced; a floor recovery uses, below), and
   record the covered log length in the index header (in memory).
5. If a compaction's thread has finished, switch to its generation (see [Compaction](#compaction)).
6. Publish a new snapshot to readers, then start the automatic compaction if it is due.

The B+tree index is **not** synced on commit; it is derived, synced at a clean close, and rebuilt
after a crash.

**Platform note.** tndb's commits are `msync`, and every other sync (directory entries, `meta`,
growth sizes, the writable-reopen sentinel, the clean-close seal) is a plain `fsync(2)` (the
`archive::data_file::fsync_file` / `fdatasync_file` helpers, which `layout::sync_file` / `sync_dir`
use). On Linux those are exactly `File::sync_all` / `sync_data`. On macOS std's versions would be
`F_FULLFSYNC`, a flush of the whole drive's write cache that stalls every other write on the drive
for 10–20 ms, and tndb's commits don't issue one either. So on macOS tndb (like MDBX's `Durable`
mode with `write_map`) is not a full power-loss barrier, and its numbers there understate the
production (Linux) sync cost.

**A clean close** commits anything uncommitted through the ordinary commit (so the removal log is
synced before the commit record), for example a dropped transaction's writes. A close that cannot
commit leaves the index marked stale, so the next open rebuilds and drops the uncommitted records.
Otherwise the close records the final log length in the index header, syncs the index, and seals
all three files, and the next open needs no rebuild.

---

## Commit modes: sync and group

`TnDatabase::open_with(root, TnDbOptions { commit, compaction })` picks how writes are committed.

- **`CommitMode::Sync`** (the default): each autocommit op, and each transaction's commit, commits
  its tables before it returns (above). `persist` has nothing to wait for. Simple, and the lowest
  latency for one writer, but every write waits on the disk on the caller's thread: on a tokio
  worker, that blocks the runtime for each sync.
- **`CommitMode::Group`** (`tndb/commit.rs`): built for production's async callers. A write
  applies to its table and is **published at once** (readable from any thread on return), without
  a sync, and is numbered from a database-wide sequence. One `tndb-commit` thread commits every
  table written since its last round: one commit per table per round, however many writes landed
  (a round's worth of votes costs one sync, not N). `persist` is the durability barrier: it
  resolves once every write numbered before the call is committed, **in every table** (a
  whole-database, FIFO barrier), and fails from the first failed write or commit on (a latch,
  never cleared), as the externalization points (#975) require.

How the committer keeps writers off the disk, and keeps the WAL invariants (`TnTable::group_commit`):

1. Under the table's lock: capture the removal log's unsynced range (if it has removals).
2. Unlocked: `msync` it (`MmapDataFile::sync_ticket`; the sync runs through the mapping's shared
   view, so writers keep appending past it).
3. Under the lock: sync any removal logged meanwhile (rare, small), so removals are durable before
   the commit record that covers them; append the commit record; capture the data log's range.
4. Unlocked: `msync` the data log (and `fsync` once if it grew).
5. Under the lock: record the synced end and the commit markers; switch to a finished compaction,
   or start one, when nothing is pending.

A writer holds the lock only for its append and index update, never across a sync.

**The durable watermark.** A round reads its target number *before* it takes the dirty tables, and
a writer marks its table dirty *before* it takes its number, so every write numbered at or below
the target is in that round (or an earlier one). The watermark advances to the target, held back
by any table still pending. A table written by an **open write transaction** is left uncommitted
until the transaction ends (it is held back, in every later round, until then), so a transaction
is never durable in part. A write committed early (numbered after the target, but in a table the
round took) triggers one more round, so the watermark reaches it without waiting for a later write.

**Transactions in group mode** apply their writes at once (the transaction's own `get` sees them)
and publish them when the transaction ends, by `commit` or by a drop (as `LayeredDatabase`'s
dropped transactions do: kept and committed, no rollback). A bare write to a table with an open
transaction is published at once but committed with the transaction.

**Clears** stay synchronous (a generation switch, durable on return), then are published.

**Crash semantics** match a write-behind layer's: a write is readable before it is durable, and a
crash keeps every write made before the last successful `persist` (and possibly some after it),
always whole transactions (each table's committed state is a prefix of its writes, ending at a
transaction boundary).

**Compaction** in group mode is started at commit rounds, and the committer also asks every table
to compact at start and daily (what `LayeredDatabase`'s writer did for the wrapped database).

Why group mode rather than a wrapper: `CompositeDatabase` exists to split MDBX's single-writer
environments (and their memory modes), which tndb's per-table files and writers do not need.
`LayeredDatabase`'s write queue makes every `persist` wait behind every queued commit (each bare
write is its own commit, and a transaction commits only once every overlapping one ends), and its
memory layer doubles RAM and makes startup reload every row; without the queue it would block
callers on every sync, as `Sync` mode does. Group commit gives non-blocking writes and a barrier of
about one sync, with no copy of the data.

---

## Crash recovery

### When the index is rebuilt (at open, writable)

Any of:

- the data log, removal log or index was not closed cleanly (`opened_unclean()`, no clean-close
  seal);
- the index fails to open or has a bad header or geometry (an `IO` error is surfaced instead);
- the index's recorded `data_file_length` differs from the data log's length (the index lags);
- the data log has records but there is no index.

### What the rebuild does (`Writer::recover`, `replay`)

1. **Validate and replay in memory, changing nothing.** Both logs are read through cloned file
   handles (`PackIter::next_raw`, CRC-checked payloads; never through the shared mapping).
   - **Data log:** each put becomes *pending*; at each commit record the pending puts become live
     (`key → position`, newest wins). The key comes from the record (keyed) or `key_fn(value)`
     (derived).
   - **Removal log:** a removal `(key, data_len)` is committed only if `data_len` is at or below the
     **start of the last commit record**. A committed removal drops the key if its live position is
     below `data_len`. A put of the same key after the removal survives.
   - Puts after the last commit record, and uncommitted removals, are an **uncommitted
     transaction**: they are discarded. Log pages can reach disk through OS writeback before their
     commit, so without commit records a crash could apply half a transaction.
2. **Fail closed.** Any of these makes the open fail, with nothing changed:
   - a tear (bad frame) in a log that was **sealed** by a clean close: a sealed log must replay
     whole (it may end in uncommitted records, from a close that could not commit; those are
     dropped);
   - in an **unclean** log, a stop *below* the log's commit marker: that is damage to committed
     data, not a crash tail;
   - a malformed record (shorter than the key; a removal of the wrong size), a removal log out of
     order, or a key function failure.
3. **Only then** cut each unclean log back to its last committed record (`rewind_to`), rebuild the
   index with `rebuild_from` (sorted, so a sequential build), record the covered length, sync the
   index, and `mark_consistent()` all three files, so the next clean close seals them.

A failed recovery changes no log byte, so retrying reaches the same verdict. The replay holds the
live `key → position` map in memory: about (key size + 8 + map overhead) per live row, e.g. around
100 MB transiently for ~1.3M rows.

### What survives a crash

Every commit that returned (`commit`, or an autocommit op) survives, including its removals, and no
part of an uncommitted transaction does. A clear survives once it returns. Each table commits
separately (see transactions above).

---

## Clearing a table: generations and spares

A table's files live in a **generation** directory, `gen-<N>`. Clearing the table moves it to a new,
empty generation, and the old one's files are deleted. Stale data therefore leaves the disk, rather
than lingering as garbage in a log.

**Spares.** Creating files is the expensive part of a clear (file creation, preallocation, syncs).
So after every open and every clear, a background thread (`tndb-spare`) prepares the **next**
generation in a uniquely named `spare-<N+1>-<pid>-<id>/` directory. Its empty logs (and an empty
index, once the key size is known) are created and synced there. A spare is never mistaken for a
generation, because only `gen-*` directories are generations.

**`clear`** (under the writer lock):

1. Take the prepared spare (waiting for it if it is still being prepared). Rename it to
   `gen-<N+1>` and `fsync` the table directory. **This is the clear**: from here a crash reopens the
   table empty. If no spare is available, the generation is created in place, with full syncs.
2. Switch the writer to the new generation, and publish an empty snapshot at the next commit.
3. **Retire** the old generation. Its files are marked delete-on-drop (no seal) and moved into that
   generation's keepalive, which its snapshots hold.
4. Start the next spare. Before preparing it, that thread deletes the old `gen-<N>` directory
   (unlinking it; the space returns when the last mapping closes).
5. When the last snapshot of the old generation drops, its files are closed (unmapped) on a
   short-lived `tndb-reap` thread, off the path of whoever dropped it.

**Open** removes any leftover `spare-*` and `compact-*`, uses the newest `gen-*`, and deletes older
ones. More than one generation exists only after a crash during a clear or a compaction's switch;
the newest is current (a clear or a switch renames a complete generation into place, and a partly
created one opens as empty). A newest generation that does not open is an **error**: it is never
deleted and an older generation is never brought back in its place, since that would erase
committed data or resurrect cleared data. A new table starts at `gen-0`. Deleting older generations
and leftovers is best-effort (logged).

**A failed clear** that may have changed which generation the next open picks (the rename into
place succeeded but its directory sync failed, or a fallback creation failed part-way) **stops the
writer**: later writes and commits fail until a restart, instead of committing to a generation the
restart would discard.

**Close** waits for a spare still being prepared, then discards it, so no background thread writes
into a closed table's directory.

---

## Compaction

Overwrites and removes leave dead records in the data log (and removal records in the removal log)
until the table is cleared. Compaction rewrites a table's live rows into its next generation in the
background and switches to it; the old generation, dead records and all, is then deleted.

**When.**

- **Automatically**, at a commit, once the data log is at least `CompactionConfig::auto_min_bytes`
  (default 64 MiB) and at least half the generation's puts are dead (`dead ≥ live`). `dead` counts
  overwrites and successful removes since the generation started; a clean close keeps it in the
  index header (`owner_value`) and a crash rebuild recounts it. The check is O(1).
- **On request**: `Database::compact()` starts one for every table with dead puts and a log of at
  least 1 MiB, and returns at once (`LayeredDatabase` calls it at startup and daily).
- **Now**: `TnDatabase::compact_table_now::<T>()` compacts regardless and waits (tooling, tests).

`TnDatabase::open_with_compaction(root, config)` sets the trigger and pace; `auto_min_bytes: None`
turns the automatic trigger off.

**How** (`table/compact.rs`):

1. **Copy** (a `tndb-compact` thread, no lock). From the last published snapshot, walk the index in
   key order and append each live row's record, byte for byte, to a new data log in
   `compact-<N+1>-<pid>-<id>/`, indexing it in a new B+tree. Dead records are never reached. Key
   order fills the new tree densely (append splits) and makes a scan of the new log sequential. The
   snapshot is released once copied, so it pins no index pages during the catch-up; the old
   generation's keepalive is held for the whole run.
2. **Catch-up** (same thread, no lock). Commits keep landing in the old generation. At each publish
   the writer tells the thread how far the old logs are committed, and the thread replays both
   logs' committed tails through their lock-free views, merging puts and removals by position as
   recovery does. Removals go to the new removal log against the new data log's length, so the new
   generation replays on its own. Rounds repeat until one has at most 1 MiB to replay (at most 8).
3. **Sync** the new logs (removal log first) and the directory. That bulk sync can take a while for
   a large table, so one more catch-up round then replays what was committed during it, followed
   by a (small) sync of that.
4. **Switch** (the writer, at the first commit after the thread finishes, between that commit and
   its publish): replay the last tail (all of it committed by then), commit and sync it, check the
   new generation holds exactly the table's rows, rename the directory to `gen-<N+1>` and `fsync`
   the table directory. The old generation is retired as in a clear (its keepalive holds it for live
   snapshots; its directory is deleted on a `tndb-reap` thread), and the publish moves readers to
   the new one.

**Pacing.** The copy pauses between 1 MiB batches to hold `CompactionConfig::bytes_per_sec`
(default 64 MiB/s; 0 is unpaced), leaving the disk to commits. A pacing sleep checks for a
cancellation every 10 ms. The catch-up is not paced: each round replays what the writer committed
during the last, so it adds no more I/O than the writer's own, and a catch-up paced below the
writer's rate would fall further behind every round and never finish (a group-mode writer easily
outruns 64 MiB/s).

**Cost.** Readers are unaffected: they keep their snapshot and move to the new generation at the
next publish. The writer is delayed only by the switch: a replay of what was committed during the
thread's final small sync, an `msync`, a rename and a directory `fsync`. On the hot path,
compaction adds a counter per write and a few O(1) checks per commit.

**Disk space.** A compaction starts only when the filesystem has room for the new generation beside
the old one: at least the data log's length (the most its live rows can take) plus 128 MiB (the new
log's growth preallocation). Otherwise it is skipped and logged; a failed probe does not block it.

**Failures.** A compaction that fails (an I/O error, a failed CRC, a row-count mismatch at the
switch, no room, no thread) is dropped, its files deleted, and the table carries on in its old
generation. It is logged as an error when it read damaged committed data, else as a warning. The
automatic trigger then backs off (1 minute, doubling to 1 hour, reset by the next successful
switch), so a lasting cause does not restart a full compaction at every commit; requested and
immediate compactions are not held back.

**Crash safety.** Before the rename, a compaction is a `compact-*` directory, never a generation:
the next open deletes it, and the table is as it was. After the rename, the new generation is the
newest and holds every committed row (as the old one does), so the next open uses it (rebuilding it
from its own logs if unclean) and deletes the old one. A switch that fails after its rename (the
directory sync) stops the writer, as a failed clear does.

**Interplay.**

- A **clear** cancels a running compaction: its thread stops at its next check and deletes its
  directory, and is reaped (in the background) at a later commit, or joined at close. A compaction
  that had already finished is discarded at once, its copy deleted in the background.
- A **clean close** switches to a compaction that has finished, and stops one that has not.
- **Disk:** while a compaction runs, the table holds both generations (the old one whole, the new
  one's live rows).

---

## Concurrency and memory safety

- **Readers** load the table map and the table's current `Published` through `ArcSwap` (a
  per-thread slot, no shared refcount write, no lock). A point read is one index descent plus a
  slice of the mapped log.
- **Writers** serialize per table on the writer mutex; different tables are written in parallel.
- **One writer per table directory.** An open takes an exclusive `flock` on `<table>/LOCK` for the
  writer's life, so a second open of the table (another `TnDatabase` in this process, a reopen while
  an iterator from a dropped instance keeps the old writer alive, or another process) fails instead
  of becoming a second writer on the same logs. A crash releases it with the process.
- **Mappings outlive their readers.** A `MapView` does not own its mapping, so a file must stay
  open while any reader can reach it. Snapshots and scans hold `Arc`s that guarantee this:
  - the current generation's files live in the table's `Writer`, which a `TableScan` keeps alive;
  - a retired (cleared or compacted away) generation's files live in its `GenAlive` keepalive,
    which its snapshots, and a compaction thread reading it, hold.

  Neither file is ever truncated or unmapped under a live reader: lock-free readers cannot be
  counted, so the design never needs to count them.
- **The index never reuses a page a live snapshot can reach** (see the btree_index README).
- **Background threads** are a spare preparer per table (joined on clear and on close), a
  compactor while one runs (joined at the switch; cancelled by a clear; joined at close), and
  short-lived reapers (detached; they only close already-unlinked files or delete a retired
  generation's directory).

---

## API reference

| item | description |
|---|---|
| `TnDatabase::open(root)` | open or create a database rooted at `root` (tables are opened separately) |
| `TnDatabase::open_with_compaction(root, config)` | the same, with a `CompactionConfig` (trigger and pace) |
| `TnDatabase::open_with(root, TnDbOptions { compaction, commit })` | the same, choosing the `CommitMode` (`Sync` or `Group`; see [Commit modes](#commit-modes-sync-and-group)) |
| `TnDatabase::compact_table_now::<T>()` | compact table `T` now and wait for the switch |
| `Database::compact()` | start a background compaction of every table with dead records |
| `Database::open_table::<T>()` | open (or create) table `T` as keyed; idempotent |
| `TnDatabase::open_table_with_key::<T>(key_of)` | open (or create) table `T` as derived-key |
| `Database` / `DbTx` / `DbTxMut` | the full trait surface (see [Reads, writes and transactions](#reads-writes-and-transactions)) |
| `TnDbTx`, `TnDbTxMut` | the transaction types |

Crate-internal building blocks (`table.rs`, `table/compact.rs`, `layout.rs`): `TnTable`
(`open_with(dir, key_fn, config)`, `insert`, `remove`, `clear`, `flush`, `get_with`,
`get_working_with`, `contains`, `is_empty`, `scan`, `first_with`, `compact`, `compact_now`,
`finish_compaction`), `TableScan`, `ScanKind`, `KeyFn`, `Compaction`, `Builder`, `replay_delta`,
`TableMeta`, `KeyMode`, `gen_dir`, `spare_dir`, `compact_dir`, `list_gens`, `remove_meta_tmp`, `remove_spares`,
`sync_file`, `sync_dir`.

---

## Limitations and future work

- **A switch waits for a commit.** A finished compaction of a table that is not written again
  switches at its next commit or clean close.
- **Cross-table atomicity.** Each table commits separately (see transactions).
- **Recovery memory.** The replay builds the live key map in memory (a streaming or two-pass rebuild
  would bound it).
- **Regrowth after a clear.** The new generation's data log starts at 1 MiB and grows geometrically,
  with one size `fsync` per growth (about 7 for a 100 MB table). Preallocating the spare to the
  previous size was tried and rejected: the spare and the current generation would both hold that
  reservation (up to 2 × 128 MiB per table).
- **Removal cost.** A commit that includes removals syncs two files (the removal log, then the data
  log).
- **Production wiring.** `open_db` still builds the MDBX composite; moving the node to tndb (and
  choosing which tables are derived-key) is separate work.

---

## Tests

`cargo test -p tn-storage tndb` (release: add `--release`; the debug run also exercises the debug-only
key check):

- **`database.rs`:**
  - the shared `Database` suite;
  - reopen after a clean close (no rebuild; point reads, scans and seeks right after the reopen);
  - a corrupt index header is rebuilt;
  - crash rebuild of committed puts, overwrites and removes, with uncommitted writes discarded;
  - a derived-key table: crash rebuild, and the log holds values only;
  - key mode fixed at creation, and idempotent opens;
  - the debug key check (`#[should_panic]`);
  - clear deletes the old generation while a pre-clear scan keeps reading it;
  - torn data-log and removal-log tails cut back to the last commit;
  - fail-closed corruption, in a sealed log and below the commit marker;
  - randomized crash recovery against a model (puts, removes, clears, commits, crashes, clean
    closes);
  - `LayeredDatabase` over tndb after a crash;
  - ops on a table that isn't open are errors; a damaged index makes `contains_key` an error;
  - a second open of the same table is refused until the first closes;
  - a close that cannot commit drops its uncommitted writes and the table still opens;
  - `Database::compact` starts a compaction a later commit switches to; `compact_table_now`;
  - group commit: the shared suite; a write is readable at once and durable after `persist`
    (through a crash); `persist` covers every table; a failed commit latches every later
    `persist`; an open transaction holds its table back (a crash keeps none of it); a dropped
    transaction is published and committed; removals logged during the committer's unlocked sync
    are synced before the commit record; a write numbered during a round waits for a later round
    (gated rounds); a write committed early still reaches the watermark; concurrent writers keep
    their persisted writes; compaction; and a randomized crash model (after a crash the table is
    exactly one of the states since the last `persist`). Mutation checks: ignoring open
    transactions, ignoring held-back tables, reading the target after taking the dirty set, and
    dropping the extra round each fail at least one of them.
- **`table.rs`:**
  - byte-level table operations and scans;
  - snapshots under concurrent commits, and page reuse;
  - crash mid-clear after the new generation is complete; an unopenable newest generation fails the
    open and nothing is deleted;
  - a clear failing after its rename stops the writer, and a reopen sees the clear;
  - a put or remove whose index step fails leaves no log record (checked through a crash rebuild);
  - a clear activates the prepared spare (with its index ready once the key size is known);
  - a leftover spare is removed on open;
  - compaction keeps exactly the live rows, with commits landing during its copy, during its
    catch-up, after its last round and uncommitted at the switch (a test gate pauses the thread);
    both logs shrink; the result survives a crash and a clean reopen;
  - a snapshot taken before the switch keeps reading the old generation;
  - a crash before the switch leaves the table as it was, and the open deletes the compaction;
  - a clear cancels a running compaction; a derived-key table compacts;
  - the automatic trigger, a requested compaction needing dead puts, and pacing (of the copy only:
    `test_tntable_compaction_catch_up_is_not_paced`);
  - a switch failing after its rename stops the writer, and the reopen has every row;
  - randomized compactions, clears, commits, crashes and clean reopens against a model;
  - a failed automatic compaction backs off instead of restarting at every commit;
  - what is committed during the thread's bulk sync is caught up by the thread, not the switch;
  - a compaction that finished before a clear is discarded at once;
  - the dead count survives a clean reopen; a compaction needs disk room; a close interrupts a
    paced compaction's sleep;
  - readers on other threads keep consistent values and scans across automatic switches.
- **`layout.rs`:** `meta` round trip and corruption, generation listing.

Crash simulation leaks the database (`std::mem::forget`), so no file is sealed and no index synced,
as after a killed process; it first releases the tables' locks (`release_locks_for_crash`, test
only), as a dead process's are.

---

## Benchmarks

All are `#[ignore]`d; run them with `--ignored --nocapture --test-threads 1`, in `--release`.

| bench | what it compares |
|---|---|
| `workload_consensus_rounds` | production-shaped consensus rounds (N = 10, 50): raw tndb, `Layered<TnDb>`, `Layered<MDBX-prod>`, MemDb |
| `workload_batch_cache` | single batch inserts with concurrent `multi_get` readers, on the cache-mode layer |
| `workload_startup_reload` | reopen 8 hours into an epoch (1.33M rows): clean reopen and crash reopen (rebuild) |
| `workload_compaction` | a long-lived table under churn, tndb with compaction off and on (raw and layered), MDBX for scale: disk use and step latency |
| `db_backend_comparison` | per-operation battery (`db_bench.rs`): tndb, MDBX (test and production configuration), MemDb |
| `kv_concurrent_read_bench` | concurrent point reads, tndb vs MemDb vs MDBX |
| `pack_vs_mdbx_bench` | raw pack-file KV (digest and B+tree index), tndb, MemDb, MDBX |

```text
cargo test --release -p tn-storage workload_ -- --ignored --nocapture --test-threads 1
```

Interpreting results on macOS: commits there do not flush the drive's cache (see the platform
note), so durable latencies understate Linux's. The first write to each page of a data log's
preallocated growth faults that page in from disk on APFS, which keeps large single inserts (the
256 KB batch-cache row) behind MDBX there, since MDBX grows sparsely; Linux's `fallocate` leaves
unwritten extents that should fault without a read (not yet measured). A bench must not sleep
before a timed region: an idle CPU slows the region that follows by up to about 2×. Compare
backends on Linux for production numbers.
