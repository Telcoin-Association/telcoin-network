# tnlog: one shared log for all tables

`tnlog` is a `tn_types::Database` whose tables share **one log** per database. Each table keeps its
own B+tree index of positions in that log. It is built beside the per-table engine
[`tndb`](../tndb/README.md), so the two can be compared on the same workloads. Cleared, removed
and overwritten bytes stay in the log until a background compaction rewrites it (see
Compaction).

| | tndb (per table) | tnlog (shared) |
|---|---|---|
| files | one log + one index per table | one log + one index per table |
| a commit syncs | each written table's log | the one log, once, however many tables |
| commit thread vs writers | takes each table's lock twice per round | takes no lock a writer takes |
| transactions | applied at once; atomic per table | buffered until commit; atomic across tables |
| clear | a new generation (durable on return; data leaves the disk) | a CLEAR record (bytes leave at the next compaction) |
| compaction | per table | the whole log, one new generation |
| recovery | per table | one replay of the log, at database open |

## Layout

```text
<root>/LOCK                        flock: one open writer per database (held until the core drops)
<root>/tables                      the manifest: table id -> name, key size (manifest.rs)
<root>/gen-<N>/log                 the shared log (a CRC-framed pack)
<root>/gen-<N>/idx-<id>/index.btx  table <id>'s B+tree index (derived; rebuilt from the log)
<root>/gen-<N>/compacted           the log's length when compaction made this generation
<root>/compact-<N+1>-<pid>-<n>/    a compaction's new generation, being built
```

The newest `gen-<N>` is the current generation; an open deletes older ones and any `compact-*`
(an unfinished compaction is never a generation), and refuses a generation with no log.

**Manifest.** It lists table ids (`u16`, never reused), names and key sizes. It is rewritten
atomically (tmp, `fsync`, rename, directory `fsync`) when a table is first opened, or when its first
row fixes its key size. Both happen before the log holds any record of that table, so a record whose
table id is missing from the manifest is damage. A table's index is created when its key size is
fixed. An index directory the manifest doesn't know is deleted at open.

## Records (`record.rs`)

```text
byte 0     kind (bits 0-6: PUT 1, REMOVE 2, CLEAR 3) | BATCH_END (bit 7)
bytes 1-2  table id (u16 LE)
PUT        key (the table's key size) ‖ value
REMOVE     key
CLEAR      (nothing)
```

**Batches.** A batch is a bare write, or a whole transaction. It is a run of consecutive records,
and only the last one carries `BATCH_END`.

**No commit records.** Replay applies a batch only once its flagged record reads back intact, so
every batch end is a commit point. A torn tail drops the incomplete batch.

**Fail-closed records.** An unknown kind or table id, or a record of the wrong length, fails the
open. Every PUT stores its key: recovery runs at database open, before any key function could be
known, so there is no derived-key mode.

## Writes: one write lock (`store.rs`)

Every write takes the database's one write lock and, under it:
1. appends its batch;
2. applies it to the touched tables' indexes;
3. publishes each touched table's snapshot;
4. numbers the batch;
5. stores the log's **frontier** as one value: the generation, the log's end, and the batch number.

So the log holds only whole batches between writes, and no snapshot ever sees part of a batch.

**A batch that fails is taken back exactly.** The log is rewound to the batch's start (nothing else
can have appended since), and each touched index is reverted to its last publish
(`BtreeIndex::revert_to_published`). Under the lock, that last publish is its state before the
batch. A batch that panics is taken back the same way, and also latches the failure. A batch that
can't be taken back stops the writer.

**Removals.** A removal of a key that isn't present writes nothing. A batch that changes nothing
isn't numbered.

**Lock order.** The registry lock (table registration, key-size fixes, and each manifest rewrite) is
taken only before the write lock, never while holding it. The commit thread holds neither.

## Reads

Readers take no lock. Each table's `Published` snapshot holds its index snapshot, the log's
`MapView`, and its own byte **limit**: the log's length at the publish. Reads check against
`min(limit, mapped)`, so no shared atomic is written per write.

A scan holds the database core, so its files stay open while the scan lives, even after every
handle has dropped. The database's `LOCK` is the core's last field, so it is released only once the
files have closed.

## Durability (`commit.rs`)

**Each round.** A sync round loads the frontier, `msync`s the log up to its end through the
generation's view, and `fsync`s a long-lived duplicate descriptor if the log grew. Then every batch
numbered up to the frontier's number is durable. No round takes the write lock.

**One sync in flight.** One round runs at a time (the core's sync lock), shared by every writer
waiting. That matters on Linux, where two overlapping syncs of one file could otherwise ack data a
failed writeback lost. The commit marker is stamped lazily by the next writer, under the write
lock.

**Modes:**
- **`CommitMode::Sync`:** each write, or transaction commit, waits until it is durable before it
  returns. The first writer waiting runs the round itself, with no thread hand-off; the others wait
  for it, and one whose batch it didn't cover leads the next. A write is readable before its writer
  returns, as in group mode.
- **`CommitMode::Group`:** writes return at once, and `persist` (async) waits for the round
  covering every earlier batch. The `tnlog-commit` thread syncs **on demand**: at once when a
  `persist` waits, otherwise once the unsynced writes are 2 ms old or 4 MiB large (`SYNC_DELAY`,
  `SYNC_BYTES`). A write wakes the thread only when it's idle. Eager syncing (a round per write)
  cost a write-heavy workload most of its throughput on this Mac: each round re-cleans the log's
  tail page, which the next append must fault back in.

**Backpressure.** In group mode a log holds at most 64 MiB unsynced (`MAX_UNSYNCED`). A write that
leaves more waits for a sync before it returns, running it itself if no round is running. So writes
nobody waits on (the batch caches are never persisted) can't outrun the disk without bound.

**Failures latch.** The first failure (a failed sync, a writer that stopped, a panic) is latched.
Every later write and `persist` fails, the watermark never moves again, and the log is never sealed
as clean, so the next open recovers it.

## Transactions

A `TnLogTxMut` encodes its writes into one byte arena (no allocation per write), per table and in
order, noting whether the table was cleared first. Its own `get` sees them (each key's last write);
nobody else does until it commits. Commit, or drop as with `LayeredDatabase`, sorts each table's
writes by key, keeps each key's last, and writes them as one batch, **atomic across tables** and in
key order (so a bulk transaction's rows scan sequentially). A batch therefore names each key at most
once per table, its clear first, which lets the write path decide each removal from the index
alone. Production
transactions only write and then commit (`CertificateStore::delete` reads through its own
transaction), so buffering changes nothing they rely on.

## Recovery (open)

**When the log is replayed.** If the log is unclean, it is replayed once and every index is
rebuilt. If the log is clean but an index is stale (unclean, missing, or covering another length),
only that table's index is rebuilt; the replay still reads the whole log.

**How the replay works.** It reads through a cloned handle. Batches apply in log order: PUT
inserts, REMOVE removes, CLEAR empties that table. Records after the last whole batch are dropped.

**Fail-closed rules (as in tndb):**
- a sealed log must replay whole;
- an unclean log may end in a tear, but not below its commit marker;
- a failed replay changes nothing.

Only then is the log cut back, each index rebuilt and synced, and both files marked consistent.

## Tests (`test.rs`)

**Coverage:**
- the shared `Database` suite in both modes;
- a clean reopen;
- a crash rebuild of interleaved tables;
- cross-table transaction atomicity (a torn flagged record drops the whole batch);
- buffered transaction visibility, and a dropped transaction being written;
- a CLEAR inside a batch;
- a failed batch taken back (injected index allocation failure; also checked after a later write
  publishes);
- a panicking batch taken back and latched;
- group `persist` and its FIFO across tables;
- concurrent `Sync` writers;
- unknown table and kind failing closed;
- a scan outliving every handle;
- a second open refused;
- a failed sync latching;
- durability only after the sync (a gate pauses the round);
- `Sync` writers syncing themselves with the commit thread stopped;
- group syncs on demand (2,000 unpersisted writes made durable within the delay by a few syncs);
- a `persist` not waiting out the delay;
- a stale index rebuilding alone, with an orphan index deleted;
- a randomized crash model over two tables;
- `revert_to_published` (randomized, in `btree_index`);
- the manifest and the record format;
- group mode's cap on unsynced bytes (the commit thread stopped);
- `SplitDatabase`: the shared suite, and routing by hint (each table's manifest entry lands in its
  environment's database);
- compaction: rows kept exactly with writes at each step (three tables, randomized, three
  compactions, clean reopen); space returned and `base` recorded; the trigger (append-only, twice
  the base, kept across a reopen); the first rows of a table unfixed at the snapshot; a table
  opened and a table cleared during one; a scan across the switch; the switch holding the
  registry; the switch's tail cap; a crash after the switch; copied rows as whole batches; a
  leftover `compact-*` deleted and a generation without a log refused; a close cancelling; the
  failures before the rename (abandoned) and after it (the writer stopped); a sync round paused
  across the switch; and compactions inside the randomized crash model.

**Mutation checks.** Each of these fails a test: applying a batch before its end, skipping the
revert, advancing the watermark before the sync, ignoring CLEAR in replay, writers never leading a
round, an eager commit thread, a `persist` that doesn't ask for a round, and a write that never
wakes an idle commit thread. Accepting writes after a latch is still refused, by the poisoned log.
For compaction: skipping the last catch-up, copied rows without the batch-end flag, a catch-up
that ignores CLEAR, installing before the rename, `base` not read at open, no empty indexes,
caching an unfixed key size, no cap on the switch's tail, a switch without the registry lock,
ignoring `has_dead`, and no latch after the rename.

## Compaction (`compact.rs`)

**Trigger.** The log compacts automatically (`CompactionConfig`, as tndb's: at 64 MiB, the copy
paced at 64 MiB/s) when it is at least `auto_min_bytes` long, at least twice `base` (its length
right after the last compaction, kept in the generation's `compacted` file; 0 if none), and may
hold a dead row (`has_dead`: set by an overwrite, an effective removal or a clear of a non-empty
table; assumed at open unless the log is empty). In steady state that is "dead ≥ live", with no
accounting per write; a log that only grows never compacts. `Database::compact` asks for one at
once (a log of at least 1 MiB). A failed compaction backs the trigger off (60 s, doubling to 1 h).

**Steps.** One `tnlog-compact` thread per database:
1. **Snapshot**, under the write lock: every table's last publish and the log's end `E`, one
   point between two batches.
2. **Copy**, no lock, paced: each table's rows in key order, each its own batch (the stored put
   record with its batch-end flag set). Each snapshot is dropped once copied.
3. **Catch-up**, no lock, unpaced, in rounds (each synced) until one had at most 1 MiB, at most
   8 rounds. A round writes the **net effect** of the old log's batches since the last one: a
   clear of each table it cleared (if the new log holds rows of it), then each touched key's last
   write, in key order: a live row copied as its own batch, or a removal of a row the new log
   holds. What was written and made dead during the compaction is not copied, so the new log
   stays about the size of the live rows. Key sizes come from the snapshot or, for a table whose
   key size was fixed since, the live table (an unfixed one is never cached).
4. **Switch**, under the registry lock (no table registered, nor its key size fixed, meanwhile)
   and then the write lock: an empty index for each table with a key size and no rows, a last
   round, `compacted` written (temp file, sync, rename, directory sync); then under the write
   lock the last few batches (if more than 4 MiB landed meanwhile, it lets go of both locks and
   tries again, at most 3 times), a row-count check against every table, a sync, the rename into
   place, a directory sync, and the install: the writer moves to the new log, indexes, view and
   frontier, and publishes every table. The install does no I/O and cannot fail.

**Failures.** Before the rename, a failure (or a panic, or less than 256 MiB free, checked before
each round and the switch) abandons the compaction: its files are deleted after the locks are
released, and the database carries on in its generation. After the rename, a failure (the
directory sync) or a panic stops the writer, latched: which generation the next open picks is
unknown. A compaction needs the generation's size plus 128 MiB free to start. A close cancels a
running compaction (checked each MiB of the copy, during pacing sleeps, between rounds and
before each sync).

**Durability across the switch.** The new log is synced before the rename, and the root
directory before the new frontier is stored, so no round acks a batch while the next open might
still pick the old generation. A sync round already running on the old generation keeps it
mapped (`GenSync`'s `GenAlive`) and acks only batches durable in both. Old snapshots and scans
keep the old generation's files (retired into its `GenAlive`, deleted on close) until they drop.

**Cost to other logs.** A compaction's writes and flushes queue on the disk ahead of other logs'
syncs. Under the worst case measured (`workload_mixed`: 256 KB batches written to the cache log as
fast as it takes them, about 400 MB/s), the epoch log's sync p99 rises from about 170 µs to
0.4–0.8 ms while the cache log compacts, most of all under the catch-up rounds' flushes. Neither
preallocating the new log, flushing it in 1 MiB pieces, nor utility I/O priority helped the
consensus measure. Compacting less often did: `SplitDatabase` compacts the cache log
automatically only from 256 MiB (`CACHE_AUTO_MIN_BYTES`, four times the default).
`workload_mixed_compaction` judges it on interleaved pairs (1000 rounds each, compaction on and
off, order rotated), since vote p99 varies about 30% run to run here and short runs hide the
effect:

| cache log compacts from | vote durable p99, on / off (median of 6 pairs) | ms per round | compactions per run |
|---|---|---|---|
| 64 MiB | 1.26 (1.10–1.33) | 1.07 | 11–12 |
| 256 MiB | 1.06–1.09 (0.90–1.18) | 1.01–1.03 | 4–5 |

At a realistic batch rate (1 MB at 25/s) neither shows a measurable effect.

## Production shape: one log per environment (`split.rs`)

**Why one log isn't enough.** One write lock serializes every append, so a large value holds up
every table while it is copied. On this Mac, a 256 KB append holds the lock about 0.5 ms, much of it
faulting fresh pages. In the `workload_mixed` bench, a stream of 256 KB batches pushed consensus
rounds (N=50) from 3.3 to 55 ms.

**`SplitDatabase`** opens three `TnLogDatabase`s under one root (`epoch/`, `kad/`, `cache/`). It
routes each table by its `TableHint`, as `open_db` splits MDBX, so the batch caches never share a
write lock with consensus. It's a thin router with no layer in between: `CompositeDatabase` would
also wrap each database in a `LayeredDatabase`, whose in-memory layer and writer thread cost tnlog
3-4x its durability latency.
- A write transaction opens one tnlog transaction per environment it writes. Each part commits as
  one batch in its own log, so it is atomic per environment, not across them.
- `persist` covers the table's environment, as with MDBX today: every production barrier is on
  consensus tables.

**Gate** (`workload_bench`, 3 runs, this Mac). On consensus alone, Split matches one log (both
2.9-3.0 ms per round at N=50; vote durable p50 0.33 ms, against tndb-group's 0.97 ms). Beside the
batch writer:

| N=50 consensus beside | | tndb-group | tnlog-group (one log) | Split |
|---|---|---|---|---|
| 256 KB batches, unpaced | ms per round | 5.0 | 54.7 | **3.3** |
| | vote durable p50 / p99 | 2.3 / 18.5 ms | 3.8 / 49.3 ms | **0.38 / 2.2 ms** |
| | own header durable p50 | 2.4 ms | 1.7 ms | **0.17 ms** |
| | batch MB/s | 389 | 331 | **396** |
| 1 MB batches at 25/s | ms per round | 4.0 | 3.05 | **2.99** |
| | vote durable p50 / p99 | 1.0 / 4.3 ms | 0.34 / 2.1 ms | **0.33 / 1.9 ms** |

## Against production today

`workload_mixed`'s `Composite<MDBX-prod>` column is `open_db`'s `CompositeDatabase<MdbxDatabase>`
(production geometry, `Durable` sync), beside `SplitDatabase` (median of 3 runs, N=50, this Mac).
The bench now runs it beside the paced stream only: beside the unpaced one, its cache layer's
unbounded write queue reached 39 GB resident.

| | Split | Composite MDBX |
|---|---|---|
| beside 256 KB batches as fast as taken: ms per round | **3.22** | 123.2 |
| vote durable p50 / p99 | **0.38 / 2.15 ms** | 63.8 / 231.5 ms |
| peak memory of that run | | 39 GB resident (its cache layer's unbounded queue) |
| beside 1 MB batches at 25/s: ms per round | **2.98** | 57.7 |
| vote durable p50 / p99 | **0.33 / 1.74 ms** | 42.7 / 74.8 ms |
| own header durable p50 | **0.13 ms** | 52.3 ms |
| vote write call p50 / p99 | 73 µs / 1.14 ms | **1 / 94 µs** |

What production's layer still does better: a write returns from memory (tnlog's take the
environment's write lock), and reads of the epoch and kad tables come from memory (batch-cache
`multi_get` runs at 55–65% of its rate; tnlog checks each record's CRC on read). After a crash
tnlog replays its logs (about 0.8 s for a 229 MB log); MDBX recovers at once. Its disk use stays
smaller (it reuses freed pages).

The node opens tnlog with the `tn-storage/tnlog` feature (`DatabaseType = SplitDatabase`, at
`<consensus db>/tnlog`); MDBX stays the default.

## Not yet

- **Write lock:** CRC computed outside it, and log growth kept off it, if the lock shows in
  measurements.
