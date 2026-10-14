# tndb: one shared log for all tables

`tndb` is a `tn_types::Database` whose tables share **one log** per database. Each table keeps its
own B+tree index of positions in that log. It replaced an earlier per-table engine of the same
name (removed; in git history), which the table below contrasts it with. This engine was called
`tnlog` while the two were compared; the measurements against the per-table engine
further down are kept as the record of that choice. Cleared, removed and overwritten bytes stay
in the log until a background compaction rewrites it (see Compaction).

| | per-table engine (removed) | tndb (shared log) |
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

The newest `gen-<N>` is the current generation. An open deletes any `compact-*` (an unfinished
compaction is never a generation), refuses a generation with no log, and deletes older generations
only once the newest has replayed: after a crash right after a switch the older one is the only
other copy (no automatic fallback to it, since the newest may hold acked batches). A new
database's `gen-0` is built in a `compact-0-*` directory and renamed into place, so no failure or
crash leaves a generation without a log.

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
batch. The rewind's zeros are flushed at once: a sync round's msync of the page the batch shares
with the last published one, or the system's writeback, may already have put the batch on disk,
and only the zeros reaching it keep a power loss from bringing it back. A batch that panics is
latched first, then taken back the same way. A batch that can't be taken back stops the writer, as
does a rollback that panics, a batch whose index is poisoned (a failed growth: it would refuse
every later growth, so one table would fail its writes while the rest carried on), and a panic
while a batch is being published.

**Removals.** A removal of a key that isn't present writes nothing. A batch that changes nothing
isn't numbered; in `Sync` mode it still waits until the state it observed is durable (an earlier
batch, perhaps not yet synced).

**Encode buffers.** Bare writes and point reads encode into per-thread buffers; one grown past
4 MiB by a large value is let go after use, so a thread does not keep the largest value it ever
wrote.

**Record size.** The log takes records up to 1 GiB (a large consensus output is one value), where a
default pack allows 16 MiB: the cap is per pack (`MmapFileOptions::max_record_size`), so packs read
from peers keep the smaller one, which guards against decompression bombs.

**Lock order.** The registry lock (table registration, key-size fixes, and each manifest rewrite) is
taken only before the write lock, never while holding it. The commit thread holds neither.

## Reads

Readers take no lock. Each table's `Published` snapshot holds its index snapshot, the log's
`MapView`, and its own byte **limit**: the log's length at the publish. Reads check against
`min(limit, mapped)`, so no shared atomic is written per write.

**Index damage is an error, never a wrong answer.** A read (a get, a scan, or a compaction's
copy) checks that the record an index entry points at is a put of that table and that key
(`record::put_value`), so a damaged position is `CorruptIndex`, not another row's value; a scan
ends there (logged), and a compaction abandons rather than copy it. A removal whose key the index
cannot read fails the batch (nothing written) rather than counting the key absent, which would
leave the row in place.

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
  covering every earlier batch. The `tndb-commit` thread syncs **on demand**: at once when a
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

**`persist` covers the writes made before it is called** (it reads the last batch number then, not
at its first poll). A `persist` still waiting when the commit thread exits (a close that could not
sync, or a crash in a test) fails rather than waiting for a sync no thread will run.

## Transactions

A `TnDbTxMut` encodes its writes into one byte arena (no allocation per write), per table and in
order, noting whether the table was cleared first. Its own `get` sees them (each key's last write);
nobody else does until it commits. Commit, or a drop without one as with `LayeredDatabase` (with
a warning), sorts each table's writes by key, keeps each key's last, and writes them as one batch, **atomic across tables** and in
key order (so a bulk transaction's rows scan sequentially). A batch therefore names each key at most
once per table, its clear first, which lets the write path decide each removal from the index
alone. A transaction dropped while a panic unwinds was cut off part way: nothing of it is
written. Production
transactions only write and then commit (`CertificateStore::delete` reads through its own
transaction), so buffering changes nothing they rely on.

## Recovery (open)

**When the log is replayed.** If the log is unclean, it is replayed once and every index is
rebuilt. If the log is clean but an index is stale (unclean, missing, or covering another length),
only that table's index is rebuilt; the replay still reads the whole log.

**How the replay works.** It reads through a cloned handle. Batches apply in log order: PUT
inserts, REMOVE removes, CLEAR empties that table. Records after the last whole batch are dropped.

**Fail-closed rules:**
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
- buffered transaction visibility, and a dropped transaction being written (and one dropped while
  panicking not);
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
  across the switch; the disk filling during the copy; an emptied log compacting at once (also
  after a reopen); writeback as the copy goes; and compactions inside the randomized crash model;
- hardening: a failed open keeping the older generation; an interrupted creation reopening; a
  removal through a damaged index failing; a misindexed row read as `CorruptIndex`;
- metrics: every series registered at open, then batches, syncs, log and unsynced bytes, a
  compaction (outcome, generation, duration, switch hold), abandons by cause, a cancel, a stall,
  the `failed` latch, and a `SplitDatabase`'s `env` labels (`metrics_util`'s debugging recorder);
- inspection: rows and dead bytes before and after a compaction, a crashed log reported unclean
  with no file changed, an open database refused, damage reported, and a `SplitDatabase`;
- failures: a panic in a rollback and in a publish latching, a poisoned index latching, a
  rollback flushing its zeros, a `Sync` no-op waiting for the state it saw, a `persist` failing
  once the commit thread stops, encode buffers shrinking, a 20 MiB value (written, replayed,
  compacted), the switch's `base` matching the written one, and index pages stamped before the
  switch;
- power loss (`archive::data_file::disk_model`: an image per tracked file of the bytes real
  flushes put on disk; a power loss keeps only the image): rows from before an unclean reopen
  surviving that open's later writes, a rolled-back batch staying gone, and the randomized model
  with half its crashes losing power;
- concurrency and scale: writers, a scanner, a persister and compactions at once; a
  `SplitDatabase` crash of all three environments and a transaction whose cache part fails
  (atomic per environment); the log outgrowing its reservation (a test-only 1 MiB one) while a
  reader scans.

**Mutation checks.** Each of these fails a test: applying a batch before its end, skipping the
revert, advancing the watermark before the sync, ignoring CLEAR in replay, writers never leading a
round, an eager commit thread, a `persist` that doesn't ask for a round, and a write that never
wakes an idle commit thread. Accepting writes after a latch is still refused, by the poisoned log.
For compaction: skipping the last catch-up, copied rows without the batch-end flag, a catch-up
that ignores CLEAR, installing before the rename, `base` not read at open, no empty indexes,
caching an unfixed key size, no cap on the switch's tail, a switch without the registry lock,
ignoring `has_dead`, and no latch after the rename. For metrics and inspection: no `failed`
gauge at a latch, abandon causes not told apart, and an inspection that takes no lock. For
failures and power loss: no latch before a panicking rollback, no latch over the publish, no stop
on a poisoned index, no flush of a rollback's zeros (the power-loss model sees the batch come
back), no open-time fsync of an unclean log (the model loses rows the reopened log built on), a
sync round that skips its msync (the randomized power-loss model), and tndb's log at the default
16 MiB record cap.

## Compaction (`compact.rs`)

**Trigger.** The log compacts automatically (`CompactionConfig`; by default at 64 MiB, the copy
paced at 64 MiB/s) when it is at least `auto_min_bytes` long, at least twice `base` (its length
right after the last compaction, kept in the generation's `compacted` file; 0 if none), and may
hold a dead row (`has_dead`: set by an overwrite, an effective removal or a clear of a non-empty
table; assumed at open unless the log is empty). In steady state that is "dead ≥ live", with no
accounting per write; a log that only grows never compacts. A batch that leaves every table
empty, in a log of at least 1 MiB with a dead row, also asks for one at once (automatic
compaction on), whatever the threshold. The epoch tables are all cleared at every epoch's end,
so the epoch log resets there: nothing is live to copy, and the switch takes about 0.1 ms.
`Database::compact` asks for one at once (a log of at least 1 MiB). A failed compaction backs
the trigger off (60 s, doubling to 1 h); a request that arrives while one runs is ignored, so it
cannot start another the moment that one ends.

**Steps.** One `tndb-compact` thread per database:
1. **Snapshot**, under the write lock: every table's last publish and the log's end `E`, one
   point between two batches.
2. **Copy**, no lock, paced: each table's rows in key order, each its own batch (the stored put
   record with its batch-end flag set). Each snapshot is dropped once copied. Each round's sync
   also publishes the new indexes (CRC-stamping their pages), so the switch stamps under the write
   lock only the pages its own tail writes. Every MiB the copy
   or a catch-up appends is handed to writeback at once (Linux: `sync_file_range(WRITE)`, which
   neither waits nor flushes the drive's cache), so the new log never piles up unflushed: on
   Linux a pile of dirty pages throttles every thread that writes, the database's writers
   included. The round syncs stay the durability points.
3. **Catch-up**, no lock, unpaced, in rounds (each synced) until one had at most 1 MiB, at most
   8 rounds (a writer faster than the catch-up is then caught up by the switch's last try). A round writes the **net effect** of the old log's batches since the last one: a
   clear of each table it cleared (if the new log holds rows of it), then each touched key's last
   write, in key order: a live row copied as its own batch, or a removal of a row the new log
   holds. What was written and made dead during the compaction is not copied, so the new log
   stays about the size of the live rows. Key sizes come from the snapshot or, for a table whose
   key size was fixed since, the live table (an unfixed one is never cached).
4. **Switch**, under the registry lock (no table registered, nor its key size fixed, meanwhile)
   and then the write lock: an empty index for each table with a key size and no rows, a last
   round, `compacted` written (temp file, sync, rename, directory sync); then under the write
   lock the last few batches (if more than 4 MiB landed meanwhile, it lets go of both locks and
   tries again; the third and last try takes up to 64 MiB under the write lock, so writers wait
   once and a compaction finishes at any sustained write rate), a row-count check against every table, a sync, the rename into
   place, a directory sync, and the install: the writer moves to the new log, indexes, view and
   frontier, and publishes every table. The install does no I/O and cannot fail.

**Failures.** Before the rename, a failure (or a panic, or less than 256 MiB free, checked once
per MiB of the copy, before each round and before the switch, or more than 64 MiB landing before
the last switch try) abandons the
compaction: its files are deleted after the locks are
released, and the database carries on in its generation. After the rename, a failure (the
directory sync) or a panic stops the writer, latched: which generation the next open picks is
unknown. A compaction needs the generation's size plus 128 MiB free to start. A close cancels a
running compaction (checked each MiB of the copy, during pacing sleeps, between rounds and
before each sync); its unfinished log is discarded without a flush, so the close does not wait on
writing out what it throws away.

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
automatically only from 256 MiB (`CACHE_AUTO_MIN_BYTES`, four times the default), and the epoch
log only from 2 GiB (`EPOCH_AUTO_MIN_BYTES`, a safety net: it resets at each epoch's end, above),
so consensus writes see no compaction of their own log mid-epoch.
`workload_mixed_compaction` judges it on interleaved pairs (1000 rounds each, compaction on and
off, order rotated), since vote p99 varies about 30% run to run here and short runs hide the
effect:

| cache log compacts from | vote durable p99, on / off (median of 6 pairs) | ms per round | compactions per run |
|---|---|---|---|
| 64 MiB | 1.26 (1.10–1.33) | 1.07 | 11–12 |
| 256 MiB | 1.06–1.09 (0.90–1.18) | 1.01–1.03 | 4–5 |

At a realistic batch rate (1 MB at 25/s, 3 pairs of 12,000 rounds on macOS), with the epoch log
compacting only at the epoch end: the cache log compacts 5 times a run, the switch holding its
write lock 0.3–0.6 ms on average (at most 1.4 ms), and the ratios are 1.01 (vote p99) and 1.01
(ms per round). Linux, before the epoch-end reset and the writeback: 9–10 compactions per
29-second run across the epoch and cache logs, the switch holding the lock 15–35 ms (outliers
to 232 ms), ordinary write-lock holds up to ~0.5 s while a compaction ran (1.3–2.3 ms without),
vote p99 ratios 1.23–1.70. Linux after both (4 pairs of 6,000 rounds): the cache log compacts 6–7
times a run, the switch holding its lock 7–10 ms on average (at most 26 ms: two or three drive
flushes, the new log's and the directory's, which block only that log's writers); the epoch log's
longest write-lock hold is the same with compaction on as off (4–26 ms); vote p99 1.04 (pairs
0.98–1.18), ms per round 1.04.

## Production shape: one log per environment (`split.rs`)

**Why one log isn't enough.** One write lock serializes every append, so a large value holds up
every table while it is copied. On this Mac, a 256 KB append holds the lock about 0.5 ms, much of it
faulting fresh pages. In the `workload_mixed` bench, a stream of 256 KB batches pushed consensus
rounds (N=50) from 3.3 to 55 ms.

**`SplitDatabase`** opens three `TnDatabase`s under one root (`epoch/`, `kad/`, `cache/`). It
routes each table by its `TableHint`, as `open_db` splits MDBX, so the batch caches never share a
write lock with consensus. It's a thin router with no layer in between: `CompositeDatabase` would
also wrap each database in a `LayeredDatabase`, whose in-memory layer and writer thread cost tndb
3-4x its durability latency.
- A write transaction opens one tndb transaction per environment it writes. Each part commits as
  one batch in its own log, so it is atomic per environment, not across them.
- `persist` covers the table's environment, as with MDBX today: every production barrier is on
  consensus tables.

**Gate** (`workload_bench`, 3 runs, this Mac). On consensus alone, Split matches one log (both
2.9-3.0 ms per round at N=50; vote durable p50 0.33 ms, against the per-table engine's 0.97 ms, both in group commit). Beside the
batch writer:

| N=50 consensus beside | | per-table, group | tndb-group (one log) | Split |
|---|---|---|---|---|
| 256 KB batches, unpaced | ms per round | 5.0 | 54.7 | **3.3** |
| | vote durable p50 / p99 | 2.3 / 18.5 ms | 3.8 / 49.3 ms | **0.38 / 2.2 ms** |
| | own header durable p50 | 2.4 ms | 1.7 ms | **0.17 ms** |
| | batch MB/s | 389 | 331 | **396** |
| 1 MB batches at 25/s | ms per round | 4.0 | 3.05 | **2.99** |
| | vote durable p50 / p99 | 1.0 / 4.3 ms | 0.34 / 2.1 ms | **0.33 / 1.9 ms** |

## Against MDBX (the previous backend)

`workload_mixed`'s `Composite<MDBX-prod>` column is the `CompositeDatabase<MdbxDatabase>` that
`open_db` built before tndb (production geometry, `Durable` sync), beside `SplitDatabase` (median of
3 runs, N=50, this Mac).
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

What MDBX's layer still did better: a write returns from memory (tndb's take the
environment's write lock), and reads of the epoch and kad tables come from memory (batch-cache
`multi_get` runs at 55–65% of its rate; tndb checks each record's CRC on read). After a crash
tndb replays its logs (about 0.8 s for a 229 MB log); MDBX recovers at once. Its disk use stays
smaller (it reuses freed pages).

**On Linux** (i7-1360P laptop, NVMe ext4 with FUA: a synchronous 4 KB write takes about 2.5 ms;
QUICK baseline, 2026-10-13), every durable ack pays a drive-cache flush, and the gap to production
widens:

| N=50 | Split | Composite / layered MDBX |
|---|---|---|
| consensus alone: ms per round | **7.1** | 1136 |
| vote durable p50 / p99 | **3.7 / 5.8 ms** | 824 ms / 2.9 s |
| beside 1 MB batches at 25/s: ms per round | **7.5** | 1508 |
| vote durable p50 / p99 | **3.4 / 9.5 ms** | 1.07 s / 3.4 s |
| startup reload, 28,800 rounds into an epoch (N=10) | **24.5 ms** | 473 ms |

A vote's durability there is about two sync rounds (~2.1 ms each). The batch stream at 25 MB/s
raises vote p99 from 5.8 to about 9 ms by sharing the drive, with compaction off as much as on.

**A drive-saturating burst:** 256 KB batches as fast as taken, 460-780 MB/s against the drive's
825 MB/s.
- **Consensus:** Split takes 29 ms per round with compaction off and 40 ms with it on (vote p99
  67 / 94 ms), against 7.1 alone. The epoch log's write lock is held up to 20-120 ms, the kernel
  blocking a writer inside it.
- **Compaction:** the batch log's compactions can't keep up; some give up after three switch tries.
- **How far from production:** that rate takes about 50-80 validators all sending ten full 1 MB
  batches in every header, every round: 4-6 Gbps into every node, and execution far beyond one
  node's EVM.

tndb is the node's consensus DB (`DatabaseType = SplitDatabase`, at `<consensus db>/tndb`, since
2026-10; MDBX before). On its first tndb open, a node that ran on MDBX
has its epoch tables (its anti-equivocation records: last proposal, votes, proposed certificates),
kad tables and own uncommitted batches (`OurNodeBatchesCache`: no other copy holds them) copied
over (`migrate_from_mdbx` in `lib.rs`) and made durable. Then the MDBX environments are deleted
(MDBX's own files, and each directory if nothing else is in it), so a later reset of tndb cannot
bring back their state from the time of the switch, and the migration is marked done
(`tndb/mdbx-migrated`). A crash before the marker reruns the copy for whatever is still there.
The other caches start empty (they refill). A downgrade to the MDBX build after the switch starts
with empty epoch tables. Abandoning the epoch tables instead would be safe only while the
network keeps making rounds as validators restart: after a stop-all halt every node would return
to the same round with no memory of what it proposed and voted there. The migration is kept for
the release that switches backends; the next one removes it with the MDBX consensus backend.

## Metrics (`metrics.rs`)

Each database creates its metric handles when it opens, labeled `env` (`epoch`, `kad`, `cache`
in a `SplitDatabase`; `unnamed` otherwise), and every series exists from then on (counters at 0,
so an `increase` rule sees the first event). The writers and the commit and compaction threads
record through the handles; the node's recorder is installed before any database opens.
Rendered names (the `tn_storage.` prefix becomes `tn_storage_`):

| metric | type | what |
|---|---|---|
| `tn_storage_tndb_batches_total` | counter | batches written (a bare write or a transaction) |
| `tn_storage_tndb_sync_seconds` | histogram | sync rounds (msync, plus fsync after a growth) |
| `tn_storage_tndb_unsynced_bytes` | gauge | log bytes not yet synced (writers wait past 64 MiB) |
| `tn_storage_tndb_log_bytes` | gauge | the current log's length |
| `tn_storage_tndb_generation` | gauge | the current generation |
| `tn_storage_tndb_write_stall_seconds{kind}` | histogram | write-lock `wait`s and `hold`s of at least 1 ms only |
| `tn_storage_tndb_compactions_total{outcome}` | counter | `switched`, `skipped`, `abandoned`, `cancelled` |
| `tn_storage_tndb_compaction_abandons_total{cause}` | counter | `disk_full`, `writer_outran`, `corruption`, `panic`, `error` |
| `tn_storage_tndb_compaction_seconds` | histogram | a switched compaction, snapshot to install |
| `tn_storage_tndb_compaction_switch_seconds` | histogram | how long the switch held the write lock |
| `tn_storage_tndb_failed` | gauge | 1 once a failure latched (a failed sync, a stopped writer): writes fail until a restart |

The write path pays a few clock reads around the write lock, one counter increment and two
gauge stores; a histogram sample only on a stall. The consensus bench showed no difference
(within noise, 6 interleaved pairs on macOS).

**Alerts** (Prometheus):
- page: `max by (env) (tn_storage_tndb_failed) > 0`: the database stopped taking writes;
- warn: `sum by (env) (increase(tn_storage_tndb_compaction_abandons_total[1h])) >= 3`: the log
  is not being compacted (the `cause` label says why; `disk_full` needs room, `corruption` an
  operator);
- warn: `max by (env) (tn_storage_tndb_unsynced_bytes) > 32 * 2^20` for 1m: syncs fall behind
  the writers (half the cap at which writers wait);
- warn: `histogram_quantile(0.99, sum by (env, le) (rate(tn_storage_tndb_sync_seconds_bucket[5m])))
  > 0.1`: slow syncs.

## Inspection (`inspect.rs`, `telcoin-network db tndb-stat`)

`inspect` reports what a database holds, from its files, changing none of them: its generation,
whether the log was closed cleanly, its length, live and dead bytes (what a compaction would give
back) and compacted base, older generations or unfinished builds left behind, and per table the
live rows and bytes and the index's state as the next open would find it (current, stale,
unclean, missing, unreadable). It opens the log read-only and replays it as an open would, so a
damaged log fails it as it would fail the open. It takes a shared lock on the database's `LOCK`,
so it refuses a database the node has open (and keeps the node from opening until it is done):
a live node is watched through the metrics. `inspect_split` covers a `SplitDatabase`'s
environments and notes the MDBX migration's marker; `telcoin-network db tndb-stat` prints it for
`<datadir>/consensus-db/tndb`, exiting non-zero if an environment could not be read.

## Limits and choices

- **Address space.** Each log reserves 16 GiB of address space and each index 1 GiB (about 62 GiB
  for a `SplitDatabase` at rest, more while a compaction holds both generations). The
  reservations are virtual, shared and not commit-charged; a process under a `ulimit -v` too small
  for them falls back to mapping each file at its length, remapping as it grows.
- **`has_dead` at open.** A log with records is assumed to hold dead rows when it opens, so a
  restart can cost one compaction that frees little. It happens only when the log is already past
  its threshold and twice its base, so it is kept as is rather than recording a dead-row count.
- **Index growth during a compaction.** Under extreme churn a table's index grows while a
  compaction's copy pins its snapshot (pages superseded meanwhile can't be reused until the
  switch replaces the index). It is bounded per compaction and negligible at production rates;
  copying unpaced for small live sets would shorten the pin, but changes the I/O pattern, so it
  waits for a Linux measurement.
- **The batch cache is evicted and compacted, not aged out.** We tried aging `NodeBatchesCache`
  out wholesale instead of removing each committed batch (2026-10-13; a prototype, then removed).
  It kept a ring of tndb databases ("segments") with compaction off and opened a new one every 5
  saved outputs, keeping 6. Results:
  - **No gain at a realistic rate.** It was measured against `Split` in interleaved pairs, N=50,
    on a Mac. At 1 MB batches 25 times a second, consensus didn't change (vote p99 and ms per
    round 1.01x) and the batch log stopped compacting. Those compactions weren't costing
    consensus anything.
  - **A gain only at an extreme rate.** At 330 MB/s of batches, vote p99 improved to 0.87x.
  - **Costs.** About 8x the disk (1.8 GB against 0.2 GB at 25 MB/s), because a store that can't
    see commits must keep 30-35 outputs for the worst case. Lookups of older batches checked up
    to 7 indexes. It added a timing assumption (the window) where eviction on commit is exact.
    The node would also have needed the worker to serve our own batches from
    `OurNodeBatchesCache` once they aged out.
  - **Why.** A compaction copies only live rows, and the batch cache's live set (the
    uncommitted batches) is small. It also runs on its own log, away from consensus.

  Faster compaction helps every log, so it is the better lever; a Linux profile of the switch
  should come first.

## Not yet

- **Write lock:** CRC computed outside it, and log growth kept off it, if the lock shows in
  measurements.
