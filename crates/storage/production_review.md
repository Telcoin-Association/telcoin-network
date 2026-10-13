# Pre-production review: Split tnlog and the branch's storage changes (2026-10-12)

> **Naming (2026-10-13):** written while the shared-log engine was called `tnlog`, beside an
> older per-table engine then called `tndb`. The per-table engine was deleted (L9) and `tnlog` was
> renamed `tndb`: code now lives in `crates/storage/src/tndb/` (`TnDatabase`, `TnDbOptions`,
> feature `tndb`, on disk at `<store>/tndb`). Read `tnlog` below as today's `tndb`, and "tndb" or
> "tndb-group" in findings and measurements as the removed per-table engine.

## Context
You asked:
- **Is the plan Split tnlog?** Yes: `SplitDatabase` on all three environments (epoch, Kademlia, batch cache compacting from 256 MiB), made the default once the Linux benches confirm.
- **Review the code likely going to production**, producing a report I can action after the Linux benches.

Tracked as item 29 in `tasks/todo.md`. Fixes come after the Linux bench results, in the order at the end.

**Scope.** This branch against `main`: 152 commits, 22K lines.
- **tnlog engine** (`crates/storage/src/tnlog/`): reviewed by an independent reviewer agent.
- **Everything else** (my review):
  - the archive layer under it, which also changes today's consensus packs: `data_file.rs`, `pack.rs`;
  - `tn-types` codec and `sealed_batch`;
  - the certificate store;
  - `MemDatabase` and `LayeredDatabase` (today's default read layer);
  - `consensus_pack` and `exec_state_pack`;
  - `lib.rs`, feature wiring, CI exposure.
- **Excluded:** the storage README's do-not-flag list, benches, and tndb as an engine.
- **The two highs I checked against the code myself.** For the rest, "confirmed" means the reviewer traced the code path.

## High

**H1. [FIXED 2026-10-12: `MmapDataFile::open_with` fsyncs a writable, unclean, non-derived file once
before trusting its bytes; tests `*_open_sync*` in `archive::data_file`.] After a process crash,
recovery treats data that was never synced as durable** (confirmed; likely in today's consensus packs too).
- **Where:**
  - `MmapDataFile::open_with` starts `flushed_end` at the logical end (`data_file.rs:674`).
  - tnlog's `GenSync::new` sets `synced = file_len` (`store.rs:306`).
  - `recover` syncs the rebuilt indexes but never the log (`store.rs` `recover`).
  - Pack recovery (`consensus_pack.rs` `recover_pack`) rewinds the log without syncing it either.
- **Scenario:**
  1. The process is killed with batch B only in the page cache.
  2. It restarts within the ~30 s writeback window. Replay reads B from the page cache, and B is marked synced.
  3. A new write C is acked by `persist`. The round msyncs only C's range, which on Linux leaves B's pages dirty.
  4. Power is lost. Replay stops at B's zeros, so C, acked, is lost. Or the open refuses with "below its commit marker".
- **Impact:** this breaks `persist`'s anti-equivocation contract. It needs a crash followed by a power loss within about 30 s.
- **Fix:** make one fix in `data_file.rs`: after an unclean open, start `flushed_end` at 0, so the first barrier flushes everything. Also derive tnlog's `GenSync.synced` from the log's synced end, not `file_len`. That covers tnlog, packs and tndb at once, at the cost of one full flush per crash recovery.
- **Test:** a power-loss model that drops bytes past the synced end. Today's `crash()` keeps the page cache, so it can't catch this.

**H2. [FIXED 2026-10-12: `migrate_from_mdbx` in `lib.rs`, called from `_open_tnlog`; tests in
`migrate_test`.] Switching a node from MDBX to tnlog mid-epoch loses its anti-equivocation
records** (both reviews).
- **Where:** `_open_tnlog` (`lib.rs`) opens a fresh `<consensus db>/tnlog`, so `Votes`, `LastProposed` and `ProposedCertificates` (issues #934, #962, #963, #975) start empty.
- **Scenario:** a validator restarts on the tnlog build mid-round. It can vote for, or propose, a conflicting header in the same round.
- **Fix:** on the first tnlog open (no `tnlog/`, MDBX `epoch/` present), copy the epoch and Kademlia tables from MDBX, then mark the migration done. Cache tables start empty. This keeps MDBX in the binary for one release.
- **Test:** open MDBX with a vote and a last proposal, then open tnlog and check both are still there.

## Medium

**M1. [FIXED 2026-10-12: older generations are deleted only after the newest replays; test `test_tnlog_failed_open_keeps_the_older_generation`.] Open deletes older generations before the newest one replays** (confirmed).
- **Where:** `open_current_gen` (`store.rs:1069`).
- **Scenario:** after a crash right after a switch, gen-N+1 fails replay (damage, or a bug). Gen-N, the only other copy, is already gone.
- **Fix:** delete older generations only after `recover` succeeds. No automatic fallback: the newer one may hold acked batches.

**M2. [FIXED 2026-10-12: `Pacer::pace` checks `check_room` once per MiB; test `test_tnlog_compaction_copy_gives_way_to_a_full_disk`.] The free-space guard isn't checked during the copy, the longest phase** (confirmed).
- **Where:** `Pacer::pace` (`compact.rs:335`) checks only for cancellation.
- **Scenario:** a 1 GiB copy at 64 MiB/s takes ~16 s. Meanwhile writers fill the disk, ENOSPC latches the environment, and it stops until restart.
- **Fix:** call `check_room` every `BATCH_BYTES` in the copy, and count the builder's own growth.

**M3. [FIXED 2026-10-12: `BtreeIndex::contains` returns `Result` (only `NotFound`/`KeySize` are absent), both callers propagate; test `test_tnlog_remove_through_a_damaged_index_fails`.] Index read errors count as "absent" when deciding removals** (confirmed).
- **Where:** `store.rs:454`: `BtreeIndex::contains` is `load(key).is_ok()`.
- **Scenario:** a damaged index page makes `remove(K)` silently write nothing, so K survives.
- **Fix:** match on `load`: `NotFound` skips, any other error returns an error. Same in `Builder::remove`.

**M4. [FIXED 2026-10-12: rounds that can't converge go on to the switch, whose last try takes up to
64 MiB under the write lock (a bounded stall); tests `test_tnlog_compaction_completes_when_the_catch_up_never_converges`,
`test_tnlog_compaction_last_switch_takes_a_large_tail`. Follow-up for the Linux stalls this left at a
realistic rate (switch holds 15–35 ms, write-lock holds up to ~0.5 s while compacting): the epoch log
resets at each epoch's end when every table is emptied (`test_tnlog_compaction_when_every_table_is_emptied`)
and compacts mid-epoch only from 2 GiB; a compaction hands each MiB it writes to writeback
(`sync_file_range` on Linux; `test_tnlog_compaction_writes_back_as_it_goes`). Linux at 1 MB @25/s:
compaction's cost to consensus 1.04x vote p99 and ms/round (was 1.23–1.70x / 1.07–1.18x); cache
switch hold avg 7–10 ms, max 14–26 ms; ordinary write-lock holds while compacting no longer differ
from runs without compaction.]
Under a sustained write rate above the copy
pace, compaction keeps abandoning** (confirmed on
Linux: churn at ~700 MB/s and the unpaced 256 KB mixed stream abandon every attempt; each attempt
also grows the table's index file, which only a successful switch replaces).
- **Scenario:** the catch-up (8 rounds) or the switch (3 tries) loses to the writer; back-off grows to 1 h; the cache log grows until ENOSPC latches it.
- **Fix:** when a compaction falls behind, run the rest unpaced. Add writer backpressure once the log passes ~4× `base`, a hard cap like the 64 MiB unsynced cap. Test with `workload_mixed`'s unpaced stream over a long run.

**C1. [EXPLAINED 2026-10-12 by the quick Linux run: it is M4. At ~700 MB/s of churn every catch-up
round's flushed sync lets tens of MB more land, so all attempts are abandoned; the extra disk is the
B+tree index (304 MB for 2,048 rows), grown while a compaction's copy pinned its snapshot and never
replaced by a switch. No leftover files or untrimmed padding.] Linux churn bench: no compaction
completes, and the compacting log leaves ~200 MB more after close** (measured). `workload_compaction` on Linux ran its 40K steps in under 0.5 s; both
group engines report 0 compactions (2–3 on the Mac), and tnlog-group's directory held 401 MB after
close against 197 MB without compaction (tndb-group 320 MB). Likely the run ends before a compaction
(with flushed catch-up rounds) can finish, and close cancels it; the extra disk is either untrimmed
preallocated padding (a close that did not commit cleanly would mean a latched failure) or files of
a cancelled compaction. Diagnose with the time-based churn run and its per-column file listing, then
fix before production.

**M5. tnlog writes block tokio workers** (measured; Linux makes it worse: write call p50 220–250 µs,
p99 1.4–1.6 ms at N=50, with lock waits averaging 50 µs and holds 13 µs, so ~150 µs of the median is
outside the lock: profile on Linux first).
- **Detail:** the environment's write lock is held for p50 73 µs and p99 ~1.1 ms in the mixed bench. Vote handlers and the proposer call `insert` from async tasks.
- **Fix:** frame and CRC records before taking the lock, and keep growth off it (README "Not yet"). Re-measure with `workload_mixed`.

**M6. ~~Every read verifies the record's CRC~~ — dropped after the Linux run (2026-10-12).** On both
machines tnlog's reads match or beat raw MDBX (256 KB: Linux 40 vs 40–44 K gets/s, Mac 88 vs 63 K;
16 KB Linux 1.3M vs 0.96M). The layered MDBX column read ~2x faster only because its memory layer
still held rows its slow disk writes had not reached. The per-read CRC stays.

*(original text, for the record)* Every read verifies the record's CRC (measured; cause suspected).
- **Detail:** `checked_payload_in` (`pack.rs:781`). Batch-cache `multi_get` runs at 55–65% of production's layered MDBX.
- **Fix:** measure first, by running the reads without the CRC. If it's confirmed, verify once per record instead of on every read. That trades some at-rest rot detection, so it's your call.

**M7. tnlog has no production metrics.**
- **Detail:** its stats are `cfg(test)` only.
- **Fix:** export sync latency, unsynced bytes, lock waits, compactions (duration, switch hold, abandons and causes) and latched failures through the node's metrics. Alert on latches and on repeated abandons.

**M8. Today's default backend: `LayeredDatabase`'s write queue is unbounded** (`layered_db.rs:411`).
- **Detail:** beside the unpaced 256 KB stream, the bench process reached 39 GB resident.
- **Fix:** bound the queue only if MDBX stays the default for a while; the layer goes away with tnlog.

## Low–medium

**LM1. [FIXED 2026-10-12: gen-0 is built in a `compact-0-*` directory and renamed into place; test `test_tnlog_interrupted_creation_reopens`.] Creating gen-0 isn't atomic** (confirmed).
- **Where:** `store.rs:1054`.
- **Scenario:** a crash or a file-creation error (EMFILE/ENOSPC) between the directory and the log leaves a log-less gen-0. Every later open fails "has no log".
- **Fix:** build gen-0 in a `compact-0-*` directory, sync it, rename it, sync the root.

**LM2. [FIXED 2026-10-12: `record::put_value` checks kind, table and key on gets, scans and the compaction copy; tests `test_tnlog_misindexed_row_is_corrupt_not_another_value`, `test_tnlog_record_put_value_checks_the_row`; no read cost measured (batch-cache gets/s equal, 10 interleaved pairs).] A read doesn't check that the record at an index position is the requested row** (plausible).
- **Where:** `value_at` (`store.rs:136`) and the compaction copy.
- **Scenario:** a damaged index position that lands on another record returns a wrong value, or panics in `decode`; compaction then copies the mismatch.
- **Fix:** check kind = PUT, the table id, and `key == payload[HEAD..]`, a ~32-byte compare. A mismatch becomes `CorruptIndex`.

## Low
- **L1. A rolled-back batch can come back after a power loss:** a concurrent msync or writeback may have flushed its page before the rollback zeroed it (`store.rs:525`). Fix: msync the zeroed range on rollback (a rare path), or latch.
- **L2. A panic inside `rollback` (or in the publish/frontier step) leaves the writer half-reverted and not latched** (`store.rs:963-983`). Fix: latch first, and `catch_unwind` the rollback.
- **L3. A poisoned index stops that table's writes, but the database carries on.** Fix: `stop` the writer when a rollback follows an I/O error or touches a poisoned index.
- **L4. A dropped pack marked `remove_on_drop` still msyncs** (`pack.rs:396`). A close waits on a cancelled compaction's flush. Fix: skip the commit when `remove_on_drop` is set.
- **L5. `install_gen` CRC-stamps every builder index page under the write lock** (`store.rs:613`). Fix: publish builder indexes after each background round.
- **L6. Values over 16 MiB (`MAX_RECORD_SIZE`) are refused;** MDBX had no limit. A large `ConsensusCache` output could exceed it (state-sync logs and refetches). Fix: test and document the limit, or chunk.
- **L7. Edge cases:**
  - in Sync mode, a no-op write returns before the state it saw is durable;
  - `persist` first polled after the last handle dropped can hang (its stop branch is unreachable).

  Fix: capture the persist target eagerly, and fail pending waiters when the commit thread exits.
- **L8. Thread-local `BUFS` never shrink;** each keeps the largest value it ever wrote. Fix: shrink past a few MiB.
- **L9. [FIXED 2026-10-13: tndb deleted; tnlog owns `CommitMode`, `CompactionConfig`, its layout helpers (`tnlog/layout.rs`) and the compaction constants; the archive's sync-ticket code and two test helpers only tndb used went with it; bench columns moved to tnlog.] tnlog depends on tndb;** `pub mod tndb` ships about 6.4K lines. Fix (S4): move `CommitMode`, `CompactionConfig`, the layout helpers and the constants into tnlog; delete tndb.
- **L10. No CI test lane runs the node on tnlog;** only the `--all-features` clippy pass compiles it. Fix: a nightly lane (DB-using crates plus e2e epoch and restart tests with `tn-storage/tnlog`), until it becomes the default.
- **L11. Address-space reservation:** 16 GiB per log and 1 GiB per index (~62 GiB at rest, more during compaction), shared mappings, not commit-charged; falls back under `ulimit -v`. Fix: document; consider 4 GiB log reserves.
- **L12. `has_dead` is assumed true at every open,** so a restart may cost one unneeded compaction. Optional: keep a dead-row count in `compacted`.
- **L13. No inspection tool** (MDBX has `mdbx_stat`). Fix: a `db tnlog-stat` subcommand; it also serves M7.
- **L14. Cosmetic:**
  - the `Core::lock` doc comment is attached to the wrong field;
  - `persist`'s dead branch;
  - `table_ksize` is a linear scan;
  - the `base` written to disk differs slightly from the in-memory one.

## Tests missing
- **Power loss:** a model dropping unsynced bytes (H1), including crash → reopen → `persist` → drop the bytes from before the first crash.
- **Generations and disk:**
  - a damaged newest generation with an older one present (M1);
  - free space dropping mid-copy (M2);
  - gen-0 creation failure or crash (LM1).
- **Damage, rollback and limits:**
  - index damage: `remove` on a damaged index (M3), a wrong-key position (LM2);
  - a rollback after a concurrent msync (L1);
  - a panic in `revert_to_published` (L2);
  - a poisoned index growth (L3);
  - a value over 16 MiB (L6);
  - close latency with a large unsynced builder (L4).
- **Concurrency and scale:**
  - a multi-threaded stress test (writers, scans, `persist`, Sync leaders and switches together); today's randomized model is single-threaded;
  - `SplitDatabase` crash and reopen of all three environments, and a `SplitTxMut` commit where one environment fails;
  - log growth past the 16 GiB reservation while rounds and readers run;
  - the H2 migration.

## Reviewed and sound
- **tnlog (reviewer):**
  - lock order (registry → write; commit thread and compactor take no writer lock);
  - no lost wakeups;
  - `seq` before publish; the growth count before the frontier;
  - switch durability (new log synced before the rename, root before the frontier); `SwitchLatch`;
  - generation lifetimes;
  - the net-effect catch-up and row check;
  - replay rules (batch end, tear vs marker), with no panics on disk data;
  - manifest atomicity;
  - rollback invisible to readers;
  - close and drop order;
  - fixed-size keys for every production table.
- **`data_file.rs`:**
  - `MapView` publication order; retired mappings never unmapped while shared;
  - no view retirement for today's packs (only reserved tnlog and B+tree files share views);
  - every barrier calls `sync_size_if_grown`;
  - the commit marker is never stamped past the synced watermark.
- **`pack.rs`:**
  - a failed append rolls back exactly;
  - the decoded-size cap is still enforced before writing (compressed and raw);
  - one-pass CRC reads;
  - `commit(&self)`.
- **Codec, `sealed_batch`, `exec_state_pack`:** byte-identical encodings, pinned by oracle tests at the ULEB128 boundaries, so batch digests are unchanged.
- **Certificate store:** notifies only after the transaction ends, consistent with drop-commits.
- **`MemDatabase`:** iterators held the table lock before this branch too; no new deadlock.
- **Features:** tnlog > redb > MDBX priority; all 14 tables opened; `sync_persist` no-op matches its contract.
- **Pre-existing, outside storage:**
  - 25 `tn-primary` fork-env tests fail under single-process `cargo test` (the Makefile notes it);
  - the `tn-node` readiness test is flaky (~25%) with either backend.

## Action order after the Linux benches (re-ranked with the Linux results, 2026-10-12)
H1, H2; C1 once diagnosed; M1–M3, LM1–LM2; M4 (C1 may be its symptom); M5 (profile on Linux); M7;
the rest. M6 dropped. Original order, for reference:
1. **H1**, the data-file fix plus the power-loss test. It touches today's packs too, so it's worth doing even if tnlog waits.
2. **H2**, the MDBX → tnlog migration.
3. **M1–M3 and LM1–LM2**: small correctness hardening, each test-first.
4. **M4**, compaction under sustained load, sharing M2's guard work.
5. **M7**, metrics, and **L13**, the inspection tool.
6. **M5 and M6**, performance, prioritized by the Linux numbers.
7. **L1–L8.**
8. **L9**, S4 cleanup: delete tndb.
9. **L10**, the CI lane.
10. **M8**, only if MDBX stays the default.

**Each fix:** a test that fails first; a mutation check for durability fixes; `workload_*` before and after for the performance items; e2e epoch and restart tests on a tnlog binary at the end. No git writes.

**L15 (added 2026-10-12, after M4).** Under extreme churn the B+tree index file grows while a
compaction's copy pins its snapshot: pages superseded meanwhile can't be reused (102 MB for 2,048
rows on the Mac at ~280 MB/s of churn). Each switch replaces it with a fresh index, so it is bounded
per compaction; production write rates make it negligible. Possible fix: copy unpaced when the live
set is small (shorter pin), or let the index reuse pages that no live snapshot can reach.

