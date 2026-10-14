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
`migrate_test`. 2026-10-13 decision (user): keep the migration for the release that switches
backends, then delete it and the MDBX consensus backend; it now also carries `OurNodeBatchesCache`
(own uncommitted batches, no other copy). Abandoning MDBX instead was ruled out: safe only while
the network keeps making rounds (a rolling upgrade) or at an epoch boundary; a stop-all halt
returns every node to the same round with no vote or proposal memory (two certificates for one
author and round).] Switching a node from MDBX to tnlog mid-epoch loses its anti-equivocation
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

**M5. tnlog writes block tokio workers** — **CLOSED 2026-10-13 after the Linux QUICK baseline: not
a production cost, and the "outside the lock" reading was a comparison error.**
- **This run:** the vote write call was p50 238 µs / p99 1.48 ms at N=50. That compares one call
  type's median with lock waits (54 µs) and holds (13 µs) averaged over every write: certificates,
  their indexes, payload rows, headers and votes, about 347 a round, of which votes are 50.
- **Why votes wait longer:** the bench starts all 50 vote tasks at once on 16 workers, so the votes
  queue on the lock together. Each handoff wakes a parked thread, which fits a vote median near
  240 µs inside the wait.
  - Outside the lock a write does only a lookup, encoding, an atomic fence and sometimes a condvar
    notify.
  - This is inferred, not timed per call.
- **Production cost:** about 50 votes a second at N=50, at about 0.25 ms each, is about 12 ms of one
  worker per second.
- **Full analysis:** the root `tndb-linux-unpaced.md`, section 3.

*(original text, for the record)* (measured; Linux makes it worse: write call p50 220–250 µs,
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

**M7. [FIXED 2026-10-13: `tn_storage_tndb_*` metrics (batches, sync latency, unsynced and log bytes, generation, write-lock stalls, compactions by outcome, abandons by cause, compaction and switch durations, the `failed` latch), labeled by `env`, recorded at events through handles made at open; alerts in the tndb README; no write-path cost measured. Tests `test_tndb_metrics_*`, `test_tndb_compaction_abandon_causes`.] tnlog has no production metrics.**
- **Detail:** its stats are `cfg(test)` only.
- **Fix:** export sync latency, unsynced bytes, lock waits, compactions (duration, switch hold, abandons and causes) and latched failures through the node's metrics. Alert on latches and on repeated abandons.

**M8. [SKIPPED 2026-10-13 (user): tndb replaces the layered MDBX path.] Today's default backend: `LayeredDatabase`'s write queue is unbounded** (`layered_db.rs:411`).
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
- **L1. [FIXED 2026-10-13: the rollback flushes its zeros; tests `test_tndb_rollback_flushes_the_zeroed_batch`, `test_tndb_power_loss_never_brings_back_a_rolled_back_batch`.] A rolled-back batch can come back after a power loss:** a concurrent msync or writeback may have flushed its page before the rollback zeroed it (`store.rs:525`). Fix: msync the zeroed range on rollback (a rare path), or latch.
- **L2. [FIXED 2026-10-13: latch before the rollback, which runs under `catch_unwind`; an unwind latch over the publish; tests `test_tndb_panic_in_rollback_latches`, `test_tndb_panic_in_publish_latches`.] A panic inside `rollback` (or in the publish/frontier step) leaves the writer half-reverted and not latched** (`store.rs:963-983`). Fix: latch first, and `catch_unwind` the rollback.
- **L3. [FIXED 2026-10-13: a rollback touching a poisoned index stops the writer; test `test_tndb_poisoned_index_latches`.] A poisoned index stops that table's writes, but the database carries on.** Fix: `stop` the writer when a rollback follows an I/O error or touches a poisoned index.
- **L4. [FIXED 2026-10-13: `PackInner::drop` skips its commit for a file to be removed; test `dropping_a_pack_marked_for_removal_does_not_sync`.] A dropped pack marked `remove_on_drop` still msyncs** (`pack.rs:396`). A close waits on a cancelled compaction's flush. Fix: skip the commit when `remove_on_drop` is set.
- **L5. [FIXED 2026-10-13: `Builder::sync` publishes the new indexes each round; test `test_tndb_compaction_stamps_index_pages_before_the_switch` (80 unpublished pages at the switch before, at most 8 after); no measurable switch-hold change at the churn bench's 2,048 rows, the saving grows with the index (about 1 µs per page).] `install_gen` CRC-stamps every builder index page under the write lock** (`store.rs:613`). Fix: publish builder indexes after each background round.
- **L6. [FIXED 2026-10-13 (user: a larger cap for tndb's log): the record cap is per pack (`MmapFileOptions::max_record_size`), tndb's log opens with 1 GiB, other packs keep 16 MiB; tests `per_pack_record_cap`, `test_tndb_values_past_16_mib`.] Values over 16 MiB (`MAX_RECORD_SIZE`) are refused;** MDBX had no limit. A large `ConsensusCache` output could exceed it (state-sync logs and refetches). Fix: test and document the limit, or chunk.
- **L7. [FIXED 2026-10-13: a `Sync` no-op waits for the last batch; `persist` reads its target when called and fails once the commit thread exits; tests `test_tndb_sync_no_op_waits_for_the_state_it_saw`, `test_tndb_persist_fails_once_the_commit_thread_stops`.] Edge cases:**
  - in Sync mode, a no-op write returns before the state it saw is durable;
  - `persist` first polled after the last handle dropped can hang (its stop branch is unreachable).

  Fix: capture the persist target eagerly, and fail pending waiters when the commit thread exits.
- **L8. [FIXED 2026-10-13: a buffer past 4 MiB is let go after use; test `test_tndb_encode_buffers_shrink_after_a_large_value`.] Thread-local `BUFS` never shrink;** each keeps the largest value it ever wrote. Fix: shrink past a few MiB.
- **L9. [FIXED 2026-10-13: tndb deleted; tnlog owns `CommitMode`, `CompactionConfig`, its layout helpers (`tnlog/layout.rs`) and the compaction constants; the archive's sync-ticket code and two test helpers only tndb used went with it; bench columns moved to tnlog.] tnlog depends on tndb;** `pub mod tndb` ships about 6.4K lines. Fix (S4): move `CommitMode`, `CompactionConfig`, the layout helpers and the constants into tnlog; delete tndb.
- **L10. [DEFERRED 2026-10-13 (user): no CI lane until tndb nears becoming the default.] No CI test lane runs the node on tnlog;** only the `--all-features` clippy pass compiles it. Fix: a nightly lane (DB-using crates plus e2e epoch and restart tests with `tn-storage/tnlog`), until it becomes the default.
- **L11. [DOCUMENTED 2026-10-13: the tndb README's Limits and choices; reservations unchanged.] Address-space reservation:** 16 GiB per log and 1 GiB per index (~62 GiB at rest, more during compaction), shared mappings, not commit-charged; falls back under `ulimit -v`. Fix: document; consider 4 GiB log reserves.
- **L12. [WON'T FIX 2026-10-13: at most one compaction after a restart, and only past the threshold and twice the base; documented in the tndb README.] `has_dead` is assumed true at every open,** so a restart may cost one unneeded compaction. Optional: keep a dead-row count in `compacted`.
- **L13. [FIXED 2026-10-13: `telcoin-network db tndb-stat` (`tndb::inspect` / `inspect_split`): read-only, refuses an open database via a shared lock; tests `test_tndb_inspect_*`, CLI `db_tndb_stat_*`.] No inspection tool** (MDBX has `mdbx_stat`). Fix: a `db tnlog-stat` subcommand; it also serves M7.
- **L14. [FIXED 2026-10-13: the `Core::lock` comment moved; `persist`'s stop branch is live (L7); `table_ksize` documented (one call per table per compaction, cached); the in-memory `base` is the written one (test `test_tndb_compaction_base_matches_the_written_one`).] Cosmetic:**
  - the `Core::lock` doc comment is attached to the wrong field;
  - `persist`'s dead branch;
  - `table_ksize` is a linear scan;
  - the `base` written to disk differs slightly from the in-memory one.

## Tests missing

> **[2026-10-13] All covered:** the power-loss model (`archive::data_file::disk_model`, tests `test_tndb_power_loss_*` and `test_tndb_random_power_losses_against_model`); M1, M2, LM1, M3, LM2 with their fixes; L1–L4 and L6 with theirs; the multi-threaded stress test (`test_tndb_concurrent_stress`); `SplitDatabase` crash and partial failure (`test_tndb_split_crash_reopens_every_environment`, `test_tndb_split_txn_with_a_failing_environment`); log growth past the reservation (`test_tndb_log_grows_past_its_reservation`); the H2 migration (`migrate_test`).
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

**L15 (added 2026-10-12, after M4). [DEFERRED 2026-10-13: needs a Linux measurement (copy pacing interacts with I/O stalls); noted in the tndb README. Linux QUICK baseline 2026-10-13: 104.8 MB index for 2,048 rows at about 100K churn steps a second; bounded per compaction; no fix needed at production rates. Kept deferred.]** Under extreme churn the B+tree index file grows while a
compaction's copy pins its snapshot: pages superseded meanwhile can't be reused (102 MB for 2,048
rows on the Mac at ~280 MB/s of churn). Each switch replaces it with a fresh index, so it is bounded
per compaction; production write rates make it negligible. Possible fix: copy unpaced when the live
set is small (shorter pin), or let the index reuse pages that no live snapshot can reach.


## Second review (2026-10-13): tndb as it stands

**Scope:**
- every tndb source file; its 74 tests checked by name for coverage;
- the tndb README;
- the node's open path and the MDBX migration (`lib.rs` `_open_tndb`, `migrate_from_mdbx`,
  `copy_table`).

The storage README's "Intentional design decisions" and "Reviewed and sound" above were not
re-raised.

**Re-checked and sound:**
- the lock order (registry → write; the commit and compaction threads take no writer lock);
- the commit thread's idle handshake (SeqCst fences on both sides);
- `wait_durable` (no lost wakeups: `advance` needs the waiter's mutex);
- sync rounds across a switch (the new log is synced before the rename, so advancing to an older
  frontier's sequence number is safe);
- `install_gen` and generation lifetimes; replay and recovery; manifest atomicity;
- `record_prior_to` (strictly-less, as MDBX's);
- the migration marker (durable before the node writes anything).

**N1. [FIXED 2026-10-13: `CompactShared::request_auto` checks `running` again under the state lock; test `test_tndb_abandoned_compaction_waits_out_its_backoff` (a test-only pause in `request_auto`), which failed first.] An abandoned compaction was retried at once, skipping its backoff.**
- **The race:** `request_auto` checked `running` before taking the state lock but not after.
  1. A writer that saw no compaction running took the lock just after one started, and set
     `wanted` again.
  2. When that compaction was abandoned, `run` set the backoff, found `wanted` set and started
     another at once.
- **Once per failure streak:** once the backoff was set, a racing request was turned away.
- **Effects:**
  - one wasted immediate retry, at the worst time (a burst, a near-full disk);
  - the backoff doubled a step early;
  - after a successful switch, a needless snapshot under the write lock.
- **The Linux run's open question:** it explains the 2 abandons per run under the unpaced burst.

**N2. [FIXED 2026-10-13: test `test_tndb_txn_dropped_while_panicking_writes_nothing`, which failed first.] A write transaction dropped while a panic unwinds committed its partial writes.**
- **The risk:** with the default panic strategy (unwind), a panic between related writes in one
  transaction (a certificate and its digest-index rows) committed half of them, silently.
  `LayeredDatabase` also commits a dropped transaction (with a warning); raw MDBX aborts it.
- **Now:**
  - a transaction dropped while `std::thread::panicking()` writes nothing (logged as an error);
  - one dropped without a commit otherwise is written as before, with a warning;
  - an empty one does nothing.

**N3. [FIXED 2026-10-13 (user: delete, not rename): tests `test_migrate_from_mdbx_keeps_epoch_and_kad_tables` (extended), `test_migrate_from_mdbx_deletes_only_mdbx_files`.] The MDBX directories stayed after the migration,** so deleting `consensus-db/tndb` later would re-import the epoch state from the time of the switch (possibly epochs old).
- **The fix:** once the copy is durable, and before the marker, `migrate_from_mdbx` deletes each
  environment it read.
  - It removes MDBX's own files (`mdbx.dat`, `mdbx.lck`), then the directory with `remove_dir`. A
    file MDBX did not make keeps its directory and is logged; nothing is removed recursively.
  - It then syncs the consensus DB directory, then writes the marker.
- **Crashes:** a crash part way reruns the copy for what is still present.
- **Mutation check:** a recursive delete is caught.
- **Trade-off:** a downgrade to the MDBX build after the switch now starts with empty epoch tables.

**N4. [FIXED 2026-10-13.] Nit:** a duplicated doc comment on `split.rs` `ENVIRONMENTS`.

**Observations, no action:**
- A switch holds the registry lock through its pre-lock catch-up and sync. That blocks only a
  table's first-ever row and new table registration, which happen at startup in production.
- A catch-up reads the old log through the snapshot's view. A log that outgrows its 16 GiB
  reservation during a compaction would make the compaction abandon (a bounds error), not misread.
- `TnDbTxMut::get` scans the transaction's writes linearly; production transactions are small.
