# `consensus.rs` + `consensus_pack.rs` review — performance, security, correctness

Scope: `crates/storage/src/consensus.rs` (`ConsensusChain`, `LatestConsensus`, staging/import) and
`crates/storage/src/consensus_pack.rs` (`ConsensusPack`, the pack actor loop, save/read/serve/decode
paths). Builds on the archive-layer review (`archive_review.md`), which covers everything below
`archive::pack` / `archive::data_file` / the indexes.

Method: re-read the crate `README.md` and excluded the 16 documented "Intentional design decisions".
Focus beyond the archive layer: the background-thread actor model, concurrency/locking, epoch
handoff, staging/import, the `recent_packs` cache, the untrusted-stream decode/verify paths, and the
crash/recovery interaction. Both files' production paths were scanned for panics/unwraps — clean
(only an unreachable `IndexPositions::encode` length-guard `panic!` fed the internal scratch buffer,
and a `_for_test` helper `expect`).

## Summary

Both files are mature and defensively written. The actor model (one background thread per pack and
per latest-consensus slot writer, all `Inner` access serialized behind a channel) removes data races;
the `parking_lot` guards are consistently "lock → clone the `Arc` handle → drop the guard → `await`
on the clone", and the only cross-`await` critical sections use the async `pack_install` mutex with a
documented lock order. The verification posture is strong. **No high-severity issues found.**
Findings are low / low-med; the non-atomic write path (C1) and the blocking-drop pattern (P1) are the
most actionable.

---

## Findings

### C1 — Correctness (Low-Med): `save_consensus_output` is not atomic across data append + index updates

`Inner::save_consensus_output` (`consensus_pack.rs:1509`) appends the header + batch records, then
updates the digest and position indexes. An error in a later step (an index `save` hitting
`WriteDataError`/`CrcError`, e.g. disk-full mid-op or a split rollback) returns *after* the data
records are already appended, without rolling the data file back.

This is safe across a **restart**: `recover_pack` re-indexes the complete-but-unindexed output from
the WAL, and the executor's replay hits the idempotent "already saved" path
(`consensus_pack.rs:1525-1530`). The residual risk is an **in-process retry** of the same output:
`consensus_pos_idx.len()` is unchanged after the failure, so a retry re-appends duplicate header +
batch records; a later `recover_pack` replay then assigns two sequential indexes to one consensus
number, mis-sequencing the number→position map. Reachable only if the caller retries a failed save
*without* restarting (the executor most likely treats a save failure as fatal — worth confirming).

**Recommendation:** make the write atomic — capture `self.data.file_len()` at entry and
`self.data.truncate(start)` on any error before returning, so a failed save leaves the pack exactly
as before (the in-memory index changes are unsynced and rebuilt from the WAL, so only orphan data
records matter). Alternatively, dedupe by consensus number during the `recover_pack` replay.

### P1 — Performance (Low-Med): `Drop` joins a background thread (a blocking fsync) on the async runtime

`ConsensusPack::drop` (`consensus_pack.rs:205`) and `LatestConsensus::drop` (`consensus.rs:68`) send
`Shutdown` and `handle.join()`; the pack thread's shutdown runs `inner.persist()` and the
`MmapDataFile` clean-close (truncate padding + write sentinel + fsync). When the **last** reference
drops on a tokio worker thread — the old writable pack at the end of `new_epoch`
(`consensus.rs:453-457`, `old_pack` drops at scope exit) and the staging pack in `clear_staging`
(`consensus.rs:731-737`) — that blocking join stalls the async worker. Mitigated because both paths
`persist().await` first, so the drop-time fsync is just the small clean-close sentinel; it is still a
blocking fsync on the runtime thread.

**Recommendation:** move the final drop off the worker (`tokio::task::spawn_blocking` to own/drop the
old handle, or `block_in_place`), or add an explicit `async fn close()` used at these sites.

### R1 — Robustness (Low): the pack / slot background threads have no panic isolation

`run_pack_loop` (`consensus_pack.rs:145`) and the `LatestConsensus` thread (`consensus.rs:159`) call
`inner.*` directly with no `catch_unwind`. A panic in any handler kills the thread, after which every
`tx.send(...)` fails and the pack is wedged (`SendFailed` / `ReceiveFailed` for all future ops). The
`inner` methods are panic-free by construction today (verified), so this is latent — a future
`unwrap` on a read/save/count path would silently wedge the pack rather than fail one op.

**Recommendation:** wrap each message handler in `catch_unwind`, returning a `PackError` on panic
(keeping the pack alive), or add a module-level note/lint pinning the panic-freedom invariant of the
`Inner` methods reachable from the loop.

### R2 — Robustness (Low): `get_static` cache fill is not atomic (duplicate entries / redundant opens)

`get_static` (`consensus.rs:1229`) scans `recent_packs` under the lock, drops it, then `open_static`
+ `push_back` outside the lock. Two concurrent misses for the same epoch both open the pack and both
`push_back`, leaving a duplicate cache entry (redundant `open_static`, an extra background thread for
one epoch, and earlier eviction of other epochs). Harmless correctness-wise (both are valid read-only
packs).

**Recommendation (optional):** dedupe on `push_back` (skip if the epoch is now present), or serialize
per-epoch opens; leave as-is if read concurrency is low.

### R3 — Robustness (Low): `Drop` skips the join when the channel is full

`ConsensusPack::drop` / `LatestConsensus::drop` take the handle, then `if try_send(Shutdown).is_ok()
{ join }`. On a full bounded channel (1000 backlog) `try_send` fails, the handle is dropped without
joining, and the caller does not wait for the final persist. Durability still holds (the detached
thread's `MmapDataFile::Drop` clean-closes when the last `Sender` drops), but the "closed and durable
on return" contract is not upheld under a full queue. Very low likelihood.

### R4 — Robustness (Low, cleanup): stale `import-{N}` dirs are not swept on startup

`ConsensusChain::new` sweeps `staging-{N}` dirs (`remove_all_staging_dirs`, `consensus.rs:651`) but
not `import-{N}` dirs. A crash mid-`stream_import` leaves an orphan `import-{N}` dir until the next
import of that same epoch (`ImportPath::new` removes it first, `consensus.rs:1519`). Disk-leak only.

**Recommendation (optional):** also sweep `import-*` on startup.

### Minor

- `recent_packs` eviction `pop_front()` (`consensus.rs:1243`) drops+joins the evicted pack's thread
  while holding the `recent_packs` mutex; fine today because evicted entries are read-only static
  packs (fast join, no fsync), but it couples a thread-join to the cache lock.

---

## Reviewed — strengths / no change (rationale)

- **Locking model sound.** `current_pack()` / `staging()` clone the `Arc` handle and drop the guard
  (`consensus.rs:1219-1226`); no `parking_lot` guard is held across an `.await`. Cross-`await` epoch
  mutation uses the async `pack_install` mutex with the documented order (before
  `current_pack`/`recent_packs`). `LatestConsensus::update` drops the state lock before its `.await`.
- **Verification posture strong (security).** `verify_epoch_meta` (`consensus_pack.rs:1879`) pins the
  embedded committee's epoch to the record epoch (prevents bcs-layout confusion) and checks the full
  epoch linkage; `iter_to_output` (`:2129`) reads the header first, runs `check_header_expectation`
  before buffering any batch, caps batch count via `max_batches_per_output` (committee-derived), and
  matches batches positionally against sorted expected digests (out-of-order rejected);
  `consensus_header_by_digest` (`:1589`) re-verifies `header.digest() == digest` (guards the
  repaired-DB same-slot edge); `fetch_error_is_absent` (`:1832`) surfaces "exists-but-unreadable"
  instead of a silent `None`.
- **Untrusted stream paths bounded + verified.** `stream_import` / `import_partial_to_staging` go
  through `AsyncPackIter` (MAX_RECORD_SIZE) + `iter_to_output` (batch cap) + `verify_epoch_meta`;
  `ImportPath` is a RAII temp dir with a per-process sentinel + static lock, and staging is cleaned on
  every failure branch (`consensus.rs:680, 700-706`).
- **Chain-layer `save_consensus_output` (`consensus.rs:751`)** rejects non-monotonic numbers and
  wrong-epoch outputs as hard errors (no silent skips), operates on an `Arc` clone, and advances the
  latest-consensus hint only after a successful save.
- **`LatestConsensus`** uses double-buffered slots with CRC + torn-slot fallback
  (`consensus.rs:149-156`); within-epoch writes are intentionally unsynced (README §6,
  non-authoritative hint), with fsync on epoch change / persist / shutdown.
- **`IndexPositions`** entries are CRC-protected and `decode` returns errors, not panics
  (`consensus_pack.rs:2458`); `encode`'s length `panic!` is only ever fed the internal scratch.
- **Read range checks.** `consensus_header_by_number` / `bytes_for_consensus` / `output_end_for_
  consensus` validate against `start_consensus_number` + `pos_idx.len()` (no saturating-to-index-0
  silent misreads).

## Suggested follow-ups (if actioned later; not part of this review)

- **C1:** inject an index-`save` failure mid-`save_consensus_output` and assert the data file length
  is unchanged after the error and a retry does not duplicate records; `open_static` after a rebuild
  reads every output by number in order. `cargo test -p tn-storage --lib`.
- **P1:** move the old-pack / staging-pack drop off the tokio worker; confirm the epoch-handoff and
  staging tests still pass.
