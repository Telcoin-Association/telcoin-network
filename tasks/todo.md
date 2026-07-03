# Memory-Leak Remediation (OOM on 16GB nodes)

Plan reference: `memory-audit-report.md` (repo root). One commit per phase; no wire/protocol changes in phases 1–3.

## Phase 0 — Report + tracking
- [x] Write audit report to `memory-audit-report.md`
- [x] Create this checklist

## Phase 1 — F1 root cause (`crates/storage/src/layered_db.rs`)
- [ ] Only push `committed_inserts` when `mem_db.is_some()` in `db_run`'s Insert arm
- [ ] Add `LayeredDbStats { retained_inserts, open_txn_count }` + `DBMessage::Stats` + sync `stats()` accessor
- [ ] Aggregate `stats()` on `CompositeDatabase` (per-DB stats)
- [ ] Test: `test_layereddb_full_memory_does_not_retain_inserts` (100 txn commits → stats {0,0}, keys readable)
- [ ] Test: `test_layereddb_cache_mode_clears_mem_after_commit` (retained 0 AND iter count == K)
- [ ] Prove regression test fails on unpatched code, passes with fix
- [ ] Commit

## Phase 2 — F2 txn-drop wedge (`crates/storage/src/layered_db.rs`)
- [ ] `DBMessage::EndTxn`; `TxnGuard { tx, committed: AtomicBool }` with Drop → EndTxn iff not committed
- [ ] `LayeredDbTxMut.guard: Arc<TxnGuard>`; `commit()` swaps committed before sending CommitTxn
- [ ] `db_run`: extract `end_txn(...)` helper; CommitTxn and EndTxn (warn) both call it
- [ ] Test: dropped-txn-then-commit persists both writes to raw MDBX handle
- [ ] Test: overlapped txn drop keeps count balanced
- [ ] Test: clone+commit+drop sends exactly one end message
- [ ] Test: composite-level dropped-txn recovery
- [ ] Commit

## Phase 3 — F4 capacity caps (`crates/consensus/primary/src/consensus_bus.rs`)
- [ ] Named consts: `sync_output` 10_000 → 1_000; `exex_certificates` → 1_000; `exex_consensus_output` → 100
- [ ] Leave `sequence` QueChannel (10_000) alone; note in PR for separate review
- [ ] Commit

## Phase 4 — F3 engine backlog bound (engine + node crates)
- [ ] `engine/src/lib.rs`: `MAX_QUEUED_OUTPUTS = 8`; gate stream poll in select
- [ ] `node.rs`: `to_engine` capacity 1000 → 64 (named const)
- [ ] `run_epoch.rs`: `check_output_continuity(last_forwarded, number) -> {Stale, Next, Gap}` in `wait_for_epoch_boundary` (skip stale, error on gap); NOT in `process_output`
- [ ] Engine gate test (poll! with pre-filled queue; stream not drained at bound; drains to completion; each output executed once)
- [ ] Unit tests for `check_output_continuity` (Stale/Next/Gap)
- [ ] Commit (ships separately — changes lag behavior, needs review sign-off)

## Phase 5 — F5 peer hygiene (optional, separable)
- [ ] Bound `published_to_peers` (time-LRU or remove on disconnect)
- [ ] `known_peers` staleness eviction (never current committee/trusted)
- [ ] Pair `add_explicit_peer`/`remove_explicit_peer`; clean blacklist on ban-prune
- [ ] Commit

## Verification
- [ ] `cargo test -p tn-storage -p tn-engine -p tn-node` + check/clippy/fmt on touched crates
- [ ] Regression proof documented (fail-on-main / pass-with-fix)
- [ ] Review section added below

## Review
(to be filled in when work completes)
