# Memory-Leak Remediation (OOM on 16GB nodes)

Plan reference: `memory-audit-report.md` (repo root). One commit per phase on `fix/memory-leak-remediation`; no wire/protocol changes in phases 1–3.

## Phase 0 — Report + tracking
- [x] Write audit report to `memory-audit-report.md`
- [x] Create this checklist

## Phase 1 — F1 root cause (`crates/storage/src/layered_db.rs`) — commit 3c69302d
- [x] Only push `committed_inserts` when `mem_db.is_some()` in `db_run`'s Insert arm
- [x] Add `LayeredDbStats { retained_inserts, open_txn_count }` + `DBMessage::Stats` + sync `stats()` accessor
- [x] Aggregate `stats()` on `CompositeDatabase` (per-DB stats)
- [x] Test: `test_layereddb_full_memory_does_not_retain_inserts` (100 txn commits → stats {0,0}, keys readable)
- [x] Test: `test_layereddb_cache_mode_clears_mem_after_commit` (retained 0 AND iter count == K, not 2K)
- [x] Regression proof: test fails on unpatched code with `retained_inserts: 100`, passes with fix

## Phase 2 — F2 txn-drop wedge (`crates/storage/src/layered_db.rs`) — commit 7ed8826d
- [x] `DBMessage::EndTxn`; `TxnGuard { tx, committed: AtomicBool }` with Drop → EndTxn iff not committed
- [x] `LayeredDbTxMut.guard: Arc<TxnGuard>`; `commit()` swaps committed before sending CommitTxn (exactly-once across clones)
- [x] `db_run`: extract `end_txn(...)` helper; CommitTxn and EndTxn (warn) both call it
- [x] Test: dropped-txn-then-commit persists both writes to raw MDBX handle
- [x] Test: overlapped txn drop keeps count balanced (no premature commit of shared physical txn)
- [x] Test: clone+commit+drop sends exactly one end message (open_txn_count == 1 pins it)
- [x] Test: composite-level dropped-txn recovery

## Phase 3 — F4 capacity caps (`consensus_bus.rs`) — commit 2b292af2
- [x] Named consts: `sync_output` 10_000 → 1_000; `exex_certificates` → 1_000; `exex_consensus_output` → 100
  (plan's "consensus_header" channel is `sync_output` in current code)
- [x] Leave `sequence` QueChannel (10_000) alone; noted in commit message for separate review

## Phase 4 — F3 engine backlog bound — commit 2ab05797 (ships separately; changes lag behavior)
- [x] `engine/src/lib.rs`: `MAX_QUEUED_OUTPUTS = 8`; gate stream poll in select (engine is now an async `run()` loop — gate maps onto the select branch condition)
- [x] `node.rs`: `to_engine` capacity 1000 → 64 (`TO_ENGINE_CAPACITY`)
- [x] `run_epoch.rs`: `check_output_continuity(last_forwarded, number) -> {Stale, Next, Gap}` in `wait_for_epoch_boundary` (skip stale, error on gap); NOT in `process_output`
- [x] node.rs startup prime: `last_forwarded_consensus_number` from `last_consensus_parent` (pack ground truth) instead of the slot-file hint — a stale hint would trip the gap check with a spurious error on the first live output (crash loop)
- [x] Engine gate IT test (poll! with queue pre-filled past the bound; stream not drained at bound — fails with "drained 2" without the gate; drains to completion; every output executed exactly once, in order via EngineUpdate stream)
- [x] Unit tests for `check_output_continuity` (Stale/Next/Gap + genesis + u64::MAX edges)

## Phase 5 — F5 peer hygiene — commit 5b174d15 (separable)
- [x] `published_to_peers`: HashSet → `BannedPeerCache` time-LRU (1h TTL, refreshed on reconnect; lazy eviction on PeerConnected — the only growth source)
- [x] `known_peers` staleness eviction in heartbeat (36h = 3× kad publication interval; never evicts prev/current/next committee or important peers) + test
- [x] Pair `add_explicit_peer` with `remove_explicit_peer` on PeerDisconnected and Banned

## Verification
- [x] `cargo test -p tn-storage -p tn-engine -p tn-node` — all green (123 + 1 + 9 IT + 13 + 4 IT)
- [x] `cargo test -p tn-network-libp2p` (195), tn-primary consensus_bus (19) + storage IT
- [x] `cargo check --workspace` clean; clippy clean on all touched crates (default features; `--all-features` E0425 on `adiri` pre-exists on main); `cargo fmt` applied
- [x] Regression proofs demonstrated for F1 (retained_inserts: 100 → 0) and F3 gate (drained 2 → 0)
- [x] Mini-soak: production `CertificateStore::write` path over `open_db` composite stack, stats flat-zero on all three layers — commit f523cb0b
- [ ] Full-network soak via `spawn_local_testnet`: NOT done — the e2e harness spawns nodes on separate runtimes and only exposes RPC endpoints; internal `CompositeDatabase::stats()` handles are unreachable without a new stats RPC/metric. Follow-up: export `retained_inserts`/`open_txn_count` as Prometheus gauges, then assert via metrics in e2e.
- [ ] Prod validation (user): canary one node; watch RSS slope 48–72h vs unpatched; alert on `tn_engine_queued_outputs` growth. Expected: flat RSS after warm-up.

## Review

**Root cause (F1)** is a two-line logic gap with outsized effect: `db_run` retained a boxed clone
of every txn insert solely so cache-mode could clear its mem mirror after commit, but full-memory
DBs (epoch/kad) pass `mem_db = None` so the drain never ran — a permanent, unbounded `Vec` of every
certificate ever written. The fix gates retention on `mem_db.is_some()`; `stats()` makes any
regression observable.

**Deviations from plan (all verified against current code):**
1. The plan's `consensus_header` broadcast is `sync_output` in current code (renamed upstream);
   same channel, same rationale.
2. The engine was refactored (bcc9dc4b) from a hand-rolled `Future` to an async `run()` loop, so
   the Phase 4 gate landed as a `tokio::select!` branch condition instead of a poll-gate — same
   semantics, and the pre-fill for the gate test needs `MAX_QUEUED_OUTPUTS + 1` because the loop
   moves one output into the in-flight slot before polling the stream.
3. Added (not in plan): startup prime of `last_forwarded_consensus_number` switched from the
   slot-file hint to `last_consensus_parent` pack ground truth. Without this, the new gap check
   could crash-loop a node after a hard crash left the slot files stale-low. This also fixes a
   pre-existing leftover-drain double-forward edge on the same staleness.
4. Phase 5 `published_to_peers` used the time-LRU option (not plain remove-on-disconnect) to
   preserve the deliberate anti-flapping dedup; eviction is lazy on connect events since the
   network loop has no free-standing timer.

**Risk notes for review:** Phase 4 intentionally converts silent lag-skip into a detected error +
restart replay — needs explicit sign-off before deploy. `remove_explicit_peer` on disconnect means
gossipsub no longer re-dials disconnected committee peers; the peer manager's committee dialing
owns reconnection (verified machinery exists: `prepare_committee_dial`, discovery heartbeat,
`MissingAuthorities`).
