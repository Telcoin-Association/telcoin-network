# Memory-Leak Audit: OOM on 16GB Nodes

**Date:** 2026-07-03
**Symptom:** Production nodes (16GB VMs) leak ~300–600MB/day over ~3 weeks until OOM; a restart
resets usage to ~10%. Growth tracks chain progress, disk usage stays small, and the leak
reproduces on both validators and observers.

Three parallel scans (storage, networking, consensus/engine) produced candidate findings; the
top findings were verified by direct code reading. The root cause (F1) is confirmed with high
confidence: it quantitatively and behaviorally matches the symptom.

---

## F1 — ROOT CAUSE (confirmed): `LayeredDatabase` retains a clone of every certificate ever written

`crates/storage/src/layered_db.rs`

- The background thread `db_run` pushes a boxed **full key+value clone** of every transactional
  insert into `committed_inserts` — unconditionally.
- The only drain is gated on `if let Some(mem_db)` — but `LayeredDatabase::open(db, full_memory=true)`
  passes `mem_db = None` to the thread. **The drain never runs for full-memory DBs.**
- `composite_db.rs`: `epoch_db` and `kad_db` are opened `full_memory=true`. Every certificate
  accepted flows through `CertificateStore::write/write_all` (txn + commit, 3 inserts each:
  `Certificates`, `CertificateDigestByRound`, `CertificateDigestByOrigin`) from
  `cert_manager.rs` — every round, forever.
- Math: 4–10 certs/round × ~1–1.5KB at ~1 round/s ≈ **350–950MB/day**. Matches the observed rate.

The retained box exists solely to clear the cache-mode mirror after commit; it is meaningless
when `mem_db` is `None`.

**Fix:** only push into `committed_inserts` when `mem_db.is_some()`. Add a `stats()` observability
hook (`retained_inserts`, `open_txn_count`) so a regression is visible in tests and soaks.

## F2 — Txn-drop wedge (confirmed, latent): no abort path stops persistence AND leaks

`crates/storage/src/layered_db.rs`

`DBMessage` has only `StartTxn`/`CommitTxn`. If any `write_txn()` handle is dropped without
`commit()`, the background thread's txn count is permanently off — **no commit ever fires again**
on that DB (the count oscillates 1↔2): writes stop persisting to disk and `committed_inserts`
grows forever (even on `cache_db`). Triggers exist on **normal** paths:

- `CertificateStore::delete` early-returns `Ok(())` with the txn open when the cert is missing.
- Error `?`-returns with a live txn in `certificate_store.rs` (write/write_all),
  `batch_fetcher.rs`, and `worker/src/network/primary.rs`.

**Fix:** an abandoned txn ends as **commit + warn**. Writes are already visible in `mem_db`
immediately, there is no rollback machinery, and overlapped logical txns share one physical txn —
an abort would discard innocent writes. Implemented with a shared `TxnGuard` (Arc) whose `Drop`
sends a new `EndTxn` message iff `commit()` was never called; exactly one `StartTxn`/end pair per
logical txn even across clones.

## F3 — Engine backlog (confirmed, conditional): unbounded `queued: VecDeque<ConsensusOutput>`

`crates/engine/src/lib.rs`

The engine loop drains the consensus-output channel into `queued` unconditionally while executing
one output at a time. If execution lags consensus (state grows over weeks; prior incidents of
execution-height divergence exist), `queued` grows without bound — each item carries a full
`CommittedSubDag` + batches. The `tn_engine_queued_outputs` gauge already tracks this.

**Fix:** bound the queue (`MAX_QUEUED_OUTPUTS = 8`) by gating the stream poll, and shrink the
`to_engine` channel (1000 → 64). Backpressure analysis: blocking the `to_engine` send stalls only
the EpochManager forwarder task, not DAG progress; `close_epoch` already waits for execution
catch-up, so no deadlock. BUT beyond the 100-slot `consensus_output` broadcast, a blocked
forwarder gets `Lagged` → warn-and-skip → **silent execution gap**. Backpressure therefore ships
with gap detection: a continuity check on forwarded consensus numbers in
`wait_for_epoch_boundary` that skips stale (already-forwarded) output and errors on a gap so the
restart path replays from the consensus DB (`replay_missed_consensus`) instead of diverging
silently.

## F4 — Bounded-but-large broadcast channels (tuning)

`crates/consensus/primary/src/consensus_bus.rs`

- `sync_output`, `exex_certificates`, `exex_consensus_output` broadcasts use
  `CHANNEL_CAPACITY = 10_000`. `sync_output` items are full `ConsensusOutput`s (subdag + batches)
  → hundreds of MB if a following/catching-up subscriber lags. (`consensus_output` is already 100.)
- **Fix:** `sync_output` → 1_000 (the state-sync producer is execution-throttled; lag hits the
  existing digest-chain fail-fast, not divergence); `exex_certificates` → 1_000;
  `exex_consensus_output` → 100 (ExEx handles `Lagged` natively via `TnExExNotification::Lagged`
  reconciliation).
- Left alone: the `sequence` QueChannel (10_000) — shrinking it backpressures Bullshark's commit
  path directly; flagged for separate review.
- Review-only note: `HdxIndex` bucket-cache backstop is 400k × 1296B ≈ 518MB per index, two per
  pack (`archive/digest_index/index.rs`) — worth revisiting for a 16GB budget; normally small.

## F5 — Slow per-peer leaks (small magnitude, real)

- `published_to_peers: HashSet<PeerId>` — insert-only (`network-libp2p/src/consensus.rs`), no
  removal; grows with peer-identity churn.
- `PeerManager::known_peers: HashMap<BlsPublicKey, NetworkInfo>` — insert-only
  (`peers/manager.rs`).
- `gossipsub.add_explicit_peer` never paired with `remove_explicit_peer`; blacklist entries for
  banned-then-pruned peers also linger.

**Fix (separable):** time-bound `published_to_peers`, staleness-evict `known_peers` (never the
current committee/trusted peers), pair explicit-peer and blacklist adds with removals.

## Ruled out (verified bounded — don't chase these)

- reth `CanonicalInMemoryState` — drained every round via `finalize_block` →
  `remove_persisted_blocks` (`tn-reth/src/lib.rs`).
- Narwhal DAG / vote aggregators / proposer maps — all GC'd by `gc_round`.
- Metrics cardinality — fixed labels only.
- Tx pool — reth defaults, bounded.
- The full-memory mem mirror itself — tracks gc'd disk state.
- KAD store, req/resp pending maps.
- MDBX mmap geometry — file-backed, reclaimable.

---

## Remediation summary

| Phase | Fix | Crates | Risk |
|-------|-----|--------|------|
| 1 | F1: stop retaining inserts on full-memory DBs; add `stats()` | tn-storage | node-local, rolling deploy safe |
| 2 | F2: `TxnGuard` ends abandoned txns (commit + warn) | tn-storage | node-local, rolling deploy safe |
| 3 | F4: broadcast capacity caps | tn-primary | node-local, rolling deploy safe |
| 4 | F3: engine queue bound + `to_engine` 64 + gap detection | tn-engine, tn-node | changes lag behavior (silent skip → detected error + restart replay); needs review sign-off |
| 5 | F5: peer-map hygiene | tn-network-libp2p | optional, separable |

## Verification

1. `cargo test -p tn-storage -p tn-engine -p tn-node` (new regression tests) + check/clippy/fmt.
2. Leak regression proof: the Phase 1 test fails on unpatched main (`retained_inserts == K`) and
   passes with the fix.
3. Soak: sustained-round e2e; `stats().retained_inserts` stays 0 across all three DBs and
   `tn_engine_queued_outputs` stays ≤ 8.
4. Prod validation: canary one node; watch RSS slope over 48–72h vs an unpatched node; alert on
   `tn_engine_queued_outputs` growth. Expected: flat RSS after warm-up instead of ~25%/week climb.
