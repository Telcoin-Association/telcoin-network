# Fix duplicate-batch findings 1 & 2 + epoch-gated fork strategy

## Plan checklist

- [x] Step 1 — Fork module `crates/types/src/fork.rs` (ForkId enum, fork_active_at, is_active, test override)
- [x] Step 2 — Cargo feature wiring (`faucet` passthrough: tn-types → executor/engine/storage → node → bin; e2e-tests; Makefile repair)
- [x] Step 3 — Subscriber realignment (`fetch_batches` dedup gated on leader_epoch; finding 26 log fix)
- [x] Step 4 — Engine hard error (BatchDigestMismatch variant, replace debug_assert)
- [x] Step 5 — Pack replay alignment (`get_consensus_output` mirror live execution per-fork)
- [x] Step 6 — Batch-builder prevention (quorum_batch_digests set, skip re-send, ack locally)
- [x] Step 7 — Proposer ingestion dedup (seen_digests, handle_our_digest extraction)
- [x] Step 8 — Tests (fork units, output.rs units, executor IT, engine IT, proposer units, batch-builder tests, pack replay test)
- [x] Verification (cargo check/clippy/nextest default + faucet feature sets, release builds)
  - `cargo check --workspace --all-targets`: clean
  - `cargo nextest run --workspace --no-fail-fast`: 643/643 pass
  - faucet suites (tn-types/executor/engine/storage): all pass
  - `cargo build --bin telcoin-network --release` (default + `--features faucet`): both artifacts build

## Test results so far

- fork units: pass (default: 2, faucet+test-utils: 3 incl. placeholder sentinel + override)
- batch-builder: 5/5 pass — pool re-accepted resubmitted txs (incident reproduced); dedup skip + local ack verified; failed-quorum rebuild same digest verified
- proposer: 7/7 pass (incl. new ingestion-dedup + retransmit-not-deduped)
- executor IT test_duplicate_batch_digest: pass under default (aligned) AND faucet (legacy +dup_count)
- engine IT: close-epoch-with-deduped-batches, misaligned-hard-error (default), misaligned-tolerated (faucet), duplicate-transactions regression: all pass
- storage pack replay: pass under default (aligned) AND faucet (legacy)
- pre-existing failures not in scope: tn-types lib-only check with test-utils (feature unification), tn-primary clippy never_loop in certificate_fetcher_tests.rs (exists without these changes)

## Review

### What changed

**Fork machinery (permanent infrastructure)**
- `crates/types/src/fork.rs` (new): `ForkId` enum with `DedupBatchDigests` variant; `fork_active_at` pure comparison; `is_active` — always true in non-faucet builds, epoch-gated (placeholder `Epoch::MAX`) in faucet builds; per-fork `AtomicU64` test override under `cfg(all(faucet, test-utils))` (`set_activation_for_test` / `clear_activation_for_test`). Module docs include the recipe for adding future forks.
- Cargo `faucet` passthrough chain: tn-types → {tn-executor, tn-engine, tn-storage} → tn-node (also tn-reth) → bin/telcoin-network; e2e-tests faucet now pulls tn-node/faucet. Makefile `test-faucet` repaired to target `e2e-tests` (the bin has no `it` test).

**Correctness fix (fork-gated)**
- `subscriber.rs fetch_batches`: `batch_digests` keeps first occurrence only (1:1 with deduped `batches`); legacy push under `cfg(faucet)` + fork-inactive, keyed on `sub_dag.leader_epoch()`. Finding 26: dup-fetch warn no longer dumps `batch_digests`, logs the certificate digest instead.
- `payload_builder.rs`: `debug_assert_eq!` → hard `TnEngineError::BatchDigestMismatch` (new variant); tolerated only in faucet builds pre-activation (with warn).
- `consensus_pack.rs get_consensus_output`: digests deduped per-fork like the subscriber; duplicate batches never loaded twice in either mode (fixes the pre-existing replay divergence where the dup batch loaded twice → phantom empty block).

**Root-cause prevention (ungated)**
- batch-builder: `quorum_batch_digests: Arc<Mutex<HashSet>>` records digests only on quorum ack; identical rebuilds are not re-proposed — acked locally so the pool re-cleans. Quorum failures never recorded → legitimate retry keeps the same digest.
- proposer: `seen_digests: HashSet` dedups `rx_our_digests` ingestion only (extracted `handle_our_digest`, always acks); `process_committed_headers` retransmit path untouched by design.

### Verification

- 343/343 tests pass across tn-types/tn-executor/tn-engine/tn-primary/tn-batch-builder/tn-storage (default features).
- Faucet suites: tn-types 83, tn-executor 4, tn-engine 10, tn-storage 87 — all pass (legacy paths exercised at epoch 0 via placeholder).
- Incident reproduced in test: pool re-accepted resubmitted mined txs; identical rebuild skipped; `from_batch_builder.recv()` timed out; pool drained via local ack.
- Workspace suite + release builds: see below.

### Open items for ops (from plan, unchanged)

1. `ForkId::DedupBatchDigests` activation epoch is `Epoch::MAX` placeholder — set the real adiri upgrade epoch (sentinel unit test will force a visible diff).
2. `etc/Dockerfile` builds WITHOUT `--features faucet` — ops must confirm the testnet image build command now that the bin exposes the feature.
3. Pre-existing, untouched: tn-primary clippy `never_loop` error in `certificate_fetcher_tests.rs`; tn-types lib-only check with `test-utils` fails without dev-dep feature unification.
