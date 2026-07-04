# Code Review: per-worker EIP-1559 base fee stack (feat/basefee-02..08)

**Date:** 2026-07-04
**Scope:** `origin/main (aa67c773)...67f7bf2f` — 16 commits, 15 files, +1529/−130
**Line refs pinned to:** `67f7bf2f6864ea516e79c6b44aff13fa2948d589`
**Method:** tn-review (6 parallel domain readers → canonical findings → findings-verifier) + scoped test execution

---

## 1. PR Stack Overview

### 1a. Per-PR breakdown

Branch names elide the `feat/` prefix for width. Commit subjects were verified against `git log origin/main..HEAD` (16 commits); all match the stack map, no discrepancies.

| PR | Branch | Commits | Intent | Key files | Tests added |
|---|---|---|---|---|---|
| 01 | `basefee-01-engine-basefee` | merged to main as #792 (`aa67c773`) | Worker pool base fee sourced from the `GasAccumulator` | `node/src/engine/{inner,mod}.rs`, `manager/node/start_epoch.rs` | node IT coverage (+81 lines, `node/tests/it/main.rs`) |
| 02 | `basefee-02-validator-u64` | `362c79b0`, `62b733fd` | `BatchValidator` takes a `u64` base-fee snapshot instead of a `BaseFeeContainer` | `batch-validator/src/validator.rs`, `node/src/engine/*`, `start_epoch.rs` | none net: batch-builder ITs adapted; validator unit tests added, then removed on PR feedback (`62b733fd`) |
| 03 | `basefee-03-compute-seam` | `dd16fc6a` | EIP-1559 compute seam wired into epoch close, inert `u64::MAX` target | `node/src/manager/node/run_epoch.rs` | `adjust_base_fees_keeps_workers_at_min` |
| 04 | `basefee-04-run-epoch-from-chain` | `e8b87b00`, `92bb6385`, `97b2ca7b`, `eb95c4f3` | Gate forward compute to the live boundary; seed fees from chain at epoch entry | `run_epoch.rs`, `manager/node.rs`, `types/src/gas_accumulator.rs` | 3 `seed_base_fees_*` unit tests |
| 05 | `basefee-05-catchup-per-worker` | `018a882a` | Restore base fee per worker on catchup (was worker 0 only) | `manager/node.rs` | extends `test_catchup_accumulator` (poisoned fee overwritten from chain) |
| 06 | `basefee-06-activate` | `065058db`, `2215b657`, `7cfbf7ef` | Activate per-worker fees from on-chain `WorkerConfigs` (inert by default); adds transient 1-GWEI fallback | `run_epoch.rs`, `manager/node.rs` | `eip1559_config_with_max_target_is_inert_at_min`, `eip1559_config_moves_fee_with_gas_vs_target`, `static_config_pins_to_configured_fee` |
| 07 | `basefee-07-num-workers-from-chain` | `352aec2c`, `3401e958`, `32e1fcad`, `5b5a69d4` | Worker count from on-chain `numWorkers()`; RwLock accumulator resize; replaces the 1-GWEI fallback with fail-open keep-current | `types/src/gas_accumulator.rs`, `tn-reth/src/lib.rs`, `tn-reth/src/test_utils.rs`, `manager/node.rs`, `run_epoch.rs` | 6 resize unit tests (`set_num_workers_*`, `clones_observe_resize`, `resize_preserves_rewards_counter_identity`); `test_worker_count_read_at_epoch_start_parent` (tn-reth); 3 node ITs (`test_sync_num_workers_from_chain_adjusts_to_on_chain_count`, `test_sync_num_workers_fail_open_when_contract_absent`, `test_sync_then_catchup_recovers_two_worker_accumulator`) |
| 08 | `basefee-08-e2e` | `67f7bf2f` | Single-worker per-worker base-fee e2e coverage | `e2e-tests/tests/it/basefee.rs` (+551), `e2e-tests/src/lib.rs` | 3 `#[ignore]` e2e tests: `test_static_fee_applied_at_epoch_boundary`, `test_eip1559_fee_rises_at_epoch_boundaries`, `test_mid_epoch_restart_recovers_static_fee` |

### 1b. Activation arc

PR 01 (merged as #792) laid the first pipe: the worker's transaction pool reads its base fee from the shared `GasAccumulator` instead of a constant. PRs 02–05 build the rest of the plumbing without moving any fee. PR 02 has the batch validator capture a `u64` snapshot at epoch start rather than hold a live container. PR 03 adds the compute seam: `adjust_base_fees` runs the real EIP-1559 formula at every epoch close, but against a hardcoded `u64::MAX` gas target. PR 04 restricts that forward computation to the live boundary and seeds fees from chain at epoch entry, so syncing and restarting nodes read the chain rather than recompute. PR 05 makes restart catchup restore fees per worker instead of worker 0 only.

"Inert by default" is a genesis property, not a code switch. The default worker config (CLI default `0:0:18446744073709551615` at `telcoin-network-cli/src/genesis/mod.rs:125`, i.e. `Eip1559 { target_gas: u64::MAX }`) means accumulated gas never exceeds target, so the computed fee only ratchets down and floors at `MIN_PROTOCOL_BASE_FEE`. Fees stay byte-identical to pre-stack behavior until governance sets a real target.

PR 06 makes the seam activation-capable: it replaces the constant with a per-worker `WorkerConfigs` read at the epoch's closing block, dispatched through the pure `next_base_fee_for_config` (`Eip1559` computes, `Static` pins). Its final commit (`7cfbf7ef`) added a 1-GWEI fallback when the config read fails; PR 07 (`5b5a69d4`) removed it, and at HEAD a failed read keeps the current per-worker fees and worker count. That matters for determinism: 1 GWEI appears nowhere on chain, so a node seeding from chain after a restart could never rederive it, while kept-current fees are exactly what seeding produces. PR 07 also makes the worker count chain-derived (RwLock resize plus `sync_num_workers_from_chain` at epoch entry), leaving the accounting layer multi-worker-ready. The runtime still spawns exactly one worker (`DEFAULT_WORKER_ID`, `start_epoch.rs:372`); multi-worker spawning is deferred to issues 4a/4b/4c/5. PR 08 closes the arc with three single-worker e2e tests: static pinning, EIP-1559 rise across boundaries, and mid-epoch restart recovery.

### 1c. Known-issues cross-reference

| Issue file | One-line summary | Severity/status as written | Status at HEAD `67f7bf2f` |
|---|---|---|---|
| `3b-catchup-per-worker.md` | Catchup restored only worker 0's base fee after a mid-epoch restart | Implemented in PR3b (cites `9197ad40`); inert single-worker | Fixed: per-worker restore via `latest_base_fee_per_worker` / `worker_id_from_header` (`manager/node.rs:172,267,282`); landed as `018a882a` |
| `4a-per-worker-gossip-topics.md` | Workers share one batch/txn gossip topic; needs `tn-worker-{chain_id}-{worker_id}` (coordinated protocol change) | Follow-up, not implemented | Out of scope—deferred: topics still chain-id-only (`config/src/network.rs:170-175`; test asserts `"tn-worker-2017"`) |
| `4b-worker-network-handles-vec.md` | `EpochManager` holds a single `Option<WorkerNetworkHandle>`; needs one handle per worker | Follow-up, not implemented | Out of scope—deferred: field unchanged (`manager/node.rs:76`) |
| `4c-local-network-stream-transport.md` | `LocalNetwork`'s `Arc<dyn ...>` handler slots are single-worker by construction; wants a stream-generic, per-worker transport | Follow-up, not implemented | Out of scope—deferred: both `Option<Arc<dyn ...>>` slots unchanged (`network-types/src/local.rs:27,29`) |
| `5-multiworker-components.md` | Spawn worker components per worker and size the accumulator from on-chain `numWorkers()` | Follow-up, not implemented | Partially addressed: PR 07 landed the accounting half (`sync_num_workers_from_chain`, `manager/node.rs:214,443`, resized each epoch entry); spawn loop still single-worker (`start_epoch.rs:372`) |
| `6-activate-workerconfigs.md` | Read `WorkerConfigs` at the closing block and apply `Eip1559`/`Static` per worker, inert by default | Implemented in PR6 (cites `bce75900`); independently reviewed, zero findings | Fixed (as `065058db` plus refinements), but the text is stale: HEAD fails open (keep current fees and count) on read failure rather than erroring on worker-count drift, and the count is no longer "fixed at startup" |
| `7-e2e-coverage.md` | Single-worker e2e: static fee applied, EIP-1559 rise, mid-epoch restart recovery | Implemented in `feat/basefee-07-e2e` (cites `e851fdcc`); all three pass | Fixed: all three tests present at HEAD in `e2e-tests/tests/it/basefee.rs`; now PR 08 / `feat/basefee-08-e2e` (`67f7bf2f`) |
| `blocker-restart-boundary-recovery.md` | A node restarting at the exact epoch boundary loses the forward-computed next-epoch fee; peers reject its batches once fees are real | BLOCKER before governance sets any real target; harmless while inert | Open: `live_boundary` gate and the skipped-seeding window unchanged (`run_epoch.rs:604-614`); still latent-only under the inert default |
| `dual-header-read-robustness.md` | Seeding and catchup derive the block range from two header sources (`finalized_header()` vs canonical-tip epoch state), risking a silent empty range | Low; close alongside the blocker | Open: pattern unchanged at both sites (`run_epoch.rs:159-168`, `manager/node.rs:160-161`); a comment documents the assumption, no single-header pin, assert, or regression test |

**Registry notes.** The registry's structural map is stale: `issues/README.md` describes a seven-branch stack ending at `feat/basefee-07-e2e` (PR7 = e2e), whereas the stack at HEAD has eight PRs, with 07 = `feat/basefee-07-num-workers-from-chain` (`352aec2c..5b5a69d4`) and 08 = `feat/basefee-08-e2e` (`67f7bf2f`). No issue file covers PR 07's scope at all; the nearest, `5-multiworker-components.md`, explicitly scoped out the epoch-boundary worker-count sync that PR 07 now ships for the accounting layer. Every commit hash in the registry (`9197ad40`, `bce75900`, `e851fdcc`) is two rewrites old: first superseded by the review-driven rewrite recorded in `tasks/review-pr6-activation.md` (`55803496`/`accc01c1`/`769dc653`), then by the current rebase (`018a882a`/`065058db`/`67f7bf2f`). Two content-level stale spots: `6-activate-workerconfigs.md` still describes fail-stop drift handling ("brings the node down cleanly") that PR 07 replaced with fail-open keep-current, and `blocker-restart-boundary-recovery.md` cites a `KNOWN LIMITATION` comment in `close_epoch` that no longer exists verbatim (the doc comment was reworded), while the `gas_accumulator` module doc the blocker flags as wrong for the restart case ("keeps the value the live producer just computed at the boundary") is still present at `types/src/gas_accumulator.rs:41-43`, so that acceptance item of the blocker also remains open.

---

## 2. Summary

28 raw findings from 5 domain readers deduplicated to 23 canonical findings. Labels: **KNOWN** = already tracked in `issues/`; **NEW** = surfaced by this review. Verification status is filled in by the findings-verifier pass.

| # | Severity | Category | Title | Label | Verification |
|---|---|---|---|---|---|
| F1 | High | Consensus Safety | Boundary-window closes skip fee compute AND seeding — full epoch on stale/MIN fees; leftover-drain shape untracked | KNOWN + NEW shape | CONFIRMED |
| F2 | Medium | Bugs | Restart e2e cannot fail if seed-from-chain is broken (vacuous assertions) | NEW | CONFIRMED (was High) |
| F3 | Low | Bugs | EIP-1559 e2e skip branch converts CI hiccups into guaranteed monotonic-assert failures | NEW | CONFIRMED (was High) |
| F4 | Low | Consensus Safety | `adjust_base_fees` fail-open treats node-local read failure as committee-deterministic (D.6) | NEW | PARTIALLY VALID (was Medium) |
| F5 | Medium | Error Handling | Fail-open worker-count sync defers into an `expect` panic crash-loop on multi-worker chains | NEW | CONFIRMED (latent) |
| F6 | Medium | Determinism | Zero-block worker's fee is unrecoverable from chain — restarted node seeds MIN while live nodes hold computed fee | NEW | CONFIRMED (latent) |
| F7 | Medium | Bugs | Genesis writer ignores WorkerConfigs constructor revert — ceremony typo silently bricks fee governance | NEW | CONFIRMED |
| F8 | Low | Bugs | Shipped doc comments assert the invariant the tracked blocker refutes; KNOWN LIMITATION marker gone | NEW | CONFIRMED (was Medium) |
| F9 | Low | Bugs | EIP-1559 e2e asserts inequality only — wrong denominator / double-apply / late-by-one-epoch all pass | NEW | PARTIALLY VALID (was Medium) |
| F10 | Low | Bugs | Mid-epoch positioning via raw sleep enforced by a hard assert (restart e2e pre-kill flake) | NEW | PARTIALLY VALID (was Medium) |
| F11 | Medium | Bugs | Final restart assertion silently skipped on timeout (`if let Some` vs `ok_or_else`) | NEW | CONFIRMED |
| F12 | Low | Bugs | Fail-open worker-count test cannot distinguish fail-open from the ≥1 clamp | NEW | CONFIRMED (was Medium) |
| F13 | Medium | Architecture | Basefee e2e tests run in no hosted CI job; ignore reason copy-pasted and wrong | NEW | CONFIRMED |
| F14 | Low | Fork Safety | Unknown-strategy fallback reinterprets `value` as an EIP-1559 target — mixed-version committee fee divergence | NEW | CONFIRMED (latent) |
| F15 | Medium | State & Funds | `Static` strategy unclamped: fee=0 bypasses MIN_PROTOCOL_BASE_FEE; u64::MAX halts inclusion for an epoch | NEW | CONFIRMED (was Low) |
| F16 | Low | Determinism | D.4 widened at HEAD: per-worker fee restore now hangs off the unpinned dual-source range | KNOWN | CONFIRMED (widening) |
| F17 | Low | Determinism | D.5 residual: close-time/entry-time same-block identity is implicit and unasserted (live-bug claim refuted) | NEW | CONFIRMED (refutation holds) |
| F18 | Low | Concurrency | `set_num_workers` quiescence precondition violated on ModeChange re-entry — value-stability, not quiescence, protects | NEW | CONFIRMED |
| F19 | Low | Error Handling | `set_worker_base_fee` silently no-ops for an uninitialized worker id | NEW | CONFIRMED |
| F20 | Low | Bugs | `land_tx_and_read_fee` infers tx block from current tip instead of a receipt | NEW | CONFIRMED |
| F21 | Low | Architecture | Helper duplication within basefee.rs and across test modules | NEW | CONFIRMED |
| F22 | Info | Memory & Safety | `inc_block` panic tripwire fires with no diagnostic linking it to the failed count sync | NEW | NOT VERIFIED (Info) |
| F23 | Info | Bugs | `getAllWorkerConfigs` hits the 30M system-call gas cap near ~4–5k workers → deterministic network-wide fail-open freeze | NEW | NOT VERIFIED (Info) |

**Verification pass (findings-verifier, 11 `Explore` subagents, anti-confirmation protocol; line refs re-pinned to `67f7bf2f`):** 23 findings → **18 CONFIRMED, 3 PARTIALLY VALID, 0 FALSE POSITIVE, 2 NOT VERIFIED** (Info, by tiering policy). False-positive rate on verified findings: **0 / 21 = 0%**; no finding was proven invalid. **Severity adjustments:** F2 High→Medium, F3 High→Low, F15 Low→Medium, and F4/F8/F9/F10/F12 Medium→Low (net High count 3→1: only F1 remains High). F5 and F6 held at Medium — their solo verifier proposed High on armed-impact grounds, calibrated down for double-latency behind unshipped multi-worker spawning (F5 is a partly-intentional halt; F6's blast radius is one idle worker, narrower than F1). F17's refutation chain was independently re-verified and holds even under a multi-block boundary output, so it was NOT escalated (residual-only).

---

## 3. Findings

Findings are consolidated from 5 parallel domain readers (accumulator/concurrency, epoch lifecycle, on-chain config, engine/validator plumbing, test quality); duplicates merged keeping the highest severity and the union of evidence. Line refs pinned to `67f7bf2f`.

### F1: Boundary-window epoch closes skip fee computation AND seeding — node runs the entire next epoch on stale/MIN fees; leftover-drain shape untracked
- **Severity**: High
- **Category**: Consensus Safety
- **Label**: KNOWN (tracked in `issues/blocker-restart-boundary-recovery.md`) — shape (c) below is NEW/untracked
- **Location**: `crates/node/src/manager/node/run_epoch.rs:612` (gate; also 218, 408, 697-699)
- **Claim**: `close_epoch(None, ...)` skips `adjust_base_fees` (the `live_boundary` gate at run_epoch.rs:604-614), and epoch-entry seeding self-disables when `tip_epoch != entered_epoch` (run_epoch.rs:697-699) — on exactly these paths the tip IS epoch N's closing block (nonce carries epoch N), so seeding is also skipped, and `catchup_accumulator` restores nothing because `blockHeight..=tip` = `(B+1)..=B` is empty (node.rs:166-167). Three trigger shapes: **(a) replay-and-close** — crash before executing N's boundary output; restart replays it → `close_epoch(None)` at run_epoch.rs:218; **(b) crash-after-close** — restart with tip = closing block; fresh accumulator stays at default MIN; **(c) leftover-drain, NO restart** — the epoch task manager exits (e.g. CVV resync) racing the boundary, `send_leftover_consensus_output_to_engine` (close_epoch.rs:114-162) drains the boundary output, `close_epoch(None)` at run_epoch.rs:408 — a live, healthy-looking node. In all shapes the node's `BatchValidator` and batch builder snapshot the wrong u64 (start_epoch.rs:376, run_epoch.rs:312) for the whole epoch: exact-match `validate_basefee` (validator.rs:188-195) rejects every honest peer batch and peers reject its batches; under `Eip1559` the error compounds at later boundaries; f+1 affected nodes stall batch quorum, and a coordinated boundary-timed upgrade restart can split the committee. Inert at HEAD only because the genesis config is `Eip1559 { target_gas: u64::MAX }` — a governance transaction alone arms it.
- **Key Question**: On the `close_epoch(None)` paths, does ANY mechanism write the committee's adjusted N+1 fee into the `BaseFeeContainer`s before the `start_epoch` snapshot — and is the leftover-drain shape (run_epoch.rs:408) genuinely reachable with a queued boundary output and no restart?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/node/src/manager/node/close_epoch.rs, crates/node/src/manager/node.rs, crates/node/src/manager/node/start_epoch.rs, crates/batch-validator/src/validator.rs, issues/blocker-restart-boundary-recovery.md
- **Source**: tn-review (R1, R2, R3, R4 — independently converged)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **High** (unchanged; inert at HEAD default, armed by one governance tx).
  - **Evidence (shape c, live non-restart)**: the CVV catch-up rejoin broadcasts+saves epoch N's boundary output then ends its task cleanly (no restart); `until_task_ends` wins the unbiased `select!` (`run_epoch.rs:334-382`), so `epoch_boundary_reached` stays false and `send_leftover_consensus_output_to_engine` returns `Some(hash)` for the boundary output (`close_epoch.rs:119-159`) — `close_epoch(None,...)` at `run_epoch.rs:408`. `live_boundary=false` skips `adjust_base_fees` (`run_epoch.rs:604-614`); `clear()` zeros gas only, not `base_fee` (`gas_accumulator.rs:293-301`); at N+1 entry the tip is N's closing block (nonce epoch=N) so `seed_base_fees_from_chain` returns early on `tip_epoch(N)!=entered(N+1)` (`run_epoch.rs:697-699`). `grep` confirms no third `set_base_fee` writer; N+1 snapshots (`start_epoch.rs:376,411,419-421`, `run_epoch.rs:312`) read the stale epoch-N fee, and exact-match `validate_basefee` (`validator.rs:188-195`) then rejects honest peers all epoch. The shipped docstring `run_epoch.rs:590-593` names this leftover-drain path but wrongly claims seeding covers it.
  - **Proposed fix**: on a `close_epoch(None)` leftover-drain close (`tip_epoch == entered-1`), re-enter N+1 in a sync/`CvvInactive` posture so the node does not produce for N+1 until it has executed an N+1 block and `seed_base_fees_from_chain` can adopt the chain fee (`run_epoch.rs:697-704`). Alternative (riskier): at entry, when `tip_epoch==entered-1`, run the forward `adjust_base_fees` only if the accumulator provably holds complete epoch-N gas. Add a regression test for the CVV-resync-at-boundary path.

### F2: Restart e2e cannot fail if `seed_base_fees_from_chain` is broken
- **Severity**: High
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:341`
- **Claim**: The test's stated purpose is proving recovery re-seeds the base fee from chain, but no assertion observes the restarted node's live fee state. Assertion 1 (lines 343-355) checks the restarted node *serves* the historical static-fee block — pure state sync reproduces the header's `base_fee_per_gas` regardless of the local `BaseFeeContainer`. Assertion 2 (lines 359-365) confirms a new tx via `client_urls[0]`; with 1 of 4 validators killed, the 3 healthy nodes hold quorum (2f+1=3) and produce/certify that block even if the restarted node recovered MIN and refuses to vote. Deleting `seed_base_fees_from_chain` entirely would leave every assertion green.
- **Key Question**: If the restarted validator's per-worker base fee were wrongly reset to MIN_PROTOCOL_BASE_FEE, would any of the test's assertions (catchup fee at :351, post-restart tx fee at :362, optional check at :368) actually fail given 3-of-4 quorum and sync-executed blocks?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, crates/node/src/manager/node/run_epoch.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Medium** (down from High).
  - **Evidence**: `client_urls[0]`=validator-1 (never killed); `kill_idx=2`=validator-3 (restarted). All three post-restart reads observe stored header values, not the restarted node's local `BaseFeeContainer`: assertion 1 (`basefee.rs:343-355`) reads `static_block`'s `baseFeePerGas` via `read_base_fee`—`get_block` (`:519-527`), reproduced by state-sync regardless of local fee (block execution derives base fee from batch/parent header, not the accumulator — verified-safe obs #10); assertion 2 (`:360-365`) submits via `client_urls[0]` and 3-of-4 quorum (2f+1=3, n=4) certifies it even if validator-3 recovered MIN; the final check (`:368-373`) reads a served header and is optional (F11). Refutation (a MIN-reset node fails to catch up, tripping `:344`'s `ok_or_else`) fails: sync executes committed blocks from header fees, not the accumulator. Deleting `seed_base_fees_from_chain` leaves every assertion green.
  - **Severity adjudication**: High—>Medium — test-efficacy gap (false-pass) on a High-importance feature; Medium sits at the top of the test-quality band, not a High production defect.
  - **Proposed fix**: query the restarted node's LOCAL live fee — after restart submit a priced tx to `client_urls[kill_idx]` (validator-3) and assert the resulting block carries `STATIC_FEE`, and/or assert validator-3's newly-produced batch is accepted by peers. Pair with F11's `ok_or_else` fix so the post-restart check is mandatory.

### F3: EIP-1559 e2e's "defensive" skip branch guarantees monotonic-assertion failures under CI hiccups
- **Severity**: High
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:208`
- **Claim**: A tx that misses its 25s confirmation deadline (`EPOCH_DURATION*2+5`, basefee.rs:495) is "skipped, not fatal" — but during that stall ~2.5 empty 10s epochs pass, and with `target_gas=1` each zero-gas epoch *decreases* the fee (pinned by `crates/types/src/gas_accumulator.rs:416`: base 8, gas 0, target 1 → 7). After two recorded epochs (fees 8, 9), one skip yields 9→8→7 and the next recorded fee violates `nf >= pf` (basefee.rs:229-232). The mechanism intended to absorb slow-CI noise converts it into a hard assertion failure whenever the fee has risen above MIN; a 2-epoch overshoot of `wait_for_epoch_at_least` has the same effect.
- **Key Question**: With `Eip1559 { target_gas: 1 }`, does an epoch containing zero gas reduce the base fee at its boundary, so that a skipped iteration between two recorded ones produces a strictly decreasing recorded pair?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, crates/types/src/gas_accumulator.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (down from High).
  - **Evidence**: `compute_next_base_fee_eip1559(cur, 0, 1)` — gas_limit=2, fee decreases by `max(1, fee/8)`, floored at MIN=7 (`gas_accumulator.rs:372-382`, test note `:477`). The loop records a fee only when a tx lands (`basefee.rs:206`); a missed 25s deadline (`EPOCH_DURATION*2+5`, `:495`) skips the iteration (`:208-211`) and `wait_for_epoch_at_least(cur+1)` jumps forward ~2-3 epochs, so >=1 empty epoch drops the fee. The windowed assert is `nf >= pf` (`:229-231`), so once the fee has risen >=8 a skip yields a strictly-decreasing recorded pair (e.g. 9->...->7) that fails hard.
  - **Severity adjudication**: High—>Low — fail-SAFE (loud spurious FAILURE, never a bug-hiding false pass); the test is `#[ignore]`d and (per F13) runs in no hosted CI, so blast radius is a misleading local/attest flake.
  - **Proposed fix**: drop the cross-iteration `nf >= pf` assert (false for `target_gas=1` across empty epochs); instead fail the test on a missed deadline (no silent skip) OR record one fee per BOUNDARY and assert the exact series via `compute_next_base_fee_eip1559`.

### F4: `adjust_base_fees` fail-open treats a node-local read failure as committee-deterministic — single-node fee divergence with no in-epoch heal (D.6)
- **Severity**: Medium
- **Category**: Consensus Safety
- **Label**: NEW
- **Location**: `crates/node/src/manager/node/run_epoch.rs:659` (FAIL-OPEN comment at 642-647)
- **Claim**: The Err arm keeps current fees, justified by "every committee member that hits the same read failure lands on the same state." That holds only for chain-global failures (contract absent, decode error). But the read path starts with `state_by_block_hash(header.hash())?` (tn-reth/src/lib.rs:1556) — an MDBX/state-provider read that can fail on one node only (I/O fault, corrupted page). That node keeps epoch-N fees while peers advance to N+1; exact-equality `validate_basefee` (batch-validator/src/validator.rs:190) then rejects all peers' batches and its own are rejected, for the entire epoch. There is no in-epoch heal: entry seeding never fires for a live producer (tip is in the previous epoch at entry, run_epoch.rs:697-700). The count half of the fail-open self-heals at the next entry sync; the fee half does not. Under `Eip1559` the error compounds at each boundary until a process restart re-seeds via catchup. f+1 concurrent local faults would stall batch quorum. Correct policy for a node-local failure on a consensus input is retry-or-halt, not proceed-with-stale.
- **Key Question**: Can `state_by_block_hash`/`worker_fee_configs_inner` return `Err` on one committee member for the canonical tip (a block it executed milliseconds earlier) while peers succeed — and if that is implausible, is the Err arm effectively dead code with a misleading justification comment?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/tn-reth/src/lib.rs, crates/batch-validator/src/validator.rs
- **Source**: tn-review (R1, R2, R3, R4 — independently converged)

- **Verdict (findings-verifier)**: PARTIALLY VALID — Confidence HIGH — Verified severity **Low** (down from Medium).
  - **Evidence**: the read does begin node-local — `state_by_block_hash(header.hash())?` (`tn-reth/src/lib.rs:1556`) and `read_state_on_chain(...)?` (`:1566`) — and the Err arm keeps current fees with no in-epoch heal for a live producer (`run_epoch.rs:659-665,697-700`); exact-match validation (`validator.rs:190`) then rejects peers. BUT the `committee-deterministic` justification is correct for the dominant classes (contract absent, decode/arity error — chain-global, identical everywhere), pruning is disabled and the just-executed canonical-tip state is guaranteed present, so the Err arm is NOT dead code and node-local divergence needs a genuine isolated I/O/corruption fault at exactly the fee read after the full block executed — very unlikely, uncorrelated across nodes (no coordinated quorum stall), inert under the MIN default.
  - **Severity adjudication**: Medium—>Low — real but low-likelihood; the comment is misleading only for the rare node-local class.
  - **Proposed fix**: distinguish provider/`state_by_block_hash` errors (bounded retry, then halt) from contract-absent/decode errors (keep-current), and scope the comment to state that a node-local provider fault is NOT committee-deterministic.

### F5: Fail-open worker-count sync converts a transient read failure into a deferred `expect` panic (startup crash-loop) on multi-worker chains
- **Severity**: Medium
- **Category**: Error Handling
- **Label**: NEW (latent until multi-worker spawning lands)
- **Location**: `crates/node/src/manager/node.rs:255` (Err arm; panics land at node.rs:173/182 → gas_accumulator.rs:317/286)
- **Claim**: All failure arms of `sync_num_workers_from_chain` (node.rs:222-238, 255-262) warn and keep the current count, documented as "the node behaves exactly as it did before worker counts were chain-derived" (node.rs:207-209). Only true single-worker. At startup the accumulator is hardcoded `GasAccumulator::new(1)` (node.rs:422); if the sync read fails on a chain whose epoch contains worker-id ≥ 1 blocks, `catchup_accumulator` immediately panics on the first worker-1 datum via `.expect("valid worker id")` (`base_fee` node.rs:173 → gas_accumulator.rs:317; `inc_block` node.rs:182 → gas_accumulator.rs:286) — a crash-loop as long as the read keeps failing, not graceful degradation. Same shape at epoch entry (run_epoch.rs:151): stale-low count panics in `payload_builder.rs:179` during replay or on the first live multi-worker block — a mid-operation crash far from the root cause. Note the read block for sync (epoch-start parent) can be up to a full epoch old, unlike other registry reads which pin the tip — raising failure plausibility (e.g. state pruning). The asymmetry with seeding (which iterates `0..num_workers` and silently ignores unknown ids, run_epoch.rs:701-704) makes catchup the panic site.
- **Key Question**: If `get_worker_fee_configs_at_block` fails at startup on a 2-worker chain, does the node reach a controlled, diagnosable error, or panic in `catchup_accumulator`'s per-worker loop on the first worker-1 header?
- **Relevant Files**: crates/node/src/manager/node.rs, crates/types/src/gas_accumulator.rs, crates/engine/src/payload_builder.rs, crates/node/src/lib.rs
- **Source**: tn-review (R1, R2, R4 — independently converged)

- **Verdict (findings-verifier)**: CONFIRMED (latent) — Confidence HIGH — Verified severity **Medium** (held; solo verifier proposed High).
  - **Evidence**: startup `GasAccumulator::new(1)` with sync-then-catchup order (`node.rs:443->444`); all sync failure arms keep the current count (`node.rs:222-238,255-262`). On a >=2-worker chain with a failed read, `catchup_accumulator` indexes block-derived worker ids via `.expect("valid worker id")` — `base_fee` (`gas_accumulator.rs:317`) / `inc_block` (`:286`, early-returns on gas_used==0 `:282`) — with no clamp, panicking on the first non-empty worker-1 block; a persistent read failure is a crash-loop. Same shape at entry via `payload_builder.rs:179`.
  - **Severity adjudication**: held at Medium (not High). Double-latent — requires BOTH multi-worker spawning (deferred; runtime hardcodes `DEFAULT_WORKER_ID` at `start_epoch.rs:372`; issues 4a/4b/4c/5) AND a config-read failure; the panic is a partly-intentional tripwire (`gas_accumulator.rs:275-279`). Value is flagging the fail-open(sync)/fail-hard(catchup) inconsistency to resolve when multi-worker lands.
  - **Proposed fix**: in `catchup_accumulator`, grow the accumulator to cover the max observed worker id (or clamp+warn) before per-worker writes, and align the two policies (both fail-open, or both fail-stop with a controlled diagnosable error).

### F6: A worker with zero blocks in the entered epoch has an unrecoverable fee — restarting/syncing nodes seed MIN while live nodes hold the boundary-computed fee
- **Severity**: Medium
- **Category**: Determinism
- **Label**: NEW (latent until multi-worker spawning + non-inert configs)
- **Location**: `crates/node/src/manager/node/run_epoch.rs:701` (skip semantics at 689-690; catchup analogue node.rs:282-291)
- **Claim**: Both recovery sources are block-derived: `latest_base_fee_per_worker` and `seed_base_fees_from_chain` leave any worker absent from the epoch's headers "as-is" — which after a restart means the default `MIN_PROTOCOL_BASE_FEE` (fresh slots). `adjust_base_fees` deliberately computes fees for governance-added or idle workers at the boundary (doc, run_epoch.rs:631-633) — e.g. `Static { fee: 500 }` — but that value exists only in live nodes' memory until the worker produces its first block. A node restarting or syncing mid-epoch (not boundary-timed — arbitrarily long after) adopts fees only for workers with on-chain blocks; the idle worker stays at MIN, and the exact-match validator rejects that worker's first batch epoch-wide. Unreachable today (worker 0 gets a block from effectively every output; the empty close block is stamped worker 0, payload_builder.rs:119); becomes a live gap the moment worker count > 1 with uneven traffic. The blocker's proposed strategy-based frontier derivation closes this only if implemented per-worker.
- **Key Question**: For a worker with a `Static`/real `Eip1559` config and zero blocks in epoch E, is there any chain-derived path by which a node entering E mid-epoch obtains the fee the live committee computed at E's open?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/node/src/manager/node.rs, crates/node/src/manager/node/start_epoch.rs, crates/types/src/gas_accumulator.rs
- **Source**: tn-review (R2, R4 — independently converged)

- **Verdict (findings-verifier)**: CONFIRMED (latent) — Confidence HIGH — Verified severity **Medium** (held; solo verifier proposed High).
  - **Evidence**: both recovery sources are block-derived and leave absent workers as-is (=MIN after restart): `latest_base_fee_per_worker` (`node.rs:282-291`) and `seed_base_fees_from_chain` (`run_epoch.rs:689-704`, fills only ids in `chain_fees`). `adjust_base_fees` computes idle/added-worker fees at the boundary (`run_epoch.rs:631-633`) but that value lives only in live memory until the worker's first block. A node entering E mid-epoch adopts fees only for workers with on-chain E-blocks; a `Static`/real-`Eip1559` worker with zero E-blocks stays at MIN and exact-match validation rejects its first batch epoch-wide. No config-aware frontier derivation exists.
  - **Severity adjudication**: held at Medium (not High). Same consensus-divergence class as F1/F4 (verified-safe obs #10) but narrower blast radius (one idle worker, not the whole node) and double-latent (multi-worker + non-default config); sits between F1 (High) and F4 (Low).
  - **Proposed fix**: on mid-epoch entry, for any worker absent from `chain_fees` derive its fee from its on-chain `WorkerFeeConfig` (`Static`->fee; `Eip1559`->recompute) rather than leaving default MIN, mirroring `adjust_base_fees`.

### F7: Genesis writer never checks the WorkerConfigs constructor result — a ceremony typo silently bricks fee governance forever
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/tn-reth/src/lib.rs:1272`
- **Claim**: `transact_pre_genesis_create` returns `Ok` even when the constructor REVERTS; lib.rs:1272-1276 only `debug!`-logs the `ExecutionResult` and commits. The contract reverts on `strategy > MAX_STRATEGY(=1)` or empty configs, and the CLI (`parse_worker_fee_configs`, telcoin-network-cli/src/genesis/mod.rs:159) accepts strategies 2..=255, as does `test_genesis_with_consensus_registry_and_workers` (test_utils.rs:727). On revert, genesis still installs the runtime code with EMPTY storage (lib.rs:1303, 1339-1343): `numWorkers()=0` and `owner()=address(0)` — permanently unownable and unconfigurable. The stack then masks it: decode yields `count=0, configs=[]`, `set_num_workers(0)` clamps to 1 (gas_accumulator.rs:338), the per-worker loop runs zero iterations, and fees freeze at MIN with no error — discoverable only when governance later tries to set a config on an ownerless contract.
- **Key Question**: After a reverted WorkerConfigs constructor in the tmp EVM, does `create_consensus_registry_genesis_accounts` return Ok with runtime code + empty storage at WORKER_CONFIGS_ADDRESS (i.e., is `ExecutionResult` ever checked for success before `take_bundle` at lib.rs:1284)?
- **Relevant Files**: crates/tn-reth/src/lib.rs, crates/tn-reth/src/test_utils.rs, crates/telcoin-network-cli/src/genesis/mod.rs, tn-contracts/src/consensus/WorkerConfigs.sol
- **Source**: tn-review (R3)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Medium** (unchanged).
  - **Evidence**: `transact_pre_genesis_create`/`create_consensus_registry_genesis_accounts` only `debug!`-log the `ExecutionResult` then commit via `take_bundle` (`tn-reth/src/lib.rs:1272-1284`) with no `is_success()` check, so a constructor Revert returns Ok with runtime code + empty storage (owner=address(0), numWorkers()=0). `parse_worker_fee_configs` accepts strategy 2..=255 with no MAX_STRATEGY check (`telcoin-network-cli/src/genesis/mod.rs:159`), as does the test helper (`test_utils.rs:727`); WorkerConfigs.sol reverts on `strategy>MAX_STRATEGY(=1)` or empty configs. Downstream masks it: decode count=0 -> `set_num_workers(0)` clamps to 1 -> zero-iteration loop -> fees frozen at MIN, discoverable only when governance later configures an ownerless contract.
  - **Note**: the identical unchecked-commit pattern governs the ConsensusRegistry deploy in the same function — a latent-High concern worth a follow-up.
  - **Proposed fix**: assert `result.result.is_success()` (bail on Revert/Halt) before `take_bundle` for every pre-genesis create; validate `strategy <= MAX_STRATEGY` and non-empty configs in `parse_worker_fee_configs` so a bad ceremony fails loudly.

### F8: Shipped doc comments assert the invariant the tracked blocker refutes; the referenced KNOWN LIMITATION marker does not exist at HEAD
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/node/src/manager/node/run_epoch.rs:601` (also 590-592; types/src/gas_accumulator.rs:41-43)
- **Claim**: `close_epoch` docs state the None paths "defer to epoch-start chain seeding so a catching-up node never diverges from the chain" (run_epoch.rs:601-603) and the seeding docs claim the paths "adopt the chain's fee via epoch-start seeding, preserving the base-fee-from-chain invariant" (590-592); the `gas_accumulator` module docs state the container "keeps the value the live producer just computed at the boundary" (gas_accumulator.rs:42). All are false in exactly the F1 window, as the author's own `issues/blocker-restart-boundary-recovery.md` documents ("the module docs … are wrong for the restart case"). That issue references an in-code `KNOWN LIMITATION` comment in `close_epoch`, but grep finds none anywhere in `crates/` at HEAD — the final wording (commit `5b5a69d4`) asserts correctness instead. The consensus-critical seam ships documenting itself as safe; a maintainer wiring activation from the code alone would conclude the recovery hole is closed.
- **Key Question**: Do the None-close paths ever reach a state where `tip_epoch == entered_epoch` at the next entry (making the doc true), or is seeding structurally skipped after every None close as the blocker states?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/types/src/gas_accumulator.rs, issues/blocker-restart-boundary-recovery.md
- **Source**: tn-review (R2, R4 — independently converged)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (down from Medium).
  - **Evidence**: all three doc sites overstate safety in the F1 window — `close_epoch` "defer to epoch-start chain seeding so a catching-up node never diverges" (`run_epoch.rs:601-603`), seeding "preserving the base-fee-from-chain invariant" (`:590-592`), gas_accumulator "keeps the value the live producer just computed at the boundary" (`gas_accumulator.rs:41-43`) — but a None-close leaves the next-entry tip at N's closing block, so seeding self-disables (`:697-699`) with nothing to seed from. `grep -rn "KNOWN LIMITATION" crates/` returns zero hits though `issues/blocker-restart-boundary-recovery.md` references such a marker.
  - **Severity adjudication**: Medium—>Low — documentation-accuracy defect; the underlying runtime hole is F1/the tracked blocker (already High).
  - **Proposed fix**: correct the three comments to state the None-close paths do NOT re-seed at the immediately-following entry (tip is the just-closed boundary block), referencing the blocker; restore an in-code `KNOWN LIMITATION:` marker at the None-close site or drop the stale issue reference.

### F9: EIP-1559 e2e asserts only inequality, never the exact `+max(1, fee/8)` step
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:219`
- **Claim**: The only checks are `fees.len() >= 2`, windowed `nf >= pf`, and `last_fee > MIN` (lines 219-242). With `target_gas=1` and exactly one 21k-gas tx per epoch, the expected series is deterministic (+1 per gas-bearing boundary), so consecutive-epoch records could assert `nf == pf + max(1, pf/8)`; a wrong denominator, double-applied adjustment, or fee applied one epoch late all pass the current inequalities. The single-worker testnet additionally makes any wrong-worker-id bug structurally invisible in all three e2e tests. The static tests, by contrast, do pin exact values.
- **Key Question**: Would a bug that applies the 12.5% adjustment twice per boundary (or uses denominator 4 instead of 8) fail any assertion in `test_eip1559_fee_rises_at_epoch_boundaries`?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: PARTIALLY VALID — Confidence HIGH — Verified severity **Low** (down from Medium).
  - **Evidence**: the only checks are `fees.len()>=2`, windowed `nf >= pf`, `nf >= MIN`, `last_fee > MIN` (`basefee.rs:219-242`); none pin the exact `nf == pf + max(1, pf/8)` step, so a double-applied adjustment, denominator-4 error, or fee-late-by-one-epoch all pass, and the single-worker testnet hides wrong-worker-id bugs. PARTIALLY VALID: the looseness is acknowledged in-code and an exact per-boundary series is not reliably assertible in THIS skip-based test without restructuring; the unit test `eip1559_config_moves_fee_with_gas_vs_target` already pins the formula.
  - **Severity adjudication**: Medium—>Low — missing-precision in one e2e assertion, not a production defect.
  - **Proposed fix**: add a deterministic assertion at the unit/IT layer (or a restructured e2e recording one fee per boundary) checking `nf == compute_next_base_fee_eip1559(pf, gas_used, target_gas)`; keep the e2e inequality as a smoke check.

### F10: Mid-epoch positioning via raw sleep, enforced by a hard assert
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:308`
- **Claim**: `wait_for_epoch_at_least` returns at an unknown phase inside the epoch (1s poll; possibly seconds after the boundary, or in a later epoch than requested). The test then sleeps `EPOCH_DURATION / 2` and hard-asserts the epoch has not advanced (lines 310-314) — >~4s of scheduling/RPC drift in a 10s epoch fails the test before the kill/restart under test even happens. `land_priced_tx_mid_epoch`'s fixed 3s sleep (line 465) makes the same phase assumption but tolerantly. No helper computes phase from the on-chain epoch start; "mid-epoch" is aspiration, not measurement.
- **Key Question**: What is the worst-case phase offset of `wait_for_epoch_at_least`'s return relative to the epoch boundary under loaded CI (two 4-node testnets in parallel per nextest e2e group), and does 5s of margin in a 10s epoch survive it?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, .config/nextest.toml
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: PARTIALLY VALID — Confidence MEDIUM — Verified severity **Low** (down from Medium).
  - **Evidence**: `wait_for_epoch_at_least` (1s poll) returns at an unknown in-epoch phase; the test then sleeps `EPOCH_DURATION/2` and hard-asserts the epoch is unchanged (`basefee.rs:307-314`), which fails if the return landed >~5s into a 10s epoch under load. PARTIALLY VALID/reduced: it fails SAFE (loud pre-kill failure, never a false pass), nominal margin is ~4-5s (a moderate, not guaranteed, flake), and the "overshoot into a later epoch" framing is imprecise (overshoot alone is harmless — the assert is relative to the just-observed epoch).
  - **Severity adjudication**: Medium—>Low — fail-safe timing fragility in one test.
  - **Proposed fix**: compute phase from the on-chain epoch start (`getCurrentEpochInfo` timestamp) and sleep to a measured mid-epoch offset, or retry the position acquisition instead of a raw sleep + hard assert.

### F11: Final restart assertion is silently skipped on timeout
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:368`
- **Claim**: The last check is `if let Some(f) = wait_for_block_fee(&client_urls[kill_idx], after_block, EPOCH_DURATION * 4)? { assert_eq!(...) }` — when the restarted node never catches up to `after_block` within the budget, `Ok(None)` skips the assert and the test passes. The analogous earlier check (lines 343-350) correctly converts `None` into a hard error via `ok_or_else`. As written, the only assertion that touches the restarted node *after* new blocks were produced is optional, compounding F2's vacuous-pass surface.
- **Key Question**: Is there any scenario where silently skipping the post-restart block check is intended, rather than an inconsistency with the `ok_or_else` treatment at basefee.rs:344?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Medium** (unchanged).
  - **Evidence**: the final post-restart check is `if let Some(f) = wait_for_block_fee(&client_urls[kill_idx], after_block, EPOCH_DURATION*4)? { assert_eq!(...) }` (`basefee.rs:368-373`); `wait_for_block_fee` returns `Ok(None)` on timeout (`:531-544`), silently skipping the assert, whereas the analogous catch-up check uses `.ok_or_else(...)?` to hard-error (`:343-350`). So the only assertion touching the restarted node after new blocks is optional.
  - **Proposed fix**: replace `if let Some` with `.ok_or_else(|| eyre!("restarted validator did not reach block {after_block} ..."))?` then `assert_eq!`, matching `:344`. (Compounds F2.)

### F12: Fail-open worker-count test cannot distinguish fail-open from the ≥1 clamp
- **Severity**: Medium
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/node/tests/it/main.rs:598`
- **Claim**: The test starts from `GasAccumulator::new(1)` and asserts the count is still 1 after `sync_num_workers_from_chain` on a chain missing the contract. But `set_num_workers` clamps to ≥1, so a regression where the failed read yields `Ok((0, []))` and the code proceeds to resize would produce the identical observable (0 → clamp → 1). Starting from `GasAccumulator::new(3)` and asserting the count stays 3 would make the assertion actually discriminate the `Err → keep current` branch (node.rs:220-238).
- **Key Question**: If `get_worker_fee_configs_at_block` on an absent contract returned `Ok((0, vec![]))` instead of `Err`, would `test_sync_num_workers_fail_open_when_contract_absent` still pass?
- **Relevant Files**: crates/node/tests/it/main.rs, crates/node/src/manager/node.rs, crates/types/src/gas_accumulator.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (down from Medium).
  - **Evidence**: the test starts from `GasAccumulator::new(1)` and asserts count==1 after a contract-absent sync (`node/tests/it/main.rs:587-610`); `set_num_workers` clamps `max(1)` and early-returns when unchanged (`gas_accumulator.rs:336-343`), so a regression returning `Ok((0, vec![]))` (0->clamp->1, already len 1) yields the identical observable — the Err->keep-current branch is not discriminated. (`test_sync_num_workers_from_chain_adjusts_to_on_chain_count` does exercise real 1->2/3->2 resizes.)
  - **Severity adjudication**: Medium—>Low — test-discrimination gap; production fail-open code is correct.
  - **Proposed fix**: start from `GasAccumulator::new(3)` and assert the count stays 3 after the failed read (0->clamp->1 would drop it to 1, distinguishing fail-open from the clamp).

### F13: Basefee e2e tests run in no hosted CI job; ignore reason is copy-pasted and wrong
- **Severity**: Medium
- **Category**: Architecture
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:82`
- **Claim**: Hosted PR CI (`.github/workflows/pr.yaml:112`) runs `cargo nextest run --locked --workspace ... --profile ci` with no `--run-ignored`, so the three `#[ignore]` basefee tests never execute there. They run only in the maintainer-local attestation flow (`etc/test-and-attest.sh:102`, via `make attest`, Makefile:89) and `make test-e2e` (Makefile:116); `make public-tests` (Makefile:186-187) filters on `test_epoch`/`test_restarts`, matching none of the three names. Flake statistics and regressions surface only on maintainer machines. The ignore string "should not run with a default cargo test, run restart tests as seperate step" (lines 82, 157, 260) is copy-pasted from restarts.rs — two of the three are not restart tests, and "seperate" is a typo.
- **Key Question**: Is `make attest` an enforced pre-merge gate for every PR, or can merges land without any execution of the ignored e2e suite?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, .github/workflows/pr.yaml, etc/test-and-attest.sh, Makefile
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Medium** (unchanged).
  - **Evidence**: hosted PR CI runs `cargo nextest ... --profile ci` with no `--run-ignored` (`.github/workflows/pr.yaml`), so the three `#[ignore]`d basefee tests never execute there; `make public-tests` filters `test_epoch`/`test_restarts` (Makefile), matching none of the three names; they run only via `make attest`/`make test-e2e` -> `etc/test-and-attest.sh`. The ignore string (typo "seperate") is copy-pasted from restarts.rs onto two non-restart tests.
  - **Nuance**: an on-chain attestation gate DOES run them pre-attestation, but it is honor-based/local (no hosted runner), so coverage is fragile and F3/F10 flakes surface only on maintainer machines; the typo alone is Info.
  - **Proposed fix**: add a hosted CI job (or extend an e2e job) running `-E 'test(basefee)' --run-ignored all`; fix each `#[ignore]` reason; make the attest e2e run a required status check.

### F14: Unknown-strategy fallback reinterprets a future strategy's `value` as an EIP-1559 gas target — mixed-version committee fee divergence
- **Severity**: Low
- **Category**: Fork Safety
- **Label**: NEW (dead code until a contract code-swap hard fork introduces strategy ≥2)
- **Location**: `crates/tn-reth/src/lib.rs:1601`
- **Claim**: For strategy ≥2, `worker_fee_configs_inner` warns and decodes `Eip1559 { target_gas: value }` (lib.rs:1601-1613) "to preserve liveness instead of halting" — rather than erroring into the caller's fail-open keep-current arm. Since base fee is enforced by exact-match batch validation, activating a new strategy before 100% of binaries understand it splits the committee: upgraded nodes apply the real semantics, fallback nodes drive fees from `value` misread as a gas target (a small percentage-style `value` forces +12.5%/epoch runaway growth), and the two camps reject each other's batches for whole epochs — worse than a halt because it presents as Byzantine batch spam, not a version error. Today the branch is unreachable (the deployed non-upgradeable contract rejects strategy >1), so it can only fire after a deliberate contract swap — which makes silent reinterpretation strictly worse than a loud error for detecting a mis-sequenced rollout. Nothing enforces the upgrade-before-set governance sequencing.
- **Key Question**: If strategy id 2 is set on-chain while part of the committee runs a binary that only knows 0/1, do the two populations compute different next-epoch base fees from identical chain state — and is there any version gate preventing that?
- **Relevant Files**: crates/tn-reth/src/lib.rs, crates/node/src/manager/node/run_epoch.rs, crates/batch-validator/src/validator.rs, tn-contracts/src/consensus/WorkerConfigs.sol
- **Source**: tn-review (R2, R3, R4 — independently converged)

- **Verdict (findings-verifier)**: CONFIRMED (latent) — Confidence HIGH — Verified severity **Low** (unchanged).
  - **Evidence**: for strategy>=2, `worker_fee_configs_inner` warns and returns `Eip1559 { target_gas: value }` (`tn-reth/src/lib.rs:1601-1613`) rather than Err; because base fee is exact-match validated, a strategy-2 config set while part of the committee only knows 0/1 makes upgraded vs fallback nodes compute different next-epoch fees from identical state and reject each other's batches, with no version gate. Unreachable on the current non-upgradeable contract (rejects strategy>1) — dead code until a deliberate contract code-swap.
  - **Severity adjudication**: Low (dead code today); forward-compat fork risk that becomes High-impact if MAX_STRATEGY is raised without a coordinated binary rollout.
  - **Proposed fix**: return Err for unknown strategies (routing into the caller's fail-open keep-current, identical on every node) instead of silently reinterpreting `value` as a gas target; document an upgrade-before-set governance sequencing requirement.

### F15: `Static` strategy has no bounds — fee=0 bypasses MIN_PROTOCOL_BASE_FEE and fee=u64::MAX halts inclusion with a prohibitive recovery cost
- **Severity**: Low
- **Category**: State & Funds
- **Label**: NEW
- **Location**: `crates/node/src/manager/node/run_epoch.rs:679`
- **Claim**: `next_base_fee_for_config`'s Static arm returns the governance value raw — unlike the Eip1559 arm, which floors at MIN_PROTOCOL_BASE_FEE (gas_accumulator.rs:381). `Static { fee: 0 }` is contract-legal by design (WorkerConfigs.sol:42-43) and propagates to zero-base-fee blocks; `Static { fee: u64::MAX }` sets every batch and block base fee to u64::MAX for at least one full epoch, excluding all transactions whose `max_fee_per_gas` < u64::MAX — including the governance transaction needed to undo it unless the owner pays an astronomical fee. No clamp or sanity band exists in the Rust decode (lib.rs:1600) or the contract setter; one bad governance value converts into an epoch-long, expensive-to-reverse inclusion halt.
- **Key Question**: With every worker's base fee at u64::MAX, can the WorkerConfigs owner actually land `setWorkerConfig` within the epoch, or is recovery deferred until validators coordinate out-of-band?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/types/src/gas_accumulator.rs, tn-contracts/src/consensus/WorkerConfigs.sol, crates/batch-validator/src/validator.rs
- **Source**: tn-review (R3)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Medium** (up from Low).
  - **Evidence**: the `Static` arm returns `fee` raw (`run_epoch.rs:679`), unlike the `Eip1559` arm which floors at MIN (`gas_accumulator.rs:381`); no clamp in the Rust decode (`lib.rs:1600`) or the contract setter (WorkerConfigs.sol). `Static{fee:0}` yields sub-MIN/zero-fee blocks (bypassing the protocol floor); `Static{fee:u64::MAX}` prices every batch/block at u64::MAX for >=1 full epoch, excluding all txs with `max_fee_per_gas < u64::MAX` — including the corrective `setWorkerConfig` (which cannot take effect until the NEXT boundary and would itself need an astronomical fee).
  - **Severity adjudication**: Low—>Medium — governance-gated (trusted, not externally exploitable), but no guardrail in either layer, `fee=0` silently violates a protocol invariant, and `fee=u64::MAX` is an epoch-long, expensive/deferred-to-reverse self-brick.
  - **Proposed fix**: clamp the `Static` arm to `[MIN_PROTOCOL_BASE_FEE, SANE_MAX]` in `next_base_fee_for_config` (and/or validate the range in the contract setter and CLI), rejecting or flooring out-of-band values.

### F16: D.4 widened at HEAD — per-worker fee restoration now hangs off the unpinned dual-source range
- **Severity**: Low
- **Category**: Determinism
- **Label**: KNOWN (tracked in `issues/dual-header-read-robustness.md`) — severity revisit recommended
- **Location**: `crates/node/src/manager/node.rs:166`
- **Claim**: The tracked issue names both HEAD sites; the delta at HEAD is a *widening*, not a new bug. On main, catchup's fee restore came straight from the finalized header (immune to the (finalized, canonical-tip-epoch-state) pair disagreeing — only gas stats used the range). At HEAD, both the catchup fee restore (node.rs:166-174 via `latest_base_fee_per_worker`) and the new epoch-entry seeding (run_epoch.rs:159-171) hang off the unpinned two-source range; an inconsistent pair now silently drops ALL per-worker fee restoration (consensus-affecting, exact-match validated) instead of only gas totals, and there are two sites instead of one. The only concrete lag mechanism found (finalize-after-canonicalize window in `execute_consensus_output`) is benign for the seeding *decision*.
- **Key Question**: Given the fee restore's move from a single-source header read to the dual-source range, should the tracked issue's severity be revisited (consequence class changed from "gas stats lost" to "fees left at MIN")?
- **Relevant Files**: crates/node/src/manager/node.rs, crates/node/src/manager/node/run_epoch.rs, issues/dual-header-read-robustness.md
- **Source**: tn-review (R2)

- **Verdict (findings-verifier)**: CONFIRMED (widening) — Confidence HIGH — Verified severity **Low** (unchanged).
  - **Evidence**: `git show aa67c773:crates/node/src/manager/node.rs` shows pre-stack main restored the fee directly from the finalized header — `.base_fee(0).set_base_fee(block.base_fee_per_gas...)` — a single source (worker 0), with the two-source `blocks_for_range` used only for gas stats. At HEAD both the catchup restore (`node.rs:166-174` via `latest_base_fee_per_worker`) and the new entry seeding (`run_epoch.rs:159-171`) derive per-worker fees from the unpinned `blockHeight..=tip` range across two header sources, so an inconsistent (empty) range now silently drops per-worker fee restoration (consensus-affecting) at TWO sites instead of losing only gas totals at one.
  - **Severity adjudication**: Low (unchanged). Answers the tracked issue's "revisit severity" ask: consequence class widened (gas-stats-lost -> fees-left-at-MIN) but likelihood stays low under this node's instant BFT finality (finalized==canonical tip). Net Low.
  - **Proposed fix**: pin both range ends to a single header (use `tip.number` for the start, or the finalized header for both) at both sites; add the dual-read-inconsistency regression test the issue requests.

### F17: D.5 residual — close-time/entry-time same-block identity is implicit and unasserted (live-bug claim refuted)
- **Severity**: Low
- **Category**: Determinism
- **Label**: NEW (residual of a refuted candidate)
- **Location**: `crates/node/src/manager/node/run_epoch.rs:623` (doc claim; entry read node.rs:200-205)
- **Claim**: The candidate bug — close-time `adjust_base_fees` (reads canonical tip) and entry-time `sync_num_workers_from_chain` (reads at `blockHeight - 1`) observing different WorkerConfigs states — was REFUTED with evidence: the canonical head is set per-block inside `execute_payload` (payload_builder.rs:223) *before* the engine-update send (tn-reth lib.rs:868-878) that gates `wait_for_consensus_execution`; no path executes further blocks between close and re-entry (boundary path discards leftovers, run_epoch.rs:402); and `blockHeight(N+1) = closing block + 1` (ConsensusRegistry.sol:988), so `blockHeight - 1` IS the closing block both reads see. The residual: this identity is entirely implicit — nothing asserts `canonical_tip().number + 1 == epoch_info.blockHeight` at either site, so correctness silently depends on the contract's `+1` convention and the no-execution-between-close-and-entry sequencing. A future change to either (e.g. multi-block boundary handling, contract upgrade) would break the identity with no tripwire.
- **Key Question**: Does the refutation chain hold under a multi-block boundary output (is the tip always the LAST block of the closing output when `adjust_base_fees` runs), and should an assert or regression test pin `canonical_tip().number + 1 == blockHeight`?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/node/src/manager/node.rs, crates/engine/src/payload_builder.rs, crates/tn-reth/src/lib.rs
- **Source**: tn-review (R2 refutation; residual filed by orchestrator per plan)

- **Verdict (findings-verifier)**: CONFIRMED — refutation HOLDS (not a live bug) — Confidence HIGH — Verified severity **Low** (unchanged; NOT escalated).
  - **Evidence**: the candidate divergence is refuted, including under a multi-block boundary output. The close-time WorkerConfigs read is at `canonical_tip()` (`lib.rs:1642-1646` -> `worker_fee_configs_inner`), which after `wait_for_consensus_execution` (`run_epoch.rs:610`) is the epoch's LAST executed closing block — the close system-call is pinned to the last batch (`output.rs:240` `close_epoch_for_last_batch`); no path executes further blocks between close and re-entry (boundary path discards leftovers, `run_epoch.rs:402`); and `blockHeight = uint64(block.number) + 1` at close (ConsensusRegistry.sol:988), so entry's `blockHeight-1` read resolves to the same closing block. The residual is real: nothing asserts `canonical_tip().number + 1 == epoch_info.blockHeight`.
  - **Proposed fix**: add `debug_assert!` plus a hard runtime check `canonical_tip().number + 1 == epoch_info.blockHeight` at both the close and entry reads, and a regression test pinning the identity across a multi-block boundary output so a future contract/boundary change trips loudly.

### F18: `set_num_workers` quiescence precondition is violated on the ModeChange re-entry path — value-stability, not quiescence, is what protects the code
- **Severity**: Low
- **Category**: Concurrency
- **Label**: NEW
- **Location**: `crates/node/src/manager/node/run_epoch.rs:151` (with close_epoch.rs:114-161, run_epoch.rs:403-413)
- **Claim**: `set_num_workers` documents "Callers must only invoke this while no consensus output is executing" (gas_accumulator.rs:333-335). Verified true for startup, Initial/NewEpoch entry, and the close-time resize (both follow `wait_for_consensus_execution`). But on a non-boundary exit, `send_leftover_consensus_output_to_engine` forwards outputs and returns `None` without any execution wait; `run_epoch` returns `ModeChange` and the next iteration calls `sync_num_workers_from_chain` → `set_num_workers` (and the seeding reads at 159-182) while the engine may still be executing those outputs and calling `inc_block`. Today benign: the same-epoch read returns an identical count (no-op early-return under the write lock, gas_accumulator.rs:340-342), grows are index-stable, and a shrink requires a prior entry-sync failure in which case committed output cannot contain the removed ids. No current panic path — but the documented invariant is not what actually protects the code, which is fragile against future callers.
- **Key Question**: Can the ModeChange re-entry sync ever observe a smaller count than a worker id present in the in-flight leftover outputs (i.e., can entry-sync failure + mid-epoch success produce a shrink below a committed batch's worker id)?
- **Relevant Files**: crates/node/src/manager/node/run_epoch.rs, crates/node/src/manager/node/close_epoch.rs, crates/types/src/gas_accumulator.rs, crates/engine/src/lib.rs
- **Source**: tn-review (R1)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (unchanged).
  - **Evidence**: `set_num_workers` documents a quiescence precondition (`gas_accumulator.rs:333-335`); the ModeChange re-entry violates it — `send_leftover_consensus_output_to_engine` forwards outputs and returns None with no execution wait (`close_epoch.rs:114-161`), then the next iteration's `sync_num_workers_from_chain`->`set_num_workers` (and seeding reads) run while the engine may still be executing/`inc_block`-ing. Benign today: a same-epoch read returns an identical count -> no-op early-return under the write lock (`:340-342`), grows are index-stable, and a shrink below an in-flight worker id cannot happen (leftover outputs are pre-boundary epoch-N, ids < count_N). Value-stability + the id bound, not quiescence, protect the code.
  - **Proposed fix**: either wait for execution of the drained leftovers before the ModeChange re-entry sync, or weaken the docstring to the true invariant (safe iff it cannot shrink below any in-flight worker id) and add a `#[should_panic]`/regression test on the ModeChange path.

### F19: `set_worker_base_fee` silently no-ops for an uninitialized worker id
- **Severity**: Low
- **Category**: Error Handling
- **Label**: NEW
- **Location**: `crates/node/src/engine/inner.rs:185`
- **Claim**: `if let Some(worker) = self.workers.get(worker_id as usize)` drops the update without any log or error when the worker's components are missing. Today the single call site (start_epoch.rs:411) runs after init-or-respawn of worker 0, so it cannot fire; but the method is the designated per-epoch repricing hook, and in the multi-worker follow-ups a mismatch between the accumulator's worker count and initialized components would leave that worker's pool charging the previous epoch's `pending_basefee` while its validator and batch builder use the new snapshot — admitting underpriced transactions into batches, with zero diagnostic. A `debug_assert!`/`warn!` on the None arm would make the invariant visible.
- **Key Question**: Is there any signal (log, metric, error) when `set_worker_base_fee` is called for a worker id with no components?
- **Relevant Files**: crates/node/src/engine/inner.rs, crates/node/src/engine/mod.rs, crates/node/src/manager/node/start_epoch.rs
- **Source**: tn-review (R4)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (unchanged; borderline Info).
  - **Evidence**: `set_worker_base_fee` is `if let Some(worker) = self.workers.get(worker_id as usize) { ... }` (`node/src/engine/inner.rs:185`) with no else — a missing worker silently drops the update. The sole call site runs after worker-0 init/respawn (`start_epoch.rs:411`) so it cannot fire today, but as the per-epoch repricing hook a future multi-worker count/component mismatch would leave a worker's pool on the prior epoch's `pending_basefee` with no diagnostic.
  - **Proposed fix**: add a `warn!`/`debug_assert!` on the None arm citing `worker_id` and the initialized worker count so the invariant breach is visible.

### F20: `land_tx_and_read_fee` infers the tx block as "current tip" instead of using a receipt
- **Severity**: Low
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:511`
- **Claim**: After the recipient balance rises, the helper reads `get_block_number` and treats that tip as the tx's block. Between the 1s-granularity balance poll and the tip read, an epoch-close block can be appended, mis-attributing block number and epoch; a late-landing tx from a previous skipped iteration can also satisfy `bal > before_bal` (line 497) on behalf of the wrong transfer. Today the fee value is identical for any block in the same epoch, so assertions survive, but the recorded (epoch, block, fee) tuples driving the eip1559 monotonic windows are approximate. `send_tel` (common.rs:561) discards the tx hash, so no receipt-based exact lookup is possible without plumbing.
- **Key Question**: Can the tip advance past the tx-bearing block between balance confirmation and `get_block_number` in a way that changes the recorded fee (not just the block number)?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, crates/e2e-tests/tests/it/common.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (unchanged).
  - **Evidence**: `land_tx_and_read_fee` reads `get_block_number` after the balance rises and treats that tip as the tx's block (`basefee.rs:483-518`); between the 1s balance poll and the tip read an epoch-close block can be appended, and a late tx from a prior skipped iteration can satisfy `bal > before_bal`; `send_tel` discards the hash (`common.rs:561`) so no receipt lookup is possible. Impact is bounded — the base fee is constant within an epoch, so the RECORDED FEE stays correct today; only (epoch, block) attribution is approximate (interacts with F3/F9).
  - **Proposed fix**: thread the tx hash out of `send_tel` and read the receipt's `blockNumber` for exact attribution (also removes the late-landing-tx ambiguity).

### F21: Helper duplication inside and across test modules
- **Severity**: Low
- **Category**: Architecture
- **Label**: NEW
- **Location**: `crates/e2e-tests/tests/it/basefee.rs:277`
- **Claim**: The restart test re-inlines `start_testnet`'s body (lines 277-287 duplicate 396-411). `current_epoch`/`wait_for_epoch_at_least` and `EPOCH_DURATION = 10` are private to basefee.rs while epochs.rs re-derives the same `getCurrentEpochInfo` polling inline (epochs.rs:36, 84-109); these belong in common.rs so future basefee tests don't copy them a third time. Separately, `test_sync_then_catchup_recovers_two_worker_accumulator` (node/tests/it/main.rs:612) adds the fourth near-identical ExecutorEngine scaffold in that file (`ExecutorEngine::new` at lines 94, 315, 483, 659).
- **Key Question**: Is there a reason the epoch-wait and testnet-boot helpers must stay module-private rather than moving to `tests/it/common.rs`?
- **Relevant Files**: crates/e2e-tests/tests/it/basefee.rs, crates/e2e-tests/tests/it/epochs.rs, crates/e2e-tests/tests/it/common.rs, crates/node/tests/it/main.rs
- **Source**: tn-review (R5)

- **Verdict (findings-verifier)**: CONFIRMED — Confidence HIGH — Verified severity **Low** (unchanged; borderline Info).
  - **Evidence**: the restart test re-inlines `start_testnet`'s body (`basefee.rs:277-287` vs `396-411`); `current_epoch`/`wait_for_epoch_at_least`/`EPOCH_DURATION` are private to basefee.rs while epochs.rs re-derives the same `getCurrentEpochInfo` polling inline (`epochs.rs:36,84-109`); `test_sync_then_catchup_recovers_two_worker_accumulator` adds a 4th near-identical ExecutorEngine scaffold (`node/tests/it/main.rs` `ExecutorEngine::new` at 94/315/483/659). No blocker to hoisting the helpers.
  - **Proposed fix**: move `start_testnet`, `current_epoch`, `wait_for_epoch_at_least`, `EPOCH_DURATION` into `tests/it/common.rs`; factor the ExecutorEngine scaffold into a shared test builder.

### F22: `inc_block` panic tripwire fires with no diagnostic linking it to the failed count sync
- **Severity**: Info
- **Category**: Memory & Safety
- **Label**: NEW
- **Location**: `crates/types/src/gas_accumulator.rs:286` (caller: crates/engine/src/payload_builder.rs:179-183)
- **Claim**: `inc_block(batch.worker_id, ...)` executes inside a blocking engine task with `expect("valid worker id")`; the only bound on `batch.worker_id` in committed output is each validating worker's equality check against its own id (validator.rs:48-51) — no execution-time check against the on-chain count. Under count divergence (F5 conditions) the panic kills the engine task and halts the node, which the docs state is intentional ("halting beats silently diverging gas totals"). Recorded as the reachability map: the tripwire fires on the first multi-worker batch after any fail-open count miss, with no diagnostic pointing at the failed contract read that caused it (the warn at node.rs:254 may be hours earlier in the log).
- **Key Question**: Should the expect at gas_accumulator.rs:286 carry the accumulator len and worker id (or the sync-failure state) so the by-design halt is diagnosable?
- **Relevant Files**: crates/types/src/gas_accumulator.rs, crates/engine/src/payload_builder.rs, crates/batch-validator/src/validator.rs
- **Source**: tn-review (R1)

- **Verdict (findings-verifier)**: NOT VERIFIED (Info) — observation only; skipped per verification-tiering policy (INFO). Reachability is the by-design tripwire documented at `gas_accumulator.rs:275-279`; see F5/F22's diagnosability suggestion (attach accumulator len + worker id to the `expect`).

### F23: `getAllWorkerConfigs` is capped by the 30M-gas system call — worker counts past ~4–5k make fee config permanently unreadable
- **Severity**: Info
- **Category**: Bugs
- **Label**: NEW
- **Location**: `crates/tn-reth/src/lib.rs:1566`
- **Claim**: The read runs through `transact_system_call` with a hard `gas_limit: 30_000_000` (crates/tn-reth/src/evm/mod.rs:173). `getAllWorkerConfigs` does ~3 cold SLOADs per worker, so around 4–5k workers (well under the uint16 ceiling of 65535 that `setNumWorkers` permits) the call goes out-of-gas, the result is non-Success, and lib.rs:1574 bails — every epoch, on every node, identically. Both consumers fail open, so fee adjustment and count sync freeze network-wide with only warn logs until governance shrinks the count. No divergence (the failure is state-deterministic); a documented-ceiling issue, not an exploit.
- **Key Question**: At what exact numWorkers does `getAllWorkerConfigs` exceed 30M gas, and is that ceiling documented anywhere governance-facing before `setNumWorkers` allows crossing it?
- **Relevant Files**: crates/tn-reth/src/lib.rs, crates/tn-reth/src/evm/mod.rs, crates/node/src/manager/node/run_epoch.rs, tn-contracts/src/consensus/WorkerConfigs.sol
- **Source**: tn-review (R3)

- **Verdict (findings-verifier)**: NOT VERIFIED (Info) — observation only; skipped per verification-tiering policy (INFO). Documented-ceiling item (30M system-call gas cap, `evm/mod.rs:173`); state-deterministic fail-open, no divergence.

### Verified-safe observations (no finding filed — preserve verbatim)

Candidate risks the readers investigated and **refuted with code evidence**, recorded so future reviews don't re-litigate them:

1. **D.5 core claim refuted** — close-time and entry-time config reads resolve to the same closing block by construction (see F17 for the evidence chain and the residual).
2. **Resize-vs-seed ordering race refuted** — startup (`sync` node.rs:443 → `catchup` node.rs:444) and epoch entry (sync :151 → seed :158-182 → replay :214 → snapshots :248/312) are strictly sequential; the engine executes nothing in between; the only fee readers are the post-seed snapshots (R2).
3. **Lock discipline sound** — consistent `RwLock.read → per-slot Mutex` order; the write lock never nests a slot lock; no guard is held across an `.await`; `RewardsCounter` has a single both-locks path with no inversion (R1).
4. **Split-brain containers eliminated** — at HEAD no production code holds a `BaseFeeContainer` outside the accumulator; validator, batch builder, and tx pool all take u64 snapshots at epoch start. The u64 snapshot additionally *fixes* a pre-existing race where the outgoing epoch's validator would flip to the new fee mid-teardown when `adjust_base_fees` wrote the shared container (R1, R4).
5. **No old-fee batch leak at the boundary** — `orphan_batches` (close_epoch.rs:73-89) decomposes uncommitted batches into transactions and resubmits them to the pool at the new fee; old-epoch batches are rejected by the epoch check (validator.rs:55-60) before the fee check (R4).
6. **Snapshot construction divergence refuted (intra-node)** — pool fee (start_epoch.rs:411), validator (start_epoch.rs:419-421), and batch builder (run_epoch.rs:312) all consume the same locally-read `base_fee` value from start_epoch.rs:376 (R4).
7. **`target_gas = 0` is safe** — `compute_next_base_fee_eip1559` returns the current fee MIN-floored (gas_accumulator.rs:373-375), no divide-by-zero; `u64::MAX` targets are saturating-safe (R3).
8. **Arity-mismatch bail unreachable against the deployed contract** — `getAllWorkerConfigs` constructs all arrays with length `numWorkers` (bail at lib.rs:1582-1592 is defensive only) (R3).
9. **Test-vs-CLI genesis encoding drift refuted** — `test_genesis_with_consensus_registry_and_workers` calls the same `RethEnv::create_consensus_registry_genesis_accounts` the CLI uses; both execute the real constructor bytecode. Only default *values* differ (tests: active `(0, 30_000_000)`; production CLI: inert `0:0:u64::MAX`) — see coverage gap G8 (R3).
10. **Fee divergence is a liveness failure, not a state fork** — executed blocks derive base fee from the batch or parent header (payload_builder.rs:101,147), never the local accumulator; a diverged node rejects/withholds batches but cannot execute a different chain (R4). This bounds the severity class of F1/F4/F6.

---

## 4. Test Coverage Analysis

### 4.1 Existing tests in the diff (21)

| # | Level | Test | Location | Covers |
|---|---|---|---|---|
| 1 | unit | `set_num_workers_grow_preserves_existing_and_defaults_new_slots` | `types/src/gas_accumulator.rs:485` | grow keeps totals, new slots default |
| 2 | unit | `set_num_workers_shrink_truncates_high_slots` | `types/src/gas_accumulator.rs:504` | shrink truncates; fresh slot on regrow |
| 3 | unit | `set_num_workers_clamps_zero_to_one` | `types/src/gas_accumulator.rs:521` | 0 → 1 clamp |
| 4 | unit | `set_num_workers_same_size_is_noop` | `types/src/gas_accumulator.rs:528` | same-size early return |
| 5 | unit | `clones_observe_resize` | `types/src/gas_accumulator.rs:541` | engine-handle clones see resize (sequential) |
| 6 | unit | `resize_preserves_rewards_counter_identity` | `types/src/gas_accumulator.rs:556` | rewards counter survives resize |
| 7 | unit | `eip1559_config_with_max_target_is_inert_at_min` | `node/.../run_epoch.rs:714` | inert default stays at MIN |
| 8 | unit | `eip1559_config_moves_fee_with_gas_vs_target` | `node/.../run_epoch.rs:729` | fee up/down vs target |
| 9 | unit | `static_config_pins_to_configured_fee` | `node/.../run_epoch.rs:740` | Static pins exactly |
| 10 | unit | `seed_base_fees_adopts_chain_fee_when_tip_in_entered_epoch` | `node/.../run_epoch.rs:757` | seeding adopts chain fee |
| 11 | unit | `seed_base_fees_keeps_computed_value_for_live_producer` | `node/.../run_epoch.rs:772` | live producer keeps computed value |
| 12 | unit | `seed_base_fees_leaves_workers_without_chain_blocks_untouched` | `node/.../run_epoch.rs:787` | absent workers untouched |
| 13 | IT | `test_catchup_accumulator` (extended) | `node/tests/it/main.rs:50` | per-worker poison + restore-from-chain |
| 14 | IT | `test_sync_num_workers_from_chain_adjusts_to_on_chain_count` | `node/tests/it/main.rs:558` | 1→2 grow, 3→2 shrink from chain |
| 15 | IT | `test_sync_num_workers_fail_open_when_contract_absent` | `node/tests/it/main.rs:587` | fail-open keeps count (but see F12) |
| 16 | IT | `test_sync_then_catchup_recovers_two_worker_accumulator` | `node/tests/it/main.rs:612` | startup order: sync → catchup restore |
| 17 | IT | `test_get_worker_fee_configs` | `tn-reth/src/lib.rs:2406` | decode both strategies + count |
| 18 | IT | `test_worker_count_read_at_epoch_start_parent` | `tn-reth/src/lib.rs:2487` | pinned read rule (mid-epoch change → next epoch) |
| 19 | e2e | `test_static_fee_applied_at_epoch_boundary` (`#[ignore]`) | `e2e-tests/tests/it/basefee.rs:83` | Static applied from epoch 1, exact value |
| 20 | e2e | `test_eip1559_fee_rises_at_epoch_boundaries` (`#[ignore]`) | `e2e-tests/tests/it/basefee.rs:158` | fee rises across boundaries (but see F3/F9) |
| 21 | e2e | `test_mid_epoch_restart_recovers_static_fee` (`#[ignore]`) | `e2e-tests/tests/it/basefee.rs:261` | mid-epoch kill+restart recovery (but see F2/F11) |

Plus signature-only adaptations (u64 snapshot API) in `batch-builder/tests/it/build_batches.rs` and `batch-validator/src/validator.rs` tests — not counted as new coverage.

### 4.2 Coverage gaps

G1–G11 were identified during planning and independently confirmed by the readers; G12–G30 were newly reported by readers. Findings ↔ gaps cross-referenced where a gap is a finding's missing regression test.

| # | Untested behavior | Code | Reported by |
|---|---|---|---|
| G1 | `adjust_base_fees` Err arm (fail-open at close) — zero coverage on a chain WITH the contract | `run_epoch.rs:659-666` | plan + R1/R2/R3/R4 (→F4) |
| G2 | Unknown strategy ≥2 fallback branch — zero coverage (unreachable via real contract; needs mocked/forged read) | `tn-reth/lib.rs:1601-1613` | plan + R3/R4 (→F14) |
| G3 | `adjust_base_fees` orchestration never invoked by any test — close-time resize + per-worker loop + worker-added-at-boundary gets computed fee | `run_epoch.rs:641-666` | plan + R2 |
| G4 | Governance config change mid-run e2e — incl. change landing in the closing block itself (the exact block `adjust_base_fees` reads) | `tn-reth/lib.rs:2487` covers pinned reads only | plan + R3/R5 |
| G5 | Multi-worker block production untested at every level (unit/IT/e2e) | `start_epoch.rs:372` hardcodes `DEFAULT_WORKER_ID` | plan + R3/R4/R5 — **blocked on issues 4a/4b/4c/5** |
| G6 | Boundary-kill restart e2e (the blocker's own acceptance test; non-MIN fee, kill at N→N+1) | `run_epoch.rs:604-614` | plan + all readers (→F1) — **blocked on blocker fix** |
| G7 | EIP-1559 decrease/floor not exercised e2e (unit only) | `gas_accumulator.rs:406-410` | plan + R5 |
| G8 | Inert-default multi-epoch e2e — production default `0:0:u64::MAX` never booted e2e; test genesis defaults to ACTIVE `(0, 30M)` | `telcoin-network-cli/src/genesis/mod.rs:125`, `test_utils.rs:718` | plan + R3/R5 |
| G9 | `test_catchup_accumulator_partial_execution` lacks a base-fee assertion | `node/tests/it/main.rs:446` | plan |
| G10 | Arity-mismatch bail untested (defensive; unreachable via deployed contract) | `tn-reth/lib.rs:1582-1592` | plan + R3 |
| G11 | `sync_num_workers_from_chain` no-header branch untested | `node.rs:220-228` | plan |
| G12 | Fail-open sync → catchup `expect` panic on a multi-worker chain (F5's failure mode: pre-sized 1, headers carry worker 1) | `node.rs:255-263`, `gas_accumulator.rs:286/317` | R1/R2/R4 (→F5) |
| G13 | Crash-after-close: catchup with tip = closing block (empty range `(B+1)..=B`) → asserts resulting fee state | `node.rs:166-174` | R1/R4 (→F1 shape b) |
| G14 | `seed_base_fees_from_chain` with a chain-fees worker id ≥ num_workers (silently ignored; asymmetric vs catchup's panic) | `run_epoch.rs:701-705` | R1/R2 |
| G15 | ModeChange re-entry: idempotent re-seed + no-op resize with live gas totals preserved, panic-free with in-flight leftovers | `run_epoch.rs:151-182`, `close_epoch.rs:114-161` | R1/R2 (→F18) |
| G16 | Concurrent `inc_block`/`base_fee` racing `set_num_workers` grow (existing `clones_observe_resize` is sequential) | `gas_accumulator.rs:541` | R1 |
| G17 | `#[should_panic]` test for the documented `inc_block`-after-shrink tripwire | `gas_accumulator.rs:286` | R5 |
| G18 | Underpriced tx rejected by pool under Static fee (claimed in prose only, basefee.rs:24-26) | `e2e-tests/tests/it/basefee.rs` | R5 |
| G19 | Pool repricing at boundary: queued txs below the new fee are parked/excluded from next epoch's batches (`pending_basefee` update alone is asserted) | `engine/inner.rs:184-191`, `node/tests/it/main.rs:228-267` | R4 |
| G20 | Cross-node validator agreement via divergent recovery paths (live-cross vs restart) — one validator accepts the other's batch | `batch-validator/src/validator.rs:610-633` | R4 |
| G21 | Genesis ceremony must FAIL loudly on invalid strategy (2..=255) or empty configs (constructor revert currently swallowed) | `tn-reth/lib.rs:1272-1276` | R3 (→F7) |
| G22 | `Static { fee: 0 }` and `Static { fee: u64::MAX }` end-to-end (build → validate → execute) | `run_epoch.rs:679` | R3/R5 (→F15) |
| G23 | Worker-count shrink across a boundary via governance (`setNumWorkers` downward) driven through adjust/entry-sync/catchup | `run_epoch.rs:650` | R3/R4 |
| G24 | Idle-worker fee divergence: worker with non-MIN config and zero blocks in epoch, restart mid-epoch, compare vs live value | `run_epoch.rs:689-702` | R2/R4 (→F6) |
| G25 | `blockHeight = closing+1` identity pin (F17 residual) — incl. `latest_base_fee_per_worker`'s closing-block exclusion resting on it | `node.rs:282-292`, `run_epoch.rs:623-625` | R1/R2 (→F17) |
| G26 | Rewards-counter epoch guard in catchup when tip == closing block (`last_executed_epoch == epoch` must skip `count_leaders`) | `node.rs:187-191` | R1 |
| G27 | "Fee first applies in epoch 1, not 2" not strictly pinned (`wait_for_epoch_at_least(1)` may overshoot into epoch 2) | `e2e-tests/tests/it/basefee.rs:114` | R5 |
| G28 | Mid-epoch restart under EIP-1559 (restart e2e is Static-only; recovered adjusted fee must match peers exactly) | `e2e-tests/tests/it/basefee.rs:261` | R4 |
| G29 | Dual-read inconsistency regression (D.4/F16): finalized lagging canonical across a boundary → empty range → silent no-seed — requested by the issue itself | `run_epoch.rs:159-171`, `node.rs:160-174` | R2 (→F16) |
| G30 | 30M-gas ceiling on `getAllWorkerConfigs`: exact numWorkers bound + governance-facing documentation | `tn-reth/lib.rs:1566`, `evm/mod.rs:173` | R3 (→F23) |

### 4.3 Available test infrastructure (verified at HEAD)

Inventory of reusable helpers future basefee tests can build on (feeds §5 "Builds on" column):

| Helper | File:Line | What it does |
|---|---|---|
| `config_local_testnet_with_worker_fee_configs` | `e2e-tests/src/lib.rs:97` | Genesis + committee for 4 validators with per-worker `--worker-fee-config` (`"ID:STRATEGY:VALUE"`) entries |
| `config_local_testnet_with_epoch_duration` | `e2e-tests/src/lib.rs:76` | Same testnet config without fee overrides; optional epoch duration |
| `get_telcoin_network_binary` | `e2e-tests/src/lib.rs:356` | Static handle to the built `telcoin-network` binary |
| `ProcessGuard` (`new`/`take`/`replace`/`kill_all`…) | `e2e-tests/tests/it/common.rs:88` | RAII child-process guard; SIGTERM→SIGKILL on drop; slot take/replace for kill-restart flows |
| `kill_child` / `send_term` | `e2e-tests/tests/it/common.rs:172/167` | Graceful single-node shutdown |
| `start_validator[_with_args]` / `start_observer` | `e2e-tests/tests/it/common.rs:319/331/370` | Spawn a node process with per-test log dirs (`run` disambiguates restarts) |
| `network_advancing` | `e2e-tests/tests/it/common.rs:292` | Poll all 4 nodes' RPC responsive within 45s |
| `acquire_test_permit` | `e2e-tests/tests/it/common.rs:45` | Process-wide 2-slot semaphore + tracing init |
| `send_tel` / `get_balance*` / `send_and_confirm` | `e2e-tests/tests/it/common.rs:561/525/540/487` | Signed legacy transfer at a chosen gas price; balance polling |
| `start_testnet` | `e2e-tests/tests/it/basefee.rs:396` | Boot 4 validators against a prepared genesis; returns guard + RPC URLs |
| `wait_for_rpc` / `current_epoch` / `wait_for_epoch_at_least` | `e2e-tests/tests/it/basefee.rs:414/428/436` | RPC-ready poll; `getCurrentEpochInfo` snapshot; epoch-target poll |
| `land_tx_and_read_fee` (+ mid-epoch variants) | `e2e-tests/tests/it/basefee.rs:483/459/471` | Submit transfer, confirm, return `(tip_block, base_fee)` |
| `read_base_fee` / `wait_for_block_fee` / `parse_hex_u64` | `e2e-tests/tests/it/basefee.rs:519/531/547` | Read a block's `baseFeePerGas`; poll a node for a height |
| `test_genesis_with_consensus_registry[_and_workers]` | `tn-reth/src/test_utils.rs:717/727` | Genesis with deployed ConsensusRegistry + WorkerConfigs; per-worker `(strategy, value)` |
| `seeded_genesis_from_random_batches` | `tn-reth/src/test_utils.rs:669` | Fund batch senders so random batches execute |
| `default_test_execution_node` | `test-utils/src/execution.rs:22` | In-process ExecutionNode over a temp Reth datadir |
| `sync_num_workers_from_chain` / `catchup_accumulator` | `node/src/manager/node.rs:214/155` | Pub production functions reused directly by node ITs |
| `spawn_consensus` | `node/tests/it/main.rs:744` | Spawns consensus components feeding a test engine (shared by the 4 engine-scaffold tests) |
| `compute_next_base_fee_eip1559` | `types/src/gas_accumulator.rs:372` | Exact next-fee oracle for pinning expected sequences |

---

## 5. Recommended Additional Tests

Every recommendation is traceable to a §4.2 gap (Gn) or §3 finding (Fn) and was checked against the 21 existing tests (§4.1) — no duplicates; proposed names were grepped against the tree. "Builds on" references §4.3 infrastructure. Effort: S ≤ ~1h, M ≤ ~half-day, L = multi-day. Every CONFIRMED Medium+ finding has a regression test here (F1→P1-1/P1-2, F2/F11→H-1, F5→P0-9, F6→P2-4, F7→P0-10, F13→P0-13, F15→P0-12).

### 5.1 P0 — actionable now; complete before governance sets any non-inert config

| # | Level | Proposed test | Scenario | Builds on | Effort | Source |
|---|---|---|---|---|---|---|
| P0-1 | IT (node) | `adjust_base_fees_keeps_fees_and_count_on_read_failure` | Set non-default fees/gas on a 1-worker accumulator, invoke `adjust_base_fees` (free fn, run_epoch.rs:641) against a RethEnv whose WorkerConfigs read fails (alloc-stripped genesis, same trick as main.rs:587); assert fees AND count unchanged. First-ever coverage of the close-time Err arm. | `default_test_execution_node`, alloc-stripped genesis pattern | S | G1 / F4 |
| P0-2 | IT (node/tn-reth) | `adjust_base_fees_resizes_and_prices_worker_added_at_boundary` | 1-worker genesis; governance `setNumWorkers(2)` + `setWorkerConfig(1, Static{500})` mid-epoch; invoke `adjust_base_fees` at the tip; assert accumulator grew to 2 and worker 1's fee == 500 (and an Eip1559 fresh worker computes MIN-floored). First direct invocation of the full orchestration incl. resize. | `test_genesis_with_consensus_registry_and_workers`, governance-tx pattern from `test_worker_count_read_at_epoch_start_parent` | M | G3 |
| P0-3 | IT (tn-reth) | `unknown_strategy_policy_is_explicit` | Stub contract at WORKER_CONFIGS_ADDRESS (raw runtime bytecode in genesis alloc returning a crafted ABI blob) reports strategy=7; pin the decode outcome. Today that asserts the `Eip1559 { target_gas: value }` fallback (lib.rs:1601); if F14's fail-loud recommendation is adopted, flip to asserting `Err`. Either way the policy stops being accidental. | genesis-alloc stub technique; `test_get_worker_fee_configs` scaffold | M | G2 / F14 |
| P0-4 | IT (tn-reth) | `worker_fee_configs_arity_mismatch_bails` | Same stub technique returning `count=2` but 1-element arrays; assert the decode bails with the arity error (lib.rs:1582-1592) and the caller's fail-open arm engages. | stub technique from P0-3 | S (after P0-3) | G10 |
| P0-5 | IT (node) | extend `test_catchup_accumulator_partial_execution` | Add per-worker base-fee assertions after partial-execution catchup (currently asserts gas/rewards only). | existing test, main.rs:446 | S | G9 |
| P0-6 | IT (node) | `sync_num_workers_keeps_count_when_header_missing` | Call `sync_num_workers_from_chain` with a block number beyond the tip; assert count kept + no panic (node.rs:220-228 branch). | `default_test_execution_node`, `sync_num_workers_from_chain` (pub) | S | G11 |
| P0-7 | IT (node) | harden `test_sync_num_workers_fail_open_when_contract_absent` | Start from `GasAccumulator::new(3)` instead of `new(1)` and assert the count stays 3, so the assertion discriminates `Err → keep` from `Ok(0) → clamp(1)`. | existing test, main.rs:587 | S | F12 |
| P0-8 | unit (types) | `inc_block_after_shrink_panics` (`#[should_panic]`) | Shrink 2→1, call `inc_block(1, ..)`; pin the documented count-divergence tripwire so a refactor can't silently soften it. | `gas_accumulator.rs` test module | S | G17 |
| P0-9 | IT (node) | `catchup_after_failed_sync_on_multiworker_chain_fails_diagnosably` | Accumulator pre-sized 1 (simulating failed sync), chain headers carrying worker 1: pin F5's failure mode. Today that documents the `expect("valid worker id")` panic (catch_unwind); if the retry-or-fail-fast fix lands, flip to asserting a clean, attributable error. | poisoning pattern from `test_catchup_accumulator`; `spawn_consensus` scaffold | M | G12 / F5 |
| P0-10 | IT (tn-reth) | `genesis_ceremony_rejects_invalid_worker_config` | Build genesis with strategy=2 (contract-illegal, CLI-accepted) and with empty configs; assert genesis creation ERRORS (acceptance test for F7's fix — today it silently produces `numWorkers()=0`, `owner()=0`). | `test_genesis_with_consensus_registry_and_workers` | M | G21 / F7 |
| P0-11 | unit (node) | `seed_base_fees_ignores_out_of_range_chain_fees` | `chain_fees` containing worker id ≥ `num_workers`; assert no panic and in-range workers still seeded (pins the loop-bound semantics, run_epoch.rs:701-705). | `seed_base_fees_*` test scaffold | S | G14 |
| P0-12 | unit (node) | `static_config_returns_value_unclamped` (+ policy decision) | Pin `next_base_fee_for_config(Static{0})` == 0 and `Static{u64::MAX}` == u64::MAX (run_epoch.rs:679) so the no-clamp policy is explicit; if a MIN-floor/sanity band is adopted per F15, flip the assertion. | `static_config_pins_to_configured_fee` scaffold | S | G22 / F15 |
| P0-13 | infra (CI) | nightly basefee e2e job | Add a scheduled/nightly CI job running `cargo nextest run -p e2e-tests -E 'test(basefee)' --run-ignored all` (~95s + build) so the three e2e tests gate something hosted; fix the copy-pasted ignore strings while there. | `.github/workflows/`, Makefile `test-e2e` | S | F13 |

### 5.2 P1 — recovery + e2e breadth

| # | Level | Proposed test | Scenario | Builds on | Effort | Source |
|---|---|---|---|---|---|---|
| P1-1 | e2e | `test_boundary_restart_recovers_next_epoch_fee` | Kill a validator in the boundary window (after N's closing block, before N+1 blocks), restart, assert its next-epoch fee matches peers and its batches certify. **Blocked on** `issues/blocker-restart-boundary-recovery.md` — this is the fix's acceptance test (also covers F1 shapes a/b at e2e level; the fix's test matrix must also cover shape (c), leftover-drain). | `ProcessGuard.take/replace`, `wait_for_epoch_at_least`, `start_testnet` | L | G6 / F1 |
| P1-2 | IT (node) | `catchup_with_tip_at_closing_block_restores_nothing_today` | Drive `catchup_accumulator` with tip == closing block (empty range `(B+1)..=B`); pin today's silent no-op, flip to asserting recovery when the blocker fix lands. Actionable NOW as the unit-level pin of F1 shape (b). | `test_catchup_accumulator` scaffold | S | G13 / F1 |
| P1-3 | e2e | `test_eip1559_fee_decays_toward_min_when_idle` | `Eip1559 { target_gas: 1 }`, land txs for 2 epochs (fee rises), then stop sending; assert the fee decreases at subsequent boundaries and floors at MIN. First e2e of the decrease path. | eip1559 e2e scaffold, `compute_next_base_fee_eip1559` oracle | M | G7 |
| P1-4 | e2e | `test_default_config_keeps_min_fee_across_epochs` | Boot the testnet with the PRODUCTION default (`0:0:u64::MAX`, no fee override); land txs across ≥2 boundaries; assert fee == MIN everywhere. Closes the test-vs-prod default blind spot (test genesis defaults to an ACTIVE config). | `config_local_testnet_with_worker_fee_configs` with explicit inert value | S | G8 |
| P1-5 | e2e | single-worker governance change mid-run | Owner tx flips worker 0's config `Static{7}` → `Static{9}` mid-epoch E; assert epoch E+1's first tx-bearing block carries 9 (and E's blocks still 7). Single-worker slice of G4, actionable now. | `start_testnet`, owner-key governance tx | M | G4 |
| P1-6 | e2e | extend restart e2e with an Eip1559 variant | Same kill/restart flow as the Static test but under `Eip1559 { target_gas: 1 }` with a fee history — the recovered value must equal the committee's exactly (catches off-by-one-adjustment recovery bugs Static can't). | `test_mid_epoch_restart_recovers_static_fee` scaffold | M | G28 |
| P1-7 | IT (node) | `mode_change_reentry_is_idempotent` | Re-enter `run_epoch` mid-epoch (ModeChange): assert re-seed is a no-op, resize is a no-op, live gas totals preserved, no panic with in-flight leftover outputs. | `spawn_consensus` scaffold | M | G15 / F18 |
| P1-8 | unit (types) | `concurrent_inc_block_during_grow_is_safe` | Threads hammering `inc_block(0)`/`base_fee(0)` while a `set_num_workers` grow loops; assert no torn reads/panics and totals conserved. (Today's `clones_observe_resize` is sequential.) | `gas_accumulator.rs` test module | M | G16 |
| P1-9 | e2e/IT | `test_underpriced_tx_excluded_under_static_fee` | Under `Static{high}`, submit a tx priced below the fee; assert it never lands while a correctly-priced one does (turns the prose claim at basefee.rs:24-26 into an assertion). | `send_tel` with explicit gas price | M | G18 |
| P1-10 | IT (node) | `queued_txs_below_new_fee_are_parked_at_boundary` | Queue txs at fee X, `set_worker_base_fee(0, Y>X)`, assert the pool parks them and the next batch excludes them (beyond the existing `pending_basefee` assertion). | `test_worker_pool_base_fee_sourced_from_accumulator` scaffold (main.rs:230) | M | G19 |
| P1-11 | IT (batch-validator) | `validators_from_divergent_recovery_paths_agree` | Build one validator via the live path and one via restart-recovery state for the same (epoch, worker); each validates the other's batch. Direct pin of the cross-node agreement invariant everything else protects indirectly. | `test_tools` validator harness (validator.rs:610) | M | G20 |
| P1-12 | IT (node) | `rewards_counter_not_double_counted_when_tip_is_closing_block` | Catchup with `last_executed_epoch == epoch_state.epoch`; assert `count_leaders` skipped (guard at node.rs:187-191). | `test_catchup_accumulator` scaffold | S | G26 |
| P1-13 | IT (tn-reth/node) | `epoch_block_height_is_closing_block_plus_one` | After a real epoch close, assert `canonical_tip().number + 1 == epoch_info.blockHeight` — pins the same-block identity both config reads rely on (F17 residual; also protects `latest_base_fee_per_worker`'s closing-block exclusion). | epoch-close IT scaffold | M | G25 / F17 |
| P1-14 | IT (node) | dual-read inconsistency regression | Force finalized ≠ canonical-tip epoch state across a boundary; assert seeding/catchup either succeed or fail loudly (per the fix chosen in `issues/dual-header-read-robustness.md` — pairs with that fix, requested by the issue itself). | `spawn_consensus` scaffold | M | G29 / F16 |

### 5.3 P2 — blocked on multi-worker runtime (issues 4a/4b/4c/5)

| # | Level | Proposed test | Scenario | Builds on | Effort | Source |
|---|---|---|---|---|---|---|
| P2-1 | e2e | `test_two_workers_carry_independent_fees` | 2-worker testnet, worker 0 `Eip1559{target:1}` + worker 1 `Static{9}`; assert each worker's blocks carry its own fee trajectory. **Blocked**: runtime spawns only `DEFAULT_WORKER_ID` (start_epoch.rs:372). | `config_local_testnet_with_worker_fee_configs` (already takes per-worker entries) | L | G5 |
| P2-2 | e2e | multi-worker governance mid-run | G4's full form: change worker 1's config mid-run, assert only worker 1's next-epoch fee moves. | P1-5 + multi-worker runtime | M | G4 / G5 |
| P2-3 | e2e/IT | `test_shrink_num_workers_across_boundary` | Governance `setNumWorkers` downward; assert close-time resize, entry sync, catchup, and batch validation all agree on the shrunk set (accounting-only IT slice is P0-2's shrink variant). | P0-2 scaffold + multi-worker runtime | M | G23 |
| P2-4 | e2e | idle-worker fee recovery | Worker 1 `Static{500}` produces no blocks in epoch E; restart a node mid-E; compare its worker-1 fee vs a live node (pins F6 — currently diverges; acceptance test for the per-worker frontier fix). | P2-1 + `ProcessGuard` restart flow | L | G24 / F6 |

### 5.4 Hardening of existing tests (fixes, from test-quality findings)

| # | Test | Fix | Source |
|---|---|---|---|
| H-1 | `test_mid_epoch_restart_recovers_static_fee` | Make it falsifiable: route the post-restart tx through the restarted node's RPC (`client_urls[kill_idx]`) so its pool/batch path is exercised, and convert the optional `if let Some` check at :368 into `ok_or_else` (same as :344). | F2, F11 |
| H-2 | `test_eip1559_fee_rises_at_epoch_boundaries` | Remove the self-defeating skip branch (:208) or record skipped epochs and assert against the exact expected series computed via `compute_next_base_fee_eip1559` (also upgrades inequality to exact-step assertions). | F3, F9 |
| H-3 | `test_mid_epoch_restart_recovers_static_fee` | Replace `sleep(EPOCH_DURATION/2)` + hard assert (:308-314) with phase computed from on-chain epoch start (or retry-until-mid-epoch), so slow CI can't fail the test before the scenario runs. | F10 |
| H-4 | `land_tx_and_read_fee` | Plumb the tx hash out of `send_tel` and read the receipt's block number instead of inferring "current tip" (:511). | F20 |
| H-5 | basefee.rs / epochs.rs / node it main.rs | Deduplicate: move `current_epoch`/`wait_for_epoch_at_least`/`EPOCH_DURATION` + testnet boot into common.rs; extract the 4× ExecutorEngine scaffold in node it into a builder. | F21 |
| H-6 | `test_static_fee_applied_at_epoch_boundary` | Record the epoch observed at first non-MIN fee and assert it is exactly 1 (guards the activation off-by-one under CI overshoot). | G27 |

---

## 6. Test Execution Results

All suites run at HEAD `67f7bf2f` on 2026-07-04 (post-rebase validation).

### 6.1 Scoped unit + integration tests

```
cargo nextest run -p tn-types -p tn-node -p tn-reth -p tn-batch-validator -p tn-batch-builder
Summary [ 105.940s] 224 tests run: 224 passed (12 slow), 0 skipped   → exit 0
```

All 224 tests pass, including the stack's 21 new/extended tests (resize semantics, boundary-fee units, seed/catchup ITs, fee-config ITs, adapted validator/builder tests).

### 6.2 Basefee e2e (`#[ignore]`d, run explicitly)

```
cargo nextest run -p e2e-tests -E 'test(basefee)' --run-ignored all
Summary [  94.814s] 3 tests run: 3 passed, 13 skipped   → exit 0
```

| Test | Result | Time |
|---|---|---|
| `basefee::test_static_fee_applied_at_epoch_boundary` | PASS | 29.2s |
| `basefee::test_mid_epoch_restart_recovers_static_fee` | PASS | 65.6s |
| `basefee::test_eip1559_fee_rises_at_epoch_boundaries` | PASS | 78.1s |

The three e2e tests pass at the rebased HEAD, confirming the 08-e2e branch is valid on top of the reworked 07 (num-workers-from-chain) branch.

---

## 7. Production-Readiness Verdict

### READY WITH BLOCKERS

**Merging this stack is safe. Activating it is not — yet.** The feature ships inert by default: the production genesis config (`0:0:u64::MAX` → `Eip1559 { target_gas: u64::MAX }`) keeps every fee pinned at `MIN_PROTOCOL_BASE_FEE`, all 224 scoped tests and all 3 e2e tests pass at the rebased HEAD (§6), verification found **zero false positives but also zero Critical findings**, and the core design was positively verified (§3 Verified-safe): the u64 snapshot removes a real pre-existing race, lock discipline is sound, split-brain fee containers are structurally eliminated, and fee divergence is bounded to a liveness failure — never a state fork.

The verdict is gated on the single CONFIRMED **High**: **F1** — every `close_epoch(None)` path skips both the fee computation and the compensating chain-seeding, so a node caught in the boundary window runs the entire next epoch on stale/MIN fees and is exact-match-rejected by peers. Two shapes are the tracked blocker (`issues/blocker-restart-boundary-recovery.md`); this review confirmed a **third, untracked shape (leftover-drain)** that needs no restart at all — a live node whose epoch task exits racing the boundary. One governance transaction arms the whole class.

### Blockers before governance sets ANY non-inert WorkerConfig

1. **Fix F1** (the tracked blocker) with a scope that covers all three shapes — replay-and-close, crash-after-close, AND leftover-drain — and update `issues/blocker-restart-boundary-recovery.md` to add shape (c). Acceptance: P1-1 boundary-restart e2e + P1-2 IT pin.
2. **Land the P0 test set (§5.1)** — 13 items, mostly S-effort. Highest-value: P0-1 (close-time fail-open arm has never executed in a test), P0-10 (genesis-ceremony acceptance), P0-7 (make the existing fail-open test discriminating), P0-13 (the 3 e2e tests currently gate nothing hosted — F13).
3. **Fix F7 (Medium)**: check `ExecutionResult::is_success()` in the genesis writer before committing, and validate strategy range in the CLI — today a ceremony typo silently produces an unownable, unconfigurable WorkerConfigs contract. The verifier notes the same unchecked pattern governs the ConsensusRegistry deploy; audit both together.
4. **Decide F15 (Medium) policy**: clamp `Static` to a sanity band (at minimum the MIN floor) or explicitly accept raw values in a governance runbook — `Static { fee: u64::MAX }` currently self-bricks inclusion for an epoch with prohibitive recovery cost.
5. **Correct the F8 docs** (Low, but cheap and load-bearing): the `close_epoch`/`gas_accumulator` comments currently assert the exact invariant the blocker refutes; anyone wiring activation from the code alone would conclude the hole is closed.

### Before multi-worker spawning ships (issues 4a/4b/4c/5)

The latent Mediums — F5 (fail-open count sync → deferred panic crash-loop), F6 (idle-worker fee unrecoverable from chain) — plus F14 (unknown-strategy fallback) and F19 (silent `set_worker_base_fee` no-op) are all inert single-worker but become live divergence/crash surfaces with a second worker. Fold them into the multi-worker follow-ups' acceptance criteria (tests P0-9, P2-4, P0-3, and a `warn!` on F19's None arm).

### Hygiene

Update the stale `issues/` registry (§1c Registry notes): README branch map (07/08), commit hashes two rewrites old, `6-activate-workerconfigs.md` describing the removed fail-stop behavior, and the blocker file's reference to a `KNOWN LIMITATION` comment that no longer exists.

**Bottom line**: merge-ready as-is; activation-ready after blockers 1–5; multi-worker-ready only after the latent set is closed. The realistic worst case if activated today: a boundary-timed restart or task-exit puts one or more validators on wrong fees for a full epoch (batch-rejection liveness degradation, compounding under Eip1559) — exactly the class the blocker tracks, now with a wider trigger surface than the issue describes.
