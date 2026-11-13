# Verification Report: storage-v2 review, round 2 (T2, T5, T6, T7)
Date: 2026-11-13
Scope: `sls_storage_v2_3` at 4acc752fb against origin/main (merge base a3df64830). Findings from T2.md, T5.md, T6.md, T7.md. Saved by the coordinator from the verifier's hand-back (the verifier cannot write files).
Method: one verifier for each Medium (T6-6 as Tier 1). Lows batched by subsystem: T2's four together, T5's two with T7's three, T6's three together. Verifiers got only the claim, the key question and the file list. Informationals not verified.

## Verdict table (by verified severity)
| ID | Initial | Verdict | Verified | One line |
|---|---|---|---|---|
| T6-6 | Medium (High if debug/trace exposed) | CONFIRMED (HIGH) | **Medium** on a node that exposes debug/trace (arguably High); Informational on a default node | Call and create calldata copies are never charged against the 160 MiB cap. One `callTracer` call at 50M gas measured 6.96 GB peak RSS with 0 bytes counted. |
| T7-1 | Low | CONFIRMED (HIGH) | **Medium** (raised) | A new jar's data and offsets files are created before its `.conf`. Any process kill in that window, not only a power loss, leaves a jar with no `.conf`, and the node then refuses to start. |
| T6-1 | Medium | PARTIALLY_VALID (HIGH) | **Medium** (only when HTTP or WS is exposed; both off by default) | Installed filters have no limit on addresses or topics: about 11 MB each, about 11 GB at the 1000-filter cap. Filling the cap is TN-0056's documented design. |
| T2-2 | Low | PARTIALLY_VALID (HIGH) | Low | The arm64 job tests only tn-lmdb's CRC copy; the copies are identical today, but nothing keeps them in sync. |
| T2-3 | Low | CONFIRMED (HIGH) | Low | `backend-gate.sh` resolves default features only; the `adiri`, `faucet`, `js-tracer` graphs are never checked (none enables mdbx today). |
| T2-5 | Low | CONFIRMED (HIGH) | Low | A caught decode panic still prints a panic message and backtrace on every read; nothing names the table or key. |
| T5-1 | Low | CONFIRMED (HIGH) | Low | A failed parallel root task (including `TipMoved`) increments no metric; a per-block log line is the only signal. |
| T5-4 | Low | CONFIRMED (MEDIUM-HIGH) | Low | No TN test would fail if TN-0140 were reverted; every TN oracle goes through the same `TrieNodeIter`. |
| T7-3 | Low | CONFIRMED (HIGH) | Low | The lock-order inversion between `gap_at` and `update_index` is real but unreachable on TN at HEAD. |
| T6-2 | Low | CONFIRMED (HIGH) | Low | `logs` subscriptions keep an unbounded client `Filter` outside the filter cap; needs WS or local IPC. |
| T6-3 | Low | CONFIRMED (HIGH) | Low | `eth_simulateV1` has no per-request gas budget: up to 256 × 50M gas per request. Upstream fixed it in b8116b42b (#24387). |
| T2-1 | Low | DESIGN_DECISION (HIGH) | Informational | Leaving `test-mdbx` out of CI is documented (`etc/ci-lanes.sh:269-271`); the migration reader is tested in the default lane. Doc drift remains (T2-4). |
| T6-7 | Low | CONFIRMED (HIGH) | Informational | The JS tracer timeout has no upper bound, but `js-tracer` and `debug` are off by default, disconnect interrupts, same upstream. |
| T7-2 | Low | CONFIRMED (HIGH) | Informational | TN-0083's resume fails under v2, but `layout::settle` heals or refuses the state first; the ledger claim is wrong for v2. |

## Verification stats
28 submitted: 2 Medium, 12 Low, 14 Informational. 14 verified (11 CONFIRMED, 2 PARTIALLY_VALID, 1 DESIGN_DECISION, 0 FALSE_POSITIVE). After verification: 3 Medium, 7 Low, 4 moved to Informational; one raised (T7-1), three lowered. 5 verification agents of 12. No re-verification needed (T6-6 HIGH confidence). No repo file edited; scratch work in the session scratchpad (`t6-1/filtermem/`, `t6-6/revm-inspectors/`). Tests run: `reth-rpc eth::filter::tests::active_filters_are_bounded` passed; `tn-lmdb-reth a_damaged_value_is_a_decode_error_not_a_panic --no-capture` passed printing 6 hook messages; the T6-6 PoC 3/3 in a scratch copy. One side effect: a verifier ran `git fetch` on `~/.cache/tn-vendor/reth` (its `origin/main` moved to 10c6cf5e7), so citations name two upstream heads.

## Findings

### T6-6. The call-trace arena's calldata and output copies are not counted by TN-0079's cap
- **Verified**: Medium where `debug`/`trace` are served to untrusted clients (arguably High: one request leads to an OOM kill); Informational on a default node, where only the ledger statement is wrong.
- **Location**: `patches/revm-inspectors/src/tracing/mod.rs:706-745, 732, 836-846, 392-432, 441-464, 751-764, 177-199, 558`; `vendor/reth/CHANGES.md:1375-1376`.
- **Evidence**: `Inspector::call` always calls `inputs.input_data(context)` (`:732`), which `Bytes::copy_from_slice`s the shared memory range (`:842`), and `start_trace_on_call` stores the copy in the arena (`:422`); `fill_trace_on_call_end` stores `output.clone()` (`:456`); `create` keeps `init_code` (`:751-764`). None is charged: `record_bytes` (`:177-199`) has one caller, `start_step` (`:558`), which runs only with `record_steps` (`:679-683`). Affected: `callTracer`/`prestateTracer` (`none()` preset, `config.rs:223-227, 262-264`), `flatCallTracer` and every parity `trace_*` (`default_parity`, `config.rs:129-142, 190-198, 250-254`), and the struct logger's call copies. No other guard runs before the allocation (`--rpc.max-response-size` at serialization only; permits limit concurrency; the JS interrupt is JS-only; `rpc_debug.rs` adds the base fee only; RPC lifts the EIP-7825 cap, `helpers/call.rs:923`). Measured: the PoC 2,080 calls / 34,062,336 bytes / `recorded_bytes=0`; the verifier's `callTracer` with 16 KiB calldata at ~122 gas per call: 10M gas 1.28 GiB (1.40 GB RSS), 50M gas 411,313 calls, 6.43 GiB held, **6.96 GB peak RSS in 0.69 s**, `recorded_bytes=0`; struct logger at 1M gas 125 MiB against 31.5 MB counted. Exposure: `debug`/`trace` off unless named (`rpc_server_args.rs:99-106, 128-135`), IPC never serves them (`env/rpc.rs:680-686`), docs advise a separate node behind auth.
- **Proposed fix** (`patches/revm-inspectors/src/tracing/mod.rs`): split `record_bytes` into a context-only `charge(&self, ctx, bytes) -> bool` (fetch_add, compare, set `ContextError::Custom("trace too large…")`; keep the interpreter halt in the step wrapper). In `call` charge `size_of::<CallTraceNode>() + input.len()`; on failure still push the trace with `Bytes::new()` (keeps `trace_stack` balanced) and return `Some(CallOutcome::new(InterpreterResult::new(InstructionResult::FatalExternalError, Bytes::new(), Gas::new(inputs.gas_limit)), inputs.return_memory_offset.clone()))`; revm turns the context error into `Err(Custom)` → -32603, TN-0079's path. Same in `create` (charge `init_code().len()`, return a `CreateOutcome`) and in `fill_trace_on_call_end` (charge `output.len()`). Tests: a `STATICCALL` loop with 16 KiB calldata under `from_geth_call_config` and `default_parity` with a 64 KiB cap, both "trace too large". Docs: TN-0079's *Why*, `PATCH.md`, `config.rs:85-90`, `mod.rs:101-104`; regenerate `upstream.diff`. Cost: one relaxed atomic per call frame on the tracing path only. Upstream: no cap in revm-inspectors 0.34.3/0.34.4.

### T7-1. A new jar's data file can exist without its `.conf`, and the next start refuses the datadir
- **Verified**: Medium, raised from Low: a plain process kill (SIGKILL, OOM) in the window suffices; the node refuses to start until an operator deletes files; no committed data lost; the window recurs at every jar rotation and at genesis.
- **Location**: `vendor/reth/crates/storage/nippy-jar/src/writer.rs:66-71`; `storage/db/src/static_file/mod.rs:38`; `fs-util/src/lib.rs:317-358`; `crates/tn-reth/src/env/mod.rs:441-447`.
- **Evidence**: `create_or_open_files` creates the data file and the offsets file (9-byte header) before `jar.freeze_config()`. On restart `iter_static_files` lists every extension-less file and calls `NippyJar::load(...)?`, which fails without the `.conf`. TN opens the provider before `layout::settle` (`mod.rs:453`) and before `check_consistency` (`:478`), so neither heals it. TN-0131 heals the opposite state (a `.conf` with empty or torn files), TN-0132 covers deletion leftovers; neither covers a data file with no `.conf`. Upstream adf5f076a has the same order.
- **Proposed fix** (rotation path only): in `NippyJarWriter::new`, when `jar.rows == 0` and either file is missing, `jar.freeze_config()?` before `create_or_open_files`; a crash after that leaves a lone rows-0 `.conf` (no listing sees it; the next creation overwrites) or TN-0131's healed state. Crash test next to `rotation_after_an_empty_offsets_file`: after a rotation delete the new jar's `.conf` and reopen. Alternative: `iter_static_files` skips a zero-length data file with no `.conf` whose offsets file is ≤ 9 bytes.

### T6-1. `--rpc.max-active-filters` bounds the number of installed filters, not their size
- **Verified**: Medium, conditional on HTTP or WS being exposed (both off and bound to 127.0.0.1 by default: `rpc_server_args.rs:26-27, 84-89, 117`; once enabled `eth` is served without JWT). Corrections: ~11 MB per filter, not 15; one client filling the cap is TN-0056's documented node-wide design (`CHANGES.md:1238`).
- **Location**: `vendor/reth/crates/rpc/rpc/src/eth/filter.rs:350-355, 614-639` (`:627-629` counts only), `:939-943`; alloy-rpc-types-eth 1.8.3 `filter.rs:20-28, 403-420, 1072-1075` (only four topic positions are limited).
- **Evidence**: no check on address/topic counts, no total-size budget, no per-IP quota; a poll refreshes the lifetime (`:260`) and filters outlive their connection (`:626`). Measured with a counting allocator: a 15 MiB body holds 365,780 addresses keeping 11,010,668 bytes; ~11 GB at the cap. Transient: each `eth_getFilterChanges` clones the filter and its bloom costs ~94 MB per poll; `eth_getLogs` pays the same without installing. `active_filters_are_bounded` passed (count only). Upstream reth main has neither cap.
- **Proposed fix**: one check, `MAX_FILTER_CRITERIA = 1_000` (addresses plus all topic values) in `new_filter` (`:350`), `logs` (`:435`) and `EngineEthFilter::logs` (`:62`); a `TooManyFilterCriteria` variant → `INVALID_PARAMS_CODE`; test 1,001 refused / 1,000 accepted; document at `rpc_server_args.rs:179-181` and the filter README; optional flag `--rpc.max-filter-criteria`. With it the cap holds ~40 MB. The same helper fixes T6-2.

### T2-2. `arm64-crc.yaml` runs on changes to the storage CRC copy but tests only tn-lmdb's
- **Verified**: Low (PARTIALLY_VALID). Path filter `.github/workflows/arm64-crc.yaml:17,27`, test step `:79`. The two files' executable code and test bodies are identical (differences: doc text, two `cfg_attr(dead_code)`, one `#[inline]`: `crates/tn-lmdb/src/crc.rs:157-159, 178-179, 232-233`, documented at `crc.rs:5-11`, `CLAUDE.md:56`), so the arm64 run does test the same kernel today; missing is a sync check. **Fix**: run both crates' CRC tests (`-p tn-lmdb -p tn-storage`, slower: tn-storage builds reth-libmdbx) or a step that strips comments/attributes and diffs the copies.

### T2-3. The backend gate resolves default features only
- **Verified**: Low. `etc/backend-gate.sh:62-71, 73` passes no `--features`; `etc/test_backend_gate.py:83-118` plants nothing behind a non-default feature; images build with `CARGO_FEATURES=adiri`/`faucet` (`Makefile:549, 556`; `etc/release.sh:285`). `cargo tree` with `faucet`, `js-tracer`, `adiri` enables no `tn-reth/mdbx` today. Partial cover: `release.sh:1318-1357` checks the CLI crate's own features only; a fresh datadir on an mdbx build would quietly start on MDBX. **Fix**: loop the gate over `("" adiri faucet js-tracer)` for `telcoin-network` (or every non-`mdbx` feature from `cargo metadata`, as `workspace_features_but_mdbx`, `ci-lanes.sh:139-149`); planted test `adiri = ["tn-reth/mdbx"]` must fail.

### T2-5. A caught decode panic still runs the panic hook
- **Verified**: Low (strictly better than main's MDBX, which let the panic propagate). `crates/tn-lmdb-reth/src/tx.rs:64-66`; sites `tx.rs:70-71`, `cursor.rs:793-811`; `DatabaseError::Decode` is a unit variant (`errors/src/db.rs:43`) so no table or key travels; no `tracing` call in either file; no hook installed; `lib.rs:192-196` silent on the hook output. The test with `--no-capture` printed 6 panic blocks of ~55 frames naming `account.rs:173`, not the table or key. **Fix**: `guarded` takes `T: Table` and the raw key; on `Err` emit `tracing::error!(table = T::NAME, key = %hex::encode(key), "damaged row: decode panicked")`; document the hook output at `lib.rs:195`; do not swap the global hook.

### T5-1. Failed parallel root tasks are not counted
- **Verified**: Low. `record_failure` (`crates/tn-reth/src/env/output_overlay/parallel.rs:671-675`) bumps a count and keeps the first error; `log_failures` (`:689-699`) logs once a block (`:415`); the only metrics in the file are the fallback and mismatch counters (`:359, 400, 461, 465, 761-766`); `metrics.rs:452-468, 516-532` reasons include no task failure; the module docs treat `TipMoved` as a broken invariant (`:74-79`). **Fix**: `tn_reth.parallel_state_root_task_failures_total{reason=provider|database|tip_moved|panicked}`, registered at zero (as `register_account_walk_fallback_series`, `metrics.rs:489`), incremented in `record_failure`; document `tip_moved` as alertable.

### T5-4. No TN test pins TN-0140 against a from-scratch root
- **Verified**: Low (MEDIUM-HIGH confidence; proving it needs a revert). The fix is `vendor/reth/crates/trie/trie/src/node_iter.rs:281-283`, pinned only by the upstream test (`:387-453`); every TN oracle goes through the same `TrieNodeIter` (`tests/it/trie_overlay.rs:131-182, 187-224`, `output_overlay/tests.rs:151-156`, the parallel and split tests); the fixtures lack the shape (`CHANGES.md:1075-1079`); the only production guard is snapshot import (`snapshot.rs:53-58, 970-971`). **Fix**: a test with upstream's seven-slot storage shape, persisted, plus the one-slot insert under the extension; assert `layered_root_with_updates`, `parallel_layered_root_for_test` and `StateRoot::new(NoopTrieCursorFactory, HashedPostStateCursorFactory(Noop, flattened))` agree.

### T7-3. Lock-order inversion between `gap_at` and `update_index` (latent)
- **Verified**: Low. `StaticFileJarProvider` holds a dashmap `Ref` (shard read guard, `jar.rs:30-32`, `mod.rs:43-44`); `gap_at` (`manager.rs:1678-1706`) takes `indexes.read()` through `get_lowest_range` (`:2363-2373`) while callers (`:2531, 2829, 3011`) hold the guard; `update_index` holds `indexes.write()` (`:1434`) then touches the map (`:1556-1572`); parking_lot is task-fair, so a two-thread deadlock exists. Unreachable on TN: the index read happens only when a jar is missing or the number is below the lowest jar; `check_contiguity` refuses gaps at every start; TN deletes segments only at startup (`layout.rs:226`); the lowest jar starts at 0. **Fix**: `gap_at` takes an owned `SegmentHeader`; callers check `jar_blocks.contains` first and only on a miss clone the header, drop the provider and call `gap_at`.

### T6-2. `logs` subscriptions are outside the filter cap
- **Verified**: Low. `pubsub.rs:109-118, 233-246, 409-425` keep the client's `Filter` unchecked; limits are 15 MB per request, 1,024 subscriptions per connection, 500 connections (`rpc_server_args.rs:65, 68, 76`) per worker server; WS off by default (the public-observer example binds WS to 0.0.0.0); closing the connection frees everything (`:286-288`); upstream the same. **Fix**: apply T6-1's check in `subscribe` before `pending.accept()` for `SubscriptionKind::Logs`; optional node-wide semaphore.

### T6-3. `eth_simulateV1` has no per-request gas budget
- **Verified**: Low. `rpc-eth-api/src/helpers/call.rs:75-77` allows 256 blocks; `:165-170` lets a client raise a block's gas limit to max(30M, 50M cap); TN-0057's `CallManyBudget` is applied only in `call_many` (`:316-328`); the TN wrapper (`crates/tn-reth/src/rpc_call.rs:320-345`) adds a permit, no budget: ~1.28e10 gas per request. Mitigations: cancellation between calls (`simulate.rs:235-238`, TN-0049) and permits. **Upstream fix: b8116b42b** (#24387, v2.3.0) makes `--rpc.gascap` a per-request budget; not in `upstream-backports.md`. **Fix**: backport it, or `CallManyBudget::new(this.call_gas_limit())` charged `parent.gas_used()` after each block; test two 50M blocks against a 50M cap error.

### T2-1. The mdbx-feature suites run in no automated lane
- **Verdict**: DESIGN_DECISION, Informational. Stated at `etc/ci-lanes.sh:269-271`; the migration reader runs in the default lane against real MDBX (`migrate/tests.rs:328, 843`); `durable-e2e.yaml:100-104` and the attest (`test-and-attest.sh:177-178`) run the migration and resync e2e; code compiled only under `mdbx` is clippy-linted but executed nowhere; no release builds `mdbx`. Doc drift (`ACTIONS.md:194`, `test-and-attest.sh:116`) is T2-4. Optional: `test-mdbx` in `durable-e2e.yaml`.

### T6-7. The JS tracer timeout is unbounded
- **Verdict**: CONFIRMED, Informational. `debug.rs:1293-1303` parses the client's value unclamped ("100y" parses; tokio treats it as far future); disconnect interrupts at each hook (`:1343-1351`, `js_tracer_interrupt.rs:95-120`); one boa callback can already loop without limit, the real bound is `--rpc.max-tracing-requests`; `js-tracer` and `debug` off by default; upstream the same. Optional: `.min(60 s)` at `:1297` and a sentence in `js-tracer.md`.

### T7-2. TN-0083's resume fails under v2, but TN never reaches it
- **Verdict**: CONFIRMED, Informational. `provider.rs:3922-3946` commits static files before the database; on resume `insert_genesis_state` rewrites block 0's changesets (`init.rs:394-398` → `either_writer.rs:137-141, 158-162`) and `increment_block(0)` returns `UnexpectedStaticFileBlockNumber(seg, 0, 1)` (`static_file/writer.rs:943-953`); the only resume test is v1 (`init.rs:738-753`). On TN `layout::settle` runs first (`env/mod.rs:453`), removing the unfinished genesis (`layout.rs:174-177, 219-230`) or refusing (tests `layout.rs:737, 763`). **Fix**: restrict TN-0083's claim to v1 and note `settle`, or delete the genesis segments (Headers last) in the resume branch with a v2 test.

## Informational (not verified, listed)
- **T2-4**: ACTIONS.md, `test-and-attest.sh:116`, `ci-lanes.sh`, `main-compiles.yaml` say "four test lanes"; three run (`ACTIONS.md:194` confirmed).
- **T2-6**: `EXEC_SHARED_BASE=1` in commit mode never fails when the sides' chains differ.
- **T2-7**: the exec bench's "about 9% of the state root" cites no run.
- **T2-8**: `create_tables_for` rewrites the layout record on every call, including over a pre-record file.
- **T5-2**: the README mismatch runbook does not say that turning the parallel paths off does not repair trie nodes already persisted.
- **T5-3**: `open-issues.md` says a replay "names the first block"; the node stops at the epoch close.
- **T6-4**: `rpc_blocking.rs` says servers are rebuilt at epoch boundaries; each is built once.
- **T6-5**: TN-0056 says the node's own pool listeners never count against the cap; they take slots (never refused).
- **T6-8**: `PATCH.md:57-60`, `js-tracer.md:124-125` say the patch matters only to JS tracers; false since TN-0079.
- **T6-9**: docs and TN-0056 say -32604; the code sends -32009 (`filter.rs:1028-1032`, jsonrpsee-types 0.26.0 `error.rs:143`).
- **T6-10**: `eth_syncing.md` says always `false`; the node returns a progress object while syncing.
- **T6-11**: four method pages describe limits or paths the code does not have.
- **T7-4**: the two changeset count functions load jars outside `load_jar`; `db stats` only.
- **T7-5**: `reverse_offset` underflows on a v1 offsets file of 2..8 bytes (debug panic, misleading release error).

## Related patterns
- T6-1 and T6-2 share a root cause (no per-filter criteria limit); one helper fixes both and caps `eth_getLogs`'s transient bloom cost.
- T6-6 and T6-8 both come from TN-0079 scoping its cap to steps; fix code and docs together.
- T7-1's ordering fix fits the TN-0131/TN-0132 crash-safety work; reuse their harness.

## Action items (by severity)
1. T6-6 (Medium): charge call input, output and init code against `max_recorded_bytes`; correct TN-0079, PATCH.md, doc comments; regenerate `upstream.diff`.
2. T7-1 (Medium): write the `.conf` before creating a new jar's files; crash test.
3. T6-1 (Medium): per-filter criteria cap (1,000) in `new_filter`, `logs`, `EngineEthFilter::logs`; reuse for T6-2.
4. T6-3 (Low): backport b8116b42b or a per-block `CallManyBudget`.
5. T6-2 (Low): the criteria check on `logs` subscriptions.
6. T5-1 (Low): task-failure counter by reason.
7. T5-4 (Low): the TN-0140 shape test against a from-scratch root.
8. T2-5 (Low): log table and key at the `guarded` catch; document the hook output.
9. T2-3 (Low): gate the release feature sets; planted test.
10. T2-2 (Low): test the tn-storage CRC copy on arm64, or diff the copies.
11. T7-3 (Low): release the jar `Ref` before `gap_at` reads the index.
12. Informational: T2-1, T6-7, T7-2 and the doc items above.
