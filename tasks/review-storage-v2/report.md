# Review of `sls_storage_v2_3` against `origin/main`: verified report

Date: 2026-11-13. Workspace: `tasks/review-storage-v2/` (ground rules, nine track briefs and
raw-finding files `T1.md`..`T9.md`, the three verification reports `verified-round{1,2,3}.md`,
the baseline). Nothing in the tree was changed by this review.

## Overview

- **Branch:** `sls_storage_v2_3` at `4acc752fb`. **Base:** `origin/main` at the merge base
  `a3df64830`. `origin/main` has since gained 12 commits (a fork merge, #1841 and after); they
  are not in this diff and were not reviewed.
- **Scope:** `git diff a3df64830...4acc752fb`: 320 files, +38,630 / −3,915. Every `file:line`
  in the track and verification files is at `4acc752fb`.
- **What the branch does:** tn-lmdb becomes the default reth backend (MDBX opt-in) and gains gap
  fill with an `auto` width, a boot record, a verified weak-head open and format versions 7 and
  8; reth storage layout v2 without RocksDB (change sets and senders in checksummed static
  files, NippyJar format 2, history and the transaction-hash index in the database), one binary
  for v1 and v2, a layout marker and format-freeze fixtures, `db reset-execution` and
  `db migrate-execution`, a hardened MDBX to tn-lmdb migration; a cross-block execution cache
  under block building; a block's state root with storage roots on parallel tasks and the
  account and hot storage walks split by first nibble, with a sampled serial check; the upstream
  trie seek fix TN-0140; an epoch guard that compares a replayed epoch's stored record with the
  closing block the node executed; the vendored-reth review fixes of PR #1829 (not on main yet).
- **Baseline at HEAD:** nightly fmt clean, `cargo check --workspace --all-targets` clean,
  `etc/ci-lanes.sh clippy` clean (`baseline.md`).
- **Method:** nine read-only tracks, each a fresh agent with a narrow brief and the do-not-flag
  lists (T1 engine delta, T2 adapter/benches/CI, T3 layouts/migration/reset/CLI, T4 execution
  cache and path, T5 state root and trie backports, T6 vendored RPC/pool and the vendor2 merge,
  T7 vendored static files and provider, T8 node guards/state sync/e2e, T9 cross-cutting
  upgrade/rollback/features/docs). 44 raw findings; about 220 suspects traced and rejected with
  line evidence; 53 coverage gaps recorded. Every Medium and Low went to an independent verifier
  that saw only the claim, the key question and the file list (Mediums alone, Lows batched by
  subsystem; the two consensus-side Mediums and the trace-arena Medium as Tier 1, one with a
  second verifier). Informationals were not verified. The verifiers ran targeted tests and two
  measured proofs of concept in scratch copies, never in the tree.

## Verdict

| Dimension | Result |
|---|---|
| Correctness and determinism | **No Critical, no High.** No path was found on which two nodes on one release compute a different block, root or persisted row from one input. The parallel state-root composition, the execution cache's invariant, the migration's commit point, the static-file commit path and the vendor2 merge all held under adversarial tracing. |
| Mediums | **5**, none a divergence: two in the operator rebuild procedure (`db reset-execution` on a restored datadir cannot complete, T8-1; a rebuild's replay closes wipe the live epoch's vote-guard rows, T8-2), one crash-robustness gap at jar creation (T7-1), two resource bounds on opt-in or non-default RPC surfaces (T6-6, T6-1). |
| Lows / Informational | 12 / 27 (the Informationals are mostly docs that promise more or other than the code does). |
| Overall | **APPROVE for merge, with T7-1, T8-1 and T8-2 fixed before the release that ships `db reset-execution` to validators, and T6-6 before any node serves `debug`/`trace` to untrusted clients.** |

Verification stats: 44 submitted; 23 verified (16 CONFIRMED, 6 PARTIALLY_VALID, 1
DESIGN_DECISION, **0 FALSE_POSITIVE**); one raised (T7-1 Low → Medium), six lowered to
Informational; 21 Informationals listed unverified.

## Findings, ranked by verified severity

Severity is for TN as deployed. "Exposed" means the RPC transport is reachable by untrusted
clients; HTTP and WS are off and bound to localhost by default, `debug`/`trace` are off unless named.

| id | verified | area | one line | fix (detail in the round file) |
|---|---|---|---|---|
| T8-1 | Medium (High arguable) | node, CLI | After `db load-state` then `db reset-execution`, the start enters epoch 0, opens an empty epoch-0 pack and fails in `get_missing_consensus` with `ConsensusNumberTooHigh` on every restart; the README promises the rebuild works | `reset_execution` refuses a datadir with `restored-state-floor` or without `epoch-0`; README and CLI README amended; a test that the refusal moves nothing |
| T8-2 | Medium | consensus guard | Every epoch close clears `Votes` and `LastProposed` whole; a reset rebuild's replay closes therefore delete the guard rows of the epoch the node was voting in, and nothing says to stay stopped. Re-voting needs a network stalled for the whole rebuild plus a conflicting header, so Medium | `clear_consensus_db_for_next_epoch` takes the closed epoch and keeps later rows of the two guard tables (warning); regression test; README "In place": stay stopped until the network closed the epoch |
| T6-6 | Medium where `debug`/`trace` exposed; Informational otherwise | revm-inspectors patch | TN-0079's 160 MiB cap counts opcode steps only; every call's calldata, output and init code is copied into the trace arena uncounted. Measured: one `callTracer` call at 50M gas, 6.96 GB peak RSS, 0 bytes counted | charge input, output and init code in `call`/`create`/`call_end`, abort with `FatalExternalError` (same -32603 path); correct TN-0079, PATCH.md, regenerate `upstream.diff` |
| T7-1 | Medium (raised) | vendored static files | A new jar's data and offsets files are created before its `.conf`; a plain process kill in that window (every rotation, genesis) leaves a jar `iter_static_files` cannot load, and the node refuses to start until an operator deletes files; no committed data lost | `freeze_config` before `create_or_open_files` for a rows-0 jar; crash test beside `rotation_after_an_empty_offsets_file` |
| T6-1 | Medium when HTTP or WS exposed | vendored RPC | `--rpc.max-active-filters` caps the count, not the size: ~11 MB per filter, ~11 GB at the cap, kept alive by polling; one client filling the cap is TN-0056's documented design | a per-filter criteria cap (1,000 addresses plus topic values) in `new_filter`, `logs`, `EngineEthFilter::logs`; same helper for T6-2 |
| T3-1 | Low | migration | The MDBX source opens non-exclusive; a holder that never commits (needs two datadirs on one `db/`) is undetected and `finish` unlinks `mdbx.dat` under it | `with_exclusive(Some(true))`, a Busy hint, an idle-holder test |
| T3-2 | Low | migration | File-existence checks run before `lock_db_dir`; a race with another process finishing gives a false "fresh datadir" refusal, transient, no file touched | lock first, then read the three files |
| T1-3 | Low | tn-lmdb | `BootFile::write` rebuilds errors without their OS code, so the adapter reports EIO for ENOSPC/EACCES on the boot record (PORTING.md promises the errno) | `sys::Context::wrap`; EACCES source-chain test |
| T2-2 | Low | CI | The arm64 CRC lane tests only tn-lmdb's copy of the kernel; the storage copy is identical today but nothing keeps them in sync | run both crates' CRC tests, or diff the copies |
| T2-3 | Low | CI | `backend-gate.sh` resolves default features only; the `adiri`/`faucet`/`js-tracer` release graphs are unchecked (none enables mdbx today) | loop the gate over the release feature sets; planted test |
| T2-5 | Low | adapter | A caught decode panic still prints the hook's message and backtrace per read, naming neither table nor key | log table and key at the catch; document the hook output |
| T5-1 | Low | state root | A failed or `TipMoved` parallel root task increments no metric; one log line per block is the only signal | `tn_reth.parallel_state_root_task_failures_total{reason}` |
| T5-4 | Low | state root | No TN test would fail if TN-0140 were reverted: every TN oracle shares `TrieNodeIter`; snapshot import is the only production guard | a test with upstream's shape through the overlay against a from-scratch root |
| T6-2 | Low | vendored RPC | `logs` subscriptions keep an unbounded client `Filter` outside the filter cap (WS or IPC) | apply the T6-1 check in `subscribe` |
| T6-3 | Low | vendored RPC | `eth_simulateV1` is outside TN-0057's budget: up to 256 × 50M gas per request | backport upstream b8116b42b (#24387), or a per-block `CallManyBudget` |
| T7-3 | Low | vendored static files | `gap_at` takes the index read lock while holding a dashmap shard ref, the inverse of `update_index`; a real two-thread deadlock, unreachable on TN at HEAD (gaps refused at startup, segments deleted only at startup) | `gap_at` takes an owned header after the provider is dropped |
| T8-3 | Low | e2e | No lane runs the real `v0.16.0-adiri` upgrade: every lane's previous binary is this tree's `mdbx` build, so resync.rs's consensus-migration branch never executes; documented in the attest script, not the README | README note; optional all-validators upgrade target |

### Informational (27, not verified; locations in the track files)

- **Engine and adapter:** T1-1 the auto gap-fill width is never clamped to `MAX_GAP_FILL`
  (the `u32::MAX` fallback is unreachable); T1-2 the weak-head read-ahead has no in-file bound on
  a forged cell's `pgno` (cost only); T2-1 the mdbx suites run in no lane by documented decision;
  T2-8 `create_tables_for` rewrites the layout record on every call.
- **Layouts, migration, reset:** T3-3 README says refused datadirs are left unchanged, but the
  open creates `db/`, `database.version` and an empty `tn-lmdb.dat` first, and the table omits
  the unwind-target refusal; T3-4 `import_chain_scaffold` comments cite a Headers-height check
  the code no longer uses; T3-5 the reset's "run again in a second" advice cannot succeed when
  both `<stamp>` and `<stamp>.partial` exist, and `db stats`/`check`/`migrate-execution` ignore a
  `.partial`; T9-1 README lists the two first-start migrations in the wrong order (reth first);
  T9-2 the `mdbx` build has no version tripwire while it still moves the consensus database
  (scope the README sentence); T7-2 TN-0083's torn-genesis resume fails under v2 but
  `layout::settle` heals or refuses first (narrow the ledger claim).
- **State root and execution:** T4-1 the stale-hit response of the execution cache is documented
  in three places that disagree and names the wrong entry point; T5-2 the mismatch runbook does
  not say that turning the parallel paths off repairs no trie node already persisted; T5-3
  `open-issues.md` says a replay "names the first block", the node stops at the epoch close.
- **RPC docs and ledger:** T6-4 `rpc_blocking.rs` says servers are rebuilt per epoch; T6-5
  TN-0056 says the node's own pool listeners never count (they take slots); T6-7 the JS tracer
  timeout is unclamped (off by default, disconnect interrupts); T6-8 PATCH.md/js-tracer.md say the
  patch matters only to JS tracers (false since TN-0079); T6-9 docs and TN-0056 say server-busy
  is -32604, the code sends -32009; T6-10 `eth_syncing.md` says always `false`, the node returns
  a progress object while syncing; T6-11 four method pages (trace_filter's blocks at a time,
  debug_traceCallMany's replay path, eth_callBundle's count source, shared-port CORS wording).
- **Static files, CI, docs:** T7-4 the two changeset count functions bypass `load_jar`'s
  unchecked-jar refusal (stats only); T7-5 `reverse_offset` underflows on a 2..8-byte v1 offsets
  file; T2-4 ACTIONS.md, the attest script, ci-lanes.sh and main-compiles.yaml still say "four
  test lanes"; T2-6 `EXEC_SHARED_BASE=1` never fails when the sides' chains differ; T2-7 one
  README bench number cites no run; T9-3 vendor notes still describe storage v1 on MDBX and
  "tndb-b owners" (the 1b20c9bfc verdict rests on it); T9-4 the hardware floors cite 2026-11-12
  runs with no commit or command recorded.

## What was traced and holds (the top items per track)

- **Determinism (T4, T5, T9):** the parallel storage roots, the sixteen account subtries and the
  storage pieces compose exactly the serial root and trie updates (same layers, outermost-layer
  nibble restriction, no stored root node, exact hash-mask reconstruction); every other shape
  falls back deterministically; the sampled check at every 256th block compares the whole
  `(root, TrieUpdates)` tuple and persists the serial result on a difference; tasks read the
  walk's snapshot (single writer, tip marker, `TipMoved` check) and a failed task fails the
  block the same way on every node. The execution cache's invariant (every entry equals the value
  at the anchor hash; fills from the parent state and the exact persisted bundle; wiped set for
  destroyed accounts; dropped on any failure) held on every path including CREATE2 over a wiped
  address, EIP-161 clears, 7702 delegations, epoch-close system calls and the adiri fork swaps;
  it is never consulted by RPC and never shared across threads; a checked hit discards the whole
  attempt on a mismatch. No environment, clock, thread-count or host read on the branch changes
  consensus-visible bytes (`TN_PARALLEL_*` selects code that computes the same root; gap fill
  rewrites pages with their own bytes; the cache knobs are test-only).
- **Engine (T1):** gap fill is byte-identical (private bounce buffer through `pread`/`pwrite`,
  gaps strictly between stamped pages, metas never inside a gap); the boot record never says
  "clean" after a failed writeback; the auto width moves only to adjacent candidates and
  nothing on disk depends on it; every overflow run gets its naming tree's tag; a `Durable`
  no-change commit syncs on the writer the transaction owns.
- **Migration and layouts (T3, T9):** the migration state table matches the code row for row;
  `database.version` 1002 is written before any data file and `finish`'s stat record closes the
  rename-to-version window; `v0.16.0-adiri` refuses the directory before any write (reth's
  `check_db_version_file` runs before `DatabaseEnv::open`) and opens reth before the consensus
  database, as the README says; the `holds_only_genesis` heal can never remove a block past
  genesis and a stop part way is found again; a v2 datadir cannot lose `storage_settings` alone.
- **Static files (T7):** a committed row can never be cut by a heal; the uncommitted tail is
  never served; every value read verifies its CRC before decode; no range reader returns a
  shorter list on a failed read; a crash in the static-files-before-database window is always
  pruned back by the consistency check under v2.
- **RPC and the vendor2 merge (T6):** every in-scope vendored file, patch, doc and TN RPC file is
  byte-identical to `origin/sls_reth_vendor2`, with the one overlapping `env/rpc.rs` line resolved
  correctly; the ledger has no duplicate ids and `vendor.py ledger` and `lockfile.py check` pass;
  every trace/debug/bundle/callMany handler moves its permit into its blocking closure; R1..R30
  landed as the PR description says.
- **Node (T8):** the epoch guard stops no honest node on a legitimate state (restored datadir,
  epoch-0 placeholder, never-executed epoch, empty closing output, other-codec record all
  traced); the block-1 anchor only relaxes header 1's parent check; the persistence order at an
  epoch boundary is unchanged; the state-sync bound is local and cannot be starved by a peer.
- **Dependencies (T2):** `Cargo.lock` adds, removes and re-pins no package (feature edges only:
  rustix, crc, crc32fast, fixed-cache =0.1.8, sha2); the revm-inspectors `upstream.diff` is
  byte-identical to the registry delta; `cargo audit` is not installed on this host.

## Coverage gaps worth adding (from the 53 recorded)

1. A test of the TN-0140 shape through the output overlay against a from-scratch root (T5-4).
2. `db reset-execution` on a snapshot-restored datadir, and a reset inside an open epoch (T8).
3. A jar data file without its `.conf` at startup; a v2 torn-genesis resume (T7).
4. A real `v0.16.0-adiri` previous binary in one upgrade lane (T8-3); the side-observer swap.
5. The migration with an idle MDBX holder; a block-level TN-0082 test (missing bytecode row).
6. A multi-node e2e asserting `exec_cache_stale_hits_total == 0` and
   `tn_reth_parallel_state_root_mismatches_total == 0` on every node.

## Action items, in order

1. **Before the release that gives validators `db reset-execution`:** T8-1, T8-2, T7-1 (each a
   small change with a test; the README "In place" section gains the stay-stopped rule).
2. **Before any node serves `debug`/`trace` or HTTP/WS to untrusted clients:** T6-6, T6-1 (+T6-2),
   T6-3 (upstream b8116b42b).
3. **Cleanup pass:** the remaining Lows (T3-1, T3-2, T1-3, T2-2, T2-3, T2-5, T5-1, T5-4, T7-3,
   T8-3) and the 27 doc items; the docs items T6-9 (error code), T6-10 (`eth_syncing`), T9-1
   (migration order) and T3-3 (what a refused open leaves) are the ones an operator would act on.

## Notes on how the review ran

- The usage limit stopped the nine tracks twice; each resumed from the `## Status` checkpoint
  in its file, and no track was restarted from scratch.
- The verifier agents cannot write report files: rounds 1 and 2 were saved by the coordinator
  from their hand-backs; round 3 was written by the verifier itself. One round-2 verifier ran
  `git fetch` on `~/.cache/tn-vendor/reth`, which moved that cache's `origin/main`.
- Files: `GROUND_RULES.md`, `briefs/`, `T1.md`..`T9.md`, `T6-docs-slice.md`,
  `T6-poc-call-arena.rs` (the T6-6 proof of concept, run only in a scratch copy),
  `verified-round1.md`, `verified-round2.md`, `verified-round3.md`, `baseline.md`, `files.txt`, `HEAD.txt`.
