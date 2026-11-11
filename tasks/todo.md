# Vendor reth into vendor/reth — 2026-10-30 (branch sls_tndb_b_vendor, base f7916c05f)

Goal: vendor only the reth crates TN needs, at the pinned v1.11.3 (d6324d63), as a nested
cargo workspace under vendor/reth; keep upstream-sync possible; then reduce.
User decisions (2026-10-30): local commits/tags only (no push); base v1.11.3; nested
workspace (root excludes vendor/reth); reduction work allowed on this branch after a tag.
Autonomous weekend run: decide, document in vendor/reth/DECISIONS.md, keep going.
Hard requirements found: CI RUSTFLAGS="-D warnings -D unused_extern_crates" now applies to
vendored code (path deps are not lint-capped); `cargo fmt --all` walks path deps.

## Phase 0 — baseline
- [x] Upstream reth clone at v1.11.3 in scratch; confirm d6324d63
- [x] Baseline: resolved reth package set + features (cargo metadata), non-reth lock snapshot
- [x] Baseline build: cargo check --workspace --all-targets (default, tndb-b); warm target

## Phase 1 — pristine import (zero behavior change)
- [x] Compute vendored set (resolve closure) + dev-dep handling decision (101 + testing-utils; drop e2e-test-utils = TN-0001)
- [x] tn/vendor.py + crates.txt + upstream.toml; verbatim import commit 21dbfd605 (tree ids == upstream), tag reth-vendor-import-v1.11.3
- [x] Root Cargo.toml → path deps; exclude vendor/reth (258df6e9a); TN-0002 fmt
- [x] Verify: lock diff only sources ✓; feature graph identical ✓; check default/tndb-b/-D warnings ✓; clippy default+all-features ✓ (clippy-driver never runs on vendored); fmt --all ✓; rocksdb gate ✓; nextest workspace 3719/0 ✓; nested-workspace reth tests ✓
- [x] Docs (dc3e2ba69); sync tested in scratch worktree: v1.11.4 clean merge + conflict + dirty refusal; v2.0.0 trial (454 files, 6 crates gone upstream)
- [x] Tag reth-vendor-v1.11.3 (pure vendoring complete)

## Reduction (worktree scratchpad/wt-p1, detached; fast-forward branch after Phase 0 tests pass)
- [x] ROADMAP P1: tn-reth off node-builder/node-ethereum; TN-0004 tests; TN-0005 remove 24 crates
- [x] ROADMAP P2: TN-0006 node-api out of rpc; TN-0007 rpc declares chain-state serde; TN-0008 slim
      node-core (devp2p), tn-reth off network/discv4/eth-wire; TN-0009 remove 10 crates
- [x] ROADMAP P6a: TN-0010 drop auth server/EngineEthApi/ValidationApi/TestingApi; TN-0011 remove 3
- [x] ROADMAP P4a: tn-types/tn-reth off reth-primitives; TN-0012 remove it  → 63 reth crates (from 101)
- [x] Static verification each step: check -D warnings all-targets, graph.py diff (features restored or
      justified), vendor.py check, nested all-targets (P1,P2,P6a), clippy default+all (P2; P4a running)
- [x] Phase 0 nextest: 3719 passed / 0 failed → tags reth-vendor-v1.11.3, reth-reduction-start on 2efdecf72
- [x] TN-0013 sync pin to v1.11.4 (no changes in scope); upstream backport survey → tn/notes/upstream-backports.md
- [x] USER REQUEST (mid-run): RocksDB gone entirely. Step A done (TN features dropped; TN-0014 vendored
      backend/features/crate removed; expanded source of provider/db-common/db-api proven identical;
      gate now covers whole Cargo.lock + manifests, negative-tested). Step B (stub/plumbing) deferred: D15.
- [x] Full nextest on reduced head: 3719 passed / 0 failed / 90 skipped (1h30m incl. scan)
- [x] e2e on final head: test-restarts 8/8, test-epochs 11/11
- [x] Final head c7489bf12: clippy default + all-features, fmt lane, rocksdb gate all pass
- [x] Final docs pass (ROADMAP status block, 9a014c395) + handoff summary; scratch worktrees removed

## Review (vendoring)

Branch head 9a014c395, local only. 101 reth crates vendored verbatim at v1.11.3 (tag
reth-vendor-import-v1.11.3), wired in with zero graph change (tag reth-vendor-v1.11.3), then
reduced to 63 (from tag reth-reduction-start, 20 commits). Synced to v1.11.4. RocksDB removed
from vendored reth and TN, guarded by etc/rocksdb-gate.sh over the whole lockfile plus manifests.
Tests: nextest 3719/0 (63-crate tree), e2e restarts 8/8 and epochs 11/11 (final head),
clippy/fmt/gate clean. Open for the maintainer: backports (ff2657600 WS origin check, JS-tracer
DoS), RocksDB step B (D15). target/ci-flags (32 GB) is a scratch build dir that can be deleted.


## Security backports + roadmap (2026-10-31; maintainer: apply High/Critical security, track rest; tndb-b/Phase 3 out of scope)
- [x] Verify each High item against TN; check GHSA (none published)
- [x] TN-0015/16 ws origin (+997af404a prereq), mutation-checked
- [x] TN-0017 JS tracer interrupt ported to revm-inspectors 0.34 (D16), mutation-checked
- [x] TN-0018 txpool sender-id growth
- [x] TN-0019 RPC blocking-work limits (6 commits; raised to High: shared blocking pool w/ engine)
- [x] Downgraded d3d7fb31d, 6e4009eed (D17); tracker rewritten; D16-D18; ROADMAP/README
- [x] Nested tests (rpc, rpc-builder, rpc-eth-api, txpool) pass; clippy lanes, fmt, gate pass
- [x] Backport head f093532ca: nextest 3719/0/90 (5 LEAK flags under load; RPC one reran clean 3/3), restarts 8/8, epochs 11/11
- [ ] Roadmap work in worktree scratchpad/wt-road (target/road), plan:
  - [x] R1a TN-0020 (f771138c3): node-core version from pin; libgit2/libz/vcpkg gone; node-core+tn-reth Fresh across commits (verified)
  - [x] R1b jemalloc dropped (0bc0f1373, D19); graph diff: 7 pkgs leave, only expected feature changes
  - [x] R2 TN-0021 (2276dc0c6): -2.3k lines; TN-0007 superseded; tn-reth declares peers secp256k1; nested tests pass
  - [x] R3 usage map regenerated (7867f29e0, agent + spot-checks)
  - [-] R4 skipped: churn across 9 TN crates, nothing leaves the graph
  - [x] dead tn-reth re-exports + root reth-config removed (8037b22e1); ROADMAP status (c8a527384)
  - [x] worktree head c8a527384: CI-flags check, clippy lanes, fmt, gate pass
  - [x] Branch fast-forwarded to c8a527384; worktree removed
  - [x] Final: nested check, full nextest + e2e on c8a527384: nextest 3719/0/90 (1 LEAK, tn-engine test reran clean 3/3), restarts 8/8, epochs 11/11; ROADMAP verified line committed afccce2ef

## Next backports (2026-10-31, user: "start on your recommended next" + static-file fixes OK; MDBX/RocksDB stay out)
- [x] TN-0022 (485c90a1d): 47544d9a7; txpool 225/225; 2 of 3 new tests fail with fix off
- [x] TN-0023 static-file series, upstream date order, one commit each (all mutation-checked except the two cache commits, whose tests are upstream's race tests):
  - [x] b629142d0 (0be1beba7)
  - [x] 01d039b1f (0ec7d956c)
  - [x] 1e17affd3 (69ad58a04) Transactions arm, TN genesis empty
  - [x] 0d1324f14 (5c7d0b5fe)
  - [x] 22e3708b2 (aea0b9eb4)
  - [x] 95679ace2 (f9e812e3e) conflict = TN's map_or spelling; test adapted to read_only(path,false); fmt fixed
- [x] Tracker (1af59b607): move applied rows; record 1b20c9bfc/6bd6bf28a/00f9bd2a9/db17c899c not applicable, adfcad70c Low; mark 294e21507/7bf9241fe as MDBX (tndb-b owners)
- [x] Verify (bg blqa7t7rt, scratchpad/verify-0023.sh, logs v23-*.log; adds make test-sigkill + tndb-b nextest): all pass; nextest 3719 + tndb 3744; restarts 8/8, epochs 11/11, sigkill 7/7; 6 LEAK flags reran clean 2x; reth-db MDBX ENOMEM at 16 threads is pre-existing (same on afccce2ef sources), passes at 2 threads, README note. Was: nested check --workspace, nested tests (transaction-pool, nippy-jar, provider, db), nested clippy, TN CI-flags check + clippy lanes + fmt + gate, full nextest + restarts + epochs
- [x] ROADMAP status + README (717a9c1d0); report

## Reduction pass 2 (2026-10-31; tag reth-reduction-2-start = 717a9c1d0; TN-0024 d3d7fb31d = 189dab4fc after it)
Measured (scratchpad/pare2): node graph 732 pkgs, 63 vendored; 74 third-party only via vendored crates.
Batch A, dependency cuts with no behavior change, one commit each, graph.py diff each:
- [x] A1 TN-0025 (398cf8329) node-core build.rs without vergen 9 (-vergen, vergen-lib, cargo_metadata, cargo-platform, derive_builder x3; build-only)
- [x] A2 TN-0026 (9b0dc3171) reth-tasks: tracing::Instrument instead of tracing-futures (-1)
- [x] A3 TN-0027 (9b0dc3171) txpool: drop docs-only aquamarine (-aquamarine, include_dir x2)
- [x] A4 TN-0028 (854cfaa10, d33faa343) db-common: drop the state-dump import TN never calls (-reth-etl, boyer-moore-magiclen, debug-helper)
- [x] A5 TN-0029 (987012555, 5d3563525) OTLP: drop node-core TraceArgs + reth-tracing-otlp; tn-reth stops enabling otlp/otlp-logs (-reth-tracing-otlp, opentelemetry-appender-tracing, otel logs features)
- [x] A6 TN-0030 (c6d02c79b) toml: drop reth.toml load/save in reth-config + node-core if TN never loads it (-toml, serde_spanned, toml_writer)
- [x] A7 TN-0031 (e7399a408, d8971952b; alloy rand now from root Cargo.toml) network-api: drop events/downloaders/noop/Peers + reth-config peers/sessions (-reth-network-p2p, reth-network-types, reth-net-banlist, alloy-rpc-types-admin); TN worker.rs Peers stub, rpc-builder bounds
- [x] A8 TN-0032 (c85f1184d, 86a0dbe59) engine-primitives: drop engine-API message types (-reth-payload-builder-primitives) if unused
- [-] A9 skipped (no package leaves; trie is consensus-sensitive) unused edges: network-api (alloy-consensus, alloy-rpc-types-eth), trie (alloy-eips, revm-database)
- [-] A10/A11 deferred to after tndb-b (MDBX builder / reth-db) notify watcher for read-only static files (-5); A11? sysinfo in StorageLock (-3 on mac)
Decision for user: js-tracer off (-30 incl. boa JS engine; JS tracers in debug_trace* return an error).
Skip: shellexpand (operator path semantics), parity-scale-codec/modular-bitfield/lz4/fixed-map (on-disk formats), reth-tracing (TN's logging stack), trie code (consensus).
Batch B: TN-0033 (8734b7c42) mev namespace server removed; node-core args = TN config rewrite (skipped); rest is storage/consensus/txpool-design scope.
- [x] Verify (bg bd05abtq9, scratchpad/verify-pare2.sh, logs vp2-*.log; recorded in b92f4624a): nested check/clippy/tests of touched crates, TN CI-flags check + clippy + fmt + gate, full nextest 3719 (tndb 3744) + restarts 8/8 + epochs 11/11 + sigkill 7/7
- [x] Docs (95614334f): CHANGES TN-0025..0033, ROADMAP status + 'Reduction pass 2', D20, usage map; [x] report

---

# tndb review — 2026-10-29

Plan: /Users/sstanf/.claude/plans/perform-a-review-of-compiled-clock.md
Workspace: tasks/review-tndb/
Fix policy (user): fix Medium+ now, ask on Low/Info. Bench gate: this Mac only.

- [x] Step 0: baseline (check, clippy, lib tests) → baseline.md; GROUND_RULES.md; briefs
- [x] Wave A: T1 write path/rollback, T2 durability/commit, T3 compaction, T4 byte layer
- [x] Wave B: T5 recovery/open, T6 txns/database/split/metrics, T7 B+tree as used, T8 perf audit
- [x] Aggregate + dedupe → raw-findings.md
- [x] findings-verifier → report-tndb.md
- [x] Present verified findings (report-tndb.md)
- [x] Fix Medium #1 (split.rs check_environments manifest check; test extended + mutation-checked; README updated; 811 pass, clippy/fmt clean)
- [x] Lows #10, #2, #13 fixed, #4's three tests added; each mutation-checked (5 mutants killed); docs #3 #6 #7 #8 #9 + marker policy (README reword) applied. 817 lib tests pass, clippy/fmt clean
- [x] Perf gate: lib tests + clippy; A/B bench 8 runs (full set, mixed paced, isolating runs) → flat; reports in tasks/review-tndb/bench/
- [x] ci-lanes test-default (3223 passed, 64 skipped) + make test-restarts (8/8 passed)
- [x] Review section below; lessons.md not needed (no corrections)

## Review

Verdict: APPROVE. No Critical or High findings in tndb or the storage files it uses directly.

Fixed (all mutation-checked, each with a test):
- #1 Medium: an environment emptied beside the `mdbx-migrated` marker opened as a fresh DB. `split.rs`
  `check_environments` now also requires the `tables` manifest and refuses to open the environment empty.
- #10 Low: a key longer than an index can hold (2032 bytes) was written to the manifest before the index
  creation failed, bricking the DB on reopen. `store.rs` `ensure_ksize` checks `Node::geometry_ok` first.
- #2 Low: the idle commit thread held an ArcSwap guard across its condvar wait, pinning the retired
  generation after a compaction switch. `commit.rs` copies the three fields out and drops the guard.
- #4 Low: compaction catch-up net-effect coverage; three tests added (a CLEAR-skipping mutant survived
  the old suite).
- #13 Low: an index open/rebuild I/O error did not name the table or path. Errors now wrap the index path.
- #3, #12 downgraded to Informational; #6-#9 README wording fixed. #5, #11, #14, #15 left as noted.

Verification: fmt + clippy clean; `cargo test --release -p tn-storage --lib` 817 passed (811 baseline + 6);
ci-lanes test-default 3223 passed; make test-restarts 8/8 passed. A/B bench (8 runs, interleaved
before/after/after/before) flat on workload_consensus_rounds and workload_mixed; early "after" slowdowns
were residual build load, confirmed by isolating runs. Linux confirmation run for the commit-thread change
is optional (macOS fsync does not flush the drive). Nothing committed.

# PR #1558 unresolved review comments — 2026-10-29

Source: MavenRain round 2 (2026-10-26, at d385904c) lists what round 1 fixed; the rest is open.
Policy (user): evaluate each, fix the legit ones. No perf regressions.

- [x] A legit (copy_table never clears; stale rows survive a retry if MDBX changed): clear_table before each copy + test — agent implementing. User: no rollback to MDBX is supported; docs worded as a retry after a failed copy
- [x] B legit (comment wrong; `try_exists` follows symlinks): comment reworded, no guard — agent implementing
- [x] C legit: note added to the CLI README's rollback section (one-way, one-time; link to validator-operations.md)
- [x] D legit (PersistError before the cut is no verdict): arm widened + FAIL_NEXT_RECOVERY_SEAL hook + test — agent implementing
- [x] E legit (every epoch's import fails before download; never stops): scope to the pending epoch, CorruptPack + db repair on the second failure — agent implementing
- [x] F legit: mutant (final sync removed) survived all 146 tndb tests; `test_tndb_power_loss_keeps_the_switch_tail` added, fails on the mutant, passes restored; README hardening list updated
- [x] G legit: 16 GiB + 7 indexes = 23 GiB per compaction, 62 GiB for all three; doc reworded
- [x] H legit (Open/IO swallowed → needless rebuild): same no-verdict guard + ENVIRONMENTAL_NEXT_VALIDATE_OPEN hook + test — agent implementing
- [x] Fixes + tests (5 new, each mutation-checked); READMEs/CLAUDE.md hooks updated; fmt clean; clippy (storage, --tests) clean; storage lib 822 passed
- [x] ci-lanes clippy (both feature sets) clean; test-default 3228 passed; make test-restarts 8/8
- [x] Review section

## Review (PR #1558 comments)

Eight unresolved comments evaluated; all eight accurate. Seven fixed, one doc-only (the symlink
comment). Five new tests, each mutation-checked. Not done: stopping the node when a pending swap
fails twice (needs a typed error through state-sync; follow-up). Not touched: the merge conflict
with main and the ctutils advisory. No hot path changed (migration runs once, `db repair` is CLI,
the swap path is import-failure-only), so no bench run. Replies drafted in
tasks/review-tndb/pr1558-replies.md. Nothing committed.

## Post-merge attest fix (2026-10-30)
- [x] `make attest` failed to compile tn-network-libp2p tests after merging main; both broken tests were byte-identical to main (`penalty` 2-arg call, `contains_key(&BlsPublicKey)` on a `PeerExchangeKey` map). Fixed in tests only; workspace `cargo test --all-features --no-run` builds; the 14 touched tests pass.
- [x] `make attest` then failed two tests, both main-side test/code mismatches from the 2026-10-28 fork merges: `test_probe_loops_open_last_server_first` (code now forgets the preferred sync server after a failed probe) and `test_identical_kad_record_refreshes_expiry` (identical republish rewrites the row only past half the TTL). Tests aligned to the current rules in `9b098d6c2`; the 17 related tests pass. Full `test-default` and `test-adiri` lanes rerun for confirmation.

## js-tracer behind an opt-in feature (2026-10-31, maintainer request)

- [x] Graph snapshot before; manifests: tn-reth `js-tracer` feature (off), forwarded by tn-node and
      the telcoin-network bin; drop the always-on `js-tracer` from reth-rpc/-eth-api/-eth-types deps
- [x] graph.py diff: boa and friends leave; check no surviving third-party crate loses a feature
- [x] TN test: JS tracer refused without the feature, runs with it (namespaces.rs)
- [x] crates/tn-reth/js-tracer.md: what it is, the issues, how to turn it on (link to the feature)
- [x] Docs: tn-reth README feature table (+ stale umbrella paragraph), root/tn-reth Cargo.toml
      comments, book debug_traceTransaction `tracer` line, ROADMAP/D21/usage-map
- [x] Verify: CI-flags check default + tndb-b + js-tracer ✓; clippy lanes (default + all-features) ✓;
      fmt ✓; new test both ways ✓; full nextest 3720/0 (3 LEAK flags reran clean) ✓; nested reth-rpc
      lib 34/34 default, 40/40 with js-tracer (6 TN-0017 tests) ✓; clean release builds off/on ✓

Review (0f5695363, size numbers in the follow-up commit): default build drops 35 packages (865 to 830)
and 7.9 MB of stripped binary (54.8 vs 62.7 MB, macOS arm64); clean build time about equal (1m50s vs
1m54s). Found while writing the doc: block traces and debug_traceCallMany ignore the trace timeout and
run to completion after a disconnect (TN-0017 covers only traceTransaction/traceCall); CHANGES.md
TN-0017 wording corrected. Nested `--locked` fails by design (the nested lockfile gains test deps).

## Review of the vendored reth (2026-11-01, maintainer request)

Workspace: tasks/review-vendor/ (HEAD.txt, GROUND_RULES.md, briefs/, T*.md, raw-findings.md, report)
Scope: TN's changes to vendor/reth against the pinned upstream (v1.11.4), the integrity of the
copy (verbatim import, sync, ledger coverage), TN's integration (manifests, lockfile, CI, gate,
tooling). Upstream reth code TN did not change is out of scope (named, not reviewed).

- [x] Delete target/ci-flags and target/lint-unused (53 GiB freed)
- [x] T0 (me, tasks/review-vendor/T0.md): import == v1.11.3 by blob id; HEAD vs v1.11.4 by blob id; every differing file in
      CHANGES.md; lockfile third-party versions vs main; cargo audit/deny on the graph
- [x] Ground rules + briefs written; 4 track agents ran 2026-11-01 (T1-T4.md; 20 raw findings):
      T1 backports + upstream security triage, T2 RPC-layer changes, T3 execution/primitives/
      provider/node-core changes (MDBX/RocksDB internals out), T4 TN integration, build, CI, tooling
- [x] Dedupe (18 distinct, raw-findings.md); 7 Medium+ verified by 4 fresh verifiers (V1-V4.md): all
      CONFIRMED, T1-2 raised to High, T4-2 lowered to Low; coordinator checked T4-1, T1-5, T4-4; PoCs in poc/,
      verifier worktrees removed
- [x] Report: tasks/review-vendor/report-vendor-reth.md (+ copy at repo root report-vendor-reth.md)

Review: no Critical. High x2 (T1-1 recursive JS accessor aborts the node, T1-2 unbounded JS callback;
both js-tracer builds only). Medium x4 in the default build (T1-4 unpermitted eth methods, T2-1 pool
nonce re-admission, T2-2 syncing subscription leak, T1-3 getLogs chunk threads), plus an adjacent TN
task-manager notifier leak already on main. Integrity of the copy, removals and backport fidelity all
hold. Four upstream fixes missing from the tracker: a5638ca, 055fafc72, 75d587208, d85d9deae.
My own js-tracer.md (2026-10-31) repeats D16's wrong bound claims; flagged in the report, not fixed.

## sls_reth_vendor: the vendored reth as its own branch off main (2026-11-02)

Goal: a branch off `main` (7167e7800) holding only the 72 vendor commits from
`sls_tndb_b..sls_tndb_b_vendor`, for a PR against `main`. Local only; no push without asking.

- [x] Worktree `.claude/worktrees/sls_reth_vendor` on new branch `sls_reth_vendor` at `main`; tn-contracts by --reference
- [x] Cherry-pick the 72 commits in order (no -x); resolve conflicts against main's pre-tndb-b code
      (24 overlap files: Cargo.toml/lock, tn-reth, Makefile, pr.yaml, gates; 3 tndb-b-only files dropped)
- [x] Cargo.lock: re-resolve offline at each conflicting commit; no third-party version changes vs main
- [x] No tndb-b code on the branch (`crates/tndb-b*`, the `tndb-b` feature)
- [x] Prep commit first: the umbrella-crate part of 9905e5468 (vendor commit 4 needs it), no tndb-b
- [x] Verify: fmt, clippy (workspace, all targets), the rocksdb gate, nested vendor tests, full nextest
- [x] Compare against sls_tndb_b_vendor: the vendor/reth tree must be identical; non-vendor diff = vendor intent only
- [ ] Remove the worktree (kept until the user is done with it); report; draft PR text (tasks/pr-sls_reth_vendor.md)

Review (2026-11-02): `sls_reth_vendor` = main 7167e7800 + 74 commits: a prep commit (the umbrella
part of 9905e5468: reth's individual crates instead of `reth`), the 72 vendor commits, and a D8
note. vendor/reth is byte-identical to sls_tndb_b_vendor; each commit's non-vendor change matches
the original except dropped tndb-b hunks (tndb-b feature lines, tndb-b-reth's three workspace deps
reth-db-api/reth-trie-common/reth-storage-errors, two tndb-b test files). Lock: 1120 -> 986
packages, none added, no version changes; every commit resolves under --locked; feature graph
identical to the old tip (830, 865 with js-tracer). fmt, clippy, gates, test-adiri 1878/1878,
test-default 3303/3304 (the failure is main's #1715 test using a non-UTF-8 file name, EILSEQ on
APFS), nested reth tests 1531 + 38 + 40. Not run: e2e. Not pushed. Commit 4's message reworded
(tndb-b-reth sentence) with filter-branch. The TN-side tags still point at sls_tndb_b_vendor.

## sls_reth_vendor: close out open issues without an execution fork (2026-11-04)

Goal: every open issue on the PR fixed or settled, with evidence that the branch executes and
stores exactly what `main` (the testnet's reth v1.11.3 git tag) does. Local commits only.
Decisions (user, 2026-11-04): patch revm-inspectors for T1-1; T2-1 in a separate PR; the task
notifier leak and the #1715 macOS test as separate branches off main; local tests only (no
testnet replay).

Fork safety (gate):
- [x] F1 Independent audit of every vendored edit on the execution/storage path vs v1.11.3, and of
      TN's import moves: fork-safe. Only TN-0023 differs, and only for a racing, torn or corrupt
      static file, where v1.11.3 returns wrong data and the branch correct data or an error.
- [x] F2 Run-time features per target (linux x86_64, macOS arm64), main vs branch: no change on any
      execution crate (revm*, precompiles, alloy-primitives/consensus, k256, secp256k1, blst,
      c-kzg); removals are serde/ssz/metrics/otlp/jemalloc/js-tracer; reth-trie parking_lot came
      from serde; reth-evm metrics gates an unused module.
- [x] F3 Mixed-binary e2e test (test_upgrade_rolling_in_place; passed vs 43dac6008, rerun on final head pending): start on main's binary, roll each validator to the branch's binary
      over its datadir with txs and epoch boundaries, fresh node on the new binary re-executes
      history, every block hash compared on every node
- [x] F4 Existing e2e suites on the branch (restarts 8/8, epochs 11/11)
- [x] F5 T3-1: a TN test pinning storage v1 for new datadirs

Review findings on this branch:
- [x] T2-2 backport d85d9deae (syncing subscription leak) — TN-0034
- [x] T1-3 port 055fafc72 (eth_getLogs chunk permits, try_acquire) — TN-0035
- [x] T1-2 + T1-4 blocking-pool permits: debug_trace* hold the permit inside the closure (TN-0036);
      eth_estimateGas/createAccessList/callBundle/fillTransaction take one; D16, js-tracer.md,
      js_tracer_interrupt.rs comment and the five doc lines corrected
- [x] T1-1 patches/revm-inspectors (0.34.3 + a5638ca recursion limit 128) via [patch.crates-io]
- [x] T1-6 WS origin check on shifted worker ports
- [x] T4-1 CI lane: vendored crates' tests + a js-tracer test build
- [x] Docs: T2-3, T2-4, T3-2, T4-7 (T1-5 with the RPC branch); tracker rows for a5638ca, 055fafc72, 75d587208, d85d9deae
- [x] Tooling: T4-2 rename detection, T4-3 ignored-file warning, T4-4 gate substring, T4-5 graph.py
      per-unit features, T4-6 [patch] sections
- [x] Re-run lanes, nested tests, F2 and F3 on the final head; update the PR text

Elsewhere:
- [x] T2-1 follow-up issue text (tasks/issue-pool-quorum-nonce-readmission.md) (pool re-admits a quorum-batched nonce; 75d587208 + repool design)
- [x] Branch off main (sls_task_notifier_leak 7647a8bda): TaskManager keeps finished tasks' shutdown notifiers
- [x] Branch off main (sls_non_utf8_test_macos 73a7bb448): #1715 non-UTF-8 test fails on macOS (APFS EILSEQ)

Review (2026-11-04): sls_reth_vendor at 05901d252, 92 commits on main 7167e7800, not pushed.
Fork safety: independent audit of the execution/storage path (fork-safe; only TN-0023 differs, on
corrupt/torn/racing static files), per-target feature graph identical to main's for every
execution crate, storage v1 pinned by a test, and test_upgrade_rolling_in_place passing against
main's binary (rolling in-place upgrade under every committee mix, observers on both binaries
re-executing from genesis, rollback to main, every block root equal on 6 nodes). Review findings
fixed except T2-1 (own PR; issue text in tasks/issue-pool-quorum-nonce-readmission.md); trace_*
and debug_executionWitness* still free permits on disconnect, as upstream. Final run: upgrade,
restarts 8/8, epochs 11/11, fmt, clippy, adiri 1882/1882, default 3307/3308 (the macOS-only
#1715 test, fixed on sls_non_utf8_test_macos), vendor lane 15 + 1536 + 38 + 44 + 414, gates,
--locked. One fix found by the final run: graph.py needed --color never under the lane's env.
Separate local branches off main: sls_task_notifier_leak (7647a8bda), sls_non_utf8_test_macos
(73a7bb448), each with a PR draft in tasks/. Agent worktrees removed; their branches
(sls_reth_vendor_{e2e,rpc,tools}) are fully merged and can be deleted.

## sls_reth_vendor: fold in the two fixes, final review, agent guide (2026-11-05)

User: merge the two fix branches (push back if extensive), final doc/code review of the vendored
code, small targeted fixes only, an agent guide (README/CLAUDE.md) with token-saving pointers and
upstream-tracking hints, and a list of issues too large for this PR.

- [x] Fold in sls_non_utf8_test_macos (4 lines) and sls_task_notifier_leak (~50 prod lines + tests)
      by cherry-pick (main squash-merges, so the branch stays linear): 9701ea70e, 663a01c60
- [ ] Final review, three fresh agents, report-only (I apply fixes):
  - [ ] docs accuracy: vendor/reth docs, tn/notes, PATCH.md, tn-reth docs, ACTIONS.md
  - [ ] code: everything after the 2026-11-01 review (43dac6008..HEAD), fork-safety lens
  - [ ] TN integration + tooling + CI: manifests, tn-reth, e2e harness, vendor.py/graph.py/gate
- [ ] vendor/reth/CLAUDE.md: read order, where things are, grep recipes, commands, rules,
      upstream tracking (tracker, pick, sync), TN-main hygiene; pointer from README
- [ ] Open issues list for reviewers (too large for this PR): vendor/reth/tn/notes/open-issues.md
- [ ] Squash-merge hazard: docs and tags reference branch commits; recommend a non-squash merge
- [ ] Apply small fixes from the review; record anything bigger in open-issues.md
- [ ] Verify: fmt, clippy, tn-types + cli tests, vendor lane, gates, test-restarts/test-upgrade
      (the notifier touches every task)
- [ ] Update the PR text, retire the two separate PR drafts, lessons, memory
- Progress 2026-11-05: review done (docs 14 findings, code 0 Critical/High/Medium + 1 Low + 5 nits,
  integration 1 High + 4 Medium + 5 Low). Commits 9f9902074 (guide + docs), 14978ac99 (TN-0037
  disconnect tests), c1f626757 (nested nextest ci profile, MSRV fallback, test_graph cwd,
  CODEOWNERS), de44838a3 (Dockerfiles, NOTICE), 3d03bc985 (nits). Final verification running
  (scratchpad/branch/final2). PR text rewritten.
- Review (2026-11-05): all items done. Final verification at 3d03bc985 (+ 6205281fa, one doc
  number): test-upgrade (prev = main 7167e7800), restarts 8/8, epochs 11/11, fmt, clippy x2,
  default 3311/3311, adiri 1886/1886, vendor lane under NEXTEST_PROFILE=ci (15 + 1538 + 38 + 46 +
  414), nested clippy, gates, --locked, vendor status/check, CI gate tests 15/15. Not run: Docker
  builds, GitHub runners. Branch not pushed (remote at 43dac6008). Separate PR drafts for the two
  folded-in fixes are superseded by pr-sls_reth_vendor.md.

## Merge main (7527b15c0) into sls_reth_vendor (2026-11-05)

Main moved 7 commits (171 files): `--forward-txs` relay in tn-reth, carried libp2p-gossipsub
patch, worker-gateway production work, #1769 (its own fix for the macOS non-UTF-8 test),
storage fixes, two advisory merges.

- [x] Conflicts: Cargo.toml (both patches), pr.yaml / main-compiles.yaml / ACTIONS.md /
      test_ci_gate.py (union of required jobs), rpc_server_args.rs (main's doc, our crate path),
      node.rs (main's #1769 replaces our cherry-picked skip), e2e common.rs (one observer
      helper with name + extra args)
- [x] Main's new tn-reth code off the `reth` umbrella: env/mod.rs, rpc_tx_forward/target.rs,
      env/rpc/tests/forward_txs.rs (`reth_primitives` -> `reth_primitives_traits`)
- [x] Gateway Dockerfile: main added `patches/` itself; drop our duplicate, keep `vendor/`
- [x] Cargo.lock: --locked OK; merged-vs-main delta == branch-vs-base delta (+58 -192)
- [x] fmt, rocksdb gate, vendor.py check/status, CI gate tests (12/12)
- [x] clippy lane (both passes), 0 warnings
- [x] commit the merge: f14943dd3
- [x] node-record-api Dockerfile lacks examples/ (workspace members; broken on main too): 5ded13999;
      main's check_gateway_image_context passes for both Dockerfiles
- [x] tests at 5ded13999: test-default 3859/3859, test-adiri 1949/1949, test-vendor-reth
      (ci profile) 15 + 1538 + 38 + 46 + 472, test-upgrade vs 7167e7800 passed, test-restarts
      10/10 (incl. main's two forward_txs tests), archive guard
- [x] graph.py: merged-vs-new-main delta == branch-vs-old-main delta (-539 +133); main's own
      commits changed nothing in the node's feature graph
- [x] PR text: macOS bullet, Dockerfile bullet/row
- [x] PR text: verification numbers

### Review
The merge was mechanical apart from two points. Main's new tn-reth code used the `reth` umbrella
crate (4 paths and `reth_primitives`), which only a compile would catch; and the node-record-api
image has lacked `examples/` since #1416, found by running main's new context check against it.
Not run after the merge: test-epochs (11/11 before; the merge touched no epoch code), Docker
builds, GitHub runners.

## sls_reth_vendor2: fix the held-back issues (2026-11-06)

Branch sls_reth_vendor2 off sls_reth_vendor 5ded13999. User: go back through the held-back issues,
fix them, commit as I go. Standing rules: local commits only, no RocksDB, no tndb-b/Phase 3 work
(RocksDB stubs, MDBX fixes stay with the storage owners), no execution fork (fork checks for
anything on the execution/storage path), one focused commit per fix with its ledger entry.

Sources: vendor/reth/tn/notes/open-issues.md, upstream-backports.md (Open), PR text "Not fixed here".

Small, RPC/pool/tooling (no execution path):
- [x] CORS: shifted workers' HTTP servers get `--ws.origins` (9b0040f6e)
- [x] `trace_*` and `debug_executionWitness*` hold their permit inside the blocking task (TN-0038, 3dd57a700)
- [x] Backports TN-0039..0047: 6e4009eed, 6d718d0c2, bf1db83ae, 8861e2724, 62808bd06, IPC
      0e14f1a8a + a68f9e19f, 9580b7fc1, 329fcfc73 (cumulative gas), 5b0120dbb; 10d130182 and
      adfcad70c not applicable
- [x] 8fa539225 + c4cd5c9b7 + 080a9cfc1 (TN-0048)
- [x] Low: 33b785d97 (TN-0050), 44a74f9ea (TN-0051), 078c22d5f + 1a6ba945a + e7ebeb869 (TN-0052)
- [x] 13a59de4b + 621f663ed (+0973a1387, 34f77be92, 329fcfc73 check): TN-0049
- [x] `vendor.py ledger` (1afc3b2b3); CI runs its unit tests only (needs the upstream clone)
- [x] Upstream survey 84caec584..44a74f9ea recorded (30dceb2ef); 8572c88fe trie root bug: open issue, not applied (fork)
- [x] Nested lockfile pinned: tn/test-only.lock + tn/lockfile.py check (282cf4473); fixed libc drift
- [x] RPC gets its own blocking pool (tn-reth rpc_blocking.rs, e5bc58f7b)
Execution/storage path (fork checks, or keep held with the reason):
- [-] 5868ee478 and 8572c88fe (trie): not applied, need a coordinated upgrade (open-issues.md);
      1a6ba945a and 078c22d5f went in as TN-0052
Last, own commits (pool admission; user said its own PR, this branch can be it):
- [~] T2-1: branch sls_t2_1 (worktree .claude/worktrees/sls_reth_vendor): TN-0053 pool backport,
      flag off; TN floors need batch-builder owners' decisions (observers, release, workers)
Not doable here: CI timing on a GitHub runner; revm-inspectors retirement (needs a reth sync);
IPC hardening (next sync). Out of scope: RocksDB stubs, MDBX fixes (storage owners).
Verify at the end: fmt, clippy, test-default, test-adiri, test-vendor-reth (ci profile), gates,
test-restarts, test-epochs, test-upgrade vs 7167e7800, graph.py vs the branch base.

### Review (2026-11-07)
sls_reth_vendor2 at dc8edba8a: 25 commits on sls_reth_vendor 5ded13999. Fixed: CORS carry, TN-0038
(trace/witness permits), TN-0039..0052 (upstream backports incl. TN-0046 callBundle gas, TN-0048
pre-execution, TN-0049 cancellation), RPC's own blocking pool, vendor.py ledger, pinned test-only
deps (fixed libc drift 0.2.191 vs TN's 0.2.183). Found and recorded, not applied: upstream
8572c88fe, a state-root bug on TN's root path (deterministic; the fix needs a coordinated upgrade).
T2-1: pool half on sls_t2_1 (b142831bb, TN-0053, flag off, txpool 235/235, its new test fails
without keep_sender_info, clippy clean); TN half waits on batch-builder owners. Final run all green:
fmt, clippy x2, nested clippy, gates, ledger, graph identical to base, default 3861/3861, adiri
1951/1951, vendor lane (16+7, lockfile check, 1560, 38, 51, 474), test-upgrade vs 7167e7800,
restarts 10/10, epochs 11/11. The endpoint scanner stalled new executables for ~1h mid-session.
Not run: Docker builds, GitHub runners. Nothing pushed.

## 2026-11-07: full review of vendored reth (report only, no fixes)
- [x] Fan out 7 read-only reviewers: RPC eth core; RPC debug/trace/builder/ipc; transaction pool;
      execution (evm/revm/chain-state); storage + trie; TN diff integrity (ledger vs edits,
      tooling, manifests); support crates (primitives, chainspec, node-core, tasks).
- [x] Verify every high/critical finding (findings-verifier), check upstream for existing fixes.
- [x] Write tasks/review-vendor-reth-2026-11-08.md: findings ranked, open issues carried forward
      (trie 8572c88fe/5868ee478, T2-1 TN half on sls_t2_1, MDBX/RocksDB stubs, revm-inspectors
      patch, CI timing), next survey point 44a74f9ea.
- Review (2026-11-08): 30 action items (1 high, 6 medium, 23 low) plus info; 0 false positives
  in verification; no execution/storage divergence found; open issues O1-O11 carried in the
  report. Rate limit hit mid-run; all 7 reviewers resumed from checkpoints (lessons.md pattern).

## 2026-11-09: fix the review's findings (user: "Assume eth is public and start work on fixing the issues")

Order from the report's "Suggested order of work", with `eth` public. One commit per item,
ledgered as TN-0054 onward (TN-0053 is T2-1 on `sls_t2_1`). Local commits only; no push.
Constraints unchanged: no execution-path behaviour change, no RocksDB, no tndb-b work.

### Step 1: `eth` public (done: TN-0054 to TN-0058, commits 10214f712..56ccc0ded)
- [x] R1 `eth_callBundle`: cap the bundle count, decode and recover inside the blocking task
- [x] R27 JSON batch limit on HTTP, WS and IPC
- [x] R2 cap on active filters and on pool pending/new-tx listeners
- [x] R6 `eth_callMany` (and the trace/debug variants): call count cap and cumulative gas budget
- [x] R5 port the rest of 055fafc72 (per-window header collection in `eth_getLogs`)

### Step 2: cheap upstream picks and TN-side edges
- [x] R8 d46664fc8, R9 d8d704cf7, R13 f12d52de2, R20 0c8b1c187, R21 fe01db183 + 0a1d3e761,
      R22 dfcb10454, R24 5b6863039, R26 (074bf32ef, 8760eb000, e3d7f0952, 3dd806a43, 4e1a5574e,
      6d117042f, 5d6634a6f) with doc page updates
- [x] R14 range parsers on permit flags; R18 `checked_sub` on instance ports; R19 IPC path;
      R23 pending→latest in TN's simulate/callMany wrappers (commits e8304133e, c2c69b1b0;
      R19 refuses a live socket rather than moving the default into the datadir)
- picks done: R8 d46664fc8 (TN-0059), R13 f12d52de2 (TN-0060), R24 5b6863039 (TN-0062),
  R22 dfcb10454 (TN-0063), R26 48df75d88 (TN-0064), 5d6634a6f (TN-0065), 8760eb000 (TN-0066),
  3dd806a43 (TN-0067), e3d7f0952 (TN-0068), R9 d8d704cf7 (TN-0069), 074bf32ef (TN-0070),
  6d117042f (TN-0071, adapted to revm-inspectors 0.34), 4e1a5574e (TN-0072),
  R20 0c8b1c187 (TN-0073), R21 0a1d3e761 (TN-0074) + fe01db183 (TN-0075), 43a7452b0
  (TN-0076, getLogs retry range)
- picks left: none

### Step 3: `debug` and `trace` hardening
- [x] R3 raw-block `debug_traceBlock` gas caps; R4 step budget in struct logger / vmTrace;
      R10 `trace_filter` fan-out (TN-0077 R10, TN-0078 R3, TN-0079 R4; plus TN-0080: the nested
      test workspace had never built against the carried revm-inspectors, since cargo does not
      forward `--config` to cargo-nextest; the patch now lives in `vendor/reth/.cargo/config.toml`)
- plan (2026-11-09, done as above):
  - R10 (TN-0077, `trace.rs`): `trace_filter` traces at most as many blocks at once as the
    permits it holds: the `try_join_all` over a chunk becomes `stream::iter(..).buffered(2)`
    (order kept), with one `TRACE_FILTER_PERMITS` constant for `acquire_many_owned` and the
    buffer. Docs: `trace/README.md` line 7, `enabling-namespaces.md` row 58.
  - R3 (TN-0078, `debug.rs`): `debug_trace_raw_block` treats the raw block as a callMany-shaped
    request: `check_call_count(call_gas_limit(), tx_count)` right after the RLP decode (bounds
    the inline signer recovery too), then a `CallManyBudget` passed into `trace_block` as
    `Option<_>`; before each tx `budget.check(tx.gas_limit())` (new method; the block rule from
    TN-0046), after it `budget.charge(gas_used)`. Stored-block paths pass `None`. No header
    refusal needed once the budget holds. The in-transaction cancel is not added: the budget
    bounds a request to `--rpc.gascap` gas. Docs: `debug/debug_traceblock.md`.
  - R4 (TN-0079, `patches/revm-inspectors` + PATCH.md): `TracingInspector` counts the bytes its
    steps record (stack words, newly copied memory, a fixed per-step overhead) and halts the
    transaction with `ContextError::Custom("trace too large ...")` past
    `TracingInspectorConfig::max_recorded_bytes` (default 160 MiB, reth's default response
    limit: a trace above it could not be returned anyway). Surfaces as -32603 `EvmCustom`.
    All struct-logger and vmTrace sites get it with no call-site change; PATCH.md gains a second
    carried change and its retire note. Docs: struct logger pages say the cap, `debug/README.md`.

### Step 4: pool
- [x] R7 nonce-continuity in the batch builder (TN side: `WorkerTxPool::best_transactions` calls
      `no_updates()`, so a build packs the pending set as of its start); R11 + the two T2-1 notes
      recorded under O3 in `vendor/reth/tn/notes/open-issues.md`

### Step 5: trie and storage (fork checks)
- [x] R15 8f5efd2b3 (TN-0081, trie walker errors propagate), R12 (TN-0082, missing bytecode row
      fails the lookup), R17 (TN-0083, torn first launch resumes), R16 (TN side: committed tip
      reads use the `Finish` checkpoint). `graph.py diff` before/after step 5: 0 package or
      feature changes. `make test-upgrade` against v0.16.0-adiri: see the review section below.

### Step 6: tooling and docs
- [x] R28 (TN-0084: the ledger check reads only *Files* sentences; directories need a trailing
      slash), R29 (the attested run runs `vendor.py status|ledger|check`), R30 (TN-0085:
      `lockfile.py check` fails when either workspace resolves revm-inspectors from the registry),
      the info-list doc drift (CLAUDE.md numbers, ROADMAP, README `tn/` listing, usage-map counts,
      TN-0019/0036/0038 supersession notes, TN-0049 omission note) and the PR text corrections.
      Found on the way: TN-0080 (the nested test lane never built against the carried patch).

### Review (2026-11-10)
- Commits on `sls_reth_vendor2`, local only: TN-0066 to TN-0085 plus the TN-side commits (R7
  `no_updates`, R16 `Finish`-checkpoint tip reads, the TN-0064 namespace test). One commit per
  item; shared ledger files carved per commit.
- Fork checks for the execution/storage-path changes (TN-0081, TN-0082, TN-0083):
  `graph.py diff` before/after: 0 packages left, 0 feature changes (both targets, run and build
  units); `make test-upgrade` with v0.16.0-adiri as the previous binary: `test_upgrade_rolling_in_place`
  passes (every node agrees on block hash, stateRoot and receiptsRoot).
- Suites: nested `reth-rpc`, `reth-rpc-eth-api`, `reth-rpc-eth-types`, `reth-transaction-pool`
  (validate), `reth-trie*` (213), `reth-revm`, `reth-db-common` all pass; `cargo.sh check
  --workspace --all-targets` clean; TN `tn-reth`, `tn-batch-builder`, `telcoin-network-cli`: 636
  passed. The patch crate's own tests (18) pass.
- Gates: `vendor.py ledger` (155 files), `vendor.py check`, `lockfile.py check` (now also proves
  the patch is resolved), `etc/rocksdb-gate.sh`, `etc/ci-lanes.sh fmt` all pass.
- Lanes, after a `syspolicyd` restart: `etc/ci-lanes.sh test-vendor-reth` passes (lockfile check,
  nested workspace 1600 tests passed and 1 skipped, reth-db 38, 73, and tn-reth 478); nested clippy
  `--workspace --lib --tests -D warnings` clean; `etc/ci-lanes.sh clippy` clean. The first attempt
  had not finished: the lane rebuilt the nested test profile in 104 min, then nextest's listing
  step hung: ten test binaries sat 6 to 8 hours at 0% CPU inside dyld, blocked in the `fcntl`
  that registers `libstd-*.dylib`'s code signature, and `syspolicyd` was in uninterruptible wait
  with no new log lines. A fresh exec of a never-run test binary still blocks after the run was
  killed, while already-assessed binaries (cargo, git, python) run. This is the macOS
  first-launch assessment (SentinelOne/syspolicyd) wedged on the machine, not a test; it needed
  `syspolicyd` restarted (`sudo killall -9 syspolicyd`), after which the lanes ran normally. The nested clippy stall earlier
  in the session (drivers asleep at 0% CPU) was most likely the same thing.
- Open issues O1 to O11 are unchanged except: O3 gained R11 and the two T2-1 notes; O6 now
  reads TN-0079/TN-0080/TN-0085; O10's `cargo.sh --config` line is now `.cargo/config.toml`.
  `tasks/pr-sls_reth_vendor2.md` (untracked) was corrected but needs the new items added before
  the PR text is reused.
- Previous-binary build needs `GIT_CONFIG_COUNT=2` with `url.<repo>/.git/modules/tn-contracts.insteadOf`
  and `protocol.file.allow=always` in an agent session (no SSH hardware key).

### Hand-off (2026-11-10, end of the review-fix work)
- `sls_reth_vendor2` is at b4a6e6028: all review fixes (TN-0054 to TN-0085 plus the TN-side
  commits), the lane results, and the merge of main 9aa1d6139. Local only, not pushed; the user
  pushes and opens the PR. `tasks/pr-sls_reth_vendor2.md` (untracked) is the PR text draft.
- main squash-merged `sls_reth_vendor` as c2fffa5f2, so a merge of main conflicts on every
  vendored file. Resolve against the fork point 5ded13999: ours for files main did not change
  after the squash, `git merge-file` against 5ded13999 for the rest (the merge commit says so).
- Verified on the merged tree: root and nested `cargo check --workspace --all-targets`, the
  vendor/lockfile/rocksdb/fmt gates, tn-reth and tn-batch-builder (509 tests). Not rerun after
  the merge: the full nested test lane, the clippy lanes, `make test-upgrade`.
- Still separate: T2-1 pool half on `sls_t2_1`; open issues O1 to O11 in
  `vendor/reth/tn/notes/open-issues.md`.

