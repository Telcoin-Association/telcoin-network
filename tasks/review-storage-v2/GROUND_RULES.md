# Ground rules for the storage-v2 review (2026-11-12)

Target: branch `sls_storage_v2_3`, HEAD in `tasks/review-storage-v2/HEAD.txt` (first line); the
merge base with `origin/main` is the second line. The scope is `git diff <merge-base>...HEAD`
(320 files, +38,630 / -3,915; the file list is `tasks/review-storage-v2/files.txt`). Every
`file:line` you cite must be at HEAD. "Main" means `origin/main`; never use local `main`.
Two commits are on `origin/main` and not here (41c31f06c, d44ae235f: a fork merge); ignore them.

## What the branch does (orientation)
- tn-lmdb becomes the default reth backend (MDBX opt-in, feature `mdbx`); the engine gained gap
  fill (write coalescing, with an `auto` width chooser), a boot record that decides when an open
  drops the file's cached pages, a verified weak-head open, format versions 7 and 8 (tree tags on
  pages and overflow runs), and a cleanup pass of review Lows (`tn_lmdb_review.md`, repo root).
- reth storage layout v2 without RocksDB: change sets and senders in static files with per-value
  CRC32 (NippyJar format 2, `.csoff` records), history indices and the transaction-hash index
  kept in the database, plain slot keys; one binary opens v1 and v2; a layout marker
  (`TN_LAYOUT_VERSION`) and format-freeze fixtures; `db reset-execution`; MDBX to tn-lmdb
  migration hardening; `db migrate-execution`.
- Execution: a cross-block execution cache under block building (`exec_cache.rs`); a block's
  state root with storage roots on parallel tasks, the account walk and hot storage tries split
  by first nibble (`output_overlay/{parallel,account_split,storage_split}.rs`, vendored hook
  TN-0139), a sampled serial check every 256th block; the upstream trie seek fix TN-0140.
- Node: a replayed epoch's stored record must match the closing block the node executed
  (`crates/node/src/manager/node/close_epoch.rs`); bounded `get_missing_consensus`; resync and
  upgrade e2e tests.
- The vendored-reth review fixes of `sls_reth_vendor2` (PR #1829, TN-0036..0085: RPC bounds and
  permits, pool backports, IPC) are in this diff too because that PR is not on main yet. They
  were reviewed before (`tasks/review-vendor-reth-2026-11-08.md`, `tasks/pr-sls_reth_vendor2.md`).

## Read first
1. `tasks/review-storage-v2/GROUND_RULES.md` (this file) and your brief.
2. The crate guides of your area: `crates/tn-lmdb/CLAUDE.md`, `crates/tn-lmdb-reth/CLAUDE.md`,
   `vendor/reth/CLAUDE.md`, `vendor/reth/DECISIONS.md`; `crates/tn-reth/README.md` (the
   sections your brief names); `vendor/reth/CHANGES.md` entries your brief names (each entry
   states *Files* and *Why*: check the code does what the entry says).
3. The do-not-flag lists: `crates/tn-lmdb/README.md` "Intentional design decisions: please do
   not flag these"; `crates/tn-lmdb-reth/src/lib.rs` "differences from MDBX"; `vendor/reth/DECISIONS.md`.

## Already decided; do not re-flag
- Everything in those do-not-flag lists. The tn-lmdb review's open decisions (M2 stamped-page
  verification, M7 latch scope, M10/M11 bench acceptance, L52 format freeze, S1/S2 two engines)
  and its fixed items (`tn_lmdb_review.md`, repo root): a repeat needs new evidence.
- The vendored-reth review's findings R1-R30 and their fixes (`tasks/review-vendor-reth-2026-11-08.md`,
  `tasks/pr-sls_reth_vendor2.md`): verify the fixes landed as described, do not re-derive them.
- MDBX parity is not a goal: a difference from MDBX that no caller depends on and that the docs
  state is not a finding.
- RocksDB is removed on purpose (`etc/rocksdb-gate.sh`); `unsafe` blocks that carry a `SAFETY:`
  comment (check the comment's claim holds; do not flag the `unsafe`); `eyre` in the node;
  fail-fast `expect` in DB-open startup paths that the docs call fail-closed.

## How to work
- Read the code before you write about it. Trace every claim to lines. Speculation is a false
  positive and costs a verifier's time. Read changed files in full, not only the hunks.
- You may run read-only commands and existing targeted tests
  (`cargo nextest run -p <crate> -E 'test(=<path>)'`; a shared build lock may make the first one
  wait; never run a whole crate's suite, the coordinator ran the baseline). Record every test you
  ran and its result. **Do not edit any repo file** and do not create files outside
  `tasks/review-storage-v2/`.
- Before claiming a path is untested, grep the crate's tests for it.
- Do not spend effort on style. Correctness, durability, determinism (two validators on one
  release must compute the same block from one input), concurrency, fail-closed behaviour and the
  upgrade path are the work. Docs that promise what the code does not do are findings (Low or
  Informational).

## Checkpointing (required)
Write your output file `tasks/review-storage-v2/<track>.md` early, and rewrite it after every
major step. Start it with a `## Status` line naming what is done and what is next, so a resumed
agent continues from the file.

## Output format
```
# <track> raw findings — storage-v2 review
Target: HEAD <sha>
## Status
<done / next>
## Reviewed
<files with line ranges read in full; supporting files with ranges>
## Tests run
<command → result>
## Findings
### <TRACK>-<n>. <Title>
- **Severity (initial)**: Critical / High / Medium / Low / Informational
- **Category**: Determinism / State & Durability / Concurrency / Error Handling / Bugs /
  Upgrade path / Security (DoS, input validation) / Architecture / Test coverage / Docs
- **Location**: `file:line` (several allowed)
- **Claim**: one standalone factual statement of what is wrong (no reasoning chain)
- **Failure scenario**: concrete inputs or interleaving → the wrong outcome
- **Key Question**: the one thing a verifier must answer
- **PoC test**: setup, fault/mutation, assertion (existing hooks where possible)
- **Proposed fix**: the smallest change that removes the root cause, and the hot path it touches
- **Relevant Files**: files a verifier must read
- **Source**: <track>
## Considered and rejected
<each suspect you traced and why it holds, with lines; as valuable as a finding>
## Coverage gaps
<behaviours no test pins, with the test that would>
## Ledger check (vendored tracks only)
<each TN-NNNN entry in scope: matches the code / does not, with lines>
```

Severity (a blockchain node; consensus bugs are safety bugs):
- Critical: two nodes on one release diverging from one input; loss of acknowledged, durable
  data; a state that survives restarts wrongly; funds at risk.
- High: a panic or stop on a production path that is not a deliberate fail-stop; a durability
  ack the disk does not back; a recovery that cuts acknowledged data; an upgrade step that
  leaves a datadir no binary opens.
- Medium: a silent wrong answer under a specific condition; a fail-closed rule with a gap; a
  lock-order, cancellation or race hazard with a real interleaving; a bound an unauthenticated
  client can exceed.
- Low: a practically-safe-but-unproven path, weak diagnostics, a missing test for an invariant a
  README claims.
- Informational: docs out of step with code, naming, dead code.
