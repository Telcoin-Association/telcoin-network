# vendor/reth: a guide for agents

TN builds reth from this directory: upstream reth v1.11.4 (`tn/upstream.toml`), cut down to the 57
crates TN uses plus the test-only `reth-testing-utils`, with TN's changes on top. It is a nested
cargo workspace that TN's root manifest reaches by path dependencies (README.md, "How TN builds
against it").

**Size: don't read the copy to understand TN's work.** `crates/` and `testing/` hold about 199k
lines of Rust, almost all of it upstream code TN never changed. TN's whole change set is
`vendor.py diff`: 110 files, about 14k lines removed and 2.5k added, most of the removals whole
files. Start from that diff or from a ledger entry, never from a directory listing.

**Read in this order:**
1. this file;
2. README.md, the section for your task (building, testing, changing vendored code, taking
   upstream changes, checking the feature graph);
3. the CHANGES.md entry for the `TN-NNNN` id you meet;
4. DECISIONS.md (D1 to D21) only when you need the reason behind a rule.

**Skip:** `Cargo.lock` (both), `patches/revm-inspectors/src/` (registry source; its
`upstream.diff` is the whole change), and any file under `crates/` that `vendor.py status` does
not list.

## Where things are

| question | where |
|---|---|
| what does TN change against upstream? | `python3 -I vendor/reth/tn/vendor.py status` (file list), `vendor.py diff <path>...` (paths relative to `vendor/reth`, e.g. `crates/rpc/rpc/src/debug.rs`) |
| why does this line differ? | the `TN(TN-NNNN):` comment at the edit, then `rg -n 'TN-NNNN' vendor/reth/CHANGES.md` |
| every TN edit site | `rg -n 'TN\(TN-' vendor/reth/crates patches` |
| which reth crates are carried | `tn/crates.txt` (one upstream path per line) |
| how TN uses each crate, with file:line | `tn/notes/usage-map.md` (a snapshot; the line numbers drift) |
| upstream fixes applied, open and skipped | `tn/notes/upstream-backports.md` |
| known issues this work leaves open | `tn/notes/open-issues.md` |
| what comes next | ROADMAP.md |
| the TN side of the boundary | `crates/tn-reth` (README.md; `js-tracer.md` for the opt-in `js-tracer` feature) |
| the carried revm-inspectors fix | `patches/revm-inspectors/PATCH.md` |
| CI for this code | `etc/ci-lanes.sh test-vendor-reth`; `.github/workflows/pr.yaml` jobs `vendor-reth-test` and `revm-inspectors-patch` |
| the RocksDB ban | `etc/rocksdb-gate.sh` (DECISIONS.md D14) |
| how reth's test-only dependencies are pinned | `tn/test-only.lock`, `tn/lockfile.py` (DECISIONS.md D7) |

TN code outside `crates/tn-reth` touches reth in a few places only: `tn-types` (primitives,
`ChainSpec`, reth's `TaskSpawner` for TN's task manager), `tn-config`, `tn-exex`, `tn-storage`
(`reth-libmdbx`), and `reth-metrics` derives in several crates (usage-map.md has the list).

## TN's edits by area

| area | ledger ids |
|---|---|
| crates removed or cut down | TN-0005, TN-0006, TN-0008, TN-0009, TN-0011, TN-0012, TN-0028 to TN-0032 |
| RPC surface removed (namespaces TN never serves) | TN-0010, TN-0021, TN-0033 |
| build only (deps, build scripts, features, formatting) | TN-0002, TN-0007, TN-0020, TN-0025 to TN-0027 |
| tests only | TN-0001, TN-0003, TN-0004 |
| RocksDB removed; storage v1 as the default | TN-0014 |
| upstream sync to v1.11.4 | TN-0013 |
| RPC security backports | TN-0015 to TN-0017, TN-0019, TN-0034, TN-0035, TN-0058, TN-0076 |
| RPC limits TN adds beyond upstream | TN-0036 to TN-0038, TN-0054 to TN-0057, TN-0061 |
| transaction pool backports | TN-0018, TN-0022, TN-0024, TN-0073 to TN-0075 |
| RPC and CLI fixes backported after v1.11.4 (review R8, R9, R13, R22, R24, R26) | TN-0059, TN-0060, TN-0062 to TN-0072 |
| static files and nippy-jar backports | TN-0023 |

CHANGES.md is ordered by section, then by id, newest last within a section.

## Fork safety

A public testnet runs these binaries. A node that executes a block, computes a root or writes a
datadir differently from the release it upgrades from splits from the network. Treat these as the
execution and storage path:
- vendored: `crates/evm/*`, `crates/ethereum/{evm,primitives,hardforks}`, `crates/revm`,
  `crates/trie/*`, `crates/storage/*` (`db-common` holds genesis init), `crates/static-file/types`,
  `crates/chainspec`, `crates/primitives-traits`, `crates/chain-state`;
- TN: `crates/tn-reth/src/evm/`, `env/execution.rs`, `env/genesis.rs`, `system_calls.rs`;
- third party: any version or feature change to revm, alloy, k256, secp256k1, blst or c-kzg.

Invariants that must hold:
- `StorageSettings::base()` returns v1 (TN-0014). Upstream main returns v2. The test
  `test_new_datadir_uses_storage_v1` (`crates/tn-reth/src/env/mod.rs`) fails if a sync takes
  upstream's line.
- Removing a crate or an edge must not switch off a feature something else relied on (D10).
- No RocksDB, in any form, anywhere in the lockfile (D14).

Before merging a change on that path:
1. `graph.py snapshot` before and after, then `graph.py diff` (README.md, "Checking the feature
   graph"). Execution crates' features must not move.
2. Build the release the testnet runs as the previous binary and run the mixed-binary test:
   `make build-e2e-bin-prev TN_PREV_REF=<release>`, then `make test-upgrade`. It upgrades
   validators in place one at a time, re-executes the chain from genesis on each binary, rolls one
   back, and requires the same block hash, `stateRoot` and `receiptsRoot` on every node.
3. Say in the ledger entry whether the change can reach execution or storage, and why not.

RPC, tracing and pool changes do not change block validity. Pool admission decides what this
validator proposes, but every validator executes the same batches the same way.

## Commands

```
python3 -I vendor/reth/tn/vendor.py status | check | ledger | diff [path...]
vendor/reth/tn/cargo.sh check --workspace --all-targets      # never -p alone (D18)
vendor/reth/tn/cargo.sh nextest run -p reth-rpc               # add --features js-tracer for JS tracers
vendor/reth/tn/cargo.sh nextest run -p reth-db --test-threads 2   # macOS MDBX ENOMEM otherwise
vendor/reth/tn/cargo.sh clippy --workspace --lib --tests -- -D warnings
etc/ci-lanes.sh test-vendor-reth                              # what CI runs for this code
etc/rocksdb-gate.sh
```

`cargo.sh` runs cargo inside the nested workspace with the revm-inspectors patch applied and
builds into `target/vendor-reth`; never run a bare `cargo` in `vendor/reth`. nextest there reads
`vendor/reth/.config/nextest.toml`, not the root's. TN's own `cargo` commands build the vendored crates as path
dependencies but never run their tests, and TN's clippy lane skips them.

## Changing vendored code

README.md, "Changing vendored code", has the procedure. In short: one focused commit, a CHANGES.md
entry, a `TN(TN-NNNN):` comment where the edit would surprise a reader of reth's code, no
cosmetic edits (each one conflicts at every sync), and the fork checks above when the change
touches the execution or storage path. `vendor.py ledger` checks the ledger against `status`:
name each changed file in an entry, from `vendor/reth` or from a crate the entry named before it.

## Reviewing this code

Review TN's diff, not the copy. A cheap integrity pass:
1. `vendor.py ledger` passes: every file `status` lists is named by an entry, and every
   `TN(TN-NNNN)` id in the code has one.
2. Every entry's files appear in `status` (by eye; an entry can name a file it later restored).
3. `vendor.py check` and `etc/rocksdb-gate.sh` pass.
4. `git diff main -- Cargo.lock` adds no package and changes no third-party version.

Then read `vendor.py diff` area by area, with the ledger entry beside it.

These are deliberate; don't report them:
- The RocksDB stubs (`providers/rocksdb_stub.rs`, `traits/rocksdb_provider.rs`) and the
  storage-v2 plumbing stay until the storage work replaces them (D15, ROADMAP "RocksDB step B").
- MDBX stays compiled (D9). Its upstream fixes are tracked for the tndb-b owners.
- reth's code style, lints and rustfmt config apply here, not TN's.
- The nested `Cargo.lock` is generated and git-ignored (D7); `tn/test-only.lock` pins what only
  reth's tests need, and `tn/lockfile.py check` guards both.
- Tests that needed a removed crate were dropped with it (D11).
- `cargo.sh check -p <crate>` fails for some crates; build with `--workspace` (D18).
- Carried crates still have code TN doesn't call; trimming it is ROADMAP work (D12, D20).
- Problems in upstream code TN didn't touch belong in the backport tracker, not in this copy.
- Items in `tn/notes/open-issues.md` are known and handled outside this copy's PRs.

## Tracking upstream reth

The upstream clone is `$RETH_UPSTREAM`, or `~/.cache/tn-vendor/reth`, which `vendor.py` clones
on first use. The last survey of upstream `main` stopped at `44a74f9ea` (2026-11-06); the tracker
records it. To survey what landed since, over the carried paths only:

```
U=${RETH_UPSTREAM:-$HOME/.cache/tn-vendor/reth}
git -C "$U" fetch origin
git -C "$U" log --no-merges --oneline 44a74f9ea..origin/main -- \
  $(grep -v '^#' vendor/reth/tn/crates.txt | awk 'NF{print $1}')
```

Triage by subject first (fix, security, panic, overflow, DoS, limit), then `git show --stat`, and
read diffs only for candidates. Skip files TN deleted (the `deleted` lines of `vendor.py status`).
Record each verdict and the new survey point in `tn/notes/upstream-backports.md`, and apply a fix
with `vendor.py pick <sha>`. A release bump is `vendor.py sync <tag>` (README.md). Watch
revm-inspectors releases too: `patches/revm-inspectors/PATCH.md` says when that patch retires.

## Tracking TN main

- Any TN commit that edits `vendor/reth` needs its ledger entry in the same PR. The vendored
  tests run only in the `test-vendor-reth` lane, so run it (or the crate's tests through
  `cargo.sh`) before pushing.
- `main` squash-merges PRs. The branch commits and the local tags README.md mentions
  (`reth-vendor-import-v1.11.3` and the others) do not reach `main`. The durable record is
  `vendor.py diff` against the pin plus the ledger.
- A branch cut from `main` before the vendoring meets conflicts in the root `Cargo.toml` (the
  `reth-*` workspace dependencies moved from the git tag to `vendor/reth` paths), in
  `Cargo.lock`, and in `crates/tn-reth` imports (it uses reth's crates directly, not the `reth`
  umbrella crate). Take `main`'s side for the manifest and lockfile, then re-add only the branch's
  own dependencies. Afterwards `git diff main -- Cargo.lock` should show no third-party version
  change.
