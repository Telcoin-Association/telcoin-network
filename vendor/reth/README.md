# vendor/reth: TN's copy of reth

Telcoin Network's execution layer is built on [reth](https://github.com/paradigmxyz/reth). This
directory holds the reth crates TN uses, as source, so TN can change them. Upstream syncs stay
possible but optional.

- Base: imported from upstream `v1.11.3` (`d6324d63e27e`), synced to `v1.11.4`
  (`2ac58a25f561`). The current pin is always in `tn/upstream.toml`.
- History: the local tags of the vendoring work (`reth-vendor-import-v1.11.3`,
  `reth-vendor-v1.11.3`, `reth-reduction-start`, `reth-reduction-2-start`; DECISIONS.md D8) are
  not in `main`'s history. The durable record is `vendor.py diff` against the pin, plus
  CHANGES.md.
- tn-lmdb (called tndb-b until 2026-11-04): the engine that replaces MDBX as reth's database
  (`crates/tn-lmdb`, through the adapter `crates/tn-lmdb-reth`). It is TN's default reth backend
  since 2026-11-09; MDBX is the opt-in `mdbx` feature of `tn-reth`, kept until testnet has
  resynced (DECISIONS.md D22). It is not `tndb`, TN's consensus database in `crates/storage`.
- Carried: the reth crates in TN's dependency graph (57 after both reduction passes; 101 at
  import), plus the test-only `reth-testing-utils`. The list is in `tn/crates.txt`.
- TN's changes against upstream: CHANGES.md, the ledger.
- Why it is built this way: DECISIONS.md.
- What comes next (retiring MDBX, trimming Ethereum-only code, alloy/revm directly): ROADMAP.md.
- Known issues left for separate changes: `tn/notes/open-issues.md`.
- Agents and reviewers: CLAUDE.md is the short path through all of this (read order, where
  things are, fork-safety checks, tracking upstream).

## Layout

```
vendor/reth/
  Cargo.toml        nested workspace: upstream's root manifest, filtered to the vendored crates
  rustfmt.toml      upstream's, so `cargo fmt` formats reth code reth's way
  clippy.toml       upstream's
  LICENSE-*         upstream's (MIT OR Apache-2.0)
  crates/...        upstream paths, e.g. crates/storage/db is upstream's crates/storage/db
  testing/...       testing/testing-utils (dev-only)
  tn/               TN tooling: vendor.py, graph.py, lockfile.py, cargo.sh, crates.txt,
                    upstream.toml, test-only.lock, and the tools' tests (test_*.py)
  tn/notes/         the usage map, the upstream-backport tracker and the open issues
  .config/          TN's nextest config for the nested workspace (upstream's is not carried)
  README.md CHANGES.md DECISIONS.md ROADMAP.md CLAUDE.md
                    TN's docs (upstream's README and CLAUDE.md are not carried)
```

Everything outside `tn/`, `.config/` and the five TN docs mirrors an upstream path. Crate names are
unchanged (`reth-db`, `reth-provider`, ...), so TN code imports `reth_db::...` exactly as before.

## How TN builds against it

The root `Cargo.toml` points each `reth-*` workspace dependency at its directory here
(`reth-db = { path = "vendor/reth/crates/storage/db" }`) and lists `vendor/reth` under
`exclude`. `vendor/reth` is a workspace of its own. Its crates inherit reth's
`[workspace.package]`, `[workspace.dependencies]` and `[workspace.lints]` from
`vendor/reth/Cargo.toml`, and TN's house lints do not apply to them. Third-party versions come
from TN's single root `Cargo.lock`.

Things that differ from the git dependency this replaced:

- Path dependencies are not lint-capped. CI's `RUSTFLAGS="-D warnings -D unused_extern_crates"`
  applies to this code, so a change here has to compile warning-free.
- `cargo fmt --all` formats path dependencies too, and CI checks it that way. rustfmt picks up
  `vendor/reth/rustfmt.toml`, so this code stays formatted reth's way. `make fmt` passes
  `--all` for the same reason (bare `cargo fmt` would skip the vendored crates).
- Clippy (`etc/ci-lanes.sh clippy`) lints workspace members only, so not this code.
- Cargo fingerprints path dependencies by file mtime. A fresh CI checkout therefore rebuilds the
  vendored crates even when the dependency cache is warm (they were cached by git revision
  before).

## Changing vendored code

1. Make the change in a focused commit. Mixing a vendored edit with unrelated TN work makes
   later syncs harder to reason about.
2. Add an entry to CHANGES.md: an id (`TN-NNNN`), the files, what changed and why, and whether
   it is worth offering upstream. CHANGES.md is the ledger. `vendor.py status` must list only
   files that some entry accounts for, which `vendor.py ledger` checks.
3. Where an edit would surprise a reader of reth's code, leave a short comment that starts with
   `TN(TN-NNNN):`. Leave out cosmetic changes. They make every upstream sync conflict for
   nothing.
4. Build and test: `make check`, the TN tests that touch the area, and reth's own tests for the
   crate (`vendor/reth/tn/cargo.sh test -p <crate>`). For a change on the execution or storage
   path, also run the fork checks in CLAUDE.md ("Fork safety"), which end with
   `make test-upgrade`.

Run `python3 -I vendor/reth/tn/vendor.py diff [PATH...]` (paths relative to `vendor/reth`) to see
the full diff of TN's changes against the pinned upstream. `status` lists the changed files.

## Running reth's own tests

reth's unit and integration tests only resolve inside the nested workspace (their
dev-dependencies are reth's). CI runs them in the `test-vendor-reth` lane
(`etc/ci-lanes.sh test-vendor-reth`, job `vendor-reth-test`), with and without `js-tracer`. By
hand:

```
vendor/reth/tn/cargo.sh test -p reth-db --lib
vendor/reth/tn/cargo.sh nextest run -p reth-provider
vendor/reth/tn/cargo.sh check --workspace --all-targets   # every vendored crate's tests compile
```

On macOS, `reth-db --lib` fails a few MDBX tests at the default thread count with `Cannot
allocate memory` (ENOMEM when opening the environment): each test reserves a large memory map,
and with 16 running at once some opens fail. The failing set changes from run to run, and all
pass with `-- --test-threads=2`. This predates every TN change to the crate.

Some crates' test code needs their own `test-utils` feature, which upstream's CI only gets from
workspace-wide `--all-features`. If `--all-targets` fails on a missing optional dependency, add
`--all-features`. (At import this hit `reth-downloaders`, which is no longer carried.)

Check and lint with `--workspace`. `cargo.sh check -p <crate>` on its own fails for several
crates that rely on features other members enable (DECISIONS.md D18), and so does `-p` with
`clippy`. Clippy does run on the vendored crates here, unlike in TN's build, where path
dependencies never get clippy, so run it after editing vendored code:
`vendor/reth/tn/cargo.sh clippy --workspace --lib --tests -- -D warnings`.

Run cargo in the nested workspace only through `cargo.sh`. A bare `cargo` there (or an editor's
rust-analyzer opened on `vendor/reth`) gets the revm-inspectors patch from `.cargo/config.toml`
(TN-0080), but not `cargo.sh`'s seeding of the nested lockfile from TN's: it may resolve versions
TN does not ship and write them into the nested lockfile (the next `cargo.sh` run overwrites it),
and it builds into `vendor/reth/target`, not `target/vendor-reth`.

`cargo.sh` seeds `vendor/reth/Cargo.lock` (git-ignored) on every run from TN's lockfile plus
`tn/test-only.lock`, which pins the 72 packages only reth's tests need, and builds into
`target/vendor-reth`. TN's own lockfile is never touched. TN's root manifest patches
revm-inspectors (`patches/revm-inspectors/PATCH.md`), and `vendor/reth/.cargo/config.toml` carries
the same patch (TN-0080), so reth's tests run against the code TN ships.

`python3 -I vendor/reth/tn/lockfile.py check`, which the `test-vendor-reth` lane runs, fails when
the nested workspace resolves a package neither file pins, or when the vendored crates' production
dependencies resolve a version TN does not ship. A test-only package can cause the second by
needing a newer shared crate: codspeed's nix 0.31.3 needs libc 0.2.186, so nix is pinned at 0.31.2
and libc stays at TN's 0.2.183. `cargo_metadata` still differs (0.19.2 against TN's 0.18.1), but
only reth's tests use it. When the check fails, `lockfile.py update` re-resolves and rewrites
`tn/test-only.lock`; pin a package lower first with `cargo.sh update -p <pkg>@<version> --precise
<older>`, and run `update` after each pin. `cargo.sh` sets `resolver.incompatible-rust-versions =
"fallback"`, so new resolutions stay at versions that support the nested workspace's
`rust-version` (1.88). nextest reads `vendor/reth/.config/nextest.toml` here (TN's, with the same
`default` and `ci` profiles as the root's), not the root's.

## Taking changes from upstream

Everything goes through `vendor/reth/tn/vendor.py`. It needs an upstream clone: set
`RETH_UPSTREAM=/path/to/reth`, or let it clone a blobless copy into `~/.cache/tn-vendor/reth`
on first use. Each command that brings upstream code in refuses to run while `vendor/reth` has
uncommitted changes, so its result can be reviewed and reverted on its own.

The engine is the same for all of them. For every file in scope it runs a 3-way merge with
base = upstream at the old point, theirs = upstream at the new point, ours = the file here.
A file TN never touched simply becomes upstream's new version. A file TN changed is merged with
`git merge-file`; where both sides changed the same lines, it leaves diff3-style conflict
markers (`<<<<<<< TN`, `||||||| upstream <old>`, `>>>>>>> upstream <new>`). A file upstream
renamed is merged at its new path against the old one (git's rename detection between the two
upstream trees, the rule `git merge` uses), so TN's change moves with it, and the merged or
conflicting line names both paths. The workspace manifest is merged in its filtered form, so
upstream churn in crates TN does not carry never shows up.

The filter also drops the entries of upstream's `[patch]` sections. Cargo applies `[patch]` only
from the root manifest of the workspace it builds, so those entries never reach TN's build, and
reth's tests (`tn/cargo.sh`) should build the same sources TN ships. `sync` and `pick` list the
entries upstream added or removed. If TN needs one, add it to the root `Cargo.toml` and to
`vendor/reth/Cargo.toml`, with a CHANGES.md entry.

### Backport one upstream fix

```
python3 -I vendor/reth/tn/vendor.py pick <upstream-commit>
```

Applies that commit's change (against its first parent) to the vendored files it touches. The pin
does not move. Record it in CHANGES.md as a backport, with the upstream commit id. A later `sync`
past that commit merges cleanly, because both sides made the identical change.

### Move to a newer upstream release

```
python3 -I vendor/reth/tn/vendor.py sync v1.11.4
```

Then:

1. Resolve the conflicts it lists. `git diff` shows the markers. Search for `<<<<<<< TN`.
2. Act on the `check:` section, which lists workspace dependencies the vendored crates now use
   but nobody carries. Usually that is a new upstream crate: `vendor.py add <path>`.
3. Act on "paths gone upstream" (crates upstream moved to another repository or deleted). The
   tool leaves those files alone. Either `vendor.py remove` the crate and depend on its new home,
   or keep it as a TN-owned fork (ROADMAP.md, "Forking a crate off").
4. Act on "upstream changed its [patch] entries", if printed (see above).
5. Act on "files in the vendored scope are git-ignored", if printed: add a negation to
   `vendor/reth/.gitignore`, or `git add` leaves those files out of the commit.
6. Update the root `Cargo.lock` (`cargo update -p <crate>` for the third-party bumps the new
   manifest asks for), then build and test as for any vendored change.
7. Add a CHANGES.md entry for the sync and commit. `tn/upstream.toml` already names the new pin.

A trial run of `sync v2.0.0` (2026-10-30) gives an idea of a major step. 454 files changed. Six
vendored crates left the reth repository: `reth-primitives-traits`, `reth-codecs`,
`reth-codecs-derive` and `reth-zstd-compressors` became crates.io dependencies,
`reth-primitives` and `reth-engine-service` were removed. One new crate was needed
(`crates/engine/execution-cache`), and revm went from 34 to 36.

### Change the vendored set

```
python3 -I vendor/reth/tn/vendor.py add crates/foo        # at the pinned commit
python3 -I vendor/reth/tn/vendor.py remove crates/foo
python3 -I vendor/reth/tn/vendor.py check                 # anything used but not carried?
```

Both update `tn/crates.txt` and merge the workspace manifest. After `remove`, delete the matching
entry from the root `Cargo.toml` if TN named the crate, and refresh `Cargo.lock` with a build.

## Verifying the copy

- `python3 -I vendor/reth/tn/vendor.py status` compares every vendored file with the pinned
  upstream and lists what differs. Right after the import it printed "identical".
- `vendor.py check` reports workspace dependencies the vendored manifests use but the nested
  workspace does not define.
- `vendor.py ledger` fails when a file `status` lists is not named by a CHANGES.md entry, or a
  `TN(TN-NNNN)` comment cites an id with no entry. An entry names a file by its path from
  `vendor/reth` (braces such as `src/{a.rs, b.rs}` expand) or from a crate it named earlier, or
  names a whole directory. It needs the upstream clone, as `status` does, so CI does not run it.
- `vendor.py status` also warns about files a `.gitignore` rule hides from git, which would
  otherwise drop out of the committed copy without anyone noticing. `sync`, `pick`, `add` and
  `remove` end with the same warning, and exit 1 when it lists files.

## Checking the feature graph

Removing a crate or a dependency edge can switch off a feature another crate relied on
(DECISIONS.md D10). Snapshot before and after, and diff:

```
python3 -I vendor/reth/tn/graph.py snapshot /tmp/before.txt [-- extra cargo tree args]
python3 -I vendor/reth/tn/graph.py snapshot /tmp/after.txt
python3 -I vendor/reth/tn/graph.py diff /tmp/before.txt /tmp/after.txt
```

A snapshot keeps each target's run-time units (what is linked into the binary) apart from its
build-time units (build scripts' dependencies, proc macros and what they use), because cargo
resolves features for the two separately. The targets default to x86_64-unknown-linux-gnu and
aarch64-apple-darwin; `-- --target <triple>` replaces them. To snapshot another checkout, such as
the commit before a change, with the current script, pass `-- --manifest-path
<checkout>/Cargo.toml`. Snapshots from before these views (one merged line per package) diff only
against each other.

## Testing the tools

```
python3 -B -I -m unittest discover -s vendor/reth/tn -p 'test_*.py' -v   # vendor.py, graph.py
python3 -B -I -m unittest discover -s etc -p 'test_rocksdb_gate.py' -v   # etc/rocksdb-gate.sh
```

The tests work in throwaway directories and never touch this tree or the upstream clone.
graph.py's test runs cargo on a small workspace of its own.
