# vendor/reth: TN's copy of reth

Telcoin Network's execution layer is built on [reth](https://github.com/paradigmxyz/reth). This
directory holds the reth crates TN uses, as source, so TN can change them. Upstream syncs stay
possible but optional.

- Base: upstream `v1.11.3` (`d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`); the pin is in
  `tn/upstream.toml`.
- Carried: the reth crates in TN's dependency graph (63 after reduction Phases 1, 2, 4a and 6a;
  101 at import), plus the test-only `reth-testing-utils`. The list is in `tn/crates.txt`.
- TN's changes against upstream: CHANGES.md, the ledger.
- Why it is built this way: DECISIONS.md.
- What comes next (tndb-b, trimming Ethereum-only code, alloy/revm directly): ROADMAP.md.

## Layout

```
vendor/reth/
  Cargo.toml        nested workspace: upstream's root manifest, filtered to the vendored crates
  rustfmt.toml      upstream's, so `cargo fmt` formats reth code reth's way
  clippy.toml       upstream's
  LICENSE-*         upstream's (MIT OR Apache-2.0)
  crates/...        upstream paths, e.g. crates/storage/db is upstream's crates/storage/db
  testing/...       testing/testing-utils (dev-only)
  tn/               TN tooling: vendor.py, cargo.sh, crates.txt, upstream.toml
  README.md CHANGES.md DECISIONS.md ROADMAP.md   (TN's docs; upstream's README is not carried)
```

Everything outside `tn/` and the four TN docs mirrors an upstream path. Crate names are
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
   files that some entry accounts for.
3. Where an edit would surprise a reader of reth's code, leave a short comment that starts with
   `TN(TN-NNNN):`. Leave out cosmetic changes. They make every upstream sync conflict for
   nothing.
4. Build and test: `make check`, the TN tests that touch the area, and reth's own tests for the
   crate (`vendor/reth/tn/cargo.sh test -p <crate>`).

Run `python3 -I vendor/reth/tn/vendor.py diff [PATH...]` to see the full diff of TN's changes
against the pinned upstream. `status` lists the changed files.

## Running reth's own tests

reth's unit and integration tests only resolve inside the nested workspace (their
dev-dependencies are reth's). Use:

```
vendor/reth/tn/cargo.sh test -p reth-db --lib
vendor/reth/tn/cargo.sh nextest run -p reth-provider
vendor/reth/tn/cargo.sh check --workspace --all-targets   # every vendored crate's tests compile
```

Some crates' test code needs their own `test-utils` feature, which upstream's CI only gets from
workspace-wide `--all-features`. If `--all-targets` fails on a missing optional dependency, add
`--all-features`. (At import this hit `reth-downloaders`, which is no longer carried.)

`cargo.sh` seeds `vendor/reth/Cargo.lock` from TN's lockfile (the nested lockfile is
git-ignored) and builds into `target/vendor-reth`. When one of reth's dev-dependencies needs a
newer version of a crate TN also uses, cargo bumps it in the nested lockfile only. At v1.11.3 that
happens for `libc` and `cargo_metadata`. TN's own lockfile is never touched.

## Taking changes from upstream

Everything goes through `vendor/reth/tn/vendor.py`. It needs an upstream clone: set
`RETH_UPSTREAM=/path/to/reth`, or let it clone a blobless copy into `~/.cache/tn-vendor/reth`
on first use. Each command that brings upstream code in refuses to run while `vendor/reth` has
uncommitted changes, so its result can be reviewed and reverted on its own.

The engine is the same for all of them. For every file in scope it runs a 3-way merge with
base = upstream at the old point, theirs = upstream at the new point, ours = the file here.
A file TN never touched simply becomes upstream's new version. A file TN changed is merged with
`git merge-file`; where both sides changed the same lines, it leaves diff3-style conflict
markers (`<<<<<<< TN`, `||||||| upstream <old>`, `>>>>>>> upstream <new>`). The workspace
manifest is merged in its filtered form, so upstream churn in crates TN does not carry never
shows up.

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
4. Update the root `Cargo.lock` (`cargo update -p <crate>` for the third-party bumps the new
   manifest asks for), then build and test as for any vendored change.
5. Add a CHANGES.md entry for the sync and commit. `tn/upstream.toml` already names the new pin.

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
  upstream and lists what differs. Right after the import (tag `reth-vendor-import-v1.11.3`) it
  printed "identical".
- `vendor.py check` reports workspace dependencies the vendored manifests use but the nested
  workspace does not define.
- `vendor.py status` also warns about files a `.gitignore` rule hides from git, which would
  otherwise drop out of the committed copy without anyone noticing.
