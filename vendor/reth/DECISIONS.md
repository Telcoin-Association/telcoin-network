# Decisions: vendoring reth

Choices made while vendoring reth, with the reasoning, so later work can revisit them knowingly.
Newest last. Entries are short; the "why" is the part to keep.

## D1. Vendor at the pinned tag, v1.11.3, with no upgrade

Decided with the maintainer, 2026-10-30. TN pinned `reth` at git tag `v1.11.3`
(`d6324d63e27ef6b7c49cdc9b1977c1b808234c7b`). Vendoring is a pure relocation: the import commit
is byte-identical to upstream and the build resolves to exactly the same third-party versions
(the root `Cargo.lock` diff is the 101 reth `source` lines only). Upgrading is a separate job for
`tn/vendor.py sync`.

## D2. A nested workspace, not members of TN's workspace

Decided with the maintainer, 2026-10-30. `vendor/reth/Cargo.toml` is reth's own root manifest,
filtered to the vendored crates, and the TN root lists `vendor/reth` under `exclude`. TN reaches
the crates through `path` dependencies.

- Upstream manifests stay untouched (`version.workspace = true`, `lints.workspace = true`, ...),
  so upstream diffs apply cleanly.
- reth's lints and formatting settings keep applying to reth's code. TN's house lints
  (`missing_docs`, `unreachable_pub`, ...) do not.
- Cost: TN's `cargo test --workspace` does not run reth's tests. `tn/cargo.sh` runs them inside
  the nested workspace.
- Cost: unlike git dependencies, path dependencies are not lint-capped. CI's
  `RUSTFLAGS="-D warnings -D unused_extern_crates"` and `cargo fmt --all` (which walks path
  dependencies) now cover the vendored code. It has to stay warning-free and formatted with reth's
  `rustfmt.toml` (carried at `vendor/reth/rustfmt.toml`).

Moving a crate into TN's workspace proper is the natural step once TN has forked it for good
(see ROADMAP.md, "Forking a crate off").

## D3. Mirror upstream paths and keep crate names

`vendor/reth/<path>` is upstream's `<path>`, and the packages keep their `reth-*` names. A diff
between two upstream points then maps file for file onto the vendored tree, and TN's code (which
says `reth_db::...`) did not change at all. Renaming a crate is part of forking it off, not of
vendoring.

## D4. Vendor exactly the resolved dependency closure, plus the test-only `reth-testing-utils`

The vendored set is every reth package in TN's resolved graph: 101 crates, the same under default
features, `tndb-b` and `--all-features`. `testing/testing-utils` (36 KB, all of its dependencies
already vendored) is added so reth's own unit tests resolve inside the nested workspace. Nested
crates (`codecs/derive`, `libmdbx-rs/mdbx-sys`) are listed separately in `tn/crates.txt`, because
each one is its own package.

## D5. Drop `reth-e2e-test-utils` instead of vendoring it

It is a dev-dependency of `reth-engine-tree` and `reth-node-ethereum` only, and it depends on
`reth-cli-commands`, which would pull reth's whole CLI into the vendored set. TN-0001 in
CHANGES.md removes the two dev-dependency lines and the integration-test directories that used
them (`engine/tree/tests/e2e-testsuite`, `ethereum/node/tests/e2e` and its assets).

## D6. One Python tool (`tn/vendor.py`) and per-file 3-way merges for every upstream intake

Alternatives considered:

- `git subtree`: it brings in all of reth's 170 MB (we carry 32 MB), and a subtree pull of the
  whole repository fights every crate we deliberately left out.
- A vendor branch merged into TN: it relies on the merge history surviving, and TN squash-merges
  PRs to `main`, which drops it.
- `git apply --3way` of upstream diffs: it needs the preimage blobs in TN's object store, and
  after squash merges they are not reliably there.

`vendor.py` sidesteps all three. It exports base and target trees from an upstream clone into
temp directories and runs `git merge-file` per file against the vendored copy. It needs no shared
history, so it survives squash merges and rebases. The workspace manifest is merged in its
filtered form, so upstream changes to crates TN does not carry never conflict. The same engine
serves `sync` (move to a new upstream point), `pick` (backport one upstream commit), `add` and
`remove` (change the vendored set).

The pin lives in `tn/upstream.toml`, and `vendor.py status` must report only the changes recorded
in CHANGES.md. That invariant is what keeps future syncs mechanical.

## D7. The nested lockfile is generated, not committed

`vendor/reth/Cargo.lock` is git-ignored and re-seeded from the root `Cargo.lock` by `tn/cargo.sh`
on every run. A second committed lockfile would drift from the one TN ships. Seeding keeps reth's
tests on TN's dependency versions, and cargo only adds the packages that reth's dev-dependencies
alone need.

## D8. Git workflow

Decided with the maintainer, 2026-10-30: local commits and tags only, on `sls_tndb_b_vendor`;
nothing is pushed. Reduction work happens on the same branch after the tag `reth-reduction-start`.

Tags:

- `reth-vendor-import-v1.11.3`: the verbatim import (tree identical to upstream; nothing built
  against it yet).
- `reth-vendor-v1.11.3`: vendoring complete and verified; TN builds from `vendor/reth`. Only the
  ledger entries made for vendoring itself (TN-0001 ...) separate it from upstream.
- `reth-reduction-start`: the last commit before code reduction (removing crates and features,
  replacing reth layers) begins.

## D9. Keep MDBX compiled for now

The tndb-b work (`tndb-b-reth`, the `tndb-b` feature) is replacing MDBX as TN's reth database,
but MDBX is still the default and the migration source. The vendored `reth-libmdbx` and
`reth-mdbx-sys` stay in place. Gating them out is listed in ROADMAP.md, for whoever lands the
tndb-b switch.
