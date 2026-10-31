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

## D10. Reduction keeps behavior identical: restore the features a removed crate enabled

Removing a crate can switch off features it enabled on crates that stay, through feature
unification. Phase 1 would have dropped revm's `secp256k1`, `blst` and `c-kzg` precompile
backends and `asm-keccak`, and reth-chain-state's `rayon`. The rule: snapshot the feature graph
before and after (`tn/graph.py`). Every feature that changes behavior or performance on a
surviving crate is restored explicitly where TN depends on that crate, with a comment saying
why. Features that only gate APIs TN does not use (serde bincode compat, RPC client traits,
`send_guard`, fixed-cache's allocating constructor) may go. The compiler proves they were
unused. Each one is named in the commit message.

## D11. Tests that need a removed crate are dropped, not kept alive

When a surviving crate's tests need a crate that reduction removed (TN-0004:
`EthereumEngineValidator` for the engine-API tests), the tests go and the ledger says so.
Keeping a removed crate as a dev-only dependency would keep its whole subgraph in the nested
workspace and defeat the reduction. Where only part of a suite is affected, only that part
goes (rpc-builder keeps its http/ws/startup tests).

## D12. Slim vendored crates in place before replacing them

Phase 2 removed the devp2p parts of `reth-node-core` (TN-0008) instead of replacing
`NodeConfig` with a TN-owned struct. The slimming took a dozen deletions and left every remaining
API as it was, so `tn-reth` and `reth-rpc-builder` kept compiling unchanged apart from the
dropped arguments. A replacement would touch every `NodeConfig` user at once. Prefer the
smallest in-place cut that removes a dependency edge, and record it in the ledger. Leave
redesigns (a TN config struct) for when the remaining surface is small enough to see whole.

## D13. A crate declares the features it uses

Feature unification hides missing feature declarations. `reth-rpc` serialized
`CanonStateNotification` without asking for `reth-chain-state/serde`, and compiled only because
the devp2p crates enabled it (TN-0007). When a removal breaks a crate this way, fix the
manifest of the crate that uses the feature, at the use site, not of whichever crate happened to
enable it. Such fixes are worth offering upstream.

## D14. No RocksDB anywhere

Decided by the maintainer, 2026-10-30: RocksDB must be gone from the vendored reth and from TN.
That means no off-by-default feature, no dev-dependency and no code path. librocksdb-sys is a
very expensive native build, and the team is removing every need for it. Step A (TN-0014)
removed the backend, the features and the crate, with the expanded source of the touched crates
proven unchanged. Step B, removing the zero-size stub and the storage-v2 plumbing typed against
it, changes compiled code and is planned in ROADMAP.md. `etc/rocksdb-gate.sh` now fails if
`Cargo.lock` or any manifest (comments aside) mentions rocksdb, so an upstream sync cannot
quietly bring it back.

## D15. RocksDB step B waits for the storage work

Step B (removing the zero-size `RocksDBProvider` stub, the `RocksBatchArg`/`RocksTxRefArg`
placeholders, `ProviderFactory`'s RocksDB field and parameters, and `storage_v2`'s RocksDB
routing) was planned on 2026-10-30 but not done unattended. Step A already removed everything
that builds or runs RocksDB, and the gate enforces it, so step B saves no build time. Step B
rewrites live storage code: the history and changeset write routing in `EitherWriter`, the
persisted `StorageSettings` (whose `storage_v2` flag is part of the on-disk metadata and has to
keep decoding), and `check_consistency`. That is the code the tndb-b work owns (Phase 3). Do it
there, or as its own reviewed change with the full test and e2e suites. Until then, a v2
database is impossible for TN because `tn-reth` pins `StorageArgs { v2: false }` and does not
expose the flag.

## D16. The JS-tracer interrupt is ported, not upgraded or removed

Upstream fixed the JS-tracer DoS (57825d41d) with `JsInspectorInterrupt` from revm-inspectors
0.44.2, which needs revm 43; TN is on revm 34 with revm-inspectors 0.34. Options, 2026-10-31:
bump revm (the "Phase 4"-sized revm 34 → 43 move, far beyond a security fix), vendor and patch
revm-inspectors (another crate to carry), drop `js-tracer` (removes a geth-compatible feature
operators may rely on), or port the mechanism. The mechanism is small: a shared `AtomicBool`
checked at every inspector hook, which halts the EVM and sets the context error. It needs no
Boa change, because Boa's loop and recursion limits, already set by revm-inspectors 0.34, bound
the work inside one callback. So TN-0017 ports it as a wrapper around `DebugInspector` in a
TN-owned file, and keeps `debug.rs` shaped like upstream's version so the next sync swaps the
wrapper for `JsInspector::with_interrupt`.

## D17. Security backports: apply what is High for TN, track the rest

Maintainer direction, 2026-10-31: apply High and Critical security fixes, track everything else
in `tn/notes/upstream-backports.md`. Severity is judged against TN, not taken from upstream or
from the first survey. Two items were raised and two lowered:

- Raised to High: upstream's RPC blocking-work limits (TN-0019, six commits; first rated Medium).
  TN's execution engine and certificate validation share tokio's blocking pool with RPC, so
  unbounded RPC blocking work delays block execution.
- Lowered to Medium: 6e4009eed (log-subscription panic), because TN's `TaskManager` ignores
  non-critical task panics, so only the lagging client's subscription ends; and d3d7fb31d
  (replacement price-bump rounding), which matters only below 10 wei, where replacements are
  nearly free anyway.

Non-security High items (txpool nonce order, storage fixes) are tracked with a recommendation,
not applied, because they are outside the direction given. The storage ones also wait for the
tndb-b work.

## D18. Per-crate standalone builds are not a goal

Several vendored crates do not build alone (`tn/cargo.sh check -p reth-rpc-eth-api` fails): they
rely on features that other workspace members enable. Some of this is upstream's convention
(reth-rpc and rpc-eth-api use tokio's `time`/`rt`/`macros` without declaring them), and some
comes from TN's removals (TN-0006 took away the path through which rpc-eth-api got
`reth-primitives-traits/serde`). Fixing one exposes the next (declaring that serde feature then
breaks reth-ethereum-primitives). D13 still applies when a change breaks TN's build or
`tn/cargo.sh check --workspace`, the two configurations that matter. Per-crate standalone
breakage is left alone. Check vendored changes with `--workspace`.

## D19. jemalloc is not compiled in until TN installs it

TN's `tn-reth` manifest enabled `jemalloc` on reth-cli-util and reth-node-core, copied from the
reth umbrella crate's default features when TN switched to individual crates (2026-10-27). That
feature only matters to a binary that sets jemalloc as its `#[global_allocator]`. reth's own
binary does; no TN binary does (only a benchmark sets an allocator, a counting wrapper over
`System`). So TN compiled jemalloc's C library and never used it. Dropped 2026-10-31, with no
runtime change. If the team wants jemalloc (reth recommends it for memory fragmentation),
re-enable the feature and install it in `bin/telcoin-network`
(`#[global_allocator] static ALLOC: reth_cli_util::allocator::Allocator =
reth_cli_util::allocator::new_allocator();`). That is a behavior change to benchmark first.
