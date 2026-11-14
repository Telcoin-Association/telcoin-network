# ROADMAP: from vendored reth to a TN-owned execution layer

Where the vendored reth goes next, in the order that makes each step cheap and safe. Read
`tn/notes/usage-map.md` first: it records what TN actually uses from each crate and where. Status
markers: **[done]**, **[next]**, **[later]**.

## Status

- reth v1.11.4, cut from 101 crates to 57 plus `reth-testing-utils` (Phases 1, 2 and 6 and
  reduction pass 2 below). RocksDB is gone (TN-0014). Every TN edit has a ledger id in
  CHANGES.md; CLAUDE.md maps the ids by area.
- tn-lmdb is the default reth database, and storage layout v2 the default for new datadirs
  (DECISIONS.md D22). Phase 3 lists what is left before MDBX retires.
- CI runs reth's own tests for the vendored crates and the `js-tracer` build (lane
  `test-vendor-reth`, job `vendor-reth-test`) and the patched crate's tests (job
  `revm-inspectors-patch`).
- `make test-upgrade` runs the previous release's binary (`make build-e2e-bin-prev
  TN_PREV_REF=<release>`) and this one side by side: rolling in-place upgrade, re-execution from
  genesis on both binaries, a rollback, and identical block hash, `stateRoot` and `receiptsRoot`
  on every node. Run it against the deployed release before rolling out a change to the
  execution or storage path (CLAUDE.md, "Fork safety").
- Known issues left open: `tn/notes/open-issues.md`. Upstream fixes not yet applied:
  `tn/notes/upstream-backports.md`.
- Next, roughly by value: Phase 3's remaining steps once testnet has resynced, Phase 5 (txpool,
  needs TN design input), Phase 4 (alloy/revm directly; touches consensus-critical execution
  code, so coordinate with whoever is changing `tn-reth/src/env` and `evm`), the Phase 2
  follow-up (worth little for build cost, see Phase 2).

## Ground rules for every step

- One logical change per commit, and every commit builds and passes. Each change to vendored
  files gets a CHANGES.md entry. `vendor.py status` must list nothing the ledger does not cover.
- Before removing a crate or a dependency edge, snapshot the feature graph and diff it
  afterwards: `python3 -I vendor/reth/tn/graph.py snapshot|diff`. A third-party crate that loses
  a feature needs an explanation in the commit. The revm precompile backends (`secp256k1`, `blst`,
  `c-kzg`) and `asm-keccak` are consensus-relevant: losing one can change results or performance
  without a compile error.
- Verification ladder, cheapest first: `cargo check --workspace --all-targets`,
  `etc/ci-lanes.sh fmt` and `clippy`, the nextest lanes including `test-vendor-reth`, then the
  e2e restart and epoch suites (`make test-restarts`, `make test-epochs`). For a change on the
  execution or storage path, also `make build-e2e-bin-prev TN_PREV_REF=<release>` and then
  `make test-upgrade`. Run reth's own tests for any vendored crate you change
  (`tn/cargo.sh test -p ...`).

## Phase 0: vendor [done]

reth v1.11.3 vendored under `vendor/reth`; 101 crates plus `reth-testing-utils`. The build
resolves to the same third-party versions and features as the git dependency did. Tag
`reth-vendor-v1.11.3`.

## Phase 1: drop the node-builder and node-ethereum layers [done]

Done 2026-10-30 (CHANGES.md TN-0004, TN-0005): 24 reth crates and 9 third-party crates left the
graph. TN never used reth's node builder; it assembles provider, EVM, pool and RPC itself.
`crates/tn-reth/Cargo.toml` now sets the revm features `reth-node-ethereum` used to (`secp256k1`,
`blst`, `c-kzg`, `memory_limit`, `asm-keccak`, and `keccak-cache-global` through reth-node-core;
`tn/notes/usage-map.md`, last section). `DefaultEthPayloadTypes` stayed: `traits.rs` tests use it
as a fixture.

## Phase 2: the Ethereum p2p and node-config stack [done]

Done 2026-10-30 (CHANGES.md TN-0006 to TN-0009): 10 reth crates and 27 third-party crates left
the graph, the devp2p stack among them. One surprise: `reth-rpc` needed `reth-chain-state`'s
`serde` feature without declaring it, and got it only through reth-network (TN-0007).

Open follow-up: a TN-owned config struct replacing `NodeConfig`, which would also let
`reth-rpc-builder` take its server arguments from a small struct instead of `reth-node-core`.
Measured 2026-10-31 after TN-0020: dropping reth-node-core would take out only nine small
pure-Rust packages (derive_builder and its two helper crates, dirs, dirs-sys, option-ext,
shellexpand, vergen-lib, and node-core itself). That is little build cost against a rewrite of
the CLI wiring operators depend on (`tn-reth/src/cli.rs`). Do the follow-up only when TN wants
to own its configuration for its own sake.

## Phase 3: tn-lmdb as the reth database [in progress: the default backend since 2026-11-09]

tn-lmdb was called tndb-b until 2026-11-04. It is TN's default reth backend since 2026-11-09
(DECISIONS.md D22): every build runs reth on `tn_lmdb_reth::TnLmdbEnv`, and the opt-in `mdbx`
feature of `tn-reth` selects MDBX, kept until testnet has resynced. Vendoring lets the tn-lmdb
work change reth's storage crates directly instead of working around them. Where MDBX is wired in (2026-10-30, at the 63-crate state):

- `reth-db`: `mdbx` is a default feature (`reth-libmdbx`, `page_size`, MDBX metrics).
- Hard `reth-db/mdbx` enables: `reth-node-core` (its `DatabaseArgs` convert to MDBX's
  `DatabaseArguments`), `reth-db-common` and `reth-provider`.
- `reth-provider`: MDBX-only constructors and `impl` blocks for `DB = DatabaseEnv` in
  `providers/database/mod.rs` and `builder.rs` (`init_db`, `open_db_read_only`,
  `DatabaseArguments`), plus test utilities on `TempDatabase<DatabaseEnv>`.
- TN's own MDBX users: `tn-storage` (feature `reth-libmdbx`, default on; the migration source
  for TN's consensus DB), `tn-reth`'s MDBX to tn-lmdb migration of the reth database
  (`src/env/database/migrate.rs`, which reads MDBX through `reth_db::mdbx`) and its opt-in `mdbx`
  build, and `tn-lmdb-reth`'s differential tests (dev-dependency).

Steps, now that tn-lmdb is the default backend, once testnet has resynced (MDBX and storage
layout v1 go together then, DECISIONS.md D22):

1. Gate the MDBX constructors and `impl` blocks in the vendored `reth-provider`,
   `reth-node-core` and `reth-db-common` behind an `mdbx` feature on each crate that forwards to
   `reth-db/mdbx`, and make `reth-db`'s default `[]`. TN enables `mdbx` only where it still needs
   it.
2. When `tn-storage`'s and `tn-reth`'s MDBX migration paths and the `mdbx` feature are retired,
   TN's build stops compiling `reth-mdbx-sys` (the C library) entirely. `vendor.py remove` `reth-libmdbx`/`reth-mdbx-sys`
   only once no TN feature uses them.
3. Decide whether static files (`reth-nippy-jar`, `StaticFileProvider`) stay or move into tn-lmdb.
4. Keep the `reth_db_*` metric names, or change them together with the e2e tests and
   `tn-metrics` that assert on them.

## RocksDB step B: remove the stub and the storage-v2 plumbing [deferred: DECISIONS.md D15]

Step A (TN-0014) removed RocksDB itself. What remains compiles in every build but never does
RocksDB work:

- `providers/rocksdb_stub.rs`: the zero-size `RocksDBProvider`, `RocksDBBatch`, `RocksTx`,
  `RocksDBBuilder`, iterators and stats types.
- `ProviderFactory` holds a `rocksdb_provider` field, and its constructors take one (TN passes
  `RocksDBProvider::new(datadir)`, `tn-reth/src/env/mod.rs`). `RocksDBProviderFactory`
  (`traits/rocksdb_provider.rs`) exposes it.
- `EitherWriter`/`EitherReader` (`either_writer.rs`) still have the `RocksBatchArg`/`RocksTxRefArg`
  parameters, typed `()`, and `storage_v2` routing in `StorageSettings` (reth-db-api
  `models/metadata.rs`).
- `check_consistency()` returns a RocksDB unwind target (always `None`); TN's startup refuses
  any unwind target it returns (`env/mod.rs`). `DatadirArgs::rocksdb_path` and `ChainPath`'s RocksDB directory live in
  reth-node-core (`args/datadir_args.rs`, `dirs.rs`), and TN sets `rocksdb_path: None` in
  `dirs.rs`, `genesis.rs` and `snapshot.rs`.

Plan: drop the stub types and the field and parameters from the provider factory; collapse
`EitherWriter`/`EitherReader` to their database and static-file arms; remove `storage_v2`'s RocksDB
routing and `rocksdb_path`; adjust `check_consistency`'s return type and TN's call sites. This
changes compiled storage code (state and history writes), so it needs the full test suite and the
e2e restart/epoch suites. Coordinate with the tn-lmdb work: `EitherWriter` is where reth abstracts
storage backends. Verify by running `etc/rocksdb-gate.sh` and grepping for `[Rr]ocks` (only
CHANGES.md and DECISIONS.md should remain).

## Phase 4: alloy and revm directly [later]

- `reth-revm` is mostly revm re-exports. Import `revm::` directly and keep only `State`,
  `StateProviderDatabase` and `CachedReads` from reth (or own them).
- `reth-evm`: import alloy-evm items from `alloy_evm`. Keep `ConfigureEvm`, `BlockBuilder` and
  `BlockAssembler` until TN's execution path (`env/execution.rs`) is rewritten on its own
  traits.
- `reth-evm-ethereum` contributes three items. TN pins all forks at genesis, so a TN-owned
  `revm_spec` and receipt builder are small.
- `reth-metrics` in nine TN crates (tn-reth included) becomes `metrics` plus `metrics-derive`
  (mechanical).
- [done, TN-0012] `reth-primitives` became `reth-primitives-traits` plus `reth-ethereum-primitives`;
  `tn-types` now owns the `Block`/`BlockBody`/`SealedBlock` aliases. Going further (TN-owned
  `Sealed*`/`Recovered*`) touches about 150 sites through `tn-types`.
- `reth-chainspec`: a TN chainspec implementing `EthChainSpec`/`EthereumHardforks`, so the
  provider, `EthApi` and pool bounds keep working.

## Phase 5: a transaction pool for TN [later]

TN wraps reth's `EthTransactionPool` (`tn-reth/src/txn_pool.rs`), runs its own maintenance
loop and disables EIP-4844 and EIP-7702. With the code vendored, candidates are: drop the blob
store and the 4844 paths, give batch building its own ordering, and simplify validation to TN's
rules.

## Phase 6: RPC surface [done]

Done 2026-10-30 and 2026-10-31 (CHANGES.md TN-0010, TN-0011, TN-0021, TN-0033): the engine-API
auth server, `EngineEthApi`, `ValidationApi`, `TestingApi` and the `admin`, `miner`,
`otterscan`, `reth`, `txpool` and `mev` namespaces are gone, and with them
`reth-rpc-engine-api`, `reth-payload-builder` and `reth-consensus-common`. TN serves only `eth`,
`net`, `web3`, `debug`, `trace` and `rpc` (`ALLOWED_MODULES` in `tn-reth/src/cli.rs`) plus its own
`tn` namespace. What stayed, and why:

- `reth-rpc-layer`: the main server uses its `CompressionLayer` and optional JWT layer.
- `eth_callBundle`, in the `eth` namespace: TN's epoch-fee adapter (`tn-reth/src/rpc_bundle.rs`)
  wraps reth's `EthBundle`.
- `RethRpcModule` (`reth-rpc-server-types`) keeps the removed names: operators' `--http.api`
  lists are validated against it (`validate_rpc_modules`). Shrink it only if nothing parses them.

At a sync, an upstream change to a deleted file merges as "left deleted"; only rpc-builder's
registry arms and the two `lib.rs` files can conflict.

## Reduction pass 2 [done]

Done 2026-10-31 (DECISIONS.md D20; CHANGES.md TN-0025 to TN-0033). Method: list the third-party
packages reachable only through vendored crates, then, per vendored crate and per dependency
edge, what would leave the graph if it went; each cut one commit with an empty third-party
`graph.py diff` or an explained one.

Left, with the reason:

- **`js-tracer`: opt-in since D21.** It compiles boa, a JavaScript engine: 35 packages, the
  largest single cut. The maintainers chose a build-time feature (`tn-reth/js-tracer`, off by
  default) over removal for now; removal is up to a team review, and
  `crates/tn-reth/js-tracer.md` lists its steps. Native tracers (`callTracer`, `prestateTracer`,
  ...) work in both builds.
- After MDBX is retired: notify and its helpers (5 packages; the read-only static-file watcher,
  whose switch runs through the MDBX `ProviderFactoryBuilder`), sysinfo (reth-db's storage-lock
  liveness check), and parity-scale-codec and modular-bitfield (on-disk encodings).
- Not worth it: shellexpand and dirs (path expansion in operator-facing flags), reth-tracing's
  stdout/file/journald stack (TN's logging), node-core's argument types (TN builds `NodeConfig`
  from them; the Phase 2 follow-up), lz4_flex and fixed-map (static-file format), reth-trie's two
  feature-forwarding dependencies (no package leaves; consensus-sensitive crate).

## Taking upstream changes as the code diverges

- `tn/notes/upstream-backports.md` is the tracker: what has been applied (with TN ids) and every
  open upstream fix that matters to TN, ranked by severity for TN. Re-survey before each sync
  and after upstream security releases.

- While a crate is only lightly changed, `vendor.py sync` and `pick` keep working. Watch reth
  releases for fixes in the crates TN still carries close to upstream: rpc, transaction-pool,
  trie, provider, chain-state.
- A trial `sync v2.0.0` (2026-10-30) showed the shape of the next major step:
  `reth-primitives-traits`, `reth-codecs(-derive)` and `reth-zstd-compressors` move to crates.io,
  `reth-primitives` and `reth-engine-service` disappear, `engine/execution-cache` is new, and revm
  goes from 34 to 36.
- Security fixes in revm and alloy arrive through crates.io, independent of reth. Bump them in
  `vendor/reth/Cargo.toml` and the root `Cargo.lock`.

## Forking a crate off

When TN has changed most of a crate and upstream syncs no longer apply to it:

1. Record the decision in DECISIONS.md and CHANGES.md.
2. `vendor.py remove` it, move it into TN's workspace (`crates/`), rename it `tn-*`, give it
   TN's lints, and fix up the dependents.
3. Its upstream fixes become manual ports from then on. Note the last upstream commit it tracked.
