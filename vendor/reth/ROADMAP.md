# ROADMAP: from vendored reth to a TN-owned execution layer

Where the vendored reth goes next, in the order that makes each step cheap and safe. Read
`tn/notes/usage-map.md` first: it records what TN actually uses from each crate and where. Status
markers: **[done]**, **[next]**, **[later]**.

## Status (2026-10-31)

- 101 reth crates were vendored at v1.11.3; 57 remain after reduction Phases 1, 2, 4a, 6a, 6b
  and the second pass (TN-0025 to TN-0033, tag `reth-reduction-2-start`). Synced to v1.11.4.
  RocksDB is gone (TN-0014); its zero-size stubs stay until the tndb-b work (D15). The ledger
  runs TN-0001 to TN-0033.
- Security backports applied (TN-0015 to TN-0019, D17): WebSocket origin check, JS-tracer
  timeout/interrupt (ported to revm-inspectors 0.34, D16), txpool sender-id growth, and RPC
  blocking-work limits. Everything else upstream is tracked in `tn/notes/upstream-backports.md`.
- Correctness backports: txpool nonce order (TN-0022) and six static-file fixes (TN-0023: jar
  cache races, empty-genesis recovery, nippy-jar torn tails, read errors). Upstream fixes to MDBX
  code are tracked for the tndb-b work. The replacement price bump rounds up (TN-0024; pool
  admission only, so no fork risk against nodes without it).
- Second reduction pass done (TN-0025 to TN-0033, D20): 6 reth crates and 19 third-party packages
  left the node graph (890 to 865 packages, the same in the tndb-b build), with no behavior change.
  See "Reduction pass 2" below for what was cut, what was left and why.
- Build cost: reth-node-core no longer rebuilds on every TN commit and libgit2 is no longer
  compiled (TN-0020); jemalloc, which no TN binary installs, is no longer compiled (D19). Three
  native C builds left the graph.
- Phase 6b done (TN-0021): about 2.3k lines of unreachable RPC code removed.
- `tn/notes/usage-map.md` was regenerated for the 63-crate tree on 2026-10-31 and updated for the
  57-crate tree after the second pass.
- Verified 2026-10-31. Backport head `f093532ca`: nextest 3719 passed, 0 failed, 90 skipped;
  `make test-restarts` 8/8; `make test-epochs` 11/11. Final head `c8a527384`: the same counts,
  plus the nested workspace check, both clippy lanes, fmt and the RocksDB gate. nextest's LEAK
  flags differ from run to run (5 tests on one run, 1 on another, none twice); each flagged test
  reran clean. TN-0022/23 head `1af59b607`: nextest 3719 passed (and 3744 with
  `tn-reth/tndb-b`), restarts 8/8, epochs 11/11, `make test-sigkill` 7/7, CI-flags check
  (default and tndb-b), clippy, fmt, the RocksDB gate, and the nested tests for nippy-jar,
  provider, txpool and db (db with two threads; see README).
- Out of scope here: Phase 3 and RocksDB step B belong to the tndb-b work, which happens
  elsewhere.
- Open decision: drop `js-tracer` (see "Reduction pass 2"). It is the largest single cut left.
- Next, roughly by value: Phase 5 (txpool, needs TN design input), Phase 4 (alloy/revm
  directly; touches consensus-critical execution code, so coordinate with whoever is changing
  `tn-reth/src/env` and `evm`), the Phase 2 follow-up (now worth little for build cost, see
  Phase 2).

## Ground rules for every step

- One logical change per commit, and every commit builds and passes. Each change to vendored
  files gets a CHANGES.md entry. `vendor.py status` must list nothing the ledger does not cover.
- Before removing a crate or a dependency edge, snapshot the feature graph and diff it
  afterwards: `python3 -I vendor/reth/tn/graph.py snapshot|diff`. A third-party crate that loses
  a feature needs an explanation in the commit. The revm precompile backends (`secp256k1`, `blst`,
  `c-kzg`) and `asm-keccak` are consensus-relevant: losing one can change results or performance
  without a compile error.
- Verification ladder, cheapest first: `cargo check --workspace --all-targets` (default and
  `--features tn-reth/tndb-b`), `etc/ci-lanes.sh fmt` and `clippy`, the nextest lanes, then the
  e2e restart and epoch suites (`make test-restarts`, `make test-epochs`, and the `-tndb`
  variants). Run reth's own tests for any vendored crate you change (`tn/cargo.sh test -p ...`).
- Tag `reth-reduction-start` sits on the last commit before the first reduction. `git diff
  reth-reduction-start` shows everything reduction has done.

## Phase 0: vendor [done]

reth v1.11.3 vendored under `vendor/reth`; 101 crates plus `reth-testing-utils`. The build
resolves to the same third-party versions and features as the git dependency did. Tag
`reth-vendor-v1.11.3`.

## Phase 1: drop the node-builder and node-ethereum layers [done]

Done 2026-10-30: 24 reth crates and 9 third-party crates left the graph (CHANGES.md TN-0004,
TN-0005). The numbered steps below are kept as a record. One correction to the plan:
`DefaultEthPayloadTypes` is not dead, because `traits.rs` tests use it as a fixture, so it
stayed.

TN never uses reth's node builder. It assembles provider, EVM, pool and RPC itself. It still
depends on `reth-node-builder` (for re-exports only) and `reth-node-ethereum` (for two types).
Those two pull in engine-tree, engine-service, engine-local, exex, downloaders, stages, era*,
payload builders, node-ethstats, consensus-debug-client, invalid-block-hooks, node-events and
node-metrics.

1. In `crates/tn-reth`, import from where the items live: `NodeConfig` and
   `RethTransactionPoolConfig` from `reth-node-core`, the node types from
   `reth-node-types`/`reth-node-api`, `PayloadTypes`/`BuiltPayload` from
   `reth-payload-primitives`, the `DEFAULT_*` constants from `reth-engine-primitives`.
2. `EthEngineTypes` comes from `reth-ethereum-engine-primitives`, and `EthPayloadAttributes` is
   alloy's `PayloadAttributes`. Delete the dead `DefaultEthPayloadTypes` and the dead
   re-exports listed in the usage map.
3. `reth-node-ethereum` enables revm `secp256k1`, `blst`, `c-kzg` and `memory_limit`, and
   forwards `asm-keccak`/`keccak-cache-global`. Enable these where TN depends on revm, then
   prove with `graph.py diff` that nothing third-party lost a feature.
4. `js-tracer` is already set on `reth-rpc` and `reth-rpc-eth-types`. Drop the forwarding
   through node-builder.
5. `vendor.py remove` every crate that left the graph, and delete their root `Cargo.toml`
   entries.

`reth-engine-local` stays: `reth-node-core` depends on it. It is a Phase 2 item, together
with `reth-rpc-engine-api`, which `reth-rpc` and `reth-rpc-builder` depend on.

## Phase 2: the Ethereum p2p and node-config stack [done]

Done 2026-10-30 as planned below (CHANGES.md TN-0006 to TN-0009): 10 reth crates and 27
third-party crates left the graph. One surprise: `reth-rpc` needed `reth-chain-state`'s `serde`
feature without declaring it, and got it only through reth-network (TN-0007). The follow-up
(a TN-owned config struct replacing `NodeConfig`) is still open.

After Phase 1, `reth-node-core` holds the devp2p stack in the graph. It is reached from
`tn-reth`, `reth-rpc-builder` (for `RpcServerArgs`) and `reth-node-api`; the RPC crates need
`reth-node-api` only for re-exports. TN's own uses are type-only: a dummy key, `NetworkArgs`
enums, stub `NetworkInfo`/`Peers` impls for the `net_` namespace, `BlockHashNumber`,
`NatResolver::None`. Of `NodeConfig`, `tn-reth` reads only `chain`, `datadir()`, `rpc`, `txpool`,
`db` and `pruning`. A simulation of the cuts below on the Phase 1 graph removes 10 reth crates
(node-api, basic-payload-builder, network, discv4, discv5, dns-discovery, ecies, eth-wire,
net-nat, engine-local) and 17 third-party ones (hickory DNS, discv5, a second libp2p-identity,
...).

- **2a** (TN-0006): take `reth-node-api` out of `reth-rpc` and `reth-rpc-eth-api`. Remove the
  blanket `RpcNodeCore` impl over `FullNodeComponents` and the `EthApiFor`-style aliases, and
  import the rest from `reth-primitives-traits` and `reth-payload-primitives`.
- **2b**: slim the vendored `reth-node-core`. Drop `NodeConfig::network` and `args/network.rs`
  (`NetworkArgs`, `DiscoveryArgs`), the `RethNetworkConfig` trait, the tip-from-network helpers
  (`max_block`, `lookup_or_fetch_tip`, `fetch_tip_from_network`, `utils.rs`'s header fetchers)
  and `dev_mining_mode` (the only `reth-engine-local` use). Keep the port-adjusting helpers for
  RPC only. `tn-reth/src/cli.rs` stops pinning network values.
- **2c**: `tn-reth` drops `reth-network`, `reth-discv4` and `reth-eth-wire`. The stub key comes
  from `secp256k1`, `BlockHashNumber` becomes alloy's `BlockNumHash`, and `DisconnectReason`
  comes from where `reth-network-api`'s `Peers` trait takes it. `vendor.py remove` the crates
  that leave.

Keep `reth-network-api` (small, and the `EthApi` bounds need its traits) and
`reth-network-p2p`/`reth-eth-wire-types` (the pool and network-api use them). A TN-owned config
struct replacing `NodeConfig` altogether is the follow-up. It would also let `reth-rpc-builder`
take its server arguments from a small struct instead of `reth-node-core`.

Measured 2026-10-31 after TN-0020: dropping reth-node-core would take out only nine small
pure-Rust packages (derive_builder and its two helper crates, dirs, dirs-sys, option-ext,
shellexpand, vergen-lib, and node-core itself). That is little build cost against a rewrite of
the CLI wiring operators depend on (`tn-reth/src/cli.rs`). Do the follow-up only when TN wants
to own its configuration for its own sake.

## Phase 3: tndb-b as the reth database [next; owned by the tndb-b work]

Vendoring lets the tndb-b work change reth's storage crates directly instead of working around
them. Where MDBX is wired in (2026-10-30, at the 63-crate state):

- `reth-db`: `mdbx` is a default feature (`reth-libmdbx`, `page_size`, MDBX metrics).
- Hard `reth-db/mdbx` enables: `reth-node-core` (its `DatabaseArgs` convert to MDBX's
  `DatabaseArguments`), `reth-db-common` and `reth-provider`.
- `reth-provider`: MDBX-only constructors and `impl` blocks for `DB = DatabaseEnv` in
  `providers/database/mod.rs` and `builder.rs` (`init_db`, `open_db_read_only`,
  `DatabaseArguments`), plus test utilities on `TempDatabase<DatabaseEnv>`.
- TN's own MDBX users: `tn-storage` (feature `reth-libmdbx`, default on; the migration source
  for TN's consensus DB) and `tndb-b-reth`'s differential tests (dev-dependency).

Steps, once tndb-b is the default backend:

1. Gate the MDBX constructors and `impl` blocks in the vendored `reth-provider`,
   `reth-node-core` and `reth-db-common` behind an `mdbx` feature on each crate that forwards to
   `reth-db/mdbx`, and make `reth-db`'s default `[]`. TN enables `mdbx` only where it still needs
   it.
2. When `tn-storage`'s MDBX migration path is retired, the `tndb-b` build stops compiling
   `reth-mdbx-sys` (the C library) entirely. `vendor.py remove` `reth-libmdbx`/`reth-mdbx-sys`
   only once no TN feature uses them.
3. Decide whether static files (`reth-nippy-jar`, `StaticFileProvider`) stay or move into tndb-b.
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
- `check_consistency()` returns a RocksDB unwind target (always `None`), which TN logs in
  `env/mod.rs`. `DatadirArgs::rocksdb_path` and `ChainPath`'s RocksDB directory live in
  reth-node-core (`args/datadir_args.rs`, `dirs.rs`), and TN sets `rocksdb_path: None` in
  `dirs.rs`, `genesis.rs` and `snapshot.rs`.

Plan: drop the stub types and the field and parameters from the provider factory; collapse
`EitherWriter`/`EitherReader` to their database and static-file arms; remove `storage_v2`'s RocksDB
routing and `rocksdb_path`; adjust `check_consistency`'s return type and TN's call sites. This
changes compiled storage code (state and history writes), so it needs the full test suite and the
e2e restart/epoch suites. Coordinate with the tndb-b work: `EitherWriter` is where reth abstracts
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
- `reth-metrics` in nine TN crates becomes `metrics` plus `metrics-derive` (mechanical).
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

6a done 2026-10-30 (CHANGES.md TN-0010, TN-0011): the engine-API auth server, `EngineEthApi`,
`ValidationApi` and `TestingApi` are gone, and with them `reth-rpc-engine-api`,
`reth-payload-builder` and `reth-consensus-common`. `reth-rpc-layer` stays: the main server uses
its `CompressionLayer` and optional JWT layer. 6b done 2026-10-31 (TN-0021): `admin`, `miner`,
`otterscan`, `reth` and `txpool` are removed, together with the unused anvil, hardhat, engine,
testing and validation traits. An upstream change to a deleted file merges as "left deleted";
only rpc-builder's registry arms and the two `lib.rs` files can conflict. `mev` stays: TN's
epoch-fee adapter serves `eth_callBundle` through `EthBundle`.

TN serves only `eth`, `net`, `web3`, `debug`, `trace` and `rpc` (`ALLOWED_MODULES` in
`tn-reth/src/cli.rs`) plus its own `tn` namespace, and overrides seven methods. The vendored
`reth-rpc` also carries `admin`, `engine` (`EngineEthApi`, the `EngineApi` re-export), `miner`,
`otterscan`, `reth`, `testing`, `txpool` and `validation` (flashbots `ValidationApi`, which
`tn-reth/src/traits.rs` says must never be registered), and `reth-rpc-builder` carries the JWT
auth server for the engine API. After Phase 2 these are the only reasons for
`reth-rpc-engine-api` and `reth-payload-builder` (both reached only through `reth-rpc`),
`reth-consensus-common` (`validation.rs`, `testing.rs`) and probably `reth-rpc-layer` (auth;
check that `CompressionLayer` is not used by the main server first).

Plan: remove the unserved modules from `reth-rpc` and their registration arms from
`reth-rpc-builder` (the `RethRpcModule` match in `lib.rs`), remove `rpc-builder`'s `auth`
module, then `vendor.py remove` what leaves. `RethRpcModule` itself lives in
`reth-rpc-server-types`. Shrink it only if nothing parses the removed names, because operators'
`--http.api` lists are validated against it (`validate_rpc_modules`). Verify with
`tn-reth`'s `env/rpc/tests` (namespace tests) and the e2e RPC tests.

## Reduction pass 2 [done]

Done 2026-10-31 (tag `reth-reduction-2-start` before it; DECISIONS.md D20). Method: list the
third-party packages reachable only through vendored crates (74 at the start), then, per
vendored crate and per dependency edge, what would leave the graph if it went. Cut, one commit
each, each with an empty third-party `graph.py diff` or an explained one:

- TN-0025: node-core's build script without vergen (7 build-time packages).
- TN-0026/27: tracing-futures and the docs-only aquamarine proc macro (5).
- TN-0028: db-common keeps `init_genesis`; the state-dump import and `DbTool` go, and with them
  reth-etl, boyer-moore-magiclen and debug-helper.
- TN-0029: reth's own OTLP exporters and `TraceArgs`; reth-tracing-otlp and the OTel log
  appender. TN wires its own OpenTelemetry layers.
- TN-0030: the reth.toml loaders; toml and three helpers.
- TN-0031: network-api keeps `NetworkInfo`/`PeersInfo`; reth-network-p2p, reth-network-types and
  reth-net-banlist go. TN now enables alloy `rand` itself (it was coming from network-types).
- TN-0032: engine-primitives without the engine-API message types; reth-payload-builder-primitives.
- TN-0033: the unreachable `mev` namespace server.

Left, with the reason:

- **`js-tracer` (decision needed).** It compiles boa, a JavaScript engine: 30 packages and the
  largest single cut left. It only serves JS tracer strings in `debug_trace*`, and `debug` is
  opt-in and off by default. Turning it off is one line in `crates/tn-reth/Cargo.toml` (drop
  `js-tracer` from reth-rpc, reth-rpc-eth-api and reth-rpc-eth-types). Native tracers
  (`callTracer`, `prestateTracer`, ...) keep working, a JS tracer request gets an error, and the
  JS-tracer DoS surface that TN-0017 hardened goes away. This changes RPC behavior for operators
  who enable `debug`, so it is the maintainers' call.
- After the tndb-b work: notify and its helpers (5 packages; the read-only static-file watcher,
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
  goes from 34 to 36. Do Phase 1 before attempting it: fewer crates, fewer conflicts.
- Security fixes in revm and alloy arrive through crates.io, independent of reth. Bump them in
  `vendor/reth/Cargo.toml` and the root `Cargo.lock`.

## Forking a crate off

When TN has changed most of a crate and upstream syncs no longer apply to it:

1. Record the decision in DECISIONS.md and CHANGES.md.
2. `vendor.py remove` it, move it into TN's workspace (`crates/`), rename it `tn-*`, give it
   TN's lints, and fix up the dependents.
3. Its upstream fixes become manual ports from then on. Note the last upstream commit it tracked.
