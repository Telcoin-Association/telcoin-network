# CHANGES: TN's changes to vendored reth

The ledger of every difference between `vendor/reth` and the pinned upstream
(`tn/upstream.toml`). `python3 -I vendor/reth/tn/vendor.py status` lists the files that differ;
each one has to be covered by an entry here. Changes to TN's own files (`tn/`, the four TN docs,
`.gitignore`) and to the filtered workspace manifest's member list are not listed.

Entry format:

- **TN-NNNN** — short title. *Kind*: build | test | fix | feature | removal | backport | sync.
  *Files*: what changed. *Why*: the reason, in a sentence or two. *Upstream*: worth offering
  upstream? (yes / no / n/a).

Use the id in code comments (`TN(TN-NNNN): ...`) where an edit would surprise a reader. Kept
newest last.

## Vendoring (base v1.11.3)

- **TN-0001** — Drop the `reth-e2e-test-utils` dev-dependency. *Kind*: test.
  *Files*: `crates/engine/tree/Cargo.toml` (dev-dependency, its `rocksdb` feature forward, the
  `e2e_testsuite` test target), `crates/ethereum/node/Cargo.toml` (dev-dependency); deleted
  `crates/engine/tree/tests/e2e-testsuite/`, `crates/ethereum/node/tests/e2e/`,
  `crates/ethereum/node/tests/assets/`.
  *Why*: `reth-e2e-test-utils` depends on `reth-cli-commands`, so carrying it would pull reth's
  whole CLI into the vendored set only for these integration tests. The crates' unit tests and
  `reth-node-ethereum`'s `tests/it` still run. See DECISIONS.md D5. *Upstream*: n/a.
  *Superseded*: both crates were removed in TN-0005, so nothing of this entry remains.

- **TN-0002** — Reformat two files with TN's pinned nightly rustfmt. *Kind*: build.
  *Files*: `crates/engine/tree/src/tree/payload_processor/mod.rs` (a `while let ... &&`
  chain), `crates/ethereum/primitives/src/receipt.rs` (import order).
  *Why*: CI runs `cargo +nightly fmt --all -- --check`, and `--all` covers path dependencies,
  so the vendored code is checked with TN's nightly (`rust-nightly`, 2026-03-20) under reth's
  `rustfmt.toml`. That nightly formats these two spots differently from the one reth used.
  When TN bumps its nightly, rerun `make fmt` and record any vendored reformatting the same
  way. *Upstream*: no. *Partly superseded*: engine-tree was removed in TN-0005; only the
  `receipt.rs` change remains.

- **TN-0003** — Fix a reth-trie unit test that does not compile upstream. *Kind*: test.
  *Files*: `crates/trie/trie/src/proof_v2/mod.rs` (`test_proof_calculator_reuse_after_error`).
  *Why*: upstream backported the test to the 1.11 branch (#22781) still naming `ProofV2Target`,
  the v2 name of the type that 1.11 calls `Target`, so `reth-trie`'s lib tests fail to build at
  v1.11.3 and v1.11.4. Found by building the nested workspace with `--all-targets`. *Upstream*:
  moot. The v1.11 line is finished and v2 has `ProofV2Target`.

## Reduction (after tag `reth-reduction-start`)

- **TN-0004** — Drop the engine-API tests that need `reth-node-ethereum`. *Kind*: test.
  *Files*: `crates/rpc/rpc-engine-api/src/engine_api.rs` (the `#[cfg(test)] mod tests`
  module), `crates/rpc/rpc-engine-api/Cargo.toml` (dev-dependency);
  `crates/rpc/rpc-builder/tests/it/utils.rs` (`launch_auth` and its imports),
  `crates/rpc/rpc-builder/tests/it/main.rs` (`mod auth`), deleted
  `crates/rpc/rpc-builder/tests/it/auth.rs`, `crates/rpc/rpc-builder/Cargo.toml`
  (dev-dependency).
  *Why*: these tests build reth's engine API around `reth-node-ethereum`'s
  `EthereumEngineValidator`, which needs `reth-ethereum-payload-builder`, so keeping them would
  keep the whole node-builder subgraph in the nested workspace. TN does not serve the engine
  API. rpc-builder's http, ws, startup, middleware and serde tests stay. *Upstream*: n/a.

- **TN-0005** — Remove 24 crates TN no longer depends on. *Kind*: removal.
  *Files*: removed from `tn/crates.txt` and the nested manifest with `vendor.py remove`:
  consensus/debug-client, engine/{invalid-block-hooks,service,tree,util}, era,
  era-downloader, era-utils, ethereum/{consensus,node,payload}, exex/{exex,types},
  net/downloaders, node/{builder,ethstats,events,metrics}, payload/validator, prune/prune,
  stages/{api,stages}, static-file/static-file, trie/parallel.
  *Why*: TN wires reth's components itself. Once `tn-reth` stopped importing through
  `reth-node-builder` and `reth-node-ethereum` (commit "tn-reth: stop depending on
  reth-node-builder and reth-node-ethereum"), nothing reaches these crates in any feature
  configuration. Features they used to enable that matter (the revm precompile backends,
  `asm-keccak`, `optional-balance-check`, chain-state `rayon`, trie-sparse `std`/`metrics`) are
  now set by `tn-reth` itself. *Upstream*: n/a.

- **TN-0006** — Take `reth-node-api` out of the RPC crates. *Kind*: removal.
  *Files*: `crates/rpc/rpc-eth-api/src/node.rs` (the blanket
  `impl<T: FullNodeComponents> RpcNodeCore for T`, imports, doc links),
  `crates/rpc/rpc-eth-api/src/helpers/{block,call,config}.rs` (imports),
  `crates/rpc/rpc-eth-api/Cargo.toml`; `crates/rpc/rpc/src/eth/core.rs` (the
  `EthRpcConverterFor`, `EthApiFor` and `EthApiBuilderFor` aliases), `crates/rpc/rpc/src/eth/mod.rs`,
  `crates/rpc/rpc/src/validation.rs` (imports from `reth-payload-primitives`),
  `crates/rpc/rpc/Cargo.toml`.
  *Why*: `reth-node-api` is the node builder's component API, and TN builds `EthApi` from
  `RpcNodeCoreAdapter`. The RPC crates used it only for re-exports (now taken from
  `reth-primitives-traits` and `reth-payload-primitives`) and for conveniences over
  `FullNodeComponents`. Without it, `reth-node-api` and `reth-basic-payload-builder` leave the
  graph. *Upstream*: no.

- **TN-0007** — Declare the `reth-chain-state` `serde` feature `reth-rpc` relies on. *Kind*: fix.
  *Files*: `crates/rpc/rpc/Cargo.toml`.
  *Why*: `reth_subscribeChainStateChanges` (`crates/rpc/rpc/src/reth.rs`) serializes
  `CanonStateNotification`, which needs `reth-chain-state/serde`. Upstream never declares it and
  gets it only because `reth-node-core` enables `reth-network/serde`, which reaches chain-state
  through feature unification. TN-0008 removes that path, so `reth-rpc` stops compiling.
  *Upstream*: yes, a real (latent) manifest bug. *Superseded*: TN-0021 removed `reth.rs`, so the
  line is back to upstream's.

- **TN-0008** — Remove the devp2p parts of `reth-node-core`. *Kind*: removal.
  *Files*: `crates/node/core/src/args/network.rs` (deleted: `NetworkArgs`, `DiscoveryArgs`),
  `crates/node/core/src/args/mod.rs`; `crates/node/core/src/node_config.rs` (the `network`
  field, `with_network`, `with_disabled_discovery`, `max_block`, `lookup_or_fetch_tip`,
  `fetch_tip_from_network`, `dev_mining_mode`; `dev()`, `adjust_instance_ports` and
  `with_unused_ports` now touch RPC only); `crates/node/core/src/cli/config.rs`
  (`RethNetworkConfig`); `crates/node/core/src/utils.rs` (`get_single_header`,
  `get_single_body`); `crates/node/core/Cargo.toml` (drops reth-network, reth-network-p2p,
  reth-discv4, reth-discv5, reth-net-nat, reth-net-banlist, reth-network-peers,
  reth-engine-local, reth-consensus, secp256k1, ipnet).
  *Why*: TN runs no devp2p network (its peers speak libp2p) and no dev-mode miner. Only
  `NodeConfig`'s network arguments kept the devp2p stack in the graph, and `tn-reth` filled them
  with disabled values. *Upstream*: no.

- **TN-0009** — Remove 10 crates TN no longer reaches. *Kind*: removal.
  *Files*: removed with `vendor.py remove`: node/api, payload/basic, net/{discv4,discv5,dns,
  ecies,eth-wire,nat,network}, engine/local.
  *Why*: after TN-0006 (reth-node-api out of the RPC crates) and TN-0008 (the devp2p parts of
  reth-node-core gone, tn-reth off reth-network/discv4/eth-wire), nothing in any feature
  configuration reaches them. 27 third-party crates left with them (discv5, hickory DNS,
  ed25519, a second libp2p-identity, moka, ...). *Upstream*: n/a.

- **TN-0010** — Remove the RPC pieces TN never serves: the engine-API auth server, `EngineEthApi`,
  flashbots `ValidationApi`, `TestingApi`. *Kind*: removal.
  *Files*: deleted `crates/rpc/rpc/src/{engine,testing,validation}.rs` and
  `crates/rpc/rpc-builder/src/auth.rs`; `crates/rpc/rpc/src/lib.rs`, `crates/rpc/rpc/Cargo.toml`
  (drops reth-rpc-engine-api, reth-consensus-common and nine dependencies only those modules
  used); `crates/rpc/rpc-builder/src/lib.rs` (`build_with_auth_server`, `create_auth_module`,
  `mod auth`), `crates/rpc/rpc-builder/src/config.rs` (`auth_server_config`,
  `auth_jwt_secret`, `flashbots_config`), `crates/rpc/rpc-builder/Cargo.toml` (two
  dev-dependencies the auth test used, see TN-0004).
  *Why*: TN serves only eth, net, web3, debug, trace and rpc (plus its own tn), never runs the
  engine API, and `tn-reth/src/traits.rs` requires that `ValidationApi` never be registered. The
  registry already mapped `flashbots` and `testing` to empty modules. `reth-rpc-engine-api`,
  `reth-payload-builder` and `reth-consensus-common` leave the graph. *Upstream*: no.

- **TN-0011** — Remove 3 crates TN no longer reaches. *Kind*: removal.
  *Files*: removed with `vendor.py remove`: rpc/rpc-engine-api, payload/builder,
  consensus/common.
  *Why*: after TN-0010 only the removed RPC modules reached them. *Upstream*: n/a.

- **TN-0012** — Remove `reth-primitives`. *Kind*: removal.
  *Files*: `crates/primitives` removed with `vendor.py remove`.
  *Why*: a re-export crate. Once `tn-types` and `tn-reth` took its items from
  `reth-primitives-traits` and `reth-ethereum-primitives` (with `tn-types` defining the `Block`,
  `BlockBody` and `SealedBlock` aliases it had provided), nothing reaches it. *Upstream*: n/a.

- **TN-0014** — Remove RocksDB: the backend, the `rocksdb`/`edge` features and the `rocksdb`
  crate. *Kind*: removal.
  *Files*: deleted `crates/storage/provider/src/providers/rocksdb/` (the real implementation);
  every `cfg(all(unix, feature = "rocksdb"))` item and every `cfg(feature = "edge")` item removed
  from `crates/storage/provider/src/{either_writer.rs, providers/database/{mod,provider}.rs,
  providers/{consistent,blockchain_provider,mod}.rs, providers/state/historical.rs,
  traits/rocksdb_provider.rs, test_utils/noop.rs}`, `crates/storage/db-common/src/init.rs` and
  `crates/storage/db-api/src/models/metadata.rs` (their `not(...)` twins became unconditional);
  the `rocksdb` and `edge` features removed from provider, storage-api, db-common, db-api and
  node-core, and the optional `rocksdb` dependency from provider and the workspace manifest.
  *Why*: maintainer decision (2026-10-30, DECISIONS.md D14): RocksDB is gone from TN in every
  configuration. Before, it was an off-by-default feature. Proof that nothing TN compiles
  changed: the macro-expanded source (`-Zunpretty=expanded`) of reth-provider, reth-db-common
  and reth-db-api is identical before and after, apart from the source line numbers `tracing`
  embeds. Cargo.lock loses rocksdb, librocksdb-sys, bzip2-sys and lz4-sys.
  *Still present*: the zero-size `RocksDBProvider` stub (`providers/rocksdb_stub.rs`) and the
  storage-v2 plumbing that takes it (ROADMAP.md, "RocksDB step B"). *Upstream*: no.

- **TN-0020** — reth-node-core's version info comes from the upstream pin, not TN's git state.
  *Kind*: build.
  *Files*: `crates/node/core/build.rs` (reads `commit` from `tn/upstream.toml` instead of running
  vergen-git2), `crates/node/core/Cargo.toml` (drops the `vergen-git2` build-dependency), the
  workspace manifest (drops `vergen-git2`).
  *Why*: inside TN's repository, upstream's build script described TN's git state and registered
  `rerun-if-changed` on TN's `.git/HEAD` and branch ref. So every TN commit or checkout rebuilt
  reth-node-core and everything that depends on it (reth-rpc-builder, tn-reth and the TN crates
  above it), and it compiled libgit2 from C. The pin is what this code actually is. The version
  strings now read `1.11.3-tn (2ac58a2)`: the pinned commit plus a `-tn` suffix, because TN carries
  changes on top. They are used only for the client version reth's database records on open, and
  for payload extra data, which TN does not build. git2, libgit2-sys, libz-sys, vcpkg and
  vergen-git2 leave the graph. *Upstream*: n/a.

- **TN-0021** — Remove the RPC namespaces TN never serves: admin, miner, otterscan, reth and
  txpool, plus the unused anvil, hardhat, engine, testing and validation traits. *Kind*: removal.
  *Files*: deleted `crates/rpc/rpc/src/{admin,miner,otterscan,reth,txpool}.rs` and
  `crates/rpc/rpc-api/src/{admin,anvil,engine,hardhat,miner,otterscan,reth,testing,txpool,validation}.rs`;
  `crates/rpc/rpc/src/lib.rs`, `crates/rpc/rpc-api/src/lib.rs`; `crates/rpc/rpc-builder/src/lib.rs`
  (`admin_api`, `register_admin`, `register_ots`, `register_reth`, `otterscan_api`, `reth_api`;
  the registry maps the five modules to empty method sets, like `flashbots` and `testing`);
  `crates/rpc/rpc-builder/tests/it/http.rs` (the admin and otterscan tests); the manifests of
  reth-rpc, reth-rpc-api and reth-rpc-builder (dependencies only those modules used;
  `revm-primitives` and `alloy-rpc-types-admin` become reth-rpc dev-dependencies for its unit
  tests).
  *Why*: TN's allowlist (`tn-reth/src/cli.rs`, `ALLOWED_MODULES`) drops these namespaces from any
  selection, so the code was compiled and never reachable. `RethRpcModule` keeps its variants, so
  operator lists that name them still parse and are warned about as before. About 2.3k lines go.
  `alloy-rpc-types-beacon` leaves the graph. `reth-chain-state`'s `serde` (TN-0007) went with
  `reth.rs`. With it, `serde` drops from reth-chain-state, reth-execution-types, reth-storage-api,
  reth-trie and parking_lot. That is no behavior change: it removes (de)serialization impls only,
  and reth-trie's optional `parking_lot` dependency, used only by its test mocks. tn-reth now
  declares reth-network-peers' `secp256k1` itself (for `Enr`), which it used to get from reth-rpc.
  *Upstream*: no.

- **TN-0025** — reth-node-core's build script no longer uses vergen. *Kind*: build.
  *Files*: `crates/node/core/{build.rs, Cargo.toml}`, the workspace `vergen` entry.
  *Why*: after TN-0020, vergen only supplied the build timestamp, the enabled features and the
  target triple. Cargo gives a build script the last two (`CARGO_FEATURE_*`, `TARGET`), and the
  timestamp is a few lines of date arithmetic (RFC 3339 UTC; `SOURCE_DATE_EPOCH` honored).
  vergen 9, vergen-lib, cargo_metadata, cargo-platform and derive_builder's three crates leave
  the build. The emitted `VERGEN_*`/`RETH_*` variables are unchanged, so `version.rs` is
  untouched. *Upstream*: no.

- **TN-0026** — reth-tasks uses `tracing::Instrument` instead of the `tracing-futures` crate.
  *Kind*: build. *Files*: `crates/tasks/{src/runtime.rs, Cargo.toml}`, the workspace entry.
  *Why*: the one import has the same trait in `tracing` itself, so `tracing-futures` leaves the
  build. *Upstream*: yes.

- **TN-0027** — reth-transaction-pool no longer depends on `aquamarine`. *Kind*: build.
  *Files*: `crates/transaction-pool/{src/lib.rs, src/pool/txpool.rs, Cargo.toml}`, the workspace
  entry.
  *Why*: aquamarine is a proc macro that renders one mermaid diagram in `TxPool`'s rustdoc
  (`#[cfg_attr(doc, ...)]`), yet it was compiled in every build, with `include_dir` and an old
  `itertools`. The diagram source stays in the doc comment as a code block. *Upstream*: no.

- **TN-0028** — reth-db-common keeps genesis initialization only. *Kind*: removal.
  *Files*: `crates/storage/db-common/src/{init.rs, lib.rs}`, `Cargo.toml`; deleted
  `src/db_tool/`; `crates/etl` removed from the vendored set.
  *Why*: TN calls only `init_genesis`. The state-dump import (`init_from_state_dump`, its JSONL
  parsers, `dump_state` and the two dump constants) and `DbTool` (the `reth db` CLI helper) were
  unused, and they were the only users of `reth-etl`, `boyer-moore-magiclen` (and its
  `debug-helper`), and of db-common's `reth-config`, `reth-fs-util`, `reth-node-types`, `eyre`,
  `serde` and `serde_json` dependencies. `compute_state_root`, which `init_genesis` uses, is
  unchanged. TN's snapshot restore never used this path (`tn-reth/src/snapshot.rs` explains
  why). *Upstream*: no.

- **TN-0029** — Remove reth's own OTLP exporters. *Kind*: removal.
  *Files*: `crates/tracing/{src/layers.rs, Cargo.toml}` (`with_span_layer`, `with_log_layer`,
  the `otlp`/`otlp-logs` features and the default), `crates/node/core/{src/args/mod.rs,
  Cargo.toml}` (deleted `src/args/trace.rs`, `TraceArgs`); `crates/tracing-otlp` removed from
  the vendored set. Outside: `crates/tn-reth/Cargo.toml` stops enabling `otlp`/`otlp-logs`.
  *Why*: TN adds its own OpenTelemetry layers (`telcoin-network-cli/src/open_telemetry.rs`) and
  never used `TraceArgs` (reth's `--tracing-otlp*`/`--logs-otlp*` flags) or the two `Layers`
  methods, so reth-tracing-otlp and `opentelemetry-appender-tracing` were compiled for nothing.
  `opentelemetry` and `opentelemetry_sdk` lose only their `default` marker feature: every
  feature it implied stays enabled through opentelemetry-otlp. *Upstream*: no.

- **TN-0030** — Remove the TOML config loaders. *Kind*: removal.
  *Files*: `crates/config/{src/config.rs, Cargo.toml}` (`Config::from_path`, `Config::save`, the
  `EXTENSION` constant and the tests that load or save files), `crates/node/core/{src/node_config.rs,
  Cargo.toml}` (`NodeConfig::load_path`; no `toml` or `serde` dependency, no `reth-config/serde`).
  *Why*: TN never reads or writes a reth.toml (`etc/archive-mode-guard.sh` refuses a loader in TN's
  own code, since a `[prune]` section would break pinned reads). With the loaders gone, `toml`,
  `toml_datetime`, `serde_spanned` and `toml_writer` leave the build; toml stays a dev-dependency
  for reth-config's backwards-compatibility parsing tests. *Upstream*: no.

- **TN-0031** — reth-network-api keeps only what reth's RPC uses. *Kind*: removal.
  *Files*: `crates/net/network-api/{src/lib.rs, src/noop.rs, Cargo.toml}` (deleted
  `src/{downloaders.rs, events.rs, test_utils/}`), `crates/config/{src/config.rs, Cargo.toml}`
  (the `peers` and `sessions` sections, `peers_config_with_basic_nodes_from_file` and their
  tests), `crates/rpc/rpc-builder/src/lib.rs` (`Peers` bounds become `PeersInfo`);
  `crates/net/{p2p, network-types, banlist}` removed from the vendored set. Outside:
  `crates/tn-reth/src/worker.rs` drops its no-op `Peers` impl, and the root `Cargo.toml` enables
  alloy's `rand` feature.
  *Why*: after TN-0021 removed the admin namespace, reth's RPC bounds only on `NetworkInfo`
  (`eth_syncing`, `eth_chainId`, `web3_clientVersion`, `eth_protocolVersion`) and `PeersInfo`
  (`net_peerCount`). `FullNetwork`, `Peers`, `PeerInfo`, the event and downloader traits and
  `NoopNetwork`'s impls of them were the only users of reth-network-p2p, reth-network-types and
  reth-net-banlist, and reth-config's devp2p sections the only other user of network-types.
  `NetworkStatus` and alloy-rpc-types-admin stay (`network_status`).
  *Feature check*: reth-network-types was what enabled alloy-primitives' `rand` feature, which
  gives alloy's maps a randomly seeded hasher (hash-flooding resistance) and `Address::random*`
  (TN's committee test builder uses it). Without it the graph silently lost the feature and the
  test builder stopped compiling, so TN now requests it directly (`alloy` `rand`); `graph.py diff`
  then shows no third-party feature change. *Upstream*: no.

- **TN-0032** — reth-engine-primitives without the engine-API message types. *Kind*: removal.
  *Files*: `crates/engine/primitives/{src/lib.rs, Cargo.toml}` (deleted `src/message.rs`; no
  tokio or futures); `crates/payload/builder-primitives` removed from the vendored set.
  *Why*: `ConsensusEngineHandle`, `BeaconEngineMessage` and `OnForkChoiceUpdated` carry
  engine-API requests into reth's engine tree. TN carries neither (Phases 1 and 6a), nothing
  referenced them, and they were the only user of reth-payload-builder-primitives. The constants,
  `TreeConfig`, `EngineTypes`, `PayloadValidator` and `ConsensusEngineEvent` that TN, node-core
  and the RPC crates use stay. *Upstream*: no.

- **TN-0033** — Remove the `mev` namespace server (`mev_simBundle`). *Kind*: removal.
  *Files*: deleted `crates/rpc/rpc/src/eth/sim_bundle.rs` and `crates/rpc/rpc-api/src/mev.rs`;
  `crates/rpc/rpc/src/eth/mod.rs`, `crates/rpc/rpc-api/{src/lib.rs, Cargo.toml}` (no
  alloy-rpc-types-mev), `crates/rpc/rpc-builder/src/lib.rs` (the `Mev` arm joins the empty arms).
  *Why*: the same reasoning as TN-0021. TN's allowlist never selects `mev`, so `EthSimBundle` and
  the `MevSimApi`/`MevFullApi` traits were unreachable. `eth_callBundle` is unaffected: it is
  `EthBundle`, registered with `eth` and wrapped by TN's epoch-fee adapter. `RethRpcModule::Mev`
  stays, so operators' module lists still parse. *Upstream*: no.

## Upstream syncs

- **TN-0013** — Sync to upstream v1.11.4 (`2ac58a25f561`). *Kind*: sync.
  *Files*: `tn/upstream.toml` only.
  *Why*: `vendor.py sync v1.11.4` found no change in the vendored scope. The release changed only
  `crates/engine/tree/src/tree/precompile_cache.rs`, which TN no longer carries (TN-0005). The pin
  moves so later syncs start from v1.11.4. *Upstream*: n/a.

## Backports (upstream commits applied on top of the pin)

Applied with `vendor.py pick <sha>`. The next `vendor.py sync` past each commit should merge
cleanly; if it conflicts, take upstream's side for the files listed here.

- **TN-0015** — Backport 997af404a: trim spaces in CORS domain parsing. *Kind*: backport.
  *Files*: `crates/rpc/rpc-builder/src/cors.rs`.
  *Why*: `--http.corsdomain "https://a, https://b"` kept the leading space on the second origin,
  so it never matched and a `"https://a, *"` list was not rejected. It is also the base the
  WebSocket origin fix (TN-0016) is written against. *Upstream*: n/a (upstream commit).

- **TN-0016** — Backport ff2657600: enforce WebSocket origin allowlists. *Kind*: backport
  (security).
  *Files*: `crates/rpc/rpc-builder/src/{cors.rs, lib.rs}`, `tests/it/startup.rs` (upstream's
  `test_ws_upgrade_origin_allowlist`), `Cargo.toml` (`reqwest` dev-dependency, which upstream
  added in 39f078e40).
  *Why*: `--ws.origins` only set CORS response headers. Browsers don't apply CORS to WebSocket
  upgrades, so any web page could open a WS connection to a node's RPC (cross-site WebSocket
  hijacking). A `WsOriginValidator` layer now answers 403 to an upgrade whose `Origin` is not on
  the list. Requests without an `Origin` header (non-browser clients) still pass, and plain HTTP
  is unchanged. Merge conflict: TN's tree lacks upstream's HTTP compression-algorithm argument,
  so the new layer sits after `maybe_compression_layer(self.http_disable_compression)`.
  Verified: with the validator disabled, the test fails (101 Switching Protocols where 403 is
  expected). *Upstream*: n/a (upstream commit).

- **TN-0017** — Backport 57825d41d: interrupt JS tracers on timeout and drop. *Kind*: backport
  (security), adapted.
  *Files*: `crates/rpc/rpc/src/debug.rs` (`JsTraceInterrupt` and the three call sites, as upstream;
  plus `TraceInspector` and tests), new `crates/rpc/rpc/src/js_tracer_interrupt.rs` (TN-owned; the
  `js_interrupt` submodule of `debug`, kept out of `debug/` because TN's root `.gitignore` ignores
  that name), `crates/rpc/rpc/Cargo.toml` (optional `humantime` as upstream; tokio `time`).
  *Why*: a JS tracer in `debug_traceTransaction`/`debug_traceCall` ignored its `timeout` and kept
  running on a blocking thread after the client dropped the request, until it finished or the node
  ran out of memory (upstream #27543). TN builds with `js-tracer` and serves `debug` when an
  operator names it (off by default). Now the timeout is honored (geth's 5 s default; expiry
  returns `ExecutionTimedOut`) and a timed-out or dropped request stops the trace at the next
  inspector hook.
  *Adaptation*: upstream uses `JsInspectorInterrupt` from revm-inspectors 0.44.2, which needs
  revm 43. `js_tracer_interrupt.rs` ports the same mechanism (a shared flag checked at every hook,
  which halts the EVM with "JavaScript tracing interrupted") as a wrapper around `DebugInspector`,
  applied only to JS tracers. Upstream's tests are ported, plus one that interrupts a trace already
  running on another thread: it stops within milliseconds, but runs over 60 s to gas exhaustion with
  the interrupt disabled. When revm-inspectors reaches 0.44.2, delete `js_tracer_interrupt.rs` and
  take upstream's `attach` (`js.with_interrupt(...)`). Block-level traces and `debug_traceCallMany`
  are unchanged, as upstream: they ignore `timeout`, and a request dropped after the trace has
  started does not stop it. (Upstream's commit message says they "stop only between
  transactions"; the code checks for a dropped request only before the blocking task starts.)
  *Upstream*: n/a (port of an upstream commit).

- **TN-0018** — Backport 792ee9245: no sender-id allocation on read-only pool lookups. *Kind*:
  backport (security).
  *Files*: `crates/transaction-pool/src/{lib.rs, pool/mod.rs}` (with upstream's test).
  *Why*: the pool's sender-id map is never pruned, and read-only queries allocated an entry for
  every unknown address. `eth_getTransactionCount(addr, "pending")` reaches
  `get_highest_transaction_by_sender`, so unauthenticated RPC calls with random addresses grew
  node memory without bound. Lookups now use a non-allocating `sender_id`. *Upstream*: n/a
  (upstream commit).

- **TN-0019** — Backport upstream's RPC blocking-work limits. *Kind*: backport (security).
  *Why*: TN runs consensus and RPC on one tokio runtime, and the execution engine
  (`crates/engine`, `spawn_blocking_task`) and certificate validation (`spawn_blocking`) use the
  same blocking pool as RPC. RPC methods that occupied blocking threads without taking a permit
  from the blocking-IO semaphore, or that released it when the client disconnected, let
  unauthenticated requests pin an unbounded number of pool threads and delay block execution.
  Applied in upstream order, one commit each:
  - b7a7a8a72: `eth_callMany` takes a blocking-IO permit (`rpc-eth-api/src/helpers/call.rs`).
  - f2d2bd233: `eth_simulateV1` takes a blocking-IO permit (same file).
  - de401d9b4: blocking-IO tasks and `eth_getLogs` range scans skip or stop work once the request is
    dropped (`blocking_task.rs`, `rpc/src/eth/filter.rs`). Conflicts only from TN's tree wrapping
    the tasks in `Box::pin` (v1.11's `TaskSpawner` takes a `BoxFuture`). `tokio::select!`
    relies on the workspace enabling tokio `macros`, as upstream does.
  - 1f94488d1: `eth_getLogs` range scans take a blocking-IO permit and hold it inside the task
    (`rpc/src/eth/filter.rs`, with upstream's test adapted to TN's `TokioTaskExecutor`).
  - e63ec720a: `eth_simulateV1`, `eth_callMany` and `eth_call` (`transact_call_at`) hold their
    permit inside the blocking task, so a client that disconnects no longer frees it while the work
    runs on. Conflict in `eth_simulateV1`, whose body predates upstream's refactor: TN's body kept,
    permit moved into its closure. Upstream's `bal.rs` change does not apply (no such file).
  - e88a669b1: `eth_getProof` holds its tracing permit inside the blocking task
    (`rpc-eth-api/src/helpers/state.rs`). Ported by hand: TN's tree has only the one proof method;
    upstream's second site (the v2 multiproof) does not exist here.

- **TN-0022** — Backport 47544d9a7: add transactions to the pending subpool in nonce order.
  *Kind*: backport.
  *Files*: `crates/transaction-pool/src/pool/txpool.rs` (with upstream's three tests).
  *Why*: when a new transaction promoted a same-sender transaction with a lower nonce (for
  example by unlocking balance), the pool added the new one first, so a live `BestTransactions`
  iterator saw nonce N+1 before N. TN's batch builder iterates a live `best_transactions()`, so
  a batch could get a nonce gap. Updates below the new nonce are now promoted first, then the new
  transaction, then the rest. Two of the three tests fail without the fix; the gap-fill case
  was already right. *Upstream*: n/a (upstream commit).

- **TN-0023** — Backport upstream's static-file fixes. *Kind*: backport.
  *Why*: TN (storage v1) keeps headers, transactions and receipts in reth's static files, heals
  them with `check_consistency` at every startup, and reads them while persistence appends. These
  fixes cover cache races, recovery and torn tails in that code. Static files are outside the
  tndb-b work, which replaces MDBX and RocksDB. Applied in upstream order, one commit each:
  - b629142d0: a jar loaded on a cache miss no longer overwrites an entry that `update_index`
    published meanwhile, so readers stop seeing a stale row count after an append
    (`provider/src/providers/static_file/manager.rs`).
  - 01d039b1f: re-initializing the index (after a heal, prune or unwind) bumps a cache
    generation, and an in-flight load re-checks it under the entry's shard lock, so a jar loaded
    before the invalidation cannot repopulate the cache (same file, with upstream's two tests).
  - 1e17affd3: startup recovery of the transaction-based segments removes row 0 when the
    database checkpoint is an empty genesis block. `last_tx_num()` saturated to 0 there, so the
    first block's first transaction survived the heal and the next append failed. TN's genesis
    is empty, so a crash between the static-file and MDBX commits of block 1 hit this. Same
    file; upstream's test fails without the fix.
  - 0d1324f14: `prune_rows` on a writer holding uncommitted rows now commits the surviving rows'
    offsets before writing the config (or drops the stale in-memory offset when all were
    pruned), so a crash cannot leave a config that claims rows whose offsets are not on disk
    (`nippy-jar/src/{writer.rs, lib.rs}`; upstream's two tests fail without the fix).
  - 22e3708b2: the last value of a jar ends at its final offset, not at the end of the data
    file, so reading the tip row while persistence appends uncommitted rows no longer returns
    extra bytes; offsets read through a stale mmap during a prune return `InconsistentState`
    instead of panicking on a bad slice (`nippy-jar/src/{cursor.rs, lib.rs}`,
    `provider/src/providers/static_file/mod.rs`; upstream's three tests fail without the fix).
  - 95679ace2: static-file read and decompression failures are returned as errors instead of
    `Ok(None)`, so a corrupt row no longer looks like a missing block
    (`db/src/static_file/cursor.rs`, `provider/src/providers/static_file/mod.rs`). The conflict
    was TN's older spelling of the same expression (`map_or(None, |v| v)`); upstream's line
    taken. Upstream's test adapted to v1.11's `read_only(path, watch_directory)`; it fails
    without the fix.

- **TN-0024** — Backport d3d7fb31d: round the replacement price bump up. *Kind*: backport.
  *Files*: `crates/transaction-pool/src/{validate/mod.rs, pool/txpool.rs}` (with upstream's
  test).
  *Why*: the required bump was `fee * (100 + bump) / 100`, which rounds down, so below 10 wei
  the 10% bump was 0 and an equal-price replacement was accepted. TN's base fee floor is 7 wei.
  The fix only tightens local pool admission (`AllTransactions::insert_tx` is the one caller);
  batch validation and execution never apply replacement rules, so nodes with and without it
  agree on every batch and block. Upstream's test fails without the fix. *Upstream*: n/a
  (upstream commit).

- **TN-0034** — Backport d85d9deae: end a `syncing` subscription when its client leaves.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/eth/pubsub.rs`, `crates/rpc/rpc-builder/tests/it/ws.rs`
  (upstream's two tests).
  *Why*: the `eth_subscribe("syncing")` task noticed a closed client only when a send failed, and
  it sends only when the sync status changes. In TN that is never for an observer, and only on a
  validator's active/inactive switch. Each abandoned subscription kept two tasks and a broadcast
  receiver that woke on every executed block, and the connection's subscription slot was never
  released after `eth_unsubscribe`. The task now also waits on the sink closing. Reachable over
  WebSocket and IPC. Conflict only in the tests: upstream moved the test imports to the module
  top and uses the newer `reth_tasks::Runtime`; TN's tree has v1.11's `TokioTaskExecutor`. Both
  tests fail without the fix (50 tasks outlive the connection; the second subscription is
  refused). *Upstream*: n/a (upstream commit).

- **TN-0035** — Port the chunk permits from 055fafc72: parallel `eth_getLogs` receipt fetches
  take their own blocking-IO permit. *Kind*: backport (security), partial.
  *Files*: `crates/rpc/rpc/src/eth/filter.rs` (`spawn_parallel_tasks`, new
  `fetch_chunk_receipts`, two tests).
  *Why*: TN-0019 gave the outer range scan a permit, but once 1,000 or more bloom-matching
  headers remained, the scan started its receipt chunks with bare `spawn_blocking` while its own
  thread waited, so one permitted scan could occupy five blocking threads (the chunk size
  rounded down, so the remainder became a fifth chunk). Each chunk now takes a permit with
  `try_acquire_owned` and runs on the scanning task when none is free (an awaited acquire could
  deadlock while outer scans hold every permit), and the chunk size rounds up, so a scan uses at
  most `DEFAULT_PARALLEL_CONCURRENCY` extra threads, each paid for. Only the permit and chunk
  sizing are ported. The rest of 055fafc72 rewrites the range scan to go window by window, a
  performance change built on helper signatures newer than v1.11.4; it conflicts in three
  places and is left out. The permit test fails without the fix (five chunks, no permits
  taken); the other pins the no-permit path, which would hang with an awaited acquire.
  *Upstream*: n/a (upstream commit). A later sync takes upstream's version of the function.
