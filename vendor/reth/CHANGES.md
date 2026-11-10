# CHANGES: TN's changes to vendored reth

The ledger of every difference between `vendor/reth` and the pinned upstream
(`tn/upstream.toml`). `python3 -I vendor/reth/tn/vendor.py status` lists the files that differ;
each one has to be covered by an entry here. Changes to TN's own files (`tn/`,
`.config/nextest.toml`, the five TN docs, `.gitignore`) and to the filtered workspace manifest's
member list are not listed.

Entry format:

- **TN-NNNN** — short title. *Kind*: build | test | fix | feature | removal | backport | sync.
  *Files*: what changed. *Why*: the reason, in a sentence or two. *Upstream*: worth offering
  upstream? (yes / no / n/a).

Use the id in code comments (`TN(TN-NNNN): ...`) where an edit would surprise a reader. Grouped
by section, newest last within each.

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
  *Later*: TN-0010 and TN-0011 removed `rpc-engine-api`, so only the rpc-builder edits remain.

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
  `crates/rpc/rpc/src/validation.rs` (imports from `reth-payload-primitives`; the file was
  deleted later by TN-0010), `crates/rpc/rpc/Cargo.toml`.
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
  *Still present*: the zero-size `RocksDBProvider` stub (`providers/rocksdb_stub.rs`; only its
  module comment changed) and the storage-v2 plumbing that takes it (ROADMAP.md, "RocksDB step
  B"). *Upstream*: no.

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
  `src/db_tool/`; `crates/etl` removed from the vendored set; the workspace manifest's
  `boyer-moore-magiclen` entry.
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
  ran out of memory (upstream #27543). TN compiles JS tracers only with its opt-in `js-tracer`
  feature (D21) and serves `debug` only when an operator names it. Now the timeout is honored (geth's 5 s default; expiry
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
  thread waited, so one permitted scan could occupy up to six blocking threads: its own and five
  chunk threads (the chunk size rounded down, so a remainder became a fifth chunk). Each chunk now takes a permit with
  `try_acquire_owned` and runs on the scanning task when none is free (an awaited acquire could
  deadlock while outer scans hold every permit), and the chunk size rounds up, so a scan uses at
  most `DEFAULT_PARALLEL_CONCURRENCY` extra threads, each paid for. Only the permit and chunk
  sizing are ported. The rest of 055fafc72 rewrites the range scan to go window by window, a
  performance change built on helper signatures newer than v1.11.4; it conflicts in three
  places and is left out. The permit test fails without the fix (five chunks, no permits
  taken); the other pins the no-permit path, which would hang with an awaited acquire.
  *Upstream*: n/a (upstream commit). A later sync takes upstream's version of the function.

- **TN-0038** — Backport a774920b7: size the `block_range` buffer for the inclusive span.
  *Kind*: backport.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`block_range`).
  *Why*: the `Vec` for an inclusive block range was allocated one short, so every multi-block read
  reallocated once. Capacity only: the blocks returned are unchanged, so it cannot reach
  execution or storage. *Upstream*: n/a (upstream commit).

- **TN-0039** — Backport 70e776bfe: find a block's static-file range with a B-tree range lookup.
  *Kind*: backport.
  *Files*: `crates/storage/provider/src/providers/static_file/manager.rs`
  (`StaticFileProviderInner::find_fixed_range_with_block_index`).
  *Why*: the lookup walked every file range of the segment for each block; `range(block..)` takes
  the first range whose `max_block` is at or above the block, the same answer in O(log n). It
  picks which file a block is read from or appended to, so it is on the storage path, but the
  result is identical by construction (the index is a `BTreeMap` keyed by `max_block`).
  *Upstream*: n/a (upstream commit).

- **TN-0040** — Backport 7839f3d87: keep one `.csoff` reader per loaded changeset jar.
  *Kind*: backport.
  *Files*: `crates/static-file/types/src/{changeset_offsets.rs, lib.rs}` (the reader reads with
  `read_exact_at` through `&self`; the module is `unix`-only), `crates/storage/provider/src/
  providers/static_file/{mod.rs, jar.rs, writer.rs, writer_tests.rs}` (`LoadedJar` opens the
  sidecar once, the jar provider uses it), `.../static_file/manager.rs` (two `let mut reader`
  that the `&self` reader no longer needs; upstream had already moved that code).
  *Why*: every changeset lookup (each storage v2 historical read that resolves through a
  changeset, and each changeset walk) did a `stat` and an `open` of the sidecar. The reader is
  now opened when the jar is loaded, which `update_index` redoes after each commit, so its length
  is the committed one, as before. A changeset jar whose header counts offsets but has no sidecar
  now fails at load instead of reading as "no offsets". Read path only: the bytes read are the
  same, so it cannot change execution or what is written. TN builds for Linux and macOS only,
  both `unix`. *Upstream*: n/a (upstream commit).

- **TN-0041** — Backport a195b777e: storage v2's `write_state` skips the plain-state conversion
  when nothing it produces goes to the database. *Kind*: backport, adapted.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`write_state`, new
  `write_bytecodes`, used by `write_state_changes` too).
  *Why*: under v2, `save_blocks` sends receipts and both changesets to static files and v2 keeps
  no plain state, so the per-block `to_plain_state_and_reverts` (a walk and copy of every changed
  account and slot) fed only the bytecode writes. The early return writes the bytecodes
  directly. Adapted: upstream writes every entry of the bundle's `contracts`, while
  `to_plain_state` drops the empty bytecode (`KECCAK_EMPTY`); TN keeps that filter, so the
  `Bytecodes` table is the same on both paths. Storage path, v2 only: the condition needs
  `use_hashed_state()`, which is false for every v1 datadir, so v1 writes are unchanged; genesis
  and other `StateWriteConfig::default()` callers still convert. *Tests*:
  `v2_matches_v1_on_a_generated_chain` (TN-0045) puts the empty bytecode in a bundle and requires
  `Bytecodes` to equal v1's; it fails without the filter. *Upstream*: yes, the `KECCAK_EMPTY`
  filter.

- **TN-0102** — Backport 345fbbbfd: the in-memory trie cursor skips the database seek on an
  exact overlay hit. *Kind*: backport.
  *Files*: `crates/trie/trie/src/trie_cursor/in_memory.rs` (`InMemoryTrieCursor`: a
  `DbCursorState` replaces `cursor_wiped`/`cursor_entry`; `seek_exact` returns an exact in-memory
  hit without touching the database cursor and marks it for a re-seek when it was behind; the
  `seeked` flag is debug-only).
  *Why*: the state root walks the account and storage tries through `InMemoryTrieCursor`, and TN's
  output overlay stacks one per run of the blocks already built in the output (`RunTrieCursor` in
  `crates/tn-reth/src/env/output_overlay/cursors.rs`). Every `seek_exact` seeked the layer below
  first, down to the database, even when the overlay held the key; at a peak output the state root
  does about 80k `StoragesTrie` exact seeks. Execution path (state root): the cursor returns the
  same entries in the same order, only fewer lower-layer seeks. *Tests*: reth-trie (the in-memory
  cursor's unit tests and `proptest_in_memory_trie_cursor`, against a reference merge; upstream
  added the assertion that an exact hit leaves the database cursor untouched), reth-trie-db's
  state-root tests, TN's `trie_overlay` tests and the exec-bench chain differential (TN-0105).
  *Upstream*: n/a (upstream commit).

- **TN-0103** — Backport 75c327ee5: the hashed post-state cursor and the in-memory trie
  cursor's `seek` skip the database seek on an exact overlay hit. *Kind*: backport.
  *Files*: `crates/trie/trie/src/hashed_cursor/post_state.rs` (`HashedPostStateCursor`: the same
  `DbCursorState` as TN-0102; `seek` returns an exact non-deleted in-memory hit without touching
  the database cursor), `crates/trie/trie/src/trie_cursor/in_memory.rs` (`seek` does the same for a
  present node).
  *Why*: TN-0102 covered the trie cursor's `seek_exact` only. The state root also seeks the
  hashed accounts and storages through `HashedPostStateCursor`, which TN stacks per run of the
  output overlay (`RunHashedCursor`) with the current block's on top; at a peak output that is
  about 125k `HashedStorages` seeks. An exact hit with a value is the first entry at or after the
  key, so the lower layers are seeked lazily, by the next `next`. Execution path (state root):
  same entries in the same order. *Tests*: as TN-0102; upstream added cursor tests that an exact
  hit leaves the database untouched until `next` and that a stale database position is
  re-seeked. *Upstream*: n/a (upstream commit).

- **TN-0104** — Backport 0720dcd37: fast path for forward in-memory cursor seeks. *Kind*:
  backport.
  *Files*: `crates/trie/trie/src/forward_cursor.rs` (`ForwardInMemoryCursor::seek` and
  `first_after` return the current entry when it already answers; the linear scan indexes the
  slice directly; binary search from 128 remaining entries instead of 64).
  *Why*: the overlay cursors of TN-0102/TN-0103 seek their sorted in-memory entries for every
  trie and hashed-state seek, mostly to a key at or just after the current entry. The answer is
  the same entry as before in every case (the old `advance_while` returned the current entry
  without moving when it already answered), so this changes only the work. Execution path (state
  root), no change in results. *Tests*: reth-trie's `forward_cursor` tests and as TN-0102.
  *Upstream*: n/a (upstream commit).

- **TN-0105** — Backport 0a21d223a: the state root reuses one storage trie cursor and one
  hashed storage cursor for every account. *Kind*: backport.
  *Files*: `crates/trie/trie/src/trie.rs` (`StateRoot::calculate` keeps the two storage cursors
  across accounts and repositions them with `set_hashed_address`; new
  `StorageRoot::calculate_with_cursors`, `calculate_with_cursor` and `StorageRootCalculation`;
  the factories no longer need `Clone`). It merged with the pin's `instrument` attribute on
  `StorageRoot::calculate`, which the pin and upstream's parent spell differently.
  *Why*: the state root built a fresh `StorageRoot`, and so two fresh cursor stacks, for every
  account with changed storage. Under TN's output overlay each stack is one boxed layer per run
  plus the database cursors and `OverlayStorageCursor` (`crates/tn-reth/src/env/output_overlay/`),
  so each root opened thousands of database cursors (measured below). TN's cursor types implement `set_hashed_address` by
  passing it down every layer (`RunHashedCursor`, `RunTrieCursor`, `OverlayStorageCursor`, which
  also switches its own address), and reth's overlay cursors reset their position and their
  in-memory view for the new account. Execution path (state root): the same walk over the same
  entries, only the cursors are reused. *Tests*: as TN-0102; TN's `trie_overlay` differential
  tests compare the layered root against a flattened reference over several accounts with
  storage, so a cursor that kept the previous account's position or overlay would fail them.
  *Measured* (TN-0102 to TN-0105 together; exec bench, tn-lmdb, storage v2, 40 outputs of 8
  batches, prefill 50k, the same cached base, on a Mac at load ~70): the chain is identical (the
  bench's differential, 3000 reads of headers, balances, storage, transactions and receipts, 0
  mismatches against a run without them; the same tip hash). Per 320 state roots, counted at the
  database trie cursors with temporary counters: `AccountsTrie` seeks 195.8k to 83.1k, `StoragesTrie` seeks 212.6k to
  109.3k, `HashedStorages` cursors opened 1.89M to 640; hashed seeks and nexts unchanged. The
  state root's mean fell from 100.5 to 84.7 ms on that loaded machine (a quiet run decides).
  *Upstream*: n/a (upstream commit).

## TN fixes (not from an upstream commit)

- **TN-0036** — Debug traces hold their tracing permit until their blocking task ends. *Kind*:
  fix (security).
  *Files*: `crates/rpc/rpc/src/debug.rs` (the six `debug_trace*` handlers, the methods behind
  them, and `trace_permit_tests`), `crates/rpc/rpc/src/js_tracer_interrupt.rs` (module comment
  only), `crates/storage/provider/src/test_utils/mock.rs` (test support: the mock hunk of upstream
  dfcb10454, so `MockEthProvider::recovered_block` returns the blocks it stores).
  *Why*: the RPC handlers held the permit only while the request was open. A trace that timed out
  (a JS tracer's `timeout`, TN-0017) or lost its client returned its permit while its blocking
  thread ran on, and the TN-0017 interrupt cannot stop a JS callback, `setup` or `result` mid-run
  (review finding T1-2). Repeated requests could therefore occupy any number of tokio's blocking
  threads, which block execution and certificate validation also use. Each trace method now takes
  the permit once, where the handler used to (before its block and transaction lookups), and moves
  it into its blocking closure, as e63ec720a does for `eth_call` (TN-0019). At most
  `--rpc.max-tracing-requests` threads run debug traces, in builds with and without `js-tracer`.
  The handlers no longer acquire, so no request takes a second permit, which would wait forever
  with a limit of 1. A request dropped before its task starts still frees the permit, when the task
  sees the closed channel. Callers of the public trace methods must not hold a tracing permit.
  Unchanged, as upstream: `debug_executionWitness*` (not a trace) and the `trace_*` namespace hold
  their permit only while the request is open.
  *Tests*: `trace_permit_tests` locks the mock's account store so a trace's thread stays inside
  its closure, with one permit. For each of the seven trace paths it drops the request mid-trace,
  and with `js-tracer` it lets a JS tracer time out; a second trace must not start a blocking task
  until the first one ends. Both tests fail before the change ("the second trace started a
  blocking task while the first one's still runs"). *Upstream*: yes; upstream main still releases
  these permits on timeout and disconnect.

- **TN-0037** — `eth_createAccessList` and `eth_callBundle` hold a blocking-IO permit inside
  their blocking task. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`create_access_list_with` takes the
  permit; `create_access_list_at` calls it directly instead of from a second blocking task, as the
  matching hunk of upstream 4630cc588 does), `crates/rpc/rpc/src/eth/bundle.rs`
  (`EthBundle::call_bundle`, and the TN-owned `blocking_io_permit_tests` module).
  *Why*: both ran the EVM on tokio's blocking pool with no permit (review finding T1-4), and
  `EthBundle`'s tracing guard is unused upstream. TN's `eth_createAccessList` and `eth_callBundle`
  wrappers delegate to these helpers, which own the blocking task, so the permit has to be taken
  here to live inside it. It is the blocking-IO permit `eth_call` and `eth_callMany` take. Without
  the `create_access_list_at` change, reth's own handler would hold a blocking thread while it
  waits for the permit. TN's `eth_estimateGas` and `eth_fillTransaction` take theirs in `tn-reth`
  (`estimate_gas_at_epoch`, which owns its task). Callers must not hold a blocking-IO permit.
  *Tests*: in `tn-reth`, `test_rpc_simulation_waits_for_blocking_io_permit` (all four methods wait
  with no permits; fails before the change) and `test_rpc_simulation_shares_one_blocking_io_permit`
  (with one permit, concurrent requests all complete). In reth-rpc, `bundle.rs`'s
  `blocking_io_permit_tests` drop each request while its blocking task is stalled on the account
  store and check that the permit stays taken until the task ends; both fail when the permit is
  held by the request instead of the task. *Upstream*: yes.

- **TN-0042** — Storage v2 writes `TransactionHashNumbers` to the database table. *Kind*: fix.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`save_blocks`).
  *Why*: TN-0014 removed `RocksDB`, which wrote this table under v2, but `save_blocks` still
  skipped the database write under v2, so a v2 node could not find a transaction by hash. The
  sorted batch now goes to the table in both layouts, in the `save_blocks` transaction. Every
  reader (`transaction_id`) and the unwind (`remove_blocks_above`) already used the table in both
  layouts (`EitherReader`/`EitherWriter` route it to the database only). Storage path, v2 only:
  the v1 write is unchanged. *Tests*: `v2_matches_v1_on_a_generated_chain` and
  `v2_history_unwind_round_trip` (TN-0045) look every transaction up by hash, and the unwound ones
  must be gone; the first fails with the v2 gate restored. *Upstream*: n/a (upstream writes it to
  `RocksDB`).

- **TN-0043** — Storage v2 writes and unwinds `AccountsHistory` and `StoragesHistory` in the
  database tables. *Kind*: fix.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`save_blocks`, new
  `HistoryTransitions`, `ensure_wiped_accounts_had_no_storage` and `insert_history_transitions`,
  `update_history_indices`, `unwind_account_history_indices`, `unwind_storage_history_indices`,
  `append_blocks_with_state`).
  *Why*: TN-0014 removed `RocksDB`, which held both history indices under v2, and left empty
  `if storage_v2 {}` arms where it wrote and unwound them, so a v2 node had no history: every
  historical account or storage read past the latest block fell back to "not yet written".
  Re-enabling `update_history_indices` alone does not work, because under v2 it reads the
  changesets back from static files, and those show only what earlier transactions committed.
  - Under v2, `save_blocks` builds the block lists from each block's in-memory reverts, with the
    same filter the static-file changeset writers use, so history has an entry exactly where a
    changeset row exists. It writes them with `insert_account_history_index` and
    `insert_storage_history_index`, in the `save_blocks` transaction, timed as
    `update_history_indices`. `append_blocks_with_state` (test-only) shares the builder in both
    layouts, which also stops it giving an account whose info did not change a history entry
    with no changeset row.
  - A wiped account's pre-existing slots are not in the reverts. v1 reads them from
    `PlainStorageState`; v2 has no plain state. The new check fails `save_blocks` if a wiped
    account had storage before the wipe (in `HashedStorages` before the batch, or changed by an
    earlier block of the batch). Since Cancun (TN's genesis) this cannot happen from execution
    (TN-0116 corrects the argument and refuses the genesis alloc that could); the check makes
    that an error instead of a silent gap.
  - The history unwinds run in both layouts.
  - `update_history_indices` indexes v2 from the static-file changesets again, but fails if the
    committed static-file tip is below the range end, instead of indexing nothing.
  Storage path: v1's `save_blocks` still calls `update_history_indices` and its unwinds are the
  same code, so v1 datadirs are written as before. *Tests* (TN-0045):
  `v2_matches_v1_on_a_generated_chain` (the tables equal v1's; fails without the v2 insert or
  with the `DoNothing` filter dropped), `v2_history_unwind_round_trip` (fails with either empty
  unwind arm restored), `v2_wipe_of_existing_storage_fails_closed`,
  `v2_wipe_of_storage_written_earlier_in_the_batch_fails_closed` and
  `v2_update_history_indices_refuses_uncommitted_changesets` (each fails with its check
  removed). *Upstream*: n/a (upstream writes these to `RocksDB`; its v2 has the same wipe gap).

- **TN-0044** — Storage v2 keys storage changesets and `StoragesHistory` by the plain slot, as v1
  does (the key form of upstream 815037e27). *Kind*: fix (format).
  *Files*: `crates/storage/provider/src/providers/static_file/manager.rs` (the static-file
  storage-changeset writer and readers), `.../providers/state/historical.rs` (storage history
  and changeset lookups, `storage_by_hashed_key`), `.../providers/database/provider.rs`
  (`HistoryTransitions`, `write_state_reverts`, `populate_bundle_state_hashed`,
  `remove_state_above`, `take_state_above`, tests), `.../providers/consistent.rs` (in-memory
  changesets, a test), `crates/primitives-traits/src/storage.rs` (removes
  `StorageSlotKey::{from_raw, to_changeset_key, to_changeset}` and `StorageEntry::slot_key`),
  `crates/storage/storage-api/src/{storage.rs, state.rs}` (docs), `crates/trie/db/src/
  {state.rs, storage.rs}` (tests), `crates/storage/db-common/src/init.rs` (a test).
  *Why*: v1.11.4's v2 wrote `keccak256(slot)` into the static-file storage changesets and looked
  history up by it, while genesis (`insert_storage_history`) writes `StoragesHistory` by the plain
  slot, so under v2 a genesis-allocated slot read as never written at every block before its
  first later change; TN's consensus registry is such a slot. Upstream went back to plain slots
  in 815037e27. TN takes the key form only, not that commit's slot-preimage machinery (pipeline
  only) or its removal of `StorageSlotKey` and `ChangesetEntry`: changeset readers now tag every
  key `Plain`, so the callers that hash through `StorageSlotKey::to_hashed` (`unwind_storage_hashing`,
  `reth-trie-db`'s reverts and prefix sets) need no change, and the few that used the raw key for
  `HashedStorages` hash it. The helpers that encoded "v2 changesets are hashed" are removed so
  nothing can use that form again. With plain keys, v2's changesets and three index tables are
  byte-identical to v1's for the same chain. Two v2 consequences:
  - History cannot be looked up by a hashed slot (v2 keeps no preimages), so a historical
    `storage_by_hashed_key` returns `UnsupportedProvider` in both layouts. Nothing in TN called it.
  - `write_state_reverts` cannot record a wiped account's slots from `HashedStorages` (it holds
    only hashes), so under v2 such a wipe is an error (see TN-0043; impossible since Cancun).
  Storage path, v2 format: any v2 datadir written before this reads wrongly. None is deployed;
  testnet is v1, whose keys were already plain. v1 reads and writes are unchanged.
  *Tests*: `v2_genesis_storage_reads_after_genesis` (db-common; fails with the hashed lookup),
  `test_populate_bundle_state_hashed_with_plain_keys`, `test_write_and_remove_state_roundtrip_v2`
  and the TN-0045 differential. *Upstream*: n/a (upstream commit).

- **TN-0045** — Tests: storage v2 without `RocksDB` against v1 on the same generated chain.
  *Kind*: test.
  *Files*: new `crates/storage/provider/src/providers/database/tn_v2_tests.rs` (TN-owned),
  `.../providers/database/mod.rs` (the `#[cfg(test)]` module line).
  *Why*: the oracle for TN-0041 to TN-0044. A seeded generator builds blocks 1 to 12 with
  transactions, account creations and updates, accounts whose only change is storage (an info
  revert of `DoNothing`), slots changed in several blocks, slots cleared, the empty bytecode
  in a bundle's contracts, and an account without storage that block 7 destroys and re-creates
  with one slot set to 7 (a storage wipe whose slot revert is `Destroyed`, inside a multi-block
  batch). The chain is saved with `save_blocks` into a v1 and a v2 factory in
  single- and multi-block batches. `v2_matches_v1_on_a_generated_chain` checks that v2 writes
  the three ex-`RocksDB` tables, that they, the changesets and `Bytecodes` equal v1's row for
  row, that every historical account and storage read at the start of each block equals v1's
  answer and the generator's state, that the re-created slot reads 7 after the wipe block (its
  only history entry), and that every transaction is found by hash.
  `v2_history_unwind_round_trip` removes blocks with `remove_block_and_execution_above` in both
  layouts, compares the tables and reads with a save of the shorter chain, then saves the removed
  blocks again and compares with a full save. `v2_history_covers_the_tip_state` walks
  `HashedAccounts` and `HashedStorages`, in both layouts, after a full save, an unwind to before
  the wipe block and a re-save, and requires a history shard for every key: a key without one
  reads as never written at every block, which no read reports as an error. Three tests cover
  the fail-closed paths of TN-0043: a v2 wipe of an account with committed storage, and of
  storage written earlier in the same batch, fail (a wipe of an account without storage saves),
  and `update_history_indices` refuses blocks whose static-file changesets are not committed yet
  while re-indexing committed ones to what `save_blocks` wrote. Each test fails with the change
  it guards undone; dropping a wiped account's slots in `HistoryTransitions::from_reverts`
  fails the differential, the unwind round trip, the re-index check and the history check.
  *Upstream*: n/a.
- **TN-0050** — Static-file syncs are a plain `fsync` on macOS. *Kind*: fix (performance, macOS
  only).
  *Files*: `crates/fs-util/{Cargo.toml, src/lib.rs}` (new `fsync_file`, used by
  `atomic_write_file`; tests), `crates/storage/nippy-jar/src/writer.rs` (data and offsets syncs,
  and the prune path's), `crates/static-file/types/{Cargo.toml, src/changeset_offsets.rs}` (the
  `.csoff` sidecar's sync, truncate and open-time heal), and the `.csoff` heal in
  `crates/storage/provider/src/providers/static_file/writer.rs`.
  *Why*: every static-file sync was `File::sync_all`, which on macOS is `fcntl(F_FULLFSYNC)`, a
  drive cache flush per call (about 5 ms each on an M1 Max): 12 a persist under storage v1 and 26
  under v2, a fixed tax that made Mac benches measure reth's static files instead of the engine.
  TN's own stores sync with a plain `fsync` on macOS (`tn_storage::archive::data_file::fsync_file`,
  tn-lmdb's `sys`), and the static files now do the same through one helper,
  `reth_fs_util::fsync_file`: `rustix::fs::fsync` on macOS (safe; rustix 1.1.4 with `fs` was
  already in the lockfile through tempfile, so only dependency edges are added), `File::sync_all`
  elsewhere, so Linux is unchanged. What is written, and the order of writes and syncs, are
  unchanged, so this cannot reach execution or block validity. *Tests*: `reth-fs-util`'s new
  tests (a file and a directory sync; a failed sync is returned, which fails if the helper
  swallows the error; `atomic_write_file` replaces the file and leaves no `.tmp`), and the
  nippy-jar, static-file-types and provider suites. *Upstream*: no; upstream calls
  `File::sync_all`, which std makes `F_FULLFSYNC` on macOS.

- **TN-0051** — `save_blocks` writes each static-file segment's configuration in the segment's
  thread. *Kind*: performance (storage commit path).
  *Files*: `crates/storage/provider/src/providers/static_file/writer.rs` (new
  `StaticFileProviderRW::sync_and_freeze`, the `index_pending` flag that `finalize` and `commit`
  honour), `crates/storage/provider/src/providers/static_file/manager.rs` (`write_segment` calls
  it; tests `frozen_rows_stay_unindexed_until_finalize`,
  `commit_publishes_rows_frozen_by_sync_and_freeze` and
  `finalize_syncs_rows_appended_after_sync_and_freeze`),
  `crates/storage/provider/src/providers/database/provider.rs` (a comment in
  `DatabaseProvider::commit`; tests `test_save_blocks_without_db_commit_heals_v{1,2}`).
  *Why*: `DatabaseProvider::commit` wrote every dirty segment's `.conf` one after another on the
  commit thread (`StaticFileWriters::finalize`), each a temp-file write and `fsync`, a rename
  and a directory `fsync`: 6 serial syncs a persist under storage v1 and 12 under v2, on the
  critical path between `save_blocks` and the database commit (Linux p50 5-21 ms, Mac about
  31 ms). Each segment's thread in `write_blocks_data` now syncs its data, offsets and `.csoff`
  and then writes its `.conf` itself, beside the database writes of `save_blocks`; `finalize`
  is left with the in-memory index update (`update_index`). What is written is unchanged, and
  every `.conf` is still durable before the database commit, because `save_blocks` joins the
  segment threads first. So a crash anywhere leaves states the startup heal already handles:
  static files ahead of the database checkpoints, which `check_consistency` prunes back (TN
  refuses only the opposite direction, an unwind). The one new path to that state is a failure
  in `save_blocks`' database arm or before the commit after the segments froze; it is the state
  a crash between `finalize` and the database commit always left. The rows stay unindexed until
  `finalize`, so readers, which route by the index, do not see them earlier. A jar loaded on a
  cache miss in that window holds the new rows, and `update_index` replaces it when it publishes
  them. A range read on `StaticFileProvider` that runs past the index tip inside such a jar can
  return them early. That is harmless: the index was already ahead of the database for the
  whole database commit before this change, so callers already see rows the database has not
  committed. Prune and unwind commits and file rotations keep the full `commit`. It cannot
  reach execution: the bytes written are the same; only when the `.conf` syncs run changes.
  *Tests*: `test_save_blocks_without_db_commit_heals_v{1,2}` (save, abort the database
  transaction: every written segment's `.conf` is on disk at the new tip while the index is
  not; a reopen through `ProviderFactory::new_checked` prunes them back to genesis without an
  unwind and the blocks save again) and `frozen_rows_stay_unindexed_until_finalize` (a cache
  miss after the freeze loads the longer jar, reads by number still stop at the index tip, and
  `finalize` publishes the rows). They fail if the configuration is not written in the
  segment thread, if it is published before `finalize`, or if `finalize` skips the index
  update, as do `test_save_blocks_v{1,2}_table_assertions` for the last.
  `commit_publishes_rows_frozen_by_sync_and_freeze` (a commit of every writer after the freeze,
  the path `DatabaseProvider::commit` takes in unwind order or with an unwind queued, publishes
  the rows) fails
  without `commit`'s `index_pending` branch, and
  `finalize_syncs_rows_appended_after_sync_and_freeze` (a row appended after the freeze gets its
  own data and offsets syncs, counted by TN-0052's stats) fails if `sync_and_freeze` leaves the
  writer marked synced. TN's
  `test_startup_heals_static_files_ahead_of_database` (tn-reth) and the engine's late-persist
  tests cover the same states through `RethEnv`. *Upstream*: maybe; it keeps upstream's crash
  semantics.

- **TN-0052** — Per-segment timings of the static-file syncs, for benches. *Kind*: TN feature
  (instrumentation).
  *Files*: `crates/storage/provider/src/providers/static_file/sync_stats.rs` (new:
  `StaticFileSyncStats`, `SegmentSyncStats`, `SyncPhaseStats`), `.../static_file/{mod.rs,
  manager.rs, writer.rs}` (the provider's `sync_stats`, `take_sync_stats`, `record_sync`; the
  writer times its syncs; test `sync_stats_record_each_phase_per_segment`),
  `crates/storage/provider/src/providers/mod.rs` (re-exports),
  `crates/storage/nippy-jar/src/writer.rs` (`sync_data` split out of `sync_all`, and
  `commit_offsets` made public, so the two syncs are timed apart).
  *Why*: reth's metrics time the static-file arm of `save_blocks` and the static-file part of
  the commit as wholes, so nothing showed which sync of which segment a persist waits on.
  `StaticFileProvider::take_sync_stats` returns, per segment, the calls, total and longest time
  of five phases since the last take: the data file's write-out and `fsync`, the offsets'
  append and `fsync`, the `.csoff` `fsync`, the `.conf` freeze (TN-0051) and the index update.
  Each provider keeps its own under a mutex (about ten uncontended locks a persist), so tests
  never share them. TN reads them through `RethEnv::take_static_file_sync_stats` (test-utils).
  Nothing written or synced changes, so this cannot reach execution or storage. *Tests*:
  `sync_stats_record_each_phase_per_segment` (each phase once per segment, `.csoff` only for a
  changeset segment, a take drains them; fails when a phase is not recorded) and tn-reth's
  `test_persist_reports_static_file_sync_stats`. *Upstream*: no.

- **TN-0100** — A timing harness for historical reads, storage v1 against v2. *Kind*: test.
  *Files*: `crates/storage/provider/src/providers/database/tn_v2_read_bench.rs` (new: the heavy
  test `v2_history_read_timing_heavy`), `.../database/mod.rs` (the module),
  `.../database/tn_v2_tests.rs` (`executed` is `pub(super)`).
  *Why*: storage v2 answers a historical account or storage read that the history resolves to a
  changeset from static files, not from a database table, and the exec bench showed those reads
  slower than v1's with a longer tail. The harness saves one generated chain (2400 blocks of 150
  account and 300 slot changes) into a v1 and a v2 factory in batches of 8 blocks, then times the
  same 20k reads in each: right after the last persist, again warm, with the changeset jars
  reloaded every 32 reads as a persist does, while a writer persists, and while a writer persists
  and another thread holds a static-file jar the way a range walk does. Under v2 it also times
  the steps of the changeset lookup (jar, `.csoff` read, cursor, binary search). Every answer is
  compared between the passes and between the layouts. `#[ignore]`d (heavy, run with
  `--release`); test code only. *Upstream*: no.

- **TN-0101** — `StaticFileProvider::update_index` loads the jar before it takes the index lock.
  *Kind*: performance (static-file read path).
  *Files*: `crates/storage/provider/src/providers/static_file/manager.rs` (`update_index`, new
  `prepare_index_update` and `PreparedIndexUpdate`; test
  `growing_commit_waits_for_jar_readers_outside_the_index_lock`).
  *Why*: `update_index`, which every static-file commit runs once per segment, held the write lock
  of the segment index while it read the jar's configuration, mapped its data and offsets files,
  opened its `.csoff`, inserted the jar into the cache (waiting for every reader of that cache
  shard) and cleaned the cache with `DashMap::retain` (waiting for every reader of every shard).
  Every static-file lookup of every segment takes that lock, so one reader holding a jar, such as
  a range walk over receipts or headers, stalled all of them for as long as it held it; storage v2
  routes each historical read that resolves to a changeset through it. Now the jar is loaded
  before the lock; when the segment grows past the indexed tip it is published to the cache before
  the lock too, and the stale jars above the new range are found under shared shard locks and
  removed one by one. Publishing before the index is the state a jar loaded on a cache miss already
  creates (TN-0051): lookups route by the index, which still ends at the old tip, and the new jar
  holds every row the old one did. A shrinking or unchanged segment (unwind, prune) is still
  published under the lock with the index, and if the index was re-initialized or the segment's
  ranges moved since the load, the jar is reloaded under the lock as before. The index, the cached
  jars and every byte read are the same once `update_index` returns, so this cannot reach
  execution or what is written. *Tests*: `growing_commit_waits_for_jar_readers_outside_the_index_lock`
  (a reader holds the headers jar, then another segment's, while a commit grows the headers: a
  lookup from a third thread must not wait for it, and the new header must be readable after the
  commit; it fails when the grown jar is published under the lock, and when the clean-up goes back
  to `retain`). In `v2_history_read_timing_heavy` (TN-0100, Mac at load ~60-80, so only the largest
  effect is clear) the longest v2 read with a jar holder fell from 82-94 ms to 4-5 ms. *Upstream*:
  yes.

- **TN-0115** — Tests: storage v2 across static-file boundaries. *Kind*: test.
  *Files*: `crates/storage/provider/src/test_utils/mod.rs` (new
  `create_test_provider_factory_with_blocks_per_file`),
  `.../providers/database/tn_v2_tests.rs` (`Chain::across_static_files`,
  `v2_matches_v1_across_static_files`, `v2_history_unwind_round_trip_across_static_files`),
  `.../providers/database/provider.rs` (`run_save_blocks_without_db_commit_heals` takes a
  blocks-per-file setting; `test_save_blocks_without_db_commit_heals_across_files_v2`).
  *Why*: every v2 test held its chain in one static file per segment (500k blocks per file
  against a 12-block chain), so nothing exercised a v2 changeset lookup in a later file, an
  unwind or heal that deletes files, or a save that rotates files in the segment threads
  (TN-0051). The TN-0045 differential and unwind round trip now also run with 4 blocks per file
  (four files; the `5..=9` and `10..=12` batches each cross into a new one); the unwind must
  delete the later files, leave two, and read the shorter chain, and the re-save must rebuild the
  full chain's tables. The aborted save of TN-0051 runs with 2 blocks per file under v2: blocks
  1 to 3 rotate every segment into a new file with a new `.csoff`; the heal on reopen must delete
  the new files with every satellite file and shorten the old ones to genesis, and the blocks
  must save again. A rotation commits the filled file during the save (`increment_block`), so
  the reader index reaches block 1 before the database commit; the test pins that (it is the
  index's state, not the new file's). *Tests*: the across-files differential and unwind fail when
  `SegmentHeader::changeset_offset_index` ignores the file's first block (the single-file ones
  pass), the unwind when `truncate_changesets` stops deleting files, and the aborted save when
  `NippyJar::delete` leaves a file's configuration behind. *Upstream*: n/a.

- **TN-0116** — A storage v2 genesis refuses an account with storage but no code; the wipe
  check's rationale is corrected. *Kind*: fix (genesis init, v2 only) and docs.
  *Files*: `crates/storage/db-common/src/init.rs` (`init_genesis_with_settings`, new
  `InitStorageError::GenesisStorageWithoutCode`; test `v2_genesis_refuses_storage_without_code`;
  `v2_genesis_storage_reads_after_genesis` gives its account code),
  `crates/storage/provider/src/providers/database/provider.rs` (the doc of
  `ensure_wiped_accounts_had_no_storage`).
  *Why*: TN-0043's check documented that a wipe of an account with storage cannot happen since
  Cancun because EIP-7610 refuses a create at an address with storage. revm does not apply that
  rule: its create collision check looks at the code hash and the nonce only. The argument that
  holds: revm wipes storage when it self-destructs an account (since EIP-6780 only one created in
  the same transaction, which revm allows only at an address with no code and a zero nonce) or
  deletes a touched empty account (EIP-161: no code, zero nonce, zero balance). Execution never
  leaves storage at an address with no code and a zero nonce: only code running at an address
  writes its storage, a created contract gets nonce 1 even with empty runtime code, and an
  EIP-7702 authorization bumps the delegating account's nonce. (A native precompile writing its
  own storage needs code in genesis; TN's TEL precompile has `0xfe`.) The one source left is a
  genesis alloc, so under v2 `init_genesis_with_settings` now refuses an account with storage
  and no code (absent or empty) before it writes anything, rather than letting a later block
  halt the chain. TN's testnet and mainnet genesis files and the precompile configuration have no
  such account (checked 2026-11-09); v1 genesis is unchanged. It cannot change execution: it only
  refuses a genesis v2 could not store faithfully. *Tests*:
  `v2_genesis_refuses_storage_without_code` (absent and empty code: v2 refuses with nothing
  written, v1 writes it; fails with the check removed). *Upstream*: maybe (upstream's v2 has the
  same wipe gap, TN-0043).

- **TN-0120** — NippyJar format version 2: a CRC32 on every value and on the configuration.
  *Kind*: feature (storage integrity; static files of storage-v2 datadirs only).
  *Files*: `Cargo.toml` (workspace `crc32fast` 1.5.0 and `crc` 3.4.0, both already in TN's root
  lockfile, so only dependency edges are added), `crates/storage/nippy-jar/{Cargo.toml,
  src/lib.rs, src/writer.rs, src/cursor.rs, src/error.rs}` (`NippyJar::with_checksums`,
  `has_checksums`, `value_checksum`, `value_seed`, `checksum`, `CHECKSUM_SIZE`;
  `NippyJarWriter::has_checksums`; `NippyJarCursor::unverified_value`; errors
  `ChecksumMismatch`, `ConfigChecksumMismatch`, `UnsupportedVersion`,
  `UnsupportedCompression`; tests).
  *Why*: NippyJar had no checksum anywhere, and under storage v2 the account and storage
  changesets, which back every historical state read (TN's pinned committee and epoch reads
  included), live in static files: a flipped bit read back as a plausible wrong value, a silent
  local fork, where tn-lmdb's page CRCs turn the same damage into an error under v1. Format:
  a version-2 jar keeps the version-1 configuration encoding (bincode) and appends a CRC32 of
  it, which `load` verifies before decoding the rest (a damaged row count or block range would
  otherwise be believed, and the writer's heal truncates to the row count). Each value is
  stored as its bytes (compressed if the jar compresses) followed by a CRC-32 (ISO-HDLC, the zlib
  CRC) of those bytes, little-endian, started from a seed of the value's position instead of the
  standard initial value: `value_seed(row, column) = 0x9E3779B9 ^ (row as u32) ^
  ((row >> 32) as u32).rotate_left(16) ^ (column as u32).wrapping_mul(0x85EBCA6B)`, in
  crc32fast's convention (`Hasher::new_with_initial`; 0 is the standard CRC), the row being the
  jar's own 0-based row index. For one input length a CRC is injective in its initial value, so
  a value read at another position fails as surely as a changed byte, and no extra bytes are
  hashed (on x86_64 every byte below 128 B costs a serial table step in crc32fast). On x86_64,
  values below 128 B are checksummed with the `crc` crate's slice-by-16 table, longer ones and
  every value on aarch64 with crc32fast (PCLMULQDQ, or the CRC instructions); the two give the
  same values (`test_checksum_implementations_agree`). Offsets include the four bytes, so the open-time heal, `prune_rows`, rotation and
  deletion, which work on lengths only, keep the framing. `NippyJarCursor::read_value`, the one
  path every value read takes, verifies the checksum before any decompression or decoding and
  hands out the value without it; a mismatch is `NippyJarError::ChecksumMismatch` naming the data
  file, row and column, never a panic, `None` or a default. `NippyJarCursor::unverified_value`
  returns a value's stored bytes without the check, for a search that only compares a prefix to
  steer itself and then reads the row it settles on verified (TN-0124). A heal never acts on a mismatch: it
  checks lengths only, and a damaged value stays an error on every read. Binding the position
  makes an offset that lands exactly on another value's boundary fail as well. A version-1 jar is
  written and read exactly as before (byte-identical files); `load` now refuses a version other
  than 1 or 2 (upstream only ever wrote 1), and a version-2 configuration naming dictionary zstd
  (`UnsupportedCompression`): the writer compresses without the dictionaries the cursor then
  `expect`s, so that path cannot read back, and no TN segment uses it. A jar's own configuration decides its format, so a
  datadir can mix both. It cannot reach execution: the stored values are unchanged, and nothing
  creates a version-2 jar until the provider's policy (TN-0122) asks for one, which only storage
  v2 does. *Tests*: `test_checksummed_jar_round_trip` (plain and LZ4, whole rows and single
  columns; sizes: four bytes a value, and an unchecked jar's files keep upstream's sizes),
  `test_checksummed_config_serialization` (version 2 and the trailer, byte for byte),
  `test_checksummed_config_damage_is_refused` (every byte of the configuration flipped, a cut
  file, an unknown version, dictionary zstd with a valid checksum), `test_checksum_detects_damaged_value` (first, middle, last stored
  byte and a checksum byte, plain and LZ4: only that value fails, its row's other column still
  reads), `test_checksum_detects_offset_moved_to_another_value` (offsets moved onto the next
  row's value fail; an unchecked jar returns the wrong value; a value shorter than a checksum
  fails), `test_checksummed_jar_heal_and_prune` (an uncommitted append healed away, a prune of
  committed and uncommitted rows, appends after each: every row verifies),
  `test_checksum_implementations_agree` (lengths 0 to 300, random and edge seeds, the standard
  check value), `test_unverified_value` (stored bytes without the checksum, damaged ones
  included; too short is an error), and the ignored `checksum_cost_heavy` (ns per checksum by
  size and implementation; aarch64 M1 Max: crc32fast 4-9 ns from 20 to 176 B). Mutation-checked:
  no verification in the cursor, no row in the seed, no configuration check, a wrong row index in
  the writer. *Upstream*: maybe (a format change upstream would want behind its own version).
  The checksums alone did not close one gap: the open-time heal's prune-direction arms
  (`consistency.rs`) trusted the offsets file's length and final offset, so an offsets file one
  row short, or a flipped high bit in the final offset, cut a committed row and rewrote the
  configuration without an error. TN-0130 refuses that in checksummed jars.

- **TN-0121** — Checksummed `.csoff` records, and the changeset read paths fail closed on
  damage. *Kind*: feature (storage integrity) and fix (static-file read paths).
  *Files*: `crates/static-file/types/{Cargo.toml, src/changeset_offsets.rs, src/lib.rs}`
  (`ChangesetOffsetWriter::new` and `ChangesetOffsetReader::new` take the jar's `checksums`;
  `changeset_offset_record_size`, `ChangesetOffsetChecksumMismatch`; `get_range` reads chunks of
  4096 records a `pread`; tests), `crates/storage/provider/src/providers/static_file/{mod.rs,
  jar.rs, manager.rs, writer.rs, writer_tests.rs}`.
  *Why*: under storage v2 the `.csoff` sidecar maps each block to its changeset rows, and a
  damaged record sends a lookup into another block's rows, which the value checksums cannot see.
  A checksummed jar's sidecar (TN-0120, the jar's version says which) has 20-byte records,
  `[offset u64][num_changes u64][crc u32]`, LE, the CRC-32 (ISO-HDLC) of the first 16 bytes
  started from `record_seed(index) = 0xC2B2AE35 ^ (index as u32) ^ ((index >> 32) as u32)
  .rotate_left(16)` (crc32fast's convention), the index being the block's distance from the
  file's first block; every record read verifies it, and a mismatch is an `InvalidData` I/O
  error wrapping `ChangesetOffsetChecksumMismatch` (file, index). Plain sidecars keep upstream's
  16-byte records. Every reader and writer is given the jar's format, from `NippyJar::
  has_checksums` (the cached reader of TN-0040, the count functions, the heal, truncation,
  rotation and `delete_current_and_open_previous`). The read paths around it: `StaticFileJarProvider
  ::read_changeset_offset` checks that the block's rows lie inside the jar before any caller sizes
  a buffer with `num_changes` or iterates `changeset_range` (a damaged count asked for an
  allocation that aborts, and a range past the rows returned a shorter changeset), returning
  `ProviderError::CorruptedChangeSetStaticFile`; `account_block_changeset` and
  `storage_changeset` treat a row missing inside the block's range as that error instead of
  skipping it (the range walks, `from_reverts` and historical storage roots read through them);
  `fetch_range_with_predicate` errors on a row missing inside a jar's own block or transaction
  range instead of returning the rows before it (outside the jar's range the data still ends
  quietly, as a pruned receipts prefix needs). `heal_changeset_sidecar` reads the committed
  records in one bulk read (it did one `pread` a block, up to 500k on the first open of a file
  near its end), reads none past the header's count (they are cut whatever they hold), errors on
  a committed record that fails its checksum, and keeps shrinking only a trailing run of records
  past the rows, which an interrupted prune leaves (offsets only grow); a record that fits again
  after one that does not is refused as damage, so a damaged middle record no longer erases the
  committed blocks after it. After a shrink it publishes the new range (`update_index`), so the
  startup consistency check that opened the writer sees the loss in the same boot instead of the
  next. The point lookups' not-found result is unchanged: every history caller already maps it to
  `AccountChangesetNotFound`/`StorageChangesetNotFound`, which TN halts on. None of this reaches
  execution: valid files read exactly as before, and the new errors need files that disagree with
  their own configuration. *Tests*: `test_checksummed_records`,
  `test_checksummed_record_damage_is_an_error`, `test_checksummed_record_bound_to_its_index`,
  `test_range_reads_across_chunks` (static-file-types), and TN-0123's provider tests.
  Mutation-checked: no record verification, no index in the seed, the heal reading a checksummed
  sidecar as plain, no range validation, no heal refusal, no `update_index`, no range error.
  *Upstream*: partly (the range validation, the missing-row errors, the heal's bulk read and
  re-index are upstream bugs).

- **TN-0122** — Storage v2 datadirs create checksummed static files. *Kind*: feature (policy).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, writer.rs, mod.rs}`
  (`StaticFileProviderInner::set_jar_checksums` and `jar_checksums`, the builder's
  `with_jar_checksums`, `checksummed_segment`; `create_jar` takes the decision),
  `crates/storage/provider/src/providers/{mod.rs, database/mod.rs, database/provider.rs}`
  (`ProviderFactory::new` and both `set_storage_settings_cache` set it from
  `StorageSettings::is_v2`).
  *Why*: the static-file provider did not know the datadir's layout, and TN-0120's format must
  reach every new jar of a v2 datadir and none of a v1 one. The factory pushes the decision
  whenever it learns the settings: at construction from the stored row, and from
  `set_storage_settings_cache`, which TN's `layout::settle` calls before genesis writes the first
  jar. New jars of every segment carry checksums under v2 (`checksummed_segment`; the changesets
  are mandatory, the others stay while the read cost is below the benches' noise); v1 datadirs
  create upstream's format byte for byte. An existing jar keeps its own format for reads and
  appends, so a v2 datadir written before this change keeps its unchecked jars until they rotate.
  It cannot reach execution: the values stored are unchanged; v1 files are unchanged.
  *Tests*: TN-0123. *Upstream*: no.

- **TN-0123** — Tests: checksummed static files through `StaticFileProvider`. *Kind*: test.
  *Files*: `crates/storage/provider/src/providers/static_file/{tn_checksum_tests.rs (new),
  mod.rs}`.
  *Why*: TN-0120 to TN-0122 end to end: `policy_picks_the_jar_format` (every segment's new jar
  and `.csoff` size with checksums on and off; senders 24 against 20 bytes a value; the factory
  setters), `damaged_changeset_value_is_an_error` (point lookups, block reads and range walks of
  both changeset segments; the other blocks still read), `damaged_block_segment_value_is_an_error`
  (header, transaction, receipt and sender by number, and the range reads),
  `damaged_changeset_offsets_are_an_error` (a record's offset and its count's high byte),
  `unchecked_changeset_offsets_past_the_rows_are_an_error` (one past the rows and a count no
  allocation holds: an error, not a short changeset or an abort), `checksummed_sidecar_heal`
  (a torn and uncommitted tail cut in 20-byte records; a damaged committed record stops the open
  and nothing is cut), `sidecar_heal_shrinks_a_trailing_run_and_refuses_a_gap` (an interrupted
  prune is healed and published at once; a gap is refused), `rotation_prune_and_mixed_formats`
  (unchecked jars keep their format while new jars are checksummed; prune and appends across
  rotations keep verifying), `missing_row_inside_a_range_is_an_error`,
  `damaged_probe_key_is_an_error_not_another_row` (TN-0124), and in `database/tn_v2_tests.rs`
  `v2_history_read_without_its_changeset_jar_fails_closed` (a v2 datadir missing the changeset
  files of blocks 4 to 7: every history read that resolves to them is `*ChangesetNotFound`,
  never another state's value, and the rest answer right; fails if a miss falls back to a
  value). *Upstream*: no.

- **TN-0124** — The changeset point lookups probe raw key bytes. *Kind*: performance (storage v2
  historical reads).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs}`
  (`changeset_lower_bound`, `changeset_lookup_missed`, `changeset_search`; used by
  `get_account_before_block` and `get_storage_before_block`),
  `crates/storage/provider/src/providers/database/tn_v2_read_bench.rs` (its search step runs the
  same search).
  *Why*: each of a lookup's ~8 binary-search probes decoded a whole `AccountBeforeTx` or
  `StorageBeforeTx` into a fresh row `Vec` only to compare its key, and with TN-0120 would also
  verify a checksum per probe (bytewise on x86_64 below 64 B). The key is the stored value's
  leading bytes (the address, then the plain slot: 20 or 52 B, raw, so byte order is key order),
  so a probe now compares them in place (`NippyJarCursor::unverified_value`), and only the row the
  search settles on is read verified and decoded, its key compared again. A value shorter than
  the key, or a probe past the jar's rows, is `CorruptedChangeSetStaticFile` (the range was
  validated by TN-0121, so only damage gets there). Fail closed: a damaged probe can only end the
  search on a row that does not match, which is not found, which every history caller turns into
  `AccountChangesetNotFound`/`StorageChangesetNotFound` (TN halts on both); on a miss in a
  checksummed jar the search reruns with every probe verified, so the damage is reported as the
  checksum error. The answers are those of the old search, which compared the same keys decoded.
  It cannot reach execution: historical reads only, same results. *Tests*:
  `damaged_probe_key_is_an_error_not_another_row` (a probe's key damaged so the search lands on an
  intact row of another key: the checksum error, never that row; real misses stay `None`; fails
  without the rerun), and every changeset lookup test of the crate (TN-0045, TN-0115, TN-0123).
  *Upstream*: maybe (the raw probe; the rerun needs TN-0120).

- **TN-0130** — A prune commits its configuration before it shrinks a file, and a checksummed
  jar shorter than its configuration is refused. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/nippy-jar/src/{writer.rs, consistency.rs, error.rs, lib.rs}`
  (`NippyJarWriter::prune_rows`, the checker's prune-direction arms,
  `NippyJarError::CommittedRowsMissing`, tests),
  `crates/storage/provider/src/providers/static_file/{writer.rs, mod.rs, tn_checksum_tests.rs,
  tn_crash_tests.rs (new)}` (`truncate_changesets`, `heal_changeset_sidecar`; tests).
  *Why*: a prune cut the data and offsets files (and the provider the `.csoff` sidecar) first and
  wrote the shrunk `.conf` last, so an interrupted prune and a damaged file left the same state,
  files shorter than the `.conf` counts, and the open-time heal had to cut committed rows to match
  whatever the files held, damage included. Now `prune_rows` writes the `.conf` with the new row
  count (durably, `atomic_write_file`) before it cuts either file, and `truncate_changesets` gives
  the segment header its final form (block range, `changeset_offsets_len`) before `prune_rows`
  writes it and commits before it cuts the sidecar. A prune stopped at any point leaves files
  longer than the `.conf`, an uncommitted tail, which the existing append-direction heal cuts.
  In a checksummed jar (TN-0120, version 2) files shorter than the `.conf` are then damage: the
  checker returns `CommittedRowsMissing` (offsets missing, or a final offset past the data) and
  cuts nothing, and the sidecar heal returns `CorruptedChangeSetStaticFile` for a sidecar shorter
  than the header or a committed record past the rows. Version-1 jars keep upstream's heal,
  since they may hold an interrupted prune of an older build. A finished prune writes the files
  it wrote before, byte for byte, in both versions (checked: eight prune and commit steps, both
  versions, all files identical to the old order), so version-1 datadirs are unchanged and an
  older build opens a jar this one pruned, finished or interrupted. Not reachable on TN's write
  path: TN never prunes at runtime; the prunes are the startup checkpoint heal's and `db
  reset-execution`'s. It cannot reach execution: the values stored and read are unchanged.
  *Tests*: `test_heal_of_files_shorter_than_the_config` (both damages: checksummed refused with
  every file unchanged, plain healed as before), `test_interrupted_prune_heals_at_every_point`
  (prunes of 1, 3 and every row with an uncommitted row, both versions: a failed `.conf` write
  cuts nothing; stopped after the `.conf`, or after the offsets cut, heals to the finished prune
  byte for byte), `changeset_prune_commits_the_header_before_it_cuts` (the same for a changeset
  prune through the provider, of rows and of blocks without changes, so the sidecar alone
  shrinks), `checksummed_changesets_shorter_than_the_header_are_refused` (rows cut by an older
  build's order, a sidecar one record short, a sidecar gone). Mutation-checked: the `.conf`
  written after the cuts, the sidecar cut before the commit, each of the three refusals removed.
  *Upstream*: maybe (the order is an upstream bug; the refusal needs TN-0120's versions).

- **TN-0131** — A jar without committed rows opens whatever a crash left in its files, and an
  empty offsets file is an error, not a panic. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/nippy-jar/src/{consistency.rs, lib.rs, error.rs}`
  (`NippyJarChecker::reset_files`, `DataReader::new`, `NippyJarError::EmptyOffsetsFile`, tests),
  `crates/storage/provider/src/providers/static_file/tn_crash_tests.rs`.
  *Why*: a rotation creates the next jar's data and offsets files, writes the 9-byte offsets
  header without a sync and then the jar's configuration (rows 0) durably. A power loss there can
  leave the configuration and an empty or torn offsets file (ext4's delayed allocation keeps the
  size at 0). The next persist reopens that jar, the heal maps the offsets file and
  `DataReader::new` indexed byte 0 of an empty map: a panic at every start, until someone deleted
  the files by hand. A configuration that counts no rows commits nothing, so whatever the files
  hold is uncommitted: the writer's open (`NippyJarChecker` in heal mode) now restores the files a
  new jar starts with (the offset size and a final offset of 0, an empty data file), writing only
  when they differ. That also covers a damaged final offset and another jar's leftover files
  (TN-0132). Every other reader of an empty offsets file gets `EmptyOffsetsFile`. Jars with rows
  are not touched; the read-only check is unchanged. It cannot reach execution: only jars with no
  rows change, and their files end as a new jar's.
  *Tests*: `test_jar_without_rows_opens_whatever_its_files_hold` (empty, torn and damaged offsets,
  leftover data and a whole other jar's files, both versions: the open restores the new-jar files
  and appends read back; the reader's error), `rotation_after_an_empty_offsets_file` (headers and
  account changesets, both versions: the rotated jar's offsets emptied, the next persist appends
  and every block reads). Mutation-checked: no reset (both fail), the reader indexing the map
  (panics). *Upstream*: yes (the panic is upstream's).

- **TN-0132** — A jar deletion stopped at any file leaves the whole jar or files no listing sees,
  and a jar created over such leftovers starts empty. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/nippy-jar/src/{lib.rs, writer.rs}` (`NippyJar::delete`,
  `NippyJarWriter::new`, `create_or_open_files`, tests),
  `crates/storage/provider/src/providers/static_file/{writer.rs, manager.rs, tn_crash_tests.rs}`
  (`delete_current_and_open_previous`, `delete_jar`; tests).
  *Why*: jars are found by their data file (`iter_static_files` parses only extension-less names
  and then loads the `.conf`). `delete_current_and_open_previous` and `delete_jar` removed a
  changeset jar's `.csoff` before `NippyJar::delete`, so a crash in between (a startup prune that
  deletes a file, or `remove_unfinished_genesis`'s `delete_segment`) left a jar that is listed
  but does not load, and startup failed until someone removed the files by hand. The sidecar now
  goes with the jar in `NippyJar::delete`, whose data file goes first, and the directory is
  synced before the satellites go (offsets, configuration, sidecar), so every stopped deletion
  leaves either the whole jar or unlisted leftovers. A leftover data or offsets file then met the
  next jar created at that path: `create_or_open_files` opened the surviving file as it was and
  wrote the new offsets header over the start of an old offsets file, keeping its stale tail, so
  the new jar's rows pointed into the old ones. A file a new jar is created next to now starts
  empty (TN-0131 handles both files surviving; the sidecar writer already truncates to the
  header's count). A jar whose configuration counts rows and has lost its data or offsets file is
  refused (`MissingFile`) instead of being created again over its configuration. It cannot reach
  execution: it changes only how files are removed and how a missing file is treated.
  *Tests*: `test_writer_creates_over_leftovers_and_refuses_a_missing_file` (leftover offsets, data,
  both; a committed jar missing either file: refused, its configuration unchanged; both
  versions), `deletion_stopped_at_any_file` (a changeset jar stopped after each file of the
  deletion order: the jars below serve, the range written again with other rows reads only them,
  also after a reopen), `deletion_removes_the_data_file_first` (a deletion that fails at the data
  file leaves the jar whole and loadable). Mutation-checked: no leftover truncation, no
  missing-file refusal, the sidecar removed before the jar. *Upstream*: yes (the `TODO(joshie)` in
  `NippyJar::delete`).

- **TN-0133** — A jar missing between two others of its segment is refused at startup, and a
  read that lands in such a gap is an error. *Kind*: fix (static-file integrity).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs,
  tn_checksum_tests.rs, tn_crash_tests.rs}` (`StaticFileProvider::check_contiguity`, called by
  `check_file_consistency` and `check_consistency`; `gap_at`, used by `account_block_changeset`,
  `storage_changeset` and `fetch_range_with_predicate`; `StaticFileGapError`; tests),
  `crates/storage/provider/src/providers/{mod.rs, database/tn_v2_tests.rs}`.
  *Why*: the index is built from the directory listing with no check that a segment's jars are
  contiguous, and a block in a gap maps to the next jar's range (`find_fixed_range_with_block_index`),
  whose jar answers "absent". Point history reads already failed closed (`*ChangesetNotFound`),
  but the changeset range walks (`from_reverts`, historical proofs and state roots) returned the
  gap's blocks as empty changesets, and `fetch_range_with_predicate` (headers, transactions,
  receipts, senders) returned the rows before the gap as the whole answer. A missing file is
  invisible to the checksums. Now `check_contiguity` lists each segment's jars and errors when
  one does not start right after the one below it (a jar missing, or one below the highest not
  full); TN runs it at every start through `ProviderFactory::check_consistency`, before any heal,
  and refuses to start. Only the joins between present jars are checked, not where the lowest jar
  starts: history expiry deletes the lowest jars first (`delete_segment_below_block`), and the
  index's own build stays as upstream's. Behind it, a block or transaction the index sends to a
  jar that does not hold it, while a lower jar exists, is `StaticFileGapError::Number` in the
  block changeset reads (so the range walks) and in `fetch_range_with_predicate`; numbers below
  the lowest jar keep upstream's answers. It cannot reach execution: contiguous files read as
  before. *Tests*: `a_jar_missing_between_two_others_is_refused` (headers, transactions and
  account changesets without their middle jar, both formats: the startup check names the two
  jars around the gap, range reads across it error, reads beside it answer; the lowest jars
  deleted instead are no gap and read as before),
  `v2_history_read_without_its_changeset_jar_fails_closed` (TN-0123's, extended: the account and
  storage changeset walks across the missing jar error, and `ProviderFactory::check_consistency`
  refuses). Mutation-checked: no startup
  check, no gap error in the changeset reads, in the range fetch, or in its transaction arm.
  *Upstream*: maybe (the contiguity check; upstream's history expiry keeps a contiguous suffix).
