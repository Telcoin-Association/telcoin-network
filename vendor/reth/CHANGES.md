# CHANGES: TN's changes to vendored reth

The ledger of every difference between `vendor/reth` and the pinned upstream
(`tn/upstream.toml`). `python3 -I vendor/reth/tn/vendor.py status` lists the files that differ;
each one has to be covered by an entry here, which `vendor.py ledger` checks. Changes to TN's own files (`tn/`,
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
  `crates/storage/provider/src/providers/rocksdb_stub.rs` (module comment);
  every `cfg(all(unix, feature = "rocksdb"))` item and every `cfg(feature = "edge")` item removed
  from `crates/storage/provider/src/{either_writer.rs, providers/database/{mod,provider}.rs,
  providers/{consistent,blockchain_provider,mod}.rs, providers/state/historical.rs,
  traits/rocksdb_provider.rs, test_utils/noop.rs}`, `crates/storage/db-common/src/init.rs` and
  `crates/storage/db-api/src/models/metadata.rs` (their `not(...)` twins became unconditional);
  the `rocksdb` and `edge` features removed from
  `crates/storage/{provider,storage-api,db-common,db-api}/Cargo.toml` and
  `crates/node/core/Cargo.toml`, and the optional `rocksdb` dependency from provider and the
  workspace manifest.
  *Why*: maintainer decision (2026-10-30, DECISIONS.md D14): RocksDB is gone from TN in every
  configuration. Before, it was an off-by-default feature. Proof that nothing TN compiles
  changed: the macro-expanded source (`-Zunpretty=expanded`) of reth-provider, reth-db-common
  and reth-db-api is identical before and after, apart from the source line numbers `tracing`
  embeds. Cargo.lock loses rocksdb, librocksdb-sys, bzip2-sys and lz4-sys.
  *Still present*: the zero-size `RocksDBProvider` stub
  (`crates/storage/provider/src/providers/rocksdb_stub.rs`; only its module comment changed) and the storage-v2 plumbing that takes it (ROADMAP.md, "RocksDB step
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
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/{call.rs, blocking_task.rs, state.rs}`,
  `crates/rpc/rpc/src/eth/filter.rs`.
  *Why*: TN runs consensus and RPC on one tokio runtime, and the execution engine
  (the engine crate's `spawn_blocking_task`) and certificate validation (`spawn_blocking`) use the
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
  *Note (2026-11-10)*: the shared blocking pool this entry argues from ended on 2026-11-06, when TN's `rpc_blocking.rs` gave RPC a blocking runtime of its own (tracker, "TN-side hardening"); the permit semantics stay as described.

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
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs}`,
  `crates/storage/nippy-jar/src/{writer.rs, cursor.rs, lib.rs}`,
  `crates/storage/db/src/static_file/cursor.rs`.
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

- **TN-0039** — Backport 6e4009eed: a log subscription survives a lagging canonical-state
  channel. *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/eth/pubsub.rs` (`EthPubSub::log_stream`).
  *Why*: the stream `expect`ed every canonical-state notification, so a subscriber that fell
  more than the channel's capacity behind panicked its subscription task on the `Lagged` error.
  TN's task manager ignores the panic, so only that client's `eth_subscribe("logs")` ended, with
  a panic in the node's log. It now reads `canonical_state_stream()`, which skips the missed
  notifications, as reth's other canonical-state consumers do. RPC only. *Tests*: none upstream;
  the change swaps one stream constructor for another. *Upstream*: n/a (upstream commit).

- **TN-0040** — Backport 6d718d0c2: `trace_filter`'s range error names the configured limit.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/trace.rs` (`TraceApi::trace_filter`).
  *Why*: the error said "limited to 100 blocks" whatever `--rpc.max-trace-filter-blocks` was.
  Message only. *Tests*: tn-reth's `trace_filter_honours_max_trace_filter_blocks` now checks the
  whole message. *Upstream*: n/a (upstream commit).

- **TN-0041** — Backport bf1db83ae: pool validation workers release the shared queue before
  validating. *Kind*: backport.
  *Files*: `crates/transaction-pool/src/validate/task.rs` (`ValidationTask::run`, a test).
  *Why*: each worker held the job receiver's lock while it ran a job (a `while let` scrutinee's
  temporary lives through the loop body), so `--txpool.additional-validation-tasks` workers
  validated one transaction at a time. TN builds its pool with these workers
  (`WorkerTxPool::build`). Pool throughput only; what is admitted does not change.
  *Tests*: upstream's `cloned_workers_validate_while_another_job_is_blocked`; transaction-pool
  228/228. *Upstream*: n/a (upstream commit).

- **TN-0042** — Backport 8861e2724: `set_block_info` notifies listeners of the transactions it
  promotes. *Kind*: backport.
  *Files*: `crates/transaction-pool/src/pool/{mod.rs,txpool.rs,update.rs}` (`TxPool::set_block_info`
  returns the promotions; `UpdateOutcome` is public), plus a TN test in `pool/mod.rs`.
  *Why*: TN calls `set_block_info` when an epoch's base fee changes (`WorkerTxPool`). Transactions
  a lower base fee promoted to pending never reached pending-transaction listeners, so
  `eth_subscribe("newPendingTransactions")` missed them. TN's batch builder reads
  `best_transactions()` on its own tick, so batching was never affected. Pool notifications only.
  *Tests*: `set_block_info_notifies_pending_listeners_of_promotions` (TN; upstream has none) parks
  a transaction below the base fee, lowers the fee, and expects its hash on a pending listener;
  it fails without the change. *Upstream*: n/a (upstream commit).

- **TN-0043** — Backport 62808bd06: the pool admits senders whose code hash is the empty hash.
  *Kind*: backport.
  *Files*: `crates/transaction-pool/src/validate/eth.rs` (the sender-is-a-contract check and
  upstream's test; the pick's test-module conflicts are with upstream tests added after v1.11.4,
  which are not taken).
  *Why*: an account can carry `Some(KECCAK_EMPTY)` as its code hash, as an account with empty
  code in a genesis allocation does. The pool refused its transactions as a contract's, while the
  EVM treats it as an EOA (EIP-3607 checks for code, not for a hash). Pool admission only: no batch
  or block validity change. *Tests*: upstream's `accepts_sender_with_empty_bytecode`;
  transaction-pool 230/230. *Upstream*: n/a (upstream commit).

- **TN-0044** — Backport upstream's IPC server fixes. *Kind*: backport.
  *Files*: `crates/rpc/ipc/src/server/mod.rs`.
  *Why*: 0e14f1a8a: a connection's service loop kept polling its closed response channel, which
  returns at once, so it spun a core until the connection's other arms ended; it now ends the
  loop. a68f9e19f: each request built a fresh subscription counter, so
  `--rpc.max-subscriptions-per-connection` never limited an IPC connection; the connection's
  counter is now shared by its requests. TN serves IPC on every worker unless `--ipcdisable` is
  set; the socket is local, so only a local process could open the subscriptions. RPC only.
  *Tests*: upstream's `test_max_subscriptions_per_connection`; reth-ipc 18/18. *Upstream*: n/a
  (upstream commits).

- **TN-0045** — Backport 9580b7fc1: a test that zstd 0.13 still decodes reth's static-file
  compression. *Kind*: backport (test only).
  *Files*: `crates/storage/nippy-jar/src/lib.rs` (`test_zstd_013_compatibility`), added
  `crates/storage/nippy-jar/tests/fixtures/zstd-0.13/` (a dictionary and blocks written with zstd
  0.13, and upstream's README for them).
  *Why*: TN's static files (headers, transactions, receipts) are zstd-compressed with
  dictionaries, and TN ships zstd 0.13.3. The test fails if a zstd bump stops reading what 0.13
  wrote, before any datadir meets it. No shipped code changes. *Tests*: reth-nippy-jar and reth-db
  51/51. *Upstream*: n/a (upstream commit).

- **TN-0046** — Port the cumulative gas check from 329fcfc73: an `eth_callBundle` request's
  transactions share its block gas limit. *Kind*: backport (security).
  *Files*: `crates/rpc/rpc/src/eth/bundle.rs` (`EthBundle::call_bundle`, the
  `BundleGasLimitExceeded` error, and the TN test module `gas_limit_tests`).
  *Why*: TN serves `eth_callBundle` publicly (`tn-reth/src/rpc_bundle.rs`). The block gas limit
  (`gasLimit`, by default the call gas cap) bounded each transaction but not their sum, so one
  request could execute any number of cap-sized transactions under one blocking-IO permit. Each
  transaction must now fit the gas the earlier ones left, as in a block. Upstream's EIP-8037 branch
  (separate state gas) and its cancellation check (which needs 13a59de4b) are not ported; the
  permit part of 329fcfc73 is TN-0037's. RPC only.
  *Tests*: `bundle_over_the_gas_limit_is_refused` (two 30,000-gas transfers under a 50,000 limit;
  fails without the check) and `bundle_within_the_gas_limit_runs`. *Upstream*: n/a (upstream
  commit).

- **TN-0047** — Backport 5b0120dbb: lower the RPC block and receipt cache defaults. *Kind*:
  backport.
  *Files*: `crates/rpc/rpc-server-types/src/constants.rs` (`DEFAULT_BLOCK_CACHE_MAX_LEN` 5000 →
  1000, `DEFAULT_RECEIPT_CACHE_MAX_LEN` 2000 → 500), `crates/rpc/rpc-eth-types/src/cache/config.rs`
  (the matching doc lines). A hand port: upstream's context has a block-access-list cache v1.11.4
  lacks.
  *Why*: TN sets no cache limits of its own (`RpcServerArgs` flattens reth's
  `RpcStateCacheArgs`), and a TN block is one batch of up to 1 MB, so the old block default could
  hold 5 GB. Operators who set `--rpc-cache.max-blocks` or `--rpc-cache.max-receipts` see no
  change. RPC memory only. *Tests*: reth-node-core and rpc-eth-types tests (none assert the
  defaults). *Upstream*: n/a (upstream commit).

- **TN-0048** — Backport upstream's pre-execution fixes for traces and replays. *Kind*:
  backport.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/{trace.rs, call.rs}`, `crates/rpc/rpc/src/debug.rs`.
  *Why*: 8fa539225: `Trace::apply_pre_execution_changes` ran reth's Ethereum `SystemCaller`
  instead of the node's block executor, so traces and replays of a TN block wrote the EIP-4788
  beacon root on every block, where TN's executor (`tn-reth/src/evm/block.rs`) writes it only on an
  output's first batch. The post-state matched only because every block of an output carries the
  same root and timestamp. It now calls `executor_for_block(..).apply_pre_execution_changes()`,
  which TN's `TnEvmConfig` supports for re-execution and which reads only the header. The
  conflicts were TN-0036's edits around the three `debug.rs` call sites; only the call's arguments
  change there. c4cd5c9b7: `debug_traceCallMany` at a transaction index replayed the block's
  earlier transactions without the block's pre-execution changes first. 080a9cfc1: the same for
  `Call::spawn_replay_transaction`, which nothing in TN calls today. RPC only: block execution does
  not go through these helpers.
  *Tests*: reth-rpc and rpc-eth-api 43/43 (49/49 with `js-tracer`); tn-reth's trace and debug
  namespace tests, which trace a mined TN block through TN's executor, and its simulation tests
  (`debug_traceCallMany` among them), 29/29. *Upstream*: n/a
  (upstream commits).

- **TN-0049** — Backport upstream's cooperative cancellation of RPC blocking work. *Kind*:
  backport (security).
  *Files*: added `crates/tasks/src/cancel.rs` (upstream's at 13a59de4b: `CancelOnDrop` with its
  finalization state, `ManualCancel`, `scope` and `is_cancelled`), `crates/tasks/src/lib.rs`;
  `crates/rpc/rpc-eth-api/src/helpers/{blocking_task.rs, call.rs, estimate.rs, trace.rs,
  pending_block.rs}`, `crates/rpc/rpc-eth-types/src/simulate.rs`,
  `crates/rpc/rpc/src/{debug.rs, trace.rs}`, `crates/rpc/rpc/src/eth/{bundle.rs, core.rs}`.
  *Why*: a client that disconnects drops its request (over HTTP; over IPC since TN-0177; not
  over WebSocket, where jsonrpsee runs each call to its end), but the blocking task it started
  ran to the end: a long `eth_call`, `eth_callMany`, `eth_estimateGas`, `eth_simulateV1`, bundle, block
  replay or trace kept its thread (and, since TN-0019 and TN-0036 to TN-0038, its permit) for
  work nobody would read. 13a59de4b runs each `spawn_blocking_io` task inside a `CancelOnDrop`
  scope that the request's drop cancels, and the long loops check `is_cancelled()` between
  transactions or binary-search steps and stop. TN's `spawn_with_state_at_block` runs on
  `spawn_blocking_io_fut` instead, so TN wraps that future in `InCancelScope`, which enters the
  scope around each poll; every replay and trace runs there. Also applied: 621f663ed's move of
  `CancelOnDrop` into reth-tasks (`call.rs` imports it from there), except that reth-revm's copy
  stays, so this change leaves the reth-revm crate untouched; 0973a1387 (`eth_simulateV1`), 34f77be92
  (the pending-block build) and 329fcfc73's check in `eth_callBundle`. A stopped request returns
  an internal error to a client that has already left. RPC only: block execution does not run in
  these scopes.
  *Tests*: upstream's `cancel.rs` tests and `blocking_task_observes_dropped_request`; TN's
  `blocking_future_observes_dropped_request`, which fails without `InCancelScope`. *Upstream*:
  n/a (upstream commits).
  *Note (2026-11-10)*: 34f77be92's test `pending_block_build_stops_when_cancelled` was not ported (the pending-block build it drives is not carried). reth-revm's `cancelled.rs` keeps upstream's own `CancelOnDrop`, whose user (upstream's payload builder) TN does not carry; `reth_tasks::CancelOnDrop` is the one TN uses.

- **TN-0050** — Backport 33b785d97: an empty revert returns `0x` as its error data. *Kind*:
  backport.
  *Files*: `crates/rpc/rpc-eth-types/src/error/mod.rs` (`RevertError` keeps its output as
  `Bytes`, with upstream's test). The conflict was TN's older doc line on `RevertError::new`.
  *Why*: a call that reverts with no output returned an error with no `data`, where geth returns
  `"0x"`; clients that decode revert data treat the missing field as a different error. RPC
  output only. *Tests*: upstream's `empty_revert_err_has_empty_data`. *Upstream*: n/a (upstream
  commit).

- **TN-0051** — Backport 44a74f9ea: refuse requests that mix legacy and EIP-1559 fee fields.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/transaction.rs` (`send_transaction_request`,
  `fill_transaction`, `sign_transaction`, and `ensure_no_conflicting_fee_fields`). A hand port:
  upstream's imports and doc context differ.
  *Why*: a request with `gasPrice` and `maxFeePerGas` or `maxPriorityFeePerGas` was built into
  whichever transaction type the conversion picked. It now fails with the error `eth_call`
  already returns for the same input. TN serves `eth_fillTransaction` (its wrapper,
  `tn-reth/src/rpc_fill_transaction.rs`, leaves a request with `gasPrice` untouched, so the check
  sees the client's fields); `eth_sendTransaction` and `eth_signTransaction` have no signers on a
  TN node. RPC input validation only. *Tests*: nested reth-rpc-eth-api tests; tn-reth's
  fill-transaction tests. *Upstream*: n/a (upstream commit).

- **TN-0052** — Backport upstream's Low fixes on the chain-state and storage paths. *Kind*:
  backport.
  *Files*: `crates/chain-state/src/in_memory.rs`, `crates/storage/codecs/src/alloy/header.rs`,
  `crates/storage/provider/src/providers/database/provider.rs`.
  *Why*: none changes what a block executes to or what a datadir stores.
  - 078c22d5f: `BlockState` drops its parent chain in a loop. The derived drop recursed once per
    in-memory ancestor it released, so dropping the head of a long unpersisted chain could
    overflow a thread's stack. Drop only; the conflict was a later upstream test in the same
    module, not taken.
  - 1a6ba945a: `AlloyHeader::from_compact` returns the buffer past the header, not the buffer it
    was given, so a type that decodes a header followed by more fields reads them from the right
    offset. TN stores headers as whole values (static files, MDBX), which ignore the returned
    buffer, so decoded headers and stored bytes are unchanged.
  - e7ebeb869: `receipts_by_block` returns `None` for a non-empty block whose receipt range holds
    fewer receipts than transactions, instead of the partial list. TN's replay for extensions
    (`tn-reth/src/env/helpers.rs`) then reports `ReplayReceiptsMissing` rather than handing an
    indexer a block that looks emptier than it is. TN never prunes, so a consistent datadir answers
    as before. A read path: block execution does not read receipts this way.
  *Tests*: upstream's `deep_chain_drop_preserves_shared_tail`; codecs' compact round-trip tests;
  upstream's `test_receipts_by_block_returns_none_for_missing_unpruned_receipts` and its two
  adjusted pruning tests.
  *Upstream*: n/a (upstream commits).

- **TN-0059** — Backport d46664fc8: `eth_simulateV1` simulates over the state of the block it
  resolved. *Kind*: backport.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`simulate_v1`).
  *Why*: the handler resolved the parent header from the request's block tag, then passed the tag
  again to `spawn_with_state_at_block`, which resolved `latest` a second time. On TN's block rate
  the simulated chain could be built on header N over the state of N+1: `NonceTooLow` under
  `validation: true`, and block numbers that describe a chain that never existed. The
  state is now loaded by the resolved block's hash; `pending` still goes through the pending state.
  RPC only.
  *Tests*: none added; upstream has none (the race needs the tip to move between two reads). The
  simulate tests in `rpc/src/eth/helpers/call.rs` and tn-reth's `simulate_v1` tests cover the
  unchanged behaviour. *Upstream*: n/a (upstream commit).

- **TN-0060** — Backport f12d52de2: `--txpool.max-batch-size` refuses 0. *Kind*: backport.
  *Files*: `crates/node/core/src/args/txpool.rs` (`TxPoolArgs::max_batch_size`, upstream's test).
  *Why*: tokio's `poll_recv_many` returns zero items for a limit of 0 without registering a waker,
  so the pool's transaction batcher returned `Pending` forever and every `eth_sendRawTransaction`
  queued on a oneshot that never answered. TN parses reth's `TxPoolArgs` unchanged
  (`tn-reth/src/cli.rs`), so the value reached TN's pool. The flag now takes 1 or
  more; the default (and every running node) is unchanged. Pool plumbing only.
  *Tests*: upstream's `txpool_max_batch_size_must_be_nonzero`. *Upstream*: n/a (upstream commit).

- **TN-0062** — Backport 5b6863039: a call with a gas price and no `gas` field stays within
  `--rpc.gascap`. *Kind*: backport.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`prepare_call_env`),
  `crates/rpc/rpc/src/eth/core.rs` (upstream's test `call_allowance_respects_rpc_gas_cap`, with a
  `build_test_eth_api_with_gas_cap` helper the vendored tests lacked).
  *Why*: `prepare_call_env` first clamps an omitted gas limit to the RPC cap, then, for a priced
  call, replaces it with the caller's allowance clamped only to the block gas limit, so an operator
  cap below the block limit did not hold for `eth_call`, `eth_estimateGas`, `eth_createAccessList`
  and the trace calls. The allowance is now clamped to the cap as well. TN's defaults
  (cap 50M above the 30M block limit) were unaffected; a lower `--rpc.gascap` now means what it
  says. RPC only.
  *Tests*: upstream's `call_allowance_respects_rpc_gas_cap`; TN-0057's `call_many_budget_tests`.
  *Upstream*: n/a (upstream commit).

- **TN-0063** — Backport dfcb10454: `eth_getTransactionBySenderAndNonce` answers `null` when the
  located block holds no transaction from the sender. *Kind*: backport.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/transaction.rs` (`get_transaction_by_sender_and_nonce`),
  `crates/rpc/rpc/src/eth/core.rs` (upstream's three tests). TN-0036 had already taken the
  commit's mock-provider hunk.
  *Why*: the binary search finds the block in which the sender's nonce passed the requested value,
  then the handler mapped "no matching transaction in that block" onto the same `HeaderNotFound`
  error as a missing block. A nonce can move without a transaction from the sender: EIP-7702
  authorizations, and on TN contract creations and the system-call sender. An
  existing block with no match is now `null`; a block the provider cannot read is still the
  header error. The vendored `TransactionInfo` has no `block_timestamp` field, so that line of
  upstream's change is not taken. RPC only.
  *Tests*: upstream's `test_transaction_by_sender_and_nonce_without_sender_transaction_returns_none`,
  `..._missing_block_returns_error` and `..._returns_matching_transaction`.
  *Upstream*: n/a (upstream commit).

- **TN-0064** — Backport 48df75d88: tracing lookups fail with geth's error code. *Kind*: backport.
  *Files*: `crates/rpc/rpc-eth-types/src/error/mod.rs` (`TracingTransactionNotFound`,
  `TracingBlockNotFound`, `GenesisNotTraceable`, their -32000 mapping, upstream's test),
  `crates/rpc/rpc/src/debug.rs` (`debug_traceBlockByNumber`/`ByHash` and `debug_traceTransaction`
  raise them; the genesis check sits after TN-0038's permit and TN's hash lookup).
  *Why*: `debug_trace*` answered a missing block or transaction with -32001 (`ResourceNotFound`,
  reth's code for `eth` lookups) where geth and the execution-apis tests use -32000; tracing
  genesis failed with "block not found" for the zero parent hash instead of saying why.
  Prerequisite of 5d6634a6f (`debug_traceCall`), which uses these variants. Error
  codes and messages only; RPC only.
  *Tests*: upstream's `tracing_lookup_errors_use_call_execution_failed_code`; the `debug` doc
  pages (`debug_traceBlockByNumber`, `ByHash`, `debug_traceTransaction`) now state -32000.
  *Upstream*: n/a (upstream commit).

- **TN-0065** — Backport 5d6634a6f: `debug_traceCall` at a missing block fails with -32000.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/debug.rs` (`debug_trace_call`, with and without `txIndex`; upstream's
  test `trace_call_out_of_range_block_error` on the permit fixture of `trace_permit_tests`).
  *Why*: the two `debug_traceCall` paths resolved their block through the `eth` helpers and so
  answered an unknown block with `eth`'s -32001 while the other `debug_trace*` methods, after
  TN-0064, use geth's -32000. The `HeaderNotFound` for the requested block is now
  mapped to `TracingBlockNotFound`; TN-0049's cancellation wrapper stays around the call. Error
  code only; RPC only.
  *Tests*: upstream's `trace_call_out_of_range_block_error`, with `eth_call` checked for -32001
  beside it. *Upstream*: n/a (upstream commit).

- **TN-0066** — Backport 8760eb000: `trace_get` selects a frame by its `traceAddress` path.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (method docs), `crates/rpc/rpc/src/trace.rs`
  (`trace_get`, which takes its TN-0038 permit itself like the other traces, where upstream's body
  takes none; `trace_get_index` keeps the flat lookup; upstream's test `trace_get_selects_tree_paths`
  in a new `tests` module, the vendored file had none, without upstream's RPC-cache seeding since
  the vendored mock provider resolves blocks after TN-0036).
  *Why*: `trace_get` took the list as a single flat index into `trace_transaction`'s output and
  answered `null` for any other list length, where OpenEthereum, Erigon and Nethermind read it as
  the frame's path in the call tree. It now returns the frame whose `traceAddress`
  equals the list, `[]` being the top-level call, and `null` when no frame has that path. Clients
  that passed `["0x0"]` for the root now get its first child; `docs/src/rpc-methods/trace/trace_get.md`
  describes the path semantics. RPC only.
  *Tests*: upstream's `trace_get_selects_tree_paths` (a root with two children and a grandchild,
  every path and three misses, over the RPC module). *Upstream*: n/a (upstream commit).

- **TN-0067** — Backport 3dd806a43: `trace_replayTransaction` returns the transaction's hash.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (return type), `crates/rpc/rpc/src/trace.rs`
  (`replay_transaction` and its handler, which keep TN-0038's permit handling; upstream's test
  `replay_transaction_includes_transaction_hash` in the `tests` module without its RPC-cache seeding,
  as TN-0066).
  *Why*: `trace_replayTransaction` returned the bare trace results, where the other Parity-style
  clients and `trace_replayBlockTransactions` wrap them with `transactionHash`. The
  result now has the same shape as one entry of a block replay, so a client can tell which
  transaction a replay belongs to; `docs/src/rpc-methods/trace/trace_replaytransaction.md` shows
  the field. RPC only.
  *Tests*: upstream's `replay_transaction_includes_transaction_hash` (with and without trace types,
  the result carries the hash and equals the block replay's entry). *Upstream*: n/a (upstream
  commit).

- **TN-0068** — Backport e3d7f0952: `trace_replayTransaction` returns `null` for an unknown
  transaction.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (return type), `crates/rpc/rpc/src/trace.rs`
  (`replay_transaction` and its handler, which keep TN-0038's permit handling; upstream's test
  `replay_missing_transaction_returns_null` in the `tests` module),
  `crates/rpc/rpc-builder/tests/it/http.rs` (the `trace` smoke test expects `null`).
  *Why*: a replay of a transaction the node does not know failed with `-32001` and
  `transaction not found`, where `trace_transaction` and `trace_get` answer `null` for the same
  miss. It now answers `null` too, as upstream and the other Parity-style clients
  do; `docs/src/rpc-methods/trace/trace_replaytransaction.md` records the change. RPC only.
  *Tests*: upstream's `replay_missing_transaction_returns_null` (with and without trace types, over
  the RPC module) and the updated `rpc-builder` smoke test. *Upstream*: n/a (upstream commit).

- **TN-0069** — Backport d8d704cf7: `trace_callMany` without a block runs on `latest`.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (method docs), `crates/rpc/rpc/src/trace.rs`
  (`trace_call_many`; upstream's test `trace_call_many_defaults_to_latest` in the `tests` module,
  its imports inside the test as the module's other tests keep them).
  *Why*: an omitted block parameter selected `pending`, where `trace_call`, `eth_call` and the
  other Parity-style clients default to `latest`. A `pending` the client names still
  selects the pending block. `docs/src/rpc-methods/trace/trace_callmany.md` records the default.
  RPC only.
  *Tests*: upstream's `trace_call_many_defaults_to_latest` (an omitted block equals `latest` and
  differs from `pending`, over the API and the RPC module). *Upstream*: n/a (upstream commit).

- **TN-0070** — Backport 074bf32ef: `trace_filter` without `fromBlock` starts at the latest block.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (method docs), `crates/rpc/rpc/src/trace.rs`
  (`trace_filter`; upstream's test `trace_filter_defaults_to_latest` in the `tests` module, its
  imports inside the test as the module's other tests keep them, and a two-permit guard where
  upstream's has one: the vendored `trace_filter` takes its two permits itself (TN-0038), so a
  one-permit guard deadlocks the direct call).
  *Why*: an omitted `fromBlock` meant genesis, so a filter that set neither bound asked for the
  whole chain and failed the `--rpc.max-trace-filter-blocks` range check on any chain longer than
  the limit. Both bounds now default to the latest block, as upstream and the other
  Parity-style clients have it; a filter with only `toBlock` below the latest block fails with
  `-32602` (`fromBlock cannot be greater than toBlock`). `docs/src/rpc-methods/trace/trace_filter.md`
  records the defaults. RPC only.
  *Tests*: upstream's `trace_filter_defaults_to_latest` (an omitted range equals the latest block
  under a one-block limit, a two-block range still passes, and `toBlock` alone below the tip fails
  with `-32602`). *Upstream*: n/a (upstream commit).

- **TN-0071** — Backport 6d117042f: `trace_replayBlockTransactions` fills `vmTrace.code`.
  *Kind*: backport (adapted).
  *Files*: `crates/rpc/rpc/src/trace.rs` (`replay_block_transactions`; upstream's test
  `replay_block_vmtrace_includes_root_and_callcode_bytecode` in the `tests` module without its
  RPC-cache seeding, as TN-0066).
  *Why*: a block replay with `vmTrace` left `code` empty in every frame, where
  `trace_replayTransaction` of the same transaction filled it from the pre-state database.
  Upstream fixes it with revm-inspectors' `into_trace_results_with_state_parts`, which
  exists from 0.44 and not in the vendored 0.34 series (`patches/revm-inspectors` carries 0.34.3).
  The vendored code uses the existing `into_trace_results_with_state` instead, which wants an
  owned `ResultAndState`, so the `vmTrace` path clones the transaction's state changes; requests
  without `vmTrace` keep the old path and copy nothing. The next revm-inspectors bump can take
  upstream's line. RPC only.
  *Tests*: upstream's `replay_block_vmtrace_includes_root_and_callcode_bytecode` (a root that
  CALLCODEs a child, two transactions, three trace-type sets: the root's and child's code appear,
  and each block entry equals the transaction's own replay). *Upstream*: n/a (upstream commit).

- **TN-0072** — Backport 4e1a5574e: `trace_replayBlockTransactions` fails on an unknown block.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/trace.rs` (`replay_block_transactions` looks the block up before
  taking its TN-0038 permit, so a miss costs no permit), `crates/rpc/rpc-builder/tests/it/http.rs`
  (upstream's smoke-test change; the vendored test already expected an error).
  *Why*: a replay of a block the node does not know answered `null`, where `trace_block` and the
  `debug` block tracers fail. It now fails with `-32001` and a `block not found`
  message, as upstream does; `docs/src/rpc-methods/trace/trace_replayblocktransactions.md`
  records it. RPC only.
  *Tests*: the `rpc-builder` trace smoke tests (`test_call_trace_functions_*`) expect the error;
  `replay_block_vmtrace_includes_root_and_callcode_bytecode` covers the known-block path.
  *Upstream*: n/a (upstream commit).

- **TN-0073** — Backport 0c8b1c187: a parked insertion's promotions reach the listeners.
  *Kind*: backport.
  *Files*: `crates/transaction-pool/src/pool/txpool.rs` (`AddedTransaction::Parked` carries the
  transactions the insertion promoted), `crates/transaction-pool/src/pool/mod.rs`
  (`on_added_transaction` notifies them; upstream's three tests and their helpers in the `tests`
  module, without the base test `all_transactions_by_sender_across_reclassification`, which comes
  from an upstream commit TN does not carry, and reading the subpools through a `sender_nonces`
  helper where upstream calls `all_transactions_by_sender`, which that commit added to the trait).
  *Why*: validation can see newer account state than the pool has applied, and inserting a
  transaction then rechecks its sender's others against that state, so lower-nonce transactions
  can become pending while the inserted one stays parked. Those promotions were dropped on the
  floor: no pending-transaction listener, full-transaction listener or event stream heard of them
  until some later pool maintenance. They are now sent as their own notifications,
  once, subject to the usual propagation filter. Pool notifications only: block validity and
  what a worker proposes do not change.
  *Tests*: upstream's `queued_insertion_notifies_older_promotions`,
  `queued_insertion_promotions_respect_propagation_filter` and
  `pending_insertion_notifies_older_promotions`. *Upstream*: n/a (upstream commit).

- **TN-0074** — Backport 0a1d3e761: the validation channel holds two jobs per worker.
  *Kind*: backport (adapted).
  *Files*: `crates/transaction-pool/src/validate/eth.rs` (`build_with_tasks` sizes the channel;
  upstream's test `configured_workers_have_bounded_handoff_capacity` in its `tests` module, on
  this path), `crates/transaction-pool/src/validate/task.rs` (a test-only `max_capacity` accessor
  on `ValidationJobSender`; upstream's `spawn`, which the commit changes, is not in this copy).
  *Why*: the channel from submitters to the validation workers held one job, so with several
  workers each handoff waited for a producer to refill it and the workers idled between jobs.
  The channel now holds two jobs per worker (`2 * (additional_tasks + 1)`), as
  upstream has it. Upstream made the change in `TransactionValidationTaskExecutor::spawn`,
  added after v1.11.4; the vendored path that spawns the workers is
  `EthTransactionValidatorBuilder::build_with_tasks`, which `tn-reth` calls, so the change lands
  there. Pool admission throughput only: what is admitted does not change.
  *Tests*: `configured_workers_have_bounded_handoff_capacity` (0, 1 and 8 extra workers give
  capacities 2, 4 and 18, and each executor validates a transaction). *Upstream*: n/a (upstream
  commit).

- **TN-0075** — Backport fe01db183: validation submitters share the bounded channel without a
  mutex, and a cancelled submit releases the in-flight gauge.
  *Kind*: backport (adapted).
  *Files*: `crates/transaction-pool/src/validate/task.rs` (`to_validation_task` is an
  `Arc<ValidationJobSender>`; `ValidationJobSender::send` decrements `inflight_validation_jobs`
  from a drop guard; upstream's tests `validation_send_metrics_track_cancellation_and_completion`
  and `cloned_executors_share_bounded_queue`; its `spawn` hunk, its
  `validation_service_error_outcomes` helper and the `SameOriginBatchValidator` tests belong to
  upstream commits this copy does not carry and are left out),
  `crates/transaction-pool/src/validate/eth.rs` (`build_with_tasks` builds the executor without
  the mutex; the TN-0074 test reads the capacity directly).
  *Why*: every submitter took one async mutex before sending into the job channel, so the
  TN-0041 parallel workers rarely had more than one queued job, and a submit cancelled while it
  waited for channel space never decremented `inflight_validation_jobs`. The mpsc
  channel is multi-producer and bounded, so it gives the same backpressure without the lock;
  the gauge is decremented when the send future ends, however it ends. Pool admission
  throughput and a metric only: what is admitted does not change.
  *Tests*: upstream's `validation_send_metrics_track_cancellation_and_completion` (eight
  producers wait on a full channel, seven are cancelled, the gauge follows; both with a drained
  slot and a closed channel) and `cloned_executors_share_bounded_queue` (64 concurrent
  submitters over all three submit paths drain through two workers); TN-0074's capacity test.
  *Upstream*: n/a (upstream commit).

- **TN-0076** — Backport 43a7452b0: the `eth_getLogs` retry range leaves out the block that
  overflowed the limit.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc/src/eth/filter.rs` (`get_logs_in_block_range_inner`'s
  `QueryExceedsMaxResults` error and its debug line; upstream's test
  `test_log_limit_retry_range_excludes_overflow_block` on the vendored `EthFilter::new` form;
  TN-0058's `test_range_scan_across_windows` asserts the new bound).
  *Why*: when a range query passed `--rpc.max-logs-per-response`, the error's suggested retry
  range ended at the block whose logs pushed the count over the cap, so a client that retried
  with exactly that range failed again with the same error. The retry range now ends at the
  block before it (or at `fromBlock` when the first block alone overflows, which the
  single-block rule then serves in full). Same logs, same limit; only the hint in the error
  changes. RPC only.
  *Tests*: `test_log_limit_retry_range_excludes_overflow_block` (three blocks with one log each
  and a limit of two: the error names blocks 100 to 101), `test_range_scan_across_windows`
  (the error names blocks 0 to 1,199). *Upstream*: n/a (upstream commit).

- **TN-0081** — Backport 8f5efd2b3: `TrieWalker::new` returns its first read's error.
  *Kind*: backport (adapted).
  *Files*: `crates/trie/trie/src/walker.rs` (`state_trie`, `storage_trie` and `new` return
  `Result`; a TN-written test module holding only upstream's two new tests, since upstream's
  module postdates v1.11.4), `crates/trie/trie/src/trie.rs`, `crates/trie/trie/src/proof/mod.rs`
  and `crates/trie/trie/src/node_iter.rs` (callers propagate or unwrap),
  `crates/trie/db/tests/walker.rs` and `crates/trie/sparse/src/parallel.rs` (test callers unwrap).
  *Why*: the walker's constructor seeked the trie root with `unwrap()`, so an MDBX read error on
  that first read (a reader slot the 5-minute timeout killed, a damaged page) panicked the
  engine's blocking thread inside the state root computation instead of returning an error the
  engine can classify; reached once per state root and per account storage root, and from the
  snapshot import. The error now propagates as `DatabaseError`.
  *Execution path*: yes (`crates/trie`), and the change can not alter a root: every root the old
  code computed, the new code computes identically; the only new behaviour is an error where the
  old code panicked. No manifest or feature change (`graph.py diff` clean).
  *Tests*: upstream's `state_trie_initial_read_timeout_returns_error` and
  `storage_trie_initial_read_timeout_returns_error`; the trie crates' suites.
  *Upstream*: n/a (upstream commit).

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

- **TN-0106** — Backport a774920b7: size the `block_range` buffer for the inclusive span.
  *Kind*: backport.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`block_range`).
  *Why*: the `Vec` for an inclusive block range was allocated one short, so every multi-block read
  reallocated once. Capacity only: the blocks returned are unchanged, so it cannot reach
  execution or storage. *Upstream*: n/a (upstream commit).

- **TN-0107** — Backport 70e776bfe: find a block's static-file range with a B-tree range lookup.
  *Kind*: backport.
  *Files*: `crates/storage/provider/src/providers/static_file/manager.rs`
  (`StaticFileProviderInner::find_fixed_range_with_block_index`).
  *Why*: the lookup walked every file range of the segment for each block; `range(block..)` takes
  the first range whose `max_block` is at or above the block, the same answer in O(log n). It
  picks which file a block is read from or appended to, so it is on the storage path, but the
  result is identical by construction (the index is a `BTreeMap` keyed by `max_block`).
  *Upstream*: n/a (upstream commit).

- **TN-0108** — Backport 7839f3d87: keep one `.csoff` reader per loaded changeset jar.
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

- **TN-0109** — Backport a195b777e: storage v2's `write_state` skips the plain-state conversion
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
  `v2_matches_v1_on_a_generated_chain` (TN-0113) puts the empty bytecode in a bundle and requires
  `Bytecodes` to equal v1's; it fails without the filter. *Upstream*: yes, the `KECCAK_EMPTY`
  filter.

- **TN-0140** — Backport 8572c88fe: the trie node iterator skips its hashed seek only when the
  walker stands exactly at the seek prefix. *Kind*: backport (consensus fix).
  *Files*: `crates/trie/trie/src/node_iter.rs` (the skip condition in `TrieNodeIter::try_next`,
  `key == &seek_prefix` instead of `key.starts_with(&seek_prefix)`; upstream's test). One
  import-only conflict in the test module; the test calls `HashedStorage::from_iter(false, ..)`,
  the pin's signature.
  *Why*: after skipping an unchanged branch, the iterator skipped the seek into the hashed
  entries when the walker's next stored key started with the seek prefix. When that key sits
  deeper than the prefix (the node at the prefix is an extension, which is not stored), hashed
  entries between the prefix and the stored branch, such as a leaf the block inserted there, were
  never visited, and the incremental root differed from a full rebuild of the same state. The
  iterator now seeks unless the walker is exactly at the prefix. Execution path (state root):
  **this changes the computed state root**, in the rare case the old code was wrong: there the old
  root was not the root of the state, and the new one is. Every TN release up to now ran the old
  code (upstream added it in 40015a821, 2025-04, spelled this way since 34fe4c7c5, 2025-06). A
  node with this fix and one without compute different roots, and so different block hashes, on
  any block where the case occurs, and an epoch record commits to the block hash. Mainnet takes it
  at genesis. Adiri takes it with no activation epoch: on 2026-11-14 an observer built with the
  fix synced adiri from genesis, and every certified epoch record's closing block (epochs 0 to
  751, up to block 529,583) matched the observer's own block and the public RPC's, so the case
  did not occur in that history. A block an older binary builds later can still differ on this
  one, so adiri validators upgrade from `v0.17.0-adiri` all at once, never rolling (a committee
  mixing the two binaries splits on such a block), and later history is checked the same way: a
  node with the fix that replays adiri stops at the first epoch whose certified closing block it
  does not reproduce (`check_stored_record`, `crates/node/src/manager/node/close_epoch.rs`), and
  such a block would need an activation epoch. It is rare on hashed keys: a random-trie probe (base tries of 1k to 49k
  accounts, about 155k inserted keys, the incremental root with the old condition against the
  fixed one) found no wrong root, while it flags upstream's crafted shape. The cost is a few extra
  hashed seeks when the walker lands deeper than the prefix.
  *Tests*: upstream's `inserted_leaf_before_deeper_stored_branch_is_included` (seven storage slots
  and one inserted under an extension: the incremental root against a rebuild); it fails without
  the fix (incremental 0x52fe.. against rebuilt 0x6b94..). reth-trie and reth-trie-db suites,
  tn-reth's `trie_overlay` tests and the format-freeze fixtures (their block hashes are unchanged,
  so the case does not occur on that chain). *Upstream*: n/a (upstream commit).

- **TN-0141** — Backport 8d97ab63c, 1b09bf5a2 and 3fba6ad70: `StoredNibblesSubKey` encodes to a
  `[u8; 65]` instead of a `Vec`, and the storage-root span is trace-level and records its root
  without formatting it first. *Kind*: backport.
  *Files*: `crates/storage/db-api/src/models/mod.rs` (`Encode for StoredNibblesSubKey`),
  `crates/trie/common/src/nibbles.rs` (`StoredNibblesSubKey::to_compact_array`; upstream's five
  tests), `crates/trie/trie/src/trie.rs` (`StorageRoot::calculate`'s `instrument`: level `trace`,
  name `storage_trie`, field `addr`; the two `storage_root` records in `calculate_with_cursors` pass
  `tracing::field::debug`). Two conflicts in `trie.rs`: the attribute line, which the pin spells
  differently (TN-0105), takes upstream's line (with it upstream's plain space for a non-breaking
  one in the doc line above); the empty-storage short circuit, whose condition upstream changed in
  a commit TN does not carry, keeps the pin's condition and takes only the record.
  *Why*: every storage-trie seek and write encodes a subkey, one heap allocation each with the
  `Vec`. TN's state root computes a storage root per changed account (TN-0105, TN-0139); the span
  on `calculate` was at INFO, so it was built, with the address formatted, whenever INFO was on for
  `trie::storage_root`, and each root was formatted into a `String` even with the span disabled.
  Execution and storage path: the encoded bytes are the same (64 bytes of nibbles, zero padded,
  then the count; upstream's test compares `to_compact_array` with `to_compact` at every length),
  so the `StoragesTrie` keys are unchanged. The span change reaches logs only; a filter on the old
  span name (`Storage trie`) or field (`hashed_address`) needs the new ones.
  *Tests*: upstream's `test_stored_nibbles_subkey_compact_array_*`; reth-trie-common, reth-db-api,
  reth-trie and reth-trie-db suites; the format-freeze writer test (the `StoragesTrie` rows of its
  fixtures are unchanged). *Upstream*: n/a (upstream commits).

- **TN-0142** — Backport 3fa855645 (its `hash_builder/state.rs` half; the other file,
  `crates/e2e-test-utils`, is not vendored): `HashBuilderState`'s codec roundtrip proptest builds
  the state from per-field strategies. *Kind*: backport (test only).
  *Files*: `crates/trie/common/src/hash_builder/state.rs` (`hash_builder_state_roundtrip` takes
  `key`, `value`, `stack`, the three mask lists and `stored_in_database` from proptest strategies
  and also checks that decoding consumes the whole buffer; the struct drops
  `reth_codecs::add_arbitrary_tests(compact)`, keeping `derive(arbitrary::Arbitrary)`). Applied
  cleanly with `vendor.py pick`.
  *Why*: with `--features arbitrary` the test drew the state through
  `proptest_arbitrary_interop::arb`, whose fixed byte buffer runs out on the variable-length
  fields, so about one run in ten aborted with "initial arbitrary call failed" before the codec
  was checked; the generated `HashBuilderStateTests::proptest` drew it the same way and goes with
  the attribute. No non-test code changes. *Tests*: the roundtrip passes 50 runs in a row (it
  failed 1 to 2 runs in 20 before); reth-trie-common suite. *Upstream*: n/a (upstream commit).

- **TN-0159** — Backport 7bf9241fe: `ProviderFactory::check_consistency` takes its read-only
  provider out of the read transaction timeout. *Kind*: backport.
  *Files*: `crates/storage/provider/src/providers/database/mod.rs` (`check_consistency` calls
  `disable_long_read_transaction_safety` on its provider; `DBProvider` joins the imports). Applied
  with `vendor.py pick`; the one conflict was the import list, which TN had already cut.
  *Why*: TN runs `check_consistency` at every start (`crates/tn-reth/src/env/mod.rs`). A heal over
  a large datadir can hold that transaction past the five-minute default, and both backends then
  fail its later reads (tn-lmdb's `TnLmdbArgs::read_transaction_timeout` copies MDBX's), so the
  start fails the same way each time. The opt-out changes no byte read or written: it only stops
  the timer for that one transaction (tn-lmdb-reth `TnLmdbTx::disable_long_read_transaction_safety`),
  so it cannot change a block, a root or a stored row. *Tests*: tn-reth startup tests; reth-provider
  suite. *Upstream*: n/a (upstream commit).

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
  *Note (2026-11-10)*: the shared blocking pool this entry argues from ended on 2026-11-06, when TN's `rpc_blocking.rs` gave RPC a blocking runtime of its own (tracker, "TN-side hardening"); the permit semantics stay as described.

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

- **TN-0038** — `trace_*` and `debug_executionWitness*` hold their tracing permits until their
  blocking tasks end. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc/src/trace.rs` (the eleven `TraceApiServer` handlers, the public
  methods behind them, and `trace_permit_tests`), `crates/rpc/rpc/src/debug.rs`
  (`debug_execution_witness_for_block`, its two handlers, and `trace_permit_tests`, which now
  covers the witness methods and uses the shared harness), `crates/rpc/rpc/src/permit_test_utils.rs`
  (TN-owned, test only: the harness TN-0036's tests used, moved out so both modules share it),
  `crates/rpc/rpc/src/lib.rs` (declares it), `crates/storage/provider/src/test_utils/mock.rs`
  (test support: `MockEthProvider::recovered_block_range` returns the stored blocks instead of
  none, so `trace_filter` has blocks to trace).
  *Why*: TN-0036 left these as upstream has them: the handler held the permit while the request
  was open, so a request whose client left returned its permit while its blocking thread ran on,
  and repeated requests could occupy any number of the blocking threads that block execution and
  certificate validation also use. Each method now takes its permit where the handler did and moves
  it into the closure its blocking task runs, as TN-0036 does for the debug traces. For the block
  traces (`trace_block`, `trace_replayBlockTransactions`, `trace_blockOpcodeGas`) that closure is
  the per-transaction callback, which the task owns until it ends. `trace_filter` keeps upstream's
  two permits, now in an `Arc` shared by its block fetches and block traces, so they return when the
  last of those tasks ends. `trace_get` takes its permit through `trace_transaction`. Callers of the
  public methods must not hold a tracing permit. `trace_block_storage_access` has no handler and is
  unchanged.
  *Tests*: `trace.rs`'s `trace_permit_tests` drops each of the eleven `trace_*` requests while its
  blocking task is stalled on the account store, with exactly the permits the method takes; a second
  request must not start a blocking task until the first one's ends. `debug.rs`'s test does the same
  for `debug_executionWitness` and `debug_executionWitnessByBlockHash`. All thirteen fail before the
  change. *Upstream*: yes; upstream main still releases these permits on disconnect.
  *Note (2026-11-10)*: the shared blocking pool this entry argues from ended on 2026-11-06, when TN's `rpc_blocking.rs` gave RPC a blocking runtime of its own (tracker, "TN-side hardening"); the permit semantics stay as described.

- **TN-0054** — `eth_callBundle` bounds its transaction count and recovers signatures in its
  blocking task. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc/src/eth/bundle.rs` (`call_bundle`, `EthBundleError::TooManyTransactions`,
  `max_bundle_transactions`, and two tests in `gas_limit_tests`).
  *Why*: the handler decoded and signature-recovered every transaction of the bundle in its
  `async fn` body, before the blocking-IO permit (TN-0037) and the gas budget (TN-0046) applied.
  jsonrpsee polls that body on the runtime that started the server, which on a TN node is the
  runtime consensus runs on; a 15 MB request holds about 100k minimal transactions, seconds of
  recovery per request, and nothing capped the count. The count is now checked first,
  against the most transactions the bundle's gas limit can hold (`call_gas_limit / 21_000`, 2,380
  at the default cap; TN-0046 would refuse the first one over anyway), and decoding, recovery and
  the blob-gas check moved into the blocking closure, after the permit, so the work runs on
  `tn-rpc-blocking`. Same results for every bundle that was accepted before. RPC only.
  *Tests*: `bundle_over_the_transaction_count_is_refused_before_decoding` (2,381 undecodable
  entries fail on the count, so nothing was decoded) and `bundle_at_the_transaction_count_is_decoded`
  (2,380 fail on the decode). *Upstream*: yes; upstream main (6351fbb92) only caches recoveries.

- **TN-0055** — A limit on calls per JSON-RPC batch, on HTTP, WS and IPC. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc-server-types/src/constants.rs` (`DEFAULT_MAX_BATCH_REQUESTS`, 100),
  `crates/node/core/src/args/rpc_server.rs` (`--rpc.max-batch-requests`, at least 1, in
  `RpcServerArgs` and `DefaultRpcServerArgs`), `crates/rpc/rpc-builder/src/config.rs`
  (`http_ws_server_builder` sets jsonrpsee's `BatchRequestConfig::Limit`; `ipc_server_builder`
  sets the IPC one), `crates/rpc/ipc/src/server/mod.rs` (`Settings::max_batch_requests`, the
  builder method, and the test `can_set_max_batch_requests`), `crates/rpc/ipc/src/server/ipc.rs`
  (`process_batch_request` refuses a longer batch with jsonrpsee's own too-big-batch error).
  *Why*: jsonrpsee defaults to unlimited batches and reth never set a limit, and reth's IPC server
  parses batches itself with no limit at all. One 15 MB batch queued about 100k calls on one
  connection, each holding its parsed parameters while waiting on a permit, and made every
  per-request cost (filters, listeners) cheap to repeat. TN's `RpcServerArgs`
  (`tn-reth/src/rpc_server_args.rs`) exposes the flag with the same default. A batch over the
  limit is refused whole with jsonrpsee's error -32010. RPC transport only.
  *Tests*: `can_set_max_batch_requests` (IPC: two calls pass at a limit of two, three are refused);
  jsonrpsee's own limit covers HTTP and WS. *Upstream*: yes.

- **TN-0056** — Caps on installed filters and on the pool listeners RPC clients register. *Kind*:
  fix (security).
  *Files*: `crates/rpc/rpc-server-types/src/constants.rs` (`DEFAULT_MAX_ACTIVE_FILTERS`, 1,000),
  `crates/rpc/rpc-eth-types/src/builder/config.rs` (`EthConfig::max_active_filters`,
  `EthFilterConfig::max_active_filters`), `crates/rpc/rpc-builder/src/config.rs` (passes it),
  `crates/node/core/src/args/rpc_server.rs` (`--rpc.max-active-filters`, at least 1),
  `crates/node/core/src/args/txpool.rs` (fills the new pool field with its default),
  `crates/rpc/rpc/src/eth/filter.rs` (`install_filter` refuses at the cap; the pending-transaction
  filters take their pool listener through the fallible methods; `EthFilterError::TooManyActiveFilters`
  and `TooManyTransactionListeners`, both -32009 server-busy; test `active_filters_are_bounded`),
  `crates/rpc/rpc/src/eth/pubsub.rs` (`newPendingTransactions` subscriptions take their listener
  the same way and fail with the same error), `crates/transaction-pool/src/config.rs`
  (`PoolConfig::max_rpc_tx_listeners`, `MAX_RPC_TX_LISTENERS` 1,024), `crates/transaction-pool/src/traits.rs`
  (`TransactionPool::try_pending_transactions_listener_for` and `try_new_transactions_listener_for`,
  with defaults that never refuse), `crates/transaction-pool/src/lib.rs` (`Pool` implements them),
  `crates/transaction-pool/src/pool/mod.rs` (`try_add_pending_listener`,
  `try_add_new_transaction_listener`, test `rpc_listeners_are_bounded`).
  *Why*: `eth_newFilter`, `eth_newBlockFilter` and `eth_newPendingTransactionFilter` inserted into
  an unbounded map; a filter polled within five minutes never expires and outlives its connection,
  so connection limits did not bound them, and each one can carry a filter near the request size
  limit. Every pending-transaction filter and every `newPendingTransactions` subscription also
  registered a pool listener that every pool insertion walks with a `try_send`, with no bound on
  their number. The node now refuses the filter past `--rpc.max-active-filters`
  (per RPC server, which TN runs one of per worker, and first come across its clients; filters are not tied to connections) and the pool refuses an RPC listener past
  `max_rpc_tx_listeners` of each kind. The pool's unconditional listener methods are unchanged, so
  TN's own listener (`tn-reth/src/txn_pool.rs`) and reth's internal ones are never refused; they
  are counted, so they take slots of the cap. TN's `RpcServerArgs` exposes the filter flag; the listener bound is a pool
  constant. RPC and pool-listener registration only: pool admission and ordering are unchanged.
  *Tests*: `active_filters_are_bounded`, `rpc_listeners_are_bounded`; the rest of the filter,
  pubsub and pool tests. *Upstream*: yes.

- **TN-0057** — `eth_callMany`, `trace_callMany` and `debug_traceCallMany` are bounded as one
  request. *Kind*: fix (security).
  *Files*: added `crates/rpc/rpc-eth-types/src/call_many.rs` (`max_calls`, `check_call_count`,
  `CallManyBudget`, with unit tests), `crates/rpc/rpc-eth-types/src/lib.rs` (declares it);
  `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`call_many`), `crates/rpc/rpc/src/trace.rs`
  (`trace_call_many`), `crates/rpc/rpc/src/debug.rs` (`debug_trace_call_many`),
  `crates/rpc/rpc/src/eth/helpers/call.rs` (`call_many_budget_tests`).
  *Why*: each of the three ran any number of calls in one blocking task with only
  `prepare_call_env`'s per-call gas cap: no call count, no cumulative gas, and cancellation only
  between calls, so a 15 MB request of about 100k calls to a loop contract ran about 5e12 gas on
  one thread, the shape TN-0046 closed for `eth_callBundle`. Each request now refuses,
  before taking a permit or reading state, more calls than its gas cap can hold at the intrinsic
  21,000 each (2,380 at the default cap), and charges every call's gas used against one budget of
  `--rpc.gascap`, failing the request once it is exceeded. A call is charged after it ran, so a
  request can overshoot by at most one call's cap; the budget is on gas used, not gas limits,
  because a call with no `gas` field defaults to the cap. Requests that stayed under the budget
  get the same results as before. RPC only.
  *Tests*: `call_many_budget_tests` (two 21,000-gas transfers pass a 50,000 budget; two 25,800-gas
  calls pass the count check and fail the budget after the second ran; a 42,000 cap refuses three
  calls before any runs);
  `call_many::tests`. The trace and debug variants share the helper and keep their existing
  tests. *Upstream*: yes.

- **TN-0058** — Port the rest of 055fafc72: `eth_getLogs` scans its range window by window.
  *Kind*: backport (security).
  *Files*: `crates/rpc/rpc/src/eth/filter.rs` (`get_logs_in_block_range_inner`, `RangeMode::new`,
  `RangeBlockMode` and its `next`, the constants above them, upstream's test
  `test_range_scan_across_windows`).
  *Why*: TN-0035 took only the chunk permits of 055fafc72 and left the range scan collecting
  every bloom-matching header of the whole range (up to `--rpc.max-blocks-per-filter`, 100,000
  blocks) before reading a single receipt, with the 20,000-log cap applying only in the receipt
  phase. An empty filter matches every bloom and most TN blocks carry no logs, so a 100,000-block
  request over quiet history never tripped the cap and held about 53 to 78 MB of sealed headers
  per request, up to 256 requests at once. The scan now reads one 1,000-header window,
  fetches that window's receipts, appends the logs and checks the cap before the next window,
  as upstream does: memory is one window, the cap can end the query after the first window, and
  a dropped request stops at the next one. Parallel receipt fetching is decided per window (at
  least 1,000 matching headers in it) and its batches hold at most 256 blocks; other windows go
  one block at a time through the receipt cache. Same logs in the same order. Kept from the
  vendored copy: `append_matching_block_logs`'s signature and the retry range in the
  `QueryExceedsMaxResults` error (TN-0076 narrows it as upstream's 43a7452b0 does). RPC only.
  *Tests*: `test_range_scan_across_windows` (2,501 blocks, three with logs, every window fetched
  in parallel; a limit of one log ends the scan in the second window; its retry-range
  assertion follows TN-0076); TN-0035's permit tests. *Upstream*: n/a (upstream commit).

- **TN-0061** — `--rpc-cache.max-concurrent-db-requests` takes 1 up to a semaphore's capacity.
  *Kind*: fix.
  *Files*: `crates/node/core/src/args/rpc_state_cache.rs` (`RpcStateCacheArgs::max_concurrent_db_requests`).
  *Why*: the flag fed a tokio semaphore unchecked. At 0 every RPC cache miss waited forever on a
  permit that never came, holding its connection; above `usize::MAX >> 3` (`Semaphore::MAX_PERMITS`)
  `Semaphore::new` panicked while the RPC server was built. TN flattens this struct into its own
  `RpcServerArgs`, so the flag reaches TN unchanged; TN's own permit flags
  (`--rpc.max-tracing-requests`, at least 2, and `--rpc.proof-permits`) got the same bounds in
  `tn-reth/src/rpc_server_args.rs`. RPC configuration only.
  *Tests*: tn-reth's `permit_flags_are_bounded` parses all three flags through TN's CLI.
  *Upstream*: yes.

- **TN-0077** — `trace_filter` replays as many blocks at once as the permits it holds.
  *Kind*: fix.
  *Files*: `crates/rpc/rpc/src/trace.rs` (`TRACE_FILTER_PERMITS`, `trace_filter`'s chunk loop).
  *Why*: the filter took two tracing permits and then traced every block of a chunk at once,
  up to `--rpc.max-tracing-requests` blocks, each in a blocking task of its own without a
  permit. The permits bounded the number of requests, not the replays: about half the permit
  count in requests, each replaying up to the permit count in blocks, could run at once. The
  chunk's traces now go through an ordered stream buffered to the two permits the
  request holds, so a `trace_filter` request occupies two tracing threads, as the permits say.
  Same traces in the same order; the chunking by `--rpc.max-tracing-requests` still bounds the
  blocks fetched at once. RPC only.
  *Tests*: `trace_filter_defaults_to_latest` and the TN-0038 permit tests run the filter over
  several blocks with a two-permit guard; the bound itself is the stream's buffer.
  *Upstream*: candidate (reth main has the same fan-out).

- **TN-0078** — `debug_traceBlock` bounds a raw block as a `debug_traceCallMany` request.
  *Kind*: fix.
  *Files*: `crates/rpc/rpc/src/debug.rs` (`debug_trace_raw_block`, `trace_block`'s `budget`
  parameter, tests `raw_block_bound_tests`),
  `crates/rpc/rpc-eth-types/src/call_many.rs` (`CallManyBudget::check`).
  *Why*: the raw-block path built the EVM env from the caller's header and ran the caller's
  transactions under it: a header with `gas_limit` `u64::MAX` and no base fee let a legacy
  transaction with gas price 0 and a gas limit of 2^63 loop until it ended, under a tracing
  permit the request held the whole time, and the cancel check of TN-0049 runs only between
  transactions. The signer recovery of up to the body-size limit in transactions
  ran inline in the async handler as well. The raw block is now bounded as the callMany
  methods are (TN-0057): its transaction count may not exceed what `--rpc.gascap` can hold
  (`check_call_count`, before the signer recovery), and its transactions share one
  `CallManyBudget`: a transaction whose gas limit exceeds what is left of the budget fails the
  request with `-32602` before it runs (`check`, the rule a block applies to its own
  transactions), and each one's gas used is charged after it ran. Stored blocks
  (`debug_traceBlockByHash`, `debug_traceBlockByNumber`) pass no budget: the chain bounded
  them already. The in-transaction cancel the review suggested is not added, because the
  budget bounds a request to the gas cap. RPC only.
  *Tests*: `raw_block_transaction_count_is_bounded_by_the_gas_cap` (one transaction more than
  the cap holds fails on the count), `raw_block_transaction_gas_is_bounded_by_the_gas_cap` (a
  transaction with a gas limit above the cap fails before it runs; the header's gas limit is
  `u64::MAX`), `check_refuses_a_gas_limit_above_what_is_left`.
  *Upstream*: candidate (reth main runs raw blocks under the caller's header too).

- **TN-0079** — The struct logger and `vmTrace` stop a trace whose recorded steps pass 160 MiB.
  *Kind*: fix (TN-owned, in the carried revm-inspectors).
  *Files*: `patches/revm-inspectors/src/tracing/config.rs` (`DEFAULT_MAX_RECORDED_BYTES`,
  `TracingInspectorConfig::max_recorded_bytes` set by every preset and kept tightest by `merge`,
  test `presets_cap_recorded_bytes`), `patches/revm-inspectors/src/tracing/mod.rs`
  (`TracingInspector::recorded_bytes` counter, `share_recorded_bytes`, `record_bytes`, the
  accounting in `start_step`; tests module), `patches/revm-inspectors/PATCH.md` and
  `upstream.diff`; `crates/rpc/rpc-eth-api/src/helpers/trace.rs` (`trace_block_until` shares
  one counter across a block's inspectors), `crates/rpc/rpc/src/trace.rs` (`trace_call_many`
  shares one across its calls), `crates/rpc/rpc/src/debug.rs` (test `recorded_bytes_tests`).
  *Why*: the tracing inspector copied the whole stack at every step for the struct logger
  (unless `disableStack`) and memory when `enableMemory` was set, with nothing counting steps or
  bytes; geth's `limit` option is ignored and the 160 MB response limit applies only once the
  trace is serialized. Under the 50M gas cap a call with a 1,000-word stack looping on cheap
  opcodes recorded tens of GB before any response was built, and the node was OOM-killed; a mined
  30M-gas transaction of that shape made every `debug_traceTransaction` or
  `trace_replayTransaction(vmTrace)` of it the same bomb. The inspector now counts the
  bytes its steps hold (the step, its stack snapshot, a fresh memory copy, return data, immediate
  bytes, the pushed values `step_end` records) and, past `max_recorded_bytes`, ends the
  transaction the way TN-0017's interrupt does: the context error becomes `EVMError::Custom`
  ("trace too large: ..."), which the RPC returns as `-32603`, and the halted interpreter cannot
  carry on past a pending action. The default is 160 MiB, reth's default `--rpc.max-response-size`.
  (A step can hold more than its JSON, so the cap can refuse a trace whose response the limit
  would accept, at about a third of the cap for a struct log with a small stack; TN-0178.) The counter survives `fuse`, so `debug_traceBlock*` and
  `debug_traceCallMany`, which reuse one `DebugInspector`, are bounded per request (the mux
  tracer's only since TN-0172); the sites that
  build one inspector per transaction (`trace_block_until` behind `trace_replayBlockTransactions`
  and `trace_filter`, and `trace_callMany`) share a counter. It counted steps only: the call
  traces every preset records, with each call's input, init code and output, stayed uncounted
  until TN-0153, so the call, prestate and parity tracers were still unbounded (the 4byte tracer
  is not a `TracingInspector`). The cap lives in the patched crate rather than a wrapper
  because the recording happens inside `TracingInspector`, which reth constructs at a dozen sites
  and `DebugInspector` wraps privately. RPC only.
  *Tests*: in the patched crate `steps_count_their_bytes`,
  `a_trace_past_the_cap_halts_the_transaction` (a 30M-gas loop with a 16-word stack ends at a
  64 KiB cap with the error, having recorded at most one step past it),
  `without_a_cap_the_transaction_runs_out_of_gas`, `the_counter_survives_fuse_and_is_shared`,
  `presets_cap_recorded_bytes`; in reth `struct_logger_stops_a_trace_past_the_recorded_bytes_cap`
  (the struct logger through `DebugInspector`, and the error maps to `-32603`).
  *Upstream*: candidate for revm-inspectors (0.44.3 caps nothing). On a revm-inspectors upgrade
  the cap must be re-ported; PATCH.md says so.

- **TN-0080** — The nested workspace carries the revm-inspectors patch in `.cargo/config.toml`.
  *Kind*: build only.
  *Files*: new `.cargo/config.toml` (the `[patch.crates-io]` entry and the
  `incompatible-rust-versions` resolver setting), `tn/cargo.sh` (no longer passes them with
  `--config`), `README.md`, `tn/notes/open-issues.md`, `patches/revm-inspectors/PATCH.md`.
  *Why*: `tn/cargo.sh` passed the patch as `cargo --config patch.crates-io...`, and cargo does not
  forward `--config` to an external subcommand: `cargo nextest` ran its inner `cargo test` without
  it, re-resolved revm-inspectors to the registry's 0.34.4 ("Adding revm-inspectors v0.34.4" at
  the top of every nextest log) and built reth's tests against it. So `cargo.sh check`, `tree` and
  `metadata` saw the carried 0.34.3, while every `cargo.sh nextest` run, the `test-vendor-reth`
  lane included, tested the registry crate; the claim in README.md and PATCH.md that reth's tests
  run against the code TN ships did not hold for them. TN's own binary was never affected: the
  root manifest's `[patch.crates-io]` and `Cargo.lock` resolve the path crate. Found when
  TN-0079's reth-side code failed to compile under nextest against a crate that lacked its new
  method. A `.cargo/config.toml` in the nested workspace applies to every cargo run from that
  directory, cargo-nextest's included; `cargo.sh` keeps the seeding and the target directory.
  Verified by `cargo tree -i revm-inspectors` from `vendor/reth` without `cargo.sh`, by
  `lockfile.py check`, and by TN-0079's reth-side tests compiling and passing under nextest.
  *Tests*: n/a (build configuration; the TN-0079 tests exercise it).
  *Upstream*: n/a.

- **TN-0082** — A missing bytecode row fails the lookup instead of executing as empty code.
  *Kind*: fix.
  *Files*: `crates/revm/src/database.rs` (`code_by_hash_ref`; test
  `missing_bytecode_is_an_error_except_for_the_empty_code`),
  `crates/storage/errors/src/provider.rs` (`ProviderError::BytecodeNotFound`).
  *Why*: `StateProviderDatabase::code_by_hash_ref` returned `Bytecode::default()` for a code hash
  the `Bytecodes` table does not hold. On a node whose datadir lost such a row (a torn write, a
  partial snapshot), every call to that contract ran as a no-op and the node committed a root
  the other validators do not; peers then rejected its execution result, and its own logs showed
  no database error. The lookup now fails with `BytecodeNotFound` unless the hash is
  the empty code's (`KECCAK256_EMPTY`), which revm resolves without a lookup anyway, so the block
  fails on the node that lost the row.
  *Execution path*: yes (`crates/revm`, every block and every RPC call). The change can not alter
  a root: whenever the row exists, the code returned is the same as before, and a healthy node
  never takes the new arm; the only new behaviour is an error where the old code silently ran
  empty code and diverged. No manifest or feature change (`graph.py diff` clean).
  *Tests*: `missing_bytecode_is_an_error_except_for_the_empty_code` over a `NoopProvider`.
  *Upstream*: candidate (reth main still returns empty code).

- **TN-0083** — A first launch interrupted between the two genesis commits resumes.
  *Kind*: fix.
  *Files*: `crates/storage/db-common/src/init.rs` (`init_genesis_with_settings`,
  `database_is_empty`; tests `init_genesis_resumes_after_a_torn_first_launch`,
  `init_genesis_refuses_a_partly_written_database`).
  *Why*: genesis initialization writes the genesis header to static files and everything else to
  MDBX, and the commit finalizes the static files before the MDBX transaction. A crash between the
  two left `block_hash(0)` set with no Headers checkpoint, and every restart returned
  `UninitializedDatabase` until the operator deleted the datadir. When the database
  holds no stage checkpoint at all, nothing was ever committed to it, so initialization now
  resumes: `insert_genesis_header` keeps the header already in static files and the MDBX half is
  written. A database with any checkpoint but not the Headers one is still refused.
  The resume holds for storage v1 only, where the header is genesis's one static-file row (the
  transactions and receipts segments get only a block range, which the resumed path sets again).
  Under storage v2 genesis also commits block 0 of both changeset segments to static files, and
  the resumed `insert_genesis_state` fails appending it again (`UnexpectedStaticFileBlockNumber`)
  at every start. TN's node never reaches the resume in either layout: `layout::settle`
  (`crates/tn-reth/src/env/layout.rs`), the only path to `init_genesis_with_settings`, runs
  first; it removes the static files of a creation stopped inside genesis's commit (TN's marker,
  no `storage_settings` row, static files holding only genesis), so genesis writes them from
  scratch, and refuses any other static files beside a database with no chain.
  *Storage path*: yes (`db-common` holds genesis init). The data written is unchanged: the same
  header, state, trie and checkpoints as a clean first launch, since the resumed path is the
  ordinary write path minus the header append. A datadir that already has chain data never
  reaches it (its checkpoints exist). No manifest or feature change.
  *Tests*: `init_genesis_resumes_after_a_torn_first_launch` (storage v1: header in static files,
  MDBX transaction dropped; init succeeds and writes the checkpoints; the next init is the
  ordinary skip), `init_genesis_refuses_a_partly_written_database`; TN's heal is
  `a_creation_stopped_inside_genesis_is_created_again` and
  `static_files_past_genesis_or_without_the_marker_stay_refused` (`layout.rs`).
  *Upstream*: candidate.

- **TN-0084** — The ledger check reads only an entry's *Files* sentence, and the attested run runs it.
  *Kind*: tooling.
  *Files*: `tn/vendor.py` (`FILES_SENTENCE`, `ledger_paths`, `names_path`), `tn/test_vendor.py`
  (`test_prose_tokens_and_bare_directories_name_nothing`), the root `etc/test-and-attest.sh`.
  *Why*: `ledger_paths` took any backticked token of an entry that started with `crates/` as a
  path and treated every token as a possible directory, so the prose "`crates/engine`" in
  TN-0019 and "`crates/revm`" in TN-0049 covered every file of two vendored crates, one of them
  on the execution path, and a future edit there needed no entry to pass. Paths now
  come from the *Files* sentence only, and a token names a directory only with a trailing slash;
  the two prose tokens are reworded, and TN-0014's *Files* sentence names the stub it changed.
  Neither `etc/ci-lanes.sh test-vendor-reth` nor `etc/test-and-attest.sh` ran `vendor.py status`,
  `ledger` or `check`, so the ledger invariant was a manual claim; the attested run
  now runs the three (they need the upstream clone, which CI lacks).
  *Tests*: `test_prose_tokens_and_bare_directories_name_nothing`; the existing ledger tests, with
  the directory token in `test_paths_expand_wrap_and_resolve_against_the_entry_crate` given its
  slash. *Upstream*: n/a.

- **TN-0085** — `lockfile.py check` fails when either workspace resolves revm-inspectors from the
  registry.
  *Kind*: tooling.
  *Files*: `tn/lockfile.py` (`PATCHED`, `unpatched`, `report_unpatched`), `tn/test_lockfile.py`.
  *Why*: the `[patch.crates-io]` entry for `patches/revm-inspectors` applies only while the
  dependency requirement in the vendored manifest (`0.34.2`) matches the patch crate's version
  (0.34.3). A sync to any 0.35 to 0.44.1 makes cargo print "patch was not used" and link the
  registry crate, and the `revm-inspectors-patch` CI job, which tests the patch crate standalone,
  stays green. The check the `test-vendor-reth` lane runs now reads the nested
  resolve and TN's `Cargo.lock` and fails when the crate has a registry or git source. TN-0080
  fixed the nested lane's own fallback to the registry; TN-0079's reth-side tests also fail to
  compile against the registry crate.
  *Tests*: `test_unpatched_reports_registry_sources_in_either_workspace`. *Upstream*: n/a.

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
  creates (TN-0118): lookups route by the index, which still ends at the old tip, and the new jar
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

- **TN-0110** — Storage v2 writes `TransactionHashNumbers` to the database table. *Kind*: fix.
  *Files*: `crates/storage/provider/src/providers/database/provider.rs` (`save_blocks`).
  *Why*: TN-0014 removed `RocksDB`, which wrote this table under v2, but `save_blocks` still
  skipped the database write under v2, so a v2 node could not find a transaction by hash. The
  sorted batch now goes to the table in both layouts, in the `save_blocks` transaction. Every
  reader (`transaction_id`) and the unwind (`remove_blocks_above`) already used the table in both
  layouts (`EitherReader`/`EitherWriter` route it to the database only). Storage path, v2 only:
  the v1 write is unchanged. *Tests*: `v2_matches_v1_on_a_generated_chain` and
  `v2_history_unwind_round_trip` (TN-0113) look every transaction up by hash, and the unwound ones
  must be gone; the first fails with the v2 gate restored. *Upstream*: n/a (upstream writes it to
  `RocksDB`).

- **TN-0111** — Storage v2 writes and unwinds `AccountsHistory` and `StoragesHistory` in the
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
  same code, so v1 datadirs are written as before. *Tests* (TN-0113):
  `v2_matches_v1_on_a_generated_chain` (the tables equal v1's; fails without the v2 insert or
  with the `DoNothing` filter dropped), `v2_history_unwind_round_trip` (fails with either empty
  unwind arm restored), `v2_wipe_of_existing_storage_fails_closed`,
  `v2_wipe_of_storage_written_earlier_in_the_batch_fails_closed` and
  `v2_update_history_indices_refuses_uncommitted_changesets` (each fails with its check
  removed). *Upstream*: n/a (upstream writes these to `RocksDB`; its v2 has the same wipe gap).

- **TN-0112** — Storage v2 keys storage changesets and `StoragesHistory` by the plain slot, as v1
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
    only hashes), so under v2 such a wipe is an error (see TN-0111; impossible since Cancun).
  Storage path, v2 format: any v2 datadir written before this reads wrongly. None is deployed;
  testnet is v1, whose keys were already plain. v1 reads and writes are unchanged.
  *Tests*: `v2_genesis_storage_reads_after_genesis` (db-common; fails with the hashed lookup),
  `test_populate_bundle_state_hashed_with_plain_keys`, `test_write_and_remove_state_roundtrip_v2`
  and the TN-0113 differential. *Upstream*: n/a (upstream commit).

- **TN-0113** — Tests: storage v2 without `RocksDB` against v1 on the same generated chain.
  *Kind*: test.
  *Files*: new `crates/storage/provider/src/providers/database/tn_v2_tests.rs` (TN-owned),
  `.../providers/database/mod.rs` (the `#[cfg(test)]` module line).
  *Why*: the oracle for TN-0109 to TN-0112. A seeded generator builds blocks 1 to 12 with
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
  the fail-closed paths of TN-0111: a v2 wipe of an account with committed storage, and of
  storage written earlier in the same batch, fail (a wipe of an account without storage saves),
  and `update_history_indices` refuses blocks whose static-file changesets are not committed yet
  while re-indexing committed ones to what `save_blocks` wrote. Each test fails with the change
  it guards undone; dropping a wiped account's slots in `HistoryTransitions::from_reverts`
  fails the differential, the unwind round trip, the re-index check and the history check.
  *Upstream*: n/a.

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
  (TN-0118). The TN-0113 differential and unwind round trip now also run with 4 blocks per file
  (four files; the `5..=9` and `10..=12` batches each cross into a new one); the unwind must
  delete the later files, leave two, and read the shorter chain, and the re-save must rebuild the
  full chain's tables. The aborted save of TN-0118 runs with 2 blocks per file under v2: blocks
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
  *Why*: TN-0111's check documented that a wipe of an account with storage cannot happen since
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
  same wipe gap, TN-0111).

- **TN-0117** — Static-file syncs are a plain `fsync` on macOS. *Kind*: fix (performance, macOS
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

- **TN-0118** — `save_blocks` writes each static-file segment's configuration in the segment's
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
  own data and offsets syncs, counted by TN-0119's stats) fails if `sync_and_freeze` leaves the
  writer marked synced. TN's
  `test_startup_heals_static_files_ahead_of_database` (tn-reth) and the engine's late-persist
  tests cover the same states through `RethEnv`. *Upstream*: maybe; it keeps upstream's crash
  semantics.

- **TN-0119** — Per-segment timings of the static-file syncs, for benches. *Kind*: TN feature
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
  append and `fsync`, the `.csoff` `fsync`, the `.conf` freeze (TN-0118) and the index update.
  Each provider keeps its own under a mutex (about ten uncontended locks a persist), so tests
  never share them. TN reads them through `RethEnv::take_static_file_sync_stats` (test-utils).
  Nothing written or synced changes, so this cannot reach execution or storage. *Tests*:
  `sync_stats_record_each_phase_per_segment` (each phase once per segment, `.csoff` only for a
  changeset segment, a take drains them; fails when a phase is not recorded) and tn-reth's
  `test_persist_reports_static_file_sync_stats`. *Upstream*: no.

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
  hashed (on x86_64 crc32fast's PCLMULQDQ path starts at 128 B, and below it its table path takes
  whole 64-byte blocks slice-by-16 and the rest a serial byte at a time: all of a value below
  64 B). On x86_64, values below 128 B are checksummed with the `crc` crate's slice-by-16 table,
  longer ones and
  every value on aarch64 with crc32fast (PCLMULQDQ, or the CRC instructions); the two give the
  same values (`test_checksum_implementations_agree`). Offsets include the four bytes, so the open-time heal, `prune_rows`, rotation and
  deletion, which work on lengths only, keep the framing. `NippyJarCursor::read_value`, the one
  path every value read takes, verifies the checksum before any decompression or decoding and
  hands out the value without it; a mismatch is `NippyJarError::ChecksumMismatch` naming the data
  file, row and column, never a panic, `None` or a default. `NippyJarCursor::unverified_value`
  returns a value's stored bytes without the check, for a search that only compares a prefix to
  steer itself and then reads the row it settles on verified (TN-0124). A heal never acts on a mismatch: it
  checks lengths only, and a damaged value stays an error on every read. Binding the position
  makes an offset that lands exactly on another value's boundary fail as well. A version-1 jar of
  the kinds TN writes (uncompressed and LZ4) is written and read as before in steady state
  (byte-identical files); later entries change version-1 behaviour on other paths: interrupted
  prunes (TN-0130), leftover files (TN-0131, TN-0132), dictionary zstd (TN-0146), damaged offsets
  (TN-0152), a configuration naming another jar (TN-0160), a failed writer (TN-0161). `load` now refuses a version other
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
  configuration without an error. TN-0130 refuses that in checksummed jars, and TN-0162 a final
  offset moved down, which the append-direction arm cut the data file to.

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
  has_checksums` (the cached reader of TN-0108, the count functions, the heal, truncation,
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
  create upstream's format byte for byte. An existing jar is read and appended in its own format;
  TN-0135 refuses a checksummed segment's jar without checksums in a v2 datadir.
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
  prune is healed and published at once; a gap is refused), `rotation_prune_and_appends`
  (prune and appends across rotations keep verifying; it mixed unchecked jars in until TN-0135),
  `missing_row_inside_a_range_is_an_error`,
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
  without the rerun), and every changeset lookup test of the crate (TN-0113, TN-0115, TN-0123).
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
  shrinks), `checksummed_changesets_shorter_than_the_header_are_refused` (an older build's
  changeset prune stopped after its `prune_rows`, before its header commit: the rows and their
  count cut, the block range and records not, built with `prune_rows` on the jar alone; a sidecar
  one record short; a sidecar gone). Mutation-checked: the `.conf`
  written after the cuts, the sidecar cut before the commit, each of the three refusals removed.
  *Upstream*: maybe (the order is an upstream bug; the refusal needs TN-0120's versions).

- **TN-0131** — A jar without committed rows opens whatever a crash left in its files, and an
  empty offsets file is an error, not a panic. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/nippy-jar/src/{consistency.rs, lib.rs, error.rs}`
  (`NippyJarChecker::reset_files`, `DataReader::new`, `NippyJarError::EmptyOffsetsFile`, tests),
  `crates/storage/provider/src/providers/static_file/tn_crash_tests.rs`.
  *Why*: a rotation writes the next jar's configuration (rows 0) durably and creates its data and
  offsets files, the 9-byte offsets header without a sync (the configuration came last until
  TN-0150). A power loss there can leave the configuration and an empty or torn offsets file (ext4's delayed allocation keeps the
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

- **TN-0134** — Format hardening from the checksum review: unknown versions refused before
  decoding, no panics on a version-1 configuration's dictionary fields, a bounded `.csoff` range
  reservation, corrected seed and CRC docs, and tests for the seed terms and a same-row column
  move. *Kind*: fix (static-file robustness) and docs.
  *Files*: `crates/storage/nippy-jar/src/{lib.rs, cursor.rs}` (`NippyJar::decode_config`,
  `NippyJarCursor::read_value`, `value_seed` and `checksum` docs, tests),
  `crates/static-file/types/src/changeset_offsets.rs` (`ChangesetOffsetReader::get_range`,
  tests), and the TN-0120 entry's x86 sentence.
  *Why*: `decode_config` decoded a configuration's body with bincode before it refused an unknown
  version; it now refuses on the version field alone, before any decode. A version-1
  configuration has no checksum, so damage can name dictionary zstd without loaded dictionaries,
  or with fewer than the jar's columns: `read_value` `expect`ed both and indexed by column, and
  now returns `DictionaryNotLoaded` (TN writes no such jar; a version-2 configuration naming it
  is refused at load, TN-0120). `get_range` reserved the caller's record count before reading,
  so a damaged header count asked for any allocation; it now reserves at most what the file
  holds, and a count past the file ends in the read's `UnexpectedEof`. The `value_seed` doc
  claimed two columns' seeds meet only 2^27 rows apart: for the columns 0 to 2 the segments use
  the closest pair is 69,757,738 rows (columns 0 and 2), still more than any jar with more than
  one column holds (headers: 500,000 rows a file). The `checksum` doc and TN-0120 said crc32fast
  is bytewise below 128 B on x86_64; it is bytewise below 64 B and on the tail past whole 64-byte
  blocks (TN-0124 had it right). It cannot reach execution: valid files decode and read as
  before. *Tests*: `test_unknown_version_refused_before_decode` (versions 0, 3 and `u64::MAX`
  with a body bincode cannot decode), `test_dictionary_zstd_without_dictionaries_is_an_error`,
  `test_value_seed_bounds` (rows 2^32 and 2^48 apart differ in every column; the documented
  closest collision, computed and found), `test_checksum_detects_value_moved_to_another_column`,
  `test_record_seed_high_half`, `test_get_range_past_the_file_is_an_error` (a count of 2^60
  records). Mutation-checked: each change reverted, each seed's high-half term and the column
  term removed. *Upstream*: partly (the cursor `expect`s and the reservation are upstream's).

- **TN-0135** — A storage-v2 datadir refuses a jar of a checksummed segment without checksums.
  *Kind*: fix (static-file integrity, policy).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs,
  tn_checksum_tests.rs}` (`StaticFileProviderInner::load_jar`, `UncheckedJarError`, at the loads
  that read a jar's values or heal it: the cache fill and its reload, `prepare_index_update`,
  `update_index`, `check_segment_consistency`, and since TN-0151 and TN-0165 the changeset counts
  and the writer's open; tests), `crates/storage/provider/src/providers/mod.rs`.
  *Why*: the format is the configuration's version (TN-0120), and two flipped bits turn version 2
  into version 1. A checksummed jar read as version 1 hands out its values with the checksum
  appended (a storage changeset's `U256` reads wrong without an error) and its 20-byte `.csoff`
  records at a 16-byte stride. A byte pattern cannot tell that from a real version-1 jar, but the
  datadir's layout can: under storage v2 (the provider's `jar_checksums`, set from the stored
  settings before any read, TN-0122) every new jar of a checksummed segment is version 2, so a
  version-1 jar there is refused (`UncheckedJarError`) at each of those loads. The loads that use
  only the header keep `NippyJar::load`, since a version-1 header decodes as the version-2 one:
  `iter_static_files` (the index, the contiguity check, metrics; TN-0160 checks its names) and
  the deletions (`delete_jar`, `delete_current_and_open_previous`). Storage-v2 datadirs written before TN-0120 hold unchecked jars and no longer
  open: accepted, as storage v2 was never released and development datadirs are recreated.
  Version-1 datadirs are unaffected (checksums off). Read-only providers opened without a
  `ProviderFactory` do not know the layout and keep reading a jar in its own format. It cannot
  reach execution: valid v1 and v2 datadirs read as before. *Tests*:
  `storage_v2_refuses_jars_without_checksums` (unchecked jars under a checksums-on provider:
  changeset, header and writer opens refused, a checksums-off reader reads them; a checksummed
  jar's configuration turned into version 1: its reads refused, the other segment's still answer),
  `rotation_prune_and_appends` (TN-0123's `rotation_prune_and_mixed_formats`, which mixed the
  formats in one datadir, now without the unchecked jars). Mutation-checked: no refusal.
  *Upstream*: no.

- **TN-0136** — `transaction_hashes_by_range` returns a failed read as an error, not a shorter
  list. *Kind*: fix (static-file integrity).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, tn_checksum_tests.rs}`
  (`StaticFileProvider::transaction_hashes_by_range`; test).
  *Why*: each chunk's task dropped the result of its `fetch_range_with_predicate`, so a read
  error (a checksum mismatch, a gap between jars, TN-0133) ended that chunk's hashes early and the
  caller got the hashes before it as the whole answer. The task now sends the error down its
  channel, and the collecting loop returns it. Its one caller in this copy is
  `DatabaseProvider::remove_blocks_above` (an unwind deletes the removed transactions' hash
  entries), which would have left entries behind; TN never unwinds a persisted block (no
  reorgs), so no TN path reaches it today. It cannot reach execution: intact files hash as
  before. *Tests*:
  `damaged_block_segment_value_is_an_error` (TN-0123's, extended: the hashes of a range holding
  the damaged transaction are the checksum error, a range beside it answers). Mutation-checked:
  the error dropped again. *Upstream*: yes (upstream drops the error the same way).

- **TN-0137** — The changeset point lookups refuse a compressed jar before their first raw-key
  probe. *Kind*: fix (static-file robustness).
  *Files*: `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs,
  tn_checksum_tests.rs}` (`changeset_search`, `CompressedChangesetJarError`; test),
  `crates/storage/provider/src/providers/mod.rs`.
  *Why*: TN-0124's search for `get_account_before_block` and `get_storage_before_block` compares
  each probe's stored bytes with the key (`unverified_value`), which is the key only in a jar
  without compression. Nothing checked that: a compressed changeset jar (the writer compresses
  only headers, `create_jar`, but nothing in a jar's configuration forbids one) would steer
  the search by compressed bytes and answer a stored change as absent, or as a corruption error
  when the compressed value is shorter than the key. The search now returns
  `CompressedChangesetJarError` for such a jar before it probes; the verified rerun
  (`changeset_lookup_missed`) decompresses its rows and is not restricted. The block reads
  (`account_block_changeset`, `storage_changeset`) decompress and are unchanged. It cannot reach
  execution: changeset jars are written uncompressed, so no lookup sees the error. *Tests*:
  `compressed_changeset_jar_is_refused_by_the_lookups` (both changeset jars rewritten with LZ4 and
  the same rows: the lookups return the typed error, the block read answers). Mutation-checked: no
  refusal (the account lookup then reports a corrupted changeset). *Upstream*: no (TN-0124's
  search is TN's).

- **TN-0138** — A static-file segment commit syncs its data, offsets and `.csoff` files at once.
  *Kind*: performance (storage commit path).
  *Files*: `crates/storage/nippy-jar/src/{writer.rs, lib.rs}` (new `NippyJarWriter::sync_jobs`
  returning `DataSync` and `OffsetsSync`, the two halves of `sync_all` over disjoint parts of the
  writer; `sync_data`, `commit_offsets` and the unsynced offsets write run through them),
  `crates/storage/provider/src/providers/static_file/writer.rs` (`StaticFileProviderRW::sync_all`
  and `sync_at_once`; a test-only `fail_syncs` fault hook),
  `.../static_file/{sync_stats.rs, manager.rs}` (`SegmentSyncStats::files_sync`; test
  `sync_stats_record_each_phase_per_segment`), `.../static_file/tn_crash_tests.rs` (tests).
  *Why*: under storage v2 a light persist (one block an output) waits on the static files, and
  each segment's commit ran its `.csoff`, data and offsets `fsync`s one after another before its
  `.conf` (quaker, slow drive state: about 7-9 ms of a 24-26 ms segment sync; about 1.2 ms an
  `fsync` on a network disk). Their order never mattered: the configuration bounds what a reopen
  keeps (the heal cuts every file back to it), and it is written only once `sync_all` returns, so
  only all three being durable before it does. `sync_all` now runs them on scoped threads
  (`std::thread::scope`, the last on the calling thread, no new dependency) through the same
  `fsync_file` helper (TN-0117), joins all of them, and returns an error if any failed: the first
  in the old serial order (`.csoff`, data, offsets), which is the one the serial code returned.
  So no `.conf` is written after a failed sync, as before; a sync whose thread cannot start runs
  on the calling thread (TN-0166), and a panic is resumed once the others ended. The `.csoff` length goes into the
  header only after its sync succeeded, as before. A failed call has run the other syncs too (the
  serial code stopped at the first failure), which leaves only bytes past the configuration. A
  failed sync is not retried on the same writer: it writes no configuration again (TN-0161), the
  persist's retry fails at its first `increment_block`, which the engine treats as terminal, and
  the next start's heal cuts those bytes. `commit` (prune, unwind, rotation) keeps its serial
  syncs. The per-file timings now overlap, so `files_sync` times the span of the three, and TN's
  persist bench reads `sf_sync` from it. It cannot reach execution: the bytes written and the
  `.conf` rule are unchanged; only when the three syncs run changes. *Tests*:
  `a_failed_sync_leaves_the_configuration_unwritten` (each of the three syncs failed, both
  formats: the call returns that error, after every file was written when the sync failed after
  its write, and the `.conf` is the last commit's; TN-0161 adds a sync that fails without running,
  and a retried sync and a finalize on the same writer refused; a reopen heals back to the `.conf`
  and the blocks append again)
  and `failed_syncs_return_the_first_in_the_serial_order` (a changeset and a block segment), both
  through the fault hook; `sync_stats_record_each_phase_per_segment` (the span holds each sync);
  the TN-0130 to TN-0137 crash and checksum tests unchanged. Mutation-checked: the error dropped
  (both fail), the last error returned (the order test fails). *Upstream*: maybe; it keeps
  upstream's crash semantics.

- **TN-0139** — `StateRoot` can take an account's storage root from a `StorageRootSource` instead
  of computing it. *Kind*: feature (state root performance).
  *Files*: `crates/trie/trie/src/trie.rs` (trait `StorageRootSource`; `StateRoot`'s field
  `storage_roots`, set by `with_storage_roots` and carried by `with_hashed_cursor_factory` and
  `with_trie_cursor_factory`; the account walk's leaf arm), `crates/trie/trie/src/lib.rs` (export).
  *Why*: TN computes the storage roots of the accounts with changed storage on rayon tasks while
  the account walk runs (`crates/tn-reth/src/env/output_overlay/parallel.rs`): at peak load two hot
  contracts' storage roots are about half of a block's root, and overlapping them with the walk cut
  the exec bench's root by 42% (peak) and 46% (busy) on a Mac. At each account leaf the walk asks
  the source; on `None` it computes the root itself with the shared cursors, exactly as before (the
  `else` arm is the old call, re-indented). A resumed in-progress storage root is always computed
  in place. The walker, hash builder, trie updates and `finalize` are unchanged. Upstream's
  `ParallelStateRoot` (reth-trie-parallel, not carried) does not fit TN: it opens its providers
  through `DatabaseProviderROFactory` with no layered overlay and spawns a task per changed account,
  not per changed storage trie. It can reach execution: without a source the root is computed as
  before; with TN's, the block's root is right exactly when every root the source returns equals
  what the walk would compute. TN checks that: one block in 256 compared with the serial root in
  production (the serial result is used on a difference), every block in shadow mode, and its
  tests. *Tests*: TN's `env::output_overlay::parallel::tests` (parallel equals serial with equal
  trie updates on random multi-block outputs persisted between outputs, one hot contract, many
  small contracts, no storage changes; a moved database tip and failed, panicked or unstarted tasks
  fall back to the walk) and `tests/it/trie_overlay.rs` (the parallel root against reth's
  merged-input oracle). Mutation-checked: the tip check removed, the tasks' prefix set emptied.
  *Upstream*: no (TN's overlay and task model).

- **TN-0143** — The ledger check fails when two entries define the same id.
  *Kind*: tooling.
  *Files*: `tn/vendor.py` (`duplicate_ids`, `cmd_ledger`), `tn/test_vendor.py`
  (`test_reports_unnamed_files_and_undefined_ids`, `test_duplicate_ids_counts_entries_per_id`).
  *Why*: `ledger_problems` reads the defined ids as a set, so a ledger in which two branches each
  took the next free id passed: every file was named and every id defined, but one id meant two
  changes. Merging sls_reth_vendor2 found eleven such ids by hand (TN-0038 to TN-0045 and TN-0050
  to TN-0052) and renumbered one side's. `vendor.py ledger` now prints each id with its entry count
  and fails. *Can reach execution or storage*: no (vendoring tooling only).
  *Upstream*: no (TN's tooling).

- **TN-0144** — A changeset jar's `.csoff` lookups answer from a bounded in-memory cache of the
  records, read a chunk at a time. *Kind*: performance (storage v2 historical reads).
  *Files*: `crates/static-file/types/src/changeset_offsets.rs` (`ChangesetOffsetCache`,
  `CHANGESET_OFFSET_CACHE_BYTES`; `ChangesetOffsetReader`: `with_cache`, `read_chunk`, `get`, a
  `Debug` without the cache's chunks; tests), `.../types/src/lib.rs` (the exports).
  *Why*: under storage v2 every historical account or storage read that resolves to a changeset
  looks up its block's `.csoff` record, and `ChangesetOffsetReader::get` read each one with its
  own 16 or 20 B `pread`: 0.46 us of a 0.8 us changeset lookup on a Mac (TN-0100's harness,
  `sf step csoff`). A lookup now reads its record's whole chunk of 64 records (one `pread` of at
  most 1280 B) into a cache and answers the chunk's later lookups from memory while it stays
  cached. The cache is one for the process (`ChangesetOffsetCache::shared`, which
  `ChangesetOffsetReader::new` uses), shared by every changeset jar of both changeset segments
  and every snapshot of them, and fixed in size: sets of 8 chunks, each behind its own lock, a
  full set evicting its least recently used chunk.
  *Bound*: at most `CHANGESET_OFFSET_CACHE_BYTES` = 4 MiB of records in all (409 sets of 8 chunks
  of 1280 B; plain records' chunks are 1 KiB), plus about 60 B of bookkeeping a chunk (~4.2 MiB),
  whatever the chain's length or the number of jars and snapshots: the records of about 105k
  blocks of both segments. Chosen from the costs (Mac): a cached lookup saves 0.46 us; a miss
  reads a chunk, and at 64 records that costs about one record's `pread` (alloc and `pread` of
  1280 B 0.48-0.61 us against 0.43-0.52 us for 20 B; 5 KiB was 0.61 us), so a working set
  larger than the cache, such as random reads over a long history, runs at about the speed of
  reading each record from the file, and the cache's size only sets how much history answers at
  the cached rate: recent history and repeated lookups, which TN-0100's harness measures (its
  2,400 blocks are 96 KiB of records). An earlier version kept every chunk a reader touched for
  as long as its jar was cached, which for sealed jars is the process's life: 40 B a block of
  history read, about 400 MB for random reads over 10M blocks. Caching only the jar being
  appended to was not taken: a persist reloads that jar, so its cache would start empty every
  persist, and after a rotation recent history is in a sealed jar.
  *Correctness*: a chunk is keyed by its reader's process-unique id (never reused) and its index.
  Each reader is one jar snapshot with a fixed length, so a chunk never changes while its reader
  lives; an append, prune, truncation, heal or rotation loads a new reader, which never meets an
  older reader's chunks, and a dropped reader's chunks are never used again and age out first.
  The stored bytes are kept, and each lookup verifies its record's checksum (TN-0121) as when it
  read it from the file, whether its chunk was cached or read again after an eviction. Chunks
  cover only the records below the header's length that the file held at the open (a length
  past the file, a damaged header, caches nothing past it; TN-0134), and a record outside them,
  or in a chunk that could not be read, is read alone as before, with the same errors. Nothing
  is sized by the header's length: the cache's size is fixed when it is made.
  Measured (Mac, TN-0100 harness): with 256-record chunks kept for the jar's life (2 runs
  against 4), `sf step csoff` p50 0.46 -> 0.04 us, account changeset lookup p50 0.79-0.83 ->
  0.33-0.38 us, unchanged in the remap phase too (a jar reloaded every 32 reads); with the
  bounded cache (2 runs, against 2 of the version before): `sf step csoff` p50 0.04 us in
  every v2 phase (cold, warm, remap, busy, busy+holder), account changeset lookup p50 0.33-0.38
  us (0.38-0.46 before). *Can reach execution or storage*: it reads the same records, so
  a block's offsets are the ones the file holds; it writes nothing.
  *Tests*: `test_chunked_lookups_match_the_file` (both formats, three chunks with a short last
  one, every record twice; a damaged record fails its checksum while its chunk's other records
  answer; a header length past the file answers the file's records and fails past them),
  `test_cache_stays_within_its_bound` (distinct lookups over three files of both formats stay
  within the cache's capacity, which is at most the bytes asked for, and each answers its own
  file's record; the shared cache is within `CHANGESET_OFFSET_CACHE_BYTES`),
  `test_lookup_after_eviction_rereads_and_verifies` (a record damaged while its chunk is cached
  answers from the cache; a later reader in the same set reads it from the file; after the
  chunk is evicted, the lookup reads it again and fails its checksum, or without checksums
  answers what the file holds). Mutation-checked: the record's place in its chunk shifted by one
  (six fail); no eviction, every chunk kept as the earlier
  version did (both new tests fail); the key without the reader's id (the eviction test fails);
  a chunk read on a miss decoded without its checksum (the eviction test and three others
  fail). *Upstream*: no (upstream reads each record with a `pread`).

- **TN-0146** — NippyJar's dictionary zstd writes with its dictionaries, and a reopened writer
  commits. *Kind*: fix (upstream bug). *Files*: `crates/storage/nippy-jar/src/{compression/zstd.rs,
  compression/mod.rs, writer.rs, lib.rs, tn_dictionary_tests.rs}`.
  *Why*: with `Zstd { use_dict: true }` (upstream's format, version 1, which keeps the trained
  dictionaries in the configuration) the writer compressed every value with a dictionary-less
  streaming encoder while the cursor decompressed with the dictionaries; a loaded dictionary kept
  only its decoder form and serialized as `unreachable!()`, so a writer reopened on a dictionary
  jar panicked at its first commit; training (`prepare_compression`) was test-only. Now the writer
  makes one `Compressor` per column from the dictionaries' raw bytes at the jar's level and keeps
  them for its life; a loaded dictionary keeps its raw bytes; training is public. Jars written by
  the old writer still read (`test_zstd_013_compatibility`'s checked-in jar). A checksummed jar
  (version 2, TN-0120) still refuses this compressor at load: its dictionaries would be rewritten
  with the configuration at every commit. *Can reach execution or storage*: no. The provider's
  `create_jar` gives headers jars LZ4 and other segments no compressor, never dictionary zstd,
  and TN never trains a dictionary; a storage-v2 datadir's jars are version 2 (TN-0122), which
  refuses it. The fix changes only how a dictionary jar is written, a jar TN reads but never
  writes, so no jar, `.conf` or digest TN writes changes. *Tests*: `test_dictionary_zstd_round_trip`
  (every value is a frame of the trained dictionary), `test_dictionary_zstd_reopen` (a reopened
  writer commits and keeps the dictionaries), `test_jars_without_a_dictionary_read_back`,
  `test_checksummed_jar_refuses_dictionary_zstd`. *Upstream*: maybe (upstream no longer writes
  dictionary jars).

- **TN-0148** — The static-file jar cache hands readers an `Arc` of the jar, so a persist that
  grows a segment never waits for the readers of its current jar. *Kind*: performance (storage
  commit path under RPC load).
  *Files*: `crates/storage/provider/src/providers/static_file/mod.rs` (`LoadedJarRef` is a struct
  holding an `Arc<LoadedJar>`, no longer the `DashMap` shard's read guard; `LoadedJar::lease`,
  `extending`, `release`), `.../static_file/manager.rs` (the cache holds `Arc<LoadedJar>`;
  `get_or_create_jar_provider`, `prepare_index_update`, `update_index`, `initialize_index`,
  `remove_cached_provider`, `delete_jar`; tests).
  *Why*: a provider held the cache's shard read guard for its whole life, and `update_index`'s
  insert of the new jar waited for every holder of that shard (TN-0101 took this wait out of the
  index lock, not out of the commit). Any reader of a segment's current jar (a range walk, a
  changeset walk, a logs query) so stalled the persist's `commit_sf` for its hold time: on quaker
  with 4 history readers `commit_sf` 0.89 ms (v1) -> 2.41 ms (v2). Readers now clone the jar's
  `Arc` and drop the shard lock at once; a growing persist replaces the cached jar under a brief
  entry lock and returns, and readers of the old jar keep their snapshot, whose files only grew.
  A jar taken out of the cache without its segment growing (a prune or unwind replacing it, the
  writer's open replacing the jar of the file holding the tip, a stale jar above the new range,
  `remove_cached_provider`, `delete_jar`, `initialize_index`) still waits for its readers, as the shard lock made it wait: a prune cuts rows in place and
  the next append writes new rows there, under a reader's mapping. Every snapshot of a file loaded
  while its segment only grew shares one lease (`Arc<()>`), so that wait covers readers of
  snapshots a growing persist already replaced; it polls the lease's count every 50 us, after the
  index lock is dropped (those readers may need it), and only these paths take it. The writer's
  open is the frequent one: it commits and re-indexes the segment at its tip without growing it,
  on the first persist after a start and on every rotation to a new file, so that persist waits
  as long as a reader holds a snapshot of the tip's file (as every persist did before).
  `delete_jar` now loads the deleted jar's configuration from its file. *Can reach execution or
  storage*: it changes which jar snapshot a reader holds and when a commit waits for readers,
  not what is written or read for a block: a reader still sees the rows its index lookup named.
  *Tests*: `growing_commit_does_not_wait_for_a_reader_of_the_jar` (a growing commit returns while
  a reader holds the current jar, which keeps its rows),
  `pruning_commit_waits_for_readers_of_every_earlier_snapshot` (a prune after a growing commit
  waits for a reader of the jar before it, then the pruned block appends again), the TN-0101
  cache tests unchanged (`growing_commit_waits_for_jar_readers_outside_the_index_lock`'s doc
  updated). Mutation-checked: the growing replace waiting for its readers (the first fails); the
  grown jar given a fresh lease (the second fails). *Upstream*: maybe; upstream's cache is the
  same.

- **TN-0149** — A NippyJar writer refuses a checksummed jar that names dictionary zstd before it
  creates any file. *Kind*: fix. *Files*: `crates/storage/nippy-jar/src/{lib.rs, writer.rs,
  tn_dictionary_tests.rs}`.
  *Why*: a checksummed jar (version 2, TN-0120) naming dictionary zstd was refused only by `load`
  (`UnsupportedCompression`). `NippyJarWriter::new` opened it, appended rows and committed, and
  every later load refused the jar, so its rows could never be read again. `load`'s check is now
  `NippyJar::check_compression`, and `NippyJarWriter::new` runs it first, with the same error.
  `save_to_writer` still writes such a configuration: it writes no rows, and the load tests make
  their refused configurations with it. *Can reach execution or storage*: no. The provider's
  `create_jar` gives headers jars LZ4 and other segments no compressor, and TN never trains a
  dictionary, so no jar TN writes is refused and no jar, `.conf` or digest changes.
  *Tests*: `test_checksummed_jar_refuses_dictionary_zstd` (the writer's refusal, and no file
  created; it fails without the writer's check). *Upstream*: no (TN-0120's checksummed format is TN's).

- **TN-0150** — A new NippyJar's configuration is written before its data and offsets files, so a
  creation stopped at any point leaves a datadir the next start opens. *Kind*: fix (static-file
  crash safety). *Files*: `crates/storage/nippy-jar/src/writer.rs` (`create_or_open_files`,
  `NippyJarWriter::new`), `crates/storage/provider/src/providers/static_file/tn_crash_tests.rs`.
  *Why*: `NippyJarWriter::new` created a new jar's data and offsets files and only then wrote its
  configuration (rows 0). Jars are found by their data file (`iter_static_files` loads the
  `.conf` beside each extension-less name), so a kill or power loss in between, at every
  rotation to a new file and at genesis, left a data file with no configuration: every later
  start failed in `iter_static_files`, before `layout::settle` or `check_consistency` ran, until
  someone removed the files by hand. `create_or_open_files` now writes the configuration first,
  then creates the files and syncs their directory (the configuration's own directory sync made
  them durable before, when it came last). A creation stopped after the configuration leaves a
  configuration no listing sees, or one beside a data file; a new jar's header has no block
  range, so the start does not index it either, and the next persist creates the jar again
  (TN-0131 resets a jar without rows). This also replaces a stopped deletion's leftover
  configuration (TN-0132) before a data file appears beside it. *Can reach execution or
  storage*: storage, but no byte changes: the configuration, data and offsets files hold what
  they held, and only the order of their creation moves, plus one directory sync per new jar.
  Nothing a reader or the consistency check sees depends on the order: both find jars through
  their data file and index only jars with a block range. A datadir an older build wrote opens
  as before. *Tests*: `rotation_stopped_before_the_next_jar_has_its_files` (headers and account
  changesets, both versions: a rotation whose configuration write fails leaves no file of the
  next jar, and one stopped after the configuration, or after the data file too, opens; the next
  persist writes the jar and every block reads; it fails with the old order, on the stray data
  file); TN-0167 adds `creation_at_genesis_stopped_before_the_jar_has_its_files` (the first jar of
  a segment). A leftover configuration of a stopped deletion that the new jar replaces is
  TN-0132's `deletion_stopped_at_any_file`. A data file without a configuration that an older
  build's creation left (files first) still fails every start in `iter_static_files`, as before:
  remove that jar's files by hand. *Upstream*: yes (upstream's order).

- **TN-0151** — The changeset counts load each jar through TN-0135's format check and count a
  segment's last file. *Kind*: fix. *Files*:
  `crates/storage/provider/src/providers/static_file/{manager.rs, tn_checksum_tests.rs}`
  (`account_changeset_count`, `storage_changeset_count`; test).
  *Why*: the two counts loaded a jar's configuration with `NippyJar::load`, outside `load_jar`,
  so in a storage-v2 datadir a jar without checksums, which TN-0135 refuses at the loads that read
  values, was counted with its own record size instead of refused. They also named each file by the
  blocks its configuration holds (the range `iter_static_files` lists), not by its fixed range,
  so a segment's last file, which holds fewer blocks than its range until it fills, was never
  found and its changes were left out of the count (upstream's line). *Can reach execution or
  storage*: no. They only read and count, and no TN crate calls them (they back the
  `*_changeset_count` methods of reth's database and consistent providers;
  `StatsReader::count_entries` does not reach them); no byte written or read for a block changes. *Tests*: `storage_v2_refuses_jars_without_checksums` (both counts
  refuse an unchecked jar under v2, and count every block, the last file's too, with checksums
  off and for the segment still checked; it fails without either change). *Upstream*: the file
  name, yes; the format check, no (TN-0135 is TN's).

- **TN-0152** — `DataReader::reverse_offset` refuses a position before an offsets file's second
  byte. *Kind*: fix. *Files*: `crates/storage/nippy-jar/src/lib.rs` (`reverse_offset`, test).
  *Why*: the position of the offset `index` places from the end was the file size less
  `offset_size * (index + 1)`, unchecked. A version-1 jar whose offsets file was cut to 2 to 8
  bytes (damage: the offset size and less than one offset) reaches it from the plain heal, which
  counts no rows in such a file and then reads its final offset: below 8 bytes the subtraction
  underflowed (a panic with overflow checks, a wrapped position and `OffsetOutOfBounds` in a
  release build), and at 8 bytes the offset size was read as the first byte of the offset, whose
  value the heal then cut the data file to. Each is now `OffsetOutOfBounds` for that index; the
  heal has already written a configuration with no rows by then (upstream's heal, which TN-0130
  keeps for version 1), so the next open resets that jar (TN-0131), as before. *Can reach
  execution or storage*: no byte changes for an intact jar. An offsets file holds its size byte
  and whole offsets, and every position read from one is past that byte, so each read computes
  the position it did; a version-2 jar refuses a short offsets file before this point
  (`CommittedRowsMissing`, TN-0130). *Tests*: `test_offsets_file_shorter_than_one_offset_is_an_error`
  (a plain jar's offsets cut to each of 2 to 8 bytes: the reader and the writer's open return
  the error; it panics without the change). *Upstream*: yes (upstream's arithmetic).

- **TN-0153** — TN-0079's trace cap also counts calls: each call's and create's trace, its input
  or init code, and its output.
  *Kind*: fix (TN-owned, in the carried revm-inspectors).
  *Files*: `patches/revm-inspectors/src/tracing/mod.rs` (`charge`, split out of `record_bytes`;
  `trace_too_large`; the charges in `Inspector::call` and `create` and in
  `fill_trace_on_call_end`, which now takes the context; tests),
  `patches/revm-inspectors/src/tracing/config.rs` (the cap's docs),
  `patches/revm-inspectors/PATCH.md` (also its scope, which said the patch matters only to
  JavaScript tracers) and `upstream.diff`; `crates/rpc/rpc/src/debug.rs` (test
  `call_tracer_stops_a_trace_past_the_recorded_bytes_cap`, sharing a helper with TN-0079's test).
  *Why*: TN-0079 counted opcode steps only. Whatever the preset, `TracingInspector` keeps a
  trace for every call and create, with a fresh copy of the call's input (`input_data`), the
  init code and the output. A contract looping on `STATICCALL` with 16 KiB of calldata, about
  130 gas a call, made one `callTracer` call at the 50M gas cap hold 6.4 GiB (6.96 GB peak RSS)
  with nothing counted; `flatCallTracer`, `prestateTracer`, every parity `trace_*` method and the
  struct logger's call traces did the same. `call` and `create` now charge the trace's node
  (384 bytes) and the input or init code before the copy is made, and the end of a call or
  create charges its output. Past the cap the call or create is not made: the hook returns the
  `FatalExternalError` outcome TN-0017's interrupt returns, with the `trace too large` context
  error, which revm takes before it hands the outcome to the calling frame and returns as
  `EVMError::Custom` (`-32603`), as for steps. The refused call's trace is still pushed, without
  its input, because revm runs `call_end` for that outcome too; past the cap an output is not
  kept. Logs and storage changes stay uncounted; gas bounds them (8 gas a byte of log data, 100
  gas or more a storage change). Answers: tracers that return each call's input and output
  (`callTracer`, `flatCallTracer`, parity's `trace`) serialize them in hex, twice what the trace
  holds, so a trace they take past the cap could not have been returned either; the nodes alone
  pass 160 MiB only past 52M gas (120 gas or more a call), beyond the call gas cap and a 30M-gas
  batch. Tracers that return no call input or output (the struct logger, `prestateTracer`,
  parity's `stateDiff` or `vmTrace` alone) now refuse a transaction whose calls copy more than
  160 MiB; they used to answer it after holding all of it. *Can reach execution or storage*: no.
  revm-inspectors runs only for RPC tracing, block execution runs none of its inspectors, and
  the change in `vendor/reth` is a test. Cost: one relaxed atomic add per call frame, on the
  tracing path only.
  *Tests*: in the patched crate `calls_count_their_bytes` (under the default cap a call trace
  completes and counts exactly its nodes, input and output),
  `calldata_past_the_cap_halts_the_transaction`, `output_past_the_cap_halts_the_transaction`,
  `init_code_past_the_cap_halts_the_transaction` and
  `transaction_input_past_the_cap_halts_before_it_runs` (each under the call tracer's and
  parity's presets at a 64 KiB cap; all five fail without the charges); in reth
  `call_tracer_stops_a_trace_past_the_recorded_bytes_cap` (through `DebugInspector`, and the
  error maps to `-32603`).
  *Upstream*: as TN-0079 (revm-inspectors 0.44.3 caps nothing); re-port with it.

- **TN-0155** — A log filter names at most 1,000 addresses and 1,000 values in each topic
  position. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc/src/eth/filter.rs` (`MAX_FILTER_VALUES_PER_POSITION`,
  `check_filter_size`, called by `eth_newFilter` and by `EthFilter::logs_for_filter`, which
  serves `eth_getLogs`, `EngineEthFilter::logs` and `eth_getFilterLogs`;
  `EthFilterError::TooManyFilterValues`, -32602 like the other query limits; tests
  `oversized_log_filters_are_refused`, `oversized_log_subscriptions_are_refused`),
  `crates/rpc/rpc/src/eth/pubsub.rs` (`subscribe` rejects a `logs` subscription over the limit
  before it accepts it).
  *Why*: TN-0056 bounds how many filters are installed, not their size. A filter's address set
  and its four topic sets took any number of values, so one 15 MB `eth_newFilter` kept about
  11 MB of sets and the 1,000 installed filters about 11 GB, for as long as a client polled them.
  A `logs` subscription kept such a filter outside that cap for the life of its connection (up
  to 1,024 per WS or IPC connection), and every `eth_getLogs` and filter poll built a bloom of
  about 256 bytes per value. The limit is geth's (`--rpc.logquerylimit`, default 1,000, which
  geth applies to `eth_newFilter`, `eth_getLogs` and `logs` subscriptions), so a filter geth
  accepts is accepted here. A filter now keeps at most about 0.3 MB of sets, about 300 MB at the
  default 1,000 filters. Upstream reth has no such limit. *Can reach execution or storage*: no.
  RPC only: the check runs before a filter is installed, queried or subscribed, and nothing on
  the block execution or storage path calls it.
  *Tests*: `oversized_log_filters_are_refused` (1,001 addresses, or 1,001 values in the fourth
  topic position, are refused by all three entry points and install nothing; 1,000 in every
  position installs), `oversized_log_subscriptions_are_refused` (refused before acceptance,
  accepted at the limit); `active_filters_are_bounded` and the other filter tests unchanged.
  Mutation-checked: `check_filter_size` accepting every filter (both fail). *Upstream*: yes.

- **TN-0156** — `eth_simulateV1` shares one gas budget of `--rpc.gascap` across all its
  blocks. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc-eth-types/src/call_many.rs` (`CallManyBudget::cap`, test
  `cap_lowers_a_gas_limit_to_what_is_left`), `crates/rpc/rpc-eth-types/src/simulate.rs`
  (`execute_transactions` takes the budget: before each call it refuses the request when less
  than 21,000 gas is left and lowers the call's gas limit, given or default, to what is left;
  after the call it charges the gas used), `crates/rpc/rpc-eth-api/src/helpers/call.rs`
  (`simulate_v1` makes one budget per request), `crates/rpc/rpc/src/eth/helpers/call.rs`
  (tests `simulated_blocks_share_the_gas_budget`, `a_simulated_call_runs_with_at_most_the_budget`).
  *Why*: TN-0057 bounded `eth_callMany` and the trace and debug variants as one request, but
  `eth_simulateV1`, in the same default `eth` namespace, could run 256 blocks
  (`--rpc.max-simulate-blocks`) with any number of calls each, and a call's own `gas` was bounded
  only by its block's gas limit, which a block override may raise to the call cap: about 1.3e10
  gas per request on one blocking thread, with cancellation only between calls. Upstream fixed
  it in b8116b42b (#24387, after v1.11.4); the pick does not apply to this base (its Amsterdam
  gas split needs a newer revm and alloy-evm), so this takes its rule: a call runs with at most
  what the earlier calls of the request left, as geth's `sanitizeCall` caps it. A request's
  calls now use at most `--rpc.gascap` gas in all (50M by default; TN-0171 bounds the
  pre-execution system calls each block also runs), the budget `eth_callMany` has, and
  fails with the same -32602 error (`request exceeds the gas budget of <cap>`). Requests that
  stay under the budget get the same results, except a call whose gas limit, given or default,
  exceeded what was left, which now runs with less gas. How the default is chosen (the block's
  gas split among the calls without `gas`, capped at the call cap) is unchanged. *Can reach execution or storage*: no.
  RPC only: simulated blocks are built on a throwaway state with no state root and are never
  stored, and nothing on the block execution or storage path calls `execute_transactions`.
  *Tests*: `simulated_blocks_share_the_gas_budget` (three one-transfer blocks pass a 63,000
  budget, the third is refused under 50,000), `a_simulated_call_runs_with_at_most_the_budget` (a
  looping call with `gas` 1,000,000 under a 100,000 budget uses 100,000 and fails, and a transfer
  after it is refused), `cap_lowers_a_gas_limit_to_what_is_left`; TN-0057's tests unchanged.
  Mutation-checked: an unlimited budget in `simulate_v1` (both simulate tests fail).
  *Upstream*: no (upstream has b8116b42b).

- **TN-0157** — TN-0056's comments and ledger text say what the code does. *Kind*: fix (docs).
  *Files*: `crates/transaction-pool/src/config.rs` (`PoolConfig::max_rpc_tx_listeners`'s doc),
  `crates/rpc/rpc/src/eth/filter.rs` (`active_filters_are_bounded` asserts the error code), the
  TN-0056 entry above.
  *Why*: TN-0056 said the node's own pool listeners never count against
  `max_rpc_tx_listeners`; they are never refused, but the cap counts every listener on the pool's
  list, so they take its slots (`rpc_listeners_are_bounded` shows it). It also named the
  server-busy error -32604; `ErrorCode::ServerIsBusy` is jsonrpsee's -32009, and the test now
  pins it. TN's filter docs had the same code and now say -32009. *Can reach execution or
  storage*: no; a doc comment and a test assertion.
  *Tests*: `active_filters_are_bounded` (asserts -32009). *Upstream*: n/a.

- **TN-0160** — A jar's configuration must name its own file, and a full checksummed jar's files
  must have its configuration's lengths. *Kind*: fix (static-file integrity).
  *Files*: `crates/storage/db/src/static_file/mod.rs` (`check_static_file_identity`,
  `StaticFileIdentityError`, called by `iter_static_files`), `crates/storage/nippy-jar/src/{lib.rs,
  error.rs}` (`NippyJar::check_file_lengths`, `NippyJarError::FileLengthMismatch`),
  `crates/storage/provider/src/providers/static_file/{manager.rs, mod.rs, tn_checksum_tests.rs}`
  (`StaticFileProviderInner::load_jar`, `LoadedJar::new`; tests).
  *Why*: TN-0120's checksums seed a value with its row and column and TN-0121's a `.csoff` record
  with its index, never with the file, and every change-set jar starts at row 0. So another jar's
  files copied or restored under a jar's name (an operator or tool mistake) verified and served
  the other range's history with no error. Two cheap checks, no format change: (a) every jar
  `iter_static_files` lists (each start, `check_contiguity`) and every load through `load_jar`
  refuses a configuration whose segment differs from its file name's, or whose expected block
  range does not end where the name's range ends or starts before it (a non-zero genesis moves a
  first jar's expected start past its name's, `SegmentHeader::set_expected_block_start`); this
  applies to both jar versions, since no writer names a file other than by its configuration's
  range. (b) `LoadedJar::new` holds a full checksummed jar (its last block in: every jar below a
  segment's latest, which TN-0133's contiguity check requires, and the latest once its last block
  is committed) to its configuration: offsets file `1 + 8 * (rows * columns + 1)` bytes and final
  offset at the data file's end. A full jar takes no more rows (the next block rotates to a new
  file) and a prune commits its configuration first (TN-0130), so only damage or another jar's
  file fails it; a jar still being written is not checked, since it holds bytes past its
  configuration until the next commit. Not caught: same-length data, offsets or `.csoff` files of
  another jar of the segment, and another jar's longer files under a segment's latest name, which
  the open-time heal cuts (`crates/tn-reth/README.md`, "Static-file checksums"). Binding the file
  into the seeds would catch every swap but changes the format. *Can reach execution or storage*:
  storage, by refusing: a datadir whose files a writer made passes both checks, and no byte
  written or value read changes. *Tests*: `a_configuration_must_name_its_own_jar` (the name's
  range, a moved genesis start, another range, another segment, a name that is no data file),
  `jar_files_under_another_jars_name_are_refused` (a swapped `.conf` and the other segment's
  `.conf`, at startup and at a load after it; another jar's data file and offsets file under a
  full jar: refused, other jars still read). *Upstream*: maybe (the name check; the length check
  needs TN-0120's version).

- **TN-0161** — A static-file writer whose sync, offsets write or prune failed writes no
  configuration again. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/nippy-jar/src/{writer.rs, error.rs, lib.rs}`
  (`NippyJarWriter::{mark_failed, has_failed}`, `NippyJarError::WriterFailed`; test),
  `crates/storage/provider/src/providers/static_file/{writer.rs, tn_crash_tests.rs}`
  (`StaticFileProviderRW::{sync_all, commit, increment_block, flush_current_changeset_offset,
  truncate_changesets}`; the test-only `fail_syncs_unrun` hook; test).
  *Why*: after a failure the writer's memory and its files can disagree, and its next commit
  wrote a configuration over them. A failed offsets write drained the offsets it had not written,
  so a later sync wrote none and the `.conf` counted rows without offsets; a `prune_rows` whose
  configuration write or cut failed had already cut its rows and offsets in memory, so the next
  append took its offset from the uncut data end and its seed from the cut row count, and the
  pruned rows' values verified as the new rows; a failed `.csoff` append dropped the block's
  record; and on Linux a retried `fsync` can report success for pages the failed one lost. The
  writer now keeps a `failed` flag, set by its own failed data or offsets sync or offsets write
  and by a failed `prune_rows`, and by the provider (`mark_failed`) when one of `sync_all`'s
  concurrent syncs (TN-0138), a `.csoff` append, sync or truncation fails. A failed writer's
  `finalize` and `prune_rows`, the only writes of its configuration, return `WriterFailed`, so a
  retried persist or commit on it writes nothing; the next start opens a new writer, whose heal
  cuts the files back to the last configuration. Not reachable in TN today: a failed persist's
  retry already fails at its first `increment_block` (`UnexpectedStaticFileBlockNumber`, terminal),
  and the only prunes are the startup heal's, on writers it just opened, which abort the start on
  any error. *Can reach execution or storage*: storage, on the failure path only: a writer that
  never failed writes what it wrote before. *Tests*: `test_interrupted_prune_heals_at_every_point`
  (after a failed prune the same writer's commit and prune are refused; the files are unchanged),
  `a_failed_sync_leaves_the_configuration_unwritten` (TN-0138's, now with a retried sync and a
  finalize refused, and each sync also failed without running). Mutation-checked: no refusal in
  `finalize` (both tests fail). *Upstream*: maybe (the drain and the prune order are upstream's).

- **TN-0162** — The open-time heal verifies a checksummed jar's last committed value before it
  cuts the data file to the final offset. *Kind*: fix (static-file integrity).
  *Files*: `crates/storage/nippy-jar/src/{consistency.rs, lib.rs}` (`NippyJarChecker`'s
  append-direction data arm, `verify_last_value`; test).
  *Why*: a data file longer than the final offset is what an interrupted append leaves, and the
  heal (run at every start on each segment's latest jar) cut it to that offset. The offset itself
  was not checked: a bit cleared in a checksummed jar's final offset made the heal cut committed
  bytes of the last value, after which every read of it failed and the next appends started
  inside it. TN-0130's refusals cover only files shorter than the configuration. Now, in a
  checksummed jar, the value that ends at the final offset (the last committed row's last
  column, starting at the offset before it) must verify before the cut; otherwise the open fails
  with `ChecksumMismatch` naming that row and column and cuts nothing. A version-1 jar keeps
  upstream's heal. *Can reach execution or storage*: storage, on damage only: an intact jar's last
  value verifies and the heal cuts what it cut before. *Tests*:
  `test_heal_refuses_a_final_offset_moved_down` (the final offset lowered by 1, 4 and 8: refused,
  files unchanged; an intact jar with an uncommitted row still healed to its committed files).
  Mutation-checked: no verification (the test fails). *Upstream*: no (TN-0120's checksums).

- **TN-0163** — The changeset sidecar heal refuses committed `.csoff` records that are not one
  contiguous run from row 0. *Kind*: fix (static-file integrity).
  *Files*: `crates/storage/provider/src/providers/static_file/{writer.rs, tn_checksum_tests.rs}`
  (`StaticFileProviderRW::heal_changeset_sidecar`; test).
  *Why*: the heal checked only that each committed record's rows fit the jar's rows and that none
  fit again after one that did not (TN-0121). Records that verify but overlap, skip rows or start
  past row 0 (another jar's sidecar, TN-0160's trigger, or a writer bug; bit rot fails the record
  checksums first) passed, and blocks then read other blocks' rows. The writer gives each block
  the rows after the previous block's, starting at row 0 (`increment_block` takes the jar's row
  count), so the heal, which reads the committed records in one bulk read already, now requires
  each record to start where the previous one's rows end and the first at row 0, and otherwise
  returns `CorruptedChangeSetStaticFile` and cuts nothing. Both jar versions: an interrupted prune
  of an older build leaves a contiguous run past the rows, which the heal still cuts. Only the
  latest jar of each changeset segment is healed at a start; older jars' records are read as
  lookups need them. *Can reach execution or storage*: storage, by refusing: records a writer
  made pass, and nothing written or read changes. *Tests*:
  `sidecar_heal_refuses_records_out_of_order` (a record pointing back into earlier rows, one
  skipping rows, a first record past row 0; both formats: refused, the sidecar unchanged);
  `sidecar_heal_shrinks_a_trailing_run_and_refuses_a_gap` and the TN-0130 crash tests unchanged.
  Mutation-checked: no check (the test fails). *Upstream*: maybe (the run check; the checksums
  are TN-0121's).

- **TN-0164** — `truncate_changesets` syncs the `.csoff` record it flushed before it writes the
  configuration. *Kind*: fix (static-file crash safety).
  *Files*: `crates/storage/provider/src/providers/static_file/writer.rs`
  (`StaticFileProviderRW::truncate_changesets`).
  *Why*: the prune flushed the pending block's `.csoff` record and then wrote the configuration
  (through `prune_rows` or `commit`, whose own sidecar sync comes after `prune_rows`), which can
  count that record, without syncing the sidecar. A crash then left a sidecar shorter than its
  header, which a checksummed jar refuses at the next start (TN-0130). `sync_all` and `commit`
  already sync the sidecar before the configuration; the prune now does too, and a failed sync
  leaves the writer failed (TN-0161). Not reachable in TN: the only prunes are the startup heal's,
  on writers just opened, which hold no pending record. *Can reach execution or storage*:
  storage: one `fsync` more per changeset prune, no byte changes. *Tests*: none new (a sync's
  ordering against a crash is not observable in a test); the TN-0130 crash tests unchanged.
  *Upstream*: maybe.

- **TN-0165** — The static-file writer's open loads the jar through TN-0135's and TN-0160's
  checks. *Kind*: fix (static-file integrity). *Files*:
  `crates/storage/provider/src/providers/static_file/{writer.rs, tn_checksum_tests.rs}`
  (`StaticFileProviderRW::open`; test).
  *Why*: the open loaded the configuration of a jar the cache already held with `NippyJar::load`.
  A checksummed jar's configuration turned into version 1 on disk while the node ran (two flipped
  bits) then ran the version-1 heal, which can cut committed rows, and its commit rewrote the
  configuration, before the index update's `load_jar` refused the jar. A jar the cache does not
  hold was already loaded through `load_jar` on the cache fill. Now the open loads through
  `load_jar` in both cases, so the refusal comes before the heal. *Can reach execution or
  storage*: storage, by refusing: a jar that passes the checks opens as before. *Tests*:
  `storage_v2_refuses_jars_without_checksums` (a cached jar's configuration turned into version 1
  with its offsets file one offset short: the writer's open is refused and both files are left as
  they are; it fails without the change). *Upstream*: no (TN-0135 is TN's).

- **TN-0166** — A static-file sync whose thread cannot start runs on the calling thread, and a
  wait for a file's readers is logged once it passes a second. *Kind*: fix (operability).
  *Files*: `crates/storage/provider/src/providers/static_file/{writer.rs, mod.rs,
  tn_crash_tests.rs}` (`sync_at_once`, the test-only `FAIL_SYNC_THREADS`,
  `LoadedJar::release`; test).
  *Why*: TN-0138 runs each segment commit's `.csoff`, data and offsets syncs on scoped threads
  (under storage v2 up to eight a persist), and a thread that could not start (`EAGAIN` under a
  cgroup pids limit or systemd `TasksMax`) was its sync's error: the persist failed and the node
  stopped. The job now waits in a slot its thread takes it from; when the thread does not start,
  the calling thread takes it and runs it after starting the others, as every sync ran before
  TN-0138, with a warning naming the phase. A sync's own failure is unchanged. TN-0148's
  `LoadedJar::release` waits, with no bound, for readers of a file's earlier snapshots before the
  file shrinks or is replaced (an RPC reader holding a changeset tip file across a rotation); a
  wait now logs a warning naming the segment and range after a second, and again with its length
  when it ends. *Can reach execution or storage*: storage, in timing only: the same syncs run, on
  another thread, before the configuration as before; no byte changes. *Tests*:
  `syncs_run_here_when_their_threads_cannot_start` (no sync thread starts: the changeset commit
  succeeds and its blocks read back, also after a reopen, both formats; it failed before). The
  release warning has no test (a log line after a one-second wait). *Upstream*: no (TN-0138 and
  TN-0148 are TN's).

- **TN-0167** — Tests: crash states of block-segment prunes and of a creation at genesis, the
  offsets-only cut of a changeset prune, and two tests' descriptions. *Kind*: test.
  *Files*: `crates/storage/provider/src/providers/static_file/{tn_crash_tests.rs,
  tn_checksum_tests.rs}`.
  *Why*: TN-0130's provider crash tests covered changeset prunes only, and the state a prune
  stopped between its offsets cut and its data cut only at the jar level; TN-0150's covered
  rotation only. `block_segment_prune_stopped_after_the_configuration_heals` (headers and
  transactions, both formats: `prune_rows`'s configuration with nothing cut, or the offsets alone
  cut, heals at the next open to the finished `prune_rows`'s files, and the kept rows read; for
  transactions the block range is left to `check_consistency`, not run there),
  `changeset_prune_commits_the_header_before_it_cuts` (now also the offsets cut without the data),
  `creation_at_genesis_stopped_before_the_jar_has_its_files` (a segment's first jar stopped after
  its configuration, or after its data file too: the next start opens and the next persist writes
  the jar, headers and account changesets, both formats). The descriptions of
  `checksummed_changesets_shorter_than_the_header_are_refused` and of a case of
  `sidecar_heal_shrinks_a_trailing_run_and_refuses_a_gap` now say which state they build: an
  older build's prune stopped between `prune_rows` and its header commit. *Can reach execution or
  storage*: no; tests only. *Upstream*: n/a.

- **TN-0170** — `eth_callMany` at a transaction index applies the block's pre-execution changes
  before it replays the block's earlier transactions, and checks for cancellation between them.
  *Kind*: fix.
  *Files*: `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`EthCall::call_many`).
  *Why*: with `stateContext.transactionIndex` inside a block, `call_many` replays the block's
  transactions before that index on the parent's state, without the block's pre-execution
  system calls (TN's EIP-4788 root on an output's first batch, the EIP-2935 parent hash on every
  block). A call or replayed transaction that read either contract saw the parent's values: the
  history contract answered zero for the parent's hash on TN. `debug_traceCallMany` (TN-0048)
  and `spawn_replay_transaction` already apply them; this uses the same
  `executor_for_block(..).apply_pre_execution_changes()`. The replay loop also checks
  `is_cancelled()` per transaction, as `debug_traceCallMany`'s does. *Can reach execution or
  storage*: no. RPC only: block execution does not call `call_many`, and the replay runs on a
  throwaway state.
  *Tests*: tn-reth `call_many_at_a_transaction_index_applies_pre_execution_changes` (block 1's
  history contract answers the genesis hash to `eth_call`, to `eth_callMany` and to
  `debug_traceCallMany` at transaction index 0; `eth_callMany` answered zero without the fix).
  *Upstream*: upstream main replays through `Trace::replay_block_until`, which applies them.

- **TN-0171** — `eth_simulateV1` refuses state overrides that replace the code its
  pre-execution system calls run, and checks for cancellation before each block.
  *Kind*: fix (security).
  *Files*: `crates/rpc/rpc-eth-types/src/simulate.rs` (`SYSTEM_CALL_TARGETS`,
  `check_system_call_overrides`), `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`simulate_v1`
  checks every block's overrides before it takes the blocking-IO permit, and `is_cancelled()`
  at the top of each block), `crates/rpc/rpc/src/eth/helpers/call.rs` (`simulate_blocks`, test
  `system_call_targets_keep_their_code`).
  *Why*: every simulated block runs the executor's pre-execution system calls before its calls:
  on TN the EIP-4788 beacon roots contract (an output's first batch, which a simulated block
  is) and the EIP-2935 history contract, each with the executor's system-call gas limit
  (100M on TN) and outside TN-0156's budget. A block's state overrides are applied before them
  and could give either address any code, so a request of 256 blocks
  (`--rpc.max-simulate-blocks`) with no calls could run about a thousand times `--rpc.gascap`, with
  cancellation checked only between calls. Overrides that set `code` on either address, or move
  a precompile onto one, are now refused with -32602 (`state override replaces the code of
  system contract <address>`) before any work; balance, nonce and storage overrides of those
  accounts are still applied. The chain's own contracts make one or two storage writes per
  call, under 100,000 gas a block, so 256 blocks of them stay under 26M gas. Requests that do
  not replace those contracts' code get the same results. *Can reach execution or storage*:
  no. RPC only: the check and the cancellation point run in `simulate_v1`, which block
  execution does not call.
  *Tests*: `system_call_targets_keep_their_code` (new code on either address, empty code, and
  a precompile moved onto the history contract are refused; a balance and storage override of
  it simulates). Mutation-checked: the check removed (the refusals fail).
  *Upstream*: no; upstream has the same shape.

- **TN-0172** — The trace cap also counts the struct logger's logs as they are built, and the mux
  tracer keeps its count across a request's transactions.
  *Kind*: fix (security; TN-owned, in the carried revm-inspectors).
  *Files*: `patches/revm-inspectors/src/tracing/builder/geth.rs` (`struct_log_copies`,
  `GethTraceBuilder::geth_traces_charged`, which `geth_traces` now calls with no limit;
  `fill_geth_trace` takes the charge), `patches/revm-inspectors/src/tracing/mod.rs`
  (`TracingInspector::try_geth_traces`; tests `struct_logs_count_their_memory_copies`,
  `struct_logs_count_their_storage_copies`), `patches/revm-inspectors/src/tracing/debug.rs`
  (`DebugInspector::get_result` builds the struct logger's frame with `try_geth_traces`;
  `DebugInspectorError::TraceTooLarge`; `fuse` fuses the mux tracer instead of rebuilding it),
  `patches/revm-inspectors/src/tracing/mux.rs` (`MuxInspector::fuse`, test
  `fuse_keeps_the_recorded_bytes`), `patches/revm-inspectors/PATCH.md` and `upstream.diff`;
  `crates/rpc/rpc-eth-types/src/error/mod.rs` (`TraceTooLarge` maps to `EvmCustom`, `-32603`,
  as the cap's error during execution does), `crates/rpc/rpc/src/debug.rs` (test
  `struct_logs_past_the_recorded_bytes_cap_are_refused`).
  *Why*: TN-0079 and TN-0153 count what the inspector records. The struct logger's result is
  built after the trace from the recorded steps, and two parts of it were never counted: every
  log gets the step's memory as one 64-character hex `String` per 32-byte word, also when the
  step shares its snapshot with the step before (recorded and counted once), and every SLOAD and
  SSTORE log gets a copy of the contract's storage seen so far. A loop over 4 KiB of memory
  recorded about 200 bytes a step and its logs then held 11 KiB a step, and the storage copies
  grow with the reads times the slots seen, so one `enableMemory` or default trace under the cap could still build
  many GB before the response limit applied. `try_geth_traces` now adds each log's memory
  strings and storage copy (64 bytes an entry) to the same counter as the log is built, and past
  the cap stops and returns the `trace too large` error (`-32603`). The logs' other fields and
  their stacks mirror the recorded steps, which the cap already counts; a struct-logger trace
  then holds at most about twice the cap at its peak. Separately, `DebugInspector::fuse` rebuilt
  the mux tracer from its config, with a fresh counter, so `debug_traceBlock*` and
  `debug_traceCallMany` with `muxTracer` were capped per transaction; it now fuses the tracing
  inspector, which keeps the counter, as the other tracers do. Traces that stay under the cap
  get the same answers. *Can reach execution or storage*: no. revm-inspectors runs only for RPC
  tracing, and the reth change is an error mapping and a test.
  *Tests*: in the patched crate `struct_logs_count_their_memory_copies`,
  `struct_logs_count_their_storage_copies` (steps under the cap, logs past it: refused; with no
  cap the logs build and are counted), `fuse_keeps_the_recorded_bytes`; in reth
  `struct_logs_past_the_recorded_bytes_cap_are_refused` (through `DebugInspector::get_result`,
  `-32603`). Mutation-checked: no charge in `try_geth_traces`, and the mux rebuilt in `fuse`
  (all three patched-crate tests fail).
  *Upstream*: as TN-0079 (revm-inspectors 0.44.3 caps nothing); re-port with it.

- **TN-0173** — A subscription whose connection's send buffer stays full for 10 seconds ends.
  *Kind*: fix (security).
  *Files*: `crates/rpc/rpc/src/eth/pubsub.rs` (`SUBSCRIPTION_SEND_TIMEOUT`,
  `pipe_from_stream_within`, which `pipe_from_stream` calls; test
  `a_subscriber_that_stops_reading_loses_the_subscription`).
  *Why*: `pipe_from_stream`, which feeds `newHeads`, `logs` and `newPendingTransactions`
  subscriptions, awaited each send without a limit. jsonrpsee buffers 1,024 messages per
  connection; once a WS or IPC client stopped reading, the send waited until the connection
  closed and the stream was not polled meanwhile. For `newPendingTransactions` with full
  transactions the stream's pool listener then kept up to `--txpool.max-new-txns` (1,024)
  transactions, evicted and replaced ones included, outside the pool's limits, for each of up
  to 1,024 listeners. Each send now waits at most 10 seconds; then the subscription ends and its stream, with the listener and what
  it held, is dropped. A client reading normally is unaffected; one more than 1,024 messages
  behind for 10 seconds loses the subscription and must subscribe again. *Can reach execution or
  storage*: no. RPC only: subscriptions run in their own tasks, and the pool removes a dropped
  listener at its next send.
  *Tests*: `a_subscriber_that_stops_reading_loses_the_subscription` (a never-read buffer of four
  messages over an endless stream: the stream is dropped once a send waits the timeout).
  Mutation-checked: a plain `send` (the stream is never dropped).
  *Upstream*: no; upstream has the same shape.

- **TN-0174** — A refused `newPendingTransactions` subscription gets the error, not an id.
  *Kind*: fix.
  *Files*: `crates/rpc/rpc/src/eth/pubsub.rs` (`subscribe` takes the pool listener through
  `pending_transactions_listener` before it accepts the subscription; `handle_accepted` shares it
  and `pipe_pending_transactions`; test
  `a_refused_pending_transactions_subscription_gets_the_error`).
  *Why*: TN-0056 says these subscriptions fail with the server-busy error when the pool refuses
  another RPC listener, but `subscribe` accepted the subscription first and the error was dropped
  in the spawned task: the client got a subscription id and then nothing. The listener (and the
  params check) now come before the acceptance, and a refusal rejects the subscription with
  -32009 (`too many pending transaction listeners`), as the pending-transaction filters do.
  Accepted subscriptions behave as before. *Can reach execution or storage*: no. RPC only.
  *Tests*: `a_refused_pending_transactions_subscription_gets_the_error` (with every RPC listener
  slot held, the hash and the full subscription are refused with -32009; once the slots are
  freed both are accepted). Mutation-checked: acceptance before the listener (the subscription
  is accepted).
  *Upstream*: n/a (TN-0056's cap is TN's).

- **TN-0175** — Raw `debug_traceBlock` decodes its block and recovers its signers in a blocking
  task. *Kind*: fix.
  *Files*: `crates/rpc/rpc/src/debug.rs` (`debug_trace_raw_block`).
  *Why*: the handler decoded the caller's RLP block (up to the request size limit), checked
  TN-0078's call count and recovered up to 2,380 signers (a parallel iterator, on a node built
  with rayon) in the `async fn`, on the runtime that polls RPC handlers. That work now runs in a
  `spawn_blocking_io` task that holds the trace permit taken first, and hands the permit on to
  the trace, as `eth_callBundle` does since TN-0054. The order of the checks and the errors are
  unchanged. *Can reach execution or storage*: no. RPC only.
  *Tests*: the raw-block tests (`raw_block_transaction_count_is_bounded_by_the_gas_cap`,
  `raw_block_transaction_gas_is_bounded_by_the_gas_cap`) and the trace permit tests, unchanged;
  where the work runs has no test.
  *Upstream*: no; upstream decodes in the handler.

- **TN-0176** — Tests for the gas budget at the trace and debug call sites. *Kind*: test.
  *Files*: `crates/rpc/rpc/src/permit_test_utils.rs` (`MockChain::eth_api_with_gas_cap`,
  `MockChain::call_of_25_800_gas`), `crates/rpc/rpc/src/debug.rs` (tests
  `raw_block_charges_each_transaction_to_the_budget`,
  `trace_call_many_calls_share_the_gas_budget`), `crates/rpc/rpc/src/trace.rs` (test
  `trace_call_many_calls_share_the_gas_budget`).
  *Why*: TN-0057 and TN-0078 charge each call's or transaction's gas to one budget per request
  in `trace_callMany`, `debug_traceCallMany` and raw `debug_traceBlock`, and only
  `eth_callMany`'s charge had a test. In the raw block the charge never fails by itself (the
  check before each transaction already holds it to what is left), so deleting it would have
  checked every transaction against the whole cap unnoticed. Each test now fails when its
  site's `charge` is removed: two 25,800-gas calls under a 50,000 cap, and two transfers with a
  30,000 gas limit, of which the second fits only if the first was not charged. The
  cancellation checks in these loops still have no test of their own. *Can reach execution or
  storage*: no; tests.
  *Tests*: the three above. Mutation-checked: each site's `charge` removed (each test fails).
  *Upstream*: n/a.

- **TN-0177** — An IPC call whose client disconnects is aborted. *Kind*: fix.
  *Files*: `crates/rpc/ipc/src/server/mod.rs` (`AbortOnDrop` around the spawned call; test
  `a_disconnected_clients_call_is_dropped`).
  *Why*: the IPC server spawns each call on its own task, for concurrency, and the connection
  awaited the task's `JoinHandle`. When the client went away the connection dropped the handle,
  which does not cancel a task, so the request future was never dropped: TN-0049's
  cancellation never fired and the call kept its blocking thread and permits until it ended. The
  handle is now aborted when the connection drops it, so the request future is dropped and its
  blocking work sees `is_cancelled()` at its next check. A call that already ended is
  unaffected. jsonrpsee's WebSocket transport also spawns each call and does not cancel it; that
  is the library's, outside this copy (`tn/notes/open-issues.md`). *Can reach execution or
  storage*: no. RPC only.
  *Tests*: `a_disconnected_clients_call_is_dropped` (a call that never ends is dropped once its
  client disconnects). Mutation-checked: no abort (the call keeps running).
  *Upstream*: no; upstream has the same shape.

- **TN-0178** — The revm-inspectors patch's docs list everything it carries and say when the
  trace cap refuses a trace the response limit would accept. *Kind*: fix (docs).
  *Files*: `patches/revm-inspectors/PATCH.md` ("Scope" lists the recursion limit, the trace cap
  with its files, and the removed `.gitignore`; the out-of-workspace test command runs every
  test; the cap's paragraph), `patches/revm-inspectors/src/tracing/config.rs`
  (`DEFAULT_MAX_RECORDED_BYTES`'s doc) and `upstream.diff`; the TN-0079 entry above; TN's
  `docs/src/rpc-methods/debug/{README.md, debug_tracetransaction.md}` and
  `docs/src/rpc-methods/trace/README.md`.
  *Why*: "Scope" said the patch mattered to JavaScript tracers only, although the trace cap
  reaches every build, so a maintainer could drop the patch from a default build and lose the
  cap; its fallback test command ran only the JavaScript tests. The cap's docs said a trace past
  the cap could not have been returned anyway. That holds for call input and output, which
  serialize in hex, but a struct-log step holds about 200 bytes against 60 to 100 of JSON with a
  small stack, and a `vmTrace` step copies the whole memory after each write, so the cap can
  refuse traces the response limit would accept, and raising `--rpc.max-response-size` does
  not raise it. The docs now say so. *Can reach execution or storage*: no; docs and a doc
  comment.
  *Tests*: n/a. *Upstream*: n/a.

- **TN-0180** — `populate_bundle_state_hashed` refuses a hashed storage changeset key with an
  error in every build. *Kind*: fix. *Files*: `crates/storage/provider/src/providers/database/
  provider.rs` (`populate_bundle_state_hashed`, test).
  *Why*: TN-0112 made changeset keys plain and guarded this function's plain-key precondition with
  a `debug_assert!`, which release builds drop. `StorageSlotKey::Hashed` is still a variant (an
  upstream sync that brings back hashed changesets, as upstream's v2 had, would produce it), and
  with a hashed key the function keyed the bundle and reverts by the hash, looked `HashedStorages`
  up with it unchanged, and its callers hashed it again: an unwind would leave the real row and
  write the old value under `keccak(keccak(slot))`. It now returns `DatabaseError::Other`
  before using the key. The cost is one enum tag compare per storage changeset entry, beside the
  entry's `HashedStorages` seek, and only on this path. *Can reach execution or storage*: storage
  only, and only to fail closed: the function runs under `remove_state_above` and
  `take_state_above` (the unwind and take paths, which TN's node does not call), not on the
  persist path, and every changeset reader tags its keys `Plain`, so no current caller sees the
  error. *Tests*: `test_populate_bundle_state_hashed_refuses_a_hashed_key` (panics on the
  `debug_assert!` without the change); `test_populate_bundle_state_hashed_with_plain_keys`
  unchanged. *Upstream*: n/a (upstream removed `StorageSlotKey` in 815037e27).

- **TN-0181** — `eth_simulateV1` holds its tracing permit for as long as its blocking work runs.
  *Kind*: fix. *Files*: `crates/rpc/rpc-eth-api/src/helpers/call.rs` (`simulate_v1` takes the
  permit after its cheap checks and moves it into the blocking work with the blocking-IO one),
  `crates/rpc/rpc-eth-api/src/core.rs` (the `eth_simulateV1` handler no longer takes it),
  `crates/rpc/rpc/src/eth/helpers/call.rs` (`simulation_chain`, test
  `simulate_takes_a_tracing_permit`); TN's `crates/tn-reth/src/rpc_call.rs` (its handler no
  longer takes it either).
  *Why*: the handler took the eth API's tracing permit (the `--rpc.proof-permits` semaphore,
  25 by default, which `eth_getProof` also takes) and held it in the request future, while
  `simulate_v1` moved only the blocking-IO permit into its blocking work. When an HTTP client
  disconnected, the future was dropped and the permit returned at once, while the simulation
  ran on to its next cancellation check (the end of a call of up to `--rpc.gascap` gas, or of a
  block's system calls), so a client that sent requests and dropped them could run more
  simulations at once than there are permits. The handler also ignored the acquire's error; a
  closed semaphore now fails the request instead of running it without a permit. Requests the
  cheap checks refuse (too many blocks, no blocks, a system contract's code overridden) no
  longer wait for a permit. *Can reach execution or storage*: no. RPC only.
  *Tests*: `simulate_takes_a_tracing_permit` (a request waits while the only permit is held and
  runs once it is free). Mutation-checked: no permit taken in `simulate_v1` (the request runs
  while the permit is held).
  *Upstream*: no; upstream has the same shape (its handler in `rpc-eth-api/src/core.rs`).
