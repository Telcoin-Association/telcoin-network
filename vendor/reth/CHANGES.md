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
  *Why*: a client that disconnects drops its request, but the blocking task it started ran to
  the end: a long `eth_call`, `eth_callMany`, `eth_estimateGas`, `eth_simulateV1`, bundle, block
  replay or trace kept its thread (and, since TN-0019 and TN-0036 to TN-0038, its permit) for
  work nobody would read. 13a59de4b runs each `spawn_blocking_io` task inside a `CancelOnDrop`
  scope that the request's drop cancels, and the long loops check `is_cancelled()` between
  transactions or binary-search steps and stop. TN's `spawn_with_state_at_block` runs on
  `spawn_blocking_io_fut` instead, so TN wraps that future in `InCancelScope`, which enters the
  scope around each poll; every replay and trace runs there. Also applied: 621f663ed's move of
  `CancelOnDrop` into reth-tasks (`call.rs` imports it from there), except that reth-revm's copy
  stays, so this change leaves `crates/revm` untouched; 0973a1387 (`eth_simulateV1`), 34f77be92
  (the pending-block build) and 329fcfc73's check in `eth_callBundle`. A stopped request returns
  an internal error to a client that has already left. RPC only: block execution does not run in
  these scopes.
  *Tests*: upstream's `cancel.rs` tests and `blocking_task_observes_dropped_request`; TN's
  `blocking_future_observes_dropped_request`, which fails without `InCancelScope`. *Upstream*:
  n/a (upstream commits).

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
  `validation: true`, and block numbers that describe a chain that never existed (review R8). The
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
  (`tn-reth/src/cli.rs`), so the value reached TN's pool (review R13). The flag now takes 1 or
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
  and the trace calls (review R24). The allowance is now clamped to the cap as well. TN's defaults
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
  authorizations, and on TN contract creations and the system-call sender (review R22). An
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
  Prerequisite of 5d6634a6f (`debug_traceCall`, review R26), which uses these variants. Error
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
  TN-0064, use geth's -32000 (review R26). The `HeaderNotFound` for the requested block is now
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
  the frame's path in the call tree (review R26). It now returns the frame whose `traceAddress`
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
  clients and `trace_replayBlockTransactions` wrap them with `transactionHash` (review R26). The
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
  miss (review R26). It now answers `null` too, as upstream and the other Parity-style clients
  do; `docs/src/rpc-methods/trace/trace_replaytransaction.md` records the change. RPC only.
  *Tests*: upstream's `replay_missing_transaction_returns_null` (with and without trace types, over
  the RPC module) and the updated `rpc-builder` smoke test. *Upstream*: n/a (upstream commit).

- **TN-0069** — Backport d8d704cf7: `trace_callMany` without a block runs on `latest`.
  *Kind*: backport.
  *Files*: `crates/rpc/rpc-api/src/trace.rs` (method docs), `crates/rpc/rpc/src/trace.rs`
  (`trace_call_many`; upstream's test `trace_call_many_defaults_to_latest` in the `tests` module,
  its imports inside the test as the module's other tests keep them).
  *Why*: an omitted block parameter selected `pending`, where `trace_call`, `eth_call` and the
  other Parity-style clients default to `latest` (review R9). A `pending` the client names still
  selects the pending block. `docs/src/rpc-methods/trace/trace_callmany.md` records the default.
  RPC only.
  *Tests*: upstream's `trace_call_many_defaults_to_latest` (an omitted block equals `latest` and
  differs from `pending`, over the API and the RPC module). *Upstream*: n/a (upstream commit).


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

- **TN-0054** — `eth_callBundle` bounds its transaction count and recovers signatures in its
  blocking task. *Kind*: fix (security).
  *Files*: `crates/rpc/rpc/src/eth/bundle.rs` (`call_bundle`, `EthBundleError::TooManyTransactions`,
  `max_bundle_transactions`, and two tests in `gas_limit_tests`).
  *Why*: the handler decoded and signature-recovered every transaction of the bundle in its
  `async fn` body, before the blocking-IO permit (TN-0037) and the gas budget (TN-0046) applied.
  jsonrpsee polls that body on the runtime that started the server, which on a TN node is the
  runtime consensus runs on; a 15 MB request holds about 100k minimal transactions, seconds of
  recovery per request, and nothing capped the count (review R1). The count is now checked first,
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
  per-request cost (filters, listeners) cheap to repeat (review R27). TN's `RpcServerArgs`
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
  and `TooManyTransactionListeners`, both -32604 server-busy; test `active_filters_are_bounded`),
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
  their number (review R2). The node now refuses the filter past `--rpc.max-active-filters`
  (node-wide; filters are not tied to connections) and the pool refuses an RPC listener past
  `max_rpc_tx_listeners` of each kind. The pool's unconditional listener methods are unchanged, so
  TN's own listener (`tn-reth/src/txn_pool.rs`) and reth's internal ones never count against the
  cap or get refused. TN's `RpcServerArgs` exposes the filter flag; the listener bound is a pool
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
  one thread, the shape TN-0046 closed for `eth_callBundle` (review R6). Each request now refuses,
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
  per request, up to 256 requests at once (review R5). The scan now reads one 1,000-header window,
  fetches that window's receipts, appends the logs and checks the cap before the next window,
  as upstream does: memory is one window, the cap can end the query after the first window, and
  a dropped request stops at the next one. Parallel receipt fetching is decided per window (at
  least 1,000 matching headers in it) and its batches hold at most 256 blocks; other windows go
  one block at a time through the receipt cache. Same logs in the same order. Kept from the
  vendored copy: `append_matching_block_logs`'s signature and the retry range in the
  `QueryExceedsMaxResults` error (upstream's 43a7452b0 narrows it; a separate pick). RPC only.
  *Tests*: `test_range_scan_across_windows` (2,501 blocks, three with logs, every window fetched
  in parallel; a limit of one log ends the scan in the second window), with its retry-range
  assertion on the vendored semantics; TN-0035's permit tests. *Upstream*: n/a (upstream commit).

- **TN-0061** — `--rpc-cache.max-concurrent-db-requests` takes 1 up to a semaphore's capacity.
  *Kind*: fix.
  *Files*: `crates/node/core/src/args/rpc_state_cache.rs` (`RpcStateCacheArgs::max_concurrent_db_requests`).
  *Why*: the flag fed a tokio semaphore unchecked. At 0 every RPC cache miss waited forever on a
  permit that never came, holding its connection; above `usize::MAX >> 3` (`Semaphore::MAX_PERMITS`)
  `Semaphore::new` panicked while the RPC server was built. TN flattens this struct into its own
  `RpcServerArgs`, so the flag reaches TN unchanged; TN's own permit flags
  (`--rpc.max-tracing-requests`, at least 2, and `--rpc.proof-permits`) got the same bounds in
  `tn-reth/src/rpc_server_args.rs` (review R14). RPC configuration only.
  *Tests*: tn-reth's `permit_flags_are_bounded` parses all three flags through TN's CLI.
  *Upstream*: yes.
