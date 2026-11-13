# How TN uses reth (snapshot 2026-10-31, reth v1.11.4)

Research behind ROADMAP.md. Regenerate it before relying on it after large changes: the
`file:line` references drift. Paths are relative to the TN repository root; a bare path such as
`env/mod.rs:188` is under `crates/tn-reth/src/`.

Taken on the branch that carried the tndb-b storage work. tndb-b is tn-lmdb since 2026-11-04
(`tn-lmdb-reth`, `TnLmdbEnv`; the rows below use those names) and TN's default reth backend since
2026-11-09 (DECISIONS.md D22). Nine manifests name `reth-metrics` (batch-builder, the consensus
executor, primary and worker, engine, network-libp2p, node, state-sync, tn-reth), all for its
derive and `metrics` re-export; the other counts below are the snapshot's.

Vendored: 57 reth crates plus the dev-only `reth-testing-utils` (`vendor/reth/tn/crates.txt`),
down from 101 (TN-0005, TN-0009, TN-0011, TN-0012, and TN-0028 to TN-0032 in the second pass).
RocksDB is gone (TN-0014). `cargo tree`
still prints v1.11.3: upstream did not bump the workspace version for v1.11.4 either.

`crates/tn-reth` is the boundary. Outside it, TN touches reth in only a few places:

- `tn-types` (`crates/types/src/`): primitives (`lib.rs:88-101`), `ChainSpec` (`genesis.rs:13`),
  and reth's `TaskSpawner` trait, implemented for TN's spawner (`task_manager.rs:626`).
- `tn-config`: `ChainSpec` (`crates/config/src/node.rs:4`).
- `tn-exex`: two items, `Chain` and `CanonStateNotification`.
- `tn-lmdb-reth`: the reth DB API (`reth-db-api`, `reth-storage-errors`), which it implements
  over tn-lmdb, the default execution database.
- `tn-storage`: `reth-libmdbx`, behind its default `reth-libmdbx` feature, for TN's old MDBX
  consensus-DB backend (migration source, tests, benches).
- Eight crates use `reth-metrics` only for its derive macro and its `metrics` re-export.
- Nothing under `bin/` depends on reth. `telcoin-network-cli` reaches reth only through tn-reth
  re-exports (`LogArgs`, `ColorMode`, `Layers`, `parse_socket_address`, `iter_static_files`, ...).

## Structural layers

TN does **not** use reth's `NodeBuilder`, `FullNodeComponents`, `launch_*`, the Engine API,
beacon consensus, reth ExEx, payload-builder services, devp2p or the Stages pipeline. Their crates
are gone; only type crates remain (reth-consensus, reth-engine-primitives, reth-stages-types,
reth-payload-*). TN assembles the components itself:

| Piece | Where | Notes |
|---|---|---|
| `NodeConfig` | wrapped by `RethConfig` (`cli.rs:63`), filled in `RethConfig::new` (`cli.rs:277-439`) | Every arg group is pinned or disabled; the network group no longer exists (TN-0008). Only the RPC, txpool and DB args come from the CLI. RPC uses TN's subset `RpcServerArgs`, converted to reth's at `rpc_server_args.rs:277`. `init_reth_defaults` (`cli.rs:169`) seeds reth's process-wide pool and IPC defaults. |
| `NodeTypes` | `TelcoinNode`, `traits.rs:36-47` | `TNPrimitives` (`= EthPrimitives`, `types.rs:35`), `ChainSpec`, `EthStorage`, `EthEngineTypes`, `DB = RethDb`. `RethDb` wraps tn-lmdb's `TnLmdbEnv`, or MDBX's `DatabaseEnv` under the opt-in `mdbx` feature (`env/database.rs:31-38`). `TNExecution` (`traits.rs:77-149`) is a fail-loud consensus stub that satisfies the RPC builder's bounds (`env/rpc.rs:280`). |
| Provider stack | `env/mod.rs:333-390`, `BlockchainProvider::new` at `env/mod.rs:188` | DB opened by `open_rw` (`env/database.rs:138`, tn-lmdb, which first migrates an MDBX database; the `mdbx` build's at `:120`, reth's `init_db`). `ProviderFactory::new` over the DB, the chain, `StaticFileProvider::read_write`, the `RocksDBProvider` stub and a `Runtime`, then `check_consistency()`, then `init_genesis_with_settings` in the layout `env/layout.rs` settles. The stub is zero-size since TN-0014; TN also sets `rocksdb_path: None` in `dirs.rs` and `env/genesis.rs` (ROADMAP, RocksDB step B). |
| EVM | `evm/config.rs:109`, `evm/{factory,block,mod,handler}.rs` | TN-owned `ConfigureEvm`, `EvmFactory` (`factory.rs:44`), `BlockExecutorFactory` (`factory.rs:141`) and `BlockExecutor` (`block.rs:1842`; system calls, epoch close, EIP-2935/4788; `block.rs` is about 2.2k lines before its tests), plus a `BlockAssembler` (`block.rs:2067`), the `Evm` (`mod.rs:179`) and a revm `Handler` (`handler.rs:63`). |
| Execution | `env/execution.rs:134-434` | `State::builder` over `StateProviderDatabase`, then `builder_for_next_block`, then `finish` against TN's `OverlayRootStateProvider` (`env/output_overlay.rs:149`, built at `execution.rs:242`), giving an `ExecutedBlock`. Saved with `save_blocks(SaveBlocksMode::Full)` and `commit` (`:342`, `:355`); the new tip goes out through `NewCanonicalChain::Commit` and `notify_canon_state` (`:385`, `:413`). `finish_executing_output` (`:427`) runs both halves. |
| Txpool | `txn_pool.rs:318-350` | `EthTransactionPool` over a `DiskFileBlobStore`, from `eth_builder().no_eip4844().no_eip7702()` and `Pool::eth_pool`. TN runs its own maintenance loop (`maintain_pool`, `txn_pool.rs:476`), not reth's. |
| RPC | `env/rpc.rs:259-400` | Built with `RpcModuleBuilder` and `EthApi::builder`, then `create_transport_rpc_modules` (`:359`). The `tn` module is merged with `merge_if_module_configured`; seven TN overrides are swapped in with `add_or_replace_if_module_configured` (`:366-382`); started by `RpcServerConfig::start` (`:400`). |
| Pruning | not used | Archive mode is enforced (`ensure_archive_mode`, `cli.rs:488`, called at `env/mod.rs:183`). Prune and stage checkpoints are written only by snapshot restore (`snapshot.rs:634-742`). |

## Per crate (direct use)

41 of the 64 vendored packages appear in a TN manifest. A row describes tn-reth's use unless it
names another crate.

| reth crate | Use | Weight |
|---|---|---|
| reth-revm | Mostly revm re-exports (`State` included). reth-owned items: `StateProviderDatabase`, `CachedReads`. `env/*`, `evm/*`, `snapshot.rs`. | heavy, ~90% plain revm |
| reth-evm | reth-owned: `ConfigureEvm`, `EvmEnvFor`, `ExecutionCtxFor`, `BlockBuilder(Outcome)`, `BlockAssembler(Input)`, `Executor`. The rest is alloy-evm re-exports (`BlockExecutor(Factory)`, `EvmFactory`, `PrecompilesMap`, ...). `evm/*`, `env/*`. | heavy |
| reth-evm-ethereum | `RethReceiptBuilder`, `revm_spec`, `revm_spec_by_timestamp_and_block_number` (`evm/config.rs`) | 3 items |
| reth-provider | `ProviderFactory`, `BlockchainProvider`, `StaticFileProvider`, the `RocksDBProvider` stub, about 36 reader/writer traits, `SaveBlocksMode`, `CanonStateNotification(Stream)`, `ChangedAccount`, `Chain`, `ExecutionOutcome`, `EthStorage`. Also tn-exex (`CanonStateNotification`). | heavy |
| reth-db / reth-db-api | `init_db`, `DatabaseEnv`, `mdbx::{DatabaseArguments, open_db_read_only, SyncMode}`, `tables::*`, the cursor/tx traits, `version`, `ClientVersion`, `DatabaseMetrics`, `iter_static_files` (`env/database.rs` and its MDBX-to-tn-lmdb `migrate.rs`, `snapshot.rs`, `env/output_overlay/*`, `metrics.rs`). tn-reth names reth-db only; tn-lmdb-reth implements reth-db-api (`Database`, `DbTx(Mut)`, the cursor and dup-cursor traits, `TableImporter`). | heavy |
| reth-trie / reth-trie-db | `StateRoot`, `HashedPostState(Sorted)`, `HashedStorage`, `TrieUpdates(Sorted)`, `TrieInput(Sorted)`, the hashed/trie cursor traits and in-memory cursor factories, `MultiProof`; `DatabaseStateRoot`, `Database{Hashed,Trie}CursorFactory`. TN's output overlay (`env/output_overlay.rs` and `output_overlay/*`) layers its cursors on them; `snapshot.rs` recomputes roots. | moderate-heavy |
| reth-chain-state | `ExecutedBlock`, `NewCanonicalChain`, `CanonicalInMemoryState`, `ComputedTrieData` | load-bearing |
| reth-transaction-pool | `Pool`, `EthTransactionPool`, `TransactionValidationTaskExecutor`, `DiskFileBlobStore`, `CanonicalStateUpdate`, `BestTransactions`, `EthPooledTransaction`, `ValidPoolTransaction`, identifiers, error types (`txn_pool.rs`, `metrics.rs`, `types.rs`) | heavy |
| reth-rpc / -rpc-builder / -rpc-eth-api / -rpc-eth-types / -rpc-api | `EthApi(Builder)`; `RpcModuleBuilder`, `TransportRpcModules`, `RethRpcModule`, `RpcModuleSelection`, `RpcServerHandle`, `RethRpcServerConfig`; the eth helper traits (`Call`, `EthCall`, `EstimateCall`, `EthFees`, `EthTransactions`, `EthApiSpec`, `Load*`, `Trace`, `SpawnBlocking`, `BuildPendingEnv`), `RpcNodeCore`, `EthCallBundleApiServer`; `EthConfig`, `EthStateCache`, `EthApiError`, `recover_raw_transaction`; `DebugApiServer`. `env/rpc.rs`, `rpc_*.rs`, `rpc_server_args.rs`. | heavy |
| reth-node-core | `NodeConfig` and its arg structs, `RpcServerArgs`, `DefaultRpcServerArgs`, `DefaultTxPoolValues`, `RethTransactionPoolConfig`, `dirs` (`ChainPath`, `MaybePlatformPath`, ...), `LogArgs`, `ColorMode`. Carries TN's otlp, keccak and `min-debug-logs` features (jemalloc dropped, DECISIONS.md D19). Its version info comes from the upstream pin (TN-0020). | moderate-heavy |
| reth-node-types / reth-payload-primitives | `NodeTypes(WithDB)`; `PayloadTypes`, `BuiltPayload`, `NewPayloadError` (`traits.rs:25-26`) | light |
| reth-ethereum-engine-primitives | `EthEngineTypes` (`TelcoinNode::Payload`). `EthBuiltPayload`, `EthPayloadAttributes` and `EthPayloadBuilderAttributes` only in the test fixture `DefaultEthPayloadTypes` (`traits.rs:157`); the `BlobSidecars` re-export is dead. Only tn-reth reaches this crate (`cargo tree -i`). | light |
| reth-consensus / reth-engine-primitives | Traits implemented as stubs on `TNExecution`; the engine `DEFAULT_*` constants pinned in `cli.rs:40-43` | light |
| reth-chainspec | `ChainSpec` (from alloy `Genesis`), `EthChainSpec`, `EthereumHardforks`, `BaseFeeParams`, `Chain`. Used in tn-reth, tn-types and tn-config. | moderate |
| reth-primitives-traits / reth-ethereum-primitives | Core TN types, re-exported by `crates/types/src/lib.rs:88-94`: `SealedHeader`, `Recovered(Block)`, `TransactionSigned`, `Receipt`, `Account`, `EthPrimitives`, ... (about 225 lines name `SealedHeader`). tn-types' `SealedBlock` alias wraps reth-primitives-traits'. | heavy across TN |
| reth-errors | `ProviderError`, `RethError`, `BlockExecutionError`, `BlockValidationError` | moderate |
| reth-stages-types / reth-prune-types | Checkpoint types written by snapshot restore | light |
| reth-network-api / -network-peers / reth-eth-wire-types | **Type-only.** `WorkerNetwork` stub impls of `NetworkInfo` and `PeersInfo` (`worker.rs:269-332`) for the `EthApi`, `web3_` and `net_` bounds (the only two network traits left, TN-0031); `Enr`, `NodeRecord`, `PeerId`; `BlockHashNumber` (`env/epoch.rs:80`); `NoopNetwork` in tests | light |
| reth-tasks, -db-common, -cli-util, -tokio-util, -tracing | `TaskSpawner` (implemented in tn-types), `Runtime`; `init_genesis_with_settings`; 3 parsers; `EventSender`; `Layers` | light |
| reth-trie-sparse | No items: a dependency only to set its `std` and `metrics` features (`lib.rs:77`) | features only |
| reth-metrics | `Metrics` derive in 9 TN crates: tn-reth, tn-node, tn-engine, tn-network-libp2p, tn-batch-builder, state-sync, tn-executor, tn-worker, tn-primary | light |
| reth-execution-types | `Chain` re-export in tn-exex (`crates/exex/src/notification.rs:36`) | 1 item |
| reth-storage-errors | tn-lmdb-reth's error mapping: `DatabaseErrorInfo`, `DatabaseWriteError`, `DatabaseWriteOperation` | light |
| reth-libmdbx | tn-storage's MDBX consensus-DB backend (`crates/storage/src/mdbx/`, migration source and tests); tn-lmdb-reth differential tests (dev-dependency) | moderate |
| reth-trie-common | Only tn-lmdb-reth's dup-sort tests (dev-dependency). tn-reth gets its types through reth-trie. | test only |

## Removed since the 2026-10-30 snapshot

| Was | Now |
|---|---|
| reth-node-builder (re-exports only) | `NodeConfig` and `RethTransactionPoolConfig` from reth-node-core; `NodeTypes(WithDB)` from reth-node-types; `BuiltPayload`, `PayloadTypes`, `NewPayloadError` from reth-payload-primitives; the `DEFAULT_*` constants from reth-engine-primitives |
| reth-node-ethereum | `EthEngineTypes` and `EthPayloadAttributes` from reth-ethereum-engine-primitives (`traits.rs:22-24`); its revm features are set by tn-reth (last section) |
| reth-primitives | TN-owned `Block`, `BlockBody` and `SealedBlock` aliases (`crates/types/src/lib.rs:97-101`); everything else from reth-primitives-traits and reth-ethereum-primitives |
| reth-network, -discv4, -eth-wire | The stub ENR key is `enr::secp256k1::SecretKey` (`worker.rs:16`, `:334-336`); `DisconnectReason` and `BlockHashNumber` come from reth-eth-wire-types; the discovery port is a local `DEVP2P_DEFAULT_PORT` (`worker.rs:273`). `NetworkArgs` and `NatResolver` went with `NodeConfig::network` (TN-0008). |

Declared but unused: root `reth-config`. Dead re-exports in `crates/tn-reth/src/lib.rs`:
`parse_duration_from_secs`, `BlobSidecars`, `FileWorkerGuard`, `TransactionPoolT`,
`DEFAULT_PERSISTENCE_THRESHOLD` (`cli.rs` imports reth-engine-primitives' copy) and `sign_message`
(`test_utils.rs` imports it directly). `traits::DefaultEthPayloadTypes` is a test fixture now.

## Transitive-only crates

None of these 23 is named in a TN manifest (except `reth-config`, root only). Each arrives
through the vendored crates listed (`cargo tree --offline --locked -e normal,build -i <pkg>`):

| Crate | Pulled in by |
|---|---|
| reth-config | reth-node-core (`PruneConfig`, `StaticFilesConfig`) |
| reth-ethereum-forks | reth-chainspec, reth-evm-ethereum, reth-node-core |
| reth-execution-errors | reth-errors, reth-evm, reth-trie, reth-trie-db, reth-trie-sparse, reth-db-common |
| reth-storage-api, reth-db-models | storage-api: reth-provider, reth-chain-state, reth-evm, reth-revm, the RPC crates, reth-transaction-pool, reth-trie-db, reth-node-core. db-models: reth-db-api, reth-storage-api |
| reth-codecs, -codecs-derive, -zstd-compressors | codecs: reth-primitives-traits, reth-ethereum-primitives, reth-db-api, reth-trie-common, reth-provider, the stage/prune types. derive: reth-codecs, reth-eth-wire-types. zstd: reth-codecs, reth-ethereum-primitives |
| reth-static-file-types, reth-nippy-jar | static-file-types: reth-db, reth-provider, reth-storage-errors, reth-db-common, reth-config. nippy-jar: reth-db, reth-provider |
| reth-fs-util | reth-db, reth-provider, reth-transaction-pool, reth-cli-util, reth-nippy-jar |
| reth-mdbx-sys | reth-libmdbx |
| reth-ipc, reth-rpc-layer | reth-rpc-builder |
| reth-rpc-convert, reth-rpc-server-types | reth-rpc, reth-rpc-eth-api, reth-rpc-eth-types, reth-node-core (server-types also reth-rpc-builder) |
| reth-testing-utils | dev-dependency of vendored crates' tests only; not in the normal/build graph |

`reth-node-core` (no devp2p stack since TN-0008) is reached from tn-reth and from
`reth-rpc-builder`, for `RpcServerArgs` (`vendor/reth/crates/rpc/rpc-builder/src/config.rs:2`).

## Features the removed crates used to set

`crates/tn-reth/Cargo.toml` now sets what reth-node-ethereum, reth-engine-tree and the `reth`
umbrella used to (TN-0005). Consensus-relevant: revm's `secp256k1`, `blst`, `c-kzg` and
`asm-keccak` (the ecrecover, BLS12-381, KZG and keccak backends; line 55, with `memory_limit`).
Also trie-sparse `std`/`metrics` (`:56`), chain-state `rayon` (`:44`), reth-revm
`optional-balance-check` (`:49`), and the node-core features (`:38-42`) that forward `asm-keccak`
and `keccak-cache-global` to alloy-primitives. The root `Cargo.toml` enables alloy's `rand`, which
reth-network-types used to set (TN-0031): it gives alloy's maps a randomly seeded hasher. Before
removing a crate or an edge, run `python3 -I vendor/reth/tn/graph.py snapshot|diff` (ROADMAP
ground rules).
