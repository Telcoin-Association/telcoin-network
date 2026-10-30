# How TN uses reth (snapshot 2026-10-30, reth v1.11.3)

Research behind ROADMAP.md. Regenerate it before relying on it after large changes: the
`file:line` references drift. Paths are relative to the TN repository root.

`crates/tn-reth` is the boundary. Outside it, TN touches reth in only a few places:

- `tn-types` and `tn-config`: primitives, chainspec, tasks.
- `tn-exex`: two items.
- `tndb-b-reth`: the reth DB API, which it implements.
- `tn-storage`: `reth-libmdbx`, for TN's own consensus DB backend.
- Eight crates use `reth-metrics` only for its derive macro.

## Structural layers

TN does **not** use reth's `NodeBuilder`, `FullNodeComponents`, `launch_*`, the Engine API,
beacon consensus, reth ExEx, payload-builder services or the Stages pipeline. It assembles the
components itself:

| Piece | Where | Notes |
|---|---|---|
| `NodeConfig` | `tn-reth/src/cli.rs:71`, filled at `cli.rs:287-540` | Every arg group is pinned or disabled. Only the RPC, txpool and DB args come from the CLI. |
| `NodeTypes` | `TelcoinNode`, `traits.rs:36` | `EthPrimitives`, `ChainSpec`, `EthStorage`, `EthEngineTypes`, `DB = RethDb`. `TNExecution` (`traits.rs:75-147`) is a fail-loud consensus stub that satisfies the RPC builder's bounds. |
| Provider stack | `env/mod.rs:333-390`, `BlockchainProvider::new` at `env/mod.rs:188` | `ProviderFactory::new(db, chain, StaticFileProvider::read_write, RocksDBProvider` stub, `Runtime)`, then `check_consistency()`, then `init_genesis`. |
| EVM | `evm/config.rs:109`, `factory.rs`, `block.rs`, `handler.rs` | TN-owned `ConfigureEvm`, `EvmFactory`, `BlockExecutorFactory` and `BlockExecutor` (about 3k lines of TN logic: system calls, epoch close, EIP-2935/4788), plus a `BlockAssembler`, the `Evm` and a revm `Handler`. |
| Execution | `env/execution.rs:134-413` | `State::builder` over `StateProviderDatabase`, then `builder_for_next_block`, then `finish`, giving an `ExecutedBlock`. Saved with `save_blocks(SaveBlocksMode::Full)` and `commit`; the new tip goes out through `NewCanonicalChain::Commit` and `notify_canon_state`. |
| Txpool | `txn_pool.rs:319-350` | `EthTransactionPool` from `eth_builder().no_eip4844().no_eip7702()` and `Pool::eth_pool`. TN runs its own maintenance loop (`maintain_pool`, about line 803), not reth's. |
| RPC | `env/rpc.rs:259-400` | Built with `RpcModuleBuilder` and `EthApi::builder`, then `create_transport_rpc_modules`. Seven TN overrides are swapped in with `add_or_replace_if_module_configured`; started by `RpcServerConfig::start`. |
| Pruning | not used | Archive mode is enforced (`cli.rs:550`). Prune and stage checkpoints are written only by snapshot restore (`snapshot.rs:634-742`). |

## Per crate (direct use)

| reth crate | Use | Weight |
|---|---|---|
| reth-revm | Mostly revm re-exports. reth-owned items: `State`, `StateProviderDatabase`, `CachedReads`. | heavy, ~90% plain revm |
| reth-evm | reth-owned: `ConfigureEvm`, `EvmEnvFor`, `ExecutionCtxFor`, `BlockBuilder(Outcome)`, `BlockAssembler(Input)`, `Executor`. The rest is alloy-evm re-exports. | heavy |
| reth-evm-ethereum | `RethReceiptBuilder`, `revm_spec*` | 3 items |
| reth-provider | `ProviderFactory`, `BlockchainProvider`, `StaticFileProvider`, `RocksDBProvider` stub, about 40 reader/writer traits, `CanonStateNotification(Stream)`, `ChangedAccount`, `Chain`, `ExecutionOutcome` | heavy |
| reth-db / reth-db-api | `init_db`, `create_db`, `DatabaseEnv`, `tables::*`, the cursor/tx traits, `version`, `DatabaseMetrics`, `iter_static_files`. tndb-b-reth implements reth-db-api. | heavy |
| reth-trie / -trie-db / -trie-common | `StateRoot`, `HashedPostState(Sorted)`, `TrieUpdates(Sorted)`, `TrieInput`, cursor traits, `MultiProof`. TN's output overlay (`env/output_overlay/*`) builds on the trie cursors. | moderate-heavy |
| reth-chain-state | `ExecutedBlock`, `NewCanonicalChain`, `CanonicalInMemoryState`, `ComputedTrieData` | load-bearing |
| reth-transaction-pool | `Pool`, `EthTransactionPool`, validator builder, `DiskFileBlobStore`, `CanonicalStateUpdate`, `BestTransactions`, error types | heavy |
| reth-rpc / -rpc-builder / -rpc-eth-api / -rpc-eth-types / -rpc-api | `EthApi(Builder)`, `RpcModuleBuilder`, `TransportRpcModules`, `RpcServerHandle`, module selection, `EthStateCache`, the eth helper traits (`Call`, `EthCall`, `EstimateCall`, `EthFees`, `EthTransactions`, `Load*`, `Trace`, `SpawnBlocking`), `DebugApiServer` | heavy |
| reth-node-core | `NodeConfig` and its arg structs, `dirs`, `LogArgs`, `ColorMode` | moderate-heavy |
| reth-node-builder | **re-exports only**: `NodeConfig`, `RethTransactionPoolConfig`, `NodeTypes(WithDB)`, `BuiltPayload`, `PayloadTypes`, `NewPayloadError`, `DEFAULT_*` constants | light, huge transitive cost |
| reth-node-ethereum | `EthEngineTypes`, `EthPayloadAttributes` | 2 items, large transitive cost |
| reth-ethereum-engine-primitives | `EthBuiltPayload` and `EthPayloadBuilderAttributes`, used only in the dead `DefaultEthPayloadTypes` (`traits.rs:155`); the `BlobSidecars` re-export, also dead | light |
| reth-consensus / reth-engine-primitives | Traits implemented as stubs on `TNExecution` | light |
| reth-chainspec | `ChainSpec` (from alloy `Genesis`), `EthChainSpec`, `EthereumHardforks`, `BaseFeeParams`. Used in tn-reth, tn-types and tn-config. | moderate |
| reth-primitives / -primitives-traits / -ethereum-primitives | Core TN types, re-exported by `tn-types/src/lib.rs:88-91`: `Sealed*`, `Recovered*`, `TransactionSigned`, `Receipt`, `Account`, ... (`SealedHeader` has about 150 uses) | heavy across TN |
| reth-errors | `ProviderError`, `RethError`, block execution and validation errors | moderate |
| reth-stages-types / reth-prune-types | Checkpoint types written by snapshot restore | light |
| reth-network / -network-api / -network-peers / -eth-wire / -discv4 | **Type-only.** A dummy secret key; `NetworkArgs` enums; `WorkerNetwork` stub impls of `NetworkInfo`, `PeersInfo` and `Peers` (`worker.rs:272-390`) for the `EthApi` and `net_` bounds; `DisconnectReason`; `BlockHashNumber`; `NatResolver::None`; `DEFAULT_DISCOVERY_PORT` | light |
| reth-tasks, -db-common, -cli-util, -tokio-util, -tracing | `TaskSpawner`, `Runtime`; `init_genesis`; 3 parsers; `EventSender`; `Layers` | light |
| reth-metrics | `Metrics` derive in 9 TN crates | light |
| reth-execution-types | `Chain` re-export in tn-exex | 1 item |
| reth-libmdbx | tn-storage's MDBX consensus-DB backend (migration source and tests) | moderate |

Declared but unused: root `reth-config`. Dead re-exports in `tn-reth/src/lib.rs`:
`parse_duration_from_secs`, `BlobSidecars`, `FileWorkerGuard`, `TransactionPoolT`,
`DEFAULT_PERSISTENCE_THRESHOLD`. Dead type: `traits::DefaultEthPayloadTypes`.

## Transitive-only crates

None of these are used by TN directly: discv5, dns-discovery, downloaders, ecies, stages, era*,
engine-tree, engine-service, engine-local, consensus-debug-client, node-ethstats,
payload-builder, basic- and ethereum-payload-builder, exex, invalid-block-hooks, node-events,
node-metrics, engine-util. They arrive through `reth-node-builder`, `reth-node-ethereum` and
`reth-node-core`. `reth-rpc-builder` depends on `reth-node-core` for `RpcServerArgs`, which keeps
node-core and its network/discovery dependencies in the graph.

## Caveat for removing reth-node-ethereum

`vendor/reth/crates/ethereum/node/Cargo.toml` enables revm features `secp256k1`, `blst`,
`c-kzg` and `memory_limit`, plus `asm-keccak` and `keccak-cache-global` through TN's feature
list. These select the precompile backends, so they are consensus-critical. Before the crate
goes, enable them on revm directly, then confirm that the feature-resolved graph of every
third-party crate is unchanged (`cargo metadata` before and after).
