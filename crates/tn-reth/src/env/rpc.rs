//! RPC server construction and startup for the node's public JSON-RPC endpoint.
//!
//! TN assembles reth's RPC builder stack over the worker's transaction pool and the
//! [`WorkerNetwork`] shim; there is no engine namespace because consensus drives execution
//! directly. Namespace exposure, transport limits, and the `eth` API knobs (gas cap, proof
//! window, cache sizes) come from the CLI allowlist (`crate::cli`) via `node_config().rpc`.
//!
//! Note: [`RethEnv::get_rpc_server`] swallows a failure to merge the TN-specific module at
//! `error!` level; the server still starts and serves the standard namespaces with the TN
//! module missing. The corrected `eth_feeHistory` install (`crate::rpc_fee_history`) and
//! the `--rpc.txfeecap` guard install (`crate::rpc_fee_cap`) return an error instead of
//! logging. Both installs' coverage is structural: the same module config drives the
//! module build and each handler replacement, so every transport that serves the eth
//! namespace gets the corrected and guarded methods, and a transport without the
//! namespace has no eth methods to correct or guard. The `Result` keeps a future
//! registration failure from passing silently (issues #1231, #1160).
//!
//! Endpoints are derived per worker: [`RethEnv::start_rpc`] shifts the operator's
//! configured http/ws ports into a per-worker band and suffixes the IPC path with the
//! worker id, so every worker in one process binds distinct endpoints. Before this
//! derivation every worker started the one shared `NodeConfig.rpc`: the second worker
//! unlinked worker 0's IPC socket silently (reth removes the file before it binds) and
//! a fixed http/ws port failed the node with `AddrInUse` (issue #1287).

use std::sync::Arc;

use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig as _, RethRpcModule, RpcModuleBuilder, RpcServerHandle},
    eth::{EthApi, EthApiBuilder},
};
use reth_provider::providers::BlockchainProvider;
use reth_rpc_eth_api::RpcNodeCore;
use reth_rpc_eth_types::EthConfig;
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use tn_types::{gas_accumulator::BaseFeeContainer, WorkerId};

use crate::{
    error::{TnRethError, TnRethResult},
    evm::TnEvmConfig,
    rpc_fee_cap::{CappedEthSubmitServer as _, EthSubmitWithCap, TxFeeCapWei},
    rpc_fee_history::{EpochFeeHistoryServer as _, FeeHistoryWithEpochBaseFee},
    traits::{TNExecution, TelcoinNode},
    worker::WorkerNetwork,
    RethEnv, RpcServer, WorkerTxPool,
};

/// Port distance between per-worker RPC endpoint bands.
///
/// reth's `--instance` shifts `http_port` down by `instance - 1` and `ws_port` up by
/// `2 * (instance - 1)`, with `instance` capped at 200 (reth v1.11.3,
/// `NodeConfig::instance`), so instance offsets span at most 199 ports on either side
/// of the configured default. Striding workers by 200 http ports (and 400 ws ports,
/// mirroring reth's doubled ws arithmetic) keeps every `(instance, worker)` pair on a
/// distinct port: an instance offset moves inside a band, a worker offset moves between
/// bands, and the bands never touch.
const WORKER_PORT_STRIDE: u32 = 200;

/// The transport a worker port derivation applies to.
///
/// Picks the shift direction: http bands stride downward from the operator's port and
/// ws bands stride upward at twice the distance, the same directions reth's
/// `--instance` arithmetic uses. Cross-transport distinctness comes from the base
/// order [`worker_rpc_server_args`] enforces (`ws_port >= http_port` when both
/// transports are enabled on fixed ports): every derived http port stays at or below
/// the http base and every derived ws port at or above the ws base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerRpcTransport {
    /// The http listener; bands stride downward.
    Http,
    /// The ws listener; bands stride upward at `2 * WORKER_PORT_STRIDE`.
    Ws,
}

impl WorkerRpcTransport {
    /// The transport name carried into [`TnRethError::WorkerRpcPort`].
    const fn name(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Ws => "ws",
        }
    }
}

/// Shift a configured listener port into `worker_id`'s band.
///
/// A port of zero is the OS-assigned sentinel (`--with-unused-ports` sets it) and every
/// bind on port zero already yields a distinct socket, so zero passes through
/// unchanged. A shifted http port that leaves `1024..=u16::MAX` (the floor is the top
/// of the privileged range, which a non-root process cannot bind) or a shifted ws port
/// that leaves `1..=u16::MAX` is an error: failing startup loudly, with the worker and
/// offset named, beats wrapping into a port another worker or instance owns (issue
/// #1287's silent-collision class) or dying later in the bind with a bare permission
/// error.
fn worker_shifted_port(
    port: u16,
    worker_id: WorkerId,
    transport: WorkerRpcTransport,
) -> TnRethResult<u16> {
    if port == 0 {
        Ok(0)
    } else {
        // Max offset is 65_535 * 400, far under `u32::MAX`, so the multiplication
        // cannot overflow; the checked shift and the floor filter are the only
        // validity checks needed.
        let offset = match transport {
            WorkerRpcTransport::Http => u32::from(worker_id) * WORKER_PORT_STRIDE,
            WorkerRpcTransport::Ws => u32::from(worker_id) * WORKER_PORT_STRIDE * 2,
        };
        let shifted = match transport {
            WorkerRpcTransport::Http => u32::from(port).checked_sub(offset),
            WorkerRpcTransport::Ws => u32::from(port).checked_add(offset),
        };
        // The downward http band must stop above the privileged range; the upward ws
        // band can never go below its base, so its floor is the whole valid range.
        let floor = match transport {
            WorkerRpcTransport::Http => 1024,
            WorkerRpcTransport::Ws => 1,
        };
        shifted
            .filter(|shifted| *shifted >= floor)
            .and_then(|shifted| u16::try_from(shifted).ok())
            .ok_or(TnRethError::WorkerRpcPort {
                worker_id,
                transport: transport.name(),
                base: port,
                offset,
            })
    }
}

/// Derive one worker's RPC server args from the operator's configured args.
///
/// Worker 0 keeps the operator's values unchanged, so single-worker nodes and existing
/// tooling see exactly the configured endpoints. A worker above 0 gets:
///
/// - `http_port` shifted down by `200 * worker_id` when the http server is enabled;
/// - `ws_port` shifted up by `400 * worker_id` when the ws server is enabled;
/// - `ipcpath` suffixed with `-w{worker_id}` when the IPC server is enabled.
///
/// When both http and ws are enabled on fixed (non-zero) ports, the derivation needs
/// `ws_port >= http_port` (reth's own default layout; equality is the shared http+ws
/// server): http bands stride down and ws bands stride up, so inverted bases would
/// let one worker's http band land on another worker's ws band. The first derived
/// worker refuses startup loudly ([`TnRethError::WorkerRpcPortOrder`]) instead of
/// colliding at bind time.
///
/// A disabled transport keeps its configured value: it never binds, so a port that a
/// shift would push out of range must not fail startup. Under `--with-unused-ports`
/// the http/ws ports are zero (OS-assigned, distinct per bind) and the one random
/// IPC path gets the per-worker suffix, so all derived endpoints stay distinct there
/// too. reth's `--instance` offsets are already applied to `base` at config build
/// (`RethConfig::new`), and the worker stride clears the whole instance range
/// ([`WORKER_PORT_STRIDE`]), so combining the two flags cannot collide either.
fn worker_rpc_server_args(
    base: &reth::args::RpcServerArgs,
    worker_id: WorkerId,
) -> TnRethResult<reth::args::RpcServerArgs> {
    let base = base.clone();
    let inverted_fixed_bases = base.http
        && base.ws
        && base.http_port != 0
        && base.ws_port != 0
        && base.ws_port < base.http_port;
    match () {
        _ if worker_id == 0 => Ok(base),
        _ if inverted_fixed_bases => Err(TnRethError::WorkerRpcPortOrder {
            worker_id,
            http: base.http_port,
            ws: base.ws_port,
        }),
        _ => {
            let http_port = if base.http {
                worker_shifted_port(base.http_port, worker_id, WorkerRpcTransport::Http)?
            } else {
                base.http_port
            };
            let ws_port = if base.ws {
                worker_shifted_port(base.ws_port, worker_id, WorkerRpcTransport::Ws)?
            } else {
                base.ws_port
            };
            let ipcpath = if base.ipcdisable {
                base.ipcpath.clone()
            } else {
                format!("{}-w{worker_id}", base.ipcpath)
            };
            Ok(reth::args::RpcServerArgs { http_port, ws_port, ipcpath, ..base })
        }
    }
}

/// Apply the operator's [`EthConfig`] values to the `eth` API builder.
///
/// Mirrors `EthApiCtx::eth_api_builder` (reth v1.11.3, `crates/node/builder/src/rpc.rs`),
/// which TN's launch path does not run. Without this, each `--rpc.*` eth flag parses and
/// then silently keeps the reth default (#1156).
///
/// Three deltas against reth's reference:
/// - cache: TN spawns no shared state cache up front, so the configured sizes go in as
///   `eth_state_cache_config` for `build` to spawn from. Equivalent for TN, which has no other
///   consumer of the cache.
/// - `send_raw_transaction_sync_timeout`: applied here, absent from reth's reference. No CLI flag
///   feeds it yet, so it re-applies the default (forward compatibility).
/// - task spawner: reth injects its own; TN keeps the builder's default executor, the same behavior
///   as before this fix.
fn apply_eth_config<N: RpcNodeCore, Rpc, NextEnv>(
    builder: EthApiBuilder<N, Rpc, NextEnv>,
    config: EthConfig,
) -> EthApiBuilder<N, Rpc, NextEnv> {
    builder
        .gas_cap(config.rpc_gas_cap.into())
        .max_simulate_blocks(config.rpc_max_simulate_blocks)
        .eth_proof_window(config.eth_proof_window)
        .eth_state_cache_config(config.cache)
        .fee_history_cache_config(config.fee_history_cache)
        .proof_permits(config.proof_permits)
        .gas_oracle_config(config.gas_oracle)
        .max_batch_size(config.max_batch_size)
        .max_blocking_io_requests(config.max_blocking_io_requests)
        .pending_block_kind(config.pending_block_kind)
        .raw_tx_forwarder(config.raw_tx_forwarder)
        .send_raw_transaction_sync_timeout(config.send_raw_transaction_sync_timeout)
        .evm_memory_limit(config.rpc_evm_memory_limit)
        .force_blob_sidecar_upcasting(config.force_blob_sidecar_upcasting)
}

impl RethEnv {
    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    ///
    /// `base_fee` is this worker's shared epoch base-fee container: the corrected
    /// `eth_feeHistory` answers its next-block entry from it (`crate::rpc_fee_history`).
    ///
    /// Errors when the corrected fee-history method or the `--rpc.txfeecap` guard
    /// (`crate::rpc_fee_cap`) cannot replace the stock eth handlers.
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        base_fee: BaseFeeContainer,
        other: impl Into<Methods>,
    ) -> eyre::Result<RpcServer> {
        let transaction_pool: EthTransactionPool<
            BlockchainProvider<TelcoinNode>,
            DiskFileBlobStore,
            TnEvmConfig,
        > = transaction_pool.into();
        let tn_execution = Arc::new(TNExecution);
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.inner.blockchain_provider.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network.clone())
            .with_executor(Box::new(self.inner.task_spawner.clone()))
            .with_evm_config(self.inner.evm_config.clone())
            .with_consensus(tn_execution.clone());

        let modules_config = self.node_config().rpc.transport_rpc_module_config();
        let eth_api = apply_eth_config(
            EthApi::builder(
                self.inner.blockchain_provider.clone(),
                transaction_pool,
                network,
                self.inner.evm_config.clone(),
            ),
            self.node_config().rpc.eth_config(),
        )
        .build();

        let engine_events = reth_tokio_util::EventSender::default();
        // Correct `eth_feeHistory`'s next-block base-fee entry: reth predicts it with
        // Ethereum's EIP-1559 schedule, and TN prices the next block from the epoch fee
        // schedule instead (issue #1231).
        let fee_history = FeeHistoryWithEpochBaseFee::new(eth_api.clone(), self.clone(), base_fee);
        // Guard the eth submission methods with the operator's `--rpc.txfeecap` (issue
        // #1160). Reth's pool validator only checks the cap for local-treated
        // transactions, and raw RPC submissions are External, so the guard runs at the
        // RPC boundary and then delegates to these same reth handlers.
        let fee_cap_guard = EthSubmitWithCap::new(
            eth_api.clone(),
            TxFeeCapWei::new(self.node_config().rpc.rpc_tx_fee_cap),
        );
        let mut server = rpc_builder.build(modules_config, eth_api, engine_events);
        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "tn::execution", "Error merging TN rpc module: {e:?}");
        }
        // Replace `eth_feeHistory` on every transport that exposes the eth namespace. A
        // transport without the namespace serves no fee-history method to correct.
        server.add_or_replace_if_module_configured(RethRpcModule::Eth, fee_history.into_rpc())?;
        // Replace `eth_sendRawTransaction` / `eth_sendRawTransactionSync` on every
        // transport that exposes the eth namespace. A transport without the namespace
        // serves no submission methods, so it needs no guard.
        server.add_or_replace_if_module_configured(RethRpcModule::Eth, fee_cap_guard.into_rpc())?;

        Ok(server)
    }

    /// Start running the RPC server for one worker.
    ///
    /// The server config comes from [`worker_rpc_server_args`], not from the shared
    /// `NodeConfig.rpc` directly, so every worker in the process binds distinct
    /// http/ws ports and a distinct IPC path (issue #1287). The endpoints the OS
    /// actually resolved are logged per worker at `info!`.
    pub async fn start_rpc(
        &self,
        server: &RpcServer,
        worker_id: WorkerId,
    ) -> TnRethResult<RpcServerHandle> {
        let server_config =
            worker_rpc_server_args(&self.node_config().rpc, worker_id)?.rpc_server_config();
        let handle = server_config.start(server).await?;
        tracing::info!(
            target: "tn::execution",
            worker_id,
            http = ?handle.http_local_addr(),
            ws = ?handle.ws_local_addr(),
            ipc = ?handle.ipc_endpoint(),
            "worker rpc endpoints resolved"
        );
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        init_reth_defaults, rpc_server_args::RpcServerArgs as TnRpcServerArgs,
        test_utils::TransactionFactory, RethChainSpec,
    };
    use alloy::{primitives::U64, rpc::types::FeeHistory};
    use jsonrpsee::{rpc_params, RpcModule};
    use reth::args::RpcStateCacheArgs;
    use reth_network_api::noop::NoopNetwork;
    use reth_rpc_eth_types::{
        builder::config::PendingBlockKind, EthStateCacheConfig, FeeHistoryCacheConfig,
        ForwardConfig, GasPriceOracleConfig,
    };
    use reth_transaction_pool::noop::NoopTransactionPool;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_types::{test_genesis, Address, Bytes, Encodable2718 as _, TaskManager, B256, U256};
    use url::Url;

    /// Build a temp env with the given `--rpc.txfeecap` value (wei) and return the
    /// built RPC server's methods plus the pool for assertions.
    ///
    /// The returned methods are the production registration: `get_rpc_server` is the
    /// only RPC construction path in the node, so a call through them exercises the
    /// same handler an operator's `eth_sendRawTransaction` request reaches. The
    /// transport selection comes from `rpc_args` (the default serves IPC only).
    fn rpc_methods_with_cap(
        cap_wei: u128,
        rpc_args: reth::args::RpcServerArgs,
        task_manager: &TaskManager,
        tmp_dir: &TempDir,
    ) -> (jsonrpsee::Methods, WorkerTxPool, Arc<RethChainSpec>) {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let rpc_args = reth::args::RpcServerArgs { rpc_tx_fee_cap: cap_wei, ..rpc_args };
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain.clone(),
            tmp_dir.path(),
            task_manager,
            None,
            rpc_args,
        )
        .expect("temp chain env");
        let pool = reth_env.init_txn_pool().expect("txn pool");
        let network = WorkerNetwork::new_for_test(reth_env.chainspec());
        let server = reth_env
            .get_rpc_server(pool.clone(), network, BaseFeeContainer::default(), RpcModule::new(()))
            .expect("rpc server with fee-cap guard");
        (server.methods_by(|name| name.starts_with("eth_send")), pool, chain)
    }

    #[tokio::test]
    async fn test_send_raw_transaction_enforces_fee_cap() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        // Cap 200,000 wei: a 21,000-gas transfer at 7 wei/gas costs at most 147,000
        // wei (under), a 1,000,000-gas budget at 7 wei/gas costs at most 7,000,000
        // wei (over).
        let (methods, pool, chain) = rpc_methods_with_cap(
            200_000,
            reth::args::RpcServerArgs::default(),
            &task_manager,
            &tmp_dir,
        );
        let mut tx_factory = TransactionFactory::new();

        let under_cap = tx_factory.create_eip1559(
            chain.clone(),
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let accepted: B256 = methods
            .call("eth_sendRawTransaction", rpc_params![Bytes::from(under_cap.encoded_2718())])
            .await
            .expect("under-cap transaction is accepted");
        assert_eq!(accepted, *under_cap.hash());
        assert_eq!(pool.pool_size().pending, 1);

        let over_cap = tx_factory.create_eip1559(
            chain,
            Some(1_000_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let over_cap_bytes = Bytes::from(over_cap.encoded_2718());
        let err = methods
            .call::<_, B256>("eth_sendRawTransaction", rpc_params![over_cap_bytes.clone()])
            .await
            .expect_err("over-cap transaction is refused");
        assert!(
            err.to_string()
                .contains("tx fee (7000000 wei) exceeds the configured cap (200000 wei)"),
            "unexpected error: {err}"
        );
        // The refused transaction never reaches the pool.
        assert_eq!(pool.pool_size().pending, 1);
        assert!(pool.get(over_cap.hash()).is_none());

        // The sync variant runs the same guard.
        let err = methods
            .call::<_, serde_json::Value>("eth_sendRawTransactionSync", rpc_params![over_cap_bytes])
            .await
            .expect_err("over-cap transaction is refused on the sync method");
        assert!(err.to_string().contains("exceeds the configured cap"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_send_raw_transaction_zero_cap_disables_check() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let (methods, pool, chain) =
            rpc_methods_with_cap(0, reth::args::RpcServerArgs::default(), &task_manager, &tmp_dir);
        let mut tx_factory = TransactionFactory::new();

        // 7,000,000 wei max fee sails through a disabled cap.
        let tx = tx_factory.create_eip1559(
            chain,
            Some(1_000_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let accepted: B256 = methods
            .call("eth_sendRawTransaction", rpc_params![Bytes::from(tx.encoded_2718())])
            .await
            .expect("zero cap accepts any fee");
        assert_eq!(accepted, *tx.hash());
        assert_eq!(pool.pool_size().pending, 1);
    }

    #[tokio::test]
    async fn test_http_transport_gets_the_fee_cap_guard() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        // `methods_by` unions transports and keeps the first occurrence, so the
        // IPC-only default above could mask an HTTP registration that missed the
        // guard. Serve HTTP alone (IPC disabled) and check its registration
        // directly: production validators serve the forwarder over HTTP.
        let rpc_args =
            reth::args::RpcServerArgs { http: true, ipcdisable: true, ..Default::default() };
        let (methods, pool, chain) =
            rpc_methods_with_cap(200_000, rpc_args, &task_manager, &tmp_dir);
        let mut tx_factory = TransactionFactory::new();

        let over_cap = tx_factory.create_eip1559(
            chain,
            Some(1_000_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let err = methods
            .call::<_, B256>(
                "eth_sendRawTransaction",
                rpc_params![Bytes::from(over_cap.encoded_2718())],
            )
            .await
            .expect_err("over-cap transaction is refused on the HTTP registration");
        assert!(err.to_string().contains("exceeds the configured cap"), "unexpected error: {err}");
        assert_eq!(pool.pool_size().pending, 0);
    }

    /// Every applied [`EthConfig`] value reaches the [`EthApiBuilder`]: build a builder over
    /// a temp env, apply a config whose asserted fields all differ from the defaults, and
    /// read each value back through the builder's getters.
    ///
    /// Regression guard for #1156, where `get_rpc_server` built the `EthApi` with reth's
    /// defaults. `max_blocking_io_requests`, `send_raw_transaction_sync_timeout`,
    /// `evm_memory_limit`, and `force_blob_sidecar_upcasting` have no builder getter, so
    /// only their setter calls in [`apply_eth_config`] cover them.
    #[tokio::test]
    async fn test_apply_eth_config_overrides_every_builder_default() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;

        let config = EthConfig {
            fee_history_cache: FeeHistoryCacheConfig { max_blocks: 5, resolution: 9 },
            raw_tx_forwarder: ForwardConfig {
                tx_forwarder: Some(Url::parse("http://127.0.0.1:8555/")?),
            },
            ..EthConfig::default()
                .rpc_gas_cap(12_345_678)
                .rpc_max_simulate_blocks(42)
                .eth_proof_window(99)
                .proof_permits(7)
                .max_batch_size(13)
                .pending_block_kind(PendingBlockKind::None)
                .state_cache(EthStateCacheConfig {
                    max_blocks: 11,
                    max_receipts: 12,
                    max_headers: 13,
                    max_concurrent_db_requests: 14,
                    max_cached_tx_hashes: 15,
                })
                .gpo_config(GasPriceOracleConfig {
                    blocks: 3,
                    percentile: 77,
                    ..Default::default()
                })
        };

        let builder = apply_eth_config(
            EthApiBuilder::new(
                reth_env.blockchain_provider().clone(),
                NoopTransactionPool::default(),
                NoopNetwork::default(),
                reth_env.evm_config().clone(),
            ),
            config.clone(),
        );

        assert_eq!(builder.get_gas_cap().0, config.rpc_gas_cap);
        assert_eq!(builder.get_max_simulate_blocks(), config.rpc_max_simulate_blocks);
        assert_eq!(builder.get_eth_proof_window(), config.eth_proof_window);
        assert_eq!(*builder.get_eth_state_cache_config(), config.cache);
        assert_eq!(*builder.get_fee_history_cache_config(), config.fee_history_cache);
        assert_eq!(builder.get_proof_permits(), config.proof_permits);
        assert_eq!(*builder.get_gas_oracle_config(), config.gas_oracle);
        assert_eq!(builder.get_max_batch_size(), config.max_batch_size);
        assert_eq!(builder.get_pending_block_kind(), config.pending_block_kind);
        assert_eq!(*builder.get_raw_tx_forwarder(), config.raw_tx_forwarder);

        Ok(())
    }

    /// The stock `eth_syncing` handler answers from the worker shim's sync flag
    /// (issue #1231).
    ///
    /// The call goes through `get_rpc_server`'s production registration, so this exercises
    /// the same handler an operator's `eth_syncing` request reaches. `SyncStatus::None`
    /// serializes as `false`; a syncing node answers a sync-progress object.
    #[tokio::test]
    async fn test_eth_syncing_answers_from_worker_shim_flag() -> eyre::Result<()> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        // `new_for_temp_chain` disables every RPC transport (issue #1165), which would
        // register no eth methods at all; inject reth's default args (IPC module set) so
        // the production registration is what answers.
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain,
            tmp_dir.path(),
            &task_manager,
            None,
            reth::args::RpcServerArgs::default(),
        )?;
        let pool = reth_env.init_txn_pool()?;
        let network = crate::worker::WorkerNetwork::new_for_test(reth_env.chainspec());
        let server = reth_env.get_rpc_server(
            pool,
            network.clone(),
            BaseFeeContainer::default(),
            RpcModule::new(()),
        )?;
        let methods = server.methods_by(|name| name == "eth_syncing");

        let synced: serde_json::Value = methods.call("eth_syncing", rpc_params![]).await?;
        assert_eq!(synced, serde_json::Value::Bool(false));

        network.set_syncing(true);
        let syncing: serde_json::Value = methods.call("eth_syncing", rpc_params![]).await?;
        assert!(syncing.is_object(), "syncing answer is a sync-progress object: {syncing}");
        assert!(syncing.get("currentBlock").is_some(), "sync object names currentBlock");
        assert_eq!(syncing.get("currentBlock"), syncing.get("highestBlock"));

        network.set_syncing(false);
        let caught_up: serde_json::Value = methods.call("eth_syncing", rpc_params![]).await?;
        assert_eq!(caught_up, serde_json::Value::Bool(false));

        Ok(())
    }

    /// The TN CLI eth flags survive the TN-args-to-reth-args conversion and land in the
    /// [`EthConfig`] that [`RethEnv::get_rpc_server`] hands to [`apply_eth_config`].
    ///
    /// The pending-block assert also pins TN's `none` default against reth's `full`
    /// (issue #1231): only the conversion carrying TN's field can produce `None` here.
    #[test]
    fn test_tn_rpc_args_reach_eth_config() {
        let args = TnRpcServerArgs {
            rpc_gas_cap: 100_000_000,
            rpc_max_simulate_blocks: 42,
            rpc_eth_proof_window: 99,
            rpc_proof_permits: 7,
            rpc_state_cache: RpcStateCacheArgs { max_blocks: 11, ..Default::default() },
            ..Default::default()
        };
        let config = reth::args::RpcServerArgs::from(args).eth_config();

        assert_eq!(config.rpc_gas_cap, 100_000_000);
        assert_eq!(config.rpc_max_simulate_blocks, 42);
        assert_eq!(config.eth_proof_window, 99);
        assert_eq!(config.proof_permits, 7);
        assert_eq!(config.cache.max_blocks, 11);
        assert_eq!(config.pending_block_kind, PendingBlockKind::None);
    }

    /// Build a temp env whose worker base-fee container holds `epoch_fee` and return the
    /// production-registered fee-quote intercepts (`eth_feeHistory`, `eth_blobBaseFee`).
    ///
    /// [`RethEnv::get_rpc_server`] is the only RPC construction path in the node, so a
    /// call through the returned methods exercises the registered handler, not a copy.
    fn fee_history_methods(
        epoch_fee: u64,
        task_manager: &TaskManager,
        tmp_dir: &TempDir,
    ) -> Methods {
        // Seed reth's process-global RPC defaults before building the args (#1165): a
        // `Default`-constructed `RpcServerArgs` reads them, and the first builder in the
        // test process locks them.
        init_reth_defaults();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain,
            tmp_dir.path(),
            task_manager,
            None,
            reth::args::RpcServerArgs::default(),
        )
        .expect("temp chain env");
        let pool = reth_env.init_txn_pool().expect("txn pool");
        let network = WorkerNetwork::new_for_test(reth_env.chainspec());
        let server = reth_env
            .get_rpc_server(pool, network, BaseFeeContainer::new(epoch_fee), RpcModule::new(()))
            .expect("rpc server with corrected fee history");
        server.methods_by(|name| name == "eth_feeHistory" || name == "eth_blobBaseFee")
    }

    /// The next-block `baseFeePerGas` entry at the tip is the worker's epoch base fee,
    /// not reth's EIP-1559 prediction, and the returned-block entries stay untouched.
    #[tokio::test]
    async fn test_fee_history_next_block_entry_is_epoch_base_fee() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        // A value no EIP-1559 step from the genesis header can produce.
        let epoch_fee = 2_468_013_579_u64;
        let methods = fee_history_methods(epoch_fee, &task_manager, &tmp_dir);

        let history: FeeHistory = methods
            .call("eth_feeHistory", rpc_params![U64::from(1_u64), "latest"])
            .await
            .expect("fee history");

        // One returned block (genesis) plus the predicted next-block entry.
        assert_eq!(history.gas_used_ratio.len(), 1);
        assert_eq!(history.base_fee_per_gas.len(), 2);
        assert_eq!(history.base_fee_per_gas.last().copied(), Some(u128::from(epoch_fee)));
        let genesis_entry = history.base_fee_per_gas.first().copied().expect("genesis entry");
        assert_ne!(genesis_entry, u128::from(epoch_fee), "only the final entry is corrected");
    }

    /// The `pending` tag takes the same corrected path: reth caps it to `latest`, so the
    /// final entry is the epoch base fee.
    #[tokio::test]
    async fn test_fee_history_pending_tag_gets_the_corrected_entry() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let epoch_fee = 1_357_924_680_u64;
        let methods = fee_history_methods(epoch_fee, &task_manager, &tmp_dir);

        let history: FeeHistory = methods
            .call("eth_feeHistory", rpc_params![U64::from(1_u64), "pending"])
            .await
            .expect("fee history for the pending tag");

        assert_eq!(history.base_fee_per_gas.last().copied(), Some(u128::from(epoch_fee)));
    }

    /// The blob-fee columns are zeroed (#1231 item 3): the stock delegate quotes the
    /// 1-wei EIP-4844 minimum from TN's `excess_blob_gas: 0` headers, and TN's pool
    /// refuses blob transactions at admission (#1159).
    #[tokio::test]
    async fn test_fee_history_blob_columns_are_zero() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let methods = fee_history_methods(1_000, &task_manager, &tmp_dir);

        let history: FeeHistory = methods
            .call("eth_feeHistory", rpc_params![U64::from(1_u64), "latest"])
            .await
            .expect("fee history");

        // Shape parity with the gas columns: one returned block plus the next-block
        // entry. The stock values here are 1 wei, so the zeros are the intercept's.
        assert_eq!(history.base_fee_per_blob_gas, vec![0, 0]);
        assert_eq!(history.blob_gas_used_ratio, vec![0.0]);
    }

    /// `eth_blobBaseFee` answers zero over the production registration (#1231 item 3);
    /// the stock handler would quote 1 wei from the genesis header's zero excess blob
    /// gas.
    #[tokio::test]
    async fn test_blob_base_fee_is_zero() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let methods = fee_history_methods(1_000, &task_manager, &tmp_dir);

        let fee: U256 =
            methods.call("eth_blobBaseFee", rpc_params![]).await.expect("blob base fee");

        assert_eq!(fee, U256::ZERO);
    }

    /// Worker 0 keeps the operator's configured endpoints byte for byte, so
    /// single-worker nodes and existing tooling see no change (#1287).
    #[test]
    fn test_worker_zero_keeps_operator_rpc_endpoints() {
        let base = reth::args::RpcServerArgs { http: true, ws: true, ..Default::default() };
        let derived = worker_rpc_server_args(&base, 0).expect("worker 0 derivation");
        assert_eq!(derived.http_port, base.http_port);
        assert_eq!(derived.ws_port, base.ws_port);
        assert_eq!(derived.ipcpath, base.ipcpath);
    }

    /// Each worker derives distinct http/ws ports and a distinct IPC path from one
    /// operator config; the #1287 regression is every worker binding the same
    /// endpoints.
    #[test]
    fn test_worker_endpoints_are_distinct_per_worker() {
        let base = reth::args::RpcServerArgs { http: true, ws: true, ..Default::default() };
        let worker_one = worker_rpc_server_args(&base, 1).expect("worker 1 derivation");
        let worker_two = worker_rpc_server_args(&base, 2).expect("worker 2 derivation");
        assert_eq!(worker_one.http_port, base.http_port - 200);
        assert_eq!(worker_two.http_port, base.http_port - 400);
        assert_eq!(worker_one.ws_port, base.ws_port + 400);
        assert_eq!(worker_two.ws_port, base.ws_port + 800);
        assert_eq!(worker_one.ipcpath, format!("{}-w1", base.ipcpath));
        assert_eq!(worker_two.ipcpath, format!("{}-w2", base.ipcpath));
    }

    /// A worker band cannot collide with any `--instance` offset. Instance arithmetic
    /// moves http down by at most 199 and ws up by at most 398 (`instance <= 200` in
    /// reth v1.11.3), and the closest approach between the two schemes is worker 1 of
    /// instance 1 against worker 0 of instance 200: the worker stride clears it on
    /// both transports.
    #[test]
    fn test_worker_bands_clear_the_instance_range() {
        // Instance 1 leaves the defaults unchanged, so `base` is instance 1's config.
        let base = reth::args::RpcServerArgs { http: true, ws: true, ..Default::default() };
        let worker_one = worker_rpc_server_args(&base, 1).expect("worker 1 derivation");
        assert!(
            worker_one.http_port < base.http_port - 199,
            "worker 1 http band must sit below every instance's http port"
        );
        assert!(
            worker_one.ws_port > base.ws_port + 398,
            "worker 1 ws band must sit above every instance's ws port"
        );
    }

    /// An enabled transport whose shifted port leaves the valid range fails startup
    /// loudly instead of wrapping into a port another worker or instance owns.
    #[test]
    fn test_out_of_range_worker_port_is_a_loud_error() {
        let low_http =
            reth::args::RpcServerArgs { http: true, http_port: 100, ..Default::default() };
        assert!(matches!(
            worker_rpc_server_args(&low_http, 1),
            Err(TnRethError::WorkerRpcPort {
                worker_id: 1,
                transport: "http",
                base: 100,
                offset: 200
            })
        ));

        let high_ws =
            reth::args::RpcServerArgs { ws: true, ws_port: u16::MAX, ..Default::default() };
        assert!(matches!(
            worker_rpc_server_args(&high_ws, 1),
            Err(TnRethError::WorkerRpcPort {
                worker_id: 1,
                transport: "ws",
                base: u16::MAX,
                offset: 400
            })
        ));
    }

    /// `--with-unused-ports` semantics survive derivation: zero http/ws ports stay
    /// zero (the OS assigns a distinct port per bind) and the flag's one random IPC
    /// path still gets the per-worker suffix (#1287: every worker shared it).
    #[test]
    fn test_zero_ports_stay_os_assigned_and_ipc_still_suffixes() {
        let base = reth::args::RpcServerArgs {
            http: true,
            ws: true,
            http_port: 0,
            ws_port: 0,
            ..Default::default()
        };
        let derived = worker_rpc_server_args(&base, 3).expect("worker 3 derivation");
        assert_eq!(derived.http_port, 0);
        assert_eq!(derived.ws_port, 0);
        assert_eq!(derived.ipcpath, format!("{}-w3", base.ipcpath));
    }

    /// Inverted transport bases (ws below http on fixed ports) would let one worker's
    /// http band land on another worker's ws band, so the first derived worker fails
    /// loudly at derivation time; worker 0 still binds the operator's ports as
    /// configured, keeping single-worker nodes on inverted bases working.
    #[test]
    fn test_inverted_transport_bases_are_a_loud_error() {
        let base = reth::args::RpcServerArgs {
            http: true,
            http_port: 8800,
            ws: true,
            ws_port: 8000,
            ..Default::default()
        };
        let worker_zero = worker_rpc_server_args(&base, 0).expect("worker 0 keeps the config");
        assert_eq!(worker_zero.http_port, 8800);
        assert_eq!(worker_zero.ws_port, 8000);
        assert!(matches!(
            worker_rpc_server_args(&base, 1),
            Err(TnRethError::WorkerRpcPortOrder { worker_id: 1, http: 8800, ws: 8000 })
        ));
    }

    /// Equal http/ws bases are reth's shared http+ws server and stay valid: worker 0
    /// keeps the combined endpoint, and derived workers split into bands that cannot
    /// collide (http descends, ws ascends).
    #[test]
    fn test_equal_transport_bases_stay_valid() {
        let base = reth::args::RpcServerArgs {
            http: true,
            http_port: 9000,
            ws: true,
            ws_port: 9000,
            ..Default::default()
        };
        let worker_one = worker_rpc_server_args(&base, 1).expect("worker 1 derivation");
        assert_eq!(worker_one.http_port, 8800);
        assert_eq!(worker_one.ws_port, 9400);
    }

    /// A derived http port inside the privileged range (below 1024) fails at
    /// derivation time with the worker and offset named, not at bind time with a
    /// bare permission error.
    #[test]
    fn test_http_band_below_the_privileged_floor_is_a_loud_error() {
        let base = reth::args::RpcServerArgs { http: true, http_port: 1100, ..Default::default() };
        assert!(matches!(
            worker_rpc_server_args(&base, 1),
            Err(TnRethError::WorkerRpcPort {
                worker_id: 1,
                transport: "http",
                base: 1100,
                offset: 200
            })
        ));
    }

    /// A disabled transport keeps its configured value even when a shift would leave
    /// the range (it never binds, so it must not fail startup), and a disabled IPC
    /// server keeps its path unsuffixed.
    #[test]
    fn test_disabled_transports_keep_their_configured_values() {
        let base = reth::args::RpcServerArgs {
            http: false,
            http_port: 100,
            ws: false,
            ws_port: u16::MAX,
            ipcdisable: true,
            ..Default::default()
        };
        let derived = worker_rpc_server_args(&base, 1).expect("worker 1 derivation");
        assert_eq!(derived.http_port, 100);
        assert_eq!(derived.ws_port, u16::MAX);
        assert_eq!(derived.ipcpath, base.ipcpath);
    }

    /// Build and start one worker's RPC server over the env's production registration.
    #[cfg(unix)]
    async fn start_worker_rpc(reth_env: &RethEnv, worker_id: WorkerId) -> RpcServerHandle {
        let pool = reth_env.init_txn_pool().expect("txn pool");
        let network = WorkerNetwork::new_for_test(reth_env.chainspec());
        let server = reth_env
            .get_rpc_server(pool, network, BaseFeeContainer::default(), RpcModule::new(()))
            .expect("rpc server");
        reth_env.start_rpc(&server, worker_id).await.expect("rpc server starts")
    }

    /// Two workers on one `RethEnv` bind two live IPC sockets. Before #1287 both
    /// workers started the same `ipcpath`, and reth unlinks the endpoint path before
    /// it binds, so the second start silently stole worker 0's socket file.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_two_workers_bind_distinct_live_ipc_sockets() {
        let tmp_dir = TempDir::new().expect("temp dir");
        let task_manager = TaskManager::default();
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let ipcpath = tmp_dir.path().join("tn-test.ipc").to_string_lossy().into_owned();
        let rpc_args = reth::args::RpcServerArgs { ipcpath: ipcpath.clone(), ..Default::default() };
        let reth_env = RethEnv::new_for_temp_chain_with_rpc_args(
            chain,
            tmp_dir.path(),
            &task_manager,
            None,
            rpc_args,
        )
        .expect("temp chain env");

        let handle_zero = start_worker_rpc(&reth_env, 0).await;
        let handle_one = start_worker_rpc(&reth_env, 1).await;

        let suffixed = format!("{ipcpath}-w1");
        assert_eq!(handle_zero.ipc_endpoint(), Some(ipcpath.clone()));
        assert_eq!(handle_one.ipc_endpoint(), Some(suffixed.clone()));
        // Both socket files exist after the second start: the derived paths differ,
        // so worker 1's bind unlinked nothing of worker 0's.
        assert!(
            std::path::Path::new(&ipcpath).exists(),
            "worker 0 IPC socket file must survive worker 1's start"
        );
        assert!(std::path::Path::new(&suffixed).exists(), "worker 1 IPC socket file must exist");
    }
}
