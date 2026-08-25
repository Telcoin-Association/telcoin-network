//! RPC server construction and startup for the node's public JSON-RPC endpoint.
//!
//! TN assembles reth's RPC builder stack over the worker's transaction pool and the
//! [`WorkerNetwork`] shim; there is no engine namespace because consensus drives execution
//! directly. Namespace exposure, transport limits, and the `eth` API knobs (gas cap, proof
//! window, cache sizes) come from the CLI allowlist (`crate::cli`) via `node_config().rpc`.
//!
//! Note: [`RethEnv::get_rpc_server`] swallows a failure to merge the TN-specific module at
//! `error!` level — the server still starts and serves the standard namespaces with the TN
//! module missing.

use std::sync::Arc;

use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig as _, RpcModuleBuilder, RpcServerHandle},
    eth::{EthApi, EthApiBuilder},
};
use reth_rpc_eth_api::RpcNodeCore;
use reth_rpc_eth_types::EthConfig;

use crate::{
    error::TnRethResult, traits::TNExecution, worker::WorkerNetwork, RethEnv, RpcServer,
    TnEthTransactionPool, WorkerTxPool,
};

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
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        other: impl Into<Methods>,
    ) -> RpcServer {
        let transaction_pool: TnEthTransactionPool = transaction_pool.into();
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
        let mut server = rpc_builder.build(modules_config, eth_api, engine_events);
        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "tn::execution", "Error merging TN rpc module: {e:?}");
        }

        server
    }

    /// Start running the RPC server for this instance.
    pub async fn start_rpc(&self, server: &RpcServer) -> TnRethResult<RpcServerHandle> {
        let server_config = self.node_config().rpc.rpc_server_config();
        Ok(server_config.start(server).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{rpc_server_args::RpcServerArgs as TnRpcServerArgs, RethChainSpec};
    use reth::args::RpcStateCacheArgs;
    use reth_network_api::noop::NoopNetwork;
    use reth_rpc_eth_types::{
        builder::config::PendingBlockKind, EthStateCacheConfig, FeeHistoryCacheConfig,
        ForwardConfig, GasPriceOracleConfig,
    };
    use reth_transaction_pool::noop::NoopTransactionPool;
    use tempfile::TempDir;
    use tn_types::{test_genesis, TaskManager};
    use url::Url;

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

    /// The five TN CLI eth flags survive the TN-args-to-reth-args conversion and land in the
    /// [`EthConfig`] that [`RethEnv::get_rpc_server`] hands to [`apply_eth_config`].
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
    }
}
