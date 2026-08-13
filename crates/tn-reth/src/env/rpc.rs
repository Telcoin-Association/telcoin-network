//! RPC server construction and startup for the node's public JSON-RPC endpoint.
//!
//! TN assembles reth's RPC builder stack over the worker's transaction pool and the
//! [`WorkerNetwork`] shim; there is no engine namespace because consensus drives execution
//! directly. Namespace exposure and transport limits come from the CLI allowlist
//! (`crate::cli`) via `node_config().rpc`.
//!
//! Note: [`RethEnv::get_rpc_server`] swallows a failure to merge the TN-specific module at
//! `error!` level; the server still starts and serves the standard namespaces with the TN
//! module missing. The `--rpc.txfeecap` guard install (`crate::rpc_fee_cap`) returns an
//! error instead of logging. The guard's coverage itself is structural: the same module
//! config drives both the module build and the guard replacement, so every transport that
//! serves the eth namespace gets the guarded methods, and a transport without the
//! namespace has no submission methods to guard. The `Result` keeps a future registration
//! failure from passing silently (issue #1160).

use std::sync::Arc;

use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig as _, RethRpcModule, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_provider::providers::BlockchainProvider;
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};

use crate::{
    error::TnRethResult,
    evm::TnEvmConfig,
    rpc_fee_cap::{CappedEthSubmitServer as _, EthSubmitWithCap, TxFeeCapWei},
    traits::{TNExecution, TelcoinNode},
    worker::WorkerNetwork,
    RethEnv, RpcServer, WorkerTxPool,
};

impl RethEnv {
    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    ///
    /// Errors when the `--rpc.txfeecap` guard cannot replace the eth submission
    /// methods (`crate::rpc_fee_cap`).
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
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
        let eth_api = EthApi::builder(
            self.inner.blockchain_provider.clone(),
            transaction_pool,
            network,
            self.inner.evm_config.clone(),
        )
        .build();

        let engine_events = reth_tokio_util::EventSender::default();
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
        // Replace `eth_sendRawTransaction` / `eth_sendRawTransactionSync` on every
        // transport that exposes the eth namespace. A transport without the namespace
        // serves no submission methods, so it needs no guard.
        server.add_or_replace_if_module_configured(RethRpcModule::Eth, fee_cap_guard.into_rpc())?;

        Ok(server)
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
    use crate::{test_utils::TransactionFactory, RethChainSpec};
    use jsonrpsee::{rpc_params, RpcModule};
    use std::sync::Arc;
    use tempfile::TempDir;
    use tn_types::{test_genesis, Address, Bytes, Encodable2718 as _, TaskManager, B256, U256};

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
            .get_rpc_server(pool.clone(), network, RpcModule::new(()))
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
}
