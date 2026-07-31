//! RPC server construction and startup for the node's public JSON-RPC endpoint.
//!
//! TN assembles reth's RPC builder stack over the worker's transaction pool and the
//! [`WorkerNetwork`] shim; there is no engine namespace because consensus drives execution
//! directly. Namespace exposure and transport limits come from the CLI allowlist
//! (`crate::cli`) via `node_config().rpc`.
//!
//! Note: [`RethEnv::get_rpc_server`] swallows a failure to merge the TN-specific module at
//! `error!` level — the server still starts and serves the standard namespaces with the TN
//! module missing.

use std::sync::Arc;

use jsonrpsee::Methods;
use reth::rpc::{
    builder::{config::RethRpcServerConfig as _, RpcModuleBuilder, RpcServerHandle},
    eth::EthApi,
};
use reth_provider::providers::BlockchainProvider;
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};

use crate::{
    error::TnRethResult,
    evm::TnEvmConfig,
    traits::{TNExecution, TelcoinNode},
    worker::WorkerNetwork,
    RethEnv, RpcServer, WorkerTxPool,
};

impl RethEnv {
    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        other: impl Into<Methods>,
    ) -> RpcServer {
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
