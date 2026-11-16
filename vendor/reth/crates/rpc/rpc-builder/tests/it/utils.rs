use reth_consensus::noop::NoopConsensus;
use reth_ethereum_primitives::EthPrimitives;
use reth_tokio_util::EventSender;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use reth_evm_ethereum::EthEvmConfig;
use reth_network_api::noop::NoopNetwork;
use reth_provider::test_utils::NoopProvider;
use reth_rpc_builder::{
    RpcModuleBuilder, RpcServerConfig, RpcServerHandle, TransportRpcModuleConfig,
};
use reth_rpc_server_types::RpcModuleSelection;
use reth_tasks::TokioTaskExecutor;
use reth_transaction_pool::test_utils::{TestPool, TestPoolBuilder};

// TN(TN-0004): the auth-server helper `launch_auth` is gone with the engine-API tests (it
// needed reth-node-ethereum's `EthereumEngineValidator`).

/// Localhost with port 0 so a free port is used.
pub const fn test_address() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
}

/// Launches a new server with http only with the given modules
pub async fn launch_http(modules: impl Into<RpcModuleSelection>) -> RpcServerHandle {
    let builder = test_rpc_builder();
    let eth_api = builder.bootstrap_eth_api();
    let server =
        builder.build(TransportRpcModuleConfig::set_http(modules), eth_api, EventSender::new(1));
    RpcServerConfig::http(Default::default())
        .with_http_address(test_address())
        .start(&server)
        .await
        .unwrap()
}

/// Launches a new server with ws only with the given modules
pub async fn launch_ws(modules: impl Into<RpcModuleSelection>) -> RpcServerHandle {
    let builder = test_rpc_builder();
    let eth_api = builder.bootstrap_eth_api();
    let server =
        builder.build(TransportRpcModuleConfig::set_ws(modules), eth_api, EventSender::new(1));
    RpcServerConfig::ws(Default::default())
        .with_ws_address(test_address())
        .start(&server)
        .await
        .unwrap()
}

/// Launches a new server with http and ws and with the given modules
pub async fn launch_http_ws(modules: impl Into<RpcModuleSelection>) -> RpcServerHandle {
    let builder = test_rpc_builder();
    let eth_api = builder.bootstrap_eth_api();
    let modules = modules.into();
    let server = builder.build(
        TransportRpcModuleConfig::set_ws(modules.clone()).with_http(modules),
        eth_api,
        EventSender::new(1),
    );
    RpcServerConfig::ws(Default::default())
        .with_ws_address(test_address())
        .with_ws_address(test_address())
        .with_http(Default::default())
        .with_http_address(test_address())
        .start(&server)
        .await
        .unwrap()
}

/// Launches a new server with http and ws and with the given modules on the same port.
pub async fn launch_http_ws_same_port(modules: impl Into<RpcModuleSelection>) -> RpcServerHandle {
    let builder = test_rpc_builder();
    let modules = modules.into();
    let eth_api = builder.bootstrap_eth_api();
    let server = builder.build(
        TransportRpcModuleConfig::set_ws(modules.clone()).with_http(modules),
        eth_api,
        EventSender::new(1),
    );
    let addr = test_address();
    RpcServerConfig::ws(Default::default())
        .with_ws_address(addr)
        .with_http(Default::default())
        .with_http_address(addr)
        .start(&server)
        .await
        .unwrap()
}

/// Returns an [`RpcModuleBuilder`] with testing components.
pub fn test_rpc_builder(
) -> RpcModuleBuilder<EthPrimitives, NoopProvider, TestPool, NoopNetwork, EthEvmConfig, NoopConsensus>
{
    RpcModuleBuilder::default()
        .with_provider(NoopProvider::default())
        .with_pool(TestPoolBuilder::default().into())
        .with_network(NoopNetwork::default())
        .with_executor(Box::new(TokioTaskExecutor::default()))
        .with_evm_config(EthEvmConfig::mainnet())
        .with_consensus(NoopConsensus::default())
}
