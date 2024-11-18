//! RPC extension that supports state sync through NVV peer request.

use crate::error::TelcoinNetworkRpcResult;
use jsonrpsee::{core::async_trait, proc_macros::rpc};

/// Telcoin Network RPC namespace.
///
/// TN-specific RPC endpoints.
#[rpc(server, namespace = "tn")]
pub trait TelcoinNetworkRpcExtApi {
    /// Transfer TEL to an address
    #[method(name = "validatorHandshake")]
    async fn handshake(&self) -> TelcoinNetworkRpcResult<()>;
}

/// The type that implements `tn` namespace trait.
pub struct TelcoinNetworkRpcExt<N> {
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
}

#[async_trait]
impl<N> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    /// Handshake method.
    ///
    /// The handshake forwards peer requests to the Primary.
    async fn handshake(&self) -> TelcoinNetworkRpcResult<()> {
        // basic verify then forward to primary
        todo!()
    }
}

impl<N> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(inner_node_network: N) -> Self {
        Self { inner_node_network }
    }
}
