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
pub struct TelcoinNetworkRpcExt<N, P> {
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    inner_node_network: N,
    /// The database provider.
    provider: P,
}

#[async_trait]
impl<N, P> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N, P>
where
    N: Send + Sync + 'static,
    P: Send + Sync + 'static,
{
    /// Handshake method.
    ///
    /// The handshake forwards peer requests to the Primary.
    async fn handshake(&self) -> TelcoinNetworkRpcResult<()> {
        // basic verify then forward to primary
        // - chain id
        // - read from staking contract to indicate with message to primary
        //   that peer is NVV or OV for peer-type
        todo!()
    }
}

impl<N, P> TelcoinNetworkRpcExt<N, P> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(inner_node_network: N, provider: P) -> Self {
        Self { inner_node_network, provider }
    }
}
