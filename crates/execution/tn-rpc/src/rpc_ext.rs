//! RPC extension that supports state sync through NVV peer request.

use crate::{
    error::{TNRpcError, TelcoinNetworkRpcResult},
    Handshake,
};
use async_trait::async_trait;
use jsonrpsee::proc_macros::rpc;
use reth_chainspec::ChainSpec;
use std::sync::Arc;

/// Telcoin Network RPC namespace.
///
/// TN-specific RPC endpoints.
#[rpc(server, namespace = "tn")]
pub trait TelcoinNetworkRpcExtApi {
    /// Transfer TEL to an address
    #[method(name = "validatorHandshake")]
    async fn handshake(&self, handshake: Handshake) -> TelcoinNetworkRpcResult<()>;
}

/// The type that implements `tn` namespace trait.
pub struct TelcoinNetworkRpcExt<N> {
    /// The chain id for this node.
    chain: Arc<ChainSpec>,
    /// The inner-node network.
    ///
    /// The interface that handles primary <-> engine network communication.
    _inner_node_network: N,
}

#[async_trait]
impl<N> TelcoinNetworkRpcExtApiServer for TelcoinNetworkRpcExt<N>
where
    N: Send + Sync + 'static,
{
    /// Handshake method.
    ///
    /// The handshake forwards peer requests to the Primary.
    async fn handshake(&self, handshake: Handshake) -> TelcoinNetworkRpcResult<()> {
        // verify proof of possession
        if !handshake.verify_proof(self.chain.genesis()) {
            return Err(TNRpcError::InvalidProofOfPossession);
        }

        // TODO: forward to primary
        // self.inner_node_network.new_peer
        Ok(())
    }
}

impl<N> TelcoinNetworkRpcExt<N> {
    /// Create new instance of the Telcoin Network RPC extension.
    pub fn new(chain: Arc<ChainSpec>, _inner_node_network: N) -> Self {
        Self { chain, _inner_node_network }
    }
}
