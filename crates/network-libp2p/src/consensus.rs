//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use std::time::Duration;

use crate::{
    codec::{TNCodec, TNMessage},
    helpers::primary_gossip_config,
    types::NetworkResult,
};
use libp2p::{
    gossipsub,
    request_response::{self, Codec, ProtocolSupport},
    swarm::NetworkBehaviour,
    StreamProtocol, Swarm, SwarmBuilder,
};
use tn_config::ConsensusConfig;
use tokio::sync::mpsc::{Receiver, Sender};

/// Custom network libp2p behaviour type for Telcoin Network.
///
/// The behavior includes gossipsub, request-response, and identify.
/// TODO: possibly KAD?
#[derive(NetworkBehaviour)]
pub struct TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// The gossipsub network behavior.
    gossipsub: gossipsub::Behaviour,
    /// The request-response network behavior.
    req_res: request_response::Behaviour<C>,
}

impl<C> TNBehavior<C>
where
    C: Codec + Send + Clone + 'static,
{
    /// Create a new instance of Self.
    pub fn new(gossipsub: gossipsub::Behaviour, req_res: request_response::Behaviour<C>) -> Self {
        Self { gossipsub, req_res }
    }
}

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to
/// other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
///
/// TODO: Primaries gossip signatures of final execution state at epoch boundaries and workers
/// gossip transactions? Publishers usually broadcast to several peers, so this may not be efficient
/// (multiple txs submitted).
pub struct ConsensusNetwork<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// The gossip network for flood publishing sealed worker blocks.
    swarm: Swarm<TNBehavior<TNCodec<Req, Res>>>,
    /// The sender for network handles.
    handle: Sender<()>,
    /// The receiver for processing network handle requests.
    commands: Receiver<()>,
}

impl<Req, Res> ConsensusNetwork<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    /// Create a new instance of Self.
    ///
    /// TODO: add NetworkResult errors before merge - using `expect` for quicker refactors
    /// !!!~~~~~~~k
    pub fn new<DB>(config: &ConsensusConfig<DB>) -> NetworkResult<Self>
    where
        // TODO: need to import tn-storage just for this trait?
        DB: tn_storage::traits::Database,
    {
        let gossipsub_config = primary_gossip_config()?;

        //
        //
        // TODO: pass keypair as arg so this function stays agnostic to primary/worker
        // - don't put helper method on key config bc that is TN-specific, and this is required by
        //   libp2p
        // - need to separate worker/primary network signatures
        let mut key_bytes = config.key_config().primary_network_keypair().as_ref().to_vec();
        let keypair = libp2p::identity::Keypair::ed25519_from_bytes(&mut key_bytes).expect("TODO");

        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            gossipsub_config,
        )
        .expect("TODO");

        // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
        //
        // revisit keypair approach

        // TODO: use const
        let codec = TNCodec::<Req, Res>::new(1024 * 1024);
        // TODO: is StreamProtocol sufficient?
        // - ProtocolSupport::Full?
        let protocols = [(StreamProtocol::new("/tn-consensus"), ProtocolSupport::Full)];
        let req_res =
            request_response::Behaviour::new(protocols, request_response::Config::default());
        let behavior = TNBehavior::new(gossipsub, req_res);

        // create swarm
        let mut swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_tokio()
            .with_quic()
            .with_behaviour(|_| behavior)
            .expect("TODO")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
            .build();

        let (handle, commands) = tokio::sync::mpsc::channel(100);
        Ok(Self { swarm, handle, commands })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_worker_network() {
        // let network = ConsensusNetwork::new();
        todo!()
    }
}
