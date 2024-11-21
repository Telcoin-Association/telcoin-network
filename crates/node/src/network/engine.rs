//! Inner-node network impl for the engine.

use super::PrimaryToEngineMessage;
use consensus_network::EngineToPrimaryClient;
use consensus_network_types::CanonicalUpdateMessage;
use reth_primitives::SealedHeader;
use tokio::sync::mpsc;

/// Engine to Primary message types.
pub enum EngineToPrimaryMessage {
    /// Engine produced a new canonical update.
    CanonicalUpdate(CanonicalUpdateMessage),
    /// Handshake received from peer.
    ///
    /// TODO: name this NewPeer / PeerRequest?
    Handshake,
}

/// The engine's handle to the inner-node network.
pub struct EngineInnerNetworkHandle {
    /// Sending half to the inner-node network for engine to primary messages.
    pub to_network: mpsc::Sender<EngineToPrimaryMessage>,
    /// Receiver for inner-node network messages.
    pub from_network: mpsc::Receiver<PrimaryToEngineMessage>,
}

impl EngineToPrimaryClient for EngineInnerNetworkHandle {
    async fn canonical_update(&self, tip: SealedHeader) -> eyre::Result<()> {
        let msg = EngineToPrimaryMessage::CanonicalUpdate(CanonicalUpdateMessage { tip });
        Ok(self.to_network.send(msg).await?)
    }
}
