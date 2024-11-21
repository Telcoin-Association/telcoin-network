//! Inner-node network impl for the engine.

use tokio::sync::mpsc;

/// Engine to Primary message types.
pub enum EngineToPrimaryMessage {
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
    pub from_network: mpsc::Receiver<()>,
}
