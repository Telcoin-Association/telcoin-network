//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use libp2p::{
    request_response::{self, Codec},
    Swarm,
};
use tokio::sync::mpsc::{Receiver, Sender};

/// The network type for consensus messages.
///
/// The primary and workers use separate instances of this network to reliably send messages to other peers within the committee. The isolation of these networks is intended to:
/// - prevent a surge in one network message type from overwhelming all network traffic
/// - provide more granular control over resource allocation
/// - allow specific network configurations based on worker/primary needs
pub struct ConsensusNetwork<C: Codec + Send + Clone + 'static> {
    /// The gossip network for flood publishing sealed worker blocks.
    swarm: Swarm<request_response::Behaviour<C>>,
    /// The sender for network handles.
    handle: Sender<()>,
    /// The receiver for processing network handle requests.
    commands: Receiver<()>,
}
