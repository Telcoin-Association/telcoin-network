//! Consensus p2p network.
//!
//! This network is used by workers and primaries to reliably send consensus messages.

use libp2p::{request_response, Swarm};
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ConsensusNetwork {
    /// The gossip network for flood publishing sealed worker blocks.
    swarm: Swarm<request_response::Behaviour>,
    /// The sender for network handles.
    handle: Sender<()>,
    /// The receiver for processing network handle requests.
    commands: Receiver<()>,
}
