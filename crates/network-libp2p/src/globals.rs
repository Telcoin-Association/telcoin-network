//! Globally accessible network data.
//!

use crate::peers::PeerService;
use parking_lot::RwLock;

pub struct NetworkGlobals {
    /// The collection of known peers.
    pub peers: RwLock<PeerService>,
}
