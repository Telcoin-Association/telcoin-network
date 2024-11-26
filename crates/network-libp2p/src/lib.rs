//! Peer-to-peer network interface for Telcoin Network built using libp2p.

mod gossip;
pub use gossip::start_gossip_publish_network;
