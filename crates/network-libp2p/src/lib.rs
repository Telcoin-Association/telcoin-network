// SPDX-License-Identifier: MIT or Apache-2.0
//! Peer-to-peer network interface for Telcoin Network built using libp2p.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    rustdoc::all,
    unused_crate_dependencies
)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod codec;
mod consensus;
pub mod error;
pub mod kad;
mod peers;
pub mod types;

// export types
pub use codec::{TNCodec, TNMessage};
pub use consensus::ConsensusNetwork;
pub use peers::{PeerExchangeMap, Penalty};

// re-export specific libp2p types
pub use libp2p::{
    gossipsub::Message as GossipMessage, identity::PeerId, request_response::ResponseChannel,
    Multiaddr,
};
#[cfg(test)]
#[path = "./tests/common.rs"]
pub(crate) mod common;
