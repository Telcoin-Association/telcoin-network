// SPDX-License-Identifier: MIT or Apache-2.0
//! Signed kademlia node records and the wire-protocol names Telcoin Network peers negotiate on.
//!
//! A validator publishes a BLS-signed [`NodeRecord`] to the kademlia DHT of each network it runs
//! (its primary swarm and every worker swarm), keyed by the raw 96-byte compressed BLS public
//! key. This crate holds the record schema, the [`RecordDomain`] its signature is bound to, and
//! the [`NetworkType`] protocol names a peer must negotiate on to reach that DHT — everything a
//! consumer needs to *read* a record without running a node.
//!
//! The node's networking crate re-exports these items so its call sites are unchanged; a
//! read-only client depends on this crate alone and pulls in none of the node's storage,
//! metrics, or peer-management machinery.

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

mod parse;
mod protocol;
mod record;

pub use parse::{parse_bls_pubkey, ParseBlsPubkeyError};
pub use protocol::{gossip_protocol_id_prefix, NetworkType};
pub use record::{NetworkInfo, NodeRecord, RecordDomain, MAX_ADVERTISED_MULTIADDRS};

// re-export the libp2p and tn-types items that appear in this crate's public API so a consumer
// can name them without a direct dependency on either
pub use libp2p::Multiaddr;
pub use tn_types::{BlsPublicKey, RpcInfo};
