// SPDX-License-Identifier: MIT or Apache-2.0
//! Read-only kademlia client for fetching a validator's BLS-signed [`NodeRecord`] from a Telcoin
//! Network DHT without running a node.
//!
//! A validator publishes one signed [`NodeRecord`] per network it runs (its primary swarm and
//! each worker swarm), keyed by the raw 96-byte compressed BLS public key. This crate dials one
//! or more bootstrap peers of exactly one of those DHTs over QUIC, negotiates kademlia on that
//! network's chain-namespaced protocol name, issues `GET_VALUE` lookups, and hands back records
//! that pass the same validation the node applies to records it learns from peers.
//!
//! The client is strictly a reader. It runs kademlia in [`libp2p::kad::Mode::Client`] so it never
//! advertises the protocol and is never inserted into a server's routing table, it disables
//! write-back caching, and it filters every inbound record push so nothing a peer sends is ever
//! stored or re-served. It never listens on a socket. It also carries a gossipsub behaviour that
//! subscribes to nothing and publishes nothing: the node fatally penalizes (bans, IP included) any
//! connection on which its chain-namespaced gossipsub protocol fails to negotiate, so a bare
//! kademlia client would be banned after its first lookup.
//!
//! Two consumers are intended: a one-shot CLI lookup that spawns a [`KadClient`], resolves a
//! single key, and shuts down; and a long-running directory service that keeps one client alive
//! and refreshes a batch of keys on an interval with [`KadClient::get_node_records`].

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

mod client;
mod config;
mod driver;
mod error;
mod verify;

pub use client::KadClient;
pub use config::{KadClientConfig, DEFAULT_QUERY_TIMEOUT};
pub use error::KadClientError;
pub use verify::VerifiedRecord;

// re-export the items that appear in this crate's public API so a consumer can name them without
// a direct dependency on the record crate or libp2p
pub use libp2p::PeerId;
pub use tn_node_record::{
    BlsPublicKey, Multiaddr, NetworkInfo, NetworkType, NodeRecord, RecordDomain, RpcInfo,
};

// dev-dependencies exercised only by the integration test in `tests/`; named here so the lib's
// own unit-test build does not trip `unused_crate_dependencies`
#[cfg(test)]
use eyre as _;
#[cfg(test)]
use serde as _;
#[cfg(test)]
use tn_config as _;
#[cfg(test)]
use tn_network_libp2p as _;
#[cfg(test)]
use tn_storage as _;
#[cfg(test)]
use tn_test_utils as _;
