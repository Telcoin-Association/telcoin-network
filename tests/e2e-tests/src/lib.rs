//! End-to-end tests for TN protocol.

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

// pub for tests
pub mod util;

#[cfg(test)]
pub mod genesis_tests;
#[cfg(test)]
pub mod restarts;

#[cfg(test)]
#[cfg(feature = "faucet")]
pub mod faucet;

// prevent unused dep warning
#[cfg(feature = "faucet")]
use futures as _;
#[cfg(feature = "faucet")]
use gcloud_sdk as _;
#[cfg(feature = "faucet")]
use k256 as _;
