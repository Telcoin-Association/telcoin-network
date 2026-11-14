//! Test utils.

mod tracing;
pub use tracing::*;

use crate::{BlsPublicKey, BootstrapServer};
use clap::{Args, Parser};
use std::collections::BTreeMap;

/// A fixed map of four bootstrap servers with real keys, one worker each, on loopback addresses.
///
/// Independent of the bundled chain lists (`chain-configs/*/bootstrap_servers.yaml`), which change
/// with releases, so tests that only need well-formed entries do not break when a list changes.
pub fn bootstrap_servers_fixture() -> BTreeMap<BlsPublicKey, BootstrapServer> {
    serde_yaml::from_str(include_str!("bootstrap_servers.yaml"))
        .expect("the bootstrap servers fixture parses")
}
/// A helper type to parse Args more easily.
#[derive(Parser, Debug)]
pub struct CommandParser<T: Args> {
    #[clap(flatten)]
    pub args: T,
}
