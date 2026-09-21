//! Configuration for a directory serving one chain and role's records.

use clap::{ArgGroup, Parser};
use libp2p::Multiaddr;
use reqwest::Url;
use std::{fmt, net::SocketAddr, path::PathBuf, str::FromStr, time::Duration};
use tn_node_record::NetworkType;
use tn_types::{BlsPublicKey, WorkerId};

/// Command-line options for the read-only directory.
#[derive(Debug, Parser)]
#[command(about = "Serve verified Telcoin DHT node records as JSON")]
#[command(group(ArgGroup::new("key_sources").required(true).multiple(true)
    .args(["key", "keys_file", "committee_file", "rpc_url"])))]
pub(crate) struct Args {
    /// HTTP listen address.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,
    /// Chain ID used for protocols, signature verification and RPC source checks.
    #[arg(long)]
    chain_id: u64,
    /// DHT role: primary or worker:ID. Public RPC advertisements live on worker DHTs.
    #[arg(long, default_value = "worker:0")]
    network: Network,
    /// QUIC bootstrap address including its terminal /p2p/PEER_ID. May be repeated.
    #[arg(long, required = true)]
    bootstrap: Vec<Multiaddr>,
    /// Raw 96-byte BLS public key in hex, optionally prefixed with 0x. May be repeated.
    #[arg(long, value_parser = parse_key)]
    key: Vec<BlsPublicKey>,
    /// JSON array of hex BLS keys, reloaded each refresh with last-good fallback.
    #[arg(long)]
    keys_file: Option<PathBuf>,
    /// Standard committee YAML file, reloaded with last-good fallback.
    #[arg(long)]
    committee_file: Option<PathBuf>,
    /// JSON-RPC source for the live committee. May be repeated; sources are unioned.
    #[arg(long)]
    rpc_url: Vec<Url>,
    /// Seconds between refresh cycles. Slow cycles skip missed interval ticks.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    refresh_seconds: u64,
    /// Deadline in seconds for each DHT lookup and each RPC source request.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..))]
    timeout_seconds: u64,
}

impl Args {
    /// The HTTP address to bind.
    pub(crate) fn bind(&self) -> SocketAddr {
        self.bind
    }
    /// The expected chain ID.
    pub(crate) fn chain_id(&self) -> u64 {
        self.chain_id
    }
    /// The network domain selected by the operator.
    pub(crate) fn network(&self) -> NetworkType {
        self.network.0
    }
    /// A stable display name included in API responses.
    pub(crate) fn network_name(&self) -> String {
        self.network.to_string()
    }
    /// Bootstrap addresses for the selected DHT.
    pub(crate) fn bootstrap(&self) -> Vec<Multiaddr> {
        self.bootstrap.clone()
    }
    /// Explicit keys that always remain in the tracked set.
    pub(crate) fn keys(&self) -> &[BlsPublicKey] {
        &self.key
    }
    /// Optional JSON key-list source.
    pub(crate) fn keys_file(&self) -> Option<&PathBuf> {
        self.keys_file.as_ref()
    }
    /// Optional committee YAML source.
    pub(crate) fn committee_file(&self) -> Option<&PathBuf> {
        self.committee_file.as_ref()
    }
    /// Live committee RPC sources.
    pub(crate) fn rpc_urls(&self) -> &[Url] {
        &self.rpc_url
    }
    /// Refresh cadence.
    pub(crate) fn refresh_interval(&self) -> Duration {
        Duration::from_secs(self.refresh_seconds)
    }
    /// Timeout for an individual network operation.
    pub(crate) fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds)
    }
}

/// Parser for the role and optional worker ID.
#[derive(Clone, Copy, Debug)]
struct Network(NetworkType);

impl FromStr for Network {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == "primary" {
            Ok(Self(NetworkType::Primary))
        } else {
            value
                .strip_prefix("worker:")
                .ok_or_else(|| "network must be primary or worker:ID".to_owned())?
                .parse::<WorkerId>()
                .map(|id| Self(NetworkType::Worker(id)))
                .map_err(|error| error.to_string())
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            NetworkType::Primary => f.write_str("primary"),
            NetworkType::Worker(id) => write!(f, "worker:{id}"),
        }
    }
}

/// Decode the same raw compressed BLS bytes used as the DHT key.
pub(crate) fn parse_key(value: &str) -> Result<BlsPublicKey, String> {
    let bytes = hex::decode(value.strip_prefix("0x").unwrap_or(value))
        .map_err(|error| error.to_string())?;
    BlsPublicKey::from_literal_bytes(&bytes).map_err(|error| format!("{error:?}"))
}

/// Canonical public representation of a DHT lookup key.
pub(crate) fn format_key(key: &BlsPublicKey) -> String {
    format!("0x{}", hex::encode(key.as_ref()))
}
