//! Daemon configuration, source and runtime errors.

use std::{fmt, io};

/// Failures reported at daemon boundaries.
#[derive(Debug)]
pub(crate) enum Error {
    /// A local file or HTTP listener failed.
    Io(io::Error),
    /// A key-list or RPC response was not valid JSON.
    Json(serde_json::Error),
    /// A committee file was not valid committee YAML.
    Yaml(serde_yaml::Error),
    /// An HTTP key-source request failed.
    Http(reqwest::Error),
    /// A source returned an RPC error or an invalid envelope.
    Rpc(String),
    /// A configured or returned BLS key could not be decoded.
    InvalidKey(String),
    /// The DHT reader could not be configured.
    Kad(tn_kad_client::Error),
    /// The refresh task terminated unexpectedly.
    RefreshStopped,
    /// A runtime task failed.
    Task(tokio::task::JoinError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Json(error) => write!(f, "invalid JSON: {error}"),
            Self::Yaml(error) => write!(f, "invalid committee file: {error}"),
            Self::Http(error) => write!(f, "key source request failed: {error}"),
            Self::Rpc(reason) => write!(f, "invalid key source response: {reason}"),
            Self::InvalidKey(reason) => write!(f, "invalid BLS public key: {reason}"),
            Self::Kad(error) => write!(f, "{error}"),
            Self::RefreshStopped => f.write_str("directory refresh task stopped"),
            Self::Task(error) => write!(f, "directory task failed: {error}"),
        }
    }
}

impl std::error::Error for Error {}
