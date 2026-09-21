//! Errors a reader can distinguish without parsing log messages.

use std::{fmt, io};

/// A failure to configure a reader or complete a verified lookup.
#[derive(Debug)]
pub enum Error {
    /// No bootstrap address contained a usable QUIC endpoint and terminal peer ID.
    InvalidBootstrap,
    /// None of the bootstrap peers could be reached.
    BootstrapUnavailable,
    /// Peers connected, but none answered on the requested chain and role protocol.
    NoCompatiblePeers,
    /// The lookup deadline expired without a verified record.
    Timeout,
    /// Copies were returned, but none passed all validation checks.
    NoVerifiedRecords,
    /// Invalid protocol or timeout configuration.
    Configuration(String),
    /// The QUIC/DNS transport could not be configured.
    Transport(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBootstrap => {
                f.write_str("no usable QUIC bootstrap address with a peer ID")
            }
            Self::BootstrapUnavailable => f.write_str("no bootstrap peer could be reached"),
            Self::NoCompatiblePeers => {
                f.write_str("no peer answered on this chain and role protocol")
            }
            Self::Timeout => f.write_str("node record lookup timed out"),
            Self::NoVerifiedRecords => f.write_str("records were returned, but none verified"),
            Self::Configuration(reason) => write!(f, "invalid reader configuration: {reason}"),
            Self::Transport(error) => write!(f, "cannot configure reader transport: {error}"),
        }
    }
}

impl std::error::Error for Error {}
