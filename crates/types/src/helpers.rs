//! Helpers for starting a node

use secp256k1::PublicKey;
use std::net::{TcpListener, UdpSocket};

const MAX_RETRIES: u32 = 1000;

/// Represents the type of socket to create
#[derive(Debug, Clone, Copy)]
pub enum SocketType {
    Tcp,
    Udp,
}

/// Configuration for port discovery
#[derive(Debug, Clone)]
pub struct PortConfig {
    pub host: String,
    pub socket_type: SocketType,
    pub max_retries: u32,
}

/// Error types for port operations
#[derive(Debug)]
pub enum PortError {
    IoError(std::io::Error),
    NoPortsAvailable,
}

/// Get an available port with the specified configuration
pub fn get_available_port(config: &PortConfig) -> Result<u16, PortError> {
    for _ in 0..config.max_retries {
        if let Ok(port) = get_ephemeral_port(&config.host, config.socket_type) {
            return Ok(port);
        }
    }
    Err(PortError::NoPortsAvailable)
}

impl From<std::io::Error> for PortError {
    fn from(error: std::io::Error) -> Self {
        PortError::IoError(error)
    }
}

/// Get an ephemeral port for the specified socket type
fn get_ephemeral_port(host: &str, socket_type: SocketType) -> std::io::Result<u16> {
    match socket_type {
        SocketType::Tcp => {
            let listener = TcpListener::bind((host, 0))?;
            Ok(listener.local_addr()?.port())
        }
        SocketType::Udp => {
            let socket = UdpSocket::bind((host, 0))?;
            Ok(socket.local_addr()?.port())
        }
    }
}

/// Convenience function for getting a TCP port
pub fn get_available_tcp_port(host: &str) -> Option<u16> {
    let config = PortConfig {
        host: host.to_string(),
        socket_type: SocketType::Tcp,
        max_retries: MAX_RETRIES,
    };

    get_available_port(&config).ok()
}

/// Convenience function for getting a UDP port
pub fn get_available_udp_port(host: &str) -> Option<u16> {
    let config = PortConfig {
        host: host.to_string(),
        socket_type: SocketType::Udp,
        max_retries: MAX_RETRIES,
    };

    get_available_port(&config).ok()
}

/// Converts a public key into an ethereum address by hashing the encoded public key with
/// keccak256.
pub fn public_key_to_address(public: PublicKey) -> crate::Address {
    // strip out the first byte because that should be the SECP256K1_TAG_PUBKEY_UNCOMPRESSED
    // tag returned by libsecp's uncompressed pubkey serialization
    let hash = crate::keccak256(&public.serialize_uncompressed()[1..]);
    crate::Address::from_slice(&hash[12..])
}
