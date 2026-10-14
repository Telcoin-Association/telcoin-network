//! Finite, ordered IP/QUIC endpoints covered by a node record's signature.

use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::{collections::HashSet, fmt};
use tn_types::NetworkPublicKey;

/// Two endpoint generations, each with one IPv4 and one IPv6 endpoint.
///
/// Writers, record readers, provider storage and peer caches share this bound. DNS names are
/// resolved by the operator's node before signing and never enter a signed record.
pub const MAX_ADVERTISED_MULTIADDRS: usize = 4;

/// Why an advertised endpoint list cannot be signed or admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressError {
    /// A record must advertise at least one endpoint.
    Empty,
    /// The endpoint count exceeds the shared migration budget.
    TooMany,
    /// Two entries name the same endpoint, including with and without a peer suffix.
    Duplicate,
    /// Only an IP, a nonzero UDP port and QUIC v1 are accepted.
    InvalidTransport,
    /// A trailing peer identity differs from the record's transport identity.
    InvalidPeerId,
}

impl fmt::Display for AddressError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "advertised endpoint list is empty",
            Self::TooMany => "advertised endpoint list exceeds the four-address budget",
            Self::Duplicate => "advertised endpoints contain a duplicate",
            Self::InvalidTransport => {
                "advertised endpoint must use IP/UDP/QUIC v1 with a nonzero port"
            }
            Self::InvalidPeerId => "advertised endpoint has a different peer identity",
        })
    }
}

impl std::error::Error for AddressError {}

/// Validate ordered endpoints without changing the bytes that are signed.
///
/// The optional `/p2p` suffix must bind to `pubkey`. DNS, relays, wildcard IPs, multicast and
/// additional protocol components are rejected. Private IPs remain usable on private networks.
pub fn validate_advertised_addresses(
    addresses: &[Multiaddr],
    pubkey: &NetworkPublicKey,
) -> Result<(), AddressError> {
    (!addresses.is_empty()).then_some(()).ok_or(AddressError::Empty)?;
    (addresses.len() <= MAX_ADVERTISED_MULTIADDRS).then_some(()).ok_or(AddressError::TooMany)?;
    let expected: PeerId = pubkey.clone().into();
    addresses.iter().try_fold(HashSet::new(), |mut seen, address| {
        let mut protocols = address.iter();
        let ip = protocols.next();
        let usable_ip = matches!(ip.as_ref(), Some(Protocol::Ip4(ip)) if !ip.is_unspecified() && !ip.is_multicast() && !ip.is_broadcast())
            || matches!(ip.as_ref(), Some(Protocol::Ip6(ip)) if !ip.is_unspecified() && !ip.is_multicast());
        (usable_ip
            && matches!(protocols.next(), Some(Protocol::Udp(port)) if port != 0)
            && matches!(protocols.next(), Some(Protocol::QuicV1)))
            .then_some(()).ok_or(AddressError::InvalidTransport)?;
        protocols.next().is_none_or(|suffix| suffix == Protocol::P2p(expected))
            .then_some(()).ok_or(AddressError::InvalidPeerId)?;
        protocols.next().is_none().then_some(()).ok_or(AddressError::InvalidTransport)?;
        let endpoint: Multiaddr = address.iter().take(3).collect();
        seen.insert(endpoint).then_some(()).ok_or(AddressError::Duplicate)?;
        Ok(seen)
    }).map(|_| ())
}
