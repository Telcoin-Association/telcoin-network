//! Bounded process-wide occupancy of return-validated connection sources.

use std::{
    collections::BTreeMap,
    fmt,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, Mutex},
};

/// Finite connection and table limits supplied by deployment configuration.
#[derive(Clone, Debug)]
pub(super) struct Limits {
    /// Process-wide simultaneous connection ceiling.
    connections: usize,
    /// Connection ceiling for one authenticated identity.
    per_peer: usize,
    /// Connection ceiling for one observed IP address.
    per_address: usize,
    /// Connection ceiling for one observed IP prefix.
    per_prefix: usize,
    /// Ceiling on distinct observed addresses retained at once.
    sources: usize,
    /// Deployment-selected IPv4 prefix length.
    ipv4_prefix: u8,
    /// Deployment-selected IPv6 prefix length.
    ipv6_prefix: u8,
}

impl Limits {
    /// Validate a finite budget without introducing production defaults.
    pub(super) fn new(
        connections: usize,
        per_peer: usize,
        per_address: usize,
        per_prefix: usize,
        sources: usize,
        prefixes: (u8, u8),
    ) -> Result<Self, AdmissionError> {
        if [connections, per_peer, per_address, per_prefix, sources].contains(&0)
            || per_peer > connections
            || per_address > per_prefix
            || per_prefix > connections
            || sources > connections
            || prefixes.0 > 32
            || prefixes.1 > 128
        {
            Err(AdmissionError::InvalidLimits)
        } else {
            Ok(Self {
                connections,
                per_peer,
                per_address,
                per_prefix,
                sources,
                ipv4_prefix: prefixes.0,
                ipv6_prefix: prefixes.1,
            })
        }
    }
}

/// Reason an established connection cannot acquire source occupancy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    /// A limit is zero, inconsistent, or has an invalid prefix length.
    InvalidLimits,
    /// The process-wide connection ceiling has been reached.
    ProcessFull,
    /// The authenticated identity has reached its connection ceiling.
    PeerFull,
    /// The observed address has reached its connection ceiling.
    AddressFull,
    /// The observed prefix has reached its connection ceiling.
    PrefixFull,
    /// No table slot remains for another observed address.
    SourcesFull,
    /// The established transport address is not a direct QUIC IP address.
    UnsupportedAddress,
    /// A connection already owns an occupancy lease.
    DuplicateConnection,
    /// A prior panic left the shared accounting lock poisoned.
    Poisoned,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLimits => "invalid source admission limits",
            Self::ProcessFull => "process connection budget exhausted",
            Self::PeerFull => "peer connection budget exhausted",
            Self::AddressFull => "source address connection budget exhausted",
            Self::PrefixFull => "source prefix connection budget exhausted",
            Self::SourcesFull => "source accounting table exhausted",
            Self::UnsupportedAddress => "source admission requires an established QUIC IP address",
            Self::DuplicateConnection => "connection already owns source occupancy",
            Self::Poisoned => "source admission accounting lock poisoned",
        })
    }
}

impl std::error::Error for AdmissionError {}

/// Active occupancy with no tombstones after the last lease leaves.
#[derive(Debug, Default)]
struct Occupancy {
    /// Owned connections, including reservations before swarm acceptance.
    connections: usize,
    /// Identity counts bounded by the connection ceiling.
    peers: BTreeMap<Vec<u8>, usize>,
    /// Address counts bounded by the table ceiling.
    addresses: BTreeMap<IpAddr, usize>,
    /// Prefix counts bounded by the table ceiling.
    prefixes: BTreeMap<IpAddr, usize>,
}

/// Process-wide source accounting occupancy, with no peer or address labels.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SourceOccupancy {
    connections: usize,
    peer_rows: usize,
    address_rows: usize,
    prefix_rows: usize,
}

impl SourceOccupancy {
    /// Reserved connections across every primary and worker swarm.
    pub const fn connections(self) -> usize {
        self.connections
    }
    /// Retained identity rows.
    pub const fn peer_rows(self) -> usize {
        self.peer_rows
    }
    /// Retained source-address rows.
    pub const fn address_rows(self) -> usize {
        self.address_rows
    }
    /// Retained network-prefix rows.
    pub const fn prefix_rows(self) -> usize {
        self.prefix_rows
    }
}

/// Shared accounting instance for every primary and worker swarm.
#[derive(Clone, Debug)]
pub(super) struct Budget {
    /// Immutable deployment-selected limits.
    limits: Limits,
    /// Occupancy updated atomically across swarms.
    occupancy: Arc<Mutex<Occupancy>>,
}

impl Budget {
    /// Create an empty budget from validated limits.
    pub(super) fn new(limits: Limits) -> Self {
        Self { limits, occupancy: Arc::new(Mutex::new(Occupancy::default())) }
    }

    /// Read all table counts under one lock without retaining source identities.
    pub(super) fn snapshot(&self) -> Result<SourceOccupancy, AdmissionError> {
        self.occupancy.lock().map_err(|_| AdmissionError::Poisoned).map(|occupancy| {
            SourceOccupancy {
                connections: occupancy.connections,
                peer_rows: occupancy.peers.len(),
                address_rows: occupancy.addresses.len(),
                prefix_rows: occupancy.prefixes.len(),
            }
        })
    }

    /// Reserve a connection after the transport proves return reachability.
    pub(super) fn acquire(&self, address: IpAddr, peer: Vec<u8>) -> Result<Lease, AdmissionError> {
        let address = canonical_address(address);
        let prefix = prefix(address, &self.limits);
        let mut occupancy = self.occupancy.lock().map_err(|_| AdmissionError::Poisoned)?;
        match () {
            () if occupancy.connections >= self.limits.connections => {
                Err(AdmissionError::ProcessFull)
            }
            () if occupancy.peers.get(&peer).copied().unwrap_or_default()
                >= self.limits.per_peer =>
            {
                Err(AdmissionError::PeerFull)
            }
            () if occupancy.addresses.get(&address).copied().unwrap_or_default()
                >= self.limits.per_address =>
            {
                Err(AdmissionError::AddressFull)
            }
            () if occupancy.prefixes.get(&prefix).copied().unwrap_or_default()
                >= self.limits.per_prefix =>
            {
                Err(AdmissionError::PrefixFull)
            }
            () if !occupancy.addresses.contains_key(&address)
                && occupancy.addresses.len() >= self.limits.sources =>
            {
                Err(AdmissionError::SourcesFull)
            }
            () => {
                occupancy.connections += 1;
                *occupancy.peers.entry(peer.clone()).or_default() += 1;
                *occupancy.addresses.entry(address).or_default() += 1;
                *occupancy.prefixes.entry(prefix).or_default() += 1;
                Ok(Lease { occupancy: self.occupancy.clone(), address, prefix, peer })
            }
        }
    }
}

/// Shared addresses and prefixes retain one row until their last lease leaves.
#[cfg(test)]
#[test]
fn occupancy_snapshot_tracks_shared_sources_and_release() -> Result<(), AdmissionError> {
    let budget = Budget::new(Limits::new(8, 3, 4, 8, 8, (24, 64))?);
    assert_eq!(budget.snapshot()?, SourceOccupancy::default());
    let address = IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1));
    let first = budget.acquire(address, vec![1])?;
    let shared = budget.acquire(address, vec![2])?;
    let other = budget.acquire(IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)), vec![3])?;
    let occupied = budget.snapshot()?;
    assert_eq!(
        (
            occupied.connections(),
            occupied.peer_rows(),
            occupied.address_rows(),
            occupied.prefix_rows()
        ),
        (3, 3, 2, 1)
    );
    drop(shared);
    drop(first);
    let remaining = budget.snapshot()?;
    assert_eq!(
        (
            remaining.connections(),
            remaining.peer_rows(),
            remaining.address_rows(),
            remaining.prefix_rows()
        ),
        (1, 1, 1, 1)
    );
    drop(other);
    assert_eq!(budget.snapshot()?, SourceOccupancy::default());
    Ok(())
}

/// Sole owner of one connection's occupancy, released on drop.
#[derive(Debug)]
pub(crate) struct Lease {
    /// Shared state whose counters this connection owns.
    occupancy: Arc<Mutex<Occupancy>>,
    /// Canonical observed address charged by this connection.
    address: IpAddr,
    /// Canonical observed prefix charged by this connection.
    prefix: IpAddr,
    /// Authenticated identity charged by this connection.
    peer: Vec<u8>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut occupancy = self.occupancy.lock().unwrap_or_else(|error| error.into_inner());
        occupancy.connections = occupancy.connections.saturating_sub(1);
        release(&mut occupancy.peers, &self.peer);
        release(&mut occupancy.addresses, &self.address);
        release(&mut occupancy.prefixes, &self.prefix);
    }
}

/// Decrement one key and expire it when its final connection leaves.
fn release<K: Ord>(counts: &mut BTreeMap<K, usize>, key: &K) {
    let empty = counts.get_mut(key).is_some_and(|count| {
        *count = count.saturating_sub(1);
        *count == 0
    });
    if empty {
        counts.remove(key);
    }
}

/// Charge IPv4-mapped IPv6 addresses to their native IPv4 source.
fn canonical_address(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(address) => IpAddr::V4(address),
        IpAddr::V6(address) => address.to_ipv4_mapped().map_or(IpAddr::V6(address), IpAddr::V4),
    }
}

/// Mask host bits without shifting by the address family's full bit width.
fn prefix(address: IpAddr, limits: &Limits) -> IpAddr {
    match address {
        IpAddr::V4(address) => {
            let mask = u32::MAX
                .checked_shl(32_u32.saturating_sub(u32::from(limits.ipv4_prefix)))
                .unwrap_or_default();
            IpAddr::V4((u32::from(address) & mask).into())
        }
        IpAddr::V6(address) => {
            let mask = u128::MAX
                .checked_shl(128_u32.saturating_sub(u32::from(limits.ipv6_prefix)))
                .unwrap_or_default();
            IpAddr::V6(Ipv6Addr::from(u128::from(address) & mask))
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
