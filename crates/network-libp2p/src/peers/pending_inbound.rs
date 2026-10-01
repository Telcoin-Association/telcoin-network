//! Finite pre-authentication slots, independent of claimed peer identities or addresses.

use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    net::{IpAddr, Ipv4Addr},
};

/// Validated source scope for pending handshakes, never an admission credential.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum AddressScope {
    /// One IPv4 source, including its IPv4-mapped IPv6 representation.
    Ipv4(Ipv4Addr),
    /// One IPv6 /64, preventing address churn within a source subnet from multiplying slots.
    Ipv6Prefix(u128),
}

impl From<IpAddr> for AddressScope {
    /// Normalize a transport-observed address to its pending-handshake source scope.
    fn from(ip: IpAddr) -> Self {
        match ip {
            IpAddr::V4(ip) => Self::Ipv4(ip),
            IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map_or_else(|| Self::Ipv6Prefix(u128::from(ip) >> 64), Self::Ipv4),
        }
    }
}

/// A finite pending-handshake budget that denied a new connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PendingInboundError {
    /// The aggregate pending-handshake budget is full.
    Total,
    /// The validated source address or IPv6 /64 budget is full.
    Address,
}

impl fmt::Display for PendingInboundError {
    /// Describe the exhausted budget without relying on a claimed peer identity.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Total => formatter.write_str("aggregate pending inbound budget exhausted"),
            Self::Address => formatter.write_str("source pending inbound budget exhausted"),
        }
    }
}

impl std::error::Error for PendingInboundError {}

/// Slots keyed by connection ID, released on authentication or any listen failure.
#[derive(Debug)]
pub(super) struct PendingInbound<K> {
    /// One source scope for each live pending connection.
    connections: HashMap<K, AddressScope>,
    /// Contributions per source scope, with zero-count entries removed immediately.
    by_address: HashMap<AddressScope, usize>,
    /// Maximum pending slots across all sources.
    max_total: usize,
    /// Maximum pending slots for any one source scope.
    max_address: usize,
}

impl<K: Eq + Hash> PendingInbound<K> {
    /// Derive a source budget that accommodates all configured priority peers sharing one NAT.
    ///
    /// It uses the same per-peer connection ceiling as the swarm and never exceeds the aggregate
    /// pending ceiling. No claimed identity or advertised address affects either budget.
    pub(super) fn new(
        max_priority_peers: usize,
        max_connections_per_peer: u32,
        max_total: u32,
    ) -> Self {
        let max_total = usize::try_from(max_total).unwrap_or(usize::MAX);
        let max_address = max_priority_peers
            .saturating_mul(usize::try_from(max_connections_per_peer).unwrap_or(usize::MAX))
            .max(1)
            .min(max_total);
        Self { connections: HashMap::new(), by_address: HashMap::new(), max_total, max_address }
    }

    /// Reserve an ordinary slot for an actual source IP, without granting identity privileges.
    pub(super) fn reserve(&mut self, id: K, ip: IpAddr) -> Result<(), PendingInboundError> {
        let scope = AddressScope::from(ip);
        let count = self.by_address.get(&scope).copied().unwrap_or_default();
        match () {
            () if self.connections.contains_key(&id) => Ok(()),
            () if self.connections.len() >= self.max_total => Err(PendingInboundError::Total),
            () if count >= self.max_address => Err(PendingInboundError::Address),
            () => {
                self.connections.insert(id, scope);
                self.by_address.insert(scope, count.saturating_add(1));
                Ok(())
            }
        }
    }

    /// Release exactly one connection's contribution; duplicate or unknown releases do nothing.
    pub(super) fn release(&mut self, id: &K) {
        self.connections.remove(id).into_iter().for_each(|scope| {
            self.by_address.get_mut(&scope).into_iter().for_each(|count| {
                *count = count.saturating_sub(1);
            });
            if self.by_address.get(&scope) == Some(&0) {
                self.by_address.remove(&scope);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingInbound, PendingInboundError};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    /// Finite source and aggregate budgets remain independent and release only their own slots.
    #[test]
    fn finite_budgets_and_exact_release() {
        let mut pending = PendingInbound::new(1, 2, 3);
        let shared = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let other = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));
        assert_eq!(pending.reserve(1, shared), Ok(()));
        assert_eq!(pending.reserve(2, shared), Ok(()));
        assert_eq!(pending.reserve(3, shared), Err(PendingInboundError::Address));
        assert_eq!(pending.reserve(3, other), Ok(()));
        assert_eq!(pending.reserve(4, other), Err(PendingInboundError::Total));
        pending.release(&1);
        pending.release(&1);
        pending.release(&9);
        assert_eq!(pending.reserve(4, shared), Ok(()));
        assert_eq!(pending.reserve(5, other), Err(PendingInboundError::Total));
        [2, 3, 4].iter().for_each(|id| pending.release(id));
        assert!(pending.connections.is_empty());
        assert!(pending.by_address.is_empty());
    }

    /// IPv6 source churn and IPv4-mapped addresses cannot multiply the source budget.
    #[test]
    fn source_scopes_and_saturating_configuration() {
        let mut pending = PendingInbound::new(1, 1, 3);
        let subnet = |host| IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 1, 2, 0, 0, 0, host));
        assert_eq!(pending.reserve(1, subnet(1)), Ok(()));
        assert_eq!(pending.reserve(2, subnet(2)), Err(PendingInboundError::Address));
        let ipv4 = Ipv4Addr::new(192, 0, 2, 1);
        assert_eq!(pending.reserve(2, IpAddr::V4(ipv4)), Ok(()));
        assert_eq!(
            pending.reserve(3, IpAddr::V6(ipv4.to_ipv6_mapped())),
            Err(PendingInboundError::Address)
        );
        assert_eq!(PendingInbound::<u64>::new(usize::MAX, u32::MAX, 3).max_address, 3);
        assert_eq!(PendingInbound::<u64>::new(0, 0, 3).max_address, 1);
        assert_eq!(
            PendingInbound::<u64>::new(1, 1, 0).reserve(1, subnet(1)),
            Err(PendingInboundError::Total)
        );
    }
}
