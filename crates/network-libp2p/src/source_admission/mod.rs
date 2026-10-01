//! Admission using observed QUIC addresses after the authenticated transport handshake.

mod core;
pub use core::{AdmissionError, SourceOccupancy};

use libp2p::{
    multiaddr::Protocol,
    swarm::{ConnectionId, FromSwarm},
    Multiaddr, PeerId,
};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};
use tn_config::SourceAdmissionConfig;

/// Process-wide established-connection accounting shared by all active swarms.
#[derive(Clone, Debug)]
pub struct SourceAdmissionBudget {
    /// Shared bounded occupancy and validated deployment limits.
    budget: core::Budget,
    /// Serialize snapshots and gauge publication across every swarm.
    measurements: Arc<Mutex<()>>,
}

impl SourceAdmissionBudget {
    /// Validate deployment limits and create one shared process budget.
    pub fn new(config: &SourceAdmissionConfig) -> Result<Self, AdmissionError> {
        core::Limits::new(
            config.max_connections(),
            config.max_connections_per_peer(),
            config.max_connections_per_address(),
            config.max_connections_per_prefix(),
            config.max_sources(),
            config.prefix_lengths(),
        )
        .map(|limits| {
            let budget =
                Self { budget: core::Budget::new(limits), measurements: Arc::new(Mutex::new(())) };
            budget.publish_occupancy();
            budget
        })
    }

    /// Current process-wide occupancy across primary and worker connections.
    pub fn snapshot(&self) -> Result<SourceOccupancy, AdmissionError> {
        self.budget.snapshot()
    }

    fn publish_occupancy(&self) {
        self.measurements
            .lock()
            .map_err(|_| AdmissionError::Poisoned)
            .and_then(|_measurements| {
                self.snapshot().map(|occupancy| {
                    metrics::gauge!("tn_network.source_connections")
                        .set(occupancy.connections() as f64);
                    metrics::gauge!("tn_network.source_peer_rows")
                        .set(occupancy.peer_rows() as f64);
                    metrics::gauge!("tn_network.source_address_rows")
                        .set(occupancy.address_rows() as f64);
                    metrics::gauge!("tn_network.source_prefix_rows")
                        .set(occupancy.prefix_rows() as f64);
                })
            })
            .unwrap_or_else(|_| {
                metrics::counter!("tn_network.source_accounting_errors_total").increment(1)
            });
    }

    /// Acquire occupancy using the remote endpoint of a completed QUIC handshake.
    ///
    /// Call only from the established-connection callbacks. Advertised discovery
    /// addresses, pending inbound arrivals, and claimed peer identities are not inputs.
    pub(crate) fn acquire(
        &self,
        address: &Multiaddr,
        peer: PeerId,
    ) -> Result<core::Lease, AdmissionError> {
        observed_ip(address).and_then(|ip| self.budget.acquire(ip, peer.to_bytes()))
    }
}

/// Leases owned by one swarm, using the process-wide budget when enabled.
#[derive(Debug, Default)]
pub(crate) struct SourceConnections {
    /// Optional shared budget installed before the swarm starts.
    budget: Option<SourceAdmissionBudget>,
    /// Leases keyed by connection identity, bounded by the process ceiling.
    leases: BTreeMap<ConnectionId, core::Lease>,
}

impl SourceConnections {
    /// Install the budget before polling this swarm.
    pub(crate) fn set_budget(&mut self, budget: Option<SourceAdmissionBudget>) {
        self.budget = budget;
        self.budget.as_ref().map_or_else(
            || {
                metrics::gauge!("tn_network.source_accounting_enabled").set(0.0);
                [
                    "tn_network.source_connections",
                    "tn_network.source_peer_rows",
                    "tn_network.source_address_rows",
                    "tn_network.source_prefix_rows",
                ]
                .into_iter()
                .for_each(|name| metrics::gauge!(name).set(0.0));
            },
            |budget| {
                metrics::gauge!("tn_network.source_accounting_enabled").set(1.0);
                budget.publish_occupancy();
            },
        );
    }

    /// Reserve only from an established-connection callback.
    pub(crate) fn reserve(
        &mut self,
        connection: ConnectionId,
        peer: PeerId,
        address: &Multiaddr,
    ) -> Result<(), AdmissionError> {
        if self.leases.contains_key(&connection) {
            Err(AdmissionError::DuplicateConnection)
        } else {
            self.budget.as_ref().map(|budget| budget.acquire(address, peer)).transpose().map(
                |lease| {
                    lease.map(|lease| self.leases.insert(connection, lease));
                    self.budget.iter().for_each(SourceAdmissionBudget::publish_occupancy);
                },
            )
        }
    }

    /// Release each closed or rejected connection even if other peer connections remain.
    ///
    /// A later behaviour's denial emits `ListenFailure` or `DialFailure` in the pinned
    /// libp2p swarm. Transport failures before reservation simply find no lease.
    pub(crate) fn on_swarm_event(&mut self, event: &FromSwarm<'_>) {
        if matches!(
            event,
            FromSwarm::ConnectionClosed(_)
                | FromSwarm::ListenFailure(_)
                | FromSwarm::DialFailure(_)
        ) {
            if let FromSwarm::ConnectionClosed(event) = event {
                self.leases.remove(&event.connection_id);
            }
            if let FromSwarm::ListenFailure(event) = event {
                self.leases.remove(&event.connection_id);
            }
            if let FromSwarm::DialFailure(event) = event {
                self.leases.remove(&event.connection_id);
            }
            self.budget.iter().for_each(SourceAdmissionBudget::publish_occupancy);
        }
    }
}

impl Drop for SourceConnections {
    fn drop(&mut self) {
        self.leases.clear();
        self.budget.iter().for_each(SourceAdmissionBudget::publish_occupancy);
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;

/// Accept only a direct QUIC endpoint, optionally suffixed by its peer identity.
fn observed_ip(address: &Multiaddr) -> Result<IpAddr, AdmissionError> {
    let mut protocols = address.iter();
    let ip = protocols
        .next()
        .and_then(|protocol| {
            if let Protocol::Ip4(ip) = protocol {
                Some(IpAddr::V4(ip))
            } else if let Protocol::Ip6(ip) = protocol {
                Some(IpAddr::V6(ip))
            } else {
                None
            }
        })
        .ok_or(AdmissionError::UnsupportedAddress)?;
    let quic = matches!(protocols.next(), Some(Protocol::Udp(_)))
        && matches!(protocols.next(), Some(Protocol::QuicV1));
    let suffix = protocols.next().is_none_or(|protocol| matches!(protocol, Protocol::P2p(_)));
    if quic && suffix && protocols.next().is_none() {
        Ok(ip)
    } else {
        Err(AdmissionError::UnsupportedAddress)
    }
}
