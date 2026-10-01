//! Process-lifetime operator connections and DAO capacity reservations.

use super::PeerManager;
use crate::{
    consensus::MAX_ESTABLISHED_CONNECTIONS_PER_PEER,
    types::{NetworkInfo, NetworkResult, NetworkType},
};
use libp2p::PeerId;
use std::{collections::BTreeMap, time::Duration};
use tn_config::NetworkConfig;
use tn_types::{now, BlsPublicKey};
use tokio::time::Instant;

/// One bounded retry schedule, owned by the swarm rather than an epoch task.
pub(super) struct OperatorRetry {
    /// Authenticated transport binding and operator-provisioned reconnect address.
    info: NetworkInfo,
    /// Earliest time for the next attempt.
    next_attempt: Instant,
    /// Delay used after the next attempted dial, capped at one minute.
    backoff: Duration,
}

impl PeerManager {
    /// Keep both directions of a configured BLS/transport binding immutable until restart.
    pub(super) fn configured_binding_conflicts(
        &self,
        key: &BlsPublicKey,
        info: &NetworkInfo,
    ) -> bool {
        self.operator_retries.iter().any(|(configured, retry)| {
            if configured == key {
                retry.info.pubkey != info.pubkey
            } else {
                retry.info.pubkey == info.pubkey
            }
        })
    }

    /// Install the validated union without granting committee or publisher authority.
    pub(crate) fn configure_operator_peers(
        &mut self,
        config: &NetworkConfig,
        network: NetworkType,
    ) -> NetworkResult<()> {
        let observers = config.dao_observers().map(|profile| profile.observers());
        let entries: BTreeMap<_, _> = config
            .trusted_nodes()
            .iter()
            .chain(observers.into_iter().flat_map(|entries| entries.iter()))
            .map(|(key, node)| (*key, node))
            .collect();
        entries.into_iter().try_for_each(|(key, node)| -> NetworkResult<()> {
            let remote = match network {
                NetworkType::Primary => Some(node.primary()),
                NetworkType::Worker(id) => node.worker(id),
            }
            .ok_or(crate::error::NetworkError::PeerMissing)?;
            let peer: PeerId = remote.network_key.clone().into();
            if !self.is_local_peer(&peer) {
                self.peers.add_trusted_peer(key, remote.network_key.clone());
                let info = NetworkInfo {
                    pubkey: remote.network_key.clone(),
                    multiaddrs: vec![remote.network_address.clone()],
                    timestamp: now(),
                    rpc: remote.rpc.clone(),
                };
                self.add_known_peer(key, info.clone());
                self.operator_retries.insert(
                    key,
                    OperatorRetry {
                        info,
                        next_attempt: Instant::now(),
                        backoff: Duration::from_secs(1),
                    },
                );
                if observers.is_some_and(|entries| entries.contains_key(&key)) {
                    self.dao_peer_ids.insert(peer);
                }
            }
            Ok(())
        })?;
        self.dao_connection_budget = config.dao_observers().map(|profile| {
            profile.max_peers().saturating_mul(MAX_ESTABLISHED_CONNECTIONS_PER_PEER)
        });
        self.retry_operator_peers();
        Ok(())
    }

    /// Retry every configured peer even while ordinary connection targets are satisfied.
    /// A swarm owns at most one schedule and one in-flight dial per configured identity.
    pub(super) fn retry_operator_peers(&mut self) {
        let now = Instant::now();
        let ready: Vec<_> = self
            .operator_retries
            .iter()
            .filter(|(_, retry)| {
                retry.next_attempt <= now && self.can_dial(&retry.info.pubkey.clone().into())
            })
            .map(|(key, retry)| (*key, retry.info.clone()))
            .collect();
        ready.into_iter().for_each(|(key, info)| {
            self.operator_retries.get_mut(&key).into_iter().for_each(|retry| {
                retry.next_attempt = now + retry.backoff;
                retry.backoff = retry.backoff.saturating_mul(2).min(Duration::from_secs(60));
            });
            self.dial_peer(info.pubkey.into(), info.multiaddrs, None);
        });
    }

    /// Preserve reserved allowances even when unrelated privileged connections fill their budget.
    pub(super) fn dao_capacity_available(&self, peer: &PeerId) -> bool {
        self.dao_connection_budget.is_none_or(|total| {
            let per_peer =
                usize::try_from(MAX_ESTABLISHED_CONNECTIONS_PER_PEER).unwrap_or(usize::MAX);
            let total = usize::try_from(total).unwrap_or(usize::MAX);
            let is_observer = self.dao_peer_ids.contains(peer);
            let used = self
                .connection_admissions
                .values()
                .filter(|admission| {
                    let id = admission.peer_id();
                    if is_observer {
                        id == *peer
                    } else {
                        !self.dao_peer_ids.contains(&id)
                    }
                })
                .count();
            let limit = if is_observer {
                per_peer
            } else {
                total.saturating_sub(self.dao_peer_ids.len().saturating_mul(per_peer))
            };
            let ordinary_population: std::collections::HashSet<_> = self
                .connection_admissions
                .values()
                .map(|admission| admission.peer_id())
                .filter(|id| !self.dao_peer_ids.contains(id))
                .collect();
            let population_limit = (total / per_peer).saturating_sub(self.dao_peer_ids.len());
            used < limit
                && (is_observer
                    || ordinary_population.contains(peer)
                    || ordinary_population.len() < population_limit)
        })
    }
}
