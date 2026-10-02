//! Reload ownership, all-swarm replacement, admission fallback and bounded retry regressions.

use super::*;
use crate::{
    common::create_multiaddr, types::NetworkType, AdmissionFallback, PeerPolicyUpdate,
    PolicyRevision,
};
use libp2p::{
    core::{transport::PortUse, Endpoint},
    swarm::{ConnectionId, NetworkBehaviour as _},
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::collections::BTreeMap;
use tn_config::{NetworkConfig, TrustedNode};
use tn_types::{BlsKeypair, BootstrapServer, NetworkKeypair, P2pNode};

/// Produce an identity with an independent authenticated transport key.
fn policy_endpoint(seed: u8) -> (BlsPublicKey, P2pNode) {
    let bls = *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public();
    (
        bls,
        P2pNode {
            network_key: NetworkKeypair::generate_ed25519().public().into(),
            network_address: create_multiaddr(None),
            rpc: None,
        },
    )
}

/// Build a production-validated snapshot with explicit worker identities.
fn publication(
    revision: PolicyRevision,
    trusted: BTreeMap<BlsPublicKey, TrustedNode>,
    bootstrap: BTreeMap<BlsPublicKey, BootstrapServer>,
    workers: usize,
) -> eyre::Result<PeerPolicyUpdate> {
    let mut value = serde_yaml::to_value(NetworkConfig::default())?;
    value
        .as_mapping_mut()
        .ok_or_else(|| eyre::eyre!("config must serialize as a mapping"))?
        .insert(serde_yaml::Value::from("trusted_nodes"), serde_yaml::to_value(trusted)?);
    let config: NetworkConfig = serde_yaml::from_value(value)?;
    Ok(PeerPolicyUpdate::accepted(
        revision,
        config.operator_peer_policy(&bootstrap, None, workers.into(), 32usize.into())?,
    ))
}

/// Create a Closed manager with a fully resolved local-only committee.
fn manager(network: NetworkType) -> PeerManager {
    let (local, _) = policy_endpoint(1);
    let mut manager = PeerManager::new(
        PeerId::random(),
        &PeerConfig::default(),
        PeerManagerMetrics::new_for(&network),
    );
    manager.configure_admission(
        AdmissionConfig::new(AdmissionMode::Closed, Duration::from_secs(300)),
        local,
    );
    manager.update_committees_at(7, HashSet::new(), HashSet::from([local]), HashSet::new());
    manager
}

/// Verify all connection entry points against the same latest policy.
fn hooks(manager: &mut PeerManager, peer: PeerId, permitted: bool) {
    let addr = create_multiaddr(None);
    assert_eq!(
        manager
            .handle_established_inbound_connection(
                ConnectionId::new_unchecked(70),
                peer,
                &addr,
                &addr
            )
            .is_ok(),
        permitted
    );
    assert_eq!(
        manager
            .handle_pending_outbound_connection(
                ConnectionId::new_unchecked(71),
                Some(peer),
                &[addr.clone()],
                Endpoint::Dialer
            )
            .is_ok(),
        permitted
    );
    assert_eq!(
        manager
            .handle_established_outbound_connection(
                ConnectionId::new_unchecked(72),
                peer,
                &addr,
                Endpoint::Dialer,
                PortUse::New
            )
            .is_ok(),
        permitted
    );
}

/// A hub replacement reaches the primary and two distinct workers and revokes obsolete grants.
#[tokio::test]
async fn peer_policy_replacement_every_swarm() -> eyre::Result<()> {
    let (old_key, old_primary) = policy_endpoint(10);
    let (_, old_zero) = policy_endpoint(11);
    let (_, old_one) = policy_endpoint(12);
    let (new_key, new_primary) = policy_endpoint(20);
    let (_, new_zero) = policy_endpoint(21);
    let (_, new_one) = policy_endpoint(22);
    let first_revision = PolicyRevision::default().next().ok_or_else(|| eyre::eyre!("revision"))?;
    let old = publication(
        first_revision,
        BTreeMap::from([(
            old_key,
            TrustedNode::new(
                old_primary.clone(),
                BTreeMap::from([(0, old_zero.clone()), (1, old_one.clone())]),
            ),
        )]),
        BTreeMap::new(),
        2,
    )?;
    let new = publication(
        first_revision.next().ok_or_else(|| eyre::eyre!("revision"))?,
        BTreeMap::from([(
            new_key,
            TrustedNode::new(
                new_primary.clone(),
                BTreeMap::from([(0, new_zero.clone()), (1, new_one.clone())]),
            ),
        )]),
        BTreeMap::new(),
        2,
    )?;
    [
        (
            NetworkType::Primary,
            old.policy().primary(),
            new.policy().primary(),
            old_primary,
            new_primary,
        ),
        (
            NetworkType::Worker(0),
            old.policy().worker(0).ok_or_else(|| eyre::eyre!("worker 0"))?,
            new.policy().worker(0).ok_or_else(|| eyre::eyre!("worker 0"))?,
            old_zero,
            new_zero,
        ),
        (
            NetworkType::Worker(1),
            old.policy().worker(1).ok_or_else(|| eyre::eyre!("worker 1"))?,
            new.policy().worker(1).ok_or_else(|| eyre::eyre!("worker 1"))?,
            old_one,
            new_one,
        ),
    ]
    .into_iter()
    .try_for_each(|(role, before, after, old_endpoint, new_endpoint)| -> eyre::Result<()> {
        let mut manager = manager(role);
        let old_id = PeerId::from(old_endpoint.network_key);
        let new_id = PeerId::from(new_endpoint.network_key);
        assert!(manager.replace_operator_policy(&old, before));
        hooks(&mut manager, old_id, true);
        manager.peers.update_connection_status(
            &old_id,
            NewConnectionStatus::Connected {
                multiaddr: create_multiaddr(None),
                direction: ConnectionDirection::Incoming,
            },
        );
        assert!(manager.replace_operator_policy(&new, after));
        assert_eq!(manager.policy_admission, HashMap::from([(new_key, new_id)]));
        assert!(!manager.peer_policy(&old_id).protects_retention());
        assert!(manager.peer_policy(&new_id).protects_retention());
        assert!(!manager.peer_policy(&old_id).exempts_load());
        assert!(manager
            .events
            .iter()
            .any(|event| matches!(event, PeerEvent::DisconnectPeer(id) if *id == old_id)));
        assert!(manager.dial_requests.iter().all(|request| request.peer_id != old_id));
        assert!(manager.dial_requests.iter().any(|request| request.peer_id == new_id));
        hooks(&mut manager, old_id, false);
        hooks(&mut manager, new_id, true);
        Ok(())
    })
}

/// Removing policy trust preserves independent committee and explicit bootstrap ownership.
#[tokio::test]
async fn peer_policy_removal_preserves_other_owners() -> eyre::Result<()> {
    let (key, endpoint) = policy_endpoint(30);
    let id = PeerId::from(endpoint.network_key.clone());
    let revision = PolicyRevision::default().next().ok_or_else(|| eyre::eyre!("revision"))?;
    let initial = publication(
        revision,
        BTreeMap::from([(key, TrustedNode::new(endpoint.clone(), BTreeMap::new()))]),
        BTreeMap::new(),
        0,
    )?;
    let removed = publication(
        revision.next().ok_or_else(|| eyre::eyre!("revision"))?,
        BTreeMap::new(),
        BTreeMap::new(),
        0,
    )?;
    let mut manager = manager(NetworkType::Primary);
    let local = manager.local_bls_key.ok_or_else(|| eyre::eyre!("local identity"))?;
    let info = NetworkInfo {
        pubkey: endpoint.network_key,
        multiaddrs: vec![endpoint.network_address],
        timestamp: tn_types::now(),
        rpc: None,
    };
    manager.add_bootstrap_peer(key, info.clone());
    manager.cache_known_peer(key, info);
    manager.update_committees_at(8, HashSet::new(), HashSet::from([local, key]), HashSet::new());
    manager.replace_operator_policy(&initial, initial.policy().primary());
    assert_eq!(manager.policy_admission.get(&key), Some(&id));
    manager.replace_operator_policy(&removed, removed.policy().primary());
    assert!(manager.policy_admission.is_empty());
    assert!(manager.policy_dials.is_empty());
    assert!(manager.pinned_peers.contains(&key));
    assert!(manager.known_peers.contains_key(&key));
    assert!(manager.peer_policy(&id).protects_retention());
    assert!(manager.peer_policy(&id).exempts_load());
    hooks(&mut manager, id, true);
    manager.update_committees_at(9, HashSet::new(), HashSet::from([local]), HashSet::new());
    assert!(!manager.peer_policy(&id).protects_retention());
    assert!(!manager.peer_policy(&id).exempts_load());
    assert!(manager.known_peers.contains_key(&key));
    hooks(&mut manager, id, true);
    Ok(())
}

/// Rejection retains accepted endpoints and cannot be cleared by a concurrent committee renewal.
#[tokio::test]
async fn peer_policy_rejection_fallback_and_recovery() -> eyre::Result<()> {
    let (key, endpoint) = policy_endpoint(40);
    let id = PeerId::from(endpoint.network_key.clone());
    let revision = PolicyRevision::default().next().ok_or_else(|| eyre::eyre!("revision"))?;
    let accepted = publication(
        revision,
        BTreeMap::from([(key, TrustedNode::new(endpoint, BTreeMap::new()))]),
        BTreeMap::new(),
        0,
    )?;
    let rejected_revision = revision.next().ok_or_else(|| eyre::eyre!("revision"))?;
    let rejected = accepted.rejected(rejected_revision);
    let recovered = publication(
        rejected_revision.next().ok_or_else(|| eyre::eyre!("revision"))?,
        BTreeMap::new(),
        BTreeMap::new(),
        0,
    )?;
    let mut manager = manager(NetworkType::Primary);
    manager.replace_operator_policy(&accepted, accepted.policy().primary());
    assert_eq!(manager.admission_status().effective(), AdmissionMode::Closed);
    manager.replace_operator_policy(&rejected, rejected.policy().primary());
    assert_eq!(manager.policy_admission, HashMap::from([(key, id)]));
    assert_eq!(manager.admission_status().effective(), AdmissionMode::Grace);
    assert_eq!(manager.admission_status().fallback(), Some(AdmissionFallback::OperatorRejected));
    let local = manager.local_bls_key.ok_or_else(|| eyre::eyre!("local identity"))?;
    manager.update_committees_at(8, HashSet::new(), HashSet::from([local]), HashSet::new());
    assert_eq!(manager.admission_status().epoch(), Some(8));
    assert_eq!(manager.admission_status().effective(), AdmissionMode::Grace);
    hooks(&mut manager, PeerId::random(), true);
    assert!(!manager.replace_operator_policy(&accepted, accepted.policy().primary()));
    manager.replace_operator_policy(&recovered, recovered.policy().primary());
    assert_eq!(manager.admission_status().effective(), AdmissionMode::Closed);
    hooks(&mut manager, id, false);
    Ok(())
}

/// Repeated reloads preserve backoff, bound work, release owned pins and retain protocol bans.
#[tokio::test(start_paused = true)]
async fn peer_policy_repeated_reload_bounded_and_bans_survive() -> eyre::Result<()> {
    let (key, endpoint) = policy_endpoint(50);
    let id = PeerId::from(endpoint.network_key.clone());
    let mut revision = PolicyRevision::default().next().ok_or_else(|| eyre::eyre!("revision"))?;
    let trusted = BTreeMap::from([(key, TrustedNode::new(endpoint.clone(), BTreeMap::new()))]);
    let initial = publication(revision, trusted.clone(), BTreeMap::new(), 0)?;
    let mut manager = manager(NetworkType::Primary);
    manager.replace_operator_policy(&initial, initial.policy().primary());
    let next_attempt =
        manager.policy_dials.get(&key).ok_or_else(|| eyre::eyre!("scheduled hub"))?.next_attempt;
    (0..64).try_for_each(|_| -> eyre::Result<()> {
        revision = revision.next().ok_or_else(|| eyre::eyre!("revision"))?;
        let update = publication(revision, trusted.clone(), BTreeMap::new(), 0)?;
        manager.replace_operator_policy(&update, update.policy().primary());
        assert_eq!(manager.policy_dials.len(), 1);
        assert_eq!(manager.policy_known.len(), 1);
        assert_eq!(manager.policy_admission.len(), 1);
        assert_eq!(manager.dial_requests.len(), 1);
        assert_eq!(
            manager.policy_dials.get(&key).map(|dial| dial.next_attempt),
            Some(next_attempt)
        );
        assert!(manager.trusted_dials.is_empty());
        Ok(())
    })?;
    manager.peers.update_connection_status(
        &id,
        NewConnectionStatus::Connected {
            multiaddr: endpoint.network_address.clone(),
            direction: ConnectionDirection::Incoming,
        },
    );
    assert!(manager.peer_to_bls(&id).is_none());
    manager.process_penalty(id, Penalty::Fatal);
    manager.known_peers.insert(
        key,
        NetworkInfo {
            pubkey: endpoint.network_key.clone(),
            multiaddrs: vec![endpoint.network_address.clone()],
            timestamp: tn_types::now(),
            rpc: None,
        },
    );
    let (_, replacement) = policy_endpoint(51);
    let replacement_id = PeerId::from(replacement.network_key.clone());
    revision = revision.next().ok_or_else(|| eyre::eyre!("revision"))?;
    let rotated = publication(
        revision,
        BTreeMap::from([(key, TrustedNode::new(replacement.clone(), BTreeMap::new()))]),
        BTreeMap::new(),
        0,
    )?;
    manager.replace_operator_policy(&rotated, rotated.policy().primary());
    assert_eq!(
        manager.auth_to_peer(key),
        Some((replacement_id, vec![replacement.network_address]))
    );
    assert!(manager.peer_banned(&replacement_id));
    assert!(manager.dial_requests.iter().all(|request| request.peer_id != replacement_id));
    hooks(&mut manager, replacement_id, false);
    revision = revision.next().ok_or_else(|| eyre::eyre!("revision"))?;
    let removed = publication(revision, BTreeMap::new(), BTreeMap::new(), 0)?;
    manager.replace_operator_policy(&removed, removed.policy().primary());
    assert!(manager.policy_dials.is_empty());
    assert!(manager.policy_known.is_empty());
    assert!(manager.policy_admission.is_empty());
    assert!(manager.trusted_retry.is_none());
    assert!(manager.peer_banned(&id));
    Ok(())
}
