//! Epoch-versioned admission, independent of reputation and resource accounting.

use crate::types::NetworkInfo;
use libp2p::PeerId;
use std::collections::{HashMap, HashSet};
use tn_config::{AdmissionConfig, AdmissionMode};
use tn_types::BlsPublicKey;
use tokio::time::Instant;

/// Why the requested Closed policy cannot currently be enforced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionFallback {
    /// No authoritative versioned committee snapshot has arrived.
    Missing,
    /// The snapshot lease expired or an older epoch attempted to replace it.
    Stale,
    /// One epoch supplied different committees, or identity mappings conflict.
    Contradictory,
    /// Committee records or identity mappings are still unresolved.
    Unresolved,
    /// A reload failed validation; the last accepted operator snapshot remains installed.
    OperatorRejected,
}

/// Observations for one swarm. These counts do not imply consensus readiness.
#[derive(Clone, Debug)]
pub struct AdmissionStatus {
    /// Requested rollout mode.
    configured: AdmissionMode,
    /// Policy currently applied to connections.
    effective: AdmissionMode,
    /// Latest accepted authoritative epoch revision.
    epoch: Option<u64>,
    /// Fallback cause, when Closed cannot be enforced.
    fallback: Option<AdmissionFallback>,
    /// Current identities with authenticated records, including the local identity.
    resolved_current: usize,
    /// Resolved-record quorum: n minus floor((n - 1) / 3).
    required_current: usize,
    /// Connected current peers, excluding the local identity.
    connected_current: usize,
}

impl AdmissionStatus {
    /// Return the configured rollout mode.
    pub fn configured(&self) -> AdmissionMode {
        self.configured
    }
    /// Return the policy currently enforced.
    pub fn effective(&self) -> AdmissionMode {
        self.effective
    }
    /// Return the latest accepted epoch revision.
    pub fn epoch(&self) -> Option<u64> {
        self.epoch
    }
    /// Return the fallback cause.
    pub fn fallback(&self) -> Option<AdmissionFallback> {
        self.fallback
    }
    /// Return the authenticated current-record count.
    pub fn resolved_current(&self) -> usize {
        self.resolved_current
    }
    /// Return the minimum resolved-record quorum.
    pub fn required_current(&self) -> usize {
        self.required_current
    }
    /// Return the connected current-peer count.
    pub fn connected_current(&self) -> usize {
        self.connected_current
    }
}

/// Independent configured grants borrowed from one coherent swarm revision.
#[derive(Debug)]
pub(super) struct OperatorBindings<'a> {
    /// Explicit grants owned by their original callers.
    explicit: &'a HashMap<BlsPublicKey, PeerId>,
    /// Grants owned by the current operator configuration.
    configured: &'a HashMap<BlsPublicKey, PeerId>,
}

impl<'a> OperatorBindings<'a> {
    /// Keep ownership separate while supplying both binding maps to admission evaluation.
    pub(super) fn new(
        explicit: &'a HashMap<BlsPublicKey, PeerId>,
        configured: &'a HashMap<BlsPublicKey, PeerId>,
    ) -> Self {
        Self { explicit, configured }
    }
}

/// Immutable committee window at one epoch revision.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitteeSnapshot {
    /// Epoch owning this window.
    epoch: u64,
    /// Previous committee, including late boundary traffic.
    previous: HashSet<BlsPublicKey>,
    /// Current committee.
    current: HashSet<BlsPublicKey>,
    /// Next committee.
    next: HashSet<BlsPublicKey>,
}

/// Authoritative policy and renewal lease. Network input cannot create snapshots.
#[derive(Default, Debug)]
pub(super) struct AdmissionPolicy {
    /// Operator-selected rollout configuration.
    config: AdmissionConfig,
    /// Latest accepted complete committee window.
    snapshot: Option<CommitteeSnapshot>,
    /// Last renewal by the epoch owner.
    renewed: Option<Instant>,
    /// Rejected update, cleared by a consistent authoritative renewal.
    fault: Option<AdmissionFallback>,
    /// Operator input validity is independent of committee renewals and rotations.
    operator_fault: Option<AdmissionFallback>,
}

impl AdmissionPolicy {
    /// Update operator validity without altering the accepted committee window or its lease.
    pub(super) fn set_operator_validity(&mut self, validity: crate::PolicyValidity) {
        self.operator_fault = match validity {
            crate::PolicyValidity::Accepted => None,
            crate::PolicyValidity::Rejected => Some(AdmissionFallback::OperatorRejected),
        };
    }

    /// Configure admission without accepting network-supplied claims.
    pub(super) fn configure(&mut self, config: AdmissionConfig) {
        self.config = config;
    }
    /// Prevent unversioned compatibility updates from enabling Closed.
    pub(super) fn invalidate(&mut self) {
        self.fault = Some(AdmissionFallback::Missing);
    }
    /// Accept increasing revisions or identical renewals. Rejected updates preserve the window.
    pub(super) fn update(
        &mut self,
        epoch: u64,
        previous: HashSet<BlsPublicKey>,
        current: HashSet<BlsPublicKey>,
        next: HashSet<BlsPublicKey>,
    ) -> bool {
        let candidate = CommitteeSnapshot { epoch, previous, current, next };
        let fault = if candidate.current.is_empty() {
            Some(AdmissionFallback::Missing)
        } else {
            self.snapshot.as_ref().and_then(|old| {
                if epoch < old.epoch {
                    Some(AdmissionFallback::Stale)
                } else if epoch == old.epoch && candidate != *old {
                    Some(AdmissionFallback::Contradictory)
                } else {
                    None
                }
            })
        };
        self.fault = fault;
        if fault.is_none() {
            self.snapshot = Some(candidate);
            self.renewed = Some(Instant::now());
            true
        } else {
            false
        }
    }

    /// Resolve grants from verified records and explicit operator identities only.
    /// Incomplete identities use Grace; missing, stale, and conflicting inputs use Open.
    pub(super) fn evaluate(
        &self,
        known: &HashMap<BlsPublicKey, NetworkInfo>,
        stubs: &HashSet<BlsPublicKey>,
        operator: OperatorBindings<'_>,
        local: Option<BlsPublicKey>,
        local_peer: PeerId,
        connected: impl Fn(&PeerId) -> bool,
    ) -> (AdmissionStatus, HashSet<PeerId>) {
        let OperatorBindings { explicit: operator, configured } = operator;
        let conflicting_operator = operator
            .iter()
            .any(|(key, peer)| configured.get(key).is_some_and(|other| other != peer));
        let operator: HashMap<_, _> =
            operator.iter().chain(configured).map(|(key, peer)| (*key, *peer)).collect();
        let resolve = |key: &BlsPublicKey| {
            if Some(*key) == local {
                Some(local_peer)
            } else {
                known
                    .get(key)
                    .map(|info| PeerId::from(info.pubkey.clone()))
                    .or_else(|| operator.get(key).copied())
            }
        };
        let keys: HashSet<_> = self
            .snapshot
            .as_ref()
            .map(|s| s.previous.iter().chain(&s.current).chain(&s.next).copied().collect())
            .unwrap_or_default();
        let identities: HashMap<_, _> = keys
            .iter()
            .chain(operator.keys())
            .filter_map(|key| resolve(key).map(|peer| (*key, peer)))
            .collect();
        let authorized: HashSet<_> = identities.values().copied().collect();
        let unresolved = keys.iter().any(|key| !identities.contains_key(key));
        let contradictory = conflicting_operator
            || authorized.len() != identities.len()
            || operator
                .iter()
                .any(|(key, peer)| resolve(key).is_some_and(|resolved| resolved != *peer));
        let (resolved_current, required_current, connected_current) = self
            .snapshot
            .as_ref()
            .map(|s| {
                let n = s.current.len();
                let resolved = s
                    .current
                    .iter()
                    .filter(|key| {
                        Some(**key) == local || (known.contains_key(key) && !stubs.contains(key))
                    })
                    .count();
                let connected_count = s
                    .current
                    .iter()
                    .filter_map(resolve)
                    .filter(|peer| *peer != local_peer && connected(peer))
                    .count();
                (resolved, n.saturating_sub(n.saturating_sub(1) / 3), connected_count)
            })
            .unwrap_or_default();
        let fallback = self.operator_fault.or(self.fault).or_else(|| match () {
            () if self.snapshot.is_none() => Some(AdmissionFallback::Missing),
            () if self.renewed.is_none_or(|at| at.elapsed() >= self.config.snapshot_max_age()) => {
                Some(AdmissionFallback::Stale)
            }
            () if contradictory => Some(AdmissionFallback::Contradictory),
            () if unresolved || resolved_current < required_current => {
                Some(AdmissionFallback::Unresolved)
            }
            () => None,
        });
        let effective = match self.config.mode() {
            AdmissionMode::Open => AdmissionMode::Open,
            AdmissionMode::Grace => AdmissionMode::Grace,
            AdmissionMode::Closed => {
                fallback.map_or(AdmissionMode::Closed, |reason| match reason {
                    AdmissionFallback::Unresolved | AdmissionFallback::OperatorRejected => {
                        AdmissionMode::Grace
                    }
                    AdmissionFallback::Missing
                    | AdmissionFallback::Stale
                    | AdmissionFallback::Contradictory => AdmissionMode::Open,
                })
            }
        };
        (
            AdmissionStatus {
                configured: self.config.mode(),
                effective,
                epoch: self.snapshot.as_ref().map(|s| s.epoch),
                fallback,
                resolved_current,
                required_current,
                connected_current,
            },
            authorized,
        )
    }
}
