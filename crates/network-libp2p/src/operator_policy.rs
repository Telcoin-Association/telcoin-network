//! Process-wide operator policy publication without per-swarm command transactions.

use std::sync::Arc;
use tn_config::OperatorPeerPolicy;

/// Monotonic reload attempt identity; rejected attempts never replace the accepted revision.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct PolicyRevision(u64);

impl PolicyRevision {
    /// Obtain the next attempt, refusing to wrap the revision space.
    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }

    /// Return the numeric identity for key-free logs and gauges.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

/// Whether the most recent operator input validated successfully.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyValidity {
    /// This attempt published a complete validated snapshot.
    Accepted,
    /// This attempt retained the accepted snapshot and requires admission fallback.
    Rejected,
}

/// One immutable publication observed by every primary and worker consumer.
#[derive(Clone, Debug)]
pub struct PeerPolicyUpdate {
    /// Most recent reload attempt, including rejected input.
    attempt: PolicyRevision,
    /// Revision owning the retained, validated snapshot.
    accepted: PolicyRevision,
    /// Endpoints shared without copying the complete fleet configuration per consumer.
    policy: Arc<OperatorPeerPolicy>,
    /// Input validity, independent of committee snapshot validity.
    validity: PolicyValidity,
}

impl PeerPolicyUpdate {
    /// Publish a fully validated policy under one revision.
    pub fn accepted(revision: PolicyRevision, policy: OperatorPeerPolicy) -> Self {
        Self {
            attempt: revision,
            accepted: revision,
            policy: Arc::new(policy),
            validity: PolicyValidity::Accepted,
        }
    }

    /// Retain exactly the accepted snapshot while recording a rejected attempt.
    pub fn rejected(&self, attempt: PolicyRevision) -> Self {
        Self { attempt, validity: PolicyValidity::Rejected, ..self.clone() }
    }

    /// Return the latest reload attempt.
    pub fn attempt(&self) -> PolicyRevision {
        self.attempt
    }

    /// Return the revision currently supplying connectivity hints.
    pub fn revision(&self) -> PolicyRevision {
        self.accepted
    }

    /// Return the immutable accepted policy.
    pub fn policy(&self) -> &OperatorPeerPolicy {
        &self.policy
    }

    /// Return the independent operator-input validity.
    pub fn validity(&self) -> PolicyValidity {
        self.validity
    }
}
