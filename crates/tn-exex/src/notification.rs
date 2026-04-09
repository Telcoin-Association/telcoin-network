// SPDX-License-Identifier: MIT OR Apache-2.0
//! Notifications sent from the execution engine and consensus layer to ExEx tasks.

use reth_chain_state::CanonStateNotification;
use reth_provider::Chain;
use std::sync::Arc;
use tn_types::{Certificate, CommittedSubDag};

/// Notifications about chain state and consensus events sent to ExEx tasks.
///
/// Unlike Reth's ExExNotification which includes reorg variants, TnExExNotification is simplified
/// because Bullshark consensus provides finality - there are no chain reorganizations.
///
/// In addition to execution-layer notifications (`ChainCommitted`), TN ExEx also supports
/// consensus-layer notifications (`CertificateCreated`, `ConsensusCommitted`) that provide
/// visibility into the full lifecycle of data before and after execution.
#[derive(Debug, Clone)]
pub enum TnExExNotification {
    /// A new chain segment has been committed.
    ///
    /// This is emitted after the execution engine has successfully executed and committed
    /// a new chain segment. Contains the full execution results including receipts.
    ChainCommitted {
        /// The newly committed chain segment containing blocks and execution data.
        new: Arc<Chain>,
    },

    /// A certificate was created or received.
    ///
    /// This covers both own certificates (created by this node's certifier after collecting
    /// a quorum of votes) and peer certificates (received and verified from other validators).
    /// ExExes that need to distinguish between own and peer certificates can inspect the
    /// certificate's `origin()` field against the node's authority ID.
    CertificateCreated {
        /// The certificate that was created or received.
        certificate: Arc<Certificate>,
    },

    /// A sub-DAG was committed by consensus.
    ///
    /// This is emitted when the Bullshark consensus protocol commits a leader and its
    /// causal history. The `CommittedSubDag` contains the ordered sequence of certificates
    /// that form this commit, along with the leader certificate and reputation scores.
    ///
    /// This notification arrives *before* execution — use `ChainCommitted` if you need
    /// post-execution data (receipts, state changes).
    ConsensusCommitted {
        /// The committed sub-DAG containing ordered certificates and leader info.
        sub_dag: Arc<CommittedSubDag>,
    },
}

impl TnExExNotification {
    /// Returns the committed chain if this is a `ChainCommitted` notification.
    pub fn committed_chain(&self) -> Option<Arc<Chain>> {
        match self {
            Self::ChainCommitted { new } => Some(Arc::clone(new)),
            _ => None,
        }
    }

    /// Returns the certificate if this is a `CertificateCreated` notification.
    pub fn certificate(&self) -> Option<Arc<Certificate>> {
        match self {
            Self::CertificateCreated { certificate } => Some(Arc::clone(certificate)),
            _ => None,
        }
    }

    /// Returns the committed sub-DAG if this is a `ConsensusCommitted` notification.
    pub fn committed_sub_dag(&self) -> Option<Arc<CommittedSubDag>> {
        match self {
            Self::ConsensusCommitted { sub_dag } => Some(Arc::clone(sub_dag)),
            _ => None,
        }
    }

    /// Try to convert a Reth `CanonStateNotification` into a `TnExExNotification`.
    ///
    /// Returns `Some(ChainCommitted)` for `Commit` notifications.
    /// Returns `None` for `Reorg` notifications — Bullshark consensus provides finality
    /// so reorgs should never occur.  Callers are expected to log the anomaly and skip.
    pub fn try_from_canon_state(notification: CanonStateNotification) -> Option<Self> {
        match notification {
            CanonStateNotification::Commit { new } => Some(Self::ChainCommitted { new }),
            CanonStateNotification::Reorg { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_committed_chain_accessor() {
        let chain = Arc::new(Chain::default());
        let notification = TnExExNotification::ChainCommitted { new: Arc::clone(&chain) };

        let result = notification.committed_chain();
        assert!(result.is_some());
        assert_eq!(Arc::as_ptr(&result.unwrap()), Arc::as_ptr(&chain));
    }

    #[test]
    fn test_certificate_accessor() {
        let cert = Arc::new(Certificate::default());
        let notification =
            TnExExNotification::CertificateCreated { certificate: Arc::clone(&cert) };

        assert!(notification.committed_chain().is_none());
        assert!(notification.committed_sub_dag().is_none());

        let result = notification.certificate();
        assert!(result.is_some());
        assert_eq!(Arc::as_ptr(&result.unwrap()), Arc::as_ptr(&cert));
    }

    #[test]
    fn test_committed_sub_dag_accessor() {
        let sub_dag = Arc::new(CommittedSubDag::default());
        let notification =
            TnExExNotification::ConsensusCommitted { sub_dag: Arc::clone(&sub_dag) };

        assert!(notification.committed_chain().is_none());
        assert!(notification.certificate().is_none());

        let result = notification.committed_sub_dag();
        assert!(result.is_some());
        assert_eq!(Arc::as_ptr(&result.unwrap()), Arc::as_ptr(&sub_dag));
    }

    #[test]
    fn test_chain_committed_returns_none_for_other_accessors() {
        let chain = Arc::new(Chain::default());
        let notification = TnExExNotification::ChainCommitted { new: chain };

        assert!(notification.certificate().is_none());
        assert!(notification.committed_sub_dag().is_none());
    }

    #[test]
    fn test_try_from_canon_state_commit() {
        let chain = Arc::new(Chain::default());
        let canon_notification = CanonStateNotification::Commit { new: Arc::clone(&chain) };

        let tn_notification = TnExExNotification::try_from_canon_state(canon_notification);
        assert!(tn_notification.is_some());

        match tn_notification.unwrap() {
            TnExExNotification::ChainCommitted { new } => {
                assert_eq!(Arc::as_ptr(&new), Arc::as_ptr(&chain));
            }
            _ => panic!("expected ChainCommitted"),
        }
    }

    #[test]
    fn test_try_from_canon_state_reorg_returns_none() {
        let old = Arc::new(Chain::default());
        let new = Arc::new(Chain::default());
        let canon_notification = CanonStateNotification::Reorg { old, new };

        // Reorg should return None, not panic
        let result = TnExExNotification::try_from_canon_state(canon_notification);
        assert!(result.is_none(), "Reorg notification should be safely skipped");
    }
}
