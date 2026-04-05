// SPDX-License-Identifier: MIT OR Apache-2.0
//! Notifications sent from the execution engine to ExEx tasks.

use reth_chain_state::CanonStateNotification;
use reth_provider::Chain;
use std::sync::Arc;

/// Notifications about committed chain state sent to ExEx tasks.
///
/// Unlike Reth's ExExNotification which includes reorg variants, TnExExNotification is simplified
/// because Bullshark consensus provides finality - there are no chain reorganizations.
#[derive(Debug, Clone)]
pub enum TnExExNotification {
    /// A new chain segment has been committed.
    ///
    /// This is the primary notification type and is emitted after the execution engine
    /// has successfully executed and committed a new chain segment.
    ChainCommitted {
        /// The newly committed chain segment containing blocks and execution data.
        new: Arc<Chain>,
    },
}

impl TnExExNotification {
    /// Returns the committed chain if this is a `ChainCommitted` notification.
    ///
    /// # Returns
    ///
    /// - `Some(Arc<Chain>)` for `ChainCommitted` variant
    /// - `None` for any future variants
    pub fn committed_chain(&self) -> Option<Arc<Chain>> {
        match self {
            Self::ChainCommitted { new } => Some(Arc::clone(new)),
        }
    }
}

impl From<CanonStateNotification> for TnExExNotification {
    /// Convert a Reth `CanonStateNotification` to `TnExExNotification`.
    ///
    /// This conversion only handles the `Commit` variant since TN consensus guarantees
    /// finality and does not produce reorgs.
    ///
    /// # Panics
    ///
    /// This conversion will panic if given a non-Commit notification, as reorgs should
    /// never occur in TN's Bullshark consensus.
    fn from(notification: CanonStateNotification) -> Self {
        match notification {
            CanonStateNotification::Commit { new } => Self::ChainCommitted { new },
            CanonStateNotification::Reorg { .. } => {
                panic!("TN ExEx received unexpected reorg notification - Bullshark consensus should never reorg")
            }
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
    fn test_from_canon_state_commit() {
        let chain = Arc::new(Chain::default());
        let canon_notification = CanonStateNotification::Commit { new: Arc::clone(&chain) };

        let tn_notification: TnExExNotification = canon_notification.into();

        match tn_notification {
            TnExExNotification::ChainCommitted { new } => {
                assert_eq!(Arc::as_ptr(&new), Arc::as_ptr(&chain));
            }
        }
    }

    #[test]
    #[should_panic(
        expected = "TN ExEx received unexpected reorg notification - Bullshark consensus should never reorg"
    )]
    fn test_from_canon_state_reorg_panics() {
        let old = Arc::new(Chain::default());
        let new = Arc::new(Chain::default());
        let canon_notification = CanonStateNotification::Reorg { old, new };

        // This should panic
        let _: TnExExNotification = canon_notification.into();
    }
}
