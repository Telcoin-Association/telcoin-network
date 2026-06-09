//! Minimal trait for ExEx notification handles.
//!
//! This trait defines the interface that tn-reth needs from an ExEx manager handle,
//! avoiding circular dependencies with the full ExEx implementation.

use reth_chain_state::CanonStateNotification;

/// Minimal interface for sending notifications to ExEx tasks.
///
/// This trait is implemented by the actual ExEx manager handle in the `tn-exex` crate.
pub trait ExExNotificationHandle: Send + Sync + Clone {
    /// Returns whether any ExExes are registered.
    fn has_exexs(&self) -> bool;

    /// Sends a canonical state notification to all registered ExExes.
    fn send_canon_notification(&self, notification: CanonStateNotification);
}

/// Empty handle for tests when no ExExes are installed.
#[derive(Clone, Debug)]
pub struct EmptyExExHandle;

impl ExExNotificationHandle for EmptyExExHandle {
    fn has_exexs(&self) -> bool {
        false
    }

    fn send_canon_notification(&self, _notification: CanonStateNotification) {
        // No-op
    }
}
