//! ExEx notification types for the full transaction lifecycle.
//!
//! TN's ExEx system covers the entire transaction lifecycle:
//! certificate accepted → consensus committed → chain executed.
//!
//! TN uses Bullshark BFT consensus with immediate finality, so there are no
//! reorgs — all committed state is immediately final.

use std::sync::Arc;
use tn_types::{Certificate, CommittedSubDag};

/// Re-export the Chain type from reth.
pub use reth_execution_types::Chain;

/// Notification sent to ExExes covering the full transaction lifecycle.
///
/// TN's ExEx system tracks transactions through three stages:
/// 1. **Certificate accepted** — a header has been certified (own or peer)
/// 2. **Consensus committed** — a sub-DAG has been committed by Bullshark
/// 3. **Chain executed** — blocks have been executed and added to the canonical chain
///
/// All stages produce final, irrevocable state thanks to BFT consensus.
///
/// # Delivery contract
///
/// Live delivery is **best-effort**: the manager fans out with a non-blocking
/// `try_send` so it can never block consensus or execution. When an ExEx cannot
/// keep up, notifications are dropped and the gap is surfaced as [`Lagged`] — at
/// which point the ExEx should reconcile via [`replay`](crate::replay). Replay
/// is the **authoritative** path; treat live notifications as at-least-once.
///
/// [`Lagged`]: TnExExNotification::Lagged
#[derive(Debug, Clone)]
pub enum TnExExNotification {
    /// A certificate has been accepted (either our own or a peer's).
    ///
    /// This is the earliest signal that a header's batches are included
    /// in the consensus DAG.
    CertificateAccepted {
        /// The accepted certificate.
        certificate: Box<Certificate>,
        /// Whether this certificate was produced by the local node.
        is_own: bool,
    },
    /// A sub-DAG has been committed by the Bullshark consensus protocol.
    ///
    /// This means the certificates in the sub-DAG are ordered and will
    /// be executed. The committed sub-DAG includes the leader certificate
    /// and all certificates it commits.
    ConsensusCommitted {
        /// The committed sub-DAG containing ordered certificates.
        sub_dag: Arc<CommittedSubDag>,
    },
    /// New blocks have been executed and added to the canonical chain.
    ///
    /// This is the final stage — the transactions have been executed by the
    /// EVM and the resulting state changes are finalized. TN's BFT consensus
    /// guarantees immediate finality with no reorgs.
    ChainExecuted {
        /// The new chain segment containing blocks, receipts, and state changes.
        new: Arc<Chain>,
    },
    /// The ExEx fell behind and live notifications were dropped.
    ///
    /// Emitted when the manager could not deliver one or more notifications
    /// because this ExEx's channel was full (the manager never blocks consensus,
    /// so it drops instead). On receipt, a stateful ExEx should reconcile by
    /// replaying from its last processed height — see
    /// [`TnExExContext::replay_from`](crate::TnExExContext::replay_from).
    ///
    /// `missed` is a best-effort count of notifications dropped since the last
    /// successful delivery; treat it as a "gap exists, go reconcile" signal
    /// rather than an exact tally.
    Lagged {
        /// Best-effort count of notifications dropped since the last delivery.
        missed: u64,
    },
}
