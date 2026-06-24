//! ExEx notification types for the full transaction lifecycle.
//!
//! These notifications are produced by the **consensus-following** (observer)
//! paths — never by the validator hot path. A follower learns about consensus in
//! three stages, and each maps to a variant:
//!
//! 1. a gossip-verified certificate is accepted
//!    ([`CertificateAccepted`](TnExExNotification::CertificateAccepted)),
//! 2. a verified consensus header is reconstructed into a full
//!    [`ConsensusOutput`](TnExExNotification::ConsensusOutput) for execution,
//! 3. the resulting blocks are executed and extend the canonical chain
//!    ([`ChainExecuted`](TnExExNotification::ChainExecuted)).
//!
//! TN uses Bullshark BFT consensus with immediate finality, so there are no
//! reorgs — all executed state is immediately final.
//!
//! # Delivery contract
//!
//! Live delivery is **best-effort**: the manager fans out with a non-blocking
//! `try_send`, so it can never block the consensus-following path. When an ExEx
//! cannot keep up, notifications are dropped and the gap is surfaced as
//! [`Lagged`](TnExExNotification::Lagged).
//!
//! Only the [`ChainExecuted`](TnExExNotification::ChainExecuted) stream is
//! recoverable from a gap — [`replay`](crate::replay) re-derives executed blocks
//! from the DB, so it is the **authoritative** catch-up path. The earlier
//! [`CertificateAccepted`](TnExExNotification::CertificateAccepted) and
//! [`ConsensusOutput`](TnExExNotification::ConsensusOutput) signals are
//! best-effort and are not reproduced by replay (see
//! [`Lagged`](TnExExNotification::Lagged)).

use std::sync::Arc;
use tn_types::{Certificate, ConsensusOutput};

/// Re-export the Chain type from reth.
pub use reth_execution_types::Chain;

/// Notification sent to ExExes covering the full transaction lifecycle.
///
/// All variants are produced by the consensus-following (observer) paths, never
/// by the validator hot path. They track consensus through three stages:
/// 1. **Certificate accepted** — a gossip-verified header is certified
/// 2. **Consensus output** — a verified consensus header is reconstructed into a full output
/// 3. **Chain executed** — blocks have been executed and added to the canonical chain
///
/// All stages produce final, irrevocable state thanks to BFT consensus.
///
/// # Delivery contract
///
/// Live delivery is **best-effort**: the manager fans out with a non-blocking
/// `try_send` so it can never block the consensus-following path. When an ExEx
/// cannot keep up, notifications are dropped and the gap is surfaced as
/// [`Lagged`]. Only [`ChainExecuted`] is recoverable from a gap, via
/// [`replay`](crate::replay) — see [`Lagged`] for what replay can and cannot
/// reconstruct.
///
/// [`Lagged`]: TnExExNotification::Lagged
/// [`ChainExecuted`]: TnExExNotification::ChainExecuted
#[derive(Debug, Clone)]
pub enum TnExExNotification {
    /// A gossip-verified certificate has been accepted on the consensus-following
    /// path.
    ///
    /// This is the earliest lifecycle signal: a peer's certificate verified
    /// against its committee, so its batches are now part of the consensus DAG.
    /// Followers have no certificates of their own, so there is no own/peer
    /// distinction. Best-effort and not recoverable via replay — see [`Lagged`].
    ///
    /// [`Lagged`]: TnExExNotification::Lagged
    CertificateAccepted {
        /// The accepted certificate.
        certificate: Box<Certificate>,
    },
    /// A full consensus output has been reconstructed for execution.
    ///
    /// On the consensus-following path a verified consensus header is turned into
    /// a complete [`ConsensusOutput`](tn_types::ConsensusOutput) — the committed
    /// sub-DAG plus its batches — and handed to the engine. This is the
    /// pre-execution signal that ordered transactions are about to run.
    /// Best-effort and not recoverable via replay — see [`Lagged`].
    ///
    /// [`Lagged`]: TnExExNotification::Lagged
    ConsensusOutput {
        /// The full consensus output (committed sub-DAG, batches, and header).
        output: ConsensusOutput,
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
    /// Emitted in two cases, both surfacing the same "a gap exists" signal:
    /// 1. **Downstream (manager → ExEx):** this ExEx's bounded channel was full, so the manager
    ///    dropped notifications rather than block.
    /// 2. **Upstream (source → manager):** the manager itself fell behind one of its source streams
    ///    (reth's canonical-state stream or a consensus broadcast), which drop lagged items
    ///    silently. The manager detects the resulting discontinuity and re-emits it as `Lagged`.
    ///
    /// # What replay can and cannot recover
    ///
    /// Only the [`ChainExecuted`] stream is recoverable: replaying from the last
    /// processed height re-derives every executed block from the DB (see
    /// [`TnExExContext::replay_from`](crate::TnExExContext::replay_from)), so the
    /// *executed chain* — the authoritative record — is never lost to a gap.
    ///
    /// Missed [`CertificateAccepted`] and [`ConsensusOutput`] notifications are
    /// **not** recoverable: they are best-effort, pre-execution signals and
    /// replay does not reproduce them. An ExEx that needs a gap-free history must
    /// derive it from `ChainExecuted` (plus replay), not from these early signals.
    ///
    /// `missed` is a best-effort count of notifications dropped since the last
    /// successful delivery; treat it as "a gap exists" rather than an exact tally.
    ///
    /// [`ChainExecuted`]: TnExExNotification::ChainExecuted
    /// [`CertificateAccepted`]: TnExExNotification::CertificateAccepted
    /// [`ConsensusOutput`]: TnExExNotification::ConsensusOutput
    Lagged {
        /// Best-effort count of notifications dropped since the last delivery.
        missed: u64,
    },
}
