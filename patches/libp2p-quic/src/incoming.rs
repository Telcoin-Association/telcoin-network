//! Aggregate counters for the decisions a listener makes on incoming
//! connection attempts (vendored addition, see PATCH.md).

use std::sync::atomic::{AtomicU64, Ordering};

/// Shared counters for the outcomes of incoming connection attempts.
///
/// One instance is created per [`crate::Config`] and shared by every listener
/// of the transport built from it. The counters only grow. They carry no
/// address or per-attempt data.
#[derive(Debug, Default)]
pub struct IncomingStats {
    retried: AtomicU64,
    accepted: AtomicU64,
    refused: AtomicU64,
    ignored: AtomicU64,
    budget_yields: AtomicU64,
}

impl IncomingStats {
    /// Attempts answered with a QUIC Retry packet (address not yet validated).
    pub fn retried(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }

    /// Attempts accepted, which start a handshake.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Attempts refused with a connection close: attempts the listener
    /// refuses, and attempts that fail in quinn `Incoming::accept` (quinn
    /// sends a close response for those).
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }

    /// Attempts dropped without a reply.
    pub fn ignored(&self) -> u64 {
        self.ignored.load(Ordering::Relaxed)
    }

    /// Times a listener stopped handling attempts because it reached
    /// [`crate::Config::max_incoming_outcomes_per_poll`] in one poll.
    pub fn budget_yields(&self) -> u64 {
        self.budget_yields.load(Ordering::Relaxed)
    }

    pub(crate) fn inc_retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_refused(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_ignored(&self) {
        self.ignored.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_budget_yields(&self) {
        self.budget_yields.fetch_add(1, Ordering::Relaxed);
    }
}

/// The part of [`crate::Config`] that a listener needs to decide on incoming
/// connection attempts.
#[derive(Debug, Clone)]
pub(crate) struct IncomingPolicy {
    /// See [`crate::Config::retry_unvalidated_incoming`].
    pub(crate) retry_unvalidated: bool,
    /// See [`crate::Config::max_incoming_outcomes_per_poll`].
    pub(crate) max_outcomes_per_poll: usize,
    /// See [`crate::Config::incoming_stats`].
    pub(crate) stats: std::sync::Arc<IncomingStats>,
}

impl IncomingPolicy {
    pub(crate) fn new(config: &crate::Config) -> Self {
        Self {
            retry_unvalidated: config.retry_unvalidated_incoming,
            max_outcomes_per_poll: config.max_incoming_outcomes_per_poll,
            stats: std::sync::Arc::clone(&config.incoming_stats),
        }
    }

    /// Decide on an attempt whose source address is not validated: send a Retry
    /// when quinn allows it, else refuse. A failed Retry is ignored, so that no
    /// more bytes go to the unvalidated address.
    pub(crate) fn decide_unvalidated(&self, incoming: quinn::Incoming) {
        if incoming.may_retry() {
            incoming.retry().map_or_else(
                |error| {
                    tracing::trace!("QUIC Retry failed, ignoring attempt");
                    error.into_incoming().ignore();
                    self.stats.inc_ignored();
                },
                |()| {
                    tracing::trace!("sent QUIC Retry to unvalidated address");
                    self.stats.inc_retried();
                },
            );
        } else {
            tracing::trace!("refusing unvalidated attempt that may not retry");
            incoming.refuse();
            self.stats.inc_refused();
        }
    }
}
