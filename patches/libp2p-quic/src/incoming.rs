//! Aggregate counters for the decisions a listener makes on incoming
//! connection attempts (vendored addition, see PATCH.md).

use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Fixed stage and reason vocabulary for incoming outcomes, including pre-application work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingOutcome {
    /// The listener requested a Retry response for an unvalidated source.
    Retried,
    /// The listener started a handshake.
    Accepted,
    /// Quinn failed to accept an attempt, with or without a close response.
    AcceptFailed,
    /// An unvalidated attempt could not be retried and was refused.
    RetryNotPermitted,
    /// A failed Retry was ignored without another response.
    RetryFailed,
    /// The endpoint validated a returning Retry token.
    RetryValidated,
    /// The endpoint validated a NEW_TOKEN address token.
    AddressValidated,
    /// The endpoint rejected an invalid Retry token.
    InvalidRetryToken,
    /// A nonempty opaque token was treated as absent because it could not be decoded.
    UndecodableToken,
    /// A NEW_TOKEN token failed validation and was treated as absent.
    InvalidAddressToken,
    /// The endpoint dropped a new Initial at max_incoming.
    QueueFull,
    /// The endpoint dropped a new Initial due to exhausted connection IDs.
    ConnectionIdsExhausted,
    /// The endpoint dropped a buffered datagram at a byte cap.
    BufferFull,
    /// An application explicitly refused an attempt at the endpoint API.
    Refused,
    /// An application explicitly ignored an attempt at the endpoint API.
    Ignored,
}

impl IncomingOutcome {
    /// Complete bounded production vocabulary.
    pub const ALL: [Self; 15] = [
        Self::Retried,
        Self::Accepted,
        Self::AcceptFailed,
        Self::RetryNotPermitted,
        Self::RetryFailed,
        Self::RetryValidated,
        Self::AddressValidated,
        Self::InvalidRetryToken,
        Self::QueueFull,
        Self::ConnectionIdsExhausted,
        Self::BufferFull,
        Self::UndecodableToken,
        Self::InvalidAddressToken,
        Self::Refused,
        Self::Ignored,
    ];

    /// Stage at which the outcome becomes observable.
    pub fn stage(self) -> &'static str {
        match self {
            Self::Retried | Self::Accepted | Self::RetryNotPermitted | Self::RetryFailed => {
                "listener"
            }
            Self::RetryValidated
            | Self::AddressValidated
            | Self::InvalidRetryToken
            | Self::UndecodableToken
            | Self::InvalidAddressToken
            | Self::QueueFull
            | Self::ConnectionIdsExhausted
            | Self::BufferFull
            | Self::AcceptFailed
            | Self::Refused
            | Self::Ignored => "endpoint",
        }
    }

    /// Fixed decision name, independent of transport errors or remote input.
    pub fn decision(self) -> &'static str {
        match self {
            Self::Retried => "retry",
            Self::Accepted => "accept",
            Self::RetryNotPermitted | Self::InvalidRetryToken | Self::Refused => "refuse",
            Self::AcceptFailed => "accept_error",
            Self::RetryFailed
            | Self::QueueFull
            | Self::ConnectionIdsExhausted
            | Self::BufferFull
            | Self::Ignored => "ignore",
            Self::RetryValidated | Self::AddressValidated => "validate",
            Self::UndecodableToken | Self::InvalidAddressToken => "revalidate",
        }
    }

    /// Fixed reason name. No addresses, peer IDs, or raw errors are retained.
    pub fn reason(self) -> &'static str {
        match self {
            Self::Retried => "unvalidated_address",
            Self::Accepted => "handshake_started",
            Self::AcceptFailed => "accept_failed",
            Self::RetryNotPermitted => "retry_not_permitted",
            Self::RetryFailed => "retry_failed",
            Self::RetryValidated => "retry_token",
            Self::AddressValidated => "address_token",
            Self::InvalidRetryToken => "invalid_retry_token",
            Self::UndecodableToken => "undecodable_token",
            Self::InvalidAddressToken => "invalid_address_token",
            Self::QueueFull => "max_incoming",
            Self::ConnectionIdsExhausted => "connection_ids",
            Self::BufferFull => "incoming_buffer_bytes",
            Self::Refused => "application_refuse",
            Self::Ignored => "application_ignore",
        }
    }
}

/// Shared counters for the outcomes of incoming connection attempts.
///
/// One instance is created per [`crate::Config`] and shared by every listener
/// of the transport built from it. The counters only grow. They carry no
/// address or per-attempt data.
#[derive(Debug, Default)]
pub struct IncomingStats {
    /// Protocol observations made before Endpoint::accept yields an incoming.
    endpoint: Arc<quinn_proto::IncomingStats>,
    /// Explicit policy refusals when Retry is not permitted.
    retry_not_permitted: AtomicU64,
    /// Accepted handshakes whose upgrade future has not resolved or been dropped.
    handshakes: AtomicUsize,
    /// Retry responses requested by the listener policy.
    retried: AtomicU64,
    /// Handshakes successfully started by the listener.
    accepted: AtomicU64,
    /// Legacy policy-refusal and accept-error aggregate.
    refused: AtomicU64,
    /// Policy Retry failures followed by a silent ignore.
    ignored: AtomicU64,
    /// Listener polls stopped by the fixed outcome budget.
    budget_yields: AtomicU64,
}

impl IncomingStats {
    /// Endpoint observations shared by every listener of this transport.
    pub fn endpoint(&self) -> &Arc<quinn_proto::IncomingStats> {
        &self.endpoint
    }

    /// Accepted inbound upgrade futures currently in flight, in handshakes.
    pub fn handshakes(&self) -> usize {
        self.handshakes.load(Ordering::Relaxed)
    }

    /// Read a fixed decision/reason counter, including protocol events before the app hook.
    pub fn count(&self, outcome: IncomingOutcome) -> u64 {
        use quinn_proto::IncomingEvent;
        match outcome {
            IncomingOutcome::Retried => self.retried(),
            IncomingOutcome::Accepted => self.accepted(),
            IncomingOutcome::AcceptFailed => self.endpoint.count(IncomingEvent::AcceptFailed),
            IncomingOutcome::RetryNotPermitted => self.retry_not_permitted.load(Ordering::Relaxed),
            IncomingOutcome::RetryFailed => self.ignored(),
            IncomingOutcome::RetryValidated => self.endpoint.count(IncomingEvent::RetryValidated),
            IncomingOutcome::AddressValidated => {
                self.endpoint.count(IncomingEvent::AddressValidated)
            }
            IncomingOutcome::InvalidRetryToken => {
                self.endpoint.count(IncomingEvent::InvalidRetryToken)
            }
            IncomingOutcome::UndecodableToken => {
                self.endpoint.count(IncomingEvent::UndecodableToken)
            }
            IncomingOutcome::InvalidAddressToken => {
                self.endpoint.count(IncomingEvent::InvalidAddressToken)
            }
            IncomingOutcome::QueueFull => self.endpoint.count(IncomingEvent::QueueFull),
            IncomingOutcome::ConnectionIdsExhausted => {
                self.endpoint.count(IncomingEvent::ConnectionIdsExhausted)
            }
            IncomingOutcome::BufferFull => self.endpoint.count(IncomingEvent::BufferFull),
            IncomingOutcome::Refused => self.endpoint.count(IncomingEvent::Refused),
            IncomingOutcome::Ignored => self.endpoint.count(IncomingEvent::Ignored),
        }
    }

    /// Track one accepted upgrade through resolution, timeout, or cancellation.
    pub(crate) fn handshake(self: &Arc<Self>) -> HandshakeGuard {
        self.handshakes.fetch_add(1, Ordering::Relaxed);
        HandshakeGuard(Arc::clone(self))
    }

    /// Count a failed accept separately while preserving the legacy refused aggregate.
    pub(crate) fn inc_accept_failed(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }
    /// Attempts answered with a QUIC Retry packet (address not yet validated).
    pub fn retried(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }

    /// Attempts accepted, which start a handshake.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Legacy aggregate of explicit policy refusals and failed `Incoming::accept` calls.
    /// A failed accept may close the connection or silently discard a stale Initial.
    /// Use [`IncomingOutcome`] for the distinct stage and reason counters.
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
        self.retry_not_permitted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_ignored(&self) {
        self.ignored.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn inc_budget_yields(&self) {
        self.budget_yields.fetch_add(1, Ordering::Relaxed);
    }
}

/// Releases an accepted handshake when its upgrade resolves or is abandoned.
#[derive(Debug)]
pub(crate) struct HandshakeGuard(Arc<IncomingStats>);

impl Drop for HandshakeGuard {
    /// Release exactly the handshake owned by this future.
    fn drop(&mut self) {
        self.0.handshakes.fetch_sub(1, Ordering::Relaxed);
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
