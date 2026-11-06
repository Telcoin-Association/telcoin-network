//! Bounded observations at the endpoint boundary, before an application sees an incoming.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Fixed protocol outcomes. These carry no remote addresses, identities, or error strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncomingEvent {
    /// A new Initial carries a valid Retry token.
    RetryValidated,
    /// A new Initial carries a valid address-validation token from NEW_TOKEN.
    AddressValidated,
    /// A decoded Retry token fails validation, including expiry or address mismatch.
    InvalidRetryToken,
    /// A nonempty token cannot be authenticated or decoded and is treated as absent.
    UndecodableToken,
    /// A NEW_TOKEN token fails address, lifetime, or replay validation.
    InvalidAddressToken,
    /// A new Initial is dropped at the effective incoming queue ceiling.
    QueueFull,
    /// A new Initial is dropped because connection IDs are exhausted.
    ConnectionIdsExhausted,
    /// A datagram for a queued incoming is dropped at a per-attempt or total buffer cap.
    BufferFull,
    /// An application explicitly refused an incoming attempt.
    Refused,
    /// An application explicitly ignored an incoming attempt.
    Ignored,
    /// An accept call failed, with or without a close response.
    AcceptFailed,
}

/// Shared, optional endpoint observations. Counters grow for this object's lifetime.
///
/// Live gauges sum all endpoints sharing this object. Queue occupancy includes both the
/// receive queue and incomings held by the application, until accept, retry, refuse, or ignore.
/// Dropping an endpoint removes its outstanding occupancy and configured ceiling.
#[derive(Debug, Default)]
pub struct IncomingStats {
    /// Initials returning valid Retry tokens.
    retry_validated: AtomicU64,
    /// Initials with valid NEW_TOKEN tokens.
    address_validated: AtomicU64,
    /// Rejected Retry tokens.
    invalid_retry_token: AtomicU64,
    /// Nonempty tokens treated as absent because they cannot be decoded.
    undecodable_token: AtomicU64,
    /// Address-validation tokens treated as absent after validation fails.
    invalid_address_token: AtomicU64,
    /// Initials dropped at max_incoming.
    queue_full: AtomicU64,
    /// Initials dropped at the connection ID limit.
    connection_ids_exhausted: AtomicU64,
    /// Buffered datagrams dropped at a byte cap.
    buffer_full: AtomicU64,
    /// Explicit API refusals.
    refused: AtomicU64,
    /// Explicit API ignores.
    ignored: AtomicU64,
    /// Accept errors, including stale queued Initials.
    accept_failed: AtomicU64,
    /// Live queued incomings.
    queued: AtomicUsize,
    /// Sum of effective max_incoming values of live listening endpoints.
    limit: AtomicUsize,
    /// Live endpoints with a server configuration.
    endpoints: AtomicUsize,
}

impl IncomingStats {
    /// Read one cumulative protocol outcome.
    pub fn count(&self, event: IncomingEvent) -> u64 {
        self.counter(event).load(Ordering::Relaxed)
    }

    /// Current incoming attempts retained before an application decision, in attempts.
    pub fn queued(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Sum of effective incoming ceilings of live listening endpoints, in attempts.
    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    /// Number of live endpoints with a server configuration.
    pub fn endpoints(&self) -> usize {
        self.endpoints.load(Ordering::Relaxed)
    }

    /// Resolve a fixed outcome to its atomic counter.
    fn counter(&self, event: IncomingEvent) -> &AtomicU64 {
        match event {
            IncomingEvent::RetryValidated => &self.retry_validated,
            IncomingEvent::AddressValidated => &self.address_validated,
            IncomingEvent::InvalidRetryToken => &self.invalid_retry_token,
            IncomingEvent::UndecodableToken => &self.undecodable_token,
            IncomingEvent::InvalidAddressToken => &self.invalid_address_token,
            IncomingEvent::QueueFull => &self.queue_full,
            IncomingEvent::ConnectionIdsExhausted => &self.connection_ids_exhausted,
            IncomingEvent::BufferFull => &self.buffer_full,
            IncomingEvent::Refused => &self.refused,
            IncomingEvent::Ignored => &self.ignored,
            IncomingEvent::AcceptFailed => &self.accept_failed,
        }
    }

    /// Record a protocol outcome at its decision site.
    pub(crate) fn record(&self, event: IncomingEvent) {
        self.counter(event).fetch_add(1, Ordering::Relaxed);
    }

    /// Account for one incoming inserted into an endpoint's slab.
    pub(crate) fn enqueue(&self) {
        self.queued.fetch_add(1, Ordering::Relaxed);
    }

    /// Release incomings removed from a slab, including endpoint destruction.
    pub(crate) fn dequeue(&self, count: usize) {
        self.queued.fetch_sub(count, Ordering::Relaxed);
    }

    /// Add an endpoint's current effective server configuration.
    pub(crate) fn add_server(&self, limit: usize) {
        self.limit.fetch_add(limit, Ordering::Relaxed);
        self.endpoints.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove an endpoint's previous effective server configuration.
    pub(crate) fn remove_server(&self, limit: usize) {
        self.limit.fetch_sub(limit, Ordering::Relaxed);
        self.endpoints.fetch_sub(1, Ordering::Relaxed);
    }
}
