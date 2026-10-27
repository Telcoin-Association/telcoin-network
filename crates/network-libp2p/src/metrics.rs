//! Prometheus metrics for the libp2p consensus networks.
//!
//! Both the primary and worker networks instantiate the same types, so every series
//! carries a `network` label (`primary` or `worker-{id}`) set at construction.

use crate::{
    peers::{PeerAdmissionDenied, Penalty, PenaltyOutcome, PutRecordRate},
    service_class::{ServiceClass, ShedReason},
    source_admission::AdmissionError,
    types::NetworkType,
};
use libp2p::{
    connection_limits,
    request_response::{InboundFailure, OutboundFailure},
    swarm::{ConnectionDenied, ListenError},
};
use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use std::{fmt, io, time::Duration};
use tn_config::{QuicConfig, SwarmNetworkBudget};
use tn_types::{TrySendError, TrySendOutcome};

/// Map a [`NetworkType`] to its metric label value.
pub(crate) fn network_label(network_type: &NetworkType) -> String {
    match network_type {
        NetworkType::Primary => "primary".to_owned(),
        NetworkType::Worker(id) => format!("worker-{id}"),
    }
}

/// Derive-backed handles for swarm-level metrics.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_network")]
struct SwarmMetricHandles {
    /// Current established connections across both directions and all peer classes.
    established_connections: Gauge,
    /// Configured established-connection ceiling; zero denotes the legacy unbounded total.
    established_connection_limit: Gauge,
    /// Configured incoming bidirectional stream ceiling for each established connection.
    inbound_streams_per_connection_limit: Gauge,
    /// Advertised receive-credit ceiling per established connection, not retained bytes or RSS.
    receive_credit_per_connection_bytes: Gauge,
    /// Gossip messages published by this node.
    gossip_published_total: Counter,
    /// Gossip publishes refused because the send queue of every recipient peer was full.
    gossip_publish_queue_full_total: Counter,
    /// Gossip messages received from peers.
    gossip_received_total: Counter,
    /// Gossip messages rejected (failed verification against authorized publishers).
    gossip_rejected_total: Counter,
    /// Inbound provider announcements dropped by the per-source rate limit.
    add_provider_rate_limited_total: Counter,
    /// Inbound provider announcements dropped because the provider is not the local peer.
    add_provider_remote_dropped_total: Counter,
    /// Graceful peer-exchange disconnects awaiting the peer's ack.
    px_disconnects_pending: Gauge,
    /// Outbound requests in flight.
    outbound_requests_pending: Gauge,
    /// Record retrievals awaiting a response or terminal failure.
    record_exchange_pending: Gauge,
    /// Peers queued for another record retrieval attempt.
    record_exchange_deferred: Gauge,
    /// Incoming QUIC attempts answered with a Retry (source address not validated).
    quic_incoming_retried_total: Counter,
    /// Incoming QUIC attempts accepted into a handshake.
    quic_incoming_accepted_total: Counter,
    /// Legacy aggregate of listener policy refusals and failed accepts, including stale Initials.
    quic_incoming_refused_total: Counter,
    /// Incoming QUIC attempts dropped without a reply.
    quic_incoming_ignored_total: Counter,
    /// Times the QUIC listener yielded after its per-poll outcome cap.
    quic_incoming_budget_yields_total: Counter,
    /// Endpoint attempts retained before accept, Retry, refuse, or ignore.
    quic_incoming_queued: Gauge,
    /// Sum of effective max_incoming values across live listening endpoints.
    quic_incoming_limit: Gauge,
    /// Live QUIC endpoints with an incoming server configuration.
    quic_incoming_endpoints: Gauge,
    /// Accepted inbound upgrade futures still pending, distinct from endpoint queue occupancy.
    quic_incoming_handshakes: Gauge,
    /// Swarm pending inbound connections after transport admission.
    pending_incoming_connections: Gauge,
}

/// The `connection_limits` bound named by a [`connection_limits::Exceeded`] refusal.
///
/// libp2p keeps the bound kind private, so the classifier reads the fixed `Display` text of
/// libp2p-connection-limits 0.7.0: "connection limit exceeded: at most {limit} {kind} are
/// allowed". It never compares the limit with the configuration, because two bounds can share
/// one value. A text that matches no known kind maps to [`Self::Unknown`], so a libp2p upgrade
/// that changes the text shows up as `unknown`. The unit test pins the reviewed text; recheck
/// the dependency's Display implementation after an upgrade.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionLimitReason {
    /// The pending incoming ceiling refused a handshake.
    PendingIncoming,
    /// The pending outgoing ceiling refused a dial.
    PendingOutgoing,
    /// The established incoming ceiling refused a connection.
    EstablishedIncoming,
    /// The established outgoing ceiling refused a connection.
    EstablishedOutgoing,
    /// The per-peer established ceiling refused a connection.
    EstablishedPerPeer,
    /// The total established ceiling refused a connection.
    EstablishedTotal,
    /// The refusal text matched no known bound.
    Unknown,
}

impl ConnectionLimitReason {
    /// Every reason, in label order.
    pub(crate) const ALL: [Self; 7] = [
        Self::PendingIncoming,
        Self::PendingOutgoing,
        Self::EstablishedIncoming,
        Self::EstablishedOutgoing,
        Self::EstablishedPerPeer,
        Self::EstablishedTotal,
        Self::Unknown,
    ];

    /// The metric label value for this reason.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PendingIncoming => "pending_incoming",
            Self::PendingOutgoing => "pending_outgoing",
            Self::EstablishedIncoming => "established_incoming",
            Self::EstablishedOutgoing => "established_outgoing",
            Self::EstablishedPerPeer => "established_per_peer",
            Self::EstablishedTotal => "established_total",
            Self::Unknown => "unknown",
        }
    }

    /// The libp2p `Display` text of the bound kind that this reason names.
    fn kind_text(self) -> Option<&'static str> {
        match self {
            Self::PendingIncoming => Some("pending incoming connections"),
            Self::PendingOutgoing => Some("pending outgoing connections"),
            Self::EstablishedIncoming => Some("established incoming connections"),
            Self::EstablishedOutgoing => Some("established outgoing connections"),
            Self::EstablishedPerPeer => Some("established connections per peer"),
            Self::EstablishedTotal => Some("established connections"),
            Self::Unknown => None,
        }
    }

    /// Classify a `connection_limits` refusal by the bound that it names.
    pub(crate) fn from_exceeded(exceeded: &connection_limits::Exceeded) -> Self {
        Self::from_text(&exceeded.to_string())
    }

    /// Classify the `Display` text of a refusal.
    ///
    /// The kind sits between the limit and " are allowed", so each kind text must match the
    /// whole suffix. A plain substring test would read "established connections per peer" as
    /// the total bound.
    fn from_text(text: &str) -> Self {
        Self::ALL
            .into_iter()
            .find(|reason| {
                reason
                    .kind_text()
                    .is_some_and(|kind| text.ends_with(&format!(" {kind} are allowed")))
            })
            .unwrap_or(Self::Unknown)
    }
}

/// The `connection_limits` bound that refused an inbound connection.
///
/// The swarm classifies the refusal by its text ([`ConnectionLimitReason`]), not by the peer
/// id: both established ceilings refuse after authentication, so a peer id cannot tell them
/// apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundDenial {
    /// The pending inbound ceiling refused a handshake before the remote was authenticated.
    PendingIncomingLimit,
    /// The per-peer established ceiling refused an authenticated connection.
    EstablishedPerPeerLimit,
    /// The total established ceiling refused an authenticated connection.
    EstablishedTotalLimit,
    /// Any other `connection_limits` bound. The swarms configure none, so this stays zero
    /// unless the refusal text is unknown.
    Other,
}

impl InboundDenial {
    /// Every denial, in label order.
    pub(crate) const ALL: [Self; 4] = [
        Self::PendingIncomingLimit,
        Self::EstablishedPerPeerLimit,
        Self::EstablishedTotalLimit,
        Self::Other,
    ];

    /// The inbound denial for a refusal of `reason`.
    pub(crate) fn from_reason(reason: ConnectionLimitReason) -> Self {
        match reason {
            ConnectionLimitReason::PendingIncoming => Self::PendingIncomingLimit,
            ConnectionLimitReason::EstablishedPerPeer => Self::EstablishedPerPeerLimit,
            ConnectionLimitReason::EstablishedTotal => Self::EstablishedTotalLimit,
            ConnectionLimitReason::PendingOutgoing
            | ConnectionLimitReason::EstablishedIncoming
            | ConnectionLimitReason::EstablishedOutgoing
            | ConnectionLimitReason::Unknown => Self::Other,
        }
    }

    /// The metric label value for this denial.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PendingIncomingLimit => "pending_incoming_limit",
            Self::EstablishedPerPeerLimit => "established_per_peer_limit",
            Self::EstablishedTotalLimit => "established_total_limit",
            Self::Other => "other_limit",
        }
    }
}

/// The decision point that refused an inbound connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RejectionStage {
    /// `handle_pending_inbound_connection` refused the remote before authentication.
    PreAuth,
    /// `handle_established_inbound_connection` refused the authenticated peer: a peer-manager
    /// policy or the source admission budget.
    AuthenticatedPolicy,
    /// A `connection_limits` ceiling, pending or established, refused the connection.
    TransportCap,
    /// The cause has a type that no known behaviour raises.
    Unknown,
}

impl RejectionStage {
    /// The metric label value for this stage.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PreAuth => "pre_auth",
            Self::AuthenticatedPolicy => "authenticated_policy",
            Self::TransportCap => "transport_cap",
            Self::Unknown => "unknown",
        }
    }
}

/// Why the swarm refused an inbound connection: the cause of a `ListenError::Denied`.
///
/// The swarm stops at the first behaviour that denies a connection and emits one
/// `IncomingConnectionError` for it, so `inbound_connections_denied_total` counts each terminal
/// rejection once. The stage counters `listen_failures_total`, `source_admission_denied_total`
/// and `closed_admission_denied_total` record the same denials at the peer manager and overlap
/// with this count on purpose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundRejection {
    /// A `connection_limits` bound refused the connection.
    Limit(InboundDenial),
    /// A peer-manager policy refused the connection.
    PeerManager(PeerAdmissionDenied),
    /// The source admission budget refused the authenticated connection.
    Source(AdmissionError),
    /// The cause has a type that no known behaviour raises.
    Unknown,
}

impl InboundRejection {
    /// Every rejection, in label order.
    pub(crate) fn all() -> impl Iterator<Item = Self> {
        InboundDenial::ALL
            .into_iter()
            .map(Self::Limit)
            .chain(PeerAdmissionDenied::ALL.into_iter().map(Self::PeerManager))
            .chain(AdmissionError::ALL.into_iter().map(Self::Source))
            .chain(std::iter::once(Self::Unknown))
    }

    /// Classify a denial cause by its concrete error type.
    pub(crate) fn from_cause(cause: &ConnectionDenied) -> Self {
        cause
            .downcast_ref::<connection_limits::Exceeded>()
            .map(|exceeded| {
                Self::Limit(InboundDenial::from_reason(ConnectionLimitReason::from_exceeded(
                    exceeded,
                )))
            })
            .or_else(|| cause.downcast_ref::<PeerAdmissionDenied>().copied().map(Self::PeerManager))
            .or_else(|| cause.downcast_ref::<AdmissionError>().copied().map(Self::Source))
            .unwrap_or(Self::Unknown)
    }

    /// The metric `reason` label value. The limit labels keep their existing values.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Limit(denial) => denial.label(),
            Self::PeerManager(denied) => denied.reason(),
            Self::Source(error) => error.label(),
            Self::Unknown => "unknown",
        }
    }

    /// The decision point that raised this rejection.
    pub(crate) fn stage(self) -> RejectionStage {
        match self {
            Self::Limit(_) => RejectionStage::TransportCap,
            Self::PeerManager(PeerAdmissionDenied::InvalidIp) => RejectionStage::PreAuth,
            Self::PeerManager(
                PeerAdmissionDenied::LocalPeer
                | PeerAdmissionDenied::BannedPeer
                | PeerAdmissionDenied::ClosedAdmission,
            )
            | Self::Source(_) => RejectionStage::AuthenticatedPolicy,
            Self::Unknown => RejectionStage::Unknown,
        }
    }
}

/// The bounded reason of a failed inbound attempt that the peer manager observes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListenFailureReason {
    /// A peer-manager policy denied the connection.
    PeerManager(PeerAdmissionDenied),
    /// Another behaviour, or the peer manager's source budget, denied the connection.
    OtherBehaviour,
    /// The transport upgrade failed.
    Transport,
    /// The remote authenticated as a peer other than the expected one.
    WrongPeerId,
    /// The remote authenticated as this node.
    LocalPeerId,
    /// The pending connection was aborted.
    Aborted,
}

impl ListenFailureReason {
    /// Every reason, in label order.
    pub(crate) fn all() -> impl Iterator<Item = Self> {
        PeerAdmissionDenied::ALL.into_iter().map(Self::PeerManager).chain([
            Self::OtherBehaviour,
            Self::Transport,
            Self::WrongPeerId,
            Self::LocalPeerId,
            Self::Aborted,
        ])
    }

    /// Classify a listen failure. Peer and address data never reach the label.
    pub(crate) fn from_error(error: &ListenError) -> Self {
        match error {
            ListenError::Denied { cause } => cause
                .downcast_ref::<PeerAdmissionDenied>()
                .copied()
                .map_or(Self::OtherBehaviour, Self::PeerManager),
            ListenError::Transport(_) => Self::Transport,
            ListenError::WrongPeerId { .. } => Self::WrongPeerId,
            ListenError::LocalPeerId { .. } => Self::LocalPeerId,
            ListenError::Aborted => Self::Aborted,
        }
    }

    /// The metric label value for this reason.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::PeerManager(denied) => denied.reason(),
            Self::OtherBehaviour => "other_behaviour_denied",
            Self::Transport => "transport",
            Self::WrongPeerId => "wrong_peer_id",
            Self::LocalPeerId => "local_peer_id",
            Self::Aborted => "aborted",
        }
    }
}

/// The class of a `SwarmEvent::ListenerError`, from the stable [`io::ErrorKind`] of the error.
///
/// The swarm boxes its transport, so most QUIC listener errors arrive as
/// [`io::ErrorKind::Other`] and count as `other`. A kind outside this set counts as `unknown`.
/// The raw error text never becomes a label.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListenerErrorReason {
    /// The listen address is already in use.
    AddressInUse,
    /// The listen address is not available on this host.
    AddressNotAvailable,
    /// The operating system refused the operation.
    PermissionDenied,
    /// A socket was refused, reset, aborted or disconnected.
    Connection,
    /// An operation timed out.
    TimedOut,
    /// The listener received invalid data or input.
    InvalidData,
    /// A wrapped transport error with no more specific kind.
    Other,
    /// Any other kind.
    Unknown,
}

impl ListenerErrorReason {
    /// Every reason, in label order.
    pub(crate) const ALL: [Self; 8] = [
        Self::AddressInUse,
        Self::AddressNotAvailable,
        Self::PermissionDenied,
        Self::Connection,
        Self::TimedOut,
        Self::InvalidData,
        Self::Other,
        Self::Unknown,
    ];

    /// The kinds that have a reason. `io::ErrorKind` is non-exhaustive, so a table lookup with
    /// an explicit [`Self::Unknown`] fallback replaces a match.
    const KINDS: [(io::ErrorKind, Self); 12] = [
        (io::ErrorKind::AddrInUse, Self::AddressInUse),
        (io::ErrorKind::AddrNotAvailable, Self::AddressNotAvailable),
        (io::ErrorKind::PermissionDenied, Self::PermissionDenied),
        (io::ErrorKind::ConnectionRefused, Self::Connection),
        (io::ErrorKind::ConnectionReset, Self::Connection),
        (io::ErrorKind::ConnectionAborted, Self::Connection),
        (io::ErrorKind::NotConnected, Self::Connection),
        (io::ErrorKind::BrokenPipe, Self::Connection),
        (io::ErrorKind::TimedOut, Self::TimedOut),
        (io::ErrorKind::InvalidData, Self::InvalidData),
        (io::ErrorKind::InvalidInput, Self::InvalidData),
        (io::ErrorKind::Other, Self::Other),
    ];

    /// Classify a listener error by its kind.
    pub(crate) fn from_kind(kind: io::ErrorKind) -> Self {
        Self::KINDS
            .into_iter()
            .find(|(known, _)| *known == kind)
            .map_or(Self::Unknown, |(_, reason)| reason)
    }

    /// The metric label value for this reason.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::AddressInUse => "address_in_use",
            Self::AddressNotAvailable => "address_not_available",
            Self::PermissionDenied => "permission_denied",
            Self::Connection => "connection",
            Self::TimedOut => "timed_out",
            Self::InvalidData => "invalid_data",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }
}

/// How a listener closed: `ok` for a clean close, `error` when the listener failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListenerCloseOutcome {
    /// The listener closed without an error.
    Ok,
    /// The listener closed because of an error.
    Error,
}

impl ListenerCloseOutcome {
    /// Every outcome, in label order.
    pub(crate) const ALL: [Self; 2] = [Self::Ok, Self::Error];

    /// The outcome of a `SwarmEvent::ListenerClosed` reason.
    pub(crate) fn from_reason(reason: &Result<(), io::Error>) -> Self {
        reason.as_ref().map_or(Self::Error, |&()| Self::Ok)
    }

    /// The metric label value for this outcome.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

/// The direction of a connection that a peer-manager stage counter records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionDirection {
    /// The remote opened the connection.
    Inbound,
    /// This node dialed the connection.
    Outbound,
}

impl ConnectionDirection {
    /// Every direction, in label order.
    pub(crate) const ALL: [Self; 2] = [Self::Inbound, Self::Outbound];

    /// The metric label value for this direction.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Inbound => "in",
            Self::Outbound => "out",
        }
    }
}

/// How a forwarded inbound request ended without a response.
///
/// `inbound_request_service_seconds` observes only answered requests, so this outcome is the
/// only record of a request that failed. A request that the primary drops at its epoch-record
/// admission cap also counts as [`ShedReason::Admission`]; when its response channel closes,
/// the same request counts here as `omitted`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InboundFailureOutcome {
    /// The request timed out before the application answered.
    Timeout,
    /// The application dropped the response channel without an answer.
    Omitted,
    /// The connection closed before the response was sent.
    Closed,
    /// An I/O error, including a codec violation.
    Io,
    /// The local node supports none of the protocols that the remote requested.
    Unsupported,
}

impl InboundFailureOutcome {
    /// Every outcome, in label order.
    pub(crate) const ALL: [Self; 5] =
        [Self::Timeout, Self::Omitted, Self::Closed, Self::Io, Self::Unsupported];

    /// The metric label value for this outcome.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Omitted => "omitted",
            Self::Closed => "closed",
            Self::Io => "io",
            Self::Unsupported => "unsupported",
        }
    }

    /// The outcome of a request-response inbound `failure`.
    pub(crate) fn from_failure(failure: &InboundFailure) -> Self {
        match failure {
            InboundFailure::Timeout => Self::Timeout,
            InboundFailure::ResponseOmission => Self::Omitted,
            InboundFailure::ConnectionClosed => Self::Closed,
            InboundFailure::Io(_) => Self::Io,
            InboundFailure::UnsupportedProtocols => Self::Unsupported,
        }
    }
}

/// One pre-resolved metric handle per [`ServiceClass`].
#[derive(Clone)]
pub(crate) struct PerClass<T> {
    /// The handle for [`ServiceClass::Vote`].
    vote: T,
    /// The handle for [`ServiceClass::EpochRecord`].
    epoch_record: T,
    /// The handle for [`ServiceClass::CertificateSync`].
    certificate_sync: T,
    /// The handle for [`ServiceClass::Batch`].
    batch: T,
    /// The handle for [`ServiceClass::Gossip`].
    gossip: T,
    /// The handle for [`ServiceClass::Other`].
    other: T,
}

impl<T> PerClass<T> {
    /// Resolve one handle per class with `resolve`.
    pub(crate) fn new(resolve: impl Fn(ServiceClass) -> T) -> Self {
        Self {
            vote: resolve(ServiceClass::Vote),
            epoch_record: resolve(ServiceClass::EpochRecord),
            certificate_sync: resolve(ServiceClass::CertificateSync),
            batch: resolve(ServiceClass::Batch),
            gossip: resolve(ServiceClass::Gossip),
            other: resolve(ServiceClass::Other),
        }
    }

    /// The handle for `class`.
    pub(crate) fn get(&self, class: ServiceClass) -> &T {
        match class {
            ServiceClass::Vote => &self.vote,
            ServiceClass::EpochRecord => &self.epoch_record,
            ServiceClass::CertificateSync => &self.certificate_sync,
            ServiceClass::Batch => &self.batch,
            ServiceClass::Gossip => &self.gossip,
            ServiceClass::Other => &self.other,
        }
    }
}

/// One pre-resolved metric handle per [`ShedReason`].
#[derive(Clone)]
struct PerShedReason<T> {
    /// The handle for [`ShedReason::QueueFull`].
    queue_full: T,
    /// The handle for [`ShedReason::Unsubscribed`].
    unsubscribed: T,
    /// The handle for [`ShedReason::Admission`].
    admission: T,
    /// The handle for [`ShedReason::ServeBudget`].
    serve_budget: T,
}

impl<T> PerShedReason<T> {
    /// Resolve one handle per reason with `resolve`.
    fn new(resolve: impl Fn(ShedReason) -> T) -> Self {
        Self {
            queue_full: resolve(ShedReason::QueueFull),
            unsubscribed: resolve(ShedReason::Unsubscribed),
            admission: resolve(ShedReason::Admission),
            serve_budget: resolve(ShedReason::ServeBudget),
        }
    }

    /// The handle for `reason`.
    fn get(&self, reason: ShedReason) -> &T {
        match reason {
            ShedReason::QueueFull => &self.queue_full,
            ShedReason::Unsubscribed => &self.unsubscribed,
            ShedReason::Admission => &self.admission,
            ShedReason::ServeBudget => &self.serve_budget,
        }
    }
}

/// One pre-resolved metric handle per [`InboundFailureOutcome`].
#[derive(Clone)]
struct PerOutcome<T> {
    /// The handle for [`InboundFailureOutcome::Timeout`].
    timeout: T,
    /// The handle for [`InboundFailureOutcome::Omitted`].
    omitted: T,
    /// The handle for [`InboundFailureOutcome::Closed`].
    closed: T,
    /// The handle for [`InboundFailureOutcome::Io`].
    io: T,
    /// The handle for [`InboundFailureOutcome::Unsupported`].
    unsupported: T,
}

impl<T> PerOutcome<T> {
    /// Resolve one handle per outcome with `resolve`.
    fn new(resolve: impl Fn(InboundFailureOutcome) -> T) -> Self {
        Self {
            timeout: resolve(InboundFailureOutcome::Timeout),
            omitted: resolve(InboundFailureOutcome::Omitted),
            closed: resolve(InboundFailureOutcome::Closed),
            io: resolve(InboundFailureOutcome::Io),
            unsupported: resolve(InboundFailureOutcome::Unsupported),
        }
    }

    /// The handle for `outcome`.
    fn get(&self, outcome: InboundFailureOutcome) -> &T {
        match outcome {
            InboundFailureOutcome::Timeout => &self.timeout,
            InboundFailureOutcome::Omitted => &self.omitted,
            InboundFailureOutcome::Closed => &self.closed,
            InboundFailureOutcome::Io => &self.io,
            InboundFailureOutcome::Unsupported => &self.unsupported,
        }
    }
}

/// One pre-resolved metric handle per [`ConnectionLimitReason`].
#[derive(Clone)]
struct PerLimitReason<T> {
    /// The handle for [`ConnectionLimitReason::PendingIncoming`].
    pending_incoming: T,
    /// The handle for [`ConnectionLimitReason::PendingOutgoing`].
    pending_outgoing: T,
    /// The handle for [`ConnectionLimitReason::EstablishedIncoming`].
    established_incoming: T,
    /// The handle for [`ConnectionLimitReason::EstablishedOutgoing`].
    established_outgoing: T,
    /// The handle for [`ConnectionLimitReason::EstablishedPerPeer`].
    established_per_peer: T,
    /// The handle for [`ConnectionLimitReason::EstablishedTotal`].
    established_total: T,
    /// The handle for [`ConnectionLimitReason::Unknown`].
    unknown: T,
}

impl<T> PerLimitReason<T> {
    /// Resolve one handle per reason with `resolve`.
    fn new(resolve: impl Fn(ConnectionLimitReason) -> T) -> Self {
        Self {
            pending_incoming: resolve(ConnectionLimitReason::PendingIncoming),
            pending_outgoing: resolve(ConnectionLimitReason::PendingOutgoing),
            established_incoming: resolve(ConnectionLimitReason::EstablishedIncoming),
            established_outgoing: resolve(ConnectionLimitReason::EstablishedOutgoing),
            established_per_peer: resolve(ConnectionLimitReason::EstablishedPerPeer),
            established_total: resolve(ConnectionLimitReason::EstablishedTotal),
            unknown: resolve(ConnectionLimitReason::Unknown),
        }
    }

    /// The handle for `reason`.
    fn get(&self, reason: ConnectionLimitReason) -> &T {
        match reason {
            ConnectionLimitReason::PendingIncoming => &self.pending_incoming,
            ConnectionLimitReason::PendingOutgoing => &self.pending_outgoing,
            ConnectionLimitReason::EstablishedIncoming => &self.established_incoming,
            ConnectionLimitReason::EstablishedOutgoing => &self.established_outgoing,
            ConnectionLimitReason::EstablishedPerPeer => &self.established_per_peer,
            ConnectionLimitReason::EstablishedTotal => &self.established_total,
            ConnectionLimitReason::Unknown => &self.unknown,
        }
    }
}

/// One counter per key, resolved once and registered at zero.
///
/// A family has at most a few dozen keys, so a linear scan finds the handle without a registry
/// lookup or a label allocation.
#[derive(Clone)]
struct KeyedCounters<K> {
    /// The resolved counter for each key.
    counters: Vec<(K, Counter)>,
}

impl<K> fmt::Debug for KeyedCounters<K> {
    /// The counters carry no useful state to print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyedCounters").finish_non_exhaustive()
    }
}

impl<K: Copy + PartialEq> KeyedCounters<K> {
    /// Resolve one counter for each of `keys` with `resolve` and register each at zero.
    fn new(keys: impl IntoIterator<Item = K>, resolve: impl Fn(K) -> Counter) -> Self {
        Self {
            counters: keys
                .into_iter()
                .map(|key| {
                    let counter = resolve(key);
                    counter.increment(0);
                    (key, counter)
                })
                .collect(),
        }
    }

    /// Count one event for `key`.
    fn increment(&self, key: K) {
        self.counters
            .iter()
            .filter(|(known, _)| *known == key)
            .for_each(|(_, counter)| counter.increment(1));
    }
}

/// Pre-resolved outbound failure counters, retaining the existing kind labels.
#[derive(Clone)]
struct PerOutboundFailure {
    /// Failed connection attempts.
    dial: Counter,
    /// Connections closed before a response arrived.
    connection: Counter,
    /// Request or response I/O failures.
    io: Counter,
    /// Requests that exceeded their timeout.
    timeout: Counter,
    /// Requests with no mutually supported protocol.
    unsupported: Counter,
}

impl PerOutboundFailure {
    /// Resolve and register every outbound failure kind at zero.
    fn new(network: &str) -> Self {
        let resolve = |kind| {
            let counter = metrics::counter!(
                "tn_network.outbound_request_failures_total",
                "network" => network.to_owned(),
                "kind" => kind,
            );
            counter.increment(0);
            counter
        };
        Self {
            dial: resolve("dial"),
            connection: resolve("connection"),
            io: resolve("io"),
            timeout: resolve("timeout"),
            unsupported: resolve("unsupported"),
        }
    }

    /// Select the pre-resolved handle for an outbound failure.
    fn get(&self, failure: &OutboundFailure) -> &Counter {
        match failure {
            OutboundFailure::DialFailure => &self.dial,
            OutboundFailure::ConnectionClosed => &self.connection,
            OutboundFailure::Io(_) => &self.io,
            OutboundFailure::Timeout => &self.timeout,
            OutboundFailure::UnsupportedProtocols => &self.unsupported,
        }
    }
}

/// The labeled swarm series, resolved once per swarm.
///
/// The swarm records through these handles, so the event loop never looks up the registry. The
/// constructor registers every series at zero, so an absent series means missing data, not
/// zero events.
#[derive(Clone)]
struct LabeledHandles {
    /// `inbound_requests_pending` per class.
    inbound_pending: PerClass<Gauge>,
    /// `inbound_request_service_seconds` per class (answered requests only).
    service_seconds: PerClass<Histogram>,
    /// `inbound_requests_shed_total` per class and reason.
    shed: PerClass<PerShedReason<Counter>>,
    /// `inbound_requests_failed_total` per class and outcome.
    failed: PerClass<PerOutcome<Counter>>,
    /// `connection_limit_rejections_total` per bound, inbound and outbound.
    limit_rejections: PerLimitReason<Counter>,
    /// `inbound_connections_denied_total` per terminal rejection reason and stage.
    inbound_rejected: KeyedCounters<InboundRejection>,
    /// `listener_errors_total` per error class.
    listener_errors: KeyedCounters<ListenerErrorReason>,
    /// `listeners_closed_total` per close outcome.
    listeners_closed: KeyedCounters<ListenerCloseOutcome>,
    /// `outbound_request_failures_total` per failure kind.
    outbound_failed: PerOutboundFailure,
}

impl fmt::Debug for LabeledHandles {
    /// The handles carry no useful state to print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LabeledHandles").finish_non_exhaustive()
    }
}

impl LabeledHandles {
    /// Resolve every labeled series for the `network` label value and register each at zero.
    fn new(network: &str) -> Self {
        Self {
            outbound_failed: PerOutboundFailure::new(network),
            inbound_pending: PerClass::new(|class| {
                metrics::gauge!(
                    "tn_network.inbound_requests_pending",
                    "network" => network.to_owned(),
                    "class" => class.label(),
                )
            }),
            service_seconds: PerClass::new(|class| {
                metrics::histogram!(
                    "tn_network.inbound_request_service_seconds",
                    "network" => network.to_owned(),
                    "class" => class.label(),
                )
            }),
            shed: PerClass::new(|class| {
                PerShedReason::new(|reason| shed_counter(network, class, reason))
            }),
            failed: PerClass::new(|class| {
                PerOutcome::new(|outcome| {
                    metrics::counter!(
                        "tn_network.inbound_requests_failed_total",
                        "network" => network.to_owned(),
                        "class" => class.label(),
                        "outcome" => outcome.label(),
                    )
                })
            }),
            limit_rejections: PerLimitReason::new(|reason| {
                metrics::counter!(
                    "tn_network.connection_limit_rejections_total",
                    "network" => network.to_owned(),
                    "reason" => reason.label(),
                )
            }),
            inbound_rejected: KeyedCounters::new(InboundRejection::all(), |rejection| {
                metrics::counter!(
                    "tn_network.inbound_connections_denied_total",
                    "network" => network.to_owned(),
                    "reason" => rejection.label(),
                    "stage" => rejection.stage().label(),
                )
            }),
            listener_errors: KeyedCounters::new(ListenerErrorReason::ALL, |reason| {
                metrics::counter!(
                    "tn_network.listener_errors_total",
                    "network" => network.to_owned(),
                    "reason" => reason.label(),
                )
            }),
            listeners_closed: KeyedCounters::new(ListenerCloseOutcome::ALL, |outcome| {
                metrics::counter!(
                    "tn_network.listeners_closed_total",
                    "network" => network.to_owned(),
                    "outcome" => outcome.label(),
                )
            }),
        }
        .registered()
    }

    /// Register every gauge and fixed counter at zero. The `KeyedCounters` families and the
    /// histograms register when resolved.
    fn registered(self) -> Self {
        ServiceClass::ALL.iter().for_each(|class| {
            self.inbound_pending.get(*class).set(0.0);
            ShedReason::ALL
                .iter()
                .for_each(|reason| self.shed.get(*class).get(*reason).increment(0));
            InboundFailureOutcome::ALL
                .iter()
                .for_each(|outcome| self.failed.get(*class).get(*outcome).increment(0));
        });
        ConnectionLimitReason::ALL
            .iter()
            .for_each(|reason| self.limit_rejections.get(*reason).increment(0));
        self
    }
}

/// Resolve the `inbound_requests_shed_total` series for one network label, class and reason.
pub(crate) fn shed_counter(network: &str, class: ServiceClass, reason: ShedReason) -> Counter {
    metrics::counter!(
        "tn_network.inbound_requests_shed_total",
        "network" => network.to_owned(),
        "class" => class.label(),
        "reason" => reason.label(),
    )
}

/// A pre-resolved shed series for inbound work that the application drops at an admission cap
/// after the swarm forwarded it.
///
/// The swarm already counted the request as forwarded and pending. The application drops the
/// response channel, so the swarm also counts the request as an `omitted` failure in
/// `inbound_requests_failed_total`.
#[derive(Clone)]
pub struct AdmissionShed {
    /// The resolved `inbound_requests_shed_total` counter.
    counter: Counter,
}

impl fmt::Debug for AdmissionShed {
    /// The counter carries no useful state to print.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmissionShed").finish_non_exhaustive()
    }
}

impl AdmissionShed {
    /// Resolve the series for `EpochRecord` requests that the primary drops at its admission
    /// cap: network `primary`, class `epoch_record`, reason `admission`.
    ///
    /// The primary swarm registers the same series at zero, so this handle only adds to it.
    pub fn epoch_record() -> Self {
        Self {
            counter: shed_counter(
                &network_label(&NetworkType::Primary),
                ServiceClass::EpochRecord,
                ShedReason::Admission,
            ),
        }
    }

    /// Count one request dropped at the admission cap.
    pub fn record(&self) {
        self.counter.increment(1);
    }
}

/// Swarm-level metrics owned by `ConsensusNetwork`.
#[derive(Clone, Debug)]
pub(crate) struct SwarmMetrics {
    /// Clear live gauges when the last owner of this swarm's metric handles shuts down.
    _quic_lifetime: std::sync::Arc<QuicGaugeLifetime>,
    /// Fixed transport outcome series, resolved once for this configured swarm.
    quic_outcomes: [(libp2p::quic::IncomingOutcome, std::sync::Arc<QuicCounter>);
        libp2p::quic::IncomingOutcome::ALL.len()],
    /// Per-lifetime source positions for the legacy transport aggregates.
    quic_legacy: [std::sync::Arc<QuicCounter>; 5],
    /// The configured swarm label for record-exchange events.
    network: String,
    /// The derive-backed handles.
    handles: SwarmMetricHandles,
    /// The labeled handles, resolved at construction.
    labeled: LabeledHandles,
}

/// Publish source deltas so a recreated swarm adds to the existing process recorder series.
#[derive(Debug)]
struct QuicCounter {
    /// Counter in the process recorder, retained across source lifetimes.
    counter: Counter,
    /// Last source total sampled by the network loop, shared by cloned metric handles.
    observed: std::sync::atomic::AtomicU64,
}

impl QuicCounter {
    /// Start observing one newly constructed transport's monotonic source counter.
    fn new(counter: Counter) -> Self {
        counter.increment(0);
        Self { counter, observed: std::sync::atomic::AtomicU64::new(0) }
    }

    /// Sample each source lifetime from the owning swarm's single event loop.
    fn publish(&self, current: u64) {
        let previous = self.observed.swap(current, std::sync::atomic::Ordering::Relaxed);
        self.counter.increment(current.saturating_sub(previous));
    }
}

/// Live transport gauges outlive individual sampling calls and clear on swarm teardown.
#[derive(Debug)]
struct QuicGaugeLifetime(SwarmMetricHandles);

impl Drop for QuicGaugeLifetime {
    /// A stopped swarm contributes no live queue entries, endpoints, or handshakes.
    fn drop(&mut self) {
        self.0.quic_incoming_queued.set(0);
        self.0.quic_incoming_limit.set(0);
        self.0.quic_incoming_endpoints.set(0);
        self.0.quic_incoming_handshakes.set(0);
        self.0.pending_incoming_connections.set(0);
    }
}

/// Convert native counts to correctly rounded gauges without truncating large ceilings.
fn quic_gauge_value(count: usize) -> f64 {
    let count = u64::try_from(count).unwrap_or(u64::MAX);
    let high = u32::try_from(count >> u32::BITS).unwrap_or(u32::MAX);
    let low = u32::try_from(count & u64::from(u32::MAX)).unwrap_or(u32::MAX);
    f64::from(high).mul_add(f64::from(u32::MAX) + 1.0, f64::from(low))
}

impl SwarmMetrics {
    /// Record effective transport ceilings using only the configured network label.
    pub(crate) fn with_capacity(
        self,
        quic: &QuicConfig,
        budget: Option<SwarmNetworkBudget>,
    ) -> Self {
        self.handles
            .established_connection_limit
            .set(f64::from(budget.map_or(0, |budget| budget.connections())));
        self.handles
            .inbound_streams_per_connection_limit
            .set(f64::from(quic.max_concurrent_stream_limit));
        self.handles.receive_credit_per_connection_bytes.set(f64::from(quic.max_connection_data));
        self.handles.established_connections.set(0.0);
        self
    }

    /// Observe established connection occupancy, including multiple connections to one peer.
    pub(crate) fn set_established_connections(&self, connections: u32) {
        self.handles.established_connections.set(f64::from(connections));
    }

    /// Count a connection that a `connection_limits` bound refused, inbound or outbound, by the
    /// bound that refused it. No peer or address labels.
    pub(crate) fn record_connection_limit_rejection(&self, reason: ConnectionLimitReason) {
        self.labeled.limit_rejections.get(reason).increment(1);
    }

    /// Create the swarm metric handles for `network_type`.
    pub(crate) fn new_for(network_type: &NetworkType) -> Self {
        let network = network_label(network_type);
        let handles = SwarmMetricHandles::new_with_labels(&[("network", network.clone())]);
        Self {
            _quic_lifetime: std::sync::Arc::new(QuicGaugeLifetime(handles.clone())),
            quic_outcomes: libp2p::quic::IncomingOutcome::ALL.map(|outcome| {
                let counter = metrics::counter!(
                    "tn_network.quic_incoming_outcomes_total",
                    "network" => network.clone(),
                    "stage" => outcome.stage(),
                    "decision" => outcome.decision(),
                    "reason" => outcome.reason(),
                );
                (outcome, std::sync::Arc::new(QuicCounter::new(counter)))
            }),
            quic_legacy: [
                &handles.quic_incoming_retried_total,
                &handles.quic_incoming_accepted_total,
                &handles.quic_incoming_refused_total,
                &handles.quic_incoming_ignored_total,
                &handles.quic_incoming_budget_yields_total,
            ]
            .map(|counter| std::sync::Arc::new(QuicCounter::new(counter.clone()))),
            handles,
            labeled: LabeledHandles::new(&network),
            network,
        }
    }

    /// Record a successfully published gossip message.
    pub(crate) fn record_gossip_published(&self) {
        self.handles.gossip_published_total.increment(1);
    }

    /// Record a gossip publish refused because every recipient peer's send queue was full.
    ///
    /// The swarm task does not retry the publish. The caller receives
    /// `PublishError::AllQueuesFull` and decides what to do.
    pub(crate) fn record_gossip_publish_queue_full(&self) {
        self.handles.gossip_publish_queue_full_total.increment(1);
    }

    /// Record a gossip message received from a peer.
    pub(crate) fn record_gossip_received(&self) {
        self.handles.gossip_received_total.increment(1);
    }

    /// Record a gossip message that failed verification.
    pub(crate) fn record_gossip_rejected(&self) {
        self.handles.gossip_rejected_total.increment(1);
    }

    /// Record an inbound provider announcement dropped before a store write.
    pub(crate) fn record_add_provider_rate_limited(&self) {
        self.handles.add_provider_rate_limited_total.increment(1);
    }

    /// Record an inbound provider announcement from a remote provider, dropped before a store
    /// write.
    pub(crate) fn record_add_provider_remote_dropped(&self) {
        self.handles.add_provider_remote_dropped_total.increment(1);
    }

    /// Update the in-flight request gauges (called once per event-loop iteration).
    pub(crate) fn set_pending(&self, px_disconnects: usize, outbound_requests: usize) {
        self.handles.px_disconnects_pending.set(px_disconnects as f64);
        self.handles.outbound_requests_pending.set(outbound_requests as f64);
    }

    /// Publish source deltas and live transport gauges from the owning network loop.
    /// Every series uses only the configured swarm and fixed outcome labels.
    pub(crate) fn record_quic_incoming(&self, stats: &libp2p::quic::IncomingStats) {
        self.quic_outcomes
            .iter()
            .for_each(|(outcome, counter)| counter.publish(stats.count(*outcome)));
        let endpoint = stats.endpoint();
        self.handles.quic_incoming_queued.set(quic_gauge_value(endpoint.queued()));
        self.handles.quic_incoming_limit.set(quic_gauge_value(endpoint.limit()));
        self.handles.quic_incoming_endpoints.set(quic_gauge_value(endpoint.endpoints()));
        self.handles.quic_incoming_handshakes.set(quic_gauge_value(stats.handshakes()));
        [
            stats.retried(),
            stats.accepted(),
            stats.refused(),
            stats.ignored(),
            stats.budget_yields(),
        ]
        .into_iter()
        .zip(&self.quic_legacy)
        .for_each(|(current, counter)| counter.publish(current));
    }

    /// Publish the swarm's inbound pending count independently of transport resource gauges.
    pub(crate) fn set_pending_incoming_connections(&self, pending: u32) {
        self.handles.pending_incoming_connections.set(f64::from(pending));
    }

    /// Record a retrieval event with a fixed, peer-independent outcome label.
    pub(crate) fn record_exchange(&self, outcome: &'static str) {
        metrics::counter!(
            "tn_network.record_exchange_total",
            "network" => self.network.clone(),
            "outcome" => outcome,
        )
        .increment(1);
    }

    /// Publish the bounded live-request and deferred-retry set sizes.
    pub(crate) fn set_record_exchange_pending(&self, pending: usize, deferred: usize) {
        self.handles.record_exchange_pending.set(u32::try_from(pending).unwrap_or(u32::MAX));
        self.handles.record_exchange_deferred.set(u32::try_from(deferred).unwrap_or(u32::MAX));
    }

    /// Record an outbound request failure by failure kind.
    pub(crate) fn record_outbound_failure(&self, failure: &OutboundFailure) {
        self.labeled.outbound_failed.get(failure).increment(1);
    }

    /// Count an inbound connection that the swarm refused, by terminal reason and stage, and
    /// return the rejection. A `connection_limits` refusal also counts in
    /// `connection_limit_rejections_total`. No peer or address labels.
    pub(crate) fn record_inbound_rejection(&self, cause: &ConnectionDenied) -> InboundRejection {
        cause
            .downcast_ref::<connection_limits::Exceeded>()
            .map(ConnectionLimitReason::from_exceeded)
            .into_iter()
            .for_each(|reason| self.record_connection_limit_rejection(reason));
        let rejection = InboundRejection::from_cause(cause);
        self.labeled.inbound_rejected.increment(rejection);
        rejection
    }

    /// Count an inbound connection refused by a `connection_limits` bound, without a cause.
    #[cfg(test)]
    pub(crate) fn record_inbound_denied(&self, denial: &InboundDenial) {
        self.labeled.inbound_rejected.increment(InboundRejection::Limit(*denial));
    }

    /// Count a `SwarmEvent::ListenerError` by the class of its error kind and return the class.
    /// The error text never becomes a label.
    pub(crate) fn record_listener_error(&self, error: &io::Error) -> ListenerErrorReason {
        let reason = ListenerErrorReason::from_kind(error.kind());
        self.labeled.listener_errors.increment(reason);
        reason
    }

    /// Count a `SwarmEvent::ListenerClosed` by its outcome: a clean close or an error.
    pub(crate) fn record_listener_closed(&self, reason: &Result<(), io::Error>) {
        self.labeled.listeners_closed.increment(ListenerCloseOutcome::from_reason(reason));
    }

    /// Export the pending inbound requests of `class`.
    pub(crate) fn set_inbound_pending(&self, class: ServiceClass, pending: u32) {
        self.labeled.inbound_pending.get(class).set(f64::from(pending));
    }

    /// Record the time from forwarding an inbound request to sending its response.
    ///
    /// Only answered requests reach the histogram. A request that fails instead counts in
    /// `inbound_requests_failed_total` (see [`Self::record_inbound_failure`]).
    pub(crate) fn record_service_time(&self, class: ServiceClass, elapsed: Duration) {
        self.labeled.service_seconds.get(class).record(elapsed.as_secs_f64());
    }

    /// Count a forwarded inbound request of `class` that ended without a response.
    pub(crate) fn record_inbound_failure(
        &self,
        class: ServiceClass,
        outcome: InboundFailureOutcome,
    ) {
        self.labeled.failed.get(class).get(outcome).increment(1);
    }

    /// Count inbound work that the swarm dropped before the application received it.
    pub(crate) fn record_inbound_shed(&self, class: ServiceClass, reason: ShedReason) {
        self.labeled.shed.get(class).get(reason).increment(1);
    }

    /// Count a forward to the application that did not queue its work as shed.
    ///
    /// A full queue counts as [`ShedReason::QueueFull`]. A queue with no subscriber drops the
    /// work instead of queuing it, so it counts as [`ShedReason::Unsubscribed`]. A closed queue
    /// occurs at the epoch boundary, not under load, so it is not counted. A broadcast failure
    /// is not a full queue, so it is not counted either.
    pub(crate) fn record_forward<T>(
        &self,
        class: ServiceClass,
        forwarded: &Result<TrySendOutcome, TrySendError<T>>,
    ) {
        forwarded
            .as_ref()
            .map_or_else(
                |error| match error {
                    TrySendError::Full(_) => Some(ShedReason::QueueFull),
                    TrySendError::Closed(_) | TrySendError::Broadcast(_) => None,
                },
                |outcome| match outcome {
                    TrySendOutcome::Queued => None,
                    TrySendOutcome::Unsubscribed => Some(ShedReason::Unsubscribed),
                },
            )
            .into_iter()
            .for_each(|reason| self.record_inbound_shed(class, reason));
    }
}

/// Derive-backed handles for peer-manager metrics.
#[derive(Metrics, Clone)]
#[metrics(scope = "tn_network")]
struct PeerManagerMetricHandles {
    /// Effective admission mode: Open = 0, Grace = 1, Closed = 2.
    admission_mode: Gauge,
    /// Resolved current-committee records, including the local identity.
    admission_resolved_current: Gauge,
    /// Minimum authenticated-record quorum for Closed.
    admission_required_current: Gauge,
    /// Connected current peers, excluding the local identity.
    admission_connected_current: Gauge,
    /// Identities with a contradicted PeerId binding. They do not change the effective mode.
    admission_contradicted: Gauge,
    /// Fallback: healthy = 0, missing = 1, stale = 2, contradictory = 3, unresolved = 4.
    admission_fallback: Gauge,
    /// Currently connected peers.
    connected_peers: Gauge,
    /// Peers known with a resolved network record (BLS key -> address).
    known_peers: Gauge,
    /// Peers tracked for discovery.
    discovery_peers: Gauge,
    /// Peers currently banned.
    banned_peers: Gauge,
    /// Connections closed (all directions).
    connections_closed_total: Counter,
    /// Failed dial attempts.
    dial_failures_total: Counter,
    /// 1 once at least one external address is confirmed (NAT traversal possible).
    external_addr_confirmed: Gauge,
    /// Peers banned for bad reputation (flow; `banned_peers` is the stock).
    peers_banned_total: Counter,
}

/// Peer-manager metrics, threaded `ConsensusNetwork::new` -> `TNBehavior::new` ->
/// `PeerManager::new`.
#[derive(Clone, Debug)]
pub(crate) struct PeerManagerMetrics {
    /// The derive-backed handles.
    handles: PeerManagerMetricHandles,
    /// The network label value for per-event labeled counters.
    network: String,
    /// `listen_failures_total` per failure reason.
    listen_failures: KeyedCounters<ListenFailureReason>,
    /// `source_admission_denied_total` per direction and source admission error.
    source_denied: KeyedCounters<(ConnectionDirection, AdmissionError)>,
    /// `closed_admission_denied_total` per direction.
    closed_denied: KeyedCounters<ConnectionDirection>,
}

impl PeerManagerMetrics {
    /// Timestamp the swarm's latest authoritative resolution observation.
    pub(crate) fn mark_committee_observed(&self) {
        let observed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |time| time.as_secs_f64());
        metrics::gauge!("tn_network.committee_observed_timestamp_seconds", "network" => self.network.clone())
            .set(observed);
    }

    /// Publish one of the three fixed committee slots, scoped to this configured swarm.
    pub(crate) fn set_committee_resolution(
        &self,
        slot: &'static str,
        expected: usize,
        unresolved: usize,
    ) {
        let count = |value| u32::try_from(value).map_or(f64::from(u32::MAX), f64::from);
        metrics::gauge!("tn_network.committee_required_members", "network" => self.network.clone(), "slot" => slot)
            .set(count(expected));
        metrics::gauge!("tn_network.committee_unresolved_members", "network" => self.network.clone(), "slot" => slot)
            .set(count(unresolved));
    }

    /// Publish admission inputs independently of connections and node consensus readiness.
    pub(crate) fn set_admission(&self, status: &crate::AdmissionStatus) {
        let mode = match status.effective() {
            tn_config::AdmissionMode::Open => 0.0,
            tn_config::AdmissionMode::Grace => 1.0,
            tn_config::AdmissionMode::Closed => 2.0,
        };
        self.handles.admission_mode.set(mode);
        self.handles
            .admission_resolved_current
            .set(u32::try_from(status.resolved_current()).map_or(f64::MAX, f64::from));
        self.handles
            .admission_required_current
            .set(u32::try_from(status.required_current()).map_or(f64::MAX, f64::from));
        self.handles
            .admission_connected_current
            .set(u32::try_from(status.connected_current()).map_or(f64::MAX, f64::from));
        self.handles
            .admission_contradicted
            .set(u32::try_from(status.contradicted()).map_or(f64::MAX, f64::from));
        self.handles.admission_fallback.set(status.fallback().map_or(0.0, |reason| match reason {
            crate::AdmissionFallback::Missing => 1.0,
            crate::AdmissionFallback::Stale => 2.0,
            crate::AdmissionFallback::Contradictory => 3.0,
            crate::AdmissionFallback::Unresolved => 4.0,
        }));
    }

    /// Record one failed inbound attempt by its bounded reason, never a peer or address.
    pub(crate) fn record_listen_failure(&self, reason: ListenFailureReason) {
        self.listen_failures.increment(reason);
    }

    /// Create the peer manager metric handles for `network_type`. The labeled denial families
    /// register every series at zero.
    pub(crate) fn new_for(network_type: &NetworkType) -> Self {
        let network = network_label(network_type);
        Self {
            handles: PeerManagerMetricHandles::new_with_labels(&[("network", network.clone())]),
            listen_failures: KeyedCounters::new(ListenFailureReason::all(), |reason| {
                metrics::counter!(
                    "tn_network.listen_failures_total",
                    "network" => network.clone(),
                    "reason" => reason.label(),
                )
            }),
            source_denied: KeyedCounters::new(
                ConnectionDirection::ALL.into_iter().flat_map(|direction| {
                    AdmissionError::ALL.into_iter().map(move |error| (direction, error))
                }),
                |(direction, error)| {
                    metrics::counter!(
                        "tn_network.source_admission_denied_total",
                        "network" => network.clone(),
                        "direction" => direction.label(),
                        "reason" => error.label(),
                    )
                },
            ),
            closed_denied: KeyedCounters::new(ConnectionDirection::ALL, |direction| {
                metrics::counter!(
                    "tn_network.closed_admission_denied_total",
                    "network" => network.clone(),
                    "direction" => direction.label(),
                )
            }),
            network,
        }
    }

    /// Update the peer-count gauges (called from the peer manager heartbeat).
    pub(crate) fn set_peer_counts(
        &self,
        connected: usize,
        known: usize,
        discovery: usize,
        banned: usize,
    ) {
        self.handles.connected_peers.set(connected as f64);
        self.handles.known_peers.set(known as f64);
        self.handles.discovery_peers.set(discovery as f64);
        self.handles.banned_peers.set(banned as f64);
    }

    /// Record an established connection by direction ({`in`, `out`}).
    pub(crate) fn record_connection_established(&self, direction: &'static str) {
        metrics::counter!(
            "tn_network.connections_established_total",
            "network" => self.network.clone(),
            "direction" => direction,
        )
        .increment(1);
    }

    /// Record a source-budget denial by direction and source admission error.
    pub(crate) fn record_source_admission_denied(
        &self,
        direction: ConnectionDirection,
        error: AdmissionError,
    ) {
        self.source_denied.increment((direction, error));
    }

    /// Record a Closed admission policy denial by direction.
    pub(crate) fn record_closed_admission_denied(&self, direction: ConnectionDirection) {
        self.closed_denied.increment(direction);
    }

    /// Record a closed connection.
    pub(crate) fn record_connection_closed(&self) {
        self.handles.connections_closed_total.increment(1);
    }

    /// Record a failed dial attempt.
    pub(crate) fn record_dial_failure(&self) {
        self.handles.dial_failures_total.increment(1);
    }

    /// Mark that an external address has been confirmed.
    pub(crate) fn record_external_addr_confirmed(&self) {
        self.handles.external_addr_confirmed.set(1.0);
    }

    /// Record an application-layer penalty by severity, class and scoring outcome.
    pub(crate) fn record_penalty(&self, penalty: &Penalty, outcome: PenaltyOutcome) {
        let severity = match penalty.severity() {
            crate::peers::Severity::Mild => "mild",
            crate::peers::Severity::Medium => "medium",
            crate::peers::Severity::Severe => "severe",
            crate::peers::Severity::Fatal => "fatal",
        };
        let class = if penalty.is_load() { "load" } else { "protocol" };
        let outcome = match outcome {
            PenaltyOutcome::Applied => "applied",
            PenaltyOutcome::Exempt => "exempt",
        };
        metrics::counter!(
            "tn_network.peer_penalties_total",
            "network" => self.network.clone(),
            "severity" => severity,
            "class" => class,
            "outcome" => outcome,
        )
        .increment(1);
    }

    /// Record a reputation ban.
    pub(crate) fn record_peer_banned(&self) {
        self.handles.peers_banned_total.increment(1);
    }

    /// Record the outcome of the per-source inbound kad `PutRecord` rate limiter.
    ///
    /// `Allowed` records nothing. `Shed` and `Flooding` bump the counter with the outcome
    /// labels {`shed`, `flood`}, and [`Self::record_put_record_budget_shed`] adds `budget`:
    /// `shed` counts records the per-source limiter drops without a penalty; `flood` counts
    /// penalty escalations, including repeated penalties above the hard ceiling; `budget`
    /// counts records the cross-source budget drops without a penalty. Operators watch `shed`
    /// to see honest shedding before it reaches the penalty threshold, and `budget` to see
    /// the shared budget bind during a restart or a flood from many sources.
    pub(crate) fn record_put_record_rate_limited(&self, rate: &PutRecordRate) {
        let outcome = match rate {
            PutRecordRate::Allowed => None,
            PutRecordRate::Shed => Some("shed"),
            PutRecordRate::Flooding => Some("flood"),
        };
        outcome.into_iter().for_each(|outcome| {
            metrics::counter!(
                "tn_network.put_records_rate_limited_total",
                "network" => self.network.clone(),
                "outcome" => outcome,
            )
            .increment(1);
        });
    }

    /// Record an inbound kad `PutRecord` shed by the budget that untrusted sources share.
    ///
    /// Bumps the rate-limit counter with the outcome label `budget`. The source is not
    /// penalized.
    pub(crate) fn record_put_record_budget_shed(&self) {
        metrics::counter!(
            "tn_network.put_records_rate_limited_total",
            "network" => self.network.clone(),
            "outcome" => "budget",
        )
        .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Counts beyond u32 retain their magnitude instead of saturating the effective ceiling.
    #[cfg(target_pointer_width = "64")]
    #[test]
    fn quic_metrics_preserve_large_ceilings() -> Result<(), std::num::TryFromIntError> {
        let count = usize::try_from(u64::from(u32::MAX) + 2)?;
        assert_eq!(quic_gauge_value(count), 4_294_967_297.0);
        assert_eq!(quic_gauge_value(usize::MAX), 18_446_744_073_709_551_616.0);
        Ok(())
    }

    /// Recreating a transport retains prior process totals and does not lose the new lifetime.
    #[test]
    fn quic_metrics_accumulate_across_swarm_restarts() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let counter =
                metrics::counter!("tn_network.quic_incoming_retried_total", "network" => "primary");
            let first = QuicCounter::new(counter.clone());
            first.publish(8);
            first.publish(8);
            let restarted = QuicCounter::new(counter);
            restarted.publish(3);
            restarted.publish(5);
        });
        assert!(snapshotter.snapshot().into_vec().iter().any(|(key, _, _, value)| key
            .key()
            .name()
            == "tn_network.quic_incoming_retried_total"
            && matches!(value, DebugValue::Counter(13))));
    }

    /// The transport family has only fixed stage/reason labels and configured swarm labels.
    #[test]
    fn quic_metrics_have_bounded_per_swarm_series() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            [NetworkType::Primary, NetworkType::Worker(0), NetworkType::Worker(1)].iter().for_each(
                |network| {
                    let swarm = SwarmMetrics::new_for(network);
                    let stats = libp2p::quic::IncomingStats::default();
                    (0..1000).for_each(|_| swarm.record_quic_incoming(&stats));
                },
            );
        });
        let snapshot = snapshotter.snapshot().into_vec();
        let outcomes: Vec<_> = snapshot
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == "tn_network.quic_incoming_outcomes_total")
            .collect();
        assert_eq!(outcomes.len(), 3 * libp2p::quic::IncomingOutcome::ALL.len());
        outcomes.iter().for_each(|(key, _, _, value)| {
            assert_eq!(key.key().labels().count(), 4);
            assert!(key.key().labels().all(|label| match label.key() {
                "network" => ["primary", "worker-0", "worker-1"].contains(&label.value()),
                "stage" => ["endpoint", "listener"].contains(&label.value()),
                "decision" => [
                    "accept",
                    "accept_error",
                    "retry",
                    "refuse",
                    "ignore",
                    "validate",
                    "revalidate"
                ]
                .contains(&label.value()),
                "reason" => libp2p::quic::IncomingOutcome::ALL
                    .iter()
                    .any(|outcome| outcome.reason() == label.value()),
                _ => false,
            }));
            assert!(matches!(value, DebugValue::Counter(0)));
        });
    }

    /// Gauge values from a stopped swarm must not retain stale resource occupancy.
    #[test]
    fn quic_metrics_clear_live_gauges_at_shutdown() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let set_live = |swarm: &SwarmMetrics| {
            swarm.handles.quic_incoming_queued.set(2);
            swarm.handles.quic_incoming_limit.set(7);
            swarm.handles.quic_incoming_endpoints.set(1);
            swarm.handles.quic_incoming_handshakes.set(3);
            swarm.set_pending_incoming_connections(4);
        };
        let swarm = metrics::with_local_recorder(&recorder, || {
            let swarm = SwarmMetrics::new_for(&NetworkType::Primary);
            set_live(&swarm);
            swarm
        });
        let observed = || {
            snapshotter
                .snapshot()
                .into_vec()
                .into_iter()
                .filter_map(|(key, _, _, value)| {
                    [
                        "tn_network.quic_incoming_queued",
                        "tn_network.quic_incoming_limit",
                        "tn_network.quic_incoming_endpoints",
                        "tn_network.quic_incoming_handshakes",
                        "tn_network.pending_incoming_connections",
                    ]
                    .contains(&key.key().name())
                    .then_some(value)
                })
                .collect::<Vec<_>>()
        };
        let before = observed();
        assert_eq!(before.len(), 5);
        assert!(
            before.iter().all(|value| matches!(value, DebugValue::Gauge(value) if value.0 > 0.0)),
            "live gauges: {before:?}"
        );
        // DebuggingRecorder consumes gauge values when snapshotting. Refill them so that
        // the post-drop assertion verifies cleanup rather than the snapshot's reset.
        set_live(&swarm);
        drop(swarm);
        let after = observed();
        assert_eq!(after.len(), 5);
        assert!(
            after.iter().all(|value| matches!(value, DebugValue::Gauge(value) if value.0 == 0.0)),
            "stopped gauges: {after:?}"
        );
    }

    /// Capacity observations have only the configured swarm label and track occupancy and shedding.
    #[test]
    fn budget_metrics_record_capacity_occupancy_and_shedding() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let swarm = SwarmMetrics::new_for(&NetworkType::Worker(2))
                .with_capacity(&QuicConfig::default(), None);
            swarm.set_established_connections(7);
            swarm.record_connection_limit_rejection(ConnectionLimitReason::EstablishedTotal);
        });
        let snapshot = snapshotter.snapshot().into_vec();
        let gauge_is = |name, expected| {
            snapshot.iter().any(|(key, _, _, value)| {
                key.key().name() == name
                    && key.key().labels().count() == 1
                    && key
                        .key()
                        .labels()
                        .all(|label| label.key() == "network" && label.value() == "worker-2")
                    && matches!(value, DebugValue::Gauge(value) if value.0 == expected)
            })
        };
        assert!(gauge_is("tn_network.established_connections", 7.0));
        assert!(gauge_is("tn_network.established_connection_limit", 0.0));
        assert!(gauge_is("tn_network.inbound_streams_per_connection_limit", 10_000.0));
        assert!(gauge_is("tn_network.receive_credit_per_connection_bytes", 104_857_600.0));
        assert!(snapshot.iter().any(|(key, _, _, value)| key.key().name()
            == "tn_network.connection_limit_rejections_total"
            && key
                .key()
                .labels()
                .any(|label| label.key() == "reason" && label.value() == "established_total")
            && matches!(value, DebugValue::Counter(1))));
        // every class x reason shed series exists at zero from construction
        let shed_series = snapshot
            .iter()
            .filter(|(key, _, _, value)| {
                key.key().name() == "tn_network.inbound_requests_shed_total"
                    && matches!(value, DebugValue::Counter(0))
            })
            .count();
        assert_eq!(shed_series, ServiceClass::ALL.len() * ShedReason::ALL.len());
    }

    /// Forward outcomes count under their shed reasons, and the admission handle adds to the
    /// primary swarm's epoch-record series.
    #[test]
    fn shed_reasons_count_per_class() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            let swarm = SwarmMetrics::new_for(&NetworkType::Primary);
            swarm.record_forward::<()>(ServiceClass::Gossip, &Ok(TrySendOutcome::Unsubscribed));
            swarm.record_forward::<()>(ServiceClass::Vote, &Ok(TrySendOutcome::Queued));
            swarm.record_forward(ServiceClass::Vote, &Err(TrySendError::Full(())));
            swarm.record_forward(ServiceClass::Vote, &Err(TrySendError::Closed(())));
            AdmissionShed::epoch_record().record();
        });
        let snapshot = snapshotter.snapshot().into_vec();
        let shed = |class: &str, reason: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == "tn_network.inbound_requests_shed_total"
                        && key.key().labels().any(|l| l.key() == "class" && l.value() == class)
                        && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
                })
                .map(|(_, _, _, value)| value)
        };
        assert!(matches!(shed("gossip", "unsubscribed"), Some(DebugValue::Counter(1))));
        assert!(matches!(shed("vote", "queue_full"), Some(DebugValue::Counter(1))));
        assert!(matches!(shed("vote", "unsubscribed"), Some(DebugValue::Counter(0))));
        assert!(matches!(shed("epoch_record", "admission"), Some(DebugValue::Counter(1))));
    }

    /// Primary and worker metrics register their expected labels and update every handle.
    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let swarm = SwarmMetrics::new_for(&NetworkType::Primary);
            swarm.record_gossip_published();
            swarm.record_gossip_publish_queue_full();
            swarm.record_gossip_received();
            swarm.record_gossip_rejected();
            swarm.set_pending(1, 2);
            swarm.record_outbound_failure(&OutboundFailure::Timeout);
            swarm.record_exchange("deferred");
            swarm.set_record_exchange_pending(2, 3);
            swarm.record_inbound_denied(&InboundDenial::PendingIncomingLimit);

            let peers = PeerManagerMetrics::new_for(&NetworkType::Worker(0));
            peers.set_peer_counts(4, 10, 3, 1);
            peers.record_connection_established("in");
            peers.record_connection_closed();
            peers.record_dial_failure();
            peers.record_external_addr_confirmed();
            peers.record_penalty(&Penalty::Severe, PenaltyOutcome::Applied);
            peers.record_peer_banned();
            peers.record_put_record_rate_limited(&PutRecordRate::Shed);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (key, _, _, value) = find("tn_network.gossip_published_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "network" && l.value() == "primary"));

        let (key, _, _, value) = find("tn_network.gossip_publish_queue_full_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "network" && l.value() == "primary"));

        let (key, _, _, value) = find("tn_network.connected_peers");
        assert!(matches!(value, DebugValue::Gauge(g) if g.0 == 4.0));
        assert!(key.key().labels().any(|l| l.key() == "network" && l.value() == "worker-0"));

        let outbound = snapshot.iter().find(|(key, ..)| {
            key.key().name() == "tn_network.outbound_request_failures_total"
                && key.key().labels().any(|l| l.key() == "kind" && l.value() == "timeout")
        });
        assert!(matches!(outbound, Some((_, _, _, DebugValue::Counter(1)))));

        // every bound has its own series from construction, so find the refused bound by label
        let denied = snapshot.iter().find(|(key, ..)| {
            key.key().name() == "tn_network.inbound_connections_denied_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "reason" && l.value() == "pending_incoming_limit")
        });
        assert!(matches!(denied, Some((_, _, _, DebugValue::Counter(1)))));

        let (key, _, _, value) = find("tn_network.record_exchange_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "outcome" && l.value() == "deferred"));
        let (_, _, _, pending) = find("tn_network.record_exchange_pending");
        assert!(matches!(pending, DebugValue::Gauge(g) if g.0 == 2.0));
        let (_, _, _, deferred) = find("tn_network.record_exchange_deferred");
        assert!(matches!(deferred, DebugValue::Gauge(g) if g.0 == 3.0));

        let (key, _, _, _) = find("tn_network.peer_penalties_total");
        assert!(key.key().labels().any(|l| l.key() == "severity" && l.value() == "severe"));

        let (key, _, _, _) = find("tn_network.connections_established_total");
        assert!(key.key().labels().any(|l| l.key() == "direction" && l.value() == "in"));

        let (key, _, _, value) = find("tn_network.put_records_rate_limited_total");
        assert!(matches!(value, DebugValue::Counter(1)));
        assert!(key.key().labels().any(|l| l.key() == "outcome" && l.value() == "shed"));

        find("tn_network.px_disconnects_pending");
        find("tn_network.outbound_requests_pending");
        find("tn_network.banned_peers");
        find("tn_network.peers_banned_total");
        find("tn_network.external_addr_confirmed");
        find("tn_network.dial_failures_total");
        find("tn_network.connections_closed_total");
    }

    /// Worker swarms must retain independent gauges and counters in the shared recorder.
    #[test]
    fn test_worker_metrics_are_isolated() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let first = SwarmMetrics::new_for(&NetworkType::Worker(0));
            let second = SwarmMetrics::new_for(&NetworkType::Worker(1));
            first.set_pending(3, 3);
            second.set_pending(7, 7);
            [&first, &second].into_iter().for_each(|swarm| {
                swarm.record_gossip_published();
                swarm.record_outbound_failure(&OutboundFailure::Timeout);
                swarm.record_inbound_denied(&InboundDenial::PendingIncomingLimit);
            });

            let first = PeerManagerMetrics::new_for(&NetworkType::Worker(0));
            let second = PeerManagerMetrics::new_for(&NetworkType::Worker(1));
            first.set_peer_counts(3, 3, 3, 3);
            second.set_peer_counts(7, 7, 7, 7);
            [&first, &second].into_iter().for_each(|peers| {
                peers.record_connection_established("in");
                peers.record_penalty(&Penalty::Severe, PenaltyOutcome::Applied);
            });
        });

        let snapshot = snapshotter.snapshot().into_vec();
        [("worker-0", 3.0), ("worker-1", 7.0)].into_iter().for_each(|(network, expected)| {
            let value = |name| {
                snapshot
                    .iter()
                    .find(|(key, ..)| {
                        key.key().name() == name
                            && key
                                .key()
                                .labels()
                                .any(|label| label.key() == "network" && label.value() == network)
                    })
                    .map(|(_, _, _, value)| value)
            };
            [
                "tn_network.px_disconnects_pending",
                "tn_network.outbound_requests_pending",
                "tn_network.connected_peers",
                "tn_network.known_peers",
                "tn_network.discovery_peers",
                "tn_network.banned_peers",
            ]
            .into_iter()
            .for_each(|name| {
                assert!(
                    matches!(value(name), Some(DebugValue::Gauge(g)) if g.0 == expected),
                    "{name} must retain {network}'s gauge value"
                );
            });
            [
                "tn_network.gossip_published_total",
                "tn_network.connections_established_total",
                "tn_network.peer_penalties_total",
            ]
            .into_iter()
            .for_each(|name| {
                assert!(
                    matches!(value(name), Some(DebugValue::Counter(1))),
                    "{name} must count {network}'s events separately"
                );
            });
            let outbound = snapshot.iter().find(|(key, ..)| {
                key.key().name() == "tn_network.outbound_request_failures_total"
                    && key.key().labels().any(|l| l.key() == "network" && l.value() == network)
                    && key.key().labels().any(|l| l.key() == "kind" && l.value() == "timeout")
            });
            assert!(matches!(outbound, Some((_, _, _, DebugValue::Counter(1)))));
            let denied = snapshot.iter().find(|(key, ..)| {
                key.key().name() == "tn_network.inbound_connections_denied_total"
                    && key.key().labels().any(|l| l.key() == "network" && l.value() == network)
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "reason" && l.value() == "pending_incoming_limit")
            });
            assert!(
                matches!(denied, Some((_, _, _, DebugValue::Counter(1)))),
                "inbound_connections_denied_total must count {network}'s refusals separately"
            );
        });
    }

    /// Every libp2p-connection-limits 0.7.0 refusal text maps to its own reason label, and an
    /// unknown text maps to `unknown`.
    #[test]
    fn connection_limit_reason_pins_libp2p_text() {
        let cases = [
            ("pending incoming connections", "pending_incoming"),
            ("pending outgoing connections", "pending_outgoing"),
            ("established incoming connections", "established_incoming"),
            ("established outgoing connections", "established_outgoing"),
            ("established connections per peer", "established_per_peer"),
            ("established connections", "established_total"),
        ];
        cases.iter().for_each(|(kind, label)| {
            let text = format!("connection limit exceeded: at most 3 {kind} are allowed");
            assert_eq!(super::ConnectionLimitReason::from_text(&text).label(), *label);
        });
        assert_eq!(super::ConnectionLimitReason::from_text("other text").label(), "unknown");
    }

    /// Every counter in the recorder as `(key, value)` pairs.
    fn counters(recorder: &DebuggingRecorder) -> Vec<(metrics::Key, u64)> {
        recorder
            .snapshotter()
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, _, _, value)| match value {
                DebugValue::Counter(count) => Some((key.key().clone(), count)),
                DebugValue::Gauge(_) | DebugValue::Histogram(_) => None,
            })
            .collect()
    }

    /// The value of the counter `name` whose labels include every pair in `labels`.
    fn count(counters: &[(metrics::Key, u64)], name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        counters
            .iter()
            .find(|(key, _)| {
                key.name() == name
                    && labels.iter().all(|(label, value)| {
                        key.labels().any(|l| l.key() == *label && l.value() == *value)
                    })
            })
            .map(|(_, count)| *count)
    }

    /// The sum of every series named `name`.
    fn total(counters: &[(metrics::Key, u64)], name: &str) -> u64 {
        counters.iter().filter(|(key, _)| key.name() == name).map(|(_, count)| count).sum()
    }

    /// The number of series named `name`.
    fn series(counters: &[(metrics::Key, u64)], name: &str) -> usize {
        counters.iter().filter(|(key, _)| key.name() == name).count()
    }

    /// Each inbound rejection, `unknown` included, moves only its own `{reason, stage}` series.
    #[test]
    fn inbound_rejection_moves_only_its_own_series() {
        let recorder = DebuggingRecorder::new();
        let swarm = metrics::with_local_recorder(&recorder, || {
            SwarmMetrics::new_for(&NetworkType::Primary)
        });
        let name = "tn_network.inbound_connections_denied_total";

        InboundRejection::all().for_each(|rejection| {
            let before = counters(&recorder);
            let recorded = match rejection {
                // a connection_limits::Exceeded cannot be built outside libp2p
                InboundRejection::Limit(denial) => {
                    swarm.record_inbound_denied(&denial);
                    rejection
                }
                InboundRejection::PeerManager(denied) => {
                    swarm.record_inbound_rejection(&ConnectionDenied::new(denied))
                }
                InboundRejection::Source(error) => {
                    swarm.record_inbound_rejection(&ConnectionDenied::new(error))
                }
                InboundRejection::Unknown => swarm
                    .record_inbound_rejection(&ConnectionDenied::new(io::Error::other("unknown"))),
            };
            assert_eq!(recorded, rejection, "the cause must classify as {rejection:?}");

            let after = counters(&recorder);
            let moved: Vec<_> = after
                .iter()
                .filter(|(key, count)| {
                    key.name() == name
                        && before.iter().find(|(old, _)| old == key).map_or(0, |(_, old)| *old)
                            != *count
                })
                .collect();
            assert_eq!(moved.len(), 1, "{rejection:?} must move exactly one series");
            assert_eq!(
                count(
                    &after,
                    name,
                    &[("reason", rejection.label()), ("stage", rejection.stage().label())]
                ),
                Some(1),
                "{rejection:?} must count under its own reason and stage"
            );
            assert_eq!(series(&after, name), InboundRejection::all().count());
        });
    }

    /// Each listener error kind moves only its reason, a kind outside the table counts as
    /// `unknown`, and each listener close counts by its outcome.
    #[test]
    fn listener_error_and_close_series() {
        let recorder = DebuggingRecorder::new();
        let swarm = metrics::with_local_recorder(&recorder, || {
            SwarmMetrics::new_for(&NetworkType::Primary)
        });
        let errors = "tn_network.listener_errors_total";

        ListenerErrorReason::KINDS
            .into_iter()
            .chain(std::iter::once((io::ErrorKind::WouldBlock, ListenerErrorReason::Unknown)))
            .for_each(|(kind, reason)| {
                let before = counters(&recorder);
                let own = count(&before, errors, &[("reason", reason.label())]);
                assert_eq!(swarm.record_listener_error(&io::Error::from(kind)), reason);
                let after = counters(&recorder);
                assert_eq!(
                    count(&after, errors, &[("reason", reason.label())]),
                    own.map(|value| value + 1),
                    "{kind:?} must count as {}",
                    reason.label()
                );
                assert_eq!(total(&after, errors), total(&before, errors) + 1);
                assert_eq!(series(&after, errors), ListenerErrorReason::ALL.len());
            });

        let closed = "tn_network.listeners_closed_total";
        swarm.record_listener_closed(&Ok(()));
        swarm.record_listener_closed(&Err(io::Error::other("listener failed")));
        swarm.record_listener_closed(&Err(io::Error::other("listener failed")));
        let after = counters(&recorder);
        assert_eq!(count(&after, closed, &[("outcome", "ok")]), Some(1));
        assert_eq!(count(&after, closed, &[("outcome", "error")]), Some(2));
        assert_eq!(series(&after, closed), ListenerCloseOutcome::ALL.len());
    }

    /// One propagated peer-manager denial adds one to its stage counter and one to the terminal
    /// counter.
    #[test]
    fn propagated_denial_counts_stage_and_terminal_once() {
        let recorder = DebuggingRecorder::new();
        let (swarm, peers) = metrics::with_local_recorder(&recorder, || {
            (
                SwarmMetrics::new_for(&NetworkType::Primary),
                PeerManagerMetrics::new_for(&NetworkType::Primary),
            )
        });

        // the peer manager counts the stage, then the swarm reports the same denial once
        peers.record_closed_admission_denied(ConnectionDirection::Inbound);
        let rejection = swarm
            .record_inbound_rejection(&ConnectionDenied::new(PeerAdmissionDenied::ClosedAdmission));

        let after = counters(&recorder);
        let stage = "tn_network.closed_admission_denied_total";
        let terminal = "tn_network.inbound_connections_denied_total";
        assert_eq!(rejection.stage(), RejectionStage::AuthenticatedPolicy);
        assert_eq!(count(&after, stage, &[("direction", "in")]), Some(1));
        assert_eq!(total(&after, stage), 1);
        assert_eq!(
            count(
                &after,
                terminal,
                &[("reason", "peer_manager_closed_admission"), ("stage", "authenticated_policy")]
            ),
            Some(1)
        );
        assert_eq!(total(&after, terminal), 1);
    }

    /// Many events from many random peers keep the series count fixed while every event counts.
    #[test]
    fn many_events_keep_series_count_fixed() {
        let recorder = DebuggingRecorder::new();
        let (swarm, peers) = metrics::with_local_recorder(&recorder, || {
            (
                SwarmMetrics::new_for(&NetworkType::Primary),
                PeerManagerMetrics::new_for(&NetworkType::Primary),
            )
        });
        let before = counters(&recorder);
        let events = 64_u64;

        (0..events).for_each(|attempt| {
            // peer and attempt data in the error text must never become a label
            let peer = libp2p::PeerId::random();
            swarm.record_inbound_rejection(&ConnectionDenied::new(io::Error::other(format!(
                "peer {peer} attempt {attempt}"
            ))));
            swarm.record_listener_error(&io::Error::other(format!("listener error for {peer}")));
            peers.record_listen_failure(ListenFailureReason::PeerManager(
                PeerAdmissionDenied::BannedPeer,
            ));
            peers.record_source_admission_denied(
                ConnectionDirection::Inbound,
                AdmissionError::PeerFull,
            );
            peers.record_closed_admission_denied(ConnectionDirection::Outbound);
        });

        let after = counters(&recorder);
        assert_eq!(after.len(), before.len(), "no event may add a series");
        [
            ("tn_network.inbound_connections_denied_total", ("reason", "unknown")),
            ("tn_network.listener_errors_total", ("reason", "other")),
            ("tn_network.listen_failures_total", ("reason", "peer_manager_banned_peer")),
            ("tn_network.source_admission_denied_total", ("reason", "peer_full")),
            ("tn_network.closed_admission_denied_total", ("direction", "out")),
        ]
        .into_iter()
        .for_each(|(name, label)| {
            assert_eq!(
                count(&after, name, &[label]),
                Some(events),
                "{name} must count every event"
            );
        });
    }

    /// Primary and worker swarms keep separate series for the new rejection and listener
    /// families.
    #[test]
    fn new_families_keep_primary_and_worker_series_separate() {
        let recorder = DebuggingRecorder::new();
        // the primary records each event once, the worker twice
        let networks =
            [(NetworkType::Primary, "primary", 1_u64), (NetworkType::Worker(0), "worker-0", 2)];
        let handles: Vec<_> = metrics::with_local_recorder(&recorder, || {
            networks
                .iter()
                .map(|(network, _, _)| {
                    (SwarmMetrics::new_for(network), PeerManagerMetrics::new_for(network))
                })
                .collect()
        });

        handles.iter().zip(networks.iter()).for_each(|((swarm, peers), (_, _, times))| {
            (0..*times).for_each(|_| {
                swarm.record_inbound_rejection(&ConnectionDenied::new(
                    PeerAdmissionDenied::BannedPeer,
                ));
                swarm.record_listener_error(&io::Error::from(io::ErrorKind::AddrInUse));
                swarm.record_listener_closed(&Ok(()));
                peers.record_listen_failure(ListenFailureReason::Aborted);
                peers.record_source_admission_denied(
                    ConnectionDirection::Outbound,
                    AdmissionError::PeerFull,
                );
                peers.record_closed_admission_denied(ConnectionDirection::Inbound);
            });
        });

        // every direction registers `peer_full` at zero, so that query names the recorded
        // direction; `count` returns the first match in registration order
        let families: [(&str, &[(&str, &str)]); 6] = [
            (
                "tn_network.inbound_connections_denied_total",
                &[("reason", "peer_manager_banned_peer")],
            ),
            ("tn_network.listener_errors_total", &[("reason", "address_in_use")]),
            ("tn_network.listeners_closed_total", &[("outcome", "ok")]),
            ("tn_network.listen_failures_total", &[("reason", "aborted")]),
            (
                "tn_network.source_admission_denied_total",
                &[("direction", "out"), ("reason", "peer_full")],
            ),
            ("tn_network.closed_admission_denied_total", &[("direction", "in")]),
        ];
        let after = counters(&recorder);
        networks.iter().for_each(|(_, network, times)| {
            families.iter().for_each(|(name, labels)| {
                let labels: Vec<_> =
                    std::iter::once(("network", *network)).chain(labels.iter().copied()).collect();
                assert_eq!(
                    count(&after, name, &labels),
                    Some(*times),
                    "{name} must count {network}'s events separately"
                );
            });
        });
    }
}
