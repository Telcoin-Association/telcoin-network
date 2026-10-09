//! Prometheus metrics for the libp2p consensus networks.
//!
//! Both the primary and worker networks instantiate the same types, so every series
//! carries a `network` label (`primary` or `worker-{id}`) set at construction.

use crate::{
    peers::{Penalty, PutRecordRate},
    service_class::{ServiceClass, ShedReason},
    types::NetworkType,
};
use libp2p::{
    connection_limits,
    request_response::{InboundFailure, OutboundFailure},
};
use reth_metrics::{
    metrics::{Counter, Gauge, Histogram},
    Metrics,
};
use std::{fmt, time::Duration};
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
    /// Gossip messages received from peers.
    gossip_received_total: Counter,
    /// Gossip messages rejected (failed verification against authorized publishers).
    gossip_rejected_total: Counter,
    /// Inbound provider announcements dropped by the per-source rate limit.
    add_provider_rate_limited_total: Counter,
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
    /// Incoming QUIC attempts refused with a connection close: refused by the listener or
    /// failed in the quinn accept.
    quic_incoming_refused_total: Counter,
    /// Incoming QUIC attempts dropped without a reply.
    quic_incoming_ignored_total: Counter,
    /// Times the QUIC listener yielded after its per-poll outcome cap.
    quic_incoming_budget_yields_total: Counter,
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
struct PerClass<T> {
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
    fn new(resolve: impl Fn(ServiceClass) -> T) -> Self {
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
    fn get(&self, class: ServiceClass) -> &T {
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
}

impl<T> PerShedReason<T> {
    /// Resolve one handle per reason with `resolve`.
    fn new(resolve: impl Fn(ShedReason) -> T) -> Self {
        Self {
            queue_full: resolve(ShedReason::QueueFull),
            unsubscribed: resolve(ShedReason::Unsubscribed),
            admission: resolve(ShedReason::Admission),
        }
    }

    /// The handle for `reason`.
    fn get(&self, reason: ShedReason) -> &T {
        match reason {
            ShedReason::QueueFull => &self.queue_full,
            ShedReason::Unsubscribed => &self.unsubscribed,
            ShedReason::Admission => &self.admission,
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

/// One pre-resolved metric handle per [`InboundDenial`].
#[derive(Clone)]
struct PerDenial<T> {
    /// The handle for [`InboundDenial::PendingIncomingLimit`].
    pending_incoming: T,
    /// The handle for [`InboundDenial::EstablishedPerPeerLimit`].
    established_per_peer: T,
    /// The handle for [`InboundDenial::EstablishedTotalLimit`].
    established_total: T,
    /// The handle for [`InboundDenial::Other`].
    other: T,
}

impl<T> PerDenial<T> {
    /// Resolve one handle per denial with `resolve`.
    fn new(resolve: impl Fn(InboundDenial) -> T) -> Self {
        Self {
            pending_incoming: resolve(InboundDenial::PendingIncomingLimit),
            established_per_peer: resolve(InboundDenial::EstablishedPerPeerLimit),
            established_total: resolve(InboundDenial::EstablishedTotalLimit),
            other: resolve(InboundDenial::Other),
        }
    }

    /// The handle for `denial`.
    fn get(&self, denial: InboundDenial) -> &T {
        match denial {
            InboundDenial::PendingIncomingLimit => &self.pending_incoming,
            InboundDenial::EstablishedPerPeerLimit => &self.established_per_peer,
            InboundDenial::EstablishedTotalLimit => &self.established_total,
            InboundDenial::Other => &self.other,
        }
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
    /// `inbound_connections_denied_total` per bound.
    inbound_denied: PerDenial<Counter>,
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
            inbound_denied: PerDenial::new(|denial| {
                metrics::counter!(
                    "tn_network.inbound_connections_denied_total",
                    "network" => network.to_owned(),
                    "reason" => denial.label(),
                )
            }),
        }
        .registered()
    }

    /// Register every gauge and counter at zero. The histograms register when resolved.
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
        InboundDenial::ALL.iter().for_each(|denial| self.inbound_denied.get(*denial).increment(0));
        self
    }
}

/// Resolve the `inbound_requests_shed_total` series for one network label, class and reason.
fn shed_counter(network: &str, class: ServiceClass, reason: ShedReason) -> Counter {
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
    /// The configured swarm label for record-exchange events.
    network: String,
    /// The derive-backed handles.
    handles: SwarmMetricHandles,
    /// The labeled handles, resolved at construction.
    labeled: LabeledHandles,
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
        Self {
            handles: SwarmMetricHandles::new_with_labels(&[("network", network.clone())]),
            labeled: LabeledHandles::new(&network),
            network,
        }
    }

    /// Record a successfully published gossip message.
    pub(crate) fn record_gossip_published(&self) {
        self.handles.gossip_published_total.increment(1);
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

    /// Update the in-flight request gauges (called once per event-loop iteration).
    pub(crate) fn set_pending(&self, px_disconnects: usize, outbound_requests: usize) {
        self.handles.px_disconnects_pending.set(px_disconnects as f64);
        self.handles.outbound_requests_pending.set(outbound_requests as f64);
    }

    /// Mirror the QUIC listener decision counters (absolute values; called once per
    /// event-loop iteration). No address labels.
    pub(crate) fn record_quic_incoming(&self, stats: &libp2p::quic::IncomingStats) {
        self.handles.quic_incoming_retried_total.absolute(stats.retried());
        self.handles.quic_incoming_accepted_total.absolute(stats.accepted());
        self.handles.quic_incoming_refused_total.absolute(stats.refused());
        self.handles.quic_incoming_ignored_total.absolute(stats.ignored());
        self.handles.quic_incoming_budget_yields_total.absolute(stats.budget_yields());
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

    /// Record an inbound connection refused by a `connection_limits` bound, by bound.
    pub(crate) fn record_inbound_denied(&self, denial: &InboundDenial) {
        self.labeled.inbound_denied.get(*denial).increment(1);
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
}

impl PeerManagerMetrics {
    /// Record one failed inbound attempt with a fixed reason label, never a peer or address.
    pub(crate) fn record_listen_failure(&self, reason: &'static str) {
        metrics::counter!(
            "tn_network.listen_failures_total",
            "network" => self.network.clone(),
            "reason" => reason,
        )
        .increment(1);
    }

    /// Create the peer manager metric handles for `network_type`.
    pub(crate) fn new_for(network_type: &NetworkType) -> Self {
        let network = network_label(network_type);
        Self {
            handles: PeerManagerMetricHandles::new_with_labels(&[("network", network.clone())]),
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

    /// Record a source-budget denial by direction ({`in`, `out`}) and reason.
    pub(crate) fn record_source_admission_denied(
        &self,
        direction: &'static str,
        reason: &'static str,
    ) {
        metrics::counter!(
            "tn_network.source_admission_denied_total",
            "network" => self.network.clone(),
            "direction" => direction,
            "reason" => reason,
        )
        .increment(1);
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

    /// Record an application-layer penalty by severity.
    pub(crate) fn record_penalty(&self, penalty: &Penalty) {
        let severity = match penalty.severity() {
            crate::peers::Severity::Mild => "mild",
            crate::peers::Severity::Medium => "medium",
            crate::peers::Severity::Severe => "severe",
            crate::peers::Severity::Fatal => "fatal",
        };
        metrics::counter!(
            "tn_network.peer_penalties_total",
            "network" => self.network.clone(),
            "severity" => severity,
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
    /// labels {`shed`, `flood`}: `shed` counts records dropped without a penalty; `flood`
    /// counts penalty escalations, including repeated penalties above the hard ceiling.
    /// Operators watch `shed` to see honest shedding before it reaches the penalty threshold.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

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
            peers.record_penalty(&Penalty::Severe);
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
                peers.record_penalty(&Penalty::Severe);
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
}
