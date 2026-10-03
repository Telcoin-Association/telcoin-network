//! Service classes for inbound network work, the reasons to shed that work, and the pending
//! inbound occupancy of one swarm by class.
//!
//! The class set and the reason set are closed, so every metric series with a class or reason
//! label has a fixed cardinality. The classes label metrics only. They do not change how the
//! swarm forwards, schedules, or sheds work.

#[cfg(test)]
#[path = "tests/service_class_tests.rs"]
mod service_class_tests;

/// The consensus work that an inbound request or gossip message serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ServiceClass {
    /// A vote request for a header.
    Vote,
    /// A request for an epoch record.
    EpochRecord,
    /// Certificate catch-up for a peer that is behind.
    CertificateSync,
    /// A batch report between workers.
    Batch,
    /// Gossip that the swarm forwards to the application.
    Gossip,
    /// All other inbound work.
    Other,
}

impl ServiceClass {
    /// Every class, in label order.
    pub const ALL: [Self; 6] = [
        Self::Vote,
        Self::EpochRecord,
        Self::CertificateSync,
        Self::Batch,
        Self::Gossip,
        Self::Other,
    ];

    /// The metric label value for this class.
    pub fn label(self) -> &'static str {
        match self {
            Self::Vote => "vote",
            Self::EpochRecord => "epoch_record",
            Self::CertificateSync => "certificate_sync",
            Self::Batch => "batch",
            Self::Gossip => "gossip",
            Self::Other => "other",
        }
    }
}

/// The reason that the swarm dropped inbound work before the application received it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShedReason {
    /// The bounded application event queue was full.
    QueueFull,
}

impl ShedReason {
    /// Every reason, in label order.
    pub(crate) const ALL: [Self; 1] = [Self::QueueFull];

    /// The metric label value for this reason.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
        }
    }
}

/// The pending inbound requests of one swarm, counted by class.
///
/// A request is pending from the time that the swarm forwards it to the application until the
/// swarm sends the response or the request fails. The swarm adds a request once when it stores
/// the `inbound_requests` entry and releases it once when it removes that entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct InboundOccupancy {
    /// Pending vote requests.
    vote: u32,
    /// Pending epoch record requests.
    epoch_record: u32,
    /// Pending certificate catch-up requests.
    certificate_sync: u32,
    /// Pending batch requests.
    batch: u32,
    /// Pending gossip. Gossip has no response, so this stays zero.
    gossip: u32,
    /// Pending requests of all other classes.
    other: u32,
}

impl InboundOccupancy {
    /// The pending requests of `class`.
    pub(crate) fn pending(&self, class: ServiceClass) -> u32 {
        match class {
            ServiceClass::Vote => self.vote,
            ServiceClass::EpochRecord => self.epoch_record,
            ServiceClass::CertificateSync => self.certificate_sync,
            ServiceClass::Batch => self.batch,
            ServiceClass::Gossip => self.gossip,
            ServiceClass::Other => self.other,
        }
    }

    /// Return this occupancy with one more pending request of `class`.
    pub(crate) fn added(self, class: ServiceClass) -> Self {
        self.map(class, |pending| pending.saturating_add(1))
    }

    /// Return this occupancy with one less pending request of `class`.
    pub(crate) fn released(self, class: ServiceClass) -> Self {
        self.map(class, |pending| pending.saturating_sub(1))
    }

    /// Return this occupancy with `f` applied to the count of `class`.
    fn map(self, class: ServiceClass, f: impl FnOnce(u32) -> u32) -> Self {
        match class {
            ServiceClass::Vote => Self { vote: f(self.vote), ..self },
            ServiceClass::EpochRecord => Self { epoch_record: f(self.epoch_record), ..self },
            ServiceClass::CertificateSync => {
                Self { certificate_sync: f(self.certificate_sync), ..self }
            }
            ServiceClass::Batch => Self { batch: f(self.batch), ..self },
            ServiceClass::Gossip => Self { gossip: f(self.gossip), ..self },
            ServiceClass::Other => Self { other: f(self.other), ..self },
        }
    }
}
