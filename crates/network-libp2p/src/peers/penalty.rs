//! Peer penalties classified independently of admission and retention privileges.

/// A temporary service or transport load signal, without evidence of a protocol violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPenalty {
    /// An outbound operation timed out or suffered a transient transport failure.
    Timeout,
    /// A request raced chain or consensus data synchronization.
    Synchronization,
    /// A header or batch belongs to an obsolete committee window.
    EpochBoundary,
    /// A primary request suffered a transient transport failure.
    Transport,
    /// Gossip delivery could not keep up with the sender.
    SlowPeer,
    /// The peer exceeded the inbound stream budget.
    StreamRateLimit,
    /// The peer exceeded the Kademlia provider-record budget.
    KademliaRateLimit,
    /// The peer exceeded the Kademlia put-record flood threshold.
    KademliaFlood,
}

/// Penalties applied to an attributable peer.
///
/// The severity-only variants describe protocol or application validation failures. Callers
/// must use [`Self::Load`] for temporary overload so trust never suppresses protocol failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Penalty {
    /// A minor protocol or application validation failure, scored at -1.
    Mild,
    /// A protocol or application validation failure, scored at -5.
    Medium,
    /// A serious protocol or application validation failure, scored at -10.
    Severe,
    /// An unforgivable protocol or cryptographic failure, setting the minimum score.
    Fatal,
    /// Temporary overload, scored only when the peer has no load-scoring exemption.
    Load(LoadPenalty),
}

/// Score impact and telemetry severity, independent of the penalty's cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Severity {
    /// Score decrement of one.
    Mild,
    /// Score decrement of five.
    Medium,
    /// Score decrement of ten.
    Severe,
    /// Set the minimum score.
    Fatal,
}

impl Penalty {
    /// Whether this is a temporary load signal rather than a protocol violation.
    pub(super) fn is_load(self) -> bool {
        matches!(self, Self::Load(_))
    }

    /// The score impact of this penalty, retaining the existing overload severity weights.
    pub(crate) fn severity(self) -> Severity {
        match self {
            Self::Mild
            | Self::Load(
                LoadPenalty::Timeout | LoadPenalty::SlowPeer | LoadPenalty::Synchronization,
            ) => Severity::Mild,
            Self::Medium
            | Self::Load(
                LoadPenalty::StreamRateLimit
                | LoadPenalty::KademliaRateLimit
                | LoadPenalty::EpochBoundary
                | LoadPenalty::Transport,
            ) => Severity::Medium,
            Self::Severe | Self::Load(LoadPenalty::KademliaFlood) => Severity::Severe,
            Self::Fatal => Severity::Fatal,
        }
    }
}

/// Whether score history can be forgiven when a peer acquires a new trust basis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) enum PenaltyHistory {
    /// Only load penalties have been observed, or no penalties have been observed.
    #[default]
    LoadOnly,
    /// A protocol violation has been observed; trust changes must preserve its score and ban.
    Protocol,
}

impl PenaltyHistory {
    /// Preserve either record's protocol history when network identities are merged.
    pub(super) fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::LoadOnly, Self::LoadOnly) => Self::LoadOnly,
            (Self::Protocol, Self::LoadOnly | Self::Protocol)
            | (Self::LoadOnly, Self::Protocol) => Self::Protocol,
        }
    }

    /// Remember protocol failures across load signals and committee updates.
    pub(super) fn record(&mut self, penalty: Penalty) {
        if !penalty.is_load() {
            *self = Self::Protocol;
        }
    }

    /// Whether trust changes may reset this peer's score and forgive its ban.
    pub(super) fn permits_forgiveness(self) -> bool {
        self == Self::LoadOnly
    }
}

#[cfg(test)]
mod tests {
    use super::{LoadPenalty, Penalty, PenaltyHistory, Severity};

    /// Load signals preserve the previous severity weights without becoming protocol failures.
    #[test]
    fn load_penalty_classification() {
        [
            (LoadPenalty::Timeout, Severity::Mild),
            (LoadPenalty::SlowPeer, Severity::Mild),
            (LoadPenalty::StreamRateLimit, Severity::Medium),
            (LoadPenalty::KademliaRateLimit, Severity::Medium),
            (LoadPenalty::KademliaFlood, Severity::Severe),
        ]
        .into_iter()
        .for_each(|(cause, severity)| {
            let penalty = Penalty::Load(cause);
            assert!(penalty.is_load());
            assert_eq!(penalty.severity(), severity);
        });
        [Penalty::Mild, Penalty::Medium, Penalty::Severe, Penalty::Fatal]
            .into_iter()
            .for_each(|penalty| assert!(!penalty.is_load()));
    }

    /// A later load signal or trust grant cannot erase a recorded protocol violation.
    #[test]
    fn protocol_history_survives_load_signals() {
        let mut history = PenaltyHistory::default();
        history.record(Penalty::Load(LoadPenalty::Timeout));
        assert!(history.permits_forgiveness());
        history.record(Penalty::Fatal);
        history.record(Penalty::Load(LoadPenalty::KademliaFlood));
        assert!(!history.permits_forgiveness());
        assert!(!PenaltyHistory::LoadOnly.merge(history).permits_forgiveness());
        assert!(!history.merge(PenaltyHistory::LoadOnly).permits_forgiveness());
        assert!(PenaltyHistory::LoadOnly.merge(PenaltyHistory::LoadOnly).permits_forgiveness());
    }
}
