//! Independent peer privileges, composed from live trust bases.
//!
//! These privileges never change connection, stream, memory, or message-class budgets.

use super::penalty::Penalty;

/// Eligibility for admission to an operator-configured topology.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Admission {
    /// No configured trust basis; ordinary public discovery policy still applies.
    #[default]
    Discovery,
    /// Authorized by operator configuration or a tracked committee slot.
    Authorized,
}

/// Treatment by ordinary population pruning and gossip mesh selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Retention {
    /// Subject to ordinary population pruning and mesh selection.
    #[default]
    Ordinary,
    /// Retained as an explicit peer, without bypassing any resource budget or ban.
    Protected,
}

/// Treatment of temporary overload in the application score model.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum LoadScoring {
    /// Apply load-induced penalties.
    #[default]
    Apply,
    /// Shed excess work without reducing the peer's score for load alone.
    Exempt,
}

/// A live source of peer privileges; overlapping sources compose independently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TrustBasis {
    /// An operator-provisioned bootstrap or explicit discovery peer.
    Bootstrap,
    /// A peer explicitly allowlisted by the operator.
    Operator,
    /// Membership in the previous, current, or next committee.
    Validator,
}

/// Independent admission, retention, and load-scoring decisions for one peer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PeerPolicy {
    /// Configured admission eligibility.
    admission: Admission,
    /// Ordinary population and mesh treatment.
    retention: Retention,
    /// Temporary overload treatment, independent of protocol penalties.
    load_scoring: LoadScoring,
}

impl PeerPolicy {
    /// Derive a policy from all currently applicable trust bases.
    pub(super) fn from_bases(bases: impl IntoIterator<Item = TrustBasis>) -> Self {
        bases.into_iter().fold(Self::default(), |policy, basis| policy.grant(basis))
    }

    /// Add one trust basis without removing privileges from another basis.
    pub(super) fn grant(self, basis: TrustBasis) -> Self {
        match basis {
            TrustBasis::Bootstrap => Self { admission: Admission::Authorized, ..self },
            TrustBasis::Operator | TrustBasis::Validator => Self {
                admission: Admission::Authorized,
                retention: Retention::Protected,
                load_scoring: LoadScoring::Exempt,
            },
        }
    }

    /// Whether this peer bypasses ordinary population pruning and mesh treatment.
    pub(super) fn protects_retention(self) -> bool {
        self.retention == Retention::Protected
    }

    /// Configured topology admission eligibility, independent of retention and load scoring.
    pub(super) fn admission(self) -> Admission {
        self.admission
    }

    /// Whether temporary load is exempt from scoring.
    pub(super) fn exempts_load(self) -> bool {
        self.load_scoring == LoadScoring::Exempt
    }

    /// Whether the score model should apply an attributable penalty.
    pub(super) fn applies(self, penalty: Penalty) -> bool {
        !penalty.is_load() || self.load_scoring == LoadScoring::Apply
    }
}

#[cfg(test)]
mod tests {
    use super::{
        super::penalty::{LoadPenalty, Penalty},
        Admission, LoadScoring, PeerPolicy, Retention, TrustBasis,
    };

    /// Ordinary, bootstrap, trusted, and all three committee slots have explicit privileges.
    #[test]
    fn policy_matrix() {
        [
            ("ordinary", None, Admission::Discovery, Retention::Ordinary, LoadScoring::Apply),
            (
                "bootstrap",
                Some(TrustBasis::Bootstrap),
                Admission::Authorized,
                Retention::Ordinary,
                LoadScoring::Apply,
            ),
            (
                "trusted",
                Some(TrustBasis::Operator),
                Admission::Authorized,
                Retention::Protected,
                LoadScoring::Exempt,
            ),
            (
                "previous",
                Some(TrustBasis::Validator),
                Admission::Authorized,
                Retention::Protected,
                LoadScoring::Exempt,
            ),
            (
                "current",
                Some(TrustBasis::Validator),
                Admission::Authorized,
                Retention::Protected,
                LoadScoring::Exempt,
            ),
            (
                "next",
                Some(TrustBasis::Validator),
                Admission::Authorized,
                Retention::Protected,
                LoadScoring::Exempt,
            ),
        ]
        .into_iter()
        .for_each(|(name, basis, admission, retention, load_scoring)| {
            let policy = PeerPolicy::from_bases(basis);
            assert_eq!(
                (policy.admission(), policy.retention, policy.load_scoring),
                (admission, retention, load_scoring),
                "{name}"
            );
            assert_eq!(policy.protects_retention(), retention == Retention::Protected);
            assert_eq!(policy.exempts_load(), load_scoring == LoadScoring::Exempt);
            assert_eq!(
                policy.applies(Penalty::Load(LoadPenalty::KademliaFlood)),
                load_scoring == LoadScoring::Apply
            );
        });
    }

    /// Every possible privilege combination keeps authenticated protocol failures scoreable.
    #[test]
    fn protocol_penalties_ignore_privileges() {
        [Admission::Discovery, Admission::Authorized].into_iter().for_each(|admission| {
            [Retention::Ordinary, Retention::Protected].into_iter().for_each(|retention| {
                [LoadScoring::Apply, LoadScoring::Exempt].into_iter().for_each(|load_scoring| {
                    let policy = PeerPolicy { admission, retention, load_scoring };
                    [Penalty::Mild, Penalty::Medium, Penalty::Severe, Penalty::Fatal]
                        .into_iter()
                        .for_each(|penalty| assert!(policy.applies(penalty)));
                });
            });
        });
    }

    /// Recomputing from live bases revokes committee privileges without removing operator trust.
    #[test]
    fn overlapping_bases_and_rotation() {
        let trusted = PeerPolicy::from_bases([TrustBasis::Operator]);
        assert_eq!(PeerPolicy::from_bases([TrustBasis::Operator, TrustBasis::Validator]), trusted);
        assert_eq!(PeerPolicy::from_bases([TrustBasis::Validator, TrustBasis::Operator]), trusted);
        assert_eq!(PeerPolicy::from_bases([TrustBasis::Operator, TrustBasis::Bootstrap]), trusted);
        assert_eq!(PeerPolicy::from_bases([TrustBasis::Bootstrap, TrustBasis::Operator]), trusted);
        assert!(!PeerPolicy::from_bases([]).protects_retention());
        assert!(PeerPolicy::from_bases([]).applies(Penalty::Load(LoadPenalty::Timeout)));
    }
}
