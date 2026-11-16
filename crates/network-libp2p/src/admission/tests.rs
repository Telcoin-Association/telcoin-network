//! Deterministic accounting tests. Rates here are fixtures, not proposed production values.

use super::*;

/// One fixture refill interval.
const TICK: Duration = Duration::from_millis(10);

/// Construct explicit fixture rates and bounded tables.
fn policy(
    aggregate: u32,
    source: u32,
    prefix: u32,
    sources: usize,
    prefixes: usize,
) -> Result<HandshakeStartPolicy, PolicyError> {
    HandshakeStartPolicy::new(
        StartRate::new(aggregate, TICK)?,
        StartRate::new(source, TICK)?,
        StartRate::new(prefix, TICK)?,
        sources,
        prefixes,
    )
}

/// Supply explicit validation provenance at the simulated transport boundary.
fn validated(address: Ipv4Addr) -> ValidatedSource {
    ValidatedSource::new(address.into(), ValidationProvenance::QuicRetry)
}

/// A distinct source prefix for each simulated listener.
fn distinct_source(id: u8) -> ValidatedSource {
    validated(Ipv4Addr::new(192, 0, id, 1))
}

/// No zero rate, zero table bound, or overflowing burst duration is accepted.
#[test]
fn invalid_policies_are_rejected() -> Result<(), PolicyError> {
    assert_eq!(StartRate::new(0, TICK), Err(PolicyError::InvalidRate));
    assert_eq!(StartRate::new(1, Duration::ZERO), Err(PolicyError::InvalidRate));
    assert_eq!(StartRate::new(2, Duration::MAX), Err(PolicyError::InvalidRate));
    assert_eq!(policy(1, 1, 1, 0, 1), Err(PolicyError::EmptyTable));
    assert_eq!(policy(1, 1, 1, 1, 0), Err(PolicyError::EmptyTable));
    assert!(policy(1, 1, 1, 1, 1).is_ok());
    Ok(())
}

/// Mapped IPv4 cannot obtain a second source allowance or a different prefix allowance.
#[test]
fn normalized_keys_share_ipv4_and_ipv6_prefixes() {
    let v4 = Ipv4Addr::new(192, 0, 2, 1);
    let direct = SourceKey::from(IpAddr::V4(v4));
    let mapped = SourceKey::from(IpAddr::V6(v4.to_ipv6_mapped()));
    assert_eq!(direct, mapped);
    assert_eq!(direct.prefix(), distinct_source(2).key().prefix());
    assert_eq!(
        direct.prefix(),
        SourceKey::from(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 255))).prefix()
    );
    assert_ne!(direct.prefix(), distinct_source(3).key().prefix());
    let v6 = |subnet, host| {
        SourceKey::from(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, subnet, 0, 0, 0, host)))
    };
    assert_eq!(v6(1, 1).prefix(), v6(1, 65535).prefix());
    assert_ne!(v6(1, 1).prefix(), v6(2, 1).prefix());
    assert_eq!(validated(v4).provenance(), ValidationProvenance::QuicRetry);
}

/// Restart never grants a fresh process burst; fractional time cannot mint a token.
#[test]
fn restart_and_refill_obey_exact_boundaries() -> Result<(), PolicyError> {
    let policy = policy(4, 8, 8, 8, 8)?;
    (0..2).for_each(|_| {
        let mut state = Accounting::new(policy);
        assert_eq!(state.admit(distinct_source(1), Duration::ZERO), StartOutcome::AggregateLimited);
        assert_eq!(
            state.admit(distinct_source(1), TICK - Duration::from_nanos(1)),
            StartOutcome::AggregateLimited
        );
        assert_eq!(state.admit(distinct_source(1), TICK), StartOutcome::Admitted);
        assert_eq!(state.admit(distinct_source(2), TICK), StartOutcome::AggregateLimited);
        assert_eq!(state.admit(distinct_source(2), TICK * 2), StartOutcome::Admitted);
    });
    Ok(())
}

/// Multiple honest swarms behind a NAT share the configured source burst and its refill.
#[test]
fn shared_nat_and_mapped_address_use_one_source_budget() -> Result<(), PolicyError> {
    let policy = policy(10, 3, 10, 8, 8)?;
    let now = policy.aggregate.window;
    let mut state = Accounting::new(policy);
    let ip = Ipv4Addr::new(192, 0, 2, 1);
    (0..3).for_each(|_| assert_eq!(state.admit(validated(ip), now), StartOutcome::Admitted));
    let mapped = ValidatedSource::new(ip.to_ipv6_mapped().into(), ValidationProvenance::QuicRetry);
    assert_eq!(state.admit(mapped, now), StartOutcome::SourceLimited);
    assert_eq!(state.admit(mapped, now + TICK), StartOutcome::Admitted);
    assert_eq!(state.sources.entries.len(), 1);
    Ok(())
}

/// Rotating addresses within a prefix cannot bypass its allowance; other prefixes remain eligible.
#[test]
fn prefix_budget_covers_multiple_sources_in_both_families() -> Result<(), PolicyError> {
    [
        (IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2))),
        (
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 2)),
        ),
    ]
    .into_iter()
    .try_for_each(|(a, b)| {
        let policy = policy(10, 10, 1, 8, 8)?;
        let now = policy.aggregate.window;
        let mut state = Accounting::new(policy);
        assert_eq!(
            state.admit(ValidatedSource::new(a, ValidationProvenance::QuicRetry), now),
            StartOutcome::Admitted
        );
        assert_eq!(
            state.admit(ValidatedSource::new(b, ValidationProvenance::QuicRetry), now),
            StartOutcome::PrefixLimited
        );
        assert_eq!(state.admit(distinct_source(3), now), StartOutcome::Admitted);
        assert_eq!(state.sources.entries.len(), 2);
        Ok(())
    })
}

/// Independent primary/worker callers and diverse sources still share a single physical allowance.
#[test]
fn concurrent_listeners_cannot_multiply_the_process_budget() -> Result<(), PolicyError> {
    let policy = policy(8, 8, 8, 64, 64)?;
    let now = policy.aggregate.window;
    let budget = HandshakeStartBudget::new(policy);
    let admitted = std::thread::scope(|scope| {
        let shared = &budget;
        let attempts = (0..32)
            .map(|id| scope.spawn(move || shared.admit_at(distinct_source(id), now)))
            .collect::<Vec<_>>();
        attempts.into_iter().try_fold(0usize, |count, attempt| {
            let outcome = attempt.join().map_err(|_| PolicyError::Unavailable)??;
            Ok::<_, PolicyError>(count + usize::from(outcome == StartOutcome::Admitted))
        })
    })?;
    assert_eq!(admitted, 8);
    let snapshot = budget.snapshot()?;
    assert_eq!(snapshot.admitted, 8);
    assert_eq!(snapshot.aggregate_limited, 24);
    assert_eq!(snapshot.retained_entries(), (8, 8));
    Ok(())
}

/// A source rejection does not spend a process token or prevent another prefix from reconnecting.
#[test]
fn rejection_is_atomic_across_allowances() -> Result<(), PolicyError> {
    let policy = policy(2, 1, 2, 8, 8)?;
    let now = policy.aggregate.window;
    let mut state = Accounting::new(policy);
    assert_eq!(state.admit(distinct_source(1), now), StartOutcome::Admitted);
    assert_eq!(state.admit(distinct_source(1), now), StartOutcome::SourceLimited);
    assert_eq!(state.admit(distinct_source(2), now), StartOutcome::Admitted);
    assert_eq!(state.admit(distinct_source(3), now), StartOutcome::AggregateLimited);
    assert_eq!(state.sources.entries.len(), 2);
    assert_eq!(state.prefixes.entries.len(), 2);
    Ok(())
}

/// A full source table preserves debt and its expiry index when an existing source starts again.
#[test]
fn source_churn_cannot_evict_unpaid_or_updated_debt() -> Result<(), PolicyError> {
    let policy = policy(10, 2, 10, 1, 8)?;
    let now = policy.aggregate.window;
    let mut state = Accounting::new(policy);
    assert_eq!(state.admit(distinct_source(1), now), StartOutcome::Admitted);
    assert_eq!(state.admit(distinct_source(1), now), StartOutcome::Admitted);
    assert_eq!(state.admit(distinct_source(2), now + TICK), StartOutcome::SourceTableFull);
    assert_eq!(
        state.admit(distinct_source(2), now + TICK * 2 - Duration::from_nanos(1)),
        StartOutcome::SourceTableFull
    );
    assert_eq!(state.admit(distinct_source(2), now + TICK * 2), StartOutcome::Admitted);
    assert_eq!(state.admit(distinct_source(1), now + TICK * 2), StartOutcome::SourceTableFull);
    assert_eq!(state.sources.entries.len(), 1);
    assert_eq!(state.sources.expiry.len(), 1);
    Ok(())
}

/// Prefix-table pressure rejects new prefixes without consuming the source table or process credit.
#[test]
fn prefix_churn_cannot_evict_debt_or_partially_charge() -> Result<(), PolicyError> {
    let policy = policy(10, 10, 2, 8, 1)?;
    let now = policy.aggregate.window;
    let mut state = Accounting::new(policy);
    assert_eq!(state.admit(distinct_source(1), now), StartOutcome::Admitted);
    let aggregate = state.aggregate;
    assert_eq!(state.admit(distinct_source(2), now), StartOutcome::PrefixTableFull);
    assert_eq!(state.aggregate, aggregate);
    assert_eq!(state.sources.entries.len(), 1);
    assert_eq!(state.admit(distinct_source(2), now + TICK), StartOutcome::Admitted);
    assert_eq!(state.prefixes.entries.len(), 1);
    assert_eq!(state.prefixes.expiry.len(), 1);
    Ok(())
}

/// Sustained source churn and safe evictions keep both indexes within their explicit capacities.
#[test]
fn sustained_churn_keeps_bookkeeping_bounded() -> Result<(), PolicyError> {
    let mut state = Accounting::new(policy(1, 1, 1, 2, 2)?);
    (0..1000u16).for_each(|id| {
        let source = validated(Ipv4Addr::from(u32::from(id) << 8));
        assert_eq!(
            state.admit(source, Duration::from_secs(u64::from(id) + 1)),
            StartOutcome::Admitted
        );
        assert!(state.sources.entries.len() <= 2);
        assert!(state.prefixes.entries.len() <= 2);
        assert_eq!(state.sources.entries.len(), state.sources.expiry.len());
        assert_eq!(state.prefixes.entries.len(), state.prefixes.expiry.len());
    });
    Ok(())
}

/// Source and prefix debt each independently block policy replacement after process debt refills.
#[test]
fn policy_changes_preserve_debt_and_cold_start() -> Result<(), PolicyError> {
    [true, false].into_iter().try_for_each(|slow_source| {
        let slow = StartRate::new(1, Duration::from_secs(1))?;
        let fast = StartRate::new(1, TICK)?;
        let old = HandshakeStartPolicy::new(
            fast,
            if slow_source { slow } else { fast },
            if slow_source { fast } else { slow },
            4,
            4,
        )?;
        let mut state = Accounting::new(old);
        assert_eq!(state.admit(distinct_source(1), TICK), StartOutcome::Admitted);
        let new = policy(2, 2, 2, 1, 1)?;
        assert_eq!(state.reconfigure(new, TICK * 2), Err(PolicyError::OutstandingDebt));
        assert_eq!(state.policy, old);
        assert_eq!(state.sources.entries.len(), 1);
        let idle = Duration::from_secs(2);
        state.reconfigure(new, idle)?;
        assert_eq!(state.policy, new);
        assert!(state.sources.entries.is_empty());
        assert!(state.prefixes.entries.is_empty());
        assert_eq!(state.snapshot.admitted, 1);
        assert_eq!(state.admit(distinct_source(2), idle), StartOutcome::AggregateLimited);
        assert_eq!(state.admit(distinct_source(2), idle + TICK), StartOutcome::Admitted);
        Ok(())
    })
}

/// Clock overflow rejects a policy transition without changing the installed policy.
#[test]
fn policy_clock_overflow_fails_without_reset() -> Result<(), PolicyError> {
    let old = policy(1, 1, 1, 1, 1)?;
    let mut state = Accounting::new(old);
    let new = policy(2, 2, 2, 2, 2)?;
    assert_eq!(state.reconfigure(new, Duration::MAX), Err(PolicyError::ClockRange));
    assert_eq!(state.policy, old);
    assert_eq!(state.aggregate, old.aggregate.window);
    Ok(())
}

/// All outcomes use six fixed labels and saturating counters instead of per-attempt log records.
#[test]
fn outcome_export_is_bounded_and_saturating() {
    let mut snapshot = AccountingSnapshot::default();
    snapshot.outcomes().for_each(|(outcome, _)| snapshot.record(outcome));
    assert!(snapshot.outcomes().all(|(_, count)| count == 1));
    assert_eq!(
        snapshot.outcomes().map(|(outcome, _)| outcome.label()).collect::<BTreeSet<_>>().len(),
        6
    );
    snapshot.admitted = u64::MAX;
    snapshot.record(StartOutcome::Admitted);
    assert_eq!(snapshot.admitted, u64::MAX);
}

/// Listener reinitialization cannot create another process budget or reset its counters.
#[test]
fn process_installation_is_unique() -> Result<(), PolicyError> {
    let policy = policy(1, 1, 1, 1, 1)?;
    let budget = HandshakeStartBudget::install(policy)?;
    budget.admit_at(distinct_source(1), Duration::ZERO)?;
    assert!(matches!(HandshakeStartBudget::install(policy), Err(PolicyError::AlreadyInstalled)));
    let shared = HandshakeStartBudget::process().ok_or(PolicyError::Unavailable)?;
    assert!(std::ptr::eq(budget, shared));
    assert_eq!(shared.snapshot()?.aggregate_limited, 1);
    Ok(())
}
