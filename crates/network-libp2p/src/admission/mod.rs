// SPDX-License-Identifier: MIT or Apache-2.0
//! Process-wide accounting contract for validated inbound handshake starts.
//!
//! This module does not enable a transport policy or choose production limits. A transport adapter
//! must call [`HandshakeStartBudget::admit`] exactly once after validating the source address and
//! before starting expensive handshake work. The current libp2p swarm hook lacks that evidence.
//! All listeners must use the single installed budget, including primary and worker listeners.
//! Address validation is reachability evidence, never committee authentication or an exemption.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};

/// The one budget shared by every inbound listener in this process.
static PROCESS_BUDGET: OnceLock<HandshakeStartBudget> = OnceLock::new();

/// Canonical source address, independent of ports and claimed peer identities.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourceKey(IpAddr);

impl From<IpAddr> for SourceKey {
    fn from(address: IpAddr) -> Self {
        Self(match address {
            IpAddr::V4(address) => IpAddr::V4(address),
            IpAddr::V6(address) => {
                address.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(address))
            }
        })
    }
}

impl SourceKey {
    /// Group IPv4 by /24 and IPv6 by /64 after mapped-IPv4 normalization.
    ///
    /// Pending-occupancy accounting must reuse this key function with separate counters.
    pub fn prefix(self) -> SourcePrefix {
        SourcePrefix(match self.0 {
            IpAddr::V4(address) => IpAddr::V4(Ipv4Addr::from(u32::from(address) & 0xffff_ff00)),
            IpAddr::V6(address) => {
                IpAddr::V6(Ipv6Addr::from(u128::from(address) & (u128::MAX << 64)))
            }
        })
    }
}

/// A canonical prefix, reusable by independent pending-occupancy accounting.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SourcePrefix(IpAddr);

/// Transport evidence that an inbound source has demonstrated address reachability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValidationProvenance {
    /// The transport verified an address-bound QUIC Retry token for this attempt and source.
    QuicRetry,
}

/// A source accompanied by explicit transport validation provenance.
///
/// This is an integration contract, not a token verifier. The transport adapter must construct this
/// from its validation result. An address, packet, claimed PeerId, or swarm event alone is
/// insufficient. There is no deserializer or conversion from a bare address.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedSource {
    /// Canonical address actually validated by the transport.
    key: SourceKey,
    /// Transport evidence, never supplied by the remote peer.
    provenance: ValidationProvenance,
}

impl ValidatedSource {
    /// Record transport validation for this specific attempt and source address.
    ///
    /// The caller must obtain provenance from successful transport validation, never infer it from
    /// a claimed identity or from arrival at a pending/established swarm hook.
    pub fn new(address: IpAddr, provenance: ValidationProvenance) -> Self {
        Self { key: address.into(), provenance }
    }

    /// Return the canonical source, also suitable for independent occupancy accounting.
    pub fn key(self) -> SourceKey {
        self.key
    }

    /// Return the transport evidence associated with this attempt.
    pub fn provenance(self) -> ValidationProvenance {
        self.provenance
    }
}

/// A burst allowance and the time needed to replenish one handshake start.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartRate {
    /// Duration represented by a completely spent burst allowance.
    window: Duration,
    /// Time required to replenish one start.
    interval: Duration,
}

impl StartRate {
    /// Validate a positive burst and refill interval without selecting production defaults.
    pub fn new(burst: u32, refill_interval: Duration) -> Result<Self, PolicyError> {
        refill_interval
            .checked_mul(burst)
            .filter(|window| !window.is_zero())
            .map(|window| Self { window, interval: refill_interval })
            .ok_or(PolicyError::InvalidRate)
    }

    /// Compute the new debt deadline if one more start fits within the burst allowance.
    fn next(self, debt_until: Duration, now: Duration) -> Option<Duration> {
        let limit = now.checked_add(self.window)?;
        debt_until.max(now).checked_add(self.interval).filter(|next| *next <= limit)
    }
}

/// Explicit process, source and prefix rates, with bounded source and prefix tables.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HandshakeStartPolicy {
    /// Physical allowance charged by all listeners and accepted starts.
    aggregate: StartRate,
    /// Allowance shared by all identities and ports at one canonical address.
    source: StartRate,
    /// Allowance shared by all sources within a canonical prefix.
    prefix: StartRate,
    /// Maximum retained source debts.
    source_capacity: usize,
    /// Maximum retained prefix debts.
    prefix_capacity: usize,
}

impl HandshakeStartPolicy {
    /// Build a policy with explicit, nonzero table bounds. No address or identity is exempt.
    pub fn new(
        aggregate: StartRate,
        source: StartRate,
        prefix: StartRate,
        source_capacity: usize,
        prefix_capacity: usize,
    ) -> Result<Self, PolicyError> {
        if source_capacity == 0 || prefix_capacity == 0 {
            Err(PolicyError::EmptyTable)
        } else {
            Ok(Self { aggregate, source, prefix, source_capacity, prefix_capacity })
        }
    }
}

/// Configuration or state failure. Admission fails closed on unavailable accounting state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyError {
    /// A rate is zero or its burst duration cannot be represented.
    InvalidRate,
    /// An accounting table has zero capacity.
    EmptyTable,
    /// A budget is already installed; every listener must use that same instance.
    AlreadyInstalled,
    /// Outstanding rate debt prevents a reset-free policy transition.
    OutstandingDebt,
    /// A new policy deadline cannot be represented by the monotonic clock.
    ClockRange,
    /// The accounting mutex was poisoned; no admission decision is safe.
    Unavailable,
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidRate => {
                "handshake rate must have a positive, representable burst duration"
            }
            Self::EmptyTable => "handshake accounting tables must have positive capacity",
            Self::AlreadyInstalled => "a process handshake budget is already installed",
            Self::OutstandingDebt => "handshake rate debt must refill before a policy change",
            Self::ClockRange => "handshake policy exceeds the monotonic clock range",
            Self::Unavailable => "handshake accounting state is unavailable",
        })
    }
}

impl std::error::Error for PolicyError {}

/// Aggregate outcomes with a fixed vocabulary and no source or identity labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartOutcome {
    /// All three budgets were charged atomically; the transport may start the handshake.
    Admitted,
    /// The physical process allowance is exhausted.
    AggregateLimited,
    /// The source allowance is exhausted.
    SourceLimited,
    /// The prefix allowance is exhausted.
    PrefixLimited,
    /// No source entry can be retained without discarding outstanding debt.
    SourceTableFull,
    /// No prefix entry can be retained without discarding outstanding debt.
    PrefixTableFull,
}

impl StartOutcome {
    /// A bounded label for metrics exporters. Never attach a source address or PeerId.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Admitted => "admitted",
            Self::AggregateLimited => "aggregate_limited",
            Self::SourceLimited => "source_limited",
            Self::PrefixLimited => "prefix_limited",
            Self::SourceTableFull => "source_table_full",
            Self::PrefixTableFull => "prefix_table_full",
        }
    }
}

/// A constant-size snapshot of aggregate decisions and retained accounting entries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccountingSnapshot {
    /// Admitted starts across every listener.
    admitted: u64,
    /// Rejections by the process allowance.
    aggregate_limited: u64,
    /// Rejections by a source allowance.
    source_limited: u64,
    /// Rejections by a prefix allowance.
    prefix_limited: u64,
    /// Rejections because the source table retains outstanding debts.
    source_table_full: u64,
    /// Rejections because the prefix table retains outstanding debts.
    prefix_table_full: u64,
    /// Retained source entries, including refilled entries eligible for eviction.
    sources: usize,
    /// Retained prefix entries, including refilled entries eligible for eviction.
    prefixes: usize,
}

impl AccountingSnapshot {
    /// Export six process counters with bounded labels and no per-attempt logging.
    pub fn outcomes(self) -> impl Iterator<Item = (StartOutcome, u64)> {
        [
            (StartOutcome::Admitted, self.admitted),
            (StartOutcome::AggregateLimited, self.aggregate_limited),
            (StartOutcome::SourceLimited, self.source_limited),
            (StartOutcome::PrefixLimited, self.prefix_limited),
            (StartOutcome::SourceTableFull, self.source_table_full),
            (StartOutcome::PrefixTableFull, self.prefix_table_full),
        ]
        .into_iter()
    }

    /// Retained source and prefix counts, independently of pending connections.
    pub fn retained_entries(self) -> (usize, usize) {
        (self.sources, self.prefixes)
    }

    /// Saturate counters so high-volume rejection cannot wrap telemetry.
    fn record(&mut self, outcome: StartOutcome) {
        let count = match outcome {
            StartOutcome::Admitted => &mut self.admitted,
            StartOutcome::AggregateLimited => &mut self.aggregate_limited,
            StartOutcome::SourceLimited => &mut self.source_limited,
            StartOutcome::PrefixLimited => &mut self.prefix_limited,
            StartOutcome::SourceTableFull => &mut self.source_table_full,
            StartOutcome::PrefixTableFull => &mut self.prefix_table_full,
        };
        *count = count.saturating_add(1);
    }
}

/// Bounded debt with an ordered eviction index. Both indexes retain at most the configured
/// capacity.
#[derive(Debug)]
struct DebtTable<K> {
    /// Debt deadline for each retained key.
    entries: BTreeMap<K, Duration>,
    /// Ordered deadlines for O(log capacity) updates and reset-free eviction.
    expiry: BTreeSet<(Duration, K)>,
}

impl<K: Copy + Ord> DebtTable<K> {
    /// Check capacity and rate without charging debt or evicting an entry.
    fn prepare(
        &self,
        key: K,
        rate: StartRate,
        capacity: usize,
        now: Duration,
        full: StartOutcome,
        limited: StartOutcome,
    ) -> Result<Duration, StartOutcome> {
        if !self.entries.contains_key(&key)
            && self.entries.len() >= capacity
            && self.expiry.first().is_none_or(|(deadline, _)| *deadline > now)
        {
            Err(full)
        } else {
            rate.next(self.entries.get(&key).copied().unwrap_or(now), now).ok_or(limited)
        }
    }

    /// Commit a prepared admission, evicting at most one fully refilled entry.
    fn commit(&mut self, key: K, deadline: Duration, capacity: usize) {
        if !self.entries.contains_key(&key) && self.entries.len() >= capacity {
            self.expiry.pop_first().into_iter().for_each(|(_, expired)| {
                self.entries.remove(&expired);
            });
        }
        self.entries.insert(key, deadline).into_iter().for_each(|previous| {
            self.expiry.remove(&(previous, key));
        });
        self.expiry.insert((deadline, key));
    }

    /// Whether every bucket has refilled, permitting a reset-free policy transition.
    fn is_refilled(&self, now: Duration) -> bool {
        self.expiry.last().is_none_or(|(deadline, _)| *deadline <= now)
    }
}

/// Rate state, independent of pending-connection occupancy counters.
#[derive(Debug)]
struct Accounting {
    /// Current explicit policy.
    policy: HandshakeStartPolicy,
    /// Time at which all process debt will have refilled.
    aggregate: Duration,
    /// Per-source rate debt.
    sources: DebtTable<SourceKey>,
    /// Per-prefix rate debt.
    prefixes: DebtTable<SourcePrefix>,
    /// Process counters preserved across policy changes.
    snapshot: AccountingSnapshot,
}

impl Accounting {
    /// Start with no aggregate credit. Restarting must not grant a fresh physical burst.
    fn new(policy: HandshakeStartPolicy) -> Self {
        Self {
            aggregate: policy.aggregate.window,
            policy,
            sources: DebtTable { entries: BTreeMap::new(), expiry: BTreeSet::new() },
            prefixes: DebtTable { entries: BTreeMap::new(), expiry: BTreeSet::new() },
            snapshot: AccountingSnapshot::default(),
        }
    }

    /// Charge all allowances or none; failed starts never erase or create source debt.
    fn charge(&mut self, source: ValidatedSource, now: Duration) -> Result<(), StartOutcome> {
        let aggregate = self
            .policy
            .aggregate
            .next(self.aggregate, now)
            .ok_or(StartOutcome::AggregateLimited)?;
        let key = source.key();
        let prefix = key.prefix();
        let source_debt = self.sources.prepare(
            key,
            self.policy.source,
            self.policy.source_capacity,
            now,
            StartOutcome::SourceTableFull,
            StartOutcome::SourceLimited,
        )?;
        let prefix_debt = self.prefixes.prepare(
            prefix,
            self.policy.prefix,
            self.policy.prefix_capacity,
            now,
            StartOutcome::PrefixTableFull,
            StartOutcome::PrefixLimited,
        )?;
        self.aggregate = aggregate;
        self.sources.commit(key, source_debt, self.policy.source_capacity);
        self.prefixes.commit(prefix, prefix_debt, self.policy.prefix_capacity);
        Ok(())
    }

    /// Account for exactly one validated attempt and export its aggregate outcome.
    fn admit(&mut self, source: ValidatedSource, now: Duration) -> StartOutcome {
        let outcome =
            self.charge(source, now).map_or_else(|outcome| outcome, |()| StartOutcome::Admitted);
        self.snapshot.record(outcome);
        outcome
    }

    /// Transition only after old debt refills, then cold-start the new process allowance.
    fn reconfigure(
        &mut self,
        policy: HandshakeStartPolicy,
        now: Duration,
    ) -> Result<(), PolicyError> {
        if self.aggregate > now || !self.sources.is_refilled(now) || !self.prefixes.is_refilled(now)
        {
            Err(PolicyError::OutstandingDebt)
        } else {
            let aggregate =
                now.checked_add(policy.aggregate.window).ok_or(PolicyError::ClockRange)?;
            self.policy = policy;
            self.aggregate = aggregate;
            self.sources.entries.clear();
            self.sources.expiry.clear();
            self.prefixes.entries.clear();
            self.prefixes.expiry.clear();
            Ok(())
        }
    }
}

/// The single physical handshake-start budget. No address or identity is exempt.
///
/// Install once at node startup and pass the same reference to primary and worker transports.
/// Listener restarts reuse [`Self::process`], never reset the budget. Process restarts start empty:
/// one aggregate interval must elapse before the first start. No per-swarm constructor is exposed.
#[derive(Debug)]
pub struct HandshakeStartBudget {
    /// Clock origin shared across listener restarts and policy changes.
    origin: Instant,
    /// Serializes reservation of all allowances across listeners.
    state: Mutex<Accounting>,
}

impl HandshakeStartBudget {
    /// Install the process budget after explicitly selecting policy values.
    pub fn install(policy: HandshakeStartPolicy) -> Result<&'static Self, PolicyError> {
        PROCESS_BUDGET.set(Self::new(policy)).map_err(|_| PolicyError::AlreadyInstalled)?;
        Self::process().ok_or(PolicyError::Unavailable)
    }

    /// Obtain the installed budget for another listener without creating fresh rate credit.
    pub fn process() -> Option<&'static Self> {
        PROCESS_BUDGET.get()
    }

    /// Construct state privately; production callers must use the single process installation.
    fn new(policy: HandshakeStartPolicy) -> Self {
        Self { origin: Instant::now(), state: Mutex::new(Accounting::new(policy)) }
    }

    /// Account for one validated start before expensive transport handshake work.
    ///
    /// Proceed only on `Ok(StartOutcome::Admitted)`. Never refund handshake failure, cancellation,
    /// or connection close. A start is spent work, independent of pending-occupancy reservations.
    pub fn admit(&self, source: ValidatedSource) -> Result<StartOutcome, PolicyError> {
        self.admit_at(source, self.origin.elapsed())
    }

    /// Reserve against an elapsed monotonic time, also allowing deterministic concurrency tests.
    fn admit_at(
        &self,
        source: ValidatedSource,
        now: Duration,
    ) -> Result<StartOutcome, PolicyError> {
        self.state
            .lock()
            .map(|mut state| state.admit(source, now))
            .map_err(|_| PolicyError::Unavailable)
    }

    /// Apply a policy only after all debt refills. Failure leaves all state unchanged.
    pub fn reconfigure(&self, policy: HandshakeStartPolicy) -> Result<(), PolicyError> {
        self.state
            .lock()
            .map_err(|_| PolicyError::Unavailable)?
            .reconfigure(policy, self.origin.elapsed())
    }

    /// Export saturating process counters and bounded table sizes without high-volume logging.
    pub fn snapshot(&self) -> Result<AccountingSnapshot, PolicyError> {
        self.state
            .lock()
            .map(|state| AccountingSnapshot {
                sources: state.sources.entries.len(),
                prefixes: state.prefixes.entries.len(),
                ..state.snapshot
            })
            .map_err(|_| PolicyError::Unavailable)
    }
}

#[cfg(test)]
mod tests;
