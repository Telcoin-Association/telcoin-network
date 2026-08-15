//! Prometheus metrics for the reth execution environment.
//!
//! The counter series are exported under the `tn_reth` scope, in two unrelated groups.
//!
//! Block building. Two series count transactions that `build_block_from_batch_payload`
//! declines to include in a block: `tn_reth.unrecoverable_txs_dropped_total` (alertable - a
//! certified batch carried a transaction whose signer could not be recovered) and
//! `tn_reth.invalid_txs_skipped_total` (expected - duplicates/invalid transactions across
//! independently-batching workers). See [`RethEnvMetrics`] for per-series semantics.
//!
//! Observer transaction forwarding. A base series counts every transaction handed to the
//! forwarder ([`crate::WorkerRpcForwarder`]): `tn_reth.forwarded_txns_queued_total`. Read
//! against it, `tn_reth.forwarded_txns_dropped_total` (labeled by `reason`, see
//! [`ForwardDropReason`]) counts the transactions the pipeline gave up on, and two series count
//! work the forwarder sheds to stay within its own bounds:
//! `tn_reth.forwarded_batches_shed_total` and `tn_reth.forwarded_txns_abandoned_total`. Two more
//! count the delivery feedback added for issue #1145:
//! `tn_reth.forward_endpoints_demoted_total` and `tn_reth.forwarded_txns_requeued_total`. See
//! [`ForwarderMetrics`] for per-series semantics.
//!
//! [`report_db_metrics`] additionally samples reth database metrics as a pre-scrape hook.
//!
//! Three gauges under the same scope report what the epoch-boundary system calls spend against
//! the budget they run under: [`EPOCH_CLOSE_SYSTEM_CALL_GAS_USED`] (labelled by `call`),
//! [`EPOCH_CLOSE_GAS_USED`], and [`EPOCH_CLOSE_SYSTEM_CALL_GAS_LIMIT`]. Nothing else can see that
//! gas — a system call is submitted with `gas_price: 0`, so it neither pays for gas nor enters
//! the block's `gas_used`. See [`record_epoch_close_gas`] and `crates/tn-reth/README.md` for the
//! dashboard and alert queries they are meant to be read with, and for what they do not cover.
//!
//! # Two mechanisms in one module
//!
//! The counters use the `reth_metrics::Metrics` derive; the epoch-close gauges use the
//! `metrics::gauge!` / `metrics::describe_gauge!` macros. The split is not stylistic:
//!
//! 1. The per-call gauge carries a `call` label, which a derive field cannot express — the derive
//!    generates one unlabelled series per field.
//! 2. The macros re-resolve the recorder on every call, whereas the derive's `Default::default()`
//!    caches the whole struct in a `static OnceLock` (`metrics-derive` `expand.rs`), fixing which
//!    recorder every later handle records against. That is invisible in production, where one
//!    recorder is installed before any metric exists, but it makes a test that installs a local
//!    recorder pass or fail on test ordering. The counter tests below work around it with
//!    `new_with_labels`; the gauges get the property for free, which is what lets a real epoch
//!    close be asserted on (`env/epoch.rs`).
//!
//! Everything here binds to the process-global `metrics` recorder, so
//! `tn_metrics::install_recorder` must run first — the node installs it before opening the
//! database (`RethEnv::new_database`), and the two registration entry points ([`init`], called
//! from `RethEnv::new`, and [`ForwarderMetrics::init`], called from
//! `WorkerRpcForwarder::new`) then force registration so every counter exists at zero from
//! process start.

use reth_metrics::{metrics::Counter, Metrics};
use std::sync::LazyLock;
use tracing::warn;

use crate::{evm::SYSTEM_CALL_GAS_LIMIT, RethDb};

/// Process-wide metrics for [`crate::RethEnv`].
///
/// A `LazyLock` static is justified here for the same reason as the engine's
/// `ENGINE_METRICS`: there is exactly one execution environment per process, and threading a
/// handle through `RethEnvInner` would touch every construction site and caller signature for
/// two counters.
///
/// First use must happen after the global recorder is installed. `tn_metrics::install_recorder`
/// runs before `RethEnv::new_database` (that ordering is a documented invariant of the
/// recorder), so a first use inside a `RethEnv` method binds to the real recorder rather than
/// the noop one.
pub(crate) static RETH_METRICS: LazyLock<RethEnvMetrics> = LazyLock::new(RethEnvMetrics::default);

/// Register both counters with the global recorder without recording an event.
///
/// Registration happens when the `LazyLock` is first forced, and the only sites that force it
/// are drop paths a healthy node never takes. Without this call the series would simply not
/// exist in a scrape until the first drop, which breaks the counters in the two ways that
/// matter: an absent series is indistinguishable from a broken exporter or a mistyped metric
/// name, so the "alert on any nonzero value" rule can never be verified in its negative case;
/// and `rate()` over a series whose very first sample is already nonzero renders no step, so a
/// one-shot drop burst on an otherwise quiet node stays invisible on the dashboard.
///
/// Called from [`crate::RethEnv::new`], which runs after `tn_metrics::install_recorder`, so
/// both counters baseline at zero from process start . . . the same way the worker and
/// executor counters they sit beside on the dashboard already do.
pub(crate) fn init() {
    LazyLock::force(&RETH_METRICS);
}

/// Metrics for building canonical blocks from certified batch payloads.
///
/// Both counters cover transactions that `build_block_from_batch_payload` silently declines to
/// include in the block it returns. The two drop sites have opposite expectedness, so they are
/// counted separately rather than summed - see the per-field docs.
#[derive(Metrics)]
#[metrics(scope = "tn_reth")]
pub(crate) struct RethEnvMetrics {
    /// Transactions dropped while building a block because their signer could not be recovered.
    ///
    /// Alert on any nonzero value. A certified sub-DAG is fixed and identical on every honest
    /// node, so an unrecoverable transaction is dropped rather than halting the network (see
    /// issues #933 / #938); reaching that path at all means a batch carrying undecodable
    /// transaction bytes was certified, i.e. batch validation was bypassed somewhere upstream.
    pub(crate) unrecoverable_txs_dropped_total: Counter,

    /// Transactions skipped while building a block because the EVM rejected them as invalid.
    ///
    /// Not an error signal, and not alertable: workers batch independently, so the same
    /// transaction can legitimately reach execution twice within one consensus output and the
    /// second copy is skipped as a duplicate. A steady nonzero rate is normal operation. The
    /// alertable counter is [`RethEnvMetrics::unrecoverable_txs_dropped_total`].
    pub(crate) invalid_txs_skipped_total: Counter,
}

/// Counter names for the observer forwarder, kept next to their only emission sites so the
/// registration in [`ForwarderMetrics::init`] and the increments below cannot drift apart.
const FORWARDED_BATCHES_SHED: &str = "tn_reth.forwarded_batches_shed_total";
const FORWARDED_TXNS_ABANDONED: &str = "tn_reth.forwarded_txns_abandoned_total";
const FORWARD_ENDPOINTS_DEMOTED: &str = "tn_reth.forward_endpoints_demoted_total";
const FORWARDED_TXNS_REQUEUED: &str = "tn_reth.forwarded_txns_requeued_total";
const FORWARDED_TXNS_QUEUED: &str = "tn_reth.forwarded_txns_queued_total";
const FORWARDED_TXNS_DROPPED: &str = "tn_reth.forwarded_txns_dropped_total";

/// Why the forwarding pipeline gave up on a forward attempt it had accepted.
///
/// The variants label `tn_reth.forwarded_txns_dropped_total`, one series per reason, all
/// registered at zero by [`ForwarderMetrics::init`]. Together with
/// `tn_reth.forwarded_txns_abandoned_total` they account for every queued transaction that was
/// not delivered, so `queued - dropped - abandoned` is the number a dashboard can trust as
/// delivered (issue #1133). The accounting is per attempt: the pre-admission reasons
/// (`EmptyCommittee`, `NoUsableEndpoint`, `BatchShed`) refuse the batch so the caller keeps
/// its transactions and requeues them on a later build (issue #1132), while `Rejected` and
/// `Unreached` happen after admission, when the transactions are already evicted as mined.
/// A `Rejected` transaction is a final loss; an `Unreached` one got no verdict, so the task
/// also returns it to the worker's own pool (issue #1145) and a later build repackages it
/// into a fresh attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForwardDropReason {
    /// The batch reached the forwarder with an empty committee or no advertised endpoint, so
    /// there was nowhere to route to. A silent no-op before it was counted; on the production
    /// path the worker guards this case first and counts it under its own scope
    /// (`tn_worker.forwarded_txns_dropped_total`).
    EmptyCommittee,
    /// Endpoints were advertised but none resolved to a usable provider - every one was
    /// refused by the [`crate::ForwardTargetPolicy`] or failed to parse. Alertable: the node
    /// cannot hand on the transactions it accepts while this condition lasts.
    NoUsableEndpoint,
    /// The whole batch was shed at admission because every forward permit was in flight. The
    /// transaction-level twin of `tn_reth.forwarded_batches_shed_total`, so shed batches also
    /// subtract from the queued denominator.
    BatchShed,
    /// A validator's RPC returned a considered rejection (bad nonce, underpriced, invalid).
    /// Not alertable on its own: the rejection would repeat at every validator, the same way
    /// `tn_reth.invalid_txs_skipped_total` is expected traffic at execution.
    Rejected,
    /// The whole fallback chain was tried inside the transaction's budget and no validator
    /// accepted the connection or answered in time. Alertable on a sustained rate: the
    /// committee is unreachable from this node while it keeps accepting transactions.
    Unreached,
}

impl ForwardDropReason {
    /// Every variant, for zero-registration in [`ForwarderMetrics::init`].
    ///
    /// Hand-maintained: the exhaustive match in [`Self::label`] is the compile-time tripwire
    /// a new variant hits, and this list must be extended in the same edit - a variant
    /// missing here skips zero-registration silently, and the register-at-zero test cannot
    /// notice because it iterates this same list.
    const ALL: [Self; 5] = [
        Self::EmptyCommittee,
        Self::NoUsableEndpoint,
        Self::BatchShed,
        Self::Rejected,
        Self::Unreached,
    ];

    /// The `reason` label value this variant records under.
    const fn label(self) -> &'static str {
        match self {
            Self::EmptyCommittee => "empty_committee",
            Self::NoUsableEndpoint => "no_usable_endpoint",
            Self::BatchShed => "batch_shed",
            Self::Rejected => "rejected",
            Self::Unreached => "unreached",
        }
    }
}

/// Metrics for the observer transaction forwarder ([`crate::WorkerRpcForwarder`]).
///
/// The shed, abandoned, and dropped series count forwarding work the node deliberately gives
/// up on to keep its own resource use bounded; the queued series is the intake denominator
/// they read against. Forwarding is best-effort, so shedding is a correct outcome rather
/// than an error - but it is an *absorbed* failure, invisible until it is not, which is
/// exactly the category that needs to show up somewhere other than a `warn!` line. Transactions
/// counted here were accepted by this node's RPC and not handed to a validator by that
/// attempt; [`ForwardDropReason`] documents which drops are final and which go back to the pool.
///
/// Associated functions rather than instance handles, matching the epoch manager's
/// `record_provider_fault_retry`: the emission sites are inside a spawned task that holds no
/// metrics handle, and late binding to the current recorder is what makes the counters
/// observable from a unit test.
pub(crate) struct ForwarderMetrics;

impl ForwarderMetrics {
    /// Register every forwarder series (shed, abandoned, demoted, requeued, queued, and one
    /// dropped series per [`ForwardDropReason`]) with the current recorder without recording
    /// an event.
    ///
    /// Called from `WorkerRpcForwarder::new`, which runs at epoch start, long after the CLI
    /// installs the global recorder. The reason to register eagerly is the same one [`init`]
    /// documents for the block-building counters: an absent series is indistinguishable from a
    /// broken exporter or a mistyped name, and a `rate()` over a series whose first sample is
    /// already nonzero renders no step, so a one-shot shed burst on an otherwise quiet node
    /// would stay invisible.
    pub(crate) fn init() {
        metrics::counter!(FORWARDED_BATCHES_SHED).increment(0);
        metrics::counter!(FORWARDED_TXNS_ABANDONED).increment(0);
        metrics::counter!(FORWARD_ENDPOINTS_DEMOTED).increment(0);
        metrics::counter!(FORWARDED_TXNS_REQUEUED).increment(0);
        metrics::counter!(FORWARDED_TXNS_QUEUED).increment(0);
        ForwardDropReason::ALL.iter().for_each(|reason| {
            metrics::counter!(FORWARDED_TXNS_DROPPED, "reason" => reason.label()).increment(0);
        });
    }

    /// Count one sealed batch dropped without being forwarded because every forward permit was
    /// already in flight (`MAX_CONCURRENT_FORWARDS`).
    ///
    /// Alertable on a sustained rate. An occasional shed means one committee RPC window was
    /// slow; a sustained one means this node is accepting transactions faster than it can hand
    /// them on, and the transactions it sheds are lost to the network unless resubmitted.
    pub(crate) fn record_batch_shed() {
        metrics::counter!(FORWARDED_BATCHES_SHED).increment(1);
    }

    /// Count transactions left unforwarded because their batch hit `FORWARD_BATCH_BUDGET`.
    ///
    /// Distinct from a shed batch: these belong to a batch that was admitted and partly
    /// forwarded, so a nonzero value means committee endpoints were slow enough that one batch
    /// could not be walked inside its budget.
    pub(crate) fn record_txns_abandoned(count: u64) {
        metrics::counter!(FORWARDED_TXNS_ABANDONED).increment(count);
    }

    /// Count endpoints demoted from forward admission after a connection-level send failure
    /// (issue #1145).
    ///
    /// A steady rate on one committee means an advertised endpoint is persistently dead: every
    /// cooldown expiry admits one probe batch against it and demotes it again. That is the
    /// intended containment, but the advertisement itself is the operator-actionable fault.
    pub(crate) fn record_endpoints_demoted(count: u64) {
        metrics::counter!(FORWARD_ENDPOINTS_DEMOTED).increment(count);
    }

    /// Count transactions returned to the worker's own pool after their forward got no verdict
    /// (issue #1145). These are recovered, not lost: the batch builder repackages them on a
    /// later build. Pairs with [`Self::record_txns_abandoned`] and the `Unreached` label of
    /// [`Self::record_txns_dropped`], whose transactions are the ones this requeue rescues.
    pub(crate) fn record_txns_requeued(count: u64) {
        metrics::counter!(FORWARDED_TXNS_REQUEUED).increment(count);
    }

    /// Count transactions handed to the forwarder, before any admission decision.
    ///
    /// The denominator the drop and abandon counters read against: without it a dashboard can
    /// see what was lost but not what fraction of the intake that loss is.
    pub(crate) fn record_txns_queued(count: u64) {
        metrics::counter!(FORWARDED_TXNS_QUEUED).increment(count);
    }

    /// Count transactions the forwarding pipeline gave up on, labeled by [`ForwardDropReason`].
    ///
    /// Every call site pairs with a `warn!` (or a documented silent no-op), so the counter is
    /// the dashboard-visible half of a loss the logs already describe: transactions counted
    /// here were accepted by this node's RPC and never delivered to a validator.
    pub(crate) fn record_txns_dropped(reason: ForwardDropReason, count: u64) {
        metrics::counter!(FORWARDED_TXNS_DROPPED, "reason" => reason.label()).increment(count);
    }
}

/// Report sampled reth database metrics (table sizes, page usage, freelist).
///
/// Intended as a pre-scrape hook for the prometheus metrics endpoint. Lives here to keep
/// reth-db types out of the node crate.
pub fn report_db_metrics(db: &RethDb) {
    use reth_db::database_metrics::DatabaseMetrics;
    db.report_metrics();
}

/// Gas spent by the epoch-boundary system calls together.
///
/// The committed calls the closing block issues to conclude the epoch, NOT every system call it
/// makes — see the README for what is excluded and why. Each call gets its own budget, so this
/// total is not the figure [`SYSTEM_CALL_GAS_LIMIT`] bounds; [`EPOCH_CLOSE_SYSTEM_CALL_GAS_USED`]
/// is.
const EPOCH_CLOSE_GAS_USED: &str = "tn_reth.epoch_close_gas_used";

/// Gas spent by one epoch-boundary system call, labelled by `call`.
///
/// The `call` label is closed over the calls an epoch-closing block can make
/// (`registry_migration`, `apply_incentives`, `apply_slashes`, `conclude_epoch`,
/// `record_base_fees`), so it adds no cardinality that grows with the chain.
const EPOCH_CLOSE_SYSTEM_CALL_GAS_USED: &str = "tn_reth.epoch_close_system_call_gas_used";

/// The per-call gas budget every epoch-boundary system call runs under.
///
/// A constant, published so a dashboard or alert expresses headroom as a ratio against the budget
/// the node actually compiled in rather than against a hardcoded 100000000.
const EPOCH_CLOSE_SYSTEM_CALL_GAS_LIMIT: &str = "tn_reth.epoch_close_system_call_gas_limit";

/// Gas one epoch-boundary system call may spend before the close logs a warning: four fifths of
/// [`SYSTEM_CALL_GAS_LIMIT`], or 80M gas.
///
/// A warning here is not an incident, it is lead time. [`SYSTEM_CALL_GAS_LIMIT`] participates in
/// consensus, so raising it is a lockstep fleet upgrade that has to be scheduled; a threshold
/// that only fired once the budget was gone would provide none.
///
/// `registry_migration` is the call the 100M headroom exists for — it walks every validator in one
/// shot — so it is the one that may legitimately trip this threshold, once, at the fork boundary.
/// The README's alert query excludes it for that reason.
const EPOCH_CLOSE_GAS_WARN_THRESHOLD: u64 = SYSTEM_CALL_GAS_LIMIT / 5 * 4;

/// The warning must land strictly before the halt it predicts, or it is not a warning.
///
/// A build error rather than a test: the threshold is derived from [`SYSTEM_CALL_GAS_LIMIT`], so
/// the only way to break the ordering is to edit this arithmetic, and that should not compile.
const _: () = assert!(EPOCH_CLOSE_GAS_WARN_THRESHOLD < SYSTEM_CALL_GAS_LIMIT);

/// Whether one call has crossed [`EPOCH_CLOSE_GAS_WARN_THRESHOLD`].
///
/// Split out from the recording so the boundary is testable without capturing a log line.
const fn is_thin_headroom(gas: u64) -> bool {
    gas >= EPOCH_CLOSE_GAS_WARN_THRESHOLD
}

/// The gas a call SPENT, before EIP-3529 refunds.
///
/// This is the quantity a gas limit bounds, and it is not what `ExecutionResult::gas_used`
/// carries. revm distinguishes `Gas::spent()` (`limit - remaining`) from `Gas::used()`
/// (`spent - refunded`), and `gas_used` is the latter. The refund is computed after execution and
/// capped at a fifth of the spend, so it never buys the call more room: a call that exhausts its
/// limit still halts, while reporting up to a fifth less than it spent.
///
/// Publishing `gas_used` would therefore slide the warning threshold with the refund. A call
/// earning the full refund reports four fifths of its spend, so the 80%-of-budget warning would
/// not fire until the spend reached the whole 100M — the halt the warning exists to predict. The
/// refundless case is just as wrong in the other direction: a `conclude_epoch` that exhausted its
/// budget would be published as 80M, the figure a dashboard reads as a fifth still to spare.
///
/// Saturating: two EVM gas figures cannot approach `u64::MAX`, but this runs on a path that is
/// fatal to the block, so a metric must not be a panic site there.
pub(crate) const fn gas_spent(gas_used: u64, gas_refunded: u64) -> u64 {
    gas_used.saturating_add(gas_refunded)
}

/// What each epoch-boundary system call spent, in the order the closing block issued them.
///
/// A list, not a field per call. The boundary's call set is not stable — two calls, then three
/// (#1012), then four (#1101), plus the legacy pre-fork pair and the one-shot fork migration — and
/// a fixed struct has to be re-typed for every change. A list absorbs a call that was added, one
/// that was skipped (`record_base_fees` issues nothing when no worker is EIP-1559), and the legacy
/// path's shorter sequence, with no type churn.
///
/// A call that did not run pushes nothing, so it leaves its series untouched rather than
/// publishing a zero that reads as "ran, and was free".
#[derive(Debug, Default)]
pub(crate) struct EpochCloseGas(Vec<(&'static str, u64)>);

impl EpochCloseGas {
    /// Record what one system call spent, under the `call` label it is published with.
    pub(crate) fn push(&mut self, call: &'static str, gas: u64) {
        self.0.push((call, gas));
    }

    /// Gas spent by every call that ran.
    ///
    /// Saturating: a handful of EVM gas figures cannot sum to anywhere near `u64::MAX`, but the
    /// total is a metric and must not be a panic site on a path that is fatal to the block.
    fn total(&self) -> u64 {
        self.0.iter().fold(0u64, |total, (_, gas)| total.saturating_add(*gas))
    }

    /// Whether no epoch-boundary system call was recorded.
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Record the gas an epoch-closing block's system calls spent, and warn on thin headroom.
///
/// Called once per epoch close from `TNBlockExecutor::finish`, after the boundary calls have
/// succeeded: a failed call aborts the block, so there is no partial epoch close to report gas
/// for. Every node runs that closing block, so the series is fleet-wide rather than
/// proposer-only, and it covers closing blocks re-executed while syncing — a replayed value is
/// still a true observation of what that epoch cost, which is what makes a `max_over_time` query
/// over these gauges meaningful.
///
/// Publishes nothing at all when no call was recorded, rather than a zero total that would read
/// as a free boundary — the same rule the per-call series follows.
pub(crate) fn record_epoch_close_gas(gas: &EpochCloseGas) {
    if gas.is_empty() {
        return;
    }

    metrics::describe_gauge!(
        EPOCH_CLOSE_GAS_USED,
        "Gas spent by the epoch-boundary system calls together (not every system call of the \
         closing block)"
    );
    metrics::describe_gauge!(
        EPOCH_CLOSE_SYSTEM_CALL_GAS_USED,
        "Gas spent by each epoch-boundary system call, before EIP-3529 refunds"
    );
    metrics::describe_gauge!(
        EPOCH_CLOSE_SYSTEM_CALL_GAS_LIMIT,
        "Gas budget each epoch-boundary system call runs under"
    );

    metrics::gauge!(EPOCH_CLOSE_GAS_USED).set(gas.total() as f64);
    metrics::gauge!(EPOCH_CLOSE_SYSTEM_CALL_GAS_LIMIT).set(SYSTEM_CALL_GAS_LIMIT as f64);

    for &(call, spent) in &gas.0 {
        metrics::gauge!(EPOCH_CLOSE_SYSTEM_CALL_GAS_USED, "call" => call).set(spent as f64);

        if is_thin_headroom(spent) {
            warn!(
                target: "engine",
                call,
                gas_spent = spent,
                gas_limit = SYSTEM_CALL_GAS_LIMIT,
                "epoch-boundary system call is approaching its gas budget — exhausting it halts \
                 the chain at an epoch boundary",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshot};

    #[test]
    fn test_metrics_register_and_update() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // construct directly (not via the static) so handles bind to the local recorder
            let metrics = RethEnvMetrics::new_with_labels(Vec::<metrics::Label>::new());
            metrics.unrecoverable_txs_dropped_total.increment(2);
            metrics.invalid_txs_skipped_total.increment(5);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered"))
        };

        let (_, _, _, value) = find("tn_reth.unrecoverable_txs_dropped_total");
        assert!(matches!(value, DebugValue::Counter(2)));
        let (_, _, _, value) = find("tn_reth.invalid_txs_skipped_total");
        assert!(matches!(value, DebugValue::Counter(5)));
    }

    /// Construction alone must register both series at zero, with no drop having happened.
    ///
    /// This is the property [`init`] relies on: a healthy node that never drops a transaction
    /// still exports both counters, so `absent()` distinguishes "no drops" from "exporter
    /// broken" and the first drop renders as a step from zero rather than as a series that
    /// appears already nonzero.
    #[test]
    fn test_metrics_register_at_zero_without_any_event() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            // `new_with_labels` rather than `default()`: the derive caches `default()` in a
            // per-type `OnceLock`, so it would bind to whichever recorder ran first in this
            // process and make the assertion depend on test ordering
            let _metrics = RethEnvMetrics::new_with_labels(Vec::<metrics::Label>::new());
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let find = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("metric {name} not registered without an event"))
        };

        let (_, _, _, value) = find("tn_reth.unrecoverable_txs_dropped_total");
        assert!(matches!(value, DebugValue::Counter(0)));
        let (_, _, _, value) = find("tn_reth.invalid_txs_skipped_total");
        assert!(matches!(value, DebugValue::Counter(0)));
    }

    /// [`ForwarderMetrics::init`] must register every forwarder series at zero, and each record
    /// helper must move only its own series.
    ///
    /// The negative half matters as much as the positive one: a single mistyped counter name
    /// would still produce a nonzero series, so each assertion pins the other counter at the
    /// value `init` left it with.
    #[test]
    fn test_forwarder_metrics_register_at_zero_and_count_separately() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            ForwarderMetrics::init();
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(matches!(series(&snapshot, FORWARDED_BATCHES_SHED), DebugValue::Counter(0)));
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_ABANDONED), DebugValue::Counter(0)));
        assert!(matches!(series(&snapshot, FORWARD_ENDPOINTS_DEMOTED), DebugValue::Counter(0)));
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_REQUEUED), DebugValue::Counter(0)));

        metrics::with_local_recorder(&recorder, || {
            ForwarderMetrics::record_batch_shed();
            ForwarderMetrics::record_batch_shed();
            ForwarderMetrics::record_txns_abandoned(7);
            ForwarderMetrics::record_endpoints_demoted(3);
            ForwarderMetrics::record_txns_requeued(5);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(matches!(series(&snapshot, FORWARDED_BATCHES_SHED), DebugValue::Counter(2)));
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_ABANDONED), DebugValue::Counter(7)));
        assert!(matches!(series(&snapshot, FORWARD_ENDPOINTS_DEMOTED), DebugValue::Counter(3)));
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_REQUEUED), DebugValue::Counter(5)));
    }

    /// [`ForwarderMetrics::init`] must register the queued base series and one dropped series
    /// per [`ForwardDropReason`] at zero, and a recorded drop must move only its own reason's
    /// series.
    ///
    /// The per-reason zero registrations carry the same weight as the unlabeled ones: a reason
    /// that only appears once it fires cannot be told apart from a mistyped label value, and
    /// its first `rate()` step is lost.
    #[test]
    fn test_forward_drop_reasons_register_at_zero_and_count_separately() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            ForwarderMetrics::init();
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_QUEUED), DebugValue::Counter(0)));
        ForwardDropReason::ALL.into_iter().for_each(|reason| {
            assert!(
                matches!(reason_series(&snapshot, reason.label()), DebugValue::Counter(0)),
                "reason {} must be registered at zero by init",
                reason.label()
            );
        });

        metrics::with_local_recorder(&recorder, || {
            ForwarderMetrics::record_txns_queued(9);
            ForwarderMetrics::record_txns_dropped(ForwardDropReason::BatchShed, 3);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(matches!(series(&snapshot, FORWARDED_TXNS_QUEUED), DebugValue::Counter(9)));
        assert!(matches!(reason_series(&snapshot, "batch_shed"), DebugValue::Counter(3)));
        ForwardDropReason::ALL
            .into_iter()
            .filter(|reason| *reason != ForwardDropReason::BatchShed)
            .for_each(|reason| {
                assert!(
                    matches!(reason_series(&snapshot, reason.label()), DebugValue::Counter(0)),
                    "a batch_shed drop must not move the {} series",
                    reason.label()
                );
            });
    }

    /// One series of a [`DebuggingRecorder`] snapshot, looked up by metric name.
    ///
    /// A free function rather than the closures the two tests above use, because this one is
    /// called against two different snapshots: it has to borrow from its argument rather than
    /// from a captured binding, and a closure cannot express that lifetime. Returning an owned
    /// value instead is not an option either, since `DebugValue` is not `Clone`.
    fn series<'a>(
        snapshot: &'a [(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
    ) -> &'a DebugValue {
        snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == name)
            .map(|(.., value)| value)
            .unwrap_or_else(|| panic!("metric {name} not registered without an event"))
    }

    /// The dropped-transactions series for one `reason` label value.
    ///
    /// [`series`] cannot serve here: all reasons share the metric name, so a name-only lookup
    /// returns whichever series the snapshot happens to list first.
    fn reason_series<'a>(
        snapshot: &'a [(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        reason: &str,
    ) -> &'a DebugValue {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == FORWARDED_TXNS_DROPPED
                    && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
            })
            .map(|(.., value)| value)
            .unwrap_or_else(|| panic!("dropped series for reason {reason} not registered"))
    }

    /// Read one gauge out of a snapshot, selecting by `call` label when one is given.
    ///
    /// Generic over the description slot so the recorder's own string type never has to be named
    /// here.
    fn gauge<D>(
        snapshot: &[(metrics_util::CompositeKey, Option<metrics::Unit>, D, DebugValue)],
        name: &str,
        call: Option<&str>,
    ) -> Option<f64> {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == name
                    && call.is_none_or(|call| {
                        key.key().labels().any(|l| l.key() == "call" && l.value() == call)
                    })
            })
            .and_then(|(.., value)| match value {
                DebugValue::Gauge(g) => Some(g.0),
                DebugValue::Counter(_) | DebugValue::Histogram(_) => None,
            })
    }

    /// Record the given calls under a local recorder and return the snapshot.
    fn record(calls: &[(&'static str, u64)]) -> Snapshot {
        let mut gas = EpochCloseGas::default();
        for &(call, spent) in calls {
            gas.push(call, spent);
        }

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || record_epoch_close_gas(&gas));
        snapshotter.snapshot()
    }

    /// A successful call's spend is its `gas_used` plus the refund it earned.
    ///
    /// The whole point of [`gas_spent`]: `ExecutionResult::gas_used` is already net of the refund,
    /// and these gauges have to publish the figure the budget bounds. Returning `gas_used` here
    /// would understate a call's budget consumption by up to a fifth.
    #[test]
    fn test_gas_spent_adds_the_refund_back() {
        // the worst case: a full EIP-3529 refund on a call that exhausted its budget
        assert_eq!(
            gas_spent(80_000_000, 20_000_000),
            SYSTEM_CALL_GAS_LIMIT,
            "spend is used + refunded"
        );
        assert_eq!(gas_spent(21_000, 0), 21_000, "no refund leaves the figure unchanged");
    }

    #[test]
    fn test_records_a_series_per_call_plus_the_total_and_the_budget() {
        let snapshot = record(&[
            ("apply_incentives", 1_000),
            ("apply_slashes", 2_000),
            ("conclude_epoch", 3_000),
            ("record_base_fees", 4_000),
        ])
        .into_vec();

        assert_eq!(
            gauge(&snapshot, EPOCH_CLOSE_GAS_USED, None),
            Some(10_000.0),
            "the total gauge must sum every call that ran"
        );
        assert_eq!(
            gauge(&snapshot, EPOCH_CLOSE_SYSTEM_CALL_GAS_LIMIT, None),
            Some(SYSTEM_CALL_GAS_LIMIT as f64),
            "the budget gauge must publish the compiled-in limit"
        );

        for (call, expected) in [
            ("apply_incentives", 1_000.0),
            ("apply_slashes", 2_000.0),
            ("conclude_epoch", 3_000.0),
            ("record_base_fees", 4_000.0),
        ] {
            assert_eq!(
                gauge(&snapshot, EPOCH_CLOSE_SYSTEM_CALL_GAS_USED, Some(call)),
                Some(expected),
                "call={call} gas"
            );
        }
    }

    /// A skipped call must leave its series absent, not publish a zero.
    ///
    /// `record_base_fees` is the live case: a closing block with no EIP-1559 worker issues no
    /// system call at all. `registry_migration` is the other, on every close but one.
    #[test]
    fn test_a_call_that_did_not_run_publishes_no_series() {
        let snapshot = record(&[("apply_incentives", 2_000), ("conclude_epoch", 3_000)]).into_vec();

        for absent in ["registry_migration", "apply_slashes", "record_base_fees"] {
            assert!(
                gauge(&snapshot, EPOCH_CLOSE_SYSTEM_CALL_GAS_USED, Some(absent)).is_none(),
                "a skipped call ({absent}) must not publish a zero"
            );
        }
        assert_eq!(
            gauge(&snapshot, EPOCH_CLOSE_SYSTEM_CALL_GAS_USED, Some("conclude_epoch")),
            Some(3_000.0),
            "the calls that ran must still be recorded"
        );
    }

    /// The same rule applied to the aggregate gauges: no observation, no series.
    ///
    /// A zero total would read as an epoch boundary that concluded for free, which is exactly the
    /// misreading the per-call rule above exists to prevent.
    #[test]
    fn test_a_close_that_recorded_nothing_publishes_nothing() {
        let snapshot = record(&[]).into_vec();
        assert!(snapshot.is_empty(), "an empty close must publish no series at all: {snapshot:?}");
    }

    /// Pin the warning boundary itself, not just the constant's arithmetic.
    ///
    /// The warning is the only signal that fires without a dashboard, so an inverted or deleted
    /// comparison has to fail a test. Asserting on the constant alone would catch neither. (That
    /// the threshold sits below the budget at all is a build-time assertion, not a test.)
    ///
    /// The literal is deliberate: raising [`SYSTEM_CALL_GAS_LIMIT`] should break this test, since
    /// the README quotes both figures in its alert query.
    #[test]
    fn test_warn_fires_at_four_fifths_of_the_budget_and_not_below() {
        assert_eq!(EPOCH_CLOSE_GAS_WARN_THRESHOLD, 80_000_000);

        assert!(!is_thin_headroom(0), "an idle boundary must not warn");
        assert!(
            !is_thin_headroom(EPOCH_CLOSE_GAS_WARN_THRESHOLD - 1),
            "one gas below the threshold must not warn"
        );
        assert!(is_thin_headroom(EPOCH_CLOSE_GAS_WARN_THRESHOLD), "the threshold itself must warn");
        assert!(is_thin_headroom(SYSTEM_CALL_GAS_LIMIT), "an exhausted budget must warn");
    }

    /// Recording twice in one process must reach the recorder installed for the second close.
    ///
    /// This is the ordering hazard that rules out the `Metrics` derive for these gauges: handles
    /// cached by `Default::default()` bind to whichever recorder ran first, so the second close
    /// would land in the first recorder and this snapshot would come back empty.
    #[test]
    fn test_a_second_close_binds_to_the_live_recorder() {
        let first = record(&[("apply_incentives", 1), ("conclude_epoch", 1)]).into_vec();
        let second = record(&[("apply_incentives", 2_000), ("conclude_epoch", 3_000)]).into_vec();

        assert_eq!(gauge(&first, EPOCH_CLOSE_GAS_USED, None), Some(2.0));
        assert_eq!(
            gauge(&second, EPOCH_CLOSE_GAS_USED, None),
            Some(5_000.0),
            "the second close must be recorded against the recorder installed for it"
        );
    }
}
