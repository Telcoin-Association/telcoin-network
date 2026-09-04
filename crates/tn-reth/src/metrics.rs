//! Prometheus metrics for the reth execution environment.
//!
//! The counter series are exported under the `tn_reth` scope, in three unrelated groups.
//!
//! Block building. Two series count transactions that `build_block_from_batch_payload`
//! declines to include in a block: `tn_reth.unrecoverable_txs_dropped_total` (alertable - a
//! certified batch carried a transaction whose signer could not be recovered) and
//! `tn_reth.invalid_txs_skipped_total` (expected - duplicates/invalid transactions across
//! independently-batching workers, labeled by `reason`, see [`InvalidTxSkipReason`]). See
//! [`RethEnvMetrics`] and the skip-reason docs for per-series semantics.
//!
//! Observer transaction forwarding. A base series counts every transaction handed to the
//! forwarder ([`crate::WorkerRpcForwarder`]): `tn_reth.forwarded_txns_queued_total`. Read
//! against it, `tn_reth.forwarded_txns_dropped_total` (labeled by `reason`, see
//! [`ForwardDropReason`]) counts the transactions the pipeline gave up on, and two series count
//! work the forwarder sheds to stay within its own bounds:
//! `tn_reth.forwarded_batches_shed_total` and `tn_reth.forwarded_txns_abandoned_total`. Two more
//! count the delivery feedback added for issue #1145:
//! `tn_reth.forward_endpoints_demoted_total` and `tn_reth.forwarded_txns_requeued_total`. One
//! further series, `tn_reth.forwarded_rejections_overridden_total`, is an integrity signal
//! rather than a loss: it counts validator verdicts that contradicted each other on one
//! forwarded transaction (issue #1167). See [`ForwarderMetrics`] for per-series semantics.
//!
//! Worker pool canonical-state subscription health. `tn_reth.canon_state_lagged_total`
//! (alertable) counts broadcast lag events observed by the pool maintenance task;
//! `tn_reth.canon_state_notifications_missed_total` carries the magnitude behind each event;
//! and `tn_reth.canon_state_resync_read_failures_total` counts canonical account reads the
//! post-lag resync absorbed and retried. See [`RethEnvMetrics`] for per-series semantics.
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
//! Three more gauges report what a region-aware committee draw (#1279) seated:
//! [`EPOCH_CLOSE_REGION_POOL`] and [`EPOCH_CLOSE_REGION_SEATED`] (both labelled by `region`) and
//! [`EPOCH_CLOSE_SINGLETON_REGIONS`]. A region assignment is a governance attestation with a
//! guaranteed seat attached (#1327), and nothing else makes that advantage visible. See
//! [`record_region_seats`].
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

use alloy_evm::InvalidTxError;
use reth_metrics::{metrics::Counter, Metrics};
use reth_revm::context::result::InvalidTransaction;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::LazyLock,
};
use tn_types::Address;
use tracing::{info, warn};

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

/// Register every [`RethEnvMetrics`] counter and every labeled skip series with the global
/// recorder without recording an event.
///
/// For the derive-based counters, registration happens when the `LazyLock` is first forced,
/// and the sites that force it are drop paths a healthy node never takes plus the pool
/// maintenance task's lag recovery (`mark_drifted` in `txn_pool.rs`), which a healthy node
/// also never reaches. Without this call the series would simply not exist in a scrape until
/// the first drop, which breaks the counters in the two ways that matter: an absent series is
/// indistinguishable from a broken exporter or a mistyped metric name, so the "alert on any
/// nonzero value" rule can never be verified in its negative case; and `rate()` over a series
/// whose very first sample is already nonzero renders no step, so a one-shot drop burst on an
/// otherwise quiet node stays invisible on the dashboard. The labeled skip series need the
/// same treatment per label value, which [`register_invalid_tx_skip_series`] provides.
///
/// Called from [`crate::RethEnv::new`], which runs after `tn_metrics::install_recorder`, so
/// every counter baselines at zero from process start . . . the same way the worker and
/// executor counters they sit beside on the dashboard already do. A lag event is exactly the
/// moment an operator needs the series to already exist with a zero baseline, or `rate()`
/// renders no step for the first event.
pub(crate) fn init() {
    LazyLock::force(&RETH_METRICS);
    register_invalid_tx_skip_series();
}

/// Metrics for the execution environment: block building from certified batch payloads, plus
/// the worker pool's canonical-state subscription health.
///
/// The unrecoverable-drop counter and the labeled skip series under [`INVALID_TXS_SKIPPED`]
/// cover transactions that `build_block_from_batch_payload` silently declines to include in
/// the block it returns. The two drop sites have opposite expectedness, so they are counted
/// separately rather than summed - see the per-series docs. The skip series lives outside
/// this struct because it carries a `reason` label, which the derive cannot express (see the
/// module docs).
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

    /// Canonical-state broadcast lag events observed by the worker pool maintenance task.
    ///
    /// Alert on any nonzero value. Each increment means the pool task fell more than the
    /// broadcast channel's capacity behind `notify_canon_state` and lost `Commit`
    /// notifications it can never replay; the task marks every pool sender dirty and reloads
    /// canonical account state in bounded chunks to recover (see `txn_pool.rs`). A nonzero
    /// rate means the worker cannot keep up with per-round pool maintenance - resync bounds
    /// the damage, but sustained lag warrants investigation. The magnitude of each event
    /// lives in [`RethEnvMetrics::canon_state_notifications_missed_total`].
    pub(crate) canon_state_lagged_total: Counter,

    /// Canonical-state notifications the worker pool maintenance task lost to broadcast lag.
    ///
    /// The magnitude behind [`RethEnvMetrics::canon_state_lagged_total`], which counts lag
    /// EVENTS: one increment there reads identically whether one notification was dropped or
    /// ten thousand were, and the count otherwise exists only in the `missed` field of the
    /// accompanying `warn!`. Read the two together - `rate(missed) / rate(lagged)` is the
    /// average gap per event, and it is the gap, not the event count, that says how much
    /// sender state the pool had to rebuild.
    pub(crate) canon_state_notifications_missed_total: Counter,

    /// Canonical account reads that failed while resyncing pool senders after a lag event.
    ///
    /// Alert on a sustained rate, not on any nonzero value. A failed read leaves its sender
    /// in the dirty set to be retried on a later maintenance iteration, so isolated
    /// increments are the intended absorption of a transient provider fault. A rate that
    /// does not fall back to zero means the resync is stuck retrying the same senders and
    /// the pool's view of them stays stale.
    pub(crate) canon_state_resync_read_failures_total: Counter,
}

/// Counter name for transactions skipped while building a block because the EVM rejected them
/// as invalid, kept next to its only emission helper so the registration in
/// [`register_invalid_tx_skip_series`] and the increment in [`record_invalid_tx_skipped`]
/// cannot drift apart.
///
/// Not an error signal, and not alertable: workers batch independently, so the same
/// transaction can legitimately reach execution twice within one consensus output and the
/// second copy is skipped as a duplicate. A steady nonzero rate is normal operation. The
/// alertable counter is [`RethEnvMetrics::unrecoverable_txs_dropped_total`].
///
/// One series per [`InvalidTxSkipReason`], so a dashboard reads the total as a sum over the
/// `reason` label. The label restores in metrics what issue #1263 / PR #1274 moved out of the
/// default operator log: the skip cause is now a `debug!` line, so without the label a
/// climbing counter cannot be told apart into expected duplicates and everything else
/// (issue #1284).
const INVALID_TXS_SKIPPED: &str = "tn_reth.invalid_txs_skipped_total";

/// Why the EVM rejected a transaction that was skipped while building a block.
///
/// The variants label [`INVALID_TXS_SKIPPED`], one series per reason, all registered at zero
/// by [`register_invalid_tx_skip_series`]. The set is closed on purpose: the underlying
/// [`InvalidTransaction`] enum is wide and version-owned by revm, so mapping it onto a small
/// fixed vocabulary keeps the label cardinality bounded across dependency upgrades. The named
/// variants are the causes block building actually produces from certified batches; everything
/// else collapses into [`Self::Other`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum InvalidTxSkipReason {
    /// The transaction's nonce was already consumed. The expected-duplicate signal: workers
    /// batch independently, so the same transaction can appear in two batches of one consensus
    /// output, and every copy after the first fails execution with exactly this cause. A
    /// steady rate here is normal operation.
    NonceTooLow,
    /// The transaction's nonce is ahead of the sender's state, i.e. a later transaction
    /// arrived without its predecessor. Batches admit transactions the pool ordered, so a
    /// sustained rate means gaps between what workers batch and what execution sees.
    NonceTooHigh,
    /// The sender could not cover `value + gas_limit * gas_price` at execution time. Balance
    /// is validated at batch admission, so this fires when earlier transactions in the same
    /// consensus output drained the account first.
    InsufficientFunds,
    /// The transaction's fee cap fell below the block base fee. Batches are validated against
    /// the base fee their worker advertised, so this cause marks a base-fee disagreement
    /// between batch admission and execution.
    FeeCapBelowBaseFee,
    /// Every other [`InvalidTransaction`] cause, and any invalid-transaction error that does
    /// not expose one. The bounded catch-all that keeps the label set fixed; the `debug!` line
    /// at the skip site still carries the exact error for anyone who needs it.
    Other,
}

impl InvalidTxSkipReason {
    /// Every variant, for zero-registration in [`register_invalid_tx_skip_series`].
    ///
    /// Hand-maintained: the exhaustive match in [`Self::label`] is the compile-time tripwire
    /// a new variant hits, and this list must be extended in the same edit - a variant
    /// missing here skips zero-registration silently, and the register-at-zero test cannot
    /// notice because it iterates this same list.
    const ALL: [Self; 5] = [
        Self::NonceTooLow,
        Self::NonceTooHigh,
        Self::InsufficientFunds,
        Self::FeeCapBelowBaseFee,
        Self::Other,
    ];

    /// The `reason` label value this variant records under.
    const fn label(self) -> &'static str {
        match self {
            Self::NonceTooLow => "nonce_too_low",
            Self::NonceTooHigh => "nonce_too_high",
            Self::InsufficientFunds => "insufficient_funds",
            Self::FeeCapBelowBaseFee => "fee_cap_below_base_fee",
            Self::Other => "other",
        }
    }

    /// Map the EVM's rejection of one transaction onto the bounded label set.
    ///
    /// The match on [`InvalidTransaction`] is exhaustive so that a revm upgrade that adds a
    /// variant fails to compile here rather than silently landing in a bucket nobody chose.
    /// An error that exposes no underlying [`InvalidTransaction`] is [`Self::Other`]: it
    /// cannot be classified, and the catch-all is exactly the series for that.
    ///
    /// The parameter is a trait object because that is the shape the caller holds:
    /// `BlockValidationError::InvalidTx` stores `Box<dyn InvalidTxError>`, so the erasure is
    /// upstream's, and a generic here would monomorphize over the same `dyn` type anyway.
    pub(crate) fn classify(error: &dyn InvalidTxError) -> Self {
        error.as_invalid_tx_err().map_or(Self::Other, |cause| match cause {
            InvalidTransaction::NonceTooLow { .. } => Self::NonceTooLow,
            InvalidTransaction::NonceTooHigh { .. } => Self::NonceTooHigh,
            InvalidTransaction::LackOfFundForMaxFee { .. } => Self::InsufficientFunds,
            InvalidTransaction::GasPriceLessThanBasefee => Self::FeeCapBelowBaseFee,
            InvalidTransaction::PriorityFeeGreaterThanMaxFee
            | InvalidTransaction::CallerGasLimitMoreThanBlock
            | InvalidTransaction::CallGasCostMoreThanGasLimit { .. }
            | InvalidTransaction::GasFloorMoreThanGasLimit { .. }
            | InvalidTransaction::RejectCallerWithCode
            | InvalidTransaction::OverflowPaymentInTransaction
            | InvalidTransaction::NonceOverflowInTransaction
            | InvalidTransaction::CreateInitCodeSizeLimit
            | InvalidTransaction::InvalidChainId
            | InvalidTransaction::MissingChainId
            | InvalidTransaction::TxGasLimitGreaterThanCap { .. }
            | InvalidTransaction::AccessListNotSupported
            | InvalidTransaction::MaxFeePerBlobGasNotSupported
            | InvalidTransaction::BlobVersionedHashesNotSupported
            | InvalidTransaction::BlobGasPriceGreaterThanMax { .. }
            | InvalidTransaction::EmptyBlobs
            | InvalidTransaction::BlobCreateTransaction
            | InvalidTransaction::TooManyBlobs { .. }
            | InvalidTransaction::BlobVersionNotSupported
            | InvalidTransaction::AuthorizationListNotSupported
            | InvalidTransaction::AuthorizationListInvalidFields
            | InvalidTransaction::EmptyAuthorizationList
            | InvalidTransaction::Eip2930NotSupported
            | InvalidTransaction::Eip1559NotSupported
            | InvalidTransaction::Eip4844NotSupported
            | InvalidTransaction::Eip7702NotSupported
            | InvalidTransaction::Eip7873NotSupported
            | InvalidTransaction::Eip7873MissingTarget
            | InvalidTransaction::Str(_) => Self::Other,
        })
    }
}

/// Register one skip series per [`InvalidTxSkipReason`] with the current recorder without
/// recording an event.
///
/// Called from [`init`]. The per-reason zero registrations carry the same weight as the
/// unlabeled ones documented there: a reason that only appears once it fires cannot be told
/// apart from a mistyped label value, and its first `rate()` step is lost.
fn register_invalid_tx_skip_series() {
    InvalidTxSkipReason::ALL.iter().for_each(|reason| {
        metrics::counter!(INVALID_TXS_SKIPPED, "reason" => reason.label()).increment(0);
    });
}

/// Count one transaction skipped while building a block, labeled by [`InvalidTxSkipReason`].
///
/// The call site pairs with a `debug!` line carrying the exact error, so the counter is the
/// dashboard-visible half of a skip the debug log already describes (issue #1284). The macro
/// path re-resolves the recorder on every call, matching [`ForwarderMetrics`] and keeping the
/// series observable from a unit test.
pub(crate) fn record_invalid_tx_skipped(reason: InvalidTxSkipReason) {
    metrics::counter!(INVALID_TXS_SKIPPED, "reason" => reason.label()).increment(1);
}

/// Counter names for the observer forwarder, kept next to their only emission sites so the
/// registration in [`ForwarderMetrics::init`] and the increments below cannot drift apart.
const FORWARDED_BATCHES_SHED: &str = "tn_reth.forwarded_batches_shed_total";
const FORWARDED_TXNS_ABANDONED: &str = "tn_reth.forwarded_txns_abandoned_total";
const FORWARD_ENDPOINTS_DEMOTED: &str = "tn_reth.forward_endpoints_demoted_total";
const FORWARDED_TXNS_REQUEUED: &str = "tn_reth.forwarded_txns_requeued_total";
const FORWARDED_TXNS_QUEUED: &str = "tn_reth.forwarded_txns_queued_total";
const FORWARDED_TXNS_DROPPED: &str = "tn_reth.forwarded_txns_dropped_total";
const FORWARDED_REJECTIONS_OVERRIDDEN: &str = "tn_reth.forwarded_rejections_overridden_total";

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
    /// A validator's RPC returned a considered rejection (bad nonce, underpriced, invalid)
    /// that stood: a second validator confirmed it, or no further validator was reachable to
    /// contradict it (issue #1167). Not alertable on its own: an honest rejection repeats at
    /// every validator, the same way `tn_reth.invalid_txs_skipped_total` is expected traffic
    /// at execution. A rejection a later delivery contradicted is not counted here - the
    /// transaction was delivered, and the contradiction is surfaced on
    /// `tn_reth.forwarded_rejections_overridden_total`.
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
/// counted here were accepted by this node's RPC and not delivered to a validator by that
/// attempt; [`ForwardDropReason`] documents which drops are final and which go back to the pool.
///
/// The overridden series is different in kind: it counts validator verdicts that contradicted
/// each other on one transaction, which is an integrity signal about the committee rather than
/// a resource decision by this node (issue #1167).
///
/// Associated functions rather than instance handles, matching the epoch manager's
/// `record_provider_fault_retry`: the emission sites are inside a spawned task that holds no
/// metrics handle, and late binding to the current recorder is what makes the counters
/// observable from a unit test.
pub(crate) struct ForwarderMetrics;

impl ForwarderMetrics {
    /// Register every forwarder series (shed, abandoned, demoted, requeued, queued, one
    /// dropped series per [`ForwardDropReason`], and the rejection-overridden series) with the
    /// current recorder without recording an event.
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
        metrics::counter!(FORWARDED_REJECTIONS_OVERRIDDEN).increment(0);
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

    /// Count one considered rejection that a later validator contradicted by accepting the
    /// same transaction (issue #1167).
    ///
    /// Alertable on a sustained rate. Honest validators share consensus state, so their
    /// verdicts on one transaction should agree; a rejection followed by an acceptance means
    /// the rejecting validator answered from divergent state or lied. A rare blip can be an
    /// honest race (the sender's account state moved between the two calls); a sustained rate,
    /// or any rate whose `rejected_by` log lines point at one validator, is a byzantine
    /// signal.
    pub(crate) fn record_rejection_overridden() {
        metrics::counter!(FORWARDED_REJECTIONS_OVERRIDDEN).increment(1);
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

/// Eligible validators declaring each region at a region-aware epoch close, labelled by `region`.
///
/// The `region` label is the on-chain `u8` region id, so the series count is bounded by 256 and
/// does not grow with the chain. Region `0` (unassigned) is published too, so a dashboard sees the
/// whole pool the draw ran over.
const EPOCH_CLOSE_REGION_POOL: &str = "tn_reth.epoch_close_region_pool";

/// Committee seats each region won at a region-aware epoch close, labelled by `region`.
const EPOCH_CLOSE_REGION_SEATED: &str = "tn_reth.epoch_close_region_seated";

/// Assigned regions with exactly one eligible validator at a region-aware epoch close.
///
/// Each such region holds a guaranteed round-one seat (#1327), so a nonzero value is the signal an
/// operator asks governance to verify a region claim on.
const EPOCH_CLOSE_SINGLETON_REGIONS: &str = "tn_reth.epoch_close_singleton_regions";

/// One region's share of a region-aware committee draw (#1327).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct RegionSeats {
    /// Eligible validators carrying this region id in the pool the draw ran over.
    pool: usize,
    /// Validators from this region seated in the trimmed committee.
    seated: usize,
}

impl RegionSeats {
    /// Count one more pool member, seated or not.
    fn plus(self, seated: bool) -> Self {
        Self {
            pool: self.pool.saturating_add(1),
            seated: self.seated.saturating_add(usize::from(seated)),
        }
    }

    /// Eligible validators carrying this region id.
    pub(crate) const fn pool(self) -> usize {
        self.pool
    }

    /// Validators from this region the committee seated.
    pub(crate) const fn seated(self) -> usize {
        self.seated
    }
}

/// Per-region pool and seat counts for one region-aware committee draw (#1327), in ascending
/// region id order.
///
/// Built by tn-reth's `assemble_new_committee` once the drawn committee is trimmed and
/// length-checked, and published by [`record_region_seats`]. Region `0` (unassigned) is tallied
/// like any other so the whole pool is visible, but only an assigned region can hold a guaranteed
/// seat, so [`Self::singleton_regions`] skips it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegionSeatReport(BTreeMap<u8, RegionSeats>);

impl RegionSeatReport {
    /// Tally `pool` (every `(address, region)` the draw ran over) against `committee` (the trimmed
    /// result).
    ///
    /// The committee is a prefix of the pool's ordering, so every seated address is in the pool;
    /// an address that is not counts toward no region.
    pub(crate) fn tally(pool: &[(Address, u8)], committee: &[Address]) -> Self {
        let seated: BTreeSet<&Address> = committee.iter().collect();
        Self(pool.iter().fold(BTreeMap::new(), |mut counts, (address, region)| {
            let seats =
                counts.get(region).copied().unwrap_or_default().plus(seated.contains(address));
            counts.insert(*region, seats);
            counts
        }))
    }

    /// Assigned (nonzero) regions with exactly one eligible validator: each holds a guaranteed
    /// round-one seat, the advantage #1327 documents.
    pub(crate) fn singleton_regions(&self) -> impl Iterator<Item = u8> + '_ {
        self.0
            .iter()
            .filter(|(region, seats)| **region != 0 && seats.pool == 1)
            .map(|(region, _)| *region)
    }

    /// Every region's counts, in ascending region id order.
    fn iter(&self) -> impl Iterator<Item = (u8, RegionSeats)> + '_ {
        self.0.iter().map(|(region, seats)| (*region, *seats))
    }
}

impl fmt::Display for RegionSeatReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .iter()
            .map(|(region, seats)| {
                format!("region {region}: {} in pool, {} seated", seats.pool, seats.seated)
            })
            .collect();
        f.write_str(&parts.join("; "))
    }
}

/// A count as a gauge value: exact below 2^32 and saturating above (a validator pool cannot reach
/// that), with no `as` cast so the conversion cannot silently wrap.
fn gauge_value(count: usize) -> f64 {
    u32::try_from(count).map_or(f64::from(u32::MAX), f64::from)
}

/// Publish which regions a region-aware committee draw seated, and flag singleton regions.
///
/// Called once per region-aware epoch close from `assemble_new_committee`, after the trimmed
/// committee has passed its length check, so a fatal undersized draw publishes nothing. Every node
/// executes the closing block, so the series is fleet-wide, and a replayed close is still a true
/// observation of that epoch's draw.
///
/// The `info` line carries the whole per-region tally. The `warn` fires only when an assigned
/// region has a single eligible validator, because that validator holds a guaranteed seat (#1327)
/// and the region claim is the thing governance should verify. The flat pre-fork arm publishes
/// nothing: there is no region tally to report, and an absent series reads as "no region-aware
/// draw yet" rather than as an empty pool.
pub(crate) fn record_region_seats(report: &RegionSeatReport) {
    let singleton_regions: Vec<u8> = report.singleton_regions().collect();
    info!(target: "engine", %report, ?singleton_regions, "region-aware committee drawn");
    if !singleton_regions.is_empty() {
        warn!(
            target: "engine",
            ?singleton_regions,
            "an assigned region with a single eligible validator holds a guaranteed committee \
             seat; the region claim is an off-chain attestation governance should verify (#1327)",
        );
    }

    metrics::describe_gauge!(
        EPOCH_CLOSE_REGION_POOL,
        "Eligible validators declaring each region at a region-aware epoch close"
    );
    metrics::describe_gauge!(
        EPOCH_CLOSE_REGION_SEATED,
        "Committee seats each region won at a region-aware epoch close"
    );
    metrics::describe_gauge!(
        EPOCH_CLOSE_SINGLETON_REGIONS,
        "Assigned regions with exactly one eligible validator, each holding a guaranteed seat"
    );

    report.iter().for_each(|(region, seats)| {
        let label = region.to_string();
        metrics::gauge!(EPOCH_CLOSE_REGION_POOL, "region" => label.clone())
            .set(gauge_value(seats.pool()));
        metrics::gauge!(EPOCH_CLOSE_REGION_SEATED, "region" => label)
            .set(gauge_value(seats.seated()));
    });
    metrics::gauge!(EPOCH_CLOSE_SINGLETON_REGIONS).set(gauge_value(singleton_regions.len()));
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
    }

    /// Construction alone must register every derive-based series at zero, with no event
    /// having happened.
    ///
    /// This is the property [`init`] relies on: a healthy node that never drops a transaction
    /// or falls behind the canonical-state broadcast still exports every counter, so
    /// `absent()` distinguishes "no events" from "exporter broken" and the first event
    /// renders as a step from zero rather than as a series that appears already nonzero.
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
        let (_, _, _, value) = find("tn_reth.canon_state_lagged_total");
        assert!(matches!(value, DebugValue::Counter(0)));
        let (_, _, _, value) = find("tn_reth.canon_state_notifications_missed_total");
        assert!(matches!(value, DebugValue::Counter(0)));
        let (_, _, _, value) = find("tn_reth.canon_state_resync_read_failures_total");
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
                matches!(
                    reason_series(&snapshot, FORWARDED_TXNS_DROPPED, reason.label()),
                    DebugValue::Counter(0)
                ),
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
        assert!(matches!(
            reason_series(&snapshot, FORWARDED_TXNS_DROPPED, "batch_shed"),
            DebugValue::Counter(3)
        ));
        ForwardDropReason::ALL
            .into_iter()
            .filter(|reason| *reason != ForwardDropReason::BatchShed)
            .for_each(|reason| {
                assert!(
                    matches!(
                        reason_series(&snapshot, FORWARDED_TXNS_DROPPED, reason.label()),
                        DebugValue::Counter(0)
                    ),
                    "a batch_shed drop must not move the {} series",
                    reason.label()
                );
            });
    }

    /// [`register_invalid_tx_skip_series`] must register one skip series per
    /// [`InvalidTxSkipReason`] at zero, and a recorded skip must move only its own reason's
    /// series.
    ///
    /// Same property the forwarder drop series is held to: a reason that only appears once it
    /// fires cannot be told apart from a mistyped label value, and its first `rate()` step is
    /// lost.
    #[test]
    fn test_invalid_tx_skip_reasons_register_at_zero_and_count_separately() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            register_invalid_tx_skip_series();
        });

        let snapshot = snapshotter.snapshot().into_vec();
        InvalidTxSkipReason::ALL.into_iter().for_each(|reason| {
            assert!(
                matches!(
                    reason_series(&snapshot, INVALID_TXS_SKIPPED, reason.label()),
                    DebugValue::Counter(0)
                ),
                "reason {} must be registered at zero",
                reason.label()
            );
        });

        metrics::with_local_recorder(&recorder, || {
            record_invalid_tx_skipped(InvalidTxSkipReason::NonceTooLow);
            record_invalid_tx_skipped(InvalidTxSkipReason::NonceTooLow);
        });

        let snapshot = snapshotter.snapshot().into_vec();
        assert!(matches!(
            reason_series(&snapshot, INVALID_TXS_SKIPPED, "nonce_too_low"),
            DebugValue::Counter(2)
        ));
        InvalidTxSkipReason::ALL
            .into_iter()
            .filter(|reason| *reason != InvalidTxSkipReason::NonceTooLow)
            .for_each(|reason| {
                assert!(
                    matches!(
                        reason_series(&snapshot, INVALID_TXS_SKIPPED, reason.label()),
                        DebugValue::Counter(0)
                    ),
                    "a nonce_too_low skip must not move the {} series",
                    reason.label()
                );
            });
    }

    /// [`InvalidTxSkipReason::classify`] must map each named cause to its own label and
    /// everything else - including an error that exposes no [`InvalidTransaction`] - to
    /// [`InvalidTxSkipReason::Other`].
    #[test]
    fn test_invalid_tx_skip_reason_classification() {
        assert_eq!(
            InvalidTxSkipReason::classify(&InvalidTransaction::NonceTooLow { tx: 1, state: 2 }),
            InvalidTxSkipReason::NonceTooLow
        );
        assert_eq!(
            InvalidTxSkipReason::classify(&InvalidTransaction::NonceTooHigh { tx: 9, state: 2 }),
            InvalidTxSkipReason::NonceTooHigh
        );
        assert_eq!(
            InvalidTxSkipReason::classify(&InvalidTransaction::LackOfFundForMaxFee {
                fee: Box::default(),
                balance: Box::default(),
            }),
            InvalidTxSkipReason::InsufficientFunds
        );
        assert_eq!(
            InvalidTxSkipReason::classify(&InvalidTransaction::GasPriceLessThanBasefee),
            InvalidTxSkipReason::FeeCapBelowBaseFee
        );
        assert_eq!(
            InvalidTxSkipReason::classify(&InvalidTransaction::RejectCallerWithCode),
            InvalidTxSkipReason::Other
        );
        assert_eq!(InvalidTxSkipReason::classify(&OpaqueTxError), InvalidTxSkipReason::Other);
    }

    /// An invalid-transaction error that exposes no underlying [`InvalidTransaction`],
    /// exercising the unclassifiable branch of [`InvalidTxSkipReason::classify`].
    #[derive(Debug)]
    struct OpaqueTxError;

    impl std::fmt::Display for OpaqueTxError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("opaque invalid-transaction error")
        }
    }

    impl std::error::Error for OpaqueTxError {}

    impl InvalidTxError for OpaqueTxError {
        fn as_invalid_tx_err(&self) -> Option<&InvalidTransaction> {
            None
        }
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

    /// The series of a `reason`-labeled counter for one label value.
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
        name: &str,
        reason: &str,
    ) -> &'a DebugValue {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == name
                    && key.key().labels().any(|l| l.key() == "reason" && l.value() == reason)
            })
            .map(|(.., value)| value)
            .unwrap_or_else(|| panic!("{name} series for reason {reason} not registered"))
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

    /// Read one gauge out of a snapshot by name and one exact `label = value` pair.
    fn labelled_gauge<D>(
        snapshot: &[(metrics_util::CompositeKey, Option<metrics::Unit>, D, DebugValue)],
        name: &str,
        label: &str,
        value: &str,
    ) -> Option<f64> {
        snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == name
                    && key.key().labels().any(|l| l.key() == label && l.value() == value)
            })
            .and_then(|(.., value)| match value {
                DebugValue::Gauge(g) => Some(g.0),
                DebugValue::Counter(_) | DebugValue::Histogram(_) => None,
            })
    }

    /// Address with the given last byte, for region tallies.
    fn addr(last_byte: u8) -> Address {
        Address::with_last_byte(last_byte)
    }

    /// Three region-1 validators, one region-2 validator, and one unassigned validator, with one
    /// seat from each region: every region's pool and seat counts must be exact, and only the
    /// assigned singleton (region 2) is flagged, never the unassigned one.
    #[test]
    fn region_seat_report_tallies_each_region_and_flags_assigned_singletons_only() {
        let pool = [(addr(1), 1), (addr(2), 1), (addr(3), 1), (addr(4), 2), (addr(5), 0)];
        let committee = [addr(4), addr(1), addr(5)];

        let report = RegionSeatReport::tally(&pool, &committee);

        let counts: Vec<(u8, usize, usize)> =
            report.iter().map(|(region, seats)| (region, seats.pool(), seats.seated())).collect();
        assert_eq!(counts, vec![(0, 1, 1), (1, 3, 1), (2, 1, 1)]);
        assert_eq!(
            report.singleton_regions().collect::<Vec<_>>(),
            vec![2],
            "region 0 has one member but no guarantee, so only region 2 is flagged",
        );
        assert_eq!(
            report.to_string(),
            "region 0: 1 in pool, 1 seated; region 1: 3 in pool, 1 seated; region 2: 1 in pool, \
             1 seated",
        );
    }

    /// [`record_region_seats`] must publish one labelled pool gauge and one labelled seat gauge
    /// per region plus the singleton count, each reading the tally's exact figure, and no series
    /// for a region absent from the pool.
    #[test]
    fn record_region_seats_publishes_labelled_gauges_per_region() {
        let pool = [(addr(1), 1), (addr(2), 1), (addr(3), 1), (addr(4), 2), (addr(5), 0)];
        let report = RegionSeatReport::tally(&pool, &[addr(4), addr(1), addr(5)]);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || record_region_seats(&report));

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_POOL, "region", "1"), Some(3.0));
        assert_eq!(labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_SEATED, "region", "1"), Some(1.0));
        assert_eq!(labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_POOL, "region", "2"), Some(1.0));
        assert_eq!(labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_SEATED, "region", "2"), Some(1.0));
        assert_eq!(labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_POOL, "region", "0"), Some(1.0));
        assert_eq!(gauge(&snapshot, EPOCH_CLOSE_SINGLETON_REGIONS, None), Some(1.0));
        assert_eq!(
            labelled_gauge(&snapshot, EPOCH_CLOSE_REGION_POOL, "region", "3"),
            None,
            "a region absent from the pool publishes no series",
        );
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
