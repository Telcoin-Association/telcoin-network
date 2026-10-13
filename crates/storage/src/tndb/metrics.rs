// SPDX-License-Identifier: MIT or Apache-2.0

//! Production metrics (see the README's Metrics section). A database creates its handles when it
//! opens, labeled with its environment (`env`), so the writers and the commit and compaction
//! threads record through them with no recorder lookup. The node installs its recorder before
//! any database opens; without one, the handles record nothing.

use std::{fmt, time::Duration};

use metrics::{Counter, Gauge, Histogram, Unit};

/// The `env` label of a database opened outside a [`super::SplitDatabase`].
pub(super) const UNNAMED_ENV: &str = "unnamed";

const BATCHES: &str = "tn_storage.tndb_batches_total";
const SYNC: &str = "tn_storage.tndb_sync_seconds";
const UNSYNCED: &str = "tn_storage.tndb_unsynced_bytes";
const LOG_BYTES: &str = "tn_storage.tndb_log_bytes";
const GENERATION: &str = "tn_storage.tndb_generation";
const STALL: &str = "tn_storage.tndb_write_stall_seconds";
const COMPACTIONS: &str = "tn_storage.tndb_compactions_total";
const ABANDONS: &str = "tn_storage.tndb_compaction_abandons_total";
const COMPACTION: &str = "tn_storage.tndb_compaction_seconds";
const SWITCH: &str = "tn_storage.tndb_compaction_switch_seconds";
const FAILED: &str = "tn_storage.tndb_failed";

/// The shortest write-lock wait or hold recorded as a stall: shorter ones (a write's hold is
/// typically tens of microseconds) are not recorded, so the hot path pays a clock read, not a
/// histogram record.
pub(super) const STALL_MIN: Duration = Duration::from_millis(1);

/// How a compaction ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Outcome {
    /// The database switched to the compacted generation.
    Switched,
    /// Nothing to compact (the log was too small, or a compaction was not due).
    Skipped,
    /// Given up: the database carries on in its generation.
    Abandoned(Cause),
    /// Stopped by a close.
    Cancelled,
}

impl Outcome {
    const LABELS: [&'static str; 4] = ["switched", "skipped", "abandoned", "cancelled"];

    fn index(self) -> usize {
        match self {
            Self::Switched => 0,
            Self::Skipped => 1,
            Self::Abandoned(_) => 2,
            Self::Cancelled => 3,
        }
    }
}

/// Why a compaction was abandoned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Cause {
    /// Too little free disk, at the start or while it ran.
    DiskFull,
    /// The writer outran every switch try.
    WriterOutran,
    /// Damaged committed data (a failed CRC, a corrupt record or index).
    Corruption,
    /// The compaction panicked.
    Panic,
    /// Anything else (an I/O error, a row count that did not match).
    Error,
}

impl Cause {
    const LABELS: [&'static str; 5] =
        ["disk_full", "writer_outran", "corruption", "panic", "error"];

    fn index(self) -> usize {
        match self {
            Self::DiskFull => 0,
            Self::WriterOutran => 1,
            Self::Corruption => 2,
            Self::Panic => 3,
            Self::Error => 4,
        }
    }
}

/// One database's metric handles (see the module docs).
pub(super) struct TnDbMetrics {
    batches: Counter,
    sync: Histogram,
    unsynced: Gauge,
    log_bytes: Gauge,
    generation: Gauge,
    stall_wait: Histogram,
    stall_hold: Histogram,
    compactions: [Counter; Outcome::LABELS.len()],
    abandons: [Counter; Cause::LABELS.len()],
    compaction: Histogram,
    switch: Histogram,
    failed: Gauge,
}

impl TnDbMetrics {
    /// Describe the metrics and create the handles of the database labeled `env`, every counter
    /// series at 0 (a series first exported at its first event hides that event from `increase`
    /// rules) and every gauge set.
    pub(super) fn new(env: &'static str) -> Self {
        describe();
        let counter = |name: &'static str, label: (&'static str, &'static str)| {
            let counter = metrics::counter!(name, "env" => env, label.0 => label.1);
            counter.increment(0);
            counter
        };
        let gauge = |name: &'static str| {
            let gauge = metrics::gauge!(name, "env" => env);
            gauge.set(0.0);
            gauge
        };
        let batches = metrics::counter!(BATCHES, "env" => env);
        batches.increment(0);
        Self {
            batches,
            sync: metrics::histogram!(SYNC, "env" => env),
            unsynced: gauge(UNSYNCED),
            log_bytes: gauge(LOG_BYTES),
            generation: gauge(GENERATION),
            stall_wait: metrics::histogram!(STALL, "env" => env, "kind" => "wait"),
            stall_hold: metrics::histogram!(STALL, "env" => env, "kind" => "hold"),
            compactions: Outcome::LABELS.map(|outcome| counter(COMPACTIONS, ("outcome", outcome))),
            abandons: Cause::LABELS.map(|cause| counter(ABANDONS, ("cause", cause))),
            compaction: metrics::histogram!(COMPACTION, "env" => env),
            switch: metrics::histogram!(SWITCH, "env" => env),
            failed: gauge(FAILED),
        }
    }

    /// The gauge set to 1 when a failure latches (see `Durability::fail`).
    pub(super) fn failed_gauge(&self) -> Gauge {
        self.failed.clone()
    }

    /// The database opened in generation `gen`, its log `log_bytes` long.
    pub(super) fn opened(&self, gen: u64, log_bytes: u64) {
        self.generation.set(gen as f64);
        self.log_bytes.set(log_bytes as f64);
    }

    /// A batch was written: the log now ends at `end`, `unsynced` bytes of it not yet synced; the
    /// writer waited `waited` for the write lock and held it `held`.
    pub(super) fn wrote(&self, end: u64, unsynced: u64, waited: Duration, held: Duration) {
        self.batches.increment(1);
        self.log_bytes.set(end as f64);
        self.unsynced.set(unsynced as f64);
        if waited >= STALL_MIN {
            self.stall_wait.record(waited.as_secs_f64());
        }
        if held >= STALL_MIN {
            self.stall_hold.record(held.as_secs_f64());
        }
    }

    /// A sync round took `took`, leaving `unsynced` bytes not yet synced.
    pub(super) fn synced(&self, took: Duration, unsynced: u64) {
        self.sync.record(took.as_secs_f64());
        self.unsynced.set(unsynced as f64);
    }

    /// The database switched to generation `gen`, its log `log_bytes` long; the switch held the
    /// write lock `held`.
    pub(super) fn switched(&self, gen: u64, log_bytes: u64, held: Duration) {
        self.generation.set(gen as f64);
        self.log_bytes.set(log_bytes as f64);
        self.switch.record(held.as_secs_f64());
    }

    /// A compaction ended as `outcome` after `took`.
    pub(super) fn compaction_ended(&self, outcome: Outcome, took: Duration) {
        self.compactions[outcome.index()].increment(1);
        match outcome {
            Outcome::Switched => self.compaction.record(took.as_secs_f64()),
            Outcome::Abandoned(cause) => self.abandons[cause.index()].increment(1),
            Outcome::Skipped | Outcome::Cancelled => {}
        }
    }
}

impl fmt::Debug for TnDbMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TnDbMetrics").finish_non_exhaustive()
    }
}

/// The metrics' units and HELP texts (idempotent: every open describes them again).
fn describe() {
    use metrics::{describe_counter, describe_gauge, describe_histogram};
    describe_counter!(
        BATCHES,
        Unit::Count,
        "tndb batches written (a bare write or a transaction)."
    );
    describe_histogram!(
        SYNC,
        Unit::Seconds,
        "tndb sync rounds: msync of the log, plus an fsync after it grew."
    );
    describe_gauge!(
        UNSYNCED,
        Unit::Bytes,
        "tndb log bytes not yet synced (writers wait past 64 MiB)."
    );
    describe_gauge!(LOG_BYTES, Unit::Bytes, "Length of the current tndb log.");
    describe_gauge!(
        GENERATION,
        Unit::Count,
        "The current tndb generation (one more after each compaction)."
    );
    describe_histogram!(
        STALL,
        Unit::Seconds,
        "tndb write-lock waits and holds of at least 1 ms (shorter ones are not recorded)."
    );
    describe_counter!(
        COMPACTIONS,
        Unit::Count,
        "tndb compactions, by outcome: switched, skipped, abandoned or cancelled."
    );
    describe_counter!(
        ABANDONS,
        Unit::Count,
        "tndb compactions abandoned, by cause: disk_full, writer_outran, corruption, panic, error."
    );
    describe_histogram!(
        COMPACTION,
        Unit::Seconds,
        "Duration of a tndb compaction that switched, snapshot to install."
    );
    describe_histogram!(
        SWITCH,
        Unit::Seconds,
        "How long a tndb compaction's switch held the write lock."
    );
    describe_gauge!(
        FAILED,
        Unit::Count,
        "1 once a tndb failure latched (a failed sync, or a stopped writer): writes fail until a \
         restart."
    );
}
