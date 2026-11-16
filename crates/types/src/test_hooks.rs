//! Test-only fault-injection hooks that node code consults at fixed points.
//!
//! Every hook is callable from any build so a crate without a `test-utils` feature of its own
//! (`tn-node`) can consult it without a cfg at the call site. Without this crate's `test-utils`
//! feature each hook is a constant "off" and reads no environment. Like the fork-epoch overrides
//! in [`crate::forks`], the switches are environment variables because e2e tests drive real node
//! processes that share no memory with the harness. The harness sets them per spawned node and
//! never exports them process-wide: a child process inherits the harness's environment, so an
//! exported value would reach every node instead of the one under test.
//!
//! The two epoch-close hooks bracket the record write of a live epoch close.
//! [`exit_before_epoch_record`] stops a node after the closing block executed and before the
//! epoch's record is written, so a restart finds the record missing. [`exit_after_epoch_record`]
//! stops it after the record is written and before the state export and the epoch tables clear, so
//! a restart finds the record present and the tables still holding the closed epoch's rows.
//!
//! [`fail_store_at_epoch`] fails one store of the node's consensus DB once an epoch's tasks are
//! running, as a failed sync does, so a test sees whether the failure stops the node then rather
//! than at the epoch's end.
//!
//! [`execution_delay`] slows a node's engine without touching its consensus, so a test can make
//! one validator's execution trail its commits by more than the activity window.
//!
//! [`own_cert_delay`] holds back a validator's own certificate in its certifier, so a test can
//! break the validator's own chain of headers on demand: the header at one round is then not a
//! parent of the validator's header at the next round, as happens to a far or slow validator on a
//! fleet (issue #1807).
//!
//! [`withhold_epoch_vote`] keeps a node's own vote for one epoch's record out of its local count
//! and off the network for a chosen number of its vote rounds, so a test can leave that record
//! uncertified on every node without stopping one.

use crate::{Database, Epoch, Round, Table};
use std::{num::NonZeroU32, time::Duration};

/// Whether this node should stop at the live close of `epoch` after the closing block executed
/// and before the epoch's record is written.
///
/// Reads `TN_TEST_EXIT_BEFORE_EPOCH_RECORD=<epoch>` once per process. It lets an e2e test stop a
/// node inside the window a restart is most exposed to: the engine has executed the closing block
/// (so the chain has entered `epoch + 1`) but the record for `epoch` and the epoch tables clear
/// that follows it have not happened. The caller stops the node the way a SIGTERM inside that
/// window would. A node never live-closes the same epoch twice, so a value left set across the
/// restart does not fire again. [`exit_after_epoch_record`] stops the node in the window that
/// follows the record write.
///
/// Unset means never. A value that does not parse as an epoch is ignored with a warning. Always
/// `false` without `test-utils`; as with the fork-epoch overrides, a node-scoped release build
/// lacks the hook and a workspace-root build without `-p` has it.
#[inline]
pub fn exit_before_epoch_record(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        EXIT_BEFORE_EPOCH_RECORD.fires_for(epoch)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = epoch;
        false
    }
}

/// Whether this node should stop at the live close of `epoch` after the epoch's record is written
/// and before the state export and the epoch tables clear that follow it.
///
/// Reads `TN_TEST_EXIT_AFTER_EPOCH_RECORD=<epoch>` once per process. It lets an e2e test stop a
/// node in the window that follows [`exit_before_epoch_record`]'s: the record for `epoch` is
/// durable, but the epoch tables still hold `epoch`'s proposals, votes and certificates, because
/// the clear runs only after the state export. A restart from there finds the record present and
/// the tables stale. The caller stops the node the way a SIGTERM inside that window would. A node
/// never live-closes the same epoch twice, so a value left set across the restart does not fire
/// again.
///
/// Unset means never. A value that does not parse as an epoch is ignored with a warning. Always
/// `false` without `test-utils`; as with the fork-epoch overrides, a node-scoped release build
/// lacks the hook and a workspace-root build without `-p` has it.
#[inline]
pub fn exit_after_epoch_record(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        EXIT_AFTER_EPOCH_RECORD.fires_for(epoch)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = epoch;
        false
    }
}

/// Latch a durability failure in the store of `db` that holds `T` if this node should fail it in
/// `epoch`, and return whether the hook fired.
///
/// Reads `TN_TEST_FAIL_STORE_AT_EPOCH=<epoch>` once per process. It lets an e2e test fail one of a
/// node's stores in the middle of an epoch the way a failed sync does: the store refuses every
/// later write and `persist` until the process restarts, and [`Database::failed`] resolves. The
/// caller consults it once the epoch's tasks are running and before it waits for the epoch
/// boundary, so the test sees whether the failure stops the node at once or only at the epoch's
/// end. It fires on every run of `epoch`, a restart into it included, so a test sets it on one run
/// of one node.
///
/// Unset means never. A value that does not parse as an epoch is ignored with a warning. Always
/// `false`, leaving `db` untouched, without `test-utils`; as with the fork-epoch overrides, a
/// node-scoped release build lacks the hook and a workspace-root build without `-p` has it. Only a
/// backend that overrides [`Database::fail_durability_for_test`] latches anything (tndb's, with
/// `tn-storage/test-utils`).
#[inline]
pub fn fail_store_at_epoch<T: Table, DB: Database>(db: &DB, epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        let fires = FAIL_STORE_AT_EPOCH.fires_for(epoch);
        if fires {
            db.fail_durability_for_test::<T>();
        }
        fires
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = (db, epoch);
        false
    }
}

/// How long this node's engine sleeps before it executes a consensus output, or `None` for no
/// delay.
///
/// Reads `TN_TEST_EXECUTION_DELAY_MS=<milliseconds>` once per process. It lets an e2e test slow
/// one validator's execution while its consensus keeps pace with the committee, the way a
/// persist-bound node's execution trails its commits under load. The engine consults it once per
/// execution task and sleeps on that task's blocking thread before the output's build, so
/// consensus sees the node's execution as slow: the task's measured time grows by the delay, while
/// the build's own execute/persist/commit laps do not. The engine skips the sleep for an output
/// without batches, which skips execution too.
///
/// Unset or zero means no delay. A value that does not parse as whole milliseconds is ignored with
/// a warning. Always `None` without `test-utils`; as with the fork-epoch overrides, a node-scoped
/// release build lacks the hook and a workspace-root build without `-p` has it.
#[inline]
pub fn execution_delay() -> Option<Duration> {
    #[cfg(feature = "test-utils")]
    {
        *EXECUTION_DELAY.get_or_init(|| {
            // a non-unicode value is lossily converted so it is warned about rather than read as
            // unset
            let raw = std::env::var_os(EXECUTION_DELAY_VAR);
            parse_execution_delay(raw.as_deref().map(|raw| raw.to_string_lossy()).as_deref())
        })
    }
    #[cfg(not(feature = "test-utils"))]
    {
        None
    }
}

/// The environment variable behind [`execution_delay`].
#[cfg(feature = "test-utils")]
const EXECUTION_DELAY_VAR: &str = "TN_TEST_EXECUTION_DELAY_MS";

/// The delay [`execution_delay`] read from [`EXECUTION_DELAY_VAR`], once per process.
#[cfg(feature = "test-utils")]
static EXECUTION_DELAY: std::sync::OnceLock<Option<Duration>> = std::sync::OnceLock::new();

/// Parses the value of [`EXECUTION_DELAY_VAR`] into the delay before each execution.
///
/// `None` (the variable is unset) and zero yield `None`. Surrounding whitespace is trimmed. A value
/// that does not parse as whole milliseconds logs a warning and yields `None`, so a typo leaves
/// execution at full speed rather than slowing it by a guess. A delay that is set is logged once,
/// so a node log shows that its execution was slowed on purpose.
#[cfg(feature = "test-utils")]
fn parse_execution_delay(raw: Option<&str>) -> Option<Duration> {
    let raw = raw?;
    let delay_ms = raw
        .trim()
        .parse::<u64>()
        .inspect_err(|err| {
            tracing::warn!(
                target: "engine",
                value = ?raw,
                %err,
                "ignoring {EXECUTION_DELAY_VAR}: not whole milliseconds; execution is not delayed",
            );
        })
        .ok()
        .filter(|delay_ms| *delay_ms > 0)?;
    tracing::warn!(
        target: "engine",
        delay_ms,
        "test hook: delaying the execution of every consensus output with batches",
    );
    Some(Duration::from_millis(delay_ms))
}

/// How this node's certifier holds back its own certificates, or `None` for no hold.
///
/// Reads three variables once per process:
/// - `TN_TEST_OWN_CERT_DELAY_MS=<milliseconds>`: how long each hold lasts. Unset or zero means no
///   hold.
/// - `TN_TEST_OWN_CERT_DELAY_STAGE=before_store|after_store`: where the hold sits (see
///   [`OwnCertDelayStage`]). Unset means `after_store`.
/// - `TN_TEST_OWN_CERT_DELAY_EVERY=<n>`: hold only the certificates of own headers whose round is a
///   multiple of `n`, so a test gets a mix of intact and broken own chains. Unset means 1 (every
///   own header).
///
/// It lets an e2e test break one validator's own chain of headers, so that the validator's header
/// at round r is not a parent of its header at round r+1, in either of the two ways the proposer
/// re-queues an earlier header's batch digests: `before_store` makes the certifier cancel the
/// header, `after_store` lets it certify too late to be a parent. The hold only delays: the
/// certificate is formed, stored and gossiped as without the hook, or not at all when a newer own
/// header cancels it first, as any cancel does.
///
/// Each hold must outlast the time from the certificate forming to the next own header reaching
/// the certifier, or the certificate is merely late. That time is at most the proposer's
/// `max_header_delay` (2.5 s by default) plus a vote collection, so a four-validator local test
/// uses 5000 ms for either stage. Hold every second header (`EVERY=2`) at least: with every own
/// header held for that long, `before_store` cancels every header, so the node's batches never
/// commit, and `after_store` keeps every own certificate out of the DAG's committed history, so no
/// own header commits and no commit notice ever re-queues the late ones. With `EVERY=2` the held
/// rounds are the even rounds, Bullshark's leader rounds, so the node loses every leader slot it
/// has; an odd rate such as 3 holds leader and non-leader rounds alike.
///
/// A value of any of the three that does not parse is ignored with a warning and turns the hook
/// off, so a typo leaves the certifier untouched rather than holding certificates at a guessed
/// stage or rate. A hold that is set is logged once. Always `None` without `test-utils`; as with
/// the fork-epoch overrides, a node-scoped release build lacks the hook and a workspace-root build
/// without `-p` has it.
#[inline]
pub fn own_cert_delay() -> Option<OwnCertDelay> {
    #[cfg(feature = "test-utils")]
    {
        *OWN_CERT_DELAY.get_or_init(|| {
            parse_own_cert_delay(
                env_lossy(OWN_CERT_DELAY_MS_VAR).as_deref(),
                env_lossy(OWN_CERT_DELAY_STAGE_VAR).as_deref(),
                env_lossy(OWN_CERT_DELAY_EVERY_VAR).as_deref(),
            )
        })
    }
    #[cfg(not(feature = "test-utils"))]
    {
        None
    }
}

/// The number of this node's vote round for the record of `epoch` when the hook withholds the
/// round's own vote, or `None` when this node votes.
///
/// Reads `TN_TEST_WITHHOLD_EPOCH_VOTE=<epoch>` and `TN_TEST_WITHHOLD_EPOCH_VOTE_ROUNDS=<n>` once
/// per process. Set on two validators of four, with a round count that covers every round the
/// test runs through, it leaves the record of `epoch` with two votes of the three it needs, so the
/// record stays uncertified on every node with no stall and no restart.
/// The caller consults it once per round, right before it would sign its own vote for a record,
/// and withholds that vote (no local count, no gossip) when it returns `Some`; the rest of the
/// round runs as usual. Each call for the named epoch counts one round, from 1, and the first `n`
/// rounds are withheld. A node signs for one record in the original round at the record's close,
/// in the failsafe at the next close and, with the switch `epoch_record_recertify`, in the
/// re-certify picks of later closes, in that order. So `n` = 1 (the default) withholds the
/// original round only, after which the failsafe can certify the record one close later; `n` = 2
/// withholds the failsafe too; a large `n` keeps this node's vote off until it restarts without
/// the hook. The count starts again in each process, so a node restarted with the hook still set
/// withholds again.
///
/// Unset means never, and `n` is read only with the epoch set. An epoch that does not parse is
/// ignored with a warning and the node votes. A round count that is not a whole number of at least
/// 1 is ignored with a warning and 1 applies. Always `None` without `test-utils`; as with the
/// fork-epoch overrides, a node-scoped release build lacks the hook and a workspace-root build
/// without `-p` has it.
#[inline]
pub fn withhold_epoch_vote(epoch: Epoch) -> Option<u32> {
    #[cfg(feature = "test-utils")]
    {
        WITHHOLD_EPOCH_VOTE
            .get_or_init(|| {
                // a non-unicode value is lossily converted so it is warned about rather than read
                // as unset
                let raw_epoch = std::env::var_os(WITHHOLD_EPOCH_VOTE_VAR);
                let raw_rounds = std::env::var_os(WITHHOLD_EPOCH_VOTE_ROUNDS_VAR);
                parse_withhold_epoch_vote(
                    raw_epoch.as_deref().map(|raw| raw.to_string_lossy()).as_deref(),
                    raw_rounds.as_deref().map(|raw| raw.to_string_lossy()).as_deref(),
                )
            })
            .as_ref()
            .and_then(|hook| hook.withhold(epoch))
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = epoch;
        None
    }
}

/// Where [`own_cert_delay`] holds back this node's own certificate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnCertDelayStage {
    /// After the votes reach quorum and before the certificate is stored.
    ///
    /// The certificate is not yet stored, so a newer own header that reaches the certifier during
    /// the hold cancels the proposal: the header is never certified and the certifier reports it
    /// to the proposer, which re-queues its batch digests with cause `canceled`.
    BeforeStore,
    /// After the certificate is stored and durable and before it enters this node's DAG (the
    /// parents of its next header) and gossip.
    ///
    /// The header is certified, but the next own header is built without it. Once no later
    /// certificate can name it as a parent it never commits, and the proposer re-queues its batch
    /// digests with cause `committed` when a later own header commits.
    AfterStore,
}

impl OwnCertDelayStage {
    /// The stage's value in `TN_TEST_OWN_CERT_DELAY_STAGE`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BeforeStore => "before_store",
            Self::AfterStore => "after_store",
        }
    }
}

/// The own-certificate hold [`own_cert_delay`] configures: how long, where, and for which rounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnCertDelay {
    /// How long each hold lasts; never zero.
    delay: Duration,
    /// Where the hold sits.
    stage: OwnCertDelayStage,
    /// Only own headers whose round is a multiple of this are held.
    every: NonZeroU32,
}

impl OwnCertDelay {
    /// A hold of `delay` at `stage` for every own header whose round is a multiple of `every`, as
    /// [`own_cert_delay`] reads it from the environment. For tests that give a certifier a hold
    /// without setting the process environment.
    #[cfg(feature = "test-utils")]
    pub fn new(delay: Duration, stage: OwnCertDelayStage, every: NonZeroU32) -> Self {
        Self { delay, stage, every }
    }

    /// How long to hold the own certificate of the header at `round` at `stage`, or `None` when
    /// this hold sits at another stage or skips that round.
    pub fn delay_for(&self, stage: OwnCertDelayStage, round: Round) -> Option<Duration> {
        (stage == self.stage && round.is_multiple_of(self.every.get())).then_some(self.delay)
    }
}

/// The environment variable that sets the length of [`own_cert_delay`]'s hold.
#[cfg(feature = "test-utils")]
const OWN_CERT_DELAY_MS_VAR: &str = "TN_TEST_OWN_CERT_DELAY_MS";

/// The environment variable that sets the stage of [`own_cert_delay`]'s hold.
#[cfg(feature = "test-utils")]
const OWN_CERT_DELAY_STAGE_VAR: &str = "TN_TEST_OWN_CERT_DELAY_STAGE";

/// The environment variable that sets which own headers [`own_cert_delay`] holds.
#[cfg(feature = "test-utils")]
const OWN_CERT_DELAY_EVERY_VAR: &str = "TN_TEST_OWN_CERT_DELAY_EVERY";

/// The hold [`own_cert_delay`] read from its three variables, once per process.
#[cfg(feature = "test-utils")]
static OWN_CERT_DELAY: std::sync::OnceLock<Option<OwnCertDelay>> = std::sync::OnceLock::new();

/// The value of the environment variable `var`, or `None` when it is unset.
///
/// A non-unicode value is lossily converted so it is warned about rather than read as unset.
#[cfg(feature = "test-utils")]
fn env_lossy(var: &str) -> Option<String> {
    std::env::var_os(var).map(|raw| raw.to_string_lossy().into_owned())
}

/// Parses the values of the three [`own_cert_delay`] variables into the hold they describe.
///
/// `ms` unset or zero yields `None` and reads nothing else. `stage` unset means `after_store`,
/// `every` unset means 1. Surrounding whitespace is trimmed. A value that does not parse (`ms` not
/// whole milliseconds, `stage` neither `before_store` nor `after_store`, `every` not a whole
/// number above zero) logs a warning naming its variable and yields `None`. A hold that is set is
/// logged once, so a node log shows that its own certificates were held on purpose.
#[cfg(feature = "test-utils")]
fn parse_own_cert_delay(
    ms: Option<&str>,
    stage: Option<&str>,
    every: Option<&str>,
) -> Option<OwnCertDelay> {
    let ignored = |var: &str, raw: &str, expected: &str| {
        tracing::warn!(
            target: "primary::certifier",
            value = ?raw,
            "ignoring {var}: not {expected}; own certificates are not held",
        );
    };
    let raw_ms = ms?;
    let Ok(delay_ms) = raw_ms.trim().parse::<u64>() else {
        ignored(OWN_CERT_DELAY_MS_VAR, raw_ms, "whole milliseconds");
        return None;
    };
    if delay_ms == 0 {
        return None;
    }
    let stage = match stage.map(|raw| (raw, raw.trim())) {
        None | Some((_, "after_store")) => OwnCertDelayStage::AfterStore,
        Some((_, "before_store")) => OwnCertDelayStage::BeforeStore,
        Some((raw, _)) => {
            ignored(OWN_CERT_DELAY_STAGE_VAR, raw, "before_store or after_store");
            return None;
        }
    };
    let every = match every {
        None => NonZeroU32::MIN,
        Some(raw) => {
            let Ok(every) = raw.trim().parse::<NonZeroU32>() else {
                ignored(OWN_CERT_DELAY_EVERY_VAR, raw, "a whole number above zero");
                return None;
            };
            every
        }
    };
    tracing::warn!(
        target: "primary::certifier",
        delay_ms,
        stage = stage.as_str(),
        every = every.get(),
        "test hook: holding this node's own certificates",
    );
    Some(OwnCertDelay { delay: Duration::from_millis(delay_ms), stage, every })
}

/// The rounds of one epoch record's vote in which a node withholds its own vote (see
/// [`withhold_epoch_vote`]).
///
/// Public so a crate's unit tests can run a hook of their own on one thread instead of the
/// process-wide one, which reads the environment.
#[cfg(feature = "test-utils")]
#[derive(Debug)]
pub struct WithholdEpochVote {
    /// The epoch whose record's vote is withheld.
    epoch: Epoch,
    /// How many of this node's rounds for the record are withheld, from the first.
    rounds: u32,
    /// How many rounds for the record have consulted the hook so far.
    seen: std::sync::atomic::AtomicU32,
}

#[cfg(feature = "test-utils")]
impl WithholdEpochVote {
    /// A hook that withholds this node's vote for the record of `epoch` in its first `rounds`
    /// rounds.
    pub const fn new(epoch: Epoch, rounds: u32) -> Self {
        Self { epoch, rounds, seen: std::sync::atomic::AtomicU32::new(0) }
    }

    /// Count one round for the record of `epoch` and return the round's number, from 1, when its
    /// vote is withheld. A round for another epoch is not counted and votes.
    pub fn withhold(&self, epoch: Epoch) -> Option<u32> {
        use std::sync::atomic::Ordering;
        if epoch != self.epoch {
            return None;
        }
        // the closure never declines, so both arms carry the previous count
        let previous = self
            .seen
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |seen| Some(seen.saturating_add(1)))
            .unwrap_or_else(|seen| seen);
        let round = previous.saturating_add(1);
        (round <= self.rounds).then_some(round)
    }
}

/// The environment variable that names the epoch for [`withhold_epoch_vote`].
#[cfg(feature = "test-utils")]
const WITHHOLD_EPOCH_VOTE_VAR: &str = "TN_TEST_WITHHOLD_EPOCH_VOTE";

/// The environment variable that names how many rounds [`withhold_epoch_vote`] withholds.
#[cfg(feature = "test-utils")]
const WITHHOLD_EPOCH_VOTE_ROUNDS_VAR: &str = "TN_TEST_WITHHOLD_EPOCH_VOTE_ROUNDS";

/// The hook behind [`withhold_epoch_vote`], read once per process.
#[cfg(feature = "test-utils")]
static WITHHOLD_EPOCH_VOTE: std::sync::OnceLock<Option<WithholdEpochVote>> =
    std::sync::OnceLock::new();

/// Parses the values of [`WITHHOLD_EPOCH_VOTE_VAR`] and [`WITHHOLD_EPOCH_VOTE_ROUNDS_VAR`] into the
/// hook.
///
/// An unset epoch yields `None`. Surrounding whitespace is trimmed. An epoch that does not parse
/// as an [`Epoch`] logs a warning and yields `None`, so a typo leaves the node voting. An unset
/// round count means 1; one that does not parse as a whole number of at least 1 logs a warning and
/// means 1, the documented default.
#[cfg(feature = "test-utils")]
fn parse_withhold_epoch_vote(
    raw_epoch: Option<&str>,
    raw_rounds: Option<&str>,
) -> Option<WithholdEpochVote> {
    let raw_epoch = raw_epoch?;
    let epoch = raw_epoch
        .trim()
        .parse::<Epoch>()
        .inspect_err(|err| {
            tracing::warn!(
                target: "epoch-manager",
                value = ?raw_epoch,
                %err,
                "ignoring {WITHHOLD_EPOCH_VOTE_VAR}: not an epoch number; the node votes as usual",
            );
        })
        .ok()?;
    let rounds = raw_rounds.map_or(1, |raw| {
        raw.trim().parse::<u32>().ok().filter(|rounds| *rounds > 0).unwrap_or_else(|| {
            tracing::warn!(
                target: "epoch-manager",
                value = ?raw,
                "ignoring {WITHHOLD_EPOCH_VOTE_ROUNDS_VAR}: not a whole number of at least 1; one round is withheld",
            );
            1
        })
    });
    Some(WithholdEpochVote::new(epoch, rounds))
}

/// The hook behind [`exit_before_epoch_record`].
#[cfg(feature = "test-utils")]
static EXIT_BEFORE_EPOCH_RECORD: EpochHook = EpochHook::new("TN_TEST_EXIT_BEFORE_EPOCH_RECORD");

/// The hook behind [`exit_after_epoch_record`].
#[cfg(feature = "test-utils")]
static EXIT_AFTER_EPOCH_RECORD: EpochHook = EpochHook::new("TN_TEST_EXIT_AFTER_EPOCH_RECORD");

/// The hook behind [`fail_store_at_epoch`].
#[cfg(feature = "test-utils")]
static FAIL_STORE_AT_EPOCH: EpochHook = EpochHook::new("TN_TEST_FAIL_STORE_AT_EPOCH");

/// A hook that fires at one epoch, named by the environment variable `var`.
#[cfg(feature = "test-utils")]
struct EpochHook {
    /// The environment variable that names the epoch.
    var: &'static str,
    /// The epoch `var` names, read once per process.
    target: std::sync::OnceLock<Option<Epoch>>,
}

#[cfg(feature = "test-utils")]
impl EpochHook {
    const fn new(var: &'static str) -> Self {
        Self { var, target: std::sync::OnceLock::new() }
    }

    /// Whether `var` names `epoch`. The variable is read on the first call only.
    fn fires_for(&self, epoch: Epoch) -> bool {
        let target = *self.target.get_or_init(|| {
            // a non-unicode value is lossily converted so it is warned about rather than read as
            // unset
            let raw = std::env::var_os(self.var);
            parse_epoch_hook(self.var, raw.as_deref().map(|raw| raw.to_string_lossy()).as_deref())
        });
        target == Some(epoch)
    }
}

/// Parses the value of the epoch hook variable `var` into the epoch the hook fires at.
///
/// `None` (the variable is unset) yields `None`. Surrounding whitespace is trimmed. A value that
/// does not parse as an [`Epoch`] logs a warning naming `var` and yields `None`, so a typo leaves
/// the node running rather than stopping it at a guessed epoch.
#[cfg(feature = "test-utils")]
fn parse_epoch_hook(var: &str, raw: Option<&str>) -> Option<Epoch> {
    let raw = raw?;
    raw.trim()
        .parse::<Epoch>()
        .inspect_err(|err| {
            tracing::warn!(
                target: "epoch-manager",
                value = ?raw,
                %err,
                "ignoring {var}: not an epoch number; the node will not stop",
            );
        })
        .ok()
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;

    /// Every epoch hook variable, for the parse tests.
    fn hook_vars() -> [&'static str; 3] {
        [EXIT_BEFORE_EPOCH_RECORD.var, EXIT_AFTER_EPOCH_RECORD.var, FAIL_STORE_AT_EPOCH.var]
    }

    /// The hook fires for the named epoch and no other. This is the only test in the crate that
    /// sets the variable, and the value latches on first read, so it holds under a
    /// single-process `cargo test` run as well as under nextest's process per test.
    #[test]
    fn exit_before_epoch_record_matches_only_the_named_epoch() {
        std::env::set_var("TN_TEST_EXIT_BEFORE_EPOCH_RECORD", "8");
        assert!(exit_before_epoch_record(8));
        assert!(!exit_before_epoch_record(7));
        assert!(!exit_before_epoch_record(9));
    }

    /// The hook fires for the named epoch and no other. This is the only test in the crate that
    /// sets the variable, and the value latches on first read, so it holds under a
    /// single-process `cargo test` run as well as under nextest's process per test. It names a
    /// different epoch than the before-record test, so a hook that read the other variable could
    /// not pass under a single-process run either.
    #[test]
    fn exit_after_epoch_record_matches_only_the_named_epoch() {
        std::env::set_var("TN_TEST_EXIT_AFTER_EPOCH_RECORD", "5");
        assert!(exit_after_epoch_record(5));
        assert!(!exit_after_epoch_record(4));
        assert!(!exit_after_epoch_record(6));
    }

    /// The store-failure hook fires for the named epoch and no other. This is the only test in the
    /// crate that sets the variable, and it names an epoch neither exit test names, so a hook that
    /// read another variable could not pass under a single-process run either. It asks the hook
    /// itself: [`fail_store_at_epoch`] needs a backend, and the e2e test that sets the variable
    /// (`test_restarts_store_failure_stops_the_node_mid_epoch`) covers the latch.
    #[test]
    fn fail_store_at_epoch_matches_only_the_named_epoch() {
        std::env::set_var("TN_TEST_FAIL_STORE_AT_EPOCH", "3");
        assert!(FAIL_STORE_AT_EPOCH.fires_for(3));
        assert!(!FAIL_STORE_AT_EPOCH.fires_for(2));
        assert!(!FAIL_STORE_AT_EPOCH.fires_for(4));
    }

    /// The delay hook reads its variable as milliseconds. This is the only test in the crate that
    /// sets the variable, and the value latches on first read, so it holds under a
    /// single-process `cargo test` run as well as under nextest's process per test.
    #[test]
    fn execution_delay_reads_milliseconds() {
        std::env::set_var(EXECUTION_DELAY_VAR, "1500");
        assert_eq!(execution_delay(), Some(Duration::from_millis(1500)));
    }

    #[test]
    fn parse_execution_delay_reads_milliseconds() {
        for (raw, expected_ms) in [("1500", 1500), (" 1\n", 1), ("+20", 20)] {
            assert_eq!(
                parse_execution_delay(Some(raw)),
                Some(Duration::from_millis(expected_ms)),
                "raw = {raw:?}"
            );
        }
    }

    #[test]
    fn parse_execution_delay_unset_zero_and_garbage_are_none() {
        assert_eq!(parse_execution_delay(None), None);
        for raw in ["0", " 0 ", "", "garbage", "-1", "1.5", "1500ms", "0x10", "\u{FFFD}"] {
            assert_eq!(parse_execution_delay(Some(raw)), None, "raw = {raw:?}");
        }
    }

    #[test]
    fn parse_epoch_hook_unset_is_none() {
        for var in hook_vars() {
            assert_eq!(parse_epoch_hook(var, None), None, "var = {var}");
        }
    }

    #[test]
    fn parse_epoch_hook_reads_epochs() {
        for var in hook_vars() {
            for (raw, expected) in
                [("8", 8), ("0", 0), (" 8\n", 8), ("+8", 8), ("4294967295", u32::MAX)]
            {
                assert_eq!(
                    parse_epoch_hook(var, Some(raw)),
                    Some(expected),
                    "var = {var}, raw = {raw:?}"
                );
            }
        }
    }

    #[test]
    fn parse_epoch_hook_ignores_garbage() {
        // negatives, fractions, units, hex and out-of-range values must not stop the node at a
        // guess; the replacement character is what a non-unicode value becomes in the lossy read
        for var in hook_vars() {
            for raw in ["garbage", "", "  ", "-1", "8.0", "8e", "0x8", "4294967296", "\u{FFFD}"] {
                assert_eq!(parse_epoch_hook(var, Some(raw)), None, "var = {var}, raw = {raw:?}");
            }
        }
    }

    // ---- bench7 digest-order-hold ----

    /// A hold of `ms` milliseconds at `stage` on every `every`-th round.
    fn hold(ms: u64, stage: OwnCertDelayStage, every: u32) -> OwnCertDelay {
        let every = NonZeroU32::new(every).expect("non-zero every");
        OwnCertDelay::new(Duration::from_millis(ms), stage, every)
    }

    /// The own-certificate hook reads all three of its variables. This is the only test in the
    /// crate that sets them, and the value latches on first read, so it holds under a
    /// single-process `cargo test` run as well as under nextest's process per test. It sets the
    /// stage and the rate away from their defaults, so a hook that ignored either could not pass.
    #[test]
    fn own_cert_delay_reads_its_three_variables() {
        std::env::set_var(OWN_CERT_DELAY_MS_VAR, "1500");
        std::env::set_var(OWN_CERT_DELAY_STAGE_VAR, "before_store");
        std::env::set_var(OWN_CERT_DELAY_EVERY_VAR, "3");
        assert_eq!(own_cert_delay(), Some(hold(1500, OwnCertDelayStage::BeforeStore, 3)));
    }

    #[test]
    fn parse_own_cert_delay_defaults_to_after_store_on_every_round() {
        assert_eq!(
            parse_own_cert_delay(Some("5000"), None, None),
            Some(hold(5000, OwnCertDelayStage::AfterStore, 1))
        );
    }

    #[test]
    fn parse_own_cert_delay_reads_values() {
        for (ms, stage, every, expected) in [
            (
                " 5000\n",
                Some("before_store"),
                Some("2"),
                hold(5000, OwnCertDelayStage::BeforeStore, 2),
            ),
            ("+20", Some(" after_store "), Some(" 1 "), hold(20, OwnCertDelayStage::AfterStore, 1)),
            ("1", Some("after_store"), Some("+4"), hold(1, OwnCertDelayStage::AfterStore, 4)),
        ] {
            assert_eq!(
                parse_own_cert_delay(Some(ms), stage, every),
                Some(expected),
                "ms = {ms:?}, stage = {stage:?}, every = {every:?}"
            );
        }
    }

    #[test]
    fn parse_own_cert_delay_unset_zero_and_garbage_are_none() {
        // unset or zero is no hold, whatever the other two say
        for stage in [None, Some("before_store"), Some("garbage")] {
            assert_eq!(parse_own_cert_delay(None, stage, Some("2")), None, "stage = {stage:?}");
            assert_eq!(parse_own_cert_delay(Some(" 0 "), stage, None), None, "stage = {stage:?}");
        }
        // a typo in any of the three turns the hook off rather than holding at a guess; the
        // replacement character is what a non-unicode value becomes in the lossy read
        for ms in ["", "garbage", "-1", "1.5", "1500ms", "0x10", "\u{FFFD}"] {
            assert_eq!(parse_own_cert_delay(Some(ms), None, None), None, "ms = {ms:?}");
        }
        for stage in ["", "before", "BEFORE_STORE", "after-store", "\u{FFFD}"] {
            assert_eq!(
                parse_own_cert_delay(Some("5000"), Some(stage), None),
                None,
                "stage = {stage:?}"
            );
        }
        for every in ["", "0", "-2", "1.5", "two", "4294967296", "\u{FFFD}"] {
            assert_eq!(
                parse_own_cert_delay(Some("5000"), None, Some(every)),
                None,
                "every = {every:?}"
            );
        }
    }

    /// A hold applies only at its own stage, and only to rounds that are a multiple of `every`.
    #[test]
    fn own_cert_delay_holds_its_stage_on_every_nth_round() {
        let after = hold(5000, OwnCertDelayStage::AfterStore, 2);
        let delay = Some(Duration::from_millis(5000));
        let held: Vec<Round> = (1..=6)
            .filter(|round| after.delay_for(OwnCertDelayStage::AfterStore, *round) == delay)
            .collect();
        assert_eq!(held, [2, 4, 6], "every second round is held");
        for round in 1..=6 {
            assert_eq!(
                after.delay_for(OwnCertDelayStage::BeforeStore, round),
                None,
                "round {round}"
            );
        }
        let every_round = hold(10, OwnCertDelayStage::BeforeStore, 1);
        for round in [1, 2, 3, Round::MAX] {
            assert_eq!(
                every_round.delay_for(OwnCertDelayStage::BeforeStore, round),
                Some(Duration::from_millis(10)),
                "round {round}"
            );
        }
    }

    // ---- bench7 epoch-record-collector-hardening: withhold hook ----

    /// The withhold hook withholds the first rounds of the named epoch and no other. This is the
    /// only test in the crate that sets the two variables, and the values latch on first read, so
    /// it holds under a single-process `cargo test` run as well as under nextest's process per
    /// test. It names an epoch the other hook tests do not.
    #[test]
    fn withhold_epoch_vote_withholds_the_first_rounds_of_the_named_epoch() {
        std::env::set_var(WITHHOLD_EPOCH_VOTE_VAR, "6");
        std::env::set_var(WITHHOLD_EPOCH_VOTE_ROUNDS_VAR, "2");
        assert_eq!(withhold_epoch_vote(5), None);
        assert_eq!(withhold_epoch_vote(6), Some(1));
        assert_eq!(withhold_epoch_vote(7), None);
        assert_eq!(withhold_epoch_vote(6), Some(2));
        assert_eq!(withhold_epoch_vote(6), None, "the third round votes");
        assert_eq!(withhold_epoch_vote(6), None);
    }

    /// The parsed hook's epoch and round count.
    fn withhold_form(epoch: Option<&str>, rounds: Option<&str>) -> Option<(Epoch, u32)> {
        parse_withhold_epoch_vote(epoch, rounds).map(|hook| (hook.epoch, hook.rounds))
    }

    #[test]
    fn parse_withhold_epoch_vote_reads_both_forms() {
        assert_eq!(withhold_form(None, None), None);
        assert_eq!(withhold_form(None, Some("3")), None, "the round count alone is no hook");
        assert_eq!(withhold_form(Some("8"), None), Some((8, 1)), "one round by default");
        assert_eq!(withhold_form(Some(" 8\n"), Some(" 3\n")), Some((8, 3)));
        assert_eq!(withhold_form(Some("0"), Some("4294967295")), Some((0, u32::MAX)));
    }

    #[test]
    fn parse_withhold_epoch_vote_ignores_garbage() {
        for raw in ["garbage", "", "-1", "8.0", "0x8", "4294967296", "\u{FFFD}"] {
            assert_eq!(withhold_form(Some(raw), Some("2")), None, "epoch = {raw:?}");
        }
        for raw in ["0", "garbage", "", "-1", "1.5", "0x2", "4294967296", "\u{FFFD}"] {
            assert_eq!(withhold_form(Some("8"), Some(raw)), Some((8, 1)), "rounds = {raw:?}");
        }
    }

    /// Rounds for other epochs are not counted; the count saturates rather than wrapping back
    /// into the withheld rounds.
    #[test]
    fn withhold_counts_only_the_named_epoch() {
        let hook = WithholdEpochVote::new(4, 1);
        assert_eq!(hook.withhold(3), None);
        assert_eq!(hook.withhold(5), None);
        assert_eq!(hook.withhold(4), Some(1));
        assert_eq!(hook.withhold(4), None);
        hook.seen.store(u32::MAX, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(hook.withhold(4), None);
        assert_eq!(hook.seen.load(std::sync::atomic::Ordering::Relaxed), u32::MAX);
    }
}
