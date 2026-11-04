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

use crate::{Database, Epoch, Table};

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
}
