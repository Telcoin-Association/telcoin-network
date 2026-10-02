//! Test-only fault-injection hooks that node code consults at fixed points.
//!
//! Every hook is callable from any build so a crate without a `test-utils` feature of its own
//! (`tn-node`) can consult it without a cfg at the call site. Without this crate's `test-utils`
//! feature each hook is a constant "off" and reads no environment. Like the fork-epoch overrides
//! in [`crate::forks`], the switches are environment variables because e2e tests drive real node
//! processes that share no memory with the harness. The harness sets them per spawned node and
//! never exports them process-wide: a child process inherits the harness's environment, so an
//! exported value would reach every node instead of the one under test.

use crate::Epoch;

/// Whether this node should stop at the live close of `epoch` after the closing block executed
/// and before the epoch's record is written.
///
/// Reads `TN_TEST_EXIT_BEFORE_EPOCH_RECORD=<epoch>` once per process. It lets an e2e test stop a
/// node inside the window a restart is most exposed to: the engine has executed the closing block
/// (so the chain has entered `epoch + 1`) but the record for `epoch` and the epoch tables clear
/// that follows it have not happened. The caller stops the node the way a SIGTERM inside that
/// window would. A node never live-closes the same epoch twice, so a value left set across the
/// restart does not fire again.
///
/// Unset means never. A value that does not parse as an epoch is ignored with a warning. Always
/// `false` without `test-utils`; as with the fork-epoch overrides, a node-scoped release build
/// lacks the hook and a workspace-root build without `-p` has it.
#[inline]
pub fn exit_before_epoch_record(epoch: Epoch) -> bool {
    #[cfg(feature = "test-utils")]
    {
        exit_before_epoch_record_target() == Some(epoch)
    }
    #[cfg(not(feature = "test-utils"))]
    {
        let _ = epoch;
        false
    }
}

/// The epoch named by `TN_TEST_EXIT_BEFORE_EPOCH_RECORD`, read once per process.
#[cfg(feature = "test-utils")]
fn exit_before_epoch_record_target() -> Option<Epoch> {
    static TARGET: std::sync::OnceLock<Option<Epoch>> = std::sync::OnceLock::new();
    *TARGET.get_or_init(|| {
        // a non-unicode value is lossily converted so it is warned about rather than read as unset
        let raw = std::env::var_os("TN_TEST_EXIT_BEFORE_EPOCH_RECORD");
        parse_exit_before_epoch_record(raw.as_deref().map(|raw| raw.to_string_lossy()).as_deref())
    })
}

/// Parses a `TN_TEST_EXIT_BEFORE_EPOCH_RECORD` value into the epoch to stop at.
///
/// `None` (the variable is unset) yields `None`. Surrounding whitespace is trimmed. A value that
/// does not parse as an [`Epoch`] logs a warning and yields `None`, so a typo leaves the node
/// running rather than stopping it at a guessed epoch.
#[cfg(feature = "test-utils")]
fn parse_exit_before_epoch_record(raw: Option<&str>) -> Option<Epoch> {
    let raw = raw?;
    raw.trim()
        .parse::<Epoch>()
        .inspect_err(|err| {
            tracing::warn!(
                target: "epoch-manager",
                value = ?raw,
                %err,
                "ignoring TN_TEST_EXIT_BEFORE_EPOCH_RECORD: not an epoch number; the node will not stop",
            );
        })
        .ok()
}

#[cfg(all(test, feature = "test-utils"))]
mod tests {
    use super::*;

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

    #[test]
    fn exit_before_epoch_record_parse_unset_is_none() {
        assert_eq!(parse_exit_before_epoch_record(None), None);
    }

    #[test]
    fn exit_before_epoch_record_parse_reads_epochs() {
        for (raw, expected) in
            [("8", 8), ("0", 0), (" 8\n", 8), ("+8", 8), ("4294967295", u32::MAX)]
        {
            assert_eq!(parse_exit_before_epoch_record(Some(raw)), Some(expected), "raw = {raw:?}");
        }
    }

    #[test]
    fn exit_before_epoch_record_parse_ignores_garbage() {
        // negatives, fractions, units, hex and out-of-range values must not stop the node at a
        // guess; the replacement character is what a non-unicode value becomes in the lossy read
        for raw in ["garbage", "", "  ", "-1", "8.0", "8e", "0x8", "4294967296", "\u{FFFD}"] {
            assert_eq!(parse_exit_before_epoch_record(Some(raw)), None, "raw = {raw:?}");
        }
    }
}
