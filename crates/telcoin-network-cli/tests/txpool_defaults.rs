//! Pins how `--txpool.max-account-slots` resolves.
//!
//! Reth reads the clap default for that flag out of a process-wide `OnceLock` whose first read
//! fixes it for the life of the process, so these assertions only mean something in a process
//! where nothing built a clap command before the seeding call. That is why they live in their own
//! integration test binary rather than beside the other CLI tests: every test here seeds the same
//! value before it parses, so any interleaving of these tests yields the same lock contents, while
//! a sibling in the library test binary (`Cli::command()` in `test_parse_help_all_subcommands`,
//! for one) could take the lock first and make the default-value assertion vacuous.

#![allow(unused_crate_dependencies)]

use telcoin_network_cli::{
    cli::{Cli, Commands},
    NoArgs,
};
use tn_reth::TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER;

/// Reth's own per-sender slot default, which TN's default replaces.
///
/// Hard-coded rather than imported so the test still fails if TN's default is ever changed to
/// coincide with reth's, which would silently make the "explicit 16" case indistinguishable from
/// an unset flag again.
const RETH_MAX_ACCOUNT_SLOTS_PER_SENDER: usize = 16;

/// Parse a node command and report the per-sender slot limit it resolved to.
///
/// Goes through the crate's own parse entry point rather than seeding the defaults directly, so
/// these tests also fail if that entry point ever stops seeding them.
fn resolved_max_account_slots(args: &[&str]) -> Option<usize> {
    let cli = Cli::<NoArgs>::try_parse_args_from(args).ok()?;
    match cli.command {
        Commands::Node(node) => Some(node.reth.txpool.max_account_slots),
        Commands::Db(_) | Commands::Genesis(_) | Commands::Keytool(_) => None,
    }
}

/// With the flag absent, the operator gets Telcoin Network's raised default.
#[test]
fn absent_flag_resolves_to_tn_default() {
    assert_eq!(
        resolved_max_account_slots(&["tn", "node"]),
        Some(TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER)
    );
}

/// An explicit reth-standard 16 is honored rather than promoted to the TN default.
///
/// This is the case that regressed: while the TN default was applied by comparing the parsed value
/// against reth's default constant, an operator-supplied 16 was indistinguishable from an unset
/// flag and was silently rewritten to 256.
#[test]
fn explicit_reth_default_is_honored() {
    assert_eq!(
        resolved_max_account_slots(&["tn", "node", "--txpool.max-account-slots", "16"]),
        Some(RETH_MAX_ACCOUNT_SLOTS_PER_SENDER)
    );
}

/// The underscore alias reaches the same value as the dashed form.
#[test]
fn explicit_reth_default_is_honored_via_alias() {
    assert_eq!(
        resolved_max_account_slots(&["tn", "node", "--txpool.max_account_slots", "16"]),
        Some(RETH_MAX_ACCOUNT_SLOTS_PER_SENDER)
    );
}

/// A value that never collided with either default keeps working.
#[test]
fn explicit_other_value_is_honored() {
    assert_eq!(
        resolved_max_account_slots(&["tn", "node", "--txpool.max-account-slots", "32"]),
        Some(32)
    );
}

/// The two defaults must stay distinct, or the flag cannot express reth's value.
#[test]
fn tn_default_differs_from_reth_default() {
    assert_ne!(TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER, RETH_MAX_ACCOUNT_SLOTS_PER_SENDER);
}
