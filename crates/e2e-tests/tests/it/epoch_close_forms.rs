//! Two epoch closes under each form of the proposer's vote fix (`quorum_exec_block`), with
//! validators held behind by the engine's execution-delay test hook.
//!
//! Every test starts four validators as processes with every runtime switch of
//! [`STACK_SWITCHES_ON`] on every node (the vote fix, the vote queue fix and the epoch-record
//! switches), in the always form (`quorum_exec_block_starved_rounds: 0`) or the adaptive form
//! (`4`), and drives light transfers through all four across the closes they test.
//!
//! - [`test_held_member_two_closes_always`] and [`test_held_member_two_closes_adaptive`]: one
//!   member trails the others by more than the fix's floor of 8 outputs at the close of epoch 0,
//!   then is released before the close of epoch 1; every node holds both certified records, the
//!   held member did not sign the first, and each record's `final_state` is the block in which the
//!   chain concluded the epoch.
//! - [`test_missed_record_round_always`] and [`test_missed_record_round_adaptive`]: two of the four
//!   members miss the vote round of epoch 1's record, which gives up without a quorum; the chain
//!   goes on and the close of epoch 2 certifies that record.
//!
//! The closing block is established from the chain, not from the record path: `concludeEpoch`
//! records block N + 1 as the next epoch's first block (`getEpochInfo(E + 1).blockHeight`), and the
//! `ConsensusRegistry`'s current epoch, read with `eth_call` at block N - 1 and at block N, turns
//! from E to E + 1 exactly in that block N. The block itself carries the 32-byte closing seed in
//! `extraData` (empty on every other block) and the closing consensus header's digest as
//! `parentBeaconBlockRoot`. The registry's `NewEpoch` event is emitted by a system call, which
//! builds no receipt; the tests report what `eth_getLogs` serves for that block without asserting
//! it.

use super::{
    common::{
        acquire_test_permit, address_from_word, call_rpc, current_epoch, epoch_seconds_remaining,
        get_block, get_block_number, get_node_info, kill_child, log_field, network_advancing,
        node_log_lines, parse_hex_u64, scrape_metric_value, scrape_metrics,
        start_validator_with_overrides, EpochSnapshot, ProcessGuard,
    },
    exec_block::{
        assert_switches, expected_switches, log_str_field, node_epoch, wait_for_epoch_on_all, Load,
        CANARY_LOG, EXECUTION_DELAY_ENV, FATAL_LOGS, STACK_SWITCHES_ON,
    },
};
use alloy::{
    primitives::{utils::parse_ether, Bytes, B256},
    providers::ProviderBuilder,
    sol_types::SolCall,
};
use e2e_tests::{config_local_testnet_with_gc_depth_and_epoch_duration, FleetOverrides};
use eyre::WrapErr as _;
use jsonrpsee::rpc_params;
use serde_json::{json, Value};
use std::{
    cell::Cell,
    collections::BTreeSet,
    path::PathBuf,
    process::Child,
    time::{Duration, Instant},
};
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_test_utils::wait_until_blocking;
use tn_types::{
    get_available_tcp_port, BlsPublicKey, ConsensusHeaderDigest, EpochCertificate, EpochRecord,
    GenesisAccount, U256,
};

/// The proposer fix's floor: a header names a block at most this many non-empty outputs behind its
/// proposer's tip (`QUORUM_EXEC_BLOCK_FLOOR_ENTRIES`), so a voter further behind refuses it.
const FLOOR_OUTPUTS: usize = 8;

/// Pause of each load thread before every transfer: about 20 transfers a second per node, enough
/// for nearly every output to carry batches (and so a block).
const CLOSE_LOAD_GAP: Duration = Duration::from_millis(50);

/// The garbage-collection depth of every fleet here, as in the mixed-switch fleet test.
const CLOSE_GC_DEPTH: u32 = 50;

/// The passphrase [`start_validator_with_overrides`] gives every node.
const PASSPHRASE: &str = "restart_test";

/// The errors, besides the stop lines of [`FATAL_LOGS`], whose appearance in any node's log means
/// the fleet is not one chain: the vote round's fork stop, the voter's execution canary and the
/// execution fork.
const FORK_ONLY_LOGS: [&str; 3] = ["we have forked", CANARY_LOG, "execution has forked"];

/// Every line of [`FATAL_LOGS`] and [`FORK_ONLY_LOGS`]: no run of any node may log one.
fn fork_logs() -> impl Iterator<Item = &'static str> {
    FATAL_LOGS.into_iter().chain(FORK_ONLY_LOGS)
}

/// The info line, with fields `epoch`, `final_state_number`, `final_state_hash`,
/// `final_consensus_number`, `final_consensus_hash`, `digest` and `branch`, a node logs at its
/// close as it publishes its own record of the epoch (`log_published_record` in `close_epoch.rs`).
const PUBLISHED_LOG: &str = "epoch record published";

/// The `branch` of [`PUBLISHED_LOG`] for a record stored before the close (certified by the
/// network and fetched by the record collector) whose closing block this node executed and
/// checked against it.
const STORED_MATCHED: &str = "stored_matched";

/// The start of the info line a node logs as it signs and publishes its own vote for a record at
/// its close; the record's epoch and digest follow. A node that holds the record certified at its
/// close casts no vote and logs none.
const OWN_VOTE_LOG: &str = "publishing epoch record vote for epoch";

/// Time limit for a node's [`PUBLISHED_LOG`] line once its registry shows the next epoch.
const PUBLISHED_TIMEOUT: Duration = Duration::from_secs(30);

/// The `branch` values of [`PUBLISHED_LOG`]: a record built at the close, a stored record whose
/// closing block the node executed and checked ([`STORED_MATCHED`]), one whose closing block it
/// restored from a snapshot.
const PUBLISHED_BRANCHES: [&str; 3] = ["built", STORED_MATCHED, "stored_restored"];

/// The prometheus counter of the closing-block check runs, by `result` (`ok`, or the comparison
/// that failed): the record build at the close, the re-derive of a record a kill left unwritten, a
/// stored record reused at the close and the previous record at every epoch entry.
const CLOSING_BLOCK_CHECKS: &str = "tn_epoch_epoch_record_closing_block_check_total";

/// The prometheus counter of the close-time base-fee identity check (canonical tip + 1 is the
/// entered epoch's `blockHeight`), by `result` (`ok` or `mismatch`).
const IDENTITY_CHECKS: &str = "tn_epoch_epoch_close_identity_check_total";

/// The prometheus gauge of the highest epoch E for which a node holds a record and its certificate
/// for every epoch `0..=E`, set by the record collector after each pass (at most 5 s apart).
const CERTIFIED_THROUGH: &str = "tn_state_sync_epoch_records_certified_through";

/// Time limit for every node's [`CERTIFIED_THROUGH`] to reach its last closed epoch.
const CERTIFIED_THROUGH_TIMEOUT: Duration = Duration::from_secs(60);

/// Time limit for one node to serve a certified record once every node is past its epoch.
const RECORD_TIMEOUT: Duration = Duration::from_secs(120);

/// The validator held behind in the held-member tests.
const HELD_MEMBER: usize = 3;

/// The validators that execute at full speed in the held-member tests.
const FAST_MEMBERS: [usize; 3] = [0, 1, 2];

/// Milliseconds [`HELD_MEMBER`] sleeps before it executes an output with batches.
///
/// Above the commit interval under load (about 0.3 s at the harness's cadence, measured by the
/// slow-member close test), so its backlog grows by one to two outputs a second while the load
/// runs: about 30 to 70 outputs over [`HELD_LOAD_LEAD_SECS`], far past [`FLOOR_OUTPUTS`], and a
/// drain of about 15 to 45 s after the close. The drain spans at least two of the record
/// collector's 5-s retries, so the held member finds the certified record stored when it closes.
const HELD_DELAY_MS: u64 = 600;

/// Epoch duration of the held-member tests: the load lead, then the held member's drain (at most
/// about 45 s), its restart and catch-up (about 20 s) inside epoch 1, with margin.
const HELD_EPOCH_SECS: u32 = 90;

/// How long before the epoch-0 boundary the load starts in the held-member tests. Before it the
/// outputs carry no batches, the delay does not apply and the held member keeps pace, so this
/// bounds the lag it builds up by the close.
const HELD_LOAD_LEAD_SECS: u64 = 35;

/// Time limit for each wait on a close in the held-member tests.
const HELD_EPOCH_TIMEOUT: Duration = Duration::from_secs(HELD_EPOCH_SECS as u64 * 3);

/// Time limit for the held member to finish its drain and close epoch 0.
const HELD_CLOSE_TIMEOUT: Duration = Duration::from_secs(150);

/// Time limit for the released member to execute up to the head the others had at its release.
const CATCH_UP_TIMEOUT: Duration = Duration::from_secs(120);

/// The epoch whose first record vote round the missed-round tests make fail.
///
/// Not epoch 0: a node stopped after executing epoch 0's closing block and before persisting record
/// 0 cannot restart ("We have epoch 0 in our database if we are past epoch 0", a known gap that
/// `eject.rs` works around too), so the release would not bring the held members back.
const MISSED_EPOCH: u32 = 1;

/// The validators held out of the first vote round in the missed-round tests: with four members a
/// record needs `floor(2 * 4 / 3) + 1 = 3` signers, so two fast members alone cannot certify it.
const ROUND_HELD: [usize; 2] = [2, 3];

/// The validators that close epoch 0 on time in the missed-round tests.
const ROUND_FAST: [usize; 2] = [0, 1];

/// Milliseconds each of [`ROUND_HELD`] sleeps before it executes an output with batches: they
/// trail the fast members into the close, as slow members do.
const ROUND_DELAY_MS: u64 = 500;

/// The test-utils hook that stops a node at the live close of the named epoch, after the closing
/// block executed and before the epoch record is written (`tn_types::test_hooks`).
const EXIT_BEFORE_EPOCH_RECORD_ENV: &str = "TN_TEST_EXIT_BEFORE_EPOCH_RECORD";

/// The warn line, with field `epoch`, a node logs as [`EXIT_BEFORE_EPOCH_RECORD_ENV`] stops it.
const EXIT_HOOK_LOG: &str = "test hook: exiting before the epoch record is written";

/// The error a vote round logs when it gives up without a quorum and no certificate for its record
/// is stored (after 25 timed-out 2.5-s waits); a round that ends while the certificate is stored
/// logs an info line instead.
const ROUND_FAILED_LOG: &str = "failed to reach quorum on epoch close";

/// The error the node-lifetime vote collector logs when a new record arrives while the previous
/// epoch's record is still uncertified, before it re-signs and republishes that record's vote.
const REVOTE_LOG: &str = "re-publishing epoch record vote for epoch";

/// Epoch duration of the missed-round tests.
///
/// The first round gives up about 65 s after the fast members close (25 timed-out 2.5-s waits plus
/// the waits in which a vote arrived); the held members are released then and need about 15 s to
/// come back. The next epoch starts at the close of [`MISSED_EPOCH`], so with 120 s it still has
/// about 40 s with all four members, under load, after the round gave up and before its own close
/// re-votes the missed record.
const ROUND_EPOCH_SECS: u32 = 120;

/// Time limit for the fast members to close [`MISSED_EPOCH`] in the missed-round tests.
const ROUND_EPOCH_TIMEOUT: Duration = Duration::from_secs(ROUND_EPOCH_SECS as u64 * 4);

/// Time limit for a held member to stop at its close of epoch 0 once the fast members closed it.
const ROUND_HELD_EXIT_TIMEOUT: Duration = Duration::from_secs(120);

/// Time limit for a fast member's first vote round to give up once it closed epoch 0.
const ROUND_GIVE_UP_TIMEOUT: Duration = Duration::from_secs(150);

/// Time limit for every node to close epoch 1 once the held members were released.
const ROUND_SECOND_CLOSE_TIMEOUT: Duration = Duration::from_secs(ROUND_EPOCH_SECS as u64 * 4);

/// A form of the proposer's vote fix.
#[derive(Clone, Copy, Debug)]
enum VoteForm {
    /// `quorum_exec_block_starved_rounds: 0`: the quorum rule on every header.
    Always,
    /// `quorum_exec_block_starved_rounds: 4`: the own tip until an own header is cancelled.
    Adaptive,
}

impl VoteForm {
    /// The form's name in test-log directories.
    fn name(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::Adaptive => "adaptive",
        }
    }

    /// The `parameters.yaml` entries every node gets, as in the benchmark's arms of this form:
    /// every switch of [`STACK_SWITCHES_ON`], with [`STARVED_ROUNDS`] set to the form's count.
    ///
    /// A switch added to the shared list is on in both forms with no change here.
    fn switches(self) -> eyre::Result<Vec<(&'static str, &'static str)>> {
        let starved = match self {
            Self::Always => "0",
            Self::Adaptive => "4",
        };
        eyre::ensure!(
            STACK_SWITCHES_ON.iter().any(|(key, _)| *key == STARVED_ROUNDS),
            "STACK_SWITCHES_ON has no `{STARVED_ROUNDS}` entry for the form to set"
        );
        Ok(STACK_SWITCHES_ON
            .iter()
            .map(|&(key, on)| (key, if key == STARVED_ROUNDS { starved } else { on }))
            .collect())
    }

    /// The switches on every node, with nothing else set.
    fn overrides(self) -> eyre::Result<FleetOverrides> {
        Ok(self.switches()?.into_iter().fold(FleetOverrides::default(), |fleet, (key, value)| {
            fleet.parameter((1..=4).map(|i| format!("validator-{i}")), key, value)
        }))
    }

    /// What the `node switches` line of every node must show.
    fn expected_switches(self) -> eyre::Result<Vec<(&'static str, &'static str)>> {
        expected_switches(&self.switches()?)
    }
}

/// The switch whose value tells the two forms of the vote fix apart.
const STARVED_ROUNDS: &str = "quorum_exec_block_starved_rounds";

/// One validator trails the others by more than the floor at the close of epoch 0 under the always
/// form; see [`held_member_two_closes`].
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_held_member_two_closes_always() -> eyre::Result<()> {
    held_member_two_closes(VoteForm::Always)
}

/// One validator trails the others by more than the floor at the close of epoch 0 under the
/// adaptive form; see [`held_member_two_closes`].
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_held_member_two_closes_adaptive() -> eyre::Result<()> {
    held_member_two_closes(VoteForm::Adaptive)
}

/// Two of four validators miss the first vote round under the always form; see
/// [`missed_record_round`].
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_missed_record_round_always() -> eyre::Result<()> {
    missed_record_round(VoteForm::Always)
}

/// Two of four validators miss the first vote round under the adaptive form; see
/// [`missed_record_round`].
#[test]
#[ignore = "should not run with a default cargo test: starts a four-validator network"]
fn test_missed_record_round_adaptive() -> eyre::Result<()> {
    missed_record_round(VoteForm::Adaptive)
}

/// A member held more than [`FLOOR_OUTPUTS`] outputs behind at the close of epoch 0 holds the
/// record the others certified without it, and every node holds both records with the chain's
/// closing blocks as their `final_state`.
///
/// [`HELD_MEMBER`] sleeps [`HELD_DELAY_MS`] before every output with batches. The load starts
/// [`HELD_LOAD_LEAD_SECS`] before the epoch-0 boundary, so the held member's backlog grows only in
/// the last part of the epoch. The test measures that backlog in outputs (distinct block nonces
/// between the held member's head when the first fast member entered epoch 1 and the chain's
/// closing block) and requires more than the floor. Once the held member has closed epoch 0 it is
/// restarted without the delay (the release), catches up inside epoch 1 and the fleet closes epoch
/// 1 with all four in step.
///
/// For records 0 and 1, on every node: a certified record that verifies, with one digest across the
/// fleet; its `final_state` is the block in which the registry's epoch turned (number and hash),
/// its committees are the registry's at that block, and the next block is the first of the next
/// epoch with the closing block as its parent. Record 0's certificate on every node, the held
/// member's own copy included, lacks the held member's signature; the held member reached its close
/// with the certified record stored: it published record 0 as [`STORED_MATCHED`] (its replay guard
/// compared the stored record with its own closing block without halting) and logged no vote of
/// its own for it. No node logs a line of [`fork_logs`]. At the end every node's
/// [`CERTIFIED_THROUGH`] is its last closed epoch.
fn held_member_two_closes(form: VoteForm) -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = format!("held_member_two_closes_{}", form.name());
    let release = form.overrides()?;
    let held = release.clone().env(
        [format!("validator-{}", HELD_MEMBER + 1)],
        EXECUTION_DELAY_ENV,
        HELD_DELAY_MS.to_string(),
    );
    let mut fleet = CloseFleet::start(&test, HELD_EPOCH_SECS, &held)?;
    let mut load = None;
    let checked = check_held_member(&mut fleet, form, &release, &mut load);
    let sent = load.map(Load::stop).transpose();
    fleet.guard.kill_all();
    let report = checked.wrap_err(format!("{test}: check logs in test_logs/{test}/"))?;
    eprintln!("{test}: {report}, {} transfers sent", sent?.unwrap_or_default());
    Ok(())
}

/// The checks of [`held_member_two_closes`]; starts the load into `load`.
fn check_held_member(
    fleet: &mut CloseFleet,
    form: VoteForm,
    release: &FleetOverrides,
    load: &mut Option<Load>,
) -> eyre::Result<String> {
    let urls = fleet.urls.clone();
    let test = fleet.test.clone();
    let expected = form.expected_switches()?;
    network_advancing(&urls)?;
    for node in 0..4 {
        assert_switches(&test, node, 0, &expected)?;
    }

    // the load starts a fixed lead before the epoch-0 boundary, which bounds the held member's lag
    let snapshot = epoch_snapshot(&urls[0])?;
    let remaining = epoch_seconds_remaining(&urls[0], &snapshot)?;
    eyre::ensure!(
        remaining > HELD_LOAD_LEAD_SECS,
        "only {remaining} s of epoch 0 were left when the fleet was up, less than the load lead of \
         {HELD_LOAD_LEAD_SECS} s"
    );
    wait_until_blocking(HELD_EPOCH_TIMEOUT, "the epoch-0 boundary is the load lead away", || {
        Ok(epoch_seconds_remaining(&urls[0], &snapshot)? <= HELD_LOAD_LEAD_SECS)
    })?;
    load.insert(Load::start(&urls, CLOSE_LOAD_GAP)).wait_started()?;

    // the lag at the first close: the held member's head when the first fast member closed
    let held_head = held_head_at_first_close(&urls)?;
    wait_until_blocking(HELD_EPOCH_TIMEOUT, "every fast member enters epoch 1", || {
        Ok(FAST_MEMBERS.iter().all(|&i| node_epoch(&urls[i]).is_ok_and(|e| e >= 1)))
    })?;
    let closing0 = closing_block_from_chain(&urls[0], 0)?;
    let lag_outputs = outputs_after(&urls[0], held_head, closing0.number)?;
    let lag_blocks = closing0.number.saturating_sub(held_head);
    eyre::ensure!(
        lag_outputs > FLOOR_OUTPUTS,
        "the held member trailed the closing block of epoch 0 by {lag_outputs} outputs ({lag_blocks} \
         blocks) when the first fast member closed it: more than the floor of {FLOOR_OUTPUTS} was \
         intended, so the close did not test what it claims"
    );

    // the held member closes late, with the record certified by the three others already stored
    wait_until_blocking(HELD_CLOSE_TIMEOUT, "the held member closes epoch 0", || {
        Ok(node_epoch(&urls[HELD_MEMBER]).is_ok_and(|e| e >= 1))
    })?;
    wait_until_blocking(PUBLISHED_TIMEOUT, "the held member publishes its record 0", || {
        Ok(!published_lines(&test, HELD_MEMBER, 0, 0)?.is_empty())
    })?;
    let held_close = published_lines(&test, HELD_MEMBER, 0, 0)?.swap_remove(0);
    let own_vote = own_vote_lines(&test, HELD_MEMBER, 0, 0)?;
    eyre::ensure!(
        log_str_field(&held_close, "branch") == Some(STORED_MATCHED) && own_vote.is_empty(),
        "the held member closed epoch 0 without the certified record stored: it published \
         {held_close:?} and logged its own vote {own_vote:?}"
    );

    // the counters live in the held member's process: read them before the release restarts it
    let held_checks = assert_close_checks(fleet, HELD_MEMBER)?;

    // the release: the held member restarts without the delay and catches up inside epoch 1
    fleet.restart(HELD_MEMBER, release)?;
    assert_switches(&test, HELD_MEMBER, 1, &expected)?;
    let target = get_block_number(&urls[0])?;
    wait_until_blocking(
        CATCH_UP_TIMEOUT,
        &format!("the released member executes block {target}"),
        || Ok(get_block_number(&urls[HELD_MEMBER]).is_ok_and(|h| h >= target)),
    )?;
    let caught_up_in = node_epoch(&urls[0])?;
    eyre::ensure!(
        caught_up_in == 1,
        "the released member caught up with block {target} only in epoch {caught_up_in}: it was not \
         in step before the second close"
    );

    wait_for_epoch_on_all(&urls, 2, HELD_EPOCH_TIMEOUT)?;
    let first = check_closed_epoch(fleet, 0)?;
    let second = check_closed_epoch(fleet, 1)?;

    // the held member did not sign record 0, yet holds it with a certificate of the others
    let held_key = node_bls_key(&urls[HELD_MEMBER])?;
    let held_index = first
        .record
        .committee
        .iter()
        .position(|key| *key == held_key)
        .ok_or_else(|| eyre::eyre!("the held member's key is not in record 0's committee"))?;
    for (node, cert) in first.certs.iter().enumerate() {
        eyre::ensure!(
            !cert.signed_authorities.contains(u32::try_from(held_index)?),
            "node {node}'s certificate of record 0 carries the held member's signature (committee \
             index {held_index}): signers {:?}",
            signers(cert)
        );
    }
    fleet.assert_one_chain_logs()?;
    let checks =
        (0..4).map(|node| assert_close_checks(fleet, node)).collect::<Result<Vec<_>, _>>()?;
    let through = assert_certified_through(fleet)?;

    Ok(format!(
        "certified through (gauge, registry epoch) per node {through:?}; held member {HELD_MEMBER} \
         trailed the closing block {} of epoch 0 by {lag_outputs} outputs \
         ({lag_blocks} blocks); its close {held_close:?}; its checks before the release: \
         {held_checks}; record 0: {}; record 1 (after the release, held index {held_index}): {}; \
         checks at the end {checks:?}",
        closing0.number,
        first.describe(),
        second.describe()
    ))
}

/// Two members held out of the first vote round leave record [`MISSED_EPOCH`] uncertified on the
/// fast members; the chain goes on and the next close certifies it on every node.
///
/// [`ROUND_HELD`] sleep [`ROUND_DELAY_MS`] before every output with batches and run with
/// [`EXIT_BEFORE_EPOCH_RECORD_ENV`] set to [`MISSED_EPOCH`], so each stops at its close of that
/// epoch after the closing block executed and before it writes or votes for the record. The delay
/// alone cannot keep them out of the window: with four members the DAG needs one of them to vote
/// for every round, so the fix's floor bounds their backlog to about 8 outputs and their close to a
/// few seconds after the fast members'. The fast members [`ROUND_FAST`] vote alone; each round must
/// log [`ROUND_FAILED_LOG`] for the record, and neither may then serve it certified. The held
/// members are then restarted without the delay or the hook (the release); they rebuild the record
/// from their closing block and enter the next epoch.
///
/// While the fleet waits for the next close, no node may serve the record certified before some
/// node has made that close. After it: at least one node logged the collector's re-vote of the
/// record ([`REVOTE_LOG`]); the records of epochs 0 to [`MISSED_EPOCH`] + 1 meet every check of
/// [`check_closed_epoch`] on every node; the next epoch committed outputs beyond its closing one;
/// no node logs a line of [`fork_logs`]; every node's [`CERTIFIED_THROUGH`] is its last closed
/// epoch.
fn missed_record_round(form: VoteForm) -> eyre::Result<()> {
    let _permit = acquire_test_permit();
    let test = format!("missed_record_round_{}", form.name());
    let release = form.overrides()?;
    let names = ROUND_HELD.map(|i| format!("validator-{}", i + 1));
    let held = release
        .clone()
        .env(names.clone(), EXECUTION_DELAY_ENV, ROUND_DELAY_MS.to_string())
        .env(names, EXIT_BEFORE_EPOCH_RECORD_ENV, MISSED_EPOCH.to_string());
    let mut fleet = CloseFleet::start(&test, ROUND_EPOCH_SECS, &held)?;
    let mut load = None;
    let checked = check_missed_round(&mut fleet, form, &release, &mut load);
    let sent = load.map(Load::stop).transpose();
    fleet.guard.kill_all();
    let report = checked.wrap_err(format!("{test}: check logs in test_logs/{test}/"))?;
    eprintln!("{test}: {report}, {} transfers sent", sent?.unwrap_or_default());
    Ok(())
}

/// The checks of [`missed_record_round`]; starts the load into `load`.
fn check_missed_round(
    fleet: &mut CloseFleet,
    form: VoteForm,
    release: &FleetOverrides,
    load: &mut Option<Load>,
) -> eyre::Result<String> {
    let urls = fleet.urls.clone();
    let test = fleet.test.clone();
    let expected = form.expected_switches()?;
    let (missed, next) = (MISSED_EPOCH, MISSED_EPOCH + 1);
    network_advancing(&urls)?;
    for node in 0..4 {
        assert_switches(&test, node, 0, &expected)?;
    }
    load.insert(Load::start(&urls, CLOSE_LOAD_GAP)).wait_started()?;

    // the fast members close the epoch; the held members stop at their close before the record
    wait_until_blocking(
        ROUND_EPOCH_TIMEOUT,
        &format!("the fast members enter epoch {next}"),
        || Ok(ROUND_FAST.iter().all(|&i| node_epoch(&urls[i]).is_ok_and(|e| e >= next))),
    )?;
    let fast_closed = Instant::now();
    for node in ROUND_HELD {
        fleet
            .wait_for_exit(node, ROUND_HELD_EXIT_TIMEOUT)
            .wrap_err_with(|| format!("held member {node} stops at its close of epoch {missed}"))?;
        eyre::ensure!(
            node_log_lines(&test, node, 0)?
                .iter()
                .any(|l| l.contains(EXIT_HOOK_LOG) && log_field(l, "epoch") == Some(missed.into())),
            "held member {node} exited without the hook's line for epoch {missed}"
        );
    }

    // the first round gives up on both fast members: two of four votes are no quorum
    let record_debug = format!("EpochRecord {{ epoch: {missed},");
    for node in ROUND_FAST {
        wait_until_blocking(
            ROUND_GIVE_UP_TIMEOUT,
            &format!("fast member {node} gives up the vote round of record {missed}"),
            || {
                Ok(node_log_lines(&test, node, 0)?
                    .iter()
                    .any(|l| l.contains(ROUND_FAILED_LOG) && l.contains(&record_debug)))
            },
        )?;
    }
    let gave_up_after = fast_closed.elapsed();
    for node in ROUND_FAST {
        eyre::ensure!(
            certified_record(&urls[node], missed).is_err(),
            "fast member {node} serves a certified record {missed} although its vote round failed"
        );
    }

    // the release: the held members restart without the delay and without the hook
    for node in ROUND_HELD {
        fleet.restart(node, release)?;
        assert_switches(&test, node, 1, &expected)?;
    }
    let released_at = Instant::now();

    // until some node closes the next epoch, no node may hold a certificate for the missed record
    let deadline = Instant::now() + ROUND_SECOND_CLOSE_TIMEOUT;
    loop {
        let certified = (0..4)
            .filter(|&node| certified_record(&urls[node], missed).is_ok())
            .collect::<Vec<_>>();
        let epochs = urls.iter().map(|url| node_epoch(url).ok()).collect::<Vec<_>>();
        if epochs.iter().all(Option::is_some) {
            let closed = epochs.iter().flatten().filter(|e| **e > next).count();
            eyre::ensure!(
                certified.is_empty() || closed > 0,
                "record {missed} is certified on nodes {certified:?} while no node has closed epoch \
                 {next} (registry epochs {epochs:?}): the first round or the release certified it, \
                 not the next close"
            );
            if closed == 4 {
                break;
            }
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "not every node closed epoch {next} within {ROUND_SECOND_CLOSE_TIMEOUT:?} of the \
             release: registry epochs {epochs:?}"
        );
        std::thread::sleep(Duration::from_secs(1));
    }
    let next_close_after = released_at.elapsed();

    let mut closed = Vec::new();
    for epoch in 0..=next {
        closed.push(check_closed_epoch(fleet, epoch)?);
    }
    let (first, second) = (&closed[missed as usize], &closed[next as usize]);
    let revote = format!("{REVOTE_LOG} {missed} ");
    let revoted = (0..4)
        .filter(|&node| {
            fleet.lines(node).is_ok_and(|lines| lines.iter().any(|l| l.contains(&revote)))
        })
        .collect::<Vec<_>>();
    eyre::ensure!(
        !revoted.is_empty(),
        "no node logged the re-vote of record {missed} at its close of epoch {next}"
    );
    let outputs = second.record.final_consensus.number - first.record.final_consensus.number;
    let blocks = second.closing.number - first.closing.number;
    eyre::ensure!(
        outputs > 1,
        "epoch {next} committed no output before its closing one (consensus numbers {} to {})",
        first.record.final_consensus.number,
        second.record.final_consensus.number
    );
    fleet.assert_one_chain_logs()?;
    let checks =
        (0..4).map(|node| assert_close_checks(fleet, node)).collect::<Result<Vec<_>, _>>()?;
    let through = assert_certified_through(fleet)?;

    Ok(format!(
        "certified through (gauge, registry epoch) per node {through:?}; checks at the end \
         {checks:?}; the first round of record {missed} gave up {:.0} s after the fast members closed epoch \
         {missed}; epoch {next} closed {:.0} s after the release with {outputs} outputs and \
         {blocks} blocks; nodes {revoted:?} re-voted record {missed}; records: {:?}",
        gave_up_after.as_secs_f64(),
        next_close_after.as_secs_f64(),
        closed.iter().map(ClosedEpoch::describe).collect::<Vec<_>>()
    ))
}

/// Four validators started as processes for one test, each restartable with its own overrides.
///
/// The genesis is built once in [`Self::start`]; every start and restart of a node runs on the
/// same datadirs and the same genesis.
struct CloseFleet {
    /// The test-log directory name under `test_logs/`.
    test: String,
    /// The fleet's datadirs; removed when the fleet drops.
    _temp: tempfile::TempDir,
    /// The path of [`Self::_temp`].
    temp_path: PathBuf,
    /// Each node's HTTP RPC url.
    urls: [String; 4],
    /// Each node's RPC port, kept for restarts.
    rpc_ports: [u16; 4],
    /// Each node's metrics address, kept for restarts.
    metrics: [String; 4],
    /// The latest run of each node.
    runs: [u32; 4],
    /// The node processes.
    guard: ProcessGuard,
}

impl CloseFleet {
    /// Configure four validators with `epoch_secs` epochs, fund the load accounts of [`Load`] and
    /// start every node as run 0 with `overrides`.
    fn start(test: &str, epoch_secs: u32, overrides: &FleetOverrides) -> eyre::Result<Self> {
        // the genesis timestamp starts epoch 0's clock, so the binary (a cargo build per test
        // process, which waited ~100 s on a busy host) is resolved before the genesis is made
        let _ = e2e_tests::get_telcoin_network_binary();
        let temp = tempfile::TempDir::with_prefix(test)?;
        let temp_path = temp.path().to_path_buf();
        let balance = U256::from(parse_ether("10_000_000")?);
        let funded = (0..4)
            .map(|i| {
                (
                    address_from_word(&Load::source(i)),
                    GenesisAccount::default().with_balance(balance),
                )
            })
            .collect::<Vec<_>>();
        config_local_testnet_with_gc_depth_and_epoch_duration(
            &temp_path,
            Some(PASSPHRASE.to_string()),
            Some(funded),
            CLOSE_GC_DEPTH,
            epoch_secs,
        )?;
        let port = || get_available_tcp_port("127.0.0.1").ok_or_else(|| eyre::eyre!("no port"));
        let mut rpc_ports = [0; 4];
        let mut metrics: [String; 4] = Default::default();
        for (rpc_port, metrics) in rpc_ports.iter_mut().zip(metrics.iter_mut()) {
            *rpc_port = port()?;
            *metrics = format!("127.0.0.1:{}", port()?);
        }
        let urls = rpc_ports.map(|p| format!("http://127.0.0.1:{p}"));
        let mut fleet = Self {
            test: test.to_string(),
            _temp: temp,
            temp_path,
            urls,
            rpc_ports,
            metrics,
            runs: [0; 4],
            guard: ProcessGuard::empty(),
        };
        for node in 0..4 {
            let child = fleet.spawn(node, 0, overrides)?;
            fleet.guard.push(child);
        }
        Ok(fleet)
    }

    /// Spawn node `node` as run `run` with `overrides`, its RPC port and its metrics address.
    fn spawn(&self, node: usize, run: u32, overrides: &FleetOverrides) -> eyre::Result<Child> {
        let args = ["--metrics", self.metrics[node].as_str()];
        start_validator_with_overrides(
            node,
            e2e_tests::get_telcoin_network_binary(),
            &self.temp_path,
            self.rpc_ports[node],
            &self.test,
            run,
            &args,
            overrides,
        )
    }

    /// Stop node `node` if it still runs and start its next run with `overrides`.
    fn restart(&mut self, node: usize, overrides: &FleetOverrides) -> eyre::Result<()> {
        if let Some(mut child) = self.guard.take(node) {
            kill_child(&mut child);
        }
        let run = self.runs[node] + 1;
        let child = self.spawn(node, run, overrides)?;
        self.guard.replace(node, child);
        self.runs[node] = run;
        Ok(())
    }

    /// Wait up to `limit` for node `node`'s process to exit on its own, then drop it from the
    /// guard, so that no teardown signals the pid of a process already reaped.
    fn wait_for_exit(&mut self, node: usize, limit: Duration) -> eyre::Result<()> {
        let deadline = Instant::now() + limit;
        loop {
            let child =
                self.guard.get_mut(node).ok_or_else(|| eyre::eyre!("node {node} is not held"))?;
            if let Some(status) = child.try_wait()? {
                tracing::info!(target: "e2e-test", node, %status, "node exited on its own");
                drop(self.guard.take(node));
                return Ok(());
            }
            eyre::ensure!(
                Instant::now() < deadline,
                "node {node} run {} still runs after {limit:?}",
                self.runs[node]
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Every log line of every run of node `node`.
    fn lines(&self, node: usize) -> eyre::Result<Vec<String>> {
        let mut lines = Vec::new();
        for run in 0..=self.runs[node] {
            lines.extend(node_log_lines(&self.test, node, run)?);
        }
        Ok(lines)
    }

    /// Node `node`'s [`PUBLISHED_LOG`] lines for `epoch` in every run, in run order.
    fn published(&self, node: usize, epoch: u32) -> eyre::Result<Vec<String>> {
        let mut lines = Vec::new();
        for run in 0..=self.runs[node] {
            lines.extend(published_lines(&self.test, node, run, epoch)?);
        }
        Ok(lines)
    }

    /// No run of any node logged a line of [`fork_logs`].
    fn assert_one_chain_logs(&self) -> eyre::Result<()> {
        for node in 0..4 {
            for run in 0..=self.runs[node] {
                let lines = node_log_lines(&self.test, node, run)?;
                for text in fork_logs() {
                    let hit = lines.iter().find(|line| line.contains(text));
                    eyre::ensure!(hit.is_none(), "node {node} run {run} logged `{text}`: {hit:?}");
                }
            }
        }
        Ok(())
    }
}

/// A closed epoch's record as every node holds it.
struct ClosedEpoch {
    /// The record (identical on every node).
    record: EpochRecord,
    /// Each node's certificate for it.
    certs: Vec<EpochCertificate>,
    /// The epoch's closing block as the chain marks it.
    closing: ChainBlock,
    /// The registry's logs in the closing block that `eth_getLogs` serves on node 0: reported, not
    /// asserted, since system calls build no receipt.
    registry_logs: usize,
    /// The `branch` of each node's [`PUBLISHED_LOG`] lines for the epoch, in run order.
    branches: Vec<Vec<String>>,
}

impl ClosedEpoch {
    /// One line for the test's report.
    fn describe(&self) -> String {
        format!(
            "epoch {} closing block {} {} ({} registry logs by eth_getLogs), signers per node \
             {:?}, published branches per node {:?}",
            self.record.epoch,
            self.closing.number,
            self.closing.hash,
            self.registry_logs,
            self.certs.iter().map(signers).collect::<Vec<_>>(),
            self.branches
        )
    }
}

/// Every node holds a certified record for `epoch` whose certificate verifies, the record's digest
/// is the same on every node, on every node it matches the chain (see [`check_record_on_chain`]),
/// and every node's [`PUBLISHED_LOG`] lines for it name the chain's closing block (see
/// [`check_published_lines`]).
fn check_closed_epoch(fleet: &CloseFleet, epoch: u32) -> eyre::Result<ClosedEpoch> {
    let mut held = Vec::new();
    for (node, url) in fleet.urls.iter().enumerate() {
        let deadline = Instant::now() + RECORD_TIMEOUT;
        let (record, cert) = loop {
            match certified_record(url, epoch) {
                Ok(found) => break found,
                Err(e) => eyre::ensure!(
                    Instant::now() < deadline,
                    "node {node} served no certified record for epoch {epoch} within \
                     {RECORD_TIMEOUT:?}: {e}"
                ),
            }
            std::thread::sleep(Duration::from_millis(500));
        };
        eyre::ensure!(
            record.verify_with_cert(&cert),
            "node {node}'s certificate for epoch {epoch} does not verify its record {}",
            record.digest()
        );
        held.push((record, cert));
    }
    let digest = held[0].0.digest();
    for (node, (record, _)) in held.iter().enumerate() {
        eyre::ensure!(
            record.digest() == digest,
            "node {node} holds record {} for epoch {epoch}, node 0 holds {digest}: {record:?}",
            record.digest()
        );
    }
    let record = held[0].0.clone();
    let mut closing = Vec::new();
    for (node, url) in fleet.urls.iter().enumerate() {
        closing.push(
            check_record_on_chain(url, &record)
                .wrap_err_with(|| format!("record {epoch} against node {node}'s chain"))?,
        );
    }
    let closing = closing.swap_remove(0);
    let registry_logs = registry_logs_in_block(&fleet.urls[0], closing.number)?;
    let branches = check_published_lines(fleet, &record, &closing)?;
    Ok(ClosedEpoch {
        record,
        certs: held.into_iter().map(|(_, cert)| cert).collect(),
        closing,
        registry_logs,
        branches,
    })
}

/// Every node logged [`PUBLISHED_LOG`] for `record`'s epoch in some run, and every such line names
/// what the chain shows for that close: the closing block's number and hash as `final_state`, the
/// closing block's `parentBeaconBlockRoot` as the `final_consensus` hash, with a `branch` of
/// [`PUBLISHED_BRANCHES`]. Its `final_consensus_number` and `digest` are the certified record's,
/// which [`check_record_on_chain`] tied to the chain. The node prints both digests through their
/// `Display`, a 16-character prefix, and so they are compared as printed. Returns each node's
/// branches in run order.
fn check_published_lines(
    fleet: &CloseFleet,
    record: &EpochRecord,
    closing: &ChainBlock,
) -> eyre::Result<Vec<Vec<String>>> {
    let epoch = record.epoch;
    let beacon = closing.consensus_digest.map(ConsensusHeaderDigest::from).ok_or_else(|| {
        eyre::eyre!(
            "the closing block {} of epoch {epoch} has no parentBeaconBlockRoot",
            closing.number
        )
    })?;
    let expected = [
        ("final_state_number", closing.number.to_string()),
        ("final_state_hash", closing.hash.clone()),
        ("final_consensus_number", record.final_consensus.number.to_string()),
        ("final_consensus_hash", beacon.to_string()),
        ("digest", record.digest().to_string()),
    ];
    let mut branches = Vec::new();
    for node in 0..4 {
        wait_until_blocking(
            PUBLISHED_TIMEOUT,
            &format!("node {node} logs `{PUBLISHED_LOG}` for epoch {epoch}"),
            || Ok(!fleet.published(node, epoch)?.is_empty()),
        )?;
        let mut node_branches = Vec::new();
        for line in fleet.published(node, epoch)? {
            for (name, want) in &expected {
                let got = log_str_field(&line, name);
                eyre::ensure!(
                    got.is_some_and(|got| got.eq_ignore_ascii_case(want)),
                    "node {node} published record {epoch} with {name}={got:?}, the chain's close \
                     shows {want}: {line}"
                );
            }
            let branch = log_str_field(&line, "branch").unwrap_or_default();
            eyre::ensure!(
                PUBLISHED_BRANCHES.contains(&branch),
                "node {node} published record {epoch} on branch {branch:?}: {line}"
            );
            node_branches.push(branch.to_owned());
        }
        branches.push(node_branches);
    }
    Ok(branches)
}

/// The `result` label and the value of every sample of the counter `name` that node `node` serves;
/// none while the node has counted nothing.
fn results(fleet: &CloseFleet, node: usize, name: &str) -> eyre::Result<Vec<(String, f64)>> {
    let prefix = format!("{name}{{");
    scrape_metrics(&fleet.metrics[node])?
        .lines()
        .filter(|line| line.starts_with(&prefix))
        .map(|line| {
            let result = line
                .split_once("result=\"")
                .and_then(|(_, rest)| rest.split_once('"'))
                .map_or_else(String::new, |(result, _)| result.to_owned());
            let value = line.rsplit(' ').next().unwrap_or_default();
            let value =
                value.parse::<f64>().map_err(|e| eyre::eyre!("{name} sample {line:?}: {e}"))?;
            Ok((result, value))
        })
        .collect()
}

/// Node `node`'s closing-block checks ([`CLOSING_BLOCK_CHECKS`]) and close-time identity checks
/// ([`IDENTITY_CHECKS`]) since its latest start all passed: each counter has an `ok` sample of at
/// least one and no other sample above zero. Returns the two `ok` counts.
fn assert_close_checks(fleet: &CloseFleet, node: usize) -> eyre::Result<String> {
    let mut ok = Vec::new();
    for name in [CLOSING_BLOCK_CHECKS, IDENTITY_CHECKS] {
        let samples = results(fleet, node, name)?;
        let passed: f64 = samples.iter().filter(|(result, _)| result == "ok").map(|(_, v)| v).sum();
        let failed = samples.iter().any(|(result, value)| result != "ok" && *value > 0.0);
        eyre::ensure!(
            passed >= 1.0 && !failed,
            "node {node} run {} counted {name} {samples:?}: every check since its start must pass, \
             and at least one must have run",
            fleet.runs[node]
        );
        ok.push(passed);
    }
    Ok(format!("node {node}: closing-block ok {}, identity ok {}", ok[0], ok[1]))
}

/// Every node's [`CERTIFIED_THROUGH`] equals the last epoch its registry shows closed (the current
/// epoch minus one), both read together, within [`CERTIFIED_THROUGH_TIMEOUT`]. Returns each node's
/// last `(gauge, registry epoch)` reading.
fn assert_certified_through(fleet: &CloseFleet) -> eyre::Result<Vec<(f64, u32)>> {
    let mut readings = Vec::new();
    for node in 0..4 {
        let last = Cell::new((f64::NAN, 0));
        wait_until_blocking(
            CERTIFIED_THROUGH_TIMEOUT,
            &format!("node {node}'s {CERTIFIED_THROUGH} is its last closed epoch"),
            || {
                let epoch = node_epoch(&fleet.urls[node])?;
                let through = scrape_metric_value(&fleet.metrics[node], CERTIFIED_THROUGH)?;
                last.set((through, epoch));
                Ok(through == f64::from(epoch) - 1.0)
            },
        )
        .wrap_err_with(|| {
            let (through, epoch) = last.get();
            format!("node {node}'s last reading: {CERTIFIED_THROUGH} {through} in epoch {epoch}")
        })?;
        readings.push(last.get());
    }
    Ok(readings)
}

/// The record matches the chain on `url`: its `final_state` is the block in which the registry's
/// epoch turned from the record's epoch to the next (number and hash); that block's nonce carries
/// the record's epoch, it alone among its neighbours carries the 32-byte closing seed in
/// `extraData`, and its `parentBeaconBlockRoot` is the record's `final_consensus` digest; the next
/// block carries the next epoch and has the closing block as its parent; and the record's
/// committees are the registry's at the closing block.
fn check_record_on_chain(url: &str, record: &EpochRecord) -> eyre::Result<ChainBlock> {
    let epoch = record.epoch;
    let closing = closing_block_from_chain(url, epoch)?;
    eyre::ensure!(
        record.final_state.number == closing.number
            && closing.hash.eq_ignore_ascii_case(&record.final_state.hash.to_string()),
        "record {epoch}'s final_state is block {} {}, the registry's epoch turned in block {} {}",
        record.final_state.number,
        record.final_state.hash,
        closing.number,
        closing.hash
    );
    // the block that ran the closing calls carries the 32-byte closing seed, its neighbours none,
    // and it was produced by the output the record names as `final_consensus`
    let previous = chain_block(url, closing.number - 1)?;
    let next = chain_block(url, closing.number + 1)?;
    eyre::ensure!(
        closing.extra_data.len() == 32
            && previous.extra_data.is_empty()
            && next.extra_data.is_empty(),
        "extraData of blocks {}, {} (closing) and {} is {}, {} and {}: only the closing block \
         should carry the 32-byte seed",
        previous.number,
        closing.number,
        next.number,
        previous.extra_data,
        closing.extra_data,
        next.extra_data
    );
    let final_consensus = record.final_consensus.hash;
    eyre::ensure!(
        closing.consensus_digest.map(ConsensusHeaderDigest::from) == Some(final_consensus),
        "the closing block {} of epoch {epoch} was produced by consensus header {:?}, record \
         {epoch}'s final_consensus is {} {final_consensus}",
        closing.number,
        closing.consensus_digest,
        record.final_consensus.number
    );
    eyre::ensure!(
        next.parent.eq_ignore_ascii_case(&closing.hash) && next.epoch() == u64::from(epoch) + 1,
        "block {} after the closing block of epoch {epoch} has parent {} and nonce epoch {}; the \
         closing block is {} of epoch {}",
        next.number,
        next.parent,
        next.epoch(),
        closing.hash,
        closing.epoch()
    );
    for (which, read_epoch, keys) in [
        ("committee", epoch, &record.committee),
        ("next_committee", epoch + 1, &record.next_committee),
    ] {
        let on_chain = registry_call(
            url,
            ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch: read_epoch },
            closing.number,
        )?
        .iter()
        .map(|raw| {
            BlsPublicKey::from_literal_bytes(raw.as_ref())
                .map_err(|e| eyre::eyre!("registry key for epoch {read_epoch}: {e:?}"))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
        eyre::ensure!(
            on_chain == *keys,
            "record {epoch}'s {which} differs from the registry's committee of epoch {read_epoch} \
             at the closing block {}: record {keys:?}, registry {on_chain:?}",
            closing.number
        );
    }
    Ok(closing)
}

/// The block on `url` in which the registry's current epoch turned from `epoch` to `epoch + 1`:
/// located through the next epoch's first block as the registry records it, and verified by the
/// registry's current epoch at that block and the one before.
fn closing_block_from_chain(url: &str, epoch: u32) -> eyre::Result<ChainBlock> {
    let head = get_block_number(url)?;
    let next = registry_call(url, ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }, head)?;
    let number = next.blockHeight.checked_sub(1).filter(|n| *n > 0).ok_or_else(|| {
        eyre::eyre!(
            "the registry on {url} records block {} as epoch {}'s first",
            next.blockHeight,
            epoch + 1
        )
    })?;
    let before = registry_call(url, ConsensusRegistry::getCurrentEpochCall {}, number - 1)?;
    let at = registry_call(url, ConsensusRegistry::getCurrentEpochCall {}, number)?;
    eyre::ensure!(
        before == epoch && at == epoch + 1,
        "the registry on {url} reports epoch {before} at block {} and {at} at block {number}: the \
         epoch did not turn from {epoch} in block {number}",
        number - 1
    );
    let block = chain_block(url, number)?;
    eyre::ensure!(
        block.epoch() == u64::from(epoch),
        "the closing block {number} of epoch {epoch} on {url} carries nonce epoch {}",
        block.epoch()
    );
    Ok(block)
}

/// The head of [`HELD_MEMBER`] at the moment the first fast member's registry turned to epoch 1.
fn held_head_at_first_close(urls: &[String; 4]) -> eyre::Result<u64> {
    let deadline = Instant::now() + HELD_EPOCH_TIMEOUT;
    loop {
        if FAST_MEMBERS.iter().any(|&i| node_epoch(&urls[i]).is_ok_and(|e| e >= 1)) {
            return get_block_number(&urls[HELD_MEMBER]);
        }
        eyre::ensure!(
            Instant::now() < deadline,
            "no fast member entered epoch 1 within {HELD_EPOCH_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The number of consensus outputs with blocks in `(after, through]` on `url`: every block of one
/// output carries that output's nonce, and the nonce changes at every output.
fn outputs_after(url: &str, after: u64, through: u64) -> eyre::Result<usize> {
    let mut nonces = BTreeSet::new();
    for number in after + 1..=through {
        nonces.insert(chain_block(url, number)?.nonce);
    }
    Ok(nonces.len())
}

/// A block as the RPC reports it.
struct ChainBlock {
    /// The block number.
    number: u64,
    /// The block hash, hex.
    hash: String,
    /// The parent hash, hex.
    parent: String,
    /// The nonce: the committing leader's `(epoch << 32) | round`.
    nonce: u64,
    /// The extra data: the 32-byte closing seed on the block that ran the epoch-closing calls,
    /// empty on every other block.
    extra_data: Bytes,
    /// The digest of the consensus header whose output produced the block.
    consensus_digest: Option<B256>,
}

impl ChainBlock {
    /// The epoch the block's nonce carries.
    fn epoch(&self) -> u64 {
        self.nonce >> 32
    }
}

/// Block `number` on `url`.
fn chain_block(url: &str, number: u64) -> eyre::Result<ChainBlock> {
    let block = get_block(url, Some(number))?;
    let text = |name: &str| {
        block
            .get(name)
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| eyre::eyre!("block {number} on {url} has no {name}"))
    };
    let nonce = block
        .get("nonce")
        .and_then(parse_hex_u64)
        .ok_or_else(|| eyre::eyre!("block {number} on {url} has no nonce"))?;
    let extra_data = text("extraData")?
        .parse::<Bytes>()
        .map_err(|e| eyre::eyre!("block {number} on {url} has extraData that is not hex: {e}"))?;
    let consensus_digest = block
        .get("parentBeaconBlockRoot")
        .and_then(Value::as_str)
        .map(str::parse::<B256>)
        .transpose()
        .map_err(|e| eyre::eyre!("block {number} on {url}: parentBeaconBlockRoot: {e}"))?;
    Ok(ChainBlock {
        number,
        hash: text("hash")?,
        parent: text("parentHash")?,
        nonce,
        extra_data,
        consensus_digest,
    })
}

/// The number of logs the `ConsensusRegistry` has in block `number` on `url`, by `eth_getLogs`.
fn registry_logs_in_block(url: &str, number: u64) -> eyre::Result<usize> {
    let tag = format!("0x{number:x}");
    let filter = json!({ "address": CONSENSUS_REGISTRY_ADDRESS, "fromBlock": tag, "toBlock": tag });
    let logs: Vec<Value> = call_rpc(url, "eth_getLogs", rpc_params![filter], 3, &tag)?;
    Ok(logs.len())
}

/// Call the `ConsensusRegistry` on `url` with `call` against the state after block `block`.
fn registry_call<C: SolCall>(url: &str, call: C, block: u64) -> eyre::Result<C::Return> {
    let tag = format!("0x{block:x}");
    let request =
        json!({ "to": CONSENSUS_REGISTRY_ADDRESS, "data": Bytes::from(call.abi_encode()) });
    let raw: Bytes = call_rpc(url, "eth_call", rpc_params![request, tag.clone()], 3, &tag)?;
    C::abi_decode_returns(&raw)
        .map_err(|e| eyre::eyre!("{} at block {block} on {url}: {e}", C::SIGNATURE))
}

/// The certified record for `epoch` that `url` serves; an error while it has none.
fn certified_record(url: &str, epoch: u32) -> eyre::Result<(EpochRecord, EpochCertificate)> {
    call_rpc(url, "tn_epochRecord", rpc_params![epoch], 0, epoch)
}

/// The committee indexes of a certificate's signers.
fn signers(cert: &EpochCertificate) -> Vec<u32> {
    cert.signed_authorities.iter().collect()
}

/// The BLS public key node `url` reports through `tn_info`.
fn node_bls_key(url: &str) -> eyre::Result<BlsPublicKey> {
    let info = get_node_info(url)?;
    let key = info
        .get("bls_public_key")
        .cloned()
        .ok_or_else(|| eyre::eyre!("tn_info on {url} has no bls_public_key: {info:?}"))?;
    Ok(serde_json::from_value(key)?)
}

/// Node `node`'s [`PUBLISHED_LOG`] lines for `epoch` in run `run`.
fn published_lines(test: &str, node: usize, run: u32, epoch: u32) -> eyre::Result<Vec<String>> {
    Ok(node_log_lines(test, node, run)?
        .into_iter()
        .filter(|line| {
            line.contains(PUBLISHED_LOG) && log_field(line, "epoch") == Some(epoch.into())
        })
        .collect())
}

/// Node `node`'s [`OWN_VOTE_LOG`] lines for `epoch` in run `run`, without the collector's re-votes
/// ([`REVOTE_LOG`]).
fn own_vote_lines(test: &str, node: usize, run: u32, epoch: u32) -> eyre::Result<Vec<String>> {
    let own_vote = format!("{OWN_VOTE_LOG} {epoch} ");
    Ok(node_log_lines(test, node, run)?
        .into_iter()
        .filter(|line| line.contains(&own_vote) && !line.contains(REVOTE_LOG))
        .collect())
}

/// The registry's current epoch snapshot on `url`.
fn epoch_snapshot(url: &str) -> eyre::Result<EpochSnapshot> {
    let provider = ProviderBuilder::new().connect_http(url.parse()?);
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(current_epoch(&provider))
}
