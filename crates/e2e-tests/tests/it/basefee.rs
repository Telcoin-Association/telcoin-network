//! E2E tests for per-worker EIP-1559 base fees driven by the on-chain `WorkerConfigs` contract.
//!
//! Worker 0's fee strategy lives in `WorkerConfigs` and prices the NEXT epoch at each boundary:
//! - `Eip1559 { target_gas }` nudges the fee toward `target_gas` (+/-12.5% per epoch, floored at
//!   [`MIN_PROTOCOL_BASE_FEE`]).
//! - `Static { fee }` pins the fee to `fee`.
//!
//! ## Where the fee lives between epochs
//!
//! The epoch-closing block computes each `Eip1559` worker's next-epoch fee and WRITES it into that
//! worker's `WorkerConfigs` `data` word, as one of that block's own system calls. Entering the next
//! epoch, every node READS the word back from exactly that block — one state read pinned to the
//! previous epoch's closing block (`read_base_fees_for_entered_epoch` in `tn-node`), not a scan of
//! the closed epoch's headers. `Static` rows are never written: their fee already is the config's
//! own value.
//!
//! So the fee for epoch N sits in the state of epoch N-1's closing block, before any block of epoch
//! N exists. A node whose tip IS that closing block — killed at the boundary, or restored from a
//! snapshot taken there — prices epoch N from what it already holds: no peer, no header scan,
//! nothing carried in memory across the crash.
//!
//! Genesis default is `Eip1559 { target_gas: u64::MAX }`, which is inert (keeps every worker at
//! `MIN_PROTOCOL_BASE_FEE` forever). To observe movement, these tests set a custom strategy at
//! genesis via `--worker-fee-config` (the
//! [`config_local_testnet_with_worker_fee_configs`](e2e_tests::config_local_testnet_with_worker_fee_configs)
//! helper).
//!
//! ## How the fee shows up on chain (learned empirically; drives the test design)
//!
//! - Epoch 0 always uses `MIN_PROTOCOL_BASE_FEE`: it has no previous epoch, so there is no closing
//!   block to read a fee from. Epoch N's fee is the one epoch N-1's closing block recorded, and it
//!   applies to blocks produced *inside* epoch N.
//! - These testnets run with skip-empty-execution: blocks are produced only when transactions exist
//!   or an epoch closes. The epoch-boundary/close blocks are produced by the *closing* producer and
//!   carry the chain-seeded (previous-epoch) fee — they do **not** reflect the fee they record for
//!   the next epoch. That new fee first appears on the next **transaction-bearing block inside the
//!   new epoch**, and only there: an empty epoch-closing block copies its parent's fee verbatim
//!   (`crates/engine/src/payload_builder.rs:124`), so an epoch that stays idle closes on the
//!   previous fee rather than the one it recorded.
//! - The testnet is single-worker (worker 0), so every block's `base_fee_per_gas` is worker 0's
//!   fee. A submitted transaction must carry a `gas_price >= base_fee` or the pool treats it as
//!   underpriced and it never lands — for the static-fee tests we price transactions above the
//!   static fee on purpose.
//!
//! The deterministic assertion every test makes is therefore: *a block committed inside epoch
//! ≥ 1 carries exactly the fee the previous epoch's closing block recorded.*
//!
//! ## Coverage
//!
//! - [`test_static_fee_applied_at_epoch_boundary`] and
//!   [`test_eip1559_fee_rises_at_epoch_boundaries`]: both strategies live, across boundaries.
//! - [`test_mid_epoch_restart_recovers_static_fee`]: restart inside an epoch (ordinary catch-up).
//! - [`test_boundary_kill_restart_recovers_next_epoch_fee`]: kill at the exact epoch increment —
//!   the case the on-chain record exists to make survivable.

use std::{path::Path, time::Duration};

use alloy::providers::{Provider, ProviderBuilder};
use tn_types::{
    gas_accumulator::compute_next_base_fee_eip1559, get_available_tcp_port, Address,
    MIN_PROTOCOL_BASE_FEE,
};
use tokio::time::Instant;
use tracing::info;

use crate::common::{
    address_from_word, current_epoch, force_kill_and_reap, get_balance, get_block_number, get_key,
    get_latest_consensus_header_number, get_tx_receipt_block, kill_child, network_advancing,
    read_base_fee, send_tel, send_term, start_validator, wait_for_epoch_at_least,
    wait_for_mid_epoch, wait_for_rpc, EpochSnapshot, ProcessGuard,
};

/// Epoch duration (seconds) for the base-fee tests. 5s is the consensus minimum epoch duration;
/// halving it from 10s (the same decoupled cut #897 applied to `epochs.rs`) roughly halves the
/// epoch-boundary portion of each test without changing any assertion or epoch/iteration count.
/// Every per-epoch wait scales with this value, but the restarted-node catch-up deadlines in
/// [`test_mid_epoch_restart_recovers_static_fee`] are floored to an absolute minimum (`.max(..)`)
/// rather than shrinking with it, because state-sync catch-up is a fixed cost independent of the
/// epoch cadence.
const EPOCH_DURATION: u64 = 5;

/// Static fee used by the deterministic tests. A clearly non-`MIN` value so any block produced
/// under the static strategy is unmistakable.
const STATIC_FEE: u64 = 1_000_000;

/// A gas price comfortably above [`STATIC_FEE`] so the gas-generating transactions are never
/// rejected as underpriced when the static strategy is active.
const HIGH_GAS_PRICE: u128 = 2_000_000;

/// Gas price for the gas-generating transactions of the EIP-1559 tests. Those fees stay tiny
/// (single/low double digits) across the handful of boundaries any one test crosses, so 250 sits
/// far above the base fee without pinning the tests to a particular fee value.
const CHEAP_GAS_PRICE: u128 = 250;

/// BLS passphrase used by [`start_validator`] (see `common.rs`).
const NODE_PASSWORD: &str = "restart_test";

/// Number of validators in the testnet (single worker each: worker 0).
const NUM_VALIDATORS: usize = 4;

/// Dev-funded account written into genesis by the harness (`--dev-funded-account test-source`).
/// Funded with one billion TEL; the sender for every gas-generating transfer.
const FUNDED_ACCOUNT: &str = "test-source";

/// Amount (wei) transferred by each gas-generating transaction: 0.001 TEL.
const TRANSFER_AMOUNT: u128 = 1_000_000_000_000_000;

/// Epoch duration (seconds) for [`test_boundary_kill_restart_recovers_next_epoch_fee`], and the
/// only place this module departs from [`EPOCH_DURATION`].
///
/// That test has to fit a whole sequence inside ONE epoch: reap the node killed at the boundary
/// (SIGTERM plus [`fast_kill`]'s 300ms poll-for-exit; the node's real shutdown takes ~2.3s), then
/// submit and confirm the new epoch's first transaction (~1-2s). At the 5s cadence that sequence
/// can spill past the next boundary and land the control transaction in the epoch AFTER the one
/// the oracle prices, where the fee has moved again: a timing flake that would read as a fee
/// mismatch. 8s keeps ~3s of worst-case slack for the sequence, at one wall-clock second saved per
/// boundary the test crosses; [`fast_kill`] exists so the reap costs its true ~2.3s instead of
/// [`kill_child`]'s 1.2s-quantized poll (up to 3.6s), which is where that slack comes from.
const BOUNDARY_EPOCH_DURATION: u64 = 8;

/// Consecutive epochs [`test_boundary_kill_restart_recovers_next_epoch_fee`] lands one transaction
/// in before the boundary kill.
///
/// Each such epoch raises worker 0's fee by one EIP-1559 step (+12.5%, minimum +1 against
/// `target_gas = 1`), so two of them lift it from [`MIN_PROTOCOL_BASE_FEE`] (7) to 8 and make the
/// expected next-epoch fee 9. That is enough separation: at every assertion, 9 is distinct from a
/// node which defaulted to MIN (7) and from one which kept the closed epoch's value (8), so
/// neither failure mode can satisfy the assertions by coincidence. The decay/readmission
/// arithmetic in steps 3-4 also stays exact on this band (`decay(step(x)) = x` for every fee in
/// 7..12, because each step is the +1 minimum and decay is its -1 mirror above MIN).
const BOUNDARY_WARMUP_EPOCHS: usize = 2;

// ---------------------------------------------------------------------------------------------
// Test 2 (written first because it is the robust, deterministic core): Static fee at boundary.
// ---------------------------------------------------------------------------------------------

/// A static per-worker fee configured at genesis must be applied to worker 0 starting in epoch 1.
///
/// Deterministic: genesis sets worker 0 = `Static { fee: STATIC_FEE }`. Genesis/epoch-0 blocks
/// carry `MIN_PROTOCOL_BASE_FEE`. After the network enters epoch 1, a transaction priced above the
/// static fee is submitted and confirmed; the block it produces must carry exactly `STATIC_FEE`.
/// Crossing a further boundary and confirming another transaction proves the static fee is re-read
/// every epoch and does not drift.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
async fn test_static_fee_applied_at_epoch_boundary() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "basefee-test", "test_static_fee_applied_at_epoch_boundary");

    let tmp_guard = tempfile::TempDir::with_prefix("basefee_static").expect("tempdir is okay");
    let temp_path = tmp_guard.path();

    // worker 0 = Static (strategy 1) with a fixed fee.
    e2e_tests::config_local_testnet_with_worker_fee_configs(
        temp_path,
        Some(NODE_PASSWORD.to_string()),
        None,
        Some(EPOCH_DURATION as u32),
        &[&format!("0:1:{STATIC_FEE}")],
    )
    .expect("failed to config");

    let (mut guard, client_urls) = start_testnet(temp_path, "basefee_static");

    // Genesis (block 0) is always MIN.
    let genesis_fee = read_base_fee(&client_urls[0], 0)?;
    assert_eq!(
        genesis_fee, MIN_PROTOCOL_BASE_FEE,
        "genesis block base fee must be MIN, got {genesis_fee}"
    );

    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    wait_for_rpc(&provider).await?;
    let funded_key = get_key(FUNDED_ACCOUNT);

    // Enter epoch 1, then land a priced transaction inside it and read its block's fee.
    let epoch1 = wait_for_epoch_at_least(&provider, 1).await?;
    info!(target: "basefee-test", epoch = epoch1.epoch_id, "reached epoch >= 1");
    let to1 = address_from_word("basefee-static-target-1");
    let (block1, fee1) = land_priced_tx_mid_epoch(&client_urls[0], &funded_key, to1, 0).await?;
    assert_eq!(
        fee1, STATIC_FEE,
        "tx block {block1} in epoch {} must carry the static fee {STATIC_FEE}, got {fee1}",
        epoch1.epoch_id
    );

    // Cross another boundary and confirm the static fee still holds.
    let epoch2 = wait_for_epoch_at_least(&provider, epoch1.epoch_id + 1).await?;
    info!(target: "basefee-test", epoch = epoch2.epoch_id, "reached next epoch");
    let to2 = address_from_word("basefee-static-target-2");
    let (block2, fee2) = land_priced_tx_mid_epoch(&client_urls[0], &funded_key, to2, 1).await?;
    assert_eq!(
        fee2, STATIC_FEE,
        "tx block {block2} in epoch {} must still carry the static fee {STATIC_FEE}, got {fee2}",
        epoch2.epoch_id
    );

    guard.kill_all();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Test 1: EIP-1559 fee rises across epoch boundaries when gas exceeds the target.
// ---------------------------------------------------------------------------------------------

/// With worker 0 = `Eip1559 { target_gas: 1 }` and real gas every epoch, the base fee starts at
/// `MIN` and rises (monotonic non-decreasing, strictly above `MIN` once gas lands) across
/// boundaries.
///
/// Each epoch a transaction is confirmed (generating gas) and the resulting block's fee is
/// recorded. Against `target_gas = 1`, any gas in an epoch forces a +12.5% (min +1) increase at
/// its boundary. The fee starts at `MIN` (7) and rises slowly, so a gas price of 250 stays far
/// above it for the handful of boundaries crossed here.
///
/// This test depends on transactions confirming in specific epochs, which is more timing-sensitive
/// than the static tests under parallel CI load. A tx that misses its confirmation deadline fails
/// the test immediately: a skipped (empty) epoch would *decrease* the fee and poison the
/// monotonic assertion, so every recorded epoch carries exactly one tx by construction. The
/// deterministic `Static` tests remain the robust core if this one proves flaky.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
async fn test_eip1559_fee_rises_at_epoch_boundaries() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "basefee-test", "test_eip1559_fee_rises_at_epoch_boundaries");

    let tmp_guard = tempfile::TempDir::with_prefix("basefee_eip1559").expect("tempdir is okay");
    let temp_path = tmp_guard.path();

    // worker 0 = Eip1559 (strategy 0) with a tiny target so any real gas exceeds it and pushes the
    // fee up ~12.5% per epoch (never below MIN).
    e2e_tests::config_local_testnet_with_worker_fee_configs(
        temp_path,
        Some(NODE_PASSWORD.to_string()),
        None,
        Some(EPOCH_DURATION as u32),
        &["0:0:1"],
    )
    .expect("failed to config");

    let (mut guard, client_urls) = start_testnet(temp_path, "basefee_eip1559");

    // Genesis is always MIN.
    let genesis_fee = read_base_fee(&client_urls[0], 0)?;
    assert_eq!(
        genesis_fee, MIN_PROTOCOL_BASE_FEE,
        "genesis block base fee must be MIN, got {genesis_fee}"
    );

    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    wait_for_rpc(&provider).await?;

    // The funded `test-source` account sends every gas-generating transfer; its nonce must be
    // monotonic across the whole test (the recipient is fixed; each tx uses the next nonce).
    let funded_key = get_key(FUNDED_ACCOUNT);
    let to = address_from_word("basefee-eip1559-target");

    // Land one tx per epoch (driving the next boundary's increase) and record the fee of the
    // tx-bearing block. Start in epoch 1 (epoch 0 is MIN by definition).
    let mut fees: Vec<(u32, u64, u64)> = Vec::new(); // (epoch, block, fee)

    let mut current = wait_for_epoch_at_least(&provider, 1).await?;
    let target_boundaries = 3u32;
    for i in 0..=target_boundaries {
        let nonce = i as u128;
        // Cheap gas price is fine: the EIP-1559 fee stays tiny (single/low double digits) across
        // these few epochs. A tx that misses its deadline is FATAL: skipping an epoch lets empty
        // 5s epochs pass (each *decreasing* the fee against `target_gas = 1`), which would
        // poison the monotonic assertion below. Every recorded epoch must land exactly one tx.
        let (block, fee) = land_cheap_tx_mid_epoch(&client_urls[0], &funded_key, to, nonce)
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "epoch {}: gas-generating tx (nonce {nonce}) missed its {}s confirmation deadline: {e}. Check test_logs/basefee_eip1559/",
                    current.epoch_id,
                    EPOCH_DURATION * 2 + 5
                )
            })?;
        info!(target: "basefee-test", epoch = current.epoch_id, block, fee, "recorded epoch fee");
        fees.push((current.epoch_id, block, fee));

        if i < target_boundaries {
            current = wait_for_epoch_at_least(&provider, current.epoch_id + 1).await?;
        }
    }

    info!(target: "basefee-test", ?fees, "collected per-epoch tx-block fees");
    assert!(
        fees.len() >= 2,
        "needed at least two epochs with a confirmed tx to compare fees; got {fees:?}. \
         Check test_logs/basefee_eip1559/."
    );

    // Assertion 1: monotonic non-decreasing, never below the protocol floor (smoke checks), and
    // the EXACT oracle step for consecutive-epoch records.
    for window in fees.windows(2) {
        let (pe, pb, pf) = window[0];
        let (ne, nb, nf) = window[1];
        assert!(
            nf >= pf,
            "base fee decreased: epoch {pe} block {pb} fee {pf} -> epoch {ne} block {nb} fee {nf}; series {fees:?}"
        );
        assert!(nf >= MIN_PROTOCOL_BASE_FEE, "fee below MIN at epoch {ne}: {nf}");

        // Exact-step check: when two records are exactly one epoch apart, epoch `pe` carried the
        // one 21k-gas tx landed above (hard-fail guarantees it), so epoch `ne`'s fee must equal
        // the tn-types oracle output precisely. A wrong denominator, a double-applied adjustment,
        // or a fee applied one epoch late all satisfy the inequalities but not this. (With
        // `target_gas = 1` the oracle clamps `gas_used` to the 2-gas elasticity bound, so the
        // expected step is insensitive to any extra gas that lands in the epoch.)
        // `wait_for_epoch_at_least` can overshoot boundaries between records, so non-consecutive
        // pairs are covered only by the inequalities above.
        if ne == pe + 1 {
            let expected = compute_next_base_fee_eip1559(pf, 21_000, 1);
            assert_eq!(
                nf, expected,
                "exact EIP-1559 step violated: epoch {pe} fee {pf} (21k gas against target 1) \
                 must yield {expected} in epoch {ne}, got {nf}; series {fees:?}"
            );
        }
    }

    // Assertion 2: by the last recorded epoch the fee strictly exceeds MIN (gas drove it up).
    let (last_epoch, _last_block, last_fee) = *fees.last().expect("non-empty");
    assert!(
        last_fee > MIN_PROTOCOL_BASE_FEE,
        "EIP-1559 fee never rose above MIN ({MIN_PROTOCOL_BASE_FEE}) by epoch {last_epoch}; series {fees:?}. \
         Check test_logs/basefee_eip1559/."
    );

    guard.kill_all();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Test 3: mid-epoch restart recovers a non-MIN fee from the chain.
// ---------------------------------------------------------------------------------------------

/// A committee node killed MID-epoch (well away from the boundary) and restarted must resume
/// accepting blocks at the on-chain static fee, proving the epoch-entry seeding re-reads the base
/// fee from the previous epoch's closing-block state on restart.
///
/// Killing mid-epoch exercises the ordinary catch-up path: the node comes back inside an epoch
/// whose fee is already settled, with blocks to sync before it can produce again. The boundary case
/// — a node whose tip IS the closing block, killed before any block of the new epoch reaches it —
/// is the harder one and is covered separately by
/// [`test_boundary_kill_restart_recovers_next_epoch_fee`]. It used to be a genuine gap: the
/// next-epoch fee computed at close lived only in the closing producer's memory until the first
/// next-epoch block existed. The closing block now records that fee on chain, so both restart
/// shapes read the same value out of the same pinned state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
async fn test_mid_epoch_restart_recovers_static_fee() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "basefee-test", "test_mid_epoch_restart_recovers_static_fee");

    let tmp_guard = tempfile::TempDir::with_prefix("basefee_restart").expect("tempdir is okay");
    let temp_path = tmp_guard.path();

    e2e_tests::config_local_testnet_with_worker_fee_configs(
        temp_path,
        Some(NODE_PASSWORD.to_string()),
        None,
        Some(EPOCH_DURATION as u32),
        &[&format!("0:1:{STATIC_FEE}")],
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; NUM_VALIDATORS] = std::array::from_fn(|_| String::new());
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("ephemeral rpc port for validator");
        *url = format!("http://127.0.0.1:{rpc_port}");
        guard.push(start_validator(i, bin, temp_path, rpc_port, "basefee_restart", 0));
    }

    network_advancing(&client_urls)?;

    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    wait_for_rpc(&provider).await?;
    let funded_key = get_key(FUNDED_ACCOUNT);

    // Advance into epoch >= 1, then land a priced tx so a block carrying the static fee is on
    // chain. This is the value the restarted node must recover.
    let epoch1 = wait_for_epoch_at_least(&provider, 1).await?;
    let seed_to = address_from_word("basefee-restart-seed");
    let (static_block, on_chain_fee) =
        land_priced_tx_mid_epoch(&client_urls[0], &funded_key, seed_to, 0).await?;
    assert_eq!(
        on_chain_fee, STATIC_FEE,
        "expected static fee {STATIC_FEE} on chain (block {static_block}) in epoch {}, got {on_chain_fee}",
        epoch1.epoch_id
    );

    // Step into a fresh epoch, then position the kill MID-epoch by MEASURED phase (host clock vs
    // the boundary block's timestamp), retrying until inside a safe window. A blind
    // half-epoch sleep followed by a hard assert would fail under scheduling/RPC drift before
    // the restart under test even happens.
    wait_for_epoch_at_least(&provider, epoch1.epoch_id + 1).await?;
    let kill_epoch = wait_for_mid_epoch(&provider, &client_urls[0]).await?;
    info!(target: "basefee-test", epoch = kill_epoch.epoch_id, "killing validator-3 mid-epoch");

    // Kill validator index 2 (validator-3).
    let kill_idx = 2usize;
    if let Some(mut taken) = guard.take(kill_idx) {
        kill_child(&mut taken);
    }
    let killed_provider = ProviderBuilder::new().connect_http(client_urls[kill_idx].parse()?);
    assert!(
        killed_provider.get_chain_id().await.is_err(),
        "validator-{} should be down after kill",
        kill_idx + 1
    );

    // Let the rest of the network advance while the node is down. Event-driven (see
    // `wait_out_downtime`) instead of a fixed 2s sleep: hold a 2s downtime floor so the restart
    // genuinely exercises catch-up, then proceed as soon as a live peer's consensus chain moves.
    wait_out_downtime(&client_urls, kill_idx, 2).await?;

    // Restart the killed node on a fresh RPC port and re-register it.
    let new_rpc_port =
        get_available_tcp_port("127.0.0.1").expect("ephemeral rpc port for restarted validator");
    client_urls[kill_idx] = format!("http://127.0.0.1:{new_rpc_port}");
    let restarted = start_validator(kill_idx, bin, temp_path, new_rpc_port, "basefee_restart", 1);
    guard.replace(kill_idx, restarted);
    let restarted_provider = ProviderBuilder::new().connect_http(client_urls[kill_idx].parse()?);
    wait_for_rpc(&restarted_provider).await?;

    // 1) The restarted node must serve the historical static-fee block with the correct fee,
    //    proving it caught up and stored the on-chain value (not MIN).
    // Catch-up is a fixed state-sync cost (the node was down only ~2s), so floor this deadline at
    // 60s rather than letting it shrink with EPOCH_DURATION.
    let recovered_fee =
        wait_for_block_fee(&client_urls[kill_idx], static_block, (EPOCH_DURATION * 6).max(60))?
            .ok_or_else(|| {
                eyre::eyre!(
                    "restarted validator-{} did not catch up to block {static_block} within {}s. Check test_logs/basefee_restart/",
                    kill_idx + 1,
                    (EPOCH_DURATION * 6).max(60)
                )
            })?;
    assert_eq!(
        recovered_fee, STATIC_FEE,
        "restarted validator-{} serves block {static_block} with fee {recovered_fee}, expected static {STATIC_FEE}",
        kill_idx + 1
    );

    // 2) Liveness + recovery proof: a NEW priced tx submitted after the restart must confirm and
    //    its block must carry the static fee. A node that reset to MIN would mis-price/reject it.
    let after_to = address_from_word("basefee-restart-after");
    let (after_block, after_fee) =
        land_tx_and_read_fee(&client_urls[0], &funded_key, after_to, 1, HIGH_GAS_PRICE).await?;
    assert_eq!(
        after_fee, STATIC_FEE,
        "post-restart tx block {after_block} carried fee {after_fee}, expected static {STATIC_FEE}"
    );

    // The restarted node must also serve that post-restart block at the same fee. Not reaching
    // the block within the budget is a hard failure — silently skipping the assert would let a
    // restarted node that never catches up pass the test.
    // Same fixed catch-up cost as above: floor at 40s instead of scaling with EPOCH_DURATION.
    let f = wait_for_block_fee(&client_urls[kill_idx], after_block, (EPOCH_DURATION * 4).max(40))?
        .ok_or_else(|| {
            eyre::eyre!(
                "restarted validator-{} did not reach post-restart block {after_block} within {}s. Check test_logs/basefee_restart/",
                kill_idx + 1,
                (EPOCH_DURATION * 4).max(40)
            )
        })?;
    assert_eq!(
        f,
        STATIC_FEE,
        "restarted validator-{} block {after_block} fee {f}, expected static {STATIC_FEE}",
        kill_idx + 1
    );

    // 3) The regression this test exists for: the restarted node's LOCAL fee state. Blocks served
    //    above could come from pure state sync (headers reproduce `base_fee_per_gas` regardless of
    //    the local `BaseFeeContainer`), and the healthy 3-node quorum certifies txs submitted via
    //    validator-1 even if the restarted node recovered MIN. Routing a priced tx through the
    //    restarted node's OWN RPC exercises its local pool/batch path: a node that recovered MIN
    //    instead of the on-chain static fee would misprice the tx and diverge on its batch path.
    let local_to = address_from_word("basefee-restart-local");
    let (local_block, local_fee) =
        land_tx_and_read_fee(&client_urls[kill_idx], &funded_key, local_to, 2, HIGH_GAS_PRICE)
            .await?;
    assert_eq!(
        local_fee,
        STATIC_FEE,
        "tx routed through restarted validator-{}'s own RPC landed in block {local_block} with \
         fee {local_fee}, expected static {STATIC_FEE}",
        kill_idx + 1
    );

    guard.kill_all();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Test 4: a node killed at the exact epoch increment recovers the next epoch's recorded fee.
// ---------------------------------------------------------------------------------------------

/// A committee node killed at the EXACT epoch increment — its tip IS the epoch-closing block, and
/// no block of the new epoch has reached it — must come back pricing the new epoch at the fee that
/// closing block recorded on chain.
///
/// This is the case [`test_mid_epoch_restart_recovers_static_fee`] stayed away from while the
/// next-epoch fee existed only in the closing producer's memory. It is now the architecture's
/// strongest point: the closing block's own system call writes every `Eip1559` worker's next-epoch
/// fee into `WorkerConfigs`, and epoch entry reads that word back from exactly this block, so a
/// node whose tip is the closing block already holds everything it needs to price the epoch it is
/// entering.
///
/// # Kill timing achieved
///
/// The kill is driven off the TARGET node's own RPC, never a peer's: the test polls
/// `getCurrentEpochInfo()` on validator-3 (through [`wait_for_epoch_at_least`], ~4x/sec) and
/// signals on the FIRST observation of `epochId == E + 1`. That registry word flips only once the
/// node has itself executed epoch E's closing block, so the signal is strictly after the close.
/// Nothing can move the node's tip during the poll's ~250ms slop: under skip-empty-execution the
/// next block appears only when a transaction lands or the NEXT epoch closes, this test submits
/// nothing until the kill returns, and the next boundary is a full epoch away. The test pins that
/// rather than trusting it — the node's head is read one RPC call before the signal and asserted
/// equal to `getEpochInfo(E + 1).blockHeight - 1`, the closing block.
///
/// The shutdown is the module's ordinary [`kill_child`] (SIGTERM, then reap), so the node lives on
/// for a moment after the signal; it cannot execute a new-epoch block in that moment for the same
/// reason nothing else can — none exists to execute. A shutdown slow enough to span the whole next
/// epoch would leave the node killed at the FOLLOWING boundary instead, still a boundary kill, and
/// every assertion below is anchored to explicit block numbers rather than to "the last block it
/// saw", so that would surface as a mismatch rather than a silent pass.
///
/// # Expected-fee oracle
///
/// Computed here, independently of any node: epoch E carries one transaction, so
/// `expected = compute_next_base_fee_eip1559(fee_E, 21_000, 1)`, the same tn-types formula
/// [`test_eip1559_fee_rises_at_epoch_boundaries`] uses. `target_gas = 1` clamps `gas_used` to the
/// 2-gas elasticity bound, so the step is +12.5% (minimum +1) for ANY non-zero gas and the exact
/// amount that landed in epoch E cannot change it. The test asserts `expected` is strictly above
/// both [`MIN_PROTOCOL_BASE_FEE`] and epoch E's own fee, so neither a node that defaulted to MIN
/// nor one that kept the previous epoch's value can pass by coincidence.
///
/// # What the restart must show
///
/// 1. Control, through a live peer: the first block of epoch E+1 — produced by a transaction priced
///    at exactly `expected`, submitted the instant the kill returns — carries `expected`. The
///    network kept going and prices new-epoch transactions at the recorded fee.
/// 2. The restarted node serves that same block at that same fee once it has caught up.
/// 3. A transaction priced at exactly `expected` and routed through the RESTARTED node's own RPC is
///    accepted and mined. This is the assertion that reaches the node's LOCAL fee state: blocks it
///    serves could come from pure header sync, and the healthy 3-node quorum certifies transactions
///    submitted elsewhere regardless, but a batch its own worker builds off a wrong local fee is
///    rejected by its peers and the transaction never lands. Step 3 waits for epoch E+3 first:
///    epoch E+1 carried the control transaction, so E+2's fee sits one step ABOVE `expected` and
///    that price would be underpriced there, while from E+3 on the idle decay puts the fee back at
///    or below `expected` (`decay(step(x)) <= x`).
/// 4. The recovered node prices the NEXT boundary correctly too: with the epoch-G transaction from
///    step 3 as the anchor, a transaction landed through the same RPC in epoch G+1 must carry
///    exactly one EIP-1559 step more. A node serving correct headers while holding a stale or
///    defaulted local fee produces a batch nobody accepts, and never gets here.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
async fn test_boundary_kill_restart_recovers_next_epoch_fee() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "basefee-test", "test_boundary_kill_restart_recovers_next_epoch_fee");

    let tmp_guard = tempfile::TempDir::with_prefix("basefee_boundary").expect("tempdir is okay");
    let temp_path = tmp_guard.path();

    // worker 0 = Eip1559 (strategy 0) with the same tiny target as
    // `test_eip1559_fee_rises_at_epoch_boundaries`, so the fee MOVES at every boundary that saw gas
    // and the oracle below is insensitive to how much gas actually landed.
    e2e_tests::config_local_testnet_with_worker_fee_configs(
        temp_path,
        Some(NODE_PASSWORD.to_string()),
        None,
        Some(BOUNDARY_EPOCH_DURATION as u32),
        &["0:0:1"],
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();
    let (mut guard, mut client_urls) = start_testnet(temp_path, "basefee_boundary");

    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    wait_for_rpc(&provider).await?;
    // `test-source` sends every transfer in this test, so its nonce must be monotonic throughout.
    let funded_key = get_key(FUNDED_ACCOUNT);
    let mut nonce: u128 = 0;

    // Warm-up: land one transaction per epoch so worker 0's fee climbs clear of MIN. The LAST of
    // these epochs is E, the epoch whose close prices the epoch the killed node must recover.
    let mut gas_epoch: Option<(EpochSnapshot, u64, u64)> = None;
    for i in 0..BOUNDARY_WARMUP_EPOCHS {
        let target = gas_epoch.map_or(1, |(snap, _, _)| snap.epoch_id + 1);
        wait_for_epoch_at_least(&provider, target).await?;
        // Land on a MEASURED mid-epoch phase so the transaction clears both boundaries and the
        // epoch credited with its gas is exactly the one recorded here.
        let snap = wait_for_mid_epoch(&provider, &client_urls[0]).await?;
        let to = address_from_word(&format!("basefee-boundary-gas-{i}"));
        let (block, fee) =
            land_tx_and_read_fee(&client_urls[0], &funded_key, to, nonce, CHEAP_GAS_PRICE)
                .await
                .map_err(|e| {
                    eyre::eyre!(
                        "epoch {}: warm-up tx (nonce {nonce}) missed its {}s confirmation \
                         deadline: {e}. Check test_logs/basefee_boundary/",
                        snap.epoch_id,
                        EPOCH_DURATION * 2 + 5
                    )
                })?;
        nonce += 1;
        info!(target: "basefee-test", epoch = snap.epoch_id, block, fee, "warm-up epoch fee");
        gas_epoch = Some((snap, block, fee));
    }
    let (closing_snap, gas_block, fee_e) =
        gas_epoch.expect("BOUNDARY_WARMUP_EPOCHS is non-zero, so the loop recorded an epoch");

    // The oracle. Epoch E carried gas, so the fee for E+1 is one EIP-1559 step up from E's.
    let expected = compute_next_base_fee_eip1559(fee_e, 21_000, 1);
    assert!(
        expected > fee_e && expected > MIN_PROTOCOL_BASE_FEE,
        "expected next-epoch fee {expected} must exceed both epoch {}'s own fee {fee_e} and MIN \
         ({MIN_PROTOCOL_BASE_FEE}); otherwise the assertions below cannot tell a recovered fee from \
         a stale or a defaulted one",
        closing_snap.epoch_id
    );

    // ---- Kill validator-3 at the epoch increment, observed on ITS OWN registry view.
    let kill_idx = 2usize;
    let killed_provider = ProviderBuilder::new().connect_http(client_urls[kill_idx].parse()?);
    let entered = wait_for_epoch_at_least(&killed_provider, closing_snap.epoch_id + 1).await?;
    // `concludeEpoch` records the entered epoch's first block as `closing block + 1`.
    let first_new_block = entered.block_height;
    let closing_block = first_new_block.saturating_sub(1);
    // One RPC call, taken before the signal: the node's tip at the kill instant.
    let head_at_kill = get_block_number(&client_urls[kill_idx])?;
    if let Some(mut taken) = guard.take(kill_idx) {
        fast_kill(&mut taken);
    }
    info!(
        target: "basefee-test",
        epoch = entered.epoch_id, closing_block, head_at_kill, fee_e, expected,
        "killed validator-3 at the epoch increment"
    );

    assert_eq!(
        entered.epoch_id,
        closing_snap.epoch_id + 1,
        "epoch poll overshot epoch {} straight to {}; the oracle prices only the next epoch",
        closing_snap.epoch_id,
        entered.epoch_id
    );
    assert_eq!(
        head_at_kill,
        closing_block,
        "validator-{}'s tip at the kill instant was block {head_at_kill}, not epoch {}'s closing \
         block {closing_block}: this was not a boundary kill",
        kill_idx + 1,
        closing_snap.epoch_id
    );
    assert!(
        (closing_snap.block_height..first_new_block).contains(&gas_block),
        "the gas-generating tx landed in block {gas_block}, outside epoch {}'s block range \
         [{}, {first_new_block}): `expected` was priced off the wrong epoch",
        closing_snap.epoch_id,
        closing_snap.block_height
    );
    assert!(
        killed_provider.get_chain_id().await.is_err(),
        "validator-{} should be down after the kill",
        kill_idx + 1
    );

    // ---- 1) Control: the network kept going and prices the new epoch at `expected`. Pricing this
    // transaction at EXACTLY the expected fee also proves the live nodes admit that price, and it
    // is what produces the new epoch's first block.
    let control_to = address_from_word("basefee-boundary-control");
    let (control_block, control_fee) =
        land_tx_and_read_fee(&client_urls[0], &funded_key, control_to, nonce, expected as u128)
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "control tx priced at the expected fee {expected} never confirmed after the \
                     boundary kill: {e}. Check test_logs/basefee_boundary/"
                )
            })?;
    nonce += 1;
    // Prove the block sits inside epoch E+1 before comparing fees: `first_new_block` is that
    // epoch's first block, and a registry read taken right after the confirm bounds it from above
    // (either the epoch has not turned over yet, or the block predates the next epoch's first
    // block).
    let after_control = current_epoch(&provider).await?;
    assert!(
        control_block >= first_new_block
            && (after_control.epoch_id == entered.epoch_id
                || (after_control.epoch_id == entered.epoch_id + 1
                    && control_block < after_control.block_height)),
        "control tx landed in block {control_block}, not inside epoch {}'s block range (first \
         block {first_new_block}; registry now at epoch {}, first block {}): it missed the epoch \
         the oracle prices. Check test_logs/basefee_boundary/",
        entered.epoch_id,
        after_control.epoch_id,
        after_control.block_height
    );
    assert_eq!(
        control_fee, expected,
        "epoch {}'s block {control_block} carried fee {control_fee}; epoch {}'s closing block \
         recorded {expected}",
        entered.epoch_id, closing_snap.epoch_id
    );

    // ---- Restart the killed node. Hold a short downtime floor so the restart genuinely exercises
    // catch-up, then bring it back on a fresh RPC port.
    wait_out_downtime(&client_urls, kill_idx, 2).await?;
    let new_rpc_port =
        get_available_tcp_port("127.0.0.1").expect("ephemeral rpc port for restarted validator");
    client_urls[kill_idx] = format!("http://127.0.0.1:{new_rpc_port}");
    let restarted = start_validator(kill_idx, bin, temp_path, new_rpc_port, "basefee_boundary", 1);
    guard.replace(kill_idx, restarted);
    let restarted_provider = ProviderBuilder::new().connect_http(client_urls[kill_idx].parse()?);
    wait_for_rpc(&restarted_provider).await?;

    // ---- 2) The restarted node's own RPC reports `expected` for the new epoch. Catch-up is a
    // fixed state-sync cost (the node was down ~2s), so floor this deadline at 60s instead of
    // letting it scale with the epoch cadence.
    let catchup_secs = (BOUNDARY_EPOCH_DURATION * 6).max(60);
    let recovered_fee = wait_for_block_fee(&client_urls[kill_idx], first_new_block, catchup_secs)?
        .ok_or_else(|| {
            eyre::eyre!(
                "restarted validator-{} did not catch up to epoch {}'s first block \
                 {first_new_block} within {catchup_secs}s. Check test_logs/basefee_boundary/",
                kill_idx + 1,
                entered.epoch_id
            )
        })?;
    assert_eq!(
        recovered_fee,
        expected,
        "restarted validator-{} serves epoch {}'s block {first_new_block} at fee {recovered_fee}, \
         expected the recorded {expected}",
        kill_idx + 1,
        entered.epoch_id
    );

    // ---- 3) The recovered node's LOCAL fee state: route a transaction priced at exactly
    // `expected` through its own RPC. `entered` is E+1, so the wait below is for E+3, which keeps
    // that price admissible (see this test's doc comment) and doubles as proof that the restarted
    // node's own registry view is advancing.
    wait_for_epoch_at_least(&restarted_provider, entered.epoch_id + 2).await?;
    let local_snap = wait_for_mid_epoch(&restarted_provider, &client_urls[kill_idx]).await?;
    let local_to = address_from_word("basefee-boundary-local");
    let (local_block, local_fee) = land_tx_and_read_fee(
        &client_urls[kill_idx],
        &funded_key,
        local_to,
        nonce,
        expected as u128,
    )
    .await
    .map_err(|e| {
        eyre::eyre!(
            "tx priced at the recovered fee {expected} and routed through restarted validator-{}'s \
             OWN RPC never confirmed: {e}. A node that recovered the wrong local fee misprices its \
             batch and its peers reject it. Check test_logs/basefee_boundary/",
            kill_idx + 1
        )
    })?;
    nonce += 1;
    info!(
        target: "basefee-test",
        epoch = local_snap.epoch_id, local_block, local_fee,
        "landed tx through the restarted node's own RPC"
    );
    assert!(
        local_fee <= expected,
        "epoch {}'s fee {local_fee} exceeds the recovered {expected} even though the network was \
         idle since the boundary: the decay argument that makes pricing at exactly {expected} \
         admissible no longer holds",
        local_snap.epoch_id
    );

    // ---- 4) The exact step across the NEXT boundary, driven entirely through the recovered node.
    // Epoch G carried the transaction above, so epoch G+1's fee is exactly one EIP-1559 step up.
    wait_for_epoch_at_least(&restarted_provider, local_snap.epoch_id + 1).await?;
    let step_snap = wait_for_mid_epoch(&restarted_provider, &client_urls[kill_idx]).await?;
    assert_eq!(
        step_snap.epoch_id,
        local_snap.epoch_id + 1,
        "epoch poll overshot epoch {} straight to {}; the exact step only holds between \
         consecutive epochs",
        local_snap.epoch_id,
        step_snap.epoch_id
    );
    assert!(
        (local_snap.block_height..step_snap.block_height).contains(&local_block),
        "the tx routed through restarted validator-{} landed in block {local_block}, outside epoch \
         {}'s block range [{}, {}): its fee {local_fee} is not that epoch's anchor",
        kill_idx + 1,
        local_snap.epoch_id,
        local_snap.block_height,
        step_snap.block_height
    );
    let step_to = address_from_word("basefee-boundary-step");
    let (step_block, step_fee) =
        land_tx_and_read_fee(&client_urls[kill_idx], &funded_key, step_to, nonce, CHEAP_GAS_PRICE)
            .await?;
    let after_step = current_epoch(&restarted_provider).await?;
    assert!(
        step_block >= step_snap.block_height
            && (after_step.epoch_id == step_snap.epoch_id
                || (after_step.epoch_id == step_snap.epoch_id + 1
                    && step_block < after_step.block_height)),
        "the follow-up tx landed in block {step_block}, not inside epoch {}'s block range (first \
         block {}; registry now at epoch {}, first block {}): the exact step is priced for epoch \
         {} only. Check test_logs/basefee_boundary/",
        step_snap.epoch_id,
        step_snap.block_height,
        after_step.epoch_id,
        after_step.block_height,
        step_snap.epoch_id
    );
    let expected_step = compute_next_base_fee_eip1559(local_fee, 21_000, 1);
    assert_eq!(
        step_fee,
        expected_step,
        "restarted validator-{} priced epoch {} at {step_fee} (block {step_block}); epoch {} \
         carried gas at fee {local_fee}, so its closing block must have recorded {expected_step}",
        kill_idx + 1,
        step_snap.epoch_id,
        local_snap.epoch_id
    );

    guard.kill_all();
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

/// Start `NUM_VALIDATORS` validators against the genesis already written under `temp_path`.
/// Returns the guard owning the children and the per-node HTTP RPC URLs.
fn start_testnet(temp_path: &Path, test: &str) -> (ProcessGuard, [String; NUM_VALIDATORS]) {
    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; NUM_VALIDATORS] = std::array::from_fn(|_| String::new());
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port =
            get_available_tcp_port("127.0.0.1").expect("ephemeral rpc port for validator");
        *url = format!("http://127.0.0.1:{rpc_port}");
        guard.push(start_validator(i, bin, temp_path, rpc_port, test, 0));
    }

    // Wait for all nodes to begin serving RPC.
    network_advancing(&client_urls).expect("network failed to start serving RPC");

    (guard, client_urls)
}

/// Boundary-kill reap: SIGTERM, then poll for exit every 300ms (up to 6s), then SIGKILL + wait.
///
/// [`kill_child`]'s shared reap (`common.rs`'s `wait_or_kill`) polls in 1.2s slots, which
/// quantizes the node's real ~2.3s shutdown up to 3.6s. This reap runs INSIDE the one epoch that
/// must also fit the control transaction (see [`BOUNDARY_EPOCH_DURATION`]), so the finer poll buys
/// back over a second of in-epoch slack. The SIGKILL escalation grace (6s) is unchanged; only the
/// poll cadence differs, and only at this call site: every other kill keeps [`kill_child`].
fn fast_kill(child: &mut std::process::Child) {
    send_term(child);
    let exited = (0..20).any(|_| {
        std::thread::sleep(Duration::from_millis(300));
        child.try_wait().ok().flatten().is_some()
    });
    if !exited {
        force_kill_and_reap(child);
    }
}

/// Wait out a killed node's downtime, then proceed once a live peer's consensus chain has advanced.
///
/// Mirrors `restarts.rs::wait_for_downtime` (#897): `min_secs` is a hard floor (so the restart
/// genuinely exercises catch-up) and the live-peer advance is only a stall-guard against the
/// network wedging, not a substitute for the floor. The CONSENSUS header is the liveness signal
/// because it advances every round even under skip-empty-execution, unlike EL block height, which
/// moves only when a tx lands or an epoch closes. A fail-safe cap keeps a wedged network from
/// hanging here rather than surfacing the stall downstream.
async fn wait_out_downtime(
    client_urls: &[String; NUM_VALIDATORS],
    killed_idx: usize,
    min_secs: u64,
) -> eyre::Result<()> {
    let peer_header = || {
        client_urls
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != killed_idx)
            .filter_map(|(_, url)| get_latest_consensus_header_number(url).ok())
            .max()
    };
    let start_header = peer_header();
    let start = Instant::now();
    let floor = Duration::from_secs(min_secs);
    // Fail-safe cap so a genuinely stalled network surfaces downstream instead of hanging here.
    let cap = Duration::from_secs(min_secs * 2 + 30);
    loop {
        let elapsed = start.elapsed();
        let advanced = start_header.zip(peer_header()).is_some_and(|(s, now)| now > s);
        if elapsed >= floor && advanced {
            return Ok(());
        }
        if elapsed >= cap {
            info!(target: "basefee-test", ?elapsed, "downtime wait hit fail-safe cap; proceeding");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Land a transaction priced above the static fee, mid-epoch, and return the produced block's
/// `(number, base_fee)`. Waits for a MEASURED mid-epoch window first (via [`wait_for_mid_epoch`],
/// the same phase check that positions the restart kill) instead of a fixed 3s sleep, so the tx
/// lands clear of a boundary at any epoch duration.
async fn land_priced_tx_mid_epoch(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
) -> eyre::Result<(u64, u64)> {
    let provider = ProviderBuilder::new().connect_http(node.parse()?);
    wait_for_mid_epoch(&provider, node).await?;
    land_tx_and_read_fee(node, funded_key, to, nonce, HIGH_GAS_PRICE).await
}

/// Land a cheap-gas-price transaction mid-epoch (fine while the EIP-1559 fee is tiny) and return
/// the produced block's `(number, base_fee)`. Waits for a MEASURED mid-epoch window first (the same
/// [`wait_for_mid_epoch`] phase check) instead of a fixed 3s sleep, so the tx lands clear of a
/// boundary at any epoch duration.
async fn land_cheap_tx_mid_epoch(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
) -> eyre::Result<(u64, u64)> {
    let provider = ProviderBuilder::new().connect_http(node.parse()?);
    wait_for_mid_epoch(&provider, node).await?;
    land_tx_and_read_fee(node, funded_key, to, nonce, CHEAP_GAS_PRICE).await
}

/// Submit a transfer, wait for it to confirm (recipient balance grows), then read the base fee of
/// the block that ACTUALLY included the tx, taken from its receipt. Returns
/// `(block_number, base_fee)`.
async fn land_tx_and_read_fee(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
    gas_price: u128,
) -> eyre::Result<(u64, u64)> {
    let before_bal = get_balance(node, &to.to_string(), 1).unwrap_or(0);
    let tx_hash = send_tel(node, funded_key, to, TRANSFER_AMOUNT, gas_price, 21_000, nonce)?;

    // Wait for the transfer to confirm. Two epoch durations covers a tx that gets orphaned at a
    // boundary and re-injected into the next epoch. The rising balance is only the LANDING
    // signal: attribution comes from the receipt below, because the tip can move (e.g. an
    // epoch-close block) between the sub-second-granularity balance poll and a tip read, and a
    // late-landing stale tx could satisfy the balance check.
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 2 + 5);
    loop {
        let bal = get_balance(node, &to.to_string(), 1).unwrap_or(before_bal);
        if bal > before_bal {
            break;
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "transfer to {to} did not confirm within {}s",
                EPOCH_DURATION * 2 + 5
            ));
        }
        // Poll ~4x/sec: at a 5s epoch a 1s cadence is coarse relative to block confirmation.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Exact attribution: the receipt names the block that included THIS tx.
    let block = get_tx_receipt_block(node, &tx_hash)?;
    let fee = read_base_fee(node, block)?;
    Ok((block, fee))
}

/// Poll `node` for up to `max_secs` until it has produced at least `block_number`, then return
/// that block's base fee. Returns `Ok(None)` if the block never appears within the budget.
fn wait_for_block_fee(node: &str, block_number: u64, max_secs: u64) -> eyre::Result<Option<u64>> {
    let deadline = std::time::Instant::now() + Duration::from_secs(max_secs);
    loop {
        if let Ok(n) = get_block_number(node) {
            if n >= block_number {
                return read_base_fee(node, block_number).map(Some);
            }
        }
        if std::time::Instant::now() >= deadline {
            return Ok(None);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
