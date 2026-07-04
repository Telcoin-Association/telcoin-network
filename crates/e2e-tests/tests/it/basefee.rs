//! E2E tests for per-worker EIP-1559 base fees driven by the on-chain `WorkerConfigs` contract.
//!
//! The node reads worker 0's fee strategy from `WorkerConfigs` at each epoch boundary and sets the
//! NEXT epoch's base fee:
//! - `Eip1559 { target_gas }` nudges the fee toward `target_gas` (+/-12.5% per epoch, floored at
//!   [`MIN_PROTOCOL_BASE_FEE`]).
//! - `Static { fee }` pins the fee to `fee`.
//!
//! Genesis default is `Eip1559 { target_gas: u64::MAX }`, which is inert (keeps every worker at
//! `MIN_PROTOCOL_BASE_FEE` forever). To observe movement, these tests set a custom strategy at
//! genesis via `--worker-fee-config` (the new
//! [`config_local_testnet_with_worker_fee_configs`](e2e_tests::config_local_testnet_with_worker_fee_configs)
//! helper).
//!
//! ## How the fee shows up on chain (learned empirically; drives the test design)
//!
//! - Epoch 0 always uses `MIN_PROTOCOL_BASE_FEE`. The new fee for epoch N is computed at the close
//!   of epoch N-1 by the live producer and applied to blocks produced *inside* epoch N.
//! - These testnets run with skip-empty-execution: blocks are produced only when transactions exist
//!   or an epoch closes. The epoch-boundary/close blocks are produced by the *closing* producer and
//!   carry the chain-seeded (previous-epoch) fee — they do **not** reflect the new fee. The new fee
//!   first appears on a **transaction-bearing block committed inside the epoch**.
//! - The testnet is single-worker (worker 0), so every block's `base_fee_per_gas` is worker 0's
//!   fee. A submitted transaction must carry a `gas_price >= base_fee` or the pool treats it as
//!   underpriced and it never lands — for the static-fee tests we price transactions above the
//!   static fee on purpose.
//!
//! The deterministic assertion every test makes is therefore: *a transaction that confirms inside
//! epoch ≥ 1 produces a block whose `base_fee_per_gas` equals the configured fee.*

use std::{path::Path, time::Duration};

use alloy::providers::{Provider, ProviderBuilder};
use serde_json::Value;
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_types::{
    gas_accumulator::compute_next_base_fee_eip1559, get_available_tcp_port, Address,
    MIN_PROTOCOL_BASE_FEE,
};
use tokio::time::Instant;
use tracing::info;

use crate::common::{
    address_from_word, get_balance, get_block, get_block_number, get_key, kill_child,
    network_advancing, send_tel, start_validator, ProcessGuard,
};

/// Epoch duration (seconds) for base-fee tests. Mirrors `epochs.rs::EPOCH_DURATION` so boundaries
/// occur on the same cadence under the `test-utils` feature.
const EPOCH_DURATION: u64 = 10;

/// Static fee used by the deterministic tests. A clearly non-`MIN` value so any block produced
/// under the static strategy is unmistakable.
const STATIC_FEE: u64 = 1_000_000;

/// A gas price comfortably above [`STATIC_FEE`] so the gas-generating transactions are never
/// rejected as underpriced when the static strategy is active.
const HIGH_GAS_PRICE: u128 = 2_000_000;

/// BLS passphrase used by [`start_validator`] (see `common.rs`).
const NODE_PASSWORD: &str = "restart_test";

/// Number of validators in the testnet (single worker each: worker 0).
const NUM_VALIDATORS: usize = 4;

/// Dev-funded account written into genesis by the harness (`--dev-funded-account test-source`).
/// Funded with one billion TEL; the sender for every gas-generating transfer.
const FUNDED_ACCOUNT: &str = "test-source";

/// Amount (wei) transferred by each gas-generating transaction: 0.001 TEL.
const TRANSFER_AMOUNT: u128 = 1_000_000_000_000_000;

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
        // 10s epochs pass (each *decreasing* the fee against `target_gas = 1`), which would
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
/// accepting blocks at the on-chain static fee, proving recovery re-seeds the base fee from the
/// chain (`seed_base_fees_from_chain`).
///
/// Deliberately avoids killing at the exact epoch boundary: there is a known, separately-tracked
/// recovery gap there (the next-epoch fee computed at close lives only in memory until the first
/// next-epoch block exists). Killing mid-epoch exercises the supported catchup path.
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

    // Step into a fresh epoch, then sleep ~half an epoch so the kill lands MID-epoch (away from
    // both the boundary we just crossed and the next one).
    let kill_epoch = wait_for_epoch_at_least(&provider, epoch1.epoch_id + 1).await?;
    tokio::time::sleep(Duration::from_secs(EPOCH_DURATION / 2)).await;
    let mid = current_epoch(&provider).await?;
    assert_eq!(
        mid.epoch_id, kill_epoch.epoch_id,
        "expected to still be mid-epoch {} when killing, but epoch advanced to {}",
        kill_epoch.epoch_id, mid.epoch_id
    );
    info!(target: "basefee-test", epoch = mid.epoch_id, "killing validator-3 mid-epoch");

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

    // Let the rest of the network advance while the node is down.
    tokio::time::sleep(Duration::from_secs(2)).await;

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
    let recovered_fee = wait_for_block_fee(&client_urls[kill_idx], static_block, EPOCH_DURATION * 6)?
        .ok_or_else(|| {
            eyre::eyre!(
                "restarted validator-{} did not catch up to block {static_block} within {}s. Check test_logs/basefee_restart/",
                kill_idx + 1,
                EPOCH_DURATION * 6
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
    let f = wait_for_block_fee(&client_urls[kill_idx], after_block, EPOCH_DURATION * 4)?
        .ok_or_else(|| {
            eyre::eyre!(
                "restarted validator-{} did not reach post-restart block {after_block} within {}s. Check test_logs/basefee_restart/",
                kill_idx + 1,
                EPOCH_DURATION * 4
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
// Helpers
// ---------------------------------------------------------------------------------------------

/// Minimal snapshot of an epoch's identity and its first EL block.
#[derive(Debug, Clone, Copy)]
struct EpochSnapshot {
    epoch_id: u32,
    /// First EL block of the epoch (the block at which the committee became active).
    #[allow(dead_code)]
    block_height: u64,
}

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

/// Poll a provider until its RPC answers `eth_chainId`.
async fn wait_for_rpc<P: Provider>(provider: &P) -> eyre::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match provider.get_chain_id().await {
            Ok(_) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(eyre::eyre!("provider RPC never became available: {e}")),
        }
    }
}

/// Read the current epoch snapshot from the `ConsensusRegistry`.
async fn current_epoch<P: Provider>(provider: &P) -> eyre::Result<EpochSnapshot> {
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);
    let info = registry.getCurrentEpochInfo().call().await?;
    Ok(EpochSnapshot { epoch_id: info.epochId, block_height: info.blockHeight })
}

/// Poll the `ConsensusRegistry` until the current epoch id is at least `target`, returning the
/// snapshot of that epoch.
async fn wait_for_epoch_at_least<P: Provider>(
    provider: &P,
    target: u32,
) -> eyre::Result<EpochSnapshot> {
    // A boundary every `EPOCH_DURATION`s; allow generous slack for CI load.
    let deadline = Instant::now() + Duration::from_secs(EPOCH_DURATION * 4 * (target as u64 + 1));
    loop {
        let snap = current_epoch(provider).await?;
        if snap.epoch_id >= target {
            return Ok(snap);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "epoch did not reach {target} within timeout (stuck at {})",
                snap.epoch_id
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Land a transaction priced above the static fee, mid-epoch, and return the produced block's
/// `(number, base_fee)`. Sleeps briefly first so the tx lands mid-epoch rather than at a boundary.
async fn land_priced_tx_mid_epoch(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
) -> eyre::Result<(u64, u64)> {
    tokio::time::sleep(Duration::from_secs(3)).await;
    land_tx_and_read_fee(node, funded_key, to, nonce, HIGH_GAS_PRICE).await
}

/// Land a cheap-gas-price transaction mid-epoch (fine while the EIP-1559 fee is tiny) and return
/// the produced block's `(number, base_fee)`.
async fn land_cheap_tx_mid_epoch(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
) -> eyre::Result<(u64, u64)> {
    tokio::time::sleep(Duration::from_secs(3)).await;
    land_tx_and_read_fee(node, funded_key, to, nonce, 250).await
}

/// Submit a transfer, wait for it to confirm (recipient balance grows), then read the new tip
/// block's base fee. Returns `(tip_block_number, base_fee)`.
async fn land_tx_and_read_fee(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
    gas_price: u128,
) -> eyre::Result<(u64, u64)> {
    let before_bal = get_balance(node, &to.to_string(), 1).unwrap_or(0);
    send_tel(node, funded_key, to, TRANSFER_AMOUNT, gas_price, 21_000, nonce)?;

    // Wait for the transfer to confirm. Two epoch durations covers a tx that gets orphaned at a
    // boundary and re-injected into the next epoch.
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
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // The block that included the transfer is the current tip (single worker, low traffic).
    let tip = get_block_number(node)?;
    let fee = read_base_fee(node, tip)?;
    Ok((tip, fee))
}

/// Read the `baseFeePerGas` (as `u64`) of `block_number` from `node` via `eth_getBlockByNumber`.
///
/// The testnet runs a single worker (worker 0), so every block's base fee is worker 0's fee.
fn read_base_fee(node: &str, block_number: u64) -> eyre::Result<u64> {
    let block = get_block(node, Some(block_number))?;
    let raw = block
        .get("baseFeePerGas")
        .ok_or_else(|| eyre::eyre!("block {block_number} on {node} has no baseFeePerGas field"))?;
    parse_hex_u64(raw).ok_or_else(|| {
        eyre::eyre!("block {block_number} on {node} baseFeePerGas is not a hex u64: {raw:?}")
    })
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

/// Parse a JSON value that is expected to be a `0x`-prefixed hex string into a `u64`.
fn parse_hex_u64(value: &Value) -> Option<u64> {
    let s = value.as_str()?;
    let hex = s.strip_prefix("0x").unwrap_or(s);
    u64::from_str_radix(hex, 16).ok()
}
