//! E2E coverage for governance ejection (`ConsensusRegistry::burn`) of validators.
//!
//! `burn` retires the validator, confiscates its stake, and swap-and-pops it out of the stored
//! current/next/subsequent committee arrays. Four scenarios are covered:
//!
//! - [`test_validator_ejected_from_future_committee_only`]: the burned validator sits ONLY in
//!   future committees, so no running committee changes shape. This test deliberately has NO
//!   dependency on the mid-epoch-ejection epoch-record fix: a future-only ejection mutates only
//!   future committee arrays, so every committee read the epoch-record producer performs at epoch
//!   close happens after the mutation and is identical on every node — the network must keep
//!   closing epochs on today's code.
//! - [`test_validator_ejected_from_current_committee_mid_epoch`]: the burned validator sits in the
//!   LIVE committee, which shrinks mid-epoch. This is the end-to-end acceptance test for the
//!   `EpochRecord::committee_compatible` tolerance: on pre-fix code every node deterministically
//!   halts at the first epoch boundary after the burn.
//! - [`test_committee_member_restarted_mid_epoch_after_ejection`]: after the burn shrinks the live
//!   committee's stored arrays, a DIFFERENT committee member is killed and restarted inside the
//!   same epoch. This is the regression test for the epoch-entry read pinning: the restart must
//!   re-derive the epoch view from the previous epoch's closing block (not the post-burn canonical
//!   tip), rejoin consensus mid-epoch, and close the epoch with records identical to the rest of
//!   the fleet.
//! - [`test_network_halts_when_ejections_shrink_committee_below_tolerance`]: governance burns TWO
//!   of the five committee members in one epoch, shrinking the committee below the
//!   `committee_compatible` tolerance. The tolerance is deliberately bounded, so the network
//!   answers with a designed fail-stop: every validator exits with code 1 at the next epoch
//!   boundary instead of running an unsafe committee.

use crate::common::{
    acquire_test_permit, assert_epoch_reached, assert_epoch_records_verify,
    create_genesis_for_test, current_epoch, fetch_verified_epoch_record,
    generate_new_validator_txs, get_block_number, get_latest_consensus_header_number,
    get_tx_receipt_block, kill_child, loop_epochs, send_owner_tx, start_nodes,
    wait_for_epoch_at_least, wait_for_head_at_least, wait_for_mid_epoch, wait_for_rpc,
    ProcessGuard, EPOCH_DURATION, INITIAL_STAKE_AMOUNT, NEW_VALIDATOR,
};
use alloy::{
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall as _,
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::wait_until;
use tn_types::{Address, EpochCertificate, EpochRecord, U256};
use tokio::time::timeout;
use tracing::info;

/// Name of the genesis-configured non-committee node used by the current-committee ejection
/// tests.
///
/// It is funded and key-configured at genesis (via [`create_genesis_for_test`]'s extra-node
/// parameter) but never stakes, so it only follows the chain — an observer in behavior, without
/// tightening the committee quorum when it is down. The below-tolerance halt test configures it
/// (genesis creation requires the extra node) but never starts it.
const GENESIS_OBSERVER: &str = "genesis-observer";

/// Per-test epoch duration (seconds) for the mid-epoch restart regression.
///
/// [`test_committee_member_restarted_mid_epoch_after_ejection`] must fit its whole in-epoch
/// sequence — fresh-boundary entry, one full leader rotation, the governance burn, the burn-block
/// execution pin, kill, restart, and the mid-epoch consensus rejoin — inside ONE epoch. That
/// sequence's worst case is ~29s, so the module-wide [`EPOCH_DURATION`] (10s) cannot host it;
/// 40s banks comfortable margin without stretching the test into extra epochs. Every timeout in
/// that test which scales with epoch length derives from this constant, not [`EPOCH_DURATION`].
const RESTART_EPOCH_DURATION: u64 = 40;

/// Find the epoch that contains `block` by walking the epoch-info ring buffer down from the
/// current epoch.
///
/// Only valid for blocks landed within the registry's 4-slot epoch-info window; older blocks
/// error rather than returning a wrapped (wrong) epoch.
async fn epoch_of_block<P: Provider>(provider: &P, block: u64) -> eyre::Result<u32> {
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);
    let current = registry.getCurrentEpochInfo().call().await?;
    // getEpochInfo(e).blockHeight is the FIRST block of epoch e
    for epoch in (current.epochId.saturating_sub(3)..=current.epochId).rev() {
        let info = registry.getEpochInfo(epoch).call().await?;
        if info.blockHeight <= block {
            return Ok(epoch);
        }
    }
    Err(eyre::eyre!("block {block} is older than the epoch info ring buffer"))
}

/// Submit a governance `burn` for `victim` with owner nonce `nonce` and wait for confirmation.
///
/// A confirmation straddling an epoch boundary can get orphaned and lost; on failure the
/// identical tx is retried once with the same nonce (idempotent: same payload, same hash) from
/// the next measured mid-epoch window. Returns the block the burn's receipt landed in.
async fn burn_with_retry<P: Provider>(
    provider: &P,
    rpc_url: &str,
    governance_wallet: &mut TransactionFactory,
    chain: Arc<RethChainSpec>,
    victim: Address,
    nonce: u64,
) -> eyre::Result<u64> {
    let calldata: Bytes =
        ConsensusRegistry::burnCall { validatorAddress: victim }.abi_encode().into();
    match send_owner_tx(rpc_url, governance_wallet, chain.clone(), calldata.clone()).await {
        Ok((_hash, block)) => Ok(block),
        Err(first_try) => {
            info!(target: "eject-test", %first_try, ?victim, "burn confirmation failed; retrying once");
            governance_wallet.set_nonce(nonce);
            wait_for_mid_epoch(provider, rpc_url).await?;
            let (_hash, block) = send_owner_tx(rpc_url, governance_wallet, chain, calldata).await?;
            Ok(block)
        }
    }
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Eject (burn) a validator that is seated ONLY in a future committee.
///
/// Scenario: 5 genesis committee validators + a 6th that mints/stakes/activates after genesis.
/// Once the 6th validator lands in a committee two epochs out (∉ current, ∉ next, ∈ subsequent),
/// governance burns it mid-epoch. The current and next committees must be byte-identical to
/// their pre-burn reads, the subsequent committee must shrink 5 → 4 without the burned
/// validator, `nextCommitteeSize` must stay 5 (eligible drops 6 → 5, no auto-decrement), and
/// every following epoch boundary must close with verifying epoch records on all nodes —
/// including the burned validator's node, which keeps following as an observer.
async fn test_validator_ejected_from_future_committee_only() -> eyre::Result<()> {
    let _permit = acquire_test_permit();

    // create validator and governance wallets (seed 33 = consensus registry owner)
    let mut new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("eject_future")?;
    let temp_path = temp_dir.path();

    let mut governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let genesis = create_genesis_for_test(
        temp_path,
        (NEW_VALIDATOR, new_validator.address()),
        governance_wallet.address(),
        &committee,
        EPOCH_DURATION,
    )?;

    // start nodes (committee + new validator)
    committee.push((NEW_VALIDATOR, new_validator.address()));
    let (procs, endpoints) = start_nodes(temp_path, &committee, "eject_future", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let _guard = ProcessGuard::new(procs);
    let newval_url = &endpoints[5].http_url;

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let txs = generate_new_validator_txs(
        temp_path,
        chain.clone(),
        &mut new_validator,
        &mut governance_wallet,
    )?;

    let rpc_url = endpoints[0].http_url.clone();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for_rpc(&provider).await?;

    // submit mint -> stake -> activate; receipt-anchor the last (activate) to pin epoch A
    let mut last_tx_block = 0u64;
    for tx in txs {
        let pending = provider.send_raw_transaction(&tx).await?;
        // txs may land at an epoch boundary, get orphaned, and be re-injected; allow two epochs
        let hash = timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??;
        last_tx_block = get_tx_receipt_block(&rpc_url, &hash.to_string())?;
    }

    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let activation_epoch = epoch_of_block(&provider, last_tx_block).await?;
    info!(target: "eject-test", activation_epoch, last_tx_block, "new validator activated");

    // 6 eligible validators, committee size stays 5: each shuffle now excludes exactly one
    assert_eq!(
        registry.getEligibleValidatorCount().call().await?,
        U256::from(6),
        "eligible count must be 6 after activation"
    );
    assert_eq!(registry.getNextCommitteeSize().call().await?, 5);

    // scenario-1 baseline for the new validator: staked + active, not retired, full stake
    let newval_addr = new_validator.address();
    assert!(!registry.isRetired(newval_addr).call().await?);
    let breakdown = registry.getBalanceBreakdown(newval_addr).call().await?;
    assert!(breakdown._0 > U256::ZERO, "outstanding balance must reflect the stake");

    wait_for_epoch_at_least(&provider, activation_epoch + 1).await?;

    // Find an epoch W (the current epoch) with newval ∉ W, ∉ W+1, ∈ W+2. The earliest committee
    // that can seat the new validator is chosen at the close of the activation epoch, two epochs
    // ahead — before that, ∉ W and ∉ W+1 hold by construction, and each fresh shuffle seats it
    // with p = 5/6. Five attempts miss with p = (1/6)^5 ≈ 0.01%.
    let mut candidate = activation_epoch + 1;
    let mut arrangement = None;
    for _attempt in 0..5 {
        let snap = wait_for_epoch_at_least(&provider, candidate).await?;
        let w = snap.epoch_id;
        let in_committee = |vals: &[ConsensusRegistry::ValidatorInfo]| {
            vals.iter().any(|v| v.validatorAddress == newval_addr)
        };
        let c_w = registry.getCommitteeValidators(w).call().await?;
        let c_w1 = registry.getCommitteeValidators(w + 1).call().await?;
        let c_w2 = registry.getCommitteeValidators(w + 2).call().await?;
        // the three reads must not straddle a boundary
        let still = registry.getCurrentEpochInfo().call().await?.epochId;
        if still != w {
            candidate = still;
            continue;
        }
        let (in_w, in_w1, in_w2) = (in_committee(&c_w), in_committee(&c_w1), in_committee(&c_w2));
        info!(target: "eject-test", w, in_w, in_w1, in_w2, "arrangement check");
        if !in_w && !in_w1 && in_w2 {
            arrangement = Some((w, c_w2.len()));
            break;
        }
        candidate = w + 1;
    }
    let (w, w2_len_before) = arrangement.ok_or_else(|| {
        eyre::eyre!("new validator not seated in a future-only committee within 5 epochs")
    })?;
    assert_eq!(w2_len_before, 5, "future committee must have 5 members before ejection");

    // pre-burn byte-identical baselines for the untouched committees
    let pre_w = registry.getCommitteeBlsPubkeys(w).call().await?;
    let pre_w1 = registry.getCommitteeBlsPubkeys(w + 1).call().await?;

    // the new validator's BLS key: assert exclusion by key, not just address
    let newval_info = Config::load_from_path_or_default::<NodeInfo>(
        temp_path.join(NEW_VALIDATOR).join("node-info.yaml").as_path(),
        ConfigFmt::YAML,
    )?;
    let newval_bls: Bytes = newval_info.bls_public_key.compress().into();
    assert!(pre_w.iter().all(|k| k != &newval_bls));
    assert!(pre_w1.iter().all(|k| k != &newval_bls));

    // governance burn, mid-epoch W
    let snap = wait_for_mid_epoch(&provider, &rpc_url).await?;
    eyre::ensure!(
        snap.epoch_id == w,
        "epoch slid past the arrangement window before the burn (at {}, wanted {w})",
        snap.epoch_id
    );
    let calldata: Bytes =
        ConsensusRegistry::burnCall { validatorAddress: newval_addr }.abi_encode().into();
    let (_burn_hash, burn_block) =
        send_owner_tx(&rpc_url, &mut governance_wallet, chain.clone(), calldata).await?;
    let burn_epoch = epoch_of_block(&provider, burn_block).await?;
    // Newval is absent from committees W and W+1, so a confirmation that slips into W+1 is
    // still a future-only ejection; only sliding beyond W+1 would invalidate the scenario.
    eyre::ensure!(
        burn_epoch == w || burn_epoch == w + 1,
        "burn landed in epoch {burn_epoch}, outside the future-only window [{w}, {}]",
        w + 1
    );
    info!(target: "eject-test", burn_block, burn_epoch, "burn confirmed");

    // ejection state: retired, stake confiscated, eligible 6 -> 5, committee size NOT decremented
    assert!(registry.isRetired(newval_addr).call().await?, "burned validator must be retired");
    let breakdown = registry.getBalanceBreakdown(newval_addr).call().await?;
    assert_eq!(breakdown._0, U256::ZERO, "outstanding balance must be confiscated");
    assert_eq!(
        registry.getEligibleValidatorCount().call().await?,
        U256::from(5),
        "eligible count must drop 6 -> 5"
    );
    assert_eq!(
        registry.getNextCommitteeSize().call().await?,
        5,
        "next committee size must NOT auto-decrement while eligible >= size"
    );

    // committees W and W+1 are byte-identical to their pre-burn reads
    assert_eq!(registry.getCommitteeBlsPubkeys(w).call().await?, pre_w);
    assert_eq!(registry.getCommitteeBlsPubkeys(w + 1).call().await?, pre_w1);

    // committee W+2 shrank 5 -> 4 and excludes the burned validator (by address and BLS key)
    let c_w2_post = registry.getCommitteeValidators(w + 2).call().await?;
    assert_eq!(c_w2_post.len(), 4, "future committee must shrink 5 -> 4");
    assert!(c_w2_post.iter().all(|v| v.validatorAddress != newval_addr));
    let w2_keys_post = registry.getCommitteeBlsPubkeys(w + 2).call().await?;
    assert_eq!(w2_keys_post.len(), 4);
    assert!(w2_keys_post.iter().all(|k| k != &newval_bls));

    // Every subsequent boundary must close on today's code (all record-producer reads are
    // post-mutation and identical across nodes). Cross 3+ boundaries so epoch W+2 runs with the
    // shrunken committee and W+3 is seated after it.
    let head_before = get_block_number(newval_url)?;
    loop_epochs(0, 3, &rpc_url, EPOCH_DURATION).await?;
    let final_snap = wait_for_epoch_at_least(&provider, w + 3).await?;

    // W+2 ran with the 4-member committee; W+3 reseats all 5 remaining validators
    let c_w2_ran = registry.getCommitteeValidators(w + 2).call().await?;
    assert_eq!(c_w2_ran.len(), 4, "epoch W+2 must have run with the shrunken committee");
    assert!(c_w2_ran.iter().all(|v| v.validatorAddress != newval_addr));
    let c_w3 = registry.getCommitteeValidators(w + 3).call().await?;
    assert_eq!(c_w3.len(), 5, "post-ejection shuffles reseat all 5 remaining validators");
    assert!(c_w3.iter().all(|v| v.validatorAddress != newval_addr));

    // Epoch records for every closed epoch exist and verify on ALL nodes, including the burned
    // validator's node (never seated in a committee, it keeps following as an observer; serving
    // each record's final block proves it executed through every boundary).
    let latest_closed = final_snap.epoch_id - 1;
    assert_epoch_records_verify(&endpoints, 0..=latest_closed, EPOCH_DURATION * 6).await?;

    // the burned validator's node keeps following the chain (epoch-close blocks keep coming)
    wait_for_head_at_least(newval_url, head_before.max(burn_block) + 1, EPOCH_DURATION * 3).await?;

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Acceptance test for the mid-epoch-ejection fix: eject (burn) a validator seated in the
/// CURRENT committee, mid-epoch, and prove the network keeps closing epochs.
///
/// On pre-fix code the network halts at the first epoch boundary after the burn:
/// `build_epoch_record` strict-compared the post-ejection committee read (4 members) against the
/// previous epoch record's 5-member `next_committee`, so every node deterministically failed to
/// close the epoch. The fix validates via `EpochRecord::committee_compatible` (shrink-tolerant);
/// `state-sync::epoch_committee_valid` shares the same predicate for syncing nodes.
///
/// Scenario: 5 genesis committee validators plus one genesis-configured non-committee follower
/// (the "observer"). After the epoch 0 -> 1 boundary passes and the victim's staked+active
/// baseline is verified, the observer is killed (its datadir preserved). Governance then burns
/// committee member `validator-5` mid-epoch E: the stored current/next/subsequent committees
/// shrink 5 -> 4 without the victim, the eligible count drops 5 -> 4, `nextCommitteeSize`
/// auto-decrements to 4, and the stake is confiscated.
///
/// THE ACCEPTANCE ASSERT: the E -> E+1 boundary closes on every surviving validator. Afterwards:
/// epoch record E carries the shrunken committee and its certificate verifies with a 3-of-4
/// quorum; the restarted observer backfills the shrink through the sync-side tolerance; epochs
/// E+1..E+3 seat exactly the 4 survivors; a normal transaction confirms; and the ejected
/// validator's node keeps following the chain as an observer.
async fn test_validator_ejected_from_current_committee_mid_epoch() -> eyre::Result<()> {
    let _permit = acquire_test_permit();

    // committee wallets + a follower wallet; seed 33 = consensus registry owner
    let observer_wallet = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(7));
    let mut nodes = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];
    // the victim: committee validator index 4
    let (victim_name, victim_addr) = nodes[4];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("eject_current")?;
    let temp_path = temp_dir.path();

    let mut governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let genesis = create_genesis_for_test(
        temp_path,
        (GENESIS_OBSERVER, observer_wallet.address()),
        governance_wallet.address(),
        &nodes,
        EPOCH_DURATION,
    )?;

    // start nodes (committee + genesis observer); the observer never stakes, it only follows
    nodes.push((GENESIS_OBSERVER, observer_wallet.address()));
    let (procs, endpoints) = start_nodes(temp_path, &nodes, "eject_current", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let mut guard = ProcessGuard::new(procs);
    let victim_url = endpoints[4].http_url.clone();
    let rpc_url = endpoints[0].http_url.clone();

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for_rpc(&provider).await?;
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);

    // ---- Phase 0: the epoch 0 -> 1 boundary passes; staked+active baseline for the victim ----
    let baseline_snap = wait_for_epoch_at_least(&provider, 1).await?;

    let victim_info = registry.getValidator(victim_addr).call().await?;
    assert_eq!(
        victim_info.currentStatus,
        ConsensusRegistry::ValidatorStatus::Active,
        "victim must be Active at baseline"
    );
    assert!(!victim_info.isRetired, "victim must not be retired at baseline");

    // one tn-namespace RPC read cross-checked against the equivalent eth_call registry read
    let victim_info_tn: ConsensusRegistry::ValidatorInfo =
        provider.raw_request("tn_getValidator".into(), (victim_addr,)).await?;
    assert_eq!(victim_info_tn, victim_info, "tn_getValidator diverges from the eth_call read");

    // (outstanding, initialStake, rewards): genesis stake shape with no slashes
    let initial_stake = parse_ether(INITIAL_STAKE_AMOUNT)?;
    let breakdown = registry.getBalanceBreakdown(victim_addr).call().await?;
    assert_eq!(breakdown._1, initial_stake, "initial stake must match the genesis stake");
    assert!(breakdown._0 >= initial_stake, "outstanding balance must cover the stake");
    assert_eq!(breakdown._0, breakdown._1 + breakdown._2, "outstanding = stake + rewards");

    // seated in the current committee; committee size targets stay at 5
    let baseline_committee = registry.getCommitteeValidators(baseline_snap.epoch_id).call().await?;
    assert_eq!(baseline_committee.len(), 5, "genesis committee must have 5 members");
    assert!(
        baseline_committee.iter().any(|v| v.validatorAddress == victim_addr),
        "victim must be seated in the current committee at baseline"
    );
    assert_eq!(registry.getNextCommitteeSize().call().await?, 5);
    assert_eq!(registry.getEligibleValidatorCount().call().await?, U256::from(5));
    info!(target: "eject-test", epoch = baseline_snap.epoch_id, "victim baseline verified");

    // Before killing the observer, require ITS datadir to hold the certified epoch-0 record.
    // `save_dummy_epoch0` is in-memory only, so a node killed after executing the epoch-0
    // closing block but before persisting the real record 0 cannot restart (`run_epoch` exits:
    // "We have epoch 0 in our database if we are past epoch 0"). That crash window is a
    // pre-existing product issue orthogonal to the ejection fix under test; gating the kill on
    // the observer serving certified record 0 keeps this test deterministic.
    fetch_verified_epoch_record(&endpoints[5].http_url, 0, EPOCH_DURATION * 6).await?;

    // ---- Kill the observer (datadir preserved for the later restart) ----
    let mut observer_child = guard.take(5).ok_or_else(|| eyre::eyre!("no observer process"))?;
    kill_child(&mut observer_child);
    let observer_provider = ProviderBuilder::new().connect_http(endpoints[5].http_url.parse()?);
    assert!(observer_provider.get_chain_id().await.is_err(), "observer must be down");

    // victim BLS key for by-key exclusion asserts
    let victim_node_info = Config::load_from_path_or_default::<NodeInfo>(
        temp_path.join(victim_name).join("node-info.yaml").as_path(),
        ConfigFmt::YAML,
    )?;
    let victim_bls = victim_node_info.bls_public_key;
    let victim_bls_bytes: Bytes = victim_bls.compress().into();

    // ---- Governance burn of a CURRENT committee member, mid-epoch E ----
    let burn_snap = wait_for_mid_epoch(&provider, &rpc_url).await?;
    let intended_epoch = burn_snap.epoch_id;

    // Pre-burn reads: with 5 eligible validators and committee size 5, EVERY committee seats all
    // 5 — so the current-committee scenario survives a burn confirmation that slides an epoch.
    // The subsequent committee (intended + 2) is the furthest stored array.
    for target in intended_epoch..=intended_epoch + 2 {
        let keys = registry.getCommitteeBlsPubkeys(target).call().await?;
        assert_eq!(keys.len(), 5, "epoch {target} committee must have 5 members before the burn");
        assert!(
            keys.iter().any(|k| k == &victim_bls_bytes),
            "victim missing from epoch {target} committee before the burn"
        );
    }

    let calldata: Bytes =
        ConsensusRegistry::burnCall { validatorAddress: victim_addr }.abi_encode().into();
    let burn_block = match send_owner_tx(
        &rpc_url,
        &mut governance_wallet,
        chain.clone(),
        calldata.clone(),
    )
    .await
    {
        Ok((_hash, block)) => block,
        Err(first_try) => {
            // A confirmation straddling a boundary can get orphaned and lost; retry the
            // identical tx once with the same nonce (idempotent: same payload, same hash).
            info!(target: "eject-test", %first_try, "burn confirmation failed; retrying once");
            governance_wallet.set_nonce(0);
            wait_for_mid_epoch(&provider, &rpc_url).await?;
            let (_hash, block) =
                send_owner_tx(&rpc_url, &mut governance_wallet, chain.clone(), calldata).await?;
            block
        }
    };

    // anchor E to the epoch the burn actually landed in (the victim sits in every committee, so
    // a slide preserves the current-committee mid-epoch scenario)
    let e = epoch_of_block(&provider, burn_block).await?;
    eyre::ensure!(
        e >= intended_epoch && e <= intended_epoch + 2,
        "burn landed in epoch {e}, outside the verified pre-burn window \
         [{intended_epoch}, {}]",
        intended_epoch + 2
    );
    info!(target: "eject-test", burn_block, epoch = e, "burn confirmed mid-epoch");

    // ---- Immediate post-burn asserts: retirement, confiscation, shrunken committees ----
    assert!(registry.isRetired(victim_addr).call().await?, "victim must be retired");
    let breakdown = registry.getBalanceBreakdown(victim_addr).call().await?;
    assert_eq!(breakdown._0, U256::ZERO, "victim stake must be confiscated");
    assert_eq!(
        registry.getEligibleValidatorCount().call().await?,
        U256::from(4),
        "eligible count must drop 5 -> 4"
    );
    assert_eq!(
        registry.getNextCommitteeSize().call().await?,
        4,
        "next committee size must auto-decrement 5 -> 4 with only 4 eligible validators"
    );
    // the current epoch E and the pre-chosen epochs E+1 and E+2 all shrink 5 -> 4, excluding the
    // victim by address and by BLS key
    for target in e..=e + 2 {
        let vals = registry.getCommitteeValidators(target).call().await?;
        assert_eq!(vals.len(), 4, "epoch {target} committee must shrink 5 -> 4");
        assert!(
            vals.iter().all(|v| v.validatorAddress != victim_addr),
            "victim must be swap-and-popped out of epoch {target} committee"
        );
        let keys = registry.getCommitteeBlsPubkeys(target).call().await?;
        assert_eq!(keys.len(), 4);
        assert!(keys.iter().all(|k| k != &victim_bls_bytes));
    }

    // ---- THE ACCEPTANCE ASSERT: the E -> E+1 epoch boundary closes ----
    //
    // At the close of epoch E every node's `build_epoch_record` reads the post-ejection committee
    // (4 members) and must accept it against the previous record's 5-member `next_committee` via
    // `EpochRecord::committee_compatible`. On pre-fix code the strict equality check fails and
    // every node halts here; the bounded wait turns that hang into a named failure.
    for endpoint in endpoints.iter().take(4) {
        assert_epoch_reached(
            &endpoint.http_url,
            e + 1,
            "ACCEPTANCE FAILED (mid-epoch current-committee ejection halted the epoch boundary)",
        )
        .await?;
    }
    info!(target: "eject-test", epoch = e + 1, "acceptance boundary closed on all survivors");

    // ---- E+1..E+3 all seat exactly the 4 survivors ----
    // Read the stored committees immediately after the boundary closes: the registry only
    // serves epochs inside a [current-3, current+2] window (`InvalidEpoch` outside it), so
    // these reads must not run after open-ended polling phases. At current == E+1 the readable
    // window spans E-2..E+3.
    let survivors: BTreeSet<Address> = nodes[..4].iter().map(|(_, addr)| *addr).collect();
    for target in e + 1..=e + 3 {
        let vals = registry.getCommitteeValidators(target).call().await?;
        let seated: BTreeSet<Address> = vals.iter().map(|v| v.validatorAddress).collect();
        assert_eq!(vals.len(), 4, "epoch {target} must seat a 4-member committee");
        assert_eq!(seated, survivors, "epoch {target} committee must be exactly the 4 survivors");
    }

    // ---- Epoch record E carries the shrunken committee; its cert quorum is 3-of-4 ----
    // every live node (4 survivors + the ejected validator's node) serves verifying records for
    // all closed epochs and has executed each record's final block
    assert_epoch_records_verify(&endpoints[..5], 0..=e, EPOCH_DURATION * 6).await?;

    let (record_e, cert_e): (EpochRecord, EpochCertificate) =
        provider.raw_request("tn_epochRecord".into(), (e,)).await?;
    assert_eq!(record_e.epoch, e);
    assert_eq!(record_e.committee.len(), 4, "record E must carry the shrunken committee");
    assert!(
        record_e.committee.iter().all(|k| k != &victim_bls),
        "record E committee must exclude the ejected validator's BLS key"
    );
    assert_eq!(record_e.super_quorum(), 3, "shrunken committee quorum must be 3-of-4");
    assert!(
        record_e.verify_with_cert(&cert_e),
        "record E certificate must verify against the shrunken committee"
    );

    // ---- Restart the observer on its old datadir: it must backfill the shrunken record E
    // through the sync-side tolerance (`state-sync::epoch_committee_valid`) ----
    wait_for_epoch_at_least(&provider, e + 2).await?;
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, &nodes[5..], "eject_current", 2)?;
    let new_child = new_children.pop().ok_or_else(|| eyre::eyre!("no observer process"))?;
    let observer_endpoint =
        new_endpoints.pop().ok_or_else(|| eyre::eyre!("no observer endpoint"))?;
    guard.replace(5, new_child);

    let observer_provider =
        ProviderBuilder::new().connect_http(observer_endpoint.http_url.parse()?);
    wait_for_rpc(&observer_provider).await?;
    // records 0..=E+1 (including the shrunken E) verify on the observer, and it serves each
    // record's final block — proving it executed through the ejection boundary
    let observer_endpoints = [observer_endpoint];
    assert_epoch_records_verify(&observer_endpoints, 0..=e + 1, EPOCH_DURATION * 6).await?;
    info!(target: "eject-test", "observer backfilled the shrunken epoch record after restart");

    // ---- A post-ejection epoch runs live on the survivors; the network serves traffic ----
    wait_for_epoch_at_least(&provider, e + 3).await?;
    // window-free read: the RUNNING epoch's committee is exactly the 4 survivors
    let live_info = registry.getCurrentEpochInfo().call().await?;
    let live_seated: BTreeSet<Address> = live_info.committee.iter().copied().collect();
    assert_eq!(live_seated, survivors, "the running epoch must seat exactly the 4 survivors");

    // a normal transaction still confirms on the shrunken network
    let recipient = Address::from_slice(&[0x77; 20]);
    let transfer = governance_wallet.create_eip1559_encoded(
        chain,
        None,
        100,
        Some(recipient),
        parse_ether("1")?,
        Bytes::default(),
    );
    let pending = provider.send_raw_transaction(&transfer).await?;
    let hash = timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??;
    let tx_block = get_tx_receipt_block(&rpc_url, &hash.to_string())?;
    info!(target: "eject-test", tx_block, "post-ejection transfer confirmed");

    // the ejected validator's node keeps following as an observer: its head advances past the
    // transfer's block and it serves the shrunken epoch-E record with a verifying cert
    wait_for_head_at_least(&victim_url, tx_block + 1, EPOCH_DURATION * 3).await?;
    fetch_verified_epoch_record(&victim_url, e, EPOCH_DURATION * 3).await?;

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Regression test for epoch-entry read pinning: a committee member killed and restarted
/// MID-EPOCH, AFTER a governance burn ejected a DIFFERENT committee member, must rejoin the SAME
/// epoch view its peers are running and close the epoch with identical records.
///
/// THE INVARIANT: every epoch-scoped entry read — committee membership, epoch info, fee and
/// rewards seeding — pins to the previous epoch's closing block, which `concludeEpoch` writes
/// exactly once at the boundary. A fresh boundary crossing, a crash-restart, and a mode-change
/// re-entry therefore all re-derive the identical epoch view: same rewards rows, same quorum
/// threshold, same round-robin leader schedule. Without the pin, a restart reads the canonical
/// TIP, which a mid-epoch `burn` has already mutated (swap-and-pop of the victim from the stored
/// current-committee array). The restarted node then enters epoch E with a 4-member view while
/// its peers run the 5-member epoch: it cannot verify their consensus certificates (wrong quorum,
/// wrong leader schedule) so its consensus height freezes until the boundary, and any close it
/// reaches seeds rewards without the ejected leader's row — a divergent withdrawals_root, hence a
/// divergent closing-block hash, hence a divergent epoch-record digest.
///
/// Scenario: 5 genesis committee validators plus the genesis-configured follower, all six
/// started (the follower extends the record-consistency assert to the sync path). Mid-epoch E,
/// governance burns committee member `validator-5` (the victim, whose node stays up); then
/// `validator-2` (the kill target) is killed and restarted on its same datadir inside the SAME
/// epoch. Premise gates make the scenario airtight rather than timing-lucky:
///
/// - VICTIM-LED COMMIT: one full leader rotation (5 commits) is observed inside epoch E before the
///   burn. The leader schedule is strict round-robin over the 5 authorities and a fresh epoch's
///   leader swap table is empty, so the victim led at least one commit — its leader-rewards row is
///   nonzero, and an entry view that drops the row provably diverges at the close (with a zero row
///   both views would close identically and the test would prove nothing).
/// - REPRO PIN: the kill target must have EXECUTED the burn block before dying. Otherwise its
///   restart tip would still be pre-burn state, and even an unpinned tip read would derive the
///   correct 5-member committee — an accidental pass.
/// - IN-EPOCH GUARDS: the epoch is entered exactly at its boundary flip (banking the full duration
///   for the sequence above), and every wait re-checks that epoch E is still current, failing
///   loudly as "premise broken" if the sequence straddles a boundary.
///
/// The detectors, in order: (a) the restarted node's consensus height re-advances past
/// everything it could have held at death — commits it can only acquire by verifying the
/// 5-member committee's certificates — while the fleet is still in epoch E; (b) the E -> E+1
/// boundary closes on all six nodes; (c) epoch records 0..=E verify on all six nodes with
/// final-state HASH equality (the record-divergence detector); (d) a transfer submitted through
/// the RESTARTED node's own RPC confirms, and the burned validator keeps following the chain as
/// an observer.
async fn test_committee_member_restarted_mid_epoch_after_ejection() -> eyre::Result<()> {
    let _permit = acquire_test_permit();

    // committee wallets + a follower wallet; seed 33 = consensus registry owner
    let observer_wallet = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(7));
    let mut nodes = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];
    // the victim (burned mid-epoch, node stays up); the kill target is validator-2 = index 1
    let victim_addr = nodes[4].1;

    // setup genesis with the longer per-test epoch (see RESTART_EPOCH_DURATION)
    let temp_dir = tempfile::TempDir::with_prefix("eject_restart")?;
    let temp_path = temp_dir.path();

    let mut governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let genesis = create_genesis_for_test(
        temp_path,
        (GENESIS_OBSERVER, observer_wallet.address()),
        governance_wallet.address(),
        &nodes,
        RESTART_EPOCH_DURATION,
    )?;

    // start all six nodes (committee + genesis observer)
    nodes.push((GENESIS_OBSERVER, observer_wallet.address()));
    let (procs, mut endpoints) = start_nodes(temp_path, &nodes, "eject_restart", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let mut guard = ProcessGuard::new(procs);
    let victim_url = endpoints[4].http_url.clone();
    let rpc_url = endpoints[0].http_url.clone();

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for_rpc(&provider).await?;
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);

    // ---- Phase 0: cross the 0 -> 1 boundary ----
    wait_for_epoch_at_least(&provider, 1).await?;

    // Before killing validator-2, require ITS datadir to hold the certified epoch-0 record.
    // `save_dummy_epoch0` is in-memory only, so a node killed after executing the epoch-0
    // closing block but before persisting the real record 0 cannot restart (`run_epoch` exits:
    // "We have epoch 0 in our database if we are past epoch 0"). That crash window is a
    // pre-existing product issue orthogonal to the pinning fix under test; gating the kill on
    // the kill target serving certified record 0 keeps this test deterministic.
    fetch_verified_epoch_record(&endpoints[1].http_url, 0, RESTART_EPOCH_DURATION * 2).await?;

    // ---- Fresh-boundary entry: catch the flip into epoch E ----
    // The in-epoch sequence below (rotation, burn, pin, kill, restart, rejoin) worst-cases at
    // ~29s; entering exactly at the flip banks the full epoch duration for it, where a
    // mid-epoch entry would squander half.
    let pre_flip = current_epoch(&provider).await?.epoch_id;
    wait_until(
        Duration::from_secs(RESTART_EPOCH_DURATION * 2),
        &format!("fresh-boundary entry: waiting for the epoch to flip past {pre_flip} on node 0"),
        || async { Ok(current_epoch(&provider).await?.epoch_id > pre_flip) },
    )
    .await?;
    let e = current_epoch(&provider).await?.epoch_id;
    // the flip poll re-checks every ~10ms, so observing a double-flip means this thread stalled
    // for a whole epoch — the "fresh entry" premise is gone
    eyre::ensure!(
        e == pre_flip + 1,
        "premise broken: entered epoch {e}, expected the fresh flip to {}",
        pre_flip + 1
    );
    info!(target: "eject-test", epoch = e, "entered epoch at a fresh boundary");

    // ---- Victim-led premise: observe one full leader rotation inside epoch E ----
    // The leader schedule is strict round-robin over the committee (leader(round) =
    // authorities[round/2 - 1 mod 5]) and a fresh epoch starts with an empty leader swap table
    // (reputation scores only finalize a swap after many more sub-dags), so with all five
    // validators live, five consecutive commits are one full rotation containing a victim-led
    // commit. Each commit appends exactly one consensus header, so height h0 + 5 == five
    // commits, all inside epoch E because h0 is read after the flip and the wait re-checks the
    // epoch. This guarantees the victim's leader-rewards row is nonzero before the burn.
    let h0 = get_latest_consensus_header_number(&rpc_url)?;
    wait_until(
        Duration::from_secs(RESTART_EPOCH_DURATION),
        &format!(
            "victim-led premise: waiting for consensus height {} (h0 {h0} + one full \
             rotation) on node 0",
            h0 + 5
        ),
        || async {
            let now = current_epoch(&provider).await?.epoch_id;
            eyre::ensure!(
                now == e,
                "premise broken: epoch flipped to {now} before one full leader rotation \
                 completed inside epoch {e}"
            );
            Ok(get_latest_consensus_header_number(&rpc_url)? >= h0 + 5)
        },
    )
    .await?;
    info!(target: "eject-test", epoch = e, "one full leader rotation observed");

    // ---- Governance burn of the victim, mid-epoch E — deliberately NO retry ----
    // A retry could slide the burn across the boundary and silently break the mid-epoch
    // premise; the ensure below turns that into a loud failure instead. Nonce 0: the burn is
    // this wallet's first transaction.
    let calldata: Bytes =
        ConsensusRegistry::burnCall { validatorAddress: victim_addr }.abi_encode().into();
    let (_burn_hash, burn_block) =
        send_owner_tx(&rpc_url, &mut governance_wallet, chain.clone(), calldata).await?;
    let burn_epoch = epoch_of_block(&provider, burn_block).await?;
    eyre::ensure!(
        burn_epoch == e,
        "premise broken: burn landed in epoch {burn_epoch}, not mid-epoch {e}"
    );
    info!(target: "eject-test", burn_block, epoch = e, "victim burned mid-epoch");

    // brief on-chain shape check: the victim is retired and the eligible set shrank 5 -> 4
    assert!(registry.isRetired(victim_addr).call().await?, "victim must be retired");
    assert_eq!(
        registry.getEligibleValidatorCount().call().await?,
        U256::from(4),
        "eligible count must drop 5 -> 4"
    );

    // ---- Repro pin: the kill target must EXECUTE the burn block before dying ----
    // The pre-fix bug is an entry read of POST-burn registry state at the canonical tip. If the
    // node died before executing the burn block, its tip at restart would still be pre-burn
    // state and even an unpinned read would derive the correct 5-member committee — the test
    // would pass pre-fix by accident. Requiring the burn block on the kill target's head makes
    // the restart provably face post-burn state at its tip.
    wait_for_head_at_least(&endpoints[1].http_url, burn_block, 10).await?;

    // ---- Kill validator-2 (datadir preserved), still mid-epoch E ----
    // h_kill: the kill target's own consensus tip just before death — a floor for the rejoin
    // assert below.
    let h_kill = get_latest_consensus_header_number(&endpoints[1].http_url)?;
    let mut target_child = guard.take(1).ok_or_else(|| eyre::eyre!("no kill-target process"))?;
    kill_child(&mut target_child);
    let downed_provider = ProviderBuilder::new().connect_http(endpoints[1].http_url.parse()?);
    assert!(downed_provider.get_chain_id().await.is_err(), "kill target must be down");
    info!(target: "eject-test", h_kill, epoch = e, "kill target down mid-epoch");

    // ---- Restart validator-2 on its same datadir, still mid-epoch E ----
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, &nodes[1..2], "eject_restart", 2)?;
    let new_child = new_children.pop().ok_or_else(|| eyre::eyre!("no restarted process"))?;
    let new_endpoint = new_endpoints.pop().ok_or_else(|| eyre::eyre!("no restarted endpoint"))?;
    guard.replace(1, new_child);
    endpoints[1] = new_endpoint;
    let restarted_provider = ProviderBuilder::new().connect_http(endpoints[1].http_url.parse()?);
    wait_for_rpc(&restarted_provider).await?;

    // ---- Premise: the kill/restart round trip stayed inside epoch E on both sides ----
    // A restart that straddled the boundary re-enters at E+1, where even an unpinned tip read
    // is correct — the test would prove nothing, so fail loudly instead of silently passing.
    let anchor_epoch = current_epoch(&provider).await?.epoch_id;
    let restarted_epoch = current_epoch(&restarted_provider).await?.epoch_id;
    eyre::ensure!(
        anchor_epoch == e && restarted_epoch == e,
        "premise broken: restart straddled the epoch boundary (node 0 at {anchor_epoch}, \
         restarted node at {restarted_epoch}, wanted {e})"
    );

    // ---- (a) THE MID-EPOCH REJOIN ASSERT: consensus re-advances within epoch E ----
    // The rejoin floor is everything the restarted node could hold WITHOUT verifying new
    // certificates: its own tip at death (h_kill) and node 0's tip observed after the restart
    // came back up (h_peer covers commits that landed between the h_kill snapshot and process
    // death). Exceeding the max of both proves the node verified and followed commits produced
    // by the 5-member committee AFTER its restart — impossible on an entry view read from the
    // post-burn tip, where the 4-member quorum and leader schedule reject every peer
    // certificate and the node's consensus height freezes until the boundary.
    let h_peer = get_latest_consensus_header_number(&rpc_url)?;
    let rejoin_floor = h_kill.max(h_peer);
    info!(
        target: "eject-test",
        h_kill, h_peer, rejoin_floor, "waiting for the mid-epoch consensus rejoin"
    );
    wait_until(
        Duration::from_secs(RESTART_EPOCH_DURATION),
        &format!(
            "mid-epoch rejoin: waiting for the restarted node's consensus height to exceed \
             {rejoin_floor} within epoch {e}"
        ),
        || async {
            let now = current_epoch(&provider).await?.epoch_id;
            eyre::ensure!(
                now == e,
                "REGRESSION (mid-epoch rejoin failed): epoch flipped to {now} while the \
                 restarted node's consensus height was still <= {rejoin_floor} — a committee \
                 member restarted after the ejection never re-verified its peers' certificates \
                 inside epoch {e}"
            );
            Ok(get_latest_consensus_header_number(&endpoints[1].http_url)? > rejoin_floor)
        },
    )
    .await?;
    info!(target: "eject-test", epoch = e, "restarted node rejoined consensus mid-epoch");

    // ---- (b) The E -> E+1 boundary closes on ALL SIX nodes ----
    for (idx, endpoint) in endpoints.iter().enumerate() {
        assert_epoch_reached(
            &endpoint.http_url,
            e + 1,
            &format!("POST-RESTART BOUNDARY (node index {idx})"),
        )
        .await?;
    }
    info!(target: "eject-test", epoch = e + 1, "boundary closed on all six nodes");

    // ---- (c) THE RECORD-DIVERGENCE ASSERT: records 0..=E hash-identical on all six ----
    // `assert_epoch_records_verify` requires every node to serve a CERTIFIED record for every
    // epoch AND to have executed each record's final block with the exact hash the record
    // commits to. An entry view that dropped the ejected leader's rewards row surfaces here:
    // divergent withdrawals in the closing block -> divergent closing-block hash -> divergent
    // record digest, reported as a hash mismatch on the restarted node.
    assert_epoch_records_verify(&endpoints, 0..=e, RESTART_EPOCH_DURATION * 2).await?;
    info!(target: "eject-test", epoch = e, "epoch records verify hash-identical on all six nodes");

    // ---- (d) Liveness through the restarted node; the burned validator keeps following ----
    // Submit the transfer via the RESTARTED node's own RPC: its pool/batch path must be live
    // again, not merely its sync path.
    let recipient = Address::from_slice(&[0x77; 20]);
    let transfer = governance_wallet.create_eip1559_encoded(
        chain,
        None,
        100,
        Some(recipient),
        parse_ether("1")?,
        Bytes::default(),
    );
    let pending = restarted_provider.send_raw_transaction(&transfer).await?;
    // a confirmation straddling a boundary can get orphaned and re-injected; allow two epochs
    let hash =
        timeout(Duration::from_secs(RESTART_EPOCH_DURATION * 2 + 11), pending.watch()).await??;
    let tx_block = get_tx_receipt_block(&endpoints[1].http_url, &hash.to_string())?;
    info!(target: "eject-test", tx_block, "post-restart transfer confirmed via the restarted node");

    // the burned validator's node keeps following as an observer: its head advances past the
    // transfer's block (the next block arrives at latest with the E+2 closing block)
    wait_for_head_at_least(&victim_url, tx_block + 1, RESTART_EPOCH_DURATION * 3).await?;

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Prove the network halts BY DESIGN when governance ejections shrink the committee below the
/// tolerated bound within one epoch.
///
/// On-chain there is no committee floor: `ConsensusRegistry::burn` only rejects an ejection that
/// would leave a committee empty or larger than the eligible set, so back-to-back burns walk the
/// stored committees 5 -> 4 -> 3 inside a single epoch. Three members is below both bounds of
/// `EpochRecord::committee_compatible` (the 4-member floor and `ceil(2 * 5 / 3) = 4`), so at the
/// next epoch boundary every validator's `build_epoch_record` rejects the shrunken committee
/// read against the previous record's `next_committee`, the error propagates out of the epoch
/// manager, and the process exits with code 1 — a deliberate fail-stop rather than running a
/// committee too small to be safe against the promised set. Recovery is manual, so the test ends
/// at the halt: every validator exits with code 1 carrying the incompatibility error on stderr,
/// and no endpoint answers RPC afterwards.
///
/// If the two burns straddle an epoch boundary the halt shifts one epoch later: 5 -> 4 is a
/// tolerated shrink and that boundary closes, then 4 -> 3 fails the 4-member floor. Either way
/// the epoch containing the SECOND burn is the epoch that cannot close.
async fn test_network_halts_when_ejections_shrink_committee_below_tolerance() -> eyre::Result<()> {
    let _permit = acquire_test_permit();

    // committee wallets; seed 33 = consensus registry owner
    let nodes = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];
    // the victims: committee validators 4 and 5
    let (victim_a_name, victim_a_addr) = nodes[3];
    let (victim_b_name, victim_b_addr) = nodes[4];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("eject_halt")?;
    let temp_path = temp_dir.path();

    let mut governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    // `create_genesis_for_test` always configures one extra non-committee node; it is funded and
    // key-configured at genesis but deliberately NEVER STARTED: at the halt boundary every
    // VALIDATOR deterministically fails `build_epoch_record` and exits, while a follower's fate
    // is timing-dependent (it may stall waiting for consensus that never comes instead of
    // exiting). Starting only validators keeps every spawned process's expected exit definite.
    let unstarted_wallet = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(7));
    let genesis = create_genesis_for_test(
        temp_path,
        (GENESIS_OBSERVER, unstarted_wallet.address()),
        governance_wallet.address(),
        &nodes,
        EPOCH_DURATION,
    )?;

    // start ONLY the 5 committee validators
    let (procs, endpoints) = start_nodes(temp_path, &nodes, "eject_halt", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let mut guard = ProcessGuard::new(procs);
    let rpc_url = endpoints[0].http_url.clone();

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    wait_for_rpc(&provider).await?;
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);

    // ---- Phase 0: cross the 0 -> 1 boundary before burning ----
    // A burn during epoch 0 would be absorbed: record 0 is built without a previous record, so
    // there is no promised `next_committee` for the shrunken committee read to be incompatible
    // with. The halt requires a boundary whose previous record promised the pre-burn committee.
    wait_for_epoch_at_least(&provider, 1).await?;

    // pre-burn baseline: 5 eligible validators, size-5 committees, both victims unretired
    assert_eq!(registry.getEligibleValidatorCount().call().await?, U256::from(5));
    assert_eq!(registry.getNextCommitteeSize().call().await?, 5);
    for victim in [victim_a_addr, victim_b_addr] {
        assert!(
            !registry.isRetired(victim).call().await?,
            "victim {victim} must not be retired at baseline"
        );
    }

    // victim BLS keys for by-key exclusion asserts
    let mut victim_bls_keys = Vec::new();
    for name in [victim_a_name, victim_b_name] {
        let node_info = Config::load_from_path_or_default::<NodeInfo>(
            temp_path.join(name).join("node-info.yaml").as_path(),
            ConfigFmt::YAML,
        )?;
        let bls: Bytes = node_info.bls_public_key.compress().into();
        victim_bls_keys.push(bls);
    }

    // ---- Two governance burns, launched mid-epoch away from the boundary ----
    let burn_snap = wait_for_mid_epoch(&provider, &rpc_url).await?;
    info!(target: "eject-test", epoch = burn_snap.epoch_id, "burning two committee validators");
    let burn_a_block = burn_with_retry(
        &provider,
        &rpc_url,
        &mut governance_wallet,
        chain.clone(),
        victim_a_addr,
        0,
    )
    .await?;
    let burn_b_block =
        burn_with_retry(&provider, &rpc_url, &mut governance_wallet, chain, victim_b_addr, 1)
            .await?;

    // Anchor the halt to where the burns actually landed. Both in one epoch is the intended
    // shape; a straddle across adjacent epochs is tolerated (the 5 -> 4 boundary closes and the
    // halt shifts one epoch later — see the doc comment). Either way the epoch containing the
    // SECOND burn cannot close.
    let burn_a_epoch = epoch_of_block(&provider, burn_a_block).await?;
    let burn_b_epoch = epoch_of_block(&provider, burn_b_block).await?;
    eyre::ensure!(
        burn_b_epoch == burn_a_epoch || burn_b_epoch == burn_a_epoch + 1,
        "burns landed in epochs {burn_a_epoch} and {burn_b_epoch}; expected one epoch or an \
         adjacent straddle"
    );
    let halt_epoch = burn_b_epoch;
    info!(
        target: "eject-test",
        burn_a_block, burn_a_epoch, burn_b_block, burn_b_epoch, halt_epoch, "burns confirmed"
    );

    // ---- Post-burn shape: both retired, 3 eligible, stored committees shrink to 3 ----
    for victim in [victim_a_addr, victim_b_addr] {
        assert!(
            registry.isRetired(victim).call().await?,
            "burned validator {victim} must be retired"
        );
    }
    assert_eq!(
        registry.getEligibleValidatorCount().call().await?,
        U256::from(3),
        "eligible count must drop 5 -> 3"
    );
    assert_eq!(
        registry.getNextCommitteeSize().call().await?,
        3,
        "next committee size must auto-decrement to the 3 remaining eligible validators"
    );
    // the next and subsequent committees relative to the halt epoch — the arrays the failing
    // record build reads — exclude both victims by address and by BLS key
    for target in halt_epoch + 1..=halt_epoch + 2 {
        let vals = registry.getCommitteeValidators(target).call().await?;
        assert_eq!(vals.len(), 3, "epoch {target} committee must shrink to 3 members");
        assert!(
            vals.iter().all(|v| {
                v.validatorAddress != victim_a_addr && v.validatorAddress != victim_b_addr
            }),
            "victims must be swap-and-popped out of epoch {target} committee"
        );
        let keys = registry.getCommitteeBlsPubkeys(target).call().await?;
        assert_eq!(keys.len(), 3);
        assert!(keys.iter().all(|k| !victim_bls_keys.contains(k)));
    }

    // ---- THE HALT ASSERT: every validator exits, on its own, with code 1 ----
    //
    // At the close of `halt_epoch` every validator's `build_epoch_record` reads the 3-member
    // committee, rejects it against the previous record's promise, and the node exits through
    // `main`'s error path. Exit code 1 distinguishes the designed error-exit from a signal
    // death (`ExitStatus::code()` returns `None` for signals).
    //
    // All 5 must exit — the epoch cannot close for anyone. Known residual risk: a validator
    // lagging at the boundary could in principle miss the closing commit once its peers exit
    // first; the single shared 4-epoch-duration deadline (bounding all validators at once, so the
    // total wait is independent of node count) absorbs realistic same-host lag, and a genuine
    // stall should fail the test rather than be tolerated.
    let exits =
        guard.wait_for_natural_exits(0..nodes.len(), Duration::from_secs(EPOCH_DURATION * 4))?;
    for (idx, status) in exits {
        eyre::ensure!(
            status.code() == Some(1),
            "validator index {idx} exited with {status:?}; expected the designed error exit \
             (code 1)"
        );
    }
    info!(target: "eject-test", halt_epoch, "all validators fail-stopped at the halt boundary");

    // ---- Tie each exit to the designed reason via the node's stderr log ----
    // `main` prints the propagated error on stderr before exiting 1; the distinctive substring
    // is from `build_epoch_record`'s error ("Last epochs next committee not compatible with
    // this epochs committee!").
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let log_dir = std::path::PathBuf::from(manifest_dir).join("test_logs").join("eject_halt");
    for (name, _) in &nodes {
        let stderr_path = log_dir.join(format!("node{name}-run1.stderr.log"));
        let stderr = std::fs::read_to_string(&stderr_path)?;
        eyre::ensure!(
            stderr.contains("not compatible with this epochs committee"),
            "{name} stderr ({}) lacks the epoch-record incompatibility error; the node exited \
             with code 1 for the wrong reason",
            stderr_path.display()
        );
    }

    // ---- Liveness-negative: the network is down, no validator endpoint answers RPC ----
    for endpoint in &endpoints {
        let dead = ProviderBuilder::new().connect_http(endpoint.http_url.parse()?);
        assert!(
            dead.get_chain_id().await.is_err(),
            "validator RPC {} still answers after the designed halt",
            endpoint.http_url
        );
    }

    Ok(())
}
