//! E2E coverage for governance ejection (`ConsensusRegistry::burn`) of validators.
//!
//! `burn` retires the validator, confiscates its stake, and swap-and-pops it out of the stored
//! current/next/subsequent committee arrays. Two scenarios are covered:
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

use crate::common::{
    acquire_test_permit, assert_epoch_reached, assert_epoch_records_verify,
    create_genesis_for_test, fetch_verified_epoch_record, generate_new_validator_txs,
    get_block_number, get_tx_receipt_block, kill_child, loop_epochs, send_owner_tx, start_nodes,
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
use tn_types::{Address, EpochCertificate, EpochRecord, U256};
use tokio::time::timeout;
use tracing::info;

/// Name of the genesis-configured non-committee node used by the mid-epoch ejection test.
///
/// It is funded and key-configured at genesis (via [`create_genesis_for_test`]'s extra-node
/// parameter) but never stakes, so it only follows the chain — an observer in behavior, without
/// tightening the committee quorum when it is down.
const GENESIS_OBSERVER: &str = "genesis-observer";

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
