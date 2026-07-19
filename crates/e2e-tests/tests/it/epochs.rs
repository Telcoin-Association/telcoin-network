//! Test the epoch boundary and validator shuffles.

use crate::common::get_block;

use super::common::{
    create_genesis_for_test, fetch_verified_epoch_record, generate_new_validator_txs, loop_epochs,
    start_nodes, ProcessGuard, NEW_VALIDATOR,
};
use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::NodeEndpoints;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{path::Path, sync::Arc, time::Duration};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::wait_until;
use tn_types::{Address, EpochCertificate, EpochRecord, Genesis};
use tokio::time::timeout;
use tracing::debug;

const MIN_EPOCHS_TO_TEST: usize = 6;
// Epoch init creates HDX index files per epoch (open_epoch_pack → new_epoch →
// ConsensusPack::open_append). With test-utils, these are ~1.3MB each (vs ~130MB in prod).
// 5s is the consensus minimum epoch duration; halving it from 10s roughly halves the
// wall time of each epoch test. The two `tn_epochRecord` certificate-availability polls
// below are floored to an absolute minimum (`.max(..)`) rather than scaling with this
// constant, because certificate production is a fixed async quorum-voting cost that does
// not shrink with the epoch cadence.
const EPOCH_DURATION: u64 = 5;

async fn test_epoch_boundary_inner(
    genesis: Genesis,
    mut governance_wallet: TransactionFactory,
    temp_path: &Path,
    new_validator: &mut TransactionFactory,
    endpoints: &[NodeEndpoints],
) -> eyre::Result<()> {
    // create transactions to make new validator eligible for future epochs
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let txs = generate_new_validator_txs(temp_path, chain, new_validator, &mut governance_wallet)?;

    // create rpc client for node1 default rpc address
    let rpc_url = &endpoints[0].http_url;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // submit txs to: issue NFT, stake, and activate new validator
    for tx in txs {
        let pending = provider.send_raw_transaction(&tx).await?;
        // Some txns will likely be submitted as epochs switch.
        // This is handled now so we can just submit and wait for the watch
        // no need to re-submit, etc.  If that becomes needed then the
        // missed txns may not be getting re-injected into the mempool.
        debug!(target: "epoch-test", "pending tx: {pending:?}");
        // Txns may land right at an epoch boundary, get orphaned, and be re-injected into
        // the next epoch. Allow two full epoch durations + startup buffer for confirmation.
        timeout(Duration::from_secs((EPOCH_DURATION * 2 + 11) as u64), pending.watch()).await??;
    }

    // cross-check the `tn` namespace ConsensusRegistry endpoints against direct eth_call reads
    assert_tn_registry_endpoints(&provider).await?;

    // retrieve current committee
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_epoch_block_height = current_epoch_info.blockHeight;

    // track the number of times the new validator was in the epoch committee
    let mut new_validator_in_committee_count = 0;

    // sleep for first epoch with 1s offset and begin assertions loop
    tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;

    let mut shuffled = false;
    let mut latest_epoch = 0u32;
    // the new validator has a 1/6 chance of being selected for the new committee
    //
    // if the new validator hasn't been shuffled in by the minimum number of epochs to test,
    // continue looping up to 99% probability that new validator is shuffled into committee
    //
    // probability (if purely random):
    // 1 - (5/6)^n >= 0.99
    // n ~= 25 iterations
    for i in 0..25 {
        // poll until the epoch changes, with a generous timeout for parallel test load
        wait_until(Duration::from_secs(EPOCH_DURATION * 4), "epoch to change", || async {
            Ok(consensus_registry.getCurrentEpochInfo().call().await? != current_epoch_info)
        })
        .await?;
        let new_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, EPOCH_DURATION);

        latest_epoch = i as u32;

        // count the number of times the new validator is in committee
        if new_epoch_info.committee.contains(&new_validator.address()) {
            new_validator_in_committee_count += 1;
        }

        // if min number of epochs have transitioned, assert new validator has been shuffled in
        // at least once to end the test
        if i > MIN_EPOCHS_TO_TEST && new_validator_in_committee_count > 0 {
            shuffled = true;
            break;
        }

        // store the last seen epoch info that is expected to change every epoch
        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;
    }

    if shuffled {
        // Verify all nodes have valid (certified) Epoch Records.
        // Poll each epoch individually — certificates are produced asynchronously
        // after epoch boundaries via quorum voting.
        // TODO issue 375, should use tn_latestConsensusHeader RPC for this when fixed.
        for ep in endpoints {
            for epoch in 0..=latest_epoch {
                // Certificate availability is a fixed async quorum-voting cost, so floor
                // this deadline at 30s instead of letting it shrink with EPOCH_DURATION.
                fetch_verified_epoch_record(&ep.http_url, epoch, (EPOCH_DURATION * 3).max(30))
                    .await?;
            }
        }
        Ok(())
    } else {
        // return error if loop didn't return
        Err(eyre::eyre!("new validator not shuffled into committee!"))
    }
}

/// Cross-check the `tn` namespace ConsensusRegistry endpoints against direct `eth_call` reads.
///
/// Both read paths resolve state at the canonical tip, so results must match modulo an epoch
/// rolling between requests (handled by retrying).
async fn assert_tn_registry_endpoints<P: Provider>(provider: &P) -> eyre::Result<()> {
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);

    // the epoch can roll between reads, so retry until all reads land in the same epoch
    let mut attempts = 0;
    let epoch_info = loop {
        let from_contract = consensus_registry.getCurrentEpochInfo().call().await?;
        let from_tn: ConsensusRegistry::EpochInfo =
            provider.raw_request("tn_getCurrentEpochInfo".into(), ()).await?;
        let epoch_from_tn: u32 = provider.raw_request("tn_getCurrentEpoch".into(), ()).await?;
        if from_tn == from_contract && epoch_from_tn == from_tn.epochId {
            break from_tn;
        }
        attempts += 1;
        assert!(
            attempts < 3,
            "tn registry endpoints never converged with eth_call reads: \
             tn={from_tn:?} contract={from_contract:?} epoch={epoch_from_tn}"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    // all validators regardless of status
    let validators: Vec<ConsensusRegistry::ValidatorInfo> =
        provider.raw_request("tn_getValidators".into(), ("Any",)).await?;
    assert!(!validators.is_empty(), "tn_getValidators(\"Any\") returned no validators");

    // `"Any"` must equal the union of the five concrete status sets, read at one pinned tip.
    // The five internal reads can no longer straddle a block commit, so a validator that changes
    // status mid-read is never double-counted or dropped. The dedup check below is the direct
    // regression guard; the length check confirms union completeness. The per-status sets are
    // fetched as separate requests, so an epoch boundary between them could move a validator
    // between sets; retry until all reads land in one epoch (mirrors the convergence loop above).
    let statuses = ["Staked", "PendingActivation", "Active", "PendingExit", "Exited"];
    let mut set_attempts = 0;
    let (any_set, per_status_total) = loop {
        let epoch_before: u32 = provider.raw_request("tn_getCurrentEpoch".into(), ()).await?;
        let any_set: Vec<ConsensusRegistry::ValidatorInfo> =
            provider.raw_request("tn_getValidators".into(), ("Any",)).await?;
        let mut per_status_total = 0usize;
        for status in statuses {
            let set: Vec<ConsensusRegistry::ValidatorInfo> =
                provider.raw_request("tn_getValidators".into(), (status,)).await?;
            per_status_total += set.len();
        }
        let epoch_after: u32 = provider.raw_request("tn_getCurrentEpoch".into(), ()).await?;
        if epoch_before == epoch_after {
            break (any_set, per_status_total);
        }
        set_attempts += 1;
        assert!(set_attempts < 3, "validator-set reads never landed in a single epoch");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };

    // union completeness (best-effort): "Any" holds exactly as many entries as the five status
    // sets combined. The `epoch_before == epoch_after` guard rules out epoch-boundary transitions,
    // but this still assumes no mid-epoch status change (e.g. a `stake`/`activate` tx) lands
    // between the separate per-status RPC requests — true in this quiescent test. The no-duplicate
    // `HashSet` check below is the load-bearing regression guard: it operates on the single atomic
    // "Any" response and needs no such assumption.
    assert_eq!(
        any_set.len(),
        per_status_total,
        "tn_getValidators(\"Any\") length must equal the sum of the five per-status sets"
    );

    // no double-count: each validator lives in exactly one status set, so the pinned "Any" union
    // must contain each validator address at most once
    let mut seen = std::collections::HashSet::new();
    for info in &any_set {
        assert!(
            seen.insert(info.validatorAddress),
            "tn_getValidators(\"Any\") double-counted validator {}",
            info.validatorAddress
        );
    }

    // `Undefined` (0) reverts on-chain: expect an eth_call-style error (code 3 with revert
    // bytes in `data`) rather than a leaked internal error string
    let revert_err = provider
        .raw_request::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
            "tn_getValidators".into(),
            ("Undefined",),
        )
        .await
        .expect_err("tn_getValidators(\"Undefined\") must revert");
    let resp = revert_err.as_error_resp().expect("revert surfaces as a JSON-RPC error response");
    assert_eq!(resp.code, 3, "on-chain revert must map to code 3: {resp:?}");
    assert!(
        resp.message.starts_with("execution reverted"),
        "revert message must match eth_call style: {resp:?}"
    );
    assert!(resp.as_revert_data().is_some(), "revert bytes must be in error data: {resp:?}");

    // a guaranteed-absent epoch record returns EIP-1474 resource-not-found
    let not_found_err = provider
        .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (u32::MAX,))
        .await
        .expect_err("epoch record for u32::MAX must not exist");
    let resp = not_found_err.as_error_resp().expect("not found surfaces as a JSON-RPC error");
    assert_eq!(resp.code, -32001, "missing record must map to -32001: {resp:?}");

    // round-trip a known validator: committee members are guaranteed to be registered
    let known_validator =
        *epoch_info.committee.first().ok_or_else(|| eyre::eyre!("empty committee"))?;
    let from_contract = consensus_registry.getValidator(known_validator).call().await?;
    let from_tn: ConsensusRegistry::ValidatorInfo =
        provider.raw_request("tn_getValidator".into(), (known_validator,)).await?;
    assert_eq!(from_tn, from_contract, "tn_getValidator mismatch for {known_validator}");

    // concurrent-burst smoke test: fire 3x the 64-permit semaphore bound at once.
    // the RPC-layer guard must queue excess reads (not reject), so every request resolves Ok.
    // catches deadlock or spurious rejection in the acquire-before-spawn path.
    let burst = (0..192).map(|_| provider.raw_request::<_, u32>("tn_getCurrentEpoch".into(), ()));
    for res in futures::future::join_all(burst).await {
        res.expect("tn_getCurrentEpoch must succeed under concurrent load");
    }

    Ok(())
}

async fn test_epoch_sync_inner(
    guard: &mut ProcessGuard,
    kill_idx: usize,
    nodes_to_start: &[(&str, Address)],
    committee: &[(&str, Address)],
    temp_path: &Path,
    endpoints: &mut Vec<NodeEndpoints>,
) -> eyre::Result<()> {
    // create rpc client for node1 default rpc address
    let rpc_url = &endpoints[0].http_url;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // sleep for first epoch with 1s offset and begin assertions loop
    tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;

    // Go through at least 5 epochs.
    loop_epochs(0, 5, &endpoints[0].http_url, EPOCH_DURATION).await?;
    // Kill a node
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }

    // Make sure the node really is down.
    let killed_url = &endpoints[2].http_url;
    let killed_provider = ProviderBuilder::new().connect_http(killed_url.parse()?);
    assert!(killed_provider.get_chain_id().await.is_err(), "Node not down!");

    loop_epochs(5, 5, &endpoints[0].http_url, EPOCH_DURATION).await?;
    // Restart the node
    let (mut new_children, mut new_endpoints) =
        start_nodes(temp_path, nodes_to_start, "epoch_sync", 2)?;
    let new_child = new_children.pop().expect("child");
    guard.replace(kill_idx, new_child);
    // Update the endpoint for the restarted node (new dynamic ports)
    endpoints[kill_idx] = new_endpoints.pop().expect("endpoint");
    let current_epoch = loop_epochs(10, 5, &endpoints[0].http_url, EPOCH_DURATION).await?;

    // Verify all nodes have valid (certified) Epoch Records.
    // The node that was down should also have all these records after syncing.
    // Poll each epoch individually — certificates are produced asynchronously
    // after epoch boundaries via quorum voting.
    // TODO issue 375, should use tn_latestConsensusHeader RPC for this when fixed.
    let latest_epoch = current_epoch - 1;
    for (i, ep) in endpoints.iter().enumerate() {
        for epoch in 0..=latest_epoch {
            let val_name = committee[i].0;
            let file_test = temp_path
                .join(val_name)
                .join("consensus-db")
                .join("epochs")
                .join(format!("epoch-{epoch}"))
                .join("data");
            let pack_file_exists = std::fs::exists(file_test).unwrap_or_default();
            assert!(pack_file_exists, "Missing an epoch pack file for {val_name} on epoch {epoch}");
            // When a new validator joins the committee mid-test, its epoch vote quorum
            // collection can time out (25 × 2.5s = ~62s) before the failed-quorum P2P
            // fallback runs. The spawn_epoch_record_collector retries every 5s
            // independently, so 60s gives enough time for it to succeed. This is a fixed
            // cost, so floor the deadline at 60s rather than scaling it with EPOCH_DURATION.
            let epoch_rec =
                fetch_verified_epoch_record(&ep.http_url, epoch, (EPOCH_DURATION * 6).max(60))
                    .await
                    .map_err(|e| eyre::eyre!("validator {val_name}: {e}"))?;
            // Make sure we have executed the final block from the epoch record.
            // This should prove we have the consensus output as well (i.e. verify the pack data).
            get_block(&ep.http_url, Some(epoch_rec.final_state.number)).expect(&format!(
                "final block for {epoch} for {val_name} missing {}",
                epoch_rec.final_state.number
            ));
        }
    }

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test a new node joining the network and being shuffled into the committee.
async fn test_epoch_boundary() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // create validator and governance wallets for adding new validator later
    let mut new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("epoch_boundary")?;
    let temp_path = temp_dir.path();

    let governance_wallet =
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
    let (procs, endpoints) = start_nodes(temp_path, &committee, "epoch_boundary", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let _guard = ProcessGuard::new(procs);

    test_epoch_boundary_inner(genesis, governance_wallet, temp_path, &mut new_validator, &endpoints)
        .await
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that sync works to fill in missing epochs.
async fn test_epoch_sync() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // create validator and governance wallets for adding new validator later
    let new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("epoch_sync")?;
    let temp_path = temp_dir.path();

    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let _genesis = create_genesis_for_test(
        temp_path,
        (NEW_VALIDATOR, new_validator.address()),
        governance_wallet.address(),
        &committee,
        EPOCH_DURATION,
    )?;

    // start nodes (committee + new validator)
    committee.push((NEW_VALIDATOR, new_validator.address()));
    let (procs, mut endpoints) = start_nodes(temp_path, &committee, "epoch_sync", 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let mut guard = ProcessGuard::new(procs);

    test_epoch_sync_inner(
        &mut guard,
        2,
        &[("validator-3", Address::from_slice(&[0x33; 20]))],
        &committee[..],
        temp_path,
        &mut endpoints,
    )
    .await
}
