//! Test the epoch boundary and validator shuffles.

use crate::common::get_block;

use super::common::{
    assert_blocks_match_consensus, assert_consensus_commit_times, assert_epoch_records_verify,
    assert_nodes_agree_on_commit_times, block_commit_time, create_genesis_for_test,
    drive_light_tx_load, fetch_verified_epoch_record, generate_new_validator_txs, loop_epochs,
    pin_fork_epochs, read_consensus_headers, scrape_metric_value, start_nodes,
    start_validator_with_args, wait_for_rpc, walk_block_commit_times, BlockCommitTime,
    ProcessGuard, CROSS_FORK_EPOCH, EVM_TIMESTAMP_CLAMPED_SERIES, NEW_VALIDATOR, NODE_PASSWORD,
    RPC_REQUEST_TIMEOUT,
};
use alloy::{
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use e2e_tests::{config_local_testnet_with_epoch_duration, NodeEndpoints};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::BTreeMap,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tn_config::{
    Config, ConfigFmt, ConfigTrait as _, KeyConfig, NetworkConfig, NodeInfo, WORKER_CONFIGS_ADDRESS,
};
use tn_reth::{
    system_calls::{ConsensusRegistry, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_storage::pack_validate::{validate_pack_file, Verdict};
use tn_test_utils::wait_until;
use tn_types::{
    get_available_tcp_port, get_available_udp_port, keccak256, Address, BootstrapServer, Epoch,
    EpochCertificate, EpochRecord, Genesis, GenesisAccount, P2pNode, B256, U256,
};
use tokio::time::timeout;
use tracing::{debug, info};

const MIN_EPOCHS_TO_TEST: usize = 6;
// Epoch init creates HDX index files per epoch (open_epoch_pack → new_epoch →
// ConsensusPack::open_append). With test-utils, these are ~1.3MB each (vs ~130MB in prod).
// 5s is the consensus minimum epoch duration; halving it from 10s roughly halves the
// wall time of each epoch test. The two `tn_epochRecord` certificate-availability polls
// below are floored to an absolute minimum (`.max(..)`) rather than scaling with this
// constant, because certificate production is a fixed async quorum-voting cost that does
// not shrink with the epoch cadence.
const EPOCH_DURATION: u64 = 5;

/// Leader epoch at which [`test_epoch_subsecond_timestamps_across_fork`] arms the sub-second
/// timestamp fork: epochs 0 and 1 commit whole seconds, every later epoch commits milliseconds.
///
/// Two pre-fork epochs rather than one because epoch 0 routinely holds a single commit: the nodes
/// start after genesis, so their first commit is already past epoch 0's boundary and closes it.
/// Epoch 1 is a full-length epoch, which is what gives the walk consecutive pre-fork commits.
const SUBSECOND_FORK_EPOCH: Epoch = 2;

/// Epoch [`test_epoch_subsecond_timestamps_across_fork`] runs the network to before it checks
/// anything.
///
/// Epochs 0 through 4 have closed by then, so the run holds a pre-fork seam (0 to 1), the fork
/// seam (1 to 2) and two post-fork seams (2 to 3 and 3 to 4), and every one of those epochs has at
/// least its closing execution block.
const SUBSECOND_TARGET_EPOCH: Epoch = 5;

/// Sub-second fork epoch for [`test_epoch_sync_subsecond_fork_activates_while_down`], chosen so
/// the whole fork epoch is committed while the killed node is down.
///
/// The kill comes once `loop_epochs` has watched three boundaries pass. That puts `epoch_at_kill`
/// at 3, or at 4 when the network is already in epoch 1 by the time the watch starts, which is
/// common because the first commit closes epoch 0 (see [`SUBSECOND_FORK_EPOCH`]). The restart comes
/// three boundaries later, at `epoch_at_kill + 3` or later. A fork epoch above the kill epoch and
/// no later than the restart epoch in both cases is 5 or 6. Only 5 also opens and closes entirely
/// while the node is down in both cases, so every commit the node first sees under the fork is one
/// it neither proposed nor voted in, and it learns about the fork only from packs it imports.
const SUBSECOND_FORK_WHILE_DOWN_EPOCH: Epoch = 5;

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

    // No pre-pad sleep is needed here: the loop's first poll waits for the epoch to change.
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
                // This poll runs only after the new validator has been shuffled into the
                // committee, so it can be waiting on the new-validator epoch record. That
                // epoch has zero quorum redundancy: super_quorum = (committee * 2) / 3 + 1 = 4,
                // exactly the established validators that remain once the new one joins. If a
                // single established vote is slow to reach the freshly-joined node, that node
                // falls back to its own vote-collection loop, which can run its full timeout
                // (25 x 2.5s = ~62.5s) before the failed-quorum record collector back-fills the
                // cert on its 5s cadence. Floor the deadline above that window (65s) rather
                // than letting it shrink with EPOCH_DURATION. (The sync test's analogous poll
                // is floored at 60s but is not exposed to this window: it kills/restarts a
                // node rather than adding one to the committee.)
                fetch_verified_epoch_record(&ep.http_url, epoch, (EPOCH_DURATION * 3).max(65))
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

/// The epochs [`test_epoch_sync_inner`] observed around its kill and restart, so a caller can
/// assert which epochs the node missed and which ones its own archive holds.
#[derive(Debug, Clone)]
struct SyncEpochs {
    /// The epochs whose pack files were fingerprinted before the kill and revalidated after the
    /// restart (see [`sealed_epochs`]).
    sealed: Range<Epoch>,
    /// The epoch validator-1 reported open right before the node was killed.
    epoch_at_kill: Epoch,
    /// The epoch validator-1 reported open right before the node was restarted.
    epoch_at_restart: Epoch,
    /// The last epoch for which every node, the restarted one included, served a verified
    /// certified record and had executed the record's final block.
    latest_epoch: Epoch,
}

/// Kill one node, advance several epochs without it, restart it against its existing datadir, and
/// assert it back-fills everything it missed.
///
/// Returns the epochs it observed (see [`SyncEpochs`]), so a caller can assert what the sealed
/// packs cover and which epochs passed while the node was down.
///
/// `test` names the log directory under `test_logs/` for the restarted node, matching the one the
/// caller used for the initial spawn.
async fn test_epoch_sync_inner(
    guard: &mut ProcessGuard,
    kill_idx: usize,
    nodes_to_start: &[(&str, Address)],
    committee: &[(&str, Address)],
    temp_path: &Path,
    test: &str,
    endpoints: &mut Vec<NodeEndpoints>,
) -> eyre::Result<SyncEpochs> {
    // create rpc client for node1 default rpc address
    let rpc_url = &endpoints[0].http_url;
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // No pre-pad sleep is needed here: loop_epochs polls until the epoch changes.
    // Go through at least 3 epochs.
    let epoch_at_kill = loop_epochs(0, 3, &endpoints[0].http_url, EPOCH_DURATION).await?;
    // Kill a node
    if let Some(mut taken) = guard.take(kill_idx) {
        super::common::kill_child(&mut taken);
    }

    // Make sure the node really is down.
    let killed_url = &endpoints[2].http_url;
    let killed_provider = ProviderBuilder::new().connect_http(killed_url.parse()?);
    assert!(killed_provider.get_chain_id().await.is_err(), "Node not down!");

    // Fingerprint the killed node's sealed packs while its datadir is quiescent: the process is
    // gone, so nothing is appending to them and nothing has re-imported them yet. Step 8 below
    // compares against these bytes once the node is back and caught up.
    let killed_datadir = temp_path.join(committee[kill_idx].0);
    let sealed = sealed_epochs(epoch_at_kill);
    let sealed_before = fingerprint_sealed_packs(&killed_datadir, sealed.clone())?;
    info!(
        target: "epoch-test",
        epoch_at_kill,
        ?sealed,
        "fingerprinted sealed epoch packs of the killed node",
    );

    let epoch_at_restart = loop_epochs(3, 3, &endpoints[0].http_url, EPOCH_DURATION).await?;
    info!(target: "epoch-test", epoch_at_restart, "restarting the killed node");
    // Restart the node
    let (mut new_children, mut new_endpoints) = start_nodes(temp_path, nodes_to_start, test, 2)?;
    let new_child = new_children.pop().expect("child");
    guard.replace(kill_idx, new_child);
    // Update the endpoint for the restarted node (new dynamic ports)
    endpoints[kill_idx] = new_endpoints.pop().expect("endpoint");
    let current_epoch = loop_epochs(6, 3, &endpoints[0].http_url, EPOCH_DURATION).await?;

    // Verify all nodes have valid (certified) Epoch Records.
    // The node that was down should also have all these records after syncing.
    // Poll each epoch individually — certificates are produced asynchronously
    // after epoch boundaries via quorum voting.
    // TODO issue 375, should use tn_latestConsensusHeader RPC for this when fixed.
    let latest_epoch = current_epoch - 1;
    // The killed node's certified records, kept so the pack revalidation below can anchor each
    // sealed pack to its predecessor (see `assert_sealed_packs_unchanged`).
    let mut killed_epoch_records = BTreeMap::new();
    for (i, ep) in endpoints.iter().enumerate() {
        for epoch in 0..=latest_epoch {
            let val_name = committee[i].0;
            let file_test = epoch_pack_path(&temp_path.join(val_name), epoch);
            let pack_file_exists = std::fs::exists(file_test).unwrap_or_default();
            assert!(pack_file_exists, "Missing an epoch pack file for {val_name} on epoch {epoch}");
            // A node was killed and restarted earlier in this test, so it must back-fill the
            // epoch certificates it missed while down. That recovery is a fixed async cost:
            // the restarted node re-collects each missing cert from its peers via the
            // 5s-cadence record collector (spawn_epoch_record_collector), independent of
            // EPOCH_DURATION. Floor the deadline at 60s rather than letting it shrink with the
            // epoch cadence. (Unlike test_epoch_boundary, this test never adds a validator to
            // the committee, so it is not exposed to the ~62.5s new-validator vote-quorum
            // window.)
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
            if i == kill_idx {
                killed_epoch_records.insert(epoch, epoch_rec);
            }
        }
    }

    // Existence and a served epoch record say nothing about the bytes on disk, so re-read them:
    // the restart must not have rewritten history it already had.
    assert_sealed_packs_unchanged(&killed_datadir, &sealed_before, &killed_epoch_records)?;

    Ok(SyncEpochs { sealed, epoch_at_kill, epoch_at_restart, latest_epoch })
}

/// The epochs whose pack files must survive a kill and restart byte-for-byte, given the epoch
/// `loop_epochs` last observed before the node was killed.
///
/// Two epochs of margin below `epoch_at_kill`, not one:
///
/// - `epoch_at_kill` was open when the node died, so its pack is mid-append. A restarted node
///   re-requests every epoch whose pack is incomplete (`state_sync::request_epochs`), so the
///   restart is entitled to replace that one wholesale.
/// - `epoch_at_kill - 1` is only known sealed on the node whose RPC `loop_epochs` polled. The node
///   this test kills can still be a beat behind executing that epoch's closing block, and a pack is
///   flushed when the NEXT epoch opens (`ConsensusChain::new_epoch` persists the outgoing pack).
///   Killing it inside that window leaves a short pack that the restart legitimately re-imports.
///
/// Everything below that closed at least one full epoch before the kill, so it is quiescent on
/// every node.
fn sealed_epochs(epoch_at_kill: Epoch) -> Range<Epoch> {
    0..epoch_at_kill.saturating_sub(1)
}

/// Path of the consensus pack `data` file for `epoch` inside a node's datadir.
///
/// This file alone is the byte stream a syncing peer receives and imports, which is why both the
/// fingerprint and the offline validator below run against it: the `idx`/`hash` sidecars are
/// needed to *use* a pack, not to judge it.
fn epoch_pack_path(datadir: &Path, epoch: Epoch) -> PathBuf {
    datadir.join("consensus-db").join("epochs").join(format!("epoch-{epoch}")).join("data")
}

/// Fingerprint the pack file of every epoch in `sealed` under `datadir`.
fn fingerprint_sealed_packs(
    datadir: &Path,
    sealed: Range<Epoch>,
) -> eyre::Result<BTreeMap<Epoch, B256>> {
    sealed
        .map(|epoch| {
            let path = epoch_pack_path(datadir, epoch);
            let bytes = std::fs::read(&path)
                .map_err(|e| eyre::eyre!("reading sealed pack {}: {e}", path.display()))?;
            Ok((epoch, keccak256(bytes)))
        })
        .collect()
}

/// Assert every pack sealed before the kill survived the restart untouched and still imports.
///
/// Two independent checks per epoch:
///
/// 1. `validate_pack_file` walks the whole `data` stream applying the integrity rules
///    `ConsensusPack::stream_import` applies to a pack arriving from a peer, so a pack that passes
///    here is one a syncing node would accept. `records` supplies the previous epoch's certified
///    [`EpochRecord`], which additionally turns on the epoch-linkage checks (start consensus
///    number, genesis exec state, the committee the previous record commits to) and anchors the
///    first header's `parent_hash`.
/// 2. Byte equality against the pre-restart fingerprint. This is the load-bearing half: a pack can
///    be internally valid and still have been rewritten — re-imported from a peer, or healed —
///    which would mask the regression this pins, that restarting into an existing datadir leaves
///    closed epochs alone.
///
/// Decoding a pack reaches three epoch-gated layouts: the [`tn_types::Committee`] in its
/// `EpochMeta` record, selected by the committee's own epoch against the multi-workers fork
/// ([`tn_types::forks::multi_workers_fork_active`]); the seed signature of every header nested in
/// a `ConsensusHeader`, selected against the seed-signature fork
/// ([`tn_types::forks::seed_signature_active`]); and the millisecond fields, each header's
/// `created_at_millis` and each sub-dag's
/// `commit_timestamp_millis`, selected against the sub-second-timestamp fork
/// (`tn_types::forks::subsecond_timestamp_active`). This process therefore has to sit on the same
/// fork epochs the nodes wrote under for ALL THREE, which is what [`pin_fork_epochs`] arranges.
fn assert_sealed_packs_unchanged(
    datadir: &Path,
    fingerprints: &BTreeMap<Epoch, B256>,
    records: &BTreeMap<Epoch, EpochRecord>,
) -> eyre::Result<()> {
    for (&epoch, before) in fingerprints {
        let path = epoch_pack_path(datadir, epoch);
        // epoch 0 has no predecessor; the validator anchors it on the default consensus header
        let previous = epoch.checked_sub(1).and_then(|prev| records.get(&prev));
        let report = validate_pack_file(&path, epoch, previous)
            .map_err(|e| eyre::eyre!("sealed pack {} did not open: {e}", path.display()))?;
        eyre::ensure!(
            report.verdict == Verdict::Valid,
            "sealed pack {} is invalid after the restart: {:?}",
            path.display(),
            report.issues,
        );

        let after = keccak256(
            std::fs::read(&path)
                .map_err(|e| eyre::eyre!("re-reading sealed pack {}: {e}", path.display()))?,
        );
        eyre::ensure!(
            after == *before,
            "sealed pack {} changed across the restart: {before} -> {after}",
            path.display(),
        );
        info!(
            target: "epoch-test",
            epoch,
            consensus_headers = report.consensus_count,
            batches = report.batch_count,
            "sealed epoch pack survived the restart unchanged",
        );
    }
    Ok(())
}

/// Index in the epoch-sync network of the node [`run_epoch_sync_scenario`] kills and restarts
/// (validator-3).
const SYNC_RESTARTED_NODE: usize = 2;

/// A finished [`run_epoch_sync_scenario`] whose network is still running, so the caller can query
/// the nodes after the scenario's own checks passed.
///
/// Dropping it stops every node and then deletes their datadirs: fields drop in declaration order,
/// so `guard` goes before `_temp_dir`.
struct EpochSyncRun {
    /// The epochs the scenario observed around the kill and restart.
    epochs: SyncEpochs,
    /// Index into `committee` and `endpoints` of the node that was killed and restarted.
    restarted: usize,
    /// Every node's endpoints in `committee` order, with the restarted node's new ports.
    endpoints: Vec<NodeEndpoints>,
    /// Every node's datadir name and execution address; validator-1 is first.
    committee: Vec<(&'static str, Address)>,
    /// Owns the node processes, the restarted node's new one included.
    guard: ProcessGuard,
    /// Holds every node's datadir until the run is dropped.
    _temp_dir: tempfile::TempDir,
}

/// Spin up the epoch-sync network, run [`test_epoch_sync_inner`] against it, and hand the still
/// running network back (see [`EpochSyncRun`]).
///
/// `test` names both the temp-dir prefix and the `test_logs/` directory, so callers running the
/// same scenario under different fork epochs keep separate node logs. Keep it short: every node's
/// IPC socket path is built under the temp dir, and a unix socket path is capped at ~104 bytes.
async fn run_epoch_sync_scenario(test: &str) -> eyre::Result<EpochSyncRun> {
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
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
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
    let (procs, mut endpoints) = start_nodes(temp_path, &committee, test, 1)?;
    // Guard ensures processes are killed on drop (normal return, error, or panic).
    let mut guard = ProcessGuard::new(procs);

    let epochs = test_epoch_sync_inner(
        &mut guard,
        SYNC_RESTARTED_NODE,
        &[committee[SYNC_RESTARTED_NODE]],
        &committee[..],
        temp_path,
        test,
        &mut endpoints,
    )
    .await?;

    Ok(EpochSyncRun {
        epochs,
        restarted: SYNC_RESTARTED_NODE,
        endpoints,
        committee,
        guard,
        _temp_dir: temp_dir,
    })
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

/// Submit a governance update to `WorkerConfigs` and wait for its transaction to confirm.
///
/// A transaction crossing an epoch boundary can be reinjected into the next epoch, so allow
/// two epoch durations plus startup slack for confirmation. Callers verify the resulting state.
async fn send_worker_config_update<P: Provider>(
    provider: &P,
    governance: &mut TransactionFactory,
    chain: Arc<RethChainSpec>,
    calldata: Bytes,
) -> eyre::Result<()> {
    let tx = governance.create_eip1559_encoded(
        chain,
        None,
        100,
        Some(WORKER_CONFIGS_ADDRESS),
        U256::ZERO,
        calldata,
    );
    let pending = provider.send_raw_transaction(&tx).await?;
    timeout(Duration::from_secs(EPOCH_DURATION * 2 + 11), pending.watch()).await??;
    Ok(())
}

/// Change the worker count and prove every original node closes an epoch under the new count.
///
/// The epoch observed after confirmation may already include the update. Waiting through its
/// successor guarantees a complete epoch under the changed count regardless of transaction timing.
async fn change_worker_count_across_epoch_boundary<P: Provider>(
    provider: &P,
    governance: &mut TransactionFactory,
    chain: Arc<RethChainSpec>,
    endpoints: &[NodeEndpoints],
    worker_count: u16,
) -> eyre::Result<()> {
    send_worker_config_update(
        provider,
        governance,
        chain,
        WorkerConfigs::setNumWorkersCall { numWorkers_: worker_count }.abi_encode().into(),
    )
    .await?;
    let configs = WorkerConfigs::new(WORKER_CONFIGS_ADDRESS, provider);
    eyre::ensure!(
        configs.numWorkers().call().await? == worker_count,
        "governance did not set the worker count to {worker_count}",
    );
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, provider);
    let observed_epoch = registry.getCurrentEpochInfo().call().await?.epochId;
    let changed_epoch = observed_epoch.saturating_add(1);
    let following_epoch = changed_epoch.saturating_add(1);

    futures::future::try_join_all(endpoints.iter().map(|endpoint| async move {
        let node = ProviderBuilder::new().connect_http(endpoint.http_url.parse()?);
        let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &node);
        wait_until(
            Duration::from_secs(EPOCH_DURATION * 8),
            &format!(
                "{} to close epoch {changed_epoch} with {worker_count} workers",
                endpoint.http_url,
            ),
            || async {
                Ok(registry.getCurrentEpochInfo().call().await?.epochId >= following_epoch)
            },
        )
        .await?;
        let record = fetch_verified_epoch_record(&endpoint.http_url, changed_epoch, 60).await?;
        eyre::ensure!(record.epoch == changed_epoch, "node served the wrong epoch record");
        Ok::<(), eyre::Report>(())
    }))
    .await?;
    Ok(())
}

/// Provision worker 1 and its bootstrap addresses without changing the one-worker genesis.
///
/// Preserve the identities in the one-worker genesis while adding local capacity for worker 1.
/// Share both workers' addresses before starting any processes.
fn provision_second_workers(temp_path: &Path, committee: &[(&str, Address)]) -> eyre::Result<()> {
    let bootstrap_peers = committee
        .iter()
        .map(|(name, _)| {
            let dir = temp_path.join(name);
            let path = dir.join("node-info.yaml");
            let keys = KeyConfig::read_config(&dir, Some(NODE_PASSWORD.to_string()))?;
            let mut info: NodeInfo = Config::load_from_path(&path, ConfigFmt::YAML)?;
            eyre::ensure!(info.p2p_info.num_workers() == 1, "fixture must start with one worker");
            let port = get_available_udp_port("127.0.0.1")
                .ok_or_else(|| eyre::eyre!("no UDP port available for worker 1"))?;
            info.p2p_info.workers.push(P2pNode {
                network_key: keys.worker_network_public_key(1),
                network_address: format!("/ip4/127.0.0.1/udp/{port}/quic-v1").parse()?,
                rpc: None,
            });
            Config::write_to_path(path, &info, ConfigFmt::YAML)?;
            Ok((
                info.bls_public_key,
                BootstrapServer::new(info.p2p_info.primary, info.p2p_info.workers),
            ))
        })
        .collect::<eyre::Result<BTreeMap<_, _>>>()?;
    let network: NetworkConfig =
        serde_json::from_value(serde_json::json!({ "bootstrap_peers": bootstrap_peers }))?;
    committee.iter().try_for_each(|(name, _)| network.write_config(&temp_path.join(name)))
}

/// Reserve worker 0's HTTP port and worker 1's derived port (200 lower) together.
fn reserve_two_worker_http_ports() -> eyre::Result<(std::net::TcpListener, std::net::TcpListener)> {
    (0..32)
        .find_map(|_| {
            let first = std::net::TcpListener::bind(("127.0.0.1", 0)).ok()?;
            let second_port = first.local_addr().ok()?.port().checked_sub(200)?;
            std::net::TcpListener::bind(("127.0.0.1", second_port))
                .ok()
                .map(|second| (first, second))
        })
        .ok_or_else(|| eyre::eyre!("could not reserve both workers' HTTP ports"))
}

/// An observer's worker-1 pool forwards through the worker-1 endpoints supplied by keytool.
/// Worker 0 advertises no endpoint, so selecting the wrong record cannot make this test pass.
#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
async fn test_epoch_observer_forwards_to_second_worker() -> eyre::Result<()> {
    use clap::Parser as _;
    use telcoin_network_cli::keytool::KeyArgs;
    use tn_types::test_utils::CommandParser;

    let _permit = super::common::acquire_test_permit();
    pin_fork_epochs(Some(0), None, None, None);
    let temp_dir = tempfile::TempDir::with_prefix("worker_rpc")?;
    e2e_tests::config_local_testnet_with_worker_fee_configs(
        temp_dir.path(),
        Some("restart_test".to_string()),
        None,
        None,
        &["0:1:7", "1:1:7"],
    )?;
    let nodes = (0..5)
        .map(|index| {
            let name =
                if index < 4 { format!("validator-{}", index + 1) } else { "observer".to_string() };
            reserve_two_worker_http_ports().map(|(first, second)| (name, first, second))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    nodes.iter().filter(|(name, _, _)| name != "observer").try_for_each(
        |(name, _, second)| -> eyre::Result<()> {
            let url = format!("http://127.0.0.1:{}", second.local_addr()?.port());
            let command = CommandParser::<KeyArgs>::try_parse_from([
                "tn",
                "set-rpc",
                "--worker-id",
                "1",
                "--http",
                &url,
            ])?;
            command.args.execute(temp_dir.path().join(name), None)
        },
    )?;

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let endpoints = nodes
        .into_iter()
        .map(|(name, first, second)| -> eyre::Result<_> {
            let base_port = first.local_addr()?.port();
            let url = format!("http://127.0.0.1:{}", second.local_addr()?.port());
            drop((first, second));
            let mut command = bin.command();
            command
                .env("TN_BLS_PASSPHRASE", "restart_test")
                .arg("node")
                .arg("--datadir")
                .arg(temp_dir.path().join(&name))
                .arg("--http")
                .arg("--http.port")
                .arg(base_port.to_string())
                .arg("--ipcpath")
                .arg(temp_dir.path().join(format!("{name}.ipc")))
                .arg("--node-name")
                .arg(format!("worker-rpc-{name}"));
            guard.push(command.spawn()?);
            Ok((name, url))
        })
        .collect::<eyre::Result<Vec<_>>>()?;
    // Empty non-epoch-closing outputs skip execution, so the height stays at genesis until the
    // forwarded transaction lands. Wait only for each worker-1 RPC server to answer.
    futures::future::try_join_all(endpoints.iter().map(|(_, url)| async move {
        let provider = ProviderBuilder::new().connect_http(url.parse()?);
        wait_until(std::time::Duration::from_secs(60), "worker 1 RPC to answer", || async {
            Ok(provider.get_block_number().await.is_ok())
        })
        .await
    }))
    .await?;

    let observer_url = endpoints
        .iter()
        .find(|(name, _)| name == "observer")
        .map(|(_, url)| url)
        .ok_or_else(|| eyre::eyre!("missing observer endpoint"))?;
    let validator_url = endpoints
        .iter()
        .find(|(name, _)| name == "validator-1")
        .map(|(_, url)| url)
        .ok_or_else(|| eyre::eyre!("missing validator endpoint"))?;
    let hash = super::common::send_tel(
        observer_url,
        &super::common::get_key("test-source"),
        Address::from_slice(&[0x77; 20]),
        1_000_000_000_000_000,
        250,
        21_000,
        0,
    )?
    .parse()?;
    let validator = ProviderBuilder::new().connect_http(validator_url.parse()?);
    wait_until(
        std::time::Duration::from_secs(60),
        "worker 1 observer transaction to be included",
        || async {
            validator
                .get_transaction_receipt(hash)
                .await
                .map(|receipt| receipt.is_some())
                .map_err(Into::into)
        },
    )
    .await?;
    let receipt = validator
        .get_transaction_receipt(hash)
        .await?
        .ok_or_else(|| eyre::eyre!("forwarded transaction receipt disappeared"))?;
    eyre::ensure!(receipt.status(), "forwarded transaction reverted");
    guard.kill_all();
    Ok(())
}

/// Governance can grow and shrink the protocol worker count while validators keep running.
///
/// Every node provisions two workers while genesis activates only one. The original processes
/// must start worker 1 at epoch entry, close epochs with both workers, and accept a decrease
/// back to one worker without restarting. A local capacity shortfall is covered by node tests.
#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
async fn test_epoch_worker_count_changes_keep_nodes_running() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    pin_fork_epochs(Some(0), None, None, None);

    let committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];
    let extra_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut governance = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let temp_dir = tempfile::TempDir::with_prefix("worker_count")?;
    let genesis = create_genesis_for_test(
        temp_dir.path(),
        (NEW_VALIDATOR, extra_validator.address()),
        governance.address(),
        &committee,
        EPOCH_DURATION,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    provision_second_workers(temp_dir.path(), &committee)?;
    let (children, endpoints) = start_nodes(temp_dir.path(), &committee, "worker_count", 1)?;
    let mut guard = ProcessGuard::new(children);
    // A quorum can commit governance before the final validator has opened its RPC listener.
    futures::future::try_join_all(endpoints.iter().map(|endpoint| async move {
        let node = ProviderBuilder::new().connect_http(endpoint.http_url.parse()?);
        wait_for_rpc(&node).await
    }))
    .await?;
    let first = endpoints.first().ok_or_else(|| eyre::eyre!("no validator endpoints"))?;
    let provider = ProviderBuilder::new().connect_http(first.http_url.parse()?);
    let configs = WorkerConfigs::new(WORKER_CONFIGS_ADDRESS, &provider);
    eyre::ensure!(configs.numWorkers().call().await? == 1, "genesis must use one worker");

    // The contract requires worker 1's configuration before governance raises the count.
    send_worker_config_update(
        &provider,
        &mut governance,
        chain.clone(),
        WorkerConfigs::setWorkerConfigCall {
            workerId: 1,
            strategy: 0,
            value: 30_000_000,
            data: Default::default(),
        }
        .abi_encode()
        .into(),
    )
    .await?;
    change_worker_count_across_epoch_boundary(
        &provider,
        &mut governance,
        chain.clone(),
        &endpoints,
        2,
    )
    .await?;
    change_worker_count_across_epoch_boundary(&provider, &mut governance, chain, &endpoints, 1)
        .await?;

    guard.kill_all();
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that sync works to fill in missing epochs.
async fn test_epoch_sync() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // whatever fork epochs the lane stated, defaults otherwise - this test is about sync, not
    // about any fork, but the harness still decodes pack bytes and must agree with the nodes
    pin_fork_epochs(None, None, None, None);

    run_epoch_sync_scenario("epoch_sync").await.map(|_run| ())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that an epoch pack archive spanning the multi-workers fork boundary (issue #554)
/// survives a restart.
///
/// The same kill/restart scenario as [`test_epoch_sync`], with the fork pinned at
/// [`CROSS_FORK_EPOCH`] so one datadir holds both committee layouts: epoch 0 written in the legacy
/// single-worker layout, every later epoch in the multi-worker one. The restarted node has to read
/// its own history back across that boundary to decide which epochs it still needs, and step 8
/// then decodes both layouts again from the harness.
async fn test_epoch_sync_across_multi_workers_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // forced rather than inherited: this test's claim is a crossing at a known multi-workers
    // epoch, so it states that fork point even when the lane exported a different one. the
    // other pins still follow the lane - this test makes no claim about them.
    pin_fork_epochs(Some(CROSS_FORK_EPOCH), None, None, None);

    let sealed = run_epoch_sync_scenario("epoch_sync_fork").await?.epochs.sealed;

    // The revalidated packs span both layouts only if the fork epoch sits strictly inside them.
    // Assert it rather than trusting the arithmetic in `CROSS_FORK_EPOCH`: a shorter run, or a
    // wider safety margin in `sealed_epochs`, would otherwise quietly reduce this to the
    // single-layout test above.
    assert!(
        sealed.start < CROSS_FORK_EPOCH && CROSS_FORK_EPOCH < sealed.end,
        "sealed epochs {sealed:?} do not straddle the multi-workers fork at \
         {CROSS_FORK_EPOCH}: the restart proved only one committee layout"
    );

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that an epoch pack archive spanning the leader-seeded-ordering fork boundary (#1260)
/// survives a restart.
///
/// The same kill/restart scenario as [`test_epoch_sync`], with the ordering fork pinned at
/// [`CROSS_FORK_EPOCH`] so one datadir holds commits linearized both ways: epoch 0 sealed under
/// the legacy DFS discovery order, every later epoch under the seeded tie-break. The
/// seed-signature fork is forced active from genesis because the gate
/// (`tn_types::forks::leader_seeded_ordering_active`) conjoins it fail-closed: with the seed fork
/// left on the lane's dormant default the pinned fork point would be inert, every epoch would
/// seal in the legacy order, and this would quietly reduce to [`test_epoch_sync`].
///
/// Unlike the multi-workers fork this one changes no serialized layout, only the order of commits
/// inside a sealed pack, so step 8's pack decode is layout-identical on both sides of the
/// boundary and needs no ordering-fork pin of its own. The kill/restart checks are still the
/// load-bearing ones: the restarted node re-reads its own history across the boundary to decide
/// which epochs it needs, back-fills what it missed, and the packs sealed under each order must
/// revalidate and survive byte-for-byte.
async fn test_epoch_sync_across_leader_seeded_ordering_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // both forced rather than inherited: this test's claim is a crossing at a known
    // ordering-fork epoch with the seed fork active beneath it (the conjunct above), so it
    // states both fork points even when the lane exported different ones. the multi-workers and
    // sub-second-timestamp pins still follow the lane - this test makes no claim about committee
    // layout or timestamp precision.
    pin_fork_epochs(None, Some(0), Some(CROSS_FORK_EPOCH), None);

    let sealed = run_epoch_sync_scenario("epoch_sync_seeded").await?.epochs.sealed;

    // The revalidated packs span both commit orders only if the fork epoch sits strictly inside
    // them. Assert it rather than trusting the arithmetic in `CROSS_FORK_EPOCH`: a shorter run,
    // or a wider safety margin in `sealed_epochs`, would otherwise quietly reduce this to the
    // single-order test above.
    assert!(
        sealed.start < CROSS_FORK_EPOCH && CROSS_FORK_EPOCH < sealed.end,
        "sealed epochs {sealed:?} do not straddle the leader-seeded-ordering fork at \
         {CROSS_FORK_EPOCH}: the restart proved only one commit order"
    );

    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that a node restarted on a consensus archive spanning the sub-second timestamp fork reads
/// commit times on both sides of it the way a node that never stopped does.
///
/// The same kill/restart scenario as [`test_epoch_sync`], with the sub-second fork pinned at
/// [`CROSS_FORK_EPOCH`] and the seed-signature fork active from genesis, because the gate
/// (`tn_types::forks::subsecond_timestamp_active`) conjoins it fail-closed. The killed node's
/// sealed packs then hold epoch 0 in the whole-second layout and epoch 1 onward in the millisecond
/// one. The restart re-reads that archive across the layout change to decide which epochs it still
/// needs, imports the post-fork packs of the epochs it slept through from its peers, and step 8
/// decodes both layouts again from the harness.
///
/// With the network still up, [`assert_restarted_node_reads_both_layouts`] then compares the
/// restarted node with validator-1 on every epoch's final block and on every block both hold. Its
/// per-epoch `subSecond` check rests on two facts. `tn_getBlockTimestampMillis` computes the flag
/// from the leader epoch of the consensus header the block was executed from
/// (`BlockTimestampMillis::with_consensus` and `ConsensusCommitTime::from` in
/// `crates/execution/tn-rpc/src/rpc_ext.rs`). And a record's final block is executed from that
/// epoch's own closing commit: `build_epoch_record` (`crates/node/src/manager/node/close_epoch.rs`)
/// commits the epoch-closing block as `final_state`, and that block's nonce carries the concluding
/// epoch (`deconstruct_nonce(ctx.nonce).0` in `crates/tn-reth/src/evm/block.rs`), which is the
/// leader's epoch (`TNPayload::nonce` in `crates/tn-reth/src/payload.rs`).
async fn test_epoch_sync_across_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // both forced rather than inherited: the claim is a crossing at a known sub-second epoch with
    // the seed fork active beneath it (the conjunct above), so it states both fork points even
    // when the lane exported different ones. the multi-workers and leader-seeded pins follow the
    // lane
    pin_fork_epochs(None, Some(0), None, Some(CROSS_FORK_EPOCH));

    let mut run = run_epoch_sync_scenario("epoch_sync_ms").await?;

    // The revalidated packs span both layouts only if the fork epoch sits strictly inside them.
    // Assert it rather than trusting the arithmetic in `CROSS_FORK_EPOCH`: a shorter run, or a
    // wider safety margin in `sealed_epochs`, would otherwise quietly reduce this to a
    // single-layout restart.
    let sealed = &run.epochs.sealed;
    assert!(
        sealed.start < CROSS_FORK_EPOCH && CROSS_FORK_EPOCH < sealed.end,
        "sealed epochs {sealed:?} do not straddle the sub-second fork at {CROSS_FORK_EPOCH}: the \
         restart proved only one commit-time layout"
    );

    assert_restarted_node_reads_both_layouts(&run, CROSS_FORK_EPOCH).await?;

    run.guard.kill_all();
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that a node down while the sub-second timestamp fork activates comes back on the
/// millisecond layout from its peers' packs alone.
///
/// The same kill/restart scenario as [`test_epoch_sync_across_subsecond_fork`], with the fork
/// pinned at [`SUBSECOND_FORK_WHILE_DOWN_EPOCH`], above the epoch the node is killed in and no
/// later than the one it restarts in. Its own archive is then entirely pre-fork: every post-fork
/// pack it holds was imported from peers, and the first post-fork commits it executes are ones it
/// did not vote in. Both epochs are asserted from what the scenario observed rather than assumed
/// from the constant.
///
/// With the network still up, [`assert_restarted_node_reads_both_layouts`] runs the same
/// comparison with validator-1 as the test above (see its doc for why `subSecond` follows the
/// epoch of the record's final block).
async fn test_epoch_sync_subsecond_fork_activates_while_down() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // both forced rather than inherited, for the reasons given in
    // `test_epoch_sync_across_subsecond_fork`
    pin_fork_epochs(None, Some(0), None, Some(SUBSECOND_FORK_WHILE_DOWN_EPOCH));

    let mut run = run_epoch_sync_scenario("epoch_sync_ms_down").await?;

    let restarted = run.committee[run.restarted].0;
    let SyncEpochs { epoch_at_kill, epoch_at_restart, .. } = run.epochs;
    assert!(
        epoch_at_kill < SUBSECOND_FORK_WHILE_DOWN_EPOCH,
        "{restarted} was killed in epoch {epoch_at_kill}, not before the sub-second fork at \
         {SUBSECOND_FORK_WHILE_DOWN_EPOCH}: its own archive may hold post-fork packs, so the run \
         did not prove a node picking the fork up from its peers"
    );
    assert!(
        SUBSECOND_FORK_WHILE_DOWN_EPOCH <= epoch_at_restart,
        "{restarted} was restarted in epoch {epoch_at_restart}, before the sub-second fork at \
         {SUBSECOND_FORK_WHILE_DOWN_EPOCH}: the fork did not activate while it was down, so the \
         run did not prove a node picking the fork up from its peers"
    );

    assert_restarted_node_reads_both_layouts(&run, SUBSECOND_FORK_WHILE_DOWN_EPOCH).await?;

    run.guard.kill_all();
    Ok(())
}

/// Assert the node `run` restarted reads commit times on both sides of the sub-second fork at
/// leader epoch `fork_epoch` the way validator-1 does.
///
/// For every epoch the scenario verified, both nodes must serve the same certified record, and the
/// record's final block must report the same commit time on both, close the epoch, carry the hash
/// the record commits to, and report `subSecond` exactly when the epoch is at or past
/// `fork_epoch`. Then each node's blocks from genesis to its head are walked with
/// [`walk_block_commit_times`] and compared height by height. On the restarted node every block up
/// to the final block of epoch `fork_epoch - 1` must commit in whole seconds and report `subSecond`
/// false, every later block must report it true, and at least one later block must commit off a
/// whole second, so the millisecond layout is actually read back rather than truncated.
///
/// The scenario sends no transactions, so a node builds one block per epoch, for the epoch's
/// closing commit, and skips every other empty commit; the walk is short.
async fn assert_restarted_node_reads_both_layouts(
    run: &EpochSyncRun,
    fork_epoch: Epoch,
) -> eyre::Result<()> {
    let latest_epoch = run.epochs.latest_epoch;
    eyre::ensure!(
        0 < fork_epoch && fork_epoch <= latest_epoch,
        "the sub-second fork at {fork_epoch} is not inside the verified epochs 0..={latest_epoch}: \
         the run certified epochs on one side of it only"
    );
    let reference_url = &run.endpoints[0].http_url;
    let restarted_url = &run.endpoints[run.restarted].http_url;
    let reference_name = run.committee[0].0;
    let restarted_name = run.committee[run.restarted].0;
    let reference = ProviderBuilder::new().connect_http(reference_url.parse()?);
    let restarted = ProviderBuilder::new().connect_http(restarted_url.parse()?);

    // the final block of every verified epoch, which also places each walked block in its epoch
    let mut final_blocks = BTreeMap::new();
    for epoch in 0..=latest_epoch {
        // already fetched and verified by the scenario, so these answer at once
        let record_timeout = (EPOCH_DURATION * 6).max(60);
        let record = fetch_verified_epoch_record(reference_url, epoch, record_timeout).await?;
        let restarted_record =
            fetch_verified_epoch_record(restarted_url, epoch, record_timeout).await?;
        eyre::ensure!(
            restarted_record == record,
            "{restarted_name} serves a different certified record for epoch {epoch} than \
             {reference_name}: {restarted_record:?} vs {record:?}"
        );
        let block = record.final_state.number;
        let expected = block_commit_time(&reference, reference_name, block).await?;
        let actual = block_commit_time(&restarted, restarted_name, block).await?;
        eyre::ensure!(
            actual == expected,
            "{restarted_name} reports {actual:?} for block {block}, the final block of epoch \
             {epoch}, and {reference_name} reports {expected:?}"
        );
        eyre::ensure!(
            actual.block_hash == record.final_state.hash && actual.closes_epoch,
            "block {block} on {restarted_name} is not the epoch-closing block the epoch {epoch} \
             record commits to: {actual:?} vs {:?}",
            record.final_state
        );
        eyre::ensure!(
            actual.sub_second == (epoch >= fork_epoch),
            "the final block of epoch {epoch} on {restarted_name} reports subSecond {} with the \
             sub-second fork at {fork_epoch}",
            actual.sub_second
        );
        info!(
            target: "epoch-test",
            epoch,
            block,
            timestamp_millis = actual.timestamp_millis,
            sub_second = actual.sub_second,
            "restarted node agrees on the epoch's final commit time",
        );
        final_blocks.insert(epoch, block);
    }

    let mut walks = Vec::with_capacity(2);
    for (provider, name) in [(&reference, reference_name), (&restarted, restarted_name)] {
        let head =
            timeout(RPC_REQUEST_TIMEOUT, provider.get_block_number()).await.map_err(|_| {
                eyre::eyre!("{name} did not answer eth_blockNumber within {RPC_REQUEST_TIMEOUT:?}")
            })??;
        walks.push(walk_block_commit_times(provider, name, 0..=head).await?);
    }
    assert_nodes_agree_on_commit_times(
        &walks,
        &[reference_name.to_string(), restarted_name.to_string()],
    )?;

    let last_pre_fork_block = final_blocks[&(fork_epoch - 1)];
    let mut pre_fork_blocks = 0usize;
    let mut post_fork_blocks = 0usize;
    let mut off_second_blocks = 0usize;
    for commit in &walks[1] {
        let post_fork = commit.block_number > last_pre_fork_block;
        eyre::ensure!(
            commit.sub_second == post_fork,
            "block {} on {restarted_name} reports subSecond {}, but the last block before the \
             sub-second fork at {fork_epoch} is {last_pre_fork_block}",
            commit.block_number,
            commit.sub_second
        );
        if post_fork {
            post_fork_blocks += 1;
            if commit.timestamp_millis % 1000 != 0 {
                off_second_blocks += 1;
            }
        } else {
            pre_fork_blocks += 1;
            eyre::ensure!(
                commit.timestamp_millis % 1000 == 0,
                "pre-fork block {} on {restarted_name} commits at {} ms, off a whole second",
                commit.block_number,
                commit.timestamp_millis
            );
        }
    }
    info!(
        target: "epoch-test",
        node = restarted_name,
        last_pre_fork_block,
        pre_fork_blocks,
        post_fork_blocks,
        off_second_blocks,
        "restarted node's commit times on both sides of the sub-second fork",
    );
    eyre::ensure!(
        off_second_blocks > 0,
        "none of the {post_fork_blocks} post-fork blocks {restarted_name} served commits off a \
         whole second: it never read a millisecond commit time back"
    );
    Ok(())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test consensus commit times across the sub-second timestamp fork, over RPC and on disk.
///
/// The fork is pinned at [`SUBSECOND_FORK_EPOCH`] (epochs 0 and 1 pre-fork) with the seed-signature
/// fork active from genesis, and a four-validator network at the harness's 500/250/250 ms cadence
/// runs under light transaction load until [`SUBSECOND_TARGET_EPOCH`] opens. Then, on every node:
///
/// - every execution block keeps a whole-second `timestamp` that never decreases, including across
///   each epoch boundary, and equals its consensus commit time floored to seconds;
/// - every block after genesis names its consensus header as its `parentBeaconBlockRoot`, and
///   blocks naming the same header report the same commit time (the walk does not require any two
///   to share one);
/// - the node served post-fork blocks: at least five that do not close an epoch, and a pair of
///   consecutive blocks from different consensus headers with the same `timestamp`;
/// - the engine never clamped an EVM timestamp ([`EVM_TIMESTAMP_CLAMPED_SERIES`] reads 0);
/// - nodes agree on every block and commit time they all hold;
/// - the node serves a certified record for every epoch below [`SUBSECOND_TARGET_EPOCH`] and has
///   executed the final block each record names, with the hash the record commits to.
///
/// Validator-1 is then stopped and its consensus chain walked on disk (see
/// [`assert_consensus_commit_times`]), and every block it served is matched to the commit time its
/// consensus header holds there.
async fn test_epoch_subsecond_timestamps_across_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // both forced rather than inherited: the claim is a crossing at a known sub-second epoch, and
    // the gate (`tn_types::forks::subsecond_timestamp_active`) conjoins the seed fork fail-closed,
    // so a dormant seed fork would keep every epoch on whole seconds and quietly reduce this to a
    // single-layout run. the multi-workers and leader-seeded pins follow the lane
    pin_fork_epochs(None, Some(0), None, Some(SUBSECOND_FORK_EPOCH));

    // short on purpose: node IPC socket paths are built under the temp dir
    let test = "subsecond_fork";
    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();

    // one funded sender per validator, so every round of load reaches every worker
    let mut senders: Vec<TransactionFactory> = (0..4u64)
        .map(|i| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(0x5ec0 + i)))
        .collect();
    let funding = U256::from(parse_ether("1_000")?);
    let accounts = senders
        .iter()
        .map(|sender| (sender.address(), GenesisAccount::default().with_balance(funding)))
        .collect();
    config_local_testnet_with_epoch_duration(
        temp_path,
        Some("restart_test".to_string()),
        Some(accounts),
        Some(EPOCH_DURATION as u32),
    )?;
    let genesis: Genesis = Config::load_from_path(
        &temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut rpc_urls = Vec::new();
    let mut metrics_addrs = Vec::new();
    for instance in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
        let metrics_port =
            get_available_tcp_port("127.0.0.1").expect("metrics port assigned by host");
        let metrics_addr = format!("127.0.0.1:{metrics_port}");
        guard.push(start_validator_with_args(
            instance,
            bin,
            temp_path,
            rpc_port,
            test,
            0,
            &["--metrics", &metrics_addr],
        ));
        rpc_urls.push(format!("http://127.0.0.1:{rpc_port}"));
        metrics_addrs.push(metrics_addr);
    }
    let providers = rpc_urls
        .iter()
        .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
        .collect::<eyre::Result<Vec<_>>>()?;
    futures::future::try_join_all(providers.iter().map(|provider| wait_for_rpc(provider))).await?;

    // the load only runs while the epochs roll; dropping it with the finished race stops it
    let reached = tokio::select! {
        reached = loop_epochs(0, SUBSECOND_TARGET_EPOCH, &rpc_urls[0], EPOCH_DURATION) => reached?,
        never = drive_light_tx_load(&providers, &mut senders, chain) => match never {},
    };
    eyre::ensure!(
        reached >= SUBSECOND_TARGET_EPOCH,
        "network stopped at epoch {reached}, short of {SUBSECOND_TARGET_EPOCH}"
    );
    info!(target: "epoch-test", reached, "sub-second fork run reached its target epoch");

    let mut served = Vec::with_capacity(providers.len());
    for (provider, url) in providers.iter().zip(&rpc_urls) {
        served.push(assert_block_commit_times(provider, url).await?);
    }
    assert_nodes_agree_on_commit_times(&served, &rpc_urls)?;

    // every node is still the process it started as, so its counter covers the whole run. the
    // scrape is blocking socket I/O with sleeps between retries, so it runs off the runtime worker
    for (addr, url) in metrics_addrs.iter().zip(&rpc_urls) {
        let clamped = tokio::task::block_in_place(|| {
            scrape_metric_value(addr, EVM_TIMESTAMP_CLAMPED_SERIES)
        })?;
        eyre::ensure!(
            clamped == 0.0,
            "{url} clamped {clamped} EVM timestamps up to their parent's: consensus let commit \
             time go backwards in a post-fork epoch (epoch 0 is pre-fork here, so validator clocks \
             lagging the genesis timestamp cannot explain it; node logs under test_logs/{test}/ \
             carry the \"evm timestamp clamped to parent\" warnings)"
        );
    }

    // the walk above compares nodes only on the blocks they all hold, and only validator-1's
    // epochs were polled. require every node to serve a certified record for each epoch the run
    // closed and to have executed that record's final block with the hash it commits to, so each
    // node went through every seam, the fork seam included, and ended each epoch on the same
    // block. the helper reads only `http_url`. eject.rs gives each record 60 s (its 10 s epochs
    // times 6); certificates take a fixed quorum-voting time that does not shrink with this
    // file's 5 s epochs, so keep the same 60 s floor as the other record polls here
    let endpoints: Vec<NodeEndpoints> = rpc_urls
        .iter()
        .map(|url| NodeEndpoints {
            http_url: url.clone(),
            ws_url: String::new(),
            ipc_path: String::new(),
        })
        .collect();
    assert_epoch_records_verify(
        &endpoints,
        0..=SUBSECOND_TARGET_EPOCH - 1,
        (EPOCH_DURATION * 6).max(60),
    )
    .await?;

    // stop validator-1 so its consensus chain can be opened from this process
    let mut validator_1 = guard.take(0).ok_or_else(|| eyre::eyre!("validator-1 is not running"))?;
    super::common::kill_child(&mut validator_1);
    eyre::ensure!(
        providers[0].get_chain_id().await.is_err(),
        "validator-1 still answers RPC after being stopped"
    );
    let headers = read_consensus_headers(&temp_path.join("validator-1")).await?;
    let commits = assert_consensus_commit_times(&headers, SUBSECOND_FORK_EPOCH)?;
    assert_blocks_match_consensus(
        &served[0],
        &commits,
        SUBSECOND_FORK_EPOCH,
        0..=SUBSECOND_TARGET_EPOCH - 1,
    )?;

    guard.kill_all();
    Ok(())
}

/// Walk every execution block `provider` serves from genesis to its current head with
/// [`walk_block_commit_times`], which checks each block's sub-second timestamp invariants, then
/// require the walk to contain the cases those checks exist for, and return each block's commit
/// time in block order.
///
/// It fails unless the node served at least one block with a sub-second commit time, so it
/// executed past the fork seam; at least five post-fork blocks that do not close an epoch, which
/// only the light load produces (without transactions the node builds one block for an epoch's
/// closing commit and skips every other empty commit); and at least one pair of consecutive
/// post-fork blocks from different consensus headers with the same `timestamp`, the case where
/// only `timestampMillis` orders two commits.
async fn assert_block_commit_times<P: Provider>(
    provider: &P,
    node: &str,
) -> eyre::Result<Vec<BlockCommitTime>> {
    const MIN_POST_FORK_OPEN_BLOCKS: usize = 5;
    let head =
        timeout(RPC_REQUEST_TIMEOUT, provider.get_block_number()).await.map_err(|_| {
            eyre::eyre!("{node} did not answer eth_blockNumber within {RPC_REQUEST_TIMEOUT:?}")
        })??;
    let served = walk_block_commit_times(provider, node, 0..=head).await?;

    let sub_second_blocks = served.iter().filter(|commit| commit.sub_second).count();
    let post_fork_open_blocks =
        served.iter().filter(|commit| commit.sub_second && !commit.closes_epoch).count();
    let closing_blocks = served.iter().filter(|commit| commit.closes_epoch).count();
    let same_second_block_pairs = served
        .windows(2)
        .filter(|pair| match pair {
            [parent, commit] => {
                parent.sub_second
                    && commit.sub_second
                    && parent.consensus_digest != commit.consensus_digest
                    && parent.timestamp == commit.timestamp
            }
            _ => false,
        })
        .count();
    info!(
        target: "epoch-test",
        node,
        head,
        sub_second_blocks,
        post_fork_open_blocks,
        closing_blocks,
        same_second_block_pairs,
        "execution block commit-time coverage",
    );
    eyre::ensure!(
        sub_second_blocks > 0,
        "{node} served no block with a sub-second commit time up to its head {head}: it never \
         executed a block past the sub-second fork at leader epoch {SUBSECOND_FORK_EPOCH}"
    );
    eyre::ensure!(
        post_fork_open_blocks >= MIN_POST_FORK_OPEN_BLOCKS,
        "{node} served {post_fork_open_blocks} post-fork blocks that do not close an epoch up to \
         its head {head}, expected at least {MIN_POST_FORK_OPEN_BLOCKS}: the light load stopped \
         putting blocks inside post-fork epochs (see the \"light-load transfer rejected\" and \
         \"light-load sender nonce resynced\" lines)"
    );
    eyre::ensure!(
        same_second_block_pairs > 0,
        "{node} served no two consecutive post-fork blocks from different consensus headers with \
         the same EVM timestamp up to its head {head}: the whole-second timestamp never tied two \
         commits, so timestampMillis never had to order them"
    );
    Ok(served)
}
