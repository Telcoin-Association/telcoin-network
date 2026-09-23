//! Test the epoch boundary and validator shuffles.

use crate::common::get_block;

use super::common::{
    create_genesis_for_test, fetch_verified_epoch_record, generate_new_validator_txs,
    get_block_commit_time, loop_epochs, read_consensus_headers, scrape_metric_value, start_nodes,
    start_validator_with_args, wait_for_rpc, BlockCommitTime, ProcessGuard, NEW_VALIDATOR,
    NODE_PASSWORD,
};
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use e2e_tests::{config_local_testnet_with_epoch_duration, NodeEndpoints};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    convert::Infallible,
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
    forks::{
        leader_seeded_ordering_fork_epoch_override, multi_workers_fork_active,
        seed_signature_active, subsecond_timestamp_fork_epoch_override,
    },
    get_available_tcp_port, get_available_udp_port, keccak256, Address, BootstrapServer,
    ConsensusHeader, Epoch, EpochCertificate, EpochRecord, Genesis, GenesisAccount, P2pNode, B256,
    U256,
};
use tokio::time::timeout;
use tracing::{debug, info, warn};

const MIN_EPOCHS_TO_TEST: usize = 6;
// Epoch init creates HDX index files per epoch (open_epoch_pack → new_epoch →
// ConsensusPack::open_append). With test-utils, these are ~1.3MB each (vs ~130MB in prod).
// 5s is the consensus minimum epoch duration; halving it from 10s roughly halves the
// wall time of each epoch test. The two `tn_epochRecord` certificate-availability polls
// below are floored to an absolute minimum (`.max(..)`) rather than scaling with this
// constant, because certificate production is a fixed async quorum-voting cost that does
// not shrink with the epoch cadence.
const EPOCH_DURATION: u64 = 5;

/// Environment variable selecting the multi-workers fork epoch (issue #554) for this process
/// and every node it spawns (`tn_types::forks::multi_workers_fork_epoch_override`).
const MULTI_WORKERS_FORK_ENV: &str = "TN_MULTI_WORKERS_FORK_EPOCH";

/// Environment variable selecting the seed-signature fork epoch (#1032) for this process and every
/// node it spawns (`tn_types::forks::seed_signature_fork_epoch_override`).
const SEED_SIGNATURE_FORK_ENV: &str = "TN_SEED_SIGNATURE_FORK_EPOCH";

/// Environment variable selecting the leader-seeded-ordering fork epoch (#1260) for this process
/// and every node it spawns (`tn_types::forks::leader_seeded_ordering_fork_epoch_override`).
const LEADER_SEEDED_ORDERING_FORK_ENV: &str = "TN_LEADER_SEEDED_ORDERING_FORK_EPOCH";

/// Environment variable selecting the sub-second-timestamp fork epoch for this process and every
/// node it spawns (`tn_types::forks::subsecond_timestamp_fork_epoch_override`).
const SUBSECOND_TIMESTAMP_FORK_ENV: &str = "TN_SUBSECOND_TIMESTAMP_FORK_EPOCH";

/// Fork epoch for the cross-fork sync tests, [`test_epoch_sync_across_multi_workers_fork`] and
/// [`test_epoch_sync_across_leader_seeded_ordering_fork`].
///
/// The kill in [`test_epoch_sync_inner`] happens after `loop_epochs` has watched three boundaries
/// pass, so the epoch open at that point is at least 3 and the sealed set — which stops two below
/// it, see [`sealed_epochs`] — always covers epochs 0 and 1. Pinning a fork at 1 therefore
/// guarantees those sealed packs straddle it: epoch 0 written pre-fork (the legacy single-worker
/// committee layout, or the legacy DFS commit order), epoch 1 onward written post-fork.
const CROSS_FORK_EPOCH: Epoch = 1;

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

/// Prometheus series of the engine counter `tn_engine.evm_timestamp_clamped_total`.
///
/// The engine bumps it whenever it raises an EVM `timestamp` to its parent's. Consensus is meant
/// to produce non-decreasing commit times on its own, so any non-zero reading is a consensus bug.
const EVM_TIMESTAMP_CLAMPED_SERIES: &str = "tn_engine_evm_timestamp_clamped_total";

/// Pause between rounds of [`drive_light_tx_load`].
const LIGHT_LOAD_INTERVAL: Duration = Duration::from_millis(750);

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

/// Kill one node, advance several epochs without it, restart it against its existing datadir, and
/// assert it back-fills everything it missed.
///
/// Returns the epochs whose pack files were fingerprinted before the kill and revalidated after
/// the restart (see [`sealed_epochs`]), so a caller can assert what those packs cover.
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
) -> eyre::Result<Range<Epoch>> {
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

    loop_epochs(3, 3, &endpoints[0].http_url, EPOCH_DURATION).await?;
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

    Ok(sealed)
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
/// ([`multi_workers_fork_active`]); the seed signature of every header nested in a
/// `ConsensusHeader`, selected against the seed-signature fork ([`seed_signature_active`]); and the
/// millisecond fields, each header's `created_at_millis` and each sub-dag's
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

/// Pin four fork epochs for this process and every node it spawns: the multi-workers fork
/// (issue #554), the seed-signature fork (#1032), the leader-seeded-ordering fork (#1260), and the
/// sub-second-timestamp fork. The PREVRANDAO and governance-Safe forks are not pinned; see below.
///
/// Step 8 decodes sealed pack bytes in the harness, and that reaches three of those gates: the
/// `EpochMeta`'s [`tn_types::Committee`] is laid out by [`multi_workers_fork_active`], and every
/// nested `ConsensusHeader` by [`seed_signature_active`] and by
/// `tn_types::forks::subsecond_timestamp_active` (the millisecond fields of its sub-DAG and of
/// the headers inside it). So the harness has to resolve all three to the same fork points the
/// nodes wrote under. Left alone the two sides disagree the same way for the first two:
/// `TestBinary::command` forwards `u32::MAX` to a child when the variable is unset, while this
/// (non-adiri) harness build is active from genesis without it. Writing the variables settles
/// both sides at once — children inherit them verbatim at spawn, and the harness's own overrides
/// latch them on first read. The sub-second-timestamp fork's unset default already agrees
/// (`TestBinary::command` forwards `0`, and this build is active from genesis), so its pin is
/// what carries a forced fork point to both sides and turns a latched-earlier override into a
/// named failure. The leader-seeded-ordering fork changes no serialized layout, only the commit
/// order nodes write inside a pack, so the harness decode does not consult it; it is pinned here
/// for the children (and against a latched-earlier override), with the always-armed `0` default
/// `TestBinary::command` forwards for it.
///
/// All four are pinned, not just the one a given test is about. Pinning only some leaves the rest
/// asymmetric whenever the suite runs outside the Makefile wrapper that exports them, and the
/// symptom is misleading: children write dormant-layout headers, the harness decodes them as
/// genesis-active, and step 8 reports a corrupt pack rather than an environment mismatch.
///
/// The other two forks cannot put the harness and the nodes at odds. PREVRANDAO changes only the
/// executed block's `mix_hash`, which step 8 never decodes and no test in this file checks, so
/// children run whatever `TestBinary::command` forwards: the lane's `TN_PREVRANDAO_FORK_EPOCH`,
/// else the dormant `u32::MAX`. Everything the governance-Safe fork is made of is `adiri`-gated,
/// so it is compiled out of this harness and of the default e2e node binary; only the
/// `make test-e2e-governance-safe` lane runs it, and that lane runs `test_governance_safe_fork`
/// alone.
///
/// Each `force_*` argument states that fork epoch outright, for a test whose claim is about a
/// specific boundary. `None` inherits whatever the lane exported, defaulting to what
/// `TestBinary::command` would have forwarded anyway (the dormant `u32::MAX`, or `0` for the
/// leader-seeded-ordering and sub-second-timestamp forks), so
/// `TN_MULTI_WORKERS_FORK_EPOCH=1 make test-epochs` keeps meaning what it says.
///
/// Call once per test, before the first node spawn and before anything in the process reads any
/// gate: the overrides are process-wide `OnceLock`s and the environment is process-wide too. That
/// is sound because nextest runs each test in its own process (`.config/nextest.toml`); under
/// plain `cargo test` two of these tests in one process would fight over it, and the assertions
/// below are what turn that into a loud failure instead of a mis-decoded pack.
fn pin_fork_epochs(
    force_multi_workers: Option<Epoch>,
    force_seed_signature: Option<Epoch>,
    force_leader_seeded: Option<Epoch>,
    force_subsecond: Option<Epoch>,
) {
    // what `TestBinary::command` would forward to a child: the value the lane exported, or the
    // stated per-fork default when it exported nothing. an unparseable value normalizes to the
    // same default the gate would have fallen back to.
    let lane = |var: &str, default: Epoch| -> Epoch {
        std::env::var(var).ok().and_then(|raw| raw.trim().parse().ok()).unwrap_or(default)
    };

    // one shared helper rather than a block per fork, mirroring `TestBinary::command`, so the
    // forks cannot drift apart in mechanism; they arm independently, so each carries its own gate
    pin_fork_epoch(
        MULTI_WORKERS_FORK_ENV,
        force_multi_workers.unwrap_or_else(|| lane(MULTI_WORKERS_FORK_ENV, u32::MAX)),
        multi_workers_fork_active,
    );
    pin_fork_epoch(
        SEED_SIGNATURE_FORK_ENV,
        force_seed_signature.unwrap_or_else(|| lane(SEED_SIGNATURE_FORK_ENV, u32::MAX)),
        seed_signature_active,
    );
    // the leader-seeded gate (`leader_seeded_ordering_active`) conjoins the seed-signature fork
    // fail-closed, so asserting through the gate would entangle this pin with the seed pin's
    // value: with the seed fork dormant the gate reads false at every epoch, pinned or not. pin
    // through the conjunct-free override reader instead; same latched-earlier failure mode.
    pin_fork_epoch_override(
        LEADER_SEEDED_ORDERING_FORK_ENV,
        force_leader_seeded.unwrap_or_else(|| lane(LEADER_SEEDED_ORDERING_FORK_ENV, 0)),
        leader_seeded_ordering_fork_epoch_override,
    );
    // `subsecond_timestamp_active` conjoins the seed-signature fork the same way, so this pin
    // goes through its conjunct-free override reader for the same reason
    pin_fork_epoch_override(
        SUBSECOND_TIMESTAMP_FORK_ENV,
        force_subsecond.unwrap_or_else(|| lane(SUBSECOND_TIMESTAMP_FORK_ENV, 0)),
        subsecond_timestamp_fork_epoch_override,
    );
}

/// Write `fork_epoch` to the `var` override and check `gate` reads the same fork point back.
///
/// Reading the gate here, rather than leaving it to whatever decodes a pack minutes later, is what
/// turns an override that latched before this pin into a named failure instead of a corrupt-looking
/// pack.
fn pin_fork_epoch(var: &str, fork_epoch: Epoch, gate: impl Fn(Epoch) -> bool) {
    std::env::set_var(var, fork_epoch.to_string());

    // The gate is `>=`, so it fires at the fork epoch and nowhere below it; both assertions hold
    // for the dormant pin too, since `u32::MAX >= u32::MAX`.
    assert!(
        gate(fork_epoch),
        "harness gate must be active at the pinned fork epoch {fork_epoch}: {var} latched to \
         another value before this test pinned it"
    );
    if let Some(below) = fork_epoch.checked_sub(1) {
        assert!(
            !gate(below),
            "harness gate must be dormant below the pinned fork epoch {fork_epoch}: {var} latched \
             to another value before this test pinned it"
        );
    }
    info!(target: "epoch-test", var, fork_epoch, "pinned a fork epoch");
}

/// Write `fork_epoch` to the `var` override and check the override reader `read` latched it.
///
/// The [`pin_fork_epoch`] variant for a fork whose public gate conjoins another fork (the
/// leader-seeded ordering conjoins the seed signature): the gate cannot witness this pin on its
/// own, but equality on the conjunct-free override reader gives callers the same guarantee, an
/// override that latched before this pin becomes a named failure instead of nodes silently
/// running a different fork point than the test states.
fn pin_fork_epoch_override(var: &str, fork_epoch: Epoch, read: impl Fn() -> Option<Epoch>) {
    std::env::set_var(var, fork_epoch.to_string());

    assert_eq!(
        read(),
        Some(fork_epoch),
        "harness override must read back the pinned fork epoch {fork_epoch}: {var} latched to \
         another value before this test pinned it"
    );
    info!(target: "epoch-test", var, fork_epoch, "pinned a fork epoch");
}

/// Spin up the epoch-sync network and run [`test_epoch_sync_inner`] against it.
///
/// `test` names both the temp-dir prefix and the `test_logs/` directory, so callers running the
/// same scenario under different fork epochs keep separate node logs. Keep it short: every node's
/// IPC socket path is built under the temp dir, and a unix socket path is capped at ~104 bytes.
async fn run_epoch_sync_scenario(test: &str) -> eyre::Result<Range<Epoch>> {
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

    test_epoch_sync_inner(
        &mut guard,
        2,
        &[("validator-3", Address::from_slice(&[0x33; 20]))],
        &committee[..],
        temp_path,
        test,
        &mut endpoints,
    )
    .await
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
/// Keytool currently generates only worker 0. Derive the second identity from the same keys
/// the node loads at startup, and share both workers' addresses before starting any processes.
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

    run_epoch_sync_scenario("epoch_sync").await.map(|_sealed| ())
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

    let sealed = run_epoch_sync_scenario("epoch_sync_fork").await?;

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

    let sealed = run_epoch_sync_scenario("epoch_sync_seeded").await?;

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
/// Test consensus commit times across the sub-second timestamp fork, over RPC and on disk.
///
/// The fork is pinned at [`SUBSECOND_FORK_EPOCH`] (epochs 0 and 1 pre-fork) with the seed-signature
/// fork active from genesis, and a four-validator network at the harness's 500/250/250 ms cadence
/// runs under light transaction load until [`SUBSECOND_TARGET_EPOCH`] opens. Then, on every node:
///
/// - every execution block keeps a whole-second `timestamp` that never decreases, including across
///   each epoch boundary, and equals its consensus commit time floored to seconds;
/// - blocks executed from one consensus header report one commit time;
/// - the engine never clamped an EVM timestamp ([`EVM_TIMESTAMP_CLAMPED_SERIES`] reads 0);
/// - nodes agree on every block and commit time they all hold.
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

    // every node is still the process it started as, so its counter covers the whole run
    for (addr, url) in metrics_addrs.iter().zip(&rpc_urls) {
        let clamped = scrape_metric_value(addr, EVM_TIMESTAMP_CLAMPED_SERIES)?;
        eyre::ensure!(
            clamped == 0.0,
            "{url} clamped {clamped} EVM timestamps up to their parent's: consensus let commit \
             time go backwards (node logs under test_logs/{test}/ carry the \"evm timestamp \
             clamped to parent\" warnings)"
        );
    }

    // stop validator-1 so its consensus chain can be opened from this process
    let mut validator_1 = guard.take(0).ok_or_else(|| eyre::eyre!("validator-1 is not running"))?;
    super::common::kill_child(&mut validator_1);
    eyre::ensure!(
        providers[0].get_chain_id().await.is_err(),
        "validator-1 still answers RPC after being stopped"
    );
    let headers = read_consensus_headers(&temp_path.join("validator-1")).await?;
    let commits = assert_consensus_commit_times(&headers)?;
    assert_blocks_match_consensus(&served[0], &commits)?;

    guard.kill_all();
    Ok(())
}

/// Keep a light transaction load on every node until the caller drops this future.
///
/// Each round sends one transfer from `senders[i]` to `providers[i]`, so every worker regularly
/// seals a batch of its own and a single commit often carries batches from several workers; the
/// execution blocks built from such a commit share a `parentBeaconBlockRoot`, which is what the
/// per-header commit-time check in [`assert_block_commit_times`] compares. A rejected send is
/// logged and skipped: the load only puts blocks inside each epoch and is not asserted on.
async fn drive_light_tx_load<P: Provider>(
    providers: &[P],
    senders: &mut [TransactionFactory],
    chain: Arc<RethChainSpec>,
) -> Infallible {
    let sink = Address::from_slice(&[0x5e; 20]);
    loop {
        for (provider, sender) in providers.iter().zip(senders.iter_mut()) {
            let tx = sender.create_eip1559_encoded(
                chain.clone(),
                None,
                100,
                Some(sink),
                U256::from(1),
                Bytes::new(),
            );
            if let Err(error) = provider.send_raw_transaction(&tx).await {
                warn!(
                    target: "epoch-test",
                    %error,
                    sender = %sender.address(),
                    "light-load transfer rejected",
                );
            }
        }
        tokio::time::sleep(LIGHT_LOAD_INTERVAL).await;
    }
}

/// Assert the sub-second timestamp invariants `provider` serves for every execution block from
/// genesis to its current head, and return each block's commit time in block order.
///
/// Per block, `tn_getBlockTimestampMillis` must describe the block `eth_getBlockByNumber` returns
/// (number, hash and `timestamp`); the `timestamp` must equal the commit time floored to whole
/// seconds and be no smaller than its parent's; and the consensus digest must be the block's
/// `parentBeaconBlockRoot`. Blocks built from one consensus output name the same consensus header
/// there and must report the same commit time.
async fn assert_block_commit_times<P: Provider>(
    provider: &P,
    node: &str,
) -> eyre::Result<Vec<BlockCommitTime>> {
    let head = provider.get_block_number().await?;
    let mut served: Vec<BlockCommitTime> = Vec::new();
    let mut by_beacon_root: BTreeMap<B256, u64> = BTreeMap::new();
    let mut shared_root_blocks = 0usize;
    for number in 0..=head {
        let block = provider
            .get_block_by_number(BlockNumberOrTag::Number(number))
            .await?
            .ok_or_else(|| eyre::eyre!("{node} has no block {number} below its head {head}"))?;
        let commit = get_block_commit_time(provider, number)
            .await
            .map_err(|e| eyre::eyre!("{node}: {e}"))?;
        let context = format!("{node} block {number}: {commit:?}");
        eyre::ensure!(
            commit.block_number == number
                && commit.block_hash == block.header.hash
                && commit.timestamp == block.header.timestamp,
            "tn_getBlockTimestampMillis disagrees with eth_getBlockByNumber (hash {}, timestamp \
             {}): {context}",
            block.header.hash,
            block.header.timestamp,
        );
        eyre::ensure!(
            commit.timestamp == commit.timestamp_millis / 1000,
            "EVM timestamp is not the consensus commit time floored to seconds: {context}"
        );
        if let Some(parent) = served.last() {
            eyre::ensure!(
                commit.timestamp >= parent.timestamp,
                "EVM timestamp went backwards from block {} at {}: {context}",
                parent.block_number,
                parent.timestamp,
            );
        }
        // genesis carries the eip-4788 field zeroed; every later block names its consensus header
        match block.header.parent_beacon_block_root.filter(|root| !root.is_zero()) {
            None => eyre::ensure!(
                number == 0 && commit.consensus_digest.is_none(),
                "execution block without a consensus header: {context}"
            ),
            Some(root) => {
                eyre::ensure!(
                    commit.consensus_digest == Some(root),
                    "consensusDigest is not the parentBeaconBlockRoot {root}: {context}"
                );
                match by_beacon_root.entry(root) {
                    Entry::Vacant(entry) => {
                        entry.insert(commit.timestamp_millis);
                    }
                    Entry::Occupied(entry) => {
                        eyre::ensure!(
                            *entry.get() == commit.timestamp_millis,
                            "blocks from consensus header {root} report different commit times \
                             ({} ms earlier): {context}",
                            entry.get(),
                        );
                        shared_root_blocks += 1;
                    }
                }
            }
        }
        served.push(commit);
    }
    info!(
        target: "epoch-test",
        node,
        head,
        consensus_headers = by_beacon_root.len(),
        shared_root_blocks,
        "execution block commit times verified",
    );
    Ok(served)
}

/// Assert every node reports the same block and commit time at every height they all hold.
///
/// Both derive from consensus output alone, so a disagreement at a shared height is a fork in the
/// execution chain (the hash) or in the commit-time derivation (the milliseconds).
fn assert_nodes_agree_on_commit_times(
    served: &[Vec<BlockCommitTime>],
    nodes: &[String],
) -> eyre::Result<()> {
    let Some((reference, others)) = served.split_first() else {
        return Ok(());
    };
    for (other, node) in others.iter().zip(nodes.iter().skip(1)) {
        for (expected, actual) in reference.iter().zip(other) {
            eyre::ensure!(
                expected == actual,
                "{node} disagrees with {} at block {}: {actual:?} vs {expected:?}",
                nodes[0],
                expected.block_number,
            );
        }
    }
    Ok(())
}

/// A consensus header's commit, as read from a node's consensus chain on disk.
#[derive(Debug, Clone, Copy)]
struct ConsensusCommit {
    /// The consensus header's number.
    number: u64,
    /// The epoch of the sub-dag's leader, which selects the commit-time layout.
    leader_epoch: Epoch,
    /// The commit time in milliseconds since the Unix epoch.
    commit_ms: u64,
}

/// Assert the commit-time invariants over a node's whole consensus chain, walked in order, and
/// return each header's commit keyed by its digest.
///
/// - Commit seconds never decrease, before the fork or after it; in particular every consecutive
///   pre-fork pair has non-decreasing seconds and millis 0 on both sides.
/// - From the fork seam on, commit milliseconds strictly increase: each post-fork header commits at
///   least 1 ms after its predecessor, including the first post-fork header after the last pre-fork
///   one (whose commit time is whole seconds) and the first header of every later epoch. At a
///   post-fork seam the protocol floor is the previous epoch's close in whole seconds, so the
///   strict step there also relies on the new epoch's leaders being created after that close, which
///   holds for nodes sharing this host's clock.
/// - A post-fork commit is never earlier than its leader header was created.
/// - Pre-fork headers carry no sub-second part: their commit time and every header in their sub-dag
///   hold millis 0, and the sub-dag serializes without `commit_timestamp_millis`, the legacy layout
///   an old binary still decodes. Post-fork sub-dags must carry that field.
///
/// Fails loudly unless the walk covered what those claims need: at least two pre-fork headers and
/// at least one consecutive pre-fork pair, the fork seam exactly once, at least two post-fork
/// epoch seams, at least one consecutive post-fork pair committed within the same second, and at
/// least one post-fork commit with non-zero millis.
fn assert_consensus_commit_times(
    headers: &[ConsensusHeader],
) -> eyre::Result<BTreeMap<B256, ConsensusCommit>> {
    let mut commits = BTreeMap::new();
    let mut pre_fork_headers = 0usize;
    let mut pre_fork_pairs = 0usize;
    let mut fork_seams = 0usize;
    let mut post_fork_seams = 0usize;
    let mut same_second_pairs = 0usize;
    let mut sub_second_commits = 0usize;
    let mut previous: Option<&ConsensusHeader> = None;
    for header in headers {
        let sub_dag = &header.sub_dag;
        let epoch = sub_dag.leader_epoch();
        let commit_ms = sub_dag.commit_timestamp_ms();
        let post_fork = epoch >= SUBSECOND_FORK_EPOCH;
        let context = format!(
            "consensus header {} (leader epoch {epoch}, committed at {} ms)",
            header.number,
            commit_ms.as_millis()
        );

        let json = serde_json::to_value(sub_dag)?;
        let fields = json
            .as_object()
            .ok_or_else(|| eyre::eyre!("{context}: sub-dag JSON is not an object: {json}"))?;
        // guards the key checks below against a renamed seconds field reading as "absent"
        eyre::ensure!(
            fields.contains_key("commit_timestamp"),
            "{context}: sub-dag JSON has no commit_timestamp field: {json}"
        );
        if post_fork {
            eyre::ensure!(
                fields.contains_key("commit_timestamp_millis"),
                "{context}: post-fork sub-dag serialized without commit_timestamp_millis"
            );
            let leader_ms = sub_dag.leader().created_at_ms();
            eyre::ensure!(
                commit_ms >= leader_ms,
                "{context}: committed before its leader was created at {} ms",
                leader_ms.as_millis()
            );
            if commit_ms.subsec_millis() != 0 {
                sub_second_commits += 1;
            }
        } else {
            pre_fork_headers += 1;
            eyre::ensure!(
                commit_ms.subsec_millis() == 0,
                "{context}: pre-fork commit time has a sub-second part"
            );
            eyre::ensure!(
                !fields.contains_key("commit_timestamp_millis"),
                "{context}: pre-fork sub-dag serialized with commit_timestamp_millis: {json}"
            );
            if let Some(stray) = sub_dag.headers().iter().find(|h| h.created_at_millis() != 0) {
                eyre::bail!(
                    "{context}: pre-fork header {} was created with {} sub-second millis",
                    stray.digest(),
                    stray.created_at_millis()
                );
            }
        }

        if let Some(prev) = previous {
            let prev_epoch = prev.sub_dag.leader_epoch();
            let prev_ms = prev.sub_dag.commit_timestamp_ms();
            let step = format!("from header {} at {} ms", prev.number, prev_ms.as_millis());
            eyre::ensure!(epoch >= prev_epoch, "{context}: leader epoch went back {step}");
            eyre::ensure!(
                commit_ms.secs() >= prev_ms.secs(),
                "{context}: commit seconds went backwards {step}"
            );
            if post_fork {
                eyre::ensure!(
                    commit_ms > prev_ms,
                    "{context}: post-fork commit time did not strictly increase {step}"
                );
            }
            let prev_post_fork = prev_epoch >= SUBSECOND_FORK_EPOCH;
            if !post_fork && !prev_post_fork {
                // both sides already hold millis 0 on their own; restated per pair so the pre-fork
                // pair claim stands on this check alone
                eyre::ensure!(
                    commit_ms.subsec_millis() == 0 && prev_ms.subsec_millis() == 0,
                    "{context}: pre-fork pair carries a sub-second part {step}"
                );
                pre_fork_pairs += 1;
            }
            if post_fork && prev_post_fork && commit_ms.secs() == prev_ms.secs() {
                same_second_pairs += 1;
            }
            if epoch != prev_epoch {
                if post_fork && !prev_post_fork {
                    fork_seams += 1;
                } else if prev_post_fork {
                    post_fork_seams += 1;
                }
            }
        }

        commits.insert(
            B256::from(header.digest()),
            ConsensusCommit {
                number: header.number,
                leader_epoch: epoch,
                commit_ms: commit_ms.as_millis(),
            },
        );
        previous = Some(header);
    }

    info!(
        target: "epoch-test",
        headers = headers.len(),
        pre_fork_headers,
        pre_fork_pairs,
        fork_seams,
        post_fork_seams,
        same_second_pairs,
        sub_second_commits,
        "consensus commit times verified",
    );
    eyre::ensure!(
        pre_fork_headers >= 2,
        "the walk holds {pre_fork_headers} pre-fork headers, expected at least 2"
    );
    eyre::ensure!(
        pre_fork_pairs > 0,
        "no two consecutive pre-fork commits: the whole-second ordering was never exercised"
    );
    eyre::ensure!(
        fork_seams == 1,
        "the walk crossed the sub-second fork seam {fork_seams} times, expected exactly once"
    );
    eyre::ensure!(
        post_fork_seams >= 2,
        "the walk crossed {post_fork_seams} post-fork epoch seams, expected at least 2"
    );
    eyre::ensure!(
        same_second_pairs > 0,
        "no two consecutive post-fork commits share a second: the strict millisecond ordering was \
         never exercised below the seconds grid"
    );
    eyre::ensure!(
        sub_second_commits > 0,
        "no post-fork commit carries non-zero millis: commit times never left the seconds grid"
    );
    Ok(commits)
}

/// Assert every block a node served over RPC reports the commit time its consensus header holds
/// in that node's consensus chain, with the matching `subSecond` flag, and that the blocks span
/// every epoch below [`SUBSECOND_TARGET_EPOCH`] (so the RPC checks crossed the fork seam and the
/// post-fork seams too).
fn assert_blocks_match_consensus(
    served: &[BlockCommitTime],
    commits: &BTreeMap<B256, ConsensusCommit>,
) -> eyre::Result<()> {
    let mut epochs = BTreeSet::new();
    for block in served {
        // genesis has no consensus header
        let Some(digest) = block.consensus_digest else { continue };
        let commit = commits.get(&digest).ok_or_else(|| {
            eyre::eyre!(
                "block {} names consensus header {digest}, absent on disk",
                block.block_number
            )
        })?;
        eyre::ensure!(
            block.consensus_number == Some(commit.number)
                && block.timestamp_millis == commit.commit_ms
                && block.sub_second == (commit.leader_epoch >= SUBSECOND_FORK_EPOCH),
            "block served over RPC {block:?} disagrees with its consensus header on disk {commit:?}"
        );
        epochs.insert(commit.leader_epoch);
    }
    eyre::ensure!(
        (0..SUBSECOND_TARGET_EPOCH).all(|epoch| epochs.contains(&epoch)),
        "served blocks cover leader epochs {epochs:?}, not every epoch below \
         {SUBSECOND_TARGET_EPOCH}"
    );
    Ok(())
}
