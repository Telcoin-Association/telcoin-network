//! Test the epoch boundary and validator shuffles.

use crate::common::get_block;

use super::common::{
    create_genesis_for_test, fetch_verified_epoch_record, generate_new_validator_txs, loop_epochs,
    start_nodes, ProcessGuard, NEW_VALIDATOR,
};
use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::NodeEndpoints;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::BTreeMap,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_storage::pack_validate::{validate_pack_file, Verdict};
use tn_test_utils::wait_until;
use tn_types::{
    forks::committee_workers_active, keccak256, Address, Epoch, EpochCertificate, EpochRecord,
    Genesis, B256,
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

/// Environment variable selecting the committee-workers fork epoch (#554) for this process and
/// every node it spawns (`tn_types::forks::committee_workers_fork_epoch_override`).
const COMMITTEE_WORKERS_FORK_ENV: &str = "TN_COMMITTEE_WORKERS_FORK_EPOCH";

/// Committee-workers fork epoch for [`test_epoch_sync_across_committee_workers_fork`].
///
/// The kill in [`test_epoch_sync_inner`] happens after `loop_epochs` has watched three boundaries
/// pass, so the epoch open at that point is at least 3 and the sealed set — which stops two below
/// it, see [`sealed_epochs`] — always covers epochs 0 and 1. Pinning the fork at 1 therefore
/// guarantees those sealed packs straddle it: epoch 0 in the legacy single-worker committee
/// layout, epoch 1 onward in the multi-worker one.
const CROSS_FORK_EPOCH: Epoch = 1;

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
/// Decoding a pack means decoding the [`tn_types::Committee`] in its `EpochMeta` record, whose bcs
/// layout is selected by the committee's own epoch against the committee-workers fork
/// ([`committee_workers_active`]). This process therefore has to sit on the same fork epoch the
/// nodes wrote under, which is what [`pin_committee_workers_fork`] arranges.
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

/// Pin the committee-workers fork epoch (#554) for this process and every node it spawns.
///
/// Step 8 decodes sealed pack bytes in the harness, so the harness's gate
/// ([`committee_workers_active`]) has to resolve to the same fork point the nodes wrote under.
/// Left alone the two disagree: `TestBinary::command` forwards `u32::MAX` to a child when the
/// variable is unset, while this (non-adiri) harness build is active from genesis without it.
/// Writing the variable settles both sides at once — children inherit it verbatim at spawn, and
/// the harness's own override latches it on first read.
///
/// `force` states a fork epoch outright, for a test whose claim is about a specific boundary.
/// `None` inherits whatever the lane exported, defaulting to the dormant `u32::MAX` that
/// `TestBinary::command` would have forwarded anyway, so `TN_COMMITTEE_WORKERS_FORK_EPOCH=1 make
/// test-epochs` keeps meaning what it says.
///
/// Call once per test, before the first node spawn and before anything in the process reads the
/// gate: the override is a process-wide `OnceLock` and the environment is process-wide too. That
/// is sound because nextest runs each test in its own process (`.config/nextest.toml`); under
/// plain `cargo test` two of these tests in one process would fight over it, and the assertions
/// below are what turn that into a loud failure instead of a mis-decoded pack.
fn pin_committee_workers_fork(force: Option<Epoch>) {
    let fork_epoch = force.unwrap_or_else(|| {
        std::env::var(COMMITTEE_WORKERS_FORK_ENV)
            .ok()
            .and_then(|raw| raw.trim().parse().ok())
            .unwrap_or(u32::MAX)
    });
    std::env::set_var(COMMITTEE_WORKERS_FORK_ENV, fork_epoch.to_string());

    // Read the gate now, while a mismatch is still cheap to explain. The gate is `>=`, so it fires
    // at the fork epoch and nowhere below it; both assertions hold for the dormant pin too, since
    // `u32::MAX >= u32::MAX`.
    assert!(
        committee_workers_active(fork_epoch),
        "harness gate must be active at the pinned fork epoch {fork_epoch}: \
         {COMMITTEE_WORKERS_FORK_ENV} latched to another value before this test pinned it"
    );
    if let Some(below) = fork_epoch.checked_sub(1) {
        assert!(
            !committee_workers_active(below),
            "harness gate must be dormant below the pinned fork epoch {fork_epoch}: \
             {COMMITTEE_WORKERS_FORK_ENV} latched to another value before this test pinned it"
        );
    }
    info!(target: "epoch-test", fork_epoch, "pinned the committee-workers fork epoch");
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

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that sync works to fill in missing epochs.
async fn test_epoch_sync() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // whatever fork epoch the lane stated, dormant by default - this test is about sync, not about
    // the fork, but the harness still decodes pack bytes and must agree with the nodes
    pin_committee_workers_fork(None);

    run_epoch_sync_scenario("epoch_sync").await.map(|_sealed| ())
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test(flavor = "multi_thread")]
/// Test that an epoch pack archive spanning the committee-workers fork boundary (#554) survives a
/// restart.
///
/// The same kill/restart scenario as [`test_epoch_sync`], with the fork pinned at
/// [`CROSS_FORK_EPOCH`] so one datadir holds both committee layouts: epoch 0 written in the legacy
/// single-worker layout, every later epoch in the multi-worker one. The restarted node has to read
/// its own history back across that boundary to decide which epochs it still needs, and step 8
/// then decodes both layouts again from the harness.
async fn test_epoch_sync_across_committee_workers_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    // forced rather than inherited: this test's claim is a crossing at a known epoch, so it states
    // the fork point even when the lane exported a different one
    pin_committee_workers_fork(Some(CROSS_FORK_EPOCH));

    let sealed = run_epoch_sync_scenario("epoch_sync_fork").await?;

    // The revalidated packs span both layouts only if the fork epoch sits strictly inside them.
    // Assert it rather than trusting the arithmetic in `CROSS_FORK_EPOCH`: a shorter run, or a
    // wider safety margin in `sealed_epochs`, would otherwise quietly reduce this to the
    // single-layout test above.
    assert!(
        sealed.start < CROSS_FORK_EPOCH && CROSS_FORK_EPOCH < sealed.end,
        "sealed epochs {sealed:?} do not straddle the committee-workers fork at \
         {CROSS_FORK_EPOCH}: the restart proved only one committee layout"
    );

    Ok(())
}
