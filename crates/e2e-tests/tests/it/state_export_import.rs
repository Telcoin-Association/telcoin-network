//! E2e test for the execution-state snapshot export/import (bootstrap) flow.
//!
//! This exercises the "Bootstrapping From a State Snapshot" path documented in `SYNC.md`: a running
//! node with `--enable-state-export` writes a portable bundle at every epoch boundary, and a fresh
//! node loads one bundle with `db load-state` and then syncs *forward* from that epoch instead of
//! replaying every epoch's consensus output from genesis.
//!
//! ## Topology: 4 validators, exactly one exporter
//!
//! A literal single producing node is not possible — `CommitteeInner::load`
//! (`crates/types/src/committee.rs`) asserts a committee larger than one, and the whole point of
//! the test (the importer must *sync and then follow*) requires a live network that keeps advancing
//! epochs while and after the observer joins. So the network is the standard 4-validator committee
//! used by every other epoch test, and `--enable-state-export` is enabled on **exactly one** of
//! them (`validator-1`). That single exporter produces the bundle the observer imports; the other
//! three only keep the quorum alive.
//!
//! ## What the restored node needs from the snapshot block
//!
//! When a node enters an epoch it reads the entered epoch's worker count and per-worker base fees
//! from ONE pinned block: the previous epoch's closing block, whose own system call recorded each
//! EIP-1559 worker's next-epoch fee into its `WorkerConfigs` row. A snapshot-imported node pins
//! that read to the snapshot block `B`, so all it needs is that `B` actually closed an epoch — no
//! walk into pre-snapshot history, and no dependence on whether any worker produced a block. That
//! is what `SnapshotRestorer::entry_readiness_precondition` validates at import.
//!
//! This test still runs a steady transaction stream, but only so the exported state is non-trivial
//! (accounts, storage, and code to round-trip) and the observer has real blocks to follow forward.
//! A companion test in this file (`test_state_export_import_idle_epoch`) runs a fully idle network
//! to prove the idle case now round-trips end to end — the hazard that used to force the exporter
//! to skip an idle epoch's bundle is gone.
//!
//! Neither of those two tests can say anything about the fee *value* the restored node enters on:
//! both run the genesis default `Eip1559 { target_gas: u64::MAX }`, which pins every worker at
//! `MIN_PROTOCOL_BASE_FEE` forever, so an `== MIN` assertion in either would hold no matter what
//! the import did. `test_state_export_import_recovers_recorded_fee` exists for that value: it sets
//! a moving fee at genesis, walks it clear of MIN, then exports an idle epoch whose recorded word
//! is provably neither MIN nor the fee of the epoch before it.
//!
//! ## Why the assertions prove *import*, not just *sync*
//!
//! A node that ignored the import and replayed from genesis would still eventually catch up, so the
//! test is careful to prove the observer actually bootstrapped from the snapshot:
//!
//! - `db load-state` reports it wrote a resume hint for the import epoch (records `0..=N`, the
//!   epoch-`N` consensus pack, and the "latest" slot hint). reth then continues from the populated
//!   tip; it never resets a non-empty datadir back to genesis.
//! - The observer's execution tip is `>= import_block` from the moment its RPC answers — a
//!   from-genesis start would begin below it.
//! - The observer's import-block hash equals the hash the network committed as epoch `N`'s
//!   `final_state`, proving the imported EVM state/header is the real one.
//! - The observer ends up with a verified epoch record and executed final block for epoch `N + 1`,
//!   which was **not** in the bundle (the bundle carried records `0..=N` and only epoch `N`'s
//!   consensus pack). That record and its blocks can only have arrived via forward sync.
//! - Finally, a transaction submitted to a validator after the observer joined shows up on the
//!   observer, proving it follows live output rather than serving a frozen snapshot.

use alloy::{
    primitives::{utils::parse_ether, Bytes},
    providers::{Provider, ProviderBuilder},
};
use e2e_tests::{
    config_local_testnet_with_epoch_duration, config_local_testnet_with_worker_fee_configs,
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, WORKER_CONFIGS_ADDRESS};
use tn_reth::{
    system_calls::{ConsensusRegistry, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::wait_until;
use tn_types::{
    get_available_tcp_port, Address, Genesis, GenesisAccount, MIN_PROTOCOL_BASE_FEE, U256,
};
use tracing::info;

use crate::common::{
    address_from_word, current_epoch, fetch_verified_epoch_record, get_balance, get_block, get_key,
    get_positive_balance_with_retry, get_tx_receipt_block, network_advancing, read_base_fee,
    send_and_confirm, send_tel, start_observer, start_validator, start_validator_with_args,
    wait_for_epoch_at_least, wait_for_mid_epoch, ProcessGuard,
};

/// Epoch duration (seconds) for this test. Held at 10s (matching the pack-import test in
/// `sync.rs`) rather than the 5s epoch tests: the export runs a full plain-state walk on a
/// background thread, then the bundle is copied and atomically renamed into place, so 10s keeps
/// ample margin for a complete bundle to appear before the observer imports it.
const EXPORT_EPOCH_DURATION: u64 = 10;

/// Epoch whose bundle the observer imports. Must be `>= 1`: an epoch-0 bundle restores state and
/// records but writes no resume hint (rebuilding the epoch-0 consensus pack needs a pre-epoch-0
/// genesis descriptor the bundle does not carry — see `SYNC.md`), so a node loaded from it would
/// not resume. Epoch 2 also leaves a real forward-sync gap to the tip.
const IMPORT_EPOCH: u32 = 2;

/// The network must be at least this many epochs in before the observer imports, so the imported
/// epoch sits comfortably behind the tip and the observer has newer epochs to sync forward through.
const MIN_LEAD_EPOCH: u32 = IMPORT_EPOCH + 2;

/// Poll `cond` until it returns `true`, failing fast if the observer process exits or the
/// wall-clock deadline passes.
///
/// Unlike the retry-heavy `call_rpc`-based helpers (`get_block_number` etc., which retry 10x on a
/// refused connection and turn a down node into a multi-minute poll), `cond` should be a single
/// fast-fail check (an `alloy` request), so the `deadline_secs` bound is a true wall-clock bound.
/// The `try_wait` guard turns an observer that dies mid-wait into an immediate, descriptive failure
/// pointing at its log, instead of a silent timeout.
async fn wait_observer<F, Fut>(
    guard: &mut ProcessGuard,
    obs_idx: usize,
    deadline_secs: u64,
    what: &str,
    cond: F,
) -> eyre::Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(deadline_secs);
    loop {
        if let Some(status) = guard.get_mut(obs_idx).and_then(|c| c.try_wait().ok().flatten()) {
            eyre::bail!(
                "observer process exited ({status}) while waiting for {what}; see \
                 crates/e2e-tests/test_logs/state_export_import/node4-run0.stderr.log"
            );
        }
        if cond().await {
            return Ok(());
        }
        if Instant::now() >= deadline {
            eyre::bail!(
                "timed out after {deadline_secs}s waiting for {what}; see \
                 crates/e2e-tests/test_logs/state_export_import/node4-run0.log"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Verify the full state export/import bootstrap flow: one validator exports epoch bundles, a fresh
/// observer imports one and then syncs forward and follows from that point.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_state_export_import_bootstrap() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_state_export_import_bootstrap_inner())
}

async fn test_state_export_import_bootstrap_inner() -> eyre::Result<()> {
    info!(target: "restart-test", "test_state_export_import_bootstrap");
    let tmp_guard = tempfile::TempDir::with_prefix("state_export_import").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();

    // A funded factory drives the transaction stream that keeps every epoch non-idle (see the
    // module docs). Its address must be known before genesis so it can be funded there.
    let mut tx_factory = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(1234));
    let funded = vec![(
        tx_factory.address(),
        GenesisAccount::default().with_balance(U256::from(parse_ether("10_000_000")?)),
    )];

    // 4 validators + an observer, all sharing one genesis; short epochs so several boundaries pass
    // within the test budget.
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        Some(funded),
        Some(EXPORT_EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    // Start the 4-validator committee. Exactly ONE validator (index 0 -> `validator-1`) runs with
    // `--enable-state-export`; it is the sole producer of the snapshot bundles.
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; 4] = Default::default();
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        *url = format!("http://127.0.0.1:{rpc_port}");
        let child = if i == 0 {
            start_validator_with_args(
                i,
                bin,
                &temp_path,
                rpc_port,
                "state_export_import",
                0,
                &["--enable-state-export"],
            )
        } else {
            start_validator(i, bin, &temp_path, rpc_port, "state_export_import", 0)
        };
        guard.push(child);
    }

    // Wait for every validator to serve RPC.
    network_advancing(&client_urls)?;

    // Drive a steady transaction stream so every epoch contains a genuine worker block (see the
    // module docs on why the exported epoch must not be idle). The chain spec comes from the
    // genesis the ceremony just wrote. The stream runs until `stop` is set at the end of the
    // test.
    let genesis: Genesis = Config::load_from_path(
        temp_path.join("validator-1").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let tx_sink = address_from_word("export-import-tx-sink");
    let stop = Arc::new(AtomicBool::new(false));
    let stream = {
        let stop = stop.clone();
        let stream_url = client_urls[1].clone();
        let chain = chain.clone();
        tokio::spawn(async move {
            let provider =
                ProviderBuilder::new().connect_http(stream_url.parse().expect("valid stream url"));
            while !stop.load(Ordering::Relaxed) {
                let raw = tx_factory.create_eip1559_encoded(
                    chain.clone(),
                    None,
                    100,
                    Some(tx_sink),
                    U256::from(1_000u64),
                    Bytes::default(),
                );
                let _ = provider.send_raw_transaction(&raw).await;
                tokio::time::sleep(Duration::from_millis(1500)).await;
            }
        })
    };

    // Let the network run a few epochs past the import epoch so (a) the epoch-`IMPORT_EPOCH` bundle
    // is fully written and (b) there is a real forward-sync gap for the observer to cross.
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4 * MIN_LEAD_EPOCH as u64),
        &format!("network to reach epoch {MIN_LEAD_EPOCH}"),
        || async { Ok(registry.getCurrentEpochInfo().call().await?.epochId >= MIN_LEAD_EPOCH) },
    )
    .await?;
    info!(target: "restart-test", "network reached epoch {MIN_LEAD_EPOCH}");

    // Confirm the transaction stream is actually landing (so the exported epoch has genuine worker
    // blocks); a broken stream would otherwise surface later as a confusing observer crash.
    let sink_balance = get_positive_balance_with_retry(&client_urls[0], &tx_sink.to_string())?;
    assert!(sink_balance > 0, "transaction stream produced no executed transfers");

    // The exporter writes the bundle atomically (temp dir renamed into place), so the directory
    // only appears once complete. Poll for it, then sanity-check the four expected files.
    let bundle_dir = temp_path
        .join("validator-1")
        .join("consensus-db")
        .join("state_exports")
        .join(format!("epoch-{IMPORT_EPOCH}"));
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4),
        &format!("exporter to write the epoch-{IMPORT_EPOCH} bundle"),
        || async { Ok(bundle_dir.is_dir()) },
    )
    .await?;
    for file in ["state_data", "consensus_data", "epoch_records", "epoch_certs"] {
        assert!(
            bundle_dir.join(file).is_file(),
            "export bundle {bundle_dir:?} is missing `{file}`"
        );
    }
    info!(target: "restart-test", ?bundle_dir, "exporter produced the bundle");

    // Anchor the import point to what the network committed: the certified epoch-`IMPORT_EPOCH`
    // record names the epoch's final executed block (number + hash). The observer must import that
    // exact block and later agree on its hash.
    let import_record = fetch_verified_epoch_record(
        &client_urls[0],
        IMPORT_EPOCH,
        (EXPORT_EPOCH_DURATION * 4).max(60),
    )
    .await?;
    let import_block = import_record.final_state.number;
    let import_hash = import_record.final_state.hash;
    info!(target: "restart-test", IMPORT_EPOCH, import_block, %import_hash, "anchored import point");

    // Import the bundle into the (fresh, config-only) observer datadir. `--datadir` is a global
    // flag, so it can precede the `db load-state` subcommand. This is a one-shot process; run it to
    // completion and capture its output. `block_in_place` keeps the blocking wait off the async
    // scheduler without requiring the command to be `Send`.
    let observer_dir = temp_path.join("observer");
    let output = tokio::task::block_in_place(|| {
        bin.command()
            .arg("--datadir")
            .arg(&observer_dir)
            .arg("db")
            .arg("load-state")
            .arg(&bundle_dir)
            .output()
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "db load-state failed (status {:?})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    info!(target: "restart-test", %stdout, "db load-state completed");
    // The resume hint is what makes the node start syncing forward from the import epoch rather
    // than from genesis; its presence in the output is the load-side proof the bundle was
    // bootstrappable (records verified, consensus pack rebuilt, slot hint written).
    assert!(
        stdout.contains(&format!("resume syncing from epoch {IMPORT_EPOCH}")),
        "db load-state did not write a resume hint for epoch {IMPORT_EPOCH}:\n{stdout}"
    );

    // Snapshot the validator's execution height now; the observer must climb from the import block
    // up to at least this height, proving it crossed the gap by syncing forward.
    let validator_height = provider.get_block_number().await?;

    // Start the observer against the imported datadir.
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let obs_idx =
        guard.push(start_observer(4, bin, &temp_path, obs_rpc_port, "state_export_import", 0));
    let obs_provider = ProviderBuilder::new().connect_http(obs_url.parse()?);

    // The observer must come up already standing on the imported tip.
    wait_observer(&mut guard, obs_idx, 60, "observer RPC to answer", || async {
        obs_provider.get_block_number().await.is_ok()
    })
    .await?;

    // Its execution head is `>= import_block` the instant it serves RPC: a node that ignored the
    // import and replayed from genesis would report a height below the import block here.
    let obs_start_block = obs_provider.get_block_number().await?;
    assert!(
        obs_start_block >= import_block,
        "observer started at block {obs_start_block}, below the import point {import_block} — it \
         did not bootstrap from the snapshot"
    );
    info!(target: "restart-test", obs_start_block, import_block, "observer started from the import point");

    // Forward sync: the observer climbs from the import block up to the validator's height
    // snapshot, crossing the epochs it did not import.
    wait_observer(
        &mut guard,
        obs_idx,
        (EXPORT_EPOCH_DURATION * 8).max(90),
        "observer to catch up via forward sync from the import point",
        || async { obs_provider.get_block_number().await.is_ok_and(|h| h >= validator_height) },
    )
    .await?;
    info!(target: "restart-test", validator_height, "observer caught up via forward sync");

    // The observer's current epoch advanced past the import epoch — it followed epoch boundaries
    // forward, not merely replayed the imported one.
    wait_observer(
        &mut guard,
        obs_idx,
        (EXPORT_EPOCH_DURATION * 4).max(60),
        &format!("observer epoch to advance past {IMPORT_EPOCH}"),
        || async {
            obs_provider
                .raw_request::<_, u32>("tn_getCurrentEpoch".into(), ())
                .await
                .is_ok_and(|epoch| epoch > IMPORT_EPOCH)
        },
    )
    .await?;

    // The imported block on the observer is byte-for-byte the block the network committed as epoch
    // `IMPORT_EPOCH`'s final state (hash equality, not just same height): this proves the imported
    // EVM state and header are the real ones, not a look-alike at the same number.
    let obs_import_block = get_block(&obs_url, Some(import_block))?;
    let obs_import_hash = obs_import_block
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| eyre::eyre!("observer block {import_block} has no hash field"))?;
    eyre::ensure!(
        obs_import_hash.eq_ignore_ascii_case(&import_hash.to_string()),
        "observer import block {import_block} hash {obs_import_hash} != committed epoch-{IMPORT_EPOCH} \
         final hash {import_hash}"
    );

    // Epoch `IMPORT_EPOCH + 1` was NOT in the bundle (it carried records `0..=IMPORT_EPOCH` and
    // only the epoch-`IMPORT_EPOCH` consensus pack), so a verified record for it on the
    // observer, plus the executed final block it commits, can only have been obtained by
    // syncing forward from the import point.
    let forward_epoch = IMPORT_EPOCH + 1;
    let forward_record =
        fetch_verified_epoch_record(&obs_url, forward_epoch, (EXPORT_EPOCH_DURATION * 6).max(75))
            .await?;
    let forward_block =
        get_block(&obs_url, Some(forward_record.final_state.number)).map_err(|e| {
            eyre::eyre!(
                "observer missing epoch-{forward_epoch} final block {}: {e}",
                forward_record.final_state.number
            )
        })?;
    let forward_hash = forward_block.get("hash").and_then(|v| v.as_str()).ok_or_else(|| {
        eyre::eyre!("observer block {} has no hash field", forward_record.final_state.number)
    })?;
    eyre::ensure!(
        forward_hash.eq_ignore_ascii_case(&forward_record.final_state.hash.to_string()),
        "observer epoch-{forward_epoch} final block hash {forward_hash} != record hash {}",
        forward_record.final_state.hash
    );
    info!(target: "restart-test", forward_epoch, "observer forward-synced a post-import epoch");

    // Liveness/following: a transaction submitted to a validator after the observer joined is
    // reflected on the observer, proving it follows live consensus output rather than serving a
    // frozen snapshot. Submitting to the validator (and confirming on the observer) needs no
    // observer -> committee tx forwarding. Uses the dev-funded `test-source` account (nonce 0),
    // which the transaction stream does not touch.
    let key = get_key("test-source");
    let to_account = address_from_word("state-export-import-target");
    send_and_confirm(&client_urls[1], &obs_url, &key, to_account, 0)?;
    info!(target: "restart-test", "observer reflected a live transaction post-import");

    // Stop the transaction stream and tear everything down.
    stop.store(true, Ordering::Relaxed);
    let _ = stream.await;
    guard.kill_all();
    Ok(())
}

/// A fully IDLE network round-trips a snapshot end to end: the exporter WRITES a bundle for an
/// epoch that contains no genuine worker block, and an observer bootstraps from that bundle and
/// follows the chain forward.
///
/// This is the inverse of what this test used to assert. An idle EIP-1559 worker used to have no
/// chain-observable fee anchor in the exported epoch, so a restored node would walk BACKWARD below
/// the snapshot block `B` into state the snapshot omitted and halt; the exporter defended against
/// that by refusing to write an idle epoch's bundle at all. The entry read is now ONE pinned read
/// at `B` — the closing block's own system call recorded each worker's next-epoch fee into its
/// `WorkerConfigs` row — so worker activity in the exported epoch is irrelevant and the hazard is
/// gone. The only remaining requirement is that `B` closed an epoch, which every exported boundary
/// satisfies by construction.
///
/// Under skip-empty-execution an idle epoch produces only its epoch-closing block, so this is also
/// the sparsest possible chain to bootstrap from: the observer's forward sync crosses epochs whose
/// entire block content is one closing block each.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_state_export_import_idle_epoch() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_state_export_import_idle_epoch_inner())
}

async fn test_state_export_import_idle_epoch_inner() -> eyre::Result<()> {
    info!(target: "restart-test", "test_state_export_import_idle_epoch");
    let tmp_guard = tempfile::TempDir::with_prefix("state_export_idle").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();

    // No funded stream account and no transaction stream: this network runs IDLE on purpose. That
    // is the whole point — every exported epoch is one the old fee-walk precondition rejected.
    //
    // Do NOT add traffic here to make room for a base-fee assertion. This fixture leaves the
    // genesis fee config at its `target_gas: u64::MAX` default, which pins every worker at
    // `MIN_PROTOCOL_BASE_FEE` for the whole run, so any fee assertion written here is a tautology.
    // Fee *values* are covered by `test_state_export_import_recovers_recorded_fee` below, which
    // pays for a moving fee with warm-up traffic and therefore cannot also be a fully idle
    // network.
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EXPORT_EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    // 4-validator committee, exporter on validator-1 (instance 0), NO tx stream.
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; 4] = Default::default();
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        *url = format!("http://127.0.0.1:{rpc_port}");
        let child = if i == 0 {
            start_validator_with_args(
                i,
                bin,
                &temp_path,
                rpc_port,
                "state_export_idle",
                0,
                &["--enable-state-export"],
            )
        } else {
            start_validator(i, bin, &temp_path, rpc_port, "state_export_idle", 0)
        };
        guard.push(child);
    }

    network_advancing(&client_urls)?;

    // Let the network run a few epochs past the import epoch so the bundle is fully written and
    // there is a real forward-sync gap for the observer to cross.
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4 * MIN_LEAD_EPOCH as u64),
        &format!("network to reach epoch {MIN_LEAD_EPOCH}"),
        || async { Ok(registry.getCurrentEpochInfo().call().await?.epochId >= MIN_LEAD_EPOCH) },
    )
    .await?;
    info!(target: "restart-test", "idle network reached epoch {MIN_LEAD_EPOCH}");

    // THE HEADLINE ASSERTION: the exporter wrote a bundle for an idle epoch. Written atomically
    // (temp dir renamed into place), so the directory only appears once complete.
    let bundle_dir = temp_path
        .join("validator-1")
        .join("consensus-db")
        .join("state_exports")
        .join(format!("epoch-{IMPORT_EPOCH}"));
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4),
        &format!("exporter to write the IDLE epoch-{IMPORT_EPOCH} bundle"),
        || async { Ok(bundle_dir.is_dir()) },
    )
    .await?;
    for file in ["state_data", "consensus_data", "epoch_records", "epoch_certs"] {
        assert!(
            bundle_dir.join(file).is_file(),
            "export bundle {bundle_dir:?} is missing `{file}`"
        );
    }
    info!(target: "restart-test", ?bundle_dir, "exporter produced a bundle for an IDLE epoch");

    // And it never took the skip path. The exporter is instance 0, so its stdout is captured to
    // node0-run0.log under this test's log dir. This is the direct negative of the assertion this
    // test used to make.
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set");
    let exporter_log = std::path::Path::new(&manifest)
        .join("test_logs")
        .join("state_export_idle")
        .join("node0-run0.log");
    if let Ok(log) = std::fs::read_to_string(&exporter_log) {
        assert!(
            !log.contains("snapshot would not be resumable"),
            "exporter skipped an export it should now accept (idle epochs are exportable)"
        );
    }

    // Anchor the import point to what the network committed: the certified epoch-`IMPORT_EPOCH`
    // record names the epoch's final executed block (number + hash).
    let import_record = fetch_verified_epoch_record(
        &client_urls[0],
        IMPORT_EPOCH,
        (EXPORT_EPOCH_DURATION * 4).max(60),
    )
    .await?;
    let import_block = import_record.final_state.number;
    let import_hash = import_record.final_state.hash;
    info!(target: "restart-test", IMPORT_EPOCH, import_block, %import_hash, "anchored idle import point");

    // Import the bundle into the fresh, config-only observer datadir. `db load-state` runs the
    // entry-readiness precondition against the imported state, so a bundle that could not seed the
    // node's first epoch entry would be rejected right here.
    let observer_dir = temp_path.join("observer");
    let output = tokio::task::block_in_place(|| {
        bin.command()
            .arg("--datadir")
            .arg(&observer_dir)
            .arg("db")
            .arg("load-state")
            .arg(&bundle_dir)
            .output()
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "db load-state rejected an idle-epoch bundle (status {:?})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains(&format!("resume syncing from epoch {IMPORT_EPOCH}")),
        "db load-state did not write a resume hint for epoch {IMPORT_EPOCH}:\n{stdout}"
    );
    info!(target: "restart-test", %stdout, "db load-state accepted the idle-epoch bundle");

    // Snapshot the validator's height; the observer must climb from the import block to at least
    // here, proving it crossed the gap by syncing forward.
    let validator_height = provider.get_block_number().await?;

    // Start the observer against the imported datadir.
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let obs_idx =
        guard.push(start_observer(4, bin, &temp_path, obs_rpc_port, "state_export_idle", 0));
    let obs_provider = ProviderBuilder::new().connect_http(obs_url.parse()?);

    wait_observer(&mut guard, obs_idx, 60, "observer RPC to answer", || async {
        obs_provider.get_block_number().await.is_ok()
    })
    .await?;

    // It comes up standing on the imported tip, not replaying from genesis. Reaching this point at
    // all is the proof the restored node's FIRST EPOCH ENTRY succeeded on an idle-epoch snapshot —
    // the exact case that used to halt it.
    let obs_start_block = obs_provider.get_block_number().await?;
    assert!(
        obs_start_block >= import_block,
        "observer started at block {obs_start_block}, below the import point {import_block} — it \
         did not bootstrap from the snapshot"
    );
    info!(target: "restart-test", obs_start_block, import_block, "observer started from the idle import point");

    // Forward sync across the epochs it did not import.
    wait_observer(
        &mut guard,
        obs_idx,
        (EXPORT_EPOCH_DURATION * 8).max(90),
        "observer to catch up via forward sync from the idle import point",
        || async { obs_provider.get_block_number().await.is_ok_and(|h| h >= validator_height) },
    )
    .await?;
    info!(target: "restart-test", validator_height, "observer caught up via forward sync");

    // It crossed epoch boundaries forward rather than merely replaying the imported one. Each of
    // those boundaries ran the entry read against an idle epoch's closing block.
    wait_observer(
        &mut guard,
        obs_idx,
        (EXPORT_EPOCH_DURATION * 4).max(60),
        &format!("observer epoch to advance past {IMPORT_EPOCH}"),
        || async {
            obs_provider
                .raw_request::<_, u32>("tn_getCurrentEpoch".into(), ())
                .await
                .is_ok_and(|epoch| epoch > IMPORT_EPOCH)
        },
    )
    .await?;

    // The imported block is byte-for-byte the block the network committed as epoch
    // `IMPORT_EPOCH`'s final state (hash equality, not just same height).
    let obs_import_block = get_block(&obs_url, Some(import_block))?;
    let obs_import_hash = obs_import_block
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| eyre::eyre!("observer block {import_block} has no hash field"))?;
    eyre::ensure!(
        obs_import_hash.eq_ignore_ascii_case(&import_hash.to_string()),
        "observer import block {import_block} hash {obs_import_hash} != committed epoch-{IMPORT_EPOCH} \
         final hash {import_hash}"
    );

    // Epoch `IMPORT_EPOCH + 1` was not in the bundle, so a verified record for it on the observer
    // can only have come from syncing forward.
    let forward_epoch = IMPORT_EPOCH + 1;
    let forward_record =
        fetch_verified_epoch_record(&obs_url, forward_epoch, (EXPORT_EPOCH_DURATION * 6).max(75))
            .await?;
    let forward_block =
        get_block(&obs_url, Some(forward_record.final_state.number)).map_err(|e| {
            eyre::eyre!(
                "observer missing epoch-{forward_epoch} final block {}: {e}",
                forward_record.final_state.number
            )
        })?;
    let forward_hash = forward_block.get("hash").and_then(|v| v.as_str()).ok_or_else(|| {
        eyre::eyre!("observer block {} has no hash field", forward_record.final_state.number)
    })?;
    eyre::ensure!(
        forward_hash.eq_ignore_ascii_case(&forward_record.final_state.hash.to_string()),
        "observer epoch-{forward_epoch} final block hash {forward_hash} != record hash {}",
        forward_record.final_state.hash
    );
    info!(target: "restart-test", forward_epoch, "observer forward-synced a post-import epoch");

    // Liveness: the observer reflects a transaction submitted to a validator AFTER it joined, so it
    // follows live consensus output rather than serving a frozen snapshot. Uses the dev-funded
    // `test-source` account (nonce 0). This is the first and only traffic on the chain, so every
    // epoch the export and import depended on stayed idle.
    let key = get_key("test-source");
    let to_account = address_from_word("state-export-idle-target");
    send_and_confirm(&client_urls[1], &obs_url, &key, to_account, 0)?;
    info!(target: "restart-test", "observer reflected a live transaction after an idle-epoch bootstrap");

    guard.kill_all();
    Ok(())
}

/// Worker 0's EIP-1559 gas target at genesis for the recorded-fee test.
///
/// `1` is the smallest useful target: any transaction at all overshoots it, so the fee takes the
/// full +12.5% step at every boundary that saw gas and the full -12.5% step at every boundary that
/// did not. At single-digit-wei magnitudes integer arithmetic makes both steps exactly 1 wei
/// (`MIN_PROTOCOL_BASE_FEE` is 7), which is what lets this test predict the recorded value exactly.
const FEE_TARGET_GAS: u64 = 1;

/// The fee (wei) worker 0 must reach before the warm-up stops generating traffic.
///
/// Every epoch after the warm-up is idle, and each idle boundary decays the fee one wei, so this
/// floor is a budget for how many idle boundaries can pass before the exported one and still leave
/// the recorded word clear of `MIN_PROTOCOL_BASE_FEE` (7). At `10` the exported epoch is at worst
/// two boundaries past the last gas-bearing one, which records `8` — still above MIN and still
/// below the exported epoch's own fee, the two facts the final assertion needs. In the common case
/// it records `10`, three wei of margin.
const FEE_WARMUP_FLOOR: u64 = 10;

/// Hard bound on warm-up iterations. Four suffice (7 -> 8 -> 9 -> 10). The loop keys off the
/// *measured* fee rather than an epoch count, so an epoch poll that skips an epoch — inserting an
/// idle, decaying epoch into the ladder — costs an extra iteration instead of silently shifting the
/// ladder out from under the oracle.
const FEE_WARMUP_MAX_EPOCHS: usize = 8;

/// Gas price for warm-up transfers, far above the single-digit-wei base fee this test runs at, so
/// the pool never treats a warm-up transaction as underpriced.
const FEE_WARMUP_GAS_PRICE: u128 = 250;

/// Value moved by each warm-up transfer (0.001 TEL) — small enough that the dev-funded
/// `test-source` account covers every iteration.
const FEE_WARMUP_AMOUNT: u128 = 1_000_000_000_000_000;

/// Read worker 0's RECORDED entry fee out of `WorkerConfigs` as of `block`.
///
/// This is the production quantity, pinned to a historical block: the `data` word of worker 0's
/// row, floored at `MIN_PROTOCOL_BASE_FEE` exactly as
/// `tn_types::gas_accumulator::entry_fee_for_worker` floors it. A node entering the epoch that
/// follows `block` reads this and nothing else, which makes it the right oracle — no inference
/// about which block header carries which epoch's fee.
async fn recorded_entry_fee<P: Provider>(provider: &P, block: u64) -> eyre::Result<u64> {
    let configs = WorkerConfigs::new(WORKER_CONFIGS_ADDRESS, provider);
    let row = configs.getWorkerConfig(0).block(block.into()).call().await?;
    let recorded = u64::try_from(row.data).map_err(|_| {
        eyre::eyre!("worker 0's recorded data word at block {block} exceeds u64: {}", row.data)
    })?;
    Ok(recorded.max(MIN_PROTOCOL_BASE_FEE))
}

/// Render `from..=to` as `block:base_fee` pairs for an assertion message.
///
/// Which block carries which epoch's fee is the easiest thing to get wrong about this test, so
/// every fee assertion below quotes this window instead of leaving a bare `left != right` behind.
/// Blocks that cannot be read are rendered `?` rather than failing the render.
fn fee_window(node: &str, from: u64, to: u64) -> String {
    (from..=to)
        .map(|b| match read_base_fee(node, b) {
            Ok(fee) => format!("{b}:{fee}"),
            Err(_) => format!("{b}:?"),
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Submit one warm-up transfer, wait for it to confirm, and return `(block, base_fee)` for the
/// block that ACTUALLY included it.
///
/// Attribution comes from the receipt rather than the tip: the rising balance is only the landing
/// signal, and the tip can move (e.g. an epoch-close block) between the balance poll and a tip
/// read. The block's `base_fee_per_gas` is the fee of the epoch that will be credited with this
/// transaction's gas, which is exactly the input the fee ladder is built from.
async fn land_fee_warmup_tx(
    node: &str,
    funded_key: &str,
    to: Address,
    nonce: u128,
) -> eyre::Result<(u64, u64)> {
    let before = get_balance(node, &to.to_string(), 1).unwrap_or(0);
    let tx_hash =
        send_tel(node, funded_key, to, FEE_WARMUP_AMOUNT, FEE_WARMUP_GAS_PRICE, 21_000, nonce)?;

    // Two epoch durations covers a transaction orphaned at a boundary and re-injected into the next
    // epoch.
    let budget = EXPORT_EPOCH_DURATION * 2 + 5;
    let deadline = Instant::now() + Duration::from_secs(budget);
    loop {
        if get_balance(node, &to.to_string(), 1).unwrap_or(before) > before {
            break;
        }
        if Instant::now() >= deadline {
            eyre::bail!("warm-up transfer to {to} did not confirm within {budget}s");
        }
        // Poll ~4x/sec: at a 10s epoch a 1s cadence is coarse relative to block confirmation.
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let block = get_tx_receipt_block(node, &tx_hash)?;
    let fee = read_base_fee(node, block)?;
    Ok((block, fee))
}

/// A snapshot-imported node enters its first epoch on the base fee the snapshot block RECORDED —
/// not on `MIN_PROTOCOL_BASE_FEE`, and not on the exported epoch's own (higher) fee.
///
/// ## Why this needs its own fixture
///
/// The other two tests in this file take the genesis default fee config
/// (`Eip1559 { target_gas: u64::MAX }`), which is inert: it pins every worker at
/// `MIN_PROTOCOL_BASE_FEE` for the life of the chain. An `== MIN` assertion under that config
/// cannot fail, whatever the import does with the recorded word, so it proves nothing. Switching
/// the worker to a static fee does not fix it either: `entry_fee_for_worker` returns a static row's
/// configured fee and never reads the `data` word, so a static worker never exercises the
/// recorded-word read.
///
/// So this test sets `Eip1559 { target_gas: 1 }` at genesis (the same knob
/// `basefee.rs::test_boundary_kill_restart_recovers_next_epoch_fee` uses) and pays for a moving fee
/// with warm-up traffic.
///
/// ## Shape
///
/// 1. **Warm up.** Land one transfer per epoch until worker 0's fee is at least
///    [`FEE_WARMUP_FLOOR`], then stop. Traffic must start in epoch 1 or later: epoch 0 has no
///    previous epoch to price from and always runs at MIN.
/// 2. **Go quiet** and take the import epoch as `current + 1`, read from the registry after the
///    last warm-up transfer confirmed. Nothing is submitted after that read, so the import epoch is
///    idle by construction — the same case the companion idle test covers — and its close records
///    one decay step down from its own fee.
/// 3. **Export and import** that idle epoch's bundle into a fresh observer.
/// 4. **Assert the value.** The oracle is the recorded word itself, read from `WorkerConfigs`
///    pinned to the snapshot block, so nothing here depends on inferring which block header carries
///    which epoch's fee. Both the live network and the imported observer must price the import
///    epoch's successor at that word, which is provably neither MIN nor the word the previous
///    boundary recorded.
///
/// The fixture guard in step 4 is load-bearing: without it, any future change that flattens the fee
/// (a different genesis default, a wider gas target, a warm-up that stops landing) turns the
/// headline assertion back into the `== MIN` tautology this test was written to escape, silently.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_state_export_import_recovers_recorded_fee() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_state_export_import_recovers_recorded_fee_inner())
}

async fn test_state_export_import_recovers_recorded_fee_inner() -> eyre::Result<()> {
    info!(target: "restart-test", "test_state_export_import_recovers_recorded_fee");
    let tmp_guard = tempfile::TempDir::with_prefix("state_export_fee").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();

    // Worker 0 on a live EIP-1559 strategy. This is the only fixture builder that takes a fee
    // config, and it is the whole reason this test is separate from the two above.
    config_local_testnet_with_worker_fee_configs(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EXPORT_EPOCH_DURATION as u32),
        &[&format!("0:0:{FEE_TARGET_GAS}")],
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    // 4-validator committee, exporter on validator-1 (instance 0).
    let mut guard = ProcessGuard::empty();
    let mut client_urls: [String; 4] = Default::default();
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        *url = format!("http://127.0.0.1:{rpc_port}");
        let child = if i == 0 {
            start_validator_with_args(
                i,
                bin,
                &temp_path,
                rpc_port,
                "state_export_fee",
                0,
                &["--enable-state-export"],
            )
        } else {
            start_validator(i, bin, &temp_path, rpc_port, "state_export_fee", 0)
        };
        guard.push(child);
    }

    network_advancing(&client_urls)?;
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);

    // ---- 1) Warm-up: one transfer per epoch until worker 0's fee clears `FEE_WARMUP_FLOOR`.
    // `test-source` sends every transfer, so its nonce must stay monotonic.
    let funded_key = get_key("test-source");
    let mut nonce: u128 = 0;
    let mut last_gas_epoch: Option<(u32, u64)> = None;
    for i in 0..FEE_WARMUP_MAX_EPOCHS {
        let next_epoch = last_gas_epoch.map_or(1, |(epoch, _)| epoch + 1);
        wait_for_epoch_at_least(&provider, next_epoch).await?;
        // Land on a MEASURED mid-epoch phase so the transaction clears both boundaries and the
        // epoch credited with its gas is exactly the one measured here.
        let snap = wait_for_mid_epoch(&provider, &client_urls[0]).await?;
        let to = address_from_word(&format!("state-export-fee-warmup-{i}"));
        let (block, fee) =
            land_fee_warmup_tx(&client_urls[0], &funded_key, to, nonce).await.map_err(|e| {
                eyre::eyre!(
                    "epoch {}: warm-up tx (nonce {nonce}) did not confirm: {e}. Check \
                     crates/e2e-tests/test_logs/state_export_fee/",
                    snap.epoch_id
                )
            })?;
        nonce += 1;
        info!(target: "restart-test", epoch = snap.epoch_id, block, fee, "fee warm-up epoch");
        last_gas_epoch = Some((snap.epoch_id, fee));
        if fee >= FEE_WARMUP_FLOOR {
            break;
        }
    }
    let (gas_epoch, gas_epoch_fee) = last_gas_epoch.expect("the warm-up loop runs at least once");
    eyre::ensure!(
        gas_epoch_fee >= FEE_WARMUP_FLOOR,
        "worker 0's fee only reached {gas_epoch_fee} after {FEE_WARMUP_MAX_EPOCHS} warm-up epochs \
         (need {FEE_WARMUP_FLOOR}); the ladder below cannot separate a recorded fee from MIN. Check \
         crates/e2e-tests/test_logs/state_export_fee/"
    );

    // ---- 2) Go quiet, and pick the import epoch from the chain rather than from `gas_epoch`.
    //
    // No transaction is submitted after this point, and this read happens after the last warm-up
    // transfer confirmed, so every epoch strictly greater than the one the registry reports here is
    // guaranteed to see zero gas. Taking `current + 1` makes the import epoch idle by construction
    // even if a warm-up transfer was orphaned across a boundary and credited to a later epoch than
    // the one measured when it was sent.
    let after_warmup = current_epoch(&provider).await?;
    let import_epoch = after_warmup.epoch_id + 1;
    let min_lead_epoch = import_epoch + 2;
    info!(
        target: "restart-test",
        gas_epoch, gas_epoch_fee, after_warmup = after_warmup.epoch_id, import_epoch,
        "fee warm-up complete; going quiet"
    );

    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4 * min_lead_epoch as u64),
        &format!("network to reach epoch {min_lead_epoch}"),
        || async { Ok(registry.getCurrentEpochInfo().call().await?.epochId >= min_lead_epoch) },
    )
    .await?;

    // ---- 3) Import the idle epoch's bundle into a fresh observer datadir.
    let bundle_dir = temp_path
        .join("validator-1")
        .join("consensus-db")
        .join("state_exports")
        .join(format!("epoch-{import_epoch}"));
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4),
        &format!("exporter to write the epoch-{import_epoch} bundle"),
        || async { Ok(bundle_dir.is_dir()) },
    )
    .await?;

    let import_record = fetch_verified_epoch_record(
        &client_urls[0],
        import_epoch,
        (EXPORT_EPOCH_DURATION * 4).max(60),
    )
    .await?;
    let import_block = import_record.final_state.number;
    let import_hash = import_record.final_state.hash;

    // ---- 4) The oracle, read straight out of chain state rather than derived from a block header.
    //
    // What a node entering epoch `import_epoch + 1` reads is one thing: worker 0's recorded `data`
    // word in `WorkerConfigs` as of `import_block`. Read exactly that, pinned to that block, so the
    // oracle is the production quantity and not an inference about which block carries which
    // epoch's fee.
    let expected = recorded_entry_fee(&provider, import_block).await?;
    // The stale alternative a broken import could serve instead: the word the PREVIOUS boundary
    // recorded, which is the fee the import epoch itself ran at. `import_block - 1` is that
    // boundary, because an idle epoch produces exactly one block.
    let stale = recorded_entry_fee(&provider, import_block - 1).await?;
    let window = fee_window(&client_urls[0], import_block.saturating_sub(3), import_block + 1);
    info!(
        target: "restart-test",
        import_epoch, import_block, %import_hash, expected, stale, %window,
        "anchored the recorded-fee import point"
    );

    // FIXTURE GUARD. Both inequalities have to hold or the headline assertion below is vacuous:
    // `> MIN` is what separates the recorded word from a defaulted one, and `< stale` is what
    // separates it from the previous boundary's word carried over unchanged. The second also
    // confirms the import epoch really was idle — a boundary that saw gas steps the word UP.
    eyre::ensure!(
        expected > MIN_PROTOCOL_BASE_FEE && expected < stale,
        "fixture is degenerate: epoch {import_epoch}'s closing block {import_block} recorded \
         {expected} for epoch {}, which must sit strictly between MIN ({MIN_PROTOCOL_BASE_FEE}) and \
         the previous boundary's {stale}. Otherwise this test cannot tell a recovered fee from a \
         defaulted or a stale one. Base fees near the import point: {window}. Check \
         crates/e2e-tests/test_logs/state_export_fee/",
        import_epoch + 1
    );

    let observer_dir = temp_path.join("observer");
    let output = tokio::task::block_in_place(|| {
        bin.command()
            .arg("--datadir")
            .arg(&observer_dir)
            .arg("db")
            .arg("load-state")
            .arg(&bundle_dir)
            .output()
    })?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "db load-state rejected the epoch-{import_epoch} bundle (status {:?})\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        stdout.contains(&format!("resume syncing from epoch {import_epoch}")),
        "db load-state did not write a resume hint for epoch {import_epoch}:\n{stdout}"
    );

    // ---- 5) Control: the live network prices the import epoch's successor at `expected`.
    // `concludeEpoch` records the entered epoch's first block as `closing block + 1`, so
    // `import_block + 1` is the first block of epoch `import_epoch + 1`. Asserting it here pins the
    // block the recorded word priced *from the chain*, so the observer comparison below is against
    // a block whose expected fee is established rather than assumed.
    let first_new_block = import_block + 1;
    let network_fee = read_base_fee(&client_urls[0], first_new_block)?;
    assert_eq!(
        network_fee, expected,
        "the live network served epoch {}'s first block {first_new_block} at fee {network_fee}, but \
         epoch {import_epoch}'s closing block {import_block} recorded {expected}. Base fees near the \
         import point: {window}",
        import_epoch + 1
    );

    let validator_height = provider.get_block_number().await?;

    // ---- 6) Start the observer on the imported datadir and let it cross the gap.
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let obs_idx =
        guard.push(start_observer(4, bin, &temp_path, obs_rpc_port, "state_export_fee", 0));
    let obs_provider = ProviderBuilder::new().connect_http(obs_url.parse()?);

    wait_observer(&mut guard, obs_idx, 60, "observer RPC to answer", || async {
        obs_provider.get_block_number().await.is_ok()
    })
    .await?;

    let obs_start_block = obs_provider.get_block_number().await?;
    assert!(
        obs_start_block >= import_block,
        "observer started at block {obs_start_block}, below the import point {import_block} — it \
         did not bootstrap from the snapshot"
    );

    wait_observer(
        &mut guard,
        obs_idx,
        (EXPORT_EPOCH_DURATION * 8).max(90),
        "observer to catch up via forward sync from the import point",
        || async { obs_provider.get_block_number().await.is_ok_and(|h| h >= validator_height) },
    )
    .await?;

    // The imported block is the one the network committed as epoch `import_epoch`'s final state, so
    // the fee read below is anchored to the right block and not merely to the right height.
    let obs_import_block = get_block(&obs_url, Some(import_block))?;
    let obs_import_hash = obs_import_block
        .get("hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| eyre::eyre!("observer block {import_block} has no hash field"))?;
    eyre::ensure!(
        obs_import_hash.eq_ignore_ascii_case(&import_hash.to_string()),
        "observer import block {import_block} hash {obs_import_hash} != committed epoch-{import_epoch} \
         final hash {import_hash}"
    );

    // ---- 7) THE HEADLINE ASSERTION. The observer's only source for epoch `import_epoch + 1`'s fee
    // is the word recorded in the imported snapshot block: it holds no pre-`B` history to walk and
    // nothing carried in memory. A failed entry read would have left it on MIN
    // (`MIN_PROTOCOL_BASE_FEE`, ruled out by the guard) or on the previous boundary's word
    // (`stale`, also ruled out) — and either would put a different `base_fee_per_gas` in its
    // executed block than the one the network committed at the same height.
    let obs_fee = read_base_fee(&obs_url, first_new_block)?;
    assert_eq!(
        obs_fee,
        expected,
        "the snapshot-imported observer serves epoch {}'s first block {first_new_block} at fee \
         {obs_fee}, expected the recorded {expected} (the previous boundary recorded {stale}; MIN is \
         {MIN_PROTOCOL_BASE_FEE}). Network fees near the import point: {window}. Check \
         crates/e2e-tests/test_logs/state_export_fee/",
        import_epoch + 1
    );
    info!(
        target: "restart-test",
        import_epoch, first_new_block, expected, stale,
        "observer entered the post-import epoch on the RECORDED fee"
    );

    guard.kill_all();
    Ok(())
}
