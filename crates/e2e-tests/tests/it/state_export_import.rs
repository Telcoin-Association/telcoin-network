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
//! ## Why the network must not be idle
//!
//! When a node enters an epoch it derives each worker's base fee, and for an EIP-1559 worker that
//! produced no genuine block in the previous epoch it walks BACKWARD through earlier epochs reading
//! `WorkerConfigs` state to find a fee anchor (`derive_idle_worker_fee_at`). A full node has all of
//! history (archive mode), but a snapshot-imported node only has state at the snapshot block `B`
//! and forward — so an idle exported epoch would make the restored node walk below `B` into state
//! the snapshot omitted and halt. That is exactly the precondition
//! `SnapshotRestorer::derive_fee_precondition` guards. To keep the exported epoch (and every epoch
//! the observer later enters) anchorable at `B`, this test runs a steady transaction stream, so
//! every epoch contains a genuine worker block. This mirrors a real network, which is never idle. A
//! companion test in this file (`test_state_export_skips_idle_epoch_bundle`) runs an idle network
//! to prove the exporter *skips* producing a bundle for an epoch it could not resume from, rather
//! than writing one that would be rejected at import.
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
use e2e_tests::config_local_testnet_with_epoch_duration;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::wait_until;
use tn_types::{get_available_tcp_port, Genesis, GenesisAccount, U256};
use tracing::info;

use crate::common::{
    address_from_word, fetch_verified_epoch_record, get_block, get_key,
    get_positive_balance_with_retry, network_advancing, send_and_confirm, start_observer,
    start_validator, start_validator_with_args, ProcessGuard,
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

/// The exporter must NOT produce a bundle it knows is un-resumable. On an IDLE network (epochs with
/// no genuine worker block), each epoch `>= 1`'s snapshot fails the fee-derivability precheck, so
/// the exporter skips it and writes no bundle. Epoch 0 is still exported (entering epoch 1 never
/// walks below the snapshot, so it is fee-resumable). The importer's reject guard is the
/// counterpart, unit-tested in `tn-reth`; an idle bundle can no longer reach it via a real
/// exporter.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_state_export_skips_idle_epoch_bundle() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_state_export_skips_idle_epoch_bundle_inner())
}

async fn test_state_export_skips_idle_epoch_bundle_inner() -> eyre::Result<()> {
    info!(target: "restart-test", "test_state_export_skips_idle_epoch_bundle");
    let tmp_guard = tempfile::TempDir::with_prefix("state_export_skip").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    // No funded stream account: this network runs IDLE on purpose. Under skip-empty-execution an
    // idle epoch produces only its epoch-closing block, which is not a genuine worker batch block,
    // so each epoch >= 1's snapshot has no fee anchor and the exporter must skip it.
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(EXPORT_EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    // 4-validator committee, exporter on validator-1 (instance 0 -> node0 log), NO tx stream.
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
                "state_export_skip",
                0,
                &["--enable-state-export"],
            )
        } else {
            start_validator(i, bin, &temp_path, rpc_port, "state_export_skip", 0)
        };
        guard.push(child);
    }

    network_advancing(&client_urls)?;

    // Advance a few epochs so the exporter has processed several idle boundaries.
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4 * MIN_LEAD_EPOCH as u64),
        &format!("network to reach epoch {MIN_LEAD_EPOCH}"),
        || async { Ok(registry.getCurrentEpochInfo().call().await?.epochId >= MIN_LEAD_EPOCH) },
    )
    .await?;

    // Positive proof the exporter ran the precheck and skipped: its node log records the skip. The
    // exporter is instance 0, so its stdout is captured to node0-run0.log under this test's dir.
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set");
    let exporter_log = std::path::Path::new(&manifest)
        .join("test_logs")
        .join("state_export_skip")
        .join("node0-run0.log");
    wait_until(
        Duration::from_secs(EXPORT_EPOCH_DURATION * 4),
        "exporter to log an idle-epoch export skip",
        || async {
            Ok(std::fs::read_to_string(&exporter_log)
                .map(|log| log.contains("snapshot would not be resumable"))
                .unwrap_or(false))
        },
    )
    .await?;

    // Direct observable: the idle epoch-IMPORT_EPOCH bundle was never written. Epoch IMPORT_EPOCH
    // closed well before MIN_LEAD_EPOCH, so its (skip) export decision is complete by now.
    let bundle_dir = temp_path
        .join("validator-1")
        .join("consensus-db")
        .join("state_exports")
        .join(format!("epoch-{IMPORT_EPOCH}"));
    assert!(
        !bundle_dir.exists(),
        "exporter wrote a bundle for idle epoch {IMPORT_EPOCH} that it should have skipped: \
         {bundle_dir:?}"
    );
    info!(target: "restart-test", "exporter correctly skipped the idle epoch-{IMPORT_EPOCH} bundle");

    guard.kill_all();
    Ok(())
}
