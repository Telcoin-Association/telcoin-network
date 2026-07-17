//! E2e tests for the cloud snapshot feature: an observer uploads a signed state snapshot to a
//! bucket at each epoch boundary, and a fresh node auto-restores that boundary state and then
//! catches up to the live tip via normal state-sync.
//!
//! These mirror the process-based harness idioms in `sync.rs`
//! (`test_observer_pack_imports_after_epoch_close`): a real 4-validator network is spawned from the
//! compiled `telcoin-network` binary, epochs are driven on a short (10s) cadence, and progress is
//! observed over RPC. The snapshot bucket is a credential-free `file://` object store rooted in the
//! test's temp dir, so the whole upload/restore round trip runs with no cloud dependencies.
//!
//! Feature constraint that shapes the upload test: the uploader refuses to publish an epoch whose
//! base fees are not chain-derivable, and the default `Eip1559 { target_gas: u64::MAX }` worker
//! never produces a genuine batch block on an idle chain, so every epoch would be skipped and
//! `latest.json` never written. Worker 0 is therefore given a `Static` fee (via the same genesis
//! knob `basefee.rs` uses); a `Static` worker's fee is pinned by config, so every epoch is
//! publishable. Pinning it at `MIN_PROTOCOL_BASE_FEE` keeps the base fee at the protocol floor so
//! the fixed-gas-price liveness transaction still lands. The uploader also skips epoch-0 publishes
//! (an epoch-0 snapshot is unsupported — a fresh node covers epoch 0 by syncing from genesis), so
//! `latest.json` only ever advertises an epoch `>= MIN_RESTORABLE_EPOCH`.

use alloy::providers::{Provider, ProviderBuilder};
use clap::Parser as _;
use e2e_tests::{
    config_local_testnet_with_epoch_duration, config_local_testnet_with_worker_fee_configs,
    get_telcoin_network_binary, setup_log_dir, TestBinary,
};
use std::{path::Path, process::Child, time::Duration};
use telcoin_network_cli::keytool::KeyArgs;
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_types::{get_available_tcp_port, test_utils::CommandParser, MIN_PROTOCOL_BASE_FEE};
use tokio::time::Instant;
use tracing::info;

use crate::common::{
    address_from_word, get_balance, get_balance_above_with_retry, get_key, network_advancing,
    send_tel, start_validator, start_validator_with_args, ProcessGuard, WEI_PER_TEL,
};

/// Epoch duration (seconds) for the snapshot tests. Matches `epochs.rs::EPOCH_DURATION` /
/// `sync.rs::PACK_IMPORT_EPOCH_DURATION` so epoch boundaries — and therefore snapshot publishes —
/// occur on the same cadence the rest of the process-based suite relies on.
const SNAPSHOT_EPOCH_DURATION: u64 = 10;

/// BLS passphrase for every node in these tests. Matches the value `start_validator` /
/// `start_validator_with_args` hard-code as `TN_BLS_PASSPHRASE`, so the genesis ceremony must
/// generate keys under the same passphrase.
const SNAPSHOT_TEST_PASSPHRASE: &str = "restart_test";

/// EVM chain id of the local testnet genesis (`0xde7e1`), the genesis-ceremony default and the
/// chain `send_tel` signs against. The snapshot manifest and pointer are bound to it.
const SNAPSHOT_CHAIN_ID: u64 = 911329;

/// Lowest epoch a snapshot can be restored/verified from. Epoch-0 snapshots are unsupported
/// (`crates/snapshot/src/verify.rs`: "nothing precedes genesis; sync from genesis instead"), and
/// the uploader now skips publishing them, so the bucket only ever advertises at least this epoch.
const MIN_RESTORABLE_EPOCH: u64 = 1;

/// Allowed lag (in EVM blocks) between the restored node's tip and a validator's tip when checking
/// catch-up parity. Small, but non-zero to tolerate the ~1s sync lag as each new epoch-close block
/// propagates.
const PARITY_DELTA: u64 = 3;

/// Log directory name (under `test_logs/`) for the upload+restore test.
const UPLOAD_RESTORE_TEST: &str = "snapshot_upload_restore";

/// Log directory name (under `test_logs/`) for the observer-only gate test.
const UPLOAD_REJECT_TEST: &str = "snapshot_upload_reject";

/// End-to-end snapshot upload and restore across an epoch boundary.
///
/// A 4-validator network plus one uploader observer (`--snapshot-upload file://<bucket>`) runs on a
/// 10s epoch cadence, with worker 0 on a `Static` fee so every epoch is publishable (see the module
/// docs). Once epochs close and their certificates aggregate, the uploader publishes snapshots and
/// writes `latest.json` last. This test then:
///
/// 1. waits for a restorable (epoch `>= MIN_RESTORABLE_EPOCH`) snapshot to appear in the bucket,
/// 2. asserts the bucket shape (the `latest.json` pointer parses, names an
///    `epoch-<N>/manifest.json` that exists, and that manifest lists the expected artifacts, all
///    present on disk),
/// 3. runs the standalone `snapshot verify` subcommand against a fresh trust root as a
///    bucket-integrity check that, deep by default, also recomputes the boundary state root from
///    scratch,
/// 4. spawns a FRESH observer (new datadir, same genesis fixtures) with `--snapshot-source file://<bucket>`
///    so it auto-restores before opening its database,
/// 5. asserts the auto-restore installed the epoch (the restore receipt on disk) and that the
///    restored node serves the installed boundary EVM state over RPC, and
/// 6. asserts the restored node RESUMES: its EVM tip advances past the restored boundary block to
///    parity with a validator's tip, and a transaction submitted to the network after the restore
///    becomes visible on the restored node's own view.
///
/// Ignore-gated like the other epoch-cadence process tests; run with
/// `cargo nextest run -p e2e-tests --run-ignored ignored-only -E 'test(snapshot)'`.
#[test]
#[ignore = "should not run with a default cargo test, run snapshot tests as a separate step"]
fn test_snapshot_upload_and_restore_across_epoch_boundary() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_snapshot_upload_and_restore_across_epoch_boundary_inner())
}

async fn test_snapshot_upload_and_restore_across_epoch_boundary_inner() -> eyre::Result<()> {
    info!(target: "snapshot-test", "test_snapshot_upload_and_restore_across_epoch_boundary");
    let tmp_guard =
        tempfile::TempDir::with_prefix("snapshot_upload_restore").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();

    // genesis ceremony for validator-1..4 + the pre-configured "observer" datadir, on the short
    // epoch cadence so a boundary lands inside the test budget. worker 0 is a `Static`-fee worker
    // at the protocol floor so every epoch's base fees are chain-derivable (otherwise the uploader
    // skips every publish) while the harness's fixed-gas-price liveness tx still lands.
    config_local_testnet_with_worker_fee_configs(
        &temp_path,
        Some(SNAPSHOT_TEST_PASSPHRASE.to_string()),
        None,
        Some(SNAPSHOT_EPOCH_DURATION as u32),
        &[&format!("0:1:{MIN_PROTOCOL_BASE_FEE}")],
    )
    .expect("failed to config");

    let bin = get_telcoin_network_binary();

    // credential-free bucket rooted in the temp dir; the uploader creates the directory on demand.
    let bucket_path = temp_path.join("snapshot-bucket");
    let bucket_url = file_url(&bucket_path);

    // start the 4 validators.
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for (i, url) in client_urls.iter_mut().enumerate() {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for validator!");
        url.push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, bin, &temp_path, rpc_port, UPLOAD_RESTORE_TEST, 0));
    }

    // start the uploader observer alongside the validators. it follows consensus like any observer
    // and, being observer-only, publishes a snapshot at every epoch boundary.
    let uploader_rpc_port =
        get_available_tcp_port("127.0.0.1").expect("Failed to get an rpc port for uploader!");
    guard.push(spawn_observer_node(
        bin,
        &temp_path,
        "observer",
        uploader_rpc_port,
        UPLOAD_RESTORE_TEST,
        &["--snapshot-upload", bucket_url.as_str()],
    ));

    // wait for the 4 validators to serve RPC.
    network_advancing(&client_urls)?;

    // (a) wait for epoch 0 to close — a quick sanity check that the network is advancing (the
    // restorable-snapshot wait below implies this, but this pinpoints "network stalled" vs
    // "uploader stalled" on failure).
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let deadline = Instant::now() + Duration::from_secs(SNAPSHOT_EPOCH_DURATION * 6);
    loop {
        let info = registry.getCurrentEpochInfo().call().await?;
        if info.epochId > 0 {
            info!(target: "snapshot-test", current_epoch = info.epochId, "epoch 0 closed");
            break;
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "epoch 0 did not close within {}s",
                SNAPSHOT_EPOCH_DURATION * 6
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    // (b) wait for `latest.json` to advertise a RESTORABLE snapshot (epoch >=
    // MIN_RESTORABLE_EPOCH). the uploader writes the pointer LAST and atomically, so reads are
    // never torn.
    let latest_json = bucket_path.join("latest.json");
    let deadline = Instant::now() + Duration::from_secs(SNAPSHOT_EPOCH_DURATION * 15);
    loop {
        if let Some(epoch) = read_latest_epoch(&latest_json) {
            if epoch >= MIN_RESTORABLE_EPOCH {
                info!(target: "snapshot-test", epoch, "restorable snapshot published to bucket");
                break;
            }
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "uploader did not publish a restorable (epoch >= {MIN_RESTORABLE_EPOCH}) snapshot \
                 within {}s; check test_logs/{UPLOAD_RESTORE_TEST}/",
                SNAPSHOT_EPOCH_DURATION * 15
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    // assert the bucket shape and capture the epoch it validated.
    let published_epoch = assert_bucket_shape(&bucket_path)?;
    assert!(
        published_epoch >= MIN_RESTORABLE_EPOCH,
        "bucket shape validated epoch {published_epoch}, expected a restorable epoch >= {MIN_RESTORABLE_EPOCH}"
    );
    info!(target: "snapshot-test", published_epoch, "bucket shape verified");

    // set up the FRESH restore node: fresh observer keys + a copy of the same genesis trust root.
    let restore_dir = temp_path.join("restore-node");
    config_fresh_observer_datadir(&temp_path, "restore-node")?;

    // bucket-integrity assertion: the standalone `snapshot verify` subcommand downloads and fully
    // verifies the snapshot against the fresh trust root, installing nothing. deep by default, it
    // also rebuilds the state and recomputes the boundary state root, so this drives that path
    // end-to-end against a real published snapshot.
    assert_snapshot_verifies(bin, &temp_path, &restore_dir, &bucket_url)?;

    // start the restore node; it auto-restores the boundary state before opening its database.
    let restore_rpc_port =
        get_available_tcp_port("127.0.0.1").expect("Failed to get an rpc port for restore node!");
    let restore_url = format!("http://127.0.0.1:{restore_rpc_port}");
    guard.push(spawn_observer_node(
        bin,
        &temp_path,
        "restore-node",
        restore_rpc_port,
        UPLOAD_RESTORE_TEST,
        &["--snapshot-source", bucket_url.as_str()],
    ));

    // datadir introspection: the restore receipt records the installed epoch and its final
    // execution block. it is written only after every install step succeeds, so its presence
    // proves the auto-restore completed; the network keeps publishing, so the installed epoch
    // is >= the first we saw in the bucket.
    let (restored_epoch, restored_boundary_block) =
        wait_for_restore_receipt(&restore_dir, SNAPSHOT_EPOCH_DURATION * 15)?;
    assert!(
        restored_epoch >= published_epoch,
        "restore receipt records epoch {restored_epoch}, which is older than the first published \
         epoch {published_epoch} we observed"
    );
    info!(target: "snapshot-test", restored_epoch, restored_boundary_block, "restore receipt confirms installed epoch");

    // state-install proof: the restored node serves RPC and its EVM holds the installed boundary
    // state. the genesis dev-funded `test-source` account carries its funded balance, which only
    // the installed snapshot state provides — a fresh-but-unrestored datadir would have nothing
    // here.
    let funded = address_from_word("test-source").to_string();
    let deadline = Instant::now() + Duration::from_secs(SNAPSHOT_EPOCH_DURATION * 15);
    let restored_balance = loop {
        if let Ok(balance) = get_balance(&restore_url, &funded, 3) {
            if balance > 0 {
                break balance;
            }
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "restored node did not serve the funded account's state within {}s; auto-restore or \
                 startup likely failed. Check test_logs/{UPLOAD_RESTORE_TEST}/",
                SNAPSHOT_EPOCH_DURATION * 15
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    info!(target: "snapshot-test", restored_balance, "restored node serves the installed boundary EVM state");

    // ---- forward-resume assertions: the restored observer must catch up and execute new output
    // ----

    // (i) the restored node's EVM tip advances PAST the restored boundary block and reaches parity
    // with a validator's tip. on this 10s-epoch chain the tip advances ~1 block per epoch close, so
    // this window crosses several boundaries; parity allows a small sync-lag delta. eth_blockNumber
    // reads the canonical tip without fetching a block body, so it works on the restored node from
    // the boundary block onward.
    let restore_provider = ProviderBuilder::new().connect_http(restore_url.parse()?);
    let deadline = Instant::now() + Duration::from_secs(SNAPSHOT_EPOCH_DURATION * 12);
    let (restored_tip, validator_tip) = loop {
        let restored_tip = restore_provider.get_block_number().await.unwrap_or(0);
        let validator_tip = provider.get_block_number().await?;
        if restored_tip > restored_boundary_block
            && validator_tip.saturating_sub(restored_tip) <= PARITY_DELTA
        {
            break (restored_tip, validator_tip);
        }
        if Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "restored node did not advance past boundary block {restored_boundary_block} to \
                 parity within {}s (restored tip {restored_tip}, validator tip {validator_tip}); \
                 forward resume likely failed. Check test_logs/{UPLOAD_RESTORE_TEST}/",
                SNAPSHOT_EPOCH_DURATION * 12
            ));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    info!(
        target: "snapshot-test",
        restored_tip, validator_tip, restored_boundary_block,
        "restored node advanced past the boundary to parity with the network"
    );

    // (ii) transaction liveness: a tx submitted to the running network becomes visible on the
    // RESTORED node's own view — proving it executes NEW consensus output into EVM state, not
    // merely header-syncs. we poll the recipient's balance directly rather than via
    // `send_and_confirm`, whose secondary base-fee-accumulator check races the per-epoch reward
    // sweep on this idle chain; the recipient, by contrast, only ever receives, so its balance
    // is monotonic.
    let key = get_key("test-source");
    let to_account = address_from_word("snapshot-restore-target");
    let transfer = 10 * WEI_PER_TEL;
    let before = get_balance(&restore_url, &to_account.to_string(), 5)?;
    send_tel(&client_urls[1], &key, to_account, transfer, 250, 21000, 0)?;
    let after =
        get_balance_above_with_retry(&restore_url, &to_account.to_string(), before + transfer - 1)?;
    assert_eq!(
        after,
        before + transfer,
        "restored node must reflect the post-restore transfer to {to_account} \
         (before {before}, after {after})"
    );
    info!(target: "snapshot-test", %to_account, transfer, "post-restore transaction visible on the restored node");

    guard.kill_all();
    Ok(())
}

/// Regression/gate test: a VALIDATOR (non-observer) started with `--snapshot-upload` must refuse to
/// run. The uploader is observer-only, so the CLI rejects the combination at startup — before any
/// database is opened — and the process exits non-zero with the observer-only error on stderr.
///
/// Not ignore-gated: it spawns a single node that exits within a couple of seconds (the gate runs
/// right after config load), so it is cheap enough to run in the normal suite once the shared
/// binary is built.
#[test]
fn test_snapshot_upload_rejected_on_validator() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    info!(target: "snapshot-test", "test_snapshot_upload_rejected_on_validator");
    let tmp_guard =
        tempfile::TempDir::with_prefix("snapshot_upload_reject").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();

    // a full genesis ceremony gives validator-1 a real config + keys so startup reaches the gate
    // (which runs after `Config::load`). default worker fee config and epoch duration are fine: the
    // node exits at the gate before any consensus runs.
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some(SNAPSHOT_TEST_PASSPHRASE.to_string()),
        None,
        None,
    )
    .expect("failed to config");

    let bin = get_telcoin_network_binary();
    let bucket_url = file_url(&temp_path.join("reject-bucket"));
    let rpc_port =
        get_available_tcp_port("127.0.0.1").expect("Failed to get an ephemeral rpc port!");

    // start validator-1 (instance 0, NO --observer) with the observer-only uploader flag.
    let mut guard = ProcessGuard::empty();
    let idx = guard.push(start_validator_with_args(
        0,
        bin,
        &temp_path,
        rpc_port,
        UPLOAD_REJECT_TEST,
        0,
        &["--snapshot-upload", bucket_url.as_str()],
    ));

    // the gate rejects before opening any database, so the process must exit quickly.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let status = loop {
        match guard.get_mut(idx).expect("child present").try_wait()? {
            Some(status) => break status,
            None => {
                if std::time::Instant::now() >= deadline {
                    return Err(eyre::eyre!(
                        "validator with --snapshot-upload did not exit within 30s; the observer-only \
                         gate should reject it immediately. Check test_logs/{UPLOAD_REJECT_TEST}/"
                    ));
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    };
    assert!(
        !status.success(),
        "validator with --snapshot-upload must exit non-zero, but it exited successfully ({status:?})"
    );

    // the binary's `main` prints the error via `eprintln!("Error: {err:?}")` before exiting, so the
    // observer-only message is captured in the child's stderr log (stdout is checked as a
    // fallback).
    let log_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_logs")
        .join(UPLOAD_REJECT_TEST);
    let stderr = std::fs::read_to_string(log_dir.join("node0-run0.stderr.log")).unwrap_or_default();
    let stdout = std::fs::read_to_string(log_dir.join("node0-run0.log")).unwrap_or_default();
    assert!(
        stderr.contains("observer-only") || stdout.contains("observer-only"),
        "expected the observer-only startup error; \nstderr:\n{stderr}\nstdout:\n{stdout}"
    );

    guard.kill_all();
    Ok(())
}

/// Build a credential-free `file://` object-store URL for an absolute local `path`.
///
/// The `file` backend requires an absolute path with an empty host; temp-dir paths are clean ASCII,
/// so `file://` + the absolute path yields the `file:///abs/path` form the store parses.
fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
}

/// Best-effort read of the epoch `latest.json` currently advertises, or `None` if it is absent or
/// not yet parseable. Used only for polling; `assert_bucket_shape` does the strict validation.
fn read_latest_epoch(latest_json: &Path) -> Option<u64> {
    let bytes = std::fs::read(latest_json).ok()?;
    let pointer: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    pointer["epoch"].as_u64()
}

/// Spawn an observer node rooted at `<base_dir>/<subdir>` with `extra_args` appended.
///
/// Mirrors `common::start_observer` but takes an explicit datadir subdir and extra CLI args, so the
/// same helper starts both the uploader (`--snapshot-upload`) and the restore (`--snapshot-source`)
/// observers on distinct datadirs. Kept local to this file rather than added to the shared harness.
fn spawn_observer_node(
    bin: &'static TestBinary,
    base_dir: &Path,
    subdir: &str,
    rpc_port: u16,
    test: &str,
    extra_args: &[&str],
) -> Child {
    let data_dir = base_dir.join(subdir);
    let ws_port = get_available_tcp_port("127.0.0.1").expect("ws port");
    // keep the IPC socket path short (under the temp-dir root, not the datadir) to stay within the
    // platform's sun_path limit, matching the pattern in `common::start_observer`.
    let ipc_path = base_dir.join(format!("{subdir}.ipc"));
    let mut command = bin.command();
    command
        .env("TN_BLS_PASSPHRASE", SNAPSHOT_TEST_PASSPHRASE)
        .arg("node")
        .arg("--observer")
        .arg("--datadir")
        .arg(&*data_dir.to_string_lossy())
        .arg("--http")
        .arg("--http.port")
        .arg(format!("{rpc_port}"))
        .arg("--ws")
        .arg("--ws.port")
        .arg(format!("{ws_port}"))
        .arg("--ipcpath")
        .arg(ipc_path.to_string_lossy().as_ref())
        .arg("--node-name")
        .arg(format!("{test}-{subdir}"));
    command.args(extra_args);

    setup_log_dir(&mut command, subdir, test, 0);

    command.spawn().expect("failed to execute")
}

/// Configure a fresh observer datadir at `<base_dir>/<subdir>`: mint fresh observer keys and copy
/// the shared genesis trust root (`committee.yaml`, `genesis.yaml`, `parameters.yaml`).
///
/// This mirrors how `config_local_testnet` seeds the pre-configured observer, but for a second,
/// independent observer with its own network identity — the node the restore path boots onto.
fn config_fresh_observer_datadir(base_dir: &Path, subdir: &str) -> eyre::Result<()> {
    let dir = base_dir.join(subdir);
    // fresh, random observer keys (distinct network identity from the uploader observer).
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "generate",
        "observer",
        "--address",
        "0xEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
    ]);
    keys_command.args.execute(dir.clone(), Some(SNAPSHOT_TEST_PASSPHRASE.to_string()))?;

    // copy the genesis trust root the restore path verifies the downloaded snapshot against.
    let shared = base_dir.join("shared-genesis");
    std::fs::create_dir_all(dir.join("genesis"))?;
    std::fs::copy(shared.join("genesis/committee.yaml"), dir.join("genesis/committee.yaml"))?;
    std::fs::copy(shared.join("genesis/genesis.yaml"), dir.join("genesis/genesis.yaml"))?;
    std::fs::copy(shared.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    Ok(())
}

/// Assert the on-disk shape of the snapshot bucket and return the epoch `latest.json` points at.
///
/// Parses `latest.json` (the pointer) with `serde_json`, checks it names an
/// `epoch-<N zero-padded>/manifest.json` that exists, then parses that manifest and confirms its
/// version/kind/chain binding and that every listed artifact — including at least one state chunk,
/// the headers, and the epoch-records — is present on disk.
fn assert_bucket_shape(bucket_path: &Path) -> eyre::Result<u64> {
    let pointer_bytes = std::fs::read(bucket_path.join("latest.json"))?;
    let pointer: serde_json::Value = serde_json::from_slice(&pointer_bytes)?;
    assert_eq!(
        pointer["format_version"].as_u64(),
        Some(1),
        "latest.json format_version must be 1: {pointer}"
    );
    assert_eq!(
        pointer["chain_id"].as_u64(),
        Some(SNAPSHOT_CHAIN_ID),
        "latest.json chain_id must bind the local chain: {pointer}"
    );
    let published_epoch = pointer["epoch"]
        .as_u64()
        .ok_or_else(|| eyre::eyre!("latest.json missing numeric `epoch`: {pointer}"))?;
    let manifest_key = pointer["manifest_key"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("latest.json missing `manifest_key`: {pointer}"))?;
    assert_eq!(
        manifest_key,
        format!("epoch-{published_epoch:010}/manifest.json"),
        "manifest_key must be the zero-padded epoch dir plus manifest.json"
    );

    let manifest_path = bucket_path.join(manifest_key);
    assert!(
        manifest_path.exists(),
        "latest.json points at {manifest_key}, but {} does not exist",
        manifest_path.display()
    );
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
    assert_eq!(manifest["format_version"].as_u64(), Some(1), "manifest format_version must be 1");
    assert_eq!(manifest["kind"].as_str(), Some("state-only"), "manifest kind must be state-only");
    assert_eq!(
        manifest["epoch"].as_u64(),
        Some(published_epoch),
        "manifest epoch must match the pointer epoch"
    );
    assert_eq!(
        manifest["chain_id"].as_u64(),
        pointer["chain_id"].as_u64(),
        "pointer and manifest chain_id must agree"
    );

    let artifacts = manifest["artifacts"]
        .as_array()
        .ok_or_else(|| eyre::eyre!("manifest missing `artifacts` array"))?;
    assert!(!artifacts.is_empty(), "manifest artifacts must not be empty");

    // manifest_key is `<epoch-dir>/manifest.json`; the artifacts live alongside it.
    let epoch_dir =
        manifest_key.strip_suffix("/manifest.json").expect("manifest_key ends with /manifest.json");
    let mut kinds = Vec::new();
    for artifact in artifacts {
        let name = artifact["name"]
            .as_str()
            .ok_or_else(|| eyre::eyre!("manifest artifact missing `name`: {artifact}"))?;
        if let Some(kind) = artifact["kind"].as_str() {
            kinds.push(kind.to_string());
        }
        let artifact_path = bucket_path.join(epoch_dir).join(name);
        assert!(
            artifact_path.exists(),
            "manifest lists artifact {name}, but {} does not exist",
            artifact_path.display()
        );
    }
    for expected in ["state-chunk", "headers", "epoch-records"] {
        assert!(
            kinds.iter().any(|k| k == expected),
            "manifest must list a {expected} artifact; got {kinds:?}"
        );
    }

    Ok(published_epoch)
}

/// Run the standalone `snapshot verify` subcommand against `bucket_url`, using `datadir`'s genesis
/// as the trust root. It downloads and fully verifies the snapshot but installs nothing, so it is a
/// self-contained bucket-integrity assertion. Verify runs deep by default (no `--skip-state-root`),
/// so this also drives the from-scratch state-root recompute end-to-end and asserts the summary
/// reports a recomputed — not skipped — state root. `cwd` scopes any stray log output to the temp
/// dir.
fn assert_snapshot_verifies(
    bin: &'static TestBinary,
    cwd: &Path,
    datadir: &Path,
    bucket_url: &str,
) -> eyre::Result<()> {
    let output = bin
        .command()
        .current_dir(cwd)
        .arg("--datadir")
        .arg(datadir)
        .arg("snapshot")
        .arg("verify")
        .arg("--source")
        .arg(bucket_url)
        .output()
        .expect("failed to run `snapshot verify`");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "`snapshot verify --source {bucket_url}` failed ({:?})\nstdout:\n{stdout}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
    );
    // deep verify is the default, so the summary recomputes and prints the boundary state root
    // rather than reporting it skipped — this drives the real from-scratch rebuild end-to-end.
    assert!(
        stdout.contains("state root:"),
        "`snapshot verify` must recompute and print the state root by default (deep verify)\nstdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("skipped"),
        "`snapshot verify` without --skip-state-root must not report a skipped state root\nstdout:\n{stdout}"
    );
    Ok(())
}

/// Poll for the restore receipt written under `<datadir>/snapshots/last-restore.json` and return
/// the installed `(epoch, final_state.number)`. The receipt is written only after every install
/// step succeeds, so its presence is proof the auto-restore ran to completion on this datadir.
fn wait_for_restore_receipt(datadir: &Path, timeout_secs: u64) -> eyre::Result<(u64, u64)> {
    let receipt_path = datadir.join("snapshots").join("last-restore.json");
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Ok(bytes) = std::fs::read(&receipt_path) {
            if let Ok(receipt) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                if let (Some(epoch), Some(final_block)) =
                    (receipt["epoch"].as_u64(), receipt["final_state"]["number"].as_u64())
                {
                    return Ok((epoch, final_block));
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(eyre::eyre!(
                "restore receipt did not appear at {} within {timeout_secs}s — auto-restore likely \
                 did not run",
                receipt_path.display()
            ));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
