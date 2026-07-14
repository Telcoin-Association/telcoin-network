//! E2e tests for syncing a new node to an existing network.

use alloy::providers::{Provider, ProviderBuilder};
use e2e_tests::config_local_testnet_with_epoch_duration;
use std::time::Duration;
use tn_reth::system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS};
use tn_test_utils::wait_until;
use tn_types::{get_available_tcp_port, EpochCertificate, EpochRecord};
use tracing::info;

use crate::common::{
    address_from_word, get_key, get_latest_consensus_header_number, network_advancing,
    send_and_confirm, start_observer, start_validator, ProcessGuard,
};

/// Epoch duration (in seconds) used by the pack-import test. Held at 10s independently of
/// `epochs.rs::EPOCH_DURATION` (which #897 cut to 5s): this pack-import regression test is
/// outside that four-test scope, and 10s keeps ample margin for the epoch-0
/// close + certify + observer pack-import path.
const PACK_IMPORT_EPOCH_DURATION: u64 = 10;

/// Regression test: an observer joining after epoch 0 has been closed and certified must
/// successfully import the epoch-0 pack rather than failing with
/// `PackError::InvalidConsensusChain` (Finding F2 in `report.md`).
///
/// Without the `consensus_pack::stream_import` parent-hash convention fix, the observer's
/// first record in the epoch-0 pack expects `parent_hash == ConsensusHeader::default().digest()`
/// but the validator's send-side computed `parent_hash` from a synthesised previous-epoch
/// sentinel. The mismatch surfaces as a "Broken consensus record chain" error and the
/// observer never advances past genesis.
#[test]
#[ignore = "should not run with a default cargo test, run restart tests as seperate step"]
fn test_observer_pack_imports_after_epoch_close() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .expect("tokio runtime");
    rt.block_on(test_observer_pack_imports_after_epoch_close_inner())
}

async fn test_observer_pack_imports_after_epoch_close_inner() -> eyre::Result<()> {
    info!(target: "restart-test", "test_observer_pack_imports_after_epoch_close");
    let tmp_guard =
        tempfile::TempDir::with_prefix("observer_pack_import").expect("tempdir is okay");
    let temp_path = tmp_guard.path().to_path_buf();
    config_local_testnet_with_epoch_duration(
        &temp_path,
        Some("restart_test".to_string()),
        None,
        Some(PACK_IMPORT_EPOCH_DURATION as u32),
    )
    .expect("failed to config");

    let bin = e2e_tests::get_telcoin_network_binary();

    // Start 4 validators (no observer yet)
    let mut guard = ProcessGuard::empty();
    let mut client_urls = [
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
        "http://127.0.0.1".to_string(),
    ];
    for i in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1")
            .expect("Failed to get an ephemeral rpc port for child!");
        client_urls[i].push_str(&format!(":{rpc_port}"));
        guard.push(start_validator(i, &bin, &temp_path, rpc_port, "observer_pack_import", 0));
    }

    // Wait for validators to start serving RPC.
    network_advancing(&client_urls)?;

    // Wait until epoch 0 has fully closed AND a certified epoch-0 record is on disk.
    // The observer can only be forced down the pack-import path once a complete persisted
    // epoch-0 pack exists on the validator side.
    let provider = ProviderBuilder::new().connect_http(client_urls[0].parse()?);
    let registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);

    // (a) wait for epoch 0 to close
    wait_until(Duration::from_secs(PACK_IMPORT_EPOCH_DURATION * 4), "epoch 0 to close", || async {
        Ok(registry.getCurrentEpochInfo().call().await?.epochId > 0)
    })
    .await?;
    let info = registry.getCurrentEpochInfo().call().await?;
    info!(target: "restart-test", current_epoch = info.epochId, "epoch 0 closed");

    // (b) wait for tn_epochRecord(0) to return a certified record
    wait_until(
        Duration::from_secs(PACK_IMPORT_EPOCH_DURATION * 3),
        "epoch 0 record to be certified on validator",
        || async {
            Ok(provider
                .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (0u32,))
                .await
                .is_ok())
        },
    )
    .await?;
    let validator_record_0: (EpochRecord, EpochCertificate) = provider
        .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (0u32,))
        .await?;
    assert!(
        validator_record_0.0.verify_with_cert(&validator_record_0.1),
        "validator-side epoch-0 record fails self-verify"
    );

    // Now start the observer fresh from genesis. With epoch 0 already on disk, any sync
    // must take the consensus_pack::stream_import path for that epoch.
    let obs_rpc_port = get_available_tcp_port("127.0.0.1")
        .expect("Failed to get an ephemeral rpc port for observer!");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    guard.push(start_observer(4, &bin, &temp_path, obs_rpc_port, "observer_pack_import", 0));

    // Wait for the observer to catch up. We compare consensus header heights to avoid
    // racing with EVM execution lag. The deadline allows pack download + verify + apply
    // on top of normal observer startup.
    let validator_height = get_latest_consensus_header_number(&client_urls[0])?;
    let max_secs = (PACK_IMPORT_EPOCH_DURATION * 6).max(60);
    wait_until(
        Duration::from_secs(max_secs),
        "observer to catch up via pack import (check logs in test_logs/observer_pack_import/)",
        || async {
            Ok(get_latest_consensus_header_number(&obs_url)
                .is_ok_and(|obs_height| obs_height >= validator_height))
        },
    )
    .await?;
    info!(target: "restart-test", validator_height, "observer caught up via pack import");

    // The observer must reconstruct the epoch-0 pack chain: ask for tn_epochRecord(0)
    // and confirm it self-verifies and matches the validator's record byte-for-byte.
    let obs_provider = ProviderBuilder::new().connect_http(obs_url.parse()?);
    wait_until(
        Duration::from_secs(PACK_IMPORT_EPOCH_DURATION * 3),
        "epoch 0 record to be available on observer",
        || async {
            Ok(obs_provider
                .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (0u32,))
                .await
                .is_ok())
        },
    )
    .await?;
    let observer_record_0: (EpochRecord, EpochCertificate) = obs_provider
        .raw_request::<_, (EpochRecord, EpochCertificate)>("tn_epochRecord".into(), (0u32,))
        .await?;
    assert!(
        observer_record_0.0.verify_with_cert(&observer_record_0.1),
        "observer epoch-0 record fails self-verify after pack import"
    );
    assert_eq!(
        observer_record_0.0, validator_record_0.0,
        "observer epoch-0 record diverges from validator after pack import"
    );

    // Final liveness check: a transaction submitted to the observer is confirmed by a
    // validator. This proves the observer is fully synced post-pack-import and not just
    // serving stale state.
    let key = get_key("test-source");
    let to_account = address_from_word("observer-pack-import-target");
    send_and_confirm(&obs_url, &client_urls[1], &key, to_account, 0)?;

    guard.kill_all();
    Ok(())
}
