//! E2e tests for syncing a new node to an existing network.

use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
};
use e2e_tests::config_local_testnet_with_epoch_duration;
use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_test_utils::wait_until;
use tn_types::{
    get_available_tcp_port, Epoch, EpochCertificate, EpochRecord, Genesis, GenesisAccount,
    NodeMode, U256,
};
use tracing::info;

use crate::{
    common::{
        address_from_word, advertise_worker_rpc, assert_nodes_agree_on_commit_times,
        block_commit_time, current_epoch, drive_light_tx_load, fetch_verified_epoch_record,
        get_block_number, get_key, get_latest_consensus_header_number, network_advancing,
        pin_fork_epochs, scrape_metric_value, send_and_confirm, start_observer,
        start_observer_with_args, start_validator, wait_for_epoch_at_least, wait_for_head_at_least,
        wait_for_rpc, walk_block_commit_times, ProcessGuard, EVM_TIMESTAMP_CLAMPED_SERIES,
    },
    restarts::wait_for_node_mode,
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
        // The late-joining observer forwards accepted txns to the committee's advertised
        // RPC endpoints; without this each seal is refused with NotValidator and the
        // txns stay pending in the observer's pool until an endpoint is discoverable.
        advertise_worker_rpc(&temp_path, i, rpc_port)?;
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

/// The sub-second timestamp fork epoch of the observer tests below. Epochs 0 and 1 commit in whole
/// seconds and every later epoch in milliseconds, so within four epochs the observer has met both
/// the legacy and the millisecond header and sub-dag layouts.
const OBSERVER_FORK_EPOCH: Epoch = 2;

/// Epoch duration (in seconds) of the observer fork tests.
///
/// Half of [`PACK_IMPORT_EPOCH_DURATION`], because no wait in these tests scales with it: every
/// certified-record poll is floored at [`OBSERVER_RECORD_TIMEOUT_SECS`] and the observer catch-up
/// bounds are fixed (see [`ObserverSchedule::catch_up_bound`]). The shorter epoch only shortens
/// the run to the epoch after the fork.
const OBSERVER_FORK_EPOCH_DURATION: u64 = 5;

/// How long a certified epoch record may take to appear on a node. Quorum voting on a record takes
/// a fixed time that does not shrink with the epoch, so this is the same 60 s floor the cross-fork
/// test in `epochs.rs` gives each record.
const OBSERVER_RECORD_TIMEOUT_SECS: u64 = 60;

/// When the observer in [`observer_across_subsecond_fork`] runs, relative to the fork epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObserverSchedule {
    /// Starts on an empty datadir once the epoch after the fork has opened and the fork epoch's
    /// record is certified, so it takes every epoch through the fork from its peers.
    JoinsAfterFork,
    /// Starts with the validators and follows the fork epoch as it is committed.
    FollowsLive,
    /// Starts with the validators, is frozen with SIGSTOP once the epoch before the fork opens,
    /// and resumes with SIGCONT once the epoch after the fork has opened and the fork epoch's
    /// record is certified.
    PausedAcrossFork,
}

impl ObserverSchedule {
    /// How long the observer gets to reach the validators' consensus height after it starts or
    /// resumes: 120 s for a node syncing from an empty datadir, as in
    /// `test_observer_late_join_catchup`, and 60 s for one that already holds the epochs before
    /// the fork, as in `test_observer_reconnect_after_pause`.
    fn catch_up_bound(self) -> Duration {
        match self {
            Self::JoinsAfterFork => Duration::from_secs(120),
            Self::FollowsLive | Self::PausedAcrossFork => Duration::from_secs(60),
        }
    }
}

/// An observer that joins after the sub-second timestamp fork syncs every epoch through it from
/// its peers and serves the same records, blocks and commit times as a validator.
///
/// The fork is pinned at [`OBSERVER_FORK_EPOCH`] with the seed-signature fork active from genesis.
/// Four validators run under light transaction load until the epoch after the fork opens and
/// validator-1 serves a certified record for the fork epoch. Only then does the observer start,
/// on an empty datadir, so it decodes the whole-second epochs and the millisecond epoch from what
/// its peers send it, never from headers it saw committed. Epochs last
/// [`OBSERVER_FORK_EPOCH_DURATION`] seconds, which shortens nothing the observer waits on. The
/// observer is checked as [`assert_observer_agrees_across_fork`] describes, then a transfer sent
/// through it must land in a post-fork block.
#[test]
#[ignore = "only run independently from all other it tests"]
fn test_epoch_observer_joins_after_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    observer_runtime()?
        .block_on(observer_across_subsecond_fork("ss_obs_join", ObserverSchedule::JoinsAfterFork))
}

/// An observer that runs from genesis follows the sub-second timestamp fork as it is committed,
/// in one process that holds its own whole-second history on disk when the first millisecond
/// header arrives.
///
/// Same network, fork pin and checks as [`test_epoch_observer_joins_after_subsecond_fork`], with
/// the observer started alongside the validators instead of after the fork.
#[test]
#[ignore = "only run independently from all other it tests"]
fn test_epoch_observer_follows_across_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    observer_runtime()?
        .block_on(observer_across_subsecond_fork("ss_obs_live", ObserverSchedule::FollowsLive))
}

/// An observer frozen across the sub-second timestamp fork, as an RPC node that is suspended or
/// partitioned while the fork epoch arrives, resumes with whole-second state in memory and takes
/// the layout switch from its catch-up path.
///
/// Same network, fork pin and checks as [`test_epoch_observer_joins_after_subsecond_fork`]. The
/// observer starts alongside the validators and is stopped with SIGSTOP once both it and
/// validator-1 have entered the epoch before the fork; the test fails if validator-1 had already
/// reached the fork epoch by then, since the observer could then have seen it committed. It is
/// resumed with SIGCONT once the epoch after the fork has opened and validator-1 serves a
/// certified record for the fork epoch.
#[test]
#[ignore = "only run independently from all other it tests"]
fn test_epoch_observer_paused_across_subsecond_fork() -> eyre::Result<()> {
    let _permit = super::common::acquire_test_permit();
    observer_runtime()?.block_on(observer_across_subsecond_fork(
        "ss_obs_pause",
        ObserverSchedule::PausedAcrossFork,
    ))
}

/// The multi-thread runtime the observer fork tests run on: the blocking RPC helpers in
/// `common.rs` move off the runtime with `block_in_place`, which a current-thread runtime rejects.
fn observer_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()
}

/// Run four validators across the sub-second timestamp fork at [`OBSERVER_FORK_EPOCH`] with an
/// observer on `schedule`, check the observer against validator-1 (see
/// [`assert_observer_agrees_across_fork`]), then send a transfer through the observer and require
/// it to land in a post-fork block.
///
/// `test` names the temp dir and the log directory under `test_logs/`. Keep it short: node IPC
/// socket paths are built under the temp dir.
async fn observer_across_subsecond_fork(
    test: &str,
    schedule: ObserverSchedule,
) -> eyre::Result<()> {
    info!(target: "restart-test", test, ?schedule, "observer across the sub-second fork");
    // both forced rather than inherited: the claim is a crossing at a known sub-second epoch, and
    // the gate (`tn_types::forks::subsecond_timestamp_active`) conjoins the seed fork fail-closed,
    // so a dormant seed fork would keep every epoch on whole seconds. the multi-workers and
    // leader-seeded pins follow the lane
    pin_fork_epochs(None, Some(0), None, Some(OBSERVER_FORK_EPOCH));

    let temp_dir = tempfile::TempDir::with_prefix(test)?;
    let temp_path = temp_dir.path();

    // one funded sender per validator for the light load. the transfer through the observer at the
    // end comes from the harness's `test-source` account, which the load never uses, so nonce 0
    let mut senders: Vec<TransactionFactory> = (0..4u64)
        .map(|i| TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(0x0b5e + i)))
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
        Some(OBSERVER_FORK_EPOCH_DURATION as u32),
    )?;
    let genesis: Genesis = Config::load_from_path(
        temp_path.join("shared-genesis").join("genesis").join("genesis.yaml"),
        ConfigFmt::YAML,
    )?;
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

    let bin = e2e_tests::get_telcoin_network_binary();
    let mut guard = ProcessGuard::empty();
    let mut rpc_urls = Vec::new();
    for instance in 0..4 {
        let rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
        // the observer forwards the transfer it accepts to the committee's advertised endpoints
        advertise_worker_rpc(temp_path, instance, rpc_port)?;
        guard.push(start_validator(instance, bin, temp_path, rpc_port, test, 0));
        rpc_urls.push(format!("http://127.0.0.1:{rpc_port}"));
    }
    let providers = rpc_urls
        .iter()
        .map(|url| Ok(ProviderBuilder::new().connect_http(url.parse()?)))
        .collect::<eyre::Result<Vec<_>>>()?;

    let obs_rpc_port = get_available_tcp_port("127.0.0.1").expect("rpc port assigned by host");
    let obs_url = format!("http://127.0.0.1:{obs_rpc_port}");
    let obs_provider = ProviderBuilder::new().connect_http(obs_url.parse()?);
    let obs_metrics = format!(
        "127.0.0.1:{}",
        get_available_tcp_port("127.0.0.1").expect("metrics port assigned by host")
    );
    let spawn_observer = |guard: &mut ProcessGuard| -> eyre::Result<Pid> {
        let child = start_observer_with_args(
            4,
            bin,
            temp_path,
            obs_rpc_port,
            test,
            0,
            &["--metrics", &obs_metrics],
        );
        let id = child.id();
        guard.push(child);
        Ok(Pid::from_raw(i32::try_from(id)?))
    };
    let observer = match schedule {
        ObserverSchedule::JoinsAfterFork => None,
        ObserverSchedule::FollowsLive | ObserverSchedule::PausedAcrossFork => {
            Some(spawn_observer(&mut guard)?)
        }
    };
    let paused = observer.filter(|_| schedule == ObserverSchedule::PausedAcrossFork);
    futures::future::try_join_all(providers.iter().map(wait_for_rpc)).await?;

    // cross the fork under light load, which is what puts blocks inside the post-fork epochs; the
    // load stops when the finished race drops it
    let crossing = async {
        if let Some(pid) = paused {
            wait_for_epoch_at_least(&providers[0], OBSERVER_FORK_EPOCH - 1).await?;
            // the observer has executed the close of the epoch before, so it holds pre-fork state
            wait_for_rpc(&obs_provider).await?;
            wait_for_epoch_at_least(&obs_provider, OBSERVER_FORK_EPOCH - 1).await?;
            signal::kill(pid, Signal::SIGSTOP)?;
            let at_pause = current_epoch(&providers[0]).await?.epoch_id;
            eyre::ensure!(
                at_pause < OBSERVER_FORK_EPOCH,
                "validator-1 was already in epoch {at_pause} when the observer was paused, so the \
                 observer may have followed the fork at {OBSERVER_FORK_EPOCH} live"
            );
            info!(target: "restart-test", at_pause, "observer paused before the fork epoch");
        }
        wait_for_epoch_at_least(&providers[0], OBSERVER_FORK_EPOCH + 1).await?;
        fetch_verified_epoch_record(&rpc_urls[0], OBSERVER_FORK_EPOCH, OBSERVER_RECORD_TIMEOUT_SECS)
            .await
    };
    let fork_record = tokio::select! {
        crossed = crossing => crossed?,
        never = drive_light_tx_load(&providers, &mut senders, chain) => match never {},
    };
    info!(
        target: "restart-test",
        fork_final_block = fork_record.final_state.number,
        "network crossed the fork and certified the fork epoch",
    );

    match schedule {
        ObserverSchedule::JoinsAfterFork => {
            spawn_observer(&mut guard)?;
        }
        ObserverSchedule::FollowsLive => {}
        ObserverSchedule::PausedAcrossFork => {
            let pid = paused.ok_or_else(|| eyre::eyre!("the paused observer has no pid"))?;
            signal::kill(pid, Signal::SIGCONT)?;
        }
    }
    let started = Instant::now();
    let validator_height = get_latest_consensus_header_number(&rpc_urls[0])?;
    wait_until(
        schedule.catch_up_bound(),
        &format!("observer to reach consensus height {validator_height} (test_logs/{test}/)"),
        || async {
            Ok(get_latest_consensus_header_number(&obs_url)
                .is_ok_and(|height| height >= validator_height))
        },
    )
    .await?;
    info!(
        target: "restart-test",
        ?schedule,
        validator_height,
        catch_up_ms = started.elapsed().as_millis(),
        "observer reached the validators' consensus height",
    );
    wait_for_node_mode(&obs_url, NodeMode::Observer)?;

    assert_observer_agrees_across_fork(
        &providers[0],
        &rpc_urls[0],
        &obs_provider,
        &obs_url,
        &obs_metrics,
    )
    .await?;

    // a transfer accepted by the observer is forwarded to the committee and lands in a block that
    // the observer serves as post-fork
    let before = get_block_number(&rpc_urls[0])?;
    let target = address_from_word("ss-observer-forward-target");
    send_and_confirm(&obs_url, &rpc_urls[0], &get_key("test-source"), target, 0)?;
    let after = get_block_number(&rpc_urls[0])?;
    eyre::ensure!(after > before, "validator-1 confirmed the transfer without a new block");
    wait_for_head_at_least(&obs_url, after, 60).await?;
    let landed = walk_block_commit_times(&obs_provider, &obs_url, before + 1..=after).await?;
    eyre::ensure!(
        landed.iter().all(|block| block.sub_second),
        "the forwarded transfer's blocks are not all post-fork on the observer: {landed:?}"
    );

    guard.kill_all();
    Ok(())
}

/// Check an observer against validator-1 for every epoch through [`OBSERVER_FORK_EPOCH`], once it
/// has reached the validators' consensus height:
///
/// - for every epoch from 0 to the fork epoch, both serve the same certified record and the same
///   commit time for the record's final block, which is sub-second exactly from the fork epoch on
///   (`subSecond` is the gate read at the epoch of the block's consensus leader,
///   `BlockTimestampMillis::with_consensus` in `crates/execution/tn-rpc/src/rpc_ext.rs`);
/// - the observer executes up to validator-1's head, and the two agree on every block and commit
///   time from genesis to there (see [`walk_block_commit_times`]);
/// - on the observer, every block up to the final block of the epoch before the fork commits in
///   whole seconds and every later block in milliseconds, and at least one later block's commit
///   time is off a whole second, so the observer serves the post-fork precision and not whole
///   seconds flagged as sub-second;
/// - the observer's engine never clamped an EVM timestamp ([`EVM_TIMESTAMP_CLAMPED_SERIES`] reads
///   0). The observer is the process it started as, so the counter covers everything it executed.
async fn assert_observer_agrees_across_fork<P: Provider>(
    validator: &P,
    validator_url: &str,
    observer: &P,
    observer_url: &str,
    observer_metrics: &str,
) -> eyre::Result<()> {
    // validator-1 is in the epoch after the fork, so its head is past the fork epoch's final block
    let head = get_block_number(validator_url)?;
    wait_for_head_at_least(observer_url, head, 60).await?;

    let mut last_pre_fork_block = 0;
    for epoch in 0..=OBSERVER_FORK_EPOCH {
        let expected =
            fetch_verified_epoch_record(validator_url, epoch, OBSERVER_RECORD_TIMEOUT_SECS).await?;
        let actual =
            fetch_verified_epoch_record(observer_url, epoch, OBSERVER_RECORD_TIMEOUT_SECS).await?;
        eyre::ensure!(
            actual == expected,
            "observer's epoch {epoch} record diverges from validator-1's: {actual:?} vs {expected:?}"
        );
        let final_block = expected.final_state.number;
        let on_validator = block_commit_time(validator, validator_url, final_block).await?;
        let on_observer = block_commit_time(observer, observer_url, final_block).await?;
        eyre::ensure!(
            on_observer == on_validator,
            "observer and validator-1 disagree on epoch {epoch}'s final block: {on_observer:?} vs \
             {on_validator:?}"
        );
        eyre::ensure!(
            on_observer.sub_second == (epoch >= OBSERVER_FORK_EPOCH),
            "epoch {epoch}'s final block reports subSecond {} with the fork at \
             {OBSERVER_FORK_EPOCH}: {on_observer:?}",
            on_observer.sub_second,
        );
        if epoch + 1 == OBSERVER_FORK_EPOCH {
            last_pre_fork_block = final_block;
        }
    }

    let served = vec![
        walk_block_commit_times(validator, validator_url, 0..=head).await?,
        walk_block_commit_times(observer, observer_url, 0..=head).await?,
    ];
    assert_nodes_agree_on_commit_times(
        &served,
        &[validator_url.to_string(), observer_url.to_string()],
    )?;

    let mut post_fork_blocks = 0usize;
    let mut off_whole_second = 0usize;
    for block in &served[1] {
        eyre::ensure!(
            block.sub_second == (block.block_number > last_pre_fork_block),
            "observer block {} sits on the wrong side of the fork, which follows block \
             {last_pre_fork_block} (the final block of epoch {}): {block:?}",
            block.block_number,
            OBSERVER_FORK_EPOCH - 1,
        );
        if block.sub_second {
            post_fork_blocks += 1;
            if block.timestamp_millis % 1000 != 0 {
                off_whole_second += 1;
            }
        }
    }
    eyre::ensure!(
        off_whole_second > 0,
        "none of the observer's {post_fork_blocks} post-fork blocks has a commit time off a whole \
         second"
    );
    info!(
        target: "restart-test",
        head,
        last_pre_fork_block,
        post_fork_blocks,
        off_whole_second,
        "observer serves the same blocks and commit times across the fork",
    );

    // blocking socket I/O with sleeps between retries, so it runs off the runtime worker
    let clamped = tokio::task::block_in_place(|| {
        scrape_metric_value(observer_metrics, EVM_TIMESTAMP_CLAMPED_SERIES)
    })?;
    eyre::ensure!(clamped == 0.0, "observer clamped {clamped} EVM timestamps up to their parent's");
    Ok(())
}
