//! Regression tests for restart replay over a fully imported pack with stale slot hints.

use super::{start_epoch::check_replay_link, *};
use crate::manager::node::tests::reth_config_and_db;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::VecDeque, path::PathBuf, time::Duration};
use tempfile::TempDir;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::{
    BlsKeypair, CommittedSubDag, ConsensusNumHash, EpochSeedChainValue, ExecHeader,
    ReputationScores, B256,
};
use tokio::io::AsyncReadExt as _;

fn output_at(
    fixture: &CommitteeFixture<MemDatabase>,
    epoch: Epoch,
    number: u64,
    parent: ConsensusHeaderDigest,
) -> ConsensusOutput {
    let committee = fixture.committee().advance_epoch_for_test(epoch);
    let certificates: Vec<_> = fixture
        .authorities()
        .map(|authority| {
            let mut certificate = fixture.certificate(&authority.header_with_round(&committee, 1));
            certificate.update_header_created_at_for_test(number);
            certificate
        })
        .collect();
    let leader = certificates.last().cloned().expect("fixture has authorities");
    let sub_dag = CommittedSubDag::new(
        certificates,
        leader,
        number,
        ReputationScores::new(&committee),
        None,
        EpochSeedChainValue::genesis_placeholder(),
    );
    ConsensusOutput::new(sub_dag, parent, number, false, VecDeque::new(), vec![])
}

/// An observer manager over a fresh datadir, with the epoch boundary at commit timestamp 6 and no
/// execution tip.
async fn observer_manager(
    temp: &TempDir,
) -> eyre::Result<(EpochManager<PathBuf, DatabaseType>, CommitteeFixture<MemDatabase>)> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let datadir = temp.path().join("observer");
    std::fs::create_dir_all(&datadir)?;
    let config = Config::default_for_test();
    let (node_config, reth_db) = reth_config_and_db(&config, &fixture.committee(), &datadir)?;
    let keys =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(601)));
    std::fs::create_dir_all(datadir.join("db"))?;
    let db = open_db(datadir.join("db"));
    let builder = TnBuilder::new(node_config, config, reth_db);
    let mut manager = EpochManager::new(builder, datadir, db, keys, "test").await?;
    manager.epoch_boundary = 6;
    Ok((manager, fixture))
}

/// Import an epoch-0 pack of six outputs from a peer, with output 1 built on `block1_parent`, and
/// save only a prefix. Execution stays at genesis.
async fn imported_epoch0_manager(
    temp: &TempDir,
    block1_parent: ConsensusHeaderDigest,
    saved: usize,
) -> eyre::Result<(
    EpochManager<PathBuf, DatabaseType>,
    CommitteeFixture<MemDatabase>,
    Vec<ConsensusOutput>,
    EpochRecord,
)> {
    let (manager, fixture) = observer_manager(temp).await?;
    let committee = fixture.committee();

    let source_dir = temp.path().join("source");
    std::fs::create_dir_all(&source_dir)?;
    let source = ConsensusChain::new_for_test(source_dir, committee.clone()).await?;
    let previous = EpochRecord {
        epoch: 0,
        committee: committee.bls_keys().iter().copied().collect(),
        next_committee: committee.bls_keys().iter().copied().collect(),
        ..Default::default()
    };
    let mut parent = block1_parent;
    let mut outputs = Vec::new();
    for number in 1..=6 {
        let output = output_at(&fixture, 0, number, parent);
        parent = output.consensus_header_hash();
        source.save_consensus_output(output.clone()).await?;
        outputs.push(output);
    }
    source.persist_current().await?;
    let mut record = previous.clone();
    record.final_consensus = ConsensusNumHash::new(6, parent);
    source.epochs().save_record(record.clone()).await?;
    // An import of the live epoch installs in place only when the record it carries is the
    // epoch's stored record, so store it on the manager's chain first.
    manager.consensus_chain.epochs().save_record(record.clone()).await?;
    let (stream, len) = source.get_epoch_stream(0).await?;
    manager
        .consensus_chain
        .stream_import(stream.take(len), &record, &previous, Duration::from_secs(5))
        .await?;
    for output in outputs.iter().take(saved) {
        manager.consensus_chain.save_consensus_output(output.clone()).await?;
    }
    manager.consensus_chain.persist_current().await?;
    source.close().await;
    Ok((manager, fixture, outputs, record))
}

/// Import six outputs, save only a prefix, and point execution at an independently chosen
/// output. This recreates the split between pack tip, saved slot and execution tip on disk.
async fn imported_manager(
    temp: &TempDir,
    saved: usize,
    executed: usize,
) -> eyre::Result<(
    EpochManager<PathBuf, DatabaseType>,
    CommitteeFixture<MemDatabase>,
    Vec<ConsensusOutput>,
    EpochRecord,
)> {
    let (manager, fixture, outputs, record) =
        imported_epoch0_manager(temp, ConsensusHeader::default().digest(), saved).await?;
    let executed_output = &outputs[executed - 1];
    manager.consensus_bus.recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(
                executed_output.number(),
                executed_output.consensus_header_hash(),
            ),
            Some(SealedHeader::new(
                ExecHeader {
                    number: executed as u64,
                    nonce: executed_output.nonce().into(),
                    parent_beacon_block_root: Some(executed_output.consensus_header_hash().into()),
                    ..Default::default()
                },
                B256::ZERO,
            )),
        );
    });
    Ok((manager, fixture, outputs, record))
}

#[tokio::test]
async fn replay_checkpoint_advances_through_imported_epoch_close_and_survives_restart(
) -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs, record) = imported_manager(&temp, 2, 2).await?;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let mut replay = manager.replay_missed_consensus(fixture.committee(), &to_engine).await?;
    assert_eq!(replay.take_epoch_close_hash(), Some(outputs[5].consensus_header_hash()));
    for expected in &outputs[2..] {
        assert_eq!(engine_rx.try_recv()?.consensus_header_hash(), expected.consensus_header_hash());
    }
    assert!(engine_rx.try_recv().is_err(), "executed prefix must not be replayed");
    assert_eq!(
        manager.consensus_chain.latest_consensus_number(),
        6,
        "startup replay must advance the slot marker before the next epoch's subscriber starts"
    );

    // A normal epoch handoff must accept exactly the next output, with no artificial gap.
    manager.consensus_chain.epochs().save_record(record.clone()).await?;
    let next_committee = fixture.committee().advance_epoch_for_test(1);
    manager.consensus_chain.new_epoch(record, next_committee).await?;
    let next = output_at(&fixture, 1, 7, outputs[5].consensus_header_hash());
    assert_eq!(manager.consensus_chain.latest_consensus_number() + 1, next.number());
    manager.consensus_chain.save_consensus_output(next).await?;
    manager.consensus_chain.persist_current().await?;
    let path = manager.tn_datadir.epochs_db_path();
    manager.consensus_chain.clone().close().await;
    drop(manager);
    let reopened = ConsensusChain::new(path, fixture.committee())?;
    assert_eq!(reopened.latest_consensus_epoch(), 1);
    assert_eq!(reopened.latest_consensus_number(), 7);
    reopened.close().await;
    Ok(())
}

#[tokio::test]
async fn replay_checkpoint_recovers_already_executed_previous_epoch_without_double_execution(
) -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs, record) = imported_manager(&temp, 2, 6).await?;
    manager.consensus_chain.epochs().save_record(record.clone()).await?;
    let next_committee = fixture.committee().advance_epoch_for_test(1);
    manager.consensus_chain.new_epoch(record, next_committee.clone()).await?;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let mut replay = manager.replay_missed_consensus(next_committee, &to_engine).await?;
    assert_eq!(replay.take_last_replayed_hash(), None);
    assert!(engine_rx.try_recv().is_err(), "committed execution must not be repeated");
    assert_eq!(
        manager.consensus_chain.latest_consensus_number(),
        6,
        "an empty replay must still repair the stale previous-epoch marker"
    );
    let next = output_at(&fixture, 1, 7, outputs[5].consensus_header_hash());
    assert_eq!(manager.consensus_chain.latest_consensus_number() + 1, next.number());
    manager.consensus_chain.clone().close().await;
    Ok(())
}

#[tokio::test]
async fn replay_checkpoint_keeps_already_saved_unexecuted_outputs_replayable() -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs, _) = imported_manager(&temp, 4, 2).await?;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    manager.replay_missed_consensus(fixture.committee(), &to_engine).await?;
    for expected in &outputs[2..] {
        assert_eq!(engine_rx.try_recv()?.consensus_header_hash(), expected.consensus_header_hash());
    }
    assert!(engine_rx.try_recv().is_err());
    assert_eq!(manager.consensus_chain.latest_consensus_number(), 6);
    manager.consensus_chain.clone().close().await;
    Ok(())
}

#[tokio::test]
async fn replay_checkpoint_does_not_promote_to_an_unreplayed_pack_tip() -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs, _) = imported_manager(&temp, 2, 2).await?;
    // Even if a pack has more stored outputs, replay must stop/checkpoint at the boundary.
    manager.epoch_boundary = 4;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let mut replay = manager.replay_missed_consensus(fixture.committee(), &to_engine).await?;
    assert_eq!(replay.take_epoch_close_hash(), Some(outputs[3].consensus_header_hash()));
    assert_eq!(engine_rx.try_recv()?.number(), 3);
    assert_eq!(engine_rx.try_recv()?.number(), 4);
    assert!(engine_rx.try_recv().is_err());
    assert_eq!(
        manager.consensus_chain.latest_consensus_number(),
        4,
        "the downloaded pack tip is not proof that its whole suffix was replayed"
    );
    manager.consensus_chain.clone().close().await;
    Ok(())
}

/// A fresh observer that saved outputs 1 to 3 and executed none of them. Output 1 is built on
/// `B256::ZERO`, which is not the default header's digest in either build flavor, like the anchor
/// adiri's block 1 was built on. Output 2 is built on output 1, or on `B256::ZERO` as well when
/// `misparent_block2` is set; output 3 is built on output 2.
async fn fresh_manager_with_saved_outputs(
    temp: &TempDir,
    misparent_block2: bool,
) -> eyre::Result<(
    EpochManager<PathBuf, DatabaseType>,
    CommitteeFixture<MemDatabase>,
    Vec<ConsensusOutput>,
)> {
    let (manager, fixture) = observer_manager(temp).await?;
    let anchor: ConsensusHeaderDigest = B256::ZERO.into();
    assert_ne!(anchor, ConsensusHeader::default().digest());
    let one = output_at(&fixture, 0, 1, anchor);
    let block2_parent = if misparent_block2 { anchor } else { one.consensus_header_hash() };
    let two = output_at(&fixture, 0, 2, block2_parent);
    let three = output_at(&fixture, 0, 3, two.consensus_header_hash());
    let outputs = vec![one, two, three];
    for output in &outputs {
        manager.consensus_chain.save_consensus_output(output.clone()).await?;
    }
    manager.consensus_chain.persist_current().await?;
    assert!(
        state_sync::last_executed_consensus_block(&manager.consensus_bus, &manager.consensus_chain)
            .await?
            .is_none(),
        "the fixture must leave execution at genesis"
    );
    Ok((manager, fixture, outputs))
}

/// Issue #1790: a node that saved outputs before executing any must replay them on restart even
/// though block 1 is not built on the default header's digest.
#[tokio::test]
async fn replay_anchors_block1_on_its_own_parent_when_nothing_executed() -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs) = fresh_manager_with_saved_outputs(&temp, false).await?;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let mut replay = manager.replay_missed_consensus(fixture.committee(), &to_engine).await?;
    assert_eq!(replay.take_epoch_close_hash(), None);
    assert_eq!(replay.take_last_replayed_hash(), Some(outputs[2].consensus_header_hash()));
    for expected in &outputs {
        assert_eq!(engine_rx.try_recv()?.consensus_header_hash(), expected.consensus_header_hash());
    }
    assert!(engine_rx.try_recv().is_err());
    assert_eq!(manager.consensus_chain.latest_consensus_number(), 3);
    manager.consensus_chain.clone().close().await;
    Ok(())
}

/// Issue #1790, the import path: a node that imported the epoch-0 pack from a peer and restarted
/// before executing any of it must replay the pack, though block 1 is not built on the default
/// header's digest. From block 2 on the replay checks each parent link up to the epoch close. The
/// importer refuses a pack whose block 2 does not build on block 1, so this path cannot carry the
/// mis-parented case `replay_rejects_block2_not_built_on_block1_when_nothing_executed` covers.
#[tokio::test]
async fn replay_anchors_imported_block1_on_its_own_parent_before_execution() -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let anchor: ConsensusHeaderDigest = B256::ZERO.into();
    assert_ne!(anchor, ConsensusHeader::default().digest());
    let (mut manager, fixture, outputs, _) = imported_epoch0_manager(&temp, anchor, 0).await?;
    assert!(
        state_sync::last_executed_consensus_block(&manager.consensus_bus, &manager.consensus_chain)
            .await?
            .is_none(),
        "the fixture must leave execution at genesis"
    );
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let mut replay = manager.replay_missed_consensus(fixture.committee(), &to_engine).await?;
    assert_eq!(replay.take_epoch_close_hash(), Some(outputs[5].consensus_header_hash()));
    for expected in &outputs {
        assert_eq!(engine_rx.try_recv()?.consensus_header_hash(), expected.consensus_header_hash());
    }
    assert!(engine_rx.try_recv().is_err());
    assert_eq!(manager.consensus_chain.latest_consensus_number(), 6);
    manager.consensus_chain.clone().close().await;
    Ok(())
}

/// The block-1 anchor exemption ends at block 1: a block 2 that does not build on block 1 still
/// halts the replay, after block 1 alone reached the engine.
#[tokio::test]
async fn replay_rejects_block2_not_built_on_block1_when_nothing_executed() -> eyre::Result<()> {
    let temp = TempDir::new()?;
    let (mut manager, fixture, outputs) = fresh_manager_with_saved_outputs(&temp, true).await?;
    let (to_engine, mut engine_rx) = mpsc::channel(8);
    let Err(err) = manager.replay_missed_consensus(fixture.committee(), &to_engine).await else {
        panic!("block 2 does not build on block 1, so replay must fail");
    };
    assert_eq!(
        err.to_string(),
        format!(
            "restart replay parent mismatch: consensus 2 has parent_hash {:?}, expected consensus \
             1's digest {:?}",
            outputs[1].parent_hash(),
            outputs[0].consensus_header_hash(),
        )
    );
    assert_eq!(engine_rx.try_recv()?.consensus_header_hash(), outputs[0].consensus_header_hash());
    assert!(engine_rx.try_recv().is_err(), "nothing past the mismatch may reach the engine");
    manager.consensus_chain.clone().close().await;
    Ok(())
}

/// A numbering gap keeps its own message, apart from a parent mismatch, and the block-1 anchor
/// exemption covers only block 1's parent hash, never its number.
#[test]
fn replay_link_reports_a_numbering_gap_by_number() {
    let header = |number, parent_hash: ConsensusHeaderDigest| ConsensusHeader {
        number,
        parent_hash,
        ..Default::default()
    };
    let genesis = ConsensusHeader::default();
    let one = header(1, B256::ZERO.into());
    let two = header(2, one.digest());
    let three = header(3, two.digest());
    assert!(check_replay_link(&genesis, &one).is_ok());
    assert!(check_replay_link(&one, &two).is_ok());

    // block 3 straight after block 1, with block 2's digest as parent
    let err = check_replay_link(&one, &three).expect_err("2 is missing");
    assert_eq!(
        err.to_string(),
        "restart replay is not contiguous: expected consensus 2 following 1, got 3"
    );
    // with nothing executed, the replay must still start at block 1
    let err = check_replay_link(&genesis, &two).expect_err("1 is missing");
    assert_eq!(
        err.to_string(),
        "restart replay is not contiguous: expected consensus 1 following 0, got 2"
    );
}
