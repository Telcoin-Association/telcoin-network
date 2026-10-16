//! Regression tests for restart replay over a fully imported pack with stale slot hints.

use super::*;
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
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let datadir = temp.path().join("observer");
    std::fs::create_dir_all(&datadir)?;
    let config = Config::default_for_test();
    let (node_config, reth_db) = reth_config_and_db(&config, &committee, &datadir)?;
    let keys =
        KeyConfig::new_with_testing_key(BlsKeypair::generate(&mut StdRng::seed_from_u64(601)));
    std::fs::create_dir_all(datadir.join("db"))?;
    let db = open_db(datadir.join("db"));
    let builder = TnBuilder::new(node_config, config, reth_db);
    let mut manager = EpochManager::new(builder, datadir, db, keys, "test").await?;
    manager.epoch_boundary = 6;

    let source_dir = temp.path().join("source");
    std::fs::create_dir_all(&source_dir)?;
    let source = ConsensusChain::new_for_test(source_dir, committee.clone()).await?;
    let previous = EpochRecord {
        epoch: 0,
        committee: committee.bls_keys().iter().copied().collect(),
        next_committee: committee.bls_keys().iter().copied().collect(),
        ..Default::default()
    };
    let mut parent = ConsensusHeader::default().digest();
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
    let (stream, len) = source.get_epoch_stream(0).await?;
    manager
        .consensus_chain
        .stream_import(stream.take(len), &record, &previous, Duration::from_secs(5))
        .await?;
    for output in outputs.iter().take(saved) {
        manager.consensus_chain.save_consensus_output(output.clone()).await?;
    }
    manager.consensus_chain.persist_current().await?;
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
    source.close().await;
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
