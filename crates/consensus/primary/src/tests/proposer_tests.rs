// Copyright (c) Telcoin, LLC
// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Proposer unit tests.
use super::*;
use crate::consensus::LeaderSwapTable;
use consensus_metrics::spawn_logged_monitored_task;
use indexmap::IndexMap;
use reth_primitives::B256;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::{fixture_payload, CommitteeFixture};
use tn_types::{BlockHash, CHANNEL_CAPACITY};

#[tokio::test]
async fn test_empty_proposal() {
    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let proposer_task = Proposer::new(
        primary.consensus_config(),
        cb.clone(),
        None, // default fatal timer
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let _proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Ensure the proposer makes a correct empty header.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round(), 1);
    assert!(header.payload().is_empty());
    assert!(header.validate(&committee, &worker_cache).is_ok());

    // TODO: assert header el state present
}

#[tokio::test]
async fn test_propose_payload_fatal_timer() {
    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();
    // long enough for proposer to build but not too long for tests
    let fatal_header_interval = Duration::from_secs(3);

    let max_num_of_batches = 10;

    // Spawn the proposer.
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let proposer_task = Proposer::new(
        primary.consensus_config(),
        cb.clone(),
        Some(fatal_header_interval),
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let created_at_ts = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();

    tracing::error!(target: "primary", "sending our digests...");
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, timestamp: created_at_ts, ack_channel: tx_ack })
        .await
        .unwrap();
    tracing::error!(target: "primary", "digests sent! receiving header...");

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round(), 1);
    assert_eq!(header.payload().get(&digest), Some(&(worker_id, created_at_ts)));
    assert!(header.validate(&committee, &worker_cache).is_ok());

    // update watch channel to simulate execution progress
    // tx_watch.send((1, BlockNumHash::new(1, B256::random()))).expect("el watch channel updated");

    tracing::error!(target: "primary", "ASSERTED HEADER IN TEST :D");

    // TODO: assert header data present
    // assert!

    // WHEN available batches are more than the maximum ones
    let batches: IndexMap<BlockHash, (WorkerId, TimestampSec)> =
        fixture_payload((max_num_of_batches * 2) as u8);

    let mut ack_list = vec![];
    for (batch_id, (worker_id, created_at)) in batches {
        let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
        cb.our_digests()
            .send(OurDigestMessage {
                digest: batch_id,
                worker_id,
                timestamp: created_at,
                ack_channel: tx_ack,
            })
            .await
            .unwrap();

        ack_list.push(rx_ack);

        tokio::task::yield_now().await;
    }
    tracing::error!(target: "primary", "created ack list");

    // AND send some parents to advance the round
    let parents: Vec<_> =
        fixture.headers().iter().take(4).map(|h| fixture.certificate(h)).collect();

    tracing::error!(target: "primary", "sending parents...");
    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    tracing::error!(target: "primary", "parents sent! awaiting rx_headers...");

    // THEN the header should contain max_num_of_batches
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.round(), 2);
    assert_eq!(header.payload().len(), max_num_of_batches);
    assert!(rx_ack.await.is_ok());
    tracing::error!(target: "primary", "max num batches confirmed");

    // Check all batches are acked.
    for rx_ack in ack_list {
        assert!(rx_ack.await.is_ok());
    }
    tracing::error!(target: "primary", "all acks received");

    // fill tx_headers before round 3 to simulate to trigger fatal timer
    // use the same header for convenience, makes no difference
    // just fill the send channel - don't call recv()
    for _ in 0..CHANNEL_CAPACITY {
        cb.headers().send(header.clone()).await.unwrap();
    }

    // send parents to advance the 2 round
    let parents: Vec<_> =
        fixture.headers_next_round().iter().take(4).map(|h| fixture.certificate(h)).collect();

    tracing::error!(target: "primary", "FINAL sending parents...");
    let result = cb.parents().send((parents, 2)).await;
    assert!(result.is_ok());
    tracing::error!(target: "primary", "FINAL parents sent! awaiting rx_headers...");

    // round should advance but proposer is stuck waiting for certifier to process previous proposal
    assert!(matches!(
        proposer_handle.await.expect("poll ready"),
        Err(ProposerError::FatalHeaderTimeout(_))
    ));
}

#[tokio::test]
async fn test_equivocation_protection_after_restart() {
    reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder(MemDatabase::default).build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();

    /* Old comments, note if test gets flakey:
     max_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
     min_header_delay
    Duration::from_secs(1_000), // Ensure it is not triggered.
    */
    // Spawn the proposer.
    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let proposer_task = Proposer::new(
        primary.consensus_config(),
        cb.clone(),
        None,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let created_at_ts = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, timestamp: created_at_ts, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send parents
    let parents: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.payload().get(&digest), Some(&(worker_id, created_at_ts)));
    assert!(header.validate(&committee, &worker_cache).is_ok());

    // TODO: assert header el state present

    // restart the proposer.
    fixture.notify_shutdown();
    assert!(proposer_handle.await.is_ok());

    let cb = ConsensusBus::new();
    let mut rx_headers = cb.headers().subscribe();
    let proposer_task = Proposer::new(
        primary.consensus_config(),
        cb.clone(),
        None,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let _proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    cb.our_digests()
        .send(OurDigestMessage { digest, worker_id, timestamp: 0, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send a superset parents, same round but different set from before
    let parents: Vec<_> =
        fixture.headers().iter().take(4).map(|h| fixture.certificate(h)).collect();

    let result = cb.parents().send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes the same header as before
    let new_header = rx_headers.recv().await.unwrap();
    if new_header.round() == header.round() {
        assert_eq!(header, new_header);
    }
}
