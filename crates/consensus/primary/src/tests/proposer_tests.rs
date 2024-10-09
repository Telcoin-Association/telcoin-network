// Copyright (c) Telcoin, LLC
// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Proposer unit tests.
use super::*;
use crate::consensus::LeaderSwapTable;
use consensus_metrics::spawn_logged_monitored_task;
use indexmap::IndexMap;
use narwhal_typed_store::open_db;
use reth_primitives::B256;
use tempfile::TempDir;
use tn_types::{
    test_utils::{fixture_payload, CommitteeFixture},
    BlockHash, Notifier,
};

#[tokio::test]
async fn test_empty_proposal() {
    // reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let name = primary.id();

    let mut tx_shutdown = Notifier::new();
    let (_tx_parents, rx_parents) = tn_types::test_channel!(1);
    let (_tx_committed_own_headers, rx_committed_own_headers) = tn_types::test_channel!(1);
    let (_tx_our_digests, rx_our_digests) = tn_types::test_channel!(1);
    let (_tx_system_messages, rx_system_messages) = tn_types::test_channel!(1);
    let (tx_headers, mut rx_headers) = tn_types::test_channel!(1);
    let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);

    let metrics = Arc::new(PrimaryMetrics::default());
    let temp_dir = TempDir::new().unwrap();
    let db = open_db(temp_dir.path());

    // Spawn the proposer.
    let proposer_store = ProposerStore::new(db);
    let proposer_task = Proposer::new(
        name,
        committee.clone(),
        proposer_store,
        /* header_num_of_batches_threshold */ 32,
        /* max_header_num_of_batches */ 100,
        /* max_header_delay */ Duration::from_millis(20),
        /* min_header_delay */ Duration::from_millis(20),
        None, // default fatal timer
        tx_shutdown.subscribe(),
        /* synchronizer */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_system_messages,
        /* tx_synchronizer */ tx_headers,
        tx_narwhal_round_updates,
        rx_committed_own_headers,
        metrics,
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
    // reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let name = primary.id();
    // long enough for proposer to build but not too long for tests
    let fatal_header_interval = Duration::from_secs(3);

    let mut tx_shutdown = Notifier::new();
    let (tx_parents, rx_parents) = tn_types::test_channel!(1);
    let (tx_our_digests, rx_our_digests) = tn_types::test_channel!(1);
    let (_tx_system_messages, rx_system_messages) = tn_types::test_channel!(1);
    let (_tx_committed_own_headers, rx_committed_own_headers) = tn_types::test_channel!(1);
    let (tx_headers, mut rx_headers) = tn_types::test_channel!(1);
    let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);

    let metrics = Arc::new(PrimaryMetrics::default());

    let max_num_of_batches = 10;

    // Spawn the proposer.
    let temp_dir = TempDir::new().unwrap();
    let proposer_store = ProposerStore::new(open_db(temp_dir.path()));
    let proposer_task = Proposer::new(
        name,
        committee.clone(),
        proposer_store,
        /* header_num_of_batches_threshold */ 1,
        /* max_header_num_of_batches */ max_num_of_batches,
        /* max_header_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* min_header_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        Some(fatal_header_interval),
        tx_shutdown.subscribe(),
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_system_messages,
        /* tx_synchronizer */ tx_headers.clone(),
        tx_narwhal_round_updates,
        rx_committed_own_headers,
        metrics,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let created_at_ts = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();

    tracing::error!(target: "primary", "sending our digests...");
    tx_our_digests
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
        tx_our_digests
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
    let result = tx_parents.send((parents, 1)).await;
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

    // update watch channel to simulate execution progress
    // tx_watch.send((2, BlockNumHash::new(2, B256::random()))).expect("el watch channel updated");

    // fill tx_headers before round 3 (capacity 1) to simulate to trigger fatal timer
    // use the same header for convenience, makes no difference
    // just fill the send channel - don't call recv()
    let fill_channel = tx_headers.send(header).await;
    assert!(fill_channel.is_ok());

    // send parents to advance the 2 round
    let parents: Vec<_> =
        fixture.headers_next_round().iter().take(4).map(|h| fixture.certificate(h)).collect();

    tracing::error!(target: "primary", "FINAL sending parents...");
    let result = tx_parents.send((parents, 2)).await;
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
    // reth_tracing::init_test_tracing();
    let fixture = CommitteeFixture::builder().build();
    let committee = fixture.committee();
    let worker_cache = fixture.worker_cache();
    let primary = fixture.authorities().next().unwrap();
    let authority_id = primary.id();
    let temp_dir = TempDir::new().unwrap();
    let proposer_store = ProposerStore::new(open_db(temp_dir.path()));

    let mut tx_shutdown = Notifier::new();
    let (tx_parents, rx_parents) = tn_types::test_channel!(1);
    let (tx_our_digests, rx_our_digests) = tn_types::test_channel!(1);
    let (_tx_system_messages, rx_system_messages) = tn_types::test_channel!(1);
    let (tx_headers, mut rx_headers) = tn_types::test_channel!(1);
    let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);
    let (_tx_committed_own_headers, rx_committed_own_headers) = tn_types::test_channel!(1);
    let metrics = Arc::new(PrimaryMetrics::default());

    // Spawn the proposer.
    let proposer_task = Proposer::new(
        authority_id,
        committee.clone(),
        proposer_store.clone(),
        /* header_num_of_batches_threshold */ 1,
        /* max_header_num_of_batches */ 10,
        /* max_header_delay */
        Duration::from_secs(1_000), // Ensure it is not triggered.
        /* min_header_delay */
        Duration::from_secs(1_000), // Ensure it is not triggered.
        None,
        tx_shutdown.subscribe(),
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_system_messages,
        /* tx_synchronizer */ tx_headers,
        tx_narwhal_round_updates,
        rx_committed_own_headers,
        metrics,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let created_at_ts = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    tx_our_digests
        .send(OurDigestMessage { digest, worker_id, timestamp: created_at_ts, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send parents
    let parents: Vec<_> =
        fixture.headers().iter().take(3).map(|h| fixture.certificate(h)).collect();

    let result = tx_parents.send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes a correct header from the provided payload.
    let header = rx_headers.recv().await.unwrap();
    assert_eq!(header.payload().get(&digest), Some(&(worker_id, created_at_ts)));
    assert!(header.validate(&committee, &worker_cache).is_ok());

    // TODO: assert header el state present

    // restart the proposer.
    tx_shutdown.notify();
    assert!(proposer_handle.await.is_ok());

    let mut tx_shutdown = Notifier::new();
    let (tx_parents, rx_parents) = tn_types::test_channel!(1);
    let (tx_our_digests, rx_our_digests) = tn_types::test_channel!(1);
    let (_tx_system_messages, rx_system_messages) = tn_types::test_channel!(1);
    let (tx_headers, mut rx_headers) = tn_types::test_channel!(1);
    let (tx_narwhal_round_updates, _rx_narwhal_round_updates) = watch::channel(0u64);
    let (_tx_committed_own_headers, rx_committed_own_headers) = tn_types::test_channel!(1);
    let metrics = Arc::new(PrimaryMetrics::default());

    let proposer_task = Proposer::new(
        authority_id,
        committee.clone(),
        proposer_store,
        /* header_num_of_batches_threshold */ 1,
        /* max_header_num_of_batches */ 10,
        /* max_header_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        /* min_header_delay */
        Duration::from_millis(1_000_000), // Ensure it is not triggered.
        None,
        tx_shutdown.subscribe(),
        /* rx_core */ rx_parents,
        /* rx_workers */ rx_our_digests,
        rx_system_messages,
        /* tx_synchronizer */ tx_headers,
        tx_narwhal_round_updates,
        rx_committed_own_headers,
        metrics,
        LeaderSchedule::new(committee.clone(), LeaderSwapTable::default()),
    );

    let _proposer_handle = spawn_logged_monitored_task!(proposer_task, "proposer test empty");

    // Send enough digests for the header payload.
    let digest = B256::random();
    let worker_id = 0;
    let (tx_ack, rx_ack) = tokio::sync::oneshot::channel();
    tx_our_digests
        .send(OurDigestMessage { digest, worker_id, timestamp: 0, ack_channel: tx_ack })
        .await
        .unwrap();

    // Create and send a superset parents, same round but different set from before
    let parents: Vec<_> =
        fixture.headers().iter().take(4).map(|h| fixture.certificate(h)).collect();

    let result = tx_parents.send((parents, 1)).await;
    assert!(result.is_ok());
    assert!(rx_ack.await.is_ok());

    // Ensure the proposer makes the same header as before
    let new_header = rx_headers.recv().await.unwrap();
    if new_header.round() == header.round() {
        assert_eq!(header, new_header);
    }
}
