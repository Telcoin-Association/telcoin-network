//! Primary tests

use crate::{
    error::PrimaryNetworkError,
    network::{handler::RequestHandler, PrimaryResponse},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tn_network_types::MockPrimaryToWorkerClient;
use tn_primary::test_utils::make_optimal_signed_certificates;
use tn_reth::test_utils::fixture_batch_with_transactions;
use tn_storage::{consensus::ConsensusChain, mem_db::MemDatabase, CertificateStore, PayloadStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{
    error::HeaderError, now, BlockNumHash, Certificate, Committee, ConsensusHeaderDigest,
    ConsensusNumHash, ExecHeader, Hash as _, SealedHeader, TaskManager,
};
use tokio::time::timeout;

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_too_new() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(100) // Need to be bigger than the gc window
        .latest_execution_block(BlockNumHash::default()) // dummy_hash would be correct here but this is the test...
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(
        matches!(result, Err(PrimaryNetworkError::InvalidHeader(HeaderError::TooNew { .. }))),
        "{result:?}"
    );
}

/// Regression for #789: a committee member can craft its own round-0 header (the Vote path is
/// committee-gated). Before the fix, `check_for_missing_parents` evaluated `header.round() - 1`
/// at round 0, wrapping to `u32::MAX` on the release binary and leaving a stale
/// `(u32::MAX, digest)` requested-parent entry. The header is now rejected before parent tracking,
/// so no underflow occurs and no stale state is left behind.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_round_zero_rejected() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Mock certificates to use as (bogus) parent digests on the crafted header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();

    // Craft a self-consistent round-0 header with non-empty parents, mirroring the attack shape.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(0)
        .latest_execution_block(BlockNumHash::default())
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // The round-0 header is rejected before parent tracking: no underflow, no stale entry.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(
        matches!(result, Err(PrimaryNetworkError::InvalidHeader(HeaderError::InvalidRound(_)))),
        "{result:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(BlockNumHash::default()) // dummy_hash would be correct here but this is the test...
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(result.is_err(), "{result:?}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_older_execution_block() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    // This will be an "older" execution block, test this still works.
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let mut dummy = ExecHeader { nonce: 110_u64.into(), ..Default::default() };
    dummy.nonce = 110_u64.into();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(SealedHeader::seal_slow(dummy)),
        )
    });
    dummy = ExecHeader { nonce: 120_u64.into(), ..Default::default() };
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(SealedHeader::seal_slow(dummy)),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    cb.app().committed_round_updates().send_replace(2);
    // Trying to build on off of a missing execution block, will be an error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new())).await;
    let result = result.unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_has_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());
    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(2)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Write some certificates from round 2 into the store, and leave out the rest to test
    // headers with some parents but not all available. Round 1 certificates should be written
    // into the storage as parents of round 2 certificates. But to test phase 2 they are left out.
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }

    cb.app().committed_round_updates().send_replace(1);
    // TEST PHASE 1: Handler should report missing parent certificates to caller.
    let missing = if let PrimaryResponse::MissingParents(missing) =
        handler.vote(author_peer, test_header.clone(), Vec::new()).await.unwrap()
    {
        missing
    } else {
        panic!("Response not missing!");
    };

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // TEST PHASE 2: Handler should re-issue the same MissingParents when proposer retries with
    // empty parents (fresh request). This avoids a deadlock where the proposer's certifier
    // restarts and the cached state causes a fatal WrongNumberOfParents error.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header.clone(), Vec::new()))
            .await;
    let missing2 = if let Ok(Ok(PrimaryResponse::MissingParents(missing))) = result {
        missing
    } else {
        panic!("Expected MissingParents response on retry, got: {result:?}");
    };
    // Should re-issue the same missing parents as before.
    let received_missing2: HashSet<_> = missing2.into_iter().collect();
    assert_eq!(expected_missing, received_missing2);

    // TEST PHASE 3: With the MissingParents state still cached, a fresh empty-parents request
    // continues to re-issue MissingParents (proposer is expected to provide them).
    // Increase round threshold to simulate header becoming old, but the cached MissingParents
    // state means the handler still re-issues the same request before TooOld is checked.
    cb.app().primary_round_updates().send_replace(100);
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new()))
            .await
            .unwrap();
    assert!(
        matches!(result, Ok(PrimaryResponse::MissingParents(_))),
        "Expected MissingParents on retry, got: {result:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_request_vote_accept_missing_parents() {
    const NUM_PARENTS: usize = 10;
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(NUM_PARENTS).unwrap())
        .build();
    let target = fixture.authorities().next().unwrap();
    let author = fixture.authorities().nth(2).unwrap();
    let author_id = author.id();
    let author_peer = *author.authority().protocol_key();

    let certificate_store = target.consensus_config().node_storage().clone();
    let payload_store = target.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(target.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        target.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let committee: Committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let ids: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _next_parents) =
        make_optimal_signed_certificates(1..=3, &genesis, &committee, ids.as_slice());

    let all_certificates = certificates.into_iter().collect::<Vec<_>>();
    let round_1_certs = all_certificates[..NUM_PARENTS].to_vec();
    let round_2_certs = all_certificates[NUM_PARENTS..(NUM_PARENTS * 2)].to_vec();
    let round_2_parents = round_2_certs[..(NUM_PARENTS / 2)].to_vec();
    let round_2_missing = round_2_certs[(NUM_PARENTS / 2)..].to_vec();

    // Create a test header.
    let test_header = author
        .header_builder(&fixture.committee())
        .author(author_id)
        .round(3)
        .parents(round_2_certs.iter().map(|c| c.digest()).collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Populate all round 1 certificates and some round 2 certificates into the storage.
    // The new header will have some round 2 certificates missing as parents, but these parents
    // should be able to get accepted.
    for cert in round_1_certs {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    for cert in round_2_parents {
        for (digest, worker_id) in cert.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
        certificate_store.write(cert.clone()).unwrap();
    }
    // Populate new header payload so they don't have to be retrieved.
    for (digest, worker_id) in test_header.payload() {
        payload_store.write_payload(digest, worker_id).unwrap();
    }

    cb.app().committed_round_updates().send_replace(2);
    // TEST PHASE 1: Handler should report missing parent certificates to caller.
    let missing = if let PrimaryResponse::MissingParents(missing) =
        handler.vote(author_peer, test_header.clone(), Vec::new()).await.unwrap()
    {
        missing
    } else {
        panic!("Response not missing!");
    };

    let expected_missing: HashSet<_> = round_2_missing.iter().map(|c| c.digest()).collect();
    let received_missing: HashSet<_> = missing.into_iter().collect();
    assert_eq!(expected_missing, received_missing);

    // TEST PHASE 2: Handler should process missing parent certificates and succeed.
    let result =
        timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, round_2_missing))
            .await
            .unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test]
async fn test_request_vote_missing_batches() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let authority_id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        primary.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != authority_id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(&fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    cb.app().committed_round_updates().send_replace(1);
    // Verify Handler synchronizes missing batches and generates a Vote.
    let _vote = timeout(Duration::from_secs(5), handler.vote(author_peer, test_header, Vec::new()))
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_request_vote_already_voted() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        primary.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(&fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    // Verify Handler generates a Vote.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    cb.app().committed_round_updates().send_replace(1);
    let vote = if let PrimaryResponse::Vote(vote) = tokio::time::timeout(
        Duration::from_secs(10),
        handler.vote(author_peer, test_header.clone(), Vec::new()),
    )
    .await
    .unwrap()
    .unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };

    // Verify the same request gets the same vote back successfully.
    let vote2 = if let PrimaryResponse::Vote(vote) =
        handler.vote(author_peer, test_header, Vec::new()).await.unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };
    assert_eq!(vote.digest(), vote2.digest());

    // Verify a different request for the same round receives an error.
    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .build();

    let response = handler.vote(author_peer, test_header, Vec::new()).await;
    assert!(response.is_err());
}

#[tokio::test]
async fn test_request_vote_created_at_in_future() {
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().unwrap();
    let id = primary.id();
    let author = fixture.authorities().nth(2).unwrap();
    let author_peer = *author.authority().protocol_key();
    let client = primary.consensus_config().local_network().clone();

    let certificate_store = primary.consensus_config().node_storage().clone();
    let payload_store = primary.consensus_config().node_storage().clone();

    let cb = ConsensusBus::new();
    let temp_dir = TempDir::new().unwrap();
    let consensus_chain =
        ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
            .await
            .unwrap();
    // Need a dummy parent so we can request a vote.
    let dummy_parent = SealedHeader::seal_slow(ExecHeader::default());
    let dummy_hash = dummy_parent.hash();
    cb.app().recent_blocks().send_modify(|blocks| {
        blocks.push_latest(
            0,
            ConsensusNumHash::new(0, ConsensusHeaderDigest::default()),
            Some(dummy_parent),
        )
    });
    let task_manager = TaskManager::default();
    let synchronizer =
        StateSynchronizer::new(primary.consensus_config(), cb.clone(), task_manager.get_spawner());
    synchronizer.spawn(&task_manager);
    let handler = RequestHandler::new(
        primary.consensus_config(),
        cb.app().clone(),
        synchronizer.clone(),
        consensus_chain,
    );

    // Make some mock certificates that are parents of our new header.
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .with_payload_batch(&fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set up mock worker.
    let mock_server = MockPrimaryToWorkerClient::default();

    client.set_primary_to_worker_local_handler(Arc::new(mock_server));

    // Verify Handler generates a Vote.

    // Set the creation time to be deep in the future (an hour)
    let created_at = now() + 60 * 60;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(2)
        .parents(certificates.keys().cloned().collect())
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .created_at(created_at)
        .build();

    // For such a future header we get back an error
    assert!(handler.vote(author_peer, test_header, Vec::new()).await.is_err());

    // Verify Handler generates a Vote.

    // Make some mock certificates that are parents of our new header.
    // New certs for a new header
    let mut certificates = HashMap::new();
    for primary in fixture.authorities().filter(|a| a.id() != id) {
        let header = primary
            .header_builder(&fixture.committee())
            .round(2)
            .with_payload_batch(&fixture_batch_with_transactions(10), 0)
            .build();

        let certificate = fixture.certificate(&header);
        let digest = certificate.clone().digest();

        certificates.insert(digest, certificate.clone());
        certificate_store.write(certificate.clone()).unwrap();
        for (digest, worker_id) in certificate.header().payload() {
            payload_store.write_payload(digest, worker_id).unwrap();
        }
    }

    // Set the creation time to be a bit in the future (1s)
    let created_at = now() + 1;

    let test_header = author
        .header_builder(&fixture.committee())
        .round(3)
        .latest_execution_block(BlockNumHash::new(0, dummy_hash))
        .parents(certificates.keys().cloned().collect())
        .with_payload_batch(&fixture_batch_with_transactions(10), 0)
        .created_at(created_at)
        .build();

    cb.app().committed_round_updates().send_replace(1);
    let _vote = if let PrimaryResponse::Vote(vote) =
        handler.vote(author_peer, test_header, Vec::new()).await.unwrap()
    {
        vote
    } else {
        panic!("not a vote!");
    };
    assert!(created_at <= now());
}
