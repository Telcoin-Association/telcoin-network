use super::*;
use tn_network_libp2p::ConsensusNetwork;
use tn_storage::mem_db::MemDatabase;
use tn_test_utils::CommitteeFixture;
use tn_types::TaskManager;

#[test]
fn epoch_change_preserves_worker_stream_peer_and_shed_permits() {
    let serve = tn_config::NetworkServeConfig::default();
    let old_epoch = TaskManager::new("old-worker-epoch");
    let next_epoch = TaskManager::new("next-worker-epoch");
    let handle = WorkerNetworkHandle::new_for_test(old_epoch.get_spawner());
    let old = handle.sync_admission(&serve);
    let peer = BlsPublicKey::default();
    let peer_permits: Vec<_> = (0..MAX_PENDING_REQUESTS_PER_PEER)
        .map(|_| try_admit_sync(&old.stream_semaphore, &old.peers, peer).expect("admit old epoch"))
        .collect();
    let other_permits: Vec<_> = (0..old.stream_semaphore.available_permits())
        .map(|_| old.stream_semaphore.try_acquire_owned().expect("fill stream budget"))
        .collect();
    let shed_permits: Vec<_> = (0..serve.worker_shed())
        .map(|_| old.shed_semaphore.try_acquire_owned().expect("fill shed budget"))
        .collect();

    let mut next_handle = handle.clone();
    next_handle.update_epoch(1);
    next_handle.update_task_spawner(next_epoch.get_spawner());
    let next = next_handle.sync_admission(&serve);
    assert_eq!(next.stream_semaphore.available_permits(), 0);
    assert!(next.shed_semaphore.try_acquire_owned().is_err());

    drop(other_permits);
    assert!(try_admit_sync(&next.stream_semaphore, &next.peers, peer).is_none());
    assert_eq!(next.peers.lock().get(&peer).copied(), Some(MAX_PENDING_REQUESTS_PER_PEER));
    drop(peer_permits);
    drop(shed_permits);
    assert_eq!(next.stream_semaphore.available_permits(), serve.batch_stream());
    assert_eq!(next.shed_semaphore.available_permits(), serve.worker_shed());
    assert!(next.peers.lock().is_empty());
    assert!(try_admit_sync(&next.stream_semaphore, &next.peers, peer).is_some());
}

#[tokio::test]
async fn worker_sync_owner_survives_epoch_abort_and_releases_permits_on_node_abort(
) -> eyre::Result<()> {
    let committee = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let config = committee.first_authority().consensus_config();
    let mut node = TaskManager::new("worker-node");
    let mut epoch = TaskManager::new("worker-epoch");
    let next_epoch = TaskManager::new("next-worker-epoch");
    let (events_tx, _events_rx) =
        tokio::sync::mpsc::channel::<NetworkEvent<WorkerRequest, WorkerResponse>>(8);
    let core = ConsensusNetwork::<WorkerRequest, WorkerResponse, MemDatabase, _>::new_for_worker(
        0,
        config.network_config(),
        events_tx,
        config.key_config().clone(),
        MemDatabase::default(),
        node.get_spawner(),
        config.worker_address(0).expect("worker 0 address"),
        None,
    )?;
    let handle = WorkerNetworkHandle::new(
        core.network_handle(),
        epoch.get_spawner(),
        0,
        0,
        config.network_config().chain_id(),
    );
    let serve = config.network_config().serve_limits();
    let admission = handle.sync_admission(serve);
    let peer = BlsPublicKey::default();
    let permit = try_admit_sync(&admission.stream_semaphore, &admission.peers, peer)
        .expect("admit transfer before epoch change");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (done_tx, mut done_rx) = tokio::sync::oneshot::channel();
    handle.get_sync_task_spawner().spawn_task("held worker sync", async move {
        let _ = started_tx.send(());
        let _ = release_rx.await;
        drop(permit);
        let _ = done_tx.send(());
        Ok(())
    });
    started_rx.await?;
    node.update_tasks();
    epoch.update_tasks();
    epoch.abort_all_tasks();
    epoch.wait_for_task_shutdown().await;
    assert!(matches!(done_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
    assert_eq!(admission.stream_semaphore.available_permits(), serve.batch_stream() - 1);
    release_tx.send(()).expect("node-owned task survived the epoch");
    done_rx.await?;

    let mut next = handle.clone();
    next.update_epoch(1);
    next.update_task_spawner(next_epoch.get_spawner());
    let next_admission = next.sync_admission(serve);
    let permit = try_admit_sync(&next_admission.stream_semaphore, &next_admission.peers, peer)
        .expect("previous transfer released its slot");
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (cancel_tx, cancel_rx) = tokio::sync::oneshot::channel::<()>();
    next.get_sync_task_spawner().spawn_task("worker sync at node shutdown", async move {
        let _permit = permit;
        let _cancel = cancel_tx;
        let _ = started_tx.send(());
        std::future::pending::<()>().await;
        Ok(())
    });
    started_rx.await?;
    node.update_tasks();
    node.abort_all_tasks();
    node.wait_for_task_shutdown().await;
    assert!(tokio::time::timeout(Duration::from_secs(5), cancel_rx).await?.is_err());
    assert_eq!(next_admission.stream_semaphore.available_permits(), serve.batch_stream());
    assert!(next_admission.peers.lock().is_empty());
    Ok(())
}
