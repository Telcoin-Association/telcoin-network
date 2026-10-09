//! Forwarder recovery requests, coalescing, and endpoint convergence.

use super::*;

/// Record refresh requests without requiring a live libp2p swarm.
#[derive(Clone, Default)]
struct RecordingRefresher {
    /// Requests shared by every clone, matching the production cache's ownership.
    requests: Arc<Mutex<Vec<BlsPublicKey>>>,
}

impl RecordingRefresher {
    /// Snapshot submitted requests for assertions.
    fn requests(&self) -> Vec<BlsPublicKey> {
        self.requests.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).clone()
    }
}

impl CommitteeRecordRefresher for RecordingRefresher {
    fn refresh_record(&self, authority: BlsPublicKey) {
        self.requests.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).push(authority);
    }
}

/// Missing URLs trigger one shared request per cooldown and rotated-out keys leave the cache.
#[tokio::test(start_paused = true)]
async fn committee_record_missing_urls_coalesce_across_forwarder_clones() {
    let manager = TaskManager::default();
    let recorder = RecordingRefresher::default();
    let forwarder =
        WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::PublicOnly, None)
            .with_record_refresher(recorder.clone());
    let committee = vec![test_key(1), test_key(2)];
    (0..100).for_each(|_| {
        assert!(!forwarder.clone().forward_txns(vec![vec![0_u8; 32]], committee.clone(), vec![]));
    });
    assert_eq!(recorder.requests(), committee);
    tokio::time::advance(UNREACHABLE_COOLDOWN).await;
    assert!(!forwarder.forward_txns(vec![vec![0_u8; 32]], committee.clone(), vec![]));
    assert_eq!(recorder.requests().len(), 4);
    assert!(!forwarder.forward_txns(vec![vec![0_u8; 32]], vec![test_key(2)], vec![]));
    let cache = forwarder.cache.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(cache.refresh_requests.keys().copied().collect::<Vec<_>>(), vec![test_key(2)]);
}

/// A transport failure requests recovery before another batch, then a new URL is usable.
#[tokio::test]
async fn committee_record_demoted_endpoint_requests_prompt_recovery() -> eyre::Result<()> {
    let closed = std::net::TcpListener::bind("127.0.0.1:0")?;
    let endpoint = format!("http://{}", closed.local_addr()?);
    drop(closed);
    let manager = TaskManager::default();
    let recorder = RecordingRefresher::default();
    let forwarder =
        WorkerRpcForwarder::new(manager.get_spawner(), ForwardTargetPolicy::AllowPrivate, None)
            .with_record_refresher(recorder.clone());
    let authority = test_key(1);
    let rpcs = vec![(authority, test_rpc(&endpoint)?)];
    assert!(forwarder.forward_txns(vec![vec![0_u8; 32]], vec![authority], rpcs.clone()));
    let drained = timeout(
        Duration::from_secs(30),
        Arc::clone(&forwarder.forwards_in_flight).acquire_many_owned(max_permits()),
    )
    .await?;
    drop(drained?);
    assert_eq!(
        recorder.requests(),
        vec![authority],
        "failure must request a new record immediately"
    );
    assert!(!forwarder.forward_txns(vec![vec![0_u8; 32]], vec![authority], rpcs));
    assert_eq!(recorder.requests(), vec![authority], "demoted endpoints must coalesce retries");
    let replacement = test_rpc("http://127.0.0.1:8545")?;
    let providers = forwarder.cached_providers(&[(authority, replacement.clone())]);
    assert_eq!(providers.get(&authority).map(|(url, _)| url), Some(&replacement.http.to_string()));
    assert!(forwarder
        .cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .unreachable
        .is_empty());
    Ok(())
}
