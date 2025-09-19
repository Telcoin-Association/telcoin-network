//! Test utilities.

use crate::{
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
    WorkerNetworkHandle, WorkerRequest, WorkerResponse,
};
use rand::rngs::StdRng;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_network_libp2p::types::{NetworkCommand, NetworkHandle};
use tn_types::{Batch, BlockHash, BlsKeypair, BlsPublicKey, SealedBatch, TaskManager, TaskSpawner};
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};
// quorum waiter tests
pub use crate::network::message::WorkerRPCError;

#[derive(Clone, Debug)]
pub struct TestMakeBlockQuorumWaiter(pub Arc<Mutex<Option<SealedBatch>>>);
impl TestMakeBlockQuorumWaiter {
    pub fn new_test() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }
}

impl QuorumWaiterTrait for TestMakeBlockQuorumWaiter {
    fn verify_batch(
        &self,
        batch: SealedBatch,
        _timeout: Duration,
        task_spawner: &TaskSpawner,
    ) -> oneshot::Receiver<Result<(), QuorumWaiterError>> {
        let data = self.0.clone();
        let (tx, rx) = oneshot::channel();
        let task_name = format!("qw-test-{}", batch.digest());
        task_spawner.spawn_task(task_name, async move {
            *data.lock().unwrap() = Some(batch);
            tx.send(Ok(()))
        });
        rx
    }
}

#[derive(Clone)]
pub struct TestRequestBatchesNetwork {
    // Worker name -> batch digests it has -> batches.
    data: Arc<TokioMutex<HashMap<BlsPublicKey, HashMap<BlockHash, Batch>>>>,
    handle: WorkerNetworkHandle,
}

impl TestRequestBatchesNetwork {
    pub fn new() -> Self {
        let data: Arc<TokioMutex<HashMap<BlsPublicKey, HashMap<BlockHash, Batch>>>> =
            Arc::new(TokioMutex::new(HashMap::new()));
        let data_clone = data.clone();
        let (tx, mut rx) = mpsc::channel(100);
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new(
            NetworkHandle::new(tx),
            task_manager.get_spawner(),
            1024 * 1024,
        );
        tokio::spawn(async move {
            let _owned = task_manager;
            while let Some(r) = rx.recv().await {
                match r {
                    NetworkCommand::ConnectedPeers { reply } => {
                        reply.send(data_clone.lock().await.keys().copied().collect()).unwrap();
                    }
                    NetworkCommand::SendRequest {
                        peer,
                        request:
                            WorkerRequest::RequestBatches { batch_digests: digests, max_response_size },
                        reply,
                    } => {
                        // Use this to simulate server side response size limit in
                        // RequestBlocks
                        const MAX_READ_BLOCK_DIGESTS: usize = 5;

                        let mut batches = Vec::new();
                        let mut total_size = 0;

                        let digests_chunks = digests
                            .chunks(MAX_READ_BLOCK_DIGESTS)
                            .map(|chunk| chunk.to_vec())
                            .collect::<Vec<_>>();
                        for digests_chunk in digests_chunks {
                            for digest in digests_chunk {
                                if let Some(batch) =
                                    data_clone.lock().await.get(&peer).unwrap().get(&digest)
                                {
                                    let batch_size = batch.size();
                                    if total_size + batch_size <= max_response_size {
                                        batches.push(batch.clone());
                                        total_size += batch_size;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }

                        reply.send(Ok(WorkerResponse::RequestBatches(batches))).unwrap();
                    }
                    _ => {}
                }
            }
        });
        Self { data, handle }
    }

    pub async fn put(&mut self, keys: &[u8], batch: Batch) {
        for key in keys {
            let key = test_pk(*key);
            let mut guard = self.data.lock().await;
            let entry = guard.entry(key).or_default();
            entry.insert(batch.digest(), batch.clone());
        }
    }

    pub fn handle(&self) -> WorkerNetworkHandle {
        self.handle.clone()
    }
}

fn test_pk(i: u8) -> BlsPublicKey {
    use rand::SeedableRng;
    let mut rng = StdRng::from_seed([i; 32]);
    *BlsKeypair::generate(&mut rng).public()
}
