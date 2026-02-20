//! Test utilities.

use crate::network::stream_codec::write_batch;
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
use tn_types::max_batch_size;
use tn_types::{
    Batch, BlockHash, BlsKeypair, BlsPublicKey, Database, SealedBatch, TaskManager, TaskSpawner,
};
use tokio::sync::{mpsc, oneshot, Mutex as TokioMutex};

#[derive(Clone, Debug)]
/// Test quorum waiter.
pub struct TestMakeBlockQuorumWaiter(pub Arc<Mutex<Option<SealedBatch>>>);
impl TestMakeBlockQuorumWaiter {
    /// New `Self` for test.
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

#[derive(Clone, Debug)]
pub struct TestRequestBatchesNetwork {
    // Worker name -> batch digests it has -> batches.
    data: Arc<TokioMutex<HashMap<BlsPublicKey, HashMap<BlockHash, Batch>>>>,
    handle: WorkerNetworkHandle,
}

impl Default for TestRequestBatchesNetwork {
    fn default() -> Self {
        Self::new()
    }
}

impl TestRequestBatchesNetwork {
    pub fn new() -> Self {
        let data: Arc<TokioMutex<HashMap<BlsPublicKey, HashMap<BlockHash, Batch>>>> =
            Arc::new(TokioMutex::new(HashMap::new()));
        let data_clone = data.clone();
        let (tx, mut rx) = mpsc::channel(100);
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new(NetworkHandle::new(tx), task_manager.get_spawner());
        tokio::spawn(async move {
            let _owned = task_manager;
            while let Some(r) = rx.recv().await {
                match r {
                    NetworkCommand::ConnectedPeers { reply } => {
                        reply.send(data_clone.lock().await.keys().copied().collect()).unwrap();
                    }
                    NetworkCommand::SendRequest {
                        peer: _,
                        request: WorkerRequest::RequestBatchesStream { batch_digests: _ },
                        reply,
                    } => {
                        // For stream requests in tests, reject them so tests fall back
                        // to request-response or we can test stream handling separately
                        reply
                            .send(Ok(WorkerResponse::RequestBatchesStream { ack: false }))
                            .unwrap();
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

/// Create `count` test batches, each with a unique transaction.
pub fn create_test_batches(count: usize) -> Vec<Batch> {
    (0..count)
        .map(|i| {
            let tx = vec![i as u8; 32]; // unique per batch
            Batch { transactions: vec![tx], ..Default::default() }
        })
        .collect()
}

/// Create a [MemDatabase] and insert the given batches.
pub fn setup_batch_db(batches: &[Batch]) -> tn_storage::mem_db::MemDatabase {
    let db = tn_storage::mem_db::MemDatabase::default();
    for batch in batches {
        db.insert::<tn_storage::tables::Batches>(&batch.digest(), batch)
            .expect("insert batch into test db");
    }
    db
}

/// Encode batches into the stream wire format for client-side test consumption.
///
/// Format: `[4-byte chunk_count][batch1][batch2]...`
pub async fn encode_batches_to_stream_bytes(batches: &[Batch]) -> Vec<u8> {
    let max_size = max_batch_size(0);
    let mut output = Vec::new();
    let mut encode_buffer = Vec::with_capacity(max_size);
    let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

    // write chunk count
    let count = batches.len() as u32;
    output.extend_from_slice(&count.to_le_bytes());

    // write each batch
    for batch in batches {
        write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer)
            .await
            .expect("encode batch");
    }

    output
}
