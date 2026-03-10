//! Test utilities.

use crate::{
    network::stream_codec::write_batch,
    quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait},
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_types::{max_batch_size, Batch, Database, SealedBatch, TaskSpawner};
use tokio::sync::oneshot;

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
        write_batch(&mut output, batch, &mut encode_buffer, &mut compressed_buffer, 0)
            .await
            .expect("encode batch");
    }

    output
}
