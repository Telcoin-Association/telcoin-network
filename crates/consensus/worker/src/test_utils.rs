//! Test utilities.

use crate::quorum_waiter::{QuorumWaiterError, QuorumWaiterTrait};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_types::{SealedBatch, TaskSpawner};
use tokio::sync::oneshot;

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
