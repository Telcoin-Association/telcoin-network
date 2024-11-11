// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::PayloadToken;
use crate::{
    tables::Payload,
    traits::{Database, DbTx, DbTxMut},
};
use std::sync::Arc;
use tn_macros::fail_point;
use tn_sync::sync::notify_read::NotifyRead;
use tn_types::{BlockHash, WorkerId};

/// Store of the batch digests for the primary node for the own created batches.
#[derive(Clone)]
pub struct PayloadStore<DB> {
    store: DB, // Payload

    /// Senders to notify for a write that happened for the specified batch digest and worker id
    notify_subscribers: Arc<NotifyRead<(BlockHash, WorkerId), ()>>,
}

impl<DB: Database> PayloadStore<DB> {
    pub fn new(store: DB) -> Self {
        Self { store, notify_subscribers: Arc::new(NotifyRead::new()) }
    }

    pub fn write(&self, digest: &BlockHash, worker_id: &WorkerId) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");

        self.store.insert::<Payload>(&(*digest, *worker_id), &0u8)?;
        self.notify_subscribers.notify(&(*digest, *worker_id), &());

        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Writes all the provided values atomically in store - either all will succeed or nothing will
    /// be stored.
    pub fn write_all(
        &self,
        keys: impl IntoIterator<Item = (BlockHash, WorkerId)> + Clone,
    ) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");
        let mut txn = self.store.write_txn()?;
        for (digest, worker_id) in keys {
            txn.insert::<Payload>(&(digest, worker_id), &0u8)?;
            self.notify_subscribers.notify(&(digest, worker_id), &());
        }

        txn.commit()?;
        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Queries the store whether the batch with provided `digest` and `worker_id` exists. It
    /// returns `true` if exists, `false` otherwise.
    pub fn contains(&self, digest: BlockHash, worker_id: WorkerId) -> eyre::Result<bool> {
        self.store.contains_key::<Payload>(&(digest, worker_id))
    }

    /// When called the method will wait until the entry of batch with `digest` and `worker_id`
    /// becomes available.
    pub async fn notify_contains(
        &self,
        digest: BlockHash,
        worker_id: WorkerId,
    ) -> eyre::Result<()> {
        let receiver = self.notify_subscribers.register_one(&(digest, worker_id));

        // let's read the value because we might have missed the opportunity
        // to get notified about it
        if self.contains(digest, worker_id)? {
            // notify any obligations - and remove the entries (including ours)
            self.notify_subscribers.notify(&(digest, worker_id), &());

            // reply directly
            return Ok(());
        }

        // now wait to hear back the result
        receiver.await;

        Ok(())
    }

    pub fn read_all(
        &self,
        keys: impl IntoIterator<Item = (BlockHash, WorkerId)>,
    ) -> eyre::Result<Vec<Option<PayloadToken>>> {
        let txn = self.store.read_txn()?;
        keys.into_iter().map(|key| txn.get::<Payload>(&key)).collect()
    }

    #[allow(clippy::let_and_return)]
    pub fn remove_all(
        &self,
        keys: impl IntoIterator<Item = (BlockHash, WorkerId)>,
    ) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");
        let mut txn = self.store.write_txn()?;

        for key in keys.into_iter() {
            txn.remove::<Payload>(&key)?;
        }

        txn.commit()?;
        fail_point!("narwhal-store-after-write");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::open_db;
    use crate::PayloadStore;
    use futures::future::join_all;
    use tempfile::TempDir;
    use tn_types::WorkerBlock;

    #[tokio::test]
    async fn test_notify_read() {
        let temp_dir = TempDir::new().unwrap();
        let db = open_db(temp_dir.path());
        let store = PayloadStore::new(db);

        // run the tests a few times
        let batch: WorkerBlock = narwhal_test_utils::fixture_batch_with_transactions(10);
        let id = batch.digest();
        let worker_id = 0;

        // now populate a batch
        store.write(&id, &worker_id).unwrap();

        // now spawn a series of tasks before writing anything in store
        let mut handles = vec![];
        for _i in 0..5 {
            let cloned_store = store.clone();
            let handle =
                tokio::spawn(async move { cloned_store.notify_contains(id, worker_id).await });

            handles.push(handle)
        }

        // and populate the rest with a write_all
        store.write_all(vec![(id, worker_id)]).unwrap();

        // now asset the notify reads return with the result
        let result = join_all(handles).await;

        assert_eq!(result.len(), 5);

        for r in result {
            let token = r.unwrap();
            assert!(token.is_ok());
        }
    }
}
