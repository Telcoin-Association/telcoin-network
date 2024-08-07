// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::PayloadToken;
use narwhal_typed_store::{
    mem_db::MemDB,
    traits::{multi_get, multi_insert, multi_remove},
    DBMap,
};
use std::sync::Arc;
use telcoin_macros::fail_point;
use telcoin_sync::sync::notify_read::NotifyRead;
use tn_types::{BatchDigest, WorkerId};

/// Store of the batch digests for the primary node for the own created batches.
#[derive(Clone)]
pub struct PayloadStore {
    store: Arc<dyn DBMap<(BatchDigest, WorkerId), PayloadToken>>,

    /// Senders to notify for a write that happened for the specified batch digest and worker id
    notify_subscribers: Arc<NotifyRead<(BatchDigest, WorkerId), ()>>,
}

impl PayloadStore {
    pub fn new(store: Arc<dyn DBMap<(BatchDigest, WorkerId), PayloadToken>>) -> Self {
        Self { store, notify_subscribers: Arc::new(NotifyRead::new()) }
    }

    pub fn new_for_tests() -> Self {
        PayloadStore::new(Arc::new(MemDB::open()))
    }

    pub fn write(&self, digest: &BatchDigest, worker_id: &WorkerId) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");

        self.store.insert(&(*digest, *worker_id), &0u8)?;
        self.notify_subscribers.notify(&(*digest, *worker_id), &());

        let _ = self.store.commit();
        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Writes all the provided values atomically in store - either all will succeed or nothing will
    /// be stored.
    pub fn write_all(
        &self,
        keys: impl IntoIterator<Item = (BatchDigest, WorkerId)> + Clone,
    ) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");

        multi_insert(&*self.store, keys.clone().into_iter().map(|e| (e, 0u8)))?;

        keys.into_iter().for_each(|(digest, worker_id)| {
            self.notify_subscribers.notify(&(digest, worker_id), &());
        });

        let _ = self.store.commit();
        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Queries the store whether the batch with provided `digest` and `worker_id` exists. It
    /// returns `true` if exists, `false` otherwise.
    pub fn contains(&self, digest: BatchDigest, worker_id: WorkerId) -> eyre::Result<bool> {
        self.store.get(&(digest, worker_id)).map(|result| result.is_some())
    }

    /// When called the method will wait until the entry of batch with `digest` and `worker_id`
    /// becomes available.
    pub async fn notify_contains(
        &self,
        digest: BatchDigest,
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
        keys: impl IntoIterator<Item = (BatchDigest, WorkerId)>,
    ) -> eyre::Result<Vec<Option<PayloadToken>>> {
        multi_get(&*self.store, keys)
    }

    #[allow(clippy::let_and_return)]
    pub fn remove_all(
        &self,
        keys: impl IntoIterator<Item = (BatchDigest, WorkerId)>,
    ) -> eyre::Result<()> {
        fail_point!("narwhal-store-before-write");

        let result = multi_remove(&*self.store, keys);

        let _ = self.store.commit();
        fail_point!("narwhal-store-after-write");
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::PayloadStore;
    use fastcrypto::hash::Hash;
    use futures::future::join_all;
    use tn_types::Batch;

    #[tokio::test]
    async fn test_notify_read() {
        let store = PayloadStore::new_for_tests();

        // run the tests a few times
        let batch: Batch = tn_types::test_utils::fixture_batch_with_transactions(10);
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
