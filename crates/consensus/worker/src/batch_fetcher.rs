//! Fetch batches from database or peers.
//!
//! This type handles requests from the primary. Requests sent to the `BatchFetcher` have already
//! been certified.

use crate::network::{error::WorkerNetworkResult, WorkerNetworkHandle};
use std::collections::{HashMap, HashSet};
use tn_storage::tables::Batches;
use tn_types::{now, Batch, BlockHash, Database, DbTxMut};
use tracing::{debug, error, instrument};

#[derive(Debug)]
/// The type to fetch batches for the primary.
pub(crate) struct BatchFetcher<DB> {
    /// The handle for the network.
    network: WorkerNetworkHandle,
    /// The local database instance.
    batch_store: DB,
}

impl<DB: Database> BatchFetcher<DB> {
    /// Create a new instance of `Self`.
    pub(crate) fn new(network: WorkerNetworkHandle, batch_store: DB) -> Self {
        Self { network, batch_store }
    }

    /// Bulk fetches payload from local storage and remote workers.
    ///
    /// This function performs infinite retries and until all batches are available. This is called
    /// internally by `Subscriber` for each consensus output. The number of batches in a committed
    /// subdag is expected to be 10s of kbs max. Returns an error if the database writes fail.
    /// Otherwise, tries all peers infinitely until all batches successfully fetched.
    ///
    /// SAFETY: 10-node committees * 6-round commit max * 5 batch max = 300 max batch digests
    /// possible 32bytes * 300 = 9.6 kb => well within 1MB max message size
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    pub(crate) async fn fetch_for_primary(
        &self,
        mut missing_digests: HashSet<BlockHash>,
    ) -> eyre::Result<HashMap<BlockHash, Batch>> {
        debug!(target: "batch_fetcher", "Attempting to fetch {} digests from peers", missing_digests.len());

        // preallocate hashmap
        let mut fetched_batches = HashMap::with_capacity(missing_digests.len());

        // loop until `missing_digests` empty
        loop {
            debug!(target: "batch_fetcher", "loop start");
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // 1) fetch from local storage
            self.fetch_local(&mut missing_digests, &mut fetched_batches)?;

            // return if all batches recovered
            if missing_digests.is_empty() {
                return Ok(fetched_batches);
            }

            // 2) fetch from peers over network
            if let Ok(mut new_batches) = self.request_batches_from_peers(&mut missing_digests).await
            {
                // set received_at timestamp for remote batches
                let mut updated_new_batches = HashMap::new();
                let mut txn = self.batch_store.write_txn()?;

                // update batch timestamps and insert to db
                for (digest, batch) in
                    new_batches.iter_mut().filter(|(d, _)| missing_digests.remove(*d))
                {
                    batch.set_received_at(now());
                    updated_new_batches.insert(*digest, batch.clone());
                    // also persist the batches, so they are available after restarts
                    txn.insert::<Batches>(digest, batch)
                        .inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to insert batch! Node shutting down..."))?;
                }

                // commit db after all inserts
                txn.commit()
                    .inspect_err(|e| error!(target: "batch_fetcher", ?e, "failed to commit batch! Node shutting down..."))?;

                // add recovered batches to final collection
                fetched_batches.extend(updated_new_batches.iter().map(|(d, b)| (*d, (*b).clone())));

                // return if done, otherwise try again
                if missing_digests.is_empty() {
                    return Ok(fetched_batches);
                }
            }
        }
    }

    /// Retrieve batches from the local database.
    fn fetch_local(
        &self,
        missing_digests: &mut HashSet<BlockHash>,
        fetched_batches: &mut HashMap<BlockHash, Batch>,
    ) -> eyre::Result<()> {
        // read from database
        debug!(target: "batch_fetcher", "Local attempt to fetch {} missing_digests", missing_digests.len());
        let local_batches = self.batch_store.multi_get::<Batches>(missing_digests.iter())?;
        for (digest, batch) in missing_digests.iter().zip(local_batches.into_iter()) {
            if let Some(batch) = batch {
                debug_assert_eq!(*digest, batch.digest());
                fetched_batches.insert(*digest, batch);
            }
        }

        // remove fetched batches from missing
        missing_digests.retain(|d| !fetched_batches.contains_key(d));

        Ok(())
    }

    /// Issue request_batches RPC and verifies response integrity
    #[instrument(level = "debug", skip_all, fields(num_digests = missing_digests.len()))]
    async fn request_batches_from_peers(
        &self,
        missing_digests: &mut HashSet<BlockHash>,
    ) -> WorkerNetworkResult<HashMap<BlockHash, Batch>> {
        // request batches and return to caller
        let recovered_batches =
            self.network.request_batches(missing_digests).await?.into_iter().collect();

        Ok(recovered_batches)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        batch_fetcher::BatchFetcher,
        network::{stream_codec::write_batch, WorkerNetworkHandle},
        test_utils::{create_test_batches, encode_batches_to_stream_bytes, setup_batch_db},
    };
    use futures::io::Cursor;
    use std::collections::{HashMap, HashSet};
    use tn_network_libp2p::error::NetworkError;
    use tn_types::{max_batch_size, Batch, BlockHash, TaskManager, B256};

    #[tokio::test]
    async fn test_validate_batches_from_stream() {
        let batches = create_test_batches(3);
        let digests: HashSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();

        // encode batches to wire format
        let bytes = encode_batches_to_stream_bytes(&batches).await;
        let mut cursor = Cursor::new(bytes);

        // create handle (sends commands nowhere)
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        // read and validate
        let result = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;
        let validated = result.expect("should validate successfully");

        assert_eq!(validated.len(), batches.len());
        let validated_digests: HashSet<BlockHash> = validated.iter().map(|(d, _)| *d).collect();
        assert_eq!(validated_digests, digests);
    }

    #[tokio::test]
    async fn test_validate_rejects_too_many_batches() {
        // encode 5 batches but only request 3 digests
        let batches = create_test_batches(5);
        let bytes = encode_batches_to_stream_bytes(&batches).await;
        let mut cursor = Cursor::new(bytes);

        // only request 3
        let digests: HashSet<BlockHash> = batches.iter().take(3).map(|b| b.digest()).collect();

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let result = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;
        assert!(matches!(result, Err(NetworkError::ProtocolError(_))));
    }

    #[tokio::test]
    async fn test_validate_rejects_unexpected_digest() {
        // encode batch A but request digest B
        let batches = create_test_batches(1);
        let bytes = encode_batches_to_stream_bytes(&batches).await;
        let mut cursor = Cursor::new(bytes);

        // request a different digest
        let fake_digest = B256::random();
        let digests: HashSet<BlockHash> = HashSet::from([fake_digest]);

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let result = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;
        assert!(matches!(result, Err(NetworkError::ProtocolError(_))));
    }

    #[tokio::test]
    async fn test_validate_rejects_duplicate_batch() {
        // manually encode the same batch twice
        let batch = Batch { transactions: vec![vec![42u8; 32]], ..Default::default() };
        let max_size = max_batch_size(0);
        let mut output = Vec::new();
        let mut encode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        // chunk count = 2
        output.extend_from_slice(&2u32.to_le_bytes());
        // write same batch twice
        write_batch(&mut output, &batch, &mut encode_buffer, &mut compressed_buffer).await.unwrap();
        write_batch(&mut output, &batch, &mut encode_buffer, &mut compressed_buffer).await.unwrap();

        let mut cursor = Cursor::new(output);

        // request set includes the batch digest (and count=2 passes the count check)
        let digest = batch.digest();
        let another_digest = B256::random();
        let digests: HashSet<BlockHash> = HashSet::from([digest, another_digest]);

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let result = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;
        assert!(matches!(result, Err(NetworkError::ProtocolError(_))));
    }

    #[tokio::test]
    async fn test_validate_detects_stream_closed() {
        // encode chunk count of 2 but write 0 batches
        let mut output = Vec::new();
        output.extend_from_slice(&2u32.to_le_bytes());
        // no batch data follows

        let mut cursor = Cursor::new(output);

        let digests: HashSet<BlockHash> = HashSet::from([B256::random(), B256::random()]);

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());

        let result = handle.read_and_validate_batches_with_timeout(&mut cursor, &digests).await;
        // should fail reading the first batch (stream too short)
        assert!(result.is_err());
    }

    // ============================================================================
    // BatchFetcher Local-Only Tests
    // ============================================================================

    #[tokio::test]
    async fn test_fetch_for_primary_all_local() {
        let batches = create_test_batches(3);
        let db = setup_batch_db(&batches);
        let digests: HashSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fetcher = BatchFetcher::new(handle, db);

        let result = fetcher.fetch_for_primary(digests.clone()).await.expect("fetch local batches");
        assert_eq!(result.len(), batches.len());
        for batch in &batches {
            assert!(result.contains_key(&batch.digest()));
        }
    }

    #[tokio::test]
    async fn test_fetch_for_primary_all_local_many() {
        let batches = create_test_batches(20);
        let db = setup_batch_db(&batches);
        let digests: HashSet<BlockHash> = batches.iter().map(|b| b.digest()).collect();

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fetcher = BatchFetcher::new(handle, db);

        let result = fetcher.fetch_for_primary(digests.clone()).await.expect("fetch local batches");
        assert_eq!(result.len(), 20);
        for batch in &batches {
            assert!(result.contains_key(&batch.digest()));
        }
    }

    // ============================================================================
    // BatchFetcher Partial Local + Stream Tests
    // ============================================================================

    /// Test the fetch_for_primary component flow where 1 of 5 batches is local
    /// and the remaining 4 are received via stream.
    ///
    /// Since `fetch_for_primary` requires a real libp2p network for the stream path,
    /// this test exercises the same internal code paths directly:
    /// 1. `fetch_local` retrieves the 1 local batch and identifies 4 missing
    /// 2. `read_and_validate_batches_with_timeout` decodes the 4 remote batches from a stream
    /// 3. The combined result contains all 5 batches
    #[tokio::test]
    async fn test_fetch_for_primary_partial_local_with_stream() {
        let all_batches = create_test_batches(5);
        // only store the first batch locally
        let db = setup_batch_db(&all_batches[..1]);
        let all_digests: HashSet<BlockHash> = all_batches.iter().map(|b| b.digest()).collect();

        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let fetcher = BatchFetcher::new(handle.clone(), db);

        // step 1: fetch_local finds 1 batch, leaves 4 missing
        let mut missing_digests = all_digests.clone();
        let mut fetched_batches = HashMap::new();
        fetcher.fetch_local(&mut missing_digests, &mut fetched_batches).unwrap();

        assert_eq!(fetched_batches.len(), 1);
        assert_eq!(missing_digests.len(), 4);
        assert!(fetched_batches.contains_key(&all_batches[0].digest()));

        // step 2: simulate streaming the 4 remaining batches from a peer
        let remote_batches: Vec<_> = all_batches[1..].to_vec();
        let bytes = encode_batches_to_stream_bytes(&remote_batches).await;
        let mut cursor = Cursor::new(bytes);

        let streamed = handle
            .read_and_validate_batches_with_timeout(&mut cursor, &missing_digests)
            .await
            .expect("should read batches from stream");

        assert_eq!(streamed.len(), 4);

        // step 3: combine local + streamed (mirrors fetch_for_primary's combine logic)
        for (digest, batch) in streamed {
            missing_digests.remove(&digest);
            fetched_batches.insert(digest, batch);
        }

        // all 5 batches recovered
        assert!(missing_digests.is_empty());
        assert_eq!(fetched_batches.len(), 5);
        for batch in &all_batches {
            assert!(fetched_batches.contains_key(&batch.digest()));
        }
    }
}
