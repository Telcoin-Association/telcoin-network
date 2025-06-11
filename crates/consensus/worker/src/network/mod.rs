//! Worker network implementation.

use crate::batch_fetcher::BatchFetcher;
use error::WorkerNetworkError;
use futures::{stream::FuturesUnordered, StreamExt};
use handler::RequestHandler;
use message::{WorkerGossip, WorkerRPCError};
pub use message::{WorkerRequest, WorkerResponse};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    error::NetworkError,
    types::{NetworkEvent, NetworkHandle, NetworkResult},
    GossipMessage, Multiaddr, PeerExchangeMap, PeerId, Penalty, ResponseChannel,
};
use tn_network_types::{FetchBatchResponse, PrimaryToWorkerClient, WorkerSynchronizeMessage};
use tn_storage::tables::Batches;
use tn_types::{
    encode, now, Batch, BatchValidation, BlockHash, Database, DbTxMut, SealedBatch, TaskManager,
    TaskSpawner, WorkerId,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, trace, warn};

mod error;
mod handler;
pub(crate) mod message;

/// Convenience type for Primary network.
pub(crate) type Req = WorkerRequest;
/// Convenience type for Primary network.
pub(crate) type Res = WorkerResponse;

/// The wrapper around worker-specific network calls.
#[derive(Clone, Debug)]
pub struct WorkerNetworkHandle {
    /// The handle to the node's network.
    handle: NetworkHandle<Req, Res>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
}

impl WorkerNetworkHandle {
    /// Create a new instance of [Self].
    pub fn new(handle: NetworkHandle<Req, Res>, task_spawner: TaskSpawner) -> Self {
        Self { handle, task_spawner }
    }

    /// Return a reference to the task spawner.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.task_spawner
    }

    //// Convenience method for creating a new Self for tests- sends events no-where and does
    //// nothing.
    pub fn new_for_test() -> Self {
        let (tx, _rx) = mpsc::channel(5);
        let tm = TaskManager::default();
        Self { handle: NetworkHandle::new(tx), task_spawner: tm.get_spawner() }
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<WorkerRequest, WorkerResponse> {
        &self.handle
    }

    /// Dial a peer.
    ///
    /// Return swarm error to caller.
    pub async fn dial(&self, peer_id: PeerId, peer_addr: Multiaddr) -> NetworkResult<()> {
        self.handle.dial(peer_id, peer_addr).await
    }

    /// Publish a batch digest to the worker network.
    pub async fn publish_batch(&self, batch_digest: BlockHash) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Batch(batch_digest));
        self.handle.publish("tn-worker".into(), data).await?;
        Ok(())
    }

    /// Publish a transaction (as raw bytes) worker network.
    /// Do this when not a committee member so a CVV can include the txn.
    pub async fn publish_txn(&self, txn: Vec<u8>) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Txn(txn));
        self.handle.publish("tn-txn".into(), data).await?;
        Ok(())
    }

    /// Report a new batch to a peer.
    async fn report_batch(&self, peer_id: PeerId, sealed_batch: SealedBatch) -> NetworkResult<()> {
        // TODO- issue 237- should we sign these batches and check the sig before accepting any
        // batches during consensus?
        let request = WorkerRequest::ReportBatch { sealed_batch };
        let res = self.handle.send_request(request, peer_id).await?;
        let res = res.await??;
        match res {
            WorkerResponse::ReportBatch => Ok(()),
            WorkerResponse::RequestBatches { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is request batches!".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is peer exchange!".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Report a new batch to peers.
    pub fn report_batch_to_peers(
        &self,
        peer_ids: Vec<PeerId>,
        sealed_batch: SealedBatch,
    ) -> Vec<oneshot::Receiver<NetworkResult<()>>> {
        let mut result = vec![];
        for peer_id in peer_ids {
            let handle = self.clone();
            let batch = sealed_batch.clone();
            let task_name = format!("ReportBatchToPeer-{peer_id}");
            let (tx, rx) = oneshot::channel();
            self.task_spawner.spawn_task(task_name, async move {
                let res = handle.report_batch(peer_id, batch).await;
                // ignore error bc quorum waiter will move on once quorum is reached
                let _ = tx.send(res);
            });

            result.push(rx);
        }
        result
    }

    /// Request a group of batches by hashes.
    async fn request_batches_from_peer(
        &self,
        peer_id: PeerId,
        batch_digests: Vec<BlockHash>,
        timeout: Duration,
    ) -> NetworkResult<Vec<Batch>> {
        let request = WorkerRequest::RequestBatches { batch_digests: batch_digests.clone() };
        let res = self.handle.send_request(request, peer_id).await?;
        let res =
            tokio::time::timeout(timeout, res).await.map_err(|_| NetworkError::Timeout)???;
        match res {
            WorkerResponse::ReportBatch => Err(NetworkError::RPCError(
                "Got wrong response, not a request batches is report batch!".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a request batches is peer exchange!".to_string(),
            )),
            WorkerResponse::RequestBatches(batches) => {
                for batch in &batches {
                    let batch_digest = batch.digest();
                    if !batch_digests.contains(&batch_digest) {
                        let msg = format!(
                            "Peer {peer_id} returned batch with digest \
                            {batch_digest} which is not part of the requested digests: {batch_digests:?}"
                        );
                        return Err(NetworkError::ProtocolError(msg));
                    }
                }
                Ok(batches)
            }
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Request a group of batches by hashes.
    /// Sends request to all our connected peers at once and returns Ok when we
    /// get a valid response or Err if no one responds with the batches.
    pub async fn request_batches(
        &self,
        requested_digests: Vec<BlockHash>,
    ) -> NetworkResult<Vec<Batch>> {
        let mut peers = self.handle.connected_peers().await?;
        if requested_digests.is_empty() || peers.is_empty() {
            // Nothing to do, either no digests requested or no one to ask.
            // Return nothing.
            return Ok(vec![]);
        }
        let mut remaining_digests = requested_digests.clone();
        let num_peers = peers.len();
        let mut all_batches = Vec::new();
        // Attempt to try different batches with different peers.
        // Ideally this will work first time and spread out the network traffic.
        // It is possible for this algorithm to send same batches to the same peer,
        // it is not that precise but should mix up things sufficiently to get batches
        // if peers have them.
        for _ in 0..num_peers {
            let mut batch_of_batches = Vec::with_capacity(num_peers);
            (0..num_peers).for_each(|_| batch_of_batches.push(vec![]));
            peers.rotate_left(1); // Change which peers we ask for which batches.
            for (i, batch) in remaining_digests.iter().enumerate() {
                batch_of_batches
                    .get_mut(i % num_peers)
                    .expect("missing index we just created!")
                    .push(*batch);
            }
            let mut futures = FuturesUnordered::new();
            for (peer, batch_digests) in peers.iter().zip(batch_of_batches.into_iter()) {
                if !batch_digests.is_empty() {
                    futures.push(self.request_batches_from_peer(
                        *peer,
                        batch_digests,
                        Duration::from_secs(3),
                    ));
                }
            }
            while let Some(res) = futures.next().await {
                match res {
                    Ok(batches) => {
                        for batch in batches {
                            let batch_digest = batch.digest();
                            if requested_digests.contains(&batch_digest) {
                                // Sanity check we actually asked for this digest...
                                if !all_batches.contains(&batch) {
                                    remaining_digests.retain(|d| *d != batch_digest);
                                    all_batches.push(batch);
                                }
                            } else {
                                // Got a batch we did not ask for...
                                warn!(target: "worker::network", "recieved a batch not requested {batch_digest}");
                            }
                        }
                        if remaining_digests.is_empty() {
                            return Ok(all_batches);
                        }
                    }
                    Err(e) => {
                        // Another worker might succeed so just log this.
                        warn!(target: "worker::network", ?e, "error requesting batches");
                    }
                }
            }
        }
        if all_batches.is_empty() {
            Err(NetworkError::RPCError("Unable to get batches from any peers!".to_string()))
        } else {
            Ok(all_batches)
        }
    }

    /// Report penalty to peer manager.
    pub(crate) async fn report_penalty(&self, peer_id: PeerId, penalty: Penalty) {
        self.handle.report_penalty(peer_id, penalty).await;
    }

    /// Notify peer manager of peer exchange information.
    pub(crate) async fn process_peer_exchange(
        &self,
        peers: PeerExchangeMap,
        channel: ResponseChannel<WorkerResponse>,
    ) {
        let _ = self.handle.process_peer_exchange(peers, channel).await;
    }

    /// Retrieve a collection of connected peers.
    pub async fn connected_peers(&self) -> NetworkResult<Vec<PeerId>> {
        self.handle.connected_peers().await
    }

    /// Update the task spawner at the epoch boundary.
    pub fn update_task_spawner(&mut self, task_spawner: TaskSpawner) {
        self.task_spawner = task_spawner
    }
}

/// Handle inter-node communication between primaries.
pub struct WorkerNetwork<DB> {
    /// Receiver for network events.
    network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
    /// Network handle to send commands.
    network_handle: WorkerNetworkHandle,
    // Request handler to process requests and return responses.
    request_handler: RequestHandler<DB>,
}

impl<DB> WorkerNetwork<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        network_events: mpsc::Receiver<NetworkEvent<Req, Res>>,
        network_handle: WorkerNetworkHandle,
        consensus_config: ConsensusConfig<DB>,
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
    ) -> Self {
        let request_handler =
            RequestHandler::new(id, validator, consensus_config, network_handle.clone());
        Self { network_events, network_handle, request_handler }
    }

    /// Run the network for the epoch.
    pub fn spawn(mut self, epoch_task_spawner: &TaskSpawner) {
        epoch_task_spawner.spawn_task("worker network events", async move {
            loop {
                while let Some(event) = self.network_events.recv().await {
                    self.process_network_event(event);
                }
            }
        });
    }

    /// Handle events concurrently.
    fn process_network_event(&self, event: NetworkEvent<Req, Res>) {
        // match event
        match event {
            NetworkEvent::Request { peer, request, channel, cancel } => match request {
                WorkerRequest::ReportBatch { sealed_batch } => {
                    self.process_report_batch(peer, sealed_batch, channel, cancel);
                }
                WorkerRequest::RequestBatches { batch_digests } => {
                    self.process_request_batches(peer, batch_digests, channel, cancel);
                }
                WorkerRequest::PeerExchange { peers } => {
                    // notify peer manager
                    self.process_peer_exchange(peers, channel);
                }
            },
            NetworkEvent::Gossip(msg, source) => {
                self.process_gossip(msg, source);
            }
        }
    }

    /// Process a new reported batch.
    ///
    /// Spawn a task to evaluate a peer's proposed header and return a response.
    fn process_report_batch(
        &self,
        _peer: PeerId,
        sealed_batch: SealedBatch,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-report-batch-{}", sealed_batch.digest());
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_report_batch(sealed_batch) => {
                    let response = match res {
                        Ok(()) => WorkerResponse::ReportBatch,
                        Err(err) => WorkerResponse::Error(message::WorkerRPCError(err.to_string())),
                    };
                    let _ = network_handle.handle.send_response(response, channel).await;
                },
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Attempt to return requested batches.
    fn process_request_batches(
        &self,
        peer: PeerId,
        batch_digests: Vec<BlockHash>,
        channel: ResponseChannel<WorkerResponse>,
        cancel: oneshot::Receiver<()>,
    ) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-request-batches-{peer}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            tokio::select! {
                res = request_handler.process_request_batches(batch_digests) => {
                    let response = match res {
                        Ok(r) => WorkerResponse::RequestBatches(r),
                        Err(err) => WorkerResponse::Error(message::WorkerRPCError(err.to_string())),
                    };

                    // TODO: penalize peer's reputation for bad request

                    let _ = network_handle.handle.send_response(response, channel).await;
                }
                // cancel notification from network layer
                _ = cancel => (),
            }
        });
    }

    /// Process gossip from a worker.
    fn process_gossip(&self, msg: GossipMessage, source: PeerId) {
        // clone for spawned tasks
        let request_handler = self.request_handler.clone();
        let network_handle = self.network_handle.clone();
        let task_name = format!("process-gossip-{source}");
        self.network_handle.get_task_spawner().spawn_task(task_name, async move {
            if let Err(e) = request_handler.process_gossip(&msg).await {
                warn!(target: "worker::network", ?e, "process_gossip");
                if let Err(e) = request_handler.process_gossip(&msg).await {
                    warn!(target: "worker::network", ?e, "process_gossip");
                    // convert error into penalty to lower peer score
                    if let Some(penalty) = e.into() {
                        network_handle.report_penalty(source, penalty).await;
                    }
                }
            }
        });
    }

    /// Process peer exchange.
    fn process_peer_exchange(
        &self,
        peers: PeerExchangeMap,
        channel: ResponseChannel<WorkerResponse>,
    ) {
        let network_handle = self.network_handle.clone();

        // notify peer manager and respond with ack
        self.network_handle.get_task_spawner().spawn_task("process-peer-exchange", async move {
            network_handle.process_peer_exchange(peers, channel).await;
        });
    }
}

/// Defines how the network receiver handles incoming primary messages.
pub struct PrimaryReceiverHandler<DB> {
    /// The batch store
    pub store: DB,
    /// Timeout on RequestBatches RPC.
    pub request_batches_timeout: Duration,
    /// Synchronize header payloads from other workers.
    pub network: Option<WorkerNetworkHandle>,
    /// Fetch certificate payloads from other workers.
    pub batch_fetcher: Option<BatchFetcher<DB>>,
    /// Validate incoming batches
    pub validator: Arc<dyn BatchValidation>,
}

#[async_trait::async_trait]
impl<DB: Database> PrimaryToWorkerClient for PrimaryReceiverHandler<DB> {
    async fn synchronize(&self, message: WorkerSynchronizeMessage) -> eyre::Result<()> {
        let Some(network) = self.network.as_ref() else {
            return Err(eyre::eyre!(
                "synchronize() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let mut missing = HashSet::new();
        for digest in message.digests.iter() {
            // Check if we already have the batch.
            match self.store.get::<Batches>(digest) {
                Ok(None) => {
                    missing.insert(*digest);
                    debug!("Requesting sync for batch {digest}");
                }
                Ok(Some(_)) => {
                    trace!("Digest {digest} already in store, nothing to sync");
                }
                Err(e) => {
                    return Err(eyre::eyre!("failed to read from batch store: {e:?}"));
                }
            };
        }
        if missing.is_empty() {
            return Ok(());
        }

        let response = tokio::time::timeout(
            self.request_batches_timeout,
            network.request_batches(missing.iter().cloned().collect()),
        )
        .await??;

        let sealed_batches_from_response: Vec<SealedBatch> =
            response.into_iter().map(|b| b.seal_slow()).collect();

        for sealed_batch in sealed_batches_from_response.into_iter() {
            if !message.is_certified {
                // This batch is not part of a certificate, so we need to validate it.
                if let Err(err) = self.validator.validate_batch(sealed_batch.clone()) {
                    return Err(eyre::eyre!("Invalid batch: {err}"));
                }
            }

            let (mut batch, digest) = sealed_batch.split();
            if missing.remove(&digest) {
                // Set received_at timestamp for remote batch.
                batch.set_received_at(now());
                let mut tx = self.store.write_txn().map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to create batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.insert::<Batches>(&digest, &batch).map_err(|e| {
                    WorkerNetworkError::Internal(format!(
                        "failed to batch transaction to commit: {e:?}"
                    ))
                })?;
                tx.commit().map_err(|e| {
                    WorkerNetworkError::Internal(format!("failed to commit batch: {e:?}"))
                })?;
            } else {
                return Err(eyre::eyre!(format!(
                    "failed to synchronize batches- received a batch {digest} we did not request!"
                )));
            }
        }

        if missing.is_empty() {
            return Ok(());
        }
        Err(eyre::eyre!("failed to synchronize batches!".to_string()))
    }

    async fn fetch_batches(&self, digests: HashSet<BlockHash>) -> eyre::Result<FetchBatchResponse> {
        let Some(batch_fetcher) = self.batch_fetcher.as_ref() else {
            return Err(eyre::eyre!(
                "fetch_batches() is unsupported via RPC interface, please call via local worker handler instead".to_string(),
            ));
        };
        let batches = batch_fetcher.fetch(digests).await;
        Ok(FetchBatchResponse { batches })
    }
}
