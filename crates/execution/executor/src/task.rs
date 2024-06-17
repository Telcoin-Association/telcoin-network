use crate::Storage;
use futures::StreamExt;
use futures_util::{future::BoxFuture, FutureExt};
use reth_beacon_consensus::{BeaconEngineMessage, ForkchoiceStatus};
use reth_evm::execute::BlockExecutorProvider;
use reth_node_api::EngineTypes;
use reth_primitives::{ChainSpec, Withdrawals};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotificationSender, Chain, StateProviderFactory,
};
use reth_rpc_types::engine::ForkchoiceState;
use reth_stages::PipelineEvent;
use reth_tokio_util::EventStream;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::ConsensusOutput;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, error, warn};

/// A Future that listens for new ready transactions and puts new blocks into storage
pub struct MiningTask<Client, Engine: EngineTypes, BlockExecutor> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    /// Shared storage to insert new blocks
    storage: Storage,
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// TODO: ideally this would just be a sender of hashes
    to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
    /// Used to notify consumers of new blocks
    canon_state_notification: CanonStateNotificationSender,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// The type used for block execution
    block_executor: BlockExecutor,
}

// === impl MiningTask ===

impl<Client, Engine, BlockExecutor> MiningTask<Client, Engine, BlockExecutor>
where
    Engine: EngineTypes,
{
    /// Creates a new instance of the task
    pub(crate) fn new(
        chain_spec: Arc<ChainSpec>,
        to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
        canon_state_notification: CanonStateNotificationSender,
        storage: Storage,
        client: Client,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        block_executor: BlockExecutor,
    ) -> Self {
        Self {
            chain_spec,
            client,
            insert_task: None,
            storage,
            to_engine,
            canon_state_notification,
            queued: Default::default(),
            pipeline_events: None,
            consensus_output_stream,
            block_executor,
        }
    }

    /// Sets the pipeline events to listen on.
    pub fn set_pipeline_events(&mut self, events: EventStream<PipelineEvent>) {
        self.pipeline_events = Some(events);
    }
}

impl<Client, Engine, BlockExecutor> Future for MiningTask<Client, Engine, BlockExecutor>
where
    Client: StateProviderFactory + CanonChainTracker + BlockReaderIdExt + Clone + Unpin + 'static,
    Engine: EngineTypes + 'static,
    BlockExecutor: BlockExecutorProvider,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // this executes output from consensus
        loop {
            // check if output is available from consensus
            match this.consensus_output_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(output))) => {
                    // queue the output for local execution
                    this.queued.push_back(output)
                }
                Poll::Ready(Some(Err(e))) => {
                    error!(target: "execution::executor", ?e, "for consensus output stream");
                }
                Poll::Ready(None) => {
                    // stream has ended
                    return Poll::Ready(());
                }
                Poll::Pending => { /* nothing to do */ }
            }

            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to queue in new insert task
                let storage = this.storage.clone();
                let output = this.queued.pop_front().expect("not empty");

                let to_engine = this.to_engine.clone();
                let client = this.client.clone();
                let chain_spec = Arc::clone(&this.chain_spec);
                let events = this.pipeline_events.take();
                let canon_state_notification = this.canon_state_notification.clone();
                let block_executor = this.block_executor.clone();

                // Create the mining future that creates a block, notifies the engine that drives
                // the pipeline
                this.insert_task = Some(Box::pin(async move {
                    let mut storage = storage.write().await;

                    // TODO: support withdrawals
                    let withdrawals = Some(Withdrawals::default());

                    match storage.build_and_execute(
                        output,
                        withdrawals,
                        &client,
                        chain_spec,
                        &block_executor,
                    ) {
                        Ok((sealed_block_with_senders, bundle_state)) => {
                            // send sealed forkchoice update to engine
                            let new_header_hash = sealed_block_with_senders.hash();

                            let state = ForkchoiceState {
                                head_block_hash: new_header_hash,
                                finalized_block_hash: new_header_hash,
                                safe_block_hash: new_header_hash,
                            };

                            drop(storage);

                            // TODO: make this a future
                            // await the fcu call rx for SYNCING, then wait for a VALID response
                            loop {
                                // send the new update to the engine, this will trigger the engine
                                // to download and execute the block we just inserted
                                let (tx, rx) = oneshot::channel();
                                let _ = to_engine.send(BeaconEngineMessage::ForkchoiceUpdated {
                                    state,
                                    payload_attrs: None,
                                    tx,
                                });

                                debug!(target: "execution::executor", ?state, "Sent forkchoice update");

                                match rx.await.unwrap() {
                                    Ok(fcu_response) => {
                                        debug!(target: "execution::executor", ?fcu_response);
                                        match fcu_response.forkchoice_status() {
                                            ForkchoiceStatus::Valid => break,
                                            ForkchoiceStatus::Invalid => {
                                                error!(target: "execution::executor", ?fcu_response, "Forkchoice update returned invalid response");
                                                return None;
                                            }
                                            ForkchoiceStatus::Syncing => {
                                                debug!(target: "execution::executor", ?fcu_response, "Forkchoice update returned SYNCING, waiting for VALID");
                                                // wait for the next fork choice update
                                                continue;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!(target: "execution::executor", ?err, "Autoseal fork choice update failed");
                                        return None;
                                    }
                                }
                            }

                            // update canon chain for rpc
                            let new_header = sealed_block_with_senders.header.clone();
                            client.set_canonical_head(new_header.clone());
                            client.set_safe(new_header.clone());
                            client.set_finalized(new_header);

                            debug!(target: "execution::executor", header=?sealed_block_with_senders.hash(), "sending block notification");

                            let chain = Arc::new(Chain::new(
                                vec![sealed_block_with_senders],
                                bundle_state,
                                None,
                            ));

                            // send block notification
                            let _ = canon_state_notification
                                .send(reth_provider::CanonStateNotification::Commit { new: chain });
                        }
                        Err(err) => {
                            warn!(target: "execution::executor", ?err, "failed to execute block")
                        }
                    }

                    events
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(events) => {
                        warn!("problem in Poll::Ready??");
                        this.pipeline_events = events;
                    }
                    Poll::Pending => {
                        this.insert_task = Some(fut);
                        break;
                    }
                }
            }
        }

        Poll::Pending
    }
}

impl<Client, Engine, EvmConfig> std::fmt::Debug for MiningTask<Client, Engine, EvmConfig>
where
    Engine: EngineTypes,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiningTask").finish_non_exhaustive()
    }
}
