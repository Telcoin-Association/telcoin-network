use crate::{mode::MiningMode, Storage};
use consensus_metrics::metered_channel::Sender;
use futures_util::{future::BoxFuture, FutureExt};
use reth_evm::execute::BlockExecutorProvider;
use reth_primitives::{ChainSpec, IntoRecoveredTransaction, Withdrawals};
use reth_provider::{BlockReaderIdExt, CanonChainTracker, StateProviderFactory};
use reth_stages::PipelineEvent;
use reth_transaction_pool::{TransactionPool, ValidPoolTransaction};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{Batch, NewBatch};
use tokio::sync::oneshot;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error, warn};

/// A Future that listens for new ready transactions and puts new blocks into storage
pub struct MiningTask<Client, Pool: TransactionPool, BlockExecutor> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// The active miner
    miner: MiningMode,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Option<UnboundedReceiverStream<PipelineEvent>>>>,
    /// Shared storage to insert new blocks
    storage: Storage,
    /// Pool where transactions are stored
    pool: Pool,
    /// backlog of sets of transactions ready to be mined
    queued: VecDeque<Vec<Arc<ValidPoolTransaction<<Pool as TransactionPool>::Transaction>>>>,
    /// Sending half of channel to worker.
    ///
    /// Worker recieves batch and forwards to `quorum_waiter`.
    to_worker: Sender<NewBatch>,
    // /// Used to notify consumers of new blocks
    // ///
    // /// TODO: can this be used anywhere else?
    // canon_state_notification: CanonStateNotificationSender,
    /// The pipeline events to listen on
    pipe_line_events: Option<UnboundedReceiverStream<PipelineEvent>>,
    /// The type used for block execution
    block_executor: BlockExecutor,
}

// === impl MiningTask ===

impl<Client, Pool: TransactionPool, BlockExecutor> MiningTask<Client, Pool, BlockExecutor> {
    /// Creates a new instance of the task
    pub(crate) fn new(
        chain_spec: Arc<ChainSpec>,
        miner: MiningMode,
        to_worker: Sender<NewBatch>,
        // canon_state_notification: CanonStateNotificationSender,
        storage: Storage,
        client: Client,
        pool: Pool,
        block_executor: BlockExecutor,
    ) -> Self {
        Self {
            chain_spec,
            client,
            miner,
            insert_task: None,
            storage,
            pool,
            to_worker,
            // canon_state_notification,
            queued: Default::default(),
            pipe_line_events: None,
            block_executor,
        }
    }

    /// Sets the pipeline events to listen on.
    pub fn set_pipeline_events(&mut self, events: UnboundedReceiverStream<PipelineEvent>) {
        self.pipe_line_events = Some(events);
    }
}

impl<BlockExecutor, Client, Pool> Future for MiningTask<Client, Pool, BlockExecutor>
where
    BlockExecutor: BlockExecutorProvider,
    Client: StateProviderFactory + CanonChainTracker + BlockReaderIdExt + Clone + Unpin + 'static,
    Pool: TransactionPool + Unpin + 'static,
    <Pool as TransactionPool>::Transaction: IntoRecoveredTransaction,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // loop to poll the tx miner and send the next batch to Worker's `BatchMaker`
        loop {
            if let Poll::Ready(transactions) = this.miner.poll(&this.pool, cx) {
                // miner returned a set of transaction that we feed to the producer
                this.queued.push_back(transactions);
            }

            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to queue in new insert task
                let storage = this.storage.clone();
                let transactions = this.queued.pop_front().expect("not empty");

                let to_worker = this.to_worker.clone();
                let client = this.client.clone();
                let chain_spec = Arc::clone(&this.chain_spec);
                let pool = this.pool.clone();
                let events = this.pipe_line_events.take();
                let block_executor = this.block_executor.clone();

                // Create the mining future that creates a batch and sends it to the CL
                this.insert_task = Some(Box::pin(async move {
                    let mut storage = storage.write().await;

                    let (transactions, tx_bytes): (Vec<_>, Vec<_>) = transactions
                        .into_iter()
                        .map(|tx| {
                            let signed = tx.to_recovered_transaction().into_signed();
                            // cast transaction into bytes as Vec<u8>
                            let tx_bytes = signed.envelope_encoded().into();
                            (signed, tx_bytes)
                        })
                        .unzip();

                    // TODO: support withdrawals
                    let withdrawals = Some(Withdrawals::default());

                    match storage.build_and_execute(
                        transactions.clone(),
                        withdrawals,
                        &client,
                        chain_spec,
                        &block_executor,
                    ) {
                        Ok((new_header, _bundle_state)) => {
                            // TODO: make this a future
                            //
                            // send the new update to the engine, this will trigger the engine
                            // to download and execute the block we just inserted
                            let (ack, rx) = oneshot::channel();
                            let _ = to_worker
                                .send(NewBatch {
                                    batch: Batch::new_with_metadata(
                                        // TODO: make batch `TransactionSigned` then convert to
                                        // bytes in `.digest` impl
                                        // NOTE: a `Batch` is a `SealedBlock`
                                        // convert txs to bytes
                                        tx_bytes,
                                        // versioned metadata for peer validation
                                        new_header.into(),
                                    ),
                                    ack,
                                })
                                .await;

                            match rx.await {
                                Ok(digest) => {
                                    debug!(target: "execution::batch_maker", ?digest, "Batch sealed:");
                                }
                                Err(err) => {
                                    error!(target: "execution::batch_maker", ?err, "Execution's BatchMaker Ack Failed:");
                                    return None;
                                }
                            }

                            // TODO: leaving this here in case `Batch` -> `SealedBlock`

                            // // seal the block
                            // let block = Block {
                            //     header: new_header.clone().unseal(),
                            //     body: transactions,
                            //     ommers: vec![],
                            //     withdrawals: None,
                            // };
                            // let sealed_block = block.seal_slow();

                            // let sealed_block_with_senders =
                            //     SealedBlockWithSenders::new(sealed_block, senders)
                            //         .expect("senders are valid");

                            // debug!(target: "execution::batch_maker",
                            // header=?sealed_block_with_senders.hash(), "sending block
                            // notification");

                            // let chain =
                            //     Arc::new(Chain::new(vec![sealed_block_with_senders],
                            // bundle_state));

                            // // send block notification
                            // let _ = canon_state_notification
                            //     .send(reth_provider::CanonStateNotification::Commit { new: chain
                            // });

                            // TODO: is this the best place to remove transactions?
                            // should the miner poll this like payload builder?

                            // TODO: this comment says dependent txs are also removed?
                            // might need to extend the trait onto another pool impl
                            //
                            // clear all transactions from pool once batch is sealed
                            pool.remove_transactions(
                                transactions.iter().map(|tx| tx.hash()).collect(),
                            );

                            drop(storage);
                        }
                        Err(err) => {
                            warn!(target: "execution::batch_maker", ?err, "failed to execute block")
                        }
                    }

                    events
                }));
            }

            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(events) => {
                        this.pipe_line_events = events;
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

impl<EvmConfig, Client, Pool: TransactionPool> std::fmt::Debug
    for MiningTask<Client, Pool, EvmConfig>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiningTask").finish_non_exhaustive()
    }
}
