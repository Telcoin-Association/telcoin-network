use crate::{mode::MiningMode, pool::maintain::changed_accounts_iter, Storage};
use consensus_metrics::metered_channel::Sender;
use futures_util::{future::BoxFuture, FutureExt, StreamExt};
use reth_blockchain_tree::{BlockValidationKind, BlockchainTreeEngine};
use reth_chainspec::ChainSpec;
use reth_evm::execute::BlockExecutorProvider;
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, IntoRecoveredTransaction, SealedBlock, Withdrawals,
};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotification, CanonStateSubscriptions,
    StateProviderFactory,
};
use reth_stages::PipelineEvent;
use reth_transaction_pool::{
    CanonicalStateUpdate, TransactionPool, TransactionPoolExt, ValidPoolTransaction,
};
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
pub struct MiningTask<Provider, Pool: TransactionPool, BlockExecutor> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The provider used to interact with the state
    provider: Provider,
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
    /// The worker's current base fee.
    base_fee: Option<u64>,
}

// === impl MiningTask ===

impl<Provider, Pool: TransactionPool, BlockExecutor> MiningTask<Provider, Pool, BlockExecutor> {
    /// Creates a new instance of the task
    pub(crate) fn new(
        chain_spec: Arc<ChainSpec>,
        miner: MiningMode,
        to_worker: Sender<NewBatch>,
        // canon_state_notification: CanonStateNotificationSender,
        storage: Storage,
        provider: Provider,
        pool: Pool,
        block_executor: BlockExecutor,
    ) -> Self {
        Self {
            chain_spec,
            provider,
            miner,
            insert_task: None,
            storage,
            pool,
            to_worker,
            // canon_state_notification,
            queued: Default::default(),
            pipe_line_events: None,
            block_executor,
            base_fee: None,
        }
    }

    /// Sets the pipeline events to listen on.
    pub fn set_pipeline_events(&mut self, events: UnboundedReceiverStream<PipelineEvent>) {
        self.pipe_line_events = Some(events);
    }
}

impl<BlockExecutor, Provider, Pool> Future for MiningTask<Provider, Pool, BlockExecutor>
where
    BlockExecutor: BlockExecutorProvider,
    Provider: StateProviderFactory
        + CanonChainTracker
        + CanonStateSubscriptions
        + BlockReaderIdExt
        + BlockchainTreeEngine
        + Clone
        + Unpin
        + 'static,
    Pool: TransactionPool + TransactionPoolExt + Unpin + 'static,
    <Pool as TransactionPool>::Transaction: IntoRecoveredTransaction,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut canon_updates = self.provider.canonical_state_stream();
        let this = self.get_mut();

        // loop to poll the tx miner and send the next batch to Worker's `BatchMaker`
        loop {
            // check for canon updates before mining the transaction pool
            while let Poll::Ready(Some(canon_update)) = canon_updates.poll_next_unpin(cx) {
                // poll canon updates stream and update pool `.on_canon_update`
                //
                // maintenance task will handle worker's pending block update
                match canon_update {
                    CanonStateNotification::Commit { new } => {
                        // TODO: ensure the engine's update includes all accounts that changed during execution
                        warn!(target: "Canon update inside batch maker!!!!\n", ?new);
                        // update pool based with canonical tip update
                        let (blocks, state) = new.inner();
                        let tip = blocks.tip();

                        let mut changed_accounts = Vec::with_capacity(state.state().len());
                        for acc in changed_accounts_iter(state) {
                            changed_accounts.push(acc);
                        }

                        // TODO: these should have already been mined when the worker proposed its block
                        let mined_transactions = blocks.transaction_hashes().collect();

                        let pending_block_base_fee = this.base_fee.unwrap_or(MIN_PROTOCOL_BASE_FEE);

                        // Canonical update
                        let update = CanonicalStateUpdate {
                            new_tip: &tip.block,          // finalized block
                            pending_block_base_fee,       // current base fee for worker
                            pending_block_blob_fee: None, // current base fee for worker
                            changed_accounts,             // finalized block
                            mined_transactions, // finalized block (but these should be a noop)
                        };

                        // sync fn so this will block until all pool updates are complete
                        this.pool.on_canonical_state_change(update);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
                }
            }

            // poll pool for pending transactions and add to queue
            if let Poll::Ready(transactions) = this.miner.poll(&this.pool, cx) {
                // miner returned a set of transaction that we feed to the producer
                this.queued.push_back(transactions);
            }

            // only build one block at a time
            if this.insert_task.is_none() {
                // check queue
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to start next insert task
                let storage = this.storage.clone();
                let transactions = this.queued.pop_front().expect("not empty");
                let to_worker = this.to_worker.clone();
                let provider = this.provider.clone();
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
                        &provider,
                        chain_spec,
                        &block_executor,
                    ) {
                        Ok((new_header, body, _bundle_state)) => {
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
                                        new_header.clone().into(),
                                    ),
                                    ack,
                                })
                                .await;

                            match rx.await {
                                Ok(digest) => {
                                    debug!(target: "execution::batch_maker", ?digest, "Batch sealed:");
                                    // create sealed block with header and transactions
                                    // use defaults for ommers, withdrawals, and
                                    let sealed_block = SealedBlock::new(new_header, body);

                                    // trying to insert the pending block here will get
                                    // removed when the next round of consensus is executed
                                    // because finalize_block removes forks
                                    //
                                    // TODO: insert block WITH senders since these were just used
                                    match provider.insert_block_without_senders(
                                        sealed_block,
                                        BlockValidationKind::SkipStateRootValidation,
                                    ) {
                                        Ok(res) => {
                                            debug!(target: "execution::batch_maker", ?res, "insert own batch OK")
                                        }
                                        Err(err) => {
                                            error!(target: "execution::batch_maker", ?err, "failed to add own worker block to tree:")
                                        }
                                    }

                                    // TODO: send WorkerBlockUpdate through channel for maintenance task
                                    //
                                    //
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
                            // pool.remove_transactions(
                            //     transactions.iter().map(|tx| tx.hash()).collect(),
                            // );

                            // // the pool should update in two ways:
                            // // - canon update should update the pool
                            // // - the worker's proposed pending block should update

                            // // // update pool
                            // pool.on_canonical_state_change(CanonicalStateUpdate {
                            //     new_tip: new_header,
                            //     pending_block_base_fee: todo!(),
                            //     pending_block_blob_fee: todo!(),
                            //     changed_accounts: todo!(),
                            //     mined_transactions: todo!(),
                            // });

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

impl<EvmConfig, Provider, Pool: TransactionPool> std::fmt::Debug
    for MiningTask<Provider, Pool, EvmConfig>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiningTask").finish_non_exhaustive()
    }
}
