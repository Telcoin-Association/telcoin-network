//! The worker's block maker monitors a transaction pool populated by incoming transactions through
//! the worker's RPC.
//!
//! The block maker is a future that

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub use block_builder::build_worker_block;
use block_builder::BlockBuilderOutput;
use consensus_metrics::metered_channel::Sender;
use error::BlockBuilderResult;
use futures_util::{FutureExt, StreamExt};
use pool::LastCanonicalUpdate;
use reth_blockchain_tree::{BlockchainTreeEngine, BlockchainTreeViewer};
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_primitives::{
    Address, BlockNumHash, IntoRecoveredTransaction, SealedHeader, TxHash, B256,
};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotification, CanonStateNotificationStream,
    CanonStateSubscriptions, Chain, ChainSpecProvider, FinalizedBlockReader, StateProviderFactory,
};
use reth_transaction_pool::{BlockInfo, CanonicalStateUpdate, TransactionPool, TransactionPoolExt};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{
    now, NewWorkerBlock, PendingBlockConfig, WorkerBlockBuilderArgs, WorkerBlockUpdateSender,
};
use tokio::sync::{broadcast, oneshot, watch};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

mod block_builder;
mod error;
mod pool;
pub use pool::{maintain_transaction_pool_future, PoolMaintenanceConfig};
#[cfg(feature = "test-utils")]
pub mod test_utils;

// blockchain provider
// tx pool
// consensus
// max round
// broadcast channel for sending WorkerBlocks after they're sealed
// canon state updates subscriber channel to receive
// basefee

// initial approach:
// - mine block when txpool pending tx notification received
//      - try to fill up entire block
//      - early network could be small blocks but faster than timer approach
//
// - impl Future for BlockProposer like Engine

/// Type alias for the blocking task that locks the tx pool and builds the next worker block.
type BlockBuildingTask = oneshot::Receiver<B256>;
/// Type alias for the blocking task that locks the tx pool and updates account state.
type PoolMaintenanceTask = oneshot::Receiver<B256>;

/// The type that builds blocks for workers to propose.
///
/// This is a future that:
/// - listens for canonical state changes and updates the tx pool
/// - polls the transaction pool for pending transactions
///     - tries to build the next worker block when there transactions are available
/// -
#[derive(Debug)]
pub struct BlockBuilder<BT, Pool, CE> {
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<BlockBuildingTask>,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// The transaction pool with pending transactions.
    pool: Pool,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    // /// Optional round of consensus to finish executing before then returning. The value is used
    // to /// track the subdag index from consensus output. The index is also considered the
    // "round" of /// consensus and is included in executed blocks as  the block's `nonce`
    // value. ///
    // /// NOTE: this is primarily useful for debugging and testing
    // max_round: Option<u64>,
    /// The sending side of broadcast channel when a worker has successfully proposed a new block.
    ///
    /// The RPC and transaction pool subsribe to these updates.
    worker_block_updates: WorkerBlockUpdateSender,
    /// The sending side to the worker's batch maker.
    ///
    /// Sending the new block through this channel triggers a broadcast to all peers.
    ///
    /// The worker's block maker sends an ack once the block has been stored in db
    /// which guarantees the worker will attempt to broadcast the new block until
    /// quorum is reached.
    to_worker: Sender<NewWorkerBlock>,
    /// The address for worker block's beneficiary.
    address: Address,
    /// Receiver stream for pending transactions in the pool.
    pending_tx_hashes_stream: ReceiverStream<TxHash>,
    /// The maximum amount of gas for a worker block.
    ///
    /// NOTE: transactions are not executed at this stage, so the worker measures the amount of gas
    /// specified by a transaction's gas limit.
    gas_limit: u64,
    /// The maximum size of collected transactions, measured in bytes.
    max_size: usize,
    /// Watch channel to ensure consistency between pool maintenance tasks.
    ///
    /// Pool maintenance happens when the engine executes a round of consensus. The block builder
    /// also applies pool updates when a worker block reaches quorum to remove mined transactions.
    latest_update: watch::Receiver<LastCanonicalUpdate>,
    /// Optional number of blocks to build before shutting down.
    ///
    /// Engine can produce multiple blocks per round of consensus, so this number may not
    /// match the subdag index or block height. To control the number of outputs, consider
    /// specifying a `max_round` for the execution engine as well.
    ///
    /// NOTE: this is only used for debugging and testing
    #[cfg(feature = "test-utils")]
    max_builds: Option<test_utils::MaxBuilds>,
}

impl<BT, Pool, CE> BlockBuilder<BT, Pool, CE>
where
    BT: CanonStateSubscriptions
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + StateProviderFactory
        + BlockchainTreeEngine
        + FinalizedBlockReader
        + CanonChainTracker
        + Clone
        + 'static,
    Pool: TransactionPoolExt + 'static,
    CE: ConfigureEvm + Clone,
{
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BT,
        pool: Pool,
        evm_config: CE,
        to_worker: Sender<NewWorkerBlock>,
        address: Address,
        pending_tx_hashes_stream: ReceiverStream<TxHash>,
        gas_limit: u64,
        max_size: usize,
        latest_update: watch::Receiver<LastCanonicalUpdate>,
        #[cfg(feature = "test-utils")] max_builds: Option<usize>,
    ) -> Self {
        // create broadcast channels
        // NOTE: it's important that worker updates are processed quickly
        let (worker_block_updates, _) = broadcast::channel(10);

        Self {
            pending_task: None,
            blockchain,
            pool,
            evm_config,
            worker_block_updates,
            to_worker,
            address,
            pending_tx_hashes_stream,
            gas_limit,
            max_size,
            latest_update,
            #[cfg(feature = "test-utils")]
            max_builds: max_builds.map(test_utils::MaxBuilds::new),
        }
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&self) -> BlockBuildingTask {
        let provider = self.blockchain.clone();
        let pool = self.pool.clone();
        let chain_spec = provider.chain_spec();
        let to_worker = self.to_worker.clone();
        let latest_update = self.latest_update.clone();

        // TODO: this is needs further scrutiny
        //
        // see https://eips.ethereum.org/EIPS/eip-4399
        //
        // The right way is to provide the prevrandao from CL,
        // then peers ensure this block is less than 2 rounds behind.
        // logic:
        // - 1 round of consensus -> worker updates
        // - this block produced
        // - this block sent to peers (async)
        // - peer updates with 2 roud of consensus
        // - peer receives this block
        // - this block is valid because it was built off round 1
        //      - if this block was built off round 0, then it's invalid
        //      - ensure parent timestamp and this timestamp is (2 * max block duration)
        //
        // For now: this provides sufficent randomness for on-chain security,
        // but requires an unacceptable amount of trust in the node operator
        //
        // TODO: move final execution to ENGINE - do not rely on mix hash at worker level
        // let prevrandao = parent.parent_beacon_block_root.unwrap_or_else(|| B256::random());
        // let (cfg, block_env) =
        //     self.cfg_and_block_env(chain_spec.as_ref(), &parent, timestamp, prevrandao);

        let config = PendingBlockConfig::new(
            chain_spec,
            self.address,
            self.gas_limit, // in wei
            self.max_size,  // in bytes
        );
        let build_args = WorkerBlockBuilderArgs::new(provider, pool.clone(), config);
        let (ack, rx) = oneshot::channel();

        // spawn block building task and forward to worker
        tokio::task::spawn(async move {
            // this is safe to call without a semaphore bc it's held as a single `Option`
            let BlockBuilderOutput { worker_block: block, mined_transactions } =
                build_worker_block(build_args, &latest_update);

            // forward to worker and wait for ack that quorum was reached
            if let Err(e) = to_worker.send(NewWorkerBlock { block, ack }).await {
                error!(target: "worker::block_builder", ?e, "failed to send next block to worker");
            }

            // wait for worker to ack quorum reached then update pool with mined transactions
            match rx.await {
                Ok(_hash) => {
                    // read from watch channel to ensure only mined transactions are updated
                    let latest = latest_update.borrow();

                    //
                    // TODO: is this a race condition?
                    // - watch channel updated by pool maintenance after calling `on_canonical_state_change`
                    //
                    // only maintenance task can guarantee correct order of applying updates, but
                    // this task needs to ensure the mined transactions are removed before building the next block

                    // create canonical state update
                    // use latest values so only mined transactions are updated
                    let update = CanonicalStateUpdate {
                        new_tip: &latest.new_tip,
                        pending_block_base_fee: latest.pending_block_base_fee,
                        pending_block_blob_fee: latest.pending_block_blob_fee,
                        changed_accounts: vec![], // only updated by engine updates
                        mined_transactions,
                    };

                    // update pool to remove mined transactions
                    pool.on_canonical_state_change(update);
                }
                Err(e) => {
                    error!(target: "worker::block_builder", ?e, "quorum waiter failed ack failed");
                }
            }
        });

        // return oneshot channel for receiving ack
        //
        // reply acknowledges the block was received and stored in db
        // see: worker::block_provider.rs
        // rx
        todo!()
    }
}

/// The [BlockBuilder] is a future that loops through the following:
/// - check/apply canonical state changes that affect the next build
/// - check the block builder is idle
/// - check if there are transactions in pending pool
/// - build next block if pending transactions are available
/// - poll any pending tasks
/// - broadcast the newly proposed block once ack received
///     - update base fee for RPC and transaction pool
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl<BT, Pool, CE> Future for BlockBuilder<BT, Pool, CE>
where
    BT: StateProviderFactory
        + CanonChainTracker
        + CanonStateSubscriptions
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + BlockReaderIdExt
        + BlockchainTreeEngine
        + Clone
        + Unpin
        + 'static,
    CE: ConfigureEvm,
    Pool: TransactionPool + TransactionPoolExt + Unpin + 'static,
    <Pool as TransactionPool>::Transaction: IntoRecoveredTransaction,
{
    type Output = BlockBuilderResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        //
        // TODO:
        //
        // Should the pending transaction notification stream be used to signal wakeup?
        // pros:
        //  - canon updates happen infrequently (~5s currently), so if no txs for a while, then no
        //    pending task, so no wakeup
        //
        // cons:
        //  - could trigger this task to wake up frequently, but maybe that's a good thing since
        //    this is one of the worker's primary responsibilities
        //
        // other option is to set an interval as specified in the config?

        //
        // TODO: apply mined transactions to tx pool

        loop {
            // only insert task if there is none
            //
            // note: it's important that the previous block build finishes before
            // inserting the next task to ensure updates are applied correctly
            if this.pending_task.is_none() {
                // TODO: is there a more efficient approach? only need pending pool stats
                // create upstream PR for reth?
                //
                // check for pending transactions
                //
                // considered using: pool.pool_size().pending
                // but that calculates size for all sub-pools
                if this.pool.pending_transactions().is_empty() {
                    // nothing pending
                    break;
                }

                // start building the next block
                this.pending_task = Some(this.spawn_execution_task());

                // don't break so pending_task receiver gets polled
            }

            // poll receiver that returns worker's ack once block is proposed
            if let Some(mut receiver) = this.pending_task.take() {
                // poll here so waker is notified when ack received
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        // TODO: update tree's pending block?
                        //
                        // ensure no errors
                        let _worker_block_hash = res?;

                        // check max_builds and possibly return early
                        #[cfg(feature = "test-utils")]
                        if let Some(max_builds) = this.max_builds.as_mut() {
                            max_builds.num_builds += 1;
                            if max_builds.has_reached_max() {
                                debug!(target: "worker::block_builder", ?max_builds, "max builds reached");
                                return Poll::Ready(Ok(()));
                            }
                        }

                        // loop again to check for engine updates and possibly start building the
                        // next block
                        continue;
                    }
                    Poll::Pending => {
                        this.pending_task = Some(receiver);

                        // break loop and return Poll::Pending
                        break;
                    }
                }
            }
        }

        // all output executed, yield back to runtime
        Poll::Pending
    }
}
