//! The worker's block maker monitors a transaction pool populated by incoming transactios through the worker's RPC.
//!
//! The Mining task polls a [MiningMode], and will return a list of transactions that are ready to
//! be mined.
//!
//! These downloaders poll the miner, assemble the block, and return transactions that are ready to
//! be mined.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use consensus_metrics::metered_channel::Sender;
use error::BlockBuilderResult;
use futures_util::{FutureExt, StreamExt};
use pool::maintain::changed_accounts_iter;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_evm::{
    execute::{
        BlockExecutionError, BlockExecutionOutput, BlockExecutorProvider, BlockValidationError,
        Executor,
    },
    ConfigureEvm,
};
use reth_primitives::{
    constants::{EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE},
    keccak256, proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber,
    Header, IntoRecoveredTransaction, SealedHeader, TransactionSigned, Withdrawals, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotification, CanonStateNotificationStream,
    CanonStateNotifications, CanonStateSubscriptions, Chain, ExecutionOutcome,
    StateProviderFactory,
};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::{CanonicalStateUpdate, TransactionPool, TransactionPoolExt};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tn_types::{now, AutoSealConsensus, NewBatch, WorkerBlockUpdateSender};
use tokio::sync::{broadcast, oneshot, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, error, trace, warn};

mod block_builder;
mod error;
mod pool;

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

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<BlockBuilderResult<SealedHeader>>;

/// The type that builds blocks for workers to propose.
///
/// This is a future that polls the transaction pool for pending transactions and tries to build the next worker block.
#[derive(Debug)]
pub struct BlockBuilder<BT, Pool, CE> {
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<PendingExecutionTask>,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// The transaction pool with pending transactions.
    pool: Pool,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    // /// Optional round of consensus to finish executing before then returning. The value is used to
    // /// track the subdag index from consensus output. The index is also considered the "round" of
    // /// consensus and is included in executed blocks as  the block's `nonce` value.
    // ///
    // /// NOTE: this is primarily useful for debugging and testing
    // max_round: Option<u64>,
    /// The sending side of broadcast channel when a worker has successfully proposed a new block.
    ///
    /// The RPC and transaction pool subsribe to these updates.
    worker_block_updates: WorkerBlockUpdateSender,
    /// Receiving end from the engine's progress. Canonical state updates
    /// are broadcast. These updates must take priority.
    canonical_state_updates: CanonStateNotificationStream,
    /// The sending side to the worker's batch maker.
    ///
    /// Sending the new block through this channel triggers a broadcast to all peers.
    ///
    /// The worker's block maker sends an ack once the block has been stored in db
    /// which guarantees the worker will attempt to broadcast the new block until
    /// quorum is reached.
    to_worker: Sender<NewBatch>,
    /// Optional number of blocks to build before shutting down.
    ///
    /// Engine can produce multiple blocks per round of consensus, so this number may not
    /// match the subdag index or block height. To control the number of outputs, consider
    /// specifying a `max_round` for the execution engine.
    ///
    /// NOTE: this is primarily useful for debugging and testing
    max_builds: Option<usize>,
    /// The number of blocks this block builder has built.
    ///
    /// NOTE: this is only used when `max_blocks` is specified.
    num_builds: Option<usize>,

    // TODO: consider using a container struct for these values
    /// The [SealedHeader] of the last fully-executed block.
    ///
    /// This information reflects the current finalized block number and hash.
    parent_header: SealedHeader,
    /// The current base fee used to build the next block.
    ///
    /// This value is updated after constructing a block and used to update
    /// transaction pool and RPC even during canonical updates.
    ///
    /// Workers have an independent base fee market and the canon state updates
    /// from the engine would overwrite them otherwise.
    base_fee: Option<u64>,
}

impl<BT, Pool, CE> BlockBuilder<BT, Pool, CE>
where
    BT: CanonStateSubscriptions,
    Pool: TransactionPoolExt,
{
    /// Create a new instance of [Self].
    pub fn new(
        pending_task: Option<PendingExecutionTask>,
        blockchain: BT,
        pool: Pool,
        evm_config: CE,
        to_worker: Sender<NewBatch>,
        max_builds: Option<usize>,
        parent_header: SealedHeader,
        base_fee: Option<u64>,
    ) -> Self {
        // create broadcast channels
        // NOTE: it's important that worker updates are processed quickly
        let (worker_block_updates, _) = broadcast::channel(10);

        // subscribe to canon state updates
        let canonical_state_updates = blockchain.canonical_state_stream();

        // start at 0 if max_builds specified
        let num_builds = max_builds.map(|_| 0);

        Self {
            pending_task,
            blockchain,
            pool,
            evm_config,
            worker_block_updates,
            canonical_state_updates,
            to_worker,
            max_builds,
            num_builds,
            parent_header,
            base_fee,
        }
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to Update pool before building the next block.
    fn process_canon_state_update(&self, update: Arc<Chain>) {
        // TODO: ensure the engine's update includes all accounts that changed during execution
        warn!(target: "Canon update inside block builder!!!!\n", ?update);

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        let mut changed_accounts = Vec::with_capacity(state.state().len());
        for acc in changed_accounts_iter(state) {
            changed_accounts.push(acc);
        }

        // TODO: these should have already been mined when the worker proposed its block?
        let mined_transactions = blocks.transaction_hashes().collect();
        let pending_block_base_fee = self.base_fee.unwrap_or(MIN_PROTOCOL_BASE_FEE);

        // Canonical update
        let update = CanonicalStateUpdate {
            new_tip: &tip.block,          // finalized block
            pending_block_base_fee,       // current base fee for worker
            pending_block_blob_fee: None, // current base fee for worker
            changed_accounts,             // finalized block
            mined_transactions,           // finalized block (but these should be a noop)
        };

        // sync fn so self will block until all pool updates are complete
        self.pool.on_canonical_state_change(update);
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask
    where
        BT: StateProviderFactory + BlockchainTreeEngine + CanonChainTracker + Clone,
    {
        todo!()
        // let provider = self.blockchain.clone();
        // let evm_config = self.evm_config.clone();
        // let parent = self.parent_header.clone();
        // let build_args = WorkerBuildArguments::new(provider, output, parent);
        // let (tx, rx) = oneshot::channel();

        // // spawn blocking task and return future
        // tokio::task::spawn_blocking(|| {
        //     // this is safe to call on blocking thread without a semaphore bc it's held in
        //     // Self::pending_tesk as a single `Option`
        //     let result = build_worker_block(evm_config, build_args);
        //     match tx.send(result) {
        //         Ok(()) => (),
        //         Err(e) => {
        //             error!(target: "engine", ?e, "error sending result from execute_consensus_output")
        //         }
        //     }
        // });

        // // oneshot receiver for execution result
        // rx
    }

    /// Check if the task has reached the maximum number of blocks to build as specified by `max_builds`.
    ///
    /// Note: this is mainly for testing and debugging purposes.
    fn has_reached_max_build(&self, progress: Option<usize>) -> bool {
        let has_reached_max_build = progress >= self.max_builds;
        if has_reached_max_build {
            trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_builds,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_build
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

        loop {
            // check for canon updates before mining the transaction pool
            while let Poll::Ready(Some(canon_update)) =
                this.canonical_state_updates.poll_next_unpin(cx)
            {
                // poll canon updates stream and update pool `.on_canon_update`
                //
                // maintenance task will handle worker's pending block update
                match canon_update {
                    CanonStateNotification::Commit { new } => {
                        this.process_canon_state_update(new);
                    }
                    _ => unreachable!("TN reorgs are impossible"),
                }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous block build finishes before
            // inserting the next task to ensure updates are applied correctly
            if this.pending_task.is_none() {
                // TODO: is there a more efficient approach?
                // only need pending pool stats
                //
                // create upstream PR for reth

                // check for pending transactions
                if this.pool.pool_size().pending == 0 {
                    // nothing pending
                    break;
                }

                // build the next block
                this.pending_task = Some(this.spawn_execution_task());
            }

            // poll receiver that returns block build result
            if let Some(mut receiver) = this.pending_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        let finalized_header = res.map_err(Into::into).and_then(|res| res);

                        // ensure no errors then store last executed header in memory
                        this.parent_header = finalized_header?;

                        // check max_builds
                        if this.max_builds.is_some() {
                            // increase num builds
                            this.num_builds = this.num_builds.map(|n| n + 1);
                            if this.has_reached_max_build(this.num_builds) {
                                // immediately terminate if the specified max number of blocks were built
                                return Poll::Ready(Ok(()));
                            }
                        }

                        // allow loop to continue: check for engine updates
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
