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
use consensus_metrics::metered_channel::Sender;
use error::{BlockBuilderError, BlockBuilderResult};
use futures_util::{FutureExt, StreamExt};
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_evm_ethereum::revm_spec_by_timestamp_after_merge;
use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, proofs, Address, Header, IntoRecoveredTransaction, SealedBlock,
    SealedHeader, TxHash, B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotification, CanonStateNotificationStream,
    CanonStateSubscriptions, ChainSpecProvider, StateProviderFactory,
};
use reth_revm::primitives::{BlockEnv, CfgEnv, CfgEnvWithHandlerCfg};
use reth_transaction_pool::{TransactionPool, TransactionPoolExt};
use std::{
    future::Future,
    pin::Pin,
    sync::mpsc::Receiver,
    task::{Context, Poll, Waker},
};
use tn_types::{
    now, NewWorkerBlock, PendingBlockConfig, WorkerBlockBuilderArgs, WorkerBlockUpdate,
    WorkerBlockUpdateSender,
};
use tokio::{
    sync::{broadcast, oneshot, watch},
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error, trace, warn};

mod block_builder;
mod error;
mod pool;
pub use pool::{maintain_transaction_pool_future, PoolMaintenanceConfig};

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
type BlockBuildingTask = JoinHandle<SealedHeader>;
// type BlockBuildingTask = oneshot::Receiver<BlockBuilderResult<WorkerBlockUpdate>>;

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
    to_worker: Sender<NewWorkerBlock>,
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
    /// The address for worker block's beneficiary.
    address: Address,
    /// Receiver stream for pending transactions in the pool.
    pending_tx_hashes_stream: ReceiverStream<TxHash>,
    /// The [SealedHeader] of the last fully-executed block (see TN Engine).
    ///
    /// This information is from the current finalized block number and hash.
    parent: SealedHeader,
    /// The watch channel containing the latest base fee.
    ///
    /// Basefees are updated by the engine after a new round of consensus was executed and should
    /// be uniform across all workers using building off the same parent. Worker uses the basefee
    /// from the latest round of consensus for proposing it's next blocks. The basefee included in
    /// the worker's block is used by the engine during final execution. Peers validate that the
    /// worker's basefee is correct based on it's parent.
    basefee: watch::Receiver<u64>,
    /// The maximum amount of gas for a worker block.
    ///
    /// NOTE: transactions are not executed at this stage, so the worker measures the amount of gas
    /// specified by a transaction's gas limit.
    gas_limit: u64,
    /// The maximum size of collected transactions, measured in bytes.
    max_size: usize,
}

impl<BT, Pool, CE> BlockBuilder<BT, Pool, CE>
where
    BT: CanonStateSubscriptions
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + StateProviderFactory
        + BlockchainTreeEngine
        + CanonChainTracker
        + Clone
        + 'static,
    Pool: TransactionPoolExt + 'static,
    CE: ConfigureEvm + Clone,
{
    /// Create a new instance of [Self].
    pub fn new(
        blockchain: BT,
        pool: Pool,
        evm_config: CE,
        to_worker: Sender<NewWorkerBlock>,
        max_builds: Option<usize>,
        address: Address,
        parent: SealedHeader,
        pending_tx_hashes_stream: ReceiverStream<TxHash>,
        basefee: watch::Receiver<u64>,
        gas_limit: u64,
        max_size: usize,
    ) -> Self {
        // create broadcast channels
        // NOTE: it's important that worker updates are processed quickly
        let (worker_block_updates, _) = broadcast::channel(10);

        // subscribe to canon state updates
        let canonical_state_updates = blockchain.canonical_state_stream();

        // start at 0 if max_builds specified
        let num_builds = max_builds.map(|_| 0);

        Self {
            pending_task: None,
            blockchain,
            pool,
            evm_config,
            worker_block_updates,
            canonical_state_updates,
            to_worker,
            max_builds,
            num_builds,
            address,
            parent,
            pending_tx_hashes_stream,
            basefee,
            gas_limit,
            max_size,
        }
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&self, basefee: u64) -> BlockBuildingTask {
        let provider = self.blockchain.clone();
        let parent = self.parent.clone();
        let pool = self.pool.clone();
        let chain_spec = provider.chain_spec();

        // TODO: use ms for worker block and sec for final block?
        //
        // sometimes worker block are produced too quickly in certain configs (<1s)
        // resulting in batch timestamp == parent timestamp
        let mut timestamp = now();
        if timestamp == parent.timestamp {
            warn!(target: "execution::block_builder", "new block timestamp same as parent - setting offset by 1sec");
            timestamp = parent.timestamp + 1;
        }

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
            parent,
            chain_spec,
            timestamp,
            self.address,
            basefee,
            None,           // TODO: no support for blobs yet
            self.gas_limit, // in wei
            self.max_size,  // in bytes
        );
        let build_args = WorkerBlockBuilderArgs::new(provider, pool, config);

        // spawn blocking task and return future
        //
        // return JoinHandle instead of oneshot because
        // build_worker_block does not return any errors
        tokio::task::spawn_blocking(|| {
            // this is safe to call on blocking thread without a semaphore bc
            // it's held in Self::pending_task as a single `Option`
            build_worker_block(build_args)
        })
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
        // Should the pending transaction notification stream be used to signal wakeup?
        // pros:
        //  - canon updates happen infrequently (~5s currently), so if no txs for a while,
        //    then no pending task, so no wakeup
        //
        // cons:
        //  - could trigger this task to wake up frequently, but maybe that's a good thing
        //    since this is one of the worker's primary responsibilities
        //
        // also need to check basefee for changed? or should this not matter?
        // don't want a race condition where this task wakes up before canon update received...
        //
        // ideally they would be sent on the same channel, but working around reth right now

        loop {
            // check for canon updates before mining the transaction pool
            //
            // this is critical to ensure worker's block is building off canonical tip
            // block until canon updates are applied
            while let Poll::Ready(update) = this.canonical_state_updates.poll_next_unpin(cx) {
                match update {
                    Some(canon_update) => {
                        // poll canon updates stream and update pool `.on_canon_update`
                        //
                        // maintenance task will handle worker's pending block update
                        match canon_update {
                            CanonStateNotification::Commit { new } => {
                                // update parent information
                                this.parent = new.tip().header.clone();
                            }
                            _ => unreachable!("TN reorgs are impossible"),
                        }
                    }
                    // stream closed
                    None => return Poll::Ready(Ok(())),
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
                //
                // considered using: pool.pool_size().pending
                // but this calculates size for all sub-pools
                if this.pool.pending_transactions().len() == 0 {
                    // nothing pending
                    break;
                }

                // build the next block
                let basefee = *this.basefee.borrow_and_update();
                this.pending_task = Some(this.spawn_execution_task(basefee));

                // don't break so pending_task gets polled
            }

            // poll receiver that returns block build result
            if let Some(mut receiver) = this.pending_task.take() {
                // poll here so waker gets called when block is ready
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        // ensure no errors then store last executed header in memory
                        this.parent = res?;

                        // TODO: should this be background or directly call
                        // pool.on_canonical_state_change()
                        // - maintenance track is non blocking BUT
                        // - maintenance task could be queued, canonical state updates, the
                        //   maintenance task overwrites parent block
                        //      - race condition
                        //
                        // Just call it here.
                        //
                        // apply worker update
                        // NOTE: this is blocking
                        // - better to have maintenance task that always checks canon and worker
                        //   updates?
                        // - need to ensure that worker update's parent block can't overwrite canon
                        //   update tip
                        // - also need to ensure pool isn't updated while building block

                        // broadcast worker update
                        //
                        // the tx pool maintenance task should pick this up?
                        // or is it better to just do it here?
                        //

                        // From parkinglot docs:
                        //
                        // read lock:
                        // Locks this RwLock with shared read access, blocking the current thread
                        // until it can be acquired. The calling thread will
                        // be blocked until there are no more writers which hold the lock. There may
                        // be other readers currently inside the lock when this method returns.
                        // Note that attempts to recursively acquire a read lock on a RwLock when
                        // the current thread already holds one may result in a deadlock.
                        // Returns an RAII guard which will release this thread's shared access once
                        // it is dropped.

                        // write lock:
                        // Locks this RwLock with exclusive write access, blocking the current
                        // thread until it can be acquired. This function
                        // will not return while other writers or other readers currently have
                        // access to the lock. Returns an RAII guard which
                        // will drop the write access of this RwLock when dropped

                        // check max_builds
                        if this.max_builds.is_some() {
                            // increase num builds
                            this.num_builds = this.num_builds.map(|n| n + 1);
                            if this.has_reached_max_build(this.num_builds) {
                                // immediately terminate if the specified max number of blocks were
                                // built
                                return Poll::Ready(Ok(()));
                            }
                        }

                        // loop again to check for engine updates
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
