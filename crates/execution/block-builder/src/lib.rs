//! The worker's block maker monitors a transaction pool populated by incoming transactions through the worker's RPC.
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
use error::BlockBuilderResult;
use futures_util::{FutureExt, StreamExt};
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_evm::{
    execute::{
        BlockExecutionError, BlockExecutionOutput, BlockExecutorProvider, BlockValidationError,
        Executor,
    },
    ConfigureEvm,
};
use reth_evm_ethereum::revm_spec_by_timestamp_after_merge;
use reth_primitives::{
    constants::{EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE},
    keccak256, proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber,
    Header, IntoRecoveredTransaction, SealedBlock, SealedHeader, TransactionSigned, Withdrawals,
    B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockReaderIdExt, CanonChainTracker, CanonStateNotification, CanonStateNotificationStream,
    CanonStateNotifications, CanonStateSubscriptions, Chain, ChainSpecProvider, ExecutionOutcome,
    StateProviderFactory,
};
use reth_revm::{
    database::StateProviderDatabase,
    primitives::{BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg},
};
use reth_transaction_pool::{CanonicalStateUpdate, TransactionPool, TransactionPoolExt};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tn_types::{
    now, AutoSealConsensus, NewBatch, PendingBlockConfig, WorkerBlockBuilderArgs,
    WorkerBlockUpdate, WorkerBlockUpdateSender,
};
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
type PendingExecutionTask = oneshot::Receiver<BlockBuilderResult<WorkerBlockUpdate>>;

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
    /// The address for worker block's beneficiary.
    pub(crate) address: Address,

    // TODO: consider using a container struct for these values
    //
    /// The [SealedHeader] of the last fully-executed block (see TN Engine).
    ///
    /// This information is from the current finalized block number and hash.
    parent_block: SealedBlock,
    //
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
    BT: CanonStateSubscriptions
        + ChainSpecProvider
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
        pending_task: Option<PendingExecutionTask>,
        blockchain: BT,
        pool: Pool,
        evm_config: CE,
        to_worker: Sender<NewBatch>,
        max_builds: Option<usize>,
        address: Address,
        parent_block: SealedBlock,
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
            address,
            parent_block,
            base_fee,
        }
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask {
        let evm_config = self.evm_config.clone();
        let provider = self.blockchain.clone();
        let parent = self.parent_block.clone();
        let pool = self.pool.clone();
        let timestamp = now();
        let chain_spec = provider.chain_spec();

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
        let prevrandao = parent.parent_beacon_block_root.unwrap_or_else(|| B256::random());
        let (cfg, block_env) =
            self.cfg_and_block_env(chain_spec.as_ref(), &parent, timestamp, prevrandao);
        let config =
            PendingBlockConfig::new(parent, block_env, cfg, chain_spec, timestamp, self.address);
        let build_args = WorkerBlockBuilderArgs::new(provider, pool, config);
        let (tx, rx) = oneshot::channel();

        // spawn blocking task and return future
        tokio::task::spawn_blocking(|| {
            // this is safe to call on blocking thread without a semaphore bc it's held in
            // Self::pending_tesk as a single `Option`
            let result = build_worker_block(evm_config, build_args);
            match tx.send(result) {
                Ok(()) => (),
                Err(e) => {
                    error!(target: "engine", ?e, "error sending result from execute_consensus_output")
                }
            }
        });

        // oneshot receiver for execution result
        rx
    }

    /// Returns the configured [`CfgEnvWithHandlerCfg`] and [`BlockEnv`] for the targeted payload
    /// (that has the `parent` as its parent).
    ///
    /// The `chain_spec` is used to determine the correct chain id and hardfork for the payload
    /// based on its timestamp.
    ///
    /// Block related settings are derived from the `parent` block and the configured attributes.
    ///
    /// NOTE: This is only intended for beacon consensus (after merge).
    fn cfg_and_block_env(
        &self,
        chain_spec: &ChainSpec,
        parent: &Header,
        timestamp: u64,
        prev_randao: B256,
    ) -> (CfgEnvWithHandlerCfg, BlockEnv) {
        // configure evm env based on parent block (finalized block from latest consensus round)
        let cfg = CfgEnv::default().with_chain_id(chain_spec.chain().id());

        // ensure we're not missing any timestamp based hardforks
        let spec_id = revm_spec_by_timestamp_after_merge(chain_spec, now());

        // TODO: support blobs with Cancun upgrade
        let blob_excess_gas_and_price = None;

        let gas_limit = U256::from(parent.gas_limit);

        let block_env = BlockEnv {
            number: U256::from(parent.number + 1),
            // NOTE: important so this primary receives any tips
            coinbase: self.address,
            timestamp: U256::from(timestamp),
            difficulty: U256::ZERO,
            prevrandao: Some(prev_randao),
            gas_limit,
            // TODO: engine should update basefees after executing round
            //
            // calculate basefee based on parent block's gas usage
            basefee: self.base_fee.map(U256::from).unwrap_or_default(),
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price,
        };

        (CfgEnvWithHandlerCfg::new_with_spec_id(cfg, spec_id), block_env)
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
        + ChainSpecProvider
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
            //
            // this is critical to ensure worker's block is building off canonical tip
            // block until canon updates are applied
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
                //
                // considered using: pool.pool_size().pending
                // but this calculates size for all sub-pools
                if this.pool.pending_transactions().len() == 0 {
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
                        // TODO: best way to handle if pending txs in pool
                        // but execution fails? Although unlikely, this approach
                        // would produce an empty worker block which should not happen.

                        let worker_update = res.map_err(Into::into).and_then(|res| res)?;

                        // ensure no errors then store last executed header in memory
                        this.parent_block = worker_update.parent().clone();

                        // TODO: should this be background or directly call pool.on_canonical_state_change()
                        // - maintenance track is non blocking BUT
                        // - maintenance task could be queued, canonical state updates, the maintenance task overwrites parent block
                        //      - race condition
                        //
                        // Just call it here.
                        //
                        // apply worker update
                        // NOTE: this is blocking
                        // - better to have maintenance task that always checks canon and worker updates?
                        // - need to ensure that worker update's parent block can't overwrite canon update tip
                        // - also need to ensure pool isn't updated while building block

                        // broadcast worker update
                        //
                        // the tx pool maintenance task should pick this up?
                        // or is it better to just do it here?
                        //

                        // From parkinglot docs:
                        //
                        // read lock:
                        // Locks this RwLock with shared read access, blocking the current thread until it can be acquired.
                        // The calling thread will be blocked until there are no more writers which hold the lock. There may be other readers currently inside the lock when this method returns.
                        // Note that attempts to recursively acquire a read lock on a RwLock when the current thread already holds one may result in a deadlock.
                        // Returns an RAII guard which will release this thread's shared access once it is dropped.

                        // write lock:
                        // Locks this RwLock with exclusive write access, blocking the current thread until it can be acquired.
                        // This function will not return while other writers or other readers currently have access to the lock.
                        // Returns an RAII guard which will drop the write access of this RwLock when dropped

                        // check max_builds
                        if this.max_builds.is_some() {
                            // increase num builds
                            this.num_builds = this.num_builds.map(|n| n + 1);
                            if this.has_reached_max_build(this.num_builds) {
                                // immediately terminate if the specified max number of blocks were built
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
