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

pub use block_builder::{build_worker_block, BlockBuilderOutput};
use error::{BlockBuilderError, BlockBuilderResult};
use futures_util::{FutureExt, StreamExt};
use reth_execution_types::ChangedAccount;
use reth_primitives::{constants::MIN_PROTOCOL_BASE_FEE, Address, TxHash};
use reth_provider::{CanonStateNotification, CanonStateNotificationStream, Chain};
use reth_transaction_pool::{CanonicalStateUpdate, TransactionPool, TransactionPoolExt};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{
    error::BlockSealError, LastCanonicalUpdate, PendingBlockConfig, WorkerBlockBuilderArgs,
    WorkerBlockSender,
};
use tokio::sync::{mpsc::Receiver, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, trace, warn};

mod block_builder;
mod error;
#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Type alias for the blocking task that locks the tx pool and builds the next worker block.
type BlockBuildingTask = oneshot::Receiver<BlockBuilderResult<Vec<TxHash>>>;

/// The type that builds blocks for workers to propose.
///
/// This is a future that:
/// - listens for canonical state changes and updates the tx pool
/// - polls the transaction pool for pending transactions
///     - tries to build the next worker block when there transactions are available
/// -
#[derive(Debug)]
pub struct BlockBuilder<BT, Pool> {
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<BlockBuildingTask>,
    /// The type used to query both the database and the blockchain tree.
    ///
    /// TODO: leaving this for now to prevent generics refactor
    _blockchain: BT,
    /// The transaction pool with pending transactions.
    pool: Pool,
    /// Canonical state changes from the engine.
    ///
    /// Notifications are sent on this stream after each round of consensus
    /// is executed. These updates are used to apply changes to the transaction pool.
    canonical_state_stream: CanonStateNotificationStream,
    /// Type to track the last canonical state update.
    ///
    /// The worker applies updates to the pool when it mines new transactions, but
    /// the canonical tip and basefee only change through engine updates. This type
    /// allows the worker to apply mined transactions updates without affecting the
    /// tip or basefee between rounds of consensus.
    ///
    /// This is a solution until TN has it's own transaction pool implementation.
    latest_canon_state: LastCanonicalUpdate,
    // /// Optional round of consensus to finish executing before then returning. The value is used
    // to /// track the subdag index from consensus output. The index is also considered the
    // "round" of /// consensus and is included in executed blocks as  the block's `nonce`
    // value. ///
    // /// NOTE: this is primarily useful for debugging and testing
    // max_round: Option<u64>,
    /// The sending side to the worker's batch maker.
    ///
    /// Sending the new block through this channel triggers a broadcast to all peers.
    ///
    /// The worker's block maker sends an ack once the block has been stored in db
    /// which guarantees the worker will attempt to broadcast the new block until
    /// quorum is reached.
    to_worker: WorkerBlockSender,
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

impl<BT, Pool> BlockBuilder<BT, Pool>
where
    Pool: TransactionPoolExt + 'static,
{
    /// Create a new instance of [Self].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        _blockchain: BT,
        pool: Pool,
        canonical_state_stream: CanonStateNotificationStream,
        latest_canon_state: LastCanonicalUpdate,
        to_worker: WorkerBlockSender,
        address: Address,
        pending_tx_hashes_receiver: Receiver<TxHash>,
        gas_limit: u64,
        max_size: usize,
        #[cfg(feature = "test-utils")] max_builds: Option<usize>,
    ) -> Self {
        let pending_tx_hashes_stream = ReceiverStream::new(pending_tx_hashes_receiver);
        Self {
            pending_task: None,
            _blockchain,
            pool,
            canonical_state_stream,
            latest_canon_state,
            to_worker,
            address,
            pending_tx_hashes_stream,
            gas_limit,
            max_size,
            #[cfg(feature = "test-utils")]
            max_builds: max_builds.map(test_utils::MaxBuilds::new),
        }
    }

    /// This method is called when a canonical state update is received.
    ///
    /// Trigger the maintenance task to update pool before building the next block.
    fn process_canon_state_update(&mut self, update: Arc<Chain>) {
        trace!(target: "worker::pool_maintenance", ?update, "canon state update from engine");

        // update pool based with canonical tip update
        let (blocks, state) = update.inner();
        let tip = blocks.tip();

        // collect all accounts that changed in last round of consensus
        let changed_accounts: Vec<ChangedAccount> = state
            .accounts_iter()
            .filter_map(|(addr, acc)| acc.map(|acc| (addr, acc)))
            .map(|(address, acc)| ChangedAccount {
                address,
                nonce: acc.nonce,
                balance: acc.balance,
            })
            .collect();

        // collect tx hashes to remove any transactions from this pool that were mined
        let mined_transactions: Vec<TxHash> = blocks.transaction_hashes().collect();

        // TODO: calculate the next basefee HERE for the entire round
        //
        // for now, always use lowest base fee possible
        let pending_block_base_fee = MIN_PROTOCOL_BASE_FEE;

        // Canonical update
        let update = CanonicalStateUpdate {
            new_tip: &tip.block,          // finalized block
            pending_block_base_fee,       // current base fee for worker (network-wide)
            pending_block_blob_fee: None, // current blob fee for worker (network-wide)
            changed_accounts,             // entire round of consensus
            mined_transactions,           // entire round of consensus
        };

        // track latest update to apply worker blocks
        let latest = LastCanonicalUpdate {
            tip: tip.block.clone(),
            pending_block_base_fee,
            pending_block_blob_fee: None,
        };

        // track canon update so worker updates don't overwrite the tip or base fees
        self.latest_canon_state = latest;

        // sync fn so self will block until all pool updates are complete
        self.pool.on_canonical_state_change(update);
    }

    /// Spawns a task to build the worker block and proposer to peers.
    ///
    /// This approach allows the block builder to yield back to the runtime while mining blocks.
    ///
    /// The task performs the following actions:
    /// - create a block
    /// - send the block to worker's block proposer
    /// - wait for ack that quorum was reached
    /// - convert result to fatal/non-fatal
    /// - return result
    ///
    /// Workers only propose one block at a time.
    fn spawn_execution_task(&self) -> BlockBuildingTask {
        let pool = self.pool.clone();
        let to_worker = self.to_worker.clone();

        // configure params for next block to build
        let config = PendingBlockConfig::new(
            self.address,
            self.latest_canon_state.clone(),
            self.gas_limit, // in wei
            self.max_size,  // in bytes
        );
        let build_args = WorkerBlockBuilderArgs::new(pool.clone(), config);
        let (result, done) = oneshot::channel();

        // spawn block building task and forward to worker
        tokio::task::spawn(async move {
            // arc dashmap/hashset rwlock for txhashes for this worker by round
            // canon updates clear set
            // successful proposals add mined txs to set

            // ack once worker reaches quorum
            let (ack, rx) = oneshot::channel();

            // this is safe to call without a semaphore bc it's held as a single `Option`
            let BlockBuilderOutput { worker_block, mined_transactions } =
                build_worker_block(build_args);

            // forward to worker and wait for ack that quorum was reached
            if let Err(e) = to_worker.send((worker_block, ack)).await {
                error!(target: "worker::block_builder", ?e, "failed to send next block to worker");
                // try to return error if worker channel closed
                let _ = result.send(Err(e.into()));
                return;
            }

            // wait for worker to ack quorum reached then update pool with mined transactions
            match rx.await {
                Ok(res) => {
                    match res {
                        Ok(_) => {
                            // signal to Self that this task is complete
                            if let Err(e) = result.send(Ok(mined_transactions)) {
                                error!(target: "worker::block_builder", ?e, "failed to send block builder result to block builder task");
                            }
                        }
                        Err(error) => {
                            error!(target: "worker::block_builder", ?error, "error while sealing block");
                            let converted = match error {
                                BlockSealError::FatalDBFailure => {
                                    // fatal - return error
                                    Err(BlockBuilderError::FatalDBFailure)
                                }
                                BlockSealError::QuorumRejected
                                | BlockSealError::AntiQuorum
                                | BlockSealError::Timeout
                                | BlockSealError::FailedQuorum => {
                                    // potentially non-fatal error
                                    //
                                    // return empty vec to indicate no transactions mined
                                    // NOTE: this will apply no changes to transaction pool
                                    Ok(vec![])
                                }
                            };

                            if let Err(e) = result.send(converted) {
                                error!(target: "worker::block_builder", ?e, "failed to send block builder result to block builder task");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(target: "worker::block_builder", ?e, "quorum waiter failed ack failed");
                }
            }
        });

        // return oneshot channel for receiving completion status
        done
    }
}

/// The [BlockBuilder] is a future that loops through the following:
/// - check/apply canonical state changes that affect the next build
/// - poll any pending block building tasks
/// - otherwise, build next block if pending transactions are available
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl<BT, Pool> Future for BlockBuilder<BT, Pool>
where
    BT: Unpin,
    Pool: TransactionPool + TransactionPoolExt + Unpin + 'static,
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
            // check for canon updates before mining the transaction pool
            //
            // this is critical to ensure worker's block is building off canonical tip
            // block until canon updates are applied
            while let Poll::Ready(Some(canon_update)) =
                this.canonical_state_stream.poll_next_unpin(cx)
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

            // TODO: would time interval be better? (min block timer in config)
            // this could wake task frequently under high network demand
            //
            // drain pending pool updates and pass waker so this gets awoken when txs are available
            while let Poll::Ready(msg) = this.pending_tx_hashes_stream.poll_next_unpin(cx) {
                match msg {
                    Some(_tx) => (/* drain nofications */),
                    None => return Poll::Ready(Ok(())), // shutdown
                }
            }

            // only propose one block at a time
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

            // poll receiver that returns mined transactions once the worker block reaches quorum
            if let Some(mut receiver) = this.pending_task.take() {
                // poll here so waker is notified when ack received
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        // TODO: update tree's pending block?

                        // ensure no fatal errors
                        let mined_transactions = res??;

                        // NOTE: empty vec returned for non-fatal error during block proposal
                        if mined_transactions.is_empty() {
                            // return pending until next canon update because the last block failed
                            // to reach quorum
                            //
                            // task should wait until next canonical update applied to pool
                            break;
                        }

                        // use latest values so only mined transactions are updated
                        let new_tip = &this.latest_canon_state.tip;
                        let pending_block_base_fee = this.latest_canon_state.pending_block_base_fee;
                        let pending_block_blob_fee = this.latest_canon_state.pending_block_blob_fee;

                        // create canonical state update
                        let update = CanonicalStateUpdate {
                            new_tip,
                            pending_block_base_fee,
                            pending_block_blob_fee,
                            changed_accounts: vec![], // only updated by engine updates
                            mined_transactions,
                        };

                        // TODO: should this be a spawned blocking task?
                        //
                        // update pool to remove mined transactions
                        this.pool.on_canonical_state_change(update);

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
