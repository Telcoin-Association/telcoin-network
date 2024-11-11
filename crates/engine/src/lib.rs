//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    rustdoc::all,
    unused_crate_dependencies
)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod error;
mod payload_builder;
use error::{EngineResult, TnEngineError};
use futures::{Future, StreamExt};
use futures_util::FutureExt;
pub use payload_builder::execute_consensus_output;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_chainspec::ChainSpec;
use reth_evm::ConfigureEvm;
use reth_primitives::SealedHeader;
use reth_provider::{
    BlockIdReader, BlockReader, CanonChainTracker, ChainSpecProvider, StageCheckpointReader,
    StateProviderFactory,
};
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use tn_types::{BuildArguments, ConsensusOutput};
use tokio::sync::oneshot;
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info, trace, warn};

/// Type alias for the blocking task that executes consensus output and returns the finalized
/// `SealedHeader`.
type PendingExecutionTask = oneshot::Receiver<EngineResult<SealedHeader>>;

/// The TN consensus engine is responsible executing state that has reached consensus.
///
/// The engine makes no attempt to track consensus. It's only purpose is to receive output from
/// consensus then try to execute it.
///
/// The engine runs until either the maximum round of consensus is reached OR the sending broadcast
/// channel is dropped. If the sending channel is dropped, the engine attempts to execute any
/// remaining output that is queued up before shutting itself down gracefully. If the maximum round
/// is reached, the engine shuts down immediately.
pub struct ExecutorEngine<BT, CE> {
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// Single active future that executes consensus output on a blocking thread and then returns
    /// the result through a oneshot channel.
    pending_task: Option<PendingExecutionTask>,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    /// Optional round of consensus to finish executing before then returning. The value is used to
    /// track the subdag index from consensus output. The index is also considered the "round" of
    /// consensus and is included in executed blocks as  the block's `nonce` value.
    ///
    /// NOTE: this is primarily useful for debugging and testing
    max_round: Option<u64>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// The [SealedHeader] of the last fully-executed block.
    ///
    /// This information reflects the current finalized block number and hash.
    parent_header: SealedHeader,
}

impl<BT, CE> ExecutorEngine<BT, CE>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + 'static,
    CE: ConfigureEvm,
{
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// The engine waits for CL to broadcast output then tries to execute.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BT,
        evm_config: CE,
        max_round: Option<u64>,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        parent_header: SealedHeader,
    ) -> Self {
        Self {
            queued: Default::default(),
            pending_task: None,
            blockchain,
            evm_config,
            max_round,
            consensus_output_stream,
            parent_header,
        }
    }

    /// Spawns a blocking task to execute consensus output.
    ///
    /// This approach allows the engine to yield back to the runtime while executing blocks.
    /// Executing blocks is cpu intensive, so a blocking task is used.
    fn spawn_execution_task(&mut self) -> PendingExecutionTask
    where
        BT: StateProviderFactory
            + ChainSpecProvider<ChainSpec = ChainSpec>
            + BlockchainTreeEngine
            + CanonChainTracker
            + Clone,
    {
        let (tx, rx) = oneshot::channel();

        // pop next output in queue and execute
        if let Some(output) = self.queued.pop_front() {
            let provider = self.blockchain.clone();
            let evm_config = self.evm_config.clone();
            let parent = self.parent_header.clone();
            let build_args = BuildArguments::new(provider, output, parent);

            // spawn blocking task and return future
            tokio::task::spawn_blocking(|| {
                // this is safe to call on blocking thread without a semaphore bc it's held in
                // Self::pending_tesk as a single `Option`
                let result = execute_consensus_output(evm_config, build_args);
                match tx.send(result) {
                    Ok(()) => (),
                    Err(e) => {
                        error!(target: "engine", ?e, "error sending result from execute_consensus_output")
                    }
                }
            });
        } else {
            let _ = tx.send(Err(TnEngineError::EmptyQueue));
        }

        // oneshot receiver for execution result
        rx
    }

    /// Check if the engine has reached the maximum round of consensus as specified by `max_round`
    /// parameter.
    ///
    /// Note: this is mainly for testing and debugging purposes.
    fn has_reached_max_round(&self, progress: u64) -> bool {
        let has_reached_max_round =
            self.max_round.map(|target| progress >= target).unwrap_or_default();
        if has_reached_max_round {
            trace!(
                target: "engine",
                ?progress,
                max_round = ?self.max_round,
                "Consensus engine reached max round for consensus"
            );
        }
        has_reached_max_round
    }
}

/// The [ExecutorEngine] is a future that loops through the following:
/// - receive messages from consensus
/// - add these messages to a queue
/// - pull from queue to start next execution task if idle
/// - poll any pending tasks that are currently being executed
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins
/// executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and
/// any output that is queued.
impl<BT, CE> Future for ExecutorEngine<BT, CE>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + StateProviderFactory
        + ChainSpecProvider<ChainSpec = ChainSpec>
        + Clone
        + Unpin
        + 'static,
    CE: ConfigureEvm,
{
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // check if output is available from consensus to keep broadcast stream from "lagging"
            match this.consensus_output_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(output))) => {
                    // queue the output for local execution
                    this.queued.push_back(output)
                }
                Poll::Ready(Some(Err(e))) => {
                    error!(target: "engine", ?e, "for consensus output stream");
                }
                Poll::Ready(None) => {
                    // the stream has ended
                    //
                    // this could indicate an error but it's also how the Primary signals engine to
                    // shutdown
                    info!(target: "engine", "ConsensusOutput channel closed. Shutting down...");

                    // only return if there are no current tasks and the queue is empty
                    // otherwise, let the loop continue so any remaining tasks and queued output is
                    // executed
                    if this.pending_task.is_none() && this.queued.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                }

                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if this.pending_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to begin executing next round of consensus
                this.pending_task = Some(this.spawn_execution_task());
            }

            // poll receiver that returns output execution result
            if let Some(mut receiver) = this.pending_task.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        let finalized_header = res.map_err(Into::into).and_then(|res| res)?;
                        // TODO: broadcast engine event
                        // this.pipeline_events = events;
                        //
                        // store last executed header in memory
                        this.parent_header = finalized_header;

                        // check max_round
                        if this.max_round.is_some()
                            && this.has_reached_max_round(this.parent_header.nonce)
                        {
                            // immediately terminate if the specified max consensus round is reached
                            return Poll::Ready(Ok(()));
                        }

                        // allow loop to continue: poll broadcast stream for next output
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

impl<BT, CE> std::fmt::Debug for ExecutorEngine<BT, CE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorEngine")
            .field("queued", &self.queued.len())
            .field("pending_task", &self.pending_task.is_some())
            .field("max_round", &self.max_round)
            .field("parent_header", &self.parent_header)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use crate::ExecutorEngine;
    use fastcrypto::hash::Hash as _;
    use narwhal_test_utils::{default_test_execution_node, seeded_genesis_from_random_batches};
    use reth_blockchain_tree::BlockchainTreeViewer;
    use reth_chainspec::ChainSpec;
    use reth_primitives::{
        constants::MIN_PROTOCOL_BASE_FEE, proofs, Address, BlockHashOrNumber, B256,
        EMPTY_OMMER_ROOT_HASH, U256,
    };
    use reth_provider::{BlockIdReader, BlockNumReader, BlockReader, TransactionVariant};
    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;
    use std::{collections::VecDeque, str::FromStr as _, sync::Arc, time::Duration};
    use tn_block_builder::test_utils::execute_test_worker_block;
    use tn_types::{
        adiri_chain_spec_arc, adiri_genesis, now, BlockHash, Certificate, CommittedSubDag,
        ConsensusOutput, ReputationScores,
    };
    use tokio::{sync::oneshot, time::timeout};
    use tokio_stream::wrappers::BroadcastStream;
    use tracing::debug;

    /// This tests that a single block is executed if the output from consensus contains no
    /// transactions.
    #[tokio::test]
    async fn test_empty_output_executes() -> eyre::Result<()> {
        init_test_tracing();
        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput` certificate
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let beneficiary = Address::from_str("0x5555555555555555555555555555555555555555")
            .expect("beneficiary address from str");
        let consensus_output = ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            blocks: Default::default(), // empty
            beneficiary,
            block_digests: Default::default(), // empty
        };

        let chain = adiri_chain_spec_arc();

        // execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let provider = execution_node.get_provider().await;
        let evm_config = execution_node.get_evm_config().await;
        let max_round = None;
        let genesis_header = chain.sealed_genesis_header();

        let engine = ExecutorEngine::new(
            provider.clone(),
            evm_config,
            max_round,
            consensus_output_stream,
            genesis_header.clone(),
        );

        // send output
        let broadcast_result = to_engine.send(consensus_output.clone());
        assert!(broadcast_result.is_ok());

        // drop sending channel to shut engine down
        drop(to_engine);

        let (tx, rx) = oneshot::channel();

        // spawn engine task
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = provider.last_block_number()?;
        let canonical_tip = provider.canonical_tip();
        let final_block = provider.finalized_block_num_hash()?.expect("finalized block");

        assert_eq!(canonical_tip, final_block);
        assert_eq!(last_block_num, final_block.number);

        let expected_block_height = 1;
        // assert 1 empty block was executed for consensus
        assert_eq!(last_block_num, expected_block_height);
        // assert canonical tip and finalized block are equal
        assert_eq!(canonical_tip, final_block);
        // assert last executed output is correct and finalized
        let last_output = execution_node.last_executed_output().await?;
        assert_eq!(last_output, sub_dag_index); // round of consensus

        // pull newly executed block from database (skip genesis)
        let expected_block = provider
            .block_with_senders(BlockHashOrNumber::Number(1), TransactionVariant::NoHash)?
            .expect("block 1 successfully executed");
        assert_eq!(expected_block_height, expected_block.number);

        // min basefee in genesis
        let expected_base_fee = MIN_PROTOCOL_BASE_FEE;
        let output_digest: B256 = consensus_output.digest().into();
        // assert expected basefee
        assert_eq!(genesis_header.base_fee_per_gas, Some(expected_base_fee));
        // basefee comes from workers - if no batches, then use parent's basefee
        assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));

        // assert blocks are executed as expected
        assert!(expected_block.senders.is_empty());
        assert!(expected_block.body.is_empty());

        // assert basefee is same as worker's block
        assert_eq!(expected_block.base_fee_per_gas, Some(expected_base_fee));
        // beneficiary overwritten
        assert_eq!(expected_block.beneficiary, beneficiary);
        // nonce matches subdag index and method all match
        assert_eq!(expected_block.nonce, sub_dag_index);
        assert_eq!(expected_block.nonce, consensus_output.nonce());

        // ommers contains headers from all batches from consensus output
        let expected_ommers = consensus_output.ommers();
        assert_eq!(expected_block.ommers, expected_ommers);
        // ommers root
        assert_eq!(expected_block.header.ommers_hash, EMPTY_OMMER_ROOT_HASH,);
        // timestamp
        assert_eq!(expected_block.timestamp, consensus_output.committed_at());
        // parent beacon block root is output digest
        assert_eq!(expected_block.parent_beacon_block_root, Some(output_digest));
        // first block's parent is expected to be genesis
        assert_eq!(expected_block.parent_hash, chain.genesis_hash());
        // expect state roots to be same because empty output has no state change
        assert_eq!(expected_block.state_root, genesis_header.state_root);
        // expect header number genesis + 1
        assert_eq!(expected_block.number, expected_block_height);

        // mix hash is xor bitwise with worker sealed block's hash and consensus output
        // just use consensus output hash if no worker blocks in the round
        let consensus_output_hash = B256::from(consensus_output.digest());
        assert_eq!(expected_block.mix_hash, consensus_output_hash);
        // bloom expected to be the same bc all proposed transactions should be good
        // ie) no duplicates, etc.
        //
        // TODO: randomly generate contract transactions as well!!!
        assert_eq!(expected_block.logs_bloom, genesis_header.logs_bloom);
        // gas limit should come from parent for empty execution
        //
        // TODO: ensure batch validation prevents peer workers from changing this value
        assert_eq!(expected_block.gas_limit, genesis_header.gas_limit);
        // no gas should be used - no txs
        assert_eq!(expected_block.gas_used, 0);
        // difficulty should be 0 to indicate first (and only) block from round
        assert_eq!(expected_block.difficulty, U256::ZERO);
        // assert extra data is empty 32-bytes (B256::ZERO)
        assert_eq!(expected_block.extra_data.as_ref(), &[0; 32]);
        // assert withdrawals are empty
        //
        // TODO: this is currently always empty
        assert_eq!(expected_block.withdrawals_root, genesis_header.withdrawals_root);

        Ok(())
    }

    /// Test the engine shuts down after the sending half of the broadcast channel is closed.
    ///
    /// One output is queued (simulating output already received) in the engine and another is sent
    /// on the channel. Then, the sender is dropped and the engine task is started.
    ///
    /// Expected result:
    /// - engine receives last broadcast
    /// - engine processes queued output first
    /// - engine processes last broadcast second
    /// - engine has no more output in queue and gracefully shuts down
    ///
    /// NOTE: all batches are built with genesis as the parent. Building blocks from historic
    /// parents is currently valid.
    #[tokio::test]
    async fn test_queued_output_executes_after_sending_channel_closed() -> eyre::Result<()> {
        init_test_tracing();
        // create batches for consensus output
        let mut batches_1 = narwhal_test_utils::batches(4); // create 4 batches
        let mut batches_2 = narwhal_test_utils::batches(4); // create 4 batches

        // okay to clone these because they are only used to seed genesis, decode transactions, and
        // recover signers
        let all_batches = [batches_1.clone(), batches_2.clone()].concat();

        // use default genesis and seed accounts to execute batches
        let genesis = adiri_genesis();
        let (genesis, txs_by_block, signers_by_block) =
            seeded_genesis_from_random_batches(genesis, all_batches.iter());
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // create execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;
        let provider = execution_node.get_provider().await;
        let parent = chain.sealed_genesis_header();

        // execute batches to update headers with valid data
        let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

        // capture values from updated batches for assertions later
        let mut batch_headers = vec![];

        // updated batches separately because they are mutated in-place
        // and need to be passed to different outputs
        //
        // update first round
        for (idx, batch) in batches_1.iter_mut().enumerate() {
            // increase basefee
            inc_base_fee += idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        // update second round
        for (idx, batch) in batches_2.iter_mut().enumerate() {
            // continue increasing basefee
            // add 4 to continue where previous round left off
            // this makes assertions easier at the end
            inc_base_fee += 4 + idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput` certificate
        let timestamp = now();
        let mut leader_1 = Certificate::default();
        // update timestamp
        leader_1.update_created_at_for_test(timestamp);
        let sub_dag_index_1 = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let mut batch_digests_1: VecDeque<BlockHash> =
            batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = Arc::new(CommittedSubDag::new(
            vec![Certificate::default()],
            leader_1,
            sub_dag_index_1,
            reputation_scores,
            previous_sub_dag,
        ));
        let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
            .expect("beneficiary address from str");
        let consensus_output_1 = ConsensusOutput {
            sub_dag: subdag_1.clone(),
            blocks: vec![batches_1],
            beneficiary: beneficiary_1,
            block_digests: batch_digests_1.clone(),
        };

        // create second output
        let mut leader_2 = Certificate::default();
        // update timestamp
        leader_2.update_created_at_for_test(timestamp + 2);
        let sub_dag_index_2 = 2;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = Some(subdag_1.as_ref());
        let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
        let subdag_2 = CommittedSubDag::new(
            vec![Certificate::default()],
            leader_2,
            sub_dag_index_2,
            reputation_scores,
            previous_sub_dag,
        )
        .into();
        let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
            .expect("beneficiary address from str");
        let consensus_output_2 = ConsensusOutput {
            sub_dag: subdag_2,
            blocks: vec![batches_2],
            beneficiary: beneficiary_2,
            block_digests: batch_digests_2.clone(),
        };

        // combine VecDeque and convert to Vec for assertions later
        batch_digests_1.extend(batch_digests_2);
        let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

        //=== Execution

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = execution_node.get_evm_config().await;
        let max_round = None;
        let parent = chain.sealed_genesis_header();

        let mut engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            max_round,
            consensus_output_stream,
            parent,
        );

        // queue the first output - simulate already received from channel
        engine.queued.push_back(consensus_output_1.clone());

        // send second output
        let broadcast_result = to_engine.send(consensus_output_2.clone());
        assert!(broadcast_result.is_ok());

        // drop sending channel before receiver has a chance to process message
        drop(to_engine);

        // channels for engine shutting down
        let (tx, rx) = oneshot::channel();

        // spawn engine task
        //
        // one output already queued up, one output waiting in broadcast stream
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = blockchain.last_block_number()?;
        let canonical_tip = blockchain.canonical_tip();
        let final_block = blockchain.finalized_block_num_hash()?.expect("finalized block");

        debug!("last block num {last_block_num:?}");
        debug!("canonical tip: {canonical_tip:?}");
        debug!("final block num {final_block:?}");

        let chain_info = blockchain.chain_info()?;
        debug!("chain info:\n{chain_info:?}");

        let expected_block_height = 8;
        // assert all 8 batches were executed
        assert_eq!(last_block_num, expected_block_height);
        // assert canonical tip and finalized block are equal
        assert_eq!(canonical_tip, final_block);
        // assert last executed output is correct and finalized
        let last_output = execution_node.last_executed_output().await?;
        assert_eq!(last_output, sub_dag_index_2); // round of consensus

        // pull newly executed blocks from database (skip genesis)
        //
        // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
        // to construct blocks from the following inputs:
        //     – Header
        //     - Transactions
        //     – Ommers
        //     – Withdrawals
        //     – Requests
        //     – Senders
        let executed_blocks = provider.block_with_senders_range(1..=expected_block_height)?;
        assert_eq!(expected_block_height, executed_blocks.len() as u64);

        // basefee intentionally increased with loop
        let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
        let output_digest_1: B256 = consensus_output_1.digest().into();
        let output_digest_2: B256 = consensus_output_2.digest().into();

        // assert blocks are executed as expected
        for (idx, txs) in txs_by_block.iter().enumerate() {
            let block = &executed_blocks[idx];
            let signers = &signers_by_block[idx];
            assert_eq!(&block.senders, signers);
            assert_eq!(&block.body, txs);

            // basefee was increased for each batch
            expected_base_fee += idx as u64;
            // assert basefee is same as worker's block
            assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

            // define re-usable variable here for asserting all values against expected output
            let mut expected_output = &consensus_output_1;
            let mut expected_beneficiary = &beneficiary_1;
            let mut expected_subdag_index = &sub_dag_index_1;
            let mut expected_parent_beacon_block_root = &output_digest_1;
            let mut expected_batch_index = idx;

            // update values based on index for all assertions below
            if idx >= 4 {
                // use different output for last 4 blocks
                expected_output = &consensus_output_2;
                expected_beneficiary = &beneficiary_2;
                expected_subdag_index = &sub_dag_index_2;
                expected_parent_beacon_block_root = &output_digest_2;
                // takeaway 4 to compensate for independent loops for executing batches
                expected_batch_index = idx - 4;
            }

            // beneficiary overwritten
            assert_eq!(&block.beneficiary, expected_beneficiary);
            // nonce matches subdag index and method all match
            assert_eq!(&block.nonce, expected_subdag_index);
            assert_eq!(block.nonce, expected_output.nonce());

            // ommers root
            assert_eq!(
                block.header.ommers_hash,
                proofs::calculate_ommers_root(&expected_output.ommers())
            );
            // timestamp
            assert_eq!(block.timestamp, expected_output.committed_at());
            // parent beacon block root is output digest
            assert_eq!(block.parent_beacon_block_root, Some(*expected_parent_beacon_block_root));

            // assert information from batch headers
            let proposed_header = &batch_headers[idx];

            if idx == 0 {
                // first block's parent is expected to be genesis
                assert_eq!(block.parent_hash, chain.genesis_hash());
                // expect header number +1 for batch bc of genesis
                assert_eq!(block.number, proposed_header.number);
            } else {
                assert_ne!(block.parent_hash, proposed_header.parent_hash);
                // TODO: this is inefficient
                //
                // assert parents executed in order (sanity check)
                let expected_parent = executed_blocks[idx - 1].header.hash_slow();
                assert_eq!(block.parent_hash, expected_parent);
                // expect block numbers NOT the same as batch's headers
                assert_ne!(block.number, proposed_header.number);
            }

            // expect state roots to be different bc worker uses ZERO
            assert_ne!(block.state_root, proposed_header.state_root);

            // mix hash is xor worker block's hash and consensus output digest
            let expected_mix_hash = proposed_header.hash() ^ *expected_parent_beacon_block_root;
            assert_eq!(block.mix_hash, expected_mix_hash);
            // bloom expected to be the same bc all proposed transactions should be good
            // ie) no duplicates, etc.
            //
            // TODO: randomly generate contract transactions as well!!!
            assert_eq!(block.logs_bloom, proposed_header.logs_bloom);
            // gas limit should come from batch
            //
            // TODO: ensure batch validation prevents peer workers from changing this value
            assert_eq!(block.gas_limit, proposed_header.gas_limit);
            // difficulty should match the batch's index within consensus output
            assert_eq!(block.difficulty, U256::from(expected_batch_index));
            // assert batch digest match extra data
            assert_eq!(&block.extra_data, all_batch_digests[idx].as_slice());
            // assert batch's withdrawals match
            //
            // TODO: this is currently always empty
            assert_eq!(block.withdrawals_root, proposed_header.withdrawals_root);
        }

        Ok(())
    }

    /// Test the engine successfully executes a duplicate batch (duplicate transactions);
    ///
    /// Expected result:
    /// - engine receives output with duplicate transactions
    /// - engine produces empty block for duplicate batch
    /// - engine has no more output in queue and gracefully shuts down
    ///
    /// NOTE: all batches are built with genesis as the parent. Building blocks from historic
    /// parents is currently valid.
    #[tokio::test]
    async fn test_execution_succeeds_with_duplicate_transactions() -> eyre::Result<()> {
        init_test_tracing();
        // create batches for consensus output
        let mut batches_1 = narwhal_test_utils::batches(4); // create 4 batches
        let mut batches_2 = narwhal_test_utils::batches(4); // create 4 batches

        // duplicate transactions in last batch for each round
        //
        // simulate duplicate batches from same round
        // and
        // duplicate transactions from a previous round
        batches_1[3] = batches_1[0].clone();
        batches_2[3] = batches_1[1].clone();

        // okay to clone these because they are only used to seed genesis, decode transactions, and
        // recover signers
        let all_batches = [batches_1.clone(), batches_2.clone()].concat();

        // use default genesis and seed accounts to execute batches
        let genesis = adiri_genesis();
        let (genesis, txs_by_block, signers_by_block) =
            seeded_genesis_from_random_batches(genesis, all_batches.iter());
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // create execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;
        let provider = execution_node.get_provider().await;
        let parent = chain.sealed_genesis_header();

        // execute batches to update headers with valid data
        let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

        // capture values from updated batches for assertions later
        let mut batch_headers = vec![];

        // updated batches separately because they are mutated in-place
        // and need to be passed to different outputs
        //
        // update first round
        for (idx, batch) in batches_1.iter_mut().enumerate() {
            // increase basefee
            inc_base_fee += idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        // update second round
        for (idx, batch) in batches_2.iter_mut().enumerate() {
            // continue increasing basefee
            // add 4 to continue where previous round left off
            // this makes assertions easier at the end
            inc_base_fee += 4 + idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        // store ref as variable for clarity
        let duplicated_batch_for_round_1 = &batches_1[0];
        let duplicated_batch_for_round_2 = &batches_1[1];
        let duplicate_batch_round_1 = &batches_1[3];
        let duplicate_batch_round_2 = &batches_2[3];

        // assert duplicate txs are same, but batches are different
        //
        // round 1
        assert_eq!(
            duplicate_batch_round_1.transactions(),
            duplicated_batch_for_round_1.transactions()
        );
        assert_ne!(duplicate_batch_round_1, duplicated_batch_for_round_1);
        // round 2
        assert_eq!(
            duplicate_batch_round_2.transactions(),
            duplicated_batch_for_round_2.transactions()
        );
        assert_ne!(duplicate_batch_round_2, duplicated_batch_for_round_2);

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        let timestamp = now();
        let mut leader_1 = Certificate::default();
        // update timestamp
        leader_1.update_created_at_for_test(timestamp);
        let sub_dag_index_1 = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let mut batch_digests_1: VecDeque<BlockHash> =
            batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = Arc::new(CommittedSubDag::new(
            vec![Certificate::default()],
            leader_1,
            sub_dag_index_1,
            reputation_scores,
            previous_sub_dag,
        ));
        let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
            .expect("beneficiary address from str");
        let consensus_output_1 = ConsensusOutput {
            sub_dag: subdag_1.clone(),
            blocks: vec![batches_1],
            beneficiary: beneficiary_1,
            block_digests: batch_digests_1.clone(),
        };

        // create second output
        let mut leader_2 = Certificate::default();
        // update timestamp
        leader_2.update_created_at_for_test(timestamp + 2);
        let sub_dag_index_2 = 2;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = Some(subdag_1.as_ref());
        let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
        let subdag_2 = CommittedSubDag::new(
            vec![Certificate::default()],
            leader_2,
            sub_dag_index_2,
            reputation_scores,
            previous_sub_dag,
        )
        .into();
        let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
            .expect("beneficiary address from str");
        let consensus_output_2 = ConsensusOutput {
            sub_dag: subdag_2,
            blocks: vec![batches_2],
            beneficiary: beneficiary_2,
            block_digests: batch_digests_2.clone(),
        };

        // combine VecDeque and convert to Vec for assertions later
        batch_digests_1.extend(batch_digests_2);
        let all_batch_digests: Vec<BlockHash> = batch_digests_1.into();

        //=== Execution

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = execution_node.get_evm_config().await;
        let max_round = None;
        let parent = chain.sealed_genesis_header();

        let mut engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            max_round,
            consensus_output_stream,
            parent,
        );

        // queue the first output - simulate already received from channel
        engine.queued.push_back(consensus_output_1.clone());

        // send second output
        let broadcast_result = to_engine.send(consensus_output_2.clone());
        assert!(broadcast_result.is_ok());

        // drop sending channel before receiver has a chance to process message
        drop(to_engine);

        // channels for engine shutting down
        let (tx, rx) = oneshot::channel();

        // spawn engine task
        //
        // one output already queued up, one output waiting in broadcast stream
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = blockchain.last_block_number()?;
        let canonical_tip = blockchain.canonical_tip();
        let final_block = blockchain.finalized_block_num_hash()?.expect("finalized block");

        debug!("last block num {last_block_num:?}");
        debug!("canonical tip: {canonical_tip:?}");
        debug!("final block num {final_block:?}");

        let chain_info = blockchain.chain_info()?;
        debug!("chain info:\n{chain_info:?}");

        // expect 1 block per batch still, but 2 blocks will be empty because they contained
        // duplicate transactions
        let expected_block_height = 8;
        let expected_duplicate_block_num_round_1 = 4;
        let expected_duplicate_block_num_round_2 = 8;
        // assert all 8 batches were executed
        assert_eq!(last_block_num, expected_block_height);
        // assert canonical tip and finalized block are equal
        assert_eq!(canonical_tip, final_block);
        // assert last executed output is correct and finalized
        let last_output = execution_node.last_executed_output().await?;
        assert_eq!(last_output, sub_dag_index_2); // round of consensus

        // pull newly executed blocks from database (skip genesis)
        //
        // Uses the provided `headers_range` to get the headers for the range, and `assemble_block`
        // to construct blocks from the following inputs:
        //     – Header
        //     - Transactions
        //     – Ommers
        //     – Withdrawals
        //     – Requests
        //     – Senders
        let executed_blocks = provider.block_with_senders_range(1..=expected_block_height)?;
        assert_eq!(expected_block_height, executed_blocks.len() as u64);

        // basefee intentionally increased with loop
        let mut expected_base_fee = MIN_PROTOCOL_BASE_FEE;
        let output_digest_1: B256 = consensus_output_1.digest().into();
        let output_digest_2: B256 = consensus_output_2.digest().into();

        // assert blocks are execute as expected
        for (idx, txs) in txs_by_block.iter().enumerate() {
            let block = &executed_blocks[idx];
            let signers = &signers_by_block[idx];
            // assert information from batch headers
            let proposed_header = &batch_headers[idx];

            // expect blocks 4 and 8 to be empty (no txs bc they are duplicates)
            // sub 1 to account for loop idx starting at 0
            if idx == expected_duplicate_block_num_round_1 - 1
                || idx == expected_duplicate_block_num_round_2 - 1
            {
                assert!(block.senders.is_empty());
                assert!(block.body.is_empty());
                // gas used should NOT be the same as bc duplicate transaction are ignored
                assert_ne!(block.gas_used, proposed_header.gas_used);
                // gas used should be zero bc all transactions were duplicates
                assert_eq!(block.gas_used, 0);
            } else {
                assert_eq!(&block.senders, signers);
                assert_eq!(&block.body, txs);
            }

            // basefee was increased for each batch
            expected_base_fee += idx as u64;
            // assert basefee is same as worker's block
            assert_eq!(block.base_fee_per_gas, Some(expected_base_fee));

            // define re-usable variable here for asserting all values against expected output
            let mut expected_output = &consensus_output_1;
            let mut expected_beneficiary = &beneficiary_1;
            let mut expected_subdag_index = &sub_dag_index_1;
            let mut expected_parent_beacon_block_root = &output_digest_1;
            let mut expected_batch_index = idx;

            // update values based on index for all assertions below
            if idx >= 4 {
                // use different output for last 4 blocks
                expected_output = &consensus_output_2;
                expected_beneficiary = &beneficiary_2;
                expected_subdag_index = &sub_dag_index_2;
                expected_parent_beacon_block_root = &output_digest_2;
                // takeaway 4 to compensate for independent loops for executing batches
                expected_batch_index = idx - 4;
            }

            // beneficiary overwritten
            assert_eq!(&block.beneficiary, expected_beneficiary);
            // nonce matches subdag index and method all match
            assert_eq!(&block.nonce, expected_subdag_index);
            assert_eq!(block.nonce, expected_output.nonce());

            // ommers root
            assert_eq!(
                block.header.ommers_hash,
                proofs::calculate_ommers_root(&expected_output.ommers())
            );
            // timestamp
            assert_eq!(block.timestamp, expected_output.committed_at());
            // parent beacon block root is output digest
            assert_eq!(block.parent_beacon_block_root, Some(*expected_parent_beacon_block_root));

            if idx == 0 {
                // first block's parent is expected to be genesis
                assert_eq!(block.parent_hash, chain.genesis_hash());
                // expect header number +1 for batch bc of genesis
                assert_eq!(block.number, proposed_header.number);
            } else {
                assert_ne!(block.parent_hash, proposed_header.parent_hash);
                // TODO: this is inefficient
                //
                // assert parents executed in order (sanity check)
                let expected_parent = executed_blocks[idx - 1].header.hash_slow();
                assert_eq!(block.parent_hash, expected_parent);
                // expect block numbers NOT the same as batch's headers
                assert_ne!(block.number, proposed_header.number);
            }

            // expect state roots to be different bc worker uses ZERO
            assert_ne!(block.state_root, proposed_header.state_root);

            // mix hash is xor worker block's hash and consensus output digest
            let expected_mix_hash = proposed_header.hash() ^ *expected_parent_beacon_block_root;
            assert_eq!(block.mix_hash, expected_mix_hash);
            // bloom expected to be the same bc all proposed transactions should be good
            // ie) no duplicates, etc.
            //
            // TODO: this doesn't actually test anything bc there are no contract txs
            assert_eq!(block.logs_bloom, proposed_header.logs_bloom);
            // gas limit should come from batch
            //
            // TODO: ensure batch validation prevents peer workers from changing this value
            assert_eq!(block.gas_limit, proposed_header.gas_limit);
            // difficulty should match the batch's index within consensus output
            assert_eq!(block.difficulty, U256::from(expected_batch_index));
            // assert batch digest match extra data
            assert_eq!(&block.extra_data, all_batch_digests[idx].as_slice());
            // assert batch's withdrawals match
            //
            // TODO: this is currently always empty
            assert_eq!(block.withdrawals_root, proposed_header.withdrawals_root);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_max_round_terminates_early() -> eyre::Result<()> {
        init_test_tracing();
        // create batches for consensus output
        let mut batches_1 = narwhal_test_utils::batches(4); // create 4 batches
        let mut batches_2 = narwhal_test_utils::batches(4); // create 4 batches

        // okay to clone these because they are only used to seed genesis, decode transactions, and
        // recover signers
        let all_batches = [batches_1.clone(), batches_2.clone()].concat();

        // use default genesis and seed accounts to execute batches
        let genesis = adiri_genesis();
        let (genesis, _txs_by_block, _signers_by_block) =
            seeded_genesis_from_random_batches(genesis, all_batches.iter());
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // create execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;
        let parent = chain.sealed_genesis_header();

        // execute batches to update headers with valid data
        let mut inc_base_fee = MIN_PROTOCOL_BASE_FEE;

        // capture values from updated batches for assertions later
        let mut batch_headers = vec![];

        // updated batches separately because they are mutated in-place
        // and need to be passed to different outputs
        //
        // update first round
        for (idx, batch) in batches_1.iter_mut().enumerate() {
            // increase basefee
            inc_base_fee += idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        // update second round
        for (idx, batch) in batches_2.iter_mut().enumerate() {
            // continue increasing basefee
            // add 4 to continue where previous round left off
            // this makes assertions easier at the end
            inc_base_fee += 4 + idx as u64;

            // this is the only way to do this right now
            let mut header = batch.sealed_header().clone().unseal();

            // update basefee and set beneficiary
            header.beneficiary = Address::random();
            header.base_fee_per_gas = Some(inc_base_fee);

            // okay to use bad hash bc execution executing block will seal slowly
            let updated = header.seal(B256::ZERO);
            batch.update_header(updated);

            // actually execute the block now
            execute_test_worker_block(batch, &parent);
            debug!("{idx}\n{:?}\n", batch);

            // store values for assertions later
            let header = batch.sealed_header().clone();
            batch_headers.push(header);
        }

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput` certificate
        let timestamp = now();
        let mut leader_1 = Certificate::default();
        // update timestamp
        leader_1.update_created_at_for_test(timestamp);
        let sub_dag_index_1 = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batch_digests_1: VecDeque<BlockHash> = batches_1.iter().map(|b| b.digest()).collect();
        let subdag_1 = Arc::new(CommittedSubDag::new(
            vec![Certificate::default()],
            leader_1,
            sub_dag_index_1,
            reputation_scores,
            previous_sub_dag,
        ));
        let beneficiary_1 = Address::from_str("0x1111111111111111111111111111111111111111")
            .expect("beneficiary address from str");
        let consensus_output_1 = ConsensusOutput {
            sub_dag: subdag_1.clone(),
            blocks: vec![batches_1],
            beneficiary: beneficiary_1,
            block_digests: batch_digests_1,
        };

        // create second output
        let mut leader_2 = Certificate::default();
        // update timestamp
        leader_2.update_created_at_for_test(timestamp + 2);
        let sub_dag_index_2 = 2;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = Some(subdag_1.as_ref());
        let batch_digests_2: VecDeque<BlockHash> = batches_2.iter().map(|b| b.digest()).collect();
        let subdag_2 = CommittedSubDag::new(
            vec![Certificate::default()],
            leader_2,
            sub_dag_index_2,
            reputation_scores,
            previous_sub_dag,
        )
        .into();
        let beneficiary_2 = Address::from_str("0x2222222222222222222222222222222222222222")
            .expect("beneficiary address from str");
        let consensus_output_2 = ConsensusOutput {
            sub_dag: subdag_2,
            blocks: vec![batches_2],
            beneficiary: beneficiary_2,
            block_digests: batch_digests_2,
        };

        //=== Execution

        let (_to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = execution_node.get_evm_config().await;
        // set max round to "1" - this should receive both digests, but stop after the first round
        let max_round = Some(1);
        let parent = chain.sealed_genesis_header();

        let mut engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            max_round,
            consensus_output_stream,
            parent,
        );

        // queue both output - simulate already received from channel
        engine.queued.push_back(consensus_output_1);
        engine.queued.push_back(consensus_output_2);

        // NOTE: sending channel is NOT dropped in this test, so engine should continue listening
        // until max block reached

        // channels for engine shutting down
        let (tx, rx) = oneshot::channel();

        // spawn engine task
        //
        // one output already queued up, one output waiting in broadcast stream
        executor.spawn_blocking(async move {
            let res = engine.await;
            let _ = tx.send(res);
        });

        let engine_task = timeout(Duration::from_secs(10), rx).await?;
        assert!(engine_task.is_ok());

        let last_block_num = blockchain.last_block_number()?;
        let canonical_tip = blockchain.canonical_tip();
        let final_block = blockchain.finalized_block_num_hash()?.expect("finalized block");

        debug!("last block num {last_block_num:?}");
        debug!("canonical tip: {canonical_tip:?}");
        debug!("final block num {final_block:?}");

        let chain_info = blockchain.chain_info()?;
        debug!("chain info:\n{chain_info:?}");

        let expected_block_height = 4;
        // assert all 4 batches were executed from round 1
        assert_eq!(last_block_num, expected_block_height);
        // assert canonical tip and finalized block are equal
        assert_eq!(canonical_tip, final_block);
        // assert last executed output is correct and finalized
        let last_output = execution_node.last_executed_output().await?;
        assert_eq!(last_output, 1);

        Ok(())
    }
}
