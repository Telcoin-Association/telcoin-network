//! Execute output from consensus layer to extend the canonical chain.
//!
//! The engine listens to a stream of output from consensus and constructs a new block.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

mod error;
mod handle;
mod payload_builder;
use error::EngineResult;
use futures::{Future, StreamExt};
use futures_util::{future::BoxFuture, FutureExt};
pub use payload_builder::execute_consensus_output;
use reth_blockchain_tree::BlockchainTreeEngine;
use reth_evm::ConfigureEvm;
use reth_primitives::{BlockNumber, SealedHeader};
use reth_provider::{
    BlockIdReader, BlockReader, CanonChainTracker, ChainSpecProvider, StageCheckpointReader,
    StateProviderFactory,
};
use reth_stages::PipelineEvent;
use reth_tasks::TaskSpawner;
use reth_tokio_util::EventStream;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};
use tn_types::{BuildArguments, ConsensusOutput};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{error, info, warn};

/// The TN consensus engine is responsible executing state that has reached consensus.
pub struct ExecutorEngine<BT, CE, Tasks> {
    /// The backlog of output from consensus that's ready to be executed.
    queued: VecDeque<ConsensusOutput>,
    /// Single active future that inserts a new block into `storage`
    // insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    insert_task: Option<BoxFuture<'static, EngineResult<SealedHeader>>>,
    // /// Used to notify consumers of new blocks
    // canon_state_notification: CanonStateNotificationSender,
    /// The type used to query both the database and the blockchain tree.
    blockchain: BT,
    /// EVM configuration for executing transactions and building blocks.
    evm_config: CE,
    /// The task executor to spawn new builds.
    executor: Tasks,
    /// Optional round of consensus to finish executing before then returning. The value is used to
    /// track the subdag index from consensus output. The index is included in executed blocks as
    /// the `nonce` value.
    ///
    /// note: this is used for debugging and testing
    max_block: Option<u64>,
    /// The pipeline events to listen on
    pipeline_events: Option<EventStream<PipelineEvent>>,
    /// Receiving end from CL's `Executor`. The `ConsensusOutput` is sent
    /// to the mining task here.
    consensus_output_stream: BroadcastStream<ConsensusOutput>,
    /// The [SealedHeader] of the last fully-executed block.
    ///
    /// This information is reflects the current finalized block number and hash.
    parent_header: SealedHeader,
}

impl<BT, CE, Tasks> ExecutorEngine<BT, CE, Tasks>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + ChainSpecProvider
        + 'static,
    CE: ConfigureEvm,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    /// Create a new instance of the [`ExecutorEngine`] using the given channel to configure
    /// the [`ConsensusOutput`] communication channel.
    ///
    /// By default the engine is started with idle pipeline.
    /// The pipeline can be launched immediately in one of the following ways descending in
    /// priority:
    /// - Explicit [`Option::Some`] target block hash provided via a constructor argument.
    /// - The process was previously interrupted amidst the pipeline run. This is checked by
    ///   comparing the checkpoints of the first ([`StageId::Headers`]) and last
    ///   ([`StageId::Finish`]) stages. In this case, the latest available header in the database is
    ///   used as the target.
    ///
    /// Propagates any database related error.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        blockchain: BT,
        evm_config: CE,
        executor: Tasks,
        max_block: Option<BlockNumber>,
        consensus_output_stream: BroadcastStream<ConsensusOutput>,
        parent_header: SealedHeader,
        // hooks: EngineHooks,
    ) -> Self {
        // let event_sender = EventSender::default();
        // let handle = BeaconConsensusEngineHandle::new(to_engine, event_sender.clone());
        Self {
            queued: Default::default(),
            insert_task: None,
            blockchain,
            evm_config,
            executor,
            max_block,
            pipeline_events: None,
            consensus_output_stream,
            parent_header,
        }
    }
}

/// The [ExecutorEngine] is a future that loops through the following:
/// - first receive and add any messages from consensus to a queue
/// - ensure the previous task is complete, then start executing the next output from queue
/// - poll any pending tasks that are currently being executed
/// - return poll pending
///
/// If a task completes, the loop continues to poll for any new output from consensus then begins executing the next task.
///
/// If the broadcast stream is closed, the engine will attempt to execute all remaining tasks and output that is queued.
impl<BT, CE, Tasks> Future for ExecutorEngine<BT, CE, Tasks>
where
    BT: BlockchainTreeEngine
        + BlockReader
        + BlockIdReader
        + CanonChainTracker
        + StageCheckpointReader
        + StateProviderFactory
        + ChainSpecProvider
        + Clone
        + Unpin
        + 'static,
    CE: ConfigureEvm,
    Tasks: TaskSpawner + Clone + Unpin + 'static,
{
    type Output = EngineResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // main executes output from consensus
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
                    // the stream has ended
                    //
                    // this could indicate an error but it's also how the Primary signals engine to shutdown
                    info!(target: "tn::engine", "ConsensusOutput channel closed. Shutting down...");

                    // only return if there are no current tasks and the queue is empty
                    // otherwise, let the loop continue so any remaining tasks and queued output is executed
                    if this.insert_task.is_none() && this.queued.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                }

                Poll::Pending => { /* nothing to do */ }
            }

            // only insert task if there is none
            //
            // note: it's important that the previous consensus output finishes executing before
            // inserting the next task to ensure the parent sealed header is finalized
            if this.insert_task.is_none() {
                if this.queued.is_empty() {
                    // nothing to insert
                    break;
                }

                // ready to begin executing next round of consensus
                let output = this.queued.pop_front().expect("not empty");
                let provider = this.blockchain.clone();
                let evm_config = this.evm_config.clone();
                let parent = this.parent_header.clone();
                let build_args = BuildArguments::new(provider, output, parent);

                // TODO: should this be on a blocking thread?
                //      YES it should
                //
                // execute the consensus output
                this.insert_task =
                    Some(Box::pin(async move { execute_consensus_output(evm_config, build_args) }));
            }

            // poll pending task that is executing output
            if let Some(mut fut) = this.insert_task.take() {
                match fut.poll_unpin(cx) {
                    Poll::Ready(final_header) => {
                        // this.pipeline_events = events;
                        //
                        // TODO: broadcast tip?
                        //
                        // ensure no errors then continue
                        this.parent_header = final_header?;

                        // TODO: check max_block/max_round

                        // poll broadcast stream for next output
                        continue;
                    }
                    Poll::Pending => {
                        this.insert_task = Some(fut);
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

impl<BT, CE, Tasks> std::fmt::Debug for ExecutorEngine<BT, CE, Tasks> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorEngine")
            .field("queued", &self.queued.len())
            .field("insert_task", &self.insert_task.is_some())
            .field("max_block", &self.max_block)
            .field("parent_header", &self.parent_header)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use crate::ExecutorEngine;
    use fastcrypto::hash::Hash as _;
    use narwhal_test_utils::default_test_execution_node;
    use reth_blockchain_tree::BlockchainTreeViewer;
    use reth_chainspec::ChainSpec;
    use reth_node_ethereum::EthEvmConfig;
    use reth_primitives::{Address, GenesisAccount, TransactionSigned, U256};
    use reth_provider::{BlockIdReader, BlockNumReader};
    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;
    use std::{str::FromStr as _, sync::Arc, time::Duration};
    use tn_types::{
        adiri_chain_spec_arc, adiri_genesis, BatchAPI as _, Certificate, CommittedSubDag,
        ConsensusOutput, ReputationScores,
    };
    use tokio::{sync::oneshot, time::timeout};
    use tokio_stream::wrappers::BroadcastStream;
    use tracing::debug;

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
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let beneficiary = Address::from_str("0x0000002cbd23b783741e8d7fcf51e459b497e4a6")
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
            batches: Default::default(),
            beneficiary,
            batch_digests: Default::default(),
        };

        let chain = adiri_chain_spec_arc();

        // execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = EthEvmConfig::default();
        let max_block = None;
        let parent = chain.sealed_genesis_header();

        let engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            max_block,
            consensus_output_stream,
            parent,
        );

        // send output
        let broadcast_result = to_engine.send(consensus_output);
        assert!(broadcast_result.is_ok());

        // drop sending channel
        drop(to_engine);

        let (tx, rx) = oneshot::channel();

        // spawn engine task
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

        assert_eq!(canonical_tip, final_block);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_task_completes_after_sending_channel_closed() -> eyre::Result<()> {
        init_test_tracing();
        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batches = tn_types::test_utils::batches(4); // create 4 batches
        let batch_digests = batches.iter().map(|b| b.digest()).collect();
        let beneficiary = Address::from_str("0xdbdbdb2cbd23b783741e8d7fcf51e459b497e4a6")
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
            batches: vec![batches.clone()],
            beneficiary,
            batch_digests,
        };

        //=== Execution

        let genesis = adiri_genesis();

        // collect txs and addresses for later assertions
        let mut txs_in_output = vec![];
        let mut senders_in_output = vec![];

        let mut accounts_to_seed = Vec::new();
        for batch in batches.into_iter() {
            for tx in batch.transactions_owned() {
                let tx_signed = TransactionSigned::decode_enveloped(&mut tx.as_ref())
                    .expect("decode tx signed");
                let address = tx_signed.recover_signer().expect("signer recoverable");
                txs_in_output.push(tx_signed);
                senders_in_output.push(address);
                // fund account with 99mil TEL
                let account = (
                    address,
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000")
                            .expect("account balance is parsed"),
                    ),
                );
                accounts_to_seed.push(account);
            }
        }
        debug!("accounts to seed: {accounts_to_seed:?}");

        // genesis
        let genesis = genesis.extend_accounts(accounts_to_seed);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // execution node components
        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node =
            default_test_execution_node(Some(chain.clone()), None, executor.clone())?;

        let (to_engine, from_consensus) = tokio::sync::broadcast::channel(1);
        let consensus_output_stream = BroadcastStream::from(from_consensus);
        let blockchain = execution_node.get_provider().await;
        let evm_config = EthEvmConfig::default();
        let max_block = None;
        let parent = chain.sealed_genesis_header();

        let engine = ExecutorEngine::new(
            blockchain.clone(),
            evm_config,
            max_block,
            consensus_output_stream,
            parent,
        );

        // send output
        let broadcast_result = to_engine.send(consensus_output);
        assert!(broadcast_result.is_ok());

        // drop sending channel
        drop(to_engine);

        let (tx, rx) = oneshot::channel();

        // spawn engine task
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

        assert_eq!(canonical_tip, final_block);

        Ok(())
    }
}
