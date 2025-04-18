//! This crate is designed to encaptulate Reth functionality and abstract it away.
//! This should allow for easier upgrades.
//! It still re-exports some stuff and a few places use Reth directly but eventually
//! it all should go through this crate.

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

use crate::traits::TNExecution;
use alloy::{
    primitives::{address, Bytes},
    sol,
    sol_types::SolCall as _,
};
use clap::Parser;
use dirs::path_to_datadir;
use error::{TnRethError, TnRethResult};
use futures::StreamExt as _;
use jsonrpsee::Methods;
use reth::{
    args::{
        DatabaseArgs, DatadirArgs, DebugArgs, DevArgs, NetworkArgs, PayloadBuilderArgs,
        PruningArgs, RpcServerArgs, TxPoolArgs,
    },
    blockchain_tree::{
        BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
    },
    builder::NodeConfig,
    prometheus_exporter::install_prometheus_recorder,
    rpc::{
        builder::{config::RethRpcServerConfig, RpcModuleBuilder, TransportRpcModules},
        eth::EthApi,
        server_types::eth::utils::recover_raw_transaction as reth_recover_raw_transaction,
    },
};
use reth_blockchain_tree::{BlockValidationKind, BlockchainTreeEngine};
use reth_chainspec::BaseFeeParams;
use reth_consensus::FullConsensus;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_eth_wire::BlockHashNumber;
use reth_evm::{
    execute::{BlockExecutorProvider, Executor as _},
    ConfigureEvm, ConfigureEvmEnv as _,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_ethereum::{BasicBlockExecutorProvider, EthExecutionStrategyFactory};
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    BlockExecutionOutput, BlockIdReader as _, BlockNumReader, BlockReader, CanonChainTracker,
    CanonStateSubscriptions as _, ChainStateBlockReader, DatabaseProviderFactory,
    HeaderProvider as _, ProviderFactory, StateProviderBox, StateProviderFactory,
    TransactionVariant,
};
use reth_revm::{
    cached::CachedReads,
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, BundleState},
    interpreter::Host,
    primitives::{EVMError, EnvWithHandlerCfg, ResultAndState, TxEnv},
    Database, DatabaseCommit, Evm, State,
};
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use std::{
    net::SocketAddr,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
};

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, rpc::builder::RpcServerHandle,
};
pub use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_errors::{CanonicalError, ProviderError, RethError};
pub use reth_node_core::args::LogArgs;
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
pub use reth_provider::ExecutionOutcome;
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::FileWorkerGuard;
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction,
};

use tn_types::{
    adiri_chain_spec_arc, calculate_transaction_root, Address, Batch, Block, BlockBody,
    BlockExt as _, BlockHashOrNumber, BlockNumHash, BlockNumber, BlockWithSenders, ExecHeader,
    Genesis, Receipt, SealedBlock, SealedBlockWithSenders, SealedHeader, TaskManager,
    TransactionSigned, B256, EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS,
    EMPTY_WITHDRAWALS, U256,
};
use tokio::sync::mpsc::{self, unbounded_channel};
use tracing::{debug, info, warn};
use traits::{TNPayload, TelcoinNode, TelcoinNodeTypes as _};

pub mod dirs;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
use worker::WorkerNetwork;
pub mod error;
pub mod worker;

/// Rpc Server type, used for getting the node started.
pub type RpcServer = TransportRpcModules<()>;

/// The system address.
pub const SYSTEM_ADDRESS: Address = address!("fffffffffffffffffffffffffffffffffffffffe");

/// The address for consensus registry.
pub const CONSENSUS_REGISTRY_ADDRESS: Address =
    address!("7777777777777777777777777777777777777777");

sol! {
    /// Define the interface for your ConsensusRegistry contract
    function concludeEpoch(address[] calldata newCommittee) external;
}

/// Defaults for chain spec clap parser.
///
/// Wrapper to intercept "adiri" chain spec. If not adiri, try reth's genesis_value_parser.
pub fn clap_genesis_parser(value: &str) -> eyre::Result<Arc<RethChainSpec>, eyre::Error> {
    let chain = match value {
        "adiri" => adiri_chain_spec_arc(),
        _ => chain_value_parser(value)?,
    };

    Ok(chain)
}

/// Reth specific command line args.
#[derive(Debug, Parser, Clone)]
pub struct RethCommand {
    /// The chain this node is running.
    ///
    /// Possible values are either a built-in chain or the path to a chain specification file.
    ///
    /// Defaults to the custom
    #[arg(
        long,
        value_name = "CHAIN_OR_PATH",
        verbatim_doc_comment,
        default_value = "adiri",
        default_value_if("dev", "true", "adiri"),
        value_parser = clap_genesis_parser,
        required = false,
    )]
    pub chain: Arc<RethChainSpec>,

    /// Enable Prometheus execution metrics.
    ///
    /// The metrics will be served at the given interface and port.
    #[arg(long, value_name = "SOCKET", value_parser = parse_socket_address, help_heading = "Execution Metrics")]
    pub metrics: Option<SocketAddr>,

    /// All networking related arguments
    #[clap(flatten)]
    pub network: NetworkArgs,

    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All payload builder related arguments
    #[clap(flatten)]
    pub builder: PayloadBuilderArgs,

    /// All debug related arguments with --debug prefix
    #[clap(flatten)]
    pub debug: DebugArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,

    /// All dev related arguments with --dev prefix
    #[clap(flatten)]
    pub dev: DevArgs,

    /// All pruning related arguments
    #[clap(flatten)]
    pub pruning: PruningArgs,
    // All engine related arguments
    //#[clap(flatten)]
    //pub engine: EngineArgs,
}

/// A wrapper abstraction around a Reth node config.
#[derive(Clone, Debug)]
pub struct RethConfig(NodeConfig<RethChainSpec>);

impl RethConfig {
    /// Create a new RethConfig wrapper.
    pub fn new<P: AsRef<Path>>(
        reth_config: RethCommand,
        instance: u16,
        config: Option<PathBuf>,
        datadir: P,
        with_unused_ports: bool,
    ) -> Self {
        // create a reth DatadirArgs from tn datadir
        let datadir = path_to_datadir(datadir.as_ref());

        let RethCommand { chain, metrics, network, rpc, txpool, builder, debug, db, dev, pruning } =
            reth_config;

        let mut this = NodeConfig {
            config,
            chain,
            metrics,
            instance,
            datadir,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
        };
        if with_unused_ports {
            this = this.with_unused_ports();
        }
        // adjust rpc instance ports
        this.adjust_instance_ports();

        Self(this)
    }
}

/// This is a wrapped abstraction around Reth.
///
/// It should allow the telcoin app to access the required functionality without
/// leaking Reth internals all over the codebase (this makes staying up to date
/// VERY time consuming).
#[derive(Clone)]
pub struct RethEnv {
    /// The type that holds all information needed to launch the node's engine.
    ///
    /// The [NodeConfig] is reth-specific and holds many helper functions that
    /// help TN stay in-sync with the Ethereum community.
    node_config: NodeConfig<RethChainSpec>,
    /// Type that fetches data from the database.
    blockchain_provider: BlockchainProvider<TelcoinNode>,
    /// The Evm configuration type.
    evm_executor: BasicBlockExecutorProvider<EthExecutionStrategyFactory>,
    /// The type to configure the EVM for execution.
    evm_config: EthEvmConfig,
    /// The transaction pool.
    tx_pool: WorkerTxPool,
}

impl std::fmt::Debug for RethEnv {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RethEnv, config: {:?}", self.node_config)
    }
}

/// Wrapper for Reth ChainSpec, just a layer of abstraction.
#[derive(Clone, Debug)]
pub struct ChainSpec(Arc<RethChainSpec>);

impl ChainSpec {
    /// Return the contained Reth ChainSpec.
    pub(crate) fn reth_chain_spec(&self) -> RethChainSpec {
        (*self.0).clone()
    }

    /// Return a reference to the ChainSpec's genesis.
    pub fn genesis(&self) -> &Genesis {
        self.0.genesis()
    }

    /// Return the sealed header for genesis.
    pub fn sealed_genesis_header(&self) -> SealedHeader {
        self.0.sealed_genesis_header()
    }

    /// Return the sealed block for genesis.
    pub fn sealed_genesis_block(&self) -> SealedBlock {
        SealedBlock::new(self.0.sealed_genesis_header(), BlockBody::default())
    }
}

impl RethEnv {
    /// Produce a new wrapped Reth environment from a config, DB path and task manager.
    pub fn new<P: AsRef<Path>>(
        reth_config: &RethConfig,
        db_path: P,
        task_manager: &TaskManager,
    ) -> eyre::Result<Self> {
        let db_path = db_path.as_ref();
        // create node builders for Primary and Worker
        //
        // Register the prometheus recorder before creating the database,
        // then start metrics
        //
        // reth calls this in node command for CLI
        // to capture db startup metrics
        // metrics for TN are unrefined and outside the scope of this PR
        //
        // this is a best-guess attempt to capture data from the exectuion layer
        // but more work is needed to ensure proper metric collection
        let _ = install_prometheus_recorder();

        info!(target: "tn::reth", path = ?db_path, "opening database");
        let database = Arc::new(init_db(db_path, reth_config.0.db.database_args())?.with_metrics());
        let node_config = reth_config.0.clone();
        let (evm_executor, evm_config) = Self::init_evm_components(&node_config);
        let provider_factory = Self::init_provider_factory(&node_config, database)?;
        let blockchain_provider =
            Self::init_blockchain_provider(task_manager, &provider_factory, evm_executor.clone())?;
        let tx_pool =
            WorkerTxPool::new(&node_config, task_manager, &provider_factory, &blockchain_provider)?;
        Ok(Self { node_config, blockchain_provider, evm_config, evm_executor, tx_pool })
    }

    /// Create a new RethEnv for testing only.
    pub fn new_for_test_with_chain<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
    ) -> eyre::Result<Self> {
        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(db_path.as_ref().to_path_buf()),
                // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
                static_files_path: None,
            },
            chain,
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config);
        Self::new(&reth_config, db_path, task_manager)
    }

    /// Initialize the provider factory and related components
    /// Create a new RethEnv for testing only.
    pub fn new_for_test<P: AsRef<Path>>(
        db_path: P,
        task_manager: &TaskManager,
    ) -> eyre::Result<Self> {
        Self::new_for_test_with_chain(adiri_chain_spec_arc(), db_path, task_manager)
    }

    /// Initialize the provider factory and related components
    fn init_provider_factory(
        node_config: &NodeConfig<RethChainSpec>,
        database: Arc<DatabaseEnv>,
    ) -> eyre::Result<ProviderFactory<TelcoinNode>> {
        // Initialize provider factory with static files
        let datadir = node_config.datadir();
        let provider_factory = ProviderFactory::new(
            database,
            Arc::clone(&node_config.chain),
            StaticFileProvider::read_write(datadir.static_files())?,
        )
        .with_static_files_metrics();

        // Initialize genesis if needed
        let genesis_hash = init_genesis(&provider_factory)?;
        debug!(target: "tn::execution", chain=%node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        Ok(provider_factory)
    }

    /// Initialize the blockchain provider and tree
    fn init_blockchain_provider(
        task_manager: &TaskManager,
        provider_factory: &ProviderFactory<TelcoinNode>,
        evm_executor: BasicBlockExecutorProvider<EthExecutionStrategyFactory>,
    ) -> eyre::Result<BlockchainProvider<TelcoinNode>> {
        // Set up metrics listener
        let (sync_metrics_tx, sync_metrics_rx) = unbounded_channel();
        let sync_metrics_listener = reth_stages::MetricsListener::new(sync_metrics_rx);
        task_manager.spawn_task("stages metrics listener task", sync_metrics_listener);

        // Initialize consensus implementation
        let tn_execution: Arc<dyn FullConsensus> = Arc::new(TNExecution);

        // Set up blockchain tree
        let tree_config = BlockchainTreeConfig::default();
        let tree_externals =
            TreeExternals::new(provider_factory.clone(), tn_execution, evm_executor);
        let tree =
            BlockchainTree::new(tree_externals, tree_config)?.with_sync_metrics_tx(sync_metrics_tx);

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));
        let blockchain_db = BlockchainProvider::new(provider_factory.clone(), blockchain_tree)?;

        Ok(blockchain_db)
    }

    /// Initialize EVM components
    fn init_evm_components(
        node_config: &NodeConfig<RethChainSpec>,
    ) -> (BasicBlockExecutorProvider<EthExecutionStrategyFactory>, EthEvmConfig) {
        let evm_config = TelcoinNode::create_evm_config(Arc::clone(&node_config.chain));
        let evm_executor = TelcoinNode::create_executor(Arc::clone(&node_config.chain));

        (evm_executor, evm_config)
    }

    /// Return the transaction pool for this Reth instance.
    pub fn worker_txn_pool(&self) -> &WorkerTxPool {
        &self.tx_pool
    }

    /// Return a channel reciever that will return each canonical block in turn.
    pub fn canonical_block_stream(&self) -> mpsc::Receiver<SealedBlock> {
        let mut stream = self.blockchain_provider.canonical_state_stream();
        let (tx, rx) = mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(latest) = stream.next().await {
                let block = latest.tip().block.clone();
                if let Err(_e) = tx.send(block).await {
                    break;
                }
            }
        });
        rx
    }

    /// Return the chainspec for this instance.
    pub fn chainspec(&self) -> ChainSpec {
        ChainSpec(self.node_config.chain.clone())
    }

    /// Construct a canonical block from a worker's block that reached consensus.
    pub fn build_block_from_batch_payload(
        &self,
        payload: TNPayload,
        batch: Batch,
        consensus_header_hash: B256,
    ) -> TnRethResult<SealedBlockWithSenders> {
        let chain_spec = self.node_config.chain.clone();
        let state_provider = self
            .blockchain_provider
            .state_by_block_hash(payload.attributes.parent_header.hash())?;
        let state = StateProviderDatabase::new(state_provider);

        // NOTE: using same approach as reth here bc I can't find the State::builder()'s methods
        // I'm not sure what `with_bundle_update` does, and using `CachedReads` is the only way
        // I can get the state root section below to compile using `db.commit(state)`.
        //
        // consider creating `CachedReads` during batch validation?
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        debug!(target: "payload_builder", parent_hash = ?payload.attributes.parent_header.hash(), parent_number = payload.attributes.parent_header.number, "building new payload");
        // collect these totals to report at the end
        let mut cumulative_gas_used = 0;
        let mut total_fees = U256::ZERO;
        let mut executed_txs = Vec::new();
        let mut senders = Vec::new();
        let mut receipts = Vec::new();

        // initialize values for execution from block env
        //
        // note: uses the worker's sealed header for "parent" values
        // note the sealed header below is more or less junk but payload trait requires it.
        let (cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref());

        let block_gas_limit: u64 = block_env.gas_limit.try_into().unwrap_or(u64::MAX);
        let base_fee = block_env.basefee.to::<u64>();
        let block_number = block_env.number.to::<u64>();

        // // apply eip-4788 pre block contract call
        // pre_block_beacon_root_contract_call(
        //     &mut db,
        //     &chain_spec,
        //     block_number,
        //     &initialized_cfg,
        //     &initialized_block_env,
        //     &attributes,
        // )?;

        // // apply eip-2935 blockhashes update
        // apply_blockhashes_update(
        //     &mut db,
        //     &chain_spec,
        //     initialized_block_env.timestamp.to::<u64>(),
        //     block_number,
        //     parent_header.hash(),
        // )
        // .map_err(|err| PayloadBuilderError::Internal(err.into()))?;

        // TODO: parallelize tx recovery when it's worth it (see
        // TransactionSigned::recover_signers())

        let env =
            EnvWithHandlerCfg::new_with_cfg_env(cfg.clone(), block_env.clone(), TxEnv::default());
        let mut evm = self.evm_config.evm_with_env(&mut db, env);

        for tx_bytes in &batch.transactions {
            let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                .inspect_err(|e| {
                    tracing::error!(
                    target: "engine",
                    batch=?batch.digest(),
                    ?tx_bytes,
                    "failed to recover signer: {e}")
                })?;

            // Configure the environment for the tx.
            *evm.tx_mut() = self.evm_config.tx_env(recovered.tx(), recovered.signer());

            let ResultAndState { result, state } = match evm.transact() {
                Ok(res) => res,
                Err(err) => {
                    match err {
                        // allow transaction errors (ie - duplicates)
                        //
                        // it's possible that another worker's batch included this transaction
                        EVMError::Transaction(err) => {
                            warn!(target: "engine", tx_hash=?recovered.hash(), ?err);
                            continue;
                        }
                        err => {
                            // this is an error that we should treat as fatal
                            // - invalid header resulting from misconfigured BlockEnv
                            // - Database error
                            // - custom error (unsure)
                            return Err(err.into());
                        }
                    }
                }
            };

            // commit changes
            evm.db_mut().commit(state);

            let gas_used = result.gas_used();

            // add gas used by the transaction to cumulative gas used, before creating the receipt
            cumulative_gas_used += gas_used;

            // Push transaction changeset and calculate header bloom filter for receipt.
            receipts.push(Some(Receipt {
                tx_type: recovered.tx_type(),
                success: result.is_success(),
                cumulative_gas_used,
                logs: result.into_logs().into_iter().collect(),
            }));

            // update add to total fees
            let miner_fee = recovered
                .effective_tip_per_gas(Some(base_fee))
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);

            // append transaction to the list of executed transactions and keep signers
            senders.push(recovered.signer());
            executed_txs.push(recovered.into_tx());
        }

        // close epoch
        payload.attributes.close_epoch.then(|| self.apply_closing_epoch_contract_call(&mut evm));

        // Release db
        drop(evm);

        // TODO: logic for withdrawals
        //
        // let WithdrawalsOutcome { withdrawals_root, withdrawals } =
        //     commit_withdrawals(&mut db, &chain_spec, attributes.timestamp,
        // attributes.withdrawals)?;

        // merge all transitions into bundle state, this would apply the withdrawal balance changes
        // and 4788 contract call
        db.merge_transitions(BundleRetention::PlainState);

        let execution_outcome =
            ExecutionOutcome::new(db.take_bundle(), vec![receipts].into(), block_number, vec![]);
        let receipts_root =
            execution_outcome.ethereum_receipts_root(block_number).expect("Number is in range");
        let logs_bloom =
            execution_outcome.block_logs_bloom(block_number).expect("Number is in range");

        // calculate the state root
        let hashed_state = db.database.db.hashed_post_state(execution_outcome.state());
        let (state_root, _trie_output) = {
            db.database.inner().state_root_with_updates(hashed_state.clone()).inspect_err(
                |err| {
                    tracing::error!(target: "payload_builder",
                        parent_hash=%payload.attributes.parent_header.hash(),
                        %err,
                        "failed to calculate state root for payload"
                    );
                },
            )?
        };

        // create the block header
        let transactions_root = calculate_transaction_root(&executed_txs);

        let header = ExecHeader {
            parent_hash: payload.parent(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: block_env.coinbase,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root: Some(EMPTY_WITHDRAWALS),
            logs_bloom,
            timestamp: payload.timestamp(),
            mix_hash: payload.prev_randao(),
            nonce: payload.attributes.nonce.into(),
            base_fee_per_gas: Some(base_fee),
            number: payload.attributes.parent_header.number + 1, /* ensure this matches the block
                                                                  * env */
            gas_limit: block_gas_limit,
            difficulty: U256::from(payload.attributes.batch_index),
            gas_used: cumulative_gas_used,
            extra_data: payload.attributes.batch_digest.into(),
            parent_beacon_block_root: Some(consensus_header_hash),
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_hash: None,
        };

        // seal the block
        let withdrawals = Some(payload.withdrawals().clone());

        // seal the block
        let block = Block {
            header,
            body: BlockBody { transactions: executed_txs, ommers: vec![], withdrawals },
        };

        let sealed_block = block.seal_slow();
        let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, senders)
            .ok_or(TnRethError::SealBlockWithSenders)?;

        Ok(sealed_block_with_senders)
    }

    /// Extend the canonical tip with one block, despite no blocks from workers are included in the
    /// output from consensus.
    pub fn build_block_from_empty_payload(
        &self,
        payload: TNPayload,
        consensus_header_digest: B256,
    ) -> TnRethResult<SealedBlockWithSenders> {
        let chain_spec = self.node_config.chain.clone();
        let state = self
            .blockchain_provider
            .state_by_block_hash(payload.attributes.parent_header.hash())
            .map_err(|err| {
                warn!(target: "engine",
                    parent_hash=%payload.attributes.parent_header.hash(),
                    %err,
                    "failed to get state for empty output",
                );
                err
            })?;
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(StateProviderDatabase::new(state)))
            .with_bundle_update()
            .build();

        // initialize values for execution from block env
        //
        // use the parent's header bc there are no batches and the header arg is not used
        let (_cfg, block_env) = payload.cfg_and_block_env(chain_spec.as_ref());

        // merge all transitions into bundle state, this would apply the withdrawal balance
        // changes and 4788 contract call
        db.merge_transitions(BundleRetention::PlainState);

        // calculate the state root
        let bundle_state = db.take_bundle();

        // calculate the state root
        let hashed_state = db.database.db.hashed_post_state(&bundle_state);
        let (state_root, _trie_output) = {
            db.database.inner().state_root_with_updates(hashed_state.clone()).inspect_err(
                |err| {
                    tracing::error!(target: "payload_builder",
                        parent_hash=%payload.attributes.parent_header.hash(),
                        %err,
                        "failed to calculate state root for payload"
                    );
                },
            )?
        };

        let header = ExecHeader {
            parent_hash: payload.parent(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary: block_env.coinbase,
            state_root,
            transactions_root: EMPTY_TRANSACTIONS,
            receipts_root: EMPTY_RECEIPTS,
            withdrawals_root: Some(EMPTY_WITHDRAWALS),
            logs_bloom: Default::default(),
            timestamp: payload.timestamp(),
            mix_hash: payload.prev_randao(),
            nonce: payload.attributes.nonce.into(),
            base_fee_per_gas: Some(payload.attributes.base_fee_per_gas),
            number: payload.attributes.parent_header.number + 1, /* ensure this matches the block
                                                                  * env */
            gas_limit: payload.attributes.gas_limit,
            difficulty: U256::ZERO, // batch index
            gas_used: 0,
            extra_data: payload.attributes.batch_digest.into(),
            parent_beacon_block_root: Some(consensus_header_digest),
            blob_gas_used: None,
            excess_blob_gas: None,
            requests_hash: None,
        };

        // seal the block
        let withdrawals = Some(payload.withdrawals().clone());

        // seal the block
        let block =
            Block { header, body: BlockBody { transactions: vec![], ommers: vec![], withdrawals } };

        let sealed_block = block.seal_slow();

        let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, vec![])
            .ok_or(TnRethError::SealBlockWithSenders)?;

        Ok(sealed_block_with_senders)
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata(&self) -> Bytes {
        // shuffle all validators for new committee
        let new_committee = self.shuffle_new_committee();

        // encode the call to bytes with method selector and args
        concludeEpochCall { newCommittee: new_committee }.abi_encode().into()
    }

    #[inline]
    fn apply_closing_epoch_contract_call<EXT, DB>(
        &self,
        evm: &mut Evm<'_, EXT, DB>,
    ) -> TnRethResult<ResultAndState>
    where
        DB: Database,
        DB::Error: core::fmt::Display,
    {
        let prev_env = Box::new(evm.context.env().clone());
        let calldata = self.generate_conclude_epoch_calldata();

        // fill tx env to execute system call to consensus registry
        self.evm_config.fill_tx_env_system_contract_call(
            &mut evm.context.evm.env,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            // concludeEpoch(address[] calldata newCommittee)
            calldata,
        );

        let mut res = match evm.transact() {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                return Err(EVMError::Custom(format!("epoch closing execution failed: {e}")).into());
            }
        };

        // NOTE: revm marks these accounts as "touched" after the contract call
        // and includes them in the result
        //
        // remove the state changes for system call
        res.state.remove(&SYSTEM_ADDRESS);
        res.state.remove(&evm.block().coinbase);

        // restore the previous env
        evm.context.evm.env = prev_env;

        Ok(res)
    }

    /// Shuffle the committee from state.
    fn shuffle_new_committee(&self) -> Vec<Address> {
        todo!()
    }

    /// Adds block to the tree and skips state root validation.
    pub fn insert_block(
        &self,
        sealed_block_with_senders: SealedBlockWithSenders,
    ) -> TnRethResult<()> {
        self.blockchain_provider.insert_block(
            sealed_block_with_senders,
            BlockValidationKind::SkipStateRootValidation,
        )?;

        Ok(())
    }

    /// Finalize block (header) executed from consensus output and update chain info.
    ///
    /// This removes canonical blocks from the tree, stores the finalized block number in the
    /// database, but still need to set_finalized afterwards for utilization in-memory for
    /// components, like RPC
    pub fn finalize_block(&self, header: SealedHeader) -> TnRethResult<()> {
        // finalize the last block executed from consensus output and update chain info
        //
        // this removes canonical blocks from the tree, stores the finalized block number in the
        // database, but still need to set_finalized afterwards for utilization in-memory for
        // components, like RPC
        self.blockchain_provider.finalize_block(header.number)?;
        self.blockchain_provider.set_finalized(header.clone());

        // update safe block last because this is less time sensitive but still needs to happen
        self.blockchain_provider.set_safe(header);
        Ok(())
    }

    /// This makes all blocks canonical, commits them to the database,
    /// broadcasts new chain on `canon_state_notification_sender`
    /// and set last executed header as the tracked header.
    pub fn make_canonical(&self, header: SealedHeader) -> TnRethResult<()> {
        // NOTE: this makes all blocks canonical, commits them to the database,
        // and broadcasts new chain on `canon_state_notification_sender`
        //
        // the canon_state_notifications include every block executed in this round
        //
        // the worker's pool maintenance task subcribes to these events
        self.blockchain_provider.make_canonical(header.hash())?;

        // set last executed header as the tracked header
        //
        // see: reth/crates/consensus/beacon/src/engine/mod.rs:update_canon_chain
        self.blockchain_provider.set_canonical_head(header.clone());
        info!(target: "engine", "canonical head for round {:?}: {:?} - {:?}", <FixedBytes<8> as Into<u64>>::into(header.nonce), header.number, header.hash());
        Ok(())
    }

    /// Look up and return the sealed header for hash.
    pub fn sealed_header_by_hash(&self, hash: B256) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.blockchain_provider.sealed_header_by_hash(hash)?)
    }

    /// Look up and return the sealed header for block number.
    pub fn sealed_header_by_number(&self, number: u64) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.blockchain_provider.database_provider_ro()?.sealed_header(number)?)
    }

    /// Look up and return the sealed block for number.
    pub fn sealed_block_by_number(&self, number: u64) -> TnRethResult<Option<SealedBlock>> {
        Ok(self
            .blockchain_provider
            .sealed_block_with_senders(
                BlockHashOrNumber::Number(number),
                TransactionVariant::NoHash,
            )?
            .map(|b| b.block))
    }

    /// Look up and return the sealed header (with senders) for hash.
    pub fn sealed_block_with_senders(
        &self,
        id: BlockHashOrNumber,
    ) -> TnRethResult<Option<SealedBlockWithSenders>> {
        Ok(self.blockchain_provider.sealed_block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Look up and return the sealed block for hash or number.
    pub fn block_with_senders(
        &self,
        id: BlockHashOrNumber,
    ) -> TnRethResult<Option<BlockWithSenders>> {
        Ok(self.blockchain_provider.block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Return the blocks with senders for a range of block numbers.
    pub fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<BlockWithSenders>> {
        Ok(self.blockchain_provider.block_with_senders_range(range)?)
    }

    /// Return the head header from the reth db.
    pub fn lookup_head(&self) -> TnRethResult<SealedHeader> {
        let head = self.node_config.lookup_head(&self.blockchain_provider)?;
        let header = self
            .blockchain_provider
            .sealed_header(head.number)?
            .expect("Failed to retrieve sealed header from head's block number");
        Ok(header)
    }

    /// If a dubug max round is set then return it.
    pub fn get_debug_max_round(&self) -> Option<u64> {
        self.node_config.debug.max_block
    }

    /// Helper to get the gas price based on the provider's latest header.
    pub fn get_gas_price(&self) -> TnRethResult<u128> {
        let header = self.lookup_head()?;
        Ok(header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into())
    }

    /// Return the execution header for hash if available.
    pub fn header(&self, hash: B256) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.blockchain_provider.header(&hash)?)
    }

    /// Return the execution header for block number if available.
    pub fn header_by_number(&self, block_num: u64) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.blockchain_provider.database_provider_ro()?.header_by_number(block_num)?)
    }

    /// Return the finalalized execution header if available.
    pub fn finalized_header(&self) -> TnRethResult<Option<ExecHeader>> {
        let finalized_block_num_hash =
            self.blockchain_provider.finalized_block_num_hash().unwrap_or_default();
        if let Some(finalized_block_num_hash) = finalized_block_num_hash {
            Ok(self.blockchain_provider.header(&finalized_block_num_hash.hash)?)
        } else {
            Ok(None)
        }
    }

    /// Return the latest canonical block number.
    pub fn last_block_number(&self) -> TnRethResult<u64> {
        Ok(self.blockchain_provider.database_provider_ro()?.last_block_number().unwrap_or(0))
    }

    /// Return the block number and hash for the current canonical tip.
    pub fn canonical_tip(&self) -> BlockNumHash {
        use reth_blockchain_tree::BlockchainTreeViewer;
        self.blockchain_provider.canonical_tip()
    }

    /// If available return the finalized block number and hash.
    pub fn finalized_block_num_hash(&self) -> TnRethResult<Option<BlockNumHash>> {
        Ok(self.blockchain_provider.finalized_block_num_hash()?)
    }

    /// Returns the block number of the last finialized block.
    pub fn last_finalized_block_number(&self) -> TnRethResult<u64> {
        Ok(self
            .blockchain_provider
            .database_provider_ro()?
            .last_finalized_block_number()?
            .unwrap_or(0))
    }

    /// Return the block number and hash of the finalized block.
    pub fn finalized_block_hash_number(&self) -> TnRethResult<BlockHashNumber> {
        let hash = self
            .blockchain_provider
            .finalized_block_hash()?
            .unwrap_or_else(|| self.node_config.chain.sealed_genesis_header().hash());
        let number = self.blockchain_provider.finalized_block_number()?.unwrap_or_default();
        Ok(BlockHashNumber { hash, number })
    }

    /// Build and return the RPC server for the instance.
    /// This probably needs better abstraction.
    pub fn get_rpc_server(
        &self,
        transaction_pool: WorkerTxPool,
        network: WorkerNetwork,
        task_manager: &TaskManager,
        other: impl Into<Methods>,
    ) -> RpcServer {
        let transaction_pool: EthTransactionPool<
            BlockchainProvider<TelcoinNode>,
            DiskFileBlobStore,
        > = transaction_pool.into();
        let tn_execution = Arc::new(TNExecution {});
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.blockchain_provider.clone())
            .with_pool(transaction_pool)
            .with_network(network)
            .with_executor(task_manager.get_spawner())
            .with_evm_config(self.evm_config.clone())
            .with_events(self.blockchain_provider.clone())
            .with_block_executor(self.evm_executor.clone())
            .with_consensus(tn_execution.clone());

        //.node_configure namespaces
        let modules_config = self.node_config.rpc.transport_rpc_module_config();
        let mut server =
            rpc_builder.build(modules_config, Box::new(EthApi::with_spawner), tn_execution);

        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "tn::execution", "Error merging TN rpc module: {e:?}");
        }
        server
    }

    /// Start running the RPC server for this instance.
    pub async fn start_rpc(&self, server: &RpcServer) -> TnRethResult<RpcServerHandle> {
        let server_config = self.node_config.rpc.rpc_server_config();
        Ok(server_config.start(server).await?)
    }

    /// Provide the state for the latest block in this instance.
    pub fn latest(&self) -> TnRethResult<StateProviderBox> {
        Ok(self.blockchain_provider.latest()?)
    }

    /// Execute a block for testing.
    pub fn execute_for_test(
        &self,
        block: &BlockWithSenders,
    ) -> TnRethResult<(BundleState, Vec<Receipt>)> {
        // create execution db
        let mut db = StateProviderDatabase::new(
            self.latest().expect("provider retrieves latest during test batch execution"),
        );
        // execute the block
        let BlockExecutionOutput { state, receipts, .. } =
            self.evm_executor.executor(&mut db).execute(block)?;
        Ok((state, receipts))
    }
}
