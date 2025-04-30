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
    hex,
    primitives::{Bytes, ChainId},
    sol_types::SolCall,
};
use clap::Parser;
use dirs::path_to_datadir;
use enr::secp256k1::rand::Rng as _;
use error::{TnRethError, TnRethResult};
use eyre::OptionExt;
use futures::StreamExt as _;
use jsonrpsee::Methods;
use rand_chacha::rand_core::SeedableRng as _;
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
use reth_chainspec::{BaseFeeParams, EthChainSpec};
use reth_consensus::FullConsensus;
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_eth_wire::BlockHashNumber;
use reth_evm::{
    env::EvmEnv,
    execute::{BlockExecutorProvider, Executor as _},
    ConfigureEvm, ConfigureEvmEnv as _,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_ethereum::{BasicBlockExecutorProvider, EthExecutionStrategyFactory};
use reth_primitives::{Log, TxType};
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
    primitives::{
        BlobExcessGasAndPrice, BlockEnv, CfgEnv, CfgEnvWithHandlerCfg, EVMError, Env,
        EnvWithHandlerCfg, ExecutionResult, ResultAndState, TxEnv,
    },
    Database, DatabaseCommit, Evm, State,
};
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use std::{
    net::SocketAddr,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
};
use system_calls::{
    ConsensusRegistry::{self, ValidatorStatus},
    CONSENSUS_REGISTRY_ADDRESS, SYSTEM_ADDRESS,
};
use tempfile::TempDir;
use tn_config::{ValidatorInfo, CONSENSUS_REGISTRY_JSON};
use tn_types::{
    adiri_chain_spec_arc, calculate_transaction_root, keccak256, Address, Block, BlockBody,
    BlockExt as _, BlockHashOrNumber, BlockNumHash, BlockNumber, BlockWithSenders, BlsSignature,
    ExecHeader, Genesis, GenesisAccount, Receipt, SealedBlock, SealedBlockWithSenders,
    SealedHeader, TaskManager, TransactionSigned, TxKind, B256, EMPTY_OMMER_ROOT_HASH,
    EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS, U256,
};
use tokio::sync::mpsc::{self, unbounded_channel};
use tracing::{debug, error, info, warn};
use traits::{TNPayload, TelcoinNode, TelcoinNodeTypes as _};

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

pub mod dirs;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
use worker::WorkerNetwork;
pub mod error;
pub mod system_calls;
pub mod worker;

/// Rpc Server type, used for getting the node started.
pub type RpcServer = TransportRpcModules<()>;

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

    /// Return the chain id.
    pub fn chain_id(&self) -> ChainId {
        self.0.chain_id()
    }
}

/// Type wrapper for a Reth DB.
/// Used primary as a opaque type to allow
/// the node launcher to create the DB upfront and reuse.
pub type RethDb = Arc<DatabaseEnv>;

impl RethEnv {
    /// Create a new Reth DB.
    /// Break this out so this can be created upfront and used even on a
    /// restart (when catching up for instance).
    pub fn new_database<P: AsRef<Path>>(
        reth_config: &RethConfig,
        db_path: P,
    ) -> eyre::Result<RethDb> {
        let db_path = db_path.as_ref();
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
        Ok(Arc::new(init_db(db_path, reth_config.0.db.database_args())?.with_metrics()))
    }

    /// Produce a new wrapped Reth environment from a config, DB path and task manager.
    ///
    /// This method MUST be called from within a tokio runtime.
    pub fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
    ) -> eyre::Result<Self> {
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
        let database = Self::new_database(&reth_config, db_path)?;
        Self::new(&reth_config, task_manager, database)
    }

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
        transactions: Vec<Vec<u8>>,
        consensus_header_hash: B256,
    ) -> TnRethResult<SealedBlockWithSenders> {
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

        debug!(
            target: "payload_builder",
            parent_hash = ?payload.attributes.parent_header.hash(),
            parent_number = payload.attributes.parent_header.number,
            "building new payload"
        );
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
        let tn_env = self.tn_env_for_evm(&payload);
        let block_gas_limit: u64 = tn_env.block.gas_limit.try_into().unwrap_or(u64::MAX);
        let base_fee = tn_env.block.basefee.to::<u64>();
        let block_number = tn_env.block.number.to::<u64>();

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

        let mut evm = self.evm_config.evm_with_env(&mut db, tn_env);

        for tx_bytes in &transactions {
            let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                .inspect_err(|e| {
                    tracing::error!(
                    target: "engine",
                    batch=?payload.attributes.batch_digest,
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

        // close epoch using leader's aggregate signature if conditions are met
        if let Some(res) = payload
            .attributes
            .close_epoch
            .map(|sig| self.apply_closing_epoch_contract_call(&mut evm, sig))
        {
            // add logs if epoch closed
            let logs = res?;
            receipts.push(Some(Receipt {
                // no better tx type
                tx_type: TxType::Legacy,
                success: true,
                cumulative_gas_used: 0,
                logs,
            }));
        }

        // Release db
        drop(evm);

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
            beneficiary: payload.suggested_fee_recipient(),
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
            beneficiary: payload.suggested_fee_recipient(),
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

    fn apply_closing_epoch_contract_call<EXT, DB>(
        &self,
        evm: &mut Evm<'_, EXT, DB>,
        randomness: BlsSignature,
    ) -> TnRethResult<Vec<Log>>
    where
        DB: Database + DatabaseCommit,
        DB::Error: core::fmt::Display,
    {
        let prev_env = Box::new(evm.context.env().clone());
        let calldata = self.generate_conclude_epoch_calldata(evm, randomness)?;

        // fill tx env to execute system call to consensus registry
        self.evm_config.fill_tx_env_system_contract_call(
            &mut evm.context.evm.env,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        );

        // execute system call to consensus registry
        let mut res = match evm.transact() {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                return Err(EVMError::Custom(format!("epoch closing execution failed: {e}")).into());
            }
        };

        // capture closing epoch log
        debug!(target: "engine", "closing epoch logs:\n{:#?}", res.result.logs());
        let closing_epoch_logs = res.result.clone().into_logs();

        // remove residual artifacts
        self.restore_evm_context_after_system_call(&mut res, evm, prev_env);

        debug!(target: "engine", "committing closing epoch state:\n{:#?}", res.state);

        // commit the changes
        evm.db_mut().commit(res.state);

        Ok(closing_epoch_logs)
    }

    /// Generate calldata for updating the ConsensusRegistry to conclude the epoch.
    fn generate_conclude_epoch_calldata<EXT, DB>(
        &self,
        evm: &mut Evm<'_, EXT, DB>,
        randomness: BlsSignature,
    ) -> TnRethResult<Bytes>
    where
        DB: Database,
        DB::Error: core::fmt::Display,
    {
        // shuffle all validators for new committee
        let new_committee = self.shuffle_new_committee(evm, randomness)?;

        // encode the call to bytes with method selector and args
        let bytes =
            ConsensusRegistry::concludeEpochCall { newCommittee: new_committee, slashes: vec![] }
                .abi_encode()
                .into();

        Ok(bytes)
    }

    /// Read eligible validators from latest state and shuffle the committee deterministically.
    fn shuffle_new_committee<EXT, DB>(
        &self,
        evm: &mut Evm<'_, EXT, DB>,
        randomness: BlsSignature,
    ) -> TnRethResult<Vec<Address>>
    where
        DB: Database,
        DB::Error: core::fmt::Display,
    {
        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let read_state =
            self.read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        debug!(target: "engine", "result after shuffle:\n{:#?}", read_state);

        // retrieve data from execution result
        let data = match read_state.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => return Err(EVMError::Custom(format!("getValidatorsCall failed: {e:?}")).into()),
        };

        // Use SolValue to decode the result
        let mut eligible_validators: Vec<ConsensusRegistry::ValidatorInfo> =
            alloy::sol_types::SolValue::abi_decode(&data, true)?;

        debug!(target: "engine",  "validators pre-shuffle {:#?}", eligible_validators);

        // simple Fisher-Yates shuffle
        //
        // create seed from hashed bls agg signature
        let mut seed = [0; 32];
        let random_bytes = keccak256(randomness.to_bytes());
        seed.copy_from_slice(random_bytes.as_slice());
        debug!(target: "engine", ?seed, "seed after");

        let mut rng = rand_chacha::ChaCha8Rng::from_seed(seed);
        for i in (1..eligible_validators.len()).rev() {
            let j = rng.gen_range(0..=i);
            eligible_validators.swap(i, j);
        }

        debug!(target: "engine",  "validators post-shuffle {:#?}", eligible_validators);

        let new_committee = eligible_validators.into_iter().map(|v| v.validatorAddress).collect();

        Ok(new_committee)
    }

    /// Read state on-chain.
    fn read_state_on_chain<EXT, DB>(
        &self,
        evm: &mut Evm<'_, EXT, DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<ResultAndState>
    where
        DB: Database,
        DB::Error: core::fmt::Display,
    {
        let prev_env = Box::new(evm.context.env().clone());

        // fill tx env to disable certain EVM checks
        self.evm_config.fill_tx_env_system_contract_call(
            &mut evm.context.evm.env,
            caller,
            contract,
            calldata,
        );

        // read from state
        let res = match evm.transact() {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state: {}", e);
                return Err(EVMError::Custom(format!("getValidatorsCall failed: {e}")).into());
            }
        };

        // restore env for evm
        evm.context.evm.env = prev_env;

        Ok(res)
    }

    /// Restore evm context after system call.
    fn restore_evm_context_after_system_call<EXT, DB>(
        &self,
        res: &mut ResultAndState,
        evm: &mut Evm<'_, EXT, DB>,
        prev_env: Box<Env>,
    ) where
        DB: Database,
    {
        // NOTE: revm marks these accounts as "touched" after the contract call
        // and includes them in the result
        //
        // remove the state changes for system call
        res.state.remove(&SYSTEM_ADDRESS);
        res.state.remove(&evm.block().coinbase);

        // restore the previous env
        evm.context.evm.env = prev_env;
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

    /// Create an EVM tx enviornment that bypasses certain checks.
    ///
    /// This method is useful for executing transactions pre-genesis.
    pub fn execute_call_tx_for_test_bypass_evm_checks(
        &self,
        header: &SealedHeader,
        txs: Vec<PregenesisRequest>,
    ) -> TnRethResult<BundleState> {
        // create execution db
        let state = StateProviderDatabase::new(
            self.latest().expect("provider retrieves latest during test batch execution"),
        );

        let mut db = State::builder().with_database(state).with_bundle_update().build();

        // Setup environment for the execution.
        let EvmEnv { cfg_env_with_handler_cfg, block_env } =
            self.evm_config.cfg_and_block_env(header);

        // setup EVM
        let mut evm = self.evm_config.evm_with_env(
            &mut db,
            EnvWithHandlerCfg::new_with_cfg_env(
                cfg_env_with_handler_cfg,
                block_env,
                Default::default(),
            ),
        );

        for tx in txs {
            // modify env to disable checks
            self.fill_tx_env_free_execution(&mut evm.context.evm.env, tx);

            // execute the transaction
            let ResultAndState { state, result } = evm.transact()?;
            debug!(target: "engine", "execution:\n{:#?}", state);
            debug!(target: "engine", "result:\n{:#?}", result);
            evm.db_mut().commit(state);
        }

        drop(evm);

        // apply changes
        db.merge_transitions(BundleRetention::PlainState);
        Ok(db.take_bundle())
    }

    /// Creates a new [`EnvWithHandlerConfg`] based on the provided payload.
    ///
    /// see reth `ConfigureEvmEnv::cfg_and_block_env` which uses a default SpecId (latest)
    ///
    /// This method is used to create TN-Specific EVM environment with the correct forks
    /// and defaults for execution.
    fn tn_env_for_evm(&self, payload: &TNPayload) -> EnvWithHandlerCfg {
        let spec_id = reth_revm::primitives::SpecId::SHANGHAI;
        let cfg_env = CfgEnv::default().with_chain_id(self.chainspec().chain_id());
        let cfg = CfgEnvWithHandlerCfg::new_with_spec_id(cfg_env, spec_id);

        // use the basefee set by the worker during batch creation
        let basefee = U256::from(payload.attributes.base_fee_per_gas);

        // ensure gas_limit enforced during block validation
        let gas_limit = U256::from(payload.attributes.gas_limit);

        // create block env
        let block_env = BlockEnv {
            // build env for the next block based on parent
            number: U256::from(payload.attributes.parent_header.number + 1),
            // special fee address
            coinbase: payload.suggested_fee_recipient(),
            timestamp: U256::from(payload.timestamp()),
            // leave difficulty zero
            // this value is useful for post-execution, but worker batches are created with this
            // value
            difficulty: U256::ZERO,
            prevrandao: Some(payload.prev_randao()),
            gas_limit,
            basefee,
            // calculate excess gas based on parent block's blob gas usage
            blob_excess_gas_and_price: payload
                .attributes
                .parent_header
                .excess_blob_gas
                .map(|price| BlobExcessGasAndPrice::new(price, false)),
        };

        EnvWithHandlerCfg::new_with_cfg_env(cfg.clone(), block_env.clone(), TxEnv::default())
    }

    /// Convenience method for compiling storage and bytecode to include genesis.
    pub fn create_consensus_registry_genesis_account(
        validators: Vec<ValidatorInfo>,
        genesis: Genesis,
        initial_stake_config: ConsensusRegistry::StakeConfig,
        owner_address: Address,
        rwtel_address: Address,
    ) -> eyre::Result<Genesis> {
        let validators: Vec<_> = validators
            .iter()
            .map(|v| ConsensusRegistry::ValidatorInfo {
                blsPubkey: v.bls_public_key.to_bytes().into(),
                validatorAddress: v.execution_address,
                activationEpoch: 0,
                exitEpoch: 0,
                currentStatus: ConsensusRegistry::ValidatorStatus::Active,
                isRetired: false,
                isDelegated: false,
                stakeVersion: 0,
            })
            .collect();

        let total_stake_balance = initial_stake_config
            .stakeAmount
            .checked_mul(U256::from(validators.len()))
            .ok_or_eyre("Failed to calculate total stake for consensus registry at genesis")?;

        let registry_bytecode =
            Self::parse_deployed_bytecode_from_json_str(CONSENSUS_REGISTRY_JSON)?;

        // extend accounts for initial execution
        let tmp_genesis = genesis.clone().extend_accounts([(
            CONSENSUS_REGISTRY_ADDRESS,
            GenesisAccount::default()
                .with_balance(total_stake_balance)
                .with_code(Some(registry_bytecode.clone().into())),
        )]);

        let chain: Arc<RethChainSpec> = Arc::new(tmp_genesis.into());

        // create temporary reth env for execution
        let task_manager = TaskManager::new("Test Task Manager");
        let tmp_dir = TempDir::new().unwrap();
        let reth_env =
            RethEnv::new_for_test_with_chain(chain.clone(), tmp_dir.path(), &task_manager)?;

        // generate calldata for initialization call
        let init_calldata = ConsensusRegistry::initializeCall {
            rwTEL_: rwtel_address,
            genesisConfig_: initial_stake_config,
            initialValidators_: validators,
            owner_: owner_address,
        }
        .abi_encode()
        .into();

        // execute the transaction
        let tx = CallRequest::new(CONSENSUS_REGISTRY_ADDRESS, SYSTEM_ADDRESS, init_calldata);

        let BundleState { state, contracts, reverts, state_size, reverts_size } = reth_env
            .execute_call_tx_for_test_bypass_evm_checks(
                &chain.sealed_genesis_header(),
                vec![tx.into()],
            )?;

        debug!(target: "engine", "contracts:\n{:#?}", contracts);
        debug!(target: "engine", "reverts:\n{:#?}", reverts);
        debug!(target: "engine", "state_size:{:#?}", state_size);
        debug!(target: "engine", "reverts_size:{:#?}", reverts_size);

        // place initialized bytecode in genesis
        let storage = state.get(&CONSENSUS_REGISTRY_ADDRESS).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });

        let genesis = genesis.extend_accounts([(
            CONSENSUS_REGISTRY_ADDRESS,
            GenesisAccount::default()
                .with_balance(total_stake_balance)
                .with_code(Some(registry_bytecode.into()))
                .with_storage(storage),
        )]);

        Ok(genesis)
    }

    /// Parse bytecode from a `&str`.
    pub fn parse_bytecode_from_json_str(json_content: &str) -> eyre::Result<Vec<u8>> {
        // parse as generic JSON Value
        let json: serde_json::Value = serde_json::from_str(json_content)?;

        // extract the specific field we want
        let abi = json["bytecode"]["object"]
            .as_str()
            .ok_or_eyre("Invalid json abi format for bytecode")?;

        // convert hex to bytes
        let bytecode = hex::decode(abi)?;
        Ok(bytecode)
    }

    /// Parse deployed bytecode from a `&str`.
    pub fn parse_deployed_bytecode_from_json_str(json_content: &str) -> eyre::Result<Vec<u8>> {
        // parse as generic JSON Value
        let json: serde_json::Value = serde_json::from_str(json_content)?;

        // extract the specific field we want
        let abi = json["deployedBytecode"]["object"]
            .as_str()
            .ok_or_eyre("Invalid json abi format for bytecode")?;

        // convert hex to bytes
        let bytecode = hex::decode(abi)?;
        Ok(bytecode)
    }

    /// Create a tx environment with all evm checks disabled.
    ///
    /// This is useful for executing transactions for pre-genesis.
    /// For future reth upgrades, see:
    /// `EthEvmConfig::fill_tx_env_for_system_call`
    ///
    /// WARNING: do not use this when executing consensus transactions.
    fn fill_tx_env_free_execution(&self, env: &mut Env, tx: PregenesisRequest) {
        // #[allow(clippy::needless_update)] // side-effect of optimism fields
        let tx = TxEnv {
            caller: tx.caller(),
            transact_to: tx.tx_kind(),
            // Explicitly set nonce to None so revm does not do any nonce checks
            nonce: None,
            gas_limit: 30_000_000,
            value: U256::ZERO,
            data: tx.data(),
            // Setting the gas price to zero enforces that no value is transferred as part of the
            // call, and that the call will not count against the block's gas limit
            gas_price: U256::ZERO,
            // The chain ID check is not relevant here and is disabled if set to None
            chain_id: None,
            // Setting the gas priority fee to None ensures the effective gas price is derived from
            // the `gas_price` field, which we need to be zero
            gas_priority_fee: None,
            access_list: Vec::new(),
            // blob fields can be None for this tx
            blob_hashes: Vec::new(),
            max_fee_per_blob_gas: None,
            // TODO remove this once this crate is no longer built with optimism
            ..Default::default()
        };
        env.tx = tx;

        // ensure the block gas limit is >= the tx
        env.block.gas_limit = U256::from(env.tx.gas_limit);

        // disable the base fee check for this call by setting the base fee to zero
        env.block.basefee = U256::ZERO;
    }
}

/// Param for requesting a call transaction.
#[derive(Debug)]
pub struct CallRequest {
    /// The caller's address.
    caller_address: Address,
    /// The contract to call.
    contract_address: Address,
    /// The data to call.
    data: Bytes,
}

impl CallRequest {
    /// Create a new instance of [Self].
    pub fn new(contract_address: Address, caller_address: Address, data: Bytes) -> Self {
        Self { contract_address, caller_address, data }
    }
}

/// Param for requesting a create transaction.
#[derive(Debug)]
pub struct CreateRequest {
    /// The caller's address.
    caller_address: Address,
    /// The transaction data.
    data: Bytes,
}

impl CreateRequest {
    /// Create a new instance of [Self].
    pub fn new(caller_address: Address, data: Bytes) -> Self {
        Self { caller_address, data }
    }
}

/// Variations of pregenesis requests.
#[derive(Debug)]
pub enum PregenesisRequest {
    /// Set the tx env to call.
    Call(CallRequest),
    /// Set the tx env to create.
    Create(CreateRequest),
}

impl PregenesisRequest {
    fn tx_kind(&self) -> TxKind {
        match self {
            PregenesisRequest::Call(tx) => TxKind::Call(tx.contract_address),
            PregenesisRequest::Create(_) => TxKind::Create,
        }
    }

    fn caller(&self) -> Address {
        match self {
            PregenesisRequest::Call(tx) => tx.caller_address,
            PregenesisRequest::Create(tx) => tx.caller_address,
        }
    }

    fn data(&self) -> Bytes {
        match self {
            PregenesisRequest::Call(tx) => tx.data.clone(),
            PregenesisRequest::Create(tx) => tx.data.clone(),
        }
    }
}

impl From<CallRequest> for PregenesisRequest {
    fn from(call: CallRequest) -> Self {
        PregenesisRequest::Call(call)
    }
}

impl From<CreateRequest> for PregenesisRequest {
    fn from(create: CreateRequest) -> Self {
        PregenesisRequest::Create(create)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::TNPayloadAttributes;
    use rand_chacha::ChaCha8Rng;
    use std::str::FromStr as _;
    use tempfile::TempDir;
    use tn_types::{
        adiri_genesis, BlsKeypair, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput,
        PrimaryInfo, ReputationScores,
    };

    /// Helper function to call `ConsensusRegistry` state on-chain.
    fn call_consensus_registry<EXT, DB, T>(
        reth_env: &RethEnv,
        evm: &mut Evm<'_, EXT, DB>,
        calldata: Bytes,
    ) -> eyre::Result<T>
    where
        DB: Database,
        DB::Error: core::fmt::Display,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = reth_env.read_state_on_chain(
            evm,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        )?;

        // retrieve epoch from state
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                let decoded = alloy::sol_types::SolValue::abi_decode(&data, true)?;
                Ok(decoded)
            }
            e => Err(eyre::eyre!("failed to read validators from state: {e:?}")),
        }
    }

    /// Helper function for creating a consensus output for tests.
    fn consensus_output_for_tests() -> ConsensusOutput {
        let mut leader = Certificate::default();
        let sub_dag_index = 0;
        leader.header.round = sub_dag_index as u32;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let beneficiary = Address::from_str("0x5555555555555555555555555555555555555555")
            .expect("beneficiary address from str");
        ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![leader.clone(), Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            close_epoch: false,
            batches: Default::default(), // empty
            beneficiary,
            batch_digests: Default::default(), // empty
            parent_hash: ConsensusHeader::default().digest(),
            number: 0,
            extra: Default::default(),
            early_finalize: false,
        }
    }

    #[tokio::test]
    async fn test_validator_shuffle() -> eyre::Result<()> {
        // remove this
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::ACTIVE)
            .with_writer(std::io::stdout)
            .try_init();

        let validator_1 = Address::from_slice(&[0x11; 20]);
        let validator_2 = Address::from_slice(&[0x22; 20]);
        let validator_3 = Address::from_slice(&[0x33; 20]);
        let validator_4 = Address::from_slice(&[0x44; 20]);
        let validator_5 = Address::from_slice(&[0x55; 20]);

        // create initial validators for testing
        let initial_validators = [validator_1, validator_2, validator_3, validator_4, validator_5];

        // create validator info objects for each address
        let validators: Vec<_> = initial_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // use deterministic seed
                let mut rng = ChaCha8Rng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                ValidatorInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    primary_info: PrimaryInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: BlsSignature::default(),
                }
            })
            .collect();

        debug!(target: "engine", "created validators for consensus registry {:#?}", validators);

        let epoch_duration = 60 * 60 * 24; // 24hrs
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(1_000_000e18),
            minWithdrawAmount: U256::from(1_000e18),
            epochIssuance: U256::from(20_000_000e18)
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        let owner = Address::random();
        let genesis = RethEnv::create_consensus_registry_genesis_account(
            validators.clone(),
            adiri_genesis(),
            initial_stake_config,
            owner,
            Address::random(), // rwtel
        )?;

        // create new env with initialized consensus registry for tests
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Test Task Manager");
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let reth_env =
            RethEnv::new_for_test_with_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();

        // create execution db
        let state = StateProviderDatabase::new(
            reth_env.latest().expect("provider retrieves latest during test batch execution"),
        );
        let mut db = State::builder().with_database(state).with_bundle_update().build();

        // setup environment for execution
        let payload = TNPayload::new(TNPayloadAttributes::new_for_test(
            chain.sealed_genesis_header(),
            &consensus_output_for_tests(),
        ));
        let tn_env = reth_env.tn_env_for_evm(&payload);
        let mut evm = reth_env.evm_config.evm_with_env(&mut db, tn_env);
        let original_env = evm.context.env().clone();

        // read curent epoch
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        let epoch = call_consensus_registry::<_, _, u32>(&reth_env, &mut evm, calldata)?;
        let expected_epoch = 0;
        assert_eq!(expected_epoch, epoch);

        // read current epoch info
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch }.abi_encode().into();
        let epoch_info = call_consensus_registry::<_, _, ConsensusRegistry::EpochInfo>(
            &reth_env, &mut evm, calldata,
        )?;
        let expected_committee = validators.iter().map(|v| v.execution_address).collect();
        let expected_epoch_info = ConsensusRegistry::EpochInfo {
            committee: expected_committee,
            blockHeight: 0,
            epochDuration: epoch_duration,
        };
        assert_eq!(epoch_info, expected_epoch_info);

        // close epoch with deterministic signature as source of randomness
        let sig = BlsSignature::default();
        reth_env.apply_closing_epoch_contract_call(&mut evm, sig)?;
        assert_eq!(&original_env, evm.context.env());

        // read new epoch info
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        let epoch = call_consensus_registry::<_, _, u32>(&reth_env, &mut evm, calldata)?;
        let expected_epoch = expected_epoch + 1;
        assert_eq!(expected_epoch, epoch);

        // read new committee (always 2 epochs ahead)
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 2 }.abi_encode().into();
        let new_epoch_info = call_consensus_registry::<_, _, ConsensusRegistry::EpochInfo>(
            &reth_env, &mut evm, calldata,
        )?;

        // ensure shuffle is deterministic
        let expected_new_committee =
            vec![validator_4, validator_5, validator_3, validator_2, validator_1];

        let expected = ConsensusRegistry::EpochInfo {
            committee: expected_new_committee,
            blockHeight: 0,
            // epoch duration set at the start
            epochDuration: Default::default(),
        };

        debug!(target: "engine", "new epoch info:{:#?}", new_epoch_info);

        assert_eq!(new_epoch_info, expected);

        // merge transitions to apply state changes
        evm.context.evm.db.merge_transitions(BundleRetention::PlainState);

        // debug! take bundle
        let bundle = evm.context.evm.db.take_bundle();
        debug!(target: "engine", "bundle from execution:\n{:#?}", bundle);

        Ok(())
    }
}
