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

use crate::{
    evm::TNEvm,
    traits::{DefaultEthPayloadTypes, TNExecution},
};
use alloy::{
    consensus::Transaction as _,
    hex,
    primitives::{Bytes, ChainId},
    sol_types::{SolCall, SolConstructor},
};
use alloy_evm::Evm;
use clap::Parser;
use dirs::path_to_datadir;
use error::{TnRethError, TnRethResult};
use evm::TnEvmConfig;
use eyre::OptionExt;
use jsonrpsee::Methods;
use reth::{
    args::{
        DatabaseArgs, DatadirArgs, DebugArgs, DevArgs, DiscoveryArgs, EngineArgs, NetworkArgs,
        PayloadBuilderArgs, PruningArgs, TxPoolArgs,
    },
    builder::NodeConfig,
    network::transactions::config::TransactionPropagationKind,
    rpc::{
        builder::{
            config::RethRpcServerConfig, RethRpcModule, RpcModuleBuilder, RpcModuleSelection,
            TransportRpcModules,
        },
        eth::EthApi,
        server_types::eth::utils::recover_raw_transaction as reth_recover_raw_transaction,
    },
};
use reth_chain_state::ExecutedTrieUpdates;
use reth_chainspec::{BaseFeeParams, EthChainSpec};
use reth_db::{init_db, DatabaseEnv};
use reth_db_common::init::init_genesis;
use reth_discv4::NatResolver;
use reth_engine_tree::{
    engine::{EngineApiRequest, FromEngine},
    tree::EngineApiTreeHandler,
};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_eth_wire::BlockHashNumber;
use reth_evm::{
    execute::{BlockBuilder, BlockBuilderOutcome},
    ConfigureEvm, EvmFactory,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_builder::{
    DEFAULT_MAX_PROOF_TASK_CONCURRENCY, DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
    DEFAULT_RESERVED_CPU_CORES,
};
use reth_node_core::node_config::DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB;
use reth_provider::{
    providers::{BlockchainProvider, StaticFileProvider},
    writer::UnifiedStorageWriter,
    BlockIdReader as _, BlockNumReader, BlockReader, CanonChainTracker,
    CanonStateSubscriptions as _, ChainSpecProvider, ChainStateBlockReader, ChainStateBlockWriter,
    DatabaseProviderFactory, HeaderProvider as _, ProviderFactory, StateProviderBox,
    StateProviderFactory, StaticFileProviderFactory, TransactionVariant,
};
use reth_revm::{
    cached::CachedReads,
    context::result::{ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, BundleState},
    DatabaseCommit, State,
};
use reth_transaction_pool::{blobstore::DiskFileBlobStore, EthTransactionPool};
use rpc_server_args::RpcServerArgs;
use serde_json::Value;
use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr},
    ops::RangeInclusive,
    path::Path,
    sync::{Arc, OnceLock},
    time::Duration,
};
use system_calls::{
    ConsensusRegistry::{self},
    EpochState, CONSENSUS_REGISTRY_ADDRESS, SYSTEM_ADDRESS,
};
use tempfile::TempDir;
use tn_config::{NodeInfo, CONSENSUS_REGISTRY_JSON};
use tn_types::{
    gas_accumulator::RewardsCounter, Address, BlockBody, BlockHashOrNumber, BlockHeader as _,
    BlockNumHash, BlockNumber, Epoch, ExecHeader, Genesis, GenesisAccount, RecoveredBlock,
    SealedBlock, SealedHeader, TaskManager, TaskSpawner, TransactionSigned, B256,
    ETHEREUM_BLOCK_GAS_LIMIT_30M, U256,
};
use tracing::{debug, error, info, warn};
use traits::{TNPrimitives, TelcoinNode};

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, rpc::builder::RpcServerHandle,
};
pub use reth_chain_state::{
    CanonicalInMemoryState, ExecutedBlockWithTrieUpdates, NewCanonicalChain,
};
pub use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_errors::{ProviderError, RethError};
pub use reth_node_core::{
    args::{ColorMode, LogArgs},
    node_config::DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
pub use reth_provider::{CanonStateNotificationStream, ExecutionOutcome};
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::FileWorkerGuard;
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction,
};

pub mod dirs;
pub mod payload;
use payload::TNPayload;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
use worker::WorkerNetwork;
pub mod error;
mod evm;
pub mod rpc_server_args;
pub mod system_calls;
pub mod worker;

#[cfg(any(feature = "test-utils", test))]
pub mod test_utils;

/// This will contain the address to receive base fees.  It is set per chain (or not if None)
/// will not change.  Implemented as a static OnceLock to work around the Reth lib interface.
static BASEFEE_ADDRESS: OnceLock<Option<Address>> = OnceLock::new();

/// Return the chains basefee address if set.
/// Note the basefee address is set once for the chain and will not change (outside of a hard fork).
pub fn basefee_address() -> Option<Address> {
    *BASEFEE_ADDRESS.get()?
}

/// Set the basefee address.  This will only work on the first call and should be during program
/// initialization. Calling more than once will do nothing, not calling early can lead to an unset
/// basefee address and a chain fork.
fn set_basefee_address(address: Option<Address>) {
    // Ignore the error.  Should probably panic on error but this will break some test environments.
    let _ = BASEFEE_ADDRESS.set(address);
}

/// Rpc Server type, used for getting the node started.
pub type RpcServer = TransportRpcModules<()>;

/// The type to receive executed blocks from the engine and update canonical/finalized block state.
pub type TnEngineApiTreeHandler = EngineApiTreeHandler<
    TNPrimitives,
    BlockchainProvider<TelcoinNode>,
    DefaultEthPayloadTypes,
    TNExecution,
    TnEvmConfig,
>;

/// The type to send to the blockchain tree (make blocks canonical/final).
pub type ToTree = std::sync::mpsc::Sender<
    FromEngine<
        EngineApiRequest<DefaultEthPayloadTypes, TNPrimitives>,
        alloy::consensus::Block<TransactionSigned>,
    >,
>;

// replace deprecated reth name with this type
/// Type alias to replace deprecated reth struct with new generic type:
/// A block with senders recovered from the block’s transactions.
///
/// This type is a SealedBlock with a list of senders that match the transactions in the block.
pub type BlockWithSenders = RecoveredBlock<reth_ethereum_primitives::Block>;

/// Reth specific command line args.
#[derive(Debug, Parser, Clone)]
pub struct RethCommand {
    /// All rpc related arguments
    #[clap(flatten)]
    pub rpc: RpcServerArgs,

    /// All txpool related arguments with --txpool prefix
    #[clap(flatten)]
    pub txpool: TxPoolArgs,

    /// All database related arguments
    #[clap(flatten)]
    pub db: DatabaseArgs,
}

/// A wrapper abstraction around a Reth node config.
#[derive(Clone, Debug)]
pub struct RethConfig(NodeConfig<RethChainSpec>);

const DEFAULT_UNUSED_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

/// All the rpc modules we allow.
/// Disallow admin, txpool.
const ALL_MODULES: [RethRpcModule; 6] = [
    RethRpcModule::Eth,
    RethRpcModule::Net,
    RethRpcModule::Web3,
    RethRpcModule::Debug,
    RethRpcModule::Trace,
    RethRpcModule::Rpc,
];

impl RethConfig {
    /// Make sure that some modules are not selected, primarily they won't work as expected with TN
    /// (or at all).
    fn validate_rpc_modules(mods: &mut Option<RpcModuleSelection>) {
        match &mods {
            Some(RpcModuleSelection::All) => {
                *mods = Some(RpcModuleSelection::Selection(HashSet::from(ALL_MODULES)));
            }
            Some(RpcModuleSelection::Standard) => {}
            Some(RpcModuleSelection::Selection(hash_set)) => {
                let mut new_set = HashSet::new();
                for r in ALL_MODULES {
                    if hash_set.contains(&r) {
                        new_set.insert(r);
                    }
                }
                if hash_set.contains(&RethRpcModule::Admin) {
                    warn!(target: "tn::reth", "Attempted to configure unsupported admin RPC module!");
                }
                if hash_set.contains(&RethRpcModule::Txpool) {
                    warn!(target: "tn::reth", "Attempted to configure unsupported txpool RPC module!");
                }
                *mods = Some(RpcModuleSelection::Selection(new_set));
            }
            None => {}
        }
    }

    /// Create a new RethConfig wrapper.
    pub fn new<P: AsRef<Path>>(
        reth_config: RethCommand,
        instance: Option<u16>,
        datadir: P,
        with_unused_ports: bool,
        chain: Arc<RethChainSpec>,
    ) -> Self {
        // create a reth DatadirArgs from tn datadir
        let datadir = path_to_datadir(datadir.as_ref());

        let RethCommand { mut rpc, txpool, db } = reth_config;
        Self::validate_rpc_modules(&mut rpc.http_api);
        Self::validate_rpc_modules(&mut rpc.ws_api);
        // We don't just use Default for these Reth args.
        // This will force us to look at new options and make sure they are good for our use.
        // We DO NOT use the Reth networking so these settings should reflect that.
        let network = NetworkArgs {
            discovery: DiscoveryArgs {
                disable_discovery: true,
                disable_nat: true,
                disable_dns_discovery: true,
                disable_discv4_discovery: true,
                enable_discv5_discovery: false,
                addr: DEFAULT_UNUSED_ADDR,
                port: 0,
                discv5_addr: None,
                discv5_addr_ipv6: None,
                discv5_port: 0,
                discv5_port_ipv6: 0,
                discv5_lookup_interval: 0,
                discv5_bootstrap_lookup_interval: 0,
                discv5_bootstrap_lookup_countdown: 0,
            },
            trusted_only: false,
            trusted_peers: vec![],
            bootnodes: None,
            dns_retries: 0,
            peers_file: None,
            identity: "Reth Null Network".to_string(),
            p2p_secret_key: None,
            no_persist_peers: true,
            nat: NatResolver::None,
            addr: DEFAULT_UNUSED_ADDR,
            port: 0,
            max_outbound_peers: None,
            max_inbound_peers: None,
            max_concurrent_tx_requests: 0,
            max_concurrent_tx_requests_per_peer: 0,
            max_seen_tx_history: 0,
            max_pending_pool_imports: 0,
            soft_limit_byte_size_pooled_transactions_response: 0,
            soft_limit_byte_size_pooled_transactions_response_on_pack_request: 0,
            max_capacity_cache_txns_pending_fetch: 0,
            net_if: None,
            tx_propagation_policy: TransactionPropagationKind::Trusted,
        };

        // Not using the Reth payload builder.
        let builder = PayloadBuilderArgs {
            extra_data: "tn-reth-na".to_string(),
            gas_limit: Some(ETHEREUM_BLOCK_GAS_LIMIT_30M),
            interval: Duration::from_secs(1),
            deadline: Duration::from_secs(1),
            max_payload_tasks: 0,
        };
        let debug = DebugArgs {
            terminate: false,
            tip: None,
            max_block: None,
            etherscan: None,
            rpc_consensus_ws: None,
            skip_fcu: None,
            skip_new_payload: None,
            reorg_frequency: None,
            reorg_depth: None,
            engine_api_store: None,
            invalid_block_hook: None,
            healthy_node_rpc_url: None,
        };
        // No Reth dev options.
        let dev = DevArgs { dev: false, block_max_transactions: None, block_time: None };
        // Ignore Reth pruning for now.
        let pruning = PruningArgs {
            full: false,
            block_interval: None,
            sender_recovery_full: false,
            sender_recovery_distance: None,
            sender_recovery_before: None,
            transaction_lookup_full: false,
            transaction_lookup_distance: None,
            transaction_lookup_before: None,
            receipts_full: false,
            receipts_distance: None,
            receipts_before: None,
            account_history_full: false,
            account_history_distance: None,
            account_history_before: None,
            storage_history_full: false,
            storage_history_distance: None,
            storage_history_before: None,
            receipts_log_filter: None,
        };

        // Parameters for configuring the engine driver.
        let engine = EngineArgs {
            persistence_threshold: DEFAULT_PERSISTENCE_THRESHOLD,
            memory_block_buffer_target: DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
            legacy_state_root_task_enabled: false,
            state_root_task_compare_updates: false,
            caching_and_prewarming_enabled: true,
            caching_and_prewarming_disabled: false,
            state_provider_metrics: false,
            cross_block_cache_size: DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB,
            accept_execution_requests_hash: false,
            max_proof_task_concurrency: DEFAULT_MAX_PROOF_TASK_CONCURRENCY,
            reserved_cpu_cores: DEFAULT_RESERVED_CPU_CORES,
            precompile_cache_enabled: false,
            state_root_fallback: false,
        };

        let mut this = NodeConfig {
            config: None,
            chain,
            metrics: None,
            instance,
            datadir,
            network,
            rpc: rpc.into(),
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            engine,
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
    /// The type to configure the EVM for execution.
    evm_config: TnEvmConfig,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
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

    /// Return the sealed header for genesis.
    pub fn sealed_genesis_block(&self) -> SealedBlock {
        let header = self.sealed_genesis_header();
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: Some(Default::default()),
        };

        SealedBlock::from_sealed_parts(header, body)
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
        info!(target: "tn::reth", path = ?db_path, "opening database");
        Ok(Arc::new(init_db(db_path, reth_config.0.db.database_args())?))
    }

    /// Produce a new wrapped Reth environment from a config, DB path and task manager.
    ///
    /// This method MUST be called from within a tokio runtime.
    pub fn new(
        reth_config: &RethConfig,
        task_manager: &TaskManager,
        database: RethDb,
        basefee_address: Option<Address>,
        rewards_counter: RewardsCounter,
    ) -> eyre::Result<Self> {
        let node_config = reth_config.0.clone();
        let evm_config = TnEvmConfig::new(reth_config.0.chain.clone(), rewards_counter);
        let provider_factory = Self::init_provider_factory(&node_config, database)?;
        let blockchain_provider = BlockchainProvider::new(provider_factory.clone())?;
        let task_spawner = task_manager.get_spawner();
        set_basefee_address(basefee_address);

        Ok(Self { node_config, blockchain_provider, evm_config, task_spawner })
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
        );

        // Initialize genesis if needed
        let genesis_hash = init_genesis(&provider_factory)?;
        debug!(target: "tn::execution", chain=%node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        Ok(provider_factory)
    }

    /// Initialize a new transaction pool for worker.
    pub fn init_txn_pool(&self) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::new(&self.node_config, &self.task_spawner, &self.blockchain_provider)
    }

    /// Return a channel reciever that will return each canonical block in turn.
    pub fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        self.blockchain_provider.canonical_state_stream()
    }

    /// Return a reference to the [TaskSpawner] for spawning tasks.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.task_spawner
    }

    /// Return the chainspec for this instance.
    pub fn chainspec(&self) -> ChainSpec {
        ChainSpec(self.node_config.chain.clone())
    }

    /// Return the canonical in-memory state.
    pub fn canonical_in_memory_state(&self) -> CanonicalInMemoryState {
        self.blockchain_provider.canonical_in_memory_state()
    }

    /// Construct a canonical block from a worker's block that reached consensus.
    pub fn build_block_from_batch_payload(
        &self,
        payload: TNPayload,
        transactions: Vec<Vec<u8>>,
    ) -> TnRethResult<ExecutedBlockWithTrieUpdates> {
        let parent_header = payload.parent_header.clone();
        debug!(target: "engine", ?parent_header, "retrieving state for next block");
        let state_provider = self.blockchain_provider.state_by_block_hash(parent_header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        debug!(
            target: "engine",
            parent = ?parent_header.num_hash(),
            "building new payload"
        );

        // collect these totals to report at the end
        let mut total_fees = U256::ZERO;

        // copy in case of error
        let batch_digest = payload.batch_digest;

        // TODO: parallelize tx recovery when it's worth it (see
        // TransactionSigned::recover_signers())
        let mut builder =
            self.evm_config.builder_for_next_block(&mut db, &parent_header, payload)?;

        builder.apply_pre_execution_changes().inspect_err(|err| {
            warn!(target: "engine", %err, "failed to apply pre-execution changes");
        })?;

        let basefee = builder.evm_mut().block().basefee;

        for tx_bytes in &transactions {
            let recovered = reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                .inspect_err(|e| {
                    error!(
                    target: "engine",
                    batch=?batch_digest,
                    ?tx_bytes,
                    "failed to recover signer: {e}")
                })?;

            let gas_used = match builder.execute_transaction(recovered.clone()) {
                Ok(gas_used) => gas_used,
                Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                    error,
                    ..
                })) => {
                    // allow transaction errors (ie - duplicates)
                    //
                    // it's possible that another worker's batch included this transaction
                    warn!(target: "engine", %error,  "skipping invalid transaction: {:#?}", recovered);
                    continue;
                }
                // this is an error that we should treat as fatal for this attempt
                Err(err) => return Err(err.into()),
            };

            // update add to total fees
            let miner_fee = recovered
                .effective_tip_per_gas(basefee)
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);
        }

        let BlockBuilderOutcome { execution_result, block, hashed_state, trie_updates } =
            builder.finish(&state_provider)?;

        debug!(target: "engine", hash=?block.hash(), "block builder outcome");
        let block_num = block.number();
        let res: ExecutedBlockWithTrieUpdates<TNPrimitives> = ExecutedBlockWithTrieUpdates::new(
            Arc::new(block),
            Arc::new(ExecutionOutcome::new(
                db.take_bundle(),
                vec![execution_result.receipts],
                block_num,
                Vec::new(),
            )),
            Arc::new(hashed_state),
            ExecutedTrieUpdates::Present(Arc::new(trie_updates)),
        );

        Ok(res)
    }

    /// Finalize block (header) executed from consensus output and update chain info.
    ///
    /// This stores the finalized block number in the
    /// database, but still need to set_finalized afterwards for utilization in-memory for
    /// components, like RPC
    pub fn finalize_block(&self, header: SealedHeader) -> TnRethResult<()> {
        let num_hash = header.num_hash();
        // persiste final block info for node recovery
        let provider = self.blockchain_provider.database_provider_rw()?;
        provider.save_finalized_block_number(header.number)?;
        // this clears up old blocks in-memory
        self.blockchain_provider.set_finalized(header.clone());

        // update safe block last because this is less time sensitive but still needs to happen
        provider.save_safe_block_number(header.number)?;
        self.blockchain_provider.set_safe(header);

        // commit db transaction
        provider.commit()?;

        // cleanup chain state in memory
        // this returns the `canonical_chain().count()` back to 0
        self.blockchain_provider.canonical_in_memory_state().remove_persisted_blocks(num_hash);

        Ok(())
    }

    /// This makes all blocks canonical, commits them to the database,
    /// broadcasts new chain on `canon_state_notification_sender`
    /// and set last executed header as the tracked header.
    ///
    /// It also clears the canonical in-memory state.
    pub fn finish_executing_output(
        &self,
        blocks: Vec<ExecutedBlockWithTrieUpdates>,
    ) -> TnRethResult<()> {
        // NOTE: this makes all blocks canonical, commits them to the database,
        // and broadcasts new chain on `canon_state_notification_sender`
        //
        // the canon_state_notifications include every block executed in this round
        //
        // the worker's pool maintenance task subcribes to these events
        debug!(
            target: "engine",
            first=?blocks.first().map(|b| b.recovered_block.num_hash()),
            last=?blocks.last().map(|b| b.recovered_block.num_hash()),
            "storing range of blocks",
        );

        // insert blocks to db
        let provider_rw = self.blockchain_provider.database_provider_rw()?;
        let static_file_provider = self.blockchain_provider.static_file_provider();
        UnifiedStorageWriter::from(&provider_rw, &static_file_provider)
            .save_blocks(blocks.clone())?;
        UnifiedStorageWriter::commit(provider_rw)?;

        // process update
        //
        // see reth::EngineApiTreeHandler::on_canonical_chain_update
        let chain_update = NewCanonicalChain::Commit { new: blocks };
        let canonical_head = chain_update.tip();
        let (epoch, round) =
            Self::deconstruct_nonce(<FixedBytes<8> as Into<u64>>::into(canonical_head.nonce));
        info!(
            target: "engine",
            "canonical head for epoch {:?} round {:?}: {:?} - {:?}",
            epoch,
            round,
            canonical_head.number,
            canonical_head.hash(),
        );

        let notification = chain_update.to_chain_notification();

        // broadcast canonical update
        self.canonical_in_memory_state().notify_canon_state(notification);

        Ok(())
    }

    /// Helper to deconstruct block nonce into epoch and round.
    pub fn deconstruct_nonce(nonce: u64) -> (u32, u32) {
        let epoch = (nonce >> 32) as u32; // Extract the upper 32 bits
        let round = nonce as u32; // Extract the lower 32 bits (truncates upper bits)
        (epoch, round)
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
            .map(|b| b.clone_sealed_block()))
    }

    /// Look up and return the sealed header (with senders) for hash.
    pub fn sealed_block_with_senders(
        &self,
        id: BlockHashOrNumber,
    ) -> TnRethResult<Option<BlockWithSenders>> {
        Ok(self.blockchain_provider.sealed_block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Return the blocks with senders for a range of block numbers.
    pub fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<BlockWithSenders>> {
        Ok(self.blockchain_provider.block_with_senders_range(range)?)
    }

    /// Return the blocks for a range of block numbers.
    pub fn blocks_for_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<SealedHeader>> {
        Ok(self.blockchain_provider.sealed_headers_range(range)?)
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
    ///
    /// This checks the canonical-in-memory-state.
    pub fn canonical_tip(&self) -> SealedHeader {
        self.blockchain_provider.canonical_in_memory_state().get_canonical_head()
    }

    /// If available return the finalized block number and hash.
    ///
    /// This checks the canonical-in-memory-state.
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

    /// Return the block number and hash of the finalized block on node startup.
    ///
    /// This method adds additional fallbacks to ensure genesis is used when the network is starting
    /// because the genesis block is not initialized as `finalized`. Nodes that start on genesis
    /// will resync with the network if it exists.
    pub fn finalized_block_hash_number_for_startup(&self) -> TnRethResult<BlockHashNumber> {
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
        other: impl Into<Methods>,
    ) -> RpcServer {
        let transaction_pool: EthTransactionPool<
            BlockchainProvider<TelcoinNode>,
            DiskFileBlobStore,
        > = transaction_pool.into();
        let tn_execution = Arc::new(TNExecution);
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.blockchain_provider.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network.clone())
            .with_executor(Box::new(self.task_spawner.clone()))
            .with_evm_config(self.evm_config.clone())
            // .with_events(self.blockchain_provider.clone())
            // .with_block_executor(self.evm_executor.clone())
            .with_consensus(tn_execution.clone());

        // //.node_configure namespaces
        let modules_config = self.node_config.rpc.transport_rpc_module_config();
        let eth_api = EthApi::builder(
            self.blockchain_provider.clone(),
            transaction_pool,
            network,
            // TODO: there is a trait definition blocking TNEvmConfig
            EthEvmConfig::new(self.blockchain_provider.chain_spec()),
        )
        .build();

        let mut server = rpc_builder.build(modules_config, eth_api);
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

    /// Create a new temp RethEnv using a specified chain spec.
    pub fn new_for_temp_chain<P: AsRef<Path>>(
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
        Self::new(&reth_config, task_manager, database, None, RewardsCounter::default())
    }

    /// Convenience method for compiling storage and bytecode to include genesis.
    pub fn create_consensus_registry_genesis_account(
        validators: Vec<NodeInfo>,
        genesis: Genesis,
        initial_stake_config: ConsensusRegistry::StakeConfig,
        owner_address: Address,
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

        let tmp_chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());
        // create temporary reth env for execution
        let task_manager = TaskManager::new("Temp Task Manager");
        let tmp_dir = TempDir::new().unwrap();
        let reth_env =
            RethEnv::new_for_temp_chain(tmp_chain.clone(), tmp_dir.path(), &task_manager)?;

        debug!(target: "engine", ?initial_stake_config, "calling constructor for consensus registry");

        let constructor_args = ConsensusRegistry::constructorCall {
            genesisConfig_: initial_stake_config,
            initialValidators_: validators,
            owner_: owner_address,
        }
        .abi_encode();

        // generate calldata for creation
        let bytecode_binding =
            Self::fetch_value_from_json_str(CONSENSUS_REGISTRY_JSON, Some("bytecode.object"))?;
        let registry_initcode =
            hex::decode(bytecode_binding.as_str().ok_or_eyre("invalid registry json")?)?;
        let mut create_registry = registry_initcode.clone();
        create_registry.extend(constructor_args);

        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        let mut tn_evm = reth_env
            .evm_config
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config.evm_env(&tmp_chain.sealed_genesis_header()));

        let ResultAndState { result, state } =
            tn_evm.transact_pre_genesis_create(owner_address, create_registry.into())?;
        debug!(target: "engine", "create consensus registry result:\n{:#?}", result);

        tn_evm.db_mut().commit(state);
        db.merge_transitions(BundleRetention::PlainState);

        // execute the transaction
        let BundleState { state, contracts, reverts, state_size, reverts_size } = db.take_bundle();

        debug!(target: "engine", "contracts:\n{:#?}", contracts);
        debug!(target: "engine", "reverts:\n{:#?}", reverts);
        debug!(target: "engine", "state_size:{:#?}", state_size);
        debug!(target: "engine", "reverts_size:{:#?}", reverts_size);

        // copy tmp values to real genesis
        let tmp_address = owner_address.create(0);
        let tmp_storage = state.get(&tmp_address).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });

        let deployed_bytecode_binding = Self::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let registry_runtimecode =
            hex::decode(deployed_bytecode_binding.as_str().ok_or_eyre("invalid registry json")?)?;
        let genesis = genesis.extend_accounts([(
            CONSENSUS_REGISTRY_ADDRESS,
            GenesisAccount::default()
                .with_balance(U256::from(total_stake_balance))
                .with_code(Some(registry_runtimecode.into()))
                .with_storage(tmp_storage),
        )]);

        Ok(genesis)
    }

    /// Fetches json info from the given string
    ///
    /// If a key is specified, return the corresponding nested object.
    /// Otherwise return the entire JSON
    /// With a generic this could be adjused to handle YAML also
    pub fn fetch_value_from_json_str(json_content: &str, key: Option<&str>) -> eyre::Result<Value> {
        let json: Value = serde_json::from_str(json_content)?;
        let result = match key {
            Some(path) => {
                let key: Vec<&str> = path.split('.').collect();
                let mut current_value = &json;
                for &k in &key {
                    current_value =
                        current_value.get(k).ok_or_else(|| eyre::eyre!("key '{}' not found", k))?;
                }
                current_value.clone()
            }
            None => json,
        };

        Ok(result)
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    ///
    /// The protocol needs the BLS pubkey for the authorities.
    /// - get current epoch info
    /// - getValidator token id by address
    /// - getValidator info by token id
    pub fn epoch_state_from_canonical_tip(&self) -> eyre::Result<EpochState> {
        // create EVM with latest state
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "retrieving epoch state from canonical tip");
        let state_provider = self.blockchain_provider.state_by_block_hash(canonical_tip.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        debug!(target: "engine", state=?db.bundle_state, hashes=?db.block_hashes, "retrieving epoch state from canonical tip");
        let mut tn_evm = self
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.evm_config.evm_env(&canonical_tip));

        // current epoch number
        let epoch = self.get_current_epoch_number(&mut tn_evm)?;

        // current epoch info
        let epoch_info = self.get_current_epoch_info(&mut tn_evm)?;
        debug!(target: "engine", ?epoch, ?epoch_info, "retrieved epoch info from canonical tip for next epoch");

        // retrieve closing timestamp for previous epoch
        let epoch_start = self
            .header_by_number(epoch_info.blockHeight.saturating_sub(1))?
            .ok_or_eyre("failed to retrieve closing epoch information")?
            .timestamp;

        // retrieve the committee
        let validators = self.get_committee_validators_by_epoch(epoch, &mut tn_evm)?;
        let epoch_state = EpochState { epoch, epoch_info, validators, epoch_start };
        debug!(target: "engine", ?epoch_state, "returning epoch state from canonical tip");

        Ok(epoch_state)
    }

    /// Extract the epoch number from a header's nonce.
    pub fn extract_epoch_from_header(header: &ExecHeader) -> Epoch {
        let nonce: u64 = header.nonce.into();
        (nonce >> 32) as u32
    }

    /// Read the curret epoch number from the [ConsensusRegistry] on-chain.
    pub fn get_current_epoch_number<DB>(&self, evm: &mut TNEvm<DB>) -> eyre::Result<u32>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.call_consensus_registry::<_, u32>(evm, calldata)
    }

    /// Read the curret epoch info from the [ConsensusRegistry] on-chain.
    pub fn get_current_epoch_info<DB>(
        &self,
        evm: &mut TNEvm<DB>,
    ) -> eyre::Result<ConsensusRegistry::EpochInfo>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(evm, calldata)
    }

    /// Retrieve all `ValidatorInfo` in the committee for the provided epoch.
    pub fn get_committee_validators_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut TNEvm<DB>,
    ) -> eyre::Result<Vec<ConsensusRegistry::ValidatorInfo>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(evm, calldata)
    }

    /// Helper function to call `ConsensusRegistry` state on-chain.
    fn call_consensus_registry<DB, T>(
        &self,
        evm: &mut TNEvm<DB>,
        calldata: Bytes,
    ) -> eyre::Result<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state =
            self.read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;

        // retrieve data from state
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                let decoded = alloy::sol_types::SolValue::abi_decode(&data)?;
                Ok(decoded)
            }
            e => Err(eyre::eyre!("failed to read validators from state: {e:?}")),
        }
    }

    /// Read state on-chain.
    fn read_state_on_chain<DB>(
        &self,
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> TnRethResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        // read from state
        let res = match evm.transact_system_call(caller, contract, calldata) {
            Ok(res) => res,
            Err(e) => {
                // fatal error
                error!(target: "engine", ?caller, ?contract, "failed to read state: {}", e);
                return Err(TnRethError::EVMCustom(format!(
                    "system call failed reading state: {e}"
                )));
            }
        };

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TransactionFactory;
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_types::{
        adiri_genesis, BlsKeypair, BlsSignature, Certificate, CommittedSubDag, ConsensusHeader,
        ConsensusOutput, FromHex, NodeP2pInfo, ReputationScores, SignatureVerificationState,
    };

    /// Helper function for creating a consensus output for tests.
    fn consensus_output_for_tests() -> ConsensusOutput {
        let mut leader = Certificate::default();
        // set signature for deterministic test results
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.header_mut_for_test().created_at = tn_types::now();
        let sub_dag_index = 0;
        leader.header.round = sub_dag_index as u32;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let beneficiary = Address::from_hex("0x5555555555555555555555555555555555555555")
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
            close_epoch: true,
            batches: Default::default(), // empty
            beneficiary,
            batch_digests: Default::default(), // empty
            parent_hash: ConsensusHeader::default().digest(),
            number: 0,
            extra: Default::default(),
            early_finalize: false,
        }
    }

    /// Build a block from TNPayload and transactions.
    fn execute_payload_and_update_canonical_chain(
        reth_env: &RethEnv,
        payload: TNPayload,
        transactions: Vec<Vec<u8>>,
    ) -> eyre::Result<ExecutedBlockWithTrieUpdates> {
        let block = reth_env.build_block_from_batch_payload(payload, transactions)?;
        // update chain state - normally handled by tn_engine::payload_builder
        let canonical_header = block.recovered_block.clone_sealed_header();
        let canonical_in_memory_state = reth_env.blockchain_provider.canonical_in_memory_state();
        canonical_in_memory_state
            .update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
        canonical_in_memory_state.set_canonical_head(canonical_header.clone());
        reth_env.finish_executing_output(vec![block.clone()])?;
        reth_env.finalize_block(canonical_header.clone())?;
        Ok(block)
    }

    #[tokio::test]
    async fn test_close_epochs() -> eyre::Result<()> {
        let validator_1 = Address::from_slice(&[0x11; 20]);
        let validator_2 = Address::from_slice(&[0x22; 20]);
        let validator_3 = Address::from_slice(&[0x33; 20]);
        let validator_4 = Address::from_slice(&[0x44; 20]);
        let validator_5 = Address::from_slice(&[0x55; 20]);

        // create validator wallet for staking later
        let mut new_validator_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

        // create initial validators for testing
        let all_validators = [
            validator_1,
            validator_2,
            validator_3,
            validator_4,
            validator_5,
            new_validator_eoa.address(),
        ];

        // create validator info objects for each address
        let mut validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // use deterministic seed
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: BlsSignature::default(),
                }
            })
            .collect();

        debug!(target: "engine", "created validators for consensus registry {:#?}", validators);

        let epoch_duration = 60 * 60 * 24; // 24hrs
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        // create genesis with funded governance safe
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = adiri_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from((50_000_000 * 10) ^ 18)), // 50mil TEL
            ),
            (
                new_validator_eoa.address(),
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
        ]);

        // remove last validator so only 5 form the initial committees
        let new_validator = validators.pop().expect("six validators");

        // update genesis with consensus registry storage
        let genesis = RethEnv::create_consensus_registry_genesis_account(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
        )?;

        // update genesis again to include stake for new validator
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let calldata =
            ConsensusRegistry::mintCall { validatorAddress: new_validator.execution_address }
                .abi_encode()
                .into();
        let mint_nft = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        let calldata = ConsensusRegistry::stakeCall {
            blsPubkey: new_validator.bls_public_key.compress().into(),
        }
        .abi_encode()
        .into();
        let stake_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            initial_stake_config.stakeAmount,
            calldata,
        );
        let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
        let activate_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );

        // create new env with initialized consensus registry for tests
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager).unwrap();
        let mut expected_epoch = 0;
        let expected_committee = validators.iter().map(|v| v.execution_address).collect();
        let mut expected_epoch_info = ConsensusRegistry::EpochInfo {
            committee: expected_committee,
            blockHeight: 0,
            epochDuration: epoch_duration,
            epochIssuance: initial_stake_config.epochIssuance,
            stakeVersion: 0,
        };

        // assert epoch state is correct
        let EpochState { epoch, epoch_info, validators: committee, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "original epoch state from canonical tip in genesis");
        assert_eq!(epoch, expected_epoch);
        assert_eq!(epoch_start, chain.genesis_timestamp());
        assert_eq!(epoch_info, expected_epoch_info);

        // assert committee matches validator args for constructor
        for v in &validators {
            let on_chain = committee
                .iter()
                .find(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(on_chain.blsPubkey.as_ref(), v.bls_public_key.to_bytes());
            assert_eq!(on_chain.activationEpoch, epoch);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert!(!on_chain.isDelegated);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // close epoch with deterministic signature as source of randomness
        // and execute the first block with txs for new validator to stake
        let mut consensus_output = consensus_output_for_tests();
        consensus_output.close_epoch = false;
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_nft, stake_tx, activate_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // now close the first epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests();
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // now close the second epoch so the new validator is active
        let consensus_output = consensus_output_for_tests();
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block3.recovered_block.clone_sealed_header();

        // read new epoch state
        let EpochState { epoch, epoch_info, validators: committee, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "new epoch state from canonical tip");
        // assert epoch info updated
        expected_epoch += 1;
        expected_epoch_info.blockHeight = 4;
        assert_eq!(expected_epoch, epoch);
        assert_eq!(epoch_start, canonical_header.timestamp);
        assert_eq!(epoch_info, expected_epoch_info);

        // create evm to read custom contract call
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config.evm_env(canonical_header.header()));

        // read new committee (always 2 epochs ahead)
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }.abi_encode().into();
        let new_epoch_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)?;

        // ensure validators in increasing order by address
        let expected_new_committee = vec![
            validator_1,
            validator_2,
            validator_3,
            validator_4,
            new_validator.execution_address,
        ];

        let expected = ConsensusRegistry::EpochInfo {
            committee: expected_new_committee,
            blockHeight: 0,
            // epoch duration set at the start
            epochDuration: Default::default(),
            // values should remain the same
            epochIssuance: Default::default(),
            stakeVersion: 0,
        };

        debug!(target: "engine", "new epoch info:{:#?}", new_epoch_info);
        assert_eq!(new_epoch_info, expected);

        // assert new committee matches validator args for constructor
        // this should be the case for the first 3 epochs
        for v in &validators {
            let on_chain = committee
                .iter()
                .find(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(on_chain.blsPubkey.as_ref(), v.bls_public_key.to_bytes());
            assert_eq!(on_chain.activationEpoch, 0);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert!(!on_chain.isDelegated);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        Ok(())
    }

    #[test]
    fn test_rpc_validator() {
        let mut mods: Option<RpcModuleSelection> = None;
        RethConfig::validate_rpc_modules(&mut mods);
        assert!(mods.is_none());
        let mut mods = Some(RpcModuleSelection::All);
        RethConfig::validate_rpc_modules(&mut mods);
        if let Some(RpcModuleSelection::Selection(mods)) = &mut mods {
            for r in ALL_MODULES {
                assert!(mods.remove(&r));
            }
        };
    }
}
