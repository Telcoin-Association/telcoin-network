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

// Used in tests
#[cfg(test)]
mod clippy {
    use proptest as _;
    use tn_reth as _;
}

use crate::{
    evm::TNEvm,
    system_calls::PRECOMPILE_GENESIS_BYTECODE,
    traits::{DefaultEthPayloadTypes, TNExecution},
};
use alloy::{
    hex,
    primitives::{Bytes, ChainId},
    sol_types::{SolCall, SolConstructor},
};
use alloy_evm::Evm;
use clap::Parser;
use dirs::path_to_datadir;
use error::{
    EvmReadError, EvmReadResult, StateReadError, StateReadResult, TnRethError, TnRethResult,
};
use evm::TnEvmConfig;
use eyre::OptionExt;
use jsonrpsee::Methods;
use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use reth::{
    args::{
        DatabaseArgs, DatadirArgs, DebugArgs, DevArgs, DiscoveryArgs, EngineArgs, EraArgs,
        EraSourceArgs, MetricArgs, NetworkArgs, PayloadBuilderArgs, PruningArgs, StaticFilesArgs,
        StorageArgs, TxPoolArgs,
    },
    builder::NodeConfig,
    network::transactions::{
        config::TransactionPropagationKind, TransactionIngressPolicy, TransactionPropagationMode,
    },
    rpc::{
        builder::{
            config::RethRpcServerConfig, RethRpcModule, RpcModuleBuilder, RpcModuleSelection,
            TransportRpcModules,
        },
        eth::EthApi,
        server_types::eth::utils::recover_raw_transaction as reth_recover_raw_transaction,
    },
};
use reth_chainspec::{BaseFeeParams, EthChainSpec};
use reth_db::init_db;
use reth_db_common::init::init_genesis;
use reth_discv4::NatResolver;
use reth_engine_tree::{
    engine::{EngineApiRequest, FromEngine},
    tree::EngineApiTreeHandler,
};
use reth_errors::{BlockExecutionError, BlockValidationError};
use reth_eth_wire::BlockHashNumber;
use reth_evm::{
    execute::{BlockBuilder, BlockBuilderOutcome, BlockExecutionOutput},
    ConfigureEvm, EvmFactory,
};
use reth_node_builder::{
    DEFAULT_MEMORY_BLOCK_BUFFER_TARGET, DEFAULT_RESERVED_CPU_CORES,
    DEFAULT_SPARSE_TRIE_MAX_STORAGE_TRIES, DEFAULT_SPARSE_TRIE_PRUNE_DEPTH,
};
use reth_node_core::node_config::DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB;
use reth_primitives_traits::SignerRecoverable as _;
use reth_provider::{
    providers::BlockchainProvider, AccountReader as _, BlockBodyIndicesProvider as _,
    BlockIdReader as _, BlockNumReader, BlockReader, CanonChainTracker,
    CanonStateSubscriptions as _, Chain, ChainStateBlockReader, ChainStateBlockWriter, DBProvider,
    DatabaseProviderFactory, ExecutionOutcome, HeaderProvider as _, ProviderFactory,
    ReceiptProvider as _, StateProvider as _, StateProviderBox, StateProviderFactory,
    TransactionVariant, TransactionsProvider as _,
};
use reth_revm::{
    cached::CachedReads,
    context::result::{EVMError, ExecutionResult, ResultAndState},
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, BundleState},
    DatabaseCommit, State,
};
use reth_transaction_pool::{
    blobstore::DiskFileBlobStore, EthTransactionPool, TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER,
};
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
    EpochState, WorkerConfigs, CONSENSUS_REGISTRY_ADDRESS, SYSTEM_ADDRESS,
};
use tempfile::TempDir;
use tn_config::{
    NodeInfo, CONSENSUS_REGISTRY_JSON, GOVERNANCE_SAFE_ADDRESS, ISSUANCE_ADDRESS, ISSUANCE_JSON,
    WORKER_CONFIGS_ADDRESS, WORKER_CONFIGS_JSON,
};
use tn_types::{
    deconstruct_nonce,
    gas_accumulator::{RewardsCounter, WorkerFeeConfig},
    Account, Address, BlockBody, BlockHashOrNumber, BlockNumHash, BlockNumber, ConsensusNumHash,
    EngineUpdate, Epoch, ExecHeader, Genesis, GenesisAccount, Receipt, Recovered, RecoveredBlock,
    Round, SealedBlock, SealedHeader, TaskManager, TaskSpawner, TransactionMeta, TransactionSigned,
    TxHash, TxNumber, B256, ETHEREUM_BLOCK_GAS_LIMIT_30M, U256,
};
use tracing::{debug, error, info, warn};
use traits::{TNPrimitives, TelcoinNode};

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, payload::BlobSidecars,
    rpc::builder::RpcServerHandle,
};
pub use reth_chain_state::{
    CanonicalInMemoryState, DeferredTrieData, ExecutedBlock, NewCanonicalChain,
};
pub use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_db::{
    mdbx::{open_db_read_only, DatabaseArguments, Error as RethMdbxError},
    static_file::iter_static_files,
    Database as RethDatabaseT, DatabaseEnv, Tables,
};
pub use reth_errors::{ProviderError, RethError};
pub use reth_node_core::{
    args::{ColorMode, LogArgs},
    node_config::DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
pub use reth_provider::{
    providers::StaticFileProvider, CanonStateNotificationStream, ChangedAccount,
};
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::{FileWorkerGuard, Layers};
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction, TransactionPool as TransactionPoolT,
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
pub mod forward;
pub mod rpc_server_args;
pub mod snapshot;
pub mod system_calls;
pub mod worker;
#[cfg(feature = "faucet")]
pub use evm::faucet_mint_role_slot;
#[cfg(not(feature = "faucet"))]
pub use evm::TIMELOCK_DURATION;
pub use evm::{
    add_bls_precompile, add_telcoin_precompile, burnCall, calculate_gas_penalty, claimCall,
    grantMintRoleCall, hasMintRoleCall, mintCall, revokeMintRoleCall, totalSupplyCall,
    BLS_G1_PRECOMPILE_ADDRESS, TELCOIN_PRECOMPILE_ADDRESS,
};
pub use forward::WorkerRpcForwarder;

#[cfg(any(feature = "test-utils", test))]
pub mod test_utils;

/// This will contain the address to receive base fees.  It is set per chain and
/// will not change.  Implemented as a static OnceLock to work around the Reth lib interface.
static BASEFEE_ADDRESS: OnceLock<Address> = OnceLock::new();

/// Return the chains basefee address if set.
/// Note the basefee address is set once for the chain and will not change (outside of a hard fork).
pub fn basefee_address() -> Address {
    *BASEFEE_ADDRESS.get().unwrap_or(&GOVERNANCE_SAFE_ADDRESS)
}

/// Set the basefee address.  This will only work on the first call and should be during program
/// initialization. Calling more than once will do nothing, not calling early can lead to an unset
/// basefee address and a chain fork.
fn set_basefee_address(address: Option<Address>) {
    // Ignore the error. Should probably panic on error but this will break some test environments.
    let _ = BASEFEE_ADDRESS.set(address.unwrap_or(GOVERNANCE_SAFE_ADDRESS));
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

/// One transaction in the chain-wide sequential transaction feed.
///
/// Storage assigns every mined transaction a sequential [`TxNumber`]; this entry pairs the
/// recovered transaction with that number and the containing block's context so consumers can
/// serve "latest N transactions" style queries without touching block bodies of empty blocks.
#[derive(Clone, Debug)]
pub struct TxFeedEntry {
    /// Sequential, chain-wide transaction number (position in the global feed).
    pub tx_number: TxNumber,
    /// The signed transaction with its sender attached.
    pub transaction: Recovered<TransactionSigned>,
    /// Number of the block containing this transaction.
    pub block_number: BlockNumber,
    /// Hash of the block containing this transaction.
    pub block_hash: B256,
    /// Timestamp of the containing block.
    pub timestamp: u64,
    /// Zero-based index of the transaction within its block.
    pub index: u64,
}

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
        let RethCommand { mut rpc, mut txpool, db } = reth_config;

        // TN default: 256 slots per sender (Reth default is 16).
        // Only apply if user hasn't explicitly set a custom value via --txpool.max-account-slots.
        if txpool.max_account_slots == TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER {
            txpool.max_account_slots = 256;
        }

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
            tx_propagation_policy: TransactionPropagationKind::None,
            tx_ingress_policy: TransactionIngressPolicy::None,
            disable_tx_gossip: true,
            propagation_mode: TransactionPropagationMode::Max(0),
            required_block_hashes: vec![],
            network_id: None,
            enforce_enr_fork_id: false,
            max_peers: None,
            netrestrict: None,
            p2p_secret_key_hex: None,
        };

        // Not using the Reth payload builder.
        let builder = PayloadBuilderArgs {
            extra_data: "tn-reth-na".to_string(),
            gas_limit: Some(ETHEREUM_BLOCK_GAS_LIMIT_30M),
            interval: Duration::from_secs(1),
            deadline: Duration::from_secs(1),
            max_payload_tasks: 0,
            max_blobs_per_block: None,
        };
        let debug = DebugArgs {
            terminate: false,
            tip: None,
            max_block: None,
            etherscan: None,
            rpc_consensus_url: None,
            skip_fcu: None,
            skip_new_payload: None,
            reorg_frequency: None,
            reorg_depth: None,
            engine_api_store: None,
            invalid_block_hook: None,
            healthy_node_rpc_url: None,
            ethstats: None,
            startup_sync_state_idle: false,
        };
        // No Reth dev options.
        let dev = DevArgs {
            dev: false,
            block_max_transactions: None,
            block_time: None,
            dev_mnemonic: Default::default(),
        };
        // Ignore Reth pruning for now.
        let pruning = PruningArgs {
            full: false,
            minimal: false,
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
            receipts_pre_merge: false,
            bodies_pre_merge: false,
            bodies_distance: None,
            bodies_before: None,
        };

        // Parameters for configuring the engine driver.
        #[allow(deprecated)] // deprecated fields
        let engine = EngineArgs {
            persistence_threshold: DEFAULT_PERSISTENCE_THRESHOLD,
            memory_block_buffer_target: DEFAULT_MEMORY_BLOCK_BUFFER_TARGET,
            legacy_state_root_task_enabled: false,
            state_root_task_compare_updates: false,
            state_cache_disabled: false,
            prewarming_disabled: false,
            state_provider_metrics: false,
            cross_block_cache_size: DEFAULT_CROSS_BLOCK_CACHE_SIZE_MB,
            accept_execution_requests_hash: false,
            multiproof_chunking_enabled: false,
            multiproof_chunk_size: 0,
            reserved_cpu_cores: DEFAULT_RESERVED_CPU_CORES,
            state_root_fallback: false,
            precompile_cache_disabled: false,
            always_process_payload_attributes_on_canonical_head: false,
            allow_unwind_canonical_header: false,
            storage_worker_count: None,
            account_worker_count: None,
            disable_proof_v2: false,
            cache_metrics_disabled: false,
            disable_trie_cache: false,
            sparse_trie_prune_depth: DEFAULT_SPARSE_TRIE_PRUNE_DEPTH,
            sparse_trie_max_storage_tries: DEFAULT_SPARSE_TRIE_MAX_STORAGE_TRIES,
            disable_sparse_trie_cache_pruning: false,
            state_root_task_timeout: None,
            // deprecated fields
            caching_and_prewarming_enabled: true,
            parallel_sparse_trie_enabled: false,
            parallel_sparse_trie_disabled: true,
            precompile_cache_enabled: true,
        };

        // metrics args
        //
        // NOTE: dead config in TN's hand-rolled init path - the node never launches reth's
        // metric server. The prometheus endpoint is owned by `tn-metrics` (`--metrics` CLI arg).
        let metrics = MetricArgs {
            prometheus: None,
            push_gateway_url: None,
            push_gateway_interval: Duration::from_mins(5), // reth default
        };

        // static files
        let static_files = StaticFilesArgs {
            blocks_per_file_headers: None,
            blocks_per_file_transactions: None,
            blocks_per_file_receipts: None,
            blocks_per_file_transaction_senders: None,
            blocks_per_file_account_change_sets: None,
            blocks_per_file_storage_change_sets: None,
        };

        // storage
        let storage = StorageArgs { v2: false };

        // Parameters to configure block history syncing.
        let era = EraArgs { enabled: false, source: EraSourceArgs { path: None, url: None } };

        let mut this = NodeConfig {
            config: None,
            chain,
            metrics,
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
            era,
            static_files,
            storage,
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
///
/// `RethEnv` wraps its fields in an `Arc<RethEnvInner>` so that cloning is cheap
/// (just an `Arc` bump) and the type is trivially `Send + Sync`.
#[derive(Clone)]
pub struct RethEnv {
    /// The inner state wrapped in an Arc for cheap cloning and thread safety.
    inner: Arc<RethEnvInner>,
}

/// Inner state for [`RethEnv`].
///
/// This struct holds all the actual fields and is wrapped in an `Arc` by `RethEnv`.
struct RethEnvInner {
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
        write!(f, "RethEnv, config: {:?}", self.inner.node_config)
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

/// Report sampled reth database metrics (table sizes, page usage, freelist).
///
/// Intended as a pre-scrape hook for the prometheus metrics endpoint. Lives here to keep
/// reth-db types out of the node crate.
pub fn report_db_metrics(db: &RethDb) {
    use reth_db::database_metrics::DatabaseMetrics;
    db.report_metrics();
}

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
        // with_metrics: record per-operation db latency metrics (noop unless the global
        // metrics recorder is installed, i.e. the node runs with `--metrics`)
        Ok(Arc::new(init_db(db_path, reth_config.0.db.database_args())?.with_metrics()))
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

        Ok(Self {
            inner: Arc::new(RethEnvInner {
                node_config,
                blockchain_provider,
                evm_config,
                task_spawner,
            }),
        })
    }

    /// Initialize the provider factory and related components
    fn init_provider_factory(
        node_config: &NodeConfig<RethChainSpec>,
        database: Arc<DatabaseEnv>,
    ) -> eyre::Result<ProviderFactory<TelcoinNode>> {
        // Initialize provider factory with static files
        let datadir = node_config.datadir();
        let rocksdb_provider = reth_provider::providers::RocksDBProvider::new(datadir.data_dir())?;
        let runtime = reth_tasks::Runtime::with_existing_handle(tokio::runtime::Handle::current())?;
        let provider_factory = ProviderFactory::new(
            database,
            Arc::clone(&node_config.chain),
            StaticFileProvider::read_write(datadir.static_files())?,
            rocksdb_provider,
            runtime,
        )?;

        // Initialize genesis if needed
        let genesis_hash = init_genesis(&provider_factory)?;
        debug!(target: "tn::execution", chain=%node_config.chain.chain, ?genesis_hash, "Initialized genesis");

        Ok(provider_factory)
    }

    /// Initialize a new transaction pool for worker.
    pub fn init_txn_pool(&self) -> eyre::Result<WorkerTxPool> {
        WorkerTxPool::new(
            &self.inner.node_config,
            &self.inner.task_spawner,
            &self.inner.blockchain_provider,
            &self.inner.evm_config,
        )
    }

    /// Return a channel reciever that will return each canonical block in turn.
    pub fn canonical_block_stream(&self) -> CanonStateNotificationStream {
        self.inner.blockchain_provider.canonical_state_stream()
    }

    /// Return a reference to the [TaskSpawner] for spawning tasks.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.inner.task_spawner
    }

    /// Return the chainspec for this instance.
    pub fn chainspec(&self) -> ChainSpec {
        ChainSpec(self.inner.node_config.chain.clone())
    }

    /// Return the canonical in-memory state.
    pub fn canonical_in_memory_state(&self) -> CanonicalInMemoryState {
        self.inner.blockchain_provider.canonical_in_memory_state()
    }

    /// Construct a canonical block from a worker's block that reached consensus.
    pub fn build_block_from_batch_payload(
        &self,
        payload: TNPayload,
        transactions: &Vec<Vec<u8>>,
        anchor_hash: B256,
        ancestors: &[DeferredTrieData],
    ) -> TnRethResult<ExecutedBlock> {
        let parent_header = payload.parent_header.clone();
        debug!(target: "engine", ?parent_header, "retrieving state for next block");
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(parent_header.hash())?;
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

        // copy in case of error
        let batch_digest = payload.batch_digest;

        let mut builder =
            self.inner.evm_config.builder_for_next_block(&mut db, &parent_header, payload)?;

        builder.apply_pre_execution_changes().inspect_err(|err| {
            warn!(target: "engine", %err, "failed to apply pre-execution changes");
        })?;

        // Phase 1: Recover all transactions (ECDSA ecrecover) in parallel via rayon.
        // Always use par_iter — the slight overhead on small batches is negligible
        // compared to the savings on large ones, and avoids an extra code path.
        //
        // A transaction whose signer cannot be recovered cannot be executed, but a
        // certified sub-DAG is fixed and identical on every honest node, so returning
        // an error here would deterministically halt (and, on restart-replay,
        // crash-loop) the whole network on a single undecodable transaction. Instead
        // drop the unrecoverable transactions and continue, mirroring the `InvalidTx`
        // tolerance of the execute phase below: every node drops the same
        // transactions from the same certified bytes, so the resulting block stays
        // deterministic (issue #933). The primary defense is validating batches before
        // they can be certified; this only bounds the blast radius if one ever is.
        let recovered_txs = transactions
            .par_iter()
            .filter_map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .inspect_err(|e| {
                        error!(
                            target: "engine",
                            batch=?batch_digest,
                            ?tx_bytes,
                            "failed to recover signer, dropping transaction: {e}"
                        )
                    })
                    .ok()
            })
            .collect::<Vec<_>>();

        // Phase 2: Execute recovered transactions sequentially.
        for recovered in recovered_txs {
            // forks are impossible
            match builder.execute_transaction(recovered.clone()) {
                Ok(_gas_used) => (),
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
            }
        }

        let BlockBuilderOutcome { execution_result, block, hashed_state, trie_updates } =
            builder.finish(&state_provider)?;

        debug!(target: "engine", hash=?block.hash(), "block builder outcome");
        let block_execution_output =
            BlockExecutionOutput { result: execution_result, state: db.take_bundle() };
        let computed_trie_data = DeferredTrieData::sort_and_build_trie_input(
            Arc::new(hashed_state),
            Arc::new(trie_updates),
            anchor_hash,
            ancestors,
        );
        let res: ExecutedBlock<TNPrimitives> = ExecutedBlock::new(
            Arc::new(block),
            Arc::new(block_execution_output),
            computed_trie_data,
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
        let provider = self.inner.blockchain_provider.database_provider_rw()?;
        provider.save_finalized_block_number(header.number)?;
        // this clears up old blocks in-memory
        self.inner.blockchain_provider.set_finalized(header.clone());

        // update safe block last because this is less time sensitive but still needs to happen
        provider.save_safe_block_number(header.number)?;
        self.inner.blockchain_provider.set_safe(header);

        // commit db transaction
        provider.commit()?;

        // cleanup chain state in memory
        // this returns the `canonical_chain().count()` back to 0
        self.inner
            .blockchain_provider
            .canonical_in_memory_state()
            .remove_persisted_blocks(num_hash);

        Ok(())
    }

    /// Advance a lagging finalized marker to the persisted canonical tip.
    ///
    /// Blocks commit in [`Self::finish_executing_output`] and the finalized/safe markers commit
    /// in [`Self::finalize_block`] — two separate database transactions. A crash between them
    /// restarts the node with `finalized marker < persisted canonical tip`, hiding the gap
    /// blocks from every marker reader (accumulator catchup, the tx-pool's last-seen seed, the
    /// RPC `finalized`/`safe` tags).
    ///
    /// Advancing the marker is sound because every persisted canonical block is consensus-final
    /// by construction: blocks only enter the canonical database from committed consensus
    /// output, so there is no speculative or reorgable segment the marker could overrun. The
    /// lag is purely a two-transaction persistence artifact.
    ///
    /// The heal MUST route through [`Self::finalize_block`] rather than a bare
    /// `save_finalized_block_number` write: the in-memory finalized/safe watches are seeded
    /// from the (stale) database at provider construction, and startup marker readers like the
    /// accumulator catchup consult the watch — a DB-only write would leave them reading the
    /// stale value.
    ///
    /// Replay is unaffected: the missed-consensus watermark derives from the persisted
    /// execution tip (which this method never moves), not the finalized marker, so healing
    /// cannot cause `replay_missed_consensus` to re-forward committed output — no
    /// double-execution is possible.
    ///
    /// Call once at startup, before anything reads the marker and before any consensus output
    /// executes. A marker ahead of the tip fails with
    /// [`TnRethError::FinalizedMarkerAheadOfTip`]: the execution database lost blocks this node
    /// already attested as final.
    ///
    /// Like every startup tip reader, this trusts the persisted tip itself; a torn write
    /// between MDBX and static files below the provider is a pre-existing exposure this method
    /// neither adds to nor guards against.
    pub fn heal_finalized_to_persisted_tip(&self) -> TnRethResult<()> {
        // One RO provider = one consistent MDBX snapshot for all three reads. Read errors
        // PROPAGATE: the public last_block_number()/last_finalized_block_number() wrappers
        // unwrap_or(0) and would misdiagnose a failed read as tip=0 here.
        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let tip = provider.last_block_number()?;
        // None = never finalized (fresh genesis). Some(0) is unreachable in production (the
        // first finalize_block call is for block >= 1), so collapsing None to 0 is safe.
        let finalized = provider.last_finalized_block_number()?.unwrap_or(0);

        if finalized > tip {
            return Err(TnRethError::FinalizedMarkerAheadOfTip { finalized, tip });
        }
        if finalized == tip {
            // Normal restart (marker caught up), or fresh genesis (marker never written and
            // tip = 0): genesis intentionally stays unfinalized so
            // finalized_block_hash_number_for_startup keeps its genesis fallback.
            return Ok(());
        }

        let header =
            provider.sealed_header(tip)?.ok_or(ProviderError::HeaderNotFound(tip.into()))?;
        drop(provider);
        warn!(
            target: "tn::reth",
            finalized,
            tip,
            "finalized marker lags the persisted canonical tip (crash between the \
             blocks-commit and finalize-commit transactions); healing marker to tip"
        );
        self.finalize_block(header)
    }

    /// This makes all blocks canonical, commits them to the database,
    /// broadcasts new chain on `canon_state_notification_sender`
    /// and set last executed header as the tracked header.
    ///
    /// It also clears the canonical in-memory state.
    pub fn finish_executing_output(
        &self,
        blocks: Vec<ExecutedBlock>,
        engine_update: Option<(Round, ConsensusNumHash, tokio::sync::mpsc::Sender<EngineUpdate>)>,
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
        let provider_rw = self.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.save_blocks(blocks.clone(), reth_provider::SaveBlocksMode::Full)?;
        provider_rw.commit()?;

        // process update
        //
        // see reth::EngineApiTreeHandler::on_canonical_chain_update
        let chain_update = NewCanonicalChain::Commit { new: blocks };
        let canonical_head = chain_update.tip();
        let (epoch, round) =
            deconstruct_nonce(<FixedBytes<8> as Into<u64>>::into(canonical_head.nonce));
        info!(
            target: "engine",
            "canonical head for epoch {:?} round {:?}: {:?} - {:?}",
            epoch,
            round,
            canonical_head.number,
            canonical_head.hash(),
        );

        if let Some((leader_round, consensus_num_hash, engine_update_tx)) = engine_update {
            engine_update_tx
                .blocking_send((
                    leader_round,
                    consensus_num_hash,
                    Some(canonical_head.clone_sealed_header()),
                ))
                .map_err(|e| {
                    error!(target: "engine", ?e, "engine update channel send failed");
                    TnRethError::EngineUpdateChannelClosed
                })?;
        }

        // broadcast canonical update
        let notification = chain_update.to_chain_notification();
        self.canonical_in_memory_state().notify_canon_state(notification);

        Ok(())
    }

    /// Look up and return the sealed header for hash.
    pub fn sealed_header_by_hash(&self, hash: B256) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.inner.blockchain_provider.sealed_header_by_hash(hash)?)
    }

    /// Look up and return the sealed header for block number.
    pub fn sealed_header_by_number(&self, number: u64) -> TnRethResult<Option<SealedHeader>> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.sealed_header(number)?)
    }

    /// Look up and return the sealed block for number.
    pub fn sealed_block_by_number(&self, number: u64) -> TnRethResult<Option<SealedBlock>> {
        Ok(self
            .inner
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
        Ok(self
            .inner
            .blockchain_provider
            .sealed_block_with_senders(id, TransactionVariant::NoHash)?)
    }

    /// Return the blocks with senders for a range of block numbers.
    pub fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<BlockWithSenders>> {
        Ok(self.inner.blockchain_provider.block_with_senders_range(range)?)
    }

    /// Return the blocks for a range of block numbers.
    pub fn blocks_for_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> TnRethResult<Vec<SealedHeader>> {
        Ok(self.inner.blockchain_provider.sealed_headers_range(range)?)
    }

    /// Build a [`Chain`] from a historical block in the database.
    ///
    /// Reads the block with recovered senders and its receipts, then constructs
    /// a `Chain` suitable for ExEx replay notifications.
    ///
    /// Returns `None` if the block does not exist in the database.
    ///
    /// # Replay fidelity
    ///
    /// The returned `Chain` carries an **empty `BundleState`**: account/storage
    /// diffs are already committed to the DB at execution time and are not
    /// re-derived here. ExExes that need historical state diffs must read them
    /// from the provider by block number. Live `ChainExecuted` notifications
    /// (from `finish_executing_output`) *do* carry the full `BundleState`.
    pub fn replay_block_as_chain(
        &self,
        block_number: BlockNumber,
    ) -> TnRethResult<Option<Arc<reth_provider::Chain>>> {
        // Read block with senders
        let Some(block) = self.inner.blockchain_provider.sealed_block_with_senders(
            BlockHashOrNumber::Number(block_number),
            TransactionVariant::NoHash,
        )?
        else {
            return Ok(None);
        };

        // Read receipts for this block. The block exists (read above), so missing
        // receipts indicate a DB inconsistency; an empty block legitimately
        // returns `Some(vec![])`. Treat `None` as an error rather than silently
        // yielding an empty receipt set (which would make a non-empty block look
        // empty to a stateful indexer).
        let receipts = self
            .inner
            .blockchain_provider
            .receipts_by_block(BlockHashOrNumber::Number(block_number))?
            .ok_or(TnRethError::ReplayReceiptsMissing(block_number))?;

        // Construct a minimal ExecutionOutcome with receipts only (no bundle state)
        let execution_outcome = ExecutionOutcome::new(
            Default::default(), // empty BundleState — state already committed to DB
            vec![receipts],
            block_number,
            Vec::new(), // no requests
        );

        Ok(Some(Arc::new(Chain::new(vec![block], execution_outcome, Default::default()))))
    }

    /// Return the head header from the reth db.
    pub fn lookup_head(&self) -> TnRethResult<SealedHeader> {
        let head = self.inner.node_config.lookup_head(&self.inner.blockchain_provider)?;
        let header = self
            .inner
            .blockchain_provider
            .sealed_header(head.number)?
            .expect("Failed to retrieve sealed header from head's block number");
        Ok(header)
    }

    /// If a dubug max round is set then return it.
    pub fn get_debug_max_round(&self) -> Option<u64> {
        self.inner.node_config.debug.max_block
    }

    /// Helper to get the gas price based on the provider's latest header.
    pub fn get_gas_price(&self) -> TnRethResult<u128> {
        let header = self.lookup_head()?;
        Ok(header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into())
    }

    /// Return the execution header for hash if available.
    pub fn header(&self, hash: B256) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.inner.blockchain_provider.header(hash)?)
    }

    /// Return the execution header for block number if available.
    pub fn header_by_number(&self, block_num: u64) -> TnRethResult<Option<ExecHeader>> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.header_by_number(block_num)?)
    }

    /// Return the finalized header, sealed with its hash, if available.
    ///
    /// Number and hash come from one logical read (the finalized num/hash pair, then the header
    /// looked up by that hash), so callers can pin block ranges, epoch classification, and state
    /// reads to this single header without consulting a second source (see `catchup_accumulator`
    /// and the epoch-entry base-fee seeding in the epoch manager, which pair this with
    /// [`Self::epoch_state_at_header`]).
    pub fn finalized_header(&self) -> TnRethResult<Option<SealedHeader>> {
        let Some(finalized_num_hash) = self.finalized_block_num_hash()? else {
            return Ok(None);
        };
        self.sealed_header_by_hash(finalized_num_hash.hash)
    }

    /// Return the latest canonical block number.
    pub fn last_block_number(&self) -> TnRethResult<u64> {
        Ok(self.inner.blockchain_provider.database_provider_ro()?.last_block_number().unwrap_or(0))
    }

    /// Return the block number and hash for the current canonical tip.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn canonical_tip(&self) -> SealedHeader {
        self.inner.blockchain_provider.canonical_in_memory_state().get_canonical_head()
    }

    /// If available return the finalized block number and hash.
    ///
    /// This checks the canonical-in-memory-state.
    pub fn finalized_block_num_hash(&self) -> TnRethResult<Option<BlockNumHash>> {
        Ok(self.inner.blockchain_provider.finalized_block_num_hash()?)
    }

    /// Returns the block number of the last finialized block.
    pub fn last_finalized_block_number(&self) -> TnRethResult<u64> {
        Ok(self
            .inner
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
            .inner
            .blockchain_provider
            .finalized_block_hash()?
            .unwrap_or_else(|| self.inner.node_config.chain.sealed_genesis_header().hash());
        let number = self.inner.blockchain_provider.finalized_block_number()?.unwrap_or_default();
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
            TnEvmConfig,
        > = transaction_pool.into();
        let tn_execution = Arc::new(TNExecution);
        let rpc_builder = RpcModuleBuilder::default()
            .with_provider(self.inner.blockchain_provider.clone())
            .with_pool(transaction_pool.clone())
            .with_network(network.clone())
            .with_executor(Box::new(self.inner.task_spawner.clone()))
            .with_evm_config(self.inner.evm_config.clone())
            .with_consensus(tn_execution.clone());

        let modules_config = self.inner.node_config.rpc.transport_rpc_module_config();
        let eth_api = EthApi::builder(
            self.inner.blockchain_provider.clone(),
            transaction_pool,
            network,
            self.inner.evm_config.clone(),
        )
        .build();

        let engine_events = reth_tokio_util::EventSender::default();
        let mut server = rpc_builder.build(modules_config, eth_api, engine_events);
        if let Err(e) = server.merge_configured(other) {
            tracing::error!(target: "tn::execution", "Error merging TN rpc module: {e:?}");
        }

        server
    }

    /// Start running the RPC server for this instance.
    pub async fn start_rpc(&self, server: &RpcServer) -> TnRethResult<RpcServerHandle> {
        let server_config = self.inner.node_config.rpc.rpc_server_config();
        Ok(server_config.start(server).await?)
    }

    /// Provide the state for the latest block in this instance.
    pub fn latest(&self) -> TnRethResult<StateProviderBox> {
        Ok(self.inner.blockchain_provider.latest()?)
    }

    /// Look up a transaction by hash, returning it with its sender and block metadata
    /// (block number/hash, index in block, base fee, block timestamp).
    pub fn transaction_by_hash_with_meta(
        &self,
        hash: TxHash,
    ) -> TnRethResult<Option<(Recovered<TransactionSigned>, TransactionMeta)>> {
        let Some((tx, meta)) =
            self.inner.blockchain_provider.transaction_by_hash_with_meta(hash)?
        else {
            return Ok(None);
        };

        // prefer the sender persisted at execution time (two point reads, no ecrecover)
        let sender = match self.inner.blockchain_provider.transaction_id(hash)? {
            Some(id) => self.inner.blockchain_provider.transaction_sender(id)?,
            None => None,
        };
        let sender = match sender {
            Some(sender) => sender,
            None => tx.recover_signer()?,
        };

        Ok(Some((Recovered::new_unchecked(tx, sender), meta)))
    }

    /// Return the receipt for a transaction hash, if the transaction has been mined.
    ///
    /// `Receipt::cumulative_gas_used` is cumulative within the block; use
    /// [`Self::receipt_by_hash_with_gas_used`] for this transaction's own gas.
    pub fn receipt_by_hash(&self, hash: TxHash) -> TnRethResult<Option<Receipt>> {
        Ok(self.inner.blockchain_provider.receipt_by_hash(hash)?)
    }

    /// Return the receipt for a transaction hash together with the gas used by that
    /// transaction alone (the delta of cumulative gas vs. the previous receipt in the block).
    pub fn receipt_by_hash_with_gas_used(
        &self,
        hash: TxHash,
    ) -> TnRethResult<Option<(Receipt, u64)>> {
        let Some(id) = self.inner.blockchain_provider.transaction_id(hash)? else {
            return Ok(None);
        };
        let Some(receipt) = self.inner.blockchain_provider.receipt(id)? else {
            return Ok(None);
        };
        let block = self
            .inner
            .blockchain_provider
            .block_by_transaction_id(id)?
            .ok_or(ProviderError::TransactionNotFound(id.into()))?;
        let indices = self
            .inner
            .blockchain_provider
            .block_body_indices(block)?
            .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;

        let gas_used = if id == indices.first_tx_num() {
            receipt.cumulative_gas_used
        } else {
            let prev = self
                .inner
                .blockchain_provider
                .receipt(id - 1)?
                .ok_or(ProviderError::ReceiptNotFound((id - 1).into()))?;
            receipt.cumulative_gas_used.saturating_sub(prev.cumulative_gas_used)
        };

        Ok(Some((receipt, gas_used)))
    }

    /// Return all receipts for a block by hash or number.
    ///
    /// `Ok(None)` means the block is unknown; an empty vec means the block has no transactions.
    pub fn receipts_by_block(
        &self,
        block: BlockHashOrNumber,
    ) -> TnRethResult<Option<Vec<Receipt>>> {
        Ok(self.inner.blockchain_provider.receipts_by_block(block)?)
    }

    /// Total number of transactions ever mined on the canonical chain.
    ///
    /// The latest transaction's [`TxNumber`] is `total - 1`; serve "latest N, newest-first"
    /// pages by reading descending [`TxNumber`] ranges via
    /// [`Self::transactions_by_tx_range_with_meta`].
    pub fn total_transactions(&self) -> TnRethResult<u64> {
        let tip = self.last_block_number()?;
        Ok(self
            .inner
            .blockchain_provider
            .block_body_indices(tip)?
            .map(|indices| indices.next_tx_num())
            .unwrap_or(0))
    }

    /// Return the transactions in a chain-wide [`TxNumber`] range (inclusive), each with its
    /// sender and containing-block metadata.
    ///
    /// Cost scales with the number of transactions returned, not the number of blocks the
    /// range spans: empty blocks are never visited. Ranges extending past the newest
    /// transaction are clamped to what exists.
    pub fn transactions_by_tx_range_with_meta(
        &self,
        range: RangeInclusive<TxNumber>,
    ) -> TnRethResult<Vec<TxFeedEntry>> {
        if range.is_empty() {
            return Ok(Vec::new());
        }
        let start = *range.start();
        let txs = self.inner.blockchain_provider.transactions_by_tx_range(range.clone())?;
        if txs.is_empty() {
            return Ok(Vec::new());
        }
        let mut senders = self.inner.blockchain_provider.senders_by_tx_range(range)?;
        if senders.len() != txs.len() {
            // defensive: persistence always writes senders alongside transactions, so a
            // misaligned read should be impossible — recover in parallel if it happens
            senders = txs
                .par_iter()
                .map(|tx| tx.recover_signer().map_err(TnRethError::from))
                .collect::<TnRethResult<Vec<_>>>()?;
        }

        let mut entries = Vec::with_capacity(txs.len());
        // cached (block_number, first_tx_num, next_tx_num, block_hash, timestamp) for the block
        // containing the previous transaction; refreshed only when the cursor crosses into the
        // next non-empty block, so each non-empty block costs three point reads and empty
        // blocks are never visited (the `TransactionBlocks` seek skips them by construction).
        let mut block_ctx: Option<(BlockNumber, TxNumber, TxNumber, B256, u64)> = None;
        for (offset, (tx, sender)) in txs.into_iter().zip(senders).enumerate() {
            let tx_number = start + offset as u64;
            let (block_number, first_tx_num, _, block_hash, timestamp) = match block_ctx {
                Some(ctx) if tx_number < ctx.2 => ctx,
                _ => {
                    let block = self
                        .inner
                        .blockchain_provider
                        .block_by_transaction_id(tx_number)?
                        .ok_or(ProviderError::TransactionNotFound(tx_number.into()))?;
                    let indices = self
                        .inner
                        .blockchain_provider
                        .block_body_indices(block)?
                        .ok_or(ProviderError::BlockBodyIndicesNotFound(block))?;
                    let header = self
                        .inner
                        .blockchain_provider
                        .sealed_header(block)?
                        .ok_or(ProviderError::HeaderNotFound(block.into()))?;
                    let ctx = (
                        block,
                        indices.first_tx_num(),
                        indices.next_tx_num(),
                        header.hash(),
                        header.timestamp,
                    );
                    block_ctx = Some(ctx);
                    ctx
                }
            };
            entries.push(TxFeedEntry {
                tx_number,
                transaction: Recovered::new_unchecked(tx, sender),
                block_number,
                block_hash,
                timestamp,
                index: tx_number.saturating_sub(first_tx_num),
            });
        }

        Ok(entries)
    }

    /// Return balance, nonce, and code hash for an account at the latest canonical state.
    pub fn retrieve_account(&self, address: &Address) -> TnRethResult<Option<Account>> {
        Ok(self.inner.blockchain_provider.basic_account(address)?)
    }

    /// Return the contract bytecode deployed at `address` at the latest canonical state
    /// (`eth_getCode` equivalent). `None` for EOAs and unknown accounts.
    pub fn account_code(&self, address: &Address) -> TnRethResult<Option<Bytes>> {
        Ok(self.latest()?.account_code(address)?.map(|code| code.original_bytes()))
    }

    /// Create a new temp RethEnv using a specified chain spec.
    pub fn new_for_temp_chain<P: AsRef<Path>>(
        chain: Arc<RethChainSpec>,
        db_path: P,
        task_manager: &TaskManager,
        rewards: Option<RewardsCounter>,
    ) -> eyre::Result<Self> {
        /// MDBX map-size ceiling for throwaway temp-chain envs. reth defaults to 8 TB per
        /// environment; `cargo test` runs a test binary as threads in ONE process, so N
        /// concurrent 8 TB virtual reservations exhaust the address space and MDBX aborts
        /// env-open with ENOMEM ("Cannot allocate memory (12)"). A temp chain never holds a
        /// full node's history, so a small ceiling is safe and lets many envs coexist.
        const TEMP_CHAIN_DB_MAX_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2 GiB
        /// Grow the temp DB file in small increments so throwaway DBs stay tiny on disk
        /// (reth pairs its 8 TB default with a 4 GiB step; mirror reth's `test()` 4 MiB step).
        const TEMP_CHAIN_DB_GROWTH_STEP: usize = 4 * 1024 * 1024; // 4 MiB

        let node_config = NodeConfig {
            datadir: DatadirArgs {
                datadir: MaybePlatformPath::from(db_path.as_ref().to_path_buf()),
                // default static path should resolve to: `DEFAULT_ROOT_DIR/<CHAIN_ID>/static_files`
                static_files_path: None,
                rocksdb_path: None,
                pprof_dumps_path: None,
            },
            chain,
            // Bound the MDBX geometry for throwaway temp chains (see const docs).
            db: DatabaseArgs {
                max_size: Some(TEMP_CHAIN_DB_MAX_SIZE),
                growth_step: Some(TEMP_CHAIN_DB_GROWTH_STEP),
                ..Default::default()
            },
            ..NodeConfig::default()
        };
        let reth_config = RethConfig(node_config);
        let database = Self::new_database(&reth_config, db_path)?;
        Self::new(&reth_config, task_manager, database, None, rewards.unwrap_or_default())
    }

    /// Convenience method for compiling storage and bytecode to include consensus registry
    /// configuration in genesis.
    pub fn create_consensus_registry_genesis_accounts(
        validators: Vec<NodeInfo>,
        genesis: Genesis,
        initial_stake_config: ConsensusRegistry::StakeConfig,
        owner_address: Address,
        worker_configs: Vec<(u8, u64)>,
    ) -> eyre::Result<Genesis> {
        // create temporary reth env for execution
        let tmp_chain: Arc<RethChainSpec> = Arc::new(genesis.clone().into());
        let task_manager = TaskManager::new("Temp Task Manager");
        let tmp_dir = TempDir::new().unwrap();
        let reth_env =
            RethEnv::new_for_temp_chain(tmp_chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();

        // The BLS proof-of-possession precompile is registered by the EVM factory at
        // `BLS_G1_PRECOMPILE_ADDRESS`, so there is nothing to deploy at genesis. The registry
        // constructor's PoP verification staticcalls this address and is served natively by the
        // precompile, which shares the exact `blst` verification the consensus layer uses to
        // produce the proofs (so the on-chain check cannot drift from the off-chain
        // encoding).
        let blsg1_address = BLS_G1_PRECOMPILE_ADDRESS;

        // prepare registry deployment
        let (validators, (bls_pubkeys, proofs)): (Vec<_>, (Vec<_>, Vec<_>)) = validators
            .iter()
            .map(|v| {
                let validator = ConsensusRegistry::ValidatorInfo {
                    validatorAddress: v.execution_address,
                    activationEpoch: 0,
                    exitEpoch: 0,
                    currentStatus: ConsensusRegistry::ValidatorStatus::Active,
                    isRetired: false,
                    stakeVersion: 0,
                    region: 0,
                };
                let bls_pubkey: tn_types::Bytes = v.bls_public_key.to_bytes().into();
                let proof = ConsensusRegistry::ProofOfPossession {
                    signature: v.proof_of_possession.to_bytes().into(),
                };

                (validator, (bls_pubkey, proof))
            })
            .unzip();

        let total_stake_balance = initial_stake_config
            .stakeAmount
            .checked_mul(U256::from(validators.len()))
            .ok_or_eyre("Failed to calculate total stake for consensus registry at genesis")?;
        debug!(target: "engine", ?initial_stake_config, "calling constructor for consensus registry");

        let constructor_args = ConsensusRegistry::constructorCall {
            genesisConfig_: initial_stake_config,
            initialValidators_: validators,
            blsPubkeys_: bls_pubkeys,
            proofsOfPossession: proofs,
            owner_: owner_address,
        }
        .abi_encode();

        // generate calldata for creation
        let registry_initcode_binding =
            Self::fetch_value_from_json_str(CONSENSUS_REGISTRY_JSON, Some("bytecode.object"))?;
        let registry_initcode_str =
            registry_initcode_binding.as_str().ok_or_eyre("invalid registry json")?;
        // The registry calls the BLS precompile directly at `BLS_G1_ADDRESS` (no linked library),
        // so its bytecode carries no link placeholder; deploy it as-is. Guard against a
        // stale artifact that still contains an unresolved `__$..$__` placeholder.
        if registry_initcode_str.contains("__$") {
            eyre::bail!(
                "ConsensusRegistry initcode has an unresolved library link placeholder; regenerate tn-contracts artifacts"
            );
        }
        let mut create_registry = hex::decode(registry_initcode_str)?;
        create_registry.extend(constructor_args);

        // after adding bls proof of possession, registry precompile exceeds size limit so disable
        // it for tmp chain
        let mut tmp_evm_no_eip170 =
            reth_env.inner.evm_config.evm_env(&tmp_chain.sealed_genesis_header())?;
        tmp_evm_no_eip170.cfg_env.limit_contract_code_size = Some(0x12000000);

        // deploy registry now that it can use the previously deployed blsg1 lib
        let tmp_registry_address = {
            let mut tn_evm =
                reth_env.inner.evm_config.evm_factory().create_evm(&mut db, tmp_evm_no_eip170);
            let ResultAndState { result, state } =
                tn_evm.transact_pre_genesis_create(owner_address, create_registry.into())?;
            debug!(target: "engine", "create consensus registry result:\n{:#?}", result);
            Self::ensure_pre_genesis_create_success("ConsensusRegistry", &result)?;

            tn_evm.db_mut().commit(state);

            // With BlsG1 now a precompile (no genesis deploy), the registry is the owner's first
            // create tx on the tmp chain.
            owner_address.create(0)
        };

        // deploy WorkerConfigs contract
        let tmp_worker_configs_address = {
            let mut tn_evm = reth_env.inner.evm_config.evm_factory().create_evm(
                &mut db,
                reth_env.inner.evm_config.evm_env(&tmp_chain.sealed_genesis_header())?,
            );

            let (strategies, values): (Vec<u8>, Vec<u64>) = worker_configs.iter().copied().unzip();
            let datas = vec![0u128; strategies.len()];
            let constructor_args =
                WorkerConfigs::constructorCall { strategies, values, datas, owner_: owner_address }
                    .abi_encode();

            let initcode_binding =
                Self::fetch_value_from_json_str(WORKER_CONFIGS_JSON, Some("bytecode.object"))?;
            let initcode =
                hex::decode(initcode_binding.as_str().ok_or_eyre("invalid worker configs json")?)?;
            let mut create_worker_configs = initcode;
            create_worker_configs.extend(constructor_args);

            let ResultAndState { result, state } =
                tn_evm.transact_pre_genesis_create(owner_address, create_worker_configs.into())?;
            debug!(target: "engine", "create worker configs result:\n{:#?}", result);
            Self::ensure_pre_genesis_create_success("WorkerConfigs", &result)?;

            tn_evm.db_mut().commit(state);

            // Second create tx on tmp chain (registry at nonce 0, worker configs at nonce 1).
            owner_address.create(1)
        };

        // execute the transactions to get final bundle state
        db.merge_transitions(BundleRetention::PlainState);
        let BundleState { state, contracts, reverts, state_size, reverts_size } = db.take_bundle();

        debug!(target: "engine", "contracts:\n{:#?}", contracts);
        debug!(target: "engine", "reverts:\n{:#?}", reverts);
        debug!(target: "engine", "state_size:{:#?}", state_size);
        debug!(target: "engine", "reverts_size:{:#?}", reverts_size);

        // construct real genesis using known values & tmp chain storage result
        let tmp_registry_storage = state.get(&tmp_registry_address).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });
        let registry_runtimecode_binding = Self::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let registry_runtimecode_str =
            registry_runtimecode_binding.as_str().ok_or_eyre("invalid registry json")?;
        let registry_runtimecode = hex::decode(registry_runtimecode_str)?;

        let tmp_worker_configs_storage = state.get(&tmp_worker_configs_address).map(|account| {
            account.storage.iter().map(|(k, v)| ((*k).into(), v.present_value.into())).collect()
        });
        let worker_configs_runtimecode_binding =
            Self::fetch_value_from_json_str(WORKER_CONFIGS_JSON, Some("deployedBytecode.object"))?;
        let worker_configs_runtimecode = hex::decode(
            worker_configs_runtimecode_binding
                .as_str()
                .ok_or_eyre("invalid worker configs json")?,
        )?;

        let issuance_json_binding =
            Self::fetch_value_from_json_str(ISSUANCE_JSON, Some("deployedBytecode.object"))?;
        let issuance_runtimecode =
            hex::decode(issuance_json_binding.as_str().ok_or_eyre("invalid issuance json")?)?;
        let genesis = genesis.extend_accounts([
            // The BLS proof-of-possession precompile lives at `blsg1_address`
            // (`BLS_G1_PRECOMPILE_ADDRESS`). Mirror the TEL precompile and give it a single `0xfe`
            // (INVALID) byte of code so the account is non-empty (never state-pruned) and any call
            // that bypasses precompile dispatch reverts instead of succeeding against an EOA.
            (
                blsg1_address,
                GenesisAccount::default().with_code(Some(PRECOMPILE_GENESIS_BYTECODE.into())),
            ),
            (
                CONSENSUS_REGISTRY_ADDRESS,
                GenesisAccount::default()
                    .with_balance(U256::from(total_stake_balance))
                    .with_code(Some(registry_runtimecode.into()))
                    .with_storage(tmp_registry_storage),
            ),
            (
                ISSUANCE_ADDRESS,
                GenesisAccount::default().with_code(Some(issuance_runtimecode.into())),
            ),
            (
                WORKER_CONFIGS_ADDRESS,
                GenesisAccount::default()
                    .with_code(Some(worker_configs_runtimecode.into()))
                    .with_storage(tmp_worker_configs_storage),
            ),
        ]);

        Ok(genesis)
    }

    /// Bail unless a pre-genesis constructor transaction succeeded.
    ///
    /// Pre-genesis creates are committed straight into genesis storage. An unchecked
    /// Revert/Halt would still ship the contract's runtime code but with EMPTY storage
    /// (e.g. an ownerless, zero-worker `WorkerConfigs`), which downstream fail-open reads
    /// mask as defaults — so the ceremony must fail loudly here instead.
    fn ensure_pre_genesis_create_success(
        contract: &str,
        result: &ExecutionResult,
    ) -> eyre::Result<()> {
        match result {
            ExecutionResult::Success { .. } => Ok(()),
            ExecutionResult::Revert { output, .. } => {
                let reason = alloy::sol_types::decode_revert_reason(output)
                    .unwrap_or_else(|| "<undecodable revert reason>".to_string());
                eyre::bail!(
                    "{contract} constructor reverted during pre-genesis create: {reason} (revert output: {output})"
                )
            }
            ExecutionResult::Halt { reason, gas_used } => eyre::bail!(
                "{contract} constructor halted during pre-genesis create: {reason:?} (gas used: {gas_used})"
            ),
        }
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
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "retrieving epoch state from canonical tip");
        self.epoch_state_at_header(&canonical_tip)
    }

    /// Read the committee and epoch information from the [ConsensusRegistry] at `header`.
    ///
    /// The registry state, the EVM environment, and therefore the returned epoch, epoch info,
    /// and committee all derive from this ONE header. Recovery paths that scan
    /// `epoch_info.blockHeight..=header.number` (catchup and epoch-entry base-fee seeding) rely
    /// on this pin: reading the range start from a different header (e.g. the canonical tip)
    /// could yield a silently empty range if finality ever lags the canonical tip.
    pub fn epoch_state_at_header(&self, header: &SealedHeader) -> eyre::Result<EpochState> {
        // create EVM with the state at the pinned header
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        debug!(target: "engine", state=?db.bundle_state, hashes=?db.block_hashes, "retrieving epoch state at header");
        let mut tn_evm = self
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.inner.evm_config.evm_env(header)?);

        // current epoch number
        let epoch = self.get_current_epoch_number(&mut tn_evm)?;

        // current epoch info
        let epoch_info = self.get_current_epoch_info(&mut tn_evm)?;
        debug!(target: "engine", ?epoch, ?epoch_info, "retrieved epoch info at header");

        // retrieve closing timestamp for previous epoch
        let epoch_start = self
            .header_by_number(epoch_info.blockHeight.saturating_sub(1))?
            .ok_or_eyre("failed to retrieve closing epoch information")?
            .timestamp;

        // retrieve the committee
        let validators = self.get_committee_validators_by_epoch(epoch, &mut tn_evm)?;
        let bls_pubkeys = self.get_committee_bls_pubkeys_by_epoch(epoch, &mut tn_evm)?;
        let epoch_state = EpochState { epoch, epoch_info, validators, bls_pubkeys, epoch_start };
        debug!(target: "engine", ?epoch_state, "returning epoch state at header");

        Ok(epoch_state)
    }

    /// Read the latest committee and epoch information from the [ConsensusRegistry] on-chain.
    pub fn validators_for_epoch(
        &self,
        epoch: u32,
    ) -> eyre::Result<Vec<ConsensusRegistry::ValidatorInfo>> {
        debug!(target: "engine", "retrieving validators for epoch {epoch}");
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.read_consensus_registry(calldata).map_err(Into::into)
    }

    /// Read the BLS pubkeys for the committee of the provided epoch from the [ConsensusRegistry]
    /// on-chain.
    pub fn bls_pubkeys_for_epoch(&self, epoch: u32) -> eyre::Result<Vec<alloy::primitives::Bytes>> {
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.read_consensus_registry(calldata).map_err(Into::into)
    }

    /// Build an EVM at the canonical tip, execute a read-only [ConsensusRegistry] call, and
    /// decode the returned data to `T`.
    ///
    /// Convenience wrapper over [`Self::read_consensus_registry_batch`] for the common
    /// single-read case (one pinned EVM, one call).
    pub fn read_consensus_registry<T>(&self, calldata: Bytes) -> EvmReadResult<T>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        self.read_consensus_registry_batch(vec![calldata])?.pop().ok_or_else(|| {
            EvmReadError::Internal("consensus registry batch read returned no result".into())
        })
    }

    /// Build a single EVM at the canonical tip and execute several read-only [ConsensusRegistry]
    /// calls against it, decoding each result to `T`.
    ///
    /// Every calldata in a batch must decode to the same Solidity type `T` (current caller: five
    /// `getValidatorsInfo(status)` reads → `Vec<ValidatorInfo>`).
    ///
    /// All calls observe ONE pinned state snapshot, so a multi-call query (e.g. unioning
    /// per-status validator sets) cannot straddle a block commit and double-count or drop a
    /// validator that changes status between reads. Each call still runs under its own fresh 30M
    /// gas budget (`transact_system_call`), so splitting a large query across calls keeps gas
    /// bounded per call.
    pub fn read_consensus_registry_batch<T>(&self, calldatas: Vec<Bytes>) -> EvmReadResult<Vec<T>>
    where
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        // create EVM with latest state
        let canonical_tip = self.canonical_tip();
        debug!(target: "engine", ?canonical_tip, "reading consensus registry batch at canonical tip");
        let state_provider = self
            .inner
            .blockchain_provider
            .state_by_block_hash(canonical_tip.hash())
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self
            .inner
            .evm_config
            .evm_env(&canonical_tip)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // reuse the one pinned EVM for every read; `call_consensus_registry` is non-committing,
        // so each read sees the same base state.
        calldatas
            .into_iter()
            .map(|calldata| self.call_consensus_registry(&mut tn_evm, calldata))
            .collect()
    }

    /// Execute a read-only contract call at the canonical tip and return the raw ABI-encoded
    /// output bytes.
    ///
    /// `eth_call`-like semantics: caller is the zero address, value 0, gas price 0, nonce and
    /// base fee checks disabled, 30M gas; no state is committed. Callers decode the returned
    /// bytes themselves (e.g. with `SolCall::abi_decode_returns`).
    pub fn read_contract(&self, contract: Address, calldata: Bytes) -> EvmReadResult<Bytes> {
        self.read_contract_inner(&self.canonical_tip(), Address::ZERO, contract, calldata)
    }

    /// Same as [`Self::read_contract`], but pinned to the state of the block identified by
    /// `block_hash`.
    pub fn read_contract_at_block(
        &self,
        block_hash: B256,
        contract: Address,
        calldata: Bytes,
    ) -> EvmReadResult<Bytes> {
        let header = self
            .sealed_header_by_hash(block_hash)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?
            .ok_or_else(|| {
                EvmReadError::Internal(format!(
                    "sealed header not found for block hash {block_hash:?}"
                ))
            })?;
        self.read_contract_inner(&header, Address::ZERO, contract, calldata)
    }

    /// Build an EVM against `header`'s state and execute one read-only call, returning raw
    /// output bytes on success and mapping Revert/Halt like the registry read path.
    fn read_contract_inner(
        &self,
        header: &SealedHeader,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> EvmReadResult<Bytes> {
        let state_provider = self
            .inner
            .blockchain_provider
            .state_by_block_hash(header.hash())
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self
            .inner
            .evm_config
            .evm_env(header)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        let result = self
            .read_state_on_chain(&mut tn_evm, caller, contract, calldata)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;

        // surface user-triggerable reverts distinctly from internal node faults
        match result.result {
            ExecutionResult::Success { output, .. } => Ok(output.into_data()),
            ExecutionResult::Revert { output, .. } => Err(EvmReadError::Revert {
                reason: alloy::sol_types::decode_revert_reason(&output),
                output,
            }),
            ExecutionResult::Halt { reason, gas_used } => Err(EvmReadError::Internal(format!(
                "contract call halted: {reason:?} (gas {gas_used})"
            ))),
        }
    }

    /// Extract the epoch number from a header's nonce.
    pub fn extract_epoch_from_header(header: &ExecHeader) -> Epoch {
        let nonce: u64 = header.nonce.into();
        (nonce >> 32) as u32
    }

    /// Read the curret epoch number from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_number<DB>(&self, evm: &mut TNEvm<DB>) -> EvmReadResult<u32>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into();
        self.call_consensus_registry::<_, u32>(evm, calldata)
    }

    /// Read the curret epoch info from the [ConsensusRegistry] on-chain.
    fn get_current_epoch_info<DB>(
        &self,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<ConsensusRegistry::EpochInfo>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(evm, calldata)
    }

    /// Retrieve all `ValidatorInfo` in the committee for the provided epoch.
    fn get_committee_validators_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<Vec<ConsensusRegistry::ValidatorInfo>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeValidatorsCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(evm, calldata)
    }

    /// Retrieve BLS pubkeys for the committee of the provided epoch.
    fn get_committee_bls_pubkeys_by_epoch<DB>(
        &self,
        epoch: Epoch,
        evm: &mut TNEvm<DB>,
    ) -> EvmReadResult<Vec<alloy::primitives::Bytes>>
    where
        DB: alloy_evm::Database,
    {
        let calldata = ConsensusRegistry::getCommitteeBlsPubkeysCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, Vec<alloy::primitives::Bytes>>(evm, calldata)
    }

    /// Read fee configs for all workers from the [`WorkerConfigs`] contract at the given header.
    ///
    /// Builds an EVM against `header`'s state and issues a single `getAllWorkerConfigs()` call.
    /// Returns the on-chain worker count alongside the decoded [`WorkerFeeConfig`]s.
    ///
    /// Failures are classified per [`StateReadError`]. The classification boundary: the state
    /// provider construction and any database fault the EVM hits while lazily reading state are
    /// [`StateReadError::Provider`] (node-local — peers reading the same block may succeed);
    /// everything downstream of a successfully-executing EVM — contract absent (empty return
    /// data), revert, halt, ABI decode, arity mismatch — plus EVM environment construction is
    /// [`StateReadError::ChainGlobal`] (a deterministic product of the pinned block, identical on
    /// every node).
    fn worker_fee_configs_inner(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(header.hash()).map_err(|e| {
                StateReadError::Provider(format!("state provider at {}: {e}", header.hash()))
            })?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self.inner.evm_config.evm_env(header).map_err(|e| {
            StateReadError::ChainGlobal(format!("evm env for {}: {e}", header.hash()))
        })?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        let calldata = WorkerConfigs::getAllWorkerConfigsCall {}.abi_encode().into();
        let result = Self::classified_system_call(
            &mut tn_evm,
            SYSTEM_ADDRESS,
            WORKER_CONFIGS_ADDRESS,
            calldata,
        )?;
        let data = match result.result {
            ExecutionResult::Success { output, .. } => output.into_data(),
            e => {
                return Err(StateReadError::ChainGlobal(format!(
                    "failed to read worker configs: {e:?}"
                )))
            }
        };
        let ret =
            <WorkerConfigs::getAllWorkerConfigsCall as alloy::sol_types::SolCall>::abi_decode_returns(
                &data,
            )
            .map_err(|e| {
                StateReadError::ChainGlobal(format!(
                    "worker configs return decode failed (contract absent at this block?): {e}"
                ))
            })?;

        let num_workers = ret.count as usize;
        if ret.strategies.len() != num_workers
            || ret.values.len() != num_workers
            || ret.datas.len() != num_workers
        {
            return Err(StateReadError::ChainGlobal(format!(
                "worker config arity mismatch: count={num_workers}, strategies={}, values={}, datas={}",
                ret.strategies.len(),
                ret.values.len(),
                ret.datas.len(),
            )));
        }

        let mut configs = Vec::with_capacity(num_workers);
        for (worker_id, (&strategy, &value)) in
            ret.strategies.iter().zip(ret.values.iter()).enumerate()
        {
            let config = match strategy {
                0 => WorkerFeeConfig::Eip1559 { target_gas: value },
                1 => WorkerFeeConfig::Static { fee: value },
                s => {
                    // The contract rejects unknown strategies, so this branch only fires when a
                    // future contract version introduces a strategy this node hasn't been
                    // updated to understand. Fall back to EIP-1559 to preserve liveness instead
                    // of halting all validators.
                    tracing::warn!(
                        target: "tn::reth",
                        worker_id,
                        strategy = s,
                        "unknown fee strategy; falling back to strategy 0 (Eip1559)"
                    );
                    WorkerFeeConfig::Eip1559 { target_gas: value }
                }
            };
            configs.push(config);
        }

        Ok((num_workers, configs))
    }

    /// Read worker fee configs from the [`WorkerConfigs`] contract at the block identified by
    /// `block_hash`.
    ///
    /// Returns the on-chain worker count and one [`WorkerFeeConfig`] per worker. Failures are
    /// classified per [`StateReadError`] (see [`Self::worker_fee_configs_inner`]); `block_hash`
    /// failing to resolve to a sealed header is [`StateReadError::Provider`] — the pinned block
    /// exists on the committee by construction, so a miss reflects this node's local view, not a
    /// chain-global fact.
    pub fn get_worker_fee_configs_at_block(
        &self,
        block_hash: B256,
    ) -> StateReadResult<(usize, Vec<WorkerFeeConfig>)> {
        let header = self
            .sealed_header_by_hash(block_hash)
            .map_err(|e| {
                StateReadError::Provider(format!("header lookup for {block_hash:?}: {e}"))
            })?
            .ok_or_else(|| {
                StateReadError::Provider(format!(
                    "sealed header not found for block hash {block_hash:?}"
                ))
            })?;
        self.worker_fee_configs_inner(&header)
    }

    /// Read the [`ConsensusRegistry`] [`EpochInfo`](ConsensusRegistry::EpochInfo) for `epoch` at
    /// the block identified by `block_hash`.
    ///
    /// Builds an EVM pinned to `block_hash`'s state and issues a single `getEpochInfo(uint32)`
    /// call. The registry keeps a ring buffer of the four most recent epochs and reverts
    /// (`InvalidEpoch`) for anything outside it, so a successful return is guaranteed to be the
    /// requested epoch's record. Used by the epoch manager to recover the previous epoch's block
    /// range from its closing block when deriving next-epoch base fees.
    ///
    /// Fails with a descriptive error if `block_hash` does not resolve to a sealed header or the
    /// registry call does not succeed.
    pub fn get_epoch_info_at_block(
        &self,
        epoch: Epoch,
        block_hash: B256,
    ) -> eyre::Result<ConsensusRegistry::EpochInfo> {
        let header = self
            .sealed_header_by_hash(block_hash)?
            .ok_or_else(|| eyre::eyre!("sealed header not found for block hash {block_hash:?}"))?;
        let state_provider = self.inner.blockchain_provider.state_by_block_hash(header.hash())?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let mut tn_evm = self
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, self.inner.evm_config.evm_env(&header)?);

        let calldata = ConsensusRegistry::getEpochInfoCall { epoch }.abi_encode().into();
        self.call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)
            .map_err(Into::into)
    }

    /// Read worker fee configs from the [`WorkerConfigs`] contract at the canonical tip.
    ///
    /// The returned `Vec`'s length is the on-chain `numWorkers()` at the canonical tip (the
    /// arity between the count and the per-worker arrays is validated in
    /// [`Self::worker_fee_configs_inner`]). Callers size their in-memory worker state (e.g. the
    /// `GasAccumulator`) to match, rather than asserting a preconceived count.
    pub fn get_worker_fee_configs(&self) -> StateReadResult<Vec<WorkerFeeConfig>> {
        let canonical_tip = self.canonical_tip();
        let (_num_workers, configs) = self.worker_fee_configs_inner(&canonical_tip)?;
        Ok(configs)
    }

    /// Read the CURRENT epoch number and [`EpochInfo`](ConsensusRegistry::EpochInfo) from the
    /// [`ConsensusRegistry`] at `header`, with failures classified per [`StateReadError`].
    ///
    /// This is the close-time identity read for the epoch manager's `adjust_base_fees`: at an
    /// epoch's closing block the registry state has already crossed to the entered epoch
    /// (`concludeEpoch` ran inside that block), so the returned info is the entered epoch's record
    /// and its `blockHeight` must equal `header.number + 1`. It reads exactly what
    /// [`Self::epoch_state_at_header`] reads for the same check but skips the committee/BLS/
    /// epoch-start lookups (a gating check needs only the epoch identity) and — unlike that
    /// method, whose failures collapse into `eyre` strings — keeps node-local provider faults
    /// (NOT committee-deterministic: retry or halt) distinguishable from chain-global failures
    /// (committee-deterministic: fail-open stays consistent).
    pub fn get_current_epoch_info_at_header(
        &self,
        header: &SealedHeader,
    ) -> StateReadResult<(Epoch, ConsensusRegistry::EpochInfo)> {
        let state_provider =
            self.inner.blockchain_provider.state_by_block_hash(header.hash()).map_err(|e| {
                StateReadError::Provider(format!("state provider at {}: {e}", header.hash()))
            })?;
        let state = StateProviderDatabase::new(&state_provider);
        let mut db = State::builder().with_database(state).with_bundle_update().build();
        let evm_env = self.inner.evm_config.evm_env(header).map_err(|e| {
            StateReadError::ChainGlobal(format!("evm env for {}: {e}", header.hash()))
        })?;
        let mut tn_evm = self.inner.evm_config.evm_factory().create_evm(&mut db, evm_env);

        // both reads observe the ONE pinned EVM state
        let epoch: Epoch = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochCall {}.abi_encode().into(),
        )?;
        let epoch_info: ConsensusRegistry::EpochInfo = Self::classified_registry_read(
            &mut tn_evm,
            ConsensusRegistry::getCurrentEpochInfoCall {}.abi_encode().into(),
        )?;
        Ok((epoch, epoch_info))
    }

    /// Execute a read-only [`ConsensusRegistry`] call on `evm` and decode the result, classifying
    /// failures per [`StateReadError`].
    ///
    /// The [`StateReadError`]-typed sibling of [`Self::call_consensus_registry`]: revert, halt,
    /// and decode failures are all deterministic products of the pinned chain state
    /// ([`StateReadError::ChainGlobal`]); only a database fault inside the EVM (via
    /// [`Self::classified_system_call`]) is node-local ([`StateReadError::Provider`]).
    fn classified_registry_read<DB, T>(evm: &mut TNEvm<DB>, calldata: Bytes) -> StateReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = Self::classified_system_call(
            evm,
            SYSTEM_ADDRESS,
            CONSENSUS_REGISTRY_ADDRESS,
            calldata,
        )?;
        match state.result {
            ExecutionResult::Success { output, .. } => {
                alloy::sol_types::SolValue::abi_decode(&output.into_data()).map_err(|e| {
                    StateReadError::ChainGlobal(format!(
                        "registry return decode failed (contract absent at this block?): {e}"
                    ))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(StateReadError::ChainGlobal(format!(
                "registry call reverted: {:?}",
                alloy::sol_types::decode_revert_reason(&output)
            ))),
            ExecutionResult::Halt { reason, gas_used } => Err(StateReadError::ChainGlobal(
                format!("registry call halted: {reason:?} (gas {gas_used})"),
            )),
        }
    }

    /// Execute a read-only system call on `evm`, classifying failures per [`StateReadError`].
    ///
    /// An [`EVMError::Database`] is a node-local provider fault surfaced by the EVM's lazy state
    /// reads (the state provider is only CONSTRUCTED up front; account/storage/bytecode loads
    /// happen during execution, so an MDBX/provider I/O fault lands here) — classified
    /// [`StateReadError::Provider`]. Every other transact failure derives deterministically from
    /// the pinned block and calldata, so it is [`StateReadError::ChainGlobal`].
    fn classified_system_call<DB>(
        evm: &mut TNEvm<DB>,
        caller: Address,
        contract: Address,
        calldata: Bytes,
    ) -> StateReadResult<ResultAndState>
    where
        DB: alloy_evm::Database,
    {
        evm.transact_system_call(caller, contract, calldata).map_err(|e| match e {
            EVMError::Database(db_err) => {
                StateReadError::Provider(format!("system call state read failed: {db_err}"))
            }
            other => {
                StateReadError::ChainGlobal(format!("system call failed reading state: {other}"))
            }
        })
    }

    /// Helper function to call `ConsensusRegistry` state on-chain.
    fn call_consensus_registry<DB, T>(
        &self,
        evm: &mut TNEvm<DB>,
        calldata: Bytes,
    ) -> EvmReadResult<T>
    where
        DB: alloy_evm::Database,
        T: alloy::sol_types::SolValue,
        T: From<
            <<T as alloy::sol_types::SolValue>::SolType as alloy::sol_types::SolType>::RustType,
        >,
    {
        let state = self
            .read_state_on_chain(evm, SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)
            .map_err(|e| EvmReadError::Internal(e.to_string()))?;

        // retrieve data from state, distinguishing user-triggerable reverts from node faults
        match state.result {
            ExecutionResult::Success { output, .. } => {
                let data = output.into_data();
                // use SolValue to decode the result
                alloy::sol_types::SolValue::abi_decode(&data).map_err(|e| {
                    EvmReadError::Internal(format!("registry return decode failed: {e}"))
                })
            }
            ExecutionResult::Revert { output, .. } => Err(EvmReadError::Revert {
                reason: alloy::sol_types::decode_revert_reason(&output),
                output,
            }),
            ExecutionResult::Halt { reason, gas_used } => Err(EvmReadError::Internal(format!(
                "registry call halted: {reason:?} (gas {gas_used})"
            ))),
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
    use std::collections::VecDeque;

    use super::*;
    use crate::{
        system_calls::ConsensusRegistry::ValidatorStatus,
        test_utils::{
            execute_payload_and_update_canonical_chain, governance_burn_tx,
            governance_owner_factory, test_genesis_with_consensus_registry, TransactionFactory,
        },
    };
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_types::{
        generate_proof_of_possession_bls_for_test, keccak256, test_genesis, BlsKeypair,
        BlsSignature, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput,
        Encodable2718 as _, NodeP2pInfo, ReputationScores, SignatureVerificationState,
    };

    /// Helper function for creating a consensus output for tests.
    fn consensus_output_for_tests(
        round: u32,
        epoch: u32,
        subdag_index: u64,
        close_epoch: bool,
    ) -> ConsensusOutput {
        let mut leader = Certificate::default();
        // set signature for deterministic test results
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.update_header_created_at_for_test(tn_types::now());
        leader.update_header_round_for_test(round);
        leader.update_header_epoch_for_test(epoch);
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let sub_dag = CommittedSubDag::new(
            vec![Certificate::default(), leader.clone()],
            leader,
            subdag_index,
            reputation_scores,
            previous_sub_dag,
        );
        ConsensusOutput::new(
            sub_dag,
            ConsensusHeader::default().digest(),
            subdag_index,
            close_epoch,
            VecDeque::new(),
            Vec::new(),
        )
    }

    /// Exercise the tx/receipt/feed read API against a persisted three-block chain:
    /// block 1 holds two transfers, block 2 is empty, block 3 holds one transfer.
    #[tokio::test]
    async fn test_read_api_tx_receipts_and_feed() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let mut factory = TransactionFactory::new();
        let recipient = Address::from_slice(&[0xcc; 20]);
        let value = U256::from(1_000_000);
        let tx1 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx2 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx3 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());

        // block 1: two transfers
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx1.encoded_2718(), tx2.encoded_2718()],
        )?;
        let block1_header = block1.recovered_block.clone_sealed_header();

        // block 2: empty
        let consensus_output = consensus_output_for_tests(2, 0, 2, false);
        let payload = TNPayload::new_for_test(block1_header.clone(), &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let block2_header = block2.recovered_block.clone_sealed_header();

        // block 3: one transfer
        let consensus_output = consensus_output_for_tests(2, 0, 3, false);
        let payload = TNPayload::new_for_test(block2_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx3.encoded_2718()],
        )?;
        let block3_header = block3.recovered_block.clone_sealed_header();

        // tx-by-hash roundtrip for the second transaction in block 1
        let tx2_hash = *tx2.hash();
        let (recovered, meta) = reth_env
            .transaction_by_hash_with_meta(tx2_hash)?
            .expect("mined transaction found by hash");
        assert_eq!(recovered.signer(), factory.address());
        assert_eq!(*recovered.hash(), tx2_hash);
        assert_eq!(meta.block_number, 1);
        assert_eq!(meta.index, 1);
        assert_eq!(meta.block_hash, block1_header.hash());
        assert_eq!(meta.timestamp, block1_header.timestamp);
        // unknown hash is Ok(None), not an error
        assert!(reth_env.transaction_by_hash_with_meta(TxHash::random())?.is_none());

        // receipts by hash: cumulative gas is block-wide, per-tx gas is the delta
        let receipt = reth_env.receipt_by_hash(tx2_hash)?.expect("receipt for mined tx");
        assert!(receipt.success);
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(tx2_hash)?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 42_000);
        // first-in-block transaction: per-tx gas equals its cumulative gas
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(*tx3.hash())?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 21_000);

        // receipts by block: number- and hash-based reads agree
        let by_number =
            reth_env.receipts_by_block(BlockHashOrNumber::Number(1))?.expect("block 1 receipts");
        let by_hash = reth_env
            .receipts_by_block(BlockHashOrNumber::Hash(block1_header.hash()))?
            .expect("block 1 receipts by hash");
        assert_eq!(by_number.len(), 2);
        assert_eq!(by_number, by_hash);
        // known-but-empty block returns Some(empty); unknown block returns None
        assert_eq!(reth_env.receipts_by_block(BlockHashOrNumber::Number(2))?, Some(vec![]));
        assert!(reth_env.receipts_by_block(BlockHashOrNumber::Number(99))?.is_none());

        // chain-wide feed: three transactions total
        assert_eq!(reth_env.total_transactions()?, 3);
        let feed = reth_env.transactions_by_tx_range_with_meta(0..=2)?;
        assert_eq!(feed.len(), 3);
        let tx_numbers: Vec<_> = feed.iter().map(|entry| entry.tx_number).collect();
        assert_eq!(tx_numbers, vec![0, 1, 2]);
        for (entry, tx) in feed.iter().zip([&tx1, &tx2, &tx3]) {
            assert_eq!(*entry.transaction.hash(), *tx.hash());
            assert_eq!(entry.transaction.signer(), factory.address());
        }
        // entries 0-1 come from block 1
        assert_eq!(feed[0].block_number, 1);
        assert_eq!(feed[0].index, 0);
        assert_eq!(feed[0].block_hash, block1_header.hash());
        assert_eq!(feed[1].block_number, 1);
        assert_eq!(feed[1].index, 1);
        // entry 2 comes from block 3 — the empty block 2 is skipped entirely
        assert_eq!(feed[2].block_number, 3);
        assert_eq!(feed[2].index, 0);
        assert_eq!(feed[2].block_hash, block3_header.hash());
        assert_eq!(feed[2].timestamp, block3_header.timestamp);

        // ranges past the newest transaction clamp to what exists
        assert_eq!(reth_env.transactions_by_tx_range_with_meta(0..=99)?.len(), 3);
        // empty range yields an empty vec
        assert!(reth_env.transactions_by_tx_range_with_meta(RangeInclusive::new(1, 0))?.is_empty());

        // newest-first page of two: read a descending window and reverse it
        let total = reth_env.total_transactions()?;
        let mut page = reth_env.transactions_by_tx_range_with_meta(total - 2..=total - 1)?;
        page.reverse();
        let hashes: Vec<_> = page.iter().map(|entry| *entry.transaction.hash()).collect();
        assert_eq!(hashes, vec![*tx3.hash(), *tx2.hash()]);

        Ok(())
    }

    /// Minimal runtime bytecode: `PUSH1 42, PUSH1 0, MSTORE, PUSH1 32, PUSH1 0, RETURN` —
    /// returns `uint256(42)` for any calldata.
    const RETURN_42: &[u8] = &[0x60, 0x2a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];
    /// Minimal runtime bytecode: `PUSH1 0, PUSH1 0, REVERT` — reverts with empty output.
    const ALWAYS_REVERT: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xfd];

    /// Exercise account/code reads and the generic read-only contract call against two
    /// bytecode fixtures deployed in genesis.
    #[tokio::test]
    async fn test_read_api_account_code_and_contract_read() -> eyre::Result<()> {
        let return_42_addr = Address::from_slice(&[0xaa; 20]);
        let always_revert_addr = Address::from_slice(&[0xbb; 20]);
        let genesis = test_genesis().extend_accounts([
            (
                return_42_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(RETURN_42))),
            ),
            (
                always_revert_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(ALWAYS_REVERT))),
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
        let genesis_hash = chain.sealed_genesis_header().hash();

        let mut factory = TransactionFactory::new();
        let genesis_balance = reth_env
            .retrieve_account(&factory.address())?
            .expect("factory funded in genesis")
            .balance;

        // execute one transfer so the factory's nonce and balance move
        let recipient = Address::from_slice(&[0xcc; 20]);
        let transfer = factory.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(recipient),
            U256::from(1_000_000),
            Bytes::new(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![transfer])?;

        // account state reflects the executed transfer
        let factory_account =
            reth_env.retrieve_account(&factory.address())?.expect("factory account exists");
        assert_eq!(factory_account.nonce, 1);
        assert!(factory_account.balance < genesis_balance);

        // genesis contract account carries the bytecode hash; unknown account is None
        let contract_account =
            reth_env.retrieve_account(&return_42_addr)?.expect("genesis contract account exists");
        assert_eq!(contract_account.bytecode_hash, Some(keccak256(RETURN_42)));
        assert!(reth_env.retrieve_account(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // eth_getCode semantics: byte-exact for contracts, None for EOAs and unknowns
        assert_eq!(reth_env.account_code(&return_42_addr)?, Some(Bytes::from_static(RETURN_42)));
        assert!(reth_env.account_code(&factory.address())?.is_none());
        assert!(reth_env.account_code(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // read-only contract call at the canonical tip
        let output = reth_env
            .read_contract(return_42_addr, Bytes::default())
            .expect("read-only call succeeds");
        assert_eq!(output.len(), 32);
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        // on-chain revert surfaces as EvmReadError::Revert with the raw output bytes
        let err = reth_env
            .read_contract(always_revert_addr, Bytes::default())
            .expect_err("reverting call must error");
        match err {
            EvmReadError::Revert { output, reason } => {
                assert!(output.is_empty());
                // alloy's `decode_revert_reason` treats any valid-UTF-8 output as a raw-string
                // reason (Vyper convention), so empty revert data yields `Some("")` rather than
                // `None`; assert there is no *meaningful* reason either way
                assert!(reason.as_deref().unwrap_or_default().is_empty());
            }
            other => panic!("expected revert error, got: {other:?}"),
        }

        // historical pinning: the same read against the genesis block's state
        let output = reth_env
            .read_contract_at_block(genesis_hash, return_42_addr, Bytes::default())
            .expect("read-only call at genesis succeeds");
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        Ok(())
    }

    /// In-protocol `ConsensusRegistry` fork over the PRE-fork testnet registry.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml`, whose `ConsensusRegistry`
    /// account carries the pre-fork runtime code and validator storage with NO per-status sets —
    /// the exact on-chain shape the fork upgrades in place. The epoch-closing block that
    /// concludes `FORK_EPOCH - 1` swaps in the new runtime and runs `migrateValidatorSets()`
    /// FIRST, then the rewards + conclude calls run on the new code over the byte-identical
    /// preserved storage.
    ///
    /// Asserts: the pre-fork code does not answer the new-ABI eligible-count call; post-fork the
    /// new code is live and the migration populated a non-empty eligible set; a preserved BLS
    /// pubkey survives the swap as 96-byte compressed; and the fork block's `state_root` is
    /// identical across two independent executions (determinism — every node re-derives the
    /// same root).
    ///
    /// NOTE: the fixture is the LIVE pre-fork deployment, pinned by
    /// `tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` and its tn-types pin test. If
    /// `chain-configs/testnet/genesis.yaml` is ever regenerated from the current (post-fork)
    /// artifact, the `pre.is_err()` probe below fails first — that means the fixture no longer
    /// mirrors the chain this fork targets; reassess the fork plan rather than updating the
    /// probes.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_swaps_code_and_migrates() -> eyre::Result<()> {
        // pre-fork fixture: old registry code + validator storage, no per-status sets
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // fork fires when the concluding epoch + 1 == FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;

        // one payload, cloned across both executions, so the determinism check compares
        // byte-identical inputs (`new_for_test` otherwise randomizes
        // beneficiary/mix_hash/digest per call)
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        // --- env 1: pre-fork state must NOT answer the new-ABI eligible-count call (old code) ---
        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("fork test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(genesis_header.header())?);
            let pre = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                pre.is_err(),
                "pre-fork registry (old code) must not expose getEligibleValidatorCount"
            );
        }

        // --- produce the fork boundary block on the production path ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post-fork: new code is live and the sets are migrated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(header.header())?);

            let eligible = env1
                .call_consensus_registry::<_, U256>(
                    &mut evm,
                    ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
                )
                .expect("post-fork getEligibleValidatorCount must succeed on the swapped-in code");
            assert!(eligible > U256::ZERO, "migration must populate a non-zero eligible count");

            // getValidators(Active) now returns address[] (new ABI) from the migrated set
            let active = env1
                .call_consensus_registry::<_, Vec<Address>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("getValidators(Active) must succeed on new code");
            assert!(!active.is_empty(), "migrated Active set must be non-empty");

            // stored BLS pubkey survives the code swap untouched: still 96-byte compressed
            let bls = env1
                .call_consensus_registry::<_, Bytes>(
                    &mut evm,
                    ConsensusRegistry::getBlsPubkeyCall { validatorAddress: active[0] }
                        .abi_encode()
                        .into(),
                )
                .expect("getBlsPubkey must succeed");
            assert_eq!(bls.len(), 96, "preserved BLS pubkey must remain 96-byte compressed");
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("fork test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "fork block state_root must be identical across independent executions"
        );

        Ok(())
    }

    /// Pre-fork epoch conclusion over the LIVE adiri registry code must speak the legacy ABI.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml` — the registry account
    /// carries the pre-fork runtime code (pinned by `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`)
    /// whose `getValidators(uint8)` returns `ValidatorInfo[]` and which has no
    /// `getValidatorsInfo`. Concluding a NORMAL (non-fork-boundary) epoch on this state
    /// exercises the code-hash gate in `read_committee_eligible_pool`: without the gate the
    /// close reverts fatally (the post-fork ABI is absent on-chain) — the exact path a fresh
    /// node onboarding to adiri or a full resync executes for every pre-fork epoch boundary.
    ///
    /// Asserts: the closing block executes; the registry code hash is untouched (no swap —
    /// epoch 3 is far from the fork boundary); the post-fork-only eligible-count call still
    /// fails; and the block's `state_root` is identical across two independent executions.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_pre_fork_epoch_close_uses_legacy_registry_read() -> eyre::Result<()> {
        // pre-fork fixture: the committed adiri genesis (old registry code + validator storage)
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // a normal epoch close, far from the fork boundary (`u32::MAX - 1`), so no swap can fire
        let concluding_epoch = 3;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("legacy read test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();

        // --- pre-probes: readable failures if the committed genesis fixture is regenerated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(genesis_header.header())?);

            // legacy ABI: getValidators(Active) folds the committee-eligible pool into one
            // ValidatorInfo[] response
            let pool = env1
                .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("pre-fork registry must answer the legacy getValidators(Active) read");
            assert!(!pool.is_empty(), "legacy eligible pool must be non-empty");

            let committee_size = env1
                .call_consensus_registry::<_, u16>(
                    &mut evm,
                    ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
                )
                .expect("pre-fork registry must answer getNextCommitteeSize");
            assert!(
                committee_size as usize <= pool.len(),
                "genesis fixture must hold enough eligible validators ({}) for the next \
                 committee ({committee_size}) — was chain-configs/testnet/genesis.yaml \
                 regenerated?",
                pool.len(),
            );
        }

        // --- the epoch-closing block executes via the legacy read (without the gate: fatal) ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post: still the pre-fork code — no swap fired, post-fork ABI still absent ---
        {
            use reth_provider::StateProvider as _;
            let code = env1
                .latest()?
                .account_code(&CONSENSUS_REGISTRY_ADDRESS)?
                .expect("registry account must have code");
            assert_eq!(
                code.0.hash_slow(),
                tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
                "a normal pre-fork epoch close must not swap the registry code"
            );

            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(header.header())?);
            let eligible = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                eligible.is_err(),
                "post-fork-only getEligibleValidatorCount must still fail on the pre-fork code"
            );
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("legacy read test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "pre-fork epoch-close state_root must be identical across independent executions"
        );

        Ok(())
    }

    /// The `ConsensusRegistry` fork must fail closed over an unexpected pre-fork deployment.
    ///
    /// The swap + `migrateValidatorSets()` assume the exact storage layout of the pinned
    /// pre-fork registry code. Here the genesis fixture's registry account is overwritten with
    /// the post-fork artifact bytes (any hash other than
    /// `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` — a stand-in for an unknown deployment), and the
    /// fork-boundary block must abort with the fail-closed gate error instead of silently
    /// migrating over an unverified layout. (Without the gate this block would execute: the
    /// migration is idempotent on the new code, making this test the discriminating check.)
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_fails_closed_on_unexpected_code() -> eyre::Result<()> {
        // overwrite the registry's code (keeping balance + storage) with the post-fork artifact
        let mut genesis = tn_types::test_genesis();
        let v2_value = RethEnv::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let v2_code: Bytes =
            hex::decode(v2_value.as_str().expect("deployedBytecode.object is a string"))?.into();
        genesis
            .alloc
            .get_mut(&CONSENSUS_REGISTRY_ADDRESS)
            .expect("testnet genesis must allocate the ConsensusRegistry account")
            .code = Some(v2_code);

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let genesis_header = chain.sealed_genesis_header();

        // drive the fork boundary: the concluding epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp = TempDir::new().unwrap();
        let tm = TaskManager::new("fail closed test");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None).unwrap();
        let err = execute_payload_and_update_canonical_chain(&env, payload, vec![])
            .expect_err("fork over an unexpected registry deployment must abort the block");
        assert!(
            format!("{err:#}").contains("failing closed"),
            "abort must come from the fail-closed code-hash gate, got: {err:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_close_epochs() -> eyre::Result<()> {
        let validator_1 = Address::from_slice(&[0x11; 20]);
        let validator_3 = Address::from_slice(&[0x33; 20]);
        let validator_4 = Address::from_slice(&[0x44; 20]);
        let validator_5 = Address::from_slice(&[0x55; 20]);

        // create validator wallet for staking later
        let mut new_validator_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

        // create validator wallet for exiting later
        let mut validator_2_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(2));
        let validator_2_address = validator_2_eoa.address();

        // create initial validators for testing
        let all_validators = [
            validator_1,
            validator_2_address,
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
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
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
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), // 50mil TEL
            ),
            (
                new_validator_eoa.address(),
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
            (
                validator_2_address,
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
        ]);

        // remove last validator so only 5 form the initial committees
        let new_validator = validators.pop().expect("six validators");

        // update genesis with consensus registry storage
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
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
        let proof = ConsensusRegistry::ProofOfPossession {
            signature: new_validator.proof_of_possession.to_bytes().into(),
        };
        let calldata = ConsensusRegistry::stakeCall {
            blsPubkey: new_validator.bls_public_key.to_bytes().into(),
            proofOfPossession: proof,
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
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let mut expected_epoch = 0;
        let expected_committee = validators.iter().map(|v| v.execution_address).collect();
        let mut expected_epoch_info = ConsensusRegistry::EpochInfo {
            committee: expected_committee,
            blockHeight: 0,
            epochId: 0,
            epochDuration: epoch_duration,
            epochIssuance: initial_stake_config.epochIssuance,
            stakeVersion: 0,
        };

        // assert epoch state is correct
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "original epoch state from canonical tip in genesis");
        assert_eq!(epoch, expected_epoch);
        assert_eq!(epoch_start, chain.genesis_timestamp());
        assert_eq!(epoch_info, expected_epoch_info);

        // assert committee matches validator args for constructor
        for v in &validators {
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, epoch);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // close epoch with deterministic signature as source of randomness
        // and execute the first block with txs for new validator to stake
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_nft, stake_tx, activate_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // now close the first epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // now close the second epoch so the new validator is active
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 3, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block3.recovered_block.clone_sealed_header();

        // read new epoch state
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target: "evm", ?epoch, ?epoch_info, ?committee, ?epoch, "new epoch state from canonical tip");
        // assert epoch info updated
        expected_epoch_info.blockHeight = 4;
        expected_epoch_info.epochId = expected_epoch as u32;
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
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, reth_env.inner.evm_config.evm_env(canonical_header.header())?);

        // read new committee (always 2 epochs ahead)
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }.abi_encode().into();
        let new_epoch_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)?;

        // ensure validators in increasing order by address
        let expected_new_committee = vec![
            validator_1,
            validator_3,
            validator_4,
            validator_2_address,
            new_validator.execution_address,
        ];

        let expected = ConsensusRegistry::EpochInfo {
            committee: expected_new_committee,
            blockHeight: 0,
            epochId: (expected_epoch + 1) as u32,
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
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, 0);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // submit validator 2 exit request
        let calldata = ConsensusRegistry::beginExitCall {}.abi_encode().into();
        let begin_exit_tx = validator_2_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 4, false);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block4 =
            execute_payload_and_update_canonical_chain(&reth_env, payload, vec![begin_exit_tx])?;
        let canonical_header = block4.recovered_block.clone_sealed_header();

        // close epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 5, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block5 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block5.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, reth_env.inner.evm_config.evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::PendingExit);

        // With the per-status sets, `getValidatorsInfo(Active)` returns strictly-active validators;
        // the committee-eligible pool is the union of Active/PendingActivation/PendingExit, so the
        // pending-exit validator is queried separately rather than partitioned out of `Active`.
        let active_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(active_validators.len(), 5);

        // validator 2 should be the single pending-exit validator
        let pending_exit = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall {
                    status: ValidatorStatus::PendingExit.into(),
                }
                .abi_encode()
                .into(),
            )?;
        assert_eq!(pending_exit.len(), 1);
        assert_eq!(
            pending_exit.first().expect("one pending validator").validatorAddress,
            validator_2_address
        );

        // close epoch again to exit validator
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 6, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block6 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block6.recovered_block.clone_sealed_header();
        // close epoch again
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 7, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block7 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block7.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .inner
            .evm_config
            .evm_factory()
            .create_evm(&mut db, reth_env.inner.evm_config.evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::Exited);

        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let eligible_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                calldata,
            )?;

        assert_eq!(eligible_validators.len(), 5);

        // ensure validator 2 has fully exited
        let (pending_exit, active_validators): (Vec<_>, Vec<_>) = eligible_validators
            .into_iter()
            .partition(|v| v.currentStatus == ValidatorStatus::PendingExit.into());

        assert_eq!(pending_exit.len(), 0);
        assert_eq!(active_validators.len(), 5);
        for v in active_validators {
            assert!(v.validatorAddress != validator_2_address);
        }

        Ok(())
    }

    /// Guards the committee backfill path in `block.rs::shuffle_new_committee`: when there are
    /// fewer strictly-active validators than the committee size, the shuffle must backfill from
    /// the `PendingExit` pool so the next committee still reaches the required size. Five
    /// genesis validators form a committee of 5; two begin exiting, leaving 3 active + 2
    /// pending-exit. Since `committeeSize (5) > active (3)`, every subsequent committee must
    /// include the two exiting validators - otherwise `concludeEpoch` would revert on its
    /// committee-size check.
    #[tokio::test]
    async fn test_committee_backfill_from_pending_exit() -> eyre::Result<()> {
        // the two validators that begin exiting need EOAs to sign their `beginExit` txns
        let mut exit_a_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(101));
        let exit_a = exit_a_eoa.address();
        let mut exit_b_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(102));
        let exit_b = exit_b_eoa.address();

        let all_validators = [
            Address::from_slice(&[0x11; 20]),
            Address::from_slice(&[0x33; 20]),
            Address::from_slice(&[0x44; 20]),
            exit_a,
            exit_b,
        ];
        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls.public(),
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let epoch_duration = 60 * 60 * 24;
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
            ),
            (exit_a, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
            (exit_b, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
        ]);

        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Backfill Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: genesis committee is the full set of 5 validators
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // two validators begin exiting (Active -> PendingExit)
        let begin_exit_a = exit_a_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );
        let begin_exit_b = exit_b_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );

        // execute the exits in the first block (no epoch close yet)
        let mut expected_epoch = 0u32;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![begin_exit_a, begin_exit_b],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // close several epochs so the post-exit committees (computed 2 epochs ahead by the shuffle)
        // become current. If the backfill is broken, `concludeEpoch` reverts on the size check.
        for round in 2..=6u64 {
            expected_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, expected_epoch, round, true);
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            // the committee must stay full at every close: active(3) < committeeSize(5) forces the
            // shuffle to backfill from the pending-exit pool each epoch
            let EpochState { validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(committee.len(), 5, "committee stays full via pending-exit backfill");
        }

        // with active(3) < committeeSize(5), the backfill must keep every committee full and
        // include the two pending-exit validators
        let EpochState { validators: committee, .. } = reth_env.epoch_state_from_canonical_tip()?;
        let committee_addrs: Vec<Address> = committee.iter().map(|v| v.validatorAddress).collect();
        assert_eq!(committee_addrs.len(), 5, "committee stays full via pending-exit backfill");
        assert!(committee_addrs.contains(&exit_a), "pending-exit validator A backfilled");
        assert!(committee_addrs.contains(&exit_b), "pending-exit validator B backfilled");

        // the backfilled validators remain PendingExit (still serving, not yet exited)
        for exiting in [exit_a, exit_b] {
            let info = reth_env.get_validator_info(canonical_header.hash(), exiting)?;
            assert_eq!(
                info.currentStatus,
                ConsensusRegistry::ValidatorStatus::PendingExit,
                "backfilled validator stays PendingExit"
            );
        }

        Ok(())
    }

    /// Governance `burn` forcibly ejects a current-committee validator mid-epoch.
    ///
    /// Pins the on-chain behavior the node's epoch-record layer must tolerate: the stored
    /// committee arrays for the current and both future epochs shrink via swap-and-pop (the
    /// last element moves into the ejected slot — order is NOT preserved), the next committee
    /// size auto-decrements to the eligible count, the validator is permanently retired with
    /// its stake confiscated, and the epoch still closes cleanly on-chain (`concludeEpoch` +
    /// `applyIncentives` system calls succeed over the shrunken committee). A direct
    /// `applyIncentives` call afterwards exercises the `isRetired` skip branch: the burned
    /// validator earns nothing while a surviving validator accrues rewards.
    #[tokio::test]
    async fn test_burn_ejects_current_committee_validator_mid_epoch() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Burn Eject Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // genesis committee of 5 for epoch 0
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // capture pre-burn committee pubkeys for the current and both future epochs
        let pre_burn = (0u32..=2)
            .map(|e| reth_env.bls_pubkeys_for_epoch(e))
            .collect::<eyre::Result<Vec<_>>>()?;

        // eject a middle slot so the swap-and-pop reorder is visible
        let target = committee[1].validatorAddress;
        let target_bls = pre_burn[0][1].clone();

        // pre-burn: full stake outstanding, no rewards
        let (outstanding, initial_stake, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(outstanding, initial_stake);
        assert!(rewards.is_zero());

        // block 1: governance burns the validator mid-epoch
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // the validator is permanently retired with its stake confiscated
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired, "burned validator is permanently retired");
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // current + both future committees shrink with EXACT swap-and-pop order: the last
        // element moves into the burned slot and the array truncates by one
        for e in 0u32..=2 {
            let post = reth_env.bls_pubkeys_for_epoch(e)?;
            let mut expected = pre_burn[e as usize].clone();
            let idx = expected
                .iter()
                .position(|k| k == &target_bls)
                .expect("burned validator in every pre-burn committee");
            let last = expected.len() - 1;
            expected.swap(idx, last);
            expected.truncate(last);
            assert_eq!(post.len(), 4);
            assert!(!post.contains(&target_bls));
            assert_eq!(post, expected, "swap-and-pop order for epoch {e}");
        }

        // positional zip pin: the address committee and pubkey committee stay index-aligned
        // (the node zips these arrays by position to build its committee)
        for e in 0u32..=2 {
            let infos = reth_env.validators_for_epoch(e)?;
            let keys = reth_env.bls_pubkeys_for_epoch(e)?;
            assert_eq!(infos.len(), keys.len());
            for (info, key) in infos.iter().zip(keys.iter()) {
                let direct =
                    reth_env.get_bls_pubkey(canonical_header.hash(), info.validatorAddress)?;
                assert_eq!(&direct, key, "epoch {e} committee arrays zip positionally");
            }
        }

        // committee size auto-decrements to the eligible count
        let next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 4);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(4));

        // block 2: close the epoch — the concludeEpoch + applyIncentives system calls must
        // succeed over the shrunken committee (on-chain close survives mid-epoch ejection)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // post-close: epoch 1 runs the 4-member committee; the newly shuffled committee
        // (two epochs ahead) is also 4 members and excludes the burned validator
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        assert_eq!(committee.len(), 4);
        assert!(committee.iter().all(|v| v.validatorAddress != target));
        let shuffled = reth_env.bls_pubkeys_for_epoch(3)?;
        assert_eq!(shuffled.len(), 4);
        assert!(!shuffled.contains(&target_bls));

        // direct applyIncentives with rewards for the burned + a surviving validator: the
        // isRetired branch skips the burned validator while the survivor accrues rewards
        let alive = committee[0].validatorAddress;
        let mut tn_evm = reth_env.tn_evm(canonical_header.hash())?;
        let calldata = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: vec![
                ConsensusRegistry::RewardInfo {
                    validatorAddress: target,
                    consensusHeaderCount: U256::from(5),
                },
                ConsensusRegistry::RewardInfo {
                    validatorAddress: alive,
                    consensusHeaderCount: U256::from(5),
                },
            ],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applyIncentives succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);
        let burned_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: target }.abi_encode().into(),
        )?;
        let alive_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: alive }.abi_encode().into(),
        )?;
        assert!(burned_rewards.is_zero(), "retired validator skipped by applyIncentives");
        assert!(alive_rewards > U256::ZERO, "surviving validator accrues rewards");

        Ok(())
    }

    /// Governance `burn` of a validator seated only in FUTURE committees leaves the current
    /// committee untouched.
    ///
    /// A sixth validator stakes and activates, so committees shuffled after its activation may
    /// seat it while the current committee predates it. Burning it mid-epoch mutates only the
    /// future committee arrays it occupies (swap-and-pop), leaves the running committee
    /// byte-identical (so the node's epoch-record comparison cannot diverge for future-only
    /// ejection, even without the mid-epoch-ejection tolerance fix), keeps
    /// `nextCommitteeSize` at 5 (eligible count drops 6 -> 5, so no auto-decrement fires),
    /// and the following epochs close cleanly.
    #[tokio::test]
    async fn test_burn_future_only_validator() -> eyre::Result<()> {
        // the sixth validator's EOA signs its own stake + activate txs
        let mut newval_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
        let newval_addr = newval_eoa.address();
        // BLS seeds 0..=4 are taken by the 5 genesis validators
        let newval_bls = BlsKeypair::generate(&mut StdRng::seed_from_u64(5));
        let newval_pop = generate_proof_of_possession_bls_for_test(&newval_bls, &newval_addr)
            .expect("pop generation failed");

        let stake_amount = U256::from(parse_ether("1_000_000")?);
        let genesis = test_genesis_with_consensus_registry(5).extend_accounts([(
            newval_addr,
            GenesisAccount::default().with_balance(stake_amount.saturating_mul(U256::from(2))),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Future Only Burn Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1 (epoch 0): governance mints the NFT, the new validator stakes and activates
        let mut governance = governance_owner_factory();
        let mint_tx = governance.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::mintCall { validatorAddress: newval_addr }.abi_encode().into(),
        );
        let stake_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            stake_amount,
            ConsensusRegistry::stakeCall {
                blsPubkey: newval_bls.public().to_bytes().into(),
                proofOfPossession: ConsensusRegistry::ProofOfPossession {
                    signature: newval_pop.to_bytes().into(),
                },
            }
            .abi_encode()
            .into(),
        );
        let activate_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::activateCall {}.abi_encode().into(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_tx, stake_tx, activate_tx],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // six validators are now committee-eligible (5 active + 1 pending activation)
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(6));

        // close epochs until some validator sits in a future committee but not the current
        // one (with 6 eligible validators and 5 seats every shuffled committee excludes
        // exactly one; the shuffle is seed-deterministic so the arrangement is stable)
        let committee_addrs = |e: u32| -> eyre::Result<Vec<Address>> {
            Ok(reth_env.validators_for_epoch(e)?.into_iter().map(|v| v.validatorAddress).collect())
        };
        let mut current_epoch = 0u32;
        let mut subdag_index = 2u64;
        let mut arrangement = None;
        while arrangement.is_none() && current_epoch < 6 {
            current_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, current_epoch, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let current = committee_addrs(current_epoch)?;
            let future: Vec<Address> = committee_addrs(current_epoch + 1)?
                .into_iter()
                .chain(committee_addrs(current_epoch + 2)?)
                .collect();
            // prefer the never-seated new validator; any future-only member proves the property
            arrangement = if !current.contains(&newval_addr) && future.contains(&newval_addr) {
                Some(newval_addr)
            } else {
                future.iter().copied().find(|v| !current.contains(v))
            };
        }
        let target = arrangement
            .expect("deterministic shuffle seats a validator in a future committee only");
        let w = current_epoch;
        let target_bls = reth_env.get_bls_pubkey(canonical_header.hash(), target)?;

        // pre-burn snapshots of the running + both future committees
        let pre_current = reth_env.bls_pubkeys_for_epoch(w)?;
        let pre_next = reth_env.bls_pubkeys_for_epoch(w + 1)?;
        let pre_subsequent = reth_env.bls_pubkeys_for_epoch(w + 2)?;
        assert!(!pre_current.contains(&target_bls));
        assert!(
            pre_next.contains(&target_bls) || pre_subsequent.contains(&target_bls),
            "target seated in a future committee"
        );
        let pre_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(pre_next_size, 5);

        // burn the future-only validator mid-epoch W
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, w, subdag_index, false);
        subdag_index += 1;
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        canonical_header = block.recovered_block.clone_sealed_header();

        // the running committee is byte-identical: future-only ejection cannot perturb the
        // current epoch (so on-chain reads for epoch W match any pre-burn snapshot exactly)
        assert_eq!(reth_env.bls_pubkeys_for_epoch(w)?, pre_current);

        // future committees shrink via swap-and-pop exactly where the target was seated
        for (e, pre) in [(w + 1, &pre_next), (w + 2, &pre_subsequent)] {
            let post = reth_env.bls_pubkeys_for_epoch(e)?;
            if let Some(idx) = pre.iter().position(|k| k == &target_bls) {
                let mut expected = pre.to_vec();
                let last = expected.len() - 1;
                expected.swap(idx, last);
                expected.truncate(last);
                assert_eq!(post, expected, "swap-and-pop order for future epoch {e}");
            } else {
                assert_eq!(&post, pre, "future committee {e} untouched");
            }
            assert!(!post.contains(&target_bls));
        }

        // no auto-decrement: 5 remaining eligible validators still cover committee size 5
        let post_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(post_next_size, 5);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(5));
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired);
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // the epoch closes cleanly and the next two committees seat without the target,
        // including the (possibly shrunken) subsequent committee going current
        for close in [w + 1, w + 2] {
            let consensus_output = consensus_output_for_tests(2, close, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let EpochState { epoch, validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(epoch, close);
            assert!(committee.iter().all(|v| v.validatorAddress != target));
        }

        Ok(())
    }

    #[test]
    fn test_parallel_recovery_preserves_order() {
        use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
        use tn_types::Encodable2718;

        // Create 20 transactions from different random signers so each tx is unique.
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let num_txs = 20;
        let mut encoded_txs = Vec::with_capacity(num_txs);
        for i in 0..num_txs {
            let mut factory =
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(i as u64));
            let tx = factory.create_eip1559(
                chain.clone(),
                None,
                100_000,
                Some(Address::ZERO),
                U256::from(1),
                Default::default(),
            );
            encoded_txs.push(tx.encoded_2718());
        }

        // Recover sequentially
        let sequential: Vec<_> = encoded_txs
            .iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("sequential recovery")
            })
            .collect();

        // Recover in parallel (using rayon, same as production code)
        let parallel: Vec<_> = encoded_txs
            .par_iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("parallel recovery")
            })
            .collect();

        // Assert same length
        assert_eq!(sequential.len(), parallel.len());

        // Assert same order by comparing tx hashes and recovered signer addresses
        for (seq, par) in sequential.iter().zip(parallel.iter()) {
            assert_eq!(seq.hash(), par.hash(), "transaction hashes must match in order");
            assert_eq!(seq.signer(), par.signer(), "recovered signers must match in order");
        }
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

    #[tokio::test]
    async fn test_get_worker_fee_configs() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with 2 workers: worker 0 = EIP-1559 (strategy 0, target 30M),
        // worker 1 = Static (strategy 1, fee 500)
        let worker_configs = vec![(0u8, 30_000_000u64), (1u8, 500u64)];
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            worker_configs,
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Fee Config Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // read back worker configs from chain state - the returned length IS the on-chain
        // numWorkers()
        let configs = reth_env.get_worker_fee_configs()?;
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0], WorkerFeeConfig::Eip1559 { target_gas: 30_000_000 });
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // the block-pinned read primitive reports the same count at genesis
        let (num_workers, configs_at_block) =
            reth_env.get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs_at_block, configs);

        Ok(())
    }

    /// Pins the per-epoch worker-count read rule: the count for epoch E is the `WorkerConfigs`
    /// state at E's first block's parent (= E-1's closing block). Governance submits
    /// `setWorkerConfig` + `setNumWorkers` during epoch 0; the count read at epoch 1's
    /// start-parent block reflects the change, while the count read at epoch 0's start-parent
    /// (genesis) still reports the original single worker.
    #[tokio::test]
    async fn test_worker_count_read_at_epoch_start_parent() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        // governance owns the WorkerConfigs contract and signs the config txs
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with a single worker (the canonical existing-chain shape)
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Count Epoch Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: epoch 0 starts at blockHeight 0, so its start-parent (saturating) is genesis
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        let epoch_zero_read_block = epoch_info.blockHeight.saturating_sub(1);
        assert_eq!(epoch_zero_read_block, 0);

        // governance grows the worker set mid-epoch: config worker 1 (Static 500), then grow
        // the count (setWorkerConfig must precede setNumWorkers per the contract)
        let set_config_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setWorkerConfigCall { workerId: 1, strategy: 1, value: 500, data: 0 }
                .abi_encode()
                .into(),
        );
        let set_num_workers_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setNumWorkersCall { numWorkers_: 2 }.abi_encode().into(),
        );

        // block 1 (mid-epoch-0): the governance txs land
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![set_config_tx, set_num_workers_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // block 2 closes epoch 0 -> epoch 1's first block will be 3, start-parent = block 2
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let close_block_hash = block2.recovered_block.clone_sealed_header().hash();

        // the new epoch's info points its start-parent at the closing block
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        let epoch_one_read_block = epoch_info.blockHeight.saturating_sub(1);
        let epoch_one_read_header = reth_env
            .sealed_header_by_number(epoch_one_read_block)?
            .expect("epoch 1 start-parent header");
        assert_eq!(epoch_one_read_header.hash(), close_block_hash);

        // epoch 1's count (read at its start-parent) reflects the governance change...
        let (num_workers, configs) =
            reth_env.get_worker_fee_configs_at_block(epoch_one_read_header.hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // ...while epoch 0's count (read at genesis) still reports the original single worker
        let genesis_hash = reth_env
            .sealed_header_by_number(epoch_zero_read_block)?
            .expect("genesis header")
            .hash();
        let (num_workers, _configs) = reth_env.get_worker_fee_configs_at_block(genesis_hash)?;
        assert_eq!(num_workers, 1);

        Ok(())
    }

    /// Classification pin: the pinned-block read paths distinguish node-local provider faults
    /// (the pinned hash/header not resolving on THIS node - peers may read fine) from
    /// chain-global failures (contract absent at the block - identical on every node). The
    /// close-time base-fee compute keys retry-then-halt vs keep-current off this split.
    #[tokio::test]
    async fn test_state_read_classifies_provider_vs_chain_global() -> eyre::Result<()> {
        // healthy provider/database, but with the system contracts stripped from the alloc so
        // the contract reads fail deterministically at every block
        let mut genesis = tn_types::test_genesis();
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        genesis.alloc.remove(&CONSENSUS_REGISTRY_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("State Read Classification Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // PROVIDER: an unknown/random block hash on a healthy env is a node-local resolution
        // failure (the fault never reaches the contract)
        let err = reth_env
            .get_worker_fee_configs_at_block(B256::random())
            .expect_err("unknown block hash must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unknown block hash must classify as Provider, got: {err}"
        );

        // CHAIN-GLOBAL: the hash resolves (genesis) but the WorkerConfigs contract is absent -
        // the call succeeds with empty return data and the decode fails deterministically
        let err = reth_env
            .get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())
            .expect_err("absent contract must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent contract must classify as ChainGlobal, got: {err}"
        );

        // the close-time identity read classifies the same way: a header this node cannot
        // resolve state for is Provider...
        let phantom = SealedHeader::new(ExecHeader::default(), B256::random());
        let err = reth_env
            .get_current_epoch_info_at_header(&phantom)
            .expect_err("phantom header must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unresolvable header state must classify as Provider, got: {err}"
        );

        // ...while an absent ConsensusRegistry at a resolvable block is ChainGlobal
        let err = reth_env
            .get_current_epoch_info_at_header(&chain.sealed_genesis_header())
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent registry must classify as ChainGlobal, got: {err}"
        );

        Ok(())
    }

    /// Regression: a `WorkerConfigs` constructor revert must FAIL genesis creation.
    ///
    /// Strategy 2 exceeds the contract's `MAX_STRATEGY` (= 1), so the constructor reverts
    /// `InvalidStrategy`. Before the fix the reverted state was committed anyway, shipping
    /// runtime code with empty storage: `numWorkers() = 0` and `owner() = address(0)` —
    /// permanently unownable and masked downstream by fail-open defaults.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_worker_config_strategy() {
        let err = crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(
            4,
            vec![(2u8, 30_000_000u64)],
        )
        .expect_err("strategy 2 exceeds the contract's MAX_STRATEGY and must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: an EMPTY worker config set must FAIL genesis creation (the
    /// `WorkerConfigs` constructor reverts `NumWorkersBelowMinimum`). See
    /// [`genesis_ceremony_rejects_invalid_worker_config_strategy`] for the pre-fix failure mode.
    #[tokio::test]
    async fn genesis_ceremony_rejects_empty_worker_configs() {
        let err =
            crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(4, vec![])
                .expect_err("empty worker configs must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: the `ConsensusRegistry` pre-genesis create is guarded by
    /// the same success check. A proof of possession generated for the WRONG address fails the
    /// constructor's BLS precompile verification (`InvalidProofOfPossession`), which must fail
    /// genesis creation instead of committing a half-initialized registry.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_consensus_registry_pop() {
        let validator_address = Address::from_slice(&[0x11; 20]);
        let wrong_address = Address::from_slice(&[0x22; 20]);
        let mut rng = StdRng::seed_from_u64(0);
        let bls = BlsKeypair::generate(&mut rng);
        // sign the proof of possession over the wrong address
        let pop = generate_proof_of_possession_bls_for_test(&bls, &wrong_address)
            .expect("pop generation failed");
        let validator = NodeInfo {
            name: "validator-0".to_string(),
            bls_public_key: *bls.public(),
            p2p_info: NodeP2pInfo::default(),
            execution_address: validator_address,
            proof_of_possession: pop,
        };

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").expect("parse stake amount")),
            minWithdrawAmount: U256::from(parse_ether("1_000").expect("parse min withdraw")),
            epochIssuance: U256::from(parse_ether("25_806").expect("parse epoch issuance")),
            epochDuration: 60 * 60 * 8,
        };

        let err = RethEnv::create_consensus_registry_genesis_accounts(
            vec![validator],
            tn_types::test_genesis(),
            initial_stake_config,
            Address::from_slice(&[0x99; 20]),
            vec![(0u8, 30_000_000u64)],
        )
        .expect_err("invalid proof of possession must fail genesis");
        assert!(
            format!("{err:#}").contains("ConsensusRegistry constructor reverted"),
            "error must name the ConsensusRegistry revert, got: {err:#}"
        );
    }
}
