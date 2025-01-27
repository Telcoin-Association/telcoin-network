//! Compatibility types for Telcoin Node and reth.
use std::{marker::PhantomData, sync::Arc};

use crate::{ExecHeader, TransactionSigned};
use reth_chainspec::ChainSpec;
use reth_db::{
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
    Database,
};
use reth_evm::{execute::BlockExecutorProvider, ConfigureEvm};
use reth_evm_ethereum::EthEvmConfig;
use reth_node_builder::{NodeTypes, NodeTypesWithDB, NodeTypesWithEngine};
use reth_node_ethereum::{
    BasicBlockExecutorProvider, EthEngineTypes, EthExecutionStrategyFactory, EthExecutorProvider,
};
use reth_primitives::EthPrimitives;
use reth_provider::EthStorage;
use reth_trie_db::MerklePatriciaTrie;

/// Telcoin Network specific node types for reth compatibility.
pub trait TelcoinNodeTypes: NodeTypesWithEngine + NodeTypesWithDB {
    /// The EVM executor type
    type Executor: BlockExecutorProvider<Primitives = EthPrimitives>;

    /// The EVM configuration type
    type EvmConfig: ConfigureEvm<Transaction = TransactionSigned, Header = ExecHeader>;

    // Add factory methods to create generic components
    fn create_evm_config(chain: Arc<ChainSpec>) -> Self::EvmConfig;
    fn create_executor(chain: Arc<ChainSpec>) -> Self::Executor;
}

#[derive(Clone)]
pub struct TelcoinNode<DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static>
{
    _phantom: PhantomData<DB>,
}

impl<DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static> NodeTypes
    for TelcoinNode<DB>
{
    type Primitives = EthPrimitives;
    type ChainSpec = ChainSpec;
    type StateCommitment = MerklePatriciaTrie;
    type Storage = EthStorage;
}

impl<DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static>
    NodeTypesWithEngine for TelcoinNode<DB>
{
    type Engine = EthEngineTypes;
}

impl<DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static> NodeTypesWithDB
    for TelcoinNode<DB>
{
    type DB = DB;
}

impl<DB: Database + DatabaseMetrics + DatabaseMetadata + Clone + Unpin + 'static> TelcoinNodeTypes
    for TelcoinNode<DB>
{
    type Executor = BasicBlockExecutorProvider<EthExecutionStrategyFactory>;
    type EvmConfig = EthEvmConfig;

    fn create_evm_config(chain: Arc<ChainSpec>) -> Self::EvmConfig {
        EthEvmConfig::new(chain)
    }

    fn create_executor(chain: Arc<ChainSpec>) -> Self::Executor {
        EthExecutorProvider::ethereum(chain)
    }
}
