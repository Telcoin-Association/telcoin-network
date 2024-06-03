//! Node implementation for reth compatibility
//!
//! Inspired by reth_node_ethereum crate.

use reth_db::{
    database::Database,
    database_metrics::{DatabaseMetadata, DatabaseMetrics},
};
use reth_node_builder::node::{FullNodeTypes, NodeTypes};
use reth_node_ethereum::EthEngineTypes;
use reth_provider::FullProvider;
use std::marker::PhantomData;

/// Type configuration for a regular Telcoin node.
#[derive(Debug, Default, Clone, Copy)]
#[non_exhaustive]
pub struct PrimaryNode<DB, Provider> {
    db: PhantomData<DB>,
    evm: PhantomData<Provider>,
}

impl<DB, Provider> NodeTypes for PrimaryNode<DB, Provider>
where
    DB: Send + Sync + 'static,
    Provider: Send + Sync + 'static,
{
    type Primitives = ();
    type Engine = EthEngineTypes;
}

impl<DB, Provider> FullNodeTypes for PrimaryNode<DB, Provider>
where
    DB: Database + DatabaseMetadata + DatabaseMetrics + Unpin + Clone + 'static,
    Provider: FullProvider<DB>,
{
    type DB = DB;
    type Provider = Provider;
}
