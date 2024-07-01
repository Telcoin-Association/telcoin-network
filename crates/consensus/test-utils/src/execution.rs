//! Test-utilities for execution/engine node.

use clap::{Args, Parser};
use core::fmt;
use reth::{
    args::DatadirArgs, builder::NodeConfig, commands::node::NoArgs, primitives::Address,
    tasks::TaskExecutor,
};
use reth_chainspec::ChainSpec;
use reth_db::{
    test_utils::{create_test_rw_db, TempDatabase},
    DatabaseEnv,
};
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use std::{str::FromStr, sync::Arc};
use telcoin_network::node::NodeCommand;
use tempfile::tempdir;
use tn_faucet::FaucetArgs;
use tn_node::engine::{ExecutionNode, TnBuilder};
use tn_types::Config;

/// Convnenience type for testing Execution Node.
pub type TestExecutionNode = ExecutionNode<Arc<TempDatabase<DatabaseEnv>>, EthExecutorProvider>;

/// A helper type to parse Args more easily.
#[derive(Parser, Debug)]
pub struct CommandParser<T: Args> {
    #[clap(flatten)]
    pub args: T,
}

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `adiri`
/// - opt_address: `0x1111111111111111111111111111111111111111`
pub fn default_test_execution_node(
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
    executor: TaskExecutor,
) -> eyre::Result<TestExecutionNode> {
    let (builder, _) = execution_builder::<NoArgs>(
        opt_chain,
        opt_address,
        executor,
        None, // optional args
    )?;

    let block_executor =
        EthExecutorProvider::new(Arc::clone(&builder.node_config.chain), EthEvmConfig::default());

    // create engine node
    let engine = ExecutionNode::new(builder, block_executor)?;

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
pub fn execution_builder<CliExt: clap::Args + fmt::Debug>(
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
    task_executor: TaskExecutor,
    opt_args: Option<Vec<&str>>,
) -> eyre::Result<(TnBuilder<Arc<TempDatabase<DatabaseEnv>>>, CliExt)> {
    let default_args = ["telcoin-network", "--dev", "--chain", "adiri"];

    // extend faucet args if provided
    let cli_args = if let Some(args) = opt_args {
        [&default_args, &args[..]].concat()
    } else {
        default_args.to_vec()
    };

    // use same approach as telcoin-network binary
    let command = NodeCommand::<CliExt>::try_parse_from(cli_args)?;

    let NodeCommand {
        config,
        chain,
        metrics,
        instance,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
        ..
    } = command;

    // overwrite chain spec if passed in
    let chain = opt_chain.unwrap_or(chain);

    let datadir = tempdir()?.into_path().into();
    let datadir = DatadirArgs { datadir, static_files_path: None };

    // set up reth node config for engine components
    let node_config = NodeConfig {
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

    // ensure unused ports
    let node_config = node_config.with_unused_ports();

    let database = create_test_rw_db();
    let mut tn_config = Config::default();

    // check args then use test defaults
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // update execution address
    tn_config.validator_info.execution_address = address;

    // TODO: this a temporary approach until upstream reth supports public rpc hooks
    let opt_faucet_args = None;

    let builder = TnBuilder { database, node_config, task_executor, tn_config, opt_faucet_args };

    Ok((builder, ext))
}

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `adiri`
/// - opt_address: `0x1111111111111111111111111111111111111111`
// #[cfg(feature = "faucet")]
pub fn faucet_test_execution_node(
    google_kms: bool,
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
    executor: TaskExecutor,
) -> eyre::Result<TestExecutionNode> {
    let faucet_args = ["--google-kms"];

    // TODO: support non-google-kms faucet
    let extended_args = if google_kms { Some(faucet_args.to_vec()) } else { None };

    // execution builder + faucet args
    let (builder, faucet) =
        execution_builder::<FaucetArgs>(opt_chain, opt_address, executor, extended_args)?;

    // replace default builder's faucet args
    let TnBuilder { database, node_config, task_executor, tn_config, .. } = builder;
    let builder = TnBuilder {
        database,
        node_config,
        task_executor,
        tn_config,
        opt_faucet_args: Some(faucet),
    };

    let block_executor =
        EthExecutorProvider::new(Arc::clone(&builder.node_config.chain), EthEvmConfig::default());

    // create engine node
    let engine = ExecutionNode::new(builder, block_executor)?;

    Ok(engine)
}
