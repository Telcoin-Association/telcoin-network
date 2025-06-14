//! Test-utilities for execution/engine node.

use clap::Parser as _;
use core::fmt;
use std::{path::Path, str::FromStr, sync::Arc};
use telcoin_network::{node::NodeCommand, NoArgs};
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_node::engine::{ExecutionNode, TnBuilder};
use tn_reth::{RethChainSpec, RethCommand, RethConfig, RethEnv};
use tn_types::{
    gas_accumulator::RewardsCounter, Address, TaskManager, TimestampSec, Withdrawals, B256,
};

/// Convenience type for testing Execution Node.
pub type TestExecutionNode = ExecutionNode;

/// Convenience function for creating engine node using tempdir and optional args.
/// Defaults if params not provided:
/// - opt_authority_identifier: `AuthorityIdentifier(1)`
/// - opt_chain: `adiri`
/// - opt_address: `0x1111111111111111111111111111111111111111`
pub fn default_test_execution_node(
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    tmp_dir: &Path,
) -> eyre::Result<TestExecutionNode> {
    let (builder, _) = execution_builder::<NoArgs>(
        opt_chain.clone(),
        opt_address,
        None, // optional args
        tmp_dir,
    )?;

    // create engine node
    let engine = if let Some(chain) = opt_chain {
        ExecutionNode::new(
            &builder,
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir, &TaskManager::default())?,
        )?
    } else {
        ExecutionNode::new(&builder, RethEnv::new_for_test(tmp_dir, &TaskManager::default())?)?
    };

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
fn execution_builder<CliExt: clap::Args + fmt::Debug>(
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    opt_args: Option<Vec<&str>>,
    tmp_dir: &Path,
) -> eyre::Result<(TnBuilder, CliExt)> {
    let default_args = ["telcoin-network", "--http", "--chain", "adiri"];

    // extend faucet args if provided
    let cli_args = if let Some(args) = opt_args {
        [&default_args, &args[..]].concat()
    } else {
        default_args.to_vec()
    };

    // use same approach as telcoin-network binary
    let command = NodeCommand::<CliExt>::try_parse_from(cli_args)?;

    let NodeCommand { instance, ext, reth, .. } = command;
    let RethCommand { rpc, txpool, db, .. } = reth;

    let reth_command = RethCommand { rpc, txpool, db };

    let mut tn_config = Config::default_for_test();
    if let Some(chain) = opt_chain {
        // overwrite chain spec if passed in
        tn_config.genesis = chain.genesis().clone();
    }

    // check args then use test defaults
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // update execution address
    tn_config.node_info.execution_address = address;

    // TODO: this a temporary approach until upstream reth supports public rpc hooks
    let opt_faucet_args = None;
    let builder = TnBuilder {
        node_config: RethConfig::new(
            reth_command,
            instance,
            tmp_dir,
            true,
            Arc::new(tn_config.chain_spec()),
        ),
        tn_config,
        opt_faucet_args,
        metrics: None,
    };

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
    opt_chain: Option<Arc<RethChainSpec>>,
    opt_address: Option<Address>,
    faucet_proxy_address: Address,
    tmp_dir: &Path,
) -> eyre::Result<TestExecutionNode> {
    let faucet_args = ["--google-kms"];

    // TODO: support non-google-kms faucet
    let extended_args = if google_kms { Some(faucet_args.to_vec()) } else { None };
    // always include default expected faucet derived from `TransactionFactory::default`
    let faucet = faucet_proxy_address.to_string();
    let extended_args =
        extended_args.map(|opt| [opt, vec!["--faucet-contract", &faucet]].concat().to_vec());

    // execution builder + faucet args
    let (builder, faucet) =
        execution_builder::<FaucetArgs>(opt_chain.clone(), opt_address, extended_args, tmp_dir)?;

    // replace default builder's faucet args
    let TnBuilder { node_config, tn_config, .. } = builder;
    let builder = TnBuilder {
        node_config: node_config.clone(),
        tn_config,
        opt_faucet_args: Some(faucet),
        metrics: None,
    };

    // create engine node
    let reth_db = RethEnv::new_database(&node_config, tmp_dir.join("db"))?;
    let engine = ExecutionNode::new(
        &builder,
        RethEnv::new(
            &node_config,
            &TaskManager::default(),
            reth_db,
            None,
            RewardsCounter::default(),
        )?,
    )?;

    Ok(engine)
}

/// Optional parameters to pass to the `execute_test_batch` function.
///
/// These optional parameters are used to replace default in the batch's header if included.
#[derive(Debug, Default)]
pub struct OptionalTestBatchParams {
    /// Optional beneficiary address.
    ///
    /// Default is `Address::random()`.
    pub beneficiary_opt: Option<Address>,
    /// Optional withdrawals.
    ///
    /// Default is `Withdrawals<vec![]>` (empty).
    pub withdrawals_opt: Option<Withdrawals>,
    /// Optional timestamp.
    ///
    /// Default is `now()`.
    pub timestamp_opt: Option<TimestampSec>,
    /// Optional mix_hash.
    ///
    /// Default is `B256::random()`.
    pub mix_hash_opt: Option<B256>,
    /// Optional base_fee_per_gas.
    ///
    /// Default is [MIN_PROTOCOL_BASE_FEE], which is 7 wei.
    pub base_fee_per_gas_opt: Option<u64>,
}
