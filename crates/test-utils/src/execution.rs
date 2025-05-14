//! Test-utilities for execution/engine node.

use clap::Parser as _;
use core::fmt;
use std::{path::Path, str::FromStr, sync::Arc};
use telcoin_network::{node::NodeCommand, NoArgs};
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_node::engine::{ExecutionNode, TnBuilder};
use tn_reth::{
    recover_raw_transaction, ExecutionOutcome, RethChainSpec, RethCommand, RethConfig, RethEnv,
};
use tn_types::{
    calculate_transaction_root, now, Address, Batch, Block, BlockBody, BlockExt as _, ExecHeader,
    Genesis, GenesisAccount, SealedHeader, TaskManager, TimestampSec, TransactionSigned,
    Withdrawals, B256, EMPTY_OMMER_ROOT_HASH, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS,
    ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE, U256,
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
            RethEnv::new_for_test_with_chain(chain.clone(), tmp_dir, &TaskManager::default())?,
        )?
    } else {
        ExecutionNode::new(&builder, RethEnv::new_for_test(tmp_dir, &TaskManager::default())?)?
    };

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
pub fn execution_builder<CliExt: clap::Args + fmt::Debug>(
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

    let NodeCommand { config: _, instance, ext, reth, datadir: _, .. } = command;
    let RethCommand { chain, metrics, rpc, txpool, db, .. } = reth;

    // overwrite chain spec if passed in
    let chain = opt_chain.unwrap_or(chain);

    let reth_command = RethCommand { chain, metrics, rpc, txpool, db };

    let mut tn_config = Config::default();

    // check args then use test defaults
    let address = opt_address.unwrap_or_else(|| {
        Address::from_str("0x1111111111111111111111111111111111111111").expect("address from 0x1s")
    });

    // update execution address
    tn_config.validator_info.execution_address = address;

    // TODO: this a temporary approach until upstream reth supports public rpc hooks
    let opt_faucet_args = None;
    let builder = TnBuilder {
        node_config: RethConfig::new(reth_command, instance, None, tmp_dir, true),
        tn_config,
        opt_faucet_args,
        consensus_metrics: None,
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
        consensus_metrics: None,
    };

    // create engine node
    let reth_db = RethEnv::new_database(&node_config, tmp_dir.join("db"))?;
    let engine = ExecutionNode::new(
        &builder,
        RethEnv::new(&node_config, &TaskManager::default(), reth_db)?,
    )?;

    Ok(engine)
}

/// Helper function to seed an instance of Genesis with accounts from a random batch.
pub fn seeded_genesis_from_random_batch(
    genesis: Genesis,
    batch: &Batch,
) -> (Genesis, Vec<TransactionSigned>, Vec<Address>) {
    let max_capacity = batch.transactions.len();
    let mut decoded_txs = Vec::with_capacity(max_capacity);
    let mut senders = Vec::with_capacity(max_capacity);
    let mut accounts_to_seed = Vec::with_capacity(max_capacity);

    // loop through the transactions
    for tx_bytes in &batch.transactions {
        let (tx, address) =
            recover_raw_transaction(tx_bytes).expect("raw transaction recovered").into_parts();
        decoded_txs.push(tx);
        senders.push(address);
        // fund account with 99mil TEL
        let account = (
            address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        );
        accounts_to_seed.push(account);
    }
    (genesis.extend_accounts(accounts_to_seed), decoded_txs, senders)
}

/// Helper function to seed an instance of Genesis with random batches.
///
/// The transactions in the randomly generated batches are decoded and their signers are recovered.
///
/// The function returns the new Genesis, the signed transactions by batch, and the addresses for
/// further use it testing.
pub fn seeded_genesis_from_random_batches<'a>(
    mut genesis: Genesis,
    batches: impl IntoIterator<Item = &'a Batch>,
) -> (Genesis, Vec<Vec<TransactionSigned>>, Vec<Vec<Address>>) {
    let mut txs = vec![];
    let mut senders = vec![];
    for batch in batches {
        let (g, t, s) = seeded_genesis_from_random_batch(genesis, batch);
        genesis = g;
        txs.push(t);
        senders.push(s);
    }
    (genesis, txs, senders)
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

/// Test utility to execute batch and return execution outcome.
///
/// This is useful for simulating execution results for account state changes.
/// Currently only used by faucet tests to obtain faucet contract account info
/// by simulating deploying proxy contract. The results are then put into genesis.
pub fn execution_outcome_for_tests(
    batch: &Batch,
    parent: &SealedHeader,
    reth_env: RethEnv,
) -> ExecutionOutcome {
    // create "empty" header with default values
    let mut header = ExecHeader {
        parent_hash: parent.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: Address::ZERO,
        state_root: Default::default(),
        transactions_root: Default::default(),
        receipts_root: Default::default(),
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom: Default::default(),
        difficulty: U256::ZERO,
        number: parent.number + 1,
        gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
        gas_used: 0,
        timestamp: now(),
        mix_hash: B256::random(),
        nonce: 0_u64.into(),
        base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
        blob_gas_used: None,
        excess_blob_gas: None,
        extra_data: Default::default(),
        parent_beacon_block_root: None,
        requests_hash: None,
    };

    // decode batch transactions
    let mut txs = vec![];
    for tx_bytes in batch.transactions() {
        let tx = recover_raw_transaction(tx_bytes)
            .expect("raw transaction recovered for test")
            .into_tx();
        txs.push(tx);
    }

    // update header's transactions root
    header.transactions_root = if batch.transactions().is_empty() {
        EMPTY_TRANSACTIONS
    } else {
        calculate_transaction_root(&txs)
    };

    // recover senders from block
    let block = Block {
        header,
        body: BlockBody {
            transactions: txs,
            ommers: vec![],
            withdrawals: Some(Default::default()),
        },
    }
    .with_recovered_senders()
    .expect("unable to recover senders while executing test batch");

    // convenience
    let block_number = block.number;

    let (state, receipts) =
        reth_env.execute_for_test(&block).expect("executor can execute test batch transactions");
    ExecutionOutcome::new(state, receipts.into(), block_number, vec![])
}

/// Test utility to get desired state changes from a temporary genesis for a subsequent one.
pub async fn get_contract_state_for_genesis(
    chain: Arc<RethChainSpec>,
    raw_txs_to_execute: Vec<Vec<u8>>,
    tmp_dir: &Path,
) -> eyre::Result<ExecutionOutcome> {
    let execution_node = default_test_execution_node(Some(chain.clone()), None, tmp_dir)?;
    let reth_env = execution_node.get_reth_env().await;

    // execute batch
    let parent = chain.sealed_genesis_header();
    let batch = Batch {
        transactions: raw_txs_to_execute,
        parent_hash: parent.hash(),
        ..Default::default()
    };
    let execution_outcome = execution_outcome_for_tests(&batch, &parent, reth_env);

    Ok(execution_outcome)
}
