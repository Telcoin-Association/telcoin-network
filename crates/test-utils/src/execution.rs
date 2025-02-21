//! Test-utilities for execution/engine node.

use alloy::signers::{k256::FieldBytes, local::PrivateKeySigner};
use clap::{Args, Parser};
use core::fmt;
use rand::{rngs::StdRng, Rng, SeedableRng};
use reth_chainspec::{BaseFeeParams, ChainSpec};
use reth_cli_commands::node::NoArgs;
use reth_db::{
    test_utils::{create_test_rw_db, TempDatabase},
    DatabaseEnv,
};
use reth_evm::execute::{BlockExecutionOutput, BlockExecutorProvider, Executor as _};
use reth_node_core::{args::DatadirArgs, node_config::NodeConfig};
use reth_primitives_traits::crypto::secp256k1::sign_message;
use reth_provider::{BlockReaderIdExt, ExecutionOutcome, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_rpc_eth_types::utils::recover_raw_transaction;
use reth_transaction_pool::{EthPooledTransaction, TransactionOrigin, TransactionPool};
use secp256k1::Secp256k1;
use std::{str::FromStr, sync::Arc};
use telcoin_network::node::NodeCommand;
use tempfile::tempdir;
use tn_config::Config;
use tn_faucet::FaucetArgs;
use tn_node::engine::{ExecutionNode, TnBuilder};
use tn_node_traits::TelcoinNode;
use tn_types::{
    adiri_genesis, calculate_transaction_root, now, public_key_to_address, AccessList, Address,
    Batch, Block, BlockBody, BlockExt as _, BlockHeader as _, Bytes, Encodable2718 as _,
    EthPrimitives, EthSignature, ExecHeader, ExecutionKeypair, Genesis, GenesisAccount,
    SealedHeader, SignedTransactionIntoRecoveredExt as _, TaskManager, TimestampSec, Transaction,
    TransactionSigned, TxEip1559, TxHash, TxKind, Withdrawals, B256, EMPTY_OMMER_ROOT_HASH,
    EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE, U256,
};
use tracing::debug;

/// Convnenience type for testing Execution Node.
pub type TestExecutionNode = ExecutionNode<TelcoinNode<Arc<TempDatabase<DatabaseEnv>>>>;

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
) -> eyre::Result<TestExecutionNode> {
    let (builder, _) = execution_builder::<NoArgs>(
        opt_chain,
        opt_address,
        None, // optional args
    )?;

    // create engine node
    let engine = ExecutionNode::new(&builder, &TaskManager::default())?;

    Ok(engine)
}

/// Create CLI command for tests calling `ExecutionNode::new`.
pub fn execution_builder<CliExt: clap::Args + fmt::Debug>(
    opt_chain: Option<Arc<ChainSpec>>,
    opt_address: Option<Address>,
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

    let builder =
        TnBuilder { database, node_config, tn_config, opt_faucet_args, consensus_metrics: None };

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
    faucet_proxy_address: Address,
) -> eyre::Result<TestExecutionNode> {
    let faucet_args = ["--google-kms"];

    // TODO: support non-google-kms faucet
    let extended_args = if google_kms { Some(faucet_args.to_vec()) } else { None };
    // always include default expected faucet derived from `TransactionFactory::default`
    let faucet = faucet_proxy_address.to_string();
    let extended_args =
        extended_args.map(|opt| [opt, vec!["--contract-address", &faucet]].concat().to_vec());

    // execution builder + faucet args
    let (builder, faucet) = execution_builder::<FaucetArgs>(opt_chain, opt_address, extended_args)?;

    // replace default builder's faucet args
    let TnBuilder { database, node_config, tn_config, .. } = builder;
    let builder = TnBuilder {
        database,
        node_config,
        tn_config,
        opt_faucet_args: Some(faucet),
        consensus_metrics: None,
    };

    // create engine node
    let engine = ExecutionNode::new(&builder, &TaskManager::default())?;

    Ok(engine)
}

/// Adiri genesis with funded [TransactionFactory] default account.
pub fn test_genesis() -> Genesis {
    let genesis = adiri_genesis();
    let default_address = TransactionFactory::default().address();
    let default_factory_account =
        vec![(default_address, GenesisAccount::default().with_balance(U256::MAX))];
    genesis.extend_accounts(default_factory_account)
}

/// Helper function to seed addresses within adiri genesis.
///
/// Each address is funded with 99_000_000 TEL at genesis.
pub fn adiri_genesis_seeded(accounts: Vec<Address>) -> Genesis {
    let accounts = accounts.into_iter().map(|acc| {
        (
            acc,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )
    });
    adiri_genesis().extend_accounts(accounts)
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
        let (tx, address) = recover_raw_transaction::<TransactionSigned>(tx_bytes)
            .expect("raw transaction recovered")
            .into_parts();
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
pub fn execution_outcome_for_tests<P, E>(
    batch: &Batch,
    parent: &SealedHeader,
    provider: &P,
    executor: &E,
) -> ExecutionOutcome
where
    P: StateProviderFactory + BlockReaderIdExt,
    E: BlockExecutorProvider<Primitives = EthPrimitives>,
{
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
        let tx = recover_raw_transaction::<TransactionSigned>(tx_bytes)
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

    // create execution db
    let mut db = StateProviderDatabase::new(
        provider.latest().expect("provider retrieves latest during test batch execution"),
    );

    // convenience
    let block_number = block.number;

    // execute the block
    let BlockExecutionOutput { state, receipts, .. } = executor
        .executor(&mut db)
        .execute(&block)
        .expect("executor can execute test batch transactions");
    ExecutionOutcome::new(state, receipts.into(), block_number, vec![])
}

/// Transaction factory
#[derive(Clone, Copy, Debug)]
pub struct TransactionFactory {
    /// Keypair for signing transactions
    keypair: ExecutionKeypair,
    /// The nonce for the next transaction constructed.
    nonce: u64,
}

impl Default for TransactionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionFactory {
    /// Create a new instance of self from a [0; 32] seed.
    ///
    /// Address: 0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7
    /// Secret: 9bf49a6a0755f953811fce125f2683d50429c3bb49e074147e0089a52eae155f
    pub fn new() -> Self {
        let mut rng = StdRng::from_seed([0; 32]);
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rng);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a provided seed.
    pub fn new_random_from_seed<R: Rng + ?Sized>(rand: &mut R) -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(rand);
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// create a new instance of self from a random seed.
    pub fn new_random() -> Self {
        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);
        Self { keypair, nonce: 0 }
    }

    /// Return the address of the signer.
    pub fn address(&self) -> Address {
        let public_key = self.keypair.public_key();
        public_key_to_address(public_key)
    }

    /// Change the nonce for the next transaction.
    pub fn set_nonce(&mut self, nonce: u64) {
        self.nonce = nonce;
    }

    /// Increment nonce after a transaction was created and signed.
    pub fn inc_nonce(&mut self) {
        self.nonce += 1;
    }

    /// Create a signed EIP1559 transaction and encode it.
    pub fn create_eip1559_encoded(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_limit: Option<u64>,
        gas_price: u128,
        to: Option<Address>,
        value: U256,
        input: Bytes,
    ) -> Vec<u8> {
        self.create_eip1559(chain, gas_limit, gas_price, to, value, input).encoded_2718()
    }

    /// Create and sign an EIP1559 transaction.
    pub fn create_eip1559(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_limit: Option<u64>,
        gas_price: u128,
        to: Option<Address>,
        value: U256,
        input: Bytes,
    ) -> TransactionSigned {
        let gas_limit = gas_limit.unwrap_or(1_000_000);
        let tx_kind = match to {
            Some(address) => TxKind::Call(address),
            None => TxKind::Create,
        };

        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain.chain.id(),
            nonce: self.nonce,
            max_priority_fee_per_gas: 0,
            max_fee_per_gas: gas_price,
            gas_limit,
            to: tx_kind,
            value,
            input,
            access_list: Default::default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for next tx
        self.inc_nonce();

        TransactionSigned::new_unhashed(transaction, signature)
    }

    /// Create and sign an EIP1559 transaction with all possible parameters passed.
    ///
    /// All arguments are optional and default to:
    /// - chain_id: 2017 (adiri testnet)
    /// - nonce: `Self::nonce` (correctly incremented)
    /// - max_priority_fee_per_gas: 0 (no tip)
    /// - max_fee_per_gas: basefee minimum (7 wei)
    /// - gas_limit: 1_000_000 wei
    /// - to: None (results in `TxKind::Create`)
    /// - value: 1TEL (1^10*18 wei)
    /// - input: empty bytes (`Bytes::default()`)
    /// - access_list: None
    ///
    /// NOTE: the nonce is still incremented to track the number of signed transactions for `Self`.
    #[allow(clippy::too_many_arguments)]
    pub fn create_explicit_eip1559(
        &mut self,
        chain_id: Option<u64>,
        nonce: Option<u64>,
        max_priority_fee_per_gas: Option<u128>,
        max_fee_per_gas: Option<u128>,
        gas_limit: Option<u64>,
        to: Option<Address>,
        value: Option<U256>,
        input: Option<Bytes>,
        access_list: Option<AccessList>,
    ) -> TransactionSigned {
        let tx_kind = match to {
            Some(address) => TxKind::Call(address),
            None => TxKind::Create,
        };

        // Eip1559
        let transaction = Transaction::Eip1559(TxEip1559 {
            chain_id: chain_id.unwrap_or(2017),
            nonce: nonce.unwrap_or(self.nonce),
            max_priority_fee_per_gas: max_priority_fee_per_gas.unwrap_or(0),
            max_fee_per_gas: max_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE.into()),
            gas_limit: gas_limit.unwrap_or(1_000_000),
            to: tx_kind,
            value: value.unwrap_or_else(|| {
                U256::from(10).checked_pow(U256::from(18)).expect("1x10^18 does not overflow")
            }),
            input: input.unwrap_or_default(),
            access_list: access_list.unwrap_or_default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for self
        self.inc_nonce();

        TransactionSigned::new_unhashed(transaction, signature)
    }

    /// Sign the transaction hash with the key in memory
    fn sign_hash(&self, hash: B256) -> EthSignature {
        // let env = std::env::var("WALLET_SECRET_KEY")
        //     .expect("Wallet address is set through environment variable");
        // let secret: B256 = env.parse().expect("WALLET_SECRET_KEY must start with 0x");
        // let secret = B256::from_slice(self.keypair.secret.as_ref());
        let secret = B256::from_slice(&self.keypair.secret_bytes());
        let signature = sign_message(secret, hash);
        signature.expect("failed to sign transaction")
    }

    /// Helper to instantiate an `alloy-signer-local::PrivateKeySigner` wrapping the default account
    pub fn get_default_signer(&self) -> eyre::Result<PrivateKeySigner> {
        // circumvent Secp256k1 <> k256 type incompatibility via FieldBytes intermediary
        let binding = self.keypair.secret_key().secret_bytes();
        let secret_bytes_array = FieldBytes::from_slice(&binding);
        Ok(PrivateKeySigner::from_field_bytes(secret_bytes_array)?)
    }

    /// Create and submit the next transaction to the provided [TransactionPool].
    pub async fn create_and_submit_eip1559_pool_tx<Pool>(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_price: u128,
        to: Address,
        value: U256,
        pool: Pool,
    ) -> TxHash
    where
        Pool: TransactionPool<Transaction = EthPooledTransaction>,
    {
        let tx = self.create_eip1559(chain, None, gas_price, Some(to), value, Bytes::new());
        let pooled_tx = tx.try_into_pooled().expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");

        pool.add_transaction(TransactionOrigin::Local, recovered.into())
            .await
            .expect("recovered tx added to pool")
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool<Pool>(&self, tx: TransactionSigned, pool: Pool) -> TxHash
    where
        Pool: TransactionPool<Transaction = EthPooledTransaction>,
    {
        let pooled_tx = tx.try_into_pooled().expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");

        debug!("transaction: \n{recovered:?}\n");

        pool.add_transaction(TransactionOrigin::Local, recovered.into())
            .await
            .expect("recovered tx added to pool")
    }
}

/// Helper to get the gas price based on the provider's latest header.
pub fn get_gas_price<Provider>(provider: &Provider) -> u128
where
    Provider: BlockReaderIdExt,
{
    let header = provider
        .latest_header()
        .expect("latest header from provider for gas price")
        .expect("latest header is some for gas price");
    header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default().into()
}

/// Test utility to get desired state changes from a temporary genesis for a subsequent one.
pub async fn get_contract_state_for_genesis(
    chain: Arc<ChainSpec>,
    raw_txs_to_execute: Vec<Vec<u8>>,
) -> eyre::Result<ExecutionOutcome> {
    let execution_node = default_test_execution_node(Some(chain.clone()), None)?;
    let provider = execution_node.get_provider().await;
    let batch_executor = execution_node.get_batch_executor().await;

    // execute batch
    let parent = chain.sealed_genesis_header();
    let batch = Batch {
        transactions: raw_txs_to_execute,
        parent_hash: parent.hash(),
        ..Default::default()
    };
    let execution_outcome =
        execution_outcome_for_tests(&batch, &parent, &provider, &batch_executor);

    Ok(execution_outcome)
}
