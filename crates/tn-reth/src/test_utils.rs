//! Transaction factory to create legit transactions for execution.

use crate::{error::TnRethResult, recover_raw_transaction, RethEnv, WorkerTxPool};
use alloy::signers::local::PrivateKeySigner;
use enr::k256::FieldBytes;
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_evm::execute::{BlockExecutorProvider as _, Executor as _};
use reth_primitives::sign_message;
use reth_provider::{BlockExecutionOutput, ExecutionOutcome};
use reth_revm::{database::StateProviderDatabase, db::BundleState};
use secp256k1::{
    rand::{self, rngs::StdRng, Rng, SeedableRng as _},
    Secp256k1,
};
use std::{path::Path, str::FromStr, sync::Arc};
use tn_types::{
    adiri_chain_spec_arc, adiri_genesis, calculate_transaction_root, now, public_key_to_address,
    AccessList, Address, Batch, Block, BlockBody, BlockExt as _, BlockWithSenders, Bytes,
    Encodable2718, EthSignature, ExecHeader, ExecutionKeypair, Genesis, GenesisAccount, Receipt,
    SealedHeader, SignedTransactionIntoRecoveredExt as _, TaskManager, Transaction,
    TransactionSigned, TxEip1559, TxHash, TxKind, B256, EMPTY_OMMER_ROOT_HASH, EMPTY_TRANSACTIONS,
    EMPTY_WITHDRAWALS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE, U256,
};
use tracing::debug;

// methods for tests
impl RethEnv {
    /// Create a new RethEnv for testing only.
    pub fn new_for_test<P: AsRef<Path>>(
        db_path: P,
        task_manager: &TaskManager,
    ) -> eyre::Result<Self> {
        Self::new_for_temp_chain(adiri_chain_spec_arc(), db_path, task_manager)
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

    /// Test utility to execute batch and return execution outcome.
    ///
    /// This is useful for simulating execution results for account state changes.
    /// Currently only used by faucet tests to obtain faucet contract account info
    /// by simulating deploying proxy contract. The results are then put into genesis.
    pub fn execution_outcome_for_tests(
        &self,
        txs: Vec<Vec<u8>>,
        parent: &SealedHeader,
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

        // decode transactions
        let mut decoded_txs = vec![];
        for tx_bytes in &txs {
            let tx = recover_raw_transaction(tx_bytes)
                .expect("raw transaction recovered for test")
                .into_tx();
            decoded_txs.push(tx);
        }

        // update header's transactions root
        header.transactions_root = if txs.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            calculate_transaction_root(&decoded_txs)
        };

        // recover senders from block
        let block = Block {
            header,
            body: BlockBody {
                transactions: decoded_txs,
                ommers: vec![],
                withdrawals: Some(Default::default()),
            },
        }
        .with_recovered_senders()
        .expect("unable to recover senders while executing test batch");

        // convenience
        let block_number = block.number;

        let (state, receipts) =
            self.execute_for_test(&block).expect("executor can execute test batch transactions");
        ExecutionOutcome::new(state, receipts.into(), block_number, vec![])
    }
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
        chain: Arc<RethChainSpec>,
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
        chain: Arc<RethChainSpec>,
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
    pub async fn create_and_submit_eip1559_pool_tx(
        &mut self,
        chain: Arc<RethChainSpec>,
        gas_price: u128,
        to: Address,
        value: U256,
        pool: WorkerTxPool,
    ) -> TxHash {
        let tx = self.create_eip1559(chain, None, gas_price, Some(to), value, Bytes::new());
        let pooled_tx = tx.try_into_pooled().expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");

        pool.add_transaction_local(recovered.into()).await.expect("recovered tx added to pool")
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool(&self, tx: TransactionSigned, pool: WorkerTxPool) -> TxHash {
        let pooled_tx = tx.try_into_pooled().expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");

        debug!("transaction: \n{recovered:?}\n");

        pool.add_transaction_local(recovered.into()).await.expect("recovered tx added to pool")
    }
}

/// Helper to get the gas price based on the provider's latest header.
pub fn get_gas_price(reth_env: &RethEnv) -> u128 {
    reth_env.get_gas_price().expect("gas price")
}

/// Create a random encoded transaction.
pub fn transaction() -> Vec<u8> {
    let mut tx_factory = TransactionFactory::new_random();
    let chain = adiri_chain_spec_arc();
    let gas_price = 100_000;
    let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

    // random transaction
    tx_factory.create_eip1559_encoded(
        chain,
        None,
        gas_price,
        Some(Address::ZERO),
        value,
        Bytes::new(),
    )
}

/// will create a batch with randomly formed transactions
/// dictated by the parameter number_of_transactions
pub fn fixture_batch_with_transactions(number_of_transactions: u32) -> Batch {
    let transactions = (0..number_of_transactions).map(|_v| transaction()).collect();

    // Put some random bytes in the header so that tests will have unique headers.
    Batch { transactions, beneficiary: Address::random(), ..Default::default() }
}

/// Create a batch with two random, valid transactions. The rest of the [Batch] uses defaults.
pub fn batch() -> Batch {
    let transactions = vec![transaction(), transaction()];
    Batch { transactions, ..Default::default() }
}

/// generate multiple fixture batches. The number of generated batches
/// are dictated by the parameter num_of_batches.
pub fn batches(num_of_batches: usize) -> Vec<Batch> {
    let mut batches = Vec::new();

    for i in 1..num_of_batches + 1 {
        batches.push(batch_with_transactions(i));
    }

    batches
}

/// Create a batch with the specified number of transactions.
pub fn batch_with_transactions(num_of_transactions: usize) -> Batch {
    let mut transactions = Vec::new();

    for _ in 0..num_of_transactions {
        transactions.push(transaction());
    }

    Batch::new_for_test(transactions, ExecHeader::default())
}

/// Adiri genesis with funded [TransactionFactory] default account.
pub fn test_genesis() -> Genesis {
    let genesis = adiri_genesis();
    let default_address = TransactionFactory::default().address();
    let default_factory_account =
        vec![(default_address, GenesisAccount::default().with_balance(U256::MAX))];
    genesis.extend_accounts(default_factory_account)
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
