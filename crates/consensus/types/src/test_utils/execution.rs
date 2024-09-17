// Copyright (c) Telcoin, LLC
// SPDX-License-Identifier: Apache-2.0

//! Specific test utils for execution layer
use crate::{adiri_genesis, now, ExecutionKeypair, TimestampSec, WorkerBlock};
use alloy::signers::{k256::FieldBytes, local::PrivateKeySigner};
use rand::{rngs::StdRng, Rng, SeedableRng};
use reth_chainspec::{BaseFeeParams, ChainSpec};
use reth_evm::execute::{BlockExecutionOutput, BlockExecutorProvider, Executor as _};
use reth_primitives::{
    constants::{
        EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS, ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE,
    },
    proofs, public_key_to_address, sign_message, Address, Block, Bytes, Genesis, GenesisAccount,
    Header, PooledTransactionsElement, SealedHeader, Signature, Transaction, TransactionSigned,
    TxEip1559, TxHash, TxKind, Withdrawals, B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{BlockReaderIdExt, ExecutionOutcome, StateProviderFactory};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::{PoolTransaction, TransactionOrigin, TransactionPool};
use reth_trie::HashedPostState;
use secp256k1::Secp256k1;
use std::{str::FromStr as _, sync::Arc};
use tracing::debug;

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
    batch: &WorkerBlock,
) -> (Genesis, Vec<TransactionSigned>, Vec<Address>) {
    let mut senders = vec![];
    let mut accounts_to_seed = Vec::new();

    // loop through the transactions
    for tx_signed in batch.transactions() {
        let address = tx_signed.recover_signer().expect("signer recoverable");
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
    (genesis.extend_accounts(accounts_to_seed), batch.transactions().clone(), senders)
}

/// Helper function to seed an instance of Genesis with random batches.
///
/// The transactions in the randomly generated batches are decoded and their signers are recovered.
///
/// The function returns the new Genesis, the signed transactions by batch, and the addresses for
/// further use it testing.
pub fn seeded_genesis_from_random_batches<'a>(
    mut genesis: Genesis,
    batches: impl IntoIterator<Item = &'a WorkerBlock>,
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

/// Attempt to update batch with accurate header information.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
pub fn execute_test_batch<P, E>(
    worker_block: &mut WorkerBlock,
    parent: &SealedHeader,
    optional_params: OptionalTestBatchParams,
    provider: &P,
    executor: &E,
) where
    P: StateProviderFactory + BlockReaderIdExt,
    E: BlockExecutorProvider,
{
    // deconstruct optional parameters for header
    let OptionalTestBatchParams {
        beneficiary_opt,
        withdrawals_opt,
        timestamp_opt,
        mix_hash_opt,
        base_fee_per_gas_opt,
    } = optional_params;

    // create "empty" header with default values
    let mut header = Header {
        parent_hash: parent.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: beneficiary_opt.unwrap_or_else(|| Address::random()),
        state_root: Default::default(),
        transactions_root: Default::default(),
        receipts_root: Default::default(),
        withdrawals_root: Some(
            withdrawals_opt
                .clone()
                .map_or(EMPTY_WITHDRAWALS, |w| proofs::calculate_withdrawals_root(&w)),
        ),
        logs_bloom: Default::default(),
        difficulty: U256::ZERO,
        number: parent.number + 1,
        gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
        gas_used: 0,
        timestamp: timestamp_opt.unwrap_or_else(now),
        mix_hash: mix_hash_opt.unwrap_or_else(|| B256::random()),
        nonce: 0,
        base_fee_per_gas: base_fee_per_gas_opt.or(Some(MIN_PROTOCOL_BASE_FEE)),
        blob_gas_used: None,
        excess_blob_gas: None,
        extra_data: Default::default(),
        parent_beacon_block_root: None,
        requests_root: None,
    };

    // update header's transactions root
    header.transactions_root = if worker_block.transactions().is_empty() {
        EMPTY_TRANSACTIONS
    } else {
        proofs::calculate_transaction_root(worker_block.transactions())
    };

    // recover senders from block
    let block = Block {
        header,
        body: worker_block.transactions().clone(),
        ommers: vec![],
        withdrawals: withdrawals_opt.clone(),
        requests: None,
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
    let BlockExecutionOutput { state, receipts, gas_used, .. } = executor
        .executor(&mut db)
        .execute((&block, U256::ZERO).into())
        .expect("executor can execute test batch transactions");
    let execution_outcome = ExecutionOutcome::new(state, receipts.into(), block_number, vec![]);
    let hashed_state = HashedPostState::from_bundle_state(&execution_outcome.state().state);

    // retrieve header to update values post-execution
    let Block { mut header, .. } = block.block;

    // update header
    header.gas_used = gas_used;
    header.state_root =
        db.state_root(hashed_state).expect("state root calculation during test batch execution");
    header.receipts_root = execution_outcome
        .receipts_root_slow(block_number)
        .expect("receipts root calculation during test batch execution");
    header.logs_bloom = execution_outcome
        .block_logs_bloom(block_number)
        .expect("logs bloom calculation during test batch execution");

    // seal header and update batch's metadata
    let sealed_header = header.seal_slow();
    worker_block.update_header(sealed_header);
}

/// Test utility to execute batch and return execution outcome.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
pub fn execution_outcome_from_test_batch_<P, E>(
    worker_block: &WorkerBlock,
    parent: &SealedHeader,
    optional_params: OptionalTestBatchParams,
    provider: &P,
    executor: &E,
) -> ExecutionOutcome
where
    P: StateProviderFactory + BlockReaderIdExt,
    E: BlockExecutorProvider,
{
    // deconstruct optional parameters for header
    let OptionalTestBatchParams {
        beneficiary_opt,
        withdrawals_opt,
        timestamp_opt,
        mix_hash_opt,
        base_fee_per_gas_opt,
    } = optional_params;

    // create "empty" header with default values
    let mut header = Header {
        parent_hash: parent.hash(),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary: beneficiary_opt.unwrap_or_else(|| Address::random()),
        state_root: Default::default(),
        transactions_root: Default::default(),
        receipts_root: Default::default(),
        withdrawals_root: Some(
            withdrawals_opt
                .clone()
                .map_or(EMPTY_WITHDRAWALS, |w| proofs::calculate_withdrawals_root(&w)),
        ),
        logs_bloom: Default::default(),
        difficulty: U256::ZERO,
        number: parent.number + 1,
        gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
        gas_used: 0,
        timestamp: timestamp_opt.unwrap_or_else(now),
        mix_hash: mix_hash_opt.unwrap_or_else(|| B256::random()),
        nonce: 0,
        base_fee_per_gas: base_fee_per_gas_opt.or(Some(MIN_PROTOCOL_BASE_FEE)),
        blob_gas_used: None,
        excess_blob_gas: None,
        extra_data: Default::default(),
        parent_beacon_block_root: None,
        requests_root: None,
    };

    // decode batch transactions
    let mut txs = vec![];
    for tx in worker_block.transactions() {
        let tx_signed = tx.clone();
        txs.push(tx_signed);
    }

    // update header's transactions root
    header.transactions_root = if worker_block.transactions().is_empty() {
        EMPTY_TRANSACTIONS
    } else {
        proofs::calculate_transaction_root(&txs)
    };

    // recover senders from block
    let block = Block {
        header,
        body: txs,
        ommers: vec![],
        withdrawals: withdrawals_opt.clone(),
        requests: None,
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
        .execute((&block, U256::ZERO).into())
        .expect("executor can execute test batch transactions");
    ExecutionOutcome::new(state, receipts.into(), block_number, vec![])
}

/// Transaction factory
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

    /// Create and sign an EIP1559 transaction.
    pub fn create_eip1559(
        &mut self,
        chain: Arc<ChainSpec>,
        gas_price: u128,
        to: Option<Address>,
        value: U256,
        input: Bytes,
    ) -> TransactionSigned {
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
            gas_limit: 1_000_000,
            to: tx_kind,
            value,
            input,
            access_list: Default::default(),
        });

        let tx_signature_hash = transaction.signature_hash();
        let signature = self.sign_hash(tx_signature_hash);

        // increase nonce for next tx
        self.inc_nonce();

        TransactionSigned::from_transaction_and_signature(transaction, signature)
    }

    /// Sign the transaction hash with the key in memory
    fn sign_hash(&self, hash: B256) -> Signature {
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
        Pool: TransactionPool,
    {
        let tx = self.create_eip1559(chain, gas_price, Some(to), value, Bytes::new());
        let pooled_tx =
            PooledTransactionsElement::try_from_broadcast(tx).expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_pooled(recovered);

        pool.add_transaction(TransactionOrigin::Local, transaction)
            .await
            .expect("recovered tx added to pool")
    }

    /// Submit a transaction to the provided pool.
    pub async fn submit_tx_to_pool<Pool>(&self, tx: TransactionSigned, pool: Pool) -> TxHash
    where
        Pool: TransactionPool,
    {
        let pooled_tx =
            PooledTransactionsElement::try_from_broadcast(tx).expect("tx valid for pool");
        let recovered = pooled_tx.try_into_ecrecovered().expect("tx is recovered");
        let transaction = <Pool::Transaction>::from_pooled(recovered);

        debug!("transaction: \n{transaction:?}\n");

        pool.add_transaction(TransactionOrigin::Local, transaction)
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

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_print_key_info() {
        // let mut rng = StdRng::from_seed([0; 32]);
        // let keypair = ExecutionKeypair::generate(&mut rng);

        let secp = Secp256k1::new();
        let (secret_key, _public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let keypair = ExecutionKeypair::from_secret_key(&secp, &secret_key);

        // let private = base64::encode(keypair.secret.as_bytes());
        let _secret = keypair.secret_bytes();
        // println!("secret: {:?}", hex::encode(secret));
        let _pubkey = keypair.public_key().serialize();
        // println!("public: {:?}", hex::encode(pubkey));

        // 9bf49a6a0755f953811fce125f2683d50429c3bb49e074147e0089a52eae155f
        // println!("{:?}", hex::encode(bytes));
        // public key hex [0; 32]
        // 029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964
        //
        // let pkey = secp256k1::PublicKey::from_str(
        //     "029bef8d556d80e43ae7e0becb3a7e6838b95defe45896ed6075bb9035d06c9964",
        // )
        // .unwrap();
        // println!("{:?}", public_key_to_address(pkey));
        // println!("pkey: {pkey:?}");
    }
}
