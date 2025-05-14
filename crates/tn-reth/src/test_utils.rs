//! Transaction factory to create legit transactions for execution.

use alloy::signers::local::PrivateKeySigner;
use enr::k256::FieldBytes;
use reth_chainspec::ChainSpec as RethChainSpec;
use reth_primitives::sign_message;
use secp256k1::{
    rand::{self, rngs::StdRng, Rng, SeedableRng as _},
    Secp256k1,
};
use std::sync::Arc;
use tn_types::{
    public_key_to_address, AccessList, Address, Bytes, Encodable2718, EthSignature,
    ExecutionKeypair, SignedTransactionIntoRecoveredExt as _, Transaction, TransactionSigned,
    TxEip1559, TxHash, TxKind, B256, MIN_PROTOCOL_BASE_FEE, U256,
};
use tracing::debug;

use crate::{RethEnv, WorkerTxPool};

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
