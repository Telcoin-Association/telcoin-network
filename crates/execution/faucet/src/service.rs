//! Faucet rpc endpoint service.
//!
//! Service for creating and signing a transaction.
//! The service submits transactions directly to the
//! transaction pool to initiate a transfer to a requesting
//! address if the address hasn't received from the faucet
//! wallet within the time period.

use crate::{Drip, FaucetWallet, GoogleKMSClient, Secp256k1PubKeyBytes};
use alloy::sol_types::SolType;
use futures::StreamExt;
use gcloud_sdk::{
    google::cloud::kms::v1::{
        digest::Digest, key_management_service_client::KeyManagementServiceClient,
        AsymmetricSignRequest, Digest as KMSDigest,
    },
    GoogleApi,
};
use humantime::format_duration;
use lru_time_cache::LruCache;
use reth::rpc::server_types::eth::{EthApiError, EthResult, RpcInvalidTransactionError};
use reth_chainspec::BaseFeeParams;
use reth_primitives::{
    Address, Signature as EthSignature, Transaction, TransactionSigned, TxEip1559, TxHash, TxKind,
    B256, U256,
};
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use reth_tasks::TaskSpawner;
use reth_transaction_pool::{PoolTransaction, TransactionOrigin, TransactionPool};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId, Signature},
    Message, SECP256K1,
};
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::{Duration, SystemTime},
};
use tn_types::PendingWorkerBlock;
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error};

/// Service for managing requests.
///
/// The faucet receives an address from the RPC, checks the LRU cache,
/// and then submits a transaction (or returns an error). The faucet is a
/// direct address -> address transfer. The faucet address is seeded in genesis.
pub(crate) struct FaucetService<Provider, Pool, Tasks> {
    /// The faucet contract's address.
    ///
    /// The value is used to send call data to the contract that mints stablecoins.
    pub(crate) faucet_contract: Address,
    /// The channel between the RPC and the faucet.
    ///
    /// The channel contains:
    /// - the receiving user's address
    /// - an optional contract address (when requesting stablecoins)
    /// - the oneshot channel to return the result
    pub(crate) request_rx:
        UnboundedReceiverStream<(Address, Option<Address>, oneshot::Sender<EthResult<TxHash>>)>,
    /// Database impl for retrieving blockchain data.
    pub(crate) provider: Provider,
    /// The pool for submitting transactions.
    pub(crate) pool: Pool,
    /// The cache for verifying an address hasn't exceeded time-based request limit.
    ///
    /// The cache maps a user's address with the contract's address to the time of the request.
    ///
    /// Users request faucet transfers for native tokens (zero address) and
    /// stablecoin tokens (contract address) at specific times.
    pub(crate) lru_cache: LruCache<(Address, Address), SystemTime>,
    /// The chain id for constructing transactions.
    pub(crate) chain_id: u64,
    /// The amount of time the LRU cache retains an address (specified in `FaucetConfig`).
    pub(crate) wait_period: Duration,
    /// The type that can spawn tasks onto the runtime.
    pub(crate) executor: Tasks,
    /// The transaction signer's information.
    pub(crate) wallet: FaucetWallet,
    /// Sending half of the cache channel.
    ///
    /// The user's address and contract address are sent through this channel
    /// then added to the LRU cache.
    pub(crate) add_to_cache_tx: UnboundedSender<(Address, Address)>,
    /// Receiving half of the cache channel.
    ///
    /// Addresses received on this channel are added to the LRU cache.
    pub(crate) update_cache_rx: UnboundedReceiver<(Address, Address)>,
    /// The watch channel for tracking the worker's latest pending block.
    ///
    /// The pending block is updated right before transactions are removed from the tx pool.
    pub(crate) watch_rx: watch::Receiver<PendingWorkerBlock>,
}

impl<Provider, Pool, Tasks> FaucetService<Provider, Pool, Tasks>
where
    Provider: BlockReaderIdExt + StateProviderFactory + Unpin + Clone + 'static,
    Pool: TransactionPool + Unpin + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    /// Calculate when the wait period is over
    fn calc_wait_period(&self, last_transfer: &SystemTime) -> EthResult<Duration> {
        // calc when the address is removed from cache
        let end =
            last_transfer.checked_add(self.wait_period).ok_or(EthApiError::InternalEthError)?;

        // calc the remaining duration
        end.duration_since(SystemTime::now()).map_err(|e| EthApiError::InvalidParams(e.to_string()))
    }

    /// Process the transfer request for the faucet service.
    ///
    /// This method intentionally uses `&mut self` to ensure nonce is incremented correctly.
    fn process_transfer_request(
        &mut self,
        user: Address,
        contract: Address,
        reply: oneshot::Sender<EthResult<TxHash>>,
    ) -> EthResult<()> {
        // create transaction based on request type
        let transaction = self.create_transaction_to_sign(user, contract)?;
        // request signature from kms
        let kms_name = self.wallet.name();
        // let kms_client = self.kms_client();
        let public_key = self.wallet.kms_public_key();
        let chain_id = self.chain_id;
        let pool = self.pool.clone();
        let add_to_cache = self.add_to_cache_tx.clone();

        // request signature and submit to txpool
        self.executor.spawn(Box::pin(async move {
            let digest = transaction.signature_hash();
            let response =
                Self::request_kms_signature(kms_name, digest, chain_id, public_key).await;

            // submit tx to pool
            match response {
                Ok(signature) => {
                    let tx_for_pool =
                        TransactionSigned::from_transaction_and_signature(transaction, signature);
                    let res = submit_transaction(pool, tx_for_pool).await;
                    if res.is_ok() {
                        // forward address to update cache
                        let _ = add_to_cache.send((user, contract));
                    }
                    // reply to rpc
                    let _ = reply.send(res);
                }
                Err(e) => error!(target: "faucet", ?e, "Error requesting KMS signature"),
            }
        }));

        Ok(())
    }

    /// Create a EIP1559 transaction with max fee per gas set to 1 TEL.
    ///
    /// This method intentionally uses `&mut self` to ensure nonce is incremented correctly.
    /// TODO: use AtomicU64 for thread safe nonce increments
    fn create_transaction_to_sign(
        &mut self,
        to: Address,
        contract: Address,
    ) -> EthResult<Transaction> {
        // find the tx nonce and gas price
        let nonce = self.get_transaction_count()?;
        let gas_price = self.gas_price()?;

        // TNFaucet.sol will drip native TEL when called with RPC param `contract == address(0)`
        let transaction = {
            // hardcoded selector: keccak256("drip(address,address)")[0..4] == 0xeb3839a7
            let selector = [235, 56, 57, 167];
            // encode params
            let params: Vec<u8> = Drip::abi_encode_params(&(&contract, &to));
            // combine params with selector to create input for contract call
            let input = [&selector, &params[..]].concat().into();

            // stablecoin transaction - call faucet contract
            Transaction::Eip1559(TxEip1559 {
                chain_id: self.chain_id,
                nonce,
                max_priority_fee_per_gas: gas_price,
                max_fee_per_gas: gas_price,
                gas_limit: 1_000_000,
                to: TxKind::Call(self.faucet_contract),
                value: U256::ZERO,
                input,
                access_list: Default::default(),
            })
        };

        debug!(target: "faucet", ?transaction);
        Ok(transaction)
    }

    /// Taken from rpc/src/eth/api/state.rs
    fn get_transaction_count(&self) -> EthResult<u64> {
        let address = self.wallet.address;
        debug!(?address, "Faucet address");
        // lookup transactions in pool
        let address_txs = self.pool.get_transactions_by_sender(address);

        if !address_txs.is_empty() {
            // get max transaction with the highest nonce
            let highest_nonce_tx = address_txs
                .into_iter()
                .reduce(|accum, item| {
                    if item.transaction.nonce() > accum.transaction.nonce() {
                        item
                    } else {
                        accum
                    }
                })
                .ok_or(EthApiError::InvalidParams(
                    "Failed to reduce the highest nonce transaction in the pool".to_string(),
                ))?;

            let tx_count = highest_nonce_tx
                .transaction
                .nonce()
                .checked_add(1)
                .ok_or(RpcInvalidTransactionError::NonceMaxValue)?;
            return Ok(tx_count);
        }

        // lookup account nonce in db and compare it to pending worker block
        let state = self.provider.latest()?;
        let latest_nonce = state.account_nonce(address)?.unwrap_or_default();
        let pending_nonce = self.watch_rx.borrow().account_nonce(&address).unwrap_or_default();
        debug!(target: "faucet", ?latest_nonce, ?pending_nonce, "comparing faucet nonces");

        Ok(std::cmp::max(latest_nonce, pending_nonce))
    }

    /// Taken from rpc/src/eth/api/fees.rs
    ///
    /// Estimate gas price for legacy transactions
    fn gas_price(&self) -> EthResult<u128> {
        let header =
            self.provider.latest_header()?.ok_or_else(|| EthApiError::UnknownBlockNumber)?;
        let next_base_fee =
            header.next_block_base_fee(BaseFeeParams::ethereum()).unwrap_or_default();
        Ok(next_base_fee.into())
    }

    /// Send a request to Google KMS and convert it to EVM compatible.
    async fn request_kms_signature(
        name: String,
        digest: B256,
        chain_id: u64,
        public_key_bytes: Secp256k1PubKeyBytes,
    ) -> eyre::Result<EthSignature> {
        // create client
        //
        // note: this is reusable, but challenging to figure out how
        // to call the .await from inside sync function (spawn, create, etc.)
        let client: GoogleKMSClient = GoogleApi::from_function(
            KeyManagementServiceClient::new,
            "https://cloudkms.googleapis.com",
            None,
        )
        .await?;

        // create message from slice before consuming digest
        // this is needed to calculate `v` below
        let message = Message::from_digest_slice(&digest.0)?;

        // assemble digest for signature
        let digest = Some(Digest::Sha256(digest.0.to_vec()));
        let digest = Some(KMSDigest { digest });
        let signed_data = client
            .get()
            .asymmetric_sign(AsymmetricSignRequest {
                name: name.clone(),
                digest,
                ..Default::default()
            })
            .await?
            .into_inner()
            .signature;

        // ensure signature is compatible with ethereum (see EIP-155)
        let mut signature = Signature::from_der(&signed_data)?;
        signature.normalize_s();
        // retrieve r, s, and v values for EthSignature
        let compact = signature.serialize_compact();

        // calculate `v` for eth signature's `odd_y_parity`
        let odd_y_parity = Self::calculate_v(&message, chain_id, &compact, &public_key_bytes)?;

        // r and s are 32 bytes each
        let (r, s) = compact.split_at(32);

        let eth_signature =
            EthSignature { r: U256::from_be_slice(r), s: U256::from_be_slice(s), odd_y_parity };

        Ok(eth_signature)
    }

    /// Try both recovery ids (0 or 1) to find the correct v value
    ///
    /// NOTE: this compares the compressed public keys for convenience
    ///       uncompressed approach is commented out
    ///
    /// How and why this works:
    /// Calculating the v value from r, s, the original hash, and the public key, especially for
    /// use in Ethereum transactions, involves trying to recover the public key from the
    /// signature and comparing it to the known public key to determine the correct v value.
    /// Ethereum uses the v value to encode the recovery id and some blockchain-specific
    /// information (like chain id in EIP-155). In Ethereum, v can typically be 27 or 28
    /// (or higher if adjusted for chain id as per EIP-155), corresponding to the two possible
    /// recovery ids (0 or 1) that can result from the ECDSA signature recovery process.
    /// In Ethereum, a signature consists of three components: r, s, and v. The r and s values
    /// are part of the ECDSA signature, and the v value is a recovery id that indicates which of
    /// the two possible public keys is the correct one (since a signature does not uniquely
    /// identify a public key).
    ///
    /// The signature from Google Cloud KMS using the secp256k1 curve is in DER format.
    /// This 64-byte format consists of the r and s components of the ECDSA signature, each being
    /// 32 bytes long.
    ///
    /// The `v` value is Ethereum-specific and must be calculated to use the KMS signature with
    /// EVM. The `v` value can be either 27 or 28 (or 35 or 36 when adding the chain ID to
    /// prevent replay attacks on different networks as per EIP-155).
    ///
    /// The `v` value in the signature not only indicates the chain ID (for replay
    /// protection) but also encodes information about the "y parity" of the point on the
    /// elliptic curve that corresponds to the public key recovered from the signature. The y
    /// parity (odd or even) is a critical component used to recover the correct public key from
    /// a given signature (r, s) and message hash.
    ///
    /// Relationship Between y_odd_parity and v:
    /// The v value for Ethereum signatures traditionally starts at 27 (or 28), where the
    /// difference (27 or 28) essentially encodes the y parity.
    ///
    /// Specifically:
    /// - If v = 35 + 2*chain_id), the y parity is even.
    /// - If v = 36 + 2*chain_id), the y parity is odd.
    ///
    /// The y_odd_parity is false if v is 35 (even y) and true if v is 36 (odd y).
    fn calculate_v(
        message: &Message,
        chain_id: u64,
        compact_signature: &[u8; 64],
        public_key_bytes: &Secp256k1PubKeyBytes, // [u8; 33]
    ) -> EthResult<bool> {
        // recovery id must be 0 or 1
        for recovery_id in [0, 1] {
            let recid = RecoveryId::from_i32(recovery_id).expect("Invalid recovery id");
            let recoverable_signature =
                RecoverableSignature::from_compact(compact_signature, recid).map_err(|e| {
                    EthApiError::InvalidParams(format!("failed to recover signature: {}", e))
                })?;
            if let Ok(recovered_key) = SECP256K1.recover_ecdsa(message, &recoverable_signature) {
                let recovered_pubkey = recovered_key.serialize();
                if recovered_pubkey == public_key_bytes.as_ref() {
                    // v is found when the recovered key matches the known public key
                    //
                    // calculate v based on EIP-155
                    let v = recovery_id as u64 + chain_id * 2 + 35;
                    let y_odd_parity = v % 2 == 0;
                    return Ok(y_odd_parity);
                }
            }
        }

        // TODO: better error type here
        Err(EthApiError::FailedToDecodeSignedTransaction)
    }
}

impl<Provider, Pool, Tasks> Future for FaucetService<Provider, Pool, Tasks>
where
    Provider: BlockReaderIdExt + StateProviderFactory + Unpin + Clone + 'static,
    Pool: TransactionPool + Unpin + Clone + 'static,
    Tasks: TaskSpawner + Clone + 'static,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            // listen for cache updates
            while let Poll::Ready(Some((address, contract))) = this.update_cache_rx.poll_recv(cx) {
                // insert user's address and contract address into LRU cache
                this.lru_cache.insert((address, contract), SystemTime::now());
            }

            match ready!(this.request_rx.poll_next_unpin(cx)) {
                None => {
                    unreachable!("faucet request_rx can't close - always listening for addresses from the rpc")
                }
                Some((address, contract, reply)) => {
                    // assign token address for checking LRU cache
                    let contract_address = if let Some(address) = contract {
                        // stablecoin transfer
                        address
                    } else {
                        // native token transfer
                        Address::ZERO
                    };

                    // check the cache for user's address
                    // use `::peek` so cache timer doesn't reset
                    if let Some(time) = this.lru_cache.peek(&(address, contract_address)) {
                        // return remaining time if address combo is still cached
                        let wait_period_over = this.calc_wait_period(time);
                        let error = match wait_period_over {
                            Ok(time) => {
                                // trim off ms, us, and ns
                                let human_readable =
                                    format_duration(Duration::new(time.as_secs(), 0));
                                let msg = format!("Wait period over at: {}", human_readable);
                                Err(EthApiError::InvalidParams(msg))
                            }
                            Err(e) => Err(e),
                        };

                        // return the error and check the next request
                        let _ = reply.send(error);
                        continue;
                    }

                    // user's request not in cache - process request
                    if let Err(e) = this.process_transfer_request(address, contract_address, reply)
                    {
                        error!(target: "faucet", ?e, "Error creating faucet transaction")
                    }
                }
            }
        }
    }
}

async fn submit_transaction<Pool>(pool: Pool, tx: TransactionSigned) -> EthResult<TxHash>
where
    Pool: TransactionPool + Clone + 'static,
{
    let recovered = tx.try_into_ecrecovered().or(Err(EthApiError::InvalidTransactionSignature))?;

    let pool_transaction = match recovered.try_into() {
        Ok(converted) => <Pool::Transaction>::from_pooled(converted),
        Err(_) => return Err(EthApiError::TransactionConversionError),
    };

    let hash = pool.add_transaction(TransactionOrigin::Local, pool_transaction).await?;
    Ok(hash)
}
