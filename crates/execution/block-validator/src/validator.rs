//! Block validator

use crate::error::BlockValidationError;
use reth_db::database::Database;
use reth_primitives::Header;
use reth_provider::{providers::BlockchainProvider, HeaderProvider};
use std::fmt::{Debug, Display};
use tn_types::{TransactionSigned, WorkerBlock};

/// Type convenience for implementing block validation errors.
type BlockValidationResult<T> = Result<T, BlockValidationError>;

/// Block validator
#[derive(Clone)]
pub struct BlockValidator<DB>
where
    DB: Database + Clone + 'static,
{
    /// Database provider to encompass tree and provider factory.
    blockchain_db: BlockchainProvider<DB>,
    /// The maximum size (in bytes) for a peer's list of transactions.
    ///
    /// The peer-proposed block's transaction list must not exceed this value.
    max_tx_bytes: usize,
    /// The maximum size (in gas) for a peer's list of transactions.
    ///
    /// The peer-proposed block's transaction list must not exceed this value.
    max_tx_gas: u64,
}

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a block of transactions (from another validator).
///
/// Invalid transactions will not receive further processing.
#[async_trait::async_trait]
pub trait BlockValidation: Clone + Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync + 'static;
    /// Determines if this block can be voted on
    async fn validate_block(&self, b: &WorkerBlock) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
impl<DB> BlockValidation for BlockValidator<DB>
where
    DB: Database + Sized + Clone + 'static,
{
    /// Error type for block validation
    type Error = BlockValidationError;

    /// Validate a peer's worker block.
    ///
    /// Workers do not execute full blocks. This method validates the required information.
    async fn validate_block(&self, block: &WorkerBlock) -> BlockValidationResult<()> {
        // TODO: validate individual transactions against parent

        // obtain info for validation
        let transactions = block.transactions();

        // retrieve parent header from provider
        //
        // first step towards validating parent's header
        let parent = self
            .blockchain_db
            .header(&block.parent_hash)?
            .ok_or(BlockValidationError::CanonicalChain { block_hash: block.parent_hash })?
            .seal(block.parent_hash);

        // validate timestamp vs parent
        self.validate_against_parent_timestamp(block.timestamp, parent.header())?;

        // validate gas limit
        self.validate_block_gas(block.total_possible_gas())?;

        // validate block size (bytes)
        self.validate_block_size_bytes(transactions)?;

        // validate beneficiary?
        // no - tips would go to someone else

        // TODO: validate basefee doesn't actually do anything yet
        self.validate_basefee()?;
        Ok(())
    }
}

impl<DB> BlockValidator<DB>
where
    DB: Database + Clone,
{
    /// Create a new instance of [Self]
    pub fn new(
        blockchain_db: BlockchainProvider<DB>,
        max_tx_bytes: usize,
        max_tx_gas: u64,
    ) -> Self {
        Self { blockchain_db, max_tx_bytes, max_tx_gas }
    }

    /// Validates the timestamp against the parent to make sure it is in the past.
    #[inline]
    fn validate_against_parent_timestamp(
        &self,
        timestamp: u64,
        parent: &Header,
    ) -> BlockValidationResult<()> {
        if timestamp <= parent.timestamp {
            return Err(BlockValidationError::TimestampIsInPast {
                parent_timestamp: parent.timestamp,
                timestamp,
            });
        }
        Ok(())
    }

    /// Possible gas used needs to be less than block's gas limit.
    ///
    /// Actual amount of gas used cannot be determined until execution.
    #[inline]
    fn validate_block_gas(&self, total_possible_gas: u64) -> BlockValidationResult<()> {
        // ensure total tx gas limit fits into block's gas limit
        if total_possible_gas > self.max_tx_gas {
            return Err(BlockValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas,
                gas_limit: self.max_tx_gas,
            });
        }
        Ok(())
    }

    /// Validate the size of transactions (in bytes).
    fn validate_block_size_bytes(
        &self,
        transactions: &[TransactionSigned],
    ) -> BlockValidationResult<()> {
        // calculate size (in bytes) of included transactions
        let total_bytes = transactions
            .iter()
            .map(|tx| tx.size())
            .reduce(|total, size| total + size)
            .ok_or(BlockValidationError::CalculateTransactionByteSize)?;

        // allow txs that equal max tx bytes
        if total_bytes > self.max_tx_bytes {
            return Err(BlockValidationError::HeaderTransactionBytesExceedsMax(total_bytes));
        }

        Ok(())
    }

    /// TODO: Validate the block's basefee
    fn validate_basefee(&self) -> BlockValidationResult<()> {
        // TODO: validate basefee by consensus round
        Ok(())
    }
}

#[cfg(any(test, feature = "test-utils"))]
/// Noop validation struct that validates any block.
#[derive(Default, Clone)]
pub struct NoopBlockValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl BlockValidation for NoopBlockValidator {
    type Error = BlockValidationError;

    async fn validate_block(&self, _block: &WorkerBlock) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use reth_beacon_consensus::EthBeaconConsensus;
    use reth_blockchain_tree::{
        BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
    };
    use reth_chainspec::ChainSpec;
    use reth_db::{
        test_utils::{create_test_rw_db, tempdir_path, TempDatabase},
        DatabaseEnv,
    };
    use reth_db_common::init::init_genesis;
    use reth_primitives::{
        constants::EMPTY_WITHDRAWALS, hex, proofs, Address, Bloom, Bytes, GenesisAccount, Header,
        SealedHeader, B256, EMPTY_OMMER_ROOT_HASH, U256,
    };
    use reth_provider::{providers::StaticFileProvider, ProviderFactory};
    use reth_prune::PruneModes;
    use std::{str::FromStr, sync::Arc};
    use tn_test_utils::TransactionFactory;
    use tn_types::{adiri_genesis, max_worker_block_gas, Consensus};
    use tracing::debug;

    /// Return the next valid block
    fn next_valid_sealed_header() -> SealedHeader {
        let timestamp = 1701790139;
        // sealed header
        //
        // intentionally used hard-coded values
        SealedHeader::new(
            Header {
                parent_hash: hex!(
                    "bf0f2065b35a695aa0d47e9633d6cc78f6e012b988f774ff7e4c8467ea7f4126"
                )
                .into(),
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                beneficiary: hex!("0000000000000000000000000000000000000000").into(),
                state_root: B256::ZERO,
                transactions_root: hex!(
                    "3facac570ec391ef164bce1757035e1a8f03d5731640879b17b7da24a027c718"
                )
                .into(),
                receipts_root: B256::ZERO,
                withdrawals_root: Some(EMPTY_WITHDRAWALS),
                logs_bloom: Bloom::default(),
                difficulty: U256::ZERO,
                number: 1,
                gas_limit: max_worker_block_gas(timestamp),
                gas_used: 3_000_000, /* TxFactory sets limit to 1_000_000 * 3txs
                                      * timestamp: 1701790139, */
                timestamp,
                mix_hash: B256::ZERO,
                nonce: 0,
                base_fee_per_gas: Some(7),
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_block_root: None,
                extra_data: Bytes::default(),
                requests_root: None,
            },
            hex!("abc832c52b74957b7ef596e35022068ba9a8ab222ed4dbbe42b3eb07d17a42ef").into(),
        )
    }

    /// Convenience type for creating test assets.
    struct TestTools {
        /// The expected transactions for the valid sealed header.
        valid_txs: Vec<TransactionSigned>,
        /// The expected sealed header.
        valid_header: SealedHeader,
        /// Validator
        validator: BlockValidator<Arc<TempDatabase<DatabaseEnv>>>,
    }

    /// Create an instance of block validator for tests.
    async fn test_types() -> TestTools {
        test_types_int(1_000_000, max_worker_block_gas(0)).await
    }

    /// Create an instance of block validator for tests.
    async fn test_types_int(max_tx_bytes: usize, max_tx_gas: u64) -> TestTools {
        // reth_tracing::init_test_tracing();
        let genesis = adiri_genesis();
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();
        debug!("seeding factory address: {factory_address:?}");

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        debug!("seeded genesis: {genesis:?}");
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 3: {transaction3:?}");

        let valid_txs = vec![transaction1, transaction2, transaction3];

        // init genesis
        let db = create_test_rw_db();
        let provider_factory = ProviderFactory::new(
            Arc::clone(&db),
            Arc::clone(&chain),
            StaticFileProvider::read_write(tempdir_path())
                .expect("static file provider read write created with tempdir path"),
        );
        let genesis_hash = init_genesis(provider_factory.clone()).expect("init genesis");
        debug!("genesis hash: {genesis_hash:?}");

        // configure blockchain tree
        let consensus: Arc<dyn Consensus> = Arc::new(EthBeaconConsensus::new(chain.clone()));

        let tree_externals = TreeExternals::new(
            provider_factory.clone(),
            Arc::clone(&consensus),
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );
        let tree_config = BlockchainTreeConfig::default();
        let tree = BlockchainTree::new(tree_externals, tree_config, PruneModes::none())
            .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        let validator = BlockValidator::new(blockchain_db, max_tx_bytes, max_tx_gas);
        let valid_header = next_valid_sealed_header();

        // block validator
        TestTools { valid_txs, valid_header, validator }
    }

    #[tokio::test]
    async fn test_valid_block() {
        let TestTools { valid_txs, valid_header, validator } = test_types().await;
        let valid_block = WorkerBlock::new_for_test(valid_txs, valid_header);
        let result = validator.validate_block(&valid_block).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_parent_hash() {
        let TestTools { valid_txs, mut valid_header, validator } = test_types().await;
        let wrong_parent_hash = B256::random();
        valid_header.set_parent_hash(wrong_parent_hash);
        // update hash since this is asserted first
        let wrong_header = valid_header.unseal().seal_slow();
        let wrong_block = WorkerBlock::new_for_test(valid_txs, wrong_header);
        assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::CanonicalChain { block_hash }) if block_hash == wrong_parent_hash
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_timestamp() {
        let TestTools { valid_txs, valid_header, validator } = test_types().await;
        let (mut header, _hash) = valid_header.split();

        // test header timestamp same as parent
        let wrong_timestamp = adiri_genesis().timestamp;
        header.timestamp = wrong_timestamp;

        // update hash since this is asserted first
        let wrong_header = header.clone().seal_slow();
        let wrong_block = WorkerBlock::new_for_test(valid_txs.clone(), wrong_header);
        assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp
        );

        // test header timestamp before parent
        header.timestamp = wrong_timestamp - 1;

        // update hash since this is asserted first
        let wrong_header = header.seal_slow();
        let wrong_block = WorkerBlock::new_for_test(valid_txs, wrong_header);
        assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp - 1
        );
    }

    #[tokio::test]
    async fn test_invalid_block_excess_gas_used() {
        // Set super low gas limit.
        let TestTools { valid_txs, valid_header, validator } = test_types_int(1_000_000, 10).await;

        let wrong_block = WorkerBlock::new_for_test(valid_txs, valid_header);
        assert_matches!(
            validator.validate_block_gas(wrong_block.total_possible_gas()),
            Err(BlockValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas: _,
                gas_limit: _
            })
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_size_in_bytes() {
        let TestTools { valid_header, validator, .. } = test_types().await;
        // create enough transactions to exceed 1MB
        // TODO: clean this up - taken from `test_types` fn
        // because validator uses provided with same genesis
        // and tx_factory needs funds
        let genesis = adiri_genesis();

        // use new tx factory to ensure correct nonces are tracked
        let mut tx_factory = TransactionFactory::new();
        let factory_address = tx_factory.address();

        // fund factory with 99mil TEL
        let account = vec![(
            factory_address,
            GenesisAccount::default().with_balance(
                U256::from_str("0x51E410C0F93FE543000000").expect("account balance is parsed"),
            ),
        )];

        let genesis = genesis.extend_accounts(account);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        // currently: 4695 txs at 1000035 bytes
        let mut too_many_txs = Vec::new();
        let mut total_bytes = 0;
        let mut total_gas = 0;
        while total_bytes < 1_000_000 {
            let tx = tx_factory.create_explicit_eip1559(
                Some(chain.chain.id()),
                None,                    // default nonce
                None,                    // no tip
                Some(7),                 // min basefee for block 1
                Some(1),                 // low gas limit to prevent excess gas used error
                Some(Address::random()), // send to random address
                Some(U256::from(100)),   // send low amount
                None,                    // no input
                None,                    // no access list
            );

            // track totals
            total_gas += tx.gas_limit();
            total_bytes += tx.size();
            too_many_txs.push(tx);
        }

        // update header so tx root is correct
        let (mut header, _hash) = valid_header.split();
        header.gas_used = total_gas;
        header.transactions_root = proofs::calculate_transaction_root(&too_many_txs);
        let bad_header = header.clone().seal_slow();

        // NOTE: these assertions aren't important but want to know if tx size changes
        assert_eq!(total_bytes, 1_000_035);
        assert_eq!(too_many_txs.len(), 4695);

        let wrong_block = WorkerBlock::new_for_test(too_many_txs, bad_header);
        assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == total_bytes
        );

        // Generate 1MB vec of 1s - total bytes are: 1_000_213
        let big_input = vec![1u8; 1_000_000];

        // create giant tx
        let max_gas = max_worker_block_gas(0);
        let giant_tx = tx_factory.create_explicit_eip1559(
            Some(chain.chain.id()),
            Some(0),                      // make this first tx in block 1
            None,                         // no tip
            Some(7),                      // min basefee for block 1
            Some(max_gas),                // high gas limit bc this is a lot of data
            None,                         // create tx
            Some(U256::ZERO),             // no transfer
            Some(Bytes::from(big_input)), // no input
            None,                         // no access list
        );

        // NOTE: the actual size just needs to be above 1MB but want to know if tx size ever changes
        let too_big = giant_tx.size();
        assert_eq!(too_big, 1_000_213);

        let txs = vec![giant_tx];
        header.gas_used = max_gas;

        // use expected value to reduce test time
        // to recalculate: proofs::calculate_transaction_root(&txs)
        let tx_root =
            hex!("00e105ab5023ebb359f3770834cad7cc32e1495c79a9cdd803ed2c0eba7cb385").into();
        header.transactions_root = tx_root;
        let bad_header = header.seal_slow();
        let wrong_block = WorkerBlock::new_for_test(txs, bad_header);
        assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == too_big
        );
    }

    // // TODO:
    // // - basefee
}
