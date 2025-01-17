//! Block validator

use reth_db::database::Database;
use reth_primitives::Header;
use reth_provider::{providers::BlockchainProvider, BlockIdReader, HeaderProvider};
use tn_types::{
    max_worker_block_gas, max_worker_block_size, SealedWorkerBlock, TransactionSigned,
    WorkerBlockValidation, WorkerBlockValidationError,
};

/// Type convenience for implementing block validation errors.
type BlockValidationResult<T> = Result<T, WorkerBlockValidationError>;

/// Block validator
#[derive(Clone)]
pub struct BlockValidator<DB>
where
    DB: Database + Clone + 'static,
{
    /// Database provider to encompass tree and provider factory.
    blockchain_db: BlockchainProvider<DB>,
}

#[async_trait::async_trait]
impl<DB> WorkerBlockValidation for BlockValidator<DB>
where
    DB: Database + Sized + Clone + 'static,
{
    /// Validate a peer's worker block.
    ///
    /// Workers do not execute full blocks. This method validates the required information.
    fn validate_block(&self, sealed_block: SealedWorkerBlock) -> BlockValidationResult<()> {
        // ensure digest matches worker block
        let (block, digest) = sealed_block.split();
        let verified_hash = block.clone().seal_slow().digest();
        if digest != verified_hash {
            return Err(WorkerBlockValidationError::InvalidDigest);
        }

        // TODO: validate individual transactions against parent

        // obtain info for validation
        let transactions = block.transactions();

        // first step towards validating parent's header
        // Note this is really a "best effort" check.  If we have not
        // executed parent_hash yet then it will use the last executed block if
        // available.  Making it manditory would require waiting to see
        // if we execute it soon to avoid false failures.
        // The primary header should get checked so this should be ok.
        let parent =
            self.blockchain_db.header(&block.parent_hash).unwrap_or_default().unwrap_or_else(
                || {
                    let finalized_block_num_hash =
                        self.blockchain_db.finalized_block_num_hash().unwrap_or_default();
                    if let Some(finalized_block_num_hash) = finalized_block_num_hash {
                        self.blockchain_db
                            .header(&finalized_block_num_hash.hash)
                            .unwrap_or_default()
                            .unwrap_or_default()
                    } else {
                        Header::default()
                    }
                },
            );

        // validate timestamp vs parent
        self.validate_against_parent_timestamp(block.timestamp, &parent)?;

        // validate gas limit
        self.validate_block_gas(block.total_possible_gas(), block.timestamp)?;

        // validate block size (bytes)
        self.validate_block_size_bytes(transactions, block.timestamp)?;

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
    pub fn new(blockchain_db: BlockchainProvider<DB>) -> Self {
        Self { blockchain_db }
    }

    /// Validates the timestamp against the parent to make sure it is in the past.
    #[inline]
    fn validate_against_parent_timestamp(
        &self,
        timestamp: u64,
        parent: &Header,
    ) -> BlockValidationResult<()> {
        if timestamp <= parent.timestamp {
            return Err(WorkerBlockValidationError::TimestampIsInPast {
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
    fn validate_block_gas(
        &self,
        total_possible_gas: u64,
        timestamp: u64,
    ) -> BlockValidationResult<()> {
        // ensure total tx gas limit fits into block's gas limit
        let max_tx_gas = max_worker_block_gas(timestamp);
        if total_possible_gas > max_tx_gas {
            return Err(WorkerBlockValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas,
                gas_limit: max_tx_gas,
            });
        }
        Ok(())
    }

    /// Validate the size of transactions (in bytes).
    fn validate_block_size_bytes(
        &self,
        transactions: &[TransactionSigned],
        timestamp: u64,
    ) -> BlockValidationResult<()> {
        // calculate size (in bytes) of included transactions
        let total_bytes = transactions
            .iter()
            .map(|tx| tx.size())
            .reduce(|total, size| total + size)
            .ok_or(WorkerBlockValidationError::CalculateTransactionByteSize)?;
        let max_tx_bytes = max_worker_block_size(timestamp);

        // allow txs that equal max tx bytes
        if total_bytes > max_tx_bytes {
            return Err(WorkerBlockValidationError::HeaderTransactionBytesExceedsMax(total_bytes));
        }

        Ok(())
    }

    /// TODO: Validate the block's basefee
    fn validate_basefee(&self) -> BlockValidationResult<()> {
        // TODO: validate basefee by consensus round
        Ok(())
    }
}

/// Noop validation struct that validates any block.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default, Clone)]
pub struct NoopBlockValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl WorkerBlockValidation for NoopBlockValidator {
    fn validate_block(&self, _block: SealedWorkerBlock) -> Result<(), WorkerBlockValidationError> {
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
        constants::MIN_PROTOCOL_BASE_FEE, hex, Address, Bytes, GenesisAccount, B256, U256,
    };
    use reth_provider::{providers::StaticFileProvider, ProviderFactory};
    use reth_prune::PruneModes;
    use std::{str::FromStr, sync::Arc};
    use tn_test_utils::{test_genesis, TransactionFactory};
    use tn_types::{adiri_genesis, max_worker_block_gas, Consensus, WorkerBlock};
    use tracing::debug;

    /// Return the next valid sealed worker block
    fn next_valid_sealed_worker_block() -> SealedWorkerBlock {
        let timestamp = 1701790139;
        // create valid transactions
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<ChainSpec> = Arc::new(test_genesis().into());

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559(
            chain,
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let valid_txs = vec![transaction1, transaction2, transaction3];

        // sealed worker block
        //
        // intentionally used hard-coded values
        SealedWorkerBlock::new(
            WorkerBlock {
                transactions: valid_txs,
                parent_hash: hex!(
                    "a0673579c1a31037ee29a7e3cb7b1495a020bf21d958269ea8291a64326667c5"
                )
                .into(),
                beneficiary: Address::ZERO,
                timestamp,
                base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
                received_at: None,
            },
            hex!("3db9ad782b190874867d04f3c26a88546a06be445c9544697499659944210593").into(),
        )
    }

    /// Convenience type for creating test assets.
    struct TestTools {
        /// The expected sealed worker block.
        valid_block: SealedWorkerBlock,
        /// Validator
        validator: BlockValidator<Arc<TempDatabase<DatabaseEnv>>>,
    }

    /// Create an instance of block validator for tests.
    async fn test_tools() -> TestTools {
        // genesis with default TransactionFactory funded
        let chain: Arc<ChainSpec> = Arc::new(test_genesis().into());

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

        let validator = BlockValidator::new(blockchain_db);
        let valid_block = next_valid_sealed_worker_block();

        // block validator
        TestTools { valid_block, validator }
    }

    #[tokio::test]
    async fn test_valid_block() {
        let TestTools { valid_block, validator } = test_tools().await;
        let result = validator.validate_block(valid_block.clone());
        assert!(result.is_ok());

        // ensure non-serialized data does not affect validity
        let (mut worker_block, _) = valid_block.split();
        worker_block.received_at = Some(tn_types::now());
        let different_block = worker_block.seal_slow();
        let result = validator.validate_block(different_block);
        assert!(result.is_ok());
    }

    //#[tokio::test]
    // This is not checked currently, leaving test for bit to make sure we want this.
    // This check will lead to occasional false errors and should not be critical since
    // we should be validating parentage when building actual blocks (including any
    // needed waits for execution).
    async fn _test_invalid_block_wrong_parent_hash() {
        let TestTools { valid_block, validator } = test_tools().await;
        let (worker_block, _) = valid_block.split();
        let WorkerBlock {
            transactions, beneficiary, timestamp, base_fee_per_gas, received_at, ..
        } = worker_block;
        let wrong_parent_hash = B256::random();
        let invalid_block = WorkerBlock {
            transactions,
            parent_hash: wrong_parent_hash,
            beneficiary,
            timestamp,
            base_fee_per_gas,
            received_at,
        };
        assert_matches!(
            validator.validate_block(invalid_block.seal_slow()),
            Err(WorkerBlockValidationError::CanonicalChain { block_hash }) if block_hash == wrong_parent_hash
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_timestamp() {
        let TestTools { valid_block, validator } = test_tools().await;
        let (mut worker_block, _) = valid_block.split();

        // test worker_block timestamp same as parent
        let wrong_timestamp = adiri_genesis().timestamp;
        worker_block.timestamp = wrong_timestamp;

        assert_matches!(
            validator.validate_block(worker_block.clone().seal_slow()),
            Err(WorkerBlockValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp
        );

        // test header timestamp before parent
        worker_block.timestamp = wrong_timestamp - 1;

        assert_matches!(
            validator.validate_block(worker_block.seal_slow()),
            Err(WorkerBlockValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp - 1
        );
    }

    #[tokio::test]
    async fn test_invalid_block_excess_gas_used() {
        // Set excessive gas limit.
        let TestTools { valid_block, validator } = test_tools().await;
        let (worker_block, _) = valid_block.split();

        // sign excessive transaction
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<ChainSpec> = Arc::new(test_genesis().into());

        // create transaction with max gas limit above the max allowed
        let invalid_transaction = tx_factory.create_eip1559(
            chain.clone(),
            Some(max_worker_block_gas(worker_block.timestamp) + 1),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let WorkerBlock {
            beneficiary, timestamp, base_fee_per_gas, received_at, parent_hash, ..
        } = worker_block;
        let invalid_block = WorkerBlock {
            transactions: vec![invalid_transaction],
            parent_hash,
            beneficiary,
            timestamp,
            base_fee_per_gas,
            received_at,
        };

        assert_matches!(
            validator
                .validate_block_gas(invalid_block.total_possible_gas(), invalid_block.timestamp),
            Err(WorkerBlockValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas: _,
                gas_limit: _
            })
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_size_in_bytes() {
        let TestTools { valid_block, validator } = test_tools().await;
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
        while total_bytes < max_worker_block_size(0) {
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
            total_bytes += tx.size();
            too_many_txs.push(tx);
        }

        // NOTE: these assertions aren't important but want to know if tx size changes
        assert_eq!(total_bytes, 1_000_035);
        assert_eq!(too_many_txs.len(), 4695);

        // update header so tx root is correct
        let (mut block, _hash) = valid_block.split();
        block.transactions = too_many_txs;
        let invalid_block = block.clone().seal_slow();

        assert_matches!(
            validator.validate_block(invalid_block),
            Err(WorkerBlockValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == total_bytes
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

        let invalid_txs = vec![giant_tx];
        block.transactions = invalid_txs;
        let invalid_block = block.seal_slow();
        assert_matches!(
            validator.validate_block(invalid_block),
            Err(WorkerBlockValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == too_big
        );
    }

    // // TODO:
    // // - basefee
}
