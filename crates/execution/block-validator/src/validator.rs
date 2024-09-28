//! Block validator

use crate::error::BlockValidationError;
use reth_db::database::Database;
use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, proofs, Bloom, Header, SealedHeader, B256, U256,
};
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
        let sealed_header = block.sealed_header();

        // retrieve parent header from provider
        //
        // first step towards validating parent's header
        let parent = self
            .blockchain_db
            .header(&sealed_header.parent_hash)?
            .ok_or(BlockValidationError::CanonicalChain { block_hash: sealed_header.parent_hash })?
            .seal(sealed_header.parent_hash);

        // validate sealed header digest
        self.validate_block_hash(sealed_header)?;

        // validate transactions root
        self.validate_transactions_root(transactions, sealed_header)?;

        // validate parent hash/parent number
        //
        // this validates the parent's hash by extension
        self.validate_against_parent_hash_number(sealed_header.header(), &parent)?;

        // validate timestamp vs parent
        self.validate_against_parent_timestamp(sealed_header.header(), parent.header())?;

        // validate gas limit
        self.validate_block_gas(sealed_header.header(), transactions)?;

        // validate block size (bytes)
        self.validate_block_size_bytes(transactions)?;

        // validate beneficiary?
        // no - tips would go to someone else

        // TODO: validate basefee doesn't actually do anything yet
        self.validate_basefee()?;

        // check empty roots to ensure malicious actor can't attack storage usage
        //
        // NOTE: does not validate extra_data
        self.validate_empty_values(sealed_header.header())
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

    /// Validate header's hash.
    #[inline]
    fn validate_block_hash(&self, header: &SealedHeader) -> BlockValidationResult<()> {
        let expected = header.header().hash_slow();
        let peer_hash = header.hash();
        if expected != peer_hash {
            return Err(BlockValidationError::BlockHash { expected, peer_hash });
        }
        Ok(())
    }

    /// Validate transaction root.
    #[inline]
    fn validate_transactions_root(
        &self,
        transactions: &Vec<TransactionSigned>,
        header: &SealedHeader,
    ) -> BlockValidationResult<()> {
        let expected = proofs::calculate_transaction_root(transactions);
        let peer_root = header.transactions_root;
        if expected != peer_root {
            return Err(BlockValidationError::TransactionRootMismatch { expected, peer_root });
        }
        Ok(())
    }

    /// Validate against parent hash number.
    #[inline]
    fn validate_against_parent_hash_number(
        &self,
        header: &Header,
        parent: &SealedHeader,
    ) -> BlockValidationResult<()> {
        // NOTE: parent hash is used to find the parent block.
        // if the parent block is found by its hash and the number matches,
        // then by extension, the parent's hash is verified
        //
        // ensure parent number is consistent.
        if parent.number + 1 != header.number {
            return Err(BlockValidationError::ParentBlockNumberMismatch {
                parent_block_number: parent.number,
                block_number: header.number,
            });
        }
        Ok(())
    }

    /// Validates the timestamp against the parent to make sure it is in the past.
    #[inline]
    fn validate_against_parent_timestamp(
        &self,
        header: &Header,
        parent: &Header,
    ) -> BlockValidationResult<()> {
        if header.is_timestamp_in_past(parent.timestamp) {
            return Err(BlockValidationError::TimestampIsInPast {
                parent_timestamp: parent.timestamp,
                timestamp: header.timestamp,
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
        header: &Header,
        transactions: &Vec<TransactionSigned>,
    ) -> BlockValidationResult<()> {
        // gas limit should be consistent amongst workers
        if header.gas_limit != self.max_tx_gas {
            return Err(BlockValidationError::InvalidGasLimit {
                expected: self.max_tx_gas,
                received: header.gas_limit,
            });
        }

        // ensure total tx gas limit fits into block's gas limit
        if header.gas_used >= header.gas_limit {
            return Err(BlockValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas: header.gas_used,
                gas_limit: header.gas_limit,
            });
        }

        // ensure accumulated max gas is correct
        let max_possible_gas = transactions
            .iter()
            .map(|tx| tx.gas_limit())
            .reduce(|total, gas| total + gas)
            .ok_or(BlockValidationError::CalculateMaxPossibleGas)?;

        if header.gas_used != max_possible_gas {
            return Err(BlockValidationError::HeaderGasUsedMismatch {
                expected: max_possible_gas,
                received: header.gas_used,
            });
        }
        Ok(())
    }

    /// Validate the size of transactions (in bytes).
    fn validate_block_size_bytes(
        &self,
        transactions: &Vec<TransactionSigned>,
    ) -> BlockValidationResult<()> {
        // calculate size (in bytes) of included transactions
        let total_bytes = transactions
            .iter()
            .map(|tx| tx.size())
            .reduce(|total, size| total + size)
            .ok_or(BlockValidationError::CalculateTransactionByteSize)?;

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

    /// Validate expected empty values for the header.
    ///
    /// This is important to prevent a storage attack where malicious actor proposes lots of extra
    /// data. NOTE: extra data is ignored
    fn validate_empty_values(&self, header: &Header) -> BlockValidationResult<()> {
        // ommers hash
        if !header.ommers_hash_is_empty() {
            return Err(BlockValidationError::NonEmptyOmmersHash(header.ommers_hash));
        }

        // state root
        if header.state_root != B256::ZERO {
            return Err(BlockValidationError::NonEmptyStateRoot(header.state_root));
        }

        // receipts root
        if header.receipts_root != B256::ZERO {
            return Err(BlockValidationError::NonEmptyReceiptsRoot(header.receipts_root));
        }

        // withdrawals root
        if header.withdrawals_root != Some(EMPTY_WITHDRAWALS) {
            return Err(BlockValidationError::NonEmptyWithdrawalsRoot(header.withdrawals_root));
        }

        // logs bloom
        if header.logs_bloom != Bloom::default() {
            return Err(BlockValidationError::NonEmptyLogsBloom(header.logs_bloom));
        }

        // mix hash
        if header.mix_hash != B256::ZERO {
            return Err(BlockValidationError::NonEmptyMixHash(header.mix_hash));
        }

        // nonce
        if header.nonce != 0 {
            return Err(BlockValidationError::NonZeroNonce(header.nonce));
        }

        // difficulty
        if header.difficulty != U256::ZERO {
            return Err(BlockValidationError::NonZeroDifficulty(header.difficulty));
        }

        // parent beacon block root
        if header.parent_beacon_block_root.is_some() {
            return Err(BlockValidationError::NonEmptyBeaconRoot(header.parent_beacon_block_root));
        }

        // blob gas used
        if header.blob_gas_used.is_some() {
            return Err(BlockValidationError::NonEmptyBlobGas(header.blob_gas_used));
        }

        // excess blob gas used
        if header.excess_blob_gas.is_some() {
            return Err(BlockValidationError::NonEmptyExcessBlobGas(header.excess_blob_gas));
        }

        // requests root
        if header.requests_root.is_some() {
            return Err(BlockValidationError::NonEmptyRequestsRoot(header.requests_root));
        }

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
    use reth::tasks::{TaskExecutor, TaskManager};
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
        constants::EMPTY_WITHDRAWALS, hex, Address, Bloom, Bytes, GenesisAccount, Header,
        SealedHeader, B256, EMPTY_OMMER_ROOT_HASH,
    };
    use reth_provider::{providers::StaticFileProvider, ProviderFactory};
    use reth_prune::PruneModes;
    use reth_tracing::init_test_tracing;
    use std::{str::FromStr, sync::Arc};
    use tn_types::{
        adiri_genesis,
        test_utils::{get_gas_price, TransactionFactory},
        Consensus,
    };
    use tracing::debug;

    /// Return the next valid block
    fn next_valid_sealed_header() -> SealedHeader {
        // sealed header
        //
        // intentionally used hard-coded values
        SealedHeader::new(
            Header {
                parent_hash: hex!(
                    "0a908204acf0691cb8924082269df1c40deea2d7f2201e82a28cf07bd2a3d4ce"
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
                gas_limit: 30_000_000,
                gas_used: 3_000_000, // TxFactory sets limit to 1_000_000 * 3txs                timestamp: 1701790139,
                timestamp: 1701790139,
                mix_hash: B256::ZERO,
                nonce: 0,
                base_fee_per_gas: Some(7),
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_block_root: None,
                extra_data: Bytes::default(),
                requests_root: None,
            },
            hex!("d962e1964e4351dbbe6095d22caee7199e4e4c0c0947d56ff23ea28ec821ebf5").into(),
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
        // init_test_tracing();
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
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
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

        let validator = BlockValidator::new(blockchain_db, 1_000_000, 30_000_000);
        let valid_header = next_valid_sealed_header();

        // block validator
        TestTools { valid_txs, valid_header, validator }
    }

    #[tokio::test]
    async fn test_valid_block() {
        let TestTools { valid_txs, valid_header, validator } = test_types().await;
        let valid_block = WorkerBlock::new(valid_txs, valid_header);
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
        let wrong_block = WorkerBlock::new(valid_txs, wrong_header);
        assert_matches::assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::CanonicalChain { block_hash }) if block_hash == wrong_parent_hash
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_parent_number() {
        let TestTools { valid_txs, mut valid_header, validator } = test_types().await;
        let wrong_block_number = 3;
        valid_header.set_block_number(wrong_block_number);
        // update hash since this is asserted first
        let wrong_header = valid_header.unseal().seal_slow();
        let wrong_block = WorkerBlock::new(valid_txs, wrong_header);
        assert_matches::assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::ParentBlockNumberMismatch{parent_block_number, block_number}) if parent_block_number == 0 && block_number == 3
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_state_root() {
        let TestTools { valid_txs, mut valid_header, validator } = test_types().await;
        let wrong_state_root = B256::random();
        valid_header.set_state_root(wrong_state_root);
        // update hash since this is asserted first
        let wrong_header = valid_header.unseal().seal_slow();
        let wrong_block = WorkerBlock::new(valid_txs, wrong_header);
        assert_matches::assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::NonEmptyStateRoot(wrong)) if wrong == wrong_state_root
        );
    }

    #[tokio::test]
    async fn test_invalid_block_wrong_transactions_root() {
        let TestTools { valid_header, validator, valid_txs: mut wrong_txs } = test_types().await;
        // remove tx
        let _ = wrong_txs.pop();
        let correct_root: B256 =
            hex!("35cacf0a6e1826b718033b80345e39387f776a2eb3422b90d4265e113ae83c89").into();
        let wrong_root = valid_header.transactions_root;
        let wrong_block = WorkerBlock::new(wrong_txs, valid_header);
        assert_matches::assert_matches!(
            validator.validate_block(&wrong_block).await,
            Err(BlockValidationError::TransactionRootMismatch { expected, peer_root }) if expected == correct_root && peer_root == wrong_root
        );
    }

    // // TODO:
    // // invalid block types for the rest of the sealed header:
    // // - logs bloom
    // // - sealed block number
    // // - BlockGasUsed
    // // etc.
}
