//! Batch validator

use crate::error::BatchValidationError;
use reth_blockchain_tree::BundleStateDataRef;
use reth_db::database::Database;
use reth_evm::execute::{BlockExecutionOutput, BlockExecutorProvider, Executor};
use reth_interfaces::{
    blockchain_tree::{error::BlockchainTreeError, BlockchainTreeViewer},
    executor::BlockValidationError,
};
use reth_primitives::{GotExpected, Hardfork, Receipts, SealedBlockWithSenders, U256};
use reth_provider::{
    providers::{BlockchainProvider, BundleStateProvider},
    BundleStateForkProvider as _, BundleStateWithReceipts, ChainSpecProvider, HeaderProvider,
    StateProviderFactory, StateRootProvider,
};
use reth_revm::database::StateProviderDatabase;
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    sync::Arc,
};
use tn_types::{Batch, Consensus};
use tracing::{debug, error};

/// Batch validator
#[derive(Clone)]
pub struct BatchValidator<DB, Evm>
where
    DB: Database + Clone + 'static,
    Evm: BlockExecutorProvider + 'static,
{
    /// Validation methods for beacon consensus.
    ///
    /// Required to remain fully compatible with Ethereum.
    consensus: Arc<dyn Consensus>,
    /// Database provider to encompass tree and provider factory.
    blockchain_db: BlockchainProvider<DB>,
    /// The executor factory to execute blocks with.
    executor_factory: Evm,
}

/// Defines the validation procedure for receiving either a new single transaction (from a client)
/// of a batch of transactions (from another validator). Invalid transactions will not receive
/// further processing.
#[async_trait::async_trait]
pub trait BatchValidation: Clone + Send + Sync + 'static {
    type Error: Display + Debug + Send + Sync + 'static;
    /// Determines if this batch can be voted on
    async fn validate_batch(&self, b: &Batch) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
// impl<DB, EF> TransactionValidator for BatchValidator<DB, EF>
impl<DB, Evm> BatchValidation for BatchValidator<DB, Evm>
where
    DB: Database + Sized + Clone + 'static,
    Evm: BlockExecutorProvider + Clone + 'static,
{
    /// Error type for batch validation
    type Error = BatchValidationError;

    /// Execute the transactions within the batch
    ///
    /// akin to `on_new_payload()` for `BeaconEngine`.
    ///
    /// BlockchainTree has several useful methods, but they are private. The publicly exposed
    /// methods would result in canonicalizing batches, which is undesireable. It is possible to
    /// append then revert the batch, but this is also very inefficient.
    ///
    /// The validator flow follows: `reth::blockchain_tree::blockchain_tree::validate_block` method.
    async fn validate_batch(&self, batch: &Batch) -> Result<(), Self::Error> {
        // check sui + reth
        //
        // ensure well-formed batch
        // verify receipts
        // ensure timestamps are valid
        // parent
        // ensure

        // try recover senders/txs
        // create sealed block with senders
        // BT: try_insert_validated_block
        // beacon consensus checks - timestamps, forks, etc.
        // ensure parent number +1 is batch's sealed header number
        // parent should be canonical - lookup in db

        // try to recover signed transactions
        let sealed_block: SealedBlockWithSenders = batch.try_into()?;

        // first, ensure valid block
        // taken from blockchain_tree::validate_block
        if let Err(e) =
            self.consensus.validate_header_with_total_difficulty(&sealed_block, U256::MAX)
        {
            error!(
                ?sealed_block,
                "Failed to validate total difficulty for block {}: {e}",
                sealed_block.header.hash()
            );
            return Err(e.into())
        }

        if let Err(e) = self.consensus.validate_header(&sealed_block) {
            error!(?sealed_block, "Failed to validate header {}: {e}", sealed_block.header.hash());
            return Err(e.into())
        }

        if let Err(e) = self.consensus.validate_block_pre_execution(&sealed_block) {
            error!(?sealed_block, "Failed to validate block {}: {e}", sealed_block.header.hash());
            return Err(e.into())
        }

        // the following is taken from BlockchainTree::try_append_canonical_chain()

        // the main reason for porting this code is bc batches may or may not
        // extend the canonical tip, but state root still needs to be validated

        // in reth, this is only done when canonical head is extended
        // but batches may be behind canonical tip, which should still
        // be considered potentially valid

        // moving this code here prevents having to revert the tree after
        // validating the batch because `on_new_payload` results in appending
        // the execution payload to either a fork or the canonical tree

        // all other methods are private

        let parent = sealed_block.parent_num_hash();
        let block_num_hash = sealed_block.num_hash();
        debug!(target: "batch_validator", head = ?block_num_hash.hash, ?parent, "Appending block to canonical chain");

        // Validate that the block is post merge
        let parent_td = self
            .blockchain_db
            .header_td(&parent.hash)?
            .ok_or(BlockchainTreeError::CanonicalChain { block_hash: parent.hash })?;

        // Pass the parent total difficulty to short-circuit unnecessary calculations.
        if !self
            .blockchain_db
            .chain_spec()
            .fork(Hardfork::Paris)
            .active_at_ttd(parent_td, U256::ZERO)
        {
            return Err(BlockValidationError::BlockPreMerge { hash: sealed_block.hash() })?
        }

        // retrieve parent header from provider
        let parent_header = self
            .blockchain_db
            .header(&parent.hash)?
            .ok_or(BlockchainTreeError::CanonicalChain { block_hash: parent.hash })?
            .seal(parent.hash);

        // read from canonical tree - updated by `Executor` and engine
        //
        // same return as BlockchainTree::canonical_chain()
        let canonical_block_hashes = self.blockchain_db.canonical_blocks();

        // from AppendableChain::new_canonical_fork() but with state root validation added
        let state = BundleStateWithReceipts::default();
        let empty = BTreeMap::new();

        // get the bundle state provider.
        let bundle_state_data_provider = BundleStateDataRef {
            state: &state,
            sidechain_block_hashes: &empty,
            canonical_block_hashes: &canonical_block_hashes,
            canonical_fork: parent,
        };

        // from AppendableChain::validate_and_execute() - private method
        //
        // ported here to prevent redundant creation of bundle state provider
        // just to check state root

        self.consensus.validate_header_against_parent(&sealed_block, &parent_header)?;

        // let (block, senders) = block.into_components();
        // let block = block.unseal();

        let block_with_senders = sealed_block.unseal();

        let canonical_fork = bundle_state_data_provider.canonical_fork();

        // NOTE: this diverges from reth-beta approach
        // but is still valid within the context of our consensus
        // because of async network conditions, workers can suggest batches
        // behind the canonical tip
        //
        // TODO: validate base fee based on parent batch
        let state_provider = self.blockchain_db.history_by_block_number(canonical_fork.number)?;

        // capture current state
        let provider = BundleStateProvider::new(state_provider, bundle_state_data_provider);
        let db = StateProviderDatabase::new(&provider);

        // executor for single block
        let executor = self.executor_factory.executor(db);
        let state = executor.execute((&block_with_senders, U256::MAX).into())?;
        let BlockExecutionOutput { state, receipts, .. } = state;
        self.consensus.validate_block_post_execution(&block_with_senders, &receipts)?;

        // create bundle state
        let bundle_state = BundleStateWithReceipts::new(
            state,
            Receipts::from_block_receipt(receipts),
            block_with_senders.number,
        );

        // TODO: enable ParallelStateRoot feature (or AsyncStateRoot)
        // for better perfomance
        // see reth::blockchain_tree::chain::AppendableChain::validate_and_execute()
        //
        // check state root
        let state_root = provider.state_root(bundle_state.state())?;
        if block_with_senders.state_root != state_root {
            return Err(BatchValidationError::BodyStateRootDiff(GotExpected {
                got: state_root,
                expected: block_with_senders.state_root,
            }))
        }

        Ok(())
    }
}

impl<DB, Evm> BatchValidator<DB, Evm>
where
    DB: Database + Clone,
    Evm: BlockExecutorProvider,
{
    /// Create a new instance of [Self]
    pub fn new(
        consensus: Arc<dyn Consensus>,
        blockchain_db: BlockchainProvider<DB>,
        executor_factory: Evm,
    ) -> Self {
        Self { consensus, blockchain_db, executor_factory }
    }
}

#[cfg(any(test, feature = "test-utils"))]
/// Noop validation struct that validates any batch.
#[derive(Default, Clone)]
pub struct NoopBatchValidator;

#[cfg(any(test, feature = "test-utils"))]
#[async_trait::async_trait]
impl BatchValidation for NoopBatchValidator {
    type Error = BatchValidationError;

    async fn validate_batch(&self, _batch: &Batch) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_beacon_consensus::EthBeaconConsensus;
    use reth_blockchain_tree::{
        BlockchainTree, BlockchainTreeConfig, ShareableBlockchainTree, TreeExternals,
    };
    use reth_db::test_utils::{create_test_rw_db, tempdir_path};
    use reth_node_core::init::init_genesis;
    use reth_primitives::{
        constants::EMPTY_WITHDRAWALS, hex, proofs::calculate_transaction_root, Address, Bloom,
        Bytes, ChainSpec, GenesisAccount, Header, SealedHeader, B256, EMPTY_OMMER_ROOT_HASH,
    };
    use reth_provider::ProviderFactory;
    use reth_tracing::init_test_tracing;
    use std::str::FromStr;
    use tn_types::{
        adiri_genesis,
        test_utils::{get_gas_price, TransactionFactory},
        VersionedMetadata,
    };

    /// Return the next valid batch
    fn next_valid_sealed_header() -> SealedHeader {
        // sealed header
        //
        // intentionally used hard-coded values
        SealedHeader::new(
            Header {
                parent_hash: hex!(
                    "9ca9e62a3599c89955a93e0c76130973221f6056af2afd738d8514a66a900ebb"
                )
                .into(),
                ommers_hash: EMPTY_OMMER_ROOT_HASH,
                beneficiary: hex!("0000000000000000000000000000000000000000").into(),
                state_root: hex!(
                    "1e6751bd30803af71e305b61cf2da9bb3fa1a6841bc333f49c7422e1f8b0d013"
                )
                .into(),
                transactions_root: hex!(
                    "a0cb6aaf543a2b8c457c17d926f26af5ddf9df0511d0fac140e6051b370d333d"
                )
                .into(),
                receipts_root: hex!(
                    "25e6b7af647c519a27cc13276a1e6abc46154b51414d174b072698df1f6c19df"
                )
                .into(),
                withdrawals_root: Some(EMPTY_WITHDRAWALS),
                logs_bloom: Bloom::default(),
                difficulty: U256::ZERO,
                number: 1,
                gas_limit: 30000000,
                gas_used: 63000,
                timestamp: 1701790139,
                mix_hash: B256::ZERO,
                nonce: 0,
                base_fee_per_gas: Some(875000000),
                blob_gas_used: None,
                excess_blob_gas: None,
                parent_beacon_block_root: None,
                extra_data: Bytes::default(),
            },
            hex!("ed9242a844ec144e25b58c085184c3c4ae8709226771659badf7e45cdd415c58").into(),
        )
    }

    #[tokio::test]
    async fn test_valid_batch() {
        init_test_tracing();
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

        // init genesis
        let db = create_test_rw_db();
        let provider_factory =
            ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain), tempdir_path())
                .expect("provider factory");
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
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // get gas price before passing db
        let gas_price = get_gas_price(&blockchain_db);

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            blockchain_db,
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );

        let sealed_header = next_valid_sealed_header();

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        let txs = vec![transaction1.clone(), transaction2.clone(), transaction3.clone()];
        let tx_root = calculate_transaction_root(&txs);
        println!("\n\ntx_root: {tx_root:?}");

        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;

        println!("result: {result:?}");

        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_parent_hash() {
        init_test_tracing();
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

        // init genesis
        let db = create_test_rw_db();
        let provider_factory =
            ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain), tempdir_path())
                .expect("provider factory");
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
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            blockchain_db.clone(),
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let gas_price = get_gas_price(&blockchain_db);
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        let wrong_parent_hash = B256::ZERO;

        // sealed header
        let mut sealed_header = next_valid_sealed_header();
        sealed_header.set_parent_hash(wrong_parent_hash);

        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;

        assert!(result.is_err())
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_state_root() {
        init_test_tracing();
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

        // init genesis
        let db = create_test_rw_db();
        let provider_factory =
            ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain), tempdir_path())
                .expect("provider factory");
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
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            blockchain_db.clone(),
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let gas_price = get_gas_price(&blockchain_db);
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        let mut sealed_header = next_valid_sealed_header();
        let wrong_state_root = hex!(
            "0000000000000000af72d17a5ed533329c894c8e181fa1616428a1e9ae51bcf2" // wrong
        )
        .into();
        sealed_header.set_state_root(wrong_state_root);

        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;

        assert!(result.is_err())
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_tx_root() {
        init_test_tracing();
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

        // init genesis
        let db = create_test_rw_db();
        let provider_factory =
            ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain), tempdir_path())
                .expect("provider factory");
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
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            blockchain_db.clone(),
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let gas_price = get_gas_price(&blockchain_db);
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        let sealed_header = next_valid_sealed_header();
        // work around
        let mut header = sealed_header.header().clone();
        let hash = sealed_header.hash();

        // update header with wrong tx root
        let wrong_tx_root = hex!(
            "00000000000000000000068270d6c1ce790c9a8d74835c315d5abecf851a8c74" // wrong
        )
        .into();
        header.transactions_root = wrong_tx_root;
        let sealed_header = SealedHeader::new(header, hash);

        // sealed header
        // let sealed_header = SealedHeader::new(
        //     Header {
        //         parent_hash: genesis_hash,
        //         ommers_hash: EMPTY_OMMER_ROOT_HASH,
        //         beneficiary: hex!("0000000000000000000000000000000000000000").into(),
        //         state_root: hex!(
        //             "c65c4aa390278016af72d17a5ed533329c894c8e181fa1616428a1e9ae51bcf2"
        //         )
        //         .into(),
        //         transactions_root: hex!(
        //             "00000000000000000000068270d6c1ce790c9a8d74835c315d5abecf851a8c74" // wrong
        //         )
        //         .into(),
        //         receipts_root: hex!(
        //             "25e6b7af647c519a27cc13276a1e6abc46154b51414d174b072698df1f6c19df"
        //         )
        //         .into(),
        //         withdrawals_root: None,
        //         logs_bloom: Bloom::default(),
        //         difficulty: U256::ZERO,
        //         number: 1,
        //         gas_limit: 30000000,
        //         gas_used: 63000,
        //         timestamp: 1701790139,
        //         mix_hash: B256::ZERO,
        //         nonce: 0,
        //         base_fee_per_gas: Some(875000000),
        //         blob_gas_used: None,
        //         excess_blob_gas: None,
        //         parent_beacon_block_root: None,
        //         extra_data: Bytes::default(),
        //     },
        //     hex!("ed9242a844ec144e25b58c085184c3c4ae8709226771659badf7e45cdd415c58").into(),
        // );

        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;

        assert!(result.is_err())
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_receipts_root() {
        init_test_tracing();
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

        // init genesis
        let db = create_test_rw_db();
        let provider_factory =
            ProviderFactory::new(Arc::clone(&db), Arc::clone(&chain), tempdir_path())
                .expect("provider factory");
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
        let tree = BlockchainTree::new(
            tree_externals,
            tree_config,
            None, // prune config
        )
        .expect("blockchain tree is valid");

        let blockchain_tree = Arc::new(ShareableBlockchainTree::new(tree));

        // provider
        let blockchain_db =
            BlockchainProvider::new(provider_factory.clone(), blockchain_tree.clone())
                .expect("blockchain db valid");

        // batch validator
        let batch_validator = BatchValidator::new(
            Arc::clone(&consensus),
            blockchain_db.clone(),
            reth_node_ethereum::EthExecutorProvider::ethereum(chain.clone()),
        );

        // tx factory - [0; 32] seed address - nonce 0-2
        //
        // transactions are deterministic bc the factory is seeded with [0; 32]

        let gas_price = get_gas_price(&blockchain_db);
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 1: {transaction1:?}");

        let transaction2 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 2: {transaction2:?}");

        let transaction3 = tx_factory.create_eip1559(
            chain.clone(),
            gas_price,
            Address::ZERO,
            value, // 1 TEL
        );
        debug!("transaction 3: {transaction3:?}");

        // sealed header
        let sealed_header = next_valid_sealed_header();
        let mut header = sealed_header.header().clone();
        let hash = sealed_header.hash();
        let wrong_receipts_root = hex!(
            "0000000000000000000003276a1e6abc46154b51414d174b072698df1f6c19df" // wrong
        )
        .into();
        header.receipts_root = wrong_receipts_root;
        let sealed_header = SealedHeader::new(header, hash);
        // let sealed_header = SealedHeader::new(
        //     Header {
        //         parent_hash: genesis_hash,
        //         ommers_hash: EMPTY_OMMER_ROOT_HASH,
        //         beneficiary: hex!("0000000000000000000000000000000000000000").into(),
        //         state_root: hex!(
        //             "c65c4aa390278016af72d17a5ed533329c894c8e181fa1616428a1e9ae51bcf2"
        //         )
        //         .into(),
        //         transactions_root: hex!(
        //             "73b96de5ae50ad0100ad668270d6c1ce790c9a8d74835c315d5abecf851a8c74"
        //         )
        //         .into(),
        //         receipts_root: hex!(
        //             "0000000000000000000003276a1e6abc46154b51414d174b072698df1f6c19df" // wrong
        //         )
        //         .into(),
        //         withdrawals_root: None,
        //         logs_bloom: Bloom::default(),
        //         difficulty: U256::ZERO,
        //         number: 1,
        //         gas_limit: 30000000,
        //         gas_used: 63000,
        //         timestamp: 1701790139,
        //         mix_hash: B256::ZERO,
        //         nonce: 0,
        //         base_fee_per_gas: Some(875000000),
        //         blob_gas_used: None,
        //         excess_blob_gas: None,
        //         parent_beacon_block_root: None,
        //         extra_data: Bytes::default(),
        //     },
        //     hex!("ed9242a844ec144e25b58c085184c3c4ae8709226771659badf7e45cdd415c58").into(),
        // );

        let transactions = vec![
            transaction1.envelope_encoded().into(),
            transaction2.envelope_encoded().into(),
            transaction3.envelope_encoded().into(),
        ];
        let metadata = VersionedMetadata::new(sealed_header.clone());
        let batch = Batch::new_with_metadata(transactions, metadata);

        let result = batch_validator.validate_batch(&batch).await;

        assert!(result.is_err())
    }

    // TODO:
    // invalid batch types for the rest of the sealed header:
    // - logs bloom
    // - sealed block number
    // - BlockGasUsed
    // etc.
}
