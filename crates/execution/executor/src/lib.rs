//! A block builder implementation for executing the output of consenus.
//!
//! The Mining task polls a [MiningMode], and will return all transactions to include in the next
//! payload.
//!
//! These downloaders poll the miner, assemble the block, and return transactions that are ready to
//! be mined.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(missing_debug_implementations, missing_docs, unreachable_pub, rustdoc::all)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use consensus_metrics::metered_channel::Receiver;
use reth_beacon_consensus::BeaconEngineMessage;
use reth_evm::execute::{BlockExecutionOutput, BlockExecutorProvider, Executor as _};
use reth_interfaces::executor::{BlockExecutionError, BlockValidationError};
use reth_node_api::EngineTypes;
use reth_primitives::{
    constants::{EMPTY_TRANSACTIONS, ETHEREUM_BLOCK_GAS_LIMIT},
    proofs, Address, Block, BlockBody, BlockHash, BlockHashOrNumber, BlockNumber, ChainSpec,
    Header, Receipts, SealedBlockWithSenders, SealedHeader, TransactionSigned, Withdrawals, B256,
    EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::{
    BlockReaderIdExt, BundleStateWithReceipts, CanonStateNotificationSender, StateProviderFactory,
};
use reth_revm::database::StateProviderDatabase;
use std::{collections::HashMap, sync::Arc};
use tn_types::{now, AutoSealConsensus, BatchAPI, ConsensusOutput};
use tokio::sync::{mpsc::UnboundedSender, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, error, trace, warn};

mod client;
mod error;
mod task;

pub use crate::client::AutoSealClient;
use error::ExecutorError;
pub use task::MiningTask;

/// Builder type for configuring the setup
#[derive(Debug)]
pub struct Executor<Client, Engine: EngineTypes, EvmConfig> {
    client: Client,
    consensus: AutoSealConsensus,
    storage: Storage,
    from_consensus: Receiver<ConsensusOutput>,
    to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
    canon_state_notification: CanonStateNotificationSender,
    evm_config: EvmConfig,
}

// === impl AutoSealBuilder ===

impl<Client, Engine, EvmConfig> Executor<Client, Engine, EvmConfig>
where
    Client: BlockReaderIdExt,
    Engine: EngineTypes,
{
    /// Creates a new builder instance to configure all parts.
    pub fn new(
        chain_spec: Arc<ChainSpec>,
        client: Client,
        from_consensus: Receiver<ConsensusOutput>,
        to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
        canon_state_notification: CanonStateNotificationSender,
        evm_config: EvmConfig,
    ) -> Self {
        let latest_header = client
            .latest_header()
            .ok()
            .flatten()
            .unwrap_or_else(|| chain_spec.sealed_genesis_header());

        Self {
            storage: Storage::new(latest_header),
            client,
            consensus: AutoSealConsensus::new(chain_spec),
            from_consensus,
            to_engine,
            canon_state_notification,
            evm_config,
        }
    }

    /// Consumes the type and returns all components
    #[track_caller]
    pub fn build(
        self,
    ) -> (AutoSealConsensus, AutoSealClient, MiningTask<Client, Engine, EvmConfig>) {
        let Self {
            client,
            consensus,
            storage,
            from_consensus,
            to_engine,
            canon_state_notification,
            evm_config,
        } = self;
        let auto_client = AutoSealClient::new(storage.clone());
        let task = MiningTask::new(
            Arc::clone(consensus.chain_spec()),
            to_engine,
            canon_state_notification,
            storage,
            client,
            from_consensus,
            evm_config,
        );
        (consensus, auto_client, task)
    }
}

/// In memory storage
#[derive(Debug, Clone, Default)]
pub(crate) struct Storage {
    inner: Arc<RwLock<StorageInner>>,
}

// == impl Storage ===

impl Storage {
    fn new(header: SealedHeader) -> Self {
        let (header, best_hash) = header.split();
        let mut storage = StorageInner {
            best_hash,
            total_difficulty: header.difficulty,
            best_block: header.number,
            ..Default::default()
        };
        storage.headers.insert(0, header);
        storage.bodies.insert(best_hash, BlockBody::default());
        Self { inner: Arc::new(RwLock::new(storage)) }
    }

    /// Returns the write lock of the storage
    pub(crate) async fn write(&self) -> RwLockWriteGuard<'_, StorageInner> {
        self.inner.write().await
    }

    /// Returns the read lock of the storage
    pub(crate) async fn read(&self) -> RwLockReadGuard<'_, StorageInner> {
        self.inner.read().await
    }
}

/// In-memory storage for the chain the auto seal engine is building.
#[derive(Default, Debug)]
pub(crate) struct StorageInner {
    /// Headers buffered for download.
    pub(crate) headers: HashMap<BlockNumber, Header>,
    /// A mapping between block hash and number.
    pub(crate) hash_to_number: HashMap<BlockHash, BlockNumber>,
    /// Bodies buffered for download.
    pub(crate) bodies: HashMap<BlockHash, BlockBody>,
    /// Tracks best block
    pub(crate) best_block: u64,
    /// Tracks hash of best block
    pub(crate) best_hash: B256,
    /// The total difficulty of the chain until this block
    pub(crate) total_difficulty: U256,
}

// === impl StorageInner ===

impl StorageInner {
    /// Returns the block hash for the given block number if it exists.
    pub(crate) fn block_hash(&self, num: u64) -> Option<BlockHash> {
        self.hash_to_number.iter().find_map(|(k, v)| num.eq(v).then_some(*k))
    }

    /// Returns the matching header if it exists.
    pub(crate) fn header_by_hash_or_number(
        &self,
        hash_or_num: BlockHashOrNumber,
    ) -> Option<Header> {
        let num = match hash_or_num {
            BlockHashOrNumber::Hash(hash) => self.hash_to_number.get(&hash).copied()?,
            BlockHashOrNumber::Number(num) => num,
        };
        self.headers.get(&num).cloned()
    }

    /// Inserts a new header+body pair
    pub(crate) fn insert_new_block(&mut self, mut header: Header, body: BlockBody) {
        header.number = self.best_block + 1;
        header.parent_hash = self.best_hash;

        self.best_hash = header.hash_slow();
        self.best_block = header.number;
        self.total_difficulty += header.difficulty;

        trace!(target: "execution::executor", num=self.best_block, hash=?self.best_hash, "inserting new block");
        self.headers.insert(header.number, header);
        self.bodies.insert(self.best_hash, body);
        self.hash_to_number.insert(self.best_hash, self.best_block);
        tracing::debug!(target: "execution::executor", storage_size=?self.bodies.len());
    }

    /// Fills in pre-execution header fields based on the current best block and given
    /// transactions.
    pub(crate) fn build_header_template(
        &self,
        transactions: &[TransactionSigned],
        chain_spec: Arc<ChainSpec>,
        parent: SealedHeader,
        timestamp: u64,
        beneficiary: Address,
        withdrawals: Option<&Withdrawals>,
    ) -> Header {
        // // check previous block for base fee
        // let base_fee_per_gas = self
        //     .headers
        //     .get(&self.best_block)
        //     .and_then(|parent| parent.next_block_base_fee(chain_spec.base_fee_params));

        // use finalized parent for this batch base fee
        let base_fee_per_gas =
            parent.next_block_base_fee(chain_spec.base_fee_params_at_timestamp(now()));

        let mut header = Header {
            parent_hash: parent.hash(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            beneficiary,
            state_root: Default::default(),
            transactions_root: Default::default(),
            receipts_root: Default::default(),
            withdrawals_root: withdrawals.map(|w| proofs::calculate_withdrawals_root(w)),
            logs_bloom: Default::default(),
            difficulty: U256::ZERO,
            number: parent.number + 1,
            gas_limit: ETHEREUM_BLOCK_GAS_LIMIT,
            gas_used: 0,
            timestamp,
            mix_hash: Default::default(),
            nonce: 0,
            base_fee_per_gas,
            blob_gas_used: None,
            excess_blob_gas: None,
            extra_data: Default::default(),
            parent_beacon_block_root: None,
        };

        header.transactions_root = if transactions.is_empty() {
            EMPTY_TRANSACTIONS
        } else {
            proofs::calculate_transaction_root(transactions)
        };

        header
    }

    /// Builds and executes a new block with the given transactions, on the provided executor.
    ///
    /// This returns the header of the executed block, as well as the poststate from execution.
    pub(crate) fn build_and_execute<Provider, Executor>(
        &mut self,
        output: ConsensusOutput,
        withdrawals: Option<Withdrawals>,
        provider: &Provider,
        chain_spec: Arc<ChainSpec>,
        executor: &Executor,
    ) -> Result<(SealedBlockWithSenders, BundleStateWithReceipts), ExecutorError>
    where
        Executor: BlockExecutorProvider,
        Provider: StateProviderFactory + BlockReaderIdExt,
    {
        // capture timestamp and beneficiary before consuming output
        let timestamp = output.committed_at();
        let beneficiary = output.beneficiary();

        // try to recover transactions and signers
        let transactions = self.try_recover_transactions(output)?;

        // TODO: save this in memory for faster execution
        //
        // use the last canonical block for next batch
        let parent = provider.finalized_header()
            .map_err(|e| {
                error!(target: "execution::executor", "error retrieving provider.finalized_header() {e}");
                BlockExecutionError::LatestBlock(e)
            })?
            .ok_or_else(|| {
                error!(target: "execution::executor", "error retrieving provider.finalized_header() returned `None`");
                BlockExecutionError::LatestBlock(reth_provider::ProviderError::FinalizedBlockNotFound)
            })?;

        debug!(target: "execution::executor", finalized=?parent);

        let header = self.build_header_template(
            &transactions,
            chain_spec.clone(),
            parent,
            timestamp,
            beneficiary,
            withdrawals.as_ref(),
        );

        let block = Block {
            header: header.clone(),
            body: transactions.clone(),
            ommers: vec![],
            withdrawals: withdrawals.clone(),
        }
        .with_recovered_senders()
        .ok_or(BlockExecutionError::Validation(BlockValidationError::SenderRecoveryError))?;

        // take ownership of senders for final result
        let senders = block.senders.clone();

        trace!(target: "execution::executor", transactions=?&block.body, "executing transactions");

        let mut db = StateProviderDatabase::new(
            // need to use finalized block - not latest (aka - "pending")
            provider.state_by_block_hash(self.best_hash)
            .map_err(|e| {
                error!(target: "execution::executor", "error retrieving provider.state_by_block_hash() {e}");
                BlockExecutionError::LatestBlock(e)
            })?
        );

        // execute the block
        let BlockExecutionOutput { state, receipts, .. } =
            executor.executor(&mut db).execute((&block, U256::ZERO).into())?;
        let bundle_state = BundleStateWithReceipts::new(
            state,
            Receipts::from_block_receipt(receipts),
            block.number,
        );

        let Block { mut header, body, .. } = block.block;
        let body =
            BlockBody { transactions: body, ommers: vec![], withdrawals: withdrawals.clone() };

        trace!(target: "execution::executor", ?bundle_state, ?header, ?body, "executed block, calculating state root and completing header");

        // calculate state root
        header.state_root = db.state_root(bundle_state.state()).map_err(|e| {
            error!(target: "execution::executor", "Failed to calculate state root on current state: {e}");
            BlockExecutionError::LatestBlock(e)
        })?;
        trace!(target: "execution::executor", root=?header.state_root, ?body, "calculated state root");

        // finally insert into storage
        self.insert_new_block(header.clone(), body);

        // TODO: calculate hash somewhere else after Storage cleanup
        //
        // prevents re-hashing the header, but if removing Storage, then `insert_new_block` won't
        // have the hash anymore
        //
        // set new header with hash that should have been updated by insert_new_block
        // let new_header = header.seal(self.best_hash);

        // TODO: clean this up
        let tx_length = transactions.len();
        let senders_length = senders.len();

        // seal the block
        let block = Block { header, body: transactions, ommers: vec![], withdrawals };
        let sealed_block = block.seal_slow();

        let sealed_block_with_senders = SealedBlockWithSenders::new(sealed_block, senders)
            .ok_or_else(|| {
                error!(target: "execution::executor", "Unable to seal block with senders");
                ExecutorError::RecoveredTransactionsLength(tx_length, senders_length)
            })?;

        Ok((sealed_block_with_senders, bundle_state))
    }

    /// Try to recover the transactions and senders from the transaction bytes.
    ///
    /// Batches are validated by peers at this point, so decoding and recovering signers should not
    /// error.
    ///
    /// TODO: optimize the `for` loops
    fn try_recover_transactions(
        &self,
        output: ConsensusOutput,
    ) -> Result<Vec<TransactionSigned>, ExecutorError> {
        // collect recovered txs
        //
        // do not recover senders here to ensure consistency with upstream reth
        let mut transactions = vec![];

        // TODO: there is a better way to do this, but wait until after execution refactor
        //
        // bathes.iter() { SealedBlockWithSenders.try_from(&batch) }
        // try_from is missing support for withdrawals -> need to add withdrawals to metadata
        //
        // need to decide if executor produces per batch or giant block

        // loop through all transactions in order
        for batches in output.batches.into_iter() {
            for batch in batches.iter() {
                // TODO: use batch -> SealedBlockWithSenders::try_from()
                for tx in batch.transactions_owned() {
                    // batches must be validated by this point,
                    // so encoding and decoding has already happened
                    // and is not expected to fail
                    let recovered =
                        TransactionSigned::decode_enveloped(&mut tx.as_ref()).map_err(|e| {
                            error!(target: "execution::executor", "Failed to decode enveloped tx: {tx:?}");
                            ExecutorError::DecodeTransaction(e)
                        })?;

                    // collect transaction + signer
                    transactions.push(recovered);
                }
            }
        }

        Ok(transactions)

        // TODO: optimize by returning `impl Iterator<Item = Vec<Bytes>` and using `flat_map` to
        // return an iter of nested vecs since this is the only place where
        // `.transactions_owned` method is used. this would save memory allocation discussion:
        // https://users.rust-lang.org/t/how-far-to-take-iterators-to-avoid-for-loops/30167/10
        //
        // output.batches.into_iter().flat_map(|batches| batches.into_iter().flat_map(|batch|
        // batch.transactions_owned().into_iter().flat_map(|tx|
        // TransactionSigned::decode_enveloped(tx.into())))).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use narwhal_test_utils::default_test_execution_node;

    use reth_primitives::GenesisAccount;
    use reth_provider::{CanonStateNotification, CanonStateSubscriptions};

    use reth_tasks::TaskManager;
    use reth_tracing::init_test_tracing;

    use std::{str::FromStr, time::Duration};
    use tn_types::{adiri_genesis, Certificate, CommittedSubDag, ReputationScores};
    use tokio::time::timeout;

    /// Unit test at this time is very complicated.
    ///
    /// Transactions need to be valid, but the helper functions to create certificates
    /// are not
    #[tokio::test]
    async fn test_execute_consensus_output() -> eyre::Result<()> {
        init_test_tracing();

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let batches = tn_types::test_utils::batches(4); // create 4 batches
        let beneficiary = Address::from_str("0xdbdbdb2cbd23b783741e8d7fcf51e459b497e4a6")
            .expect("beneficiary address from str");
        let consensus_output = ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            batches: vec![batches.clone()],
            beneficiary,
        };

        //=== Execution

        let genesis = adiri_genesis();

        // collect txs and addresses for later assertions
        let mut txs_in_output = vec![];
        let mut senders_in_output = vec![];

        let mut accounts_to_seed = Vec::new();
        for batch in batches.into_iter() {
            for tx in batch.transactions_owned() {
                let tx_signed = TransactionSigned::decode_enveloped(&mut tx.as_ref())
                    .expect("decode tx signed");
                let address = tx_signed.recover_signer().expect("signer recoverable");
                txs_in_output.push(tx_signed);
                senders_in_output.push(address);
                // fund account with 99mil TEL
                let account = (
                    address,
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000")
                            .expect("account balance is parsed"),
                    ),
                );
                accounts_to_seed.push(account);
            }
        }
        debug!("accounts to seed: {accounts_to_seed:?}");

        // genesis
        let genesis = genesis.extend_accounts(accounts_to_seed);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node = default_test_execution_node(Some(chain), None, executor)?;
        let (to_executor, from_consensus) = tn_types::test_channel!(1);
        execution_node.start_engine(from_consensus).await?;
        tokio::task::yield_now().await;

        let provider = execution_node.get_provider().await;
        let mut canon_state_notification_receiver = provider.subscribe_to_canonical_state();

        //=== Testing begins

        // send output to executor
        let res = to_executor.send(consensus_output).await;
        debug!("res: {:?}", res);
        assert!(res.is_ok());

        // wait for next canonical block
        let too_long = Duration::from_secs(5);
        let canon_update = timeout(too_long, canon_state_notification_receiver.recv())
            .await
            .expect("next canonical block created within time")
            .expect("canon update is Some()");

        assert_matches!(canon_update, CanonStateNotification::Commit { .. });
        let canonical_tip = canon_update.tip();
        debug!("canon update: {:?}", canonical_tip);

        // ensure canonical tip is the next block
        // assert_eq!(canonical_tip.header, storage_sealed_header);

        // ensure database and provider are updated
        let canonical_hash = canonical_tip.hash();

        // wait for forkchoice to finish updating
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut current_finalized_header = provider
                .finalized_header()
                .expect("blockchain db has some finalized header 1")
                .expect("some finalized header 2");
            while canonical_hash != current_finalized_header.hash() {
                // sleep - then look up finalized in db again
                println!("\nwaiting for engine to complete forkchoice update...\n");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                current_finalized_header = provider
                    .finalized_header()
                    .expect("blockchain db has some finalized header 1")
                    .expect("some finalized header 2");
            }
            tx.send(current_finalized_header)
        });

        let current_finalized_header = timeout(too_long, rx)
            .await
            .expect("next canonical block created within time")
            .expect("finalized block retrieved from db");

        debug!("update completed...");
        assert_eq!(canonical_hash, current_finalized_header.hash());

        // assert canonical tip contains all txs and senders in batches
        assert_eq!(canonical_tip.block.body, txs_in_output);
        assert_eq!(canonical_tip.block.beneficiary, beneficiary);
        assert_eq!(canonical_tip.senders, senders_in_output);
        Ok(())
    }

    /// TODO: test fails because all transactions must pass or everything fails during execution.
    #[tokio::test]
    async fn test_execute_consensus_output_with_duplicate_transactions() -> eyre::Result<()> {
        init_test_tracing();

        //=== Consensus
        //
        // create consensus output bc transactions in batches
        // are randomly generated
        //
        // for each tx, seed address with funds in genesis
        //
        // TODO: this does not use a "real" `ConsensusOutput`
        //
        // refactor with valid data once test util helpers are in place
        let leader = Certificate::default();
        let sub_dag_index = 1;
        let reputation_scores = ReputationScores::default();
        let previous_sub_dag = None;
        let mut batches = tn_types::test_utils::batches(4); // create 4 batches

        // duplicate transactions for one batch
        //
        // TODO: test batches use default metadata. only transactions are unique.
        //
        // replace last batch with clone of the first - causes duplicate transaction
        batches[3] = batches[0].clone();

        let beneficiary = Address::from_str("0xdbdbdb2cbd23b783741e8d7fcf51e459b497e4a6")
            .expect("beneficiary address from str");
        let consensus_output = ConsensusOutput {
            sub_dag: CommittedSubDag::new(
                vec![Certificate::default()],
                leader,
                sub_dag_index,
                reputation_scores,
                previous_sub_dag,
            )
            .into(),
            batches: vec![batches.clone()],
            beneficiary,
        };

        //=== Execution

        let genesis = adiri_genesis();

        // collect txs and addresses for later assertions
        let mut txs_in_output = vec![];
        let mut senders_in_output = vec![];

        let mut accounts_to_seed = Vec::new();
        for batch in batches.into_iter() {
            for tx in batch.transactions_owned() {
                let tx_signed = TransactionSigned::decode_enveloped(&mut tx.as_ref())
                    .expect("decode tx signed");
                let address = tx_signed.recover_signer().expect("signer recoverable");
                txs_in_output.push(tx_signed);
                senders_in_output.push(address);
                // fund account with 99mil TEL
                let account = (
                    address,
                    GenesisAccount::default().with_balance(
                        U256::from_str("0x51E410C0F93FE543000000")
                            .expect("account balance is parsed"),
                    ),
                );
                accounts_to_seed.push(account);
            }
        }
        debug!("accounts to seed: {accounts_to_seed:?}");

        // genesis
        let genesis = genesis.extend_accounts(accounts_to_seed);
        let chain: Arc<ChainSpec> = Arc::new(genesis.into());

        let manager = TaskManager::current();
        let executor = manager.executor();
        let execution_node = default_test_execution_node(Some(chain), None, executor)?;
        let (to_executor, from_consensus) = tn_types::test_channel!(1);
        execution_node.start_engine(from_consensus).await?;
        tokio::task::yield_now().await;

        let provider = execution_node.get_provider().await;
        let mut canon_state_notification_receiver = provider.subscribe_to_canonical_state();

        //=== Testing begins

        // send output to executor
        let res = to_executor.send(consensus_output).await;
        debug!("res: {:?}", res);
        assert!(res.is_ok());

        // wait for next canonical block
        let too_long = Duration::from_secs(5);
        let canon_update = timeout(too_long, canon_state_notification_receiver.recv())
            .await
            .expect("next canonical block created within time")
            .expect("canon update is Some()");

        assert_matches!(canon_update, CanonStateNotification::Commit { .. });
        let canonical_tip = canon_update.tip();
        debug!("canon update: {:?}", canonical_tip);

        // ensure database and provider are updated
        let canonical_hash = canonical_tip.hash();

        // wait for forkchoice to finish updating
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let mut current_finalized_header = provider
                .finalized_header()
                .expect("blockchain db has some finalized header 1")
                .expect("some finalized header 2");
            while canonical_hash != current_finalized_header.hash() {
                // sleep - then look up finalized in db again
                println!("\nwaiting for engine to complete forkchoice update...\n");
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                current_finalized_header = provider
                    .finalized_header()
                    .expect("blockchain db has some finalized header 1")
                    .expect("some finalized header 2");
            }
            tx.send(current_finalized_header)
        });

        let current_finalized_header = timeout(too_long, rx)
            .await
            .expect("next canonical block created within time")
            .expect("finalized block retrieved from db");

        debug!("update completed...");
        assert_eq!(canonical_hash, current_finalized_header.hash());

        // assert canonical tip contains all txs and senders in batches
        assert_eq!(canonical_tip.block.body, txs_in_output);
        assert_eq!(canonical_tip.block.beneficiary, beneficiary);
        assert_eq!(canonical_tip.senders, senders_in_output);
        Ok(())
    }
}
