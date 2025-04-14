//! Block validator

use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
use tn_reth::{recover_signed_transaction, RethEnv};
use tn_types::{
    max_batch_gas, max_batch_size, BatchValidation, BatchValidationError, BlockHash, ExecHeader,
    SealedBatch, TransactionSigned, TransactionTrait as _, PARALLEL_SENDER_RECOVERY_THRESHOLD,
};

/// Type convenience for implementing block validation errors.
type BatchValidationResult<T> = Result<T, BatchValidationError>;

/// Block validator
#[derive(Clone)]
pub struct BatchValidator {
    /// Database provider to encompass tree and provider factory.
    reth_env: RethEnv,
}

impl BatchValidation for BatchValidator {
    /// Validate a peer's batch.
    ///
    /// Workers do not execute full batches. This method validates the required information.
    fn validate_batch(&self, sealed_batch: SealedBatch) -> BatchValidationResult<()> {
        // ensure digest matches batch
        let (batch, digest) = sealed_batch.split();
        let verified_hash = batch.clone().seal_slow().digest();
        if digest != verified_hash {
            return Err(BatchValidationError::InvalidDigest);
        }

        // TODO: validate individual transactions against parent

        // obtain info for validation
        let transactions = batch.transactions();

        // first step towards validating parent's header
        // Note this is really a "best effort" check.  If we have not
        // executed parent_hash yet then it will use the last executed batch if
        // available.  Making it manditory would require waiting to see
        // if we execute it soon to avoid false failures.
        // The primary header should get checked so this should be ok.
        let parent =
            self.reth_env.header(batch.parent_hash).unwrap_or_default().unwrap_or_else(|| {
                if let Some(header) = self.reth_env.finalized_header().unwrap_or_default() {
                    header
                } else {
                    ExecHeader::default()
                }
            });

        // validate timestamp vs parent
        self.validate_against_parent_timestamp(batch.timestamp, &parent)?;

        // validate batch size (bytes)
        self.validate_batch_size_bytes(transactions, batch.timestamp)?;

        // validate txs decode
        let decoded_txs = self.decode_transactions(transactions, digest)?;

        // validate gas limit
        self.validate_batch_gas(&decoded_txs, batch.timestamp)?;

        // no-op
        self.validate_basefee()?;
        Ok(())
    }
}

impl BatchValidator {
    /// Create a new instance of [Self]
    pub fn new(reth_env: RethEnv) -> Self {
        Self { reth_env }
    }

    /// Validates the timestamp against the parent to make sure it is in the past.
    #[inline]
    fn validate_against_parent_timestamp(
        &self,
        timestamp: u64,
        parent: &ExecHeader,
    ) -> BatchValidationResult<()> {
        if timestamp <= parent.timestamp {
            return Err(BatchValidationError::TimestampIsInPast {
                parent_timestamp: parent.timestamp,
                timestamp,
            });
        }
        Ok(())
    }

    /// Validate the size of transactions (in bytes).
    fn validate_batch_size_bytes(
        &self,
        transactions: &[Vec<u8>],
        timestamp: u64,
    ) -> BatchValidationResult<()> {
        // calculate size (in bytes) of included transactions
        let total_bytes = transactions
            .iter()
            .map(|tx| tx.len())
            .reduce(|total, size| total + size)
            .ok_or(BatchValidationError::EmptyBatch)?;
        let max_tx_bytes = max_batch_size(timestamp);

        // allow txs that equal max tx bytes
        if total_bytes > max_tx_bytes {
            return Err(BatchValidationError::HeaderTransactionBytesExceedsMax(total_bytes));
        }

        Ok(())
    }

    /// Decode transactions to ensure encode/decode is valid.
    ///
    /// The decoded transactions are then used to validate max batch gas.
    #[inline]
    fn decode_transactions(
        &self,
        transactions: &Vec<Vec<u8>>,
        digest: BlockHash,
    ) -> BatchValidationResult<Vec<TransactionSigned>> {
        if transactions.len() < *PARALLEL_SENDER_RECOVERY_THRESHOLD {
            transactions
                .iter()
                .map(|tx| Self::recover_and_validate(tx, digest))
                .collect::<BatchValidationResult<Vec<_>>>()
        } else {
            transactions
                .par_iter()
                .map(|tx| Self::recover_and_validate(tx, digest))
                .collect::<BatchValidationResult<Vec<_>>>()
        }
    }

    /// Possible gas used needs to be less than block's gas limit.
    ///
    /// Actual amount of gas used cannot be determined until execution.
    #[inline]
    fn validate_batch_gas(
        &self,
        transactions: &[TransactionSigned],
        timestamp: u64,
    ) -> BatchValidationResult<()> {
        // calculate total using tx gas limit
        let total_possible_gas = transactions
            .iter()
            .map(|tx| tx.gas_limit())
            .reduce(|total, size| total + size)
            .ok_or(BatchValidationError::EmptyBatch)?;

        // ensure total tx gas limit fits into block's gas limit
        let max_tx_gas = max_batch_gas(timestamp);
        if total_possible_gas > max_tx_gas {
            return Err(BatchValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas,
                gas_limit: max_tx_gas,
            });
        }

        Ok(())
    }

    /// TODO: Validate the block's basefee
    fn validate_basefee(&self) -> BatchValidationResult<()> {
        Ok(())
    }

    /// Helper function for decoding and recovering transactions.
    fn recover_and_validate(
        tx: &[u8],
        digest: BlockHash,
    ) -> BatchValidationResult<TransactionSigned> {
        recover_signed_transaction(tx)
            .map_err(|e| BatchValidationError::RecoverTransaction(digest, e.to_string()))
    }
}

/// Noop validation struct that validates any block.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Default, Clone)]
pub struct NoopBatchValidator;

#[cfg(any(test, feature = "test-utils"))]
impl BatchValidation for NoopBatchValidator {
    fn validate_batch(&self, _batch: SealedBatch) -> Result<(), BatchValidationError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::{path::Path, str::FromStr, sync::Arc};
    use tempfile::TempDir;
    use tn_reth::RethChainSpec;
    use tn_test_utils::{test_genesis, TransactionFactory};
    use tn_types::{
        adiri_genesis, hex_literal::hex, max_batch_gas, Address, Batch, Bytes, Encodable2718 as _,
        GenesisAccount, TaskManager, B256, MIN_PROTOCOL_BASE_FEE, U256,
    };

    /// Return the next valid sealed batch
    fn next_valid_sealed_batch() -> SealedBatch {
        let timestamp = 1701790139;
        // create valid transactions
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let genesis_hash = chain.genesis_hash();

        // create 3 transactions
        let transaction1 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction2 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let transaction3 = tx_factory.create_eip1559_encoded(
            chain,
            None,
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let valid_txs = vec![transaction1, transaction2, transaction3];
        let batch = Batch {
            transactions: valid_txs,
            parent_hash: genesis_hash,
            beneficiary: Address::ZERO,
            timestamp,
            base_fee_per_gas: Some(MIN_PROTOCOL_BASE_FEE),
            received_at: None,
        };

        // sealed batch
        //
        // intentionally used hard-coded values
        SealedBatch::new(
            batch,
            hex!("9f0a6a575088d4aa97eb832e8486a5647109d82e90747c735285393db1d65271").into(),
        )
    }

    /// Convenience type for creating test assets.
    struct TestTools {
        /// The expected sealed batch.
        valid_batch: SealedBatch,
        /// Validator
        validator: BatchValidator,
    }

    /// Create an instance of block validator for tests.
    async fn test_tools(path: &Path, task_manager: &TaskManager) -> TestTools {
        // genesis with default TransactionFactory funded
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let validator = BatchValidator::new(
            RethEnv::new_for_test_with_chain(chain.clone(), path, task_manager).unwrap(),
        );
        let valid_batch = next_valid_sealed_batch();

        // block validator
        TestTools { valid_batch, validator }
    }

    #[tokio::test]
    async fn test_valid_batch() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let result = validator.validate_batch(valid_batch.clone());
        assert!(result.is_ok());

        // ensure non-serialized data does not affect validity
        let (mut batch, _) = valid_batch.split();
        batch.received_at = Some(tn_types::now());
        let different_block = batch.seal_slow();
        let result = validator.validate_batch(different_block);
        assert!(result.is_ok());
    }

    //#[tokio::test]
    // This is not checked currently, leaving test for bit to make sure we want this.
    // This check will lead to occasional false errors and should not be critical since
    // we should be validating parentage when building actual blocks (including any
    // needed waits for execution).
    async fn _test_invalid_batch_wrong_parent_hash() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (batch, _) = valid_batch.split();
        let Batch { transactions, beneficiary, timestamp, base_fee_per_gas, received_at, .. } =
            batch;
        let wrong_parent_hash = B256::random();
        let invalid_batch = Batch {
            transactions,
            parent_hash: wrong_parent_hash,
            beneficiary,
            timestamp,
            base_fee_per_gas,
            received_at,
        };
        assert_matches!(
            validator.validate_batch(invalid_batch.seal_slow()),
            Err(BatchValidationError::CanonicalChain { block_hash }) if block_hash == wrong_parent_hash
        );
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_timestamp() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // test batch timestamp same as parent
        let wrong_timestamp = adiri_genesis().timestamp;
        batch.timestamp = wrong_timestamp;

        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()),
            Err(BatchValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp
        );

        // test header timestamp before parent
        batch.timestamp = wrong_timestamp - 1;

        assert_matches!(
            validator.validate_batch(batch.seal_slow()),
            Err(BatchValidationError::TimestampIsInPast{parent_timestamp, timestamp}) if parent_timestamp == wrong_timestamp && timestamp == wrong_timestamp - 1
        );
    }

    #[tokio::test]
    async fn test_invalid_batch_excess_gas_used() {
        // Set excessive gas limit.
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (batch, _) = valid_batch.split();

        // sign excessive transaction
        let mut tx_factory = TransactionFactory::new();
        let value = U256::from(10).checked_pow(U256::from(18)).expect("1e18 doesn't overflow U256");
        let gas_price = 7;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());

        // create transaction with max gas limit above the max allowed
        let invalid_transaction = tx_factory.create_eip1559_encoded(
            chain.clone(),
            Some(max_batch_gas(batch.timestamp) + 1),
            gas_price,
            Some(Address::ZERO),
            value, // 1 TEL
            Bytes::new(),
        );

        let Batch { beneficiary, timestamp, base_fee_per_gas, received_at, parent_hash, .. } =
            batch;
        let invalid_batch = Batch {
            transactions: vec![invalid_transaction],
            parent_hash,
            beneficiary,
            timestamp,
            base_fee_per_gas,
            received_at,
        };

        let decoded_txs = validator
            .decode_transactions(invalid_batch.transactions(), invalid_batch.digest())
            .expect("txs decode correctly");

        assert_matches!(
            validator.validate_batch_gas(&decoded_txs, invalid_batch.timestamp),
            Err(BatchValidationError::HeaderMaxGasExceedsGasLimit {
                total_possible_gas: _,
                gas_limit: _
            })
        );
    }

    #[tokio::test]
    async fn test_invalid_batch_wrong_size_in_bytes() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        // create enough transactions to exceed 1MB
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
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        // currently: 9714 txs
        let mut too_many_txs = Vec::new();
        let mut total_bytes = 0;
        while total_bytes < max_batch_size(0) {
            let tx = tx_factory
                .create_explicit_eip1559(
                    Some(chain.chain.id()),
                    None,                    // default nonce
                    None,                    // no tip
                    Some(7),                 // min basefee for block 1
                    Some(1),                 // low gas limit to prevent excess gas used error
                    Some(Address::random()), // send to random address
                    Some(U256::from(100)),   // send low amount
                    None,                    // no input
                    None,                    // no access list
                )
                .encoded_2718();

            // track totals
            total_bytes += tx.len();
            too_many_txs.push(tx);
        }

        // NOTE: these assertions aren't important but want to know if tx size changes
        assert_eq!(too_many_txs.len(), 9714);

        // update header so tx root is correct
        let (mut block, _hash) = valid_batch.split();
        block.transactions = too_many_txs;
        let invalid_batch = block.clone().seal_slow();

        assert_matches!(
            validator.validate_batch(invalid_batch),
            Err(BatchValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == total_bytes
        );

        // Generate 1MB vec of 1s - total bytes are: 1_000_213
        let big_input = vec![1u8; 1_000_000];

        // create giant tx
        let max_gas = max_batch_gas(0);
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
        let too_big = giant_tx.encoded_2718();
        let expected_len = too_big.len();
        assert_eq!(expected_len, 1_000_090);

        let invalid_txs = vec![too_big];
        block.transactions = invalid_txs;
        let invalid_batch = block.seal_slow();
        assert_matches!(
            validator.validate_batch(invalid_batch),
            Err(BatchValidationError::HeaderTransactionBytesExceedsMax(wrong)) if wrong == expected_len
        );
    }

    #[tokio::test]
    async fn test_invalid_batch_empty_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // test batch with no transactions
        batch.transactions = Vec::new();

        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()),
            Err(BatchValidationError::EmptyBatch)
        );
    }

    #[tokio::test]
    async fn test_invalid_batch_decode_transactions() {
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::default();
        let TestTools { valid_batch, validator } = test_tools(tmp_dir.path(), &task_manager).await;
        let (mut batch, _) = valid_batch.split();

        // test batch with bad decode
        batch.transactions = vec![b"this is a bad batch".to_vec()];

        assert_matches!(
            validator.validate_batch(batch.clone().seal_slow()),
            Err(BatchValidationError::RecoverTransaction(_, _))
        );
    }
}
