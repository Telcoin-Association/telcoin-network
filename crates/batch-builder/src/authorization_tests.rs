//! Cross-pool EIP-7702 policy and execution regressions for issue #1334.

use crate::{build_batch, test_utils::TestPool};
use futures_util::{stream, TryStreamExt as _};
use std::sync::Arc;
use tempfile::TempDir;
use tn_batch_validator::BatchValidator;
use tn_reth::{
    payload::TNPayload,
    recover_raw_transaction,
    test_utils::{
        consensus_output_for_tests, execute_payload_and_update_canonical_chain, TransactionFactory,
    },
    ExecutedBlock, RethChainSpec, RethEnv, WorkerTxPool,
};
use tn_types::{
    gas_accumulator::BaseFeeContainer, test_genesis, Address, Authorization, Batch,
    BatchBuilderArgs, BatchValidation as _, BatchValidationError, Bytes, Encodable2718 as _,
    GenesisAccount, SealedHeader, TaskManager, TransactionSigned, TxEip7702, MIN_PROTOCOL_BASE_FEE,
    U256,
};

/// Funded, contiguous transactions actually admitted and packed in the regression.
const SEQUENCE_LEN: usize = 8;
/// Sweep the account's balance to its caller: PUSH0 x4, SELFBALANCE, CALLER, GAS, CALL, STOP.
const SWEEP: &[u8] = &[0x5f, 0x5f, 0x5f, 0x5f, 0x47, 0x33, 0x5a, 0xf1, 0x00];

/// Independently funded signers on one Prague-enabled chain.
struct Scenario {
    /// Account owning the authorization and ordinary transaction sequence.
    alice: TransactionFactory,
    /// Independent sponsor with its own outer nonce.
    bob: TransactionFactory,
    /// Genesis shared by independent execution providers.
    chain: Arc<RethChainSpec>,
    /// Sweep delegate deployed in genesis.
    delegate: Address,
    /// Initial balance covering the cumulative maximum cost of the sequence.
    balance: U256,
}

impl Scenario {
    /// Fund both accounts, leaving Alice initially undelegated.
    fn new() -> Self {
        let alice = TransactionFactory::new();
        let bob = TransactionFactory::new_random();
        let delegate = Address::repeat_byte(0x34);
        let balance = U256::from(1_000_000_000_u64);
        let genesis = test_genesis().extend_accounts([
            (alice.address(), GenesisAccount::default().with_balance(balance)),
            (bob.address(), GenesisAccount::default().with_balance(balance)),
            (delegate, GenesisAccount::default().with_code(Some(Bytes::from_static(SWEEP)))),
        ]);
        Self { alice, bob, chain: Arc::new(genesis.into()), delegate, balance }
    }

    /// Open an independent provider, keeping its directory and task manager alive.
    fn environment(&self) -> eyre::Result<(TempDir, TaskManager, RethEnv)> {
        let directory = TempDir::new()?;
        let tasks = TaskManager::default();
        RethEnv::new_for_temp_chain(self.chain.clone(), directory.path(), &tasks, None)
            .map(|env| (directory, tasks, env))
    }

    /// Bob's valid authorization from initially undelegated Alice at nonce zero.
    fn foreign_installation(&self) -> TransactionSigned {
        let authorization = self.alice.sign_authorization(Authorization {
            chain_id: U256::from(self.chain.chain.id()),
            address: self.delegate,
            nonce: 0,
        });
        self.bob.sign_eip7702(TxEip7702 {
            chain_id: self.chain.chain.id(),
            gas_limit: 100_000,
            max_fee_per_gas: u128::from(MIN_PROTOCOL_BASE_FEE),
            to: self.alice.address(),
            authorization_list: vec![authorization],
            ..Default::default()
        })
    }

    /// Create the funded sequence, transferring a positive value in every transaction.
    fn sequence(&mut self) -> Vec<TransactionSigned> {
        (0..SEQUENCE_LEN)
            .map(|_| {
                self.alice.create_eip1559(
                    self.chain.clone(),
                    Some(21_000),
                    u128::from(MIN_PROTOCOL_BASE_FEE),
                    Some(Address::repeat_byte(0x55)),
                    U256::from(1),
                    Bytes::new(),
                )
            })
            .collect()
    }
}

/// Submit transactions in nonce order through production external ingress.
async fn admit(pool: &WorkerTxPool, transactions: &[TransactionSigned]) -> eyre::Result<()> {
    stream::iter(transactions.iter().map(Ok::<_, eyre::Report>))
        .try_for_each(|transaction| async {
            let recovered = recover_raw_transaction(&transaction.encoded_2718())?;
            pool.add_recovered_transaction_external(recovered).await.map(|_| ()).map_err(Into::into)
        })
        .await
}

/// Pack the production pool using the worker's selection routine.
fn pack(pool: WorkerTxPool) -> Batch {
    build_batch(
        BatchBuilderArgs { pool, beneficiary: Address::ZERO, epoch: 0 },
        0,
        MIN_PROTOCOL_BASE_FEE,
    )
    .batch
}

/// Apply production peer validation before a batch can be certified.
fn validate(env: &RethEnv, batch: &Batch) -> Result<(), BatchValidationError> {
    BatchValidator::new(env.clone(), None, 0, MIN_PROTOCOL_BASE_FEE, 0)
        .validate_batch(batch.clone().seal_slow())
}

/// Execute at an explicit parent; this helper does not stand in for certification.
fn execute(
    env: &RethEnv,
    parent: SealedHeader,
    transactions: Vec<Vec<u8>>,
) -> eyre::Result<ExecutedBlock> {
    let output = consensus_output_for_tests(2, 0, parent.number.saturating_add(1), false);
    let timestamp = parent.timestamp.saturating_add(1);
    let mut payload = TNPayload::new_for_test(parent, &output);
    payload.timestamp = timestamp;
    payload.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
    execute_payload_and_update_canonical_chain(env, payload, transactions)
}

/// The restriction rejects a hidden cross-pool conflict in either admission order.
#[tokio::test]
async fn foreign_authorization_is_rejected_across_independent_pools() -> eyre::Result<()> {
    check_independent_pools(true).await?;
    check_independent_pools(false).await
}

/// Keep both providers at genesis until admission and peer validation finish.
async fn check_independent_pools(foreign_first: bool) -> eyre::Result<()> {
    let mut scenario = Scenario::new();
    let (_alice_dir, _alice_tasks, alice_env) = scenario.environment()?;
    let (_bob_dir, _bob_tasks, bob_env) = scenario.environment()?;
    let alice_pool = alice_env.init_txn_pool(BaseFeeContainer::default())?;
    let bob_pool = bob_env.init_txn_pool(BaseFeeContainer::default())?;
    let foreign = scenario.foreign_installation();
    let sequence = scenario.sequence();
    let maximum_cost =
        U256::from(SEQUENCE_LEN) * (U256::from(21_000 * MIN_PROTOCOL_BASE_FEE) + U256::from(1));
    assert!(scenario.balance >= maximum_cost);
    assert!(alice_env.account_code(&scenario.alice.address())?.is_none());

    if !foreign_first {
        admit(&alice_pool, &sequence).await?;
    }
    let error = bob_pool
        .add_recovered_transaction_external(recover_raw_transaction(&foreign.encoded_2718())?)
        .await
        .err()
        .ok_or_else(|| eyre::eyre!("foreign authorization entered Bob's pool"))?;
    assert!(error.to_string().contains("authorizations must belong to the transaction sender"));
    if foreign_first {
        admit(&alice_pool, &sequence).await?;
    }
    assert_eq!(alice_pool.pool_size().pending, SEQUENCE_LEN);
    assert_eq!(bob_pool.pool_size().pending, 0);

    let batch = pack(alice_pool);
    assert_eq!(batch.transactions.len(), SEQUENCE_LEN);
    validate(&bob_env, &batch)?;
    let foreign_batch = Batch {
        transactions: vec![foreign.encoded_2718()],
        base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
        ..Default::default()
    };
    assert!(matches!(validate(&alice_env, &foreign_batch),
        Err(BatchValidationError::NonSelfAuthorization { hash }) if hash == *foreign.hash()));

    // A restored or synthetic pool cannot bypass the builder's matching rule.
    let bypass_pool = TestPool::new(&foreign_batch.transactions);
    let removed = bypass_pool.removed_unsupported_handle();
    let output = build_batch(
        BatchBuilderArgs { pool: bypass_pool, beneficiary: Address::ZERO, epoch: 0 },
        0,
        MIN_PROTOCOL_BASE_FEE,
    );
    assert!(output.batch.transactions.is_empty());
    assert_eq!(
        removed.lock().map_err(|_| eyre::eyre!("poisoned eviction record"))?.as_slice(),
        &[*foreign.hash()]
    );

    let block = execute(&alice_env, scenario.chain.sealed_genesis_header(), batch.transactions)?;
    assert_eq!(block.recovered_block.body().transactions.len(), SEQUENCE_LEN);
    let alice = alice_env
        .retrieve_account(&scenario.alice.address())?
        .ok_or_else(|| eyre::eyre!("missing funded Alice"))?;
    assert_eq!(alice.nonce, u64::try_from(SEQUENCE_LEN)?);
    assert!(alice.balance < scenario.balance);
    Ok(())
}

/// Alice installs delegation herself, then Bob's ordinary sponsored call passes every gate.
#[tokio::test]
async fn self_installation_then_sponsored_call_executes() -> eyre::Result<()> {
    let mut scenario = Scenario::new();
    let (_alice_dir, _alice_tasks, alice_env) = scenario.environment()?;
    let (_bob_dir, _bob_tasks, bob_env) = scenario.environment()?;
    let authorization = scenario.alice.sign_authorization(Authorization {
        chain_id: U256::from(scenario.chain.chain.id()),
        address: scenario.delegate,
        nonce: 1,
    });
    let installation = scenario.alice.sign_eip7702(TxEip7702 {
        chain_id: scenario.chain.chain.id(),
        gas_limit: 100_000,
        max_fee_per_gas: u128::from(MIN_PROTOCOL_BASE_FEE),
        to: Address::ZERO,
        authorization_list: vec![authorization],
        ..Default::default()
    });
    let pool = alice_env.init_txn_pool(BaseFeeContainer::default())?;
    admit(&pool, &[installation]).await?;
    let batch = pack(pool);
    assert_eq!(batch.transactions.len(), 1);
    validate(&bob_env, &batch)?;
    let installed =
        execute(&alice_env, scenario.chain.sealed_genesis_header(), batch.transactions.clone())?;
    execute(&bob_env, scenario.chain.sealed_genesis_header(), batch.transactions)?;
    let parent = installed.recovered_block.clone_sealed_header();
    let expected_code = [vec![0xef, 0x01, 0x00], scenario.delegate.to_vec()].concat();
    assert_eq!(alice_env.account_code(&scenario.alice.address())?, Some(expected_code.into()));
    let before = alice_env
        .retrieve_account(&scenario.alice.address())?
        .ok_or_else(|| eyre::eyre!("missing delegated Alice"))?;
    assert_eq!(before.nonce, 2);

    let sponsor_pool = bob_env.init_txn_pool(BaseFeeContainer::default())?;
    let call = scenario.bob.create_eip1559(
        scenario.chain.clone(),
        Some(100_000),
        u128::from(MIN_PROTOCOL_BASE_FEE),
        Some(scenario.alice.address()),
        U256::ZERO,
        Bytes::new(),
    );
    admit(&sponsor_pool, &[call]).await?;
    let sponsored = pack(sponsor_pool);
    assert_eq!(sponsored.transactions.len(), 1);
    validate(&alice_env, &sponsored)?;
    let executed = execute(&alice_env, parent, sponsored.transactions)?;
    assert_eq!(executed.recovered_block.body().transactions.len(), 1);
    assert_eq!(
        alice_env
            .retrieve_account(&scenario.alice.address())?
            .ok_or_else(|| eyre::eyre!("missing Alice after call"))?
            .balance,
        U256::ZERO
    );
    let bob = alice_env
        .retrieve_account(&scenario.bob.address())?
        .ok_or_else(|| eyre::eyre!("missing Bob after call"))?;
    assert_eq!(bob.nonce, 1);
    assert!(bob.balance > scenario.balance, "the sponsor receives the sweep and pays its fee");
    Ok(())
}

/// Read the nonce and balance of a funded account from the provider's canonical state.
fn account(env: &RethEnv, address: Address) -> eyre::Result<(u64, U256)> {
    env.retrieve_account(&address)?
        .map(|account| (account.nonce, account.balance))
        .ok_or_else(|| eyre::eyre!("missing account {address}"))
}

/// Encode signed transactions for direct execution.
fn encode(transactions: &[TransactionSigned]) -> Vec<Vec<u8>> {
    transactions.iter().map(|transaction| transaction.encoded_2718()).collect()
}

/// Bob's installation executes first. It sweeps Alice, then every transaction in her
/// sequence is skipped and no fee is charged for any of them.
#[tokio::test]
async fn bob_first_execution_skips_alice_sequence_without_fees() -> eyre::Result<()> {
    let mut scenario = Scenario::new();
    let (_dir, _tasks, env) = scenario.environment()?;
    let alice = scenario.alice.address();
    let bob = scenario.bob.address();
    let foreign = scenario.foreign_installation();
    let sequence = encode(&scenario.sequence());

    let installed = execute(
        &env,
        scenario.chain.sealed_genesis_header(),
        encode(std::slice::from_ref(&foreign)),
    )?;
    assert_eq!(installed.recovered_block.body().transactions.len(), 1);
    assert_eq!(installed.execution_output.result.receipts.len(), 1);
    let expected_code = [vec![0xef, 0x01, 0x00], scenario.delegate.to_vec()].concat();
    assert_eq!(env.account_code(&alice)?, Some(expected_code.into()));
    let (alice_nonce, alice_balance) = account(&env, alice)?;
    assert_eq!(alice_nonce, 1);
    assert_eq!(alice_balance, U256::ZERO, "the sweep drains Alice");
    let (bob_nonce, bob_balance) = account(&env, bob)?;
    assert_eq!(bob_nonce, 1);
    assert!(bob_balance > scenario.balance, "Bob receives the sweep and pays only its fee");

    let parent = installed.recovered_block.clone_sealed_header();
    let skipped = execute(&env, parent, sequence)?;
    assert!(skipped.recovered_block.body().transactions.is_empty());
    assert!(skipped.execution_output.result.receipts.is_empty());
    assert_eq!(skipped.execution_output.result.gas_used, 0);
    assert_eq!(account(&env, alice)?, (1, alice_balance), "no nonce change and no fee");
    Ok(())
}

/// Alice's sequence executes first. Bob's later installation carries a stale tuple, so it
/// is inert: Bob pays for the transaction and Alice's account does not change.
#[tokio::test]
async fn alice_first_execution_leaves_the_foreign_tuple_inert() -> eyre::Result<()> {
    let mut scenario = Scenario::new();
    let (_dir, _tasks, env) = scenario.environment()?;
    let alice = scenario.alice.address();
    let bob = scenario.bob.address();
    let foreign = scenario.foreign_installation();
    let sequence = encode(&scenario.sequence());
    let sequence_len = u64::try_from(SEQUENCE_LEN)?;

    let executed = execute(&env, scenario.chain.sealed_genesis_header(), sequence)?;
    assert_eq!(executed.recovered_block.body().transactions.len(), SEQUENCE_LEN);
    let receipts = &executed.execution_output.result.receipts;
    assert_eq!(receipts.len(), SEQUENCE_LEN);
    assert!(receipts.iter().all(|receipt| receipt.success));
    let (alice_nonce, alice_balance) = account(&env, alice)?;
    assert_eq!(alice_nonce, sequence_len);

    let parent = executed.recovered_block.clone_sealed_header();
    let inert = execute(&env, parent, encode(std::slice::from_ref(&foreign)))?;
    assert_eq!(inert.recovered_block.body().transactions.len(), 1);
    let receipts = &inert.execution_output.result.receipts;
    assert_eq!(receipts.len(), 1);
    assert!(receipts.iter().all(|receipt| receipt.success));
    assert!(env.account_code(&alice)?.is_none(), "the stale tuple installs nothing");
    assert_eq!(account(&env, alice)?, (sequence_len, alice_balance));
    let (bob_nonce, bob_balance) = account(&env, bob)?;
    assert_eq!(bob_nonce, 1);
    assert!(bob_balance < scenario.balance, "Bob pays the fee for his own transaction");
    Ok(())
}

/// After Alice installs delegation herself, her next ordinary transaction still passes
/// admission, packing and peer validation.
#[tokio::test]
async fn delegated_sender_ordinary_transaction_stays_admissible() -> eyre::Result<()> {
    let mut scenario = Scenario::new();
    let (_alice_dir, _alice_tasks, alice_env) = scenario.environment()?;
    let (_bob_dir, _bob_tasks, bob_env) = scenario.environment()?;
    let authorization = scenario.alice.sign_authorization(Authorization {
        chain_id: U256::from(scenario.chain.chain.id()),
        address: scenario.delegate,
        nonce: 1,
    });
    let installation = scenario.alice.sign_eip7702(TxEip7702 {
        chain_id: scenario.chain.chain.id(),
        gas_limit: 100_000,
        max_fee_per_gas: u128::from(MIN_PROTOCOL_BASE_FEE),
        to: Address::ZERO,
        authorization_list: vec![authorization],
        ..Default::default()
    });
    scenario.alice.set_nonce(2);
    let pool = alice_env.init_txn_pool(BaseFeeContainer::default())?;
    admit(&pool, &[installation]).await?;
    let batch = pack(pool);
    assert_eq!(batch.transactions.len(), 1);
    validate(&bob_env, &batch)?;
    execute(&alice_env, scenario.chain.sealed_genesis_header(), batch.transactions.clone())?;
    execute(&bob_env, scenario.chain.sealed_genesis_header(), batch.transactions)?;
    let expected_code = [vec![0xef, 0x01, 0x00], scenario.delegate.to_vec()].concat();
    assert_eq!(alice_env.account_code(&scenario.alice.address())?, Some(expected_code.into()));
    assert_eq!(account(&alice_env, scenario.alice.address())?.0, 2);

    let ordinary = scenario.alice.create_eip1559(
        scenario.chain.clone(),
        Some(21_000),
        u128::from(MIN_PROTOCOL_BASE_FEE),
        Some(Address::repeat_byte(0x55)),
        U256::from(1),
        Bytes::new(),
    );
    let pool = alice_env.init_txn_pool(BaseFeeContainer::default())?;
    admit(&pool, std::slice::from_ref(&ordinary)).await?;
    assert_eq!(pool.pool_size().pending, 1);
    let batch = pack(pool);
    assert_eq!(batch.transactions, vec![ordinary.encoded_2718()]);
    validate(&bob_env, &batch)?;
    Ok(())
}
