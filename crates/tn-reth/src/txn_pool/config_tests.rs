//! Regression coverage for operator transaction-pool settings (issue #1340).

use super::*;
use crate::{test_utils::TransactionFactory, RethChainSpec, RethEnv};
use clap::Parser;
use reth_transaction_pool::{error::PoolErrorKind, TransactionEvent};
use tempfile::TempDir;
use tn_types::{test_genesis, Bytes, Encodable2718 as _, GenesisAccount, TaskManager};

/// Parse the same flattened reth arguments used by TN's node command.
#[derive(Parser)]
struct PoolArgs {
    /// Operator settings delivered to the production pool constructor.
    #[command(flatten)]
    txpool: reth::args::TxPoolArgs,
}

/// Construct the production pool over funded accounts, retaining its backing environment.
fn configured_pool(
    senders: &[Address],
    directory: &TempDir,
    tasks: &TaskManager,
    args: &[&str],
) -> eyre::Result<(RethEnv, WorkerTxPool)> {
    let genesis = test_genesis().extend_accounts(
        senders.iter().map(|sender| (*sender, GenesisAccount::default().with_balance(U256::MAX))),
    );
    let env = RethEnv::new_for_temp_chain(Arc::new(genesis.into()), directory.path(), tasks, None)?;
    let mut config = env.node_config().clone();
    config.txpool = PoolArgs::try_parse_from(args)?.txpool;
    WorkerTxPool::new(
        &config,
        env.get_task_spawner(),
        env.blockchain_provider(),
        env.evm_config(),
        BaseFeeContainer::default(),
    )
    .map(|pool| (env, pool))
}

/// Create a valid transfer whose gas, fee and input exercise admission or parking policy.
fn transaction(
    factory: &mut TransactionFactory,
    gas_limit: u64,
    max_fee: u128,
    input: Bytes,
) -> eyre::Result<EthPooledTransaction> {
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let signed = factory.create_eip1559(
        chain,
        Some(gas_limit),
        max_fee,
        Some(Address::ZERO),
        U256::ZERO,
        input,
    );
    recover_pooled_transaction(&signed.encoded_2718())
}

/// A parsed byte limit reaches the validator, including the exact encoded-size boundary.
#[tokio::test]
async fn byte_limit_reaches_validator() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let mut factory = TransactionFactory::new();
    let exact = transaction(&mut factory, 100_000, 100, Bytes::new())?;
    let limit = exact.encoded_length();
    let limit_arg = limit.to_string();
    let (_env, pool) = configured_pool(
        &[factory.address()],
        &directory,
        &tasks,
        &["tn", "--txpool.max-tx-input-bytes", &limit_arg],
    )?;
    pool.0.add_transaction(TransactionOrigin::External, exact).await?;
    let oversized = transaction(&mut factory, 100_000, 100, Bytes::from(vec![1; 256]))?;
    let result = pool.0.add_transaction(TransactionOrigin::External, oversized).await;
    assert!(matches!(result, Err(PoolError {
        kind: PoolErrorKind::InvalidTransaction(InvalidPoolTransactionError::OversizedData {
            limit: actual, ..
        }), ..
    }) if actual == limit));
    Ok(())
}

/// The per-transaction gas setting accepts the boundary and rejects one gas above it.
#[tokio::test]
async fn gas_limit_reaches_validator() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let mut factory = TransactionFactory::new();
    let (_env, pool) = configured_pool(
        &[factory.address()],
        &directory,
        &tasks,
        &["tn", "--txpool.max-tx-gas", "21000"],
    )?;
    let exact = transaction(&mut factory, 21_000, 100, Bytes::new())?;
    pool.0.add_transaction(TransactionOrigin::External, exact).await?;
    let oversized = transaction(&mut factory, 21_001, 100, Bytes::new())?;
    let result = pool.0.add_transaction(TransactionOrigin::External, oversized).await;
    assert!(matches!(
        result,
        Err(PoolError {
            kind: PoolErrorKind::InvalidTransaction(
                InvalidPoolTransactionError::MaxTxGasLimitExceeded(21_001, 21_000)
            ),
            ..
        })
    ));
    Ok(())
}

/// An unsupported fee floor fails pool construction with an actionable startup error.
#[tokio::test]
async fn priority_fee_is_rejected_at_startup() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let result =
        configured_pool(&[], &directory, &tasks, &["tn", "--txpool.minimum-priority-fee", "1"]);
    assert_eq!(
        result.err().and_then(|error| error.downcast::<TxPoolConfigError>().ok()),
        Some(TxPoolConfigError::MinimumPriorityFee)
    );
    Ok(())
}

/// A pool cannot admit transactions larger than the batch protocol can carry.
#[tokio::test]
async fn byte_limit_cannot_exceed_batch_limit() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let maximum = max_batch_size(0);
    let configured = maximum + 1;
    let limit_arg = configured.to_string();
    let result = configured_pool(
        &[],
        &directory,
        &tasks,
        &["tn", "--txpool.max-tx-input-bytes", &limit_arg],
    );
    assert_eq!(
        result.err().and_then(|error| error.downcast::<TxPoolConfigError>().ok()),
        Some(TxPoolConfigError::InputLimitExceedsBatch { configured, maximum })
    );
    let boundary_directory = TempDir::new()?;
    let limit_arg = maximum.to_string();
    configured_pool(
        &[],
        &boundary_directory,
        &tasks,
        &["tn", "--txpool.max-tx-input-bytes", &limit_arg],
    )
    .map(|_| ())
}

/// Expiry covers nonce gaps and underpriced transactions, while pending transfers survive.
#[tokio::test]
async fn lifetime_expires_parked_transactions_only() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let mut factory = TransactionFactory::new();
    let mut underpriced = TransactionFactory::new_random();
    let (_env, pool) = configured_pool(
        &[factory.address(), underpriced.address()],
        &directory,
        &tasks,
        &["tn", "--txpool.lifetime", "60"],
    )?;
    let mut info = pool.0.block_info();
    info.pending_basefee = 50;
    pool.0.set_block_info(info);
    let pending = transaction(&mut factory, 21_000, 100, Bytes::new())?;
    let pending_hash = pool.0.add_transaction(TransactionOrigin::External, pending).await?.hash;
    factory.set_nonce(2);
    let queued = transaction(&mut factory, 21_000, 100, Bytes::new())?;
    let queued_hash = pool.0.add_transaction(TransactionOrigin::External, queued).await?.hash;
    let basefee = transaction(&mut underpriced, 21_000, 7, Bytes::new())?;
    let basefee_hash = pool.0.add_transaction(TransactionOrigin::External, basefee).await?.hash;
    assert_eq!(pool.pool_size().pending, 1);
    assert_eq!(pool.pool_size().queued, 1);
    assert_eq!(pool.pool_size().basefee, 1);
    let queued_tx =
        pool.get(&queued_hash).ok_or_else(|| eyre::eyre!("queued transaction missing"))?;
    let basefee_tx =
        pool.get(&basefee_hash).ok_or_else(|| eyre::eyre!("basefee transaction missing"))?;
    pool.evict_stale_transactions(queued_tx.timestamp + Duration::from_secs(59));
    assert!(pool.get(&queued_hash).is_some());
    pool.evict_stale_transactions(queued_tx.timestamp + Duration::from_secs(60));
    assert!(pool.get(&queued_hash).is_none());
    pool.evict_stale_transactions(basefee_tx.timestamp + Duration::from_secs(60));
    assert!(pool.get(&basefee_hash).is_none());
    assert!(pool.get(&pending_hash).is_some());
    Ok(())
}

/// Local and private origins retain reth's expiry exemption unless nolocals is set.
#[tokio::test]
async fn lifetime_honors_local_exemptions() -> eyre::Result<()> {
    use futures::TryStreamExt as _;

    futures::stream::iter([
        (TransactionOrigin::Local, false),
        (TransactionOrigin::Private, false),
        (TransactionOrigin::Local, true),
        (TransactionOrigin::Private, true),
    ])
    .map(Ok)
    .try_for_each(|(origin, no_exemptions)| async move {
        let directory = TempDir::new()?;
        let tasks = TaskManager::default();
        let mut factory = TransactionFactory::new();
        factory.set_nonce(1);
        let args: &[&str] = if no_exemptions {
            &["tn", "--txpool.lifetime", "60", "--txpool.nolocals"]
        } else {
            &["tn", "--txpool.lifetime", "60"]
        };
        let (_env, pool) = configured_pool(&[factory.address()], &directory, &tasks, args)?;
        let tx = transaction(&mut factory, 21_000, 100, Bytes::new())?;
        let hash = pool.0.add_transaction(origin, tx).await?.hash;
        assert_eq!(pool.pool_size().queued, 1);
        let pooled = pool.get(&hash).ok_or_else(|| eyre::eyre!("queued transaction missing"))?;
        pool.evict_stale_transactions(pooled.timestamp + Duration::from_secs(60));
        assert_eq!(pool.get(&hash).is_none(), no_exemptions);
        Ok::<_, eyre::Report>(())
    })
    .await
}

/// A zero lifetime expires on a timer without canonical notifications or wall-clock sleeps.
#[tokio::test(start_paused = true)]
async fn lifetime_timer_runs_on_an_idle_chain() -> eyre::Result<()> {
    let directory = TempDir::new()?;
    let tasks = TaskManager::default();
    let mut factory = TransactionFactory::new();
    factory.set_nonce(1);
    let (_env, pool) = configured_pool(
        &[factory.address()],
        &directory,
        &tasks,
        &["tn", "--txpool.lifetime", "0"],
    )?;
    let tx = transaction(&mut factory, 21_000, 100, Bytes::new())?;
    let events = pool.0.add_transaction_and_subscribe(TransactionOrigin::External, tx).await?;
    let discarded =
        events.filter(|event| futures::future::ready(matches!(event, TransactionEvent::Discarded)));
    tokio::pin!(discarded);
    let event = tokio::time::timeout(Duration::from_secs(1), discarded.next()).await?;
    assert!(matches!(event, Some(TransactionEvent::Discarded)));
    assert_eq!(pool.pool_size().queued, 0);
    Ok(())
}
