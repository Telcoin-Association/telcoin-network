//! Controlled schedules for duplicate amplification remaining after peer-batch deferral.

use futures_util::future::join_all;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use std::{collections::VecDeque, io, sync::Arc};
use tempfile::TempDir;
use tn_batch_builder::test_utils::build_test_batch;
use tn_batch_validator::BatchValidator;
use tn_engine::execute_consensus_output;
use tn_reth::{payload::BuildArguments, test_utils::TransactionFactory, RethChainSpec, RethEnv};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    max_batch_gas, test_genesis, Address, BatchBuilderArgs, BatchValidation, Bytes, Certificate,
    CertifiedBatch, CommittedSubDag, ConsensusHeaderDigest, ConsensusOutput, Encodable2718,
    EpochSeedChainValue, ReputationScores, TaskManager, MIN_PROTOCOL_BASE_FEE, U256,
};

/// Two builders can seal one copy each before either peer validation records the transaction.
#[tokio::test]
async fn concurrent_builders_execute_one_copy_and_skip_the_other() -> eyre::Result<()> {
    duplicate_build_schedule(1).await
}

/// The same schedule wastes almost a full batch's gas capacity when repeated for simple transfers.
#[tokio::test]
async fn concurrent_builders_duplicate_a_full_batch_of_transfers() -> eyre::Result<()> {
    duplicate_build_schedule(usize::try_from(max_batch_gas(0) / 21_000)?).await
}

/// Run both selection steps before peer validation, then execute the distinct producer batches.
///
/// No builder timers or propagation delays are simulated. The ordering of these calls fixes the
/// race schedule independently of machine speed, and execution uses the production engine path.
async fn duplicate_build_schedule(transaction_count: usize) -> eyre::Result<()> {
    let tmp_dir = TempDir::new()?;
    let task_manager = TaskManager::default();
    let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
    let reth_env = RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
    let first_pool = reth_env.init_txn_pool(BaseFeeContainer::default())?;
    let second_pool = reth_env.init_txn_pool(BaseFeeContainer::default())?;
    let mut factory = TransactionFactory::new();
    let sender = factory.address();
    let before = reth_env
        .retrieve_account(&sender)?
        .ok_or_else(|| io::Error::other("funded sender missing from genesis"))?;
    let transactions: Vec<_> = (0..transaction_count)
        .map(|_| {
            factory.create_eip1559(
                chain.clone(),
                Some(21_000),
                u128::from(MIN_PROTOCOL_BASE_FEE),
                Some(Address::ZERO),
                U256::from(1),
                Bytes::new(),
            )
        })
        .collect();
    join_all(transactions.iter().map(|tx| async {
        assert_eq!(factory.submit_tx_to_pool(tx.clone(), first_pool.clone()).await, *tx.hash());
        assert_eq!(factory.submit_tx_to_pool(tx.clone(), second_pool.clone()).await, *tx.hash());
    }))
    .await;
    let encoded: Vec<_> = transactions.iter().map(Encodable2718::encoded_2718).collect();
    let first = build_test_batch(
        BatchBuilderArgs {
            pool: first_pool.clone(),
            beneficiary: Address::from([1; 20]),
            epoch: 0,
        },
        0,
        MIN_PROTOCOL_BASE_FEE,
    );
    let second = build_test_batch(
        BatchBuilderArgs {
            pool: second_pool.clone(),
            beneficiary: Address::from([2; 20]),
            epoch: 0,
        },
        0,
        MIN_PROTOCOL_BASE_FEE,
    );
    assert_eq!(first.transactions(), &encoded);
    assert_eq!(second.transactions(), &encoded);
    assert_ne!(first.digest(), second.digest());

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let (first_validator, second_validator) = metrics::with_local_recorder(&recorder, || {
        (
            BatchValidator::new(reth_env.clone(), Some(first_pool), 0, MIN_PROTOCOL_BASE_FEE, 0),
            BatchValidator::new(reth_env.clone(), Some(second_pool), 0, MIN_PROTOCOL_BASE_FEE, 0),
        )
    });
    first_validator.validate_batch(second.clone().seal_slow())?;
    second_validator.validate_batch(first.clone().seal_slow())?;
    let retained = snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
        (key.key().name() == "tn_peer_batch.retained_hashes"
            && key.key().labels().any(|label| label.key() == "worker" && label.value() == "0"))
        .then_some(value)
    });
    let expected_retained = f64::from(u32::try_from(transaction_count.saturating_mul(2))?);
    assert!(matches!(retained, Some(DebugValue::Gauge(value)) if value.0 == expected_retained));

    let batch_digests = VecDeque::from([first.digest(), second.digest()]);
    let batches = [first, second]
        .into_iter()
        .map(|batch| CertifiedBatch { address: batch.beneficiary, batches: vec![batch] })
        .collect();
    let output = ConsensusOutput::new(
        CommittedSubDag::new(
            vec![Certificate::default(); 2],
            Certificate::default(),
            0,
            ReputationScores::default(),
            None,
            EpochSeedChainValue::genesis_placeholder(),
        ),
        ConsensusHeaderDigest::default(),
        0,
        false,
        batch_digests,
        batches,
    );
    let args = BuildArguments::new(reth_env.clone(), output, chain.sealed_genesis_header());
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(16);
    let final_header = tokio::task::spawn_blocking(move || {
        metrics::with_local_recorder(&recorder, || {
            execute_consensus_output(
                args,
                GasAccumulator::default(),
                tn_types::repack_monitor::RepackMonitor::default(),
                engine_update_tx,
            )
        })
    })
    .await??;
    let skipped = snapshotter.snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
        (key.key().name() == "tn_reth.invalid_txs_skipped_total"
            && key
                .key()
                .labels()
                .any(|label| label.key() == "reason" && label.value() == "nonce_too_low"))
        .then_some(value)
    });
    let expected_skipped = u64::try_from(transaction_count)?;
    assert!(matches!(skipped, Some(DebugValue::Counter(value)) if value == expected_skipped));
    assert_eq!(final_header.number, 2);
    let first_block = reth_env
        .sealed_block_by_number(1)?
        .ok_or_else(|| io::Error::other("first execution block missing"))?;
    let second_block = reth_env
        .sealed_block_by_number(2)?
        .ok_or_else(|| io::Error::other("second execution block missing"))?;
    let expected_gas = u64::try_from(transaction_count)?.saturating_mul(21_000);
    assert_eq!(first_block.body().transactions.len(), transaction_count);
    assert!(second_block.body().transactions.is_empty());
    assert_eq!(first_block.header().gas_used, expected_gas);
    assert_eq!(second_block.header().gas_used, 0);
    let after = reth_env
        .retrieve_account(&sender)?
        .ok_or_else(|| io::Error::other("sender missing after execution"))?;
    assert_eq!(after.nonce, u64::try_from(transaction_count)?);
    let expected_debit = U256::from(transaction_count)
        + U256::from(expected_gas) * U256::from(MIN_PROTOCOL_BASE_FEE);
    assert_eq!(
        after.balance,
        before
            .balance
            .checked_sub(expected_debit)
            .ok_or_else(|| io::Error::other("fixture balance cannot cover transfers and fees"))?
    );
    Ok(())
}
