//! Integration coverage for independent workers sharing one execution engine.

use super::*;
use futures::{StreamExt as _, TryStreamExt as _};
use tn_reth::{payload::BuildArguments, test_utils::batch_with_transactions};
use tn_types::{test_chain_spec_arc, CertifiedBatch};

/// Interleaved batches preserve worker IDs, gas accounting, and fees, including an idle worker.
#[tokio::test]
async fn test_multi_worker_execution_isolation() -> eyre::Result<()> {
    let temp_dir = TempDir::with_prefix("multi_worker_execution")?;
    let worker_fee =
        |worker_id: WorkerId| MIN_PROTOCOL_BASE_FEE + 1000 * (u64::from(worker_id) + 1);
    let batches: Vec<_> = [(1, 2), (0, 1), (1, 3)]
        .into_iter()
        .map(|(worker_id, transactions)| Batch {
            base_fee_per_gas: worker_fee(worker_id),
            ..batch_with_transactions(test_chain_spec_arc(), transactions, worker_id)
        })
        .collect();
    let genesis = test_genesis_with_consensus_registry_and_workers(
        4,
        (0..3).map(|worker_id| (1, worker_fee(worker_id))).collect(),
    );
    let (genesis, _, _) = seeded_genesis_from_random_batches(genesis, batches.iter());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let accumulator = GasAccumulator::new(3);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        temp_dir.path(),
        Some(accumulator.clone()),
    )?;
    let task_manager = TaskManager::default();

    futures::stream::iter(0..3)
        .then(|worker_id| {
            let execution_node = &execution_node;
            let accumulator = &accumulator;
            let task_manager = &task_manager;
            async move {
                accumulator.base_fee(worker_id).set_base_fee(worker_fee(worker_id));
                execution_node
                    .initialize_worker_components(
                        worker_id,
                        WorkerNetworkHandle::new_for_test(task_manager.get_spawner()),
                        NoopEngineToPrimary,
                        accumulator.base_fee(worker_id),
                        accumulator.worker_base_fee(worker_id),
                    )
                    .await?;
                assert_eq!(accumulator.get_values(worker_id), (0, 0, 0));
                eyre::Ok(())
            }
        })
        .try_collect::<()>()
        .await?;

    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let authorities = committee.authorities();
    let authority = authorities.first().ok_or_else(|| eyre::eyre!("authority"))?;
    let producer = authority.execution_address();
    let mut leader = Certificate::default();
    leader.update_header_author_for_test(authority.id());
    leader.update_header_round_for_test(1);
    leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    accumulator.rewards_counter().set_committee(committee);
    let sub_dag = CommittedSubDag::new(
        vec![leader.clone()],
        leader,
        1,
        ReputationScores::default(),
        None,
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );
    let output = ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        1,
        false,
        batches.iter().map(Batch::digest).collect(),
        vec![CertifiedBatch { address: producer, batches: batches.clone() }],
    );
    let reth_env = execution_node.get_reth_env().await;
    let args = BuildArguments::new(reth_env.clone(), output, chain.sealed_genesis_header());
    let (engine_update_tx, mut engine_update_rx) = mpsc::channel(1);
    let execution_accumulator = accumulator.clone();
    let final_header = tokio::task::spawn_blocking(move || {
        tn_engine::execute_consensus_output(
            args,
            execution_accumulator,
            tn_types::repack_monitor::RepackMonitor::default(),
            engine_update_tx,
        )
    })
    .await??;
    assert_eq!(final_header.number, 3);
    assert!(engine_update_rx.try_recv().is_ok(), "execution must publish its canonical update");

    (0_u64..).zip(batches.iter()).try_for_each(|(index, batch)| {
        let header = reth_env
            .sealed_header_by_number(index + 1)?
            .ok_or_else(|| eyre::eyre!("missing block {}", index + 1))?;
        assert_eq!(
            header.difficulty,
            U256::from((index << 16) | u64::from(batch.worker_id)),
            "block attribution must retain both the batch index and worker ID",
        );
        assert_eq!(header.base_fee_per_gas, Some(worker_fee(batch.worker_id)));
        assert_eq!(header.gas_used, u64::try_from(batch.transactions.len())? * 21_000);
        eyre::Ok(())
    })?;

    futures::stream::iter([(0, 1, 21_000), (1, 2, 105_000), (2, 0, 0)])
        .then(|(worker_id, expected_blocks, expected_gas)| {
            let execution_node = &execution_node;
            let accumulator = &accumulator;
            async move {
                let (blocks, gas_used, _) = accumulator.get_values(worker_id);
                assert_eq!(blocks, expected_blocks, "worker {worker_id} block count");
                assert_eq!(gas_used, expected_gas, "worker {worker_id} gas usage");
                assert_eq!(accumulator.base_fee(worker_id).base_fee(), worker_fee(worker_id));
                assert_eq!(
                    execution_node
                        .get_worker_transaction_pool(&worker_id)
                        .await?
                        .block_info()
                        .pending_basefee,
                    worker_fee(worker_id),
                    "worker {worker_id} pool fee",
                );
                eyre::Ok(())
            }
        })
        .try_collect::<()>()
        .await
}
