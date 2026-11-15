//! The batch positions the engine hands the block writer with each consensus output's blocks
//! (`RethEnv::saved_tx_positions` records what the writer received), and the batches it hands the
//! decoded-batch cache after the persist (`RethEnv::cache_persisted_batches`).

use crate::IT_TEST_GUARD;
use std::sync::{Arc, Mutex, PoisonError};
use tempfile::TempDir;
use tn_engine::{execute_consensus_output, TnEngineError, PERSIST_OUTPUT_ATTEMPTS};
use tn_reth::{
    payload::BuildArguments,
    test_utils::{
        batch_with_transactions, create_committee_from_state, seeded_genesis_from_random_batches,
        test_genesis_with_consensus_registry,
    },
    PersistedBatchSink, PersistedBatches, RethChainSpec, RethEnv,
};
use tn_test_utils::default_test_execution_node;
use tn_types::{
    gas_accumulator::GasAccumulator, now, test_chain_spec_arc, Batch, Certificate, CertifiedBatch,
    CommittedSubDag, ConsensusHeaderDigest, ConsensusOutput, ReputationScores, SealedHeader, B256,
};

/// Bytes that do not decode as a transaction, so signer recovery drops them.
const UNDECODABLE_TX: &[u8] = &[0xde, 0xad, 0xbe, 0xef];

/// Records each hand-over to the decoded-batch cache: its digests and batches.
#[derive(Debug, Default)]
struct RecordingSink(Mutex<Vec<Vec<(B256, Batch)>>>);

impl RecordingSink {
    /// The hand-overs so far, oldest first.
    fn handed(&self) -> Vec<Vec<(B256, Batch)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }
}

impl PersistedBatchSink for RecordingSink {
    fn batches_persisted(&self, batches: PersistedBatches) {
        let batches = batches.iter().map(|(digest, batch)| (digest, batch.clone())).collect();
        self.0.lock().unwrap_or_else(PoisonError::into_inner).push(batches);
    }
}

/// One consensus output to execute.
struct Plan {
    /// The output's batches, all in one certificate.
    batches: Vec<Batch>,
    /// The batch digests the output lists, one per batch.
    digests: Vec<B256>,
    /// Whether the output closes the epoch.
    close_epoch: bool,
    /// Pre-commit persist faults to inject before execution.
    persist_faults: u32,
}

impl Plan {
    /// `batches`, each listed under its own digest.
    fn aligned(batches: Vec<Batch>) -> Self {
        let digests = batches.iter().map(Batch::digest).collect();
        Self { batches, digests, close_epoch: false, persist_faults: 0 }
    }
}

/// An output executed by [`execute`].
struct Run {
    /// What `execute_consensus_output` returned.
    result: Result<SealedHeader, TnEngineError>,
    /// The node's execution environment.
    reth_env: RethEnv,
    /// The decoded-batch cache's stand-in, installed before execution.
    sink: Arc<RecordingSink>,
    /// The node's datadir, kept for the run's lifetime.
    _tmp_dir: TempDir,
}

/// Execute `plan`'s output through `execute_consensus_output` on a fresh node whose genesis funds
/// the senders of `funded` (decodable transactions only), with a [`RecordingSink`] installed.
async fn execute(funded: &[&Batch], plan: Plan) -> eyre::Result<Run> {
    let tmp_dir = TempDir::new()?;
    let genesis = test_genesis_with_consensus_registry(4);
    let (genesis, ..) = seeded_genesis_from_random_batches(genesis, funded.iter().copied());
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let gas_accumulator = GasAccumulator::new(1);
    let execution_node = default_test_execution_node(
        Some(chain.clone()),
        None,
        tmp_dir.path(),
        Some(gas_accumulator.clone()),
    )?;
    let committee =
        create_committee_from_state(execution_node.epoch_state_from_canonical_tip().await?).await?;
    let leader_id = committee.authorities().first().expect("first authority").id();
    let producer = committee.authority(&leader_id).expect("leader").execution_address();
    gas_accumulator.rewards_counter().set_committee(committee);

    let mut leader = Certificate::default();
    leader.update_header_author_for_test(leader_id);
    leader.update_header_round_for_test(1);
    // genesis is stamped with `now()`, so the leader never predates the parent block
    leader.update_header_created_at_for_test(now());
    let sub_dag = CommittedSubDag::new(
        vec![Certificate::default(), leader.clone()],
        leader,
        1,
        ReputationScores::default(),
        None,
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );
    let certificates = if plan.batches.is_empty() {
        Vec::new()
    } else {
        vec![CertifiedBatch { address: producer, batches: plan.batches }]
    };
    let output = ConsensusOutput::new(
        sub_dag,
        ConsensusHeaderDigest::default(),
        0,
        plan.close_epoch,
        plan.digests.into(),
        certificates,
    );

    let reth_env = execution_node.get_reth_env().await;
    let sink = Arc::new(RecordingSink::default());
    reth_env.set_persisted_batch_sink(sink.clone())?;
    reth_env.inject_persist_provider_faults(plan.persist_faults);
    let (engine_update_tx, _engine_update_rx) = tokio::sync::mpsc::channel(64);
    let args = BuildArguments::new(reth_env.clone(), output, chain.sealed_genesis_header());
    let result = tokio::task::spawn_blocking(move || {
        execute_consensus_output(
            args,
            gas_accumulator,
            tn_types::repack_monitor::RepackMonitor::default(),
            engine_update_tx,
        )
    })
    .await?;
    Ok(Run { result, reth_env, sink, _tmp_dir: tmp_dir })
}

/// One block's positions as the writer receives them.
fn positions(positions: &[u32]) -> Option<Arc<[u32]>> {
    Some(positions.into())
}

/// A transaction whose signer does not recover is dropped at recovery, and its position with it:
/// the writer receives the positions of the other transactions, with a gap at the drop.
#[tokio::test]
async fn test_positions_skip_an_unrecoverable_transaction() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let batch = batch_with_transactions(test_chain_spec_arc(), 4, 0);
    let mut with_drop = batch.clone();
    with_drop.transactions.insert(2, UNDECODABLE_TX.to_vec());

    let run = execute(&[&batch], Plan::aligned(vec![with_drop])).await?;
    assert_eq!(run.result?.number, 1);
    assert_eq!(run.reth_env.saved_tx_positions(), [vec![positions(&[0, 1, 3, 4])]]);
    Ok(())
}

/// A transaction execution skips as invalid (a replay of the batch's first transaction, whose
/// nonce is spent) loses its position: the writer receives the kept transactions' positions.
#[tokio::test]
async fn test_positions_skip_an_invalid_transaction() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let batch = batch_with_transactions(test_chain_spec_arc(), 4, 0);
    let mut with_replay = batch.clone();
    with_replay.transactions.insert(2, batch.transactions[0].clone());

    let run = execute(&[&batch], Plan::aligned(vec![with_replay])).await?;
    let tip = run.result?;
    let block = run.reth_env.sealed_block_by_number(tip.number)?.expect("the block");
    assert_eq!(block.body().transactions.len(), 4, "the replay is skipped");
    assert_eq!(run.reth_env.saved_tx_positions(), [vec![positions(&[0, 1, 3, 4])]]);
    Ok(())
}

/// One batch committed twice in an output builds two blocks under the same digest. Both are
/// stored by position (the second keeps none of its transactions, all replays), and the cache is
/// handed the batch once.
#[tokio::test]
async fn test_a_batch_committed_twice_is_stored_by_position_in_both_blocks() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let batch = batch_with_transactions(test_chain_spec_arc(), 3, 0);
    let digest = batch.digest();
    let plan =
        Plan { digests: vec![digest, digest], ..Plan::aligned(vec![batch.clone(), batch.clone()]) };

    let run = execute(&[&batch], plan).await?;
    assert_eq!(run.result?.number, 2, "one block per committed batch");
    assert_eq!(run.reth_env.saved_tx_positions(), [vec![positions(&[0, 1, 2]), positions(&[])]]);
    assert_eq!(run.sink.handed(), [vec![(digest, batch)]]);
    Ok(())
}

/// An output whose digest list names each block's batch wrongly (here swapped) builds the same
/// blocks, but no block's batch hashes to its header's `ommers_hash`: both are stored inline, and
/// the cache is handed nothing.
#[tokio::test]
async fn test_a_misaligned_output_is_stored_inline() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let chain = test_chain_spec_arc();
    let (x, y) =
        (batch_with_transactions(chain.clone(), 2, 0), batch_with_transactions(chain, 1, 0));
    let plan =
        Plan { digests: vec![y.digest(), x.digest()], ..Plan::aligned(vec![x.clone(), y.clone()]) };

    let run = execute(&[&x, &y], plan).await?;
    let tip = run.result?;
    assert_eq!(tip.number, 2);
    assert_eq!(tip.ommers_hash, x.digest(), "the second block names the first batch");
    let first = run.reth_env.sealed_block_by_number(1)?.expect("block 1");
    assert_eq!(first.body().transactions.len(), 2, "block 1 holds batch x");
    assert_eq!(run.reth_env.saved_tx_positions(), [vec![None, None]]);
    assert!(run.sink.handed().is_empty());
    Ok(())
}

/// An epoch-closing output without batches builds one empty block, which has no batch and so no
/// positions.
#[tokio::test]
async fn test_the_empty_closing_block_has_no_positions() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let plan = Plan { close_epoch: true, ..Plan::aligned(Vec::new()) };

    let run = execute(&[], plan).await?;
    let tip = run.result?;
    assert_eq!((tip.number, tip.ommers_hash), (1, B256::ZERO), "the empty closing block");
    assert_eq!(run.reth_env.saved_tx_positions(), [vec![None]]);
    assert!(run.sink.handed().is_empty());
    Ok(())
}

/// A persist that fails and is retried gives the writer the output's positions on the attempt
/// that succeeds, whether that is a retry inside the window or the final attempt. A failed attempt
/// faults before it reaches the writer, so the writer is called once.
#[tokio::test]
async fn test_a_persist_retry_writes_the_same_positions() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let chain = test_chain_spec_arc();
    let (x, y) =
        (batch_with_transactions(chain.clone(), 2, 0), batch_with_transactions(chain, 3, 0));
    for faults in [1, PERSIST_OUTPUT_ATTEMPTS - 1] {
        let plan = Plan { persist_faults: faults, ..Plan::aligned(vec![x.clone(), y.clone()]) };
        let run = execute(&[&x, &y], plan).await?;
        assert_eq!(run.result?.number, 2);
        assert_eq!(run.reth_env.persist_attempt_count(), faults + 1, "{faults} retries");
        assert_eq!(
            run.reth_env.saved_tx_positions(),
            [vec![positions(&[0, 1]), positions(&[0, 1, 2])]],
            "{faults} faults"
        );
    }
    Ok(())
}

/// The cache is handed the output's batches after the persist commits, and never when it fails.
#[tokio::test]
async fn test_the_cache_is_filled_after_the_persist() -> eyre::Result<()> {
    let _guard = IT_TEST_GUARD.lock();
    let chain = test_chain_spec_arc();
    let (x, y) =
        (batch_with_transactions(chain.clone(), 2, 0), batch_with_transactions(chain, 1, 0));

    let run = execute(&[&x, &y], Plan::aligned(vec![x.clone(), y.clone()])).await?;
    assert_eq!(run.result?.number, 2);
    assert_eq!(run.sink.handed(), [vec![(x.digest(), x.clone()), (y.digest(), y.clone())]]);

    let failing = Plan {
        persist_faults: PERSIST_OUTPUT_ATTEMPTS,
        ..Plan::aligned(vec![x.clone(), y.clone()])
    };
    let run = execute(&[&x, &y], failing).await?;
    assert!(run.result.is_err(), "every persist attempt fails");
    assert_eq!(run.reth_env.last_block_number()?, 0, "nothing committed");
    assert!(run.sink.handed().is_empty(), "nothing handed to the cache");
    Ok(())
}
