//! Native slot execution, quorum fallback, and recovery regressions.

use super::*;
use rand::{rngs::StdRng, SeedableRng};
use tn_storage::mem_db::MemDatabase;
use tn_types::{
    BatchBucket, BatchSlotOutput, BatchSlotParent, BatchSlotVoteStore, BatchSlots, BlsKeypair,
    Committee, CommitteeBuilder, Hash as _, SignedBatchSlotRecord,
};

/// Hold the suite's MDBX guard on a blocking thread while driving an async test.
async fn serialized<F, T>(test: F) -> eyre::Result<()>
where
    F: FnOnce() -> T + Send + 'static,
    T: std::future::Future<Output = eyre::Result<()>>,
{
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        let _guard = IT_TEST_GUARD.lock().map_err(|_| eyre::eyre!("MDBX test guard poisoned"))?;
        runtime.block_on(test())
    })
    .await?
}

/// A real execution database with four independently signing slot producers.
struct Fixture {
    /// Execution and admission share this controller and canonical database.
    env: RethEnv,
    /// Per-worker accounting advanced by real output execution.
    gas: GasAccumulator,
    /// The epoch's fixed voting weights and producer identities.
    committee: Committee,
    /// Independent signing keys, including the producer that may be unavailable.
    keys: Vec<BlsKeypair>,
    /// A funded sender's initial transaction.
    batch: Batch,
    /// The sender's deterministic slot bucket.
    bucket: BatchBucket,
    /// Fresh epoch state for reconstruction comparisons.
    initial: BatchSlots,
    /// Own the publication service for deterministic teardown.
    publisher: tokio::task::JoinHandle<()>,
    /// Keep MDBX files alive until every environment handle is dropped.
    _directory: TempDir,
}

impl Fixture {
    /// Create one funded transaction and install the same service used by live execution.
    async fn new() -> eyre::Result<Self> {
        let mut batch = tn_reth::test_utils::batch_with_transactions(test_chain_spec_arc(), 1, 0);
        batch.base_fee_per_gas = MIN_PROTOCOL_BASE_FEE;
        Self::with_batch(batch).await
    }

    /// Seed only the supplied batch's senders, leaving transfer recipients unfunded at genesis.
    async fn with_batch(batch: Batch) -> eyre::Result<Self> {
        let directory = TempDir::new()?;
        let (genesis, _, _) = seeded_genesis_from_random_batches(
            test_genesis_with_consensus_registry(4),
            std::iter::once(&batch),
        );
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let gas = GasAccumulator::new(1);
        let node = default_test_execution_node(
            Some(chain.clone()),
            None,
            directory.path(),
            Some(gas.clone()),
        )?;
        let env = node.get_reth_env().await;
        let mut rng = StdRng::seed_from_u64(1377);
        let mut builder = CommitteeBuilder::new(0);
        let keys = (0_u8..4)
            .map(|ordinal| {
                let key = BlsKeypair::generate(&mut rng);
                builder.add_authority(*key.public(), Address::from([ordinal; 20]));
                key
            })
            .collect();
        let committee = builder.build();
        gas.rewards_counter().set_committee(committee.clone());
        let initial = BatchSlots::new(
            env.chainspec().chain_id().into(),
            committee.clone(),
            BatchSlotParent::new(B256::ZERO, chain.sealed_genesis_header().hash()),
        )?;
        let encoded =
            batch.transactions.first().ok_or_else(|| eyre::eyre!("missing fixture transaction"))?;
        let sender = tn_reth::recover_raw_transaction(encoded)?.signer();
        let bucket = initial.bucket(sender);
        let store = BatchSlotVoteStore::new(MemDatabase::default(), 0);
        store.initialize().await?;
        let receiver = env.batch_slots().install(initial.clone(), None);
        let control = env.batch_slots().clone();
        let publisher = tokio::spawn(async move { control.serve(store, receiver).await });
        Ok(Self {
            env,
            gas,
            committee,
            keys,
            batch,
            bucket,
            initial,
            publisher,
            _directory: directory,
        })
    }

    /// Require a published view instead of substituting an empty controller on failure.
    fn slots(&self) -> eyre::Result<Arc<BatchSlots>> {
        self.env.batch_slots().snapshot().ok_or_else(|| eyre::eyre!("slot controller absent"))
    }

    /// Have the current assigned validator sign this sender's retained transaction.
    fn proposal(&self, batch: Batch) -> eyre::Result<SignedBatchSlotRecord> {
        self.proposal_for(self.bucket, batch)
    }

    /// Sign for any bucket in the fixture, including a newly funded transfer recipient.
    fn proposal_for(
        &self,
        bucket: BatchBucket,
        mut batch: Batch,
    ) -> eyre::Result<SignedBatchSlotRecord> {
        let slots = self.slots()?;
        let owner = slots.producer(slots.position(bucket)?)?;
        let key = self
            .keys
            .iter()
            .find(|key| key.public() == owner)
            .ok_or_else(|| eyre::eyre!("producer key absent"))?;
        batch.beneficiary = self
            .committee
            .authorities()
            .iter()
            .find(|authority| authority.protocol_key() == owner)
            .ok_or_else(|| eyre::eyre!("producer absent from committee"))?
            .execution_address();
        slots.sign_proposal(bucket, *key.public(), batch, key).map_err(Into::into)
    }

    /// Build an ordered availability-certified output from canonical signed envelopes.
    fn output(
        &self,
        records: &[SignedBatchSlotRecord],
        number: u32,
        close: bool,
    ) -> eyre::Result<ConsensusOutput> {
        let mut leader = Certificate::default();
        let authority = self
            .committee
            .authorities()
            .first()
            .map(|authority| authority.id())
            .ok_or_else(|| eyre::eyre!("empty committee"))?;
        leader.update_header_author_for_test(authority);
        leader.update_header_round_for_test(number);
        let batches =
            records.iter().map(SignedBatchSlotRecord::envelope).collect::<Result<Vec<_>, _>>()?;
        let digests = batches.iter().map(|batch| batch.digest()).collect();
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            u64::from(number),
            ReputationScores::default(),
            None,
            tn_types::EpochSeedChainValue::genesis_placeholder(),
        );
        Ok(ConsensusOutput::new(
            sub_dag,
            ConsensusHeaderDigest::default(),
            u64::from(number),
            close,
            digests,
            vec![CertifiedBatch { address: authority.execution_address(), batches }],
        ))
    }

    /// Bound the test so a missing publication service fails visibly instead of hanging CI.
    async fn execute(
        &self,
        output: ConsensusOutput,
    ) -> eyre::Result<Result<SealedHeader, TnEngineError>> {
        let args = BuildArguments::new(self.env.clone(), output, self.env.canonical_tip());
        let gas = self.gas.clone();
        let (updates, _receiver) = tokio::sync::mpsc::channel(8);
        let task = tokio::task::spawn_blocking(move || {
            execute_consensus_output(
                args,
                gas,
                tn_types::repack_monitor::RepackMonitor::enabled(),
                updates,
            )
        });
        timeout(Duration::from_secs(20), task).await?.map_err(Into::into)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.env.batch_slots().disable();
        self.publisher.abort();
    }
}

#[tokio::test]
async fn duplicate_proposals_execute_once_and_keep_original_randomness() -> eyre::Result<()> {
    serialized(|| async {
        let fixture = Fixture::new().await?;
        let proposal = fixture.proposal(fixture.batch.clone())?;
        let output = fixture.output(&[proposal.clone(), proposal.clone()], 1, false)?;
        let digest =
            output.get_batch_digest(0).ok_or_else(|| eyre::eyre!("missing batch digest"))?;
        let mix = output.prev_randao(0, digest);
        let header = fixture.execute(output.clone()).await??;
        assert_eq!(header.number, 1, "losing proposals must not create empty execution blocks");
        assert!(header.gas_used > 0, "the selected transaction must execute");
        assert_eq!(header.mix_hash, mix);
        assert_eq!(header.ommers_hash, digest);
        let position = fixture.slots()?.position(fixture.bucket)?;
        assert_eq!(position.sequence().value(), 1);
        assert_eq!(position.parent().execution(), header.hash());
        let recovered = BatchSlotOutput::recover(
            fixture.initial.clone(),
            output.digest().into(),
            [&proposal, &proposal],
            header.hash(),
        )?
        .0;
        assert_eq!(recovered.position(fixture.bucket)?, position);
        let repeated = fixture.execute(fixture.output(&[proposal], 2, false)?).await??;
        assert_eq!(
            repeated.hash(),
            header.hash(),
            "a resolved sequence performs no additional EVM work"
        );
        let stale_body = fixture.proposal(fixture.batch.clone())?;
        assert!(
            fixture.env.validate_slot_transactions(fixture.slots()?.as_ref(), &stale_body).is_err(),
            "a new sequence must not admit the executed sender nonce"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn timeout_quorum_preserves_fallback_without_duplicate_execution() -> eyre::Result<()> {
    serialized(|| async {
        let fixture = Fixture::new().await?;
        let original = fixture.proposal(fixture.batch.clone())?;
        let original_owner = *original.authority();
        let initial = fixture.slots()?;
        // The unavailable owner's signature is absent from the retry quorum.
        let votes = fixture
            .keys
            .iter()
            .filter(|key| key.public() != &original_owner)
            .map(|key| initial.sign_timeout(fixture.bucket, *key.public(), key))
            .collect::<Result<Vec<_>, _>>()?;
        let retry_output = fixture.output(&votes, 1, false)?;
        let retry_anchor = fixture.execute(retry_output.clone()).await??;
        assert_eq!(retry_anchor.number, 1, "timeout-only output needs one durable anchor");
        assert_eq!(retry_anchor.gas_used, 0);
        assert_eq!(fixture.slots()?.position(fixture.bucket)?.view().value(), 1);
        assert_eq!(fixture.slots()?.position(fixture.bucket)?.sequence().value(), 1);
        assert_eq!(
            fixture.slots()?.position(fixture.bucket)?.parent().execution(),
            retry_anchor.hash()
        );
        let replacement = fixture.proposal(fixture.batch.clone())?;
        assert_ne!(replacement.authority(), original.authority());
        let selected_output = fixture.output(&[replacement.clone(), original.clone()], 2, false)?;
        let selected = fixture.execute(selected_output.clone()).await??;
        assert_eq!(
            selected.number, 2,
            "only the fallback executes after the old sequence times out"
        );
        assert!(selected.gas_used > 0);
        let (recovered, timed_out) = BatchSlotOutput::recover(
            fixture.initial.clone(),
            retry_output.digest().into(),
            &votes,
            retry_anchor.hash(),
        )?;
        let (recovered, history) = BatchSlotOutput::recover(
            recovered,
            selected_output.digest().into(),
            [&replacement, &original],
            selected.hash(),
        )?;
        assert_eq!(recovered.position(fixture.bucket)?, fixture.slots()?.position(fixture.bucket)?);
        assert_eq!(history.len(), 1);
        assert!(recovered
            .vote_for_authorization(
                &original,
                timed_out.first().ok_or_else(|| eyre::eyre!("missing timeout authorization"))?
            )
            .is_ok());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn repeated_sender_nonce_is_rejected_before_execution_or_slot_publication() -> eyre::Result<()>
{
    serialized(|| async {
        let fixture = Fixture::new().await?;
        let mut batch = fixture.batch.clone();
        batch.transactions.extend(fixture.batch.transactions.clone());
        let record = fixture.proposal(batch)?;
        let before = fixture.env.canonical_tip();
        let result = fixture.execute(fixture.output(&[record], 1, false)?).await?;
        assert!(matches!(result, Err(TnEngineError::BatchSlotAdmission(_))));
        assert_eq!(fixture.env.canonical_tip().hash(), before.hash());
        assert_eq!(
            fixture.slots()?.position(fixture.bucket)?,
            fixture.initial.position(fixture.bucket)?
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn timeout_refresh_allows_newly_funded_sender_to_spend() -> eyre::Result<()> {
    serialized(|| async {
        use tn_reth::test_utils::TransactionFactory;

        let chain = test_chain_spec_arc();
        let mut funder = TransactionFactory::new();
        let mut rng = StdRng::seed_from_u64(13779);
        let mut recipient = TransactionFactory::new_random_from_seed(&mut rng);
        let funded = funder.create_eip1559_encoded(
            chain.clone(),
            Some(21_000),
            u128::from(MIN_PROTOCOL_BASE_FEE),
            Some(recipient.address()),
            U256::from(10u64).pow(U256::from(18u64)),
            Bytes::new(),
        );
        let fixture = Fixture::with_batch(Batch {
            transactions: vec![funded],
            epoch: 0,
            beneficiary: Address::ZERO,
            base_fee_per_gas: MIN_PROTOCOL_BASE_FEE,
            worker_id: 0,
            received_at: None,
        })
        .await?;
        let bucket = fixture.initial.bucket(recipient.address());
        assert_ne!(bucket, fixture.bucket, "the funding transfer must come from another bucket");
        let pending = Batch {
            transactions: vec![recipient.create_eip1559_encoded(
                chain,
                Some(21_000),
                u128::from(MIN_PROTOCOL_BASE_FEE),
                Some(funder.address()),
                U256::from(1u64),
                Bytes::new(),
            )],
            ..fixture.batch.clone()
        };
        let stale = fixture.proposal_for(bucket, pending.clone())?;
        assert!(fixture.env.validate_slot_transactions(fixture.slots()?.as_ref(), &stale).is_err());
        let transfer = fixture.proposal(fixture.batch.clone())?;
        fixture.execute(fixture.output(&[transfer], 1, false)?).await??;
        assert!(
            fixture.env.validate_slot_transactions(fixture.slots()?.as_ref(), &stale).is_err(),
            "incoming funds cannot alter an already published admission snapshot"
        );
        let slots = fixture.slots()?;
        let unavailable_owner = slots.producer(slots.position(bucket)?)?;
        let votes = fixture
            .keys
            .iter()
            .filter(|key| key.public() != unavailable_owner)
            .map(|key| slots.sign_timeout(bucket, *key.public(), key))
            .collect::<Result<Vec<_>, _>>()?;
        let anchor = fixture.execute(fixture.output(&votes, 2, false)?).await??;
        assert_eq!(fixture.slots()?.position(bucket)?.parent().execution(), anchor.hash());
        let retry = fixture.proposal_for(bucket, pending)?;
        fixture.env.validate_slot_transactions(fixture.slots()?.as_ref(), &retry)?;
        let spent = fixture.execute(fixture.output(&[retry], 3, false)?).await??;
        assert_eq!(spent.number, 3);
        assert_eq!(spent.gas_used, 21_000);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn epoch_closes_on_last_selected_batch_after_duplicate_filtering() -> eyre::Result<()> {
    serialized(|| async {
        let fixture = Fixture::new().await?;
        let proposal = fixture.proposal(fixture.batch.clone())?;
        let output = fixture.output(&[proposal.clone(), proposal], 1, true)?;
        let header = fixture.execute(output).await??;
        assert_eq!(header.number, 1);
        assert_eq!(
            fixture.env.epoch_state_from_canonical_tip()?.epoch,
            1,
            "the sole selected batch must execute the epoch-closing system calls"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn failed_execution_commit_does_not_open_a_successor_slot() -> eyre::Result<()> {
    serialized(|| async {
        let fixture = Fixture::new().await?;
        let proposal = fixture.proposal(fixture.batch.clone())?;
        let before = fixture.env.canonical_tip();
        fixture.env.inject_persist_provider_faults(PERSIST_OUTPUT_ATTEMPTS);
        assert!(fixture.execute(fixture.output(&[proposal], 1, false)?).await?.is_err());
        assert_eq!(fixture.env.canonical_tip().hash(), before.hash());
        assert_eq!(fixture.env.last_block_number()?, before.number);
        assert_eq!(
            fixture.slots()?.position(fixture.bucket)?,
            fixture.initial.position(fixture.bucket)?,
            "availability voters must retain the previous durable slot opening"
        );
        Ok(())
    })
    .await
}
