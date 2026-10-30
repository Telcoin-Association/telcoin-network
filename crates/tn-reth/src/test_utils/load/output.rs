//! The bench's consensus outputs: a [`Workload`]'s batches packed into the `ConsensusOutput`s
//! `tn_engine::execute_consensus_output` runs.
//!
//! [`OutputFactory`] cuts [`OutputConfig::batches`] batches of about [`OutputConfig::batch_gas`]
//! expected gas each from the workload and wraps them in one output, shaped like a committed
//! sub-dag:
//!
//! - **Batches** carry what execution reads from a batch: the transactions, the leader's epoch,
//!   [`OutputConfig::base_fee`] (epoch 0's production fee is `MIN_PROTOCOL_BASE_FEE`: the node
//!   reads per-worker fees only from a previous epoch's closing block), the producer's execution
//!   address as the beneficiary, and [`OutputConfig::worker_id`]. All of an output's batches sit in
//!   one `CertifiedBatch` from the producer, and the output's batch digests and the leader header's
//!   payload list them in order.
//! - **Leader and producer.** On a `registry` spec the committee is read from the
//!   `ConsensusRegistry` at the canonical tip ([`OutputFactory::committee`], what a caller passes
//!   to `RewardsCounter::set_committee`), the leader rotates through its authorities by consensus
//!   number, and the leader's execution address produces the batches. Without the registry the
//!   leader is the default authority and [`NO_REGISTRY_PRODUCER`] produces the batches: execution
//!   reads the committee only to pay an empty epoch-closing output, and counting a leader that no
//!   committee knows is harmless.
//! - **Consensus numbers and digests** chain: each output's parent hash is the previous output's
//!   digest and its number is one more. The leader round is twice the number (Bullshark commits
//!   leaders on even rounds) and the sub-dag index is the number. The engine checks none of this
//!   between outputs, but the chain keeps every output's digest distinct and matches what the node
//!   expects from consensus. Numbers and rounds keep rising across epochs: production starts each
//!   epoch's DAG afresh, but execution reads the round only into the block nonce, and the round is
//!   how a resumed factory recovers its number without a consensus database.
//! - **The epoch seed chain** folds each commit into the last: the first commit of an epoch folds
//!   into the epoch root, every later one into the previous commit's value. It feeds `PREVRANDAO`
//!   and the committee shuffle of an epoch-closing output.
//! - **Closing an epoch.** [`OutputFactory::next_output_closing`] builds the next output with
//!   `close_epoch` set, so its last block runs the epoch boundary's `ConsensusRegistry` calls
//!   (`concludeEpoch`) seeded by the output's chained committee shuffle seed; it needs a `registry`
//!   spec. The next epoch's committee exists only once that close has executed, so no output
//!   follows until the caller executes it and calls [`OutputFactory::begin_epoch`], which reads the
//!   new epoch and committee from the registry at the tip, rotates the leaders through it and
//!   restarts the seed chain at the new epoch's root. [`OutputFactory::generate`] therefore never
//!   crosses an epoch boundary.
//! - **Resuming.** [`OutputFactory::new`] starts after the env's canonical tip, the way the node
//!   learns its last executed output: the tip's `parent_beacon_block_root` is that output's digest
//!   (`ConsensusBus::last_executed_consensus_block`) and its nonce carries the leader's round, so
//!   the number is half the round. On a `registry` spec the epoch is the registry's at the tip, as
//!   the node reads it (`RethEnv::epoch_state_from_canonical_tip`): `concludeEpoch` runs inside the
//!   closing block, so a tip that closed an epoch starts the factory in the next one while its
//!   nonce still carries the closed epoch. Without the registry no epoch closes and the nonce's
//!   epoch is the epoch. On a genesis tip the first output is number 1 on the default consensus
//!   header. The seed chain restarts at the epoch root.
//! - **Commit times** are in milliseconds (the sub-second timestamp fork is active from genesis
//!   outside `adiri`), and the EVM `timestamp` is their whole seconds. The first output commits one
//!   second after the tip's `timestamp` and each later one [`OutputConfig::interval_ms`] after the
//!   one before, so `TNPayload::new` never clamps a block's timestamp up to its parent's.
//!
//! [`OutputFactory::generate`] builds and signs many outputs up front, so a bench times only
//! execution. Each [`LoadOutput`] keeps per-batch metadata ([`BatchMeta`]: kinds and exact expected
//! gas) beside its `ConsensusOutput`; the raw transactions live only in the output's batches
//! ([`LoadOutput::raw_txs`]).

use super::{
    state::PrefillSpec,
    workload::{LoadBatch, LoadKind, Workload, WorkloadConfig},
};
use crate::{test_utils::committee_from_epoch_state, RethEnv};
use std::collections::VecDeque;
use tn_types::{
    address, deconstruct_nonce, Address, AuthorityIdentifier, Batch, BlsSignature, Certificate,
    CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader, ConsensusHeaderDigest,
    ConsensusOutput, Epoch, EpochSeedChainValue, Hash as _, HeaderBuilder, ReputationScores,
    SignatureVerificationState, TimestampMs, WorkerId, MIN_PROTOCOL_BASE_FEE,
};

/// The batch producer, and so every block's beneficiary, when the genesis has no
/// `ConsensusRegistry`.
pub const NO_REGISTRY_PRODUCER: Address = address!("0x10ad0000000000000000000000000000000000be");

/// How an [`OutputFactory`] shapes its outputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutputConfig {
    /// The batches, and so the blocks, in each output.
    pub batches: usize,
    /// Each batch's target expected gas.
    pub batch_gas: u64,
    /// The commit-time step between consecutive outputs, in milliseconds.
    pub interval_ms: u64,
    /// The worker every batch comes from.
    pub worker_id: WorkerId,
    /// The base fee every batch carries, and so every block's base fee.
    pub base_fee: u64,
}

impl Default for OutputConfig {
    /// 8 batches of 15M gas, one output per second, worker 0 and epoch 0's base fee.
    fn default() -> Self {
        Self {
            batches: 8,
            batch_gas: 15_000_000,
            interval_ms: 1_000,
            worker_id: 0,
            base_fee: MIN_PROTOCOL_BASE_FEE,
        }
    }
}

/// What a batch of a [`LoadOutput`] holds besides its raw transactions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BatchMeta {
    /// Each transaction's kind, in execution order.
    pub kinds: Vec<LoadKind>,
    /// Each transaction's exact expected gas use, parallel to `kinds`.
    pub gas: Vec<u64>,
}

impl BatchMeta {
    /// The gas the batch is expected to use.
    pub fn expected_gas(&self) -> u64 {
        self.gas.iter().sum()
    }

    /// How many of the batch's transactions are of `kind`.
    pub fn count(&self, kind: LoadKind) -> usize {
        self.kinds.iter().filter(|k| **k == kind).count()
    }

    /// The batch's transaction count.
    pub fn len(&self) -> usize {
        self.kinds.len()
    }

    /// Whether the batch has no transactions.
    pub fn is_empty(&self) -> bool {
        self.kinds.is_empty()
    }
}

/// One output of an [`OutputFactory`]: the `ConsensusOutput` and each batch's metadata.
#[derive(Clone, Debug)]
pub struct LoadOutput {
    /// The output, as consensus hands it to execution.
    pub output: ConsensusOutput,
    /// Each batch's metadata, in execution order.
    pub batches: Vec<BatchMeta>,
}

impl LoadOutput {
    /// The output's batch at `index`, in execution order.
    pub fn batch(&self, index: usize) -> &Batch {
        self.output
            .batches()
            .iter()
            .flat_map(|certified| &certified.batches)
            .nth(index)
            .expect("batch index within the output")
    }

    /// The EIP-2718 encoded transactions of the batch at `index`.
    pub fn raw_txs(&self, index: usize) -> &[Vec<u8>] {
        &self.batch(index).transactions
    }

    /// The output's transaction count.
    pub fn tx_count(&self) -> usize {
        self.batches.iter().map(BatchMeta::len).sum()
    }

    /// The gas the output is expected to use.
    pub fn expected_gas(&self) -> u64 {
        self.batches.iter().map(BatchMeta::expected_gas).sum()
    }

    /// How many of the output's transactions are of `kind`.
    pub fn count(&self, kind: LoadKind) -> usize {
        self.batches.iter().map(|batch| batch.count(kind)).sum()
    }
}

/// A deterministic stream of production-shaped consensus outputs over a [`Workload`] (see the
/// module docs).
#[derive(Debug)]
pub struct OutputFactory {
    /// The transactions.
    workload: Workload,
    /// How outputs are shaped.
    config: OutputConfig,
    /// The committee read from the `ConsensusRegistry`, on a `registry` spec.
    committee: Option<Committee>,
    /// The leaders in rotation and each one's execution address.
    leaders: Vec<(AuthorityIdentifier, Address)>,
    /// The leaders' epoch.
    epoch: Epoch,
    /// Whether the last output closes [`Self::epoch`]: no output follows until
    /// [`Self::begin_epoch`] enters the next epoch.
    close_pending: bool,
    /// The last output's consensus number.
    number: u64,
    /// The last output's digest, the next output's parent hash.
    parent: ConsensusHeaderDigest,
    /// The next output's commit time, in milliseconds.
    commit_ms: u64,
    /// The last output's sub-dag, which floors the next commit time.
    previous: Option<CommittedSubDag>,
    /// The epoch seed chain value the next commit folds into.
    seed: EpochSeedChainValue,
}

impl OutputFactory {
    /// A factory whose first output follows `env`'s canonical tip, over a workload of `spec`
    /// drawn by `workload` from the senders' nonces in `env`.
    pub fn new(
        env: &RethEnv,
        spec: PrefillSpec,
        workload: WorkloadConfig,
        config: OutputConfig,
    ) -> eyre::Result<Self> {
        eyre::ensure!(config.batches > 0, "an output needs batches");
        eyre::ensure!(config.batch_gas > 0, "a batch needs a gas target");
        eyre::ensure!(config.interval_ms > 0, "commit times must rise");

        let tip = env.canonical_tip();
        let (nonce_epoch, round) = deconstruct_nonce(u64::from(tip.nonce));
        let (number, parent) = if tip.number == 0 {
            (0, ConsensusHeader::default().digest())
        } else {
            let digest = tip
                .parent_beacon_block_root
                .ok_or_else(|| eyre::eyre!("tip {} has no consensus header digest", tip.number))?;
            eyre::ensure!(
                round > 0 && round % 2 == 0,
                "tip {} has leader round {round}, not twice a consensus number",
                tip.number
            );
            (u64::from(round / 2), digest.into())
        };

        // the node reads its epoch from the registry at the tip: a tip that closed an epoch
        // already reads the next one, while its nonce still carries the closed epoch
        let (epoch, committee, leaders) = if spec.registry {
            let (committee, leaders) = registry_committee(env)?;
            (committee.epoch(), Some(committee), leaders)
        } else {
            (nonce_epoch, None, vec![(AuthorityIdentifier::default(), NO_REGISTRY_PRODUCER)])
        };

        Ok(Self {
            workload: Workload::new(env, spec, workload)?,
            config,
            committee,
            leaders,
            epoch,
            close_pending: false,
            number,
            parent,
            commit_ms: (tip.timestamp + 1) * 1_000,
            previous: None,
            seed: EpochSeedChainValue::epoch_root(epoch),
        })
    }

    /// The committee read from the `ConsensusRegistry` on a `registry` spec, for
    /// `RewardsCounter::set_committee`; `None` without the registry.
    pub fn committee(&self) -> Option<&Committee> {
        self.committee.as_ref()
    }

    /// The consensus number of the last output built, or of the env's last executed output before
    /// the first.
    pub fn last_number(&self) -> u64 {
        self.number
    }

    /// The leaders' epoch: the next output's, or while a close is pending the epoch it closes.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The next output: [`OutputConfig::batches`] batches from the workload, committed
    /// [`OutputConfig::interval_ms`] after the last.
    ///
    /// # Panics
    ///
    /// While a close is pending: after [`Self::next_output_closing`], until [`Self::begin_epoch`].
    pub fn next_output(&mut self) -> LoadOutput {
        self.build(false)
    }

    /// The next output, built like [`Self::next_output`] and closing its epoch: its last block runs
    /// the epoch boundary's `ConsensusRegistry` calls, seeded by the output's committee shuffle
    /// seed (the epoch seed chain value as of this commit).
    ///
    /// No output follows until the caller executes this one and calls [`Self::begin_epoch`].
    /// Refuses without the registry (a `registry` spec), which the close calls, and while a close
    /// is pending.
    pub fn next_output_closing(&mut self) -> eyre::Result<LoadOutput> {
        eyre::ensure!(self.committee.is_some(), "closing an epoch needs a `registry` spec");
        eyre::ensure!(!self.close_pending, "epoch {} is already closing", self.epoch);
        Ok(self.build(true))
    }

    /// Enter the epoch after a pending close once `env` has executed the closing output: read the
    /// new epoch and its committee from the `ConsensusRegistry` at the canonical tip, rotate the
    /// leaders through it and restart the epoch seed chain at the new epoch's root. Consensus
    /// numbers, leader rounds and commit times continue across the boundary. Returns the new
    /// committee, for `RewardsCounter::set_committee`.
    ///
    /// Refuses without a pending close, and unless the registry at the tip has entered the epoch
    /// after the closing one.
    pub fn begin_epoch(&mut self, env: &RethEnv) -> eyre::Result<&Committee> {
        eyre::ensure!(self.close_pending, "no epoch close is pending");
        let (committee, leaders) = registry_committee(env)?;
        let epoch = committee.epoch();
        eyre::ensure!(
            Some(epoch) == self.epoch.checked_add(1),
            "the registry is in epoch {epoch} at the tip: execute the output closing epoch {} first",
            self.epoch
        );
        self.epoch = epoch;
        self.leaders = leaders;
        self.seed = EpochSeedChainValue::epoch_root(epoch);
        self.close_pending = false;
        Ok(&*self.committee.insert(committee))
    }

    /// The next output, closing its epoch if `close`.
    fn build(&mut self, close: bool) -> LoadOutput {
        assert!(
            !self.close_pending,
            "epoch {} is closing: execute the closing output and call `begin_epoch` first",
            self.epoch
        );
        let number = self.number + 1;
        let round = u32::try_from(2 * number).expect("leader round fits a u32");
        let (leader_id, producer) =
            self.leaders[(number % self.leaders.len() as u64) as usize].clone();
        let OutputConfig { batches: count, batch_gas, worker_id, base_fee, .. } = self.config;

        let mut batches = Vec::with_capacity(count);
        let mut metas = Vec::with_capacity(count);
        let mut digests = VecDeque::with_capacity(count);
        for _ in 0..count {
            let LoadBatch { txs, kinds, gas } = self.workload.next_batch(batch_gas);
            let batch = Batch {
                transactions: txs,
                epoch: self.epoch,
                beneficiary: producer,
                base_fee_per_gas: base_fee,
                worker_id,
                received_at: None,
            };
            digests.push_back(batch.digest());
            batches.push(batch);
            metas.push(BatchMeta { kinds, gas });
        }

        let header = HeaderBuilder::default()
            .author(leader_id)
            .round(round)
            .epoch(self.epoch)
            .created_at_ms(TimestampMs::from_millis(self.commit_ms))
            .payload(digests.iter().map(|digest| (*digest, worker_id)).collect())
            .build();
        let mut leader = Certificate::default();
        leader.update_header_for_test(header);
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        let sub_dag = CommittedSubDag::new(
            vec![leader.clone()],
            leader,
            number,
            ReputationScores::default(),
            self.previous.take(),
            self.seed,
        );
        let output = ConsensusOutput::new(
            sub_dag.clone(),
            self.parent,
            number,
            close,
            digests,
            vec![CertifiedBatch { address: producer, batches }],
        );

        self.seed = EpochSeedChainValue::from_committed(output.committee_shuffle_seed());
        self.previous = Some(sub_dag);
        self.parent = output.digest();
        self.number = number;
        self.commit_ms += self.config.interval_ms;
        self.close_pending = close;
        LoadOutput { output, batches: metas }
    }

    /// The next `n` outputs, built and signed up front. They never cross an epoch boundary: the
    /// next epoch's committee exists only once the closing output has executed, so the closing
    /// output comes from [`Self::next_output_closing`] and the next epoch's after
    /// [`Self::begin_epoch`].
    ///
    /// # Panics
    ///
    /// While a close is pending, as [`Self::next_output`].
    pub fn generate(&mut self, n: usize) -> Vec<LoadOutput> {
        (0..n).map(|_| self.next_output()).collect()
    }
}

/// The `ConsensusRegistry`'s committee at `env`'s canonical tip, in the registry's current epoch,
/// and its authorities in leader rotation with each one's execution address.
fn registry_committee(
    env: &RethEnv,
) -> eyre::Result<(Committee, Vec<(AuthorityIdentifier, Address)>)> {
    let committee = committee_from_epoch_state(env.epoch_state_from_canonical_tip()?)?;
    let leaders = committee
        .authorities()
        .iter()
        .map(|authority| (authority.id(), authority.execution_address()))
        .collect();
    Ok((committee, leaders))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            execute_payload_and_update_canonical_chain,
            load::{open_production_env, state::REGISTRY_VALIDATORS},
        },
    };
    use std::{collections::HashSet, panic::AssertUnwindSafe, sync::Arc};
    use tn_types::{max_batch_gas, TaskManager};

    /// Each batch's target: the transaction a batch leaves for the next (at most a 1 KiB creation,
    /// about 275k gas) stays within 5% of it.
    const BATCH_GAS: u64 = 6_000_000;

    /// A small prefill: 2k accounts, a few senders, two pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 2_000, senders: 8, pools: 2, seed: 13, registry: false }
    }

    /// [`spec`] on a `ConsensusRegistry` genesis.
    fn registry_spec() -> PrefillSpec {
        PrefillSpec { registry: true, ..spec() }
    }

    /// The seed chain value a commit at `round` folds from `previous` (the factory's leaders carry
    /// the default seed signature).
    fn folded(previous: EpochSeedChainValue, round: u32) -> tn_types::B256 {
        previous.fold(round, &BlsSignature::default()).into_inner()
    }

    /// Three batches of [`BATCH_GAS`] per output.
    fn config() -> OutputConfig {
        OutputConfig { batches: 3, batch_gas: BATCH_GAS, ..OutputConfig::default() }
    }

    /// An env on a prefilled genesis in a temp dir, opened as the node opens it.
    struct Bench {
        env: RethEnv,
        _tasks: TaskManager,
        _tmp: tempfile::TempDir,
    }

    impl Bench {
        fn open(spec: &PrefillSpec) -> eyre::Result<Self> {
            let tmp = tempfile::tempdir()?;
            let tasks = TaskManager::default();
            let env =
                open_production_env(tmp.path(), Arc::new(spec.genesis().into()), &[], &tasks)?;
            Ok(Self { env, _tasks: tasks, _tmp: tmp })
        }

        /// A factory on the env with the workload seeded by `seed`.
        fn factory(&self, spec: PrefillSpec, seed: u64) -> eyre::Result<OutputFactory> {
            let workload = WorkloadConfig { seed, ..WorkloadConfig::default() };
            OutputFactory::new(&self.env, spec, workload, config())
        }

        /// Execute and persist `load` one block per batch, with each payload built as
        /// `execute_consensus_output` builds it, and check that every block runs all its batch's
        /// transactions, uses exactly the expected gas and keeps the commit time.
        fn execute(&self, load: &LoadOutput) -> eyre::Result<()> {
            let output = &load.output;
            for (index, meta) in load.batches.iter().enumerate() {
                let batch = load.batch(index);
                let digest = output.get_batch_digest(index).expect("a digest per batch");
                let payload = TNPayload::new(
                    self.env.canonical_tip(),
                    batch.beneficiary,
                    index,
                    digest,
                    output,
                    output.digest().into(),
                    batch.base_fee_per_gas,
                    max_batch_gas(output.leader().epoch()),
                    output.prev_randao(index, digest),
                    batch.worker_id,
                );
                let block = execute_payload_and_update_canonical_chain(
                    &self.env,
                    payload,
                    batch.transactions.clone(),
                )?;
                let header = block.recovered_block.header();
                assert_eq!(block.execution_output.result.receipts.len(), meta.len());
                assert_eq!(header.gas_used, meta.expected_gas());
                assert_eq!(header.timestamp, output.committed_at());
            }
            Ok(())
        }
    }

    /// Outputs hold the configured batches near their gas target, chain numbers and digests from
    /// the genesis consensus header, and commit one second apart from one second past genesis.
    #[tokio::test]
    async fn test_output_factory_shape_and_chain() -> eyre::Result<()> {
        let bench = Bench::open(&spec())?;
        let genesis = bench.env.canonical_tip();
        let mut factory = bench.factory(spec(), 1)?;
        assert_eq!(factory.last_number(), 0);
        assert!(factory.committee().is_none(), "no committee without the registry");

        let outputs = factory.generate(4);
        let mut parent = ConsensusHeader::default().digest();
        for (i, load) in (0u64..).zip(&outputs) {
            let output = &load.output;
            let number = i + 1;
            assert_eq!(output.number(), number);
            assert_eq!(output.parent_hash(), parent, "output {number} follows its parent");
            assert_eq!(u64::from(output.leader_round()), 2 * number);
            assert_eq!(output.leader().epoch(), 0);
            assert!(!output.close_epoch());
            assert_eq!(output.committed_at_ms().as_millis(), (genesis.timestamp + 1 + i) * 1_000);

            assert_eq!(load.batches.len(), 3);
            assert_eq!(output.flatten_batches().len(), 3);
            assert_eq!(output.batch_digests().len(), 3);
            let payload: Vec<_> = output.leader().payload().keys().copied().collect();
            assert_eq!(payload, Vec::from(output.batch_digests().clone()));
            for (index, meta) in load.batches.iter().enumerate() {
                let batch = load.batch(index);
                assert_eq!(output.get_batch_digest(index), Some(batch.digest()));
                assert_eq!(batch.transactions.len(), meta.len());
                assert!(meta.expected_gas() <= BATCH_GAS, "batch {index} within its target");
                assert!(meta.expected_gas() * 100 >= BATCH_GAS * 95, "batch {index} near it");
                assert_eq!(batch.beneficiary, NO_REGISTRY_PRODUCER);
                assert_eq!(batch.base_fee_per_gas, MIN_PROTOCOL_BASE_FEE);
                assert_eq!((batch.worker_id, batch.epoch), (0, 0));
            }
            assert_eq!(output.batches()[0].address, NO_REGISTRY_PRODUCER);
            let raw: usize = (0..3).map(|index| load.raw_txs(index).len()).sum();
            assert_eq!(load.tx_count(), raw);
            let by_kind: usize = LoadKind::ALL.iter().map(|kind| load.count(*kind)).sum();
            assert_eq!(by_kind, raw);
            parent = output.digest();
        }
        assert_eq!(factory.last_number(), 4);
        Ok(())
    }

    /// The same spec, seed and env give byte-identical outputs; another workload seed does not.
    #[tokio::test]
    async fn test_output_factory_is_deterministic() -> eyre::Result<()> {
        let bench = Bench::open(&spec())?;
        let first = bench.factory(spec(), 1)?.generate(2);
        let again = bench.factory(spec(), 1)?.generate(2);
        for (a, b) in first.iter().zip(&again) {
            assert_eq!(a.output.digest(), b.output.digest());
            assert_eq!(a.batches, b.batches);
            for index in 0..a.batches.len() {
                assert_eq!(a.raw_txs(index), b.raw_txs(index));
            }
        }
        let other = bench.factory(spec(), 2)?.next_output();
        assert_ne!(other.output.digest(), first[0].output.digest());
        assert_ne!(other.raw_txs(0), first[0].raw_txs(0));
        Ok(())
    }

    /// A factory opened on an env that executed outputs continues after the last one: the next
    /// number, the last digest as parent, a commit time past the tip, and nonces that execute.
    #[tokio::test]
    async fn test_output_factory_resumes_after_executed_outputs() -> eyre::Result<()> {
        let bench = Bench::open(&spec())?;
        let executed = bench.factory(spec(), 1)?.generate(2);
        for load in &executed {
            bench.execute(load)?;
        }
        let tip = bench.env.canonical_tip();
        assert_eq!(tip.number, 6);

        let mut resumed = bench.factory(spec(), 3)?;
        assert_eq!(resumed.last_number(), 2);
        let next = resumed.next_output();
        assert_eq!(next.output.number(), 3);
        assert_eq!(next.output.parent_hash(), executed[1].output.digest());
        assert_eq!(next.output.leader_round(), 6);
        assert_eq!(next.output.committed_at(), tip.timestamp + 1);
        bench.execute(&next)?;
        assert_eq!(bench.env.canonical_tip().number, 9);
        Ok(())
    }

    /// On a `registry` genesis the committee comes from the `ConsensusRegistry`, leaders rotate
    /// through it, and each output's batches come from its leader's execution address.
    #[tokio::test]
    async fn test_output_factory_registry_leaders() -> eyre::Result<()> {
        let spec = PrefillSpec { registry: true, ..spec() };
        let bench = Bench::open(&spec)?;
        let mut factory = bench.factory(spec, 1)?;
        let committee = factory.committee().cloned().expect("a registry committee");
        assert_eq!(committee.size(), REGISTRY_VALIDATORS);

        let outputs = factory.generate(REGISTRY_VALIDATORS);
        let mut leaders = HashSet::new();
        for load in &outputs {
            let author = load.output.leader().author();
            let authority = committee.authority(author).expect("the leader is a member");
            let producer = authority.execution_address();
            assert_eq!(load.output.batches()[0].address, producer);
            assert!((0..load.batches.len()).all(|index| load.batch(index).beneficiary == producer));
            leaders.insert(author.clone());
        }
        assert_eq!(leaders.len(), REGISTRY_VALIDATORS, "every member leads once in a rotation");
        Ok(())
    }

    /// A closing output carries the chained committee shuffle seed and closes the epoch on
    /// execution. Nothing follows it until `begin_epoch`, which waits for the close to execute;
    /// the next epoch's outputs then carry its number and its committee's leaders, continue the
    /// consensus chain and fold the seed chain from the new epoch's root.
    #[tokio::test]
    async fn test_output_factory_closes_an_epoch() -> eyre::Result<()> {
        let spec = registry_spec();
        let bench = Bench::open(&spec)?;
        let mut factory = bench.factory(spec, 1)?;
        assert_eq!(factory.epoch(), 0);
        let first = factory.next_output();
        let closing = factory.next_output_closing()?;
        let output = &closing.output;
        assert!(output.close_epoch());
        assert_eq!((output.number(), output.leader().epoch()), (2, 0));
        assert_eq!(output.parent_hash(), first.output.digest());
        let chained = EpochSeedChainValue::from_committed(first.output.committee_shuffle_seed());
        assert_eq!(
            output.committee_shuffle_seed(),
            folded(chained, output.leader_round()),
            "the closing seed folds the epoch's previous commit"
        );

        // nothing follows a pending close, and the next epoch waits for the close to execute
        let next = std::panic::catch_unwind(AssertUnwindSafe(|| factory.next_output()));
        assert!(next.is_err(), "no output while a close is pending");
        assert!(factory.next_output_closing().is_err(), "one close at a time");
        bench.execute(&first)?;
        assert!(factory.begin_epoch(&bench.env).is_err(), "the close has not executed");

        bench.execute(&closing)?;
        let tip = bench.env.canonical_tip();
        assert_eq!(&tip.extra_data[..], output.committee_shuffle_seed().as_slice());
        assert_eq!(bench.env.epoch_state_from_canonical_tip()?.epoch, 1, "the close ran");
        let committee = factory.begin_epoch(&bench.env)?.clone();
        assert_eq!((committee.epoch(), factory.epoch()), (1, 1));
        assert_eq!(factory.committee().map(Committee::epoch), Some(1));
        assert!(factory.begin_epoch(&bench.env).is_err(), "no close is pending");

        let next = factory.next_output();
        let output = &next.output;
        assert!(!output.close_epoch());
        assert_eq!((output.number(), output.leader().epoch()), (3, 1));
        assert_eq!(output.parent_hash(), closing.output.digest());
        assert!(committee.authority(output.leader().author()).is_some(), "a new-epoch leader");
        assert!((0..next.batches.len()).all(|index| next.batch(index).epoch == 1));
        assert_eq!(
            output.committee_shuffle_seed(),
            folded(EpochSeedChainValue::epoch_root(1), output.leader_round()),
            "the new epoch's first commit folds its epoch root"
        );
        bench.execute(&next)?;
        Ok(())
    }

    /// A factory opened on an env whose tip closed an epoch starts in the next epoch, as the node
    /// reads its epoch from the registry: the next number after the closing output, the new
    /// epoch's committee and leaders, and the seed chain from the new epoch's root.
    #[tokio::test]
    async fn test_output_factory_resumes_after_an_epoch_close() -> eyre::Result<()> {
        let spec = registry_spec();
        let bench = Bench::open(&spec)?;
        let closing = bench.factory(spec, 1)?.next_output_closing()?;
        bench.execute(&closing)?;
        let tip = bench.env.canonical_tip();
        assert_eq!(
            deconstruct_nonce(u64::from(tip.nonce)).0,
            0,
            "the nonce keeps the closed epoch"
        );

        let mut resumed = bench.factory(spec, 2)?;
        assert_eq!((resumed.last_number(), resumed.epoch()), (1, 1));
        let committee = resumed.committee().cloned().expect("a registry committee");
        assert_eq!(committee.epoch(), 1);
        let next = resumed.next_output();
        let output = &next.output;
        assert_eq!((output.number(), output.leader().epoch()), (2, 1));
        assert_eq!(output.parent_hash(), closing.output.digest());
        assert!(committee.authority(output.leader().author()).is_some(), "a new-epoch leader");
        assert_eq!(
            output.committee_shuffle_seed(),
            folded(EpochSeedChainValue::epoch_root(1), output.leader_round())
        );
        bench.execute(&next)?;
        Ok(())
    }

    /// Closing an epoch calls the `ConsensusRegistry`, so a factory without one refuses and builds
    /// nothing.
    #[tokio::test]
    async fn test_output_factory_refuses_a_close_without_the_registry() -> eyre::Result<()> {
        let bench = Bench::open(&spec())?;
        let mut factory = bench.factory(spec(), 1)?;
        assert!(factory.next_output_closing().is_err());
        assert_eq!(factory.next_output().output.number(), 1, "the refusal built no output");
        Ok(())
    }
}
