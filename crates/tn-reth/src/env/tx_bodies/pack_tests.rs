//! Transactions stored by their position in real consensus packs.
//!
//! [`OutputFactory`] outputs on a `ConsensusRegistry` genesis are saved into a real
//! [`ConsensusChain`] across three epochs (0 and 1 closed by the registry, 2 live) and executed as
//! the engine executes them (pack saved and persisted first, then the blocks persisted with their
//! transactions' batch positions) into a datadir that stores transactions inline and one that
//! stores them by position. Every transaction then reads back the same from both, through the
//! live pack, the closed packs and the decoded-batch cache, including while an epoch handoff
//! seals the pack being read. The damage cases check what a lost, partial, misplaced or damaged
//! pack and a row past its batch or naming another transaction give: an error naming what failed,
//! never a missing or another transaction.

use std::{
    fs,
    io::{Read as _, Seek as _, SeekFrom, Write as _},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    init_reth_defaults,
    payload::TNPayload,
    test_utils::load::{
        output::{LoadOutput, OutputConfig, OutputFactory},
        state::PrefillSpec,
        workload::WorkloadConfig,
    },
    NewCanonicalChain, OutputBuildScope, ProviderError, RethChainSpec, RethCommand, RethConfig,
    RethEnv, TxBodies, TxBodyError, TxBodyErrorKind, TxPackCoverageError, TX_BODY_CACHE_BYTES,
};
use clap::Parser as _;
use reth_provider::{
    providers::TxBodyReadError, BlockBodyIndicesProvider as _, BlockNumReader as _,
    BlockReader as _, DatabaseProviderFactory as _, HeaderProvider as _, TransactionsProvider as _,
};
use tempfile::TempDir;
use tn_storage::{
    archive::pack_iter::PackIter,
    consensus::ConsensusChain,
    consensus_pack::{BatchReadError, PackRecord, DATA_NAME},
};
use tn_types::{
    gas_accumulator::GasAccumulator, max_batch_gas, BlockNumber, Committee, ConsensusNumHash,
    Decodable2718 as _, Encodable2718 as _, Epoch, EpochRecord, Hash as _, TaskManager,
    TransactionSigned, TxNumber, B256,
};

/// Outputs per epoch besides the closing one.
const OUTPUTS_PER_EPOCH: usize = 2;

/// A small registry genesis: a few senders, a few hundred funded accounts, one pool.
fn spec() -> PrefillSpec {
    PrefillSpec { accounts: 200, senders: 4, pools: 1, seed: 23, registry: true }
}

/// Two batches of about 600k gas per output: a few transactions each.
fn output_config() -> OutputConfig {
    OutputConfig { batches: 2, batch_gas: 600_000, ..OutputConfig::default() }
}

/// An env on `datadir`, opened as the node opens it, asking a new datadir to store its
/// transactions `tx_bodies`, with a decoded-batch cache of `cache_bytes` and no stand-in for packs.
fn open(
    datadir: &Path,
    chain: &Arc<RethChainSpec>,
    tx_bodies: TxBodies,
    cache_bytes: usize,
    tasks: &TaskManager,
) -> eyre::Result<RethEnv> {
    init_reth_defaults();
    let mut command = RethCommand::try_parse_from(["tn-reth"])?;
    command.rpc.http = true;
    command.rpc.ipcdisable = true;
    let config = RethConfig::new(command, None, datadir, true, chain.clone())
        .with_new_datadir_tx_bodies(tx_bodies)
        .with_tx_body_cache_bytes(cache_bytes);
    let database = RethEnv::new_database(&config, datadir.join("db"))?;
    let env = RethEnv::new(&config, tasks, database, None, GasAccumulator::default())?;
    eyre::ensure!(env.datadir_format().tx_bodies == tx_bodies, "the datadir's format");
    Ok(env)
}

/// Execute `load` on `env` as the engine does: one block per batch under its digest, the blocks
/// persisted with their transactions' batch positions, the persisted batches handed to the cache,
/// then the blocks announced and finalized. `positions` may rewrite the positions persisted.
fn execute_with(
    env: &RethEnv,
    load: &LoadOutput,
    positions: impl FnOnce(&mut Vec<Option<Arc<[u32]>>>),
) -> eyre::Result<()> {
    let output = &load.output;
    let mut scope = OutputBuildScope::new();
    let cims = env.canonical_in_memory_state();
    let mut parent = env.canonical_tip();
    let (mut blocks, mut tx_positions) = (Vec::new(), Vec::new());
    for (index, recovered) in RethEnv::recover_output_signers(output).into_iter().enumerate() {
        let batch = load.batch(index);
        let digest = output.get_batch_digest(index).expect("a digest per batch");
        let payload = TNPayload::new(
            parent.clone(),
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
        let built = env.build_block_from_recovered_payload(payload, recovered, &mut scope)?;
        parent = built.block.recovered_block.clone_sealed_header();
        cims.update_chain(NewCanonicalChain::Commit { new: vec![built.block.clone()] });
        cims.set_canonical_head(parent.clone());
        blocks.push(built.block);
        tx_positions.push(built.tx_positions);
    }
    positions(&mut tx_positions);
    env.persist_executed_output(&blocks, &tx_positions)?;
    env.cache_persisted_batches(output, &tx_positions);
    env.announce_executed_output(blocks, None)?;
    env.finalize_block(parent)?;
    Ok(())
}

/// [`execute_with`], persisting the positions the build gave.
fn execute(env: &RethEnv, load: &LoadOutput) -> eyre::Result<()> {
    execute_with(env, load, |_| ())
}

/// One stored transaction: its number, its block, the epoch and batch digest its block's header
/// names, its encoding and its hash.
#[derive(Clone, Debug, PartialEq, Eq)]
struct StoredTx {
    id: TxNumber,
    block: BlockNumber,
    epoch: Epoch,
    digest: B256,
    encoded: Vec<u8>,
    hash: B256,
}

/// Every transaction `env` stores, read through its database provider by number, by hash and by
/// range, each read checked against the others.
fn read_all(env: &RethEnv) -> eyre::Result<Vec<StoredTx>> {
    let provider = env.blockchain_provider().database_provider_ro()?;
    let tip = provider.last_block_number()?;
    let count = provider.block_body_indices(tip)?.expect("the tip's body").next_tx_num();
    let by_range = provider.transactions_by_tx_range(0..count)?;
    eyre::ensure!(by_range.len() as u64 == count, "{} of {count} by range", by_range.len());
    let mut stored = Vec::new();
    for (id, ranged) in (0..count).zip(by_range) {
        let tx = provider.transaction_by_id(id)?.expect("stored");
        let hash = *tx.tx_hash();
        let by_hash = provider.transaction_by_hash(hash)?.expect("indexed");
        eyre::ensure!(tx.encoded_2718() == ranged.encoded_2718(), "tx {id}: by number vs range");
        eyre::ensure!(tx.encoded_2718() == by_hash.encoded_2718(), "tx {id}: by number vs hash");
        let block = provider.block_by_transaction_id(id)?.expect("in a block");
        let header = provider.header_by_number(block)?.expect("its header");
        stored.push(StoredTx {
            id,
            block,
            epoch: RethEnv::extract_epoch_from_header(&header),
            digest: header.ommers_hash,
            encoded: tx.encoded_2718(),
            hash,
        });
    }
    Ok(stored)
}

/// Read transaction `tx` by number through `env`'s database provider.
fn read(env: &RethEnv, tx: &StoredTx) -> Result<Vec<u8>, ProviderError> {
    let provider = env.blockchain_provider().database_provider_ro()?;
    Ok(provider.transaction_by_id(tx.id)?.expect("a row").encoded_2718())
}

/// The resolver's error under `err`, a read that failed resolving a row.
fn resolve_error(err: &ProviderError) -> &TxBodyError {
    match err {
        ProviderError::Other(inner) => inner.as_error().downcast_ref(),
        _ => None,
    }
    .unwrap_or_else(|| panic!("not a resolve error: {err}"))
}

/// The vendored read side's error under `err`.
fn read_error(err: &ProviderError) -> Option<&TxBodyReadError> {
    match err {
        ProviderError::Other(err) => err.as_error().downcast_ref(),
        _ => None,
    }
}

/// Copy directory `src` to `dst` (which must not exist), files and subdirectories.
fn copy_dir(src: &Path, dst: &Path) -> eyre::Result<()> {
    fs::create_dir(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

/// The epoch record closing `epoch` of `committee` at `closing`, before `next`.
fn record(
    epoch: Epoch,
    committee: &Committee,
    next: &Committee,
    closing: &LoadOutput,
) -> EpochRecord {
    EpochRecord {
        epoch,
        committee: committee.bls_keys().iter().copied().collect(),
        next_committee: next.bls_keys().iter().copied().collect(),
        final_consensus: ConsensusNumHash::new(
            closing.output.number(),
            closing.output.consensus_header_hash(),
        ),
        ..Default::default()
    }
}

/// Epochs of outputs saved into real packs and executed into one datadir per format.
struct Run {
    tmp: TempDir,
    chain: Arc<RethChainSpec>,
    consensus: ConsensusChain,
    /// Every output, in order.
    outputs: Vec<LoadOutput>,
    /// Each epoch's committee.
    committees: Vec<Committee>,
    /// One env per format asked, in order, each with its task manager.
    envs: Vec<(TxBodies, RethEnv, TaskManager)>,
}

impl Run {
    fn epochs_dir(&self) -> PathBuf {
        self.tmp.path().join("epochs")
    }

    fn datadir(&self, tx_bodies: TxBodies) -> PathBuf {
        self.tmp.path().join(tx_bodies.to_string())
    }

    fn env(&self, tx_bodies: TxBodies) -> &RethEnv {
        &self.envs.iter().find(|(format, ..)| *format == tx_bodies).expect("executed").1
    }

    /// Save `epochs` epochs of outputs into packs and execute them into a datadir per format of
    /// `formats`, each with a cache of `cache_bytes`, the packs attached before the first output.
    /// Every epoch but the last is closed through the registry and handed off: `during_handoff`
    /// is called with the closing epoch before the handoff starts, and the closure it returns runs
    /// once the handoff returns.
    async fn new(
        formats: &[TxBodies],
        cache_bytes: usize,
        epochs: usize,
        mut during_handoff: impl FnMut(&Run, Epoch) -> Box<dyn FnOnce() + Send>,
    ) -> eyre::Result<Self> {
        let tmp = TempDir::new()?;
        let spec = spec();
        let chain: Arc<RethChainSpec> = Arc::new(spec.genesis().into());
        let mut envs = Vec::new();
        for &format in formats {
            let tasks = TaskManager::new("tx body packs");
            let path = tmp.path().join(format.to_string());
            let env = open(&path, &chain, format, cache_bytes, &tasks)?;
            envs.push((format, env, tasks));
        }
        let workload = WorkloadConfig { seed: 5, ..WorkloadConfig::default() };
        let mut factory = OutputFactory::new(&envs[0].1, spec, workload, output_config())?;
        let committee = factory.committee().cloned().expect("a registry committee");
        let epochs_dir = tmp.path().join("epochs");
        fs::create_dir_all(&epochs_dir)?;
        let consensus = ConsensusChain::new_for_test(epochs_dir.clone(), committee.clone()).await?;
        for (_, env, _) in &envs {
            env.attach_tx_packs(&consensus, epochs_dir.clone())?;
        }
        let mut run =
            Self { tmp, chain, consensus, outputs: Vec::new(), committees: vec![committee], envs };

        for epoch in 0..epochs as Epoch {
            let closes = (epoch as usize) + 1 < epochs;
            let mut loads = factory.generate(OUTPUTS_PER_EPOCH);
            if closes {
                loads.push(factory.next_output_closing()?);
            }
            for load in loads {
                assert_eq!(load.output.leader().epoch(), epoch);
                // the pack first, durable, as state sync saves and persists before execution
                run.consensus.save_consensus_output(load.output.clone()).await?;
                run.consensus.persist_current().await?;
                for (_, env, _) in &run.envs {
                    execute(env, &load)?;
                }
                run.outputs.push(load);
            }
            if closes {
                let next = factory.begin_epoch(&run.envs[0].1)?.clone();
                let closing = run.outputs.last().expect("the closing output");
                let record = record(epoch, &run.committees[epoch as usize], &next, closing);
                let finish = during_handoff(&run, epoch);
                run.consensus.new_epoch(record, next.clone()).await?;
                finish();
                run.committees.push(next);
            }
        }
        Ok(run)
    }

    /// The pointer datadir reopened with a cache of `cache_bytes`, packs attached.
    fn reopen_pointers(&self, cache_bytes: usize) -> eyre::Result<(RethEnv, TaskManager)> {
        let (env, tasks) = self.reopen_unattached(cache_bytes)?;
        env.attach_tx_packs(&self.consensus, self.epochs_dir())?;
        Ok((env, tasks))
    }

    /// The pointer datadir reopened with a cache of `cache_bytes`, no packs attached.
    fn reopen_unattached(&self, cache_bytes: usize) -> eyre::Result<(RethEnv, TaskManager)> {
        let tasks = TaskManager::new("tx body packs reopened");
        let path = self.datadir(TxBodies::Pointers);
        let env = open(&path, &self.chain, TxBodies::Pointers, cache_bytes, &tasks)?;
        Ok((env, tasks))
    }

    /// Drop the env of `tx_bodies`, so its datadir can be reopened.
    fn close(&mut self, tx_bodies: TxBodies) {
        self.envs.retain(|(format, ..)| *format != tx_bodies);
    }
}

/// A reader of `txs` through `env` in a loop on its own thread, started once it has read them all
/// once, until the returned stop runs: every read must give the transaction's bytes. The stop
/// waits for two more passes (so a whole pass ran after whatever happened meanwhile), then
/// asserts no read failed.
fn reader(env: RethEnv, txs: Vec<StoredTx>) -> Box<dyn FnOnce() + Send> {
    let stop = Arc::new(AtomicBool::new(false));
    let passes = Arc::new(AtomicUsize::new(0));
    let thread = std::thread::spawn({
        let (stop, passes) = (stop.clone(), passes.clone());
        move || -> Vec<String> {
            let mut failures = Vec::new();
            while !stop.load(Ordering::Acquire) {
                for tx in &txs {
                    let read = env
                        .blockchain_provider()
                        .database_provider_ro()
                        .and_then(|provider| provider.transaction_by_id(tx.id));
                    match read {
                        Ok(Some(read)) if read.encoded_2718() == tx.encoded => {}
                        other => failures.push(format!("tx {}: {other:?}", tx.id)),
                    }
                }
                passes.fetch_add(1, Ordering::AcqRel);
            }
            failures
        }
    });
    // the handoff starts after a whole pass of the live pack
    while passes.load(Ordering::Acquire) == 0 {
        std::thread::yield_now();
    }
    Box::new(move || {
        let after = passes.load(Ordering::Acquire) + 2;
        while passes.load(Ordering::Acquire) < after {
            std::thread::yield_now();
        }
        stop.store(true, Ordering::Release);
        let failures = thread.join().expect("the reader");
        assert!(failures.is_empty(), "reads across the handoff failed: {failures:?}");
    })
}

/// Executed from real packs, a pointer datadir reads every transaction exactly as the inline one
/// does: with no cache (the live epoch from the live pack, the closed ones from their packs), with
/// the default cache (filled from the packs, then answering), and while each epoch's handoff seals
/// the pack a reader is reading. The inline datadir needs no pack at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_packs_resolve_every_transaction_as_the_inline_datadir_stores_it() -> eyre::Result<()>
{
    let mut run = Run::new(&[TxBodies::Inline, TxBodies::Pointers], 0, 3, |run, epoch| {
        // the epoch's transactions as the inline datadir stores them, read through the pointer
        // datadir (no cache: every read takes the packs) while the handoff seals the epoch
        let inline = read_all(run.env(TxBodies::Inline)).expect("inline reads");
        let txs: Vec<StoredTx> = inline.into_iter().filter(|tx| tx.epoch == epoch).collect();
        assert!(!txs.is_empty(), "epoch {epoch} stored transactions");
        reader(run.env(TxBodies::Pointers).clone(), txs)
    })
    .await?;

    let inline = read_all(run.env(TxBodies::Inline))?;
    for epoch in 0..3 {
        assert!(inline.iter().any(|tx| tx.epoch == epoch), "epoch {epoch} stored transactions");
    }
    assert_eq!(read_all(run.env(TxBodies::Pointers))?, inline, "no cache");

    // the default cache: the first pass fills it from the packs, the second reads it
    run.close(TxBodies::Pointers);
    let (env, _tasks) = run.reopen_pointers(TX_BODY_CACHE_BYTES)?;
    assert_eq!(read_all(&env)?, inline, "default cache, from the packs");
    assert_eq!(read_all(&env)?, inline, "default cache, from the cache");
    drop(env);

    // the inline datadir attaches without checking for packs, and reads without them
    run.close(TxBodies::Inline);
    fs::rename(run.epochs_dir().join("epoch-1"), run.tmp.path().join("epoch-1-aside"))?;
    let tasks = TaskManager::new("inline reopened");
    let env = open(&run.datadir(TxBodies::Inline), &run.chain, TxBodies::Inline, 0, &tasks)?;
    env.attach_tx_packs(&run.consensus, run.epochs_dir())?;
    assert_eq!(read_all(&env)?, inline);
    Ok(())
}

/// Assert `failure` is the resolve of `tx`'s block, epoch and batch.
fn assert_names(failure: &TxBodyError, tx: &StoredTx) {
    assert_eq!((failure.block, failure.epoch, failure.digest), (tx.block, tx.epoch, tx.digest));
}

/// A lost pack stops the start (the coverage check names the epoch); a pack lost after the check,
/// a partial copy of the epoch, another epoch's pack under its name and a damaged batch record
/// each fail the read of a transaction stored in it with an error naming the block, the epoch and
/// the batch, while the other epochs' transactions still read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_or_damaged_pack_refuses_the_start_or_fails_the_read() -> eyre::Result<()> {
    let mut run = Run::new(&[TxBodies::Pointers], 0, 3, |_, _| Box::new(|| ())).await?;
    let stored = read_all(run.env(TxBodies::Pointers))?;
    run.close(TxBodies::Pointers);
    let epochs_dir = run.epochs_dir();
    let epoch1 = epochs_dir.join("epoch-1");
    let aside = run.tmp.path().join("epoch-1-aside");
    // epoch 1's first transaction (its first output) and its last (its closing output)
    let in_epoch1: Vec<&StoredTx> = stored.iter().filter(|tx| tx.epoch == 1).collect();
    let (first1, last1) = (in_epoch1[0], *in_epoch1.last().expect("epoch 1 transactions"));
    assert_ne!(first1.digest, last1.digest);
    let in_epoch0 = stored.iter().find(|tx| tx.epoch == 0).expect("epoch 0 transactions");
    let in_epoch2 = stored.iter().find(|tx| tx.epoch == 2).expect("epoch 2 transactions");

    // a lost pack: the start refuses, naming the epoch
    fs::rename(&epoch1, &aside)?;
    let (env, _tasks) = run.reopen_unattached(0)?;
    let err = env.attach_tx_packs(&run.consensus, epochs_dir.clone()).expect_err("refused");
    let coverage = err.downcast_ref::<TxPackCoverageError>().expect("the coverage refusal");
    let blocks = stored[0].block..=stored.last().expect("transactions").block;
    assert_eq!(
        (coverage.blocks.clone(), coverage.epochs.clone(), &coverage.epochs_dir, &coverage.missing),
        (blocks, 0..=2, &epochs_dir, &vec![1])
    );
    let message = err.to_string();
    for says in [
        "lacks epoch(s) 1.",
        "Restore `consensus-db` from a backup",
        "db load-state` from epoch 1 or a later one",
    ] {
        assert!(message.contains(says), "{message}");
    }
    drop(env);

    // a pack lost after the check: its transactions fail, the other epochs' still read
    fs::rename(&aside, &epoch1)?;
    let (env, _tasks) = run.reopen_pointers(0)?;
    fs::rename(&epoch1, &aside)?;
    let err = read(&env, first1).expect_err("the pack is lost");
    let lost = resolve_error(&err);
    assert_names(lost, first1);
    assert!(matches!(lost.kind, TxBodyErrorKind::PackMissing), "{lost}");
    assert_eq!(read(&env, in_epoch0)?, in_epoch0.encoded);
    assert_eq!(read(&env, in_epoch2)?, in_epoch2.encoded);
    drop(env);

    // a partial copy of epoch 1, holding its first output only: the later batches are missing
    let partial_dir = run.tmp.path().join("partial");
    fs::create_dir_all(&partial_dir)?;
    let committees = &run.committees;
    let partial = ConsensusChain::new_for_test(partial_dir.clone(), committees[0].clone()).await?;
    let closing0 = &run.outputs[OUTPUTS_PER_EPOCH];
    let first_of_epoch1 = &run.outputs[OUTPUTS_PER_EPOCH + 1];
    let record0 = record(0, &committees[0], &committees[1], closing0);
    partial.new_epoch(record0, committees[1].clone()).await?;
    partial.save_consensus_output(first_of_epoch1.output.clone()).await?;
    partial.persist_current().await?;
    let record1 = record(1, &committees[1], &committees[2], first_of_epoch1);
    partial.new_epoch(record1, committees[2].clone()).await?;
    partial.close().await;
    copy_dir(&partial_dir.join("epoch-1"), &epoch1)?;
    let (env, _tasks) = run.reopen_pointers(0)?;
    assert_eq!(read(&env, first1)?, first1.encoded, "the copy holds the first output");
    let err = read(&env, last1).expect_err("the copy lacks the batch");
    let missing = resolve_error(&err);
    assert_names(missing, last1);
    assert!(matches!(missing.kind, TxBodyErrorKind::BatchMissing), "{missing}");
    drop(env);
    fs::remove_dir_all(&epoch1)?;

    // epoch 0's pack under epoch 1's name: refused at its open as another epoch's, never read
    copy_dir(&epochs_dir.join("epoch-0"), &epoch1)?;
    let (env, _tasks) = run.reopen_pointers(0)?;
    let err = read(&env, first1).expect_err("another epoch's pack");
    let misplaced = resolve_error(&err);
    assert_names(misplaced, first1);
    assert!(
        matches!(&misplaced.kind, TxBodyErrorKind::Read(e) if matches!(**e, BatchReadError::Open(_))),
        "another epoch's pack fails its open: {misplaced}"
    );
    drop(env);
    fs::remove_dir_all(&epoch1)?;

    // a byte of the batch record holding epoch 1's last transaction flipped at rest
    fs::rename(&aside, &epoch1)?;
    let data = epoch1.join(DATA_NAME);
    let at = batch_record_position(&data, 1, last1.digest)? + 8;
    flip_byte(&data, at)?;
    let (env, _tasks) = run.reopen_pointers(0)?;
    let err = read(&env, last1).expect_err("a damaged record");
    let damaged = resolve_error(&err);
    assert_names(damaged, last1);
    assert!(
        matches!(&damaged.kind, TxBodyErrorKind::Read(e) if matches!(**e, BatchReadError::Read(_))),
        "a damaged record is a read error: {damaged}"
    );
    assert_eq!(read(&env, first1)?, first1.encoded, "the other batches still read");
    Ok(())
}

/// Where the record of batch `digest` starts in the sealed data log `data` of `epoch`.
fn batch_record_position(data: &Path, epoch: Epoch, digest: B256) -> eyre::Result<u64> {
    let file = fs::File::open(data)?;
    let end = file.metadata()?.len();
    let mut records = PackIter::<PackRecord, fs::File>::open(file, u64::from(epoch), end)?;
    loop {
        let at = records.logical_position();
        match records.next().transpose()? {
            Some(PackRecord::Batch(batch)) if batch.digest() == digest => return Ok(at),
            Some(_) => {}
            None => eyre::bail!("no batch {digest} in {}", data.display()),
        }
    }
}

/// Flip every bit of the byte at `at` in `path`.
fn flip_byte(path: &Path, at: u64) -> eyre::Result<()> {
    let mut file = fs::OpenOptions::new().read(true).write(true).open(path)?;
    let mut byte = [0u8];
    file.seek(SeekFrom::Start(at))?;
    file.read_exact(&mut byte)?;
    byte[0] = !byte[0];
    file.seek(SeekFrom::Start(at))?;
    file.write_all(&byte)?;
    Ok(())
}

/// Rows hand-written one position past the transactions they belong to (the persist told the
/// positions of the next transactions): by number a row reads the batch's next transaction; by
/// hash the resolved transaction's hash is not the one asked, and the read fails; the last row is
/// past its batch and fails either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_past_their_batch_or_naming_another_transaction_fail() -> eyre::Result<()> {
    let run = Run::new(&[TxBodies::Pointers], 0, 1, |_, _| Box::new(|| ())).await?;
    let env = run.env(TxBodies::Pointers);
    let first = read_all(env)?.len() as TxNumber;
    // one more output, whose first block's rows each name the next position
    let workload = WorkloadConfig { seed: 9, ..WorkloadConfig::default() };
    let load = OutputFactory::new(env, spec(), workload, output_config())?.next_output();
    run.consensus.save_consensus_output(load.output.clone()).await?;
    run.consensus.persist_current().await?;
    let batch = load.batch(0).clone();
    let len = batch.transactions.len();
    assert!(len >= 2, "the first batch holds {len} transactions");
    execute_with(env, &load, |positions| {
        let built = positions[0].as_ref().expect("stored by position").to_vec();
        assert_eq!(built, (0..len as u32).collect::<Vec<_>>(), "no transaction dropped");
        positions[0] = Some((1..=len as u32).collect());
    })?;

    let provider = env.blockchain_provider().database_provider_ro()?;
    let hash_at = |pos: usize| -> B256 {
        *TransactionSigned::decode_2718_exact(&batch.transactions[pos]).expect("decodes").tx_hash()
    };
    let read = provider.transaction_by_id(first)?.expect("a row");
    assert_eq!(read.encoded_2718(), batch.transactions[1], "by number: the next transaction");
    let err = provider.transaction_by_hash(hash_at(0)).expect_err("another transaction");
    match read_error(&err) {
        Some(TxBodyReadError::TxBodyMismatch { hash, tx, resolved }) => {
            assert_eq!((*hash, *tx, *resolved), (hash_at(0), first, hash_at(1)));
        }
        other => panic!("expected a hash mismatch, got {other:?}: {err}"),
    }
    // the last row names the position after the batch's last
    let last = first + len as TxNumber - 1;
    let by_number = provider.transaction_by_id(last).map(|tx| tx.expect("a row").encoded_2718());
    let by_hash =
        provider.transaction_by_hash(hash_at(len - 1)).map(|tx| tx.expect("a row").encoded_2718());
    for read in [by_number, by_hash] {
        let err = read.expect_err("past the batch");
        let failure = resolve_error(&err);
        assert_eq!((failure.epoch, failure.digest), (0, batch.digest()), "{failure}");
        assert!(
            matches!(failure.kind, TxBodyErrorKind::PositionOutOfRange { pos, len: l }
                if pos as usize == len && l == len),
            "{failure}"
        );
    }
    Ok(())
}
