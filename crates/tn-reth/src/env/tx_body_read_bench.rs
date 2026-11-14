//! On-demand benchmark of the reads a datadir that stores its transactions by batch position
//! (layout marker version 2, `env::tx_bodies`) answers from consensus packs, against the same
//! chain stored inline, and of the space each takes: [`tx_body_read_bench`], `#[ignore]`d.
//!
//! ```text
//! TMPDIR=<dir on the disk under test> cargo test --release -p tn-reth --lib \
//!     env::tx_body_read_bench::tx_body_read_bench -- --ignored --exact --nocapture
//! ```
//!
//! It needs the tn-lmdb build (the default features): transactions are stored by position on
//! storage layout v2 only, which the MDBX build cannot open. On the MDBX build it prints so and
//! passes.
//!
//! **Setup.** A `ConsensusRegistry` genesis ([`PrefillSpec`], `PREFILL` accounts and `SENDERS`
//! senders). [`OutputFactory`] outputs of `BATCHES` batches near `GAS` gas each over `EPOCHS`
//! epochs: `OUTPUTS` per epoch, and in every epoch but the last one more that closes it. Each
//! output is saved into a real consensus chain (`ConsensusChain::new_for_test` in the temp dir's
//! `epochs/`) and made durable (`persist_current`) before it executes, as state sync saves outputs,
//! then executed as the engine executes it (`recover_output_signers`, one block per batch with
//! `build_block_from_recovered_payload`, `persist_executed_output` with the transactions' batch
//! positions, `cache_persisted_batches`, the blocks announced and finalized) into a datadir that
//! stores transactions inline and one that stores them by position, whose resolver reads the
//! chain's packs (`RethEnv::attach_tx_packs`). After each closing output the epoch is handed off
//! (`ConsensusChain::new_epoch`), which seals its pack; the last epoch stays live. Both datadirs
//! open as the node opens them (production `--db.*` defaults, durable sync) and are closed after
//! the build, then reopened for each column below, so no read is answered from the canonical
//! in-memory state and each column starts from its own cache state.
//!
//! **Reads**, each through the env's `BlockchainProvider` (the provider the RPC serves from), on a
//! block or transaction drawn at random (seeded) from the column's blocks:
//! - `tx by hash`: `transaction_by_hash_with_meta`, the `eth_getTransactionByHash` path;
//! - `receipt by hash`: `transaction_by_hash_with_meta`, `receipt_by_hash`, then the block's
//!   receipts (`receipts_by_block`), the `eth_getTransactionReceipt` path;
//! - `full block`: `sealed_block_with_senders`, the shape of `eth_getBlockByNumber(.., true)` and
//!   `eth_getBlockReceipts`;
//! - `logs k=1`, `logs k=10`, `logs k=all`: `transaction_by_id` of the block's first k
//!   transactions, one call each, as `eth_getLogs` fetches the transaction of each matching
//!   receipt;
//! - `tx range`: `RethEnv::transactions_by_tx_range_with_meta` of `RANGE` transactions from a
//!   block's first, across blocks (the ExEx feed);
//! - `rpc tx by hash` and `rpc receipt`: `eth_getTransactionByHash` and `eth_getTransactionReceipt`
//!   through the env's in-process RPC methods (no socket), end to end.
//!
//! **Columns**, each a fresh open of a datadir (p50 and p99 of `SAMPLES` timed reads per cell, in
//! µs; before each timed read an untimed step puts the column's state in place):
//! - `inline`: the inline datadir; the step runs the read once (a warm page cache);
//! - `recent`: by position, default cache (`TX_BODY_CACHE_BYTES`); the step copies the read's
//!   batches into the cache's recent part as a persist does (`TxBodySource::fill_recent`);
//! - `cold hit`: by position, default cache, closed epochs; the step runs the read once, which
//!   reads the packs and fills the cache's cold part;
//! - `pack warm`: by position, no cache (budget 0), closed epochs; the step runs the read once, so
//!   the pack's reader is open and its pages cached;
//! - `handle miss`: as `pack warm`, then the step drops the reader of each epoch the read touches
//!   (`PackSource::forget`), so the timed read opens the pack again, as a read of an epoch outside
//!   the reader LRU does (its bloom and index reads): the cost that sets the LRU's size;
//! - `cold pages` (Linux only; skipped elsewhere, which has no per-file page-cache eviction): as
//!   `handle miss`, and the step also drops the epoch's pack files from the page cache
//!   (`posix_fadvise(POSIX_FADV_DONTNEED)`);
//! - `live pack`: by position, no cache, the live epoch's blocks: the live pack's own reads.
//!
//! The step checks the state it set: the batch in the part of the cache the column names, none
//! cached with budget 0, the reader dropped. A read that answers nothing, or other than every
//! transaction asked, fails the bench.
//!
//! **Space**, of the closed datadirs: allocated bytes (`du`) and apparent bytes (the files'
//! lengths) of `static_files/` and of the `Transactions` segment's files (data, `.off`, `.conf`),
//! of `db/`, and once of `epochs/` (the packs, which a node keeps either way), then the
//! `Transactions` segment's bytes per transaction and what storing by position saved.
//!
//! **Knobs** (environment variables `TN_TX_READ_BENCH_*`, each refused when malformed, as is an
//! unknown name):
//! - `OUTPUTS` (3): outputs per epoch besides the closing one, at least 1;
//! - `EPOCHS` (3): epochs, every one but the last closed, at least 2;
//! - `BATCHES` (20) and `GAS` (30000000): batches per output and each batch's target gas (the
//!   execution bench's peak profile: about 600 transactions a batch);
//! - `PREFILL` (10000), `SENDERS` (1000), `POOLS` (8): the genesis;
//! - `SAMPLES` (300): timed reads per cell, at least 1;
//! - `RANGE` (1000): transactions per `tx range` read, at least 1;
//! - `SEED` (0): the workload's and the draws' seed.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use clap::Parser as _;
use jsonrpsee::{rpc_params, Methods, RpcModule};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_provider::{
    BlockBodyIndicesProvider as _, BlockNumReader as _, BlockReader as _,
    DatabaseProviderFactory as _, HeaderProvider as _, ReceiptProvider as _, TransactionVariant,
    TransactionsProvider as _,
};
use tempfile::TempDir;
use tn_storage::{archive::data_file::drop_cached_pages, consensus::ConsensusChain};
use tn_types::{
    gas_accumulator::{BaseFeeContainer, GasAccumulator},
    max_batch_gas, Batch, BlockNumber, Committee, ConsensusNumHash, Epoch, EpochRecord, Hash as _,
    NodeMode, TaskManager, TxNumber, B256,
};

use crate::{
    init_reth_defaults,
    payload::TNPayload,
    test_utils::load::{
        dir_bytes, static_file_sizes, workload::WorkloadConfig, LoadOutput, OutputConfig,
        OutputFactory, PrefillSpec, SegmentFiles,
    },
    worker::WorkerNetwork,
    NewCanonicalChain, OutputBuildScope, RethChainSpec, RethCommand, RethConfig, RethEnv, TxBodies,
    TX_BODY_CACHE_BYTES,
};

/// The knobs' prefix.
const KNOB_PREFIX: &str = "TN_TX_READ_BENCH_";

/// Every knob, after the prefix.
const KNOB_NAMES: [&str; 10] = [
    "OUTPUTS", "EPOCHS", "BATCHES", "GAS", "PREFILL", "SENDERS", "POOLS", "SAMPLES", "RANGE",
    "SEED",
];

/// The bench's knobs (see the module docs).
#[derive(Clone, Copy, Debug)]
struct Knobs {
    outputs: usize,
    epochs: usize,
    batches: usize,
    gas: u64,
    prefill: usize,
    senders: usize,
    pools: usize,
    samples: usize,
    range: u64,
    seed: u64,
}

impl Knobs {
    /// The knobs from this process's environment, the defaults for those unset.
    fn from_env() -> eyre::Result<Self> {
        let mut set = HashMap::new();
        for (name, value) in std::env::vars() {
            let Some(knob) = name.strip_prefix(KNOB_PREFIX) else { continue };
            eyre::ensure!(
                KNOB_NAMES.contains(&knob),
                "unknown knob {name}: this bench reads {KNOB_PREFIX}{{{}}}",
                KNOB_NAMES.join(",")
            );
            set.insert(knob.to_owned(), value);
        }
        let number = |knob: &str, default: u64, min: u64| -> eyre::Result<u64> {
            let Some(value) = set.get(knob) else { return Ok(default) };
            let parsed: u64 = value.parse().map_err(|e| {
                eyre::eyre!("{KNOB_PREFIX}{knob}={value:?} is not a whole number: {e}")
            })?;
            eyre::ensure!(parsed >= min, "{KNOB_PREFIX}{knob}={parsed} is below {min}");
            Ok(parsed)
        };
        let count = |knob: &str, default: usize, min: usize| -> eyre::Result<usize> {
            Ok(usize::try_from(number(knob, default as u64, min as u64)?)?)
        };
        Ok(Self {
            outputs: count("OUTPUTS", 3, 1)?,
            epochs: count("EPOCHS", 3, 2)?,
            batches: count("BATCHES", 20, 1)?,
            gas: number("GAS", 30_000_000, 1)?,
            prefill: count("PREFILL", 10_000, 0)?,
            senders: count("SENDERS", 1_000, 1)?,
            pools: count("POOLS", 8, 1)?,
            samples: count("SAMPLES", 300, 1)?,
            range: number("RANGE", 1_000, 1)?,
            seed: number("SEED", 0, 0)?,
        })
    }

    /// The genesis.
    fn spec(&self) -> PrefillSpec {
        PrefillSpec {
            accounts: self.prefill,
            senders: self.senders,
            pools: self.pools,
            seed: self.seed,
            registry: true,
        }
    }
}

/// A read the bench times (see the module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Read {
    TxByHash,
    ReceiptByHash,
    FullBlock,
    /// The block's first k transactions by id; `None` for all of them.
    Logs(Option<u64>),
    TxRange,
    RpcTx,
    RpcReceipt,
}

impl Read {
    /// Every read, in the table's order.
    const ALL: [Self; 9] = [
        Self::TxByHash,
        Self::ReceiptByHash,
        Self::FullBlock,
        Self::Logs(Some(1)),
        Self::Logs(Some(10)),
        Self::Logs(None),
        Self::TxRange,
        Self::RpcTx,
        Self::RpcReceipt,
    ];

    /// Its row label.
    fn label(self) -> String {
        match self {
            Self::TxByHash => "tx by hash".to_owned(),
            Self::ReceiptByHash => "receipt by hash".to_owned(),
            Self::FullBlock => "full block".to_owned(),
            Self::Logs(Some(k)) => format!("logs k={k}"),
            Self::Logs(None) => "logs k=all".to_owned(),
            Self::TxRange => "tx range".to_owned(),
            Self::RpcTx => "rpc tx by hash".to_owned(),
            Self::RpcReceipt => "rpc receipt".to_owned(),
        }
    }
}

/// A column: which datadir, its cache, its blocks and the untimed step before each read (see the
/// module docs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Column {
    Inline,
    Recent,
    ColdHit,
    PackWarm,
    HandleMiss,
    ColdPages,
    LivePack,
}

impl Column {
    /// Every column, in the table's order.
    const ALL: [Self; 7] = [
        Self::Inline,
        Self::Recent,
        Self::ColdHit,
        Self::PackWarm,
        Self::HandleMiss,
        Self::ColdPages,
        Self::LivePack,
    ];

    /// Its header.
    fn label(self) -> &'static str {
        match self {
            Self::Inline => "inline",
            Self::Recent => "recent",
            Self::ColdHit => "cold hit",
            Self::PackWarm => "pack warm",
            Self::HandleMiss => "handle miss",
            Self::ColdPages => "cold pages",
            Self::LivePack => "live pack",
        }
    }

    /// The datadir it reads.
    fn tx_bodies(self) -> TxBodies {
        if self == Self::Inline {
            TxBodies::Inline
        } else {
            TxBodies::Pointers
        }
    }

    /// Its decoded-batch cache budget in bytes.
    fn cache_bytes(self) -> usize {
        match self {
            Self::Inline | Self::Recent | Self::ColdHit => TX_BODY_CACHE_BYTES,
            Self::PackWarm | Self::HandleMiss | Self::ColdPages | Self::LivePack => 0,
        }
    }

    /// Whether it reads blocks of `epoch`, `live` being the live epoch.
    fn reads_epoch(self, epoch: Epoch, live: Epoch) -> bool {
        match self {
            Self::Inline | Self::Recent => true,
            Self::LivePack => epoch == live,
            Self::ColdHit | Self::PackWarm | Self::HandleMiss | Self::ColdPages => epoch != live,
        }
    }
}

/// An executed block with transactions.
#[derive(Clone, Debug)]
struct BlockInfo {
    number: BlockNumber,
    /// The epoch whose pack holds its batch (the header nonce's high half).
    epoch: Epoch,
    /// Its batch's digest (the header's `ommers_hash`).
    digest: B256,
    first_tx: TxNumber,
    hashes: Vec<B256>,
}

impl BlockInfo {
    fn txs(&self) -> u64 {
        self.hashes.len() as u64
    }
}

/// The chain both datadirs hold, and where they are.
struct Chain {
    tmp: TempDir,
    chain_spec: Arc<RethChainSpec>,
    consensus: ConsensusChain,
    /// Every block with transactions, in order.
    blocks: Vec<BlockInfo>,
    /// Every executed batch by digest, for the `recent` column's fill.
    batches: HashMap<B256, Batch>,
    /// The live epoch.
    live: Epoch,
}

impl Chain {
    fn datadir(&self, tx_bodies: TxBodies) -> PathBuf {
        self.tmp.path().join(tx_bodies.to_string())
    }

    fn epochs_dir(&self) -> PathBuf {
        self.tmp.path().join("epochs")
    }

    /// The transactions the chain holds.
    fn txs(&self) -> u64 {
        self.blocks.last().map_or(0, |block| block.first_tx + block.txs())
    }

    /// The blocks whose transactions `read` of `sample` touches: its block, and for a range the
    /// blocks after it that the range reaches.
    fn touched(&self, read: Read, sample: &Sample) -> &[BlockInfo] {
        let first = sample.block;
        if read != Read::TxRange {
            return &self.blocks[first..=first];
        }
        let end = self.blocks[first].first_tx + sample.range;
        let last = self.blocks[first..]
            .iter()
            .position(|block| block.first_tx + block.txs() >= end)
            .map_or(self.blocks.len() - 1, |offset| first + offset);
        &self.blocks[first..=last]
    }
}

/// One draw: a block (an index into [`Chain::blocks`]), a transaction of it, and the range read's
/// length (clamped to the chain's end).
#[derive(Clone, Copy, Debug)]
struct Sample {
    block: usize,
    tx: usize,
    range: u64,
}

/// A datadir opened as the node opens it, with its in-process RPC methods.
struct Opened {
    env: RethEnv,
    methods: Methods,
    tasks: TaskManager,
}

impl Opened {
    /// Close the env and wait for its tasks, so its datadir can be opened again.
    async fn close(self) -> eyre::Result<()> {
        let Self { env, methods, mut tasks } = self;
        drop((methods, env));
        tasks.update_tasks();
        tasks.signal_shutdown();
        tasks.abort();
        let grace = Duration::from_secs(10);
        eyre::ensure!(tasks.wait_for_exit(grace).await, "the env's tasks ran on after {grace:?}");
        Ok(())
    }
}

/// Open the datadir `datadir` of `chain` storing transactions `tx_bodies`, with a decoded-batch
/// cache of `cache_bytes`, as the node opens it; with `packs` (the consensus chain and its epochs
/// dir), its resolver reads them (an inline datadir takes none).
fn open(
    datadir: &Path,
    chain: &Arc<RethChainSpec>,
    packs: Option<(&ConsensusChain, PathBuf)>,
    tx_bodies: TxBodies,
    cache_bytes: usize,
) -> eyre::Result<Opened> {
    init_reth_defaults();
    let tasks = TaskManager::new("tx body read bench");
    let mut command = RethCommand::try_parse_from(["tn-reth", "--db.sync-mode", "durable"])?;
    command.rpc.http = true;
    command.rpc.ipcdisable = true;
    let config = RethConfig::new(command, None, datadir, true, chain.clone())
        .with_new_datadir_tx_bodies(tx_bodies)
        .with_tx_body_cache_bytes(cache_bytes);
    let database = RethEnv::new_database(&config, datadir.join("db"))?;
    let env = RethEnv::new(&config, &tasks, database, None, GasAccumulator::default())?;
    eyre::ensure!(env.datadir_format().tx_bodies == tx_bodies, "the datadir's format");
    if let Some((consensus, epochs_dir)) = packs {
        env.attach_tx_packs(consensus.clone(), epochs_dir)?;
    }
    let pool = env.init_txn_pool_without_maintenance(BaseFeeContainer::default())?;
    let methods = env
        .get_rpc_server(
            pool,
            WorkerNetwork::new_for_test(env.chainspec()),
            GasAccumulator::new(1).worker_base_fee(0),
            || NodeMode::CvvActive,
            RpcModule::new(()),
        )?
        .http_methods(|_| true)
        .ok_or_else(|| eyre::eyre!("the env serves no HTTP methods"))?;
    Ok(Opened { env, methods, tasks })
}

/// Execute `load` on `env` as the engine does: one block per batch under its digest, the blocks
/// persisted with their transactions' batch positions, the persisted batches handed to the cache,
/// then the blocks announced and finalized.
fn execute(env: &RethEnv, load: &LoadOutput) -> eyre::Result<()> {
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
    env.persist_executed_output(&blocks, &tx_positions)?;
    env.cache_persisted_batches(output, &tx_positions);
    env.announce_executed_output(blocks, None)?;
    env.finalize_block(parent)?;
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

/// Build the chain (see the module docs) and close both datadirs.
async fn build(knobs: &Knobs) -> eyre::Result<Chain> {
    let tmp = tempfile::Builder::new().prefix("tx-body-read-bench-").tempdir()?;
    let spec = knobs.spec();
    let chain_spec: Arc<RethChainSpec> = Arc::new(spec.genesis().into());
    let epochs_dir = tmp.path().join("epochs");
    std::fs::create_dir_all(&epochs_dir)?;
    let datadir = |tx_bodies: TxBodies| tmp.path().join(tx_bodies.to_string());
    // the consensus chain needs the genesis committee, which the factory reads from an env
    let inline =
        open(&datadir(TxBodies::Inline), &chain_spec, None, TxBodies::Inline, TX_BODY_CACHE_BYTES)?;
    let config =
        OutputConfig { batches: knobs.batches, batch_gas: knobs.gas, ..OutputConfig::default() };
    let workload = WorkloadConfig { seed: knobs.seed, ..WorkloadConfig::default() };
    let mut factory = OutputFactory::new(&inline.env, spec, workload, config)?;
    let committee = factory.committee().cloned().expect("a registry genesis has a committee");
    let consensus = ConsensusChain::new_for_test(epochs_dir.clone(), committee.clone()).await?;
    let pointers = open(
        &datadir(TxBodies::Pointers),
        &chain_spec,
        Some((&consensus, epochs_dir.clone())),
        TxBodies::Pointers,
        TX_BODY_CACHE_BYTES,
    )?;
    let targets = [&inline.env, &pointers.env];
    let mut committees = vec![committee];
    let mut batches = HashMap::new();
    let started = Instant::now();
    for epoch in 0..knobs.epochs as Epoch {
        let closes = (epoch as usize) + 1 < knobs.epochs;
        let mut loads = factory.generate(knobs.outputs);
        if closes {
            loads.push(factory.next_output_closing()?);
        }
        for load in &loads {
            eyre::ensure!(load.output.leader().epoch() == epoch, "an output of another epoch");
            // the pack first, durable, as state sync saves and persists before execution
            consensus.save_consensus_output(load.output.clone()).await?;
            consensus.persist_current().await?;
            for env in targets {
                execute(env, load)?;
            }
            for index in 0..load.batches.len() {
                let digest = load.output.get_batch_digest(index).expect("a digest per batch");
                batches.insert(digest, load.batch(index).clone());
            }
        }
        if closes {
            let next = factory.begin_epoch(targets[0])?.clone();
            let closing = loads.last().expect("the closing output");
            let record = record(epoch, &committees[epoch as usize], &next, closing);
            consensus.new_epoch(record, next.clone()).await?;
            committees.push(next);
        }
        let secs = started.elapsed().as_secs_f64();
        println!("tx_body_read_bench: epoch {epoch} built ({secs:.1} s)");
    }
    let blocks = block_infos(&inline.env)?;
    inline.close().await?;
    pointers.close().await?;
    let live = knobs.epochs as Epoch - 1;
    Ok(Chain { tmp, chain_spec, consensus, blocks, batches, live })
}

/// Every block of `env` with transactions: its epoch, digest, first transaction and hashes.
fn block_infos(env: &RethEnv) -> eyre::Result<Vec<BlockInfo>> {
    let provider = env.blockchain_provider().database_provider_ro()?;
    let mut blocks = Vec::new();
    for number in 1..=provider.last_block_number()? {
        let indices = provider.block_body_indices(number)?.expect("a body");
        if indices.tx_count == 0 {
            continue;
        }
        let header = provider.header_by_number(number)?.expect("a header");
        let hashes = provider
            .transactions_by_tx_range(indices.tx_num_range())?
            .iter()
            .map(|tx| *tx.tx_hash())
            .collect();
        blocks.push(BlockInfo {
            number,
            epoch: RethEnv::extract_epoch_from_header(&header),
            digest: header.ommers_hash,
            first_tx: indices.first_tx_num,
            hashes,
        });
    }
    Ok(blocks)
}

/// `SAMPLES` draws from the blocks `column` reads: a block, a transaction of it, and a range
/// from the block's first transaction that ends inside the column's blocks.
fn draws(knobs: &Knobs, chain: &Chain, column: Column, rng: &mut StdRng) -> Vec<Sample> {
    let eligible: Vec<usize> = (0..chain.blocks.len())
        .filter(|&i| column.reads_epoch(chain.blocks[i].epoch, chain.live))
        .collect();
    let last_tx = eligible.last().map_or(0, |&i| chain.blocks[i].first_tx + chain.blocks[i].txs());
    (0..knobs.samples)
        .map(|_| {
            let block = eligible[rng.random_range(0..eligible.len())];
            let info = &chain.blocks[block];
            let tx = rng.random_range(0..info.hashes.len());
            Sample { block, tx, range: knobs.range.min(last_tx - info.first_tx) }
        })
        .collect()
}

/// Run `read` of `sample` once on `opened`; an answer that is missing or short is an error.
fn run(chain: &Chain, opened: &Opened, read: Read, sample: &Sample) -> eyre::Result<()> {
    let provider = opened.env.blockchain_provider();
    let block = &chain.blocks[sample.block];
    let hash = block.hashes[sample.tx];
    let missing = || eyre::eyre!("{read:?} of block {} answered nothing", block.number);
    match read {
        Read::TxByHash => {
            provider.transaction_by_hash_with_meta(hash)?.ok_or_else(missing)?;
        }
        Read::ReceiptByHash => {
            let (_, meta) = provider.transaction_by_hash_with_meta(hash)?.ok_or_else(missing)?;
            provider.receipt_by_hash(hash)?.ok_or_else(missing)?;
            provider.receipts_by_block(meta.block_number.into())?.ok_or_else(missing)?;
        }
        Read::FullBlock => {
            let full = provider
                .sealed_block_with_senders(block.number.into(), TransactionVariant::WithHash)?
                .ok_or_else(missing)?;
            eyre::ensure!(full.body().transactions.len() as u64 == block.txs(), "a short block");
        }
        Read::Logs(k) => {
            let k = k.unwrap_or(block.txs()).min(block.txs());
            for id in block.first_tx..block.first_tx + k {
                provider.transaction_by_id(id)?.ok_or_else(missing)?;
            }
        }
        Read::TxRange => {
            let range = block.first_tx..=block.first_tx + sample.range - 1;
            let txs = opened.env.transactions_by_tx_range_with_meta(range)?;
            eyre::ensure!(txs.len() as u64 == sample.range, "a short range");
        }
        Read::RpcTx | Read::RpcReceipt => {
            let method = if read == Read::RpcTx {
                "eth_getTransactionByHash"
            } else {
                "eth_getTransactionReceipt"
            };
            let handle = tokio::runtime::Handle::current();
            let answer = handle
                .block_on(opened.methods.call::<_, serde_json::Value>(method, rpc_params![hash]))
                .map_err(|e| eyre::eyre!("{method}: {e}"))?;
            eyre::ensure!(!answer.is_null(), "{method} answered null");
        }
    }
    Ok(())
}

/// The untimed step before each timed read of `column` (see the module docs), and the check
/// that it set the state the column names.
fn prepare(
    chain: &Chain,
    opened: &Opened,
    column: Column,
    read: Read,
    sample: &Sample,
) -> eyre::Result<()> {
    let touched = chain.touched(read, sample);
    let source = &opened.env.inner.tx_bodies;
    match column {
        Column::Recent => {
            source.fill_recent(
                touched.iter().map(|block| (block.digest, &chain.batches[&block.digest])),
            );
        }
        Column::Inline | Column::ColdHit | Column::PackWarm | Column::LivePack => {
            run(chain, opened, read, sample)?;
        }
        Column::HandleMiss | Column::ColdPages => {
            run(chain, opened, read, sample)?;
            let packs = source.packs().ok_or_else(|| eyre::eyre!("no packs attached"))?;
            for epoch in epochs(touched) {
                packs.forget(epoch);
                eyre::ensure!(!packs.open_epochs().contains(&epoch), "epoch {epoch}'s reader kept");
                if column == Column::ColdPages {
                    evict_pack(&chain.epochs_dir().join(format!("epoch-{epoch}")))?;
                }
            }
        }
    }
    if column != Column::Inline {
        for block in touched {
            let (recent, cold) = source.cache().holds(&block.digest);
            let expected = match column {
                Column::Recent => recent,
                Column::ColdHit => cold && !recent,
                _ => !recent && !cold,
            };
            eyre::ensure!(
                expected,
                "{column:?}: the batch of block {} is cached recent {recent}, cold {cold}",
                block.number
            );
        }
    }
    Ok(())
}

/// The distinct epochs of `blocks`, in order.
fn epochs(blocks: &[BlockInfo]) -> Vec<Epoch> {
    let mut epochs: Vec<Epoch> = blocks.iter().map(|block| block.epoch).collect();
    epochs.dedup();
    epochs
}

/// Drop every file of the pack dir `dir` from the page cache (Linux; [`drop_cached_pages`]).
fn evict_pack(dir: &Path) -> eyre::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_file() {
            drop_cached_pages(&std::fs::File::open(&path)?)
                .map_err(|e| eyre::eyre!("evicting {}: {e}", path.display()))?;
        }
    }
    Ok(())
}

/// The p50 and p99 (nearest rank) of `samples`, in µs.
fn p50_p99(samples: &mut [Duration]) -> (f64, f64) {
    samples.sort_unstable();
    let rank = |p: usize| samples[(samples.len() * p).div_ceil(100).saturating_sub(1)];
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    (us(rank(50)), us(rank(99)))
}

/// Time every read of `column` on a fresh open of its datadir: per read, its p50 and p99 in µs.
async fn measure(
    knobs: &Knobs,
    chain: &Chain,
    column: Column,
    rng: &mut StdRng,
) -> eyre::Result<Vec<(f64, f64)>> {
    let tx_bodies = column.tx_bodies();
    let packs = (tx_bodies == TxBodies::Pointers).then(|| (&chain.consensus, chain.epochs_dir()));
    let opened =
        open(&chain.datadir(tx_bodies), &chain.chain_spec, packs, tx_bodies, column.cache_bytes())?;
    let mut cells = Vec::new();
    for read in Read::ALL {
        let samples = draws(knobs, chain, column, rng);
        let timed = tokio::task::block_in_place(|| -> eyre::Result<Vec<Duration>> {
            let mut timed = Vec::with_capacity(samples.len());
            for sample in &samples {
                prepare(chain, &opened, column, read, sample)?;
                let start = Instant::now();
                run(chain, &opened, read, sample)?;
                timed.push(start.elapsed());
            }
            Ok(timed)
        });
        let mut timed =
            timed.map_err(|e| e.wrap_err(format!("{} / {}", column.label(), read.label())))?;
        cells.push(p50_p99(&mut timed));
    }
    opened.close().await?;
    Ok(cells)
}

/// A path's apparent and allocated bytes as the space section prints them, in MiB.
fn mib_pair(bytes: (u64, u64)) -> String {
    let mib = |b: u64| b as f64 / (1 << 20) as f64;
    format!("{:>9.2} {:>9.2}", mib(bytes.1), mib(bytes.0))
}

/// The space section: each closed datadir's `static_files/`, its `Transactions` segment's files
/// and `db/`, then the packs once, and the segment's bytes per transaction.
fn space(chain: &Chain) -> eyre::Result<String> {
    let txs = chain.txs().max(1);
    let mut out = format!(
        "space (MiB allocated, apparent; {} transactions):\n  {:<34} {:>9} {:>9}\n",
        chain.txs(),
        "path",
        "alloc",
        "apparent"
    );
    let mut per_tx = Vec::new();
    for tx_bodies in [TxBodies::Inline, TxBodies::Pointers] {
        let datadir = chain.datadir(tx_bodies);
        let static_files = datadir.join("static_files");
        let segment = static_file_sizes(&static_files)?
            .into_iter()
            .find(|(name, _)| *name == "transactions")
            .map_or_else(SegmentFiles::default, |(_, files)| files);
        let rows = [
            ("static_files/", dir_bytes(&static_files)?),
            ("  transactions data", segment.data),
            ("  transactions .off", segment.offsets),
            ("  transactions .conf", segment.config),
            ("  transactions total", segment.total()),
            ("db/", dir_bytes(&datadir.join("db"))?),
        ];
        for (path, bytes) in rows {
            out += &format!("  {:<34} {}\n", format!("{tx_bodies} {path}"), mib_pair(bytes));
        }
        per_tx.push((tx_bodies, segment.total()));
    }
    let packs = mib_pair(dir_bytes(&chain.epochs_dir())?);
    out += &format!("  {:<34} {packs}\n", "epochs/ (the packs, shared)");
    for (tx_bodies, (len, allocated)) in &per_tx {
        out += &format!(
            "  {tx_bodies} transactions: {:.1} B/tx allocated, {:.1} B/tx apparent\n",
            *allocated as f64 / txs as f64,
            *len as f64 / txs as f64
        );
    }
    let saved = |part: fn(&(u64, u64)) -> u64| {
        part(&per_tx[0].1) as f64 / (1 << 20) as f64 - part(&per_tx[1].1) as f64 / (1 << 20) as f64
    };
    out += &format!(
        "  saved by storing by position: {:.2} MiB allocated, {:.2} MiB apparent\n",
        saved(|bytes| bytes.1),
        saved(|bytes| bytes.0)
    );
    Ok(out)
}

/// The bench (see the module docs).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "on-demand performance measurement; run with --release --ignored --exact --nocapture"]
async fn tx_body_read_bench() -> eyre::Result<()> {
    if !crate::env::StorageLayout::V2.is_supported() {
        println!(
            "tx_body_read_bench: transactions are stored by position on layout v2 only, which \
             this build (MDBX) cannot open: nothing to measure"
        );
        return Ok(());
    }
    let knobs = Knobs::from_env()?;
    println!("tx_body_read_bench: {knobs:?}, TMPDIR {}", std::env::temp_dir().display());
    let chain = build(&knobs).await?;
    let epochs_txs: Vec<String> = (0..knobs.epochs as Epoch)
        .map(|epoch| {
            let blocks: Vec<&BlockInfo> =
                chain.blocks.iter().filter(|b| b.epoch == epoch).collect();
            let txs: u64 = blocks.iter().map(|b| b.txs()).sum();
            format!("epoch {epoch}: {} blocks, {txs} txs", blocks.len())
        })
        .collect();
    println!(
        "tx_body_read_bench: {} txs over {} blocks ({}; epoch {} live)",
        chain.txs(),
        chain.blocks.len(),
        epochs_txs.join(", "),
        chain.live
    );

    let mut rng = StdRng::seed_from_u64(knobs.seed.wrapping_add(1));
    let mut columns = Vec::new();
    let mut notes = Vec::new();
    for column in Column::ALL {
        if column == Column::ColdPages && !cfg!(target_os = "linux") {
            notes.push("cold pages: skipped (Linux only: no per-file page-cache eviction here)");
            continue;
        }
        let started = Instant::now();
        columns.push((column, measure(&knobs, &chain, column, &mut rng).await?));
        let secs = started.elapsed().as_secs_f64();
        println!("tx_body_read_bench: {} measured ({secs:.1} s)", column.label());
    }

    let mut out = format!(
        "\ntx_body_read_bench: p50/p99 µs per read, {} samples per cell, {} txs per batch\n  {:<16}",
        knobs.samples,
        chain.txs() / chain.blocks.len().max(1) as u64,
        "read"
    );
    for (column, _) in &columns {
        out += &format!(" {:>17}", column.label());
    }
    out.push('\n');
    for (row, read) in Read::ALL.into_iter().enumerate() {
        out += &format!("  {:<16}", read.label());
        for (_, cells) in &columns {
            let (p50, p99) = cells[row];
            out += &format!(" {:>17}", format!("{p50:.1}/{p99:.1}"));
        }
        out.push('\n');
    }
    for note in notes {
        out += &format!("  {note}\n");
    }
    out += &space(&chain)?;
    println!("{out}");
    Ok(())
}
