//! TN(TN-0100): a timing harness for historical account and storage reads, storage v1
//! (changesets in the database) against storage v2 (changesets in static files).
//!
//! One generated chain is saved into a v1 and a v2 factory in batches of [`BATCH`] blocks, the
//! shape of a consensus output. The same historical reads then run against each, in four
//! phases:
//! - `cold`: the first pass after the last persist, which remapped the static files;
//! - `warm`: the same reads again;
//! - `remap`: the same reads, with the changeset jars reloaded every [`REMAP_EVERY`] reads, as a
//!   persist does, but without a writer;
//! - `busy`: the same reads while a writer thread persists further batches;
//! - `busy+holder`: as `busy`, while another thread holds the headers jar for [`HOLD`] at a time,
//!   as a range walk over a static file does.
//!
//! Each read is timed as `HistoricalStateProviderRef::basic_account` / `storage` (`full`), and,
//! when the history resolves it to a changeset, the changeset lookup alone (`cs`). Under v2 the
//! changeset lookup is also split into its steps: the jar, the `.csoff` read, the cursor and
//! the binary search. Every answer is compared between the layouts.
//!
//! Run: `vendor/reth/tn/cargo.sh test --release -p reth-provider --lib --
//! v2_history_read_timing_heavy --ignored --nocapture`.

use super::tn_v2_tests::executed;
use crate::{
    providers::{HistoryInfo, RocksDBBuilder, StaticFileProvider},
    test_utils::MockNodeTypesWithDB,
    ChangeSetReader, HistoricalStateProviderRef, ProviderFactory, SaveBlocksMode,
    StaticFileProviderFactory, StorageChangeSetReader,
};
use alloy_primitives::{keccak256, map::HashMap, Address, BlockNumber, B256, U256};
use rand::Rng;
use reth_chain_state::ExecutedBlock;
use reth_chainspec::MAINNET;
use reth_db::{
    mdbx::DatabaseArguments,
    static_file::{AccountChangesetMask, StorageChangesetMask},
    test_utils::TempDatabase,
};
use reth_db_api::models::StorageSettings;
use reth_ethereum_primitives::Block;
use reth_primitives_traits::{SealedBlock, SealedHeader, StorageSlotKey};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{AccountReader, StateProvider, StorageSettingsCache};
use reth_testing_utils::generators::{self, random_block, BlockParams};
use revm_database::BundleState;
use revm_state::AccountInfo;
use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

type Factory = ProviderFactory<MockNodeTypesWithDB>;

/// Plain accounts the chain changes.
const ACCOUNTS: usize = 4000;
/// Slots per token contract.
const SLOTS: usize = 4000;
/// Account changes per block.
const ACCOUNT_CHANGES: usize = 150;
/// Slot changes per token per block.
const SLOT_CHANGES: usize = 150;
/// Blocks saved before the reads.
const BLOCKS: BlockNumber = 2400;
/// Blocks the busy phases' writer saves.
const WRITER_BLOCKS: BlockNumber = 800;
/// Blocks per `save_blocks` + commit.
const BATCH: BlockNumber = 8;
/// Reads per phase (half accounts, half slots).
const QUERIES: usize = 20_000;
/// Reads between two jar reloads in the `remap` phase.
const REMAP_EVERY: usize = 32;
/// How long the `busy+holder` phase's holder keeps a jar.
const HOLD: Duration = Duration::from_millis(2);

const TOKENS: [Address; 2] = [Address::repeat_byte(0xa1), Address::repeat_byte(0xb2)];

fn account(i: usize) -> Address {
    Address::left_padding_from(&(i as u64 + 1).to_be_bytes())
}

fn slot(s: usize) -> U256 {
    U256::from_be_bytes(keccak256((s as u64).to_be_bytes()).0)
}

/// Blocks `0..=BLOCKS + WRITER_BLOCKS`: block 1 creates every account and slot, each later
/// block changes [`ACCOUNT_CHANGES`] accounts and [`SLOT_CHANGES`] slots of each token.
fn generate() -> Vec<ExecutedBlock> {
    let mut rng = generators::rng_with_seed(b"tn-0100 v2 history read timing");
    let mut balances = vec![U256::ZERO; ACCOUNTS];
    let mut slots = [vec![U256::ZERO; SLOTS], vec![U256::ZERO; SLOTS]];
    let token_info = AccountInfo { nonce: 1, ..Default::default() };

    let genesis = SealedBlock::<Block>::from_sealed_parts(
        SealedHeader::new(alloy_consensus::Header { number: 0, ..Default::default() }, B256::ZERO),
        Default::default(),
    );
    let mut parent = genesis.hash();
    let mut blocks = vec![executed(genesis, BundleState::default())];

    for number in 1..=BLOCKS + WRITER_BLOCKS {
        let block = random_block(
            &mut rng,
            number,
            BlockParams {
                parent: Some(parent),
                tx_count: Some(0),
                ommers_count: Some(0),
                ..Default::default()
            },
        );
        parent = block.hash();

        let mut builder = BundleState::builder(number..=number);
        let accounts: Vec<usize> = if number == 1 {
            (0..ACCOUNTS).collect()
        } else {
            let mut picked: Vec<usize> =
                (0..ACCOUNT_CHANGES).map(|_| rng.random_range(0..ACCOUNTS)).collect();
            picked.sort_unstable();
            picked.dedup();
            picked
        };
        for i in accounts {
            let before = (number > 1).then(|| AccountInfo {
                balance: balances[i],
                nonce: 1,
                ..Default::default()
            });
            balances[i] = U256::from(number * 1_000_000 + i as u64 + 1);
            let info = AccountInfo { balance: balances[i], nonce: 1, ..Default::default() };
            if let Some(before) = &before {
                builder = builder.state_original_account_info(account(i), before.clone());
            }
            builder = builder.state_present_account_info(account(i), info).revert_account_info(
                number,
                account(i),
                Some(before),
            );
        }
        for (t, token) in TOKENS.into_iter().enumerate() {
            let picked: Vec<usize> = if number == 1 {
                (0..SLOTS).collect()
            } else {
                let mut picked: Vec<usize> =
                    (0..SLOT_CHANGES).map(|_| rng.random_range(0..SLOTS)).collect();
                picked.sort_unstable();
                picked.dedup();
                picked
            };
            let mut present = HashMap::default();
            let mut reverts = Vec::new();
            for s in picked {
                let old = slots[t][s];
                let new = U256::from(number * 1_000_000 + s as u64 + 1);
                slots[t][s] = new;
                present.insert(slot(s), (old, new));
                reverts.push((slot(s), old));
            }
            if number == 1 {
                builder = builder
                    .state_present_account_info(token, token_info.clone())
                    .revert_account_info(number, token, Some(None));
            } else {
                builder = builder
                    .state_original_account_info(token, token_info.clone())
                    .state_present_account_info(token, token_info.clone());
            }
            builder = builder.state_storage(token, present).revert_storage(number, token, reverts);
        }
        blocks.push(executed(block, builder.build()));
    }
    blocks
}

fn save(factory: &Factory, blocks: &[ExecutedBlock]) {
    let provider_rw = factory.provider_rw().unwrap();
    provider_rw.save_blocks(blocks.to_vec(), SaveBlocksMode::Full).unwrap();
    provider_rw.commit().unwrap();
}

/// A test factory whose database may grow past `DatabaseArguments::test`'s 64 MiB.
fn factory() -> Factory {
    let datadir = reth_db::test_utils::tempdir_path();
    let static_files = datadir.join("static_files");
    std::fs::create_dir_all(&static_files).unwrap();
    let args = DatabaseArguments::test().with_geometry_max_size(Some(8 << 30));
    let db = reth_db::init_db(datadir.join("db"), args).unwrap();
    ProviderFactory::new(
        Arc::new(TempDatabase::new(db, datadir.clone())),
        MAINNET.clone(),
        StaticFileProvider::read_write(static_files).unwrap(),
        RocksDBBuilder::new(datadir.join("rocksdb")).with_default_tables().build().unwrap(),
        reth_tasks::Runtime::test(),
    )
    .unwrap()
}

/// A factory in `settings` with genesis and blocks `1..=BLOCKS`.
fn build(settings: StorageSettings, blocks: &[ExecutedBlock]) -> Factory {
    let factory = factory();
    factory.set_storage_settings_cache(settings);
    save(&factory, &blocks[..1]);
    for batch in blocks[1..=BLOCKS as usize].chunks(BATCH as usize) {
        save(&factory, batch);
    }
    factory
}

#[derive(Clone, Copy, Debug)]
enum Query {
    Account(BlockNumber, Address),
    Storage(BlockNumber, Address, B256),
}

fn queries() -> Vec<Query> {
    let mut rng = generators::rng_with_seed(b"tn-0100 queries");
    (0..QUERIES)
        .map(|i| {
            let block = rng.random_range(2..=BLOCKS);
            if i % 2 == 0 {
                Query::Account(block, account(rng.random_range(0..ACCOUNTS)))
            } else {
                let token = TOKENS[rng.random_range(0..2)];
                Query::Storage(block, token, B256::from(slot(rng.random_range(0..SLOTS))))
            }
        })
        .collect()
}

/// The v2 changeset lookup, step by step, as `StaticFileProvider::get_*_before_block` does it.
#[derive(Default)]
struct Steps {
    jar: Vec<Duration>,
    csoff: Vec<Duration>,
    cursor: Vec<Duration>,
    search: Vec<Duration>,
    probes: u64,
}

fn sf_steps(factory: &Factory, query: Query, changeset_block: BlockNumber, steps: &mut Steps) {
    let sf = factory.static_file_provider();
    let (segment, address) = match query {
        Query::Account(_, address) => (StaticFileSegment::AccountChangeSets, address),
        Query::Storage(_, address, _) => (StaticFileSegment::StorageChangeSets, address),
    };
    let t0 = Instant::now();
    let jar = sf.get_segment_provider_for_block(segment, changeset_block, None).unwrap();
    let t1 = Instant::now();
    let offset = jar.read_changeset_offset(changeset_block).unwrap().unwrap();
    let t2 = Instant::now();
    let mut cursor = jar.cursor().unwrap();
    let t3 = Instant::now();
    let range = offset.changeset_range();
    let (mut low, mut high) = (range.start, range.end);
    while low < high {
        let mid = low + (high - low) / 2;
        steps.probes += 1;
        let less = match query {
            Query::Account(..) => {
                cursor.get_one::<AccountChangesetMask>(mid.into()).unwrap().unwrap().address <
                    address
            }
            Query::Storage(_, _, key) => {
                let change = cursor.get_one::<StorageChangesetMask>(mid.into()).unwrap().unwrap();
                (change.address, change.key) < (address, key)
            }
        };
        if less {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    let t4 = Instant::now();
    steps.jar.push(t1 - t0);
    steps.csoff.push(t2 - t1);
    steps.cursor.push(t3 - t2);
    steps.search.push(t4 - t3);
}

/// Timings of one phase, by query kind and by how the history resolved it.
#[derive(Default)]
struct Phase {
    full: BTreeMap<&'static str, Vec<Duration>>,
    cs: BTreeMap<&'static str, Vec<Duration>>,
    open: Vec<Duration>,
    steps: Steps,
    answers: Vec<String>,
}

fn run_phase(
    factory: &Factory,
    queries: &[Query],
    remap: bool,
    steps: bool,
    stop: Option<&AtomicBool>,
) -> Phase {
    let mut phase = Phase::default();
    let sf = factory.static_file_provider();
    for (i, query) in queries.iter().copied().enumerate() {
        if stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
            break
        }
        if remap && i % REMAP_EVERY == 0 {
            for segment in
                [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
            {
                if let Some(tip) = sf.get_highest_static_file_block(segment) {
                    sf.update_index(segment, Some(tip)).unwrap();
                }
            }
        }
        let t0 = Instant::now();
        let provider = factory.provider().unwrap();
        let t1 = Instant::now();
        phase.open.push(t1 - t0);
        let block = match query {
            Query::Account(block, _) | Query::Storage(block, ..) => block,
        };
        let state = HistoricalStateProviderRef::new(&provider, block);
        let (kind, answer, history) = match query {
            Query::Account(_, address) => {
                let t = Instant::now();
                let answer = state.basic_account(&address).unwrap();
                let full = t.elapsed();
                let history = state.account_history_lookup(address).unwrap();
                (("account", full), format!("{answer:?}"), history)
            }
            Query::Storage(_, address, key) => {
                let t = Instant::now();
                let answer = state.storage(address, key).unwrap();
                let full = t.elapsed();
                let history =
                    state.storage_history_lookup(address, StorageSlotKey::plain(key)).unwrap();
                (("storage", full), format!("{answer:?}"), history)
            }
        };
        let resolved = match history {
            HistoryInfo::InChangeset(_) => "changeset",
            HistoryInfo::InPlainState | HistoryInfo::MaybeInPlainState => "plain",
            HistoryInfo::NotYetWritten => "unwritten",
        };
        let key: &'static str = match (kind.0, resolved) {
            ("account", "changeset") => "account/changeset",
            ("account", "plain") => "account/plain",
            ("storage", "changeset") => "storage/changeset",
            ("storage", "plain") => "storage/plain",
            _ => "other",
        };
        phase.full.entry(key).or_default().push(kind.1);
        phase.answers.push(answer);

        if let HistoryInfo::InChangeset(changeset_block) = history {
            let t = Instant::now();
            match query {
                Query::Account(_, address) => {
                    provider.get_account_before_block(changeset_block, address).unwrap().unwrap();
                }
                Query::Storage(_, address, key) => {
                    provider
                        .get_storage_before_block(changeset_block, address, key)
                        .unwrap()
                        .unwrap();
                }
            }
            phase.cs.entry(kind.0).or_default().push(t.elapsed());
            if steps {
                sf_steps(factory, query, changeset_block, &mut phase.steps);
            }
        }
    }
    phase
}

fn row(out: &mut String, name: &str, values: &mut [Duration]) {
    if values.is_empty() {
        return
    }
    values.sort_unstable();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    let at = |q: f64| us(values[((values.len() - 1) as f64 * q) as usize]);
    let mean = values.iter().map(|d| us(*d)).sum::<f64>() / values.len() as f64;
    writeln!(
        out,
        "    {name:<22} {:>6} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2}",
        values.len(),
        at(0.5),
        at(0.9),
        at(0.99),
        at(0.999),
        us(*values.last().unwrap()),
        mean
    )
    .unwrap();
}

fn report(layout: &str, name: &str, phase: &mut Phase) -> String {
    let mut out = String::new();
    writeln!(out, "  {layout} {name}            n       p50       p90       p99     p99.9       max      mean  (us)")
        .unwrap();
    row(&mut out, "provider open", &mut phase.open);
    for (key, values) in &mut phase.full {
        row(&mut out, &format!("full {key}"), values);
    }
    for (key, values) in &mut phase.cs {
        row(&mut out, &format!("cs {key}"), values);
    }
    let steps = &mut phase.steps;
    if !steps.jar.is_empty() {
        let lookups = steps.jar.len() as f64;
        row(&mut out, "sf step jar", &mut steps.jar);
        row(&mut out, "sf step csoff", &mut steps.csoff);
        row(&mut out, "sf step cursor", &mut steps.cursor);
        row(&mut out, "sf step search", &mut steps.search);
        writeln!(out, "    probes per lookup {:.1}", steps.probes as f64 / lookups).unwrap();
    }
    out
}

/// The reads while a writer thread saves the blocks after `BLOCKS + skip` in batches. With
/// `holder`, another thread keeps taking the headers jar and holding it for [`HOLD`], as a range
/// walk over a static file does. Returns the phase and the blocks the writer saved.
fn busy_phase(
    factory: &Factory,
    blocks: &Arc<Vec<ExecutedBlock>>,
    skip: usize,
    queries: &[Query],
    steps: bool,
    holder: bool,
) -> (Phase, usize) {
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (factory, blocks, stop) = (factory.clone(), Arc::clone(blocks), Arc::clone(&stop));
        std::thread::spawn(move || {
            let mut saved = 0;
            for batch in blocks[BLOCKS as usize + 1 + skip..].chunks(BATCH as usize) {
                if stop.load(Ordering::Relaxed) {
                    break
                }
                save(&factory, batch);
                saved += batch.len();
            }
            stop.store(true, Ordering::Relaxed);
            saved
        })
    };
    let holder = holder.then(|| {
        let (factory, stop) = (factory.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            let sf = factory.static_file_provider();
            while !stop.load(Ordering::Relaxed) {
                let jar =
                    sf.get_segment_provider_for_block(StaticFileSegment::Headers, 1, None).unwrap();
                std::thread::sleep(HOLD);
                drop(jar);
                std::thread::sleep(HOLD / 2);
            }
        })
    });
    let phase = run_phase(factory, queries, false, steps, Some(&stop));
    stop.store(true, Ordering::Relaxed);
    let saved = writer.join().unwrap();
    if let Some(holder) = holder {
        holder.join().unwrap();
    }
    (phase, saved)
}

/// Heavy timing harness for v2's historical reads; prints a table per layout and phase.
#[test]
#[ignore = "heavy: run with --release"]
fn v2_history_read_timing_heavy() {
    let started = Instant::now();
    let blocks = Arc::new(generate());
    let queries = queries();
    eprintln!("generated {} blocks in {:?}", blocks.len(), started.elapsed());

    let mut answers = Vec::new();
    let mut out = String::new();
    for (layout, settings) in [("v1", StorageSettings::v1()), ("v2", StorageSettings::v2())] {
        let started = Instant::now();
        let factory = build(settings, &blocks);
        eprintln!("{layout}: saved {BLOCKS} blocks in {:?}", started.elapsed());
        let steps = layout == "v2";

        let mut cold = run_phase(&factory, &queries, false, steps, None);
        let mut warm = run_phase(&factory, &queries, false, steps, None);
        let mut remap = run_phase(&factory, &queries, true, steps, None);
        assert_eq!(cold.answers, warm.answers, "{layout}: warm answers differ");
        assert_eq!(cold.answers, remap.answers, "{layout}: remap answers differ");

        let (mut busy, saved) = busy_phase(&factory, &blocks, 0, &queries, steps, false);
        let (mut held, held_saved) = busy_phase(&factory, &blocks, saved, &queries, steps, true);
        for phase in [&busy, &held] {
            let read = phase.answers.len();
            assert_eq!(phase.answers[..], cold.answers[..read], "{layout}: busy answers differ");
        }

        out += &report(layout, "cold", &mut cold);
        out += &report(layout, "warm", &mut warm);
        out += &report(layout, "remap", &mut remap);
        let (read, held_read) = (busy.answers.len(), held.answers.len());
        out += &report(layout, &format!("busy ({read} reads, {saved} blocks saved)"), &mut busy);
        out += &report(
            layout,
            &format!("busy+holder ({held_read} reads, {held_saved} blocks saved)"),
            &mut held,
        );
        answers.push(cold.answers);
    }
    assert_eq!(answers[0], answers[1], "v1 and v2 answers differ");
    eprintln!("{out}");
}
