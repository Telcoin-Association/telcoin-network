//! TN(TN-0201): the `Transactions` rows `save_blocks_with_tx_positions` writes. A jar with tagged
//! values (version 3) stores a position row per transaction of a block given positions and the
//! transactions inline (tag 0) otherwise; the jar a block lands in decides, also when a call
//! crosses into a new jar; positions that do not fit their block fail the save; a jar without
//! tagged values is written exactly as without positions.

use super::{decode_tx_row, JarFormat, StaticFileWriter, TxPositionsError, TxRow};
use crate::{
    test_utils::{create_test_provider_factory_with_blocks_per_file, MockNodeTypesWithDB},
    ProviderFactory, SaveBlocksMode, StaticFileProviderFactory,
};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{BlockNumber, TxNumber, B256};
use reth_chain_state::{ComputedTrieData, ExecutedBlock};
use reth_db_api::models::StorageSettings;
use reth_ethereum_primitives::{Block, Receipt, TransactionSigned};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_nippy_jar::{NippyJar, NippyJarChecker, NippyJarCursor};
use reth_primitives_traits::{BlockBody as _, SealedBlock, SealedHeader};
use reth_static_file_types::{SegmentHeader, StaticFileSegment};
use reth_storage_api::StorageSettingsCache;
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use reth_testing_utils::generators::{self, random_block, BlockParams};
use std::{collections::BTreeMap, path::Path, sync::Arc};

type Factory = ProviderFactory<MockNodeTypesWithDB>;

/// Blocks per static file: a save of blocks 1 to 9 crosses into two new jars.
const BLOCKS_PER_FILE: u64 = 4;
/// The last generated block.
const TIP: BlockNumber = 10;
/// A block without transactions.
const EMPTY: BlockNumber = 6;

/// Genesis and blocks 1 to [`TIP`], each with 1 to 3 transactions but [`EMPTY`].
fn chain() -> Vec<ExecutedBlock> {
    let mut rng = generators::rng_with_seed(b"tn-0201 transaction rows");
    let genesis = SealedBlock::<Block>::from_sealed_parts(
        SealedHeader::new(alloy_consensus::Header::default(), B256::ZERO),
        Default::default(),
    );
    let mut parent = genesis.hash();
    let mut blocks = vec![executed(genesis)];
    for number in 1..=TIP {
        let tx_count = if number == EMPTY { 0 } else { 1 + (number % 3) as u8 };
        let block = random_block(
            &mut rng,
            number,
            BlockParams {
                parent: Some(parent),
                tx_count: Some(tx_count),
                ommers_count: Some(0),
                ..Default::default()
            },
        );
        parent = block.hash();
        blocks.push(executed(block));
    }
    blocks
}

/// An executed block with one receipt per transaction and no state change.
fn executed(block: SealedBlock<Block>) -> ExecutedBlock {
    let receipts = vec![Receipt::default(); block.body().transaction_count()];
    ExecutedBlock::new(
        Arc::new(block.try_recover().unwrap()),
        Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts,
                requests: Default::default(),
                gas_used: 0,
                blob_gas_used: 0,
            },
            state: Default::default(),
        }),
        ComputedTrieData::default(),
    )
}

/// Positions for `block`'s transactions: strictly increasing, with gaps (dropped transactions),
/// some past one LEB128 byte.
fn positions(block: &ExecutedBlock) -> Arc<[u32]> {
    let number = block.recovered_block().header().number as u32;
    (0..block.recovered_block().body().transaction_count() as u32)
        .map(|i| number * 100 + 3 * i)
        .collect()
}

/// A factory with [`BLOCKS_PER_FILE`]: storage v2 (checksummed jars) if `checksums`, tx pointer
/// jars if `pointers`.
fn factory(checksums: bool, pointers: bool) -> Factory {
    let factory = create_test_provider_factory_with_blocks_per_file(BLOCKS_PER_FILE);
    factory.set_storage_settings_cache(if checksums {
        StorageSettings::v2()
    } else {
        StorageSettings::v1()
    });
    factory.static_file_provider().set_tx_pointer_jars(pointers);
    factory
}

fn save(
    factory: &Factory,
    blocks: &[ExecutedBlock],
    tx_positions: &[Option<Arc<[u32]>>],
) -> ProviderResult<()> {
    let provider_rw = factory.provider_rw()?;
    provider_rw.save_blocks_with_tx_positions(
        blocks.to_vec(),
        SaveBlocksMode::Full,
        tx_positions,
    )?;
    provider_rw.commit()
}

/// Every `Transactions` row on disk by transaction number, with its jar's format, read through
/// the jar's cursor (which verifies each value's checksum). Each jar also passes the heal's
/// consistency check.
fn tx_rows(factory: &Factory) -> BTreeMap<TxNumber, (JarFormat, TxRow<TransactionSigned>)> {
    let static_files = factory.static_file_provider();
    let mut rows = BTreeMap::new();
    for (range, format) in static_files.jar_formats(StaticFileSegment::Transactions).unwrap() {
        let path = static_files.directory().join(StaticFileSegment::Transactions.filename(&range));
        let jar = NippyJar::<SegmentHeader>::load(&path).unwrap();
        assert_eq!(JarFormat::of(&jar), format);
        NippyJarChecker::new(NippyJar::<SegmentHeader>::load(&path).unwrap())
            .check_consistency()
            .unwrap();
        let Some(tx_range) = jar.user_header().tx_range() else { continue };
        let mut cursor = NippyJarCursor::new(&jar).unwrap();
        for row in 0..jar.rows() {
            let value = cursor.row_by_number(row).unwrap().unwrap()[0];
            let decoded = decode_tx_row(value, jar.has_tagged_values()).unwrap();
            rows.insert(tx_range.start() + row as u64, (format, decoded));
        }
    }
    rows
}

/// The jars' formats, by the first block of each.
fn jar_formats(factory: &Factory) -> Vec<(BlockNumber, JarFormat)> {
    let formats = factory.static_file_provider().jar_formats(StaticFileSegment::Transactions);
    formats.unwrap().into_iter().map(|(range, format)| (range.start(), format)).collect()
}

/// What a row should hold.
#[derive(Debug, PartialEq, Eq)]
enum Expected {
    /// The transaction's 2718 encoding, untagged (`false`) or after tag 0 (`true`).
    Inline(Vec<u8>, bool),
    Ref(u32),
}

fn rows_as_expected(factory: &Factory) -> BTreeMap<TxNumber, Expected> {
    tx_rows(factory)
        .into_iter()
        .map(|(tx, (format, row))| {
            let expected = match row {
                TxRow::Inline(tx) => {
                    Expected::Inline(tx.encoded_2718(), format == JarFormat::TaggedTx)
                }
                TxRow::Ref(position) => Expected::Ref(position),
            };
            (tx, expected)
        })
        .collect()
}

/// The rows `blocks` should have, given whether each block's jar has tagged values and its
/// positions.
fn expected_rows(
    blocks: &[ExecutedBlock],
    first_tx: TxNumber,
    tagged: impl Fn(BlockNumber) -> bool,
    tx_positions: &[Option<Arc<[u32]>>],
) -> BTreeMap<TxNumber, Expected> {
    let mut rows = BTreeMap::new();
    let mut tx = first_tx;
    for (i, block) in blocks.iter().enumerate() {
        let b = block.recovered_block();
        let positions = tx_positions.get(i).cloned().flatten();
        for (k, transaction) in b.body().transactions.iter().enumerate() {
            let row = match (&positions, tagged(b.header().number)) {
                (Some(positions), true) => Expected::Ref(positions[k]),
                (_, tagged) => Expected::Inline(transaction.encoded_2718(), tagged),
            };
            rows.insert(tx, row);
            tx += 1;
        }
    }
    rows
}

fn tx_count(blocks: &[ExecutedBlock]) -> u64 {
    blocks.iter().map(|b| b.recovered_block().body().transaction_count() as u64).sum()
}

fn positions_error(err: &ProviderError) -> &TxPositionsError {
    err.downcast_other_ref::<TxPositionsError>().unwrap_or_else(|| panic!("{err}"))
}

/// A pointer datadir from genesis: one save of blocks 1 to 9, which crosses into two new jars,
/// writes position rows for the blocks given positions and inline rows (tag 0, counted) for the
/// one given none, in every jar; every jar has tagged values and every value verifies.
#[test]
fn rows_follow_the_positions_across_new_jars() {
    let blocks = chain();
    let factory = factory(true, true);
    let static_files = factory.static_file_provider();
    save(&factory, &blocks[..1], &[]).unwrap();

    let mut tx_positions: Vec<_> = blocks[1..=9].iter().map(|b| Some(positions(b))).collect();
    tx_positions[2] = None; // block 3
    save(&factory, &blocks[1..=9], &tx_positions).unwrap();
    save(&factory, &blocks[10..], &[Some(positions(&blocks[10]))]).unwrap();

    assert_eq!(
        jar_formats(&factory),
        [(0, JarFormat::TaggedTx), (4, JarFormat::TaggedTx), (8, JarFormat::TaggedTx)]
    );
    let mut expected = expected_rows(&blocks[1..=9], 0, |_| true, &tx_positions);
    let next = tx_count(&blocks[1..=9]);
    expected.extend(expected_rows(&blocks[10..], next, |_| true, &[Some(positions(&blocks[10]))]));
    assert_eq!(rows_as_expected(&factory), expected);
    assert!(expected.values().any(|row| matches!(row, Expected::Ref(p) if *p >= 128)));
    let block3 = blocks[3].recovered_block().body().transaction_count() as u64;
    assert_eq!(static_files.tagged_jar_inline_tx_rows(), block3);

    // `save_blocks` gives no positions: inline rows, counted.
    let factory = self::factory(true, true);
    save(&factory, &blocks[..1], &[]).unwrap();
    let provider_rw = factory.provider_rw().unwrap();
    provider_rw.save_blocks(blocks[1..=5].to_vec(), SaveBlocksMode::Full).unwrap();
    provider_rw.commit().unwrap();
    assert_eq!(rows_as_expected(&factory), expected_rows(&blocks[1..=5], 0, |_| true, &[]));
    let static_files = factory.static_file_provider();
    assert_eq!(static_files.tagged_jar_inline_tx_rows(), tx_count(&blocks[1..=5]));
}

/// The policy turned on mid-way: the jar that exists keeps its format, so the block a call
/// writes there stores its transactions as before, and the jars the call creates have tagged
/// values and store position rows. Turned off again, the existing tagged jar keeps its format.
#[test]
fn a_new_jar_takes_the_policy_mid_call() {
    let blocks = chain();
    let factory = factory(true, false);
    let static_files = factory.static_file_provider();
    save(&factory, &blocks[..=2], &[]).unwrap();
    assert_eq!(jar_formats(&factory), [(0, JarFormat::Checksummed)]);

    static_files.set_tx_pointer_jars(true);
    let tx_positions: Vec<_> = blocks[3..=9].iter().map(|b| Some(positions(b))).collect();
    save(&factory, &blocks[3..=9], &tx_positions).unwrap();
    static_files.set_tx_pointer_jars(false);
    let last = [Some(positions(&blocks[10]))];
    save(&factory, &blocks[10..], &last).unwrap();

    assert_eq!(
        jar_formats(&factory),
        [(0, JarFormat::Checksummed), (4, JarFormat::TaggedTx), (8, JarFormat::TaggedTx)]
    );
    let mut expected = expected_rows(&blocks[1..=2], 0, |_| false, &[]);
    let first = tx_count(&blocks[1..=2]);
    expected.extend(expected_rows(&blocks[3..=9], first, |number| number >= 4, &tx_positions));
    let first = tx_count(&blocks[1..=9]);
    expected.extend(expected_rows(&blocks[10..], first, |_| true, &last));
    assert_eq!(rows_as_expected(&factory), expected);
    assert_eq!(static_files.tagged_jar_inline_tx_rows(), 0);
}

/// Positions for another number of blocks, or a block's positions not one per transaction or
/// not strictly increasing, fail the save in a pointer datadir, and nothing is committed. Only
/// the block count is checked before anything is written; the per-block checks run when that
/// block's rows are written, so a later block's bad positions fail a save whose earlier blocks
/// were fine.
#[test]
fn positions_that_do_not_fit_fail_the_save() {
    let blocks = chain();
    let good: Vec<_> = blocks[1..=3].iter().map(|b| Some(positions(b))).collect();
    let count = |positions: usize| TxPositionsError::BlockCount { blocks: 3, positions };
    let with_block_2 = |block_2: &[u32]| {
        let mut tx_positions = good.clone();
        tx_positions[1] = Some(block_2.into());
        tx_positions
    };
    let block_2 = blocks[2].recovered_block().body().transaction_count();
    assert_eq!(block_2, 3);
    let cases = [
        (good[..2].to_vec(), count(2)),
        ([good.clone(), vec![None]].concat(), count(4)),
        (
            with_block_2(&[1, 2]),
            TxPositionsError::Count { block: 2, transactions: 3, positions: 2 },
        ),
        (
            with_block_2(&[1, 2, 3, 4]),
            TxPositionsError::Count { block: 2, transactions: 3, positions: 4 },
        ),
        (
            with_block_2(&[1, 5, 5]),
            TxPositionsError::NotIncreasing { block: 2, index: 2, previous: 5, position: 5 },
        ),
        (
            with_block_2(&[7, 2, 9]),
            TxPositionsError::NotIncreasing { block: 2, index: 1, previous: 7, position: 2 },
        ),
    ];
    for (tx_positions, expected) in cases {
        let factory = factory(true, true);
        save(&factory, &blocks[..1], &[]).unwrap();
        let err = save(&factory, &blocks[1..=3], &tx_positions).unwrap_err();
        assert_eq!(positions_error(&err), &expected);
        assert!(tx_rows(&factory).is_empty(), "{expected}");
    }

    // The block count is checked in any datadir, also of a save without blocks and by
    // `write_blocks_data` itself; the rest only where the rows are written.
    let factory = factory(true, false);
    save(&factory, &blocks[..1], &[]).unwrap();
    let err = save(&factory, &blocks[1..=3], &good[..1]).unwrap_err();
    assert_eq!(positions_error(&err), &count(1));
    let err = save(&factory, &[], &good[..1]).unwrap_err();
    assert_eq!(positions_error(&err), &TxPositionsError::BlockCount { blocks: 0, positions: 1 });
    let static_files = factory.static_file_provider();
    let runtime = reth_tasks::Runtime::test();
    let err = static_files
        .write_blocks_data(&blocks[1..=3], &[0, 2, 5], &good[..2], Default::default(), &runtime)
        .unwrap_err();
    assert_eq!(positions_error(&err), &count(2));
    save(&factory, &blocks[1..=3], &good).unwrap();
}

/// A jar without tagged values ignores the positions: v1 (upstream's format), v2 (checksummed)
/// and v1 with the tx pointer flag on (which needs checksums) each write every static file byte
/// for byte as the same saves without positions do.
#[test]
fn untagged_jars_ignore_positions() {
    let blocks = chain();
    let with_positions: Vec<_> = blocks.iter().map(|b| Some(positions(b))).collect();
    for (checksums, pointers) in [(false, false), (true, false), (false, true)] {
        let without = factory(checksums, false);
        let with = factory(checksums, pointers);
        for range in [0..1, 1..10, 10..11] {
            save(&without, &blocks[range.clone()], &[]).unwrap();
            save(&with, &blocks[range.clone()], &with_positions[range]).unwrap();
        }
        let format = if checksums { JarFormat::Checksummed } else { JarFormat::Plain };
        assert_eq!(jar_formats(&with), [(0, format), (4, format), (8, format)]);
        let files = static_files(&without);
        let tx_files = files.keys().filter(|name| name.starts_with("static_file_transactions"));
        assert_eq!(tx_files.count(), 3 * 3);
        assert_eq!(files, static_files(&with), "{checksums} {pointers}");
        assert_eq!(with.static_file_provider().tagged_jar_inline_tx_rows(), 0);
    }
}

/// Every file in the factory's static files directory, by name.
fn static_files(factory: &Factory) -> BTreeMap<String, Vec<u8>> {
    let dir = factory.static_file_provider().directory().to_path_buf();
    files_in(&dir)
}

fn files_in(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .map(|path| {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            (name, std::fs::read(&path).unwrap())
        })
        .collect()
}

/// A position row asked of a jar without tagged values is an error and writes nothing.
#[test]
fn position_rows_need_a_tagged_jar() {
    let blocks = chain();
    for (checksums, pointers) in [(false, true), (true, false)] {
        let factory = factory(checksums, pointers);
        save(&factory, &blocks[..=1], &[]).unwrap();
        let static_files = factory.static_file_provider();
        let mut writer = static_files.latest_writer(StaticFileSegment::Transactions).unwrap();
        assert!(!writer.tagged_tx_rows());
        writer.increment_block(2).unwrap();
        let next = tx_count(&blocks[..=1]);
        let err = writer.append_transaction_ref(next, 1).unwrap_err();
        assert!(matches!(positions_error(&err), TxPositionsError::UntaggedJar { .. }), "{err}");
        let tx = &blocks[2].recovered_block().body().transactions[0];
        writer.append_transaction(next, tx).unwrap();
    }

    let factory = factory(true, true);
    save(&factory, &blocks[..=1], &[]).unwrap();
    let static_files = factory.static_file_provider();
    let writer = static_files.latest_writer(StaticFileSegment::Transactions).unwrap();
    assert!(writer.tagged_tx_rows());
    drop(writer);
    let writer = static_files.latest_writer(StaticFileSegment::Receipts).unwrap();
    assert!(!writer.tagged_tx_rows());
}
