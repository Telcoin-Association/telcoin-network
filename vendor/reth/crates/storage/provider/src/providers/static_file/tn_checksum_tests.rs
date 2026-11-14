//! TN(TN-0123): static files with checksums (TN-0120, TN-0121) through `StaticFileProvider`: the
//! policy that picks a new jar's format, damage reaching every reader as an error, the sidecar
//! heal, rotation and prune, and datadirs that hold both formats.

use super::manager::{StaticFileProviderBuilder, StaticFileWriter};
use crate::{
    providers::StaticFileProvider, test_utils::create_test_provider_factory, HeaderProvider,
    ReceiptProvider, StaticFileProviderFactory, TransactionsProvider, TransactionsProviderExt,
};
use alloy_consensus::{Header, SignableTransaction, Transaction, TxLegacy};
use alloy_primitives::{Address, Signature, B256, U160, U256};
use reth_db::{
    models::{AccountBeforeTx, StorageBeforeTx},
    test_utils::create_test_static_files_dir,
};
use reth_db_api::models::StorageSettings;
use reth_ethereum_primitives::{EthPrimitives, Receipt, TransactionSigned};
use reth_nippy_jar::{ColumnResult, NippyJar, NippyJarCursor, NippyJarError, NippyJarWriter};
use reth_primitives_traits::Account;
use reth_static_file_types::{
    changeset_offset_record_size, ChangesetOffsetChecksumMismatch, SegmentHeader, StaticFileSegment,
};
use reth_storage_api::{ChangeSetReader, StorageChangeSetReader, StorageSettingsCache};
use reth_storage_errors::provider::ProviderError;
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

/// Changes a block holds in each changeset segment.
pub(super) const CHANGES: u64 = 5;

pub(super) fn provider(
    dir: &Path,
    checksums: bool,
    blocks_per_file: u64,
) -> StaticFileProvider<EthPrimitives> {
    StaticFileProviderBuilder::read_write(dir)
        .with_blocks_per_file(blocks_per_file)
        .with_jar_checksums(checksums)
        .build()
        .unwrap()
}

pub(super) fn address(block: u64, change: u64) -> Address {
    Address::from(U160::from(block * 1_000 + change + 1))
}

fn slot(change: u64) -> B256 {
    B256::with_last_byte(change as u8 + 1)
}

pub(super) fn account_changes(block: u64) -> Vec<AccountBeforeTx> {
    (0..CHANGES)
        .map(|change| AccountBeforeTx {
            address: address(block, change),
            info: Some(Account { nonce: block, balance: U256::from(change), bytecode_hash: None }),
        })
        .collect()
}

fn storage_changes(block: u64) -> Vec<StorageBeforeTx> {
    (0..CHANGES)
        .map(|change| StorageBeforeTx {
            address: address(block, 0),
            key: slot(change),
            value: U256::from(block * 100 + change + 1),
        })
        .collect()
}

/// Appends both changeset segments' blocks `blocks`.
pub(super) fn write_changesets(
    provider: &StaticFileProvider<EthPrimitives>,
    blocks: std::ops::Range<u64>,
) {
    let mut accounts = provider.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
    for block in blocks.clone() {
        accounts.append_account_changeset(account_changes(block), block).unwrap();
    }
    accounts.commit().unwrap();
    drop(accounts);
    let mut storage = provider.latest_writer(StaticFileSegment::StorageChangeSets).unwrap();
    for block in blocks {
        storage.append_storage_changeset(storage_changes(block), block).unwrap();
    }
    storage.commit().unwrap();
}

/// Appends headers, one transaction, receipt and sender a block, for blocks `0..blocks`.
pub(super) fn write_block_segments(provider: &StaticFileProvider<EthPrimitives>, blocks: u64) {
    let mut headers = provider.latest_writer(StaticFileSegment::Headers).unwrap();
    for number in 0..blocks {
        let header = Header { number, gas_limit: 30_000_000 + number, ..Default::default() };
        headers.append_header(&header, &B256::with_last_byte(number as u8 + 1)).unwrap();
    }
    headers.commit().unwrap();
    drop(headers);
    for segment in [
        StaticFileSegment::Transactions,
        StaticFileSegment::Receipts,
        StaticFileSegment::TransactionSenders,
    ] {
        let mut writer = provider.latest_writer(segment).unwrap();
        for number in 0..blocks {
            writer.increment_block(number).unwrap();
            match segment {
                StaticFileSegment::Transactions => {
                    let tx = TxLegacy { nonce: number, ..Default::default() };
                    let tx: TransactionSigned = tx.into_signed(Signature::test_signature()).into();
                    writer.append_transaction(number, &tx).unwrap();
                }
                StaticFileSegment::Receipts => {
                    let receipt = Receipt { cumulative_gas_used: number + 1, ..Default::default() };
                    writer.append_receipt(number, &receipt).unwrap();
                }
                _ => writer.append_transaction_sender(number, &address(number, 0)).unwrap(),
            }
        }
        writer.commit().unwrap();
    }
}

/// The data file of `segment`'s jar holding `block`.
pub(super) fn jar_path(
    provider: &StaticFileProvider<EthPrimitives>,
    segment: StaticFileSegment,
    block: u64,
) -> PathBuf {
    provider.directory().join(segment.filename(&provider.find_fixed_range(segment, block)))
}

pub(super) fn load(path: &Path) -> NippyJar<SegmentHeader> {
    NippyJar::<SegmentHeader>::load(path).unwrap()
}

/// The data file position of the value at `row` and `column` of the jar at `path`.
fn value_start(path: &Path, row: usize, column: usize) -> u64 {
    let jar = load(path);
    jar.open_data_reader().unwrap().offset(row * jar.columns() + column).unwrap()
}

/// XORs `mask` into the byte at `pos` of `path`.
fn flip(path: &Path, pos: u64, mask: u8) {
    let mut file = OpenOptions::new().read(true).write(true).open(path).unwrap();
    let mut byte = [0u8];
    file.seek(SeekFrom::Start(pos)).unwrap();
    file.read_exact(&mut byte).unwrap();
    file.seek(SeekFrom::Start(pos)).unwrap();
    file.write_all(&[byte[0] ^ mask]).unwrap();
}

/// Whether `err` is a value's or a changeset offset record's checksum failure.
fn is_checksum_error(err: &ProviderError) -> bool {
    let ProviderError::Other(err) = err else { return false };
    let err = err.as_error();
    matches!(err.downcast_ref::<NippyJarError>(), Some(NippyJarError::ChecksumMismatch { .. })) ||
        err.downcast_ref::<std::io::Error>()
            .and_then(|err| err.get_ref())
            .is_some_and(|inner| inner.is::<ChangesetOffsetChecksumMismatch>())
}

/// The policy: a provider with checksums on creates checksummed jars for every segment, with
/// 20-byte `.csoff` records; one with checksums off creates upstream's format. A
/// `ProviderFactory` turns them on for storage v2 and off for v1.
#[test]
fn policy_picks_the_jar_format() {
    for checksums in [false, true] {
        let (dir, _) = create_test_static_files_dir();
        let provider = provider(dir.path(), checksums, 1_000);
        write_changesets(&provider, 0..3);
        write_block_segments(&provider, 3);
        for segment in StaticFileSegment::iter() {
            assert!(super::checksummed_segment(segment), "{segment}");
            assert_eq!(provider.jar_checksums(segment), checksums, "{segment}");
            let jar = load(&jar_path(&provider, segment, 0));
            assert_eq!(jar.has_checksums(), checksums, "{segment}");
        }
        for segment in [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
        {
            let csoff = jar_path(&provider, segment, 0).with_extension("csoff");
            assert_eq!(
                std::fs::metadata(csoff).unwrap().len(),
                3 * changeset_offset_record_size(checksums)
            );
        }
        // Senders are 20 bytes a value: four more with a checksum.
        let senders = jar_path(&provider, StaticFileSegment::TransactionSenders, 0);
        let per_value = if checksums { 24 } else { 20 };
        assert_eq!(std::fs::metadata(senders).unwrap().len(), 3 * per_value);
    }

    let factory = create_test_provider_factory();
    let static_files = factory.static_file_provider();
    assert!(!static_files.jar_checksums(StaticFileSegment::AccountChangeSets));
    factory.set_storage_settings_cache(StorageSettings::v2());
    assert!(StaticFileSegment::iter().all(|segment| static_files.jar_checksums(segment)));
    factory.provider_rw().unwrap().set_storage_settings_cache(StorageSettings::v1());
    assert!(!StaticFileSegment::iter().any(|segment| static_files.jar_checksums(segment)));
    factory.provider().unwrap().set_storage_settings_cache(StorageSettings::v2());
    assert!(StaticFileSegment::iter().all(|segment| static_files.jar_checksums(segment)));
}

/// A changed byte in a changeset value is an error from the point lookups, the block reads and the
/// range walks; the other blocks still read.
#[test]
fn damaged_changeset_value_is_an_error() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_changesets(&provider, 0..3);

    // Block 1's third change, row 7 of each jar; the byte after the key.
    let accounts = jar_path(&provider, StaticFileSegment::AccountChangeSets, 0);
    let storage = jar_path(&provider, StaticFileSegment::StorageChangeSets, 0);
    flip(&accounts, value_start(&accounts, 7, 0) + 20, 0x01);
    flip(&storage, value_start(&storage, 7, 0) + 52, 0x01);
    let provider =
        StaticFileProviderBuilder::<&Path>::read_only(dir.path()).build::<EthPrimitives>();
    let provider = provider.unwrap();

    let err = provider.get_account_before_block(1, address(1, 2)).unwrap_err();
    assert!(is_checksum_error(&err), "{err}");
    let err = provider.get_storage_before_block(1, address(1, 0), slot(2)).unwrap_err();
    assert!(is_checksum_error(&err), "{err}");
    assert!(is_checksum_error(&provider.account_block_changeset(1).unwrap_err()));
    assert!(is_checksum_error(&provider.storage_changeset(1).unwrap_err()));
    assert!(is_checksum_error(&provider.account_changesets_range(0..3).unwrap_err()));
    assert!(is_checksum_error(&provider.storage_changesets_range(0..3).unwrap_err()));

    for block in [0, 2] {
        assert_eq!(provider.account_block_changeset(block).unwrap(), account_changes(block));
        assert_eq!(provider.storage_changeset(block).unwrap().len(), CHANGES as usize);
        let change = provider.get_account_before_block(block, address(block, 2)).unwrap();
        assert_eq!(change, Some(account_changes(block)[2].clone()));
    }
}

/// A changed byte in a header, transaction, receipt or sender is an error from the reads by
/// number; the other rows still read.
#[test]
fn damaged_block_segment_value_is_an_error() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_block_segments(&provider, 3);
    for segment in [
        StaticFileSegment::Headers,
        StaticFileSegment::Transactions,
        StaticFileSegment::Receipts,
        StaticFileSegment::TransactionSenders,
    ] {
        let path = jar_path(&provider, segment, 0);
        flip(&path, value_start(&path, 1, 0) + 1, 0x01);
    }
    drop(provider);
    let provider = provider_ro(dir.path());

    assert!(is_checksum_error(&provider.header_by_number(1).unwrap_err()));
    assert!(is_checksum_error(&provider.transaction_by_id(1).unwrap_err()));
    assert!(is_checksum_error(&provider.receipt(1).unwrap_err()));
    assert!(is_checksum_error(&provider.transaction_sender(1).unwrap_err()));
    assert!(is_checksum_error(&provider.headers_range(0..3).unwrap_err()));
    assert!(is_checksum_error(&provider.receipts_by_tx_range(0..3).unwrap_err()));
    // TN(TN-0136): the hashes of a range holding the damaged transaction are an error too.
    assert!(is_checksum_error(&provider.transaction_hashes_by_range(0..3).unwrap_err()));
    assert_eq!(provider.transaction_hashes_by_range(2..3).unwrap().len(), 1);
    for number in [0, 2] {
        assert_eq!(provider.header_by_number(number).unwrap().unwrap().number, number);
        assert_eq!(provider.transaction_by_id(number).unwrap().unwrap().nonce(), number);
        assert_eq!(provider.receipt(number).unwrap().unwrap().cumulative_gas_used, number + 1);
        assert_eq!(provider.transaction_sender(number).unwrap(), Some(address(number, 0)));
    }
}

fn provider_ro(dir: &Path) -> StaticFileProvider<EthPrimitives> {
    StaticFileProviderBuilder::read_only(dir).build().unwrap()
}

/// A changed byte in a `.csoff` record is an error, before its `num_changes` sizes anything: a
/// damaged high byte would otherwise ask for an allocation that aborts.
#[test]
fn damaged_changeset_offsets_are_an_error() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_changesets(&provider, 0..3);
    let csoff =
        jar_path(&provider, StaticFileSegment::AccountChangeSets, 0).with_extension("csoff");
    drop(provider);

    // Record 1's offset, then its `num_changes`' high byte.
    for pos in [20, 20 + 15] {
        flip(&csoff, pos, 0x40);
        let provider = provider_ro(dir.path());
        assert!(is_checksum_error(&provider.account_block_changeset(1).unwrap_err()));
        let err = provider.get_account_before_block(1, address(1, 2)).unwrap_err();
        assert!(is_checksum_error(&err), "{err}");
        assert!(is_checksum_error(&provider.account_changesets_range(0..3).unwrap_err()));
        assert_eq!(provider.account_block_changeset(2).unwrap(), account_changes(2));
        drop(provider);
        flip(&csoff, pos, 0x40);
    }
}

/// An unchecked jar's `.csoff` range is validated against the jar's rows: past them it is an
/// error, not a shorter changeset (block reads) nor a buffer sized by a damaged count.
#[test]
fn unchecked_changeset_offsets_past_the_rows_are_an_error() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), false, 1_000);
    write_changesets(&provider, 0..3);
    let csoff =
        jar_path(&provider, StaticFileSegment::AccountChangeSets, 0).with_extension("csoff");
    drop(provider);

    // Block 2's `num_changes`: one past the rows, then a count no allocation can hold.
    for num_changes in [CHANGES + 1, u64::MAX / 2] {
        let mut file = OpenOptions::new().write(true).open(&csoff).unwrap();
        file.seek(SeekFrom::Start(2 * 16 + 8)).unwrap();
        file.write_all(&num_changes.to_le_bytes()).unwrap();
        drop(file);
        let provider = provider_ro(dir.path());
        for result in [
            provider.account_block_changeset(2).map(drop),
            provider.get_account_before_block(2, address(2, 4)).map(drop),
            provider.account_changesets_range(0..3).map(drop),
        ] {
            assert!(
                matches!(result, Err(ProviderError::CorruptedChangeSetStaticFile)),
                "{num_changes}: {result:?}"
            );
        }
        assert_eq!(provider.account_block_changeset(1).unwrap(), account_changes(1));
    }
}

/// The sidecar heal at writer open, in 20-byte records: a partial record and records past the
/// committed count are cut; a committed record that fails its checksum stops the open and nothing
/// is cut.
#[test]
fn checksummed_sidecar_heal() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_changesets(&provider, 0..3);
    let csoff =
        jar_path(&provider, StaticFileSegment::AccountChangeSets, 0).with_extension("csoff");
    drop(provider);

    // Uncommitted records and a partial one, as a crash leaves them.
    let committed = std::fs::read(&csoff).unwrap();
    let mut longer = committed.clone();
    longer.extend_from_slice(&committed[20..40]);
    longer.extend_from_slice(&[9; 7]);
    std::fs::write(&csoff, &longer).unwrap();
    let provider = provider_rw(dir.path());
    drop(provider.latest_writer(StaticFileSegment::AccountChangeSets).unwrap());
    assert_eq!(std::fs::read(&csoff).unwrap(), committed);
    assert_eq!(provider.account_block_changeset(2).unwrap(), account_changes(2));
    drop(provider);

    // A damaged committed record: the open fails and the sidecar is left as it is.
    flip(&csoff, 25, 0x02);
    let damaged = std::fs::read(&csoff).unwrap();
    let provider = provider_rw(dir.path());
    let err = provider.latest_writer(StaticFileSegment::AccountChangeSets).map(drop).unwrap_err();
    assert!(is_checksum_error(&err), "{err}");
    assert_eq!(std::fs::read(&csoff).unwrap(), damaged);
    assert_eq!(
        provider.get_highest_static_file_block(StaticFileSegment::AccountChangeSets),
        Some(2)
    );
}

pub(super) fn provider_rw(dir: &Path) -> StaticFileProvider<EthPrimitives> {
    StaticFileProviderBuilder::read_write(dir).with_blocks_per_file(1_000).build().unwrap()
}

/// An unchecked sidecar whose committed records go past the rows only as a trailing run (an
/// interrupted prune) is cut back and the shrunk range is published at once; one whose bad
/// record is followed by a good one is refused and left as it is.
#[test]
fn sidecar_heal_shrinks_a_trailing_run_and_refuses_a_gap() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), false, 1_000);
    write_changesets(&provider, 0..4);
    let path = jar_path(&provider, StaticFileSegment::AccountChangeSets, 0);
    let csoff = path.with_extension("csoff");
    drop(provider);
    let clean = std::fs::read(&csoff).unwrap();

    // Block 1's record points past the rows while blocks 2 and 3 fit: refused.
    let mut gap = clean.clone();
    gap[16 + 8..16 + 16].copy_from_slice(&(100 * CHANGES).to_le_bytes());
    std::fs::write(&csoff, &gap).unwrap();
    let provider = provider_rw(dir.path());
    let result = provider.latest_writer(StaticFileSegment::AccountChangeSets).map(drop);
    assert!(matches!(result, Err(ProviderError::CorruptedChangeSetStaticFile)), "{result:?}");
    assert_eq!(std::fs::read(&csoff).unwrap(), gap);
    drop(provider);

    // An interrupted prune of blocks 2 and 3: the rows went, the records and header did not.
    std::fs::write(&csoff, &clean).unwrap();
    let mut writer = NippyJarWriter::new(load(&path)).unwrap();
    writer.prune_rows(2 * CHANGES as usize).unwrap();
    drop(writer);
    let provider = provider_rw(dir.path());
    assert_eq!(
        provider.get_highest_static_file_block(StaticFileSegment::AccountChangeSets),
        Some(3)
    );
    drop(provider.latest_writer(StaticFileSegment::AccountChangeSets).unwrap());
    assert_eq!(std::fs::metadata(&csoff).unwrap().len(), 2 * 16);
    // The shrunk range is published by the open that healed it.
    assert_eq!(
        provider.get_highest_static_file_block(StaticFileSegment::AccountChangeSets),
        Some(1)
    );
    assert_eq!(provider.account_block_changeset(1).unwrap(), account_changes(1));
}

/// Rotation, prune and appends keep every checksummed jar verifying.
#[test]
fn rotation_prune_and_appends() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 2);
    write_changesets(&provider, 0..7);
    for block in [0, 2, 4, 6] {
        for segment in [StaticFileSegment::AccountChangeSets, StaticFileSegment::StorageChangeSets]
        {
            assert!(load(&jar_path(&provider, segment, block)).has_checksums());
        }
    }
    let read_all = |provider: &StaticFileProvider<EthPrimitives>, blocks: std::ops::Range<u64>| {
        for block in blocks {
            assert_eq!(provider.account_block_changeset(block).unwrap(), account_changes(block));
            let change = provider.get_storage_before_block(block, address(block, 0), slot(3));
            assert_eq!(change.unwrap().unwrap().value, U256::from(block * 100 + 4));
        }
    };
    read_all(&provider, 0..7);

    // Prune back into the jar 4..=5, then append past the rotation again.
    let mut accounts = provider.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();
    accounts.prune_account_changesets(4).unwrap();
    accounts.commit().unwrap();
    drop(accounts);
    let mut storage = provider.latest_writer(StaticFileSegment::StorageChangeSets).unwrap();
    storage.prune_storage_changesets(4).unwrap();
    storage.commit().unwrap();
    drop(storage);
    read_all(&provider, 0..5);
    write_changesets(&provider, 5..9);
    read_all(&provider, 0..9);
    drop(provider);
    read_all(&provider_ro(dir.path()), 0..9);
}

/// Whether `err` is TN-0135's refusal of a jar without checksums.
fn is_unchecked_jar(err: &ProviderError) -> bool {
    err.downcast_other_ref::<super::UncheckedJarError>().is_some()
}

/// TN(TN-0135): a provider with checksums on (storage v2) refuses a jar of a checksummed segment
/// that has none, at every load that reads or opens it: one written without checksums (a
/// storage-v2 datadir from before them), and a checksummed jar whose configuration's version became
/// 1 (its trailer dropped), which would otherwise read its values with their checksums appended. A
/// provider with checksums off reads the unchecked jars. TN(TN-0151): the changeset counts refuse
/// them too. TN(TN-0165): so does the writer's open of a jar the cache holds.
#[test]
fn storage_v2_refuses_jars_without_checksums() {
    // Written without checksums, opened with them.
    let (dir, _) = create_test_static_files_dir();
    let unchecked = provider(dir.path(), false, 2);
    write_changesets(&unchecked, 0..3);
    write_block_segments(&unchecked, 3);
    drop(unchecked);
    let opened = provider(dir.path(), true, 2);
    assert!(is_unchecked_jar(&opened.account_block_changeset(0).unwrap_err()));
    assert!(is_unchecked_jar(&opened.header_by_number(0).unwrap_err()));
    let err = opened.latest_writer(StaticFileSegment::StorageChangeSets).map(drop).unwrap_err();
    assert!(is_unchecked_jar(&err), "{err}");
    assert!(is_unchecked_jar(&opened.account_changeset_count().unwrap_err()));
    assert!(is_unchecked_jar(&opened.storage_changeset_count().unwrap_err()));
    drop(opened);
    let read_only = provider_ro(dir.path());
    assert_eq!(read_only.account_block_changeset(0).unwrap(), account_changes(0));
    assert_eq!(read_only.account_changeset_count().unwrap() as u64, 3 * CHANGES);
    assert_eq!(read_only.storage_changeset_count().unwrap() as u64, 3 * CHANGES);
    drop(read_only);

    // A checksummed jar whose configuration says version 1.
    let (dir, _) = create_test_static_files_dir();
    let checked = provider(dir.path(), true, 2);
    write_changesets(&checked, 0..3);
    let path = jar_path(&checked, StaticFileSegment::StorageChangeSets, 0);
    drop(checked);
    let config = path.with_extension("conf");
    let mut bytes = std::fs::read(&config).unwrap();
    assert_eq!(bytes[..8], 2u64.to_le_bytes());
    bytes[..8].copy_from_slice(&1u64.to_le_bytes());
    bytes.truncate(bytes.len() - 4);
    std::fs::write(&config, bytes).unwrap();
    assert!(!load(&path).has_checksums());
    let reopened = provider(dir.path(), true, 2);
    let err = reopened.get_storage_before_block(0, address(0, 0), slot(3)).unwrap_err();
    assert!(is_unchecked_jar(&err), "{err}");
    assert!(is_unchecked_jar(&reopened.storage_changeset(1).unwrap_err()));
    assert!(is_unchecked_jar(&reopened.storage_changeset_count().unwrap_err()));
    assert_eq!(reopened.account_block_changeset(1).unwrap(), account_changes(1));
    assert_eq!(reopened.account_changeset_count().unwrap() as u64, 3 * CHANGES);
    drop(reopened);

    // TN-0165: turned into version 1 after the jar was cached, its offsets file one offset short;
    // the writer's open, a cache hit, is refused and leaves both files as they are. It ran the
    // version-1 heal, which cut a row and rewrote the configuration, before a later load refused.
    let (dir, _) = create_test_static_files_dir();
    write_changesets(&provider(dir.path(), true, 1_000), 0..3);
    let checked = provider(dir.path(), true, 1_000);
    let path = jar_path(&checked, StaticFileSegment::AccountChangeSets, 0);
    assert_eq!(checked.account_block_changeset(2).unwrap(), account_changes(2));
    let config = path.with_extension("conf");
    let mut bytes = std::fs::read(&config).unwrap();
    bytes[..8].copy_from_slice(&1u64.to_le_bytes());
    bytes.truncate(bytes.len() - 4);
    std::fs::write(&config, &bytes).unwrap();
    let offsets = path.with_extension("off");
    let mut short = std::fs::read(&offsets).unwrap();
    short.truncate(short.len() - 8);
    std::fs::write(&offsets, &short).unwrap();
    let err = checked.latest_writer(StaticFileSegment::AccountChangeSets).map(drop).unwrap_err();
    assert!(is_unchecked_jar(&err), "{err}");
    assert_eq!(std::fs::read(&config).unwrap(), bytes);
    assert_eq!(std::fs::read(&offsets).unwrap(), short);
}

/// A range read that meets a row missing inside a jar's own range is an error, not a shorter
/// result; past the data it still ends quietly.
#[test]
fn missing_row_inside_a_range_is_an_error() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), false, 2);
    write_block_segments(&provider, 5);
    let path = jar_path(&provider, StaticFileSegment::Headers, 2);
    drop(provider);
    assert_eq!(provider_ro(dir.path()).headers_range(0..9).unwrap().len(), 5);

    // The jar for blocks 2..=3 loses its last row but keeps claiming block 3.
    let mut writer = NippyJarWriter::new(load(&path)).unwrap();
    writer.prune_rows(1).unwrap();
    drop(writer);
    let provider = provider_ro(dir.path());
    let err = provider.headers_range(0..5).unwrap_err();
    assert!(err.to_string().contains("has no row for 3"), "{err}");
    assert_eq!(provider.headers_range(0..3).unwrap().len(), 3);
}

/// TN(TN-0124): the point lookups probe the stored keys unverified. A damaged probe key that
/// steers the search onto an intact row of another key never returns that row: the miss reruns the
/// search verifying every probe and reports the checksum error. A real miss is still `None`.
#[test]
fn damaged_probe_key_is_an_error_not_another_row() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_changesets(&provider, 0..3);
    let accounts = jar_path(&provider, StaticFileSegment::AccountChangeSets, 0);
    let storage = jar_path(&provider, StaticFileSegment::StorageChangeSets, 0);
    drop(provider);

    let provider = provider_ro(dir.path());
    assert_eq!(
        provider.get_account_before_block(1, address(1, 0)).unwrap(),
        Some(account_changes(1)[0].clone())
    );
    assert_eq!(provider.get_account_before_block(1, address(2, 0)).unwrap(), None);
    assert_eq!(provider.get_storage_before_block(1, address(1, 0), slot(9)).unwrap(), None);
    drop(provider);

    // Block 1 is rows 5..10; the first probe is row 7 (change 2). Its key's last byte now sorts it
    // below change 0's, so the search walks right and ends on change 3's intact row.
    flip(&accounts, value_start(&accounts, 7, 0) + 19, 0x03);
    flip(&storage, value_start(&storage, 7, 0) + 51, 0x03);
    let provider = provider_ro(dir.path());
    let err = provider.get_account_before_block(1, address(1, 0)).unwrap_err();
    assert!(is_checksum_error(&err), "{err}");
    let err = provider.get_storage_before_block(1, address(1, 0), slot(0)).unwrap_err();
    assert!(is_checksum_error(&err), "{err}");
    // A lookup whose probes avoid the damaged row still answers.
    assert_eq!(
        provider.get_account_before_block(0, address(0, 0)).unwrap(),
        Some(account_changes(0)[0].clone())
    );
}

/// TN(TN-0137): the changeset point lookups refuse a compressed jar with a typed error instead of
/// comparing its compressed bytes as keys (by those bytes, a stored change reported absent, or
/// here, values shorter than the key, a corruption error). The block reads decompress every row and
/// still answer.
#[test]
fn compressed_changeset_jar_is_refused_by_the_lookups() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), false, 1_000);
    write_changesets(&provider, 0..3);
    let jars = [
        jar_path(&provider, StaticFileSegment::AccountChangeSets, 0),
        jar_path(&provider, StaticFileSegment::StorageChangeSets, 0),
    ];
    drop(provider);

    // Change 3 is not its block's first, so a search over compressed bytes does not land on it.
    let provider = provider_ro(dir.path());
    assert_eq!(
        provider.get_account_before_block(1, address(1, 3)).unwrap(),
        Some(account_changes(1)[3].clone())
    );
    assert!(provider.get_storage_before_block(1, address(1, 0), slot(3)).unwrap().is_some());
    drop(provider);

    // The same rows written again with compression; the `.csoff` sidecars still hold the blocks.
    for path in &jars {
        let (header, values) = {
            let jar = load(path);
            let mut cursor = NippyJarCursor::new(&jar).unwrap();
            let mut values: Vec<ColumnResult<Vec<u8>>> = Vec::new();
            while let Some(row) = cursor.next_row().unwrap() {
                values.push(Ok(row[0].to_vec()));
            }
            (jar.user_header().clone(), values)
        };
        let rows = values.len() as u64;
        let mut writer = NippyJarWriter::new(NippyJar::new(1, path, header).with_lz4()).unwrap();
        writer.append_rows(vec![values], rows).unwrap();
        writer.commit().unwrap();
        assert!(load(path).compressor().is_some());
    }

    let provider = provider_ro(dir.path());
    let is_compressed_jar = |err: &ProviderError| {
        err.downcast_other_ref::<super::CompressedChangesetJarError>().is_some()
    };
    let err = provider.get_account_before_block(1, address(1, 3)).unwrap_err();
    assert!(is_compressed_jar(&err), "{err}");
    let err = provider.get_storage_before_block(1, address(1, 0), slot(3)).unwrap_err();
    assert!(is_compressed_jar(&err), "{err}");
    assert_eq!(provider.account_block_changeset(1).unwrap(), account_changes(1));
}

/// TN(TN-0160): a jar's configuration must name the segment and block range of its file name: the
/// name's own range passes, and so does a first jar whose expected start a non-zero genesis moved
/// past the name's; another range or segment is refused.
#[test]
fn a_configuration_must_name_its_own_jar() {
    use reth_db::static_file::check_static_file_identity;

    let path = Path::new("static_file_account-change-sets_0_499999");
    let header = |segment| SegmentHeader::new((0..=499_999).into(), None, None, segment);
    let mut own = header(StaticFileSegment::AccountChangeSets);
    check_static_file_identity(path, &own).unwrap();
    own.set_expected_block_start(502);
    check_static_file_identity(path, &own).unwrap();
    check_static_file_identity(path, &header(StaticFileSegment::StorageChangeSets)).unwrap_err();
    let next = SegmentHeader::new(
        (500_000..=999_999).into(),
        None,
        None,
        StaticFileSegment::AccountChangeSets,
    );
    check_static_file_identity(path, &next).unwrap_err();
    check_static_file_identity(&path.with_extension("conf"), &own).unwrap_err();
}

/// TN(TN-0160): the checksums bind a value to its row and column, not to its file. A configuration
/// under another jar's name (another range, or the other changeset segment's) is refused at
/// startup and, when it changes while the node runs, at the jar's load. A full jar whose data or
/// offsets file is another jar's, of another length, is refused at its load; reads of the other
/// jars still answer.
#[test]
fn jar_files_under_another_jars_name_are_refused() {
    const ACCOUNTS: StaticFileSegment = StaticFileSegment::AccountChangeSets;
    const STORAGE: StaticFileSegment = StaticFileSegment::StorageChangeSets;
    let is_identity = |err: &str| err.contains("holds the configuration of another jar");
    let setup = || {
        let (dir, _) = create_test_static_files_dir();
        let static_files = provider(dir.path(), true, 2);
        write_block_segments(&static_files, 6);
        write_changesets(&static_files, 0..6);
        let paths = [0, 2, 4].map(|block| jar_path(&static_files, ACCOUNTS, block));
        (dir, paths, jar_path(&static_files, STORAGE, 0))
    };
    let conf = |path: &Path| path.with_extension("conf");

    // The configurations of jars 0..=1 and 2..=3 swapped, or the storage jar's at 0..=1.
    for other in [false, true] {
        let (dir, paths, storage) = setup();
        let donor = if other { conf(&storage) } else { conf(&paths[1]) };
        let original = std::fs::read(conf(&paths[0])).unwrap();
        std::fs::copy(&donor, conf(&paths[0])).unwrap();
        let err = StaticFileProviderBuilder::read_write(dir.path())
            .with_blocks_per_file(2)
            .with_jar_checksums(true)
            .build::<EthPrimitives>()
            .unwrap_err();
        assert!(is_identity(&err.to_string()), "{err}");

        // Swapped after the start, before the jar is loaded.
        std::fs::write(conf(&paths[0]), &original).unwrap();
        let static_files = provider(dir.path(), true, 2);
        std::fs::copy(&donor, conf(&paths[0])).unwrap();
        let err = static_files.account_block_changeset(0).unwrap_err();
        assert!(is_identity(&err.to_string()), "{err}");
        assert_eq!(static_files.account_block_changeset(2).unwrap(), account_changes(2));
    }

    // The full jar 0..=1 with the storage jar's data file, or the headers jar's offsets file.
    for file in ["data", "offsets"] {
        let (dir, paths, storage) = setup();
        let (from, to) = if file == "data" {
            (storage, paths[0].clone())
        } else {
            let static_files = provider_ro(dir.path());
            let headers = jar_path(&static_files, StaticFileSegment::Headers, 0);
            (headers.with_extension("off"), paths[0].with_extension("off"))
        };
        std::fs::copy(from, to).unwrap();
        let static_files = provider(dir.path(), true, 2);
        let err = static_files.account_block_changeset(0).unwrap_err();
        assert!(
            matches!(
                err.downcast_other_ref::<NippyJarError>(),
                Some(NippyJarError::FileLengthMismatch { file: found, .. }) if *found == file
            ),
            "{file}: {err}"
        );
        assert_eq!(static_files.account_block_changeset(2).unwrap(), account_changes(2));
    }
}

/// TN(TN-0163): the sidecar heal refuses committed records that do not form one run from row 0,
/// each block starting where the previous block's rows end, even when every record fits the rows
/// and verifies: one pointing back into the previous blocks' rows, one skipping rows, and a first
/// record not at row 0. Both formats; the jar is left as it is. They would map blocks to other
/// blocks' rows.
#[test]
fn sidecar_heal_refuses_records_out_of_order() {
    use reth_static_file_types::{ChangesetOffset, ChangesetOffsetWriter};

    let runs: [[(u64, u64); 4]; 3] = [
        [(0, 5), (5, 5), (0, 5), (15, 5)],
        [(0, 5), (5, 5), (12, 3), (15, 5)],
        [(1, 4), (5, 5), (10, 5), (15, 5)],
    ];
    for checksums in [false, true] {
        for run in runs {
            let (dir, _) = create_test_static_files_dir();
            let provider = provider(dir.path(), checksums, 1_000);
            write_changesets(&provider, 0..4);
            let path = jar_path(&provider, StaticFileSegment::AccountChangeSets, 0);
            drop(provider);
            let csoff = path.with_extension("csoff");
            std::fs::remove_file(&csoff).unwrap();
            let mut writer = ChangesetOffsetWriter::new(&csoff, 0, checksums).unwrap();
            for (offset, changes) in run {
                writer.append(&ChangesetOffset::new(offset, changes)).unwrap();
            }
            writer.sync().unwrap();
            drop(writer);
            let records = std::fs::read(&csoff).unwrap();

            let provider = provider_rw(dir.path());
            let result = provider.latest_writer(StaticFileSegment::AccountChangeSets).map(drop);
            assert!(
                matches!(result, Err(ProviderError::CorruptedChangeSetStaticFile)),
                "checksums {checksums}, {run:?}: {result:?}"
            );
            assert_eq!(std::fs::read(&csoff).unwrap(), records);
        }
    }
}
