//! TN(TN-0130 to TN-0133): the crash points of the static-file prune, rotation and deletion paths
//! through `StaticFileProvider`, in both jar formats: every state a stopped operation leaves opens
//! as the state before it or after it, and files shorter than their committed configuration, or
//! a jar missing between two others, are refused. TN(TN-0138): a failed sync of a segment commit
//! writes no configuration.

use super::{
    manager::StaticFileWriter,
    tn_checksum_tests::{
        account_changes, address, jar_path, load, provider, provider_rw, write_block_segments,
        write_changesets, CHANGES,
    },
    SyncPhase,
};
use crate::providers::StaticFileProvider;
use reth_db::{models::AccountBeforeTx, test_utils::create_test_static_files_dir};
use reth_ethereum_primitives::EthPrimitives;
use reth_nippy_jar::NippyJarWriter;
use reth_static_file_types::{changeset_offset_record_size, StaticFileSegment};
use reth_storage_api::ChangeSetReader;
use reth_storage_errors::provider::ProviderError;
use std::path::{Path, PathBuf};

const ACCOUNTS: StaticFileSegment = StaticFileSegment::AccountChangeSets;

/// The data, offsets, configuration and `.csoff` files of the jar at `path`.
fn changeset_files(path: &Path) -> [Vec<u8>; 4] {
    ["", "off", "conf", "csoff"].map(|ext| {
        let file = if ext.is_empty() { path.to_path_buf() } else { path.with_extension(ext) };
        std::fs::read(file).unwrap()
    })
}

/// Writes `files` ([`changeset_files`]) as the jar at `path`.
fn write_changeset_files(path: &Path, files: &[Vec<u8>; 4]) {
    for (ext, bytes) in ["", "off", "conf", "csoff"].iter().zip(files) {
        let file: PathBuf =
            if ext.is_empty() { path.to_path_buf() } else { path.with_extension(ext) };
        std::fs::write(file, bytes).unwrap();
    }
}

/// Appends account changesets for `blocks`; the blocks from `empty_from` on change nothing.
fn write_accounts(
    provider: &StaticFileProvider<EthPrimitives>,
    blocks: std::ops::Range<u64>,
    empty_from: u64,
) {
    let mut writer = provider.latest_writer(ACCOUNTS).unwrap();
    for block in blocks {
        let changes = if block < empty_from { account_changes(block) } else { Vec::new() };
        writer.append_account_changeset(changes, block).unwrap();
    }
    writer.commit().unwrap();
}

/// Asserts that the account changesets of `0..=tip` read back and that `tip` is the highest block.
fn assert_accounts(provider: &StaticFileProvider<EthPrimitives>, tip: u64, empty_from: u64) {
    assert_eq!(provider.get_highest_static_file_block(ACCOUNTS), Some(tip));
    for block in 0..=tip {
        let expected = if block < empty_from { account_changes(block) } else { Vec::new() };
        assert_eq!(provider.account_block_changeset(block).unwrap(), expected, "block {block}");
    }
}

/// A changeset prune commits its header before it cuts the rows or the sidecar. A prune whose
/// configuration cannot be written (a directory where its temporary file goes) has cut nothing:
/// the next open reads every block. Stopped after the configuration, with the rows, the offsets
/// or the sidecar not cut yet, the next open heals to the finished prune, byte for byte. Both
/// formats, a prune of rows and one of blocks without changes (only the sidecar shrinks). Fails
/// if the rows or the sidecar are cut before the header is committed.
#[test]
fn changeset_prune_commits_the_header_before_it_cuts() {
    let tip = 5;
    for checksums in [false, true] {
        // (prune to, the first block without changes)
        for (last_block, empty_from) in [(1, tip + 1), (3, 4)] {
            let case = format!("checksums {checksums}, prune to {last_block}");
            let (dir, _) = create_test_static_files_dir();
            let provider = provider(dir.path(), checksums, 1_000);
            write_accounts(&provider, 0..tip + 1, empty_from);
            let path = jar_path(&provider, ACCOUNTS, 0);
            let before = changeset_files(&path);

            // The configuration cannot be written: the prune fails and nothing is cut.
            let mut writer = provider.latest_writer(ACCOUNTS).unwrap();
            let blocker = path.with_extension("tmp");
            std::fs::create_dir(&blocker).unwrap();
            writer.prune_account_changesets(last_block).unwrap();
            assert!(writer.commit().is_err(), "{case}");
            drop(writer);
            drop(provider);
            std::fs::remove_dir(&blocker).unwrap();
            let provider = provider_rw(dir.path());
            drop(provider.latest_writer(ACCOUNTS).unwrap());
            assert_eq!(changeset_files(&path), before, "{case}");
            assert_accounts(&provider, tip, empty_from);

            // The finished prune.
            let mut writer = provider.latest_writer(ACCOUNTS).unwrap();
            writer.prune_account_changesets(last_block).unwrap();
            writer.commit().unwrap();
            drop(writer);
            drop(provider);
            let after = changeset_files(&path);

            // Stopped after the configuration: nothing cut, then the rows cut but not the sidecar.
            let [data, offsets, config, _] = after.clone();
            for stopped in [
                [before[0].clone(), before[1].clone(), config.clone(), before[3].clone()],
                [data.clone(), offsets.clone(), config.clone(), before[3].clone()],
            ] {
                write_changeset_files(&path, &stopped);
                let provider = provider_rw(dir.path());
                drop(provider.latest_writer(ACCOUNTS).unwrap());
                assert_eq!(changeset_files(&path), after, "{case}");
                assert_accounts(&provider, last_block, empty_from);
            }
        }
    }
}

/// The state a prune that cut the rows before its header left (an older build's order): the rows
/// of blocks 2 and 3 gone, their records and the header still there. A plain jar heals it as
/// before (`sidecar_heal_shrinks_a_trailing_run_and_refuses_a_gap`); a checksummed jar refuses it
/// as damage and cuts nothing, as it refuses a sidecar shorter than its header.
#[test]
fn checksummed_changesets_shorter_than_the_header_are_refused() {
    let (dir, _) = create_test_static_files_dir();
    let provider = provider(dir.path(), true, 1_000);
    write_changesets(&provider, 0..4);
    let path = jar_path(&provider, ACCOUNTS, 0);
    drop(provider);
    let clean = changeset_files(&path);
    assert!(load(&path).has_checksums());

    let refused = |files: &[Vec<u8>; 4]| {
        let provider = provider_rw(dir.path());
        let result = provider.latest_writer(ACCOUNTS).map(drop);
        assert!(matches!(result, Err(ProviderError::CorruptedChangeSetStaticFile)), "{result:?}");
        assert_eq!(&changeset_files(&path), files);
    };

    // The rows of blocks 2 and 3 cut, the header and the records not.
    let mut writer = NippyJarWriter::new(load(&path)).unwrap();
    writer.prune_rows(2 * CHANGES as usize).unwrap();
    drop(writer);
    refused(&changeset_files(&path));

    // The sidecar one record short of the header.
    write_changeset_files(&path, &clean);
    let record = changeset_offset_record_size(true) as usize;
    let mut short = clean;
    short[3].truncate(short[3].len() - record);
    write_changeset_files(&path, &short);
    refused(&short);

    // The sidecar gone: the jar does not load (nor is the sidecar created again).
    std::fs::remove_file(path.with_extension("csoff")).unwrap();
    let provider = provider_rw(dir.path());
    assert!(provider.latest_writer(ACCOUNTS).is_err());
    assert!(!path.with_extension("csoff").exists());
}

/// TN(TN-0131): a power loss right after a rotation wrote the next jar's configuration (rows 0)
/// can leave that jar's offsets file empty. The next persist reopens the jar and appends to it,
/// in both formats, for a block segment and a changeset segment; it used to panic on the empty
/// offsets map at every start.
#[test]
fn rotation_after_an_empty_offsets_file() {
    use alloy_consensus::Header;
    use alloy_primitives::B256;
    use reth_storage_api::HeaderProvider;

    let header = |number: u64| Header { number, ..Default::default() };
    let hash = |number: u64| B256::with_last_byte(number as u8 + 1);
    for checksums in [false, true] {
        let (dir, _) = create_test_static_files_dir();
        let provider = provider(dir.path(), checksums, 2);
        let mut headers = provider.latest_writer(StaticFileSegment::Headers).unwrap();
        for number in 0..2 {
            headers.append_header(&header(number), &hash(number)).unwrap();
        }
        headers.commit().unwrap();
        // The rotation opens the next jar, whose configuration is written at once.
        headers.append_header(&header(2), &hash(2)).unwrap();
        drop(headers);
        write_accounts(&provider, 0..2, 2);
        let mut accounts = provider.latest_writer(ACCOUNTS).unwrap();
        accounts.append_account_changeset(account_changes(2), 2).unwrap();
        drop(accounts);
        let next = [StaticFileSegment::Headers, ACCOUNTS]
            .map(|segment| provider.directory().join(segment.filename(&(2..=3).into())));
        drop(provider);
        for path in &next {
            assert_eq!(load(path).rows(), 0);
            std::fs::write(path.with_extension("off"), []).unwrap();
        }

        let provider = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
        let mut headers = provider.latest_writer(StaticFileSegment::Headers).unwrap();
        for number in 2..4 {
            headers.append_header(&header(number), &hash(number)).unwrap();
        }
        headers.commit().unwrap();
        drop(headers);
        let mut accounts = provider.latest_writer(ACCOUNTS).unwrap();
        for block in 2..4 {
            accounts.append_account_changeset(account_changes(block), block).unwrap();
        }
        accounts.commit().unwrap();
        drop(accounts);
        for number in 0..4 {
            assert_eq!(provider.header_by_number(number).unwrap(), Some(header(number)));
        }
        assert_accounts(&provider, 3, 4);
        for path in &next {
            assert_eq!(load(path).rows() as u64, if path == &next[0] { 2 } else { 2 * CHANGES });
        }
    }
}

/// Changes unlike [`account_changes`] (fewer, other accounts), for blocks written again.
fn other_changes(block: u64) -> Vec<AccountBeforeTx> {
    (0..2)
        .map(|change| AccountBeforeTx { address: address(block + 500, change), info: None })
        .collect()
}

/// Writes account changesets 4 and 5 again, as [`other_changes`], and reads every block.
fn rewrite_and_read(provider: &StaticFileProvider<EthPrimitives>) {
    let mut writer = provider.latest_writer(ACCOUNTS).unwrap();
    for block in 4..6 {
        writer.append_account_changeset(other_changes(block), block).unwrap();
    }
    writer.commit().unwrap();
    drop(writer);
    assert_eq!(provider.get_highest_static_file_block(ACCOUNTS), Some(5));
    for block in 0..6 {
        let expected = if block < 4 { account_changes(block) } else { other_changes(block) };
        assert_eq!(provider.account_block_changeset(block).unwrap(), expected, "block {block}");
    }
}

/// TN(TN-0132): a jar deletion stopped after any of its files (data, offsets, configuration,
/// sidecar: `NippyJar::delete`'s order) leaves files no listing sees. The next open serves the
/// jars below, and the jar written again at that range starts empty: it reads only its new rows.
/// Both formats.
#[test]
fn deletion_stopped_at_any_file() {
    for checksums in [false, true] {
        for kept in [&["off", "conf", "csoff"][..], &["conf", "csoff"], &["csoff"], &[]] {
            let (dir, _) = create_test_static_files_dir();
            let provider = provider(dir.path(), checksums, 2);
            write_accounts(&provider, 0..6, 6);
            let jar = jar_path(&provider, ACCOUNTS, 4);
            drop(provider);
            std::fs::remove_file(&jar).unwrap();
            for ext in ["off", "conf", "csoff"] {
                if !kept.contains(&ext) {
                    std::fs::remove_file(jar.with_extension(ext)).unwrap();
                }
            }

            let provider = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
            assert_accounts(&provider, 3, 6);
            rewrite_and_read(&provider);
            drop(provider);
            let provider = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
            drop(provider.latest_writer(ACCOUNTS).unwrap());
            for block in 0..6 {
                let expected =
                    if block < 4 { account_changes(block) } else { other_changes(block) };
                assert_eq!(provider.account_block_changeset(block).unwrap(), expected);
            }
        }
    }
}

/// TN(TN-0132): a changeset prune whose deletion of a jar fails at its first file (the data file
/// replaced by a directory) leaves that jar whole: the next open reads every block. Fails if the
/// sidecar goes before the data file, which left a jar that does not load.
#[test]
fn deletion_removes_the_data_file_first() {
    for checksums in [false, true] {
        let (dir, _) = create_test_static_files_dir();
        let provider = provider(dir.path(), checksums, 2);
        write_accounts(&provider, 0..6, 6);
        let jar = jar_path(&provider, ACCOUNTS, 4);
        let moved = jar.with_extension("moved");

        let mut writer = provider.latest_writer(ACCOUNTS).unwrap();
        std::fs::rename(&jar, &moved).unwrap();
        std::fs::create_dir(&jar).unwrap();
        std::fs::write(jar.join("block"), b"x").unwrap();
        writer.prune_account_changesets(1).unwrap();
        assert!(writer.commit().is_err());
        drop(writer);
        drop(provider);
        std::fs::remove_dir_all(&jar).unwrap();
        std::fs::rename(&moved, &jar).unwrap();

        let provider = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
        drop(provider.latest_writer(ACCOUNTS).unwrap());
        assert_accounts(&provider, 5, 6);
    }
}

/// TN(TN-0133): a jar missing between two others of its segment (blocks 2 and 3 of headers,
/// transactions and account changesets) is refused at startup, and every range read across it is
/// an error instead of a shorter answer; reads that do not reach the gap still answer. The lowest
/// jars deleted on purpose (as history expiry does) are not a gap.
#[test]
fn a_jar_missing_between_two_others_is_refused() {
    use reth_nippy_jar::NippyJar;
    use reth_static_file_types::SegmentHeader;
    use reth_storage_api::{HeaderProvider, TransactionsProvider};

    let is_gap = |err: &ProviderError| {
        matches!(
            err.downcast_other_ref::<super::StaticFileGapError>(),
            Some(super::StaticFileGapError::Number { .. })
        )
    };
    for checksums in [false, true] {
        let (dir, _) = create_test_static_files_dir();
        let static_files = provider(dir.path(), checksums, 2);
        write_block_segments(&static_files, 6);
        write_accounts(&static_files, 0..6, 6);
        let segments = [StaticFileSegment::Headers, StaticFileSegment::Transactions, ACCOUNTS];
        let middle = segments.map(|segment| jar_path(&static_files, segment, 2));
        drop(static_files);
        for path in &middle {
            NippyJar::<SegmentHeader>::load(path).unwrap().delete().unwrap();
        }

        let static_files = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
        let err = static_files.check_contiguity().unwrap_err();
        assert!(
            matches!(
                err.downcast_other_ref::<super::StaticFileGapError>(),
                Some(super::StaticFileGapError::Jars { before, after, .. })
                    if (before.start(), before.end(), after.start()) == (0, 1, 4)
            ),
            "{err}"
        );
        assert!(is_gap(&static_files.headers_range(0..6).unwrap_err()));
        assert!(is_gap(&static_files.headers_range(2..3).unwrap_err()));
        assert!(is_gap(&static_files.transactions_by_tx_range(0..6).unwrap_err()));
        assert!(is_gap(&static_files.account_changesets_range(0..6).unwrap_err()));
        assert!(is_gap(&static_files.account_block_changeset(3).unwrap_err()));
        assert_eq!(static_files.headers_range(0..2).unwrap().len(), 2);
        assert_eq!(static_files.headers_range(4..6).unwrap().len(), 2);
        assert_eq!(static_files.transactions_by_tx_range(4..6).unwrap().len(), 2);
        assert_eq!(static_files.account_block_changeset(4).unwrap(), account_changes(4));
        drop(static_files);

        // The lowest jars gone instead: no gap, and the blocks below read as absent.
        let (dir, _) = create_test_static_files_dir();
        let static_files = provider(dir.path(), checksums, 2);
        write_block_segments(&static_files, 6);
        write_accounts(&static_files, 0..6, 6);
        let lowest = segments.map(|segment| jar_path(&static_files, segment, 0));
        drop(static_files);
        for path in &lowest {
            NippyJar::<SegmentHeader>::load(path).unwrap().delete().unwrap();
        }
        let static_files = super::tn_checksum_tests::provider(dir.path(), checksums, 2);
        static_files.check_contiguity().unwrap();
        assert_eq!(static_files.headers_range(0..6).unwrap().len(), 0);
        assert_eq!(static_files.headers_range(2..6).unwrap().len(), 4);
        assert_eq!(static_files.transactions_by_tx_range(2..6).unwrap().len(), 4);
        assert_eq!(static_files.account_block_changeset(1).unwrap(), Vec::new());
        assert_accounts_from(&static_files, 2);
    }
}

/// Asserts that the account changesets from `first` to block 5 read back.
fn assert_accounts_from(provider: &StaticFileProvider<EthPrimitives>, first: u64) {
    for block in first..6 {
        assert_eq!(provider.account_block_changeset(block).unwrap(), account_changes(block));
    }
    let walked = provider.account_changesets_range(first..6).unwrap();
    assert_eq!(walked.len() as u64, (6 - first) * CHANGES);
}

/// The lengths of the data, offsets and `.csoff` files of the jar at `path`.
fn synced_file_lens(path: &Path) -> [u64; 3] {
    [path.to_path_buf(), path.with_extension("off"), path.with_extension("csoff")]
        .map(|file| std::fs::metadata(file).unwrap().len())
}

/// Whether `err` is the fault `fail_syncs` injected into `phase`.
fn is_injected(err: &ProviderError, phase: SyncPhase) -> bool {
    err.to_string().contains(&format!("injected {phase:?} sync fault"))
}

/// TN(TN-0138): a segment commit's `.csoff`, data and offsets syncs run at once, and its
/// configuration is written only when all three succeeded. With any one of them failed,
/// `sync_and_freeze` returns that error once all three ended (each file holds the new blocks),
/// and the configuration on disk is still the last commit's. A reopen heals the files back to it
/// (the new blocks gone, the committed ones read back, the blocks append again); the same writer,
/// its sync retried, commits them all. Both formats. Fails if the configuration is written after
/// a failed sync, or if the call returns before the other syncs ended.
#[test]
fn a_failed_sync_leaves_the_configuration_unwritten() {
    let tip = 3;
    for checksums in [false, true] {
        for phase in [SyncPhase::Csoff, SyncPhase::Data, SyncPhase::Offsets] {
            for retry in [false, true] {
                let case = format!("checksums {checksums}, {phase:?}, retry {retry}");
                let (dir, _) = create_test_static_files_dir();
                let static_files = provider(dir.path(), checksums, 1_000);
                write_accounts(&static_files, 0..tip + 1, tip + 1);
                let path = jar_path(&static_files, ACCOUNTS, 0);
                let config = std::fs::read(path.with_extension("conf")).unwrap();
                let before = synced_file_lens(&path);

                let mut writer = static_files.latest_writer(ACCOUNTS).unwrap();
                for block in tip + 1..tip + 3 {
                    writer.append_account_changeset(account_changes(block), block).unwrap();
                }
                writer.fail_syncs = vec![phase];
                let err = writer.sync_and_freeze().unwrap_err();
                assert!(is_injected(&err, phase), "{case}: {err}");
                assert_eq!(std::fs::read(path.with_extension("conf")).unwrap(), config, "{case}");
                let after = synced_file_lens(&path);
                for (file, (before, after)) in
                    ["data", "off", "csoff"].iter().zip(before.iter().zip(after))
                {
                    assert!(after > *before, "{case}: {file} not written");
                }

                if retry {
                    writer.fail_syncs.clear();
                    writer.sync_and_freeze().unwrap();
                    writer.finalize().unwrap();
                    drop(writer);
                    assert_accounts(&static_files, tip + 2, tip + 3);
                    drop(static_files);
                    let static_files = provider(dir.path(), checksums, 1_000);
                    drop(static_files.latest_writer(ACCOUNTS).unwrap());
                    assert_accounts(&static_files, tip + 2, tip + 3);
                } else {
                    drop(writer);
                    drop(static_files);
                    let static_files = provider(dir.path(), checksums, 1_000);
                    drop(static_files.latest_writer(ACCOUNTS).unwrap());
                    assert_eq!(std::fs::read(path.with_extension("conf")).unwrap(), config);
                    assert_eq!(synced_file_lens(&path), before, "{case}");
                    assert_accounts(&static_files, tip, tip + 1);
                    write_accounts(&static_files, tip + 1..tip + 3, tip + 3);
                    assert_accounts(&static_files, tip + 2, tip + 3);
                }
            }
        }
    }
}

/// TN(TN-0138): with several of a commit's syncs failed, the error is the first in the order the
/// syncs used to run one after another (`.csoff`, data, offsets), the one that order returned,
/// for a changeset segment and for a block segment (no `.csoff`); no configuration is written.
#[test]
fn failed_syncs_return_the_first_in_the_serial_order() {
    use alloy_consensus::Header;
    use alloy_primitives::B256;

    for (failed, expected) in [
        (vec![SyncPhase::Offsets, SyncPhase::Data], SyncPhase::Data),
        (vec![SyncPhase::Offsets, SyncPhase::Csoff], SyncPhase::Csoff),
        (vec![SyncPhase::Offsets, SyncPhase::Data, SyncPhase::Csoff], SyncPhase::Csoff),
    ] {
        let (dir, _) = create_test_static_files_dir();
        let static_files = provider_rw(dir.path());
        write_accounts(&static_files, 0..2, 2);
        let path = jar_path(&static_files, ACCOUNTS, 0);
        let config = std::fs::read(path.with_extension("conf")).unwrap();
        let mut writer = static_files.latest_writer(ACCOUNTS).unwrap();
        writer.append_account_changeset(account_changes(2), 2).unwrap();
        writer.fail_syncs = failed.clone();
        let err = writer.sync_and_freeze().unwrap_err();
        assert!(is_injected(&err, expected), "{failed:?}: {err}");
        assert_eq!(std::fs::read(path.with_extension("conf")).unwrap(), config, "{failed:?}");
    }

    let (dir, _) = create_test_static_files_dir();
    let static_files = provider_rw(dir.path());
    let segment = StaticFileSegment::Headers;
    let mut writer = static_files.latest_writer(segment).unwrap();
    writer.append_header(&Header::default(), &B256::ZERO).unwrap();
    writer.commit().unwrap();
    let path = jar_path(&static_files, segment, 0);
    let config = std::fs::read(path.with_extension("conf")).unwrap();
    writer.append_header(&Header { number: 1, ..Default::default() }, &B256::ZERO).unwrap();
    for (failed, expected) in [
        (vec![SyncPhase::Offsets], SyncPhase::Offsets),
        (vec![SyncPhase::Offsets, SyncPhase::Data], SyncPhase::Data),
    ] {
        writer.fail_syncs = failed.clone();
        let err = writer.sync_and_freeze().unwrap_err();
        assert!(is_injected(&err, expected), "headers {failed:?}: {err}");
        assert_eq!(std::fs::read(path.with_extension("conf")).unwrap(), config, "{failed:?}");
    }
}
