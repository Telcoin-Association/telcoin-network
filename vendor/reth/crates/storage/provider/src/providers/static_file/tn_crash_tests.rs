//! TN(TN-0130): the crash points of the static-file prune, rotation and deletion paths through
//! `StaticFileProvider`, in both jar formats: every state a stopped operation leaves opens as the
//! state before it or after it, and files shorter than their committed configuration are refused
//! in a checksummed jar.

use super::{
    manager::StaticFileWriter,
    tn_checksum_tests::{
        account_changes, jar_path, load, provider, provider_rw, write_changesets, CHANGES,
    },
};
use crate::providers::StaticFileProvider;
use reth_db::test_utils::create_test_static_files_dir;
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
    let mut short = clean.clone();
    short[3].truncate(short[3].len() - record);
    write_changeset_files(&path, &short);
    refused(&short);

    // The sidecar gone: the jar does not load (nor is the sidecar created again).
    std::fs::remove_file(path.with_extension("csoff")).unwrap();
    let provider = provider_rw(dir.path());
    assert!(provider.latest_writer(ACCOUNTS).is_err());
    assert!(!path.with_extension("csoff").exists());
}
