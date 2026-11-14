//! reth's static file database table import and access

use reth_nippy_jar::{NippyJar, NippyJarError};
use reth_static_file_types::{
    SegmentHeader, SegmentRangeInclusive, StaticFileMap, StaticFileSegment,
};
use std::path::{Path, PathBuf};

mod cursor;
pub use cursor::StaticFileCursor;

mod mask;
pub use mask::*;

mod masks;
pub use masks::*;

/// Alias type for a map of [`StaticFileSegment`] and sorted lists of existing static file ranges.
type SortedStaticFiles = StaticFileMap<Vec<(SegmentRangeInclusive, SegmentHeader)>>;

/// Given the `static_files` directory path, it returns a list over the existing `static_files`
/// organized by [`StaticFileSegment`]. Each segment has a sorted list of block ranges and
/// segment headers as presented in the file configuration.
pub fn iter_static_files(path: &Path) -> Result<SortedStaticFiles, NippyJarError> {
    if !path.exists() {
        reth_fs_util::create_dir_all(path).map_err(|err| NippyJarError::Custom(err.to_string()))?;
    }

    let mut static_files = SortedStaticFiles::default();
    let entries = reth_fs_util::read_dir(path)
        .map_err(|err| NippyJarError::Custom(err.to_string()))?
        .filter_map(Result::ok);
    for entry in entries {
        if entry.metadata().is_ok_and(|metadata| metadata.is_file()) &&
            let Some((segment, _)) =
                StaticFileSegment::parse_filename(&entry.file_name().to_string_lossy())
        {
            let jar = NippyJar::<SegmentHeader>::load(&entry.path())?;
            // TN(TN-0160): a configuration of another jar under this name is refused.
            check_static_file_identity(&entry.path(), jar.user_header())
                .map_err(|err| NippyJarError::Internal(Box::new(err)))?;

            if let Some(block_range) = jar.user_header().block_range() {
                static_files
                    .entry(segment)
                    .and_modify(|headers| headers.push((block_range, jar.user_header().clone())))
                    .or_insert_with(|| vec![(block_range, jar.user_header().clone())]);
            }
        }
    }

    // Sort by block end range.
    for range_list in static_files.values_mut() {
        range_list.sort_by_key(|(block_range, _)| block_range.end());
    }

    Ok(static_files)
}

/// TN(TN-0160): checks that the configuration loaded for the jar at `path` is that jar's: the
/// segment its file name names, and an expected block range that ends where the name's range ends
/// and starts no earlier (a non-zero genesis moves a first jar's expected start past its name's,
/// [`SegmentHeader::set_expected_block_start`]). A jar's checksums bind each value to its row and
/// column, not to its file, so a configuration copied or restored under another jar's name would
/// otherwise be believed. Every file a writer creates passes: its name and configuration come
/// from one range.
pub fn check_static_file_identity(
    path: &Path,
    header: &SegmentHeader,
) -> Result<(), StaticFileIdentityError> {
    let named = path
        .file_name()
        .and_then(|name| StaticFileSegment::parse_filename(&name.to_string_lossy()));
    let expected = header.expected_block_range();
    match named {
        Some((segment, range))
            if segment == header.segment() &&
                expected.end() == range.end() &&
                expected.start() >= range.start() =>
        {
            Ok(())
        }
        _ => {
            tracing::error!(
                target: "reth::static_file",
                path = %path.display(),
                segment = %header.segment(),
                %expected,
                "Static file holds the configuration of another jar"
            );
            Err(StaticFileIdentityError {
                path: path.to_path_buf(),
                segment: header.segment(),
                expected,
            })
        }
    }
}

/// TN(TN-0160): the configuration of the jar at `path` names another segment or block range than
/// the file's name ([`check_static_file_identity`]).
#[derive(Debug, thiserror::Error)]
#[error(
    "static file {} holds the configuration of another jar: {segment} blocks {expected}",
    path.display()
)]
pub struct StaticFileIdentityError {
    /// The jar's data file, whose name says which jar it is.
    pub path: PathBuf,
    /// The segment the configuration names.
    pub segment: StaticFileSegment,
    /// The block range the configuration names.
    pub expected: SegmentRangeInclusive,
}
