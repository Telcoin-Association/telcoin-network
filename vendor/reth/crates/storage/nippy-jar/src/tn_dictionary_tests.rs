//! TN(TN-0146): dictionary zstd with the dictionaries in the configuration (upstream's format,
//! version 1), which TN never writes but reads.

use super::*;
use rand::{rngs::SmallRng, RngCore, SeedableRng};
use std::num::NonZeroU32;

/// Values that compress (a shared shape, a few varying bytes) and values that do not (random),
/// of 0 to ~2 KiB.
fn values(seed: u64, rows: usize) -> Vec<Vec<u8>> {
    let mut rng = SmallRng::seed_from_u64(seed);
    (0..rows)
        .map(|row| match row % 5 {
            0 => Vec::new(),
            1 => {
                let mut random = vec![0; 1 + row % 97];
                rng.fill_bytes(&mut random);
                random
            }
            _ => {
                let mut value = b"receipt\x02\x01".to_vec();
                for log in 0..1 + row % 7 {
                    value.extend_from_slice(&[0xdd; 20]);
                    value.extend_from_slice(&(row as u64 * 31 + log as u64).to_be_bytes());
                    value.extend_from_slice(&[0u8; 24]);
                    value.extend_from_slice(&(rng.next_u32() % 1000).to_be_bytes());
                }
                value
            }
        })
        .collect()
}

/// A dictionary zstd jar in upstream's format, its dictionaries trained on `rows`.
fn dictionary_jar(path: &Path, rows: &[Vec<u8>]) -> NippyJar {
    let mut jar = NippyJar::new_without_header(2, path).with_zstd(true, 4096);
    jar.prepare_compression(vec![rows.to_vec(), rows.to_vec()]).unwrap();
    jar
}

fn append(writer: &mut NippyJarWriter, rows: &[Vec<u8>]) {
    for row in rows {
        for _ in 0..writer.jar().columns {
            writer.append_column(Some(Ok(row))).unwrap();
        }
    }
}

/// Every row of the jar at `path` reads back as `rows`, through every read path.
fn assert_rows(path: &Path, rows: &[Vec<u8>]) {
    let jar = NippyJar::load_without_header(path).unwrap();
    assert_eq!(jar.rows, rows.len());
    NippyJarChecker::new(NippyJar::load_without_header(path).unwrap()).check_consistency().unwrap();
    let mut cursor = NippyJarCursor::new(&jar).unwrap();
    for (row, expected) in rows.iter().enumerate() {
        let values = cursor.row_by_number(row).unwrap().unwrap();
        assert!(values.iter().all(|value| value == expected), "row {row}");
        let values = cursor.row_by_number_with_cols(row, 1).unwrap().unwrap();
        assert_eq!(values, vec![expected.as_slice()], "row {row}");
    }
    cursor.reset();
    for expected in rows {
        assert!(cursor.next_row().unwrap().unwrap().iter().all(|value| value == expected));
    }
    assert!(cursor.next_row().unwrap().is_none());
}

/// The dictionary id that the stored frame of row `row`, column 0, names, if any.
fn frame_dictionary(path: &Path, row: usize) -> Option<NonZeroU32> {
    let jar = NippyJar::load_without_header(path).unwrap();
    let cursor = NippyJarCursor::new(&jar).unwrap();
    zstd::zstd_safe::get_dict_id_from_frame(cursor.unverified_value(row, 0).unwrap().unwrap())
}

/// Every value of the jar at `path` is a frame of one dictionary. The cursor decompresses with
/// the configuration's dictionaries and zstd refuses a frame that names another, so with
/// [`assert_rows`] this shows the writer compressed with the jar's dictionaries.
fn assert_dictionary_frames(path: &Path, rows: usize) {
    let id = frame_dictionary(path, 0);
    assert!(id.is_some(), "row 0 names no dictionary");
    for row in 0..rows {
        assert_eq!(frame_dictionary(path, row), id, "row {row}");
    }
}

/// The writer compresses a dictionary jar's values with its trained dictionaries (it ignored
/// them, and every frame named no dictionary), and every row reads back.
#[test]
fn test_dictionary_zstd_round_trip() {
    let rows = values(1, 300);
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path();

    let mut writer = NippyJarWriter::new(dictionary_jar(path, &rows)).unwrap();
    append(&mut writer, &rows);
    writer.commit().unwrap();
    drop(writer);
    assert_rows(path, &rows);
    assert_dictionary_frames(path, rows.len());
}

/// A writer reopened on a dictionary jar commits (it panicked serializing the loaded
/// dictionaries) and keeps compressing with them; every row reads back.
#[test]
fn test_dictionary_zstd_reopen() {
    let rows = values(2, 300);
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path();

    let mut writer = NippyJarWriter::new(dictionary_jar(path, &rows)).unwrap();
    append(&mut writer, &rows[..200]);
    writer.commit().unwrap();
    drop(writer);

    let mut writer = NippyJarWriter::new(NippyJar::load_without_header(path).unwrap()).unwrap();
    append(&mut writer, &rows[200..]);
    writer.commit().unwrap();
    drop(writer);
    assert_rows(path, &rows);
    assert_dictionary_frames(path, rows.len());
}

/// A dictionary jar written by upstream's writer, which ignored the dictionaries, still reads
/// back (`test_zstd_013_compatibility` holds the checked-in one), as do zstd jars without
/// dictionaries and jars without compression, through the same cursor.
#[test]
fn test_jars_without_a_dictionary_read_back() {
    let rows = values(3, 100);
    for jar in [
        |path: &Path| NippyJar::new_without_header(1, path).with_zstd(false, 0),
        |path: &Path| NippyJar::new_without_header(1, path).with_zstd(false, 0).with_checksums(),
        |path: &Path| NippyJar::new_without_header(1, path).with_checksums(),
        |path: &Path| NippyJar::new_without_header(1, path),
    ] {
        let file = tempfile::NamedTempFile::new().unwrap();
        let mut writer = NippyJarWriter::new(jar(file.path())).unwrap();
        append(&mut writer, &rows);
        writer.commit().unwrap();
        drop(writer);
        assert_rows(file.path(), &rows);
    }
}

/// A checksummed jar still refuses dictionary zstd: its dictionaries would live in the
/// configuration, which every commit rewrites, and no jar TN writes uses them. `load` refuses
/// such a configuration, and a writer refuses such a jar before it creates any file, so no row
/// is written that `load` would then never read.
#[test]
fn test_checksummed_jar_refuses_dictionary_zstd() {
    let rows = values(4, 50);
    let file = tempfile::NamedTempFile::new().unwrap();
    let mut config = Vec::new();
    dictionary_jar(file.path(), &rows).with_checksums().save_to_writer(&mut config).unwrap();
    assert!(matches!(
        NippyJar::<()>::load_from_reader(&config[..]),
        Err(NippyJarError::UnsupportedCompression)
    ));

    let dir = tempfile::tempdir().unwrap();
    let jar = dictionary_jar(&dir.path().join("jar"), &rows).with_checksums();
    assert!(matches!(NippyJarWriter::new(jar), Err(NippyJarError::UnsupportedCompression)));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0, "no file is created");
}
