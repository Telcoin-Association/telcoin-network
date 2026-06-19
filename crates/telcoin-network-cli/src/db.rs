//! DB diagnostics command.
//!
//! `db stats` prints read-only statistics for the execution database. `db validate` walks a
//! consensus epoch pack's `data` stream and reports integrity issues, reproducing the importer's
//! `MissingBatches` check and classifying each missing batch as Absent (a real data gap) vs
//! Misordered (present, but in the wrong consensus-header group).

use clap::{Args, Parser, Subcommand};
use comfy_table::{Cell, Row, Table as ComfyTable};
use eyre::{bail, eyre};
use human_bytes::human_bytes;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tn_config::TelcoinDirs as _;
use tn_reth::{
    iter_static_files, open_db_read_only, traits::TNPrimitives, DatabaseArguments, DatabaseEnv,
    RethDatabaseT as _, RethMdbxError, StaticFileProvider, Tables,
};
use tn_storage::{consensus_pack::DATA_NAME, pack_validate::validate_pack_file};
use tn_types::Epoch;

/// Inspect and diagnose telcoin-network databases.
#[derive(Debug, Parser)]
pub struct DbCommand {
    /// Database diagnostics subcommand.
    #[command(subcommand)]
    command: DbSubcommand,
}

/// Supported database diagnostics subcommands.
#[derive(Debug, Subcommand)]
enum DbSubcommand {
    /// Print execution database statistics.
    Stats,

    /// Validate a consensus epoch pack file: walk the `data` stream and report integrity issues.
    Validate(DbValidateArgs),
}

impl DbCommand {
    /// Execute the database diagnostics command.
    ///
    /// `datadir` is the resolved top-level `--datadir`. Because that flag is global, clap
    /// propagates it to every level, so it can be placed before `db`, between `db` and the
    /// subcommand (reth-style, `telcoin-network db --datadir PATH stats`), or after the
    /// subcommand.
    pub fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        match &self.command {
            DbSubcommand::Stats => {
                let db_path = datadir.reth_db_path();
                let db = open_db_read_only(&db_path, DatabaseArguments::default())?;
                if let Some(static_files_table) = static_files_summary_table_for_datadir(&datadir)?
                {
                    println!("{static_files_table}");
                    println!();
                } else {
                    println!(
                        "(no static files directory found at {})",
                        datadir.join("static_files").display()
                    );
                    println!();
                }
                println!("{}", db_stats_table(&db)?);
            }
            DbSubcommand::Validate(args) => args.execute()?,
        }
        Ok(())
    }
}

/// Validate a consensus epoch pack file.
#[derive(Debug, Args)]
pub struct DbValidateArgs {
    /// Path to a pack `data` stream file, or an `epoch-NN` directory containing one.
    ///
    /// The `data` stream is self-contained for validation — the sidecar `idx`/`hash`/`bhash`
    /// indexes are not required.
    pub path: PathBuf,

    /// Epoch number of the pack.
    ///
    /// Required unless it can be derived from an `epoch-NN` directory in the path. The header
    /// `uid` is derived from the epoch, so an incorrect value fails to open the file.
    #[arg(long)]
    pub epoch: Option<Epoch>,
}

impl DbValidateArgs {
    /// Validate the pack and print the report to stdout.
    fn execute(&self) -> eyre::Result<()> {
        let (data_file, epoch) = resolve_data_file_and_epoch(&self.path, self.epoch)?;

        let report = validate_pack_file(&data_file, epoch, None)
            .map_err(|e| eyre!("failed to validate pack {}: {e}", data_file.display()))?;

        // Report goes to stdout (tracing/logs go to stderr/file).
        print!("{report}");
        Ok(())
    }
}

/// Resolve the user-supplied `path` to a concrete `data` file and an epoch.
///
/// Accepts:
/// - a bare `data` file (epoch from `--epoch`, else an `epoch-NN` parent directory),
/// - an `epoch-NN` directory (epoch from the directory name, overridable with `--epoch`),
/// - any directory containing a `data` file (epoch from `--epoch`).
fn resolve_data_file_and_epoch(
    path: &Path,
    epoch_opt: Option<Epoch>,
) -> eyre::Result<(PathBuf, Epoch)> {
    let (data_file, dir_for_epoch) = if path.is_dir() {
        let candidate = path.join(DATA_NAME);
        if !candidate.is_file() {
            bail!("directory {} does not contain a `{DATA_NAME}` pack file", path.display());
        }
        (candidate, Some(path.to_path_buf()))
    } else if path.is_file() {
        (path.to_path_buf(), path.parent().map(Path::to_path_buf))
    } else {
        bail!("path does not exist: {}", path.display());
    };

    let epoch = match epoch_opt {
        Some(epoch) => epoch,
        None => dir_for_epoch.as_deref().and_then(epoch_from_dir_name).ok_or_else(|| {
            eyre!(
                "could not determine epoch from {}; pass --epoch <N> \
                 (or point at an `epoch-NN` directory)",
                path.display()
            )
        })?,
    };

    Ok((data_file, epoch))
}

/// Parse an epoch out of an `epoch-NN` directory name.
fn epoch_from_dir_name(dir: &Path) -> Option<Epoch> {
    dir.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("epoch-"))
        .and_then(|num| num.parse::<Epoch>().ok())
}

#[derive(Debug, Clone)]
struct StaticFileSegmentStats {
    segment: String,
    block_range: String,
    tx_range: String,
    total_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TableStats {
    name: &'static str,
    entries: u64,
    branch_pages: u64,
    leaf_pages: u64,
    overflow_pages: u64,
    total_size: u64,
}

fn stats_table(stats: &[TableStats]) -> ComfyTable {
    let mut table = ComfyTable::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_header([
        "Table Name",
        "# Entries",
        "Branch Pages",
        "Leaf Pages",
        "Overflow Pages",
        "Total Size",
    ]);

    let mut total_size = 0_u64;
    for stat in stats {
        total_size = total_size.saturating_add(stat.total_size);

        let mut row = Row::new();
        row.add_cell(Cell::new(stat.name))
            .add_cell(Cell::new(stat.entries))
            .add_cell(Cell::new(stat.branch_pages))
            .add_cell(Cell::new(stat.leaf_pages))
            .add_cell(Cell::new(stat.overflow_pages))
            .add_cell(Cell::new(human_bytes(stat.total_size as f64)));
        table.add_row(row);
    }

    let max_widths = table.column_max_content_widths();
    let mut separator = Row::new();
    for width in max_widths {
        separator.add_cell(Cell::new("-".repeat(width as usize)));
    }
    table.add_row(separator);

    let mut row = Row::new();
    row.add_cell(Cell::new("Tables"))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(human_bytes(total_size as f64)));
    table.add_row(row);

    table
}

fn file_len_if_exists(path: &Path) -> eyre::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err.into()),
    }
}

fn static_files_summary_table_for_datadir(datadir: &Path) -> eyre::Result<Option<ComfyTable>> {
    let static_files_dir = datadir.join("static_files");
    if !static_files_dir.exists() {
        return Ok(None);
    }
    let static_file_provider =
        StaticFileProvider::<TNPrimitives>::read_only(&static_files_dir, false)?;

    let mut stats = Vec::new();
    for (segment, ranges) in iter_static_files(&static_files_dir)?.into_iter() {
        let mut segment_size = 0_u64;

        for (block_range, _header) in &ranges {
            let fixed_block_range =
                static_file_provider.find_fixed_range(segment, block_range.start());
            let jar_provider = static_file_provider
                .get_segment_provider_for_range(segment, || Some(fixed_block_range), None)?
                .ok_or_else(|| eyre::eyre!("Failed to get static file provider for {segment:?}"))?;

            segment_size = segment_size.saturating_add(
                file_len_if_exists(jar_provider.data_path())?
                    + file_len_if_exists(&jar_provider.index_path())?
                    + file_len_if_exists(&jar_provider.offsets_path())?
                    + file_len_if_exists(&jar_provider.config_path())?,
            );

            drop(jar_provider);
            static_file_provider.remove_cached_provider(segment, fixed_block_range.end());
        }

        stats.push(StaticFileSegmentStats {
            segment: segment.to_string(),
            // `ranges` is sorted ascending by block end, so span the first jar's start to the last
            // jar's end to report the full block range a segment covers (not just its first jar).
            block_range: match (ranges.first(), ranges.last()) {
                (Some((first, _)), Some((last, _))) => {
                    format!("{}..={}", first.start(), last.end())
                }
                _ => String::new(),
            },
            // Tx ranges can be empty (e.g. the Headers segment), so span the first non-empty
            // jar's start to the last non-empty jar's end — mirroring `block_range` above, not
            // just the first jar's range.
            tx_range: {
                let start =
                    ranges.iter().find_map(|(_, header)| header.tx_range().map(|r| r.start()));
                let end =
                    ranges.iter().rev().find_map(|(_, header)| header.tx_range().map(|r| r.end()));
                match (start, end) {
                    (Some(start), Some(end)) => format!("{start}..={end}"),
                    _ => "N/A".to_string(),
                }
            },
            total_size: segment_size,
        });
    }

    Ok(Some(static_files_summary_table(&stats)))
}

fn static_files_summary_table(stats: &[StaticFileSegmentStats]) -> ComfyTable {
    let mut table = ComfyTable::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_header(["Segment", "Block Range", "Transaction Range", "Size"]);

    let mut total_size = 0_u64;
    for stat in stats {
        total_size = total_size.saturating_add(stat.total_size);
        let mut row = Row::new();
        row.add_cell(Cell::new(&stat.segment))
            .add_cell(Cell::new(&stat.block_range))
            .add_cell(Cell::new(&stat.tx_range))
            .add_cell(Cell::new(human_bytes(stat.total_size as f64)));
        table.add_row(row);
    }

    let max_widths = table.column_max_content_widths();
    let mut separator = Row::new();
    for width in max_widths {
        separator.add_cell(Cell::new("-".repeat(width as usize)));
    }
    table.add_row(separator);

    let mut total_row = Row::new();
    total_row
        .add_cell(Cell::new("Total"))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(""))
        .add_cell(Cell::new(human_bytes(total_size as f64)));
    table.add_row(total_row);

    table
}

fn db_stats_table(db: &DatabaseEnv) -> eyre::Result<ComfyTable> {
    let mut stats = Vec::new();

    db.view(|tx| {
        let mut db_tables = Tables::ALL.iter().map(|table| table.name()).collect::<Vec<_>>();
        db_tables.sort();

        for db_table in db_tables {
            // A read-only transaction cannot create a sub-database, so a table that was never
            // written reports `NotFound`. Skip it rather than aborting the whole report — this
            // keeps `db stats` working against an older on-disk DB whose schema
            // predates a table later added to `Tables::ALL` (schema skew).
            let table_db = match tx.inner().open_db(Some(db_table)) {
                Ok(table_db) => table_db,
                Err(RethMdbxError::NotFound) => continue,
                Err(err) => return Err(eyre::eyre!("Could not open table {db_table}: {err}")),
            };
            let table_stats = tx.inner().db_stat(table_db.dbi()).map_err(|err| {
                eyre::eyre!("Could not read statistics for table {db_table}: {err}")
            })?;

            let page_size = table_stats.page_size() as usize;
            let leaf_pages = table_stats.leaf_pages();
            let branch_pages = table_stats.branch_pages();
            let overflow_pages = table_stats.overflow_pages();
            let num_pages = leaf_pages + branch_pages + overflow_pages;
            let table_size = page_size.saturating_mul(num_pages) as u64;

            stats.push(TableStats {
                name: db_table,
                entries: table_stats.entries() as u64,
                branch_pages: branch_pages as u64,
                leaf_pages: leaf_pages as u64,
                overflow_pages: overflow_pages as u64,
                total_size: table_size,
            });
        }

        Ok::<(), eyre::Report>(())
    })??;

    Ok(stats_table(&stats))
}

#[cfg(test)]
mod tests {
    use super::{file_len_if_exists, static_files_summary_table};
    use crate::{
        cli::{Cli, Commands},
        NoArgs,
    };
    use std::{fs, path::Path};

    #[test]
    fn static_files_summary_table_renders_segment_breakdown() {
        let table = static_files_summary_table(&[super::StaticFileSegmentStats {
            segment: "headers".to_string(),
            block_range: "0..=9".to_string(),
            tx_range: "N/A".to_string(),
            total_size: 10,
        }]);

        let rendered = table.to_string();
        assert!(rendered.contains("Segment"), "missing segment header: {rendered}");
        assert!(rendered.contains("headers"), "missing segment name: {rendered}");
        assert!(rendered.contains("0..=9"), "missing block range: {rendered}");
        assert!(rendered.contains("Total"), "missing total row: {rendered}");
    }

    #[test]
    fn global_datadir_parses_between_db_and_stats() {
        // `--datadir` is a global flag, so clap accepts it between `db` and the subcommand (the
        // reth-style invocation) and captures it on the top-level Cli — no per-subcommand flag
        // is needed.
        let cli = Cli::<NoArgs>::try_parse_args_from(["tn", "db", "--datadir", "/tmp/x", "stats"])
            .expect("cli parsed");
        assert_eq!(cli.datadir.as_deref(), Some(Path::new("/tmp/x")));
        let Commands::Db(_) = cli.command else {
            panic!("expected the db subcommand");
        };
    }

    #[test]
    fn file_len_if_exists_returns_zero_for_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let existing_path = temp_dir.path().join("present.bin");
        fs::write(&existing_path, [1_u8, 2, 3, 4]).unwrap();

        assert_eq!(file_len_if_exists(&existing_path).unwrap(), 4);
        assert_eq!(file_len_if_exists(&temp_dir.path().join("missing.bin")).unwrap(), 0);
    }

    #[test]
    fn parse_db_validate_subcommand() {
        let cli = Cli::<NoArgs>::try_parse_args_from(["tn", "db", "validate", "/tmp/epoch-3"])
            .expect("cli parsed");
        let Commands::Db(_) = cli.command else {
            panic!("expected the db subcommand");
        };
    }
}
