//! DB diagnostics command.

use clap::{Parser, Subcommand};
use comfy_table::{Cell, Row, Table as ComfyTable};
use human_bytes::human_bytes;
use reth_db::mdbx::open_db_read_only;
use reth_db::mdbx::DatabaseArguments;
use reth_db::static_file::iter_static_files;
use reth_db::Database as _;
use reth_db::DatabaseEnv;
use reth_provider::providers::StaticFileProvider;
use std::{fs, path::{Path, PathBuf}};
use tn_config::TelcoinDirs as _;
use tn_reth::traits::TNPrimitives;

/// Inspect the execution database and print read-only statistics.
#[derive(Debug, Parser)]
pub struct DbCommand {
    /// The path to the data directory. Overrides the global --datadir flag when specified here.
    ///
    /// Placing --datadir between 'db' and the subcommand mirrors the reth CLI style:
    ///   telcoin-network db --datadir /app/data stats
    #[arg(long, value_name = "DATA_DIR")]
    datadir: Option<PathBuf>,

    /// Database diagnostics subcommand.
    #[command(subcommand)]
    command: DbSubcommand,
}

/// Supported database diagnostics subcommands.
#[derive(Debug, Subcommand)]
enum DbSubcommand {
    /// Print execution database statistics.
    Stats,
}

impl DbCommand {
    /// Execute the database diagnostics command.
    ///
    /// `global_datadir` is the value of the top-level `--datadir` flag (if any). The
    /// per-subcommand `--datadir` on `DbCommand` takes precedence when provided, enabling
    /// the reth-style invocation: `telcoin-network db --datadir PATH stats`.
    pub fn execute(&self, global_datadir: PathBuf) -> eyre::Result<()> {
        let datadir = self.datadir.clone().unwrap_or(global_datadir);
        match self.command {
            DbSubcommand::Stats => {
                let db_path = datadir.reth_db_path();
                let db = open_db_read_only(&db_path, DatabaseArguments::default())?;
                if let Some(static_files_table) = static_files_summary_table_for_datadir(&datadir)? {
                    println!("{static_files_table}");
                    println!();
                } else {
                    println!("(no static files directory found at {})", datadir.join("static_files").display());
                    println!();
                }
                println!("{}", db_stats_table(&db)?);
            }
        }
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::{file_len_if_exists, static_files_summary_table};
    use std::fs;

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
    fn file_len_if_exists_returns_zero_for_missing_file() {
        let temp_dir = tempfile::tempdir().unwrap();
        let existing_path = temp_dir.path().join("present.bin");
        fs::write(&existing_path, [1_u8, 2, 3, 4]).unwrap();

        assert_eq!(file_len_if_exists(&existing_path).unwrap(), 4);
        assert_eq!(file_len_if_exists(&temp_dir.path().join("missing.bin")).unwrap(), 0);
    }
}

fn file_len_if_exists(path: &Path) -> eyre::Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err.into()),
    }
}

fn static_files_summary_table_for_datadir(datadir: &PathBuf) -> eyre::Result<Option<ComfyTable>> {
    let static_files_dir = datadir.join("static_files");
    if !static_files_dir.exists() {
        return Ok(None);
    }
    let static_file_provider =
        StaticFileProvider::<TNPrimitives>::read_only(&static_files_dir, false)?;

    let mut stats = Vec::new();
    for (segment, ranges) in iter_static_files(&static_files_dir)?.into_iter() {
        let mut segment_size = 0_u64;

        for (block_range, header) in &ranges {
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

            if header.tx_range().is_some() {
                continue;
            }
        }

        stats.push(StaticFileSegmentStats {
            segment: segment.to_string(),
            block_range: ranges
                .first()
                .and_then(|(range, _)| Some(format!("{}..={}", range.start(), range.end())))
                .unwrap_or_default(),
            tx_range: ranges
                .iter()
                .find_map(|(_, header)| {
                    header.tx_range().map(|range| format!("{}..={}", range.start(), range.end()))
                })
                .unwrap_or_else(|| "N/A".to_string()),
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
        let mut db_tables =
            reth_db::Tables::ALL.iter().map(|table| table.name()).collect::<Vec<_>>();
        db_tables.sort();

        for db_table in db_tables {
            let table_db = tx.inner().open_db(Some(db_table))?;
            let table_stats = tx.inner().db_stat(table_db.dbi()).map_err(|err| {
                eyre::eyre!("Could not read statistics for table {db_table}: {err}")
            })?;

            let page_size = table_stats.page_size() as usize;
            let leaf_pages = table_stats.leaf_pages();
            let branch_pages = table_stats.branch_pages();
            let overflow_pages = table_stats.overflow_pages();
            let num_pages = leaf_pages + branch_pages + overflow_pages;
            let table_size = page_size.saturating_mul(num_pages as usize) as u64;

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
