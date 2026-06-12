//! DB diagnostics command.

use clap::Parser;
use comfy_table::{Cell, Row, Table as ComfyTable};
use human_bytes::human_bytes;
use reth_db::DatabaseEnv;
use reth_db::Database as _;
use reth_db::mdbx::DatabaseArguments;
use reth_db::mdbx::open_db_read_only;
use tn_config::TelcoinDirs as _;
use std::path::PathBuf;

/// Inspect the execution database and print read-only statistics.
#[derive(Debug, Parser)]
pub struct DbCommand;

impl DbCommand {
    /// Execute the database diagnostics command.
    pub fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        let db_path = datadir.reth_db_path();
        let db = open_db_read_only(&db_path, DatabaseArguments::default())?;
        println!("{}", db_stats_table(&db)?);
        Ok(())
    }
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

fn db_stats_table(db: &DatabaseEnv) -> eyre::Result<ComfyTable> {
    let mut stats = Vec::new();

    db.view(|tx| {
        let mut db_tables = reth_db::Tables::ALL.iter().map(|table| table.name()).collect::<Vec<_>>();
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

