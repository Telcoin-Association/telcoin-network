//! DB diagnostics command.

use clap::{Parser, Subcommand};
use comfy_table::{Cell, Row, Table as ComfyTable};
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
use tn_storage::consensus_pack::{ConsensusPack, RewriteOutcome, DATA_NAME};
use tn_types::Epoch;

/// Inspect the execution database and print read-only statistics.
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
    /// Rewrite pre-fork consensus epoch packs into the current serialization.
    ///
    /// Epoch packs written before the v0.11.0-adiri fork store an `EpochMeta` whose committee
    /// predates `P2pNode.rpc`, so they only decode via a legacy compat fallback. This rewrites
    /// each affected epoch in place — preserving the original at `epoch-{N}.bak-<unix-secs>` — so
    /// the packs decode under the current format and can be served to peers. Stop the node first.
    MigrateConsensusPacks {
        /// Limit the migration to a single epoch. Default: every `epoch-{N}` dir found.
        #[arg(long)]
        epoch: Option<Epoch>,
        /// Probe each pack and report what would change, without modifying anything.
        #[arg(long)]
        dry_run: bool,
    },
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
            DbSubcommand::MigrateConsensusPacks { epoch, dry_run } => {
                migrate_consensus_packs(datadir, *epoch, *dry_run)?;
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

/// `data` file length for `epoch-{epoch}` under `epochs_dir`, or 0 if absent.
fn epoch_data_len(epochs_dir: &Path, epoch: Epoch) -> u64 {
    file_len_if_exists(&epochs_dir.join(format!("epoch-{epoch}")).join(DATA_NAME)).unwrap_or(0)
}

/// One row of the migration summary table.
struct MigrationRow {
    epoch: Epoch,
    status: &'static str,
    records: String,
    size: String,
    note: String,
}

fn migration_table(rows: &[MigrationRow]) -> ComfyTable {
    let mut table = ComfyTable::new();
    table.load_preset(comfy_table::presets::ASCII_MARKDOWN);
    table.set_header(["Epoch", "Status", "Records", "Size", "Note"]);
    for row in rows {
        let mut r = Row::new();
        r.add_cell(Cell::new(row.epoch))
            .add_cell(Cell::new(row.status))
            .add_cell(Cell::new(&row.records))
            .add_cell(Cell::new(&row.size))
            .add_cell(Cell::new(&row.note));
        table.add_row(r);
    }
    table
}

/// Rewrite pre-fork consensus epoch packs under `datadir` into the current serialization.
///
/// Enumerates `epoch-{N}` dirs under the consensus epochs directory; `only_epoch` limits to one.
/// `dry_run` performs only the read-only probe and changes nothing. Prints a summary table and
/// returns an error (non-zero exit) if any epoch failed.
fn migrate_consensus_packs(
    datadir: PathBuf,
    only_epoch: Option<Epoch>,
    dry_run: bool,
) -> eyre::Result<()> {
    let epochs_dir = datadir.epochs_db_path();
    if !epochs_dir.exists() {
        eyre::bail!("no consensus epochs directory at {}", epochs_dir.display());
    }

    // Collect `epoch-{N}` dirs. The exact-parse skips backups (`epoch-{N}.bak-*`) and the scratch
    // dir, whose names do not parse as a bare epoch number.
    let mut epochs = Vec::new();
    for entry in fs::read_dir(&epochs_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else { continue };
        let Some(rest) = name.strip_prefix("epoch-") else { continue };
        let Ok(epoch) = rest.parse::<Epoch>() else { continue };
        if only_epoch.is_none() || only_epoch == Some(epoch) {
            epochs.push(epoch);
        }
    }
    epochs.sort_unstable();

    if epochs.is_empty() {
        if let Some(e) = only_epoch {
            eyre::bail!("no epoch-{e} directory under {}", epochs_dir.display());
        }
        println!("No epoch packs found under {}", epochs_dir.display());
        return Ok(());
    }

    let mut rows = Vec::with_capacity(epochs.len());
    let mut failures = 0_usize;

    if dry_run {
        for epoch in epochs {
            let row = match ConsensusPack::epoch_pack_needs_rewrite(&epochs_dir, epoch) {
                Ok(needs) => MigrationRow {
                    epoch,
                    status: if needs { "would migrate" } else { "current" },
                    records: "-".to_string(),
                    size: human_bytes(epoch_data_len(&epochs_dir, epoch) as f64),
                    note: String::new(),
                },
                Err(e) => {
                    failures += 1;
                    MigrationRow {
                        epoch,
                        status: "failed",
                        records: "-".to_string(),
                        size: "-".to_string(),
                        note: e.to_string(),
                    }
                }
            };
            rows.push(row);
        }
    } else {
        // Scratch dir under consensus-db (same filesystem as `epochs`) so the rewrite's final
        // swap is an atomic rename. Cleaned up at the end.
        let tmp_dir = datadir.consensus_db_path().join(".migrate-tmp");
        let _ = fs::create_dir_all(&tmp_dir);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
        for epoch in epochs {
            let outcome =
                runtime.block_on(ConsensusPack::rewrite_legacy_epoch(&epochs_dir, epoch, &tmp_dir));
            let row = match outcome {
                Ok(RewriteOutcome::AlreadyCurrent) => MigrationRow {
                    epoch,
                    status: "current",
                    records: "-".to_string(),
                    size: human_bytes(epoch_data_len(&epochs_dir, epoch) as f64),
                    note: String::new(),
                },
                Ok(RewriteOutcome::Migrated { records, size_before, size_after, backup }) => {
                    MigrationRow {
                        epoch,
                        status: "migrated",
                        records: records.to_string(),
                        size: format!(
                            "{} → {}",
                            human_bytes(size_before as f64),
                            human_bytes(size_after as f64)
                        ),
                        note: format!("backup: {}", backup.display()),
                    }
                }
                Err(e) => {
                    failures += 1;
                    MigrationRow {
                        epoch,
                        status: "failed",
                        records: "-".to_string(),
                        size: "-".to_string(),
                        note: e.to_string(),
                    }
                }
            };
            rows.push(row);
        }
        let _ = fs::remove_dir_all(&tmp_dir);
    }

    println!("{}", migration_table(&rows));

    if failures > 0 {
        eyre::bail!("{failures} epoch pack(s) failed to migrate; see the table above");
    }
    Ok(())
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
    fn migrate_consensus_packs_subcommand_parses() {
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "db",
            "--datadir",
            "/tmp/x",
            "migrate-consensus-packs",
            "--epoch",
            "5",
            "--dry-run",
        ])
        .expect("cli parsed");
        assert_eq!(cli.datadir.as_deref(), Some(Path::new("/tmp/x")));
        let Commands::Db(db) = cli.command else { panic!("expected the db subcommand") };
        match db.command {
            super::DbSubcommand::MigrateConsensusPacks { epoch, dry_run } => {
                assert_eq!(epoch, Some(5));
                assert!(dry_run);
            }
            other => panic!("expected migrate-consensus-packs, got {other:?}"),
        }
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
