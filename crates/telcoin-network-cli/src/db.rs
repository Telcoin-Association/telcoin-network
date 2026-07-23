//! DB diagnostics command.
//!
//! `db stats` prints read-only statistics for the execution database. `db validate` walks a
//! consensus epoch pack's `data` stream and reports integrity issues, reproducing the importer's
//! `MissingBatches` check and classifying each missing batch as Absent (a real data gap) vs
//! Misordered (present, but in the wrong consensus-header group).

use crate::{node::NamedChain, version::SHORT_VERSION};
use clap::{Args, Parser, Subcommand};
use comfy_table::{Cell, Row, Table as ComfyTable};
use eyre::{bail, eyre};
use human_bytes::human_bytes;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, TelcoinDirs as _};
use tn_reth::{
    iter_static_files, open_db_read_only, snapshot::SnapshotRestorer, traits::TNPrimitives,
    DatabaseArguments, DatabaseEnv, RethCommand, RethConfig, RethDatabaseT as _, RethEnv,
    RethMdbxError, StaticFileProvider, Tables,
};
use tn_storage::{
    consensus_pack::{ConsensusPack, DATA_NAME},
    epoch_records::{EpochRecordDb, EpochRecordValidation},
    exec_state_pack::ExecStatePackReader,
    pack_validate::validate_pack_file,
};
use tn_types::{
    BlockNumHash, Committee, Epoch, EpochCertificate, EpochDigest, EpochRecord, ExecHeader,
    SealedHeader, TaskManager, B256,
};

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

    /// Load an EVM state-export pack into a new reth database under the datadir.
    LoadState(DbLoadStateArgs),
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
            DbSubcommand::LoadState(args) => args.execute(datadir)?,
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

/// Restore an EVM state-export pack into a new reth database under the datadir.
#[derive(Debug, Args)]
pub struct DbLoadStateArgs {
    /// Path to an exec-state pack directory (contains a `state_data` file), e.g. an `epoch-NN`
    /// export produced by `--enable-state-export`.
    pub pack: PathBuf,

    /// Named chain whose genesis to initialize (bundled). If omitted, genesis is loaded from the
    /// datadir config. Genesis is the trust root and must match the chain the pack came from.
    #[arg(long)]
    pub chain: Option<NamedChain>,
}

impl DbLoadStateArgs {
    /// Resolve the genesis chain spec, then restore the pack into a fresh reth DB under `datadir`.
    fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        // Genesis chain spec: bundled via `--chain`, else from the datadir config (mirrors the node
        // command). Genesis is the trust root, so it must match the chain the pack came from.
        let tn_config = match self.chain {
            Some(NamedChain::Adiri | NamedChain::TestNet) => {
                Config::load_adiri(&datadir, false, SHORT_VERSION)?
            }
            Some(NamedChain::MainNet) => Config::load_mainnet(&datadir, false, SHORT_VERSION)?,
            None => Config::load(&datadir, false, SHORT_VERSION)?,
        };

        // `RethConfig::new` is the only public constructor; `RethCommand` has no `Default`, so
        // parse an empty arg list for its defaults (rpc/txpool are irrelevant to a one-shot
        // restore).
        let reth_config = RethConfig::new(
            RethCommand::parse_from(["telcoin-network"]),
            None,
            &datadir,
            false,
            Arc::new(tn_config.chain_spec()),
        );

        let (block, root) = restore_pack(&reth_config, datadir.reth_db_path(), &self.pack)?;
        println!(
            "restored execution state at block {} (state root {root:#x}) into {}",
            block.number,
            datadir.reth_db_path().display()
        );

        // Rebuild the consensus + epoch-records packs from the bundle's data-only files, when the
        // bundle carries them. A plain copy would not work: these files have no index sidecars and
        // the packs do not rebuild indexes on open, so we reconstruct fully-indexed, queryable
        // packs at their datadir homes under `consensus-db/epochs/`, verifying each epoch record
        // against its certificate as we go.
        let bundle_consensus = self.pack.join("consensus_data");
        let bundle_records = self.pack.join("epoch_records");
        let bundle_certs = self.pack.join("epoch_certs");
        match (bundle_consensus.exists(), bundle_records.exists(), bundle_certs.exists()) {
            (true, true, true) => {
                // The genesis committee is the trust root for record verification; load it exactly
                // as the node does (its yaml was materialized by the `Config::load_*` step above).
                let genesis_committee =
                    Config::load_from_path::<Committee>(datadir.committee_path(), ConfigFmt::YAML)
                        .map_err(|e| {
                            eyre!(
                                "failed to load genesis committee from {} (needed to verify epoch \
                                 records): {e}",
                                datadir.committee_path().display()
                            )
                        })?;
                restore_consensus_and_records(
                    &datadir.epochs_db_path(),
                    &genesis_committee,
                    &bundle_consensus,
                    &bundle_records,
                    &bundle_certs,
                )?;
            }
            (false, false, false) => {} // state-only pack — nothing else to restore
            _ => bail!(
                "incomplete export bundle: expected consensus_data, epoch_records, and \
                 epoch_certs together (re-export with the current version)"
            ),
        }
        Ok(())
    }
}

/// Build a fresh reth DB from `reth_config` and restore the exec-state pack at `pack` into it,
/// returning the snapshot block and its recomputed state root.
///
/// Runs inside a one-shot tokio runtime because `SnapshotRestorer::open` (reth provider setup)
/// requires a runtime context; the restore steps themselves are synchronous.
fn restore_pack(
    reth_config: &RethConfig,
    db_path: PathBuf,
    pack: &Path,
) -> eyre::Result<(BlockNumHash, B256)> {
    // The reth provider setup (`RethEnv::new` inside `SnapshotRestorer::open`) captures the current
    // tokio handle, so establish a runtime context. The restore steps are otherwise synchronous;
    // any tasks the provider spawns run on the multi-thread runtime's workers.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    let _guard = runtime.enter();

    let db = RethEnv::new_database(reth_config, db_path)?;
    let task_manager = TaskManager::new("db-load-state");

    let mut reader = ExecStatePackReader::open(pack)
        .map_err(|e| eyre!("failed to open state pack {}: {e}", pack.display()))?;
    let final_state = BlockNumHash::new(reader.meta().block_number, reader.meta().block_hash);
    let window = scaffold_window(reader.headers());

    // `open` refuses a datadir that already holds chain data, so this only lands in a fresh one.
    let restorer = SnapshotRestorer::open(reth_config, db, &task_manager)?;
    restorer.import_chain_scaffold(&window, final_state)?;
    let root = restorer.import_state(&mut reader)?;
    restorer.finish(final_state)?;
    Ok((final_state, root))
}

/// Per-record read timeout for the consensus stream import. The source is a local file so reads are
/// fast; this is a generous ceiling that fails cleanly on a truncated/corrupt pack instead of
/// hanging.
const STREAM_IMPORT_TIMEOUT: Duration = Duration::from_secs(60);

/// Rebuild fully-indexed, verified consensus + epoch-records packs under `epochs_dir` from an
/// export bundle's data-only files.
///
/// The bundle's `epoch_records` / `epoch_certs` / `consensus_data` are bare pack `data` streams
/// with no index sidecars, and the packs do not rebuild indexes on open, so we reconstruct them:
/// every epoch record is verified against its certificate (anchored to `genesis_committee`, then
/// chained forward) and re-saved with its cert — rebuilding `epochs.pack` + its indexes — and the
/// closed epoch's consensus pack is stream-imported (rebuilding `epoch-{N}/` + its idx/hash/bhash).
fn restore_consensus_and_records(
    epochs_dir: &Path,
    genesis_committee: &Committee,
    bundle_consensus: &Path,
    bundle_records: &Path,
    bundle_certs: &Path,
) -> eyre::Result<()> {
    fs::create_dir_all(epochs_dir)?;

    // Read the records straight from the bare bundle pack (no index needed). They drive the
    // records-DB rebuild, the per-record verification, and the consensus stream import (previous
    // epoch + final consensus number).
    let records = EpochRecordDb::read_records_from_pack(bundle_records).map_err(|e| {
        eyre!("failed to read epoch records from {}: {e}", bundle_records.display())
    })?;
    if records.is_empty() {
        bail!("bundle epoch_records contains no records");
    }
    // Certificates are matched to records by digest (`cert.epoch_hash == record.digest()`). Not
    // every record necessarily has one: the just-closed tip epoch's cert is only aggregated at the
    // next epoch's start, so it is typically absent at export time.
    let certs = EpochRecordDb::read_certs_from_pack(bundle_certs).map_err(|e| {
        eyre!("failed to read epoch certificates from {}: {e}", bundle_certs.display())
    })?;
    let cert_by_hash: HashMap<EpochDigest, EpochCertificate> =
        certs.into_iter().map(|c| (c.epoch_hash, c)).collect();
    // `save_record` appends in order starting at epoch 0, so the bundle must hold a contiguous run
    // from epoch 0. Check up front for a clear error instead of a mid-loop `EpochOutOfOrder`.
    for (i, record) in records.iter().enumerate() {
        if record.epoch as usize != i {
            bail!(
                "bundle epoch records are not contiguous from epoch 0 (position {i} is epoch {})",
                record.epoch
            );
        }
    }
    let n = records.last().expect("records non-empty").epoch;

    // Both `save_record` and `stream_import` are async; the exec-state restore already built and
    // dropped its own runtime, so drive the rebuild to completion on a fresh one here.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    runtime.block_on(async {
        // 1. Rebuild the indexed epoch-records DB, verifying each record against its certificate as
        //    we go. Verify BEFORE saving so `validate_downloaded_record` anchors epoch 0 to the
        //    seeded genesis committee and epoch N to the already-saved record N-1.
        let db = EpochRecordDb::open(epochs_dir)
            .map_err(|e| eyre!("failed to open epoch records db: {e}"))?;

        // Seed a dummy epoch-0 record carrying the genesis committee (mirrors the node's genesis
        // bootstrap in run_epoch.rs); it is the trusted anchor for verifying epoch 0 and is only
        // consulted while the real record index is still empty.
        let genesis_keys: Vec<_> = genesis_committee.bls_keys().into_iter().collect();
        db.save_dummy_epoch0(EpochRecord {
            epoch: 0,
            committee: genesis_keys.clone(),
            next_committee: genesis_keys,
            ..Default::default()
        })
        .await
        .map_err(|e| eyre!("failed to seed genesis committee: {e}"))?;

        for (i, record) in records.iter().enumerate() {
            match cert_by_hash.get(&record.digest()) {
                Some(cert) => {
                    // The blessed validator: checks epoch, parent hash, committee compatibility,
                    // and a super-quorum certificate from the trusted
                    // (chained-from-genesis) committee.
                    match db.validate_downloaded_record(record.epoch, record, cert).await {
                        EpochRecordValidation::Valid => {}
                        other => bail!(
                            "epoch {} record failed certificate verification: {other:?}",
                            record.epoch
                        ),
                    }
                    db.save(record.clone(), cert.clone())
                        .await
                        .map_err(|e| eyre!("failed to save epoch record {}: {e}", record.epoch))?;
                }
                None => {
                    // No certificate in the bundle — expected for the just-closed tip epoch, whose
                    // cert is only aggregated at the next epoch's start. Anchor it to the verified
                    // chain by its parent hash and store it unverified rather than failing.
                    let expected_parent =
                        if i == 0 { EpochDigest::default() } else { records[i - 1].digest() };
                    if record.parent_hash != expected_parent {
                        bail!(
                            "epoch {} record has no certificate and does not chain to its \
                             predecessor",
                            record.epoch
                        );
                    }
                    eprintln!(
                        "warning: epoch {} record has no certificate in the bundle (not yet \
                         collected at export time); stored unverified but chained to the verified \
                         predecessor",
                        record.epoch
                    );
                    db.save_record(record.clone())
                        .await
                        .map_err(|e| eyre!("failed to save epoch record {}: {e}", record.epoch))?;
                }
            }
        }
        db.persist().await.map_err(|e| eyre!("failed to persist epoch records: {e}"))?;
        drop(db);

        // 2. Rebuild the closed epoch's consensus pack. Epoch 0 would need a pre-epoch-0 genesis
        //    descriptor that a data-only bundle doesn't carry, so skip it (records + state still
        //    land); every later epoch uses the previous record as its genesis link.
        if n == 0 {
            eprintln!(
                "warning: skipping epoch-0 consensus pack rebuild (requires a genesis descriptor \
                 not present in the bundle); restored epoch records and execution state only"
            );
            return Ok(());
        }
        let previous = &records[(n - 1) as usize];
        let final_record = &records[n as usize];
        let epoch_dir = epochs_dir.join(format!("epoch-{n}"));
        let file = tokio::fs::File::open(bundle_consensus).await.map_err(|e| {
            eyre!("failed to open consensus data {}: {e}", bundle_consensus.display())
        })?;
        // Landing directly at `epochs_dir` (not a temp) is safe here: this is an offline, single
        // writer restore into a fresh datadir, so the online rename/install-lock dance is unneeded.
        let pack = ConsensusPack::stream_import(
            epochs_dir,
            file,
            n,
            previous,
            final_record.final_consensus.number,
            STREAM_IMPORT_TIMEOUT,
        )
        .await
        .map_err(|e| {
            let _ = fs::remove_dir_all(&epoch_dir);
            eyre!("consensus stream import for epoch {n} failed: {e}")
        })?;
        pack.persist().await.map_err(|e| eyre!("failed to persist consensus pack: {e}"))?;
        // The chain was verified as it streamed; confirm the rebuilt tip is exactly the epoch's
        // final consensus header before declaring success.
        let tip = pack.latest_consensus_header().await;
        drop(pack);
        let tip_ok = matches!(
            &tip,
            Some(header)
                if header.number == final_record.final_consensus.number
                    && header.digest() == final_record.final_consensus.hash
        );
        if !tip_ok {
            let _ = fs::remove_dir_all(&epoch_dir);
            bail!("rebuilt consensus pack tip does not match epoch {n} final consensus: {tip:?}");
        }
        Ok(())
    })?;

    println!(
        "restored and verified epoch records 0..={n} and consensus pack for epoch {n} into {}",
        epochs_dir.display()
    );
    Ok(())
}

/// Turn a pack's embedded headers (snapshot header first, then ancestors) into the ascending,
/// genesis-excluded window `SnapshotRestorer::import_chain_scaffold` expects.
fn scaffold_window(headers: &[ExecHeader]) -> Vec<SealedHeader> {
    let mut window: Vec<SealedHeader> =
        headers.iter().cloned().map(SealedHeader::seal_slow).collect();
    window.sort_by_key(|h| h.number);
    window.retain(|h| h.number != 0);
    window
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

    #[test]
    fn parse_db_load_state_subcommand() {
        let cli = Cli::<NoArgs>::try_parse_args_from(["tn", "db", "load-state", "/tmp/epoch-3"])
            .expect("cli parsed");
        let Commands::Db(_) = cli.command else {
            panic!("expected the db subcommand");
        };
    }

    #[test]
    fn scaffold_window_orders_and_drops_genesis() {
        use tn_types::ExecHeader;
        let header = |number| ExecHeader { number, ..Default::default() };
        // out of order and including genesis (block 0)
        let headers = vec![header(3), header(1), header(0), header(2)];
        let numbers: Vec<u64> = super::scaffold_window(&headers).iter().map(|h| h.number).collect();
        assert_eq!(numbers, vec![1, 2, 3], "ascending, contiguous, genesis dropped");
    }
}
