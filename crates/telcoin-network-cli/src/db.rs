//! DB diagnostics command.
//!
//! `db stats` prints read-only statistics for the execution database. `db validate` walks a
//! consensus epoch pack's `data` stream and reports integrity issues, reproducing the importer's
//! `MissingBatches` check and classifying each missing batch as Absent (a real data gap) vs
//! Misordered (present, but in the wrong consensus-header group).

use crate::{
    node::{validate_faucet_build, NamedChain},
    version::SHORT_VERSION,
};
use clap::{Args, Parser, Subcommand};
use comfy_table::{Cell, Row, Table as ComfyTable};
use eyre::{bail, eyre};
use human_bytes::human_bytes;
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, PidLock, TelcoinDirs as _};
use tn_reth::{
    iter_static_files, open_db_read_only, snapshot::SnapshotRestorer, DatabaseArguments,
    DatabaseEnv, RethCommand, RethConfig, RethDatabaseT as _, RethEnv, RethMdbxError,
    StaticFileProvider, TNPrimitives, Tables,
};
use tn_storage::{
    consensus::ConsensusChain,
    consensus_pack::{
        legacy_migration_dry_run, pack_unsealed_version, wal_consistent_end, ConsensusPack,
        EpochMigrate, EpochRepair, DATA_NAME, SENTINEL_MIN_VERSION,
    },
    epoch_records::{validate_record_against_anchor, EpochRecordDb, EpochRecordValidation},
    exec_state_pack::ExecStatePackReader,
    pack_validate::{
        classify_physical_corruption, incomplete_trailing_output, recovery_refusal,
        validate_pack_file, validate_pack_file_bounded, Verdict,
    },
};
use tn_types::{
    BlockNumHash, BlsPublicKey, Committee, Epoch, EpochCertificate, EpochDigest, EpochRecord,
    ExecHeader, SealedHeader, TaskManager, B256,
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
    /// Read-only (never mutates). Validate the CURRENT/latest epoch only with the node STOPPED —
    /// see the arg help.
    Validate(DbValidateArgs),

    /// Repair consensus epoch packs at rest: truncate a torn data-file tail and rebuild indexes.
    /// The node MUST be stopped. Dry-run by default; pass `--force` to apply.
    Repair(DbRepairArgs),

    /// Migrate legacy (pre-v2) consensus epoch packs up to the current v2 format: rewrite the data
    /// log header-first, rebuild indexes, and re-seal. The node MUST be stopped. Dry-run by
    /// default; pass `--force` to apply.
    Migrate(DbMigrateArgs),

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
            DbSubcommand::Repair(args) => args.execute(datadir)?,
            DbSubcommand::Migrate(args) => args.execute(datadir)?,
            DbSubcommand::LoadState(args) => args.execute(datadir)?,
        }
        Ok(())
    }
}

/// Validate a consensus epoch pack file.
///
/// Read-only: this never mutates the pack. It opens the `data` stream read-only via mmap, so
/// validating the CURRENT/latest epoch — the one a running node holds open for append — is unsafe:
/// the node truncates that file on epoch-close and remaps it on growth, and a concurrent truncate
/// can crash this command with a SIGBUS (node data is never harmed). `validate` takes a pack path
/// rather than the datadir, so it does not consult the `telcoin.pid` lock — validate the
/// current/latest epoch only with the node STOPPED. A sealed past epoch is safe to validate live,
/// except briefly during an epoch handoff before the just-sealed pack has its clean-close sentinel.
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

        // Warn (do not refuse) if this looks like the current/latest epoch a running node may hold
        // open — `db validate` maps it read-only, so a concurrent truncate/grow could SIGBUS us.
        warn_if_current_epoch(&data_file, epoch);

        // A legacy v0 (batches-first) pack is only ever read by its migration to v2, so it is
        // judged by a dry run of that migration (what `db migrate`, `db repair` and the node all
        // run on it), not by the header-first walks below.
        if matches!(pack_unsealed_version(&data_file, epoch), Some((0, _))) {
            return match legacy_migration_dry_run(&data_file, epoch) {
                Ok(outputs) => {
                    println!(
                        "legacy v0 pack: a dry run of its migration to v2 copies {outputs} \
                         output(s) cleanly; `db migrate --force`, or the node when it opens or \
                         reads this epoch, converts it."
                    );
                    Ok(())
                }
                Err(why) => {
                    println!("legacy v0 pack: its migration to v2 refuses: {why}");
                    bail!("pack {} is INVALID (see report above)", data_file.display());
                }
            };
        }

        // Physical framing first: a torn/corrupt record stream cannot be walked for logical checks,
        // so classify the failure mode (truncatable tail vs data-losing corruption) and report the
        // recommended operator action instead of bailing with a bare read error.
        if let Some(corruption) = classify_physical_corruption(&data_file, epoch)
            .map_err(|e| eyre!("failed to open pack {}: {e}", data_file.display()))?
        {
            print!("{corruption}");
            // A truncatable tail (torn/unacked) heals on the next append-open, but the intact
            // committed prefix before it must still be logically checked — otherwise `db validate`
            // reports nothing useful for the normal shape of any crashed current epoch. Bound the
            // walk at the last COMPLETE output (the WAL `consistent_end`), NOT at the
            // first bad record offset: a tear inside the in-flight output would
            // otherwise report that output's unwritten batches as false "absent" and
            // flip the verdict INVALID right after "SAFE".
            let mut prefix_invalid = false;
            // "Truncatable" is only what recovery would do: a tear past which an acked output
            // still lies is refused by a node restart and `db repair --force` alike, and a legacy
            // pack's tail is judged by the migration. Say so instead of promising a self-heal.
            if corruption.kind.is_truncatable() {
                if let Some(why) = recovery_refusal(&data_file, epoch) {
                    println!("NOT TRUNCATABLE: recovery would refuse to truncate this tail: {why}");
                    bail!("pack {} is corrupt (see report above)", data_file.display());
                }
                print_legacy_tail_note(&data_file, epoch);
            }
            if corruption.kind.is_truncatable() && corruption.records_ok_before > 0 {
                let bound = wal_consistent_end(&data_file, epoch).unwrap_or(corruption.offset);
                eprintln!("\nValidating the intact prefix before the tear (up to byte {bound})...",);
                match validate_pack_file_bounded(&data_file, epoch, None, Some(bound)) {
                    Ok(report) => {
                        prefix_invalid = report.verdict == Verdict::Invalid;
                        print!("{report}");
                    }
                    Err(e) => {
                        // The prefix could not be checked, so it cannot be reported healthy.
                        eprintln!("bounded validation of the intact prefix failed: {e}");
                        prefix_invalid = true;
                    }
                }
            }
            // Exit code: a truncatable tail whose intact prefix is Valid is a benign, self-healing
            // shape → success. A data-losing corruption kind (or an INVALID prefix) is a real
            // problem → non-zero exit so scripts/operators notice.
            if !corruption.kind.is_truncatable() || prefix_invalid {
                bail!("pack {} is corrupt (see report above)", data_file.display());
            }
            return require_current_epoch_for_tail(&data_file, epoch);
        }

        // Every record frames, but an unclean pack can still end in an incomplete output whose
        // last record happens to end the file: an unacked in-flight write that recovery truncates,
        // not corruption. Report it as such and validate the complete prefix before it.
        if let Some(end) = incomplete_trailing_output(&data_file, epoch) {
            if let Some(why) = recovery_refusal(&data_file, epoch) {
                println!(
                    "NOT TRUNCATABLE: the pack was not cleanly closed and its last output is \
                     incomplete (bytes past offset {end}), but recovery would refuse to truncate \
                     it: {why}"
                );
                bail!("pack {} is corrupt (see report above)", data_file.display());
            }
            println!(
                "TRUNCATABLE: the pack was not cleanly closed and its last output is incomplete \
                 (bytes past offset {end}); this unacked in-flight write is truncated by a node \
                 restart or `db repair --force`."
            );
            print_legacy_tail_note(&data_file, epoch);
            eprintln!("\nValidating the complete prefix (up to byte {end})...");
            let report = validate_pack_file_bounded(&data_file, epoch, None, Some(end))
                .map_err(|e| eyre!("failed to validate pack {}: {e}", data_file.display()))?;
            print!("{report}");
            if report.verdict == Verdict::Invalid {
                bail!("pack {} is INVALID (see report above)", data_file.display());
            }
            return require_current_epoch_for_tail(&data_file, epoch);
        }

        let report = validate_pack_file(&data_file, epoch, None)
            .map_err(|e| eyre!("failed to validate pack {}: {e}", data_file.display()))?;

        // Report goes to stdout (tracing/logs go to stderr/file).
        print!("{report}");
        // Non-zero exit on an INVALID verdict so automation can tell a healthy datadir from a
        // corrupt one by exit status.
        if report.verdict == Verdict::Invalid {
            bail!("pack {} is INVALID (see report above)", data_file.display());
        }
        Ok(())
    }
}

/// A truncatable tail heals only where the node opens the epoch for append: the epoch it resumes
/// (or one it has yet to reach). The node only reads a past epoch and never truncates it, so there
/// the tail needs `db repair`: report that and fail. A legacy (pre-v2) pack is exempt, since the
/// node migrates it to v2 when it reads the epoch, which drops the tail.
fn require_current_epoch_for_tail(data_file: &Path, epoch: Epoch) -> eyre::Result<()> {
    let legacy = pack_unsealed_version(data_file, epoch)
        .is_some_and(|(version, _)| version < SENTINEL_MIN_VERSION);
    if !legacy && is_past_epoch(data_file, epoch) {
        println!(
            "ACTION NEEDED: epoch {epoch} is a past epoch, which the node only reads and never \
             truncates: stop the node and run `telcoin-network db repair --epoch {epoch} --force`."
        );
        bail!("pack {} needs `db repair` (see report above)", data_file.display());
    }
    Ok(())
}

/// Whether `epoch` is before the epoch the node resumes from, read from the latest-consensus hint
/// in the epochs directory holding `data_file`'s `epoch-N` directory (what the node itself opens
/// for append). Falls back to the directory listing when there is no hint.
fn is_past_epoch(data_file: &Path, epoch: Epoch) -> bool {
    let resume = data_file.parent().and_then(Path::parent).and_then(ConsensusChain::resume_epoch);
    match resume {
        Some(resume) => epoch < resume,
        None => later_epoch_exists(data_file, epoch),
    }
}

/// Whether the epochs directory holding `data_file`'s `epoch-N` directory also holds a later
/// epoch. `false` when the layout cannot be read (e.g. a bare data file outside an epochs dir).
fn later_epoch_exists(data_file: &Path, epoch: Epoch) -> bool {
    data_file
        .parent()
        .and_then(Path::parent)
        .and_then(|epochs_dir| ConsensusPack::epoch_dirs(epochs_dir).ok())
        .is_some_and(|epochs| epochs.last().is_some_and(|&last| last > epoch))
}

/// For a legacy (pre-v2) pack whose unacked tail is truncatable, say how it goes away: the
/// migration to v2 drops it, whether run by `db migrate` or by the node when it opens or reads the
/// epoch. (A v2 pack's tail is covered by the printed corruption's recommended action.)
fn print_legacy_tail_note(data_file: &Path, epoch: Epoch) {
    if let Some((version, _)) = pack_unsealed_version(data_file, epoch) {
        if version < SENTINEL_MIN_VERSION {
            println!(
                "note: legacy (v{version}) pack — migrating it to v2 (`db migrate --force`, or the \
                 node when it opens or reads this epoch) drops this unacked tail."
            );
        }
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

/// Best-effort warning when `data_file` is a pack a running node may still be WRITING — which
/// `db validate` maps read-only, so a concurrent truncate/grow could SIGBUS this process. Past
/// epochs are safe to validate live ONCE SEALED, but there is no single "current epoch" number:
/// just after an epoch transition the previous epoch is briefly padded-and-unsealed while it
/// finishes closing, and a catching-up node holds an older epoch open while newer epoch dirs
/// already exist. So warn on two signals, whatever the epoch number: (a) it is the highest
/// `epoch-NN` dir (the usual live epoch), or (b) the pack carries no clean-close sentinel
/// (`opened_unclean` — a writer is still finishing it). Never refuses — validation is read-only and
/// is valid once the node is stopped.
fn warn_if_current_epoch(data_file: &Path, epoch: Epoch) {
    let is_highest = data_file
        .parent()
        .and_then(Path::parent)
        .and_then(|epochs_dir| ConsensusPack::epoch_dirs(epochs_dir).ok())
        .is_some_and(|all| all.last().copied() == Some(epoch));
    // `None` if the pack cannot be opened — leave that to the classifier/validator below.
    let unsealed = matches!(pack_unsealed_version(data_file, epoch), Some((_, true)));
    if is_highest || unsealed {
        let why = if is_highest {
            "it is the current/latest epoch a running node holds open for append"
        } else {
            "it has no clean-close sentinel — a writer may still be finishing it (an epoch just \
             transitioned, or a catching-up node holds it open)"
        };
        eprintln!(
            "WARNING: epoch {epoch} may be written by a running node ({why}). `db validate` maps it \
             read-only; if the node truncates or grows the file concurrently this command can crash \
             (SIGBUS). Validate it only with the node STOPPED. (Node data is not modified either way.)"
        );
    }
}

/// Parse an epoch out of an `epoch-NN` directory name.
fn epoch_from_dir_name(dir: &Path) -> Option<Epoch> {
    dir.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("epoch-"))
        .and_then(|num| num.parse::<Epoch>().ok())
}

/// Select which epochs `db repair` targets, given all `epoch-{N}` dirs (sorted ascending) and an
/// optional `--epoch`. In all-mode (`requested == None`) the current/latest epoch — the one a
/// running node holds open for append — is skipped and returned as the second element; `--epoch N`
/// targets exactly N (no skip). The caller validates that a requested epoch exists.
///
/// "Current" here is a best-effort heuristic: the highest-numbered `epoch-{N}` directory. That is
/// the live epoch except in the brief window during an epoch transition when the next epoch's
/// directory already exists on disk before the node has switched to it — the `LatestConsensus`
/// slot, not the directory listing, is the authoritative current epoch. This skip is only a
/// convenience guard against fat-fingering a repair of the live pack; the real safety requirement
/// is that the node is stopped (a running node holds its pack mmap'd for append regardless of which
/// epoch is "current"). Pass `--epoch N` to target an exact epoch when the heuristic would pick
/// wrong.
fn repair_targets(all: &[Epoch], requested: Option<Epoch>) -> (Vec<Epoch>, Option<Epoch>) {
    let current = all.last().copied();
    match requested {
        Some(e) => (vec![e], None),
        None => (all.iter().copied().filter(|e| Some(*e) != current).collect(), current),
    }
}

/// Repair consensus epoch pack files at rest.
///
/// Truncates a torn `data`-file tail and rebuilds missing/corrupt sidecar indexes from the data log
/// (the source of truth), then re-seals the pack. Damage that truncation cannot fix — a
/// torn/corrupt epoch-meta, or mid-log data corruption — is reported (re-sync required), never
/// touched.
///
/// The node MUST be stopped: with `--force` this opens packs for append and rewrites them, which
/// would corrupt a running node's memory mapping. It refuses to run while the datadir PID lock
/// (`telcoin.pid`) is held by a live node, and it takes that lock for its own run so a node cannot
/// start mid-repair. It is a dry run by default (read-only classification, no writes) and requires
/// `--force` to apply. In repair-all mode the current/latest epoch (the one a running node holds
/// open for append) is skipped; repair it explicitly with `--epoch N` once the node is confirmed
/// stopped.
#[derive(Debug, Args)]
pub struct DbRepairArgs {
    /// Repair only this epoch. Without it, every epoch except the current/latest is repaired.
    #[arg(long)]
    pub epoch: Option<Epoch>,

    /// Apply repairs. Without this the command is a dry run: it reports what it would repair but
    /// writes nothing. Stop the node before passing `--force`.
    #[arg(long)]
    pub force: bool,
}

impl DbRepairArgs {
    /// Assess (and, with `--force`, repair) the consensus epoch packs under the datadir.
    fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        let epochs_dir = datadir.epochs_db_path();
        if !epochs_dir.is_dir() {
            bail!("no consensus epochs directory at {}", epochs_dir.display());
        }

        // Loud safety banner in both modes (stderr; the report goes to stdout).
        eprintln!(
            "WARNING: `db repair` rewrites consensus pack files. The node MUST be stopped first — \
             repairing files a running node holds mapped will corrupt them."
        );
        if !self.force {
            eprintln!("(dry run: reporting only; re-run with `--force` to apply)");
        }

        // Refuse to touch pack files a live node holds mapped. Take the datadir PID lock (writing
        // our own PID) so a node cannot start mid-repair either; both are released when
        // this returns. A stale lock from a previous crash is reclaimed. Do this before any
        // read, in both dry-run and apply modes, so operators get a clear error rather than
        // a confusing partial report.
        let _pid_lock = PidLock::acquire(&datadir)?;

        let all = ConsensusPack::epoch_dirs(&epochs_dir)
            .map_err(|e| eyre!("failed to list epochs under {}: {e}", epochs_dir.display()))?;
        if let Some(e) = self.epoch {
            if !all.contains(&e) {
                bail!("epoch {e} not found under {}", epochs_dir.display());
            }
        }
        let (targets, skipped_current) = repair_targets(&all, self.epoch);
        if let Some(cur) = skipped_current {
            println!(
                "epoch {cur}: SKIPPED (current/latest epoch) — repair explicitly with \
                 `--epoch {cur}` once the node is confirmed stopped"
            );
        }

        // Both `ConsensusPack::repair_epoch` and `EpochRecordDb::persist` are async; drive them on
        // a dedicated runtime (mirrors the restore path).
        let runtime =
            tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
        runtime.block_on(async {
            // Counts both applied repairs (`--force` → `Repaired`) and dry-run findings
            // (`WouldRepair`); the two variants are mutually exclusive per invocation, and the
            // summary verb below reflects which one actually ran.
            let mut actionable = 0usize;
            let mut unrepairable = 0usize;
            let mut errored = 0usize;
            for epoch in &targets {
                match ConsensusPack::repair_epoch(&epochs_dir, *epoch, self.force).await {
                    Ok(EpochRepair::Healthy) => println!("epoch {epoch}: OK"),
                    Ok(EpochRepair::Repaired(what)) => {
                        actionable += 1;
                        println!("epoch {epoch}: REPAIRED — {what}");
                    }
                    Ok(EpochRepair::WouldRepair(what)) => {
                        actionable += 1;
                        println!("epoch {epoch}: would repair — {what}");
                    }
                    Ok(EpochRepair::Unrepairable(why)) => {
                        unrepairable += 1;
                        println!("epoch {epoch}: UNREPAIRABLE — {why}");
                    }
                    Err(e) => {
                        errored += 1;
                        println!("epoch {epoch}: ERROR — {e}");
                    }
                }
            }

            // The shared epoch-records DB (`epochs.pack`/`epoch_certs.pack`) auto-heals on open;
            // only touch it under --force.
            if self.force {
                match EpochRecordDb::open(epochs_dir.as_path()) {
                    Ok(db) => {
                        db.persist()
                            .await
                            .map_err(|e| eyre!("failed to persist epoch-records DB: {e}"))?;
                        db.close().await;
                        println!(
                            "epoch-records DB: opened and healed (torn tails truncated; \
                             digest/position indexes rebuilt from the data logs if a prior crash \
                             left them inconsistent)"
                        );
                    }
                    Err(e) => {
                        errored += 1;
                        println!(
                            "epoch-records DB: could not open to heal ({e}); re-sync/restore may be \
                             required"
                        );
                    }
                }
            } else {
                // Predict, read-only, what the `--force` open would do, so a records DB that would
                // refuse to open (and so block node startup) is reported now, not only on apply.
                match EpochRecordDb::assess(epochs_dir.as_path()) {
                    Ok(None) => println!("epoch-records DB: OK"),
                    Ok(Some(what)) => {
                        actionable += 1;
                        println!("epoch-records DB: would heal with `--force` — {what}");
                    }
                    Err(e) => {
                        errored += 1;
                        println!(
                            "epoch-records DB: would fail to open ({e}); re-sync/restore may be \
                             required"
                        );
                    }
                }
            }

            let verb = if self.force { "repaired" } else { "to repair (dry run)" };
            println!(
                "\nsummary: {actionable} epoch(s) {verb}, {unrepairable} unrepairable (data loss / \
                 re-sync), {errored} error(s)."
            );
            // Non-zero exit when anything could not be made healthy, so automation can distinguish a
            // clean datadir from one needing attention (a dry run that only found `WouldRepair` items
            // is still success — nothing is wrong yet that this tool refused to handle).
            if unrepairable > 0 || errored > 0 {
                bail!(
                    "{unrepairable} epoch(s) unrepairable, {errored} error(s) (see report above)"
                );
            }
            Ok::<(), eyre::Report>(())
        })?;
        Ok(())
    }
}

/// Migrate legacy (pre-v2) consensus epoch pack files up to the current v2 format.
///
/// v2 is the only writable pack format: it carries the clean-close sentinel that lets recovery tell
/// a truncatable unacked tail from at-rest corruption of committed data. A pre-v2 pack (v0
/// batches-first or v1 header-first, from before this format existed) never carried a sentinel, so
/// leaving it in place makes recovery/repair guess at "sealed" and risks truncating committed data.
/// Migration rewrites the data log into a fresh v2 log (reordering a v0 batches-first log
/// header-first), rebuilds the indexes, and installs it atomically. A node reopening its current
/// epoch after an upgrade migrates it automatically; this command lets an operator upgrade every
/// epoch at once (it may become mandatory in a future release).
///
/// The node MUST be stopped: with `--force` this rewrites pack files, which would corrupt a running
/// node's memory mapping. It refuses to run while the datadir PID lock (`telcoin.pid`) is held by a
/// live node (and takes that lock for its own run), and is a dry run by default, requiring
/// `--force` to apply. A legacy pack whose data log is damaged below the acked frontier is reported
/// (re-sync required), never truncated.
#[derive(Debug, Args)]
pub struct DbMigrateArgs {
    /// Migrate only this epoch. Without it, every legacy epoch is migrated.
    #[arg(long)]
    pub epoch: Option<Epoch>,

    /// Apply migrations. Without this the command is a dry run: it reports what it would migrate
    /// but writes nothing. Stop the node before passing `--force`.
    #[arg(long)]
    pub force: bool,
}

impl DbMigrateArgs {
    /// Assess (and, with `--force`, apply) the v1/v0→v2 migration of consensus epoch packs under
    /// the datadir.
    fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        let epochs_dir = datadir.epochs_db_path();
        if !epochs_dir.is_dir() {
            bail!("no consensus epochs directory at {}", epochs_dir.display());
        }

        // Loud safety banner in both modes (stderr; the report goes to stdout).
        eprintln!(
            "WARNING: `db migrate` rewrites consensus pack files. The node MUST be stopped first — \
             rewriting files a running node holds mapped will corrupt them."
        );
        if !self.force {
            eprintln!("(dry run: reporting only; re-run with `--force` to apply)");
        }

        // Refuse to run while a live node holds the datadir PID lock, and take it for our own run
        // so a node cannot start mid-migration; released when this returns. A stale lock is
        // reclaimed.
        let _pid_lock = PidLock::acquire(&datadir)?;

        let all = ConsensusPack::epoch_dirs(&epochs_dir)
            .map_err(|e| eyre!("failed to list epochs under {}: {e}", epochs_dir.display()))?;
        if let Some(e) = self.epoch {
            if !all.contains(&e) {
                bail!("epoch {e} not found under {}", epochs_dir.display());
            }
        }
        let targets: Vec<Epoch> = match self.epoch {
            Some(e) => vec![e],
            None => all,
        };

        let runtime =
            tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
        runtime.block_on(async {
            // `actionable` counts applied migrations (`--force` → `Migrated`) or dry-run findings
            // (`WouldMigrate`); the two are mutually exclusive per invocation.
            let mut actionable = 0usize;
            let mut already = 0usize;
            let mut corrupt = 0usize;
            for epoch in &targets {
                match ConsensusPack::migrate_epoch(&epochs_dir, *epoch, self.force).await {
                    Ok(EpochMigrate::AlreadyCurrent) => {
                        already += 1;
                        println!("epoch {epoch}: already v2");
                    }
                    Ok(EpochMigrate::Migrated(what)) => {
                        actionable += 1;
                        println!("epoch {epoch}: MIGRATED — {what}");
                    }
                    Ok(EpochMigrate::WouldMigrate(what)) => {
                        actionable += 1;
                        println!("epoch {epoch}: would migrate — {what}");
                    }
                    Ok(EpochMigrate::Corrupt(why)) => {
                        corrupt += 1;
                        println!("epoch {epoch}: CORRUPT — {why}");
                    }
                    Err(e) => {
                        corrupt += 1;
                        println!("epoch {epoch}: ERROR — {e}");
                    }
                }
            }

            let verb = if self.force { "migrated" } else { "to migrate (dry run)" };
            println!(
                "\nsummary: {actionable} epoch(s) {verb}, {already} already v2, {corrupt} corrupt \
                 (re-sync required)."
            );
            // Non-zero exit when any pack could not be migrated, so scripts/operators notice.
            if corrupt > 0 {
                bail!("{corrupt} epoch(s) could not be migrated (see report above)");
            }
            Ok::<(), eyre::Report>(())
        })?;
        Ok(())
    }
}

/// Restore an EVM state-export pack into a new reth database under the datadir.
#[derive(Debug, Args)]
pub struct DbLoadStateArgs {
    /// Path to an exec-state pack directory (contains a `state_data` file), e.g. an `epoch-NN`
    /// export produced by `--enable-state-export`.
    ///
    /// The pack's final block must be one that closed an epoch — a restored node seeds its first
    /// epoch entry from a single state read at that block, so restore validates this and refuses
    /// the pack otherwise. Bundles written by `--enable-state-export` always satisfy it: an export
    /// is triggered at epoch close, on the epoch's final block.
    pub pack: PathBuf,

    /// Named chain whose genesis to initialize (bundled). If omitted, genesis is loaded from the
    /// datadir config. Genesis is the trust root and must match the chain the pack came from.
    #[arg(long)]
    pub chain: Option<NamedChain>,
}

impl DbLoadStateArgs {
    /// Resolve the genesis chain spec, then restore the pack into a fresh reth DB under `datadir`.
    fn execute(&self, datadir: PathBuf) -> eyre::Result<()> {
        // This writes chain data into the datadir: refuse to run while a live node holds the
        // datadir PID lock, and take it for our own run so a node cannot start mid-import;
        // released when this returns. Every config load below needs the datadir to exist, and so
        // does the lockfile: say so directly rather than through the lockfile's open error.
        if !datadir.is_dir() {
            bail!("datadir {} does not exist", datadir.display());
        }
        let _pid_lock = PidLock::acquire(&datadir)?;

        // Genesis chain spec: bundled via `--chain`, else from the datadir config (mirrors the node
        // command). Genesis is the trust root, so it must match the chain the pack came from.
        let tn_config = self.chain.map_or_else(
            || Config::load(&datadir, SHORT_VERSION),
            |chain| match chain {
                NamedChain::Adiri | NamedChain::TestNet => {
                    Config::load_adiri(&datadir, SHORT_VERSION)
                }
                NamedChain::MainNet => Config::load_mainnet(&datadir, SHORT_VERSION),
            },
        )?;

        // A faucet-compiled binary must not prepare mainnet chain data either: same guard as
        // the node command, refused before any datadir mutation.
        validate_faucet_build(tn_reth::FAUCET_ENABLED, tn_config.genesis())?;

        // Reject a non-resumable bundle BEFORE `restore_pack` mutates the datadir, so a refusal
        // leaves the target untouched.
        let bundle_consensus = self.pack.join("consensus_data");
        let bundle_records = self.pack.join("epoch_records");
        let bundle_certs = self.pack.join("epoch_certs");
        let records =
            reject_non_resumable_bundle(&bundle_consensus, &bundle_records, &bundle_certs)?;

        // The genesis committee is the trust root for record verification; load it exactly as the
        // node does (its yaml was materialized by the `Config::load_*` step above). This is loaded
        // here, not inside the import closure, because the pre-import chain walk below needs it —
        // and a datadir missing its committee should fail before the import, not after it.
        let genesis_committee =
            Config::load_from_path::<Committee>(datadir.committee_path(), ConfigFmt::YAML)
                .map_err(|e| {
                    eyre!(
                        "failed to load genesis committee from {} (needed to verify epoch \
                         records): {e}",
                        datadir.committee_path().display()
                    )
                })?;

        // Verify the WHOLE certificate chain before any chain data is created. This is the
        // expensive check in trust terms and the cheap one in wall-clock: one super-quorum BLS
        // verification per epoch record, no database and no writes, against a multi-GB
        // `restore_pack` below. A bundle with a forged certificate, a broken parent link, or a
        // committee that does not anchor to the local genesis is refused in well under a second
        // with no chain data written, instead of after an import that can run for hours has already
        // committed. (The `Config::load_*` step above may have materialized the genesis committee
        // yaml into the datadir; that is config, not chain data, and it is what this check reads.)
        let certs = read_bundle_certs(&bundle_certs)?;
        let last_epoch_record =
            verify_record_chain_in_memory(&genesis_committee.bls_keys(), &records, &certs)?;

        // Bind the exec-state pack to the now certificate-verified tip record, still before any
        // datadir mutation (the reth DB is created inside `restore_pack`). A mismatch — e.g.
        // `state_data` and `epoch_records` copied from different exports — is refused while the
        // target is still untouched, instead of after a multi-GB import has already committed.
        check_state_pack_matches_record(&self.pack, last_epoch_record)?;

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

        // Everything below creates chain data under the datadir (the reth db + its static files,
        // then the consensus db). `db load-state` targets a FRESH node — `SnapshotRestorer::open`
        // refuses a datadir that already holds reth chain data — so any chain-data dir this import
        // creates is ours to remove if it fails. Snapshot which already exist first, so a failure
        // removes only what we created, never a real node's pre-existing data, and never the
        // operator's keys/config/genesis (which live in sibling subdirectories).
        let preexisting: HashSet<PathBuf> =
            chain_data_dirs(&datadir).into_iter().filter(|dir| dir.exists()).collect();

        let outcome = (|| -> eyre::Result<(BlockNumHash, B256)> {
            let (block, root) = restore_pack(&reth_config, datadir.reth_db_path(), &self.pack)?;

            // Rebuild the consensus + epoch-records packs from the bundle's data-only files. A
            // plain copy would not work: these files have no index sidecars and the
            // packs do not rebuild indexes on open, so we reconstruct fully-indexed,
            // queryable packs at their datadir homes under `consensus-db/epochs/`,
            // verifying each epoch record against its certificate as we go. The bundle
            // was classified complete-and-resumable above, and its record chain was already
            // verified in memory; re-verifying here is what keeps the guarantee attached to the
            // code that persists the records, at a cost that is noise next to the import.
            restore_consensus_and_records(
                &datadir.epochs_db_path(),
                &genesis_committee,
                &bundle_consensus,
                &bundle_records,
                &bundle_certs,
            )?;
            Ok((block, root))
        })();

        let (block, root) = match outcome {
            Ok(ok) => ok,
            Err(e) => {
                // Surgically remove only the chain-data dirs this import created; the operator's
                // keys, node info, and genesis config are left intact so a failed import never
                // costs them irreplaceable material.
                clean_created_chain_data(&datadir, &preexisting);
                return Err(e.wrap_err(format!(
                    "state import failed; removed the chain data it created under {} (node keys \
                     and config left intact) — fix the export bundle and re-run `db load-state`",
                    datadir.display()
                )));
            }
        };

        println!(
            "restored execution state at block {} (state root {root:#x}) into {}",
            block.number,
            datadir.reth_db_path().display()
        );
        Ok(())
    }
}

/// The chain-data directories a state import creates under `datadir`: the reth db, its static
/// files, and the consensus db (which holds `epochs/`). These — and only these — are removed when
/// an import fails. The operator's irreplaceable material (`node-keys/`, `node-info.yaml`,
/// `genesis/`) lives in sibling paths and is deliberately excluded, so a failed import never
/// destroys keys the way a blanket `rm -rf <datadir>` would. Mirrors `TelcoinDirs::reth_db_path` /
/// `consensus_db_path` (plus reth's sibling `static_files` dir, which has no accessor).
fn chain_data_dirs(datadir: &Path) -> [PathBuf; 3] {
    [datadir.join("db"), datadir.join("static_files"), datadir.join("consensus-db")]
}

/// Best-effort removal of the chain data a failed import created — every [`chain_data_dirs`]
/// entry not already present in `preexisting`, then the restored-state floor marker
/// (`import_chain_scaffold` writes it before any chain data commits, so a failed import can
/// leave it orphaned; a stale floor would poison a later normal sync of this datadir by
/// refusing pinned reads it can actually serve). The marker goes last: removing it before the
/// dirs could leave restored chain data without its floor if this cleanup itself dies midway.
/// Skipping pre-existing dirs is a safety guard: even if `db load-state` were pointed at a
/// populated datadir, this never deletes data the import did not create. Removal errors are
/// logged, not fatal (the import is already failing).
fn clean_created_chain_data(datadir: &Path, preexisting: &HashSet<PathBuf>) {
    for dir in chain_data_dirs(datadir) {
        if preexisting.contains(&dir) {
            continue;
        }
        match fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                eprintln!("warning: could not remove {} during import cleanup: {e}", dir.display());
            }
        }
    }
    let marker = tn_reth::RethEnv::restored_state_floor_marker(datadir);
    fs::remove_file(&marker)
        .or_else(|e| (e.kind() == std::io::ErrorKind::NotFound).then_some(()).ok_or(e))
        .unwrap_or_else(|e| {
            eprintln!("warning: could not remove {} during import cleanup: {e}", marker.display())
        });
}

/// Reject an export bundle that cannot produce a resumable node, returning `Err` before any datadir
/// mutation so a refusal leaves the target untouched.
///
/// A resumable node needs both the consensus side of the bundle (`consensus_data` + `epoch_records`
/// + `epoch_certs`, present together) AND a closed epoch `>= 1`:
///
/// - A state-only pack (none of the three present) carries no consensus, so a node loaded from it
///   would have execution state past genesis with no consensus output that produced it and halt.
/// - A partial bundle (some but not all three present) is a version/corruption mismatch.
/// - An epoch-0 bundle cannot have its consensus pack rebuilt — that needs a pre-epoch-0 genesis
///   descriptor the data-only bundle does not carry — so it too would not resume.
///
/// The epoch check is a lightweight structural read of the records (no index sidecar needed) and
/// says nothing about their cryptography; the returned records are handed to
/// [`verify_record_chain_in_memory`], which verifies the chain — still before any datadir mutation.
/// `restore_consensus_and_records` re-reads and re-verifies them while persisting.
fn reject_non_resumable_bundle(
    bundle_consensus: &Path,
    bundle_records: &Path,
    bundle_certs: &Path,
) -> eyre::Result<Vec<EpochRecord>> {
    match (bundle_consensus.exists(), bundle_records.exists(), bundle_certs.exists()) {
        (true, true, true) => {
            let records = EpochRecordDb::read_records_from_pack(bundle_records).map_err(|e| {
                eyre!("failed to read epoch records from {}: {e}", bundle_records.display())
            })?;
            let last_epoch = records
                .last()
                .ok_or_else(|| eyre!("bundle epoch_records contains no records"))?
                .epoch;
            if last_epoch == 0 {
                bail!(
                    "epoch-0 bundle cannot produce a resumable node (rebuilding the epoch-0 \
                     consensus pack requires a pre-epoch-0 genesis descriptor the bundle does not \
                     carry); bootstrap from a later epoch's complete bundle instead"
                );
            }
            Ok(records)
        }
        (false, false, false) => bail!(
            "state-only pack cannot produce a resumable node; re-export a complete bundle with \
             consensus_data, epoch_records, and epoch_certs"
        ),
        _ => bail!(
            "incomplete export bundle: expected consensus_data, epoch_records, and epoch_certs \
             together (re-export with the current version)"
        ),
    }
}

/// Read an export bundle's bare `epoch_certs` pack and index it by the record digest each
/// certificate covers (`cert.epoch_hash == record.digest()`), which is how records and certificates
/// are paired everywhere below.
fn read_bundle_certs(bundle_certs: &Path) -> eyre::Result<HashMap<EpochDigest, EpochCertificate>> {
    let certs = EpochRecordDb::read_certs_from_pack(bundle_certs).map_err(|e| {
        eyre!("failed to read epoch certificates from {}: {e}", bundle_certs.display())
    })?;
    Ok(certs.into_iter().map(|cert| (cert.epoch_hash, cert)).collect())
}

/// Reject a bundle whose records are not a contiguous run starting at epoch 0.
///
/// `save_record` appends in order from epoch 0, so a gap cannot be rebuilt at all. Checking it up
/// front turns a mid-loop `EpochOutOfOrder` into a message that names the offending position.
fn reject_non_contiguous_records(records: &[EpochRecord]) -> eyre::Result<()> {
    records.iter().enumerate().try_for_each(|(position, record)| {
        let slot = Epoch::try_from(position).map_err(|_| {
            eyre!("bundle holds more epoch records than an epoch number can address")
        })?;
        (record.epoch == slot).then_some(()).ok_or_else(|| {
            eyre!(
                "bundle epoch records are not contiguous from epoch 0 (position {position} is \
                 epoch {})",
                record.epoch
            )
        })
    })
}

/// Verify the bundle's entire epoch-record chain against the local genesis committee, in memory,
/// returning the verified tip record.
///
/// This is the pre-import trust gate. It opens no database and writes nothing, so it can run before
/// any chain data exists: a bundle carrying a forged certificate, a broken parent link, a committee
/// that does not anchor to the local genesis, or a missing certificate is refused before
/// [`chain_data_dirs`] hold anything, rather than after `restore_pack` has committed a multi-GB
/// import. The work is one super-quorum BLS verification per epoch record.
///
/// [`EpochRecordDb::validate_downloaded_record`] cannot be used here: it anchors epoch `k` to
/// record `k - 1` *as already stored in a database*, which is exactly the interleaving of verifying
/// and saving that forces verification to wait for the import. So the walk carries the previous
/// record as the accumulator instead and calls the shared [`validate_record_against_anchor`]
/// predicate — the same four checks the database path applies, so the two cannot drift apart.
///
/// Each record is anchored at its *position* in the bundle rather than its self-declared
/// `record.epoch`, so a record for the wrong slot cannot satisfy the walk.
fn verify_record_chain_in_memory<'records>(
    genesis_keys: &BTreeSet<BlsPublicKey>,
    records: &'records [EpochRecord],
    cert_by_hash: &HashMap<EpochDigest, EpochCertificate>,
) -> eyre::Result<&'records EpochRecord> {
    reject_non_contiguous_records(records)?;

    records
        .iter()
        .enumerate()
        .try_fold(None::<&'records EpochRecord>, |previous, (position, record)| {
            let slot = Epoch::try_from(position).map_err(|_| {
                eyre!("bundle holds more epoch records than an epoch number can address")
            })?;
            // Epoch 0 anchors to the local genesis committee with the null parent digest (mirroring
            // the dummy record the node seeds); every later epoch anchors to the record the walk
            // just verified.
            let (parent_hash, committee): (EpochDigest, BTreeSet<BlsPublicKey>) = previous
                .map_or_else(
                    || (EpochDigest::default(), genesis_keys.clone()),
                    |prev| (prev.digest(), prev.next_committee.iter().copied().collect()),
                );
            let cert = cert_by_hash.get(&record.digest()).ok_or_else(|| {
                eyre!(
                    "epoch {} record has no certificate in the bundle; a complete bundle must \
                     carry a cert for every epoch through N — re-export with the current version",
                    record.epoch
                )
            })?;

            match validate_record_against_anchor(slot, record, cert, parent_hash, &committee) {
                EpochRecordValidation::Valid => Ok(Some(record)),
                EpochRecordValidation::Invalid {
                    epoch_matches,
                    parents_match,
                    committee_valid,
                    cert_valid,
                } => Err(eyre!(
                    "epoch {slot} record failed certificate verification (slot: {epoch_matches}, \
                     parent link: {parents_match}, committee anchored to local genesis: \
                     {committee_valid}, super-quorum certificate: {cert_valid}); the bundle's \
                     record chain does not verify against this node's genesis committee — check \
                     that --chain matches the network the bundle came from, then re-export"
                )),
                // Unreachable: the anchor is supplied by this walk, never looked up.
                EpochRecordValidation::NoAnchor => Err(eyre!(
                    "epoch {slot} record could not be anchored while verifying the bundle's \
                     record chain"
                )),
            }
        })?
        .ok_or_else(|| eyre!("bundle epoch_records contains no records"))
}

/// Bind the exec-state pack to the certificate-verified tip record's `final_state`, returning `Err`
/// on any mismatch. Opening the pack reads only its meta + headers (cheap), so this runs as a
/// pre-flight check before any datadir mutation: a mismatch means the pack's `state_data` and
/// `epoch_records` came from different exports, and refusing here leaves the target untouched.
///
/// `record` has already been verified against its certificate by
/// [`verify_record_chain_in_memory`], so pinning the exec pack to it here makes the super-quorum
/// certificate over the record transitively cover the exec pack's identity. That coverage reaches
/// the pack's ancestor headers too, not just its tip: `SnapshotRestorer::import_chain_scaffold`
/// walks the parent-hash links across the whole window, and a header hash commits to every field
/// of its header, so the certified tip hash pins each ancestor in turn.
fn check_state_pack_matches_record(pack: &Path, record: &EpochRecord) -> eyre::Result<()> {
    let reader = ExecStatePackReader::open(pack)
        .map_err(|e| eyre!("failed to open state pack {}: {e}", pack.display()))?;
    let state_final = BlockNumHash::new(reader.meta().block_number, reader.meta().block_hash);
    if state_final != record.final_state {
        bail!(
            "state pack final state {}:{:#x} does not match certified epoch-{} record final_state \
             {}:{:#x}; the pack's state_data and epoch_records appear to come from different \
             exports — re-export a single complete bundle",
            state_final.number,
            state_final.hash,
            record.epoch,
            record.final_state.number,
            record.final_state.hash,
        );
    }
    Ok(())
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
    // Reject a bundle the node could not resume from BEFORE declaring the import complete. The
    // restored node seeds its first epoch entry from ONE pinned read at the snapshot's final block,
    // so that block must be the one that closed an epoch and its `WorkerConfigs` rows must all read
    // back as fees. Fail here with a clear, block- and worker-naming message instead of letting
    // that surface as a cryptic runtime crash at the node's first epoch entry.
    restorer
        .entry_readiness_precondition(final_state)
        .map_err(|e| eyre!("{e}; bootstrap from a later epoch's bundle instead"))?;
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
///
/// A complete bundle carries a certificate for every record through the tip epoch N (the exporter
/// waits for N's cert before writing the bundle), so every epoch — including the epoch-0 genesis
/// anchor — is fully verified against its cert. Epoch 0 is additionally checked for committee
/// compatibility with the seeded genesis committee, binding the bundle to the local chain.
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
    // Certificates are matched to records by digest (`cert.epoch_hash == record.digest()`). A
    // complete bundle carries one for every record through the tip epoch N, including the epoch-0
    // genesis anchor.
    let cert_by_hash = read_bundle_certs(bundle_certs)?;
    reject_non_contiguous_records(&records)?;
    let n = records.last().ok_or_else(|| eyre!("bundle epoch_records contains no records"))?.epoch;

    // Both `save_record` and `stream_import` are async; the exec-state restore already built and
    // dropped its own runtime, so drive the rebuild to completion on a fresh one here.
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_io().enable_time().build()?;
    runtime.block_on(async {
        // 1. Rebuild the indexed epoch-records DB, verifying each record against its certificate as
        //    we go.
        let db = EpochRecordDb::open(epochs_dir)
            .map_err(|e| eyre!("failed to open epoch records db: {e}"))?;
        verify_and_save_epoch_records(&db, genesis_committee.bls_keys(), &records, &cert_by_hash)
            .await?;
        db.persist().await.map_err(|e| eyre!("failed to persist epoch records: {e}"))?;
        db.close().await;

        // 2. Rebuild the closed epoch's consensus pack. Epoch 0 would need a pre-epoch-0 genesis
        //    descriptor that a data-only bundle doesn't carry, so the pack cannot be rebuilt and a
        //    node loaded from it would not resume; every later epoch uses the previous record as
        //    its genesis link. The caller rejects epoch-0 bundles up front (before mutating the
        //    datadir); bail here too so there is no silent-success path if this is reached
        //    directly.
        if n == 0 {
            bail!(
                "epoch-0 bundle cannot produce a resumable node (rebuilding the epoch-0 consensus \
                 pack requires a pre-epoch-0 genesis descriptor not present in the bundle); \
                 bootstrap from a later epoch's complete bundle instead"
            );
        }
        let previous = &records[(n - 1) as usize];
        let final_record = &records[n as usize];
        let epoch_dir = epochs_dir.join(format!("epoch-{n}"));
        let file = tokio::fs::File::open(bundle_consensus).await.map_err(|e| {
            eyre!("failed to open consensus data {}: {e}", bundle_consensus.display())
        })?;
        // Landing directly at `epochs_dir` (not a temp) is safe here: this is an offline, single
        // writer restore into a fresh datadir, so the online rename/install-lock dance is unneeded.
        // No free-space floor: the floor guards the node's live stores against peer-supplied
        // bytes, and this is the operator's own bundle into a datadir nothing else is using; a
        // real shortage surfaces as the write error.
        let pack = ConsensusPack::stream_import_with_floor(
            epochs_dir,
            file,
            n,
            previous,
            final_record.final_consensus.number,
            STREAM_IMPORT_TIMEOUT,
            0,
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
        pack.close().await;
        // Read-back failure and tip mismatch are different diagnoses and get different messages: a
        // mismatch means the bundle rebuilt into the wrong chain, whereas an `Err` means the pack
        // could not be read at all. Both roll the epoch dir back, matching the import error path
        // above, since this restore wrote that directory itself and owns the cleanup.
        let tip = tip.map_err(|e| {
            let _ = fs::remove_dir_all(&epoch_dir);
            eyre!("failed to read back the rebuilt consensus pack tip for epoch {n}: {e}")
        })?;
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

        // Point the consensus "latest" slot hint at this epoch's final consensus so a node started
        // on this datadir resumes syncing from here instead of from genesis.
        ConsensusChain::write_latest_consensus_hint(
            epochs_dir,
            n,
            final_record.final_consensus.number,
        )
        .map_err(|e| eyre!("failed to write consensus slot hint: {e}"))?;
        Ok(())
    })?;

    println!(
        "restored and verified epoch records 0..={n} and consensus pack for epoch {n} into {}; \
         a node started here will resume syncing from epoch {n}",
        epochs_dir.display()
    );
    Ok(())
}

/// Verify each bundle record against its certificate and save it into `db`, rebuilding the indexed
/// epoch-records DB from the trusted genesis committee forward.
///
/// A dummy epoch-0 record carrying `genesis_keys` is seeded first (mirroring the node's genesis
/// bootstrap): it is the trusted anchor for verifying epoch 0 and is only consulted while the real
/// record index is still empty. Records are then verified BEFORE being saved so
/// `validate_downloaded_record` anchors epoch 0 to the seeded committee and epoch k to the
/// already-saved record k-1.
///
/// Every epoch — including epoch 0 — must carry a certificate in `cert_by_hash`; a missing one is a
/// hard error, because a complete bundle exports a cert for every record through the tip epoch N
/// and a record cannot be trusted without one. Epoch 0 is verified against the seeded genesis
/// committee (its cert plus committee compatibility with `genesis_keys`); every later epoch k is
/// verified against the already-saved record k-1.
async fn verify_and_save_epoch_records(
    db: &EpochRecordDb,
    genesis_keys: std::collections::BTreeSet<BlsPublicKey>,
    records: &[EpochRecord],
    cert_by_hash: &HashMap<EpochDigest, EpochCertificate>,
) -> eyre::Result<()> {
    // Seed the dummy epoch-0 anchor with the genesis committee.
    let genesis_keys_vec: Vec<_> = genesis_keys.iter().copied().collect();
    db.save_dummy_epoch0(EpochRecord {
        epoch: 0,
        committee: genesis_keys_vec.clone(),
        next_committee: genesis_keys_vec,
        ..Default::default()
    })
    .await
    .map_err(|e| eyre!("failed to seed genesis committee: {e}"))?;

    for record in records.iter() {
        // Every epoch must carry a certificate: a complete bundle exports one for every record
        // through the tip epoch N, so a missing cert means the bundle is incomplete or stale. Fully
        // verify each record against its cert, anchored to the trusted chain (the sequential loop
        // saved k-1 before verifying k, so once N's cert is present it verifies against N-1).
        let Some(cert) = cert_by_hash.get(&record.digest()) else {
            bail!(
                "epoch {} record has no certificate in the bundle; a complete bundle must carry a \
                 cert for every epoch through N — re-export with the current version",
                record.epoch
            );
        };

        // A single path validates every epoch: epoch 0 anchors to the seeded dummy (the local
        // genesis committee), every later epoch to the already-saved previous record.
        match db.validate_downloaded_record(record.epoch, record, cert).await {
            EpochRecordValidation::Valid => {}
            other => {
                bail!("epoch {} record failed certificate verification: {other:?}", record.epoch)
            }
        }
        db.save(record.clone(), cert.clone())
            .await
            .map_err(|e| eyre!("failed to save epoch record {}: {e}", record.epoch))?;
    }
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
    use super::{file_len_if_exists, repair_targets, static_files_summary_table};
    use crate::{
        cli::{Cli, Commands},
        NoArgs,
    };
    use std::{collections::HashMap, fs, path::Path};

    #[test]
    fn repair_targets_skips_current_epoch_in_all_mode() {
        // All-mode: every epoch except the current/latest, which is returned as skipped.
        assert_eq!(repair_targets(&[0, 1, 2], None), (vec![0, 1], Some(2)));
        // Single epoch is the current one: skipped, nothing to repair.
        assert_eq!(repair_targets(&[5], None), (vec![], Some(5)));
        // No epochs on disk.
        assert_eq!(repair_targets(&[], None), (vec![], None));
        // Explicit --epoch targets exactly that epoch (including the current one), no skip.
        assert_eq!(repair_targets(&[0, 1, 2], Some(2)), (vec![2], None));
        assert_eq!(repair_targets(&[0, 1, 2], Some(0)), (vec![0], None));
    }

    #[cfg(unix)]
    #[test]
    fn db_repair_refuses_while_a_live_node_holds_the_datadir_lock() {
        use tn_config::{PidLock, TelcoinDirs as _};
        let dir = tempfile::tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        // `db repair` reaches the datadir lock only after the epochs dir exists.
        fs::create_dir_all(datadir.epochs_db_path()).unwrap();

        // A held advisory lock stands in for a running node holding the datadir.
        let held = PidLock::acquire(&datadir).expect("acquire datadir lock");
        let err = super::DbRepairArgs { epoch: None, force: true }
            .execute(datadir.clone())
            .expect_err("repair must refuse while a node holds the lock");
        assert!(err.to_string().contains("another telcoin process"), "unexpected error: {err}");
        // The refusal must not disturb the running node's lockfile.
        assert_eq!(
            fs::read_to_string(datadir.node_pid_path()).unwrap().trim().parse::<u32>().unwrap(),
            std::process::id(),
            "the held lock's PID must be left intact"
        );

        // Once the node releases the lock (drop clears the file), a dry-run repair takes the lock
        // and releases it on exit.
        drop(held);
        super::DbRepairArgs { epoch: None, force: false }
            .execute(datadir.clone())
            .expect("dry-run repair should succeed on an empty epochs dir with no lock held");
        // Released: the lockfile stays (it is never unlinked) but no longer records a holder, and a
        // fresh acquire succeeds.
        assert!(
            fs::read_to_string(datadir.node_pid_path()).unwrap().is_empty(),
            "repair must clear its PID on exit"
        );
        drop(PidLock::acquire(&datadir).expect("repair must release its lock on exit"));
    }

    /// A torn tail is only self-healing in the current epoch; whether a pack is a past epoch is
    /// read from the epochs directory beside it.
    #[test]
    fn later_epoch_exists_reads_the_epochs_directory() {
        let dir = tempfile::tempdir().unwrap();
        for epoch in [3, 4] {
            fs::create_dir_all(dir.path().join(format!("epoch-{epoch}"))).unwrap();
        }
        let data = |epoch: u32| dir.path().join(format!("epoch-{epoch}")).join(super::DATA_NAME);
        assert!(super::later_epoch_exists(&data(3), 3), "epoch 4 follows epoch 3");
        assert!(!super::later_epoch_exists(&data(4), 4), "epoch 4 is the latest");
        let bare = tempfile::tempdir().unwrap();
        assert!(
            !super::later_epoch_exists(&bare.path().join(super::DATA_NAME), 0),
            "a bare data file has no epochs beside it"
        );
    }

    /// Whether a pack is a past epoch comes from the latest-consensus hint the node resumes from,
    /// not the directory listing: a later `epoch-{N}` can exist before the node switches to it (an
    /// epoch handoff interrupted after creating the next epoch's directory, or an epoch imported
    /// ahead while catching up), and the node still opens the hinted epoch for append, healing its
    /// tail.
    #[test]
    fn past_epoch_follows_the_latest_consensus_hint() {
        let dir = tempfile::tempdir().unwrap();
        for epoch in [3, 4] {
            fs::create_dir_all(dir.path().join(format!("epoch-{epoch}"))).unwrap();
        }
        let data = |epoch: u32| dir.path().join(format!("epoch-{epoch}")).join(super::DATA_NAME);
        // The slot format: epoch (u32 LE), number (u64 LE), crc32 (LE) over the two. The other
        // slot is empty, which reads as (0, 0).
        let write_hint = |epoch: u32| {
            let mut slot = Vec::with_capacity(16);
            slot.extend_from_slice(&epoch.to_le_bytes());
            slot.extend_from_slice(&100u64.to_le_bytes());
            let crc = crc32fast::hash(&slot);
            slot.extend_from_slice(&crc.to_le_bytes());
            fs::write(dir.path().join("consensus_slot1"), &slot).unwrap();
            fs::write(dir.path().join("consensus_slot2"), []).unwrap();
        };
        let past = |epoch: u32| super::require_current_epoch_for_tail(&data(epoch), epoch).is_err();

        assert!(past(3), "without a hint the directory listing decides: epoch 4 follows epoch 3");
        write_hint(3);
        assert!(!past(3), "the node resumes epoch 3 for append");
        assert!(!past(4), "epoch 4 is ahead of the node, opened for append when it gets there");
        write_hint(4);
        assert!(past(3), "the node resumes epoch 4, so epoch 3 is past");
    }

    /// `db load-state` writes chain data into the datadir, so like repair and migrate it must
    /// refuse to run while a live node holds the datadir lock, before touching anything.
    #[cfg(unix)]
    #[test]
    fn db_load_state_refuses_while_a_live_node_holds_the_datadir_lock() {
        use tn_config::PidLock;
        let dir = tempfile::tempdir().unwrap();
        let datadir = dir.path().to_path_buf();
        let _held = PidLock::acquire(&datadir).expect("acquire datadir lock");
        let err = super::DbLoadStateArgs { pack: dir.path().join("bundle"), chain: None }
            .execute(datadir)
            .expect_err("load-state must refuse while a node holds the lock");
        assert!(err.to_string().contains("another telcoin process"), "unexpected error: {err}");
    }

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

    /// A pack whose epoch meta is torn with nothing behind it holds no committed data, but both
    /// open doors refuse it and `db repair` reports it unrepairable, so `db validate` must fail
    /// (exit non-zero) rather than report a self-healing shape.
    #[test]
    fn db_validate_fails_on_a_torn_epoch_meta() {
        use tn_storage::{
            archive::pack::{Pack, PackCompression, DATA_HEADER_BYTES},
            consensus_pack::{PackRecord, PACK_VERSION},
        };
        let dir = tempfile::tempdir().unwrap();
        let epoch_dir = dir.path().join("epoch-0");
        fs::create_dir_all(&epoch_dir).unwrap();
        let data_file = epoch_dir.join(super::DATA_NAME);
        // A header-only pack, then an unclean tail: a meta size prefix claiming more bytes than
        // follow it.
        drop(
            Pack::<PackRecord>::open(&data_file, 0, false, PackCompression::ZStd, PACK_VERSION)
                .expect("create pack"),
        );
        let mut bytes = fs::read(&data_file).unwrap();
        bytes.truncate(DATA_HEADER_BYTES);
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0xAB; 10]);
        fs::write(&data_file, &bytes).unwrap();

        let args = super::DbValidateArgs { path: epoch_dir, epoch: None };
        assert!(args.execute().is_err(), "a torn epoch meta must fail validation");
    }

    /// A legacy v0 (batches-first) pack is only ever read by its migration to v2, so `db validate`
    /// reports a dry run of that migration: success for one that migrates, failure (exit non-zero)
    /// for one the migration refuses.
    #[test]
    fn db_validate_judges_a_v0_pack_by_its_migration() {
        use tn_storage::{
            archive::pack::{Pack, PackCompression},
            consensus_pack::{EpochMeta, PackRecord},
        };
        let dir = tempfile::tempdir().unwrap();
        let epoch_dir = dir.path().join("epoch-0");
        fs::create_dir_all(&epoch_dir).unwrap();
        let data_file = epoch_dir.join(super::DATA_NAME);
        let write_v0 = |records: &[PackRecord]| {
            let _ = fs::remove_file(&data_file);
            let mut pack =
                Pack::<PackRecord>::open(&data_file, 0, false, PackCompression::ZStd, 0).unwrap();
            for record in records {
                pack.append(record).unwrap();
            }
            pack.commit().unwrap();
        };
        let validate = || super::DbValidateArgs { path: epoch_dir.clone(), epoch: None }.execute();

        // The meta's committee must decode (a real committee of more than one authority).
        let mut committee = tn_types::CommitteeBuilder::new(0);
        for (i, signer) in test_signers(40, 4).iter().enumerate() {
            committee.add_authority(signer.public_key(), tn_types::Address::repeat_byte(i as u8));
        }
        let meta =
            PackRecord::EpochMeta(EpochMeta { committee: committee.build(), ..Default::default() });
        write_v0(std::slice::from_ref(&meta));
        assert_eq!(super::legacy_migration_dry_run(&data_file, 0), Ok(0));
        assert!(validate().is_ok(), "a v0 pack that migrates cleanly validates");

        // A batch with no consensus header after it belongs to no output: the migration refuses.
        write_v0(&[meta, PackRecord::Batch(tn_types::Batch::default())]);
        assert!(super::legacy_migration_dry_run(&data_file, 0).is_err());
        assert!(validate().is_err(), "a v0 pack the migration refuses fails validation");
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

    /// Write a bare `epoch_records` pack file (an `epochs.pack`, no sidecar index) holding a
    /// contiguous run of records `0..=last_epoch`, mirroring the file an export bundle carries.
    /// Returns the path to that file.
    fn write_records_bundle(dir: &Path, last_epoch: u32) -> std::path::PathBuf {
        use tn_storage::epoch_records::{EpochRecordDb, RECORDS_NAME};
        use tn_types::EpochRecord;
        let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
        rt.block_on(async {
            let db = EpochRecordDb::open(dir).expect("open epoch records db");
            for epoch in 0..=last_epoch {
                db.save_record(EpochRecord { epoch, ..Default::default() })
                    .await
                    .expect("save record");
            }
            db.persist().await.expect("persist records");
        });
        dir.join(RECORDS_NAME)
    }

    #[test]
    fn reject_state_only_bundle() {
        // A state-only pack carries none of the three consensus files, so it cannot resume.
        let dir = tempfile::tempdir().unwrap();
        let err = super::reject_non_resumable_bundle(
            &dir.path().join("consensus_data"),
            &dir.path().join("epoch_records"),
            &dir.path().join("epoch_certs"),
        )
        .expect_err("state-only pack must be refused");
        assert!(
            err.to_string().contains("state-only pack cannot produce a resumable node"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_partial_bundle() {
        // Some-but-not-all of the three consensus files present is a version/corruption mismatch.
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("consensus_data"), b"x").unwrap();
        fs::write(dir.path().join("epoch_records"), b"x").unwrap();
        // epoch_certs intentionally absent
        let err = super::reject_non_resumable_bundle(
            &dir.path().join("consensus_data"),
            &dir.path().join("epoch_records"),
            &dir.path().join("epoch_certs"),
        )
        .expect_err("partial bundle must be refused");
        assert!(err.to_string().contains("incomplete export bundle"), "unexpected error: {err}");
    }

    #[test]
    fn reject_epoch_0_bundle() {
        // A complete bundle whose latest record is epoch 0 cannot rebuild its consensus pack, so it
        // is refused even though all three files are present.
        let bundle = tempfile::tempdir().unwrap();
        let records_src = tempfile::tempdir().unwrap();
        let records_file = write_records_bundle(records_src.path(), 0);
        fs::copy(&records_file, bundle.path().join("epoch_records")).unwrap();
        fs::write(bundle.path().join("consensus_data"), b"x").unwrap();
        fs::write(bundle.path().join("epoch_certs"), b"x").unwrap();

        let err = super::reject_non_resumable_bundle(
            &bundle.path().join("consensus_data"),
            &bundle.path().join("epoch_records"),
            &bundle.path().join("epoch_certs"),
        )
        .expect_err("epoch-0 bundle must be refused");
        assert!(
            err.to_string().contains("epoch-0 bundle cannot produce a resumable node"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accept_complete_resumable_bundle() {
        // Positive control: a complete bundle whose records end at epoch >= 1 passes the up-front
        // classification, proving the refusal is specific rather than a blanket reject.
        let bundle = tempfile::tempdir().unwrap();
        let records_src = tempfile::tempdir().unwrap();
        let records_file = write_records_bundle(records_src.path(), 2);
        fs::copy(&records_file, bundle.path().join("epoch_records")).unwrap();
        fs::write(bundle.path().join("consensus_data"), b"x").unwrap();
        fs::write(bundle.path().join("epoch_certs"), b"x").unwrap();

        super::reject_non_resumable_bundle(
            &bundle.path().join("consensus_data"),
            &bundle.path().join("epoch_records"),
            &bundle.path().join("epoch_certs"),
        )
        .expect("complete epoch>=1 bundle must be accepted");
    }

    #[test]
    fn reject_state_pack_final_state_mismatch() {
        // The exec-state pack must name the same final state as the certified tip record. A pack
        // whose meta disagrees (e.g. state_data and epoch_records copied from different exports) is
        // refused before any datadir write.
        use tn_storage::exec_state_pack::ExecStatePackWriter;
        use tn_types::{BlockNumHash, EpochRecord, ExecHeader, B256};
        let dir = tempfile::tempdir().unwrap();
        let root = B256::from([7u8; 32]);
        let snapshot = ExecHeader { number: 1, state_root: root, ..Default::default() };
        ExecStatePackWriter::create(dir.path(), root, std::slice::from_ref(&snapshot))
            .expect("create pack")
            .finish()
            .expect("finish pack");
        // Record names a different block number than the pack's meta (block 1), so it cannot match
        // regardless of hash.
        let record = EpochRecord {
            epoch: 3,
            final_state: BlockNumHash::new(6, B256::from([1u8; 32])),
            ..Default::default()
        };
        let err = super::check_state_pack_matches_record(dir.path(), &record)
            .expect_err("mismatched final state must be refused");
        assert!(
            err.to_string().contains("does not match certified epoch-3 record final_state"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn accept_state_pack_final_state_match() {
        // Positive control: when the pack's meta equals the record's final_state the binding check
        // passes, proving the refusal above is specific rather than a blanket reject.
        use tn_storage::exec_state_pack::{ExecStatePackReader, ExecStatePackWriter};
        use tn_types::{BlockNumHash, EpochRecord, ExecHeader, B256};
        let dir = tempfile::tempdir().unwrap();
        let root = B256::from([7u8; 32]);
        let snapshot = ExecHeader { number: 1, state_root: root, ..Default::default() };
        ExecStatePackWriter::create(dir.path(), root, std::slice::from_ref(&snapshot))
            .expect("create pack")
            .finish()
            .expect("finish pack");
        // Read the pack's own meta so the record's final_state matches it exactly.
        let meta_final = {
            let reader = ExecStatePackReader::open(dir.path()).expect("open pack");
            BlockNumHash::new(reader.meta().block_number, reader.meta().block_hash)
        };
        let record = EpochRecord { epoch: 3, final_state: meta_final, ..Default::default() };
        super::check_state_pack_matches_record(dir.path(), &record)
            .expect("matching final state must be accepted");
    }

    #[test]
    fn clean_created_chain_data_removes_only_created_chain_dirs() {
        // A failed import must remove only the chain-data dirs it created — never a pre-existing
        // node's data, and never the operator's irreplaceable keys/config (finding #7).
        use std::{collections::HashSet, path::PathBuf};
        let datadir = tempfile::tempdir().unwrap();
        let d = datadir.path();
        for sub in ["db", "static_files", "consensus-db", "node-keys", "genesis"] {
            fs::create_dir_all(d.join(sub)).unwrap();
        }
        fs::write(d.join("node-info.yaml"), b"identity").unwrap();
        fs::write(d.join("node-keys").join("bls.key"), b"secret").unwrap();

        // `db` was present before the import (simulating a real node's data), so it must survive;
        // the import created `static_files` and `consensus-db`.
        let preexisting: HashSet<PathBuf> = [d.join("db")].into_iter().collect();
        super::clean_created_chain_data(d, &preexisting);

        // Chain-data dirs the import created are removed:
        assert!(!d.join("static_files").exists(), "created static_files must be removed");
        assert!(!d.join("consensus-db").exists(), "created consensus-db must be removed");
        // Pre-existing data is never removed:
        assert!(d.join("db").exists(), "pre-existing db must be preserved");
        // Keys and config are never touched:
        assert!(d.join("node-keys").join("bls.key").exists(), "node keys must be preserved");
        assert!(d.join("node-info.yaml").exists(), "node-info.yaml must be preserved");
        assert!(d.join("genesis").exists(), "genesis dir must be preserved");
    }

    // --- import verification (`verify_and_save_epoch_records`) ---
    //
    // These drive the per-record verify/save loop directly (skipping the consensus-pack stream
    // import, which needs a real `consensus_data` file), exercising the change that a tip epoch >=
    // 1 without a cert is now rejected rather than stored unverified.

    use rand::{rngs::StdRng, SeedableRng as _};
    use roaring::RoaringBitmap;
    use tempfile::TempDir;
    use tn_storage::epoch_records::EpochRecordDb;
    use tn_types::{
        BlsAggregateSignature, BlsKeypair, BlsPublicKey, BlsSignature, BlsSigner, EpochCertificate,
        EpochDigest, EpochRecord, Signer as _,
    };

    /// Minimal [`BlsSigner`] wrapper around a keypair for building test certs.
    #[derive(Clone)]
    struct TestSigner(std::sync::Arc<BlsKeypair>);

    impl BlsSigner for TestSigner {
        fn request_signature_direct(&self, msg: &[u8]) -> BlsSignature {
            self.0.sign(msg)
        }
        fn public_key(&self) -> BlsPublicKey {
            *self.0.public()
        }
    }

    /// Build a record for `epoch` (committee = `next_committee` = the signers' keys) and a
    /// super-quorum certificate over it, chaining to `parent_hash`.
    fn signed_pair(
        epoch: u32,
        signers: &[TestSigner],
        parent_hash: EpochDigest,
    ) -> (EpochRecord, EpochCertificate) {
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        let record = EpochRecord {
            epoch,
            committee: committee.clone(),
            next_committee: committee,
            parent_hash,
            ..Default::default()
        };
        let sigs: Vec<BlsSignature> =
            signers.iter().map(|s| record.sign_vote(s).signature).collect();
        let signature =
            BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate").to_signature();
        let mut signed_authorities = RoaringBitmap::new();
        for i in 0..signers.len() as u32 {
            signed_authorities.push(i);
        }
        let cert = EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities };
        (record, cert)
    }

    /// A complete bundle (a cert for every epoch through the tip) is verified and saved end to end.
    #[tokio::test]
    async fn verify_and_save_accepts_complete_bundle() {
        let dir = TempDir::with_prefix("verify_complete").expect("temp dir");
        let mut rng = StdRng::seed_from_u64(1);
        let signers: Vec<TestSigner> = (0..4)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect();
        let genesis_keys: std::collections::BTreeSet<BlsPublicKey> =
            signers.iter().map(|s| s.public_key()).collect();

        // Records 0..=2, each with a cert, chained by parent hash.
        let mut records = Vec::new();
        let mut cert_by_hash = HashMap::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..=2u32 {
            let (record, cert) = signed_pair(epoch, &signers, parent);
            parent = record.digest();
            cert_by_hash.insert(cert.epoch_hash, cert);
            records.push(record);
        }

        let db = EpochRecordDb::open(dir.path()).expect("open db");
        super::verify_and_save_epoch_records(&db, genesis_keys, &records, &cert_by_hash)
            .await
            .expect("a complete bundle must verify and save");

        // Every record is stored with its cert.
        for record in &records {
            let cert = db.cert_by_digest(record.digest()).await;
            assert!(cert.is_some(), "epoch {} should have a stored cert", record.epoch);
        }
    }

    /// A tip epoch >= 1 whose cert is absent is now rejected (previously it was stored unverified).
    #[tokio::test]
    async fn verify_and_save_rejects_tip_without_cert() {
        let dir = TempDir::with_prefix("verify_missing_tip").expect("temp dir");
        let mut rng = StdRng::seed_from_u64(2);
        let signers: Vec<TestSigner> = (0..4)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect();
        let genesis_keys: std::collections::BTreeSet<BlsPublicKey> =
            signers.iter().map(|s| s.public_key()).collect();

        // Records 0..=2, but drop the tip (epoch 2) cert to model a stale/incomplete bundle.
        let mut records = Vec::new();
        let mut cert_by_hash = HashMap::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..=2u32 {
            let (record, cert) = signed_pair(epoch, &signers, parent);
            parent = record.digest();
            if epoch != 2 {
                cert_by_hash.insert(cert.epoch_hash, cert);
            }
            records.push(record);
        }

        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let err = super::verify_and_save_epoch_records(&db, genesis_keys, &records, &cert_by_hash)
            .await
            .expect_err("a tip epoch without a cert must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 2 record has no certificate in the bundle"),
            "unexpected error: {msg}"
        );
    }

    /// Epoch 0 must carry a cert like every other epoch: a bundle whose epoch-0 record has no cert
    /// is rejected, even when every later epoch carries one.
    #[tokio::test]
    async fn verify_and_save_rejects_genesis_without_cert() {
        let dir = TempDir::with_prefix("verify_genesis_no_cert").expect("temp dir");
        let mut rng = StdRng::seed_from_u64(3);
        let signers: Vec<TestSigner> = (0..4)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect();
        let genesis_keys: std::collections::BTreeSet<BlsPublicKey> =
            signers.iter().map(|s| s.public_key()).collect();

        // Epoch 0 without a cert; epochs 1 and 2 with certs.
        let mut records = Vec::new();
        let mut cert_by_hash = HashMap::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..=2u32 {
            let (record, cert) = signed_pair(epoch, &signers, parent);
            parent = record.digest();
            if epoch != 0 {
                cert_by_hash.insert(cert.epoch_hash, cert);
            }
            records.push(record);
        }

        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let err = super::verify_and_save_epoch_records(&db, genesis_keys, &records, &cert_by_hash)
            .await
            .expect_err("epoch 0 without a cert must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 0 record has no certificate in the bundle"),
            "unexpected error: {msg}"
        );
    }

    /// A bundle whose records chain from a DIFFERENT genesis committee than the local one (the
    /// trust root loaded from `--chain`) is rejected: a bundle from the wrong chain cannot be
    /// imported. Epoch 0 carries a cert (real bundles do — it is aggregated at epoch 1's
    /// start), so it goes through the with-cert path and `validate_downloaded_record` finds its
    /// committee incompatible with the seeded local genesis committee.
    #[tokio::test]
    async fn verify_and_save_rejects_wrong_genesis_committee() {
        let dir = TempDir::with_prefix("verify_wrong_genesis").expect("temp dir");
        let mut rng = StdRng::seed_from_u64(4);
        // The bundle's records are chained from committee A...
        let signers_a: Vec<TestSigner> = (0..4)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect();
        // ...but the local genesis committee (the trust root) is a different set B.
        let signers_b: Vec<TestSigner> = (0..4)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect();
        let genesis_keys_b: std::collections::BTreeSet<BlsPublicKey> =
            signers_b.iter().map(|s| s.public_key()).collect();

        // Records 0..=2 chained from committee A, each with a cert.
        let mut records = Vec::new();
        let mut cert_by_hash = HashMap::new();
        let mut parent = EpochDigest::default();
        for epoch in 0..=2u32 {
            let (record, cert) = signed_pair(epoch, &signers_a, parent);
            parent = record.digest();
            cert_by_hash.insert(cert.epoch_hash, cert);
            records.push(record);
        }

        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let err =
            super::verify_and_save_epoch_records(&db, genesis_keys_b, &records, &cert_by_hash)
                .await
                .expect_err("a bundle from a different genesis committee must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 0 record failed certificate verification"),
            "unexpected error: {msg}"
        );
    }

    // --- pre-import chain verification (`verify_record_chain_in_memory`) ---
    //
    // These pin the ordering: the whole certificate chain is walked before the datadir is touched,
    // so a bad bundle is refused without first paying for the multi-GB state import. The walk takes
    // no path and opens no database, which is what lets it run that early.

    /// `count` deterministic test signers derived from `seed`.
    fn test_signers(seed: u64, count: usize) -> Vec<TestSigner> {
        let mut rng = StdRng::seed_from_u64(seed);
        (0..count)
            .map(|_| TestSigner(std::sync::Arc::new(BlsKeypair::generate(&mut rng))))
            .collect()
    }

    /// The BLS keys of `signers`, in the shape a genesis committee reaches the walk.
    fn keys_of(signers: &[TestSigner]) -> std::collections::BTreeSet<BlsPublicKey> {
        signers.iter().map(|s| s.public_key()).collect()
    }

    /// A super-quorum certificate over `record`'s digest produced by `signers` — which need not be
    /// the record's own committee, so this doubles as the forged-certificate builder.
    fn cert_over(record: &EpochRecord, signers: &[TestSigner]) -> EpochCertificate {
        let sigs: Vec<BlsSignature> =
            signers.iter().map(|s| record.sign_vote(s).signature).collect();
        let signature =
            BlsAggregateSignature::aggregate(&sigs, true).expect("aggregate").to_signature();
        let signed_authorities: RoaringBitmap =
            (0..signers.len()).filter_map(|i| u32::try_from(i).ok()).collect();
        EpochCertificate { epoch_hash: record.digest(), signature, signed_authorities }
    }

    /// [`signed_pair`] with an explicit `final_state` — the field that binds a bundle's tip record
    /// to its exec-state pack.
    fn signed_pair_with_final_state(
        epoch: u32,
        signers: &[TestSigner],
        parent_hash: EpochDigest,
        final_state: tn_types::BlockNumHash,
    ) -> (EpochRecord, EpochCertificate) {
        let committee: Vec<BlsPublicKey> = signers.iter().map(|s| s.public_key()).collect();
        let record = EpochRecord {
            epoch,
            committee: committee.clone(),
            next_committee: committee,
            parent_hash,
            final_state,
            ..Default::default()
        };
        let cert = cert_over(&record, signers);
        (record, cert)
    }

    /// A correctly chained, fully certified run of records `0..=last_epoch` signed by `signers`,
    /// with the digest-keyed certificate index the walk consumes. Only the tip carries
    /// `tip_final_state`; the earlier records keep the default.
    fn signed_chain(
        last_epoch: u32,
        signers: &[TestSigner],
        tip_final_state: tn_types::BlockNumHash,
    ) -> (Vec<EpochRecord>, HashMap<EpochDigest, EpochCertificate>) {
        let (records, certs, _) = (0..=last_epoch).fold(
            (Vec::new(), HashMap::new(), EpochDigest::default()),
            |(mut records, mut certs, parent), epoch| {
                let final_state = if epoch == last_epoch {
                    tip_final_state
                } else {
                    tn_types::BlockNumHash::default()
                };
                let (record, cert) =
                    signed_pair_with_final_state(epoch, signers, parent, final_state);
                let next_parent = record.digest();
                certs.insert(cert.epoch_hash, cert);
                records.push(record);
                (records, certs, next_parent)
            },
        );
        (records, certs)
    }

    /// A chain whose committee genuinely rotates: record k is signed by, and declares, committee k,
    /// and hands committee k+1 forward as its `next_committee` (the tip hands its own forward).
    ///
    /// Every other fixture here sets `committee` and `next_committee` to the same value, which
    /// makes the epoch-to-epoch anchor unobservable: a walk that threaded a record's own
    /// committee forward instead of the one it handed on would satisfy all of them. This is the
    /// fixture that tells those two apart.
    fn rotating_chain(
        committees: &[Vec<TestSigner>],
    ) -> (Vec<EpochRecord>, HashMap<EpochDigest, EpochCertificate>) {
        let (records, certs, _) = committees.iter().enumerate().fold(
            (Vec::new(), HashMap::new(), EpochDigest::default()),
            |(mut records, mut certs, parent), (position, signers)| {
                let successors = committees.get(position + 1).unwrap_or(signers);
                let record = EpochRecord {
                    epoch: u32::try_from(position).expect("epoch fits in u32"),
                    committee: signers.iter().map(|s| s.public_key()).collect(),
                    next_committee: successors.iter().map(|s| s.public_key()).collect(),
                    parent_hash: parent,
                    ..Default::default()
                };
                let cert = cert_over(&record, signers);
                let next_parent = record.digest();
                certs.insert(cert.epoch_hash, cert);
                records.push(record);
                (records, certs, next_parent)
            },
        );
        (records, certs)
    }

    /// Write an export bundle's `epoch_records` + `epoch_certs` packs into `dir` through the
    /// records DB — the same writer an export uses — so the files under test have the real
    /// on-disk shape.
    fn write_signed_bundle(
        dir: &Path,
        records: &[EpochRecord],
        certs: &HashMap<EpochDigest, EpochCertificate>,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime");
        let db = rt.block_on(async { EpochRecordDb::open(dir).expect("open epoch records db") });
        // One `block_on` per record: the pack appends in epoch order, so these saves must land
        // sequentially rather than being driven concurrently.
        records.iter().for_each(|record| {
            let cert = certs.get(&record.digest()).expect("cert for record").clone();
            rt.block_on(db.save(record.clone(), cert)).expect("save record and cert");
        });
        rt.block_on(db.persist()).expect("persist bundle");
    }

    /// Positive control: a complete, correctly chained bundle verifies and yields its tip record,
    /// proving the refusals below are specific rather than a blanket reject.
    #[test]
    fn verify_chain_accepts_complete_bundle() {
        let signers = test_signers(11, 4);
        let (records, certs) = signed_chain(2, &signers, tn_types::BlockNumHash::default());

        let tip = super::verify_record_chain_in_memory(&keys_of(&signers), &records, &certs)
            .expect("a complete, correctly chained bundle must verify");
        assert_eq!(tip.epoch, 2, "the verified tip must be the bundle's last record");
    }

    /// A forged tip certificate — a super-quorum from a committee that is not the record's — is
    /// refused. Under the old ordering this was caught only after the state import had committed.
    #[test]
    fn verify_chain_rejects_forged_tip_certificate() {
        let signers = test_signers(12, 4);
        let impostors = test_signers(13, 4);
        let (records, certs) = signed_chain(2, &signers, tn_types::BlockNumHash::default());

        // Same digest, so this replaces the tip's real certificate in the index.
        let tip = records.last().expect("chain is non-empty");
        let forged = cert_over(tip, &impostors);
        let certs: HashMap<EpochDigest, EpochCertificate> =
            certs.into_iter().chain(std::iter::once((forged.epoch_hash, forged))).collect();

        let err = super::verify_record_chain_in_memory(&keys_of(&signers), &records, &certs)
            .expect_err("a forged tip certificate must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 2 record failed certificate verification"),
            "unexpected: {msg}"
        );
        assert!(msg.contains("super-quorum certificate: false"), "unexpected: {msg}");
    }

    /// A chain whose committee actually rotates verifies: epoch k + 1 is anchored to the committee
    /// epoch k handed forward, not to epoch k's own. Without this the two are the same value in
    /// every fixture, so a walk threading the wrong one would go unnoticed.
    #[test]
    fn verify_chain_accepts_a_rotating_committee() {
        let committees = vec![test_signers(31, 4), test_signers(32, 4), test_signers(33, 4)];
        let (records, certs) = rotating_chain(&committees);
        let genesis_keys = keys_of(committees.first().expect("at least one committee"));

        let tip = super::verify_record_chain_in_memory(&genesis_keys, &records, &certs)
            .expect("a genuinely rotating committee chain must verify");
        assert_eq!(tip.epoch, 2, "the verified tip must be the bundle's last record");
    }

    /// The negative control for rotation: a successor whose committee is not the one its
    /// predecessor handed forward is refused, so the accept above is not just accepting
    /// anything.
    #[test]
    fn verify_chain_rejects_a_successor_outside_the_handed_forward_committee() {
        let committees = vec![test_signers(34, 4), test_signers(35, 4)];
        let (records, certs) = rotating_chain(&committees);
        // Epoch 1 hands its own committee forward, so an epoch-2 record from an unrelated committee
        // is exactly the successor the handoff does not authorise. Its parent link and its own
        // certificate are genuine, so only the committee anchor can refuse it.
        let usurpers = test_signers(36, 4);
        let parent = records.last().expect("chain is non-empty").digest();
        let (spliced, spliced_cert) = signed_pair(2, &usurpers, parent);
        let records: Vec<EpochRecord> =
            records.into_iter().chain(std::iter::once(spliced)).collect();
        let certs: HashMap<EpochDigest, EpochCertificate> = certs
            .into_iter()
            .chain(std::iter::once((spliced_cert.epoch_hash, spliced_cert)))
            .collect();
        let genesis_keys = keys_of(committees.first().expect("at least one committee"));

        let err = super::verify_record_chain_in_memory(&genesis_keys, &records, &certs)
            .expect_err("a successor outside the handed-forward committee must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 2 record failed certificate verification"),
            "unexpected: {msg}"
        );
        assert!(msg.contains("committee anchored to local genesis: false"), "unexpected: {msg}");
    }

    /// A bundle chained from a different genesis committee than the local one is refused at epoch
    /// 0 — the trust-root check, now applied before the import rather than after it.
    #[test]
    fn verify_chain_rejects_wrong_genesis_committee() {
        let bundle_signers = test_signers(14, 4);
        let local_signers = test_signers(15, 4);
        let (records, certs) = signed_chain(2, &bundle_signers, tn_types::BlockNumHash::default());

        let err = super::verify_record_chain_in_memory(&keys_of(&local_signers), &records, &certs)
            .expect_err("a bundle from another genesis committee must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 0 record failed certificate verification"),
            "unexpected: {msg}"
        );
        assert!(msg.contains("committee anchored to local genesis: false"), "unexpected: {msg}");
    }

    /// Every epoch must carry a certificate; a bundle missing one is refused before the import.
    #[test]
    fn verify_chain_rejects_missing_certificate() {
        let signers = test_signers(16, 4);
        let (records, certs) = signed_chain(2, &signers, tn_types::BlockNumHash::default());
        let tip_digest = records.last().expect("chain is non-empty").digest();
        let certs: HashMap<EpochDigest, EpochCertificate> =
            certs.into_iter().filter(|(digest, _)| *digest != tip_digest).collect();

        let err = super::verify_record_chain_in_memory(&keys_of(&signers), &records, &certs)
            .expect_err("a bundle missing the tip certificate must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 2 record has no certificate in the bundle"),
            "unexpected: {msg}"
        );
    }

    /// A record spliced in with a genuine certificate but the wrong parent digest is refused: the
    /// chain link is checked, not just each record in isolation.
    #[test]
    fn verify_chain_rejects_broken_parent_link() {
        let signers = test_signers(17, 4);
        let (records, certs) = signed_chain(1, &signers, tn_types::BlockNumHash::default());
        // Epoch 2 must chain to epoch 1; point it at epoch 0's digest instead. Everything else
        // about the record — its committee, its certificate — is genuine.
        let wrong_parent = records.first().expect("chain is non-empty").digest();
        let (spliced, spliced_cert) = signed_pair(2, &signers, wrong_parent);
        let records: Vec<EpochRecord> =
            records.into_iter().chain(std::iter::once(spliced)).collect();
        let certs: HashMap<EpochDigest, EpochCertificate> = certs
            .into_iter()
            .chain(std::iter::once((spliced_cert.epoch_hash, spliced_cert)))
            .collect();

        let err = super::verify_record_chain_in_memory(&keys_of(&signers), &records, &certs)
            .expect_err("a broken parent link must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("epoch 2 record failed certificate verification"),
            "unexpected: {msg}"
        );
        assert!(msg.contains("parent link: false"), "unexpected: {msg}");
    }

    /// A gap in the record run is refused with the offending position named, before the import.
    #[test]
    fn verify_chain_rejects_non_contiguous_records() {
        let signers = test_signers(18, 4);
        let (records, certs) = signed_chain(2, &signers, tn_types::BlockNumHash::default());
        let records: Vec<EpochRecord> =
            records.into_iter().filter(|record| record.epoch != 1).collect();

        let err = super::verify_record_chain_in_memory(&keys_of(&signers), &records, &certs)
            .expect_err("a non-contiguous record run must be refused");
        let msg = err.to_string();
        assert!(
            msg.contains("not contiguous from epoch 0 (position 1 is epoch 2)"),
            "unexpected: {msg}"
        );
    }

    /// The hoisted check is not a weaker check: a bundle the pre-import walk refuses is one the
    /// persisting path refuses too. Moving verification earlier changes *when* the refusal arrives,
    /// never *whether* it does.
    #[tokio::test]
    async fn preflight_refuses_exactly_what_the_persisting_path_refuses() {
        let bundle_signers = test_signers(19, 4);
        let local_signers = test_signers(20, 4);
        let local_keys = keys_of(&local_signers);
        let (records, certs) = signed_chain(2, &bundle_signers, tn_types::BlockNumHash::default());

        let preflight = super::verify_record_chain_in_memory(&local_keys, &records, &certs);
        assert!(preflight.is_err(), "the pre-import walk must refuse this bundle");

        let dir = TempDir::with_prefix("preflight_agreement").expect("temp dir");
        let db = EpochRecordDb::open(dir.path()).expect("open db");
        let persisting =
            super::verify_and_save_epoch_records(&db, local_keys, &records, &certs).await;
        assert!(persisting.is_err(), "the persisting path must refuse the same bundle");
    }

    /// The ordering, end to end: `db load-state` refuses a bundle whose record chain does not
    /// verify against the local genesis committee *before* it creates any chain data.
    ///
    /// The discriminating assertion is the absence of the import's cleanup wrapper. Every failure
    /// raised inside the import closure is wrapped with "state import failed; removed the chain
    /// data it created", so a bare verification error proves `restore_pack` never ran. Asserting
    /// only that the chain-data dirs are absent would not tell the two orderings apart, because the
    /// failure path deletes them either way.
    #[test]
    fn load_state_refuses_a_bad_chain_before_creating_chain_data() {
        use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo, TelcoinDirs as _};
        use tn_storage::{
            epoch_records::{CERTS_NAME, RECORDS_NAME},
            exec_state_pack::{ExecStatePackReader, ExecStatePackWriter},
        };
        use tn_types::{BlockNumHash, ExecHeader, B256};

        let datadir = tempfile::tempdir().expect("datadir");
        let datadir_path = datadir.path().to_path_buf();
        // `Config::load_adiri` reads node-info.yaml and materializes genesis/committee.yaml; the
        // adiri genesis itself is compiled in, so this needs no network, no keys, no passphrase.
        Config::write_to_path(datadir_path.node_info_path(), NodeInfo::default(), ConfigFmt::YAML)
            .expect("seed node info");

        // A real exec-state pack, so the bundle clears `check_state_pack_matches_record` and the
        // only thing left that can refuse it is the certificate chain.
        let bundle = tempfile::tempdir().expect("bundle");
        let root = B256::from([7u8; 32]);
        let snapshot = ExecHeader { number: 1, state_root: root, ..Default::default() };
        ExecStatePackWriter::create(bundle.path(), root, std::slice::from_ref(&snapshot))
            .expect("create pack")
            .finish()
            .expect("finish pack");
        let pack_final_state = {
            let reader = ExecStatePackReader::open(bundle.path()).expect("open pack");
            BlockNumHash::new(reader.meta().block_number, reader.meta().block_hash)
        };

        // Records signed by a committee that is not adiri's genesis committee: a well-formed
        // bundle from the wrong chain.
        let signers = test_signers(21, 4);
        let (records, certs) = signed_chain(1, &signers, pack_final_state);
        let records_src = tempfile::tempdir().expect("records src");
        write_signed_bundle(records_src.path(), &records, &certs);
        fs::copy(records_src.path().join(RECORDS_NAME), bundle.path().join("epoch_records"))
            .expect("copy records");
        fs::copy(records_src.path().join(CERTS_NAME), bundle.path().join("epoch_certs"))
            .expect("copy certs");
        fs::write(bundle.path().join("consensus_data"), b"x").expect("write consensus data");

        let args = super::DbLoadStateArgs {
            pack: bundle.path().to_path_buf(),
            chain: Some(super::NamedChain::Adiri),
        };
        let err = args
            .execute(datadir_path.clone())
            .expect_err("a bundle from another chain's committee must be refused");
        let msg = format!("{err:#}");

        assert!(msg.contains("failed certificate verification"), "unexpected error: {msg}");
        assert!(
            !msg.contains("state import failed"),
            "the refusal must arrive before the state import, not after it: {msg}"
        );
        super::chain_data_dirs(&datadir_path).iter().for_each(|dir| {
            assert!(!dir.exists(), "{} must not exist after a pre-import refusal", dir.display());
        });
    }
}
