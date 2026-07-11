//! Cloud snapshot subcommands.
//!
//! `snapshot restore` downloads a signed snapshot from remote object storage, verifies it against
//! the node's local trust root, and lays it down onto an empty datadir. `snapshot verify` runs the
//! same download-and-verify path but installs nothing, so an operator can vet a bucket before
//! committing to a restore. `snapshot create` exports a STOPPED node's datadir at an epoch boundary
//! and uploads it, for an operator who wants to publish snapshots out-of-band from the background
//! uploader service.
//!
//! Restore and create both need a [`RethConfig`], which `tn-snapshot` cannot build itself (it is
//! assembled from reth clap arguments reachable only here). This module builds one exactly the way
//! the `node` command does — see [`build_reth_config`] and `node.rs`'s `RethConfig::new` call.

use crate::version::SHORT_VERSION;
use clap::{Args, Parser, Subcommand};
use eyre::{eyre, WrapErr as _};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, TelcoinDirs};
use tn_reth::{RethChainSpec, RethCommand, RethConfig, RethEnv};
use tn_snapshot::{
    export::{export_epoch, ExportArgs},
    restore::{restore_from_snapshot, verify_snapshot_source, RestoreReceipt, VerifiedSummary},
    service::publish_exported,
    store::SnapshotStore,
};
use tn_storage::epoch_records::EpochRecordDb;
use tn_types::{gas_accumulator::RewardsCounter, now, Genesis, TaskManager, B256};
use tokio::runtime::Builder;
use tracing::info;

/// Produce, verify, and restore cloud chain snapshots.
#[derive(Debug, Parser)]
pub struct SnapshotCommand {
    /// Snapshot subcommand.
    #[command(subcommand)]
    command: SnapshotSubcommand,
}

impl SnapshotCommand {
    /// Execute the snapshot command.
    ///
    /// All three subcommands drive async object-store and reth I/O, so this builds a small
    /// multi-thread runtime (mirroring the `node` command's runtime) and blocks on the selected
    /// subcommand. `datadir` is the resolved global `--datadir`, handled identically to the other
    /// top-level subcommands.
    pub fn execute(self, datadir: PathBuf) -> eyre::Result<()> {
        let runtime = Builder::new_multi_thread()
            .thread_name("tn-snapshot")
            .enable_io()
            .enable_time()
            .build()?;

        runtime.block_on(async move {
            match self.command {
                SnapshotSubcommand::Restore(args) => args.execute(datadir).await,
                SnapshotSubcommand::Verify(args) => args.execute(datadir).await,
                SnapshotSubcommand::Create(args) => args.execute(datadir).await,
            }
        })
    }
}

/// Supported snapshot subcommands.
#[derive(Debug, Subcommand)]
enum SnapshotSubcommand {
    /// Download, verify, and install a snapshot onto an empty datadir.
    Restore(RestoreArgs),

    /// Download and fully verify a snapshot without installing anything.
    Verify(VerifyArgs),

    /// Export a stopped node's datadir at an epoch boundary and upload it.
    Create(CreateArgs),
}

/// Restore a snapshot from a remote bucket onto an empty datadir.
#[derive(Debug, Args)]
pub struct RestoreArgs {
    /// Object-store URL of the snapshot bucket to restore from.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `http(s)://`, and `file://`. Cloud backends
    /// read credentials from the standard per-scheme environment variables (for example `AWS_*`).
    #[arg(long, value_name = "URL")]
    pub source: String,

    /// Pin the restore to a specific epoch instead of the bucket's `latest.json` pointer.
    ///
    /// A bucket cannot forge a snapshot for the wrong committee, but it CAN withhold the newest
    /// one and serve an older, still-valid snapshot. Pinning an epoch bypasses the pointer and
    /// defeats withholding; when omitted, the chosen epoch is logged so it can be checked.
    #[arg(long, value_name = "EPOCH")]
    pub epoch: Option<u32>,

    /// Additional reth arguments (database tuning, etc.).
    #[clap(flatten)]
    pub reth: RethCommand,
}

impl RestoreArgs {
    /// Download, verify, and install the snapshot, then print the restore receipt.
    ///
    /// Precondition errors from `tn-snapshot` (chain data already present, missing trust root, an
    /// interrupted prior restore) surface verbatim so an operator sees the precise cause.
    async fn execute(self, datadir: PathBuf) -> eyre::Result<()> {
        let RestoreArgs { source, epoch, reth } = self;
        let bundle = build_reth_config(&datadir, reth)?;

        // errors surface as-is: ChainDataExists / MissingTrustRoot / RestoreIncomplete carry
        // precise messages an auto-restore caller (and an operator) can act on.
        let receipt = restore_from_snapshot(&datadir, &source, epoch, &bundle.config).await?;
        print_restore_receipt(&datadir, &receipt);
        Ok(())
    }
}

/// Verify a snapshot from a remote bucket without installing it.
#[derive(Debug, Args)]
pub struct VerifyArgs {
    /// Object-store URL of the snapshot bucket to verify.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `http(s)://`, and `file://`. Cloud backends
    /// read credentials from the standard per-scheme environment variables (for example `AWS_*`).
    #[arg(long, value_name = "URL")]
    pub source: String,

    /// Pin verification to a specific epoch instead of the bucket's `latest.json` pointer.
    ///
    /// As with `restore`, pinning bypasses the pointer; when omitted, the chosen epoch is logged.
    #[arg(long, value_name = "EPOCH")]
    pub epoch: Option<u32>,
}

impl VerifyArgs {
    /// Download and fully verify the snapshot, then print the verified summary. Nothing is written
    /// to the datadir.
    async fn execute(self, datadir: PathBuf) -> eyre::Result<()> {
        let VerifyArgs { source, epoch } = self;
        // no RethConfig needed: verify downloads and checks, but never installs.
        let summary = verify_snapshot_source(&datadir, &source, epoch).await?;
        print_verified_summary(&summary);
        Ok(())
    }
}

/// Export a stopped node's datadir at an epoch boundary and upload the snapshot.
#[derive(Debug, Args)]
pub struct CreateArgs {
    /// Object-store URL to upload the created snapshot to.
    ///
    /// Supported schemes: `s3://`, `gs://`, `az://`, `http(s)://`, and `file://`. Cloud backends
    /// read credentials from the standard per-scheme environment variables (for example `AWS_*`).
    #[arg(long, value_name = "URL")]
    pub out: String,

    /// Additional reth arguments (database tuning, etc.).
    #[clap(flatten)]
    pub reth: RethCommand,
}

impl CreateArgs {
    /// Export the closed epoch at the datadir's canonical tip and upload it.
    ///
    /// The node MUST be stopped, and stopped exactly at an epoch boundary: the canonical tip has to
    /// equal the latest epoch record's final execution block (number and hash). Exporting a
    /// historical (non-boundary) block is a follow-up, so a tip that is not a boundary is a clear
    /// error rather than a partial export.
    async fn execute(self, datadir: PathBuf) -> eyre::Result<()> {
        let CreateArgs { out, reth } = self;

        // build the reth config from the local genesis, exactly as the node command does before
        // constructing a RethEnv (node.rs's RethConfig::new). chain_id and genesis_hash bind the
        // manifest to this chain.
        let bundle = build_reth_config(&datadir, reth)?;

        // open the stopped node's reth database and construct a read-only environment over it. the
        // task manager stays alive for the whole export so the env's spawner is not cancelled.
        let reth_db = RethEnv::new_database(&bundle.config, datadir.reth_db_path())
            .wrap_err("snapshot create: opening reth database")?;
        let task_manager = TaskManager::new("snapshot-create");
        let reth_env =
            RethEnv::new(&bundle.config, &task_manager, reth_db, None, RewardsCounter::default())
                .wrap_err("snapshot create: constructing reth environment")?;

        // epoch records live under the datadir's epochs path, the same place restore seeds them.
        let epoch_db = EpochRecordDb::open(datadir.epochs_db_path())
            .map_err(|e| eyre!("snapshot create: opening epoch records: {e}"))?;

        let record = epoch_db.latest_record().await.ok_or_else(|| {
            eyre!(
                "snapshot create: no epoch records found; a snapshot requires at least one \
                 closed epoch"
            )
        })?;
        let final_state = record.final_state;

        // v1 boundary guard: the canonical tip must be exactly this record's closing block. read
        // the tip straight from the committed database (not the in-memory canonical head),
        // matching how a freshly-opened env reads an existing datadir.
        let tip_number = reth_env
            .last_block_number()
            .wrap_err("snapshot create: reading the canonical tip number")?;
        let tip_hash = reth_env
            .sealed_header_by_number(tip_number)
            .wrap_err("snapshot create: reading the canonical tip header")?
            .ok_or_else(|| {
                eyre!("snapshot create: canonical tip header at block {tip_number} is missing")
            })?
            .hash();
        if tip_number != final_state.number || tip_hash != final_state.hash {
            return Err(eyre!(
                "snapshot create requires the node stopped exactly at an epoch boundary; \
                 tip {tip_number} ({tip_hash}) vs record {} ({}) — historical-block export is a \
                 follow-up",
                final_state.number,
                final_state.hash
            ));
        }

        // pin the state view and confirm the pin observes the same boundary tip before exporting.
        let pinned =
            reth_env.pin_state_view().wrap_err("snapshot create: pinning the state view")?;
        if !pinned.verify_tip(final_state).wrap_err("snapshot create: verifying the pinned tip")? {
            return Err(eyre!(
                "snapshot create: pinned state view does not observe the expected boundary tip \
                 {} ({})",
                final_state.number,
                final_state.hash
            ));
        }

        // export the closed epoch into a local staging directory under the snapshots path.
        let staging_root = datadir.snapshots_path().join("create-staging");
        std::fs::create_dir_all(&staging_root)?;
        let exported = export_epoch(ExportArgs {
            reth_env: &reth_env,
            pinned_state: pinned,
            epoch_record: &record,
            epoch_records: &epoch_db,
            staging_root: &staging_root,
            chain_id: bundle.chain_id,
            genesis_hash: bundle.genesis_hash,
            node_version: SHORT_VERSION.to_string(),
            created_at: now(),
        })
        .await
        .wrap_err("snapshot create: exporting epoch")?;

        // never publish an epoch a restored node could not resume fee calculation from.
        if !exported.fee_derivable {
            let _ = std::fs::remove_dir_all(&staging_root);
            return Err(eyre!(
                "snapshot create: refusing to publish epoch {}: an EIP-1559 worker has no \
                 chain-observable base-fee anchor in this epoch",
                exported.manifest.epoch
            ));
        }

        // upload via the service's tested publish path: artifacts, then manifest bytes, then the
        // latest.json pointer LAST, with each upload verified against its manifest entry.
        let store =
            SnapshotStore::open(&out).wrap_err("snapshot create: opening destination store")?;
        let epoch = exported.manifest.epoch;
        let summary = publish_exported(&store, bundle.chain_id, epoch, &exported)
            .await
            .wrap_err("snapshot create: uploading snapshot")?;

        // staging is disposable once the snapshot is live.
        let _ = std::fs::remove_dir_all(&staging_root);

        println!("snapshot created and published");
        println!("  bucket:    {out}");
        println!("  epoch:     {epoch}");
        println!("  artifacts: {}", summary.artifacts);
        println!("  bytes:     {}", summary.bytes);
        info!(
            target: "tn::snapshot",
            epoch,
            bucket = %out,
            artifacts = summary.artifacts,
            bytes = summary.bytes,
            "published epoch snapshot"
        );

        Ok(())
    }
}

/// A [`RethConfig`] built from the datadir's local genesis, plus the chain binding derived from it.
struct RethConfigBundle {
    /// The assembled reth config, ready to open the reth database.
    config: RethConfig,
    /// EVM chain id read from the local genesis.
    chain_id: u64,
    /// Genesis block hash derived from the local genesis.
    genesis_hash: B256,
}

/// Build a [`RethConfig`] from the datadir's local genesis, mirroring the node command's
/// construction (see `node.rs`'s `RethConfig::new` call).
///
/// `tn-snapshot` cannot construct a [`RethConfig`] itself, so restore and create assemble one here
/// from the same reth clap arguments the node uses. The genesis is the only local file required, so
/// this works on a not-yet-synced datadir (restore) and on a stopped node's datadir (create). The
/// tool never binds ports, so `instance` and `with_unused_ports` are fixed to the defaults.
fn build_reth_config<TND: TelcoinDirs + AsRef<Path>>(
    datadir: &TND,
    reth: RethCommand,
) -> eyre::Result<RethConfigBundle> {
    let genesis: Genesis = Config::load_from_path(datadir.genesis_file_path(), ConfigFmt::YAML)
        .wrap_err("reading local genesis to build the reth config")?;
    let chain_id = genesis.config.chain_id;
    let chain_spec: RethChainSpec = genesis.into();
    let genesis_hash = chain_spec.sealed_genesis_header().hash();
    let config = RethConfig::new(reth, None, datadir, false, Arc::new(chain_spec));
    Ok(RethConfigBundle { config, chain_id, genesis_hash })
}

/// Print a completed restore receipt and log the installed epoch prominently.
fn print_restore_receipt<TND: TelcoinDirs>(datadir: &TND, receipt: &RestoreReceipt) {
    // the prominent epoch line: withholding is the one attack a valid trust root cannot rule out,
    // so the installed epoch must be easy to see and check.
    info!(
        target: "tn::snapshot",
        epoch = receipt.epoch,
        block = receipt.final_state.number,
        "snapshot restore installed epoch {}",
        receipt.epoch
    );

    let receipt_path = datadir.snapshots_path().join("last-restore.json");
    println!("snapshot restore complete");
    println!("  epoch:            {}", receipt.epoch);
    println!("  final block:      {} ({})", receipt.final_state.number, receipt.final_state.hash);
    println!(
        "  final consensus:  {} ({})",
        receipt.final_consensus.number, receipt.final_consensus.hash
    );
    println!("  state root:       {}", receipt.state_root);
    println!("  accounts:         {}", receipt.counts.accounts);
    println!("  storage slots:    {}", receipt.counts.storage_slots);
    println!("  bytecodes:        {}", receipt.counts.bytecodes);
    println!("  headers:          {}", receipt.counts.headers);
    println!("  records:          {}", receipt.counts.records);
    println!("  chain id:         {}", receipt.chain_id);
    println!("  node version:     {}", receipt.node_version);
    println!("  source:           {}", receipt.source_url);
    println!("  receipt written:  {}", receipt_path.display());
}

/// Print the summary of a successful verification. Nothing was installed.
fn print_verified_summary(summary: &VerifiedSummary) {
    println!("snapshot verification succeeded");
    println!("  epoch:            {}", summary.epoch);
    println!("  final block:      {} ({})", summary.final_state.number, summary.final_state.hash);
    println!(
        "  final consensus:  {} ({})",
        summary.final_consensus.number, summary.final_consensus.hash
    );
    println!("  accounts:         {}", summary.counts.accounts);
    println!("  storage slots:    {}", summary.counts.storage_slots);
    println!("  bytecodes:        {}", summary.counts.bytecodes);
    println!("  headers:          {}", summary.counts.headers);
    println!("  records:          {}", summary.counts.records);
    println!("  fee derivable:    {}", summary.fee_derivable);
    println!("  chain id:         {}", summary.chain_id);
    println!("  node version:     {}", summary.node_version);
    println!("  source:           {}", summary.source_url);
}

#[cfg(test)]
mod tests {
    use crate::{
        cli::{Cli, Commands},
        NoArgs,
    };

    /// Extract the snapshot subcommand from a parsed CLI, panicking with context otherwise.
    fn snapshot_command(cli: Cli<NoArgs>) -> super::SnapshotCommand {
        match cli.command {
            Commands::Snapshot(command) => *command,
            other => panic!("expected the snapshot subcommand, got {other:?}"),
        }
    }

    #[test]
    fn parse_restore_with_source_and_epoch() {
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "snapshot",
            "restore",
            "--source",
            "file:///tmp/bucket",
            "--epoch",
            "7",
        ])
        .expect("cli parsed");
        let super::SnapshotSubcommand::Restore(args) = snapshot_command(cli).command else {
            panic!("expected the restore subcommand");
        };
        assert_eq!(args.source, "file:///tmp/bucket");
        assert_eq!(args.epoch, Some(7));
    }

    #[test]
    fn parse_restore_without_epoch() {
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "snapshot",
            "restore",
            "--source",
            "s3://bucket/prefix",
        ])
        .expect("cli parsed");
        let super::SnapshotSubcommand::Restore(args) = snapshot_command(cli).command else {
            panic!("expected the restore subcommand");
        };
        assert_eq!(args.source, "s3://bucket/prefix");
        assert_eq!(args.epoch, None);
    }

    #[test]
    fn parse_verify_with_epoch() {
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "snapshot",
            "verify",
            "--source",
            "gs://bucket",
            "--epoch",
            "3",
        ])
        .expect("cli parsed");
        let super::SnapshotSubcommand::Verify(args) = snapshot_command(cli).command else {
            panic!("expected the verify subcommand");
        };
        assert_eq!(args.source, "gs://bucket");
        assert_eq!(args.epoch, Some(3));
    }

    #[test]
    fn parse_create_with_out() {
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "snapshot",
            "create",
            "--out",
            "file:///tmp/out",
        ])
        .expect("cli parsed");
        let super::SnapshotSubcommand::Create(args) = snapshot_command(cli).command else {
            panic!("expected the create subcommand");
        };
        assert_eq!(args.out, "file:///tmp/out");
    }

    #[test]
    fn restore_requires_source() {
        // --source is mandatory, so a bare `restore` must fail to parse.
        Cli::<NoArgs>::try_parse_args_from(["tn", "snapshot", "restore"])
            .expect_err("restore without --source must fail");
    }

    #[test]
    fn create_requires_out() {
        Cli::<NoArgs>::try_parse_args_from(["tn", "snapshot", "create"])
            .expect_err("create without --out must fail");
    }

    #[test]
    fn global_datadir_parses_before_snapshot_subcommand() {
        // `--datadir` is global, so clap accepts it between `snapshot` and the subcommand and
        // captures it on the top-level Cli.
        let cli = Cli::<NoArgs>::try_parse_args_from([
            "tn",
            "snapshot",
            "--datadir",
            "/tmp/x",
            "verify",
            "--source",
            "file:///tmp/bucket",
        ])
        .expect("cli parsed");
        assert_eq!(cli.datadir.as_deref(), Some(std::path::Path::new("/tmp/x")));
        let super::SnapshotSubcommand::Verify(_) = snapshot_command(cli).command else {
            panic!("expected the verify subcommand");
        };
    }
}
