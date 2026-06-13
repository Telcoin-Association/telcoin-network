//! `db` subcommand: inspect and validate consensus epoch pack files.
//!
//! Phase 1 implements `--validate`: walk a pack's `data` stream and report integrity issues,
//! reproducing the importer's `MissingBatches` check and classifying each missing batch as Absent
//! (a real data gap) vs Misordered (present, but in the wrong consensus-header group).
//!
//! Phase 2 will extend this with `--repair` (`--reorder`, `--add-from`, `--truncate-to-valid`,
//! ...).

use clap::Args;
use eyre::{bail, eyre};
use std::path::{Path, PathBuf};
use tn_storage::{consensus_pack::DATA_NAME, pack_validate::validate_pack_file};
use tn_types::Epoch;

/// Inspect / validate a consensus epoch pack file.
#[derive(Debug, Args)]
pub struct DbArgs {
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

    /// Validate the pack: walk the `data` stream and report integrity issues.
    #[arg(long)]
    pub validate: bool,
}

impl DbArgs {
    /// Execute the `db` command.
    pub fn execute(self, _datadir: PathBuf) -> eyre::Result<()> {
        let (data_file, epoch) = resolve_data_file_and_epoch(&self.path, self.epoch)?;

        if !self.validate {
            bail!("nothing to do: pass --validate (repair lands in a later phase)");
        }

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
