//! The bench's environment: a reth env opened as the node opens it, cheap copies of a datadir
//! (evicted from the page cache on Linux), the temp-dir guard against memory file systems, and the
//! on-disk size of the database.

use super::StorageLayout;
use crate::{
    init_reth_defaults, RethChainSpec, RethCommand, RethConfig, RethDb, RethEnv, StaticFileSegment,
};
use clap::Parser as _;
use serde::Serialize;
use std::{
    os::unix::fs::MetadataExt as _,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};
use tn_storage::disk::{drop_cached_pages, fsync_file};
use tn_types::{gas_accumulator::GasAccumulator, TaskManager};

/// This build's database data file inside the database directory.
#[cfg(not(feature = "tn-lmdb"))]
const DATA_FILE: &str = "mdbx.dat";

/// This build's database data file inside the database directory.
#[cfg(feature = "tn-lmdb")]
const DATA_FILE: &str = tn_lmdb_reth::DATA_FILE;

/// Open the reth env in `datadir` (database in `datadir/db`) as the node opens it: the `--db.*`
/// flags in `db_flags` parsed by the node's CLI (`RethCommand`), resolved through
/// [`RethConfig::new`], then [`RethEnv::new_database`] and [`RethEnv::new`].
///
/// - `db_flags` are command-line words such as `["--db.sync-mode", "durable"]`; every flag must be
///   a `--db.*` flag. With none, the database runs on the production defaults, never the temp
///   chain's bounded map (`RethEnv::new_for_temp_chain`).
/// - The HTTP RPC is enabled, so the env serves `RethEnv::get_rpc_server`'s HTTP modules in
///   process; IPC is disabled. Every port is 0 (`with_unused_ports`) and nothing binds unless the
///   caller starts the server.
/// - The base-fee recipient is the default (`None`), with [`RethEnv::new`]'s process-global side
///   effects.
///
/// The database stays open until the env, its clones and `task_manager`'s tasks holding it are
/// dropped; the tn-lmdb build holds a lock on it until then, so a second open of the same datadir
/// fails meanwhile. [`open_production_env_with_db`] also returns the database handle.
pub fn open_production_env(
    datadir: &Path,
    chain: Arc<RethChainSpec>,
    db_flags: &[&str],
    task_manager: &TaskManager,
) -> eyre::Result<RethEnv> {
    open_production_env_with_db(datadir, chain, db_flags, task_manager).map(|(env, _)| env)
}

/// [`open_production_env`], returning the env's database handle beside it, for reads the env does
/// not offer ([`db_counters`](fn@super::db_counters), [`db_page_size`](super::db_page_size)).
///
/// The handle is one more owner of the database: it stays open (and, on tn-lmdb, locked) until the
/// handle drops too, so drop it with the env before reopening the datadir.
pub fn open_production_env_with_db(
    datadir: &Path,
    chain: Arc<RethChainSpec>,
    db_flags: &[&str],
    task_manager: &TaskManager,
) -> eyre::Result<(RethEnv, RethDb)> {
    open_env(datadir, chain, db_flags, None, task_manager)
}

/// [`open_production_env_with_db`] in the storage layout `layout`: a new datadir is created in it
/// ([`StorageLayout::select`], which refuses a layout the env cannot select yet), and the opened
/// env's datadir must hold it ([`StorageLayout::of`]), new or not; otherwise the env is dropped
/// and this is an error.
pub fn open_production_env_with_layout(
    datadir: &Path,
    chain: Arc<RethChainSpec>,
    db_flags: &[&str],
    layout: StorageLayout,
    task_manager: &TaskManager,
) -> eyre::Result<(RethEnv, RethDb)> {
    let (env, db) = open_env(datadir, chain, db_flags, Some(layout), task_manager)?;
    let held = StorageLayout::of(&env)?;
    eyre::ensure!(
        held == layout,
        "the datadir {} holds storage layout {}, not the requested {}",
        datadir.display(),
        held.label(),
        layout.label()
    );
    Ok((env, db))
}

/// Open the env as [`open_production_env_with_db`] does, selecting `layout` (if any) for a new
/// datadir.
fn open_env(
    datadir: &Path,
    chain: Arc<RethChainSpec>,
    db_flags: &[&str],
    layout: Option<StorageLayout>,
    task_manager: &TaskManager,
) -> eyre::Result<(RethEnv, RethDb)> {
    if let Some(flag) = db_flags.iter().find(|w| w.starts_with('-') && !w.starts_with("--db.")) {
        eyre::bail!("open_production_env takes only --db.* flags, not {flag}");
    }
    init_reth_defaults();
    let mut command =
        RethCommand::try_parse_from(std::iter::once("tn-reth").chain(db_flags.iter().copied()))?;
    command.rpc.http = true;
    command.rpc.ipcdisable = true;
    let mut reth_config = RethConfig::new(command, None, datadir, true, chain);
    if let Some(layout) = layout {
        layout.select(&mut reth_config)?;
    }
    let database = RethEnv::new_database(&reth_config, datadir.join("db"))?;
    let env = RethEnv::new(
        &reth_config,
        task_manager,
        database.clone(),
        None,
        GasAccumulator::default(),
    )?;
    Ok((env, database))
}

/// Copy the datadir `src` to `dst`, sharing blocks where the file system can: `cp -c -R`
/// (`clonefile`) on macOS, `cp -a --reflink=auto --sparse=always` elsewhere. A database's
/// preallocated or sparse tail stays unallocated in the copy.
///
/// On Linux the copy is then pushed out of the page cache ([`evict_from_page_cache`]), so the env
/// opened on it starts cold, as a restarted node's does, on either backend. Without it the copy's
/// pages stay cached as `cp` wrote them, in large folios, and a page a commit dirties through a
/// shared mapping writes back its whole folio: measured on Linux 7.1, ext4, 439 KiB written per
/// 4 KiB page dirtied in a file written by `write(2)` and still cached, against 4 KiB in one read
/// cold at random, which inflated every commit of the run.
///
/// Where the file system clones (APFS always, XFS and btrfs with reflink), the copy shares every
/// block with `src`, so the first write to each shared block costs a copy-on-write in the file
/// system (a new block and an extent update) that a node's own file never pays: timings taken
/// there include it, on both backends, in proportion to each one's writes. ext4 has no reflink:
/// `cp` copies in full there and the copy's timings are free of it.
///
/// `dst` must not exist; its parent must. `src` must not be open by a writer (close its env
/// first): the copy is file by file, so a database written meanwhile copies torn.
pub fn clone_datadir(src: &Path, dst: &Path) -> eyre::Result<()> {
    if dst.symlink_metadata().is_ok() {
        eyre::bail!("clone_datadir: the destination {} already exists", dst.display());
    }
    let mut cp = Command::new("cp");
    #[cfg(target_os = "macos")]
    cp.args(["-c", "-R"]);
    #[cfg(not(target_os = "macos"))]
    cp.args(["-a", "--reflink=auto", "--sparse=always"]);
    let output = cp.arg(src).arg(dst).output()?;
    if !output.status.success() {
        eyre::bail!(
            "clone_datadir: copying {} to {} failed ({}): {}",
            src.display(),
            dst.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    evict_from_page_cache(dst)
        .map_err(|e| eyre::eyre!("clone_datadir: the copy {} stays cached: {e}", dst.display()))?;
    Ok(())
}

/// Push every regular file under `dir` (recursively, symlinks not followed) out of the page cache
/// on Linux: each is synced ([`fsync_file`]), so none of its pages is dirty, then its cached pages
/// are dropped ([`drop_cached_pages`]: `posix_fadvise(POSIX_FADV_DONTNEED)`). Returns the number
/// of files evicted. A failure is an error naming the file.
///
/// Elsewhere it does nothing and returns 0: macOS has no per-file eviction (no `posix_fadvise`),
/// and no page-cache folio writeback of this kind to undo.
fn evict_from_page_cache(dir: &Path) -> eyre::Result<usize> {
    if !cfg!(target_os = "linux") {
        return Ok(0);
    }
    let files = regular_files(dir)?;
    for path in &files {
        let evict = || {
            let file = std::fs::File::open(path)?;
            fsync_file(&file)?;
            drop_cached_pages(&file)
        };
        evict().map_err(|e: std::io::Error| {
            eyre::eyre!("evicting {} from the page cache failed: {e}", path.display())
        })?;
    }
    Ok(files.len())
}

/// The regular files under `dir`, recursively, symlinks not followed, in no particular order.
fn regular_files(dir: &Path) -> eyre::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)
        .map_err(|e| eyre::eyre!("reading the directory {} failed: {e}", dir.display()))?
    {
        let path = entry?.path();
        let meta = path.symlink_metadata()?;
        if meta.is_dir() {
            files.extend(regular_files(&path)?);
        } else if meta.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

/// The type of the file system holding `dir`, from Linux's `/proc/self/mountinfo` (see
/// [`fs_type_in`]); `None` where that file does not exist, as on macOS.
pub fn fs_type(dir: &Path) -> Option<String> {
    let dir = dir.canonicalize().ok()?;
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    fs_type_in(&dir, &mounts)
}

/// The type of the file system holding the canonical path `dir`, by the `mountinfo` table (the
/// format of Linux's `/proc/self/mountinfo`): the mount whose point is the longest prefix of
/// `dir`, by whole components.
pub fn fs_type_in(dir: &Path, mountinfo: &str) -> Option<String> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let (fields, rest) = line.split_once(" - ")?;
            // The mount point is the fifth field, with a space as `\040`.
            let point = fields.split(' ').nth(4)?.replace("\\040", " ");
            let fs = rest.split(' ').next()?;
            dir.starts_with(&point).then(|| (point.len(), fs.to_owned()))
        })
        // Of equal lengths `max_by_key` takes the last: a later mount on a point hides the earlier.
        .max_by_key(|(len, _)| *len)
        .map(|(_, fs)| fs)
}

/// Print the bench's temp dir, and stop the run when it is on a file system held in memory
/// (tmpfs, ramfs): syncs there cost nothing, so durable commits would time no disk.
pub fn announce_temp_dir(dir: &Path) {
    announce_dir_on(dir, fs_type(dir));
}

/// [`announce_temp_dir`] for `dir` on the file system `fs`.
fn announce_dir_on(dir: &Path, fs: Option<String>) {
    match &fs {
        Some(fs) => println!("bench temp dir: {} ({fs})", dir.display()),
        None => println!("bench temp dir: {}", dir.display()),
    }
    assert!(
        !matches!(fs.as_deref(), Some("tmpfs" | "ramfs")),
        "the bench temp dir {} is on {}: syncs there cost nothing, so durable commits would time \
         no disk. Set TMPDIR to a directory on a real disk, for this command only.",
        dir.display(),
        fs.unwrap_or_default(),
    );
}

/// The length and the allocated bytes of this build's database data file (`mdbx.dat` or
/// `tn-lmdb.dat`) in the database directory `db_dir`.
pub fn db_file_bytes(db_dir: &Path) -> eyre::Result<(u64, u64)> {
    let meta = std::fs::metadata(db_dir.join(DATA_FILE))?;
    Ok((meta.len(), meta.blocks() * 512))
}

/// The total length and allocated bytes of the regular files under `dir` (recursively, symlinks
/// not followed), such as a datadir's `static_files`.
pub fn dir_bytes(dir: &Path) -> eyre::Result<(u64, u64)> {
    let mut total = (0, 0);
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.path().symlink_metadata()?;
        if meta.is_dir() {
            let (len, allocated) = dir_bytes(&entry.path())?;
            total = (total.0 + len, total.1 + allocated);
        } else if meta.is_file() {
            total = (total.0 + meta.len(), total.1 + meta.blocks() * 512);
        }
    }
    Ok(total)
}

/// One static-file segment's files under a datadir's `static_files` ([`static_file_sizes`]),
/// each part as its total length and allocated bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SegmentFiles {
    /// The segment's jars (block ranges): its data files.
    pub jars: u64,
    /// The data files.
    pub data: (u64, u64),
    /// The offsets files (`.off`).
    pub offsets: (u64, u64),
    /// The config files (`.conf`).
    pub config: (u64, u64),
    /// The changeset offsets sidecars (`.csoff`), which only the changeset segments have.
    pub csoff: (u64, u64),
    /// Its other files (a temp file, say).
    pub other: (u64, u64),
}

impl SegmentFiles {
    /// Every part's length and allocated bytes, summed.
    pub fn total(&self) -> (u64, u64) {
        [self.data, self.offsets, self.config, self.csoff, self.other]
            .into_iter()
            .fold((0, 0), |sum, part| (sum.0 + part.0, sum.1 + part.1))
    }
}

/// The label [`static_file_sizes`] gives the files of no segment.
pub const NO_SEGMENT: &str = "other";

/// The files under `dir` (a datadir's `static_files`) by segment, in reth's segment order
/// (`StaticFileSegment::iter`), the segments without files left out: a file
/// `static_file_<segment>_<start>_<end>` is a jar's data, and its `.off`, `.conf` and `.csoff`
/// siblings the jar's offsets, config and changeset offsets. Files of no segment, and anything
/// under a subdirectory, come last as [`NO_SEGMENT`]'s `other`, when there are any.
pub fn static_file_sizes(dir: &Path) -> eyre::Result<Vec<(&'static str, SegmentFiles)>> {
    let mut segments: Vec<(&'static str, SegmentFiles)> = StaticFileSegment::iter()
        .map(|segment| (segment.as_str(), SegmentFiles::default()))
        .collect();
    let mut unnamed = SegmentFiles::default();
    let add = |part: &mut (u64, u64), bytes: (u64, u64)| {
        *part = (part.0 + bytes.0, part.1 + bytes.1);
    };
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let meta = entry.path().symlink_metadata()?;
        if meta.is_dir() {
            add(&mut unnamed.other, dir_bytes(&entry.path())?);
            continue;
        }
        if !meta.is_file() {
            continue;
        }
        let bytes = (meta.len(), meta.blocks() * 512);
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let (stem, extension) = name.split_once('.').unwrap_or((&name, ""));
        let segment = stem
            .strip_prefix("static_file_")
            .and_then(|rest| rest.split('_').next())
            .and_then(|label| segments.iter_mut().find(|(name, _)| *name == label));
        let Some((_, files)) = segment else {
            add(&mut unnamed.other, bytes);
            continue;
        };
        match extension {
            "" => {
                files.jars += 1;
                add(&mut files.data, bytes);
            }
            "off" => add(&mut files.offsets, bytes),
            "conf" => add(&mut files.config, bytes),
            "csoff" => add(&mut files.csoff, bytes),
            _ => add(&mut files.other, bytes),
        }
    }
    segments.retain(|(_, files)| *files != SegmentFiles::default());
    if unnamed != SegmentFiles::default() {
        segments.push((NO_SEGMENT, unnamed));
    }
    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests_at, execute_payload_and_update_canonical_chain,
            TransactionFactory,
        },
    };
    use reth_db::mdbx::SyncMode;
    use tn_types::{test_genesis, Address, Bytes, SealedHeader, U256};

    /// Production `--db.*` with durable sync, as the bench runs by default.
    const DURABLE: [&str; 2] = ["--db.sync-mode", "durable"];

    /// Execute and persist one block of one transfer from `factory` on top of `parent`.
    fn persist_block(
        env: &RethEnv,
        chain: &Arc<RethChainSpec>,
        factory: &mut TransactionFactory,
        parent: SealedHeader,
    ) -> eyre::Result<SealedHeader> {
        let number = parent.number + 1;
        let tx = factory.create_eip1559_encoded(
            chain.clone(),
            Some(21_000),
            7,
            Some(Address::ZERO),
            U256::from(100),
            Bytes::new(),
        );
        let output =
            consensus_output_for_tests_at(number as u32, 0, number, false, parent.timestamp + 1);
        let payload = TNPayload::new_for_test(parent, &output);
        let block = execute_payload_and_update_canonical_chain(env, payload, vec![tx])?;
        Ok(block.recovered_block.clone_sealed_header())
    }

    /// The persisted tip of the env opened on `datadir`: the database's last block number and
    /// its header, which must also be the canonical head the env starts from.
    fn reopened_tip(datadir: &Path, chain: &Arc<RethChainSpec>) -> eyre::Result<SealedHeader> {
        let task_manager = TaskManager::default();
        let env = open_production_env(datadir, chain.clone(), &DURABLE, &task_manager)?;
        let number = env.last_block_number()?;
        let header = env.sealed_header_by_number(number)?.expect("the last block's header");
        assert_eq!(env.canonical_tip(), header, "the env starts from the persisted tip");
        Ok(header)
    }

    /// A clone of a closed datadir opens at the original's tip, and writing to the clone leaves
    /// the original as it was.
    #[tokio::test]
    async fn test_clone_datadir_reopens_at_the_same_tip() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let original = tmp.path().join("original");
        let clone = tmp.path().join("clone");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut factory = TransactionFactory::new();

        let tip = {
            let task_manager = TaskManager::default();
            let env = open_production_env(&original, chain.clone(), &DURABLE, &task_manager)?;
            let mut parent = chain.sealed_genesis_header();
            for _ in 0..2 {
                parent = persist_block(&env, &chain, &mut factory, parent)?;
            }
            assert_eq!(env.last_block_number()?, 2, "both blocks are persisted");
            parent
        };
        let (len, allocated) = db_file_bytes(&original.join("db"))?;
        assert!(len > 0 && allocated > 0, "the database file has data: {len} / {allocated}");
        assert!(dir_bytes(&original.join("static_files"))?.0 > 0, "the static files have data");

        clone_datadir(&original, &clone)?;
        assert_eq!(reopened_tip(&clone, &chain)?, tip, "the clone opens at the original's tip");

        // extend the clone, then the original must still end at its own tip
        {
            let task_manager = TaskManager::default();
            let env = open_production_env(&clone, chain.clone(), &DURABLE, &task_manager)?;
            persist_block(&env, &chain, &mut factory, tip.clone())?;
        }
        assert_eq!(reopened_tip(&clone, &chain)?.number, 3, "the clone took a block");
        assert_eq!(reopened_tip(&original, &chain)?, tip, "the original is as it was");
        Ok(())
    }

    /// `regular_files` finds every regular file under a directory, in nested directories too,
    /// and neither a directory nor a symlink (to a file or to a directory).
    #[test]
    fn test_regular_files_finds_every_regular_file() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let root = tmp.path();
        std::fs::create_dir_all(root.join("db/sub"))?;
        std::fs::create_dir(root.join("empty"))?;
        for name in ["top", "db/data", "db/sub/deep"] {
            std::fs::write(root.join(name), name)?;
        }
        std::os::unix::fs::symlink(root.join("top"), root.join("link"))?;
        std::os::unix::fs::symlink(root.join("db"), root.join("db-link"))?;
        let mut found = regular_files(root)?;
        found.sort();
        let want: Vec<PathBuf> =
            ["db/data", "db/sub/deep", "top"].iter().map(|name| root.join(name)).collect();
        assert_eq!(found, want);

        // Eviction syncs and drops exactly those files on Linux; elsewhere it does nothing, not
        // even read the directory, so a missing one is no error there.
        let evicted = evict_from_page_cache(root)?;
        assert_eq!(evicted, if cfg!(target_os = "linux") { 3 } else { 0 });
        let missing = evict_from_page_cache(&root.join("missing"));
        if cfg!(target_os = "linux") {
            let message = missing.expect_err("a missing directory").to_string();
            assert!(message.contains("missing"), "the error names the directory: {message}");
        } else {
            assert_eq!(missing?, 0, "a no-op off Linux");
        }
        Ok(())
    }

    /// The env runs on the node's parsed config: the `--db.*` flags as given and none of the
    /// temp chain's caps, the HTTP RPC on and IPC off.
    #[tokio::test]
    async fn test_open_production_env_runs_the_node_config() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let task_manager = TaskManager::default();
        let env = open_production_env(tmp.path(), chain, &DURABLE, &task_manager)?;
        let config = env.node_config();
        assert_eq!(config.db.sync_mode, Some(SyncMode::Durable), "the db flag reaches the db");
        assert_eq!(config.db.max_size, None, "the production map size, not the temp chain's");
        assert!(config.rpc.http, "the HTTP RPC is on");
        assert!(config.rpc.ipcdisable, "IPC is off");
        Ok(())
    }

    /// The env refuses a flag outside `--db.*` and a malformed `--db.*` flag, before opening.
    #[tokio::test]
    async fn test_open_production_env_refuses_other_flags() {
        let tmp = tempfile::tempdir().expect("temp dir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let task_manager = TaskManager::default();
        let err =
            open_production_env(tmp.path(), chain.clone(), &["--http.api", "eth"], &task_manager)
                .expect_err("a non-db flag is refused");
        assert!(err.to_string().contains("--http.api"), "{err}");
        let err = open_production_env(tmp.path(), chain, &["--db.sync-mode", "x"], &task_manager)
            .expect_err("a malformed db flag is refused");
        assert!(err.to_string().contains("--db.sync-mode"), "{err}");
        assert!(!tmp.path().join("db").exists(), "nothing is opened");
    }

    /// The clone refuses a destination that exists, and leaves it alone.
    /// The static files split by segment and part, in reth's segment order, the files of no
    /// segment last; a production env's genesis writes the headers, transactions and receipts
    /// segments, whose parts sum to the directory's bytes.
    #[tokio::test]
    async fn test_static_file_sizes_by_segment() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let dir = tmp.path();
        for (name, len) in [
            ("static_file_storage-change-sets_0_499999", 100),
            ("static_file_storage-change-sets_0_499999.csoff", 7),
            ("static_file_headers_0_499999", 10),
            ("static_file_headers_500000_999999", 20),
            ("static_file_headers_0_499999.off", 3),
            ("static_file_headers_0_499999.conf", 5),
            ("static_file_headers_0_499999.conf.tmp", 1),
            ("lock", 2),
        ] {
            std::fs::write(dir.join(name), vec![1u8; len])?;
        }
        let sizes = static_file_sizes(dir)?;
        let names: Vec<&str> = sizes.iter().map(|(name, _)| *name).collect();
        assert_eq!(names, ["headers", "storage-change-sets", NO_SEGMENT]);
        let len = |part: (u64, u64)| part.0;
        let headers = sizes[0].1;
        assert_eq!(headers.jars, 2);
        assert_eq!(
            [headers.data, headers.offsets, headers.config, headers.csoff, headers.other].map(len),
            [30, 3, 5, 0, 1]
        );
        let changes = sizes[1].1;
        assert_eq!((changes.jars, len(changes.data), len(changes.csoff)), (1, 100, 7));
        assert_eq!(len(sizes[2].1.other), 2);
        assert_eq!(len(headers.total()), 39);
        assert!(headers.total().1 >= headers.data.1, "allocated bytes sum too");

        let datadir = tmp.path().join("datadir");
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tasks = TaskManager::default();
        let env = open_production_env(&datadir, chain, &DURABLE, &tasks)?;
        drop(env);
        let sizes = static_file_sizes(&datadir.join("static_files"))?;
        let names: Vec<&str> = sizes.iter().map(|(name, _)| *name).collect();
        assert!(names.starts_with(&["headers", "transactions", "receipts"]), "{names:?}");
        let total: u64 = sizes.iter().map(|(_, files)| files.total().0).sum();
        assert_eq!(total, dir_bytes(&datadir.join("static_files"))?.0);
        assert!(sizes[0].1.jars >= 1 && sizes[0].1.config.0 > 0, "{sizes:?}");
        Ok(())
    }

    /// A new datadir opened in layout v1 holds v1, reopens in it and reads it back; v2 is refused
    /// until the env can select it, before any datadir is created.
    #[tokio::test]
    async fn test_open_with_layout_selects_and_checks_it() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let datadir = tmp.path().join("v1");
        for run in ["new", "reopened"] {
            let tasks = TaskManager::new(run);
            let (env, _db) = open_production_env_with_layout(
                &datadir,
                chain.clone(),
                &DURABLE,
                StorageLayout::V1,
                &tasks,
            )?;
            assert_eq!(StorageLayout::of(&env)?, StorageLayout::V1, "{run}");
        }
        let v2 = tmp.path().join("v2");
        let tasks = TaskManager::default();
        let Err(refused) =
            open_production_env_with_layout(&v2, chain, &DURABLE, StorageLayout::V2, &tasks)
        else {
            panic!("v2 cannot be selected yet");
        };
        assert!(refused.to_string().contains("cannot be selected yet"), "{refused}");
        assert!(!v2.join("db").exists(), "refused before the database opens");
        assert_eq!(StorageLayout::parse("v2")?, StorageLayout::V2);
        assert!(StorageLayout::parse("v3").is_err());
        Ok(())
    }

    #[test]
    fn test_clone_datadir_refuses_existing_destination() -> eyre::Result<()> {
        let tmp = tempfile::tempdir()?;
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        std::fs::create_dir_all(src.join("db"))?;
        std::fs::write(src.join("db").join("file"), b"source")?;
        std::fs::create_dir(&dst)?;

        let err = clone_datadir(&src, &dst).expect_err("an existing destination is refused");
        assert!(err.to_string().contains("already exists"), "{err}");
        assert!(std::fs::read_dir(&dst)?.next().is_none(), "the destination is untouched");

        // a fresh destination copies the tree
        let fresh = tmp.path().join("fresh");
        clone_datadir(&src, &fresh)?;
        assert_eq!(std::fs::read(fresh.join("db").join("file"))?, b"source");
        Ok(())
    }

    /// The mount table's longest prefix by whole components wins, a later mount on the same
    /// point hides the earlier, and `\040` is a space.
    #[test]
    fn test_fs_type_in_mountinfo() {
        let mountinfo = "\
22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/sda1 rw
30 22 0:26 / /tmp rw,nosuid,nodev shared:2 - tmpfs tmpfs rw
31 30 8:2 / /tmp/disk rw,relatime shared:3 - xfs /dev/sdb1 rw
32 22 0:27 / /mnt/my\\040disk rw,relatime shared:4 - btrfs /dev/sdc1 rw
33 22 0:28 / /scratch rw shared:5 - ext4 /dev/sdd1 rw
34 33 0:29 / /scratch rw shared:6 - ramfs ramfs rw
";
        let fs = |dir: &str| fs_type_in(Path::new(dir), mountinfo);
        assert_eq!(fs("/tmp/bench").as_deref(), Some("tmpfs"));
        assert_eq!(fs("/tmp/disk/bench").as_deref(), Some("xfs"), "the longest prefix wins");
        assert_eq!(fs("/tmpx/bench").as_deref(), Some("ext4"), "prefixes are whole components");
        assert_eq!(fs("/mnt/my disk/bench").as_deref(), Some("btrfs"), "\\040 is a space");
        assert_eq!(fs("/scratch/bench").as_deref(), Some("ramfs"), "the later mount hides");
        assert_eq!(fs_type_in(Path::new("/tmp"), "").as_deref(), None, "no table, no type");
    }

    /// A temp dir on a memory file system stops the run.
    #[test]
    #[should_panic(expected = "is on tmpfs")]
    fn test_announce_refuses_tmpfs() {
        announce_dir_on(Path::new("/tmp/bench"), Some("tmpfs".to_owned()));
    }

    /// A ramfs stops the run too; a disk or an unknown file system does not.
    #[test]
    #[should_panic(expected = "is on ramfs")]
    fn test_announce_refuses_ramfs() {
        announce_dir_on(Path::new("/srv/bench"), Some("ext4".to_owned()));
        announce_dir_on(Path::new("/srv/bench"), None);
        announce_dir_on(Path::new("/srv/bench"), Some("ramfs".to_owned()));
    }
}
