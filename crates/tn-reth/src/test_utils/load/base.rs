//! The bench's cached base datadir: a closed datadir holding a prefilled genesis, built once per
//! spec and backend and cloned for every run, and the slim chain spec that reopens it.
//!
//! A large genesis is slow to build: the chain spec computes its state root over the whole alloc
//! in memory, and `init_genesis` writes every account and computes the root again from the
//! database. [`build_base`] does that once into a cache directory and returns the cached base on
//! later calls. A run clones the base ([`clone_datadir`](super::clone_datadir)) and opens the
//! clone with [`slim_chain`]; nothing writes to the base itself.
//!
//! **Layout.** `<cache>/<name>/` is a datadir (`db/`, `static_files/`, ...) plus [`META_FILE`], a
//! JSON [`BaseMeta`]. The name is [`base_dir_name`]: `base-<backend>-<key>`, with `<key>` 16 hex
//! digits of `keccak256` over the JSON of the [`PrefillSpec`] and [`LAYOUT_VERSION`]. A later step
//! that derives a datadir from a base (one aged by N outputs, say) names it by passing its own
//! `extra` component, which [`base_dir_name`] appends. A build runs in `<cache>/<name>.building/`
//! and becomes the base by one rename, after its meta is synced, so an interrupted build leaves
//! only a `.building` dir, which the next build removes. One process builds in a cache at a time.
//!
//! **What invalidates a base.** A different spec or backend names a different dir. A change to
//! the genesis outside the prefill (the token or router code, the test genesis) changes the
//! [`BaseMeta::slim_genesis_hash`], and [`build_base`] refuses the stale base. A change that
//! keeps the genesis but not the datadir (a reth table layout) must bump [`LAYOUT_VERSION`].
//! [`build_base`] never rebuilds over an existing base: a base whose meta does not match, or a
//! dir without a meta, is an error naming the mismatch, and the caller removes it.
//!
//! **Why a slim chain.** Opening an env builds its chain spec from a `Genesis`, and
//! `From<Genesis>` computes the genesis header, state root included, over the whole alloc: a
//! 1M-account root on every open. [`slim_chain`] builds the chain spec from the spec's genesis
//! without the senders and the accounts ([`PrefillSpec::slim`]), then replaces its genesis header
//! with the base's cached one. `init_genesis` checks an existing genesis by the header's hash
//! alone, so the slim chain opens the base without reading the alloc, and a header that differs
//! from the database's is refused (`GenesisHashMismatch`). The slim chain's `genesis().alloc`
//! lacks the prefill; nothing on TN's open or execute path reads the alloc.

use super::{
    env::{db_file_bytes, dir_bytes, open_production_env},
    PrefillSpec,
};
use crate::RethChainSpec;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tn_storage::disk::{fsync_directory, fsync_file};
use tn_types::{keccak256, ExecHeader, SealedHeader, TaskManager, B256};

/// The base's meta file inside its datadir.
pub const META_FILE: &str = "bench-base.json";

/// The version of the base datadir's layout. Bump it when a base built by earlier code no longer
/// matches what this code would build for the same spec and the genesis check cannot tell
/// ([`BaseMeta::slim_genesis_hash`]): a reth table layout, say.
pub const LAYOUT_VERSION: u32 = 1;

/// This build's database backend, as [`BaseMeta::backend`] records it.
pub const BACKEND: &str = if cfg!(feature = "tndb-b") { "tndb-b" } else { "mdbx" };

/// The `--db.*` flags a base is built with: production defaults, durable sync.
const BUILD_DB_FLAGS: [&str; 2] = ["--db.sync-mode", "durable"];

/// What a cached base holds, written as [`META_FILE`] in its datadir.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BaseMeta {
    /// [`LAYOUT_VERSION`] of the code that built the base.
    pub layout_version: u32,
    /// The backend that built the base ([`BACKEND`]); the other build cannot open it.
    pub backend: String,
    /// The prefilled genesis the base holds.
    pub spec: PrefillSpec,
    /// The genesis hash of [`PrefillSpec::slim`]'s chain spec at build time: a change to the
    /// genesis outside the prefill changes it.
    pub slim_genesis_hash: B256,
    /// The hash of [`Self::genesis_header`].
    pub genesis_hash: B256,
    /// The base's genesis header, which [`slim_chain`] opens it with.
    pub genesis_header: ExecHeader,
    /// When the build finished, in seconds since the Unix epoch.
    pub built_at: u64,
    /// The whole build, in milliseconds (whole numbers, so the meta reads back exactly).
    pub build_ms: u64,
    /// Deriving the genesis and its chain spec (the state root in memory), in milliseconds.
    pub genesis_ms: u64,
    /// Opening the env, which writes the genesis to the database (`init_genesis`), in
    /// milliseconds.
    pub init_ms: u64,
    /// The length of the database data file ([`db_file_bytes`]).
    pub db_len: u64,
    /// The allocated bytes of the database data file.
    pub db_allocated: u64,
    /// The total length of the files under `static_files` ([`dir_bytes`]).
    pub static_files_len: u64,
    /// The allocated bytes of the files under `static_files`.
    pub static_files_allocated: u64,
}

impl BaseMeta {
    /// The base's sealed genesis header.
    pub fn sealed_header(&self) -> SealedHeader {
        SealedHeader::new(self.genesis_header.clone(), self.genesis_hash)
    }

    /// Refuse this meta, read from the base `dir`, unless it describes a base of `spec` built by
    /// this code on this backend.
    fn check(&self, spec: &PrefillSpec, dir: &Path) -> eyre::Result<()> {
        let refuse = |what: String| {
            eyre::eyre!(
                "the cached base {} does not match: {what}. Remove the dir to rebuild it",
                dir.display()
            )
        };
        if self.layout_version != LAYOUT_VERSION {
            return Err(refuse(format!(
                "layout version {} (this code: {LAYOUT_VERSION})",
                self.layout_version
            )));
        }
        if self.backend != BACKEND {
            return Err(refuse(format!("backend {} (this build: {BACKEND})", self.backend)));
        }
        if self.spec != *spec {
            return Err(refuse(format!("spec {:?} (requested: {spec:?})", self.spec)));
        }
        let header_hash = self.genesis_header.hash_slow();
        if header_hash != self.genesis_hash {
            return Err(refuse(format!(
                "genesis header hashes to {header_hash}, the meta records {}",
                self.genesis_hash
            )));
        }
        let slim_hash = spec.slim_chain().genesis_hash();
        if slim_hash != self.slim_genesis_hash {
            return Err(refuse(format!(
                "slim genesis hash {} (this code: {slim_hash}): the genesis outside the prefill \
                 changed",
                self.slim_genesis_hash
            )));
        }
        Ok(())
    }
}

impl PrefillSpec {
    /// This spec without the senders and the accounts: the same base genesis (with the registry
    /// when `registry` is set), tokens, router and pools, so its genesis is small. The tokens'
    /// storage then holds only the router's balances.
    pub fn slim(&self) -> Self {
        Self { accounts: 0, senders: 0, ..*self }
    }

    /// The chain spec of [`Self::slim`]'s genesis, its own genesis header unchanged.
    fn slim_chain(&self) -> RethChainSpec {
        self.slim().genesis().into()
    }
}

/// The dir name of the base of `spec` on this backend: `base-<backend>-<key>`, with `<key>` the
/// first 8 bytes of `keccak256(json(spec, LAYOUT_VERSION))` in hex, then `-<extra>` when `extra`
/// is not empty. [`build_base`] passes no `extra`; a datadir derived from a base passes its own
/// (such as `aged200`), so it caches beside the base under its own name.
pub fn base_dir_name(spec: &PrefillSpec, extra: &str) -> eyre::Result<String> {
    let hash = keccak256(serde_json::to_vec(&(spec, LAYOUT_VERSION))?);
    let key: String = hash[..8].iter().map(|byte| format!("{byte:02x}")).collect();
    let mut name = format!("base-{BACKEND}-{key}");
    if !extra.is_empty() {
        name.push('-');
        name.push_str(extra);
    }
    Ok(name)
}

/// The cached base datadir of `spec` in `cache_dir` and its meta, built first when absent.
///
/// - An existing base `<cache_dir>/<base_dir_name>` is returned when its [`META_FILE`] matches (see
///   [`BaseMeta`]'s checks: layout version, backend, spec, header hash, slim genesis hash);
///   otherwise this is an error naming the mismatch, and the base is left as it is.
/// - Otherwise the base is built in `<name>.building` (a stale one removed first): the env opens
///   with production `--db.*` and durable sync, writing the genesis, and closes; the meta is
///   written and synced; the dir is renamed to `<name>`.
///
/// Must run within a tokio runtime ([`open_production_env`]); with `spec.registry`, deriving the
/// genesis needs one too. Do not clone or open the base while this builds it.
pub fn build_base(spec: &PrefillSpec, cache_dir: &Path) -> eyre::Result<(PathBuf, BaseMeta)> {
    let name = base_dir_name(spec, "")?;
    let dir = cache_dir.join(&name);
    if dir.symlink_metadata().is_ok() {
        let meta_path = dir.join(META_FILE);
        let json = std::fs::read(&meta_path).map_err(|e| {
            eyre::eyre!(
                "the cached base {} has no readable {META_FILE} ({e}). Remove the dir to \
                 rebuild it",
                dir.display()
            )
        })?;
        let meta: BaseMeta = serde_json::from_slice(&json).map_err(|e| {
            eyre::eyre!("the cached base meta {} does not parse: {e}", meta_path.display())
        })?;
        meta.check(spec, &dir)?;
        return Ok((dir, meta));
    }

    std::fs::create_dir_all(cache_dir)?;
    let building = cache_dir.join(format!("{name}.building"));
    if building.symlink_metadata().is_ok() {
        std::fs::remove_dir_all(&building)?;
    }
    std::fs::create_dir(&building)?;

    let start = Instant::now();
    let chain: Arc<RethChainSpec> = Arc::new(spec.genesis().into());
    let genesis_done = start.elapsed();
    let header = {
        let task_manager = TaskManager::default();
        let env = open_production_env(&building, chain.clone(), &BUILD_DB_FLAGS, &task_manager)?;
        let header = env.sealed_header_by_number(0)?.ok_or_else(|| {
            eyre::eyre!("the base {} has no genesis header after opening", building.display())
        })?;
        if header.hash() != chain.genesis_hash() {
            eyre::bail!(
                "the base {} stored genesis {}, its chain spec computed {}",
                building.display(),
                header.hash(),
                chain.genesis_hash()
            );
        }
        // the env, then its tasks, drop here: the database closes before it is measured or moved
        drop(env);
        header
    };
    let init_done = start.elapsed();
    drop(chain);

    let (db_len, db_allocated) = db_file_bytes(&building.join("db"))?;
    let (static_files_len, static_files_allocated) = dir_bytes(&building.join("static_files"))?;
    let meta = BaseMeta {
        layout_version: LAYOUT_VERSION,
        backend: BACKEND.to_owned(),
        spec: *spec,
        slim_genesis_hash: spec.slim_chain().genesis_hash(),
        genesis_hash: header.hash(),
        genesis_header: header.header().clone(),
        built_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        build_ms: start.elapsed().as_millis() as u64,
        genesis_ms: genesis_done.as_millis() as u64,
        init_ms: (init_done - genesis_done).as_millis() as u64,
        db_len,
        db_allocated,
        static_files_len,
        static_files_allocated,
    };
    write_meta(&building, &meta)?;
    std::fs::rename(&building, &dir)?;
    fsync_directory(cache_dir)?;
    Ok((dir, meta))
}

/// Write `meta` as `dir`'s [`META_FILE`]: a temp file, synced, renamed into place, and the dir
/// synced.
fn write_meta(dir: &Path, meta: &BaseMeta) -> eyre::Result<()> {
    let tmp = dir.join(format!("{META_FILE}.tmp"));
    let mut file = File::create(&tmp)?;
    file.write_all(&serde_json::to_vec_pretty(meta)?)?;
    fsync_file(&file)?;
    drop(file);
    std::fs::rename(&tmp, dir.join(META_FILE))?;
    fsync_directory(dir)?;
    Ok(())
}

/// The chain spec that opens a clone of the base `meta` describes: [`PrefillSpec::slim`]'s
/// genesis with the base's genesis header in place of its own (see the module docs). Its genesis
/// hash is the base's, but its `genesis().alloc` lacks the senders and the accounts.
///
/// With `meta.spec.registry`, deriving the genesis needs a tokio runtime.
pub fn slim_chain(meta: &BaseMeta) -> Arc<RethChainSpec> {
    let mut chain = meta.spec.slim_chain();
    chain.genesis_header = meta.sealed_header();
    Arc::new(chain)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_utils::load::{
            announce_temp_dir, clone_datadir,
            state::{balance_slot, ACCOUNT_NATIVE, ACCOUNT_TOKENS, STABLECOIN_A},
        },
        RethEnv,
    };
    use reth_provider::StateProvider as _;
    use tn_types::U256;

    /// A small prefill: 2k accounts, a few senders and pools.
    fn spec() -> PrefillSpec {
        PrefillSpec { accounts: 2_000, senders: 4, pools: 2, seed: 7, registry: false }
    }

    /// Open the datadir `dir` with `chain`, durable, as a run opens a clone.
    fn open(
        dir: &Path,
        chain: Arc<RethChainSpec>,
        task_manager: &TaskManager,
    ) -> eyre::Result<RethEnv> {
        open_production_env(dir, chain, &BUILD_DB_FLAGS, task_manager)
    }

    /// A second build of the same spec returns the cached base: the same meta, the dir as it was.
    #[tokio::test]
    async fn test_build_base_reuses_the_cached_base() -> eyre::Result<()> {
        let cache = tempfile::tempdir()?;
        let spec = spec();
        // an interrupted build's leftovers, which the build removes
        let stale = cache.path().join(format!("{}.building", base_dir_name(&spec, "")?));
        std::fs::create_dir(&stale)?;
        std::fs::write(stale.join("stale"), b"interrupted")?;

        let (dir, meta) = build_base(&spec, cache.path())?;
        assert_eq!(dir, cache.path().join(base_dir_name(&spec, "")?), "the base is named by key");
        assert!(!dir.join("stale").exists(), "the stale build is not part of the base");
        assert!(dir.join(META_FILE).is_file(), "the meta is written");
        assert_eq!(meta.backend, BACKEND);
        assert_eq!(meta.layout_version, LAYOUT_VERSION);
        assert_eq!(
            meta.genesis_hash,
            RethChainSpec::from(spec.genesis()).genesis_hash(),
            "the cached header is the full genesis's"
        );
        assert!(meta.db_len > 0 && meta.static_files_len > 0, "sizes are recorded: {meta:?}");
        let leftovers: Vec<_> = std::fs::read_dir(cache.path())?
            .map(|entry| entry.map(|e| e.file_name()))
            .collect::<Result<_, _>>()?;
        assert_eq!(leftovers, [dir.file_name().expect("a name")], "no .building dir is left");

        // a rebuild would replace the dir and its marker, and record another build
        std::fs::write(dir.join("marker"), b"kept")?;
        let (again, cached) = build_base(&spec, cache.path())?;
        assert_eq!(again, dir);
        assert_eq!(cached, meta, "the second call returns the first build's meta");
        assert!(dir.join("marker").is_file(), "the base dir is the first build's");
        Ok(())
    }

    /// A clone of the base opens with the slim chain at the cached genesis, with the prefilled
    /// state readable; the slim chain itself holds no prefilled account.
    #[tokio::test]
    async fn test_clone_opens_with_slim_chain() -> eyre::Result<()> {
        let cache = tempfile::tempdir()?;
        let spec = spec();
        let (dir, meta) = build_base(&spec, cache.path())?;
        let clone = cache.path().join("run");
        clone_datadir(&dir, &clone)?;

        let chain = slim_chain(&meta);
        let holder = spec.account(7);
        assert!(!chain.genesis().alloc.contains_key(&holder), "the slim alloc has no accounts");
        assert_eq!(chain.genesis_hash(), meta.genesis_hash);

        let task_manager = TaskManager::default();
        let env = open(&clone, chain, &task_manager)?;
        assert_eq!(env.last_block_number()?, 0, "the base holds genesis only");
        assert_eq!(env.canonical_tip().hash(), meta.genesis_hash, "the tip is the cached genesis");
        let state = env.latest()?;
        assert_eq!(
            state.storage(STABLECOIN_A, balance_slot(holder))?,
            Some(U256::from(ACCOUNT_TOKENS)),
            "the prefilled token balance reads back"
        );
        let account = env.retrieve_account(&holder)?.expect("a prefilled account");
        assert_eq!(account.balance, U256::from(ACCOUNT_NATIVE), "the native balance reads back");
        Ok(())
    }

    /// A slim chain carrying another base's genesis header cannot open this base's clone:
    /// `init_genesis` refuses the hash.
    #[tokio::test]
    async fn test_slim_chain_with_another_header_is_refused() -> eyre::Result<()> {
        let cache = tempfile::tempdir()?;
        let (dir, meta) = build_base(&spec(), cache.path())?;
        let (other_dir, other) = build_base(&PrefillSpec { seed: 8, ..spec() }, cache.path())?;
        assert_ne!(dir, other_dir, "the seed keys the base");
        assert_ne!(other.genesis_hash, meta.genesis_hash);

        let clone = cache.path().join("run");
        clone_datadir(&dir, &clone)?;
        let task_manager = TaskManager::default();
        let err = open(&clone, slim_chain(&other), &task_manager)
            .expect_err("another genesis header is refused");
        let message = format!("{err:?}");
        assert!(
            message.contains("genesis hash in the storage does not match")
                && message.contains(&meta.genesis_hash.to_string()),
            "the refusal is init_genesis's hash mismatch: {message}"
        );
        Ok(())
    }

    /// A base whose meta does not match the request, or that has no meta, is an error naming the
    /// mismatch, and the base is left as it is.
    #[tokio::test]
    async fn test_build_base_refuses_a_mismatched_base() -> eyre::Result<()> {
        let cache = tempfile::tempdir()?;
        let spec = spec();
        let (dir, meta) = build_base(&spec, cache.path())?;

        let mut header = meta.genesis_header.clone();
        header.timestamp += 1;
        let tampered: [(&str, BaseMeta); 5] = [
            ("layout version", BaseMeta { layout_version: LAYOUT_VERSION + 1, ..meta.clone() }),
            ("backend other", BaseMeta { backend: "other".to_owned(), ..meta.clone() }),
            ("spec", BaseMeta { spec: PrefillSpec { pools: 3, ..spec }, ..meta.clone() }),
            ("genesis header hashes", BaseMeta { genesis_header: header, ..meta.clone() }),
            ("slim genesis hash", BaseMeta { slim_genesis_hash: B256::ZERO, ..meta.clone() }),
        ];
        for (named, bad) in tampered {
            write_meta(&dir, &bad)?;
            let err = build_base(&spec, cache.path()).expect_err("a mismatched base is refused");
            assert!(err.to_string().contains(named), "the error names the {named}: {err}");
            let kept: BaseMeta = serde_json::from_slice(&std::fs::read(dir.join(META_FILE))?)?;
            assert_eq!(kept, bad, "the refused base is left as it is");
        }

        std::fs::remove_file(dir.join(META_FILE))?;
        let err = build_base(&spec, cache.path()).expect_err("a base without a meta is refused");
        assert!(err.to_string().contains(META_FILE), "the error names the meta file: {err}");
        assert!(dir.join("db").is_dir(), "the base without a meta is left as it is");
        Ok(())
    }

    /// The dir name keys every spec field and the extra component, and names the backend.
    #[test]
    fn test_base_dir_name_keys_the_spec() -> eyre::Result<()> {
        let spec = spec();
        let name = base_dir_name(&spec, "")?;
        assert!(name.starts_with(&format!("base-{BACKEND}-")), "{name}");
        assert_eq!(name.len(), format!("base-{BACKEND}-").len() + 16, "{name}");
        assert_eq!(name, base_dir_name(&spec, "")?, "the name is stable");
        let others = [
            PrefillSpec { accounts: spec.accounts + 1, ..spec },
            PrefillSpec { senders: spec.senders + 1, ..spec },
            PrefillSpec { pools: spec.pools + 1, ..spec },
            PrefillSpec { seed: spec.seed + 1, ..spec },
            PrefillSpec { registry: !spec.registry, ..spec },
        ];
        for other in others {
            assert_ne!(base_dir_name(&other, "")?, name, "{other:?} names another base");
        }
        assert_eq!(base_dir_name(&spec, "aged200")?, format!("{name}-aged200"));
        Ok(())
    }

    /// Build (or reuse) a base at bench scale and print what it cost. On demand:
    /// `TN_EXEC_BENCH_PREFILL` accounts (default 1,000,000), `TN_EXEC_BENCH_SENDERS` senders
    /// (default 10,000), in `TN_EXEC_BENCH_CACHE_DIR` (default a temp dir, removed after). Run it
    /// in release, under `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) for the peak
    /// memory.
    #[tokio::test]
    #[ignore = "on demand: builds a bench-scale base"]
    async fn build_base_at_scale() -> eyre::Result<()> {
        let knob = |name: &str, default: usize| -> eyre::Result<usize> {
            std::env::var(name).map_or(Ok(default), |v| Ok(v.replace('_', "").parse()?))
        };
        let spec = PrefillSpec {
            accounts: knob("TN_EXEC_BENCH_PREFILL", 1_000_000)?,
            senders: knob("TN_EXEC_BENCH_SENDERS", 10_000)?,
            pools: 64,
            seed: 0,
            registry: false,
        };
        let temp;
        let cache = match std::env::var_os("TN_EXEC_BENCH_CACHE_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => {
                temp = tempfile::tempdir()?;
                temp.path().to_path_buf()
            }
        };
        std::fs::create_dir_all(&cache)?;
        announce_temp_dir(&cache);

        let start = Instant::now();
        let (dir, meta) = build_base(&spec, &cache)?;
        let mib = |bytes: u64| bytes as f64 / (1 << 20) as f64;
        let secs = |ms: u64| ms as f64 / 1000.0;
        println!("base {} ({BACKEND}, sync durable): {spec:?}", dir.display());
        println!(
            "call {:.1} s; recorded build {:.1} s = genesis + chain spec {:.1} s, open and \
             init_genesis {:.1} s",
            start.elapsed().as_secs_f64(),
            secs(meta.build_ms),
            secs(meta.genesis_ms),
            secs(meta.init_ms)
        );
        println!(
            "db file {:.1} MiB (allocated {:.1} MiB), static files {:.1} MiB (allocated {:.1} MiB)",
            mib(meta.db_len),
            mib(meta.db_allocated),
            mib(meta.static_files_len),
            mib(meta.static_files_allocated)
        );
        Ok(())
    }
}
