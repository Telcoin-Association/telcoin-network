//! The storage layout of TN's reth execution database, and the guard that opens each datadir in
//! the layout it was created with.
//!
//! reth has two layouts (`StorageSettings`). v1 keeps change sets, transaction senders and the
//! history indices in database tables, and plain state beside hashed state. v2 moves change sets
//! and senders into static files and keeps only hashed state; its storage history is keyed by the
//! hashed slot. A datadir's layout is chosen when it is created and recorded in reth's `Metadata`
//! table (`storage_settings`; a missing row reads as v1). Nothing converts a datadir from one
//! layout to the other, and flipping the row alone would make every historical read wrong.
//!
//! [`settle`] runs at every open of the provider factory, before reth's consistency check (which
//! prunes static files ahead of the database) and before genesis is written:
//! - A new datadir (no chain in its database or its static files) gets the layout requested for new
//!   datadirs ([`crate::RethConfig::new_datadir_layout`], by default
//!   [`StorageLayout::for_new_datadirs`]).
//! - An existing datadir opens in its stored layout, never flipped. A request for the other layout
//!   is logged: an operator who runs a new binary on an old datadir keeps a working node, and
//!   `telcoin-network db reset-execution` rebuilds it in the new layout.
//! - v2 is refused on the MDBX build, for a new datadir and for an existing one.
//! - TN's own marker ([`TN_LAYOUT_KEY`]) records the layout and a format version beside reth's row,
//!   so a later change to TN's format (packed trie keys, a different transaction-hash index) is an
//!   explicit conversion or refusal, never a silent misread.
//!
//! Refused, before anything else touches the datadir:
//! - a `storage_settings` row or a marker that does not parse;
//! - a marker of a format version this binary does not know (a newer binary wrote it);
//! - a marker whose layout disagrees with `storage_settings`;
//! - an existing v2 datadir without the marker (no TN build wrote it);
//! - a v1 datadir whose static files hold v2-only segments, or a v2 datadir whose static files hold
//!   none (another datadir's static files);
//! - static files holding a chain beside a database that holds none: reth's consistency check would
//!   prune them down to the empty database. The one exception is healed: a creation stopped inside
//!   genesis's commit (TN's marker, no `storage_settings` row, static files holding only this
//!   chain's genesis) is created again ([`holds_only_genesis`]).

use std::fmt;

use reth_chainspec::EthChainSpec as _;
use reth_provider::{
    providers::ProviderFactory, BlockHashReader as _, ChainSpecProvider as _, DBProvider as _,
    DatabaseProviderFactory as _, MetadataProvider as _, MetadataWriter as _,
    StageCheckpointReader as _, StaticFileProviderFactory as _, StaticFileSegment, StorageSettings,
    StorageSettingsCache as _,
};
use reth_stages_types::StageId;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::traits::TelcoinNode;

/// The layout the tn-lmdb build gives a new datadir: v2, with the history indices and the
/// transaction-hash index in the database (vendor/reth CHANGES.md TN-0042..0044). The MDBX build
/// always gives v1 ([`StorageLayout::for_new_datadirs`]); an existing datadir keeps its own.
const NEW_DATADIR_LAYOUT_TN_LMDB: StorageLayout = StorageLayout::V2;

/// The `Metadata` key of TN's layout marker ([`LayoutMarker`]).
pub(crate) const TN_LAYOUT_KEY: &str = "tn_storage_layout";

/// The format version of TN's reth database that this binary writes and reads.
///
/// 1: reth's v1 or v2 layout on the vendored reth's table formats. A change to how TN stores any
/// table (for example packed trie keys, or transaction hashes in a separate index) takes the next
/// version, and an open of an older version converts it or refuses it.
pub(crate) const TN_LAYOUT_VERSION: u32 = 1;

/// The `Metadata` key of reth's storage settings:
/// `reth_storage_api::metadata::keys::STORAGE_SETTINGS`, which `reth_provider` does not re-export.
pub(crate) const RETH_SETTINGS_KEY: &str = "storage_settings";

/// Whether this build can run the v2 layout: only on tn-lmdb.
const V2_SUPPORTED: bool = !cfg!(feature = "mdbx");

/// The storage layout of a reth execution datadir.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageLayout {
    /// reth's v1 layout: change sets, senders and history indices in database tables, plain state
    /// beside hashed state.
    V1,
    /// reth's v2 layout without RocksDB: change sets and senders in static files, hashed state
    /// only. tn-lmdb only.
    V2,
}

impl StorageLayout {
    /// The layout this build gives a new datadir: v1 on the MDBX build (v2 is tn-lmdb only), and
    /// [`NEW_DATADIR_LAYOUT_TN_LMDB`] on the tn-lmdb build.
    pub const fn for_new_datadirs() -> Self {
        if V2_SUPPORTED {
            NEW_DATADIR_LAYOUT_TN_LMDB
        } else {
            Self::V1
        }
    }

    /// reth's settings for this layout.
    pub const fn settings(self) -> StorageSettings {
        match self {
            Self::V1 => StorageSettings::v1(),
            Self::V2 => StorageSettings::v2(),
        }
    }

    /// Whether this build can run the layout: v2 is tn-lmdb only.
    pub const fn is_supported(self) -> bool {
        match self {
            Self::V1 => true,
            Self::V2 => V2_SUPPORTED,
        }
    }

    /// The layout of reth's settings.
    pub const fn of(settings: StorageSettings) -> Self {
        if settings.is_v2() {
            Self::V2
        } else {
            Self::V1
        }
    }
}

impl fmt::Display for StorageLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
        })
    }
}

/// TN's layout marker, the JSON value of [`TN_LAYOUT_KEY`] in reth's `Metadata` table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LayoutMarker {
    /// The format version ([`TN_LAYOUT_VERSION`]).
    pub(crate) version: u32,
    /// The layout, which must agree with reth's `storage_settings`.
    pub(crate) layout: StorageLayout,
}

impl LayoutMarker {
    /// This binary's marker for `layout`.
    const fn current(layout: StorageLayout) -> Self {
        Self { version: TN_LAYOUT_VERSION, layout }
    }

    /// The marker's bytes in the `Metadata` table.
    pub(crate) fn to_bytes(self) -> eyre::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self)?)
    }
}

/// Settle the layout of the datadir `factory` opens, as the module docs say, and return it.
///
/// `requested` is the layout for a new datadir. On return the factory's cached settings are the
/// datadir's layout.
pub(super) fn settle(
    factory: &ProviderFactory<TelcoinNode>,
    requested: StorageLayout,
) -> eyre::Result<StorageLayout> {
    let (stored, marker, db_tip) = {
        let provider = factory.provider()?;
        (
            stored_layout(provider.get_metadata(RETH_SETTINGS_KEY)?)?,
            stored_marker(provider.get_metadata(TN_LAYOUT_KEY)?)?,
            provider.get_stage_checkpoint(StageId::Headers)?.map(|c| c.block_number),
        )
    };
    let static_files = factory.static_file_provider();
    let sf_tip = static_files.get_highest_static_file_block(StaticFileSegment::Headers);

    let layout = match (db_tip, sf_tip) {
        (None, Some(_)) if stored.is_none() && marker.is_some() && holds_only_genesis(factory)? => {
            remove_unfinished_genesis(factory)?;
            create(factory, requested)?
        }
        (None, Some(sf_tip)) => eyre::bail!(
            "the reth database holds no chain, but its static files ({}) hold headers up to \
             block {sf_tip}: the database was removed or replaced, or the datadir's creation \
             stopped before genesis committed. Opening it would prune the static files down to \
             the empty database. Restore the datadir's `db` directory, or move `db` and \
             `static_files` aside together (`telcoin-network db reset-execution`) to rebuild \
             the execution database from genesis",
            static_files.directory().display(),
        ),
        (None, None) => create(factory, requested)?,
        (Some(_), _) => existing(factory, stored, marker, requested)?,
    };
    factory.set_storage_settings_cache(layout.settings());
    Ok(layout)
}

/// Whether the datadir's static files hold this chain's genesis block and nothing past it: every
/// segment ends at the genesis block or is empty, and the genesis header in them is the chain's.
///
/// With TN's marker and no `storage_settings` row in a database that holds no chain, that is a
/// creation stopped inside genesis's commit: the marker commits first ([`create`]), then genesis
/// finalizes its static files, then commits the database, writing reth's row and the checkpoints
/// with the chain. The stop leaves only what genesis derives from the chain spec, so
/// [`remove_unfinished_genesis`] can remove it and the open create the datadir again. Anything else
/// beside an empty database is refused.
fn holds_only_genesis(factory: &ProviderFactory<TelcoinNode>) -> eyre::Result<bool> {
    let chain = factory.chain_spec();
    let genesis = chain.genesis_header().number;
    let static_files = factory.static_file_provider();
    let past_genesis = StaticFileSegment::iter().any(|segment| {
        static_files.get_highest_static_file_block(segment).is_some_and(|block| block > genesis)
    });
    if past_genesis {
        return Ok(false);
    }
    Ok(static_files.block_hash(genesis)? == Some(chain.genesis_hash()))
}

/// Remove the static files a creation stopped inside genesis's commit left
/// ([`holds_only_genesis`]), so genesis can write them again. The headers go last: until they are
/// gone the next open finds the same state and removes the rest again.
fn remove_unfinished_genesis(factory: &ProviderFactory<TelcoinNode>) -> eyre::Result<()> {
    let static_files = factory.static_file_provider();
    warn!(target: "tn::reth", dir = %static_files.directory().display(), "the reth datadir's creation stopped before genesis committed: removing its genesis static files to create it again");
    let headers_last = StaticFileSegment::iter()
        .filter(|segment| *segment != StaticFileSegment::Headers)
        .chain([StaticFileSegment::Headers]);
    for segment in headers_last {
        static_files.delete_segment(segment)?;
    }
    Ok(())
}

/// The layout a `storage_settings` row records, or `None` without the row. A row that does not
/// parse is an error: reth's own read takes it for a missing row (v1), which on a v2 datadir would
/// misread every change set and history index.
pub(crate) fn stored_layout(row: Option<Vec<u8>>) -> eyre::Result<Option<StorageLayout>> {
    row.map(|bytes| {
        serde_json::from_slice::<StorageSettings>(&bytes).map(StorageLayout::of).map_err(|e| {
            eyre::eyre!(
                "the reth database's storage_settings row does not parse ({e}): {:?}; refusing to \
                 guess its storage layout",
                String::from_utf8_lossy(&bytes)
            )
        })
    })
    .transpose()
}

/// TN's layout marker, or `None` without the row. A row that does not parse, or of a format
/// version this binary does not know, is an error.
pub(crate) fn stored_marker(row: Option<Vec<u8>>) -> eyre::Result<Option<LayoutMarker>> {
    let Some(bytes) = row else { return Ok(None) };
    let marker = serde_json::from_slice::<LayoutMarker>(&bytes).map_err(|e| {
        eyre::eyre!(
            "the reth database's {TN_LAYOUT_KEY} row does not parse ({e}): {:?}; a newer binary \
             may have written it. Run the binary that wrote this datadir, or rebuild the execution \
             database (`telcoin-network db reset-execution`)",
            String::from_utf8_lossy(&bytes)
        )
    })?;
    if marker.version != TN_LAYOUT_VERSION {
        eyre::bail!(
            "the reth database is in TN's storage format version {} ({TN_LAYOUT_KEY}), and this \
             binary reads only version {TN_LAYOUT_VERSION}: run the binary that wrote this datadir, \
             or rebuild the execution database (`telcoin-network db reset-execution`)",
            marker.version,
        );
    }
    Ok(Some(marker))
}

/// Refuse `layout` for `what` (a new datadir, or this datadir) on a build without v2.
fn refuse_unsupported(layout: StorageLayout, what: &str) -> eyre::Result<()> {
    if layout == StorageLayout::V2 && !V2_SUPPORTED {
        eyre::bail!(
            "storage layout v2 for {what} needs the tn-lmdb backend, and this build runs reth on \
             MDBX: run a build without the `mdbx` feature"
        );
    }
    Ok(())
}

/// A new datadir: record `layout` in the marker, before genesis writes reth's row with it.
///
/// The marker commits first, in its own transaction. A stop before genesis commits leaves a
/// database without a chain, which the next open takes for a new datadir again and overwrites.
fn create(
    factory: &ProviderFactory<TelcoinNode>,
    layout: StorageLayout,
) -> eyre::Result<StorageLayout> {
    refuse_unsupported(layout, "a new datadir")?;
    let provider = factory.database_provider_rw()?;
    provider.write_metadata(TN_LAYOUT_KEY, LayoutMarker::current(layout).to_bytes()?)?;
    provider.commit()?;
    info!(target: "tn::reth", %layout, version = TN_LAYOUT_VERSION, "new reth datadir: storage layout");
    Ok(layout)
}

/// An existing datadir: its stored layout, checked against TN's marker and its static files.
fn existing(
    factory: &ProviderFactory<TelcoinNode>,
    stored: Option<StorageLayout>,
    marker: Option<LayoutMarker>,
    requested: StorageLayout,
) -> eyre::Result<StorageLayout> {
    let layout = stored.unwrap_or(StorageLayout::V1);
    refuse_unsupported(layout, "this datadir")?;
    match marker {
        Some(marker) if marker.layout != layout => eyre::bail!(
            "the reth database's storage_settings say layout {layout}, but TN's marker \
             ({TN_LAYOUT_KEY}) says {}: the settings were changed without converting the data, \
             which would misread every history index. Restore the datadir from a backup, or \
             rebuild the execution database (`telcoin-network db reset-execution`)",
            marker.layout,
        ),
        Some(_) => {}
        None if layout == StorageLayout::V2 => eyre::bail!(
            "the reth database is in storage layout v2 without TN's marker ({TN_LAYOUT_KEY}): no \
             TN build wrote it, and an unfinished v2 write path may have left it without its \
             history indices. Rebuild the execution database (`telcoin-network db \
             reset-execution`)"
        ),
        None => {
            // A datadir from before the marker: every one is v1.
            let provider = factory.database_provider_rw()?;
            provider.write_metadata(TN_LAYOUT_KEY, LayoutMarker::current(layout).to_bytes()?)?;
            provider.commit()?;
            info!(target: "tn::reth", %layout, version = TN_LAYOUT_VERSION, "recorded TN's storage layout marker in an existing reth datadir");
        }
    }
    match layout {
        StorageLayout::V1 => refuse_v2_segments(factory)?,
        StorageLayout::V2 => refuse_v1_static_files(factory)?,
    }
    if layout == requested {
        info!(target: "tn::reth", %layout, "reth datadir: storage layout");
    } else {
        warn!(target: "tn::reth", %layout, new_datadirs = %requested, "this reth datadir keeps its storage layout: `telcoin-network db reset-execution` rebuilds it in the layout new datadirs get");
    }
    Ok(layout)
}

/// Refuse the static files of a v1 datadir that hold a segment only v2 writes: they are another
/// datadir's (a half-done swap of `db` and `static_files`), and v1's consistency check skips
/// those segments, so it would not notice.
fn refuse_v2_segments(factory: &ProviderFactory<TelcoinNode>) -> eyre::Result<()> {
    let static_files = factory.static_file_provider();
    let found: Vec<String> = [
        StaticFileSegment::TransactionSenders,
        StaticFileSegment::AccountChangeSets,
        StaticFileSegment::StorageChangeSets,
    ]
    .into_iter()
    .filter_map(|segment| {
        static_files
            .get_highest_static_file_block(segment)
            .map(|block| format!("{segment} up to block {block}"))
    })
    .collect();
    if !found.is_empty() {
        eyre::bail!(
            "the reth database is in storage layout v1, but its static files ({}) hold segments \
             only v2 writes ({}): they belong to another datadir. Restore this datadir's own \
             `static_files`, or swap `db` and `static_files` together",
            static_files.directory().display(),
            found.join(", "),
        );
    }
    Ok(())
}

/// Refuse the static files of a v2 datadir that hold headers but no transaction senders segment,
/// which v2's genesis always writes (`init_genesis_with_settings`), and before the database
/// commit: they are a v1 datadir's (a half-done swap of `db` and `static_files`), and v2's
/// consistency check would prune their headers and transactions to this database's checkpoints.
fn refuse_v1_static_files(factory: &ProviderFactory<TelcoinNode>) -> eyre::Result<()> {
    let static_files = factory.static_file_provider();
    let headers = static_files.get_highest_static_file_block(StaticFileSegment::Headers);
    let senders = static_files.get_highest_static_file_block(StaticFileSegment::TransactionSenders);
    if let (Some(headers), None) = (headers, senders) {
        eyre::bail!(
            "the reth database is in storage layout v2, but its static files ({}) hold headers up \
             to block {headers} and no {} segment, which every v2 datadir has from genesis: they \
             belong to a v1 datadir. Restore this datadir's own `static_files`, or swap `db` and \
             `static_files` together",
            static_files.directory().display(),
            StaticFileSegment::TransactionSenders,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        payload::TNPayload,
        test_utils::{
            consensus_output_for_tests, execute_payload_and_update_canonical_chain,
            test_genesis_with_consensus_registry,
        },
        RethChainSpec, RethConfig, RethEnv,
    };
    use reth_db::{tables, transaction::DbTxMut as _};
    use reth_provider::StaticFileWriter as _;
    use std::{path::Path, sync::Arc};
    use tempfile::TempDir;
    use tn_types::TaskManager;

    /// A chain spec with a consensus registry, so a test can execute a block on it.
    fn chain() -> Arc<RethChainSpec> {
        Arc::new(test_genesis_with_consensus_registry(4).into())
    }

    /// Open the datadir `path` with `layout` for a new datadir, run `f` on the env, then close it.
    fn with_env<T>(
        chain: &Arc<RethChainSpec>,
        path: &Path,
        layout: StorageLayout,
        f: impl FnOnce(&RethEnv) -> eyre::Result<T>,
    ) -> eyre::Result<T> {
        let tm = TaskManager::new("layout test");
        let env = RethEnv::new_for_temp_chain_with_layout(chain.clone(), path, &tm, layout)?;
        f(&env)
    }

    /// Open the datadir `path` with `layout` for a new datadir, and return the open's error.
    fn open_err(chain: &Arc<RethChainSpec>, path: &Path, layout: StorageLayout) -> String {
        match with_env(chain, path, layout, |_| Ok(())) {
            Ok(()) => panic!("the open of {} was not refused", path.display()),
            Err(e) => format!("{e:#}"),
        }
    }

    /// The datadir's layout as reth's row, TN's marker and the factory's cache say it.
    fn recorded(env: &RethEnv) -> eyre::Result<(Option<StorageLayout>, Option<LayoutMarker>)> {
        let provider = env.blockchain_provider().database_provider_ro()?;
        let cached = StorageLayout::of(provider.cached_storage_settings());
        let stored = stored_layout(provider.get_metadata(RETH_SETTINGS_KEY)?)?;
        let marker = stored_marker(provider.get_metadata(TN_LAYOUT_KEY)?)?;
        assert_eq!(Some(cached), stored.or(Some(StorageLayout::V1)), "the cache is the stored row");
        Ok((stored, marker))
    }

    /// Write the `Metadata` row `key` as `value`, or delete it with `None`.
    fn set_row(env: &RethEnv, key: &str, value: Option<Vec<u8>>) -> eyre::Result<()> {
        let provider = env.blockchain_provider().database_provider_rw()?;
        match value {
            Some(value) => provider.write_metadata(key, value)?,
            None => {
                provider.tx_ref().delete::<tables::Metadata>(key.to_string(), None)?;
            }
        }
        provider.commit()?;
        Ok(())
    }

    /// The names and sizes of the files under `dir`, sorted.
    fn listing(dir: &Path) -> Vec<(String, u64)> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .expect("list the directory")
            .map(|entry| {
                let entry = entry.expect("a directory entry");
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    entry.metadata().expect("its metadata").len(),
                )
            })
            .collect();
        files.sort();
        files
    }

    /// The defaults, pinned: the default (tn-lmdb) build gives a new datadir v2, and the MDBX
    /// build v1, both by [`StorageLayout::for_new_datadirs`] and by the config the node builds
    /// from its parsed command ([`RethConfig::new`]). A new datadir opened the node's way
    /// ([`RethEnv::new_database`], then [`RethEnv::new`]) records it in reth's row and TN's
    /// marker. The node's path never reads the temp chains' test override
    /// (`TEST_NEW_DATADIR_LAYOUT_VAR`), so this passes in the lane run that sets it to v1.
    #[tokio::test]
    async fn the_default_build_gives_new_datadirs_v2() -> eyre::Result<()> {
        use clap::Parser as _;

        let expected = if cfg!(feature = "mdbx") { StorageLayout::V1 } else { StorageLayout::V2 };
        assert_eq!(StorageLayout::for_new_datadirs(), expected, "for_new_datadirs");
        let tmp = TempDir::new()?;
        let command = crate::RethCommand::try_parse_from(["tn-reth"])?;
        let config = RethConfig::new(command, None, tmp.path(), true, chain());
        assert_eq!(config.new_datadir_layout(), expected, "the node's config");
        let tm = TaskManager::new("default layout");
        let database = RethEnv::new_database(&config, tmp.path().join("db"))?;
        let env = RethEnv::new(&config, &tm, database, None, Default::default())?;
        let (stored, marker) = recorded(&env)?;
        assert_eq!(stored, Some(expected), "reth's row");
        assert_eq!(marker, Some(LayoutMarker::current(expected)), "TN's marker");
        Ok(())
    }

    /// A new temp chain datadir gets the temp chains' layout for new datadirs (the default's, or
    /// the test override's), in reth's row and TN's marker, and every reopen keeps it.
    #[tokio::test]
    async fn a_new_datadir_gets_the_layout_for_new_datadirs() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let layout = crate::env::genesis::temp_chain_new_datadir_layout()?;
        match std::env::var(crate::env::genesis::TEST_NEW_DATADIR_LAYOUT_VAR) {
            // the lane run that sets the override: temp chains must get what it names
            Ok(named) => assert_eq!(layout.to_string(), named, "the test override"),
            Err(_) => assert_eq!(layout, StorageLayout::for_new_datadirs(), "the default"),
        }
        if cfg!(feature = "mdbx") {
            assert_eq!(layout, StorageLayout::V1, "the MDBX build gives v1");
        }
        for run in ["new", "reopened"] {
            let tm = TaskManager::new(run);
            let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None)?;
            let (stored, marker) = recorded(&env)?;
            assert_eq!(stored, Some(layout), "{run}");
            assert_eq!(marker, Some(LayoutMarker::current(layout)), "{run}");
        }
        Ok(())
    }

    /// A new datadir asked for v1 is v1 on either build, and a reopen asked for v2 keeps v1.
    #[tokio::test]
    async fn an_existing_datadir_keeps_its_layout_whatever_is_requested() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        for requested in [StorageLayout::V1, StorageLayout::V2] {
            let (stored, marker) = with_env(&chain, tmp.path(), requested, recorded)?;
            assert_eq!(stored, Some(StorageLayout::V1), "asked for {requested}");
            assert_eq!(marker, Some(LayoutMarker::current(StorageLayout::V1)));
        }
        Ok(())
    }

    /// The tn-lmdb build creates a v2 datadir, and a reopen asked for v1 keeps v2.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn tn_lmdb_creates_a_v2_datadir_and_keeps_it() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        for requested in [StorageLayout::V2, StorageLayout::V1] {
            let (stored, marker) = with_env(&chain, tmp.path(), requested, recorded)?;
            assert_eq!(stored, Some(StorageLayout::V2), "asked for {requested}");
            assert_eq!(marker, Some(LayoutMarker::current(StorageLayout::V2)));
        }
        Ok(())
    }

    /// The MDBX build refuses to create a v2 datadir, and leaves it new: a v1 open then creates
    /// it as v1.
    #[cfg(feature = "mdbx")]
    #[tokio::test]
    async fn mdbx_refuses_to_create_a_v2_datadir() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V2);
        assert!(err.contains("needs the tn-lmdb backend"), "{err}");
        let (stored, _) = with_env(&chain, tmp.path(), StorageLayout::V1, recorded)?;
        assert_eq!(stored, Some(StorageLayout::V1));
        Ok(())
    }

    /// The MDBX build refuses a datadir whose rows say v2.
    #[cfg(feature = "mdbx")]
    #[tokio::test]
    async fn mdbx_refuses_a_v2_datadir() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            set_row(env, RETH_SETTINGS_KEY, Some(serde_json::to_vec(&StorageSettings::v2())?))?;
            set_row(env, TN_LAYOUT_KEY, Some(LayoutMarker::current(StorageLayout::V2).to_bytes()?))
        })?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V1);
        assert!(err.contains("for this datadir needs the tn-lmdb backend"), "{err}");
        Ok(())
    }

    /// A datadir from before the marker (and before reth's row) opens as v1 and gets the marker.
    #[tokio::test]
    async fn a_datadir_without_the_marker_is_v1_and_gets_it() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            set_row(env, TN_LAYOUT_KEY, None)?;
            set_row(env, RETH_SETTINGS_KEY, None)
        })?;
        let (stored, marker) = with_env(&chain, tmp.path(), StorageLayout::V2, recorded)?;
        assert_eq!(stored, None, "the open does not write reth's row into an existing datadir");
        assert_eq!(marker, Some(LayoutMarker::current(StorageLayout::V1)));
        Ok(())
    }

    /// A v2 datadir without TN's marker was not written by a TN build: refused.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn a_v2_datadir_without_the_marker_is_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V2, |env| set_row(env, TN_LAYOUT_KEY, None))?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V2);
        assert!(err.contains("in storage layout v2 without TN's marker"), "{err}");
        Ok(())
    }

    /// A marker of an unknown format version, one that does not parse, and one with a field this
    /// binary does not know are all refused.
    #[tokio::test]
    async fn an_unknown_marker_is_refused() -> eyre::Result<()> {
        let chain = chain();
        let cases: [(&[u8], &str); 4] = [
            (br#"{"version":2,"layout":"v1"}"#, "format version 2"),
            (br#"{"version":0,"layout":"v1"}"#, "format version 0"),
            (br#"{"version":1,"layout":"v1","packed_trie_keys":true}"#, "does not parse"),
            (b"not json", "does not parse"),
        ];
        for (marker, expected) in cases {
            let tmp = TempDir::new()?;
            with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
                set_row(env, TN_LAYOUT_KEY, Some(marker.to_vec()))
            })?;
            let err = open_err(&chain, tmp.path(), StorageLayout::V1);
            assert!(err.contains(expected), "{}: {err}", String::from_utf8_lossy(marker));
        }
        Ok(())
    }

    /// A marker whose layout disagrees with reth's row is refused: the row was flipped without
    /// converting the data.
    #[tokio::test]
    async fn a_marker_disagreeing_with_the_settings_is_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            set_row(env, TN_LAYOUT_KEY, Some(LayoutMarker::current(StorageLayout::V2).to_bytes()?))
        })?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V1);
        assert!(err.contains("storage_settings say layout v1, but TN's marker"), "{err}");
        Ok(())
    }

    /// A `storage_settings` row that does not parse is refused, where reth's own read would take
    /// it for v1.
    #[tokio::test]
    async fn an_unparseable_settings_row_is_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            set_row(env, RETH_SETTINGS_KEY, Some(b"{\"storage_v2\":\"yes\"}".to_vec()))
        })?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V1);
        assert!(err.contains("storage_settings row does not parse"), "{err}");
        Ok(())
    }

    /// Static files holding a chain beside a database that holds none (the database removed) are
    /// refused, and left as they were: reth's consistency check would prune them to genesis.
    #[tokio::test]
    async fn static_files_beside_an_empty_database_are_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            let output = consensus_output_for_tests(2, 0, 1, false);
            let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
            execute_payload_and_update_canonical_chain(env, payload, vec![])?;
            assert_eq!(env.last_block_number()?, 1);
            Ok(())
        })?;
        // a temp chain keeps its database files in the datadir's root, beside `static_files`
        let static_files = tmp.path().join("static_files");
        let before = listing(&static_files);
        for entry in std::fs::read_dir(tmp.path())? {
            let path = entry?.path();
            if path != static_files {
                if path.is_dir() {
                    std::fs::remove_dir_all(&path)?;
                } else {
                    std::fs::remove_file(&path)?;
                }
            }
        }

        let err = open_err(&chain, tmp.path(), StorageLayout::V1);
        assert!(err.contains("holds no chain, but its static files"), "{err}");
        assert!(err.contains("headers up to block 1"), "{err}");
        assert_eq!(listing(&static_files), before, "the static files are left as they were");
        Ok(())
    }

    /// The datadir a creation stopped inside genesis's commit leaves in `path`: a database holding
    /// only TN's marker (it commits before genesis), beside the static files genesis finalized
    /// before its database commit. The static files are copied from a datadir of `chain` with
    /// `blocks` executed blocks (0: genesis only), and the marker is written unless `marker` is
    /// false. Returns the static files' listing.
    fn stopped_inside_genesis(
        chain: &Arc<RethChainSpec>,
        path: &Path,
        blocks: u64,
        marker: bool,
    ) -> eyre::Result<Vec<(String, u64)>> {
        use reth_db::{transaction::DbTx as _, Database as _};

        let layout = crate::env::genesis::temp_chain_new_datadir_layout()?;
        let source = path.with_extension("source");
        with_env(chain, &source, layout, |env| {
            let mut parent = chain.sealed_genesis_header();
            for number in 1..=blocks {
                let output = consensus_output_for_tests(2 * number as u32, 0, number, false);
                let payload = TNPayload::new_for_test(parent, &output);
                let executed = execute_payload_and_update_canonical_chain(env, payload, vec![])?;
                parent = executed.recovered_block.clone_sealed_header();
            }
            Ok(())
        })?;
        let config = RethConfig::from_node_config(reth_node_core::node_config::NodeConfig {
            chain: chain.clone(),
            db: crate::env::database::temp_chain_db_args(),
            ..Default::default()
        });
        let database = RethEnv::new_database(&config, path)?;
        if marker {
            let tx = database.tx_mut()?;
            tx.put::<tables::Metadata>(
                TN_LAYOUT_KEY.to_string(),
                LayoutMarker::current(layout).to_bytes()?,
            )?;
            tx.commit()?;
        }
        drop(database);
        copy_tree(&source.join("static_files"), &path.join("static_files"), &[])?;
        Ok(listing(&path.join("static_files")))
    }

    /// A creation stopped inside genesis's commit (TN's marker, no `storage_settings` row, the
    /// static files holding only genesis) is created again at the next open: its genesis static
    /// files are removed and written again, and the datadir runs on.
    #[tokio::test]
    async fn a_creation_stopped_inside_genesis_is_created_again() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let path = tmp.path().join("datadir");
        stopped_inside_genesis(&chain, &path, 0, true)?;
        let layout = crate::env::genesis::temp_chain_new_datadir_layout()?;

        with_env(&chain, &path, layout, |env| {
            assert_eq!(recorded(env)?, (Some(layout), Some(LayoutMarker::current(layout))));
            assert_eq!(env.last_block_number()?, 0);
            let output = consensus_output_for_tests(2, 0, 1, false);
            let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &output);
            execute_payload_and_update_canonical_chain(env, payload, vec![])?;
            assert_eq!(env.last_block_number()?, 1);
            Ok(())
        })?;
        with_env(&chain, &path, layout, |env| {
            assert_eq!(env.last_block_number()?, 1, "a normal reopen");
            Ok(())
        })
    }

    /// Only that state is healed. Beside a database that holds no chain the open refuses, and
    /// leaves the static files as they were, when the database has no marker (no TN build was
    /// creating it: the database was replaced) or the static files hold a block past genesis.
    #[tokio::test]
    async fn static_files_past_genesis_or_without_the_marker_stay_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let layout = crate::env::genesis::temp_chain_new_datadir_layout()?;
        for (name, blocks, marker) in [("no-marker", 0, false), ("past-genesis", 1, true)] {
            let path = tmp.path().join(name);
            let before = stopped_inside_genesis(&chain, &path, blocks, marker)?;
            let err = open_err(&chain, &path, layout);
            assert!(err.contains("holds no chain, but its static files"), "{name}: {err}");
            assert!(err.contains(&format!("headers up to block {blocks}")), "{name}: {err}");
            assert_eq!(listing(&path.join("static_files")), before, "{name}: left as they were");
        }
        Ok(())
    }

    /// A v1 datadir whose static files hold a segment only v2 writes (another datadir's static
    /// files) is refused.
    #[tokio::test]
    async fn v2_segments_beside_a_v1_database_are_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        with_env(&chain, tmp.path(), StorageLayout::V1, |env| {
            let static_files = env.blockchain_provider().static_file_provider();
            let mut writer = static_files.latest_writer(StaticFileSegment::AccountChangeSets)?;
            writer.append_account_changeset(Vec::new(), 0)?;
            writer.commit()?;
            Ok(())
        })?;
        let err = open_err(&chain, tmp.path(), StorageLayout::V1);
        assert!(err.contains("hold segments only v2 writes"), "{err}");
        assert!(err.contains("AccountChangeSets up to block 0"), "{err}");
        Ok(())
    }

    /// Copy the directory tree `from` into `to`, leaving out the entries named in `skip`.
    fn copy_tree(from: &Path, to: &Path, skip: &[&str]) -> eyre::Result<()> {
        std::fs::create_dir_all(to)?;
        for entry in std::fs::read_dir(from)? {
            let entry = entry?;
            let name = entry.file_name();
            if skip.iter().any(|s| name == *s) {
                continue;
            }
            if entry.file_type()?.is_dir() {
                copy_tree(&entry.path(), &to.join(&name), &[])?;
            } else {
                std::fs::copy(entry.path(), to.join(&name))?;
            }
        }
        Ok(())
    }

    /// A v2 database beside a v1 datadir's static files (the other half-done swap) is refused
    /// before reth's consistency check, which would prune the static files' two blocks to the
    /// database's one: the static files are left as they were.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn v1_static_files_beside_a_v2_database_are_refused() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let (v1, v2, swapped) =
            (tmp.path().join("v1"), tmp.path().join("v2"), tmp.path().join("swapped"));
        for (path, layout, blocks) in [(&v1, StorageLayout::V1, 2), (&v2, StorageLayout::V2, 1)] {
            with_env(&chain, path, layout, |env| {
                let mut parent = chain.sealed_genesis_header();
                for number in 1..=blocks {
                    let output = consensus_output_for_tests(2 * number as u32, 0, number, false);
                    let payload = TNPayload::new_for_test(parent, &output);
                    let executed =
                        execute_payload_and_update_canonical_chain(env, payload, vec![])?;
                    parent = executed.recovered_block.clone_sealed_header();
                }
                assert_eq!(env.last_block_number()?, blocks);
                Ok(())
            })?;
        }
        // a temp chain keeps its database files in the datadir's root, beside `static_files`
        copy_tree(&v2, &swapped, &["static_files"])?;
        copy_tree(&v1.join("static_files"), &swapped.join("static_files"), &[])?;
        let before = listing(&swapped.join("static_files"));

        let err = open_err(&chain, &swapped, StorageLayout::V2);
        assert!(err.contains("storage layout v2, but its static files"), "{err}");
        assert!(
            err.contains("headers up to block 2") && err.contains("TransactionSenders"),
            "{err}"
        );
        assert_eq!(listing(&swapped.join("static_files")), before, "left as they were");
        // the v2 datadir's own static files open
        with_env(&chain, &v2, StorageLayout::V2, |env| {
            assert_eq!(env.last_block_number()?, 1);
            Ok(())
        })
    }
}
