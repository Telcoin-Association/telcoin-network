//! The storage layout of TN's reth execution database, and the guard that opens each datadir in
//! the layout it was created with.
//!
//! reth has two layouts (`StorageSettings`). v1 keeps change sets, transaction senders and the
//! history indices in database tables, and plain state beside hashed state. v2 moves change sets
//! and senders into static files and keeps only hashed state; its storage change sets and history
//! are keyed by the plain slot, as v1's are (vendor/reth CHANGES.md TN-0112). A datadir's layout is
//! chosen when it is created and recorded in reth's `Metadata` table (`storage_settings`; a missing
//! row reads as v1). Nothing converts a datadir from one layout to the other, and flipping the row
//! alone would make every historical read wrong.
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
//! - The marker's version also records where the datadir keeps its transactions ([`TxBodies`]):
//!   version 1 ([`LAYOUT_VERSION_INLINE_TXS`]) stores each one in the `Transactions` static files;
//!   version 2 ([`LAYOUT_VERSION_TX_POINTERS`], layout v2 only) stores its position in the
//!   consensus batch its block executed, every `Transactions` jar with tagged values (`NippyJar`
//!   version 3). A new datadir gets version 2 when its layout is v2 and pointers are requested
//!   ([`crate::RethConfig::new_datadir_tx_bodies`], by default [`TxBodies::for_new_datadirs`]); an
//!   existing one keeps its version, and [`settle`] sets the static files' jar policy
//!   (`set_tx_pointer_jars`) from it at every open.
//!
//! Refused, before reth's consistency check or anything after it touches the datadir (the
//! database's own open runs first and may already have created or migrated `db/`):
//! - a `storage_settings` row or a marker that does not parse;
//! - a marker of a format version this binary does not know (a newer binary wrote it), or of
//!   version 2 with layout v1 (no TN build writes it);
//! - a marker whose layout disagrees with `storage_settings`;
//! - an existing v2 datadir without the marker (no TN build wrote it);
//! - a v1 datadir whose static files hold v2-only segments, or a v2 datadir whose static files hold
//!   none (another datadir's static files);
//! - a version-1 datadir holding a `Transactions` jar with tagged values, or a version-2 datadir
//!   holding one without ([`refuse_mixed_tx_jars`]: a half-done swap of `db` and `static_files`);
//! - static files holding a chain beside a database that holds none: reth's consistency check would
//!   prune them down to the empty database. The one exception is healed: a creation stopped inside
//!   genesis's commit (TN's marker, no `storage_settings` row, static files holding only this
//!   chain's genesis) is created again ([`holds_only_genesis`]).

use std::fmt;

use reth_chainspec::EthChainSpec as _;
use reth_provider::{
    providers::{JarFormat, ProviderFactory},
    BlockHashReader as _, ChainSpecProvider as _, DBProvider as _, DatabaseProviderFactory as _,
    MetadataProvider as _, MetadataWriter as _, StageCheckpointReader as _,
    StaticFileProviderFactory as _, StaticFileSegment, StorageSettings, StorageSettingsCache as _,
};
use reth_stages_types::StageId;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::traits::TelcoinNode;

/// The layout the tn-lmdb build gives a new datadir: v2, with the history indices and the
/// transaction-hash index in the database (vendor/reth CHANGES.md TN-0110..0112). The MDBX build
/// always gives v1 ([`StorageLayout::for_new_datadirs`]); an existing datadir keeps its own.
const NEW_DATADIR_LAYOUT_TN_LMDB: StorageLayout = StorageLayout::V2;

/// The `Metadata` key of TN's layout marker ([`LayoutMarker`]).
pub(crate) const TN_LAYOUT_KEY: &str = "tn_storage_layout";

/// Format version 1 of TN's reth database: reth's v1 or v2 layout on the vendored reth's table
/// formats, every transaction stored in the `Transactions` static files ([`TxBodies::Inline`]).
pub(crate) const LAYOUT_VERSION_INLINE_TXS: u32 = 1;

/// Format version 2 of TN's reth database: reth's v2 layout on tn-lmdb, every `Transactions` jar
/// with tagged values (`NippyJar` version 3), whose rows store each transaction's position in the
/// consensus batch its block executed ([`TxBodies::Pointers`]), resolved through the consensus
/// packs. No TN build writes it with layout v1.
pub(crate) const LAYOUT_VERSION_TX_POINTERS: u32 = 2;

/// The newest format version of TN's reth database, which this binary reads with every older one
/// ([`stored_marker`]) and writes for a new datadir that stores transactions by position.
///
/// A change to how TN stores any table (for example packed trie keys, or transaction hashes in a
/// separate index) takes the next version, and an open of an older version converts it or refuses
/// it.
pub(crate) const TN_LAYOUT_VERSION: u32 = LAYOUT_VERSION_TX_POINTERS;

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

/// Where a reth execution datadir keeps its transactions, fixed when the datadir is created and
/// recorded by its marker's version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxBodies {
    /// Each transaction in the `Transactions` static files ([`LAYOUT_VERSION_INLINE_TXS`]).
    Inline,
    /// Each transaction's position in the consensus batch its block executed, read back from the
    /// consensus packs ([`LAYOUT_VERSION_TX_POINTERS`]). Layout v2 only.
    Pointers,
}

impl TxBodies {
    /// What this build requests for a new datadir: [`Self::Pointers`] on the tn-lmdb build, and
    /// [`Self::Inline`] on the MDBX build, whose datadirs are layout v1 (pointers need v2).
    pub const fn for_new_datadirs() -> Self {
        if V2_SUPPORTED {
            Self::Pointers
        } else {
            Self::Inline
        }
    }

    /// The marker version that records this.
    const fn marker_version(self) -> u32 {
        match self {
            Self::Inline => LAYOUT_VERSION_INLINE_TXS,
            Self::Pointers => LAYOUT_VERSION_TX_POINTERS,
        }
    }
}

impl fmt::Display for TxBodies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Inline => "inline",
            Self::Pointers => "pointers",
        })
    }
}

/// A reth execution datadir's format: its storage layout and where it keeps its transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatadirFormat {
    /// reth's storage layout.
    pub layout: StorageLayout,
    /// Where the transactions are kept.
    pub tx_bodies: TxBodies,
}

impl DatadirFormat {
    /// The format a new datadir gets for this request: the request, except that pointers on
    /// layout v1 fall back to inline (version 2 needs layout v2).
    pub const fn for_new_datadir(self) -> Self {
        match (self.layout, self.tx_bodies) {
            (StorageLayout::V1, TxBodies::Pointers) => {
                Self { layout: StorageLayout::V1, tx_bodies: TxBodies::Inline }
            }
            _ => self,
        }
    }
}

impl fmt::Display for DatadirFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "layout {}, transactions {}", self.layout, self.tx_bodies)
    }
}

/// TN's layout marker, the JSON value of [`TN_LAYOUT_KEY`] in reth's `Metadata` table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LayoutMarker {
    /// The format version: [`LAYOUT_VERSION_INLINE_TXS`] or [`LAYOUT_VERSION_TX_POINTERS`].
    pub(crate) version: u32,
    /// The layout, which must agree with reth's `storage_settings`.
    pub(crate) layout: StorageLayout,
}

impl LayoutMarker {
    /// The marker that records `format`.
    pub(crate) const fn of(format: DatadirFormat) -> Self {
        Self { version: format.tx_bodies.marker_version(), layout: format.layout }
    }

    /// Where the datadir keeps its transactions, by the marker's version. Only a marker
    /// [`stored_marker`] accepted is asked.
    pub(crate) const fn tx_bodies(self) -> TxBodies {
        if self.version == LAYOUT_VERSION_TX_POINTERS {
            TxBodies::Pointers
        } else {
            TxBodies::Inline
        }
    }

    /// The marker's bytes in the `Metadata` table.
    pub(crate) fn to_bytes(self) -> eyre::Result<Vec<u8>> {
        Ok(serde_json::to_vec(&self)?)
    }
}

/// Settle the format of the datadir `factory` opens, as the module docs say, and return it.
///
/// `requested` is the format for a new datadir. On return the factory's cached settings are the
/// datadir's layout, and its static files create `Transactions` jars with tagged values exactly
/// when the datadir stores transactions by position.
pub(super) fn settle(
    factory: &ProviderFactory<TelcoinNode>,
    requested: DatadirFormat,
) -> eyre::Result<DatadirFormat> {
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

    let format = match (db_tip, sf_tip) {
        (None, Some(_)) if stored.is_none() && marker.is_some() && holds_only_genesis(factory)? => {
            remove_unfinished_genesis(factory)?;
            create(factory, requested)?
        }
        (None, Some(sf_tip)) => eyre::bail!(
            "the reth database holds no chain, but its static files ({}) hold headers up to \
             block {sf_tip}: the database was removed or replaced, or the datadir's creation \
             stopped before genesis committed. Opening it would prune the static files down to \
             the empty database. Restore the datadir's `db` directory whole (this start may \
             have created an empty one: remove it first, never copy a backup into it), or move \
             `db` and `static_files` aside together (`telcoin-network db reset-execution`) to \
             rebuild the execution database from genesis",
            static_files.directory().display(),
        ),
        (None, None) => create(factory, requested)?,
        (Some(_), _) => existing(factory, stored, marker, requested)?,
    };
    factory.set_storage_settings_cache(format.layout.settings());
    // before genesis writes a new datadir's first jar, and at every open of an existing one
    static_files.set_tx_pointer_jars(format.tx_bodies == TxBodies::Pointers);
    Ok(format)
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

/// TN's layout marker, or `None` without the row. A row that does not parse, of a format version
/// this binary does not know, or of version 2 with layout v1 is an error.
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
    match (marker.version, marker.layout) {
        (LAYOUT_VERSION_INLINE_TXS, _) | (LAYOUT_VERSION_TX_POINTERS, StorageLayout::V2) => {}
        (LAYOUT_VERSION_TX_POINTERS, StorageLayout::V1) => eyre::bail!(
            "the reth database is in TN's storage format version {LAYOUT_VERSION_TX_POINTERS} \
             ({TN_LAYOUT_KEY}) with layout v1, which no TN build writes: version \
             {LAYOUT_VERSION_TX_POINTERS} needs layout v2. Restore the datadir from a backup, or \
             rebuild the execution database (`telcoin-network db reset-execution`)"
        ),
        (version, _) => eyre::bail!(
            "the reth database is in TN's storage format version {version} ({TN_LAYOUT_KEY}), and \
             this binary reads versions {LAYOUT_VERSION_INLINE_TXS} to {TN_LAYOUT_VERSION}: a newer \
             binary may have written it. Run the binary that wrote this datadir, or rebuild the \
             execution database (`telcoin-network db reset-execution`)"
        ),
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

/// A new datadir: record the format `requested` gives it ([`DatadirFormat::for_new_datadir`]) in
/// the marker, before genesis writes reth's row with its layout.
///
/// The marker commits first, in its own transaction. A stop before genesis commits leaves a
/// database without a chain, which the next open takes for a new datadir again and overwrites.
fn create(
    factory: &ProviderFactory<TelcoinNode>,
    requested: DatadirFormat,
) -> eyre::Result<DatadirFormat> {
    refuse_unsupported(requested.layout, "a new datadir")?;
    let format = requested.for_new_datadir();
    if format != requested {
        info!(target: "tn::reth", layout = %format.layout, "a new reth datadir in storage layout v1 stores its transactions inline: storing them by batch position needs layout v2");
    }
    let marker = LayoutMarker::of(format);
    let provider = factory.database_provider_rw()?;
    provider.write_metadata(TN_LAYOUT_KEY, marker.to_bytes()?)?;
    provider.commit()?;
    info!(target: "tn::reth", layout = %format.layout, tx_bodies = %format.tx_bodies, version = marker.version, "new reth datadir: storage format");
    Ok(format)
}

/// An existing datadir: its stored layout and TN's marker's version, checked against each other
/// and against its static files.
fn existing(
    factory: &ProviderFactory<TelcoinNode>,
    stored: Option<StorageLayout>,
    marker: Option<LayoutMarker>,
    requested: DatadirFormat,
) -> eyre::Result<DatadirFormat> {
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
            // A datadir from before the marker: every one is v1 and stores transactions inline.
            let marker = LayoutMarker::of(DatadirFormat { layout, tx_bodies: TxBodies::Inline });
            let provider = factory.database_provider_rw()?;
            provider.write_metadata(TN_LAYOUT_KEY, marker.to_bytes()?)?;
            provider.commit()?;
            info!(target: "tn::reth", %layout, version = marker.version, "recorded TN's storage layout marker in an existing reth datadir");
        }
    }
    let format = DatadirFormat {
        layout,
        tx_bodies: marker.map_or(TxBodies::Inline, LayoutMarker::tx_bodies),
    };
    match layout {
        StorageLayout::V1 => refuse_v2_segments(factory)?,
        StorageLayout::V2 => refuse_v1_static_files(factory)?,
    }
    refuse_mixed_tx_jars(factory, format.tx_bodies)?;
    let new_datadirs = requested.for_new_datadir();
    if format == new_datadirs {
        info!(target: "tn::reth", layout = %format.layout, tx_bodies = %format.tx_bodies, "reth datadir: storage format");
    } else {
        warn!(target: "tn::reth", %format, %new_datadirs, "this reth datadir keeps its storage format: `telcoin-network db reset-execution` rebuilds it in the format new datadirs get");
    }
    Ok(format)
}

/// Refuse a datadir whose `Transactions` jars do not match where its marker says it keeps its
/// transactions ([`TxBodies`]): a version-1 datadir holding a jar with tagged values, written by
/// a datadir whose transactions live in consensus packs, or a version-2 datadir holding a jar
/// without them. Either is a half-done swap of `db` and `static_files`, and reading on would
/// misread its transactions. Reads each jar's configuration, nothing else.
fn refuse_mixed_tx_jars(
    factory: &ProviderFactory<TelcoinNode>,
    tx_bodies: TxBodies,
) -> eyre::Result<()> {
    let static_files = factory.static_file_provider();
    let foreign: Vec<String> = static_files
        .jar_formats(StaticFileSegment::Transactions)?
        .into_iter()
        .filter(|(_, format)| (*format == JarFormat::TaggedTx) != (tx_bodies == TxBodies::Pointers))
        .map(|(range, _)| format!("blocks {}..={}", range.start(), range.end()))
        .collect();
    if foreign.is_empty() {
        return Ok(());
    }
    let (marked, found) = match tx_bodies {
        TxBodies::Inline => (
            "stores its transactions inline",
            "with tagged values, written by a datadir whose transactions live in consensus packs",
        ),
        TxBodies::Pointers => (
            "stores its transactions by their position in consensus batches",
            "without tagged values, which are another datadir's static files",
        ),
    };
    eyre::bail!(
        "the reth database {marked} ({TN_LAYOUT_KEY} version {}), but its static files ({}) hold \
         {} jars ({}) {found}. Restore this datadir's own `static_files`, or swap `db` and \
         `static_files` together",
        tx_bodies.marker_version(),
        static_files.directory().display(),
        StaticFileSegment::Transactions,
        foreign.join(", "),
    )
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

    /// Open the datadir `path` with `format` for a new datadir, run `f` on the env, then close it.
    fn with_format<T>(
        chain: &Arc<RethChainSpec>,
        path: &Path,
        format: DatadirFormat,
        f: impl FnOnce(&RethEnv) -> eyre::Result<T>,
    ) -> eyre::Result<T> {
        let tm = TaskManager::new("format test");
        let env = RethEnv::new_for_temp_chain_with_format(chain.clone(), path, &tm, format)?;
        f(&env)
    }

    /// The format `layout` and `tx_bodies` make.
    const fn format(layout: StorageLayout, tx_bodies: TxBodies) -> DatadirFormat {
        DatadirFormat { layout, tx_bodies }
    }

    /// The marker of a datadir in `layout` that stores its transactions inline.
    const fn inline(layout: StorageLayout) -> LayoutMarker {
        LayoutMarker::of(format(layout, TxBodies::Inline))
    }

    /// The format a new temp chain datadir in `layout` gets.
    fn temp_chain_format(layout: StorageLayout) -> eyre::Result<DatadirFormat> {
        let tx_bodies = crate::env::genesis::temp_chain_new_datadir_tx_bodies()?;
        Ok(format(layout, tx_bodies).for_new_datadir())
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
        // the MDBX build stores transactions inline (marker version 1), tn-lmdb by position
        let (tx_bodies, version) = if cfg!(feature = "mdbx") {
            (TxBodies::Inline, LAYOUT_VERSION_INLINE_TXS)
        } else {
            (TxBodies::Pointers, LAYOUT_VERSION_TX_POINTERS)
        };
        assert_eq!(StorageLayout::for_new_datadirs(), expected, "for_new_datadirs");
        assert_eq!(TxBodies::for_new_datadirs(), tx_bodies, "TxBodies::for_new_datadirs");
        let tmp = TempDir::new()?;
        let command = crate::RethCommand::try_parse_from(["tn-reth"])?;
        let config = RethConfig::new(command, None, tmp.path(), true, chain());
        assert_eq!(config.new_datadir_layout(), expected, "the node's config");
        assert_eq!(config.new_datadir_tx_bodies(), tx_bodies, "the node's config");
        let tm = TaskManager::new("default layout");
        let database = RethEnv::new_database(&config, tmp.path().join("db"))?;
        let env = RethEnv::new(&config, &tm, database, None, Default::default())?;
        let (stored, marker) = recorded(&env)?;
        assert_eq!(stored, Some(expected), "reth's row");
        assert_eq!(marker, Some(LayoutMarker { version, layout: expected }), "TN's marker");
        assert_eq!(env.datadir_format(), format(expected, tx_bodies));
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
        let expected = temp_chain_format(layout)?;
        for run in ["new", "reopened"] {
            let tm = TaskManager::new(run);
            let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None)?;
            let (stored, marker) = recorded(&env)?;
            assert_eq!(stored, Some(layout), "{run}");
            assert_eq!(marker, Some(LayoutMarker::of(expected)), "{run}");
            assert_eq!(env.datadir_format(), expected, "{run}");
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
            assert_eq!(marker, Some(inline(StorageLayout::V1)));
        }
        Ok(())
    }

    /// The tn-lmdb build creates a v2 datadir, and a reopen asked for v1 keeps v2.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn tn_lmdb_creates_a_v2_datadir_and_keeps_it() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let expected = temp_chain_format(StorageLayout::V2)?;
        for requested in [StorageLayout::V2, StorageLayout::V1] {
            let (stored, marker) = with_env(&chain, tmp.path(), requested, recorded)?;
            assert_eq!(stored, Some(StorageLayout::V2), "asked for {requested}");
            assert_eq!(marker, Some(LayoutMarker::of(expected)));
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
            set_row(env, TN_LAYOUT_KEY, Some(inline(StorageLayout::V2).to_bytes()?))
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
        assert_eq!(marker, Some(inline(StorageLayout::V1)));
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

    /// A marker of an unknown format version, one of version 2 with layout v1 (no TN build writes
    /// it), one that does not parse, and one with a field this binary does not know are all
    /// refused.
    #[tokio::test]
    async fn an_unknown_marker_is_refused() -> eyre::Result<()> {
        let chain = chain();
        let cases: [(&[u8], &str); 6] = [
            (br#"{"version":2,"layout":"v1"}"#, "with layout v1, which no TN build writes"),
            (br#"{"version":3,"layout":"v1"}"#, "format version 3"),
            (br#"{"version":3,"layout":"v2"}"#, "a newer binary may have written it"),
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
            set_row(env, TN_LAYOUT_KEY, Some(inline(StorageLayout::V2).to_bytes()?))
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
                LayoutMarker::of(temp_chain_format(layout)?).to_bytes()?,
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
            let marker = LayoutMarker::of(temp_chain_format(layout)?);
            assert_eq!(recorded(env)?, (Some(layout), Some(marker)));
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

    /// Execute blocks 1..=`blocks` on `env`'s chain.
    fn execute_blocks(env: &RethEnv, chain: &Arc<RethChainSpec>, blocks: u64) -> eyre::Result<()> {
        let mut parent = chain.sealed_genesis_header();
        for number in 1..=blocks {
            let output = consensus_output_for_tests(2 * number as u32, 0, number, false);
            let payload = TNPayload::new_for_test(parent, &output);
            let executed = execute_payload_and_update_canonical_chain(env, payload, vec![])?;
            parent = executed.recovered_block.clone_sealed_header();
        }
        Ok(())
    }

    /// The formats of `env`'s `Transactions` jars, and whether new ones get tagged values.
    fn tx_jars(env: &RethEnv) -> eyre::Result<(Vec<JarFormat>, bool)> {
        let static_files = env.blockchain_provider().static_file_provider();
        let formats = static_files
            .jar_formats(StaticFileSegment::Transactions)?
            .into_iter()
            .map(|(_, format)| format)
            .collect();
        Ok((formats, static_files.tx_pointer_jars(StaticFileSegment::Transactions)))
    }

    /// A new v2 datadir asked for pointers records marker version 2, and its `Transactions` jars
    /// (genesis's and those after it) have tagged values. A reopen asked for inline keeps all of
    /// it, the jar policy included.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn a_pointer_datadir_gets_version_2_and_keeps_it() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let pointers = format(StorageLayout::V2, TxBodies::Pointers);
        for requested in [pointers, format(StorageLayout::V2, TxBodies::Inline)] {
            with_format(&chain, tmp.path(), requested, |env| {
                assert_eq!(env.datadir_format(), pointers, "asked for {requested}");
                let (_, marker) = recorded(env)?;
                assert_eq!(
                    marker,
                    Some(LayoutMarker {
                        version: LAYOUT_VERSION_TX_POINTERS,
                        layout: StorageLayout::V2
                    })
                );
                if requested == pointers {
                    execute_blocks(env, &chain, 1)?;
                }
                assert_eq!(
                    tx_jars(env)?,
                    (vec![JarFormat::TaggedTx], true),
                    "asked for {requested}"
                );
                Ok(())
            })?;
        }
        Ok(())
    }

    /// A new v2 datadir asked for inline records marker version 1 and has no tagged jars; a reopen
    /// asked for pointers keeps it inline. A new v1 datadir asked for pointers falls back to
    /// inline, which is what the MDBX build always does.
    #[tokio::test]
    async fn an_inline_datadir_keeps_version_1() -> eyre::Result<()> {
        let chain = chain();
        let layouts: &[StorageLayout] = if cfg!(feature = "mdbx") {
            &[StorageLayout::V1]
        } else {
            &[StorageLayout::V1, StorageLayout::V2]
        };
        for &layout in layouts {
            let tmp = TempDir::new()?;
            let first =
                if layout == StorageLayout::V1 { TxBodies::Pointers } else { TxBodies::Inline };
            for (run, tx_bodies) in [first, TxBodies::Pointers].into_iter().enumerate() {
                with_format(&chain, tmp.path(), format(layout, tx_bodies), |env| {
                    assert_eq!(env.datadir_format(), format(layout, TxBodies::Inline));
                    assert_eq!(recorded(env)?.1, Some(inline(layout)), "{layout} {tx_bodies}");
                    if run == 0 {
                        execute_blocks(env, &chain, 1)?;
                    }
                    let (jars, policy) = tx_jars(env)?;
                    assert!(!jars.is_empty() && !jars.contains(&JarFormat::TaggedTx), "{jars:?}");
                    assert!(!policy, "{layout} {tx_bodies}: no tagged jars from now on");
                    Ok(())
                })?;
            }
        }
        Ok(())
    }

    /// A pointer datadir's database beside an inline datadir's static files is refused, and the
    /// other way round: each is a half-done swap. Both are left as they were.
    #[cfg(not(feature = "mdbx"))]
    #[tokio::test]
    async fn mixed_tx_jars_are_refused_both_ways() -> eyre::Result<()> {
        let chain = chain();
        let tmp = TempDir::new()?;
        let (inline_dir, pointer_dir) = (tmp.path().join("inline"), tmp.path().join("pointers"));
        for (path, tx_bodies) in
            [(&inline_dir, TxBodies::Inline), (&pointer_dir, TxBodies::Pointers)]
        {
            with_format(&chain, path, format(StorageLayout::V2, tx_bodies), |env| {
                execute_blocks(env, &chain, 1)
            })?;
        }
        let cases = [
            (
                "pointers db, inline static files",
                &pointer_dir,
                &inline_dir,
                "without tagged values",
            ),
            ("inline db, pointer static files", &inline_dir, &pointer_dir, "with tagged values"),
        ];
        for (case, db_from, static_files_from, expected) in cases {
            let swapped = tmp.path().join(case.replace([' ', ','], "-"));
            copy_tree(db_from, &swapped, &["static_files"])?;
            copy_tree(&static_files_from.join("static_files"), &swapped.join("static_files"), &[])?;
            let before = listing(&swapped.join("static_files"));
            for requested in [TxBodies::Inline, TxBodies::Pointers] {
                let err = match with_format(
                    &chain,
                    &swapped,
                    format(StorageLayout::V2, requested),
                    |_| Ok(()),
                ) {
                    Ok(()) => panic!("{case}: the open was not refused"),
                    Err(e) => format!("{e:#}"),
                };
                assert!(err.contains(expected), "{case}: {err}");
                assert!(err.contains("Transactions jars (blocks 0..=499999)"), "{case}: {err}");
            }
            assert_eq!(listing(&swapped.join("static_files")), before, "{case}: left as they were");
        }
        Ok(())
    }
}
