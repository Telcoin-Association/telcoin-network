use super::{
    metrics::StaticFileProviderMetrics, writer::StaticFileWriters, LoadedJar,
    StaticFileJarProvider, StaticFileProviderRW, StaticFileProviderRWRefMut, StaticFileSyncStats,
    SyncPhase,
};
use crate::{
    changeset_walker::{StaticFileAccountChangesetWalker, StaticFileStorageChangesetWalker},
    to_range, BlockHashReader, BlockNumReader, BlockReader, BlockSource, EitherWriter,
    EitherWriterDestination, HeaderProvider, ReceiptProvider, StageCheckpointReader, StatsReader,
    TransactionVariant, TransactionsProvider, TransactionsProviderExt,
};
use alloy_consensus::{transaction::TransactionMeta, Header};
use alloy_eips::{eip2718::Encodable2718, BlockHashOrNumber};
use alloy_primitives::{b256, keccak256, Address, BlockHash, BlockNumber, TxHash, TxNumber, B256};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::RwLock;
use reth_chain_state::ExecutedBlock;
use reth_chainspec::{ChainInfo, ChainSpecProvider, EthChainSpec, NamedChain};
use reth_db::{
    lockfile::StorageLock,
    static_file::{
        iter_static_files, BlockHashMask, HeaderMask, HeaderWithHashMask, ReceiptMask,
        StaticFileCursor, StorageChangesetMask, TransactionMask, TransactionSenderMask,
    },
};
use reth_db_api::{
    cursor::DbCursorRO,
    models::{AccountBeforeTx, BlockNumberAddress, StorageBeforeTx, StoredBlockBodyIndices},
    table::{Decompress, Table, Value},
    tables,
    transaction::DbTx,
};
use reth_ethereum_primitives::{Receipt, TransactionSigned};
use reth_nippy_jar::{NippyJar, NippyJarChecker, NippyJarError, CONFIG_FILE_EXTENSION};
use reth_node_types::NodePrimitives;
use reth_primitives_traits::{
    dashmap::DashMap, AlloyBlockHeader as _, BlockBody as _, RecoveredBlock, SealedHeader,
    SignedTransaction, StorageSlotKey,
};
use reth_prune_types::PruneSegment;
use reth_stages_types::PipelineTarget;
use reth_static_file_types::{
    find_fixed_range, ChangesetOffsetReader, HighestStaticFiles, SegmentHeader,
    SegmentRangeInclusive, StaticFileMap, StaticFileSegment, DEFAULT_BLOCKS_PER_STATIC_FILE,
};
use reth_storage_api::{
    BlockBodyIndicesProvider, ChangeSetReader, ChangesetEntry, DBProvider, PruneCheckpointReader,
    StorageChangeSetReader, StorageSettingsCache,
};
use reth_storage_errors::provider::{ProviderError, ProviderResult, StaticFileWriterError};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    ops::{Bound, Deref, Range, RangeBounds, RangeInclusive},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
};
use tracing::{debug, info, info_span, instrument, trace, warn};

/// Alias type for a map that can be queried for block or transaction ranges. It uses `u64` to
/// represent either a block or a transaction number end of a static file range.
type SegmentRanges = BTreeMap<u64, SegmentRangeInclusive>;

/// Access mode on a static file provider. RO/RW.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum StaticFileAccess {
    /// Read-only access.
    #[default]
    RO,
    /// Read-write access.
    RW,
}

impl StaticFileAccess {
    /// Returns `true` if read-only access.
    pub const fn is_read_only(&self) -> bool {
        matches!(self, Self::RO)
    }

    /// Returns `true` if read-write access.
    pub const fn is_read_write(&self) -> bool {
        matches!(self, Self::RW)
    }
}

/// Context for static file block writes.
///
/// Contains target segments and pruning configuration.
#[derive(Debug, Clone, Copy, Default)]
pub struct StaticFileWriteCtx {
    /// Whether transaction senders should be written to static files.
    pub write_senders: bool,
    /// Whether receipts should be written to static files.
    pub write_receipts: bool,
    /// Whether account changesets should be written to static files.
    pub write_account_changesets: bool,
    /// Whether storage changesets should be written to static files.
    pub write_storage_changesets: bool,
    /// The current chain tip block number (for pruning).
    pub tip: BlockNumber,
    /// The prune mode for receipts, if any.
    pub receipts_prune_mode: Option<reth_prune_types::PruneMode>,
    /// Whether receipts are prunable (based on storage settings and prune distance).
    pub receipts_prunable: bool,
}

/// [`StaticFileProvider`] manages all existing [`StaticFileJarProvider`].
///
/// "Static files" contain immutable chain history data, such as:
///  - transactions
///  - headers
///  - receipts
///
/// This provider type is responsible for reading and writing to static files.
#[derive(Debug)]
pub struct StaticFileProvider<N>(pub(crate) Arc<StaticFileProviderInner<N>>);

impl<N> Clone for StaticFileProvider<N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

/// Builder for [`StaticFileProvider`] that allows configuration before initialization.
#[derive(Debug)]
pub struct StaticFileProviderBuilder<P> {
    access: StaticFileAccess,
    use_metrics: bool,
    blocks_per_file: StaticFileMap<u64>,
    path: P,
    genesis_block_number: u64,
    /// TN(TN-0122): whether the jars the provider creates carry checksums.
    jar_checksums: bool,
}

impl<P: AsRef<Path>> StaticFileProviderBuilder<P> {
    /// Creates a new builder with read-write access.
    pub fn read_write(path: P) -> Self {
        Self {
            path,
            access: StaticFileAccess::RW,
            blocks_per_file: Default::default(),
            use_metrics: false,
            genesis_block_number: 0,
            jar_checksums: false,
        }
    }

    /// Creates a new builder with read-only access.
    pub fn read_only(path: P) -> Self {
        Self {
            path,
            access: StaticFileAccess::RO,
            blocks_per_file: Default::default(),
            use_metrics: false,
            genesis_block_number: 0,
            jar_checksums: false,
        }
    }

    /// Set custom blocks per file for specific segments.
    ///
    /// Each static file segment is stored across multiple files, and each of these files contains
    /// up to the specified number of blocks of data. When the file gets full, a new file is
    /// created with the new block range.
    ///
    /// This setting affects the size of each static file, and can be set per segment.
    ///
    /// If it is changed for an existing node, existing static files will not be affected and will
    /// be finished with the old blocks per file setting, but new static files will use the new
    /// setting.
    pub fn with_blocks_per_file_for_segments(
        mut self,
        segments: &<StaticFileMap<u64> as Deref>::Target,
    ) -> Self {
        for (segment, &blocks_per_file) in segments {
            self.blocks_per_file.insert(segment, blocks_per_file);
        }
        self
    }

    /// Set a custom number of blocks per file for all segments.
    pub fn with_blocks_per_file(mut self, blocks_per_file: u64) -> Self {
        for segment in StaticFileSegment::iter() {
            self.blocks_per_file.insert(segment, blocks_per_file);
        }
        self
    }

    /// Set a custom number of blocks per file for a specific segment.
    pub fn with_blocks_per_file_for_segment(
        mut self,
        segment: StaticFileSegment,
        blocks_per_file: u64,
    ) -> Self {
        self.blocks_per_file.insert(segment, blocks_per_file);
        self
    }

    /// Enables metrics on the [`StaticFileProvider`].
    pub const fn with_metrics(mut self) -> Self {
        self.use_metrics = true;
        self
    }

    /// Sets the genesis block number for the [`StaticFileProvider`].
    ///
    /// This configures the genesis block number, which is used to determine the starting point
    /// for block indexing and querying operations.
    ///
    /// # Arguments
    ///
    /// * `genesis_block_number` - The block number of the genesis block.
    ///
    /// # Returns
    ///
    /// Returns `Self` to allow method chaining.
    pub const fn with_genesis_block_number(mut self, genesis_block_number: u64) -> Self {
        self.genesis_block_number = genesis_block_number;
        self
    }

    /// TN(TN-0122): makes the jars the provider creates carry checksums
    /// ([`StaticFileProviderInner::set_jar_checksums`]).
    pub const fn with_jar_checksums(mut self, enabled: bool) -> Self {
        self.jar_checksums = enabled;
        self
    }

    /// Builds the final [`StaticFileProvider`] and initializes the index.
    pub fn build<N: NodePrimitives>(self) -> ProviderResult<StaticFileProvider<N>> {
        let mut provider = StaticFileProviderInner::new(self.path, self.access)?;
        if self.use_metrics {
            provider.metrics = Some(Arc::new(StaticFileProviderMetrics::default()));
        }

        for (segment, blocks_per_file) in *self.blocks_per_file {
            provider.blocks_per_file.insert(segment, blocks_per_file);
        }
        provider.genesis_block_number = self.genesis_block_number;
        provider.jar_checksums = AtomicBool::new(self.jar_checksums);

        let provider = StaticFileProvider(Arc::new(provider));
        provider.initialize_index()?;
        Ok(provider)
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Creates a new [`StaticFileProvider`] with the given [`StaticFileAccess`].
    fn new(path: impl AsRef<Path>, access: StaticFileAccess) -> ProviderResult<Self> {
        let provider = Self(Arc::new(StaticFileProviderInner::new(path, access)?));
        provider.initialize_index()?;
        Ok(provider)
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Creates a new [`StaticFileProvider`] with read-only access.
    ///
    /// Set `watch_directory` to `true` to track the most recent changes in static files. Otherwise,
    /// new data won't be detected or queryable.
    ///
    /// Watching is recommended if the read-only provider is used on a directory that an active node
    /// instance is modifying.
    ///
    /// See also [`StaticFileProvider::watch_directory`].
    pub fn read_only(path: impl AsRef<Path>, watch_directory: bool) -> ProviderResult<Self> {
        let provider = Self::new(path, StaticFileAccess::RO)?;

        if watch_directory {
            provider.watch_directory();
        }

        Ok(provider)
    }

    /// Creates a new [`StaticFileProvider`] with read-write access.
    pub fn read_write(path: impl AsRef<Path>) -> ProviderResult<Self> {
        Self::new(path, StaticFileAccess::RW)
    }

    /// Watches the directory for changes and updates the in-memory index when modifications
    /// are detected.
    ///
    /// This may be necessary, since a non-node process that owns a [`StaticFileProvider`] does not
    /// receive `update_index` notifications from a node that appends/truncates data.
    pub fn watch_directory(&self) {
        let provider = self.clone();
        reth_tasks::spawn_os_thread("sf-watch", move || {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = RecommendedWatcher::new(
                move |res| tx.send(res).unwrap(),
                notify::Config::default(),
            )
            .expect("failed to create watcher");

            watcher
                .watch(&provider.path, RecursiveMode::NonRecursive)
                .expect("failed to watch path");

            // Some backends send repeated modified events
            let mut last_event_timestamp = None;

            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        // We only care about modified data events
                        if !matches!(
                            event.kind,
                            notify::EventKind::Modify(_) |
                                notify::EventKind::Create(_) |
                                notify::EventKind::Remove(_)
                        ) {
                            continue;
                        }

                        // We only trigger a re-initialization if a configuration file was
                        // modified. This means that a
                        // static_file_provider.commit() was called on the node after
                        // appending/truncating rows
                        for segment in event.paths {
                            // Ensure it's a file with the .conf extension
                            if segment
                                .extension()
                                .is_none_or(|s| s.to_str() != Some(CONFIG_FILE_EXTENSION))
                            {
                                continue;
                            }

                            // Ensure it's well formatted static file name
                            if StaticFileSegment::parse_filename(
                                &segment.file_stem().expect("qed").to_string_lossy(),
                            )
                            .is_none()
                            {
                                continue;
                            }

                            // If we can read the metadata and modified timestamp, ensure this is
                            // not an old or repeated event.
                            if let Ok(current_modified_timestamp) =
                                std::fs::metadata(&segment).and_then(|m| m.modified())
                            {
                                if last_event_timestamp.is_some_and(|last_timestamp| {
                                    last_timestamp >= current_modified_timestamp
                                }) {
                                    continue;
                                }
                                last_event_timestamp = Some(current_modified_timestamp);
                            }

                            info!(target: "providers::static_file", updated_file = ?segment.file_stem(), "re-initializing static file provider index");
                            if let Err(err) = provider.initialize_index() {
                                warn!(target: "providers::static_file", "failed to re-initialize index: {err}");
                            }
                            break;
                        }
                    }

                    Err(err) => warn!(target: "providers::watcher", "watch error: {err:?}"),
                }
            }
        });
    }
}

impl<N: NodePrimitives> Deref for StaticFileProvider<N> {
    type Target = StaticFileProviderInner<N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
type BeforeCacheInsert = Box<dyn FnOnce(StaticFileSegment) + Send + 'static>;

#[cfg(test)]
#[derive(Default)]
struct BeforeCacheInsertHook(parking_lot::Mutex<Option<BeforeCacheInsert>>);

#[cfg(test)]
impl Debug for BeforeCacheInsertHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BeforeCacheInsertHook").finish_non_exhaustive()
    }
}

#[cfg(test)]
impl BeforeCacheInsertHook {
    fn set(&self, hook: impl FnOnce(StaticFileSegment) + Send + 'static) {
        let previous = self.0.lock().replace(Box::new(hook));
        assert!(previous.is_none(), "cache fill hook already installed");
    }

    fn fire(&self, segment: StaticFileSegment) {
        if let Some(hook) = self.0.lock().take() {
            hook(segment);
        }
    }
}

/// TN(TN-0101): what [`StaticFileProvider::prepare_index_update`] did before the index lock.
struct PreparedIndexUpdate {
    /// The segment's file range the jar was loaded for.
    fixed_range: SegmentRangeInclusive,
    /// The jar cache generation before the load.
    generation: u64,
    /// The loaded jar's header.
    header: SegmentHeader,
    /// The loaded jar, when it was not published before the lock.
    jar: Option<LoadedJar>,
}

/// [`StaticFileProviderInner`] manages all existing [`StaticFileJarProvider`].
#[derive(Debug)]
pub struct StaticFileProviderInner<N> {
    /// Maintains a map which allows for concurrent access to different `NippyJars`, over different
    /// segments and ranges.
    map: DashMap<(BlockNumber, StaticFileSegment), LoadedJar>,
    /// Incremented before clearing the jar cache so in-flight loads cannot repopulate it with a
    /// snapshot from before the invalidation.
    cache_generation: AtomicU64,
    /// Indexes per segment.
    indexes: RwLock<StaticFileMap<StaticFileSegmentIndex>>,
    /// This is an additional index that tracks the expired height, this will track the highest
    /// block number that has been expired (missing). The first, non expired block is
    /// `expired_history_height + 1`.
    ///
    /// This is effectively the transaction range that has been expired:
    /// [`StaticFileProvider::delete_segment_below_block`] and mirrors
    /// `static_files_min_block[transactions] - blocks_per_file`.
    ///
    /// This additional tracker exists for more efficient lookups because the node must be aware of
    /// the expired height.
    earliest_history_height: AtomicU64,
    /// Directory where `static_files` are located
    path: PathBuf,
    /// Maintains a writer set of [`StaticFileSegment`].
    writers: StaticFileWriters<N>,
    /// Metrics for the static files.
    metrics: Option<Arc<StaticFileProviderMetrics>>,
    /// Access rights of the provider.
    access: StaticFileAccess,
    /// Number of blocks per file, per segment.
    blocks_per_file: StaticFileMap<u64>,
    /// Write lock for when access is [`StaticFileAccess::RW`].
    _lock_file: Option<StorageLock>,
    /// Genesis block number, default is 0;
    genesis_block_number: u64,
    /// TN(TN-0052): sync timings since the last [`StaticFileProvider::take_sync_stats`].
    sync_stats: parking_lot::Mutex<StaticFileSyncStats>,
    /// TN(TN-0122): whether the jars this provider creates carry checksums.
    jar_checksums: AtomicBool,
    #[cfg(test)]
    before_cache_insert: BeforeCacheInsertHook,
    #[cfg(test)]
    after_cache_validation: BeforeCacheInsertHook,
}

impl<N> StaticFileProviderInner<N> {
    /// TN(TN-0052): returns the per-segment sync timings recorded since the last call and starts
    /// a new period. A bench calls it after each persist's commit for that persist's split.
    pub fn take_sync_stats(&self) -> StaticFileSyncStats {
        std::mem::take(&mut *self.sync_stats.lock())
    }

    /// TN(TN-0052): records one sync phase of `segment`.
    pub(crate) fn record_sync(
        &self,
        segment: StaticFileSegment,
        phase: SyncPhase,
        elapsed: std::time::Duration,
    ) {
        self.sync_stats.lock().record(segment, phase, elapsed);
    }

    /// TN(TN-0122): sets whether the jars this provider creates from now on carry checksums
    /// (`reth_nippy_jar::NippyJar::with_checksums`), for the segments [`checksummed_segment`]
    /// names. [`ProviderFactory`](crate::ProviderFactory) sets it from the datadir's storage
    /// settings: on for storage v2, off for v1, whose files stay upstream's format byte for byte.
    /// A jar that exists is read and appended in its own format, which its configuration records;
    /// with checksums on, a checksummed segment's jar without them is refused (TN-0135,
    /// [`Self::load_jar`]).
    pub fn set_jar_checksums(&self, enabled: bool) {
        self.jar_checksums.store(enabled, Ordering::Relaxed);
    }

    /// TN(TN-0122): whether a jar of `segment` this provider creates now carries checksums.
    pub fn jar_checksums(&self, segment: StaticFileSegment) -> bool {
        self.jar_checksums.load(Ordering::Relaxed) && checksummed_segment(segment)
    }

    /// TN(TN-0135): loads the configuration of `segment`'s jar at `path`. Under storage v2 (the
    /// provider's checksums on) every jar of a checksummed segment must carry checksums: a
    /// version-1 jar there is refused, since two flipped bits turn a version-2 configuration into
    /// a version-1 one, whose values would then read with their checksums appended and whose
    /// 20-byte `.csoff` records at a 16-byte stride. A storage-v2 datadir from before checksums
    /// is refused the same way (storage v2 was never released).
    pub(crate) fn load_jar(
        &self,
        segment: StaticFileSegment,
        path: &Path,
    ) -> ProviderResult<NippyJar<SegmentHeader>> {
        let jar = NippyJar::<SegmentHeader>::load(path).map_err(ProviderError::other)?;
        if self.jar_checksums(segment) && !jar.has_checksums() {
            tracing::error!(
                target: "reth::static_file",
                %segment,
                path = %path.display(),
                "Static file without checksums in a storage v2 datadir"
            );
            return Err(ProviderError::other(UncheckedJarError { path: path.to_path_buf() }))
        }
        Ok(jar)
    }
}

/// TN(TN-0135): a jar of a checksummed segment without checksums, in a storage-v2 datadir
/// ([`StaticFileProviderInner::load_jar`]).
#[derive(Debug)]
pub struct UncheckedJarError {
    /// The jar's data file.
    pub path: PathBuf,
}

impl core::fmt::Display for UncheckedJarError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "static file {} has no checksums, but every jar of its segment in a storage v2 \
             datadir must (its configuration is damaged, or it predates checksums)",
            self.path.display()
        )
    }
}

impl core::error::Error for UncheckedJarError {}

/// TN(TN-0122): the segments whose new jars carry checksums when the provider's checksums are on
/// (storage v2). The changesets are mandatory: every historical state read below the tip resolves
/// through them, TN's pinned committee and epoch reads included. The others are checked too, as
/// long as the read cost stays below the benches' noise.
pub const fn checksummed_segment(segment: StaticFileSegment) -> bool {
    match segment {
        // Mandatory.
        StaticFileSegment::AccountChangeSets |
        StaticFileSegment::StorageChangeSets |
        // The default for the rest; a segment leaves this list if its reads show the cost.
        StaticFileSegment::Headers |
        StaticFileSegment::Transactions |
        StaticFileSegment::Receipts |
        StaticFileSegment::TransactionSenders => true,
    }
}

/// TN(TN-0133): a segment's static files skip blocks between two of its jars: a jar is missing,
/// or one below the highest is not full. Its blocks would read as absent.
#[derive(Debug)]
pub enum StaticFileGapError {
    /// Found at startup ([`StaticFileProvider::check_contiguity`]): `after` does not start right
    /// after `before`.
    Jars {
        /// The segment.
        segment: StaticFileSegment,
        /// The lower jar's block range.
        before: SegmentRangeInclusive,
        /// The next jar's block range.
        after: SegmentRangeInclusive,
    },
    /// Found by a read: no jar holds `number` (a block, or a transaction of a transaction
    /// segment), though the segment has jars below and above it.
    Number {
        /// The segment.
        segment: StaticFileSegment,
        /// The block or transaction.
        number: u64,
    },
}

impl core::fmt::Display for StaticFileGapError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Jars { segment, before, after } => write!(
                f,
                "{segment} static files skip blocks: the jar with blocks {after} does not follow \
                 the one with blocks {before} (a jar is missing or incomplete)"
            ),
            Self::Number { segment, number } => write!(
                f,
                "{segment} static files have no jar for {number}, though they have jars below \
                 and above it (a jar is missing)"
            ),
        }
    }
}

impl core::error::Error for StaticFileGapError {}

impl<N: NodePrimitives> StaticFileProviderInner<N> {
    /// Creates a new [`StaticFileProviderInner`].
    fn new(path: impl AsRef<Path>, access: StaticFileAccess) -> ProviderResult<Self> {
        let _lock_file = if access.is_read_write() {
            StorageLock::try_acquire(path.as_ref()).map_err(ProviderError::other)?.into()
        } else {
            None
        };

        let mut blocks_per_file = StaticFileMap::default();
        for segment in StaticFileSegment::iter() {
            blocks_per_file.insert(segment, DEFAULT_BLOCKS_PER_STATIC_FILE);
        }

        let provider = Self {
            map: Default::default(),
            cache_generation: Default::default(),
            indexes: Default::default(),
            writers: Default::default(),
            earliest_history_height: Default::default(),
            path: path.as_ref().to_path_buf(),
            metrics: None,
            access,
            blocks_per_file,
            _lock_file,
            genesis_block_number: 0,
            sync_stats: Default::default(),
            jar_checksums: Default::default(),
            #[cfg(test)]
            before_cache_insert: Default::default(),
            #[cfg(test)]
            after_cache_validation: Default::default(),
        };

        Ok(provider)
    }

    pub const fn is_read_only(&self) -> bool {
        self.access.is_read_only()
    }

    /// Each static file has a fixed number of blocks. This gives out the range where the requested
    /// block is positioned.
    ///
    /// If the specified block falls into one of the ranges of already initialized static files,
    /// this function will return that range.
    ///
    /// If no matching file exists, this function will derive a new range from the end of the last
    /// existing file, if any.
    pub fn find_fixed_range_with_block_index(
        &self,
        segment: StaticFileSegment,
        block_index: Option<&SegmentRanges>,
        block: BlockNumber,
    ) -> SegmentRangeInclusive {
        let blocks_per_file =
            self.blocks_per_file.get(segment).copied().unwrap_or(DEFAULT_BLOCKS_PER_STATIC_FILE);

        if let Some(block_index) = block_index {
            // Find first block range that contains the requested block
            if let Some((_, range)) = block_index.range(block..).next() {
                // Found matching range for an existing file using block index
                return *range;
            } else if let Some((_, range)) = block_index.last_key_value() {
                // Didn't find matching range for an existing file, derive a new range from the end
                // of the last existing file range.
                //
                // `block` is always higher than `range.end()` here, because `block_index` holds no
                // range with a `max_block` greater than or equal to `block`
                let blocks_after_last_range = block - range.end();
                let segments_to_skip = (blocks_after_last_range - 1) / blocks_per_file;
                let start = range.end() + 1 + segments_to_skip * blocks_per_file;
                return SegmentRangeInclusive::new(start, start + blocks_per_file - 1);
            }
        }
        // No block index is available, derive a new range using the fixed number of blocks,
        // starting from the beginning.
        find_fixed_range(block, blocks_per_file)
    }

    /// Each static file has a fixed number of blocks. This gives out the range where the requested
    /// block is positioned.
    ///
    /// If the specified block falls into one of the ranges of already initialized static files,
    /// this function will return that range.
    ///
    /// If no matching file exists, this function will derive a new range from the end of the last
    /// existing file, if any.
    ///
    /// This function will block indefinitely if a write lock for
    /// [`Self::indexes`] is already acquired. In that case, use
    /// [`Self::find_fixed_range_with_block_index`].
    pub fn find_fixed_range(
        &self,
        segment: StaticFileSegment,
        block: BlockNumber,
    ) -> SegmentRangeInclusive {
        self.find_fixed_range_with_block_index(
            segment,
            self.indexes.read().get(segment).map(|index| &index.expected_block_ranges_by_max_block),
            block,
        )
    }

    /// Get genesis block number
    pub const fn genesis_block_number(&self) -> u64 {
        self.genesis_block_number
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Reports metrics for the static files.
    ///
    /// This uses the in-memory index to get file sizes from mmap handles instead of reading
    /// filesystem metadata.
    pub fn report_metrics(&self) -> ProviderResult<()> {
        let Some(metrics) = &self.metrics else { return Ok(()) };

        let static_files = iter_static_files(&self.path).map_err(ProviderError::other)?;
        for (segment, headers) in &*static_files {
            let mut entries = 0;
            let mut size = 0;

            for (block_range, _) in headers {
                let fixed_block_range = self.find_fixed_range(segment, block_range.start());
                let jar_provider = self
                    .get_segment_provider_for_range(segment, || Some(fixed_block_range), None)?
                    .ok_or_else(|| {
                        ProviderError::MissingStaticFileBlock(segment, block_range.start())
                    })?;

                entries += jar_provider.rows();
                size += jar_provider.size() as u64;
            }

            metrics.record_segment(segment, size, headers.len(), entries);
        }

        Ok(())
    }

    /// Writes headers for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    fn write_headers(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
    ) -> ProviderResult<()> {
        for block in blocks {
            let b = block.recovered_block();
            w.append_header(b.header(), &b.hash())?;
        }
        Ok(())
    }

    /// Writes transactions for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    fn write_transactions(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
        tx_nums: &[TxNumber],
    ) -> ProviderResult<()> {
        for (block, &first_tx) in blocks.iter().zip(tx_nums) {
            let b = block.recovered_block();
            w.increment_block(b.number())?;
            for (i, tx) in b.body().transactions().iter().enumerate() {
                w.append_transaction(first_tx + i as u64, tx)?;
            }
        }
        Ok(())
    }

    /// Writes transaction senders for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    fn write_transaction_senders(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
        tx_nums: &[TxNumber],
    ) -> ProviderResult<()> {
        for (block, &first_tx) in blocks.iter().zip(tx_nums) {
            let b = block.recovered_block();
            w.increment_block(b.number())?;
            for (i, sender) in b.senders_iter().enumerate() {
                w.append_transaction_sender(first_tx + i as u64, sender)?;
            }
        }
        Ok(())
    }

    /// Writes receipts for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    fn write_receipts(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
        tx_nums: &[TxNumber],
        ctx: &StaticFileWriteCtx,
    ) -> ProviderResult<()> {
        for (block, &first_tx) in blocks.iter().zip(tx_nums) {
            let block_number = block.recovered_block().number();
            w.increment_block(block_number)?;

            // skip writing receipts if pruning configuration requires us to.
            if ctx.receipts_prunable &&
                ctx.receipts_prune_mode
                    .is_some_and(|mode| mode.should_prune(block_number, ctx.tip))
            {
                continue
            }

            for (i, receipt) in block.execution_outcome().receipts.iter().enumerate() {
                w.append_receipt(first_tx + i as u64, receipt)?;
            }
        }
        Ok(())
    }

    /// Writes account changesets for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    fn write_account_changesets(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
    ) -> ProviderResult<()> {
        for block in blocks {
            let block_number = block.recovered_block().number();
            let reverts = block.execution_outcome().state.reverts.to_plain_state_reverts();

            let changeset: Vec<_> = reverts
                .accounts
                .into_iter()
                .flatten()
                .map(|(address, info)| AccountBeforeTx { address, info: info.map(Into::into) })
                .collect();
            w.append_account_changeset(changeset, block_number)?;
        }
        Ok(())
    }

    /// Writes storage changesets for all blocks to the static file segment.
    #[instrument(level = "debug", target = "providers::db", skip_all)]
    fn write_storage_changesets(
        w: &mut StaticFileProviderRWRefMut<'_, N>,
        blocks: &[ExecutedBlock<N>],
    ) -> ProviderResult<()> {
        for block in blocks {
            let block_number = block.recovered_block().number();
            let reverts = block.execution_outcome().state.reverts.to_plain_state_reverts();

            let changeset: Vec<_> = reverts
                .storage
                .into_iter()
                .flatten()
                .flat_map(|revert| {
                    revert.storage_revert.into_iter().map(move |(key, revert_to_slot)| {
                        StorageBeforeTx {
                            address: revert.address,
                            // TN(TN-0044): the plain slot, as in v1's changesets (upstream
                            // 815037e27).
                            key: StorageSlotKey::from_u256(key).as_b256(),
                            value: revert_to_slot.to_previous_value(),
                        }
                    })
                })
                .collect();
            w.append_storage_changeset(changeset, block_number)?;
        }
        Ok(())
    }

    /// Writes to a static file segment using the provided closure.
    ///
    /// The closure receives a mutable reference to the segment writer. After the closure completes,
    /// `sync_and_freeze()` flushes writes to disk and writes the segment's configuration.
    fn write_segment<F>(
        &self,
        segment: StaticFileSegment,
        first_block_number: BlockNumber,
        f: F,
    ) -> ProviderResult<()>
    where
        F: FnOnce(&mut StaticFileProviderRWRefMut<'_, N>) -> ProviderResult<()>,
    {
        let mut w = self.get_writer(first_block_number, segment)?;
        f(&mut w)?;
        // TN(TN-0051): the configuration is written here, in the segment's thread, not by
        // `finalize` on the commit thread, which only updates the reader index.
        w.sync_and_freeze()
    }

    /// Writes all static file data for multiple blocks in parallel per-segment.
    ///
    /// This spawns tasks on the storage thread pool for each segment type and each task calls
    /// `sync_and_freeze()` on its writer when done.
    #[instrument(level = "debug", target = "providers::static_file", skip_all)]
    pub fn write_blocks_data(
        &self,
        blocks: &[ExecutedBlock<N>],
        tx_nums: &[TxNumber],
        ctx: StaticFileWriteCtx,
        runtime: &reth_tasks::Runtime,
    ) -> ProviderResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let first_block_number = blocks[0].recovered_block().number();

        let mut r_headers = None;
        let mut r_txs = None;
        let mut r_senders = None;
        let mut r_receipts = None;
        let mut r_account_changesets = None;
        let mut r_storage_changesets = None;

        runtime.storage_pool().in_place_scope(|s| {
            s.spawn(|_| {
                r_headers =
                    Some(self.write_segment(StaticFileSegment::Headers, first_block_number, |w| {
                        Self::write_headers(w, blocks)
                    }));
            });

            s.spawn(|_| {
                r_txs = Some(self.write_segment(
                    StaticFileSegment::Transactions,
                    first_block_number,
                    |w| Self::write_transactions(w, blocks, tx_nums),
                ));
            });

            if ctx.write_senders {
                s.spawn(|_| {
                    r_senders = Some(self.write_segment(
                        StaticFileSegment::TransactionSenders,
                        first_block_number,
                        |w| Self::write_transaction_senders(w, blocks, tx_nums),
                    ));
                });
            }

            if ctx.write_receipts {
                s.spawn(|_| {
                    r_receipts = Some(self.write_segment(
                        StaticFileSegment::Receipts,
                        first_block_number,
                        |w| Self::write_receipts(w, blocks, tx_nums, &ctx),
                    ));
                });
            }

            if ctx.write_account_changesets {
                s.spawn(|_| {
                    r_account_changesets = Some(self.write_segment(
                        StaticFileSegment::AccountChangeSets,
                        first_block_number,
                        |w| Self::write_account_changesets(w, blocks),
                    ));
                });
            }

            if ctx.write_storage_changesets {
                s.spawn(|_| {
                    r_storage_changesets = Some(self.write_segment(
                        StaticFileSegment::StorageChangeSets,
                        first_block_number,
                        |w| Self::write_storage_changesets(w, blocks),
                    ));
                });
            }
        });

        r_headers.ok_or(StaticFileWriterError::ThreadPanic("headers"))??;
        r_txs.ok_or(StaticFileWriterError::ThreadPanic("transactions"))??;
        if ctx.write_senders {
            r_senders.ok_or(StaticFileWriterError::ThreadPanic("senders"))??;
        }
        if ctx.write_receipts {
            r_receipts.ok_or(StaticFileWriterError::ThreadPanic("receipts"))??;
        }
        if ctx.write_account_changesets {
            r_account_changesets
                .ok_or(StaticFileWriterError::ThreadPanic("account_changesets"))??;
        }
        if ctx.write_storage_changesets {
            r_storage_changesets
                .ok_or(StaticFileWriterError::ThreadPanic("storage_changesets"))??;
        }
        Ok(())
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and start index that can be
    /// either block or transaction.
    pub fn get_segment_provider(
        &self,
        segment: StaticFileSegment,
        number: u64,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        if segment.is_block_or_change_based() {
            self.get_segment_provider_for_block(segment, number, None)
        } else {
            self.get_segment_provider_for_transaction(segment, number, None)
        }
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and start index that can be
    /// either block or transaction.
    ///
    /// If the segment is not found, returns [`None`].
    pub fn get_maybe_segment_provider(
        &self,
        segment: StaticFileSegment,
        number: u64,
    ) -> ProviderResult<Option<StaticFileJarProvider<'_, N>>> {
        let provider = if segment.is_block_or_change_based() {
            self.get_segment_provider_for_block(segment, number, None)
        } else {
            self.get_segment_provider_for_transaction(segment, number, None)
        };

        match provider {
            Ok(provider) => Ok(Some(provider)),
            Err(
                ProviderError::MissingStaticFileBlock(_, _) |
                ProviderError::MissingStaticFileTx(_, _),
            ) => Ok(None),
            Err(err) => Err(err),
        }
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and block.
    pub fn get_segment_provider_for_block(
        &self,
        segment: StaticFileSegment,
        block: BlockNumber,
        path: Option<&Path>,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        self.get_segment_provider_for_range(
            segment,
            || self.get_segment_ranges_from_block(segment, block),
            path,
        )?
        .ok_or(ProviderError::MissingStaticFileBlock(segment, block))
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and transaction.
    pub fn get_segment_provider_for_transaction(
        &self,
        segment: StaticFileSegment,
        tx: TxNumber,
        path: Option<&Path>,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        self.get_segment_provider_for_range(
            segment,
            || self.get_segment_ranges_from_transaction(segment, tx),
            path,
        )?
        .ok_or(ProviderError::MissingStaticFileTx(segment, tx))
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and block or transaction.
    ///
    /// `fn_range` should make sure the range goes through `find_fixed_range`.
    pub fn get_segment_provider_for_range(
        &self,
        segment: StaticFileSegment,
        fn_range: impl Fn() -> Option<SegmentRangeInclusive>,
        path: Option<&Path>,
    ) -> ProviderResult<Option<StaticFileJarProvider<'_, N>>> {
        // If we have a path, then get the block range from its name.
        // Otherwise, check `self.available_static_files`
        let block_range = match path {
            Some(path) => StaticFileSegment::parse_filename(
                &path
                    .file_name()
                    .ok_or_else(|| {
                        ProviderError::MissingStaticFileSegmentPath(segment, path.to_path_buf())
                    })?
                    .to_string_lossy(),
            )
            .and_then(|(parsed_segment, block_range)| {
                if parsed_segment == segment {
                    return Some(block_range);
                }
                None
            }),
            None => fn_range(),
        };

        // Return cached `LoadedJar` or insert it for the first time, and then, return it.
        if let Some(block_range) = block_range {
            return Ok(Some(self.get_or_create_jar_provider(segment, &block_range)?));
        }

        Ok(None)
    }

    /// Gets the [`StaticFileJarProvider`] of the requested path.
    pub fn get_segment_provider_for_path(
        &self,
        path: &Path,
    ) -> ProviderResult<Option<StaticFileJarProvider<'_, N>>> {
        StaticFileSegment::parse_filename(
            &path
                .file_name()
                .ok_or_else(|| ProviderError::MissingStaticFilePath(path.to_path_buf()))?
                .to_string_lossy(),
        )
        .map(|(segment, block_range)| self.get_or_create_jar_provider(segment, &block_range))
        .transpose()
    }

    /// Given a segment and block range it removes the cached provider from the map.
    ///
    /// CAUTION: cached provider should be dropped before calling this or IT WILL deadlock.
    pub fn remove_cached_provider(
        &self,
        segment: StaticFileSegment,
        fixed_block_range_end: BlockNumber,
    ) {
        self.map.remove(&(fixed_block_range_end, segment));
    }

    /// This handles history expiry by deleting all static files for the given segment below the
    /// given block.
    ///
    /// For example if block is 1M and the blocks per file are 500K this will delete all individual
    /// files below 1M, so 0-499K and 500K-999K.
    ///
    /// This will not delete the file that contains the block itself, because files can only be
    /// removed entirely.
    ///
    /// # Safety
    ///
    /// This method will never delete the highest static file for the segment, even if the
    /// requested block is higher than the highest block in static files. This ensures we always
    /// maintain at least one static file if any exist.
    ///
    /// Returns a list of `SegmentHeader`s from the deleted jars.
    pub fn delete_segment_below_block(
        &self,
        segment: StaticFileSegment,
        block: BlockNumber,
    ) -> ProviderResult<Vec<SegmentHeader>> {
        // Nothing to delete if block is 0.
        if block == 0 {
            return Ok(Vec::new());
        }

        let highest_block = self.get_highest_static_file_block(segment);
        let mut deleted_headers = Vec::new();

        loop {
            let Some(block_height) = self.get_lowest_range_end(segment) else {
                return Ok(deleted_headers);
            };

            // Stop if we've reached the target block or the highest static file
            if block_height >= block || Some(block_height) == highest_block {
                return Ok(deleted_headers);
            }

            debug!(
                target: "providers::static_file",
                ?segment,
                ?block_height,
                "Deleting static file below block"
            );

            // now we need to wipe the static file, this will take care of updating the index and
            // advance the lowest tracked block height for the segment.
            let header = self.delete_jar(segment, block_height).inspect_err(|err| {
                warn!( target: "providers::static_file", ?segment, %block_height, ?err, "Failed to delete static file below block")
            })?;

            deleted_headers.push(header);
        }
    }

    /// Given a segment and block, it deletes the jar and all files from the respective block range.
    ///
    /// CAUTION: destructive. Deletes files on disk.
    ///
    /// This will re-initialize the index after deletion, so all files are tracked.
    ///
    /// Returns the `SegmentHeader` of the deleted jar.
    pub fn delete_jar(
        &self,
        segment: StaticFileSegment,
        block: BlockNumber,
    ) -> ProviderResult<SegmentHeader> {
        let fixed_block_range = self.find_fixed_range(segment, block);
        let key = (fixed_block_range.end(), segment);
        let file = self.path.join(segment.filename(&fixed_block_range));
        let jar = if let Some((_, jar)) = self.map.remove(&key) {
            jar.jar
        } else {
            debug!(
                target: "providers::static_file",
                ?file,
                ?fixed_block_range,
                ?block,
                "Loading static file jar for deletion"
            );
            NippyJar::<SegmentHeader>::load(&file).map_err(ProviderError::other)?
        };

        let header = jar.user_header().clone();

        // TN(TN-0132): `NippyJar::delete` removes the sidecar too, after the data file.
        jar.delete().map_err(ProviderError::other)?;

        // SAFETY: this is currently necessary to ensure that certain indexes like
        // `static_files_min_block` have the correct values after pruning.
        self.initialize_index()?;

        Ok(header)
    }

    /// Deletes ALL static file jars for the given segment, including the highest one.
    ///
    /// CAUTION: destructive. Deletes all files on disk for this segment.
    ///
    /// This is used for `PruneMode::Full` where all data should be removed.
    ///
    /// Returns a list of `SegmentHeader`s from the deleted jars.
    pub fn delete_segment(&self, segment: StaticFileSegment) -> ProviderResult<Vec<SegmentHeader>> {
        let mut deleted_headers = Vec::new();

        while let Some(block_height) = self.get_highest_static_file_block(segment) {
            debug!(
                target: "providers::static_file",
                ?segment,
                ?block_height,
                "Deleting static file jar"
            );

            let header = self.delete_jar(segment, block_height).inspect_err(|err| {
                warn!(target: "providers::static_file", ?segment, %block_height, ?err, "Failed to delete static file jar")
            })?;

            deleted_headers.push(header);
        }

        Ok(deleted_headers)
    }

    /// Given a segment and block range it returns a cached
    /// [`StaticFileJarProvider`]. TODO(joshie): we should check the size and pop N if there's too
    /// many.
    fn get_or_create_jar_provider(
        &self,
        segment: StaticFileSegment,
        fixed_block_range: &SegmentRangeInclusive,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        let key = (fixed_block_range.end(), segment);

        // Avoid using `entry` directly to avoid a write lock in the common case.
        trace!(target: "providers::static_file", ?segment, ?fixed_block_range, "Getting provider");
        let mut provider: StaticFileJarProvider<'_, N> = if let Some(jar) = self.map.get(&key) {
            trace!(target: "providers::static_file", ?segment, ?fixed_block_range, "Jar found in cache");
            jar.into()
        } else {
            let generation = self.cache_generation.load(Ordering::Acquire);
            trace!(target: "providers::static_file", ?segment, ?fixed_block_range, generation, "Creating jar from scratch");
            let path = self.path.join(segment.filename(fixed_block_range));
            // TN(TN-0135): through the storage-v2 format check, here and at every jar load below.
            let jar = self.load_jar(segment, &path)?;
            let loaded = LoadedJar::new(jar)?;
            #[cfg(test)]
            self.before_cache_insert.fire(segment);

            // The cache may have been populated since the initial miss, including by
            // `update_index` publishing a newer snapshot while we loaded this jar without a lock.
            // Preserve that entry instead of overwriting it with our potentially stale snapshot.
            self.map
                .entry(key)
                .or_try_insert_with(|| -> ProviderResult<_> {
                    // Validate while holding the entry's shard lock so clearing the cache cannot
                    // slip between validation and insertion. If invalidated, reload once under
                    // this lock rather than retrying unboundedly during frequent pruning.
                    let loaded = if self.cache_generation.load(Ordering::Acquire) == generation {
                        loaded
                    } else {
                        trace!(target: "providers::static_file", ?segment, ?fixed_block_range, generation, "Reloading jar after cache invalidation");
                        drop(loaded);
                        LoadedJar::new(self.load_jar(segment, &path)?)?
                    };
                    #[cfg(test)]
                    self.after_cache_validation.fire(segment);
                    Ok(loaded)
                })?
                .downgrade()
                .into()
        };

        if let Some(metrics) = &self.metrics {
            provider = provider.with_metrics(metrics.clone());
        }
        Ok(provider)
    }

    /// Gets a static file segment's block range from the provider inner block
    /// index.
    fn get_segment_ranges_from_block(
        &self,
        segment: StaticFileSegment,
        block: u64,
    ) -> Option<SegmentRangeInclusive> {
        let indexes = self.indexes.read();
        let index = indexes.get(segment)?;

        (index.max_block >= block).then(|| {
            self.find_fixed_range_with_block_index(
                segment,
                Some(&index.expected_block_ranges_by_max_block),
                block,
            )
        })
    }

    /// Gets a static file segment's fixed block range from the provider inner
    /// transaction index.
    fn get_segment_ranges_from_transaction(
        &self,
        segment: StaticFileSegment,
        tx: u64,
    ) -> Option<SegmentRangeInclusive> {
        let indexes = self.indexes.read();
        let index = indexes.get(segment)?;
        let available_block_ranges_by_max_tx = index.available_block_ranges_by_max_tx.as_ref()?;

        // It's more probable that the request comes from a newer tx height, so we iterate
        // the static_files in reverse.
        let mut static_files_rev_iter = available_block_ranges_by_max_tx.iter().rev().peekable();

        while let Some((tx_end, block_range)) = static_files_rev_iter.next() {
            if tx > *tx_end {
                // request tx is higher than highest static file tx
                return None;
            }
            let tx_start = static_files_rev_iter.peek().map(|(tx_end, _)| *tx_end + 1).unwrap_or(0);
            if tx_start <= tx {
                return Some(self.find_fixed_range_with_block_index(
                    segment,
                    Some(&index.expected_block_ranges_by_max_block),
                    block_range.end(),
                ));
            }
        }
        None
    }

    /// TN(TN-0101): the file work of [`Self::update_index`], done before it takes the index
    /// lock: the segment's file range from the current index, its jar loaded and, when the
    /// segment grows past the indexed tip, the jar published to the cache.
    ///
    /// Publishing first is safe when the segment grows: lookups route by the index, which still
    /// stops at the old tip, and the new jar holds every row the old one did. A jar loaded on a
    /// cache miss can hold rows past the index tip in the same way (TN-0051). When the segment
    /// shrinks or stays, the jar is published under the lock with the index, as upstream does,
    /// so no lookup routed by the old index meets a jar without its rows.
    fn prepare_index_update(
        &self,
        segment: StaticFileSegment,
        segment_max_block: BlockNumber,
    ) -> ProviderResult<PreparedIndexUpdate> {
        let generation = self.cache_generation.load(Ordering::Acquire);
        let (fixed_range, grows) = {
            let indexes = self.indexes.read();
            let index = indexes.get(segment);
            let fixed_range = self.find_fixed_range_with_block_index(
                segment,
                index.map(|index| &index.expected_block_ranges_by_max_block),
                segment_max_block,
            );
            (fixed_range, index.is_none_or(|index| segment_max_block > index.max_block))
        };
        let jar = self.load_jar(segment, &self.path.join(segment.filename(&fixed_range)))?;
        let header = jar.user_header().clone();
        let loaded = LoadedJar::new(jar)?;
        let jar = if grows {
            debug!(target: "providers::static_file", ?segment, "Inserting grown jar into cache");
            self.map.insert((fixed_range.end(), segment), loaded);
            None
        } else {
            Some(loaded)
        };
        Ok(PreparedIndexUpdate { fixed_range, generation, header, jar })
    }

    /// Updates the inner transaction and block indexes alongside the internal cached providers in
    /// `self.map`.
    ///
    /// Any entry higher than `segment_max_block` will be deleted from the previous structures.
    ///
    /// If `segment_max_block` is None it means there's no static file for this segment.
    pub fn update_index(
        &self,
        segment: StaticFileSegment,
        segment_max_block: Option<BlockNumber>,
    ) -> ProviderResult<()> {
        debug!(
            target: "providers::static_file",
            ?segment,
            ?segment_max_block,
            "Updating provider index"
        );
        // TN(TN-0101): load the jar before taking the index lock, and when the segment grows,
        // publish it to the jar cache before the index too. Every static-file lookup of every
        // segment takes the index lock, so holding it across the file reads, the maps and the
        // wait for this jar's readers stalled all of them, storage v2's changeset reads included.
        let prepared = match segment_max_block {
            Some(segment_max_block) => Some(self.prepare_index_update(segment, segment_max_block)?),
            None => None,
        };
        let mut indexes = self.indexes.write();

        match segment_max_block {
            Some(segment_max_block) => {
                let fixed_range = self.find_fixed_range_with_block_index(
                    segment,
                    indexes.get(segment).map(|index| &index.expected_block_ranges_by_max_block),
                    segment_max_block,
                );

                // The prepared jar holds only if nothing re-initialized the index or moved the
                // segment's ranges since; otherwise load it under the lock, as upstream does.
                let prepared = prepared.filter(|prepared| {
                    prepared.fixed_range == fixed_range &&
                        prepared.generation == self.cache_generation.load(Ordering::Acquire)
                });
                let (header, unpublished) = match prepared {
                    Some(PreparedIndexUpdate { header, jar, .. }) => (header, jar),
                    None => {
                        let jar = self
                            .load_jar(segment, &self.path.join(segment.filename(&fixed_range)))?;
                        (jar.user_header().clone(), Some(LoadedJar::new(jar)?))
                    }
                };

                let index = indexes
                    .entry(segment)
                    .and_modify(|index| {
                        // Update max block
                        index.max_block = segment_max_block;

                        // Update expected block range index

                        // Remove all expected block ranges that are less than the new max block
                        index
                            .expected_block_ranges_by_max_block
                            .retain(|_, block_range| block_range.start() < fixed_range.start());
                        // Insert new expected block range
                        index
                            .expected_block_ranges_by_max_block
                            .insert(fixed_range.end(), fixed_range);
                    })
                    .or_insert_with(|| StaticFileSegmentIndex {
                        min_block_range: None,
                        max_block: segment_max_block,
                        expected_block_ranges_by_max_block: BTreeMap::from([(
                            fixed_range.end(),
                            fixed_range,
                        )]),
                        available_block_ranges_by_max_tx: None,
                    });

                // Update min_block to track the lowest block range of the segment.
                // This is initially set by initialize_index() on node startup, but must be updated
                // as the file grows to prevent stale values.
                //
                // Without this update, min_block can remain at genesis (e.g. Some([0..=0]) or None)
                // even after syncing to higher blocks (e.g. [0..=100]). A stale
                // min_block causes get_lowest_static_file_block() to return the
                // wrong end value, which breaks pruning logic that relies on it for
                // safety checks.
                //
                // Example progression:
                // 1. Node starts, initialize_index() sets min_block = [0..=0]
                // 2. Sync to block 100, this update sets min_block = [0..=100]
                // 3. Pruner calls get_lowest_static_file_block() -> returns 100 (correct). Without
                //    this update, it would incorrectly return 0 (stale)
                if let Some(current_block_range) = header.block_range() {
                    if let Some(min_block_range) = index.min_block_range.as_mut() {
                        // delete_jar WILL ALWAYS re-initialize all indexes, so we are always
                        // sure that current_min is always the lowest.
                        if current_block_range.start() == min_block_range.start() {
                            *min_block_range = current_block_range;
                        }
                    } else {
                        index.min_block_range = Some(current_block_range);
                    }
                }

                // Updates the tx index by first removing all entries which have a higher
                // block_start than our current static file.
                if let Some(tx_range) = header.tx_range() {
                    // Current block range has the same block start as `fixed_range``, but block end
                    // might be different if we are still filling this static file.
                    if let Some(current_block_range) = header.block_range() {
                        let tx_end = tx_range.end();

                        // Considering that `update_index` is called when we either append/truncate,
                        // we are sure that we are handling the latest data
                        // points.
                        //
                        // Here we remove every entry of the index that has a block start higher or
                        // equal than our current one. This is important in the case
                        // that we prune a lot of rows resulting in a file (and thus
                        // a higher block range) deletion.
                        if let Some(index) = index.available_block_ranges_by_max_tx.as_mut() {
                            index
                                .retain(|_, block_range| block_range.start() < fixed_range.start());
                            index.insert(tx_end, current_block_range);
                        } else {
                            index.available_block_ranges_by_max_tx =
                                Some(BTreeMap::from([(tx_end, current_block_range)]));
                        }
                    }
                } else if segment.is_tx_based() {
                    // The unwinded file has no more transactions/receipts. However, the highest
                    // block is within this files' block range. We only retain
                    // entries with block ranges before the current one.
                    if let Some(index) = index.available_block_ranges_by_max_tx.as_mut() {
                        index.retain(|_, block_range| block_range.start() < fixed_range.start());
                    }

                    // If the index is empty, just remove it.
                    index.available_block_ranges_by_max_tx.take_if(|index| index.is_empty());
                }

                // Update the cached provider, unless it was published before the lock.
                if let Some(loaded) = unpublished {
                    debug!(target: "providers::static_file", ?segment, "Inserting updated jar into cache");
                    self.map.insert((fixed_range.end(), segment), loaded);
                }

                // Delete any cached provider that no longer has an associated jar.
                // TN(TN-0101): find them under shared shard locks and remove only those;
                // `retain` write-locks every shard and so waits for every jar reader.
                debug!(target: "providers::static_file", ?segment, "Cleaning up jar map");
                let stale: Vec<_> = self
                    .map
                    .iter()
                    .map(|entry| *entry.key())
                    .filter(|(end, seg)| *seg == segment && *end > fixed_range.end())
                    .collect();
                for key in stale {
                    self.map.remove(&key);
                }
            }
            None => {
                debug!(target: "providers::static_file", ?segment, "Removing segment from index");
                indexes.remove(segment);
            }
        };

        debug!(target: "providers::static_file", ?segment, "Updated provider index");
        Ok(())
    }

    /// Initializes the inner transaction and block index
    pub fn initialize_index(&self) -> ProviderResult<()> {
        let mut indexes = self.indexes.write();
        indexes.clear();

        for (segment, headers) in &*iter_static_files(&self.path).map_err(ProviderError::other)? {
            // Update first and last block for each segment
            //
            // It's safe to call `expect` here, because every segment has at least one header
            // associated with it.
            let min_block_range = Some(headers.first().expect("headers are not empty").0);
            let max_block = headers.last().expect("headers are not empty").0.end();

            let mut expected_block_ranges_by_max_block = BTreeMap::default();
            let mut available_block_ranges_by_max_tx = None;

            for (block_range, header) in headers {
                // Update max expected block -> expected_block_range index
                expected_block_ranges_by_max_block
                    .insert(header.expected_block_end(), header.expected_block_range());

                // Update max tx -> block_range index
                if let Some(tx_range) = header.tx_range() {
                    let tx_end = tx_range.end();

                    available_block_ranges_by_max_tx
                        .get_or_insert_with(BTreeMap::default)
                        .insert(tx_end, *block_range);
                }
            }

            indexes.insert(
                segment,
                StaticFileSegmentIndex {
                    min_block_range,
                    max_block,
                    expected_block_ranges_by_max_block,
                    available_block_ranges_by_max_tx,
                },
            );
        }

        // If this is a re-initialization, invalidate in-flight cache fills before clearing the
        // cache. A fill that started before this point either reloads under its shard lock or
        // publishes before the clear reaches that shard and is removed by it.
        self.cache_generation.fetch_add(1, Ordering::AcqRel);
        self.map.clear();

        // initialize the expired history height to the lowest static file block
        if let Some(lowest_range) =
            indexes.get(StaticFileSegment::Transactions).and_then(|index| index.min_block_range)
        {
            // the earliest height is the lowest available block number
            self.earliest_history_height
                .store(lowest_range.start(), std::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    /// TN(TN-0133): errors when a segment's jars, as listed on disk, skip or overlap blocks
    /// between two of them: a jar is missing, or one below the highest is not full. The blocks of
    /// the gap would read as absent (a range walk comes back short, a point read finds no
    /// changeset). Only the joins between present jars are checked, not where the lowest jar
    /// starts, so a segment whose lowest jars were deleted on purpose (history expiry deletes the
    /// lowest first, `delete_segment_below_block`) passes.
    pub fn check_contiguity(&self) -> ProviderResult<()> {
        for (segment, jars) in &*iter_static_files(&self.path).map_err(ProviderError::other)? {
            for pair in jars.windows(2) {
                let (before, after) = (pair[0].0, pair[1].0);
                if after.start() != before.end() + 1 {
                    tracing::error!(
                        target: "reth::static_file",
                        %segment,
                        %before,
                        %after,
                        "Static files skip blocks between two jars"
                    );
                    return Err(ProviderError::other(StaticFileGapError::Jars {
                        segment,
                        before,
                        after,
                    }))
                }
            }
        }
        Ok(())
    }

    /// TN(TN-0133): the error for a read of `number` (a block, or a transaction of a transaction
    /// segment) that the index sent to `provider`'s jar, which does not hold it, while lower jars
    /// exist: the jar that held it is missing ([`Self::check_contiguity`] refuses that at
    /// startup). `None` when the jar holds `number`, or when `number` lies below the segment's
    /// lowest jar (deleted on purpose, or never written).
    fn gap_at(
        &self,
        segment: StaticFileSegment,
        provider: &StaticFileJarProvider<'_, N>,
        number: u64,
    ) -> Option<ProviderError> {
        let header = provider.user_header();
        let jar_blocks = header.block_range()?;
        let missing = if segment.is_tx_based() {
            // The index sends a transaction to the first jar whose last transaction is at or
            // past it, so only one below the jar's first can be missing, and only if the jar is
            // not the lowest.
            header.tx_range().is_some_and(|txs| number < txs.start()) &&
                self.get_lowest_range_start(segment).is_some_and(|low| jar_blocks.start() > low)
        } else {
            // The index sends only blocks up to the highest one here.
            !jar_blocks.contains(number) &&
                self.get_lowest_range_start(segment).is_some_and(|low| number >= low)
        };
        missing.then(|| {
            tracing::error!(
                target: "reth::static_file",
                %segment,
                number,
                path = %provider.data_path().display(),
                "No static file jar holds this number, between the segment's lowest and highest"
            );
            ProviderError::other(StaticFileGapError::Number { segment, number })
        })
    }

    /// Ensures that any broken invariants which cannot be healed on the spot return a pipeline
    /// target to unwind to.
    ///
    /// Two types of consistency checks are done for:
    ///
    /// 1) When a static file fails to commit but the underlying data was changed.
    /// 2) When a static file was committed, but the required database transaction was not.
    ///
    /// For 1) it can self-heal if `self.access.is_read_only()` is set to `false`. Otherwise, it
    /// will return an error.
    /// For 2) the invariants below are checked, and if broken, might require a pipeline unwind
    /// to heal.
    ///
    /// For each static file segment:
    /// * the corresponding database table should overlap or have continuity in their keys
    ///   ([`TxNumber`] or [`BlockNumber`]).
    /// * its highest block should match the stage checkpoint block number if it's equal or higher
    ///   than the corresponding database table last entry.
    ///
    /// Returns a [`Option`] of [`PipelineTarget::Unwind`] if any healing is further required.
    ///
    /// WARNING: No static file writer should be held before calling this function, otherwise it
    /// will deadlock.
    #[instrument(skip(self, provider), fields(read_only = self.is_read_only()))]
    pub fn check_consistency<Provider>(
        &self,
        provider: &Provider,
    ) -> ProviderResult<Option<PipelineTarget>>
    where
        Provider: DBProvider
            + BlockReader
            + StageCheckpointReader
            + PruneCheckpointReader
            + ChainSpecProvider
            + StorageSettingsCache,
        N: NodePrimitives<Receipt: Value, BlockHeader: Value, SignedTx: Value>,
    {
        // OVM historical import is broken and does not work with this check. It's importing
        // duplicated receipts resulting in having more receipts than the expected transaction
        // range.
        //
        // If we detect an OVM import was done (block #1 <https://optimistic.etherscan.io/block/1>), skip it.
        // More on [#11099](https://github.com/paradigmxyz/reth/pull/11099).
        if provider.chain_spec().is_optimism() &&
            reth_chainspec::Chain::optimism_mainnet() == provider.chain_spec().chain_id()
        {
            // check whether we have the first OVM block: <https://optimistic.etherscan.io/block/0xbee7192e575af30420cae0c7776304ac196077ee72b048970549e4f08e875453>
            const OVM_HEADER_1_HASH: B256 =
                b256!("0xbee7192e575af30420cae0c7776304ac196077ee72b048970549e4f08e875453");
            if provider.block_number(OVM_HEADER_1_HASH)?.is_some() {
                info!(target: "reth::cli",
                    "Skipping storage verification for OP mainnet, expected inconsistency in OVM chain"
                );
                return Ok(None);
            }
        }

        info!(target: "reth::cli", "Verifying storage consistency.");

        // TN(TN-0133): a jar missing between two others cannot be healed.
        self.check_contiguity()?;

        let mut unwind_target: Option<BlockNumber> = None;

        let mut update_unwind_target = |new_target| {
            unwind_target =
                unwind_target.map(|current| current.min(new_target)).or(Some(new_target));
        };

        for segment in self.segments_to_check(provider) {
            let span = info_span!(
                "Checking consistency for segment",
                ?segment,
                initial_highest_block = tracing::field::Empty,
                highest_block = tracing::field::Empty,
                highest_tx = tracing::field::Empty,
            );
            let _guard = span.enter();

            debug!(target: "reth::providers::static_file", "Checking consistency for segment");

            // Heal file-level inconsistencies and get before/after highest block
            let (initial_highest_block, mut highest_block) = self.maybe_heal_segment(segment)?;
            span.record("initial_highest_block", initial_highest_block);
            span.record("highest_block", highest_block);

            // Only applies to block-based static files. (Headers)
            //
            // The updated `highest_block` may have decreased if we healed from a pruning
            // interruption.
            if initial_highest_block != highest_block {
                info!(
                    target: "reth::providers::static_file",
                    unwind_target = highest_block,
                    "Setting unwind target."
                );
                update_unwind_target(highest_block.unwrap_or_default());
            }

            // Only applies to transaction-based static files. (Receipts & Transactions)
            //
            // Make sure the last transaction matches the last block from its indices, since a heal
            // from a pruning interruption might have decreased the number of transactions without
            // being able to update the last block of the static file segment.
            let highest_tx = self.get_highest_static_file_tx(segment);
            span.record("highest_tx", highest_tx);
            debug!(target: "reth::providers::static_file", "Checking tx index segment");

            if let Some(highest_tx) = highest_tx {
                let mut last_block = highest_block.unwrap_or_default();
                debug!(target: "reth::providers::static_file", last_block, highest_tx, "Verifying last transaction matches last block indices");
                loop {
                    let Some(indices) = provider.block_body_indices(last_block)? else {
                        debug!(target: "reth::providers::static_file", last_block, "Block body indices not found, static files ahead of database");
                        // If the block body indices can not be found, then it means that static
                        // files is ahead of database, and the `ensure_invariants` check will fix
                        // it by comparing with stage checkpoints.
                        break
                    };

                    debug!(target: "reth::providers::static_file", last_block, last_tx_num = indices.last_tx_num(), "Found block body indices");

                    if indices.last_tx_num() <= highest_tx {
                        break
                    }

                    if last_block == 0 {
                        debug!(target: "reth::providers::static_file", "Reached block 0 in verification loop");
                        break
                    }

                    last_block -= 1;

                    info!(
                        target: "reth::providers::static_file",
                        highest_block = self.get_highest_static_file_block(segment),
                        unwind_target = last_block,
                        "Setting unwind target."
                    );
                    span.record("highest_block", last_block);
                    highest_block = Some(last_block);
                    update_unwind_target(last_block);
                }
            }

            debug!(target: "reth::providers::static_file", "Ensuring invariants for segment");

            match self.ensure_invariants_for(provider, segment, highest_tx, highest_block)? {
                Some(unwind) => {
                    debug!(target: "reth::providers::static_file", unwind_target=unwind, "Invariants check returned unwind target");
                    update_unwind_target(unwind);
                }
                None => {
                    debug!(target: "reth::providers::static_file", "Invariants check completed, no unwind needed")
                }
            }
        }

        Ok(unwind_target.map(PipelineTarget::Unwind))
    }

    /// Heals file-level (`NippyJar`) inconsistencies for eligible static file
    /// segments.
    ///
    /// Call before [`Self::check_consistency`] so files are internally
    /// consistent.
    ///
    /// Uses the same segment-skip logic as [`Self::check_consistency`], but
    /// does not compare with database checkpoints or prune against them.
    pub fn check_file_consistency<Provider>(&self, provider: &Provider) -> ProviderResult<()>
    where
        Provider: DBProvider + ChainSpecProvider + StorageSettingsCache + PruneCheckpointReader,
    {
        info!(target: "reth::cli", "Healing static file inconsistencies.");

        // TN(TN-0133): a jar missing between two others cannot be healed; refuse before any heal.
        self.check_contiguity()?;

        for segment in self.segments_to_check(provider) {
            let _guard = info_span!("Healing static file segment", ?segment).entered();
            let _ = self.maybe_heal_segment(segment)?;
        }

        Ok(())
    }

    /// Returns the static file segments that should be checked/healed for this provider.
    fn segments_to_check<'a, Provider>(
        &'a self,
        provider: &'a Provider,
    ) -> impl Iterator<Item = StaticFileSegment> + 'a
    where
        Provider: DBProvider + ChainSpecProvider + StorageSettingsCache + PruneCheckpointReader,
    {
        StaticFileSegment::iter()
            .filter(move |segment| self.should_check_segment(provider, *segment))
    }

    /// True if the given segment should be checked/healed for this provider.
    fn should_check_segment<Provider>(
        &self,
        provider: &Provider,
        segment: StaticFileSegment,
    ) -> bool
    where
        Provider: DBProvider + ChainSpecProvider + StorageSettingsCache + PruneCheckpointReader,
    {
        match segment {
            StaticFileSegment::Headers | StaticFileSegment::Transactions => true,
            StaticFileSegment::Receipts => {
                if EitherWriter::receipts_destination(provider).is_database() {
                    // Old pruned nodes (including full node) do not store receipts as static
                    // files.
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping receipts segment: receipts stored in database");
                    return false;
                }

                if NamedChain::Gnosis == provider.chain_spec().chain_id() ||
                    NamedChain::Chiado == provider.chain_spec().chain_id()
                {
                    // Gnosis and Chiado's historical import is broken and does not work with
                    // this check. They are importing receipts along
                    // with importing headers/bodies.
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping receipts segment: broken historical import for gnosis/chiado");
                    return false;
                }

                true
            }
            StaticFileSegment::TransactionSenders => {
                if EitherWriterDestination::senders(provider).is_database() {
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping senders segment: senders stored in database");
                    return false;
                }

                if Self::is_segment_fully_pruned(provider, PruneSegment::SenderRecovery) {
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping senders segment: fully pruned");
                    return false;
                }

                true
            }
            StaticFileSegment::AccountChangeSets => {
                if EitherWriter::account_changesets_destination(provider).is_database() {
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping account changesets segment: changesets stored in database");
                    return false;
                }
                true
            }
            StaticFileSegment::StorageChangeSets => {
                if EitherWriter::storage_changesets_destination(provider).is_database() {
                    debug!(target: "reth::providers::static_file", ?segment, "Skipping storage changesets segment: changesets stored in database");
                    return false
                }
                true
            }
        }
    }

    /// Returns `true` if the given prune segment has a checkpoint with
    /// [`reth_prune_types::PruneMode::Full`], indicating all data for this segment has been
    /// intentionally deleted.
    fn is_segment_fully_pruned<Provider>(provider: &Provider, segment: PruneSegment) -> bool
    where
        Provider: PruneCheckpointReader,
    {
        provider
            .get_prune_checkpoint(segment)
            .ok()
            .flatten()
            .is_some_and(|checkpoint| checkpoint.prune_mode.is_full())
    }

    /// Checks consistency of the latest static file segment and throws an
    /// error if at fault.
    ///
    /// Read-only.
    fn check_segment_consistency(&self, segment: StaticFileSegment) -> ProviderResult<()> {
        debug!(target: "reth::providers::static_file", "Checking segment consistency");
        if let Some(latest_block) = self.get_highest_static_file_block(segment) {
            let file_path = self
                .directory()
                .join(segment.filename(&self.find_fixed_range(segment, latest_block)));
            debug!(target: "reth::providers::static_file", ?file_path, latest_block, "Loading NippyJar for consistency check");

            let jar = self.load_jar(segment, &file_path)?;
            debug!(target: "reth::providers::static_file", "NippyJar loaded, checking consistency");

            NippyJarChecker::new(jar).check_consistency().map_err(ProviderError::other)?;
            debug!(target: "reth::providers::static_file", "NippyJar consistency check passed");
        } else {
            debug!(target: "reth::providers::static_file", "No static file block found, skipping consistency check");
        }
        Ok(())
    }

    /// Attempts to heal file-level (`NippyJar`) inconsistencies for a single static file segment.
    ///
    /// Returns the highest block before and after healing, which can be used to detect
    /// if healing from a pruning interruption decreased the highest block.
    ///
    /// File consistency is broken if:
    ///
    /// * appending data was interrupted before a config commit, then data file will be truncated
    ///   according to the config.
    ///
    /// * pruning data was interrupted before a config commit, then we have deleted data that we are
    ///   expected to still have. We need to check the Database and unwind everything accordingly.
    ///
    /// **Note:** In read-only mode, this will return an error if a consistency issue is detected,
    /// since healing requires write access.
    fn maybe_heal_segment(
        &self,
        segment: StaticFileSegment,
    ) -> ProviderResult<(Option<BlockNumber>, Option<BlockNumber>)> {
        let initial_highest_block = self.get_highest_static_file_block(segment);
        debug!(target: "reth::providers::static_file", ?initial_highest_block, "Initial highest block for segment");

        if self.access.is_read_only() {
            // Read-only mode: cannot modify files, so just validate consistency and error if
            // broken.
            debug!(target: "reth::providers::static_file", "Checking segment consistency (read-only)");
            self.check_segment_consistency(segment)?;
        } else {
            // Writable mode: fetching the writer will automatically heal any file-level
            // inconsistency by truncating data to match the last committed config.
            debug!(target: "reth::providers::static_file", "Fetching latest writer which might heal any potential inconsistency");
            self.latest_writer(segment)?;
        }

        // The updated `highest_block` may have decreased if we healed from a
        // pruning interruption.
        let highest_block = self.get_highest_static_file_block(segment);

        Ok((initial_highest_block, highest_block))
    }

    /// Ensure invariants for each corresponding table and static file segment.
    fn ensure_invariants_for<Provider>(
        &self,
        provider: &Provider,
        segment: StaticFileSegment,
        highest_tx: Option<u64>,
        highest_block: Option<BlockNumber>,
    ) -> ProviderResult<Option<BlockNumber>>
    where
        Provider: DBProvider + BlockReader + StageCheckpointReader,
        N: NodePrimitives<Receipt: Value, BlockHeader: Value, SignedTx: Value>,
    {
        match segment {
            StaticFileSegment::Headers => self
                .ensure_invariants::<_, tables::Headers<N::BlockHeader>>(
                    provider,
                    segment,
                    highest_block,
                    highest_block,
                ),
            StaticFileSegment::Transactions => self
                .ensure_invariants::<_, tables::Transactions<N::SignedTx>>(
                    provider,
                    segment,
                    highest_tx,
                    highest_block,
                ),
            StaticFileSegment::Receipts => self
                .ensure_invariants::<_, tables::Receipts<N::Receipt>>(
                    provider,
                    segment,
                    highest_tx,
                    highest_block,
                ),
            StaticFileSegment::TransactionSenders => self
                .ensure_invariants::<_, tables::TransactionSenders>(
                    provider,
                    segment,
                    highest_tx,
                    highest_block,
                ),
            StaticFileSegment::AccountChangeSets => self
                .ensure_invariants::<_, tables::AccountChangeSets>(
                    provider,
                    segment,
                    highest_tx,
                    highest_block,
                ),
            StaticFileSegment::StorageChangeSets => self
                .ensure_changeset_invariants_by_block::<_, tables::StorageChangeSets, _>(
                    provider,
                    segment,
                    highest_block,
                    |key| key.block_number(),
                ),
        }
    }

    /// Check invariants for each corresponding table and static file segment:
    ///
    /// * the corresponding database table should overlap or have continuity in their keys
    ///   ([`TxNumber`] or [`BlockNumber`]).
    /// * its highest block should match the stage checkpoint block number if it's equal or higher
    ///   than the corresponding database table last entry.
    ///   * If the checkpoint block is higher, then request a pipeline unwind to the static file
    ///     block. This is expressed by returning [`Some`] with the requested pipeline unwind
    ///     target.
    ///   * If the checkpoint block is lower, then heal by removing rows from the static file. In
    ///     this case, the rows will be removed and [`None`] will be returned.
    ///
    /// * If the database tables overlap with static files and have contiguous keys, or the
    ///   checkpoint block matches the highest static files block, then [`None`] will be returned.
    #[instrument(skip(self, provider, segment), fields(table = T::NAME))]
    fn ensure_invariants<Provider, T: Table<Key = u64>>(
        &self,
        provider: &Provider,
        segment: StaticFileSegment,
        highest_static_file_entry: Option<u64>,
        highest_static_file_block: Option<BlockNumber>,
    ) -> ProviderResult<Option<BlockNumber>>
    where
        Provider: DBProvider + BlockReader + StageCheckpointReader,
    {
        debug!(target: "reth::providers::static_file", "Ensuring invariants");
        let mut db_cursor = provider.tx_ref().cursor_read::<T>()?;

        if let Some((db_first_entry, _)) = db_cursor.first()? {
            debug!(target: "reth::providers::static_file", db_first_entry, "Found first database entry");
            if let (Some(highest_entry), Some(highest_block)) =
                (highest_static_file_entry, highest_static_file_block)
            {
                // If there is a gap between the entry found in static file and
                // database, then we have most likely lost static file data and need to unwind so we
                // can load it again
                if !(db_first_entry <= highest_entry || highest_entry + 1 == db_first_entry) {
                    info!(
                        target: "reth::providers::static_file",
                        ?db_first_entry,
                        ?highest_entry,
                        unwind_target = highest_block,
                        "Setting unwind target."
                    );
                    return Ok(Some(highest_block));
                }
            }

            if let Some((db_last_entry, _)) = db_cursor.last()? &&
                highest_static_file_entry
                    .is_none_or(|highest_entry| db_last_entry > highest_entry)
            {
                debug!(target: "reth::providers::static_file", db_last_entry, "Database has entries beyond static files, no unwind needed");
                return Ok(None)
            }
        } else {
            debug!(target: "reth::providers::static_file", "No database entries found");
        }

        let highest_static_file_entry = highest_static_file_entry.unwrap_or_default();
        let highest_static_file_block = highest_static_file_block.unwrap_or_default();

        // If static file entry is ahead of the database entries, then ensure the checkpoint block
        // number matches.
        let stage_id = segment.to_stage_id();
        let checkpoint_block_number =
            provider.get_stage_checkpoint(stage_id)?.unwrap_or_default().block_number;
        debug!(target: "reth::providers::static_file", ?stage_id, checkpoint_block_number, "Retrieved stage checkpoint");

        // If the checkpoint is ahead, then we lost static file data. May be data corruption.
        if checkpoint_block_number > highest_static_file_block {
            info!(
                target: "reth::providers::static_file",
                checkpoint_block_number,
                unwind_target = highest_static_file_block,
                "Setting unwind target."
            );
            return Ok(Some(highest_static_file_block));
        }

        // If the checkpoint is ahead, or matches, then nothing to do.
        if checkpoint_block_number >= highest_static_file_block {
            debug!(target: "reth::providers::static_file", "Invariants ensured, returning None");
            return Ok(None);
        }

        // If the checkpoint is behind, then we failed to do a database commit
        // **but committed** to static files on executing a stage, or the
        // reverse on unwinding a stage.
        //
        // All we need to do is to prune the extra static file rows.
        info!(
            target: "reth::providers",
            from = highest_static_file_block,
            to = checkpoint_block_number,
            "Unwinding static file segment."
        );
        let mut writer = self.latest_writer(segment)?;

        match segment {
            StaticFileSegment::Headers => {
                let prune_count = highest_static_file_block - checkpoint_block_number;
                debug!(target: "reth::providers::static_file", prune_count, "Pruning headers");
                // TODO(joshie): is_block_meta
                writer.prune_headers(prune_count)?;
            }
            StaticFileSegment::Transactions |
            StaticFileSegment::Receipts |
            StaticFileSegment::TransactionSenders => {
                if let Some(block) = provider.block_body_indices(checkpoint_block_number)? {
                    // `last_tx_num()` saturates to zero for an empty genesis block, but row zero
                    // belongs to the first non-empty block and must be removed as well.
                    let number = highest_static_file_entry
                        .saturating_add(1)
                        .saturating_sub(block.next_tx_num());
                    debug!(target: "reth::providers::static_file", prune_count = number, checkpoint_block_number, "Pruning transaction based segment");

                    match segment {
                        StaticFileSegment::Transactions => {
                            writer.prune_transactions(number, checkpoint_block_number)?
                        }
                        StaticFileSegment::Receipts => {
                            writer.prune_receipts(number, checkpoint_block_number)?
                        }
                        StaticFileSegment::TransactionSenders => {
                            writer.prune_transaction_senders(number, checkpoint_block_number)?
                        }
                        StaticFileSegment::Headers |
                        StaticFileSegment::AccountChangeSets |
                        StaticFileSegment::StorageChangeSets => {
                            unreachable!()
                        }
                    }
                } else {
                    debug!(target: "reth::providers::static_file", checkpoint_block_number, "No block body indices found for checkpoint block");
                }
            }
            StaticFileSegment::AccountChangeSets => {
                writer.prune_account_changesets(checkpoint_block_number)?;
            }
            StaticFileSegment::StorageChangeSets => {
                writer.prune_storage_changesets(checkpoint_block_number)?;
            }
        }

        debug!(target: "reth::providers::static_file", "Committing writer after pruning");
        writer.commit()?;
        debug!(target: "reth::providers::static_file", "Writer committed successfully");

        debug!(target: "reth::providers::static_file", "Invariants ensured, returning None");
        Ok(None)
    }

    fn ensure_changeset_invariants_by_block<Provider, T, F>(
        &self,
        provider: &Provider,
        segment: StaticFileSegment,
        highest_static_file_block: Option<BlockNumber>,
        block_from_key: F,
    ) -> ProviderResult<Option<BlockNumber>>
    where
        Provider: DBProvider + BlockReader + StageCheckpointReader,
        T: Table,
        F: Fn(&T::Key) -> BlockNumber,
    {
        debug!(
            target: "reth::providers::static_file",
            ?segment,
            ?highest_static_file_block,
            "Ensuring changeset invariants"
        );
        let mut db_cursor = provider.tx_ref().cursor_read::<T>()?;

        if let Some((db_first_key, _)) = db_cursor.first()? {
            let db_first_block = block_from_key(&db_first_key);
            if let Some(highest_block) = highest_static_file_block &&
                !(db_first_block <= highest_block || highest_block + 1 == db_first_block)
            {
                info!(
                    target: "reth::providers::static_file",
                    ?db_first_block,
                    ?highest_block,
                    unwind_target = highest_block,
                    ?segment,
                    "Setting unwind target."
                );
                return Ok(Some(highest_block))
            }

            if let Some((db_last_key, _)) = db_cursor.last()? &&
                highest_static_file_block
                    .is_none_or(|highest_block| block_from_key(&db_last_key) > highest_block)
            {
                debug!(
                    target: "reth::providers::static_file",
                    ?segment,
                    "Database has entries beyond static files, no unwind needed"
                );
                return Ok(None)
            }
        } else {
            debug!(target: "reth::providers::static_file", ?segment, "No database entries found");
        }

        let highest_static_file_block = highest_static_file_block.unwrap_or_default();

        let stage_id = segment.to_stage_id();
        let checkpoint_block_number =
            provider.get_stage_checkpoint(stage_id)?.unwrap_or_default().block_number;

        if checkpoint_block_number > highest_static_file_block {
            info!(
                target: "reth::providers::static_file",
                checkpoint_block_number,
                unwind_target = highest_static_file_block,
                ?segment,
                "Setting unwind target."
            );
            return Ok(Some(highest_static_file_block))
        }

        if checkpoint_block_number < highest_static_file_block {
            info!(
                target: "reth::providers",
                ?segment,
                from = highest_static_file_block,
                to = checkpoint_block_number,
                "Unwinding static file segment."
            );
            let mut writer = self.latest_writer(segment)?;
            match segment {
                StaticFileSegment::AccountChangeSets => {
                    writer.prune_account_changesets(checkpoint_block_number)?;
                }
                StaticFileSegment::StorageChangeSets => {
                    writer.prune_storage_changesets(checkpoint_block_number)?;
                }
                _ => unreachable!("invalid segment for changeset invariants"),
            }
            writer.commit()?;
        }

        Ok(None)
    }

    /// Returns the earliest available block number that has not been expired and is still
    /// available.
    ///
    /// This means that the highest expired block (or expired block height) is
    /// `earliest_history_height.saturating_sub(1)`.
    ///
    /// Returns `0` if no history has been expired.
    pub fn earliest_history_height(&self) -> BlockNumber {
        self.earliest_history_height.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Gets the lowest static file's block range if it exists for a static file segment.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_lowest_range(&self, segment: StaticFileSegment) -> Option<SegmentRangeInclusive> {
        self.indexes.read().get(segment).and_then(|index| index.min_block_range)
    }

    /// Gets the lowest static file's block range start if it exists for a static file segment.
    ///
    /// For example if the lowest static file has blocks 0-499, this will return 0.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_lowest_range_start(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        self.get_lowest_range(segment).map(|range| range.start())
    }

    /// Gets the lowest static file's block range end if it exists for a static file segment.
    ///
    /// For example if the static file has blocks 0-499, this will return 499.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_lowest_range_end(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        self.get_lowest_range(segment).map(|range| range.end())
    }

    /// Gets the highest static file's block height if it exists for a static file segment.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_highest_static_file_block(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        self.indexes.read().get(segment).map(|index| index.max_block)
    }

    /// Converts a range to a bounded `RangeInclusive` capped to the highest static file block.
    ///
    /// This is necessary because static file iteration beyond the tip would loop forever:
    /// blocks beyond the static file tip return `Ok(empty)` which is indistinguishable from
    /// blocks with no changes. We cap the end to the highest available block regardless of
    /// whether the input was unbounded or an explicit large value like `BlockNumber::MAX`.
    fn bound_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
        segment: StaticFileSegment,
    ) -> RangeInclusive<BlockNumber> {
        let highest_block = self.get_highest_static_file_block(segment).unwrap_or(0);

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&n) => n.min(highest_block),
            Bound::Excluded(&n) => n.saturating_sub(1).min(highest_block),
            Bound::Unbounded => highest_block,
        };

        start..=end
    }

    /// Gets the highest static file transaction.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_highest_static_file_tx(&self, segment: StaticFileSegment) -> Option<TxNumber> {
        self.indexes
            .read()
            .get(segment)
            .and_then(|index| index.available_block_ranges_by_max_tx.as_ref())
            .and_then(|index| index.last_key_value().map(|(last_tx, _)| *last_tx))
    }

    /// Gets the highest static file block for all segments.
    pub fn get_highest_static_files(&self) -> HighestStaticFiles {
        HighestStaticFiles {
            receipts: self.get_highest_static_file_block(StaticFileSegment::Receipts),
        }
    }

    /// Iterates through segment `static_files` in reverse order, executing a function until it
    /// returns some object. Useful for finding objects by [`TxHash`] or [`BlockHash`].
    pub fn find_static_file<T>(
        &self,
        segment: StaticFileSegment,
        func: impl Fn(StaticFileJarProvider<'_, N>) -> ProviderResult<Option<T>>,
    ) -> ProviderResult<Option<T>> {
        if let Some(ranges) =
            self.indexes.read().get(segment).map(|index| &index.expected_block_ranges_by_max_block)
        {
            // Iterate through all ranges in reverse order (highest to lowest)
            for range in ranges.values().rev() {
                if let Some(res) = func(self.get_or_create_jar_provider(segment, range)?)? {
                    return Ok(Some(res));
                }
            }
        }

        Ok(None)
    }

    /// Fetches data within a specified range across multiple static files.
    ///
    /// This function iteratively retrieves data using `get_fn` for each item in the given range.
    /// It continues fetching until the end of the range is reached or the provided `predicate`
    /// returns false.
    pub fn fetch_range_with_predicate<T, F, P>(
        &self,
        segment: StaticFileSegment,
        range: Range<u64>,
        mut get_fn: F,
        mut predicate: P,
    ) -> ProviderResult<Vec<T>>
    where
        F: FnMut(&mut StaticFileCursor<'_>, u64) -> ProviderResult<Option<T>>,
        P: FnMut(&T) -> bool,
    {
        let mut result = Vec::with_capacity((range.end - range.start).min(100) as usize);

        /// Resolves to the provider for the given block or transaction number.
        ///
        /// If the static file is missing, the `result` is returned.
        macro_rules! get_provider {
            ($number:expr) => {{
                match self.get_segment_provider(segment, $number) {
                    Ok(provider) => provider,
                    Err(
                        ProviderError::MissingStaticFileBlock(_, _) |
                        ProviderError::MissingStaticFileTx(_, _),
                    ) => return Ok(result),
                    Err(err) => return Err(err),
                }
            }};
        }

        let mut provider = get_provider!(range.start);
        let mut cursor = provider.cursor()?;

        // advances number in range
        'outer: for number in range {
            // The `retrying` flag ensures a single retry attempt per `number`. If `get_fn` fails to
            // access data in two different static files, it halts further attempts by returning
            // an error, effectively preventing infinite retry loops.
            let mut retrying = false;

            // advances static files if `get_fn` returns None
            'inner: loop {
                match get_fn(&mut cursor, number)? {
                    Some(res) => {
                        if !predicate(&res) {
                            break 'outer;
                        }
                        result.push(res);
                        break 'inner;
                    }
                    None => {
                        if retrying {
                            // TN(TN-0121): a number inside the jar's own range has a row: the
                            // range and the row count come from one configuration. Without one
                            // the jar is damaged; outside it the data ends (or starts later).
                            let header = provider.user_header();
                            let jar_range = if segment.is_tx_based() {
                                header.tx_range()
                            } else {
                                header.block_range()
                            };
                            if jar_range.is_some_and(|range| range.contains(number)) {
                                return Err(ProviderError::other(NippyJarError::Custom(format!(
                                    "static file {} has no row for {number}, inside its range \
                                     {jar_range:?}",
                                    provider.data_path().display()
                                ))))
                            }
                            // TN(TN-0133): nor is a number in a gap between jars the end.
                            if let Some(err) = self.gap_at(segment, &provider, number) {
                                return Err(err)
                            }
                            return Ok(result);
                        }
                        // There is a very small chance of hitting a deadlock if two consecutive
                        // static files share the same bucket in the
                        // internal dashmap and we don't drop the current provider
                        // before requesting the next one.
                        drop(cursor);
                        drop(provider);
                        provider = get_provider!(number);
                        cursor = provider.cursor()?;
                        retrying = true;
                    }
                }
            }
        }

        result.shrink_to_fit();

        Ok(result)
    }

    /// Fetches data within a specified range across multiple static files.
    ///
    /// Returns an iterator over the data. Yields [`None`] if the data for the specified number is
    /// not found.
    pub fn fetch_range_iter<'a, T, F>(
        &'a self,
        segment: StaticFileSegment,
        range: Range<u64>,
        get_fn: F,
    ) -> ProviderResult<impl Iterator<Item = ProviderResult<Option<T>>> + 'a>
    where
        F: Fn(&mut StaticFileCursor<'_>, u64) -> ProviderResult<Option<T>> + 'a,
        T: std::fmt::Debug,
    {
        let mut provider = self.get_maybe_segment_provider(segment, range.start)?;
        Ok(range.map(move |number| {
            match provider
                .as_ref()
                .map(|provider| get_fn(&mut provider.cursor()?, number))
                .and_then(|result| result.transpose())
            {
                Some(result) => result.map(Some),
                None => {
                    // There is a very small chance of hitting a deadlock if two consecutive
                    // static files share the same bucket in the internal dashmap and we don't drop
                    // the current provider before requesting the next one.
                    provider.take();
                    provider = self.get_maybe_segment_provider(segment, number)?;
                    provider
                        .as_ref()
                        .map(|provider| get_fn(&mut provider.cursor()?, number))
                        .and_then(|result| result.transpose())
                        .transpose()
                }
            }
        }))
    }

    /// Returns directory where `static_files` are located.
    pub fn directory(&self) -> &Path {
        &self.path
    }

    /// Retrieves data from the database or static file, wherever it's available.
    ///
    /// # Arguments
    /// * `segment` - The segment of the static file to check against.
    /// * `index_key` - Requested index key, usually a block or transaction number.
    /// * `fetch_from_static_file` - A closure that defines how to fetch the data from the static
    ///   file provider.
    /// * `fetch_from_database` - A closure that defines how to fetch the data from the database
    ///   when the static file doesn't contain the required data or is not available.
    pub fn get_with_static_file_or_database<T, FS, FD>(
        &self,
        segment: StaticFileSegment,
        number: u64,
        fetch_from_static_file: FS,
        fetch_from_database: FD,
    ) -> ProviderResult<Option<T>>
    where
        FS: Fn(&Self) -> ProviderResult<Option<T>>,
        FD: Fn() -> ProviderResult<Option<T>>,
    {
        // If there is, check the maximum block or transaction number of the segment.
        let static_file_upper_bound = if segment.is_block_or_change_based() {
            self.get_highest_static_file_block(segment)
        } else {
            self.get_highest_static_file_tx(segment)
        };

        if static_file_upper_bound
            .is_some_and(|static_file_upper_bound| static_file_upper_bound >= number)
        {
            return fetch_from_static_file(self);
        }
        fetch_from_database()
    }

    /// Gets data within a specified range, potentially spanning different `static_files` and
    /// database.
    ///
    /// # Arguments
    /// * `segment` - The segment of the static file to query.
    /// * `block_or_tx_range` - The range of data to fetch.
    /// * `fetch_from_static_file` - A function to fetch data from the `static_file`.
    /// * `fetch_from_database` - A function to fetch data from the database.
    /// * `predicate` - A function used to evaluate each item in the fetched data. Fetching is
    ///   terminated when this function returns false, thereby filtering the data based on the
    ///   provided condition.
    pub fn get_range_with_static_file_or_database<T, P, FS, FD>(
        &self,
        segment: StaticFileSegment,
        mut block_or_tx_range: Range<u64>,
        fetch_from_static_file: FS,
        mut fetch_from_database: FD,
        mut predicate: P,
    ) -> ProviderResult<Vec<T>>
    where
        FS: Fn(&Self, Range<u64>, &mut P) -> ProviderResult<Vec<T>>,
        FD: FnMut(Range<u64>, P) -> ProviderResult<Vec<T>>,
        P: FnMut(&T) -> bool,
    {
        let mut data = Vec::new();

        // If there is, check the maximum block or transaction number of the segment.
        if let Some(static_file_upper_bound) = if segment.is_block_or_change_based() {
            self.get_highest_static_file_block(segment)
        } else {
            self.get_highest_static_file_tx(segment)
        } && block_or_tx_range.start <= static_file_upper_bound
        {
            let end = block_or_tx_range.end.min(static_file_upper_bound + 1);
            data.extend(fetch_from_static_file(
                self,
                block_or_tx_range.start..end,
                &mut predicate,
            )?);
            block_or_tx_range.start = end;
        }

        if block_or_tx_range.end > block_or_tx_range.start {
            data.extend(fetch_from_database(block_or_tx_range, predicate)?)
        }

        Ok(data)
    }

    /// Returns static files directory
    #[cfg(any(test, feature = "test-utils"))]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns transaction index
    #[cfg(any(test, feature = "test-utils"))]
    pub fn tx_index(&self, segment: StaticFileSegment) -> Option<SegmentRanges> {
        self.indexes
            .read()
            .get(segment)
            .and_then(|index| index.available_block_ranges_by_max_tx.as_ref())
            .cloned()
    }

    /// Returns expected block index
    #[cfg(any(test, feature = "test-utils"))]
    pub fn expected_block_index(&self, segment: StaticFileSegment) -> Option<SegmentRanges> {
        self.indexes
            .read()
            .get(segment)
            .map(|index| &index.expected_block_ranges_by_max_block)
            .cloned()
    }
}

#[derive(Debug)]
struct StaticFileSegmentIndex {
    /// Min static file block range.
    ///
    /// This index is initialized on launch to keep track of the lowest, non-expired static file
    /// per segment and gets updated on [`StaticFileProvider::update_index`].
    ///
    /// This tracks the lowest static file per segment together with the block range in that
    /// file. E.g. static file is batched in 500k block intervals then the lowest static file
    /// is [0..499K], and the block range is start = 0, end = 499K.
    ///
    /// This index is mainly used for history expiry, which targets transactions, e.g. pre-merge
    /// history expiry would lead to removing all static files below the merge height.
    min_block_range: Option<SegmentRangeInclusive>,
    /// Max static file block.
    max_block: u64,
    /// Expected static file block ranges indexed by max expected blocks.
    ///
    /// For example, a static file for expected block range `0..=499_000` may have only block range
    /// `0..=1000` contained in it, as it's not fully filled yet. This index maps the max expected
    /// block to the expected range, i.e. block `499_000` to block range `0..=499_000`.
    expected_block_ranges_by_max_block: SegmentRanges,
    /// Available on disk static file block ranges indexed by max transactions.
    ///
    /// For example, a static file for block range `0..=499_000` may only have block range
    /// `0..=1000` and transaction range `0..=2000` contained in it. This index maps the max
    /// available transaction to the available block range, i.e. transaction `2000` to block range
    /// `0..=1000`.
    available_block_ranges_by_max_tx: Option<SegmentRanges>,
}

/// Helper trait to manage different [`StaticFileProviderRW`] of an `Arc<StaticFileProvider`
pub trait StaticFileWriter {
    /// The primitives type used by the static file provider.
    type Primitives: Send + Sync + 'static;

    /// Returns a mutable reference to a [`StaticFileProviderRW`] of a [`StaticFileSegment`].
    fn get_writer(
        &self,
        block: BlockNumber,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>>;

    /// Returns a mutable reference to a [`StaticFileProviderRW`] of the latest
    /// [`StaticFileSegment`].
    fn latest_writer(
        &self,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>>;

    /// Commits all changes of all [`StaticFileProviderRW`] of all [`StaticFileSegment`].
    fn commit(&self) -> ProviderResult<()>;

    /// Returns `true` if the static file provider has unwind queued.
    fn has_unwind_queued(&self) -> bool;

    /// Finalizes all static file writers by committing their configuration to disk.
    ///
    /// Returns an error if prune is queued (use [`Self::commit`] instead).
    fn finalize(&self) -> ProviderResult<()>;
}

impl<N: NodePrimitives> StaticFileWriter for StaticFileProvider<N> {
    type Primitives = N;

    fn get_writer(
        &self,
        block: BlockNumber,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>> {
        if self.access.is_read_only() {
            return Err(ProviderError::ReadOnlyStaticFileAccess);
        }

        trace!(target: "providers::static_file", ?block, ?segment, "Getting static file writer.");
        self.writers.get_or_create(segment, || {
            StaticFileProviderRW::new(segment, block, Arc::downgrade(&self.0), self.metrics.clone())
        })
    }

    fn latest_writer(
        &self,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>> {
        let genesis_number = self.0.as_ref().genesis_block_number();
        self.get_writer(
            self.get_highest_static_file_block(segment).unwrap_or(genesis_number),
            segment,
        )
    }

    fn commit(&self) -> ProviderResult<()> {
        self.writers.commit()
    }

    fn has_unwind_queued(&self) -> bool {
        self.writers.has_unwind_queued()
    }

    fn finalize(&self) -> ProviderResult<()> {
        self.writers.finalize()
    }
}

impl<N: NodePrimitives> ChangeSetReader for StaticFileProvider<N> {
    fn account_block_changeset(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<Vec<reth_db::models::AccountBeforeTx>> {
        let provider = match self.get_segment_provider_for_block(
            StaticFileSegment::AccountChangeSets,
            block_number,
            None,
        ) {
            Ok(provider) => provider,
            Err(ProviderError::MissingStaticFileBlock(_, _)) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        // TN(TN-0133): a block in a gap between jars is an error, not an empty changeset.
        if let Some(err) =
            self.gap_at(StaticFileSegment::AccountChangeSets, &provider, block_number)
        {
            return Err(err)
        }

        if let Some(offset) = provider.read_changeset_offset(block_number)? {
            let mut cursor = provider.cursor()?;
            let mut changeset = Vec::with_capacity(offset.num_changes() as usize);

            for i in offset.changeset_range() {
                // TN(TN-0121): a row missing inside the block's range is damage, not a shorter
                // changeset.
                let change = cursor
                    .get_one::<reth_db::static_file::AccountChangesetMask>(i.into())?
                    .ok_or(ProviderError::CorruptedChangeSetStaticFile)?;
                changeset.push(change)
            }
            Ok(changeset)
        } else {
            Ok(Vec::new())
        }
    }

    fn get_account_before_block(
        &self,
        block_number: BlockNumber,
        address: Address,
    ) -> ProviderResult<Option<reth_db::models::AccountBeforeTx>> {
        let provider = match self.get_segment_provider_for_block(
            StaticFileSegment::AccountChangeSets,
            block_number,
            None,
        ) {
            Ok(provider) => provider,
            Err(ProviderError::MissingStaticFileBlock(_, _)) => return Ok(None),
            Err(err) => return Err(err),
        };

        let Some(offset) = provider.read_changeset_offset(block_number)? else {
            return Ok(None);
        };

        let mut cursor = provider.cursor()?;
        let range = offset.changeset_range();
        // TN(TN-0124): an account change's stored value starts with its address.
        let low = changeset_lower_bound(&mut cursor, range.clone(), address.as_slice())?;

        if low < range.end &&
            let Some(change) = cursor
                .get_one::<reth_db::static_file::AccountChangesetMask>(low.into())?
                .filter(|change| change.address == address)
        {
            return Ok(Some(change));
        }

        changeset_lookup_missed(&mut cursor, range, address.as_slice())?;
        Ok(None)
    }

    fn account_changesets_range(
        &self,
        range: impl core::ops::RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<(BlockNumber, reth_db::models::AccountBeforeTx)>> {
        let range = self.bound_range(range, StaticFileSegment::AccountChangeSets);
        self.walk_account_changeset_range(range).collect()
    }

    fn account_changeset_count(&self) -> ProviderResult<usize> {
        let mut count = 0;

        let static_files = iter_static_files(&self.path).map_err(ProviderError::other)?;
        if let Some(changeset_segments) = static_files.get(StaticFileSegment::AccountChangeSets) {
            for (block_range, header) in changeset_segments {
                let data_path =
                    self.path.join(StaticFileSegment::AccountChangeSets.filename(block_range));
                let csoff_path = data_path.with_extension("csoff");
                if csoff_path.exists() {
                    let len = header.changeset_offsets_len();
                    // TN(TN-0121): the jar's configuration says whether its records carry
                    // checksums.
                    let checksums = NippyJar::<SegmentHeader>::load(&data_path)
                        .map_err(ProviderError::other)?
                        .has_checksums();
                    let reader = ChangesetOffsetReader::new(&csoff_path, len, checksums)
                        .map_err(ProviderError::other)?;
                    let offsets = reader.get_range(0, len).map_err(ProviderError::other)?;
                    for offset in offsets {
                        count += offset.num_changes() as usize;
                    }
                }
            }
        }

        Ok(count)
    }
}

/// TN(TN-0124): the first row of a block's changeset `range` whose key is not below `key`, the
/// key being the leading bytes of the stored value (`AccountBeforeTx` and `StorageBeforeTx` encode
/// the address, then the plain slot, first and raw, so byte order is key order).
///
/// The probes compare the stored bytes without verifying or decoding them; the caller reads the
/// row it settles on through a verified read and compares its decoded key, so a damaged probe can
/// only end the search at a row that does not match: not found, which every history caller turns
/// into an error (`AccountChangesetNotFound` / `StorageChangesetNotFound`). To report the damage
/// itself, [`changeset_lookup_missed`] reruns the search verifying every probe.
pub(crate) fn changeset_lower_bound(
    cursor: &mut StaticFileCursor<'_>,
    range: Range<u64>,
    key: &[u8],
) -> ProviderResult<u64> {
    changeset_search(cursor, range, key, false)
}

/// TN(TN-0124): a point lookup that found no row reruns its search with every probe verified,
/// when the jar has checksums, so a damaged probe that misdirected it is reported as the checksum
/// error rather than as a missing change. A true miss returns `Ok(())`.
fn changeset_lookup_missed(
    cursor: &mut StaticFileCursor<'_>,
    range: Range<u64>,
    key: &[u8],
) -> ProviderResult<()> {
    if cursor.jar().has_checksums() {
        changeset_search(cursor, range, key, true)?;
    }
    Ok(())
}

fn changeset_search(
    cursor: &mut StaticFileCursor<'_>,
    range: Range<u64>,
    key: &[u8],
    verify: bool,
) -> ProviderResult<u64> {
    let (mut low, mut high) = (range.start, range.end);
    while low < high {
        let mid = low + (high - low) / 2;
        // `read_changeset_offset` checked the range against the jar's rows, so every probe has a
        // row; a stored value shorter than the key is damage.
        let below = if verify {
            let row =
                cursor.get(mid.into(), 0b1)?.ok_or(ProviderError::CorruptedChangeSetStaticFile)?;
            row[0].get(..key.len()).ok_or(ProviderError::CorruptedChangeSetStaticFile)? < key
        } else {
            let stored = cursor
                .unverified_value(mid as usize, 0)
                .map_err(ProviderError::other)?
                .ok_or(ProviderError::CorruptedChangeSetStaticFile)?;
            stored.get(..key.len()).ok_or(ProviderError::CorruptedChangeSetStaticFile)? < key
        };
        if below {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok(low)
}

impl<N: NodePrimitives> StorageChangeSetReader for StaticFileProvider<N> {
    fn storage_changeset(
        &self,
        block_number: BlockNumber,
    ) -> ProviderResult<Vec<(BlockNumberAddress, ChangesetEntry)>> {
        let provider = match self.get_segment_provider_for_block(
            StaticFileSegment::StorageChangeSets,
            block_number,
            None,
        ) {
            Ok(provider) => provider,
            Err(ProviderError::MissingStaticFileBlock(_, _)) => return Ok(Vec::new()),
            Err(err) => return Err(err),
        };
        // TN(TN-0133): a block in a gap between jars is an error, not an empty changeset.
        if let Some(err) =
            self.gap_at(StaticFileSegment::StorageChangeSets, &provider, block_number)
        {
            return Err(err)
        }

        if let Some(offset) = provider.read_changeset_offset(block_number)? {
            let mut cursor = provider.cursor()?;
            let mut changeset = Vec::with_capacity(offset.num_changes() as usize);

            for i in offset.changeset_range() {
                // TN(TN-0121): a row missing inside the block's range is damage, not a shorter
                // changeset.
                let change = cursor
                    .get_one::<StorageChangesetMask>(i.into())?
                    .ok_or(ProviderError::CorruptedChangeSetStaticFile)?;
                let block_address = BlockNumberAddress((block_number, change.address));
                // TN(TN-0044): static-file storage changesets hold plain slots.
                let entry =
                    ChangesetEntry { key: StorageSlotKey::plain(change.key), value: change.value };
                changeset.push((block_address, entry));
            }
            Ok(changeset)
        } else {
            Ok(Vec::new())
        }
    }

    fn get_storage_before_block(
        &self,
        block_number: BlockNumber,
        address: Address,
        storage_key: B256,
    ) -> ProviderResult<Option<ChangesetEntry>> {
        let provider = match self.get_segment_provider_for_block(
            StaticFileSegment::StorageChangeSets,
            block_number,
            None,
        ) {
            Ok(provider) => provider,
            Err(ProviderError::MissingStaticFileBlock(_, _)) => return Ok(None),
            Err(err) => return Err(err),
        };

        let Some(offset) = provider.read_changeset_offset(block_number)? else {
            return Ok(None);
        };

        let mut cursor = provider.cursor()?;
        let range = offset.changeset_range();
        // TN(TN-0124): a storage change's stored value starts with its address and plain slot.
        let mut key = [0u8; 52];
        key[..20].copy_from_slice(address.as_slice());
        key[20..].copy_from_slice(storage_key.as_slice());
        let low = changeset_lower_bound(&mut cursor, range.clone(), &key)?;

        if low < range.end &&
            let Some(change) = cursor
                .get_one::<StorageChangesetMask>(low.into())?
                .filter(|change| change.address == address && change.key == storage_key)
        {
            // TN(TN-0044): static-file storage changesets hold plain slots.
            return Ok(Some(ChangesetEntry {
                key: StorageSlotKey::plain(change.key),
                value: change.value,
            }));
        }

        changeset_lookup_missed(&mut cursor, range, &key)?;
        Ok(None)
    }

    fn storage_changesets_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<(BlockNumberAddress, ChangesetEntry)>> {
        let range = self.bound_range(range, StaticFileSegment::StorageChangeSets);
        self.walk_storage_changeset_range(range).collect()
    }

    fn storage_changeset_count(&self) -> ProviderResult<usize> {
        let mut count = 0;

        let static_files = iter_static_files(&self.path).map_err(ProviderError::other)?;
        if let Some(changeset_segments) = static_files.get(StaticFileSegment::StorageChangeSets) {
            for (block_range, header) in changeset_segments {
                let data_path =
                    self.path.join(StaticFileSegment::StorageChangeSets.filename(block_range));
                let csoff_path = data_path.with_extension("csoff");
                if csoff_path.exists() {
                    let len = header.changeset_offsets_len();
                    // TN(TN-0121): the jar's configuration says whether its records carry
                    // checksums.
                    let checksums = NippyJar::<SegmentHeader>::load(&data_path)
                        .map_err(ProviderError::other)?
                        .has_checksums();
                    let reader = ChangesetOffsetReader::new(&csoff_path, len, checksums)
                        .map_err(ProviderError::other)?;
                    let offsets = reader.get_range(0, len).map_err(ProviderError::other)?;
                    for offset in offsets {
                        count += offset.num_changes() as usize;
                    }
                }
            }
        }

        Ok(count)
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Creates an iterator for walking through account changesets in the specified block range.
    ///
    /// This returns a lazy iterator that fetches changesets block by block to avoid loading
    /// everything into memory at once.
    ///
    /// Accepts any range type that implements `RangeBounds<BlockNumber>`, including:
    /// - `Range<BlockNumber>` (e.g., `0..100`)
    /// - `RangeInclusive<BlockNumber>` (e.g., `0..=99`)
    /// - `RangeFrom<BlockNumber>` (e.g., `0..`) - iterates until exhausted
    pub fn walk_account_changeset_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> StaticFileAccountChangesetWalker<Self> {
        StaticFileAccountChangesetWalker::new(self.clone(), range)
    }

    /// Creates an iterator for walking through storage changesets in the specified block range.
    pub fn walk_storage_changeset_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> StaticFileStorageChangesetWalker<Self> {
        StaticFileStorageChangesetWalker::new(self.clone(), range)
    }
}

impl<N: NodePrimitives<BlockHeader: Value>> HeaderProvider for StaticFileProvider<N> {
    type Header = N::BlockHeader;

    fn header(&self, block_hash: BlockHash) -> ProviderResult<Option<Self::Header>> {
        self.find_static_file(StaticFileSegment::Headers, |jar_provider| {
            Ok(jar_provider
                .cursor()?
                .get_two::<HeaderWithHashMask<Self::Header>>((&block_hash).into())?
                .and_then(|(header, hash)| {
                    if hash == block_hash {
                        return Some(header);
                    }
                    None
                }))
        })
    }

    fn header_by_number(&self, num: BlockNumber) -> ProviderResult<Option<Self::Header>> {
        self.get_segment_provider_for_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.header_by_number(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Self::Header>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            to_range(range),
            |cursor, number| cursor.get_one::<HeaderMask<Self::Header>>(number.into()),
            |_| true,
        )
    }

    fn sealed_header(
        &self,
        num: BlockNumber,
    ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        self.get_segment_provider_for_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.sealed_header(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<BlockNumber>,
        predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            to_range(range),
            |cursor, number| {
                Ok(cursor
                    .get_two::<HeaderWithHashMask<Self::Header>>(number.into())?
                    .map(|(header, hash)| SealedHeader::new(header, hash)))
            },
            predicate,
        )
    }
}

impl<N: NodePrimitives> BlockHashReader for StaticFileProvider<N> {
    fn block_hash(&self, num: u64) -> ProviderResult<Option<B256>> {
        self.get_segment_provider_for_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.block_hash(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            start..end,
            |cursor, number| cursor.get_one::<BlockHashMask>(number.into()),
            |_| true,
        )
    }
}

impl<N: NodePrimitives<SignedTx: Value + SignedTransaction, Receipt: Value>> ReceiptProvider
    for StaticFileProvider<N>
{
    type Receipt = N::Receipt;

    fn receipt(&self, num: TxNumber) -> ProviderResult<Option<Self::Receipt>> {
        self.get_segment_provider_for_transaction(StaticFileSegment::Receipts, num, None)
            .and_then(|provider| provider.receipt(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn receipt_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Receipt>> {
        if let Some(num) = self.transaction_id(hash)? {
            return self.receipt(num);
        }
        Ok(None)
    }

    fn receipts_by_block(
        &self,
        _block: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Receipt>>> {
        unreachable!()
    }

    fn receipts_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Receipt>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Receipts,
            to_range(range),
            |cursor, number| cursor.get_one::<ReceiptMask<Self::Receipt>>(number.into()),
            |_| true,
        )
    }

    fn receipts_by_block_range(
        &self,
        _block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Receipt>>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives<SignedTx: Value, Receipt: Value, BlockHeader: Value>> TransactionsProviderExt
    for StaticFileProvider<N>
{
    fn transaction_hashes_by_range(
        &self,
        tx_range: Range<TxNumber>,
    ) -> ProviderResult<Vec<(TxHash, TxNumber)>> {
        let tx_range_size = (tx_range.end - tx_range.start) as usize;

        // Transactions are different size, so chunks will not all take the same processing time. If
        // chunks are too big, there will be idle threads waiting for work. Choosing an
        // arbitrary smaller value to make sure it doesn't happen.
        let chunk_size = 100;

        // iterator over the chunks
        let chunks = tx_range
            .clone()
            .step_by(chunk_size)
            .map(|start| start..std::cmp::min(start + chunk_size as u64, tx_range.end));
        let mut channels = Vec::with_capacity(tx_range_size.div_ceil(chunk_size));

        for chunk_range in chunks {
            let (channel_tx, channel_rx) = mpsc::channel();
            channels.push(channel_rx);

            let manager = self.clone();

            // Spawn the task onto the global rayon pool
            // This task will send the results through the channel after it has calculated
            // the hash.
            rayon::spawn(move || {
                let mut rlp_buf = Vec::with_capacity(128);
                let _ = manager.fetch_range_with_predicate(
                    StaticFileSegment::Transactions,
                    chunk_range,
                    |cursor, number| {
                        Ok(cursor
                            .get_one::<TransactionMask<Self::Transaction>>(number.into())?
                            .map(|transaction| {
                                rlp_buf.clear();
                                let _ = channel_tx
                                    .send(calculate_hash((number, transaction), &mut rlp_buf));
                            }))
                    },
                    |_| true,
                );
            });
        }

        let mut tx_list = Vec::with_capacity(tx_range_size);

        // Iterate over channels and append the tx hashes unsorted
        for channel in channels {
            while let Ok(tx) = channel.recv() {
                let (tx_hash, tx_id) = tx.map_err(|boxed| *boxed)?;
                tx_list.push((tx_hash, tx_id));
            }
        }

        Ok(tx_list)
    }
}

impl<N: NodePrimitives<SignedTx: Decompress + SignedTransaction>> TransactionsProvider
    for StaticFileProvider<N>
{
    type Transaction = N::SignedTx;

    fn transaction_id(&self, tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
        self.find_static_file(StaticFileSegment::Transactions, |jar_provider| {
            let mut cursor = jar_provider.cursor()?;
            if cursor
                .get_one::<TransactionMask<Self::Transaction>>((&tx_hash).into())?
                .and_then(|tx| (tx.trie_hash() == tx_hash).then_some(tx))
                .is_some()
            {
                Ok(cursor.number())
            } else {
                Ok(None)
            }
        })
    }

    fn transaction_by_id(&self, num: TxNumber) -> ProviderResult<Option<Self::Transaction>> {
        self.get_segment_provider_for_transaction(StaticFileSegment::Transactions, num, None)
            .and_then(|provider| provider.transaction_by_id(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn transaction_by_id_unhashed(
        &self,
        num: TxNumber,
    ) -> ProviderResult<Option<Self::Transaction>> {
        self.get_segment_provider_for_transaction(StaticFileSegment::Transactions, num, None)
            .and_then(|provider| provider.transaction_by_id_unhashed(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn transaction_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Transaction>> {
        self.find_static_file(StaticFileSegment::Transactions, |jar_provider| {
            Ok(jar_provider
                .cursor()?
                .get_one::<TransactionMask<Self::Transaction>>((&hash).into())?
                .and_then(|tx| (tx.trie_hash() == hash).then_some(tx)))
        })
    }

    fn transaction_by_hash_with_meta(
        &self,
        _hash: TxHash,
    ) -> ProviderResult<Option<(Self::Transaction, TransactionMeta)>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_block(
        &self,
        _block_id: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Transaction>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_block_range(
        &self,
        _range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Transaction>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Transaction>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Transactions,
            to_range(range),
            |cursor, number| cursor.get_one::<TransactionMask<Self::Transaction>>(number.into()),
            |_| true,
        )
    }

    fn senders_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Address>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::TransactionSenders,
            to_range(range),
            |cursor, number| cursor.get_one::<TransactionSenderMask>(number.into()),
            |_| true,
        )
    }

    fn transaction_sender(&self, id: TxNumber) -> ProviderResult<Option<Address>> {
        self.get_segment_provider_for_transaction(StaticFileSegment::TransactionSenders, id, None)
            .and_then(|provider| provider.transaction_sender(id))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }
}

impl<N: NodePrimitives> BlockNumReader for StaticFileProvider<N> {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn best_block_number(&self) -> ProviderResult<BlockNumber> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn last_block_number(&self) -> ProviderResult<BlockNumber> {
        Ok(self.get_highest_static_file_block(StaticFileSegment::Headers).unwrap_or_default())
    }

    fn block_number(&self, _hash: B256) -> ProviderResult<Option<BlockNumber>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }
}

/* Cannot be successfully implemented but must exist for trait requirements */

impl<N: NodePrimitives<SignedTx: Value, Receipt: Value, BlockHeader: Value>> BlockReader
    for StaticFileProvider<N>
{
    type Block = N::Block;

    fn find_block_by_hash(
        &self,
        _hash: B256,
        _source: BlockSource,
    ) -> ProviderResult<Option<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block(&self, _id: BlockHashOrNumber) -> ProviderResult<Option<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn pending_block(&self) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn pending_block_and_receipts(
        &self,
    ) -> ProviderResult<Option<(RecoveredBlock<Self::Block>, Vec<Self::Receipt>)>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn recovered_block(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn sealed_block_with_senders(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_range(&self, _range: RangeInclusive<BlockNumber>) -> ProviderResult<Vec<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_with_senders_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn recovered_block_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_by_transaction_id(&self, _id: TxNumber) -> ProviderResult<Option<BlockNumber>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives> BlockBodyIndicesProvider for StaticFileProvider<N> {
    fn block_body_indices(&self, _num: u64) -> ProviderResult<Option<StoredBlockBodyIndices>> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_body_indices_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<StoredBlockBodyIndices>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives> StatsReader for StaticFileProvider<N> {
    fn count_entries<T: Table>(&self) -> ProviderResult<usize> {
        match T::NAME {
            tables::CanonicalHeaders::NAME |
            tables::Headers::<Header>::NAME |
            tables::HeaderTerminalDifficulties::NAME => Ok(self
                .get_highest_static_file_block(StaticFileSegment::Headers)
                .map(|block| block + 1)
                .unwrap_or_default()
                as usize),
            tables::Receipts::<Receipt>::NAME => Ok(self
                .get_highest_static_file_tx(StaticFileSegment::Receipts)
                .map(|receipts| receipts + 1)
                .unwrap_or_default() as usize),
            tables::Transactions::<TransactionSigned>::NAME => Ok(self
                .get_highest_static_file_tx(StaticFileSegment::Transactions)
                .map(|txs| txs + 1)
                .unwrap_or_default()
                as usize),
            tables::TransactionSenders::NAME => Ok(self
                .get_highest_static_file_tx(StaticFileSegment::TransactionSenders)
                .map(|txs| txs + 1)
                .unwrap_or_default() as usize),
            _ => Err(ProviderError::UnsupportedProvider),
        }
    }
}

/// Calculates the tx hash for the given transaction and its id.
#[inline]
fn calculate_hash<T>(
    entry: (TxNumber, T),
    rlp_buf: &mut Vec<u8>,
) -> Result<(B256, TxNumber), Box<ProviderError>>
where
    T: Encodable2718,
{
    let (tx_id, tx) = entry;
    tx.encode_2718(rlp_buf);
    Ok((keccak256(rlp_buf), tx_id))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{atomic::Ordering, mpsc},
        thread,
        time::{Duration, Instant},
    };

    use alloy_consensus::Header;
    use alloy_primitives::B256;
    use reth_chain_state::EthPrimitives;
    use reth_db::test_utils::create_test_static_files_dir;
    use reth_static_file_types::{SegmentRangeInclusive, StaticFileSegment};

    use super::StaticFileWriter;
    use crate::{providers::StaticFileProvider, BlockHashReader, StaticFileProviderBuilder};

    #[test]
    fn recovery_prunes_transaction_zero_at_empty_genesis() -> eyre::Result<()> {
        use crate::{
            test_utils::create_test_provider_factory, StaticFileProviderFactory,
            TransactionsProvider,
        };
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_primitives::Signature;
        use reth_db::{tables, transaction::DbTxMut};

        let factory = create_test_provider_factory();
        let provider = factory.provider_rw()?;
        provider.tx_ref().put::<tables::BlockBodyIndices>(0, Default::default())?;
        provider.commit()?;

        let static_files = factory.static_file_provider();
        let segment = StaticFileSegment::Transactions;
        let tx = TxLegacy::default().into_signed(Signature::test_signature()).into();
        {
            let mut writer = static_files.latest_writer(segment)?;
            writer.increment_block(0)?;
            writer.increment_block(1)?;
            writer.append_transaction(0, &tx)?;
            writer.commit()?;
        }
        assert!(static_files.transaction_by_id(0)?.is_some());

        assert_eq!(static_files.check_consistency(&factory.provider()?)?, None);
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(0));
        assert!(static_files.transaction_by_id(0)?.is_none());

        let mut writer = static_files.latest_writer(segment)?;
        writer.increment_block(1)?;
        writer.append_transaction(0, &tx)?;
        writer.commit()?;
        Ok(())
    }

    #[test]
    fn stale_cache_fill_does_not_survive_index_reinitialization() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir)
                .with_blocks_per_file_for_segment(StaticFileSegment::Headers, 10)
                .with_blocks_per_file_for_segment(StaticFileSegment::Receipts, 2)
                .build()?;

        let hash_0 = B256::from([0x10; 32]);
        let header_0 = Header { number: 0, ..Default::default() };
        {
            let mut writer = static_files.latest_writer(StaticFileSegment::Headers)?;
            writer.append_header(&header_0, &hash_0)?;
            writer.commit()?;
        }

        // Create two receipt jars. Deleting either one reinitializes every segment index and
        // invalidates the shared jar cache, matching the pruning path from the incidents.
        {
            let mut writer = static_files.latest_writer(StaticFileSegment::Receipts)?;
            for block in 0..=3 {
                writer.increment_block(block)?;
            }
            writer.commit()?;
        }
        static_files.delete_jar(StaticFileSegment::Receipts, 0)?;

        let (loaded_tx, loaded_rx) = mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = mpsc::sync_channel(0);
        static_files.before_cache_insert.set(move |segment| {
            assert_eq!(segment, StaticFileSegment::Headers);
            loaded_tx.send(()).expect("test controller dropped");
            resume_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("test controller did not resume");
        });

        // Pause a cache fill after it loads the header jar containing only block 0.
        let reader_static_files = static_files.clone();
        let reader = thread::spawn(move || reader_static_files.block_hash(0));
        loaded_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("reader did not reach cache-fill hook");

        // Publish a newer snapshot of the same header jar, then simulate unrelated segment
        // pruning invalidating the whole cache while the old load remains in flight.
        let hash_1 = B256::from([0x11; 32]);
        let header_1 = Header { number: 1, ..Default::default() };
        {
            let mut writer = static_files.latest_writer(StaticFileSegment::Headers)?;
            writer.append_header(&header_1, &hash_1)?;
            writer.commit()?;
        }
        static_files.delete_jar(StaticFileSegment::Receipts, 2)?;

        resume_tx.send(()).expect("reader dropped before cache publication");
        assert_eq!(reader.join().expect("reader panicked")?, Some(hash_0));
        assert_eq!(
            static_files.block_hash(1)?,
            Some(hash_1),
            "cache fill started before reinitialization repopulated an invalidated jar"
        );

        Ok(())
    }

    #[test]
    fn cache_fill_validation_is_serialized_with_index_reinitialization() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).with_blocks_per_file(10).build()?;
        let hash = B256::from([0x10; 32]);
        {
            let mut writer = static_files.latest_writer(StaticFileSegment::Headers)?;
            writer.append_header(&Header::default(), &hash)?;
            writer.commit()?;
        }
        static_files.remove_cached_provider(StaticFileSegment::Headers, 9);

        let generation = static_files.cache_generation.load(Ordering::Acquire);
        let (validated_tx, validated_rx) = mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = mpsc::sync_channel(0);
        static_files.after_cache_validation.set(move |segment| {
            assert_eq!(segment, StaticFileSegment::Headers);
            validated_tx.send(()).expect("test controller dropped");
            resume_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("test controller did not resume");
        });

        let reader_static_files = static_files.clone();
        let reader = thread::spawn(move || reader_static_files.block_hash(0));
        validated_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("reader did not reach cache validation");

        // The shard must remain locked between checking the generation and inserting the jar.
        // Otherwise a clear can finish here and the reader can repopulate the cache afterward.
        assert!(static_files.map.try_get(&(9, StaticFileSegment::Headers)).is_locked());
        let invalidator_static_files = static_files.clone();
        let invalidator = thread::spawn(move || invalidator_static_files.initialize_index());
        let deadline = Instant::now() + Duration::from_secs(10);
        while static_files.cache_generation.load(Ordering::Acquire) == generation {
            assert!(Instant::now() < deadline, "reinitialization did not invalidate the cache");
            thread::sleep(Duration::from_millis(1));
        }

        // Invalidation has started, but clearing this shard must wait for publication and the
        // reader's guard to be released. The published snapshot must not survive that clear.
        resume_tx.send(()).expect("reader dropped before cache publication");
        assert_eq!(reader.join().expect("reader panicked")?, Some(hash));
        invalidator.join().expect("invalidator panicked")?;
        assert!(static_files.map.is_empty());
        assert_eq!(static_files.block_hash(0)?, Some(hash));

        Ok(())
    }

    #[test]
    fn test_find_fixed_range_with_block_index() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let sf_rw: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).with_blocks_per_file(100).build()?;

        let segment = StaticFileSegment::Headers;

        // Test with None - should use default behavior
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, None, 0),
            SegmentRangeInclusive::new(0, 99)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, None, 250),
            SegmentRangeInclusive::new(200, 299)
        );

        // Test with empty index - should fall back to default behavior
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&BTreeMap::new()), 150),
            SegmentRangeInclusive::new(100, 199)
        );

        // Create block index with existing ranges
        let block_index = BTreeMap::from_iter([
            (99, SegmentRangeInclusive::new(0, 99)),
            (199, SegmentRangeInclusive::new(100, 199)),
            (299, SegmentRangeInclusive::new(200, 299)),
        ]);

        // Test blocks within existing ranges - should return the matching range
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 0),
            SegmentRangeInclusive::new(0, 99)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 50),
            SegmentRangeInclusive::new(0, 99)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 99),
            SegmentRangeInclusive::new(0, 99)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 100),
            SegmentRangeInclusive::new(100, 199)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 150),
            SegmentRangeInclusive::new(100, 199)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 199),
            SegmentRangeInclusive::new(100, 199)
        );

        // Test blocks beyond existing ranges - should derive new ranges from the last range
        // Block 300 is exactly one segment after the last range
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 300),
            SegmentRangeInclusive::new(300, 399)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 350),
            SegmentRangeInclusive::new(300, 399)
        );

        // Block 500 skips one segment (300-399)
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 500),
            SegmentRangeInclusive::new(500, 599)
        );

        // Block 1000 skips many segments
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&block_index), 1000),
            SegmentRangeInclusive::new(1000, 1099)
        );

        // Test with block index having different sizes than blocks_per_file setting
        // This simulates the scenario where blocks_per_file was changed between runs
        let mixed_size_index = BTreeMap::from_iter([
            (49, SegmentRangeInclusive::new(0, 49)),     // 50 blocks
            (149, SegmentRangeInclusive::new(50, 149)),  // 100 blocks
            (349, SegmentRangeInclusive::new(150, 349)), // 200 blocks
        ]);

        // Blocks within existing ranges should return those ranges regardless of size
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 25),
            SegmentRangeInclusive::new(0, 49)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 100),
            SegmentRangeInclusive::new(50, 149)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 200),
            SegmentRangeInclusive::new(150, 349)
        );

        // Block after the last range should derive using current blocks_per_file (100)
        // from the end of the last range (349)
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 350),
            SegmentRangeInclusive::new(350, 449)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 450),
            SegmentRangeInclusive::new(450, 549)
        );
        assert_eq!(
            sf_rw.find_fixed_range_with_block_index(segment, Some(&mixed_size_index), 550),
            SegmentRangeInclusive::new(550, 649)
        );

        Ok(())
    }

    /// TN(TN-0051): `sync_and_freeze` writes the configuration in the segment's thread, before
    /// `finalize` updates the index on the commit thread, so for the rest of `save_blocks` a jar
    /// loaded on a cache miss holds rows the index does not name yet. Readers route by the index,
    /// so reads by number still find nothing past the old tip, and `finalize` replaces the cached
    /// jar when it publishes the rows. (A range read that runs past the index tip inside such a
    /// jar can return the new rows early; the index is ahead of the database for the whole
    /// database commit anyway, so callers already see rows the database has not committed.)
    #[test]
    fn frozen_rows_stay_unindexed_until_finalize() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).with_blocks_per_file(10).build()?;
        let segment = StaticFileSegment::Headers;
        let hashes = [0x10, 0x11, 0x12].map(|byte| B256::from([byte; 32]));
        {
            let mut writer = static_files.latest_writer(segment)?;
            writer.append_header(&Header::default(), &hashes[0])?;
            writer.commit()?;
        }

        let mut writer = static_files.latest_writer(segment)?;
        for number in 1..=2u64 {
            let header = Header { number, ..Default::default() };
            writer.append_header(&header, &hashes[number as usize])?;
        }
        writer.sync_and_freeze()?;

        // A cache miss now loads the configuration with all three rows.
        static_files.remove_cached_provider(segment, 9);
        assert_eq!(static_files.block_hash(0)?, Some(hashes[0]));
        assert_eq!(static_files.map.get(&(9, segment)).expect("jar cached").jar.rows(), 3);

        // The index still ends at block 0, so the frozen rows are not readable by number.
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(0));
        assert_eq!(static_files.block_hash(1)?, None);
        assert_eq!(static_files.block_hash(2)?, None);

        writer.finalize()?;
        drop(writer);
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(2));
        assert_eq!(static_files.block_hash(2)?, Some(hashes[2]));
        assert_eq!(static_files.map.get(&(9, segment)).expect("jar cached").jar.rows(), 3);
        Ok(())
    }

    /// TN(TN-0101): a commit that grows the headers while a reader holds a jar, the headers'
    /// own or another segment's, waits for the reader without the index lock, so lookups go on,
    /// and the new header is readable once the commit returns.
    #[test]
    fn growing_commit_waits_for_jar_readers_outside_the_index_lock() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).build()?;
        let hashes = [0x20, 0x21, 0x22].map(|byte| B256::from([byte; 32]));
        {
            let mut headers = static_files.latest_writer(StaticFileSegment::Headers)?;
            headers.append_header(&Header::default(), &hashes[0])?;
            headers.commit()?;
            let mut changesets =
                static_files.latest_writer(StaticFileSegment::AccountChangeSets)?;
            changesets.append_account_changeset(Vec::new(), 0)?;
            changesets.commit()?;
        }

        // The headers' own jar (the cache insert waits for it), then another segment's (the
        // clean-up of jars above the new range must not wait for it).
        for (number, held) in
            [(1u64, StaticFileSegment::Headers), (2, StaticFileSegment::AccountChangeSets)]
        {
            // A reader holds the jar, as a range walk does.
            let held = static_files.get_segment_provider_for_block(held, 0, None)?;
            thread::scope(|scope| -> eyre::Result<()> {
                let commit = scope.spawn(|| -> eyre::Result<()> {
                    let mut headers = static_files.latest_writer(StaticFileSegment::Headers)?;
                    let header = Header { number, ..Default::default() };
                    headers.append_header(&header, &hashes[number as usize])?;
                    Ok(headers.commit()?)
                });
                // Give the commit time to reach the jar cache.
                thread::sleep(Duration::from_millis(200));
                let (tx, rx) = mpsc::channel();
                let lookups = &static_files;
                scope.spawn(move || {
                    let lookup = lookups
                        .get_segment_provider_for_block(
                            StaticFileSegment::AccountChangeSets,
                            0,
                            None,
                        )
                        .map(drop);
                    let _ = tx.send(lookup);
                });
                let lookup = rx.recv_timeout(Duration::from_secs(2));
                drop(held);
                assert!(
                    matches!(lookup, Ok(Ok(()))),
                    "block {number}: a lookup waited for the held jar: {lookup:?}"
                );
                commit.join().expect("commit thread")
            })?;
            let highest = static_files.get_highest_static_file_block(StaticFileSegment::Headers);
            assert_eq!(highest, Some(number));
            assert_eq!(static_files.block_hash(number)?, Some(hashes[number as usize]));
        }
        Ok(())
    }

    /// TN(TN-0052): a persist records each sync phase once for its segment, `.csoff` only for
    /// changeset segments, and a take drains what was recorded.
    #[test]
    fn sync_stats_record_each_phase_per_segment() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).build()?;
        let mut headers = static_files.latest_writer(StaticFileSegment::Headers)?;
        let mut changesets = static_files.latest_writer(StaticFileSegment::AccountChangeSets)?;
        // Opening a writer runs its heal, which updates the index; start after it.
        let _ = static_files.take_sync_stats();

        headers.append_header(&Header::default(), &B256::ZERO)?;
        headers.sync_and_freeze()?;
        headers.finalize()?;
        changesets.append_account_changeset(Vec::new(), 0)?;
        changesets.sync_and_freeze()?;
        changesets.finalize()?;

        let stats = static_files.take_sync_stats();
        assert_eq!(
            stats.iter().map(|(segment, _)| segment).collect::<Vec<_>>(),
            [StaticFileSegment::Headers, StaticFileSegment::AccountChangeSets]
        );
        for segment in [StaticFileSegment::Headers, StaticFileSegment::AccountChangeSets] {
            let s = stats.segment(segment);
            for (phase, phase_stats) in [
                ("data", s.data_sync),
                ("offsets", s.offsets_sync),
                ("conf", s.conf_freeze),
                ("update_index", s.update_index),
            ] {
                assert_eq!(phase_stats.calls, 1, "{segment:?} {phase}");
                assert!(phase_stats.max <= phase_stats.total, "{segment:?} {phase}");
            }
        }
        assert_eq!(stats.segment(StaticFileSegment::Headers).csoff_sync.calls, 0);
        assert_eq!(stats.segment(StaticFileSegment::AccountChangeSets).csoff_sync.calls, 1);
        assert!(static_files.take_sync_stats().is_empty(), "a take drains the stats");
        Ok(())
    }

    /// TN(TN-0051): after `sync_and_freeze` the writer is clean, so a commit of every writer
    /// (`StaticFileWriters::commit`, which `DatabaseProvider::commit` runs instead of `finalize`
    /// in unwind order or with an unwind queued) has nothing to write, but must still publish
    /// the frozen rows.
    #[test]
    fn commit_publishes_rows_frozen_by_sync_and_freeze() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).build()?;
        let segment = StaticFileSegment::Headers;
        let hashes = [0x30, 0x31, 0x32].map(|byte| B256::from([byte; 32]));
        {
            let mut writer = static_files.latest_writer(segment)?;
            writer.append_header(&Header::default(), &hashes[0])?;
            writer.commit()?;
            for number in 1..=2u64 {
                let header = Header { number, ..Default::default() };
                writer.append_header(&header, &hashes[number as usize])?;
            }
            writer.sync_and_freeze()?;
        }
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(0));

        static_files.commit()?;
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(2));
        assert_eq!(static_files.block_hash(2)?, Some(hashes[2]));
        Ok(())
    }

    /// TN(TN-0051): `sync_and_freeze` leaves the writer unsynced, so `finalize` syncs the rows
    /// appended after it before it writes their configuration.
    #[test]
    fn finalize_syncs_rows_appended_after_sync_and_freeze() -> eyre::Result<()> {
        let (static_dir, _) = create_test_static_files_dir();
        let static_files: StaticFileProvider<EthPrimitives> =
            StaticFileProviderBuilder::read_write(&static_dir).build()?;
        let segment = StaticFileSegment::Headers;
        let mut writer = static_files.latest_writer(segment)?;
        // Opening a writer runs its heal, which updates the index; start after it.
        let _ = static_files.take_sync_stats();

        writer.append_header(&Header::default(), &B256::ZERO)?;
        writer.sync_and_freeze()?;
        writer.append_header(&Header { number: 1, ..Default::default() }, &B256::ZERO)?;
        writer.finalize()?;
        drop(writer);

        let stats = static_files.take_sync_stats();
        let stats = stats.segment(segment);
        assert_eq!(stats.data_sync.calls, 2, "the second row's data sync");
        assert_eq!(stats.offsets_sync.calls, 2, "the second row's offsets sync");
        assert_eq!(stats.conf_freeze.calls, 2);
        assert_eq!(static_files.get_highest_static_file_block(segment), Some(1));
        Ok(())
    }
}
