// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod database;
pub mod errors;
pub(crate) mod iter;
pub mod metrics;
pub mod util;

use metrics::SamplingInterval;
use rocksdb::{
    checkpoint::Checkpoint, AsColumnFamilyRef, BlockBasedOptions, BottommostLevelCompaction,
    CStrLike, Cache, ColumnFamilyDescriptor, CompactOptions, DBPinnableSlice, DBWithThreadMode,
    Error, IteratorMode, LiveFile, MultiThreaded, OptimisticTransactionOptions, ReadOptions,
    Transaction, WriteOptions,
};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    ffi::CStr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{info, instrument, warn};

pub use errors::TypedStoreError;
use telcoin_macros::{fail_point, nondeterministic};

// Write buffer size per RocksDB instance can be set via the env var below.
// If the env var is not set, use the default value in MiB.
const ENV_VAR_DB_WRITE_BUFFER_SIZE: &str = "DB_WRITE_BUFFER_SIZE_MB";
const DEFAULT_DB_WRITE_BUFFER_SIZE: usize = 1024;

// Write ahead log size per RocksDB instance can be set via the env var below.
// If the env var is not set, use the default value in MiB.
const ENV_VAR_DB_WAL_SIZE: &str = "DB_WAL_SIZE_MB";
const DEFAULT_DB_WAL_SIZE: usize = 1024;

// Environment variable to control behavior of write throughput optimized tables.
const ENV_VAR_L0_NUM_FILES_COMPACTION_TRIGGER: &str = "L0_NUM_FILES_COMPACTION_TRIGGER";
const DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER: usize = 6;
const ENV_VAR_MAX_WRITE_BUFFER_SIZE_MB: &str = "MAX_WRITE_BUFFER_SIZE_MB";
const DEFAULT_MAX_WRITE_BUFFER_SIZE_MB: usize = 256;
const ENV_VAR_MAX_WRITE_BUFFER_NUMBER: &str = "MAX_WRITE_BUFFER_NUMBER";
const DEFAULT_MAX_WRITE_BUFFER_NUMBER: usize = 6;
const ENV_VAR_TARGET_FILE_SIZE_BASE_MB: &str = "TARGET_FILE_SIZE_BASE_MB";
const DEFAULT_TARGET_FILE_SIZE_BASE_MB: usize = 128;

// Set to 1 to disable blob storage for transactions and effects.
const ENV_VAR_DISABLE_BLOB_STORAGE: &str = "DISABLE_BLOB_STORAGE";

const ENV_VAR_MAX_BACKGROUND_JOBS: &str = "MAX_BACKGROUND_JOBS";

// TODO: remove this after Rust rocksdb has the TOTAL_BLOB_FILES_SIZE property built-in.
// From https://github.com/facebook/rocksdb/blob/bd80433c73691031ba7baa65c16c63a83aef201a/include/rocksdb/db.h#L1169
const ROCKSDB_PROPERTY_TOTAL_BLOB_FILES_SIZE: &CStr =
    unsafe { CStr::from_bytes_with_nul_unchecked("rocksdb.total-blob-file-size\0".as_bytes()) };

#[cfg(test)]
mod tests;

/// Repeatedly attempt an Optimistic Transaction until it succeeds.
///
/// Since many callsites (e.g. the consensus handler) cannot proceed in the case of failed writes,
/// this will loop forever until the transaction succeeds.
#[macro_export]
macro_rules! retry_transaction {
    ($transaction:expr) => {
        retry_transaction!($transaction, Some(20))
    };

    (
        $transaction:expr,
        $max_retries:expr // should be an Option<int type>, None for unlimited
        $(,)?

    ) => {{
        use rand::{
            distributions::{Distribution, Uniform},
            rngs::ThreadRng,
        };
        use tokio::time::{sleep, Duration};
        use tracing::{error, info};

        let mut retries = 0;
        let max_retries = $max_retries;
        loop {
            let status = $transaction;
            match status {
                Err(TypedStoreError::RetryableTransactionError) => {
                    retries += 1;
                    // Randomized delay to help racing transactions get out of each other's way.
                    let delay = {
                        let mut rng = ThreadRng::default();
                        Duration::from_millis(Uniform::new(0, 50).sample(&mut rng))
                    };
                    if let Some(max_retries) = max_retries {
                        if retries > max_retries {
                            error!(?max_retries, "max retries exceeded");
                            break status;
                        }
                    }
                    if retries > 10 {
                        // TODO: monitoring needed?
                        error!(?delay, ?retries, "excessive transaction retries...");
                    } else {
                        info!(?delay, ?retries, "transaction write conflict detected, sleeping");
                    }
                    sleep(delay).await;
                }
                _ => break status,
            }
        }
    }};
}

#[macro_export]
macro_rules! retry_transaction_forever {
    ($transaction:expr) => {
        $crate::retry_transaction!($transaction, None)
    };
}

#[derive(Debug)]
pub struct DBWithThreadModeWrapper {
    pub underlying: rocksdb::DBWithThreadMode<MultiThreaded>,
    pub metric_conf: MetricConf,
    pub db_path: PathBuf,
}

#[derive(Debug)]
pub struct OptimisticTransactionDBWrapper {
    pub underlying: rocksdb::OptimisticTransactionDB<MultiThreaded>,
    pub metric_conf: MetricConf,
    pub db_path: PathBuf,
}

/// Thin wrapper to unify interface across different db types
#[derive(Debug)]
pub enum RocksDB {
    DBWithThreadMode(DBWithThreadModeWrapper),
    OptimisticTransactionDB(OptimisticTransactionDBWrapper),
}

macro_rules! delegate_call {
    ($self:ident.$method:ident($($args:ident),*)) => {
        match $self {
            Self::DBWithThreadMode(d) => d.underlying.$method($($args),*),
            Self::OptimisticTransactionDB(d) => d.underlying.$method($($args),*),
        }
    }
}

impl Drop for RocksDB {
    fn drop(&mut self) {
        delegate_call!(self.cancel_all_background_work(/* wait */ true))
    }
}

impl RocksDB {
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, rocksdb::Error> {
        delegate_call!(self.get(key))
    }

    pub fn multi_get_cf<'a, 'b: 'a, K, I, W>(
        &'a self,
        keys: I,
        readopts: &ReadOptions,
    ) -> Vec<Result<Option<Vec<u8>>, rocksdb::Error>>
    where
        K: AsRef<[u8]>,
        I: IntoIterator<Item = (&'b W, K)>,
        W: 'b + AsColumnFamilyRef,
    {
        delegate_call!(self.multi_get_cf_opt(keys, readopts))
    }

    pub fn batched_multi_get_cf_opt<'a, I, K>(
        &self,
        cf: &impl AsColumnFamilyRef,
        keys: I,
        sorted_input: bool,
        readopts: &ReadOptions,
    ) -> Vec<Result<Option<DBPinnableSlice<'_>>, Error>>
    where
        K: AsRef<[u8]> + 'a,
        I: IntoIterator<Item = &'a K>,
    {
        delegate_call!(self.batched_multi_get_cf_opt(cf, keys, sorted_input, readopts))
    }

    pub fn property_int_value_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        name: impl CStrLike,
    ) -> Result<Option<u64>, rocksdb::Error> {
        delegate_call!(self.property_int_value_cf(cf, name))
    }

    pub fn get_pinned_cf_opt<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'_>>, rocksdb::Error> {
        delegate_call!(self.get_pinned_cf_opt(cf, key, readopts))
    }

    pub fn cf_handle(&self, name: &str) -> Option<Arc<rocksdb::BoundColumnFamily<'_>>> {
        delegate_call!(self.cf_handle(name))
    }

    pub fn create_cf<N: AsRef<str>>(
        &self,
        name: N,
        opts: &rocksdb::Options,
    ) -> Result<(), rocksdb::Error> {
        delegate_call!(self.create_cf(name, opts))
    }

    pub fn drop_cf(&self, name: &str) -> Result<(), rocksdb::Error> {
        delegate_call!(self.drop_cf(name))
    }

    pub fn delete_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        writeopts: &WriteOptions,
    ) -> Result<(), rocksdb::Error> {
        fail_point!("delete-cf-before");
        let ret = delegate_call!(self.delete_cf_opt(cf, key, writeopts));
        fail_point!("delete-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    pub fn path(&self) -> &Path {
        delegate_call!(self.path())
    }

    pub fn put_cf<K, V>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        value: V,
        writeopts: &WriteOptions,
    ) -> Result<(), rocksdb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        fail_point!("put-cf-before");
        let ret = delegate_call!(self.put_cf_opt(cf, key, value, writeopts));
        fail_point!("put-cf-after");
        #[allow(clippy::let_and_return)]
        ret
    }

    pub fn key_may_exist_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> bool {
        delegate_call!(self.key_may_exist_cf_opt(cf, key, readopts))
    }

    pub fn try_catch_up_with_primary(&self) -> Result<(), rocksdb::Error> {
        delegate_call!(self.try_catch_up_with_primary())
    }

    pub fn transaction_without_snapshot(
        &self,
    ) -> Result<Transaction<'_, rocksdb::OptimisticTransactionDB>, TypedStoreError> {
        match self {
            Self::OptimisticTransactionDB(db) => Ok(db.underlying.transaction()),
            Self::DBWithThreadMode(_) => panic!(),
        }
    }

    pub fn transaction(
        &self,
    ) -> Result<Transaction<'_, rocksdb::OptimisticTransactionDB>, TypedStoreError> {
        match self {
            Self::OptimisticTransactionDB(db) => {
                let mut tx_opts = OptimisticTransactionOptions::new();
                tx_opts.set_snapshot(true);

                Ok(db.underlying.transaction_opt(&WriteOptions::default(), &tx_opts))
            }
            Self::DBWithThreadMode(_) => panic!(),
        }
    }

    pub fn raw_iterator_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl AsColumnFamilyRef,
        readopts: ReadOptions,
    ) -> RocksDBRawIter<'b> {
        match self {
            Self::DBWithThreadMode(db) => {
                RocksDBRawIter::DB(db.underlying.raw_iterator_cf_opt(cf_handle, readopts))
            }
            Self::OptimisticTransactionDB(db) => RocksDBRawIter::OptimisticTransactionDB(
                db.underlying.raw_iterator_cf_opt(cf_handle, readopts),
            ),
        }
    }

    pub fn iterator_cf<'a: 'b, 'b>(
        &'a self,
        cf_handle: &impl AsColumnFamilyRef,
        readopts: ReadOptions,
        mode: IteratorMode<'_>,
    ) -> RocksDBIter<'b> {
        match self {
            Self::DBWithThreadMode(db) => {
                RocksDBIter::DB(db.underlying.iterator_cf_opt(cf_handle, readopts, mode))
            }
            Self::OptimisticTransactionDB(db) => RocksDBIter::OptimisticTransactionDB(
                db.underlying.iterator_cf_opt(cf_handle, readopts, mode),
            ),
        }
    }

    pub fn compact_range_cf<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        delegate_call!(self.compact_range_cf(cf, start, end))
    }

    pub fn compact_range_to_bottom<K: AsRef<[u8]>>(
        &self,
        cf: &impl AsColumnFamilyRef,
        start: Option<K>,
        end: Option<K>,
    ) {
        let opt = &mut CompactOptions::default();
        opt.set_bottommost_level_compaction(BottommostLevelCompaction::ForceOptimized);
        delegate_call!(self.compact_range_cf_opt(cf, start, end, opt))
    }

    pub fn flush(&self) -> Result<(), TypedStoreError> {
        delegate_call!(self.flush()).map_err(|e| TypedStoreError::RocksDBError(e.into_string()))
    }

    pub fn checkpoint(&self, path: &Path) -> Result<(), TypedStoreError> {
        let checkpoint = match self {
            Self::DBWithThreadMode(d) => Checkpoint::new(&d.underlying)?,
            Self::OptimisticTransactionDB(d) => Checkpoint::new(&d.underlying)?,
        };
        checkpoint
            .create_checkpoint(path)
            .map_err(|e| TypedStoreError::RocksDBError(e.to_string()))?;
        Ok(())
    }

    pub fn flush_cf(&self, cf: &impl AsColumnFamilyRef) -> Result<(), rocksdb::Error> {
        delegate_call!(self.flush_cf(cf))
    }

    pub fn set_options_cf(
        &self,
        cf: &impl AsColumnFamilyRef,
        opts: &[(&str, &str)],
    ) -> Result<(), rocksdb::Error> {
        delegate_call!(self.set_options_cf(cf, opts))
    }

    pub fn get_sampling_interval(&self) -> SamplingInterval {
        match self {
            Self::DBWithThreadMode(d) => d.metric_conf.read_sample_interval.new_from_self(),
            Self::OptimisticTransactionDB(d) => d.metric_conf.read_sample_interval.new_from_self(),
        }
    }

    pub fn multiget_sampling_interval(&self) -> SamplingInterval {
        match self {
            Self::DBWithThreadMode(d) => d.metric_conf.read_sample_interval.new_from_self(),
            Self::OptimisticTransactionDB(d) => d.metric_conf.read_sample_interval.new_from_self(),
        }
    }

    pub fn write_sampling_interval(&self) -> SamplingInterval {
        match self {
            Self::DBWithThreadMode(d) => d.metric_conf.write_sample_interval.new_from_self(),
            Self::OptimisticTransactionDB(d) => d.metric_conf.write_sample_interval.new_from_self(),
        }
    }

    pub fn iter_sampling_interval(&self) -> SamplingInterval {
        match self {
            Self::DBWithThreadMode(d) => d.metric_conf.iter_sample_interval.new_from_self(),
            Self::OptimisticTransactionDB(d) => d.metric_conf.iter_sample_interval.new_from_self(),
        }
    }

    pub fn db_name(&self) -> String {
        match self {
            Self::DBWithThreadMode(d) => {
                d.metric_conf.db_name_override.clone().unwrap_or_else(|| self.default_db_name())
            }
            Self::OptimisticTransactionDB(d) => {
                d.metric_conf.db_name_override.clone().unwrap_or_else(|| self.default_db_name())
            }
        }
    }

    pub fn live_files(&self) -> Result<Vec<LiveFile>, Error> {
        delegate_call!(self.live_files())
    }

    fn default_db_name(&self) -> String {
        self.path().file_name().and_then(|f| f.to_str()).unwrap_or("unknown").to_string()
    }
}

#[derive(Debug, Default)]
pub struct MetricConf {
    pub db_name_override: Option<String>,
    pub read_sample_interval: SamplingInterval,
    pub write_sample_interval: SamplingInterval,
    pub iter_sample_interval: SamplingInterval,
}

impl MetricConf {
    pub fn with_db_name(db_name: &str) -> Self {
        Self {
            db_name_override: Some(db_name.to_string()),
            read_sample_interval: SamplingInterval::default(),
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }
    pub fn with_sampling(read_interval: SamplingInterval) -> Self {
        Self {
            db_name_override: None,
            read_sample_interval: read_interval,
            write_sample_interval: SamplingInterval::default(),
            iter_sample_interval: SamplingInterval::default(),
        }
    }
}
const CF_METRICS_REPORT_PERIOD_MILLIS: u64 = 1000;
const METRICS_ERROR: i64 = -1;

macro_rules! delegate_iter_call {
    ($self:ident.$method:ident($($args:ident),*)) => {
        match $self {
            Self::DB(db) => db.$method($($args),*),
            Self::OptimisticTransactionDB(db) => db.$method($($args),*),
            Self::OptimisticTransaction(db) => db.$method($($args),*),
        }
    }
}

pub enum RocksDBRawIter<'a> {
    DB(rocksdb::DBRawIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>),
    OptimisticTransactionDB(
        rocksdb::DBRawIteratorWithThreadMode<'a, rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    ),
    OptimisticTransaction(
        rocksdb::DBRawIteratorWithThreadMode<
            'a,
            Transaction<'a, rocksdb::OptimisticTransactionDB<MultiThreaded>>,
        >,
    ),
}

impl RocksDBRawIter<'_> {
    pub fn valid(&self) -> bool {
        delegate_iter_call!(self.valid())
    }
    pub fn key(&self) -> Option<&[u8]> {
        delegate_iter_call!(self.key())
    }
    pub fn value(&self) -> Option<&[u8]> {
        delegate_iter_call!(self.value())
    }
    pub fn next(&mut self) {
        delegate_iter_call!(self.next())
    }
    pub fn prev(&mut self) {
        delegate_iter_call!(self.prev())
    }
    pub fn seek<K: AsRef<[u8]>>(&mut self, key: K) {
        delegate_iter_call!(self.seek(key))
    }
    pub fn seek_to_last(&mut self) {
        delegate_iter_call!(self.seek_to_last())
    }
    pub fn seek_to_first(&mut self) {
        delegate_iter_call!(self.seek_to_first())
    }
    pub fn seek_for_prev<K: AsRef<[u8]>>(&mut self, key: K) {
        delegate_iter_call!(self.seek_for_prev(key))
    }
    pub fn status(&self) -> Result<(), rocksdb::Error> {
        delegate_iter_call!(self.status())
    }
}

pub enum RocksDBIter<'a> {
    DB(rocksdb::DBIteratorWithThreadMode<'a, DBWithThreadMode<MultiThreaded>>),
    OptimisticTransactionDB(
        rocksdb::DBIteratorWithThreadMode<'a, rocksdb::OptimisticTransactionDB<MultiThreaded>>,
    ),
}

impl Iterator for RocksDBIter<'_> {
    type Item = Result<(Box<[u8]>, Box<[u8]>), Error>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::DB(db) => db.next(),
            Self::OptimisticTransactionDB(db) => db.next(),
        }
    }
}

pub fn read_size_from_env(var_name: &str) -> Option<usize> {
    match env::var(var_name).ok()?.parse::<usize>() {
        Ok(size) => Some(size),
        Err(e) => {
            warn!("Env var {} does not contain valid usize integer: {}", var_name, e);
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReadWriteOptions {
    pub ignore_range_deletions: bool,
    // Whether to sync to disk on every write.
    sync_to_disk: bool,
}

impl ReadWriteOptions {
    pub fn readopts(&self) -> ReadOptions {
        let mut readopts = ReadOptions::default();
        readopts.set_ignore_range_deletions(self.ignore_range_deletions);
        readopts
    }

    pub fn writeopts(&self) -> WriteOptions {
        let mut opts = WriteOptions::default();
        opts.set_sync(self.sync_to_disk);
        opts
    }

    pub fn set_ignore_range_deletions(mut self, ignore: bool) -> Self {
        self.ignore_range_deletions = ignore;
        self
    }
}

impl Default for ReadWriteOptions {
    fn default() -> Self {
        Self {
            ignore_range_deletions: true,
            sync_to_disk: std::env::var("SUI_DB_SYNC_TO_DISK").map_or(false, |v| v != "0"),
        }
    }
}
// TODO: refactor this into a builder pattern, where rocksdb::Options are
// generated after a call to build().
#[derive(Default, Clone)]
pub struct DBOptions {
    pub options: rocksdb::Options,
    pub rw_options: ReadWriteOptions,
}

impl DBOptions {
    // Optimize lookup perf for tables where no scans are performed.
    // If non-trivial number of values can be > 512B in size, it is beneficial to also
    // specify optimize_for_large_values_no_scan().
    pub fn optimize_for_point_lookup(mut self, block_cache_size_mb: usize) -> DBOptions {
        // NOTE: this overwrites the block options.
        self.options.optimize_for_point_lookup(block_cache_size_mb as u64);
        self
    }

    // Optimize write and lookup perf for tables which are rarely scanned, and have large values.
    // https://rocksdb.org/blog/2021/05/26/integrated-blob-db.html
    pub fn optimize_for_large_values_no_scan(mut self, min_blob_size: u64) -> DBOptions {
        if env::var(ENV_VAR_DISABLE_BLOB_STORAGE).is_ok() {
            info!("Large value blob storage optimization is disabled via env var.");
            return self;
        }

        // Blob settings.
        self.options.set_enable_blob_files(true);
        self.options.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
        self.options.set_enable_blob_gc(true);
        // Since each blob can have non-trivial size overhead, and compression does not work across
        // blobs, set a min blob size in bytes to so small transactions and effects are kept
        // in sst files.
        self.options.set_min_blob_size(min_blob_size);

        // Increase write buffer size to 256MiB.
        let write_buffer_size = read_size_from_env(ENV_VAR_MAX_WRITE_BUFFER_SIZE_MB)
            .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_SIZE_MB)
            * 1024
            * 1024;
        self.options.set_write_buffer_size(write_buffer_size);
        // Since large blobs are not in sst files, reduce the target file size and base level
        // target size.
        let target_file_size_base = 64 << 20;
        self.options.set_target_file_size_base(target_file_size_base);
        // Level 1 default to 64MiB * 6 ~ 384MiB.
        let max_level_zero_file_num = read_size_from_env(ENV_VAR_L0_NUM_FILES_COMPACTION_TRIGGER)
            .unwrap_or(DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER);
        self.options
            .set_max_bytes_for_level_base(target_file_size_base * max_level_zero_file_num as u64);

        self
    }

    // Optimize tables with a mix of lookup and scan workloads.
    pub fn optimize_for_read(mut self, block_cache_size_mb: usize) -> DBOptions {
        self.options.set_block_based_table_factory(&get_block_options(block_cache_size_mb));
        self
    }

    // Optimize DB receiving significant insertions.
    pub fn optimize_db_for_write_throughput(mut self, db_max_write_buffer_gb: u64) -> DBOptions {
        self.options.set_db_write_buffer_size(db_max_write_buffer_gb as usize * 1024 * 1024 * 1024);
        self.options.set_max_total_wal_size(db_max_write_buffer_gb * 1024 * 1024 * 1024);
        self
    }

    // Optimize tables receiving significant insertions.
    pub fn optimize_for_write_throughput(mut self) -> DBOptions {
        // Increase write buffer size to 256MiB.
        let write_buffer_size = read_size_from_env(ENV_VAR_MAX_WRITE_BUFFER_SIZE_MB)
            .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_SIZE_MB)
            * 1024
            * 1024;
        self.options.set_write_buffer_size(write_buffer_size);
        // Increase write buffers to keep to 6 before slowing down writes.
        let max_write_buffer_number = read_size_from_env(ENV_VAR_MAX_WRITE_BUFFER_NUMBER)
            .unwrap_or(DEFAULT_MAX_WRITE_BUFFER_NUMBER);
        self.options.set_max_write_buffer_number(max_write_buffer_number.try_into().unwrap());
        // Keep 1 write buffer so recent writes can be read from memory.
        self.options.set_max_write_buffer_size_to_maintain((write_buffer_size).try_into().unwrap());

        // Increase compaction trigger for level 0 to 6.
        let max_level_zero_file_num = read_size_from_env(ENV_VAR_L0_NUM_FILES_COMPACTION_TRIGGER)
            .unwrap_or(DEFAULT_L0_NUM_FILES_COMPACTION_TRIGGER);
        self.options.set_level_zero_file_num_compaction_trigger(
            max_level_zero_file_num.try_into().unwrap(),
        );
        self.options.set_level_zero_slowdown_writes_trigger(
            (max_level_zero_file_num * 4).try_into().unwrap(),
        );
        self.options
            .set_level_zero_stop_writes_trigger((max_level_zero_file_num * 5).try_into().unwrap());

        // Increase sst file size to 128MiB.
        self.options.set_target_file_size_base(
            read_size_from_env(ENV_VAR_TARGET_FILE_SIZE_BASE_MB)
                .unwrap_or(DEFAULT_TARGET_FILE_SIZE_BASE_MB) as u64
                * 1024
                * 1024,
        );

        // Increase level 1 target size to 256MiB * 6 ~ 1.5GiB.
        self.options
            .set_max_bytes_for_level_base((write_buffer_size * max_level_zero_file_num) as u64);

        self
    }

    // Optimize tables receiving significant deletions.
    // TODO: revisit when intra-epoch pruning is enabled.
    pub fn optimize_for_pruning(mut self) -> DBOptions {
        self.options.set_min_write_buffer_number_to_merge(2);
        self
    }
}

/// Creates a default RocksDB option, to be used when RocksDB option is unspecified.
pub fn default_db_options() -> DBOptions {
    let mut opt = rocksdb::Options::default();

    // One common issue when running tests on Mac is that the default ulimit is too low,
    // leading to I/O errors such as "Too many open files". Raising fdlimit to bypass it.
    if let Ok(outcome) = fdlimit::raise_fd_limit() {
        // on windows raise_fd_limit return None
        match outcome {
            fdlimit::Outcome::LimitRaised { to, .. } => {
                opt.set_max_open_files((to / 8) as i32);
            }
            fdlimit::Outcome::Unsupported => (),
        }
    }

    // The table cache is locked for updates and this determines the number
    // of shards, ie 2^10. Increase in case of lock contentions.
    opt.set_table_cache_num_shard_bits(10);

    // LSM compression settings
    opt.set_min_level_to_compress(2);
    opt.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opt.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opt.set_bottommost_zstd_max_train_bytes(1024 * 1024, true);

    opt.set_max_background_jobs(
        read_size_from_env(ENV_VAR_MAX_BACKGROUND_JOBS).unwrap_or(2).try_into().unwrap(),
    );

    // Sui uses multiple RocksDB in a node, so total sizes of write buffers and WAL can be higher
    // than the limits below.
    //
    // RocksDB also exposes the option to configure total write buffer size across multiple
    // instances via `write_buffer_manager`. But the write buffer flush policy (flushing the
    // buffer receiving the next write) may not work well. So sticking to per-db write buffer
    // size limit for now.
    //
    // The environment variables are only meant to be emergency overrides. They may go away in
    // future. If you need to modify an option, either update the default value, or override the
    // option in Sui / Narwhal.
    opt.set_db_write_buffer_size(
        read_size_from_env(ENV_VAR_DB_WRITE_BUFFER_SIZE).unwrap_or(DEFAULT_DB_WRITE_BUFFER_SIZE)
            * 1024
            * 1024,
    );
    opt.set_max_total_wal_size(
        read_size_from_env(ENV_VAR_DB_WAL_SIZE).unwrap_or(DEFAULT_DB_WAL_SIZE) as u64 * 1024 * 1024,
    );

    opt.increase_parallelism(4);
    opt.set_enable_pipelined_write(true);

    opt.set_block_based_table_factory(&get_block_options(128));

    // Set memtable bloomfilter.
    opt.set_memtable_prefix_bloom_ratio(0.02);

    DBOptions { options: opt, rw_options: ReadWriteOptions::default() }
}

fn get_block_options(block_cache_size_mb: usize) -> BlockBasedOptions {
    // Set options mostly similar to those used in optimize_for_point_lookup(),
    // except non-default binary and hash index, to hopefully reduce lookup latencies
    // without causing any regression for scanning, with slightly more memory usages.
    // https://github.com/facebook/rocksdb/blob/11cb6af6e5009c51794641905ca40ce5beec7fee/options/options.cc#L611-L621
    let mut block_options = BlockBasedOptions::default();
    // Increase block size to 16KiB.
    // https://github.com/EighteenZi/rocksdb_wiki/blob/master/Memory-usage-in-RocksDB.md#indexes-and-filter-blocks
    block_options.set_block_size(16 * 1024);
    // Configure a block cache.
    block_options.set_block_cache(&Cache::new_lru_cache(block_cache_size_mb << 20));
    // Set a bloomfilter with 1% false positive rate.
    block_options.set_bloom_filter(10.0, false);
    // From https://github.com/EighteenZi/rocksdb_wiki/blob/master/Block-Cache.md#caching-index-and-filter-blocks
    block_options.set_pin_l0_filter_and_index_blocks_in_cache(true);
    block_options
}

fn prepare_db_options(db_options: Option<rocksdb::Options>) -> rocksdb::Options {
    // Customize database options
    let mut options = db_options.unwrap_or_else(|| default_db_options().options);
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    options
}

/// Opens a database with options, and a number of column families with individual options that are
/// created if they do not exist.
#[instrument(level="debug", skip_all, fields(path = ?path.as_ref()), err)]
pub fn open_cf_opts_transactional<P: AsRef<Path>>(
    path: P,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[(&str, rocksdb::Options)],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let path = path.as_ref();
    let cfs = populate_missing_cfs(opt_cfs, path)?;
    // See comment above for explanation of why nondeterministic is necessary here.
    nondeterministic!({
        let options = prepare_db_options(db_options);
        let rocksdb = rocksdb::OptimisticTransactionDB::<MultiThreaded>::open_cf_descriptors(
            &options,
            path,
            cfs.into_iter().map(|(name, opts)| ColumnFamilyDescriptor::new(name, opts)),
        )?;
        Ok(Arc::new(RocksDB::OptimisticTransactionDB(OptimisticTransactionDBWrapper {
            underlying: rocksdb,
            metric_conf,
            db_path: PathBuf::from(path),
        })))
    })
}

/// Opens a database with options, and a number of column families with individual options that are
/// created if they do not exist.
pub fn open_cf_opts_secondary<P: AsRef<Path>>(
    primary_path: P,
    secondary_path: Option<P>,
    db_options: Option<rocksdb::Options>,
    metric_conf: MetricConf,
    opt_cfs: &[(&str, rocksdb::Options)],
) -> Result<Arc<RocksDB>, TypedStoreError> {
    let primary_path = primary_path.as_ref();
    let secondary_path = secondary_path.as_ref().map(|p| p.as_ref());
    // See comment above for explanation of why nondeterministic is necessary here.
    nondeterministic!({
        // Customize database options
        let mut options = db_options.unwrap_or_else(|| default_db_options().options);

        // try to raise the fdlimit
        // does nothing on windows
        let _ = fdlimit::raise_fd_limit();
        // This is a requirement by RocksDB when opening as secondary
        options.set_max_open_files(-1);

        let mut opt_cfs: std::collections::HashMap<_, _> = opt_cfs.iter().cloned().collect();
        let cfs = rocksdb::DBWithThreadMode::<MultiThreaded>::list_cf(&options, primary_path)
            .ok()
            .unwrap_or_default();

        let default_db_options = default_db_options();
        // Add CFs not explicitly listed
        for cf_key in cfs.iter() {
            if !opt_cfs.contains_key(&cf_key[..]) {
                opt_cfs.insert(cf_key, default_db_options.options.clone());
            }
        }

        let primary_path = primary_path.to_path_buf();
        let secondary_path = secondary_path.map(|q| q.to_path_buf()).unwrap_or_else(|| {
            let mut s = primary_path.clone();
            s.pop();
            s.push("SECONDARY");
            s.as_path().to_path_buf()
        });

        let rocksdb = {
            options.create_if_missing(true);
            options.create_missing_column_families(true);
            let db = rocksdb::DBWithThreadMode::<MultiThreaded>::open_cf_descriptors_as_secondary(
                &options,
                &primary_path,
                &secondary_path,
                opt_cfs
                    .iter()
                    .map(|(name, opts)| ColumnFamilyDescriptor::new(*name, (*opts).clone())),
            )?;
            db.try_catch_up_with_primary()?;
            db
        };
        Ok(Arc::new(RocksDB::DBWithThreadMode(DBWithThreadModeWrapper {
            underlying: rocksdb,
            metric_conf,
            db_path: secondary_path,
        })))
    })
}

pub fn list_tables(path: std::path::PathBuf) -> eyre::Result<Vec<String>> {
    const DB_DEFAULT_CF_NAME: &str = "default";

    let opts = rocksdb::Options::default();
    rocksdb::DBWithThreadMode::<rocksdb::MultiThreaded>::list_cf(&opts, path)
        .map_err(|e| e.into())
        .map(|q| {
            q.iter()
                .filter_map(|s| {
                    // The `default` table is not used
                    if s != DB_DEFAULT_CF_NAME {
                        Some(s.clone())
                    } else {
                        None
                    }
                })
                .collect()
        })
}

#[derive(Clone)]
pub struct DBMapTableConfigMap(BTreeMap<String, DBOptions>);
impl DBMapTableConfigMap {
    pub fn new(map: BTreeMap<String, DBOptions>) -> Self {
        Self(map)
    }

    pub fn to_map(&self) -> BTreeMap<String, DBOptions> {
        self.0.clone()
    }
}

pub enum RocksDBAccessType {
    Primary,
    Secondary(Option<PathBuf>),
}

pub fn safe_drop_db(path: PathBuf) -> Result<(), rocksdb::Error> {
    rocksdb::DB::destroy(&rocksdb::Options::default(), path)
}

fn populate_missing_cfs(
    input_cfs: &[(&str, rocksdb::Options)],
    path: &Path,
) -> Result<Vec<(String, rocksdb::Options)>, rocksdb::Error> {
    let mut cfs = vec![];
    let input_cf_index: HashSet<_> = input_cfs.iter().map(|(name, _)| *name).collect();
    let existing_cfs =
        rocksdb::DBWithThreadMode::<MultiThreaded>::list_cf(&rocksdb::Options::default(), path)
            .ok()
            .unwrap_or_default();

    for cf_name in existing_cfs {
        if !input_cf_index.contains(&cf_name[..]) {
            cfs.push((cf_name, rocksdb::Options::default()));
        }
    }
    cfs.extend(input_cfs.iter().map(|(name, opts)| (name.to_string(), (*opts).clone())));
    Ok(cfs)
}

/// Given a vec<u8>, find the value which is one more than the vector
/// if the vector was a big endian number.
/// If the vector is already minimum, don't change it.
#[cfg(test)]
fn big_endian_saturating_add_one(v: &mut [u8]) {
    if is_max(v) {
        return;
    }
    for i in (0..v.len()).rev() {
        if v[i] == u8::MAX {
            v[i] = 0;
        } else {
            v[i] += 1;
            break;
        }
    }
}

/// Check if all the bytes in the vector are 0xFF
#[cfg(test)]
fn is_max(v: &[u8]) -> bool {
    v.iter().all(|&x| x == u8::MAX)
}
