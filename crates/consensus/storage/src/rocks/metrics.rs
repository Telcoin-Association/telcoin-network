// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use prometheus::{
    default_registry, register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_vec_with_registry, HistogramVec, IntCounterVec, IntGaugeVec, Registry,
};
use rocksdb::{perf::set_perf_stats, PerfContext, PerfMetric, PerfStatsLevel};
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

thread_local! {
    static PER_THREAD_ROCKS_PERF_CONTEXT: std::cell::RefCell<rocksdb::PerfContext>  = RefCell::new(PerfContext::default());
}

const LATENCY_SEC_BUCKETS: &[f64] =
    &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1., 2.5, 5., 10., 20., 30., 60., 90.];

#[derive(Debug, Clone)]
// A struct for sampling based on number of operations or duration.
// Sampling happens if the duration expires and after number of operations
pub struct SamplingInterval {
    // Sample once every time duration
    pub once_every_duration: Duration,
    // Sample once every number of operations
    pub after_num_ops: u64,
    // Counter for keeping track of previous sample
    pub counter: Arc<AtomicU64>,
}

impl Default for SamplingInterval {
    fn default() -> Self {
        // Enabled with 60 second interval
        SamplingInterval::new(Duration::from_secs(60), 0)
    }
}

impl SamplingInterval {
    pub fn new(once_every_duration: Duration, after_num_ops: u64) -> Self {
        let counter = Arc::new(AtomicU64::new(1));
        if !once_every_duration.is_zero() {
            let counter = counter.clone();
            std::thread::spawn(move || loop {
                if counter.load(Ordering::SeqCst) > after_num_ops {
                    counter.store(0, Ordering::SeqCst);
                }
                std::thread::sleep(once_every_duration);
            });
        }
        SamplingInterval { once_every_duration, after_num_ops, counter }
    }
    pub fn new_from_self(&self) -> SamplingInterval {
        SamplingInterval::new(self.once_every_duration, self.after_num_ops)
    }
    pub fn sample(&self) -> bool {
        if self.once_every_duration.is_zero() {
            self.counter.fetch_add(1, Ordering::Relaxed) % (self.after_num_ops + 1) == 0
        } else {
            self.counter.fetch_add(1, Ordering::Relaxed) == 0
        }
    }
}

#[derive(Debug)]
pub struct ColumnFamilyMetrics {
    pub rocksdb_total_sst_files_size: IntGaugeVec,
    pub rocksdb_total_blob_files_size: IntGaugeVec,
    pub rocksdb_size_all_mem_tables: IntGaugeVec,
    pub rocksdb_num_snapshots: IntGaugeVec,
    pub rocksdb_oldest_snapshot_time: IntGaugeVec,
    pub rocksdb_actual_delayed_write_rate: IntGaugeVec,
    pub rocksdb_is_write_stopped: IntGaugeVec,
    pub rocksdb_block_cache_capacity: IntGaugeVec,
    pub rocksdb_block_cache_usage: IntGaugeVec,
    pub rocksdb_block_cache_pinned_usage: IntGaugeVec,
    pub rocskdb_estimate_table_readers_mem: IntGaugeVec,
    pub rocksdb_mem_table_flush_pending: IntGaugeVec,
    pub rocskdb_compaction_pending: IntGaugeVec,
    pub rocskdb_num_running_compactions: IntGaugeVec,
    pub rocksdb_num_running_flushes: IntGaugeVec,
    pub rocksdb_estimate_oldest_key_time: IntGaugeVec,
    pub rocskdb_background_errors: IntGaugeVec,
    pub rocksdb_estimated_num_keys: IntGaugeVec,
}

impl ColumnFamilyMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            rocksdb_total_sst_files_size: register_int_gauge_vec_with_registry!(
                "rocksdb_total_sst_files_size",
                "The storage size occupied by the sst files in the column family",
                &["cf_name"],
                registry,
            )?,
            rocksdb_total_blob_files_size: register_int_gauge_vec_with_registry!(
                "rocksdb_total_blob_files_size",
                "The storage size occupied by the blob files in the column family",
                &["cf_name"],
                registry,
            )?,
            rocksdb_size_all_mem_tables: register_int_gauge_vec_with_registry!(
                "rocksdb_size_all_mem_tables",
                "The memory size occupied by the column family's in-memory buffer",
                &["cf_name"],
                registry,
            )?,
            rocksdb_num_snapshots: register_int_gauge_vec_with_registry!(
                "rocksdb_num_snapshots",
                "Number of snapshots held for the column family",
                &["cf_name"],
                registry,
            )?,
            rocksdb_oldest_snapshot_time: register_int_gauge_vec_with_registry!(
                "rocksdb_oldest_snapshot_time",
                "Unit timestamp of the oldest unreleased snapshot",
                &["cf_name"],
                registry,
            )?,
            rocksdb_actual_delayed_write_rate: register_int_gauge_vec_with_registry!(
                "rocksdb_actual_delayed_write_rate",
                "The current actual delayed write rate. 0 means no delay",
                &["cf_name"],
                registry,
            )?,
            rocksdb_is_write_stopped: register_int_gauge_vec_with_registry!(
                "rocksdb_is_write_stopped",
                "A flag indicating whether writes are stopped on this column family. 1 indicates writes have been stopped.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_block_cache_capacity: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_capacity",
                "The block cache capacity of the column family.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_block_cache_usage: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_usage",
                "The memory size used by the column family in the block cache.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_block_cache_pinned_usage: register_int_gauge_vec_with_registry!(
                "rocksdb_block_cache_pinned_usage",
                "The memory size used by the column family in the block cache where entries are pinned",
                &["cf_name"],
                registry,
            )?,
            rocskdb_estimate_table_readers_mem: register_int_gauge_vec_with_registry!(
                "rocskdb_estimate_table_readers_mem",
                "The estimated memory size used for reading SST tables in this column
                family such as filters and index blocks. Note that this number does not
                include the memory used in block cache.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_mem_table_flush_pending: register_int_gauge_vec_with_registry!(
                "rocksdb_mem_table_flush_pending",
                "A 1 or 0 flag indicating whether a memtable flush is pending.
                If this number is 1, it means a memtable is waiting for being flushed,
                but there might be too many L0 files that prevents it from being flushed.",
                &["cf_name"],
                registry,
            )?,
            rocskdb_compaction_pending: register_int_gauge_vec_with_registry!(
                "rocskdb_compaction_pending",
                "A 1 or 0 flag indicating whether a compaction job is pending.
                If this number is 1, it means some part of the column family requires
                compaction in order to maintain shape of LSM tree, but the compaction
                is pending because the desired compaction job is either waiting for
                other dependent compactions to be finished or waiting for an available
                compaction thread.",
                &["cf_name"],
                registry,
            )?,
            rocskdb_num_running_compactions: register_int_gauge_vec_with_registry!(
                "rocskdb_num_running_compactions",
                "The number of compactions that are currently running for the column family.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_num_running_flushes: register_int_gauge_vec_with_registry!(
                "rocksdb_num_running_flushes",
                "The number of flushes that are currently running for the column family.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_estimate_oldest_key_time: register_int_gauge_vec_with_registry!(
                "rocksdb_estimate_oldest_key_time",
                "Estimation of the oldest key timestamp in the DB. Only available
                for FIFO compaction with compaction_options_fifo.allow_compaction = false.",
                &["cf_name"],
                registry,
            )?,
            rocksdb_estimated_num_keys: register_int_gauge_vec_with_registry!(
                "rocksdb_estimated_num_keys",
                "The estimated number of keys in the table",
                &["cf_name"],
                registry,
            )?,
            rocskdb_background_errors: register_int_gauge_vec_with_registry!(
                "rocskdb_background_errors",
                "The accumulated number of RocksDB background errors.",
                &["cf_name"],
                registry,
            )?,
        })
    }
}

#[derive(Debug)]
pub struct OperationMetrics {
    pub rocksdb_iter_latency_seconds: HistogramVec,
    pub rocksdb_iter_bytes: HistogramVec,
    pub rocksdb_iter_keys: HistogramVec,
    pub rocksdb_get_latency_seconds: HistogramVec,
    pub rocksdb_get_bytes: HistogramVec,
    pub rocksdb_multiget_latency_seconds: HistogramVec,
    pub rocksdb_multiget_bytes: HistogramVec,
    pub rocksdb_put_latency_seconds: HistogramVec,
    pub rocksdb_put_bytes: HistogramVec,
    pub rocksdb_delete_latency_seconds: HistogramVec,
    pub rocksdb_deletes: IntCounterVec,
    pub rocksdb_batch_commit_latency_seconds: HistogramVec,
    pub rocksdb_batch_commit_bytes: HistogramVec,
}

impl OperationMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            rocksdb_iter_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_iter_latency_seconds",
                "Rocksdb iter latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_iter_bytes: register_histogram_vec_with_registry!(
                "rocksdb_iter_bytes",
                "Rocksdb iter size in bytes",
                &["cf_name"],
                registry,
            )?,
            rocksdb_iter_keys: register_histogram_vec_with_registry!(
                "rocksdb_iter_keys",
                "Rocksdb iter num keys",
                &["cf_name"],
                registry,
            )?,
            rocksdb_get_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_get_latency_seconds",
                "Rocksdb get latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_get_bytes: register_histogram_vec_with_registry!(
                "rocksdb_get_bytes",
                "Rocksdb get call returned data size in bytes",
                &["cf_name"],
                registry
            )?,
            rocksdb_multiget_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_multiget_latency_seconds",
                "Rocksdb multiget latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_multiget_bytes: register_histogram_vec_with_registry!(
                "rocksdb_multiget_bytes",
                "Rocksdb multiget call returned data size in bytes",
                &["cf_name"],
                registry,
            )?,
            rocksdb_put_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_put_latency_seconds",
                "Rocksdb put latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_put_bytes: register_histogram_vec_with_registry!(
                "rocksdb_put_bytes",
                "Rocksdb put call puts data size in bytes",
                &["cf_name"],
                registry,
            )?,
            rocksdb_delete_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_delete_latency_seconds",
                "Rocksdb delete latency in seconds",
                &["cf_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_deletes: register_int_counter_vec_with_registry!(
                "rocksdb_deletes",
                "Rocksdb delete calls",
                &["cf_name"],
                registry
            )?,
            rocksdb_batch_commit_latency_seconds: register_histogram_vec_with_registry!(
                "rocksdb_write_batch_commit_latency_seconds",
                "Rocksdb schema batch commit latency in seconds",
                &["db_name"],
                LATENCY_SEC_BUCKETS.to_vec(),
                registry,
            )?,
            rocksdb_batch_commit_bytes: register_histogram_vec_with_registry!(
                "rocksdb_batch_commit_bytes",
                "Rocksdb schema batch commit size in bytes",
                &["db_name"],
                registry,
            )?,
        })
    }
}

pub struct RocksDBPerfContext;

impl Default for RocksDBPerfContext {
    fn default() -> Self {
        set_perf_stats(PerfStatsLevel::EnableTime);
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context| {
            perf_context.borrow_mut().reset();
        });
        RocksDBPerfContext {}
    }
}

impl Drop for RocksDBPerfContext {
    fn drop(&mut self) {
        set_perf_stats(PerfStatsLevel::Disable);
    }
}

#[derive(Debug)]
pub struct ReadPerfContextMetrics {
    pub user_key_comparison_count: IntCounterVec,
    pub block_cache_hit_count: IntCounterVec,
    pub block_read_count: IntCounterVec,
    pub block_read_byte: IntCounterVec,
    pub block_read_nanos: IntCounterVec,
    pub block_checksum_nanos: IntCounterVec,
    pub block_decompress_nanos: IntCounterVec,
    pub get_read_bytes: IntCounterVec,
    pub multiget_read_bytes: IntCounterVec,
    pub get_snapshot_nanos: IntCounterVec,
    pub get_from_memtable_nanos: IntCounterVec,
    pub get_from_memtable_count: IntCounterVec,
    pub get_post_process_nanos: IntCounterVec,
    pub get_from_output_files_nanos: IntCounterVec,
    pub db_mutex_lock_nanos: IntCounterVec,
    pub db_condition_wait_nanos: IntCounterVec,
    pub merge_operator_nanos: IntCounterVec,
    pub read_index_block_nanos: IntCounterVec,
    pub read_filter_block_nanos: IntCounterVec,
    pub new_table_block_iter_nanos: IntCounterVec,
    pub block_seek_nanos: IntCounterVec,
    pub find_table_nanos: IntCounterVec,
    pub bloom_memtable_hit_count: IntCounterVec,
    pub bloom_memtable_miss_count: IntCounterVec,
    pub bloom_sst_hit_count: IntCounterVec,
    pub bloom_sst_miss_count: IntCounterVec,
    pub key_lock_wait_time: IntCounterVec,
    pub key_lock_wait_count: IntCounterVec,
    pub internal_delete_skipped_count: IntCounterVec,
    pub internal_skipped_count: IntCounterVec,
}

impl ReadPerfContextMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            user_key_comparison_count: register_int_counter_vec_with_registry!(
                "user_key_comparison_count",
                "Helps us figure out whether too many comparisons in binary search can be a problem,
                especially when a more expensive comparator is used. Moreover, since number of comparisons
                is usually uniform based on the memtable size, the SST file size for Level 0 and size of other
                levels, an significant increase of the counter can indicate unexpected LSM-tree shape.
                You may want to check whether flush/compaction can keep up with the write speed",
                &["cf_name"],
                registry,
            )?,
            block_cache_hit_count: register_int_counter_vec_with_registry!(
                "block_cache_hit_count",
                "Tells us how many times we read data blocks from block cache, and block_read_count tells us how many
                times we have to read blocks from the file system (either block cache is disabled or it is a cache miss).
                We can evaluate the block cache efficiency by looking at the two counters over time.",
                &["cf_name"],
                registry,
            )?,
            block_read_count: register_int_counter_vec_with_registry!(
                "block_read_count",
                "Tells us how many times we have to read blocks from the file system (either block cache is disabled or it is a cache miss)",
                &["cf_name"],
                registry,
            )?,
            block_read_byte: register_int_counter_vec_with_registry!(
                "block_read_byte",
                "Tells us how many total bytes we read from the file system. It can tell us whether a slow query can be caused by reading
                large blocks from the file system. Index and bloom filter blocks are usually large blocks. A large block can also be the result
                of a very large key or value",
                &["cf_name"],
                registry,
            )?,
            block_read_nanos: register_int_counter_vec_with_registry!(
                "block_read_nanos",
                "Total nanos spent on block reads",
                &["cf_name"],
                registry,
            )?,
            block_checksum_nanos: register_int_counter_vec_with_registry!(
                "block_checksum_nanos",
                "Total nanos spent on verifying block checksum",
                &["cf_name"],
                registry,
            )?,
            block_decompress_nanos: register_int_counter_vec_with_registry!(
                "block_decompress_nanos",
                "Total nanos spent on decompressing a block",
                &["cf_name"],
                registry,
            )?,
            get_read_bytes: register_int_counter_vec_with_registry!(
                "get_read_bytes",
                "Total bytes for values returned by Get",
                &["cf_name"],
                registry,
            )?,
            multiget_read_bytes: register_int_counter_vec_with_registry!(
                "multiget_read_bytes",
                "Total bytes for values returned by MultiGet.",
                &["cf_name"],
                registry,
            )?,
            get_snapshot_nanos: register_int_counter_vec_with_registry!(
                "get_snapshot_nanos",
                "Time spent in getting snapshot.",
                &["cf_name"],
                registry,
            )?,
            get_from_memtable_nanos: register_int_counter_vec_with_registry!(
                "get_from_memtable_nanos",
                "Time spent on reading data from memtable.",
                &["cf_name"],
                registry,
            )?,
            get_from_memtable_count: register_int_counter_vec_with_registry!(
                "get_from_memtable_count",
                "Number of memtables queried",
                &["cf_name"],
                registry,
            )?,
            get_post_process_nanos: register_int_counter_vec_with_registry!(
                "get_post_process_nanos",
                "Total nanos spent after Get() finds a key",
                &["cf_name"],
                registry,
            )?,
            get_from_output_files_nanos: register_int_counter_vec_with_registry!(
                "get_from_output_files_nanos",
                "Total nanos reading from output files",
                &["cf_name"],
                registry,
            )?,
            db_mutex_lock_nanos: register_int_counter_vec_with_registry!(
                "db_mutex_lock_nanos",
                "Time spent on acquiring db mutex",
                &["cf_name"],
                registry,
            )?,
            db_condition_wait_nanos: register_int_counter_vec_with_registry!(
                "db_condition_wait_nanos",
                "Time spent waiting with a condition variable created with DB Mutex.",
                &["cf_name"],
                registry,
            )?,
            merge_operator_nanos: register_int_counter_vec_with_registry!(
                "merge_operator_nanos",
                "Time spent on merge operator.",
                &["cf_name"],
                registry,
            )?,
            read_index_block_nanos: register_int_counter_vec_with_registry!(
                "read_index_block_nanos",
                "Time spent on reading index block from block cache or SST file",
                &["cf_name"],
                registry,
            )?,
            read_filter_block_nanos: register_int_counter_vec_with_registry!(
                "read_filter_block_nanos",
                "Time spent on reading filter block from block cache or SST file",
                &["cf_name"],
                registry,
            )?,
            new_table_block_iter_nanos: register_int_counter_vec_with_registry!(
                "new_table_block_iter_nanos",
                "Time spent on creating data block iterator",
                &["cf_name"],
                registry,
            )?,
            block_seek_nanos: register_int_counter_vec_with_registry!(
                "block_seek_nanos",
                "Time spent on seeking a key in data/index blocks",
                &["cf_name"],
                registry,
            )?,
            find_table_nanos: register_int_counter_vec_with_registry!(
                "find_table_nanos",
                "Time spent on finding or creating a table reader",
                &["cf_name"],
                registry,
            )?,
            bloom_memtable_hit_count: register_int_counter_vec_with_registry!(
                "bloom_memtable_hit_count",
                "Total number of mem table bloom hits",
                &["cf_name"],
                registry,
            )?,
            bloom_memtable_miss_count: register_int_counter_vec_with_registry!(
                "bloom_memtable_miss_count",
                "Total number of mem table bloom misses",
                &["cf_name"],
                registry,
            )?,
            bloom_sst_hit_count: register_int_counter_vec_with_registry!(
                "bloom_sst_hit_count",
                "Total number of SST table bloom hits",
                &["cf_name"],
                registry,
            )?,
            bloom_sst_miss_count: register_int_counter_vec_with_registry!(
                "bloom_sst_miss_count",
                "Total number of SST table bloom misses",
                &["cf_name"],
                registry,
            )?,
            key_lock_wait_time: register_int_counter_vec_with_registry!(
                "key_lock_wait_time",
                "Time spent waiting on key locks in transaction lock manager",
                &["cf_name"],
                registry,
            )?,
            key_lock_wait_count: register_int_counter_vec_with_registry!(
                "key_lock_wait_count",
                "Number of times acquiring a lock was blocked by another transaction",
                &["cf_name"],
                registry,
            )?,
            internal_delete_skipped_count: register_int_counter_vec_with_registry!(
                "internal_delete_skipped_count",
                "Total number of deleted keys skipped during iteration",
                &["cf_name"],
                registry,
            )?,
            internal_skipped_count: register_int_counter_vec_with_registry!(
                "internal_skipped_count",
                "Totall number of internal keys skipped during iteration",
                &["cf_name"],
                registry,
            )?,
        })
    }

    pub fn report_metrics(&self, cf_name: &str) {
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context_cell| {
            set_perf_stats(PerfStatsLevel::Disable);
            let perf_context = perf_context_cell.borrow();
            self.user_key_comparison_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::UserKeyComparisonCount));
            self.block_cache_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockCacheHitCount));
            self.block_read_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadCount));
            self.block_read_byte
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadByte));
            self.block_read_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadTime));
            self.block_read_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockReadCount));
            self.block_checksum_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockChecksumTime));
            self.block_decompress_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockDecompressTime));
            self.get_read_bytes
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetReadBytes));
            self.multiget_read_bytes
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::MultigetReadBytes));
            self.get_snapshot_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetSnapshotTime));
            self.get_from_memtable_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromMemtableTime));
            self.get_from_memtable_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromMemtableCount));
            self.get_post_process_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetPostProcessTime));
            self.get_from_output_files_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::GetFromOutputFilesTime));
            self.db_mutex_lock_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::DbMutexLockNanos));
            self.db_condition_wait_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::DbConditionWaitNanos));
            self.merge_operator_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::MergeOperatorTimeNanos));
            self.read_index_block_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::ReadIndexBlockNanos));
            self.read_filter_block_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::ReadFilterBlockNanos));
            self.new_table_block_iter_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::NewTableBlockIterNanos));
            self.block_seek_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BlockSeekNanos));
            self.find_table_nanos
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::FindTableNanos));
            self.bloom_memtable_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomMemtableHitCount));
            self.bloom_memtable_miss_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomMemtableMissCount));
            self.bloom_sst_hit_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomSstHitCount));
            self.bloom_sst_miss_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::BloomSstMissCount));
            self.key_lock_wait_time
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitTime));
            self.key_lock_wait_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitCount));
            self.internal_delete_skipped_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::InternalDeleteSkippedCount));
            self.internal_skipped_count
                .with_label_values(&[cf_name])
                .inc_by(perf_context.metric(PerfMetric::InternalKeySkippedCount));
        });
    }
}

#[derive(Debug)]
pub struct WritePerfContextMetrics {
    pub write_wal_nanos: IntCounterVec,
    pub write_memtable_nanos: IntCounterVec,
    pub write_delay_nanos: IntCounterVec,
    pub write_pre_and_post_process_nanos: IntCounterVec,
    pub write_db_mutex_lock_nanos: IntCounterVec,
    pub write_db_condition_wait_nanos: IntCounterVec,
    pub write_key_lock_wait_nanos: IntCounterVec,
    pub write_key_lock_wait_count: IntCounterVec,
}

impl WritePerfContextMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            write_wal_nanos: register_int_counter_vec_with_registry!(
                "write_wal_nanos",
                "Total nanos spent on writing to WAL",
                &["cf_name"],
                registry,
            )?,
            write_memtable_nanos: register_int_counter_vec_with_registry!(
                "write_memtable_nanos",
                "Total nanos spent on writing to memtable",
                &["cf_name"],
                registry,
            )?,
            write_delay_nanos: register_int_counter_vec_with_registry!(
                "write_delay_nanos",
                "Total nanos spent on delaying or throttling write",
                &["cf_name"],
                registry,
            )?,
            write_pre_and_post_process_nanos: register_int_counter_vec_with_registry!(
                "write_pre_and_post_process_nanos",
                "Total nanos spent on writing a record, excluding the above four things",
                &["cf_name"],
                registry,
            )?,
            write_db_mutex_lock_nanos: register_int_counter_vec_with_registry!(
                "write_db_mutex_lock_nanos",
                "Time spent on acquiring db mutex",
                &["cf_name"],
                registry,
            )?,
            write_db_condition_wait_nanos: register_int_counter_vec_with_registry!(
                "write_db_condition_wait_nanos",
                "Time spent waiting with a condition variable created with DB Mutex.",
                &["cf_name"],
                registry,
            )?,
            write_key_lock_wait_nanos: register_int_counter_vec_with_registry!(
                "write_key_lock_wait_time",
                "Time spent waiting on key locks in transaction lock manager",
                &["cf_name"],
                registry,
            )?,
            write_key_lock_wait_count: register_int_counter_vec_with_registry!(
                "write_key_lock_wait_count",
                "Number of times acquiring a lock was blocked by another transaction",
                &["cf_name"],
                registry,
            )?,
        })
    }
    pub fn report_metrics(&self, db_name: &str) {
        PER_THREAD_ROCKS_PERF_CONTEXT.with(|perf_context_cell| {
            set_perf_stats(PerfStatsLevel::Disable);
            let perf_context = perf_context_cell.borrow();
            self.write_wal_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteWalTime));
            self.write_memtable_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteMemtableTime));
            self.write_delay_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WriteDelayTime));
            self.write_pre_and_post_process_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::WritePreAndPostProcessTime));
            self.write_db_mutex_lock_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::DbMutexLockNanos));
            self.write_db_condition_wait_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::DbConditionWaitNanos));
            self.write_key_lock_wait_nanos
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitTime));
            self.write_key_lock_wait_count
                .with_label_values(&[db_name])
                .inc_by(perf_context.metric(PerfMetric::KeyLockWaitCount));
        });
    }
}

#[derive(Debug)]
pub struct DBMetrics {
    pub op_metrics: OperationMetrics,
    pub cf_metrics: ColumnFamilyMetrics,
    pub read_perf_ctx_metrics: ReadPerfContextMetrics,
    pub write_perf_ctx_metrics: WritePerfContextMetrics,
}

impl DBMetrics {
    fn try_new(registry: &Registry) -> Result<Self, prometheus::Error> {
        Ok(Self {
            op_metrics: OperationMetrics::try_new(registry)?,
            cf_metrics: ColumnFamilyMetrics::try_new(registry)?,
            read_perf_ctx_metrics: ReadPerfContextMetrics::try_new(registry)?,
            write_perf_ctx_metrics: WritePerfContextMetrics::try_new(registry)?,
        })
    }
}

impl Default for DBMetrics {
    fn default() -> Self {
        // try_new() should not fail except under certain conditions with testing (see comment
        // below). This pushes the panic or retry decision lower and supporting try_new
        // allways a user to deal with errors if desired (have a non-panic option).
        // We always want do use default_registry() when not in test.
        match Self::try_new(default_registry()) {
            Ok(metrics) => metrics,
            Err(_) => {
                // If we are in a test then don't panic on prometheus errors (usually an already
                // registered error) but try again with a new Registry. This is not
                // great for prod code, however should not happen, but will happen in tests due to
                // how Rust runs them so lets just gloss over it. cfg(test) does not
                // always work as expected.
                Self::try_new(&Registry::new()).expect("Prometheus error, are you using it wrong?")
            }
        }
    }
}
