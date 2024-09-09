// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use rocksdb::{properties, AsColumnFamilyRef, Transaction};
use tracing::error;

use crate::{
    rocks::CF_METRICS_REPORT_PERIOD_MILLIS,
    traits::{DBIter, Database, DbTx, DbTxMut, Table},
    BATCHES_CF, CERTIFICATES_CF, CERTIFICATE_DIGEST_BY_ORIGIN_CF, CERTIFICATE_DIGEST_BY_ROUND_CF,
    COMMITTED_SUB_DAG_INDEX_CF, LAST_COMMITTED_CF, LAST_PROPOSED_CF, PAYLOAD_CF, VOTES_CF,
};
use std::{fmt::Debug, path::Path, sync::Arc, time::Duration};

use super::{
    be_fix_int_ser, default_db_options,
    iter::Iter,
    metrics::{DBMetrics, RocksDBPerfContext, SamplingInterval},
    open_cf_opts_transactional, MetricConf, ReadWriteOptions, METRICS_ERROR,
    ROCKSDB_PROPERTY_TOTAL_BLOB_FILES_SIZE,
};

pub struct RocksDbTxMut<'txn> {
    db: RocksDatabase,
    txn: Transaction<'txn, rocksdb::OptimisticTransactionDB>,
}

impl<'txn> Debug for RocksDbTxMut<'txn> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RocksDbTxMut")
    }
}

impl<'txn> DbTx for RocksDbTxMut<'txn> {
    fn get<T: crate::traits::Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let cf = self
            .db
            .rocksdb
            .cf_handle(T::NAME)
            .unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let key_buf = be_fix_int_ser(key)?;
        Ok(self
            .txn
            .get_cf(&cf, key_buf)
            .map(|res| res.and_then(|bytes| bcs::from_bytes::<T::Value>(&bytes).ok()))?)
    }
}

impl<'txn> DbTxMut for RocksDbTxMut<'txn> {
    fn insert<T: crate::traits::Table>(
        &mut self,
        key: &T::Key,
        value: &T::Value,
    ) -> eyre::Result<()> {
        let cf = self
            .db
            .rocksdb
            .cf_handle(T::NAME)
            .unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db
            .db_metrics
            .op_metrics
            .rocksdb_put_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let perf_ctx =
            if self.db.write_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let key_buf = be_fix_int_ser(key)?;
        let value_buf = bcs::to_bytes(value)?;
        self.db
            .db_metrics
            .op_metrics
            .rocksdb_put_bytes
            .with_label_values(&[T::NAME])
            .observe((key_buf.len() + value_buf.len()) as f64);
        if perf_ctx.is_some() {
            self.db.db_metrics.write_perf_ctx_metrics.report_metrics(T::NAME);
        }
        self.txn.put_cf(&cf, &key_buf, &value_buf)?;
        Ok(())
    }

    fn remove<T: crate::traits::Table>(&mut self, key: &T::Key) -> eyre::Result<()> {
        let cf = self
            .db
            .rocksdb
            .cf_handle(T::NAME)
            .unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db
            .db_metrics
            .op_metrics
            .rocksdb_delete_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let perf_ctx =
            if self.db.write_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let key_buf = be_fix_int_ser(key)?;
        self.txn.delete_cf(&cf, key_buf)?;
        self.db.db_metrics.op_metrics.rocksdb_deletes.with_label_values(&[T::NAME]).inc();
        if perf_ctx.is_some() {
            self.db.db_metrics.write_perf_ctx_metrics.report_metrics(T::NAME);
        }
        Ok(())
    }

    fn clear_table<T: crate::traits::Table>(&mut self) -> eyre::Result<()> {
        // This is not using the transaction but ReDB can so leaving in the TXN for now...
        let _ = self.db.rocksdb.drop_cf(T::NAME);
        self.db.rocksdb.create_cf(T::NAME, &default_db_options().options)?;
        Ok(())
    }

    fn commit(self) -> eyre::Result<()> {
        Ok(self.txn.commit()?)
    }
}

/// An interface to a btree map database. This is mainly intended
/// for tests and performing benchmark comparisons or anywhere where an ephemeral database is
/// useful.
#[derive(Clone)]
pub struct RocksDatabase {
    rocksdb: Arc<super::RocksDB>,
    opts: ReadWriteOptions,
    db_metrics: Arc<DBMetrics>,
    get_sample_interval: SamplingInterval,
    write_sample_interval: SamplingInterval,
    iter_sample_interval: SamplingInterval,
    metrics_task_cancel_handle: Arc<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl Drop for RocksDatabase {
    fn drop(&mut self) {
        let _ = Arc::get_mut(&mut self.metrics_task_cancel_handle)
            .map(|c| c.take().map(|c| c.send(())));
    }
}

impl RocksDatabase {
    fn new(
        db: Arc<super::RocksDB>,
        opts: &ReadWriteOptions,
        opt_cfs: &[(&'static str, rocksdb::Options)],
    ) -> Self {
        let db_cloned = db.clone();
        let db_metrics = Arc::new(DBMetrics::default());
        let db_metrics_cloned = db_metrics.clone();
        let (sender, mut recv) = tokio::sync::oneshot::channel();
        let cfs: Arc<Vec<(&'static str, Arc<DBMetrics>)>> =
            Arc::new(opt_cfs.iter().map(|(cf, _)| (*cf, Arc::new(DBMetrics::default()))).collect());
        tokio::task::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(CF_METRICS_REPORT_PERIOD_MILLIS));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let cfs_cloned = cfs.clone();
                        for (cf, db_metrics) in cfs_cloned.iter() {
                            let db_metrics = db_metrics.clone();
                            let cf = *cf;
                            let db_cloned = db_cloned.clone();
                            if let Err(e) = tokio::task::spawn_blocking(move || {
                                Self::report_metrics(&db_cloned, cf, &db_metrics);
                            }).await {
                                error!("Failed to log metrics with error: {}", e);
                            }
                        }
                    }
                    _ = &mut recv => break,
                }
            }
        });
        Self {
            rocksdb: db.clone(),
            opts: opts.clone(),
            db_metrics: db_metrics_cloned,
            get_sample_interval: db.get_sampling_interval(),
            write_sample_interval: db.write_sampling_interval(),
            iter_sample_interval: db.iter_sampling_interval(),
            metrics_task_cancel_handle: Arc::new(Some(sender)),
        }
    }

    pub fn open_db<P: AsRef<Path>>(path: P) -> eyre::Result<RocksDatabase> {
        let db_options = default_db_options().optimize_db_for_write_throughput(2);
        let mut metrics_conf = MetricConf::with_db_name("consensus_epoch");
        metrics_conf.read_sample_interval = SamplingInterval::new(Duration::from_secs(60), 0);
        let cf_options = db_options.options.clone();
        let column_family_options = vec![
            (LAST_PROPOSED_CF, cf_options.clone()),
            (VOTES_CF, cf_options.clone()),
            (
                CERTIFICATES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (CERTIFICATE_DIGEST_BY_ROUND_CF, cf_options.clone()),
            (CERTIFICATE_DIGEST_BY_ORIGIN_CF, cf_options.clone()),
            (PAYLOAD_CF, cf_options.clone()),
            (
                BATCHES_CF,
                default_db_options()
                    .optimize_for_write_throughput()
                    .optimize_for_large_values_no_scan(1 << 10)
                    .options,
            ),
            (LAST_COMMITTED_CF, cf_options.clone()),
            (COMMITTED_SUB_DAG_INDEX_CF, cf_options),
        ];
        let rocksdb = open_cf_opts_transactional(
            path,
            Some(db_options.options),
            metrics_conf,
            &column_family_options,
        )
        .expect("Cannot open database");
        Ok(Self::new(rocksdb, &crate::rocks::ReadWriteOptions::default(), &column_family_options))
    }

    /// Open a DB with a single table, for testing.
    pub fn open_db_with_table<T: Table, P: AsRef<Path>>(path: P) -> eyre::Result<RocksDatabase> {
        let db_options = default_db_options().optimize_db_for_write_throughput(2);
        let mut metrics_conf = MetricConf::with_db_name("consensus_epoch");
        metrics_conf.read_sample_interval = SamplingInterval::new(Duration::from_secs(60), 0);
        let cf_options = db_options.options.clone();
        let column_family_options = vec![(T::NAME, cf_options.clone())];
        let rocksdb = open_cf_opts_transactional(
            path,
            Some(db_options.options),
            metrics_conf,
            &column_family_options,
        )
        .expect("Cannot open database");
        Ok(Self::new(rocksdb, &crate::rocks::ReadWriteOptions::default(), &column_family_options))
    }

    fn get_int_property(
        rocksdb: &super::RocksDB,
        cf: &impl AsColumnFamilyRef,
        property_name: &'static std::ffi::CStr,
    ) -> Result<i64, super::TypedStoreError> {
        match rocksdb.property_int_value_cf(cf, property_name) {
            Ok(Some(value)) => Ok(value.try_into().unwrap()),
            Ok(None) => Ok(0),
            Err(e) => Err(super::TypedStoreError::RocksDBError(e.into_string())),
        }
    }

    fn report_metrics(rocksdb: &Arc<super::RocksDB>, cf_name: &str, db_metrics: &Arc<DBMetrics>) {
        let cf = rocksdb.cf_handle(cf_name).expect("Failed to get cf");
        db_metrics.cf_metrics.rocksdb_total_sst_files_size.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::TOTAL_SST_FILES_SIZE)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_total_blob_files_size.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, ROCKSDB_PROPERTY_TOTAL_BLOB_FILES_SIZE)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_size_all_mem_tables.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::SIZE_ALL_MEM_TABLES)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_num_snapshots.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::NUM_SNAPSHOTS)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_oldest_snapshot_time.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::OLDEST_SNAPSHOT_TIME)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_actual_delayed_write_rate.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::ACTUAL_DELAYED_WRITE_RATE)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_is_write_stopped.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::IS_WRITE_STOPPED)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_block_cache_capacity.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_CAPACITY)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_block_cache_usage.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_USAGE)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_block_cache_pinned_usage.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::BLOCK_CACHE_PINNED_USAGE)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocskdb_estimate_table_readers_mem.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_TABLE_READERS_MEM)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_estimated_num_keys.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_NUM_KEYS)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_mem_table_flush_pending.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::MEM_TABLE_FLUSH_PENDING)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocskdb_compaction_pending.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::COMPACTION_PENDING)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocskdb_num_running_compactions.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::NUM_RUNNING_COMPACTIONS)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_num_running_flushes.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::NUM_RUNNING_FLUSHES)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocksdb_estimate_oldest_key_time.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::ESTIMATE_OLDEST_KEY_TIME)
                .unwrap_or(METRICS_ERROR),
        );
        db_metrics.cf_metrics.rocskdb_background_errors.with_label_values(&[cf_name]).set(
            Self::get_int_property(rocksdb, &cf, properties::BACKGROUND_ERRORS)
                .unwrap_or(METRICS_ERROR),
        );
    }

    /// Returns an unbounded iterator visiting each key-value pair in the map.
    /// This is potentially unsafe as it can perform a full table scan
    fn unbounded_iter_inner<T: Table>(&self) -> Iter<'_, T::Key, T::Value> {
        let cf =
            self.rocksdb.cf_handle(T::NAME).unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_iter_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let bytes_scanned =
            self.db_metrics.op_metrics.rocksdb_iter_bytes.with_label_values(&[T::NAME]);
        let keys_scanned =
            self.db_metrics.op_metrics.rocksdb_iter_keys.with_label_values(&[T::NAME]);
        let _perf_ctx =
            if self.iter_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let db_iter = self.rocksdb.raw_iterator_cf(&cf, self.opts.readopts());
        Iter::new(
            T::NAME.to_string(),
            db_iter,
            Some(_timer),
            _perf_ctx,
            Some(bytes_scanned),
            Some(keys_scanned),
            Some(self.db_metrics.clone()),
        )
    }
}

impl Database for RocksDatabase {
    type TX<'txn> = RocksDbTxMut<'txn>;
    type TXMut<'txn> = RocksDbTxMut<'txn>;

    fn read_txn(&self) -> eyre::Result<Self::TX<'_>> {
        let txn = self.rocksdb.transaction()?;
        Ok(RocksDbTxMut { db: self.clone(), txn })
    }

    fn write_txn(&self) -> eyre::Result<Self::TXMut<'_>> {
        let txn = self.rocksdb.transaction()?;
        Ok(RocksDbTxMut { db: self.clone(), txn })
    }

    fn contains_key<T: Table>(&self, key: &T::Key) -> eyre::Result<bool> {
        let cf =
            self.rocksdb.cf_handle(T::NAME).unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let key_buf = be_fix_int_ser(key)?;
        // [`rocksdb::DBWithThreadMode::key_may_exist_cf`] can have false positives,
        // but no false negatives. We use it to short-circuit the absent case
        let readopts = self.opts.readopts();
        Ok(self.rocksdb.key_may_exist_cf(&cf, &key_buf, &readopts)
            && self.rocksdb.get_pinned_cf_opt(&cf, &key_buf, &readopts)?.is_some())
    }

    fn get<T: Table>(&self, key: &T::Key) -> eyre::Result<Option<T::Value>> {
        let cf =
            self.rocksdb.cf_handle(T::NAME).unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_get_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let perf_ctx =
            if self.get_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let key_buf = be_fix_int_ser(key)?;
        let res = self.rocksdb.get_pinned_cf_opt(&cf, &key_buf, &self.opts.readopts())?;
        self.db_metrics
            .op_metrics
            .rocksdb_get_bytes
            .with_label_values(&[T::NAME])
            .observe(res.as_ref().map_or(0.0, |v| v.len() as f64));
        if perf_ctx.is_some() {
            self.db_metrics.read_perf_ctx_metrics.report_metrics(T::NAME);
        }
        match res {
            Some(data) => Ok(Some(bcs::from_bytes(&data)?)),
            None => Ok(None),
        }
    }

    fn insert<T: Table>(&self, key: &T::Key, value: &T::Value) -> eyre::Result<()> {
        let cf =
            self.rocksdb.cf_handle(T::NAME).unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_put_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let perf_ctx =
            if self.write_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let key_buf = be_fix_int_ser(key)?;
        let value_buf = bcs::to_bytes(value)?;
        self.db_metrics
            .op_metrics
            .rocksdb_put_bytes
            .with_label_values(&[T::NAME])
            .observe((key_buf.len() + value_buf.len()) as f64);
        if perf_ctx.is_some() {
            self.db_metrics.write_perf_ctx_metrics.report_metrics(T::NAME);
        }
        self.rocksdb.put_cf(&cf, &key_buf, &value_buf, &self.opts.writeopts())?;
        Ok(())
    }

    fn remove<T: Table>(&self, key: &T::Key) -> eyre::Result<()> {
        let cf =
            self.rocksdb.cf_handle(T::NAME).unwrap_or_else(|| panic!("invalid table {}", T::NAME));
        let _timer = self
            .db_metrics
            .op_metrics
            .rocksdb_delete_latency_seconds
            .with_label_values(&[T::NAME])
            .start_timer();
        let perf_ctx =
            if self.write_sample_interval.sample() { Some(RocksDBPerfContext) } else { None };
        let key_buf = be_fix_int_ser(key)?;
        self.rocksdb.delete_cf(&cf, key_buf, &self.opts.writeopts())?;
        self.db_metrics.op_metrics.rocksdb_deletes.with_label_values(&[T::NAME]).inc();
        if perf_ctx.is_some() {
            self.db_metrics.write_perf_ctx_metrics.report_metrics(T::NAME);
        }
        Ok(())
    }

    fn clear_table<T: Table>(&self) -> eyre::Result<()> {
        let _ = self.rocksdb.drop_cf(T::NAME);
        self.rocksdb.create_cf(T::NAME, &default_db_options().options)?;
        Ok(())
    }

    fn is_empty<T: Table>(&self) -> bool {
        self.iter::<T>().next().is_none()
    }

    fn iter<T: Table>(&self) -> DBIter<'_, T> {
        Box::new(self.unbounded_iter_inner::<T>())
    }

    fn skip_to<T: Table>(&self, key: &T::Key) -> eyre::Result<DBIter<'_, T>> {
        Ok(Box::new(self.unbounded_iter_inner::<T>().skip_to(key)?))
    }

    fn reverse_iter<T: Table>(&self) -> DBIter<'_, T> {
        Box::new(self.unbounded_iter_inner::<T>().skip_to_last().reverse())
    }

    fn record_prior_to<T: Table>(&self, key: &T::Key) -> Option<(T::Key, T::Value)> {
        self.unbounded_iter_inner::<T>().skip_prior_to(key).map(|mut r| r.next()).unwrap_or(None)
    }

    fn last_record<T: Table>(&self) -> Option<(T::Key, T::Value)> {
        self.unbounded_iter_inner::<T>().skip_to_last().next()
    }
}
