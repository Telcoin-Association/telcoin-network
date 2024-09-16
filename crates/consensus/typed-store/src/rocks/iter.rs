// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{marker::PhantomData, sync::Arc};

use prometheus::{Histogram, HistogramTimer};
use rocksdb::Direction;
use tn_types::{decode, encode_key, try_decode_key};

use super::{
    errors::TypedStoreError,
    metrics::{DBMetrics, RocksDBPerfContext},
    RocksDBRawIter,
};
use serde::{de::DeserializeOwned, Serialize};

/// An iterator over all key-value pairs in a data map.
pub struct Iter<'a, K, V> {
    cf_name: String,
    db_iter: RocksDBRawIter<'a>,
    // *const here is an equivalent to `impl !Send for Iter` (which is not a stable feature at the
    // moment)
    _phantom: PhantomData<*const (K, V)>,
    direction: Direction,
    is_initialized: bool,
    _timer: Option<HistogramTimer>,
    _perf_ctx: Option<RocksDBPerfContext>,
    bytes_scanned: Option<Histogram>,
    keys_scanned: Option<Histogram>,
    db_metrics: Option<Arc<DBMetrics>>,
    bytes_scanned_counter: usize,
    keys_returned_counter: usize,
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> Iter<'a, K, V> {
    pub(super) fn new(
        cf_name: String,
        db_iter: RocksDBRawIter<'a>,
        _timer: Option<HistogramTimer>,
        _perf_ctx: Option<RocksDBPerfContext>,
        bytes_scanned: Option<Histogram>,
        keys_scanned: Option<Histogram>,
        db_metrics: Option<Arc<DBMetrics>>,
    ) -> Self {
        Self {
            cf_name,
            db_iter,
            _phantom: PhantomData,
            direction: Direction::Forward,
            is_initialized: false,
            _timer,
            _perf_ctx,
            bytes_scanned,
            keys_scanned,
            db_metrics,
            bytes_scanned_counter: 0,
            keys_returned_counter: 0,
        }
    }
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> Iterator for Iter<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        // implicitly set iterator to the first entry in the column family if it hasn't been
        // initialized used for backward compatibility
        if !self.is_initialized {
            self.db_iter.seek_to_first();
            self.is_initialized = true;
        }
        if self.db_iter.valid() {
            let raw_key = self.db_iter.key().expect("Valid iterator failed to get key");
            let raw_value = self.db_iter.value().expect("Valid iterator failed to get value");
            self.bytes_scanned_counter += raw_key.len() + raw_value.len();
            self.keys_returned_counter += 1;
            let key = try_decode_key(raw_key).ok();
            let value = Some(decode(raw_value));
            match self.direction {
                Direction::Forward => self.db_iter.next(),
                Direction::Reverse => self.db_iter.prev(),
            }
            key.and_then(|k| value.map(|v| (k, v)))
        } else {
            None
        }
    }
}

impl<'a, K, V> Drop for Iter<'a, K, V> {
    fn drop(&mut self) {
        if let Some(bytes_scanned) = self.bytes_scanned.take() {
            bytes_scanned.observe(self.bytes_scanned_counter as f64);
        }
        if let Some(keys_scanned) = self.keys_scanned.take() {
            keys_scanned.observe(self.keys_returned_counter as f64);
        }
        if let Some(db_metrics) = self.db_metrics.take() {
            db_metrics.read_perf_ctx_metrics.report_metrics(&self.cf_name);
        }
    }
}

impl<'a, K: Serialize, V> Iter<'a, K, V> {
    /// Skips all the elements that are smaller than the given key,
    /// and either lands on the key or the first one greater than
    /// the key.
    pub fn skip_to(mut self, key: &K) -> Result<Self, TypedStoreError> {
        self.is_initialized = true;
        self.db_iter.seek(encode_key(key));
        Ok(self)
    }

    /// Moves the iterator the element given or
    /// the one prior to it if it does not exist. If there is
    /// no element prior to it, it returns an empty iterator.
    pub fn skip_prior_to(mut self, key: &K) -> Result<Self, TypedStoreError> {
        self.is_initialized = true;
        self.db_iter.seek_for_prev(encode_key(key));
        Ok(self)
    }

    /// Seeks to the last key in the database (at this column family).
    pub fn skip_to_last(mut self) -> Self {
        self.is_initialized = true;
        self.db_iter.seek_to_last();
        self
    }

    /// Seeks to the first key in the database (at this column family).
    pub fn _seek_to_first(mut self) -> Self {
        self.is_initialized = true;
        self.db_iter.seek_to_first();
        self
    }

    /// Will make the direction of the iteration reverse and will
    /// create a new `RevIter` to consume. Every call to `next` method
    /// will give the next element from the end.
    pub fn reverse(mut self) -> RevIter<'a, K, V> {
        self.direction = Direction::Reverse;
        RevIter::new(self)
    }
}

/// An iterator with a reverted direction to the original. The `RevIter`
/// is hosting an iteration which is consuming in the opposing direction.
/// It's not possible to do further manipulation (ex re-reverse) to the
/// iterator.
pub struct RevIter<'a, K, V> {
    iter: Iter<'a, K, V>,
}

impl<'a, K, V> RevIter<'a, K, V> {
    fn new(iter: Iter<'a, K, V>) -> Self {
        Self { iter }
    }
}

impl<'a, K: DeserializeOwned, V: DeserializeOwned> Iterator for RevIter<'a, K, V> {
    type Item = (K, V);

    /// Will give the next item backwards
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}
