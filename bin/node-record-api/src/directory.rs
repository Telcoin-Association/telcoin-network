//! Periodically refreshed record cache and read-only HTTP routes.

use crate::{
    cli::{format_key, parse_key},
    sources::{SourceStatus, Sources},
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use futures::{stream, StreamExt};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tn_kad_client::Client;
use tn_node_record::NodeRecord;
use tn_types::{now, BlsPublicKey};
use tokio::sync::RwLock;
use tokio_stream::wrappers::IntervalStream;

/// Immutable snapshots shared with HTTP handlers through a short-lived lock.
pub(crate) type Shared = Arc<RwLock<Snapshot>>;

/// Result of the latest lookup, separate from any retained verified record.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum LookupStatus {
    /// The key has not yet been queried.
    Pending,
    /// A verified copy was returned.
    Found,
    /// The DHT successfully returned no copies.
    Missing,
    /// The lookup failed; a previously verified copy may still be included.
    Failed {
        /// Human-readable reason for the failure.
        error: String,
    },
}

/// One tracked key and its newest cached verified record.
#[derive(Clone, Debug, Serialize)]
struct Entry {
    /// Canonical hex encoding of the raw DHT key.
    key: String,
    /// Newest verified copy retained for this key.
    record: Option<NodeRecord>,
    /// Time this cached copy was admitted, in Unix seconds.
    verified_at: Option<u64>,
    /// Outcome of the most recent lookup.
    lookup: LookupStatus,
}

impl Entry {
    /// Start tracking a key without claiming a record has been found.
    fn new(key: &BlsPublicKey) -> Self {
        Self {
            key: format_key(key),
            record: None,
            verified_at: None,
            lookup: LookupStatus::Pending,
        }
    }

    /// Update lookup health, preserving verified data on failures and preventing rollback.
    fn update(&mut self, result: Result<Option<NodeRecord>, tn_kad_client::Error>) {
        result
            .inspect_err(|error| self.lookup = LookupStatus::Failed { error: error.to_string() })
            .into_iter()
            .for_each(|record| {
                self.lookup =
                    if record.is_some() { LookupStatus::Found } else { LookupStatus::Missing };
                if record.is_none() {
                    self.record = None;
                    self.verified_at = None;
                }
                record
                    .filter(|incoming| {
                        self.record
                            .as_ref()
                            .is_none_or(|cached| incoming.info.timestamp > cached.info.timestamp)
                    })
                    .into_iter()
                    .for_each(|record| {
                        self.record = Some(record);
                        self.verified_at = Some(now());
                    });
            });
    }
}

/// A complete directory refresh served atomically to HTTP consumers.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct Snapshot {
    /// Chain used for signature verification and protocol negotiation.
    chain_id: u64,
    /// Primary or worker network used for all lookups.
    network: String,
    /// Completion time of the latest refresh cycle, in Unix seconds.
    refreshed_at: Option<u64>,
    /// Health of each reloadable key source.
    sources: Vec<SourceStatus>,
    /// Tracked records, ordered by raw BLS key.
    records: Vec<Entry>,
}

impl Snapshot {
    /// Initialize the directory before the first source refresh.
    pub(crate) fn new(chain_id: u64, network: String) -> Self {
        Self { chain_id, network, refreshed_at: None, sources: Vec::new(), records: Vec::new() }
    }
}

/// Read-only HTTP endpoints. Requests never cause DHT lookups or source fetches.
pub(crate) fn router(shared: Shared) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/v1/records", get(records))
        .route("/v1/records/{key}", get(record))
        .with_state(shared)
}

/// Report whether at least one refresh cycle has completed.
async fn health(State(shared): State<Shared>) -> StatusCode {
    if shared.read().await.refreshed_at.is_some() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Serve a complete cached directory and its source health.
async fn records(State(shared): State<Shared>) -> Json<Snapshot> {
    Json(shared.read().await.clone())
}

/// Serve one tracked key, including its lookup status even when no record was found.
async fn record(
    Path(key): Path<String>,
    State(shared): State<Shared>,
) -> Result<Json<Entry>, StatusCode> {
    let key = parse_key(&key).map_err(|_| StatusCode::BAD_REQUEST)?;
    let canonical = format_key(&key);
    shared
        .read()
        .await
        .records
        .iter()
        .find(|entry| entry.key == canonical)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

/// Refresh independently of HTTP requests, retaining each key's newest verified copy.
pub(crate) async fn refresh(
    shared: Shared,
    sources: Sources,
    client: Client,
    refresh_interval: Duration,
) {
    let mut interval = tokio::time::interval(refresh_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    IntervalStream::new(interval)
        .fold(
            (client, sources, BTreeMap::<BlsPublicKey, Entry>::new()),
            |(client, mut sources, mut cache), _tick| {
                let shared = shared.clone();
                async move {
                    let (keys, statuses) = sources.refresh().await;
                    cache.retain(|key, _entry| keys.contains(key));
                    let (client, cache) = stream::iter(keys)
                        .fold((client, cache), |(mut client, mut cache), key| async move {
                            let result = client.lookup(&key).await;
                            cache.entry(key).or_insert_with(|| Entry::new(&key)).update(result);
                            (client, cache)
                        })
                        .await;
                    {
                        let mut snapshot = shared.write().await;
                        snapshot.refreshed_at = Some(now());
                        snapshot.sources = statuses;
                        snapshot.records = cache.values().cloned().collect();
                    }
                    (client, sources, cache)
                }
            },
        )
        .await;
}

#[cfg(test)]
mod tests;
