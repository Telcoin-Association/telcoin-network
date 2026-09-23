//! The HTTP routes and their JSON shapes.
//!
//! | Route | Behaviour |
//! |---|---|
//! | `GET /healthz`, `GET /health` | `200 {"status":"ok"}` while the process lives |
//! | `GET /readyz`, `GET /ready` | `200` once the first cycle is done and a fresh record is cached, else `503` with a reason |
//! | `GET /v1/records` | envelope plus every cached record |
//! | `GET /v1/records/{key}` | one record by base58 or `0x`-hex BLS key; `400` on a bad key, `404` when unknown |
//! | `GET /v1/rpcs` | envelope plus only the records that advertise an RPC endpoint (the site payload) |
//!
//! Every `/v1/*` success carries `Cache-Control: public, max-age=30`: the data changes on a
//! minutes-scale cadence, so a short shared cache absorbs a page-load storm without going stale.
//! `HEAD` is answered for every `GET`; `OPTIONS` is answered by the CORS layer in
//! [`crate::server`].

use std::sync::{Arc, PoisonError};

use axum::{
    extract::{Path, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{from_fn, map_response},
    response::Response,
    routing::get,
    Json, Router,
};
use serde::Serialize;
use tn_kad_client::{NetworkType, PeerId};
use tn_node_record::parse_bls_pubkey;
use tn_types::now;

use crate::{
    cache::RecordView,
    error::ApiError,
    readiness::{self, Ready},
    refresh::SharedCache,
    telemetry::track_http,
};

/// Liveness probe path.
pub const HEALTH_PATH: &str = "/healthz";

/// Liveness probe alias (the worker gateway's spelling).
pub const HEALTH_ALIAS: &str = "/health";

/// Readiness probe path.
pub const READY_PATH: &str = "/readyz";

/// Readiness probe alias (the worker gateway's spelling).
pub const READY_ALIAS: &str = "/ready";

/// The `Cache-Control` value on `/v1/*` successes.
const V1_CACHE_CONTROL: &str = "public, max-age=30";

/// The `verification` value every served record carries: the record's BLS signature was checked
/// against the key it is published under, for this chain and worker network.
const VERIFICATION: &str = "bls_self_signed";

/// Whether `path` is an orchestration probe, which the rate limiter exempts.
pub fn is_probe_path(path: &str) -> bool {
    matches!(path, HEALTH_PATH | HEALTH_ALIAS | READY_PATH | READY_ALIAS)
}

/// Shared state handed to every handler.
#[derive(Clone, Debug)]
pub struct ApiState {
    /// The cache the refresh loop fills.
    pub cache: SharedCache,
    /// The chain whose DHT is read; echoed in every envelope.
    pub chain_id: u64,
    /// The network label (`worker-0`), echoed in every envelope and record.
    pub network: Arc<str>,
}

impl ApiState {
    /// Build the state for `network_type`'s label.
    pub fn new(cache: SharedCache, chain_id: u64, network_type: NetworkType) -> Self {
        Self { cache, chain_id, network: network_label(network_type).into() }
    }
}

/// The `network` label for a DHT: `primary` or `worker-<id>`.
pub fn network_label(network_type: NetworkType) -> String {
    match network_type {
        NetworkType::Primary => "primary".to_string(),
        NetworkType::Worker(id) => format!("worker-{id}"),
    }
}

/// The `/healthz` body.
#[derive(Debug, Serialize)]
struct StatusBody {
    /// Always `ok`.
    status: &'static str,
}

/// The `/readyz` success body.
#[derive(Debug, Serialize)]
struct ReadyBody {
    /// Always `ready`.
    status: &'static str,
    /// Refresh cycles completed so far.
    cycles_completed: u64,
    /// Records in the cache.
    records_cached: usize,
}

impl From<Ready> for ReadyBody {
    fn from(ready: Ready) -> Self {
        Self {
            status: "ready",
            cycles_completed: ready.cycles_completed,
            records_cached: ready.records_cached,
        }
    }
}

/// The envelope around a record list.
#[derive(Debug, Serialize)]
pub struct Envelope {
    /// The chain whose DHT the records came from.
    pub chain_id: u64,
    /// The DHT's network label (`worker-0`).
    pub network: String,
    /// Unix time this response was generated.
    pub generated_at: u64,
    /// Unix time of the last refresh cycle that fetched at least one record, or `null`.
    pub last_refresh_at: Option<u64>,
    /// How many keys the daemon is tracking.
    pub keys_tracked: usize,
    /// The records.
    pub records: Vec<RecordJson>,
}

/// A BLS public key in both textual encodings.
#[derive(Debug, Serialize)]
pub struct BlsKeyJson {
    /// The canonical base58 form.
    pub base58: String,
    /// The `0x`-prefixed hex of the 96 compressed bytes (the form contracts and `tn_*` RPC use).
    pub hex: String,
}

/// An advertised RPC endpoint pair.
#[derive(Debug, Serialize)]
pub struct RpcJson {
    /// The HTTP(S) JSON-RPC URL.
    pub http: String,
    /// The WebSocket JSON-RPC URL, if advertised.
    pub ws: Option<String>,
}

/// One served record.
#[derive(Debug, Serialize)]
pub struct RecordJson {
    /// The key the record is published under and signed by.
    pub bls_pubkey: BlsKeyJson,
    /// The validator's libp2p network public key (base58 protobuf).
    pub network_pubkey: String,
    /// The libp2p peer id derived from `network_pubkey`.
    pub peer_id: String,
    /// The advertised DHT multiaddrs.
    pub multiaddrs: Vec<String>,
    /// The advertised RPC endpoints, or `null` when the validator does not expose RPC.
    pub rpc: Option<RpcJson>,
    /// The record's signed publication time (unix seconds).
    pub record_timestamp: u64,
    /// Seconds since `record_timestamp`.
    pub record_age_seconds: u64,
    /// Unix time the daemon last fetched this record.
    pub fetched_at: u64,
    /// Seconds since `fetched_at`.
    pub fetch_age_seconds: u64,
    /// Whether `fetched_at` is older than the staleness threshold.
    pub stale: bool,
    /// How many valid copies the last fetch returned.
    pub copies_seen: usize,
    /// How the record was authenticated (always `bls_self_signed`).
    pub verification: &'static str,
    /// The DHT's network label (`worker-0`).
    pub network: String,
}

impl RecordJson {
    /// Render a cache view at `now` under `network`.
    fn from_view(view: &RecordView, now: u64, network: &str) -> Self {
        let info = &view.cached.record.info;
        Self {
            bls_pubkey: BlsKeyJson {
                base58: view.key.to_string(),
                hex: format!("0x{}", hex::encode(view.key.as_ref())),
            },
            network_pubkey: info.pubkey.to_string(),
            peer_id: PeerId::from(info.pubkey.clone()).to_string(),
            multiaddrs: info.multiaddrs.iter().map(ToString::to_string).collect(),
            rpc: info.rpc.as_ref().map(|rpc| RpcJson {
                http: rpc.http.to_string(),
                ws: rpc.ws.as_ref().map(ToString::to_string),
            }),
            record_timestamp: info.timestamp,
            record_age_seconds: now.saturating_sub(info.timestamp),
            fetched_at: view.cached.fetched_at,
            fetch_age_seconds: now.saturating_sub(view.cached.fetched_at),
            stale: view.stale,
            copies_seen: view.cached.copies_seen,
            verification: VERIFICATION,
            network: network.to_string(),
        }
    }
}

/// Build the routes (without the edge layers, which [`crate::server`] adds).
pub fn routes(state: ApiState) -> Router {
    let v1 = Router::new()
        .route("/v1/records", get(list_records))
        .route("/v1/records/{key}", get(get_record))
        .route("/v1/rpcs", get(list_rpcs))
        .route_layer(map_response(cache_control));
    Router::new()
        .route(HEALTH_PATH, get(liveness))
        .route(HEALTH_ALIAS, get(liveness))
        .route(READY_PATH, get(readiness_probe))
        .route(READY_ALIAS, get(readiness_probe))
        .merge(v1)
        .route_layer(from_fn(track_http))
        .fallback(no_route)
        .with_state(state)
}

/// Add the shared-cache header to a `/v1/*` success. Errors are left uncached so a key that
/// appears on the next cycle is not masked by a cached `404`.
async fn cache_control(mut response: Response) -> Response {
    if response.status() == StatusCode::OK {
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static(V1_CACHE_CONTROL));
    }
    response
}

/// Liveness: always `200` while the process runs.
async fn liveness() -> Json<StatusBody> {
    Json(StatusBody { status: "ok" })
}

/// Readiness: `200` per [`crate::readiness`], else `503` with the reason.
async fn readiness_probe(State(state): State<ApiState>) -> Result<Json<ReadyBody>, ApiError> {
    let cache = state.cache.read().unwrap_or_else(PoisonError::into_inner);
    readiness::check(&cache, now())
        .map(|ready| Json(ReadyBody::from(ready)))
        .map_err(|reason| ApiError::NotReady(reason.to_string()))
}

/// Every cached record.
async fn list_records(State(state): State<ApiState>) -> Json<Envelope> {
    Json(envelope(&state, |_| true))
}

/// Only the records that advertise RPC.
async fn list_rpcs(State(state): State<ApiState>) -> Json<Envelope> {
    Json(envelope(&state, |view| view.cached.record.info.rpc.is_some()))
}

/// One record by key.
async fn get_record(
    State(state): State<ApiState>,
    Path(key): Path<String>,
) -> Result<Json<RecordJson>, ApiError> {
    let key = parse_bls_pubkey(&key).map_err(|err| ApiError::InvalidKey(err.to_string()))?;
    let now = now();
    let cache = state.cache.read().unwrap_or_else(PoisonError::into_inner);
    cache
        .get(&key, now)
        .map(|view| Json(RecordJson::from_view(&view, now, &state.network)))
        .ok_or(ApiError::RecordNotFound)
}

/// Unmatched paths answer JSON, like every other error.
async fn no_route() -> ApiError {
    ApiError::NoRoute
}

/// Snapshot the cache into an envelope, keeping the views `keep` accepts.
fn envelope(state: &ApiState, keep: impl Fn(&RecordView) -> bool) -> Envelope {
    let now = now();
    let cache = state.cache.read().unwrap_or_else(PoisonError::into_inner);
    Envelope {
        chain_id: state.chain_id,
        network: state.network.to_string(),
        generated_at: now,
        last_refresh_at: cache.last_refresh_at(),
        keys_tracked: cache.keys_tracked(),
        records: cache
            .snapshot(now)
            .iter()
            .filter(|view| keep(view))
            .map(|view| RecordJson::from_view(view, now, &state.network))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{test_support::*, RecordCache};
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request},
    };
    use std::{collections::BTreeSet, sync::RwLock};
    use tower::ServiceExt as _;

    fn state() -> ApiState {
        ApiState::new(
            Arc::new(RwLock::new(RecordCache::new(config()))),
            2017,
            NetworkType::Worker(0),
        )
    }

    async fn call(
        app: &Router,
        method: Method,
        path: &str,
    ) -> (StatusCode, Option<HeaderValue>, serde_json::Value) {
        let request =
            Request::builder().method(method).uri(path).body(Body::empty()).expect("request");
        let response = app.clone().oneshot(request).await.expect("response");
        let status = response.status();
        let cache_control = response.headers().get(header::CACHE_CONTROL).cloned();
        let bytes = to_bytes(response.into_body(), 1 << 20).await.expect("body");
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("json body")
        };
        (status, cache_control, json)
    }

    async fn get(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
        let (status, _, json) = call(app, Method::GET, path).await;
        (status, json)
    }

    #[tokio::test]
    async fn health_is_always_ok() {
        let app = routes(state());
        for path in [HEALTH_PATH, HEALTH_ALIAS] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(body, serde_json::json!({ "status": "ok" }));
        }
    }

    #[tokio::test]
    async fn readiness_flips_after_a_fresh_record() {
        let state = state();
        let app = routes(state.clone());
        let (status, body) = get(&app, READY_PATH).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "not_ready");
        assert!(body["message"].as_str().expect("reason").contains("first refresh cycle"));

        let a = key(KEY_A);
        let set: BTreeSet<_> = [a].into_iter().collect();
        let now = now();
        state.cache.write().expect("lock").apply_cycle(
            now,
            1,
            &set,
            vec![(a, Ok(Some(verified(a, now, Some(rpc())))))],
        );
        for path in [READY_PATH, READY_ALIAS] {
            let (status, body) = get(&app, path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(body["status"], "ready");
            assert_eq!(body["records_cached"], 1);
        }
    }

    #[tokio::test]
    async fn record_by_key_is_400_404_or_200() {
        let state = state();
        let app = routes(state.clone());
        let a = key(KEY_A);

        let (status, body) = get(&app, "/v1/records/0xzz").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "invalid_key");
        assert!(body["message"].as_str().expect("message").contains("hex"));

        let (status, body) = get(&app, &format!("/v1/records/{a}")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, serde_json::json!({ "error": "not_found" }));

        let now = now();
        let set: BTreeSet<_> = [a].into_iter().collect();
        state.cache.write().expect("lock").apply_cycle(
            now,
            1,
            &set,
            vec![(a, Ok(Some(verified(a, now - 5, Some(rpc())))))],
        );
        // base58 and 0x-hex both address the record; the success is cacheable
        let hex = format!("0x{}", hex::encode(a.as_ref()));
        for path in [format!("/v1/records/{a}"), format!("/v1/records/{hex}")] {
            let (status, cache_control, body) = call(&app, Method::GET, &path).await;
            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(cache_control.expect("cache-control"), V1_CACHE_CONTROL);
            assert_eq!(body["bls_pubkey"]["base58"], a.to_string());
            assert_eq!(body["bls_pubkey"]["hex"], hex);
            assert_eq!(body["rpc"]["http"], "https://validator.example:8545/");
            assert_eq!(body["rpc"]["ws"], "wss://validator.example:8546/");
            assert_eq!(body["verification"], "bls_self_signed");
            assert_eq!(body["network"], "worker-0");
            assert_eq!(body["stale"], false);
            assert_eq!(body["copies_seen"], 1);
            assert!(body["peer_id"].as_str().expect("peer id").starts_with("12D3Koo"));
            assert!(body["record_age_seconds"].as_u64().expect("age") >= 5);
        }
        // errors are not cacheable
        let (_, cache_control, _) = call(&app, Method::GET, "/v1/records/0xzz").await;
        assert!(cache_control.is_none());
    }

    #[tokio::test]
    async fn rpcs_projection_filters_records_without_rpc() {
        let state = state();
        let app = routes(state.clone());
        let a = key(KEY_A);
        let b = key(KEY_B);
        let now = now();
        let set: BTreeSet<_> = [a, b].into_iter().collect();
        state.cache.write().expect("lock").apply_cycle(
            now,
            1,
            &set,
            vec![
                (a, Ok(Some(verified(a, now, Some(rpc()))))),
                (b, Ok(Some(verified(b, now, None)))),
            ],
        );

        let (status, body) = get(&app, "/v1/records").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["chain_id"], 2017);
        assert_eq!(body["network"], "worker-0");
        assert_eq!(body["keys_tracked"], 2);
        assert_eq!(body["last_refresh_at"], now);
        assert_eq!(body["records"].as_array().expect("records").len(), 2);

        let (status, cache_control, body) = call(&app, Method::GET, "/v1/rpcs").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cache_control.expect("cache-control"), V1_CACHE_CONTROL);
        let records = body["records"].as_array().expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["bls_pubkey"]["base58"], a.to_string());
        assert!(records[0]["rpc"].is_object());

        // HEAD is answered for every GET, with no body
        let (status, cache_control, body) = call(&app, Method::HEAD, "/v1/rpcs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(cache_control.is_some());
        assert!(body.is_null());
    }

    #[tokio::test]
    async fn unmatched_paths_answer_json_404() {
        let app = routes(state());
        let (status, body) = get(&app, "/nope").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "no_route");
    }
}
