//! Cache freshness and HTTP behavior tests without network-triggered lookups.

use super::*;
use crate::error::Error;
use axum::{
    body::{to_bytes, Body},
    http::Request,
};
use libp2p::{identity::Keypair, multiaddr::Protocol, Multiaddr};
use rand::{rngs::StdRng, SeedableRng};
use serde_json::Value;
use std::net::Ipv4Addr;
use tn_node_record::{NetworkType, RecordDomain};
use tn_types::{BlsKeypair, Signer};
use tower::ServiceExt;

/// Reproducible key used for cache entries and HTTP paths.
fn key(seed: u64) -> BlsKeypair {
    BlsKeypair::generate(&mut StdRng::seed_from_u64(seed))
}

/// Synthetic already-verified cache input; signature validation belongs to the DHT client tests.
fn record_with_timestamp(timestamp: u64) -> NodeRecord {
    let signer = key(1);
    let address = Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::LOCALHOST))
        .with(Protocol::Udp(9000))
        .with(Protocol::QuicV1);
    let mut record = NodeRecord::build(
        RecordDomain::new(2017, NetworkType::Primary),
        Keypair::generate_ed25519().public().into(),
        address,
        None,
        |bytes| signer.sign(bytes),
    );
    record.info.timestamp = timestamp;
    record
}

/// Lookup errors preserve verified data, older copies cannot roll it back, and clean misses clear
/// it.
#[test]
fn cache_retains_newest_on_error_and_clears_on_clean_miss() {
    let mut entry = Entry::new(key(1).public());
    entry.update(Ok(Some(record_with_timestamp(20))));
    entry.update(Ok(Some(record_with_timestamp(10))));
    assert_eq!(entry.record.as_ref().map(|record| record.info.timestamp), Some(20));
    entry.update(Err(tn_kad_client::Error::Timeout));
    assert!(matches!(entry.lookup, LookupStatus::Failed { .. }));
    assert_eq!(entry.record.as_ref().map(|record| record.info.timestamp), Some(20));
    entry.update(Ok(None));
    assert!(entry.record.is_none());
    assert!(entry.verified_at.is_none());
    assert!(matches!(entry.lookup, LookupStatus::Missing));
}

/// Execute an HTTP request against the in-memory router.
async fn request(app: Router, method: &str, path: &str) -> Result<axum::response::Response, Error> {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .body(Body::empty())
        .map_err(|error| Error::Rpc(error.to_string()))?;
    app.oneshot(request).await.map_err(|error| match error {})
}

/// Endpoints expose snapshots and input errors; write methods have no route.
#[tokio::test]
async fn http_routes_are_read_only_and_serve_cached_records() -> Result<(), Error> {
    let shared = Arc::new(RwLock::new(Snapshot::new(2017, "primary".into())));
    let app = router(shared.clone());
    assert_eq!(
        request(app.clone(), "GET", "/healthz").await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let signer = key(2);
    let mut entry = Entry::new(signer.public());
    entry.update(Ok(None));
    {
        let mut snapshot = shared.write().await;
        snapshot.refreshed_at = Some(1);
        snapshot.records.push(entry);
    }
    assert_eq!(request(app.clone(), "GET", "/healthz").await?.status(), StatusCode::OK);
    let response = request(app.clone(), "GET", "/v1/records").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 8192)
        .await
        .map_err(|error| Error::Rpc(error.to_string()))?;
    let body: Value = serde_json::from_slice(&bytes).map_err(Error::Json)?;
    assert_eq!(body.get("chain_id"), Some(&Value::from(2017)));
    assert_eq!(body.pointer("/records/0/key"), Some(&Value::from(format_key(signer.public()))));
    let path = format!("/v1/records/{}", format_key(signer.public()));
    assert_eq!(request(app.clone(), "GET", &path).await?.status(), StatusCode::OK);
    assert_eq!(
        request(app.clone(), "POST", "/v1/records").await?.status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        request(app.clone(), "GET", "/v1/records/invalid").await?.status(),
        StatusCode::BAD_REQUEST
    );
    let missing = format!("/v1/records/{}", format_key(key(3).public()));
    assert_eq!(request(app, "GET", &missing).await?.status(), StatusCode::NOT_FOUND);
    Ok(())
}
