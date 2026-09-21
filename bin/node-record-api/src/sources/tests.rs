//! Source failures retain prior sets independently and never erase the static floor.

use super::*;
use crate::cli::format_key;
use axum::{extract::State, routing::post, Json, Router};
use clap::Parser;
use rand::{rngs::StdRng, SeedableRng};
use std::sync::Arc;
use tn_types::BlsKeypair;
use tokio::{net::TcpListener, sync::RwLock};

/// Deterministic validator key for a source fixture.
fn key(seed: u64) -> BlsPublicKey {
    *BlsKeypair::generate(&mut StdRng::seed_from_u64(seed)).public()
}

/// Parse source options without creating a DHT client or making a network request.
fn args(options: Vec<String>) -> Result<Args, Error> {
    Args::try_parse_from(
        ["node-record-api", "--chain-id", "2017", "--bootstrap", "/ip4/127.0.0.1/udp/9000/quic-v1"]
            .into_iter()
            .map(str::to_owned)
            .chain(options),
    )
    .map_err(|error| Error::Rpc(error.to_string()))
}

/// Malformed files retain their last good set; a later successful empty file removes only its keys.
#[tokio::test]
async fn failed_file_retains_keys_and_static_floor() -> Result<(), Error> {
    let directory = tempfile::tempdir().map_err(Error::Io)?;
    let path = directory.path().join("keys.json");
    let floor = key(1);
    let file_key = key(2);
    tokio::fs::write(&path, json!([format_key(&file_key)]).to_string()).await.map_err(Error::Io)?;
    let args = args(vec![
        "--key".into(),
        format_key(&floor),
        "--keys-file".into(),
        path.display().to_string(),
    ])?;
    let mut sources = Sources::new(&args)?;
    let (keys, statuses) = sources.refresh().await;
    assert_eq!(keys, Keys::from([floor, file_key]));
    assert!(statuses.iter().all(|status| status.error.is_none()));
    tokio::fs::write(&path, b"not valid JSON").await.map_err(Error::Io)?;
    let (keys, statuses) = sources.refresh().await;
    assert_eq!(keys, Keys::from([floor, file_key]));
    assert!(statuses.iter().all(|status| status.error.is_some() && status.last_success.is_some()));
    tokio::fs::write(&path, b"[]").await.map_err(Error::Io)?;
    let (keys, statuses) = sources.refresh().await;
    assert_eq!(keys, Keys::from([floor]));
    assert!(statuses.iter().all(|status| status.error.is_none()));
    Ok(())
}

/// Controllable read-only JSON-RPC response source.
enum Mode {
    /// Serve a committee on the expected chain.
    Healthy(Vec<String>),
    /// Return a JSON-RPC failure instead of a result.
    Failed,
    /// Identify a different chain.
    WrongChain,
}

/// Local RPC fixture with an automatically stopped HTTP task.
struct RpcServer {
    /// HTTP URL supplied to the source reader.
    url: String,
    /// Response mode changed synchronously between source refreshes.
    mode: Arc<RwLock<Mode>>,
    /// Server task kept alive until the fixture is dropped.
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for RpcServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Answer the three read-only calls required to discover a live committee.
async fn respond(State(mode): State<Arc<RwLock<Mode>>>, Json(request): Json<Value>) -> Json<Value> {
    let method = request.get("method").and_then(Value::as_str).unwrap_or_default();
    let response = match &*mode.read().await {
        Mode::Healthy(keys) => {
            let result = match method {
                "eth_chainId" => json!("0x7e1"),
                "tn_getCurrentEpoch" => json!(12),
                "tn_getCommitteeBlsPubkeys" => {
                    assert_eq!(request.get("params"), Some(&json!([12])));
                    json!(keys)
                }
                _ => Value::Null,
            };
            json!({"jsonrpc":"2.0", "id":1, "result":result})
        }
        Mode::Failed => {
            json!({"jsonrpc":"2.0", "id":1, "error":{"code":-32000,"message":"unavailable"}})
        }
        Mode::WrongChain => json!({"jsonrpc":"2.0", "id":1, "result":"0x1"}),
    };
    Json(response)
}

/// Bind a real local HTTP listener on an OS-assigned port.
async fn rpc_server(keys: Vec<BlsPublicKey>) -> Result<RpcServer, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(Error::Io)?;
    let url = format!("http://{}", listener.local_addr().map_err(Error::Io)?);
    let mode = Arc::new(RwLock::new(Mode::Healthy(keys.iter().map(format_key).collect())));
    let app = Router::new().route("/", post(respond)).with_state(mode.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    Ok(RpcServer { url, mode, task })
}

/// Independent live sources retain their own keys on RPC, chain and decoding failures.
#[tokio::test]
async fn rpc_failures_preserve_only_the_failed_sources_last_good_set() -> Result<(), Error> {
    let floor = key(3);
    let first_key = key(4);
    let second_key = key(5);
    let replacement = key(6);
    let first = rpc_server(vec![first_key]).await?;
    let second = rpc_server(vec![second_key]).await?;
    let args = args(vec![
        "--key".into(),
        format_key(&floor),
        "--rpc-url".into(),
        first.url.clone(),
        "--rpc-url".into(),
        second.url.clone(),
    ])?;
    let mut sources = Sources::new(&args)?;
    assert_eq!(sources.refresh().await.0, Keys::from([floor, first_key, second_key]));
    *first.mode.write().await = Mode::Healthy(vec![format_key(&replacement)]);
    *second.mode.write().await = Mode::Failed;
    let (keys, statuses) = sources.refresh().await;
    assert_eq!(keys, Keys::from([floor, replacement, second_key]));
    assert_eq!(statuses.iter().filter(|status| status.error.is_some()).count(), 1);
    *second.mode.write().await = Mode::WrongChain;
    assert_eq!(sources.refresh().await.0, Keys::from([floor, replacement, second_key]));
    *second.mode.write().await = Mode::Healthy(vec!["0x01".into()]);
    assert_eq!(sources.refresh().await.0, Keys::from([floor, replacement, second_key]));
    *second.mode.write().await = Mode::Healthy(Vec::new());
    assert_eq!(sources.refresh().await.0, Keys::from([floor, replacement]));
    Ok(())
}
