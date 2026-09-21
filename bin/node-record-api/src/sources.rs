//! Union independent key sources, retaining each source's last successful set on failure.

use crate::{
    cli::{parse_key, Args},
    error::Error,
};
use futures::{stream, StreamExt};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::PathBuf};
use tn_types::{now, BlsPublicKey, Committee, Epoch};

/// The keys tracked by one successful source snapshot.
type Keys = BTreeSet<BlsPublicKey>;

/// Source health included alongside cached records in the HTTP response.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct SourceStatus {
    /// Configuration slot, without exposing RPC credentials.
    source: String,
    /// Number of keys retained from this source's last successful response.
    key_count: usize,
    /// Most recent successful refresh, in Unix seconds.
    last_success: Option<u64>,
    /// Most recent failure; last-good keys are retained while this is present.
    error: Option<String>,
}

/// One independent source of validator keys.
#[derive(Debug)]
enum Input {
    /// JSON array of hex public keys.
    KeysFile(PathBuf),
    /// Standard serialized committee.
    CommitteeFile(PathBuf),
    /// Current committee discovered via plain JSON-RPC.
    Rpc(Url),
}

/// An input and its independently retained last-good result.
#[derive(Debug)]
struct Source {
    /// Where to obtain the next set.
    input: Input,
    /// Most recent complete, successfully decoded set.
    last_good: Keys,
    /// Health metadata for operators.
    status: SourceStatus,
}

impl Source {
    /// Initialize a source without claiming any successful observation.
    fn new(input: Input, label: String) -> Self {
        Self {
            input,
            last_good: Keys::new(),
            status: SourceStatus { source: label, key_count: 0, last_success: None, error: None },
        }
    }

    /// Apply a complete source result; failures do not remove any retained keys.
    fn update(&mut self, result: Result<Keys, Error>) {
        let result = result.inspect_err(|error| self.status.error = Some(error.to_string()));
        result.into_iter().for_each(|keys| {
            self.status.key_count = keys.len();
            self.status.last_success = Some(now());
            self.status.error = None;
            self.last_good = keys;
        });
    }
}

/// Static floor plus independently refreshed file and live-committee sources.
#[derive(Debug)]
pub(crate) struct Sources {
    /// Explicit CLI keys, including non-committee validators.
    floor: Keys,
    /// Reloadable sources, each with its own last-good set.
    sources: Vec<Source>,
    /// HTTP client with a bounded request timeout.
    http: reqwest::Client,
    /// Chain every RPC source must identify before its committee is accepted.
    chain_id: u64,
}

impl Sources {
    /// Build the configured source union without fetching any live data.
    pub(crate) fn new(args: &Args) -> Result<Self, Error> {
        let files = args
            .keys_file()
            .cloned()
            .map(Input::KeysFile)
            .into_iter()
            .chain(args.committee_file().cloned().map(Input::CommitteeFile));
        let sources = files
            .chain(args.rpc_urls().iter().cloned().map(Input::Rpc))
            .enumerate()
            .map(|(index, input)| {
                let kind = match &input {
                    Input::KeysFile(_) => "keys-file",
                    Input::CommitteeFile(_) => "committee-file",
                    Input::Rpc(_) => "rpc",
                };
                Source::new(input, format!("{kind}:{index}"))
            })
            .collect();
        reqwest::Client::builder().timeout(args.timeout()).build().map_err(Error::Http).map(
            |http| Self {
                floor: args.keys().iter().copied().collect(),
                sources,
                http,
                chain_id: args.chain_id(),
            },
        )
    }

    /// Refresh each source and union its current or retained keys with the static floor.
    pub(crate) async fn refresh(&mut self) -> (Keys, Vec<SourceStatus>) {
        let http = &self.http;
        let chain_id = self.chain_id;
        stream::iter(self.sources.iter_mut())
            .for_each(|source| async move {
                source.update(read_keys(&source.input, http, chain_id).await);
            })
            .await;
        let keys = self
            .floor
            .iter()
            .copied()
            .chain(self.sources.iter().flat_map(|source| source.last_good.iter().copied()))
            .collect();
        (keys, self.sources.iter().map(|source| source.status.clone()).collect())
    }
}

/// Fetch and completely decode one source before changing its retained key set.
async fn read_keys(input: &Input, http: &reqwest::Client, chain_id: u64) -> Result<Keys, Error> {
    match input {
        Input::KeysFile(path) => {
            let bytes = tokio::fs::read(path).await.map_err(Error::Io)?;
            let keys: Vec<String> = serde_json::from_slice(&bytes).map_err(Error::Json)?;
            keys.iter().map(|key| parse_key(key).map_err(Error::InvalidKey)).collect()
        }
        Input::CommitteeFile(path) => {
            let bytes = tokio::fs::read(path).await.map_err(Error::Io)?;
            serde_yaml::from_slice::<Committee>(&bytes)
                .map(|committee| committee.bls_keys())
                .map_err(Error::Yaml)
        }
        Input::Rpc(url) => {
            let remote_chain: String = rpc(http, url, "eth_chainId", json!([])).await?;
            let remote_chain = u64::from_str_radix(remote_chain.trim_start_matches("0x"), 16)
                .map_err(|error| Error::Rpc(error.to_string()))?;
            (remote_chain == chain_id).then_some(()).ok_or_else(|| {
                Error::Rpc(format!("expected chain {chain_id}, received {remote_chain}"))
            })?;
            let epoch: Epoch = rpc(http, url, "tn_getCurrentEpoch", json!([])).await?;
            let keys: Vec<String> =
                rpc(http, url, "tn_getCommitteeBlsPubkeys", json!([epoch])).await?;
            keys.iter().map(|key| parse_key(key).map_err(Error::InvalidKey)).collect()
        }
    }
}

/// JSON-RPC response envelope; an error never becomes an empty committee.
#[derive(Deserialize)]
struct RpcResponse<T> {
    /// Must identify JSON-RPC 2.0.
    jsonrpc: String,
    /// Must echo the request ID.
    id: u64,
    /// Successful result, if present.
    result: Option<T>,
    /// Remote RPC failure, if present.
    error: Option<Value>,
}

/// Make a read-only JSON-RPC call and validate its envelope before decoding the result.
async fn rpc<T: DeserializeOwned>(
    http: &reqwest::Client,
    url: &Url,
    method: &str,
    params: Value,
) -> Result<T, Error> {
    let response = http
        .post(url.clone())
        .json(&json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params }))
        .send()
        .await
        .map_err(|error| Error::Http(error.without_url()))?
        .error_for_status()
        .map_err(|error| Error::Http(error.without_url()))?
        .json::<RpcResponse<T>>()
        .await
        .map_err(|error| Error::Http(error.without_url()))?;
    (response.jsonrpc == "2.0" && response.id == 1)
        .then_some(())
        .ok_or_else(|| Error::Rpc("unexpected JSON-RPC version or response ID".into()))?;
    response.error.map_or(Ok(()), |error| Err(Error::Rpc(error.to_string())))?;
    response.result.ok_or_else(|| Error::Rpc("missing result".into()))
}

#[cfg(test)]
mod tests;
