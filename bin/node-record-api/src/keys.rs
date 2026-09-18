//! Where the set of BLS keys to look up comes from.
//!
//! The DHT is keyed by BLS public key, so the daemon needs a list of keys to query; it never
//! enumerates the DHT. Three sources exist and combine as `live ∪ floor`:
//!
//! - [`RpcCommitteeKeys`] (**live**): the current committee, read from a node's JSON-RPC
//!   (`tn_getCurrentEpochInfo` then `tn_getCommitteeBlsPubkeys`). It is the only source that tracks
//!   committee rotation without operator action, and the only one that yields the epoch boundary
//!   the scheduler uses.
//! - [`CommitteeFileKeys`] (**floor**): the authorities of a `committee.yaml`, re-read every cycle
//!   so an updated file is picked up without a restart.
//! - [`StaticKeys`] (**floor**): a YAML list of keys, parsed once at startup.
//!
//! **`tn_getCommitteeBlsPubkeys` returns the COMMITTEE, not all validators.** A staked validator
//! that is not in the current committee but advertises RPC is invisible to the RPC source; list
//! it in the committee file or the static list so it is tracked anyway.
//!
//! Every source keeps the last set it resolved successfully, and a failed refresh reuses it:
//! a transient RPC blip or a half-written committee file must not shrink the directory. The
//! failure is logged and counted (`tn_node_record_api_key_source_failures_total{source}`) so it
//! is visible without being disruptive.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tn_kad_client::{BlsPublicKey, Multiaddr};
use tn_node_record::{parse_bls_pubkey, ParseBlsPubkeyError};
use tn_types::{Committee, WorkerId};
use tracing::{debug, warn};
use url::Url;

use crate::epoch::{next_boundary, EpochInfoSummary};

/// The combined key sources and the union they resolve to each cycle.
#[derive(Debug)]
pub struct KeySet {
    /// The configured sources, in resolution order.
    sources: Vec<KeySource>,
}

/// What one cycle's key resolution produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedKeys {
    /// The union of every source's current set.
    pub keys: BTreeSet<BlsPublicKey>,
    /// The epoch summary from the live source, when it refreshed successfully this cycle.
    pub epoch: Option<EpochInfoSummary>,
    /// The labels of the sources whose refresh failed this cycle (their last good set was
    /// reused).
    pub failed_sources: Vec<&'static str>,
}

impl KeySet {
    /// Combine `sources`.
    pub fn new(sources: Vec<KeySource>) -> Self {
        Self { sources }
    }

    /// Whether a live (RPC) source is configured, and so whether epoch boundaries will be known.
    pub fn has_live_source(&self) -> bool {
        self.sources.iter().any(|source| matches!(source, KeySource::Rpc(_)))
    }

    /// Refresh every source and union their sets. A source that fails contributes its last good
    /// set and is named in [`ResolvedKeys::failed_sources`].
    pub async fn resolve(&mut self) -> ResolvedKeys {
        let mut resolved = ResolvedKeys::default();
        for source in &mut self.sources {
            match source.refresh().await {
                Ok(epoch) => {
                    if epoch.is_some() {
                        resolved.epoch = epoch;
                    }
                }
                Err(err) => {
                    warn!(
                        target: "tn::node_record_api",
                        source = source.label(),
                        %err,
                        "key source refresh failed; reusing its last good set"
                    );
                    resolved.failed_sources.push(source.label());
                }
            }
            resolved.keys.extend(source.keys().iter().copied());
        }
        resolved
    }
}

/// One place the daemon learns keys from.
#[derive(Debug)]
pub enum KeySource {
    /// The live committee via a node's JSON-RPC.
    Rpc(RpcCommitteeKeys),
    /// The authorities of a committee file, re-read each cycle.
    CommitteeFile(CommitteeFileKeys),
    /// A fixed list.
    Static(StaticKeys),
}

impl KeySource {
    /// The `source` metric label.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Rpc(_) => "rpc",
            Self::CommitteeFile(_) => "committee_file",
            Self::Static(_) => "static",
        }
    }

    /// Re-resolve the source's set, returning the epoch summary if the source knows one. On an
    /// error the previous set is retained.
    async fn refresh(&mut self) -> Result<Option<EpochInfoSummary>, KeySourceError> {
        match self {
            Self::Rpc(rpc) => rpc.refresh().await.map(Some),
            Self::CommitteeFile(file) => file.reload().map(|()| None),
            Self::Static(_) => Ok(None),
        }
    }

    /// The source's current (last good) set.
    fn keys(&self) -> &BTreeSet<BlsPublicKey> {
        match self {
            Self::Rpc(rpc) => &rpc.keys,
            Self::CommitteeFile(file) => &file.keys,
            Self::Static(fixed) => &fixed.keys,
        }
    }
}

/// Why a key source failed to refresh (or, for file sources, to load at startup).
#[derive(Debug, thiserror::Error)]
pub enum KeySourceError {
    /// The JSON-RPC HTTP request failed (connect, timeout, non-2xx, unreadable body).
    #[error("json-rpc request failed: {0}")]
    Http(#[from] reqwest::Error),
    /// The node answered with a JSON-RPC error object.
    #[error("json-rpc error {code}: {message}")]
    JsonRpc {
        /// The JSON-RPC error code.
        code: i64,
        /// The JSON-RPC error message.
        message: String,
    },
    /// The node's answer did not have the expected shape.
    #[error("json-rpc response malformed: {0}")]
    Malformed(String),
    /// A key file could not be read.
    #[error("failed to read {path}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The I/O error.
        source: std::io::Error,
    },
    /// A key file is not the YAML shape expected of it.
    #[error("failed to parse {path}: {source}")]
    Yaml {
        /// The file.
        path: PathBuf,
        /// The YAML error.
        source: serde_yaml::Error,
    },
    /// A static key list entry is not a BLS public key.
    #[error("invalid bls public key {entry:?} in {path}: {source}")]
    InvalidKey {
        /// The file.
        path: PathBuf,
        /// The offending entry.
        entry: String,
        /// Why it did not parse.
        source: ParseBlsPubkeyError,
    },
}

/// The live committee, read from a node's JSON-RPC.
///
/// Plain JSON-RPC 2.0 over `reqwest`; the three methods used are `tn_getCurrentEpochInfo`,
/// `eth_getBlockByNumber` (for the epoch's start timestamp), and `tn_getCommitteeBlsPubkeys`.
#[derive(Debug)]
pub struct RpcCommitteeKeys {
    /// The HTTP client, carrying the request timeout.
    client: reqwest::Client,
    /// The node's JSON-RPC endpoint.
    url: Url,
    /// The last committee resolved successfully.
    keys: BTreeSet<BlsPublicKey>,
}

/// The fields of `tn_getCurrentEpochInfo`'s result the daemon reads. Unknown fields (the
/// committee addresses, issuance, stake version) are ignored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct EpochInfoRaw {
    /// The execution block height at which the epoch started.
    #[serde(rename = "blockHeight")]
    pub block_height: u64,
    /// The epoch id.
    #[serde(rename = "epochId")]
    pub epoch_id: u32,
    /// The epoch duration in seconds.
    #[serde(rename = "epochDuration")]
    pub epoch_duration: u32,
}

/// The one field of an `eth_getBlockByNumber` block the daemon reads.
#[derive(Debug, Deserialize)]
struct BlockTimestampRaw {
    /// The block timestamp as a `0x`-hex quantity.
    timestamp: String,
}

/// A JSON-RPC 2.0 response envelope.
#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    /// The result, absent on error (and `null` for an unknown block).
    #[serde(default)]
    result: Option<Value>,
    /// The error object, present on failure.
    #[serde(default)]
    error: Option<JsonRpcErrorObject>,
}

/// A JSON-RPC 2.0 error object.
#[derive(Debug, Deserialize)]
struct JsonRpcErrorObject {
    /// The error code.
    code: i64,
    /// The error message.
    message: String,
}

impl RpcCommitteeKeys {
    /// Build a source against `url` whose requests are bounded by `timeout`.
    pub fn new(url: Url, timeout: Duration) -> Result<Self, KeySourceError> {
        let client = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self { client, url, keys: BTreeSet::new() })
    }

    /// Fetch the current epoch and its committee, replacing the held set on success.
    async fn refresh(&mut self) -> Result<EpochInfoSummary, KeySourceError> {
        let info = parse_epoch_info(self.call("tn_getCurrentEpochInfo", json!([])).await?)?;
        // the epoch started at the previous epoch's closing block, `blockHeight - 1`; epoch 0
        // (and any registry that reports height 0) saturates to genesis, as the node does
        let closing = info.block_height.saturating_sub(1);
        let block =
            self.call("eth_getBlockByNumber", json!([format!("0x{closing:x}"), false])).await?;
        let epoch_start = parse_block_timestamp(block)?;
        let summary = epoch_summary(&info, epoch_start);

        let keys = parse_committee_keys(
            self.call("tn_getCommitteeBlsPubkeys", json!([info.epoch_id])).await?,
        )?;
        debug!(
            target: "tn::node_record_api",
            epoch = summary.epoch_id,
            keys = keys.len(),
            next_boundary = summary.next_boundary_unix,
            "resolved committee from rpc"
        );
        self.keys = keys;
        Ok(summary)
    }

    /// One JSON-RPC 2.0 call, returning the raw `result`.
    async fn call(&self, method: &str, params: Value) -> Result<Value, KeySourceError> {
        let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
        let response: JsonRpcResponse = self
            .client
            .post(self.url.clone())
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(error) = response.error {
            return Err(KeySourceError::JsonRpc { code: error.code, message: error.message });
        }
        response.result.ok_or_else(|| KeySourceError::Malformed(format!("{method}: no result")))
    }
}

/// Decode a `tn_getCurrentEpochInfo` result.
pub fn parse_epoch_info(result: Value) -> Result<EpochInfoRaw, KeySourceError> {
    decode("tn_getCurrentEpochInfo", result)
}

/// Extract the timestamp from an `eth_getBlockByNumber` result. A `null` result (the node does
/// not have the block) is malformed for our purposes: the epoch's start is unknowable.
pub fn parse_block_timestamp(result: Value) -> Result<u64, KeySourceError> {
    if result.is_null() {
        return Err(KeySourceError::Malformed("eth_getBlockByNumber: block not found".into()));
    }
    let block: BlockTimestampRaw = decode("eth_getBlockByNumber", result)?;
    let hex = block
        .timestamp
        .strip_prefix("0x")
        .or_else(|| block.timestamp.strip_prefix("0X"))
        .ok_or_else(|| {
            KeySourceError::Malformed(format!(
                "eth_getBlockByNumber: timestamp {:?} is not 0x-hex",
                block.timestamp
            ))
        })?;
    u64::from_str_radix(hex, 16).map_err(|err| {
        KeySourceError::Malformed(format!(
            "eth_getBlockByNumber: timestamp {:?}: {err}",
            block.timestamp
        ))
    })
}

/// Decode a `tn_getCommitteeBlsPubkeys` result: a list of `0x`-hex compressed keys. An element
/// that does not parse is logged and skipped rather than failing the whole committee, so one
/// malformed registry entry cannot blank the directory.
pub fn parse_committee_keys(result: Value) -> Result<BTreeSet<BlsPublicKey>, KeySourceError> {
    let entries: Vec<String> = decode("tn_getCommitteeBlsPubkeys", result)?;
    Ok(entries
        .iter()
        .filter_map(|entry| {
            parse_bls_pubkey(entry)
                .inspect_err(|err| {
                    warn!(
                        target: "tn::node_record_api",
                        entry,
                        %err,
                        "skipping unparseable committee bls key from rpc"
                    );
                })
                .ok()
        })
        .collect())
}

/// Derive the epoch summary from the raw info and the epoch's start timestamp.
pub fn epoch_summary(info: &EpochInfoRaw, epoch_start: u64) -> EpochInfoSummary {
    EpochInfoSummary {
        epoch_id: info.epoch_id,
        next_boundary_unix: next_boundary(epoch_start, u64::from(info.epoch_duration)),
    }
}

/// Deserialize a JSON-RPC `result` into `T`, naming the method in the error.
fn decode<T: DeserializeOwned>(method: &str, result: Value) -> Result<T, KeySourceError> {
    serde_json::from_value(result)
        .map_err(|err| KeySourceError::Malformed(format!("{method}: {err}")))
}

/// The authorities of a `committee.yaml`, re-read every cycle.
#[derive(Debug)]
pub struct CommitteeFileKeys {
    /// The file.
    path: PathBuf,
    /// The last set parsed successfully.
    keys: BTreeSet<BlsPublicKey>,
}

impl CommitteeFileKeys {
    /// Load the file once; a file that does not parse at startup is an error.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, KeySourceError> {
        let path = path.into();
        let keys = committee_keys(&load_committee(&path)?);
        Ok(Self { path, keys })
    }

    /// Re-read the file, keeping the previous set if it no longer parses.
    fn reload(&mut self) -> Result<(), KeySourceError> {
        self.keys = committee_keys(&load_committee(&self.path)?);
        Ok(())
    }
}

/// Parse a committee file.
pub fn load_committee(path: &Path) -> Result<Committee, KeySourceError> {
    let text = std::fs::read_to_string(path)
        .map_err(|source| KeySourceError::Read { path: path.to_path_buf(), source })?;
    serde_yaml::from_str(&text)
        .map_err(|source| KeySourceError::Yaml { path: path.to_path_buf(), source })
}

/// The protocol (BLS) keys of every authority in `committee`.
pub fn committee_keys(committee: &Committee) -> BTreeSet<BlsPublicKey> {
    committee.authorities().iter().map(|authority| *authority.protocol_key()).collect()
}

/// The DHT addresses of every bootstrap server's worker `worker_id`, in key order. A bootstrap
/// entry that advertises fewer workers is skipped (the list is a dial hint, not an invariant).
pub fn worker_bootstrap_addrs(committee: &Committee, worker_id: WorkerId) -> Vec<Multiaddr> {
    committee
        .bootstrap_servers()
        .values()
        .filter_map(|server| server.worker(worker_id).map(|node| node.network_address.clone()))
        .collect()
}

/// A fixed key list.
#[derive(Debug)]
pub struct StaticKeys {
    /// The keys.
    keys: BTreeSet<BlsPublicKey>,
}

impl StaticKeys {
    /// A list from already-parsed keys.
    pub fn new(keys: impl IntoIterator<Item = BlsPublicKey>) -> Self {
        Self { keys: keys.into_iter().collect() }
    }

    /// The keys, in key order.
    pub fn into_keys(self) -> Vec<BlsPublicKey> {
        self.keys.into_iter().collect()
    }

    /// Load a YAML list of base58 or `0x`-hex keys. Every entry must parse: the list is operator
    /// input, so a typo is a startup error rather than a silently shorter directory.
    pub fn load(path: &Path) -> Result<Self, KeySourceError> {
        let text = std::fs::read_to_string(path)
            .map_err(|source| KeySourceError::Read { path: path.to_path_buf(), source })?;
        parse_static_keys(&text, path).map(Self::new)
    }
}

/// Parse the YAML list behind `--keys-file`; `path` only labels errors.
pub fn parse_static_keys(yaml: &str, path: &Path) -> Result<Vec<BlsPublicKey>, KeySourceError> {
    let entries: Vec<String> = serde_yaml::from_str(yaml)
        .map_err(|source| KeySourceError::Yaml { path: path.to_path_buf(), source })?;
    entries
        .into_iter()
        .map(|entry| {
            parse_bls_pubkey(&entry).map_err(|source| KeySourceError::InvalidKey {
                path: path.to_path_buf(),
                entry,
                source,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    /// The committee fixture in this crate's current on-disk shape.
    const FIXTURE: &str = include_str!("../tests/fixtures/committee.yaml");

    /// The real testnet committee, relative to the crate.
    fn testnet_committee_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../chain-configs/testnet/committee.yaml")
    }

    fn hex_of(key: &BlsPublicKey) -> String {
        format!("0x{}", hex::encode(key.as_ref()))
    }

    #[test]
    fn committee_fixture_yields_keys_and_worker_bootstrap_addrs() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(FIXTURE.as_bytes()).expect("write");
        let source = CommitteeFileKeys::load(file.path()).expect("fixture loads");
        assert_eq!(source.keys.len(), 2);

        let committee = load_committee(file.path()).expect("fixture parses");
        assert_eq!(committee_keys(&committee), source.keys);
        let addrs = worker_bootstrap_addrs(&committee, 0);
        assert_eq!(addrs.len(), 2);
        for addr in &addrs {
            let text = addr.to_string();
            assert!(text.contains("/udp/49594/"), "{text}");
            assert!(text.contains("/p2p/"), "{text}");
        }
        // the fixture's bootstrap entries advertise a single worker
        assert!(worker_bootstrap_addrs(&committee, 1).is_empty());
    }

    #[test]
    fn real_testnet_committee_has_five_validators_on_the_worker_port() {
        let path = testnet_committee_path();
        let committee = load_committee(&path).expect("testnet committee parses");
        let keys = committee_keys(&committee);
        assert_eq!(keys.len(), 5);
        let addrs = worker_bootstrap_addrs(&committee, 0);
        assert_eq!(addrs.len(), 5);
        for addr in addrs {
            let text = addr.to_string();
            assert!(text.contains("/udp/49594/quic-v1/p2p/"), "{text}");
        }
        let source = CommitteeFileKeys::load(&path).expect("loads");
        assert_eq!(source.keys, keys);
    }

    #[tokio::test]
    async fn committee_file_reload_failure_keeps_last_good_set() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(FIXTURE.as_bytes()).expect("write");
        let source = CommitteeFileKeys::load(file.path()).expect("fixture loads");
        let good = source.keys.clone();
        let mut set = KeySet::new(vec![KeySource::CommitteeFile(source)]);

        // clobber the file: the next resolve fails the source but still serves the old set
        std::fs::write(file.path(), "authorities: [not a map]\n").expect("clobber");
        let resolved = set.resolve().await;
        assert_eq!(resolved.keys, good);
        assert_eq!(resolved.failed_sources, vec!["committee_file"]);
        assert!(resolved.epoch.is_none());
        assert!(!set.has_live_source());
    }

    #[test]
    fn static_keys_accept_base58_and_hex() {
        let committee = load_committee(&testnet_committee_path()).expect("parses");
        let keys: Vec<_> = committee_keys(&committee).into_iter().collect();
        let yaml = format!("- {}\n- \"{}\"\n", keys[0], hex_of(&keys[1]));
        let parsed = parse_static_keys(&yaml, Path::new("keys.yaml")).expect("parses");
        assert_eq!(parsed, vec![keys[0], keys[1]]);

        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(yaml.as_bytes()).expect("write");
        let source = StaticKeys::load(file.path()).expect("loads");
        assert_eq!(source.keys.len(), 2);
    }

    #[test]
    fn static_keys_reject_a_bad_entry() {
        let err = parse_static_keys("- 0xzz\n", Path::new("keys.yaml")).expect_err("bad entry");
        assert!(matches!(err, KeySourceError::InvalidKey { .. }), "{err}");
        let err = parse_static_keys("not: a list\n", Path::new("keys.yaml")).expect_err("bad yaml");
        assert!(matches!(err, KeySourceError::Yaml { .. }), "{err}");
    }

    #[test]
    fn epoch_info_parses_the_sol_struct_json() {
        // the alloy `sol!` struct serializes with its solidity field names; the fields we do not
        // read are present and ignored
        let result = json!({
            "committee": ["0x0033a370616805b1fd275b7ffab83fc41d665ccb"],
            "epochIssuance": "0xde0b6b3a7640000",
            "blockHeight": 12345,
            "epochId": 7,
            "epochDuration": 28800,
            "stakeVersion": 1
        });
        let info = parse_epoch_info(result).expect("parses");
        assert_eq!(
            info,
            EpochInfoRaw { block_height: 12_345, epoch_id: 7, epoch_duration: 28_800 }
        );
        let summary = epoch_summary(&info, 1_700_000_000);
        assert_eq!(summary, EpochInfoSummary { epoch_id: 7, next_boundary_unix: 1_700_028_800 });

        let err = parse_epoch_info(json!({ "epochId": 7 })).expect_err("missing fields");
        assert!(matches!(err, KeySourceError::Malformed(_)), "{err}");
    }

    #[test]
    fn block_timestamp_parses_hex_quantity() {
        let block = json!({ "number": "0x3038", "timestamp": "0x6553f100", "hash": "0xabc" });
        assert_eq!(parse_block_timestamp(block).expect("parses"), 0x6553_f100);
        assert!(matches!(parse_block_timestamp(Value::Null), Err(KeySourceError::Malformed(_))));
        assert!(matches!(
            parse_block_timestamp(json!({ "timestamp": "12" })),
            Err(KeySourceError::Malformed(_))
        ));
    }

    #[test]
    fn committee_keys_skip_a_bad_hex_element() {
        let committee = load_committee(&testnet_committee_path()).expect("parses");
        let keys: Vec<_> = committee_keys(&committee).into_iter().collect();
        let result = json!([hex_of(&keys[0]), "0xdeadbeef", hex_of(&keys[1])]);
        let parsed = parse_committee_keys(result).expect("parses");
        assert_eq!(parsed, [keys[0], keys[1]].into_iter().collect::<BTreeSet<_>>());

        let err = parse_committee_keys(json!({"not": "a list"})).expect_err("wrong shape");
        assert!(matches!(err, KeySourceError::Malformed(_)), "{err}");
    }

    #[tokio::test]
    async fn union_is_live_plus_floor_and_reuses_live_on_failure() {
        let committee = load_committee(&testnet_committee_path()).expect("parses");
        let keys: Vec<_> = committee_keys(&committee).into_iter().collect();
        // an rpc source against a port nobody listens on fails every refresh; seed it with the
        // set a previous successful refresh would have left behind
        let mut rpc = RpcCommitteeKeys::new(
            "http://127.0.0.1:1/".parse().expect("url"),
            Duration::from_millis(200),
        )
        .expect("client");
        rpc.keys = [keys[1], keys[2]].into_iter().collect();
        let mut set = KeySet::new(vec![
            KeySource::Rpc(rpc),
            KeySource::Static(StaticKeys::new([keys[0], keys[1]])),
        ]);
        assert!(set.has_live_source());
        let resolved = set.resolve().await;
        // live (previous set) ∪ floor, deduplicated; the blip shrinks nothing
        assert_eq!(resolved.keys, [keys[0], keys[1], keys[2]].into_iter().collect());
        assert_eq!(resolved.failed_sources, vec!["rpc"]);
        assert!(resolved.epoch.is_none());
    }
}
