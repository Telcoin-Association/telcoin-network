//! Forward transactions accepted by a non-committee ("observer") worker to the committee.
//!
//! Instead of pushing raw transaction bytes over the libp2p worker protocol, an observer
//! forwards each transaction to the JSON-RPC endpoint the owning validator advertised on its
//! worker record (issue #804), so the submitter gets the same RPC experience they would get
//! talking to a validator directly. Routing mirrors [`submit_txn_if_mine`]: a transaction is
//! sent to the validator whose committee slot owns the sender, so all transactions from one
//! account converge on a single validator and nonce ordering is preserved. A validator that has
//! not advertised an endpoint (or is momentarily unreachable) is skipped in favor of one that
//! has.
//!
//! [`submit_txn_if_mine`]: tn_types::BatchValidation::submit_txn_if_mine

use crate::recover_raw_transaction;
use alloy::{
    providers::{Provider as _, RootProvider},
    transports::{RpcError, TransportErrorKind},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tn_types::{BlsPublicKey, RpcInfo, TaskSpawner, TxnForwarder};
use tokio::time::timeout;
use tracing::{debug, warn};

/// Bounds a single validator's `eth_sendRawTransaction` round-trip so one unresponsive endpoint
/// cannot stall the fallback chain; on timeout the forward tries the next validator.
const FORWARD_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the total time spent forwarding one transaction across all its fallback validators, so a
/// whole unresponsive committee cannot make a single transaction cost `committee_size` back-to-back
/// [`FORWARD_SEND_TIMEOUT`]s. When it elapses the transaction is left unforwarded and the next one
/// proceeds.
const FORWARD_TX_BUDGET: Duration = Duration::from_secs(15);

/// JSON-RPC error codes reth returns for a validator-local, transient condition (a full pool,
/// `-32003`, or an internal/IO error, `-32603`) rather than a verdict on the transaction itself.
/// The forward falls through to the next advertised validator on these, since another validator
/// may still accept the transaction.
const TRANSIENT_RPC_CODES: [i64; 2] = [-32003, -32603];

/// Substring of reth's `eth_sendRawTransaction` error message when the transaction is already in a
/// validator's pool (`code -32000`). Treated as a successful delivery, not a failure to retry.
const ALREADY_KNOWN_MESSAGE: &str = "already known";

/// Forwards observer transactions to validators over their advertised JSON-RPC endpoints.
#[derive(Clone)]
pub struct WorkerRpcForwarder {
    /// Spawner used to run the (best-effort, non-blocking) forward on a background task.
    task_spawner: TaskSpawner,
    /// HTTP providers cached per advertised endpoint so consecutive batch seals reuse the
    /// underlying connection pool instead of paying a fresh TCP+TLS handshake per validator
    /// per seal. Entries for endpoints that are no longer advertised are evicted on each
    /// forward, so the cache stays bounded by the current committee's advertisement set.
    providers: Arc<Mutex<BTreeMap<String, RootProvider>>>,
}

impl std::fmt::Debug for WorkerRpcForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerRpcForwarder")
    }
}

impl WorkerRpcForwarder {
    /// Create a new forwarder that runs forwards on `task_spawner`.
    pub fn new(task_spawner: TaskSpawner) -> Self {
        Self { task_spawner, providers: Arc::new(Mutex::new(BTreeMap::new())) }
    }

    /// Resolve the advertised endpoints to providers, reusing cached ones where the endpoint
    /// is unchanged. A malformed URL is dropped here so the per-transaction loop only sees
    /// usable endpoints. The endpoint string is reparsed into the exact URL type the provider
    /// expects. `RootProvider` needs no fillers: the transactions are already signed raw
    /// bytes, and `send_raw_transaction` submits them as-is.
    fn cached_providers(
        &self,
        validator_rpcs: &[(BlsPublicKey, RpcInfo)],
    ) -> BTreeMap<BlsPublicKey, RootProvider> {
        let mut cache = self.providers.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let advertised: BTreeSet<String> =
            validator_rpcs.iter().map(|(_, rpc)| rpc.http.to_string()).collect();
        cache.retain(|url, _| advertised.contains(url));
        validator_rpcs
            .iter()
            .filter_map(|(key, rpc)| {
                let url = rpc.http.to_string();
                cache
                    .get(&url)
                    .cloned()
                    .or_else(|| match url.parse() {
                        Ok(parsed) => {
                            let provider = RootProvider::new_http(parsed);
                            cache.insert(url, provider.clone());
                            Some(provider)
                        }
                        Err(err) => {
                            debug!(
                                target: "worker::forward",
                                endpoint = %rpc.http,
                                %err,
                                "skipping validator with an unparseable RPC endpoint"
                            );
                            None
                        }
                    })
                    .map(|provider| (*key, provider))
            })
            .collect()
    }
}

impl TxnForwarder for WorkerRpcForwarder {
    fn forward_txns(
        &self,
        transactions: Vec<Vec<u8>>,
        committee_slots: Vec<BlsPublicKey>,
        validator_rpcs: Vec<(BlsPublicKey, RpcInfo)>,
    ) {
        let committee_size = committee_slots.len() as u64;
        if transactions.is_empty() || committee_size == 0 || validator_rpcs.is_empty() {
            return;
        }

        let providers = self.cached_providers(&validator_rpcs);
        if providers.is_empty() {
            warn!(
                target: "worker::forward",
                "no usable validator RPC endpoints; dropping forwarded transactions"
            );
            return;
        }
        // Fallback order: every usable endpoint, so a transaction whose owning validator has
        // not advertised (or is unreachable) can still reach the committee.
        let fallbacks: Vec<BlsPublicKey> = providers.keys().cloned().collect();

        self.task_spawner.spawn_task("forward-txns", async move {
            for tx in &transactions {
                // Route by sender so all transactions from one account land on the same
                // validator (matches `submit_txn_if_mine`), then fall back to any endpoint.
                let owner = owning_validator(tx, committee_size, &committee_slots);
                let ordered = owner.into_iter().chain(fallbacks.iter().cloned());

                // Bound the whole fallback chain for this transaction: even if every advertised
                // validator accepts the connection but never answers, one transaction cannot cost
                // more than `FORWARD_TX_BUDGET` before the next transaction proceeds.
                let outcome = timeout(FORWARD_TX_BUDGET, async {
                    let mut tried = BTreeSet::new();
                    let mut result = ForwardOutcome::NoEndpointReached;
                    for key in ordered {
                        if !tried.insert(key) {
                            continue;
                        }
                        let Some(provider) = providers.get(&key) else {
                            continue;
                        };
                        // Bound each validator's round-trip: an endpoint that accepts the
                        // connection but never answers must not stall the whole fallback chain.
                        let disposition = timeout(
                            FORWARD_SEND_TIMEOUT,
                            provider.send_raw_transaction(tx.as_slice()),
                        )
                        .await
                        .map_or_else(
                            |_elapsed| Disposition::TryNext("send timed out".to_string()),
                            |res| res.err().map_or(Disposition::Delivered, classify_error),
                        );

                        match disposition {
                            // Delivered, or a considered rejection every validator would repeat:
                            // either way this transaction is done, so stop the fallback chain.
                            Disposition::Delivered => {
                                result = ForwardOutcome::Delivered;
                                break;
                            }
                            Disposition::Rejected(reason) => {
                                result = ForwardOutcome::Rejected(reason);
                                break;
                            }
                            // Endpoint-local problem (timeout, transport error, full pool): the
                            // transaction's fate is unknown here, so try the next validator.
                            Disposition::TryNext(reason) => {
                                debug!(
                                    target: "worker::forward",
                                    reason = %reason,
                                    "validator RPC did not accept the forwarded transaction; trying next"
                                );
                            }
                        }
                    }
                    result
                })
                .await
                .unwrap_or(ForwardOutcome::NoEndpointReached);

                match outcome {
                    ForwardOutcome::Delivered => {}
                    ForwardOutcome::Rejected(reason) => warn!(
                        target: "worker::forward",
                        reason = %reason,
                        "a validator rejected the forwarded transaction; not retrying other validators"
                    ),
                    ForwardOutcome::NoEndpointReached => warn!(
                        target: "worker::forward",
                        "could not forward transaction to any advertised validator RPC"
                    ),
                }
            }
            Ok(())
        });
    }
}

/// Return the BLS key of the committee slot that owns `tx_bytes`, matching the receiver-side
/// routing in `submit_txn_if_mine`. Returns `None` if the transaction cannot be recovered or the
/// derived slot is out of range.
fn owning_validator(
    tx_bytes: &[u8],
    committee_size: u64,
    committee_slots: &[BlsPublicKey],
) -> Option<BlsPublicKey> {
    let recovered = recover_raw_transaction(tx_bytes).ok()?;
    let sender = recovered.signer();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&sender.as_slice()[0..8]);
    let slot = (u64::from_le_bytes(bytes) % committee_size) as usize;
    committee_slots.get(slot).cloned()
}

/// What to do after one timed attempt to forward a transaction to one validator.
#[derive(Debug, PartialEq, Eq)]
enum Disposition {
    /// The validator accepted the transaction, or it was already in a validator's pool.
    Delivered,
    /// The validator returned a considered rejection of the transaction itself (bad nonce,
    /// underpriced, invalid, wrong fork). No other validator will accept it either.
    Rejected(String),
    /// This endpoint gave no verdict (timeout, transport error, or a transient full pool); the
    /// transaction may still be accepted by another validator.
    TryNext(String),
}

/// Terminal result of forwarding one transaction across the ordered validators.
#[derive(Debug, PartialEq, Eq)]
enum ForwardOutcome {
    /// A validator accepted it (or already had it).
    Delivered,
    /// A validator gave a considered rejection, so the chain was stopped without delivery.
    Rejected(String),
    /// No advertised validator gave a verdict (all timed out or were unreachable).
    NoEndpointReached,
}

/// Classify a failed `send_raw_transaction` by whether the server returned a JSON-RPC error (a
/// verdict about the transaction) or the transport failed (an endpoint problem).
fn classify_error(err: RpcError<TransportErrorKind>) -> Disposition {
    err.as_error_resp().map(|payload| (payload.code, payload.message.to_string())).map_or_else(
        || Disposition::TryNext(err.to_string()),
        |(code, message)| classify_server_error(code, message),
    )
}

/// Classify a server-side JSON-RPC rejection of a forwarded transaction.
fn classify_server_error(code: i64, message: String) -> Disposition {
    if TRANSIENT_RPC_CODES.contains(&code) {
        // A full pool or an internal error is validator-local; another validator may accept it.
        Disposition::TryNext(message)
    } else if message.to_ascii_lowercase().contains(ALREADY_KNOWN_MESSAGE) {
        // Already in a validator's pool: the transaction is delivered.
        Disposition::Delivered
    } else {
        // Every validator shares consensus state, so a considered rejection repeats everywhere.
        Disposition::Rejected(format!("code {code}: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, SeedableRng};
    use tn_types::{BlsKeypair, TaskManager};

    fn test_forwarder() -> WorkerRpcForwarder {
        WorkerRpcForwarder::new(TaskManager::default().get_spawner())
    }

    fn test_key(seed: u8) -> BlsPublicKey {
        *BlsKeypair::generate(&mut StdRng::from_seed([seed; 32])).public()
    }

    fn test_rpc(url: &str) -> eyre::Result<RpcInfo> {
        Ok(RpcInfo { http: url.parse()?, ws: None })
    }

    fn cached_urls(forwarder: &WorkerRpcForwarder) -> Vec<String> {
        forwarder.providers.lock().map(|cache| cache.keys().cloned().collect()).unwrap_or_default()
    }

    #[test]
    fn cached_providers_resolves_every_advertised_endpoint() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let rpcs = vec![
            (test_key(1), test_rpc("http://127.0.0.1:8545")?),
            (test_key(2), test_rpc("http://127.0.0.1:8546")?),
        ];

        let resolved = forwarder.cached_providers(&rpcs);

        assert_eq!(resolved.len(), 2);
        assert_eq!(cached_urls(&forwarder).len(), 2);
        Ok(())
    }

    #[test]
    fn cached_providers_reuses_cache_and_evicts_stale_endpoints() -> eyre::Result<()> {
        let forwarder = test_forwarder();
        let live = (test_key(1), test_rpc("http://127.0.0.1:8545")?);
        let stale = (test_key(2), test_rpc("http://127.0.0.1:8546")?);
        let first = forwarder.cached_providers(&[live.clone(), stale]);
        assert_eq!(first.len(), 2);

        // A repeat advertisement resolves from the cache without growing it; the endpoint
        // that is no longer advertised is evicted.
        let second = forwarder.cached_providers(&[live]);

        assert_eq!(second.len(), 1);
        assert_eq!(cached_urls(&forwarder), vec!["http://127.0.0.1:8545/".to_string()]);
        Ok(())
    }

    #[test]
    fn classify_server_error_maps_reth_rejections() -> eyre::Result<()> {
        // A duplicate ("already known", code -32000) is a delivery, not a failure to retry.
        assert_eq!(
            classify_server_error(-32000, "already known".to_string()),
            Disposition::Delivered
        );
        // A full pool (code -32003) is transient: fall through to the next validator.
        assert_eq!(
            classify_server_error(-32003, "txpool is full".to_string()),
            Disposition::TryNext("txpool is full".to_string())
        );
        // An internal error (code -32603) is also validator-local: try the next validator.
        assert_eq!(
            classify_server_error(-32603, "database error".to_string()),
            Disposition::TryNext("database error".to_string())
        );
        // A considered rejection (nonce too low, code -32000) stops the fallback chain.
        assert!(matches!(
            classify_server_error(-32000, "nonce too low: next nonce 42, tx nonce 41".to_string()),
            Disposition::Rejected(_)
        ));
        Ok(())
    }
}
