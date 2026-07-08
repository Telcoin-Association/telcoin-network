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
use alloy::providers::{Provider as _, RootProvider};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tn_types::{BlsPublicKey, RpcInfo, TaskSpawner, TxnForwarder};
use tracing::{debug, warn};

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

                let mut tried = BTreeSet::new();
                let mut forwarded = false;
                for key in ordered {
                    if !tried.insert(key) {
                        continue;
                    }
                    let Some(provider) = providers.get(&key) else {
                        continue;
                    };
                    match provider.send_raw_transaction(tx.as_slice()).await {
                        Ok(_) => {
                            forwarded = true;
                            break;
                        }
                        Err(err) => {
                            debug!(
                                target: "worker::forward",
                                %err,
                                "failed to forward transaction to a validator RPC; trying next"
                            );
                        }
                    }
                }
                if !forwarded {
                    warn!(
                        target: "worker::forward",
                        "could not forward transaction to any advertised validator RPC"
                    );
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
}
