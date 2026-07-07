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
use alloy::providers::{Provider as _, ProviderBuilder};
use std::collections::{BTreeMap, BTreeSet};
use tn_types::{BlsPublicKey, RpcInfo, TaskSpawner, TxnForwarder};
use tracing::{debug, warn};

/// Forwards observer transactions to validators over their advertised JSON-RPC endpoints.
#[derive(Clone)]
pub struct WorkerRpcForwarder {
    /// Spawner used to run the (best-effort, non-blocking) forward on a background task.
    task_spawner: TaskSpawner,
}

impl std::fmt::Debug for WorkerRpcForwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WorkerRpcForwarder")
    }
}

impl WorkerRpcForwarder {
    /// Create a new forwarder that runs forwards on `task_spawner`.
    pub fn new(task_spawner: TaskSpawner) -> Self {
        Self { task_spawner }
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

        self.task_spawner.spawn_task("forward-txns", async move {
            // Build one HTTP provider per advertised validator, keyed by BLS key. A malformed
            // URL is dropped here so the per-transaction loop only sees usable endpoints. The
            // endpoint string is reparsed into the exact URL type the provider expects.
            let mut providers = BTreeMap::new();
            for (key, rpc) in &validator_rpcs {
                match rpc.http.as_str().parse() {
                    Ok(url) => {
                        providers.insert(key.clone(), ProviderBuilder::new().connect_http(url));
                    }
                    Err(err) => {
                        debug!(
                            target: "worker::forward",
                            endpoint = %rpc.http,
                            %err,
                            "skipping validator with an unparseable RPC endpoint"
                        );
                    }
                }
            }
            if providers.is_empty() {
                warn!(
                    target: "worker::forward",
                    "no usable validator RPC endpoints; dropping forwarded transactions"
                );
                return Ok(());
            }
            // Fallback order: every usable endpoint, so a transaction whose owning validator has
            // not advertised (or is unreachable) can still reach the committee.
            let fallbacks: Vec<BlsPublicKey> = providers.keys().cloned().collect();

            for tx in &transactions {
                // Route by sender so all transactions from one account land on the same
                // validator (matches `submit_txn_if_mine`), then fall back to any endpoint.
                let owner = owning_validator(tx, committee_size, &committee_slots);
                let ordered = owner.into_iter().chain(fallbacks.iter().cloned());

                let mut tried = BTreeSet::new();
                let mut forwarded = false;
                for key in ordered {
                    if !tried.insert(key.clone()) {
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
