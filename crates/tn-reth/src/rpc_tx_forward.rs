//! Forward raw transaction submissions to operator-configured validator RPC endpoints.
//!
//! A public RPC node (an observer) started with `--forward-txs` relays `eth_sendRawTransaction`
//! and `eth_sendRawTransactionSync` to a private, ordered list of validator RPC targets instead
//! of inserting the transaction into its own pool. Every other method is still served locally.
//! `target` parses the flag value into that list and `client` holds the per-target JSON-RPC
//! clients with their failover rules.
//!
//! [`EthSubmitForwarded`] replaces the two submission methods the same way the `--rpc.txfeecap`
//! guard (`crate::rpc_fee_cap`) does on a non-forwarding node, and takes that guard's place:
//!
//! - `eth_sendRawTransaction`: run the local precheck, then forward the client's bytes. The local
//!   pool is never touched, so the observer's batch builder cannot send the transaction a second
//!   time over the node-record path (`crate::forward`).
//! - `eth_sendRawTransactionSync`: run the precheck, subscribe to this node's canonical stream,
//!   forward a plain `eth_sendRawTransaction`, then wait for the receipt locally, as reth's default
//!   implementation does. The validator only ever receives `eth_sendRawTransaction`, so a public
//!   client cannot hold a validator connection open for the confirmation wait.
//!
//! The client sees what it would see talking to the validator directly: the validator's hash,
//! or its JSON-RPC error object with code, message and data unchanged. The node's own answers
//! are the fee-cap error (the precheck) and, when no target answers, one fixed `-32603`
//! "transaction submission unavailable" error.
//!
//! Known limitation: a forwarded transaction is not in this node's pool, so
//! `eth_getTransactionByHash` and the `pending` nonce on this node reflect it only once it is
//! executed.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt as _;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_chain_state::CanonStateSubscriptions as _;
use reth_rpc_eth_api::{
    helpers::{EthTransactions, LoadReceipt},
    EthApiTypes,
};
use reth_rpc_eth_types::{block::convert_transaction_receipt, EthApiError};
use tn_types::{Bytes, B256};

use crate::{
    metrics::{RpcTxForwardMetrics, RpcTxForwardOutcome},
    rpc_fee_cap::TxFeeCapWei,
};

mod client;
mod target;

pub(crate) use client::TxForwarder;
pub(crate) use target::parse_forward_targets;
pub use target::{ForwardTarget, ForwardTargets, TxForwardConfig};

/// The two `eth` submission methods a forwarding node overrides.
///
/// Method names match reth's `EthApiServer` exactly so the built server swaps the handlers in
/// place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait ForwardedEthSubmit {
    /// `eth_sendRawTransaction`: check locally, then forward to the configured targets.
    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256>;

    /// `eth_sendRawTransactionSync`: check locally, forward a plain `eth_sendRawTransaction`,
    /// then wait for the receipt on this node's canonical chain.
    #[method(name = "sendRawTransactionSync")]
    async fn send_raw_transaction_sync(
        &self,
        bytes: Bytes,
    ) -> RpcResult<alloy::rpc::types::TransactionReceipt>;
}

/// Forwarding handler for the eth submission methods.
#[derive(Debug, Clone)]
pub(crate) struct EthSubmitForwarded<Api> {
    /// The reth `EthApi`, used only for the receipt wait of the sync method.
    eth_api: Api,
    /// The process-wide relay every worker's RPC server shares.
    forwarder: Arc<TxForwarder>,
    /// The `--rpc.txfeecap` value, enforced before forwarding.
    cap: TxFeeCapWei,
}

impl<Api> EthSubmitForwarded<Api> {
    /// Create the handler from the built `EthApi`, the shared forwarder and the fee cap.
    pub(crate) const fn new(eth_api: Api, forwarder: Arc<TxForwarder>, cap: TxFeeCapWei) -> Self {
        Self { eth_api, forwarder, cap }
    }

    /// Run [`Self::precheck`] and count a refusal as `rejected_locally`.
    fn local_check(&self, bytes: &[u8]) -> Result<(), EthApiError> {
        self.precheck(bytes).inspect_err(|_| {
            RpcTxForwardMetrics::record_outcome(RpcTxForwardOutcome::RejectedLocally);
        })
    }

    /// The checks a submission must pass before any target sees it.
    ///
    /// The `--rpc.txfeecap` guard, exactly as a non-forwarding node runs it: with the default
    /// cap of 0 nothing is decoded and the client's bytes go out untouched, and with a cap the
    /// transaction is decoded, without signer recovery, to price it.
    fn precheck(&self, bytes: &[u8]) -> Result<(), EthApiError> {
        self.cap.enforce(bytes)
    }
}

#[async_trait]
impl<Api> ForwardedEthSubmitServer for EthSubmitForwarded<Api>
where
    Api: EthTransactions
        + LoadReceipt
        + EthApiTypes<NetworkTypes = alloy::network::Ethereum>
        + Clone
        + Send
        + Sync
        + 'static,
{
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        // keep reth's request-trace parity on the node's submission path
        tracing::trace!(target: "rpc::eth", ?bytes, "Serving eth_sendRawTransaction");
        self.local_check(&bytes)?;
        self.forwarder.submit(&bytes).await
    }

    async fn send_raw_transaction_sync(
        &self,
        bytes: Bytes,
    ) -> RpcResult<alloy::rpc::types::TransactionReceipt> {
        tracing::trace!(target: "rpc::eth", ?bytes, "Serving eth_sendRawTransactionSync");
        self.local_check(&bytes)?;
        // subscribe before forwarding so a block executed before the forward returns is seen
        let mut stream = self.eth_api.provider().canonical_state_stream();
        let hash = self.forwarder.submit(&bytes).await?;
        let duration = self.eth_api.send_raw_transaction_sync_timeout();
        let found = tokio::time::timeout(duration, async {
            while let Some(notification) = stream.next().await {
                let chain = notification.committed();
                let receipt = chain.find_transaction_and_receipt_by_hash(hash).and_then(
                    |(block, tx, receipt, all_receipts)| {
                        convert_transaction_receipt(
                            block,
                            all_receipts,
                            tx,
                            receipt,
                            self.eth_api.converter(),
                        )
                    },
                );
                if receipt.is_some() {
                    return receipt;
                }
            }
            None
        })
        .await;
        match found {
            Ok(Some(Ok(receipt))) => Ok(receipt),
            Ok(Some(Err(err))) => Err(Api::Error::from(err).into()),
            // the stream ended or the wait ran out: reth's own answer for an unconfirmed
            // transaction, the same one the validator would return
            Ok(None) | Err(_) => {
                Err(EthApiError::TransactionConfirmationTimeout { hash, duration }.into())
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
