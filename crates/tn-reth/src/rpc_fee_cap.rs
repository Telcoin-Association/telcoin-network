//! Enforce `--rpc.txfeecap` on the RPC transaction-submission methods.
//!
//! Reth's pool validator checks the fee cap only for transactions it treats as local
//! (`LocalTransactionConfig::is_local`). Reth's `eth_sendRawTransaction` submits with
//! `TransactionOrigin::External`, so the validator check never runs for public RPC
//! traffic (issue #1160). This module closes that gap at the RPC boundary:
//! [`EthSubmitWithCap`] replaces the two raw submission methods with handlers that
//! check the cap first and then delegate to the unchanged reth implementations.
//!
//! The check reproduces the validator's arithmetic and is never laxer: the maximum
//! fee is `max_fee_per_gas * gas_limit`, plus the blob fee bound for EIP-4844
//! transactions, computed with saturating `U256` operations. (Reth saturates the
//! intermediate products in `u128` before widening, so in overflow corners far
//! beyond real balances reth compares a smaller quantity; this guard keeps full
//! `U256` precision.) An over-cap transaction returns the same error object reth's
//! validator produces (`RpcPoolError::ExceedsFeeCap`). A cap of zero disables the
//! check. This matches the flag's documented semantics and the validator's own
//! `Some(0) | None` skip.
//!
//! Error precedence matches reth for the cases this module decides itself: empty
//! bytes and undecodable bytes return the same errors reth's
//! `recover_raw_transaction` returns. A transaction that is over the cap and also
//! has an invalid signature returns the fee-cap error here; reth would have
//! reported the signature first. A consequence: a client can learn the configured
//! cap from the error string without a valid signature. The cap is operator
//! configuration, not a secret, and any funded account learns it the same way.
//! The delegated path is otherwise unchanged.

use async_trait::async_trait;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_rpc_eth_api::{
    helpers::{EthTransactions, LoadReceipt},
    EthApiTypes,
};
use reth_rpc_eth_types::{error::RpcPoolError, EthApiError};
use tn_types::{Bytes, Decodable2718 as _, PooledTransaction, TransactionTrait as _, B256, U256};

/// The `--rpc.txfeecap` value in wei. Zero disables the cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxFeeCapWei(u128);

impl TxFeeCapWei {
    /// Wrap a cap value parsed from `--rpc.txfeecap`.
    pub(crate) const fn new(cap_wei: u128) -> Self {
        Self(cap_wei)
    }

    /// Check one raw transaction against the cap.
    ///
    /// Decodes without signer recovery: the delegated reth handler recovers and
    /// validates the signature itself.
    pub(crate) fn enforce(&self, raw: &[u8]) -> Result<(), EthApiError> {
        let Self(cap_wei) = *self;
        match () {
            () if cap_wei == 0 => Ok(()),
            () if raw.is_empty() => Err(EthApiError::EmptyRawTransactionData),
            () => PooledTransaction::decode_2718_exact(raw)
                .map_err(|_| EthApiError::FailedToDecodeSignedTransaction)
                .and_then(|tx| Self::check_cap(&tx, cap_wei)),
        }
    }

    /// Compare the transaction's maximum fee against the cap.
    ///
    /// Reproduces reth's validator check (`EthTransactionValidator`) and is never
    /// laxer: the compared quantity is `cost - value`, which is `max_fee_per_gas *
    /// gas_limit` plus `max_fee_per_blob_gas * blob_gas_used` for EIP-4844
    /// transactions, in saturating `U256` arithmetic.
    fn check_cap(tx: &PooledTransaction, cap_wei: u128) -> Result<(), EthApiError> {
        let gas_fee = U256::from(tx.max_fee_per_gas()).saturating_mul(U256::from(tx.gas_limit()));
        let blob_fee = tx
            .max_fee_per_blob_gas()
            .zip(tx.blob_gas_used())
            .map(|(max_fee_per_blob_gas, blob_gas_used)| {
                U256::from(max_fee_per_blob_gas).saturating_mul(U256::from(blob_gas_used))
            })
            .unwrap_or_default();
        let max_tx_fee_wei = gas_fee.saturating_add(blob_fee);
        (max_tx_fee_wei <= U256::from(cap_wei)).then_some(()).ok_or_else(|| {
            EthApiError::PoolError(RpcPoolError::ExceedsFeeCap {
                max_tx_fee_wei: max_tx_fee_wei.saturating_to(),
                tx_fee_cap_wei: cap_wei,
            })
        })
    }
}

/// The two `eth` submission methods TN overrides.
///
/// Method names match reth's `EthApiServer` exactly so the built server can swap
/// the handlers in place with `TransportRpcModules::add_or_replace_if_module_configured`.
#[rpc(server, namespace = "eth")]
pub(crate) trait CappedEthSubmit {
    /// `eth_sendRawTransaction`: check the fee cap, then delegate to reth.
    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256>;

    /// `eth_sendRawTransactionSync`: check the fee cap, then delegate to reth.
    #[method(name = "sendRawTransactionSync")]
    async fn send_raw_transaction_sync(
        &self,
        bytes: Bytes,
    ) -> RpcResult<alloy::rpc::types::TransactionReceipt>;
}

/// Fee-cap guard over reth's `EthApi` submission methods.
#[derive(Debug, Clone)]
pub(crate) struct EthSubmitWithCap<Api> {
    /// The reth `EthApi` this guard delegates to once the cap check passes.
    eth_api: Api,
    /// The configured cap.
    cap: TxFeeCapWei,
}

impl<Api> EthSubmitWithCap<Api> {
    /// Create a new guard from the built `EthApi` and the parsed flag value.
    pub(crate) const fn new(eth_api: Api, cap: TxFeeCapWei) -> Self {
        Self { eth_api, cap }
    }
}

#[async_trait]
impl<Api> CappedEthSubmitServer for EthSubmitWithCap<Api>
where
    Api: EthTransactions
        + LoadReceipt
        + EthApiTypes<NetworkTypes = alloy::network::Ethereum>
        + Clone
        + Send
        + Sync
        + 'static,
    jsonrpsee::types::ErrorObject<'static>: From<<Api as EthApiTypes>::Error>,
{
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        // Keep reth's request-trace parity: operators grep this target on the
        // node's only submission path.
        tracing::trace!(target: "rpc::eth", ?bytes, "Serving eth_sendRawTransaction");
        self.cap.enforce(&bytes)?;
        Ok(EthTransactions::send_raw_transaction(&self.eth_api, bytes).await?)
    }

    async fn send_raw_transaction_sync(
        &self,
        bytes: Bytes,
    ) -> RpcResult<alloy::rpc::types::TransactionReceipt> {
        tracing::trace!(target: "rpc::eth", ?bytes, "Serving eth_sendRawTransactionSync");
        self.cap.enforce(&bytes)?;
        Ok(EthTransactions::send_raw_transaction_sync(&self.eth_api, bytes).await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_utils::TransactionFactory, RethChainSpec};
    use alloy::{
        consensus::{
            BlobTransactionSidecar, BlobTransactionSidecarVariant, Signed, TxEip4844,
            TxEip4844WithSidecar,
        },
        primitives::Signature,
    };
    use std::sync::Arc;
    use tn_types::{test_genesis, Address, Encodable2718 as _};

    /// One signed EIP-1559 transaction with `max_fee_per_gas` 7 and gas limit 21,000,
    /// so the maximum fee is exactly 147,000 wei.
    fn encoded_tx_with_147_000_wei_fee() -> Vec<u8> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut tx_factory = TransactionFactory::new();
        tx_factory
            .create_eip1559(
                chain,
                Some(21_000),
                7,
                Some(Address::ZERO),
                U256::from(100),
                Bytes::new(),
            )
            .encoded_2718()
    }

    #[test]
    fn test_enforce_refuses_over_cap() {
        let raw = encoded_tx_with_147_000_wei_fee();
        let err = TxFeeCapWei::new(146_999).enforce(&raw).unwrap_err();
        assert_eq!(err.to_string(), "tx fee (147000 wei) exceeds the configured cap (146999 wei)");
    }

    #[test]
    fn test_enforce_allows_fee_equal_to_cap() {
        let raw = encoded_tx_with_147_000_wei_fee();
        assert!(TxFeeCapWei::new(147_000).enforce(&raw).is_ok());
    }

    #[test]
    fn test_enforce_zero_cap_disables_check() {
        let raw = encoded_tx_with_147_000_wei_fee();
        assert!(TxFeeCapWei::new(0).enforce(&raw).is_ok());
    }

    #[test]
    fn test_enforce_empty_bytes() {
        let err = TxFeeCapWei::new(1).enforce(&[]).unwrap_err();
        assert_eq!(err.to_string(), "empty transaction data");
    }

    #[test]
    fn test_enforce_undecodable_bytes() {
        let err = TxFeeCapWei::new(1).enforce(b"not a transaction").unwrap_err();
        assert_eq!(err.to_string(), "failed to decode signed transaction");
    }

    #[test]
    fn test_enforce_counts_blob_fee() {
        // One versioned hash makes `blob_gas_used` 131,072 (one blob), so the blob
        // fee bound is 3 * 131,072 = 393,216 wei on top of the 147,000 wei gas fee.
        let tx = TxEip4844 {
            chain_id: 2017,
            nonce: 0,
            gas_limit: 21_000,
            max_fee_per_gas: 7,
            max_priority_fee_per_gas: 1,
            to: Address::ZERO,
            value: U256::ZERO,
            access_list: Default::default(),
            blob_versioned_hashes: vec![B256::ZERO],
            max_fee_per_blob_gas: 3,
            input: Bytes::new(),
        };
        let signed = Signed::new_unchecked(
            TxEip4844WithSidecar::from_tx_and_sidecar(
                tx,
                BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar::default()),
            ),
            Signature::test_signature(),
            B256::ZERO,
        );
        let raw = PooledTransaction::Eip4844(signed).encoded_2718();
        let total = 147_000 + 393_216;

        assert!(TxFeeCapWei::new(total).enforce(&raw).is_ok());
        let err = TxFeeCapWei::new(total - 1).enforce(&raw).unwrap_err();
        assert_eq!(
            err.to_string(),
            format!("tx fee ({total} wei) exceeds the configured cap ({} wei)", total - 1)
        );
    }
}
