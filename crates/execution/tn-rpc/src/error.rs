//! Public errors for TN RPC endpoints.
//!
//! These errors are returned by the RPC for public requests to the `tn` namespace.

use thiserror::Error;
use tn_types::{hex::encode_prefixed, Bytes};

/// The result type for TN RPC namespace.
pub(crate) type TelcoinNetworkRpcResult<T> = Result<T, TNRpcError>;

/// JSON-RPC error code for an on-chain revert (matches `eth_call` behavior).
const EXECUTION_REVERTED: i32 = 3;
/// EIP-1474 "resource not found".
const RESOURCE_NOT_FOUND: i32 = -32001;
/// JSON-RPC 2.0 internal error.
const INTERNAL_ERROR: i32 = -32603;
/// JSON-RPC 2.0 invalid params.
const INVALID_PARAMS: i32 = -32602;

/// Error type for public RPC endpoints in the `tn` namespace.
#[derive(Debug, Error)]
pub enum TNRpcError {
    /// Requested item not found.
    #[error("Not Found.")]
    NotFound,
    /// On-chain ConsensusRegistry call reverted (eth_call-style; revert bytes in `data`).
    #[error("{message}")]
    Revert {
        /// `execution reverted[: <reason>]`, mirroring `eth_call`.
        message: String,
        /// Raw ABI-encoded revert bytes, hex-encoded into the error `data` field.
        output: Bytes,
    },
    /// Internal failure reading the registry. Detail logged server-side only.
    #[error("consensus registry read failed")]
    Internal,
    /// Caller supplied an invalid parameter (e.g. a malformed BLS public key).
    #[error("{0}")]
    InvalidParams(String),
}

impl From<TNRpcError> for jsonrpsee_types::ErrorObject<'static> {
    fn from(error: TNRpcError) -> Self {
        match error {
            TNRpcError::NotFound => rpc_error(RESOURCE_NOT_FOUND, error.to_string(), None),
            TNRpcError::Revert { message, output } => {
                rpc_error(EXECUTION_REVERTED, message, Some(&output))
            }
            TNRpcError::Internal => rpc_error(INTERNAL_ERROR, error.to_string(), None),
            TNRpcError::InvalidParams(msg) => rpc_error(INVALID_PARAMS, msg, None),
        }
    }
}

/// Constructs a JSON-RPC error for jsonrpsee compatibility.
pub(crate) fn rpc_error(
    code: i32,
    msg: impl Into<String>,
    data: Option<&[u8]>,
) -> jsonrpsee_types::ErrorObject<'static> {
    jsonrpsee_types::ErrorObject::owned(
        code,
        msg.into(),
        data.map(|data| {
            jsonrpsee::core::to_json_raw_value(&encode_prefixed(data))
                .expect("string is serializable")
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::{Revert, SolError as _};
    use jsonrpsee_types::ErrorObject;

    #[test]
    fn not_found_maps_to_resource_not_found() {
        let err: ErrorObject<'static> = TNRpcError::NotFound.into();
        assert_eq!(err.code(), -32001);
        assert_eq!(err.message(), "Not Found.");
        assert!(err.data().is_none());
    }

    #[test]
    fn revert_maps_to_execution_reverted_with_data() {
        let output: Bytes = Revert::from("boom").abi_encode().into();
        let err: ErrorObject<'static> = TNRpcError::Revert {
            message: "execution reverted: boom".to_string(),
            output: output.clone(),
        }
        .into();
        assert_eq!(err.code(), 3);
        assert_eq!(err.message(), "execution reverted: boom");
        let data = err.data().expect("revert carries data").get().to_owned();
        let expected = format!("\"{}\"", encode_prefixed(&output));
        assert_eq!(data, expected);
    }

    #[test]
    fn internal_maps_to_internal_error_without_detail() {
        let err: ErrorObject<'static> = TNRpcError::Internal.into();
        assert_eq!(err.code(), -32603);
        assert_eq!(err.message(), "consensus registry read failed");
        assert!(err.data().is_none());
    }

    #[test]
    fn invalid_params_maps_to_invalid_params_code() {
        let err: ErrorObject<'static> = TNRpcError::InvalidParams("bad pubkey".to_string()).into();
        assert_eq!(err.code(), -32602);
        assert_eq!(err.message(), "bad pubkey");
        assert!(err.data().is_none());
    }
}
