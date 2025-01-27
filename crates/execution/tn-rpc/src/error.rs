//! Public errors for TN RPC endpoints.
//!
//! These errors are returned by the RPC for public requests to the `tn` namespace.

use thiserror::Error;
use tn_types::hex::encode_prefixed;

/// The result type for TN RPC namespace.
pub type TelcoinNetworkRpcResult<T> = Result<T, TNRpcError>;

/// Error type for public RPC endpoints in the `tn` namespace.
#[derive(Debug, Error)]
pub enum TNRpcError {
    /// Handshake client provided an invalid signature for network key.
    #[error("Invalid proof of possession for provided network key or genesis.")]
    InvalidProofOfPossession,
}

impl From<TNRpcError> for jsonrpsee_types::ErrorObject<'static> {
    fn from(error: TNRpcError) -> Self {
        // TODO: update this when adding errors
        match error {
            TNRpcError::InvalidProofOfPossession => rpc_error(401, error.to_string(), None),
            // _ => rpc_error(500, error.to_string(), None),
        }
    }
}

/// Constructs a JSON-RPC error for jsonrpsee compatibility.
pub fn rpc_error(
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
