//! Public errors for TN RPC endpoints.
//!
//! These errors are returned by the RPC for public requests to the `tn` namespace.

use reth_primitives::{hex::encode_prefixed, ChainId};
use thiserror::Error;

/// The result type for TN RPC namespace.
pub type TelcoinNetworkRpcResult<T> = Result<T, TNRpcError>;

/// Error type for public RPC endpoints in the `tn` namespace.
#[derive(Debug, Error)]
pub enum TNRpcError {
    /// Handshake client is trying to join the wrong network.
    ///
    /// Return this node's chain id.
    #[error("Incompatible chain id for chain: {0}")]
    InvalidChainId(ChainId),
    /// Handshake client provided an invalid signature for network key.
    #[error("Invalid proof of possession for provided network key.")]
    InvalidProofOfPossession,
}

impl From<TNRpcError> for jsonrpsee_types::ErrorObject<'static> {
    fn from(error: TNRpcError) -> Self {
        // TODO: update this when adding errors
        match error {
            _ => rpc_err(500, error.to_string(), None),
        }
    }
}

/// Constructs a JSON-RPC error for jsonrpsee compatibility.
pub fn rpc_err(
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
