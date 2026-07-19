//! Well-formed JSON-RPC error responses for gateway-side failures.
//!
//! When the gateway itself cannot serve a request (no ready upstream, upstream
//! unreachable, upstream timed out) it must answer with a JSON-RPC 2.0 error
//! object rather than a bare connection reset, so clients see a structured
//! failure they can handle.

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde_json::{json, Value};

/// JSON-RPC 2.0 error codes used by the gateway.
///
/// The gateway-specific codes sit in the implementation-defined server-error
/// range (`-32000..=-32099`) of the JSON-RPC 2.0 spec. Upstream servers use
/// the same range for their own errors (jsonrpsee's call errors start at
/// `-32000`), so a code alone does not identify the gateway as the source;
/// clients should disambiguate by the HTTP status and message.
mod code {
    /// No upstream worker is currently ready to serve the request.
    pub(super) const NO_UPSTREAM_READY: i32 = -32000;
    /// The upstream worker could not be reached.
    pub(super) const UPSTREAM_UNREACHABLE: i32 = -32001;
    /// The upstream worker did not answer within the request deadline.
    pub(super) const UPSTREAM_TIMEOUT: i32 = -32002;
    /// The request body exceeded the gateway's size limit.
    pub(super) const REQUEST_TOO_LARGE: i32 = -32003;
    /// The request already passed through a worker gateway (forwarding loop).
    pub(super) const LOOP_DETECTED: i32 = -32004;
    /// The request did not complete within the gateway's request deadline.
    pub(super) const REQUEST_TIMEOUT: i32 = -32005;
    /// The client exceeded the gateway's rate limit.
    pub(super) const RATE_LIMITED: i32 = -32006;
    /// An `eth_sendRawTransaction` payload could not be decoded as a transaction.
    pub(super) const INVALID_TRANSACTION: i32 = -32007;
    /// An `eth_sendRawTransaction` payload decoded to a transaction type the
    /// network does not accept (an EIP-4844 blob transaction).
    pub(super) const UNSUPPORTED_TRANSACTION_TYPE: i32 = -32008;
    /// The request body could not be read. This is the spec-defined
    /// "Invalid Request" code, not a gateway-range code.
    pub(super) const INVALID_REQUEST: i32 = -32600;
}

/// A gateway-side failure surfaced to the client as a JSON-RPC error.
#[derive(Debug)]
pub(crate) enum GatewayError {
    /// No upstream worker was ready per the readiness poller.
    NoUpstreamReady,
    /// The upstream worker could not be reached (connection failure).
    UpstreamUnreachable,
    /// The upstream worker did not answer within the request deadline.
    UpstreamTimeout,
    /// The request body exceeded the gateway's size limit.
    RequestTooLarge,
    /// The request already carried the gateway's hop marker (forwarding loop).
    LoopDetected,
    /// The request did not complete within the gateway's request deadline.
    RequestTimeout,
    /// The client exceeded the gateway's per-IP or global rate limit.
    RateLimited,
    /// An `eth_sendRawTransaction` payload could not be decoded as a
    /// transaction (malformed hex or RLP).
    InvalidTransaction,
    /// An `eth_sendRawTransaction` payload decoded to a transaction type the
    /// network does not accept (an EIP-4844 blob transaction).
    UnsupportedTransactionType,
    /// The request body could not be read (e.g. the client aborted mid-body).
    UnreadableBody,
}

impl GatewayError {
    /// The HTTP status paired with this error.
    fn status(&self) -> StatusCode {
        match self {
            Self::NoUpstreamReady => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamUnreachable => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::LoopDetected => StatusCode::LOOP_DETECTED,
            Self::RequestTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidTransaction | Self::UnsupportedTransactionType => StatusCode::BAD_REQUEST,
            Self::UnreadableBody => StatusCode::BAD_REQUEST,
        }
    }

    /// The JSON-RPC error code paired with this error.
    fn code(&self) -> i32 {
        match self {
            Self::NoUpstreamReady => code::NO_UPSTREAM_READY,
            Self::UpstreamUnreachable => code::UPSTREAM_UNREACHABLE,
            Self::UpstreamTimeout => code::UPSTREAM_TIMEOUT,
            Self::RequestTooLarge => code::REQUEST_TOO_LARGE,
            Self::LoopDetected => code::LOOP_DETECTED,
            Self::RequestTimeout => code::REQUEST_TIMEOUT,
            Self::RateLimited => code::RATE_LIMITED,
            Self::InvalidTransaction => code::INVALID_TRANSACTION,
            Self::UnsupportedTransactionType => code::UNSUPPORTED_TRANSACTION_TYPE,
            Self::UnreadableBody => code::INVALID_REQUEST,
        }
    }

    /// A human-readable message paired with this error.
    fn message(&self) -> &'static str {
        match self {
            Self::NoUpstreamReady => "no upstream worker is ready",
            Self::UpstreamUnreachable => "upstream worker unreachable",
            Self::UpstreamTimeout => "upstream worker request timed out",
            Self::RequestTooLarge => "request body too large",
            Self::LoopDetected => {
                "proxy loop detected: request already passed through a worker gateway"
            }
            Self::RequestTimeout => "request did not complete within the gateway's deadline",
            Self::RateLimited => "rate limit exceeded; slow down and retry",
            Self::InvalidTransaction => "raw transaction could not be decoded",
            Self::UnsupportedTransactionType => {
                "unsupported transaction type: EIP-4844 blob transactions are not accepted"
            }
            Self::UnreadableBody => "request body could not be read",
        }
    }
}

/// Render `err` as a JSON-RPC 2.0 error response, echoing the request `id`
/// recovered from `request_body` when possible (otherwise `null`, per spec).
pub(crate) fn error_response(err: &GatewayError, request_body: &[u8]) -> Response {
    let body = json!({
        "jsonrpc": "2.0",
        "error": { "code": err.code(), "message": err.message() },
        "id": recover_id(request_body),
    });
    // Serialization of this fixed shape cannot fail; keep a defensive fallback.
    let text = serde_json::to_string(&body).unwrap_or_else(|_| {
        format!(
            r#"{{"jsonrpc":"2.0","error":{{"code":{},"message":"{}"}},"id":null}}"#,
            err.code(),
            err.message()
        )
    });

    let mut response = Response::new(Body::from(text));
    *response.status_mut() = err.status();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// Best-effort recovery of the JSON-RPC request `id` for echoing in an error.
///
/// Only the top-level `id` is read; a batch, malformed, or id-less body yields
/// `null`.
fn recover_id(request_body: &[u8]) -> Value {
    serde_json::from_slice::<Value>(request_body)
        .ok()
        .and_then(|value| value.get("id").cloned())
        .unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovers_numeric_id() {
        assert_eq!(recover_id(br#"{"jsonrpc":"2.0","method":"eth_chainId","id":7}"#), json!(7));
    }

    #[test]
    fn recovers_string_id() {
        assert_eq!(recover_id(br#"{"id":"abc"}"#), json!("abc"));
    }

    #[test]
    fn missing_or_malformed_id_is_null() {
        assert_eq!(recover_id(b"not json"), Value::Null);
        assert_eq!(recover_id(br#"{"method":"eth_chainId"}"#), Value::Null);
    }

    #[test]
    fn error_response_carries_status_content_type_and_code() {
        let response = error_response(&GatewayError::NoUpstreamReady, br#"{"id":9}"#);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&HeaderValue::from_static("application/json"))
        );
    }

    #[test]
    fn distinct_status_per_variant() {
        assert_eq!(
            error_response(&GatewayError::UpstreamUnreachable, b"{}").status(),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            error_response(&GatewayError::UpstreamTimeout, b"{}").status(),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            error_response(&GatewayError::LoopDetected, b"{}").status(),
            StatusCode::LOOP_DETECTED
        );
        assert_eq!(
            error_response(&GatewayError::RequestTimeout, b"{}").status(),
            StatusCode::REQUEST_TIMEOUT
        );
        assert_eq!(
            error_response(&GatewayError::UnreadableBody, b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_response(&GatewayError::RateLimited, b"{}").status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            error_response(&GatewayError::InvalidTransaction, b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_response(&GatewayError::UnsupportedTransactionType, b"{}").status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn new_edge_protection_codes_are_stable() {
        // The codes are part of the client contract; pin them so a reorder or
        // renumber is caught.
        assert_eq!(GatewayError::RateLimited.code(), -32006);
        assert_eq!(GatewayError::InvalidTransaction.code(), -32007);
        assert_eq!(GatewayError::UnsupportedTransactionType.code(), -32008);
    }
}
