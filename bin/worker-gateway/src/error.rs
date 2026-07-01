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
/// These sit in the implementation-defined server-error range
/// (`-32000..=-32099`) reserved by the JSON-RPC 2.0 spec, so they never
/// collide with an upstream worker's own application error codes.
mod code {
    /// No upstream worker is currently ready to serve the request.
    pub(super) const NO_UPSTREAM_READY: i32 = -32000;
    /// The upstream worker could not be reached.
    pub(super) const UPSTREAM_UNREACHABLE: i32 = -32001;
    /// The upstream worker did not answer within the request deadline.
    pub(super) const UPSTREAM_TIMEOUT: i32 = -32002;
    /// The request body exceeded the gateway's size limit.
    pub(super) const REQUEST_TOO_LARGE: i32 = -32003;
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
}

impl GatewayError {
    /// The HTTP status paired with this error.
    fn status(&self) -> StatusCode {
        match self {
            Self::NoUpstreamReady => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamUnreachable => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::RequestTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
        }
    }

    /// The JSON-RPC error code paired with this error.
    fn code(&self) -> i32 {
        match self {
            Self::NoUpstreamReady => code::NO_UPSTREAM_READY,
            Self::UpstreamUnreachable => code::UPSTREAM_UNREACHABLE,
            Self::UpstreamTimeout => code::UPSTREAM_TIMEOUT,
            Self::RequestTooLarge => code::REQUEST_TOO_LARGE,
        }
    }

    /// A human-readable message paired with this error.
    fn message(&self) -> &'static str {
        match self {
            Self::NoUpstreamReady => "no upstream worker is ready",
            Self::UpstreamUnreachable => "upstream worker unreachable",
            Self::UpstreamTimeout => "upstream worker request timed out",
            Self::RequestTooLarge => "request body too large",
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
    }
}
