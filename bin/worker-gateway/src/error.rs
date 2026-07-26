//! Well-formed JSON-RPC error responses for gateway-side failures.
//!
//! When the gateway itself cannot serve a request (no ready upstream, upstream
//! unreachable, upstream timed out) it must answer with a JSON-RPC 2.0 error
//! object rather than a bare connection reset, so clients see a structured
//! failure they can handle.

use std::{borrow::Cow, fmt};

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde::{
    de::{IgnoredAny, MapAccess, Visitor},
    Deserialize, Deserializer,
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

    /// A stable, machine-readable reason label for the
    /// `tn_worker_gateway_rejections_total` metric. It is kept in lockstep with
    /// the variants; a rename is a metrics-contract change, so the labels are
    /// pinned by a test.
    fn reason(&self) -> &'static str {
        match self {
            Self::NoUpstreamReady => "no_upstream_ready",
            Self::UpstreamUnreachable => "upstream_unreachable",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::RequestTooLarge => "request_too_large",
            Self::LoopDetected => "loop_detected",
            Self::RequestTimeout => "request_timeout",
            Self::RateLimited => "rate_limited",
            Self::InvalidTransaction => "invalid_transaction",
            Self::UnsupportedTransactionType => "unsupported_transaction_type",
            Self::UnreadableBody => "unreadable_body",
        }
    }
}

/// Render `err` as a JSON-RPC 2.0 error response, echoing the request `id`
/// recovered from `request_body` when possible (otherwise `null`, per spec).
///
/// A caller that has already parsed the body for its own reasons should take
/// the id from that parse and call [`error_response_with_id`] instead, rather
/// than paying for a second parse of the same bytes here.
pub(crate) fn error_response(err: &GatewayError, request_body: &[u8]) -> Response {
    error_response_with_id(err, RequestId::recover(request_body))
}

/// Render `err` as a JSON-RPC 2.0 error response echoing an `id` the caller has
/// already recovered.
pub(crate) fn error_response_with_id(err: &GatewayError, id: RequestId) -> Response {
    // Every gateway-side rejection funnels through here, so this is the single
    // point that records the rejection metric (by reason, plus the `rejected`
    // arm of the request counter).
    crate::telemetry::record_rejection(err.reason());

    let body = json!({
        "jsonrpc": "2.0",
        "error": { "code": err.code(), "message": err.message() },
        "id": id.0,
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

/// The JSON-RPC request `id` echoed back on an error response.
///
/// `null` is the "no id" case the spec mandates for a request whose id the
/// server could not read: a batch, a malformed body, or an id-less call.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequestId(Value);

impl RequestId {
    /// The top-level `id` of a request the caller has already parsed.
    ///
    /// The raw-transaction screen parses the body to decide whether to reject
    /// it; taking the id from that parse is what keeps the screening reject
    /// path to a single parse of the request.
    pub(crate) fn from_request(request: &Value) -> Self {
        Self(request.get("id").cloned().unwrap_or(Value::Null))
    }

    /// Best-effort recovery of the top-level `id` from an unparsed body.
    ///
    /// Only the `id` member is materialized: every other member deserializes
    /// into serde's ignored-value sink, which skips it in place. A multi-megabyte
    /// `params` therefore costs a scan of the bytes rather than a `Value` tree
    /// several times the size of the request.
    ///
    /// The whole body is still scanned, deliberately. JSON-RPC does not fix
    /// member order and clients routinely serialize `id` after a large
    /// `params`, so recovering from a bounded leading slice would echo `null`
    /// for well-formed large requests.
    pub(crate) fn recover(request_body: &[u8]) -> Self {
        serde_json::from_slice::<IdMember>(request_body)
            .map(|parsed| Self(parsed.0))
            .unwrap_or(Self(Value::Null))
    }
}

/// Deserialization shim that materializes a request's `id` member and nothing
/// else. See [`RequestId::recover`].
struct IdMember(Value);

impl<'de> Deserialize<'de> for IdMember {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(IdMemberVisitor)
    }
}

/// Visitor behind [`IdMember`]: walks a request's members, keeps `id`, and
/// discards the rest without materializing them.
struct IdMemberVisitor;

impl<'de> Visitor<'de> for IdMemberVisitor {
    type Value = IdMember;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC request object")
    }

    /// Only a JSON object carries a top-level `id`, and this visitor accepts
    /// nothing else, so a batch (a JSON array) is a type error here rather than
    /// a body whose first element could masquerade as an `id`.
    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<Self::Value, A::Error> {
        // Every non-`id` member deserializes into serde's ignored-value sink,
        // which skips it in place: a multi-megabyte `params` costs a scan of the
        // bytes rather than a `Value` tree several times the size of the input.
        // A repeated `id` keeps the last occurrence, which is how a full parse
        // into `Value` resolves a duplicated member.
        std::iter::from_fn(|| {
            members.next_key::<Cow<'_, str>>().transpose().map(|member| {
                member.and_then(|member| match member.as_ref() {
                    "id" => members.next_value::<Value>().map(Some),
                    _ => members.next_value::<IgnoredAny>().map(|_| None),
                })
            })
        })
        .try_fold(Value::Null, |id, member| member.map(|found| found.unwrap_or(id)))
        .map(IdMember)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The recovered id as a plain `Value`, for comparison against `json!`.
    fn recovered(request_body: &[u8]) -> Value {
        RequestId::recover(request_body).0
    }

    #[test]
    fn recovers_numeric_id() {
        assert_eq!(recovered(br#"{"jsonrpc":"2.0","method":"eth_chainId","id":7}"#), json!(7));
    }

    #[test]
    fn recovers_string_id() {
        assert_eq!(recovered(br#"{"id":"abc"}"#), json!("abc"));
    }

    #[test]
    fn missing_or_malformed_id_is_null() {
        assert_eq!(recovered(b"not json"), Value::Null);
        assert_eq!(recovered(br#"{"method":"eth_chainId"}"#), Value::Null);
    }

    #[test]
    fn recovers_an_id_that_trails_a_large_params() {
        // JSON-RPC does not fix member order and raw-transaction clients
        // routinely serialize `id` after the payload, so recovery must read
        // past the whole `params` rather than a leading slice of the body.
        let payload = "a".repeat(64 * 1024);
        let body =
            format!(r#"{{"method":"eth_sendRawTransaction","params":["{payload}"],"id":11}}"#);
        assert_eq!(recovered(body.as_bytes()), json!(11));
    }

    #[test]
    fn batch_body_recovers_null() {
        // A batch has no top-level id. Gating on the object opener is what stops
        // serde's struct-from-sequence accommodation from mapping the batch's
        // first element onto `id`.
        assert_eq!(recovered(br#"[{"method":"eth_chainId","id":3}]"#), Value::Null);
    }

    #[test]
    fn duplicated_id_member_keeps_the_last() {
        // Last-wins is how a full parse into `Value` resolves a duplicated
        // member; recovery must not diverge from it on a malformed body.
        assert_eq!(recovered(br#"{"id":1,"id":2}"#), json!(2));
    }

    #[test]
    fn leading_whitespace_does_not_hide_the_id() {
        assert_eq!(recovered(b"  \n\t{\"id\":4}"), json!(4));
    }

    #[test]
    fn trailing_bytes_recover_null() {
        // The body must be exactly one JSON value; garbage after it is not a
        // request whose id we can trust.
        assert_eq!(recovered(br#"{"id":4} trailing"#), Value::Null);
    }

    #[test]
    fn parsed_request_yields_its_id() {
        assert_eq!(RequestId::from_request(&json!({"id": 9, "method": "eth_chainId"})).0, json!(9));
        assert_eq!(RequestId::from_request(&json!({"method": "eth_chainId"})).0, Value::Null);
        assert_eq!(RequestId::from_request(&json!([{"id": 9}])).0, Value::Null);
    }

    #[test]
    fn recovery_matches_a_full_parse_of_the_body() {
        // The whole point of the shim is to cost less, not to answer
        // differently. This pins it against the full-`Value` parse it replaced,
        // over the shapes where a cheaper reader could plausibly drift:
        // member order, duplicates, non-object bodies, and malformed input.
        fn full_parse(request_body: &[u8]) -> Value {
            serde_json::from_slice::<Value>(request_body)
                .ok()
                .and_then(|value| value.get("id").cloned())
                .unwrap_or(Value::Null)
        }

        let bodies: [&[u8]; 18] = [
            br#"{"jsonrpc":"2.0","method":"eth_chainId","id":7}"#,
            br#"{"id":7,"jsonrpc":"2.0","method":"eth_chainId"}"#,
            br#"{"method":"eth_sendRawTransaction","params":["0x00"],"id":"tx-1"}"#,
            br#"{"id":"abc"}"#,
            br#"{"id":0}"#,
            br#"{"id":-1}"#,
            br#"{"id":1.5}"#,
            br#"{"id":null}"#,
            br#"{"id":{"nested":[1,2,3]}}"#,
            br#"{"id":[1,2]}"#,
            br#"{"id":1,"id":2}"#,
            br#"{"method":"eth_chainId"}"#,
            br#"{}"#,
            br#"[{"method":"eth_chainId","id":3}]"#,
            b"7",
            b"\"bare string\"",
            b"not json",
            b"",
        ];
        bodies.iter().for_each(|body| {
            assert_eq!(
                RequestId::recover(body).0,
                full_parse(body),
                "{}",
                String::from_utf8_lossy(body)
            );
        });
    }

    #[test]
    fn recovering_and_reusing_a_parse_agree() {
        // The two entry points must not drift: a reject path that reuses its own
        // parse has to echo exactly what a re-parse of the body would echo.
        let bodies: [&[u8]; 5] = [
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":"tx-1"}"#,
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":0}"#,
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"]}"#,
            br#"{"id":null}"#,
            br#"{"id":{"nested":[1,2,3]}}"#,
        ];
        bodies.iter().for_each(|body| {
            let parsed: Value = serde_json::from_slice(body).expect("body parses");
            assert_eq!(RequestId::recover(body), RequestId::from_request(&parsed), "{body:?}");
        });
    }

    #[tokio::test]
    async fn error_response_with_id_echoes_the_supplied_id() {
        let response =
            error_response_with_id(&GatewayError::InvalidTransaction, RequestId(json!("tx-1")));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["id"], json!("tx-1"));
        assert_eq!(body["error"]["code"], json!(-32007));
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

    #[test]
    fn reason_labels_are_stable() {
        // The reason labels are the `tn_worker_gateway_rejections_total{reason}`
        // metric contract (dashboards and alerts key on them); pin every variant
        // so a rename or an arm-crossing reorder is caught.
        assert_eq!(GatewayError::NoUpstreamReady.reason(), "no_upstream_ready");
        assert_eq!(GatewayError::UpstreamUnreachable.reason(), "upstream_unreachable");
        assert_eq!(GatewayError::UpstreamTimeout.reason(), "upstream_timeout");
        assert_eq!(GatewayError::RequestTooLarge.reason(), "request_too_large");
        assert_eq!(GatewayError::LoopDetected.reason(), "loop_detected");
        assert_eq!(GatewayError::RequestTimeout.reason(), "request_timeout");
        assert_eq!(GatewayError::RateLimited.reason(), "rate_limited");
        assert_eq!(GatewayError::InvalidTransaction.reason(), "invalid_transaction");
        assert_eq!(
            GatewayError::UnsupportedTransactionType.reason(),
            "unsupported_transaction_type"
        );
        assert_eq!(GatewayError::UnreadableBody.reason(), "unreadable_body");
    }
}
