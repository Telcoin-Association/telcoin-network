//! JSON-RPC reverse-proxy handler.
//!
//! Forwards a client request's method, JSON-RPC body, and content type to the
//! first ready upstream worker and returns the upstream's status, body, and
//! content type. Other headers are not forwarded in either direction, with
//! three deliberate additions on the upstream hop: `X-Forwarded-For` /
//! `X-Forwarded-Proto` (client identity for worker-side logs and the PR3 rate
//! limits) and the `X-TN-Gateway` hop marker (loop protection; an inbound
//! request that already carries it is rejected instead of forwarded). The
//! upstream response body is streamed through, never buffered whole. When no
//! upstream is ready, or the upstream cannot be reached / times out, the
//! client receives a well-formed JSON-RPC error instead (see [`crate::error`]).

use std::net::SocketAddr;

use axum::{
    body::{Body, Bytes},
    extract::{
        rejection::{BytesRejection, FailedToBufferBody},
        ConnectInfo, State,
    },
    http::{header, HeaderMap, HeaderName, HeaderValue, Method},
    response::Response,
};
use reqwest::Client;
use serde_json::Value;
use tn_types::{Decodable2718, PooledTransaction};
use tracing::{debug, warn};
use url::Url;

use crate::{
    error::{error_response, GatewayError},
    server::AppState,
    telemetry,
};

/// Default maximum request body the gateway will buffer before forwarding.
///
/// A guard against unbounded memory use; the effective limit is configurable
/// via `--max-request-bytes` (this value is that flag's default).
pub(crate) const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

/// The one JSON-RPC method whose payload the gateway inspects before
/// forwarding (a raw-transaction submission).
const SEND_RAW_TRANSACTION: &str = "eth_sendRawTransaction";

/// Marker header stamped on every forwarded request. An inbound request that
/// already carries it has looped back through a gateway (an upstream URL or
/// VIP that points at a gateway instead of a worker) and is rejected rather
/// than forwarded, breaking the loop at the first revisit.
pub(crate) const HOP_HEADER: HeaderName = HeaderName::from_static("x-tn-gateway");

/// De-facto standard header carrying the client IP chain to the upstream.
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// De-facto standard header carrying the client-facing scheme to the upstream.
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// Forward a JSON-RPC request to the first ready upstream worker.
///
/// `body` is the final extractor (it consumes the request body), so it must
/// stay last in the parameter list.
pub(crate) async fn proxy(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    // Track this proxied request in the in-flight gauge (the autoscaling signal)
    // and time it; the guard releases both on every return path below.
    let _in_flight = telemetry::RequestInFlight::enter();

    let body = match body {
        Ok(body) => body,
        Err(rejection) => return reject_body(&rejection),
    };

    // A request that already carries the hop marker has passed through a
    // gateway before: some upstream URL points back at a gateway, and
    // forwarding again would loop until fds run out.
    if headers.contains_key(HOP_HEADER) {
        warn!(
            target: "gateway::proxy",
            "proxy loop detected (inbound request already carries the gateway hop marker); \
             check that upstream URLs point at workers, not gateways"
        );
        return error_response(&GatewayError::LoopDetected, body.as_ref());
    }

    // Shallow pre-flight for raw-transaction submissions: reject a payload the
    // worker would also reject (undecodable, or an EIP-4844 blob) before paying
    // for an upstream round-trip.
    if let Some(err) = screen_raw_transaction(body.as_ref()) {
        warn!(target: "gateway::proxy", ?err, "rejecting eth_sendRawTransaction before forwarding");
        return error_response(&err, body.as_ref());
    }

    let Some(rpc_url) = state.readiness.first_ready_rpc_url() else {
        warn!(target: "gateway::proxy", "no upstream worker ready; rejecting request");
        return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
    };

    match forward(&state.http, method, &headers, body.clone(), rpc_url, peer).await {
        Ok(response) => {
            telemetry::record_forwarded();
            response
        }
        Err(err) => {
            warn!(target: "gateway::proxy", ?err, "forwarding to upstream failed");
            error_response(&err, body.as_ref())
        }
    }
}

/// Answer a body-buffering failure: a length-limit trip is a client error worth
/// warning about; any other buffering failure (e.g. the client aborted mid-body)
/// is not "oversized" and is logged quietly at debug.
fn reject_body(rejection: &BytesRejection) -> Response {
    match rejection {
        BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_)) => {
            warn!(target: "gateway::proxy", %rejection, "rejecting oversized request body");
            error_response(&GatewayError::RequestTooLarge, b"")
        }
        _ => {
            debug!(target: "gateway::proxy", %rejection, "failed to buffer request body");
            error_response(&GatewayError::UnreadableBody, b"")
        }
    }
}

/// Forward one request to `rpc_url` and adapt the upstream response back into an
/// axum response, preserving the status, body, and content type.
async fn forward(
    client: &Client,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
    rpc_url: Url,
    peer: SocketAddr,
) -> Result<Response, GatewayError> {
    // JSON-RPC is content-type `application/json`; preserve the client's header
    // when present, default to it otherwise.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));

    let upstream = client
        .request(method, rpc_url)
        .header(header::CONTENT_TYPE, content_type)
        .header(HOP_HEADER, HeaderValue::from_static("1"))
        .header(X_FORWARDED_FOR, forwarded_for(headers, peer))
        .header(X_FORWARDED_PROTO, HeaderValue::from_static("http"))
        .body(body)
        .send()
        .await
        .map_err(classify_error)?;

    let status = upstream.status();
    let upstream_content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();

    // Stream the upstream body through instead of buffering it whole: response
    // sizes are client-controlled (`eth_getLogs`, `debug_*`, large batches can
    // reach the worker's ~160 MB response cap), so N concurrent buffered
    // responses would exhaust gateway memory. The proxy client's total request
    // timeout keeps bounding the stream, so a stalled upstream or slow-reading
    // client cannot hold the connection past the deadline.
    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    if let Some(content_type) = upstream_content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    }
    Ok(response)
}

/// The `X-Forwarded-For` value for the upstream hop: the immediate peer
/// appended to any chain a prior proxy supplied.
fn forwarded_for(headers: &HeaderMap, peer: SocketAddr) -> HeaderValue {
    let peer_ip = peer.ip().to_string();
    let chain = headers
        .get(X_FORWARDED_FOR)
        .and_then(|previous| previous.to_str().ok())
        .map(|previous| format!("{previous}, {peer_ip}"))
        .unwrap_or(peer_ip);
    HeaderValue::from_str(&chain).unwrap_or_else(|_| HeaderValue::from_static("unknown"))
}

/// Classify a `reqwest` forwarding failure into a client-facing gateway error.
fn classify_error(err: reqwest::Error) -> GatewayError {
    if err.is_timeout() {
        GatewayError::UpstreamTimeout
    } else {
        GatewayError::UpstreamUnreachable
    }
}

/// Shallow pre-flight for `eth_sendRawTransaction`.
///
/// Returns `Some(error)` only when `body` is a single `eth_sendRawTransaction`
/// call whose raw transaction cannot be decoded, or decodes to an EIP-4844 blob
/// transaction (which the network does not accept). Every other request —
/// including batches, other methods, and any structurally-off submission —
/// returns `None` and is forwarded unchanged.
///
/// The decode uses the same pooled wire format the worker's RPC accepts and
/// never recovers the signer, so it cannot reject a transaction the worker
/// would have accepted (no false rejections); it only front-runs a rejection
/// the worker would issue anyway.
fn screen_raw_transaction(body: &[u8]) -> Option<GatewayError> {
    // Fast path: skip JSON parsing entirely unless the method name is present.
    if !mentions_send_raw_transaction(body) {
        return None;
    }
    let request: Value = serde_json::from_slice(body).ok()?;
    // Single-call objects only; a batch (a JSON array) is forwarded and left to
    // the worker to validate per element.
    if request.get("method").and_then(Value::as_str) != Some(SEND_RAW_TRANSACTION) {
        return None;
    }
    // A submission whose params are structurally off (missing / not a string)
    // is forwarded so the worker returns its own canonical parameter error.
    let raw_hex = request.get("params").and_then(|params| params.get(0)).and_then(Value::as_str)?;

    // From here the payload is unambiguously a raw transaction, so a decode
    // failure is a real rejection rather than a reason to forward.
    let Some(raw) = decode_hex(raw_hex) else {
        return Some(GatewayError::InvalidTransaction);
    };
    let mut buf = raw.as_slice();
    match PooledTransaction::decode_2718(&mut buf) {
        Err(_) => Some(GatewayError::InvalidTransaction),
        Ok(tx) if tx.is_eip4844() => Some(GatewayError::UnsupportedTransactionType),
        Ok(_) => None,
    }
}

/// Whether `body` is valid UTF-8 mentioning the raw-transaction method. JSON is
/// UTF-8 by definition, so a non-UTF-8 body is not a JSON-RPC call we inspect.
fn mentions_send_raw_transaction(body: &[u8]) -> bool {
    std::str::from_utf8(body).is_ok_and(|text| text.contains(SEND_RAW_TRANSACTION))
}

/// Decode a `0x`-prefixed (or bare) hex string into bytes, or `None` if it is
/// not valid hex.
fn decode_hex(value: &str) -> Option<Vec<u8>> {
    let trimmed = value.strip_prefix("0x").or_else(|| value.strip_prefix("0X")).unwrap_or(value);
    tn_types::hex::decode(trimmed).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical EIP-155 example transaction (a signed legacy transfer): a
    /// well-formed, non-blob raw transaction that must be forwarded untouched.
    const EIP155_LEGACY_TX: &str = "0xf86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83";

    fn send_raw(params: &str) -> Vec<u8> {
        format!(r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":{params},"id":1}}"#)
            .into_bytes()
    }

    #[test]
    fn valid_legacy_transaction_is_forwarded() {
        assert!(screen_raw_transaction(&send_raw(&format!("[\"{EIP155_LEGACY_TX}\"]"))).is_none());
    }

    #[test]
    fn undecodable_transaction_is_rejected() {
        // Valid hex, but not a decodable transaction envelope.
        let err = screen_raw_transaction(&send_raw(r#"["0xdeadbeef"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    #[test]
    fn blob_typed_payload_is_not_forwarded() {
        // A type-`0x03` (EIP-4844) prefix with a truncated body cannot decode as
        // a pooled transaction, so it is rejected rather than forwarded. Real
        // blob submissions decode and hit the `is_eip4844` reject; either way a
        // blob-typed payload never reaches an upstream.
        let err = screen_raw_transaction(&send_raw(r#"["0x03c0"]"#));
        assert!(err.is_some());
    }

    #[test]
    fn non_hex_param_is_rejected() {
        let err = screen_raw_transaction(&send_raw(r#"["not-hex"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    #[test]
    fn other_methods_are_forwarded() {
        let body = br#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"#;
        assert!(screen_raw_transaction(body).is_none());
    }

    #[test]
    fn batched_send_raw_is_forwarded() {
        // A batch is a JSON array with no top-level "method"; it is forwarded and
        // validated per element by the worker.
        let body = format!(
            r#"[{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":1}}]"#
        );
        assert!(screen_raw_transaction(body.as_bytes()).is_none());
    }

    #[test]
    fn missing_params_are_forwarded() {
        assert!(screen_raw_transaction(&send_raw("[]")).is_none());
    }

    #[test]
    fn non_json_body_is_forwarded() {
        // Mentions the method name but is not JSON: nothing to inspect, forward.
        assert!(screen_raw_transaction(b"garbage eth_sendRawTransaction garbage").is_none());
    }

    #[test]
    fn unrelated_body_skips_parsing() {
        assert!(screen_raw_transaction(br#"{"method":"net_version","id":1}"#).is_none());
    }
}
