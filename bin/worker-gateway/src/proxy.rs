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
use tracing::{debug, warn};
use url::Url;

use crate::{
    error::{error_response, GatewayError},
    server::AppState,
};

/// Maximum request body the gateway will buffer before forwarding.
///
/// A coarse guard against unbounded memory use; PR3 (edge protections) replaces
/// this with a configurable, metric-backed request-size limit.
pub(crate) const MAX_REQUEST_BYTES: usize = 25 * 1024 * 1024;

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

    let Some(rpc_url) = state.readiness.first_ready_rpc_url() else {
        warn!(target: "gateway::proxy", "no upstream worker ready; rejecting request");
        return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
    };

    match forward(&state.http, method, &headers, body.clone(), rpc_url, peer).await {
        Ok(response) => response,
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
