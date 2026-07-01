//! JSON-RPC reverse-proxy handler.
//!
//! Forwards a client request verbatim to the first ready upstream worker and
//! returns the upstream's response verbatim. When no upstream is ready, or the
//! upstream cannot be reached / times out, the client receives a well-formed
//! JSON-RPC error instead (see [`crate::error`]).

use axum::{
    body::{Body, Bytes},
    extract::{rejection::BytesRejection, State},
    http::{header, HeaderMap, HeaderValue, Method},
    response::Response,
};
use reqwest::Client;
use tracing::warn;
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

/// Forward a JSON-RPC request to the first ready upstream worker.
///
/// `body` is the final extractor (it consumes the request body), so it must
/// stay last in the parameter list.
pub(crate) async fn proxy(
    State(state): State<AppState>,
    method: Method,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    // A body over the `DefaultBodyLimit` is rejected before we can read it;
    // surface that as a JSON-RPC error rather than axum's plain-text 413.
    let body = match body {
        Ok(body) => body,
        Err(rejection) => {
            warn!(target: "gateway::proxy", %rejection, "rejecting oversized request body");
            return error_response(&GatewayError::RequestTooLarge, b"");
        }
    };

    let Some(rpc_url) = state.readiness.first_ready_rpc_url() else {
        warn!(target: "gateway::proxy", "no upstream worker ready; rejecting request");
        return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
    };

    match forward(&state.http, method, &headers, body.clone(), rpc_url).await {
        Ok(response) => response,
        Err(err) => {
            warn!(target: "gateway::proxy", ?err, "forwarding to upstream failed");
            error_response(&err, body.as_ref())
        }
    }
}

/// Forward one request to `rpc_url` and adapt the upstream response back into an
/// axum response, preserving the status body and content type.
async fn forward(
    client: &Client,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
    rpc_url: Url,
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
        .body(body)
        .send()
        .await
        .map_err(classify_error)?;

    let status = upstream.status();
    let upstream_content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let bytes = upstream.bytes().await.map_err(classify_error)?;

    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    if let Some(content_type) = upstream_content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    }
    Ok(response)
}

/// Classify a `reqwest` forwarding failure into a client-facing gateway error.
fn classify_error(err: reqwest::Error) -> GatewayError {
    if err.is_timeout() {
        GatewayError::UpstreamTimeout
    } else {
        GatewayError::UpstreamUnreachable
    }
}
