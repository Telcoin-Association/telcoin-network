//! Well-formed JSON-RPC error responses for gateway-side failures.
//!
//! When the gateway itself cannot serve a request (no ready upstream, upstream
//! unreachable, upstream timed out) it must answer with a JSON-RPC 2.0 error
//! object rather than a bare connection reset, so clients see a structured
//! failure they can handle. The same envelope replaces an upstream's error
//! answer whose body is not JSON (see [`upstream_error_response`]).

use std::{borrow::Cow, fmt};

use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde::{
    de::{IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::{json, Value};

/// Longest string `id` that an error response echoes, in bytes of its decoded
/// UTF-8 text (after JSON escapes are resolved, not as written on the wire).
///
/// JSON-RPC ids are short correlation tokens (a counter, a UUID), so a longer
/// string is not one a client needs back. Like an array, an object or a
/// boolean, which the spec does not allow as an id, it echoes as `null`, so
/// the id a reject path keeps is never larger than this, whatever the client
/// sends.
const MAX_ID_BYTES: usize = 256;

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
    /// The upstream (a worker, or the query upstream) could not be reached.
    pub(super) const UPSTREAM_UNREACHABLE: i32 = -32001;
    /// The upstream did not answer within the request deadline.
    pub(super) const UPSTREAM_TIMEOUT: i32 = -32002;
    /// The request body exceeded the gateway's size limit, or a batch its
    /// length limit.
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
    /// network does not accept (an EIP-4844 blob or EIP-7702 set-code
    /// transaction).
    pub(super) const UNSUPPORTED_TRANSACTION_TYPE: i32 = -32008;
    /// Every in-flight slot the request could use is taken (its route's cap,
    /// or the cap on concurrent requests to the worker).
    pub(super) const OVERLOADED: i32 = -32009;
    /// The request did not fit in what is left of the gateway's in-flight
    /// request-byte budget (`--max-inflight-request-bytes`).
    pub(super) const INFLIGHT_BYTES_EXHAUSTED: i32 = -32010;
    /// The request calls a method the gateway refuses to forward
    /// (`--denied-method-prefixes`, applied when `--redirect-queries` is unset).
    pub(super) const METHOD_NOT_ALLOWED: i32 = -32011;
    /// The upstream answered with an error status and a body that is not
    /// JSON (a worker's `429` text/plain, for example).
    pub(super) const UPSTREAM_ERROR: i32 = -32012;
    /// The worker answered a remembered submission `2xx`, but its answer
    /// could not be read whole; the transaction may have been accepted.
    pub(super) const UPSTREAM_ANSWER_LOST: i32 = -32013;
    /// The request body could not be read, or the request was not a `POST`.
    /// This is the spec-defined "Invalid Request" code, not a gateway-range
    /// code.
    pub(super) const INVALID_REQUEST: i32 = -32600;
}

/// The EIP-2718 type byte of an EIP-4844 blob transaction.
const EIP4844_TX_TYPE: u8 = 3;

/// The EIP-2718 type byte of an EIP-7702 set-code transaction.
const EIP7702_TX_TYPE: u8 = 4;

/// A gateway-side failure surfaced to the client as a JSON-RPC error.
#[derive(Debug)]
pub(crate) enum GatewayError {
    /// No upstream worker was ready per the readiness poller.
    NoUpstreamReady,
    /// The upstream (a worker, or the query upstream) could not be reached
    /// (connection failure).
    UpstreamUnreachable,
    /// The upstream did not answer within the request deadline.
    UpstreamTimeout,
    /// The worker answered `2xx` but its answer to a remembered submission
    /// could not be read whole; the transaction may have been accepted.
    UpstreamAnswerLost,
    /// The request body exceeded the gateway's size limit.
    RequestTooLarge,
    /// A batch carried more calls than `--max-batch-len` allows.
    BatchTooLong,
    /// The request already carried the gateway's hop marker (forwarding loop).
    LoopDetected,
    /// The request carried the query-redirect marker into a gateway that
    /// itself redirects queries: a query some gateway already redirected,
    /// because a `--redirect-queries` URL leads back to a redirecting gateway
    /// or two redirecting gateways are chained.
    RedirectLoopDetected,
    /// The request did not complete within the gateway's request deadline.
    RequestTimeout {
        /// Whether the gateway had started sending the request upstream when
        /// the deadline fired. If it had, the upstream may have received and
        /// acted on it, so the client cannot treat the error as "not
        /// delivered".
        forwarding_started: bool,
    },
    /// The client exceeded the gateway's per-IP or global rate limit.
    RateLimited,
    /// An `eth_sendRawTransaction` payload could not be decoded as a
    /// transaction (malformed hex or RLP).
    InvalidTransaction,
    /// An `eth_sendRawTransaction` payload decoded to a transaction type the
    /// network does not accept (an EIP-4844 blob or EIP-7702 set-code
    /// transaction). Carries the EIP-2718 type byte, which the message names.
    UnsupportedTransactionType(u8),
    /// The request's body bytes did not fit in what is left of the in-flight
    /// request-byte budget, so it was refused before its body was read.
    InflightBytesExhausted,
    /// The request calls a method whose name starts with a denied prefix
    /// (`--denied-method-prefixes`, applied when `--redirect-queries` is unset).
    MethodNotAllowed,
    /// The request body could not be read (e.g. the client aborted mid-body).
    /// Also answers a batch the gateway cannot read to its end, whose length,
    /// and so its cost, is unknown.
    UnreadableBody,
    /// Every in-flight slot the request could use is taken. The gateway
    /// answers at once instead of queueing, so one saturated route cannot
    /// hold the connection slots the other route needs.
    Overloaded,
    /// The upstream answered with a non-2xx `status` and a body that is not
    /// JSON, so it carries no JSON-RPC answer to relay. The client gets the
    /// upstream's status with a JSON-RPC error in place of the body.
    UpstreamError {
        /// The upstream's HTTP status, passed through to the client.
        status: StatusCode,
    },
    /// The request's HTTP method was not `POST`. JSON-RPC over HTTP is
    /// `POST` only, so the request is answered locally, without reading its
    /// body or forwarding it.
    NonPostMethod,
}

/// The `Retry-After` value, in seconds, on the capacity `503`s
/// ([`GatewayError::Overloaded`] and [`GatewayError::InflightBytesExhausted`]).
/// Both free up as soon as a request in flight finishes, so a short fixed wait
/// is enough, and a client or proxy that honours the header backs off instead
/// of retrying at once against the cap.
const CAPACITY_RETRY_AFTER_SECS: &str = "1";

impl GatewayError {
    /// The `Retry-After` header this error's response carries: only the
    /// capacity `503`s, whose messages ask the client to retry.
    fn retry_after(&self) -> Option<HeaderValue> {
        matches!(self, Self::Overloaded | Self::InflightBytesExhausted)
            .then(|| HeaderValue::from_static(CAPACITY_RETRY_AFTER_SECS))
    }

    /// The HTTP status paired with this error.
    fn status(&self) -> StatusCode {
        match self {
            Self::NoUpstreamReady => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamUnreachable => StatusCode::BAD_GATEWAY,
            Self::UpstreamTimeout => StatusCode::GATEWAY_TIMEOUT,
            Self::UpstreamAnswerLost => StatusCode::BAD_GATEWAY,
            Self::RequestTooLarge | Self::BatchTooLong => StatusCode::PAYLOAD_TOO_LARGE,
            Self::LoopDetected | Self::RedirectLoopDetected => StatusCode::LOOP_DETECTED,
            Self::RequestTimeout { .. } => StatusCode::REQUEST_TIMEOUT,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidTransaction | Self::UnsupportedTransactionType(_) => {
                StatusCode::BAD_REQUEST
            }
            Self::InflightBytesExhausted => StatusCode::SERVICE_UNAVAILABLE,
            Self::MethodNotAllowed => StatusCode::FORBIDDEN,
            Self::UnreadableBody => StatusCode::BAD_REQUEST,
            Self::Overloaded => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamError { status } => *status,
            Self::NonPostMethod => StatusCode::METHOD_NOT_ALLOWED,
        }
    }

    /// The JSON-RPC error code paired with this error.
    fn code(&self) -> i32 {
        match self {
            Self::NoUpstreamReady => code::NO_UPSTREAM_READY,
            Self::UpstreamUnreachable => code::UPSTREAM_UNREACHABLE,
            Self::UpstreamTimeout => code::UPSTREAM_TIMEOUT,
            Self::UpstreamAnswerLost => code::UPSTREAM_ANSWER_LOST,
            Self::RequestTooLarge | Self::BatchTooLong => code::REQUEST_TOO_LARGE,
            Self::LoopDetected | Self::RedirectLoopDetected => code::LOOP_DETECTED,
            Self::RequestTimeout { .. } => code::REQUEST_TIMEOUT,
            Self::RateLimited => code::RATE_LIMITED,
            Self::InvalidTransaction => code::INVALID_TRANSACTION,
            Self::UnsupportedTransactionType(_) => code::UNSUPPORTED_TRANSACTION_TYPE,
            Self::InflightBytesExhausted => code::INFLIGHT_BYTES_EXHAUSTED,
            Self::MethodNotAllowed => code::METHOD_NOT_ALLOWED,
            Self::UnreadableBody | Self::NonPostMethod => code::INVALID_REQUEST,
            Self::Overloaded => code::OVERLOADED,
            Self::UpstreamError { .. } => code::UPSTREAM_ERROR,
        }
    }

    /// A human-readable message paired with this error. Every message is
    /// static except the one for a transaction type without a name here,
    /// which the screen cannot produce today (it refuses only types 3 and 4),
    /// and the upstream error's, which names the upstream's status.
    fn message(&self) -> Cow<'static, str> {
        let message = match self {
            Self::NoUpstreamReady => "no upstream worker is ready",
            Self::UpstreamUnreachable => "upstream unreachable",
            Self::UpstreamTimeout => "upstream request timed out",
            Self::UpstreamAnswerLost => {
                "upstream answered, but its answer was lost; the transaction may have been \
                 accepted, so look it up by hash before sending it again"
            }
            Self::RequestTooLarge => "request body too large",
            Self::BatchTooLong => "batch too long",
            Self::LoopDetected => {
                "proxy loop detected: request already passed through a worker gateway"
            }
            Self::RedirectLoopDetected => {
                "redirect loop detected: request was already redirected by a worker gateway"
            }
            Self::RequestTimeout { forwarding_started: false } => {
                "request did not complete within the gateway's deadline"
            }
            Self::RequestTimeout { forwarding_started: true } => {
                "request did not complete within the gateway's deadline; the request may have \
                 reached the upstream"
            }
            Self::RateLimited => "rate limit exceeded; slow down and retry",
            Self::InvalidTransaction => "raw transaction could not be decoded",
            Self::UnsupportedTransactionType(EIP4844_TX_TYPE) => {
                "unsupported transaction type: EIP-4844 blob transactions are not accepted"
            }
            Self::UnsupportedTransactionType(EIP7702_TX_TYPE) => {
                "unsupported transaction type: EIP-7702 set-code transactions are not accepted"
            }
            Self::UnsupportedTransactionType(ty) => {
                return Cow::Owned(format!(
                    "unsupported transaction type: type {ty:#04x} transactions are not accepted"
                ));
            }
            Self::InflightBytesExhausted => {
                "gateway is holding too many request bytes; retry shortly"
            }
            Self::MethodNotAllowed => "method not allowed through this gateway",
            Self::UnreadableBody => "request body could not be read",
            Self::Overloaded => "gateway overloaded; retry later",
            Self::UpstreamError { status } => {
                return format!("upstream returned {} without a JSON-RPC body", status.as_u16())
                    .into();
            }
            Self::NonPostMethod => "only POST is accepted",
        };
        Cow::Borrowed(message)
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
            Self::UpstreamAnswerLost => "upstream_answer_lost",
            Self::RequestTooLarge => "request_too_large",
            Self::BatchTooLong => "batch_too_long",
            Self::LoopDetected => "loop_detected",
            Self::RedirectLoopDetected => "redirect_loop_detected",
            Self::RequestTimeout { .. } => "request_timeout",
            Self::RateLimited => "rate_limited",
            Self::InvalidTransaction => "invalid_transaction",
            Self::UnsupportedTransactionType(_) => "unsupported_transaction_type",
            Self::InflightBytesExhausted => "inflight_bytes_exhausted",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::UnreadableBody => "unreadable_body",
            Self::Overloaded => "overloaded",
            Self::UpstreamError { .. } => "upstream_error",
            Self::NonPostMethod => "method_not_post",
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
    render(err, id)
}

/// Replace an upstream's non-JSON error answer with a JSON-RPC error that
/// keeps the upstream's `status` and echoes the request `id` recovered from
/// `request_body`.
///
/// The gateway did forward this request, so it is counted under its own
/// `upstream_error` outcome rather than as a rejection (see
/// [`crate::telemetry::record_upstream_error`]).
pub(crate) fn upstream_error_response(status: StatusCode, request_body: &[u8]) -> Response {
    let err = GatewayError::UpstreamError { status };
    crate::telemetry::record_upstream_error(err.reason());
    render(&err, RequestId::recover(request_body))
}

/// Render `err` as a JSON-RPC 2.0 error response echoing `id`, without
/// recording any metric.
fn render(err: &GatewayError, id: RequestId) -> Response {
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
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    if let Some(retry_after) = err.retry_after() {
        headers.insert(header::RETRY_AFTER, retry_after);
    }
    response
}

/// The JSON-RPC request `id` echoed back on an error response.
///
/// `null` is the "no id" case the spec mandates for a request whose id the
/// server could not read: a batch, a malformed body, or an id-less call. The
/// gateway also echoes `null` for an id outside the echo bound (see
/// [`BoundedId`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RequestId(Value);

impl RequestId {
    /// The top-level `id` of a request the caller has already parsed.
    ///
    /// Now only a test helper: it is the reference `id` extraction that
    /// [`Self::recover`] is checked against for ids within the echo bound (see
    /// [`BoundedId`]), since the screen no longer builds a `Value` for the
    /// request to take an id from. It applies no bound of its own.
    #[cfg(test)]
    pub(crate) fn from_request(request: &Value) -> Self {
        Self(request.get("id").cloned().unwrap_or(Value::Null))
    }

    /// The top-level `id` a caller extracted on its own.
    ///
    /// Now only a test helper, like [`Self::from_request`]: the raw-transaction
    /// screen skips the `id` member entirely while parsing and recovers it via
    /// [`Self::recover`] on its reject paths, so nothing outside tests holds an
    /// extracted id to wrap.
    #[cfg(test)]
    pub(crate) fn from_id(id: Value) -> Self {
        Self(id)
    }

    /// Best-effort recovery of the top-level `id` from an unparsed body.
    ///
    /// Only the `id` member is materialized: every other member deserializes
    /// into serde's ignored-value sink, which skips it in place. A multi-megabyte
    /// `params` therefore costs a scan of the bytes rather than a `Value` tree
    /// several times the size of the request.
    ///
    /// The `id` itself is kept only when it is `null`, a number serde_json can
    /// represent, or a string of at most [`MAX_ID_BYTES`] bytes (see
    /// [`BoundedId`]). An array, an object or a boolean is skipped in place like
    /// any other member. A longer string is parsed but not kept: one with
    /// escapes is decoded into the parser's scratch buffer, at most the size of
    /// the body, and dropped. Each recovers as `null`, so an attacker-sized `id`
    /// on a reject path costs at most one copy of its bytes, never a `Value`
    /// tree.
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

    /// Best-effort recovery of the `id` of the call at `index` in an unparsed
    /// batch body, read the way [`Self::recover`] reads a single call's.
    ///
    /// The elements before it are skipped in place and the ones after it
    /// drained, so this too costs one scan of the body and materializes only
    /// the one `id`. Anything that is not a batch with an object at `index`
    /// recovers `null`.
    pub(crate) fn recover_element(request_body: &[u8], index: usize) -> Self {
        let mut deserializer = serde_json::Deserializer::from_slice(request_body);
        (&mut deserializer)
            .deserialize_seq(ElementIdVisitor { index })
            .and_then(|id| deserializer.end().map(|()| Self(id)))
            .unwrap_or(Self(Value::Null))
    }
}

/// Visitor behind [`RequestId::recover_element`]: the `id` of the batch
/// element at `index`, every other element skipped in place.
struct ElementIdVisitor {
    /// The position of the element whose id is kept.
    index: usize,
}

impl<'de> Visitor<'de> for ElementIdVisitor {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC batch")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Self::Value, A::Error> {
        for _ in 0..self.index {
            if elements.next_element::<IgnoredAny>()?.is_none() {
                return Ok(Value::Null);
            }
        }
        let id = elements.next_element::<IdMember>()?.map_or(Value::Null, |IdMember(id)| id);
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(id)
    }
}

/// Deserialization shim that reads a request's `id` member within the echo
/// bound (see [`BoundedId`]) and skips every other member. See
/// [`RequestId::recover`].
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
        // The `id` goes through `BoundedId`, which never builds a tree either.
        // A repeated `id` keeps the last occurrence, which is how a full parse
        // into `Value` resolves a duplicated member.
        std::iter::from_fn(|| {
            members.next_key::<Cow<'_, str>>().transpose().map(|member| {
                member.and_then(|member| match member.as_ref() {
                    "id" => members.next_value::<BoundedId>().map(|id| Some(id.0)),
                    _ => members.next_value::<IgnoredAny>().map(|_| None),
                })
            })
        })
        .try_fold(Value::Null, |id, member| member.map(|found| found.unwrap_or(id)))
        .map(IdMember)
    }
}

/// A request `id` read within the bound an error response echoes: `null`, a
/// number, or a string of at most [`MAX_ID_BYTES`] bytes. Anything else reads
/// as `null`; an array or an object is drained into serde's ignored-value
/// sink rather than built.
struct BoundedId(Value);

impl<'de> Deserialize<'de> for BoundedId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(BoundedIdVisitor)
    }
}

/// Visitor behind [`BoundedId`].
struct BoundedIdVisitor;

impl<'de> Visitor<'de> for BoundedIdVisitor {
    type Value = BoundedId;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(BoundedId(Value::Null))
    }

    /// A boolean is not a valid JSON-RPC id.
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(BoundedId(Value::Null))
    }

    fn visit_i64<E>(self, id: i64) -> Result<Self::Value, E> {
        Ok(BoundedId(Value::from(id)))
    }

    fn visit_u64<E>(self, id: u64) -> Result<Self::Value, E> {
        Ok(BoundedId(Value::from(id)))
    }

    fn visit_f64<E>(self, id: f64) -> Result<Self::Value, E> {
        Ok(BoundedId(Value::from(id)))
    }

    fn visit_str<E>(self, id: &str) -> Result<Self::Value, E> {
        Ok(BoundedId(if id.len() <= MAX_ID_BYTES { Value::from(id) } else { Value::Null }))
    }

    fn visit_seq<A: SeqAccess<'de>>(self, elements: A) -> Result<Self::Value, A::Error> {
        IgnoredAny.visit_seq(elements).map(|_| BoundedId(Value::Null))
    }

    fn visit_map<A: MapAccess<'de>>(self, members: A) -> Result<Self::Value, A::Error> {
        IgnoredAny.visit_map(members).map(|_| BoundedId(Value::Null))
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
    fn recovers_the_id_of_a_batch_element() {
        let batch = br#"[{"id":1},{"method":"m","params":["0x00"],"id":"tx-2"},{"id":[3]},7]"#;
        let element = |index| RequestId::recover_element(batch, index).0;
        assert_eq!(element(0), json!(1));
        assert_eq!(element(1), json!("tx-2"));
        // a structured id is outside the echo bound (#1595), as for a single call
        assert_eq!(element(2), Value::Null);
        // not an object, past the end, and not a batch at all
        assert_eq!(element(3), Value::Null);
        assert_eq!(element(4), Value::Null);
        assert_eq!(RequestId::recover_element(br#"{"id":1}"#, 0).0, Value::Null);
        assert_eq!(RequestId::recover_element(br#"[{"id":1}] trailing"#, 0).0, Value::Null);
    }

    /// A batch element's id goes through the same bound as a single call's:
    /// a long string or a large array echoes `null`, and an element after one
    /// still recovers its own id.
    #[test]
    fn oversized_element_id_echoes_null() {
        let long = "a".repeat(MAX_ID_BYTES + 1);
        let array = format!("[{}0]", "0,".repeat(64 * 1024));
        let batch = format!(r#"[{{"id":"{long}"}},{{"id":{array}}},{{"id":"tx-3"}}]"#);
        let element = |index| RequestId::recover_element(batch.as_bytes(), index).0;
        assert_eq!(element(0), Value::Null);
        assert_eq!(element(1), Value::Null);
        assert_eq!(element(2), json!("tx-3"));
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
        // member order, duplicates, number and string encodings, non-object
        // bodies, and malformed input. Every id here is within the echo bound;
        // ids outside it echo `null` by design and are pinned on their own.
        fn full_parse(request_body: &[u8]) -> Value {
            serde_json::from_slice::<Value>(request_body)
                .ok()
                .and_then(|value| value.get("id").cloned())
                .unwrap_or(Value::Null)
        }

        let bodies: [&[u8]; 20] = [
            br#"{"jsonrpc":"2.0","method":"eth_chainId","id":7}"#,
            br#"{"id":7,"jsonrpc":"2.0","method":"eth_chainId"}"#,
            br#"{"method":"eth_sendRawTransaction","params":["0x00"],"id":"tx-1"}"#,
            br#"{"id":"abc"}"#,
            br#"{"id":"esc\"aped\u00e9"}"#,
            br#"{"id":0}"#,
            br#"{"id":-1}"#,
            br#"{"id":1.5}"#,
            br#"{"id":1e3}"#,
            br#"{"id":18446744073709551615}"#,
            br#"{"id":-9223372036854775808}"#,
            br#"{"id":null}"#,
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
        // parse has to echo exactly what a re-parse of the body would echo, for
        // every id within the echo bound.
        let bodies: [&[u8]; 5] = [
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":"tx-1"}"#,
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":0}"#,
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"]}"#,
            br#"{"id":null}"#,
            br#"{"id":1.5}"#,
        ];
        bodies.iter().for_each(|body| {
            let parsed: Value = serde_json::from_slice(body).expect("body parses");
            assert_eq!(RequestId::recover(body), RequestId::from_request(&parsed), "{body:?}");
        });
    }

    #[test]
    fn oversized_id_echoes_null() {
        let body =
            format!(r#"{{"jsonrpc":"2.0","method":"eth_chainId","id":"{}"}}"#, "a".repeat(300));
        assert_eq!(recovered(body.as_bytes()), Value::Null);
    }

    #[test]
    fn array_and_object_ids_echo_null() {
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":[0,0,0]}"#), Value::Null);
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":{"a":1}}"#), Value::Null);
    }

    #[test]
    fn boolean_id_echoes_null() {
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":true}"#), Value::Null);
    }

    #[test]
    fn string_id_at_the_limit_echoes_unchanged() {
        let at_limit = "a".repeat(MAX_ID_BYTES);
        let body = format!(r#"{{"method":"eth_chainId","id":"{at_limit}"}}"#);
        assert_eq!(recovered(body.as_bytes()), json!(at_limit));

        let over_limit = "a".repeat(MAX_ID_BYTES + 1);
        let body = format!(r#"{{"method":"eth_chainId","id":"{over_limit}"}}"#);
        assert_eq!(recovered(body.as_bytes()), Value::Null);
    }

    #[test]
    fn deeply_nested_id_is_skipped_without_a_tree() {
        // building this id as a `Value` hits serde_json's recursion limit and
        // fails the whole recovery; skipping it in place reaches the repeated
        // id after it, and the last id wins
        let nested = format!("{}{}", "[".repeat(200), "]".repeat(200));
        let body = format!(r#"{{"method":"eth_chainId","id":{nested},"id":5}}"#);
        assert_eq!(recovered(body.as_bytes()), json!(5));
    }

    #[test]
    fn string_id_bound_counts_decoded_utf8_bytes() {
        // a two-byte character counts twice
        let at_limit = "\u{e9}".repeat(MAX_ID_BYTES / 2);
        let body = format!(r#"{{"id":"{at_limit}"}}"#);
        assert_eq!(recovered(body.as_bytes()), json!(at_limit));
        let over_limit = "\u{e9}".repeat(MAX_ID_BYTES / 2 + 1);
        let body = format!(r#"{{"id":"{over_limit}"}}"#);
        assert_eq!(recovered(body.as_bytes()), Value::Null);

        // an escape counts as the byte it decodes to, not the six on the wire
        let escaped = r"\u0041".repeat(MAX_ID_BYTES);
        let body = format!(r#"{{"id":"{escaped}"}}"#);
        assert_eq!(recovered(body.as_bytes()), json!("A".repeat(MAX_ID_BYTES)));
        let escaped = r"\u0041".repeat(MAX_ID_BYTES + 1);
        let body = format!(r#"{{"id":"{escaped}"}}"#);
        assert_eq!(recovered(body.as_bytes()), Value::Null);
    }

    #[test]
    fn numeric_and_short_string_ids_echo_unchanged() {
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":1}"#), json!(1));
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":-5}"#), json!(-5));
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":1.5}"#), json!(1.5));
        assert_eq!(recovered(br#"{"method":"eth_chainId","id":"abc"}"#), json!("abc"));
    }

    #[test]
    fn oversized_id_after_large_params_echoes_null() {
        // the id trails 900 KiB of params, so recovery scans the whole body to
        // reach it and then has to skip it rather than build it
        let params = "a".repeat(900 * 1024);
        let id = format!("[{}0]", "0,".repeat(32 * 1024));
        let body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{params}"],"id":{id}}}"#
        );
        assert_eq!(recovered(body.as_bytes()), Value::Null);
    }

    /// A request of exactly `size` bytes whose `id` is an array of zeros
    /// filling everything but the envelope.
    fn array_id_body(size: usize) -> Vec<u8> {
        let head = r#"{"jsonrpc":"2.0","method":"eth_chainId","id":["#;
        let tail = "0]}";
        let fill = size - head.len() - tail.len();
        // each element is "0,"; an odd remainder becomes leading whitespace
        let body = format!("{}{head}{}{tail}", " ".repeat(fill % 2), "0,".repeat(fill / 2));
        body.into_bytes()
    }

    /// Peak RSS under the WG-02 load: 500 concurrent 1 MiB requests whose `id`
    /// is an array, sent with the hop marker so every one takes a reject path
    /// that recovers the id. Before the id was bounded, each request built a
    /// `Value` about 34 times the size of its `id` while it was rejected.
    /// The RSS bound alone depends on how many worker threads build an id at
    /// once; on unfixed code the `id: null` assertion fails on any host, since
    /// the hop-marker path echoed the whole array.
    ///
    /// It measures the whole process (client and server share it) and needs a
    /// quiet machine, so it is run by hand:
    /// `cargo nextest run -p tn-worker-gateway --run-ignored only --no-capture
    /// -E 'test(peak_rss_stays_under_the_manifest_limit_with_oversized_ids)'`.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "measures peak RSS of the test process; run by hand"]
    async fn peak_rss_stays_under_the_manifest_limit_with_oversized_ids() {
        use crate::{
            proxy::HOP_HEADER,
            readiness::{GatewayReadiness, ReadinessThresholds},
            server::{inflight_slots, router, AppState, RouterLayers},
        };
        use axum::body::Bytes;
        use std::{net::SocketAddr, num::NonZeroU32, sync::Arc, time::Duration};
        use tokio::{net::TcpListener, task::JoinSet};

        const MAX_REQUEST_BYTES: usize = 1024 * 1024;
        const REQUESTS: usize = 500;
        // the reference manifest's memory limit (1Gi), in the kB `VmHWM` uses
        const MANIFEST_LIMIT_KB: u64 = 1024 * 1024;

        let thresholds = ReadinessThresholds {
            failure: NonZeroU32::MIN,
            success: NonZeroU32::MIN,
            rpc_failure: None,
        };
        let state = AppState {
            readiness: Arc::new(GatewayReadiness::new(&[], thresholds)),
            http: reqwest::Client::new(),
            query_upstream: None,
            query_header: None,
            recent_submissions: None,
            denied_method_prefixes: Arc::from([]),
            max_batch_len: None,
            query_http: None,
            upstream_request_timeout: Duration::from_secs(5),
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
            pool_read_slots: inflight_slots(0),
            draining: Arc::default(),
        };
        let app = router(
            state,
            Duration::from_secs(60),
            MAX_REQUEST_BYTES,
            None,
            RouterLayers::default(),
            Arc::default(),
        );
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        tokio::spawn(async move {
            // serves until the runtime shuts down at the end of the test; a
            // serve failure shows up as failed requests below
            let _ = axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
                .await;
        });

        let body = Bytes::from(array_id_body(MAX_REQUEST_BYTES));
        assert_eq!(body.len(), MAX_REQUEST_BYTES);

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/");
        let mut requests = JoinSet::new();
        (0..REQUESTS).for_each(|_| {
            let request = client.post(&url).header(HOP_HEADER, "1").body(body.clone());
            requests.spawn(async move {
                let response = request.send().await.expect("send");
                let status = response.status();
                (status, response.text().await.expect("response body"))
            });
        });
        while let Some(joined) = requests.join_next().await {
            let (status, text) = joined.expect("request task");
            assert_eq!(status, StatusCode::LOOP_DETECTED, "{text}");
            let response: Value = serde_json::from_str(&text).expect("json error body");
            assert_eq!(response["id"], Value::Null);
        }

        let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
        let peak = status.lines().find(|line| line.starts_with("VmHWM:")).expect("VmHWM line");
        println!("{peak}");
        let peak_kb: u64 = peak
            .split_whitespace()
            .nth(1)
            .and_then(|kb| kb.parse().ok())
            .expect("VmHWM value in kB");
        assert!(
            peak_kb < MANIFEST_LIMIT_KB,
            "peak RSS {peak_kb} kB is not under the 1Gi reference limit"
        );
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
            error_response(&GatewayError::RedirectLoopDetected, b"{}").status(),
            StatusCode::LOOP_DETECTED
        );
        assert_eq!(
            error_response(&GatewayError::RequestTimeout { forwarding_started: false }, b"{}")
                .status(),
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
            error_response(&GatewayError::BatchTooLong, b"{}").status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            error_response(&GatewayError::InvalidTransaction, b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_response(&GatewayError::UnsupportedTransactionType(4), b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            error_response(&GatewayError::NonPostMethod, b"{}").status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }

    #[test]
    fn new_edge_protection_codes_are_stable() {
        // The codes are part of the client contract; pin them so a reorder or
        // renumber is caught.
        assert_eq!(GatewayError::BatchTooLong.code(), -32003);
        assert_eq!(GatewayError::RateLimited.code(), -32006);
        assert_eq!(GatewayError::InvalidTransaction.code(), -32007);
        assert_eq!(GatewayError::UnsupportedTransactionType(3).code(), -32008);
        assert_eq!(GatewayError::UnsupportedTransactionType(4).code(), -32008);
        assert_eq!(GatewayError::Overloaded.code(), -32009);
        assert_eq!(GatewayError::InflightBytesExhausted.code(), -32010);
        assert_eq!(GatewayError::MethodNotAllowed.code(), -32011);
        assert_eq!(GatewayError::UpstreamError { status: StatusCode::FORBIDDEN }.code(), -32012);
        assert_eq!(GatewayError::UpstreamAnswerLost.code(), -32013);
        assert_eq!(GatewayError::NonPostMethod.code(), -32600);
    }

    /// An over-length batch shares the "too large" code and status with an
    /// oversized body, so a client that already handles `413` / `-32003`
    /// handles it too, while its own reason label and message tell the two
    /// apart.
    #[tokio::test]
    async fn batch_too_long_code_and_reason_are_stable() {
        let err = GatewayError::BatchTooLong;
        assert_eq!((err.code(), err.reason()), (-32003, "batch_too_long"));
        assert_ne!(err.reason(), GatewayError::RequestTooLarge.reason());

        let response = error_response(&err, br#"[{"id":1}]"#);
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["error"]["code"], json!(-32003));
        assert_eq!(body["error"]["message"], json!("batch too long"));
        assert_eq!(body["id"], Value::Null);
    }

    /// WG-52: an EIP-7702 rejection used to be reported as "EIP-4844 blob".
    /// The message names the type that was refused; the code and reason stay
    /// one per variant, so dashboards and clients are unaffected.
    #[tokio::test]
    async fn rejected_type_is_named() {
        let message = |ty| GatewayError::UnsupportedTransactionType(ty).message();
        assert_eq!(
            message(4),
            "unsupported transaction type: EIP-7702 set-code transactions are not accepted"
        );
        assert_eq!(
            message(3),
            "unsupported transaction type: EIP-4844 blob transactions are not accepted"
        );
        assert_eq!(
            message(0x7f),
            "unsupported transaction type: type 0x7f transactions are not accepted"
        );

        let response = error_response_with_id(
            &GatewayError::UnsupportedTransactionType(4),
            RequestId(json!(1)),
        );
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["error"]["code"], json!(-32008));
        let rendered = body["error"]["message"].as_str().expect("message");
        assert!(rendered.contains("EIP-7702") && !rendered.contains("EIP-4844"), "{rendered}");
    }

    #[test]
    fn inflight_bytes_code_and_reason_are_stable() {
        // the budget refusal is a retryable overload signal with its own code
        // and metric reason, so clients and dashboards can tell it apart from
        // a rate limit or a missing upstream
        let err = GatewayError::InflightBytesExhausted;
        assert_eq!(err.code(), -32010);
        assert_eq!(err.reason(), "inflight_bytes_exhausted");
        assert_eq!(err.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error_response(&err, b"").status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn reason_labels_are_stable() {
        // The reason labels are the `tn_worker_gateway_rejections_total{reason}`
        // metric contract (dashboards and alerts key on them); pin every variant
        // so a rename or an arm-crossing reorder is caught.
        assert_eq!(GatewayError::NoUpstreamReady.reason(), "no_upstream_ready");
        assert_eq!(GatewayError::UpstreamUnreachable.reason(), "upstream_unreachable");
        assert_eq!(GatewayError::UpstreamTimeout.reason(), "upstream_timeout");
        assert_eq!(GatewayError::UpstreamAnswerLost.reason(), "upstream_answer_lost");
        assert_eq!(GatewayError::RequestTooLarge.reason(), "request_too_large");
        assert_eq!(GatewayError::BatchTooLong.reason(), "batch_too_long");
        assert_eq!(GatewayError::LoopDetected.reason(), "loop_detected");
        assert_eq!(GatewayError::RedirectLoopDetected.reason(), "redirect_loop_detected");
        assert_eq!(
            GatewayError::RequestTimeout { forwarding_started: false }.reason(),
            "request_timeout"
        );
        assert_eq!(
            GatewayError::RequestTimeout { forwarding_started: true }.reason(),
            "request_timeout"
        );
        assert_eq!(GatewayError::RateLimited.reason(), "rate_limited");
        assert_eq!(GatewayError::InvalidTransaction.reason(), "invalid_transaction");
        assert_eq!(
            GatewayError::UnsupportedTransactionType(4).reason(),
            "unsupported_transaction_type"
        );
        assert_eq!(GatewayError::InflightBytesExhausted.reason(), "inflight_bytes_exhausted");
        assert_eq!(GatewayError::MethodNotAllowed.reason(), "method_not_allowed");
        assert_eq!(GatewayError::UnreadableBody.reason(), "unreadable_body");
        assert_eq!(GatewayError::Overloaded.reason(), "overloaded");
        assert_eq!(
            GatewayError::UpstreamError { status: StatusCode::FORBIDDEN }.reason(),
            "upstream_error"
        );
        assert_eq!(GatewayError::NonPostMethod.reason(), "method_not_post");
    }

    #[tokio::test]
    async fn overloaded_code_and_reason_are_stable() {
        // clients back off on the 503 and dashboards key on the reason, so the
        // whole overload envelope is pinned, not just its code
        assert_eq!(GatewayError::Overloaded.code(), -32009);
        assert_eq!(GatewayError::Overloaded.reason(), "overloaded");

        let response = error_response(&GatewayError::Overloaded, br#"{"id":"slot-1"}"#);
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(body["error"]["code"], json!(-32009));
        assert_eq!(body["error"]["message"], json!("gateway overloaded; retry later"));
        assert_eq!(body["id"], json!("slot-1"));
    }

    #[test]
    fn capacity_503s_carry_retry_after() {
        // both capacity refusals ask the client to retry, so both say when
        for err in [GatewayError::Overloaded, GatewayError::InflightBytesExhausted] {
            let response = error_response(&err, b"");
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE, "{err:?}");
            assert_eq!(
                response.headers().get(header::RETRY_AFTER),
                Some(&HeaderValue::from_static("1")),
                "{err:?}"
            );
        }
        // the rate limit's 429, and the 503 for a missing upstream, are unchanged
        for err in [GatewayError::RateLimited, GatewayError::NoUpstreamReady] {
            let response = error_response(&err, b"");
            assert_eq!(response.headers().get(header::RETRY_AFTER), None, "{err:?}");
        }
    }

    #[tokio::test]
    async fn upstream_error_code_and_reason_are_stable() {
        // every upstream status maps to the one code and reason, and the
        // status itself passes through to the client
        for status in [
            StatusCode::FOUND,
            StatusCode::FORBIDDEN,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
        ] {
            let err = GatewayError::UpstreamError { status };
            assert_eq!((err.code(), err.reason()), (-32012, "upstream_error"), "{status}");

            let response = upstream_error_response(status, br#"{"id":"call-3"}"#);
            assert_eq!(response.status(), status);
            assert_eq!(
                response.headers().get(header::CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json"))
            );
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
            let body: Value = serde_json::from_slice(&bytes).expect("json");
            assert_eq!(body["error"]["code"], json!(-32012));
            assert_eq!(
                body["error"]["message"],
                json!(format!("upstream returned {} without a JSON-RPC body", status.as_u16()))
            );
            assert_eq!(body["id"], json!("call-3"));
        }
    }

    #[test]
    fn non_post_code_and_reason_are_stable() {
        // A non-POST request shares the spec's "Invalid Request" code with an
        // unreadable body; its status, message and reason label tell the two
        // apart, so pin all of them.
        let err = GatewayError::NonPostMethod;
        assert_eq!(err.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(err.code(), -32600);
        assert_eq!(err.message(), "only POST is accepted");
        assert_eq!(err.reason(), "method_not_post");
    }

    #[tokio::test]
    async fn method_not_allowed_code_and_reason_are_stable() {
        // code, status, message and reason are the client and metrics contract
        // for a refused method; pin all four, end to end through the renderer
        let err = GatewayError::MethodNotAllowed;
        assert_eq!(err.code(), -32011);
        assert_eq!(err.reason(), "method_not_allowed");

        let response = error_response(&err, br#"{"jsonrpc":"2.0","method":"tn_x","id":"q-1"}"#);
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.expect("body");
        let body: Value = serde_json::from_slice(&bytes).expect("json");
        assert_eq!(
            body,
            json!({
                "jsonrpc": "2.0",
                "error": { "code": -32011, "message": "method not allowed through this gateway" },
                "id": "q-1",
            })
        );
    }
}
