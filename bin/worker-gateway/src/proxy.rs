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

use std::{borrow::Cow, fmt, net::SocketAddr};

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
use serde::{
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserializer,
};
use tn_types::{Decodable2718, PooledTransaction};
use tracing::{debug, warn};
use url::Url;

use crate::{
    error::{error_response, error_response_with_id, GatewayError, RequestId},
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
    // for an upstream round-trip. The screen recovers the request id itself on
    // the paths that reject, so nothing here re-parses the body.
    if let Some((err, id)) = screen_raw_transaction(body.as_ref()) {
        warn!(target: "gateway::proxy", ?err, "rejecting eth_sendRawTransaction before forwarding");
        return error_response_with_id(&err, id);
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
    // timeout bounds a stalled *upstream* (hyper keeps polling the body while
    // its write buffer has room, so the timeout is observed) but not a
    // slow-reading *client*: under downstream backpressure hyper stops polling
    // the body and the poll-driven timeout never fires. That side is bounded
    // at the connection layer instead: `TCP_USER_TIMEOUT` plus the
    // connection-lifetime cap (see [`crate::server::accept_loop`]).
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
/// Returns `Some((error, id))` only when `body` is a single
/// `eth_sendRawTransaction` call whose raw transaction cannot be decoded, or
/// decodes to an EIP-4844 blob transaction (which the network does not accept).
/// Every other request — including batches, other methods, and any
/// structurally-off submission — returns `None` and is forwarded unchanged.
///
/// The `id` rides along with the rejection so the response path does not have
/// to parse the body again. Only the reject paths recover it, and they read it
/// member-by-member from the bytes; the forward path skips the `id` in place
/// like every other member the screen does not use, so a request that is
/// forwarded never materializes its id at all (see [`ScreenFields`]).
///
/// The decode uses the same pooled wire format the worker's RPC accepts and
/// never recovers the signer, so it cannot reject a transaction the worker
/// would have accepted (no false rejections); it only front-runs a rejection
/// the worker would issue anyway.
fn screen_raw_transaction(body: &[u8]) -> Option<(GatewayError, RequestId)> {
    // Fast path: skip JSON parsing entirely unless the method name is present.
    if !mentions_send_raw_transaction(body) {
        return None;
    }
    // Only the two members the screen reads are materialized; everything else
    // is skipped in place. Parsing into a `Value` here would build a tree several
    // times the size of the request, and the substring test above is satisfied by
    // the method name appearing anywhere (a string value, a key, padding), so the
    // request reaching this point is attacker-shaped and attacker-sized.
    let fields: ScreenFields = serde_json::from_slice(body).ok()?;
    // Single-call objects only; a batch (a JSON array) fails the map visitor and
    // is forwarded, left to the worker to validate per element.
    if fields.method.as_deref() != Some(SEND_RAW_TRANSACTION) {
        return None;
    }
    // A submission whose params are structurally off (missing / not a string)
    // is forwarded so the worker returns its own canonical parameter error.
    let raw_hex = fields.raw_transaction.as_deref()?;

    // From here the payload is unambiguously a raw transaction, so a decode
    // failure is a real rejection rather than a reason to forward. Only a
    // rejection echoes the id, and every rejection recovers it through the one
    // call site below: one more scan of bytes the screen has already read once.
    // The forward path, which is where an attacker-shaped request lands after
    // costing the screen its parse, allocates nothing for the id.
    let raw_tx_error = decode_hex(raw_hex).map_or(Some(GatewayError::InvalidTransaction), |raw| {
        let mut buf = raw.as_slice();
        match PooledTransaction::decode_2718(&mut buf) {
            Err(_) => Some(GatewayError::InvalidTransaction),
            Ok(tx) if !tn_types::batch_allowlisted_tx_type(&tx) => {
                Some(GatewayError::UnsupportedTransactionType)
            }
            Ok(_) => None,
        }
    });
    raw_tx_error.map(|err| (err, RequestId::recover(body)))
}

/// Whether `body` is valid UTF-8 mentioning the raw-transaction method. JSON is
/// UTF-8 by definition, so a non-UTF-8 body is not a JSON-RPC call we inspect.
fn mentions_send_raw_transaction(body: &[u8]) -> bool {
    std::str::from_utf8(body).is_ok_and(|text| text.contains(SEND_RAW_TRANSACTION))
}

/// The members [`screen_raw_transaction`] reads, materialized without building a
/// `Value` tree for the whole request.
///
/// Same technique, and the same reason, as [`RequestId::recover`]'s `IdMember`
/// in [`crate::error`]: every member the screen does not read deserializes into
/// serde's ignored-value sink, which skips it in place. Cost is a scan of the
/// bytes plus the raw-transaction string itself, rather than a `Value` tree
/// several times the size of the request.
///
/// The `id` is deliberately not among the members kept. Materializing it here
/// would keep the amplification this type exists to remove, just moved into
/// one member: all of a request's bulk can sit in `id`, and it would be built
/// eagerly even for a request that is then forwarded and the id dropped
/// unused. The reject paths, the only readers of the id, recover it from the
/// bytes instead (see [`RequestId::recover`]).
struct ScreenFields {
    /// Top-level `method`, when it is a string.
    method: Option<String>,
    /// First element of `params`, when `params` is an array whose first element
    /// is a string. This is the only unbounded member kept, and it is kept at
    /// its wire size: it is the hex payload the screen exists to decode.
    raw_transaction: Option<String>,
}

impl<'de> serde::Deserialize<'de> for ScreenFields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ScreenFieldsVisitor)
    }
}

/// Visitor behind [`ScreenFields`].
struct ScreenFieldsVisitor;

impl<'de> Visitor<'de> for ScreenFieldsVisitor {
    type Value = ScreenFields;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC request object")
    }

    /// Accepts an object only, so a batch (a JSON array) is a type error here
    /// and the caller forwards it, which is what the previous `Value`-based
    /// screen did when `get("method")` came back `None` on an array.
    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<Self::Value, A::Error> {
        let mut fields = ScreenFields { method: None, raw_transaction: None };
        // A repeated member keeps the last occurrence, which is how a full parse
        // into `Value` resolves a duplicated key.
        while let Some(member) = members.next_key::<Cow<'_, str>>()? {
            match member.as_ref() {
                "method" => fields.method = members.next_value::<MaybeString>()?.into_option(),
                "params" => {
                    fields.raw_transaction = members.next_value::<FirstParam>()?.0;
                }
                _ => {
                    members.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(fields)
    }
}

/// A `params` array reduced to its first element when that element is a string.
///
/// The remaining elements are drained through the ignored-value sink, so a
/// `params` array of any length costs a scan rather than an allocation per
/// element.
struct FirstParam(Option<String>);

impl<'de> serde::Deserialize<'de> for FirstParam {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(FirstParamVisitor)
    }
}

struct FirstParamVisitor;

impl<'de> Visitor<'de> for FirstParamVisitor {
    type Value = FirstParam;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC params array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Self::Value, A::Error> {
        let first = elements.next_element::<MaybeString>()?.and_then(MaybeString::into_option);
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(FirstParam(first))
    }

    /// `params` given as an object (or anything else) has no first element, and
    /// the previous screen's `params.get(0)` returned `None` for those too.
    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<Self::Value, A::Error> {
        while members.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(FirstParam(None))
    }

    serde::forward_to_deserialize_any! {}
}

/// A value kept only when it is a string, with any other shape consumed and
/// discarded rather than erroring.
///
/// The previous screen read `method` and `params[0]` through `Value::as_str`,
/// which yields `None` for a non-string without failing the parse. This
/// reproduces that: a non-string in either position leaves the field unset and
/// the request is forwarded, instead of the whole screen bailing out.
enum MaybeString {
    Str(String),
    Other,
}

impl MaybeString {
    fn into_option(self) -> Option<String> {
        match self {
            Self::Str(value) => Some(value),
            Self::Other => None,
        }
    }
}

impl<'de> serde::Deserialize<'de> for MaybeString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(MaybeStringVisitor)
    }
}

struct MaybeStringVisitor;

impl<'de> Visitor<'de> for MaybeStringVisitor {
    type Value = MaybeString;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(MaybeString::Str(value.to_owned()))
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Self::Value, E> {
        Ok(MaybeString::Str(value))
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(MaybeString::Other)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    /// Nested containers are drained through the ignored-value sink so an
    /// oversized non-string member costs a scan, not an allocation per node.
    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Self::Value, A::Error> {
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(MaybeString::Other)
    }

    fn visit_map<A: MapAccess<'de>>(self, mut members: A) -> Result<Self::Value, A::Error> {
        while members.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(MaybeString::Other)
    }
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
    use alloy::consensus::TxEip7702;
    use serde_json::Value;
    use tn_types::{Encodable2718, EthSignature, SignableTransaction, U256};

    /// The canonical EIP-155 example transaction (a signed legacy transfer): a
    /// well-formed, non-blob raw transaction that must be forwarded untouched.
    const EIP155_LEGACY_TX: &str = "0xf86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83";

    /// The hex of a genuine, decodable transaction of a type outside the batch
    /// allowlist: EIP-7702 (type 4) is the cheapest such type, needing no
    /// sidecar. Built from a default body and a dummy signature, since
    /// `decode_2718` checks structure, not signature validity.
    fn eip7702_raw_hex() -> String {
        let signature = EthSignature::new(U256::from(1), U256::from(1), false);
        let signed = TxEip7702::default().into_signed(signature);
        let encoded = PooledTransaction::Eip7702(signed).encoded_2718();
        format!("0x{}", tn_types::hex::encode(encoded))
    }

    fn send_raw(params: &str) -> Vec<u8> {
        format!(r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":{params},"id":1}}"#)
            .into_bytes()
    }

    /// The screen's verdict alone, for the cases that do not assert on the id.
    fn screen_err(body: &[u8]) -> Option<GatewayError> {
        screen_raw_transaction(body).map(|(err, _)| err)
    }

    /// A comparable projection of a screen verdict. `GatewayError` is not
    /// `PartialEq` and widening it just for a test is not worth it, so compare
    /// its `Debug` rendering alongside the id, which is `PartialEq`.
    fn verdict(result: Option<(GatewayError, RequestId)>) -> Option<(String, RequestId)> {
        result.map(|(err, id)| (format!("{err:?}"), id))
    }

    /// The extraction this fix replaced, verbatim, kept as the reference the new
    /// member-by-member reader is checked against. Only the extraction differs;
    /// the decode and verdict below it are the same code in both paths.
    fn reference_screen(body: &[u8]) -> Option<(GatewayError, RequestId)> {
        if !mentions_send_raw_transaction(body) {
            return None;
        }
        let request: Value = serde_json::from_slice(body).ok()?;
        if request.get("method").and_then(Value::as_str) != Some(SEND_RAW_TRANSACTION) {
            return None;
        }
        let raw_hex =
            request.get("params").and_then(|params| params.get(0)).and_then(Value::as_str)?;
        let id = RequestId::from_id(request.get("id").cloned().unwrap_or(Value::Null));
        let Some(raw) = decode_hex(raw_hex) else {
            return Some((GatewayError::InvalidTransaction, id));
        };
        let mut buf = raw.as_slice();
        match PooledTransaction::decode_2718(&mut buf) {
            Err(_) => Some((GatewayError::InvalidTransaction, id)),
            Ok(tx) if !tn_types::batch_allowlisted_tx_type(&tx) => {
                Some((GatewayError::UnsupportedTransactionType, id))
            }
            Ok(_) => None,
        }
    }

    /// The new reader must agree with the old `Value` parse on every shape, so
    /// the fix is a memory change and not a behaviour change. In particular it
    /// must not start rejecting anything it used to forward. The single
    /// documented exception is an `id` nested past serde_json's recursion
    /// limit, pinned by
    /// [`deeply_nested_id_rejects_locally_where_the_old_parse_forwarded`].
    #[test]
    fn extraction_matches_the_previous_value_parse() {
        let valid = format!("[\"{EIP155_LEGACY_TX}\"]");
        let bodies: Vec<Vec<u8>> = vec![
            // Ordinary calls, valid and invalid.
            send_raw(&valid),
            send_raw(r#"["0xdeadbeef"]"#),
            send_raw(r#"["not-hex"]"#),
            // Decodes cleanly, but to a type outside the batch allowlist.
            send_raw(&format!("[\"{}\"]", eip7702_raw_hex())),
            send_raw("[]"),
            send_raw(r#"[123]"#),
            send_raw(r#"[null]"#),
            send_raw(r#"[{"nested":"object"}]"#),
            send_raw(r#"[["nested","array"]]"#),
            send_raw(r#"{"not":"an array"}"#),
            // Extra and reordered members, and a params array with trailing junk.
            format!(
                r#"{{"extra":{{"deep":[1,2,3]}},"method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}",{{"x":1}}],"id":"abc"}}"#
            )
            .into_bytes(),
            format!(
                r#"{{"params":["{EIP155_LEGACY_TX}"],"id":null,"method":"eth_sendRawTransaction"}}"#
            )
            .into_bytes(),
            // Missing / non-string / duplicated members.
            br#"{"method":"eth_sendRawTransaction"}"#.to_vec(),
            br#"{"method":123,"params":["0xdeadbeef"],"id":1}"#.to_vec(),
            br#"{"method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":1,"id":2}"#.to_vec(),
            format!(
                r#"{{"method":"eth_chainId","method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":1}}"#
            )
            .into_bytes(),
            // Structured ids, on the reject and the forward verdict. The screen
            // no longer reads the id while parsing; these prove the id it
            // recovers on rejection is still the one the old parse echoed.
            br#"{"method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":[1,2,3]}"#.to_vec(),
            br#"{"method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":{"n":{"id":7}}}"#
                .to_vec(),
            format!(
                r#"{{"method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":[1,2,3]}}"#
            )
            .into_bytes(),
            // Not an object: a batch, and a bare string mentioning the method.
            format!(r#"[{{"method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"]}}]"#)
                .into_bytes(),
            br#""eth_sendRawTransaction""#.to_vec(),
            // The method name present only as data, never as the method.
            br#"{"method":"eth_call","params":["eth_sendRawTransaction"],"id":1}"#.to_vec(),
            // Malformed JSON that still trips the substring test.
            br#"{"method":"eth_sendRawTransaction","params":["#.to_vec(),
            b"eth_sendRawTransaction".to_vec(),
            // Nothing to do with the screen at all.
            br#"{"method":"eth_chainId","params":[],"id":1}"#.to_vec(),
        ];

        for body in bodies {
            assert_eq!(
                verdict(screen_raw_transaction(&body)),
                verdict(reference_screen(&body)),
                "screen disagreed with the previous parse on: {}",
                String::from_utf8_lossy(&body)
            );
        }
    }

    /// The one documented divergence from the previous `Value` parse, pinned
    /// deliberately rather than fixed. serde_json caps `Value` deserialization
    /// at 128 frames of recursion, so under the old screen an `id` nested past
    /// that limit failed the whole `ScreenFields` parse and the request was
    /// forwarded regardless of its transaction: a forward by accident of the
    /// recursion limit, not by a verdict. The new reader skips the id
    /// iteratively, with no depth bound, so the transaction is now screened on
    /// its merits and a reject-worthy payload is rejected locally.
    /// `RequestId::recover` materializes the id as a `Value` and hits the same
    /// limit, so the rejection echoes `null`. Only bodies the worker would
    /// reject anyway change verdict; a valid transaction forwards under both
    /// readers, so the no-false-rejection invariant is unchanged.
    #[test]
    fn deeply_nested_id_rejects_locally_where_the_old_parse_forwarded() {
        let deep_id = format!("{}0{}", "[".repeat(200), "]".repeat(200));
        let body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":{deep_id}}}"#
        )
        .into_bytes();

        // The old parse forwarded by accident: the deep id failed the whole
        // `Value` parse before any verdict was reached.
        assert_eq!(verdict(reference_screen(&body)), None);

        // The new reader skips the id, rejects the undecodable transaction,
        // and id recovery falls back to `null` at the same recursion limit.
        let (err, id) = screen_raw_transaction(&body).expect("undecodable tx must be rejected");
        assert!(matches!(err, GatewayError::InvalidTransaction));
        assert_eq!(id, RequestId::from_id(Value::Null));

        // The same deep id on a valid transaction: forwarded by both readers.
        let valid_body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":{deep_id}}}"#
        )
        .into_bytes();
        assert_eq!(verdict(screen_raw_transaction(&valid_body)), None);
        assert_eq!(verdict(reference_screen(&valid_body)), None);
    }

    /// A body whose only mention of the method is inside a huge unrelated member
    /// is the payload this fix exists for: it clears the substring test, so it
    /// reaches the parse, and under the previous code that parse built a `Value`
    /// tree an order of magnitude larger than the request. It must still reach
    /// the same verdict (forwarded, since `method` is not the raw-transaction
    /// call) without materializing the tree.
    #[test]
    fn oversized_unrelated_member_is_handled_without_building_a_tree() {
        let filler = "1,".repeat(400_000);
        let body = format!(
            r#"{{"method":"eth_chainId","note":"eth_sendRawTransaction","junk":[{}0],"id":1}}"#,
            filler
        )
        .into_bytes();
        assert!(body.len() > 800_000, "fixture should be large: {}", body.len());

        assert_eq!(verdict(screen_raw_transaction(&body)), None);
        assert_eq!(verdict(screen_raw_transaction(&body)), verdict(reference_screen(&body)));
    }

    /// The review payload for this commit: all of the bulk in `id`, past the
    /// substring gate, on requests that are then forwarded. Under the previous
    /// reader `id` was the one member still materialized as a full `Value`, so
    /// this shape rebuilt the amplification the screen fix removed, eagerly,
    /// for an id that was then dropped unused. The verdicts must still match
    /// the old parse; the id is never touched on the forward path.
    #[test]
    fn oversized_id_is_skipped_on_the_forward_path() {
        let filler = "1,".repeat(400_000);
        let bodies = [
            // The method name only as data: forwarded without reading the id.
            format!(r#"{{"x":"eth_sendRawTransaction","id":[{filler}0]}}"#).into_bytes(),
            // A real, valid submission: forwarded on its merits, id unread.
            format!(
                r#"{{"method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":[{filler}0]}}"#
            )
            .into_bytes(),
        ];
        bodies.iter().for_each(|body| {
            assert!(body.len() > 800_000, "fixture should be large: {}", body.len());
            assert_eq!(verdict(screen_raw_transaction(body)), None);
            assert_eq!(verdict(screen_raw_transaction(body)), verdict(reference_screen(body)));
        });
    }

    /// The same shape, but a real submission carrying a large trailing params
    /// array: the transaction must still be screened and rejected on its merits.
    #[test]
    fn oversized_params_tail_does_not_stop_the_screen() {
        let filler = ",\"pad\"".repeat(200_000);
        let body = format!(
            r#"{{"method":"eth_sendRawTransaction","params":["0xdeadbeef"{}],"id":7}}"#,
            filler
        )
        .into_bytes();

        let (err, id) = screen_raw_transaction(&body).expect("undecodable tx must be rejected");
        assert!(matches!(err, GatewayError::InvalidTransaction));
        assert_eq!(id, RequestId::from_id(serde_json::json!(7)));
    }

    #[test]
    fn valid_legacy_transaction_is_forwarded() {
        assert!(screen_err(&send_raw(&format!("[\"{EIP155_LEGACY_TX}\"]"))).is_none());
    }

    #[test]
    fn undecodable_transaction_is_rejected() {
        // Valid hex, but not a decodable transaction envelope.
        let err = screen_err(&send_raw(r#"["0xdeadbeef"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    #[test]
    fn blob_typed_payload_is_not_forwarded() {
        // A type-`0x03` (EIP-4844) prefix with a truncated body cannot decode as
        // a pooled transaction, so it is rejected rather than forwarded. Real
        // blob submissions decode and hit the `is_eip4844` reject; either way a
        // blob-typed payload never reaches an upstream.
        let err = screen_err(&send_raw(r#"["0x03c0"]"#));
        assert!(err.is_some());
    }

    /// The previously untested reject arm: a payload that decodes cleanly but
    /// to a type outside the batch allowlist (legacy / EIP-2930 / EIP-1559)
    /// must be rejected as unsupported, with its id, not as undecodable. The
    /// old parse rejected it the same way, so the fixture rides the
    /// equivalence corpus too.
    #[test]
    fn decodable_but_disallowed_tx_type_is_rejected_with_its_id() {
        let body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{}"],"id":42}}"#,
            eip7702_raw_hex()
        )
        .into_bytes();

        let (err, id) = screen_raw_transaction(&body).expect("disallowed type must be rejected");
        assert!(matches!(err, GatewayError::UnsupportedTransactionType));
        assert_eq!(id, RequestId::from_id(serde_json::json!(42)));
        assert_eq!(verdict(screen_raw_transaction(&body)), verdict(reference_screen(&body)));
    }

    #[test]
    fn non_hex_param_is_rejected() {
        let err = screen_err(&send_raw(r#"["not-hex"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    #[test]
    fn rejection_carries_the_id_a_re_parse_would_have_recovered() {
        // The point of threading the id out of the screen: the client must see
        // exactly the id that re-parsing the body would have produced, across
        // every id shape a submission can carry.
        let bodies = [
            send_raw(r#"["0xdeadbeef"]"#),
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["not-hex"],"id":"tx-7"}"#.to_vec(),
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"]}"#
                .to_vec(),
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":null}"#.to_vec(),
            br#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":[7,8]}"#.to_vec(),
        ];
        bodies.iter().for_each(|body| {
            let (_, id) = screen_raw_transaction(body).expect("rejected");
            assert_eq!(id, RequestId::recover(body), "{}", String::from_utf8_lossy(body));
        });
    }

    #[test]
    fn rejection_id_survives_a_payload_serialized_before_it() {
        // `id` after a large `params` is the ordering that rules out recovering
        // it from a bounded prefix of the body; the reused parse is unaffected.
        let payload = format!("0xdead{}", "beef".repeat(16 * 1024));
        let body = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{payload}"],"id":31}}"#
        )
        .into_bytes();
        let (err, id) = screen_raw_transaction(&body).expect("rejected");
        assert!(matches!(err, GatewayError::InvalidTransaction));
        assert_eq!(id, RequestId::recover(&body));
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
