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
//!
//! With `--redirect-queries` set, only transaction submissions go to the
//! worker; every other call goes to the query upstream (see [`scan`]),
//! which is not readiness-gated, never falls back to the worker, and gets the
//! `X-TN-Gateway-Redirect` marker in place of `X-TN-Gateway`.

use std::{
    borrow::Cow, fmt, marker::PhantomData, net::SocketAddr, num::NonZeroUsize, sync::Arc,
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::{
        rejection::{BytesRejection, FailedToBufferBody},
        ConnectInfo, State,
    },
    http::{header, HeaderMap, HeaderName, HeaderValue, Method},
    response::Response,
    Extension,
};
use reqwest::{redirect::Policy, Client};
use serde::{
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::de::SliceRead;
use tn_types::{Decodable2718, PooledTransaction, Typed2718};
use tracing::{debug, warn};
use url::Url;

use crate::{
    error::{error_response, error_response_with_id, GatewayError, RequestId},
    ratelimit::RateLimiters,
    server::AppState,
    telemetry,
};

/// Default maximum request body the gateway will buffer before forwarding.
///
/// A guard against unbounded memory use; the effective limit is configurable
/// via `--max-request-bytes` (this value is that flag's default). 1 MiB is four
/// times the largest admissible submission: the worker's pool admits at most
/// 128 KiB of raw transaction (reth's `DEFAULT_MAX_TX_INPUT_BYTES`), about
/// 256 KiB once hex-encoded. Each open connection can buffer one body this
/// large, so peak request memory is roughly `--max-connections` times this value
/// (see the README's "Request size" section).
pub(crate) const MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// Default maximum number of calls in one JSON-RPC batch (`--max-batch-len`).
///
/// Neither the worker nor jsonrpsee bounds a batch's length, so without a cap
/// one request can carry as many calls as fit in the body: thousands in the
/// 1 MiB default, about 135k in the worker's own 15 MiB request cap. 50 is far
/// above what a wallet or an exchange batches in practice.
pub(crate) const DEFAULT_MAX_BATCH_LEN: usize = 50;

/// The one JSON-RPC method whose payload the gateway inspects before
/// forwarding (a raw-transaction submission).
const SEND_RAW_TRANSACTION: &str = "eth_sendRawTransaction";

/// The raw-transaction submission that waits for the receipt.
const SEND_RAW_TRANSACTION_SYNC: &str = "eth_sendRawTransactionSync";

/// The JSON-RPC methods that submit a transaction, and so the only calls a
/// gateway with `--redirect-queries` sends to the worker. They are exactly the
/// two methods the node's fee-cap guard replaces
/// (`crates/tn-reth/src/env/rpc.rs`), matched exactly and case-sensitively, as
/// jsonrpsee matches method names. Both contain [`SEND_RAW_TRANSACTION`], so
/// its substring test is a fast path for either.
const SUBMISSION_METHODS: [&str; 2] = [SEND_RAW_TRANSACTION, SEND_RAW_TRANSACTION_SYNC];

/// The proxy client's `User-Agent`, so the operator of a query upstream can
/// tell gateway traffic apart.
const USER_AGENT: &str = concat!("tn-worker-gateway/", env!("CARGO_PKG_VERSION"));

/// Marker header stamped on every request forwarded to a worker. An inbound request that
/// already carries it has looped back through a gateway (an upstream URL or
/// VIP that points at a gateway instead of a worker) and is rejected rather
/// than forwarded, breaking the loop at the first revisit.
pub(crate) const HOP_HEADER: HeaderName = HeaderName::from_static("x-tn-gateway");

/// Marker header stamped on every request sent to the query upstream, in place
/// of [`HOP_HEADER`]: a public RPC that sits behind a gateway of its own would
/// reject the hop marker as a loop. Only a gateway that itself redirects
/// rejects an inbound request carrying this marker, which catches a
/// `--redirect-queries` URL that leads back to a redirecting gateway (for
/// example the deployment's own advertised endpoint) after one hop.
const REDIRECT_HEADER: HeaderName = HeaderName::from_static("x-tn-gateway-redirect");

/// De-facto standard header carrying the client IP chain to the upstream.
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");

/// De-facto standard header carrying the client-facing scheme to the upstream.
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

/// Forward a JSON-RPC request to the first ready upstream worker or, when
/// `--redirect-queries` is set and the request is not made only of
/// submissions, to the query upstream.
///
/// `limiters` is present when the rate-limit layer is installed and admitted
/// the request on its first token (see [`crate::ratelimit::rate_limit`]).
/// `body` is the final extractor (it consumes the request body), so it must
/// stay last in the parameter list.
pub(crate) async fn proxy(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    headers: HeaderMap,
    limiters: Option<Extension<Arc<RateLimiters>>>,
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

    // a redirecting gateway that receives the redirect marker is being sent
    // reads that some gateway already redirected: its query upstream leads
    // back to a redirecting gateway, and redirecting again would loop. a
    // gateway without a redirect forwards such a request like any other, so a
    // gateway can front the public rpc.
    if state.query_upstream.is_some() && headers.contains_key(REDIRECT_HEADER) {
        warn!(
            target: "gateway::proxy",
            "redirect loop detected (inbound request already carries the query-redirect marker); \
             check that --redirect-queries does not lead to a gateway that redirects"
        );
        return error_response(&GatewayError::LoopDetected, body.as_ref());
    }

    // One pass over the body answers every question asked of it below: how
    // many calls it carries, whether the transaction screen refuses it, and
    // where it routes.
    let scan = scan(body.as_ref(), state.max_batch_len);

    // A batch the scan could not read to its end has an unknown length: the
    // worker skips an element serde's typed readers refuse (an out-of-range
    // number, an unpaired surrogate escape) and runs the rest, so forwarding
    // it would dodge the length cap and the per-call charge. It is refused
    // whole, before it is charged, and has no single id to echo.
    if scan.unreadable_batch {
        warn!(target: "gateway::proxy", "rejecting a batch the gateway cannot read to its end");
        return error_response(&GatewayError::UnreadableBody, b"");
    }

    // A batch over the length cap is refused whole. A batch has no single id
    // to echo, so the error carries `null`.
    if state.max_batch_len.is_some_and(|max| scan.len > max.get()) {
        warn!(target: "gateway::proxy", "rejecting a batch longer than --max-batch-len");
        return error_response(&GatewayError::BatchTooLong, b"");
    }

    // The rate-limit layer charged one token before the body was read; a
    // batch pays one more for each call beyond its first before it is
    // screened or forwarded. Like the layer's own refusal, this one cannot
    // name an id.
    let charged = limiters.as_ref().map_or(Ok(()), |Extension(limiters)| {
        limiters.charge(Some(peer.ip()), scan.len.saturating_sub(1))
    });
    if let Err(err) = charged {
        return error_response(&err, b"");
    }

    // Shallow pre-flight for raw-transaction submissions, alone or in a batch
    // of nothing but submissions: reject a payload the worker would also
    // reject (undecodable, or a type the network does not accept) before
    // paying for an upstream round-trip. The scan recovers the refused call's
    // request id itself on the paths that reject.
    if let Some((err, id)) = scan.rejection {
        warn!(target: "gateway::proxy", ?err, "rejecting a raw-transaction submission before forwarding");
        return error_response_with_id(&err, id);
    }

    // with a redirect configured, every call but a submission goes to the
    // query upstream, with no readiness gate and no fallback to the worker: a
    // fallback would put the read load on the validator exactly when the
    // public rpc is struggling.
    let query_upstream = state.query_upstream.as_ref().filter(|_| is_query(scan.calls));
    let (route, upstream_url) = match query_upstream {
        Some(query_upstream) => (Route::Query, query_upstream.clone()),
        None => match state.readiness.first_ready_rpc_url() {
            Some(rpc_url) => (Route::Worker, rpc_url),
            None => {
                warn!(target: "gateway::proxy", "no upstream worker ready; rejecting request");
                return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
            }
        },
    };

    match forward(&state.http, route, method, &headers, body.clone(), upstream_url.clone(), peer)
        .await
    {
        Ok(response) => {
            telemetry::record_forwarded();
            telemetry::record_routed(route.label(), "forwarded");
            response
        }
        Err(source) => {
            let err = classify_error(&source);
            let result = if matches!(err, GatewayError::UpstreamTimeout) {
                "timeout"
            } else {
                "unreachable"
            };
            telemetry::record_routed(route.label(), result);
            // reqwest's `Display` appends the full request url, whose userinfo,
            // path or query can carry a credential, so the log names the
            // upstream by origin and renders the cause with the url removed.
            let source = source.without_url();
            warn!(
                target: "gateway::proxy",
                ?err,
                route = route.label(),
                upstream = %UpstreamOrigin(&upstream_url),
                cause = %ErrorChain(&source),
                "forwarding to upstream failed"
            );
            error_response(&err, body.as_ref())
        }
    }
}

/// Whether a request goes to the query upstream rather than the worker,
/// counting a mixed batch on the way (see [`scan`]).
fn is_query(calls: Calls) -> bool {
    if calls == Calls::MixedBatch {
        telemetry::record_mixed_batch();
    }
    calls.route() == Route::Query
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

/// Forward one request to `upstream_url` and adapt the upstream response back
/// into an axum response, preserving the status, body, and content type.
///
/// The `route` picks the marker header: [`HOP_HEADER`] toward a worker,
/// [`REDIRECT_HEADER`] toward the query upstream, never both.
///
/// A transport failure is returned as the raw `reqwest` error so the caller can
/// log its cause before [`classify_error`] reduces it to a client-facing error.
async fn forward(
    client: &Client,
    route: Route,
    method: Method,
    headers: &HeaderMap,
    body: Bytes,
    upstream_url: Url,
    peer: SocketAddr,
) -> Result<Response, reqwest::Error> {
    // JSON-RPC is content-type `application/json`; preserve the client's header
    // when present, default to it otherwise.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));
    let marker = match route {
        Route::Worker => HOP_HEADER,
        Route::Query => REDIRECT_HEADER,
    };

    let upstream = client
        .request(method, upstream_url)
        .header(header::CONTENT_TYPE, content_type)
        .header(marker, HeaderValue::from_static("1"))
        .header(X_FORWARDED_FOR, forwarded_for(headers, peer))
        .header(X_FORWARDED_PROTO, HeaderValue::from_static("http"))
        .body(body)
        .send()
        .await?;

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

/// Build the client that forwards requests on both routes.
///
/// Redirects are never followed. reqwest follows up to ten by default and
/// replays a POST body on `307`/`308`, so a query upstream answering with a
/// redirect (a misbehaving or compromised public RPC, or anything on the path
/// to it) could otherwise bounce a read onto the private worker or any internal
/// host the gateway can reach. A `3xx` from either upstream is passed through
/// to the client like any other status, without its `Location` (only
/// `Content-Type` is copied back), so the client cannot follow it either.
///
/// TLS is rustls with the platform's native root store, which only the query
/// route can use (worker URLs must be `http`). An image without CA
/// certificates still builds the client, but every `https` request then fails.
pub(crate) fn proxy_client(
    connect_timeout: Duration,
    request_timeout: Duration,
) -> reqwest::Result<Client> {
    Client::builder()
        .use_rustls_tls()
        .redirect(Policy::none())
        .user_agent(USER_AGENT)
        .connect_timeout(connect_timeout)
        .timeout(request_timeout)
        .build()
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
fn classify_error(err: &reqwest::Error) -> GatewayError {
    if err.is_timeout() {
        GatewayError::UpstreamTimeout
    } else {
        GatewayError::UpstreamUnreachable
    }
}

/// Renders only a URL's origin, `scheme://host:port`, for logs.
///
/// An upstream URL can carry a credential in its userinfo, path or query (a
/// hosted RPC provider's API key, for example), so log lines name an upstream
/// by origin alone. The port is always written, falling back to the scheme's
/// default, so two upstreams on one host stay distinguishable.
pub(crate) struct UpstreamOrigin<'a>(pub(crate) &'a Url);

impl fmt::Display for UpstreamOrigin<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let url = self.0;
        write!(f, "{}://{}", url.scheme(), url.host_str().unwrap_or_default())?;
        if let Some(port) = url.port_or_known_default() {
            write!(f, ":{port}")?;
        }
        Ok(())
    }
}

/// Renders an error followed by every [`std::error::Error::source`] beneath it,
/// joined with `": "`.
///
/// A `reqwest` error's own message is only its kind ("error sending request");
/// the reason a forward failed (connection refused, DNS failure, reset) sits
/// further down the source chain.
struct ErrorChain<'a>(&'a dyn std::error::Error);

impl fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(err) = source {
            write!(f, ": {err}")?;
            source = err.source();
        }
        Ok(())
    }
}

/// Which upstream a request goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Route {
    /// The first ready worker: every call without `--redirect-queries`, and
    /// only submissions with it.
    Worker,
    /// The `--redirect-queries` endpoint: every call that is not a submission.
    Query,
}

impl Route {
    /// The `route` label on `tn_worker_gateway_routed_requests_total`.
    fn label(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Query => "query",
        }
    }
}

/// What a request body holds, as far as routing is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Calls {
    /// One submission, or a non-empty batch of nothing but submissions.
    Submissions,
    /// No submission, or nothing the classifier can read as one.
    Queries,
    /// A batch holding both submissions and other calls (an element that is
    /// not an object counts as another call).
    MixedBatch,
}

impl Calls {
    /// Only an all-submission body goes to the worker.
    fn route(self) -> Route {
        match self {
            Self::Submissions => Route::Worker,
            Self::Queries | Self::MixedBatch => Route::Query,
        }
    }
}

/// What one pass over a request body found: where the body routes and whether
/// the transaction screen refuses it.
struct Scan {
    /// How many calls the body carries: 1 for anything but a batch, and for a
    /// batch its element count, which stops one past the length cap.
    len: usize,
    /// How the body routes under `--redirect-queries`.
    calls: Calls,
    /// The screen's verdict: the error and the request id to answer it with,
    /// or `None` to forward the body.
    rejection: Option<(GatewayError, RequestId)>,
    /// A batch the pass could not read to its end for any reason but the
    /// length cap, so `len` is only a lower bound on its calls.
    unreadable_batch: bool,
}

/// Read a request body once, counting its calls, classifying it for
/// `--redirect-queries` and screening its raw-transaction submission in the
/// same pass.
///
/// Length: a batch's elements are counted, every element whatever its shape,
/// and the count stops one past `max_batch_len` (`None` counts them all), so
/// the caller can refuse an over-length batch having paid for no more than
/// the cap. A batch the pass cannot read to its end for any other reason is
/// reported as unreadable, because its count is then only a lower bound: an
/// element serde's typed readers refuse (an out-of-range number, an unpaired
/// surrogate escape) ends the pass, while the worker skips that element and
/// runs the rest of the batch.
///
/// Routing: only a body made entirely of submissions ([`SUBMISSION_METHODS`])
/// goes to the worker. A method name is read as the worker reads it, unicode
/// escapes included. Everything else goes to the query upstream, and so does
/// everything ambiguous, which keeps it away from the validator: a body that
/// is not JSON, a `method` that is not a string, an empty batch, and bytes
/// trailing the JSON value. `eth_sendTransaction` is a query too: no node
/// configures a signer, so the worker could only refuse it.
///
/// A batch that mixes submissions with other calls goes, whole, to the query
/// upstream; a batch element that is not an object counts as another call.
/// Sending a mixed batch to the worker would let a client put one submission
/// in front of any number of reads and push them all onto the validator.
/// Clients lose nothing, because the public RPC accepts submissions too; the
/// submission just reaches the network through it instead of through this
/// validator.
///
/// Screening: a submission, alone or in a batch made only of submissions, is
/// refused when the raw transaction in its first positional param cannot be
/// decoded or decodes to a type the network does not accept (see
/// [`screen_transaction`]). In a batch the first refused element answers for
/// the whole batch, with its own id, and later elements are not decoded.
/// Every other request is forwarded unchanged: other methods, a mixed batch,
/// a submission whose params are named or structurally off (the worker
/// answers with its own parameter error), and a single call that is not
/// exactly one JSON value.
///
/// Cost: one deserializer reads the body, and every member the gateway does
/// not act on is skipped in place, so the cost is a scan of the bytes rather
/// than a `Value` tree several times the size of the request. Keys are matched
/// where they lie and never copied; a `method` or hex raw transaction is
/// copied only when it is written with escapes. The request id is not read at
/// all: only a rejection needs it, and a rejection recovers it from the bytes
/// with one more scan (see [`RequestId::recover`] and
/// [`RequestId::recover_element`]), so a forwarded request never materializes
/// its id, which is where all of a request's bulk can sit.
fn scan(body: &[u8], max_batch_len: Option<NonZeroUsize>) -> Scan {
    // the worker's server drops leading ascii whitespace, form feed included,
    // before it reads a body, where json allows only four whitespace bytes;
    // the scan reads the body the worker reads, so a prefix cannot hide a
    // batch from the count or a call from the screen
    let body = body.trim_ascii_start();
    let batch = is_batch(body);
    // fast path: a body that is not a batch and cannot name a submission
    // method is one call and holds no submission, and nothing is parsed
    if !batch && !may_name_a_submission(body) {
        return Scan { len: 1, calls: Calls::Queries, rejection: None, unreadable_batch: false };
    }
    let mut state = ScanState::default();
    let mut deserializer = body_deserializer(body);
    let parsed = deserializer
        .deserialize_any(ScanVisitor { state: &mut state, max_batch_len })
        .and_then(|()| deserializer.end())
        .is_ok();
    let unreadable_batch = batch && !parsed && !state.capped;
    let calls = match (state.submission, state.other) {
        (true, false) if parsed => Calls::Submissions,
        (true, true) => Calls::MixedBatch,
        _ => Calls::Queries,
    };
    // a verdict stands only on a body of nothing but submissions that is
    // exactly one json value: otherwise the body is forwarded, and the
    // upstream answers its parse error or its other calls
    let rejection =
        state.rejection.filter(|_| calls == Calls::Submissions).map(|(err, element)| {
            let id = element.map_or_else(
                || RequestId::recover(body),
                |index| RequestId::recover_element(body, index),
            );
            (err, id)
        });
    Scan { len: if batch { state.len } else { 1 }, calls, rejection, unreadable_batch }
}

/// The deserializer a request body is read with. [`scan`] is its only caller,
/// so a body is parsed once however many questions the handler asks of it.
fn body_deserializer(body: &[u8]) -> serde_json::Deserializer<SliceRead<'_>> {
    #[cfg(test)]
    tests::count_parse();
    serde_json::Deserializer::from_slice(body)
}

/// What a [`ScanVisitor`] pass has met so far. It lives outside the visitor
/// so [`scan`] can still read it after an error ends the pass early.
#[derive(Debug, Default)]
struct ScanState {
    /// Calls read so far: 1 for a single call, the elements read for a batch.
    len: usize,
    /// The length cap, not a reader, ended the pass.
    capped: bool,
    /// At least one call was a submission.
    submission: bool,
    /// At least one call was something else, a batch element that is not an
    /// object included.
    other: bool,
    /// The screen's verdict on the first submission it refuses, with the
    /// refused call's index when it is a batch element.
    rejection: Option<(GatewayError, Option<usize>)>,
}

impl ScanState {
    /// Record one call by its method.
    fn record(&mut self, method: RpcMethod) {
        if method.is_submission() {
            self.submission = true;
        } else {
            self.other = true;
        }
    }

    /// Record one batch element; one that is not an object counts as another
    /// call. A submission is screened while the batch can still be all
    /// submissions, up to the first one the screen refuses.
    fn record_element(&mut self, element: Element<'_>) {
        match element {
            Element::Call(call) => {
                self.record(call.method);
                if !self.other && self.rejection.is_none() {
                    self.rejection = call.screen().map(|err| (err, Some(self.len)));
                }
            }
            Element::Other => self.other = true,
        }
    }

    /// Whether the calls read so far mix submissions with other calls.
    fn is_mixed(&self) -> bool {
        self.submission && self.other
    }
}

/// Visitor behind [`scan`]: a single call or a batch of calls.
///
/// An error here is a verdict, not a failure, when it ends a batch scan early
/// (the batch is over the length cap); otherwise the body is not well-formed
/// JSON or holds a value serde's typed readers refuse. [`scan`] reads what the
/// state recorded either way.
struct ScanVisitor<'s> {
    /// Where the pass records what it has met.
    state: &'s mut ScanState,
    /// The length cap a batch count stops one past, or `None` for no cap.
    max_batch_len: Option<NonZeroUsize>,
}

impl<'de> Visitor<'de> for ScanVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC request object or batch")
    }

    /// A single call: classified, and screened when it is a submission.
    fn visit_map<A: MapAccess<'de>>(self, members: A) -> Result<Self::Value, A::Error> {
        let call = Call::read(members)?;
        self.state.len = 1;
        self.state.record(call.method);
        self.state.rejection = call.screen().map(|err| (err, None));
        Ok(())
    }

    /// A batch: counted and classified element by element. Once the batch is
    /// known to be mixed its route is settled, and the rest of it is only
    /// counted, each element skipped in place. The count stops one past the
    /// length cap, which ends the scan.
    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Self::Value, A::Error> {
        let Self { state, max_batch_len } = self;
        loop {
            let read = if state.is_mixed() {
                elements.next_element::<IgnoredAny>()?.is_some()
            } else {
                elements
                    .next_element::<Element<'de>>()?
                    .map(|element| state.record_element(element))
                    .is_some()
            };
            if !read {
                return Ok(());
            }
            state.len += 1;
            if max_batch_len.is_some_and(|max| state.len > max.get()) {
                state.capped = true;
                return Err(de::Error::custom("batch longer than the length cap"));
            }
        }
    }
}

/// One call object, reduced to what the gateway acts on.
struct Call<'de> {
    /// The call's `method`.
    method: RpcMethod,
    /// The first element of `params` when it has a shape the worker reads as
    /// bytes: the raw transaction, if the call is a submission.
    raw_transaction: Option<RawTransaction<'de>>,
}

impl<'de> Call<'de> {
    /// Read a call object's `method` and first `params` element, skipping
    /// every other member in place.
    ///
    /// A repeated member keeps its last occurrence, as a `Value` parse does.
    /// jsonrpsee's derived request type rejects a duplicated member outright,
    /// so such a call is an invalid request at either upstream; the choice
    /// only decides which one says so.
    fn read<A: MapAccess<'de>>(mut members: A) -> Result<Self, A::Error> {
        let mut call = Self { method: RpcMethod::Other, raw_transaction: None };
        while let Some(member) = members.next_key::<Member>()? {
            match member {
                Member::Method => {
                    call.method =
                        RpcMethod::named(members.next_value::<MaybeStr<'de>>()?.0.as_deref());
                }
                Member::Params => call.raw_transaction = members.next_value::<FirstParam<'de>>()?.0,
                Member::Other => {
                    members.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(call)
    }

    /// The screen's verdict on this call: `None` unless it is a submission
    /// whose raw transaction the worker would refuse.
    fn screen(&self) -> Option<GatewayError> {
        if !self.method.is_submission() {
            return None;
        }
        self.raw_transaction.as_ref().and_then(screen_transaction)
    }
}

/// A call's `method`, reduced to what the gateway acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcMethod {
    /// One of [`SUBMISSION_METHODS`].
    Submission,
    /// Any other method, or a `method` that is not a string.
    Other,
}

impl RpcMethod {
    /// The method a `method` member names, matched against
    /// [`SUBMISSION_METHODS`] exactly and case-sensitively, as jsonrpsee
    /// matches method names.
    fn named(name: Option<&str>) -> Self {
        if name.is_some_and(|name| SUBMISSION_METHODS.contains(&name)) {
            Self::Submission
        } else {
            Self::Other
        }
    }

    /// Whether the method is one of [`SUBMISSION_METHODS`].
    fn is_submission(self) -> bool {
        self == Self::Submission
    }
}

/// A call-object member the scan acts on, matched where the key lies: a key
/// borrowed from the body, or one serde decoded from escapes into its scratch
/// buffer, is compared in place and never copied.
enum Member {
    /// `method`.
    Method,
    /// `params`.
    Params,
    /// Any other member.
    Other,
}

impl Lenient<'_> for Member {
    fn other() -> Self {
        Self::Other
    }

    fn string(name: &str) -> Self {
        match name {
            "method" => Self::Method,
            "params" => Self::Params,
            _ => Self::Other,
        }
    }
}

impl<'de> Deserialize<'de> for Member {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A batch element: a call object, or anything else.
enum Element<'de> {
    /// A call object.
    Call(Call<'de>),
    /// An element that is not an object; the upstream answers it as an
    /// invalid request.
    Other,
}

impl<'de> Lenient<'de> for Element<'de> {
    fn other() -> Self {
        Self::Other
    }

    fn object<A: MapAccess<'de>>(members: A) -> Result<Self, A::Error> {
        Call::read(members).map(Self::Call)
    }
}

impl<'de> Deserialize<'de> for Element<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A `params` member reduced to its first element when that element holds a
/// raw transaction.
///
/// The remaining elements are drained through the ignored-value sink, so a
/// `params` array of any length costs a scan rather than an allocation per
/// element. Named params (an object) and any other shape have no first
/// element and leave the call to the upstream.
struct FirstParam<'de>(Option<RawTransaction<'de>>);

impl<'de> Lenient<'de> for FirstParam<'de> {
    fn other() -> Self {
        Self(None)
    }

    fn array<A: SeqAccess<'de>>(mut elements: A) -> Result<Self, A::Error> {
        let first = elements.next_element::<MaybeRawTransaction<'de>>()?.and_then(|first| first.0);
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Self(first))
    }
}

impl<'de> Deserialize<'de> for FirstParam<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A raw transaction in one of the two shapes the worker's `Bytes` parameter
/// accepts.
enum RawTransaction<'de> {
    /// A hex string, `0x`-prefixed or bare, borrowed from the body unless it
    /// is written with escapes.
    Hex(Cow<'de, str>),
    /// An array of integers from 0 to 255.
    Bytes(Vec<u8>),
}

impl RawTransaction<'_> {
    /// The transaction's bytes, or `None` when the hex is not valid hex.
    fn bytes(&self) -> Option<Cow<'_, [u8]>> {
        match self {
            Self::Hex(hex) => decode_hex(hex).map(Cow::Owned),
            Self::Bytes(bytes) => Some(Cow::Borrowed(bytes)),
        }
    }
}

/// A first param kept only when it is a raw transaction: a string, or an
/// array whose every element is an integer from 0 to 255, as alloy's `Bytes`
/// reads it. Any other shape, an array holding anything else included, is
/// consumed and discarded, and the worker answers it with its own parameter
/// error.
struct MaybeRawTransaction<'de>(Option<RawTransaction<'de>>);

impl<'de> Lenient<'de> for MaybeRawTransaction<'de> {
    fn other() -> Self {
        Self(None)
    }

    fn borrowed_string(value: &'de str) -> Self {
        Self(Some(RawTransaction::Hex(Cow::Borrowed(value))))
    }

    fn string(value: &str) -> Self {
        Self(Some(RawTransaction::Hex(Cow::Owned(value.to_owned()))))
    }

    fn array<A: SeqAccess<'de>>(mut elements: A) -> Result<Self, A::Error> {
        let mut bytes = Vec::new();
        while let Some(MaybeByte(byte)) = elements.next_element()? {
            let Some(byte) = byte else {
                while elements.next_element::<IgnoredAny>()?.is_some() {}
                return Ok(Self(None));
            };
            bytes.push(byte);
        }
        Ok(Self(Some(RawTransaction::Bytes(bytes))))
    }
}

impl<'de> Deserialize<'de> for MaybeRawTransaction<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A value kept only when it is an integer from 0 to 255.
struct MaybeByte(Option<u8>);

impl Lenient<'_> for MaybeByte {
    fn other() -> Self {
        Self(None)
    }

    fn unsigned(value: u64) -> Self {
        Self(u8::try_from(value).ok())
    }
}

impl<'de> Deserialize<'de> for MaybeByte {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A value kept only when it is a string, borrowed from the body unless it is
/// written with escapes; any other shape is consumed and discarded.
///
/// The screen once read `method` through `Value::as_str`, which yields `None`
/// for a non-string without failing the parse. This reproduces that: a
/// `method` that is not a string reads as another call, instead of the whole
/// scan bailing out.
struct MaybeStr<'de>(Option<Cow<'de, str>>);

impl<'de> Lenient<'de> for MaybeStr<'de> {
    fn other() -> Self {
        Self(None)
    }

    fn borrowed_string(value: &'de str) -> Self {
        Self(Some(Cow::Borrowed(value)))
    }

    fn string(value: &str) -> Self {
        Self(Some(Cow::Owned(value.to_owned())))
    }
}

impl<'de> Deserialize<'de> for MaybeStr<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(LenientVisitor(PhantomData))
    }
}

/// A value the scan reads leniently: every JSON shape is accepted, and a shape
/// the reader has no use for is skipped in place and read as [`Self::other`].
///
/// A reader in the scan must accept every JSON shape, because a failure ends
/// the whole pass: an unexpected shape anywhere in a call would hide the
/// call's method from the classifier, or its transaction from the screen.
/// Only malformed JSON, or a value serde's typed readers refuse (a number out
/// of `f64` range, a string or key with an unpaired surrogate escape), ends a
/// scan early; [`scan`] reports a batch so ended as unreadable. Containers a
/// reader does not take are drained through the ignored-value sink, so an
/// oversized member costs a scan, not an allocation per node.
trait Lenient<'de>: Sized {
    /// The value for a shape the reader has no use for.
    fn other() -> Self;

    /// Read a string borrowed from the body (it holds no escapes).
    fn borrowed_string(value: &'de str) -> Self {
        Self::string(value)
    }

    /// Read a string serde decoded from escapes.
    fn string(_value: &str) -> Self {
        Self::other()
    }

    /// Read a non-negative integer.
    fn unsigned(_value: u64) -> Self {
        Self::other()
    }

    /// Read an array.
    fn array<A: SeqAccess<'de>>(mut elements: A) -> Result<Self, A::Error> {
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Self::other())
    }

    /// Read an object.
    fn object<A: MapAccess<'de>>(mut members: A) -> Result<Self, A::Error> {
        while members.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
        Ok(Self::other())
    }
}

/// Visitor behind every [`Lenient`] reader.
struct LenientVisitor<T>(PhantomData<T>);

impl<'de, T: Lenient<'de>> Visitor<'de> for LenientVisitor<T> {
    type Value = T;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("any JSON value")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Self::Value, E> {
        Ok(T::other())
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<Self::Value, E> {
        Ok(T::other())
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(T::unsigned(value))
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Self::Value, E> {
        Ok(T::other())
    }

    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(T::borrowed_string(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(T::string(value))
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(T::other())
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(T::other())
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, elements: A) -> Result<Self::Value, A::Error> {
        T::array(elements)
    }

    fn visit_map<A: MapAccess<'de>>(self, members: A) -> Result<Self::Value, A::Error> {
        T::object(members)
    }
}

/// The screen's verdict on one raw transaction: `None` to forward it, or the
/// error to refuse it with.
///
/// The decode uses the same pooled wire format the worker's RPC accepts and
/// never recovers the signer, so it cannot reject a transaction the worker
/// would have accepted (no false rejections); it only front-runs a rejection
/// the worker would issue anyway. From here the payload is unambiguously a
/// raw transaction, so a decode failure is a real rejection rather than a
/// reason to forward.
fn screen_transaction(raw: &RawTransaction<'_>) -> Option<GatewayError> {
    raw.bytes().map_or(Some(GatewayError::InvalidTransaction), |raw| {
        let mut buf = raw.as_ref();
        match PooledTransaction::decode_2718(&mut buf) {
            Err(_) => Some(GatewayError::InvalidTransaction),
            Ok(tx) if !tn_types::batch_allowlisted_tx_type(&tx) => {
                Some(GatewayError::UnsupportedTransactionType(tx.ty()))
            }
            Ok(_) => None,
        }
    })
}

/// Whether `body`, its leading whitespace already dropped, is a JSON array.
fn is_batch(body: &[u8]) -> bool {
    body.first() == Some(&b'[')
}

/// Whether `body` can name a submission method: it mentions the
/// raw-transaction method (both submission methods contain it), or it holds a
/// unicode escape, which can spell any name. No other JSON escape decodes to a
/// letter or `_`.
///
/// The body need not be valid UTF-8: the worker's server skips an invalid
/// byte inside a member it does not read and runs the call, so each valid
/// stretch of the body is searched in place. Both needles are ASCII, so a
/// match never spans an invalid byte.
fn may_name_a_submission(body: &[u8]) -> bool {
    body.utf8_chunks().any(|chunk| {
        let text = chunk.valid();
        text.contains(SEND_RAW_TRANSACTION) || text.contains("\\u")
    })
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
    use alloy::{
        consensus::{TxEip1559, TxEip2930, TxEip4844, TxEip4844WithSidecar, TxEip7702, TxLegacy},
        eips::{eip4844::BlobTransactionSidecar, eip7594::BlobTransactionSidecarVariant},
    };
    use serde_json::{value::RawValue, Value};
    use std::cell::Cell;
    use tn_types::{Encodable2718, EthSignature, SignableTransaction, U256};

    std::thread_local! {
        /// Deserializers [`body_deserializer`] has built on this thread.
        static PARSES: Cell<usize> = const { Cell::new(0) };
    }

    /// Count one deserializer built over a request body (see
    /// [`body_deserializer`]).
    pub(super) fn count_parse() {
        PARSES.with(|parses| parses.set(parses.get() + 1));
    }

    /// The screen's verdict on a body, from the one pass that also routes it.
    fn screen_raw_transaction(body: &[u8]) -> Option<(GatewayError, RequestId)> {
        scan(body, None).rejection
    }

    /// How a body routes, from the one pass that also screens it.
    fn classify(body: &[u8]) -> Calls {
        scan(body, None).calls
    }

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

    /// One transaction of every EIP-2718 type the pooled wire format carries,
    /// in type order, each built from a default body and a dummy signature
    /// (the decode checks structure, not the signature). The blob transaction
    /// carries an empty sidecar, which is enough to decode.
    fn one_transaction_per_type() -> [PooledTransaction; 5] {
        let signature = EthSignature::new(U256::from(1), U256::from(1), false);
        let sidecar = BlobTransactionSidecarVariant::Eip4844(BlobTransactionSidecar::default());
        let blob = TxEip4844WithSidecar::from_tx_and_sidecar(TxEip4844::default(), sidecar);
        [
            PooledTransaction::Legacy(TxLegacy::default().into_signed(signature)),
            PooledTransaction::Eip2930(TxEip2930::default().into_signed(signature)),
            PooledTransaction::Eip1559(TxEip1559::default().into_signed(signature)),
            PooledTransaction::Eip4844(blob.into_signed(signature)),
            PooledTransaction::Eip7702(TxEip7702::default().into_signed(signature)),
        ]
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

    /// The substring gate the previous screen parsed behind.
    fn mentions_send_raw_transaction(body: &[u8]) -> bool {
        std::str::from_utf8(body).is_ok_and(|text| text.contains(SEND_RAW_TRANSACTION))
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
                Some((GatewayError::UnsupportedTransactionType(tx.ty()), id))
            }
            Ok(_) => None,
        }
    }

    /// The new reader must agree with the old `Value` parse on every shape the
    /// old screen read, so the fix is a memory change and not a behaviour
    /// change. In particular it must not start rejecting anything it used to
    /// forward. The documented exceptions are an `id` nested past serde_json's
    /// recursion limit, pinned by
    /// [`deeply_nested_id_rejects_locally_where_the_old_parse_forwarded`], and
    /// the shapes the screen reads since WG-32, which the old one forwarded
    /// unread although the worker refuses them too: `eth_sendRawTransactionSync`,
    /// an escaped method name, a byte-array param and a batch, each pinned by
    /// its own test.
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
    /// that limit failed the whole parse and the request was forwarded
    /// regardless of its transaction: a forward by accident of the recursion
    /// limit, not by a verdict. The new reader skips the id
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
        assert!(matches!(err, GatewayError::UnsupportedTransactionType(4)));
        assert_eq!(id, RequestId::from_id(serde_json::json!(42)));
        assert_eq!(verdict(screen_raw_transaction(&body)), verdict(reference_screen(&body)));
    }

    /// WG-41: the screen must refuse exactly the transaction types the
    /// worker's pool refuses. The pool's set is fixed by its validator in
    /// `crates/tn-reth/src/txn_pool.rs` (`.no_eip4844().no_eip7702()`: it
    /// admits legacy, EIP-2930 and EIP-1559 only), a crate the gateway does
    /// not link, so that set is written out here; the screen's comes from
    /// `tn_types::batch_allowlisted_tx_type`. A change on either side fails
    /// this test instead of letting the two sets drift apart by convention.
    #[test]
    fn screen_verdicts_match_the_pool_allowlist() {
        // the pool's admission set, by EIP-2718 type byte
        const POOL_ADMITS: [u8; 3] = [0, 1, 2];
        let transactions = one_transaction_per_type();
        let types = transactions.each_ref().map(Typed2718::ty);
        assert_eq!(types, [0, 1, 2, 3, 4], "one transaction of every pooled type");

        for tx in transactions {
            let ty = tx.ty();
            let admitted = POOL_ADMITS.contains(&ty);
            assert_eq!(tn_types::batch_allowlisted_tx_type(&tx), admitted, "type {ty}");
            let hex = format!("0x{}", tn_types::hex::encode(tx.encoded_2718()));
            let verdict = screen_err(&send_raw(&format!("[\"{hex}\"]")));
            if admitted {
                assert!(verdict.is_none(), "type {ty} must be forwarded: {verdict:?}");
            } else {
                assert!(
                    matches!(verdict, Some(GatewayError::UnsupportedTransactionType(refused)) if refused == ty),
                    "type {ty} must be refused as unsupported: {verdict:?}"
                );
            }
        }
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
        // A batch of nothing but valid submissions passes the screen element
        // by element and is forwarded.
        let body = format!(
            r#"[{{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":1}}]"#
        );
        assert!(screen_raw_transaction(body.as_bytes()).is_none());
    }

    /// WG-32: `eth_sendRawTransactionSync` carries a raw transaction exactly
    /// like `eth_sendRawTransaction`, and is screened the same way, alone and
    /// in a batch.
    #[test]
    fn sync_submissions_are_screened() {
        let sync = |params: &str, id: &str| {
            format!(
                r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransactionSync","params":{params},"id":{id}}}"#
            )
        };
        let (err, id) = screen_raw_transaction(sync(r#"["0xdeadbeef"]"#, "9").as_bytes())
            .expect("undecodable tx must be rejected");
        assert!(matches!(err, GatewayError::InvalidTransaction));
        assert_eq!(id, RequestId::from_id(serde_json::json!(9)));

        let disallowed = sync(&format!("[\"{}\"]", eip7702_raw_hex()), "10");
        assert!(matches!(
            screen_err(disallowed.as_bytes()),
            Some(GatewayError::UnsupportedTransactionType(4))
        ));
        let valid = sync(&format!("[\"{EIP155_LEGACY_TX}\"]"), "11");
        assert!(screen_err(valid.as_bytes()).is_none());

        let batch = format!("[{valid},{}]", sync(r#"["0xdeadbeef"]"#, r#""s-2""#));
        let (err, id) =
            screen_raw_transaction(batch.as_bytes()).expect("bad element must be rejected");
        assert!(matches!(err, GatewayError::InvalidTransaction));
        assert_eq!(id, RequestId::from_id(serde_json::json!("s-2")));
    }

    /// WG-32: a method name written with unicode escapes slipped past the
    /// substring gate unread, while the worker unescapes it into a submission.
    /// The scan now reads names as the worker does, member keys included.
    #[test]
    fn escaped_method_name_is_screened() {
        for body in [
            r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0054ransaction","params":["0xdeadbeef"],"id":5}"#,
            r#"{"jsonrpc":"2.0","method":"\u0065th_sendRawTransactionSync","params":["0xdeadbeef"],"id":5}"#,
            r#"{"jsonrpc":"2.0","m\u0065thod":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":5}"#,
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","p\u0061rams":["0xdeadbeef"],"id":5}"#,
            r#"[{"jsonrpc":"2.0","method":"eth_sendRaw\u0054ransaction","params":["0xdeadbeef"],"id":5}]"#,
        ] {
            assert_eq!(classify(body.as_bytes()), Calls::Submissions, "{body}");
            let (err, id) = screen_raw_transaction(body.as_bytes()).expect(body);
            assert!(matches!(err, GatewayError::InvalidTransaction), "{body}");
            assert_eq!(id, RequestId::from_id(serde_json::json!(5)), "{body}");
        }
        // an escaped payload is decoded as the worker decodes it
        let escaped_payload = format!(
            r#"{{"method":"eth_sendRawTransaction","params":["\u0030x{}"],"id":6}}"#,
            &EIP155_LEGACY_TX[2..]
        );
        assert!(screen_err(escaped_payload.as_bytes()).is_none());
    }

    /// WG-32: alloy's `Bytes`, the worker's parameter type, also reads an
    /// array of integers from 0 to 255, so the screen reads that shape too.
    /// An array holding anything else is not bytes to the worker either, and
    /// is left to the worker's own parameter error.
    #[test]
    fn byte_array_param_is_screened() {
        let as_array = |hex: &str| {
            let bytes = decode_hex(hex).expect("hex");
            let bytes: Vec<String> = bytes.iter().map(u8::to_string).collect();
            send_raw(&format!("[[{}]]", bytes.join(",")))
        };
        assert!(screen_err(&as_array(EIP155_LEGACY_TX)).is_none(), "valid bytes are forwarded");
        assert!(matches!(
            screen_err(&as_array(&eip7702_raw_hex())),
            Some(GatewayError::UnsupportedTransactionType(4))
        ));
        for params in ["[[222,173,190,239]]", "[[]]", "[[123]]"] {
            let err = screen_err(&send_raw(params));
            assert!(matches!(err, Some(GatewayError::InvalidTransaction)), "{params}");
        }
        for params in ["[[1,256]]", "[[-1]]", "[[1.0]]", r#"[[1,"2"]]"#, "[[[1]]]", "[[null]]"] {
            assert!(screen_err(&send_raw(params)).is_none(), "{params}");
        }
    }

    /// WG-32: named params have no first positional element, so the screen
    /// leaves them to the worker, which reads and validates them itself.
    #[test]
    fn named_params_are_left_to_the_worker() {
        let disallowed = format!(r#"{{"bytes":"{}"}}"#, eip7702_raw_hex());
        for params in [r#"{"bytes":"0xdeadbeef"}"#, r#"{"0":"0xdeadbeef"}"#, disallowed.as_str()] {
            let body = send_raw(params);
            assert!(screen_err(&body).is_none(), "{params}");
            assert_eq!(classify(&body), Calls::Submissions, "{params}");
        }
    }

    /// The worker's server skips an invalid UTF-8 byte inside a member it
    /// does not read and runs the call, so such a body is read and screened
    /// like its UTF-8 twin.
    #[test]
    fn non_utf8_submission_is_screened() {
        let body = |filler: &[u8], raw: &str| {
            let mut body = br#"{"jsonrpc":"2.0","x":""#.to_vec();
            body.extend_from_slice(filler);
            body.extend_from_slice(
                format!(r#"","method":"eth_sendRawTransaction","params":["{raw}"],"id":7}}"#)
                    .as_bytes(),
            );
            body
        };
        for raw in [eip7702_raw_hex(), EIP155_LEGACY_TX.to_string()] {
            let non_utf8 = body(&[0xff], &raw);
            assert!(std::str::from_utf8(&non_utf8).is_err());
            assert_eq!(classify(&non_utf8), Calls::Submissions);
            assert_eq!(
                verdict(screen_raw_transaction(&non_utf8)),
                verdict(screen_raw_transaction(&body(b"a", &raw))),
            );
        }
        let refused = screen_raw_transaction(&body(&[0xff], &eip7702_raw_hex()));
        assert_eq!(refused.map(|(_, id)| id), Some(RequestId::recover(br#"{"id":7}"#)));
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

    #[test]
    fn upstream_origin_drops_userinfo_path_and_query() {
        for (url, origin) in [
            ("http://user:secret@10.0.0.7:8545/key/abc?token=xyz#frag", "http://10.0.0.7:8545"),
            ("https://rpc.example.com/v1/0123456789abcdef", "https://rpc.example.com:443"),
            ("http://worker.internal/", "http://worker.internal:80"),
            ("http://[::1]:8545/", "http://[::1]:8545"),
        ] {
            let url = Url::parse(url).expect("url");
            assert_eq!(UpstreamOrigin(&url).to_string(), origin);
        }
    }

    /// One link of a hand-built error chain.
    #[derive(Debug)]
    struct Link(&'static str, Option<Box<Link>>);

    impl fmt::Display for Link {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Link {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|link| link as _)
        }
    }

    #[test]
    fn error_chain_joins_every_source() {
        let refused = Link("connection refused", None);
        let connect = Link("client error (Connect)", Some(Box::new(refused)));
        let err = Link("error sending request", Some(Box::new(connect)));
        assert_eq!(
            ErrorChain(&err).to_string(),
            "error sending request: client error (Connect): connection refused"
        );
        assert_eq!(ErrorChain(&Link("alone", None)).to_string(), "alone");
    }

    /// A real transport failure: the raw error's `Display` carries the url, the
    /// fields the proxy logs do not, and the cause reaches below reqwest's own
    /// message.
    #[tokio::test]
    async fn forwarding_failure_log_fields_hide_the_url() {
        // nothing listens on port 1, as in the server's unreachable-upstream test
        let url = Url::parse("http://user:secret@127.0.0.1:1/apikey123?token=xyz").expect("url");
        let err =
            Client::new().post(url.clone()).send().await.expect_err("nothing listens on port 1");
        assert!(matches!(classify_error(&err), GatewayError::UpstreamUnreachable));
        assert!(err.to_string().contains("apikey123"), "raw display should carry the url: {err}");

        let cause = ErrorChain(&err.without_url()).to_string();
        for secret in ["secret", "apikey123", "xyz"] {
            assert!(!cause.contains(secret), "cause leaks {secret:?}: {cause}");
        }
        assert!(
            cause.starts_with("error sending request: "),
            "cause should have a source: {cause}"
        );
        assert_eq!(UpstreamOrigin(&url).to_string(), "http://127.0.0.1:1");
    }

    /// A JSON-RPC call to `method` with empty params.
    fn call(method: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":[],"id":1}}"#)
    }

    #[test]
    fn each_submission_method_alone_goes_to_the_worker() {
        for method in SUBMISSION_METHODS {
            assert_eq!(classify(call(method).as_bytes()), Calls::Submissions, "{method}");
        }
        let signed = send_raw(&format!("[\"{EIP155_LEGACY_TX}\"]"));
        assert_eq!(classify(&signed).route(), Route::Worker);
    }

    #[test]
    fn other_methods_go_to_the_query_upstream() {
        for method in [
            "eth_sendTransaction",
            "eth_call",
            "eth_chainId",
            "eth_getLogs",
            "eth_getTransactionCount",
            "tn_info",
            "debug_traceTransaction",
        ] {
            let calls = classify(call(method).as_bytes());
            assert_eq!(calls, Calls::Queries, "{method}");
            assert_eq!(calls.route(), Route::Query, "{method}");
        }
    }

    #[test]
    fn all_submission_batch_goes_to_the_worker() {
        let submission = call("eth_sendRawTransaction");
        let sync = call("eth_sendRawTransactionSync");
        for body in [format!("[{submission}]"), format!("[{submission},{sync},{submission}]")] {
            assert_eq!(classify(body.as_bytes()), Calls::Submissions, "{body}");
        }
    }

    #[test]
    fn mixed_batch_goes_whole_to_the_query_upstream() {
        let submission = call("eth_sendRawTransaction");
        let read = call("eth_getLogs");
        for body in [
            format!("[{submission},{read}]"),
            format!("[{read},{submission}]"),
            format!("[{read},{read},{submission},{read}]"),
            format!("[{submission},{submission},{read}]"),
        ] {
            let calls = classify(body.as_bytes());
            assert_eq!(calls, Calls::MixedBatch, "{body}");
            assert_eq!(calls.route(), Route::Query, "{body}");
        }
    }

    #[test]
    fn unreadable_bodies_go_to_the_query_upstream() {
        let submission = call("eth_sendRawTransaction");
        let bodies = [
            "[]".to_string(),
            "eth_sendRawTransaction".to_string(),
            "garbage eth_sendRawTransaction garbage".to_string(),
            r#""eth_sendRawTransaction""#.to_string(),
            r#"{"method":123,"params":["eth_sendRawTransaction"],"id":1}"#.to_string(),
            r#"{"method":null,"eth_sendRawTransaction":1}"#.to_string(),
            r#"{"method":"eth_sendRawTransaction","params":["#.to_string(),
            format!("{submission} trailing"),
            format!("[{submission}] trailing"),
            format!("{submission}{submission}"),
            format!("[{submission},1]"),
            format!(r#"[{submission},"eth_sendRawTransaction"]"#),
        ];
        for body in bodies {
            assert_eq!(classify(body.as_bytes()).route(), Route::Query, "{body}");
        }
        // the batch with bytes after it reaches neither upstream: the handler
        // refuses a batch the scan cannot read to its end
        assert!(scan(format!("[{submission}] trailing").as_bytes(), None).unreadable_batch);
        let mut not_utf8 = submission.into_bytes();
        not_utf8.push(0xff);
        assert_eq!(classify(&not_utf8).route(), Route::Query);
    }

    #[test]
    fn method_name_inside_params_goes_to_the_query_upstream() {
        for body in [
            r#"{"jsonrpc":"2.0","method":"eth_call","params":[{"data":"eth_sendRawTransaction"},"latest"],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_getBalance","params":["eth_sendRawTransactionSync"],"id":1}"#,
            r#"{"jsonrpc":"2.0","eth_sendRawTransaction":{"method":"eth_sendRawTransaction"},"method":"eth_call","id":1}"#,
        ] {
            assert_eq!(classify(body.as_bytes()), Calls::Queries, "{body}");
        }
    }

    #[test]
    fn case_variants_and_escaped_names_go_to_the_query_upstream() {
        for method in [
            "eth_sendrawtransaction",
            "ETH_SENDRAWTRANSACTION",
            "Eth_sendRawTransaction",
            "eth_sendRawTransactionsync",
            "eth_sendRawTransactionX",
            " eth_sendRawTransaction",
            "eth_sendRawTransaction ",
        ] {
            assert_eq!(classify(call(method).as_bytes()), Calls::Queries, "{method:?}");
        }
        // an escaped name is read as the worker reads it: one that unescapes
        // to a case variant is a query like the variant, and one that
        // unescapes to a submission is a submission (see
        // `escaped_method_name_is_screened`)
        let escaped_variant =
            r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0074ransaction","params":[],"id":1}"#;
        assert_eq!(classify(escaped_variant.as_bytes()), Calls::Queries);
        let escaped =
            r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0054ransaction","params":[],"id":1}"#;
        assert_eq!(classify(escaped.as_bytes()), Calls::Submissions);
    }

    /// post-rev-11: a submission batched with an element that is not an
    /// object used to end the scan at that element, so the batch was counted
    /// as unreadable rather than mixed. It routes to the query upstream either
    /// way; now it is counted in `tn_worker_gateway_mixed_batches_total` too.
    #[test]
    fn submission_batched_with_a_non_object_element_is_mixed() {
        let submission = call("eth_sendRawTransaction");
        for other in
            ["1", "null", "true", r#""eth_sendRawTransaction""#, "[]", &format!("[{submission}]")]
        {
            for body in [format!("[{submission},{other}]"), format!("[{other},{submission}]")] {
                let calls = classify(body.as_bytes());
                assert_eq!(calls, Calls::MixedBatch, "{body}");
                assert!(is_query(calls), "{body}");
            }
        }
    }

    /// post-dos-4: the classifier used to parse a body a second time after
    /// the screen, allocating a `String` per key and per batch element. A
    /// 1 MiB body built to clear the substring gate and carry as many keys and
    /// elements as fit is now read by one deserializer, whose one pass yields
    /// both the route and the screen's verdict. Counted at the single site
    /// that builds a deserializer over a body ([`body_deserializer`]).
    #[test]
    fn body_is_parsed_once() {
        let element = r#"{"method":"eth_sendRawTransaction","a":0,"b":0,"c":0,"d":0,"e":0}"#;
        let elements = MAX_REQUEST_BYTES / (element.len() + 1);
        let batch = format!("[{}]", vec![element; elements].join(","));
        let keys: String =
            (0..MAX_REQUEST_BYTES / 14).map(|key| format!(r#","k{key:07}":0"#)).collect();
        let single = format!(r#"{{"method":"eth_chainId","eth_sendRawTransaction":0{keys}}}"#);
        for (body, calls) in [(batch, Calls::Submissions), (single, Calls::Queries)] {
            assert!(
                body.len() > MAX_REQUEST_BYTES * 9 / 10,
                "fixture should be large: {}",
                body.len()
            );
            assert!(
                body.len() <= MAX_REQUEST_BYTES,
                "fixture must fit the body cap: {}",
                body.len()
            );
            PARSES.with(|parses| parses.set(0));
            let scan = scan(body.as_bytes(), None);
            assert_eq!(PARSES.with(Cell::get), 1, "one deserializer per body");
            assert_eq!(scan.calls, calls);
            assert!(scan.rejection.is_none());
        }
    }

    /// A batch element serde's typed readers refuse ends the pass, while the
    /// worker skips that element and runs every other call. Wherever such an
    /// element sits, the batch is reported unreadable, so the handler refuses
    /// it rather than forward a batch whose count is only a lower bound. A
    /// value the pass reads or skips without complaint is counted as usual.
    #[test]
    fn batch_the_scan_cannot_read_to_its_end_is_unreadable() {
        let max = NonZeroUsize::new(50);
        let read = call("eth_getBalance");
        let submission = call("eth_sendRawTransaction");
        let reads = vec![read.as_str(); 20].join(",");
        let triggers = [
            "1e400",
            r#""\udc00""#,
            r#"{"\udc00":1}"#,
            r#"{"jsonrpc":"2.0","method":1e400,"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[1e400],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[[1e400]],"id":1}"#,
        ];
        for trigger in triggers {
            for body in [
                format!("[{trigger},{reads}]"),
                format!("[{reads},{trigger},{reads}]"),
                format!("[{submission},{trigger}]"),
            ] {
                assert!(scan(body.as_bytes(), max).unreadable_batch, "{body}");
            }
        }
        // so is a batch that is not exactly one JSON value
        for body in [format!("[{submission}] trailing"), format!("[{submission},{read}")] {
            assert!(scan(body.as_bytes(), max).unreadable_batch, "{body}");
        }
        // a value read or skipped without complaint is counted
        let ignored_id = r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":"\udc00"}"#;
        for body in
            [format!("[1e-400,{read}]"), format!("[1,{read}]"), format!("[{ignored_id},{read}]")]
        {
            let scan = scan(body.as_bytes(), max);
            assert_eq!((scan.len, scan.unreadable_batch), (2, false), "{body}");
        }
        // only a batch is reported: anything else is one call, read or not
        for body in [
            format!("{submission} trailing"),
            r#""eth_sendRawTransaction""#.to_string(),
            "1e400".to_string(),
        ] {
            let scan = scan(body.as_bytes(), max);
            assert_eq!((scan.len, scan.unreadable_batch), (1, false), "{body}");
        }
        // the length cap ending the pass makes a batch too long, not unreadable
        let long = format!("[{}]", vec![read.as_str(); 60].join(","));
        let scan = scan(long.as_bytes(), max);
        assert_eq!((scan.len, scan.unreadable_batch), (51, false));
    }

    /// The scan counts every batch element, whatever its shape and route, and
    /// stops one past the cap, so an over-length batch is refused having cost
    /// no more than the cap to read.
    #[test]
    fn batch_count_stops_one_past_the_cap() {
        let max = NonZeroUsize::new(3);
        let submission = call("eth_sendRawTransaction");
        let read = call("eth_chainId");
        let batch = |element: &str, len: usize| format!("[{}]", vec![element; len].join(","));
        assert_eq!(scan(read.as_bytes(), max).len, 1);
        assert_eq!(scan(b"[]", max).len, 0);
        assert_eq!(scan(b"not json", max).len, 1);
        // the worker drops leading ascii whitespace, form feed included
        assert_eq!(scan(format!("\u{c}{}", batch(read.as_str(), 1_000)).as_bytes(), max).len, 4);
        for element in [read.as_str(), submission.as_str(), "1"] {
            assert_eq!(scan(batch(element, 3).as_bytes(), max).len, 3, "{element}");
            assert_eq!(scan(batch(element, 1_000).as_bytes(), max).len, 4, "{element}");
            assert_eq!(scan(batch(element, 1_000).as_bytes(), None).len, 1_000, "{element}");
        }
        // a mixed batch is still counted to its end once its route is settled
        let mixed = format!("[{submission},{read},{}]", vec![read.as_str(); 98].join(","));
        let scan = scan(mixed.as_bytes(), None);
        assert_eq!((scan.len, scan.calls), (100, Calls::MixedBatch));
    }

    /// What jsonrpsee 0.26, the worker's JSON-RPC server, makes of a body:
    /// refused whole, or the calls it reads, each with the method it would
    /// run (`None` for a call it refuses and runs nothing for).
    #[derive(Debug, PartialEq)]
    enum WorkerReads {
        /// Not a request at all: the server answers a parse error.
        Refused,
        /// A single call.
        Single(Option<String>),
        /// A batch, element by element.
        Batch(Vec<Option<String>>),
    }

    /// Read `body` as jsonrpsee 0.26 does: `read_body` in
    /// `jsonrpsee-core/src/http_helpers.rs` picks single or batch by the
    /// first byte that is not ASCII whitespace in the first 128 and drops the
    /// bytes before it; `handle_rpc_call` in `jsonrpsee-server/src/server.rs`
    /// then reads a single call, or each element of a `Vec<&RawValue>`, as a
    /// `Request`, else as a `Notification`, else refuses it.
    fn worker_reads(body: &[u8]) -> WorkerReads {
        let method_of = |call: &[u8]| {
            serde_json::from_slice::<jsonrpsee_types::Request<'_>>(call)
                .map(|request| request.method.into_owned())
                .or_else(|_| {
                    serde_json::from_slice::<jsonrpsee_types::Notification<'_, Option<&RawValue>>>(
                        call,
                    )
                    .map(|notification| notification.method.into_owned())
                })
                .ok()
        };
        let Some(start) = body.iter().take(128).position(|byte| !byte.is_ascii_whitespace()) else {
            return WorkerReads::Refused;
        };
        let body = &body[start..];
        match body.first() {
            Some(b'{') => WorkerReads::Single(method_of(body)),
            Some(b'[') => serde_json::from_slice::<Vec<&RawValue>>(body)
                .map(|elements| {
                    WorkerReads::Batch(
                        elements
                            .iter()
                            .map(|element| method_of(element.get().as_bytes()))
                            .collect(),
                    )
                })
                .unwrap_or(WorkerReads::Refused),
            _ => WorkerReads::Refused,
        }
    }

    /// post-rev-5: the shield rests on the scan reading a body as the worker
    /// does. For every body the scan routes to the worker, each call jsonrpsee
    /// would run must be a submission; a call the two read differently (a
    /// duplicated member, which the scan resolves to its last occurrence) must
    /// be one jsonrpsee refuses outright, because its derived request type
    /// rejects duplicated fields. And wherever jsonrpsee reads calls, the
    /// scan must count the same number or report a batch it cannot read to
    /// its end, which the gateway refuses whole; otherwise a batch could
    /// dodge the length cap and the per-call charge.
    #[test]
    fn worker_bound_bodies_parse_as_submissions_only_under_jsonrpsee() {
        let submission = call("eth_sendRawTransaction");
        let sync = call("eth_sendRawTransactionSync");
        let read = call("eth_getBalance");
        let notification = r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":[]}"#;
        let reordered =
            r#"{"id":"a","params":[],"method":"eth_sendRawTransaction","jsonrpc":"2.0"}"#;
        let escaped_name = r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0054ransaction","id":1}"#;
        let escaped_key = r#"{"jsonrpc":"2.0","m\u0065thod":"eth_sendRawTransactionSync","id":1}"#;
        // read differently: the scan keeps the last `method`, jsonrpsee
        // refuses the duplicate
        let duplicated =
            r#"{"jsonrpc":"2.0","method":"eth_call","method":"eth_sendRawTransaction","id":1}"#;
        let duplicated_escaped = r#"{"jsonrpc":"2.0","method":"eth_call","m\u0065thod":"eth_sendRawTransaction","id":1}"#;
        let duplicated_last_read =
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","method":"eth_call","id":1}"#;
        // the worker skips an invalid utf-8 byte inside a member it does not
        // read and runs the call
        let mut non_utf8 = br#"{"jsonrpc":"2.0","x":""#.to_vec();
        non_utf8.push(0xff);
        non_utf8.extend_from_slice(br#"","method":"eth_sendRawTransaction","params":[],"id":1}"#);
        // jsonrpsee reads an array as a request too, so this element runs as a
        // submission; the scan counts it as another call and does not screen it
        let positional =
            format!(r#"[["2.0",1,"eth_sendRawTransaction",["{}"]]]"#, eip7702_raw_hex());

        let worker_bound: Vec<String> = vec![
            submission.clone(),
            sync.clone(),
            notification.to_string(),
            reordered.to_string(),
            escaped_name.to_string(),
            escaped_key.to_string(),
            duplicated.to_string(),
            duplicated_escaped.to_string(),
            format!("[{submission},{sync},{notification},{reordered}]"),
            format!("[{escaped_name},{escaped_key},{duplicated}]"),
            format!("\u{c}{submission}"),
            format!(" \t\r\n\u{c}[{submission},{sync}]"),
        ];
        let kept_away: Vec<String> = vec![
            read.clone(),
            duplicated_last_read.to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0074ransaction","id":1}"#.to_string(),
            r#"{"jsonrpc":"2.0","Method":"eth_sendRawTransaction","id":1}"#.to_string(),
            format!("[{submission},{read}]"),
            format!("[{read},{escaped_name}]"),
            format!("[{submission},1]"),
            format!("[{submission},{duplicated_last_read}]"),
            format!("\u{c}{read}"),
            format!("\u{c}[{submission},{read}]"),
            "[]".to_string(),
            format!("{submission} trailing"),
            positional.clone(),
        ];
        let worker_bound: Vec<Vec<u8>> =
            worker_bound.into_iter().map(String::into_bytes).chain([non_utf8]).collect();
        let kept_away: Vec<Vec<u8>> = kept_away.into_iter().map(String::into_bytes).collect();

        for body in worker_bound.iter().chain(&kept_away) {
            let scan = scan(body, None);
            let reads = worker_reads(body);
            let body = String::from_utf8_lossy(body);
            let calls = match &reads {
                WorkerReads::Refused => Vec::new(),
                WorkerReads::Single(call) => vec![call.clone()],
                WorkerReads::Batch(calls) => calls.clone(),
            };
            if reads != WorkerReads::Refused && !scan.unreadable_batch {
                assert_eq!(
                    scan.len,
                    calls.len(),
                    "the scan counts what the worker reads: {body:?}"
                );
            }
            if scan.calls == Calls::Submissions {
                for method in calls.iter().flatten() {
                    assert!(
                        SUBMISSION_METHODS.contains(&method.as_str()),
                        "the worker would run {method} from a worker-bound body: {body:?}"
                    );
                }
            }
        }
        for body in &worker_bound {
            let text = String::from_utf8_lossy(body);
            assert_eq!(classify(body), Calls::Submissions, "{text:?}");
        }
        for body in &kept_away {
            let text = String::from_utf8_lossy(body);
            assert_eq!(classify(body).route(), Route::Query, "{text:?}");
        }
        // the duplicate-key cases only stay safe because jsonrpsee refuses them
        for body in [duplicated, duplicated_escaped] {
            assert_eq!(worker_reads(body.as_bytes()), WorkerReads::Single(None), "{body}");
        }
        assert_eq!(
            worker_reads(positional.as_bytes()),
            WorkerReads::Batch(vec![Some(SEND_RAW_TRANSACTION.to_string())])
        );

        // an element serde's typed readers refuse ends the scan, while
        // jsonrpsee skips it and runs the rest of the batch: wherever it sits,
        // the scan reports the batch unreadable and the gateway refuses it
        let triggers = [
            "1e400".to_string(),
            "-1e400".to_string(),
            "1".repeat(321),
            r#""\udc00""#.to_string(),
            r#""\ud800""#.to_string(),
            r#"{"\udc00":1}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":1e400,"id":1}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[1e400],"id":1}"#.to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["\udc00"],"id":1}"#
                .to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[[1e400]],"id":1}"#.to_string(),
        ];
        for trigger in &triggers {
            for body in [
                format!("[{trigger},{read},{read}]"),
                format!("[{submission},{trigger}]"),
                format!(" [{trigger},{submission}]"),
            ] {
                assert!(matches!(worker_reads(body.as_bytes()), WorkerReads::Batch(_)), "{body}");
                assert!(scan(body.as_bytes(), None).unreadable_batch, "{body}");
            }
        }
        // a value the scan reads or skips without complaint is counted
        let ignored_id = r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":"\udc00"}"#;
        for body in [format!("[1e-400,{read}]"), format!("[{ignored_id},{read}]")] {
            let WorkerReads::Batch(calls) = worker_reads(body.as_bytes()) else {
                panic!("jsonrpsee reads a batch: {body}");
            };
            let scan = scan(body.as_bytes(), None);
            assert_eq!((scan.len, scan.unreadable_batch), (calls.len(), false), "{body}");
        }
    }

    #[test]
    fn duplicated_method_keeps_the_last() {
        let last_submission =
            r#"{"method":"eth_call","method":"eth_sendRawTransaction","params":[],"id":1}"#;
        assert_eq!(classify(last_submission.as_bytes()), Calls::Submissions);
        let last_read =
            r#"{"method":"eth_sendRawTransaction","method":"eth_call","params":[],"id":1}"#;
        assert_eq!(classify(last_read.as_bytes()), Calls::Queries);
        let batch = format!("[{last_read},{}]", call("eth_sendRawTransaction"));
        assert_eq!(classify(batch.as_bytes()), Calls::MixedBatch);
    }

    /// `use_rustls_tls` exists only when a rustls feature is enabled on
    /// reqwest, so this stops compiling if the gateway's manifest drops it and
    /// the crate is built on its own (`-p tn-worker-gateway`). A whole-workspace
    /// build can hide that through feature unification, which is why the
    /// standalone build is gated too.
    #[test]
    fn rustls_backend_is_compiled_in() {
        let bare = Client::builder().use_rustls_tls().build();
        assert!(bare.is_ok(), "{bare:?}");
        let proxy = proxy_client(Duration::from_secs(1), Duration::from_secs(1));
        assert!(proxy.is_ok(), "{proxy:?}");
    }
}
