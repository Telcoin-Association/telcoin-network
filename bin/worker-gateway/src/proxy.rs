//! JSON-RPC reverse-proxy handler.
//!
//! Forwards a client's `POST` (its JSON-RPC body and content type) to the
//! first ready upstream worker and returns the upstream's status, body, and
//! content type; a request with any other method is answered locally with a
//! `405` error envelope before its body is read. Other headers are not
//! forwarded in either direction, with three deliberate additions on the
//! upstream hop: `X-Forwarded-For` / `X-Forwarded-Proto` (client identity for
//! the upstream's logs and any per-client limits it applies) and the
//! `X-TN-Gateway` hop marker (loop protection; an inbound request that already
//! carries it is rejected instead of forwarded). The two identity headers are
//! built from the client the gateway resolved, never copied from the request:
//! `X-Forwarded-For` names the peer, or the forwarded client and then the
//! trusted proxy that vouched for it, and `X-Forwarded-Proto` is `http` unless
//! a trusted proxy reported `https` (see [`crate::identity`]). The upstream
//! response body is streamed through, never buffered whole, except the small
//! answer to a submission whose hash the gateway would remember (see
//! [`submission_accepted`]). When no upstream is ready, or the upstream cannot
//! be reached / times out, the client receives a well-formed JSON-RPC error
//! instead (see [`crate::error`]). So does a client whose upstream answers an
//! error status without a JSON body (a worker's `429` text/plain, say): it
//! keeps the upstream's status, with a JSON-RPC error in place of the body.
//!
//! With `--redirect-queries` set, transaction submissions go to the worker,
//! and so do the few single reads only its pool can answer: a nonce at the
//! `pending` tag, and a lookup of a submission this gateway forwarded recently
//! (see [`scan`] and [`PoolRead`]). Every other call goes to the query
//! upstream, which is not readiness-gated, never falls back to the worker,
//! gets the `X-TN-Gateway-Redirect` marker in place of `X-TN-Gateway`, plus the
//! `--redirect-queries-header` header when one is configured (see
//! [`QueryHeader`]; a worker never receives it), and has its response content
//! type passed through only when it is JSON. Without `--redirect-queries`, a
//! call whose method starts with a `--denied-method-prefixes` entry is refused
//! before forwarding (see [`Scan::denied`]).
//!
//! Every request is classified, redirect or not: a submission counts against
//! `--max-inflight-submissions` and everything else against
//! `--max-inflight-queries`. A request whose class has no free slot is answered
//! at once with a `503` overload error instead of waiting, so a stalled route
//! cannot pile up connections that the other route needs. A forward to a
//! worker also takes a slot on `--max-upstream-inflight`, the same way, which
//! bounds this gateway's share of the worker's RPC connection limit; with
//! `--redirect-queries`, the reads kept on the worker hold at most a fifth of
//! those slots, so they cannot crowd out the submissions.

use std::{
    borrow::Cow,
    fmt,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, OnceLock,
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{Body, Bytes, HttpBody as _},
    extract::{
        rejection::{BytesRejection, FailedToBufferBody},
        FromRequest as _, FromRequestParts, Request, State,
    },
    http::{header, request::Parts, HeaderMap, HeaderName, HeaderValue, Method, StatusCode},
    response::Response,
    Extension, RequestExt as _,
};
use futures::{Stream, StreamExt as _};
use reqwest::{redirect::Policy, Client, ClientBuilder, RequestBuilder};
use serde::{
    de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use serde_json::de::SliceRead;
use tn_types::{Decodable2718, PooledTransaction, Typed2718, B256};
use tracing::{debug, warn};
use url::Url;

use crate::{
    cli::Settings,
    dns::{self, CachingResolver, LookupHost},
    error::{
        error_response, error_response_with_id, upstream_error_response, GatewayError, RequestId,
    },
    identity::{Forwarding, X_FORWARDED_FOR, X_FORWARDED_PROTO},
    ratelimit::{RateLimiters, SubmissionBudget},
    server::{AccessSampled, AppState, RecentSubmissions, PROXY_HEADER_WARNINGS},
    telemetry::{self, Throttle},
};

/// Default maximum request body the gateway will buffer before forwarding.
///
/// A guard against unbounded memory use; the effective limit is configurable
/// via `--max-request-bytes` (this value is that flag's default). 1 MiB is four
/// times the largest admissible submission: the worker's pool admits at most
/// 128 KiB of raw transaction (reth's `DEFAULT_MAX_TX_INPUT_BYTES`), about
/// 256 KiB once hex-encoded. Every in-flight request reserves its declared body
/// size, or this whole cap when the body is chunked, against the in-flight byte
/// budget (`--max-inflight-request-bytes`), which bounds the total buffered
/// across connections (see the README's "Request size" section).
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

/// The JSON-RPC methods that submit a transaction, and so the calls a gateway
/// with `--redirect-queries` always sends to the worker. They are exactly the
/// two methods the node's fee-cap guard replaces
/// (`crates/tn-reth/src/env/rpc.rs`), matched exactly and case-sensitively, as
/// jsonrpsee matches method names. Both contain [`SEND_RAW_TRANSACTION`], so
/// its substring test is a fast path for either.
const SUBMISSION_METHODS: [&str; 2] = [SEND_RAW_TRANSACTION, SEND_RAW_TRANSACTION_SYNC];

/// The nonce read a gateway with `--redirect-queries` keeps on the worker when
/// its block parameter is [`PENDING_TAG`] (see [`PoolRead`]).
const GET_TRANSACTION_COUNT: &str = "eth_getTransactionCount";

/// The lookups by hash a gateway with `--redirect-queries` keeps on the worker
/// for a submission it forwarded recently (see [`PoolRead`]).
const LOOKUPS_BY_HASH: [&str; 2] = ["eth_getTransactionByHash", "eth_getTransactionReceipt"];

/// The prefix [`GET_TRANSACTION_COUNT`] and both [`LOOKUPS_BY_HASH`] share, so
/// its substring test is a fast path for all three.
const POOL_READ_PREFIX: &str = "eth_getTransaction";

/// The block tag that asks for the chain state plus the node's pool.
const PENDING_TAG: &str = "pending";

/// Bytes beyond the request's own length that the proxy reads of a worker's
/// answer to a submission whose hash it would remember (see
/// [`submission_accepted`]). The answer is a transaction hash or a short error
/// object plus the request's `id` echoed back, and the id is the client's to
/// make as long as the request cap allows, so a fixed cap would turn a valid
/// submission with a long id into an error. A healthy worker stays far below
/// the request's length plus this.
pub(crate) const SUBMISSION_ANSWER_SLACK: usize = 4 * 1024;

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
///
/// The marker says only that some gateway redirected the request, not which
/// one, so it cannot tell a loop from a chain: a redirecting gateway must not
/// sit behind another redirecting gateway. With A redirecting to B and B
/// redirecting to C, every query through A gets `508` from B. A gateway
/// without `--redirect-queries` may sit anywhere on the query path. The
/// rejection counts under its own reason, `redirect_loop_detected`, apart
/// from the hop marker's `loop_detected`.
const REDIRECT_HEADER: HeaderName = HeaderName::from_static("x-tn-gateway-redirect");

/// Throttle for the hop-marker loop warning.
static HOP_LOOP_WARNINGS: Throttle = Throttle::new("hop_loop");

/// Throttle for the redirect-marker loop warning.
static REDIRECT_LOOP_WARNINGS: Throttle = Throttle::new("redirect_loop");

/// Throttle for the warning on a batch the scan cannot read to its end.
static UNREADABLE_BATCH_WARNINGS: Throttle = Throttle::new("unreadable_batch");

/// Throttle for the warning on a batch over `--max-batch-len`.
static BATCH_TOO_LONG_WARNINGS: Throttle = Throttle::new("batch_too_long");

/// Throttle for the raw-transaction screen's rejection warning.
static SCREEN_WARNINGS: Throttle = Throttle::new("screen");

/// Throttle for the no-ready-upstream warning.
static NO_UPSTREAM_WARNINGS: Throttle = Throttle::new("no_upstream");

/// Throttle for the oversized-body warning.
pub(crate) static OVERSIZED_BODY_WARNINGS: Throttle = Throttle::new("oversized_body");

/// Throttle for the warning on a worker's answer to a submission that could
/// not be read whole (see [`relay_submission_answer`]).
static SUBMISSION_ANSWER_WARNINGS: Throttle = Throttle::new("submission_answer");

/// Throttles for the forwarding-failure warning, one per route, so a failing
/// query upstream cannot hide a failing worker.
static FORWARD_FAILURE_WARNINGS: PerRoute =
    PerRoute::new("forward_failure_worker", "forward_failure_query");

/// Throttles for the upstream-error warning, one per route.
static UPSTREAM_ERROR_WARNINGS: PerRoute =
    PerRoute::new("upstream_error_worker", "upstream_error_query");

/// Every per-request (or, for a PROXY header, per-connection) warning
/// throttle, summarized by [`telemetry::run_throttle_summaries`].
pub(crate) static WARNING_THROTTLES: [&Throttle; 13] = [
    &HOP_LOOP_WARNINGS,
    &REDIRECT_LOOP_WARNINGS,
    &UNREADABLE_BATCH_WARNINGS,
    &BATCH_TOO_LONG_WARNINGS,
    &SCREEN_WARNINGS,
    &NO_UPSTREAM_WARNINGS,
    &OVERSIZED_BODY_WARNINGS,
    &SUBMISSION_ANSWER_WARNINGS,
    &FORWARD_FAILURE_WARNINGS.worker,
    &FORWARD_FAILURE_WARNINGS.query,
    &UPSTREAM_ERROR_WARNINGS.worker,
    &UPSTREAM_ERROR_WARNINGS.query,
    &PROXY_HEADER_WARNINGS,
];

/// The JSON-RPC call [`chain_id_request`] builds: the startup chain-id check
/// and the readiness probe of the query upstream both send it.
const CHAIN_ID_CALL: &str = r#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"#;

/// The largest `eth_chainId` answer [`fetch_chain_id`] reads; a real one is
/// under a hundred bytes.
const MAX_CHAIN_ID_ANSWER_BYTES: usize = 64 * 1024;

/// The access log's `route` for a request the gateway answered itself,
/// without forwarding it to an upstream.
pub(crate) const REJECTED_ROUTE: &str = "rejected";

/// Forward a JSON-RPC request to the first ready upstream worker or, when
/// `--redirect-queries` is set and the request is neither made only of
/// submissions nor one of the reads only the worker's pool can answer (see
/// [`PoolRead`]), to the query upstream. Without `--redirect-queries`, a
/// request that calls a method `--denied-method-prefixes` denies is refused
/// before it is charged or takes a slot (see [`Scan::denied`]).
///
/// A worker forward that fails to connect, a connect timeout included, counts
/// toward `--upstream-failure-threshold` (see
/// [`crate::readiness::GatewayReadiness::record_rpc_failure`]); one that gets
/// any response resets the count. A timeout after the request was sent does
/// not count, since the method and params decide how long the worker takes.
/// Startup keeps `--upstream-request-timeout` longer than
/// `--upstream-connect-timeout`, so a connection setup that stalls (the name
/// lookup, TCP or TLS) ends at the connect timeout, as a connect failure that
/// counts, before the request deadline can end it as an ordinary timeout.
///
/// Only a `POST` gets this far: [`PostOnly`] answers any other method before
/// the body is read. `forwarding_started` is the request-deadline
/// middleware's marker (see [`ForwardingStarted`]), set here just before the
/// request is sent upstream and again once the upstream has answered.
/// `limiters` is present when the rate-limit layer is installed and admitted
/// the request on its first token (see [`crate::ratelimit::rate_limit`]).
/// `submission_budget` is present when `--rate-limit-submissions` is set: the
/// edge then charged only the per-IP bucket, and the request's first call is
/// charged here to the global or the submission bucket once the request is
/// classified (see [`SubmissionBudget`]).
/// `request` is the final extractor: its headers are taken first, then its
/// body is read here, while the router's byte-budget layer
/// (`server::reserve_request_bytes`) holds the bytes it reserved for it, so it
/// must stay last in the parameter list.
///
/// When the access log samples the request, its [`AccessSampled`] extension
/// is taken from `request` (the handler is at its extractor limit) and the
/// proxy notes the request's class once it has read the body and its route
/// once it starts the forward (see [`AccessRecord`]); a request the log skips
/// is not classified for it at all.
pub(crate) async fn proxy(
    State(state): State<AppState>,
    Extension(forwarding): Extension<Forwarding>,
    _post: PostOnly,
    forwarding_started: Option<Extension<ForwardingStarted>>,
    limiters: Option<Extension<Arc<RateLimiters>>>,
    submission_budget: Option<Extension<SubmissionBudget>>,
    mut request: Request,
) -> Response {
    // the headers move out of the request first; reading the body needs only
    // its extensions (the body limit). the body is read before the guard below
    // is entered, so a client still uploading its body is not counted in flight
    // and the upload stays out of the duration
    let headers = std::mem::take(request.headers_mut());
    // the access log's record, on a request it samples
    let access =
        request.extensions_mut().remove::<AccessSampled>().map(|AccessSampled(record)| record);
    let body = read_body(request).await;

    // Track this proxied request in the in-flight gauge (the autoscaling signal)
    // and time it. The guard releases both on every return path below, except
    // a relayed response: its body takes the guard and releases it once it has
    // finished streaming to the client (or is dropped).
    let in_flight = telemetry::RequestInFlight::enter();

    let body = match body {
        Ok(body) => body,
        Err(response) => return response,
    };

    // A request that already carries the hop marker has passed through a
    // gateway before: some upstream URL points back at a gateway, and
    // forwarding again would loop until fds run out.
    if headers.contains_key(HOP_HEADER) {
        if let Some(suppressed) = HOP_LOOP_WARNINGS.admit() {
            warn!(
                target: "gateway::proxy",
                suppressed,
                "proxy loop detected (inbound request already carries the gateway hop marker); \
                 check that upstream URLs point at workers, not gateways"
            );
        }
        // the loop checks come before the scan, so a sampled request is
        // scanned here for the access log's class
        if let Some(access) = &access {
            access.classify(&body, scan(&body, None, &[]).calls);
        }
        return error_response(&GatewayError::LoopDetected, body.as_ref());
    }

    // a redirecting gateway that receives the redirect marker is being sent
    // reads that some gateway already redirected: its query upstream leads
    // back to a redirecting gateway, and redirecting again would loop. the
    // marker cannot tell that loop from a chain of two redirecting gateways,
    // which is refused the same way (see `REDIRECT_HEADER`). a gateway without
    // a redirect forwards such a request like any other, so a gateway can
    // front the public rpc.
    if state.query_upstream.is_some() && headers.contains_key(REDIRECT_HEADER) {
        if let Some(suppressed) = REDIRECT_LOOP_WARNINGS.admit() {
            warn!(
                target: "gateway::proxy",
                suppressed,
                "redirect loop detected (inbound request already carries the query-redirect \
                 marker); check that --redirect-queries does not lead to a gateway that redirects"
            );
        }
        if let Some(access) = &access {
            access.classify(&body, scan(&body, None, &[]).calls);
        }
        return error_response(&GatewayError::RedirectLoopDetected, body.as_ref());
    }

    // without a redirect every call would reach the worker, so the namespaces
    // a validator should not serve publicly are refused; with one, the query
    // upstream decides what it serves and the list is inert
    let denied_prefixes: &[String] =
        if state.query_upstream.is_none() { &state.denied_method_prefixes } else { &[] };

    // One pass over the body answers every question asked of it below: how
    // many calls it carries, whether the method policy or the transaction
    // screen refuses it, and where it routes.
    let scan = scan(body.as_ref(), state.max_batch_len, denied_prefixes);
    // a sampled request is classified for the access log here, whatever comes
    // of it below: a request answered without a forward is logged with its
    // class and the `rejected` route
    if let Some(access) = &access {
        access.classify(&body, scan.calls);
    }

    // A batch the scan could not read to its end has an unknown length: the
    // worker skips an element serde's typed readers refuse (an out-of-range
    // number, an unpaired surrogate escape) and runs the rest, so forwarding
    // it would dodge the length cap and the per-call charge. It is refused
    // whole, before it is charged, and has no single id to echo.
    if scan.unreadable_batch {
        if let Some(suppressed) = UNREADABLE_BATCH_WARNINGS.admit() {
            warn!(
                target: "gateway::proxy",
                suppressed,
                "rejecting a batch the gateway cannot read to its end"
            );
        }
        return error_response(&GatewayError::UnreadableBody, b"");
    }

    // A batch over the length cap is refused whole. A batch has no single id
    // to echo, so the error carries `null`.
    if state.max_batch_len.is_some_and(|max| scan.len > max.get()) {
        if let Some(suppressed) = BATCH_TOO_LONG_WARNINGS.admit() {
            warn!(
                target: "gateway::proxy",
                suppressed,
                "rejecting a batch longer than --max-batch-len"
            );
        }
        return error_response(&GatewayError::BatchTooLong, b"");
    }

    // A call the method policy refuses is answered before the batch charge
    // and before any slot, so it costs no more than the edge's first token. A
    // single call gets its own id; a batch, refused whole, gets `null`. The
    // method is the client's choice, not an operator fault, so this stays at
    // debug; the rejection metric counts it by reason.
    if let Some(id) = scan.denied {
        debug!(target: "gateway::proxy", "refusing a denied method before forwarding");
        return error_response_with_id(&GatewayError::MethodNotAllowed, id);
    }

    // the class picks the rate budget, the in-flight cap and, with a
    // redirect, the upstream
    let class = scan.calls.class();

    // with `--rate-limit-submissions` the edge charged only the per-IP bucket,
    // since it could not tell the classes apart: the first call is charged
    // here, a submission to its own budget and anything else to the global
    // bucket, and a refusal gives the per-IP token back, so a refused
    // submission spends no read budget and a refused read no submission budget
    if let Some(Extension(budget)) = &submission_budget {
        if let Err(err) = budget.charge(class == Class::Submission) {
            return error_response(&err, body.as_ref());
        }
    }

    // The first call is paid for, by the rate-limit layer before the body was
    // read or, with a submission budget, by the layer's per-IP charge and the
    // class charge above. A batch pays one more for each call beyond its first
    // before it is screened or forwarded, to the same client the layer charged
    // (the resolved `ClientAddr`, not the TCP peer), from the per-IP and
    // global buckets whatever its class. Like the layer's own refusal, this
    // one cannot name an id.
    let charged = limiters.as_ref().map_or(Ok(()), |Extension(limiters)| {
        limiters.charge_batch(Some(forwarding.client_addr().0), scan.len.saturating_sub(1))
    });
    if let Err(err) = charged {
        if let Some(Extension(budget)) = &submission_budget {
            budget.refund(class == Class::Submission);
        }
        return error_response(&err, b"");
    }

    // Shallow pre-flight for raw-transaction submissions, alone or in a batch
    // of nothing but submissions: reject a payload the worker would also
    // reject (undecodable, or a type the network does not accept) before
    // paying for an upstream round-trip. The scan recovers the refused call's
    // request id itself on the paths that reject, and hands back the hash of a
    // single `eth_sendRawTransaction` it decoded (`scan.submitted`), so nothing
    // decodes it twice.
    if let Some((err, id)) = scan.rejection {
        if let Some(suppressed) = SCREEN_WARNINGS.admit() {
            warn!(
                target: "gateway::proxy",
                ?err,
                suppressed,
                "rejecting a raw-transaction submission before forwarding"
            );
        }
        return error_response_with_id(&err, id);
    }

    // fail fast rather than queue: a route that has used up its slots (a
    // stalled query upstream, say) must not hold connections the other route
    // needs. the slot is held until the response body has been sent or
    // dropped (see `UpstreamBody`); every early return frees it at once.
    let class_slots = match class {
        Class::Submission => &state.submission_slots,
        Class::Query => &state.query_slots,
    };
    let Some(class_slot) = telemetry::InFlightSlot::route(class_slots, class.label()) else {
        debug!(
            target: "gateway::proxy",
            route = class.label(),
            "in-flight cap reached; rejecting request"
        );
        return error_response(&GatewayError::Overloaded, body.as_ref());
    };

    // with a redirect configured, every call but a submission or a read only
    // the worker's pool can answer goes to the query upstream, with no
    // readiness gate and no fallback to the worker: a fallback would put the
    // read load on the validator exactly when the public rpc is struggling.
    let recent = state.recent_submissions.as_deref();
    let query_upstream =
        state.query_upstream.as_ref().filter(|_| is_query(scan.calls, scan.pool_read, recent));
    let (route, upstream_url) = match query_upstream {
        Some(query_upstream) => (Route::Query, query_upstream.clone()),
        None => match state.readiness.first_ready_rpc_url() {
            Some(rpc_url) => (Route::Worker, rpc_url),
            None => {
                if let Some(suppressed) = NO_UPSTREAM_WARNINGS.admit() {
                    warn!(
                        target: "gateway::proxy",
                        suppressed,
                        "no upstream worker ready; rejecting request"
                    );
                }
                return error_response(&GatewayError::NoUpstreamReady, body.as_ref());
            }
        },
    };

    // with a redirect, the reads kept on the worker (see [`PoolRead`]) are
    // queries that take worker slots, so together they may hold at most a
    // fifth of them (`pool_read_slots`), failing fast like the caps above:
    // while the worker is slow they cannot take every worker slot and leave
    // submissions a `503`. without a redirect every read is a worker forward,
    // and the lowered query cap already keeps a fifth free for submissions.
    let pool_read_slot =
        if route == Route::Worker && class == Class::Query && state.query_upstream.is_some() {
            match Arc::clone(&state.pool_read_slots).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    debug!(
                        target: "gateway::proxy",
                        "worker pool-read cap reached; rejecting request"
                    );
                    return error_response(&GatewayError::Overloaded, body.as_ref());
                }
            }
        } else {
            None
        };

    // every gateway in front of a worker draws on its one rpc connection
    // limit, so this gateway holds at most `--max-upstream-inflight` of it,
    // failing fast like the class caps above. unlike the class slot, this one
    // is released at the response head: the worker frees its own permit once
    // it has built the response, before the body is written.
    let upstream_slot = match route {
        Route::Worker => match telemetry::InFlightSlot::upstream(&state.upstream_slots) {
            Some(slot) => Some(slot),
            None => {
                debug!(target: "gateway::proxy", "worker in-flight cap reached; rejecting request");
                return error_response(&GatewayError::Overloaded, body.as_ref());
            }
        },
        Route::Query => None,
    };

    // from here on the upstream may receive the request, so a deadline that
    // fires later can no longer tell the client it was not delivered, and the
    // access log names the route whatever the forward's result
    if let Some(Extension(forwarding_started)) = &forwarding_started {
        forwarding_started.mark();
    }
    if let Some(access) = &access {
        access.forwarding(route);
    }

    match forward(&state, route, &headers, body.clone(), upstream_url.clone(), forwarding).await {
        Ok(upstream) => {
            // the worker has answered with its head, so it no longer holds a
            // connection permit for this request: free its slots here, before
            // a submission's answer is read whole below or any body is relayed
            drop(upstream_slot);
            drop(pool_read_slot);
            // the upstream has answered: what is left only passes that answer
            // on, so the request deadline no longer cuts it (see
            // [`ForwardingStarted`])
            if let Some(Extension(forwarding_started)) = &forwarding_started {
                forwarding_started.mark_answered();
            }
            // any answer, an error status included, shows that the worker's
            // rpc port is up
            if route == Route::Worker {
                state.readiness.record_rpc_success(&upstream_url);
            }
            // an error status without a json body (a worker's `429`
            // text/plain, say) carries no json-rpc answer: replace it with the
            // gateway's envelope, keeping the status and echoing the request's
            // id
            if is_non_json_error(&upstream) {
                let status = upstream.status();
                telemetry::record_routed(route.label(), "upstream_error");
                if let Some(suppressed) = UPSTREAM_ERROR_WARNINGS.get(route).admit() {
                    warn!(
                        target: "gateway::proxy",
                        route = route.label(),
                        upstream = %UpstreamOrigin(&upstream_url),
                        status = status.as_u16(),
                        suppressed,
                        "upstream answered an error status without a JSON-RPC body"
                    );
                }
                return upstream_error_response(status, body.as_ref());
            }
            // a redirect with a json body passes through as the upstream's own
            // answer, but the gateway follows no redirect, so it is logged
            // like the non-json one above (which covers every other `3xx`): a
            // worker behind a front that answers `307`, say, is a
            // misconfiguration no client can fix
            if upstream.status().is_redirection() {
                if let Some(suppressed) = UPSTREAM_ERROR_WARNINGS.get(route).admit() {
                    warn!(
                        target: "gateway::proxy",
                        route = route.label(),
                        upstream = %UpstreamOrigin(&upstream_url),
                        status = upstream.status().as_u16(),
                        suppressed,
                        "upstream answered a redirect, which the gateway does not follow"
                    );
                }
            }
            // a single submission the worker took is remembered, so that a
            // lookup of its hash follows it to the worker's pool (see
            // [`PoolRead`]). the worker answers a transaction it refused with a
            // 2xx as well, carrying a json-rpc `error`, so only a json-rpc
            // `result` counts: the answer is read whole to tell, and only on
            // this path. it is read here, while the in-flight guard, the class
            // slot and the byte reservation are still held (the worker slot no
            // longer is); they drop when this returns
            let remember = recent
                .zip(scan.submitted)
                .filter(|_| route == Route::Worker && upstream.status().is_success());
            if let Some((recent, hash)) = remember {
                return relay_submission_answer(
                    upstream,
                    &upstream_url,
                    body.as_ref(),
                    recent,
                    hash,
                )
                .await;
            }
            telemetry::record_forwarded();
            telemetry::record_routed(route.label(), "forwarded");
            relay(upstream, route, upstream_url, in_flight, class_slot)
        }
        Err(source) => {
            let err = classify_error(&source);
            // only a failed connect shows that the worker's rpc port is down.
            // reqwest flags a connect timeout as a connect error too, so a
            // worker that drops syns still counts. a timeout or failure after
            // the request was sent neither counts nor resets the count: the
            // client's method and params decide how long the worker takes
            // (eth_sendRawTransactionSync waits up to 30 s for inclusion).
            // the request deadline also runs during connection setup, and
            // reqwest reports it the same way whether or not the request was
            // sent, so a bare timeout cannot count; startup keeps that deadline
            // longer than the connect timeout, which covers the name lookup,
            // tcp and tls, so a stalled setup always ends as a connect error
            // first. a lookup the gateway's own resolver refused because every
            // lookup slot was busy is a connect error too, but it says nothing
            // about the worker, so it does not count either. the gateway running
            // out of file descriptors (EMFILE, ENFILE) fails the connect too, and
            // says nothing about the worker either.
            let connect_failed = source.is_connect()
                && !dns::is_saturated(&source)
                && !crate::fd::is_exhausted(&source);
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
            if let Some(suppressed) = FORWARD_FAILURE_WARNINGS.get(route).admit() {
                warn!(
                    target: "gateway::proxy",
                    ?err,
                    route = route.label(),
                    upstream = %UpstreamOrigin(&upstream_url),
                    cause = %ErrorChain(&source),
                    suppressed,
                    "forwarding to upstream failed"
                );
            }
            // passive health: a worker whose rpc port keeps failing to connect
            // is marked not-ready, so later requests go to the next ready
            // upstream. this request is answered with its own error and never
            // retried.
            if route == Route::Worker && connect_failed {
                state.readiness.record_rpc_failure(&upstream_url);
            }
            error_response(&err, body.as_ref())
        }
    }
}

/// Whether a request goes to the query upstream rather than the worker,
/// counting a mixed batch on the way: anything but submissions (see
/// [`scan`]) and the reads only the worker's pool can answer (see
/// [`PoolRead`]).
fn is_query(calls: Calls, pool_read: Option<PoolRead>, recent: Option<&RecentSubmissions>) -> bool {
    if calls == Calls::MixedBatch {
        telemetry::record_mixed_batch();
    }
    calls.route() == Route::Query && !pool_read.is_some_and(|read| read.stays_on_worker(recent))
}

/// Whether a worker's answer to one submission is a JSON-RPC success: an
/// object with a non-null `result` member and no `error` member (a `null`
/// `error` counts as none). Anything else, an answer that is not JSON
/// included, means the worker did not take the transaction.
fn submission_accepted(answer: &[u8]) -> bool {
    /// The two members that tell a JSON-RPC success from an error; every other
    /// member is skipped in place.
    #[derive(serde::Deserialize)]
    struct Answer {
        result: Option<IgnoredAny>,
        error: Option<IgnoredAny>,
    }

    // a derived struct also deserializes from a json array, taking its
    // members in order, so an answer that is not an object is refused first
    answer.trim_ascii_start().starts_with(b"{")
        && serde_json::from_slice::<Answer>(answer)
            .is_ok_and(|answer| answer.result.is_some() && answer.error.is_none())
}

/// Pass on a worker's `2xx` answer to a single `eth_sendRawTransaction`,
/// remembering the transaction's `hash` in `recent` when the answer is a
/// JSON-RPC success (see [`submission_accepted`]).
///
/// The answer is read whole, at most the request's length plus
/// [`SUBMISSION_ANSWER_SLACK`], and passed on unchanged with the worker's
/// status and content type. An answer that cannot be read within the cap
/// (longer than it, a body that fails, or one that runs past the request
/// timeout) is lost: the client gets `502` / `-32001`, and the forward counts
/// once, as `routed_requests_total{route="worker", result="body_failed"}`,
/// never as `forwarded`.
async fn relay_submission_answer(
    upstream: reqwest::Response,
    upstream_url: &Url,
    request_body: &[u8],
    recent: &RecentSubmissions,
    hash: B256,
) -> Response {
    let status = upstream.status();
    let content_type = upstream.headers().get(header::CONTENT_TYPE).cloned();
    let cap = request_body.len().saturating_add(SUBMISSION_ANSWER_SLACK);
    match read_answer(upstream, cap).await {
        Ok(answer) => {
            if submission_accepted(&answer) {
                recent.record(hash);
            }
            telemetry::record_forwarded();
            telemetry::record_routed(Route::Worker.label(), "forwarded");
            upstream_response(status, content_type, Body::from(answer))
        }
        Err(lost) => {
            telemetry::record_routed(Route::Worker.label(), "body_failed");
            if let Some(suppressed) = SUBMISSION_ANSWER_WARNINGS.admit() {
                warn!(
                    target: "gateway::proxy",
                    route = Route::Worker.label(),
                    upstream = %UpstreamOrigin(upstream_url),
                    cause = %lost,
                    suppressed,
                    "reading the worker's answer to a submission failed"
                );
            }
            error_response(&GatewayError::UpstreamUnreachable, request_body)
        }
    }
}

/// Read an upstream's answer whole, giving up once it runs past `cap` bytes.
/// The read runs under the proxy client's request timeout, so an upstream
/// that stalls mid-answer fails it instead of holding it.
async fn read_answer(mut upstream: reqwest::Response, cap: usize) -> Result<Bytes, AnswerLost> {
    let mut answer = Vec::new();
    while let Some(chunk) =
        upstream.chunk().await.map_err(|err| AnswerLost::Failed(err.without_url()))?
    {
        // `answer` never holds more than `cap`, so this cannot underflow
        if chunk.len() > cap - answer.len() {
            return Err(AnswerLost::TooLong);
        }
        answer.extend_from_slice(&chunk);
    }
    Ok(answer.into())
}

/// Why [`read_answer`] could not read an answer whole.
enum AnswerLost {
    /// The answer ran past the cap.
    TooLong,
    /// The body failed or ran past the request timeout. The error carries no
    /// url, so it can be logged.
    Failed(reqwest::Error),
}

impl fmt::Display for AnswerLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLong => f.write_str("answer longer than the read cap"),
            Self::Failed(err) => ErrorChain(err).fmt(f),
        }
    }
}

/// Buffer the request body into one allocation.
///
/// A body with a declared length is copied chunk by chunk into a buffer of
/// exactly that size, so each read-buffer chunk is released as soon as it is
/// copied and the body is never copied again. The `Bytes` extractor would keep
/// every chunk until the body ends and then join them into a fresh buffer,
/// nearly doubling what a held body costs. A chunked body still goes through
/// the extractor, which enforces `--max-request-bytes` while reading.
async fn read_body(request: Request) -> Result<Bytes, Response> {
    let declared = request.body().size_hint().exact().and_then(|len| usize::try_from(len).ok());
    let Some(len) = declared else {
        return Bytes::from_request(request, &())
            .await
            .map_err(|rejection| reject_body(&rejection));
    };
    // the router's byte-budget layer (`server::reserve_request_bytes`, always
    // installed with the same cap as the body limit) refuses a declared length
    // over the cap before this runs, and hyper never yields more than the
    // declared length, so the body limit cannot trip here and the allocation is
    // bounded by the cap: a read error is the client aborting the body
    let mut body = Vec::with_capacity(len);
    let mut chunks = request.into_limited_body().into_data_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|err| {
            debug!(target: "gateway::proxy", %err, "failed to buffer request body");
            error_response(&GatewayError::UnreadableBody, b"")
        })?;
        body.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(body))
}

/// Answer a body-buffering failure: a length-limit trip is a client error worth
/// warning about; any other buffering failure (e.g. the client aborted mid-body)
/// is not "oversized" and is logged quietly at debug.
fn reject_body(rejection: &BytesRejection) -> Response {
    match rejection {
        BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_)) => {
            if let Some(suppressed) = OVERSIZED_BODY_WARNINGS.admit() {
                warn!(
                    target: "gateway::proxy",
                    %rejection,
                    suppressed,
                    "rejecting oversized request body"
                );
            }
            error_response(&GatewayError::RequestTooLarge, b"")
        }
        _ => {
            debug!(target: "gateway::proxy", %rejection, "failed to buffer request body");
            error_response(&GatewayError::UnreadableBody, b"")
        }
    }
}

/// Answer a request whose method is not `POST` with the `405` error envelope,
/// without reading its body or forwarding it (JSON-RPC over HTTP is `POST`
/// only).
pub(crate) fn reject_non_post(method: &Method) -> Response {
    debug!(target: "gateway::proxy", %method, "rejecting non-POST request");
    error_response(&GatewayError::NonPostMethod, b"")
}

/// Forward one request to `upstream_url` as a `POST` and return the
/// upstream's response with its body still unread (see [`relay`]).
///
/// The `route` picks the marker header: [`HOP_HEADER`] toward a worker,
/// [`REDIRECT_HEADER`] toward the query upstream, never both; the configured
/// [`QueryHeader`] goes to the query upstream only (see [`stamp_route`]).
/// `forwarding` supplies `X-Forwarded-For` and `X-Forwarded-Proto`; the
/// client's own values of those headers are not copied.
/// The query route also gets its own deadline (`--query-request-timeout`) in
/// place of the client's, so a stalled query upstream frees its slots sooner;
/// the worker route keeps the client's `--upstream-request-timeout`.
///
/// A transport failure is returned as the raw `reqwest` error so the caller can
/// log its cause before [`classify_error`] reduces it to a client-facing error.
async fn forward(
    state: &AppState,
    route: Route,
    headers: &HeaderMap,
    body: Bytes,
    upstream_url: Url,
    forwarding: Forwarding,
) -> Result<reqwest::Response, reqwest::Error> {
    // JSON-RPC is content-type `application/json`; preserve the client's header
    // when present, default to it otherwise.
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .cloned()
        .unwrap_or_else(|| HeaderValue::from_static("application/json"));

    let request = state.http.post(upstream_url).header(header::CONTENT_TYPE, content_type);
    let request = stamp_route(request, route, state.query_header.as_ref())
        .header(X_FORWARDED_FOR, forwarding.x_forwarded_for())
        .header(X_FORWARDED_PROTO, forwarding.x_forwarded_proto())
        .body(body);
    // a per-request timeout replaces the client's total timeout for this
    // request only
    let request = match (route, state.query_request_timeout) {
        (Route::Query, Some(timeout)) => request.timeout(timeout),
        _ => request,
    };
    request.send().await
}

/// Whether an upstream response is an error status without a JSON body, which
/// the gateway replaces with its own JSON-RPC error rather than relaying.
///
/// Every non-2xx status counts, `3xx` included (redirects are never followed;
/// see [`proxy_client`]). A JSON body, `application/json` or any `+json`
/// subtype with any parameters, is relayed unchanged whatever the status: it
/// is the upstream's own JSON-RPC error.
fn is_non_json_error(upstream: &reqwest::Response) -> bool {
    !upstream.status().is_success()
        && !is_json_content_type(upstream.headers().get(header::CONTENT_TYPE))
}

/// Whether a `Content-Type` names JSON: `application/json` or an
/// `application/*+json` subtype, in any letter case and whatever its
/// parameters (such as `charset`). A missing or unreadable header is not
/// JSON, and neither is a comma-separated list.
fn is_json_content_type(content_type: Option<&HeaderValue>) -> bool {
    let Some(Ok(content_type)) = content_type.map(HeaderValue::to_str) else {
        return false;
    };
    // a browser splits a Content-Type value on commas and takes the last valid
    // type (Fetch "extract a MIME type"), so a list must never pass on the
    // strength of its first element
    if content_type.contains(',') {
        return false;
    }
    let essence = content_type
        .split_once(';')
        .map_or(content_type, |(essence, _parameters)| essence)
        .trim()
        .to_ascii_lowercase();
    essence == "application/json"
        || essence.strip_prefix("application/").is_some_and(|subtype| subtype.ends_with("+json"))
}

/// Adapt an upstream response back into an axum response, preserving the
/// status, body, and content type.
///
/// The `route` decides how far the content type is trusted: a worker's passes
/// through as is, while the query upstream's passes only when it is JSON
/// ([`is_json_content_type`]) and is replaced by `application/json` otherwise.
/// The query upstream is outside the operator's control and its answer is
/// served on the validator's origin, so it must not be able to label that
/// answer HTML or script.
///
/// The body streams through an [`UpstreamBody`], so a failure after the head
/// has gone out is still logged and counted against `route`, and `in_flight`
/// and `class_slot` stay held until the body has streamed: the request stays
/// in the in-flight gauge and its duration, and on its class's in-flight cap.
fn relay(
    upstream: reqwest::Response,
    route: Route,
    upstream_url: Url,
    in_flight: telemetry::RequestInFlight,
    class_slot: telemetry::InFlightSlot,
) -> Response {
    let status = upstream.status();
    let upstream_content_type = upstream.headers().get(header::CONTENT_TYPE);
    let content_type = match route {
        Route::Worker => upstream_content_type.cloned(),
        Route::Query => Some(
            upstream_content_type
                .filter(|content_type| is_json_content_type(Some(content_type)))
                .cloned()
                .unwrap_or_else(|| HeaderValue::from_static("application/json")),
        ),
    };

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
    let body = UpstreamBody {
        inner: upstream.bytes_stream(),
        route,
        upstream_url,
        _in_flight: in_flight,
        _class_slot: class_slot,
    };
    upstream_response(status, content_type, Body::from_stream(body))
}

/// The response passed on for an upstream's answer: its status and content
/// type around `body`, and no other upstream header (a `3xx`'s `Location`
/// included; see [`proxy_client`]).
fn upstream_response(
    status: StatusCode,
    content_type: Option<HeaderValue>,
    body: Body,
) -> Response {
    let mut response = Response::new(body);
    *response.status_mut() = status;
    if let Some(content_type) = content_type {
        response.headers_mut().insert(header::CONTENT_TYPE, content_type);
    }
    response
}

/// An upstream's response body on its way to the client, logging and counting
/// a failure mid-stream.
///
/// By the time the body fails, its status and headers have gone to the
/// client, which therefore sees a truncated response (a chunked body without
/// its final chunk) whatever the gateway does. The failure is logged at debug,
/// naming the upstream by origin, and counted as
/// `routed_requests_total{route, result="body_failed"}` next to the
/// `forwarded` result the head already recorded.
///
/// It is the one wrapper around a relayed body, and it holds the request's
/// guards for as long as the body lives: [`proxy`] returns as soon as the
/// upstream's response head arrives, but the body streams to the client after
/// that, holding an inbound connection. hyper drops the body once it has
/// written the last chunk, or when the stream fails or the connection closes,
/// and that releases them.
struct UpstreamBody<S> {
    /// The upstream's body stream.
    inner: S,
    /// The route the request took.
    route: Route,
    /// The upstream the body comes from, only ever logged by origin.
    upstream_url: Url,
    /// Holds the request in the in-flight gauge and its duration timer until
    /// the body has finished streaming, or is dropped because the client went
    /// away or the connection closed.
    _in_flight: telemetry::RequestInFlight,
    /// The request's slot on its class's in-flight cap. Tying it to the body
    /// keeps a query upstream that sends its head and then stalls inside
    /// `--max-inflight-queries`, so it cannot take the connections
    /// submissions need.
    _class_slot: telemetry::InFlightSlot,
}

impl<S> Stream for UpstreamBody<S>
where
    S: Stream<Item = reqwest::Result<Bytes>> + Unpin,
{
    type Item = reqwest::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Err(err))) => {
                // a reqwest error's display can carry the request url, so the
                // cause is rendered with it removed
                let err = err.without_url();
                telemetry::record_routed(self.route.label(), "body_failed");
                debug!(
                    target: "gateway::proxy",
                    route = self.route.label(),
                    upstream = %UpstreamOrigin(&self.upstream_url),
                    cause = %ErrorChain(&err),
                    "upstream response body failed mid-stream"
                );
                Poll::Ready(Some(Err(err)))
            }
            polled => polled,
        }
    }
}

/// Stamp the marker for `route` on a request bound upstream and, toward the
/// query upstream only, the configured [`QueryHeader`]: a worker never
/// receives it.
///
/// It is the one place the header is set: a forward ([`forward`]), the startup
/// chain-id check and the readiness probe ([`chain_id_request`]) all pass
/// through it.
fn stamp_route(
    request: RequestBuilder,
    route: Route,
    query_header: Option<&QueryHeader>,
) -> RequestBuilder {
    match route {
        Route::Worker => request.header(HOP_HEADER, HeaderValue::from_static("1")),
        Route::Query => {
            let request = request.header(REDIRECT_HEADER, HeaderValue::from_static("1"));
            match query_header {
                Some(query_header) => {
                    request.header(query_header.name.clone(), query_header.value.clone())
                }
                None => request,
            }
        }
    }
}

/// One `eth_chainId` call to `url`, stamped for `route` like a forwarded call
/// (see [`stamp_route`]): toward the query upstream it carries the configured
/// `query_header`, toward a worker never.
///
/// Both requests the gateway makes on its own start here: the startup
/// chain-id check ([`fetch_chain_id`]) and the readiness poller's probe of the
/// query upstream ([`crate::readiness::QueryProbe`]), so the probe reaches an
/// upstream that requires the header the way a redirected read does.
pub(crate) fn chain_id_request(
    client: &Client,
    route: Route,
    url: &Url,
    query_header: Option<&QueryHeader>,
) -> RequestBuilder {
    let request = client
        .post(url.clone())
        .header(header::CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(CHAIN_ID_CALL);
    stamp_route(request, route, query_header)
}

/// Ask an upstream for its chain id with one `eth_chainId` call, stamped for
/// `route` like a forwarded call (see [`stamp_route`]): the query upstream gets
/// the configured `query_header` and a worker never does.
///
/// Send it through the proxy client so `--upstream-request-timeout` bounds the
/// whole exchange, answer included; the answer is read up to
/// [`MAX_CHAIN_ID_ANSWER_BYTES`]. The error is a reason that is safe to log: it
/// never carries the URL, the answer or the header.
pub(crate) async fn fetch_chain_id(
    client: &Client,
    route: Route,
    url: &Url,
    query_header: Option<&QueryHeader>,
) -> Result<u64, String> {
    // reqwest's `Display` appends the request url, which can carry a credential
    let transport = |err: reqwest::Error| ErrorChain(&err.without_url()).to_string();
    let mut response =
        chain_id_request(client, route, url, query_header).send().await.map_err(transport)?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("answered HTTP {status}"));
    }
    let mut answer = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(transport)? {
        if answer.len().saturating_add(chunk.len()) > MAX_CHAIN_ID_ANSWER_BYTES {
            return Err(format!("the answer exceeds {MAX_CHAIN_ID_ANSWER_BYTES} bytes"));
        }
        answer.extend_from_slice(&chunk);
    }
    parse_chain_id(&answer)
        .ok_or_else(|| String::from("the answer is not a JSON-RPC result holding a hex chain id"))
}

/// The chain id in an `eth_chainId` answer: a `result` holding a `0x`-prefixed
/// hex quantity that fits a `u64`.
fn parse_chain_id(answer: &[u8]) -> Option<u64> {
    /// The one member of the answer that matters.
    #[derive(serde::Deserialize)]
    struct ChainIdAnswer {
        /// The chain id as a hex quantity.
        result: String,
    }
    let answer: ChainIdAnswer = serde_json::from_slice(answer).ok()?;
    let hex = answer.result.strip_prefix("0x")?;
    // `from_str_radix` also takes a leading `+`, which a quantity never has
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Start a client for the upstream hop with the settings every upstream client
/// shares: rustls, trusting the platform's native root store plus every
/// `--upstream-ca-cert` certificate, the `--upstream-client-cert` identity when
/// one is configured, no proxy, the upstream connect timeout, and a resolver
/// over `lookup` (the system resolver, [`dns::SystemLookup`], outside tests).
///
/// The forwarding client ([`proxy_client`]) and the readiness poller's client
/// ([`readiness_client`]) both start here and can only be built from here, so
/// a setting that governs how the gateway reaches an upstream cannot reach one
/// hop and silently miss the other: an `https` worker URL and its readiness
/// URL are verified the same way and see the same client certificate. Both
/// finish the builder with redirects off, so neither hop follows a `3xx`
/// anywhere. The forwarding client also
/// serves the `--redirect-queries` route and the poller's probe of it (see
/// [`crate::readiness::QueryProbe`]), so the extra CA certificates are trusted
/// there too, and the client certificate goes to that endpoint if it asks for
/// one (rustls sends it only on request). An upstream whose
/// certificate chains to none of these roots fails the handshake: a forward to
/// it fails and a poll marks it not ready, so a wrong CA fails closed.
///
/// Proxies are off. reqwest otherwise sends every request through
/// `HTTP_PROXY`, `HTTPS_PROXY` or `ALL_PROXY` from the environment, loopback
/// and private addresses included unless `NO_PROXY` lists them, so a variable
/// set for some other program would carry forwarded calls, readiness polls
/// and the upstreams' answers through a host the operator never chose.
///
/// Upstream names resolve through a [`CachingResolver`] that caches each
/// host's addresses for `--dns-cache-ttl` and caps how many lookups run at once
/// at `--max-concurrent-dns-lookups` (see [`crate::dns`]). Each call installs a
/// new one, so every client built from here has its own cache and its own cap
/// (see [`crate::app::UpstreamClients`]). A lookup the resolver refuses fails
/// the connection at once, which [`classify_error`] reports as
/// `UpstreamUnreachable`; passive health does not count that refusal against
/// the worker (see [`dns::is_saturated`]).
pub(crate) fn client_builder(
    settings: &Settings,
    lookup: Arc<dyn LookupHost>,
) -> UpstreamClientBuilder {
    let resolver =
        CachingResolver::new(lookup, settings.dns_cache_ttl, settings.max_concurrent_dns_lookups);
    let builder = settings.upstream_ca_certs.iter().cloned().fold(
        Client::builder()
            .use_rustls_tls()
            .no_proxy()
            .connect_timeout(settings.upstream_connect_timeout)
            .dns_resolver(Arc::new(resolver)),
        ClientBuilder::add_root_certificate,
    );
    UpstreamClientBuilder(match &settings.upstream_identity {
        Some(identity) => builder.identity(identity.clone()),
        None => builder,
    })
}

/// A builder that only [`client_builder`] can make (the field is private to
/// this module), so both upstream hops share every upstream setting (the TLS
/// inputs, the resolver, the no-proxy policy) and one added there later
/// reaches both.
pub(crate) struct UpstreamClientBuilder(ClientBuilder);

#[cfg(test)]
impl UpstreamClientBuilder {
    /// A builder with [`client_builder`]'s rustls backend, no-proxy policy and
    /// `connect_timeout`, but no `Settings`: no extra CA, no client
    /// certificate and the default resolver. For tests whose upstreams are
    /// plain-`http` IP literals.
    pub(crate) fn for_test(connect_timeout: Duration) -> Self {
        Self(Client::builder().use_rustls_tls().no_proxy().connect_timeout(connect_timeout))
    }
}

/// Finish `builder` (see [`client_builder`]) into the client that forwards
/// requests on both routes.
///
/// Redirects are never followed. reqwest follows up to ten by default and
/// replays a POST body on `307`/`308`, so a query upstream answering with a
/// redirect (a misbehaving or compromised public RPC, or anything on the path
/// to it) could otherwise bounce a read onto the private worker or any internal
/// host the gateway can reach. A `3xx` from either upstream is passed through
/// to the client like any other status, without its `Location` (only
/// `Content-Type` is copied back), so the client cannot follow it either, and
/// [`proxy`] logs a warning for it, whatever its body.
///
/// TLS, the connect timeout, the resolver and the no-proxy policy come from
/// `builder`. reqwest builds a client over an empty native root store without
/// complaint, so startup checks the store itself when an `https` upstream
/// depends on it (see [`crate::app`]).
pub(crate) fn proxy_client(
    builder: UpstreamClientBuilder,
    request_timeout: Duration,
) -> reqwest::Result<Client> {
    builder.0.redirect(Policy::none()).user_agent(USER_AGENT).timeout(request_timeout).build()
}

/// Finish `builder` (see [`client_builder`]) into the readiness poller's
/// client.
///
/// Redirects are never followed here either. A readiness endpoint answering
/// with a redirect would otherwise send the poller to whatever host its
/// `Location` names, any internal host the gateway can reach included, and a
/// ready payload served there would mark the worker ready on another node's
/// word. The poll gets the `3xx` response itself instead and counts it as
/// not ready, so only a `2xx` answer from the configured readiness URL can
/// mark its worker ready.
///
/// TLS, the connect timeout, the resolver and the no-proxy policy come from
/// `builder`, as for [`proxy_client`]. There is no request timeout: the
/// poller bounds each poll with `--readiness-poll-timeout` (see
/// [`crate::readiness::run_poller`]).
pub(crate) fn readiness_client(builder: UpstreamClientBuilder) -> reqwest::Result<Client> {
    builder.0.redirect(Policy::none()).build()
}

/// Classify a `reqwest` forwarding failure into a client-facing gateway error.
fn classify_error(err: &reqwest::Error) -> GatewayError {
    if err.is_timeout() {
        GatewayError::UpstreamTimeout
    } else {
        GatewayError::UpstreamUnreachable
    }
}

/// Extractor that admits only `POST` requests and answers any other method
/// through [`reject_non_post`], with `Allow: POST`.
///
/// It reads nothing but the request head, so ahead of the body extractor it
/// refuses a request before any of its body is read: a client cannot make the
/// gateway buffer a body it is going to refuse anyway.
pub(crate) struct PostOnly;

impl<S: Send + Sync> FromRequestParts<S> for PostOnly {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if parts.method == Method::POST {
            return Ok(Self);
        }
        // a 405 must name the methods the resource supports (rfc 9110,
        // section 15.5.6)
        let mut response = reject_non_post(&parts.method);
        response.headers_mut().insert(header::ALLOW, HeaderValue::from_static("POST"));
        Err(response)
    }
}

/// How far [`proxy`] has got with sending a request upstream.
///
/// The request-deadline middleware (`crate::server::enforce_request_deadline`)
/// puts a fresh marker in every request's extensions and reads it when the
/// deadline fires: a request that never started forwarding cannot have reached
/// an upstream, one that did may have, and the client is told which. Once the
/// upstream has answered, the deadline no longer cuts the request: all that is
/// left is passing that answer on (the worker's answer to a submission is read
/// whole first, under the proxy client's request timeout), and cutting it then
/// would turn an answer the upstream already gave, a transaction it accepted
/// included, into a timeout.
#[derive(Clone, Debug, Default)]
pub(crate) struct ForwardingStarted(Arc<AtomicU8>);

impl ForwardingStarted {
    /// The request is about to be sent upstream.
    const STARTED: u8 = 1;
    /// The upstream has answered with a response head.
    const ANSWERED: u8 = 2;

    /// Record that the request is about to be sent upstream.
    fn mark(&self) {
        self.0.store(Self::STARTED, Ordering::Release);
    }

    /// Record that the upstream has answered.
    fn mark_answered(&self) {
        self.0.store(Self::ANSWERED, Ordering::Release);
    }

    /// Whether the request may have reached an upstream.
    pub(crate) fn is_marked(&self) -> bool {
        self.0.load(Ordering::Acquire) >= Self::STARTED
    }

    /// Whether the upstream has answered, so that only passing the answer on
    /// is left.
    pub(crate) fn is_answered(&self) -> bool {
        self.0.load(Ordering::Acquire) == Self::ANSWERED
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

/// A header sent on every request to the query upstream and never to a worker
/// (`--redirect-queries-header`), such as a public RPC's API key.
///
/// Every redirected read leaves from the gateway's address, so a public RPC
/// that meters by address would charge all of this gateway's clients to one
/// quota; a key lets it meter the gateway instead. The value is marked
/// sensitive and `Debug` prints only `<redacted>`, so neither [`AppState`] nor
/// the resolved settings can leak it through a debug print.
#[derive(Clone)]
pub(crate) struct QueryHeader {
    /// The header name.
    name: HeaderName,
    /// The header value, marked sensitive.
    value: HeaderValue,
}

impl QueryHeader {
    /// Parse one `Name: value` line, trimming whitespace around the line, the
    /// name and the value.
    ///
    /// Errors name the problem but never echo any part of the line, which can
    /// hold a credential even when it is malformed.
    pub(crate) fn parse(line: &str) -> eyre::Result<Self> {
        let (name, value) = line
            .trim()
            .split_once(':')
            .ok_or_else(|| eyre::eyre!("expected one `Name: value` line, found no `:`"))?;
        let name = HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| eyre::eyre!("the header name is not a valid HTTP header name"))?;
        let value = value.trim();
        eyre::ensure!(!value.is_empty(), "the header value is empty");
        let mut value = HeaderValue::from_str(value).map_err(|_| {
            eyre::eyre!(
                "the header value is not a valid HTTP header value (one line, no control \
                 characters)"
            )
        })?;
        value.set_sensitive(true);
        Ok(Self { name, value })
    }

    /// The header's name and value, for tests.
    #[cfg(test)]
    pub(crate) fn parts(&self) -> (&HeaderName, &HeaderValue) {
        (&self.name, &self.value)
    }
}

impl fmt::Debug for QueryHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
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

/// What the proxy learned about a request the access log samples, kept in
/// the request's [`AccessSampled`] slot for the log to read as it writes the
/// request's line (see [`crate::server::AccessLog`]): the class and size
/// once the body has been read and classified, and the route once a forward
/// has started. A request answered or dropped before its body was read has
/// neither.
///
/// It holds two fixed labels and a length, never text copied from the
/// request, so the line built from it cannot carry a URL, header value, body,
/// params or method name.
#[derive(Debug, Default)]
pub(crate) struct AccessRecord {
    /// Method class (`batch`, `submission` or `query`) and the buffered
    /// body's length in bytes, set once the proxy has classified the body.
    classified: OnceLock<(&'static str, u64)>,
    /// The upstream a forward was started to, whatever its result (a request
    /// timeout or a cancellation included).
    forwarded: OnceLock<Route>,
}

impl AccessRecord {
    /// Note the class of a request whose body the proxy read: `batch` for a
    /// JSON-RPC batch, whatever it holds; otherwise `submission` for a single
    /// call to one of the [`SUBMISSION_METHODS`] and `query` for anything
    /// else, a body that is not JSON-RPC included, as the scan's `calls` has
    /// it. Only the first note counts.
    fn classify(&self, body: &[u8], calls: Calls) {
        let class = if is_batch(body.trim_ascii_start()) { "batch" } else { calls.class().label() };
        let bytes_in = u64::try_from(body.len()).unwrap_or(u64::MAX);
        self.classified.get_or_init(|| (class, bytes_in));
    }

    /// Note that a forward to `route` has started.
    fn forwarding(&self, route: Route) {
        self.forwarded.get_or_init(|| route);
    }

    /// The method class and the body's length, or `None` when the request was
    /// answered or dropped before the proxy read and classified its body.
    pub(crate) fn class(&self) -> Option<(&'static str, u64)> {
        self.classified.get().copied()
    }

    /// `worker` or `query` once a forward has started, or [`REJECTED_ROUTE`]
    /// for a request the gateway answered without forwarding it.
    pub(crate) fn route(&self) -> &'static str {
        self.forwarded.get().map_or(REJECTED_ROUTE, |route| route.label())
    }
}

/// Which upstream a request goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    /// The first ready worker: every call without `--redirect-queries`, and
    /// with it only submissions and the reads only its pool can answer.
    Worker,
    /// The `--redirect-queries` endpoint: every other call.
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

/// One [`Throttle`] per [`Route`], for a log call site that serves both.
struct PerRoute {
    /// The worker route's throttle.
    worker: Throttle,
    /// The query route's throttle.
    query: Throttle,
}

impl PerRoute {
    /// Two fresh throttles, whose summary lines name them `worker_site` and
    /// `query_site`.
    const fn new(worker_site: &'static str, query_site: &'static str) -> Self {
        Self { worker: Throttle::new(worker_site), query: Throttle::new(query_site) }
    }

    /// The throttle for `route`.
    fn get(&self, route: Route) -> &Throttle {
        match route {
            Route::Worker => &self.worker,
            Route::Query => &self.query,
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

    /// Only an all-submission body is in the submission class; a mixed batch
    /// counts as a query, as it is routed.
    fn class(self) -> Class {
        match self {
            Self::Submissions => Class::Submission,
            Self::Queries | Self::MixedBatch => Class::Query,
        }
    }
}

/// The capacity class a request counts against, whether or not
/// `--redirect-queries` is set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    /// A request made only of submissions.
    Submission,
    /// Everything else.
    Query,
}

impl Class {
    /// The `route` label on `tn_worker_gateway_route_inflight`.
    fn label(self) -> &'static str {
        match self {
            Self::Submission => "submission",
            Self::Query => "query",
        }
    }
}

/// What one pass over a request body found: where the body routes and whether
/// the transaction screen refuses it.
struct Scan {
    /// How many calls the body carries: 1 for anything but a batch, and for a
    /// batch its element count, which stops one past the length cap.
    len: usize,
    /// How the body routes under `--redirect-queries`, and so its in-flight
    /// class.
    calls: Calls,
    /// The screen's verdict: the error and the request id to answer it with,
    /// or `None` to forward the body.
    rejection: Option<(GatewayError, RequestId)>,
    /// The hash of a single `eth_sendRawTransaction` whose transaction the
    /// screen decoded and passed, which the proxy remembers once the worker
    /// takes it (see [`RecentSubmissions`]). A submission in a batch and an
    /// `eth_sendRawTransactionSync`, which answers with the receipt, have
    /// none.
    submitted: Option<B256>,
    /// A single call only the worker's pool can answer, which a gateway with
    /// `--redirect-queries` may keep on the worker (see [`PoolRead`]).
    pool_read: Option<PoolRead>,
    /// A batch the pass could not read to its end for any reason but the
    /// length cap, so `len` is only a lower bound on its calls.
    unreadable_batch: bool,
    /// The method policy's verdict (`--denied-method-prefixes`, in force only
    /// without `--redirect-queries`): the id to answer
    /// [`GatewayError::MethodNotAllowed`] with, a single call's own or `null`
    /// for a batch, or `None` to go on.
    denied: Option<RequestId>,
}

/// Read a request body once, counting its calls, classifying it for its
/// in-flight cap and for `--redirect-queries`, and screening its
/// raw-transaction submission in the same pass.
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
/// is not JSON, a `method` that is not a string, a call with more than one
/// `method` member, an empty batch, and bytes trailing the JSON value. A batch
/// holding a call with more than one `method` is unreadable, like any batch
/// the pass cannot read to its end. `eth_sendTransaction` is a query too: no
/// node configures a signer, so the worker could only refuse it.
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
/// decoded or is of a type the network does not accept (see
/// [`screen_transaction`]). In a batch the first refused element answers for
/// the whole batch, with its own id, and later elements are not decoded.
/// Every other request is forwarded unchanged: other methods, a mixed batch,
/// a submission whose params are named or structurally off (the worker
/// answers with its own parameter error), and a single call that is not
/// exactly one JSON value. A single `eth_sendRawTransaction` the screen passes
/// hands back its transaction's hash.
///
/// Pool reads: a single call that is exactly one JSON value is also checked
/// for the reads only the worker's pool can answer (see [`PoolRead`]); a
/// batch never is, so it keeps the route its calls give it.
///
/// Method policy: `denied_prefixes` is the `--denied-method-prefixes` list in
/// force, empty with a redirect. A body is denied when any call in it, alone
/// or anywhere in a batch, names a method that starts with one of the
/// prefixes, matched case-sensitively from the first character as jsonrpsee
/// matches names, after unicode escapes are decoded. A batch element that is
/// an array names the method jsonrpsee would run from it, its third member
/// when that is a string (see [`Element::Positional`]); a `method` that is not
/// a string, and a batch element that is neither an object nor an array, name
/// nothing. With a list in force the pass reads every call: neither the
/// single-call fast path nor the skip of a mixed batch's remaining elements is
/// taken. A body that starts with `{` or `[` but that the pass cannot read
/// (short of the length cap) is denied too: the worker's server decodes each
/// batch element on its own and runs the calls around one it cannot decode, so
/// an unreadable body is no proof that no denied call runs. A body that starts
/// with anything else is refused unread by the worker's server, and is not
/// denied.
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
fn scan(body: &[u8], max_batch_len: Option<NonZeroUsize>, denied_prefixes: &[String]) -> Scan {
    // the worker's server drops leading ascii whitespace, form feed included,
    // before it reads a body, where json allows only four whitespace bytes;
    // the scan reads the body the worker reads, so a prefix cannot hide a
    // batch from the count or a call from the screen
    let body = body.trim_ascii_start();
    let batch = is_batch(body);
    // fast path: a body that is not a batch and can name neither a
    // submission method nor a pool read is one call that routes as a query,
    // and nothing is parsed. The method policy needs every call's name, so a
    // deny list in force turns the fast path off.
    if !batch && denied_prefixes.is_empty() && !may_name_a_routed_method(body) {
        return Scan {
            len: 1,
            calls: Calls::Queries,
            rejection: None,
            submitted: None,
            pool_read: None,
            unreadable_batch: false,
            denied: None,
        };
    }
    let mut state = ScanState { denied_prefixes, ..ScanState::default() };
    let mut deserializer = body_deserializer(body);
    // whether the visitor read the whole value is kept apart from the
    // trailing-bytes check: only the first decides whether the method policy
    // saw every call the worker could run
    let read =
        deserializer.deserialize_any(ScanVisitor { state: &mut state, max_batch_len }).is_ok();
    let parsed = read && deserializer.end().is_ok();
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
    // a single call's hash and pool read stand only when it is exactly one
    // json value, like the verdict
    let submitted = state.submitted.filter(|_| calls == Calls::Submissions);
    let pool_read = state.pool_read.filter(|_| parsed);
    // with a deny list in force, a body the worker's server would decode
    // (one that starts with `{` or `[`) and that the pass could not read is
    // denied as well; one cut short by the length cap is refused anyway. A
    // single call answers with its own id, a batch with `null`.
    let unread = !denied_prefixes.is_empty()
        && !read
        && !state.capped
        && matches!(body.first(), Some(b'{' | b'['));
    let denied = (state.denied || unread).then(|| RequestId::recover(body));
    Scan {
        len: if batch { state.len } else { 1 },
        calls,
        rejection,
        submitted,
        pool_read,
        unreadable_batch,
        denied,
    }
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
struct ScanState<'p> {
    /// The method policy's prefixes, empty when no policy is in force (a
    /// redirect, or an empty `--denied-method-prefixes`).
    denied_prefixes: &'p [String],
    /// A call read so far, a positional batch element included, names a
    /// method that starts with one of `denied_prefixes`.
    denied: bool,
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
    /// The hash of a single `eth_sendRawTransaction` the screen passed.
    submitted: Option<B256>,
    /// A single call only the worker's pool can answer.
    pool_read: Option<PoolRead>,
}

impl ScanState<'_> {
    /// Record one call by its method, and whether the method policy denies
    /// it.
    fn record(&mut self, call: &Call<'_>) {
        if call.method.is_submission() {
            self.submission = true;
        } else {
            self.other = true;
        }
        self.denied |= self.denies(call.name.as_deref());
    }

    /// Whether the method policy denies a call named `name`: the name starts
    /// with one of `denied_prefixes`, matched case-sensitively from the first
    /// character. A call that names nothing is never denied.
    fn denies(&self, name: Option<&str>) -> bool {
        name.is_some_and(|name| {
            self.denied_prefixes.iter().any(|prefix| name.starts_with(prefix.as_str()))
        })
    }

    /// Record one batch element; one that is not an object counts as another
    /// call, and an array is checked against the method policy by the name
    /// jsonrpsee would read from it (see [`Element::Positional`]). A
    /// submission is screened while the batch can still be all submissions,
    /// up to the first one the screen refuses.
    fn record_element(&mut self, element: Element<'_>) {
        match element {
            Element::Call(call) => {
                self.record(&call);
                if !self.other && self.rejection.is_none() {
                    self.rejection = call.screen().err().map(|err| (err, Some(self.len)));
                }
            }
            Element::Positional(name) => {
                self.other = true;
                self.denied |= self.denies(name.as_deref());
            }
            Element::Other => self.other = true,
        }
    }

    /// Whether the calls read so far mix submissions with other calls.
    fn is_mixed(&self) -> bool {
        self.submission && self.other
    }

    /// Whether the rest of a batch can be skipped unread: the calls read so
    /// far settle its route (it is mixed), and no method policy needs the
    /// names of the calls still to come.
    fn can_skip_the_rest(&self) -> bool {
        self.is_mixed() && self.denied_prefixes.is_empty()
    }
}

/// Visitor behind [`scan`]: a single call or a batch of calls.
///
/// An error here is a verdict, not a failure, when it ends a batch scan early
/// (the batch is over the length cap); otherwise the body is not well-formed
/// JSON or holds a value serde's typed readers refuse. [`scan`] reads what the
/// state recorded either way.
struct ScanVisitor<'s, 'p> {
    /// Where the pass records what it has met.
    state: &'s mut ScanState<'p>,
    /// The length cap a batch count stops one past, or `None` for no cap.
    max_batch_len: Option<NonZeroUsize>,
}

impl<'de> Visitor<'de> for ScanVisitor<'_, '_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON-RPC request object or batch")
    }

    /// A single call: classified, screened when it is a submission, and
    /// checked for a pool read.
    fn visit_map<A: MapAccess<'de>>(self, members: A) -> Result<Self::Value, A::Error> {
        let call = Call::read(members)?;
        self.state.len = 1;
        self.state.record(&call);
        match call.screen() {
            Ok(tx) => {
                self.state.submitted = tx
                    .filter(|_| call.method == RpcMethod::Submission { sync: false })
                    .map(|tx| *tx.hash());
            }
            Err(err) => self.state.rejection = Some((err, None)),
        }
        self.state.pool_read = call.pool_read();
        Ok(())
    }

    /// A batch: counted and classified element by element. Once the batch is
    /// known to be mixed its route is settled, and the rest of it is only
    /// counted, each element skipped in place, unless a method policy is in
    /// force, which reads every call's name. The count stops one past the
    /// length cap, which ends the scan.
    fn visit_seq<A: SeqAccess<'de>>(self, mut elements: A) -> Result<Self::Value, A::Error> {
        let Self { state, max_batch_len } = self;
        loop {
            let read = if state.can_skip_the_rest() {
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
    /// The call's `method` as the worker reads it, unicode escapes decoded,
    /// when it is a string: borrowed from the body unless it is written with
    /// escapes. Only the method policy reads the name (see
    /// [`ScanState::record`]).
    name: Option<Cow<'de, str>>,
    /// The first element of `params` when it has a shape the worker reads as
    /// bytes: the raw transaction, if the call is a submission, or the hash,
    /// if it is a lookup by hash.
    raw_transaction: Option<RawTransaction<'de>>,
    /// The second element of `params` is the string [`PENDING_TAG`].
    pending: bool,
}

impl<'de> Call<'de> {
    /// Read a call object's `method` and first two `params` elements,
    /// skipping every other member in place.
    ///
    /// A second `method` member, however its key is written, makes the call
    /// unreadable, as jsonrpsee's derived request type refuses a duplicated
    /// member outright: keeping either occurrence would let the route rest on
    /// a name the worker never reads. A single call so refused routes like
    /// any other body the scan cannot read, and a batch holding one is
    /// unreadable (see [`scan`]). A repeated `params` keeps its last
    /// occurrence, as a `Value` parse does; it decides no route, and
    /// jsonrpsee refuses the call either way, though the screen may answer it
    /// first with its own code (see [`screen_transaction`]).
    fn read<A: MapAccess<'de>>(mut members: A) -> Result<Self, A::Error> {
        let mut call =
            Self { method: RpcMethod::Other, name: None, raw_transaction: None, pending: false };
        let mut has_method = false;
        while let Some(member) = members.next_key::<Member>()? {
            match member {
                Member::Method => {
                    if has_method {
                        return Err(de::Error::duplicate_field("method"));
                    }
                    has_method = true;
                    let name = members.next_value::<MaybeStr<'de>>()?.0;
                    call.method = RpcMethod::named(name.as_deref());
                    call.name = name;
                }
                Member::Params => {
                    let params = members.next_value::<LeadingParams<'de>>()?;
                    (call.raw_transaction, call.pending) = (params.first, params.pending);
                }
                Member::Other => {
                    members.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(call)
    }

    /// The screen's verdict on this call: the error the worker would refuse
    /// it with, or the decoded transaction of a submission that passes, or
    /// `None` when there is nothing to screen (another method, or params
    /// without a raw transaction).
    fn screen(&self) -> Result<Option<PooledTransaction>, GatewayError> {
        if !self.method.is_submission() {
            return Ok(None);
        }
        self.raw_transaction.as_ref().map(screen_transaction).transpose()
    }

    /// The read only the worker's pool can answer that this call is, if any
    /// (see [`PoolRead`]). Only a string is read as a hash; a hash that does
    /// not parse is no lookup of a remembered submission.
    fn pool_read(&self) -> Option<PoolRead> {
        match (self.method, &self.raw_transaction) {
            (RpcMethod::TransactionCount, _) => self.pending.then_some(PoolRead::PendingNonce),
            (RpcMethod::LookupByHash, Some(RawTransaction::Hex(hash))) => {
                hash.parse().ok().map(PoolRead::Lookup)
            }
            _ => None,
        }
    }
}

/// A single read that only the worker's pool can answer, and that a gateway
/// with `--redirect-queries` therefore keeps on the worker.
///
/// The query upstream has not seen the worker's pool, whose transactions are
/// not gossiped (`propagate: false` in `crates/tn-reth/src/txn_pool.rs`), so
/// it answers these from the state before a client's own submissions, and a
/// client that sends transactions back to back reuses a nonce. Every other
/// read at `pending` (`eth_call`, `eth_getBalance`, `eth_estimateGas`) stays
/// on the query upstream. So does a batch: [`scan`] reads a pool read from a
/// single call only, so a batch keeps the route its calls give it. Method
/// names and the tag match exactly and case-sensitively.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolRead {
    /// [`GET_TRANSACTION_COUNT`] whose block parameter is the string
    /// [`PENDING_TAG`].
    PendingNonce,
    /// One of [`LOOKUPS_BY_HASH`] for this hash.
    Lookup(B256),
}

impl PoolRead {
    /// Whether the read stays on the worker: a pending nonce read always, a
    /// lookup only for the hash of a submission this gateway forwarded
    /// recently (`recent`; `None` turns that half off).
    fn stays_on_worker(self, recent: Option<&RecentSubmissions>) -> bool {
        match self {
            Self::PendingNonce => true,
            Self::Lookup(hash) => recent.is_some_and(|recent| recent.contains(&hash)),
        }
    }
}

/// A call's `method`, reduced to what the gateway acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RpcMethod {
    /// One of [`SUBMISSION_METHODS`]; `sync` for
    /// `eth_sendRawTransactionSync`, which answers with the receipt rather
    /// than the hash.
    Submission {
        /// The method is `eth_sendRawTransactionSync`.
        sync: bool,
    },
    /// [`GET_TRANSACTION_COUNT`].
    TransactionCount,
    /// One of [`LOOKUPS_BY_HASH`].
    LookupByHash,
    /// Any other method, or a `method` that is not a string.
    Other,
}

impl RpcMethod {
    /// The method a `method` member names, matched against
    /// [`SUBMISSION_METHODS`] and the pool reads exactly and case-sensitively,
    /// as jsonrpsee matches method names.
    fn named(name: Option<&str>) -> Self {
        match name {
            Some(name) if SUBMISSION_METHODS.contains(&name) => {
                Self::Submission { sync: name == SEND_RAW_TRANSACTION_SYNC }
            }
            Some(GET_TRANSACTION_COUNT) => Self::TransactionCount,
            Some(name) if LOOKUPS_BY_HASH.contains(&name) => Self::LookupByHash,
            _ => Self::Other,
        }
    }

    /// Whether the method is one of [`SUBMISSION_METHODS`].
    fn is_submission(self) -> bool {
        matches!(self, Self::Submission { .. })
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

/// A batch element: a call object, an array, or anything else.
enum Element<'de> {
    /// A call object.
    Call(Call<'de>),
    /// An array. jsonrpsee's derived `Request` reads one positionally,
    /// `[jsonrpc, id, method, params]`, and runs it when it has exactly four
    /// elements, so its `method` (when a string, escapes decoded) is kept
    /// for the method policy. It still routes as another call and is not
    /// screened.
    Positional(Option<Cow<'de, str>>),
    /// Any other element; the upstream answers it as an invalid request.
    Other,
}

impl<'de> Lenient<'de> for Element<'de> {
    fn other() -> Self {
        Self::Other
    }

    fn array<A: SeqAccess<'de>>(mut elements: A) -> Result<Self, A::Error> {
        // the third member is the `method` jsonrpsee reads; a name read from
        // a shape it refuses (another length, a bad `jsonrpc`) still denies
        // the batch, although the worker would answer that element as an
        // invalid request: the check errs toward refusing
        let mut name = None;
        for index in 0usize.. {
            let more = if index == 2 {
                elements.next_element::<MaybeStr<'de>>()?.map(|method| name = method.0).is_some()
            } else {
                elements.next_element::<IgnoredAny>()?.is_some()
            };
            if !more {
                break;
            }
        }
        Ok(Self::Positional(name))
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
/// raw transaction (or a hash, for a lookup by hash), and whether its second
/// element is the string [`PENDING_TAG`] (for a nonce read).
///
/// The remaining elements are drained through the ignored-value sink, so a
/// `params` array of any length costs a scan rather than an allocation per
/// element. Named params (an object) and any other shape have no positional
/// elements and leave the call to the upstream.
struct LeadingParams<'de> {
    /// The first element, when it holds a raw transaction.
    first: Option<RawTransaction<'de>>,
    /// The second element is [`PENDING_TAG`].
    pending: bool,
}

impl<'de> Lenient<'de> for LeadingParams<'de> {
    fn other() -> Self {
        Self { first: None, pending: false }
    }

    fn array<A: SeqAccess<'de>>(mut elements: A) -> Result<Self, A::Error> {
        let Some(MaybeRawTransaction(first)) = elements.next_element()? else {
            return Ok(Self::other());
        };
        let pending = elements.next_element::<PendingTag>()?.is_some_and(|tag| tag.0);
        while elements.next_element::<IgnoredAny>()?.is_some() {}
        Ok(Self { first, pending })
    }
}

impl<'de> Deserialize<'de> for LeadingParams<'de> {
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

/// Whether a value is the string [`PENDING_TAG`], compared where it lies and
/// never copied; any other shape is consumed and discarded.
struct PendingTag(bool);

impl Lenient<'_> for PendingTag {
    fn other() -> Self {
        Self(false)
    }

    fn string(value: &str) -> Self {
        Self(value == PENDING_TAG)
    }
}

impl<'de> Deserialize<'de> for PendingTag {
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

/// The screen's verdict on one raw transaction: the decoded transaction, to
/// forward it, or the error to refuse it with.
///
/// The decode uses the same pooled wire format the worker's RPC accepts and
/// never recovers the signer, so it cannot reject a transaction the worker
/// would have accepted (no false rejections). A typed envelope of a type the
/// network does not accept is refused from its type byte, before the decode
/// copies a blob sidecar or hashes anything. From here the payload is
/// unambiguously a raw transaction, so a decode failure is a real rejection
/// rather than a reason to forward.
///
/// The screen answers for the transaction, not for the rest of the call:
/// [`Call::read`] checks neither `jsonrpc` nor `id` and keeps the last of two
/// `params`. So a submission the worker would refuse for another reason (a
/// missing or wrong `jsonrpc`, a repeated `params`) gets the gateway's own
/// code for its transaction where the worker would answer an invalid request,
/// and a notification (no `id`), which the worker would run without answering,
/// gets an answer.
fn screen_transaction(raw: &RawTransaction<'_>) -> Result<PooledTransaction, GatewayError> {
    let raw = raw.bytes().ok_or(GatewayError::InvalidTransaction)?;
    // a typed envelope starts with its type byte, 0x01..=0x7f; a legacy
    // transaction is an rlp list, from 0xc0
    let refused = raw.first().copied().filter(|&ty| {
        (1..=0x7f).contains(&ty) && !tn_types::batch_allowlisted_tx_type(&EnvelopeType(ty))
    });
    if let Some(ty) = refused {
        return Err(GatewayError::UnsupportedTransactionType(ty));
    }
    let mut buf = raw.as_ref();
    match PooledTransaction::decode_2718(&mut buf) {
        Err(_) => Err(GatewayError::InvalidTransaction),
        Ok(tx) if !tn_types::batch_allowlisted_tx_type(&tx) => {
            Err(GatewayError::UnsupportedTransactionType(tx.ty()))
        }
        Ok(tx) => Ok(tx),
    }
}

/// An EIP-2718 type byte on its own, so [`screen_transaction`] can ask
/// [`tn_types::batch_allowlisted_tx_type`] about a typed envelope before it
/// decodes the transaction, and the allowlist keeps one definition.
struct EnvelopeType(u8);

impl Typed2718 for EnvelopeType {
    fn ty(&self) -> u8 {
        self.0
    }
}

/// Whether `body`, its leading whitespace already dropped, is a JSON array.
fn is_batch(body: &[u8]) -> bool {
    body.first() == Some(&b'[')
}

/// Whether `body` can name a method the scan acts on: it mentions the
/// raw-transaction method (both submission methods contain it) or
/// [`POOL_READ_PREFIX`] (every pool read contains it), or it holds a unicode
/// escape, which can spell any name. No other JSON escape decodes to a letter
/// or `_`.
///
/// The body need not be valid UTF-8: the worker's server skips an invalid
/// byte inside a member it does not read and runs the call, so each valid
/// stretch of the body is searched in place. Every needle is ASCII, so a
/// match never spans an invalid byte.
fn may_name_a_routed_method(body: &[u8]) -> bool {
    body.utf8_chunks().any(|chunk| {
        let text = chunk.valid();
        text.contains(SEND_RAW_TRANSACTION)
            || text.contains(POOL_READ_PREFIX)
            || text.contains("\\u")
    })
}

/// Decode a hex string as the worker's `Bytes` parameter reads it (one
/// lowercase `0x` prefix, or none), or `None` if the worker would refuse it.
fn decode_hex(value: &str) -> Option<Vec<u8>> {
    tn_types::hex::decode(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::UpstreamWorker,
        identity::TrustedProxies,
        readiness::{GatewayReadiness, ReadinessThresholds},
        server::inflight_slots,
    };
    use alloy::{
        consensus::{TxEip1559, TxEip2930, TxEip4844, TxEip4844WithSidecar, TxEip7702, TxLegacy},
        eips::{eip4844::BlobTransactionSidecar, eip7594::BlobTransactionSidecarVariant},
    };
    use axum::http::StatusCode;
    use serde_json::{value::RawValue, Value};
    use std::{cell::Cell, num::NonZeroU32, sync::Mutex};
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

    /// [`super::scan`] with no method policy in force, as a gateway with
    /// `--redirect-queries` or an empty `--denied-method-prefixes` reads a
    /// body. The tests that count, route and screen bodies ask nothing of the
    /// policy; the policy's own tests go through [`calls_denied_method`].
    fn scan(body: &[u8], max_batch_len: Option<NonZeroUsize>) -> Scan {
        super::scan(body, max_batch_len, &[])
    }

    /// Whether the method policy refuses a body under `prefixes`, from the one
    /// pass that also counts, routes and screens it.
    fn calls_denied_method(body: &[u8], prefixes: &[String]) -> bool {
        super::scan(body, None, prefixes).denied.is_some()
    }

    /// The screen's verdict on a body, from the one pass that also routes it.
    fn screen_raw_transaction(body: &[u8]) -> Option<(GatewayError, RequestId)> {
        scan(body, None).rejection
    }

    /// How a body routes, from the one pass that also screens it.
    fn classify(body: &[u8]) -> Calls {
        scan(body, None).calls
    }

    /// Whether a gateway with `--redirect-queries` keeps a body on the worker
    /// as a read only its pool can answer, from the one pass that also routes
    /// and screens it.
    fn is_pool_read(body: &[u8], recent: Option<&RecentSubmissions>) -> bool {
        scan(body, None).pool_read.is_some_and(|read| read.stays_on_worker(recent))
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
    /// [`deeply_nested_id_rejects_locally_where_the_old_parse_forwarded`]; an
    /// `id` outside the echo bound on a rejected request, which echoes `null`
    /// where the old parse echoed it whole (an array or object id is pinned by
    /// [`structured_id_on_a_rejection_echoes_null`], a long string or boolean
    /// id by the bound tests in `error.rs`); and the shapes the screen reads
    /// since WG-32, which the old one forwarded unread although the worker
    /// refuses them too: `eth_sendRawTransactionSync`, an escaped method name,
    /// a byte-array param and a batch, each pinned by its own test.
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
            // A structured id on the forward verdict. The screen no longer
            // reads the id while parsing; on a rejection a structured id echoes
            // `null` by design (see `structured_id_on_a_rejection_echoes_null`).
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
    /// `RequestId::recover` reads an array `id` as `null` without building it,
    /// so the rejection echoes `null`. Only bodies the worker would
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
        // and id recovery echoes `null` for an array id.
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

    /// A rejected request's id is recovered within the echo bound: an array or
    /// object id echoes `null` where the old parse echoed it whole, and the
    /// verdict is the one the old parse reached.
    #[test]
    fn structured_id_on_a_rejection_echoes_null() {
        let bodies = [
            br#"{"method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":[1,2,3]}"#.to_vec(),
            br#"{"method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":{"n":{"id":7}}}"#
                .to_vec(),
        ];
        bodies.iter().for_each(|body| {
            let (err, id) = screen_raw_transaction(body).expect("undecodable tx must be rejected");
            assert!(matches!(err, GatewayError::InvalidTransaction));
            assert_eq!(id, RequestId::from_id(Value::Null));
            let (reference_err, _) = reference_screen(body).expect("the old parse rejected it too");
            assert_eq!(format!("{err:?}"), format!("{reference_err:?}"));
        });
    }

    /// A body whose only mention of the method is inside a huge unrelated member
    /// is the payload this fix exists for: it clears the substring test, so it
    /// reaches the parse, and under the previous code that parse built a `Value`
    /// tree an order of magnitude larger than the request. It must still reach
    /// the same verdict (forwarded, since `method` is not the raw-transaction
    /// call) without materializing the tree. The forward verdict and route are
    /// pinned, and a variant whose member ends in `1e400` fails a reader that
    /// builds the member as a `Value` (serde_json's in-place skip accepts the
    /// number, a `Value` refuses it); no allocation is measured.
    #[test]
    fn oversized_unrelated_member_is_handled_without_building_a_tree() {
        let filler = "1,".repeat(400_000);
        let body_ending_in = |last: &str| {
            format!(
                r#"{{"method":"eth_chainId","note":"eth_sendRawTransaction","junk":[{filler}{last}],"id":1}}"#
            )
            .into_bytes()
        };
        let body = body_ending_in("0");
        assert!(body.len() > 800_000, "fixture should be large: {}", body.len());

        assert_eq!(verdict(screen_raw_transaction(&body)), None);
        assert_eq!(verdict(screen_raw_transaction(&body)), verdict(reference_screen(&body)));

        // one query, forwarded, with no hash or pool read, and no method the
        // shipped policy denies
        let assert_outcome = |body: &[u8]| {
            let scan = scan(body, None);
            assert!(scan.rejection.is_none(), "forwarded");
            assert_eq!(
                (scan.len, scan.calls, scan.submitted, scan.pool_read),
                (1, Calls::Queries, None, None)
            );
            assert!(!calls_denied_method(body, &default_denied()), "not denied");
        };
        assert_outcome(&body);
        let sentinel = body_ending_in("1e400");
        assert!(serde_json::from_slice::<Value>(&sentinel).is_err(), "a tree build fails");
        assert_outcome(&sentinel);
    }

    /// The review payload for this commit: all of the bulk in `id`, past the
    /// substring gate, on requests that are then forwarded. Under the previous
    /// reader `id` was the one member still materialized as a full `Value`, so
    /// this shape rebuilt the amplification the screen fix removed, eagerly,
    /// for an id that was then dropped unused. The verdicts must still match
    /// the old parse; the id is never touched on the forward path. The forward
    /// verdict and route are pinned, and a variant whose id ends in `1e400`
    /// fails a reader that builds the id as a `Value` (serde_json's in-place
    /// skip accepts the number, a `Value` refuses it); no allocation is
    /// measured.
    #[test]
    fn oversized_id_is_skipped_on_the_forward_path() {
        let filler = "1,".repeat(400_000);
        let bodies_ending_in = |last: &str| {
            [
                // The method name only as data: forwarded without reading the id.
                format!(r#"{{"x":"eth_sendRawTransaction","id":[{filler}{last}]}}"#).into_bytes(),
                // A real, valid submission: forwarded on its merits, id unread.
                format!(
                    r#"{{"method":"eth_sendRawTransaction","params":["{EIP155_LEGACY_TX}"],"id":[{filler}{last}]}}"#
                )
                .into_bytes(),
            ]
        };
        let bodies = bodies_ending_in("0");
        bodies.iter().for_each(|body| {
            assert!(body.len() > 800_000, "fixture should be large: {}", body.len());
            assert_eq!(verdict(screen_raw_transaction(body)), None);
            assert_eq!(verdict(screen_raw_transaction(body)), verdict(reference_screen(body)));
        });

        // the method-less body is one query; the submission goes to the worker
        // and hands back its hash; neither names a method the shipped policy
        // denies
        let raw = decode_hex(EIP155_LEGACY_TX).expect("fixture hex");
        let expected = [
            (1, Calls::Queries, None, None),
            (1, Calls::Submissions, Some(tn_types::keccak256(raw)), None),
        ];
        let assert_outcomes = |bodies: &[Vec<u8>; 2]| {
            for (body, expected) in bodies.iter().zip(expected) {
                let scan = scan(body, None);
                assert!(scan.rejection.is_none(), "forwarded");
                assert_eq!((scan.len, scan.calls, scan.submitted, scan.pool_read), expected);
                assert!(!calls_denied_method(body, &default_denied()), "not denied");
            }
        };
        assert_outcomes(&bodies);
        let sentinels = bodies_ending_in("1e400");
        for sentinel in &sentinels {
            assert!(serde_json::from_slice::<Value>(sentinel).is_err(), "a tree build fails");
        }
        assert_outcomes(&sentinels);
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

    /// The forward verdict on a valid submission carries its transaction hash,
    /// which the proxy remembers once the worker takes it; nothing else does.
    #[test]
    fn valid_submission_hands_back_its_hash() {
        let raw = decode_hex(EIP155_LEGACY_TX).expect("fixture hex");
        let single = send_raw(&format!("[\"{EIP155_LEGACY_TX}\"]"));
        assert_eq!(scan(&single, None).submitted, Some(tn_types::keccak256(raw)));
        let sync = format!(
            r#"{{"jsonrpc":"2.0","method":"eth_sendRawTransactionSync","params":["{EIP155_LEGACY_TX}"],"id":1}}"#
        );
        assert_eq!(scan(sync.as_bytes(), None).submitted, None);
        // a submission inside a batch is screened but not remembered
        let mut batch = b"[".to_vec();
        batch.extend_from_slice(&single);
        batch.push(b']');
        let batch = scan(&batch, None);
        assert_eq!((batch.calls, batch.submitted), (Calls::Submissions, None));
    }

    #[test]
    fn undecodable_transaction_is_rejected() {
        // Valid hex, but not a decodable transaction envelope.
        let err = screen_err(&send_raw(r#"["0xdeadbeef"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    #[test]
    fn blob_typed_payload_is_not_forwarded() {
        // A type-`0x03` (EIP-4844) prefix with a truncated body: the `0x03`
        // type byte is refused before the truncated body is decoded, so a
        // blob-typed payload never reaches an upstream, whether or not its body
        // would decode.
        let err = screen_err(&send_raw(r#"["0x03c0"]"#));
        assert!(matches!(err, Some(GatewayError::UnsupportedTransactionType(3))), "{err:?}");
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

    // the pool's admission set, by EIP-2718 type byte
    const POOL_ADMITS: [u8; 3] = [0, 1, 2];

    /// WG-41: the screen must refuse exactly the transaction types the
    /// worker's pool refuses. The pool's set is fixed by its validator in
    /// `crates/tn-reth/src/txn_pool.rs` (`.no_eip4844().no_eip7702()`: it
    /// admits legacy, EIP-2930 and EIP-1559 only), a crate the gateway does
    /// not link, so that set is written out here; the screen's comes from
    /// `tn_types::batch_allowlisted_tx_type`. A change on either side fails
    /// this test instead of letting the two sets drift apart by convention.
    #[test]
    fn screen_verdicts_match_the_pool_allowlist() {
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

    /// The refusal of a type the network does not accept is decided from the
    /// type byte, so a refused type is refused even when its body cannot
    /// decode, while an admitted type whose body cannot decode is refused as
    /// undecodable.
    #[test]
    fn refused_type_is_refused_from_its_type_byte() {
        for ty in 1..=0x7f_u8 {
            // the type byte and an empty rlp list, which no type decodes
            let verdict = screen_err(&send_raw(&format!("[\"0x{ty:02x}c0\"]")));
            if POOL_ADMITS.contains(&ty) {
                assert!(
                    matches!(verdict, Some(GatewayError::InvalidTransaction)),
                    "type {ty} must be refused as undecodable: {verdict:?}"
                );
            } else {
                assert!(
                    matches!(verdict, Some(GatewayError::UnsupportedTransactionType(refused)) if refused == ty),
                    "type {ty} must be refused as unsupported: {verdict:?}"
                );
            }
        }
        // neither is a typed envelope of a refused type
        for hex in ["0x00c0", "0xc0"] {
            let verdict = screen_err(&send_raw(&format!("[\"{hex}\"]")));
            assert!(
                matches!(verdict, Some(GatewayError::InvalidTransaction)),
                "{hex} must be refused as undecodable: {verdict:?}"
            );
        }
    }

    #[test]
    fn non_hex_param_is_rejected() {
        let err = screen_err(&send_raw(r#"["not-hex"]"#));
        assert!(matches!(err, Some(GatewayError::InvalidTransaction)));
    }

    /// The worker's `Bytes` parameter strips one lowercase `0x` and nothing
    /// else, so an upper-case or doubled prefix must be refused as
    /// undecodable, not decoded and forwarded or refused for its type.
    #[test]
    fn hex_prefixes_the_worker_refuses_are_refused() {
        let bare = &EIP155_LEGACY_TX[2..];
        for hex in [EIP155_LEGACY_TX, bare] {
            let verdict = screen_err(&send_raw(&format!("[\"{hex}\"]")));
            assert!(verdict.is_none(), "{hex} must be forwarded: {verdict:?}");
        }
        for hex in
            [format!("0X{bare}"), format!("0x{EIP155_LEGACY_TX}"), format!("0X{EIP155_LEGACY_TX}")]
        {
            assert!(decode_hex(&hex).is_none(), "{hex} must not decode");
            let verdict = screen_err(&send_raw(&format!("[\"{hex}\"]")));
            assert!(
                matches!(verdict, Some(GatewayError::InvalidTransaction)),
                "{hex} must be refused as undecodable: {verdict:?}"
            );
        }
        // a refused type behind an upper-case prefix is undecodable to the
        // worker, so it is not refused for its type
        let upper = format!("0X{}", &eip7702_raw_hex()[2..]);
        let verdict = screen_err(&send_raw(&format!("[\"{upper}\"]")));
        assert!(matches!(verdict, Some(GatewayError::InvalidTransaction)), "{verdict:?}");
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
        for params in ["[[222,173,190,239]]", "[[]]"] {
            let err = screen_err(&send_raw(params));
            assert!(matches!(err, Some(GatewayError::InvalidTransaction)), "{params}");
        }
        // a lone byte in 0x01..=0x7f is the type byte of a refused typed
        // envelope
        let err = screen_err(&send_raw("[[123]]"));
        assert!(matches!(err, Some(GatewayError::UnsupportedTransactionType(123))), "{err:?}");
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

    /// A `tracing` writer that keeps everything written to it.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        /// Everything logged so far.
        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("logs lock")).into_owned()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("logs lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// The warning `proxy()` itself writes when a forward fails, captured on
    /// both routes: it names the upstream by origin and carries the cause from
    /// below reqwest's own message, while the url's credential (userinfo,
    /// path and query), which the raw error's `Display` would print, appears
    /// nowhere in the gateway's logs.
    ///
    /// The throttles are process-wide statics, so this test relies on
    /// nextest's one-process-per-test model, which `make test`, the gate and
    /// CI use: under a single-process `cargo test`, another test failing a
    /// forward on either route within 10 s suppresses the line this test reads.
    #[tokio::test]
    async fn forwarding_failure_log_hides_the_url() {
        let logs = CapturedLogs::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("gateway=trace")
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _logging = tracing::subscriber::set_default(subscriber);

        // nothing listens on port 1, as in the server's unreachable-upstream test
        let worker_url =
            Url::parse("http://user:secret@127.0.0.1:1/apikey123?token=xyz").expect("url");
        let query_url =
            Url::parse("http://quser:qsecret@127.0.0.1:1/qkey456?token=qtok").expect("url");
        let secrets = ["secret", "apikey123", "xyz", "qsecret", "qkey456", "qtok"];
        let raw = Client::new().post(worker_url.clone()).send().await.expect_err("port 1");
        assert!(matches!(classify_error(&raw), GatewayError::UpstreamUnreachable));
        assert!(raw.to_string().contains("apikey123"), "raw display should carry the url: {raw}");

        let worker = UpstreamWorker {
            worker_id: 0,
            rpc_url: worker_url,
            readiness_url: Url::parse("http://127.0.0.1:1/health/workers").expect("url"),
        };
        let nz = |n| NonZeroU32::new(n).expect("nonzero");
        let thresholds = ReadinessThresholds { failure: nz(3), success: nz(2), rpc_failure: None };
        let state = AppState {
            readiness: Arc::new(GatewayReadiness::new(&[worker], thresholds)),
            http: proxy_client(
                UpstreamClientBuilder::for_test(Duration::from_secs(2)),
                Duration::from_secs(5),
            )
            .expect("client"),
            query_upstream: Some(query_url),
            query_header: None,
            recent_submissions: None,
            denied_method_prefixes: Arc::from([]),
            max_batch_len: NonZeroUsize::new(DEFAULT_MAX_BATCH_LEN),
            query_request_timeout: None,
            submission_slots: inflight_slots(0),
            query_slots: inflight_slots(0),
            upstream_slots: inflight_slots(0),
            pool_read_slots: inflight_slots(0),
            draining: Arc::default(),
        };
        state.readiness.set_ready(0, true);
        let forwarding = TrustedProxies::default()
            .resolve("127.0.0.1".parse().expect("peer"), &HeaderMap::new());
        for body in [
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":[],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_call","params":[],"id":2}"#,
        ] {
            let response = proxy(
                State(state.clone()),
                Extension(forwarding),
                PostOnly,
                None,
                None,
                None,
                Request::new(Body::from(body)),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{body}");
        }

        let text = logs.text();
        let failures: Vec<_> =
            text.lines().filter(|line| line.contains("forwarding to upstream failed")).collect();
        assert_eq!(failures.len(), 2, "one warning per route: {text}");
        for (line, route) in failures.iter().zip(["worker", "query"]) {
            assert!(line.contains(&format!("route=\"{route}\"")), "{line}");
            assert!(line.contains("upstream=http://127.0.0.1:1 "), "{line}");
            assert!(line.contains("cause=error sending request: "), "cause has a source: {line}");
        }
        for secret in secrets {
            assert!(!text.contains(secret), "the logs leak {secret:?}: {text}");
        }
    }

    #[test]
    fn json_content_types_are_recognized() {
        let json =
            |value: &'static str| is_json_content_type(Some(&HeaderValue::from_static(value)));
        for value in [
            "application/json",
            "Application/JSON",
            "application/json; charset=utf-8",
            " application/json ;charset=utf-8",
            "application/problem+json",
            "application/vnd.api+JSON; ext=1",
        ] {
            assert!(json(value), "{value}");
        }
        for value in
            ["text/plain", "text/plain; charset=utf-8", "text/html", "application/jsonp", ""]
        {
            assert!(!json(value), "{value}");
        }
        assert!(!is_json_content_type(None));
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
            r#"{"method":["eth_sendRawTransaction"],"params":["0xdeadbeef"],"id":1}"#.to_string(),
            r#"{"method":{"method":"eth_sendRawTransaction"},"id":1}"#.to_string(),
            format!(r#"[{submission},{{"method":["eth_sendRawTransaction"],"id":2}}]"#),
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
                assert!(is_query(calls, None, None), "{body}");
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
    /// would run must be a submission. A duplicated `method`, which
    /// jsonrpsee's derived request type refuses outright, is unreadable to the
    /// scan too, so such a call never routes to the worker whichever name
    /// comes last. And wherever jsonrpsee reads calls, the
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
        // a duplicated `method`: jsonrpsee refuses the call, and the scan
        // cannot read it
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
            format!("[{submission},{sync},{notification},{reordered}]"),
            format!("[{escaped_name},{escaped_key}]"),
            format!("\u{c}{submission}"),
            format!(" \t\r\n\u{c}[{submission},{sync}]"),
        ];
        let kept_away: Vec<String> = vec![
            read.clone(),
            duplicated.to_string(),
            duplicated_escaped.to_string(),
            duplicated_last_read.to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_sendRaw\u0074ransaction","id":1}"#.to_string(),
            r#"{"jsonrpc":"2.0","Method":"eth_sendRawTransaction","id":1}"#.to_string(),
            format!("[{submission},{read}]"),
            format!("[{read},{escaped_name}]"),
            format!("[{submission},1]"),
            format!("[{submission},{duplicated_last_read}]"),
            format!("[{escaped_name},{escaped_key},{duplicated}]"),
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
        // jsonrpsee refuses a duplicated `method` whichever name comes last,
        // and so does the scan
        for body in [duplicated, duplicated_escaped, duplicated_last_read] {
            assert_eq!(worker_reads(body.as_bytes()), WorkerReads::Single(None), "{body}");
            let single = scan(body.as_bytes(), None);
            assert_eq!((single.calls, single.submitted), (Calls::Queries, None), "{body}");
            let batch = format!("[{submission},{body}]");
            assert!(scan(batch.as_bytes(), None).unreadable_batch, "{batch}");
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
            // a positional element's method slot is read too, so a value the
            // typed readers refuse there ends the scan
            r#"["2.0",1,1e400,[]]"#.to_string(),
            r#"["2.0",1,"\ud800",[]]"#.to_string(),
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

    /// A call with more than one `method` member is unreadable, as jsonrpsee
    /// refuses it, whichever name comes last and however the key is written:
    /// alone it routes like any body the scan cannot read, to the query
    /// upstream, with no verdict, hash or pool read; in a batch it makes the
    /// batch unreadable. A method policy in force denies it, like any other
    /// call it cannot read.
    #[test]
    fn duplicated_method_makes_the_call_unreadable() {
        let submission = call("eth_sendRawTransaction");
        let duplicates = [
            r#"{"method":"eth_call","method":"eth_sendRawTransaction","params":[],"id":1}"#,
            r#"{"method":"eth_sendRawTransaction","method":"eth_call","params":[],"id":1}"#,
            r#"{"method":"eth_sendRawTransaction","method":"eth_sendRawTransaction","params":[],"id":1}"#,
            r#"{"method":"eth_call","m\u0065thod":"eth_sendRawTransactionSync","params":[],"id":1}"#,
            r#"{"method":null,"method":"eth_sendRawTransaction","params":[],"id":1}"#,
            r#"{"method":"eth_getTransactionCount","params":["0x00","pending"],"method":"eth_getTransactionCount","id":1}"#,
        ];
        let prefixes = ["debug_".to_string()];
        for body in duplicates {
            let single = scan(body.as_bytes(), None);
            assert_eq!(single.calls, Calls::Queries, "{body}");
            assert!(is_query(single.calls, single.pool_read, None), "{body}");
            assert!(single.rejection.is_none() && single.submitted.is_none(), "{body}");
            assert!(single.pool_read.is_none() && !single.unreadable_batch, "{body}");
            assert!(calls_denied_method(body.as_bytes(), &prefixes), "{body}");
            for batch in [format!("[{body}]"), format!("[{submission},{body}]")] {
                assert!(scan(batch.as_bytes(), None).unreadable_batch, "{batch}");
            }
        }
        // a repeated `params` decides no route, and keeps its last occurrence
        let params_twice =
            r#"{"method":"eth_sendRawTransaction","params":[],"params":["0xdeadbeef"],"id":1}"#;
        assert_eq!(classify(params_twice.as_bytes()), Calls::Submissions);
        assert!(screen_raw_transaction(params_twice.as_bytes()).is_some());
    }

    /// A JSON-RPC call to `method` with the given params.
    fn call_with(method: &str, params: &str) -> String {
        format!(r#"{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":1}}"#)
    }

    /// The test address every nonce read below asks about.
    const ADDRESS: &str = "0x3535353535353535353535353535353535353535";

    #[test]
    fn only_a_single_pending_nonce_read_is_a_pool_read() {
        for (params, pool_read) in [
            (format!(r#"["{ADDRESS}","pending"]"#), true),
            (format!(r#"["{ADDRESS}","pending","extra"]"#), true),
            (format!(r#"["{ADDRESS}","latest"]"#), false),
            (format!(r#"["{ADDRESS}","Pending"]"#), false),
            (format!(r#"["{ADDRESS}","0x10"]"#), false),
            (format!(r#"["{ADDRESS}"]"#), false),
            (format!(r#"["{ADDRESS}",{{"blockNumber":"pending"}}]"#), false),
            (r#"{"address":"pending"}"#.to_string(), false),
            ("null".to_string(), false),
        ] {
            let body = call_with("eth_getTransactionCount", &params);
            assert_eq!(is_pool_read(body.as_bytes(), None), pool_read, "{body}");
        }
        let pending = call_with("eth_getTransactionCount", &format!(r#"["{ADDRESS}","pending"]"#));
        // a batch keeps the route `classify` gives it
        assert!(!is_pool_read(format!("[{pending}]").as_bytes(), None));
        assert!(!is_pool_read(format!("{pending} trailing").as_bytes(), None));
        // every other read at the pending tag stays on the query upstream
        for method in ["eth_call", "eth_getBalance", "eth_estimateGas", "eth_gettransactioncount"] {
            let body = call_with(method, &format!(r#"["{ADDRESS}","pending"]"#));
            assert!(!is_pool_read(body.as_bytes(), None), "{method}");
        }
    }

    #[test]
    fn only_a_remembered_hash_lookup_is_a_pool_read() {
        let remembered = B256::repeat_byte(0x11);
        let recent = RecentSubmissions::new(Duration::from_secs(60), 16);
        recent.record(remembered);
        for method in LOOKUPS_BY_HASH {
            let hit = call_with(method, &format!(r#"["{remembered}"]"#));
            assert!(is_pool_read(hit.as_bytes(), Some(&recent)), "{method}");
            // the same lookup with the hash routing off
            assert!(!is_pool_read(hit.as_bytes(), None), "{method}");
            for params in [
                format!(r#"["{}"]"#, B256::repeat_byte(0x22)),
                r#"["0x1234"]"#.to_string(),
                "[]".to_string(),
                "[17]".to_string(),
            ] {
                let miss = call_with(method, &params);
                assert!(!is_pool_read(miss.as_bytes(), Some(&recent)), "{miss}");
            }
        }
    }

    #[test]
    fn an_answer_with_a_result_is_an_accepted_submission() {
        let hash = B256::repeat_byte(0x11);
        for answer in [
            format!(r#"{{"jsonrpc":"2.0","result":"{hash}","id":1}}"#),
            // member order and surrounding whitespace do not matter
            format!(" \n{{\"id\":\"abc\",\"result\":\"{hash}\",\"jsonrpc\":\"2.0\"}}\n"),
            // a null error is no error
            format!(r#"{{"jsonrpc":"2.0","result":"{hash}","error":null,"id":1}}"#),
        ] {
            assert!(submission_accepted(answer.as_bytes()), "{answer}");
        }
    }

    #[test]
    fn an_answer_without_a_lone_result_is_not_an_accepted_submission() {
        let error = r#"{"code":-32003,"message":"insufficient funds"}"#;
        for answer in [
            // an error, as jsonrpsee answers a refused transaction with a 200
            format!(r#"{{"jsonrpc":"2.0","error":{error},"id":1}}"#),
            // a null result
            r#"{"jsonrpc":"2.0","result":null,"id":1}"#.to_string(),
            // both members
            format!(r#"{{"jsonrpc":"2.0","result":"0x01","error":{error},"id":1}}"#),
            // neither member
            r#"{"jsonrpc":"2.0","id":1}"#.to_string(),
            // not json
            "worker".to_string(),
            String::new(),
            r#"{"jsonrpc":"2.0","result":"0x01""#.to_string(),
            // a json array, a batch answer included, even one whose elements
            // would fill the two members in order
            r#"[{"jsonrpc":"2.0","result":"0x01","id":1}]"#.to_string(),
            r#"["0x01",null]"#.to_string(),
        ] {
            assert!(!submission_accepted(answer.as_bytes()), "{answer}");
        }
    }

    /// The shipped `--denied-method-prefixes` default, as the CLI resolves it.
    fn default_denied() -> Vec<String> {
        crate::cli::DEFAULT_DENIED_METHOD_PREFIXES.split(',').map(String::from).collect()
    }

    #[test]
    fn prefix_match_is_case_sensitive_and_anchored() {
        let prefixes = default_denied();
        for method in ["tn_getStuff", "debug_traceTransaction", "trace_block", "admin_peers", "tn_"]
        {
            assert!(calls_denied_method(call(method).as_bytes(), &prefixes), "{method}");
        }
        for method in [
            "TN_x",
            "Tn_getStuff",
            "DEBUG_traceTransaction",
            "xtn_",
            "eth_tn_x",
            " tn_x",
            "tn",
            "eth_call",
            "net_version",
        ] {
            assert!(!calls_denied_method(call(method).as_bytes(), &prefixes), "{method:?}");
        }
    }

    /// The worker's server unescapes method names and runs every call object in
    /// a batch around the elements it cannot read, so the scan has to as well.
    #[test]
    fn denied_call_is_found_behind_escapes_and_junk_elements() {
        let prefixes = default_denied();
        let denied = call("tn_getStuff");
        let deep = format!("{}{}", "[".repeat(512), "]".repeat(512));
        // jsonrpsee's derived `Request` reads a four-element array
        // positionally, `[jsonrpc, id, method, params]`, and runs it
        let positional = [
            r#"[["2.0",1,"tn_info",[]]]"#.to_string(),
            r#"[["2.0",1,"debug_traceBlockByNumber",["latest",{}]]]"#.to_string(),
            r#"[["2.0",null,"tn_info",null]]"#.to_string(),
            format!(r#"[{},["2.0",1,"tn_info",[]]]"#, call("eth_call")),
        ];
        let mut denied_bodies = vec![
            r#"{"jsonrpc":"2.0","method":"\u0074n_getStuff","params":[],"id":1}"#.to_string(),
            format!("[1,{denied}]"),
            format!(r#"["tn_x",null,true,1.5,{{"method":7}},{denied}]"#),
            format!("[{deep},{denied}]"),
            format!(r#"[{{"params":{deep},"method":"eth_call"}},{denied}]"#),
            format!("[{denied},{}]", call("eth_call")),
            // the worker's server skips a leading form feed; serde_json does not
            format!("\x0C{denied}"),
            format!(" \x0C\n[{denied}]"),
            // an element the worker answers as an invalid request does not
            // hide the calls around it, wherever it sits
            format!(r#"["\ud800",{denied}]"#),
            format!(r#"[{denied},{{"\ud800":1}}]"#),
            format!(r#"[{{"method":"\ud800"}},{denied}]"#),
            format!("[{denied},1e999]"),
            format!(r#"[{{"method":-1e400}},{denied}]"#),
            format!("[{denied},{}]", "9".repeat(400)),
        ];
        denied_bodies.extend(positional.iter().cloned());
        for body in &denied_bodies {
            assert!(calls_denied_method(body.as_bytes(), &prefixes), "{body}");
        }
        // nothing the worker could run as a denied call is refused
        let allowed_bodies = vec![
            "not json, tn_getStuff".to_string(),
            r#"{"method":["tn_x"],"id":1}"#.to_string(),
            r#""tn_x""#.to_string(),
            "[]".to_string(),
            r#"{"jsonrpc":"2.0","method":"eth_call","params":["tn_x"],"id":1}"#.to_string(),
            format!("[{deep}]"),
            format!("[{},{}]", call("eth_call"), call("eth_blockNumber")),
            r#"[["2.0",1,"eth_call",[]]]"#.to_string(),
            // a denied-looking string outside the method slot names nothing
            r#"[["tn_x",1,"eth_call",[]]]"#.to_string(),
        ];
        for body in &allowed_bodies {
            assert!(!calls_denied_method(body.as_bytes(), &prefixes), "{body}");
        }
        assert!(!calls_denied_method(denied.as_bytes(), &[]), "an empty list denies nothing");

        // the policy's model of the worker agrees with jsonrpsee's own reader:
        // every body the worker would run a denied call from is denied
        let runs_a_denied_call = |body: &[u8]| {
            let calls = match worker_reads(body) {
                WorkerReads::Refused => Vec::new(),
                WorkerReads::Single(call) => vec![call],
                WorkerReads::Batch(calls) => calls,
            };
            calls
                .iter()
                .flatten()
                .any(|method| prefixes.iter().any(|prefix| method.starts_with(prefix.as_str())))
        };
        for body in &positional {
            assert!(runs_a_denied_call(body.as_bytes()), "jsonrpsee runs a denied call: {body}");
        }
        for body in denied_bodies.iter().chain(&allowed_bodies) {
            assert!(
                !runs_a_denied_call(body.as_bytes())
                    || calls_denied_method(body.as_bytes(), &prefixes),
                "{body}"
            );
        }
    }

    /// The method policy reads every call in the one pass that counts, routes
    /// and screens a body: a deny list in force turns off the fast path for a
    /// single call naming no routed method and the skip of a mixed batch's
    /// remaining elements, each of which would otherwise hide a denied call,
    /// and the body is still parsed once. A single call is refused with its
    /// own id, a batch with `null`.
    #[test]
    fn deny_list_turns_off_both_scan_shortcuts() {
        let prefixes = default_denied();
        let submission =
            r#"{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x00"],"id":1}"#;
        let mixed = format!("[{submission},{},{}]", call("eth_call"), call("tn_getStuff"));
        let single = call("tn_getStuff");
        let unreadable = r#"{"jsonrpc":"2.0","method":"eth_call","params":[1e999],"id":1}"#;
        for body in [single.as_str(), unreadable] {
            assert!(!may_name_a_routed_method(body.as_bytes()), "the fast path takes {body}");
        }

        for (body, id) in [
            (mixed.as_str(), Value::Null),
            (single.as_str(), serde_json::json!(1)),
            (unreadable, serde_json::json!(1)),
        ] {
            PARSES.with(|parses| parses.set(0));
            let denied = super::scan(body.as_bytes(), None, &prefixes);
            assert_eq!(PARSES.with(Cell::get), 1, "one deserializer per body: {body}");
            assert_eq!(denied.denied, Some(RequestId::from_id(id)), "{body}");

            PARSES.with(|parses| parses.set(0));
            let allowed = scan(body.as_bytes(), None);
            assert!(PARSES.with(Cell::get) <= 1, "{body}");
            assert_eq!(allowed.denied, None, "{body}");
            assert_eq!((denied.len, denied.calls), (allowed.len, allowed.calls), "{body}");
        }
        assert_eq!(super::scan(mixed.as_bytes(), None, &prefixes).calls, Calls::MixedBatch);
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
        let settings = crate::cli::try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=https://10.0.0.7:8545",
            "--upstream-readiness-url=https://10.0.0.7:8551/health/workers",
        ])
        .expect("parse")
        .into_settings()
        .expect("settings");
        let proxy = proxy_client(
            client_builder(&settings, Arc::new(crate::dns::SystemLookup)),
            Duration::from_secs(1),
        );
        assert!(proxy.is_ok(), "{proxy:?}");
        let readiness =
            readiness_client(client_builder(&settings, Arc::new(crate::dns::SystemLookup)));
        assert!(readiness.is_ok(), "{readiness:?}");
    }

    #[test]
    fn chain_id_answers_parse_only_as_hex_quantities() {
        for (answer, expected) in [
            (r#"{"jsonrpc":"2.0","id":1,"result":"0x7e1"}"#, Some(2017)),
            (r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#, Some(1)),
            (r#"{"jsonrpc":"2.0","id":1,"result":"0xffffffffffffffff"}"#, Some(u64::MAX)),
            (r#"{"jsonrpc":"2.0","id":1,"result":"0x10000000000000000"}"#, None),
            (r#"{"jsonrpc":"2.0","id":1,"result":"0x+1"}"#, None),
            (r#"{"jsonrpc":"2.0","id":1,"result":"0x"}"#, None),
            (r#"{"jsonrpc":"2.0","id":1,"result":"7e1"}"#, None),
            (r#"{"jsonrpc":"2.0","id":1,"result":2017}"#, None),
            (r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"no"}}"#, None),
            ("not json", None),
        ] {
            assert_eq!(parse_chain_id(answer.as_bytes()), expected, "{answer}");
        }
    }
}
