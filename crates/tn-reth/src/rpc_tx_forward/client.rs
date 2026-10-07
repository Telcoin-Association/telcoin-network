//! The HTTP side of `--forward-txs`: one JSON-RPC client per target, tried in configured order.
//!
//! [`TxForwarder::submit`] sends the client's bytes as `eth_sendRawTransaction` to the first
//! target that is not demoted, then down the list. A JSON-RPC error reply is the validator's
//! verdict and ends the walk: the client receives the error object unchanged. A request too large
//! to send (refused by the client's own size cap, or answered with HTTP 413) also ends the walk,
//! with the oversized-request error: the bytes are at fault, not the target, and failing over
//! would upload them to every target in turn. Anything else short of a reply (a refused or reset
//! connection, a timeout, another non-2xx status, an oversized or malformed body) demotes the
//! target for [`DEMOTION_COOLDOWN`] and moves to the next one.
//!
//! Demoted targets are still tried, after every healthy one, so a list whose targets are all
//! demoted keeps serving. When a target's cooldown runs out, one submission probes it in its
//! configured place while the others keep trying it last, so a target that still hangs costs one
//! attempt timeout per cooldown rather than one per submission. When every target fails or
//! [`SUBMIT_BUDGET`] runs out, the client receives one fixed error that names no target and
//! carries no data. An attempt the budget cut short of the attempt timeout is not charged to its
//! target.
//!
//! A target's URL is private operator configuration, so this module never logs it: log lines and
//! metric labels name a target by its index in the list. The HTTP connector underneath
//! (hyper-util) logs each new connection's resolved `ip:port` at debug, which the default
//! `--log.file.filter debug` writes to the node's log file, and stdout carries it too when it
//! runs at debug. Adding `hyper_util::client::legacy::connect=info` to `--log.file.filter` and
//! `--log.stdout.filter` keeps it out. Keeping the address out of logs is hygiene; the
//! validator's firewall is what protects the target.
//!
//! All work runs inside the request future and no task is spawned. Clients connect lazily, so
//! building a forwarder dials nothing.

use std::{
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use jsonrpsee::{
    core::client::{ClientT as _, Error as ClientError},
    http_client::{transport::Error as TransportError, HttpClient, HttpClientBuilder},
    rpc_params,
    types::{
        error::{INTERNAL_ERROR_CODE, OVERSIZED_REQUEST_CODE, OVERSIZED_REQUEST_MSG},
        ErrorObject, ErrorObjectOwned,
    },
};
use tn_types::{Bytes, B256};
use tokio::time::Instant;
use tracing::{debug, warn};

use super::target::ForwardTargets;
use crate::metrics::{RpcTxForwardFailure, RpcTxForwardMetrics, RpcTxForwardOutcome};

/// The longest one target gets to answer one submission, including the wait for a connection
/// slot. Matches the node-record forwarder's per-send timeout.
pub(crate) const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest one submission spends across every target before the client gets the fixed
/// unavailable error. Matches the node-record forwarder's per-transaction budget.
pub(crate) const SUBMIT_BUDGET: Duration = Duration::from_secs(15);

/// How long a target that failed below the JSON-RPC layer is tried only after the healthy ones.
pub(crate) const DEMOTION_COOLDOWN: Duration = Duration::from_secs(30);

/// The largest response body read from a target. An `eth_sendRawTransaction` reply is a hash or
/// an error object, so anything near this is not a reply.
pub(crate) const MAX_RESPONSE_BYTES: u32 = 64 * 1024;

/// Added to the node's own request size limit for the forwarded request. The client rebuilds the
/// JSON-RPC envelope with its own request id and the `0x` prefix an incoming request may omit, so
/// a submission the node accepted at its limit grows by a few bytes on the way out.
const REQUEST_SIZE_HEADROOM: u32 = 1024;

/// The most requests in flight to one target. Further submissions wait for a slot inside their
/// attempt timeout, which bounds the load one public node can put on its validator.
pub(crate) const MAX_INFLIGHT_PER_TARGET: usize = 128;

/// The least time between two "all targets failed" warnings.
const UNAVAILABLE_WARN_INTERVAL: Duration = Duration::from_secs(30);

/// The message of the one error a client sees when no target answered.
pub(crate) const UNAVAILABLE_MESSAGE: &str = "transaction submission unavailable";

/// Marks a timestamp slot that has never been written.
const NEVER: u64 = u64::MAX;

/// Relays raw transactions to the ordered `--forward-txs` targets with failover and demotion.
///
/// One instance per process, shared by every worker's RPC server through an `Arc`, so all
/// lanes share the connection pools, the demotion state and the metrics.
pub(crate) struct TxForwarder {
    /// The targets in configured (failover) order.
    targets: Vec<Target>,
    /// The attempt timeout, submit budget and demotion cooldown.
    timeouts: Timeouts,
    /// The zero point of the millisecond clock behind the demotion and warning timestamps.
    started: Instant,
    /// When the last "all targets failed" warning was logged, in ms since `started`.
    last_unavailable_warn_ms: AtomicU64,
}

impl TxForwarder {
    /// Build one lazily connecting HTTP client per target.
    ///
    /// `max_request_bytes` is the node's own `--rpc.max-request-size`, so any submission this
    /// node accepted fits the forwarded request. Building an `https://` client installs rustls's
    /// `ring` crypto provider as the process default when none is installed yet (jsonrpsee does
    /// this to pick one of the two providers the build enables).
    pub(crate) fn new(targets: &ForwardTargets, max_request_bytes: u32) -> eyre::Result<Self> {
        let targets = targets
            .as_slice()
            .iter()
            .enumerate()
            .map(|(index, target)| {
                HttpClientBuilder::default()
                    .request_timeout(ATTEMPT_TIMEOUT)
                    .max_response_size(MAX_RESPONSE_BYTES)
                    .max_request_size(max_request_bytes.saturating_add(REQUEST_SIZE_HEADROOM))
                    .max_concurrent_requests(MAX_INFLIGHT_PER_TARGET)
                    .build(target.as_str())
                    .map(|client| Target { client, demoted_until_ms: AtomicU64::new(0) })
                    .map_err(|e| {
                        eyre::eyre!("failed to build the client for forward target {index}: {e}")
                    })
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        RpcTxForwardMetrics::init(targets.len());
        Ok(Self {
            targets,
            timeouts: Timeouts::default(),
            started: Instant::now(),
            last_unavailable_warn_ms: AtomicU64::new(NEVER),
        })
    }

    /// Replace the production timeouts so tests run in milliseconds.
    #[cfg(test)]
    pub(crate) fn with_timeouts(
        mut self,
        attempt: Duration,
        budget: Duration,
        cooldown: Duration,
    ) -> Self {
        self.timeouts = Timeouts { attempt, budget, cooldown };
        self
    }

    /// Forward one raw transaction and return the first target's verdict.
    ///
    /// `Ok` carries the hash a target returned. `Err` carries a target's JSON-RPC error object
    /// exactly as received, the oversized-request error when the request is too large to send,
    /// or the fixed unavailable error when no target answered within [`SUBMIT_BUDGET`].
    pub(crate) async fn submit(&self, bytes: &Bytes) -> Result<B256, ErrorObjectOwned> {
        let deadline = Instant::now() + self.timeouts.budget;
        for index in self.attempt_order() {
            let Some(target) = self.targets.get(index) else { continue };
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            // a timeout shorter than the attempt timeout says nothing about the target, so it
            // ends the submission without demoting or counting the target
            let budget_cut = remaining < self.timeouts.attempt;
            // the outer timeout also covers the wait for one of the target's request slots
            let attempt = tokio::time::timeout(
                remaining.min(self.timeouts.attempt),
                target.client.request::<B256, _>("eth_sendRawTransaction", rpc_params![bytes]),
            )
            .await;
            let kind = match attempt {
                Ok(Ok(hash)) => {
                    target.clear_demotion();
                    RpcTxForwardMetrics::record_outcome(RpcTxForwardOutcome::Accepted);
                    return Ok(hash);
                }
                Ok(Err(ClientError::Call(error))) => {
                    // the target answered: its verdict is final and the target is healthy
                    debug!(
                        target: "tn::rpc::forward",
                        target_index = index,
                        code = error.code(),
                        "target answered with an error"
                    );
                    target.clear_demotion();
                    RpcTxForwardMetrics::record_outcome(RpcTxForwardOutcome::UpstreamError);
                    return Err(error);
                }
                Ok(Err(error)) if is_oversized_request(&error) => {
                    // the bytes are at fault, not the target: no demotion, no failover
                    debug!(
                        target: "tn::rpc::forward",
                        target_index = index,
                        %error,
                        "request too large to forward"
                    );
                    RpcTxForwardMetrics::record_outcome(RpcTxForwardOutcome::UpstreamError);
                    return Err(oversized_request_error());
                }
                Ok(Err(error)) => {
                    let kind = failure_kind(&error);
                    debug!(
                        target: "tn::rpc::forward",
                        target_index = index,
                        kind = kind.label(),
                        %error,
                        "forward attempt failed"
                    );
                    kind
                }
                Err(_elapsed) if budget_cut => break,
                Err(_elapsed) => {
                    debug!(
                        target: "tn::rpc::forward",
                        target_index = index,
                        kind = RpcTxForwardFailure::Timeout.label(),
                        "forward attempt timed out"
                    );
                    RpcTxForwardFailure::Timeout
                }
            };
            target.demote_until(self.now_ms().saturating_add(millis(self.timeouts.cooldown)));
            RpcTxForwardMetrics::record_target_failure(index, kind);
        }
        RpcTxForwardMetrics::record_outcome(RpcTxForwardOutcome::Unavailable);
        self.warn_unavailable();
        Err(unavailable_error())
    }

    /// Target indices in attempt order: healthy targets in configured order, then demoted ones
    /// in configured order.
    ///
    /// The demotion state is read once, up front, so a target demoted during this submission is
    /// not tried a second time. A target whose cooldown has run out is tried in its configured
    /// place by the one submission that claims its probe ([`Target::is_demoted`]).
    fn attempt_order(&self) -> Vec<usize> {
        let now = self.now_ms();
        let probe = millis(self.timeouts.attempt);
        let mut order: Vec<(bool, usize)> = self
            .targets
            .iter()
            .enumerate()
            .map(|(index, target)| (target.is_demoted(now, probe), index))
            .collect();
        order.sort_unstable();
        order.into_iter().map(|(_, index)| index).collect()
    }

    /// Log that every target failed, at most once per [`UNAVAILABLE_WARN_INTERVAL`].
    fn warn_unavailable(&self) {
        let now = self.now_ms();
        let interval = millis(UNAVAILABLE_WARN_INTERVAL);
        let due = self
            .last_unavailable_warn_ms
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                (last == NEVER || now.saturating_sub(last) >= interval).then_some(now)
            })
            .is_ok();
        if due {
            warn!(
                target: "tn::rpc::forward",
                targets = self.targets.len(),
                "all --forward-txs targets failed; raw transaction submissions are being refused"
            );
        }
    }

    /// Milliseconds since this forwarder was built.
    fn now_ms(&self) -> u64 {
        millis(self.started.elapsed())
    }
}

impl fmt::Debug for TxForwarder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // the clients' own Debug prints the target URL
        f.debug_struct("TxForwarder").field("targets", &self.targets.len()).finish_non_exhaustive()
    }
}

/// One configured target and its demotion state.
struct Target {
    /// The lazily connecting JSON-RPC client for this target.
    client: HttpClient,
    /// The forwarder clock (ms) until which this target is tried after the healthy ones; 0 when
    /// healthy. A probe claim moves an expired deadline one attempt timeout ahead.
    demoted_until_ms: AtomicU64,
}

impl Target {
    /// Whether the target is demoted at `now_ms`.
    ///
    /// When the cooldown has run out, one caller claims the probe by moving the deadline
    /// `probe_ms` ahead and tries the target in its configured place; every other caller keeps
    /// trying it last until the probe clears or re-demotes it. A probe that is cancelled before
    /// it ends lapses after `probe_ms`, and the next caller probes again.
    fn is_demoted(&self, now_ms: u64, probe_ms: u64) -> bool {
        let until = self.demoted_until_ms.load(Ordering::Relaxed);
        if until == 0 {
            return false;
        }
        if until > now_ms {
            return true;
        }
        self.demoted_until_ms
            .compare_exchange(
                until,
                now_ms.saturating_add(probe_ms),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
    }

    /// Try this target after the healthy ones until `until_ms`.
    fn demote_until(&self, until_ms: u64) {
        self.demoted_until_ms.store(until_ms, Ordering::Relaxed);
    }

    /// Restore the target to its configured place in the order.
    fn clear_demotion(&self) {
        self.demoted_until_ms.store(0, Ordering::Relaxed);
    }
}

/// The forwarder's time bounds; production uses the module constants.
#[derive(Debug, Clone, Copy)]
struct Timeouts {
    /// Per-target attempt timeout.
    attempt: Duration,
    /// Whole-submission budget across targets.
    budget: Duration,
    /// Demotion cooldown after a failed attempt.
    cooldown: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self { attempt: ATTEMPT_TIMEOUT, budget: SUBMIT_BUDGET, cooldown: DEMOTION_COOLDOWN }
    }
}

/// Classify an attempt that ended without a JSON-RPC reply.
fn failure_kind(error: &ClientError) -> RpcTxForwardFailure {
    match error {
        ClientError::RequestTimeout => RpcTxForwardFailure::Timeout,
        // JSON that is not a JSON-RPC reply to the request: it does not parse as a response,
        // carries another request's id, or its result is not a hash
        ClientError::ParseError(_) | ClientError::InvalidRequestId(_) => {
            RpcTxForwardFailure::Malformed
        }
        // connection failures, non-2xx statuses, over-cap bodies and 2xx bodies that are empty or
        // not JSON all arrive as transport errors; the remaining variants do not occur on an HTTP
        // request but count the same
        _ => RpcTxForwardFailure::Transport,
    }
}

/// Whether `error` says the request was too large to send: refused by the client's own size cap
/// before any I/O, or answered with HTTP 413 by the target.
fn is_oversized_request(error: &ClientError) -> bool {
    match error {
        ClientError::Transport(error) => matches!(
            error.downcast_ref::<TransportError>(),
            Some(TransportError::RequestTooLarge | TransportError::Rejected { status_code: 413 })
        ),
        _ => false,
    }
}

/// The error a client sees when its request is too large to forward: the code and message the
/// node's own RPC server uses for a body over its limit (`-32007` "Request is too big"), without
/// the limit as data, which could describe a target's configuration.
pub(crate) fn oversized_request_error() -> ErrorObjectOwned {
    ErrorObject::owned(OVERSIZED_REQUEST_CODE, OVERSIZED_REQUEST_MSG, None::<()>)
}

/// The one error a client sees when no target answered: `-32603`, a fixed message, no data, so
/// it can carry neither a target nor timing detail.
pub(crate) fn unavailable_error() -> ErrorObjectOwned {
    ErrorObject::owned(INTERNAL_ERROR_CODE, UNAVAILABLE_MESSAGE, None::<()>)
}

/// A duration in whole milliseconds, saturating.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
