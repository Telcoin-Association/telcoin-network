//! Unit tests for the `--forward-txs` relay, plus the loopback validator stand-ins the
//! integration tests in `env/rpc/tests/forward_txs.rs` share.

use super::{
    client::{oversized_request_error, unavailable_error},
    *,
};
use crate::metrics::RpcTxForwardFailure;
use jsonrpsee::{
    server::{Server, ServerHandle},
    types::{
        error::{INTERNAL_ERROR_CODE, OVERSIZED_REQUEST_CODE},
        ErrorObject, ErrorObjectOwned,
    },
    RpcModule,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// One call a fake upstream received: the method name and its raw JSON params.
pub(crate) type Call = (String, String);

/// What a fake upstream answers to every call it serves.
#[derive(Clone, Debug)]
pub(crate) enum Reply {
    /// A JSON-RPC result carrying this hash.
    Hash(B256),
    /// This JSON-RPC error object.
    Error(ErrorObjectOwned),
    /// A JSON-RPC result that is not a transaction hash.
    Garbage,
}

/// A validator stand-in: a jsonrpsee server on loopback that records every call it serves and
/// answers each with the same scripted [`Reply`].
#[derive(Debug)]
pub(crate) struct FakeUpstream {
    /// The URL to configure as a forward target.
    pub(crate) url: String,
    /// Every call served, in arrival order.
    calls: Arc<Mutex<Vec<Call>>>,
    /// Keeps the server running for the life of the fake.
    _handle: ServerHandle,
}

impl FakeUpstream {
    /// Serve `eth_sendRawTransaction` only.
    pub(crate) async fn start(reply: Reply) -> eyre::Result<Self> {
        Self::serving(["eth_sendRawTransaction"], reply).await
    }

    /// Serve every method in `methods`, so a call that leaks to the upstream is recorded
    /// instead of being refused as an unknown method.
    pub(crate) async fn serving(
        methods: impl IntoIterator<Item = &'static str>,
        reply: Reply,
    ) -> eyre::Result<Self> {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut module = RpcModule::new(());
        for method in methods {
            let calls = calls.clone();
            let reply = reply.clone();
            module.register_method(method, move |params, _, _| {
                calls.lock().push((method.to_string(), params.as_str().unwrap_or("").to_string()));
                match &reply {
                    Reply::Hash(hash) => Ok(serde_json::json!(hash)),
                    Reply::Error(error) => Err(error.clone()),
                    Reply::Garbage => Ok(serde_json::json!("not a transaction hash")),
                }
            })?;
        }
        let server = Server::builder().build("127.0.0.1:0").await?;
        let url = format!("http://{}", server.local_addr()?);
        let handle = server.start(module);
        Ok(Self { url, calls, _handle: handle })
    }

    /// Every call served so far.
    pub(crate) fn calls(&self) -> Vec<Call> {
        self.calls.lock().clone()
    }

    /// The method names of every call served so far.
    pub(crate) fn methods(&self) -> Vec<String> {
        self.calls().into_iter().map(|(method, _)| method).collect()
    }
}

/// A loopback URL nothing listens on: bind an ephemeral port, then release it.
pub(crate) fn closed_endpoint() -> eyre::Result<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    Ok(format!("http://{addr}"))
}

/// A loopback endpoint that completes the TCP handshake and never answers.
///
/// Accepted connections are held for the life of the process. `map_while` ends the accept loop
/// on a persistent accept failure instead of spinning on it.
pub(crate) fn blackhole_endpoint() -> eyre::Result<String> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    std::thread::spawn(move || {
        let _held: Vec<std::net::TcpStream> = listener.incoming().map_while(Result::ok).collect();
    });
    Ok(format!("http://{addr}"))
}

/// A loopback endpoint that writes `response` verbatim on every connection, then drains it.
///
/// Draining until the client hangs up keeps the socket open while the client finishes sending,
/// so the client never races a reset against the response read.
pub(crate) async fn canned_endpoint(response: String) -> eyre::Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let response = response.clone();
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt as _;
                let (mut reader, mut writer) = socket.split();
                if writer.write_all(response.as_bytes()).await.is_ok() {
                    // the connection ends either way; nothing to report
                    let _ = tokio::io::copy(&mut reader, &mut tokio::io::sink()).await;
                }
            });
        }
    });
    Ok(format!("http://{addr}"))
}

/// A minimal HTTP/1.1 response.
fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// A forwarder over `urls` in order, with a short attempt timeout.
fn forwarder<S: AsRef<str>>(urls: &[S]) -> eyre::Result<TxForwarder> {
    let list = urls.iter().map(AsRef::as_ref).collect::<Vec<&str>>().join(",");
    Ok(TxForwarder::new(&parse_forward_targets(&list)?, None, 1024 * 1024)?.with_timeouts(
        Duration::from_millis(500),
        Duration::from_secs(5),
        Duration::from_secs(30),
    ))
}

/// Opaque submission bytes: the forwarder relays without decoding.
fn raw_tx() -> Bytes {
    Bytes::from_static(b"\x02opaque signed transaction")
}

/// The one call an upstream receives for [`raw_tx`].
fn raw_tx_call() -> Call {
    ("eth_sendRawTransaction".to_string(), format!("[\"{}\"]", raw_tx()))
}

/// The counters of one snapshot of a debugging recorder. Taking a snapshot resets the recorder's
/// counters, so each snapshot holds the counts since the previous one.
struct Counters(Vec<(metrics::Key, u64)>);

impl Counters {
    /// Snapshot every counter `snapshotter` holds.
    fn take(snapshotter: &Snapshotter) -> Self {
        let counters =
            snapshotter.snapshot().into_vec().into_iter().filter_map(|(key, _, _, value)| {
                match value {
                    DebugValue::Counter(count) => Some((key.key().clone(), count)),
                    _ => None,
                }
            });
        Self(counters.collect())
    }

    /// The counter `name` whose labels include every pair in `labels`.
    fn get(&self, name: &str, labels: &[(&str, &str)]) -> Option<u64> {
        self.0.iter().find_map(|(key, count)| {
            let matches = key.name() == name
                && labels
                    .iter()
                    .all(|(k, v)| key.labels().any(|l| l.key() == *k && l.value() == *v));
            matches.then_some(*count)
        })
    }

    /// The submissions counted under `outcome`.
    fn outcome(&self, outcome: &str) -> Option<u64> {
        self.get("tn_reth.rpc_tx_forwarded_total", &[("outcome", outcome)])
    }

    /// The failed attempts of `kind` counted against the target at index `target`.
    fn failures(&self, target: &str, kind: RpcTxForwardFailure) -> Option<u64> {
        self.get(
            "tn_reth.rpc_tx_forward_target_failures_total",
            &[("target", target), ("kind", kind.label())],
        )
    }
}

#[tokio::test]
async fn test_forward_returns_upstream_hash_verbatim() -> eyre::Result<()> {
    let hash = B256::repeat_byte(0xab);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;

    let result = forwarder(&[&upstream.url])?.submit(&raw_tx()).await;

    assert_eq!(result, Ok(hash));
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

/// Code, message and the raw `data` text all pass through untouched.
#[tokio::test]
async fn test_forward_passes_upstream_error_object_verbatim() -> eyre::Result<()> {
    let error = ErrorObject::owned(
        -32000,
        "nonce too low: next nonce 5, tx nonce 3",
        Some(serde_json::json!({"k": [1, 2]})),
    );
    let upstream = FakeUpstream::start(Reply::Error(error)).await?;

    let err = forwarder(&[&upstream.url])?.submit(&raw_tx()).await.expect_err("upstream error");

    assert_eq!(err.code(), -32000);
    assert_eq!(err.message(), "nonce too low: next nonce 5, tx nonce 3");
    assert_eq!(err.data().map(|data| data.get()), Some(r#"{"k":[1,2]}"#));
    Ok(())
}

/// A JSON-RPC error is the validator's verdict: the next target never sees the transaction.
#[tokio::test]
async fn test_forward_does_not_fail_over_on_upstream_error() -> eyre::Result<()> {
    let first =
        FakeUpstream::start(Reply::Error(ErrorObject::owned(-32000, "already known", None::<()>)))
            .await?;
    let second = FakeUpstream::start(Reply::Hash(B256::repeat_byte(1))).await?;

    let err = forwarder(&[&first.url, &second.url])?.submit(&raw_tx()).await.expect_err("error");

    assert_eq!(err.message(), "already known");
    assert_eq!(first.calls(), vec![raw_tx_call()]);
    assert!(second.calls().is_empty(), "no failover on a JSON-RPC error");
    Ok(())
}

#[tokio::test]
async fn test_forward_fails_over_on_connection_refused() -> eyre::Result<()> {
    let hash = B256::repeat_byte(2);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;

    let result = forwarder(&[&closed_endpoint()?, &upstream.url])?.submit(&raw_tx()).await;

    assert_eq!(result, Ok(hash));
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

#[tokio::test]
async fn test_forward_fails_over_on_timeout() -> eyre::Result<()> {
    let hash = B256::repeat_byte(3);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let forwarder = forwarder(&[&blackhole_endpoint()?, &upstream.url])?.with_timeouts(
        Duration::from_millis(300),
        Duration::from_secs(5),
        Duration::from_secs(30),
    );

    let started = Instant::now();
    let result = forwarder.submit(&raw_tx()).await;

    assert_eq!(result, Ok(hash));
    assert!(started.elapsed() >= Duration::from_millis(300), "the hung target was tried first");
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

#[tokio::test]
async fn test_forward_fails_over_on_http_error_status() -> eyre::Result<()> {
    let hash = B256::repeat_byte(4);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let bad_gateway = canned_endpoint(http_response("502 Bad Gateway", "bad gateway")).await?;

    let result = forwarder(&[&bad_gateway, &upstream.url])?.submit(&raw_tx()).await;

    assert_eq!(result, Ok(hash));
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

#[tokio::test]
async fn test_forward_fails_over_on_oversized_response() -> eyre::Result<()> {
    let hash = B256::repeat_byte(5);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let body = "x".repeat(client::MAX_RESPONSE_BYTES as usize + 1);
    let oversized = canned_endpoint(http_response("200 OK", &body)).await?;

    let result = forwarder(&[&oversized, &upstream.url])?.submit(&raw_tx()).await;

    assert_eq!(result, Ok(hash));
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

/// The client gets one fixed object that names no target: no host, no port, no data.
#[tokio::test]
async fn test_forward_all_targets_failed_returns_fixed_error_without_target() -> eyre::Result<()> {
    let first = closed_endpoint()?;
    let second = closed_endpoint()?;

    let err = forwarder(&[&first, &second])?.submit(&raw_tx()).await.expect_err("unavailable");

    assert_eq!(err, unavailable_error());
    assert_eq!(err.code(), INTERNAL_ERROR_CODE);
    assert_eq!(err.message(), "transaction submission unavailable");
    assert!(err.data().is_none());
    let ports = [&first, &second].map(|url| url.rsplit(':').next().unwrap_or_default().to_string());
    for printed in [err.to_string(), format!("{err:?}"), serde_json::to_string(&err)?] {
        assert!(!printed.contains("127.0.0.1"), "{printed} names a target host");
        for port in &ports {
            assert!(!printed.contains(port.as_str()), "{printed} names a target port");
        }
    }
    Ok(())
}

/// Two hung targets cost the submit budget, not two full attempt timeouts. The clock is paused,
/// so the elapsed time is exact rather than subject to host load.
#[tokio::test(start_paused = true)]
async fn test_forward_budget_bounds_total_latency() -> eyre::Result<()> {
    let forwarder = forwarder(&[&blackhole_endpoint()?, &blackhole_endpoint()?])?.with_timeouts(
        Duration::from_secs(1),
        Duration::from_millis(1200),
        Duration::from_secs(30),
    );

    let started = tokio::time::Instant::now();
    let err = forwarder.submit(&raw_tx()).await.expect_err("both targets hang");
    let elapsed = started.elapsed();

    assert_eq!(err, unavailable_error());
    assert!(elapsed >= Duration::from_millis(1200), "the budget was spent: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "two full attempts would take 2s: {elapsed:?}");
    Ok(())
}

/// The second target gets only the 200 ms left of the budget. Its timeout ends the submission
/// without demoting or counting it, since it never had its full attempt timeout.
#[tokio::test(start_paused = true)]
async fn test_forward_budget_cut_attempt_is_not_charged_to_target() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // the test runtime polls every future on this thread, so the guard covers the awaits too
    let _guard = metrics::set_default_local_recorder(&recorder);
    let forwarder = forwarder(&[&blackhole_endpoint()?, &blackhole_endpoint()?])?.with_timeouts(
        Duration::from_secs(1),
        Duration::from_millis(1200),
        Duration::from_secs(30),
    );

    let err = forwarder.submit(&raw_tx()).await.expect_err("both targets hang");

    assert_eq!(err, unavailable_error());
    let counters = Counters::take(&snapshotter);
    assert_eq!(counters.failures("0", RpcTxForwardFailure::Timeout), Some(1));
    assert_eq!(counters.failures("1", RpcTxForwardFailure::Timeout), Some(0));
    assert_eq!(counters.outcome("unavailable"), Some(1));
    Ok(())
}

/// A submission the node accepted at exactly its request size limit still goes out, although
/// the forwarded envelope adds the `0x` prefix the client omitted.
#[tokio::test]
async fn test_forward_request_at_node_limit_is_forwarded() -> eyre::Result<()> {
    let hash = B256::repeat_byte(8);
    let upstream = FakeUpstream::start(Reply::Hash(hash)).await?;
    let prefixed = raw_tx().to_string();
    let unprefixed = prefixed.strip_prefix("0x").unwrap_or(&prefixed);
    // the smallest envelope a client can send for raw_tx: id 0 and no 0x prefix
    let incoming = format!(
        r#"{{"jsonrpc":"2.0","id":0,"method":"eth_sendRawTransaction","params":["{unprefixed}"]}}"#
    );
    let node_limit = u32::try_from(incoming.len())?;
    let forwarder = TxForwarder::new(&parse_forward_targets(&upstream.url)?, None, node_limit)?;

    assert_eq!(forwarder.submit(&raw_tx()).await, Ok(hash));
    assert_eq!(upstream.calls(), vec![raw_tx_call()]);
    Ok(())
}

/// A target that answers 413 refused the request, not the submission: the client gets the
/// oversized-request error, the next target never receives the upload, and the target keeps its
/// place in the order.
#[tokio::test]
async fn test_forward_payload_too_large_is_final_answer() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // the test runtime polls every future on this thread, so the guard covers the awaits too
    let _guard = metrics::set_default_local_recorder(&recorder);
    // close after each response so the second submission is answered on a fresh connection
    let too_large = canned_endpoint(
        "HTTP/1.1 413 Payload Too Large\r\nconnection: close\r\ncontent-length: 0\r\n\r\n"
            .to_string(),
    )
    .await?;
    let second = FakeUpstream::start(Reply::Hash(B256::repeat_byte(9))).await?;
    let forwarder = forwarder(&[&too_large, &second.url])?;

    for _ in 0..2 {
        let err = forwarder.submit(&raw_tx()).await.expect_err("413 is final");
        assert_eq!(err, oversized_request_error());
        assert_eq!(err.code(), OVERSIZED_REQUEST_CODE);
        assert!(err.data().is_none());
    }

    // not demoted: the second submission went to the first target again
    assert!(second.calls().is_empty(), "no failover on 413");
    let counters = Counters::take(&snapshotter);
    assert_eq!(counters.failures("0", RpcTxForwardFailure::Transport), Some(0));
    assert_eq!(counters.outcome("upstream_error"), Some(2));
    assert_eq!(counters.outcome("unavailable"), Some(0));
    Ok(())
}

/// A target that failed below the JSON-RPC layer goes to the back of the order until its
/// cooldown ends, then gets its configured place back.
#[tokio::test]
async fn test_forward_tries_demoted_target_last_until_cooldown() -> eyre::Result<()> {
    let hash = B256::repeat_byte(6);
    let flaky = FakeUpstream::start(Reply::Garbage).await?;
    let healthy = FakeUpstream::start(Reply::Hash(hash)).await?;
    let forwarder = forwarder(&[&flaky.url, &healthy.url])?.with_timeouts(
        Duration::from_millis(500),
        Duration::from_secs(5),
        Duration::from_millis(300),
    );

    assert_eq!(forwarder.submit(&raw_tx()).await, Ok(hash));
    assert_eq!((flaky.calls().len(), healthy.calls().len()), (1, 1));

    // demoted: the healthy target answers first and the flaky one is not reached
    assert_eq!(forwarder.submit(&raw_tx()).await, Ok(hash));
    assert_eq!((flaky.calls().len(), healthy.calls().len()), (1, 2));

    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(forwarder.submit(&raw_tx()).await, Ok(hash));
    assert_eq!((flaky.calls().len(), healthy.calls().len()), (2, 3), "cooldown over: first again");
    Ok(())
}

/// Every outcome and every per-target failure lands in its own series, labeled by the
/// target's index.
#[tokio::test]
async fn test_forward_metrics_count_outcomes() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // the test runtime polls every future on this thread, so the guard covers the awaits too
    let _guard = metrics::set_default_local_recorder(&recorder);

    let accepting = FakeUpstream::start(Reply::Hash(B256::repeat_byte(7))).await?;
    let rejecting =
        FakeUpstream::start(Reply::Error(ErrorObject::owned(-32000, "underpriced", None::<()>)))
            .await?;
    let closed = closed_endpoint()?;

    let relay = forwarder(&[&closed, &accepting.url])?;
    relay.submit(&raw_tx()).await.expect("second target accepts");
    forwarder(&[&rejecting.url])?.submit(&raw_tx()).await.expect_err("upstream error");
    forwarder(&[&closed])?.submit(&raw_tx()).await.expect_err("unavailable");
    let handler = EthSubmitForwarded::new((), Arc::new(relay), TxFeeCapWei::new(1));
    handler.local_check(b"not a transaction").expect_err("over the cap check");

    let counters = Counters::take(&snapshotter);
    for outcome in ["accepted", "upstream_error", "unavailable", "rejected_locally"] {
        assert_eq!(counters.outcome(outcome), Some(1), "outcome {outcome}");
    }

    // the closed endpoint was target 0 in both lists that named it
    assert_eq!(counters.failures("0", RpcTxForwardFailure::Transport), Some(2));
    assert_eq!(counters.failures("0", RpcTxForwardFailure::Timeout), Some(0));
    assert_eq!(counters.failures("0", RpcTxForwardFailure::Malformed), Some(0));
    // registered at zero for the second target, which never failed
    assert_eq!(counters.failures("1", RpcTxForwardFailure::Transport), Some(0));
    Ok(())
}

/// When a hung target's cooldown runs out, one submission probes it and the others keep trying
/// it last, so the hung target costs one attempt timeout per cooldown, not one per submission.
#[tokio::test]
async fn test_forward_single_probe_after_cooldown() -> eyre::Result<()> {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    // the test runtime polls every future on this thread, so the guard covers the awaits too
    let _guard = metrics::set_default_local_recorder(&recorder);
    let hash = B256::repeat_byte(10);
    let healthy = FakeUpstream::start(Reply::Hash(hash)).await?;
    let forwarder = forwarder(&[&blackhole_endpoint()?, &healthy.url])?.with_timeouts(
        Duration::from_millis(300),
        Duration::from_secs(2),
        Duration::from_millis(200),
    );

    assert_eq!(forwarder.submit(&raw_tx()).await, Ok(hash));
    let demoting = Counters::take(&snapshotter);
    assert_eq!(demoting.failures("0", RpcTxForwardFailure::Timeout), Some(1));

    tokio::time::sleep(Duration::from_millis(250)).await;
    let tx = raw_tx();
    let results = futures::future::join_all((0..8).map(|_| forwarder.submit(&tx))).await;

    assert!(results.iter().all(|result| *result == Ok(hash)), "{results:?}");
    // the snapshot above reset the counters, so this is the count since the cooldown ran out
    assert_eq!(
        Counters::take(&snapshotter).failures("0", RpcTxForwardFailure::Timeout),
        Some(1),
        "exactly one of the eight submissions probed the hung target"
    );
    assert_eq!(healthy.calls().len(), 9);
    Ok(())
}

/// Building an https client must not panic on TLS or crypto-provider setup. Dials nothing.
#[tokio::test]
async fn test_https_target_client_builds() -> eyre::Result<()> {
    let targets = parse_forward_targets("https://node1.telcoin.network,https://[::1]:9443")?;
    TxForwarder::new(&targets, None, 1024 * 1024)?;
    Ok(())
}
