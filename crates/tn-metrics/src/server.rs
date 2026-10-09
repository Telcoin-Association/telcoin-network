//! Minimal HTTP `/metrics` endpoint for Prometheus scrapes.
//!
//! Uses the node healthcheck server's raw-TCP pattern. Complete bounded request headers
//! negotiate optional gzip, and each accepted scrape receives the full rendered registry.
//! Node operators must protect the endpoint with a firewall.

use std::{
    fmt,
    future::poll_fn,
    io::{self, Write},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use flate2::{write::GzEncoder, Compression};
use tn_types::TaskSpawner;
use tokio::{
    io::{AsyncRead, AsyncWriteExt, ReadBuf},
    net::TcpListener,
    sync::oneshot,
    time,
};
use tracing::{debug, info};

use crate::recorder::install_recorder;

/// Maximum complete request-header size, including the final CRLF pair.
const MAX_REQUEST_HEADER_BYTES: usize = 16 * 1024;

/// Compression input and output limit; larger registries remain complete identity responses.
const MAX_GZIP_BYTES: usize = 64 * 1024 * 1024;

/// Representation negotiated for a scrape response.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ContentEncoding {
    /// Send the complete rendered registry without compression.
    #[default]
    Identity,
    /// Send a gzip stream when it fits the compression budget.
    Gzip,
}

/// Complete response body, retaining its representation for HTTP framing.
#[derive(Debug)]
enum ResponseBody {
    /// Complete metrics exposition text.
    Identity(String),
    /// Complete gzip stream containing metrics exposition text.
    Gzip(Vec<u8>),
}

impl ResponseBody {
    /// Borrow the bytes that will be written to the socket.
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Identity(body) => body.as_bytes(),
            Self::Gzip(body) => body.as_slice(),
        }
    }

    /// Frame the selected representation with its encoded byte length.
    fn header(&self) -> String {
        let encoding = match self {
            Self::Identity(_) => "",
            Self::Gzip(_) => "Content-Encoding: gzip\r\n",
        };
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n{encoding}Vary: Accept-Encoding\r\nConnection: close\r\n\r\n",
            self.as_bytes().len(),
        )
    }
}

/// Gzip output buffer with a fixed byte and allocation-growth budget.
#[derive(Debug, Default)]
struct BoundedGzipWriter {
    /// Bytes produced so far, including the gzip header and trailer.
    bytes: Vec<u8>,
}

impl Write for BoundedGzipWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let length = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_GZIP_BYTES)
            .ok_or_else(|| io::Error::other("metrics gzip output exceeds compression budget"))?;
        if length > self.bytes.capacity() {
            let capacity = self.bytes.capacity().saturating_mul(2).max(length).min(MAX_GZIP_BYTES);
            self.bytes
                .try_reserve_exact(capacity.saturating_sub(self.bytes.len()))
                .map_err(io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Accept only valid HTTP qvalues with a nonzero quality, without floating-point parsing.
fn quality_allows_gzip(value: &str) -> bool {
    value.split_once('.').map_or_else(
        || value == "1",
        |(whole, fraction)| {
            fraction.len() <= 3
                && fraction.bytes().all(|byte| byte.is_ascii_digit())
                && ((whole == "0" && fraction.bytes().any(|byte| byte != b'0'))
                    || (whole == "1" && fraction.bytes().all(|byte| byte == b'0')))
        },
    )
}

/// Treat missing or malformed quality parameters as refusing gzip.
fn parameter_allows_gzip(parameter: &str) -> bool {
    parameter.split_once('=').map_or_else(
        || !parameter.trim().eq_ignore_ascii_case("q"),
        |(name, value)| !name.trim().eq_ignore_ascii_case("q") || quality_allows_gzip(value.trim()),
    )
}

/// Combine repeated preferences conservatively so an explicit refusal remains effective.
fn combine_encoding(previous: Option<ContentEncoding>, next: ContentEncoding) -> ContentEncoding {
    previous.filter(|encoding| *encoding == ContentEncoding::Identity).unwrap_or(next)
}

/// Negotiate gzip across all Accept-Encoding fields, with explicit entries overriding wildcard.
fn request_encoding(headers: &[u8]) -> ContentEncoding {
    let headers = String::from_utf8_lossy(headers);
    let (gzip, wildcard) = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.trim().eq_ignore_ascii_case("accept-encoding"))
        .flat_map(|(_, value)| value.split(','))
        .fold((None, None), |(gzip, wildcard), value| {
            let mut parts = value.split(';');
            let coding = parts.next().unwrap_or_default().trim();
            let encoding = if parts.all(parameter_allows_gzip) {
                ContentEncoding::Gzip
            } else {
                ContentEncoding::Identity
            };
            if coding.eq_ignore_ascii_case("gzip") {
                (Some(combine_encoding(gzip, encoding)), wildcard)
            } else if coding == "*" {
                (gzip, Some(combine_encoding(wildcard, encoding)))
            } else {
                (gzip, wildcard)
            }
        });
    gzip.or(wildcard).unwrap_or_default()
}

/// Read complete request headers within a fixed buffer, including fragmented terminators.
async fn read_request_encoding<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<ContentEncoding> {
    let mut headers = [0u8; MAX_REQUEST_HEADER_BYTES];
    let mut length = 0;
    poll_fn(|cx| {
        let remaining = headers
            .get_mut(length..)
            .ok_or_else(|| io::Error::other("invalid metrics request-header offset"))?;
        let mut buffer = ReadBuf::new(remaining);
        match Pin::new(&mut *reader).poll_read(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {
                let read = buffer.filled().len();
                length += read;
                let received = headers
                    .get(..length)
                    .ok_or_else(|| io::Error::other("invalid metrics request-header length"))?;
                let encoding = received
                    .windows(4)
                    .position(|bytes| bytes == b"\r\n\r\n")
                    .and_then(|offset| received.get(..offset + 4))
                    .map(request_encoding);
                encoding.map_or_else(
                    || match () {
                        () if read == 0 => Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "incomplete metrics headers",
                        ))),
                        () if length == MAX_REQUEST_HEADER_BYTES => {
                            Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "metrics headers exceed byte limit",
                            )))
                        }
                        () => {
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    },
                    |encoding| Poll::Ready(Ok(encoding)),
                )
            }
        }
    })
    .await
}

/// Compress one complete registry at level one within the input and output budgets.
fn compress_metrics(bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() > MAX_GZIP_BYTES {
        Err(io::Error::other("metrics gzip input exceeds compression budget"))
    } else {
        let mut encoder = GzEncoder::new(BoundedGzipWriter::default(), Compression::fast());
        encoder.write_all(bytes)?;
        encoder.finish().map(|writer| writer.bytes)
    }
}

/// Preserve the full registry on identity requests and whenever bounded compression fails.
fn encode_metrics(body: String, encoding: ContentEncoding) -> ResponseBody {
    match encoding {
        ContentEncoding::Identity => ResponseBody::Identity(body),
        ContentEncoding::Gzip => compress_metrics(body.as_bytes()).map_or_else(
            |error| {
                debug!(target: "tn::metrics", ?error, "metrics gzip unavailable; using identity");
                ResponseBody::Identity(body)
            },
            ResponseBody::Gzip,
        ),
    }
}

/// Callbacks executed before each scrape renders the registry.
///
/// Used for metrics that are sampled on demand rather than recorded on events,
/// e.g. process stats and database table sizes.
pub struct MetricsHooks {
    /// The hooks to run, in registration order.
    hooks: Vec<Box<dyn Fn() + Send + Sync>>,
}

impl Default for MetricsHooks {
    /// The default hook set samples process metrics (cpu, memory, fds) on every scrape.
    fn default() -> Self {
        let collector = metrics_process::Collector::default();
        // register HELP text once; requires the global recorder to be installed already
        collector.describe();
        Self { hooks: vec![Box::new(move || collector.collect())] }
    }
}

impl fmt::Debug for MetricsHooks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsHooks").field("hooks", &self.hooks.len()).finish()
    }
}

impl MetricsHooks {
    /// Create an empty hook set (no process metrics).
    pub fn empty() -> Self {
        Self { hooks: Vec::new() }
    }

    /// Add a hook to run before each scrape.
    ///
    /// Hooks run on the blocking thread pool (see [`start_metrics_server`]), so synchronous
    /// db reads or other blocking sampling are acceptable. They still execute inline with the
    /// scrape response, so avoid unbounded work that would delay every scrape.
    pub fn with_hook(mut self, hook: impl Fn() + Send + Sync + 'static) -> Self {
        self.hooks.push(Box::new(hook));
        self
    }

    /// Run all hooks in registration order.
    fn run(&self) {
        for hook in &self.hooks {
            hook();
        }
    }
}

/// Start the Prometheus scrape endpoint and return the bound address.
///
/// Installs the global recorder if it isn't installed yet (idempotent - the node CLI
/// installs it much earlier, before reth components are constructed). Registers a static
/// `tn_info{version=...} 1` gauge for dashboards, then spawns two non-critical tasks: one serves
/// scrapes, the other runs registry upkeep every 5s (drains stale histogram samples) on its own
/// cadence so upkeep never waits behind an in-flight scrape.
///
/// Each scrape's synchronous work (pre-scrape hooks, full-registry render, and bounded level-one
/// gzip when requested) runs on the blocking pool via the task spawner. The serial serve loop
/// permits at most one such task at a time. Compression never runs on a shared runtime worker.
///
/// # Security Considerations
///
/// Like the healthcheck endpoint, this accepts connections from any source. It reads bounded
/// complete headers to negotiate gzip, without authenticating or restricting the request path.
/// Operators must firewall the port.
/// Binding to a loopback address (e.g. `127.0.0.1:9001`) with a local scraper relaying
/// to remote storage is the recommended deployment.
pub async fn start_metrics_server(
    addr: SocketAddr,
    task_spawner: &TaskSpawner,
    version: &'static str,
    hooks: MetricsHooks,
) -> eyre::Result<SocketAddr> {
    let handle = install_recorder()?;

    // IMPORTANT: use firewall to protect this endpoint
    let listener = TcpListener::bind(addr).await?;
    let listen_on = listener.local_addr()?;

    // static info series so dashboards can surface the running version
    metrics::gauge!("tn_info", "version" => version).set(1.0);

    info!(target: "tn::metrics", ?listen_on, "prometheus metrics endpoint listening");

    // share hooks across per-scrape blocking tasks; clone the spawner so the serve loop can
    // offload synchronous work (hooks + render) to the blocking pool.
    let hooks = Arc::new(hooks);
    let spawner = task_spawner.clone();

    // registry upkeep runs on its own task so it ticks on a steady 5s cadence regardless of
    // scrape activity - draining stale histogram samples must never wait behind an in-flight
    // scrape's render + write. run_upkeep is a cheap sample drain and is safe to run while a
    // render is in flight on the blocking pool (the handle is Send + Sync and is already used
    // concurrently with metric recording across the node).
    let upkeep_handle = handle.clone();
    task_spawner.spawn_task("metrics-upkeep", async move {
        let mut upkeep = time::interval(Duration::from_secs(5));
        upkeep.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        loop {
            upkeep.tick().await;
            upkeep_handle.run_upkeep();
        }
    });

    task_spawner.spawn_task("metrics", async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    // drain the request before responding - closing a socket with
                    // unread bytes in the kernel buffer sends RST instead of FIN,
                    // resetting the response mid-flight on the scraper's side. the
                    // timeout bounds idle probes that connect but never send.
                    let response = async {
                        let encoding = time::timeout(
                            Duration::from_secs(2),
                            read_request_encoding(&mut socket),
                        )
                        .await
                        .map_err(io::Error::other)??;
                        // offload hooks, the full-registry render, and optional bounded gzip
                        // to the blocking pool. The hooks and render are synchronous (the db hook
                        // walks every table plus the mdbx freelist; render
                        // serializes the whole registry), so running them
                        // inline would pin a shared runtime worker. the
                        // body returns over the oneshot; a panicking hook
                        // drops the sender, so we skip the scrape instead of stalling.
                        let (tx, rx) = oneshot::channel();
                        let hooks = hooks.clone();
                        let render = handle.clone();
                        spawner.spawn_blocking_task("metrics-render", move || {
                            hooks.run();
                            let _ = tx.send(encode_metrics(render.render(), encoding));
                            Ok(())
                        });
                        rx.await.map_err(io::Error::other)
                    }
                    .await;
                    if let Ok(body) = response {
                        // write the status line + headers and the body as separate frames so
                        // the rendered registry (potentially several MB) is not copied a
                        // second time just to prepend the status line. ignore errors (client
                        // disconnect, etc.) then drop the connection. the timeout bounds a
                        // client that connects but never drains the body - without it, TCP
                        // backpressure would block write_all indefinitely and freeze this
                        // serve task.
                        let header = body.header();
                        let _ = time::timeout(Duration::from_secs(5), async {
                            let _ = socket.write_all(header.as_bytes()).await;
                            let _ = socket.write_all(body.as_bytes()).await;
                            let _ = socket.shutdown().await;
                        })
                        .await;
                    }
                }
                // transient accept errors (e.g. fd exhaustion) must not kill the node
                Err(e) => debug!(target: "tn::metrics", ?e, "metrics endpoint accept error"),
            }
        }
    });

    Ok(listen_on)
}

#[cfg(test)]
mod tests {
    use std::{io::Read, task::Context};

    use super::*;
    use flate2::read::GzDecoder;
    use tn_types::TaskManager;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// Reader that deterministically splits a request into small ready chunks.
    #[derive(Debug)]
    struct FragmentedReader<'a> {
        /// Request bytes not yet read.
        bytes: &'a [u8],
        /// Maximum bytes supplied by one poll.
        chunk_bytes: usize,
    }

    impl AsyncRead for FragmentedReader<'_> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let length = self.bytes.len().min(self.chunk_bytes).min(buffer.remaining());
            let (chunk, remaining) = self
                .bytes
                .split_at_checked(length)
                .ok_or_else(|| io::Error::other("invalid test request chunk"))?;
            buffer.put_slice(chunk);
            self.bytes = remaining;
            Poll::Ready(Ok(()))
        }
    }

    /// Gzip tokens and qualities are case-insensitive and malformed qualities fail closed.
    #[test]
    fn gzip_negotiation_respects_quality_and_wildcard() {
        [
            ("", ContentEncoding::Identity),
            ("identity", ContentEncoding::Identity),
            ("deflate, gzip", ContentEncoding::Gzip),
            ("GZIP;Q=1.000", ContentEncoding::Gzip),
            ("gzip;q=0.001", ContentEncoding::Gzip),
            ("gzip;q=0", ContentEncoding::Identity),
            ("gzip;q=0.000", ContentEncoding::Identity),
            ("gzip;q=NaN", ContentEncoding::Identity),
            ("gzip;q=-1", ContentEncoding::Identity),
            ("gzip;q=1.001", ContentEncoding::Identity),
            ("gzip;q=.5", ContentEncoding::Identity),
            ("gzip;q=0.0001", ContentEncoding::Identity),
            ("gzip;q", ContentEncoding::Identity),
            ("gzip;q=", ContentEncoding::Identity),
            ("gzip;q=1;q=0", ContentEncoding::Identity),
            ("*;q=0.5", ContentEncoding::Gzip),
            ("*;q=0", ContentEncoding::Identity),
            ("*;q=1, gzip;q=0", ContentEncoding::Identity),
            ("gzip;q=0, *;q=1", ContentEncoding::Identity),
            ("gzip;q=NaN, *;q=1", ContentEncoding::Identity),
            ("gzip;q=0, gzip;q=1", ContentEncoding::Identity),
        ]
        .into_iter()
        .for_each(|(value, expected)| {
            let headers = format!("GET /metrics HTTP/1.1\r\nAccept-Encoding: {value}\r\n\r\n");
            assert_eq!(request_encoding(headers.as_bytes()), expected, "{value}");
        });
    }

    /// Duplicate fields retain explicit refusals, including invalid bytes in a quality.
    #[test]
    fn duplicate_accept_encoding_fields_preserve_explicit_refusal() {
        assert_eq!(
            request_encoding(
                b"GET / HTTP/1.1\r\nAccept-Encoding: *\r\naCcEpT-EnCoDiNg: GZIP;q=0\r\n\r\n"
            ),
            ContentEncoding::Identity,
        );
        assert_eq!(
            request_encoding(
                b"GET / HTTP/1.1\r\nAccept-Encoding: gzip;q=0\r\nAccept-Encoding: gzip\r\n\r\n"
            ),
            ContentEncoding::Identity,
        );
        assert_eq!(
            request_encoding(
                b"GET / HTTP/1.1\r\nAccept-Encoding: gzip;q=\xff\r\nAccept-Encoding: *\r\n\r\n"
            ),
            ContentEncoding::Identity,
        );
        assert_eq!(
            request_encoding(
                b"GET / HTTP/1.1\r\nAccept-Encoding: br\r\nAccept-Encoding: GZIP\r\n\r\n"
            ),
            ContentEncoding::Gzip,
        );
    }

    /// Headers beyond the former single-read buffer and split terminators are parsed completely.
    #[tokio::test]
    async fn fragmented_headers_negotiate_gzip() -> io::Result<()> {
        let request = format!(
            "GET /metrics HTTP/1.1\r\nX-Padding: {}\r\nAccept-Encoding: gzip\r\n\r\n",
            "x".repeat(2048),
        );
        let mut reader = FragmentedReader { bytes: request.as_bytes(), chunk_bytes: 1 };
        assert_eq!(read_request_encoding(&mut reader).await?, ContentEncoding::Gzip);
        Ok(())
    }

    /// A complete header exactly at the cap succeeds, while oversized and truncated headers fail.
    #[tokio::test]
    async fn request_headers_enforce_complete_size_bound() -> io::Result<()> {
        let prefix = "GET /metrics HTTP/1.1\r\nX-Padding: ";
        let suffix = "\r\nAccept-Encoding: gzip\r\n\r\n";
        let padding = MAX_REQUEST_HEADER_BYTES.saturating_sub(prefix.len() + suffix.len());
        let request = format!("{prefix}{}{suffix}", "x".repeat(padding));
        let mut reader = FragmentedReader { bytes: request.as_bytes(), chunk_bytes: 257 };
        assert_eq!(read_request_encoding(&mut reader).await?, ContentEncoding::Gzip);

        let oversized = format!("{prefix}{}{suffix}", "x".repeat(padding + 1));
        let mut reader = FragmentedReader { bytes: oversized.as_bytes(), chunk_bytes: 257 };
        assert_eq!(
            read_request_encoding(&mut reader).await.err().map(|error| error.kind()),
            Some(io::ErrorKind::InvalidData),
        );
        let mut reader = FragmentedReader { bytes: b"GET / HTTP/1.1\r\n", chunk_bytes: 1 };
        assert_eq!(
            read_request_encoding(&mut reader).await.err().map(|error| error.kind()),
            Some(io::ErrorKind::UnexpectedEof),
        );
        Ok(())
    }

    /// Bytes after the complete header cannot inject an encoding preference.
    #[tokio::test]
    async fn request_body_does_not_change_encoding() -> io::Result<()> {
        let mut reader = FragmentedReader {
            bytes: b"GET / HTTP/1.1\r\n\r\nAccept-Encoding: gzip\r\n",
            chunk_bytes: MAX_REQUEST_HEADER_BYTES,
        };
        assert_eq!(read_request_encoding(&mut reader).await?, ContentEncoding::Identity);
        Ok(())
    }

    /// Both encodings preserve all text, and gzip framing counts encoded bytes.
    #[test]
    fn response_bodies_preserve_full_metrics_and_frame_encoded_length() -> io::Result<()> {
        let text = "# TYPE tn_sample gauge\ntn_sample{label=\"repeat\"} 123\n".repeat(4096);
        let identity = encode_metrics(text.clone(), ContentEncoding::Identity);
        assert_eq!(identity.as_bytes(), text.as_bytes());
        assert!(!identity.header().contains("Content-Encoding:"));
        assert!(identity.header().contains("Vary: Accept-Encoding\r\n"));
        let gzip = encode_metrics(text.clone(), ContentEncoding::Gzip);
        assert!(gzip.header().contains("Content-Encoding: gzip\r\n"));
        assert!(gzip.header().contains(&format!("Content-Length: {}\r\n", gzip.as_bytes().len())));
        assert!(gzip.as_bytes().len() < text.len());
        let mut decoded = String::new();
        GzDecoder::new(gzip.as_bytes()).read_to_string(&mut decoded)?;
        assert_eq!(decoded, text);
        Ok(())
    }

    /// An oversized compression input still returns the entire identity registry.
    #[test]
    fn oversized_gzip_input_falls_back_to_complete_identity() {
        let text = "x".repeat(MAX_GZIP_BYTES + 1);
        let response = encode_metrics(text, ContentEncoding::Gzip);
        assert_eq!(response.as_bytes().len(), MAX_GZIP_BYTES + 1);
        assert!(response.as_bytes().iter().all(|byte| *byte == b'x'));
        assert!(!response.header().contains("Content-Encoding:"));
    }

    /// The gzip writer refuses bytes beyond its budget without changing the existing output.
    #[test]
    fn gzip_output_writer_enforces_byte_limit() {
        let mut writer = BoundedGzipWriter { bytes: vec![b'x'; MAX_GZIP_BYTES] };
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.bytes.len(), MAX_GZIP_BYTES);
        assert_eq!(writer.bytes.last(), Some(&b'x'));
    }

    /// Scrape one complete response with the same client deadline as the endpoint regression.
    async fn scrape_response(addr: SocketAddr, request: &[u8]) -> eyre::Result<Vec<u8>> {
        time::timeout(Duration::from_secs(5), async {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(request).await?;
            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<_, io::Error>(response)
        })
        .await
        .map_err(eyre::Report::from)
        .and_then(|response| response.map_err(Into::into))
    }

    /// End-to-end scrape test. This is the ONLY test suite allowed to install the global
    /// recorder - instrumented crates must use `metrics::with_local_recorder` instead.
    #[tokio::test]
    async fn test_metrics_endpoint_serves_tn_and_reth_metrics() -> eyre::Result<()> {
        let task_manager = TaskManager::default();
        let task_spawner = task_manager.get_spawner();

        // port 0: let the OS assign
        let addr = start_metrics_server(
            "127.0.0.1:0".parse()?,
            &task_spawner,
            "test-version",
            MetricsHooks::default().with_hook(|| {
                metrics::counter!("tn_test_scrapes").increment(1);
            }),
        )
        .await?;

        // record one tn metric and one fake reth-internal metric
        metrics::counter!("tn_test_counter").increment(1);
        metrics::counter!("db.fake").increment(1);

        let response_str = tokio::time::timeout(Duration::from_secs(5), async move {
            let mut stream = TcpStream::connect(addr).await?;
            stream.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await?;

            let mut response = Vec::new();
            stream.read_to_end(&mut response).await?;
            Ok::<_, eyre::Error>(String::from_utf8_lossy(&response).to_string())
        })
        .await??;

        assert!(response_str.starts_with("HTTP/1.1 200 OK"), "{response_str}");
        assert!(response_str.contains("Content-Type: text/plain; version=0.0.4"), "{response_str}");
        // tn metrics pass through untouched
        assert!(response_str.contains("tn_test_counter 1"), "{response_str}");
        // non-tn metrics render with the reth prefix
        assert!(response_str.contains("reth_db_fake 1"), "{response_str}");
        // the version info series is registered by the server
        assert!(response_str.contains("tn_info{version=\"test-version\"} 1"), "{response_str}");
        // the default hook samples process metrics on scrape
        assert!(response_str.contains("reth_process_cpu_seconds_total"), "{response_str}");
        assert!(response_str.contains("tn_test_scrapes 1"), "{response_str}");
        assert!(!response_str.contains("Content-Encoding:"), "{response_str}");

        // A second render must sample hooks and include all metrics in a decodable gzip stream.
        let request = format!(
            "GET /metrics HTTP/1.1\r\nX-Padding: {}\r\naCcEpT-EnCoDiNg: GZIP\r\n\r\n",
            "x".repeat(2048),
        );
        let response = scrape_response(addr, request.as_bytes()).await?;
        let header_end = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .map(|offset| offset + 4)
            .ok_or_else(|| eyre::eyre!("missing response headers"))?;
        let headers = std::str::from_utf8(
            response
                .get(..header_end)
                .ok_or_else(|| eyre::eyre!("invalid response header range"))?,
        )?;
        let body =
            response.get(header_end..).ok_or_else(|| eyre::eyre!("invalid response body range"))?;
        assert!(headers.contains("Content-Encoding: gzip\r\n"), "{headers}");
        assert!(headers.contains("Vary: Accept-Encoding\r\n"), "{headers}");
        let content_length = headers
            .lines()
            .find_map(|line| line.strip_prefix("Content-Length: "))
            .and_then(|length| length.parse::<usize>().ok());
        assert_eq!(content_length, Some(body.len()));
        let mut decoded = String::new();
        GzDecoder::new(body).read_to_string(&mut decoded)?;
        assert!(decoded.contains("tn_test_counter 1"), "{decoded}");
        assert!(decoded.contains("reth_db_fake 1"), "{decoded}");
        assert!(decoded.contains("tn_info{version=\"test-version\"} 1"), "{decoded}");
        assert!(decoded.contains("reth_process_cpu_seconds_total"), "{decoded}");
        assert!(decoded.contains("tn_test_scrapes 2"), "{decoded}");

        // Explicit refusal in a repeated field overrides wildcard and still runs fresh hooks.
        let refused = scrape_response(
            addr,
            b"GET /metrics HTTP/1.1\r\nAccept-Encoding: *\r\nAccept-Encoding: gzip;q=0\r\n\r\n",
        )
        .await?;
        let refused = String::from_utf8(refused)?;
        assert!(!refused.contains("Content-Encoding:"), "{refused}");
        assert!(refused.contains("tn_test_scrapes 3"), "{refused}");
        assert!(refused.contains("tn_test_counter 1"), "{refused}");

        Ok(())
    }
}
