//! Command-line interface and resolved runtime settings.

use std::{
    fs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};

use clap::Parser;
use eyre::WrapErr as _;
use reqwest::{Certificate, Identity};
use tracing::warn;
use url::Url;

use crate::{
    config::{GatewayConfig, UpstreamWorker},
    identity::TrustedProxies,
    proxy::{QueryHeader, UpstreamOrigin},
    ratelimit::{PrefixLen, PrefixPolicy, RateLimit},
};

/// Stateless reverse proxy in front of Telcoin Network worker JSON-RPC.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub(crate) struct Cli {
    /// Address the gateway listens on for client JSON-RPC, and for its own
    /// `/health` (liveness) and `/ready` (readiness) endpoints.
    #[arg(long, env = "WORKER_GATEWAY_LISTEN_ADDR", default_value = "0.0.0.0:8545")]
    pub(crate) listen_addr: SocketAddr,

    /// Address of a second listener that serves only the probe endpoints
    /// (`/health`, `/ready` and `/ready/any`), outside the client connection
    /// cap and the rate limits, so a flood that holds every client connection
    /// cannot make an orchestrator's probes time out. The probes stay on
    /// `--listen-addr` too. Unset (the default) opens no probe listener. The
    /// probe listener has no connection cap, so bind it where only the
    /// orchestrator reaches it.
    #[arg(long, env = "WORKER_GATEWAY_PROBE_ADDR")]
    pub(crate) probe_addr: Option<SocketAddr>,

    /// Path to a YAML file listing the upstream workers. Mutually exclusive
    /// with the inline `--upstream-*` flags; provide one source or the other.
    #[arg(
        long,
        env = "WORKER_GATEWAY_CONFIG",
        conflicts_with_all = ["upstream_rpc_url", "upstream_readiness_url", "worker_id"]
    )]
    pub(crate) config: Option<PathBuf>,

    /// Inline single upstream: the worker JSON-RPC base URL, `http` or `https`
    /// (e.g. `http://127.0.0.1:8545`).
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_RPC_URL")]
    pub(crate) upstream_rpc_url: Option<Url>,

    /// Inline single upstream: the node readiness URL, `http` or `https`
    /// (e.g. `http://127.0.0.1:8551/health/workers`).
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_READINESS_URL")]
    pub(crate) upstream_readiness_url: Option<Url>,

    /// Inline single upstream: the worker id reported by the readiness endpoint.
    #[arg(long, env = "WORKER_GATEWAY_WORKER_ID", default_value_t = 0)]
    pub(crate) worker_id: u16,

    /// JSON-RPC endpoint (`http` or `https`) that serves every call except
    /// transaction submissions and the few reads only the worker's pool can
    /// answer, typically a public RPC. When set, only `eth_sendRawTransaction`
    /// and `eth_sendRawTransactionSync`, alone or in a batch made only of them,
    /// reach the worker, plus a single `eth_getTransactionCount` at `pending`
    /// and a single lookup of a submission forwarded recently (see
    /// `--recent-submission-ttl`); everything else, including a batch that
    /// mixes submissions with other calls, goes here, with no readiness gate
    /// and no fallback to the worker. Must not point at the gateway itself or
    /// at a worker's RPC host and port.
    #[arg(long, env = "WORKER_GATEWAY_REDIRECT_QUERIES")]
    pub(crate) redirect_queries: Option<Url>,

    /// Header sent on every request to the `--redirect-queries` endpoint and
    /// never to a worker, written as one `Name: value` line: for example a
    /// public RPC's API key, so it can meter this gateway by key rather than by
    /// the one address all redirected reads share. Every client's redirected
    /// call carries it, so use a key scoped to public reads, and quote the
    /// whole line on a command line. Requires `--redirect-queries`; set this or
    /// `--redirect-queries-header-file`, not both. The value is never logged.
    /// Default: none.
    #[arg(long, env = "WORKER_GATEWAY_REDIRECT_QUERIES_HEADER", hide_env_values = true)]
    pub(crate) redirect_queries_header: Option<String>,

    /// Path to a file holding the `--redirect-queries-header` line, which keeps
    /// the secret out of the process arguments and environment (a mounted
    /// secret, for example). Whitespace around the line, a trailing newline
    /// included, is trimmed. Requires `--redirect-queries`; set this or
    /// `--redirect-queries-header`, not both. Default: none.
    #[arg(long, env = "WORKER_GATEWAY_REDIRECT_QUERIES_HEADER_FILE")]
    pub(crate) redirect_queries_header_file: Option<PathBuf>,

    /// How long, with `--redirect-queries` set, the gateway remembers the hash
    /// of an `eth_sendRawTransaction` that a worker accepted: until then,
    /// `eth_getTransactionByHash` and `eth_getTransactionReceipt` for that hash
    /// go to the worker, whose pool holds the transaction, instead of the
    /// `--redirect-queries` endpoint, which has not seen it. At most 10,000
    /// hashes are remembered at once. Default 60 s; `0` disables the hash
    /// routing (a nonce read at `pending` goes to the worker either way).
    /// Ignored without `--redirect-queries`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RECENT_SUBMISSION_TTL",
        default_value = "60s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) recent_submission_ttl: Duration,

    /// How often to poll each upstream's readiness endpoint, and to probe the
    /// `--redirect-queries` endpoint for `/ready/any`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_READINESS_POLL_INTERVAL",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) readiness_poll_interval: Duration,

    /// Per-poll timeout for the readiness endpoint and the `--redirect-queries`
    /// probe (a poll that overruns it counts as failed; see
    /// `--readiness-failure-threshold`).
    #[arg(
        long,
        env = "WORKER_GATEWAY_READINESS_POLL_TIMEOUT",
        default_value = "2s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) readiness_poll_timeout: Duration,

    /// Consecutive failed readiness polls that turn a ready upstream not-ready
    /// (default 3), so one slow or failed poll alone leaves readiness
    /// unchanged. Must be at least 1; `0` is rejected at startup.
    #[arg(long, env = "WORKER_GATEWAY_READINESS_FAILURE_THRESHOLD", default_value = "3")]
    pub(crate) readiness_failure_threshold: NonZeroU32,

    /// Consecutive successful readiness polls that turn a not-ready upstream
    /// ready, at startup too (default 2). Must be at least 1; `0` is rejected
    /// at startup.
    #[arg(long, env = "WORKER_GATEWAY_READINESS_SUCCESS_THRESHOLD", default_value = "2")]
    pub(crate) readiness_success_threshold: NonZeroU32,

    /// Consecutive forwards to an upstream worker that fail to connect (a
    /// connect timeout included) before that upstream is marked not-ready
    /// (default 3). A timeout after the request was sent does not count, since
    /// the method and params decide how long the worker takes. The upstream
    /// stays not-ready until the readiness poller sees
    /// `--readiness-success-threshold` successful polls in a row, and later
    /// requests go to the next ready upstream; the failing requests are
    /// answered with their error, never retried. A forward that gets any
    /// response resets the count. `0` disables this passive health check.
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_FAILURE_THRESHOLD", default_value_t = 3)]
    pub(crate) upstream_failure_threshold: u32,

    /// Connect timeout when forwarding a request to an upstream: a worker, or the
    /// `--redirect-queries` endpoint.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT",
        default_value = "2s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_connect_timeout: Duration,

    /// Overall per-request deadline when forwarding to an upstream: a worker,
    /// or the `--redirect-queries` endpoint when `--query-request-timeout` is
    /// `0`. The default outlasts the 30 s reth gives
    /// `eth_sendRawTransactionSync` to wait for its receipt
    /// (`--rpc.send-raw-transaction-sync-timeout`), so a slow Sync call gets
    /// the worker's own answer rather than a gateway timeout.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT",
        default_value = "35s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_request_timeout: Duration,

    /// Overall per-request deadline when forwarding to the `--redirect-queries`
    /// endpoint (default 10s; `0` disables it, and the query route then uses
    /// `--upstream-request-timeout`). A stalled query upstream frees each read's
    /// slot this soon, while submissions keep the longer worker deadline. With
    /// `--redirect-queries` set it must not exceed `--upstream-request-timeout`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_QUERY_REQUEST_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) query_request_timeout: Duration,
    /// PEM file of extra CA certificates to trust for `https` upstreams, on
    /// top of the platform's native root store. Every certificate in the file
    /// is trusted; the file is read once at startup. Applies to the worker RPC
    /// and readiness URLs and to the `--redirect-queries` endpoint. Unset (the
    /// default) trusts the native root store only.
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_CA_CERT")]
    pub(crate) upstream_ca_cert: Option<PathBuf>,

    /// PEM file holding the client certificate, followed by any intermediates,
    /// that the gateway presents to an `https` upstream asking for one (mutual
    /// TLS). Requires `--upstream-client-key`; read once at startup. Applies to
    /// the worker RPC and readiness URLs and to a `--redirect-queries` endpoint
    /// that asks for a client certificate. Unset (the default) presents none.
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_CLIENT_CERT")]
    pub(crate) upstream_client_cert: Option<PathBuf>,

    /// PEM file holding the private key (PKCS#8, PKCS#1 or SEC1, unencrypted)
    /// for `--upstream-client-cert`. Requires `--upstream-client-cert`; read
    /// once at startup. Unset (the default) presents no client certificate.
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_CLIENT_KEY")]
    pub(crate) upstream_client_key: Option<PathBuf>,

    /// How long a resolved upstream host name is cached (default `30s`). An
    /// expired answer is still served while one background lookup refreshes it,
    /// and while refreshes fail, for up to a day past its expiry. `0` disables
    /// the cache: every new upstream connection looks its host up again.
    #[arg(
        long,
        env = "WORKER_GATEWAY_DNS_CACHE_TTL",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) dns_cache_ttl: Duration,

    /// Most DNS lookups for upstream hosts that may run at once (default `8`).
    /// Only one lookup per host runs at a time; a lookup beyond the cap fails at
    /// once instead of waiting, so a request to a host with no cached answer is
    /// answered as an unreachable upstream (a refusal the gateway caused
    /// itself, so it does not count toward `--upstream-failure-threshold`). The
    /// forwarding client and the readiness poller each get their own cache and
    /// their own cap of this size. Must be at least `1`.
    #[arg(long, env = "WORKER_GATEWAY_MAX_CONCURRENT_DNS_LOOKUPS", default_value = "8")]
    pub(crate) max_concurrent_dns_lookups: NonZeroUsize,

    /// How long a new connection may take to send its complete request headers
    /// before it is disconnected (slow-loris guard). With `--proxy-protocol`, a
    /// connection from a trusted proxy first gets this long for its PROXY
    /// header, so its request headers may complete up to twice this long after
    /// accept.
    #[arg(
        long,
        env = "WORKER_GATEWAY_HEADER_READ_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) header_read_timeout: Duration,

    /// Maximum concurrently-open inbound connections; further connections wait
    /// in the OS accept backlog until a slot frees up.
    #[arg(long, env = "WORKER_GATEWAY_MAX_CONNECTIONS", default_value = "500")]
    pub(crate) max_connections: NonZeroUsize,

    /// Maximum concurrently-open inbound connections from one client (default
    /// 32; `0` disables the cap). A client is its network prefix, masked by
    /// `--rate-limit-per-ip-v4-prefix` / `--rate-limit-per-ip-v6-prefix` exactly
    /// as the per-IP rate limiter masks it, even when per-IP rate limiting is
    /// off. The address masked is the peer's, or with `--proxy-protocol` the
    /// source a trusted front's PROXY header names. A connection from an
    /// address in `--trusted-proxies` is not counted: its requests are
    /// attributed to clients by `X-Forwarded-For` one at a time, and counting
    /// it would put every client behind that front under one cap. A connection
    /// over the cap is closed at once, without a response, so one client
    /// cannot hold every `--max-connections` slot.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_CONNECTIONS_PER_IP",
        default_value_t = crate::server::DEFAULT_MAX_CONNECTIONS_PER_IP
    )]
    pub(crate) max_connections_per_ip: usize,

    /// Maximum in-flight requests made only of transaction submissions
    /// (default 256; `0` means unlimited). A submission over the cap is
    /// answered at once with a `503` overload error instead of waiting. The
    /// class is the same with or without `--redirect-queries`.
    #[arg(long, env = "WORKER_GATEWAY_MAX_INFLIGHT_SUBMISSIONS", default_value_t = 256)]
    pub(crate) max_inflight_submissions: usize,

    /// Maximum in-flight requests of every other kind, mixed batches included
    /// (default 256, or the ceiling below without `--redirect-queries`; `0`
    /// means unlimited). A request over the cap is answered at once with a
    /// `503` overload error instead of waiting, so a stalled query upstream
    /// cannot take the connection slots submissions need; keep it below
    /// `--max-connections` (the gateway warns at startup otherwise).
    /// Without `--redirect-queries` every read is forwarded to the worker, so
    /// the cap is lowered at startup to at most `--max-upstream-inflight` less
    /// a fifth of it (at least one slot is held back, but the cap stays at
    /// least 1); `0` counts as above that ceiling, and lowering a value set
    /// here logs a warning. The slots held back stay free for submissions.
    #[arg(long, env = "WORKER_GATEWAY_MAX_INFLIGHT_QUERIES")]
    pub(crate) max_inflight_queries: Option<usize>,

    /// Maximum concurrent requests this gateway forwards to the worker
    /// (default 100; `0` means unlimited). A request for the worker over the
    /// cap is answered at once with a `503` overload error instead of waiting.
    /// Every gateway in front of a worker shares its `--rpc.max-connections`
    /// (500 by default), so keep the number of gateways times this cap below
    /// it. Without `--redirect-queries`, reads may hold at most this cap less a
    /// fifth of it (at least one slot), so the rest stays free for submissions
    /// (see `--max-inflight-queries`).
    #[arg(long, env = "WORKER_GATEWAY_MAX_UPSTREAM_INFLIGHT", default_value_t = 100)]
    pub(crate) max_upstream_inflight: usize,

    /// Transport-stall deadline for inbound connections (`TCP_USER_TIMEOUT`):
    /// a connection whose peer leaves written response data unacknowledged, or
    /// its receive window closed, for this long is forcibly closed by the
    /// kernel. This kills a fully stalled reader at the transport layer, at a
    /// cost: the option replaces the kernel's default retransmit budget
    /// (typically ~15min via `tcp_retries2`), so a peer whose path stays
    /// black-holed past the deadline is dropped where stock TCP might have
    /// recovered. Linux-family kernels only; elsewhere only
    /// `--max-connection-duration` bounds a stalled reader. `0` disables.
    #[arg(
        long,
        env = "WORKER_GATEWAY_TCP_USER_TIMEOUT",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) tcp_user_timeout: Duration,

    /// Cap on a single inbound connection's total lifetime, keep-alive
    /// sessions included. The cap fires independent of connection progress, so
    /// it also bounds a client that trickles reads slowly enough to keep the
    /// transport-stall guard from firing. At the cap the connection takes no
    /// new requests: an idle one closes at once, and an exchange still in
    /// flight gets the whole-request deadline to finish before the connection
    /// is closed, so a response still streaming then is cut off; size the cap
    /// well above the longest legitimate transfer. Must be at least the gateway's own
    /// single-request bound (`--header-read-timeout`, twice with `--proxy-protocol`, plus the
    /// whole-request deadline `--upstream-request-timeout` +
    /// `--header-read-timeout`) so the first request on a connection can never
    /// be cut off. `0` disables.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_CONNECTION_DURATION",
        default_value = "10m",
        value_parser = humantime::parse_duration
    )]
    pub(crate) max_connection_duration: Duration,

    /// Maximum request body the gateway will accept, in bytes (default 1 MiB).
    /// Requests whose body exceeds this are rejected with a JSON-RPC "request
    /// too large" error before being forwarded, and a declared
    /// `Content-Length` over it is rejected before the body is read. The total
    /// buffered across connections is bounded by
    /// `--max-inflight-request-bytes`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_REQUEST_BYTES",
        default_value_t = crate::proxy::MAX_REQUEST_BYTES
    )]
    pub(crate) max_request_bytes: usize,

    /// Maximum number of calls in one JSON-RPC batch (default 50; `0` means
    /// unlimited). A longer batch is refused whole with a JSON-RPC "batch too
    /// long" error (`413`, `-32003`) before it is screened or forwarded, on
    /// either route. The effective cap never exceeds the smallest enabled
    /// rate-limit burst: a batch needs one token per call at once, so a longer
    /// one could never pass.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_BATCH_LEN",
        default_value_t = crate::proxy::DEFAULT_MAX_BATCH_LEN
    )]
    pub(crate) max_batch_len: usize,

    /// Budget of request-body bytes held across all in-flight requests, in
    /// bytes (default 512 MiB, at most 4294967295; `0` disables). Each request
    /// reserves its `Content-Length`, or `--max-request-bytes` when the body is
    /// chunked, before the body is read, and holds it until the request has
    /// been forwarded or rejected. A request that does not fit in what is left
    /// is answered at once with a JSON-RPC "in-flight bytes exhausted" error
    /// (`503`) instead of waiting. Must be at least `--max-request-bytes`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_INFLIGHT_REQUEST_BYTES",
        default_value_t = crate::server::MAX_INFLIGHT_REQUEST_BYTES
    )]
    pub(crate) max_inflight_request_bytes: u32,

    /// Cap on each inbound connection's read buffer, in bytes (default 64 KiB,
    /// minimum 8192, which hyper enforces). The buffer grows up to this size
    /// and stays allocated while the connection is open, so a connection
    /// holding a request body costs the body plus this much. A request's head
    /// (request line and headers) must fit in it, or the request is answered
    /// `431`; it also bounds how much of a streamed response is queued per
    /// connection.
    #[arg(
        long,
        env = "WORKER_GATEWAY_HTTP1_MAX_BUF_SIZE",
        default_value_t = crate::server::HTTP1_MAX_BUF_SIZE
    )]
    pub(crate) http1_max_buf_size: usize,

    /// Sustained per-client-IP request rate, in requests per second (`0`
    /// disables per-IP rate limiting); a batch counts one request per call.
    /// The client IP is the immediate TCP peer, or, when that peer is in
    /// `--trusted-proxies`, the client it forwarded the request for; behind a
    /// proxy that is not listed, every client shares the proxy's bucket (see
    /// the README).
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP", default_value_t = 100)]
    pub(crate) rate_limit_per_ip: u32,

    /// Burst allowance for the per-IP rate limit, in requests (`0` derives twice
    /// the sustained rate). Ignored when per-IP rate limiting is disabled.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_BURST", default_value_t = 0)]
    pub(crate) rate_limit_per_ip_burst: u32,

    /// IPv6 network prefix, in bits, that a client address is masked to before
    /// it keys its per-IP bucket and its `--max-connections-per-ip` count (the
    /// cap uses it even when per-IP rate limiting is off). The default `/64` is
    /// the smallest subnet routed to one customer, so a client rotating
    /// addresses inside its own allocation keeps spending one bucket instead of
    /// minting a fresh one per address. Widen it (up to `/128`) only to meter
    /// individual addresses.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_V6_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V6_PREFIX
    )]
    pub(crate) rate_limit_per_ip_v6_prefix: u8,

    /// IPv4 network prefix, in bits, that a client address is masked to before
    /// it keys its per-IP bucket and its `--max-connections-per-ip` count (the
    /// cap uses it even when per-IP rate limiting is off). The default `/32` is
    /// a single address: it preserves the gateway's per-address behaviour and
    /// never groups unrelated customers that share one carrier-grade NAT.
    /// Narrow it only for a deployment whose clients genuinely map to larger
    /// IPv4 allocations.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_V4_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V4_PREFIX
    )]
    pub(crate) rate_limit_per_ip_v4_prefix: u8,

    /// Sustained gateway-wide request rate across all clients, in requests per
    /// second (`0` disables the global rate limit); a batch counts one request
    /// per call.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_GLOBAL", default_value_t = 3_000)]
    pub(crate) rate_limit_global: u32,

    /// Burst allowance for the global rate limit, in requests (`0` derives twice
    /// the sustained rate). Ignored when the global rate limit is disabled.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_GLOBAL_BURST", default_value_t = 0)]
    pub(crate) rate_limit_global_burst: u32,

    /// Sustained gateway-wide rate reserved for requests made only of
    /// transaction submissions, in requests per second, with a burst of twice
    /// the rate (default `0` disables it). When set, submissions are metered by
    /// this budget and every other request by the global rate limit, so a read
    /// flood that empties the global bucket no longer refuses submissions; the
    /// per-IP limit still applies to both.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_SUBMISSIONS", default_value_t = 0)]
    pub(crate) rate_limit_submissions: u32,
    /// Comma-separated IPv4/IPv6 CIDR ranges of the proxies in front of the
    /// gateway (a bare address is a single host). A request whose TCP peer is
    /// inside one is attributed to the right-most `X-Forwarded-For` address that
    /// is not itself in a listed range, and that proxy's `X-Forwarded-Proto` is
    /// passed on; every other request is attributed to its peer, whatever
    /// headers it carries. A listed proxy's own connections are not counted by
    /// `--max-connections-per-ip`. Unset (the default) trusts no proxy. List
    /// only the proxies' own addresses: a range that also covers clients lets
    /// them choose their own rate-limit key and escape the per-client
    /// connection cap. Each listed proxy must append the address it
    /// received the connection from to `X-Forwarded-For` (or replace the header
    /// with it) and set `X-Forwarded-Proto` itself; one that passes a client's
    /// headers through lets that client choose its own rate-limit key.
    #[arg(long, env = "WORKER_GATEWAY_TRUSTED_PROXIES")]
    pub(crate) trusted_proxies: Option<String>,

    /// Expect a PROXY protocol v2 header (the binary form, for TCP over IPv4
    /// or IPv6) at the start of every connection from a peer in
    /// `--trusted-proxies`, and take the client address from it;
    /// `X-Forwarded-For` is then believed only when that address is itself in
    /// `--trusted-proxies`. A connection from a trusted peer whose header is
    /// missing, malformed or not complete within `--header-read-timeout` is
    /// closed; any other peer is served as plain HTTP. Off by default; requires
    /// `--trusted-proxies`. Turn it on only once every listed proxy sends the
    /// header on every connection: from a listed proxy that does not, a
    /// client's own first bytes are read as the header.
    #[arg(long, env = "WORKER_GATEWAY_PROXY_PROTOCOL")]
    pub(crate) proxy_protocol: bool,

    /// How long to drain in-flight requests on SIGTERM before forcing close.
    #[arg(
        long,
        env = "WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) graceful_shutdown_timeout: Duration,

    /// Address to expose Prometheus metrics on (`GET /metrics`), on a listener
    /// separate from the client JSON-RPC port. Unset (the default) disables the
    /// metrics endpoint entirely; bind it to an internal interface, not the
    /// public edge. Named `--metrics` to match the node's flag.
    #[arg(long = "metrics", env = "WORKER_GATEWAY_METRICS_ADDR")]
    pub(crate) metrics_addr: Option<SocketAddr>,

    /// Tracing filter directive (e.g. `info,worker_gateway=debug`).
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub(crate) log_filter: String,
}

/// Fully-resolved runtime settings, derived from [`Cli`].
#[derive(Debug)]
pub(crate) struct Settings {
    /// Address the gateway listens on.
    pub(crate) listen_addr: SocketAddr,
    /// Address of the probe-only listener, or `None` when the probes are
    /// served on the listen address alone.
    pub(crate) probe_addr: Option<SocketAddr>,
    /// Upstream workers, in preference order.
    pub(crate) upstreams: Vec<UpstreamWorker>,
    /// Endpoint serving every call the worker does not (`--redirect-queries`),
    /// or `None` when every call goes to the workers.
    pub(crate) query_upstream: Option<Url>,
    /// Header sent to the query upstream only (`--redirect-queries-header` or
    /// `--redirect-queries-header-file`), or `None`. Its `Debug` is redacted.
    pub(crate) query_header: Option<QueryHeader>,
    /// How long a forwarded submission's hash keeps its lookups on the worker
    /// under `--redirect-queries`, or `None` when that routing is disabled.
    pub(crate) recent_submission_ttl: Option<Duration>,
    /// Readiness poll interval.
    pub(crate) readiness_poll_interval: Duration,
    /// Readiness poll timeout.
    pub(crate) readiness_poll_timeout: Duration,
    /// Consecutive failed polls that turn a ready upstream not-ready.
    pub(crate) readiness_failure_threshold: NonZeroU32,
    /// Consecutive successful polls that turn a not-ready upstream ready.
    pub(crate) readiness_success_threshold: NonZeroU32,
    /// Consecutive failed worker forwards that mark an upstream not-ready, or
    /// `None` when passive health checking is off.
    pub(crate) upstream_failure_threshold: Option<NonZeroU32>,
    /// Upstream connect timeout.
    pub(crate) upstream_connect_timeout: Duration,
    /// Upstream per-request deadline.
    pub(crate) upstream_request_timeout: Duration,
    /// Per-request deadline on the query route, or `None` when it follows the
    /// upstream per-request deadline.
    pub(crate) query_request_timeout: Option<Duration>,
    /// Extra trust anchors for `https` upstreams (`--upstream-ca-cert`), added
    /// to the native root store; empty when the flag is unset.
    pub(crate) upstream_ca_certs: Vec<Certificate>,
    /// Client certificate and key presented to an `https` upstream that asks
    /// for one (`--upstream-client-cert`, `--upstream-client-key`), or `None`.
    pub(crate) upstream_identity: Option<Identity>,
    /// How long a resolved upstream host name is cached; zero disables the
    /// cache.
    pub(crate) dns_cache_ttl: Duration,
    /// Most upstream DNS lookups that may run at once.
    pub(crate) max_concurrent_dns_lookups: NonZeroUsize,
    /// Inbound header read deadline (slow-loris guard).
    pub(crate) header_read_timeout: Duration,
    /// Maximum concurrently-open inbound connections.
    pub(crate) max_connections: NonZeroUsize,
    /// Maximum concurrently-open inbound connections per client prefix (keyed
    /// by `rate_limit_prefix`), or `None` when the cap is disabled.
    pub(crate) max_connections_per_ip: Option<NonZeroUsize>,
    /// In-flight cap for submission requests (`0` = unlimited).
    pub(crate) max_inflight_submissions: usize,
    /// In-flight cap for every other request (`0` = unlimited), as lowered by
    /// [`effective_query_cap`].
    pub(crate) max_inflight_queries: usize,
    /// Cap on concurrent requests forwarded to the worker (`0` = unlimited).
    pub(crate) max_upstream_inflight: usize,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`) for inbound connections,
    /// or `None` when disabled.
    pub(crate) tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single inbound connection's total lifetime, or `None`
    /// when uncapped.
    pub(crate) max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub(crate) max_request_bytes: usize,
    /// Maximum calls in one JSON-RPC batch, or `None` when unlimited.
    pub(crate) max_batch_len: Option<NonZeroUsize>,
    /// Budget of request-body bytes held across all in-flight requests, or
    /// `None` when unlimited.
    pub(crate) max_inflight_request_bytes: Option<NonZeroU32>,
    /// Cap on each inbound connection's read buffer, in bytes.
    pub(crate) http1_max_buf_size: usize,
    /// Per-client-IP rate limit, or `None` when disabled.
    pub(crate) rate_limit_per_ip: Option<RateLimit>,
    /// Network prefix each client address is masked to before it keys a
    /// per-client bucket.
    pub(crate) rate_limit_prefix: PrefixPolicy,
    /// Gateway-wide rate limit, or `None` when disabled.
    pub(crate) rate_limit_global: Option<RateLimit>,
    /// Gateway-wide rate budget for submissions, or `None` when disabled.
    pub(crate) rate_limit_submissions: Option<RateLimit>,
    /// Proxies whose `X-Forwarded-For` identifies the client; empty when no
    /// proxy is trusted.
    pub(crate) trusted_proxies: TrustedProxies,
    /// Whether a trusted proxy's connection opens with a PROXY protocol v2
    /// header naming the client.
    pub(crate) proxy_protocol: bool,
    /// Graceful-shutdown drain deadline.
    pub(crate) graceful_shutdown_timeout: Duration,
    /// Address to expose the Prometheus scrape endpoint on, or `None` when
    /// metrics are disabled.
    pub(crate) metrics_addr: Option<SocketAddr>,
}

impl Cli {
    /// Resolve the CLI into [`Settings`], loading the YAML upstream list or
    /// building a single inline upstream from the `--upstream-*` flags.
    pub(crate) fn into_settings(self) -> eyre::Result<Settings> {
        let upstreams = self.resolve_upstreams()?;
        eyre::ensure!(!upstreams.is_empty(), "no upstream workers configured");
        upstreams.iter().try_for_each(|upstream| {
            ensure_http_or_https(&upstream.rpc_url)?;
            ensure_http_or_https(&upstream.readiness_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.rpc_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.readiness_url)
        })?;
        plaintext_worker_urls(&upstreams).for_each(|(worker_id, url)| {
            warn!(
                target: "gateway",
                worker_id,
                upstream = %UpstreamOrigin(url),
                "worker upstream uses plain http to a host that is not a loopback or private \
                 address; submissions, reads and readiness cross the network unencrypted"
            );
        });
        self.probe_addr
            .map_or(Ok(()), |probe_addr| ensure_probe_addr(self.listen_addr, probe_addr))?;
        let query_header = self.resolve_query_header()?;
        let query_upstream = self
            .redirect_queries
            .map(|url| ensure_query_upstream(self.listen_addr, &url, &upstreams).map(|()| url))
            .transpose()?;
        let query_request_timeout = resolve_optional_duration(self.query_request_timeout);
        // the query deadline exists to shorten the query route; one longer than
        // the worker's would not, and could outlast the whole-request deadline
        // built from `--upstream-request-timeout`. without a redirect the flag
        // is unused, so it is not checked.
        if let Some(query_timeout) = query_request_timeout.filter(|_| query_upstream.is_some()) {
            eyre::ensure!(
                query_timeout <= self.upstream_request_timeout,
                "--query-request-timeout ({}) exceeds --upstream-request-timeout ({}); lower it, \
                 or set it to 0 so the query route uses --upstream-request-timeout",
                humantime::format_duration(query_timeout),
                humantime::format_duration(self.upstream_request_timeout),
            );
        }
        let upstream_ca_certs =
            self.upstream_ca_cert.as_deref().map(load_ca_certs).transpose()?.unwrap_or_default();
        let upstream_identity = load_identity(
            self.upstream_client_cert.as_deref(),
            self.upstream_client_key.as_deref(),
        )?;
        let max_connection_duration = resolve_optional_duration(self.max_connection_duration);
        // The longest a single request stays live from the gateway's own point
        // of view: up to `header_read_timeout` reading the head before the
        // service's whole-request deadline (see [`crate::app`]) is even armed,
        // plus that deadline itself. With `--proxy-protocol` a trusted front's
        // connection may first spend up to another `header_read_timeout` on its
        // PROXY header. A connection cap below this bound could cut off a
        // request the gateway still considers live, so reject the combination
        // at startup.
        let proxy_header_bound =
            if self.proxy_protocol { self.header_read_timeout } else { Duration::ZERO };
        let single_request_bound = self
            .header_read_timeout
            .saturating_add(self.upstream_request_timeout.saturating_add(self.header_read_timeout))
            .saturating_add(proxy_header_bound);
        max_connection_duration.map_or(Ok(()), |cap| {
            eyre::ensure!(
                cap >= single_request_bound,
                "--max-connection-duration ({}) is shorter than the gateway's own \
                 single-request bound (--header-read-timeout, twice with --proxy-protocol, plus \
                 the whole-request deadline --upstream-request-timeout + --header-read-timeout \
                 = {}); raise the cap or set it to 0 to disable it",
                humantime::format_duration(cap),
                humantime::format_duration(single_request_bound),
            );
            Ok(())
        })?;
        let requested_queries = self.max_inflight_queries.unwrap_or(DEFAULT_MAX_INFLIGHT_QUERIES);
        let max_inflight_queries = effective_query_cap(
            requested_queries,
            self.max_upstream_inflight,
            query_upstream.is_some(),
        );
        // lowering the default is the documented behaviour; lowering a value
        // the operator chose is worth a warning
        if self.max_inflight_queries.is_some() && max_inflight_queries != requested_queries {
            warn!(
                target: "gateway",
                max_inflight_queries = requested_queries,
                max_upstream_inflight = self.max_upstream_inflight,
                effective = max_inflight_queries,
                "without --redirect-queries reads share --max-upstream-inflight with submissions; \
                 lowering --max-inflight-queries so a fifth of the worker slots stays free for \
                 submissions"
            );
        }
        if query_cap_reaches_connections(max_inflight_queries, self.max_connections) {
            warn!(
                target: "gateway",
                max_inflight_queries,
                max_connections = self.max_connections.get(),
                "--max-inflight-queries is unlimited or not below --max-connections; a saturated \
                 query route can then hold every connection and starve submissions"
            );
        }
        let rate_limit_prefix = resolve_prefix_policy(
            self.rate_limit_per_ip_v4_prefix,
            self.rate_limit_per_ip_v6_prefix,
        )?;
        let trusted_proxies = self
            .trusted_proxies
            .as_deref()
            .unwrap_or_default()
            .parse::<TrustedProxies>()
            .map_err(|err| eyre::eyre!("invalid --trusted-proxies: {err}"))?;
        eyre::ensure!(
            !self.proxy_protocol || !trusted_proxies.is_empty(),
            "--proxy-protocol requires --trusted-proxies: the PROXY header is read only from a \
             trusted proxy, so with none listed it would never be read"
        );
        let rate_limit_per_ip =
            resolve_rate_limit(self.rate_limit_per_ip, self.rate_limit_per_ip_burst);
        let rate_limit_global =
            resolve_rate_limit(self.rate_limit_global, self.rate_limit_global_burst);
        // a batch of k calls needs k tokens at once and no bucket ever holds
        // more than its burst, so a batch longer than the smallest enabled
        // burst could only ever get a 429 that no retry satisfies; cap batches
        // there so such a batch gets the final 413 instead
        let smallest_burst = [rate_limit_per_ip, rate_limit_global]
            .iter()
            .flatten()
            .filter_map(|limit| NonZeroUsize::try_from(limit.burst()).ok())
            .min();
        let max_batch_len = match (NonZeroUsize::new(self.max_batch_len), smallest_burst) {
            (cap, Some(burst)) if cap.is_none_or(|cap| burst < cap) => {
                warn!(
                    target: "gateway",
                    max_batch_len = self.max_batch_len,
                    burst = burst.get(),
                    "--max-batch-len is above the smallest rate-limit burst, and a batch needs \
                     one token per call at once; capping batches at the burst"
                );
                Some(burst)
            }
            (cap, _) => cap,
        };
        let max_inflight_request_bytes =
            resolve_inflight_byte_budget(self.max_inflight_request_bytes, self.max_request_bytes)?;
        // hyper panics on a smaller read buffer, so refuse it at startup
        eyre::ensure!(
            self.http1_max_buf_size >= crate::server::HTTP1_MIN_BUF_SIZE,
            "--http1-max-buf-size ({}) is below hyper's minimum of {} bytes",
            self.http1_max_buf_size,
            crate::server::HTTP1_MIN_BUF_SIZE,
        );
        // A per-client cap at or above the global cap never refuses anything,
        // since the global permit runs out first. Warn rather than reject:
        // raising the cap that far is a legitimate way to neutralise it behind
        // a front that hides client addresses.
        let max_connections_per_ip = NonZeroUsize::new(self.max_connections_per_ip);
        if let Some(per_ip) =
            max_connections_per_ip.filter(|per_ip| *per_ip >= self.max_connections)
        {
            warn!(
                target: "gateway",
                max_connections = %self.max_connections,
                max_connections_per_ip = %per_ip,
                "--max-connections-per-ip is at least --max-connections, so one client prefix can \
                 still hold every connection slot; lower it below --max-connections, or set it to \
                 0 to disable the cap explicitly"
            );
        }
        Ok(Settings {
            listen_addr: self.listen_addr,
            probe_addr: self.probe_addr,
            upstreams,
            query_upstream,
            query_header,
            recent_submission_ttl: resolve_optional_duration(self.recent_submission_ttl),
            readiness_poll_interval: self.readiness_poll_interval,
            readiness_poll_timeout: self.readiness_poll_timeout,
            readiness_failure_threshold: self.readiness_failure_threshold,
            readiness_success_threshold: self.readiness_success_threshold,
            upstream_failure_threshold: NonZeroU32::new(self.upstream_failure_threshold),
            upstream_connect_timeout: self.upstream_connect_timeout,
            upstream_request_timeout: self.upstream_request_timeout,
            query_request_timeout,
            upstream_ca_certs,
            upstream_identity,
            dns_cache_ttl: self.dns_cache_ttl,
            max_concurrent_dns_lookups: self.max_concurrent_dns_lookups,
            header_read_timeout: self.header_read_timeout,
            max_connections: self.max_connections,
            max_connections_per_ip,
            max_inflight_submissions: self.max_inflight_submissions,
            max_inflight_queries,
            max_upstream_inflight: self.max_upstream_inflight,
            tcp_user_timeout: resolve_optional_duration(self.tcp_user_timeout),
            max_connection_duration,
            max_request_bytes: self.max_request_bytes,
            max_batch_len,
            max_inflight_request_bytes,
            http1_max_buf_size: self.http1_max_buf_size,
            rate_limit_per_ip,
            rate_limit_prefix,
            rate_limit_global,
            // no burst flag: the burst derives like a `0` burst on the others
            rate_limit_submissions: resolve_rate_limit(self.rate_limit_submissions, 0),
            trusted_proxies,
            proxy_protocol: self.proxy_protocol,
            graceful_shutdown_timeout: self.graceful_shutdown_timeout,
            metrics_addr: self.metrics_addr,
        })
    }

    /// Build the upstream list from `--config` when present, otherwise from the
    /// inline `--upstream-rpc-url` + `--upstream-readiness-url` pair.
    fn resolve_upstreams(&self) -> eyre::Result<Vec<UpstreamWorker>> {
        match &self.config {
            Some(path) => GatewayConfig::load(path).map(|config| config.upstreams),
            None => {
                let (rpc_url, readiness_url) = self
                    .upstream_rpc_url
                    .as_ref()
                    .zip(self.upstream_readiness_url.as_ref())
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "provide --config, or both --upstream-rpc-url and --upstream-readiness-url"
                        )
                    })?;
                Ok(vec![UpstreamWorker {
                    worker_id: self.worker_id,
                    rpc_url: rpc_url.clone(),
                    readiness_url: readiness_url.clone(),
                }])
            }
        }
    }

    /// Resolve `--redirect-queries-header` or `--redirect-queries-header-file`
    /// into the header sent to the query upstream.
    ///
    /// Setting both, or either without `--redirect-queries`, is a startup
    /// error. No error carries the header line, which holds a credential even
    /// when it is malformed.
    fn resolve_query_header(&self) -> eyre::Result<Option<QueryHeader>> {
        let (flag, line) = match (&self.redirect_queries_header, &self.redirect_queries_header_file)
        {
            (None, None) => return Ok(None),
            (Some(_), Some(_)) => eyre::bail!(
                "--redirect-queries-header and --redirect-queries-header-file are both set; use \
                 one"
            ),
            (Some(line), None) => ("--redirect-queries-header", Ok(line.clone())),
            (None, Some(path)) => (
                "--redirect-queries-header-file",
                fs::read_to_string(path).wrap_err_with(|| {
                    format!("failed to read --redirect-queries-header-file `{}`", path.display())
                }),
            ),
        };
        eyre::ensure!(
            self.redirect_queries.is_some(),
            "{flag} is set without --redirect-queries; the header is only ever sent to the query \
             upstream"
        );
        QueryHeader::parse(&line?).map(Some).wrap_err_with(|| format!("invalid {flag}"))
    }
}

/// Turn a duration flag into `Some(duration)`, or `None` when zero (the
/// flag's disabled sentinel, mirroring the `0`-disables rate-limit flags).
fn resolve_optional_duration(value: Duration) -> Option<Duration> {
    (!value.is_zero()).then_some(value)
}

/// `--max-inflight-queries` when it is not set (before [`effective_query_cap`]).
const DEFAULT_MAX_INFLIGHT_QUERIES: usize = 256;

/// The query cap the gateway enforces for `--max-inflight-queries`.
///
/// Without a redirect every read is a forward to the worker, so the query cap
/// is also the reads' share of `--max-upstream-inflight`. Left at or above the
/// worker cap, reads alone could hold every worker slot and every submission
/// would be refused with `503`. The cap is therefore lowered to the worker cap
/// less a fifth of it (at least one slot), never below 1 since `0` means
/// unlimited; an unlimited query cap counts as above that ceiling. With a
/// redirect, or an unlimited worker cap, the value is kept as given: with a
/// redirect the only reads a worker sees are the single nonce reads and
/// lookups kept on it (see `crate::proxy`), and those finish quickly.
fn effective_query_cap(
    max_inflight_queries: usize,
    max_upstream_inflight: usize,
    redirect: bool,
) -> usize {
    if redirect || max_upstream_inflight == 0 {
        return max_inflight_queries;
    }
    let reserve = (max_upstream_inflight / 5).max(1);
    let ceiling = max_upstream_inflight.saturating_sub(reserve).max(1);
    match max_inflight_queries {
        0 => ceiling,
        cap => cap.min(ceiling),
    }
}

/// Whether a query cap of `cap` (`0` = unlimited) lets a saturated query route
/// take every inbound connection, leaving none for submissions.
fn query_cap_reaches_connections(cap: usize, max_connections: NonZeroUsize) -> bool {
    cap == 0 || cap >= max_connections.get()
}

/// Turn a `(rate, burst)` flag pair into a [`RateLimit`], or `None` when the
/// rate is `0` (limiter disabled). A `0` burst derives twice the sustained rate
/// so a modest headroom is the default without a second flag.
fn resolve_rate_limit(rate: u32, burst: u32) -> Option<RateLimit> {
    NonZeroU32::new(rate).map(|rate| {
        let burst = if burst == 0 { rate.get().saturating_mul(2) } else { burst };
        // `burst` is now >= `rate.get()` >= 1, so it is non-zero; fall back to
        // the (non-zero) rate rather than panic if that ever fails to hold.
        RateLimit::new(rate, NonZeroU32::new(burst).unwrap_or(rate))
    })
}

/// Turn `--max-inflight-request-bytes` into a budget, or `None` when `0`
/// (unlimited). A budget below `--max-request-bytes` is a startup error: a
/// chunked request reserves the whole per-request cap, so it could never be
/// admitted, and a declared body between the two would always be refused with
/// a `503` that tells the client to retry.
fn resolve_inflight_byte_budget(
    budget: u32,
    max_request_bytes: usize,
) -> eyre::Result<Option<NonZeroU32>> {
    let Some(budget) = NonZeroU32::new(budget) else { return Ok(None) };
    eyre::ensure!(
        usize::try_from(budget.get()).is_ok_and(|budget| budget >= max_request_bytes),
        "--max-inflight-request-bytes ({budget}) is below --max-request-bytes \
         ({max_request_bytes}), so a chunked request could never be admitted; raise the budget \
         or set it to 0 to disable it"
    );
    Ok(Some(budget))
}

/// Validate the two per-IP prefix flags into a [`PrefixPolicy`]. A length wider
/// than its address family allows is a startup error rather than a silently
/// clamped value: a `/200` in a deployment's config is a typo, and quietly
/// meaning `/32` would hide it.
fn resolve_prefix_policy(v4: u8, v6: u8) -> eyre::Result<PrefixPolicy> {
    PrefixLen::v4(v4)
        .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v4-prefix: {err}"))
        .and_then(|v4| {
            PrefixLen::v6(v6)
                .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v6-prefix: {err}"))
                .map(|v6| PrefixPolicy::new(v4, v6))
        })
}

/// Reject a worker RPC or readiness URL whose scheme is neither `http` nor
/// `https` at startup, rather than at the first forward or poll. An `https`
/// URL is verified against the native root store plus any `--upstream-ca-cert`
/// certificates (see [`crate::proxy::client_builder`]). The message names the
/// URL by origin only.
fn ensure_http_or_https(url: &Url) -> eyre::Result<()> {
    eyre::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported URL scheme `{}` in worker upstream `{}`: use http or https",
        url.scheme(),
        UpstreamOrigin(url)
    );
    Ok(())
}

/// Read the `--upstream-ca-cert` PEM bundle into trust anchors.
///
/// The file is read once, at startup. An unreadable or empty file, one that
/// holds no PEM certificate, or one with a certificate rustls cannot parse is
/// a startup error rather than a client that silently trusts nothing extra.
/// Messages name the file by path only, never its content.
fn load_ca_certs(path: &Path) -> eyre::Result<Vec<Certificate>> {
    let pem = read_pem_file("--upstream-ca-cert", path)?;
    let certs = Certificate::from_pem_bundle(&pem).map_err(|_| {
        eyre::eyre!(
            "--upstream-ca-cert file `{}` is not a valid PEM certificate bundle",
            path.display()
        )
    })?;
    eyre::ensure!(
        !certs.is_empty(),
        "--upstream-ca-cert file `{}` holds no PEM certificate",
        path.display()
    );
    eyre::ensure!(
        rustls_accepts(|builder| {
            certs.iter().cloned().fold(builder, reqwest::ClientBuilder::add_root_certificate)
        }),
        "--upstream-ca-cert file `{}` holds a certificate that cannot be parsed",
        path.display()
    );
    Ok(certs)
}

/// Read the `--upstream-client-cert` and `--upstream-client-key` PEM files into
/// the identity presented to an upstream that asks for a client certificate.
///
/// Both flags or neither: one without the other is a startup error rather than
/// a gateway that silently presents nothing. Each file is read once, at
/// startup; messages name the files by path only, never their content. A key
/// that does not match the certificate, or a certificate rustls cannot parse,
/// is reported here against both paths (see [`rustls_accepts`]).
fn load_identity(cert: Option<&Path>, key: Option<&Path>) -> eyre::Result<Option<Identity>> {
    let (cert, key) = match (cert, key) {
        (None, None) => return Ok(None),
        (Some(cert), Some(key)) => (cert, key),
        (Some(_), None) => eyre::bail!("--upstream-client-cert requires --upstream-client-key"),
        (None, Some(_)) => eyre::bail!("--upstream-client-key requires --upstream-client-cert"),
    };
    // `Identity::from_pem` takes the key and the certificate chain in one buffer
    let mut pem = read_pem_file("--upstream-client-key", key)?;
    pem.push(b'\n');
    pem.extend(read_pem_file("--upstream-client-cert", cert)?);
    let identity = Identity::from_pem(&pem).map_err(|_| {
        eyre::eyre!(
            "--upstream-client-cert `{}` and --upstream-client-key `{}` do not hold a PEM \
             certificate and an unencrypted PEM private key",
            cert.display(),
            key.display()
        )
    })?;
    eyre::ensure!(
        rustls_accepts(|builder| builder.identity(identity.clone())),
        "--upstream-client-cert `{}` and --upstream-client-key `{}` do not hold a certificate \
         and the private key that matches it",
        cert.display(),
        key.display()
    );
    Ok(Some(identity))
}

/// Read the PEM file that `flag` names, rejecting an unreadable or empty file.
/// The error names the flag and the path, never the content.
fn read_pem_file(flag: &str, path: &Path) -> eyre::Result<Vec<u8>> {
    let pem = std::fs::read(path)
        .map_err(|err| eyre::eyre!("cannot read {flag} file `{}`: {err}", path.display()))?;
    eyre::ensure!(
        !pem.iter().all(u8::is_ascii_whitespace),
        "{flag} file `{}` is empty",
        path.display()
    );
    Ok(pem)
}

/// Whether rustls accepts the TLS material `configure` adds to a bare client.
///
/// In a rustls-only build reqwest decodes the PEM armour when a file is loaded,
/// but rustls parses what is inside only when a client is built, so a bad block
/// would otherwise fail the client build in [`crate::app`] with an error that
/// names no file. A throwaway client built here, without the native root store,
/// lets the caller report the file by flag and path. The rustls error is
/// dropped, as it can quote what it failed to parse.
fn rustls_accepts(
    configure: impl FnOnce(reqwest::ClientBuilder) -> reqwest::ClientBuilder,
) -> bool {
    configure(reqwest::Client::builder().use_rustls_tls().tls_built_in_root_certs(false))
        .build()
        .is_ok()
}

/// Validate the `--redirect-queries` URL: `http` or `https`, with a host and no
/// fragment, not the gateway itself, and not on any worker's RPC host and port.
///
/// The last check is an error rather than a warning because reads sent to a
/// worker land on the validator, which is the load the redirect exists to
/// remove. It compares host and port whatever the scheme, so an `https` URL on
/// a worker's `http` socket is caught too. Plain `http` to a host that is not a
/// loopback or private address literal is accepted with a warning: the reads
/// and their answers then cross the network unencrypted. Messages name the URL
/// by origin only, since a hosted RPC URL can carry an API key in its path.
fn ensure_query_upstream(
    listen_addr: SocketAddr,
    url: &Url,
    upstreams: &[UpstreamWorker],
) -> eyre::Result<()> {
    eyre::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported URL scheme `{}` in --redirect-queries: use http or https",
        url.scheme()
    );
    eyre::ensure!(url.has_host(), "--redirect-queries URL has no host");
    eyre::ensure!(
        url.fragment().is_none(),
        "--redirect-queries URL `{}` carries a fragment; remove it",
        UpstreamOrigin(url)
    );
    ensure_not_gateway(listen_addr, url)?;
    if let Some(worker) =
        upstreams.iter().find(|upstream| same_host_and_port(url, &upstream.rpc_url))
    {
        eyre::bail!(
            "--redirect-queries `{}` is worker {}'s RPC host and port, so reads would still \
             reach the validator; point it at a separate JSON-RPC endpoint",
            UpstreamOrigin(url),
            worker.worker_id
        );
    }
    if plaintext_to_public_host(url) {
        warn!(
            target: "gateway",
            upstream = %UpstreamOrigin(url),
            "--redirect-queries uses plain http to a host that is not a loopback or private \
             address; reads and their answers cross the network unencrypted"
        );
    }
    Ok(())
}

/// The worker RPC and readiness URLs that use plain `http` to a host that is
/// not a loopback or private address literal, each with its worker id.
///
/// Such a hop may cross a network the operator does not control, where an
/// on-path attacker can drop submissions behind their computable hash and
/// forge reads and readiness. Startup warns about each one, as
/// [`ensure_query_upstream`] does for the redirect, rather than refusing it:
/// the address may still be on a private network the check cannot see.
fn plaintext_worker_urls(upstreams: &[UpstreamWorker]) -> impl Iterator<Item = (u16, &Url)> + '_ {
    upstreams
        .iter()
        .flat_map(|upstream| {
            [(upstream.worker_id, &upstream.rpc_url), (upstream.worker_id, &upstream.readiness_url)]
        })
        .filter(|(_, url)| plaintext_to_public_host(url))
}

/// Whether two URLs name the same host and port, whatever their schemes.
fn same_host_and_port(a: &Url, b: &Url) -> bool {
    a.port_or_known_default() == b.port_or_known_default()
        && match (url_host_ip(a), url_host_ip(b)) {
            (Some(a), Some(b)) => a == b,
            _ => a.host() == b.host(),
        }
}

/// Whether `url` is plain `http` to anything but a loopback or private address
/// literal. A domain name (other than `localhost`) counts as public, since it
/// cannot be checked without resolving it.
fn plaintext_to_public_host(url: &Url) -> bool {
    let local = url_host_ip(url).is_some_and(|ip| match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local(),
    });
    url.scheme() == "http" && !local
}

/// Reject an upstream URL that can only point back at the gateway itself (a
/// loopback/unspecified host, or the listen interface, on the listen port):
/// forwarding to it would loop. The default listen port (`8545`) matches the
/// worker's default RPC port, so a single-host setup left on defaults hits
/// this. The runtime hop-header guard (see [`crate::proxy`]) catches the
/// loops this startup check cannot see, e.g. a VIP that fronts the gateways.
fn ensure_not_gateway(listen_addr: SocketAddr, url: &Url) -> eyre::Result<()> {
    let same_port = url.port_or_known_default() == Some(listen_addr.port());
    let listen_ip = listen_addr.ip();
    let hits_gateway = url_host_ip(url)
        .map(|ip| {
            let same_family = ip.is_ipv4() == listen_ip.is_ipv4();
            ip == listen_ip
                || (same_family && ip.is_unspecified())
                || (same_family && ip.is_loopback() && listen_ip.is_unspecified())
        })
        .unwrap_or(false);
    eyre::ensure!(
        !(same_port && hits_gateway),
        "upstream URL `{}` points at the gateway's own listen address ({listen_addr}), \
         so forwarding to it would loop; change the upstream URL or --listen-addr",
        UpstreamOrigin(url)
    );
    Ok(())
}

/// Reject a `--probe-addr` that collides with `--listen-addr`: the same
/// address, or the same port with one side on an unspecified address that
/// covers the other. An unspecified IPv6 address covers both families (tokio
/// leaves `IPV6_V6ONLY` unset, so `[::]` binds dual-stack on a default Linux
/// host); an unspecified IPv4 address covers only IPv4. Port `0` asks for an
/// ephemeral port, so it never collides. The two listeners cannot share a
/// socket, and the probes are only out of the client connection cap's reach
/// on a socket of their own.
fn ensure_probe_addr(listen_addr: SocketAddr, probe_addr: SocketAddr) -> eyre::Result<()> {
    let (listen_ip, probe_ip) = (listen_addr.ip(), probe_addr.ip());
    let covers =
        |wide: IpAddr, other: IpAddr| wide.is_unspecified() && (wide.is_ipv6() || other.is_ipv4());
    let overlaps =
        listen_ip == probe_ip || covers(listen_ip, probe_ip) || covers(probe_ip, listen_ip);
    let same_port = listen_addr.port() != 0 && listen_addr.port() == probe_addr.port();
    eyre::ensure!(
        !(same_port && overlaps),
        "--probe-addr ({probe_addr}) collides with --listen-addr ({listen_addr}); give the \
         probe listener a port of its own"
    );
    Ok(())
}

/// The upstream host as an IP when it names one (`localhost` counts; other
/// domain names cannot be checked without resolving them).
fn url_host_ip(url: &Url) -> Option<IpAddr> {
    url.host().and_then(|host| match host {
        url::Host::Domain(name) => {
            name.eq_ignore_ascii_case("localhost").then_some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        }
        url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        url::Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn cli_with(config: Option<&str>, rpc: Option<&str>, readiness: Option<&str>) -> Cli {
        let mut argv = vec!["worker-gateway".to_string()];
        if let Some(path) = config {
            argv.push(format!("--config={path}"));
        }
        if let Some(url) = rpc {
            argv.push(format!("--upstream-rpc-url={url}"));
        }
        if let Some(url) = readiness {
            argv.push(format!("--upstream-readiness-url={url}"));
        }
        Cli::parse_from(argv)
    }

    #[test]
    fn inline_upstream_resolves() -> eyre::Result<()> {
        let settings = cli_with(
            None,
            Some("http://127.0.0.1:8544"),
            Some("http://127.0.0.1:8551/health/workers"),
        )
        .into_settings()?;
        assert_eq!(settings.upstreams.len(), 1);
        Ok(())
    }

    #[test]
    fn zero_disables_write_path_guards() -> eyre::Result<()> {
        let settings = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--tcp-user-timeout=0s",
            "--max-connection-duration=0",
        ])
        .into_settings()?;
        assert_eq!(settings.tcp_user_timeout, None);
        assert_eq!(settings.max_connection_duration, None);
        Ok(())
    }

    #[test]
    fn write_path_guards_default_on() -> eyre::Result<()> {
        let settings = cli_with(
            None,
            Some("http://127.0.0.1:8544"),
            Some("http://127.0.0.1:8551/health/workers"),
        )
        .into_settings()?;
        assert_eq!(settings.tcp_user_timeout, Some(Duration::from_secs(30)));
        assert_eq!(settings.max_connection_duration, Some(Duration::from_secs(600)));
        Ok(())
    }

    #[test]
    fn connection_cap_below_request_deadline_is_rejected() {
        // Default single-request bound is 10s (header phase) + 35s + 10s
        // (whole-request deadline) = 55s; a cap of the bare whole-request
        // deadline (45s) could still cut off a request whose headers took the
        // full header window to arrive.
        let result = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=45s",
        ])
        .into_settings();
        assert!(result.is_err(), "a cap below the single-request bound must be a startup error");

        // The boundary itself is accepted.
        let boundary = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=55s",
        ])
        .into_settings();
        assert!(boundary.is_ok(), "a cap equal to the single-request bound must be accepted");
    }

    /// WG-32: reth gives `eth_sendRawTransactionSync` 30 s to wait for its
    /// receipt by default (`--rpc.send-raw-transaction-sync-timeout`). The
    /// gateway's default upstream deadline must outlast it, or a slow Sync
    /// call races the gateway's own `504`.
    #[test]
    fn default_upstream_timeout_exceeds_reths_sync_deadline() -> eyre::Result<()> {
        let reth_sync_deadline = Duration::from_secs(30);
        let timeout = cli_with_flags(&[]).into_settings()?.upstream_request_timeout;
        assert!(timeout > reth_sync_deadline, "{timeout:?}");
        assert!(timeout >= Duration::from_secs(35), "{timeout:?}");
        Ok(())
    }

    #[test]
    fn thresholds_of_zero_are_rejected_at_startup() {
        for flag in ["--readiness-failure-threshold=0", "--readiness-success-threshold=0"] {
            let result = Cli::try_parse_from([
                "worker-gateway",
                "--upstream-rpc-url=http://10.0.0.7:8545",
                "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
                flag,
            ]);
            assert!(result.is_err(), "{flag} must fail startup");
        }

        // the defaults are 3 failures and 2 successes, and any positive value is accepted
        let defaults = cli_with_flags(&[]).into_settings().expect("defaults");
        assert_eq!(defaults.readiness_failure_threshold.get(), 3);
        assert_eq!(defaults.readiness_success_threshold.get(), 2);
        let ones =
            cli_with_flags(&["--readiness-failure-threshold=1", "--readiness-success-threshold=1"])
                .into_settings()
                .expect("thresholds of 1");
        assert_eq!(ones.readiness_failure_threshold.get(), 1);
        assert_eq!(ones.readiness_success_threshold.get(), 1);
    }

    #[test]
    fn zero_upstream_failure_threshold_disables_passive_health() -> eyre::Result<()> {
        let defaults = cli_with_flags(&[]).into_settings()?;
        assert_eq!(defaults.upstream_failure_threshold.map(NonZeroU32::get), Some(3));
        let off = cli_with_flags(&["--upstream-failure-threshold=0"]).into_settings()?;
        assert_eq!(off.upstream_failure_threshold, None);
        Ok(())
    }

    #[test]
    fn worker_id_conflicts_with_config() {
        let result =
            Cli::try_parse_from(["worker-gateway", "--config=gateway.yaml", "--worker-id=3"]);
        assert!(result.is_err(), "--worker-id alongside --config must error, not be ignored");
    }

    #[test]
    fn self_pointing_upstream_is_rejected() {
        // Default listen address is 0.0.0.0:8545; a loopback upstream on the
        // same port is the gateway itself, and forwarding to it would loop.
        let result = cli_with(
            None,
            Some("http://127.0.0.1:8545"),
            Some("http://127.0.0.1:8551/health/workers"),
        )
        .into_settings();
        assert!(result.is_err());
    }

    #[test]
    fn same_port_on_another_host_is_accepted() -> eyre::Result<()> {
        // Port reuse across hosts is the normal deployment shape.
        let settings = cli_with(
            None,
            Some("http://10.0.0.7:8545"),
            Some("http://10.0.0.7:8551/health/workers"),
        )
        .into_settings()?;
        assert_eq!(settings.upstreams.len(), 1);
        Ok(())
    }

    #[test]
    fn inline_requires_both_urls() {
        let result = cli_with(None, Some("http://127.0.0.1:8545"), None).into_settings();
        assert!(result.is_err());
    }

    #[test]
    fn https_upstream_is_accepted() -> eyre::Result<()> {
        let settings = cli_with(
            None,
            Some("https://10.0.0.7:8545"),
            Some("https://10.0.0.7:8551/health/workers"),
        )
        .into_settings()?;
        assert_eq!(settings.upstreams.len(), 1);
        assert!(settings.upstream_ca_certs.is_empty(), "no extra trust anchors by default");
        Ok(())
    }

    #[test]
    fn non_http_worker_schemes_are_rejected() {
        for (rpc, readiness) in [
            ("ws://10.0.0.7:8545", "http://10.0.0.7:8551/health/workers"),
            ("http://10.0.0.7:8545", "ftp://10.0.0.7:8551/health/workers"),
        ] {
            let result = cli_with(None, Some(rpc), Some(readiness)).into_settings();
            assert!(result.is_err(), "{rpc} / {readiness} must be rejected");
        }
    }

    #[test]
    fn unreadable_ca_file_error_names_the_path_only() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let missing = dir.path().join("missing-ca.pem");
        let empty = dir.path().join("empty-ca.pem");
        std::fs::write(&empty, " \n")?;
        let no_pem = dir.path().join("no-pem.pem");
        std::fs::write(&no_pem, "s3cr3t plain text without a pem block\n")?;
        let key_only = dir.path().join("key-only.pem");
        std::fs::write(
            &key_only,
            "s3cr3t note\n-----BEGIN PRIVATE KEY-----\nczNjcjN0\n-----END PRIVATE KEY-----\n",
        )?;
        let garbled = dir.path().join("garbled-ca.pem");
        std::fs::write(
            &garbled,
            "-----BEGIN CERTIFICATE-----\ns3cr3t!!not base64\n-----END CERTIFICATE-----\n",
        )?;
        // valid base64 ("s3cr3t not a certificate") in a certificate block,
        // which only rustls rejects
        let not_x509 = dir.path().join("not-x509-ca.pem");
        std::fs::write(
            &not_x509,
            "-----BEGIN CERTIFICATE-----\nczNjcjN0IG5vdCBhIGNlcnRpZmljYXRl\n-----END CERTIFICATE-----\n",
        )?;
        // a directory cannot be read as a file, whoever runs the test
        for path in [missing.as_path(), dir.path(), &empty, &no_pem, &key_only, &garbled, &not_x509]
        {
            let flag = format!("--upstream-ca-cert={}", path.display());
            let message = match cli_with_flags(&[flag.as_str()]).into_settings() {
                Ok(_) => panic!("{} must be rejected", path.display()),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("--upstream-ca-cert"), "flag missing from: {message}");
            assert!(message.contains(&path.display().to_string()), "path missing from: {message}");
            assert!(!message.contains("s3cr3t"), "file content leaked into: {message}");
        }
        Ok(())
    }

    #[test]
    fn cert_without_key_is_rejected_at_startup() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["gateway.test".to_string()])?;
        let cert_path = dir.path().join("client.pem");
        std::fs::write(&cert_path, cert.pem())?;
        let key_path = dir.path().join("client-key.pem");
        std::fs::write(&key_path, key_pair.serialize_pem())?;
        let cert_flag = format!("--upstream-client-cert={}", cert_path.display());
        let key_flag = format!("--upstream-client-key={}", key_path.display());

        for (flags, missing) in [
            ([cert_flag.as_str()], "--upstream-client-key"),
            ([key_flag.as_str()], "--upstream-client-cert"),
        ] {
            let message = match cli_with_flags(&flags).into_settings() {
                Ok(_) => panic!("{flags:?} alone must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(missing), "`{missing}` missing from: {message}");
        }

        let settings = cli_with_flags(&[cert_flag.as_str(), key_flag.as_str()]).into_settings()?;
        assert!(settings.upstream_identity.is_some(), "both files make an identity");
        assert!(cli_with_flags(&[]).into_settings()?.upstream_identity.is_none());
        Ok(())
    }

    #[test]
    fn client_identity_errors_name_the_paths_only() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["gateway.test".to_string()])?;
        let cert_path = dir.path().join("client.pem");
        std::fs::write(&cert_path, cert.pem())?;
        // a key file holding no key: the pair cannot make an identity
        let key_path = dir.path().join("not-a-key.pem");
        std::fs::write(&key_path, "s3cr3t text without a pem block\n")?;
        let missing = dir.path().join("missing-key.pem");
        // a key that parses but belongs to another certificate, which only
        // rustls rejects
        let other_key_path = dir.path().join("other-key.pem");
        let other = rcgen::generate_simple_self_signed(vec!["other.test".to_string()])?;
        std::fs::write(&other_key_path, other.key_pair.serialize_pem())?;
        // valid base64 ("s3cr3t not a certificate") in a certificate block
        // next to a real key, which only rustls rejects
        let not_x509_path = dir.path().join("not-x509-client.pem");
        std::fs::write(
            &not_x509_path,
            "-----BEGIN CERTIFICATE-----\nczNjcjN0IG5vdCBhIGNlcnRpZmljYXRl\n-----END CERTIFICATE-----\n",
        )?;
        let own_key_path = dir.path().join("client-key.pem");
        std::fs::write(&own_key_path, key_pair.serialize_pem())?;
        // (certificate file, key file, whether the error must name the
        // certificate file too)
        for (cert, key, names_cert) in [
            (&cert_path, &key_path, true),
            (&cert_path, &missing, false),
            (&cert_path, &other_key_path, true),
            (&not_x509_path, &own_key_path, true),
        ] {
            let message = match cli_with_flags(&[
                format!("--upstream-client-cert={}", cert.display()).as_str(),
                format!("--upstream-client-key={}", key.display()).as_str(),
            ])
            .into_settings()
            {
                Ok(_) => panic!("{} / {} must be rejected", cert.display(), key.display()),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(&key.display().to_string()), "path missing from: {message}");
            if names_cert {
                let cert = cert.display().to_string();
                assert!(message.contains(&cert), "path missing from: {message}");
            }
            assert!(!message.contains("s3cr3t"), "file content leaked into: {message}");
            assert!(!message.contains("BEGIN"), "pem content leaked into: {message}");
        }
        Ok(())
    }

    #[test]
    fn ca_file_certificates_are_loaded() -> eyre::Result<()> {
        let dir = tempfile::tempdir()?;
        let bundle = dir.path().join("ca.pem");
        let one = rcgen::generate_simple_self_signed(vec!["one.test".to_string()])?.cert.pem();
        let two = rcgen::generate_simple_self_signed(vec!["two.test".to_string()])?.cert.pem();
        std::fs::write(&bundle, format!("{one}{two}"))?;
        let flag = format!("--upstream-ca-cert={}", bundle.display());
        let settings = cli_with_flags(&[flag.as_str()]).into_settings()?;
        assert_eq!(settings.upstream_ca_certs.len(), 2, "every certificate in the file is trusted");
        Ok(())
    }

    /// An inline-upstream CLI on another host (so the self-pointing guard does
    /// not trip) plus whatever extra flags a test needs.
    fn cli_with_flags(extra: &[&str]) -> Cli {
        let mut argv = vec![
            "worker-gateway".to_string(),
            "--upstream-rpc-url=http://10.0.0.7:8545".to_string(),
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers".to_string(),
        ];
        argv.extend(extra.iter().map(|flag| (*flag).to_string()));
        Cli::parse_from(argv)
    }

    #[test]
    fn edge_protection_defaults() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        assert_eq!(settings.max_request_bytes, 1_048_576);
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(50));
        let per_ip = settings.rate_limit_per_ip.expect("per-ip limit on by default");
        assert_eq!(per_ip.rate().get(), 100);
        // A zero burst flag derives twice the sustained rate.
        assert_eq!(per_ip.burst().get(), 200);
        let global = settings.rate_limit_global.expect("global limit on by default");
        assert_eq!(global.rate().get(), 3_000);
        assert_eq!(global.burst().get(), 6_000);
        Ok(())
    }

    #[test]
    fn per_ip_connection_cap_defaults_to_32_and_zero_disables_it() -> eyre::Result<()> {
        let default = cli_with_flags(&[]).into_settings()?;
        assert_eq!(default.max_connections_per_ip, NonZeroUsize::new(32));
        let disabled = cli_with_flags(&["--max-connections-per-ip=0"]).into_settings()?;
        assert_eq!(disabled.max_connections_per_ip, None);
        Ok(())
    }

    #[test]
    fn per_ip_connection_cap_at_the_global_cap_is_kept() -> eyre::Result<()> {
        // Warned about at startup, not rejected: raising the cap that far is
        // how a gateway behind an address-hiding front neutralises it.
        let settings = cli_with_flags(&["--max-connections=8", "--max-connections-per-ip=8"])
            .into_settings()?;
        assert_eq!(settings.max_connections_per_ip, NonZeroUsize::new(8));
        Ok(())
    }

    #[test]
    fn zero_rate_disables_the_limiter() -> eyre::Result<()> {
        let settings =
            cli_with_flags(&["--rate-limit-per-ip=0", "--rate-limit-global=0"]).into_settings()?;
        assert!(settings.rate_limit_per_ip.is_none());
        assert!(settings.rate_limit_global.is_none());
        Ok(())
    }

    #[test]
    fn submission_budget_is_off_by_default_and_derives_its_burst() -> eyre::Result<()> {
        assert!(cli_with_flags(&[]).into_settings()?.rate_limit_submissions.is_none());
        let settings = cli_with_flags(&["--rate-limit-submissions=50"]).into_settings()?;
        let submissions = settings.rate_limit_submissions.expect("submission budget on");
        assert_eq!((submissions.rate().get(), submissions.burst().get()), (50, 100));
        Ok(())
    }

    #[test]
    fn explicit_burst_is_honored() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--rate-limit-per-ip=40", "--rate-limit-per-ip-burst=50"])
            .into_settings()?;
        let per_ip = settings.rate_limit_per_ip.expect("per-ip limit on");
        assert_eq!(per_ip.rate().get(), 40);
        assert_eq!(per_ip.burst().get(), 50);
        Ok(())
    }

    #[test]
    fn per_ip_prefix_defaults_are_v4_32_and_v6_64() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        // /32 keeps IPv4 metering per address; /64 collapses an IPv6
        // allocation's rotating addresses onto one bucket.
        assert_eq!(settings.rate_limit_prefix.v4_bits(), 32);
        assert_eq!(settings.rate_limit_prefix.v6_bits(), 64);
        Ok(())
    }

    #[test]
    fn per_ip_prefixes_are_configurable() -> eyre::Result<()> {
        let settings = cli_with_flags(&[
            "--rate-limit-per-ip-v4-prefix=24",
            "--rate-limit-per-ip-v6-prefix=48",
        ])
        .into_settings()?;
        assert_eq!(settings.rate_limit_prefix.v4_bits(), 24);
        assert_eq!(settings.rate_limit_prefix.v6_bits(), 48);
        Ok(())
    }

    #[test]
    fn out_of_range_per_ip_prefix_is_rejected() {
        let v4 = cli_with_flags(&["--rate-limit-per-ip-v4-prefix=33"]).into_settings();
        assert!(v4.is_err(), "a /33 IPv4 prefix must fail startup, not be clamped");
        let v6 = cli_with_flags(&["--rate-limit-per-ip-v6-prefix=129"]).into_settings();
        assert!(v6.is_err(), "a /129 IPv6 prefix must fail startup, not be clamped");
    }

    #[test]
    fn trusted_proxies_default_to_none_and_parse_a_list() -> eyre::Result<()> {
        assert_eq!(cli_with_flags(&[]).into_settings()?.trusted_proxies, TrustedProxies::default());
        let settings = cli_with_flags(&["--trusted-proxies=10.0.0.0/8, 192.0.2.7,2001:db8::/32"])
            .into_settings()?;
        assert_eq!(settings.trusted_proxies.len(), 3);
        assert!(settings.trusted_proxies.contains("10.1.2.3".parse()?));
        assert!(settings.trusted_proxies.contains("2001:db8::1".parse()?));
        assert!(!settings.trusted_proxies.contains("192.0.2.8".parse()?));
        Ok(())
    }

    #[test]
    fn invalid_cidr_is_rejected_at_startup() {
        for list in [
            "10.0.0.0/33",
            "2001:db8::/129",
            "10.0.0.0/",
            "/8",
            "not-an-ip",
            "10.0.0.0/8,,192.0.2.0/24",
            "10.0.0.0/8,",
            // host bits below the prefix: a typo would otherwise widen trust
            "10.0.0.1/8",
        ] {
            let flag = format!("--trusted-proxies={list}");
            let message = match cli_with_flags(&[flag.as_str()]).into_settings() {
                Ok(_) => panic!("`{list}` must fail startup"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("--trusted-proxies"), "{list}: {message}");
        }
    }

    #[test]
    fn proxy_protocol_without_trusted_proxies_is_rejected_at_startup() -> eyre::Result<()> {
        assert!(!cli_with_flags(&[]).into_settings()?.proxy_protocol, "off by default");
        let message = match cli_with_flags(&["--proxy-protocol"]).into_settings() {
            Ok(_) => panic!("--proxy-protocol with no trusted proxy must fail startup"),
            Err(err) => format!("{err:?}"),
        };
        assert!(message.contains("--trusted-proxies"), "{message}");
        let settings = cli_with_flags(&["--proxy-protocol", "--trusted-proxies=10.0.0.0/8"])
            .into_settings()?;
        assert!(settings.proxy_protocol);
        Ok(())
    }

    #[test]
    fn proxy_protocol_counts_its_header_in_the_single_request_bound() {
        // 10s for the proxy header, then the default 55s bound
        let flags = ["--proxy-protocol", "--trusted-proxies=10.0.0.0/8"];
        let below = cli_with_flags(&[&flags[..], &["--max-connection-duration=55s"]].concat())
            .into_settings();
        assert!(below.is_err(), "the proxy header's read time must count toward the bound");
        let boundary = cli_with_flags(&[&flags[..], &["--max-connection-duration=65s"]].concat())
            .into_settings();
        assert!(boundary.is_ok(), "a cap equal to the bound must be accepted");
    }

    #[test]
    fn inflight_caps_default_on_and_zero_means_unlimited() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        // without a redirect the query cap keeps a fifth of the worker cap back
        assert_eq!((settings.max_inflight_submissions, settings.max_inflight_queries), (256, 80));
        assert_eq!(settings.max_upstream_inflight, 100);

        let settings = cli_with_flags(&[
            "--max-inflight-submissions=0",
            "--max-inflight-queries=4",
            "--max-upstream-inflight=0",
        ])
        .into_settings()?;
        assert_eq!((settings.max_inflight_submissions, settings.max_inflight_queries), (0, 4));
        assert_eq!(settings.max_upstream_inflight, 0);
        assert_eq!(
            crate::server::inflight_slots(settings.max_inflight_submissions).available_permits(),
            tokio::sync::Semaphore::MAX_PERMITS
        );
        Ok(())
    }

    #[test]
    fn without_a_redirect_reads_leave_a_fifth_of_the_worker_slots() -> eyre::Result<()> {
        let query_cap = |flags: &[&str]| -> eyre::Result<usize> {
            Ok(cli_with_flags(flags).into_settings()?.max_inflight_queries)
        };
        // the default worker cap of 100 keeps 20 slots back from reads
        assert_eq!(query_cap(&[])?, 80);
        // a cap at or below the ceiling is kept; unlimited is lowered
        assert_eq!(query_cap(&["--max-inflight-queries=50"])?, 50);
        assert_eq!(query_cap(&["--max-inflight-queries=80"])?, 80);
        assert_eq!(query_cap(&["--max-inflight-queries=0"])?, 80);
        assert_eq!(query_cap(&["--max-inflight-queries=256"])?, 80);
        assert_eq!(query_cap(&["--max-upstream-inflight=5"])?, 4);
        // at least one slot is held back, and the cap never drops to 0
        assert_eq!(query_cap(&["--max-upstream-inflight=2"])?, 1);
        assert_eq!(query_cap(&["--max-upstream-inflight=1"])?, 1);
        // an unlimited worker cap leaves nothing to share
        assert_eq!(query_cap(&["--max-upstream-inflight=0"])?, 256);
        assert_eq!(query_cap(&["--max-upstream-inflight=0", "--max-inflight-queries=0"])?, 0);
        // with a redirect only the few reads kept on the worker take a worker
        // slot, so the cap is kept as given
        let redirect = "--redirect-queries=https://rpc.example.com/";
        assert_eq!(query_cap(&[redirect])?, 256);
        assert_eq!(query_cap(&[redirect, "--max-inflight-queries=0"])?, 0);
        Ok(())
    }

    #[test]
    fn query_cap_warning_covers_unlimited_and_the_connection_cap() {
        let connections = NonZeroUsize::new(500).expect("nonzero");
        for (cap, warns) in [(0, true), (80, false), (499, false), (500, true), (600, true)] {
            assert_eq!(query_cap_reaches_connections(cap, connections), warns, "{cap}");
        }
    }

    #[test]
    fn max_request_bytes_is_configurable() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--max-request-bytes=1024"]).into_settings()?;
        assert_eq!(settings.max_request_bytes, 1_024);
        Ok(())
    }

    #[test]
    fn max_batch_len_is_configurable_and_zero_is_unlimited() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--max-batch-len=7"]).into_settings()?;
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(7));
        let settings = cli_with_flags(&[
            "--max-batch-len=0",
            "--rate-limit-per-ip=0",
            "--rate-limit-global=0",
        ])
        .into_settings()?;
        assert_eq!(settings.max_batch_len, None);
        Ok(())
    }

    /// A batch needs one token per call at once and no bucket holds more than
    /// its burst, so the effective cap stops at the smallest enabled burst: a
    /// longer batch gets the final `413`, not a `429` no retry can satisfy.
    #[test]
    fn max_batch_len_is_capped_at_the_smallest_burst() -> eyre::Result<()> {
        let settings = cli_with_flags(&[
            "--max-batch-len=500",
            "--rate-limit-per-ip-burst=300",
            "--rate-limit-global-burst=400",
        ])
        .into_settings()?;
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(300));
        // `0` means unlimited before the clamp, so the defaults cap it at the
        // per-ip burst, or at the global one with per-ip limiting off
        let settings = cli_with_flags(&["--max-batch-len=0"]).into_settings()?;
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(200));
        let settings =
            cli_with_flags(&["--max-batch-len=0", "--rate-limit-per-ip=0"]).into_settings()?;
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(6_000));
        // a cap within every burst is kept
        let settings = cli_with_flags(&["--max-batch-len=200"]).into_settings()?;
        assert_eq!(settings.max_batch_len, NonZeroUsize::new(200));
        // with both limiters off the cap is as given
        for (flag, cap) in
            [("--max-batch-len=500", NonZeroUsize::new(500)), ("--max-batch-len=0", None)]
        {
            let settings =
                cli_with_flags(&[flag, "--rate-limit-per-ip=0", "--rate-limit-global=0"])
                    .into_settings()?;
            assert_eq!(settings.max_batch_len, cap, "{flag}");
        }
        Ok(())
    }

    #[test]
    fn probe_addr_equal_to_listen_addr_is_rejected_at_startup() -> eyre::Result<()> {
        // the default listen address is 0.0.0.0:8545, so both of these would
        // bind the client listener's socket
        for probe in ["0.0.0.0:8545", "127.0.0.1:8545"] {
            let flag = format!("--probe-addr={probe}");
            let result = cli_with_flags(&[flag.as_str()]).into_settings();
            assert!(result.is_err(), "{probe} collides with the listen address");
        }
        let result = cli_with_flags(&["--listen-addr=10.0.0.5:8545", "--probe-addr=10.0.0.5:8545"])
            .into_settings();
        assert!(result.is_err(), "a probe address equal to the listen address must be rejected");

        // an unspecified ipv6 address binds dual-stack, so it covers ipv4 too
        for flags in [
            ["--listen-addr=0.0.0.0:8545", "--probe-addr=[::]:8545"],
            ["--listen-addr=[::]:8545", "--probe-addr=127.0.0.1:8545"],
        ] {
            let result = cli_with_flags(&flags).into_settings();
            assert!(result.is_err(), "{flags:?} collide on the dual-stack socket");
        }

        // a port of its own, or another address on the listen port, is accepted
        let settings = cli_with_flags(&["--probe-addr=0.0.0.0:8546"]).into_settings()?;
        assert_eq!(settings.probe_addr, Some("0.0.0.0:8546".parse()?));
        let settings =
            cli_with_flags(&["--listen-addr=10.0.0.5:8545", "--probe-addr=10.0.0.6:8545"])
                .into_settings()?;
        assert_eq!(settings.probe_addr, Some("10.0.0.6:8545".parse()?));

        // an unspecified ipv4 address does not cover ipv6, and port 0 gives
        // each listener an ephemeral port of its own
        let settings = cli_with_flags(&["--listen-addr=0.0.0.0:8545", "--probe-addr=[::1]:8545"])
            .into_settings()?;
        assert_eq!(settings.probe_addr, Some("[::1]:8545".parse()?));
        let settings = cli_with_flags(&["--listen-addr=127.0.0.1:0", "--probe-addr=127.0.0.1:0"])
            .into_settings()?;
        assert_eq!(settings.probe_addr, Some("127.0.0.1:0".parse()?));

        // unset opens no probe listener
        assert_eq!(cli_with_flags(&[]).into_settings()?.probe_addr, None);
        Ok(())
    }

    #[test]
    fn dns_flags_default_and_parse() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        assert_eq!(settings.dns_cache_ttl, Duration::from_secs(30));
        assert_eq!(settings.max_concurrent_dns_lookups.get(), 8);

        let settings = cli_with_flags(&["--dns-cache-ttl=0", "--max-concurrent-dns-lookups=2"])
            .into_settings()?;
        assert_eq!(settings.dns_cache_ttl, Duration::ZERO, "0 disables the cache");
        assert_eq!(settings.max_concurrent_dns_lookups.get(), 2);
        Ok(())
    }

    #[test]
    fn zero_dns_lookup_cap_is_rejected() {
        let result = Cli::try_parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
            "--max-concurrent-dns-lookups=0",
        ]);
        assert!(result.is_err(), "a zero cap would refuse every lookup; it must fail at startup");
    }

    #[test]
    fn inflight_byte_budget_defaults_on_and_zero_disables() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        assert_eq!(settings.max_inflight_request_bytes.map(NonZeroU32::get), Some(536_870_912));
        let settings = cli_with_flags(&["--max-inflight-request-bytes=0"]).into_settings()?;
        assert_eq!(settings.max_inflight_request_bytes, None);
        Ok(())
    }

    #[test]
    fn inflight_byte_budget_must_cover_the_request_cap_and_fit_a_u32() {
        // a chunked request reserves the whole per-request cap, so a smaller
        // budget could never admit one
        let below = cli_with_flags(&["--max-inflight-request-bytes=1048575"]).into_settings();
        assert!(below.is_err(), "a budget below --max-request-bytes must fail startup");
        let equal = cli_with_flags(&["--max-inflight-request-bytes=1048576"]).into_settings();
        assert!(equal.is_ok(), "a budget equal to --max-request-bytes must be accepted");
        // one permit per byte, counted in a u32
        let past_u32 = Cli::try_parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
            "--max-inflight-request-bytes=4294967296",
        ]);
        assert!(past_u32.is_err(), "a budget past u32::MAX must fail startup");
    }

    #[test]
    fn max_buf_size_below_hypers_minimum_is_rejected_at_startup() -> eyre::Result<()> {
        // hyper's `max_buf_size` panics below 8 KiB; the gateway must refuse
        // the value at startup rather than panic on the first connection
        let below = cli_with_flags(&["--http1-max-buf-size=8191"]).into_settings();
        assert!(below.is_err(), "a read buffer below 8192 bytes must fail startup");
        let minimum = cli_with_flags(&["--http1-max-buf-size=8192"]).into_settings()?;
        assert_eq!(minimum.http1_max_buf_size, 8_192);
        assert_eq!(cli_with_flags(&[]).into_settings()?.http1_max_buf_size, 65_536);
        Ok(())
    }

    #[test]
    fn no_query_redirect_by_default() -> eyre::Result<()> {
        assert_eq!(cli_with_flags(&[]).into_settings()?.query_upstream, None);
        Ok(())
    }

    #[test]
    fn recent_submission_ttl_defaults_to_a_minute_and_zero_disables() -> eyre::Result<()> {
        let default = cli_with_flags(&[]).into_settings()?;
        assert_eq!(default.recent_submission_ttl, Some(Duration::from_secs(60)));
        let disabled = cli_with_flags(&["--recent-submission-ttl=0"]).into_settings()?;
        assert_eq!(disabled.recent_submission_ttl, None);
        let custom = cli_with_flags(&["--recent-submission-ttl=5s"]).into_settings()?;
        assert_eq!(custom.recent_submission_ttl, Some(Duration::from_secs(5)));
        Ok(())
    }

    #[test]
    fn https_and_http_redirects_are_accepted() -> eyre::Result<()> {
        for url in [
            "https://rpc.example.com/v1/0123456789abcdef",
            "http://10.0.0.9:8545/",
            // the worker's host on another port is a different endpoint
            "http://10.0.0.7:9545/",
        ] {
            let flag = format!("--redirect-queries={url}");
            let settings = cli_with_flags(&[flag.as_str()]).into_settings()?;
            assert_eq!(settings.query_upstream, Some(Url::parse(url)?), "{url}");
        }
        Ok(())
    }

    #[test]
    fn query_timeout_above_upstream_timeout_is_rejected_at_startup() -> eyre::Result<()> {
        let redirect = "--redirect-queries=https://rpc.example.com/";
        let result = cli_with_flags(&[
            redirect,
            "--upstream-request-timeout=5s",
            "--query-request-timeout=6s",
        ])
        .into_settings();
        assert!(result.is_err(), "a query deadline above the worker's must fail startup");
        // the 10s default is above a 5s worker deadline too
        let result = cli_with_flags(&[redirect, "--upstream-request-timeout=5s"]).into_settings();
        assert!(result.is_err(), "the default query deadline must still be checked");

        // the boundary, the default, and the disabled sentinel are accepted
        let settings = cli_with_flags(&[
            redirect,
            "--upstream-request-timeout=5s",
            "--query-request-timeout=5s",
        ])
        .into_settings()?;
        assert_eq!(settings.query_request_timeout, Some(Duration::from_secs(5)));
        let settings = cli_with_flags(&[redirect]).into_settings()?;
        assert_eq!(settings.query_request_timeout, Some(Duration::from_secs(10)));
        let settings = cli_with_flags(&[
            redirect,
            "--upstream-request-timeout=5s",
            "--query-request-timeout=0",
        ])
        .into_settings()?;
        assert_eq!(settings.query_request_timeout, None);

        // without a redirect the flag is unused and not checked
        cli_with_flags(&["--upstream-request-timeout=5s"]).into_settings()?;
        Ok(())
    }

    #[test]
    fn non_http_redirect_schemes_are_rejected() {
        for url in ["ws://rpc.example.com/", "wss://rpc.example.com/", "ftp://rpc.example.com/"] {
            let flag = format!("--redirect-queries={url}");
            let result = cli_with_flags(&[flag.as_str()]).into_settings();
            assert!(result.is_err(), "{url} must be rejected");
        }
    }

    #[test]
    fn redirect_to_the_gateway_itself_is_rejected() {
        // default listen address is 0.0.0.0:8545
        let result = cli_with_flags(&["--redirect-queries=http://127.0.0.1:8545/"]).into_settings();
        assert!(result.is_err(), "a redirect onto the gateway's own listener must be rejected");
    }

    #[test]
    fn redirect_to_a_worker_rpc_host_and_port_is_rejected() {
        // the worker in `cli_with_flags` serves rpc on 10.0.0.7:8545
        for url in [
            "http://10.0.0.7:8545",
            "http://10.0.0.7:8545/some/path?key=1",
            "https://10.0.0.7:8545/",
        ] {
            let flag = format!("--redirect-queries={url}");
            let result = cli_with_flags(&[flag.as_str()]).into_settings();
            assert!(result.is_err(), "{url} is the worker and must be rejected");
        }
    }

    #[test]
    fn redirect_errors_name_the_url_by_origin_only() {
        // the gateway itself, worker 0's RPC host and port, and a fragment
        for url in [
            "http://user:s3cr3t@127.0.0.1:8545/k3y?token=t0k3n",
            "http://user:s3cr3t@10.0.0.7:8545/k3y?token=t0k3n",
            "https://user:s3cr3t@rpc.example.com/k3y?token=t0k3n#frag",
        ] {
            let flag = format!("--redirect-queries={url}");
            let message = match cli_with_flags(&[flag.as_str()]).into_settings() {
                Ok(_) => panic!("{url} must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            for secret in ["s3cr3t", "k3y", "t0k3n"] {
                assert!(!message.contains(secret), "`{secret}` leaked into: {message}");
            }
        }
    }

    #[test]
    fn redirect_with_a_fragment_is_rejected() {
        let result =
            cli_with_flags(&["--redirect-queries=https://rpc.example.com/#frag"]).into_settings();
        assert!(result.is_err());
    }

    #[test]
    fn plaintext_to_public_worker_host_warns() -> eyre::Result<()> {
        let worker = |worker_id, rpc: &str, readiness: &str| -> eyre::Result<UpstreamWorker> {
            Ok(UpstreamWorker {
                worker_id,
                rpc_url: Url::parse(rpc)?,
                readiness_url: Url::parse(readiness)?,
            })
        };
        let upstreams = [
            worker(0, "http://10.0.0.7:8545", "http://10.0.0.7:8551/health/workers")?,
            worker(1, "http://203.0.113.5:8545", "https://203.0.113.5:8551/health/workers")?,
            worker(2, "https://worker.example.com/", "http://worker.example.com:8551/health")?,
            worker(3, "https://203.0.113.9:8545", "https://203.0.113.9:8551/health/workers")?,
            worker(4, "http://localhost:8545", "http://[::1]:8551/health/workers")?,
        ];
        let warned: Vec<(u16, String)> = plaintext_worker_urls(&upstreams)
            .map(|(worker_id, url)| (worker_id, UpstreamOrigin(url).to_string()))
            .collect();
        assert_eq!(
            warned,
            [
                (1, "http://203.0.113.5:8545".to_string()),
                (2, "http://worker.example.com:8551".to_string())
            ]
        );
        Ok(())
    }

    #[test]
    fn plaintext_warning_spares_loopback_and_private_hosts() -> eyre::Result<()> {
        for (url, warns) in [
            ("http://rpc.example.com/", true),
            ("http://203.0.113.5:8545/", true),
            ("http://[2001:db8::1]:8545/", true),
            ("https://rpc.example.com/", false),
            ("https://203.0.113.5/", false),
            ("http://localhost:8545/", false),
            ("http://127.0.0.1:8545/", false),
            ("http://10.1.2.3:8545/", false),
            ("http://192.168.1.10:8545/", false),
            ("http://[::1]:8545/", false),
            ("http://[fd00::7]:8545/", false),
        ] {
            assert_eq!(plaintext_to_public_host(&Url::parse(url)?), warns, "{url}");
        }
        Ok(())
    }

    /// A redirect the query-header tests can attach a header to.
    const REDIRECT: &str = "--redirect-queries=https://rpc.example.com/";

    /// The secret every query-header test hides in its header line.
    const SECRET: &str = "s3cr3t";

    /// A temporary file holding `contents`, and the
    /// `--redirect-queries-header-file` flag naming it.
    fn header_file(contents: &str) -> eyre::Result<(tempfile::NamedTempFile, String)> {
        let mut file = tempfile::NamedTempFile::new()?;
        file.write_all(contents.as_bytes())?;
        let flag = format!("--redirect-queries-header-file={}", file.path().display());
        Ok((file, flag))
    }

    #[test]
    fn header_file_is_read_and_trimmed() -> eyre::Result<()> {
        let (_file, flag) = header_file(&format!("  X-Api-Key:  {SECRET}-k3y \r\n\n"))?;
        let settings = cli_with_flags(&[REDIRECT, flag.as_str()]).into_settings()?;
        let header = settings.query_header.ok_or_else(|| eyre::eyre!("header not resolved"))?;
        let (name, value) = header.parts();
        assert_eq!(name.as_str(), "x-api-key");
        assert_eq!(value.to_str()?, "s3cr3t-k3y");
        assert!(value.is_sensitive(), "the value must be marked sensitive");

        // the flag form is trimmed the same way
        let settings =
            cli_with_flags(&[REDIRECT, "--redirect-queries-header= X-Api-Key: s3cr3t-k3y "])
                .into_settings()?;
        let header = settings.query_header.ok_or_else(|| eyre::eyre!("header not resolved"))?;
        assert_eq!(header.parts().1.to_str()?, "s3cr3t-k3y");
        assert!(!format!("{header:?}").contains(SECRET), "debug output must be redacted");
        Ok(())
    }

    #[test]
    fn header_without_a_redirect_is_rejected_at_startup() -> eyre::Result<()> {
        let line = format!("X-Api-Key: {SECRET}");
        let (_file, file_flag) = header_file(&line)?;
        let flag = format!("--redirect-queries-header={line}");
        for flag in [flag.as_str(), file_flag.as_str()] {
            let message = match cli_with_flags(&[flag]).into_settings() {
                Ok(_) => panic!("{flag} without --redirect-queries must fail startup"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("without --redirect-queries"), "{message}");
            assert!(!message.contains(SECRET), "the value leaked into: {message}");

            // the same flag with a redirect is accepted
            let settings = cli_with_flags(&[REDIRECT, flag]).into_settings()?;
            assert!(settings.query_header.is_some(), "{flag}");
        }
        Ok(())
    }

    #[test]
    fn both_header_forms_is_an_error() -> eyre::Result<()> {
        let line = format!("X-Api-Key: {SECRET}");
        let (_file, file_flag) = header_file(&line)?;
        let flag = format!("--redirect-queries-header={line}");
        let message =
            match cli_with_flags(&[REDIRECT, flag.as_str(), file_flag.as_str()]).into_settings() {
                Ok(_) => panic!("both header forms together must fail startup"),
                Err(err) => format!("{err:?}"),
            };
        assert!(message.contains("both set"), "{message}");
        assert!(!message.contains(SECRET), "the value leaked into: {message}");
        Ok(())
    }

    #[test]
    fn malformed_header_error_omits_the_value() -> eyre::Result<()> {
        for line in [
            // no `:` at all
            "s3cr3t".to_string(),
            // a name that is not a header token
            format!("Bearer {SECRET}: value"),
            // an empty name
            format!(": {SECRET}"),
            // an empty value
            format!("X-{SECRET}:"),
            // control characters in the value
            format!("X-Api-Key: {SECRET}\u{7f}"),
            format!("X-Api-Key: {SECRET}\0"),
            // two lines
            format!("X-Api-Key: {SECRET}\nX-Other: {SECRET}"),
        ] {
            let (_file, file_flag) = header_file(&line)?;
            let flag = format!("--redirect-queries-header={line}");
            for (flag, name) in [
                (flag.as_str(), "invalid --redirect-queries-header"),
                (file_flag.as_str(), "invalid --redirect-queries-header-file"),
            ] {
                let message = match cli_with_flags(&[REDIRECT, flag]).into_settings() {
                    Ok(_) => panic!("{line:?} must be rejected"),
                    Err(err) => format!("{err:?}"),
                };
                assert!(message.contains(name), "{message}");
                assert!(!message.contains(SECRET), "the value of {line:?} leaked into: {message}");
            }
        }
        Ok(())
    }
}
