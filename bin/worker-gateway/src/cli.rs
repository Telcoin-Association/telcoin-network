//! Command-line interface and resolved runtime settings.

use std::{
    collections::BTreeSet,
    fmt, fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};

use axum::http::HeaderValue;
use clap::{Parser, ValueEnum};
use eyre::WrapErr as _;
use reqwest::{Certificate, Identity};
use tracing::warn;
use url::Url;

use crate::{
    config::{GatewayConfig, UpstreamWorker},
    identity::TrustedProxies,
    proxy::{QueryHeader, UpstreamOrigin},
    ratelimit::{PrefixLen, PrefixPolicy, RateLimit},
    server::CorsOrigins,
};

/// The `--denied-method-prefixes` default: the `tn` namespace, which the node
/// says a validator should not serve publicly, and the `debug`, `trace` and
/// `admin` namespaces, which an operator may enable on the worker for itself.
pub(crate) const DEFAULT_DENIED_METHOD_PREFIXES: &str = "tn_,debug_,trace_,admin_";

/// Longest `--shutdown-delay` accepted at startup. A longer delay holds a
/// terminating gateway's listener open past any sensible termination grace
/// period, so the orchestrator would kill it before the drain even starts.
const MAX_SHUTDOWN_DELAY: Duration = Duration::from_secs(60);

/// The `--max-request-bytes` default, [`crate::proxy::MAX_REQUEST_BYTES`], as
/// the flag's non-zero type; evaluating it fails the build if that constant is
/// ever `0`.
const DEFAULT_MAX_REQUEST_BYTES: NonZeroUsize =
    NonZeroUsize::new(crate::proxy::MAX_REQUEST_BYTES).expect("MAX_REQUEST_BYTES is non-zero");

/// The smallest `--max-request-bytes` that admits the largest submission a
/// worker accepts: reth's pool admits at most 128 KiB of raw transaction
/// (`DEFAULT_MAX_TX_INPUT_BYTES`, see [`crate::proxy::MAX_REQUEST_BYTES`]),
/// twice that once hex-encoded, plus 1 KiB of JSON-RPC envelope. A smaller cap
/// still starts, with a warning.
const MIN_SUBMISSION_REQUEST_BYTES: usize = 2 * 128 * 1024 + 1024;

/// The shortest `--rate-limit-per-ip-v4-prefix` that starts without a warning.
/// A shorter prefix puts every client inside one such block (65536 addresses
/// at `/16`) into one per-IP bucket and one connection count.
const MIN_QUIET_V4_PREFIX: u8 = 16;

/// The shortest `--rate-limit-per-ip-v6-prefix` that starts without a warning:
/// `/32` is a typical allocation to a whole provider, so a shorter prefix groups
/// many unrelated networks into one per-IP bucket and one connection count.
const MIN_QUIET_V6_PREFIX: u8 = 32;

/// Mostly stateless reverse proxy in front of Telcoin Network worker JSON-RPC.
#[derive(Parser)]
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
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_RPC_URL", hide_env_values = true)]
    pub(crate) upstream_rpc_url: Option<String>,

    /// Inline single upstream: the node readiness URL, `http` or `https`
    /// (e.g. `http://127.0.0.1:8551/health/workers`).
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_READINESS_URL", hide_env_values = true)]
    pub(crate) upstream_readiness_url: Option<String>,

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
    /// at a worker's RPC or readiness host and port, where `localhost` counts
    /// as both `127.0.0.1` and `[::1]`, and an unspecified host as any loopback
    /// address; another loopback address at a worker's port is accepted with a
    /// warning.
    #[arg(long, env = "WORKER_GATEWAY_REDIRECT_QUERIES", hide_env_values = true)]
    pub(crate) redirect_queries: Option<String>,

    /// Header sent on every request to the `--redirect-queries` endpoint and
    /// never to a worker, written as one `Name: value` line: for example a
    /// public RPC's API key, so it can meter this gateway by key rather than by
    /// the one address all redirected reads share. Every client's redirected
    /// call carries it, so use a key scoped to public reads, and quote the
    /// whole line on a command line. Names the gateway sets itself, such as
    /// `Content-Type`, `Host` or `X-Forwarded-For`, and hop-by-hop names are
    /// refused at startup. Requires `--redirect-queries`; set this or
    /// `--redirect-queries-header-file`, not both. The value is never logged.
    /// Prefer `--redirect-queries-header-file`, which keeps the secret out of
    /// the process arguments, the environment and any argument error.
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

    /// Comma-separated method-name prefixes the gateway refuses when
    /// `--redirect-queries` is unset (default `tn_,debug_,trace_,admin_`). A
    /// call whose `method` starts with one of them, matched case-sensitively
    /// from the first character, gets a JSON-RPC "method not allowed" error
    /// and never reaches the worker; a batch holding one is refused whole.
    /// Whitespace around an entry is ignored, and an empty value allows every
    /// method. With `--redirect-queries` set the list is not applied: the query
    /// upstream decides what it serves.
    #[arg(
        long,
        env = "WORKER_GATEWAY_DENIED_METHOD_PREFIXES",
        value_delimiter = ',',
        default_value = DEFAULT_DENIED_METHOD_PREFIXES
    )]
    pub(crate) denied_method_prefixes: Vec<String>,

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
    /// `--readiness-failure-threshold`). At most `--readiness-poll-interval`.
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

    /// Deadline for the upstream's response head, connection setup included,
    /// and the longest the response body may go without data, when forwarding
    /// to an upstream: a worker, or the `--redirect-queries` endpoint when
    /// `--query-request-timeout` is `0`; a body that keeps arriving is never
    /// cut. The default outlasts the 30 s reth gives
    /// `eth_sendRawTransactionSync` to wait for its receipt
    /// (`--rpc.send-raw-transaction-sync-timeout`), so a slow Sync call gets
    /// the worker's own answer rather than a gateway timeout. It covers
    /// connection setup as well, so it must be longer than
    /// `--upstream-connect-timeout`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT",
        default_value = "35s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_request_timeout: Duration,

    /// Deadline for the `--redirect-queries` endpoint's response head,
    /// connection setup included, and the longest its response body may go
    /// without data; a body that keeps arriving is never cut. The default is
    /// 10s, or `--upstream-request-timeout` when that is shorter; `0` disables
    /// it, and the query route then uses `--upstream-request-timeout`. A
    /// stalled query upstream frees each read's slot this soon, while
    /// submissions keep the longer worker deadline. With `--redirect-queries`
    /// set, a value given here must not exceed `--upstream-request-timeout`.
    #[arg(
        long,
        env = "WORKER_GATEWAY_QUERY_REQUEST_TIMEOUT",
        value_parser = humantime::parse_duration
    )]
    pub(crate) query_request_timeout: Option<Duration>,
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
    /// (see `--max-inflight-queries`). With it, the reads kept on the worker
    /// (pending nonce reads and recent submission lookups) may hold at most a
    /// fifth of it (at least one slot), and one over that share is answered at
    /// once with the same `503`. At `1` no slot stays free for submissions, and
    /// startup warns.
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
    /// `--max-inflight-request-bytes`. Must be greater than 0 (`0` does not mean
    /// unlimited here; it would refuse every body); startup warns below 257 KiB,
    /// the largest admissible submission.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_REQUEST_BYTES",
        default_value_t = DEFAULT_MAX_REQUEST_BYTES
    )]
    pub(crate) max_request_bytes: NonZeroUsize,

    /// Maximum number of calls in one JSON-RPC batch (default 50; `0` means
    /// unlimited). A longer batch is refused whole with a JSON-RPC "batch too
    /// long" error (`413`, `-32003`) before its screen verdict is applied or
    /// it is forwarded, on either route. The effective cap never exceeds the
    /// smallest per-IP or global burst: a batch needs one token per call at
    /// once, so a longer one could never pass. The submission budget does not
    /// lower it.
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
    /// individual addresses. `0` is rejected: it does not disable masking, it
    /// would put every client of the family into one bucket and one connection
    /// count; startup warns below /32 (IPv6) or /16 (IPv4).
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
    /// IPv4 allocations. `0` is rejected: it does not disable masking, it would
    /// put every client of the family into one bucket and one connection count;
    /// startup warns below /32 (IPv6) or /16 (IPv4).
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
    /// the rate (default `0` disables it). When set, a submission request's
    /// first call is metered by this budget and every other request by the
    /// global rate limit; a submission batch's further calls are charged to
    /// the per-IP and global limits like any batch's, so this bounds submission
    /// requests, not submitted transactions. A read flood that empties the
    /// global bucket no longer refuses single submissions; the per-IP limit
    /// still applies to both.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_SUBMISSIONS", default_value_t = 0)]
    pub(crate) rate_limit_submissions: u32,

    /// Most client prefixes the per-IP rate limiter tracks at once (default
    /// 100,000); a first-seen client beyond it shares one overflow bucket with
    /// every other untracked client (see the README's Rate limiting section).
    /// Memory (about 100 bytes an entry) and the once-a-second idle sweep,
    /// which walks the whole table under the lock every per-IP admission takes
    /// (about 1 ms per 100,000 entries), grow with this value; startup warns
    /// above 1,000,000. Ignored when the per-IP rate limit is disabled.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_MAX_ENTRIES",
        default_value_t = crate::ratelimit::DEFAULT_MAX_PER_IP_ENTRIES
    )]
    pub(crate) rate_limit_per_ip_max_entries: NonZeroUsize,
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
    /// `--proxy-protocol-from` (by default the `--trusted-proxies` ranges),
    /// and take the client address from it; `X-Forwarded-For` is then
    /// believed only when that address is itself in `--trusted-proxies`. A
    /// connection from such a peer whose header is missing, malformed or not
    /// complete within `--header-read-timeout` is closed; any other peer is
    /// served as plain HTTP. Off by default; requires a non-empty
    /// `--proxy-protocol-from` or `--trusted-proxies`. Turn it on only once
    /// every listed peer sends the header on every connection: from a listed
    /// peer that does not, a client's own first bytes are read as the header.
    #[arg(long, env = "WORKER_GATEWAY_PROXY_PROTOCOL")]
    pub(crate) proxy_protocol: bool,

    /// Comma-separated IPv4/IPv6 CIDR ranges of the peers whose connections
    /// must open with a PROXY protocol v2 header under `--proxy-protocol` (a
    /// bare address is a single host). Unset or blank, the `--trusted-proxies`
    /// ranges. `--trusted-proxies` keeps naming the peers whose
    /// `X-Forwarded-For` is believed and whose own connections
    /// `--max-connections-per-ip` does not count, so an L4 balancer can be
    /// listed here without its address vouching for any `X-Forwarded-For`,
    /// and an HTTP proxy can be trusted without sending PROXY headers.
    /// Requires `--proxy-protocol`.
    #[arg(long, env = "WORKER_GATEWAY_PROXY_PROTOCOL_FROM")]
    pub(crate) proxy_protocol_from: Option<String>,

    /// How long to keep accepting and serving after SIGTERM while `/ready` and
    /// `/ready/any` answer `503` with `"draining": true`, so fronts that probe
    /// them (DNS health checks, external load balancers) stop routing here
    /// before the listener closes; the probe listener (`--probe-addr`) keeps
    /// answering through it too. The in-flight drain
    /// (`--graceful-shutdown-timeout`) starts after it. At most `60s`. `0` (the
    /// default) disables the delay: the listeners close as soon as SIGTERM
    /// arrives.
    #[arg(
        long,
        env = "WORKER_GATEWAY_SHUTDOWN_DELAY",
        default_value = "0s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) shutdown_delay: Duration,

    /// How long to drain in-flight requests on SIGTERM before forcing close.
    /// Default: the request deadline, `--upstream-request-timeout` +
    /// `--header-read-timeout` (45s at the defaults), so a request accepted just
    /// before shutdown can finish; an explicit value is kept as given, and one
    /// below the deadline logs a startup warning.
    #[arg(
        long,
        env = "WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT",
        value_parser = humantime::parse_duration
    )]
    pub(crate) graceful_shutdown_timeout: Option<Duration>,

    /// Address to expose Prometheus metrics on (`GET /metrics`), on a listener
    /// separate from the client JSON-RPC port. Unset (the default) disables the
    /// metrics endpoint entirely; bind it to an internal interface, not the
    /// public edge. Named `--metrics` to match the node's flag.
    #[arg(long = "metrics", env = "WORKER_GATEWAY_METRICS_ADDR")]
    pub(crate) metrics_addr: Option<SocketAddr>,

    /// Browser origins allowed to call the gateway cross-origin (CORS): a
    /// comma-separated list of `http` or `https` origins written as
    /// `scheme://host[:port]` with no path, or `*` alone for any origin. Unset
    /// (the default) leaves CORS off and no CORS header is ever added. When
    /// set, the gateway answers every preflight `OPTIONS` itself, never
    /// forwarding it, and allows `POST` with a `Content-Type` header.
    #[arg(long, env = "WORKER_GATEWAY_CORS_ALLOWED_ORIGINS")]
    pub(crate) cors_allowed_origins: Option<String>,

    /// Fraction of proxied requests written to the access log (tracing target
    /// `gateway::access`), from `0` to `1`: `1` logs every request, rate-limit
    /// `429`s included, `0.01` one in a hundred, and the default `0` disables
    /// the log. A line carries the client's network prefix, the method class,
    /// the route, the status, the latency and the request size, never a URL,
    /// body, params or header value. A value outside `0..=1` is a startup
    /// error.
    #[arg(long, env = "WORKER_GATEWAY_ACCESS_LOG_SAMPLE", default_value_t = 0.0)]
    pub(crate) access_log_sample: f64,

    /// Most access-log lines written per second, applied after
    /// `--access-log-sample`: a sampled request over it writes no line and is
    /// counted in `tn_worker_gateway_access_log_suppressed_total`. The window
    /// is a fixed second, so a burst across one window edge can write about
    /// twice the ceiling within a second at most. Defaults to `1000`; `0`
    /// means unlimited.
    #[arg(long, env = "WORKER_GATEWAY_ACCESS_LOG_MAX_LINES_PER_SECOND", default_value_t = 1000)]
    pub(crate) access_log_max_lines_per_second: u32,

    /// Tracing filter directive (e.g. `info,worker_gateway=debug`).
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub(crate) log_filter: String,

    /// Log line format: `text` (the default), human-readable lines, or `json`,
    /// one JSON object per line for a log collector. `text` carries colour
    /// codes only when stdout is a terminal and `NO_COLOR` is unset or empty;
    /// `json` never does. Any other value is rejected at startup.
    #[arg(long, env = "WORKER_GATEWAY_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    pub(crate) log_format: LogFormat,
}

/// Prints only the flags that identify a deployment: the three URL flags by
/// origin (see [`UpstreamOrigin`]), since a worker or hosted RPC URL can carry
/// a credential in its userinfo, path or query, and `--redirect-queries-header`
/// as `<redacted>`. Every other field, a flag added later included, is left out
/// rather than printed.
impl fmt::Debug for Cli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let origin = |value: &Option<String>| {
            value.as_deref().map(|value| {
                Url::parse(value).map_or_else(
                    |_| String::from("<invalid url>"),
                    |url| UpstreamOrigin(&url).to_string(),
                )
            })
        };
        f.debug_struct("Cli")
            .field("listen_addr", &self.listen_addr)
            .field("probe_addr", &self.probe_addr)
            .field("config", &self.config)
            .field("upstream_rpc_url", &origin(&self.upstream_rpc_url))
            .field("upstream_readiness_url", &origin(&self.upstream_readiness_url))
            .field("worker_id", &self.worker_id)
            .field("redirect_queries", &origin(&self.redirect_queries))
            .field(
                "redirect_queries_header",
                &self.redirect_queries_header.as_ref().map(|_| "<redacted>"),
            )
            .field("redirect_queries_header_file", &self.redirect_queries_header_file)
            .finish_non_exhaustive()
    }
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
    /// Method-name prefixes refused when `query_upstream` is `None`; empty
    /// allows every method.
    pub(crate) denied_method_prefixes: Vec<String>,
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
    /// Cap on concurrent reads kept on the worker under `--redirect-queries`
    /// (`0` = unlimited), derived from `max_upstream_inflight` by
    /// [`worker_pool_read_cap`]; without a redirect nothing takes it.
    pub(crate) max_worker_pool_reads: usize,
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
    /// Most client prefixes the per-IP rate limiter tracks at once.
    pub(crate) rate_limit_per_ip_max_entries: NonZeroUsize,
    /// Proxies whose `X-Forwarded-For` identifies the client; empty when no
    /// proxy is trusted.
    pub(crate) trusted_proxies: TrustedProxies,
    /// The peers whose connections must open with a PROXY protocol v2 header
    /// naming the client (`--proxy-protocol-from`, else the trusted proxies),
    /// or `None` when `--proxy-protocol` is off.
    pub(crate) proxy_protocol: Option<TrustedProxies>,
    /// How long to keep serving, with `/ready` and `/ready/any` reporting
    /// draining, between the shutdown notice and closing the listeners; zero
    /// closes them at once.
    pub(crate) shutdown_delay: Duration,
    /// Graceful-shutdown drain deadline.
    pub(crate) graceful_shutdown_timeout: Duration,
    /// Address to expose the Prometheus scrape endpoint on, or `None` when
    /// metrics are disabled.
    pub(crate) metrics_addr: Option<SocketAddr>,
    /// Origins allowed to call the gateway cross-origin, or `None` when CORS is
    /// off.
    pub(crate) cors_allowed_origins: Option<CorsOrigins>,
    /// Fraction of proxied requests the access log samples, in `0..=1`; `0`
    /// disables the log.
    pub(crate) access_log_sample: f64,
    /// Most access-log lines written per second after sampling, or `None`
    /// when unlimited.
    pub(crate) access_log_max_lines_per_second: Option<NonZeroU32>,
}

impl Cli {
    /// Resolve the CLI into [`Settings`], loading the YAML upstream list or
    /// building a single inline upstream from the `--upstream-*` flags.
    pub(crate) fn into_settings(self) -> eyre::Result<Settings> {
        [
            ("--readiness-poll-interval", self.readiness_poll_interval),
            ("--readiness-poll-timeout", self.readiness_poll_timeout),
            ("--upstream-connect-timeout", self.upstream_connect_timeout),
            ("--upstream-request-timeout", self.upstream_request_timeout),
            ("--header-read-timeout", self.header_read_timeout),
        ]
        .into_iter()
        .try_for_each(|(flag, value)| ensure_non_zero(flag, value))?;
        // a poll cycle joins every upstream's poll and the query probe, so it
        // lasts as long as the slowest, up to the timeout; past the interval
        // the next cycle of every upstream waits for it
        eyre::ensure!(
            self.readiness_poll_timeout <= self.readiness_poll_interval,
            "--readiness-poll-timeout ({}) must not be longer than --readiness-poll-interval ({}): \
             a poll cycle lasts up to the timeout, so one hanging upstream or query probe would \
             stretch every upstream's poll cadence",
            humantime::format_duration(self.readiness_poll_timeout),
            humantime::format_duration(self.readiness_poll_interval),
        );
        // the request deadline covers connection setup too. one that is not
        // longer than the connect timeout ends a stalled connect first, and
        // reqwest reports that like a worker that is slow to answer, so passive
        // health would never count it (see `proxy::proxy`)
        eyre::ensure!(
            self.upstream_request_timeout > self.upstream_connect_timeout,
            "--upstream-request-timeout ({}) must be longer than --upstream-connect-timeout ({}): \
             the request deadline includes connection setup, so a stalled connect would end as a \
             request timeout and never count toward --upstream-failure-threshold",
            humantime::format_duration(self.upstream_request_timeout),
            humantime::format_duration(self.upstream_connect_timeout),
        );
        let upstreams = self.resolve_upstreams()?;
        eyre::ensure!(!upstreams.is_empty(), "no upstream workers configured");
        ensure_unique_worker_ids(&upstreams)?;
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
                "worker upstream uses plain http to a host that is not a loopback, private, \
                 CGNAT or link-local address; submissions, reads and readiness cross the network \
                 unencrypted"
            );
        });
        self.probe_addr
            .map_or(Ok(()), |probe_addr| ensure_probe_addr(self.listen_addr, probe_addr))?;
        let query_header = self.resolve_query_header()?;
        let query_upstream = self
            .redirect_queries
            .map(|value| {
                let url = parse_url_flag("--redirect-queries", &value)?;
                ensure_query_upstream(self.listen_addr, &url, &upstreams).map(|()| url)
            })
            .transpose()?;
        let query_request_timeout = resolve_query_request_timeout(
            self.query_request_timeout,
            self.upstream_request_timeout,
        );
        // the query deadline exists to shorten the query route; one longer than
        // the worker's would not, and could outlast the whole-request deadline
        // built from `--upstream-request-timeout`. the default is clamped to the
        // worker's deadline, so only a value set explicitly can fail here, and
        // without a redirect the flag is unused, so it is not checked.
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
        let deadline = request_deadline(self.upstream_request_timeout, self.header_read_timeout);
        let proxy_header_bound =
            if self.proxy_protocol { self.header_read_timeout } else { Duration::ZERO };
        let single_request_bound =
            self.header_read_timeout.saturating_add(deadline).saturating_add(proxy_header_bound);
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
        // the reads that may hold a worker slot, counted as the startup line
        // in `app::run` counts them
        let worker_bound_reads = if query_upstream.is_some() {
            worker_pool_read_cap(self.max_upstream_inflight)
        } else {
            max_inflight_queries
        };
        if submission_reserve(self.max_upstream_inflight, worker_bound_reads) == Some(0) {
            warn!(
                target: "gateway",
                max_upstream_inflight = self.max_upstream_inflight,
                "no worker slot stays free for submissions: one slow read can hold every worker \
                 slot, and every concurrent submission is then refused with 503; raise \
                 --max-upstream-inflight"
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
        warn_on_short_prefix(
            "--rate-limit-per-ip-v4-prefix",
            self.rate_limit_per_ip_v4_prefix,
            MIN_QUIET_V4_PREFIX,
        );
        warn_on_short_prefix(
            "--rate-limit-per-ip-v6-prefix",
            self.rate_limit_per_ip_v6_prefix,
            MIN_QUIET_V6_PREFIX,
        );
        let trusted_proxies = self
            .trusted_proxies
            .as_deref()
            .unwrap_or_default()
            .parse::<TrustedProxies>()
            .map_err(|err| eyre::eyre!("invalid --trusted-proxies: {err}"))?;
        warn_on_whole_families(
            "--trusted-proxies",
            &trusted_proxies,
            "is a trusted proxy, may choose its own rate-limit key and is not counted by \
             --max-connections-per-ip",
        );
        // blank, as an unset variable templated to "" is, means unset
        let proxy_protocol_from = self
            .proxy_protocol_from
            .as_deref()
            .filter(|list| !list.trim().is_empty())
            .map(|list| {
                list.parse::<TrustedProxies>()
                    .map_err(|err| eyre::eyre!("invalid --proxy-protocol-from: {err}"))
            })
            .transpose()?;
        eyre::ensure!(
            self.proxy_protocol || proxy_protocol_from.is_none(),
            "--proxy-protocol-from requires --proxy-protocol: without it no PROXY header is read"
        );
        if let Some(senders) = &proxy_protocol_from {
            warn_on_whole_families(
                "--proxy-protocol-from",
                senders,
                "must open with a PROXY header and so may name any source address as its own",
            );
        }
        let proxy_protocol = self
            .proxy_protocol
            .then(|| proxy_protocol_from.unwrap_or_else(|| trusted_proxies.clone()));
        eyre::ensure!(
            proxy_protocol.as_ref().is_none_or(|senders| !senders.is_empty()),
            "--proxy-protocol requires --proxy-protocol-from or --trusted-proxies: the PROXY \
             header is read only from the peers they list, so with none listed it would never \
             be read"
        );
        let rate_limit_per_ip =
            resolve_rate_limit(self.rate_limit_per_ip, self.rate_limit_per_ip_burst);
        let rate_limit_global =
            resolve_rate_limit(self.rate_limit_global, self.rate_limit_global_burst);
        if rate_limit_per_ip.is_some()
            && self.rate_limit_per_ip_max_entries.get() > crate::ratelimit::MAX_QUIET_PER_IP_ENTRIES
        {
            warn!(
                target: "gateway",
                max_entries = self.rate_limit_per_ip_max_entries.get(),
                "--rate-limit-per-ip-max-entries is above 1,000,000: each idle-bucket sweep \
                 walks the whole table under the lock every per-IP admission takes (about 1 ms \
                 per 100,000 entries)"
            );
        }
        // a batch of k calls needs k tokens at once and no bucket ever holds
        // more than its burst, so a batch longer than the smallest per-ip or
        // global burst could only ever get a 429 that no retry satisfies; cap
        // batches there so such a batch gets the final 413 instead. the
        // submission budget charges only a batch's first call, so it is left out
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
                    "--max-batch-len is above the smallest per-IP or global burst, and a batch \
                     needs one token per call at once; capping batches at the burst"
                );
                Some(burst)
            }
            (cap, _) => cap,
        };
        let max_inflight_request_bytes = resolve_inflight_byte_budget(
            self.max_inflight_request_bytes,
            self.max_request_bytes.get(),
        )?;
        if self.max_request_bytes.get() < MIN_SUBMISSION_REQUEST_BYTES {
            warn!(
                target: "gateway",
                max_request_bytes = self.max_request_bytes.get(),
                floor = MIN_SUBMISSION_REQUEST_BYTES,
                "--max-request-bytes is below one hex-encoded 128 KiB transaction plus its \
                 JSON-RPC envelope; the largest admissible submission would get 413"
            );
        }
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

        eyre::ensure!(
            self.shutdown_delay <= MAX_SHUTDOWN_DELAY,
            "--shutdown-delay ({}) is longer than the {} maximum; a terminating gateway would \
             keep its listener open that long before draining",
            humantime::format_duration(self.shutdown_delay),
            humantime::format_duration(MAX_SHUTDOWN_DELAY),
        );
        // the drain defaults to the request deadline so a request accepted just
        // before shutdown can finish; a shorter explicit value is the
        // operator's choice, so it is kept, with a warning
        let graceful_shutdown_timeout = self.graceful_shutdown_timeout.unwrap_or(deadline);
        if graceful_shutdown_timeout < deadline {
            warn!(
                target: "gateway",
                graceful_shutdown_timeout = %humantime::format_duration(graceful_shutdown_timeout),
                request_deadline = %humantime::format_duration(deadline),
                "--graceful-shutdown-timeout is shorter than the request deadline \
                 (--upstream-request-timeout + --header-read-timeout), so a request accepted just \
                 before shutdown can be cut with no response"
            );
        }
        let cors_allowed_origins =
            self.cors_allowed_origins.as_deref().map(resolve_cors_origins).transpose()?;
        // `contains` is false for NaN too, so every value that is not a
        // fraction fails here
        eyre::ensure!(
            (0.0..=1.0).contains(&self.access_log_sample),
            "invalid --access-log-sample {}: use a fraction from 0 to 1",
            self.access_log_sample
        );
        Ok(Settings {
            listen_addr: self.listen_addr,
            probe_addr: self.probe_addr,
            upstreams,
            query_upstream,
            query_header,
            recent_submission_ttl: resolve_optional_duration(self.recent_submission_ttl),
            denied_method_prefixes: resolve_denied_method_prefixes(self.denied_method_prefixes),
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
            max_worker_pool_reads: worker_pool_read_cap(self.max_upstream_inflight),
            tcp_user_timeout: resolve_optional_duration(self.tcp_user_timeout),
            max_connection_duration,
            max_request_bytes: self.max_request_bytes.get(),
            max_batch_len,
            max_inflight_request_bytes,
            http1_max_buf_size: self.http1_max_buf_size,
            rate_limit_per_ip,
            rate_limit_prefix,
            rate_limit_global,
            // no burst flag: the burst derives like a `0` burst on the others
            rate_limit_submissions: resolve_rate_limit(self.rate_limit_submissions, 0),
            rate_limit_per_ip_max_entries: self.rate_limit_per_ip_max_entries,
            trusted_proxies,
            proxy_protocol,
            shutdown_delay: self.shutdown_delay,
            graceful_shutdown_timeout,
            metrics_addr: self.metrics_addr,
            cors_allowed_origins,
            access_log_sample: self.access_log_sample,
            access_log_max_lines_per_second: NonZeroU32::new(self.access_log_max_lines_per_second),
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
                    .as_deref()
                    .zip(self.upstream_readiness_url.as_deref())
                    .ok_or_else(|| {
                        eyre::eyre!(
                            "provide --config, or both --upstream-rpc-url and --upstream-readiness-url"
                        )
                    })?;
                Ok(vec![UpstreamWorker {
                    worker_id: self.worker_id,
                    rpc_url: parse_url_flag("--upstream-rpc-url", rpc_url)?,
                    readiness_url: parse_url_flag("--upstream-readiness-url", readiness_url)?,
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

/// Trim each `--denied-method-prefixes` entry and drop the empty ones, so an
/// empty value allows every method rather than denying all of them (every name
/// starts with the empty prefix) and `tn_, debug_` still denies `debug_`.
fn resolve_denied_method_prefixes(prefixes: Vec<String>) -> Vec<String> {
    prefixes
        .into_iter()
        .map(|prefix| prefix.trim().to_owned())
        .filter(|prefix| !prefix.is_empty())
        .collect()
}

/// Log line format selected by `--log-format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum LogFormat {
    /// Human-readable lines.
    Text,
    /// One JSON object per line, with the event's `timestamp`, `level`,
    /// `target` and `fields`.
    Json,
}

/// Warn when the ranges of `flag` cover a whole address family, naming the
/// family and what every client of it that can reach the gateway then does
/// (`consequence`). IPv4 and IPv4-mapped clients are matched against the IPv4
/// ranges only and native IPv6 clients against the IPv6 ranges only, so each
/// family is judged on its own.
fn warn_on_whole_families(flag: &str, ranges: &TrustedProxies, consequence: &str) {
    for (family, covered) in [
        ("IPv4 address, IPv4-mapped ones included", ranges.covers_all_ipv4()),
        ("native IPv6 address", ranges.covers_all_ipv6()),
    ] {
        if covered {
            warn!(
                target: "gateway",
                "{flag} covers every {family}: every such client that can reach the gateway \
                 {consequence}; list only the fronts' own addresses"
            );
        }
    }
}

/// Warn when the per-IP prefix `flag` is shorter than `floor` bits: every
/// client inside one such prefix shares one per-IP bucket and one
/// `--max-connections-per-ip` count, so one busy client can throttle or lock
/// out every other client of its block.
fn warn_on_short_prefix(flag: &str, bits: u8, floor: u8) {
    if bits < floor {
        warn!(
            target: "gateway",
            "{flag} is /{bits}: every client inside one /{bits} shares one per-IP bucket and one \
             --max-connections-per-ip count"
        );
    }
}

/// Reject a zero value for a duration flag that has no disabled sentinel. A
/// zero readiness poll interval panics the poller (`tokio::time::interval`),
/// and a zero timeout or deadline fails every poll, forward or request it
/// bounds, so either is a configuration error reported at startup.
fn ensure_non_zero(flag: &str, value: Duration) -> eyre::Result<()> {
    eyre::ensure!(!value.is_zero(), "{flag} must be greater than zero");
    Ok(())
}

/// Reject an upstream list that names one worker id twice. Readiness
/// transitions, their log lines and the `tn_worker_gateway_upstream_ready`
/// gauge are keyed by worker id, so two entries with one id would be reported
/// as a single worker whose state flips with whichever entry was polled last.
fn ensure_unique_worker_ids(upstreams: &[UpstreamWorker]) -> eyre::Result<()> {
    let mut seen = BTreeSet::new();
    match upstreams.iter().map(|upstream| upstream.worker_id).find(|id| !seen.insert(*id)) {
        Some(id) => eyre::bail!(
            "worker id {id} appears more than once in the upstream list; list each worker once"
        ),
        None => Ok(()),
    }
}

/// Turn a duration flag into `Some(duration)`, or `None` when zero (the
/// flag's disabled sentinel, mirroring the `0`-disables rate-limit flags).
fn resolve_optional_duration(value: Duration) -> Option<Duration> {
    (!value.is_zero()).then_some(value)
}

/// The service's whole-request deadline: the upstream hop's own
/// `upstream_request_timeout` plus a `header_read_timeout`-sized budget for
/// reading the body, so a trickled body cannot hold a request slot without
/// bound. It is also the default drain on shutdown
/// (`--graceful-shutdown-timeout`), so a request accepted just before it can
/// finish.
pub(crate) fn request_deadline(
    upstream_request_timeout: Duration,
    header_read_timeout: Duration,
) -> Duration {
    upstream_request_timeout.saturating_add(header_read_timeout)
}

/// `--query-request-timeout` when it is not set, unless the worker's deadline
/// is shorter.
const DEFAULT_QUERY_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The query route's deadline for `--query-request-timeout`, or `None` when
/// it is `0` and the route follows `--upstream-request-timeout`. Unset, it is
/// [`DEFAULT_QUERY_REQUEST_TIMEOUT`] clamped to `upstream`, so a worker
/// deadline under 10 s needs no second flag; a value set explicitly is kept as
/// given, for the caller to check against `upstream`.
fn resolve_query_request_timeout(value: Option<Duration>, upstream: Duration) -> Option<Duration> {
    resolve_optional_duration(value.unwrap_or(DEFAULT_QUERY_REQUEST_TIMEOUT.min(upstream)))
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
/// lookups kept on it (see `crate::proxy`), and those have their own share of
/// the worker slots (see [`worker_pool_read_cap`]).
fn effective_query_cap(
    max_inflight_queries: usize,
    max_upstream_inflight: usize,
    redirect: bool,
) -> usize {
    if redirect || max_upstream_inflight == 0 {
        return max_inflight_queries;
    }
    let reserve = worker_slot_fifth(max_upstream_inflight);
    let ceiling = max_upstream_inflight.saturating_sub(reserve).max(1);
    match max_inflight_queries {
        0 => ceiling,
        cap => cap.min(ceiling),
    }
}

/// The most worker slots the reads kept on the worker under
/// `--redirect-queries` (pending nonce reads and recent submission lookups,
/// see `crate::proxy`) may hold at once: a fifth of `--max-upstream-inflight`,
/// at least one slot, or `0` (unlimited) when the worker cap is.
///
/// Those reads are queries, but they share the worker slots with the
/// submissions the redirect keeps on the worker; while the worker is slow they
/// would otherwise pile up until every worker slot is theirs and every
/// submission is refused with `503`.
fn worker_pool_read_cap(max_upstream_inflight: usize) -> usize {
    match max_upstream_inflight {
        0 => 0,
        cap => worker_slot_fifth(cap),
    }
}

/// A fifth of a worker cap of `max_upstream_inflight` slots, at least one:
/// without a redirect, the share reads leave free for submissions (see
/// [`effective_query_cap`]); with one, the share the reads kept on the worker
/// may hold (see [`worker_pool_read_cap`]).
fn worker_slot_fifth(max_upstream_inflight: usize) -> usize {
    (max_upstream_inflight / 5).max(1)
}

/// The worker slots reads can never hold, which stay free for submissions:
/// `max_upstream_inflight` less the reads that may hold a worker slot at once,
/// which is the effective query cap `max_inflight_queries` without a redirect
/// (as [`effective_query_cap`] left it) and the share of the reads kept on the
/// worker with one (see [`worker_pool_read_cap`]); `None` when the worker cap
/// is unlimited. An unlimited read cap holds nothing back, and neither does one
/// at or above the worker cap.
pub(crate) fn submission_reserve(
    max_upstream_inflight: usize,
    worker_bound_reads: usize,
) -> Option<usize> {
    (max_upstream_inflight != 0).then(|| match worker_bound_reads {
        0 => 0,
        cap => max_upstream_inflight.saturating_sub(cap),
    })
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
/// meaning `/32` would hide it. Exactly `/0` is a startup error too: the
/// neighbouring flags read `0` as disabled, but here it would key a whole
/// family to one bucket and one connection count. [`PrefixLen`] itself still
/// accepts `/0`; only the operator boundary refuses it.
fn resolve_prefix_policy(v4: u8, v6: u8) -> eyre::Result<PrefixPolicy> {
    eyre::ensure!(
        v4 > 0,
        "--rate-limit-per-ip-v4-prefix 0 would put every IPv4 client in one bucket and one \
         connection count; it does not disable masking"
    );
    eyre::ensure!(
        v6 > 0,
        "--rate-limit-per-ip-v6-prefix 0 would put every IPv6 client in one bucket and one \
         connection count; it does not disable masking"
    );
    PrefixLen::v4(v4)
        .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v4-prefix: {err}"))
        .and_then(|v4| {
            PrefixLen::v6(v6)
                .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v6-prefix: {err}"))
                .map(|v6| PrefixPolicy::new(v4, v6))
        })
}

/// Validate `--cors-allowed-origins` into [`CorsOrigins`]: `*` alone allows any
/// origin; otherwise every comma-separated entry, trimmed, must be an origin
/// [`parse_cors_origin`] accepts. An empty entry, and a `*` beside other
/// entries, are errors rather than silently dropped or widened: both are typos
/// whose intent is unclear. Errors name the entry by its 1-based position, not
/// its text, since an origin list can be long.
fn resolve_cors_origins(value: &str) -> eyre::Result<CorsOrigins> {
    let entries: Vec<&str> = value.split(',').map(str::trim).collect();
    if entries == ["*"] {
        return Ok(CorsOrigins::Any);
    }
    entries
        .iter()
        .zip(1..)
        .map(|(entry, position)| {
            eyre::ensure!(
                !entry.is_empty(),
                "invalid --cors-allowed-origins: entry {position} is empty"
            );
            eyre::ensure!(
                *entry != "*",
                "invalid --cors-allowed-origins: entry {position} is `*`, which must be the only \
                 entry"
            );
            parse_cors_origin(entry).ok_or_else(|| {
                eyre::eyre!(
                    "invalid --cors-allowed-origins: entry {position} is not an http or https \
                     origin of the form scheme://host[:port] with no path, query, fragment or \
                     credentials"
                )
            })
        })
        .collect::<eyre::Result<Vec<_>>>()
        .map(CorsOrigins::List)
}

/// Parse one `--cors-allowed-origins` entry into the `Origin` value a browser
/// sends for it, or `None` when it is not an `http` or `https`
/// `scheme://host[:port]`. The result is the origin's serialization (lowercase
/// scheme and host, punycode, default port dropped), since the CORS layer
/// compares it byte for byte with the request's `Origin`.
fn parse_cors_origin(entry: &str) -> Option<HeaderValue> {
    // the url parser turns a bare origin into one with path `/`, hiding a
    // trailing slash, treats `\` like `/` for http and https, and silently
    // drops tab, LF and CR, so anything past the authority (and userinfo), a
    // backslash and any control character are rejected on the raw text
    let (_, authority) = entry.split_once("://")?;
    if authority.contains(['/', '\\', '?', '#', '@'])
        || authority.chars().any(|c| c.is_ascii_control())
    {
        return None;
    }
    let url = Url::parse(entry).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    HeaderValue::from_str(&url.origin().ascii_serialization()).ok()
}

/// Reject a worker RPC or readiness URL whose scheme is neither `http` nor
/// `https` at startup, rather than at the first forward or poll. An `https`
/// URL is verified against the native root store plus any `--upstream-ca-cert`
/// certificates (see [`crate::proxy::client_builder`]). The message names the
/// URL by origin only, since a worker URL can carry a credential in its
/// userinfo, path or query.
///
/// A scheme the URL standard does not define is named not at all: a value
/// written without one, such as `key:secret@host:port`, parses with part of
/// the credential as its scheme (and so as its origin).
fn ensure_http_or_https(url: &Url) -> eyre::Result<()> {
    eyre::ensure!(
        url.is_special(),
        "a worker upstream URL has no http:// or https:// scheme; write it as \
         http(s)://host:port[/path]"
    );
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

/// Parse the value of the URL flag `flag` (`--upstream-rpc-url`,
/// `--upstream-readiness-url` or `--redirect-queries`) into a URL.
///
/// These flags are taken as strings and parsed here rather than by clap,
/// because clap's parse error repeats the rejected value whole, and a worker or
/// hosted RPC URL can carry a credential or an API key in its userinfo, path or
/// query. The error names the flag and what is wrong without echoing the value.
fn parse_url_flag(flag: &str, value: &str) -> eyre::Result<Url> {
    Url::parse(value).map_err(|err| {
        eyre::eyre!("{flag} is not a valid URL ({err}); write it as http(s)://host[:port][/path]")
    })
}

/// A startup message for a clap parse error that does not repeat what was
/// typed, or `None` when clap's own output is safe to print.
///
/// Clap quotes the offending token whole: the value of a flag it could not
/// parse, and any argument it did not expect. An unquoted
/// `--redirect-queries-header X-Api-Key: <key>` leaves the key behind as such
/// a stray argument, so those kinds get a message that names the flag, or a
/// similar flag clap suggests, and never the token. A value error keeps the
/// parser's own reason: the flags that can carry a secret are taken as strings
/// and parsed after clap, so the typed parsers only ever see durations,
/// numbers, addresses and the like. Help, version and the kinds that carry
/// only flag names (a missing or conflicting argument, for example) return
/// `None`, and the caller lets clap print and exit as usual.
pub(crate) fn redacted_parse_error(err: &clap::Error) -> Option<String> {
    use clap::error::{ContextKind, ContextValue, ErrorKind};

    let context = |kind| match err.get(kind) {
        Some(ContextValue::String(value)) => Some(value.as_str()),
        _ => None,
    };
    match err.kind() {
        ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand => {
            let suggestion = context(ContextKind::SuggestedArg)
                .map(|flag| format!("; a similar flag exists: {flag}"))
                .unwrap_or_default();
            Some(format!(
                "unexpected argument (not repeated here, as it may be part of a \
                 secret){suggestion}; quote a --redirect-queries-header line whole, as in \
                 --redirect-queries-header 'Name: value', or use --redirect-queries-header-file; \
                 see --help"
            ))
        }
        ErrorKind::InvalidValue | ErrorKind::ValueValidation | ErrorKind::TooManyValues => {
            let arg = context(ContextKind::InvalidArg).unwrap_or("an argument");
            let reason = std::error::Error::source(err)
                .map(|source| format!(": {source}"))
                .unwrap_or_default();
            let possible = match err.get(ContextKind::ValidValue) {
                Some(ContextValue::Strings(values)) if !values.is_empty() => {
                    format!("; possible values: {}", values.join(", "))
                }
                _ => String::new(),
            };
            Some(format!(
                "invalid value for '{arg}'{reason} (the value is not repeated here){possible}; \
                 see --help"
            ))
        }
        _ => None,
    }
}

/// Validate the `--redirect-queries` URL: `http` or `https`, with a host and no
/// fragment, not the gateway itself, and not on any worker's RPC or readiness
/// host and port.
///
/// The last check is an error rather than a warning because reads sent to a
/// worker's node land on the validator, which is the load the redirect exists
/// to remove. It compares host and port whatever the scheme, so an `https` URL
/// on a worker's `http` socket is caught too, an IPv4-mapped IPv6 literal
/// matches the IPv4 address it names, `localhost` matches `127.0.0.1` and
/// `[::1]`, and an unspecified host (`0.0.0.0`, `[::]`) matches any loopback or
/// unspecified one (see [`same_host`]). A URL elsewhere on a worker's machine
/// (see [`same_machine`]), at another port or on another loopback address at
/// the worker's port, is accepted with a warning, since that endpoint may serve
/// a separate node on the validator's machine. Plain `http` to a host that is
/// not an address literal unroutable from the internet (see
/// [`plaintext_to_public_host`]) is accepted with a warning: the reads and
/// their answers then cross the network unencrypted. Messages name the URL by
/// origin only, since a hosted RPC URL can carry an API key in its path.
fn ensure_query_upstream(
    listen_addr: SocketAddr,
    url: &Url,
    upstreams: &[UpstreamWorker],
) -> eyre::Result<()> {
    // the scheme is not echoed either: a value written without one, such as
    // `key:secret@host`, parses with part of the credential as its scheme
    eyre::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "unsupported URL scheme in --redirect-queries: use http or https"
    );
    eyre::ensure!(url.has_host(), "--redirect-queries URL has no host");
    eyre::ensure!(
        url.fragment().is_none(),
        "--redirect-queries URL `{}` carries a fragment; remove it",
        UpstreamOrigin(url)
    );
    ensure_not_gateway(listen_addr, url)?;
    upstreams.iter().try_for_each(|upstream| {
        [("RPC", &upstream.rpc_url), ("readiness", &upstream.readiness_url)]
            .into_iter()
            .try_for_each(|(endpoint, worker_url)| {
                eyre::ensure!(
                    !same_host_and_port(url, worker_url),
                    "--redirect-queries `{}` is worker {}'s {endpoint} host and port, so reads \
                     would still reach the validator; point it at a separate JSON-RPC endpoint",
                    UpstreamOrigin(url),
                    upstream.worker_id
                );
                Ok(())
            })
    })?;
    if let Some(worker) = upstreams.iter().find(|upstream| {
        same_machine(url, &upstream.rpc_url) || same_machine(url, &upstream.readiness_url)
    }) {
        warn!(
            target: "gateway",
            upstream = %UpstreamOrigin(url),
            worker_id = worker.worker_id,
            "--redirect-queries is on a worker's host at another port, or on another loopback \
             address at its port; unless that endpoint serves a separate node, reads still reach \
             the validator's machine"
        );
    }
    if plaintext_to_public_host(url) {
        warn!(
            target: "gateway",
            upstream = %UpstreamOrigin(url),
            "--redirect-queries uses plain http to a host that is not a loopback, private, \
             CGNAT or link-local address; reads and their answers cross the network unencrypted"
        );
    }
    Ok(())
}

/// The worker RPC and readiness URLs that use plain `http` to a host that is
/// not an address literal unroutable from the internet (see
/// [`plaintext_to_public_host`]), each with its worker id.
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
    a.port_or_known_default() == b.port_or_known_default() && same_host(a, b)
}

/// Whether two URLs name the same host. Hosts that name addresses (see
/// [`url_host_ips`]) match when they share one, so an IPv4-mapped IPv6 literal
/// (`[::ffff:10.0.0.7]`) matches the IPv4 address it names and `localhost`
/// matches `127.0.0.1` and `[::1]`; other domain names match only as written.
/// An unspecified address (`0.0.0.0`, `[::]`) also matches any loopback or
/// unspecified address, in either family: a node bound to one answers on the
/// loopback addresses, and a URL naming one connects to this machine. Two
/// explicit loopback addresses match only when they are equal, since `[::1]`
/// or `127.0.0.2` beside `127.0.0.1` can be another node's socket at the same
/// port; [`same_machine`] is the wider test that matches them.
fn same_host(a: &Url, b: &Url) -> bool {
    let this_machine = |ip: &IpAddr| ip.is_loopback() || ip.is_unspecified();
    url_host_ips(a).zip(url_host_ips(b)).map_or_else(
        || a.host() == b.host(),
        |(a, b)| {
            a.iter().any(|ip| b.contains(ip))
                || (a.iter().any(IpAddr::is_unspecified) && b.iter().any(this_machine))
                || (b.iter().any(IpAddr::is_unspecified) && a.iter().any(this_machine))
        },
    )
}

/// Whether two URLs name hosts on this same machine: the same host (see
/// [`same_host`]), or two loopback or unspecified addresses in either family,
/// each of which names this machine. Only the redirect's warning uses this
/// wider test, since two different explicit loopback addresses can be two
/// sockets.
fn same_machine(a: &Url, b: &Url) -> bool {
    let this_machine = |url: &Url| {
        url_host_ips(url)
            .is_some_and(|ips| ips.iter().any(|ip| ip.is_loopback() || ip.is_unspecified()))
    };
    same_host(a, b) || (this_machine(a) && this_machine(b))
}

/// Whether `url` is plain `http` to anything but an address literal that is
/// not routable from the internet: loopback, private (RFC 1918 and IPv6
/// unique local), shared (the `100.64.0.0/10` carrier-grade NAT range) or link
/// local (`169.254.0.0/16`, `fe80::/10`). A domain name (other than
/// `localhost`) counts as public, since it cannot be checked without resolving
/// it.
fn plaintext_to_public_host(url: &Url) -> bool {
    let local = url_host_ips(url).is_some_and(|ips| {
        ips.iter().all(|ip| match ip {
            IpAddr::V4(ip) => {
                let [first, second, ..] = ip.octets();
                let shared = first == 100 && (second & 0b1100_0000) == 64;
                ip.is_loopback() || ip.is_private() || shared || ip.is_link_local()
            }
            IpAddr::V6(ip) => {
                ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local()
            }
        })
    });
    url.scheme() == "http" && !local
}

/// Reject an upstream URL that can only point back at the gateway itself (a
/// loopback/unspecified host, or the listen interface, on the listen port):
/// forwarding to it would loop. The default listen port (`8545`) matches the
/// worker's default RPC port, so a single-host setup left on defaults hits
/// this. The runtime hop-header guard (see [`crate::proxy`]) catches the
/// loops this startup check cannot see, e.g. a VIP that fronts the gateways,
/// or a URL naming one of this host's other addresses.
///
/// The URL hits the gateway when any address its host names (see
/// [`url_host_ips`]; `localhost` names both loopbacks) reaches the listener:
/// the listen address itself, an unspecified address (which connects to this
/// host) of a family the listener takes, or a loopback address when the
/// listener is unspecified and takes its family. An unspecified IPv6 listener
/// takes both families (tokio leaves `IPV6_V6ONLY` unset, so `[::]` binds
/// dual-stack on a default Linux host), as [`ensure_probe_addr`] assumes; an
/// unspecified IPv4 listener takes only IPv4. An IPv4-mapped IPv6 address
/// (`[::ffff:127.0.0.1]`) is compared as the IPv4 address it names, on either
/// side, since a dual-stack socket reaches the same listener through both
/// spellings.
fn ensure_not_gateway(listen_addr: SocketAddr, url: &Url) -> eyre::Result<()> {
    let same_port = url.port_or_known_default() == Some(listen_addr.port());
    let listen_ip = listen_addr.ip().to_canonical();
    let hits_gateway = url_host_ips(url).is_some_and(|ips| {
        ips.into_iter().any(|ip| {
            let same_family = ip.is_ipv4() == listen_ip.is_ipv4();
            let takes_family = listen_ip.is_unspecified() && (listen_ip.is_ipv6() || ip.is_ipv4());
            ip == listen_ip
                || (ip.is_unspecified() && (same_family || takes_family))
                || (ip.is_loopback() && takes_family)
        })
    });
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
/// ephemeral port, so it never collides. An IPv4-mapped IPv6 address
/// (`[::ffff:127.0.0.1]`) is compared as the IPv4 address it names, on either
/// side, as [`ensure_not_gateway`] does: it binds the same IPv4 port. The two
/// listeners cannot share a socket, and the probes are only out of the client
/// connection cap's reach on a socket of their own.
fn ensure_probe_addr(listen_addr: SocketAddr, probe_addr: SocketAddr) -> eyre::Result<()> {
    let (listen_ip, probe_ip) = (listen_addr.ip().to_canonical(), probe_addr.ip().to_canonical());
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

/// The addresses the upstream host names without a lookup: an IP literal, or
/// `localhost`, which names both loopback addresses (a client may connect to
/// either). `None` for any other domain name, which cannot be checked without
/// resolving it. An IPv4-mapped IPv6 literal is returned as the IPv4 address it
/// names, so every caller compares and classifies it the same way.
fn url_host_ips(url: &Url) -> Option<Vec<IpAddr>> {
    url.host().and_then(|host| match host {
        url::Host::Domain(name) => name
            .eq_ignore_ascii_case("localhost")
            .then(|| vec![IpAddr::V4(Ipv4Addr::LOCALHOST), IpAddr::V6(Ipv6Addr::LOCALHOST)]),
        url::Host::Ipv4(ip) => Some(vec![IpAddr::V4(ip)]),
        url::Host::Ipv6(ip) => Some(vec![IpAddr::V6(ip).to_canonical()]),
    })
}

/// The command `main` parses, with every arg's `env` fallback removed.
#[cfg(test)]
fn test_command() -> clap::Command {
    use clap::CommandFactory as _;

    Cli::command().mut_args(|arg| arg.env(None))
}

/// Parse `argv` as `main` does, minus clap's `env` fallbacks, so an ambient
/// `WORKER_GATEWAY_*` variable cannot change a test's outcome.
#[cfg(test)]
pub(crate) fn try_parse_test_argv<I, T>(argv: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    use clap::FromArgMatches as _;

    let mut matches = test_command().try_get_matches_from(argv)?;
    Cli::from_arg_matches_mut(&mut matches).map_err(|err| err.format(&mut test_command()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::Write as _,
        sync::{Arc, Mutex},
    };

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
        try_parse_test_argv(argv).expect("argv parses")
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
        let settings = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--tcp-user-timeout=0s",
            "--max-connection-duration=0",
        ])
        .expect("argv parses")
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
        let result = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=45s",
        ])
        .expect("argv parses")
        .into_settings();
        assert!(result.is_err(), "a cap below the single-request bound must be a startup error");

        // The boundary itself is accepted.
        let boundary = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=55s",
        ])
        .expect("argv parses")
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
            let result = try_parse_test_argv([
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
    fn delay_over_a_minute_is_rejected_at_startup() -> eyre::Result<()> {
        // off by default
        assert_eq!(cli_with_flags(&[]).into_settings()?.shutdown_delay, Duration::ZERO);

        // the boundary itself is accepted
        let settings = cli_with_flags(&["--shutdown-delay=60s"]).into_settings()?;
        assert_eq!(settings.shutdown_delay, Duration::from_secs(60));

        let message = match cli_with_flags(&["--shutdown-delay=61s"]).into_settings() {
            Ok(_) => panic!("a delay over a minute must be a startup error"),
            Err(err) => err.to_string(),
        };
        assert!(message.contains("--shutdown-delay"), "the error must name the flag: {message}");
        Ok(())
    }

    #[test]
    fn worker_id_conflicts_with_config() {
        let result =
            try_parse_test_argv(["worker-gateway", "--config=gateway.yaml", "--worker-id=3"]);
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
        let message = match cli_with(None, Some("http://127.0.0.1:8545"), None).into_settings() {
            Ok(_) => panic!("an inline upstream without a readiness URL must be rejected"),
            Err(err) => format!("{err:?}"),
        };
        assert!(
            message.contains(
                "provide --config, or both --upstream-rpc-url and --upstream-readiness-url"
            ),
            "{message}"
        );
    }

    #[test]
    fn test_argv_has_no_env_fallbacks() {
        use clap::CommandFactory as _;

        let with_env = |command: clap::Command| {
            command.get_arguments().filter(|arg| arg.get_env().is_some()).count()
        };
        // every flag `main` parses has a WORKER_GATEWAY_* fallback ...
        assert!(with_env(Cli::command()) > 40, "{}", with_env(Cli::command()));
        // ... and the command the tests parse has none
        assert_eq!(with_env(test_command()), 0);
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

    /// Startup errors about a worker URL name it by origin only: a scheme error
    /// and a self-pointing error, for both the RPC and the readiness URL. A
    /// value written without a scheme parses with part of its credential as
    /// the scheme, so its error names neither the scheme nor the origin.
    ///
    /// A hosted RPC URL put in a worker flag by mistake carries its API key in
    /// its path or query: as a websocket URL it is refused by origin only; as
    /// an `https` URL it is a valid worker URL, so startup accepts it and
    /// prints nothing of it; as plain `http` it draws the plaintext warning,
    /// which names its origin only.
    #[test]
    fn startup_errors_name_origins_only() {
        let ready = "http://10.0.0.7:8551/health/workers";
        let userinfo = ["user", "pw@", "/path"].as_slice();
        let schemeless = ["apikey", "s3cr3t"].as_slice();
        let hosted_secrets = ["alice", "pw@", "/v1", "k3y", "apikey", "s3cr3t"];
        let hosted = "alice:pw@rpc.example.com/v1/k3y?apikey=s3cr3t";
        let hosted_ws = format!("wss://{hosted}");
        for (rpc, readiness, secrets, origin) in [
            ("ftp://user:pw@host/path", ready, userinfo, Some("`ftp://host:21`")),
            ("http://10.0.0.7:8544", "ftp://user:pw@host/path", userinfo, Some("`ftp://host:21`")),
            (
                "http://user:pw@127.0.0.1:8545/path",
                ready,
                userinfo,
                Some("`http://127.0.0.1:8545`"),
            ),
            (
                "http://10.0.0.7:8544",
                "http://user:pw@127.0.0.1:8545/path",
                userinfo,
                Some("`http://127.0.0.1:8545`"),
            ),
            ("apikey:s3cr3t@10.0.0.7:8545", ready, schemeless, None),
            ("http://10.0.0.7:8544", "apikey:s3cr3t@10.0.0.7:8545", schemeless, None),
            (
                hosted_ws.as_str(),
                ready,
                hosted_secrets.as_slice(),
                Some("`wss://rpc.example.com:443`"),
            ),
            (
                "http://10.0.0.7:8544",
                hosted_ws.as_str(),
                hosted_secrets.as_slice(),
                Some("`wss://rpc.example.com:443`"),
            ),
        ] {
            let message = match cli_with(None, Some(rpc), Some(readiness)).into_settings() {
                Ok(_) => panic!("{rpc} / {readiness} must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            for secret in secrets {
                assert!(!message.contains(secret), "`{secret}` leaked into: {message}");
            }
            if let Some(origin) = origin {
                assert!(message.contains(origin), "the origin is missing from: {message}");
            }
        }
        for (scheme, warns) in [("https", false), ("http", true)] {
            let rpc = format!("{scheme}://{hosted}");
            let (settings, logs) = into_settings_logged(cli_with(None, Some(&rpc), Some(ready)));
            if let Err(err) = settings {
                panic!("a {scheme} worker URL must be accepted: {err:?}");
            }
            assert_eq!(logs.contains("worker upstream uses plain http"), warns, "{logs}");
            assert_eq!(logs.contains("upstream=http://rpc.example.com:80"), warns, "{logs}");
            for secret in hosted_secrets {
                assert!(!logs.contains(secret), "`{secret}` leaked into: {logs}");
            }
        }
    }

    #[test]
    fn zero_poll_interval_is_rejected() {
        let message = match cli_with_flags(&["--readiness-poll-interval=0s"]).into_settings() {
            Ok(_) => panic!("a zero poll interval must fail startup, not panic the poller"),
            Err(err) => format!("{err:?}"),
        };
        assert!(message.contains("--readiness-poll-interval"), "{message}");
    }

    #[test]
    fn readiness_poll_timeout_above_interval_is_rejected() -> eyre::Result<()> {
        let message =
            match cli_with_flags(&["--readiness-poll-interval=1s", "--readiness-poll-timeout=3s"])
                .into_settings()
            {
                Ok(_) => panic!("a poll timeout longer than the interval must fail startup"),
                Err(err) => format!("{err:?}"),
            };
        for text in ["--readiness-poll-timeout (3s)", "--readiness-poll-interval (1s)"] {
            assert!(message.contains(text), "{message}");
        }
        // equal is harmless: a cycle then never outlasts the interval
        let settings =
            cli_with_flags(&["--readiness-poll-interval=1s", "--readiness-poll-timeout=1s"])
                .into_settings()?;
        assert_eq!(settings.readiness_poll_timeout, Duration::from_secs(1));
        Ok(())
    }

    #[test]
    fn zero_timeouts_are_rejected() -> eyre::Result<()> {
        for flag in [
            "--readiness-poll-timeout",
            "--upstream-connect-timeout",
            "--upstream-request-timeout",
            "--header-read-timeout",
        ] {
            let zero = format!("{flag}=0s");
            let message = match cli_with_flags(&[zero.as_str()]).into_settings() {
                Ok(_) => panic!("{zero} must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(flag), "{message}");
            // the smallest non-zero value is still accepted, the request
            // deadline with a connect timeout below it
            let tiny = format!("{flag}=1ms");
            let mut flags = vec![tiny.as_str()];
            if flag == "--upstream-request-timeout" {
                flags.push("--upstream-connect-timeout=1us");
            }
            cli_with_flags(&flags).into_settings()?;
        }
        Ok(())
    }

    #[test]
    fn duplicate_worker_ids_are_rejected() -> eyre::Result<()> {
        let config = |second_id: u16| -> eyre::Result<tempfile::NamedTempFile> {
            let mut file = tempfile::NamedTempFile::new()?;
            write!(
                file,
                r#"upstreams:
  - worker_id: 0
    rpc_url: "http://10.0.0.7:8545"
    readiness_url: "http://10.0.0.7:8551/health/workers"
  - worker_id: {second_id}
    rpc_url: "http://10.0.0.8:8545"
    readiness_url: "http://10.0.0.8:8551/health/workers"
"#
            )?;
            Ok(file)
        };
        let duplicate = config(0)?;
        let path = duplicate.path().display().to_string();
        let message = match cli_with(Some(&path), None, None).into_settings() {
            Ok(_) => panic!("two upstreams with worker id 0 must be rejected"),
            Err(err) => format!("{err:?}"),
        };
        assert!(message.contains("worker id 0"), "{message}");

        let distinct = config(1)?;
        let path = distinct.path().display().to_string();
        assert_eq!(cli_with(Some(&path), None, None).into_settings()?.upstreams.len(), 2);
        Ok(())
    }

    #[test]
    fn mapped_ipv6_self_url_is_rejected() -> eyre::Result<()> {
        let ready = "http://10.0.0.7:8551/health/workers";
        for (listen, rpc, readiness) in [
            ("127.0.0.1:8545", "http://[::ffff:127.0.0.1]:8545/", ready),
            ("127.0.0.1:8545", "http://10.0.0.7:8544", "http://[::ffff:127.0.0.1]:8545/health"),
            ("0.0.0.0:8545", "http://[::ffff:127.0.0.1]:8545/", ready),
            ("0.0.0.0:8545", "http://[::ffff:0.0.0.0]:8545/", ready),
        ] {
            let result = try_parse_test_argv([
                "worker-gateway".to_string(),
                format!("--listen-addr={listen}"),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={readiness}"),
            ])
            .expect("argv parses")
            .into_settings();
            assert!(result.is_err(), "{rpc} / {readiness} is the gateway on {listen}");
        }
        // a mapped address of another host, or on another port, is accepted
        for rpc in ["http://[::ffff:10.0.0.7]:8545/", "http://[::ffff:127.0.0.1]:8544/"] {
            try_parse_test_argv([
                "worker-gateway".to_string(),
                "--listen-addr=127.0.0.1:8545".to_string(),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={ready}"),
            ])
            .expect("argv parses")
            .into_settings()?;
        }
        Ok(())
    }

    /// The self check holds across address families: `[::]` binds
    /// dual-stack, so it takes IPv4 loopback connections too; `localhost`
    /// names both loopbacks; a mapped literal is the IPv4 address it names.
    /// An IPv4 listener does not take IPv6 connections, so `[::1]` on its port
    /// is another socket.
    #[test]
    fn self_url_is_rejected_across_address_families() -> eyre::Result<()> {
        let ready = "http://10.0.0.7:8551/health/workers";
        let settings = |listen: &str, rpc: &str, redirect: Option<&str>| {
            let mut argv = vec![
                "worker-gateway".to_string(),
                format!("--listen-addr={listen}"),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={ready}"),
            ];
            argv.extend(redirect.map(|url| format!("--redirect-queries={url}")));
            try_parse_test_argv(argv).expect("argv parses").into_settings()
        };
        for (listen, url) in [
            ("[::]:8545", "http://127.0.0.1:8545/"),
            ("[::]:8545", "http://127.0.0.2:8545/"),
            ("[::]:8545", "http://[::ffff:127.0.0.1]:8545/"),
            ("[::]:8545", "http://0.0.0.0:8545/"),
            ("[::]:8545", "http://[::1]:8545/"),
            ("[::]:8545", "http://localhost:8545/"),
            ("0.0.0.0:8545", "http://[::ffff:127.0.0.1]:8545/"),
            ("0.0.0.0:8545", "http://localhost:8545/"),
            ("[::1]:8545", "http://localhost:8545/"),
            ("127.0.0.1:8545", "http://localhost:8545/"),
        ] {
            let message = match settings(listen, url, None) {
                Ok(_) => panic!("{url} is the gateway on {listen}"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("own listen address"), "{message}");
            let message = match settings(listen, "http://10.0.0.7:8545", Some(url)) {
                Ok(_) => panic!("a redirect to {url} is the gateway on {listen}"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("own listen address"), "{message}");
        }
        // another socket: IPv6 beside an IPv4 listener, another loopback
        // family beside a specific one, or another port
        for (listen, url) in [
            ("0.0.0.0:8545", "http://[::1]:8545/"),
            ("0.0.0.0:8545", "http://[::]:8545/"),
            ("127.0.0.1:8545", "http://[::1]:8545/"),
            ("[::1]:8545", "http://127.0.0.1:8545/"),
            ("[::]:8545", "http://127.0.0.1:8544/"),
        ] {
            settings(listen, url, None)?;
            settings(listen, "http://10.0.0.7:8545", Some(url))?;
        }
        Ok(())
    }

    /// `localhost` names both `127.0.0.1` and `[::1]`, so a redirect to it is
    /// a worker on either one, and the other way round. Another explicit
    /// loopback address is not refused (see
    /// `distinct_loopback_redirect_at_a_workers_port_starts_with_a_warning`).
    #[test]
    fn localhost_redirect_matches_a_worker_on_either_loopback() -> eyre::Result<()> {
        let flags = |rpc: &str, redirect: &str| {
            try_parse_test_argv([
                "worker-gateway".to_string(),
                "--listen-addr=10.0.0.5:8545".to_string(),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={rpc}health/workers"),
                format!("--redirect-queries={redirect}"),
            ])
            .expect("argv parses")
            .into_settings()
        };
        for (rpc, redirect) in [
            ("http://[::1]:9545/", "http://localhost:9545/"),
            ("http://127.0.0.1:9545/", "http://localhost:9545/"),
            ("http://localhost:9545/", "http://[::1]:9545/"),
            ("http://localhost:9545/", "http://[::ffff:127.0.0.1]:9545/"),
        ] {
            let message = match flags(rpc, redirect) {
                Ok(_) => panic!("{redirect} is the worker at {rpc}"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("worker 0's RPC host and port"), "{message}");
        }
        // an address off this machine is another host to the check
        flags("http://[::1]:9545/", "http://10.0.0.9:9545/")?;
        Ok(())
    }

    /// Two explicit loopback addresses can be two sockets at one port, so a
    /// redirect to `[::1]` or `127.0.0.2` beside a worker on `127.0.0.1`, or the
    /// other way round, is not refused; it is still this machine, so startup
    /// warns.
    #[test]
    fn distinct_loopback_redirect_at_a_workers_port_starts_with_a_warning() -> eyre::Result<()> {
        const WARNING: &str = "--redirect-queries is on a worker's host at another port, or on \
                               another loopback address at its port";
        for (worker, redirect) in [
            ("127.0.0.1", "http://[::1]:8545/"),
            ("127.0.0.1", "http://127.0.0.2:8545/"),
            ("[::1]", "http://127.0.0.1:8545/"),
        ] {
            let cli = try_parse_test_argv([
                "worker-gateway".to_string(),
                "--listen-addr=10.0.0.5:8545".to_string(),
                format!("--upstream-rpc-url=http://{worker}:8545/"),
                format!("--upstream-readiness-url=http://{worker}:8551/health/workers"),
                format!("--redirect-queries={redirect}"),
            ])
            .expect("argv parses");
            let (settings, logs) = into_settings_logged(cli);
            let settings = settings.unwrap_or_else(|err| {
                panic!("{redirect} beside a worker on {worker}:8545 must start: {err:?}")
            });
            assert_eq!(settings.query_upstream, Some(Url::parse(redirect)?), "{redirect}");
            assert!(logs.contains(WARNING), "{redirect} beside {worker} must warn: {logs}");
            assert!(logs.contains("worker_id=0"), "{logs}");
        }
        Ok(())
    }

    /// Every spelling of a worker's own address stays refused: an IPv4-mapped
    /// literal, `localhost`, and an unspecified address, since a node bound to
    /// one answers on the loopback addresses.
    #[test]
    fn loopback_spellings_of_a_workers_address_stay_refused() {
        // the gateway listens on another port, so its own-address check, which
        // takes `0.0.0.0` beside an IPv4 listener, is not what trips
        let flags = |worker: &str, redirect: &str| {
            try_parse_test_argv([
                "worker-gateway".to_string(),
                "--listen-addr=10.0.0.5:9000".to_string(),
                format!("--upstream-rpc-url=http://{worker}:8545/"),
                format!("--upstream-readiness-url=http://{worker}:8551/health/workers"),
                format!("--redirect-queries={redirect}"),
            ])
            .expect("argv parses")
            .into_settings()
        };
        for (worker, redirect) in [
            ("127.0.0.1", "http://[::ffff:127.0.0.1]:8545/"),
            ("127.0.0.1", "http://0.0.0.0:8545/"),
            ("127.0.0.1", "http://[::]:8545/"),
            ("127.0.0.1", "http://localhost:8545/"),
            ("[::1]", "http://[::]:8545/"),
            ("[::1]", "http://0.0.0.0:8545/"),
            ("[::1]", "http://localhost:8545/"),
        ] {
            let message = match flags(worker, redirect) {
                Ok(_) => panic!("{redirect} is the worker on {worker}:8545"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("worker 0's RPC host and port"), "{redirect}: {message}");
        }
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
        try_parse_test_argv(argv).expect("argv parses")
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
    fn per_ip_max_entries_defaults_to_one_hundred_thousand() -> eyre::Result<()> {
        let default = cli_with_flags(&[]).into_settings()?;
        assert_eq!(default.rate_limit_per_ip_max_entries.get(), 100_000);
        let set = cli_with_flags(&["--rate-limit-per-ip-max-entries=2"]).into_settings()?;
        assert_eq!(set.rate_limit_per_ip_max_entries.get(), 2);
        Ok(())
    }

    /// The flag declares its `WORKER_GATEWAY_*` alias. Read from the parser's
    /// definition rather than by setting the variable, which would leak into
    /// every other test in the process.
    #[test]
    fn per_ip_max_entries_reads_its_env_alias() {
        use clap::CommandFactory as _;
        let command = Cli::command();
        let arg = command
            .get_arguments()
            .find(|arg| arg.get_long() == Some("rate-limit-per-ip-max-entries"))
            .expect("the flag is declared");
        assert_eq!(
            arg.get_env().and_then(|env| env.to_str()),
            Some("WORKER_GATEWAY_RATE_LIMIT_PER_IP_MAX_ENTRIES")
        );
    }

    #[test]
    fn per_ip_max_entries_rejects_zero() {
        let argv = [
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
            "--rate-limit-per-ip-max-entries=0",
        ];
        let err = try_parse_test_argv(argv).expect_err("a zero-entry table was accepted");
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    /// A per-IP table cap above 1,000,000 entries starts with a warning, since
    /// both idle sweeps walk the whole table under the lock every per-IP
    /// admission takes; the ceiling itself is quiet, and so is a cap the
    /// disabled per-IP limit ignores.
    #[test]
    fn per_ip_max_entries_above_a_million_warns() -> eyre::Result<()> {
        const WARNING: &str = "--rate-limit-per-ip-max-entries is above 1,000,000";
        let (settings, logs) =
            into_settings_logged(cli_with_flags(&["--rate-limit-per-ip-max-entries=1000001"]));
        settings?;
        assert!(logs.contains(WARNING), "a cap above the ceiling did not warn: {logs}");
        let (settings, logs) =
            into_settings_logged(cli_with_flags(&["--rate-limit-per-ip-max-entries=1000000"]));
        settings?;
        assert!(!logs.contains(WARNING), "the ceiling itself is quiet: {logs}");
        let (settings, logs) = into_settings_logged(cli_with_flags(&[
            "--rate-limit-per-ip-max-entries=1000001",
            "--rate-limit-per-ip=0",
        ]));
        settings?;
        assert!(!logs.contains(WARNING), "ignored with the per-IP limit off: {logs}");
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
    fn per_ip_prefix_zero_is_rejected() {
        for flag in ["--rate-limit-per-ip-v4-prefix", "--rate-limit-per-ip-v6-prefix"] {
            let arg = format!("{flag}=0");
            let err = cli_with_flags(&[arg.as_str()])
                .into_settings()
                .expect_err("a /0 per-IP prefix must fail startup");
            let message = err.to_string();
            assert!(message.contains(flag), "{message}");
            assert!(message.contains("does not disable masking"), "{message}");
        }
    }

    #[test]
    fn short_per_ip_prefix_warns() -> eyre::Result<()> {
        for (flag, short, floor) in
            [("--rate-limit-per-ip-v4-prefix", 8, 16), ("--rate-limit-per-ip-v6-prefix", 16, 32)]
        {
            let warning = format!("{flag} is /{short}: every client inside one /{short} shares");
            let arg = format!("{flag}={short}");
            let (settings, logs) = into_settings_logged(cli_with_flags(&[arg.as_str()]));
            settings?;
            assert!(logs.contains(&warning), "{flag}={short} must warn: {logs}");
            // the floor itself is quiet
            let arg = format!("{flag}={floor}");
            let (settings, logs) = into_settings_logged(cli_with_flags(&[arg.as_str()]));
            settings?;
            assert!(!logs.contains(flag), "{flag}={floor} must not warn: {logs}");
        }
        Ok(())
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
    fn trusted_ranges_covering_a_whole_family_warn_for_that_family() -> eyre::Result<()> {
        const IPV4: &str = "--trusted-proxies covers every IPv4 address, IPv4-mapped ones included";
        const IPV6: &str = "--trusted-proxies covers every native IPv6 address";
        for (list, warns_v4, warns_v6) in [
            ("0.0.0.0/0", true, false),
            // halves that add up to the whole family
            ("0.0.0.0/1,128.0.0.0/1", true, false),
            ("::ffff:0:0/96", true, false),
            // `::/0` covers native ipv6 only
            ("::/0", false, true),
            ("::/1,8000::/1,0.0.0.0/0", true, true),
            ("10.0.0.0/8,2001:db8::/32", false, false),
            ("0.0.0.0/1,::/0", false, true),
        ] {
            let flag = format!("--trusted-proxies={list}");
            let (settings, logs) = into_settings_logged(cli_with_flags(&[flag.as_str()]));
            settings?;
            assert_eq!(logs.contains(IPV4), warns_v4, "{list}: {logs}");
            assert_eq!(logs.contains(IPV6), warns_v6, "{list}: {logs}");
            assert_eq!(
                logs.matches("covers every").count(),
                usize::from(warns_v4) + usize::from(warns_v6),
                "{list}: {logs}"
            );
        }
        Ok(())
    }

    #[test]
    fn proxy_protocol_without_trusted_proxies_is_rejected_at_startup() -> eyre::Result<()> {
        assert!(cli_with_flags(&[]).into_settings()?.proxy_protocol.is_none(), "off by default");
        // a blank --proxy-protocol-from falls back to the (empty) trusted list
        for flags in [&["--proxy-protocol"][..], &["--proxy-protocol", "--proxy-protocol-from= "]] {
            let message = match cli_with_flags(flags).into_settings() {
                Ok(_) => panic!("{flags:?} with no peer to read the header from must fail startup"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("--trusted-proxies"), "{message}");
            assert!(message.contains("--proxy-protocol-from"), "{message}");
        }
        let settings = cli_with_flags(&["--proxy-protocol", "--trusted-proxies=10.0.0.0/8"])
            .into_settings()?;
        assert!(settings.proxy_protocol.is_some());
        Ok(())
    }

    #[test]
    fn proxy_protocol_from_defaults_to_the_trusted_proxies_and_is_kept_apart() -> eyre::Result<()> {
        let trusted = "--trusted-proxies=192.0.2.0/24";
        // unset or blank: the trusted proxies send the header
        for flags in [
            &["--proxy-protocol", trusted][..],
            &["--proxy-protocol", trusted, "--proxy-protocol-from="],
            &["--proxy-protocol", trusted, "--proxy-protocol-from= "],
        ] {
            let settings = cli_with_flags(flags).into_settings()?;
            assert_eq!(
                settings.proxy_protocol.as_ref(),
                Some(&settings.trusted_proxies),
                "{flags:?}"
            );
        }
        // set: its own list, and the trusted proxies stay as they are
        let settings = cli_with_flags(&[
            "--proxy-protocol",
            trusted,
            "--proxy-protocol-from=10.0.0.0/8, 2001:db8::/32",
        ])
        .into_settings()?;
        let senders = settings.proxy_protocol.expect("PROXY protocol on");
        assert_eq!(senders.len(), 2);
        assert!(senders.contains("10.1.2.3".parse()?));
        assert!(senders.contains("2001:db8::1".parse()?));
        assert!(!senders.contains("192.0.2.1".parse()?));
        assert_eq!(settings.trusted_proxies, "192.0.2.0/24".parse::<TrustedProxies>()?);
        // a sender list needs no trusted proxy at all
        let settings = cli_with_flags(&["--proxy-protocol", "--proxy-protocol-from=10.0.0.0/8"])
            .into_settings()?;
        assert!(settings.trusted_proxies.is_empty());
        assert_eq!(settings.proxy_protocol.map(|senders| senders.len()), Some(1));
        Ok(())
    }

    #[test]
    fn proxy_protocol_from_without_proxy_protocol_or_invalid_is_rejected() {
        for (flags, expected) in [
            (
                &["--trusted-proxies=10.0.0.0/8", "--proxy-protocol-from=10.0.0.0/8"][..],
                "--proxy-protocol-from requires --proxy-protocol",
            ),
            (
                &["--proxy-protocol", "--proxy-protocol-from=10.0.0.1/8"],
                "invalid --proxy-protocol-from",
            ),
            (
                &["--proxy-protocol", "--proxy-protocol-from=10.0.0.0/8,,"],
                "invalid --proxy-protocol-from",
            ),
        ] {
            let message = match cli_with_flags(flags).into_settings() {
                Ok(_) => panic!("{flags:?} must fail startup"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(expected), "{flags:?}: {message}");
        }
    }

    #[test]
    fn proxy_protocol_from_covering_a_whole_family_warns_once() -> eyre::Result<()> {
        const SENDERS: &str = "--proxy-protocol-from covers every IPv4 address";
        const TRUSTED: &str = "--trusted-proxies covers every IPv4 address";
        let (settings, logs) = into_settings_logged(cli_with_flags(&[
            "--proxy-protocol",
            "--proxy-protocol-from=0.0.0.0/1,128.0.0.0/1",
        ]));
        settings?;
        assert_eq!(logs.matches(SENDERS).count(), 1, "{logs}");
        assert!(!logs.contains(TRUSTED), "{logs}");
        // by default the senders are the trusted proxies, warned about once
        let (settings, logs) = into_settings_logged(cli_with_flags(&[
            "--proxy-protocol",
            "--trusted-proxies=0.0.0.0/0",
        ]));
        settings?;
        assert_eq!(logs.matches(TRUSTED).count(), 1, "{logs}");
        assert!(!logs.contains(SENDERS), "{logs}");
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
        // slot, and they have their own share, so the cap is kept as given
        let redirect = "--redirect-queries=https://rpc.example.com/";
        assert_eq!(query_cap(&[redirect])?, 256);
        assert_eq!(query_cap(&[redirect, "--max-inflight-queries=0"])?, 0);
        Ok(())
    }

    #[test]
    fn reads_kept_on_the_worker_hold_at_most_a_fifth_of_its_slots() -> eyre::Result<()> {
        let pool_read_cap = |flags: &[&str]| -> eyre::Result<usize> {
            Ok(cli_with_flags(flags).into_settings()?.max_worker_pool_reads)
        };
        let redirect = "--redirect-queries=https://rpc.example.com/";
        // the default worker cap of 100 lets them hold 20 slots
        assert_eq!(pool_read_cap(&[redirect])?, 20);
        assert_eq!(pool_read_cap(&[redirect, "--max-upstream-inflight=10"])?, 2);
        // a small worker cap still lets one through
        for cap in ["1", "4", "9"] {
            let flag = format!("--max-upstream-inflight={cap}");
            assert_eq!(pool_read_cap(&[redirect, &flag])?, 1, "{flag}");
        }
        // an unlimited worker cap leaves them unlimited too
        let settings = cli_with_flags(&[redirect, "--max-upstream-inflight=0"]).into_settings()?;
        assert_eq!(settings.max_worker_pool_reads, 0);
        assert_eq!(
            crate::server::inflight_slots(settings.max_worker_pool_reads).available_permits(),
            tokio::sync::Semaphore::MAX_PERMITS
        );
        // the share follows the worker cap, never the query cap
        assert_eq!(pool_read_cap(&[redirect, "--max-inflight-queries=4"])?, 20);
        Ok(())
    }

    #[test]
    fn submission_reserve_of_zero_warns() -> eyre::Result<()> {
        const WARNING: &str = "no worker slot stays free for submissions";
        let redirect = "--redirect-queries=https://rpc.example.com/";
        for flags in [&["--max-upstream-inflight=1"][..], &["--max-upstream-inflight=1", redirect]]
        {
            let (settings, logs) = into_settings_logged(cli_with_flags(flags));
            settings?;
            assert!(logs.contains(WARNING), "{flags:?}: {logs}");
            assert!(logs.contains("max_upstream_inflight=1"), "{flags:?}: {logs}");
        }
        for flags in
            [&[][..], &[redirect], &["--max-upstream-inflight=2"], &["--max-upstream-inflight=0"]]
        {
            let (settings, logs) = into_settings_logged(cli_with_flags(flags));
            settings?;
            assert!(!logs.contains(WARNING), "{flags:?}: {logs}");
        }
        Ok(())
    }

    #[test]
    fn submission_reserve_is_the_worker_cap_less_the_query_cap() -> eyre::Result<()> {
        let reserve = |flags: &[&str]| -> eyre::Result<Option<usize>> {
            let settings = cli_with_flags(flags).into_settings()?;
            let worker_bound_reads = if settings.query_upstream.is_some() {
                settings.max_worker_pool_reads
            } else {
                settings.max_inflight_queries
            };
            Ok(submission_reserve(settings.max_upstream_inflight, worker_bound_reads))
        };
        // the defaults hold back a fifth of the 100 worker slots
        assert_eq!(reserve(&[])?, Some(20));
        assert_eq!(reserve(&["--max-upstream-inflight=5"])?, Some(1));
        assert_eq!(reserve(&["--max-inflight-queries=50"])?, Some(50));
        // a worker cap of 1 cannot spare a slot, and an unlimited one needs none
        assert_eq!(reserve(&["--max-upstream-inflight=1"])?, Some(0));
        assert_eq!(reserve(&["--max-upstream-inflight=0"])?, None);
        // with a redirect only the reads kept on the worker take a worker
        // slot, up to their fifth, whatever the query cap says
        let redirect = "--redirect-queries=https://rpc.example.com/";
        assert_eq!(reserve(&[redirect])?, Some(80));
        assert_eq!(reserve(&[redirect, "--max-inflight-queries=0"])?, Some(80));
        assert_eq!(reserve(&[redirect, "--max-inflight-queries=40"])?, Some(80));
        assert_eq!(reserve(&[redirect, "--max-upstream-inflight=10"])?, Some(8));
        assert_eq!(reserve(&[redirect, "--max-upstream-inflight=0"])?, None);
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
    fn rate_out_of_range_is_rejected_at_startup() -> eyre::Result<()> {
        for rate in ["-0.1", "1.01", "2", "NaN", "inf"] {
            let result = cli_with_flags(&[&format!("--access-log-sample={rate}")]).into_settings();
            assert!(result.is_err(), "--access-log-sample={rate} must fail startup");
        }
        // the access log is off unless asked for, and both ends of the range
        // are accepted
        assert!(cli_with_flags(&[]).into_settings()?.access_log_sample.abs() < f64::EPSILON);
        for (flag, rate) in [("0", 0.0), ("0.25", 0.25), ("1", 1.0)] {
            let settings =
                cli_with_flags(&[&format!("--access-log-sample={flag}")]).into_settings()?;
            assert!((settings.access_log_sample - rate).abs() < f64::EPSILON, "{flag}");
        }
        Ok(())
    }

    #[test]
    fn access_log_max_lines_per_second_parses() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        assert_eq!(settings.access_log_max_lines_per_second, NonZeroU32::new(1000));
        let settings = cli_with_flags(&["--access-log-max-lines-per-second=0"]).into_settings()?;
        assert_eq!(settings.access_log_max_lines_per_second, None, "0 is unlimited");
        let settings = cli_with_flags(&["--access-log-max-lines-per-second=5"]).into_settings()?;
        assert_eq!(settings.access_log_max_lines_per_second, NonZeroU32::new(5));
        Ok(())
    }

    #[test]
    fn max_request_bytes_is_configurable() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--max-request-bytes=1024"]).into_settings()?;
        assert_eq!(settings.max_request_bytes, 1_024);
        Ok(())
    }

    #[test]
    fn graceful_shutdown_defaults_to_the_request_deadline() -> eyre::Result<()> {
        let settings = cli_with_flags(&[]).into_settings()?;
        assert_eq!(settings.graceful_shutdown_timeout, Duration::from_secs(45));
        let settings =
            cli_with_flags(&["--upstream-request-timeout=20s", "--header-read-timeout=5s"])
                .into_settings()?;
        assert_eq!(settings.graceful_shutdown_timeout, Duration::from_secs(25));
        Ok(())
    }

    #[test]
    fn graceful_shutdown_explicit_value_is_kept_and_warns_below_the_deadline() -> eyre::Result<()> {
        const WARNING: &str = "--graceful-shutdown-timeout is shorter than the request deadline";
        let (settings, logs) =
            into_settings_logged(cli_with_flags(&["--graceful-shutdown-timeout=30s"]));
        assert_eq!(settings?.graceful_shutdown_timeout, Duration::from_secs(30));
        assert!(logs.contains(WARNING), "{logs}");
        assert!(logs.contains("request_deadline=45s"), "{logs}");
        let (settings, logs) =
            into_settings_logged(cli_with_flags(&["--graceful-shutdown-timeout=60s"]));
        assert_eq!(settings?.graceful_shutdown_timeout, Duration::from_secs(60));
        assert!(!logs.contains(WARNING), "{logs}");
        // the derived default is the deadline itself, so it never warns
        let (settings, logs) = into_settings_logged(cli_with_flags(&[]));
        settings?;
        assert!(!logs.contains(WARNING), "{logs}");
        Ok(())
    }

    #[test]
    fn max_request_bytes_zero_is_rejected() {
        // `0` would refuse every body, so clap refuses it rather than letting
        // it read as "unlimited" the way the sibling caps do
        let err = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
            "--max-request-bytes=0",
        ])
        .expect_err("--max-request-bytes 0 must not parse");
        assert!(err.to_string().contains("--max-request-bytes"), "{err}");
    }

    #[test]
    fn max_request_bytes_below_the_submission_floor_warns() -> eyre::Result<()> {
        const WARNING: &str = "--max-request-bytes is below one hex-encoded 128 KiB transaction";
        let (settings, logs) = into_settings_logged(cli_with_flags(&["--max-request-bytes=1024"]));
        assert_eq!(settings?.max_request_bytes, 1_024);
        assert!(logs.contains(WARNING), "{logs}");
        assert!(logs.contains("max_request_bytes=1024"), "{logs}");
        assert!(logs.contains("floor=263168"), "{logs}");
        let (settings, logs) = into_settings_logged(cli_with_flags(&[]));
        settings?;
        assert!(!logs.contains(WARNING), "the default must not warn: {logs}");
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
    /// its burst, so the effective cap stops at the smallest per-IP or global
    /// burst: a longer batch gets the final `413`, not a `429` no retry can
    /// satisfy.
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
        let result = try_parse_test_argv([
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
        let past_u32 = try_parse_test_argv([
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
    fn denied_method_prefixes_default_trim_and_empty() -> eyre::Result<()> {
        let default = cli_with_flags(&[]).into_settings()?.denied_method_prefixes;
        assert_eq!(default, ["tn_", "debug_", "trace_", "admin_"]);
        let trimmed = cli_with_flags(&["--denied-method-prefixes=tn_, debug_ ,,"])
            .into_settings()?
            .denied_method_prefixes;
        assert_eq!(trimmed, ["tn_", "debug_"]);
        let empty =
            cli_with_flags(&["--denied-method-prefixes="]).into_settings()?.denied_method_prefixes;
        assert!(empty.is_empty(), "an empty value must allow everything: {empty:?}");
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
        cli_with_flags(&["--upstream-request-timeout=5s", "--query-request-timeout=6s"])
            .into_settings()?;
        Ok(())
    }

    /// The default deadline follows a shorter worker deadline instead of
    /// failing startup, and stays 10 s under a longer one.
    #[test]
    fn default_query_timeout_is_clamped_to_the_upstream_timeout() -> eyre::Result<()> {
        let redirect = "--redirect-queries=https://rpc.example.com/";
        let settings =
            cli_with_flags(&[redirect, "--upstream-request-timeout=5s"]).into_settings()?;
        assert_eq!(settings.query_request_timeout, Some(Duration::from_secs(5)));
        let settings =
            cli_with_flags(&[redirect, "--upstream-request-timeout=10s"]).into_settings()?;
        assert_eq!(settings.query_request_timeout, Some(Duration::from_secs(10)));
        let settings =
            cli_with_flags(&[redirect, "--upstream-request-timeout=60s"]).into_settings()?;
        assert_eq!(settings.query_request_timeout, Some(Duration::from_secs(10)));
        Ok(())
    }

    /// The request deadline covers connection setup, so one that is not longer
    /// than the connect timeout ends a stalled connect first, as a timeout that
    /// passive health cannot tell from a slow answer.
    #[test]
    fn request_timeout_not_above_connect_timeout_is_rejected_at_startup() -> eyre::Result<()> {
        for request in ["100ms", "2s"] {
            let flag = format!("--upstream-request-timeout={request}");
            let err = cli_with_flags(&[flag.as_str(), "--upstream-connect-timeout=2s"])
                .into_settings()
                .expect_err("a request deadline that does not outlast the connect timeout");
            let message = err.to_string();
            assert!(
                message.contains("--upstream-request-timeout")
                    && message.contains("--upstream-connect-timeout"),
                "{message}"
            );
            // the message names the flags and their values, never an upstream
            assert!(!message.contains("10.0.0.7"), "{message}");
        }

        // the defaults, and a deadline just above the connect timeout, pass
        let settings = cli_with_flags(&[]).into_settings()?;
        assert!(settings.upstream_request_timeout > settings.upstream_connect_timeout);
        let settings =
            cli_with_flags(&["--upstream-request-timeout=2001ms", "--upstream-connect-timeout=2s"])
                .into_settings()?;
        assert_eq!(settings.upstream_request_timeout, Duration::from_millis(2001));
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

    /// clap no longer parses `--redirect-queries`, so it cannot echo a value
    /// it rejects; the startup error says what is wrong without the value.
    #[test]
    fn bad_redirect_value_is_not_echoed() {
        for (value, problem) in [
            ("https://user:s3cr3t@rpc.example.com:99999/k3y?token=t0k3n", "invalid port number"),
            ("rpc.example.com/k3y?token=t0k3n", "relative URL without a base"),
            ("http://[::1/k3y?token=t0k3n", "invalid IPv6 address"),
            ("http://user:s3cr3t@/k3y?token=t0k3n", "empty host"),
            ("user:s3cr3t@rpc.example.com/k3y?token=t0k3n", "unsupported URL scheme"),
        ] {
            let cli = try_parse_test_argv([
                "worker-gateway".to_string(),
                "--upstream-rpc-url=http://10.0.0.7:8545".to_string(),
                "--upstream-readiness-url=http://10.0.0.7:8551/health/workers".to_string(),
                format!("--redirect-queries={value}"),
            ])
            .unwrap_or_else(|err| panic!("clap must take any value; it rejected one: {err}"));
            let message = match cli.into_settings() {
                Ok(_) => panic!("{value} must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("--redirect-queries"), "{message}");
            assert!(message.contains(problem), "`{problem}` missing from: {message}");
            for secret in ["user", "s3cr3t", "k3y", "t0k3n", "rpc.example.com"] {
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
    fn redirect_on_a_readiness_host_and_port_is_rejected() {
        // the worker in `cli_with_flags` serves readiness on 10.0.0.7:8551
        for url in [
            "http://10.0.0.7:8551/",
            "http://10.0.0.7:8551/k3y?token=t0k3n",
            "https://10.0.0.7:8551/",
        ] {
            let flag = format!("--redirect-queries={url}");
            let message = match cli_with_flags(&[flag.as_str()]).into_settings() {
                Ok(_) => panic!("{url} is the worker's readiness endpoint and must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("worker 0's readiness host and port"), "{message}");
            for secret in ["k3y", "t0k3n"] {
                assert!(!message.contains(secret), "`{secret}` leaked into: {message}");
            }
        }
    }

    /// Collects everything a fmt subscriber writes.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("capture lock").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Resolve `cli` while recording the gateway's log lines, and return the
    /// settings with what was logged.
    fn into_settings_logged(cli: Cli) -> (eyre::Result<Settings>, String) {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter("gateway=debug")
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let settings = tracing::subscriber::with_default(subscriber, || cli.into_settings());
        let logs = String::from_utf8(captured.0.lock().expect("capture lock").clone())
            .expect("utf-8 logs");
        (settings, logs)
    }

    #[test]
    fn redirect_on_a_worker_host_with_another_port_warns() -> eyre::Result<()> {
        const WARNING: &str = "--redirect-queries is on a worker's host at another port";
        // the worker in `cli_with_flags` is 10.0.0.7 (RPC 8545, readiness 8551)
        for url in [
            "http://10.0.0.7:9545/k3y?token=t0k3n",
            "https://10.0.0.7/k3y?token=t0k3n",
            "http://[::ffff:10.0.0.7]:9545/k3y?token=t0k3n",
        ] {
            let flag = format!("--redirect-queries={url}");
            let (settings, logs) = into_settings_logged(cli_with_flags(&[flag.as_str()]));
            settings?;
            assert!(logs.contains(WARNING), "{url} must warn: {logs}");
            assert!(logs.contains("worker_id=0"), "{logs}");
            for secret in ["k3y", "t0k3n"] {
                assert!(!logs.contains(secret), "`{secret}` leaked into: {logs}");
            }
        }
        // another host does not warn
        let (settings, logs) =
            into_settings_logged(cli_with_flags(&["--redirect-queries=http://10.0.0.9:8545/"]));
        settings?;
        assert!(!logs.contains(WARNING), "{logs}");
        Ok(())
    }

    #[test]
    fn mapped_ipv6_redirect_matches_an_ipv4_worker() {
        // the worker in `cli_with_flags` is 10.0.0.7 (RPC 8545, readiness 8551)
        for url in ["http://[::ffff:10.0.0.7]:8545/", "http://[::ffff:10.0.0.7]:8551/"] {
            let flag = format!("--redirect-queries={url}");
            let result = cli_with_flags(&[flag.as_str()]).into_settings();
            assert!(result.is_err(), "{url} is the worker and must be rejected");
        }
        // and the other way round: a worker written as a mapped literal
        let result = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://[::ffff:10.0.0.7]:8545",
            "--upstream-readiness-url=http://[::ffff:10.0.0.7]:8551/health/workers",
            "--redirect-queries=http://10.0.0.7:8545/",
        ])
        .expect("argv parses")
        .into_settings();
        assert!(result.is_err(), "10.0.0.7:8545 is the mapped worker and must be rejected");
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
            ("http://[::ffff:10.0.0.7]:8545/", false),
            ("http://[::ffff:127.0.0.1]:8545/", false),
            ("http://[::ffff:203.0.113.5]:8545/", true),
        ] {
            assert_eq!(plaintext_to_public_host(&Url::parse(url)?), warns, "{url}");
        }
        Ok(())
    }

    /// Carrier-grade NAT (`100.64.0.0/10`) and link-local addresses are not
    /// routable from the internet either, so a plain `http` hop to one draws
    /// no warning; the addresses just outside each range still do.
    #[test]
    fn plaintext_warning_spares_cgnat_and_link_local_hosts() -> eyre::Result<()> {
        for (url, warns) in [
            // 100.64.0.0/10
            ("http://100.64.0.1:8545/", false),
            ("http://100.100.1.2:8545/", false),
            ("http://100.127.255.254:8545/", false),
            ("http://[::ffff:100.64.0.1]:8545/", false),
            ("http://100.63.255.255:8545/", true),
            ("http://100.128.0.1:8545/", true),
            // 169.254.0.0/16
            ("http://169.254.0.1:8545/", false),
            ("http://169.254.255.254:8545/", false),
            ("http://169.253.255.255:8545/", true),
            ("http://169.255.0.1:8545/", true),
            // fe80::/10
            ("http://[fe80::1]:8545/", false),
            ("http://[febf:ffff::1]:8545/", false),
            ("http://[fe7f::1]:8545/", true),
            ("http://[fec0::1]:8545/", true),
        ] {
            assert_eq!(plaintext_to_public_host(&Url::parse(url)?), warns, "{url}");
        }
        // and at startup, for a worker URL
        let (settings, logs) = into_settings_logged(cli_with(
            None,
            Some("http://100.64.0.7:8545"),
            Some("http://[fe80::7]:8551/health/workers"),
        ));
        settings?;
        assert!(!logs.contains("worker upstream uses plain http"), "{logs}");
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

    #[test]
    fn cors_origins_parse_to_the_form_browsers_send() -> eyre::Result<()> {
        assert_eq!(cli_with_flags(&[]).into_settings()?.cors_allowed_origins, None);

        let any = cli_with_flags(&["--cors-allowed-origins= * "]).into_settings()?;
        assert_eq!(any.cors_allowed_origins, Some(CorsOrigins::Any));

        // lowercase, punycode, no default port: what a browser puts in `Origin`
        let list = cli_with_flags(&[
            "--cors-allowed-origins=https://App.Example.com:443, http://127.0.0.1:3000,\
             http://[::1]:8080,https://b\u{fc}cher.example",
        ])
        .into_settings()?;
        let expected = [
            "https://app.example.com",
            "http://127.0.0.1:3000",
            "http://[::1]:8080",
            "https://xn--bcher-kva.example",
        ];
        assert_eq!(
            list.cors_allowed_origins,
            Some(CorsOrigins::List(expected.into_iter().map(HeaderValue::from_static).collect()))
        );
        Ok(())
    }

    #[test]
    fn invalid_origin_is_rejected_at_startup() {
        // each bad entry sits at the given 1-based position behind good ones
        let good = "https://ok.example";
        for (list, position) in [
            ("https://bad.example/".to_string(), 1),
            (format!("{good},https://bad.example/path"), 2),
            (format!("{good},{good},https://bad.example?key=1"), 3),
            (format!("{good},https://bad.example#frag"), 2),
            (format!("{good},https://user:pw@bad.example"), 2),
            (format!("{good},https://bad.example\\path"), 2),
            (format!("{good},https://bad.example\\"), 2),
            (format!("{good},https://\\bad.example"), 2),
            (format!("{good},https://bad.ex\tample"), 2),
            (format!("{good},ftp://bad.example"), 2),
            (format!("{good},wss://bad.example"), 2),
            (format!("{good},bad.example"), 2),
            (format!("{good},bad.example:8080"), 2),
            (format!("{good},null"), 2),
            (format!("{good},https://"), 2),
            (format!("{good},,{good}"), 2),
            (format!("{good},"), 2),
            (String::new(), 1),
            (format!("{good},*"), 2),
            (format!("*,{good}"), 1),
        ] {
            let flag = format!("--cors-allowed-origins={list}");
            let message = match cli_with_flags(&[flag.as_str()]).into_settings() {
                Ok(_) => panic!("`{list}` must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(&format!("entry {position} ")), "`{list}`: {message}");
            // the error names the position, never the offending text
            for text in ["bad.example", "ok.example", "user:pw"] {
                assert!(!message.contains(text), "`{text}` leaked into: {message}");
            }
        }
    }

    #[test]
    fn unknown_log_format_is_rejected_at_startup() -> eyre::Result<()> {
        let argv = |format: Option<&str>| {
            let mut argv = vec![
                "worker-gateway".to_string(),
                "--upstream-rpc-url=http://10.0.0.7:8545".to_string(),
                "--upstream-readiness-url=http://10.0.0.7:8551/health/workers".to_string(),
            ];
            argv.extend(format.map(|format| format!("--log-format={format}")));
            argv
        };

        for unknown in ["yaml", "JSON", "logfmt", ""] {
            let err = try_parse_test_argv(argv(Some(unknown)))
                .expect_err("an unknown --log-format must fail to parse");
            assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue, "{unknown:?}");
        }

        assert_eq!(try_parse_test_argv(argv(None))?.log_format, LogFormat::Text);
        assert_eq!(try_parse_test_argv(argv(Some("text")))?.log_format, LogFormat::Text);
        assert_eq!(try_parse_test_argv(argv(Some("json")))?.log_format, LogFormat::Json);
        Ok(())
    }

    #[test]
    fn redirect_to_this_machine_by_another_spelling_is_caught() -> eyre::Result<()> {
        // off the default listen port 8545, so the gateway self-pointing check
        // is not what trips
        let worker = |redirect: &str| {
            try_parse_test_argv([
                "worker-gateway".to_string(),
                "--upstream-rpc-url=http://127.0.0.1:8544".to_string(),
                "--upstream-readiness-url=http://127.0.0.1:8551/health/workers".to_string(),
                format!("--redirect-queries={redirect}"),
            ])
            .expect("argv parses")
        };
        for redirect in ["http://0.0.0.0:8544", "http://[::]:8544"] {
            let message = match worker(redirect).into_settings() {
                Ok(_) => panic!("{redirect} reaches the worker on 127.0.0.1:8544"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains("is worker 0's RPC host and port"), "{redirect}: {message}");
        }
        // another loopback address at the worker's port can be another node's
        // socket, as can another port, so these only warn
        for redirect in ["http://[::1]:8544", "http://127.0.0.2:8544", "http://[::1]:9545"] {
            let (settings, logs) = into_settings_logged(worker(redirect));
            settings?;
            assert!(
                logs.contains("--redirect-queries is on a worker's host at another port"),
                "{redirect}: {logs}"
            );
        }
        Ok(())
    }

    #[test]
    fn probe_addr_mapped_loopback_collides() {
        let message = match try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
            "--listen-addr=0.0.0.0:9000",
            "--probe-addr=[::ffff:127.0.0.1]:9000",
        ])
        .expect("argv parses")
        .into_settings()
        {
            Ok(_) => panic!("a mapped loopback probe address on the listen port must collide"),
            Err(err) => format!("{err:?}"),
        };
        assert!(message.contains("collides with --listen-addr"), "{message}");
    }

    #[test]
    fn cli_debug_redacts_urls_and_header() -> eyre::Result<()> {
        let cli = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=https://gw:s3cret@worker-0.internal:8545/rpc-path?key=qsecret1",
            "--upstream-readiness-url=https://gw:s3cret@worker-0.internal/ready-path?key=qsecret2",
            "--redirect-queries=https://user:pw0rd@rpc.example.com/v2/apikey123?key=qsecret3",
            "--redirect-queries-header=X-Api-Key: hdrsecret",
        ])?;
        let debug = format!("{cli:?}");
        for shown in [
            "https://worker-0.internal:8545",
            "https://worker-0.internal:443",
            "https://rpc.example.com:443",
            "<redacted>",
        ] {
            assert!(debug.contains(shown), "`{shown}` missing from: {debug}");
        }
        for secret in [
            "s3cret",
            "pw0rd",
            "rpc-path",
            "ready-path",
            "apikey123",
            "qsecret",
            "hdrsecret",
            "X-Api-Key",
        ] {
            assert!(!debug.contains(secret), "`{secret}` leaked into: {debug}");
        }
        // a flag the impl does not name is left out, never printed
        assert!(!debug.contains("log_filter"), "{debug}");

        // a value that does not parse is not printed either
        let cli = try_parse_test_argv([
            "worker-gateway",
            "--upstream-rpc-url=https://gw:s3cret@worker-0.internal:85x45/",
        ])?;
        let debug = format!("{cli:?}");
        assert!(debug.contains("<invalid url>"), "{debug}");
        assert!(!debug.contains("s3cret"), "{debug}");
        Ok(())
    }

    #[test]
    fn malformed_worker_url_is_not_echoed() {
        // a credential in the userinfo and a typo in the port
        const URL: &str = "https://gw:s3cret@worker-0.internal:85x45/";
        let rpc = "--upstream-rpc-url=http://10.0.0.7:8545".to_string();
        let readiness = "--upstream-readiness-url=http://10.0.0.7:8551/health/workers".to_string();
        for (flag, argv) in [
            ("--upstream-rpc-url", [format!("--upstream-rpc-url={URL}"), readiness]),
            ("--upstream-readiness-url", [rpc, format!("--upstream-readiness-url={URL}")]),
        ] {
            let cli =
                try_parse_test_argv(std::iter::once("worker-gateway".to_string()).chain(argv))
                    .expect("clap takes the worker URL as a string");
            let message = match cli.into_settings() {
                Ok(_) => panic!("{flag}: a malformed URL must be rejected"),
                Err(err) => format!("{err:?}"),
            };
            assert!(message.contains(&format!("{flag} is not a valid URL")), "{message}");
            for leak in ["s3cret", "worker-0.internal:85x45"] {
                assert!(!message.contains(leak), "`{leak}` leaked into: {message}");
            }
        }
    }

    #[test]
    fn url_env_values_are_hidden_from_help() {
        use clap::CommandFactory as _;

        let command = Cli::command();
        for id in [
            "upstream_rpc_url",
            "upstream_readiness_url",
            "redirect_queries",
            "redirect_queries_header",
        ] {
            let arg = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .unwrap_or_else(|| panic!("no argument `{id}`"));
            assert!(arg.is_hide_env_values_set(), "`{id}` must hide its env value in --help");
        }
    }

    #[test]
    fn unquoted_header_value_is_not_echoed() {
        let err = try_parse_test_argv([
            "worker-gateway",
            "--redirect-queries=https://rpc.example",
            "--redirect-queries-header",
            "X-Api-Key:",
            "abc123",
        ])
        .expect_err("the stray half of an unquoted header line must fail to parse");
        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        // control: clap's own message quotes the stray token
        assert!(err.to_string().contains("abc123"), "{err}");

        let message = redacted_parse_error(&err).expect("a stray token is redacted");
        assert!(!message.contains("abc123"), "the header value leaked into: {message}");
        assert!(message.contains("--redirect-queries-header"), "{message}");
    }

    #[test]
    fn value_errors_are_redacted() {
        use clap::error::ErrorKind;

        for (argv, kind, flag) in [
            (
                ["worker-gateway", "--max-request-bytes=s3cret"],
                ErrorKind::ValueValidation,
                "--max-request-bytes",
            ),
            (["worker-gateway", "--log-format=s3cret"], ErrorKind::InvalidValue, "--log-format"),
        ] {
            let err = try_parse_test_argv(argv).expect_err("an unparsable value must fail");
            assert_eq!(err.kind(), kind, "{flag}");
            // control: clap's own message quotes the value
            assert!(err.to_string().contains("s3cret"), "{err}");

            let message = redacted_parse_error(&err).expect("a value error is redacted");
            assert!(message.contains(flag), "{message}");
            assert!(!message.contains("s3cret"), "the value leaked into: {message}");
        }
    }

    #[test]
    fn help_and_version_are_left_to_clap() {
        for flag in ["--help", "--version"] {
            let err = try_parse_test_argv(["worker-gateway", flag])
                .expect_err("help and version stop the parse");
            assert_eq!(redacted_parse_error(&err), None, "{flag}");
        }
        // a conflict carries only flag names, so clap prints it as is
        let err = try_parse_test_argv([
            "worker-gateway",
            "--config=gateway.yaml",
            "--upstream-rpc-url=http://10.0.0.7:8545",
        ])
        .expect_err("--config conflicts with the inline upstream flags");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        assert_eq!(redacted_parse_error(&err), None);
    }
}
