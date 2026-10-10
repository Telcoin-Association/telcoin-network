//! Command-line interface and resolved runtime settings.

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use tracing::warn;
use url::Url;

use crate::{
    config::{GatewayConfig, UpstreamWorker},
    proxy::UpstreamOrigin,
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

    /// Path to a YAML file listing the upstream workers. Mutually exclusive
    /// with the inline `--upstream-*` flags; provide one source or the other.
    #[arg(
        long,
        env = "WORKER_GATEWAY_CONFIG",
        conflicts_with_all = ["upstream_rpc_url", "upstream_readiness_url", "worker_id"]
    )]
    pub(crate) config: Option<PathBuf>,

    /// Inline single upstream: the worker JSON-RPC base URL
    /// (e.g. `http://127.0.0.1:8545`).
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_RPC_URL")]
    pub(crate) upstream_rpc_url: Option<Url>,

    /// Inline single upstream: the node readiness URL
    /// (e.g. `http://127.0.0.1:8551/health/workers`).
    #[arg(long, env = "WORKER_GATEWAY_UPSTREAM_READINESS_URL")]
    pub(crate) upstream_readiness_url: Option<Url>,

    /// Inline single upstream: the worker id reported by the readiness endpoint.
    #[arg(long, env = "WORKER_GATEWAY_WORKER_ID", default_value_t = 0)]
    pub(crate) worker_id: u16,

    /// JSON-RPC endpoint (`http` or `https`) that serves every call except
    /// transaction submissions, typically a public RPC. When set, only
    /// `eth_sendRawTransaction` and `eth_sendRawTransactionSync`, alone or in a
    /// batch made only of them, reach the worker; everything else, including a
    /// batch that mixes submissions with other calls, goes here, with no
    /// readiness gate and no fallback to the worker. Must not point at the
    /// gateway itself or at a worker's RPC or readiness host and port.
    #[arg(long, env = "WORKER_GATEWAY_REDIRECT_QUERIES")]
    pub(crate) redirect_queries: Option<String>,

    /// How often to poll each upstream's readiness endpoint.
    #[arg(
        long,
        env = "WORKER_GATEWAY_READINESS_POLL_INTERVAL",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) readiness_poll_interval: Duration,

    /// Per-poll timeout for the readiness endpoint (a slow or failed poll marks
    /// the upstream not-ready).
    #[arg(
        long,
        env = "WORKER_GATEWAY_READINESS_POLL_TIMEOUT",
        default_value = "2s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) readiness_poll_timeout: Duration,

    /// Connect timeout when forwarding a request to an upstream: a worker, or the
    /// `--redirect-queries` endpoint.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT",
        default_value = "2s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_connect_timeout: Duration,

    /// Overall per-request deadline when forwarding to an upstream: a worker, or
    /// the `--redirect-queries` endpoint.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_request_timeout: Duration,

    /// How long a new connection may take to send its complete request headers
    /// before it is disconnected (slow-loris guard).
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

    /// Hard cap on a single inbound connection's total lifetime, keep-alive
    /// sessions included. The cap fires independent of connection progress, so
    /// it also bounds a client that trickles reads slowly enough to keep the
    /// transport-stall guard from firing. The close is abrupt: an exchange
    /// still in flight when a long-lived keep-alive session hits the cap is
    /// cut off mid-stream, so size the cap well above the longest legitimate
    /// transfer. Must be at least the gateway's own single-request bound
    /// (`--header-read-timeout` plus the whole-request deadline) so the first
    /// request on a connection can never be cut off. `0` disables.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_CONNECTION_DURATION",
        default_value = "10m",
        value_parser = humantime::parse_duration
    )]
    pub(crate) max_connection_duration: Duration,

    /// Maximum request body the gateway will accept, in bytes (default 1 MiB).
    /// Requests whose body exceeds this are rejected with a JSON-RPC "request
    /// too large" error before being forwarded. Each open connection can buffer
    /// one body this large, so keep `--max-connections` times this value well
    /// under the process memory limit.
    #[arg(
        long,
        env = "WORKER_GATEWAY_MAX_REQUEST_BYTES",
        default_value_t = crate::proxy::MAX_REQUEST_BYTES
    )]
    pub(crate) max_request_bytes: usize,

    /// Sustained per-client-IP request rate, in requests per second (`0`
    /// disables per-IP rate limiting). The client IP is the immediate TCP peer;
    /// run the gateway directly edge-facing, not behind an untrusted proxy that
    /// hides it (see the README).
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP", default_value_t = 100)]
    pub(crate) rate_limit_per_ip: u32,

    /// Burst allowance for the per-IP rate limit, in requests (`0` derives twice
    /// the sustained rate). Ignored when per-IP rate limiting is disabled.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_BURST", default_value_t = 0)]
    pub(crate) rate_limit_per_ip_burst: u32,

    /// IPv6 network prefix, in bits, that a client address is masked to before
    /// it keys its per-IP bucket. The default `/64` is the smallest subnet
    /// routed to one customer, so a client rotating addresses inside its own
    /// allocation keeps spending one bucket instead of minting a fresh one per
    /// address. Widen it (up to `/128`) only to meter individual addresses.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_V6_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V6_PREFIX
    )]
    pub(crate) rate_limit_per_ip_v6_prefix: u8,

    /// IPv4 network prefix, in bits, that a client address is masked to before
    /// it keys its per-IP bucket. The default `/32` is a single address: it
    /// preserves the gateway's per-address behaviour and never groups unrelated
    /// customers that share one carrier-grade NAT. Narrow it only for a
    /// deployment whose clients genuinely map to larger IPv4 allocations.
    #[arg(
        long,
        env = "WORKER_GATEWAY_RATE_LIMIT_PER_IP_V4_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V4_PREFIX
    )]
    pub(crate) rate_limit_per_ip_v4_prefix: u8,

    /// Sustained gateway-wide request rate across all clients, in requests per
    /// second (`0` disables the global rate limit).
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_GLOBAL", default_value_t = 3_000)]
    pub(crate) rate_limit_global: u32,

    /// Burst allowance for the global rate limit, in requests (`0` derives twice
    /// the sustained rate). Ignored when the global rate limit is disabled.
    #[arg(long, env = "WORKER_GATEWAY_RATE_LIMIT_GLOBAL_BURST", default_value_t = 0)]
    pub(crate) rate_limit_global_burst: u32,

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
    /// Upstream workers, in preference order.
    pub(crate) upstreams: Vec<UpstreamWorker>,
    /// Endpoint serving every non-submission call (`--redirect-queries`), or
    /// `None` when every call goes to the workers.
    pub(crate) query_upstream: Option<Url>,
    /// Readiness poll interval.
    pub(crate) readiness_poll_interval: Duration,
    /// Readiness poll timeout.
    pub(crate) readiness_poll_timeout: Duration,
    /// Upstream connect timeout.
    pub(crate) upstream_connect_timeout: Duration,
    /// Upstream per-request deadline.
    pub(crate) upstream_request_timeout: Duration,
    /// Inbound header read deadline (slow-loris guard).
    pub(crate) header_read_timeout: Duration,
    /// Maximum concurrently-open inbound connections.
    pub(crate) max_connections: NonZeroUsize,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`) for inbound connections,
    /// or `None` when disabled.
    pub(crate) tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single inbound connection's total lifetime, or `None`
    /// when uncapped.
    pub(crate) max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub(crate) max_request_bytes: usize,
    /// Per-client-IP rate limit, or `None` when disabled.
    pub(crate) rate_limit_per_ip: Option<RateLimit>,
    /// Network prefix each client address is masked to before it keys a
    /// per-client bucket.
    pub(crate) rate_limit_prefix: PrefixPolicy,
    /// Gateway-wide rate limit, or `None` when disabled.
    pub(crate) rate_limit_global: Option<RateLimit>,
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
        [
            ("--readiness-poll-interval", self.readiness_poll_interval),
            ("--readiness-poll-timeout", self.readiness_poll_timeout),
            ("--upstream-connect-timeout", self.upstream_connect_timeout),
            ("--upstream-request-timeout", self.upstream_request_timeout),
            ("--header-read-timeout", self.header_read_timeout),
        ]
        .into_iter()
        .try_for_each(|(flag, value)| ensure_non_zero(flag, value))?;
        let upstreams = self.resolve_upstreams()?;
        eyre::ensure!(!upstreams.is_empty(), "no upstream workers configured");
        ensure_unique_worker_ids(&upstreams)?;
        upstreams.iter().try_for_each(|upstream| {
            ensure_http_scheme(&upstream.rpc_url)?;
            ensure_http_scheme(&upstream.readiness_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.rpc_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.readiness_url)
        })?;
        let query_upstream = self
            .redirect_queries
            .map(|value| {
                let url = parse_query_upstream(&value)?;
                ensure_query_upstream(self.listen_addr, &url, &upstreams).map(|()| url)
            })
            .transpose()?;
        let max_connection_duration = resolve_optional_duration(self.max_connection_duration);
        // The longest a single request stays live from the gateway's own point
        // of view: up to `header_read_timeout` reading the head before the
        // service's whole-request deadline (see [`crate::app`]) is even armed,
        // plus that deadline itself. A connection cap below this bound could
        // cut off a request the gateway still considers live, so reject the
        // combination at startup.
        let single_request_bound = self
            .header_read_timeout
            .saturating_add(self.upstream_request_timeout.saturating_add(self.header_read_timeout));
        max_connection_duration.map_or(Ok(()), |cap| {
            eyre::ensure!(
                cap >= single_request_bound,
                "--max-connection-duration ({}) is shorter than the gateway's own \
                 single-request bound (--header-read-timeout plus the whole-request deadline \
                 --upstream-request-timeout + --header-read-timeout = {}); raise the cap or \
                 set it to 0 to disable it",
                humantime::format_duration(cap),
                humantime::format_duration(single_request_bound),
            );
            Ok(())
        })?;
        let rate_limit_prefix = resolve_prefix_policy(
            self.rate_limit_per_ip_v4_prefix,
            self.rate_limit_per_ip_v6_prefix,
        )?;
        Ok(Settings {
            listen_addr: self.listen_addr,
            upstreams,
            query_upstream,
            readiness_poll_interval: self.readiness_poll_interval,
            readiness_poll_timeout: self.readiness_poll_timeout,
            upstream_connect_timeout: self.upstream_connect_timeout,
            upstream_request_timeout: self.upstream_request_timeout,
            header_read_timeout: self.header_read_timeout,
            max_connections: self.max_connections,
            tcp_user_timeout: resolve_optional_duration(self.tcp_user_timeout),
            max_connection_duration,
            max_request_bytes: self.max_request_bytes,
            rate_limit_per_ip: resolve_rate_limit(
                self.rate_limit_per_ip,
                self.rate_limit_per_ip_burst,
            ),
            rate_limit_prefix,
            rate_limit_global: resolve_rate_limit(
                self.rate_limit_global,
                self.rate_limit_global_burst,
            ),
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

/// Reject non-`http` worker URLs at startup. The hop to a worker is HTTP-only:
/// the gateway carries a TLS backend for `--redirect-queries`, but TLS to
/// workers (and to their readiness endpoints) is not supported, so an `https`
/// worker URL is a configuration error reported here rather than a surprise at
/// runtime. The message names the URL by origin only, since a worker URL can
/// carry a credential in its userinfo, path or query.
///
/// A scheme the URL standard does not define is named not at all: a value
/// written without one, such as `key:secret@host:port`, parses with part of
/// the credential as its scheme (and so as its origin).
fn ensure_http_scheme(url: &Url) -> eyre::Result<()> {
    eyre::ensure!(
        url.is_special(),
        "a worker upstream URL has no http:// scheme; write it as http://host:port[/path]"
    );
    eyre::ensure!(
        url.scheme() == "http",
        "unsupported URL scheme `{}` in `{}`: worker upstreams are HTTP-only; TLS is \
         supported only for --redirect-queries",
        url.scheme(),
        UpstreamOrigin(url)
    );
    Ok(())
}

/// Parse the `--redirect-queries` value into a URL.
///
/// The flag is taken as a string and parsed here rather than by clap, because
/// clap's parse error repeats the rejected value whole, and a hosted RPC URL
/// can carry an API key in its userinfo, path or query. The error names what
/// is wrong without echoing the value.
fn parse_query_upstream(value: &str) -> eyre::Result<Url> {
    Url::parse(value).map_err(|err| {
        eyre::eyre!(
            "--redirect-queries is not a valid URL ({err}); write it as \
             http(s)://host[:port][/path]"
        )
    })
}

/// Validate the `--redirect-queries` URL: `http` or `https`, with a host and no
/// fragment, not the gateway itself, and not on any worker's RPC or readiness
/// host and port.
///
/// The last check is an error rather than a warning because reads sent to a
/// worker's node land on the validator, which is the load the redirect exists
/// to remove. It compares host and port whatever the scheme, so an `https` URL
/// on a worker's `http` socket is caught too, and an IPv4-mapped IPv6 literal
/// matches the IPv4 address it names. A URL on a worker's host but another
/// port is accepted with a warning, since that port may serve a separate node
/// on the validator's machine. Plain `http` to a host that is not a loopback
/// or private address literal is accepted with a warning: the reads and their
/// answers then cross the network unencrypted. Messages name the URL by origin
/// only, since a hosted RPC URL can carry an API key in its path.
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
        same_host(url, &upstream.rpc_url) || same_host(url, &upstream.readiness_url)
    }) {
        warn!(
            target: "gateway",
            upstream = %UpstreamOrigin(url),
            worker_id = worker.worker_id,
            "--redirect-queries is on a worker's host at another port; unless that port serves \
             a separate node, reads still reach the validator's machine"
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

/// Whether two URLs name the same host and port, whatever their schemes.
fn same_host_and_port(a: &Url, b: &Url) -> bool {
    a.port_or_known_default() == b.port_or_known_default() && same_host(a, b)
}

/// Whether two URLs name the same host. An IPv4-mapped IPv6 literal
/// (`[::ffff:10.0.0.7]`) matches the IPv4 address it names, and `localhost`
/// matches the IPv4 loopback address (see [`url_host_ip`]); other domain names
/// match only as written.
fn same_host(a: &Url, b: &Url) -> bool {
    match (url_host_ip(a), url_host_ip(b)) {
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
///
/// An IPv4-mapped IPv6 address (`[::ffff:127.0.0.1]`) is compared as the IPv4
/// address it names, on either side (the URL's through [`url_host_ip`]), since
/// a dual-stack socket reaches the same listener through both spellings.
fn ensure_not_gateway(listen_addr: SocketAddr, url: &Url) -> eyre::Result<()> {
    let same_port = url.port_or_known_default() == Some(listen_addr.port());
    let listen_ip = listen_addr.ip().to_canonical();
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

/// The upstream host as an IP when it names one (`localhost` counts; other
/// domain names cannot be checked without resolving them). An IPv4-mapped IPv6
/// literal is returned as the IPv4 address it names, so every caller compares
/// and classifies it the same way.
fn url_host_ip(url: &Url) -> Option<IpAddr> {
    url.host().and_then(|host| match host {
        url::Host::Domain(name) => {
            name.eq_ignore_ascii_case("localhost").then_some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        }
        url::Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        url::Host::Ipv6(ip) => Some(IpAddr::V6(ip).to_canonical()),
    })
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
        // Default single-request bound is 10s (header phase) + 30s + 10s
        // (whole-request deadline) = 50s; a cap of the bare whole-request
        // deadline (40s) could still cut off a request whose headers took the
        // full header window to arrive.
        let result = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=40s",
        ])
        .into_settings();
        assert!(result.is_err(), "a cap below the single-request bound must be a startup error");

        // The boundary itself is accepted.
        let boundary = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://127.0.0.1:8544",
            "--upstream-readiness-url=http://127.0.0.1:8551/health/workers",
            "--max-connection-duration=50s",
        ])
        .into_settings();
        assert!(boundary.is_ok(), "a cap equal to the single-request bound must be accepted");
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
    fn https_upstream_is_rejected() {
        let result = cli_with(
            None,
            Some("https://127.0.0.1:8545"),
            Some("http://127.0.0.1:8551/health/workers"),
        )
        .into_settings();
        assert!(result.is_err());
    }

    /// Startup errors about a worker URL name it by origin only: a scheme error
    /// and a self-pointing error, for both the RPC and the readiness URL. A
    /// value written without a scheme parses with part of its credential as
    /// the scheme, so its error names neither the scheme nor the origin.
    #[test]
    fn startup_errors_name_origins_only() {
        let ready = "http://10.0.0.7:8551/health/workers";
        let userinfo = ["user", "pw@", "/path"].as_slice();
        let schemeless = ["apikey", "s3cr3t"].as_slice();
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
            // the smallest non-zero value is still accepted
            let tiny = format!("{flag}=1ms");
            cli_with_flags(&[tiny.as_str()]).into_settings()?;
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
            let result = Cli::parse_from([
                "worker-gateway".to_string(),
                format!("--listen-addr={listen}"),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={readiness}"),
            ])
            .into_settings();
            assert!(result.is_err(), "{rpc} / {readiness} is the gateway on {listen}");
        }
        // a mapped address of another host, or on another port, is accepted
        for rpc in ["http://[::ffff:10.0.0.7]:8545/", "http://[::ffff:127.0.0.1]:8544/"] {
            Cli::parse_from([
                "worker-gateway".to_string(),
                "--listen-addr=127.0.0.1:8545".to_string(),
                format!("--upstream-rpc-url={rpc}"),
                format!("--upstream-readiness-url={ready}"),
            ])
            .into_settings()?;
        }
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
    fn zero_rate_disables_the_limiter() -> eyre::Result<()> {
        let settings =
            cli_with_flags(&["--rate-limit-per-ip=0", "--rate-limit-global=0"]).into_settings()?;
        assert!(settings.rate_limit_per_ip.is_none());
        assert!(settings.rate_limit_global.is_none());
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
    fn max_request_bytes_is_configurable() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--max-request-bytes=1024"]).into_settings()?;
        assert_eq!(settings.max_request_bytes, 1_024);
        Ok(())
    }

    #[test]
    fn no_query_redirect_by_default() -> eyre::Result<()> {
        assert_eq!(cli_with_flags(&[]).into_settings()?.query_upstream, None);
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
            let cli = Cli::try_parse_from([
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
        let result = Cli::parse_from([
            "worker-gateway",
            "--upstream-rpc-url=http://[::ffff:10.0.0.7]:8545",
            "--upstream-readiness-url=http://[::ffff:10.0.0.7]:8551/health/workers",
            "--redirect-queries=http://10.0.0.7:8545/",
        ])
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
}
