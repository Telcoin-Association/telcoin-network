//! Command-line interface and resolved runtime settings.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use url::Url;

use crate::{
    config::{GatewayConfig, UpstreamWorker},
    ratelimit::RateLimit,
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

    /// Connect timeout when forwarding a request to an upstream worker.
    #[arg(
        long,
        env = "WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT",
        default_value = "2s",
        value_parser = humantime::parse_duration
    )]
    pub(crate) upstream_connect_timeout: Duration,

    /// Overall per-request deadline when forwarding to an upstream worker.
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

    /// Maximum request body the gateway will accept, in bytes. Requests whose
    /// body exceeds this are rejected with a JSON-RPC "request too large" error
    /// before being forwarded.
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
    /// Maximum accepted request body size, in bytes.
    pub(crate) max_request_bytes: usize,
    /// Per-client-IP rate limit, or `None` when disabled.
    pub(crate) rate_limit_per_ip: Option<RateLimit>,
    /// Gateway-wide rate limit, or `None` when disabled.
    pub(crate) rate_limit_global: Option<RateLimit>,
    /// Graceful-shutdown drain deadline.
    pub(crate) graceful_shutdown_timeout: Duration,
}

impl Cli {
    /// Resolve the CLI into [`Settings`], loading the YAML upstream list or
    /// building a single inline upstream from the `--upstream-*` flags.
    pub(crate) fn into_settings(self) -> eyre::Result<Settings> {
        let upstreams = self.resolve_upstreams()?;
        eyre::ensure!(!upstreams.is_empty(), "no upstream workers configured");
        upstreams.iter().try_for_each(|upstream| {
            ensure_http_scheme(&upstream.rpc_url)?;
            ensure_http_scheme(&upstream.readiness_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.rpc_url)?;
            ensure_not_gateway(self.listen_addr, &upstream.readiness_url)
        })?;
        Ok(Settings {
            listen_addr: self.listen_addr,
            upstreams,
            readiness_poll_interval: self.readiness_poll_interval,
            readiness_poll_timeout: self.readiness_poll_timeout,
            upstream_connect_timeout: self.upstream_connect_timeout,
            upstream_request_timeout: self.upstream_request_timeout,
            header_read_timeout: self.header_read_timeout,
            max_connections: self.max_connections,
            max_request_bytes: self.max_request_bytes,
            rate_limit_per_ip: resolve_rate_limit(
                self.rate_limit_per_ip,
                self.rate_limit_per_ip_burst,
            ),
            rate_limit_global: resolve_rate_limit(
                self.rate_limit_global,
                self.rate_limit_global_burst,
            ),
            graceful_shutdown_timeout: self.graceful_shutdown_timeout,
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

/// Reject non-`http` upstream URLs at startup. The gateway is HTTP-only (the
/// workspace `reqwest` is built without a TLS backend), so an `https` URL would
/// parse and boot but then fail every forward and readiness probe with an
/// opaque runtime error; catching it here turns that into a clear startup error.
fn ensure_http_scheme(url: &Url) -> eyre::Result<()> {
    eyre::ensure!(
        url.scheme() == "http",
        "unsupported URL scheme `{}` in `{url}`: the worker gateway is HTTP-only (no TLS)",
        url.scheme()
    );
    Ok(())
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
        "upstream URL `{url}` points at the gateway's own listen address ({listen_addr}), \
         so forwarding to it would loop; change the upstream URL or --listen-addr"
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
        assert_eq!(settings.max_request_bytes, 26_214_400);
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
    fn max_request_bytes_is_configurable() -> eyre::Result<()> {
        let settings = cli_with_flags(&["--max-request-bytes=1024"]).into_settings()?;
        assert_eq!(settings.max_request_bytes, 1_024);
        Ok(())
    }
}
