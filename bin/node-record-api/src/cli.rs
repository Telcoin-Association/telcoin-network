//! Command-line interface and resolved runtime settings.
//!
//! Every flag has a `NODE_RECORD_API_<FLAG>` environment fallback so a container can be
//! configured entirely from its environment. [`Cli::into_settings`] validates the combination
//! and reads the key files once, so a misconfiguration is a startup error with the offending
//! flag named rather than a daemon that serves nothing.

use std::{
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use clap::Parser;
use tn_kad_client::{BlsPublicKey, Multiaddr};
use url::Url;

use crate::{
    keys::{load_committee, worker_bootstrap_addrs, StaticKeys},
    ratelimit::{PrefixLen, PrefixPolicy, RateLimit},
};

/// Default cap on a request body. The API is `GET`-only, so anything beyond a small header-scale
/// body is not a legitimate request.
pub const DEFAULT_MAX_REQUEST_BYTES: usize = 16 * 1024;

/// HTTP/JSON directory of validators' DHT-advertised JSON-RPC endpoints.
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Address the API listens on, for `/v1/*` and its own `/healthz` (liveness) and `/readyz`
    /// (readiness) endpoints.
    #[arg(long, env = "NODE_RECORD_API_LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub listen_addr: SocketAddr,

    /// Address to expose Prometheus metrics on (`GET /metrics`), on a listener separate from
    /// the API port. Unset (the default) disables the metrics endpoint entirely; bind it to an
    /// internal interface, not the public edge. Named `--metrics` to match the node's flag.
    #[arg(long = "metrics", env = "NODE_RECORD_API_METRICS_ADDR")]
    pub metrics_addr: Option<SocketAddr>,

    /// The chain whose worker DHT is read (testnet is `2017`). Required with no default: the
    /// chain id is folded into the kademlia protocol name and every record's signing domain, so
    /// a wrong value does not fail loudly, it yields `NoPeerAnswered` or `InvalidRecords` for
    /// every key and an empty directory. A committee file does not carry the chain id.
    #[arg(long, env = "NODE_RECORD_API_CHAIN_ID")]
    pub chain_id: u64,

    /// Which worker DHT to read. Only worker records carry RPC endpoints.
    #[arg(long, env = "NODE_RECORD_API_WORKER_ID", default_value_t = 0)]
    pub worker_id: u16,

    /// A worker DHT bootstrap address (`/ip4/../udp/../quic-v1/p2p/<peer-id>`). Repeatable;
    /// comma-separated in the environment. When omitted, the bootstrap addresses are the
    /// `worker_id` worker of every bootstrap server in `--committee-file`.
    #[arg(long, env = "NODE_RECORD_API_BOOTSTRAP", value_delimiter = ',')]
    pub bootstrap: Vec<Multiaddr>,

    /// A `committee.yaml`. Its authorities form part of the tracked key floor (re-read every
    /// cycle), and its bootstrap servers supply the DHT addresses when `--bootstrap` is unset.
    #[arg(long, env = "NODE_RECORD_API_COMMITTEE_FILE")]
    pub committee_file: Option<PathBuf>,

    /// A YAML list of BLS public keys (base58 or `0x`-hex) to always track. Every entry must
    /// parse; a typo is a startup error.
    #[arg(long, env = "NODE_RECORD_API_KEYS_FILE")]
    pub keys_file: Option<PathBuf>,

    /// A node's JSON-RPC URL. Enables the live committee source (`tn_getCurrentEpochInfo` +
    /// `tn_getCommitteeBlsPubkeys`) and epoch-boundary scheduling. Note this returns the
    /// committee, not every validator; use `--committee-file` / `--keys-file` for the rest.
    #[arg(long, env = "NODE_RECORD_API_RPC_URL")]
    pub rpc_url: Option<Url>,

    /// Per-request deadline for the JSON-RPC calls behind `--rpc-url`.
    #[arg(
        long,
        env = "NODE_RECORD_API_RPC_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    pub rpc_timeout: Duration,

    /// How often to refresh every tracked key from the DHT. Must exceed `--query-timeout`.
    #[arg(
        long,
        env = "NODE_RECORD_API_REFRESH_INTERVAL",
        default_value = "5m",
        value_parser = humantime::parse_duration
    )]
    pub refresh_interval: Duration,

    /// How long after an epoch boundary to run the extra refresh that picks up the new
    /// committee (only with `--rpc-url`).
    #[arg(
        long,
        env = "NODE_RECORD_API_EPOCH_GRACE",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub epoch_grace: Duration,

    /// Per-lookup deadline against the DHT; also bounds the bootstrap dial each cycle.
    #[arg(
        long,
        env = "NODE_RECORD_API_QUERY_TIMEOUT",
        default_value = "15s",
        value_parser = humantime::parse_duration
    )]
    pub query_timeout: Duration,

    /// DHT lookups in flight at once within a cycle.
    #[arg(long, env = "NODE_RECORD_API_LOOKUP_CONCURRENCY", default_value_t = 4)]
    pub lookup_concurrency: usize,

    /// Drop a cached record not successfully refreshed for this long. Must exceed
    /// `--refresh-interval`.
    #[arg(
        long,
        env = "NODE_RECORD_API_RECORD_TTL",
        default_value = "24h",
        value_parser = humantime::parse_duration
    )]
    pub record_ttl: Duration,

    /// Drop a cached record whose key has been absent from the tracked set for this many
    /// consecutive cycles.
    #[arg(long, env = "NODE_RECORD_API_ABSENT_CYCLES_BEFORE_EVICT", default_value_t = 3)]
    pub absent_cycles_before_evict: u32,

    /// How long a new connection may take to send its complete request headers before it is
    /// disconnected (slow-loris guard).
    #[arg(
        long,
        env = "NODE_RECORD_API_HEADER_READ_TIMEOUT",
        default_value = "10s",
        value_parser = humantime::parse_duration
    )]
    pub header_read_timeout: Duration,

    /// Deadline for a whole request once its headers are in.
    #[arg(
        long,
        env = "NODE_RECORD_API_REQUEST_TIMEOUT",
        default_value = "5s",
        value_parser = humantime::parse_duration
    )]
    pub request_timeout: Duration,

    /// Maximum concurrently-open inbound connections; further connections wait in the OS accept
    /// backlog until a slot frees up.
    #[arg(long, env = "NODE_RECORD_API_MAX_CONNECTIONS", default_value = "500")]
    pub max_connections: NonZeroUsize,

    /// Transport-stall deadline for inbound connections (`TCP_USER_TIMEOUT`): a connection whose
    /// peer leaves written response data unacknowledged, or its receive window closed, for this
    /// long is forcibly closed by the kernel. Linux-family kernels only; elsewhere only
    /// `--max-connection-duration` bounds a stalled reader. `0` disables.
    #[arg(
        long,
        env = "NODE_RECORD_API_TCP_USER_TIMEOUT",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub tcp_user_timeout: Duration,

    /// Hard cap on a single inbound connection's total lifetime, keep-alive sessions included.
    /// Must be at least `--header-read-timeout` plus `--request-timeout` so the first request on
    /// a connection can never be cut off. `0` disables.
    #[arg(
        long,
        env = "NODE_RECORD_API_MAX_CONNECTION_DURATION",
        default_value = "10m",
        value_parser = humantime::parse_duration
    )]
    pub max_connection_duration: Duration,

    /// Maximum request body the API will accept, in bytes (the API is `GET`-only).
    #[arg(
        long,
        env = "NODE_RECORD_API_MAX_REQUEST_BYTES",
        default_value_t = DEFAULT_MAX_REQUEST_BYTES
    )]
    pub max_request_bytes: usize,

    /// Sustained per-client-IP request rate, in requests per second (`0` disables per-IP rate
    /// limiting). The client IP is the immediate TCP peer; run the daemon edge-facing.
    #[arg(long, env = "NODE_RECORD_API_RATE_LIMIT_PER_IP", default_value_t = 100)]
    pub rate_limit_per_ip: u32,

    /// Burst allowance for the per-IP rate limit, in requests (`0` derives twice the sustained
    /// rate). Ignored when per-IP rate limiting is disabled.
    #[arg(long, env = "NODE_RECORD_API_RATE_LIMIT_PER_IP_BURST", default_value_t = 0)]
    pub rate_limit_per_ip_burst: u32,

    /// IPv6 network prefix, in bits, that a client address is masked to before it keys its
    /// per-IP bucket (`/64` is the smallest subnet routed to one customer).
    #[arg(
        long,
        env = "NODE_RECORD_API_RATE_LIMIT_PER_IP_V6_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V6_PREFIX
    )]
    pub rate_limit_per_ip_v6_prefix: u8,

    /// IPv4 network prefix, in bits, that a client address is masked to before it keys its
    /// per-IP bucket (`/32` meters individual addresses).
    #[arg(
        long,
        env = "NODE_RECORD_API_RATE_LIMIT_PER_IP_V4_PREFIX",
        default_value_t = crate::ratelimit::DEFAULT_V4_PREFIX
    )]
    pub rate_limit_per_ip_v4_prefix: u8,

    /// Sustained daemon-wide request rate across all clients, in requests per second (`0`
    /// disables the global rate limit).
    #[arg(long, env = "NODE_RECORD_API_RATE_LIMIT_GLOBAL", default_value_t = 3_000)]
    pub rate_limit_global: u32,

    /// Burst allowance for the global rate limit, in requests (`0` derives twice the sustained
    /// rate). Ignored when the global rate limit is disabled.
    #[arg(long, env = "NODE_RECORD_API_RATE_LIMIT_GLOBAL_BURST", default_value_t = 0)]
    pub rate_limit_global_burst: u32,

    /// How long to drain in-flight requests on SIGTERM before forcing close.
    #[arg(
        long,
        env = "NODE_RECORD_API_GRACEFUL_SHUTDOWN_TIMEOUT",
        default_value = "30s",
        value_parser = humantime::parse_duration
    )]
    pub graceful_shutdown_timeout: Duration,

    /// Tracing filter directive (e.g. `info,tn::node_record_api=debug`).
    #[arg(long, env = "NODE_RECORD_API_LOG_FILTER", default_value = "info")]
    pub log_filter: String,
}

/// Fully-resolved runtime settings, derived from [`Cli`].
#[derive(Debug)]
pub struct Settings {
    /// Address the API listens on.
    pub listen_addr: SocketAddr,
    /// Address to expose the Prometheus scrape endpoint on, or `None` when metrics are disabled.
    pub metrics_addr: Option<SocketAddr>,
    /// The chain whose worker DHT is read.
    pub chain_id: u64,
    /// Which worker DHT is read.
    pub worker_id: u16,
    /// Worker DHT bootstrap addresses, each carrying `/p2p/<peer-id>`.
    pub bootstrap: Vec<Multiaddr>,
    /// The committee file, when configured (re-read every cycle).
    pub committee_file: Option<PathBuf>,
    /// The static key floor, already parsed.
    pub static_keys: Vec<BlsPublicKey>,
    /// The live committee source's JSON-RPC URL, when configured.
    pub rpc_url: Option<Url>,
    /// Per-request deadline for JSON-RPC calls.
    pub rpc_timeout: Duration,
    /// Refresh cadence.
    pub refresh_interval: Duration,
    /// Delay after an epoch boundary before the extra refresh.
    pub epoch_grace: Duration,
    /// Per-lookup DHT deadline.
    pub query_timeout: Duration,
    /// DHT lookups in flight at once.
    pub lookup_concurrency: NonZeroUsize,
    /// Cached record TTL.
    pub record_ttl: Duration,
    /// Absent-from-key-set eviction threshold, in cycles.
    pub absent_cycles_before_evict: u32,
    /// Inbound header read deadline (slow-loris guard).
    pub header_read_timeout: Duration,
    /// Whole-request deadline.
    pub request_timeout: Duration,
    /// Maximum concurrently-open inbound connections.
    pub max_connections: NonZeroUsize,
    /// Transport-stall deadline (`TCP_USER_TIMEOUT`), or `None` when disabled.
    pub tcp_user_timeout: Option<Duration>,
    /// Hard cap on a single inbound connection's lifetime, or `None` when uncapped.
    pub max_connection_duration: Option<Duration>,
    /// Maximum accepted request body size, in bytes.
    pub max_request_bytes: usize,
    /// Per-client-IP rate limit, or `None` when disabled.
    pub rate_limit_per_ip: Option<RateLimit>,
    /// Network prefix each client address is masked to before it keys a per-client bucket.
    pub rate_limit_prefix: PrefixPolicy,
    /// Daemon-wide rate limit, or `None` when disabled.
    pub rate_limit_global: Option<RateLimit>,
    /// Graceful-shutdown drain deadline.
    pub graceful_shutdown_timeout: Duration,
}

impl Cli {
    /// Validate the flag combination and resolve it into [`Settings`], reading the committee and
    /// key files once so a bad file fails startup.
    pub fn into_settings(self) -> eyre::Result<Settings> {
        let lookup_concurrency = NonZeroUsize::new(self.lookup_concurrency)
            .ok_or_else(|| eyre::eyre!("--lookup-concurrency must be at least 1"))?;
        eyre::ensure!(
            self.absent_cycles_before_evict >= 1,
            "--absent-cycles-before-evict must be at least 1 (0 would evict every record every \
             cycle)"
        );
        eyre::ensure!(
            !self.refresh_interval.is_zero(),
            "--refresh-interval must be greater than 0"
        );
        eyre::ensure!(
            self.refresh_interval > self.query_timeout,
            "--refresh-interval ({}) must exceed --query-timeout ({}); a cycle must be able to \
             finish before the next is due",
            humantime::format_duration(self.refresh_interval),
            humantime::format_duration(self.query_timeout),
        );
        eyre::ensure!(
            self.record_ttl > self.refresh_interval,
            "--record-ttl ({}) must exceed --refresh-interval ({}); otherwise every record is \
             evicted before it can be refreshed",
            humantime::format_duration(self.record_ttl),
            humantime::format_duration(self.refresh_interval),
        );
        if let Some(url) = &self.rpc_url {
            eyre::ensure!(
                matches!(url.scheme(), "http" | "https"),
                "unsupported --rpc-url scheme `{}` in `{url}`: expected http or https",
                url.scheme()
            );
        }
        eyre::ensure!(
            self.rpc_url.is_some() || self.committee_file.is_some() || self.keys_file.is_some(),
            "no key source configured: provide at least one of --rpc-url, --committee-file, or \
             --keys-file"
        );

        // the committee file is read here so both a bad file and an empty worker list fail
        // startup; the refresh loop re-reads it on every cycle
        let committee = self
            .committee_file
            .as_deref()
            .map(|path| load_committee(path).map_err(|err| eyre::eyre!("--committee-file: {err}")))
            .transpose()?;
        let bootstrap = if self.bootstrap.is_empty() {
            let Some(committee) = &committee else {
                eyre::bail!(
                    "no bootstrap source configured: provide --bootstrap or --committee-file"
                );
            };
            let addrs = worker_bootstrap_addrs(committee, self.worker_id);
            eyre::ensure!(
                !addrs.is_empty(),
                "no bootstrap server in the committee file advertises worker {}",
                self.worker_id
            );
            addrs
        } else {
            self.bootstrap.clone()
        };
        for addr in &bootstrap {
            eyre::ensure!(
                addr.protocol_stack().any(|protocol| protocol == "p2p"),
                "bootstrap address {addr} has no /p2p/<peer-id> component; kademlia keys its \
                 routing table by peer id, so the address cannot be dialed without one"
            );
        }

        let static_keys = self
            .keys_file
            .as_deref()
            .map(|path| StaticKeys::load(path).map_err(|err| eyre::eyre!("--keys-file: {err}")))
            .transpose()?
            .map(StaticKeys::into_keys)
            .unwrap_or_default();

        let max_connection_duration = resolve_optional_duration(self.max_connection_duration);
        // The longest a single request stays live: up to `header_read_timeout` reading the head
        // before the whole-request deadline is armed, plus that deadline itself. A connection
        // cap below this bound could cut off a request the daemon still considers live.
        let single_request_bound = self.header_read_timeout.saturating_add(self.request_timeout);
        if let Some(cap) = max_connection_duration {
            eyre::ensure!(
                cap >= single_request_bound,
                "--max-connection-duration ({}) is shorter than the single-request bound \
                 (--header-read-timeout + --request-timeout = {}); raise the cap or set it to 0 \
                 to disable it",
                humantime::format_duration(cap),
                humantime::format_duration(single_request_bound),
            );
        }
        let rate_limit_prefix = resolve_prefix_policy(
            self.rate_limit_per_ip_v4_prefix,
            self.rate_limit_per_ip_v6_prefix,
        )?;

        Ok(Settings {
            listen_addr: self.listen_addr,
            metrics_addr: self.metrics_addr,
            chain_id: self.chain_id,
            worker_id: self.worker_id,
            bootstrap,
            committee_file: self.committee_file,
            static_keys,
            rpc_url: self.rpc_url,
            rpc_timeout: self.rpc_timeout,
            refresh_interval: self.refresh_interval,
            epoch_grace: self.epoch_grace,
            query_timeout: self.query_timeout,
            lookup_concurrency,
            record_ttl: self.record_ttl,
            absent_cycles_before_evict: self.absent_cycles_before_evict,
            header_read_timeout: self.header_read_timeout,
            request_timeout: self.request_timeout,
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
        })
    }
}

/// Turn a duration flag into `Some(duration)`, or `None` when zero (the flag's disabled
/// sentinel, mirroring the `0`-disables rate-limit flags).
fn resolve_optional_duration(value: Duration) -> Option<Duration> {
    (!value.is_zero()).then_some(value)
}

/// Turn a `(rate, burst)` flag pair into a [`RateLimit`], or `None` when the rate is `0`
/// (limiter disabled). A `0` burst derives twice the sustained rate so a modest headroom is the
/// default without a second flag.
fn resolve_rate_limit(rate: u32, burst: u32) -> Option<RateLimit> {
    NonZeroU32::new(rate).map(|rate| {
        let burst = if burst == 0 { rate.get().saturating_mul(2) } else { burst };
        // `burst` is now >= `rate.get()` >= 1, so it is non-zero; fall back to the (non-zero)
        // rate rather than panic if that ever fails to hold.
        RateLimit::new(rate, NonZeroU32::new(burst).unwrap_or(rate))
    })
}

/// Validate the two per-IP prefix flags into a [`PrefixPolicy`]. A length wider than its
/// address family allows is a startup error rather than a silently clamped value.
fn resolve_prefix_policy(v4: u8, v6: u8) -> eyre::Result<PrefixPolicy> {
    PrefixLen::v4(v4)
        .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v4-prefix: {err}"))
        .and_then(|v4| {
            PrefixLen::v6(v6)
                .map_err(|err| eyre::eyre!("invalid --rate-limit-per-ip-v6-prefix: {err}"))
                .map(|v6| PrefixPolicy::new(v4, v6))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory as _;
    use std::{io::Write as _, path::Path};

    const BOOTSTRAP: &str =
        "/ip4/127.0.0.1/udp/49594/quic-v1/p2p/12D3KooWHGdbYerasxspuhZW4KqNMbbpqvLNCG6LGpS5yx6b8x2x";

    fn testnet_committee() -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../chain-configs/testnet/committee.yaml")
            .to_string_lossy()
            .into_owned()
    }

    /// A minimal valid CLI (explicit bootstrap, rpc key source) plus `extra` flags.
    fn cli_with(extra: &[&str]) -> Cli {
        let mut argv = vec![
            "node-record-api".to_string(),
            "--chain-id=2017".to_string(),
            format!("--bootstrap={BOOTSTRAP}"),
            "--rpc-url=http://127.0.0.1:8545".to_string(),
        ];
        argv.extend(extra.iter().map(|flag| (*flag).to_string()));
        Cli::parse_from(argv)
    }

    fn settings_with(extra: &[&str]) -> eyre::Result<Settings> {
        cli_with(extra).into_settings()
    }

    fn error_of(result: eyre::Result<Settings>) -> String {
        result.err().map(|err| err.to_string()).unwrap_or_default()
    }

    #[test]
    fn minimal_cli_resolves_with_defaults() -> eyre::Result<()> {
        let settings = settings_with(&[])?;
        assert_eq!(settings.chain_id, 2017);
        assert_eq!(settings.worker_id, 0);
        assert_eq!(settings.bootstrap.len(), 1);
        assert!(settings.static_keys.is_empty());
        assert_eq!(settings.refresh_interval, Duration::from_secs(300));
        assert_eq!(settings.query_timeout, Duration::from_secs(15));
        assert_eq!(settings.record_ttl, Duration::from_secs(24 * 3_600));
        assert_eq!(settings.lookup_concurrency.get(), 4);
        assert_eq!(settings.absent_cycles_before_evict, 3);
        assert_eq!(settings.request_timeout, Duration::from_secs(5));
        assert_eq!(settings.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(settings.tcp_user_timeout, Some(Duration::from_secs(30)));
        assert_eq!(settings.max_connection_duration, Some(Duration::from_secs(600)));
        let per_ip = settings.rate_limit_per_ip.expect("per-ip limit on by default");
        assert_eq!(per_ip.rate().get(), 100);
        // a zero burst flag derives twice the sustained rate
        assert_eq!(per_ip.burst().get(), 200);
        let global = settings.rate_limit_global.expect("global limit on by default");
        assert_eq!(global.rate().get(), 3_000);
        assert_eq!(global.burst().get(), 6_000);
        assert_eq!(settings.rate_limit_prefix.v4_bits(), 32);
        assert_eq!(settings.rate_limit_prefix.v6_bits(), 64);
        assert!(settings.metrics_addr.is_none());
        Ok(())
    }

    #[test]
    fn chain_id_is_required() {
        let result = Cli::try_parse_from(["node-record-api", &format!("--bootstrap={BOOTSTRAP}")]);
        assert!(result.is_err(), "--chain-id has no default");
    }

    #[test]
    fn requires_a_bootstrap_source() {
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            "--rpc-url=http://127.0.0.1:8545",
        ])
        .into_settings();
        assert!(error_of(result).contains("no bootstrap source"));
    }

    #[test]
    fn committee_file_supplies_bootstrap_and_keys() -> eyre::Result<()> {
        let settings = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--committee-file={}", testnet_committee()),
        ])
        .into_settings()?;
        assert_eq!(settings.bootstrap.len(), 5);
        for addr in &settings.bootstrap {
            assert!(addr.to_string().contains("/udp/49594/quic-v1/p2p/"));
        }
        assert!(settings.committee_file.is_some());
        assert!(settings.rpc_url.is_none());

        // an explicit --bootstrap takes precedence over the file's servers
        let settings = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--committee-file={}", testnet_committee()),
            &format!("--bootstrap={BOOTSTRAP}"),
        ])
        .into_settings()?;
        assert_eq!(settings.bootstrap.len(), 1);

        // a worker the file's servers do not advertise is a startup error
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            "--worker-id=3",
            &format!("--committee-file={}", testnet_committee()),
        ])
        .into_settings();
        assert!(error_of(result).contains("advertises worker 3"));
        Ok(())
    }

    #[test]
    fn bad_committee_file_is_a_startup_error() {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        file.write_all(b"authorities: nope\n").expect("write");
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--committee-file={}", file.path().display()),
        ])
        .into_settings();
        assert!(error_of(result).contains("--committee-file"));
    }

    #[test]
    fn requires_a_key_source() {
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--bootstrap={BOOTSTRAP}"),
        ])
        .into_settings();
        assert!(error_of(result).contains("no key source"));
    }

    #[test]
    fn keys_file_is_parsed_and_validated() -> eyre::Result<()> {
        let mut good = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(good, "- {}", crate::cache::test_support::KEY_A)?;
        let settings = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--bootstrap={BOOTSTRAP}"),
            &format!("--keys-file={}", good.path().display()),
        ])
        .into_settings()?;
        assert_eq!(settings.static_keys.len(), 1);

        let mut bad = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(bad, "- 0xzz")?;
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            &format!("--bootstrap={BOOTSTRAP}"),
            &format!("--keys-file={}", bad.path().display()),
        ])
        .into_settings();
        assert!(error_of(result).contains("--keys-file"));
        Ok(())
    }

    #[test]
    fn bootstrap_without_peer_id_is_rejected() {
        let result = Cli::parse_from([
            "node-record-api",
            "--chain-id=2017",
            "--bootstrap=/ip4/127.0.0.1/udp/49594/quic-v1",
            "--rpc-url=http://127.0.0.1:8545",
        ])
        .into_settings();
        let message = error_of(result);
        assert!(message.contains("/p2p/"), "{message}");
        assert!(message.contains("/ip4/127.0.0.1/udp/49594/quic-v1"), "names the address");
    }

    #[test]
    fn refresh_interval_must_be_positive_and_exceed_query_timeout() {
        assert!(error_of(settings_with(&["--refresh-interval=0s"])).contains("greater than 0"));
        let message = error_of(settings_with(&["--refresh-interval=10s", "--query-timeout=15s"]));
        assert!(message.contains("--refresh-interval"), "{message}");
        assert!(message.contains("--query-timeout"), "{message}");
        // equal is rejected too; strictly greater is accepted
        assert!(settings_with(&["--refresh-interval=15s", "--query-timeout=15s"]).is_err());
        assert!(settings_with(&["--refresh-interval=16s", "--query-timeout=15s"]).is_ok());
    }

    #[test]
    fn record_ttl_must_exceed_refresh_interval() {
        let message = error_of(settings_with(&["--record-ttl=5m"]));
        assert!(message.contains("--record-ttl"), "{message}");
        assert!(settings_with(&["--record-ttl=5m1s"]).is_ok());
    }

    #[test]
    fn connection_cap_below_single_request_bound_is_rejected() {
        // defaults: 10s header read + 5s request = 15s
        let message = error_of(settings_with(&["--max-connection-duration=14s"]));
        assert!(message.contains("--max-connection-duration"), "{message}");
        assert!(settings_with(&["--max-connection-duration=15s"]).is_ok(), "boundary accepted");
        // zero disables the cap and skips the rule
        let settings = settings_with(&["--max-connection-duration=0"]).expect("disabled");
        assert_eq!(settings.max_connection_duration, None);
    }

    #[test]
    fn lookup_concurrency_must_be_at_least_one() {
        assert!(
            error_of(settings_with(&["--lookup-concurrency=0"])).contains("--lookup-concurrency")
        );
        assert_eq!(
            settings_with(&["--lookup-concurrency=8"]).expect("ok").lookup_concurrency.get(),
            8
        );
    }

    #[test]
    fn absent_cycles_must_be_at_least_one() {
        assert!(error_of(settings_with(&["--absent-cycles-before-evict=0"]))
            .contains("--absent-cycles-before-evict"));
    }

    #[test]
    fn rpc_url_must_be_http_or_https() {
        let with_rpc = |url: &str| {
            Cli::parse_from([
                "node-record-api",
                "--chain-id=2017",
                &format!("--bootstrap={BOOTSTRAP}"),
                &format!("--rpc-url={url}"),
            ])
            .into_settings()
        };
        let message = error_of(with_rpc("ws://127.0.0.1:8546"));
        assert!(message.contains("--rpc-url"), "{message}");
        assert!(with_rpc("https://rpc.example/").is_ok());
    }

    #[test]
    fn out_of_range_per_ip_prefix_is_rejected() {
        assert!(settings_with(&["--rate-limit-per-ip-v4-prefix=33"]).is_err());
        assert!(settings_with(&["--rate-limit-per-ip-v6-prefix=129"]).is_err());
    }

    #[test]
    fn zero_disables_rate_limits_and_write_path_guards() -> eyre::Result<()> {
        let settings = settings_with(&[
            "--rate-limit-per-ip=0",
            "--rate-limit-global=0",
            "--tcp-user-timeout=0",
        ])?;
        assert!(settings.rate_limit_per_ip.is_none());
        assert!(settings.rate_limit_global.is_none());
        assert_eq!(settings.tcp_user_timeout, None);
        Ok(())
    }

    #[test]
    fn env_fallbacks_are_named_after_the_flags() {
        // one representative: the whole surface uses the same prefix
        let cli = Cli::command();
        for arg in cli.get_arguments() {
            let Some(long) = arg.get_long() else { continue };
            if long == "help" || long == "version" {
                continue;
            }
            let env = arg.get_env().and_then(|env| env.to_str()).unwrap_or_default();
            assert!(env.starts_with("NODE_RECORD_API_"), "--{long} has env {env:?}");
        }
    }
}
