//! The gateway's file-descriptor budget.
//!
//! Every inbound connection, every upstream connection and every blocking name
//! lookup holds a descriptor, so the caps only work if the process may open that
//! many. At startup the gateway raises its soft `RLIMIT_NOFILE` to the hard
//! limit ([`raise_limit`]) and warns when the caps could still need more
//! ([`warn_on_budget`]); it never refuses to start over it. When descriptors do
//! run out, an upstream connect fails with `EMFILE` or `ENFILE`, which
//! [`is_exhausted`] recognises so passive health does not blame the worker.

use tracing::{debug, warn};

use crate::cli::Settings;

/// Descriptors held outside the capped pools: the client, probe and metrics
/// listeners, the readiness polls (one connection per upstream, plus the
/// `--redirect-queries` probe), stdio and the runtime's own handles.
const RESERVE: u64 = 64;

/// `errno` for "too many open files" in this process (`EMFILE`), the same on
/// Linux and macOS.
const EMFILE: i32 = 24;

/// `errno` for "too many open files" system-wide (`ENFILE`), the same on Linux
/// and macOS.
const ENFILE: i32 = 23;

/// Raise the soft `RLIMIT_NOFILE` to the hard limit, as the node does, and
/// return the limit now in force, or `None` when it is unknown (the platform is
/// unsupported or the call failed, which is logged).
pub(crate) fn raise_limit() -> Option<u64> {
    match fdlimit::raise_fd_limit() {
        Ok(fdlimit::Outcome::LimitRaised { from, to }) => {
            debug!(target: "gateway", from, to, "raised the file descriptor limit");
            Some(to)
        }
        Ok(fdlimit::Outcome::Unsupported) => None,
        Err(err) => {
            warn!(target: "gateway", %err, "could not raise the file descriptor limit");
            None
        }
    }
}

/// Warn when the caps in `settings` could need more descriptors than
/// `soft_limit`. Past the limit accepts and upstream connects fail with
/// `EMFILE` instead of the caps answering, so the operator should raise the
/// limit or lower the caps; startup goes on either way.
pub(crate) fn warn_on_budget(settings: &Settings, soft_limit: u64) {
    if let Some(demand) = over_budget(settings, soft_limit) {
        warn!(
            target: "gateway",
            demand,
            soft_limit,
            "the connection, upstream and lookup caps can hold more file descriptors than the \
             soft RLIMIT_NOFILE, so accepts and upstream connects can fail with EMFILE before a \
             cap is reached; raise LimitNOFILE (ulimit -n), or lower --max-connections, \
             --max-upstream-inflight or --max-inflight-queries"
        );
    }
}

/// Whether `err` is, or was caused by, the gateway itself running out of file
/// descriptors (`EMFILE` or `ENFILE`).
///
/// hyper-util reports such a failure on an upstream connect as a connect error,
/// like a worker whose port is closed, but it says nothing about the worker, so
/// passive health skips it, as it skips [`crate::dns::is_saturated`].
pub(crate) fn is_exhausted(err: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(err), |err| err.source()).any(|err| {
        err.downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            .is_some_and(|code| code == EMFILE || code == ENFILE)
    })
}

/// The demand when it exceeds `soft_limit`, or `None` when the caps fit.
fn over_budget(settings: &Settings, soft_limit: u64) -> Option<u64> {
    Some(demand(settings)).filter(|demand| *demand > soft_limit)
}

/// The most descriptors the caps in `settings` let the gateway hold at once:
/// one per inbound connection (`--max-connections`), one per worker request
/// (`--max-upstream-inflight`, or every connection when it is `0`), one per
/// redirected query when `--redirect-queries` is set (`--max-inflight-queries`,
/// or every connection when it is `0`), one per blocking lookup for each of the
/// two resolvers (the forwarding client's and the readiness poller's, each
/// capped at `--max-concurrent-dns-lookups`), plus [`RESERVE`].
///
/// The probe listener's connections are not capped and so are not counted.
fn demand(settings: &Settings) -> u64 {
    let connections = settings.max_connections.get();
    let or_connections = |cap: usize| if cap == 0 { connections } else { cap };
    let worker = or_connections(settings.max_upstream_inflight);
    let queries = settings.query_upstream.as_ref().map_or(0, |_| {
        // without a redirect every read goes to the worker and is counted there
        or_connections(settings.max_inflight_queries)
    });
    let lookups = settings.max_concurrent_dns_lookups.get().saturating_mul(2);
    [connections, worker, queries, lookups]
        .into_iter()
        .map(|count| u64::try_from(count).unwrap_or(u64::MAX))
        .fold(RESERVE, u64::saturating_add)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::Parser as _;
    use std::{error::Error, fmt, io};

    /// Settings for an inline worker on another host plus `extra` flags.
    fn settings_with(extra: &[&str]) -> Settings {
        let mut argv = vec![
            "worker-gateway",
            "--upstream-rpc-url=http://10.0.0.7:8545",
            "--upstream-readiness-url=http://10.0.0.7:8551/health/workers",
        ];
        argv.extend_from_slice(extra);
        Cli::try_parse_from(argv).expect("argv parses").into_settings().expect("settings resolve")
    }

    #[test]
    fn demand_counts_every_cap() {
        // 500 connections + 100 worker requests + 2 x 8 lookups + the reserve
        assert_eq!(demand(&settings_with(&[])), 500 + 100 + 16 + 64);
        // a redirect adds the query route's 256
        let redirect = settings_with(&["--redirect-queries=http://10.0.0.9:8545/"]);
        assert_eq!(demand(&redirect), 500 + 100 + 256 + 16 + 64);
        // an unlimited upstream cap is bounded by the connections instead
        let unlimited = settings_with(&[
            "--redirect-queries=http://10.0.0.9:8545/",
            "--max-upstream-inflight=0",
            "--max-inflight-queries=0",
        ]);
        assert_eq!(demand(&unlimited), 500 + 500 + 500 + 16 + 64);
    }

    #[test]
    fn budget_warns_only_above_the_soft_limit() {
        let defaults = settings_with(&[]);
        assert_eq!(over_budget(&defaults, 1024), None);
        // demand equal to the limit still fits
        assert_eq!(over_budget(&defaults, 680), None);
        assert_eq!(over_budget(&defaults, 679), Some(680));
        let raised = settings_with(&["--max-connections=2000"]);
        assert_eq!(over_budget(&raised, 1024), Some(2000 + 100 + 16 + 64));
        // an unlimited hard limit, which fdlimit reports as u64::MAX, always fits
        assert_eq!(over_budget(&raised, u64::MAX), None);
    }

    /// A layer of an error chain, like hyper-util's connect error over the
    /// socket error.
    #[derive(Debug)]
    struct Layer(Option<Box<dyn Error + Send + Sync + 'static>>);

    impl fmt::Display for Layer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("tcp connect error")
        }
    }

    impl Error for Layer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            self.0.as_deref().map(|err| err as &(dyn Error + 'static))
        }
    }

    /// `cause` under two layers, as reqwest wraps a connect failure.
    fn chain(cause: Option<io::Error>) -> Layer {
        let cause = cause.map(|err| Box::new(err) as Box<dyn Error + Send + Sync>);
        Layer(Some(Box::new(Layer(cause))))
    }

    #[test]
    fn fd_exhaustion_is_not_the_workers_fault() {
        for errno in [EMFILE, ENFILE] {
            let err = chain(Some(io::Error::from_raw_os_error(errno)));
            assert!(is_exhausted(&err), "errno {errno} is the gateway's own exhaustion");
        }
        // a refused connect is the worker's port, and counts
        let refused = chain(Some(io::Error::from_raw_os_error(111)));
        assert!(!is_exhausted(&refused));
        // so does a connect failure with no io cause
        assert!(!is_exhausted(&chain(None)));
        assert!(!is_exhausted(&chain(Some(io::Error::other("tls handshake failed")))));
    }
}
