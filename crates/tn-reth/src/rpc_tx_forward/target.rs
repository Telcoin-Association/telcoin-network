//! The validator RPC targets that `--forward-txs` relays raw transaction submissions to.
//!
//! [`parse_forward_targets`] takes the whole comma-separated flag value, so a malformed entry,
//! a duplicate, or an over-long list is a clap error at startup. Each trimmed entry is one of:
//!
//! - a full `http://` or `https://` URL. Without a port it keeps the scheme's default port, so an
//!   `https` URL with no port dials 443. A path is allowed for reverse proxies. `user:password`
//!   userinfo is sent as a basic-auth header; a username alone is refused, since the client sends
//!   no header without a password. Query strings and fragments are refused.
//! - a bare IPv6 address, which dials port 8545. A bare IPv6 address cannot carry a port: the last
//!   group parses as part of the address, so `2001:db8::1:8566` is that address on 8545. Bracket it
//!   (`[2001:db8::1]:8566`) to name a port.
//! - `ip:port` or `[ipv6]:port`.
//! - `[ipv6]` without a port, which dials 8545.
//! - `host` or `host:port`, where the host is an IPv4 address or a domain name and the port
//!   defaults to 8545.
//!
//! An entry without a scheme dials plain `http://`. Domain names are not resolved here: the
//! name is kept so TLS can verify it and a DNS change is picked up on the next connection.
//!
//! The list keeps the operator's order, which is the failover order. Entries that normalize to
//! the same URL are refused, and so is a list longer than [`MAX_TARGETS`].
//!
//! A target is private operator configuration: the validator behind it firewalls its RPC to the
//! forwarding nodes. [`ForwardTarget`]'s `Debug` therefore prints no part of the URL, and
//! [`TxForwardConfig`]'s prints only the target count.

use std::{
    fmt,
    net::{Ipv6Addr, SocketAddr},
};

use reth::rpc::builder::constants::DEFAULT_HTTP_RPC_PORT;
use url::{Host, Url};

/// The most targets one `--forward-txs` list may name.
///
/// Every target holds its own connection pool, and an unreachable target costs a submission up
/// to one attempt timeout before the next is tried, so a long list buys nothing but latency.
pub(crate) const MAX_TARGETS: usize = 8;

/// One normalized validator RPC endpoint.
///
/// `Debug` is redacted: the URL may carry credentials, and its host is the address the
/// validator keeps private.
#[derive(Clone, PartialEq, Eq)]
pub struct ForwardTarget(Url);

impl ForwardTarget {
    /// The normalized URL the forwarding client dials.
    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Debug for ForwardTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ForwardTarget(<redacted>)")
    }
}

/// The ordered failover list parsed from `--forward-txs`.
///
/// Never empty, at most [`MAX_TARGETS`] long, and free of duplicates after normalization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForwardTargets(Vec<ForwardTarget>);

impl ForwardTargets {
    /// The targets in failover order.
    pub(crate) fn as_slice(&self) -> &[ForwardTarget] {
        &self.0
    }
}

/// Why a `--forward-txs` value was refused.
///
/// Positions are 1-based entries in the comma-separated list. No variant echoes the entry
/// itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ForwardTargetError {
    /// An entry is empty or whitespace, including a leading, doubled, or trailing comma.
    #[error("forward target {position} is empty")]
    Empty {
        /// The entry's 1-based position in the list.
        position: usize,
    },
    /// An entry is not a URL, an IP address, or `host[:port]`.
    #[error("forward target {position} is not a URL, an IP address, or host[:port]: {reason}")]
    Invalid {
        /// The entry's 1-based position in the list.
        position: usize,
        /// What was wrong with it.
        reason: String,
    },
    /// A URL entry names a scheme other than `http` or `https`.
    #[error("forward target {position} must use http:// or https://")]
    UnsupportedScheme {
        /// The entry's 1-based position in the list.
        position: usize,
    },
    /// An entry names port 0.
    #[error("forward target {position} names port 0")]
    ZeroPort {
        /// The entry's 1-based position in the list.
        position: usize,
    },
    /// A URL entry carries a query string or a fragment.
    #[error("forward target {position} must not carry a query or fragment")]
    QueryOrFragment {
        /// The entry's 1-based position in the list.
        position: usize,
    },
    /// Two entries normalize to the same endpoint.
    #[error("forward targets {first} and {second} name the same endpoint")]
    Duplicate {
        /// The earlier entry's 1-based position.
        first: usize,
        /// The later entry's 1-based position.
        second: usize,
    },
    /// The list names more than [`MAX_TARGETS`] entries.
    #[error("{count} forward targets given, at most {max} are allowed")]
    TooMany {
        /// How many entries the list has.
        count: usize,
        /// The limit.
        max: usize,
    },
}

/// The node's transaction-forwarding settings from `--forward-txs` and `--sanitize-txs`.
///
/// `Default` is forwarding off. `Debug` prints the number of targets, never the targets.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct TxForwardConfig {
    /// The ordered failover list, or `None` when submissions go to the local pool.
    pub targets: Option<ForwardTargets>,
    /// Decode and check each transaction locally before forwarding it.
    pub sanitize: bool,
}

impl fmt::Debug for TxForwardConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TxForwardConfig")
            .field("targets", &self.targets.as_ref().map_or(0, |targets| targets.0.len()))
            .field("sanitize", &self.sanitize)
            .finish()
    }
}

/// Parse a `--forward-txs` value: a comma-separated, ordered list of validator RPC targets.
///
/// See the module docs for the accepted forms. The whole list is refused if any entry is
/// invalid, two entries normalize to the same URL, or there are more than [`MAX_TARGETS`].
pub(crate) fn parse_forward_targets(value: &str) -> Result<ForwardTargets, ForwardTargetError> {
    let entries: Vec<&str> = value.split(',').map(str::trim).collect();
    if entries.len() > MAX_TARGETS {
        return Err(ForwardTargetError::TooMany { count: entries.len(), max: MAX_TARGETS });
    }
    let targets = entries
        .into_iter()
        .enumerate()
        .map(|(index, entry)| parse_entry(index + 1, entry).map(ForwardTarget))
        .collect::<Result<Vec<_>, _>>()?;
    targets.iter().enumerate().try_for_each(|(index, target)| {
        targets[..index].iter().position(|earlier| earlier.0 == target.0).map_or(Ok(()), |first| {
            Err(ForwardTargetError::Duplicate { first: first + 1, second: index + 1 })
        })
    })?;
    Ok(ForwardTargets(targets))
}

/// Normalize one trimmed entry into the URL the client dials.
///
/// The bare-IPv6 check runs before any `host:port` split, since an IPv6 address is itself a
/// run of colon-separated groups.
fn parse_entry(position: usize, entry: &str) -> Result<Url, ForwardTargetError> {
    let invalid = |reason: &dyn fmt::Display| ForwardTargetError::Invalid {
        position,
        reason: reason.to_string(),
    };
    if entry.is_empty() {
        return Err(ForwardTargetError::Empty { position });
    }
    if entry.contains("://") {
        return parse_url_entry(position, entry);
    }
    if let Ok(ip) = entry.parse::<Ipv6Addr>() {
        return with_default_port(&format!("[{ip}]")).map_err(|e| invalid(&e));
    }
    if let Ok(socket) = entry.parse::<SocketAddr>() {
        if socket.port() == 0 {
            return Err(ForwardTargetError::ZeroPort { position });
        }
        return Url::parse(&format!("http://{socket}/")).map_err(|e| invalid(&e));
    }
    if let Some(inner) = entry.strip_prefix('[').and_then(|rest| rest.strip_suffix(']')) {
        let ip = inner.parse::<Ipv6Addr>().map_err(|e| invalid(&e))?;
        return with_default_port(&format!("[{ip}]")).map_err(|e| invalid(&e));
    }
    parse_host_entry(position, entry)
}

/// Validate an entry that names its own scheme.
fn parse_url_entry(position: usize, entry: &str) -> Result<Url, ForwardTargetError> {
    let url = Url::parse(entry)
        .map_err(|e| ForwardTargetError::Invalid { position, reason: e.to_string() })?;
    match () {
        () if !matches!(url.scheme(), "http" | "https") => {
            Err(ForwardTargetError::UnsupportedScheme { position })
        }
        () if url.host().is_none() => {
            Err(ForwardTargetError::Invalid { position, reason: "missing host".to_string() })
        }
        () if url.port() == Some(0) => Err(ForwardTargetError::ZeroPort { position }),
        () if url.query().is_some() || url.fragment().is_some() => {
            Err(ForwardTargetError::QueryOrFragment { position })
        }
        // the client builds the basic-auth header only when a password is present and strips
        // the username either way, so a username alone would be dropped without a word
        () if !url.username().is_empty() && url.password().is_none() => {
            Err(ForwardTargetError::Invalid {
                position,
                reason: "userinfo needs user:password; a username alone is not sent".to_string(),
            })
        }
        () => Ok(url),
    }
}

/// Validate a scheme-less `host` or `host:port` entry, where the host is an IPv4 address or a
/// domain name.
///
/// The port is split off by hand rather than left to the URL parser, which drops a port equal
/// to the scheme default: `host:80` must dial 80, not fall back to 8545.
fn parse_host_entry(position: usize, entry: &str) -> Result<Url, ForwardTargetError> {
    let invalid =
        |reason: &str| ForwardTargetError::Invalid { position, reason: reason.to_string() };
    let (host, port) = match entry.rsplit_once(':') {
        Some((host, port)) => (host, port.parse::<u16>().map_err(|_| invalid("invalid port"))?),
        None => (entry, DEFAULT_HTTP_RPC_PORT),
    };
    if port == 0 {
        return Err(ForwardTargetError::ZeroPort { position });
    }
    let mut url = Url::parse(&format!("http://{host}/"))
        .map_err(|e| ForwardTargetError::Invalid { position, reason: e.to_string() })?;
    if !matches!(url.host(), Some(Host::Ipv4(_) | Host::Domain(_))) {
        return Err(invalid("the host must be an IPv4 address or a domain name"));
    }
    if url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid("an entry without a scheme is host[:port] only; use a full URL"));
    }
    url.set_port(Some(port)).map_err(|()| invalid("invalid port"))?;
    Ok(url)
}

/// `http://{host}:8545/` for a host that names no port.
fn with_default_port(host: &str) -> Result<Url, url::ParseError> {
    Url::parse(&format!("http://{host}:{DEFAULT_HTTP_RPC_PORT}/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a one-entry list and return its normalized URL.
    fn normalized(entry: &str) -> String {
        let targets = parse_forward_targets(entry).expect("valid target");
        assert_eq!(targets.0.len(), 1);
        targets.0[0].0.to_string()
    }

    #[test]
    fn test_parse_target_https_url_keeps_scheme_default_port() {
        let targets = parse_forward_targets("https://node1.telcoin.network").expect("valid");
        let url = &targets.0[0].0;
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.port_or_known_default(), Some(443));
        assert_eq!(url.as_str(), "https://node1.telcoin.network/");
        // a plain http url keeps 80 the same way
        assert_eq!(normalized("http://node1.telcoin.network"), "http://node1.telcoin.network/");
    }

    #[test]
    fn test_parse_target_url_with_port_and_path() {
        assert_eq!(
            normalized("https://proxy.example.com:9443/tn/rpc"),
            "https://proxy.example.com:9443/tn/rpc"
        );
        // userinfo is kept for the basic-auth header
        assert_eq!(normalized("http://user:pw@10.0.0.1:8545"), "http://user:pw@10.0.0.1:8545/");
    }

    /// The client sends userinfo only as `user:password`, so a username alone is refused at
    /// startup instead of being dropped on every request.
    #[test]
    fn test_parse_target_refuses_username_without_password() {
        assert!(
            matches!(
                parse_forward_targets("https://token@node1.telcoin.network"),
                Err(ForwardTargetError::Invalid { position: 1, .. })
            ),
            "a username alone must be refused"
        );
        assert_eq!(
            normalized("https://user:pw@node1.telcoin.network"),
            "https://user:pw@node1.telcoin.network/"
        );
    }

    #[test]
    fn test_parse_target_ipv4_with_port() {
        assert_eq!(normalized("25.22.57.112:8566"), "http://25.22.57.112:8566/");
    }

    #[test]
    fn test_parse_target_ipv4_without_port_defaults_8545() {
        assert_eq!(normalized("25.22.57.112"), "http://25.22.57.112:8545/");
    }

    #[test]
    fn test_parse_target_bare_ipv6_defaults_8545() {
        assert_eq!(normalized("2001:0db8::1428:57ab"), "http://[2001:db8::1428:57ab]:8545/");
        assert_eq!(normalized("::1"), "http://[::1]:8545/");
    }

    #[test]
    fn test_parse_target_bracketed_ipv6_with_and_without_port() {
        assert_eq!(normalized("[2001:db8::1]:8566"), "http://[2001:db8::1]:8566/");
        assert_eq!(normalized("[2001:db8::1]"), "http://[2001:db8::1]:8545/");
    }

    /// Documented behaviour: the trailing group of a bare IPv6 address is part of the address,
    /// never a port.
    #[test]
    fn test_parse_target_bare_ipv6_cannot_carry_port() {
        assert_eq!(normalized("2001:db8::1:8566"), "http://[2001:db8::1:8566]:8545/");
    }

    #[test]
    fn test_parse_target_hostname_with_and_without_port() {
        assert_eq!(normalized("validator.example.com"), "http://validator.example.com:8545/");
        assert_eq!(normalized("validator.example.com:9000"), "http://validator.example.com:9000/");
        assert_eq!(normalized("localhost"), "http://localhost:8545/");
        // an explicit port equal to the http default is kept, not replaced by 8545
        assert_eq!(normalized("validator.example.com:80"), "http://validator.example.com/");
    }

    #[test]
    fn test_parse_target_rejects_invalid() {
        let cases: [(&str, ForwardTargetError); 6] = [
            ("", ForwardTargetError::Empty { position: 1 }),
            ("1.2.3.4,,5.6.7.8", ForwardTargetError::Empty { position: 2 }),
            ("1.2.3.4,", ForwardTargetError::Empty { position: 2 }),
            ("ws://1.2.3.4:8546", ForwardTargetError::UnsupportedScheme { position: 1 }),
            ("ftp://host.example.com", ForwardTargetError::UnsupportedScheme { position: 1 }),
            ("http://1.2.3.4:8545/?key=1", ForwardTargetError::QueryOrFragment { position: 1 }),
        ];
        for (value, expected) in cases {
            assert_eq!(parse_forward_targets(value), Err(expected), "value {value:?}");
        }

        let zero_port = ["1.2.3.4:0", "http://1.2.3.4:0", "host.example.com:0", "[::1]:0"];
        for value in zero_port {
            assert_eq!(
                parse_forward_targets(value),
                Err(ForwardTargetError::ZeroPort { position: 1 }),
                "value {value:?}"
            );
        }
        assert_eq!(
            parse_forward_targets("http://1.2.3.4#frag"),
            Err(ForwardTargetError::QueryOrFragment { position: 1 })
        );

        let invalid = [
            "1.2.3.4:70000",
            "http://1.2.3.4:70000",
            "host.example.com/path",
            "host.example.com:8545/path",
            "user@host.example.com",
            "host.example.com:",
            "[not-an-ip]",
            "http://",
        ];
        for value in invalid {
            assert!(
                matches!(
                    parse_forward_targets(value),
                    Err(ForwardTargetError::Invalid { position: 1, .. })
                ),
                "value {value:?} must be refused as invalid"
            );
        }
    }

    #[test]
    fn test_parse_targets_preserves_order_and_trims() {
        let targets = parse_forward_targets(" 10.0.0.2:8545 , https://b.example.com,10.0.0.1 ")
            .expect("valid list");
        let urls: Vec<String> = targets.0.iter().map(|target| target.0.to_string()).collect();
        assert_eq!(
            urls,
            ["http://10.0.0.2:8545/", "https://b.example.com/", "http://10.0.0.1:8545/"]
        );
    }

    #[test]
    fn test_parse_targets_rejects_duplicates_after_normalization() {
        assert_eq!(
            parse_forward_targets("1.2.3.4,http://1.2.3.4:8545"),
            Err(ForwardTargetError::Duplicate { first: 1, second: 2 })
        );
        assert_eq!(
            parse_forward_targets(
                "10.0.0.9,http://NODE.example.com,https://x.example.com,node.example.com:80"
            ),
            Err(ForwardTargetError::Duplicate { first: 2, second: 4 })
        );
    }

    #[test]
    fn test_parse_targets_rejects_more_than_max() {
        let at_max: Vec<String> = (1..=MAX_TARGETS).map(|i| format!("10.0.0.{i}")).collect();
        assert_eq!(parse_forward_targets(&at_max.join(",")).expect("max is allowed").0.len(), 8);

        let over: Vec<String> = (1..=MAX_TARGETS + 1).map(|i| format!("10.0.0.{i}")).collect();
        assert_eq!(
            parse_forward_targets(&over.join(",")),
            Err(ForwardTargetError::TooMany { count: MAX_TARGETS + 1, max: MAX_TARGETS })
        );
    }

    /// No `Debug` path prints a host, port, or credential.
    #[test]
    fn test_forward_target_debug_redacts() {
        let targets =
            parse_forward_targets("https://user:secret@node1.telcoin.network:9443,10.1.2.3:8566")
                .expect("valid list");
        let config = TxForwardConfig { targets: Some(targets.clone()), sanitize: true };
        for printed in [
            format!("{:?}", targets.0[0]),
            format!("{targets:?}"),
            format!("{:?}", Some(&targets)),
            format!("{config:?}"),
            format!("{config:#?}"),
        ] {
            for secret in ["node1", "telcoin", "9443", "user", "secret", "10.1.2.3", "8566"] {
                assert!(!printed.contains(secret), "{printed:?} leaks {secret:?}");
            }
        }
        assert_eq!(format!("{config:?}"), "TxForwardConfig { targets: 2, sanitize: true }");
    }
}
