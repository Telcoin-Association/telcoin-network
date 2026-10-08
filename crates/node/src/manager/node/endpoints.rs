//! Startup-only resolution of operator endpoints into finite signed IP records.

use eyre::{eyre, WrapErr as _};
use futures::TryStreamExt as _;
use std::{net::SocketAddr, time::Duration};
use tn_config::EndpointMapping;
use tn_network_libp2p::{validate_advertised_addresses, MAX_ADVERTISED_MULTIADDRS};
use tn_types::{Multiaddr, NetworkPublicKey, Protocol};

/// One DNS query's answer budget; excess answers fail startup rather than being silently retained.
const MAX_DNS_ANSWERS: usize = MAX_ADVERTISED_MULTIADDRS;
/// Maximum time spent awaiting any one startup DNS lookup.
const DNS_TIMEOUT: Duration = Duration::from_secs(5);

/// Supported operator-side DNS address families.
#[derive(Clone, Copy, Debug)]
enum DnsFamily {
    /// Retain at most one IPv4 and one IPv6 answer.
    Both,
    /// Retain at most one IPv4 answer.
    V4,
    /// Retain at most one IPv6 answer.
    V6,
}

/// Validate a local IP/QUIC listener; wildcard IPs are allowed only on this bind path.
pub(super) fn validate_listener(address: &Multiaddr, key: &NetworkPublicKey) -> eyre::Result<()> {
    matches!(address.iter().next(), Some(Protocol::Ip4(_) | Protocol::Ip6(_)))
        .then_some(())
        .ok_or_else(|| eyre!("listener must use a local IP address: {address}"))?;
    validate_configured(address, key)
}

/// Resolve a mapping once, serially, without DNS names in records or background refresh tasks.
///
/// At most four queries are started per mapping; timeouts cannot cause an unbounded retry loop.
/// `/dnsaddr`, TCP, relay paths and identities belonging to another worker are rejected.
pub(super) async fn resolve_advertised(
    mapping: Option<&EndpointMapping>,
    fallback: Multiaddr,
    key: &NetworkPublicKey,
) -> eyre::Result<Vec<Multiaddr>> {
    let configured = mapping.map(|mapping| mapping.advertise().to_vec()).unwrap_or(vec![fallback]);
    (!configured.is_empty() && configured.len() <= MAX_ADVERTISED_MULTIADDRS)
        .then_some(())
        .ok_or_else(|| eyre!("advertise requires one to {MAX_ADVERTISED_MULTIADDRS} endpoints"))?;
    configured.iter().try_for_each(|address| validate_configured(address, key))?;
    let addresses = futures::stream::iter(configured.into_iter().map(Ok::<_, eyre::Report>))
        .try_fold(Vec::new(), |mut addresses, address| async move {
            let resolved = resolve_one(address).await?;
            (addresses.len() + resolved.len() <= MAX_ADVERTISED_MULTIADDRS)
                .then_some(())
                .ok_or_else(|| {
                    eyre!("resolved advertise list exceeds {MAX_ADVERTISED_MULTIADDRS} endpoints")
                })?;
            addresses.extend(resolved);
            Ok(addresses)
        })
        .await?;
    validate_advertised_addresses(&addresses, key).map(|()| addresses).map_err(Into::into)
}

/// Check syntax before DNS work, accepting DNS/IP, UDP, QUIC and an optional matching identity.
fn validate_configured(address: &Multiaddr, key: &NetworkPublicKey) -> eyre::Result<()> {
    let mut protocols = address.iter();
    let host = protocols.next();
    let permitted_host = matches!(host.as_ref(), Some(Protocol::Ip4(_) | Protocol::Ip6(_)))
        || matches!(host.as_ref(), Some(Protocol::Dns(name) | Protocol::Dns4(name) | Protocol::Dns6(name))
            if !name.is_empty() && name.len() <= 253);
    (permitted_host
        && matches!(protocols.next(), Some(Protocol::Udp(port)) if port != 0)
        && matches!(protocols.next(), Some(Protocol::QuicV1)))
    .then_some(())
    .ok_or_else(|| eyre!("advertise endpoint must use IP or DNS with UDP/QUIC v1: {address}"))?;
    protocols
        .next()
        .is_none_or(|suffix| suffix == Protocol::P2p(key.clone().into()))
        .then_some(())
        .ok_or_else(|| eyre!("advertise endpoint has a different peer identity: {address}"))?;
    protocols
        .next()
        .is_none()
        .then_some(())
        .ok_or_else(|| eyre!("advertise endpoint contains extra protocols: {address}"))
}

/// Resolve one endpoint, preserving its transport and optional peer suffix.
async fn resolve_one(address: Multiaddr) -> eyre::Result<Vec<Multiaddr>> {
    let dns = address.iter().next().and_then(|host| {
        if let Protocol::Dns(name) = host {
            Some((name.into_owned(), DnsFamily::Both))
        } else if let Protocol::Dns4(name) = host {
            Some((name.into_owned(), DnsFamily::V4))
        } else if let Protocol::Dns6(name) = host {
            Some((name.into_owned(), DnsFamily::V6))
        } else {
            None
        }
    });
    if dns.is_some() {
        let (host, family) = dns.ok_or_else(|| eyre!("missing DNS host"))?;
        resolve_dns(tokio::net::lookup_host((host.as_str(), 0)), family)
            .await
            .wrap_err_with(|| format!("advertise DNS lookup failed for {host}"))
            .map(|selected| {
                selected
                    .into_iter()
                    .map(|answer| {
                        let ip = match answer.ip() {
                            std::net::IpAddr::V4(ip) => Protocol::Ip4(ip),
                            std::net::IpAddr::V6(ip) => Protocol::Ip6(ip),
                        };
                        std::iter::once(ip).chain(address.iter().skip(1)).collect()
                    })
                    .collect()
            })
    } else {
        Ok(vec![address])
    }
}

/// Bound the lifetime and answer set of a DNS operation, including an unresponsive resolver.
async fn resolve_dns<I>(
    lookup: impl std::future::Future<Output = std::io::Result<I>>,
    family: DnsFamily,
) -> eyre::Result<Vec<SocketAddr>>
where
    I: Iterator<Item = SocketAddr>,
{
    tokio::time::timeout(DNS_TIMEOUT, lookup)
        .await
        .wrap_err("advertise DNS lookup timed out")?
        .wrap_err("advertise DNS lookup failed")
        .and_then(|answers| select_dns_answers(answers, family))
}

#[cfg(test)]
#[path = "endpoints_tests.rs"]
mod tests;

/// Select the lowest IP in each permitted family from a finite answer set.
fn select_dns_answers(
    answers: impl Iterator<Item = SocketAddr>,
    family: DnsFamily,
) -> eyre::Result<Vec<SocketAddr>> {
    let mut answers: Vec<_> = answers.take(MAX_DNS_ANSWERS + 1).collect();
    (answers.len() <= MAX_DNS_ANSWERS)
        .then_some(())
        .ok_or_else(|| eyre!("advertise DNS answer count exceeds {MAX_DNS_ANSWERS}"))?;
    answers.sort_unstable();
    let selected = answers.into_iter().fold(Vec::<SocketAddr>::new(), |mut selected, answer| {
        let permitted = match family {
            DnsFamily::Both => true,
            DnsFamily::V4 => answer.is_ipv4(),
            DnsFamily::V6 => answer.is_ipv6(),
        };
        if permitted && !selected.iter().any(|prior| prior.is_ipv4() == answer.is_ipv4()) {
            selected.push(answer);
        }
        selected
    });
    (!selected.is_empty())
        .then_some(selected)
        .ok_or_else(|| eyre!("advertise DNS has no answers in the requested address family"))
}
