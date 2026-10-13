//! Deterministic endpoint migration and resolver-budget regressions.

use super::*;
use tn_types::NetworkKeypair;

/// Worker mappings preserve their independent listeners and authenticate each worker's own key.
#[tokio::test]
async fn workers_have_independent_endpoint_mappings() -> eyre::Result<()> {
    let keys = [
        NetworkKeypair::ed25519_from_bytes([26; 32])?.public().into(),
        NetworkKeypair::ed25519_from_bytes([27; 32])?.public().into(),
    ];
    let configuration: tn_config::NetworkConfig = serde_json::from_value(serde_json::json!({
        "endpoints": {"workers": {
            "0": {"listen": "/ip4/127.0.0.1/udp/9001/quic-v1", "advertise": ["/ip4/192.0.2.1/udp/9001/quic-v1"]},
            "1": {"listen": "/ip4/127.0.0.1/udp/9002/quic-v1", "advertise": ["/ip4/192.0.2.2/udp/9002/quic-v1"]}
        }}
    }))?;
    let workers = configuration.endpoints().workers();
    assert_eq!(workers.len(), 2);
    assert_ne!(
        workers.get(&0).and_then(EndpointMapping::listen),
        workers.get(&1).and_then(EndpointMapping::listen)
    );
    futures::stream::iter(workers.values().zip(keys.iter()).map(Ok::<_, eyre::Report>))
        .try_for_each(|(mapping, key)| async move {
            let listener =
                mapping.listen().cloned().ok_or_else(|| eyre!("missing worker listener"))?;
            let resolved = resolve_advertised(Some(mapping), listener.clone(), key).await?;
            assert!(!resolved.contains(&listener));
            let advertised =
                resolved.first().cloned().ok_or_else(|| eyre!("missing worker advertisement"))?;
            let suffixed = advertised
                .with_p2p(key.clone().into())
                .map_err(|_| eyre!("invalid worker identity"))?;
            validate_advertised_addresses(&[suffixed], key)?;
            Ok(())
        })
        .await
}

/// DNS selection is deterministic and rejects excess answers and absent address families.
#[test]
fn dns_selection_is_bounded_and_family_specific() -> eyre::Result<()> {
    let answers = ["192.0.2.2:0", "[2001:db8::2]:0", "192.0.2.1:0", "[2001:db8::1]:0"]
        .into_iter()
        .map(str::parse)
        .collect::<Result<Vec<SocketAddr>, _>>()?;
    let both = select_dns_answers(answers.clone().into_iter(), DnsFamily::Both)?;
    assert_eq!(both.len(), 2);
    assert_eq!(both.first().map(ToString::to_string).as_deref(), Some("192.0.2.1:0"));
    assert_eq!(both.last().map(ToString::to_string).as_deref(), Some("[2001:db8::1]:0"));
    assert!(select_dns_answers(answers.clone().into_iter(), DnsFamily::V4)?
        .iter()
        .all(SocketAddr::is_ipv4));
    assert!(select_dns_answers(answers.into_iter(), DnsFamily::V6)?
        .iter()
        .all(SocketAddr::is_ipv6));
    let address: SocketAddr = "192.0.2.1:0".parse()?;
    assert!(select_dns_answers(std::iter::empty(), DnsFamily::Both).is_err());
    assert!(select_dns_answers(std::iter::repeat_n(address, MAX_DNS_ANSWERS + 1), DnsFamily::Both)
        .is_err());
    assert!(select_dns_answers(std::iter::once(address), DnsFamily::V6).is_err());
    Ok(())
}

/// An unresponsive resolver reaches the deadline using a paused clock.
#[tokio::test(start_paused = true)]
async fn dns_resolution_has_a_deadline() {
    let started = tokio::time::Instant::now();
    let lookup = futures::future::pending::<std::io::Result<std::vec::IntoIter<SocketAddr>>>();
    let error = resolve_dns(lookup, DnsFamily::Both).await.err();
    assert!(error.is_some_and(|error| error.to_string().contains("timed out")));
    assert_eq!(started.elapsed(), Duration::from_secs(5));
}

/// A mapping supports a wildcard listener, overlap, retirement and rollback without DNS work.
#[tokio::test]
async fn mappings_cover_overlap_retirement_and_rollback() -> eyre::Result<()> {
    let key: NetworkPublicKey = NetworkKeypair::ed25519_from_bytes([23; 32])?.public().into();
    let fallback: Multiaddr = "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?;
    let mapping: EndpointMapping = serde_json::from_value(serde_json::json!({
        "listen": "/ip4/0.0.0.0/udp/9000/quic-v1",
        "advertise": ["/ip4/192.0.2.1/udp/9000/quic-v1", "/ip4/192.0.2.2/udp/9000/quic-v1"]
    }))?;
    let listener = mapping.listen().ok_or_else(|| eyre!("missing listener"))?;
    validate_listener(listener, &key)?;
    let overlap = resolve_advertised(Some(&mapping), fallback.clone(), &key).await?;
    assert_eq!(overlap.len(), 2);
    assert!(!overlap.contains(listener));
    let old = overlap.first().cloned().ok_or_else(|| eyre!("missing old endpoint"))?;
    let new = overlap.last().cloned().ok_or_else(|| eyre!("missing new endpoint"))?;
    let retirement: EndpointMapping =
        serde_json::from_value(serde_json::json!({"advertise": [new.to_string()]}))?;
    let rollback: EndpointMapping =
        serde_json::from_value(serde_json::json!({"advertise": [old.to_string()]}))?;
    assert_eq!(resolve_advertised(Some(&retirement), fallback.clone(), &key).await?, vec![new]);
    assert_eq!(resolve_advertised(Some(&rollback), fallback.clone(), &key).await?, vec![old]);
    assert_eq!(resolve_advertised(None, fallback.clone(), &key).await?, vec![fallback]);
    Ok(())
}

/// Excess operator lists and cross-worker identities fail before a DNS query is started.
#[tokio::test]
async fn mappings_reject_excess_and_foreign_identities() -> eyre::Result<()> {
    let key: NetworkPublicKey = NetworkKeypair::ed25519_from_bytes([24; 32])?.public().into();
    let other: NetworkPublicKey = NetworkKeypair::ed25519_from_bytes([25; 32])?.public().into();
    let fallback: Multiaddr = "/ip4/127.0.0.1/udp/9000/quic-v1".parse()?;
    let excessive: EndpointMapping = serde_json::from_value(serde_json::json!({
        "advertise": std::iter::repeat_n("/dns/must-not-resolve.invalid/udp/9000/quic-v1", MAX_ADVERTISED_MULTIADDRS + 1).collect::<Vec<_>>()
    }))?;
    let error = resolve_advertised(Some(&excessive), fallback.clone(), &key).await.err();
    assert!(error.is_some_and(|error| error.to_string().contains("requires one to")));
    let foreign =
        fallback.clone().with_p2p(other.into()).map_err(|_| eyre!("invalid fixture suffix"))?;
    let foreign: EndpointMapping =
        serde_json::from_value(serde_json::json!({"advertise": [foreign.to_string()]}))?;
    assert!(resolve_advertised(Some(&foreign), fallback, &key).await.is_err());
    Ok(())
}
