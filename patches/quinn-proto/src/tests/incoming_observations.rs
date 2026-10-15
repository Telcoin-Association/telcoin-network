//! Deterministic observations at real endpoint decision sites.

use super::{token::FakeTimeSource, *};
use crate::token::{IncomingToken, Token, TokenPayload};
use rand::{SeedableRng, rngs::StdRng};

/// Construct an observed endpoint using the existing packet-driven protocol harness.
fn observed_pair(limit: usize) -> (Pair, Arc<IncomingStats>) {
    let stats = Arc::new(IncomingStats::default());
    let mut endpoint = EndpointConfig::default();
    endpoint.incoming_stats(Arc::clone(&stats));
    let mut server = server_config();
    server.max_incoming(limit);
    let mut pair = Pair::new(Arc::new(endpoint), server);
    pair.server.handle_incoming = Box::new(|_| IncomingConnectionBehavior::Wait);
    (pair, stats)
}

/// Deliver a new client's Initial while holding the application decision open.
fn queued_initial(pair: &mut Pair) -> Result<Incoming, &'static str> {
    pair.begin_connect(client_config());
    pair.drive_client();
    pair.server.drive_incoming(pair.time, pair.client.addr);
    pair.server
        .waiting_incoming
        .pop()
        .ok_or("Initial did not reach incoming queue")
}

/// Every terminal disposition must release the actual endpoint slab entry.
#[derive(Clone, Copy)]
enum Disposition {
    /// Successful accept.
    Accept,
    /// Accept after the queued Initial's idle deadline.
    Timeout,
    /// Explicit connection refusal.
    Refuse,
    /// Explicit silent discard.
    Ignore,
    /// Stateless Retry response.
    Retry,
}

/// Occupancy is measured at insertion and removal, including failed accept.
#[test]
fn incoming_observations_release_every_disposition() -> Result<(), &'static str> {
    [
        Disposition::Accept,
        Disposition::Timeout,
        Disposition::Refuse,
        Disposition::Ignore,
        Disposition::Retry,
    ]
    .into_iter()
    .try_for_each(|disposition| {
        let (mut pair, stats) = observed_pair(7);
        let incoming = queued_initial(&mut pair)?;
        assert_eq!(stats.limit(), 7);
        assert_eq!(stats.endpoints(), 1);
        assert_eq!(stats.queued(), 1);
        match disposition {
            Disposition::Accept => {
                pair.server
                    .try_accept(incoming, pair.time)
                    .map_err(|_| "accept failed")?;
            }
            Disposition::Timeout => {
                assert!(matches!(
                    pair.server
                        .try_accept(incoming, pair.time + Duration::from_secs(3600)),
                    Err(ConnectionError::TimedOut)
                ));
            }
            Disposition::Refuse => {
                pair.server.refuse(incoming, &mut Vec::new());
            }
            Disposition::Ignore => pair.server.ignore(incoming),
            Disposition::Retry => {
                pair.server
                    .endpoint
                    .retry(incoming, &mut Vec::new())
                    .map_err(|_| "retry failed")?;
            }
        }
        assert_eq!(
            stats.count(IncomingEvent::Refused),
            u64::from(matches!(disposition, Disposition::Refuse))
        );
        assert_eq!(
            stats.count(IncomingEvent::Ignored),
            u64::from(matches!(disposition, Disposition::Ignore))
        );
        assert_eq!(
            stats.count(IncomingEvent::AcceptFailed),
            u64::from(matches!(disposition, Disposition::Timeout))
        );
        assert_eq!(stats.queued(), 0);
        drop(pair);
        assert_eq!(stats.limit(), 0);
        assert_eq!(stats.endpoints(), 0);
        Ok(())
    })
}

/// Saturation is observed before the app hook and remains bounded across many source addresses.
#[test]
fn incoming_observations_cap_and_shutdown() -> Result<(), &'static str> {
    let (mut pair, stats) = observed_pair(1);
    let incoming = queued_initial(&mut pair)?;
    (1..=1000).for_each(|port| {
        pair.client.addr.set_port(port);
        pair.client
            .addr
            .set_ip(Ipv6Addr::from(u128::from(port)).into());
        pair.begin_connect(client_config());
        pair.drive_client();
        pair.server.drive_incoming(pair.time, pair.client.addr);
    });
    assert_eq!(stats.queued(), 1);
    assert!(stats.count(IncomingEvent::QueueFull) >= 1000);
    assert_eq!(stats.count(IncomingEvent::InvalidRetryToken), 0);
    // An undecided incoming is owned by the endpoint; shutdown releases its slab entry.
    pair.server.waiting_incoming.push(incoming);
    drop(pair);
    assert_eq!(stats.queued(), 0);
    assert_eq!(stats.limit(), 0);
    assert_eq!(stats.endpoints(), 0);
    Ok(())
}

/// The effective default and replacement ceilings come from ServerConfig itself.
#[test]
fn incoming_observations_effective_configuration() {
    let (mut pair, stats) = observed_pair(3);
    let default = server_config();
    let expected = default.max_incoming;
    pair.server.set_server_config(Some(Arc::new(default)));
    assert_eq!(stats.limit(), expected);
    assert_eq!(stats.endpoints(), 1);
    pair.server.set_server_config(None);
    assert_eq!(stats.limit(), 0);
    assert_eq!(stats.endpoints(), 0);
}

/// Opaque and decoded-but-invalid address tokens are observable even when treated as absent.
#[test]
fn incoming_observations_fallback_tokens() -> Result<(), &'static str> {
    let (mut pair, stats) = observed_pair(7);
    let incoming = queued_initial(&mut pair)?;
    let mut header = crate::packet::InitialHeader {
        dst_cid: ConnectionId::new(&[0; 8]),
        src_cid: ConnectionId::new(&[1; 8]),
        token: Bytes::new(),
        number: crate::packet::PacketNumber::U8(0),
        version: 1,
    };
    let server = server_config();
    header.token = Bytes::from_static(b"not a token");
    let opaque = IncomingToken::from_header(&header, &server, pair.client.addr, Some(&stats))
        .map_err(|_| "opaque token should fall back")?;
    assert!(!opaque.validated);
    assert_eq!(stats.count(IncomingEvent::UndecodableToken), 1);
    let token = Token::new(
        TokenPayload::Validation {
            ip: Ipv4Addr::new(192, 0, 2, 1).into(),
            issued: SystemTime::now(),
        },
        &mut StdRng::seed_from_u64(7),
    );
    header.token = token.encode(&*server.token_key).into();
    let wrong_ip = IncomingToken::from_header(&header, &server, pair.client.addr, Some(&stats))
        .map_err(|_| "address token should fall back")?;
    assert!(!wrong_ip.validated);
    assert_eq!(stats.count(IncomingEvent::InvalidAddressToken), 1);
    pair.server.ignore(incoming);
    Ok(())
}

/// Real Retry returns and expired tokens are counted before application admission.
#[test]
fn incoming_observations_retry_return_and_invalid_token() {
    [false, true].into_iter().for_each(|expired| {
        let (mut pair, stats) = observed_pair(7);
        let time = Arc::new(FakeTimeSource::new());
        let mut server = server_config();
        server
            .time_source(time.clone())
            .retry_token_lifetime(Duration::from_secs(1));
        pair.server.set_server_config(Some(Arc::new(server)));
        pair.server.handle_incoming = Box::new(validate_incoming);
        pair.begin_connect(client_config());
        pair.drive_client();
        pair.drive_server();
        pair.drive_client();
        if expired {
            time.advance(Duration::from_secs(2));
        }
        pair.drive();
        assert_eq!(
            stats.count(IncomingEvent::RetryValidated),
            u64::from(!expired)
        );
        assert_eq!(
            stats.count(IncomingEvent::InvalidRetryToken),
            u64::from(expired)
        );
        assert_eq!(stats.count(IncomingEvent::AddressValidated), 0);
        assert_eq!(stats.queued(), 0);
    });
}
