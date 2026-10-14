//! Candidate fixtures using the node's carried QUIC patch and incoming limit derivation.

use bytes::Bytes;
use libp2p::identity::Keypair;
use libp2p_quic::{Config, IncomingStats};
use serde_json::{json, Value};
use std::{
    fmt,
    net::{Ipv4Addr, SocketAddr},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[path = "../../../crates/network-libp2p/src/quic_incoming.rs"]
mod incoming;

#[cfg(test)]
#[path = "../../../crates/config/src/network/quic.rs"]
mod settings;

/// Watchdog for a fixture operation, not a latency acceptance threshold.
const WATCHDOG: Duration = Duration::from_secs(30);

/// Explicit fixture failures at the process and transport boundary.
#[derive(Debug)]
pub enum Error {
    /// Invalid fixture configuration or an unexpected protocol result.
    Setup(String),
    /// Socket or artifact I/O failed.
    Io(std::io::Error),
    /// A QUIC connection failed.
    Connection(quinn::ConnectionError),
    /// A connection could not be initiated.
    Connect(quinn::ConnectError),
    /// A fixture watchdog expired.
    Timeout(tokio::time::error::Elapsed),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for Error {}

/// Bounded observations for one listener, including polls that return no connection event.
pub struct Observer {
    /// The exact counters installed in the candidate transport.
    stats: Arc<IncomingStats>,
    /// Last emitted aggregate counters, to avoid redundant records.
    previous: (u64, u64, u64, u64, u64),
    /// Effective incoming settings recorded before the endpoint starts.
    configuration: Value,
}

impl Observer {
    /// Apply the node's shared incoming-limit derivation to the candidate.
    ///
    /// The default node has 45 priority-peer slots and eight connections per peer.
    /// Overrides must come from the candidate node's configuration, not a guessed load limit.
    pub fn new(config: &mut Config, retry: bool, peers: usize, connections: u32) -> Self {
        let stats = Arc::new(IncomingStats::default());
        incoming::QuicIncomingLimits::new(peers, connections).apply(
            config,
            retry,
            Arc::clone(&stats),
        );
        Self {
            stats,
            previous: (0, 0, 0, 0, 0),
            configuration: json!({"applied": true, "requested_retry": retry,
                "retry_unvalidated_incoming": config.retry_unvalidated_incoming,
                "priority_peers": peers, "connections_per_peer": connections,
                "max_incoming": config.max_incoming,
                "incoming_buffer_size": config.incoming_buffer_size,
                "incoming_buffer_size_total": config.incoming_buffer_size_total,
                "outcomes_per_poll": config.max_incoming_outcomes_per_poll}),
        }
    }

    /// Effective listener policy, distinct from the stock transport configuration.
    pub fn configuration(&self) -> Value {
        self.configuration.clone()
    }

    /// Emit only a changed aggregate observation, with no address or token labels.
    pub fn record(&mut self) -> Option<Value> {
        let current = (
            self.stats.retried(),
            self.stats.accepted(),
            self.stats.refused(),
            self.stats.ignored(),
            self.stats.budget_yields(),
        );
        let changed = current != self.previous;
        self.previous = current;
        changed.then(|| {
            json!({"event": "outcomes", "retried": current.0,
            "accepted": current.1, "refused": current.2, "ignored": current.3,
            "budget_yields": current.4})
        })
    }
}

/// One supplied token and the last NEW_TOKEN received from a server.
#[derive(Default)]
struct Tokens {
    /// A token supplied once when establishing a fresh connection.
    supplied: Mutex<Option<Bytes>>,
    /// A bounded slot for the most recent server-issued validation token.
    received: Mutex<Option<Bytes>>,
    /// Notification that the server sent a validation token.
    ready: tokio::sync::Notify,
}

impl Tokens {
    /// Supply an opaque token without pretending that validation tokens are Retry tokens.
    #[cfg(test)]
    fn new(token: Option<Bytes>) -> Self {
        Self { supplied: Mutex::new(token), ..Self::default() }
    }

    /// Wait for an actual NEW_TOKEN frame rather than a scheduling delay.
    #[cfg(test)]
    async fn received(&self) -> Result<Bytes, Error> {
        tokio::time::timeout(WATCHDOG, self.ready.notified()).await.map_err(Error::Timeout)?;
        self.received
            .lock()
            .map_err(|_| Error::Setup("token lock poisoned".into()))?
            .take()
            .ok_or_else(|| Error::Setup("token notification without token".into()))
    }
}

impl quinn_proto::TokenStore for Tokens {
    fn insert(&self, _server: &str, token: Bytes) {
        let _stored = self.received.lock().map(|mut slot| *slot = Some(token));
        self.ready.notify_one();
    }

    fn take(&self, _server: &str) -> Option<Bytes> {
        self.supplied.lock().ok().and_then(|mut token| token.take())
    }
}

/// Create a real libp2p TLS client, so every emitted Initial has a valid first flight.
fn client_config(tokens: Arc<Tokens>) -> Result<quinn::ClientConfig, Error> {
    let tls = libp2p_tls::make_client_config(&Keypair::generate_ed25519(), None)
        .map_err(|error| Error::Setup(format!("{error}")))?;
    let crypto = quinn::crypto::rustls::QuicClientConfig::try_from(tls)
        .map_err(|error| Error::Setup(format!("{error}")))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.token_store(tokens);
    Ok(config)
}

/// Capture an Initial from a real client at a loopback sink, without altering its ciphertext.
pub async fn write_initial(path: &Path) -> Result<(), Error> {
    initial_packet().await.and_then(|packet| std::fs::write(path, packet).map_err(Error::Io))
}

/// Obtain ciphertext from a real TLS client for controlled packet experiments.
async fn initial_packet() -> Result<Vec<u8>, Error> {
    let sink = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.map_err(Error::Io)?;
    let mut endpoint =
        quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).map_err(Error::Io)?;
    endpoint.set_default_client_config(client_config(Arc::new(Tokens::default()))?);
    let connecting = endpoint
        .connect(sink.local_addr().map_err(Error::Io)?, "localhost")
        .map_err(Error::Connect)?;
    let mut datagram = vec![0; 65535];
    let size = tokio::time::timeout(WATCHDOG, sink.recv(&mut datagram))
        .await
        .map_err(Error::Timeout)?
        .map_err(Error::Io)?;
    datagram.truncate(size);
    drop(connecting);
    endpoint.close(0u32.into(), b"fixture done");
    endpoint.wait_idle().await;
    Ok(datagram)
}

#[cfg(test)]
mod tests {
    //! Real Quinn and production listener regressions for release qualification.
    use super::*;
    use futures::{
        future::{join_all, poll_fn},
        stream::{self, FuturesUnordered},
        task::{waker, ArcWake},
        StreamExt, TryStreamExt,
    };
    use libp2p::core::{
        transport::{ListenerId, TransportEvent},
        Transport,
    };
    use std::{
        pin::Pin,
        sync::atomic::{AtomicU64, Ordering},
        task::{Poll, Waker},
    };
    use tokio::sync::watch;

    /// A listener upgrade driven concurrently with continued incoming polling.
    type Upgrade = <libp2p_quic::tokio::Transport as Transport>::ListenerUpgrade;

    /// Outcomes observed during a poll, and violations of its cooperative-work contract.
    #[derive(Clone, Copy, Default)]
    struct Progress {
        /// Reachability challenges sent before any handshake acceptance.
        retried: u64,
        /// Attempts that reached handshake acceptance.
        accepted: u64,
        /// Polls that exceeded the cap or failed to wake at the cap.
        violations: u64,
    }

    /// Count wakeups while forwarding every wake to the real executor.
    struct WakeProbe {
        /// Original task waker.
        parent: Waker,
        /// Wake calls observed during this poll.
        calls: AtomicU64,
    }

    impl ArcWake for WakeProbe {
        fn wake_by_ref(probe: &Arc<Self>) {
            probe.calls.fetch_add(1, Ordering::SeqCst);
            probe.parent.wake_by_ref();
        }
    }

    /// A transport event or an independently completed listener upgrade.
    enum Event {
        /// The real patched transport emitted an event.
        Transport(TransportEvent<Upgrade, libp2p_quic::Error>),
        /// An accepted handshake completed, or its peer rejected it.
        Upgrade(Result<(libp2p::PeerId, libp2p_quic::Connection), libp2p_quic::Error>),
    }

    /// Own a real patched listener and stop its background task when the test finishes.
    struct Server {
        /// Bound loopback address.
        address: SocketAddr,
        /// Aggregate counters from the candidate endpoint.
        stats: Arc<IncomingStats>,
        /// Per-poll observations published even on Pending/no-event paths.
        progress: watch::Receiver<Progress>,
        /// Task retaining the listener, handshakes and completed connections.
        task: tokio::task::JoinHandle<Result<(), Error>>,
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Server {
        /// Start the production incoming policy with a one-outcome poll cap.
        async fn start(lifetime: Option<Duration>) -> Result<Self, Error> {
            let key = Keypair::generate_ed25519();
            Self::with_key(lifetime, &key).await
        }

        /// Restart with a stable peer identity while the endpoint generates fresh token keys.
        async fn with_key(lifetime: Option<Duration>, key: &Keypair) -> Result<Self, Error> {
            let settings = settings::QuicConfig::default();
            let mut config = settings.apply_to(Config::new(key));
            let observer = Observer::new(&mut config, settings.retry_unvalidated_incoming, 45, 8);
            let limits = incoming::QuicIncomingLimits::new(45, 8).with_outcomes_per_poll(1);
            assert_eq!(limits.queue_bounds(), (720, 5888, 4_239_360));
            config.max_incoming_outcomes_per_poll = 1;
            config.retry_token_lifetime = lifetime;
            let stats = Arc::clone(&observer.stats);
            let mut transport = libp2p_quic::tokio::Transport::new(config);
            transport
                .listen_on(
                    ListenerId::next(),
                    "/ip4/127.0.0.1/udp/0/quic-v1"
                        .parse()
                        .map_err(|error| Error::Setup(format!("{error}")))?,
                )
                .map_err(|error| Error::Setup(format!("{error}")))?;
            let first = poll_fn(|cx| Pin::new(&mut transport).poll(cx)).await;
            let address = match first {
                TransportEvent::NewAddress { listen_addr, .. } => {
                    let printed = listen_addr.to_string();
                    let port = printed
                        .split("/udp/")
                        .nth(1)
                        .and_then(|part| part.split('/').next())
                        .and_then(|part| part.parse::<u16>().ok())
                        .ok_or_else(|| Error::Setup(format!("unexpected address {printed}")))?;
                    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
                }
                TransportEvent::Incoming { .. }
                | TransportEvent::AddressExpired { .. }
                | TransportEvent::ListenerClosed { .. }
                | TransportEvent::ListenerError { .. } => {
                    Err(Error::Setup("listener did not report its address".into()))?
                }
            };
            let (updates, progress) = watch::channel(Progress::default());
            let observed = Arc::clone(&stats);
            let task = tokio::spawn(async move {
                stream::repeat_with(|| Ok::<_, Error>(()))
                    .try_fold(
                        (transport, FuturesUnordered::<Upgrade>::new(), Vec::new(), 0u64),
                        |(mut transport, mut upgrades, mut connections, mut violations), ()| {
                            let updates = updates.clone();
                            let observed = Arc::clone(&observed);
                            async move {
                                let event = poll_fn(|cx| {
                                    let before = observed.retried()
                                        + observed.refused()
                                        + observed.ignored();
                                    let yields = observed.budget_yields();
                                    let probe = Arc::new(WakeProbe {
                                        parent: cx.waker().clone(),
                                        calls: AtomicU64::new(0),
                                    });
                                    let wake = waker(Arc::clone(&probe));
                                    let mut counted = std::task::Context::from_waker(&wake);
                                    let upgraded = upgrades.poll_next_unpin(&mut counted);
                                    let ready = match upgraded {
                                        Poll::Ready(value) => value.map(Event::Upgrade),
                                        Poll::Pending => None,
                                    };
                                    let next = ready.map_or_else(
                                        || {
                                            Pin::new(&mut transport)
                                                .poll(&mut counted)
                                                .map(Event::Transport)
                                        },
                                        Poll::Ready,
                                    );
                                    let delta = observed.retried()
                                        + observed.refused()
                                        + observed.ignored()
                                        - before;
                                    let capped = delta == 1;
                                    let valid_yield = next.is_pending()
                                        && observed.budget_yields() > yields
                                        && probe.calls.load(Ordering::SeqCst) > 0;
                                    violations += u64::from(delta > 1 || (capped && !valid_yield));
                                    updates.send_replace(Progress {
                                        retried: observed.retried(),
                                        accepted: observed.accepted(),
                                        violations,
                                    });
                                    next
                                })
                                .await;
                                match event {
                                    Event::Transport(TransportEvent::Incoming {
                                        upgrade, ..
                                    }) => {
                                        upgrades.push(upgrade);
                                    }
                                    Event::Upgrade(result) => {
                                        let _retained = result.ok().map(|(_peer, connection)| {
                                            connections.push(connection)
                                        });
                                    }
                                    Event::Transport(TransportEvent::ListenerError {
                                        error,
                                        ..
                                    }) => Err(Error::Setup(format!("listener error: {error}")))?,
                                    Event::Transport(TransportEvent::ListenerClosed { .. }) => {
                                        Err(Error::Setup("listener closed".into()))?
                                    }
                                    Event::Transport(TransportEvent::NewAddress { .. })
                                    | Event::Transport(TransportEvent::AddressExpired { .. }) => {}
                                }
                                Ok((transport, upgrades, connections, violations))
                            }
                        },
                    )
                    .await
                    .map(|_state| ())
            });
            Ok(Self { address, stats, progress, task })
        }

        /// Await a counter notification, without sleeps or latency assertions.
        async fn accepted(&mut self, count: u64) -> Result<(), Error> {
            tokio::time::timeout(WATCHDOG, self.progress.wait_for(|p| p.accepted >= count))
                .await
                .map_err(Error::Timeout)?
                .map_err(|error| Error::Setup(format!("{error}")))?;
            assert_eq!(self.progress.borrow().violations, 0, "every capped poll yields and wakes");
            Ok(())
        }
    }

    /// A fresh client with an explicitly controlled validation-token store.
    fn client(ip: Ipv4Addr, token: Option<Bytes>) -> Result<(quinn::Endpoint, Arc<Tokens>), Error> {
        let tokens = Arc::new(Tokens::new(token));
        let mut endpoint = quinn::Endpoint::client(SocketAddr::from((ip, 0))).map_err(Error::Io)?;
        endpoint.set_default_client_config(client_config(Arc::clone(&tokens))?);
        Ok((endpoint, tokens))
    }

    /// Complete a real handshake, retaining the endpoint for reconnects.
    async fn connect(
        endpoint: &quinn::Endpoint,
        address: SocketAddr,
    ) -> Result<quinn::Connection, Error> {
        let connecting = endpoint.connect(address, "localhost").map_err(Error::Connect)?;
        tokio::time::timeout(WATCHDOG, connecting)
            .await
            .map_err(Error::Timeout)?
            .map_err(Error::Connection)
    }

    /// Obtain a genuine NEW_TOKEN after the first Retry-validated connection.
    async fn validation_token(server: &mut Server) -> Result<Bytes, Error> {
        let (endpoint, tokens) = client(Ipv4Addr::LOCALHOST, None)?;
        let connection = connect(&endpoint, server.address).await?;
        let token = tokens.received().await?;
        server.accepted(1).await?;
        assert!(server.stats.retried() > 0);
        connection.close(0u32.into(), b"fixture reconnect");
        Ok(token)
    }

    /// A valid NEW_TOKEN skips Retry; Bloom replay rejection falls back to a fresh Retry.
    #[tokio::test]
    async fn valid_and_replayed_validation_tokens() -> Result<(), Error> {
        let mut server = Server::start(None).await?;
        let token = validation_token(&mut server).await?;
        let before = server.stats.retried();
        let (first, _tokens) = client(Ipv4Addr::LOCALHOST, Some(token.clone()))?;
        let connection = connect(&first, server.address).await?;
        server.accepted(2).await?;
        assert_eq!(server.stats.retried(), before, "valid validation token skips Retry");
        let (replay, _tokens) = client(Ipv4Addr::LOCALHOST, Some(token))?;
        let replayed = connect(&replay, server.address).await?;
        server.accepted(3).await?;
        assert!(
            server.stats.retried() > before,
            "resolved Bloom log rejects replay and rechallenges"
        );
        connection.close(0u32.into(), b"done");
        replayed.close(0u32.into(), b"done");
        Ok(())
    }

    /// Malformed or differently IP-bound validation tokens cannot skip address validation.
    #[tokio::test]
    async fn malformed_and_wrong_ip_validation_tokens() -> Result<(), Error> {
        let mut server = Server::start(None).await?;
        let token = validation_token(&mut server).await?;
        let before = server.stats.retried();
        let (bad, _tokens) = client(Ipv4Addr::LOCALHOST, Some(Bytes::from_static(b"not a token")))?;
        let malformed = connect(&bad, server.address).await?;
        server.accepted(2).await?;
        assert!(server.stats.retried() > before, "opaque malformed token is treated as absent");
        let before = server.stats.retried();
        let (bound, _tokens) = client(Ipv4Addr::new(127, 0, 0, 2), Some(token))?;
        let wrong_ip = connect(&bound, server.address).await?;
        server.accepted(3).await?;
        assert!(server.stats.retried() > before, "NEW_TOKEN binds the IP, not the UDP port");
        malformed.close(0u32.into(), b"done");
        wrong_ip.close(0u32.into(), b"done");
        Ok(())
    }

    /// A new endpoint/token key rechallenges an old token and still admits the honest peer.
    #[tokio::test]
    async fn restarted_listener_rechallenges_old_token() -> Result<(), Error> {
        let key = Keypair::generate_ed25519();
        let mut original = Server::with_key(None, &key).await?;
        let token = validation_token(&mut original).await?;
        drop(original);
        let mut restarted = Server::with_key(None, &key).await?;
        let (endpoint, _tokens) = client(Ipv4Addr::LOCALHOST, Some(token))?;
        let connection = connect(&endpoint, restarted.address).await?;
        restarted.accepted(1).await?;
        assert!(restarted.stats.retried() > 0, "key changes invalidate opaque old tokens");
        connection.close(0u32.into(), b"done");
        Ok(())
    }

    /// Expired Retry tokens fail before the listener accepts any handshake work.
    #[tokio::test]
    async fn expired_retry_has_no_accepted_event() -> Result<(), Error> {
        let server = Server::start(Some(Duration::ZERO)).await?;
        let (endpoint, _tokens) = client(Ipv4Addr::LOCALHOST, None)?;
        assert!(connect(&endpoint, server.address).await.is_err());
        assert!(server.stats.retried() > 0);
        assert_eq!(server.stats.accepted(), 0);
        Ok(())
    }

    /// Request a genuine Retry and retain its source port for binding and replay tests.
    async fn retry_token(server: &Server) -> Result<(quinn::Endpoint, Bytes), Error> {
        let socket =
            tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.map_err(Error::Io)?;
        let packet = initial_packet().await?;
        socket.send_to(&packet, server.address).await.map_err(Error::Io)?;
        let mut reply = vec![0; 65535];
        let (size, remote) = tokio::time::timeout(WATCHDOG, socket.recv_from(&mut reply))
            .await
            .map_err(Error::Timeout)?
            .map_err(Error::Io)?;
        assert_eq!(remote, server.address);
        reply.truncate(size);
        assert_eq!(reply.first().copied().map(|byte| (byte >> 4) & 3), Some(3), "QUIC v1 Retry");
        let destination = reply
            .get(5)
            .copied()
            .map(usize::from)
            .ok_or_else(|| Error::Setup("truncated Retry destination CID".into()))?;
        let source_length = 6usize.saturating_add(destination);
        let source = reply
            .get(source_length)
            .copied()
            .map(usize::from)
            .ok_or_else(|| Error::Setup("truncated Retry source CID".into()))?;
        let start = source_length.saturating_add(1).saturating_add(source);
        let end = reply
            .len()
            .checked_sub(16)
            .ok_or_else(|| Error::Setup("Retry without integrity tag".into()))?;
        let token = Bytes::copy_from_slice(
            reply
                .get(start..end)
                .filter(|token| !token.is_empty())
                .ok_or_else(|| Error::Setup("Retry without token".into()))?,
        );
        quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            socket.into_std().map_err(Error::Io)?,
            Arc::new(quinn::TokioRuntime),
        )
        .map(|endpoint| (endpoint, token))
        .map_err(Error::Io)
    }

    /// Retry tokens bind the UDP address and cannot validate a different source port.
    #[tokio::test]
    async fn wrong_port_retry_has_no_accepted_event() -> Result<(), Error> {
        let server = Server::start(None).await?;
        let (_original, token) = retry_token(&server).await?;
        let (different, _tokens) = client(Ipv4Addr::LOCALHOST, Some(token))?;
        assert!(connect(&different, server.address).await.is_err());
        assert_eq!(server.stats.accepted(), 0, "decoded wrong-binding Retry fails before accept");
        Ok(())
    }

    /// Undecryptable tokens fall back to Retry rather than a made-up single-use rule.
    #[tokio::test]
    async fn malformed_retry_is_rechallenged() -> Result<(), Error> {
        let mut server = Server::start(None).await?;
        let (_original, token) = retry_token(&server).await?;
        let mut corrupted = token.to_vec();
        let _changed = corrupted.last_mut().map(|byte| *byte ^= 1);
        let before = server.stats.retried();
        let (endpoint, _tokens) = client(Ipv4Addr::LOCALHOST, Some(Bytes::from(corrupted)))?;
        let connection = connect(&endpoint, server.address).await?;
        server.accepted(1).await?;
        assert!(server.stats.retried() > before);
        connection.close(0u32.into(), b"done");
        Ok(())
    }

    /// Retry tokens prove reachability and are not a single-use connection-identity barrier.
    ///
    /// Quinn restores the original destination CID encoded in the Retry token. A new client
    /// rejects that mismatched transport parameter after server acceptance. Address validation
    /// and successful authenticated connection establishment are therefore separate results.
    #[tokio::test]
    async fn replayed_retry_can_reach_accept_but_not_change_connection_identity(
    ) -> Result<(), Error> {
        let mut server = Server::start(None).await?;
        let (mut endpoint, token) = retry_token(&server).await?;
        endpoint
            .set_default_client_config(client_config(Arc::new(Tokens::new(Some(token.clone()))))?);
        assert!(connect(&endpoint, server.address).await.is_err());
        server.accepted(1).await?;
        endpoint.set_default_client_config(client_config(Arc::new(Tokens::new(Some(token))))?);
        assert!(connect(&endpoint, server.address).await.is_err());
        server.accepted(2).await?;
        Ok(())
    }

    /// Construct a real Quinn server with the same libp2p TLS authentication layer.
    fn quinn_server() -> Result<quinn::Endpoint, Error> {
        let tls = libp2p_tls::make_server_config(&Keypair::generate_ed25519())
            .map_err(|error| Error::Setup(format!("{error}")))?;
        let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
            .map_err(|error| Error::Setup(format!("{error}")))?;
        quinn::Endpoint::server(
            quinn::ServerConfig::with_crypto(Arc::new(crypto)),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        )
        .map_err(Error::Io)
    }

    /// Await a genuine Incoming value with a bounded fixture watchdog.
    async fn incoming(endpoint: &quinn::Endpoint) -> Result<quinn::Incoming, Error> {
        tokio::time::timeout(WATCHDOG, endpoint.accept())
            .await
            .map_err(Error::Timeout)?
            .ok_or_else(|| Error::Setup("endpoint stopped before Incoming".into()))
    }

    /// Accept and Refuse are exercised on real Incoming values, not mocked outcomes.
    #[tokio::test]
    async fn real_accept_and_refuse_outcomes() -> Result<(), Error> {
        let server = quinn_server()?;
        let (endpoint, _tokens) = client(Ipv4Addr::LOCALHOST, None)?;
        let address = server.local_addr().map_err(Error::Io)?;
        let first = endpoint.connect(address, "localhost").map_err(Error::Connect)?;
        let attempt = incoming(&server).await?;
        assert!(
            !attempt.remote_address_validated() && attempt.may_retry(),
            "resolved Quinn invariant"
        );
        let accepting = attempt.accept().map_err(Error::Connection)?;
        let (accepted, connected) =
            tokio::try_join!(async { accepting.await.map_err(Error::Connection) }, async {
                first.await.map_err(Error::Connection)
            })?;
        accepted.close(0u32.into(), b"done");
        connected.close(0u32.into(), b"done");
        let refused = endpoint.connect(address, "localhost").map_err(Error::Connect)?;
        incoming(&server).await?.refuse();
        assert!(tokio::time::timeout(WATCHDOG, refused).await.map_err(Error::Timeout)?.is_err());
        Ok(())
    }

    /// Ignore produces no accepted connection; failed Retry returns the genuine Incoming.
    ///
    /// The shipped listener cannot reach Refuse/Ignore from an unvalidated Incoming under
    /// Quinn 0.11.18: unvalidated implies may_retry. These API cases protect the dependency
    /// contract without claiming that unreachable listener counters fire.
    #[tokio::test]
    async fn real_retry_and_ignore_outcomes_without_accept() -> Result<(), Error> {
        let server = quinn_server()?;
        let mut config = client_config(Arc::new(Tokens::default()))?;
        let mut transport = quinn::TransportConfig::default();
        transport.max_idle_timeout(Some(
            Duration::from_millis(200)
                .try_into()
                .map_err(|error| Error::Setup(format!("{error}")))?,
        ));
        config.transport_config(Arc::new(transport));
        let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
            .map_err(Error::Io)?;
        endpoint.set_default_client_config(config);
        let connecting = endpoint
            .connect(server.local_addr().map_err(Error::Io)?, "localhost")
            .map_err(Error::Connect)?;
        let first = incoming(&server).await?;
        assert!(!first.remote_address_validated() && first.may_retry());
        first.retry().map_err(|error| Error::Setup(format!("{error}")))?;
        let validated = incoming(&server).await?;
        assert!(validated.remote_address_validated());
        assert!(!validated.may_retry());
        validated
            .retry()
            .err()
            .ok_or_else(|| Error::Setup("second Retry unexpectedly succeeded".into()))?
            .into_incoming()
            .ignore();
        let failed = tokio::time::timeout(WATCHDOG, connecting).await.map_err(Error::Timeout)?;
        assert!(matches!(failed, Err(quinn::ConnectionError::TimedOut)));
        Ok(())
    }

    /// Concurrent honest reconnects use the real listener while every capped poll wakes.
    #[tokio::test(flavor = "current_thread")]
    async fn mass_reconnects_preserve_poll_progress() -> Result<(), Error> {
        let mut server = Server::start(None).await?;
        let clients =
            (0..45).map(|_| client(Ipv4Addr::LOCALHOST, None)).collect::<Result<Vec<_>, _>>()?;
        let first =
            join_all(clients.iter().map(|(endpoint, _tokens)| connect(endpoint, server.address)))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
        server.accepted(45).await?;
        first.iter().for_each(|connection| connection.close(0u32.into(), b"honest reconnect"));
        let second =
            join_all(clients.iter().map(|(endpoint, _tokens)| connect(endpoint, server.address)))
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
        server.accepted(90).await?;
        assert!(server.stats.retried() >= 90, "every new unvalidated dial is challenged");
        assert!(server.stats.budget_yields() >= 90);
        assert!(server.progress.borrow().retried >= 90);
        second.iter().for_each(|connection| connection.close(0u32.into(), b"done"));
        Ok(())
    }
}
