//! Process fixture for QUIC handshakes, streams and reconnects across transport releases.

use futures::{
    future::poll_fn, stream, AsyncReadExt, AsyncWriteExt, Future, StreamExt, TryStreamExt,
};
use libp2p::{
    core::{
        muxing::StreamMuxerExt,
        transport::{DialOpts, ListenerId, PortUse, TransportEvent},
        Endpoint, Transport,
    },
    identity::Keypair,
    Multiaddr,
};
use libp2p_quic as quic;
use serde_json::{json, Value};
use std::{
    fmt,
    fs::File,
    io::{Read, Write},
    pin::Pin,
    time::Instant,
};

// Compile the production defaults and mapping against each independently locked release.
#[path = "../../../crates/config/src/network/quic.rs"]
mod settings;

/// Fresh connections made with one dialer identity and transport.
const CONNECTIONS: u32 = 3;
/// Bidirectional streams exercised on each connection.
const STREAMS: u32 = 3;
/// Bytes checked in both directions on every stream.
const PAYLOAD_BYTES: usize = 32 * 1024;

/// Failures at the process fixture boundary.
#[derive(Debug)]
enum Error {
    /// A socket, file or stream operation failed.
    Io(std::io::Error),
    /// The resolved QUIC implementation rejected or closed a connection.
    Quic(quic::Error),
    /// Configuration or event serialization failed.
    Json(serde_json::Error),
    /// Invalid arguments, addresses or transport setup.
    Setup(String),
    /// The echo or its acknowledgement did not match the sent bytes.
    Payload,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => error.fmt(f),
            Self::Quic(error) => error.fmt(f),
            Self::Json(error) => error.fmt(f),
            Self::Setup(error) => error.fmt(f),
            Self::Payload => f.write_str("stream payload or acknowledgement mismatch"),
        }
    }
}

impl std::error::Error for Error {}

/// Flush one machine-readable event so the controller never waits on buffered output.
fn report(event: Value) -> Result<(), Error> {
    let mut output = std::io::stdout().lock();
    serde_json::to_writer(&mut output, &event).map_err(Error::Json)?;
    writeln!(output).map_err(Error::Io)?;
    output.flush().map_err(Error::Io)
}

/// A deterministic payload shared by the traffic producer and checker.
fn payload() -> Vec<u8> {
    b"telcoin-quic-fixture".iter().copied().cycle().take(PAYLOAD_BYTES).collect()
}

/// Check bytes at the stream boundary.
fn check_payload(actual: &[u8], expected: &[u8]) -> Result<(), Error> {
    if actual == expected {
        Ok(())
    } else {
        Err(Error::Payload)
    }
}

/// Echo each stream, then retain the connection until the dialer closes it.
async fn serve(connection: quic::Connection) -> Result<(), Error> {
    let mut connection = stream::iter(0..STREAMS)
        .map(Ok::<_, Error>)
        .try_fold(connection, |mut connection, sample| async move {
            let started = Instant::now();
            let mut substream = poll_fn(|cx| {
                let _event = connection.poll_unpin(cx)?;
                connection.poll_inbound_unpin(cx)
            })
            .await
            .map_err(Error::Quic)?;
            let mut received = vec![0; PAYLOAD_BYTES];
            substream.read_exact(&mut received).await.map_err(Error::Io)?;
            check_payload(&received, &payload())?;
            substream.write_all(&received).await.map_err(Error::Io)?;
            substream.flush().await.map_err(Error::Io)?;
            let mut ack = [0];
            substream.read_exact(&mut ack).await.map_err(Error::Io)?;
            check_payload(&ack, b"!")?;
            substream.write_all(b".").await.map_err(Error::Io)?;
            substream.close().await.map_err(Error::Io)?;
            report(json!({"event": "echo", "sample": sample, "bytes": received.len(),
                "elapsed_us": started.elapsed().as_micros()}))?;
            Ok(connection)
        })
        .await?;
    // The background muxer poll only reports address changes. Inbound polling observes closure.
    poll_fn(|cx| connection.poll_inbound_unpin(cx)).await.map_or_else(
        |error| report(json!({"event": "closed", "reason": format!("{error:?}")})),
        |_stream| Err(Error::Setup("unexpected extra stream".into())),
    )
}

/// Poll the listener between connections and report failed handshakes as evidence.
async fn listen(mut transport: quic::tokio::Transport, key: Keypair) -> Result<(), Error> {
    transport
        .listen_on(
            ListenerId::next(),
            "/ip4/127.0.0.1/udp/0/quic-v1"
                .parse()
                .map_err(|error| Error::Setup(format!("{error}")))?,
        )
        .map_err(|error| Error::Setup(format!("{error}")))?;
    let peer = key.public().to_peer_id();
    stream::repeat_with(|| Ok::<_, Error>(()))
        .try_fold(transport, |mut transport, ()| async move {
            match poll_fn(|cx| Pin::new(&mut transport).poll(cx)).await {
                TransportEvent::NewAddress { listen_addr, .. } => {
                    let address = listen_addr.with(libp2p::multiaddr::Protocol::P2p(peer));
                    report(json!({"event": "listening", "address": address.to_string()}))
                }
                TransportEvent::Incoming { upgrade, .. } => {
                    let started = Instant::now();
                    report(json!({"event": "incoming"}))?;
                    let result = async {
                        let (peer, connection) = upgrade.await.map_err(Error::Quic)?;
                        report(json!({"event": "accepted", "peer": peer.to_string(),
                            "elapsed_us": started.elapsed().as_micros()}))?;
                        serve(connection).await
                    }
                    .await;
                    result.or_else(|error| {
                        report(json!({"event": "failed", "error": format!("{error:?}"),
                            "elapsed_ms": started.elapsed().as_millis()}))
                    })
                }
                TransportEvent::AddressExpired { listen_addr, .. } => {
                    Err(Error::Setup(format!("listen address expired: {listen_addr}")))
                }
                TransportEvent::ListenerClosed { reason, .. } => {
                    Err(Error::Setup(format!("listener closed: {reason:?}")))
                }
                TransportEvent::ListenerError { error, .. } => Err(Error::Quic(error)),
            }?;
            Ok(transport)
        })
        .await
        .map(|_transport| ())
}

/// Establish fresh connections to one authenticated peer and check every echoed byte.
async fn dial(transport: quic::tokio::Transport, address: Multiaddr) -> Result<(), Error> {
    let _transport = stream::iter(0..CONNECTIONS)
        .map(Ok::<_, Error>)
        .try_fold(transport, |mut transport, round| {
            let address = address.clone();
            async move {
                let started = Instant::now();
                let mut connecting = std::pin::pin!(transport
                    .dial(address, DialOpts { role: Endpoint::Dialer, port_use: PortUse::Reuse })
                    .map_err(|error| Error::Setup(format!("{error}")))?);
                let result = poll_fn(|cx| {
                    let _event = Pin::new(&mut transport).poll(cx);
                    connecting.as_mut().poll(cx)
                })
                .await;
                let (peer, connection) = result.map_err(|error| {
                    let _reported =
                        report(json!({"event": "failed", "error": format!("{error:?}"),
                        "elapsed_ms": started.elapsed().as_millis()}));
                    Error::Quic(error)
                })?;
                report(json!({"event": "connected", "round": round, "peer": peer.to_string(),
                    "elapsed_us": started.elapsed().as_micros()}))?;
                let mut connection = stream::iter(0..STREAMS)
                    .map(Ok::<_, Error>)
                    .try_fold(connection, |mut connection, sample| async move {
                        let started = Instant::now();
                        let mut substream = poll_fn(|cx| {
                            let _event = connection.poll_unpin(cx)?;
                            connection.poll_outbound_unpin(cx)
                        })
                        .await
                        .map_err(Error::Quic)?;
                        let sent = payload();
                        substream.write_all(&sent).await.map_err(Error::Io)?;
                        substream.flush().await.map_err(Error::Io)?;
                        let mut received = vec![0; sent.len()];
                        substream.read_exact(&mut received).await.map_err(Error::Io)?;
                        check_payload(&received, &sent)?;
                        substream.write_all(b"!").await.map_err(Error::Io)?;
                        substream.close().await.map_err(Error::Io)?;
                        let mut ack = [0];
                        substream.read_exact(&mut ack).await.map_err(Error::Io)?;
                        check_payload(&ack, b".")?;
                        report(json!({"event": "verified", "round": round, "sample": sample,
                            "bytes": sent.len(), "elapsed_us": started.elapsed().as_micros()}))?;
                        Ok(connection)
                    })
                    .await?;
                poll_fn(|cx| connection.poll_close_unpin(cx)).await.map_err(Error::Quic)?;
                Ok(transport)
            }
        })
        .await?;
    report(json!({"event": "complete", "connections": CONNECTIONS, "streams": STREAMS}))?;
    // Keep the transport and runtime alive to flush the last connection's close packet. The
    // controller acknowledges only after the listener observes that close, without a
    // timing-based sleep.
    tokio::task::spawn_blocking(|| {
        let mut ack = [0];
        std::io::stdin().read_exact(&mut ack).map_err(Error::Io)?;
        check_payload(&ack, b"!")
    })
    .await
    .map_err(|error| Error::Setup(format!("controller acknowledgement task: {error}")))?
}

/// Load production settings, record the applied configuration and select the process role.
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
    let mut args = std::env::args().skip(1);
    let role = args.next().ok_or_else(|| Error::Setup("expected listen or dial".into()))?;
    let config_path =
        args.next().ok_or_else(|| Error::Setup("expected settings JSON path".into()))?;
    let settings: settings::QuicConfig =
        serde_json::from_reader(File::open(config_path).map_err(Error::Io)?)
            .map_err(Error::Json)?;
    let key = Keypair::generate_ed25519();
    let config = settings.apply_to(quic::Config::new(&key));
    report(json!({"event": "configuration", "release": env!("CARGO_PKG_VERSION"),
        "peer": key.public().to_peer_id().to_string(), "settings": settings,
        "applied": {"handshake_timeout": config.handshake_timeout,
            "max_idle_timeout": config.max_idle_timeout,
            "keep_alive_interval": config.keep_alive_interval,
            "max_concurrent_stream_limit": config.max_concurrent_stream_limit,
            "max_stream_data": config.max_stream_data,
            "max_connection_data": config.max_connection_data}}))?;
    let transport = quic::tokio::Transport::new(config);
    match role.as_str() {
        "listen" => listen(transport, key).await,
        "dial" => {
            let address =
                args.next().ok_or_else(|| Error::Setup("expected dial address".into()))?;
            dial(transport, address.parse().map_err(|error| Error::Setup(format!("{error}")))?)
                .await
        }
        role => Err(Error::Setup(format!("unknown role: {role}"))),
    }
}
