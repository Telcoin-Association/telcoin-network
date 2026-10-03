//! Independent QUIC peer used to qualify the carried transport against registry peers.

use futures::{
    StreamExt, future,
    io::{AsyncReadExt, AsyncWriteExt},
    stream,
};
use libp2p_core::{
    Endpoint, Multiaddr, Transport,
    muxing::StreamMuxerExt,
    transport::{DialOpts, ListenerId, PortUse},
};
use libp2p_identity::{Keypair, PeerId};
use std::{fmt, io, pin::Pin, time::Duration};

/// Failures at the command, transport, or application boundary.
#[derive(Debug)]
enum Error {
    /// Invalid arguments or an unexpected protocol result.
    Invalid(String),
    /// Application stream I/O failed.
    Io(io::Error),
    /// The QUIC transport failed.
    Transport(String),
}

impl fmt::Display for Error {
    /// Render the original failure at the process boundary.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) | Self::Transport(message) => f.write_str(message),
            Self::Io(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    /// Preserve stream I/O errors as their concrete category.
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Convert an external library error without hiding a failed operation.
fn transport_error(error: impl fmt::Display) -> Error {
    Error::Transport(error.to_string())
}

/// Construct the normal QUIC provider, with the node's default listener limits for the patch.
fn transport(key: &Keypair) -> libp2p_quic::tokio::Transport {
    let config = libp2p_quic::Config::new(key);
    #[cfg(feature = "carried")]
    let config = {
        let mut config = config;
        config.handshake_timeout = Duration::from_secs(65);
        config.max_idle_timeout = 30_000;
        config.keep_alive_interval = Duration::from_secs(5);
        config.max_concurrent_stream_limit = 10_000;
        config.max_stream_data = 50 * 1024 * 1024;
        config.max_connection_data = 100 * 1024 * 1024;
        config.retry_unvalidated_incoming = true;
        config.max_incoming = Some(720);
        config.incoming_buffer_size = Some(5888);
        config.incoming_buffer_size_total = Some(4_239_360);
        config.max_incoming_outcomes_per_poll = 128;
        config
    };
    libp2p_quic::tokio::Transport::new(config)
}

/// Serve two authenticated connections through one listener identity.
async fn listen(address: Multiaddr) -> Result<(), Error> {
    let key = Keypair::generate_ed25519();
    let mut transport = transport(&key);
    transport.listen_on(ListenerId::next(), address).map_err(transport_error)?;
    let address = stream::poll_fn(|cx| Pin::new(&mut transport).poll(cx).map(Some))
        .filter_map(|event| future::ready(event.into_new_address()))
        .next()
        .await
        .ok_or_else(|| Error::Invalid("listener ended".into()))?;
    println!("READY {address}/p2p/{}", key.public().to_peer_id());
    serve(&mut transport).await?;
    serve(&mut transport).await
}

/// Accept an authenticated connection and confirm delivery before closing it.
async fn serve(transport: &mut libp2p_quic::tokio::Transport) -> Result<(), Error> {
    let upgrade = stream::poll_fn(|cx| Pin::new(&mut *transport).poll(cx).map(Some))
        .filter_map(|event| future::ready(event.into_incoming().map(|(upgrade, _)| upgrade)))
        .next()
        .await
        .ok_or_else(|| Error::Invalid("listener ended".into()))?;
    let (peer, mut connection) = upgrade.await.map_err(transport_error)?;
    let mut inbound =
        future::poll_fn(|cx| connection.poll_inbound_unpin(cx)).await.map_err(transport_error)?;
    let mut frame = [0_u8; 32];
    inbound.read_exact(&mut frame).await?;
    inbound.write_all(&frame).await?;
    inbound.flush().await?;
    let mut acknowledgement = [0_u8; 4];
    inbound.read_exact(&mut acknowledgement).await?;
    if acknowledgement != *b"DONE" {
        Err(Error::Invalid("missing delivery acknowledgement".into()))?;
    }
    inbound.close().await?;
    println!("ECHO {peer}");
    Ok(())
}

/// Dial independently, compare the authenticated identity, and validate an echoed frame.
async fn dial(address: Multiaddr, expected: PeerId) -> Result<(), Error> {
    let key = Keypair::generate_ed25519();
    let mut transport = transport(&key);
    let dial = transport
        .dial(address, DialOpts { role: Endpoint::Dialer, port_use: PortUse::Reuse })
        .map_err(transport_error)?;
    let (peer, mut connection) =
        match future::select(dial, future::poll_fn(|cx| Pin::new(&mut transport).poll(cx))).await {
            future::Either::Left((result, _)) => result.map_err(transport_error)?,
            future::Either::Right((event, _)) => {
                Err(Error::Invalid(format!("unexpected dial event: {event:?}")))?
            }
        };
    if peer != expected {
        Err(Error::Invalid("authenticated peer identity differs from the expected peer".into()))?;
    }
    let mut outbound =
        future::poll_fn(|cx| connection.poll_outbound_unpin(cx)).await.map_err(transport_error)?;
    let frame = [73_u8; 32];
    outbound.write_all(&frame).await?;
    outbound.flush().await?;
    let mut response = [0_u8; 32];
    outbound.read_exact(&mut response).await?;
    if response != frame {
        Err(Error::Invalid("application echo differs".into()))?;
    }
    outbound.write_all(b"DONE").await?;
    outbound.close().await?;
    println!("VERIFIED {peer}");
    // Keep the endpoint's Tokio workers alive until the listener's final FIN is acknowledged.
    let mut release = String::new();
    io::stdin().read_line(&mut release)?;
    if release != "EXIT\n" {
        Err(Error::Invalid("missing listener completion signal".into()))?;
    }
    Ok(())
}

/// Parse the explicit peer mode and run with a deadline, without convergence sleeps.
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Error> {
    let mut arguments = std::env::args().skip(1);
    let mode = arguments.next().ok_or_else(|| Error::Invalid("missing listen/dial mode".into()))?;
    let address: Multiaddr = arguments
        .next()
        .ok_or_else(|| Error::Invalid("missing address".into()))?
        .parse()
        .map_err(transport_error)?;
    let operation = async {
        match () {
            () if mode == "crypto" => {
                let provider = rustls::crypto::aws_lc_rs::default_provider();
                println!(
                    "GROUPS {}",
                    provider
                        .kx_groups
                        .iter()
                        .map(|group| format!("{:?}", group.name()))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                println!(
                    "CIPHERS {}",
                    provider
                        .cipher_suites
                        .iter()
                        .map(|suite| format!("{:?}", suite.suite()))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                Ok(())
            }
            () if mode == "listen" => listen(address).await,
            () if mode == "dial" => {
                let expected = arguments
                    .next()
                    .ok_or_else(|| Error::Invalid("missing expected peer".into()))?
                    .parse()
                    .map_err(transport_error)?;
                dial(address, expected).await
            }
            () => Err(Error::Invalid("mode must be listen, dial, or crypto".into())),
        }
    };
    tokio::time::timeout(Duration::from_secs(90), operation).await.map_err(transport_error)?
}
