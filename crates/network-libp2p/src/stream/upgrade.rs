use futures::{future::BoxFuture, FutureExt};
use libp2p::{core::UpgradeInfo, swarm::StreamProtocol, InboundUpgrade, OutboundUpgrade, Stream};
use std::io;

use crate::stream::behavior::TN_STREAM_PROTOCOL;

/// Protocol upgrade for stream-based sync.
///
/// Both inbound and outbound upgrades simply return the raw stream.
/// Application-layer correlation (e.g. writing a request digest) is
/// handled by the caller after the stream is established.
#[derive(Debug, Clone)]
pub struct TNStreamProtocol;

impl UpgradeInfo for TNStreamProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(TN_STREAM_PROTOCOL)
    }
}

impl InboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        async move { Ok(stream) }.boxed()
    }
}

impl OutboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        async move { Ok(stream) }.boxed()
    }
}

/// Errors that can occur during stream sync operations.
#[derive(Debug)]
pub enum StreamError {
    /// An I/O error occurred during stream operations.
    Io(io::Error),
    /// The protocol upgrade failed during stream negotiation.
    UpgradeFailed,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::UpgradeFailed => write!(f, "Protocol upgrade failed"),
        }
    }
}

impl std::error::Error for StreamError {}
