use libp2p::{core::UpgradeInfo, swarm::StreamProtocol, InboundUpgrade, OutboundUpgrade, Stream};
use std::{
    convert::Infallible,
    future::{ready, Ready},
};

use crate::stream::behavior::TN_STREAM_PROTOCOL;

/// Protocol upgrade for streaming data.
///
/// Both inbound and outbound upgrades simply return the raw stream.
/// Application-layer correlation (e.g. writing a request digest) is
/// handled by the caller after the stream is established.
#[derive(Debug, Clone)]
pub(crate) struct TNStreamProtocol;

impl UpgradeInfo for TNStreamProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(TN_STREAM_PROTOCOL)
    }
}

impl InboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // logic in application layer
    fn upgrade_inbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        ready(Ok(stream))
    }
}

impl OutboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = Infallible;
    type Future = Ready<Result<Self::Output, Self::Error>>;

    // logic in application layer
    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        ready(Ok(stream))
    }
}

/// Errors that can occur during streaming operations.
#[derive(Debug)]
pub enum StreamError {
    /// The protocol upgrade failed during stream negotiation.
    UpgradeFailed,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UpgradeFailed => write!(f, "Protocol upgrade failed"),
        }
    }
}

impl std::error::Error for StreamError {}
