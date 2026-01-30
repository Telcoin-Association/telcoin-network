use futures::{future::BoxFuture, AsyncReadExt, FutureExt};
use libp2p::core::UpgradeInfo;
use libp2p::swarm::StreamProtocol;
use libp2p::{InboundUpgrade, OutboundUpgrade, Stream};
use serde::{Deserialize, Serialize};
use std::io;

use crate::stream::behavior::TN_STREAM_PROTOCOL;

/// Protocol upgrade for stream-based sync.
///
/// This protocol handles the establishment of streams between peers for bulk data transfer.
/// The inbound side reads a [`StreamHeader`] to identify the resource being synced,
/// while the outbound side returns the raw stream for the caller to write data.
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
    type Output = (Stream, StreamHeader);
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut stream: Stream, _: Self::Info) -> Self::Future {
        async move {
            // Read the stream header first (4-byte length prefix + BCS-encoded header)
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut header_buf = vec![0u8; len];
            stream.read_exact(&mut header_buf).await?;

            let header: StreamHeader = bcs::from_bytes(&header_buf).map_err(io::Error::other)?;

            Ok((stream, header))
        }
        .boxed()
    }
}

impl OutboundUpgrade<Stream> for TNStreamProtocol {
    type Output = Stream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        // Just return the stream; caller will write header
        async move { Ok(stream) }.boxed()
    }
}

/// Header sent at the start of a sync stream to identify the resource being synced.
///
/// This header is written by the stream initiator (after successful req/res negotiation)
/// and read by the acceptor to correlate the stream with a previously negotiated sync request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHeader {
    /// Generic resource identifier for the data being synced.
    ///
    /// Interpretation depends on the sync type:
    /// - For epoch sync: the epoch number
    /// - For batch sync: the batch sequence or round number
    pub resource_id: u64,
    /// ID matching the request-response negotiation.
    ///
    /// This allows the receiver to correlate this stream with the
    /// previously received SyncStateRequest and its response.
    pub request_id: u64,
    /// Expected hash of the complete data to be transferred.
    ///
    /// Used for integrity verification after the transfer completes.
    pub expected_hash: [u8; 32],
}

/// Errors that can occur during stream sync operations.
#[derive(Debug)]
pub enum StreamSyncError {
    /// An I/O error occurred during stream operations.
    Io(io::Error),
    /// The protocol upgrade failed during stream negotiation.
    UpgradeFailed,
    /// The stream header was invalid or could not be parsed.
    InvalidHeader,
    /// The stream was closed unexpectedly by the peer.
    StreamClosed,
    /// A timeout occurred waiting for the stream to be established.
    Timeout,
}

impl std::fmt::Display for StreamSyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::UpgradeFailed => write!(f, "Protocol upgrade failed"),
            Self::InvalidHeader => write!(f, "Invalid stream header"),
            Self::StreamClosed => write!(f, "Stream closed unexpectedly"),
            Self::Timeout => write!(f, "Stream establishment timeout"),
        }
    }
}

impl std::error::Error for StreamSyncError {}
