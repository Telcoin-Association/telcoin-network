use futures::{future::BoxFuture, AsyncReadExt, AsyncWriteExt, FutureExt};
use libp2p::core::UpgradeInfo;
use libp2p::swarm::StreamProtocol;
use libp2p::{InboundUpgrade, OutboundUpgrade, Stream};
use serde::{Deserialize, Serialize};
use std::io;

use crate::stream::behavior::EPOCH_SYNC_PROTOCOL;

#[derive(Debug, Clone)]
pub struct EpochSyncProtocol;

impl UpgradeInfo for EpochSyncProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(EPOCH_SYNC_PROTOCOL)
    }
}

impl InboundUpgrade<Stream> for EpochSyncProtocol {
    type Output = (Stream, StreamHeader);
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut stream: Stream, _: Self::Info) -> Self::Future {
        async move {
            // Read the stream header first
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await?;
            let len = u32::from_le_bytes(len_buf) as usize;

            let mut header_buf = vec![0u8; len];
            stream.read_exact(&mut header_buf).await?;

            let header: StreamHeader =
                bcs::from_bytes(&header_buf).map_err(|e| io::Error::other(e))?;

            Ok((stream, header))
        }
        .boxed()
    }
}

impl OutboundUpgrade<Stream> for EpochSyncProtocol {
    type Output = Stream;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: Stream, _: Self::Info) -> Self::Future {
        // Just return the stream; caller will write header
        async move { Ok(stream) }.boxed()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHeader {
    pub epoch: u64,
    pub request_id: u64,
    pub expected_hash: [u8; 32],
}

#[derive(Debug)]
pub enum EpochSyncError {
    Io(io::Error),
    UpgradeFailed,
    InvalidHeader,
}
