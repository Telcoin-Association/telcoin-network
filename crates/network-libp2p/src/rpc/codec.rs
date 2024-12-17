//! Encoding/decoding for RPC.

use std::io::{self, Write as _};

use super::procedures::{InboundRequest, RPCCodedResponse, RPCResponse};
use ssz::Encode;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};
use unsigned_varint::codec::Uvi;

type CodecResult<T> = Result<T, TNCodecError>;

#[derive(Debug, Error)]
pub enum TNCodecError {
    /// From std::io::Error
    #[error("IO error: {0}")]
    IoError(String),
    /// Internal error encoding/decoding message.
    #[error("{0}")]
    InternalError(&'static str),
}

impl From<io::Error> for TNCodecError {
    fn from(err: io::Error) -> Self {
        TNCodecError::IoError(err.to_string())
    }
}
/// The type that encodes/decodes RPC messages.
pub struct TNCodec {
    /// Encoder for message length prefix (unsigned protobuf varint).
    prefix: Uvi<usize>,
    /// The maximum size (bytes) in a chunked request/response.
    max_chunk_size: usize,
}

impl TNCodec {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        let prefix = Uvi::default();
        Self { prefix, max_chunk_size }
    }
}

impl Encoder<RPCCodedResponse> for TNCodec {
    type Error = TNCodecError;

    fn encode(
        &mut self,
        item: RPCCodedResponse,
        dst: &mut libp2p::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let bytes: Vec<u8> = match item {
            RPCCodedResponse::Success(data) => data.as_ssz_bytes(),
            RPCCodedResponse::Error(rpcerror_code) => Vec::new(),
        };
        // encoded bytes must be within `max_chunk_size`
        if bytes.len() > self.max_chunk_size {
            return Err(TNCodecError::InternalError("attempting to encode data > max_packet_size"));
        }

        // encode uncompressed bytes length prefix using unsigned varint
        self.prefix.encode(bytes.len(), dst).map_err(TNCodecError::from)?;

        // compress data
        let mut writer = snap::write::FrameEncoder::new(Vec::new());
        writer.write_all(&bytes).map_err(TNCodecError::from)?;
        writer.flush().map_err(TNCodecError::from)?;

        // add compressed bytes to prefix
        dst.extend_from_slice(writer.get_ref());
        Ok(())
    }
}

impl Decoder for TNCodec {
    type Item = InboundRequest;

    type Error = TNCodecError;

    fn decode(
        &mut self,
        src: &mut libp2p::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        todo!()
    }
}
