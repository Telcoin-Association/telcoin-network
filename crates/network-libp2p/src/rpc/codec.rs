//! Encoding/decoding for RPC.

use std::io::{self, Cursor, ErrorKind, Read as _, Write as _};

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
    /// Data failed to be decoded.
    #[error("{0}")]
    InvalidData(String),
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
    /// The temporary length of decoded bytes while framing via buffers.
    ///
    /// See [tokio_util::codec::decoder::Decoder] for more information.
    temp_len: Option<usize>,
    /// The maximum size (bytes) in a chunked request/response.
    max_chunk_size: usize,
}

impl TNCodec {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        let prefix = Uvi::default();
        Self { prefix, temp_len: None, max_chunk_size }
    }

    /// Process the length prefix for messages as an unsigned protobuf varint.
    fn process_length_prefix(
        &mut self,
        bytes: &mut libp2p::bytes::BytesMut,
    ) -> CodecResult<Option<usize>> {
        if let Some(length) = self.temp_len {
            Ok(Some(length))
        } else {
            // decode the prefix and update the tracked temp length
            //
            // NOTE: length of > 10 bytes is decoding error (uint64)
            let res = self.prefix.decode(bytes).map_err(TNCodecError::from)?.inspect(|len| {
                // update temp length if some
                self.temp_len = Some(*len);
            });

            Ok(res)
        }
    }

    /// Process the sanitized bytes.
    fn complete_rpc_request<T>(&mut self, bytes: &[u8]) -> CodecResult<T> {
        // reset temp value
        self.temp_len = None;
        todo!()
    }

    /// Process errors during decoding.
    fn process_decode_error<T>(
        &self,
        error: std::io::Error,
        num_bytes: u64,
        max_compressed_len: u64,
    ) -> CodecResult<Option<T>> {
        match error.kind() {
            ErrorKind::UnexpectedEof => {
                // snappy decodes based on `max_compressed_len`
                // so an unfilled buffer is treated as malicious
                //
                // TODO: this results in a peer being banned
                if num_bytes >= max_compressed_len {
                    Err(TNCodecError::InvalidData(format!(
                        "snappy message perceived as malicious: num_bytes {}, max_compressed_len {}",
                        num_bytes, max_compressed_len
                    )))
                } else {
                    // not enough bytes to decode yet
                    Ok(None)
                }
            }
            _ => Err(TNCodecError::from(error)),
        }
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
        // TODO: protocol request limits
        //
        // // Should not attempt to decode rpc chunks with `length > max_packet_size` or not within bounds of
        // // packet size for ssz container corresponding to `self.protocol`.
        // let ssz_limits = self.protocol.rpc_request_limits(&self.fork_context.spec);
        // if ssz_limits.is_out_of_bounds(length, self.max_packet_size) {
        //     return Err(RPCError::InvalidData(format!(
        //         "RPC request length for protocol {:?} is out of bounds, length {}",
        //         self.protocol.versioned_protocol, length
        //     )));
        // }
        //

        // decode message's reported length in prefix
        let Some(prefix) = self.process_length_prefix(src)? else { return Ok(None) };

        // determine max compression length possible based on msg prefix
        //
        // NOTE: usize -> u64 won't lose precision
        let max_compressed_len = snap::raw::max_compress_len(prefix) as u64;

        // read message up to the max possible compression length
        let limited_reader = Cursor::new(src.as_ref()).take(max_compressed_len);
        let mut reader = snap::read::FrameDecoder::new(limited_reader);
        let mut decoded_buffer = vec![0; prefix];

        match reader.read_exact(&mut decoded_buffer) {
            Ok(()) => self.complete_rpc_request(
                // self.protocol.versioned_protocol,
                &decoded_buffer,
                // &self.fork_context.spec,
            ),
            Err(e) => self.process_decode_error(
                e,
                reader.get_ref().get_ref().position(),
                max_compressed_len,
            ),
        }
    }
}
