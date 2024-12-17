//! Encoding/decoding for RPC.

use unsigned_varint::codec::Uvi;

/// The type that encodes/decodes RPC messages.
pub struct TNCodec {
    /// Encoder for message length prefix (unsigned protobuf varint).
    prefix_encoder: Uvi<usize>,
    /// The maximum size (bytes) in a chunked request/response.
    max_chunk_size: usize,
}

impl TNCodec {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        let prefix_encoder = Uvi::default();
        Self { prefix_encoder, max_chunk_size }
    }
}
