//! Stream protocol constants and frame types.
//!
//! Defines the wire protocol for stream-based messaging, supporting both typed request-response
//! and raw byte streaming. The protocol is generic - application-layer types (like epoch sync)
//! are defined elsewhere.

use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};

/// Stream protocol identifier for Telcoin Network.
pub const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn/stream/1.0.0");

/// Size of the frame header in bytes.
pub const FRAME_HEADER_SIZE: usize = 14;

/// Stream message type discriminant.
///
/// Used as a single byte in the frame header to indicate the type of message.
/// This enum is generic - it doesn't know about application-specific types like epoch sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamMessageType {
    /// Typed request (serialized application message).
    /// The payload contains a compressed, BCS-encoded request.
    TypedRequest = 0x01,

    /// Typed response to a request.
    /// The payload contains a compressed, BCS-encoded response.
    TypedResponse = 0x02,

    /// Begin raw byte streaming mode.
    /// After this frame, subsequent data is raw bytes until RawStreamEnd or stream close.
    /// The payload contains metadata about the stream (application-defined).
    RawStreamBegin = 0x10,

    /// End raw byte streaming mode.
    /// Returns to framed mode. The payload may contain final metadata.
    RawStreamEnd = 0x11,

    /// Error response.
    ErrorResponse = 0xFE,

    /// Cancel an in-progress request.
    Cancel = 0xFF,
}

impl TryFrom<u8> for StreamMessageType {
    type Error = InvalidMessageType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::TypedRequest),
            0x02 => Ok(Self::TypedResponse),
            0x10 => Ok(Self::RawStreamBegin),
            0x11 => Ok(Self::RawStreamEnd),
            0xFE => Ok(Self::ErrorResponse),
            0xFF => Ok(Self::Cancel),
            _ => Err(InvalidMessageType(value)),
        }
    }
}

impl From<StreamMessageType> for u8 {
    fn from(value: StreamMessageType) -> Self {
        value as u8
    }
}

/// Error when parsing an invalid message type byte.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("invalid stream message type: {0:#x}")]
pub struct InvalidMessageType(pub u8);

/// Frame flags byte.
///
/// Provides additional metadata about the frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FrameFlags(u8);

impl FrameFlags {
    /// No flags set.
    pub const NONE: Self = Self(0);

    /// Indicates more data follows (for chunked/streaming responses).
    pub const HAS_MORE: Self = Self(0x01);

    /// Check if the `has_more` flag is set.
    #[inline]
    pub fn has_more(&self) -> bool {
        self.0 & 0x01 != 0
    }

    /// Set or clear the `has_more` flag.
    #[inline]
    pub fn set_has_more(&mut self, val: bool) {
        if val {
            self.0 |= 0x01;
        } else {
            self.0 &= !0x01;
        }
    }

    /// Create flags with `has_more` set.
    #[inline]
    pub fn with_has_more(has_more: bool) -> Self {
        Self(if has_more { 0x01 } else { 0x00 })
    }
}

impl From<u8> for FrameFlags {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<FrameFlags> for u8 {
    fn from(value: FrameFlags) -> Self {
        value.0
    }
}

/// Frame header for stream messages.
///
/// All stream messages start with this 14-byte header:
/// ```text
/// +----------+----------+-------------+------------+
/// | req_id   | msg_type | flags       | payload_len|
/// | 8 bytes  | 1 byte   | 1 byte      | 4 bytes    |
/// +----------+----------+-------------+------------+
/// ```
///
/// The payload follows immediately after the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameHeader {
    /// Unique request ID for correlation.
    /// Responses echo the request ID from the original request.
    pub request_id: u64,

    /// Type of message (request, response, raw stream, etc.).
    pub message_type: StreamMessageType,

    /// Additional flags for the frame.
    pub flags: FrameFlags,

    /// Length of the payload in bytes.
    /// Does not include the header size.
    pub payload_len: u32,
}

impl FrameHeader {
    /// Create a new frame header.
    pub fn new(
        request_id: u64,
        message_type: StreamMessageType,
        flags: FrameFlags,
        payload_len: u32,
    ) -> Self {
        Self { request_id, message_type, flags, payload_len }
    }

    /// Create a typed request header.
    pub fn typed_request(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamMessageType::TypedRequest, FrameFlags::NONE, payload_len)
    }

    /// Create a typed response header.
    pub fn typed_response(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamMessageType::TypedResponse, FrameFlags::NONE, payload_len)
    }

    /// Create a raw stream begin header.
    ///
    /// The `has_more` flag indicates whether raw bytes will follow this frame.
    pub fn raw_stream_begin(request_id: u64, payload_len: u32, has_more: bool) -> Self {
        Self::new(
            request_id,
            StreamMessageType::RawStreamBegin,
            FrameFlags::with_has_more(has_more),
            payload_len,
        )
    }

    /// Create a raw stream end header.
    pub fn raw_stream_end(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamMessageType::RawStreamEnd, FrameFlags::NONE, payload_len)
    }

    /// Create an error response header.
    pub fn error(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamMessageType::ErrorResponse, FrameFlags::NONE, payload_len)
    }

    /// Create a cancel header.
    pub fn cancel(request_id: u64) -> Self {
        Self::new(request_id, StreamMessageType::Cancel, FrameFlags::NONE, 0)
    }

    /// Encode the header to bytes.
    pub fn encode(&self) -> [u8; FRAME_HEADER_SIZE] {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8] = self.message_type.into();
        buf[9] = self.flags.into();
        buf[10..14].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    /// Decode a header from bytes.
    pub fn decode(buf: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, InvalidMessageType> {
        let request_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let message_type = StreamMessageType::try_from(buf[8])?;
        let flags = FrameFlags::from(buf[9]);
        let payload_len = u32::from_le_bytes(buf[10..14].try_into().unwrap());

        Ok(Self { request_id, message_type, flags, payload_len })
    }
}

/// Error response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamError {
    /// Error code for programmatic handling.
    pub code: StreamErrorCode,

    /// Human-readable error message.
    pub message: String,
}

impl StreamError {
    /// Create a new stream error.
    pub fn new(code: StreamErrorCode, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

/// Error codes for stream errors.
///
/// These are network-level error codes. Application-specific error codes
/// should be defined in the application layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum StreamErrorCode {
    /// Unknown or unspecified error.
    Unknown = 0,

    /// The peer cannot fulfill the request (resource constraints).
    Unavailable = 1,

    /// The request was malformed or invalid.
    BadRequest = 2,

    /// Internal error on the serving peer.
    InternalError = 3,

    /// Request was cancelled.
    Cancelled = 4,

    /// Request timed out.
    Timeout = 5,

    /// Application-level error (check error message for details).
    /// Used when the application layer returns an error that doesn't
    /// map to a specific network-level code.
    ApplicationError = 100,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_header_roundtrip() {
        let header =
            FrameHeader::new(12345, StreamMessageType::TypedRequest, FrameFlags::NONE, 1024);

        let encoded = header.encode();
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE);

        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_frame_header_with_flags() {
        let header = FrameHeader::raw_stream_begin(999, 2048, true);

        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.request_id, 999);
        assert_eq!(decoded.message_type, StreamMessageType::RawStreamBegin);
        assert!(decoded.flags.has_more());
        assert_eq!(decoded.payload_len, 2048);
    }

    #[test]
    fn test_frame_flags() {
        let mut flags = FrameFlags::NONE;
        assert!(!flags.has_more());

        flags.set_has_more(true);
        assert!(flags.has_more());

        flags.set_has_more(false);
        assert!(!flags.has_more());
    }

    #[test]
    fn test_message_type_roundtrip() {
        for mt in [
            StreamMessageType::TypedRequest,
            StreamMessageType::TypedResponse,
            StreamMessageType::RawStreamBegin,
            StreamMessageType::RawStreamEnd,
            StreamMessageType::ErrorResponse,
            StreamMessageType::Cancel,
        ] {
            let byte: u8 = mt.into();
            let decoded = StreamMessageType::try_from(byte).unwrap();
            assert_eq!(decoded, mt);
        }
    }

    #[test]
    fn test_invalid_message_type() {
        assert!(StreamMessageType::try_from(0x00).is_err());
        assert!(StreamMessageType::try_from(0x50).is_err());
    }
}
