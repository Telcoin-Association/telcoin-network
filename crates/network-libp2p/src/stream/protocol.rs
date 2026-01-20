//! Stream protocol constants and frame types.
//!
//! Defines the wire protocol for stream-based messaging, supporting both typed request-response
//! (for backwards compatibility) and raw byte streaming (for epoch pack files).

use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};

/// Stream protocol identifier for Telcoin Network.
pub const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn/stream/1.0.0");

/// Size of the frame header in bytes.
pub const FRAME_HEADER_SIZE: usize = 14;

/// Stream request types discriminant.
///
/// Used as a single byte in the frame header to indicate the type of message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamRequestType {
    /// Traditional typed request-response (serialized application messages).
    /// The payload contains a compressed, BCS-encoded request or response.
    TypedRequest = 0x01,

    /// Typed response to a request.
    TypedResponse = 0x02,

    /// Stream epoch pack file request.
    /// After the initial metadata response, raw pack file bytes follow.
    EpochStreamRequest = 0x10,

    /// Epoch stream metadata response (sent before raw bytes).
    EpochStreamMeta = 0x11,

    /// Error response.
    ErrorResponse = 0xFE,

    /// Cancel an in-progress request.
    Cancel = 0xFF,
}

impl TryFrom<u8> for StreamRequestType {
    type Error = InvalidRequestType;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::TypedRequest),
            0x02 => Ok(Self::TypedResponse),
            0x10 => Ok(Self::EpochStreamRequest),
            0x11 => Ok(Self::EpochStreamMeta),
            0xFE => Ok(Self::ErrorResponse),
            0xFF => Ok(Self::Cancel),
            _ => Err(InvalidRequestType(value)),
        }
    }
}

impl From<StreamRequestType> for u8 {
    fn from(value: StreamRequestType) -> Self {
        value as u8
    }
}

/// Error when parsing an invalid request type byte.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("invalid stream request type: {0:#x}")]
pub struct InvalidRequestType(pub u8);

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

    /// Type of message (request, response, error, etc.).
    pub request_type: StreamRequestType,

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
        request_type: StreamRequestType,
        flags: FrameFlags,
        payload_len: u32,
    ) -> Self {
        Self { request_id, request_type, flags, payload_len }
    }

    /// Create a typed request header.
    pub fn typed_request(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamRequestType::TypedRequest, FrameFlags::NONE, payload_len)
    }

    /// Create a typed response header.
    pub fn typed_response(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamRequestType::TypedResponse, FrameFlags::NONE, payload_len)
    }

    /// Create an epoch stream request header.
    pub fn epoch_stream_request(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamRequestType::EpochStreamRequest, FrameFlags::NONE, payload_len)
    }

    /// Create an epoch stream metadata response header.
    pub fn epoch_stream_meta(request_id: u64, payload_len: u32, has_more: bool) -> Self {
        Self::new(
            request_id,
            StreamRequestType::EpochStreamMeta,
            FrameFlags::with_has_more(has_more),
            payload_len,
        )
    }

    /// Create an error response header.
    pub fn error(request_id: u64, payload_len: u32) -> Self {
        Self::new(request_id, StreamRequestType::ErrorResponse, FrameFlags::NONE, payload_len)
    }

    /// Create a cancel header.
    pub fn cancel(request_id: u64) -> Self {
        Self::new(request_id, StreamRequestType::Cancel, FrameFlags::NONE, 0)
    }

    /// Encode the header to bytes.
    pub fn encode(&self) -> [u8; FRAME_HEADER_SIZE] {
        let mut buf = [0u8; FRAME_HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8] = self.request_type.into();
        buf[9] = self.flags.into();
        buf[10..14].copy_from_slice(&self.payload_len.to_le_bytes());
        buf
    }

    /// Decode a header from bytes.
    pub fn decode(buf: &[u8; FRAME_HEADER_SIZE]) -> Result<Self, InvalidRequestType> {
        let request_id = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let request_type = StreamRequestType::try_from(buf[8])?;
        let flags = FrameFlags::from(buf[9]);
        let payload_len = u32::from_le_bytes(buf[10..14].try_into().unwrap());

        Ok(Self { request_id, request_type, flags, payload_len })
    }
}

/// Request to stream an epoch pack file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochStreamRequest {
    /// The epoch number to stream.
    pub epoch: u64,

    /// Byte offset to start streaming from.
    /// Use 0 to start from the beginning.
    /// Use a non-zero value to resume an interrupted transfer.
    pub start_offset: u64,
}

impl EpochStreamRequest {
    /// Create a new epoch stream request.
    pub fn new(epoch: u64, start_offset: u64) -> Self {
        Self { epoch, start_offset }
    }

    /// Create a request to stream from the beginning.
    pub fn from_start(epoch: u64) -> Self {
        Self::new(epoch, 0)
    }
}

/// Metadata response for an epoch stream.
///
/// Sent before the raw pack file bytes begin streaming.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochStreamResponse {
    /// The epoch being streamed.
    pub epoch: u64,

    /// Total size of the pack file in bytes, if known.
    /// May be `None` if the file is still being written.
    pub total_size: Option<u64>,

    /// Unique identifier of the pack file.
    /// Used to verify consistency when resuming interrupted transfers.
    pub pack_uid: u64,
}

impl EpochStreamResponse {
    /// Create a new epoch stream response.
    pub fn new(epoch: u64, total_size: Option<u64>, pack_uid: u64) -> Self {
        Self { epoch, total_size, pack_uid }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum StreamErrorCode {
    /// Unknown or unspecified error.
    Unknown = 0,

    /// The requested epoch is not available.
    EpochNotFound = 1,

    /// The requested offset is out of bounds.
    InvalidOffset = 2,

    /// The peer cannot fulfill the request (resource constraints).
    Unavailable = 3,

    /// The request was malformed or invalid.
    BadRequest = 4,

    /// Internal error on the serving peer.
    InternalError = 5,

    /// Request was cancelled.
    Cancelled = 6,

    /// Request timed out.
    Timeout = 7,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_header_roundtrip() {
        let header =
            FrameHeader::new(12345, StreamRequestType::TypedRequest, FrameFlags::NONE, 1024);

        let encoded = header.encode();
        assert_eq!(encoded.len(), FRAME_HEADER_SIZE);

        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn test_frame_header_with_flags() {
        let header = FrameHeader::epoch_stream_meta(999, 2048, true);

        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.request_id, 999);
        assert_eq!(decoded.request_type, StreamRequestType::EpochStreamMeta);
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
    fn test_request_type_roundtrip() {
        for rt in [
            StreamRequestType::TypedRequest,
            StreamRequestType::TypedResponse,
            StreamRequestType::EpochStreamRequest,
            StreamRequestType::EpochStreamMeta,
            StreamRequestType::ErrorResponse,
            StreamRequestType::Cancel,
        ] {
            let byte: u8 = rt.into();
            let decoded = StreamRequestType::try_from(byte).unwrap();
            assert_eq!(decoded, rt);
        }
    }

    #[test]
    fn test_invalid_request_type() {
        assert!(StreamRequestType::try_from(0x00).is_err());
        assert!(StreamRequestType::try_from(0x50).is_err());
    }
}
