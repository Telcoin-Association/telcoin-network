//! Frame-tagged transport for bulk-sync exchanges.

use crate::codec::{decode_message, encode_message};
use futures::{AsyncRead, AsyncWrite};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// A single frame in a bulk-sync exchange.
///
/// Each frame is one [`encode_message`]/[`decode_message`] unit on the wire, so
/// the BCS enum discriminant doubles as the frame tag. An exchange opens with a
/// [`SyncFrame::Req`] carrying the typed request; the responder answers
/// [`SyncFrame::Ack`] or [`SyncFrame::Deny`]; an accepted exchange then streams
/// zero or more [`SyncFrame::Data`] frames terminated by [`SyncFrame::End`], or
/// aborts with [`SyncFrame::Err`].
///
/// `R` is the typed request the [`SyncFrame::Req`] frame carries
/// ([`WorkerSyncRequest`](crate::sync::WorkerSyncRequest) or
/// [`PrimarySyncRequest`](crate::sync::PrimarySyncRequest)).
///
/// The variant order is the wire format: BCS encodes each variant by its index,
/// so variants must never be reordered or removed without a protocol-version
/// bump. `frame_tags_are_stable` pins the mapping.
///
/// [`encode_message`]: crate::encode_message
/// [`decode_message`]: crate::decode_message
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncFrame<R> {
    /// REQ: opening frame carrying the typed request that initiates the
    /// exchange.
    Req(R),
    /// ACK: the responder accepted the request and will stream [`SyncFrame::Data`]
    /// frames.
    Ack,
    /// DENY: the responder declined the request (e.g. it is over capacity) so the
    /// requester can give up immediately instead of waiting out its timeout.
    Deny(DenyReason),
    /// DATA: a chunk of opaque response payload. The payload encoding is defined
    /// by each exchange's cutover, not by this frame layer. Encoded as one byte string
    /// (byte-identical to a sequence of `u8`), not byte by byte.
    Data(#[serde(with = "tn_types::byte_vec")] Vec<u8>),
    /// END: orderly end of the response stream.
    End,
    /// ERR: the responder aborted the exchange after an error.
    Err(SyncFrameError),
}

/// Why a responder declined a sync request in a [`SyncFrame::Deny`] frame.
///
/// The wire value is the variant's BCS index, which the explicit discriminant states and
/// `frame_tags_are_stable` pins. A value this build does not know decodes as
/// [`DenyReason::AtCapacity`] instead of failing the frame, so a reason that a newer
/// responder sends still reads as a denial, not as a malformed frame. New reasons must be
/// unit variants that take the next free value. A decoder that predates
/// [`DenyReason::Busy`] rejects an unknown value: its [`read_frame`] returns an
/// `InvalidData` io error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenyReason {
    /// The responder is at its global concurrency cap and is shedding load; the requester
    /// should retry elsewhere or back off. A responder that predates [`DenyReason::Busy`]
    /// also sends it for a per-peer refusal.
    AtCapacity = 0,
    /// The responder does not hold the requested data.
    Unavailable = 1,
    /// The responder refused this requester under its per-peer serve cap or a per-key hold
    /// budget. The requester should skip the peer and retry it later. The refusal is not a
    /// failure of the responder.
    Busy = 2,
}

impl DenyReason {
    /// The variant names in wire order, for self-describing formats.
    const NAMES: &'static [&'static str] = &["AtCapacity", "Unavailable", "Busy"];

    /// The reason's value on the wire.
    const fn wire_value(self) -> u32 {
        match self {
            Self::AtCapacity => 0,
            Self::Unavailable => 1,
            Self::Busy => 2,
        }
    }

    /// The reason's variant name.
    const fn name(self) -> &'static str {
        match self {
            Self::AtCapacity => "AtCapacity",
            Self::Unavailable => "Unavailable",
            Self::Busy => "Busy",
        }
    }

    /// Decode a wire value. A value this build does not know is [`DenyReason::AtCapacity`].
    const fn from_wire(value: u64) -> Self {
        match value {
            1 => Self::Unavailable,
            2 => Self::Busy,
            // 0, and every value this build does not know
            0 | 3..=u64::MAX => Self::AtCapacity,
        }
    }

    /// Decode a variant name. A name this build does not know is [`DenyReason::AtCapacity`].
    fn from_name(name: &str) -> Self {
        Self::NAMES
            .iter()
            .position(|known| *known == name)
            .and_then(|index| u64::try_from(index).ok())
            .map_or(Self::AtCapacity, Self::from_wire)
    }
}

impl Serialize for DenyReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_unit_variant("DenyReason", self.wire_value(), self.name())
    }
}

impl<'de> Deserialize<'de> for DenyReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_enum("DenyReason", Self::NAMES, DenyReasonVisitor)
    }
}

/// Decodes a [`DenyReason`] from its variant tag, leniently (see [`DenyReason`]).
struct DenyReasonVisitor;

impl<'de> serde::de::Visitor<'de> for DenyReasonVisitor {
    type Value = DenyReason;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a DenyReason unit variant")
    }

    fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::EnumAccess<'de>,
    {
        let (DenyReasonTag(reason), variant) = data.variant::<DenyReasonTag>()?;
        serde::de::VariantAccess::unit_variant(variant).map(|()| reason)
    }
}

/// The variant tag of a [`DenyReason`]: a wire index or a variant name.
struct DenyReasonTag(DenyReason);

impl<'de> Deserialize<'de> for DenyReasonTag {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_identifier(DenyReasonTagVisitor)
    }
}

/// Maps a variant tag to a [`DenyReason`], unknown tags to [`DenyReason::AtCapacity`].
struct DenyReasonTagVisitor;

impl serde::de::Visitor<'_> for DenyReasonTagVisitor {
    type Value = DenyReasonTag;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a DenyReason variant index or name")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DenyReasonTag(DenyReason::from_wire(value)))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(DenyReasonTag(DenyReason::from_name(value)))
    }
}

/// Why a responder aborted an exchange in a [`SyncFrame::Err`] frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncFrameError {
    /// The responder hit an internal error while producing the response.
    Internal,
    /// The opening request frame was malformed or violated the protocol.
    Malformed,
}

/// Write a single [`SyncFrame`] to `io`, length-prefixed and snappy-compressed
/// via [`encode_message`].
///
/// `max_frame_size` bounds the encoded frame; the caller supplies reusable
/// buffers to avoid repeated allocation across frames.
///
/// [`encode_message`]: crate::encode_message
pub async fn write_frame<T, R>(
    io: &mut T,
    frame: &SyncFrame<R>,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_frame_size: usize,
) -> std::io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
    R: Serialize + Sync,
{
    encode_message(io, frame, encode_buffer, compressed_buffer, max_frame_size).await
}

/// Read a single [`SyncFrame`] from `io` via [`decode_message`].
///
/// `max_frame_size` bounds the decoded frame so a peer cannot force an oversized
/// allocation; the caller supplies reusable buffers. An [`SyncFrame::Err`] frame
/// is a successful read (`Ok`), distinct from a transport error (`Err`).
///
/// [`decode_message`]: crate::decode_message
pub async fn read_frame<T, R>(
    io: &mut T,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
    max_frame_size: usize,
) -> std::io::Result<SyncFrame<R>>
where
    T: AsyncRead + Unpin + Send,
    R: DeserializeOwned,
{
    decode_message(io, decode_buffer, compressed_buffer, max_frame_size).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SyncFrame` as it was derived before `Data` was encoded as a byte string (element by
    /// element): the oracle the byte-string encoding must match exactly.
    #[derive(Serialize, Deserialize)]
    enum ElementwiseFrame {
        Req(u32),
        Ack,
        Deny(DenyReason),
        Data(Vec<u8>),
        End,
        Err(SyncFrameError),
    }

    /// Encoding a `Data` payload as one byte string leaves the frame's bytes exactly as the
    /// element-by-element derive wrote them, and each encoding decodes the other's bytes.
    #[test]
    fn data_frame_encodes_identically_as_a_byte_string() {
        for len in [0_usize, 1, 127, 128, 16_384, 1 << 20] {
            let payload: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let frame = SyncFrame::<u32>::Data(payload.clone());
            let bytes = bcs::to_bytes(&frame).expect("encode frame");
            assert_eq!(
                bytes,
                bcs::to_bytes(&ElementwiseFrame::Data(payload.clone())).expect("encode oracle"),
                "{len}-byte payload: bytes must not change"
            );
            let decoded: SyncFrame<u32> = bcs::from_bytes(&bytes).expect("decode frame");
            assert_eq!(decoded, frame);
            let old: ElementwiseFrame =
                bcs::from_bytes(&bytes).expect("decode with the old derive");
            assert!(matches!(old, ElementwiseFrame::Data(p) if p == payload));
        }
    }

    /// A 1 MiB cap, generous enough that no test frame is rejected for size
    /// except the one that deliberately overflows it.
    const MAX_FRAME: usize = 1024 * 1024;

    /// Round-trip a frame through `write_frame`/`read_frame` over an in-memory
    /// buffer, returning the decoded frame.
    async fn roundtrip<R>(frame: &SyncFrame<R>) -> SyncFrame<R>
    where
        R: Serialize + DeserializeOwned + Sync,
    {
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        let mut wire = Vec::new();
        write_frame(&mut wire, frame, &mut enc, &mut comp, MAX_FRAME).await.expect("write frame");

        let (mut dec, mut comp2) = (Vec::new(), Vec::new());
        read_frame(&mut wire.as_ref(), &mut dec, &mut comp2, MAX_FRAME).await.expect("read frame")
    }

    #[tokio::test]
    async fn every_control_frame_round_trips() {
        // unit request payload: the control frames are request-type agnostic
        for frame in [
            SyncFrame::<()>::Ack,
            SyncFrame::Deny(DenyReason::AtCapacity),
            SyncFrame::Deny(DenyReason::Unavailable),
            SyncFrame::Deny(DenyReason::Busy),
            SyncFrame::Data(Vec::new()),
            SyncFrame::Data(vec![0, 1, 2, 3, 4, 5, 6, 7]),
            SyncFrame::End,
            SyncFrame::Err(SyncFrameError::Internal),
            SyncFrame::Err(SyncFrameError::Malformed),
        ] {
            assert_eq!(roundtrip(&frame).await, frame);
        }
    }

    #[tokio::test]
    async fn req_frame_round_trips() {
        let frame = SyncFrame::Req(0xC0FFEE_u32);
        assert_eq!(roundtrip(&frame).await, frame);
    }

    /// The BCS discriminant is the wire tag. Pin the exact bytes so a reorder or
    /// insertion of a `SyncFrame` variant cannot silently change the framing of
    /// an already-deployed protocol.
    #[test]
    fn frame_tags_are_stable() {
        let tag = |frame: &SyncFrame<()>| bcs::to_bytes(frame).expect("encode frame");
        assert_eq!(tag(&SyncFrame::Req(())), vec![0]);
        assert_eq!(tag(&SyncFrame::Ack), vec![1]);
        assert_eq!(tag(&SyncFrame::Deny(DenyReason::AtCapacity)), vec![2, 0]);
        assert_eq!(tag(&SyncFrame::Deny(DenyReason::Unavailable)), vec![2, 1]);
        assert_eq!(tag(&SyncFrame::Deny(DenyReason::Busy)), vec![2, 2]);
        assert_eq!(tag(&SyncFrame::Data(Vec::new())), vec![3, 0]);
        assert_eq!(tag(&SyncFrame::End), vec![4]);
        assert_eq!(tag(&SyncFrame::Err(SyncFrameError::Internal)), vec![5, 0]);
        assert_eq!(tag(&SyncFrame::Err(SyncFrameError::Malformed)), vec![5, 1]);
    }

    /// A deny reason this build does not know decodes as `AtCapacity`, so a newer
    /// responder's reason reads as a denial and not as a malformed frame. The reason is a
    /// ULEB128 index: `[0x80, 0x01]` is 128.
    #[test]
    fn unknown_deny_reason_decodes_as_at_capacity() {
        let at_capacity = SyncFrame::<()>::Deny(DenyReason::AtCapacity);
        let next: SyncFrame<()> = bcs::from_bytes(&[2, 3]).expect("decode an unknown reason");
        assert_eq!(next, at_capacity);
        let wide: SyncFrame<()> =
            bcs::from_bytes(&[2, 0x80, 0x01]).expect("decode a two-byte unknown reason");
        assert_eq!(wide, at_capacity);
    }

    #[tokio::test]
    async fn frame_larger_than_max_is_rejected() {
        let frame = SyncFrame::<()>::Data(vec![0u8; 256]);
        let (mut enc, mut comp) = (Vec::new(), Vec::new());
        let mut wire = Vec::new();
        // a 64-byte cap is smaller than the 256-byte payload
        let result = write_frame(&mut wire, &frame, &mut enc, &mut comp, 64).await;
        assert!(result.is_err());
    }
}
