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
    /// by each exchange's cutover, not by this frame layer.
    Data(Vec<u8>),
    /// END: orderly end of the response stream.
    End,
    /// ERR: the responder aborted the exchange after an error.
    Err(SyncFrameError),
}

/// Why a responder declined a sync request in a [`SyncFrame::Deny`] frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DenyReason {
    /// The responder is at its concurrency cap (global or per-peer) and is
    /// shedding load; the requester should retry elsewhere or back off.
    AtCapacity,
    /// The responder does not hold the requested data.
    Unavailable,
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
        assert_eq!(tag(&SyncFrame::Data(Vec::new())), vec![3, 0]);
        assert_eq!(tag(&SyncFrame::End), vec![4]);
        assert_eq!(tag(&SyncFrame::Err(SyncFrameError::Internal)), vec![5, 0]);
        assert_eq!(tag(&SyncFrame::Err(SyncFrameError::Malformed)), vec![5, 1]);
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
