//! Typed sync-frame layer for bulk request/response exchanges.
//!
//! An exchange carries its request as the first frame of the stream, and the
//! responder answers in the same stream: there is no separate request-response
//! handshake or digest correlation. This is the sole bulk-transfer path.
//!
//! ## Framing
//!
//! Every exchange is a sequence of [`SyncFrame`]s, each written as a single
//! [`encode_message`]/[`decode_message`] unit (length-prefixed, snappy
//! compressed, BCS encoded). The BCS enum discriminant is the frame tag:
//!
//! 1. The requester opens with a [`SyncFrame::Req`] carrying the typed request
//!    ([`WorkerSyncRequest`] or [`PrimarySyncRequest`]).
//! 2. The responder answers [`SyncFrame::Ack`] to accept or [`SyncFrame::Deny`] to shed load, so an
//!    over-capacity responder declines immediately instead of making the requester wait out its
//!    timeout.
//! 3. An accepted exchange streams zero or more [`SyncFrame::Data`] frames and finishes with
//!    [`SyncFrame::End`], or aborts with [`SyncFrame::Err`].
//!
//! ## Protocols
//!
//! The frames ride on streams negotiated with the per-role sync protocols
//! (`/tn-primary-sync/0.0.1`, `/tn-worker-{id}-sync/0.0.1`), which the stream
//! behaviour registers as its sole upgrade. Every bulk path (worker batch, epoch
//! pack, missing certificates, consensus output) rides this layer.
//!
//! [`encode_message`]: crate::encode_message
//! [`decode_message`]: crate::decode_message

mod frame;
mod request;

pub use frame::{read_frame, write_frame, DenyReason, SyncFrame, SyncFrameError};
pub use request::{PrimarySyncRequest, WorkerSyncRequest};
