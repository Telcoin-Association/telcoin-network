//! Typed sync-frame layer for bulk request/response exchanges.
//!
//! The legacy bulk path opens a raw `/tn-stream` substream and correlates it
//! back to a prior request-response handshake by writing a 32-byte digest. This
//! module is the additive groundwork for folding that handshake into the stream
//! itself: an exchange carries its request as the first frame, and the responder
//! answers in the same stream.
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
//! behaviour registers alongside the legacy `/tn-stream/0.0.1` upgrade. This
//! layer is inert for now: no call site opens a sync stream until the
//! per-exchange cutovers migrate the worker batch, epoch pack, and missing
//! certificate paths.
//!
//! [`encode_message`]: crate::encode_message
//! [`decode_message`]: crate::decode_message

mod frame;
mod request;

pub use frame::{read_frame, write_frame, DenyReason, SyncFrame, SyncFrameError};
pub use request::{PrimarySyncRequest, WorkerSyncRequest};
