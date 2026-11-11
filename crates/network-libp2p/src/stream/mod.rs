//! Stream protocol for efficient bulk data transfer.
//!
//! This module provides a libp2p behavior for establishing raw streams between
//! peers. Application-layer concerns (framing, data format) are handled by the
//! caller after the stream is established.
//!
//! ## Protocol Flow
//!
//! 1. **Requestor** opens a stream to the responder using the chain-namespaced per-role sync
//!    protocol (`/tn-primary-sync-<chain>/0.0.1` or `/tn-worker-<id>-sync-<chain>/0.0.1`)
//! 2. **Requestor** writes its request as the first [`SyncFrame`](crate::sync::SyncFrame) on the
//!    stream. The open returns before the responder confirms the protocol (`V1Lazy`), so the
//!    request travels with the protocol proposal and the open costs no round trip
//! 3. **Requestor** reads the response. A responder that does not serve the protocol rejects the
//!    proposal, and the first read fails with an error that [`is_unsupported_protocol`] recognizes
//! 4. Both sides use the raw stream for the typed sync data transfer
//! 5. Transfer completes and the stream closes

mod behavior;
mod handler;
mod upgrade;

pub(crate) use behavior::{StreamBehavior, StreamEvent};
pub use upgrade::{is_unsupported_protocol, StreamError};
