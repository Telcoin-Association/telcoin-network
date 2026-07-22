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
//!    stream
//! 3. Both sides use the raw stream for the typed sync data transfer
//! 4. Transfer completes and the stream closes

mod behavior;
mod handler;
mod upgrade;

pub(crate) use behavior::{StreamBehavior, StreamEvent};
pub use upgrade::StreamError;
