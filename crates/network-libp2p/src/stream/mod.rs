//! Stream protocol for efficient bulk data transfer.
//!
//! This module provides a libp2p behavior for establishing raw streams between
//! peers. Application-layer concerns (correlation, headers, data format) are
//! handled by the caller after the stream is established.
//!
//! ## Protocol Flow
//!
//! 1. **Requestor** negotiates via request-response
//! 2. **Requestor** opens a stream to the responder using `/tn-stream/0.0.1`
//! 3. Both sides use the raw stream for application-specific data transfer
//! 4. Transfer completes and the stream closes

mod behavior;
mod handler;
mod upgrade;

pub(crate) use behavior::{StreamBehavior, StreamEvent};
pub use upgrade::StreamError;

/// Which bulk-stream protocol an exchange uses.
///
/// An outbound open names the protocol it wants to negotiate; an inbound stream
/// reports the protocol that was negotiated. The two protocols share the same
/// raw-stream upgrade but are read differently by the application: the legacy
/// path correlates by a 32-byte request digest, the sync path carries the typed
/// [`SyncFrame`](crate::sync::SyncFrame) layer with the request in its first
/// frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    /// The legacy `/tn-stream/0.0.1` raw stream, correlated to a prior
    /// request-response handshake by a written request digest.
    Legacy,
    /// The per-role typed sync protocol (`/tn-primary-sync/0.0.1` or
    /// `/tn-worker-{id}-sync/0.0.1`), carrying the [`SyncFrame`](crate::sync::SyncFrame)
    /// layer.
    Sync,
}
