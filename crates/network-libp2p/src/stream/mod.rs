//! Stream-based sync protocol for efficient bulk data transfer.
//!
//! This module provides a libp2p behavior for establishing streams between peers
//! after an initial request-response negotiation. The two-phase approach allows
//! for negotiation before committing to a potentially large data transfer.
//!
//! ## Protocol Flow
//!
//! 1. **Requestor** sends a `SyncStateRequest` via request-response to negotiate sync parameters
//! 2. **Responder** replies with `SyncStateResponse` containing metadata (total size, chunk count)
//! 3. **Requestor** opens a stream to the responder using the `/tn-sync/1.0.0` protocol
//! 4. **Responder** accepts the stream and begins sending data
//! 5. Transfer completes and the stream closes
//!
//! This module is generic and can be used for different sync types:
//! - Primaries use it for epoch sync
//! - Workers use it for batch sync

mod behavior;
mod handler;
mod upgrade;

pub use behavior::{StreamBehavior, StreamCommand, StreamEvent, TN_STREAM_PROTOCOL};
pub use upgrade::{StreamHeader, StreamSyncError, TNStreamProtocol};
