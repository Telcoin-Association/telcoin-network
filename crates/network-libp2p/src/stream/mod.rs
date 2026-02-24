//! Stream-based sync protocol for efficient bulk data transfer.
//!
//! This module provides a libp2p behavior for establishing raw streams between
//! peers. Application-layer concerns (correlation, headers, data format) are
//! handled by the caller after the stream is established.
//!
//! ## Protocol Flow
//!
//! 1. **Requestor** negotiates via request-response
//! 2. **Requestor** opens a stream to the responder using `/tn-stream/1.0.0`
//! 3. Both sides use the raw stream for application-specific data transfer
//! 4. Transfer completes and the stream closes

mod behavior;
mod handler;
mod upgrade;

pub(crate) use behavior::{StreamBehavior, StreamEvent};
pub use upgrade::StreamError;
