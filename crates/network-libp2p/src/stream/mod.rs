//! Stream-based messaging for consensus network.
//!
//! This module provides stream-based communication as a replacement for request-response,
//! enabling efficient streaming of large data (like epoch pack files) with real-time verification.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     APPLICATION LAYER                           │
//! │  - Defines types implementing TNStreamMessage trait             │
//! │  - Knows ConsensusHeader, Certificate, Batch types              │
//! │  - Verifies hashes match expected digests                       │
//! │  - Writes verified records to local pack file                   │
//! │  - Decides when to abort (bad hash, timeout)                    │
//! └─────────────────────────────────────────────────────────────────┘
//!                       ↑↓ raw bytes + StreamEvent<Req, Res>
//! ┌─────────────────────────────────────────────────────────────────┐
//! │              NETWORK LAYER (StreamManager<Req, Res>)            │
//! │  - Generic over Req: TNStreamMessage, Res: TNStreamMessage      │
//! │  - Streams raw bytes (does NOT deserialize application types)   │
//! │  - Manages stream lifecycle per peer                            │
//! │  - Handles backpressure, timeouts, errors                       │
//! │  - Reports peer penalties via existing PeerManager              │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use serde::{de::DeserializeOwned, Serialize};
use std::fmt;

mod codec;
mod handler;
mod manager;
mod protocol;

/// Trait for stream-based network messages.
///
/// This is analogous to `TNMessage` for request-response, but for the stream protocol.
/// Application-layer types (like epoch sync requests) implement this trait.
pub trait TNStreamMessage:
    Send + Sync + Serialize + DeserializeOwned + Clone + fmt::Debug + 'static
{
    /// Returns true if this message initiates raw byte streaming mode.
    ///
    /// When true, after sending/receiving this message, the stream switches to raw byte
    /// mode for high-throughput data transfer (e.g., streaming epoch pack files).
    /// The application controls when to spawn background tasks for these transfers.
    fn initiates_raw_stream(&self) -> bool {
        false
    }
}

pub use codec::StreamCodec;
pub use handler::{spawn_stream_tasks, ReadEvent, StreamHandle, StreamHandlerConfig};
pub use manager::{RawStreamHandle, StreamEvent, StreamManager, StreamNetworkError};
pub use protocol::{
    FrameFlags, FrameHeader, StreamError, StreamErrorCode, StreamMessageType, TN_STREAM_PROTOCOL,
};
