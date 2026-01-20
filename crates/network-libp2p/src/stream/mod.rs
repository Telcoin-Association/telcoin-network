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
//! │  - Knows ConsensusHeader, Certificate, Batch types              │
//! │  - Verifies hashes match expected digests                       │
//! │  - Writes verified records to local pack file                   │
//! │  - Decides when to abort (bad hash, timeout)                    │
//! └─────────────────────────────────────────────────────────────────┘
//!                               ↑↓ raw bytes + stream control
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                     NETWORK LAYER (StreamManager)               │
//! │  - Streams raw bytes (does NOT deserialize application types)   │
//! │  - Manages stream lifecycle per peer                            │
//! │  - Handles backpressure, timeouts, errors                       │
//! │  - Reports peer penalties via existing PeerManager              │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

mod codec;
mod handler;
mod manager;
mod protocol;

pub use codec::StreamCodec;
pub use handler::{StreamHandle, StreamHandlerConfig};
pub use manager::{
    EpochStreamError, EpochStreamHandle, StreamEvent, StreamManager, StreamManagerError,
};
pub use protocol::{
    EpochStreamRequest, EpochStreamResponse, FrameFlags, FrameHeader, StreamError, StreamErrorCode,
    StreamRequestType, TN_STREAM_PROTOCOL,
};
