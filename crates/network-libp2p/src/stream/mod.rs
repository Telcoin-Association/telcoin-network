// SPDX-License-Identifier: MIT or Apache-2.0
//! Streaming protocol for efficient request/response messaging.
//!
//! Uses `libp2p-stream` to replace the traditional request-response pattern,
//! eliminating per-message round-trip latency and the 1 MiB message size bottleneck.

pub mod codec;

use libp2p::StreamProtocol;

/// Protocol identifier for TN streaming.
pub const TN_STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/tn/stream/1.0.0");
