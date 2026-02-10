# Implementation Plan: Stream-Based Batch Fetching for Workers

## Background

This plan continues work on the libp2p stream infrastructure for the Telcoin Network consensus layer. The stream protocol was initially built to support bulk data transfer for state synchronization.

### Previous Work Completed

The generic stream infrastructure in `crates/network-libp2p/src/stream/` is complete:

- `StreamBehavior` integrated into `TNBehavior`
- `StreamHandler` for per-connection stream management
- `TNStreamProtocol` for protocol upgrades (inbound/outbound)
- `StreamHeader` for stream identification and correlation
- `NetworkCommand::OpenSyncStream` for opening streams
- `NetworkEvent::InboundSyncStream` for receiving streams
- `NetworkHandle::open_sync_stream()` method

### Current Task

Replace the current request-response batch fetching in workers with a stream-based approach. Workers need to sync missing batches when they receive certificates containing batch digests they don't have locally.

**Key files for context:**

- `crates/consensus/worker/src/batch_fetcher.rs` - Current batch fetching logic
- `crates/consensus/worker/src/network/mod.rs` - Worker network event handling
- `crates/consensus/worker/src/network/handler.rs` - Request processing
- `crates/consensus/worker/src/network/message.rs` - Worker message types
- `crates/network-libp2p/src/stream/` - Stream infrastructure
- `crates/network-libp2p/src/codec.rs` - TNCodec with snappy compression pattern

---

## Overview

Replace the current request-response batch fetching in workers with a stream-based approach that:

1. Negotiates sync via request-response (uses `request_digest` for correlation)
2. Requestor opens stream to responder with `request_digest` in header
3. Responder validates stream using pending request, looks up batches from DB, sends length-prefixed batches
4. Requestor validates each batch in real-time as it arrives
5. Invalid batch terminates stream immediately and penalizes peer

**Key Design Principles:**

- Network layer remains **generic** (StreamHeader includes the request type)
- Per-batch bounded buffer (max 1MB per batch in memory - based on config)
- Security-first: validate before storing, terminate on first invalid batch
- Try one peer at a time with fallback
- Reuse existing codec patterns from `tn-network-libp2p`
- Extend existing `WorkerNetworkError` rather than creating new error types

---

## Stream Protocol Format

```
┌─────────────────────────────────────────────────────────────────────┐
│ StreamHeader (written by requestor, read by responder)              │
├─────────────────────────────────────────────────────────────────────┤
│ [4-byte len][BCS-encoded header]                                    │
│                                                                     │
│ StreamHeader {                                                      │
|   resource_id: u64,                   // generic resource id        |
│   request_digest: [u8; 32],           // hash of original request   │
│ }                                                                   │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ Batch Data (written by responder, read by requestor)                │
├─────────────────────────────────────────────────────────────────────┤
│ [4-byte batch_count: u32]                                           │
├─────────────────────────────────────────────────────────────────────┤
│ For each batch (reusing TNCodec pattern):                           │
│   [4-byte uncompressed_len][snappy-compressed BCS-encoded batch]    │
└─────────────────────────────────────────────────────────────────────┘
```

**Size validation formula:**

```rust
let max_allowed_size = requested_batch_count * max_batch_size(epoch);
// e.g., 10 batches * 1MB = 10MB max total
```

---

## Request-Response Negotiation Flow

### Step 1: Requestor sends request

```rust
WorkerRequest::RequestBatches {
    batch_digests: Vec<BlockHash>,  // the batches we need
    max_response_size: usize,       // ignored for stream mode
}
```

### Step 2: Responder checks availability and returns acceptance

```rust
WorkerResponse::RequestBatchesStream {
    ack: bool, // true if able to fulfill request, otherwise false
}
```

**Why `request_digest`?**

- Computed as `hash(encode(original_request))` by both peers
- Requestor includes hash of original request in StreamHeader.request_digest
- Responder validates incoming stream's request_digest matches pending request
- Prevents rogue streams / ensures correlation between negotiation and stream

### Step 3: Requestor opens stream

```rust
// Requestor computes the same hash
let request_digest = hash(encode(&original_request));

// Open stream with hash for correlation
let stream = network.open_sync_stream(
    peer,
    request_digest,    // used to negotiate outbound stream protocol
    stream_header,   // StreamHeader with original `Req`
).await?;
```

### Step 4: Responder accepts inbound stream

```rust
// In WorkerNetwork::process_network_event
NetworkEvent::InboundStream { peer, stream, header } => {
    // Validate header.request_digest matches a pending request from this peer
    let key = (peer, header.request_digest);
    if let Some(pending) = self.pending_batch_requests.remove(&key) {
        // Look up batches from DB using pending.batch_digests
        // Send batches over stream
        self.send_batches_over_stream(stream, &pending.batch_digests).await;
    } else {
        // No matching pending request - drop stream, penalize peer
        drop(stream);
        self.report_penalty(peer, Penalty::Fatal);
    }
}
```

---

## Pending Request Tracking (Responder Side)

**Key insight:** Don't store batches in memory. Store only the request metadata and look up batches from DB when the stream arrives.

```rust
/// Tracks a pending batch stream request awaiting stream establishment.
struct PendingBatchRequest {
    /// When this request was created (for timeout cleanup).
    created_at: std::time::Instant,
}

/// Key for pending requests: (peer_bls, request_digest)
type PendingBatchRequestKey = (BlsPublicKey, [u8; 32]);
```

The responder:

1. Receives `WorkerRequest::RequestBatches { batch_digests, .. }`
2. Computes `request_digest = hash(encode(request))`
3. Stores `PendingBatchRequest { batch_digests, created_at }` keyed by `(peer, request_digest)`
4. Returns `WorkerResponse::RequestBatchesStream { ack: true }`
5. When stream arrives with matching `(peer, request_digest)`:
   - Looks up batches from DB using `PendingBatchRequest::batch_digests`
   - Streams batches to requestor

---

## Codec Reuse Strategy

The existing `TNCodec` in `crates/network-libp2p/src/codec.rs` provides:

- Length-prefixed messages (4-byte prefix for uncompressed length)
- Snappy compression
- BCS serialization
- Buffer reuse patterns

**For stream reading/writing, create new module:** `crates/consensus/worker/src/network/stream_codec.rs`

```rust
//! Stream codec utilities for batch sync.
//!
//! Reuses patterns from tn-network-libp2p's TNCodec for consistent
//! length-prefixed, snappy-compressed BCS encoding.

use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use snap::{read::FrameDecoder, write::FrameEncoder};
use std::io::{Read, Write};
use tn_types::{encode_into_buffer, Batch, BlockHash, max_batch_size, Epoch};

use super::error::{WorkerNetworkError, WorkerNetworkResult};

/// Read a single length-prefixed, snappy-compressed batch from a stream.
///
/// Returns the decoded batch or an error if:
/// - The length prefix exceeds max_batch_size
/// - Decompression fails
/// - BCS deserialization fails
pub async fn read_batch<T>(
    io: &mut T,
    epoch: Epoch,
    decode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
) -> WorkerNetworkResult<Batch>
where
    T: AsyncRead + Unpin + Send,
{
    // Clear buffers
    decode_buffer.clear();
    compressed_buffer.clear();

    // Read 4-byte length prefix (uncompressed size)
    let mut prefix = [0u8; 4];
    io.read_exact(&mut prefix).await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to read batch prefix: {e}"))
    })?;

    let length = u32::from_le_bytes(prefix) as usize;

    // SECURITY: Validate length before allocation
    let max_size = max_batch_size(epoch);
    if length > max_size {
        return Err(WorkerNetworkError::InvalidRequest(format!(
            "batch size {length} exceeds max {max_size}"
        )));
    }

    // Resize decode buffer to reported size
    decode_buffer.resize(length, 0);

    // Read compressed data (limited by max possible compression size)
    let max_compress_len = snap::raw::max_compress_len(length);
    io.take(max_compress_len as u64)
        .read_to_end(compressed_buffer)
        .await
        .map_err(|e| WorkerNetworkError::Internal(format!("failed to read batch data: {e}")))?;

    // Decompress
    let reader = std::io::Cursor::new(&compressed_buffer);
    let mut decoder = FrameDecoder::new(reader);
    decoder.read_exact(decode_buffer).map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to decompress batch: {e}"))
    })?;

    // Deserialize
    bcs::from_bytes(decode_buffer).map_err(WorkerNetworkError::Decode)
}

/// Write a single batch as length-prefixed, snappy-compressed data.
pub async fn write_batch<T>(
    io: &mut T,
    batch: &Batch,
    encode_buffer: &mut Vec<u8>,
    compressed_buffer: &mut Vec<u8>,
) -> WorkerNetworkResult<()>
where
    T: AsyncWrite + Unpin + Send,
{
    // Clear buffers
    encode_buffer.clear();
    compressed_buffer.clear();

    // Encode batch
    encode_into_buffer(encode_buffer, batch).map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to encode batch: {e}"))
    })?;

    // Write length prefix (uncompressed size)
    let prefix = (encode_buffer.len() as u32).to_le_bytes();
    io.write_all(&prefix).await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to write batch prefix: {e}"))
    })?;

    // Compress and write
    let mut encoder = FrameEncoder::new(&mut *compressed_buffer);
    encoder.write_all(encode_buffer).map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to compress batch: {e}"))
    })?;
    encoder.flush().map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to flush compression: {e}"))
    })?;

    io.write_all(compressed_buffer).await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to write batch data: {e}"))
    })?;

    Ok(())
}

/// Read batch count header from stream.
pub async fn read_chunk_count<T>(io: &mut T) -> WorkerNetworkResult<u32>
where
    T: AsyncRead + Unpin + Send,
{
    let mut buf = [0u8; 4];
    io.read_exact(&mut buf).await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to read batch count: {e}"))
    })?;
    Ok(u32::from_le_bytes(buf))
}

/// Write batch count header to stream.
pub async fn write_batch_count<T>(io: &mut T, count: u32) -> WorkerNetworkResult<()>
where
    T: AsyncWrite + Unpin + Send,
{
    io.write_all(&count.to_le_bytes()).await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to write batch count: {e}"))
    })?;
    Ok(())
}

/// Send batches over stream, looking up from database.
pub async fn send_batches_over_stream<DB, S>(
    stream: &mut S,
    store: &DB,
    batch_digests: &[BlockHash],
) -> WorkerNetworkResult<()>
where
    DB: Database,
    S: AsyncWrite + Unpin + Send,
{
    // Allocate reusable buffers
    let mut encode_buffer = Vec::with_capacity(max_batch_size(Epoch::default()));
    let mut compressed_buffer = Vec::with_capacity(
        snap::raw::max_compress_len(max_batch_size(Epoch::default()))
    );

    // Look up batches from DB
    let batches: Vec<_> = store
        .multi_get::<Batches>(batch_digests.iter())
        .map_err(|e| WorkerNetworkError::Internal(format!("DB error: {e}")))?
        .into_iter()
        .filter_map(|opt| opt)
        .collect();

    // Write batch count
    write_batch_count(stream, batches.len() as u32).await?;

    // Write each batch
    for batch in batches {
        write_batch(stream, &batch, &mut encode_buffer, &mut compressed_buffer).await?;
    }

    stream.flush().await.map_err(|e| {
        WorkerNetworkError::Internal(format!("failed to flush stream: {e}"))
    })?;

    Ok(())
}
```

---

## Error Handling

Extend existing `WorkerNetworkError` with stream-specific variants:

**File:** `crates/consensus/worker/src/network/error.rs`

```rust
#[derive(Debug, thiserror::Error)]
pub enum WorkerNetworkError {
    // ... existing variants ...

    /// Peer sent more batches than expected.
    #[error("Peer sent too many batches: expected {expected}, received {received}")]
    TooManyBatches { expected: usize, received: usize },

    /// Peer sent a batch we didn't request.
    #[error("Received unexpected batch with digest {0}")]
    UnexpectedBatch(BlockHash),

    /// Peer sent duplicate batch.
    #[error("Received duplicate batch with digest {0}")]
    DuplicateBatch(BlockHash),

    /// Stream was closed unexpectedly.
    #[error("Stream closed unexpectedly")]
    StreamClosed,

    /// No matching pending request for inbound stream.
    #[error("No pending request matches stream hash")]
    UnknownStreamRequest,

    /// Request hash mismatch between negotiation and stream.
    #[error("Request hash mismatch")]
    RequestHashMismatch,
}

impl From<WorkerNetworkError> for Option<Penalty> {
    fn from(val: WorkerNetworkError) -> Self {
        match val {
            // ... existing matches ...

            // Malicious behavior - fatal penalty
            WorkerNetworkError::TooManyBatches { .. } => Some(Penalty::Fatal),
            WorkerNetworkError::UnexpectedBatch(_) => Some(Penalty::Fatal),
            WorkerNetworkError::DuplicateBatch(_) => Some(Penalty::Fatal),
            WorkerNetworkError::UnknownStreamRequest => Some(Penalty::Fatal),
            WorkerNetworkError::RequestHashMismatch => Some(Penalty::Fatal),

            // Connection issues - no penalty
            WorkerNetworkError::StreamClosed => None,
        }
    }
}
```

---

## Implementation Phases

### Phase 1: Add Message Types

**File:** `crates/consensus/worker/src/network/message.rs`

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum WorkerResponse {
    // ... existing variants ...

    /// Accept batch request.
    /// Requestor should open stream with request hash in StreamHeader.expected_hash.
    RequestBatchesStream {
        ack: bool, // true if able to fulfill request, otherwise false
    }
}
```

### Phase 2: Add Stream Codec Module

**File:** `crates/consensus/worker/src/network/stream_codec.rs` (NEW)

See "Codec Reuse Strategy" section above.

### Phase 3: Add Error Variants

**File:** `crates/consensus/worker/src/network/error.rs`

Add the new error variants and penalty mappings.

### Phase 4: Add Pending Request Tracking

**File:** `crates/consensus/worker/src/network/mod.rs`

```rust
use std::collections::HashMap;

/// Tracks a pending batch stream request awaiting stream establishment.
struct PendingBatchRequest {
    /// The batch digests requested (looked up from DB when stream arrives).
    batch_digests: Vec<BlockHash>,
    /// When this request was created (for timeout cleanup).
    created_at: std::time::Instant,
}

pub struct WorkerNetwork<DB, Events> {
    // ... existing fields ...

    /// Pending batch requests awaiting stream from requestor.
    /// Key: (peer_bls, request_digest)
    pending_batch_requests: HashMap<(BlsPublicKey, [u8; 32]), PendingBatchRequest>,
}
```

### Phase 5: Implement Responder Side

**File:** `crates/consensus/worker/src/network/handler.rs`

Add `process_request_batches_for_stream` and `compute_request_digest` functions.

**File:** `crates/consensus/worker/src/network/mod.rs`

- Update `process_request_batches` to optionally use stream mode
- Handle `NetworkEvent::InboundSyncStream` to send batches

### Phase 6: Implement Requestor Side (BatchFetcher)

**File:** `crates/consensus/worker/src/batch_fetcher.rs`

Add:

- `fetch_batches_via_stream` - single peer stream fetch
- `read_and_validate_batches` - real-time batch validation
- `fetch_via_stream` - public method with peer fallback

### Phase 7: Add Timeout Cleanup

**File:** `crates/consensus/worker/src/network/mod.rs`

```rust
const PENDING_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

impl<DB, Events> WorkerNetwork<DB, Events> {
    fn cleanup_stale_pending_requests(&mut self) {
        let now = std::time::Instant::now();
        self.pending_batch_requests.retain(|_, pending| {
            now.duration_since(pending.created_at) < PENDING_REQUEST_TIMEOUT
        });
    }
}
```

---

## Files to Modify

| File                                                  | Changes                                              |
| ----------------------------------------------------- | ---------------------------------------------------- |
| `crates/consensus/worker/src/network/message.rs`      | Add `RequestBatchesStream` response variant          |
| `crates/consensus/worker/src/network/error.rs`        | Add stream-specific error variants                   |
| `crates/consensus/worker/src/network/stream_codec.rs` | **NEW** - Stream read/write utilities                |
| `crates/consensus/worker/src/network/mod.rs`          | Add pending request tracking, handle inbound streams |
| `crates/consensus/worker/src/network/handler.rs`      | Add `process_request_batches_for_stream` method      |
| `crates/consensus/worker/src/batch_fetcher.rs`        | Add stream-based fetch methods                       |

---

## Security Considerations

1. **Memory bounded**: Per-batch buffer capped at `max_batch_size(epoch)` (1MB)
2. **Size validation before allocation**: Check length prefix before allocating buffer
3. **Hash validation**: `request_digest` prevents rogue/replayed streams
4. **Real-time validation**: Each batch validated immediately, stream terminated on first error
5. **Peer penalties**: All malicious behaviors result in peer score reduction (Fatal for protocol violations)
6. **Timeout protection**: All operations have timeouts to prevent hanging
7. **No batch storage in pending requests**: Only metadata stored, batches looked up from DB on demand

---

## Verification

After implementation:

1. `cargo build -p tn-worker` - verify compilation
2. `cargo test -p tn-worker` - verify existing tests pass
3. `cargo clippy -p tn-worker` - check for warnings
4. Manual testing with invalid batch injection (future test phase)
