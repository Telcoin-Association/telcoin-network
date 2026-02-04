# Worker Network Module

This module implements the worker's network layer for batch synchronization between consensus peers.
It provides both request-response RPC and stream-based transfer for efficient batch replication.

## Overview

Workers need to synchronize batches (collections of transactions) with peers.
When a worker receives batch digests it doesn't have locally, it requests the full batches from peers who have them.
For large transfers, stream-based transfer is more efficient than individual RPC calls.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      WorkerNetworkHandle                        │
│  (Public API for batch operations)                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  request_batches() ──► Negotiates stream with peer              │
│                        Opens libp2p::Stream                     │
│                        Calls read_and_validate_batches()        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       RequestHandler                            │
│  (Processes incoming requests from peers)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  process_request_batches_stream() ──► Validates request         │
│                                       Calls stream_codec::      │
│                                       send_batches_over_stream  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        stream_codec                             │
│  (Wire protocol for batch streaming)                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  send_batches_over_stream()  ──► Chunked sending with backpres. │
│  write_batch()               ──► Single batch serialization     │
│  read_batch()                ──► Single batch deserialization   │
│  write_batch_count()         ──► Chunk header                   │
│  read_chunk_count()          ──► Chunk header parsing           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Stream Protocol

### Wire Format

Batches are transferred in chunks to prevent memory exhaustion. Each chunk follows this format:

```
┌─────────────────────────────────────────────────────────────────┐
│ CHUNK                                                           │
├─────────────┬───────────────────────────────────────────────────┤
│ Batch Count │ Batch 0 │ Batch 1 │ ... │ Batch N │ <flush>       │
│ (4 bytes)   │         │         │     │         │               │
└─────────────┴───────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ BATCH                                                           │
├──────────────────┬─────────────────┬────────────────────────────┤
│ Uncompressed Len │ Compressed Len  │ Snappy-Compressed Data     │
│ (4 bytes LE)     │ (4 bytes LE)    │ (variable)                 │
└──────────────────┴─────────────────┴────────────────────────────┘
```

A complete transfer consists of one or more chunks, where each chunk contains up to `BATCH_DIGESTS_READ_CHUNK_SIZE` (200) batches.

### Why Two Length Fields?

The batch header includes both uncompressed and compressed lengths for precise stream parsing:

1. **Uncompressed length**: Used to validate against `max_batch_size` before allocation, preventing memory exhaustion attacks.

2. **Compressed length**: Required for exact stream reading. Without it, the receiver cannot determine where one batch ends and the next begins in a multi-batch stream.

TCP is a byte stream, not a message stream. The receiver's `read_exact()` calls have no relationship to the sender's `write_all()` calls. Including the compressed length allows the receiver to read exactly the right number of bytes for each batch.

#### Why Not Derive Compressed Length?

While `snap::raw::max_compress_len(uncompressed_len)` gives an upper bound, using it with `read_to_end()` would over-read into subsequent batches.
The snappy `FrameDecoder` doesn't provide a way to know how many compressed bytes it consumed from the underlying reader.

### Chunking Strategy

Batches are sent in chunks of up to 200 for several reasons:

1. **Memory bounds**: Limits database reads and in-memory batch accumulation on the sender.

2. **Backpressure**: Each chunk is flushed before starting the next, allowing TCP flow control to naturally throttle the sender if the receiver falls behind.

3. **Progress visibility**: The receiver can begin validation as chunks complete rather than waiting for the entire transfer.

### Flush Strategy

The stream is flushed once per chunk (not per batch) because:

1. **Throughput**: Fewer syscalls and better TCP segment packing. Flushing per batch would generate up to 200x more flush operations.

2. **No latency benefit**: The receiver reads batches sequentially with `read_exact()`, which blocks until all bytes arrive regardless of flush timing.

3. **Logical unit**: A chunk represents a complete unit of work from the sender's database read.

## Security Measures

### Size Validation Before Allocation

```rust
let max_batch_size = max_batch_size(epoch);
if uncompressed_length > max_batch_size {
    return Err(WorkerNetworkError::InvalidRequest(...));
}
```

The uncompressed length is validated against protocol limits before any buffer allocation.
This prevents a malicious peer from causing memory exhaustion by claiming an enormous batch size.

### Compressed Length Validation

```rust
let max_compressed = snap::raw::max_compress_len(max_batch_size);
if compressed_len > max_compressed {
    return Err(...);
}

// Cross-check: compressed size must be consistent with claimed uncompressed size
let expected_max = snap::raw::max_compress_len(uncompressed_len);
if compressed_len > expected_max {
    return Err(...);
}
```

The compressed length is validated against both absolute limits and consistency with the uncompressed length.
This catches malicious peers claiming small uncompressed sizes but large compressed sizes.

### Batch Count Validation

```rust
if batch_count > requested_digests.len() {
    return Err(NetworkError::ProtocolError(...));
}
```

The receiver validates that the peer isn't sending more batches than were requested.

### Digest Verification

```rust
let batch_digest = batch.digest();
if !requested_digests.contains(&batch_digest) {
    return Err(NetworkError::ProtocolError(...));
}
```

After deserializing each batch, its digest is computed and verified against the set of requested digests.
This ensures peers cannot substitute arbitrary data.

### Duplicate Detection

```rust
if !received_digests.insert(batch_digest) {
    return Err(NetworkError::ProtocolError(...));
}
```

The receiver tracks which digests have been received and rejects duplicates.

### Buffer Reuse

Buffers are allocated once at the start of a transfer and reused across batches:

```rust
let mut encode_buffer = Vec::with_capacity(max_size);
let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));
```

This prevents repeated allocations and ensures consistent memory bounds throughout the transfer.

## Compression

The protocol uses [Snappy framing format](https://github.com/google/snappy/blob/main/framing_format.txt) via the `snap` crate's `FrameEncoder`/`FrameDecoder`. This provides:

1. **Streaming compression**: Data is compressed in chunks, allowing partial decompression.

2. **CRC checksums**: Each snappy frame includes a CRC-32C checksum for integrity verification.

3. **Compatibility**: Standard format with broad tooling support.

Note that batches containing already-compressed or high-entropy data (e.g., encrypted transaction payloads) may not benefit from compression.
Snappy handles this gracefully by emitting uncompressed frames when compression doesn't reduce size.

## Error Handling

All stream operations return `WorkerNetworkResult`, with errors categorized as:

- `StreamClosed`: Peer closed connection (may indicate peer doesn't have requested batches)
- `InvalidRequest`: Protocol violation (malformed data, size limits exceeded)
- `Internal`: Unexpected errors (database failures, encoding errors)

Protocol violations should trigger peer penalties via the peer manager.

## Testing

Stream codec functions are tested in isolation with mock streams.
Integration tests in `tests/it/network_tests.rs` verify end-to-end batch synchronization between workers.
