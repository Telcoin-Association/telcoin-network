# Worker Network Module

This module implements the worker's network layer for batch synchronization between consensus peers.
Batches are replicated over the typed sync-stream protocol (`/tn-worker-{id}-sync`); the request
travels in the opening frame, and the response is streamed back frame by frame.

## Overview

Workers need to synchronize batches (collections of transactions) with peers.
When a worker receives batch digests it doesn't have locally, it requests the full batches from peers who have them.
A single fetch can exceed the request-response message-size limit, so the transfer is streamed rather than returned in one RPC.

The legacy request-response `/tn-stream` batch path (an ack-plus-digest handshake that then opened a
correlated raw stream) has been removed. Batch fetch now folds the request into the opening
[`SyncFrame::Req`] frame of a sync stream, so there is no separate handshake or `(peer, digest)`
correlation map to maintain.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      WorkerNetworkHandle                         │
│  (Public API for batch operations)                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  request_batches() ──► Tries connected peers with bounded retry │
│                        request_batches_from_peer_sync()         │
│                          opens a /tn-worker-{id}-sync stream    │
│                          writes SyncFrame::Req(Batches{..})     │
│                          reads Ack, then read_sync_batches()    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                       RequestHandler                            │
│  (Serves inbound sync streams from peers)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  process_inbound_sync_stream() ──► try_admit_sync() (or Deny)   │
│                                    reads opening Req frame       │
│  process_sync_batches_stream() ──► stream_codec::               │
│                                    send_sync_batches_over_stream │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                        stream_codec                             │
│  (Serves batches as typed sync frames)                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  send_sync_batches_over_stream() ──► Ack, then one Data frame   │
│                                      per batch, then End        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Sync Protocol

### Frame Sequence

A batch fetch is a single sync-stream exchange:

```
requester                         responder
─────────                         ─────────
Req(Batches{digests, epoch})  ──►
                              ◄──  Ack            (accepted; serving begins)
                              ◄──  Data(batch_0)
                              ◄──  Data(batch_1)
                              ◄──  ...
                              ◄──  End            (transfer complete)
```

Instead of `Ack`, a responder that is shedding load writes `Deny(DenyReason::AtCapacity)` and closes
without reading, so the requester gives up on that peer immediately and tries another. A serve that
fails partway (storage or encoding error) is terminated with `Err(SyncFrameError::Internal)`, and a
malformed opening frame is answered with `Err(SyncFrameError::Malformed)`.

Each batch is one [`SyncFrame::Data`] frame carrying the BCS-encoded batch. The stream is delimited
by the terminating `End` frame; there is no on-wire batch count. The responder reads batches from the
database in chunks of `BATCH_DIGESTS_READ_CHUNK_SIZE` (200) to bound the working set of a single
serve — this is a database-read batching detail, not a wire-visible framing.

### Frame Wire Format

Every frame is length-prefixed and Snappy-compressed by the shared frame codec
(`tn_network_libp2p`'s `encode_message` / `decode_message`):

```
┌─────────────────────────────────────────────────────────────────┐
│ FRAME BODY                                                      │
├──────────────────┬─────────────────┬────────────────────────────┤
│ Uncompressed Len │ Compressed Len  │ Snappy-Compressed Data     │
│ (4 bytes LE)     │ (4 bytes LE)    │ (variable)                 │
└──────────────────┴─────────────────┴────────────────────────────┘
```

### Why Two Length Fields?

Each frame body carries both its uncompressed and compressed lengths for safe, precise stream parsing:

1. **Uncompressed length**: Validated against the per-epoch frame-size bound
   (`max_batch_size(epoch) + SYNC_FRAME_OVERHEAD`) *before* any buffer is grown, preventing memory
   exhaustion attacks.

2. **Compressed length**: Required for exact stream reading. TCP is a byte stream, not a message
   stream, so the receiver's `read_exact()` calls have no relationship to the sender's `write_all()`
   calls. Knowing the compressed length lets the receiver read exactly the right number of bytes for
   each frame instead of over-reading into the next one.

#### Why Not Derive Compressed Length?

While `snap::raw::max_compress_len(uncompressed_len)` gives an upper bound, using it with
`read_to_end()` would over-read into subsequent frames. The snappy `FrameDecoder` doesn't provide a
way to know how many compressed bytes it consumed from the underlying reader.

## Retry Logic

### `request_batches()` — Bounded Retries with Backoff

When a worker needs batches it doesn't have locally, `request_batches()` tries connected peers
one at a time. If a peer is unavailable (it sheds with `Deny`, or a transient stream error occurs),
the next peer is tried.

If **all** peers fail a single pass, the method retries up to `MAX_BATCH_REQUEST_RETRIES` (3)
times through the full peer list, with a `BATCH_REQUEST_RETRY_DELAY` (500ms) pause between
attempts. This gives peer capacity time to free up.

```
Attempt 1: peer_a → deny, peer_b → deny, peer_c → deny
  ↓ sleep 500ms
Attempt 2: peer_a → Ack → batches received → return Ok
```

Key behaviors:
- **Re-fetches peers** each attempt to pick up newly connected nodes.
- **Returns immediately** on any partial success (at least one batch received).
- **Accumulates batches** across retries — digests fulfilled on earlier attempts are removed.
- **Returns `RPCError`** only after all retries are exhausted with zero batches.

A transient post-negotiation failure (a slow but sync-capable peer) is classified as a retryable
failure rather than an "unsupported" verdict, so a momentarily-slow peer is not cached as unusable
for the rest of the epoch.

## Security Measures

### Concurrency Limiting

#### Global Semaphore (`MAX_CONCURRENT_BATCH_STREAMS`)

A tokio `Semaphore` with `MAX_CONCURRENT_BATCH_STREAMS` (5) permits bounds the total number of
concurrent inbound batch serves. Admission happens in `try_admit_sync()` *before* the serving task
is spawned; the permit is held for the whole serve via the `SyncStreamPermit` RAII guard and released
when it drops.

If no permit is available, the responder writes `Deny(DenyReason::AtCapacity)` and the requesting peer
tries another node.

#### Per-Peer Rate Limiting (`MAX_PENDING_REQUESTS_PER_PEER`)

Each peer is limited to `MAX_PENDING_REQUESTS_PER_PEER` (2) concurrent in-flight batch serves,
tracked in the `sync_stream_peers` map. This prevents a single malicious peer from monopolizing all
global semaphore slots. `try_admit_sync()` admits a stream only when both the global semaphore has a
permit and the peer is below its per-peer cap, incrementing the peer's count on admission;
`SyncStreamPermit::drop` decrements it (and removes the entry at zero).

#### Oversized Request Truncation (`MAX_BATCH_DIGESTS_PER_REQUEST`)

Requests exceeding `MAX_BATCH_DIGESTS_PER_REQUEST` (500) digests are truncated rather than
rejected, on both the requester and responder sides. This serves as many batches as possible while
bounding memory:

```
Worst-case allocation: 500 entries × ~1MB per batch ≈ 500MB
vs. uncapped: ~33k digests (1MB message limit) × 1MB ≈ 33GB
```

### Size Validation Before Allocation

```rust
if uncompressed_len > max_message_size {
    return Err(...);
}
```

Each frame's uncompressed length is validated against the per-epoch bound before any buffer is grown,
and decompression is capped at that length. This prevents a malicious peer from causing memory
exhaustion by claiming an enormous size.

### Compressed Length Validation

```rust
if compressed_len > snap::raw::max_compress_len(uncompressed_len) {
    return Err(...);
}
```

The compressed length is validated for consistency with the claimed uncompressed length, and the
compressed body is streamed in bounded chunks so committed memory tracks bytes actually received.
This catches peers claiming small uncompressed sizes but large compressed sizes.

### Batch Count Validation

```rust
if batches.len() >= requested_digests.len() {
    return Err(...); // peer sent too many batches
}
```

The receiver (`read_sync_batches()`) validates that the peer isn't sending more batches than were
requested. Partial responses are allowed (a peer may hold only some of the requested batches).

### Digest Verification

```rust
let batch_digest = batch.digest();
if !requested_digests.contains(&batch_digest) {
    return Err(...); // unexpected batch
}
```

After decoding each batch, its digest is recomputed and verified against the set of requested digests.
This ensures peers cannot substitute arbitrary data.

### Duplicate Detection

```rust
if !received_digests.insert(batch_digest) {
    return Err(...); // duplicate batch
}
```

The receiver tracks which digests have been received and rejects duplicates.

### Buffer Reuse

The decode and decompression buffers are allocated once at the start of a transfer and reused across
every frame in the read loop, so a long transfer does not repeatedly reallocate and stays within
consistent memory bounds.

## Compression

The protocol uses [Snappy framing format](https://github.com/google/snappy/blob/main/framing_format.txt) via the `snap` crate's `FrameEncoder`/`FrameDecoder`. This provides:

1. **Streaming compression**: Data is compressed in chunks, allowing partial decompression.

2. **CRC checksums**: Each snappy frame includes a CRC-32C checksum for integrity verification.

3. **Compatibility**: Standard format with broad tooling support.

Note that batches containing already-compressed or high-entropy data (e.g., encrypted transaction payloads) may not benefit from compression.
Snappy handles this gracefully by emitting uncompressed frames when compression doesn't reduce size.

## Error Handling

All stream operations return `WorkerNetworkResult`, with errors categorized as:

- `StreamClosed`: Peer closed connection (may indicate the peer doesn't have the requested batches)
- `InvalidRequest`: Protocol violation (malformed data, size limits exceeded)
- `Internal`: Unexpected errors (database failures, encoding errors)

Protocol violations should trigger peer penalties via the peer manager.

## Testing

Stream codec functions are tested in isolation with mock streams.
Integration tests in `tests/it/network_tests.rs` verify end-to-end batch synchronization between workers.

Concurrency tests cover:
- Global semaphore exhaustion sheds with `Deny(AtCapacity)`
- RAII permit release when a `SyncStreamPermit` is dropped
- Per-peer limit enforcement across distinct peers
- Capacity at exactly `MAX_CONCURRENT_BATCH_STREAMS` across multiple peers
- Retry logic succeeding after initial peer rejection
