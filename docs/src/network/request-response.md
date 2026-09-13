# Request-response

Telcoin Network runs two independent libp2p request-response behaviours on every node.
The consensus RPC carries votes, batch reports, and epoch records between peers of the same role.
A dedicated peer-exchange protocol carries the goodbye a node sends before it disconnects.
Both share one codec and one bounded read path; they differ in what they carry and in how their failures are scored.

This page is for RPC providers, dapp and indexer operators, bridge partners, and validators
who need to know what a node accepts on these protocols, what it rejects,
and which rejections cost a peer reputation.

The implementation lives in the libp2p networking crate:
the [codec and its read-path bounds](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/codec.rs),
the [protocol identifiers and message plumbing](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/types.rs),
and the [behaviour wiring and failure handling](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/consensus.rs).

## The two protocols

The consensus RPC is namespaced by role, worker index, and chain id, and is currently at protocol version `/0.0.2`.
A primary advertises `/tn-primary-{chain_id}/0.0.2`; worker `k` advertises `/tn-worker-{k}-{chain_id}/0.0.2`.
Both sides support the protocol fully, so any node can be requester or responder.
[Transport](transport.md) explains the `/0.0.2` version bump and the full protocol-identifier table.

The peer-exchange goodbye is a separate behaviour on its own name, still at `/0.0.1`:
`/tn-primary-peer-exchange-{chain_id}/0.0.1` and `/tn-worker-{k}-peer-exchange-{chain_id}/0.0.1`.
Its request is the disconnecting node's exchange map and its response is an empty acknowledgement.
Splitting it out means the exchange map no longer has to ride inside the consensus request enums,
and a node that cannot negotiate the dedicated protocol falls back to the variant still embedded in those enums.
A node holds at most 10 goodbyes in flight and waits at most 3 seconds for the acknowledgement before disconnecting anyway.

## Wire format

Every message on both protocols, in both directions, is framed identically:

```text
[4-byte LE uncompressed_len][4-byte LE compressed_len][snappy-framed BCS]
```

The two length prefixes are little-endian `u32`.
They do not count toward the maximum message size; only the decoded payload does.
The payload is BCS-serialized and then snappy-compressed in framed format.

## Read-path bounds

The decoder validates before it commits memory, in this order.

1. **The declared uncompressed length is checked against the maximum message size before anything is allocated.**
   The default maximum is 1 MiB.
   A prefix larger than that is rejected at the header, so no buffer is sized from an attacker's number.
2. **The declared compressed length is checked against snappy's maximum expansion for that uncompressed size.**
   Snappy bounds how far compressed output can grow relative to its input, so a compressed length past that bound cannot describe the uncompressed length the peer just declared.
   Catching it at the header means a lying prefix is rejected before the body read starts rather than after it.
3. **The body is streamed in 8 KiB chunks.**
   Committed memory tracks bytes that actually arrived rather than the declared prefix.
   A peer cannot make a node reserve a megabyte by claiming one: a peer that declares a large body and then withholds it stalls holding only what it sent, until the request timeout reaps the stream.
   An early end of stream is an error, not a short read.
4. **Decompression is bounded to the validated uncompressed length**, so a malformed or hostile snappy stream cannot expand past the maximum message size.
5. **The final decompressed length must equal the declared length exactly.**
   A mismatch in either direction is rejected before deserialization is attempted.

Only after all five checks does the decoder hand the bytes to BCS.
A failure at any step surfaces as an I/O error on the stream, which the failure table below classifies.

## Configuration

| Setting | Value |
|---------|-------|
| Request timeout, inbound and outbound | 10 seconds |
| Concurrent streams per connection | 100 |
| Maximum message size | 1 MiB (operator-tunable as `max_rpc_message_size`) |

The timeout and stream cap are libp2p's defaults and are not overridden.
The message size is read from node configuration and applies to the consensus RPC and the goodbye protocol alike.

## Message inventory

Primary requests are `Vote`, `PeerExchange`, and `EpochRecord`.
`Vote` carries the proposed header and any parent certificates the requester thinks the responder may be missing.
`EpochRecord` carries an optional epoch number and an optional digest; with neither set, the responder returns the latest epoch record it has.

Primary responses are `Vote`, `MissingParents`, `EpochRecord`, `PeerExchange`, `Error`, and `RecoverableError`.
`MissingParents` is the responder saying it could not verify the header's parents and naming the digests it needs before it can vote.

Worker requests are `ReportBatch` and `PeerExchange`.
Worker responses are `ReportBatch` (an acceptance with no payload), `PeerExchange`, `Error`, and `RecoverableError`.

A response of the wrong shape for the request is a permanent error at the requester, never a retryable condition.

## Permanent and recoverable errors

Both roles split application-layer failures into two response variants, and the distinction is load-bearing.

`Error` is a permanent rejection.
The responder evaluated the request and refused it on its merits: an invalid header or batch, a decode failure, a malformed request, a peer that is not in the committee, an unavailable epoch, a batch epoch mismatch, a duplicate batch.
An identical request would be refused the same way, so the requester must not retry it.

`RecoverableError` is a transient responder-side condition: a momentary batch-store write failure, internal channel pressure during an epoch transition, an internal timeout, a database read or commit failure.
The request is likely to succeed later, so the requester retries rather than giving up on the peer.
A worker retries a batch report up to three times with backoff when the response is recoverable, and stops immediately when it is permanent.
The primary's vote path retries a recoverable response up to six more times at 250 ms intervals before treating it as permanent.
Without the split, a peer that hit a one-off internal error would be indistinguishable from a peer that deliberately rejected the payload.

> [!NOTE]
> One epoch mismatch is deliberately classified recoverable rather than permanent.
> When a peer reports an invalid-epoch header and the peer's epoch is exactly one ahead of ours,
> the responder returns `RecoverableError`, because that is a race at the epoch boundary and not a fault.
> Every other epoch mismatch, including a peer more than one epoch ahead or any epoch behind, is a permanent `Error`.

## Failure classification and penalties

Transport-level failures are not the peer's fault and carry no penalty.
Charging them would ban honest peers on ordinary wide-area conditions:
with several requests in flight at disconnect time, one penalty each would compound into an immediate ban.
[Peers](peers.md) explains what each penalty level costs and how many occurrences reach a ban.

| Failure | Outbound | Inbound |
|---------|----------|---------|
| Dial failure | None | Not applicable |
| Connection closed | None | None |
| Timeout | Mild | None |
| I/O error of a transport-flap kind | None | None |
| I/O error of any other kind | Medium | Medium |
| Unsupported protocols | None | None |
| Response omission | Not applicable | None |
| Any peer-exchange goodbye failure | None | None |

A transport-flap I/O error is a connection reset, connection abort, timed-out read, unexpected end of file, broken pipe, or interrupted call.
Anything else is very likely a codec violation, which is why it is the one failure kind that carries a Medium penalty in both directions.

`UnsupportedProtocols` carries no penalty in either direction, and that exemption is deliberate.
Failing to negotiate a common protocol is honest version or role skew:
the peer runs an older release, a different role's protocol set, or a different chain id.
Penalizing it would turn a chain-id or version split into a network partition,
because the nodes that most need to find each other again during a rolling upgrade would ban each other first.
The node logs a warning for operator visibility instead.

Peer-exchange goodbye failures are never penalized in either direction.
A goodbye precedes a disconnect, so there is no relationship left to protect.
The one failure that changes course is an outbound `UnsupportedProtocols`, which triggers the fallback to the embedded exchange variant.

## Application-layer admission control

Below the network layer, the primary and worker handlers cap the work a peer can commission.
Load beyond a cap is shed, not queued: the requester gets a fast denial and tries another peer.

| Limit | Value | What it bounds |
|-------|-------|----------------|
| Pending sync requests per peer | 2 | A single peer's in-flight sync streams, so it cannot fill the global slots and starve other peers. Enforced separately by the primary and the worker. |
| Concurrent epoch stream operations | 5 | Pending plus active epoch streams on a primary, across all peers. |
| Concurrent epoch-record serves | 5 | Concurrent `EpochRecord` request-response serves on a primary. Its own budget, separate from epoch streams, so an epoch-record flood and the stream paths cannot starve one another. |
| Concurrent batch stream operations | 5 | Pending plus active batch streams on a worker, across all peers. |
| Concurrent shed tasks | 8 | Short-lived tasks that write a denial to a stream refused at admission. Enforced separately by the primary and the worker, so total sync-task fan-out is the admitted cap plus this. |
| Consensus-result tallies | 20 | Distinct pending consensus results a primary tracks while tallying signatures. A floor, not a ceiling: the effective cap is the larger of this and the committee size. |
| Epoch votes deduplicated | 1024 | Distinct verified epoch votes retained for ingress deduplication, so a replayed vote is dropped before a second signature verification. |
| Batch digests per request | 500 | Digests a peer may name in one sync batch request. Sized well above the realistic maximum and roughly sixty-six times below what would fit in a 1 MiB message. |

The last three bound the gossip and sync ingress paths of the same handlers rather than the request-response arms,
but they are the caps that decide how much work one peer's traffic can commission from a node, so operators size against them together.

## Source of truth

| Behavior | Code |
|----------|------|
| Wire format, read-path bounds, chunked body read | `crates/network-libp2p/src/codec.rs` |
| Protocol identifiers and versions | `crates/network-libp2p/src/types.rs` (`NetworkType`) |
| Permanent and recoverable error variants | `crates/network-libp2p/src/error.rs` (`NetworkError`, `RpcFailure`) |
| Behaviour config, failure classification, penalties | `crates/network-libp2p/src/consensus.rs` |
| Message size, goodbye budget and timeout | `crates/config/src/network.rs` (`LibP2pConfig`) |
| Primary message variants and error classification | `crates/consensus/primary/src/network/message.rs` |
| Worker message variants and error classification | `crates/consensus/worker/src/network/message.rs` |
| Primary admission-control limits | `crates/consensus/primary/src/network/mod.rs` |
| Worker admission-control limits | `crates/consensus/worker/src/network/mod.rs` |

This page mirrors those files.
Update this page when those files change.
