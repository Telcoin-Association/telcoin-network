# Sync streams

Telcoin Network moves bulk data — sealed batches, epoch pack files, missing certificates, single consensus outputs — over a dedicated stream protocol rather than over request-response.
A node opens one substream, writes its request as the first frame, and reads the response off that same stream until the responder ends it.
There is no separate handshake and no digest correlation: the request travels in the stream that carries the answer.

This page is for validators, RPC providers, dapp and indexer operators, and bridge partners who need to know how a node catches up and what it accepts while doing so.
It covers the exchange shape, the frame tags, the request discriminants, and the limits that bound an open.

The implementation lives in
[`crates/network-libp2p/src/stream/`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/stream/),
which holds the custom libp2p behaviour and connection handler, and
[`crates/network-libp2p/src/sync/`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/sync/),
which holds the frame layer and the typed requests.
[Request-response](request-response.md) covers the RPC path this one deliberately avoids, and [Transport](transport.md) has the full protocol-ID reference table.

## Why this is not request-response

The request-response codec caps a single message at 1 MiB by default.
That bound is right for a vote, a header, or a peer-exchange map, and wrong for catch-up.
A single missing-certificates exchange is allowed to stream up to 64 MiB of encoded certificates, and an epoch pack is chunked at 256 KiB per frame precisely because the whole pack can run to hundreds of megabytes.
Expressing those transfers as request-response would mean hundreds of correlated RPCs, each needing its own pending-request entry on both sides.

The stream path is a custom behaviour and connection handler, not libp2p's `libp2p-stream`.
Writing it in-tree is what makes the open budget, the per-peer inbound rate limit, and the failure classification below possible.
Each node advertises exactly one stream protocol, chosen by its role and namespaced by chain id:

```text
primary   /tn-primary-sync-{chain_id}/0.0.1
worker    /tn-worker-{worker_id}-sync-{chain_id}/0.0.1
```

A worker advertises only its own worker index, so worker 0 of one validator never negotiates a sync stream with worker 1 of another.
The chain id in the protocol name means a node on one chain fails negotiation against a node on another rather than exchanging data with it.

## The exchange

```text
1. requester opens a substream on the role's sync protocol
2. requester writes  Req(<typed request>)    <- always the first frame
3. responder writes  Ack                     <- accepted
                 or  Deny(<reason>)          <- declined; stream closes
4. responder writes  Data(<bytes>) ...       <- zero or more payload frames
5. responder writes  End                     <- orderly finish
                 or  Err(<reason>)           <- aborted after an error
6. both sides close the stream
```

The requester waits 5 seconds for the first response frame.
A peer that negotiates the protocol but never answers trips that timeout, and the requester moves to the next peer instead of stalling the whole catch-up.
On the responding side, a peer that opens a stream and never sends its request frame is dropped after 5 seconds, so it cannot hold an admission slot by staying silent.

## Frame tags

Every frame is a self-contained encoded unit, and the enum discriminant is the frame tag.

```text
tag   frame           payload
----  --------------  -------------------------------------------
 0    Req(request)    the typed request for this role
 1    Ack             none
 2    Deny(reason)    0 = AtCapacity, 1 = Unavailable
 3    Data(bytes)     an opaque chunk of response payload
 4    End             none
 5    Err(reason)     0 = Internal, 1 = Malformed
```

`Data` payload encoding is defined by each exchange, not by the frame layer.
The frame layer only guarantees that a `Data` frame arrives whole or not at all.

The typed request inside a `Req` frame carries its own discriminant, one table per role:

```text
WorkerSyncRequest                    on /tn-worker-{worker_id}-sync-{chain_id}/0.0.1
tag   variant              fields
----  -------------------  ------------------------------------
 0    Batches              batch_digests, epoch
```

```text
PrimarySyncRequest                   on /tn-primary-sync-{chain_id}/0.0.1
tag   variant              fields
----  -------------------  ------------------------------------
 0    EpochPack            epoch
 1    MissingCertificates  exclusive_lower_bound, skip_rounds
 2    EpochPackPartial     epoch, last_consensus_number
 3    ConsensusOutput      number
```

Note that the request tags are not in source-declaration reading order by name: `MissingCertificates` is 1 and `EpochPackPartial` is 2, because the partial variant was added after the missing-certificates one and appended rather than inserted.

## Variant order is the wire format

> [!WARNING]
> Reordering, inserting, or removing a variant in any of these enums changes the bytes a deployed peer sees, without changing a single type signature.
> The code still compiles, the round-trip tests still pass, and a peer running the old build decodes the wrong variant from the same tag byte.
> This is silent corruption, not a build break.

The protocol version in the name (`0.0.1`) is the only thing that can signal such a change, so a variant reorder requires a version bump and the negotiation failure that comes with it.
New variants may be appended at the end, because appending leaves every existing tag where it was.

Two tests pin the mapping: one asserts the exact frame tag bytes, the other asserts the leading discriminant byte of each request variant.
A reorder that would otherwise pass every behavioural test fails those.

## Frame encoding

Each frame is a single codec unit and inherits the encoding described in [Request-response](request-response.md): a length-prefixed, snappy-compressed BCS message, read against the bytes that actually arrive rather than against the length a peer declares.
The difference is the size bound.
Request-response applies one fixed 1 MiB cap to every message, while each sync exchange supplies its own frame cap: a worker batch exchange sizes it from the epoch's maximum batch size, and a primary epoch-pack exchange sizes it from the 256 KiB pack chunk.
The cap bounds one frame, so no peer can force an oversized allocation, but it does not bound the transfer, which is what makes bulk catch-up possible on this path.

An `Err` frame is a successful read.
It reports that the responder gave up, which is different from the transport failing underneath the exchange, and the two are handled differently.

## Deny is backpressure, not a fault

A responder admits a bounded number of concurrent sync streams: five per role, and at most two in-flight sync streams from any one peer.
Past that it does not drop the stream.
It writes `Deny(AtCapacity)` and closes.

That distinction matters to a requester's wall-clock cost.
A dropped stream costs the requester its full 5-second ack timeout before it can try elsewhere; an explicit `Deny` costs it one round trip.
A node catching up against a busy committee therefore keeps moving instead of serializing on timeouts, and a busy responder sheds load without being scored for it.

`Deny(Unavailable)` is the other normal case: the peer speaks the protocol and has capacity but does not hold the requested data.

The shed path is itself bounded.
Writing a `Deny` costs a short-lived task, and at most eight of those run at once; beyond that budget the stream is dropped without a `Deny`, and the requester sees a reset and retries elsewhere.

## Limits

| Limit | Value | What it bounds |
|-------|-------|----------------|
| Pending outbound opens | 1024 | Opens buffered across the whole behaviour; past this an open is rejected immediately rather than queued |
| Total open budget | 15s | Dial, connection establishment, and substream negotiation combined; on expiry the caller gets a timeout |
| Substream negotiation | 10s | A single outbound substream negotiation, enforced by libp2p |
| Stale sweep | 1s | How often expired pending opens are failed and cleared |
| Inbound streams per peer | 256 per second | Inbound sync streams accepted from one peer per one-second window; the next one is refused |
| Pending outbound per connection | 256 | Opens buffered by one connection handler before it sheds |

The 15-second and 10-second budgets nest: the total open budget covers a dial that may itself have to establish a connection, while the negotiation timeout applies only once a connection exists.
An open that is dispatch-ready when its peer disconnects is re-queued for dialing rather than handed to a handler that no longer exists.

## Failure classification

Stream failures are classified into a taxonomy that mirrors the request-response one, so comparable misbehaviour scores comparably on both paths:

```text
failure               penalty   rationale
--------------------  --------  -----------------------------------------
DialFailure           none      transport-level; not the peer's fault
Timeout               mild      a stalled open
UnsupportedProtocol   none      honest version or role skew
Io (transport flap)   none      reset, abort, EOF, broken pipe, interrupt
Io (other)            medium    a genuine protocol or codec violation
InboundRateLimited    medium    abuse of the inbound stream path
```

A failed negotiation is not even reported to the behaviour.
Probing whether a peer speaks the sync protocol is a normal thing to do during a rolling upgrade, and a probe that comes back negative should cost nothing.

**These penalties are classified and logged, not enforced.**
Every classified failure produces a log line recording the penalty it would incur, and nothing is applied to the peer's score.
Enforcement is gated on telemetry confirming the classification does not fire on healthy peers.
Until it is turned on, a peer cannot be banned by anything on this page — see [Peers](peers.md) for the scoring that is enforced.

## Source of truth

| Behavior | Code |
|----------|------|
| Frame tags, deny and error reasons, frame read/write | `crates/network-libp2p/src/sync/frame.rs` |
| Typed request enums and their pinned discriminants | `crates/network-libp2p/src/sync/request.rs` |
| Open budget, stale sweep, inbound rate limit, pending-open cap | `crates/network-libp2p/src/stream/behavior.rs` |
| Negotiation timeout, per-connection pending cap, error classification | `crates/network-libp2p/src/stream/handler.rs` |
| Advertised upgrade, failure taxonomy and penalty mapping | `crates/network-libp2p/src/stream/upgrade.rs` |
| Penalty reporting (metrics-only, not enforced) | `crates/network-libp2p/src/consensus.rs` (`process_stream_event`) |
| Sync protocol IDs | `crates/network-libp2p/src/types.rs` (`sync_protocol`) |
| Shared codec bounds | `crates/network-libp2p/src/codec.rs` |
| Per-exchange frame and transfer caps | `crates/consensus/primary/src/network/sync_codec.rs`, `crates/consensus/worker/src/network/handle.rs` |

This page mirrors those files.
Update this page when those files change.
