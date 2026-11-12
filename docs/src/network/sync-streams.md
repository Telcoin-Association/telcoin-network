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

The outbound open is lazy.
The requester writes `Req` without waiting for the responder to accept the protocol, so the request frame travels in the same flight as the protocol proposal and is already buffered when the responder starts its opening read.
The requester waits 5 seconds for the first response frame.
A peer that negotiates the protocol but never answers trips that timeout, and the requester moves to the next peer instead of stalling the whole catch-up.
On a primary that probe counts as failed, and the peer is marked for the fetched epoch.
A protocol rejection, a clean close or a reset on the first read of a consensus-output request, and a rejection of a partial epoch-pack request, leave no mark only when they come within 1 second of the request write (`SYNC_BUSY_DENY_WINDOW`); later they count as a failed probe too, so a peer cannot hold every probe for most of the timeout and then close to stay unmarked.
On the worker, the whole batch stream is also bounded (95 seconds), so one peer can use at most 120 seconds of a 480-second `request_batches` call: the batches validated before the bound are kept, and the peer is recorded as stalled for the epoch and asked after the unmarked peers of its tier on later calls; a complete exchange slower than its allowance (15 seconds plus 1 second per batch) is marked too, and an empty answer is marked only when it took 5 seconds or more.
Each tier of the connected-peer list is shuffled per request so no peer keeps a fixed early place.
On the responding side, a peer that opens a stream and never sends its request frame is dropped after 1 second if it is a public-tier peer and after 5 seconds if it is a committee peer.
While it waits it holds only an opening-read slot, never a serve slot.
A public-tier peer is charged a mild penalty for a silent, unreadable or unexpected opening, for a valid request that arrives in the second half of its 1-second opening window, for a stalled serve, and, for an epoch pack, for a serve cut at the 200-second hold limit, and, for a missing-certificates or consensus-output serve, for a serve cut at the 60-second hold limit, and, for a worker batch serve, for a serve cut at the 60-second hold limit and for a request above the 500-digest cap.
A late request is charged whether it is then admitted or refused; a prompt `AtCapacity` or `Busy` refusal is not charged (see [Admission](#admission)).
A full pool answers `Deny(AtCapacity)`; on a primary, the per-peer cap and a per-key hold budget answer `Deny(Busy)`.
A requester that receives `Busy` skips that responder for the rest of the walk and does not mark it failed.
On a primary the requester probes it after the fresh peers on later walks of that epoch; a `Busy` that arrives more than 1 second after the request write (`SYNC_BUSY_DENY_WINDOW`) puts the responder with the peers marked failed for that epoch instead, still without a failure mark.

## Frame tags

Every frame is a self-contained encoded unit, and the enum discriminant is the frame tag.

```text
tag   frame           payload
----  --------------  -------------------------------------------
 0    Req(request)    the typed request for this role
 1    Ack             none
 2    Deny(reason)    0 = AtCapacity, 1 = Unavailable, 2 = Busy
 3    Data(bytes)     an opaque chunk of response payload
 4    End             none
 5    Err(reason)     0 = Internal, 1 = Malformed
```

`Deny(Busy)` means the per-peer serve cap or a per-key hold budget refused the request; retry later.
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

A responder admits a bounded number of concurrent sync streams: each serve class has its own slot pool with a separate reserve for each committee key, and one peer can hold at most two in-flight sync streams (see [Admission](#admission)).
Past that it does not drop the stream.
It writes a `Deny` and closes.
A full pool answers `Deny(AtCapacity)`.
On a primary, a refusal by the per-peer cap or by a per-key hold budget answers `Deny(Busy)`: the responder has room, but not for this key now.
A requester that receives `Busy` skips that responder for the rest of the walk at debug level, and does not mark the responder failed or forget it as a sync server.
On a primary it probes that responder after every fresh peer on the later walks of the same epoch, and, when the `Busy` came more than 1 second after the request write, among the peers marked failed for that epoch, so a peer cannot hold every probe for most of the 5-second ack timeout and still sort ahead of them.
The 1-second window starts at the requester's own write, so it also covers the round trip and any queueing on the connection; an honest `Busy` that arrives late costs its server only that place in the probe order.

That distinction matters to a requester's wall-clock cost.
A dropped stream costs the requester its full 5-second ack timeout before it can try elsewhere; an explicit `Deny` costs it the open, the request write and one round trip.
A node catching up against a busy committee therefore keeps moving instead of serializing on timeouts, and a busy responder sheds load without being scored for it.

`Deny(Unavailable)` is the other normal case: the peer speaks the protocol and has capacity but does not hold the requested data.

The shed path is itself bounded.
Writing a `Deny` costs a short-lived task, and at most eight of those run at once; beyond that budget the stream is dropped without a `Deny`, and the requester sees a reset and retries elsewhere.

## Admission

A responder admits an inbound sync stream in stages, so a peer that opens streams and stays silent cannot use the slots that serve real requests.

Each peer is in one of two tiers.
A peer whose BLS key is in the current committee or the next committee is in the committee tier; every other peer is in the public tier.
The committee keys are captured when the network is built for an epoch.
The hold books are not: the opening and serve hold books are built once per node process and shared by every epoch's network, so an epoch boundary or an epoch re-run does not refill a key's budget; a restart does.

1. **Per-peer cap.** One peer can hold at most two in-flight sync streams. A third stream is shed; on a primary it is answered `Deny(Busy)` where the shed path writes a frame.
2. **Opening read.** The stream takes an opening-read slot while the responder reads the request frame, for at most 1 second for a public peer and 5 seconds for a committee peer. A public peer whose valid request arrives in the second half of its 1-second window is charged a mild penalty, whether the request is then admitted or refused. There are 8 slots for any peer and 1 more for each committee key, which only that key can take. The opening-read slot is released as soon as the read ends, before any control write or close; the per-peer slot lasts for the whole task. A stream that never sends a valid request never takes a serve slot. The request frame travels with the protocol proposal, so an honest opening holds its opening-read slot for about the responder's read time, not a round trip. A public key may hold opening-read slots for 10 seconds per 300-second window (`PUBLIC_OPENING_HOLD_BUDGET`, `PUBLIC_OPENING_HOLD_WINDOW`), so the budget meters slow openers only. A key with less than one 1-second read bound of that budget left is shed without a charge and answered `Deny(Busy)`. A full opening pool answers `Deny(AtCapacity)`.
3. **Serve.** The request selects its serve class: epoch pack, missing certificates, or consensus output. Each class has its own pool of 5 slots for any peer and 1 more for each committee key, which only that key can take. So public peers that fill one class cannot block a committee peer or a different class. A full class answers `Deny(AtCapacity)`. A public key holds at most one serve slot at a time across all three classes (`PUBLIC_SYNC_SERVES_PER_PEER`). In each class a public key may hold serve slots for one public hold limit per window: 200 seconds per 1000-second window for an epoch pack (`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`, `PUBLIC_EPOCH_PACK_SERVE_HOLD_WINDOW`), and 60 seconds per 300-second window for missing certificates and for consensus output (`PUBLIC_SYNC_SERVE_TIMEOUT`, `PUBLIC_SYNC_SERVE_HOLD_WINDOW`). A key is leased only with a full hold limit of budget left. A second serve, or a serve outside the budget, is refused `Deny(Busy)` without a charge. A committee key takes no serve cap and no hold budget.

The worker batch sync stream uses the same tiers, with one pool of 5 slots for any peer and 1 more for each committee key.

Each committee key has its own reserve in each pool, and no committee key can take the reserve of another.
So committee keys that hold streams without progress cannot stop a different committee key from catching up.
The per-peer cap of two lets a committee key open its second stream from the slots for any peer.

These budgets raise the cost of filling the public share; they do not close it while keys cost nothing.
In each serve class the hold equals the budget, so a hold of H is followed by 5H of refusal, and a public key holds the class at most 1/6 of the time.
So about 30 public keys per class keep the 5 public serve slots of that class busy on one primary.
The same 30 keys cover all three classes, because each key is then busy 1/6 of the time in each class and 1/2 in total.
The opening pool takes about 240 keys in steady state and about 120 in a book's first 300-second window.
The books belong to each responder, so the same keys do this on every primary.

Each serve is also bounded in time:

- An epoch-pack serve has a 10-second write timeout per frame, a throughput floor of 1 MiB per 30-second window, and a total deadline of 30 seconds for each started MiB of pack plus 30 seconds. A requester that reads at the floor finishes inside the deadline; a 100 MiB pack gets 50.5 minutes. That floor-derived deadline is the only time bound for a committee-tier serve. A public-tier epoch-pack serve is also cut by its own fixed, size-independent bound, `PUBLIC_EPOCH_PACK_SERVE_TIMEOUT` (200 seconds), and a serve that outlives it is charged as a stall. That bound is not scaled by pack length: it is the unit the per-key hold budget leases against, and the deadline above already scales with the pack. A public requester whose pack cannot finish inside 200 seconds at its read rate is cut and charged by every responder. So a public requester must read a pack of L bytes at no less than L/200 seconds on each stream. The requester reads a fresh peer at a target of 8 MiB per 30-second window (`SYNC_TARGET_MIN_BYTES`) and a peer already de-prioritised this epoch at the 1 MiB floor, both prorated over the real window length and checked on the `End` frame too. Only a serve that completes at the target rate (plus a 1-second latency allowance) makes a peer the preferred sync server; a slower completed serve de-prioritises the peer for the epoch instead. The target is about 2.24 Mbit/s for each stream, so the three epoch fetchers together need about 6.7 Mbit/s. A public key holds at most one serve per responder, and each class at most 1/6 of the time, so across N responders it gets about N/6 concurrent epoch-pack serves and needs about 18 responders to fetch three packs in parallel. When a committee-tier requester (its own key in the current or next committee) cuts a fresh peer at the target rate, it probes that peer again at the floor in the same walk, with no sleep; a timeout or any other failure does not cause this second probe. A public-tier requester skips that same-walk re-probe, because the cut peer refuses it `Busy` for about five times its hold. It keeps the (peer, epoch) slow mark and reads that peer at the floor in a later walk. The slow marks are keyed by peer and epoch, so a walk on another epoch never clears them. The preference is dropped at epoch rotation, and every 16th walk on one handle (`SYNC_PREFERENCE_EXPLORE_EVERY`) ignores it, so another peer that serves at the target rate can take the slot. A peer-supplied record whose stored length is more than the zstd worst-case compressed size for its decompressed content is refused as undecodable, so a record of decompressed size n is credited to the rate meter at most about n + n/256 + 127 bytes; the bound does not refuse padding that fits under it, and bytes after the last decoded record are still credited.
- A missing-certificates or consensus-output serve is bounded at 200 seconds in total. A public-tier serve of either class is also cut at a fixed 60-second hold limit (`PUBLIC_SYNC_SERVE_TIMEOUT`) and charged as a stall. So a public requester must read a consensus output of L bytes at no less than L/60 seconds. Pack records are capped at 16 MiB, so the largest output needs about 273 KiB/s, the requester's own target rate.
- A worker batch serve has 5 seconds to write its `Ack` and 30 seconds for each data frame, inside the 200-second total. A public-tier worker batch serve is also cut at a fixed 60-second hold limit (`PUBLIC_BATCH_SERVE_TIMEOUT`) and charged as a stall; a public key serves on one worker stream at a time and may hold worker serve permits for 60 seconds per 300-second window (`PUBLIC_BATCH_SERVE_HOLD_BUDGET`, `PUBLIC_BATCH_SERVE_HOLD_WINDOW`), and is refused `AtCapacity` outside that budget. A public key may hold worker opening-read permits for 10 seconds per 300-second window (`PUBLIC_OPENING_HOLD_BUDGET`, `PUBLIC_OPENING_HOLD_WINDOW`) and is shed `AtCapacity`, uncharged, outside it; a key is leased only with a full 60-second hold limit of serve budget left.

A public-tier peer is charged a mild penalty for a silent, unreadable or unexpected opening, for a valid request that arrives in the second half of its 1-second opening window (whether the request is then admitted or refused), for a stalled serve, and, for an epoch pack, for a serve cut at the 200-second hold limit, and, for a missing-certificates or consensus-output serve, for a serve cut at the 60-second hold limit, and, for a worker batch serve, for a serve cut at the 60-second hold limit and for a request above the 500-digest cap.
A serve stalls when it hits a write timeout or a missed deadline.
On the primary, the whole-serve timeout of a missing-certificates or consensus-output serve is a stall.
On the primary, a requester that resets or closes the stream during the serve is also a stall: a `TimedOut`, `ConnectionReset` or `NotConnected` stream error counts as a stall, the same set the worker uses.
On the worker, a requester that stops or closes the stream after the `Ack` is a stall.
A prompt `AtCapacity` or `Busy` refusal is not charged, because an honest peer gets it when a pool is full or its own budget is spent.
Committee peers are not charged by admission.
A mild penalty lowers the peer score by 1.0; the score thresholds are -20.0 for a disconnect and -50.0 for a ban, so, ignoring decay, twenty stalls disconnect a public peer and fifty ban it.
These admission penalties are enforced, unlike the stream-failure penalties under [Failure classification](#failure-classification).

## Limits

| Limit | Value | What it bounds |
|-------|-------|----------------|
| Pending outbound opens | 1024 | Opens buffered across the whole behaviour; past this an open is rejected immediately rather than queued |
| Total open budget | 15s | Dial, connection establishment, and substream negotiation combined; on expiry the caller gets a timeout |
| Substream negotiation | 10s | A single outbound substream negotiation, enforced by libp2p |
| Stale sweep | 1s | How often expired pending opens are failed and cleared |
| Inbound streams per peer | 256 per second | Inbound sync streams accepted from one peer per one-second window; the next one is refused |
| Pending outbound per connection | 256 | Opens buffered by one connection handler before it sheds |
| In-flight sync streams per peer | 2 | Sync streams one peer can hold on a responder at once |
| Opening reads | 8 + 1 per committee key | Streams waiting up to 1s (public) or 5s (committee) for their request frame |
| Requester opening write (primary) | 1s | Deadline for writing and flushing the opening request frame (`SYNC_REQUEST_WRITE_TIMEOUT`) |
| Serve slots per class | 5 + 1 per committee key | Concurrent serves of each class: epoch pack, missing certificates, consensus output |
| Worker batch serves | 5 + 1 per committee key | Concurrent worker batch sync serves |
| Epoch-pack serve deadline | 30s per started MiB, plus 30s | Total time one epoch-pack serve can take |
| Public epoch-pack serve hold limit | 200s | Fixed, size-independent bound on one public-tier epoch-pack serve (`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`); a serve that outlives it is dropped and charged as a stall; committee-tier serves keep the throughput-floor deadline |
| Public worker batch serve hold limit | 60s | Fixed, size-independent bound on one public-tier worker batch serve (`PUBLIC_BATCH_SERVE_TIMEOUT`); a serve that outlives it is dropped and charged as a stall; committee-tier serves keep the 200s stream bound |
| Public worker batch serves per key | 1 | Worker batch serves one public key may hold at a time (`PUBLIC_BATCH_SERVES_PER_PEER`); a second request is refused `AtCapacity` without a charge |
| Public worker batch-serve hold budget | 60s per 300s | Serve-permit hold time one public key may use per window (`PUBLIC_BATCH_SERVE_HOLD_BUDGET`, `PUBLIC_BATCH_SERVE_HOLD_WINDOW`); a key is leased only with a full 60s hold limit left; outside that it is refused `AtCapacity` without a charge |
| Public worker opening hold budget | 10s per 300s | Opening-read hold time one public key may use per window (`PUBLIC_OPENING_HOLD_BUDGET`, `PUBLIC_OPENING_HOLD_WINDOW`); a key with less than one read bound left is shed `AtCapacity` without a charge |
| Public primary opening hold budget | 10s per 300s | Opening-read hold time one public key may use per window on a primary (`PUBLIC_OPENING_HOLD_BUDGET`, `PUBLIC_OPENING_HOLD_WINDOW`); a key with less than one read bound left is shed `Busy` without a charge |
| Public primary serves per key | 1 | Sync serves one public key may hold on a primary at a time, across all three classes (`PUBLIC_SYNC_SERVES_PER_PEER`); a second request is refused `Busy` without a charge |
| Public missing-certificates and consensus-output serve hold limit | 60s | Fixed bound on one public-tier serve of either class (`PUBLIC_SYNC_SERVE_TIMEOUT`); a serve that outlives it is dropped and charged as a stall; committee-tier serves keep the 200s total bound |
| Public epoch-pack serve hold budget | 200s per 1000s | Epoch-pack serve-permit hold time one public key may use per window (`PUBLIC_EPOCH_PACK_SERVE_TIMEOUT`, `PUBLIC_EPOCH_PACK_SERVE_HOLD_WINDOW`); a key is leased only with a full 200s hold limit left; outside that it is refused `Busy` without a charge |
| Public missing-certificates and consensus-output serve hold budget | 60s per 300s, per class | Serve-permit hold time one public key may use per window in each of the two classes (`PUBLIC_SYNC_SERVE_TIMEOUT`, `PUBLIC_SYNC_SERVE_HOLD_WINDOW`); a key is leased only with a full 60s hold limit left; outside that it is refused `Busy` without a charge |

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
Because the outbound open is lazy, the open succeeds before the responder answers the protocol proposal, so the "speaks sync" signal comes from the first read instead.
A first read that fails because the responder rejected the protocol is classed `UnsupportedProtocol` and records that the peer does not speak sync.
A peer is recorded as speaking sync only after a first frame is read from it.

**These stream-failure penalties are classified and logged, not enforced.**
Every classified failure produces a log line recording the penalty it would incur, and nothing is applied to the peer's score.
Enforcement is gated on telemetry confirming the classification does not fire on healthy peers.
The admission penalties in [Admission](#admission) are different: they are applied to the peer's score.
Apart from those admission penalties, nothing on this page changes a peer's score; see [Peers](peers.md) for the scoring that is enforced.

## Source of truth

| Behavior | Code |
|----------|------|
| Frame tags, deny and error reasons, frame read/write | `crates/network-libp2p/src/sync/frame.rs` |
| Typed request enums and their pinned discriminants | `crates/network-libp2p/src/sync/request.rs` |
| Open budget, stale sweep, inbound rate limit, pending-open cap | `crates/network-libp2p/src/stream/behavior.rs` |
| Negotiation timeout, per-connection pending cap, error classification | `crates/network-libp2p/src/stream/handler.rs` |
| Advertised upgrade, failure taxonomy and penalty mapping | `crates/network-libp2p/src/stream/upgrade.rs` |
| Peer tiers, tiered slot pools, per-peer stream slots | `crates/network-libp2p/src/sync/admission.rs` |
| Primary sync admission, serve classes, admission penalties | `crates/consensus/primary/src/network/mod.rs` |
| Penalty reporting (metrics-only, not enforced) | `crates/network-libp2p/src/consensus.rs` (`process_stream_event`) |
| Sync protocol IDs | `crates/network-libp2p/src/types.rs` (`sync_protocol`) |
| Shared codec bounds | `crates/network-libp2p/src/codec.rs` |
| Per-exchange frame and transfer caps | `crates/consensus/primary/src/network/sync_codec.rs`, `crates/consensus/worker/src/network/handle.rs` |

This page mirrors those files.
Update this page when those files change.
