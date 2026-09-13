# Peers

Every Telcoin Network node keeps a reputation score for every peer it has met.
Malformed responses, failed validation, protocol abuse, and DHT flooding all report a penalty into that one score, and a peer whose score falls far enough is disconnected and then banned.
Reputation is the only defense that touches every subsystem, so it is also the answer to "why did that node stop talking to me".

This page is for RPC providers, dapp and indexer operators, bridge partners, and validators.
It documents the score model, the penalty magnitudes, the connection ceilings a node enforces, the two separate ban tables, how IP bans are derived, and the metrics that expose all of it.

The implementation lives in
[`crates/network-libp2p/src/peers/manager.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/peers/manager.rs),
[`crates/network-libp2p/src/peers/score.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/peers/score.rs), and
[`crates/network-libp2p/src/peers/all_peers.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/peers/all_peers.rs),
with every default in
[`crates/config/src/network.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/config/src/network.rs).
[Gossip](gossip.md), [Discovery](discovery.md), [Request-Response](request-response.md), and [Sync Streams](sync-streams.md) describe the subsystems that report the penalties.

## The score model

A peer starts at `0.0` and is clamped to the range `[-100.0, 100.0]`.
The score decays exponentially toward zero with a half-life of 300 seconds:

```text
decay_factor = e^(-ln(2) / 300 * seconds_since_last_update)
new_score    = old_score * decay_factor
```

Decay applies equally to positive and negative scores, so a peer that behaves for ten minutes has shed roughly three quarters of any penalty it accumulated.
A peer's reputation is derived from its current score every time it is read, never stored, so an operator who changes the thresholds changes behavior immediately.

## Penalties and thresholds

Four penalty severities exist, and the number of variants is kept deliberately small:

| Penalty  | Score change              | Meaning |
|----------|---------------------------|---------|
| `Mild`   | −1.0                      | An error that is very unlikely to be malicious. |
| `Medium` | −5.0                      | An error that is probably not malicious but is not free. |
| `Severe` | −10.0                     | Not necessarily malicious, but will not be tolerated. |
| `Fatal`  | set to −100.0 (the floor) | Unforgivable; bans on the first occurrence. |

Two thresholds act on the resulting score, and both trigger at or below their value:

- **−20.0 — disconnect.** The node closes the connection.
- **−50.0 — ban.** The node closes the connection, blacklists the peer in gossipsub, removes it from the Kademlia routing table, and refuses future connections.

From a fresh score of `0.0`, ignoring decay, that is 50 `Mild`, 10 `Medium`, or 5 `Severe` penalties before a ban.
Penalties are not debounced: each report is evaluated immediately and any one of them can be the report that crosses a threshold.

> [!WARNING]
> Crossing the ban threshold pushes the peer's decay clock forward by 30 minutes.
> A banned score does not decay during that window, so the lockout is real time served rather than a formality — the score only begins recovering after the 30 minutes elapse, and the peer becomes reconnectable once decay lifts it back above −50.0.

## What makes a peer important

The peer manager has no validator/observer enum.
It has an identity and two independent bases for trust.

An identity is either `Confirmed`, carrying the peer's BLS public key, or `Unidentified`, carrying only its libp2p peer id.
A peer first seen as unidentified is re-keyed onto its confirmed identity the moment its BLS key is learned.

The two trust bases are kept separate on purpose:

- **Operator allowlisting** is sticky.
  It is set when the peer record is constructed, from the node's own trusted and bootstrap configuration, and epoch rotation never alters it.
- **Validator membership** is derived live from the previous, current, and next committee slots.
  It is never stored on the peer record, so it cannot drift out of sync with rotation, and a validator rotating out of the committee can never strip operator trust.

Either basis makes a peer "important", which means four things: it is exempt from the score model entirely, it is skipped by heartbeat pruning, it is allowed to connect past the connection ceiling, and it is added as a gossipsub explicit peer when it connects.
A `Severe` or `Fatal` penalty suppressed for an important peer is logged as a warning, because an exempt peer misbehaving badly enough to earn one is operationally significant.

When a peer enters a tracked committee its score is primed to the maximum of `100.0`, any ban is forgiven, and its observed IPs are cleared from the per-IP ban counter.
Priming the score means that if it later rotates out and re-enters the score model, it starts from a clean maximum rather than a stale value.

## Connection limits

Every limit derives from one target, which itself derives from Kademlia's bucket size `K_VALUE` of 20:

```text
target_num_peers        = (K_VALUE / 2) + K_VALUE     = (20 / 2) + 20    = 30
max_peers               = ceil(30 * (1 + 0.3))                           = 39
max_outbound_dialing    = ceil(30 * (1 + 0.3 + 0.2 / 2))                 = 42
max_priority_peers      = ceil(30 * (1 + 0.3 + 0.2))                     = 45
target_outbound_peers   = ceil(30 * 0.3)                                 =  9
min_outbound_only_peers = ceil(30 * 0.2)                                 =  6
max_discovery_peers     = 30 * 2                                         = 60
```

Three of these are enforced on the connection path today.
An inbound connection is refused once connected-or-dialing peers reach 39.
An outbound connection is refused once connected peers reach 42, the higher ceiling reflecting that this node chose to dial.
The discovery candidate pool is trimmed to 60.
An important peer bypasses both connection ceilings.
The remaining three values — 45 priority peers, 9 target outbound, and 6 minimum outbound-only — are configured and resolvable but are not read by any production code path at present.

A separate per-peer ceiling caps concurrent established connections from a single peer at **8**.
A peer needs at most one inbound and one outbound connection at a time, so eight is headroom for reconnection churn while bounding a hostile peer to a fixed, small fan-out.

Dial attempts time out after 15 seconds; a peer still dialing past that is marked disconnected, because dialing peers count against the inbound limit.

## The heartbeat

The heartbeat runs every 30 seconds and does all periodic maintenance.
It times out stale dials, decays every non-exempt peer's score, unbans peers whose decayed score has recovered, updates the peer-count gauges, releases expired DHT rate-limit budgets, and refills the discovery pool.
It can never penalize a peer: penalties only arrive from the application layer.

The heartbeat then prunes back to the target of 30 connected peers.
Candidates are shuffled first, then sorted by score and then by Kademlia routability, so the lowest-scoring peers that do not participate in routing are dropped first and equal scores are broken without bias.
Important peers are filtered out of the candidate list entirely.
Pruned peers are disconnected with a peer-exchange payload so they can find other nodes, and are temporarily banned so they do not immediately reconnect.

## Two ban tables

A node maintains two ban tables that never merge, because they answer different questions.

**Reputation bans** live with the peer record.
They are earned by score, they carry the peer's observed IP addresses into the per-IP ban counter, they blacklist the peer in gossipsub, and they end only when decay lifts the score back above the ban threshold.
At most 100 reputation bans are retained; when that fills, the oldest are pruned and their peers unbanned, keeping the freshest bans.

**Temporary bans** live in a separate LRU cache keyed by peer id, with a 600-second timeout and a cap of 100 entries.
They are applied for excess-peer disconnects, for reputation disconnects, and alongside reputation bans, and they prevent reconnection at the swarm level without touching the peer's stored state.
They also outlive the peer record, so a peer pruned from the database can still be refused on the basis of its earlier temporary ban.
Because the cache is only swept on the heartbeat, the effective ban duration is quantized to the 30-second interval.

Keeping them separate is what lets a peer be refused a connection while its stored reputation is still healthy — an over-capacity node turning peers away is not accusing them of anything.
Committee members are lifted out of the temporary-ban cache on every rotation so a follow-up dial can reach them.

## IP bans

An IP address is banned once **more than one** banned peer is associated with it.

The per-IP counter is fed **only** by IPs observed on real inbound and outbound connections.
Self-advertised addresses from signed peer records are never counted, and this is load-bearing: an advertised address is attacker-controlled, so counting it would let a peer get a third party's IP banned by simply claiming it.
The two address sets are deliberately independent — the advertised multiaddr set is capped at 1 entry per peer and drives dialing and peer exchange, while the observed-IP set drives ban accounting and nothing else.

The observed set is capped at **16** IPs per peer, enough for dual-stack, DHCP renewal, and mobile roaming.
At the cap a new IP is refused rather than evicting an old one.
That keeps the set growing monotonically, which matters because per-IP counts are incremented from the set when a ban starts and decremented from it when the ban ends: an eviction between those two reads would strand a count and leave an IP banned after its peer was unbanned, penalizing every honest peer sharing it.

Committee members have their observed IPs removed from the counter when they join, so a validator's address cannot be blocked during its initial connection storm.

## Layered refusal

The peer manager is the first behaviour in the node's libp2p behaviour list, and the per-peer connection limiter is second.
Behaviour callbacks run in declaration order and short-circuit on the first denial, so a banned peer is rejected before gossipsub, request-response, or Kademlia register any per-peer state for it, and an over-cap connection is denied before those behaviours allocate anything either.

Refusal happens at several points along the same connection: on the pending outbound dial, on the pending inbound connection whose source IP is checked against the IP ban list, and again on each established connection.
As a final backstop, a banned peer that nonetheless reaches the `PeerConnected` event is refused registration and disconnected outright, rather than being added to the routing table where it would drive a redial loop.

## Why committee records never expire

The map that resolves a validator's BLS public key to its network address has no TTL, by design.
An expiry-driven resolution failure would be a consensus liveness bug: a validator whose record aged out mid-epoch would simply stop being recognized by its peers.

The map is bounded structurally instead of by time.
An entry survives only if it is pinned — an operator-provisioned trusted, bootstrap, or explicit peer, whose count is set by node configuration — or if its key still sits in one of the three tracked committee slots.
Every committee rotation prunes the rest.
Records restored from local persistence at startup are deliberately not pinned, and records learned from the DHT are cached only for keys already in a tracked committee slot, so neither path can grow the map without bound.

## Metrics

Every series below is exported under the `tn_network` prefix and carries a `network` label whose value is `primary` or `worker-{worker_id}`, so read each series per label — the primary and worker swarms are independent.

| Metric | What it tells you |
|--------|-------------------|
| `connected_peers` | Peers currently connected, sampled each heartbeat. Compare against the target of 30 and the ceiling of 39. |
| `known_peers` | Committee members with a resolved network record. A value below the committee size means discovery is still chasing keys. |
| `discovery_peers` | Dial candidates held in the discovery pool, capped at 60. Persistently low means discovery is starved. |
| `banned_peers` | Size of the temporary-ban cache, not the reputation-ban table. Rises during excess-peer churn. |
| `peers_banned_total` | Cumulative reputation bans. This is the flow that matches the ban threshold; `banned_peers` is a different stock. |
| `peer_penalties_total` | Penalties applied, labelled `severity` with `mild`, `medium`, `severe`, or `fatal`. The leading indicator for bans. |
| `connections_established_total` | Connections established, labelled `direction` with `in` or `out`. |
| `connections_closed_total` | Connections closed, all directions. |
| `dial_failures_total` | Failed dial attempts. |
| `external_addr_confirmed` | `1` once any external address is confirmed, meaning NAT traversal is possible. Stuck at `0` means inbound connectivity is unlikely. |
| `gossip_published_total` | Gossip messages published by this node. |
| `gossip_received_total` | Gossip messages received from peers. |
| `gossip_rejected_total` | Gossip messages that failed publisher verification. Sustained non-zero means a peer is publishing to a topic it is not authorized for. |
| `add_provider_rate_limited_total` | Inbound DHT provider announcements dropped by the per-source rate limit. |
| `outbound_requests_pending` | Outbound requests in flight. |
| `outbound_request_failures_total` | Outbound request failures, labelled `kind` with the failure cause. |
| `px_disconnects_pending` | Graceful peer-exchange disconnects awaiting the peer's acknowledgement. |

## Source of truth

| Behavior | Code |
|----------|------|
| Score decay, penalty magnitudes, thresholds | `crates/network-libp2p/src/peers/score.rs` |
| Penalty severities, trust basis, peer identity | `crates/network-libp2p/src/peers/types.rs` |
| Per-peer state, observed IPs, exemption | `crates/network-libp2p/src/peers/peer.rs` |
| Reputation bans, committee slots, pruning | `crates/network-libp2p/src/peers/all_peers.rs` |
| Heartbeat, connection limits, temporary bans, `known_peers` | `crates/network-libp2p/src/peers/manager.rs` |
| Per-IP ban counter and threshold | `crates/network-libp2p/src/peers/banned.rs` |
| Temporary-ban LRU cache | `crates/network-libp2p/src/peers/cache.rs` |
| Connection denial ordering | `crates/network-libp2p/src/peers/behavior.rs`, `crates/network-libp2p/src/consensus.rs` |
| Metric names and labels | `crates/network-libp2p/src/metrics.rs` |
| `PeerConfig` and `ScoreConfig` defaults | `crates/config/src/network.rs` |

This page mirrors those files.
Update this page when those files change.
