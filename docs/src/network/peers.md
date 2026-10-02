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

Penalties have four severities and an explicit cause. `Mild`, `Medium`, `Severe`, and `Fatal` report attributable protocol or validation failures. `LoadMild`, `LoadMedium`, and `LoadSevere` report transient load with the same score changes for ordinary peers.

| Severity | Score change | Examples |
|----------|--------------|----------|
| Mild | -1.0 | Invalid requests; `LoadMild` for timeouts and slow gossip consumers. |
| Medium | -5.0 | Malformed responses; `LoadMedium` for inbound stream and provider rate limits. |
| Severe | -10.0 | Invalid validation data; `LoadSevere` for Kademlia put-record flooding. |
| Fatal | Set to -100.0 | Invalid signatures, invalid encoding, and authenticated protocol violations. |

Operator-provisioned hubs and tracked committee members ignore load penalties. Protocol penalties apply to every peer. Rate-limited work is still dropped for privileged peers, so load exemption never creates an unlimited service allowance.

The score thresholds remain -20.0 for disconnection and -50.0 for a ban. From a fresh score of 0.0, ignoring decay, 50 mild, 10 medium, or 5 severe penalties cause a ban. Each report is evaluated immediately.

Crossing the ban threshold delays score decay for 30 minutes. Epoch rotation, identity discovery, and repeated hub installation preserve attributable protocol penalties and their bans. Committee admission may still forgive load-only penalties.

## What makes a peer important

A peer's identity is either `Confirmed`, carrying its BLS public key, or `Unidentified`, carrying its libp2p PeerId. Transport authentication proves the remote network key, and signed node records prove their advertised BLS binding. Configuration supplies expected identities and address hints; it does not replace either verification.

Operator allowlisting remains sticky across epoch rotation. Validator membership derives from the previous, current, and next committee sets. Both grant connection-retention privileges and exemption from load-induced penalties, independently of permission to publish committee-only gossip. Neither grants exemption from protocol penalties. Finite connection, stream, message, and rate budgets continue to apply.

Bootstrap entries supply discovery hints. They do not acquire operator retention or load privileges automatically. A compatible trusted entry takes precedence for persistent dial addresses. Conflicting BLS/PeerId bindings fail startup with the offending configuration field.

### Trusted hub configuration

Add `trusted_nodes` to the network configuration, keyed by each hub's BLS public key. Replace the key placeholders with the corresponding values from that hub's node information. This example is for a node configured with worker IDs 0 and 1:

```yaml
trusted_nodes:
  "<hub BLS public key>":
    primary:
      network_key: "<hub primary network key>"
      network_address: /ip4/203.0.113.10/udp/9000/quic-v1
    workers:
      0:
        network_key: "<hub worker 0 network key>"
        network_address: /ip4/203.0.113.10/udp/9001/quic-v1
      1:
        network_key: "<hub worker 1 network key>"
        network_address: /ip4/203.0.113.10/udp/9002/quic-v1
```

Every locally configured worker ID must be present, including workers provisioned ahead of activation. Missing or out-of-range IDs, contradictory BLS/PeerId assignments, and mismatched `/p2p` address suffixes reject startup before any swarm is spawned. An absent or empty `trusted_nodes` map preserves existing configurations. The primary uses only `primary`; worker `k` uses only `workers[k]`.

Registration succeeds even when a hub is offline. The first dial is immediate, followed by delays of 1, 2, 4, 8, 16, 32, and at most 60 seconds during an outage. A successful connection resets the delay. Dial attempts continue across epoch transitions and while unrelated peers satisfy the discovery population target. An in-flight dial is never duplicated.

Each swarm owns one shared retry timer and one schedule per configured hub. Dropping the network task cancels all its retries. Repeated registration preserves connection state and protocol bans. Learned records cannot replace configured identities; changing a hub's network key requires updating the configuration and restarting the node.

When a connection becomes available, missing signed records for configured peers are queried through Kademlia. Unresolved records are retried on the existing peer-manager heartbeat, with at most one lookup in flight per key. This also allows a replacement peer-exchange connection to confirm a hub whose first record push was interrupted. Configuration alone never authorizes its gossip.

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

Each swarm also caps pending inbound connections (handshakes that are not yet established) at **1024**.
This cap is a memory bound only, not an admission policy.
QUIC Retry, enabled by default, validates the source address before a handshake can occupy a slot.
If `retry_unvalidated_incoming` is disabled as an operator rollback, a forged QUIC Initial datagram can hold a slot for the 10 second transport timeout, so a small cap would let a cheap flood refuse honest peers.
Refusals show in `inbound_connections_denied_total`.

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
| `inbound_connections_denied_total` | Inbound connections refused by a connection limit, labelled `reason` with `pending_incoming_limit` or `established_per_peer_limit`. A sustained `pending_incoming_limit` rate means the pending ceiling is full. |
| `external_addr_confirmed` | `1` once any external address is confirmed, meaning NAT traversal is possible. Stuck at `0` means inbound connectivity is unlikely. |
| `gossip_published_total` | Gossip messages published by this node. |
| `gossip_received_total` | Gossip messages received from peers. |
| `gossip_rejected_total` | Gossip messages that failed publisher verification. Sustained non-zero means a peer is publishing to a topic it is not authorized for. |
| `add_provider_rate_limited_total` | Inbound DHT provider announcements dropped by the per-source rate limit. |
| `outbound_requests_pending` | Outbound requests in flight. |
| `outbound_request_failures_total` | Outbound request failures, labelled `kind` with the failure cause. |
| `px_disconnects_pending` | Graceful peer-exchange disconnects awaiting the peer's acknowledgement. |


## Reloading trusted and bootstrap connectivity

On Unix, edit the existing YAML file `<datadir>/network-config`, then send `SIGHUP` to the node PID:

```sh
kill -HUP <node-pid>
```

Use the same OS account that owns the node and its data directory, or an authorized system administrator. File permissions and Unix signal permissions define the operator interface. Write a complete sibling file, preserve its owner and permissions, and atomically rename it over `network-config` before signaling. Reload only changes `trusted_nodes` and `bootstrap_peers`. Other network settings, the supported worker IDs, and admission rollout mode require a restart.

The entire file is decoded and every primary/worker identity binding is validated before publication. A CLI bootstrap override keeps precedence for the process lifetime. An empty bootstrap map selects the original genesis fallback, so it does not revoke genesis bootstrap peers. To replace that fallback, provide a nonempty map. Trusted and bootstrap entries may share a BLS key only with consistent transport identities; trusted hints take precedence for reconnects when both own the same identity.

One immutable revision is published to the primary and every configured worker, including workers awaiting activation. Each swarm replaces its admission grants, reconnect schedules, retained hints and gossip treatment together between swarm polls. Application is asynchronous across swarms; no swarm installs a partial endpoint map. Committee epochs have independent revisions and do not replace the operator snapshot.

Removing an entry cancels its configuration-owned future retries and queued dials, removes its policy pin, and strips its former retention, mesh and load-scoring privileges. Separate explicit grants and previous/current/next committee membership survive. A remaining bootstrap grant still supplies admission and retained record hints, with ordinary retention and load scoring. In valid Closed mode, live connections with no remaining grant are disconnected without adding a reputation penalty. Open and Grace keep those connections as ordinary peers. In-flight dials are checked at establishment against the applied policy. Protocol evidence and bans survive transport replacements; configuration does not authenticate BLS records or authorize committee gossip.

A missing, malformed, oversized or contradictory file publishes a rejected attempt while retaining the exact last accepted snapshot. Closed admission falls back to Grace on affected swarms until a valid reload clears the operator fault. Committee renewals cannot clear that fault; their own missing, stale or contradictory inputs retain the existing Open/Grace fallback rules. Signatures and finite connection, stream, message and memory budgets remain active in every mode.

Reload reads at most 1 MiB plus one overflow byte. The union of configured BLS entries cannot exceed startup `peer_config.target_num_peers`, using the same validation at startup and reload. Signals are processed serially and coalesced, watch retains only the latest publication, and each swarm shares one retry timer with at most one schedule per configured peer. Unchanged endpoints retain backoff. Shutdown stops the reader and drops retry state with the swarms.

Observe `tn_node.peer_policy_reload_total`, labeled only by finite `outcome` and `reason` values, and the per-network `tn_network.peer_policy_updates_total`, `peer_policy_attempt` and `peer_policy_revision` metrics. Revisions are gauge values, never label values. `admission_fallback` value `5` identifies rejected operator input. Reload logs contain attempt numbers, accepted revisions and rejection categories without keys or file contents.


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
