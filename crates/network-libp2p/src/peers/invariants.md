# PeerManager Invariants

## 0. Audience & how to read this doc

This document is the property catalog for the custom libp2p `PeerManager` behavior.

Each invariant block has exactly four lines:

- **Statement** — the property in declarative form.
- **Why** — what the property protects (correctness, DoS resistance, accounting consistency, etc.).
- **Enforced at** — `file.rs:line` references that implement or guard the property.
- **Violation impact** — what fails at the node or swarm level if the property is broken.

The module surface that exposes these guarantees outside `peers/` is declared in `mod.rs` (`PeerManager`, `PeerEvent`, `PeerExchangeMap`, `Penalty`); the canonical event and action types referenced throughout are defined in `types.rs` (`PeerEvent`, `PeerAction`, `Penalty`, `DialRequest`, `ConnectionDirection`, `ConnectionType`, `PeerExchangeMap`).

Cross-crate properties (e.g., gossipsub blacklist propagation, kad routing-table eviction) are documented as _boundary_ invariants in §10. The mechanisms that satisfy them live outside `peers/` and are listed in Appendix A as not enforced within `peers/`.

## 1. State model overview

The peer manager owns a `HashMap<PeerId, Peer>` keyed by libp2p `PeerId` (the field `AllPeers::peers` at `all_peers.rs:48`). Each `Peer` carries a `Score` (decaying float in `[min_score, max_score]`), a `ConnectionStatus` (the disjoint enum below), and bookkeeping for trust, routability, and observed multiaddrs. A secondary index `bls_index: HashMap<BlsPublicKey, PeerId>` (`all_peers.rs:53`) maps validator identity (BLS public key) to the corresponding network `PeerId`; its consistency with `Peer::bls_public_key` is the subject of §1a. A separate `BannedPeers` struct counts banned-peer occurrences per IP, and a separate `BannedPeerCache<PeerId>` enforces a _temporary_ ban that is independent of reputation.

```
                +----------------------+
                |       Unknown        |  ← default for new entries
                +----------+-----------+
                           |
              +------------v------------+
              |        Dialing          |  ← outbound attempt registered
              +----+------------+-------+
                   | success    | timeout / failure
              +----v----+       +-------v--------+
              |Connected|       |  Disconnected  |
              +----+----+       +-------+--------+
                   | reputation/peer-limit prune
              +----v---------------+
              |   Disconnecting    |  ← banned: bool tag inside variant
              +----+---------------+
                   | swarm ConnectionClosed
              +----v---+    Reputation drops below ban threshold
              | Banned |  ← also entered directly via Penalty::Fatal
              +--------+
```

The `Banned` connection state and the `Reputation::Banned` tier are _related but distinct_: `Reputation::Banned` is a property of the score; `ConnectionStatus::Banned` is a property of the connection lifecycle. The `temporarily_banned` cache adds a _third_ axis that is independent of both.

## 1a. BLS / PeerId binding

This section covers the `bls_index ↔ Peer::bls_public_key` correspondence and the orphan semantics that arise when validators rotate their libp2p network key (changing `PeerId` while keeping the same BLS key).

### INV-043 — bls_index is the inverse of Peer::bls_public_key

- **Statement:** `bls_index[bls] == pid` if and only if `peers[pid].bls_public_key == Some(bls)`, for every non-`None` BLS value. Forward and reverse mappings never diverge.
- **Why:** Consensus addresses validators by BLS key (via `PeerManager::auth_to_peer`) while libp2p addresses peers by `PeerId` (via `PeerManager::peer_to_bls`). A diverging pair would silently mis-route messages or misattribute behavior across validator identities.
- **Enforced at:** `all_peers.rs:6-16` (module-level invariant doc), `:107-122` (`rebind_bls`, the single mutation funnel), `:860-870` (`remove_peer`, the single eviction funnel), `peer.rs:163-165` (`clear_bls`); `pub(super)` visibility on `Peer::bls_public_key` (`peer.rs:26`) blocks direct mutation outside this module.
- **Violation impact:** `auth_to_peer(bls)` may return the wrong `PeerId` or `None` for a still-known validator; reputation penalties may attach to a stale `PeerId` while consensus reports the new one — silent data corruption that surfaces only as missing votes or unreachable peers.

### INV-044 — Rotation preserves the orphan Peer record

- **Statement:** When `rebind_bls(pid, bls)` rebinds `bls` from an existing `old_pid` to a different `pid`, the `Peer` record at `old_pid` is retained in `peers` with `bls_public_key = None`; all other fields (score, connection status, observed IPs, listening addrs) are preserved. The orphan is unreachable from the BLS index until its next `update_net` rebinds it.
- **Why:** Score and connection history must survive a network-key rotation so that a peer cannot launder accrued penalties by rotating its libp2p identity while reusing its BLS key. The retained state is also useful for in-flight connection accounting when the old endpoint has not yet disconnected.
- **Enforced at:** `all_peers.rs:115-121` (the orphan-clearing branch of `rebind_bls`), `peer.rs:158-165` (`clear_bls`); test coverage at `tests/peers.rs:675-693` (`peer_id_rotation_same_bls_orphans_old_peer`) and `tests/peers.rs:645-672` (`bls_rotation_same_peer_id_evicts_old_index`).
- **Violation impact:** Either dropping the orphan eagerly (losing score history → laundering channel) or leaving the BLS index pointing at the old `PeerId` (consensus loses the validator from `auth_to_peer`). The connected-orphan cleanup itself is lazy and not explicitly bounded — see Appendix A10.

## 2. Score & decay

### INV-001 — Aggregate score is bounded

- **Statement:** `Score::aggregate_score ∈ [min_score, max_score]` (default `[-100.0, 100.0]`) for every penalty path except `Penalty::Fatal`, which sets the score _exactly_ to `min_score`.
- **Why:** Keeps reputation comparisons monotone and prevents arithmetic overflow in clamp math; gives every other penalty a predictable distance from the ban threshold.
- **Enforced at:** `score.rs:84-100` (`apply_penalty`), `score.rs:103-106` (`add` clamps via `f64::clamp`).
- **Violation impact:** A score outside `[-100, 100]` would either bypass the ban threshold (peer cannot be banned) or land below it permanently (peer can never recover via decay).

### INV-002 — Penalties are a no-op on trusted peers

- **Statement:** `Peer::apply_penalty` skips `Score::apply_penalty` entirely when `is_trusted == true` and returns the peer's reputation unchanged.
- **Why:** Validators (committee members) and explicitly-configured trusted peers must never be banned by application-layer feedback, even adversarial feedback designed to evict them.
- **Enforced at:** `peer.rs:198-205`.
- **Violation impact:** A single malicious caller could ban a sitting committee member by repeatedly reporting it; consensus would lose votes from that validator until next epoch.

### INV-003 — Score never increases except via decay toward zero

- **Statement:** No public API on `Score` adds a positive delta; `apply_penalty` only adds negative deltas, and `update_at` multiplies by a decay factor in `(0, 1]`.
- **Why:** There is no "good behavior" credit channel — recovery is purely a function of _not_ misbehaving for long enough. Removing this discipline would allow a peer to actively earn score and pre-stage immunity to a future penalty.
- **Enforced at:** `score.rs:84-100` (`apply_penalty`), `score.rs:123-136` (`update_at` decay multiplication).
- **Violation impact:** A peer could build up positive headroom, then commit a `Penalty::Severe` action without triggering a ban.

### INV-004 — Banned peers have decay deferred for a fixed window

- **Statement:** When `update_score` observes a fresh transition into the `is_banned()` state, `last_updated` is moved forward by `banned_before_decay_secs` (default 12 hours), causing `update_at` to be a no-op for that window.
- **Why:** Without the deferral, a peer that just hit the ban threshold would begin decaying back toward zero on the next heartbeat tick, undoing the ban almost immediately.
- **Enforced at:** `score.rs:141-155` (`update_score` defers `last_updated` at `:153`), `score.rs:123-126` (`checked_duration_since` returns `None` for future timestamps).
- **Violation impact:** A peer that crosses the ban threshold could become unbanned within one heartbeat (default 30s), gutting the rate-limit on reconnection attempts.

### INV-005 — Trusted peers do not decay

- **Statement:** `Peer::heartbeat` short-circuits when `is_trusted == true`, leaving the score at its initialized `max_score` indefinitely.
- **Why:** Trusted peers do not need decay (they cannot accrue penalties anyway — see INV-002), and skipping their score update keeps heartbeat work proportional to non-trusted peers.
- **Enforced at:** `peer.rs:312-343` (`heartbeat`; the `if !self.is_trusted` guard at `:313`).
- **Violation impact:** None at the reputation level (penalties are already no-op), but committee membership-driven trust would silently drift over time if decay were applied with a downward bias.

### INV-006 — Decay is exponential with the configured half-life

- **Statement:** Each heartbeat applies `score *= exp(halflife_decay * dt)` where `halflife_decay = -ln(2) / score_halflife`; one decay step per peer per heartbeat tick.
- **Why:** Exponential decay gives a smooth, time-symmetric forgiveness curve — transient peers' penalties fade within a few half-lives rather than persisting indefinitely.
- **Enforced at:** `score.rs:123-136` (decay math at `:130-132`), `config/src/network.rs:443-446` (`halflife_decay` derivation, `score_halflife` knob).
- **Violation impact:** Linear decay would leave large penalties effectively permanent; faster decay would make `Penalty::Severe` too cheap.

### INV-007 — Score is per-PeerId, not per-connection

- **Statement:** A peer with multiple concurrent inbound/outbound subconnections has exactly one `Score` instance; closing one connection does not reset it.
- **Why:** Reputation must reflect the _identity_ misbehaving, not the connection slot. Otherwise a peer could rotate connections to launder penalties.
- **Enforced at:** `all_peers.rs:48` (`peers: HashMap<PeerId, Peer>`), `peer.rs:237-270` (counter increments on existing `Connected { num_in, num_out }` without replacing the `Peer`).
- **Violation impact:** A peer could open many connections, misbehave on one, drop it, and continue using the others without penalty.

## 3. Reputation tiers

### INV-008 — Reputation is a pure function of score and two thresholds

- **Statement:** `Peer::reputation()` reads only `score.aggregate_score()`, `config.min_score_for_ban`, and `config.min_score_for_disconnect`. No other state influences the tier.
- **Why:** A pure function makes reputation deterministic given the score, which is required for the heartbeat logic to predict reputation transitions and emit `ReputationUpdate` correctly.
- **Enforced at:** `peer.rs:176-184`.
- **Violation impact:** A tier that depends on connection state or time would create cycles between `apply_penalty` and the connection-state machine, producing nondeterministic ban/unban thrashing.

### INV-009 — Tier ordering is total and monotone in score

- **Statement:** `score ≤ min_score_for_ban ⇒ Banned`; `min_score_for_ban < score ≤ min_score_for_disconnect ⇒ Disconnected`; `score > min_score_for_disconnect ⇒ Trusted`. The match arms are non-overlapping and exhaustive.
- **Why:** Monotonicity lets `Peer::heartbeat` decide unban purely by comparing previous and current tier; a non-monotone mapping would require remembering the score _path_.
- **Enforced at:** `peer.rs:176-184`.
- **Violation impact:** Non-monotone tiers would let a peer transition `Trusted → Banned → Trusted` within a single decay window without ever crossing the ban threshold.

### INV-010 — Tier transitions emit one PeerAction; idempotent re-emissions are filtered

- **Statement:** `process_penalty` returns `PeerAction::NoAction` when `new_reputation == prior_reputation`; `process_ban` retains a duplicate `Banned` event by removing prior `Unbanned` events for the same peer.
- **Why:** Without idempotency filtering, repeated `Penalty::Mild` reports against a peer that is already `Disconnected` would emit a spurious disconnect on every report.
- **Enforced at:** `all_peers.rs:168-170` (no-op on equal reputation inside `process_penalty`), `manager.rs:382-394` (`process_ban` retains/replaces), `types.rs:41-53` (`PeerAction` variants).
- **Violation impact:** Spam of `PeerEvent::Banned` / `DisconnectPeer` events to consumers (`ConsensusNetwork`) and metrics inflation; potential double-blacklist or double-removal in gossipsub/kad.

## 4. Banning (peer-level, IP-level, temporary)

### INV-011 — An IP is banned iff its banned-peer count exceeds the threshold

- **Statement:** `BannedPeers::ip_banned(ip)` returns `true` iff the IP's count in `banned_peers_by_ip` is strictly greater than `BANNED_PEERS_PER_IP_THRESHOLD` (currently `1`, so two or more banned peers from the IP).
- **Why:** Single-peer bans should not block an entire NAT or VPN exit IP; the threshold provides minimal protection against collateral damage while still defending against coordinated misbehavior from one address.
- **Enforced at:** `banned.rs:20` (threshold constant), `banned.rs:95-108` (`banned_ips`), `banned.rs:114-116` (`ip_banned`).
- **Violation impact:** An off-by-one in the comparison would either ban any IP after a single peer ban (too aggressive — kills shared NATs) or never ban an IP at all (no IP-level defense).

### INV-012 — Adding a trusted peer removes its IPs from the banned set

- **Statement:** `add_trusted_peer` and `new_epoch` both call `BannedPeers::remove_banned_peer` / `remove_validator_ip` to drop the trusted peer's IPs from `banned_peers_by_ip`.
- **Why:** Without this, a validator that shares an IP with previously-banned non-validators would be silently rejected on incoming connections even though it is trusted.
- **Enforced at:** `all_peers.rs:127-139` (`add_trusted_peer`, `remove_banned_peer` at `:136`), `banned.rs:65-78` (`remove_validator_ip`), `all_peers.rs:974-977` (`new_epoch` calls `remove_validator_ip` per committee member).
- **Violation impact:** Committee members behind a shared IP could fail to connect at epoch boundaries, stalling consensus.

### INV-013 — temporarily_banned is independent of reputation

- **Statement:** `temporarily_banned: BannedPeerCache<PeerId>` is consulted in addition to `peers.peer_banned()` inside `PeerManager::peer_banned`; either set returning true means the peer is banned for connection purposes.
- **Why:** Excess-peer disconnects must rate-limit reconnection without permanently lowering the peer's reputation. The cache and the reputation tier serve different purposes: cache prevents immediate-reconnect storms; reputation classifies behavior.
- **Enforced at:** `manager.rs:339-347`.
- **Violation impact:** Excess peers could immediately reconnect, creating a churn loop that wastes connection slots and CPU.

### INV-014 — Temporary bans expire exactly once with one Unbanned event

- **Statement:** Each peer in `temporarily_banned` is removed at most once when its TTL expires inside `BannedPeerCache::heartbeat`, and exactly one `PeerEvent::Unbanned` is emitted per removal.
- **Why:** Duplicate `Unbanned` events would lead consumers to over-credit a peer (e.g., re-add to gossipsub mesh, re-insert into kad), wasting work and potentially racing with subsequent state changes.
- **Enforced at:** `cache.rs:84-105` (`heartbeat` pops each key at most once), `manager.rs:532-536` (`unban_temp_banned_peers`).
- **Violation impact:** Repeated `Unbanned` events would confuse downstream blacklist tracking in `ConsensusNetwork` and may cause gossipsub remesh oscillation.

### INV-015 — Temporary-ban resolution is bounded by the heartbeat interval

- **Statement:** Because `BannedPeerCache::heartbeat` runs only inside `PeerManager::heartbeat`, the effective ban duration is `excess_peers_reconnection_timeout` rounded _up_ to the next heartbeat tick.
- **Why:** Polling the cache continuously would burn CPU; binding cache cleanup to the heartbeat reuses an already-scheduled wake-up. The cost is granularity — bans can be slightly longer than configured, never shorter.
- **Enforced at:** `cache.rs:21-24` (doc), `manager.rs:270` (`unban_temp_banned_peers` called once per heartbeat).
- **Violation impact:** A polling implementation would over-spin; a non-heartbeat-driven implementation could under-rate-limit if cleanup runs before the timeout actually elapses.

### INV-016 — max_banned_peers is a soft cap; the oldest is evicted on overflow

- **Statement:** When `banned_peers.total() > max_banned_peers`, `prune_banned_peers` removes the peer with the oldest `Banned { instant }` and emits `PeerEvent::Unbanned` for each pruned peer.
- **Why:** Without a cap, an attacker who can connect (or appear to connect) from many distinct PeerIds could exhaust memory in `peers` and `banned_peers_by_ip`. Evicting the oldest favors recently-misbehaved peers.
- **Enforced at:** `all_peers.rs:873-895` (`prune_banned_peers`), `manager.rs:485-486` (pruned peers turned into `PeerEvent::Unbanned` events inside `register_disconnected`).
- **Violation impact:** Pure memory growth on long-running nodes; missing the `Unbanned` emission would desync gossipsub blacklist with the local ban set.

### INV-017 — Bans are not persisted across process restarts

- **Statement:** All ban state — `peers`, `banned_peers`, `temporarily_banned` — lives in process memory and is reinitialized on `PeerManager::new`.
- **Why:** Persistence was deemed unnecessary given short ban windows and the fact that the score itself rebuilds quickly from observed behavior; restart is also a self-heal path for mis-banned peers.
- **Enforced at:** `manager.rs:77-100` (constructor; no on-disk reads).
- **Violation impact:** A node restart resets every peer's score to `default_score`. This is intentional; security researchers should know that any restart-driven recovery is a valid evasion path for an attacker who can trigger restarts. See Appendix A3.

## 5. Connection state

### INV-018 — ConnectionStatus is disjoint and exhaustive

- **Statement:** A peer's `connection_status` is exactly one of `{Connected, Dialing, Disconnected, Banned, Disconnecting, Unknown}`. Every state-transition handler matches all six variants explicitly.
- **Why:** Exhaustive match is what guarantees the state machine has no implicit fall-through paths; libp2p connections can deliver events in surprising orders, so the handlers must cover every prior state.
- **Enforced at:** `status.rs:11-42` (enum), `all_peers.rs:370-403` (`handle_status_transition` dispatch), `:406-439`, `:442-474`, `:477-510`, `:546-578`, `:580-630` (per-variant handlers).
- **Violation impact:** An unhandled variant would silently drop a transition, leaving the peer in an inconsistent state relative to the swarm's view.

### INV-019 — Connection counters are nonzero iff the peer is Connected

- **Statement:** `num_in + num_out > 0` if and only if `connection_status == Connected { .. }`. All non-`Connected` states reset counters to zero on entry.
- **Why:** If a peer remained `Connected { num_in: 0, num_out: 0 }`, `connected_peer_ids` would over-count and limit checks would fail.
- **Enforced at:** `peer.rs:237-270` (`register_incoming` / `register_outgoing` maintain counters and reset to `num_in: 1, num_out: 0` (or `0,1`) on transition from any non-`Connected` variant).
- **Violation impact:** `peer_limit_reached` would be wrong, causing under- or over-acceptance of incoming connections.

### INV-020 — Connected cannot be entered from Banned or Disconnecting{banned:true}

- **Statement:** The connection-state machine only transitions into `Connected` from `Dialing`, `Unknown`, `Disconnected`, `Disconnecting{banned:false}`, or another `Connected` (counter increment). `Banned` and `Disconnecting{banned:true}` paths log an error and drop the IP from the banned set as a safety net.
- **Why:** Establishing a connection with a banned peer is a contract violation; if it happens (e.g., swarm event races a ban decision) the handler emits a clear log and forces the bookkeeping back into a coherent shape.
- **Enforced at:** `all_peers.rs:406-439` (`handle_connected_transition`; the `Banned` arm at `:419-422` logs the error and removes banned IPs).
- **Violation impact:** Silent acceptance of a banned peer with no log signal would let the operator miss real attacks. The error log at `all_peers.rs:420` is the audit trail.

### INV-021 — Banned peers cannot be dialed

- **Statement:** `Peer::can_dial()` returns `false` for `ConnectionStatus::Banned`, and `PeerManager::dial_peer` rejects with `NetworkError::DialBannedPeer` if the stored connection status is `Banned`.
- **Why:** A dial to a banned peer would defeat the ban; even if the swarm-level `peer_banned()` check would catch it later, rejecting at the call site preserves the dial-request queue contract (INV-025).
- **Enforced at:** `peer.rs:291-299` (`can_dial`), `manager.rs:134-147` (`dial_peer` matches `Banned` first), `manager.rs:665-667` (`PeerManager::can_dial` also gates on `temporarily_banned` via `peer_banned`).
- **Violation impact:** A dial would reach the swarm, fail at `handle_established_outbound_connection` (INV-040), and silently bloat error logs while wasting an outbound slot.

### INV-022 — register_disconnected is idempotent

- **Statement:** Calling `register_disconnected` on a peer already in `Disconnected` is a no-op — `handle_disconnected_transition` returns `PeerAction::NoAction` and does not double-increment `disconnected_peers`.
- **Why:** `ConnectionClosed` events can fire on already-disconnected peers if multiple subconnections close in quick succession; the count must stay correct.
- **Enforced at:** `all_peers.rs:477-510` (`handle_disconnected_transition`; `Disconnected → Disconnected` arm at `:484` is empty; only the `Unknown | Connected | Dialing` arm at `:491-499` increments `disconnected_peers`).
- **Violation impact:** `disconnected_peers` would over-count, causing premature pruning of valid disconnected-peer records.

### INV-023 — Incoming connection without prior Dialing is treated as Incoming

- **Statement:** A `FromSwarm::ConnectionEstablished` event whose endpoint is `ConnectedPoint::Listener` registers the peer as `ConnectionType::IncomingConnection` regardless of prior state, including `Unknown`.
- **Why:** Peers that dial us were never in our `Dialing` state; the handler must accept them without requiring a precursor transition.
- **Enforced at:** `behavior.rs:217-263` (`on_connection_established`), `peer.rs:237-251` (`register_incoming` accepts any prior state).
- **Violation impact:** Inbound peers would be rejected or accounted as outbound, breaking direction-specific limits (`max_outbound_dialing_peers`).

## 6. Dialing & dial-request queue

### INV-024 — At most one in-flight dial per PeerId

- **Statement:** `AllPeers::pending_dials` is keyed by `PeerId`, so repeated `register_dial_attempt` calls for the same peer overwrite (not duplicate) the entry.
- **Why:** The swarm's `DialOpts::peer_id(..).condition(PeerCondition::Disconnected)` already coalesces, and the reply channel is single-shot — duplicates would have no way to notify the original caller.
- **Enforced at:** `all_peers.rs:64` (field type), `all_peers.rs:338-348` (`register_dial_attempt`; `pending_dials.insert` at `:346` overwrites), `manager.rs:190-192` (`dial_attempt_already_registered` filter).
- **Violation impact:** First caller's reply oneshot is dropped silently. Logged consequence captured in Appendix A1.

### INV-025 — dial_requests entries satisfy gating predicates at enqueue

- **Statement:** Every entry pushed onto `dial_requests` is filtered through `PeerManager::dial_peer`, which rejects banned, connected, or already-dialing peers before the push.
- **Why:** The dequeue path in `poll` does not re-validate, so the gate must be tight at enqueue. The dial that reaches the swarm is the dial that was approved.
- **Enforced at:** `manager.rs:125-184` (`dial_peer` filters by `ConnectionStatus`), `manager.rs:212-214` (`next_dial_request` is a pure pop).
- **Violation impact:** A banned-but-not-yet-rejected dial could reach the swarm. See Appendix A2 for the dequeue-time gap.

### INV-026 — DialFailure transitions Dialing → Disconnected and replies once

- **Statement:** A `FromSwarm::DialFailure` event handled by `on_dial_failure` calls `register_disconnected` (transitioning from `Dialing` to `Disconnected`) and `notify_dial_result(Err(..))` exactly once per pending dial.
- **Why:** A peer left in `Dialing` past the failure would consume an outbound slot until the heartbeat dial-timeout swept it. The single-shot reply is also a hard contract of `oneshot::Sender`.
- **Enforced at:** `behavior.rs:294` (`on_dial_failure`), `all_peers.rs:359-364` (`notify_dial_result` removes from `pending_dials`), `all_peers.rs:501-505` (dial-timeout path inside `handle_disconnected_transition` also calls `notify_dial_result` with `NetworkError::Dial("dial attempt timedout")`).
- **Violation impact:** Leaked outbound slot; caller's task awaiting the oneshot would hang indefinitely.

### INV-027 — register_dial_attempt is called exactly once per dial

- **Statement:** PeerManager- and kad-initiated dials both route through `register_dial_attempt`, which is called once per logical dial: once by `poll → next_dial_request` for manager-initiated dials and once by `handle_pending_outbound_connection` for kad-initiated dials.
- **Why:** kademlia bypasses the manager's queue entirely and goes through `handle_pending_outbound_connection`; without this single funnel for dial registration, kad dials would never appear in `Dialing` state, breaking dial-timeout and connection counting.
- **Enforced at:** `behavior.rs:23-67` (`handle_pending_outbound_connection` intercepts kad dials), `behavior.rs:181` (`register_dial_attempt` call inside `poll` for manager-initiated dials).
- **Violation impact:** kad-initiated dials would be invisible to peer counting, allowing the swarm to dial more outbound peers than configured.

## 7. Discovery & peer-exchange

### INV-028 — discovery_peers is capped at max_discovery_peers

- **Statement:** `discovery_heartbeat` prunes the `discovery_peers` map down to `max_discovery_peers()` (default `target_num_peers * 2`) every tick.
- **Why:** Without a cap, peer-exchange responses from many connected peers would accumulate without bound; with a cap, discovery candidates are bounded by a multiple of the target connection count.
- **Enforced at:** `manager.rs:720-778` (`discovery_heartbeat`; pruning block at `:754-769`), `config/src/network.rs:405-407` (`max_discovery_peers`).
- **Violation impact:** Memory growth proportional to the number of peer-exchange responses received.

### INV-029 — Discovery candidates pass eligible_for_discovery before insertion

- **Statement:** Peers added to `discovery_peers` via `process_peer_exchange` or `process_peers_for_discovery` first pass `eligible_for_discovery`: address parses to IPv4/IPv6, no IP is banned, and `can_dial` returns true.
- **Why:** Without this filter, peer exchange could be used to seed dial attempts against banned peers or invalid addresses — turning peer-exchange into a poisoning vector.
- **Enforced at:** `manager.rs:547-585` (`process_peer_exchange` filters via `eligible_for_discovery` at `:562`), `manager.rs:712-717` (`process_peers_for_discovery` filters at `:713`), `manager.rs:705-707` (`eligible_for_discovery` definition), `types.rs:124-144` (`PeerExchangeMap` shape consumed by discovery).
- **Violation impact:** Adversaries could leverage peer-exchange to force this node to dial banned peers, log-spam, or waste outbound slots.

### INV-030 — Low-discovery-count emits Discovery event for kad

- **Statement:** When `discovery_peers.len() < max_discovery_peers()` after dial top-up, `PeerEvent::Discovery` is pushed so `ConsensusNetwork` can drive a new kad walk.
- **Why:** kad otherwise has no signal that this node is short on candidates; without the event, a node that loses peers cannot recover.
- **Enforced at:** `manager.rs:770-773` (else-branch in `discovery_heartbeat`).
- **Violation impact:** Network partition recovery becomes manual — the node remains under-connected indefinitely.

### INV-031 — Peer-exchange responses only include Connected peers

- **Statement:** `AllPeers::peer_exchange` filters `peers` to only those whose `connection_status().is_connected()` is true; banned, dialing, disconnecting, and disconnected peers are never gossiped.
- **Why:** Telling a peer about banned or dead peers either poisons their candidate set (banned) or wastes their dial slots (dead). Connected peers are most likely useful to a fresh peer.
- **Enforced at:** `all_peers.rs:774-786` (`peer_exchange`).
- **Violation impact:** Information leak (banned peer IDs propagate), or downstream dial-storm into dead addresses.

## 8. Epoch & committee membership

### INV-032 — New epoch makes every committee member trusted with max score

- **Statement:** After `PeerManager::new_epoch(committee)`: every `PeerId` resolved from `committee` is in `AllPeers::current_committee`, has `is_trusted == true`, has `score == max_score`, is absent from `temporarily_banned`, and has no IP entry in `banned_peers_by_ip` (via `remove_validator_ip`).
- **Why:** Committee members must connect at every epoch boundary regardless of prior misbehavior; an attacker who gets a future validator banned in epoch _N_ must not have that ban survive into epoch _N+1_.
- **Enforced at:** `manager.rs:604-622` (`new_epoch`; temporary-ban removal at `:609-611`), `all_peers.rs:928-982` (`AllPeers::new_epoch`; `make_trusted` + `remove_validator_ip` at `:974-977`).
- **Violation impact:** A validator could be locked out of its own epoch, losing votes and stalling consensus.

### INV-033 — Validators are exempt from prune_connected_peers

- **Statement:** `prune_connected_peers` filters out any peer satisfying `is_peer_validator(peer_id)` or `peer.is_trusted()` before disconnecting for connection-limit reasons.
- **Why:** A node that prunes its own validators because of an incoming-peer flood is self-DoSing; validators always take priority over excess connections.
- **Enforced at:** `manager.rs:493-527` (`prune_connected_peers`), `manager.rs:505-514` (filter on `is_peer_validator || is_trusted`).
- **Violation impact:** Under a connection-flood attack, this node could disconnect from committee members and stop participating in consensus.

### INV-034 — Banned validators are forcibly unbanned at epoch boundary

- **Statement:** `AllPeers::new_epoch` walks each committee peer's `ConnectionStatus`; if `Banned { .. }` or `Disconnecting { banned: true }`, it calls `update_connection_status(.., Unbanned)` and emits the corresponding `PeerAction::Unban`.
- **Why:** Combined with INV-032, this is the _active_ part of validator forgiveness — `make_trusted` alone would not move the connection state out of `Banned`.
- **Enforced at:** `all_peers.rs` (the `Disconnecting { banned: true }` and `Banned` arms inside `promote_committee_member`, which is invoked from both `new_epoch` and `upsert_peer`).
- **Violation impact:** A validator that was banned mid-epoch would stay `Banned` into the next epoch, blocking reconnection until manual operator intervention.

### INV-045 — Committee and trusted peers are never evicted by ban/disconnect pruning

- **Statement:** Neither `prune_banned_peers` nor `prune_disconnected_peers` may remove a peer satisfying `AllPeers::is_protected` (member of `current_committee` or `Peer::is_trusted`). `collect_excess_peers` skips such peers entirely. If the resulting eviction set is smaller than the requested excess, both pruners emit a single `warn!` naming both numbers and accept the temporary overshoot.
- **Why:** Without this protection, a sitting committee validator that hits the `Banned` connection state (operator-issued ban, state-machine `Disconnecting{banned:true}` path, or accidental score collapse before trust was set) can be evicted by `remove_peer`, which also drops its `bls_index` entry. The next `new_epoch` then resolves the BLS to nothing, and the validator is silently locked out of its own committee (F-1). Persistent `is_trusted` plus this guard give the "once-staked, never-forgotten" property without requiring a new permanent flag.
- **Enforced at:** `all_peers.rs` (`is_protected` helper; the `is_protected` early-`continue` in `collect_excess_peers`; the warn-on-shortfall block inside `prune_banned_peers` and `prune_disconnected_peers`).
- **Violation impact:** Committee validators may be silently locked out of their epoch after a transient ban; correlated bans from a coordinated peer could drop multiple validators at once.

### INV-046 — Unresolved committee BLS keys are surfaced for kad lookup

- **Statement:** Every BLS key in `current_committee_keys` whose value is `None` is reported in the second element of the `AllPeers::new_epoch` return tuple, and `PeerManager::new_epoch` pushes a single `PeerEvent::MissingAuthorities(unresolved)` event onto its queue. `ConsensusNetwork` consumes that event and issues `kad.get_record(bls_key)` for each entry.
- **Why:** A committee member that this node has never observed (cold restart, network partition, late-arriving validator) cannot be resolved from `bls_index` alone. Without an active discovery signal the node has no way to dial that validator until kad happens to walk into them — which may be never on a small network.
- **Enforced at:** `all_peers.rs` (`new_epoch` collects `unresolved`), `manager.rs` (pushes `PeerEvent::MissingAuthorities` when `unresolved` is non-empty), `consensus.rs` (consumer of `MissingAuthorities`).
- **Violation impact:** New or rebooting committee members would only be reachable opportunistically via kad's own discovery cycle; epoch participation could lag by minutes or fail entirely for nodes behind partitions.

### INV-047 — Resolved committee BLS keys map to trusted committee peers

- **Statement:** If `current_committee_keys[bls] == Some(peer_id)`, then `peer_id ∈ current_committee` and `peers[peer_id].is_trusted == true`. The promotion helper `AllPeers::promote_committee_member` is the single funnel that establishes this triple, called from both `new_epoch` (BLS resolved up front) and `upsert_peer` (BLS resolved later by a kad lookup).
- **Why:** The trio (`current_committee_keys`, `current_committee`, `is_trusted`) must stay consistent so that `is_peer_validator` and `prune_connected_peers` (INV-033) and the pruning protection (INV-045) all see the same committee membership. Splitting the promotion logic between two call sites was the original source of F-1: a kad-driven `add_known_peer` could refresh `bls_index` without updating committee state, so the next read of `is_peer_validator` would lie.
- **Enforced at:** `all_peers.rs` (`promote_committee_member` writes all three; `new_epoch` and `upsert_peer` are the only callers).
- **Violation impact:** A "resolved" committee entry with a peer that is not trusted or not in `current_committee` would let `prune_banned_peers` evict the validator (defeats INV-045) or let `process_penalty` ban it (defeats INV-002).

## 9. Heartbeat & periodic maintenance

### INV-035 — Heartbeat is the only periodic-state-change driver

- **Statement:** `heartbeat()` is invoked only from `NetworkBehaviour::poll` when `heartbeat_ready` returns true; no other code path drives decay, dial-timeout cleanup, temp-ban expiry, prune-to-target, or discovery top-up.
- **Why:** Concentrating periodic work in a single call site bounds the maximum staleness of every periodic property to one heartbeat interval — easy to reason about and easy to test.
- **Enforced at:** `manager.rs:247-274` (`heartbeat`), `manager.rs:239-245` (`heartbeat_ready`), `behavior.rs:165-167` (the sole call site, inside `poll`).
- **Violation impact:** A second driver could double-decay or double-prune, breaking conservation properties.

### INV-036 — Heartbeat executes a fixed maintenance sequence

- **Statement:** Within one `heartbeat()` invocation, the sequence is: (1) per-peer `heartbeat_maintenance` (dial timeouts + score decay + unban actions), (2) `prune_connected_peers`, (3) `unban_temp_banned_peers`, (4) `discovery_heartbeat`.
- **Why:** Order matters: dial timeouts must finalize before pruning (peers freed by timeout become candidates); unban events must fire before discovery (newly-unbanned peers become eligible for discovery).
- **Enforced at:** `manager.rs:247-274` (the literal call sequence at `:249, :267, :270, :273`).
- **Violation impact:** Re-ordering could either prune peers about to be unbanned (lost capacity) or attempt discovery against still-banned candidates (wasted dial slots).

### INV-037 — Dialing peers past dial_timeout are forced to Disconnected

- **Statement:** Any peer in `Dialing { instant }` where `instant + dial_timeout < Instant::now()` is transitioned to `Disconnected` during `heartbeat_maintenance`, and the dial-result oneshot (if any) is notified with `NetworkError::Dial("dial attempt timedout")`.
- **Why:** Stuck dials count toward `max_outbound_dialing_peers`; without timeout sweeping, a misbehaving peer that accepts a TCP handshake and never completes libp2p negotiation would silently consume an outbound slot.
- **Enforced at:** `all_peers.rs:255-276` (`heartbeat_maintenance`; the timeout filter at `:256-267` and the `update_connection_status(.., Disconnected)` call at `:271`), `all_peers.rs:501-505` (`notify_dial_result` with `Dial("dial attempt timedout")` inside `handle_disconnected_transition`).
- **Violation impact:** Outbound slot exhaustion under hostile peer conditions; caller awaiting the oneshot hangs.

### INV-038 — Heartbeat maintenance cannot worsen any peer's reputation

- **Statement:** `update_peer_scores` only emits `ReputationUpdate::Unbanned` actions through `update_connection_status`; the `Banned` and `Disconnect` arms are logged as errors and dropped.
- **Why:** Decay can only move scores toward zero (improving them); a heartbeat-driven worsening would indicate a clock bug or an unhandled penalty leaking into decay logic.
- **Enforced at:** `all_peers.rs:285-315` (`update_peer_scores`); the error arm at `:294-302` drops `Banned`/`Disconnect` with an error log.
- **Violation impact:** Silent ban during heartbeat would mean a clock or decay-math bug; surfacing it via error log is the canary.

## 10. NetworkBehaviour ↔ Swarm contract

### INV-039 — Pending inbound connections are rejected for banned IPs

- **Statement:** `handle_pending_inbound_connection` rejects with `ConnectionDenied` if `sanitize_ip_addr` fails, which happens when no IP can be extracted from the multiaddr or any extracted IP is banned.
- **Why:** This is the earliest hook the swarm offers — rejecting here saves the cost of a TLS handshake.
- **Enforced at:** `behavior.rs:69-77` (`handle_pending_inbound_connection`), `behavior.rs:199-208` (`sanitize_ip_addr`), `manager.rs:673-686` (`has_valid_unbanned_ips`).
- **Violation impact:** Inbound IP-banned peers could establish full connections and consume slots until `register_peer_connection` checks `peer_banned`.

### INV-040 — Established inbound/outbound connections to banned peers are rejected

- **Statement:** `handle_established_inbound_connection` and `handle_established_outbound_connection` both call `self.peer_banned(&peer)` and return `ConnectionDenied` on true. Outbound additionally calls `sanitize_ip_addr` on the resolved address.
- **Why:** kad may dial by `PeerId` alone, so the IP check at outbound-pending is impossible there; this is the safety net.
- **Enforced at:** `behavior.rs:79-93` (inbound), `behavior.rs:95-113` (outbound; `sanitize_ip_addr` at `:110`), `manager.rs:339-347` (`peer_banned` checks both `temporarily_banned` and reputation).
- **Violation impact:** Banned peers complete handshake — the only remaining defense is `register_peer_connection` (`manager.rs:433-436`), which logs an error but still proceeds with state updates.

### INV-041 — PeerManager only emits ToSwarm::Dial for queue-validated peers

- **Statement:** `ToSwarm::Dial` is emitted from `poll()` exclusively via `next_dial_request`, which consumes entries previously validated by `dial_peer` (INV-025). PeerManager never constructs `DialOpts` for a peer outside this queue path.
- **Why:** Single funnel for dial emission keeps the gating predicates centralized and the swarm's view consistent with `pending_dials`.
- **Enforced at:** `behavior.rs:184-189` (the only `ToSwarm::Dial` emission inside `poll`), `manager.rs:125-184` (the only queue producer).
- **Violation impact:** A bypass path would let dial requests escape the ban check; the dequeue-time gap (Appendix A2) is already the closest existing breach.

### INV-042 — PeerEvent::Banned is propagated to gossipsub + kad

- **Statement:** `ConsensusNetwork` consumes each `PeerEvent::Banned(peer_id)` exactly once and calls `gossipsub.blacklist_peer(&peer_id)` followed by `kademlia.remove_peer(&peer_id)`.
- **Why:** Without this propagation, a peer banned by `PeerManager` would still be a gossipsub mesh member (receiving and processing messages) and a kad routing-table entry (poisoning route discovery).
- **Enforced at:** `consensus.rs:1146-1151` (the consumer matches `PeerEvent::Banned`, calls `gossipsub.blacklist_peer` and `kademlia.remove_peer`; line numbers in that crate may drift independently); the producer lives in `peers/` at `manager.rs:393` (`process_ban` pushes the event) and is routed via the `poll_events` queue.
- **Violation impact:** Banned peers continue receiving gossip and remain reachable via kad. This invariant is _not enforced within peers/_ — see Appendix A4 for the timing gap.

## Appendix A. Invariants NOT currently enforced (known gaps)

These properties are _expected_ by readers of the code but not _guaranteed_ by current implementation. They are documented here so:

- security researchers know where to probe;
- maintainers know which gaps to close in future hardening passes.

### A1 — Re-dial overwrites the prior oneshot reply

**Where:** `all_peers.rs:346` (`pending_dials.insert(peer_id, reply)` inside `register_dial_attempt`).
**Symptom:** If two callers request a dial to the same `PeerId` before the first completes, the second's reply channel replaces the first's. The first caller's `oneshot::Receiver` is dropped silently; the awaiting task either hangs (no timeout) or receives a `RecvError` (with a timeout).
**Mitigation:** Caller-side timeouts on `oneshot::Receiver`; or change `pending_dials` to `HashMap<PeerId, Vec<oneshot::Sender<_>>>` and notify all.

### A2 — dial_requests entries are not re-validated at dequeue

**Where:** `manager.rs:212-214` (`next_dial_request`) — a plain `pop_front` with no `ConnectionStatus` recheck.
**Symptom:** A peer can be banned (`process_penalty → Banned`) between the `dial_peer` push and the `next_dial_request` pop, yet the dial is still emitted to the swarm. The swarm-level `peer_banned` check in `handle_established_outbound_connection` (`behavior.rs:95-113`) is the _only_ current defense, and it logs an error because the dial reaches that late stage.
**Mitigation:** Re-check `self.peer_banned(&peer_id) || !self.can_dial(&peer_id)` inside `next_dial_request` before returning the request.

### A3 — Bans are not persisted across restarts

**Where:** `manager.rs:77-100` (constructor).
**Symptom:** A restart clears `peers`, `banned_peers`, and `temporarily_banned`. Every previously-banned peer becomes default-scored again and can immediately reconnect.
**Stated intent:** Intentional (see INV-017). Researchers should treat node-restart cycles as an evasion path; operators should know that any persistent ban must come from operator-managed allow/deny lists, not from in-process reputation.

### A4 — Race between PeerManager ban and gossipsub blacklist

**Where:** `manager.rs:393` (`process_ban` pushes `PeerEvent::Banned`) → `consensus.rs:1146-1151` (consume, blacklist, remove).
**Symptom:** Between the moment `PeerManager` decides to ban a peer and the moment `ConsensusNetwork` actually calls `blacklist_peer`, one or more gossip messages from that peer may already be in the gossipsub queue and reach validation.
**Mitigation:** This is a property of the event-bus architecture — eliminating it would require an in-line blacklist call from within `PeerManager`. As long as message validation tolerates a small number of post-ban messages, this is acceptable.

### A5 — IP-ban does not eject already-connected peers

**Where:** `behavior.rs:79-113` (`handle_established_*`) — IP-ban checks fire on _new_ connections only. Once a peer is `Connected`, an IP ban targeting their address has no immediate disconnect path.
**Symptom:** A `Connected` peer whose IP becomes banned (e.g., another peer from the same IP gets banned, pushing the count past the threshold) continues operating until they trigger their own per-peer ban or the connection drops for other reasons. Subsequent connection attempts from that IP are blocked, but the live connection persists. The error log at `all_peers.rs:420` (`handle_connected_transition` Banned arm) is the audit trail for the racey edge.
**Mitigation:** Add an explicit "drop all live connections from this IP" step in `process_ban`.

### A6 — No allowlist at penalty call-sites

**Where:** Every caller of `PeerManager::process_penalty`. The trusted-peer short-circuit in `Peer::apply_penalty` (INV-002) is the _only_ mechanism preventing a validator from being banned.
**Symptom:** A future caller that bypasses `Peer::apply_penalty` — e.g., directly mutates `Score::telcoin_score` via a new helper — would have no guard against banning a validator. Today the field is `pub(super)` to `score.rs` only, so the gap is narrow but not enforced by the type system.
**Mitigation:** Introduce a `Score` newtype with constructor guards, or move trusted-peer checks earlier in the call chain.

### A7 — temporarily_banned and reputation can disagree

**Where:** `manager.rs:339-347` (`peer_banned` ORs them) — but updates are not atomic across both. A peer can be in `temporarily_banned` while having `Reputation::Trusted`, and vice versa.
**Symptom:** Different code paths (connection acceptance, dial gating, peer-exchange filtering, validator forgiveness) see different "banned" statuses depending on which set they query. Most code paths use `peer_banned`, which ORs correctly, but ad-hoc reads of either set in isolation can be wrong.
**Mitigation:** Standardize on `peer_banned` everywhere; flag direct reads of `temporarily_banned` or `reputation().banned()` as code smell.

### A8 — Per-peer connection cap is absent

**Where:** `peer.rs:241, 260` — `num_in` and `num_out` counters increment without an upper bound.
**Symptom:** A single peer can hold an arbitrary number of subconnections to this node; only the aggregate `max_peers()` is enforced. A peer that opens many connections counts as one in `connected_peer_ids` (a `HashMap` keyed by `PeerId`) but consumes multiple swarm-level slots.
**Mitigation:** Cap `num_in + num_out` per peer in `register_incoming` and `register_outgoing`; reject the swarm-level connection with `ConnectionDenied` when the cap is reached.

### A9 — Two ban thresholds can drift

**Where:** `peer.rs:180` reads `PeerConfig::min_score_for_ban` (via `Peer::reputation`); `score.rs:160` reads `ScoreConfig::min_score_before_ban` (via `Score::is_banned`). Both default to `-50.0`, but they are configured independently.
**Symptom:** If an operator overrides only one of the two, `Peer::reputation()` and `Score::is_banned()` can disagree about whether the peer is banned. `Score::is_banned` is the trigger for the decay-deferral window (INV-004); `Peer::reputation` drives `PeerAction` emission (INV-008-010). A mismatch would either skip the ban-window deferral (peer recovers faster than intended) or apply it without producing a ban event.
**Mitigation:** Collapse to a single source of truth, or assert equality at config-load time.

### A10 — Connected orphans (post-rotation) have no explicit cap

**Where:** `all_peers.rs:115-121` (`rebind_bls` clears `Peer::bls_public_key` but does not change `connection_status`); `:898-916` (`prune_disconnected_peers` only evicts peers in `ConnectionStatus::Disconnected`).
**Symptom:** When a validator rotates its `PeerId` under the same BLS key (INV-044), the old `Peer` record is retained with `bls_public_key = None`. If the old endpoint stays `Connected`, that orphan record lives in `peers` indefinitely — there is no per-`PeerId` cap and no orphan-specific sweeper. Aggregate `max_peers()` bounds the connected pool overall, but a flood of rotated identities held open could exhaust slots that would otherwise serve legitimate peers.
**Mitigation:** Either disconnect the orphan's `PeerId` eagerly inside `rebind_bls`, or sweep `peers` for `bls_public_key == None && connection_status.is_connected()` during heartbeat and disconnect them.

## Appendix B. Configuration knobs referenced by these invariants

Defaults are taken from `crates/config/src/network.rs`. Deployments may override any of these via the config file.

| Knob                                            | Default                 | Source               | Referenced by                      |
| ----------------------------------------------- | ----------------------- | -------------------- | ---------------------------------- |
| `PeerConfig::heartbeat_interval`                | 30 s                    | `network.rs:353`     | INV-015, INV-035, INV-036          |
| `PeerConfig::target_num_peers`                  | `K_VALUE * 3/2`         | `network.rs:350-354` | INV-028, INV-030, INV-033, INV-036 |
| `PeerConfig::max_peers()`                       | `target * (1 + excess)` | `network.rs:372-374` | INV-019, A8                        |
| `PeerConfig::peer_excess_factor`                | 0.3                     | `network.rs:358`     | INV-019 (indirect)                 |
| `PeerConfig::excess_peers_reconnection_timeout` | 600 s                   | `network.rs:362`     | INV-013, INV-014, INV-015          |
| `PeerConfig::dial_timeout`                      | 15 s                    | `network.rs:355`     | INV-037                            |
| `PeerConfig::max_banned_peers`                  | 100                     | `network.rs:363`     | INV-016                            |
| `PeerConfig::max_disconnected_peers`            | 100                     | `network.rs:364`     | INV-022, A10                       |
| `PeerConfig::min_score_for_disconnect`          | -20.0                   | `network.rs:356`     | INV-008, INV-009                   |
| `PeerConfig::min_score_for_ban`                 | -50.0                   | `network.rs:357`     | INV-008, INV-009, A9               |
| `PeerConfig::max_discovery_peers()`             | `target * 2`            | `network.rs:405-407` | INV-028, INV-030                   |
| `PeerConfig::max_outbound_dialing_peers()`      | derived                 | `network.rs:398-402` | INV-027, INV-037                   |
| `ScoreConfig::default_score`                    | 0.0                     | `network.rs:436`     | INV-001, INV-017                   |
| `ScoreConfig::max_score`                        | 100.0                   | `network.rs:438`     | INV-001, INV-005, INV-032          |
| `ScoreConfig::min_score`                        | -100.0                  | `network.rs:439`     | INV-001                            |
| `ScoreConfig::score_halflife`                   | 600 s                   | `network.rs:440`     | INV-006                            |
| `ScoreConfig::banned_before_decay_secs`         | 12 * 3600 s (12 hours)  | `network.rs:441`     | INV-004                            |
| `ScoreConfig::min_score_before_ban`             | -50.0                   | `network.rs:443`     | INV-004, A9                        |
| `BANNED_PEERS_PER_IP_THRESHOLD`                 | 1                       | `peers/banned.rs:20` | INV-011                            |
| `Penalty::Mild`                                 | -1.0                    | `peers/score.rs:90`  | INV-003                            |
| `Penalty::Medium`                               | -5.0                    | `peers/score.rs:91`  | INV-003                            |
| `Penalty::Severe`                               | -10.0                   | `peers/score.rs:92`  | INV-003                            |
| `Penalty::Fatal`                                | → `min_score`           | `peers/score.rs:93`  | INV-001                            |
