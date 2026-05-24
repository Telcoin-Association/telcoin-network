# tn-bug-scan — Verified Findings

_Git hash: cb8aa049_
_Target scope: crates/network-libp2p/src/peers/**, network-libp2p/{consensus,kad}.rs, state-sync/**, node/manager/node/epoch.rs, consensus/primary/consensus_bus.rs_
_Generated: 2026-05-21_

## Summary

| Severity | Count |
|---|---|
| CRITICAL | 0 |
| HIGH | 3 |
| MEDIUM | 5 |
| LOW | 4 |
| INFO | 1 |
| **Total** | **13** |

---

## HIGH severity

### [HIGH] [consensus] Unresolved-committee-window Fatal penalty causes self-DoS on returning CVV

- **Location:** `crates/network-libp2p/src/consensus.rs:813` (also: `consensus.rs:824`, `consensus.rs:1140-1142`, `peers/all_peers.rs:1044-1048`, `peers/all_peers.rs:982-1019`)
- **Category:** consensus
- **Secondary categories:** concurrency, error-propagation
- **Failure mode:** consensus-stall
- **Repro conditions:** Local node L receives `NetworkCommand::NewEpoch{committee}` containing BLS_v for validator V_x whose PeerId L has never observed. `peer_manager.new_epoch` marks BLS_v as unresolved (`current_committee_keys[bls_v] = None`), pushes `PeerEvent::MissingAuthorities([bls_v])`, ConsensusNetwork issues `kad.get_record(bls_v)`. In parallel, V_x's libp2p stack opens an inbound connection directly to L (V_x knows L from a prior epoch's persisted NodeRecord). V_x is registered as a regular peer (not yet trusted — bls_index does not yet contain BLS_v). V_x subscribes to a gossipsub topic and publishes a message. L's `verify_gossip` (consensus.rs:988) returns Reject (either because authorized_publishers does not yet include BLS_v for this epoch — see CF-4 — or because `peer_to_bls(V_x.pid)` returns None during the unresolved window). `process_penalty(V_x.pid, Penalty::Fatal)` fires (consensus.rs:813). V_x is not trusted → score → min → Banned → `PeerEvent::Banned` → `gossipsub.blacklist_peer + kademlia.remove_peer` (consensus.rs:1140-1142). Eventually the kad query resolves, `add_known_peer → upsert_peer → promote_committee_member` runs. The peer is now trusted (score reset to max via `make_trusted`), its connection_status transitions Banned → Disconnected via `handle_unbanned_transition`. `PeerEvent::Unbanned` propagates → `gossipsub.remove_blacklisted_peer`. **However, the kad routing-table entry was never restored** (no re-add path), and V_x.pid is still present in `temporarily_banned` (the BannedPeerCache is NOT cleared by promotion — only by the next `new_epoch` cycle or by 600s TTL expiry). Any subsequent V_x reconnect is denied by `peer_banned` at `handle_established_inbound_connection` for up to 600 seconds or until next epoch boundary.
- **Affected invariant:** INV-002 (trusted peers cannot be banned — violated by ordering: V_x is banned BEFORE becoming trusted), INV-032 (new_epoch removes temporarily_banned per committee member — only applied at new_epoch tick, not during the unresolved-window resolution), INV-042 boundary (PeerEvent::Banned propagates to gossipsub + kad; no symmetric "re-add kad on Unbanned" exists), Appendix A4 (race between PeerManager ban and gossipsub blacklist — here the eventual recovery is incomplete).
- **Root cause:** Cat 2 (ordering): the peer manager processes the penalty before kad resolution lands. State gap (GAP-2): `promote_committee_member` leaves the peer in `ConnectionStatus::Disconnected`, the swarm-level cleanup at PeerEvent::Banned removed the kad routing entry, and there is no symmetric re-add on Unban. Cross-feed CF-1: F-1 (Feynman flagged the penalty path) + F-7 (Fatal severity) + GAP-2 (post-promote state is Disconnected, not Connected) + Appendix A4 (gossipsub/kad/PeerManager are eventually-consistent across the event-bus). For a 10-node committee, locking out one CVV for 600 seconds increases the probability of consensus-stall when combined with normal validator availability variance.
- **Recommended fix:** (1) Inside `PeerManager::process_penalty`, before applying any penalty, check if the BLS associated with the peer is in `current_committee_keys` as `Some(&None)` (unresolved committee member) and SKIP penalty in that window. (2) Make `promote_committee_member` ALSO call `temporarily_banned.remove(peer_id)` to symmetrically clear the cache when a peer is promoted. (3) Add a re-add path on `PeerEvent::Unbanned` in `consensus.rs:1144` that re-introduces the peer to kademlia via `kademlia.add_address(&peer_id, addr)` for any known multiaddrs.
- **Confidence:** HIGH
- **Source:** Cross-feed P2→P3 (CF-1 in phase4-summary.md)
- **Discovery path:** Phase 2 Feynman flagged `process_penalty` writing to coupled state without checking the unresolved-CVV-window (F-1). Phase 3 state-check confirmed `promote_committee_member` leaves connection_status Disconnected (GAP-2). Phase 4 iteration 1 cross-fed: Fatal-penalty propagation through PeerEvent::Banned reaches gossipsub.blacklist_peer + kademlia.remove_peer; the recovery via PeerEvent::Unbanned only clears gossipsub. Phase 5 Scenario S1 traced 24 ordered events including the kad-resolution arrival.

### [HIGH] [concurrency] Observer-mode peers are pruned identically to non-CVV peers, stalling state-sync

- **Location:** `crates/network-libp2p/src/peers/manager.rs:490-524` (`prune_connected_peers`), `crates/network-libp2p/src/peers/all_peers.rs:743-750` (`is_peer_validator` returns true only for CVVs), `crates/state-sync/src/lib.rs:64-95` (`spawn_state_sync` only spawns for Observer / CvvInactive)
- **Category:** concurrency
- **Secondary categories:** consensus
- **Failure mode:** liveness-degradation
- **Repro conditions:** Node O runs in `NodeMode::Observer` and has connections to 5 CVV peers plus 12 non-CVV peers (target_num_peers = 15, excess of 2). O's state-sync tasks (`spawn_track_recent_consensus`, `spawn_fetch_consensus`) depend on these CVV peers to fetch consensus headers and pack files. On CVV V_k's peer manager: V_k has 30 connections at heartbeat tick. `prune_connected_peers` (V_k's manager.rs:490) sorts by score+routability. O is treated as a non-validator non-trusted peer. O's score and routability are below the cutoff → O is among the 3 peers chosen for `disconnect_peer(_, true)` → V_k emits `PeerEvent::DisconnectPeerX(O.pid, ...)`. If multiple CVVs simultaneously prune O (because O is one of their low-score peers), O loses several CVV connections in one heartbeat round. O's `request_consensus` calls hit `Err(NetworkError::NoPeers)` at consensus.rs:674 / `SendRequestAny`. `get_consensus_header` (state-sync/consensus.rs:88) sleeps 5s, retries, sleeps 5s — indefinitely. O's `discovery_heartbeat` (manager.rs:738) attempts to re-fill, but the CVVs (now disconnected) are not in `discovery_peers`. O is stuck.
- **Affected invariant:** Propose new INV-048: "Peers serving as state-sync sources for an Observer-mode node must be exempt from `prune_connected_peers` while state-sync is incomplete." The peer layer currently has no concept of NodeMode (the `Peer` struct in `peer.rs:24-53` carries no observer flag; only `is_trusted` distinguishes operator-pinned peers and committee members).
- **Root cause:** Cat 2 (assumption): the peer layer assumes all non-CVV non-trusted peers are equivalent. State gap: the `Peer` struct does not carry a NodeMode/observer flag and the protection rule in `prune_connected_peers` (`!is_peer_validator(peer_id) && !peer.is_trusted()`) does not check NodeMode. CF-3: Combined with F-8 (IP-ban poisoning), observers behind shared-NAT setups are doubly vulnerable.
- **Recommended fix:** Add a `is_observer: bool` (or `node_role: NodeRole` enum) field to `Peer`. When the local node observes a remote peer running in observer mode (signaled via NodeRecord extension or via an explicit "I am an observer" peer-exchange field), set the flag. Update `is_protected` (`all_peers.rs:849`) and `prune_connected_peers`'s filter (`manager.rs:505`) to also exempt observers. Alternative: have the application layer (consensus.rs) explicitly mark observers via `add_explicit_peer`-style API.
- **Confidence:** HIGH
- **Source:** Feynman-only (F-6), reinforced by user direction.
- **Discovery path:** Phase 2 F-6 directly flagged the gap (user direction explicit). Phase 3 GAP confirmed no NodeMode-aware protection exists. Phase 5 Scenario S2 walked the prune-cascade scenario where multiple CVVs simultaneously prune the observer.

### [HIGH] [concurrency] NewEpoch and UpdateAuthorizedPublishers timing race admits wrong-committee gossip rejection

- **Location:** `crates/network-libp2p/src/consensus.rs:524-527` (`UpdateAuthorizedPublishers` handler), `crates/network-libp2p/src/consensus.rs:702-717` (`NewEpoch` handler), `crates/network-libp2p/src/consensus.rs:988-1009` (`verify_gossip`)
- **Category:** concurrency
- **Secondary categories:** consensus
- **Failure mode:** liveness-degradation
- **Repro conditions:** The consensus engine emits `NetworkCommand::NewEpoch{committee}` and `NetworkCommand::UpdateAuthorizedPublishers{authorities}` as two separate `await` calls on the network handle. The network main loop (`process_command` at consensus.rs:522) consumes them serially. If `NewEpoch` arrives BEFORE `UpdateAuthorizedPublishers`, `peer_manager.new_epoch(committee)` runs first, marking some PeerIds as trusted committee members in the peer layer, but `authorized_publishers` for the consensus-output topic still contains the OLD epoch's BLS set. A new committee member V_x (whose BLS is in the new committee) publishes a gossip message on the consensus topic. `verify_gossip` looks up the topic's authorized publishers, finds V_x.bls is NOT in the (stale) set, returns Reject. `process_penalty(V_x.propagation_source, Penalty::Fatal)` fires (consensus.rs:813). V_x's PeerId is now Banned even though V_x.bls IS in `current_committee`. The trusted check inside `Peer::apply_penalty` (peer.rs:181) DOES protect V_x — `is_trusted` was set by `promote_committee_member`. So this scenario only fires if V_x.bls is ALSO unresolved (CF-1 territory) OR if the trusted promotion hasn't completed for V_x.pid yet (e.g., race within `new_epoch` itself).
- **Affected invariant:** No INV currently codifies the cross-layer ordering. Propose: "Updates to `current_committee_keys` and `authorized_publishers` for the same epoch must be applied atomically OR `verify_gossip` must consult both `current_committee_keys` AND `authorized_publishers` to determine acceptance."
- **Root cause:** Cat 2 (ordering): two-step update pattern with no atomic boundary. Cross-feed CF-4: a stale `authorized_publishers` rejects valid gossip from a fresh CVV. State gap: the network main loop doesn't enforce ordering on related-command pairs.
- **Recommended fix:** (1) Merge `NewEpoch` and `UpdateAuthorizedPublishers` into a single command. (2) Or: have `verify_gossip` also consult `peer_manager.current_committee_keys` so a CVV in the current committee is accepted even when authorized_publishers is stale (or vice versa).
- **Confidence:** MEDIUM (the application-layer caller probably emits these in the right order in practice; the network code does not enforce it).
- **Source:** Cross-feed P2→P3→P4 (CF-4 in phase4-summary.md, iteration 2).
- **Discovery path:** Phase 2 F-7 flagged GossipsubNotSupported = Fatal. Phase 4 iteration 2 extended the scrutiny to verify_gossip's predicates and found the cross-layer ordering race. Phase 5 Scenario S1 step 12 included this as the Path A failure.

---

## MEDIUM severity

### [MEDIUM] [state-atomicity] Trusted-orphan accumulation under repeated PeerId rotation

- **Location:** `crates/network-libp2p/src/peers/all_peers.rs:107-122` (`rebind_bls`), `peers/all_peers.rs:156-192` (`upsert_peer` rotation arm), `peer.rs:338-343` (`make_trusted` — `is_trusted` set, never cleared), Appendix A10
- **Category:** state-atomicity
- **Secondary categories:** consensus
- **Failure mode:** liveness-degradation
- **Repro conditions:** Validator V repeatedly rotates its libp2p network keypair (operator-driven config change with no BLS change). On each rotation: `upsert_peer(BLS_v, network_new, addrs_new)` runs. `rebind_bls` orphans the old PeerId (clears its `bls_public_key`). The rotation arm at `upsert_peer:175-183` removes the old PeerId from `current_committee` and promotes the new one. **The old PeerId's `is_trusted` flag is preserved (set in a prior promotion via `make_trusted` at peer.rs:341; never cleared).** Across K rotations: K orphan PeerIds with `is_trusted == true`, `bls_public_key == None`. `is_protected` returns true for each (trusted-OR branch). All orphans survive `prune_banned_peers` and `prune_disconnected_peers` indefinitely. They count toward aggregate `max_peers` (default ~75 = target * 1.3).
- **Affected invariant:** Appendix A10 documented (no orphan cap, no orphan sweeper).
- **Root cause:** Cat 5 (defensive code reveals broken invariant): `is_trusted` was added by `make_trusted` as a permanent flag for the "once-staked, never-forgotten" property (INV-045). The flag is intentionally never cleared so a transient committee-membership lapse doesn't lose the protection. But the orphan case is the symmetric problem: the trusted flag persists even when the orphan no longer corresponds to any active identity. Asymmetric delete pattern (4.5): `rebind_bls` clears `bls_public_key` but not `is_trusted`. The `current_committee` is symmetrically managed but `is_trusted` is not.
- **Recommended fix:** Per Appendix A10's recommendation, add an orphan sweeper to heartbeat: scan `peers` for `bls_public_key == None && connection_status.is_connected() && is_trusted` orphans older than N heartbeats and disconnect them. OR: clear `is_trusted` inside `rebind_bls` when the BLS is being cleared from a Peer record that the new PeerId is replacing (this requires distinguishing rotation-orphan from never-promoted-orphan).
- **Confidence:** HIGH
- **Source:** State-only with Feynman validation (F-2).
- **Discovery path:** Phase 1 mapper identified (is_trusted, bls_public_key, current_committee) triple. Phase 2 traced rotation path and confirmed is_trusted never cleared. Phase 5 Scenario S5 walks K-rotation accumulation.

### [MEDIUM] [panic-surface] KadStore.num_records can underflow via double-remove of the same expired record

- **Location:** `crates/network-libp2p/src/kad.rs:380-391` (`RecordStore::remove`)
- **Category:** panic-surface
- **Secondary categories:** state-atomicity
- **Failure mode:** state-corruption (counter wrong → liveness-degradation as new puts return Err(MaxRecords))
- **Repro conditions:** `kad.rs:389` reads `self.num_records -= 1` without saturating_sub, asymmetric to `evict_expired_records` (kad.rs:249) which uses `saturating_sub`. Sequence: (1) `put(record_X)` brings `num_records` to some N > 0. (2) On next `put`, `evict_expired_records` runs because `num_records >= max_records`, finds `record_X` expired, calls `self.db.remove::<KadRecords>(&key)` → Ok → counts++ → `num_records.saturating_sub(evicted)` (saturating). (3) libp2p-kad's internal `PutRecordJob` independently TTL-evicts record_X, calls `RecordStore::remove(record_X.key)` (kad.rs:380). `db.remove` returns Ok (vacuous remove of already-absent key — standard KV behavior). `num_records -= 1` unchecked → if `num_records` was already 0 (or after many such double-removes), underflows to `usize::MAX`. After underflow, every future `put` finds `num_records >= max_records` (true since MAX >= any), runs evict_expired_records (no rows to evict), returns `Err(MaxRecords)`. **No new kad records can be cached for the rest of the process lifetime.**
- **Affected invariant:** No INV in invariants.md (kad.rs is outside `peers/`). Local invariant: `KadStore::num_records == count(records in DB)` at every quiescent point.
- **Root cause:** Cat 5 (defensive code is asymmetric): the eviction path uses saturating_sub but the libp2p-callback path does not. Cross-feed CF-2: combined with restart load (F-10), a node that restarts with many expired records is at increased risk because both eviction and TTL-job paths may target the same record.
- **Recommended fix:** Replace `self.num_records -= 1` (kad.rs:389) with `self.num_records = self.num_records.saturating_sub(1)`. Same fix for `self.num_providers -= 1` (kad.rs:493 in `remove_provider`).
- **Confidence:** MEDIUM (bug is present; reachability depends on libp2p-kad's exact TTL-job semantics — needs a focused test).
- **Source:** Feynman-only (F-4) confirmed by state-asymmetry analysis.
- **Discovery path:** Phase 2 F-4 spotted the asymmetric arithmetic. Phase 3 GAP-4 confirmed the unchecked decrement. Phase 4 iteration 1 step C joint interrogation confirmed reachability via double-remove. Phase 5 Scenario S3 walked the restart-trigger.

### [MEDIUM] [error-propagation] GossipsubNotSupported is penalized Fatal, killing transient non-CVV peers

- **Location:** `crates/network-libp2p/src/consensus.rs:822-825`
- **Category:** error-propagation
- **Secondary categories:** consensus
- **Failure mode:** liveness-degradation
- **Repro conditions:** A peer connects to local node L without negotiating the gossipsub protocol (e.g., during a rolling upgrade where the peer's gossipsub support is temporarily absent, or an NVV / observer that uses request-response only). libp2p emits `GossipEvent::GossipsubNotSupported{peer_id}`. consensus.rs:824 fires `process_penalty(peer_id, Penalty::Fatal)`. The penalty applies INV-002's trusted-peer check (peer.rs:181) — trusted peers (CVVs + operator-pinned) are protected; non-trusted peers are not. A non-CVV peer's score → min → Banned → temporarily_banned + gossipsub.blacklist_peer + kademlia.remove_peer.
- **Affected invariant:** INV-002 protects only `is_trusted` peers. Non-CVV peers (observers, NVVs, transient peers) have no protection from this single-event fatal verdict.
- **Root cause:** Cat 5 (defensive code masks an assumption): the author assumed `GossipsubNotSupported` indicates a malicious peer. In reality, it can fire for transient protocol-negotiation reasons, especially during rolling upgrades.
- **Recommended fix:** Replace `Penalty::Fatal` with `Penalty::Severe` (or even `Medium`) so multiple events are required to ban. The peer would still eventually be banned if it persistently fails to negotiate gossipsub, but a single event would not be terminal.
- **Confidence:** HIGH (penalty severity is a design choice; behavior is deterministic).
- **Source:** Feynman-only (F-7).
- **Discovery path:** Phase 2 F-7 flagged penalty severity. Phase 4 iteration 1 step B confirmed via coupled-state analysis (writes score → min, no path to "soft warning").

### [MEDIUM] [consensus] IP-ban check denies CVV connection acceptance when sibling peer behind shared NAT is banned

- **Location:** `crates/network-libp2p/src/peers/banned.rs:20,95-108,114-116` (`BANNED_PEERS_PER_IP_THRESHOLD`, `banned_ips`, `ip_banned`), `peers/behavior.rs:199-208` (`sanitize_ip_addr`), `peers/manager.rs:687-704` (`has_valid_unbanned_ips`)
- **Category:** consensus
- **Secondary categories:** error-propagation
- **Failure mode:** consensus-stall (probabilistic)
- **Repro conditions:** Validators V_a, V_b co-located behind a single egress NAT IP_X. Two non-CVV peers P, Q (also from IP_X) misbehave and are banned independently. `banned_peers_by_ip[IP_X]` reaches 2. `ip_banned(IP_X) = 2 > 1 = true` (banned.rs:115). V_a's libp2p connection drops (transient network blip). V_a attempts to reconnect to local node L. `handle_pending_inbound_connection → sanitize_ip_addr → has_valid_unbanned_ips` → `is_ip_banned(IP_X) == true` → ConnectionDenied (behavior.rs:201-205). V_a's reconnection silently refused. **There is NO `is_peer_validator` or `is_protected` check at this layer** — INV-033 only protects committee members from pruning, not from connection denial. V_a stays disconnected until `prune_banned_peers` eventually evicts P or Q (which only happens when `banned_peers.total() > max_banned_peers`, default 100 — could take significant time in a quiet network).
- **Affected invariant:** INV-011 (threshold semantics — correct), INV-012 (`new_epoch` / `add_trusted_peer` clears validator IPs from banned set) — only fires at epoch boundary, NOT mid-epoch when sibling peers get banned. Asymmetric protection: INV-033 protects pruning, no analogous invariant protects connection acceptance.
- **Root cause:** Cat 5 (defensive layering reveals gap): the IP-ban check pre-dates committee-awareness. The trusted-peer-allowlist is enforced at penalty level (INV-002, peer.rs:181) and pruning level (INV-033, INV-045) but NOT at the IP-ban inbound connection gate. State gap GAP-1 confirmed.
- **Recommended fix:** Inside `has_valid_unbanned_ips` (manager.rs:691-704), add an early-allow path: if the connection's peer IS A KNOWN CVV (via `auth_to_peer` lookup by the connection's source PeerId or known multiaddr), bypass the IP-ban check. Alternative: maintain a `committee_ips: HashSet<IpAddr>` that mirrors the IPs of current committee members and is excluded from the `banned_ips()` return value.
- **Confidence:** HIGH (code path traced; environment-dependent trigger).
- **Source:** Feynman + State-check cross-feed (F-8 + GAP-1).
- **Discovery path:** Phase 2 F-8 flagged threshold. Phase 3 GAP-1 found the connection-acceptance protection gap. Phase 5 Scenario S4 walked the shared-NAT denial.

### [MEDIUM] [state-atomicity] KadStore restart load opens default-scored peer window vulnerable to Fatal-penalty self-DoS

- **Location:** `crates/network-libp2p/src/consensus.rs:284-299` (kad store load), `crates/network-libp2p/src/peers/manager.rs:601-630` (`new_epoch` is the only place that marks peers trusted — runs AFTER startup), Appendix A3
- **Category:** state-atomicity
- **Secondary categories:** consensus
- **Failure mode:** consensus-stall (probabilistic)
- **Repro conditions:** Node N restarts. `ConsensusNetwork::new` iterates `kad_store.records()` (consensus.rs:284) and calls `behavior.peer_manager.add_known_peer(key, ..., ...)` for each. `add_known_peer → upsert_peer → rebind_bls` populates `bls_index[key] = pid`. **No `promote_committee_member` runs because `current_committee_keys` is empty (no new_epoch yet).** The peer record at pid is created with `Score::default` (score = 0.0 = default_score, NOT trusted, NOT in current_committee). The network startup continues with `provide_our_data` (consensus.rs:441). Eventually the consensus engine's epoch-startup logic emits `NetworkCommand::NewEpoch` and (later) `UpdateAuthorizedPublishers`. Between the kad-load step and the new_epoch step, the loaded peer records are in `peers` as default-scored non-trusted peers. If any Fatal-penalty event fires on one of these PeerIds during this window — gossip arrives on a stale connection, libp2p emits `GossipsubNotSupported` for a peer mid-renegotiation, etc. — the peer is banned. New_epoch's `temporarily_banned.remove(peer_id)` at manager.rs:606 cleans the cache for known CVVs, BUT the peer was already pushed to gossipsub blacklist + kad removal via PeerEvent::Banned (consensus.rs:1140). The Unban-on-promote path (via promote_committee_member's update_connection_status) clears gossipsub but not kad routing — same as F-1.
- **Affected invariant:** Appendix A3 documents in-memory ban erasure on restart (intentional). The gap is: the symmetric problem — peers loaded from disk are NOT in any trusted state UNTIL new_epoch runs, leaving a window where they can be banned.
- **Root cause:** Cat 2 (ordering): kad-record load runs before new_epoch establishes committee. The window is small (startup) but reachable by any pending penalty event consumed in the same poll cycle.
- **Recommended fix:** During `ConsensusNetwork::new` after the kad-record load loop (consensus.rs:299), trigger an initial committee resolution before any penalty events can be consumed. Alternative: have `add_known_peer` on startup take an `is_initial_load: bool` flag that pre-populates `is_trusted = true` for any BLS that's in the upcoming committee.
- **Confidence:** MEDIUM (window-dependent; small in normal startup; bigger if network commands queue while peer manager processes startup).
- **Source:** Feynman + state-check (F-10).
- **Discovery path:** Phase 2 F-10. Phase 3 GAP-2 confirmed the post-load pre-promotion vulnerability. Phase 5 Scenario S1 substantially overlaps.

---

## LOW severity

### [LOW] [error-propagation] Dial requests are not re-validated at dequeue (Appendix A2)

- **Location:** `crates/network-libp2p/src/peers/manager.rs:209-211` (`next_dial_request`), `peers/behavior.rs:175-189` (poll dial emit)
- **Category:** error-propagation
- **Secondary categories:** consensus
- **Failure mode:** liveness-degradation
- **Repro conditions:** `dial_peer` (manager.rs:122) validates the peer at enqueue. Between push and pop, a `process_penalty` can ban the peer. `next_dial_request` (manager.rs:209) is a plain `pop_front` with no re-check. `behavior.poll` emits `ToSwarm::Dial`. The swarm-level `peer_banned` check at `handle_established_outbound_connection` (behavior.rs:104) catches the ban and returns `ConnectionDenied`, but the dial wasted a slot AND an error log was emitted (behavior.rs:105 "established outbound connection with banned peer — disconnecting...").
- **Affected invariant:** Appendix A2 documented.
- **Root cause:** Cat 6 (return/error): mitigation exists at the swarm-level but the queue layer is loose.
- **Recommended fix:** Per Appendix A2: `next_dial_request` re-checks `self.peer_banned(&peer_id) || !self.can_dial(&peer_id)` before returning the request. If the gate fails, drop the request, notify the reply with the appropriate error, and pop the next.
- **Confidence:** HIGH (documented).
- **Source:** Feynman-only (F-3) confirms Appendix A2.
- **Discovery path:** Phase 2 enumerated documented Appendix gaps; F-3 verified.

### [LOW] [panic-surface] num_in/num_out u8 counters lack per-peer cap (Appendix A8)

- **Location:** `crates/network-libp2p/src/peers/peer.rs:223,242` (`register_incoming`, `register_outgoing`), `peers/status.rs:14-18` (`num_in: u8, num_out: u8`)
- **Category:** panic-surface
- **Secondary categories:** state-atomicity
- **Failure mode:** state-corruption (counter wraps; INV-019 breaks)
- **Repro conditions:** A peer opens >255 inbound (or outbound) subconnections to the local node. `*num_in += 1` (peer.rs:223) wraps at 256 → 0. The peer is still in `ConnectionStatus::Connected{..}`. INV-019 (`num_in + num_out > 0 iff Connected`) violates: the peer is Connected but counters are 0 (with 256 actual subconnections).
- **Affected invariant:** INV-019, Appendix A8 documented.
- **Root cause:** Cat 4 (state assumption): u8 chosen without bound check.
- **Recommended fix:** Add a per-peer subconnection cap. When `num_in + num_out` reaches a configured limit, reject the swarm-level connection with `ConnectionDenied` at `handle_established_inbound_connection` / `handle_established_outbound_connection`.
- **Confidence:** HIGH (documented).
- **Source:** Feynman-only (F-5).
- **Discovery path:** Phase 2.

### [LOW] [state-atomicity] disconnected_peers counter uses unchecked addition

- **Location:** `crates/network-libp2p/src/peers/all_peers.rs:529, 572, 687`
- **Category:** state-atomicity
- **Failure mode:** silent-wrong-state (premature pruning)
- **Repro conditions:** `disconnected_peers += 1` is unchecked at three sites. The decrement is `saturating_sub` everywhere. If a state transition is ever processed twice for the same peer, the counter accumulates upward and never recovers. No specific code path in scope produces double-processing, but the asymmetric defense is a code smell.
- **Affected invariant:** INV-022 (idempotence of register_disconnected) holds for the entry point, but the asymmetric arithmetic provides no defense against any FUTURE bug.
- **Root cause:** Cat 5 (defensive code asymmetric).
- **Recommended fix:** Use `self.disconnected_peers = self.disconnected_peers.saturating_add(1)` at the three increment sites. Tightens INV-019 / INV-022 defense in depth.
- **Confidence:** MEDIUM (no current path triggers double-counting).
- **Source:** State-only (F-9).
- **Discovery path:** Phase 3 GAP-5.

### [LOW] [error-propagation] open_epoch_pack 30-second timeout has no retry mechanism

- **Location:** `crates/node/src/manager/node/epoch.rs:496-507`
- **Category:** error-propagation
- **Failure mode:** liveness-degradation
- **Repro conditions:** Node restarts during a sustained network partition with the previous-epoch record missing from local DB. `open_epoch_pack` triggers `record_by_epoch_with_timeout(previous_epoch, Duration::from_secs(30))`. If the partition exceeds 30s, the function returns Err. `run_epoch` propagates the error and the node terminates. Operator must wait for partition to heal AND restart manually.
- **Affected invariant:** No INV. Operational liveness concern.
- **Root cause:** Cat 6 (error propagation): timeout returns Err with no retry loop.
- **Recommended fix:** Wrap the timeout in a retry loop with backoff, or signal a "partition mode" that maintains the node in a holding state until at least one peer becomes reachable.
- **Confidence:** HIGH.
- **Source:** Feynman-only (F-11).
- **Discovery path:** Phase 2.

---

## INFO

### [INFO] [error-propagation] Sustained protected-peer prune overshoot generates warn-log spam

- **Location:** `crates/network-libp2p/src/peers/all_peers.rs:927-933, :958-965`
- **Category:** error-propagation
- **Failure mode:** liveness-degradation (negligible; observability concern)
- **Repro conditions:** When `collect_excess_peers` finds the unprotected eviction set smaller than the requested excess (because too many committee/trusted peers are banned/disconnected), `prune_banned_peers` and `prune_disconnected_peers` log a warn each heartbeat. INV-045 documents the overshoot as accepted, but the warn fires every heartbeat until pressure subsides.
- **Affected invariant:** INV-045 (intentional overshoot).
- **Root cause:** Defensive log without rate-limiting.
- **Recommended fix:** Rate-limit the warn-log (e.g., emit at most once per minute even if the overshoot persists). Or: emit at info level instead of warn for sustained-state messages.
- **Confidence:** HIGH.
- **Source:** Feynman-only (F-12).
- **Discovery path:** Phase 2.

---

## Findings catalog cross-check

Every finding cites either an INV-XXX or an Appendix-Ax (or proposes a new INV). No downgrades applied beyond F-12 already at INFO. No findings dropped for evidence insufficiency.
