# Domain Bug Patterns — peers / state-sync / kad / epoch

_Git hash: cb8aa049_
_Target scope: crates/network-libp2p/src/peers/**, consensus.rs, kad.rs, crates/state-sync/**, crates/node/src/manager/node/epoch.rs, crates/consensus/primary/src/consensus_bus.rs_
_User hints: 10-node committees; validator joining via kad; mid-epoch keypair rotation; crash-restart; orphan accumulation; collect_excess_peers off-by-one; score decay; IP-ban THRESHOLD=1; observer; state-sync deps; INV-043..INV-047 + Appendix A1..A10_
_Generated: 2026-05-21_

## Domain summary

The peer manager is a state machine on `HashMap<PeerId, Peer>` with five connection states + Banned + Unknown. Two reputation thresholds (`min_score_for_disconnect`, `min_score_for_ban`) plus a separate temporary-ban LRU and an IP-ban counter intersect. Committee membership intersects all three: committee members are `is_trusted == true`, exempt from penalties (INV-002), exempt from pruning (INV-045), and forcibly unbanned at new_epoch (INV-034). The `bls_index` is the only routing surface from BLS-key (consensus identity) to `PeerId` (libp2p identity) (INV-043).

State-sync (Observer / CvvInactive) lives outside `peers/` but depends on peer discoverability for header and pack-file fetches. The `peers/` layer has zero awareness of `NodeMode::Observer` — observers are evicted by `prune_connected_peers` like any other non-validator peer.

Kademlia drives committee resolution via `MissingAuthorities → kad.get_record(bls) → add_known_peer → upsert_peer`. The unresolved-committee window opens at every new_epoch tick for any committee member whose BLS the local node has never seen, and closes only when kad walks back with a record.

## Worked examples

### Worked example 1 — Banned-peer dial reaches swarm (Appendix A2)

```
manager.rs:122  dial_peer(peer_id)
  ↓ peer_id NOT yet banned — pass gate at :131, push to dial_requests
manager.rs:369  process_penalty(peer_id, Penalty::Fatal)
  ↓ all_peers.rs:212  update_connection_status(..., Banned)
behavior.rs:165 poll → next_dial_request() pops the request (no re-gate)
behavior.rs:185 ToSwarm::Dial emitted with peer_id
behavior.rs:104 handle_established_outbound_connection sees peer_banned == true
                returns ConnectionDenied — error path, error log
```

Failure mode: liveness-degradation + error-log noise; the only saving grace is the established-connection-time check. Documented in invariants.md Appendix A2.

### Worked example 2 — Observer in-mesh pruned mid-fetch

```
state-sync/lib.rs:74  Observer spawns spawn_track_recent_consensus
state-sync/consensus.rs:79..91  get_consensus_header() awaits network.request_consensus
PeerManager::heartbeat → prune_connected_peers (manager.rs:264, :490)
  ↓ INV-033: `is_peer_validator` returns false for observer (Observer is NOT in current_committee)
  ↓ Observer is_trusted = false (not in committee, not operator-pinned)
  ↓ peer added to ready_to_prune; disconnect_peer fires
network.request_consensus times out (peer gone). Fall-through goes to sleep 5s, retry forever.
```

Failure mode: liveness-degradation under load. Observer cannot complete state-sync. INV-033 gap.

### Worked example 3 — Score decay window vs heartbeat

```
A non-CVV peer accumulates Penalty::Severe (-10) twice over 60s.
heartbeat (manager.rs:244) fires every 30s by default.
At minute 5, peer reaches -50 → Reputation::Banned.
score.rs:153 sets last_updated += banned_before_decay (12h).
Score `checked_duration_since` returns None for the next 12h.
12h later, decay begins, peer recovers in ~10 min if quiet.
```

This is correct (INV-004) — but the same path applied to a soon-to-be committee member (epoch N+1) means the validator enters the new epoch already pre-loaded near the ban threshold (`telcoin_score` survives `new_epoch` for non-committee peers). The committee-side path overwrites Score with `Score::new_max` (peer.rs:341 in `make_trusted`), so trusted promotion DOES erase carryover. **The risk is for peers who become committee members mid-epoch via the rotation arm.**

## Bug-scenario templates (multi-event sequences)

### Template T1 — Unresolved CVV evicted mid-epoch

1. Node R restarts. In-memory state cleared (INV-017, Appendix A3).
2. Epoch transition: new_epoch(committee). For BLS keys not in bls_index, `current_committee_keys[bls] = None`, MissingAuthorities pushed (INV-046).
3. ConsensusNetwork issues `kad.get_record(bls)` for each. Records resolve over seconds-to-minutes.
4. Before resolution lands, an unrelated peer joins and triggers `prune_connected_peers` heartbeat path. `is_protected` checks `current_committee.contains(peer_id) OR peer.is_trusted()`. The not-yet-resolved committee member's PeerId is NOT in `current_committee` (only resolved ones are inserted via `promote_committee_member`). It is NOT trusted (only the rotation arm makes the orphan trusted; never-seen-before peers don't go through promote until kad resolves).
5. If that committee peer happened to connect inbound before kad resolution, it gets `is_protected == false` → falls into pruning sort.
6. Eviction may evict the still-unresolved-from-the-local-node's-view committee member. The next packet from that peer rejected (`peer_banned`). Self-DoS.

Failure mode: consensus-stall on small committees if multiple unresolved members are evicted.
Invariant violated: spirit of INV-045 (committee peer evicted) — but INV-045 only applies AFTER promote_committee_member runs. The gap is in the WINDOW.

### Template T2 — Mid-epoch keypair rotation + new_epoch race

1. Validator V rotates libp2p keypair at PeerId rotation boundary (operator config change).
2. Old peer record (PeerId_old) remains as orphan (INV-044), is_trusted == true (preserved).
3. Almost-simultaneously, `new_epoch` fires.
4. `new_epoch` clears `current_committee` (all_peers.rs:1038), clears `current_committee_keys`.
5. If kad resolves bls_v BEFORE new_epoch in the same tick: bls_v → PeerId_new, promoted, current_committee = {PeerId_new}.
6. If new_epoch fires while orphan PeerId_old still in `peers` with `is_trusted=true` and `bls_public_key=None`: orphan is not in current_committee (peer_id check would fail since current_committee was just cleared) but IS is_trusted → `is_protected` returns true via the trusted branch → orphan protected from eviction.

This means the orphan survives. But: if the new PeerId_new comes in via kad mid-rotation and the `current_committee_keys[bls_v]` is `Some(&Some(PeerId_old))` because new_epoch already ran:
- INV-047 rotation arm: `upsert_peer` removes `PeerId_old` from `current_committee`. The orphan still has `is_trusted=true` from before, so `is_protected` still returns true.
- Orphan accumulates (Appendix A10 — no sweeper).

Repeated rotations grow `peers` without bound.

### Template T3 — IP-ban poisons validator sharing subnet

1. Two validators V_A, V_B share a hosted-VM subnet (e.g. AWS or DigitalOcean exit). IPs differ but only by host octet.
2. NOT same IP — banned.rs uses exact `IpAddr` match (banned.rs:115). Subnet sharing is fine.
3. Real case: V_A and V_B share the SAME IP because both run behind a single egress NAT. `BANNED_PEERS_PER_IP_THRESHOLD = 1` (banned.rs:20). `ip_banned` returns true when `count > 1` (banned.rs:115).
4. V_A gets a single Fatal penalty: banned_peers_by_ip[IP] = 1 (NOT yet banned — count not > 1).
5. Some other random peer X (same IP, e.g. a peer-exchange that resolved to the same NAT box) gets Fatal: banned_peers_by_ip[IP] = 2. `ip_banned(IP)` now true.
6. V_B's next reconnect attempt at heartbeat: `has_valid_unbanned_ips` returns false because the IP is in `banned_ips()` (returns IPs where count > 1). V_B's incoming/outgoing connection denied.
7. However, INV-033 protects V_B if it is already in current_committee. The hole is: when V_B's connection drops (network blip, scheduled restart) and tries to reconnect, the ban check fires BEFORE V_B's CVV status is consulted at the connection level (behavior.rs:88 `peer_banned` → `peers.peer_banned` → `peer.known_ip_addresses().any(ip_banned)` is true; CVV check at INV-033 only applies in `prune_connected_peers`, not in connection acceptance).

Failure mode: consensus-stall (10-node committee, 1 validator behind NAT cannot reconnect to network if any sibling peer gets banned).

### Template T4 — Bootstrap → new_epoch ordering reorder

1. `init_network_for_epoch(handle, bootstrap_peers, committee_keys, initial_epoch=true)` (epoch.rs:1296) must `add_bootstrap_peers` BEFORE `new_epoch`.
2. Inside, `add_bootstrap_peers` → `peer.add_known_peer(bls, info.network_key, vec![info.network_address])`, which calls `upsert_peer`.
3. `upsert_peer` inserts the peer into `peers`, updates `bls_index`, and only promotes if `current_committee_keys[bls] == Some(&None)`. At the bootstrap step, `current_committee_keys` is empty, so the check returns `None` → no promotion.
4. Then `new_epoch` runs, sees `bls_index[bls] = peer_id`, calls `promote_committee_member` synchronously.
5. **Correct outcome.**
6. **However:** if a developer ever inverts the order (`new_epoch` first, then `add_bootstrap_peers`), then `new_epoch` sees an empty `bls_index`, marks every committee BLS as unresolved, fires `MissingAuthorities`. The bootstrap peers ARE then upserted, BUT — because `current_committee_keys[bls] = Some(&None)`, `upsert_peer` goes through the promotion arm correctly. So even in the reordered case, the kad-promotion logic works.
7. **Real risk** is silent: between `add_bootstrap_peers` and `new_epoch`, if any heartbeat fires that prunes the not-yet-promoted bootstrap peer (`is_trusted = false`, not in `current_committee`), it gets pruned.

### Template T5 — Restart wipes ban state, no exponential backoff

1. Validator V_attack accrues penalties, gets banned. Score state in memory only (INV-017).
2. Operator restarts node R.
3. On restart, `PeerManager::new` initializes `peers = Default`, `banned_peers = Default`, `temporarily_banned = Default`.
4. V_attack reconnects: clean slate.
5. V_attack misbehaves identically: takes same N events to re-ban.
6. Restart cycle becomes evasion path (Appendix A3, documented as intentional).
7. **But:** also wipes legitimate score history. A node that misbehaves N times accumulates N events to reach ban, and a restart resets it. Mild penalties never accumulate to ban if peer can flap-restart. Documented intentional, but worth flagging since the network has no operator-deny list mechanism (manager.rs constructor takes no on-disk reads).

### Template T6 — kad.rs num_records underflow on race

1. kad.rs:380 `remove(k)` reads `self.kad_type`, calls `self.db.remove(...)`.
2. If `is_ok() == true` → `self.num_records -= 1` (line 389). 
3. **Critical:** this is **NOT** `saturating_sub`. If a race or a bug ever causes `remove` to fire with `num_records == 0`, this panics in debug, wraps in release.
4. Race surface: `RecordStore::remove` is called from libp2p's kad on TTL expiration. If `evict_expired_records` also fires (used saturating_sub at line 249), and the same record is removed twice (once by TTL eviction, once by put-replacement?), the second remove sees `is_ok == true` but `num_records` was already decremented.
5. Actually `db.remove` returns Ok even if the row was already absent in most KV stores. So a double-remove is plausible.

Failure mode: panic-surface (crash) on release in pathological case; certain liveness-degradation if it wraps.

## Coupled state table

| State A | State B | Coupling invariant | Enforced via | Risk |
|---|---|---|---|---|
| `bls_index[bls]` | `peers[pid].bls_public_key` | bidirectional iff (INV-043) | rebind_bls (all_peers.rs:107), remove_peer (:899), clear_bls (peer.rs:145) | Direct write breaks pair (`pub(super)` mitigates) |
| `current_committee_keys[bls]` | `current_committee`, `peers[pid].is_trusted` | promote_committee_member triple-write (INV-047) | all_peers.rs:982; new_epoch (:1033), upsert_peer (:156) | Window: new_epoch+kad latency |
| `temporarily_banned` | `peers[pid].reputation` | OR'd by peer_banned (Appendix A7) | manager.rs:336 | Drift on path that reads only one |
| `banned_peers.total` | `banned_peers.banned_peers_by_ip` | total = sum(by_ip) within reason? | remove_banned_peer saturating_sub (banned.rs:43) | banned_peers can desync if same IP has multiple peers (NOTE comment at :38) |
| `peers[pid].is_trusted` | `current_committee.contains(pid)` | trusted ⇐ committee, but not ⇒ (rotation orphan has is_trusted but not in committee) | promote_committee_member set both; nothing clears is_trusted when committee changes | Trusted orphan accumulation (A10) |
| `discovery_peers` | (kad routing table) | kad routability vs PeerManager `routable` | update_routing_for_peer (kad.rs RoutingUpdated handler at consensus.rs:1312) | Race during ban: kad removes peer but `routable` may not flip if peer reconnects quickly |
| `num_records` | actual rows in KadRecords table | strict equality | put (kad.rs:339), remove (:380), evict_expired_records (:219) | Underflow on race (unchecked `-= 1`) |
| `pending_dials` | `peers[pid].connection_status == Dialing` | one-to-one (INV-024) | register_dial_attempt (:373) | Re-dial silently drops the prior reply (A1) |

## Telcoin-network-specific red flags

- **HashMap iteration in pruning paths** — `connected_peers_by_score_and_routability` (all_peers.rs:816) iterates `self.peers` (HashMap) and `sort_by_key` afterward. Stable sort, so the unstable iteration order is washed out by the deterministic sort key (Score implements Ord). **Confirm:** every peer with equal score is tie-broken by `.shuffle()` (line 821), which uses `rand::rng()` — non-seeded. This is local-only state (no consensus dependence) so not a fork risk, but it is non-deterministic across restarts and may behave differently across reboots.
- **collect_excess_peers BinaryHeap with HashMap source** — iteration order of source HashMap is non-deterministic, BUT the heap selects on instant (oldest first). When two banned peers have the same instant, the heap retains arbitrary one. Not a determinism issue since pruning is local.
- **rand::rng() for tie-breaking in pruning** — non-seeded; restart-to-restart non-determinism but no consensus impact.
- **`unwrap_or_default()` on tn::observer fetcher** (state-sync/lib.rs:138) — silent default header masks DB issues. Logging level appropriate.
- **`.unwrap_or(true)` on publisher_is_banned** (consensus.rs:1356) — defensive: records without publisher are rejected. Correct.
- **Saturating arithmetic in connection counters** — `disconnected_peers.saturating_sub(1)` everywhere. INV-022 demands idempotence and this is the mechanism; but `disconnected_peers += 1` (all_peers.rs:529, 572, 687) is NOT saturating. If counts ever desync, addition keeps climbing while subtraction floors at 0. Counts could grow with no upper bound.
- **`expect("Key is not new")` and `expect("Key must exist")` in cache.rs:55,72** — bypassed under normal flow because insert/remove validate, but if `BannedPeerCache::insert` runs concurrently with itself (unlikely; manager.rs is single-threaded) the linear scan `position` could miss. Reachable only on developer error invoking on wrong key, but it's a panic-surface that should be replaced with `.expect_or_default()` or `if let Some`. LOW.
- **`num_records -= 1` in kad.rs:389 NOT saturating** — see Worked example 6 / Template T6.
- **SystemTime↔Instant translation in kad.rs:125-163** — wall-clock dependent; cross-restart kad TTL accuracy depends on the host clock. Documented sloppy in the comment. No consensus implication (kad records are p2p discovery only).
- **NO observer differentiation at PeerManager** — `is_peer_validator` only checks `current_committee` (CVVs). NodeMode::Observer peers do not get any protection. Observer that runs state-sync depends on at least one CVV peer staying connected — and that CVV is protected, but the observer itself can be evicted by another node's `prune_connected_peers` if the observer is the lowest-scoring connection. Cross-layer state-sync stalls.
- **`Score::is_banned` reads `min_score_before_ban` while `Peer::reputation` reads `min_score_for_ban`** (Appendix A9) — two thresholds default to the same value but configurable independently. Documented.
- **`debug_assert!`-free** — no debug_asserts found in scope.
- **Lock-across-await audit:** no `parking_lot::Mutex` or `std::sync::Mutex` held across `.await` in the scope. The only synchronization is `tokio::sync::oneshot` for dial replies and `tokio::sync::Semaphore` in state-sync (consensus.rs:188). Safe.
- **`spawn_track_recent_consensus` shutdown path:** the task drops the rx_shutdown noticer reference at function exit. Confirmed it uses `&rx_shutdown` (consensus.rs:161) — Noticer is dropped at end-of-function which is correct.

## Cross-layer paths

- **gossipsub → PeerManager:** consensus.rs:813 (Penalty::Fatal on invalid gossip), :824 (GossipsubNotSupported → Fatal), :828 (SlowPeer → Mild). All three fire `process_penalty` on the propagation_source PeerId. If the propagation_source happens to be a committee member, INV-002 (trusted peers immune to penalties) protects them. **But:** if the peer is observed but is NOT YET in `current_committee` (unresolved-CVV window — Template T1), the penalty applies. A racing GossipsubNotSupported penalty during the unresolved window can permanently disqualify a new validator.
- **state-sync → request_consensus → peer_to_bls:** consensus.rs:1177 reads peer_to_bls. If the peer was banned between request submission and response arrival (heartbeat fired in between), peer_to_bls still returns Some (peer record still in `peers` until pruning) — but the bus event is silently dropped at `try_send`. Observer's request just times out.

## Test-coverage map

- INV-043, INV-044, INV-045 ✓ — `peers.rs` test suite covers rotation, orphan creation, prune skip on protected.
- INV-046 ✓ — `new_epoch_reports_unresolved_bls_keys`.
- INV-047 (both arms) ✓ — `committee_member_rotation_promotes_new_peer_id`, `upsert_peer_promotes_unresolved_committee_member`, `upsert_peer_promotes_banned_committee_member_via_kad`.
- **MISSING tests:**
  - Eviction window race between `new_epoch` (unresolved CVV) and `prune_connected_peers` (Template T1).
  - Score carryover from non-CVV to mid-epoch promoted CVV (note: `make_trusted` resets to max — this is good but no test pins this behavior).
  - IP-ban THRESHOLD=1 with shared-NAT validator scenario (Template T3).
  - Observer-mode pruning protection (no protection currently exists; need to either add tests demonstrating the gap or add the protection).
  - kad.rs num_records underflow regression (Template T6).
  - collect_excess_peers behavior when `excess > unprotected_count` (covered by `prune_with_only_protected_peers_keeps_all` and `prune_skips_protected_peer_logs_warn`).
