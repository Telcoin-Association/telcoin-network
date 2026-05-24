# Phase 3 — State Cross-Check (full)

## Mutation matrix

For each state variable, list every code path that mutates it AND every path that reads it:

| State variable | Mutations | Reads |
|---|---|---|
| `peers: HashMap<PeerId, Peer>` | upsert_peer (insert), add_trusted_peer (insert/replace), ensure_peer_exists (insert default), remove_peer (remove), all_peers.rs:263 (insert default), all `update_connection_status → handle_*_transition → peer.set_connection_status` paths | get_peer (everywhere), is_peer_validator (via current_committee), iter (peer_exchange, heartbeat_maintenance, etc) |
| `bls_index: HashMap<BlsPublicKey, PeerId>` | rebind_bls (all_peers.rs:111,115), remove_peer (:904-906) | auth_to_peer (:730), contains_bls (:737), new_epoch lookup (:1044), peer_to_bls (via Peer::bls_public_key, NOT bls_index — interesting!) |
| `peers[pid].bls_public_key` | rebind_bls (orphan clear at :118), upsert_peer (via update_net), add_trusted_peer (via new_trusted), clear_bls (peer.rs:145) | peer_to_bls (all_peers.rs:723), all_peers.rs:109 (rebind_bls read), peer_exchange filter (peer.rs:803) |
| `current_committee: HashSet<PeerId>` | new_epoch (clear at :1038, insert at promote_committee_member :987), upsert_peer rotation arm (remove :181) | is_peer_validator → is_peer_cvv (:749), is_protected (:850) |
| `current_committee_keys: HashMap<BlsPublicKey, Option<PeerId>>` | new_epoch (clear at :1039, insert None at :1046, insert Some via promote_committee_member :988), upsert_peer (via promote at :188) | upsert_peer should_promote match (:173) |
| `peers[pid].is_trusted` | new_trusted (peer.rs:67), make_trusted (peer.rs:339), nothing clears it back to false ever | is_protected (all_peers.rs:850), is_trusted (peer.rs:328), peer_is_important (manager.rs:597), prune_connected_peers (:505), Peer::apply_penalty (:181), Peer::heartbeat (:295) |
| `peers[pid].connection_status` | Peer::set_connection_status (peer.rs:202), Peer::register_incoming/outgoing/dialing | connection_status() (peer.rs:207), used everywhere |
| `peers[pid].score` | Score::apply_penalty (peer.rs:182), Score::update_at (:132), make_trusted (:341 overwrites with new_max), Peer::new_trusted (:66 starts at new_max), Peer::default (Score::default = default_score) | aggregate_score (everywhere), is_banned (score.rs:158), reputation (peer.rs:158) |
| `banned_peers.total` | add_banned_peer (banned.rs:82 saturating_add), remove_banned_peer (:43 saturating_sub), remove_validator_ip (:75 saturating_sub) | total() (:90), prune_banned_peers (excess calc, all_peers.rs:913) |
| `banned_peers.banned_peers_by_ip` | add_banned_peer (banned.rs:85 += 1), remove_banned_peer (:50 saturating_sub), remove_validator_ip (:73 remove_entry) | ip_banned (:114), banned_ips (:95) |
| `disconnected_peers: usize` | += 1 at all_peers.rs:529, 572, 687; saturating_sub at :452, :489, :596, :970 | prune_disconnected_peers (:947) |
| `temporarily_banned: BannedPeerCache<PeerId>` | insert (manager.rs:280, 285, 291), remove (manager.rs:114, 606), heartbeat (cache.rs:84-105 drain) | contains (cache.rs:108) → peer_banned (manager.rs:343), len (cache.rs:113) |
| `discovery_peers: HashMap<PeerId, Vec<Multiaddr>>` | insert (manager.rs:579, 734), remove (manager.rs:764, 778), retain (manager.rs:741) | discovery_heartbeat (manager.rs:738) |
| `events: VecDeque<PeerEvent>` | push_back (everywhere), retain (process_ban) | poll_events (manager.rs:223) |
| `dial_requests: VecDeque<DialRequest>` | push_back (manager.rs:180), pop_front (next_dial_request :209) | next_dial_request only |
| `pending_dials: HashMap<PeerId, oneshot::Sender>` | insert (all_peers.rs:381 — overwrites!), remove (reply_for_dial_attempt :390) | notify_dial_result (:394) |
| `num_records (kad.rs)` | put (kad.rs:365 += 1), remove (:389 -= 1 unchecked), evict_expired_records (:249 saturating_sub) | put limit check (:358), records() iter (no read) |
| `num_providers (kad.rs)` | add_provider (:451 += 1), remove_provider (:493 -= 1 unchecked), evict_expired_providers (:285 saturating_sub) | add_provider limit check (:402) |
| `published_to_peers: HashSet<PeerId>` (consensus.rs) | insert (consensus.rs:1125) | insert returns bool (used to gate publish_our_data_to_peer) |
| `kad_record_queries: HashMap<QueryId, KadQuery>` | insert (consensus.rs:1153), remove (close_kad_query :1489) | process_kad_query_result (:1450) |
| `connected_peers: VecDeque<PeerId>` (consensus.rs) | push_back (:1130), retain (:1037, :1099) | front, rotate_left (:666, :668) |
| `authorized_publishers: HashMap<String, Option<HashSet<BlsPublicKey>>>` | assigned wholesale (consensus.rs:526), insert (line 598) | verify_gossip (line 999) |

## Parallel-path comparison

### Pair 1: trusted-peer protection (INV-002, INV-045)

| Path | `is_trusted` check before penalty? | `is_protected` check before prune? | IP-ban bypass? |
|---|---|---|---|
| `process_penalty → Peer::apply_penalty` | ✓ (peer.rs:181) | n/a | n/a |
| `prune_banned_peers → collect_excess_peers → is_protected` | n/a | ✓ (all_peers.rs:871) | n/a |
| `prune_disconnected_peers → collect_excess_peers → is_protected` | n/a | ✓ | n/a |
| `prune_connected_peers` (manager.rs:490) | n/a | ✓ (manager.rs:505 — filters validator OR is_trusted) | n/a |
| `handle_pending_inbound_connection → sanitize_ip_addr → has_valid_unbanned_ips` | n/a | n/a | ✗ |
| `handle_established_inbound_connection → peer_banned` | n/a | n/a | ✗ (peer_banned ORs ip-ban) |
| `handle_established_outbound_connection → peer_banned + sanitize_ip_addr` | n/a | n/a | ✗ |

**GAP-1: IP-ban check does NOT respect committee membership.** A CVV behind a shared NAT exit IP can be denied connection acceptance even though INV-002 says they should never be banned. The protections are paired at the penalty layer (correctly) but the IP-ban layer has no analogous bypass.

### Pair 2: penalty applies to (score, connection_status, banned_peers)

| Mutation path | score | connection_status | banned_peers |
|---|---|---|---|
| `process_penalty(Fatal) on non-trusted` | ✓ → min | → Banned | += 1 (if not already banned) |
| `process_penalty(Severe) on non-trusted` | ✓ → score - 10 | possibly → Banned (if crosses threshold) | possibly += 1 |
| `Peer::heartbeat` (decay) | ✓ → score * decay | possibly → Unbanned (if crosses up) | possibly -= 1 |
| `make_trusted` (promotion) | ✓ → max | unchanged | unchanged (but remove_validator_ip on IP set) |

**GAP-2: make_trusted overwrites score but does NOT reset connection_status.** If a peer is in `ConnectionStatus::Banned` and gets promoted to committee, `make_trusted` only resets the score. The `update_connection_status(.., Unbanned)` call in `promote_committee_member` IS what flips the connection_status. But the path goes through `handle_unbanned_transition` (all_peers.rs:680) which for `ConnectionStatus::Banned`:
- sets status to Disconnected (NOT Connected)
- calls `remove_banned_peer` (decrements counters)
- emits `PeerAction::Unban(ips)`

So after promotion, the peer is `Disconnected` (no live connection) AND `is_trusted` AND in `current_committee`. The peer needs to actively reconnect via dial or the next inbound connection. If the swarm-level connection was severed when ban happened, the peer is silent until a kad/dial cycle re-establishes. **This is the gap window where consensus can stall.**

### Pair 3: bls_index ↔ current_committee_keys consistency

| Mutation path | bls_index updated? | current_committee_keys updated? |
|---|---|---|
| `add_trusted_peer` | ✓ (via rebind_bls) | ✗ |
| `upsert_peer` first call | ✓ | ✓ IF current_committee_keys[bls] == Some(&None) or Some(&Some(other)) |
| `upsert_peer` second call same bls | ✓ (re-rebinds, no change) | unchanged (already Some(&Some(self))) |
| `new_epoch` (clear) | ✗ | ✓ (clear) |
| `new_epoch` (per resolved) | n/a (read-only) | ✓ (via promote_committee_member) |
| `new_epoch` (per unresolved) | n/a | ✓ (insert None) |
| `remove_peer` | ✓ (drops bls if pointing to this pid) | ✗ |

**GAP-3: remove_peer does NOT clear current_committee_keys.** If a peer is removed by `prune_banned_peers` or `prune_disconnected_peers` (e.g., not a protected peer), and that peer's BLS was in `current_committee_keys` as `Some(&Some(pid))`, the map still says "resolved" even though bls_index no longer has the entry. The next lookup via `auth_to_peer(bls)` returns None. The `current_committee` still contains the (dead) PeerId. `is_peer_validator(dead_pid)` returns true. Any reconnect to the dead PeerId from a new endpoint (e.g., reverse-direction) would be treated as a validator. Subtle drift.

In practice, `is_protected` would skip such a peer (still in current_committee), so the eviction shouldn't happen. INV-045 protects us. UNLESS the peer was added to current_committee, then removed by some path... but only upsert_peer's rotation arm removes from current_committee, and that path explicitly calls promote on the new PeerId immediately.

Verdict: GAP-3 closed by INV-045 in practice. LOW.

### Pair 4: temporarily_banned ↔ peers[pid].reputation (Appendix A7)

| Mutation path | temporarily_banned | reputation |
|---|---|---|
| `apply_peer_action(Ban)` | insert | unchanged (already Banned via process_penalty) |
| `apply_peer_action(Disconnect)` | insert | already Disconnected |
| `apply_peer_action(DisconnectWithPX)` | insert | (no change — peer was not penalized, just exceeded count) |
| `apply_peer_action(Unban)` | nothing | restored by handle_unbanned_transition |
| `unban_temp_banned_peers` (heartbeat) | remove (TTL expired) | unchanged |
| `Peer::heartbeat` (decay) | nothing | possibly Unbanned |
| `new_epoch` | remove (per committee member, line 606) | unchanged at this point; promote runs separately |

**GAP-4 (matches A7):** Reputation can be Trusted (committee, score max) while temporarily_banned still has the entry. The `peer_banned` ORs them so reads via `peer_banned` get the right answer. But `auth_to_peer` (no consultation of temporarily_banned) returns a peer that may currently be temp-banned for dial purposes. The dial would then fail. INV-013 documented this is intentional but it can cause confusing logs.

LOW concern, already documented.

### Pair 5: disconnected_peers counter ↔ actual count of disconnected peers

| Mutation path | counter change |
|---|---|
| handle_disconnected_transition: Unknown/Connected/Dialing → Disconnected | += 1 (unchecked) |
| handle_disconnected_normal: Disconnecting{!banned} → Disconnected | += 1 (unchecked) |
| handle_unbanned_transition: Banned → Disconnected | += 1 (saturating_add) |
| handle_connected_transition: Disconnected → Connected | saturating_sub |
| handle_dialing_transition: Disconnected → Dialing | saturating_sub |
| handle_disconnecting_transition: Disconnected → Disconnecting | saturating_sub |
| handle_banned_transition: Disconnected → Banned | saturating_sub |
| prune_disconnected_peers: remove_peer | saturating_sub per removal |
| Peer::default initialization | 0 |

**GAP-5: asymmetric overflow/underflow protection.** Decrement is saturating. Increment isn't. If a state transition is processed twice for the same peer (e.g., a libp2p event delivered twice), the counter grows. If the actual count is correct (`peer.connection_status` is the single source of truth), the counter is wrong. Triggers premature `prune_disconnected_peers` activity.

I don't see a path that double-processes a transition under nominal operation. But there's no overflow protection — if I'm wrong, the counter grows unboundedly until usize::MAX.

LOW concern (no double-processing path identified), but the asymmetry is a code smell. Tighten to `saturating_add` on the +=1 sites.

## Feynman-enriched targets cross-reference

| Feynman SUSPECT/VULNERABLE | State gap confirmation | Cross-feed insight |
|---|---|---|
| F-1 (unresolved-CVV window penalty) | GAP-2 (promote returns Disconnected) | A peer caught in this window ends up unbanned-but-disconnected after promotion, requires re-dial. If gossipsub blacklist has already been applied (consensus.rs:1140), it persists until Unbanned event is consumed. Cross-layer race. **HIGH severity.** |
| F-2 (orphan accumulation) | (no coupled-state gap) | Memory growth bounded by max_peers aggregate, but orphans take slots from useful peers. **MEDIUM.** |
| F-3 (next_dial_request no re-gate) | GAP-2 (sort of — the dial reaches swarm) | Documented A2. **LOW.** |
| F-4 (kad num_records underflow) | (state-only, no Feynman context) | Underflow on race with `evict_expired_records`. **MEDIUM.** |
| F-5 (num_in/num_out u8 wrap) | (no coupled-state gap, internal counter) | A8 documented. **LOW.** |
| F-6 (Observer pruning) | (gap NOT in invariants.md) | The peer layer is unaware of NodeMode. Observers must remain discoverable for state-sync, but are pruned identically to regular peers. **HIGH (per user direction).** |
| F-7 (GossipsubNotSupported = Fatal) | (no coupled gap, just penalty severity) | INV-002 protects CVVs but not NVVs/observers. **MEDIUM.** |
| F-8 (IP-ban THRESHOLD=1 + shared NAT) | GAP-1 (no IP-ban bypass for CVVs) | Two paths: peer-level ban (INV-002 protects) and IP-level ban (no protection). **MEDIUM.** |
| F-9 (disconnected_peers unchecked +=) | GAP-5 (asymmetric protection) | No clear double-counting path under nominal flow. **LOW.** |
| F-10 (restart load + immediate penalty window) | GAP-2 (peer in non-trusted state pre-new_epoch) | A peer loaded from kad on restart is default-scored, NOT trusted, until first new_epoch runs. Window is small (startup-only) but a single Fatal penalty in this window can DoS a committee member. **MEDIUM.** |
| F-11 (open_epoch_pack 30s timeout no retry) | (cross-crate gap) | Node terminates on sustained partition. **LOW (operational).** |
| F-12 (log spam under protected overshoot) | INV-045 accepted | Documented intentional. **LOW.** |

## Net new findings for Phase 4 cross-feed

- **CF-1:** GAP-2 + F-1 + F-7 cross-feed: an unresolved CVV caught in a GossipsubNotSupported event is penalized Fatal, marked Banned, kad+gossipsub blacklist propagate via PeerEvent::Banned, THEN kad resolution lands and promote_committee_member tries to recover. Sequence: ban-propagate happens FIRST (via events queue), Unban event piggy-backs after promote_committee_member. The blacklist clears, but kad routing for this peer is gone (remove_peer was called on the Banned event consumer at consensus.rs:1142). Until a kad GetClosestPeers walks back to this validator, no dial possible. **Self-DoS for entire epoch.**

- **CF-2:** F-4 (kad underflow) + F-10 (restart load) cross-feed: on restart, KadStore loads N records, num_records = N. If any are TTL-expired and get evicted at first put, evict path uses saturating. If libp2p-kad's own TTL job fires `remove` for the same expired record (legitimate behavior), num_records -= 1 underflow. Restart-after-many-expirations is a higher-likelihood trigger.

- **CF-3:** F-8 (IP-ban) + F-6 (Observer): an observer behind a shared-NAT validator is doubly vulnerable. The observer can be IP-banned by sibling-peer's ban AND has no protection from pruning. State-sync stalls.
