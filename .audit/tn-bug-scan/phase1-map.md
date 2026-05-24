# Phase 1 — Function-state Matrix and Coupled-state Map

## Function-state matrix (peers/)

| Function | Reads | Writes | Coupled pairs touched |
|---|---|---|---|
| `AllPeers::rebind_bls` | `peers`, `bls_index` | `bls_index`, `peers[old_pid].bls_public_key` (via clear_bls) | (bls_index, bls_public_key) |
| `AllPeers::add_trusted_peer` | — | `peers[pid]`, `bls_index`, `banned_peers` | (bls_index, bls_public_key), (banned_peers, ip-counts) |
| `AllPeers::upsert_peer` | `current_committee_keys`, `peers[pid]` | `peers[pid]`, `bls_index`, `current_committee`, `current_committee_keys`, `peers[pid].is_trusted` (via promote_committee_member), `banned_peers` | (bls_index, bls_public_key), (current_committee, current_committee_keys, is_trusted) |
| `AllPeers::process_penalty` | `peers[pid]` | `peers[pid].score`, `peers[pid].connection_status`, `banned_peers` (via downstream handle_*) | (score, connection_status), (connection_status, banned_peers) |
| `AllPeers::heartbeat_maintenance` | `peers` | `peers[pid].connection_status`, `peers[pid].score`, `banned_peers` (via downstream) | (score, reputation), (reputation, connection_status) |
| `AllPeers::update_connection_status` | `peers[pid]` | `peers[pid].connection_status`, `disconnected_peers`, `banned_peers` | (connection_status, disconnected_peers), (connection_status, banned_peers) |
| `AllPeers::register_disconnected` | `peers[pid]` | `peers[pid].connection_status`, `disconnected_peers`, `banned_peers`, calls `prune_disconnected_peers` + `prune_banned_peers` | All connection-state + ban couplings |
| `AllPeers::collect_excess_peers` | `peers` (RO) | none | none |
| `AllPeers::prune_banned_peers` | `peers`, `banned_peers` | `peers` (remove_peer), `banned_peers`, `bls_index` (via remove_peer) | (peers, banned_peers, bls_index) |
| `AllPeers::prune_disconnected_peers` | `peers`, `disconnected_peers` | `peers`, `disconnected_peers`, `bls_index` (via remove_peer) | (peers, disconnected_peers, bls_index) |
| `AllPeers::promote_committee_member` | `peers[pid]` | `current_committee`, `current_committee_keys`, `peers[pid].is_trusted`, `peers[pid].connection_status` (via update_connection_status), `banned_peers` (via remove_validator_ip) | (current_committee, current_committee_keys, is_trusted) |
| `AllPeers::new_epoch` | `bls_index` | `current_committee` (clear+repopulate), `current_committee_keys` (clear+repopulate); delegates to promote_committee_member per resolved BLS | (current_committee, current_committee_keys) |
| `PeerManager::new_epoch` | `peers` (via auth_to_peer) | `temporarily_banned`, delegates to AllPeers::new_epoch, pushes MissingAuthorities event | (temporarily_banned, current_committee) |
| `PeerManager::dial_peer` | `peers[pid].connection_status` | `dial_requests` | (dial_requests, connection_status) |
| `PeerManager::process_penalty` | — | delegates to AllPeers + apply_peer_action | (score, action, banned_peers, temporarily_banned) |
| `PeerManager::apply_peer_action` | — | `temporarily_banned`, `events`, calls process_ban | (temporarily_banned, events) |
| `PeerManager::heartbeat` | — | delegates to AllPeers, then `prune_connected_peers`, `unban_temp_banned_peers`, `discovery_heartbeat` | All |
| `PeerManager::prune_connected_peers` | `peers` (via connected_peers_by_score_and_routability), `current_committee`, `peers[pid].is_trusted` | `events` (via disconnect_peer) | (connection_status, events) |
| `PeerManager::discovery_heartbeat` | `discovery_peers` | `discovery_peers`, `events` | (discovery_peers, dial_requests) |
| `PeerManager::add_known_peer` | — | delegates to AllPeers::upsert_peer + apply_peer_action | (bls_index, current_committee_keys, is_trusted) |
| `PeerManager::find_authorities` | `peers` (via contains_bls) | `events` | (events) |
| `Peer::apply_penalty` | `is_trusted` | `score` | (is_trusted, score) |
| `Peer::heartbeat` | `is_trusted`, `score`, `connection_status` | `score` | (score, reputation) |
| `Score::apply_penalty` | — | `telcoin_score`, `aggregate_score`, `last_updated` | (telcoin_score, aggregate_score, last_updated) |
| `Score::update_at` | `last_updated`, `telcoin_score` | `telcoin_score`, `last_updated`, `aggregate_score` | (telcoin_score, last_updated) |
| `BannedPeers::add_banned_peer` | `peer.known_ip_addresses` | `total`, `banned_peers_by_ip` | (total, banned_peers_by_ip) |
| `BannedPeers::remove_banned_peer` | `banned_peers_by_ip` | `total` (saturating_sub), `banned_peers_by_ip` (counter--) | (total, banned_peers_by_ip) |
| `BannedPeers::remove_validator_ip` | `banned_peers_by_ip` | `total` (saturating_sub), `banned_peers_by_ip` (full remove) | (total, banned_peers_by_ip) — see Bug-1 below |
| `BannedPeerCache::insert` | `map` | `map`, `list` | (map, list) |
| `BannedPeerCache::remove` | `map` | `map`, `list` | (map, list) |
| `BannedPeerCache::heartbeat` | `list` | `list`, `map` (drains expired) | (map, list) |

## Coupled-state dependency map

```
                           (bls_index)
                              ↕
                        (peers[pid].bls_public_key)
                              ↕ (rotation/orphan)
                        (peers[pid].is_trusted)
                              ↕
                        (current_committee_keys[bls])
                              ↕
                        (current_committee)
                              ↕ (only via promote_committee_member)
                        (peers[pid].connection_status)
                              ↕
                        (disconnected_peers count)
                              ↕
                        (banned_peers.total)
                              ↕
                        (banned_peers.banned_peers_by_ip)
                              ↕
                        (temporarily_banned cache)
                              ↕
                        (peers[pid].score)
                              ↕
                        (peers[pid].reputation())  ← pure function of score
```

## Cross-crate couplings

- **PeerManager ↔ ConsensusNetwork (consensus.rs):**
  - PeerEvent::Banned → consensus.rs:1140 → `gossipsub.blacklist_peer`, `kademlia.remove_peer`. Two-step propagation; window between push and consume (Appendix A4).
  - PeerEvent::MissingAuthorities → consensus.rs:1149 → `kad.get_record` for each BLS. The record arrives later, calls `add_known_peer`, which calls `upsert_peer`. Coupling: the local epoch must STILL be the same when this arrives, else the resolution targets the wrong epoch's committee.
  - PeerEvent::PeerConnected → consensus.rs:1101 → push our NodeRecord direct, add address to kad. If banned at this exact moment, the disconnect at 1112 prevents add. Race window narrow.

- **PeerManager ↔ KadStore (kad.rs):**
  - `add_known_peer` reads-side has no atomicity with KadStore's own `put`. The peer record in `bls_index` exists before its NodeRecord is persisted in the KadStore. Restart sequence: load from kad → call add_known_peer.

- **PeerManager ↔ state-sync (state-sync/*.rs):**
  - state-sync calls `network.request_consensus` which goes through `peer_to_bls` (consensus.rs:1177). If peer was pruned between the request submission and the response, the response is silently dropped at `try_send` boundary at consensus.rs:1180.
  - No direct coupling: state-sync has no callbacks into peers/. The peer manager is opaque to state-sync.

- **PeerManager ↔ Node Epoch Manager (node/manager/node/epoch.rs):**
  - `init_network_for_epoch` (epoch.rs:1296) sequences `add_bootstrap_peers` then `new_epoch`. The two are separate `.await` calls on the network handle, so a race against the network task event loop is possible. Bootstrap peers may be in-flight upsert when new_epoch runs and sees an incomplete bls_index.

## Coupling hypothesis (Q0.5)

1. **Hyp-1:** `current_committee_keys[bls] == None` window — a CVV is recognised by consensus but not yet by peer manager. Eviction during this window evicts a soon-to-be committee member. (Confirmed in domain-patterns Template T1.)
2. **Hyp-2:** Rotation orphan accumulates with `is_trusted == true` and `bls_public_key == None`. Aggregate `max_peers` is bounded, but the orphans take slots from legitimate peers. (Confirmed in Appendix A10.)
3. **Hyp-3:** `Penalty::Fatal` on `GossipsubNotSupported` (consensus.rs:824) is excessive — a real production cause might be a transient protocol-negotiation glitch, not a malicious peer.
4. **Hyp-4:** `peer_banned` ORs `temporarily_banned` with `peers.peer_banned`. The two are not updated atomically (Appendix A7). Specific read sites bypass the OR — needed audit.
5. **Hyp-5:** `disconnected_peers` counter is `saturating_sub` on decrement but unchecked `+= 1` on increment. If the state-machine ever double-counts an increment, the counter accumulates upward and never recovers. (Confirmed; see Bug B1 below.)
6. **Hyp-6:** Restart erasure path loads kad records into peer manager (consensus.rs:284-299) without re-applying any ban state. A previously-banned peer reconnects as default-scored.
7. **Hyp-7:** IP-ban THRESHOLD = 1 means count > 1 triggers ban; one validator's ban does NOT poison the IP — but TWO peers from the same IP do. With NAT-shared validators this is a real concern.
