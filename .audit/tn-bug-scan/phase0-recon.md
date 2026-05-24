# Phase 0 — Bug Recon

## Ranked priority targets

### P0 (highest priority — multiple categories)

| # | File:line(s) | Categories | Why |
|---|---|---|---|
| 1 | `all_peers.rs:982-1019` `promote_committee_member` | state-atomicity, consensus | Single funnel for `(current_committee, current_committee_keys, is_trusted)` triple. Window between new_epoch unresolved and kad resolution is the highest-value bug source. INV-046, INV-047. |
| 2 | `all_peers.rs:858-893` `collect_excess_peers` + `:912-973` `prune_*_peers` | state-atomicity, consensus | INV-045 protection. The skip-and-continue keeps protected peers but the eviction count is now `<= excess`, leaving the cap exceeded. Documented as accepted overshoot but worth checking if downstream counters get desync'd. |
| 3 | `all_peers.rs:1033-1056` `new_epoch` | consensus, state-atomicity | Clears `current_committee` and `current_committee_keys` BEFORE walking the new committee. If anything panics between clear and re-insert, the node is committee-less. No-async, single-thread so safe BUT promote_committee_member's calls to update_connection_status are non-trivial. |
| 4 | `consensus.rs:1149-1155` `MissingAuthorities` handler | concurrency, error-propagation | One kad query per missing BLS — the queries can race with later new_epoch events (next epoch arriving while this one's queries outstanding). `close_kad_query` at :1488 adds the resolved peer via `add_known_peer` even after the epoch that requested it has ended. |
| 5 | `kad.rs:380-391` `RecordStore::remove` | panic-surface | `self.num_records -= 1` without saturating_sub. |
| 6 | `peer.rs:241-251` `register_incoming/outgoing` | concurrency (Appendix A8) | `num_in`/`num_out` increment without per-peer cap. |
| 7 | `behavior.rs:160-193` `poll` | concurrency, consensus | `next_dial_request` no re-gate (Appendix A2). Ban window between dial_peer push and pop. |

### P1 (single category, clear failure mode)

| # | File:line(s) | Categories | Why |
|---|---|---|---|
| 8 | `state-sync/lib.rs:64-95` `spawn_state_sync` + observer pruning | concurrency | Observer is NOT protected from prune_connected_peers in peers/. State-sync deps on discoverability. |
| 9 | `node/manager/node/epoch.rs:438-511` `open_epoch_pack` | concurrency, error-propagation | Pre-dial loop (line 483-494) calls `new_epoch(committee_keys)` if `connected_peers_count() == 0` AND then dials each committee BLS asynchronously. The await at line 499 (`record_by_epoch_with_timeout(30s)`) waits while these dials are in flight — timeout failure means epoch fails to start. |
| 10 | `cache.rs:55,72` `expect("Key is not new")`, `expect("Key must exist")` | panic-surface | Internal invariants but reachable if caller misuses. |
| 11 | `consensus.rs:824` `GossipsubNotSupported → Penalty::Fatal` | error-propagation, consensus | Fatal penalty on legitimate unsupported protocol negotiation. A peer that doesn't subscribe to gossipsub for any reason is killed. |
| 12 | `banned.rs:20,100,115` `BANNED_PEERS_PER_IP_THRESHOLD = 1` | consensus | `>` comparison with threshold 1 means count of 2+ bans the IP. Validators sharing NAT exit IP could be banned by sibling. |
| 13 | `all_peers.rs:529,572,687` `disconnected_peers += 1` | state-atomicity | Non-saturating addition; saturating_sub on remove. Counts can desync upward and never recover. |
| 14 | `score.rs:103-106` `add` | determinism | f64 arithmetic, clamp. Should be fine; flag for verification. |
| 15 | `peer.rs:198-205` `apply_penalty trusted short-circuit` | consensus | INV-002. Verify no bypass path. |
| 16 | `consensus.rs:284-299` `KadStore restart record load` | state-atomicity | On restart, iterates `kad_store.records()` and calls `add_known_peer` for each. If a record was banned in previous lifetime, the ban is lost (Appendix A3) and the peer re-enters as default-scored. |

## Hotspot heat-map

| Region | Concurrency | Determinism | Consensus | State-atomicity | Panic-surface | Fork-risk | Error-propagation |
|---|---|---|---|---|---|---|---|
| `peers/all_peers.rs` | **H** | M | **H** | **H** | M | L | L |
| `peers/peer.rs` | L | L | M | **H** | L | L | L |
| `peers/manager.rs` | **H** | M | M | **H** | M | L | M |
| `peers/banned.rs` | L | L | **H** | M | L | L | L |
| `peers/score.rs` | L | M | M | M | M | L | L |
| `peers/cache.rs` | L | L | L | L | **H** | L | L |
| `peers/behavior.rs` | **H** | L | M | M | L | L | M |
| `kad.rs` | M | M | L | M | **H** | L | M |
| `consensus.rs` | **H** | M | M | **H** | M | L | **H** |
| `state-sync/lib.rs` | **H** | L | M | M | L | L | M |
| `state-sync/consensus.rs` | **H** | L | L | M | L | L | **H** |
| `state-sync/epoch.rs` | M | L | M | M | L | L | M |
| `node/manager/node/epoch.rs` | **H** | L | M | M | L | L | M |
