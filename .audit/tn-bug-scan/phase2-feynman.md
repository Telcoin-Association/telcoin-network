# Phase 2 — Feynman Interrogation (full)

For every priority target from Phase 0, ask: "WHY does this line work? What ordering does it assume? What goes wrong if the assumption fails?"

## P0-1 — `promote_committee_member` (all_peers.rs:982-1019)

Verdict: **SUSPECT**

The function inserts into `current_committee` and `current_committee_keys` BEFORE checking the connection status. If `ensure_peer_exists` panics (it does not — but the path through `update_connection_status → handle_unbanned_transition` can match unexpected variants). If the function is called twice (once via `new_epoch` and once via `upsert_peer` for the same BLS in the same tick), the second call re-inserts identically — idempotent.

**Suspicion:** the rotation arm at `upsert_peer:175-183` removes `other_pid` from `current_committee` BEFORE calling `promote_committee_member`. If `other_pid` was the local node itself (impossible — self can't be in `peers`), no impact. If `other_pid` had `is_trusted == true` from a PRIOR epoch's promotion, it stays trusted (orphan). The orphan is now NOT in `current_committee` but IS trusted, and remains in `peers`. **Across N rotations the trusted-orphan set grows without bound** — Appendix A10 confirmed.

Targets for Phase 3 state-trace: `(current_committee, current_committee_keys, is_trusted)` triple after every code path that mutates any one.

## P0-2 — `collect_excess_peers` + `prune_*_peers` (all_peers.rs:858-973)

Verdict: **SOUND** with documented overshoot.

Re-read:
```rust
let mut excess_peers = BinaryHeap::with_capacity(excess);
for (peer_id, peer) in &self.peers {
    if self.is_protected(peer_id, peer) { continue; }
    if let Some(instant) = filter(peer.connection_status()) {
        let entry = (Reverse(instant), *peer_id, peer.known_ip_addresses().collect());
        if excess_peers.len() < excess {
            excess_peers.push(entry);
        } else if let Some(current_max) = excess_peers.peek() {
            if entry.0 < current_max.0 {
                excess_peers.pop();
                excess_peers.push(entry);
            }
        }
    }
}
```

`excess_peers` is a min-heap that grows up to `excess` and then keeps the OLDEST `excess` entries among the unprotected. The "off-by-one" the user hint mentioned: is this correctly `excess` or should it be `excess + protected_count`?

`excess = total - max`. `total` is `banned_peers.total()` (or `disconnected_peers`). `max` is `max_banned_peers` (or `max_disconnected_peers`). Both totals INCLUDE protected peers.

Example: total = 10 banned, max = 5, excess = 5. Among the 10, 2 are protected. The heap fills with 5 unprotected. We evict 5 unprotected. Final state: 5 - 5 = 0 unprotected banned + 2 protected banned = 2 banned. We are now BELOW the cap (2 < 5). 

Now: total = 10 banned, max = 5, excess = 5. Among the 10, 7 are protected. Only 3 unprotected. The heap fills with 3 (`excess_peers.len() == 3 < excess == 5`). We evict 3. Final: 0 unprotected + 7 protected = 7. We are ABOVE cap by 2. The warn-log at all_peers.rs:927-933 says "capped by protected peers", and we accept the overshoot (INV-045).

**No off-by-one.** The math is correct. The user hint about "evict `excess + K`" misread the loop — the loop NEVER evicts more than `excess`. It evicts MIN(excess, unprotected_count).

**Question:** what if `excess` itself is large and there are exactly `excess` protected peers? The heap is empty, we evict 0. Total stays at total. The next heartbeat: still total banned (no removals), still excess, still 0 evictions. We could end up with EVERY heartbeat warn-logging the cap until the protected peers stop being banned.

**Verdict update: SUSPECT** — not a bug per se, but log spam under sustained pressure. INV-045 accepted the overshoot; the noise is the cost.

## P0-3 — `new_epoch` (all_peers.rs:1033-1056)

Verdict: **SUSPECT**

```rust
self.current_committee.clear();
self.current_committee_keys.clear();
for bls_key in committee {
    let Some(peer_id) = self.bls_index.get(&bls_key).copied() else {
        warn!(...);
        self.current_committee_keys.insert(bls_key, None);
        unresolved.push(bls_key);
        continue;
    };
    if let Some(action) = self.promote_committee_member(bls_key, peer_id) {
        actions.push((peer_id, action));
    }
}
```

**Suspicion 1:** Between `clear()` and the loop, there's no `await` (single-threaded), so the consensus task running this can't be preempted by another peer-manager task. Safe.

**Suspicion 2:** committee is a `HashSet<BlsPublicKey>` — iteration order is non-deterministic. Does the order matter? `current_committee` and `current_committee_keys` are independent of order. `promote_committee_member` is independent of order. The `unresolved` Vec is order-dependent — but downstream consumers (`MissingAuthorities` event) iterate it to spawn one kad query per key, where order doesn't affect correctness. Safe.

**Suspicion 3:** What happens to a previously-trusted-orphan whose BLS rotated out? Suppose epoch N committee included V_a with PeerId_a; epoch N+1 has V_a with PeerId_b (rotation). When new_epoch fires for N+1: it sees bls_v in `bls_index` (from the rotation that already ran via upsert_peer), promotes PeerId_b. PeerId_a's `is_trusted` is NEVER cleared. Across many rotations, the orphan list grows. **Appendix A10 confirmed.**

**Suspicion 4:** The unresolved committee window. In epoch N+1, peer V_a is unknown (not in bls_index). `current_committee_keys[v_a] = None`, unresolved.push(v_a). MissingAuthorities event fires. kad queries take seconds. **In that window:**
- `is_peer_validator(any_pid)` — `current_committee` only contains resolved peers. V_a's PeerId is unknown to local node.
- IF V_a is connected inbound (e.g. dialed us before its NodeRecord propagated), V_a's PeerId is in `peers` but NOT in `current_committee`. `peer_is_important` returns false. `is_protected` returns false. V_a is eligible for `prune_connected_peers`.
- IF a `Penalty::Fatal` is dispatched to V_a's PeerId in this window (e.g. via gossipsub `GossipsubNotSupported`), `process_penalty` checks `is_trusted` — V_a is NOT trusted yet — and applies the penalty. V_a is BANNED.
- When kad later resolves V_a's record, `upsert_peer` runs. `current_committee_keys[bls_v] == Some(&None)` → `should_promote = true` → `promote_committee_member` runs. Inside: `ensure_peer_exists` finds V_a Banned. `handle_unbanned_transition` matches the Banned arm at all_peers.rs:680 → emits `Unban` action. V_a's score is left at min (Penalty::Fatal forced it to min, see score.rs:93). After `make_trusted` at peer.rs:341, the score IS replaced with `Score::new_max`. So eventually V_a is recovered — IF the unban event propagates AND IF V_a tries to reconnect AND IF the local node hasn't already kicked V_a out of the swarm and removed the kad routing entry.

**The window between Fatal-penalty-during-unresolved and kad-resolution is the gap.** The peer is silently penalised, the swarm-level state machine processes the disconnect, gossipsub blacklists, kad removes the routing entry — all BEFORE the kad resolution lands. When the resolution arrives, the peer is marked trusted again, but the gossipsub blacklist persists until `PeerEvent::Unbanned` propagates (consensus.rs:1147). The Unbanned event IS pushed by promote_committee_member's update_connection_status call — so the chain is correct, but the ordering is racy.

Target for Phase 3 state-trace: gossipsub blacklist state, kad routing-table state, `current_committee` membership, all at the moment a `GossipsubNotSupported` event fires on an unresolved-committee BLS.

## P0-4 — `MissingAuthorities` consumer (consensus.rs:1149-1155)

Verdict: **SUSPECT**

```rust
PeerEvent::MissingAuthorities(missing) => {
    for bls_key in missing {
        let key = kad::RecordKey::new(&bls_key);
        let query_id = self.swarm.behaviour_mut().kademlia.get_record(key);
        self.kad_record_queries.insert(query_id, bls_key.into());
    }
}
```

**Suspicion:** what if a previous `MissingAuthorities` query is still in-flight when a new epoch fires its own `MissingAuthorities`?
- `self.kad_record_queries` is keyed by `QueryId`. The previous query has a unique QueryId from libp2p-kad's monotonic counter. The new query has a different ID.
- Both run to completion concurrently. Both close via `close_kad_query`.
- `close_kad_query` (line 1488-1498) calls `add_known_peer(query.request, ...)`. The request is the BLS key. If both queries are for DIFFERENT BLS keys (one from epoch N, one from epoch N+1), they don't collide.
- **If a node skips epoch N and goes from N-1 to N+1**, the epoch-N-MissingAuthorities is still in-flight while epoch-N+1-MissingAuthorities fires. The epoch-N resolution might land AFTER new_epoch(N+1) cleared `current_committee_keys`. When the resolution arrives, `upsert_peer` runs `should_promote = match current_committee_keys.get(&bls_v) { Some(&None) => true, ... }`. Since epoch N+1 cleared this map, `current_committee_keys.get(&bls_v)` is None (not "Some(None)"). The match fall-through is `_ => false`. **No promotion.** The peer is correctly upserted into bls_index without becoming a committee member. Safe-by-default.

**Sub-suspicion:** does `close_kad_query` ever land for a BLS that ISN'T in `current_committee_keys` but IS in `bls_index`? `add_known_peer` calls `upsert_peer` which calls `rebind_bls` first. If `bls_index[bls]` already exists pointing to a DIFFERENT PeerId (rotation across processes), `rebind_bls` orphans the old one. If the BLS is in `current_committee_keys` as `Some(&None)`, promotion happens. The path is consistent.

Verdict update: **SOUND** (matched against all cases). The kad-resolution arrival ordering is handled correctly.

## P0-5 — `RecordStore::remove` (kad.rs:380-391)

Verdict: **VULNERABLE**

```rust
fn remove(&mut self, k: &RecordKey) {
    let key = self.key_to_hash(k);
    if match self.kad_type {
        KadStoreType::Primary => self.db.remove::<KadRecords>(&key),
        KadStoreType::Worker => self.db.remove::<KadWorkerRecords>(&key),
    }
    .is_ok()
    {
        // Record was removed so dec num_records.
        self.num_records -= 1;
    }
}
```

`self.num_records -= 1` is NOT saturating. If `num_records == 0` and `remove` is called (and `db.remove` returns `Ok(())` for a non-existent key — which most KV stores do), this underflows.

**Can `db.remove` return Ok for non-existent keys?** Looking at the contract: `tn_storage::Database::remove` returns `Result<(), Error>`. In tn-storage's REDB-backed Database impl, `Database::remove` typically deletes if present and returns Ok regardless of presence. The MemDatabase test impl likely behaves the same. So yes, double-remove of an already-removed key returns Ok and triggers the underflow.

**Trigger sequence:**
1. `put(record_X)` — num_records = 1.
2. `evict_expired_records()` fires, removes record_X — num_records = 0.
3. libp2p-kad's TTL job calls `remove(record_X.key)` (it doesn't know about eviction) — `db.remove` returns Ok (key absent → vacuous OK), `num_records -= 1` underflows to `usize::MAX`.
4. Next `put` checks `num_records >= max_records` → `usize::MAX >= max_records` → true → tries `evict_expired_records` again → no rows to evict → returns `Err(MaxRecords)` permanently.

The store is now BROKEN for the rest of this process. Every kad put fails. New peer records cannot be cached.

Failure mode: liveness-degradation (state-corruption indirectly).

Also `evict_expired_records` uses `saturating_sub` (line 249) — protected. But the `remove` path does not. Asymmetric defenses.

Note: `add_provider` similarly increments `num_providers += 1` but `remove_provider` (line 493) also uses `num_providers -= 1` unchecked. Same vulnerability for provider records.

## P0-6 — `register_incoming/outgoing` per-peer cap absent (peer.rs:241-251, Appendix A8)

Verdict: **VULNERABLE** (documented as Appendix A8)

```rust
match &mut self.connection_status {
    ConnectionStatus::Connected { num_in, .. } => *num_in += 1,
    ...
}
```

No cap. A single peer can open many subconnections; they all count as one in `connected_peer_ids` (HashMap keyed by PeerId) but consume swarm slots. The aggregate `max_peers()` doesn't catch this (swarm slot count > peer count).

`num_in` is `u8` (status.rs:15 — `num_in: u8`). Hits 255 → next increment wraps to 0. INV-019 (`num_in + num_out > 0 iff Connected`) breaks: the peer is Connected but counters are 0, so:
- `connected_peer_ids` correctly returns the PeerId (the filter uses `is_connected()` which matches `Connected{..}` regardless of counters).
- BUT when counters were 0 and a subconnection closes, the state machine has no way to detect "all subconnections closed" because the count was wrong.

Lower-severity than the kad underflow, but it IS a bounded-input → unbounded-state pattern.

## P0-7 — `next_dial_request` no re-gate (Appendix A2)

Verdict: **VULNERABLE** (documented)

```rust
pub(super) fn next_dial_request(&mut self) -> Option<DialRequest> {
    self.dial_requests.pop_front()
}
```

`dial_peer` filters at enqueue. Between push and pop, a `process_penalty` can fire and ban the peer. The pop returns the request, the swarm dials, the established-outbound-connection callback rejects with "peer is banned" (behavior.rs:104). Error log + dial attempt wasted.

Documented in invariants.md. Recommended fix already documented (re-check at dequeue).

## P1-8 — Observer pruning (state-sync/lib.rs:64-95 ↔ peers/manager.rs:490-524)

Verdict: **VULNERABLE**

state-sync only spawns its background tasks when `NodeMode == Observer || CvvInactive`. The state-sync tasks depend on peer discovery to fetch consensus headers and pack files. The peer manager has NO visibility into NodeMode. `is_peer_validator` returns true only for CVVs. Observers and CvvInactive nodes are pruned identically to any non-CVV peer.

The user direction is explicit: "observer pruning ... flag as bugs". This is a real cross-layer defect.

**Trigger sequence:**
1. Observer O syncing from peer V (a CVV).
2. O has reached `target_num_peers`, V is at the bottom of the score list (V has high latency or whatever).
3. Heartbeat fires `prune_connected_peers`. V is in `current_committee` → skipped. But the observer ITSELF can be pruned BY V's peer manager if O lands at the bottom of V's score list.
4. V disconnects O. O retries via state-sync's `request_consensus` which loops on tokio::time::sleep(5s) (state-sync/consensus.rs:88). O drains its peer pool and stalls.
5. If O has only V as its primary upstream, O's state-sync stalls indefinitely.

Failure mode: liveness-degradation. Observer cannot follow consensus.

## P1-9 — `open_epoch_pack` pre-dial deadlock avoidance (epoch.rs:438-511)

Verdict: **SOUND** under nominal conditions, **SUSPECT** under sustained partition.

The function pre-dials committee peers if `connected_peers_count() == 0` to avoid the deadlock (described in the source comment at line 478-480). The await at line 499 waits 30 seconds for the missing epoch record.

If no peer is reachable within 30 seconds (partition, all-CVVs-down): returns Err and `run_epoch` propagates the error. Node fails to start the epoch. **No retry inside open_epoch_pack.** The caller (run_epoch) would need to retry, but the path I traced bubbles the error up to `node_run` which terminates.

Failure mode: liveness-degradation under sustained network partition. Node cannot start until partition heals AND operator restarts.

## P1-10 — `cache.rs:55,72` expects

Verdict: **SOUND** under normal flow (LOW)

`BannedPeerCache::insert` (line 47-64) calls `self.map.insert` first. If insert returns false (key already present), goes to the else branch. `let position = self.list.iter().position(|e| e.key == key).expect("Key is not new")`. If the invariant (`map ↔ list`) is broken — list missing a key that map has — this panics. The `check_invariant` at line 119 verifies this in test builds only.

Path where this could break: the map has the key, but the list doesn't. Only way to insert into map without into list is if `insert` returned (already in map) and went to the else branch, where it removes from list and re-pushes. The remove uses position(key) and `expect("Position is not occupied")`. If the remove is called and the linear scan returns `None`, we panic.

Concurrent access? `BannedPeerCache` is owned by `PeerManager` (single-threaded). Safe under normal flow.

Verdict: **SOUND** (LOW concern — internal invariant only).

## P1-11 — `GossipsubNotSupported → Penalty::Fatal` (consensus.rs:824)

Verdict: **VULNERABLE**

```rust
GossipEvent::GossipsubNotSupported { peer_id } => {
    trace!(...);
    self.swarm.behaviour_mut().peer_manager.process_penalty(peer_id, Penalty::Fatal);
}
```

`GossipsubNotSupported` fires when a peer connects without supporting the gossipsub protocol. In a homogeneous validator network, this should never happen for legitimate peers. BUT:
- During rolling upgrades or protocol version changes, a node might temporarily not advertise gossipsub.
- A new peer that hasn't yet completed protocol negotiation.
- An NVV (non-voting validator) that's purely consuming via state-sync without participating in gossip.

Fatal penalty = -100 score immediately. The peer is banned. INV-002 protects CVVs (trusted, no penalty applied) but NOT NVVs or observers (which are NOT trusted at the peer level).

Failure mode: liveness-degradation. NVV-style peers can be kicked off by protocol-quirk events.

## P1-12 — `BANNED_PEERS_PER_IP_THRESHOLD = 1` (banned.rs:20,100,115)

Verdict: **SUSPECT**

```rust
const BANNED_PEERS_PER_IP_THRESHOLD: usize = 1;
...
pub(super) fn ip_banned(&self, ip: &IpAddr) -> bool {
    self.banned_peers_by_ip.get(ip).is_some_and(|count| *count > BANNED_PEERS_PER_IP_THRESHOLD)
}
```

Threshold = 1, comparison is `>`. So count must be ≥ 2 for the IP to be banned. **One peer ban does NOT poison the IP.**

But in a 10-node committee scenario behind a shared NAT exit (validators V_a, V_b sharing IP_X):
- V_a banned: count[IP_X] = 1. IP not yet banned.
- Some random peer X (also through IP_X) gets banned for unrelated reasons: count[IP_X] = 2. **IP_X is now banned.**
- V_b's reconnect attempt: `peer_banned(V_b)` checks `peer.known_ip_addresses().any(ip_banned)` (all_peers.rs:762). True. V_b rejected.
- INV-033 only protects V_b from PRUNING (active eviction), NOT from CONNECTION ACCEPTANCE. There is no "is_protected" check in `handle_pending_inbound_connection` (behavior.rs:69) or `register_peer_connection` (manager.rs:425) for IP-based bans.

**Fix candidate:** Add a CVV-IP allowlist that bypasses IP-ban checks for committee members.

## P1-13 — `disconnected_peers += 1` non-saturating (all_peers.rs:529, 572, 687)

Verdict: **SUSPECT**

```rust
self.disconnected_peers += 1;
```
appears at all_peers.rs:529 (handle_disconnected_transition for Unknown/Connected/Dialing → Disconnected), :572 (handle_disconnected_normal), :687 (handle_unbanned_transition).

The decrement uses `saturating_sub` everywhere. If a state transition is ever processed twice (e.g., due to a libp2p event being delivered multiple times — INV-022 says register_disconnected is idempotent and the matches are correct, but `handle_disconnected_transition`'s `Unknown | Connected | Dialing → Disconnected` arm WILL fire if called twice from Unknown with a state that's been re-set to Unknown in between).

The peer's connection_status starts at Unknown (Default). Transition to Disconnected: +1. If state is reset to Unknown (no path does this in normal code — INV-018 says Unknown is the default initial state ONLY), then transition to Disconnected again: +1. Counter is now 2 for 1 actual peer.

I don't see a path that resets to Unknown after a peer is established. But under unhandled-variant evolution (e.g., a new ConnectionStatus variant added to status.rs and a transition handler that doesn't match it), the default-Unknown initialization could trigger an over-count.

Failure mode: silent-wrong-state. `prune_disconnected_peers` would think there are more disconnected peers than there actually are, triggers eviction earlier than expected. LOW-to-MEDIUM.

## P1-14 — `Score::add` f64 arithmetic + clamp

Verdict: **SOUND**

`telcoin_score + score` then `.clamp(min, max)`. f64 can lose precision but the clamp protects boundaries. f64 NaN propagation: `telcoin_score = f64::NAN` → `NaN + (-1.0) = NaN`. `f64::clamp(NaN, min, max)` returns NaN (NaN-input passthrough). Subsequent comparison `aggregate_score <= min_score_before_ban` would return `false` (NaN comparisons false), so the peer is NEVER banned. But: nothing in the code path produces NaN. The Score init is `default_score: f64 = 0.0` (network.rs:436). Decay multiplies by `exp(halflife_decay * dt)`, always positive and bounded. Penalty subtracts a constant. NaN paths impossible without operator misconfiguration.

## P1-15 — `apply_penalty` trusted short-circuit

Verdict: **SOUND** (INV-002 directly enforced)

```rust
pub(super) fn apply_penalty(&mut self, penalty: Penalty) -> Reputation {
    if !self.is_trusted {
        self.score.apply_penalty(penalty);
    }
    self.reputation()
}
```

The `is_trusted` short-circuit IS the protection. Trusted peers' scores stay at max. The `pub(super)` visibility on `apply_penalty` plus the orchestration through `process_penalty` makes this airtight (Appendix A6 notes the absence of allowlist at call-sites but is_trusted IS the call-site allowlist).

## P1-16 — KadStore restart record load (consensus.rs:284-299)

Verdict: **SUSPECT**

```rust
for record in kad_store.records() {
    match BlsPublicKey::from_literal_bytes(record.key.as_ref()) {
        Ok(key) => {
            let record: NodeRecord = decode(&record.value);
            behavior.peer_manager.add_known_peer(
                key,
                record.info.pubkey,
                record.info.multiaddrs,
            );
        }
        Err(error) => {
            error!(target: "network-kad", ?error, "Invalid/corrupt KAD DB store!");
        }
    }
}
```

On restart:
1. KadStore loads from DB. records() iter filters expired records (kad.rs:301-315).
2. For each non-expired record, `add_known_peer` is called.
3. `add_known_peer` → `upsert_peer` → indexes in `bls_index`. Default-scored. NOT trusted (no committee context yet — `current_committee_keys` is empty).
4. AFTER restart finishes, ConsensusNetwork::run → `provide_our_data` (line 441) but new_epoch is NOT yet called.
5. The first new_epoch will see all these BLS keys already in bls_index, promote them, set is_trusted = true.

**Between step 3 and step 5, the peers are NOT trusted.** If a `process_penalty` fires on any of them (e.g., a returning kad query, a gossipsub message arriving on a stale connection), the peer is penalized. Score state was wiped (Appendix A3 — restart clears in-memory ban state) so we start at default_score = 0.0. A Fatal penalty sets to min, peer banned. The committee member is banned BEFORE new_epoch runs to protect them.

**Restart-and-immediate-Fatal-penalty is the attack window for inducing a self-DoS.**

Failure mode: liveness-degradation, potential consensus-stall on small committees.

## Suspect/vulnerable target list for Phase 3

| ID | Target | Severity hypothesis | INV/Appendix |
|---|---|---|---|
| F-1 | promote_committee_member triple invariant during unresolved-CVV-window penalty | HIGH | INV-046, INV-047 |
| F-2 | Orphan accumulation with is_trusted preserved | MEDIUM | Appendix A10 |
| F-3 | next_dial_request no re-gate | LOW | Appendix A2 |
| F-4 | RecordStore::remove num_records underflow | MEDIUM | (no INV — Appendix-Ax extension) |
| F-5 | num_in/num_out u8 wrap, no per-peer cap | LOW | Appendix A8 |
| F-6 | Observer not protected from prune_connected_peers | HIGH | (gap NOT in invariants.md — propose new INV) |
| F-7 | GossipsubNotSupported → Fatal on legitimate quirks | MEDIUM | Cross-references INV-002 (which doesn't help NVVs) |
| F-8 | BANNED_PEERS_PER_IP_THRESHOLD=1 + shared-NAT validator | MEDIUM | INV-011 |
| F-9 | disconnected_peers unchecked addition | LOW | INV-022 |
| F-10 | KadStore restart load + immediate penalty window | MEDIUM | Appendix A3 (restart erasure) |
| F-11 | open_epoch_pack 30s timeout with no retry | LOW | (no INV — node-level liveness) |
| F-12 | log spam under sustained protected-peer overshoot | LOW | INV-045 (accepted) |
