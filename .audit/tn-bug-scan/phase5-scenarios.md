# Phase 5 — Failure-Scenario Tracing

For each high-priority finding, build the concrete event chain.

## Scenario S1 — Unresolved CVV banned mid-epoch (CF-1 + F-1)

**Event class:** epoch transition + kad resolution latency + concurrent load.

**Topology:** 10-node committee. Validator V_x rejoins network after a config-only restart (its BLS key unchanged, libp2p PeerId regenerated due to operator seed change, OR V_x is a newly-elected committee member who has never previously connected to this node).

**Step-by-step:**

1. Local node L receives `NetworkCommand::NewEpoch { committee }` for epoch E+1.
2. `peer_manager.new_epoch(committee)` runs.
3. For V_x's BLS key (unknown to L's `bls_index`), L inserts `current_committee_keys[bls_v] = None` and pushes `MissingAuthorities([bls_v])` event.
4. ConsensusNetwork polls the event in its main loop, calls `kad.get_record(key)`. Network round-trip: 2-10 seconds typical, 60s timeout configured (consensus.rs:274).
5. **In parallel:** V_x's libp2p stack contacts L directly (V_x knows L's address from a prior epoch's NodeRecord persisted in V_x's kad store). V_x opens an inbound connection to L.
6. L's behavior.rs:69 `handle_pending_inbound_connection` runs. V_x's source IP is not banned. Accept.
7. L's behavior.rs:79 `handle_established_inbound_connection` runs. `peer_banned(V_x.pid)` — V_x is not yet in L's `peers`, so `peer_banned` returns false (peers.get returns None, `is_some_and` returns false; temporarily_banned empty). Accept.
8. L's behavior.rs:217 `on_connection_established` runs. V_x's PeerId registered as `IncomingConnection`. `peer_limit_reached` and `peer_is_important` checks; `peer_is_important` reads `is_peer_validator || is_trusted` — both false (V_x's BLS isn't yet resolved). If L is at peer limit, V_x is disconnected immediately with PX. If L has capacity, V_x is accepted as a regular peer.
9. **Critical:** V_x subscribes to gossipsub topics, including the consensus-output topic.
10. The consensus engine on V_x publishes its first gossip message (e.g., a block proposal or vote).
11. L's gossipsub layer delivers the message to L's `process_gossip_event` (consensus.rs:766).
12. `verify_gossip` (line 988) checks `authorized_publishers[topic].contains(V_x.bls)`. Two failure paths:
    - Path A: L's `authorized_publishers` for this topic hasn't been updated for epoch E+1 yet (CF-4). V_x.bls not in the set. Reject.
    - Path B: V_x's BLS resolution via kad hasn't completed yet. `peer_to_bls(V_x.pid)` returns None. The `bls_key.is_some()` check fails. Reject.
13. `process_penalty(V_x.pid, Penalty::Fatal)` fires.
14. `Peer::apply_penalty` runs. V_x is NOT trusted (no promotion yet — `current_committee_keys[bls_v]` is `Some(&None)` but V_x.bls isn't in `bls_index` yet, so the local node has NO connection between V_x.pid and V_x.bls). V_x's score → min.
15. `update_connection_status(V_x.pid, NewConnectionStatus::Banned)` runs. V_x.pid moves to Banned.
16. `apply_peer_action(Ban(ips))` runs → temporarily_banned.insert(V_x.pid), PeerEvent::Banned pushed.
17. ConsensusNetwork polls PeerEvent::Banned. consensus.rs:1140: `gossipsub.blacklist_peer(&V_x.pid)`, `kademlia.remove_peer(&V_x.pid)`.
18. **Eventually:** kad query from step 4 completes. ConsensusNetwork::process_kad_query_result → close_kad_query → add_known_peer(V_x.bls, ...).
19. `add_known_peer` → `upsert_peer`. Now bls_index gets V_x.bls → V_x.pid'. The PeerId in the kad record (V_x.pid') matches V_x.pid that connected directly — same network keypair.
20. `current_committee_keys[V_x.bls] == Some(&None)` → `should_promote = true` → `promote_committee_member`.
21. promote_committee_member: `current_committee.insert(V_x.pid)`. `ensure_peer_exists`. V_x's `ConnectionStatus` is `Banned`. `handle_unbanned_transition` (`Banned` arm): set status to Disconnected, remove banned IPs, emit `PeerAction::Unban(ips)`.
22. Peer record: `is_trusted` set via `make_trusted`. Score reset to `Score::new_max`.
23. Unban action propagated: `apply_peer_action(Unban) → PeerEvent::Unbanned`.
24. ConsensusNetwork polls PeerEvent::Unbanned. consensus.rs:1147: `gossipsub.remove_blacklisted_peer(&V_x.pid)`.

**Final state:**
- V_x.pid is in `peers` as `Disconnected`, `is_trusted == true`.
- gossipsub has un-blacklisted V_x.pid.
- **kademlia.remove_peer was called, but no re-add path is taken.** V_x.pid is NOT in L's kad routing table.
- L cannot dial V_x without a kad query OR an explicit PX exchange OR V_x re-dialing.
- L's outgoing dials for V_x require an explicit BLS lookup (`auth_to_peer(V_x.bls)`) which returns V_x.pid AND the listening_addrs from `update_net`. The addresses ARE present (they came in via the kad record).
- But the swarm-level connection was severed at step 17 (gossipsub blacklist + kademlia routing remove + temporarily_banned insert at step 16).
- temporarily_banned.contains(V_x.pid) is still TRUE — the BannedPeerCache wasn't cleared by promotion.

**Wait — let me re-check step 22.** In `PeerManager::new_epoch` (manager.rs:601-630), the FIRST thing it does is iterate the committee and call `temporarily_banned.remove(peer_id)` for each (lines 603-610). So at the original new_epoch call (step 2), V_x's bls wasn't yet in bls_index, so `auth_to_peer(bls)` returned None — meaning we didn't try to remove V_x from temporarily_banned during new_epoch.

After step 16 inserts V_x.pid into temporarily_banned, there is no path during `promote_committee_member` that removes it. So **V_x is permanently in temporarily_banned until either TTL expires (default 600s — `excess_peers_reconnection_timeout`) or the NEXT new_epoch fires (which removes via lines 603-610).**

Result: V_x is in committee, is_trusted, score max, BUT temporarily_banned. Any inbound from V_x: `peer_banned(V_x.pid) = temp_banned.contains || peer.peer_banned = true || false = true`. ConnectionDenied. V_x cannot reconnect for up to 600 seconds OR until the next epoch.

**For a 10-node committee, losing one validator for 600 seconds may cause consensus stall** (depending on Bullshark's f tolerance — with N=10, f=3, you can lose 3 and still progress; 4 → stall).

**Failure mode:** consensus-stall (probabilistic), liveness-degradation (definite for V_x).

**Invariants violated:** INV-002 (intent — trusted peers should not be banned, but V_x was banned BEFORE being trusted). INV-032 (intent — new_epoch makes committee members trusted with max score and removes them from temporarily_banned, but this only fires for committee members ALREADY in bls_index at new_epoch time).

**Appendix gap widened:** A4 (race between PeerManager ban and gossipsub blacklist) — the race here is BAD because the eventual recovery doesn't fully restore kad routing.

---

## Scenario S2 — Observer state-sync stall under prune pressure (CF-3 + F-6)

**Event class:** network partition + concurrent load.

**Topology:** 10-node committee + 1 observer node O. O is doing state-sync to catch up to current epoch.

**Step-by-step:**

1. O has connections to 5 CVV peers (committee members of the current epoch).
2. O's PeerManager: target_num_peers = `K_VALUE * 3/2` (typically 15). O has 5 + a few extra non-committee peers = 12 total. Below target. No pruning.
3. O publishes/receives gossip; sends `request_consensus` to fetch headers.
4. Several rounds of inbound peer-exchange responses cause O to dial discovery peers; O reaches 18 total connections.
5. Heartbeat fires (manager.rs:244). `prune_connected_peers` (manager.rs:490):
   - `connected_peers_by_score_and_routability` returns all 18 sorted by score (low first), non-routable first. The 5 CVVs have score `max` (committee, trusted). The non-CVV peers vary.
   - `excess_peer_count = 18 - 15 = 3`.
   - Filter loop: for each peer in sorted order, if `!is_peer_validator(peer_id) && !peer.is_trusted()`, eligible for prune. CVVs filtered out (is_peer_validator returns true). The 3 lowest non-CVV peers selected.
   - These 3 are disconnected via `disconnect_peer(_, true)` — `PeerEvent::DisconnectPeerX`.
6. NORMAL CASE. Now consider: what if THIS NODE O is dialed BY a different observer O', and O' becomes the lowest-scored peer in O's list?
7. Or, more realistically: the 5 CVVs have score max; suppose one CVV V_h has a temporary issue and accumulates a Penalty::Mild from gossipsub SlowPeer at consensus.rs:828. V_h's score drops slightly (-1). V_h is still in committee (is_trusted), so `Peer::apply_penalty` SHORT-CIRCUITS at peer.rs:181 — score is NOT modified. V_h's score remains max.
8. So all 5 CVVs remain at max forever (correctly).
9. The non-CVV-non-trusted peers compete on score. If O is the "lowest-scored" peer in CVV V_k's perspective (CVV V_k has 30 peers, score-sorts them), CVV V_k's heartbeat prunes O.
10. CVV V_k sends `DisconnectPeerX` to O. O receives the disconnect.
11. O is down to 17 (then 16, then 15... ) connections. If multiple CVVs simultaneously decide O is excess (because their own peer pool is well-populated), O could lose ALL its CVV connections in one heartbeat round.
12. O's state-sync `request_consensus` finds the network handle's `connected_peers` empty. `SendRequestAny` returns `Err(NetworkError::NoPeers)` at consensus.rs:674.
13. O's state-sync `get_consensus_header` (state-sync/consensus.rs:78) gets the Err, sleeps 5s, retries (state-sync/consensus.rs:88).
14. **No re-dial mechanism for the lost CVV peers.** O's PeerManager's `discovery_heartbeat` (every 30s default) tries to re-dial discovery candidates, but the CVVs may not be in discovery_peers (they were promoted past discovery).
15. O eventually times out; state-sync stalls indefinitely.

**Failure mode:** liveness-degradation (definite).

**Invariants violated:** the SPIRIT of "state-sync depends on observers staying connected to CVVs" is violated. There is NO invariant currently codifying this. Propose new INV-048: observer-mode peers must be exempt from `prune_connected_peers` when they hold the only path to a peer with the role of providing state-sync data.

**Appendix gap widened:** none currently — needs a new appendix entry or new INV.

---

## Scenario S3 — KadStore num_records underflow on restart (CF-2 + F-4)

**Event class:** node restart + mid-flush state.

**Topology:** any node, after running for >24 hours so its kad store has accumulated records that pass their TTL during the next session.

**Step-by-step:**

1. Node N runs for several days, accumulating M kad records, some of which are at or past TTL.
2. N restarts.
3. `KadStore::new` counts num_records from db.iter (kad.rs:201). The expired records ARE counted (iter is unfiltered at this site).
4. `ConsensusNetwork::new` loads records via `kad_store.records()` (consensus.rs:284) — this iterator DOES filter expired (kad.rs:309). So only fresh records are added to peer_manager.
5. After startup, libp2p-kad's internal `PutRecordJob` runs (consensus.rs:441 → `kademlia.set_mode(Server)` triggers the publication cycle).
6. PutRecordJob's TTL-eviction logic identifies expired records in the store, calls `RecordStore::remove(key)` for each.
7. `KadStore::remove` (kad.rs:380): `self.db.remove::<KadRecords>(&key)`. The row exists, returns Ok. `self.num_records -= 1`.
8. As long as removes don't outpace inserts, num_records stays correct.
9. **The trigger:** suppose two paths target the same expired record:
   - Path A: PutRecordJob calls `RecordStore::remove(K)`.
   - Path B: `evict_expired_records` (kad.rs:219) — but when is this called? Only from `put` (kad.rs:360) and `add_provider` (kad.rs:404), when the store is full.
10. Sequence to trigger underflow:
    a. num_records reaches the cap (max_records).
    b. New put → evict_expired_records → finds K is expired → `db.remove::<KadRecords>(K)` → Ok → evicted++.
    c. self.num_records.saturating_sub(evicted) — saturating, safe.
    d. Now num_records = max_records - 1. Still 1 from cap.
    e. PutRecordJob ALSO had K queued for removal. Calls `RecordStore::remove(K)`. `db.remove(K)` again — vacuous (already gone) but returns Ok in most KV stores.
    f. `self.num_records -= 1` (kad.rs:389). Underflow if previous op put us at 0; with the buffer, only underflows if many such double-removes happen consecutively.
11. With enough double-removes, num_records wraps to usize::MAX.
12. Next put: `if self.num_records >= self.config.max_records` → true → evict_expired_records → finds nothing more to evict → still >= max_records → returns Err(MaxRecords).
13. Every subsequent kad put fails. New peer NodeRecords cannot be cached.

**Mitigation that masks the issue:** the test at kad.rs:843-857 confirms that expired-removal works under nominal "single removal" scenarios. There's no test for the double-remove case.

**Failure mode:** state-corruption (num_records counter wrong) → liveness-degradation (future puts fail) → eventual fork-risk if NodeRecords for new committee members can't be cached, leading to repeated kad walks.

**Repro test that would catch it:**
```rust
#[test]
fn test_double_remove_does_not_underflow() {
    let mut kad_store = KadStore::new(...);
    let rec = test_record(false);
    kad_store.put(rec.clone()).unwrap();
    assert_eq!(kad_store.num_records, 1);
    kad_store.remove(&rec.key);
    assert_eq!(kad_store.num_records, 0);
    kad_store.remove(&rec.key); // double remove
    assert_eq!(kad_store.num_records, 0, "double remove must not underflow");
}
```

**Invariant:** NO INV in invariants.md (kad.rs is outside the peer manager's purview). Propose: kad store counters must be saturating in both directions.

---

## Scenario S4 — IP-ban poisons CVV behind shared NAT (F-8)

**Event class:** concurrent load + correlated peer misbehavior.

**Topology:** 2 committee members V_a, V_b co-located behind a single NAT egress IP IP_X (e.g., both running on the same operator-managed Kubernetes cluster with a single SNAT). Local audit node L is a third party.

**Step-by-step:**

1. L's `banned_peers_by_ip[IP_X]` starts at 0.
2. Non-CVV peer P (some random network participant from IP_X — perhaps another node operator running observer mode through the same NAT) misbehaves. P is banned.
3. `add_banned_peer(P)` walks P's IPs. `banned_peers_by_ip[IP_X] += 1 → 1`.
4. `ip_banned(IP_X)`: count > 1? `1 > 1 == false`. IP not yet banned. OK.
5. Some time later, ANOTHER non-CVV peer Q (also from IP_X) misbehaves. Q is banned.
6. `banned_peers_by_ip[IP_X] += 1 → 2`.
7. `ip_banned(IP_X)`: `2 > 1 == true`. **IP_X is now in `banned_ips()`**.
8. V_a's libp2p stack experiences a transient network issue (e.g., DC switch fail-over). V_a's connection to L drops.
9. V_a's libp2p attempts to reconnect to L.
10. L's `handle_pending_inbound_connection` runs. `sanitize_ip_addr(V_a.addr)` → `has_valid_unbanned_ips` → finds IP_X is_ip_banned → returns false → ConnectionDenied at behavior.rs:201-205.
11. V_a's reconnection is silently refused. L logs "Connection denied: peer has no valid unbanned IP addresses" (behavior.rs:202).
12. **There is NO `is_peer_validator` check at this layer.** INV-033 only protects against PRUNING, not connection acceptance.
13. V_a is locked out of L for as long as IP_X remains banned. Some other path eventually removes a peer from `banned_peers_by_ip[IP_X]` (e.g., one of P or Q's records gets cleared by `prune_banned_peers`).
14. `prune_banned_peers` would skip CVV V_a but evict P/Q based on banned-instant. After both evicted, count drops to 0. IP unbanned. V_a can finally reconnect.

**Time to recovery:** depends on `max_banned_peers` (default 100) — until the banned_peers set fills past 100, P/Q aren't evicted. They sit in the banned set indefinitely. **In a quiet network where banned_peers stays under 100, V_a can be locked out for the entire epoch.**

**Failure mode:** consensus-stall if multiple CVVs lose connectivity (4+ out of 10 means f+1 violations).

**Repro:**
1. Set up 3 nodes L, V_a, V_b. Use iptables on a shared host to MASQUERADE both V_a and V_b through IP_X.
2. Connect L to V_a, V_b (committee).
3. Add 2 ephemeral non-CVV peers P, Q from IP_X. Have them send malformed gossip to L. Both get banned.
4. `banned_peers_by_ip[IP_X] = 2`, IP_X banned.
5. Restart V_a's network or use `iptables -A INPUT -s L_addr -j DROP; sleep 5; iptables -D INPUT -s L_addr -j DROP` to force a reconnect.
6. V_a's reconnect to L is denied.
7. Assert L's logs include "Connection denied" for V_a.

**Invariant violated:** INV-012 (intent — adding a trusted peer removes its IPs from the banned set) is enforced only at `add_trusted_peer` and `new_epoch` boundaries. Mid-epoch IP-ban additions due to non-CVV peers can re-poison the IP without re-clearing for the CVV.

**Appendix gap widened:** A5 (IP-ban does not eject already-connected peers) is the disconnect side. This is the symmetric ingress side.

---

## Scenario S5 — Orphan accumulation under repeated key rotations (F-2)

**Event class:** mid-epoch keypair rotation (operator-induced).

**Topology:** node N runs continuously. Operator periodically rotates libp2p keypair seed in config.

**Step-by-step:**

1. Initial state: node N's `peers` has 100 entries, including V's PeerId_0 (V's BLS_v → PeerId_0).
2. V rotates network keypair to PeerId_1 (operator config change). V re-emits NodeRecord with new pubkey, signed by BLS_v.
3. N receives V's new NodeRecord via kad. `upsert_peer(BLS_v, V.networkpubkey_1, addrs_1)`.
4. `rebind_bls(PeerId_1, BLS_v)`:
   - peer at PeerId_1: does not exist yet. No old_bls to clear.
   - `bls_index.insert(BLS_v, PeerId_1)` returns the old value `PeerId_0`. Since `PeerId_0 != PeerId_1`, look up `peers[PeerId_0]` — exists — call `orphan.clear_bls()`. PeerId_0's `bls_public_key = None`.
5. Create new Peer at PeerId_1 with addrs_1. Inserted.
6. `current_committee_keys[BLS_v] == Some(&Some(PeerId_0))` (resolved in prior new_epoch). Match arm: `Some(&Some(other_pid)) if other_pid != peer_id`. **True** (other_pid = PeerId_0, peer_id = PeerId_1). Remove PeerId_0 from `current_committee`. Call promote_committee_member(BLS_v, PeerId_1).
7. promote_committee_member: insert PeerId_1 into current_committee, current_committee_keys[BLS_v] = Some(PeerId_1), make_trusted(PeerId_1). PeerId_0's `is_trusted` flag is NOT touched here (it was set to true in a prior epoch's promotion). PeerId_0 remains `is_trusted == true`.
8. After step 7: peers has both PeerId_0 (is_trusted=true, bls=None) and PeerId_1 (is_trusted=true, bls=BLS_v).
9. PeerId_0 is now an orphan. It is NOT in current_committee. It IS is_trusted. By `is_protected`, since trusted ⇒ protected. PeerId_0 survives pruning.
10. Operator rotates V again to PeerId_2. Repeat: PeerId_1 becomes orphan with is_trusted=true.
11. After K rotations: K orphan PeerId records, all is_trusted=true.

**Per-orphan memory cost:** Peer struct is ~200-500 bytes (HashSet<Multiaddr>, listening_addrs Vec, etc.). 100 rotations × 500 bytes = 50KB. Trivial.

**Real cost:** orphans count toward `max_peers` aggregate. With max_peers = ~75 (target * 1.3), 75 orphans means no slots for legitimate new peers. `peer_limit_reached` would always return true. New inbound connections rejected.

**Likelihood:** rotation is operator-rare. Default network config doesn't auto-rotate. The user hint mentions "mid-epoch libp2p keypair rotation; crash-restart of committee member with new keys" — this is an operator-driven escape hatch, used during incident response. After M rotations of K validators per session, orphan count = M*K.

**Failure mode:** liveness-degradation as max_peers fills with orphans.

**Mitigation in code:** None. Appendix A10 documented this gap and proposed two mitigations.

---

## Scenario S6 — pending_dials overwrite drops dial result (Appendix A1)

**Event class:** concurrent load.

**Topology:** node N issues two dial requests to the same peer P within the heartbeat interval.

**Step-by-step:**

1. Caller A calls `network_handle.dial(P, addr)` → returns oneshot_a.
2. Network main loop processes Dial command, calls `peer_manager.dial_peer(P, [addr], Some(oneshot_a))`. Goes through `dial_requests` queue.
3. Before the dial completes, Caller B also calls `network_handle.dial(P, addr)` → returns oneshot_b. The dial goes to queue.
4. behavior.rs poll loop pops first dial request. Calls `register_dial_attempt(P, Some(oneshot_a))`. `pending_dials.insert(P, oneshot_a)`. Status = Dialing. Issues ToSwarm::Dial.
5. Next poll cycle: `dial_attempt_already_registered(P) == true`. Second dial returns early at manager.rs:131-143 with the AlreadyDialing error sent on oneshot_b. **Wait — this is in `dial_peer` (manager.rs:122).** Let me re-read.
6. Looking at the actual code: caller B's request goes through `network_handle.dial → NetworkCommand::Dial → dial_peer`. In `dial_peer`:
   - `peers.get_peer(&peer_id)` matches `peer.connection_status()` against ConnectionStatus. P's status is Dialing. Returns the AlreadyDialing error via the reply channel. The request is NOT pushed onto dial_requests.
7. So the documented Appendix A1 scenario only manifests when the second caller goes through a path that DOESN'T check the status. Looking at `register_dial_attempt` (all_peers.rs:373-383):
   ```rust
   self.update_connection_status(&peer_id, NewConnectionStatus::Dialing);
   if let Some(reply) = reply {
       self.pending_dials.insert(peer_id, reply);
   }
   ```
8. `pending_dials.insert` overwrites without checking. If the path that calls `register_dial_attempt` directly (bypassing `dial_peer`'s status check) is used, the overwrite happens.
9. The only direct caller of `register_dial_attempt` outside `dial_peer` is in `handle_pending_outbound_connection` (behavior.rs:51) — when kad initiates an outbound and the peer manager doesn't have it as Dialing yet. The reply is `None` in this kad-initiated case (behavior.rs:51), so no oneshot to drop.
10. Then in behavior.rs:181, the manager-initiated dial calls `register_dial_attempt(peer_id, reply)` where reply is from the original DialRequest. This runs AFTER `next_dial_request` pops the request and immediately before `ToSwarm::Dial` is emitted.
11. So the order is:
    a. dial_peer pushes request_1 (oneshot_a). dial_requests = [req_1].
    b. dial_peer for SAME peer pushes nothing (rejected at status check).
    c. behavior.poll pops req_1, calls register_dial_attempt(P, Some(oneshot_a)), pending_dials[P] = oneshot_a, status = Dialing. ToSwarm::Dial.

12. Single-flight enforced by `dial_peer`'s status check. **Appendix A1 documents the case where the check is bypassed.** I need to find any path that pushes to dial_requests for the same peer twice OR a path that calls `register_dial_attempt` with a Some(reply) for a peer already pending. Looking at the code, the only such path requires the status to not be Dialing when dial_peer is called. **Trigger:** if dial_peer is called twice with the same peer in the SAME tick BEFORE the first poll cycle processes the request, the status is NOT YET Dialing (it's still whatever it was before — possibly Disconnected or Unknown). Both pushes succeed. Both pop sequentially:
    a. dial_peer(P) call 1: P status is Disconnected. dial_peer pushes req_1 (oneshot_a). dial_requests = [req_1].
    b. dial_peer(P) call 2: P status is STILL Disconnected (no update yet). dial_peer pushes req_2 (oneshot_b). dial_requests = [req_1, req_2].
    c. poll pops req_1. register_dial_attempt(P, Some(oneshot_a)). pending_dials[P] = oneshot_a. Status = Dialing. ToSwarm::Dial emitted. **Note:** poll returns Ready(ToSwarm::Dial) AND exits the function. The next poll iteration starts fresh.
    d. Next poll: pops req_2. register_dial_attempt(P, Some(oneshot_b)). pending_dials.insert(P, oneshot_b) **overwrites oneshot_a, drops it silently**. The original caller of req_1 awaits oneshot_a forever (or until libp2p delivers a DialFailure which can also notify via on_dial_failure → notify_dial_result, but that consumes pending_dials[P] which is now oneshot_b).

13. **Result:** Caller A's await hangs OR gets the result intended for Caller B. Caller B may also miss results if pending_dials is overwritten again (e.g., a third dial).

**Failure mode:** silent-wrong-state (caller-B-context result returned to caller-A).

**Likelihood:** depends on whether the upper layers ever call dial twice for the same peer in the same tick. Audit: `NetworkCommand::Dial` is from the application layer; if the application's network state has a stale view of who is connected, it could double-dial. The application has `connected_peers` VecDeque (consensus.rs:160) which is event-driven, so a peer that just disconnected may not be reflected immediately. Race plausible.

**Invariant violated:** INV-024 ("at most one in-flight dial per PeerId") is intended but enforced only by the status check, which fails when called before the status updates. The documented INV-024 mitigation (caller-side timeouts on oneshot::Receiver) shifts blame to callers.

---

## Scenario S7 — open_epoch_pack pre-dial under network partition (F-11)

**Event class:** network partition.

**Topology:** node N restarts during a network partition that has isolated N from most CVVs.

**Step-by-step:**

1. N starts up after a crash mid-epoch. Previous epoch E's record is missing from DB (e.g., killed during failed-quorum recovery).
2. `run_epoch` calls `open_epoch_pack(committee, spawner)`.
3. `previous_epoch_rec = None` (DB lookup miss). Enters the else-branch at epoch.rs:467.
4. `requested_missing_epoch` watch updated. Warn log.
5. `connected_peers_count() == 0` → enter pre-dial block.
6. `primary_network_handle.inner_handle().new_epoch(committee_keys).await` — triggers a NewEpoch on the running peer manager. The bls_index is empty (kad records may or may not have been loaded yet — depends on timing of when the primary network finished its startup vs when run_epoch runs). Unresolved BLSes added to current_committee_keys[bls]=None, MissingAuthorities pushed.
7. For each committee BLS, `dial_peer_bls(handle, bls, spawner)` is called (epoch.rs:488). This spawns an async task that attempts dial. **But the BLS is unresolved (no addrs in bls_index), so `auth_to_peer(bls)` returns None.** The dial command falls through to `Err(NetworkError::PeerMissing)` (consensus.rs:583).
8. **The pre-dial loop fires Err(PeerMissing) for every BLS in the committee** since none are resolved yet. No dial commands actually reach the swarm. The kad MissingAuthorities events fire instead — those go through the swarm.
9. Wait for record with 30s timeout (epoch.rs:496-500).
10. During the wait, kad queries may resolve some BLSes IF peers respond. If the partition isolates N from kad respondents, queries time out (60s in kad config).
11. After 30s, `record_by_epoch_with_timeout` returns None. Error returned: "Missing previous epoch record for epoch X after waiting".
12. `run_epoch` propagates the error to the caller.
13. Node terminates (or whatever the upstream handler does — likely log + exit).

**Failure mode:** liveness-degradation (node cannot start). For an operator, the recovery is to wait for partition to heal AND restart manually.

**Improvement candidate:** retry inside open_epoch_pack with backoff, or signal a "partition mode" that maintains the run loop while waiting.

---

## Scenario coverage check

| Phase 2 finding | Scenario built? |
|---|---|
| F-1 (CF-1) | S1 ✓ |
| F-2 | S5 ✓ |
| F-3 (A2) | covered conceptually in S1 step 7 |
| F-4 (CF-2) | S3 ✓ |
| F-5 (A8) | covered in domain patterns (low-traffic in scenarios) |
| F-6 (CF-3) | S2 ✓ |
| F-7 | covered in S1 (step 12 — verify_gossip rejection path) |
| F-8 | S4 ✓ |
| F-9 (counter asymmetry) | LOW — no scenario built |
| F-10 (restart + Fatal window) | covered in S1 (substantially same window as CF-1) |
| F-11 | S7 ✓ |
| F-12 (log spam) | LOW — no scenario |
| CF-4 (NewEpoch / UpdateAuthorizedPublishers timing) | covered in S1 (Path A at step 12) |
