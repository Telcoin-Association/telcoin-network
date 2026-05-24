# Phase 4 — Feedback Loop Summary

## Iteration 1

### Step A — state gaps → Feynman re-interrogation

For each GAP from Phase 3, ask the Feynman questions.

**GAP-1 (IP-ban does not respect committee membership):**
- WHY doesn't `peer_banned` consult `is_protected`? The function pre-dates the committee-aware protection. `peer_banned` is called at multiple low-level sites (behavior.rs:88, :104, manager.rs:336) where the call site doesn't have CVV context.
- ASSUMPTION: the author assumed IP-ban is a separate concern from per-peer ban, and INV-012 + INV-032 (`remove_validator_ip` on new_epoch / add_trusted_peer) would clear the IP at epoch boundary. **But this only clears IPs at promotion time, not during the epoch when sibling peers get newly banned.**
- DOWNSTREAM: A returning committee member trying to reconnect mid-epoch gets denied by IP-ban check. Their inbound connection is rejected at behavior.rs:88. No event fired (just a ConnectionDenied propagated to libp2p). The peer manager doesn't know V_b tried to reconnect.

**GAP-2 (promote_committee_member leaves peer in Disconnected):**
- WHY does the promote path NOT trigger a re-dial? promote_committee_member only manipulates indices; the swarm is the source of truth for connection.
- ASSUMPTION: the next discovery_heartbeat or the peer's own inbound reconnect will re-establish.
- DOWNSTREAM: If kad routing was removed (PeerEvent::Banned → kademlia.remove_peer), discovery_heartbeat has no entry to dial. PeerEvent::MissingAuthorities can re-fire kad lookups (the find_authorities path) but only if something explicitly triggers it. After unresolved-CVV-window resolution AND ban-and-unban cycle, the peer manager has the bls→pid mapping but no addresses in kad routing.

**GAP-3 (remove_peer does not clear current_committee_keys):**
- WHY doesn't remove_peer touch current_committee_keys? Asymmetric design: current_committee_keys is owned by the epoch lifecycle (new_epoch / upsert_peer / promote), while remove_peer is a pruning utility.
- ASSUMPTION: pruning never targets a committee peer because `is_protected` filter precedes remove_peer.
- DOWNSTREAM: If `is_protected` is ever bypassed (developer error, future refactor), the peer is removed but current_committee_keys retains the stale Some(pid). Defensive depth missing.

**GAP-4 (temporarily_banned can disagree with reputation):**
- WHY two banned sets? Different lifetimes: temp-ban is for excess-peer rejection, reputation is for misbehavior. INV-013 confirms.
- ASSUMPTION: code uses `peer_banned` everywhere. Audit shows `auth_to_peer` (all_peers.rs:730) does NOT consult temporarily_banned — it just returns the (pid, addrs) tuple. The caller is responsible for filtering. consensus.rs:725 `OpenStream` calls `auth_to_peer` and then opens a stream without checking temp-ban.
- DOWNSTREAM: stream open is attempted against a temp-banned peer, gets rejected at established_outbound check. Wasted attempt.

**GAP-5 (disconnected_peers asymmetric):**
- WHY non-saturating increment? Most likely overlooked — saturating_sub was added for INV-022 idempotence, but the additive sites never got the parallel treatment.
- ASSUMPTION: state transitions are not double-counted. No specific code path violates this currently.
- DOWNSTREAM: future code change could violate.

### Step B — Feynman findings → state dependency expansion

**F-1 (unresolved-CVV penalty window):** Does this WRITE to coupled state?
- Yes: writes score, writes connection_status (transition through Banned), writes banned_peers IP counters.
- ORDERING concern: ban-propagation event (PeerEvent::Banned → consensus.rs:1140) fires BEFORE the eventual kad-resolution. The consequence is that gossipsub.blacklist_peer and kademlia.remove_peer both run on the unresolved-CVV's PeerId. AFTER kad resolution lands, `upsert_peer` → `promote_committee_member` → `update_connection_status(Unbanned)` → emits PeerEvent::Unban. The Unban event eventually clears gossipsub blacklist (consensus.rs:1147), but kademlia.remove_peer was permanent (no re-add path). Without a kad routing entry, the peer is unreachable until kad walks back. **Cross-layer atomicity violation.**

**F-6 (Observer pruning):** Coupled state?
- Yes: writes connection_status (Disconnecting), writes temporarily_banned (insert), writes events (DisconnectPeerX).
- ASSUMPTION violation: state-sync assumes observers stay connected long enough to fetch consensus headers. The peer layer's eviction logic violates this.
- ADD COUPLED PAIR: (NodeMode, is_protected). Currently NodeMode is not tracked in `Peer`. A new field (e.g., `is_observer: bool` similar to `is_trusted`) and protection rule would close this.

**F-7 (GossipsubNotSupported = Fatal):** Coupled state?
- Writes score → min, connection_status → Banned, banned_peers IPs.
- ASSUMPTION: GossipsubNotSupported is always malicious. No, it can fire for transient protocol-negotiation reasons.
- DOWNSTREAM: a transient quirk evicts an honest peer for a full ban cycle.

**F-8 (IP-ban THRESHOLD=1):** Coupled state?
- Writes banned_peers_by_ip.
- ASSUMPTION: IP-ban only bans the IP, not the peer at that IP. But `peer_banned` ORs ip_banned with reputation, so an unban-banned peer at an ip_banned IP is rejected at connection time. The connection-acceptance path treats ip_banned as a hard block.
- ADD COUPLED PAIR: (current_committee, peer's IP set). Need to bypass ip_banned for CVV-resident IPs.

### Step C — masking code → joint interrogation

**Defensive mask 1: `disconnected_peers.saturating_sub` (all_peers.rs:452, 489, 596, 970):** Why? Feynman: handles a state transition that COULD theoretically over-decrement. State-check: the increments are NOT saturating, so the WRONG direction is asymmetrically protected. Joint conclusion: the design intent was likely "+= 1 once per peer", but the saturating-sub is the safety net for the path where decrement is called more than once. The asymmetry hides a potential count-rot, but no current path exercises it.

**Defensive mask 2: `BannedPeers::remove_banned_peer` `total.saturating_sub` (banned.rs:43):** Why? Feynman: the function is called from many sites (Banned→Connected, Banned→Dialing, Unban event, prune, validator promotion). Each call assumes the IP-counts and total are in sync. State-check: `add_banned_peer` increments `total` once per call AND increments `banned_peers_by_ip[ip]` per IP. `remove_banned_peer` decrements `total` once AND walks IPs to decrement counts. The mask hides that callers can over-call remove relative to add: the comment at banned.rs:42 reads "This method always reduces the total number of banned peers by 1." Joint conclusion: the documented intent is "always -1 per call" so the saturating is the floor. A clear specification, but the absence of a paired count audit means counter drift could occur silently.

**Defensive mask 3: `evict_expired_records` saturating_sub (kad.rs:249) BUT `remove` unchecked `-= 1` (kad.rs:389):** Why the asymmetry? Feynman: evict_expired runs in a batch context where many removals can fire (could legitimately try to remove a record that's already gone if a concurrent path removes it). State-check: `remove` is the libp2p-kad TTL job's interface — called once per record per TTL tick. If TTL job and evict_expired both target the same key, the second call sees `db.remove` return Ok (vacuous remove returns Ok) and decrements an already-zero counter. **CONFIRMED: defensive mask is asymmetric and the unprotected path IS reachable.** F-4 is HIGH confidence MEDIUM severity.

**Defensive mask 4: `peer_banned` ORs `temporarily_banned.contains(peer_id) || peers.peer_banned(peer_id)` (manager.rs:343):** Why OR? Because the two sets can disagree (Appendix A7). The OR is the unifier. But it hides the underlying disagreement. Joint conclusion: documented intentional design, but the underlying disagreement is what causes confusing logs ("peer is temp-banned" without context).

**Defensive mask 5: `.unwrap_or_default()` in state-sync/lib.rs:138 (last_consensus_block):** Why? Hides DB-read failures. Joint conclusion: state-sync gracefully degrades to "from genesis" when DB read fails. This is a real-world resilience choice but it MASKS DB corruption. The error path returns an Ok(None) when the DB is corrupt, and the caller can't tell.

### Step D — convergence check

Iteration 1 produced:
- New coupled pair (NodeMode, is_protected) → flag for new INV proposal
- New coupled pair (current_committee, peer's IP set) for IP-ban bypass
- Confirmed F-4 is a real bug via state asymmetry analysis
- Confirmed F-1 is a cross-layer atomicity violation involving gossipsub + kad + peer manager

**Did this iteration produce NEW findings?** Yes:
- **CF-1** (F-1 + F-7 + GAP-2): unresolved-CVV-GossipsubNotSupported-Fatal-loop. **HIGH-CRITICAL.**
- **CF-2** (F-4 + F-10): kad underflow more likely on restart. **MEDIUM.**
- **CF-3** (F-6 + F-8): observer behind shared-NAT validator double-vulnerable. **HIGH.**

Continuing to iteration 2.

## Iteration 2

### Step A — new gaps from iteration 1

**Did CF-1 reveal a new mutation path?** Let me trace the exact sequence:
1. Epoch N+1 fires `NetworkCommand::NewEpoch { committee }` (consensus.rs:702).
2. `peer_manager.new_epoch(committee)`. For each committee member NOT in bls_index: `current_committee_keys[bls] = None`, push BLS to unresolved.
3. `MissingAuthorities` event pushed.
4. ConsensusNetwork polls events; consumes MissingAuthorities; for each unresolved BLS, calls `kad.get_record`.
5. Meanwhile, an observer-mode peer that just connected (NOT yet in any committee) sends a gossip message on a topic this node is subscribed to.
6. `process_gossip_event` runs `verify_gossip`. The peer's BLS is NOT in `authorized_publishers` for this topic (no resolution). `is_some_and(|auth| auth.is_none() || (bls_key.is_some() && auth.as_ref().expect(...).contains(...)))` returns false.
7. `GossipAcceptance::Reject` returned.
8. `process_penalty(propagation_source, Penalty::Fatal)` fires (consensus.rs:813).
9. The propagation_source is NOT (yet) a trusted peer.
10. `process_penalty` → score to min → connection_status → Banned → PeerEvent::Banned → consensus.rs:1140 → gossipsub.blacklist_peer + kademlia.remove_peer.

So the scenario is: a soon-to-be-CVV connects EARLY, sends a gossip message on a topic where its BLS isn't yet authorized (because no authorized_publishers map for the new epoch yet OR because the epoch boundary hasn't synced authorities yet), and gets killed.

**Is this realistic?** Yes, on a 10-node committee at epoch boundary, where a returning committee member is mid-resolution. The gossip mesh might re-form before authorized_publishers fully updates.

**Has the authorized_publishers map been updated to include the new epoch's BLS?** Looking at consensus.rs:702-717 (NewEpoch handler) — it ONLY calls `peer_manager.new_epoch(committee)`. It does NOT update `authorized_publishers`. Authorized publishers are updated separately via `NetworkCommand::UpdateAuthorizedPublishers` (line 524) which has its own producer in the consensus layer.

This is a NEW gap: **the timing of UpdateAuthorizedPublishers vs NewEpoch is unsynchronized.** If a peer publishes a gossip message between NewEpoch (committee updated) and UpdateAuthorizedPublishers (auth set updated), and the peer's BLS is in the new committee, verify_gossip rejects it. Fatal penalty applied to a legitimate new committee member.

### Step B — Feynman on the new gap

**Why are NewEpoch and UpdateAuthorizedPublishers separate?** Probably for layering: peer-manager only cares about peer identity; gossipsub validation cares about per-topic authorized publishers. The two are populated by different consumers.

**What is the assumed ordering?** Most likely UpdateAuthorizedPublishers before NewEpoch, because the gossip validation must be ready before peer-manager flags the peers as trusted. But the code path doesn't enforce this; it's up to the application layer to send the commands in the right order.

**Downstream:** if NewEpoch arrives first AND a peer publishes IMMEDIATELY, the peer is penalized fatally. If UpdateAuthorizedPublishers arrives first, the peer is accepted regardless of CVV status (since the auth set authorizes it).

**Realistic likelihood:** the consensus engine's epoch-transition code path almost certainly sends both commands tightly. But the gap is unsynchronized; a slow tokio scheduler could deliver them out of order.

### Step C — masking + joint

**Defensive mask:** `verify_gossip`'s `auth.is_none()` short-circuit (consensus.rs:1000) — if `authorized_publishers.get(topic)` returns `Some(None)`, the message is unconditionally accepted. This is the "no restriction" path.

If a topic's authorized_publishers entry is set to `Some(None)`, ALL gossip is accepted. This is the unrestricted-topic mode. Is this used? `Subscribe` command (consensus.rs:595-600) takes `publishers: Option<HashSet<BlsPublicKey>>`. The caller can subscribe with `None` (unrestricted) or `Some(set)` (restricted to set). Both paths are valid.

Joint conclusion: the gap exists when (a) the topic is restricted (`Some(set)`) AND (b) the set is stale (doesn't include the new CVV BLS yet). The mask hides nothing on the unrestricted path. The bug is real for restricted topics during epoch transitions.

### Step D — convergence check

Iteration 2 produced:
- **CF-4:** unsynchronized NewEpoch vs UpdateAuthorizedPublishers timing — committee member punished mid-transition. **HIGH.**

Continuing to iteration 3.

## Iteration 3

### Step A — new gaps from iteration 2

The CF-4 finding is in scope of consensus.rs (which is in scope). Let me trace if there are any state-level gaps in handling the UpdateAuthorizedPublishers command.

The command at line 524-527: `self.authorized_publishers = authorities; send_or_log_error!(reply, Ok(()), "UpdateAuthorizedPublishers");`. Direct assignment, replaces the map wholesale. No coupling to new_epoch.

**Gap:** there is no event that ties the two together. Both are commands consumed in the single-task event loop, so they're serialized — but the caller-side ordering controls the race.

Let me grep for the upstream callers to see if there's a synchronization at the producer side. (Out of scope for the audit, but worth a one-liner.)

### Step B — extending Feynman

**Is there a "current epoch" guard in verify_gossip?** No. `verify_gossip` consults `authorized_publishers` directly. No staleness check. A message arriving from a peer whose BLS was in the PREVIOUS epoch's authorized_publishers would be ACCEPTED if the new epoch's update hasn't landed yet.

This is the symmetric problem: the OLD committee's BLS is still in `authorized_publishers` after the epoch boundary if `UpdateAuthorizedPublishers` is late. A peer that was a CVV in epoch N-1 but is NOT in epoch N's committee can still propagate gossip messages (false-positive acceptance).

### Step C — masking

**verify_gossip's accept-on-no-source-id path:** `gossip.source.is_some_and(...)` — if `source` is None, the entire predicate is false, GossipAcceptance::Reject. Reasonable.

### Step D — convergence check

Iteration 3 produced one minor refinement (the symmetric stale-old-committee accept), but it's the same root cause as CF-4 (unsynchronized command timing). **Convergence reached.** No new findings beyond what's already in CF-4.

## Loop converged at iteration 3.

## Net new findings from feedback loop

| ID | Description | Severity hypothesis |
|---|---|---|
| CF-1 | Unresolved-CVV-window penalty (Fatal) bans a future committee member; promote_committee_member can't restore kad routing entry. | HIGH |
| CF-2 | kad.rs num_records underflow triggered by restart with expired records + libp2p TTL job remove. | MEDIUM |
| CF-3 | Observer behind shared-NAT validator double-vulnerable (no observer protection + IP-ban poisoning). | HIGH |
| CF-4 | Unsynchronized NewEpoch vs UpdateAuthorizedPublishers; legitimate new CVV punished by stale gossip validation. | HIGH |

All four cross-feed findings are downstream of the unresolved-CVV window AND/OR IP-ban THRESHOLD=1 design choices, plus the missing observer-mode awareness.
