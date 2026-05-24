# Security Findings — `peer-manager-cleanup` Branch (Verified)

Date: 2026-05-21
Branch: `peer-manager-cleanup` (6 commits ahead of `main`)
Verifier: tn-findings-verifier (Verify Mode)

This report consolidates verification of the 14 non-INFO findings (6 HIGH, 6 MEDIUM, 8 LOW) produced by 10 parallel security agents against the `peer-manager-cleanup` branch. Each finding is re-derived against the actual source — verifier's job was to find evidence AGAINST the claim, not for it.

---

## Priority-question answers (TL;DR)

**Q1 — Can a remote peer cause a validator to be banned/evicted/hidden from `auth_to_peer`?**

**Mostly NO, with two caveats:**
- Once a peer is in `current_committee` and `is_trusted=true`: `Peer::apply_penalty` is a no-op (peer.rs:181-183, INV-002); `prune_*` skips it via `is_protected` (all_peers.rs:849-851 / :869-873, INV-045). Penalty-driven ban is blocked.
- **CAVEAT A (HIGH-confirmed) — Pre-epoch ban-then-evict**: A future committee member that is NOT YET trusted (penalty arrives before `new_epoch` runs) can be banned and even evicted from `bls_index` via `prune_banned_peers`. When `new_epoch` runs, the BLS is unresolved (`MissingAuthorities`), kad lookup recovers it, `upsert_peer → promote_committee_member` re-creates the peer record — but the stale `PeerManager::temporarily_banned[old_pid]` is never cleared (M2/H2-related). Net effect: validator is temporarily hidden from `auth_to_peer` until KAD resolves, and once resolved the new PeerId is trusted, but the old PeerId remains in `temporarily_banned` for up to `excess_peers_reconnection_timeout` (600s).
- **CAVEAT B (LOW)** — Shared-IP race (L7-confirmed): if a non-validator from V's IP gets banned between V losing trusted status and `new_epoch`, V's inbound connections are rejected at `sanitize_ip_addr` (behavior.rs:69-77) until `remove_validator_ip` runs at the next epoch boundary.

**Q2 — Can a remote peer impersonate a validator via BLS↔PeerId hijack?**

**NO for steady-state, YES for limited stale-record replay (H5-confirmed).**
- `peer_record_valid` (consensus.rs:386-406) checks BLS signature over `NetworkInfo` AND requires `record.publisher == verified.pubkey.into()`. Both PUT path (consensus.rs:1377) and GET path (consensus.rs:1241,1447) gate on this. An attacker without the validator's BLS private key cannot forge a new record.
- **CAVEAT (HIGH-confirmed) — H5 stale-record replay**: After `kad_record_ttl` expires locally, the only freshness gate (`is_newer_record`, consensus.rs:1413-1431) reverts to `true` for any incoming record because `store.get(&key) == None`. An attacker who archived an OLD validly-signed `NodeRecord` (signatures don't expire) can replay it via PUT; this re-pins `bls_index[bls] = old_PeerId` and routes traffic to a dead PeerId if the validator rotated. The attacker need not hold any key — they need only possess any old signed record.
- **CAVEAT (HIGH-confirmed) — H4 startup load**: Records persisted to the on-disk KAD store are loaded at startup (consensus.rs:284-298) via `decode` (panic-on-fail) and fed directly to `add_known_peer` WITHOUT re-running `peer_record_valid`. Any tampered REDB row or record persisted by an older binary is restored verbatim.

---

## Confirmed HIGH findings (sorted by severity, then by exploit feasibility)

### H1 — Orphan `Peer` records with stale `is_trusted=true` are unevictable (CONFIRMED, HIGH)

- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:107-122` (`rebind_bls`), `:849-851` (`is_protected`), `:858-893` (`collect_excess_peers`), `:912-943` (`prune_banned_peers`), `:946-973` (`prune_disconnected_peers`); `peer.rs:145-147` (`clear_bls` does NOT clear `is_trusted`); `manager.rs:490-524` (`prune_connected_peers` skips trusted).
- **Verified verdict**: CONFIRMED. The `clear_bls` method (peer.rs:145-147) only sets `bls_public_key = None`; it does not touch `is_trusted`. After rotation, `is_protected(orphan_pid) = current_committee.contains(orphan_pid) || peer.is_trusted()` returns `true` via the second disjunct because the orphan's `is_trusted` was set by the previous `promote_committee_member` call (all_peers.rs:1014). `collect_excess_peers` (`:869-873`) hits `continue` on every protected peer.
- **Exploit shape**: A current committee member rotating its libp2p keypair repeatedly under the same BLS adds one permanent orphan entry per rotation. Each rotation requires a valid KAD PUT (a signed `NodeRecord`), which any holder of the BLS key can produce — but only the actual validator (or whoever holds their BLS material) can effect this. So this is a **slow-burn DoS by a misbehaving or compromised validator**, not a remote-peer DoS. Even so, it's pre-INV-045 the orphan grew unbounded; with INV-045 nothing reaps these records.
- **Severity**: HIGH (was HIGH). Confirmed exploit requires possession of a committee BLS keypair; if an outside attacker steals one, this is one of several abuses they could do.
- **Test coverage**: `tests/peers.rs:701-743` (`committee_member_rotation_promotes_new_peer_id`) explicitly asserts the orphan survives with `is_trusted == true` — the test encodes the gap as expected behavior.
- **Proposed fix (one of three)**:
  1. **`clear_bls` also clears `is_trusted`** (`peer.rs:145-147`):
     ```rust
     pub(super) fn clear_bls(&mut self) {
         self.bls_public_key = None;
         self.is_trusted = false;
     }
     ```
     Rationale: an orphan with no BLS cannot be a committee member; persisting `is_trusted` only serves the rare case where the same `PeerId` is re-bound to a new BLS via `update_net` (in which case the next `promote_committee_member` would re-set `is_trusted` anyway).
     - Risk: if the orphan is still `Connected`, it now becomes prunable from `prune_connected_peers`. That is correct behavior — the orphan's BLS-keyed identity is no longer authoritative.
  2. **Add orphan sweeper to heartbeat** (`manager.rs:244-271`):
     - Walk `peers` for `bls_public_key == None && connection_status.is_disconnected_or_unknown()` and `remove_peer`. Caps the per-rotation footprint without touching the per-peer protection logic. Requires care that the orphan's `is_trusted` isn't being relied on elsewhere.
  3. **Re-check `current_committee.contains(pid)` in `is_protected`** and drop the `peer.is_trusted()` disjunct: requires re-thinking the "trusted at startup" pathway (`add_trusted_peer`) which also uses `is_trusted` to mark explicitly-configured trusted peers. Recommended only if (1) is too aggressive.
- **Recommended**: Option 1 (1-line change). Add a regression test asserting the orphan loses `is_trusted` and a follow-up assertion that `prune_connected_peers` can evict the orphan.
- **Similar-pattern sweep**: Other callers of `clear_bls` — only `rebind_bls` (all_peers.rs:118). Other readers of `is_trusted` outside this module — `peer.rs:328-330` (`Peer::is_trusted` getter) used by `manager.rs:597` (`peer_is_important`), `manager.rs:505` (`prune_connected_peers`), and the `is_protected` predicate itself. With fix (1) all of these correctly treat the orphan as no longer trusted.

### H2 — Mid-epoch unban-via-rotation bypasses temporary-ban rate-limiter (CONFIRMED, HIGH)

- **Location**: `all_peers.rs:156-192` (`upsert_peer` rotation arm), `:982-1019` (`promote_committee_member`), `manager.rs:601-630` (`PeerManager::new_epoch`), `manager.rs:64,280,285,291,343` (`temporarily_banned` writes / reads).
- **Verified verdict**: CONFIRMED. `promote_committee_member` (all_peers.rs:982-1019) calls `update_connection_status(.., Unbanned)` for any `Banned` / `Disconnecting{banned:true}` peer and `peer.make_trusted()` (peer.rs:338-343) which resets score to max. The unban happens INSIDE `AllPeers` — but `PeerManager::temporarily_banned` is **outside** `AllPeers`. Walking the call chain: `add_known_peer` (manager.rs:634-647) calls `self.peers.upsert_peer(...)`, which returns a `(PeerId, PeerAction::Unban(_))`. The Unban action is then applied via `apply_peer_action` (manager.rs:295-298) which only emits `PeerEvent::Unbanned(peer_id)` — it does NOT call `self.temporarily_banned.remove(&peer_id)`. The remove only happens in `PeerManager::new_epoch` (manager.rs:603-610) for BLS keys ALREADY in `bls_index`. For the rotation-arm case (where the BLS *was* indexed at a different PeerId), this loop runs once for the OLD PeerId — the new PeerId never has its temp-ban entry cleared.
  - However: was the new PeerId ever IN `temporarily_banned`? Only if the new PeerId had a prior ban — usually not. So the attack is actually narrower than the original claim.
  - **The actual exploit shape**: An attacker who already has a valid committee BLS keypair and a previously-banned PeerId can rotate to a NEW PeerId and instantly become trusted. The OLD PeerId's ban state (in `temporarily_banned` AND in the `Banned` connection-state) is bypassed for the new PeerId. The `banned_before_decay_secs` (12h) window is bypassed because `make_trusted` sets `score = max_score` directly.
- **Severity**: HIGH (was HIGH). Confirmed. Mid-epoch ban-laundering by a validator is real. The mitigating factor: requires holding the committee BLS key. An external attacker without BLS access cannot trigger this.
- **Proposed fix (architectural — flag for human review)**:
  - **Option A (strict)**: In `upsert_peer` rotation arm (all_peers.rs:175-183), check if the OLD PeerId is in any banned state and refuse promotion of the new PeerId until the next epoch boundary. This would force the validator to take the ban consequences. Requires careful semantics — `Disconnecting{banned:true}` from a transient score collapse should perhaps still be forgiven, but operator-driven bans should not be.
  - **Option B (passive)**: At promotion time, propagate the OLD PeerId's `temporarily_banned` entry forward to the new PeerId. This preserves the rate limit across rotations without forcing punishment. Implementation: add a `PeerManager::transfer_temp_ban(old_pid, new_pid)` and call it from `add_known_peer` when `upsert_peer` returns a rotation-arm `Unban`.
  - **Option C (accept as designed)**: Rotation is documented as operator-driven (invariants.md INV-047), so a validator that wants to rotate accepts they get full forgiveness. Document explicitly that BLS-keyed forgiveness IS the design and is the canonical "second-chance" mechanism for validators.
- **Recommended**: Option C with explicit documentation, escalating to Option B if operational telemetry shows abuse. Pure mid-epoch laundering requires the attacker to hold a committee BLS key — at that point they already have far more dangerous capabilities.
- **Similar-pattern sweep**: Look for other ban-state stores that aren't reset by promotion: `BannedPeers::banned_peers_by_ip` is handled by `remove_validator_ip` (banned.rs:65-78) called from `promote_committee_member` (all_peers.rs:1015) — good. `BannedPeerCache::temporarily_banned` is the only gap.

### H4 — Startup KAD-store load bypasses signature check (CONFIRMED, HIGH)

- **Location**: `crates/network-libp2p/src/consensus.rs:284-298` (`ConsensusNetwork::new` loop).
- **Verified verdict**: CONFIRMED. The loop reads each record from `kad_store.records()`, decodes via the panicking `decode` (not `try_decode`), and calls `peer_manager.add_known_peer(key, record.info.pubkey, record.info.multiaddrs)` with no signature verification. The wire PUT path (consensus.rs:1377) and GET path (consensus.rs:1241,1447) both gate on `peer_record_valid`. The startup load does NOT.
- **Trust boundary analysis**: The on-disk KAD store is operator-controlled (a REDB file inside the consensus DB). Treating it as a trust boundary is defensible IF (a) the operator runs only the official binary and (b) the binary writes only verified records. The wire-write path verifies (consensus.rs:1377). However:
  1. An older binary version may have written records without the publisher-equality check (added later) — those records are restored on upgrade.
  2. A future regression that writes without validation would be silently amplified at every restart.
  3. `decode` (panicking) on a corrupted row crashes the node before listeners start — a denial-of-service against an operator who upgrades into a corrupt-DB state.
- **Severity**: HIGH (was HIGH). Defense-in-depth violation; not directly remote-exploitable but provides forward-compatibility risk and a panic surface.
- **Proposed fix (clear, no API change)**:
  ```rust
  // crates/network-libp2p/src/consensus.rs:284-299
  for record in kad_store.records() {
      // Match the wire-PUT path: re-verify the BLS signature on every load.
      let owned = record.into_owned();
      let (key, node_record) = match Self::peer_record_valid_inner(&owned) {
          Some(v) => v,
          None => {
              warn!(target: "network-kad", "discarding invalid kad record on startup load");
              continue;
          }
      };
      behavior.peer_manager.add_known_peer(
          key,
          node_record.info.pubkey,
          node_record.info.multiaddrs,
      );
  }
  ```
  Where `peer_record_valid_inner` is `peer_record_valid` extracted to a free function or `&self`-free associated function (it doesn't actually use `self` — just `record`). Use `try_decode` instead of `decode` to convert the panic into a logged drop.
- **Similar-pattern sweep**: Search for other startup-load paths that consume persisted state without re-validation. Authorized publisher list, key-config, and BLS material are all operator-managed at startup. No other paths read network-derived records from disk and feed them to peer-manager.

### H5 — Stale-record replay after local TTL expiry (CONFIRMED, HIGH)

- **Location**: `consensus.rs:1413-1431` (`is_newer_record`), `:1377-1391` (PUT path), `kad.rs:217-251` (`evict_expired_records`).
- **Verified verdict**: CONFIRMED. `is_newer_record` returns `true` if `store.get(&record.key) == None` (consensus.rs:1428-1430). On a node where the local KAD store has evicted the record (TTL expiry — `kad_record_ttl` configured per LibP2pConfig), an attacker who possesses ANY old validly-signed `NodeRecord` for any validator can PUT it; `peer_record_valid` (consensus.rs:386-406) accepts it because (a) the BLS signature is still cryptographically valid (signatures don't expire), (b) the publisher PeerId matches the OLD record's `info.pubkey`. After acceptance, `add_known_peer → upsert_peer → rebind_bls` re-pins `bls_index[bls] = OLD_PeerId`.
- **Impact for a validator that has rotated**: the new PeerId resolves correctly via `current_committee` (because epoch promotion ran). But `auth_to_peer(bls)` returns the OLD PeerId. The send paths `SendRequest` (consensus.rs:644-657) and `OpenStream` (consensus.rs:722-747) use `auth_to_peer`, so request-response and bulk streams route to the OLD PeerId — which is no longer connected. Result: silent message loss for that validator until the validator publishes a new record at the kad publication interval (12h default).
- **Severity**: HIGH (was HIGH). Confirmed — unauthenticated impersonation of a validator's PeerId binding under attacker-controlled timing.
- **Proposed fix**: Add a monotonic per-BLS timestamp gate persisted across local store evictions. Two implementation options:
  - **Option A**: Add a separate `latest_record_timestamp: HashMap<BlsPublicKey, TimestampSec>` to `ConsensusNetwork`, persisted to a small REDB table. `is_newer_record` consults this map even when `store.get(&key) == None`. Updated on every accepted PUT.
  - **Option B**: Keep a tombstone in the KAD store for evicted records — record only the timestamp, drop the rest. Cheaper. Tombstones must themselves be capped (LRU or by-age).
  - **Option C (cheapest)**: Bound the freshness window in absolute terms: reject any record whose `info.timestamp` is older than `now() - max_record_age` (e.g., `2 * kad_publication_interval = 24h`). This still allows a 24h replay window but constrains the attack horizon.
- **Recommended**: Option C as immediate mitigation, Option A as long-term fix. Option C is ~5 lines:
  ```rust
  // in peer_record_valid, after BLS verify:
  let age = now().saturating_sub(verified.1.info.timestamp);
  if age > self.config.max_record_age {
      warn!(target: "network-kad", record_age = age, "rejecting stale record");
      return None;
  }
  ```
- **Similar-pattern sweep**: `process_kad_query_result` (consensus.rs:1439-1485) only compares timestamps between records returned by the same query — no absolute freshness gate. Same fix should be applied there.

### H6 — `bls_index` unbounded growth per accepted KAD record (PARTIALLY VALID, downgraded MEDIUM)

- **Location**: `all_peers.rs:115` (`bls_index.insert`), `consensus.rs:1377-1391` (gate is `peer_record_valid`).
- **Verified verdict**: PARTIALLY VALID. The KAD `MemoryStoreConfig::default()` caps the on-disk store (`kad.rs:198`, `:358-364`: `max_records` from libp2p default = 1024). The wire-PUT path only invokes `add_known_peer` when `is_newer_record` (consensus.rs:1379) returns true AND the store accepts the put (which can fail with `Error::MaxRecords`). However, the store check fires AFTER `add_known_peer`'s side effects on `peers` and `bls_index` — actually no, re-reading: consensus.rs:1379-1391 calls `store_mut().put(record)?` first (line 1383-1385), and only on success calls `add_known_peer`. So a hard cap of ~1024 entries is enforced on the PUT path.
- **But the GET path is different**: `close_kad_query` (consensus.rs:1488-1498) calls `add_known_peer` from a successful kad GET without touching the on-disk store. Each GET can add a peer to `bls_index` independent of the on-disk cap.
- **And the startup load (H4)** loads every record from disk into `bls_index` without an explicit count limit (bounded only by the on-disk count).
- **Net growth bound**: `bls_index` is bounded by the union of (disk-store records) + (GET-resolved unique BLS keys). With H4 fix and H5 fix in place, disk-store records are validated and timestamped, so the union grows at most by one per legitimate validator. Without H4 fix, the disk records bypass validation — but the count is still bounded by `max_records=1024`.
- **Severity**: Downgrade to MEDIUM. Not unbounded in practice. Worth tracking but not exploitable for memory exhaustion at the scale of 1024 entries.
- **Proposed fix**: Add an explicit cap on `bls_index` (e.g., 4 * expected committee size) and reject `upsert_peer` calls when at cap unless the BLS is in `current_committee_keys`. Low priority.

### H3 — `add_trusted_peer` destroys existing `Peer` state (PARTIALLY VALID, downgraded MEDIUM)

- **Location**: `all_peers.rs:127-138`.
- **Verified verdict**: PARTIALLY VALID. The unconditional `self.peers.insert(peer_id, trusted_peer)` (line 137) is real — it overwrites any existing `Peer` record at `peer_id`. However, the call chain is constrained:
  - `add_trusted_peer` is called from exactly two places: `add_trusted_peer_and_dial` (manager.rs:103-119) and itself via test code (tests/peer_manager.rs:140).
  - `add_trusted_peer_and_dial` is invoked by `NetworkCommand::AddTrustedPeerAndDial` (consensus.rs:537-545), which is dispatched only by the public `NetworkHandle::add_trusted_peer_and_dial` (types.rs:399-410).
  - **Grep results**: No non-test caller of `NetworkHandle::add_trusted_peer_and_dial` exists in the workspace (grep `add_trusted_peer_and_dial` outside tests yields zero hits in node/state-sync/network-types). The command exists for operator-CLI use but is currently not wired to any caller.
- The destructive behavior IS a defect — but it cannot be triggered today by any code path that reaches the workspace's running binary. Once an operator wires this up (e.g., CLI command, RPC admin endpoint), the destructive `insert` becomes a real liveness hazard.
- **Severity**: Downgrade to MEDIUM (was HIGH). Latent defect, no current exploit path. Should still be fixed before exposing `add_trusted_peer` to any caller.
- **Proposed fix**: Make `add_trusted_peer` idempotent — if a record exists, mutate it in place (`make_trusted`, `update_net`); only insert a fresh `Peer::new_trusted` when no prior record exists.
  ```rust
  pub(super) fn add_trusted_peer(
      &mut self,
      bls_public_key: BlsPublicKey,
      network_key: NetworkPublicKey,
      addr: Vec<Multiaddr>,
  ) {
      let peer_id: PeerId = network_key.clone().into();
      let _ = self.banned_peers.remove_banned_peer(/* ips */);
      self.rebind_bls(peer_id, bls_public_key);
      match self.peers.get_mut(&peer_id) {
          Some(peer) => {
              peer.update_net(bls_public_key, network_key, addr);
              peer.make_trusted();
          }
          None => {
              self.peers.insert(peer_id, Peer::new_trusted(bls_public_key, network_key, addr));
          }
      }
  }
  ```

---

## Confirmed MEDIUM findings

### M1 — First-arrival race in `bls_index` (CONFIRMED, MEDIUM)

- **Location**: `all_peers.rs:107-122` (`rebind_bls`), `:156-192` (`upsert_peer`), `consensus.rs:1377-1391` (PUT gate), `:1413-1431` (`is_newer_record`).
- **Verified verdict**: CONFIRMED. `upsert_peer` always overwrites `bls_index[bls]` regardless of the incoming record's timestamp. The `is_newer_record` check exists only in the PUT path (consensus.rs:1379). The GET path (`process_kad_query_result` → `close_kad_query` → `add_known_peer`) and the startup path do not consult timestamps; they overwrite based on arrival order.
- **Impact**: Two honest nodes whose KAD GET queries return different records for the same BLS in different orders end up with different `auth_to_peer` mappings. Consensus authenticates on BLS (not PeerId), so this is per-node routing divergence — not a fork — but messages routed to a stale PeerId are silently dropped.
- **Severity**: MEDIUM. Confirmed.
- **Proposed fix**: Make `upsert_peer` consult the existing peer's stored timestamp and reject older records. Requires adding a per-record timestamp to `Peer` (which was REMOVED in commit `78f74891`). The cleaner fix is to keep an in-memory `record_timestamp: Option<TimestampSec>` on `Peer` (no on-disk impact — that was the original concern) and compare in `upsert_peer`:
  ```rust
  pub(super) fn upsert_peer(
      &mut self,
      bls_public_key: BlsPublicKey,
      network_key: NetworkPublicKey,
      addrs: Vec<Multiaddr>,
      record_timestamp: TimestampSec,  // NEW
  ) -> Option<(PeerId, PeerAction)> {
      let peer_id: PeerId = network_key.clone().into();
      // reject if we have a newer record
      if let Some(existing_pid) = self.bls_index.get(&bls_public_key) {
          if let Some(existing) = self.peers.get(existing_pid) {
              if existing.record_timestamp() > Some(record_timestamp) {
                  return None;
              }
          }
      }
      // ... rest unchanged
  }
  ```
  Note: the timestamp must be passed through `add_known_peer` from the call sites (consensus.rs:1387, :1491, :548) — three changes plus signature update.

### M2 — `temporarily_banned` shadow persists after BLS-index eviction + KAD recovery (CONFIRMED, MEDIUM)

- **Location**: `manager.rs:601-630` (`PeerManager::new_epoch`), `:276-302` (`apply_peer_action`), `all_peers.rs:982-1019` (`promote_committee_member`).
- **Verified verdict**: CONFIRMED. Re-deriving: pre-epoch the future validator V is penalty-flooded (V is not yet trusted; INV-002 does not protect them yet), V's score drops to `Banned`, `apply_peer_action` inserts V_pid into `temporarily_banned` (manager.rs:280), `prune_banned_peers` may evict V from `peers` and `bls_index`. `new_epoch` runs: V's BLS is not in `bls_index`, so the `temporarily_banned.remove(&peer_id)` loop at manager.rs:603-610 does NOT fire for V. `MissingAuthorities` emitted; kad recovers V; `add_known_peer → upsert_peer → promote_committee_member` re-creates V with `is_trusted=true`. The promotion emits `PeerAction::Unban(_)` which is applied via `apply_peer_action` (manager.rs:295-298) — only pushes `PeerEvent::Unbanned`, does NOT scrub `temporarily_banned`. V's reconnect attempts hit `peer_banned` (manager.rs:343) for up to `excess_peers_reconnection_timeout` (600s).
- **Severity**: MEDIUM. Confirmed. Liveness gap for newly-promoted committee members that were banned pre-epoch.
- **Proposed fix**: Scrub `temporarily_banned` when an `Unban` action is applied as part of committee promotion. In `apply_peer_action` (manager.rs:295-302):
  ```rust
  PeerAction::Unban(ip_addrs) => {
      debug!(...);
      self.temporarily_banned.remove(&peer_id);  // NEW
      self.push_event(PeerEvent::Unbanned(peer_id));
  }
  ```
  This is safe because the only path emitting `Unban` today is committee promotion (`promote_committee_member` calls `update_connection_status(.., Unbanned)`); the unconditional scrub mirrors the existing scrub in `new_epoch` for already-resolved committee members (manager.rs:606).

### M3 — `dial_requests` VecDeque is unbounded (REJECTED — INFORMATIONAL)

- **Location**: `manager.rs:45,180,209-211`.
- **Verified verdict**: REJECTED at MEDIUM. Re-deriving: producers of `dial_requests` are (a) `dial_peer` itself, called from `NetworkCommand::Dial`, `NetworkCommand::DialBls`, and `add_trusted_peer_and_dial`, and (b) `discovery_heartbeat` for kad-discovered peers. The command channel is bounded at 100 (consensus.rs:328). `discovery_heartbeat` bounds its own work by `peers_needed = target_num_peers - connected_or_dialing` and only emits `target_num_peers` dials max per heartbeat. Producer rate is bounded. The consumer drains 1-per-poll, but `poll` is called on every swarm-tick — typically multiple times per second. Under any realistic load `dial_requests.len()` stays under a few dozen. The lack of a cap is a code smell but not a real DoS vector.
- **Severity**: Downgrade to INFORMATIONAL. Document the implicit bound rather than add an explicit cap.

### M4 — Heartbeat work scales linearly with `peers` size (CONFIRMED, LOW)

- **Location**: `all_peers.rs:816-825` (`connected_peers_by_score_and_routability`), `:285-315` (`update_peer_scores`), `manager.rs:244-271`.
- **Verified verdict**: CONFIRMED but low impact. The O(N log N) sort in `connected_peers_by_score_and_routability` runs every heartbeat (30s default). With realistic peer counts (target ~6, max ~30 + dialing ~5), N stays under 50 and the cost is microseconds. The amplification claim ("H1 orphans inflate N") is real but bounded by the natural ceiling on `peers` (orphans only accumulate per rotation; rotations are operator-driven and rare).
- **Severity**: Downgrade to LOW. Worth tracking but not actionable until peer counts grow by 100x.
- **Proposed fix**: Defer until H1 fix lands; orphan accumulation is the only realistic growth driver.

### M5 — `BannedPeers::banned_peers_by_ip` and `Peer::multiaddrs` uncapped (CONFIRMED, MEDIUM)

- **Location**: `banned.rs:28,81-87` (`banned_peers_by_ip`), `peer.rs:132,220,239` (`multiaddrs` extension).
- **Verified verdict**: CONFIRMED.
  - `banned_peers_by_ip` is a `HashMap<IpAddr, usize>` with no cap. The OUTER ban count is capped at `max_banned_peers=100`, but each banned peer can contribute many IP entries (one per multiaddr observed). A long-lived banned peer that rotates listening addresses can extend the per-peer `multiaddrs` set, and each `add_banned_peer` call enumerates that set into `banned_peers_by_ip`.
  - `Peer::multiaddrs: HashSet<Multiaddr>` is extended without bound in `update_net` (peer.rs:132), `register_incoming` (`:220`), `register_outgoing` (`:239`). A misbehaving peer rotating addresses can grow this set across the peer's lifetime.
- **Severity**: MEDIUM. Confirmed. Realistic exploit: long-lived banned peer that opens many subconnections from different addresses (or publishes records advertising many addresses). Per-peer memory growth is bounded by Multiaddr serialization cost (~ hundreds of bytes per address) times the rotation rate.
- **Proposed fix**:
  - Cap `Peer::multiaddrs` at e.g. 32 entries with LRU eviction. Add a small `VecDeque<Multiaddr>` wrapper:
    ```rust
    fn add_multiaddr(&mut self, addr: Multiaddr) {
        if !self.multiaddrs.insert(addr.clone()) { return; }
        // optional: track insertion order for eviction
        if self.multiaddrs.len() > MAX_OBSERVED_ADDRS_PER_PEER {
            // drop oldest, requires VecDeque
        }
    }
    ```
  - `banned_peers_by_ip` doesn't need its own cap if `multiaddrs` is capped — its size is bounded by `max_banned_peers * MAX_OBSERVED_ADDRS_PER_PEER`.

### M6 — `add_trusted_peer_and_dial` defensive gap (REJECTED — same as H3)

- **Location**: `manager.rs:99-119`, `consensus.rs:537-545`.
- **Verified verdict**: REJECTED as separate finding. This is the same concern as H3: any future caller of `add_trusted_peer_and_dial` reaching the destructive `add_trusted_peer` path. With H3 fix (idempotent add), this concern dissolves. No additional remediation needed.

---

## Confirmed LOW findings

### L1 — INV-046 has no liveness retry (CONFIRMED, LOW)

- **Location**: `manager.rs:622-629` (`new_epoch` one-shot emission), `:649-664` (`find_authorities` exists but not called from heartbeat), `consensus.rs:1149-1155, 1261-1275`.
- **Verified verdict**: CONFIRMED. `MissingAuthorities` is emitted once per `new_epoch`; on KAD GET failure (`FinishedWithNoAdditionalRecord` at consensus.rs:1261, or `Err` at `:1267`), `close_kad_query` is called but no retry is scheduled. `find_authorities` (manager.rs:649-664) exists but is only called from `NetworkCommand::FindAuthorities`, which has no in-process scheduled caller — only operator-driven via `NetworkHandle::find_authorities`.
- **Severity**: LOW. A KAD partition that drops the lookup leaves the validator unresolved until the next epoch boundary (typically minutes). Consensus tolerates this by routing via other validators.
- **Proposed fix**: Add a periodic re-scan in `heartbeat` that collects `current_committee_keys` entries with value `None` and emits a fresh `PeerEvent::MissingAuthorities`:
  ```rust
  // in PeerManager::heartbeat, after discovery_heartbeat:
  let unresolved: Vec<_> = self.peers.unresolved_committee_keys().collect();
  if !unresolved.is_empty() {
      self.events.push_back(PeerEvent::MissingAuthorities(unresolved));
  }
  ```
  Where `unresolved_committee_keys` is a new accessor on `AllPeers` that iterates `current_committee_keys` and returns the `None` entries.

### L2 — `NodeRecord` signature does not bind `publisher` (CONFIRMED, LOW — by design)

- **Location**: `types.rs:583-617` (signing payload), `consensus.rs:386-406` (publisher equality check).
- **Verified verdict**: CONFIRMED. The signed payload is `encode(NetworkInfo { pubkey, multiaddrs, timestamp })`; the `publisher: Option<PeerId>` field on the wrapping `kad::Record` is not in the signing payload. `peer_record_valid` does a runtime check (consensus.rs:395-403) that `record.publisher == verified.1.info.pubkey.into()`. This check works because `PeerId` is derived from `pubkey`, so the two values are inherently bound — but the binding is by derivation, not cryptographic signature.
- **Severity**: LOW. The runtime equality check is correct and provides equivalent guarantees. Worth noting as a brittle point for future refactors.
- **Proposed fix**: Include `publisher` in the signed payload as a defense-in-depth measure. Requires a wire format change (breaking) — defer to the next protocol version bump.

### L3 — `kad_record_queries` HashMap grows per outstanding lookup (CONFIRMED, LOW)

- **Location**: `manager.rs:601-630, 649-664`, `consensus.rs:1149-1155`.
- **Verified verdict**: CONFIRMED. `kad_record_queries` is grown by one entry per `MissingAuthorities` BLS key. Bounded in practice by committee size (default ~4-10 validators) and the absence of attacker amplification (only committee BLS keys reach `MissingAuthorities`). Cleanup happens in `close_kad_query` (consensus.rs:1488). If kad timeouts misfire, entries could accumulate — but kad has its own 60s query timeout (consensus.rs:274), so leaks are bounded.
- **Severity**: LOW. No realistic memory growth. Worth adding a hard cap (e.g. 4 * committee size) as defense.

### L4 — `pending_dials` overwrites prior oneshot (CONFIRMED LOW; documented at A1)

- **Location**: `all_peers.rs:64,373-383`.
- **Verified verdict**: CONFIRMED. `register_dial_attempt` does `pending_dials.insert(peer_id, reply)` (line 381), overwriting any prior sender. The first caller's `oneshot::Receiver` is dropped. Caller-side timeouts mitigate the hang. The invariants doc already documents this at A1.
- **Severity**: LOW.
- **Proposed fix**: Either add a caller-side timeout doc requirement in the public `NetworkHandle::dial_by_bls` (types.rs:439) or change `pending_dials` to `HashMap<PeerId, Vec<oneshot::Sender<_>>>` and broadcast results.

### L5 — Unknown peer + `Banned` status inconsistency (REJECTED — unreachable)

- **Location**: `all_peers.rs:246-280`, `peer.rs:178-184, 196`.
- **Verified verdict**: REJECTED. The path requires `update_connection_status(unknown_peer_id, Banned)`. Tracing callers of `update_connection_status` with `NewConnectionStatus::Banned`: it's only called from `process_penalty` (all_peers.rs:212), which itself early-returns at `:233` if `self.peers.get_mut(peer_id).is_none()`. So the unknown-peer + Banned path is unreachable. The defensive code in `ensure_peer_exists` (`:267-274`) handles the (impossible) case but does NOT introduce inconsistency in practice.
- **Severity**: REJECTED.

### L6 — Peer-exchange leak surface (REJECTED — currently safe, latent)

- **Location**: `all_peers.rs:798-810`.
- **Verified verdict**: REJECTED at LOW. `peer_exchange` correctly filters by `connection_status().is_connected()` AND `bls_public_key.is_some()`. Orphans are excluded. The claim is "future code paths might leak" — that's a hypothetical, not a current defect. INV-031 holds today.
- **Severity**: INFORMATIONAL.

### L7 — Shared-IP race window before `new_epoch` (CONFIRMED, LOW)

- **Location**: `behavior.rs:69-77` (`handle_pending_inbound_connection`), `banned.rs:60-78` (`remove_validator_ip`), `all_peers.rs:1033-1056` (`new_epoch`).
- **Verified verdict**: CONFIRMED. `sanitize_ip_addr` rejects inbound connections from any IP in the banned set. `remove_validator_ip` runs only at epoch boundary (called from `promote_committee_member`). Between epoch boundaries, a validator sharing an IP with a banned peer cannot accept incoming connections. Dial-side retries mitigate (the validator can still initiate outgoing), but the asymmetry can stall reconnection for up to one epoch duration (configured per-network).
- **Severity**: LOW. Confirmed but engineerable only by timing penalties to epoch boundaries.
- **Proposed fix**: When `add_known_peer` runs for a BLS that's already in `current_committee_keys`, call `remove_validator_ip` eagerly. Trivial addition in `add_known_peer` (manager.rs:634-647).

### L8 — KAD lookup race transient `auth_to_peer` divergence (CONFIRMED, LOW)

- **Location**: `all_peers.rs:107-122, 156-192`, `consensus.rs:1377-1431`.
- **Verified verdict**: CONFIRMED. Same root cause as M1. Divergence converges via publisher-stamped timestamp at the PUT path. Consensus authenticates on BLS (not PeerId), so this is per-node routing divergence — eventually consistent. Bounded by gossip closure (typically seconds).
- **Severity**: LOW. Subsumed by M1 fix.

---

## Rejected findings (audit trail)

| ID | Why rejected |
|----|--------------|
| M3 | Producer rates externally bounded (100-cap command channel + discovery target). Consumer drain is per-poll, not per-heartbeat. Realistic `dial_requests.len()` under any load is <50. |
| M4 | Confirmed but downgraded to LOW. Realistic N stays under 50; sort cost is microseconds. Amplification depends on H1 orphan accumulation, which requires operator-driven rotations. |
| M6 | Same root concern as H3. Once H3 is fixed (idempotent `add_trusted_peer`), M6 dissolves. |
| L5 | Code path unreachable — `process_penalty` early-returns for unknown peers (all_peers.rs:233-235), so `update_connection_status(unknown_pid, Banned)` cannot fire from the only caller. |
| L6 | Hypothetical-only; INV-031 holds today. No current code path leaks orphan data. |
| H6 (partial) | Downgraded to MEDIUM. KAD store cap of `max_records=1024` bounds the PUT path; GET path adds a small amount per legitimate validator. Not realistic memory exhaustion. |
| H3 (partial) | Downgraded to MEDIUM. Destructive `insert` is real but no non-test caller reaches it today. Latent defect worth fixing before exposing the API. |

---

## Invariant verification matrix

Invariants documented in `crates/network-libp2p/src/peers/invariants.md` (INV-001..INV-047 with skips; Appendix A1-A10). Each mapped to ENFORCED / DOCUMENTED-BUT-NOT-ENFORCED / OUT-OF-SCOPE.

| Invariant | Status | Citation / Notes |
|-----------|--------|------------------|
| INV-001 | ENFORCED | `score.rs:84-100, 103-106` (`apply_penalty` + `add` clamp) |
| INV-002 | ENFORCED | `peer.rs:181-183` (the `if !self.is_trusted` guard) |
| INV-003 | ENFORCED | `score.rs:84-100, 123-136` (only negative deltas; decay multiplies in (0,1]) |
| INV-004 | ENFORCED | `score.rs:141-155` (the `last_updated += banned_before_decay()` at `:153`) |
| INV-005 | ENFORCED | `peer.rs:294-325` (the `if !self.is_trusted` guard at `:295`) |
| INV-006 | ENFORCED | `score.rs:123-136` |
| INV-007 | ENFORCED | `all_peers.rs:48` + `peer.rs:219-233, 238-252` |
| INV-008 | ENFORCED | `peer.rs:158-164` (pure function of score + 2 thresholds) |
| INV-009 | ENFORCED | `peer.rs:158-164` (exhaustive non-overlapping match) |
| INV-010 | ENFORCED | `all_peers.rs:203-205` (`process_penalty` no-op on equal); `manager.rs:379-391` (`process_ban` retain) |
| INV-011 | ENFORCED | `banned.rs:20, 95-108, 114-116` |
| INV-012 | ENFORCED | `all_peers.rs:135` (`add_trusted_peer`); `:1015` (`promote_committee_member` calls `remove_validator_ip`) |
| INV-013 | ENFORCED | `manager.rs:343` (`peer_banned` ORs `temporarily_banned` and `peers.peer_banned`) |
| INV-014 | ENFORCED | `cache.rs` (`heartbeat`); `manager.rs:529-533` (`unban_temp_banned_peers`) |
| INV-015 | ENFORCED | `manager.rs:267` (sole call site in heartbeat) |
| INV-016 | ENFORCED | `all_peers.rs:911-943` (`prune_banned_peers`); `manager.rs:482-484` (event extension) |
| INV-017 | ENFORCED | `manager.rs:73-97` (constructor; no on-disk reads of ban state) |
| INV-018 | ENFORCED | `all_peers.rs:405-438` (`handle_status_transition` exhaustive match) |
| INV-019 | ENFORCED | `peer.rs:219-252` (counter maintenance + reset on non-Connected entry) |
| INV-020 | ENFORCED | `all_peers.rs:441-473` (`handle_connected_transition` Banned arm logs + clears) |
| INV-021 | ENFORCED | `peer.rs:273-281`; `manager.rs:131-144` (`dial_peer` Banned arm) |
| INV-022 | ENFORCED | `all_peers.rs:512-545` (`handle_disconnected_transition` Disconnected arm is empty) |
| INV-023 | ENFORCED | `behavior.rs:217-262`; `peer.rs:219-233` (accepts any prior state) |
| INV-024 | ENFORCED | `all_peers.rs:373-383` (`pending_dials.insert` overwrites) |
| INV-025 | ENFORCED | `manager.rs:122-181` (`dial_peer` filters); `:209-211` (`next_dial_request` is pure pop). Gap at A2 documented. |
| INV-026 | ENFORCED | `behavior.rs:294-303`; `all_peers.rs:394-399` (`notify_dial_result`) |
| INV-027 | ENFORCED | `behavior.rs:23-65` (kad-dial intercept); `:181` (manager-dial registration) |
| INV-028 | ENFORCED | `manager.rs:738-796` (`discovery_heartbeat` prune block at `:772-787`) |
| INV-029 | ENFORCED | `manager.rs:559-562, 723-725, 730-735` (`eligible_for_discovery` filters in both intake paths) |
| INV-030 | ENFORCED | `manager.rs:788-792` (else-branch emits Discovery event) |
| INV-031 | ENFORCED | `all_peers.rs:798-810` (filter on `is_connected()` AND `bls_public_key.is_some()`) |
| INV-032 | ENFORCED | `manager.rs:601-630`; `all_peers.rs:1033-1056` (calls `promote_committee_member` which sets trusted + clears IP ban). NOTE: For pre-epoch banned validators that get evicted, see M2 — `temporarily_banned` is NOT cleared (gap). |
| INV-033 | ENFORCED | `manager.rs:490-524` (filter on `is_peer_validator || is_trusted` at `:504-509`) |
| INV-034 | ENFORCED | `all_peers.rs:993-1007` (`promote_committee_member` Banned + Disconnecting{banned:true} arms emit Unban) |
| INV-035 | ENFORCED | `behavior.rs:165-167` (sole call site in `poll`) |
| INV-036 | ENFORCED | `manager.rs:244-271` (literal sequence at `:246, :264, :267, :270`) |
| INV-037 | ENFORCED | `all_peers.rs:290-307` (`heartbeat_maintenance`); notify at `:537-540` |
| INV-038 | ENFORCED | `all_peers.rs:320-350` (`update_peer_scores` drops Banned/Disconnect arms with error) |
| INV-039 | ENFORCED | `behavior.rs:69-77`; `:199-207` (`sanitize_ip_addr`); `manager.rs:691-704` |
| INV-040 | ENFORCED | `behavior.rs:79-93, 95-113`; `manager.rs:336-344` (`peer_banned`) |
| INV-041 | ENFORCED | `behavior.rs:184-189` (only `ToSwarm::Dial`) |
| INV-042 | ENFORCED | `consensus.rs:1137-1143` (`PeerEvent::Banned` consumer calls `gossipsub.blacklist_peer` and `kademlia.remove_peer`) |
| INV-043 | ENFORCED | `all_peers.rs:107-122` (`rebind_bls`); `:899-909` (`remove_peer`); `peer.rs:145-147` (`clear_bls`); test coverage `tests/peers.rs:645-693` |
| INV-044 | ENFORCED | `all_peers.rs:115-121`; `peer.rs:145-147`; test `tests/peers.rs:675-693`. But INV-044 leaves the orphan with `is_trusted=true` — see H1 for the related defect. |
| INV-045 | ENFORCED (with H1 caveat) | `all_peers.rs:849-851, 869-873, 926-932, 958-964` (is_protected + early-continue in collect_excess_peers + warn-on-shortfall). H1 confirms the orphan-trusted growth gap; the protection itself works as documented. |
| INV-046 | ENFORCED (one-shot only — see L1) | `all_peers.rs:1042-1048` (collects unresolved); `manager.rs:613-629` (pushes MissingAuthorities); `consensus.rs:1149-1155` (consumer). One-shot per `new_epoch`; no retry — see L1. |
| INV-047 | ENFORCED | `all_peers.rs:982-1019` (`promote_committee_member` single funnel); `:170-185` (`upsert_peer` rotation arm evicts old PeerId before promotion); tests at `tests/peers.rs:811-845, 1042-1126`. |
| A1 | DOCUMENTED-BUT-NOT-ENFORCED | `all_peers.rs:381` (overwriting `pending_dials.insert`) — see L4. |
| A2 | DOCUMENTED-BUT-NOT-ENFORCED | `manager.rs:209-211` (`next_dial_request` no re-validation). Mitigated by `behavior.rs:95-113` outbound-banned check. |
| A3 | DOCUMENTED-BUT-NOT-ENFORCED (intentional) | Per INV-017. Intentional design. |
| A4 | DOCUMENTED-BUT-NOT-ENFORCED (intentional) | Architectural; documented. |
| A5 | DOCUMENTED-BUT-NOT-ENFORCED | `behavior.rs:69-113` confirmed IP-ban check only fires on new connections. |
| A6 | DOCUMENTED-BUT-NOT-ENFORCED | Narrow gap; `Score::telcoin_score` is `pub(super)` to `score.rs` only. |
| A7 | DOCUMENTED-BUT-NOT-ENFORCED | `manager.rs:343` ORs them correctly via `peer_banned`. M2 is the manifestation of the disagreement. |
| A8 | DOCUMENTED-BUT-NOT-ENFORCED | `peer.rs:222, 241` increment without cap. No remote exploit today (libp2p caps per-protocol connections). |
| A9 | DOCUMENTED-BUT-NOT-ENFORCED | Two config knobs default to same value. Operator override could cause drift. |
| A10 | **EXTENDED BY H1** | `rebind_bls` (all_peers.rs:107-122) clears BLS but not `is_trusted`. H1 confirms this extends the orphan retention from just `Connected` (documented A10) to ALL connection states via `is_protected`. Recommended fix elevates A10 to enforced. |

---

## Recommended actions (prioritized)

| # | Action | Severity | Effort | Notes |
|---|--------|----------|--------|-------|
| 1 | **H4 fix**: re-run `peer_record_valid` on every record loaded from `kad_store.records()` at startup; replace `decode` with `try_decode` | HIGH | XS (15 lines, 1h) | Pure additive; no behavior change for valid records |
| 2 | **H5 fix (immediate)**: reject records whose `info.timestamp` is older than `now() - max_record_age` (config knob, default 24h) | HIGH | S (10 lines + config, 2h) | Reduces stale-replay window to bounded horizon |
| 3 | **H5 fix (long-term)**: persist per-BLS latest-timestamp table; consult in `is_newer_record` even when store entry evicted | HIGH | M (new REDB table, 1d) | Eliminates replay window entirely |
| 4 | **H1 fix**: clear `is_trusted` inside `clear_bls` (peer.rs:145-147); add regression test that prunable-after-rotation | HIGH | XS (3 lines + test, 1h) | Closes the orphan retention gap |
| 5 | **M2 fix**: scrub `temporarily_banned` in `apply_peer_action` Unban arm | MEDIUM | XS (1 line, 30m) | Restores liveness for pre-epoch banned validators |
| 6 | **H3 fix**: make `add_trusted_peer` idempotent (mutate-in-place when record exists) | MEDIUM | S (10 lines + test, 1h) | Hardens the public API surface before it's wired up |
| 7 | **M5 fix**: cap `Peer::multiaddrs` with LRU eviction (e.g., max 32) | MEDIUM | S (~30 lines, 3h) | Bounds per-peer memory under address rotation |
| 8 | **L1 fix**: re-emit `MissingAuthorities` in heartbeat for still-unresolved committee BLS | LOW | XS (10 lines, 1h) | Improves partition recovery |
| 9 | **L7 fix**: call `remove_validator_ip` eagerly in `add_known_peer` when BLS is already in `current_committee_keys` | LOW | XS (5 lines, 30m) | Closes the shared-IP race window |
| 10 | **M1 fix**: re-add per-`Peer` `record_timestamp` (in-memory only) and consult in `upsert_peer` | MEDIUM | M (signature changes through 3 callers, 1d) | Eliminates first-arrival race |
| 11 | **H2 (decision)**: document forgiveness-by-rotation as intentional OR add ban-state transfer in `upsert_peer` rotation arm | HIGH | M (architectural decision + impl) | Requires human review — see H2 options |
| 12 | **L4 fix**: change `pending_dials` to `HashMap<PeerId, Vec<oneshot::Sender<_>>>` OR document caller-timeout requirement | LOW | S (~20 lines, 2h) | Fixes A1 |
| 13 | **L3 fix**: cap `kad_record_queries` (e.g., 4 * committee size) | LOW | XS (5 lines, 30m) | Defense in depth |
| 14 | **L2 (defer)**: include `publisher` in signed payload | LOW | M (wire format break, deferred) | Wait for next protocol bump |

Effort legend: XS = under 1h, S = 1-4h, M = 1-3 days.
