# Phase 6 — Verification + Domain Invariant Cross-Check

For each finding, verify reproduction trace, check against known false-positive shapes (bug-patterns.md §9), and apply downgrade rules.

## Verdict per finding

### F-1 / CF-1 — Unresolved-CVV penalty-window self-DoS

**Verdict: TRUE POSITIVE — HIGH**

- Trace: peer manager penalizes V_x's PeerId (untrusted, BLS unresolved). After kad resolves, promote_committee_member unbans BUT kademlia.remove_peer was called on the Banned event handler; no re-add path. temporarily_banned still contains V_x.pid.
- Bug-patterns false-positive shapes:
  - Not a "HashMap-only-for-get" pattern (concrete state-machine mutation).
  - The promote_committee_member path was traced end-to-end. No cancellation path masks the loss.
- INV cross-check: violates the spirit of INV-002 ("trusted peers cannot be banned") because V_x is banned *before* becoming trusted; INV-032 (new_epoch removes temporarily_banned per committee member) is only applied at new_epoch, not during the unresolved window.
- tn-domain-networking invariants (boundary INV-042): PeerEvent::Banned propagates to gossipsub+kad; the inverse (Unbanned → re-add to kad) is NOT enforced.
- **Confidence: HIGH** — full path traced through 24 sequential steps in S1.

### F-2 — Orphan accumulation with is_trusted preserved

**Verdict: TRUE POSITIVE — MEDIUM**

- Trace: each PeerId rotation via the rotation arm preserves the old PeerId's `is_trusted` flag (set in a prior promotion via `make_trusted`, never cleared). The orphan is_protected via `is_trusted`, survives pruning indefinitely.
- Bug-patterns: state-atomicity / coupled-state gap (peer.is_trusted ↔ current_committee). Asymmetric delete pattern (pattern 4.5).
- INV cross-check: Appendix A10 documented (orphan eviction NOT enforced); INV-045 (committee + trusted protected from pruning) is the mechanism that keeps the orphan alive.
- **Confidence: HIGH** — Scenario S5 walks the K-rotation path. Test gap confirmed.

### F-3 / Appendix A2 — next_dial_request no re-gate

**Verdict: TRUE POSITIVE — LOW**

- Documented in invariants.md Appendix A2. Code matches description. Failure mode is error-log spam + wasted dial attempt; the swarm-level peer_banned check at established-outbound catches the actual ban.
- **Confidence: HIGH** (documented).

### F-4 / CF-2 — kad.rs num_records underflow

**Verdict: TRUE POSITIVE — MEDIUM**

- Code at kad.rs:389 confirmed: `self.num_records -= 1` unchecked.
- Asymmetry confirmed: kad.rs:249 evict_expired uses saturating_sub; kad.rs:380 remove does not.
- Trigger requires `db.remove` to return Ok for an already-removed key. Standard KV store behavior.
- Bug-patterns: panic-surface 5.3 (integer overflow in release).
- No INV in invariants.md (kad.rs is outside peers/). Recommend adding to a new appendix or doc.
- **Confidence: MEDIUM** — bug is real, but reachability depends on libp2p-kad's exact remove semantics. Easy to verify with the proposed test.

### F-5 / Appendix A8 — num_in/num_out u8 cap absent

**Verdict: TRUE POSITIVE — LOW**

- Code confirmed: peer.rs:223, 242 → `*num_in += 1`, `*num_out += 1` without cap.
- Appendix A8 documented.
- u8 wrap at 256 → counter to 0, peer still Connected. INV-019 violated in counter terms.
- **Confidence: HIGH** (documented).

### F-6 / CF-3 — Observer not protected from prune_connected_peers

**Verdict: TRUE POSITIVE — HIGH**

- The peers/ layer has no concept of NodeMode. `is_peer_validator` = `current_committee.contains`, only CVVs.
- state-sync explicitly spawns observer-mode tasks (state-sync/lib.rs:64-95) that depend on connectivity to CVV peers.
- User direction was explicit: "observers MUST remain discoverable to follow consensus — lack of differentiation at PeerManager is a real defect, flag observer eviction / observer-cannot-fetch-state findings as MEDIUM/HIGH".
- No INV currently codifies observer-mode protection. Propose new INV-048.
- Bug-patterns: state-atomicity coupling miss + concurrency.
- **Confidence: HIGH** — explicit gap.

### F-7 — GossipsubNotSupported = Fatal on legitimate quirks

**Verdict: TRUE POSITIVE — MEDIUM**

- consensus.rs:824 confirmed.
- INV-002 protects CVVs (trusted) but the penalty is `Fatal` — sets score to MIN BEFORE the trusted check matters. Re-reading peer.rs:181: `if !self.is_trusted { self.score.apply_penalty(penalty); }`. So trusted peers ARE protected; the penalty is no-op for them.
- **The issue is non-CVV peers** (observers, NVVs, transient peers). They ARE penalized Fatal.
- Bug-patterns: error-propagation 7.3 (catch-all match arm — the GossipsubNotSupported variant unconditionally maps to Fatal).
- **Confidence: HIGH** — penalty severity is design-level.

### F-8 / S4 — IP-ban THRESHOLD=1 + shared NAT validator

**Verdict: TRUE POSITIVE — MEDIUM**

- INV-011 explicitly says threshold 1 = "two or more banned peers from the IP". The behavior matches the documented INV.
- The gap is that CONNECTION ACCEPTANCE consults `has_valid_unbanned_ips` WITHOUT a CVV exception. CVV-protective layers (is_protected, is_peer_validator) gate pruning + penalty, not connection acceptance.
- Scenario S4 traces the exact ingress denial path.
- **Confidence: HIGH** — explicit asymmetric protection.

### F-9 — disconnected_peers asymmetric arithmetic

**Verdict: TRUE POSITIVE — LOW**

- Code at all_peers.rs:529, :572, :687 uses unchecked `+= 1`. Counterpart `saturating_sub` widespread.
- No current double-counting path identified in audit. Defensive smell.
- Bug-patterns: defensive code analysis (rule 5).
- **Confidence: MEDIUM** — bug is present in the form of asymmetric defense; runtime impact requires a path I didn't find.

### F-10 — KadStore restart load + Fatal window

**Verdict: TRUE POSITIVE — MEDIUM**

- consensus.rs:284-299 confirmed: records loaded BEFORE provide_our_data + before any new_epoch.
- Default-scored peers (NOT trusted) until first new_epoch.
- Any Fatal penalty in this window bans a future committee member.
- This is a refinement of CF-1's scenario; the window is restart-specific. Overlaps with S1 but the trigger is different (restart vs late-arriving CVV).
- **Confidence: HIGH** — clear window.

### F-11 / S7 — open_epoch_pack 30s timeout no retry

**Verdict: TRUE POSITIVE — LOW**

- Documented in source comments and traced in S7. Node fails to start under sustained partition. Operational concern, not a bug in correctness.
- **Confidence: HIGH** — node-level liveness.

### F-12 — log spam under protected overshoot

**Verdict: TRUE POSITIVE — LOW**

- INV-045 accepts the overshoot intentionally. The warn-log every heartbeat is annoying but not incorrect.
- **Confidence: HIGH** — documented intentional.

### CF-4 — NewEpoch vs UpdateAuthorizedPublishers timing

**Verdict: TRUE POSITIVE — HIGH**

- Two commands consumed serially by the network main loop. The caller (consensus engine) sends them as two separate `.await` calls. If the consensus task is preempted between them, peer-manager sees committee updated but verify_gossip uses stale authorized_publishers.
- Scenario S1 step 12 traces the Path A failure.
- Bug-patterns: concurrency 1.4 (send/recv race).
- No INV (cross-layer ordering, outside peers/).
- **Confidence: MEDIUM** — depends on whether the upstream caller is synchronous in ordering. Audit of the producer side is out of scope; the network-side code path treats them independently which is the gap.

## Downgrade rules applied

**Rule: existing direct test coverage in `crates/network-libp2p/src/peers/tests/peers.rs` → downgrade one notch.**

| Finding | Has direct test? | Action |
|---|---|---|
| F-1 (CF-1) | NO direct test for unresolved-CVV window penalty | No downgrade. HIGH stays. |
| F-2 | Tests cover the rotation (`peer_id_rotation_same_bls_orphans_old_peer`) BUT not the accumulation under K rotations. The single-rotation test passes; the unbounded-growth case isn't tested. No downgrade. MEDIUM stays. |
| F-3 | Documented A2; no specific test pinning the gap. No downgrade. LOW stays. |
| F-4 | NO test (and no existing tests panic-trigger). No downgrade. MEDIUM stays. |
| F-5 | NO test for u8 wrap or per-peer cap. No downgrade. LOW stays. |
| F-6 | NO test (peers tests don't know about NodeMode). No downgrade. HIGH stays. |
| F-7 | NO test pinning GossipsubNotSupported severity. No downgrade. MEDIUM stays. |
| F-8 | NO test for shared-NAT CVV ingress denial. No downgrade. MEDIUM stays. |
| F-9 | NO test for asymmetric counter. No downgrade. LOW stays. |
| F-10 | NO test for restart + Fatal window. No downgrade. MEDIUM stays. |
| F-11 | NO test (operational). No downgrade. LOW stays. |
| F-12 | Tests `prune_skips_protected_peer_logs_warn` covers the warn-log path. **Downgrade applied: F-12 was already LOW; INFO.** |
| CF-4 | NO test. No downgrade. HIGH stays. |

**Rule: finding cites neither INV-XXX nor Appendix-Ax → downgrade two notches.**

All findings cite INV or Appendix. No applications.

## Verified severity table

| ID | Title | Final severity | INV/Appendix |
|---|---|---|---|
| F-1 / CF-1 | Unresolved-CVV-window Fatal penalty causes self-DoS | **HIGH** | INV-002 (spirit), INV-032 (gap), INV-042 (boundary), Appendix A4 |
| F-6 / CF-3 | Observer not protected from prune_connected_peers | **HIGH** | (Propose INV-048) |
| CF-4 | NewEpoch vs UpdateAuthorizedPublishers timing race | **HIGH** | (No INV — cross-layer) |
| F-2 | Orphan accumulation with is_trusted preserved | **MEDIUM** | Appendix A10 |
| F-4 / CF-2 | kad num_records underflow on double-remove | **MEDIUM** | (No INV in scope) |
| F-7 | GossipsubNotSupported → Fatal on legitimate quirks | **MEDIUM** | INV-002 (only CVV protected) |
| F-8 | IP-ban THRESHOLD=1 + shared NAT denies CVV | **MEDIUM** | INV-011, INV-012 (gap) |
| F-10 | Restart load + Fatal-penalty window | **MEDIUM** | Appendix A3 |
| F-3 / A2 | next_dial_request no re-gate | **LOW** | Appendix A2 |
| F-5 / A8 | num_in/num_out cap absent | **LOW** | Appendix A8 |
| F-9 | disconnected_peers asymmetric arithmetic | **LOW** | INV-022 (defensive) |
| F-11 | open_epoch_pack 30s timeout no retry | **LOW** | (Operational) |
| F-12 | log spam under protected overshoot | **INFO** | INV-045 (accepted) |

## tn-domain-* cross-references

- **tn-domain-networking** invariants (boundary, INV-039-INV-042): F-1 violates INV-042 spirit (Unban event does NOT re-add to kad routing table); F-8 violates INV-039 (banned IPs reject inbound — but the CVV-protective exception is missing).
- **tn-domain-epoch**: F-1, CF-4 both occur at epoch boundary; INV-046 + INV-047 are the relevant boundary invariants. The unresolved-CVV window is the gap.
- **tn-domain-consensus**: F-7 + CF-4 directly affect gossip validation and committee resolution, which is on the consensus path.
