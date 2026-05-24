# tn-bug-scan — Raw Findings (pre-verification)

_Git hash: cb8aa049_
_Generated: 2026-05-21_

This file contains all candidate findings discovered during Phase 2 (Feynman), Phase 3 (State Check), and Phase 4 (Cross-Feed Loop). Findings here may include items downgraded, deduplicated, or merged in `tn-bug-scan-verified.md`. Refer to the verified report for the authoritative ticket list with severity, repro, and fix recommendations.

## Phase 2 candidates (Feynman-only)

- **F-1:** Unresolved-CVV-window penalty leads to permanent eviction. **Promoted to HIGH (CF-1).**
- **F-2:** Orphan accumulation with is_trusted preserved (Appendix A10). **MEDIUM.**
- **F-3:** next_dial_request no re-gate (Appendix A2). **LOW.**
- **F-4:** RecordStore::remove num_records unchecked decrement (kad.rs:389). **MEDIUM (CF-2).**
- **F-5:** num_in/num_out u8 cap absent (Appendix A8). **LOW.**
- **F-6:** Observer not protected from prune_connected_peers. **HIGH (CF-3).**
- **F-7:** GossipsubNotSupported → Fatal. **MEDIUM.**
- **F-8:** BANNED_PEERS_PER_IP_THRESHOLD=1 + shared NAT denies CVV reconnect. **MEDIUM.**
- **F-9:** disconnected_peers asymmetric arithmetic. **LOW.**
- **F-10:** KadStore restart load + Fatal-penalty window. **MEDIUM.**
- **F-11:** open_epoch_pack 30s timeout no retry. **LOW.**
- **F-12:** log spam under protected overshoot. **INFO.**

## Phase 3 state gaps (Coupled-state)

- **GAP-1:** IP-ban check has no CVV bypass. Merged with F-8.
- **GAP-2:** promote_committee_member leaves peer in Disconnected. Merged with F-1 (key cross-feed).
- **GAP-3:** remove_peer does not clear current_committee_keys. **DROPPED** — INV-045 in practice prevents reachability.
- **GAP-4:** temporarily_banned can disagree with reputation. **Already documented** as Appendix A7. Not promoted to ticket.
- **GAP-5:** disconnected_peers asymmetric arithmetic. Merged with F-9.

## Phase 4 cross-feed findings

- **CF-1:** F-1 + F-7 + GAP-2 + INV-042 boundary. **HIGH.**
- **CF-2:** F-4 + F-10. **MEDIUM.** kad-store restart with expired records.
- **CF-3:** F-6 + F-8. **HIGH.** Observer behind shared-NAT validator double-vulnerable. Final ticket on F-6 references CF-3.
- **CF-4:** New finding from iteration 2 — NewEpoch vs UpdateAuthorizedPublishers timing. **HIGH.**

## Phase 5 scenarios

| Scenario | Event class | Severity |
|---|---|---|
| S1 | Epoch transition + kad latency + concurrent load (CF-1) | HIGH |
| S2 | Observer prune cascade (F-6) | HIGH |
| S3 | KadStore num_records underflow on restart (F-4) | MEDIUM |
| S4 | IP-ban poisons CVV via shared NAT (F-8) | MEDIUM |
| S5 | Orphan accumulation under K rotations (F-2) | MEDIUM |
| S6 | pending_dials overwrite (A1) | LOW (Appendix-only, deferred) |
| S7 | open_epoch_pack 30s timeout (F-11) | LOW |

## Findings dropped during verification

- **GAP-3** — reachable in theory only if INV-045 is bypassed. Not promoted.
- **GAP-4** — Appendix A7 already documents this. Mention in domain patterns but no new ticket.
- **A1 (pending_dials overwrite)** — documented. Reachability requires simultaneous double-dial from the application layer. Not raised because Phase 6 verification found the dial_peer status-check gate makes the trigger narrow.

## Deduplication notes

- F-10 substantially overlaps S1's restart variant; kept as separate finding because the trigger condition (restart) differs from CF-1's (kad latency).
- CF-3's "observer + IP-ban double-vulnerability" merged into F-6's recommendation discussion. Not raised as a separate ticket.

## Open questions surfaced during audit (not raised as findings)

1. Is `NetworkCommand::AddBootstrapPeers` (consensus.rs:555) idempotent across restarts? It checks `auth_to_peer(bls).is_none()` before calling `add_known_peer`. If the KadStore restart-load (consensus.rs:284) populates the peer first, this avoids double-promotion. Worth pinning a test for restart-then-bootstrap order.
2. The `pending_px_disconnects` (consensus.rs:1066-1093) spawns one tokio task per PX disconnect with a timeout. If many PX disconnects fire in a short window, the task spawner is hit hard. Audit only verified the `config.max_px_disconnects` gate prevents unbounded spawn — no severity finding.
3. `state-sync/consensus.rs:271-280` "we are giving up on this epoch for now" — pack-file fetch giving up after 100 attempts. The note in the source says "this is not a real solution, without getting this pack file execution will be stuck." Documented operational concern.
4. `consensus.rs:665-666` `connected_peers.rotate_left(1)` then `front()` for round-robin. Empty-list check at :665 prevents panic. Implementation is correct.
