# Code Review: `peer-manager-cleanup` branch (4 commits ahead of `main`)

Date: 2026-05-20
Scope: 10 changed files in `crates/network-libp2p/` plus a doc-comment update in `crates/node/src/manager/node/epoch.rs`. The refactor:
- Drops `PeerManager::known_peers` and `known_peerids` caches; introduces `AllPeers::bls_index: HashMap<BlsPublicKey, PeerId>` as the inverse of `Peer::bls_public_key`.
- Funnels every BLS mutation through `rebind_bls` / `remove_peer`; introduces `Peer::clear_bls`, `Peer::listening_addrs_clone`, and replacing-not-extending of `listening_addrs` in `update_net`.
- Adds `is_protected` prune-skip so committee members and trusted peers cannot be evicted by `prune_banned_peers` / `prune_disconnected_peers`.
- Splits `new_epoch` resolution into resolved-now / unresolved-deferred; the unresolved set is surfaced as `PeerEvent::MissingAuthorities`; `upsert_peer` now runs `promote_committee_member` to self-heal late-arriving committee members.
- Removes a never-used `record_timestamp()` / `TimestampSec` field from `Peer`.
- Adds a 492-line invariants spec (`peers/invariants.md`) documenting 47 numbered invariants + 10 known gaps.

## Summary

6 findings raised → 4 confirmed as real, 2 confirmed as anti-findings (intentional behavior). 0 false positives. The refactor consolidates a previously dual-cache design behind a single, well-documented secondary index with a tight mutation funnel; the prune-protection patch closes a real "silent validator lockout" gap (F-1 noted in INV-045). One real Medium-severity correctness gap remains around mid-epoch validator `PeerId` rotation. No Critical or High findings.

| # | Title | Verified Severity | Status |
|---|-------|---|---|
| 1 | Mid-epoch `PeerId` rotation leaves stale committee binding and unprotected new peer | Medium | CONFIRMED |
| 2 | `is_trusted` is set-only; protected set grows unbounded across epochs | Low | CONFIRMED (doc-only) |
| 3 | Bootstrap vs kad multiaddr asymmetry at `consensus.rs:560` | Informational | CONFIRMED (intentional) |
| 4 | Redundant `auth_to_peer` guard in `AddBootstrapPeers` | Informational | CONFIRMED (safe to remove) |
| 5 | `MissingAuthorities` re-emission is bounded (anti-finding) | Informational | CONFIRMED (no issue) |
| 6 | `banned_peers` overshoot under all-protected case (anti-finding) | Informational | CONFIRMED (no issue) |

## Findings

### 1. Mid-epoch `PeerId` rotation leaves stale committee binding and unprotected new peer
- **Severity (verified)**: Medium
- **Status**: CONFIRMED (HIGH confidence)
- **Category**: Consensus Safety / Bugs
- **Location**: `crates/network-libp2p/src/peers/all_peers.rs:147-168` (`upsert_peer`), `:107-122` (`rebind_bls`), `:958-995` (`promote_committee_member`)

**Verified claim**: Same-BLS + new-PeerId rotation while the BLS is already resolved in `current_committee_keys` produces a corrupted dual state.

**Trace through live code**:

1. `rebind_bls` (`all_peers.rs:107-122`): `peer_id_new` is unknown → the `peer.bls_public_key()` branch at `:108-114` is skipped. `bls_index.insert(bls, peer_id_new)` at `:115` returns `Some(peer_id_old)`. Since `peer_id_old != peer_id_new`, the orphan branch fires and `peers[peer_id_old].clear_bls()` is called (`peer.rs:145-147` sets `bls_public_key = None`).
2. Gate in `upsert_peer` (`all_peers.rs:162-167`): `current_committee_keys.get(&bls)` returns `Some(&Some(peer_id_old))`, NOT `Some(&None)`. `promote_committee_member` is NOT invoked.
3. Post-state:
   - `bls_index[bls] = peer_id_new`
   - `peers[peer_id_new]`: fresh `Peer::new(...)` → `is_trusted = false`, NOT in `current_committee`.
   - `peers[peer_id_old]`: `bls_public_key = None`, `is_trusted = true`, STILL in `current_committee`.
   - `current_committee_keys[bls] = Some(peer_id_old)` — STALE.
4. Penalty path for `peer_id_new`: `Peer::apply_penalty` (`peer.rs:180-187`) gates the score update on `!self.is_trusted`. Since `peer_id_new.is_trusted == false`, penalties apply normally and can drive the peer to `Reputation::Banned`. NO `is_trusted` short-circuit protects it.
5. Production reachability via `consensus.rs:1387-1391`: `peer_record_valid` at `consensus.rs:386-406` validates that (a) the NodeRecord is BLS-signed, and (b) `record.publisher == verified.1.info.pubkey.clone().into()`. A validator that legitimately rotates its libp2p keypair and re-publishes a NodeRecord signed by its unchanged BLS key passes both checks. The path IS reachable from normal traffic.
6. Test coverage: `tests/peers.rs:675-693` (`peer_id_rotation_same_bls_orphans_old_peer`) covers the orphan-survives case but never calls `new_epoch`. No existing test exercises the in-committee scenario.

**Hypothetical failing test** (demonstrates the issue; do not commit):

```rust
#[test]
fn peer_id_rotation_mid_epoch_leaves_stale_committee_binding() {
    let mut all_peers = create_all_peers(None);
    let addr = create_multiaddr(None);

    // (1) Register peer A and resolve a committee containing its BLS.
    let (_, net_a, peer_a) = bls_net_peer(80);
    let mut rng = StdRng::from_seed([81; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    all_peers.upsert_peer(bls, net_a, vec![addr.clone()]);

    let mut committee = HashSet::new();
    committee.insert(bls);
    let (_, unresolved) = all_peers.new_epoch(committee);
    assert!(unresolved.is_empty());
    assert!(all_peers.is_peer_validator(&peer_a));
    assert!(all_peers.get_peer(&peer_a).unwrap().is_trusted());

    // (2) Validator rotates its libp2p key mid-epoch — same BLS, new PeerId.
    let (_, net_b, peer_b) = bls_net_peer(82);
    all_peers.upsert_peer(bls, net_b, vec![addr]);

    // ----- EXPECTED post-fix behavior (CURRENTLY FAILS on this branch) -----
    assert!(all_peers.is_peer_validator(&peer_b),
        "peer_b should inherit committee membership after rotation");
    assert!(all_peers.get_peer(&peer_b).map(|p| p.is_trusted()).unwrap_or(false),
        "peer_b should be trusted after rotation");
    assert_eq!(all_peers.current_committee_keys.get(&bls), Some(&Some(peer_b)),
        "current_committee_keys must repoint to peer_b");
    assert!(!all_peers.is_peer_validator(&peer_a),
        "peer_a must no longer be in current_committee");
}
```

All four expected-fix assertions fail on this branch.

**Proposed fix** (relax the gate to also handle rotation):

```rust
let should_promote = match self.current_committee_keys.get(&bls_public_key) {
    Some(&None) => true,
    Some(&Some(other_pid)) if other_pid != peer_id => {
        // demote the orphan: drop from current_committee
        self.current_committee.remove(&other_pid);
        // (optionally) clear orphan's is_trusted via a new Peer::demote_trusted helper
        true
    }
    _ => false,
};
if should_promote {
    return self.promote_committee_member(bls_public_key, peer_id).map(|a| (peer_id, a));
}
```

Add a regression test mirroring the one above. Document as a new INV-048 in `peers/invariants.md` linked from INV-047.

**Why Medium, not High**: The only actor who can trigger this is the validator itself (it must publish a fresh NodeRecord signed by its own BLS). The blast radius is "this validator silently demotes itself mid-epoch and becomes ban-eligible" — a self-inflicted liveness regression during routine key rotation, not an external attack. But the failure is silent and produces a confused dual state, so it remains a real correctness gap.

---

### 2. `is_trusted` is set-only; protected set grows unbounded across epochs
- **Severity (verified)**: Low
- **Status**: CONFIRMED (HIGH confidence)
- **Category**: Architecture / DoS
- **Location**: `crates/network-libp2p/src/peers/peer.rs:337-343` (`make_trusted`), `crates/network-libp2p/src/peers/all_peers.rs:819-826` (`is_protected`)

**Verified claim**: `Peer::make_trusted` (`peer.rs:338-343`) is the only writer of `is_trusted`; no inverse exists. Exhaustive grep confirms only `new_trusted` (peer.rs:67), `make_trusted` (peer.rs:339-340), and the getter touch the field. `promote_committee_member` (`all_peers.rs:990`) calls `make_trusted` for every committee resolution, so the trusted set is monotonically increasing across epochs. `is_protected` (`all_peers.rs:826`) uses `peer.is_trusted()` as one disjunct.

**Bound**: Lifetime union of all committee memberships the node has ever observed. For Telcoin's anticipated validator counts and `max_peers()` (typically 50-300), this is academic. Long-lived nodes on networks with high churn could eventually saturate `max_peers()`, silently disabling ban-pruning.

**Operator visibility**: The `warn!` blocks at `all_peers.rs:902-909` and `:933-941` only fire when the prune path is invoked AND short on candidates. A node maintaining a healthy peer count may never trigger them.

**Proposed fix** (recommended): documentation-only — add Appendix A11 to `peers/invariants.md` capturing that the trusted set is monotone, the lifetime bound is validator-set churn, restart clears it, and the existing `warn!` is the canary. The "once-staked, never-forgotten" property of INV-045 is intentional; only the operator-visibility gap needs closing.

Optional: add a `max_trusted_peers` knob to `PeerConfig` and a one-shot `warn!` at heartbeat when exceeded. Not required.

---

### 3. Bootstrap vs kad multiaddr asymmetry at `consensus.rs:560`
- **Severity (verified)**: Informational
- **Status**: CONFIRMED intentional (HIGH confidence)
- **Category**: Architecture / Diagnostics
- **Location**: `crates/network-libp2p/src/consensus.rs:555-563` (`AddBootstrapPeers`)

**Verified claim**: `consensus.rs:560` passes `vec![info.network_address]` (single address); kad paths at `:288-292`, `:1387-1391`, `:1491-1495` pass `Vec<Multiaddr>` from `NodeRecord::info::multiaddrs`.

**Evidence**:
- `crates/types/src/committee.rs:25-32`: `P2pNode` defines `pub network_address: Multiaddr` (singular) by design.
- The asymmetry reflects data-shape reality: bootstrap config carries one address per peer; kad records carry many.
- `Peer::update_net` now replaces `listening_addrs`, so a later kad record will correctly supersede the bootstrap entry without history loss in `multiaddrs`.

**Proposed fix** (comment-only):

```rust
for (bls, info) in peers {
    if peer.auth_to_peer(bls).is_none() {
        // bootstrap config exposes a single `network_address`; kad paths pass
        // the full `multiaddrs` Vec from the verified NodeRecord.
        peer.add_known_peer(bls, info.network_key, vec![info.network_address]);
    }
}
```

---

### 4. Redundant `auth_to_peer` guard in `AddBootstrapPeers`
- **Severity (verified)**: Informational
- **Status**: CONFIRMED (HIGH confidence)
- **Category**: Optimization
- **Location**: `crates/network-libp2p/src/consensus.rs:559` (the `if peer.auth_to_peer(bls).is_none()` check before `add_known_peer`)

**Verified claim**: The guard at `consensus.rs:559` is dead weight post-refactor; `upsert_peer` is idempotent.

**Caller analysis**: Exhaustive grep shows the only caller of `add_bootstrap_peers` is `crates/node/src/manager/node/epoch.rs:1303`, gated on `if initial_epoch` (line 1302). The command runs at most once per node start. Cost of removing the guard: one extra `bls_index` get + one `update_net` per bootstrap entry. Removing the guard introduces no regression and is slightly more consistent with the "kad record wins via replacement" semantics elsewhere.

**Proposed fix**:

```rust
NetworkCommand::AddBootstrapPeers { peers, reply } => {
    let peer = &mut self.swarm.behaviour_mut().peer_manager;
    for (bls, info) in peers {
        peer.add_known_peer(bls, info.network_key, vec![info.network_address]);
    }
    let _ = reply.send(Ok(()));
}
```

Low-priority cleanup. Alternatively keep the guard and add a one-line comment.

---

### 5. `MissingAuthorities` re-emission is bounded (anti-finding)
- **Severity (verified)**: Informational
- **Status**: CONFIRMED no issue (HIGH confidence)
- **Category**: DoS

**Verified claim**: No amplification loop in `MissingAuthorities` emission.

**Exhaustive grep results**:
- Producers of `PeerEvent::MissingAuthorities`: exactly 2 — `manager.rs:629` (from `new_epoch`) and `manager.rs:664` (from `find_authorities`).
- Callers of `PeerManager::find_authorities`: exactly 2 production sites — `epoch.rs:782` and `epoch.rs:787` (both inside `init_committee_for_epoch`, runs once per epoch handle, primary + worker). One test-only caller at `tests/network_tests.rs:1662`.
- Callers of network `new_epoch`: 3 production sites — `epoch.rs:486`, `epoch.rs:1305`, `consensus.rs:716` (command dispatcher).
- Consumer at `consensus.rs:1149-1155`: one `kad.get_record` per entry, tracked by `QueryId` in `self.kad_record_queries`. No replay loop.

Bounded by `2 callers × epoch-frequency × authority-count`. If a third caller is added later this analysis must be revisited.

---

### 6. `banned_peers` overshoot under all-protected case (anti-finding)
- **Severity (verified)**: Informational
- **Status**: CONFIRMED no issue (HIGH confidence)
- **Category**: DoS

**Verified claim**: The all-protected case accepts overshoot without panic, spin, or count corruption.

**Live code at `all_peers.rs:887-949`**:
- `prune_banned_peers` (887-919): `excess = banned_peers.total().saturating_sub(max_banned_peers)`; calls `collect_excess_peers` which skips protected peers (`:847`); emits `warn!` "banned peer prune capped by protected (committee/trusted) peers" naming `requested` + `evicted` (`:902-909`); evicts whatever it found.
- `prune_disconnected_peers` (921-949): identical structure; warn at `:934-941`.
- No panic, no infinite loop, no `disconnected_peers` corruption (the `saturating_sub` at `:946` is symmetric to the eviction count).
- INV-045 (`invariants.md:318`) documents the contract: "emit a single `warn!` naming both numbers and accept the temporary overshoot."

Practical bound on the overshoot is the union of committee+trusted set, which is the same concern as Finding 2. No separate fix needed.

---

## Action items (ordered by severity)

1. **Medium (Finding 1)** — Patch `upsert_peer` gate at `crates/network-libp2p/src/peers/all_peers.rs:162-167` to handle rotation-while-resolved. Add regression test (template in Finding 1). Add INV-048 to `peers/invariants.md`.
2. **Low (Finding 2)** — Add Appendix A11 to `crates/network-libp2p/src/peers/invariants.md` documenting the monotone trusted set and its practical bound.
3. **Informational (Finding 3)** — Add one-line comment at `crates/network-libp2p/src/consensus.rs:560` explaining the single-vs-vector asymmetry.
4. **Informational (Finding 4)** — Either drop the `auth_to_peer(bls).is_none()` guard at `crates/network-libp2p/src/consensus.rs:559` or annotate it as an intentional "register exactly once" marker.

Findings 5 and 6 are confirmed anti-findings; no action required beyond keeping them documented for future readers.
