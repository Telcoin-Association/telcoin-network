# Security Review: network-libp2p

**Date:** 2026-04-07  
**Reviewer:** AI-assisted (review-tn skill)  
**Status:** Pending Human Review  
**Risk Tier:** HIGH

## Crate Profile

| Property | Value |
|----------|-------|
| Path | `crates/network-libp2p` |
| Package | `tn-network-libp2p` |
| Purpose | libp2p swarm integration: gossip, Kademlia, request-response, QUIC, peer scoring and bans |
| Entry points | ~121 `pub` / `pub(crate)` functions (production `src/`, excluding `tests/`) |
| Direct dependencies | 14 |
| Unsafe blocks | 0 |
| Lines of Rust | ~6,900 (production `src/`, excluding `tests/`) |

**External inputs (attack surface):**

- Inbound libp2p streams (gossipsub, Kad, request-response), deserialized BCS/snappy payloads, peer discovery metadata, multiaddrs from dialing peers, local config and storage-backed Kad records.

**Trust boundaries:**

- Remote `PeerId` / multiaddr vs locally authenticated committee / BLS-mapped identities; swarm events vs `PeerManager` and `ConsensusNetwork` command handlers.

**Cross-crate dependents (blast radius):**

- `tn-node`, `tn-primary`, `tn-executor`, `tn-worker` (all depend on `tn-network-libp2p` via workspace).

**Key Reth/Alloy integration points:**

- None in this crate; execution types enter via `tn-types` / consensus callers.

## Summary

The peer manager and consensus networking layer show **five high-severity** issues (ban/dial policy, disconnect/PX flow, Kad/provider handling, unauthenticated direct requests) and **five medium-severity** issues (discovery vs temp ban, gossip/ban window, Kad vs live connection, `expect` usage, discovery buffer growth). All items below are **Unconfirmed** until human review per review-tn Phase 4.

| ID | Title | Severity | Category | Status |
|----|-------|----------|----------|--------|
| NL-001 | `remove_validator_ip` drops full IP bucket; `total` inconsistent | High | State consistency | Unconfirmed |
| NL-002 | `dial_peer` ignores `temporarily_banned` | High | State consistency | Unconfirmed |
| NL-003 | `eligible_for_discovery` uses `AllPeers::can_dial` only (no temp ban) | Medium | State consistency | Unconfirmed |
| NL-004 | `disconnect_peer` double-emits disconnect + `DisconnectWithPX` via `Disconnecting` | High | API sharp edges | Unconfirmed |
| NL-005 | Inbound Kad `AddProvider` stored without TN validation | High | Input validation | Unconfirmed |
| NL-006 | `SendRequestDirect` skips `auth_to_peer` | High | Cryptographic safety | Unconfirmed |
| NL-007 | Gossip `verify_gossip` vs ban/blacklist window | Medium | State consistency | Unconfirmed |
| NL-008 | Kad / `connected_peers` updated on `DisconnectPeerX` before connection closes | Medium | State consistency | Unconfirmed |
| NL-009 | `verify_gossip` uses `expect` on `Option` | Medium | Error handling | Unconfirmed |
| NL-010 | Unbounded `discovery_peers` growth before heartbeat prune | Medium | Resource exhaustion | Unconfirmed |

**Totals:** 10 findings — 0 Critical, 5 High, 5 Medium, 0 Low, 0 Informational

*AI pre-verification (2026-04-07): NL-001–NL-010 were traced to cited source locations; this does not replace human confirmation.*

## Findings

### NL-001: `remove_validator_ip` corrupts IP ban accounting

- **Severity:** High  
- **Category:** State consistency  
- **Location:** `crates/network-libp2p/src/peers/banned.rs` (see `remove_validator_ip` / `add_banned_peer` pairing)  
- **Description:** `add_banned_peer` increments per-IP refcount in `banned_peers_by_ip`. `remove_validator_ip` removes the entire IP key and decrements `total` once, regardless of how many peers shared that IP bucket.  
- **Impact:** Shared-IP deployments: removing one validator’s IP can clear accounting for other banned peers on the same IP; `total` diverges from the real banned set.  
- **Proposed Fix:**

```rust
// Mirror remove_banned_peer: for each affected peer/IP, decrement count;
// remove IP key only when count hits zero; adjust total by logical peer removals.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-002: `dial_peer` bypasses temporary ban

- **Severity:** High  
- **Category:** State consistency  
- **Location:** `crates/network-libp2p/src/peers/manager.rs` (`dial_peer`; compare `PeerManager::can_dial`)  
- **Description:** Dial path blocks `ConnectionStatus::Banned` but does not consult `temporarily_banned`, unlike `can_dial`.  
- **Impact:** Discovery and explicit dials can target temporarily banned peers; wasted work and asymmetric policy vs Kad paths that use `can_dial`.  
- **Proposed Fix:**

```rust
if self.temporarily_banned.contains(&peer_id) {
    return /* reject dial, same as can_dial */;
}
// or: if !self.can_dial(&peer_id) { return ... }
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-003: Discovery eligibility omits temporary ban

- **Severity:** Medium  
- **Category:** State consistency  
- **Location:** `crates/network-libp2p/src/peers/manager.rs` (`eligible_for_discovery`); `crates/network-libp2p/src/peers/all_peers.rs` (`AllPeers::can_dial`)  
- **Description:** `eligible_for_discovery` uses `self.peers.can_dial(&info.peer_id)` instead of `self.can_dial(&info.peer_id)`. `AllPeers::can_dial` has no view of `temporarily_banned`.  
- **Impact:** Temp-banned peers may still be considered eligible for discovery-driven dialing.  
- **Proposed Fix:**

```rust
self.can_dial(&info.peer_id) // instead of self.peers.can_dial(...)
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-004: `disconnect_peer` duplicates disconnect events and PX vs `support_discovery`

- **Severity:** High  
- **Category:** API sharp edges  
- **Location:** `crates/network-libp2p/src/peers/manager.rs` (`disconnect_peer`); `crates/network-libp2p/src/peers/all_peers.rs` (`handle_disconnecting_transition`, `apply_peer_action`)  
- **Description:** `disconnect_peer` emits `DisconnectPeer` / `DisconnectPeerX`, then transitions to `Disconnecting { banned: false }`. For connected/dialing peers, `handle_disconnecting_transition` can return `PeerAction::DisconnectWithPX` when `banned` is false, causing a second disconnect path and temp-ban side effects. Intent of `support_discovery` may not match the action chosen.  
- **Impact:** Double disconnect instructions; possible `DisconnectWithPX` even when discovery support is off, depending on state machine path.  
- **Proposed Fix:**

```rust
// Single source of truth: derive PeerAction from support_discovery / banned
// explicitly; avoid both a manual PeerEvent and an action-driven duplicate.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-005: Unvalidated inbound Kad `AddProvider`

- **Severity:** High  
- **Category:** Input validation  
- **Location:** `crates/network-libp2p/src/consensus.rs` (Kad inbound handling: `PutRecord` vs `AddProvider`)  
- **Description:** `PutRecord` is validated via `process_kad_put_request`; `AddProvider` may call `store_mut().add_provider` without the same TN-specific checks.  
- **Impact:** Remote peers can inject provider records that bypass committee/BLS-aligned validation, polluting DHT state if Kad providers are used.  
- **Proposed Fix:**

```rust
// Reject AddProvider unless validated (committee keyspace, signature policy),
// or disable the code path if unused.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-006: `SendRequestDirect` without BLS / committee auth

- **Severity:** High  
- **Category:** Cryptographic safety  
- **Location:** `crates/network-libp2p/src/consensus.rs` (`SendRequest` vs `SendRequestDirect`)  
- **Description:** `SendRequest` uses `auth_to_peer`; `SendRequestDirect` sends on an existing connection without that mapping.  
- **Impact:** Callers can target arbitrary `PeerId` on an open connection, bypassing the authenticated peer binding used elsewhere.  
- **Proposed Fix:**

```rust
// Require auth_to_peer (or equivalent) for SendRequestDirect, or restrict API
// to internal/test-only with compile-time or runtime guards + documentation.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-007: Gossip acceptance vs ban/blacklist window

- **Severity:** Medium  
- **Category:** State consistency  
- **Location:** `crates/network-libp2p/src/consensus.rs` (`verify_gossip`; `PeerEvent::Banned` / blacklist handling)  
- **Description:** Gossip validation uses committee/topic auth but not necessarily `peer_banned` / reputation ban state; blacklist follows disconnect lifecycle.  
- **Impact:** Short window where a peer is score-banned or disconnecting but gossip from that publisher identity may still be accepted. May be acceptable if documented.  
- **Proposed Fix:**

```rust
// Optionally reject in verify_gossip when peer_banned / reputation says banned;
// or document the intentional window explicitly in ops/security docs.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-008: Kad and `connected_peers` ahead of live connection on PX disconnect

- **Severity:** Medium  
- **Category:** State consistency  
- **Location:** `crates/network-libp2p/src/consensus.rs` (`DisconnectPeerX` handling)  
- **Description:** On `DisconnectPeerX`, Kad `remove_peer` and `connected_peers` updates may run before the PX-driven disconnect completes.  
- **Impact:** Routing and round-robin peer selection omit a peer that is still connected until teardown finishes (e.g. affects `SendRequestAny`-style paths).  
- **Proposed Fix:**

```rust
// Defer Kad/connected_peers pruning until disconnect completes, or document
// the invariant and harden callers that assume immediate consistency.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-009: Panic-prone `expect` in `verify_gossip`

- **Severity:** Medium  
- **Category:** Error handling  
- **Location:** `crates/network-libp2p/src/consensus.rs` (`verify_gossip`)  
- **Description:** `auth.as_ref().expect(...)` / `bls_key.expect(...)` after conditional chains; future refactors can violate assumed invariants and panic on the hot path.  
- **Impact:** Node crash on unexpected `None` instead of controlled `Reject`.  
- **Proposed Fix:**

```rust
let Some(auth) = auth.as_ref() else {
    return MessageAcceptance::Reject;
};
let Some(bls_key) = bls_key else {
    return MessageAcceptance::Reject;
};
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

### NL-010: Unbounded `discovery_peers` growth

- **Severity:** Medium  
- **Category:** Resource exhaustion  
- **Location:** `crates/network-libp2p/src/peers/manager.rs` (`discovery_peers` extend vs `discovery_heartbeat` pruning)  
- **Description:** Peers are appended before the next heartbeat pruning cycle.  
- **Impact:** Memory spike under bursty discovery traffic.  
- **Proposed Fix:**

```rust
// Cap length on insert (e.g. same pattern as peer-exchange path) or drop
// oldest when over capacity before extend.
```

- **Status:** Unconfirmed  
- **Reviewer Notes:** —

---

## Variant Analysis Notes

After human confirmation, consider workspace-wide searches for: duplicate disconnect / ban state transitions; `can_dial` vs raw `AllPeers::can_dial`; direct `send_request` without `auth_to_peer`; Kad inbound handlers without symmetric validation. Methodology: see review-tn `references/variant-patterns.md` if present in the skill bundle.

## Coverage Statement

**Analyzed:**

- `consensus.rs`, `kad.rs`, `codec.rs`, `lib.rs`, `peers/*`, `stream/*` — integration and peer management paths.

**Not analyzed (with reason):**

- `crates/e2e-tests` — on hold per TN audit tracker.  
- Exhaustive `tn-config` default audit and full transitive supply-chain CVE pass — out of scope for this crate report.

## Dependency Snapshot

| Dependency | Version | Notes |
|------------|---------|-------|
| libp2p | 0.56.0 (workspace) | Large attack surface; keep patched; review fork/feature deltas. |
| tokio | workspace | Standard async runtime. |
| bcs, snap | workspace | Deserialization bounds — ensure callers enforce limits on payload size. |
| tn-types, tn-config, tn-storage | workspace | Trust boundary at type/config boundaries. |
| Other direct deps | workspace | serde, tracing, futures, thiserror, bs58, async-trait, rand, serde_with — no separate CVE sweep in this pass. |

No notable additional dependency risks identified beyond normal workspace pinning; run `cargo audit` periodically at workspace root.
