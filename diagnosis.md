# Diagnosis: Committee Nodes Banned During Restart

## Root Cause

The primary network's peer manager resolves a committee member's BLS key to the **wrong PeerId** (worker PeerId instead of primary PeerId). This means the actual primary PeerId is never marked `is_trusted`, so when a Fatal penalty arrives, the peer is instantly banned despite being a committee member.

## Evidence

### The BLS-to-PeerId Mismatch

Each validator has two separate PeerIds: one for the primary network and one for the worker network. Each network also has its own separate peer manager with its own kad store (`bls_index`).

From validator-1's logs, the first `new_epoch` call (primary network) at `19:04:46.579`:

```
adding committee member pDmpE29Y.../12D3KooWHGdbYerasxspuhZW4KqNMbbpqvLNCG6LGpS5yx6b8x2x
```

But `12D3KooWHGdbYerasxspuhZW4KqNMbbpqvLNCG6LGpS5yx6b8x2x` is **validator-3's worker PeerId**, not its primary PeerId. From `node3.yaml`:

```yaml
primary:
  network_address: /ip4/35.203.41.59/.../p2p/12D3KooWCnizjv4ctsBTwwYJxZP3duCuoebJfbk1fUQv6Vg1dVUh  # <-- this is the real primary PeerId
worker:
  network_address: /ip4/35.203.41.59/.../p2p/12D3KooWHGdbYerasxspuhZW4KqNMbbpqvLNCG6LGpS5yx6b8x2x  # <-- this got registered instead
```

### The Ban Sequence

3 seconds later at `19:04:49.923`, validator-3's **real** primary PeerId receives a Fatal penalty:

```
peer_id=12D3KooWCnizjv4ctsBTwwYJxZP3duCuoebJfbk1fUQv6Vg1dVUh prior_reputation=Trusted new_reputation=Banned
```

`prior_reputation=Trusted` means the peer had a good score (not banned), but `is_trusted` was `false` — so the penalty was applied. If `is_trusted` were `true`, `apply_penalty()` at `peer.rs:181` would have skipped the penalty entirely.

The ban cascaded to an IP ban on `35.203.41.59`, which then blocked both of validator-3's PeerIds (primary and worker) since they share the same IP.

### Timeline

| Time | Event |
|------|-------|
| 19:04:46.579 | V1 primary network: `new_epoch()` registers V3's BLS key → V3's **worker** PeerId (wrong) |
| 19:04:47.083 | V1 worker network: `new_epoch()` registers V3's BLS key → V3's **primary** PeerId (also wrong, swapped) |
| 19:04:47.157 | V3's primary PeerId connects to V1's primary network. Peer exists, NOT trusted |
| 19:04:49.923 | Fatal penalty applied to V3's primary PeerId. `is_trusted=false`, so penalty goes through |
| 19:04:49.923 | V3's primary PeerId banned. IP `35.203.41.59` added to IP ban list |
| 19:05:11.002 | V2 also bans V3's primary PeerId with the same mismatch pattern |

### How the BLS Index Gets Cross-Contaminated

The `bls_index` in `AllPeers` maps `BlsPublicKey → PeerId`. During node startup, both the primary and worker networks populate their `bls_index` from the same bootstrap server data and kad records. The bug is that the kad store on the primary network discovers V3's worker PeerId for its BLS key (or vice versa), because:

1. Both the primary and worker swarms share the same kad key space (BLS keys are the record keys)
2. When a validator publishes its kad record, the primary swarm may pick up the worker's PeerId-to-BLS mapping if the worker record arrives first
3. The `bls_index` doesn't differentiate between primary and worker PeerIds — it just maps BLS → PeerId

When `new_epoch()` runs, it calls `bls_index.get(&bls_key)` to resolve committee members. If the wrong PeerId is in the index, the wrong peer gets marked trusted, and the real peer remains unprotected.

## Affected Nodes

From the logs of this specific deployment (`20260523_140612-update-staggered-v1-banned-v5`):

- **Validator 1** banned validator 3's primary PeerId at `19:04:49.923` (IP: `35.203.41.59`)
- **Validator 2** banned validator 3's primary PeerId at `19:05:11.002` (IP: `35.203.41.59`)
- Both bans resulted in IP bans that blocked all of validator 3's connections

## Why This Only Surfaces During Staggered Restarts

During a staggered restart, nodes start at different times (V1 at 19:04:46, V2 at 19:05:05, V3 at 19:05:21, V4 at 19:05:39, V5 at 19:05:57). Early-starting nodes receive gossip (e.g., certificates) from already-running nodes. If the gossip fails validation (e.g., `TooNew` certificates from a more advanced epoch), the sender receives a Fatal penalty. On a normal running network, committee members are protected by `is_trusted=true`. During restarts, the cross-contaminated `bls_index` means `is_trusted` is applied to the wrong PeerId, leaving the real sender unprotected.

## Fix

The `bls_index` must never map a BLS key to a PeerId from the wrong network. Two approaches:

### Option A: Validate PeerId Network Membership (Recommended)

When `upsert_peer` or `add_known_peer` adds a BLS-to-PeerId mapping, verify the PeerId belongs to the correct network (primary vs worker). This could be enforced by checking the network key embedded in the PeerId against the expected network key for that network type.

### Option B: Separate Kad Key Spaces

Use distinct kad record keys for primary and worker networks so they can never cross-contaminate each other's `bls_index`. For example, prefix the BLS key with a network type identifier.

### Option C: Defensive — Never Ban Committee BLS Keys Regardless of PeerId

When applying a penalty, if the peer's BLS key is in `current_committee_keys` (regardless of which PeerId it resolved to), skip the penalty. This is a defense-in-depth measure but doesn't fix the underlying index corruption.

## Reproduction

1. Deploy 5 validators with primary and worker networks
2. Stop all validators
3. Restart validators with staggered timing (~15s between each)
4. Observe that early-starting validators ban committee peers via Fatal penalties
5. Check logs for `prior_reputation=Trusted new_reputation=Banned` events where `is_trusted=false`
