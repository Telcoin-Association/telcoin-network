# tn\_admissionSnapshot

Returns the node's admission view of each of its swarms: the swarm's connection admission status and every BLS key it lists, with the slots the key holds, the addresses the swarm would reach it at, and whether its PeerId is in the authorized set that Closed admission enforces.
Host firewall tooling reads it on a validator's loopback RPC to keep a P2P allowlist a superset of the peers the node admits, including the validators that can be drawn into a committee before they need a connection; see [Host network observability](../../getting-started/host-network-observability.md#the-validators-set).

Listing a key grants it nothing.
The `scheduled` and `eligible` keys are tracked for export only, and `admitted` reports whether the key's PeerId is in the authorized set that Closed admission enforces, whatever the effective mode.
Every RPC server of a node serves the same snapshot, whichever worker's server answers.
A validator that serves this method keeps `tn` on a loopback-only listener; the warning on the `tn` namespace page explains why a validator must not expose `tn` publicly.

#### Parameters

`None`

#### Returns

`Object` - The admission snapshot.
Like [tn\_info](tn_info.md), its field names are snake\_case:

* `version`: `Number` - The schema version, `1`.
  A reader must not interpret a snapshot whose version it does not know.
* `epoch`: `Number` or `null` - The primary swarm's accepted admission epoch; `null` before the primary accepts a committee window, or when the primary did not answer.
* `swarms`: `Array` - One object per swarm, the primary first and then every configured worker swarm in worker id order, including workers outside the epoch's on-chain worker range:
  * `network`: `String` - The swarm's metrics label: `"primary"`, or `"worker-N"` for worker `N`.
  * `worker_id`: `Number` or `null` - The worker id, or `null` for the primary.
  * `available`: `Boolean` - Whether the swarm answered.
    A swarm that errors or does not answer within 2 seconds is listed with `available: false`, a `null` `tracked_epoch`, a `null` `admission` and no entries; that empty list is not an empty tracked set.
  * `tracked_epoch`: `Number` or `null` - The epoch of the latest scheduled and eligible key publication the swarm accepted, or `null` before the first.
  * `admission`: `Object` or `null` - The swarm's connection admission status, `null` exactly when `available` is `false`.
    Check `available` before reading it.
    * `configured`: `String` - The admission mode the operator configured: `"Open"`, `"Grace"` or `"Closed"`.
    * `effective`: `String` - The admission mode applied to connections, one of the same three; below `configured` while a fallback holds.
    * `epoch`: `Number` or `null` - The latest accepted committee window epoch, or `null` before one is accepted.
    * `fallback`: `String` or `null` - Why `Closed` cannot be enforced: `"Missing"` (no versioned committee window has arrived), `"Stale"` (the window's lease expired or an older epoch tried to replace it), `"Contradictory"` (one epoch supplied different committees) or `"Unresolved"` (committee records or identity mappings are still unresolved); `null` when nothing prevents `Closed`.
      It is reported in every configured mode.
    * `resolved_current`: `Number` - Current committee identities with authenticated records, including the node's own.
    * `required_current`: `Number` - The resolved-record quorum `Closed` requires: n minus floor((n - 1) / 3).
    * `connected_current`: `Number` - Connected current committee peers, excluding the node itself.
    * `contradicted`: `Number` - Identities whose PeerId another identity also claims, or whose PeerId differs from the configured binding.
  * `entries`: `Array` - One object per BLS key the swarm lists, sorted by key bytes.
    The swarm's own key is never listed.
    * `bls_pubkey`: `DATA`, 96 bytes - The key's compressed BLS12-381 public key, lowercase hex with a `0x` prefix.
    * `slots`: `Array` of `String` - Every slot the key holds, in this order:
      * `"previous"`, `"current"`, `"next"` - A member of that committee of the accepted window, or of the swarm's committee slots before a window is accepted.
      * `"scheduled"` - A member of the committee two epochs ahead.
      * `"eligible"` - In the validator pool the committee draw reads: pending activation, active or pending exit.
      * `"trusted"` - Configured as a trusted node.
      * `"bootstrap"` - Configured as a bootstrap peer.
    * `peer_id`: `String` or `null` - The base58 PeerId of `network_pubkey`; `null` when `source` is `"unresolved"`.
    * `network_pubkey`: `String` or `null` - The network public key of the held record or configured binding, in the encoding [tn\_info](tn_info.md) uses for `primary_network_key`; `null` when `source` is `"unresolved"`.
    * `multiaddrs`: `Array` of `String` - The addresses the swarm uses for the key: the launch address when `source` is `"stub"`, otherwise those of the held record or configured binding; empty when `source` is `"unresolved"`.
    * `record_multiaddrs`: `Array` of `String` - The signed record's own addresses.
      Present only when `source` is `"stub"`, a signed record is held and its addresses differ from the launch address; otherwise the field is omitted.
      The two lists differ when the operator's inventory is stale or the validator moved, so a firewall that must not cut a live peer allows both.
    * `record_timestamp`: `Number` or `null` - The signed record's timestamp in seconds since the Unix epoch; `null` while the swarm holds no signed record for the key.
      A configured binding is not a signed record.
    * `source`: `String` - Where `multiaddrs` came from:
      * `"record"` - A signed node record the swarm verified.
      * `"config"` - An operator-configured binding (trusted, bootstrap or explicit peer) with no signed record yet.
      * `"stub"` - A `committee_peers` launch address, which replaces the addresses of any signed record.
      * `"unresolved"` - Neither a signed record nor a configured binding is held; the entry has no identity and no addresses.
    * `admitted`: `Boolean` - Whether the key's identity is in the authorized set that `Closed` admission enforces.
      It is `true` only for a key in the accepted committee window or with a trusted or bootstrap grant, whose identity, resolved as admission resolves it from a cached record or a configured binding, is authorized.
      Before a window is accepted, only trusted and bootstrap keys can be admitted.
      A key held only as `scheduled` or `eligible` is never admitted, even when its record claims an authorized PeerId.
      The flag does not depend on the effective mode: under `Open` or `Grace` a peer outside the authorized set may still connect.

A key that has just moved from the eligible pool into the committee window reports its tracked record, with `source` `"record"`, until its committee lookup lands.
Admission does not resolve an identity through that record, so such an entry has a `peer_id` but `admitted: false`, and the key still counts as unresolved toward the swarm's `"Unresolved"` fallback.

#### Errors

* `-32001` (`Not Found.`) - The node's networks have not started yet.
  The node answers this rather than an empty snapshot, which a reader would take for swarms that track nobody.
* `-32601` (method not found) - The node runs a release without this method.

#### Load on the node

Each swarm answers on the command channel that also carries its certificate and batch publishes, so the node bounds what RPC callers can add to it.
It runs one fetch at a time, and a caller that arrives during a fetch waits for it and shares its result.
A snapshot is served unchanged for 1 second after its fetch finished, and fetches start at least 1 second apart, also when a caller disconnects in the middle of one.
Each swarm has 2 seconds to answer; a swarm that errors or times out is reported unavailable and does not hold up the others.

#### Reading the snapshot for a firewall

A snapshot is authoritative for membership only when its `version` is known and the primary swarm is `available`, reports a `tracked_epoch` below neither its `admission.epoch` nor the last applied one, and has a `fallback` of `null` or `"Unresolved"`.
Otherwise keep the last applied set; the [fail-open rule](../../getting-started/host-network-observability.md#fail-open-rule) has the details, including how to read worker swarms and unresolved entries.

A primary `tracked_epoch` lower than its `admission.epoch` means `pool-unpublished`.
The node has not published this epoch's validator pool, because the pool read failed or has not run yet, and the swarm still lists the previous epoch's scheduled and eligible keys.
Keep the last applied set and treat the snapshot as stale.
At an epoch start the committee window moves before the pool read completes, so one read in that window can be stale and the next one is fresh.
An equal value is authoritative.
A higher value can be accepted with a warning.

#### Example

#### Request

```
curl http://127.0.0.1:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_admissionSnapshot","params":[],"id":1}'
```

#### Result

A `Closed` primary swarm at epoch 42 that lists a current committee member it admits and an eligible validator it does not:

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "version": 1,
    "epoch": 42,
    "swarms": [
      {
        "network": "primary",
        "worker_id": null,
        "available": true,
        "tracked_epoch": 42,
        "admission": {
          "configured": "Closed",
          "effective": "Closed",
          "epoch": 42,
          "fallback": null,
          "resolved_current": 4,
          "required_current": 3,
          "connected_current": 3,
          "contradicted": 0
        },
        "entries": [
          {
            "bls_pubkey": "0xaecb3fe3d22a958237df2fdb9c6932c837687151bf8b819f18296cce5c5967fb215f16445486c93dbf6a2c933969a93601c2fda599ece5eb4f44c7a311f061d1390e8337230c72085387af3bef1854ef45307aea81704241cbf9ad10717cf4df",
            "slots": ["current", "eligible"],
            "peer_id": "12D3KooWRawPbxPtP1eZaJpumGnyWX2DcUyd3RQnydr3eAto4Az7",
            "network_pubkey": "4XTTMGud3T6rxzNnDbyHKtJgof7ft69y5Ae22hrFogQWL1wmD",
            "multiaddrs": ["/ip4/203.0.113.10/udp/49590/quic-v1"],
            "record_timestamp": 1793750400,
            "source": "record",
            "admitted": true
          },
          {
            "bls_pubkey": "0xb77ba5d4bf7524aa429f1700dac5ac0a6de363e66ab0ef0aa13f2872c43a6923122ac96b24db2503f7c6ae4f33ae9ab400174206049b6caaba895383af5609ff2cde4d65ce83ba31f9ada80ba83bb47afb60bbc49dd40b9c7b57410bf8bbc525",
            "slots": ["eligible"],
            "peer_id": "12D3KooWSrKnMZUcSxK8G7wmBbXdU8nFEfWGhLu6H8xjn8LmCSJb",
            "network_pubkey": "4XTTMJB1SChwh4KSnHnQBJdRTcjRuiLVipZWL1MNVpMxJAD5h",
            "multiaddrs": ["/ip4/198.51.100.7/udp/49590/quic-v1"],
            "record_timestamp": 1793736000,
            "source": "record",
            "admitted": false
          }
        ]
      }
    ]
  }
}
```

A worker swarm that did not answer in time appears as:

```
{
  "network": "worker-0",
  "worker_id": 0,
  "available": false,
  "tracked_epoch": null,
  "admission": null,
  "entries": []
}
```
