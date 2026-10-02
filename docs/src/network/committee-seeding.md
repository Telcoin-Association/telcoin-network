# Local launch committee inventory

Issue [#1477](https://github.com/Telcoin-Association/telcoin-network/issues/1477)
requires validators to find one another without a hub or an existing peer database.
The operator-owned `committee_peers` map in the datadir's YAML `network-config`
file is the authoritative launch inventory. Distribute the same complete map to
every validator before starting the launch network.

Committee membership still comes from chain state. The map binds a BLS key to
the expected libp2p network identity and dial address on the primary and every
worker swarm. libp2p authenticates the remote transport key when connecting.
Adding an inventory entry grants no committee membership, gossip publishing
permission, or operator trust classification.

## Provisioning

Use the same shape as `committee.yaml`'s `bootstrap_servers`, with an explicit
`workers` list indexed by worker ID. Include every current validator, including
the local validator, and every configured worker. Keep primary, worker 0 and
worker 1 keys distinct. A two-worker entry looks like this:

```yaml
committee_peers:
  <validator-BLS-public-key>:
    primary:
      network_key: <primary-network-public-key>
      network_address: /ip4/192.0.2.10/udp/9000/quic-v1
    workers:
      - network_key: <worker-0-network-public-key>
        network_address: /ip4/192.0.2.10/udp/9001/quic-v1
      - network_key: <worker-1-network-public-key>
        network_address: /ip4/192.0.2.10/udp/9002/quic-v1
```

Replace the placeholders and documentation addresses with operator-provided
keys and reachable addresses. Repeat the entry for every validator. The
genesis committee's bootstrap inventory can be copied as a starting point,
but verify that it contains every launch validator and that its endpoints and
worker count are current. An empty or absent `committee_peers` map preserves
discovery-based startup for existing deployments. It does not qualify a
hub-independent Closed launch.

Startup validates coverage, local advertised keys, network-key uniqueness
across validators and swarms, and any `/p2p/<PeerId>` address component before
spawning networks. Each worker must have its own matching identity and entry.
Failures include the affected BLS key and swarm in the diagnostic.

## Selection, union and conflicts

Bootstrap selection is unchanged: a CLI override takes precedence over
`bootstrap_peers` in `network-config`, which takes precedence over the genesis
bootstrap set. A selected empty map falls back to genesis; a selected nonempty
map replaces genesis in full.

After that selection, startup installs bootstrap hints and then committee
hints on the primary and every worker. Unrelated bootstrap and trusted peers
remain present, including non-committee hubs. The committee map is independent
of the bootstrap replacement rule, so selecting hubs as bootstrap peers cannot
erase validator bindings.

- Repeated entries with the same BLS/network binding are idempotent. The
  committee inventory's address takes precedence over a bootstrap or cached
  address for that binding.
- Repeated BLS keys within the YAML committee map are rejected, including
  identical duplicates, so an earlier operator entry is never silently lost.
- Different network keys for the same BLS key and swarm are errors. A network
  key assigned to two BLS keys or to different validator swarms is also an error.
- A contradictory restored or trusted binding rejects the swarm's whole seed
  batch before inserting any committee hint. Reconcile configuration and
  cached peer records before restarting.
- Startup dials each BLS key once per swarm across the bootstrap/committee
  union, and never dials the local validator. Existing bootstrap and committee
  retry behavior remains in effect.

## Launch invariant and manual changes

For the process lifetime, each seeded BLS key keeps its configured network key
and address on each swarm. Signed discovery can refresh RPC metadata but cannot
silently change those launch bindings. Chain committee updates still control
membership and authorization, including during epoch transitions. Seeded hints
survive those transitions so direct dialing needs no hub lookup.

Until automated rotation is qualified, coordinate changes manually:

1. Arrange the validator's new address or network keys with all launch
   operators. Keep the old endpoints available during the coordinated change.
2. Update the corresponding primary or worker entry in every distributed
   `network-config`. Update that validator's node configuration and key material
   at the same time. Reconcile matching bootstrap and trusted entries too.
3. Stop the affected deployment, remove stale peer-cache data for re-keyed
   identities using the deployment's datadir recovery procedure, and restart
   validators with the updated inventory. Preserve execution and consensus
   data according to that procedure.
4. With all hubs unavailable, verify direct connections to every other
   committee member on the primary and each configured worker. Repeat on a
   node with freshly provisioned peer storage. Enable Closed only after its
   admission policy and this connectivity check have both been qualified.

The regression test
`committee_seeding_connects_every_swarm_without_hubs_after_fresh_restart`
starts all fixture validators on the primary and two worker swarms without a
hub. It checks direct connectivity, discards every swarm and peer database,
and repeats from the same serialized inventory. Peer-manager regressions pin
the union, atomic conflict rejection, and fixed launch bindings.
