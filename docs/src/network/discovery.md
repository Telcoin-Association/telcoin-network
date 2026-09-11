# Discovery

Telcoin Network nodes find each other through a Kademlia DHT of signed node records.
A record maps a validator's BLS public key to the network key and address its swarm is listening on, so a node holding a committee's key list can resolve every member to a dialable peer.

This page is for RPC providers, dapp and indexer operators, bridge partners, and validators who need to know how a node locates the committee, what it admits into its routing table, and what it rejects.
Every value below is the constant the node compiles with.

The record format and its signing domain live in [`crates/network-libp2p/src/types.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/types.rs).
The discovery loop lives in [`crates/network-libp2p/src/consensus.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/consensus.rs).
The persistent DHT store lives in [`crates/network-libp2p/src/kad.rs`](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/network-libp2p/src/kad.rs).
[Transport](transport.md) covers the protocols these queries negotiate and [Peers](peers.md) covers the reputation scoring that discovery feeds.

## The node record

A node record is keyed on the **primary's BLS public key**, on the primary network and on every worker network alike.
One key therefore resolves an entire node: the primary swarm and each worker swarm publish their own record under the same key, on their own DHT.

The value is a signed `NetworkInfo`: the swarm's network public key, its advertised multiaddrs, a publication timestamp, and optional RPC endpoint information.
Only worker records carry RPC info, and only for validators that choose to advertise it publicly.
The timestamp is what lets a node keep the freshest record when several arrive for the same key.

## Domain separation

The bytes a record's BLS signature covers are wider than the bytes on the wire:

```text
signed_payload = label            # "telcoin-network/node-record/v1"
               || chain_id        # from genesis
               || role            # 0 = primary, 1 = worker
               || worker_id       # 0 for a primary record
               || NetworkInfo     # the only part transmitted
```

The label, chain id, role, and worker id form a domain that is **never transmitted**.
A verifier rebuilds the domain from its own network identity, so a record verifies only where it was meant to be published.
A validly signed worker record does not verify on the primary DHT, a worker 0 record does not verify on worker 1, and a record signed for one chain does not verify on another.
This is what closes cross-role and cross-chain record replay.

The trailing `v1` is the signed-payload schema version.
Records signed by software predating the domain do not verify and are dropped after an upgrade, including at startup when the persisted store is reloaded.

## What validation requires

An inbound record is accepted only when all of the following hold.

1. The record key parses as a BLS public key.
2. The signature verifies against that key over the domain the verifier rebuilt for itself.
3. The record advertises at most **1** multiaddr.
   An honest node publishes exactly one address per record, so a longer list is only an attempt to write attacker-chosen bytes into the peer entry.
4. The record's publisher equals the peer id derived from the network public key inside the record.
   Without this, a peer could republish someone else's stale record under its own connection.

A record that fails any of these earns the sender a `Fatal` penalty, which bans immediately.
A record that is valid but older than the one already stored is ignored without penalty: republication after a restart is normal.

Passing validation is not the same as entering the resolution cache.
A record learned from a DHT query is cached only when its BLS key belongs to the previous, current, or next committee.
A peer that pushes its **own** record over its own authenticated connection has its key-to-peer-id binding confirmed so the live connection survives, but a non-committee key never enters the committee resolution cache.

## Kademlia configuration

- **`Mode::Server`, unconditionally.**
  Every node serves DHT queries, including observers and RPC nodes.
  A node that only consumed the DHT would take routing capacity without returning any.
- **Manual k-bucket inserts.**
  libp2p never adds a peer to the routing table on its own; a peer is added when the node's own peer manager registers the connection, and removed when it disconnects or is banned.
  Discovery and reputation therefore share one admission decision.
- **k-bucket size 20**, the standard Kademlia replication parameter.
- **Record TTL 48 hours**, applied to both records and provider records.
- **Republication every 12 hours**, comfortably inside the TTL so a node's own record never lapses.
- **60 second query timeout.**
  A closest-peers query that times out still yields the peers it reached, and those are kept: queries are slowest exactly when the node is short of peers and needs them most.
- **`FilterBoth` record filtering.**
  No inbound record or provider record reaches the store without first passing through the validation above.
  This makes the node's own handler the sole write path for peer-supplied data.

## The record store

The store is persistent, backed by the consensus database rather than memory, so a restart does not erase what the node learned.
Rows are namespaced by the same role discriminant and worker id that the signing domain uses, so a primary and each of its workers keep entirely separate record and provider tables in the shared database.

Sizing bounds the node's DHT storage duty at 1024 records, a 65 KiB maximum value size, 1024 provided keys, and 20 providers per key.
Provider records carry the same 48 hour TTL, which means a saturated provider table expires nothing for two days; the eviction scan that runs when the table is full is therefore throttled to at most once per 60 seconds so a full table cannot be turned into a full-table scan per inbound message.

Rows whose bytes no longer decode — after a schema change or on-disk corruption — are purged at startup instead of failing the read.

## Six ways a node finds the committee

1. **Bootstrap peers.**
   The first contacts come from the committee's own declared bootstrap servers, read from committee state at startup, not from a CLI flag or a static file the operator maintains.
   The primary network takes each server's primary address; a worker network takes that server's entry for its own worker id, and skips servers that run fewer workers.
   Bootstrap peers are registered once per process, before committee membership is applied, and are pinned so committee rotation never evicts them.
2. **Trusted and explicit peers.**
   Operator-provisioned peers are pinned the same way and are penalty-exempt: severe and fatal penalties are recorded but never applied, so a trusted peer cannot be scored out of the peer set.
   See [Peers](peers.md) for what exemption covers.
3. **Record lookup on missing authorities.**
   When the committee changes, or when a caller asks for validator RPC endpoints, the node issues a DHT record lookup for every committee key it cannot yet resolve.
   Polling callers report the same key as missing on every call, so lookups are deduped against in-flight queries: at most one live query per key, re-armed as soon as that query ends.
   Without the dedupe, one unreachable authority would fan out into duplicate queries for as long as it stayed unreachable.
4. **Closest-peers walk on the discovery heartbeat.**
   The peer manager's heartbeat runs every 30 seconds.
   When the pool of dialable discovery candidates is below its target, the node issues a closest-peers query for a random key, which is the standard Kademlia way to sample unfamiliar regions of the routing table.
   Candidates are dialed at random from the pool until the connection target is met.
5. **Peer exchange on graceful disconnect.**
   A node that is over its peer target disconnects gracefully and sends a peer-exchange map first, which the receiver folds into its discovery pool.
   Exchange results fill spare capacity only, never displacing existing entries, which keeps DHT-discovered peers ahead of hearsay from a departing connection.
6. **Direct record push to first-time peers.**
   On a first connection the node pushes its own record straight to the new peer, so that peer can resolve its BLS key immediately instead of waiting up to 12 hours for the next republication.
   The de-dup set behind this is a 10,000-entry LRU, so a peer that reconnects repeatedly is pushed to once, not once per connection.

Every candidate from any source is filtered before it is dialed: never the node's own identity, at most one address, at least one valid IP, no banned IP, and not already connected or dialing.

## Why query results are not written back

A record that arrives as the answer to a lookup is promoted only into the in-memory resolution cache.
It is deliberately not written to the persistent store.

The node needs the resolution, not the record.
Storing a third-party record would enlist libp2p's replication job to republish it on every replication run, roughly hourly, turning every node into an hourly replicator of records it was never asked to serve.
It would also spend the store's record cap — the node's actual DHT storage duty — on query traffic.
Nothing is lost, because peers push their own records on first connect, and that is the path that legitimately fills the store.

## Rate limits on inbound writes

Two inbound message types write to the consensus database, and both are rate-limited per source before any signature is verified or any row is touched.

| Message | Budget | Window | Over budget |
|---------|--------|--------|-------------|
| `PutRecord` | 30 per source | 60s fixed | dropped, `Medium` penalty |
| `AddProvider` | 5 per provider | 60s sliding | dropped, `Medium` penalty |

Each admitted `PutRecord` costs a roughly 1 ms BLS verification plus a database write on the same task that relays consensus gossip, and a self-signed record from an unbanned peer passes every other check, so without a budget one peer could starve the event loop with valid records.
Honest traffic sits far below both numbers: a node refreshes its record on the 12 hour republication cadence and libp2p's roughly hourly replication run, which is a handful of messages per minute even after a restart.
Each admitted `AddProvider` costs a row decode, merge, re-encode, and a physical database commit, and repeating it for a key already stored skips the store's capacity gate entirely; honest traffic peaks at about one message per peer per minute.

Both window maps are capped at 1024 tracked sources.
At capacity, a source that is not already tracked is **denied** rather than evicting a tracked one.
The alternative would hand an attacker a reset button: cycle fresh peer identities until your own tracking entry is evicted, then start a fresh budget.

A `Medium` penalty bans a peer after roughly ten occurrences; [Peers](peers.md) has the full scoring model.

## Epoch startup

Epoch startup blocks until the node has at least one connected peer on the network it is joining, polling every 500 milliseconds and giving up after 240 attempts — roughly 2 minutes.
Failing that, startup errors out rather than hanging indefinitely on a network the node cannot join.

Dials to individual committee members are more forgiving.
They retry with backoff doubling to 120 seconds and give up only after ten retries **and** only once at least one other peer is connected, so a node that is completely isolated keeps trying rather than abandoning its only routes in.

> [!NOTE]
> Reaching a peer is only the first step.
> A connection still has to clear the peer manager's admission checks and, for gossip, the topic publisher allowlists described in [Gossip](gossip.md).

## Source of truth

| Behavior | Code |
|----------|------|
| Record format, signing domain, validation | `crates/network-libp2p/src/types.rs` (`NodeRecord`, `RecordDomain`) |
| Kademlia configuration, discovery loop, record push | `crates/network-libp2p/src/consensus.rs` |
| Persistent record and provider store, sizing | `crates/network-libp2p/src/kad.rs` (`KadStore`) |
| Inbound rate limits, discovery heartbeat, peer pools | `crates/network-libp2p/src/peers/manager.rs` |
| Advertised address cap | `crates/network-libp2p/src/peers/peer.rs` (`MAX_MULTIADDRS_PER_PEER`) |
| TTL, republication interval, k-bucket size, peer targets | `crates/config/src/network.rs` |
| Bootstrap peer sourcing, startup peer wait | `crates/node/src/manager/node/start_epoch.rs` |

This page mirrors those files.
Update this page when those files change.
