# DAO observer hub profile

DAO deployment operators own the observer public-key inventory, approve changes, measure the
capacity needed on each hub, and roll out its `network.yaml`. Each hub keeps its own BLS and
transport keys. Observers keep their own keys too. Provisioning this profile grants admission,
connection retention and forgiveness of temporary load penalties, using the same separated policy
as explicit trusted peers. It does not add a validator, change any committee, authorize publication
on committee-only gossip topics, or exempt an observer from protocol or cryptographic penalties.

The optional `dao_observers` profile reserves connectivity on the primary and every configured
worker swarm. Ordinary peers, bootstrap peers, validators and unrelated trusted peers cannot use
its reserved connection allowances. A full ordinary peer population therefore cannot prevent an
authenticated configured observer from attaching or reattaching.

## Provisioning

Collect each observer's BLS public key and its primary transport public key and dialable QUIC
multiaddress. Collect a distinct transport key and address for every worker ID the hub runs,
including spare configured swarms. Use the public keys in the same serialization format as the
node's existing `node_info` configuration. Never copy observer or validator private keys to a hub.
Worker IDs are explicit map keys; a missing worker entry is an error, not a fallback to worker zero.

Merge this shape into the hub's `network.yaml`, replacing the placeholders with reviewed public
inventory values. Additional observers are additional BLS-keyed entries. `trusted_nodes` uses the
same entry schema and remains independent of the DAO inventory.

```yaml
dao_observers:
  max_peers: 48
  observers:
    '<observer BLS public key>':
      primary:
        network_key: '<observer primary transport public key>'
        network_address: /dns4/observer.example.org/udp/49590/quic-v1
      workers:
        0:
          network_key: '<observer worker 0 transport public key>'
          network_address: /dns4/observer.example.org/udp/49594/quic-v1
        1:
          network_key: '<observer worker 1 transport public key>'
          network_address: /dns4/observer.example.org/udp/49595/quic-v1
```

The number 48 is illustrative, not a measured deployment recommendation. Set `P = max_peers`
from the hub's measured connection, memory and traffic capacity. Startup requires at least
`max_priority_peers() + U`, where `U` is the number of distinct configured BLS identities in the
union of `trusted_nodes` and `dao_observers.observers`. Identical overlapping entries count once.
With the default peer settings, ordinary priority headroom is 45, so one DAO observer requires at
least 46 and two DAO observers plus one unrelated trusted node require at least 48. Add measured
headroom for committee connections and other deployment needs within the same absolute budget.

For `O` configured observer transport identities on a swarm:

- Each observer reserves at most eight concurrent established or reserved connections.
- Non-observers together can reserve at most `8 * (P - O)` connections.
- At most `P - O` distinct non-observer identities can hold reservations or connections.
- The composed libp2p limiter caps established connections at `8 * P`, with at most eight per
  peer, and caps pending outbound connections at `8 * P`.
- Pending inbound authentication remains capped at 1024. QUIC attempt queues and buffered
  datagrams are derived from `P` and the same eight-connection allowance. These limits apply
  separately to each swarm; budget the host for its primary plus all worker swarms.

Ordinary population limits remain separate. DAO identities do not reduce ordinary population
headroom or cause ordinary peers to be pruned simply by using their reservations. Existing stream,
request size, gossip validation, Kademlia rate and service budgets still apply. Load forgiveness
does not disable rate limiting or give the observer an unlimited service allowance. Protocol
violations remain scoreable and bannable, including while the observer is reconnecting.

Startup rejects conflicting BLS/transport bindings, transport keys shared between swarms,
contradictory trusted/DAO entries, conflicting bootstrap identities, `/p2p` address/key mismatches,
missing applicable workers, and insufficient or overflowing budgets before spawning node networks.
Configuration is a transport identity pin: learned records cannot rotate a provisioned transport
key without an inventory rollout. QUIC still authenticates the transport identity, and signed
records and messages still undergo their ordinary cryptographic and protocol validation.

## Reconnection and lifecycle

The swarm installs and dials its matching entries at process startup. Every configured identity
has one retry schedule in the peer manager, retaining its operator-provisioned dial address even
when a learned record supplies different discovery addresses. Retries continue while ordinary peers remain connected
and across committee rotations. Failed attempts back off exponentially from one second to at most
60 seconds, evaluated on the configured peer heartbeat. A connected, already dialing or banned
peer is not dialed again. Swarm shutdown drops its schedules and pending work; there are no
epoch-scoped retry tasks to leak or duplicate.

## Replacement and revocation

Inventory updates take effect through a reviewed configuration rollout and hub restart. This
profile does not implement hot reload. DAO deployment operators must track which hub processes
have applied each inventory revision.

For address or transport-key replacement, edit the corresponding primary or worker entry, review
all swarms, and restart each hub. For a new observer principal, replace its BLS-keyed entry and all
transport identities. Remove the old entry when its attachment is no longer authorized. For
revocation, remove the observer entry and restart every hub carrying that revision. Removed keys
lose their DAO reservations and reconnect schedules on the restarted hub. A key independently
present in `trusted_nodes` keeps that separate trust basis, without a DAO reservation. Bootstrap
configuration and on-chain committee membership are unchanged by either operation.

Removing `dao_observers` disables the deployment profile and its absolute hub connection budget.
To keep the finite hub budget during revocation, retain the profile with `observers: {}` and a
measured `max_peers`. Monitor authenticated connections, dial failures and protocol penalties during
rollout. Exercise a disconnect/reconnect on each applicable swarm while ordinary capacity is full,
then verify the revoked identity no longer receives the reservation on each restarted hub.
