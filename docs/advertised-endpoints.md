# Advertised and listen endpoints

`network.yaml` can configure endpoints independently of `node_info.p2p_info`. Transport keys and
BLS authority identities remain in node info. An absent mapping preserves the node-info address.

```yaml
endpoints:
  primary:
    listen: /ip4/0.0.0.0/udp/49593/quic-v1
    advertise:
      - /ip4/192.0.2.10/udp/49593/quic-v1
  workers:
    0:
      listen: /ip4/0.0.0.0/udp/49594/quic-v1
      advertise:
        - /dns4/worker-old.example.com/udp/49594/quic-v1
        - /dns4/worker-new.example.com/udp/49594/quic-v1
    1:
      listen: /ip6/::/udp/49595/quic-v1
      advertise:
        - /ip6/2001:db8::10/udp/49595/quic-v1
```

The documentation addresses above must be replaced with endpoints peers can reach. `listen` is
optional. `PRIMARY_LISTENER_MULTIADDR` and `WORKER_LISTENER_MULTIADDR` retain precedence for the
primary and worker 0. Worker N greater than zero has `WORKER_N_LISTENER_MULTIADDR`. An endpoint
mapping for an unconfigured worker fails startup. Each worker has its own transport key and local
listener; different `/p2p` suffixes cannot hide a duplicated bind address. An optional advertised
`/p2p` suffix must match that network's own key.

## Record and resource policy

The existing BCS `NetworkInfo` vector and `telcoin-network/node-record/v1` signing domain are
unchanged. Order, endpoint bytes, timestamp and optional RPC info remain signed, bound to chain,
role and worker id. Publishers must still match the transport PeerId, and received records keep
their existing source and return-validation checks. Unsupported signing labels never authenticate.

Each signed record has one to four distinct IP/UDP/QUIC v1 endpoints: two endpoint generations,
each with one IPv4 and one IPv6 address. TCP, relay paths, DNS names on the wire, unspecified IPs,
multicast, IPv4 broadcast, zero ports, foreign PeerIds and canonical duplicates fail validation.
Private IPs remain usable on private networks. Provider storage and peer caches derive their
ceilings from the same record limit. Existing record-byte, peer-count, query, dial-queue and
connection-admission budgets remain in force.

Configured order is dial preference. Explicit peer-manager dials attempt one endpoint at a time
and use at most the four provided addresses. A successful existing connection is retained during
overlap. An accepted newer signed record replaces the cached dial hints and peer-exchange set;
retirement does not erase observed connection IPs or ban accounting. Kademlia's own bounded query
and connection scheduling remains independent of this explicit dial order.

## DNS policy

Only operator configuration accepts `/dns`, `/dns4` and `/dns6`, followed by UDP/QUIC v1 and an
optional matching PeerId. `/dnsaddr` and arbitrary DNS transports are rejected. Resolution is
performed serially at startup before network tasks start. There are at most four queries per
configured network identity, each with a five-second deadline and at most four inspected answers
(plus one overflow sentinel). Excess answers fail startup. `/dns` retains the lowest IPv4 and
lowest IPv6 address; `/dns4` and `/dns6` retain one address in the requested family. Empty results,
missing requested families, duplicates and a resolved list over four endpoints fail startup.

Signed records retain only these resolved IPs. Peers never resolve another validator's advertised
DNS name. DNS changes do not redirect live dialing: re-resolution requires an operator restart
and a new signed record. This prevents peer-triggered DNS rebinding and unbounded resolver refresh
tasks. System resolver internals and timeout cancellation can outlive a lookup, but each startup
can launch at most four lookups per configured identity and has no retry loop. Operators must
control their DNS records and confirm resolved IP ownership and reachability before restarting.

## Supported wire and signing matrix

| Reader/writer profile | One v1 endpoint | Two to four v1 endpoints | Pre-domain signatures |
| --- | --- | --- | --- |
| v0.15.0-adiri (`763febbb`) | Authenticates and admits | Decodes and authenticates, rejects the one-address admission limit | Rejects |
| Main before this change (`16d4dbda`) | Authenticates and admits | Decodes and authenticates, rejects the one-address admission limit | Rejects |
| This change | Authenticates and admits | Authenticates and admits within the shared bound | Rejects |

v0.14.0-adiri (`250e520c`) signs bare `NetworkInfo` rather than the domain-bound v1 payload. It
is outside the supported domain-bound matrix: records from it do not authenticate here, and it
does not authenticate v1 records. The decoder's pre-RPC legacy fallback remains available for
inspection, but does not confer authenticated acceptance. Tests freeze the supported old reader's
wire shape and signing payload, check both directions, reject altered signed bytes and cross-worker
domains, and keep pre-domain records rejected. No new wire field or signing version is introduced.

## Rollout, overlap and retirement

1. Notify validator operators and network providers before changing endpoints. Confirm that all
   supported record consumers, including read-only DHT clients, have the new four-address policy.
   Upgrade readers first while continuing to publish one endpoint. A record with multiple endpoints
   cannot be used during a mixed rollout with one-address readers.
2. Provision and test the new reachable endpoint for the same transport identity. Keep the old
   endpoint operational. For every primary and worker, configure old and new addresses together,
   in the desired preference order, and restart the node to publish a fresh signed overlap record.
   Bind the local socket independently; public and local UDP ports may differ.
3. Observe successful reachability from each peer network, current signed DHT records, established
   connections and consensus participation across the relevant epoch boundary. Do not use a fixed
   sleep as evidence that propagation finished. Retain the old endpoint through the configured
   record TTL, replication interval, disconnected-peer recovery and the operator's epoch overlap
   window. A live old connection may remain until it closes.
4. Remove the old address from `advertise` and restart to sign the retirement record. Confirm peers
   have accepted the newer record before withdrawing the old listener, route or provider resource.
   Existing timestamp freshness rules apply; the retirement timestamp must exceed the overlap
   record. Withdraw obsolete bootstrap dial hints separately, since configuration is an independent
   operator input rather than a signed record.
5. For rollback, keep the old route available, restore its address to the signed list, restore its
   local listener mapping if needed, and restart. Verify the new rollback record is newer than the
   retirement record and that every worker remains reachable under its own transport key.

Advertising an endpoint grants no proxy authorization and bypasses no admission rule. The provider
must preserve the source information required by consensus networking and QUIC address validation,
and QUIC authentication must terminate at the validator's configured transport identity. An arbitrary
proxy that substitutes its own identity or obscures the required source is unsupported. Obtain
provider confirmation for the actual routing setup before production migration.
