# Established source admission

Issue [#1467](https://github.com/Telcoin-Association/telcoin-network/issues/1467) tracks
production qualification. This implementation is opt-in. Honest peer populations,
shared-NAT measurements, deployment prefix policy, and process resource calibration
are still pending. Leaving `network_config.source_admission` absent disables it.

## Validation provenance

The peer manager reserves occupancy only in libp2p's established inbound and outbound
connection callbacks, after the QUIC transport completes its authenticated handshake.
The address comes from the transport's remote endpoint. Peer discovery, signed advertised
addresses, Identify messages, and unauthenticated identity claims cannot charge a source.
Pending inbound callbacks do not charge occupancy, even when QUIC Retry is disabled.
An unsuccessful handshake cannot charge a victim address's established quota.

The adapter accepts direct `/ip4/.../udp/.../quic-v1` and
`/ip6/.../udp/.../quic-v1` endpoints, with an optional final `/p2p/...` component.
Unsupported transport shapes fail admission when the budget is enabled. The authenticated
peer ID supplied by libp2p is the identity key; the address suffix grants no privilege.
IPv4-mapped IPv6 addresses use the same counters as native IPv4. The carried QUIC
transport disables migration, so the observed source remains stable for the connection.
Enabling migration requires a validated address-change accounting contract first.

This established-connection evidence is later than handshake-start validation. The
transport work in [#1447](https://github.com/Telcoin-Association/telcoin-network/pull/1447)
defines the separate return-validation contract for start allowances. Its adapter must
use transport-produced validation provenance and the same canonical address/prefix policy.
An established peer ID or an advertised address cannot substitute for that provenance.
This change does not gate handshake starts or charge spoofable pre-validation arrivals.

## Ownership, bounds, and expiration

The node constructs one budget before any primary or worker swarm starts. All swarms
share its counters for their entire process lifetime, including epoch transitions.
Connection IDs belong to a swarm. Each successful reservation owns one non-cloneable
lease in that swarm; two swarms may independently use the same connection ID.
One atomic admission checks all ceilings before modifying any counter.

Identity replacement leaves existing source and prefix occupancy intact. Distinct sources
and prefixes still consume the aggregate process connection ceiling. Every connection
counts, including simultaneous inbound and outbound connections to one peer.

The shared address and prefix tables each hold at most `max_sources` entries. The identity
table and the combined live leases across all swarms hold at most `max_connections`
entries. Ordered maps release their entries when removed, so alternating activity across
swarms does not retain a separate hash-table high-water allocation in each swarm.

A lease ends on its connection's close, a later behaviour's inbound or outbound denial,
or swarm shutdown. Closing one connection releases its charge even if that peer has
another established connection. A failure before reservation has no lease to release.
An address, prefix, or identity entry expires immediately when its last lease ends.
Live connections never expire from accounting while still consuming transport resources.
This is concurrent occupancy accounting; reconnect rate debt belongs to the transport's
handshake-start policy. No per-source tombstone survives a fully closed population.

## Explicit deployment configuration

Every field is required when `source_admission` is present. There are no historical
default limits or prefix lengths to copy into production.

| Field | Meaning |
| --- | --- |
| `max_connections` | Established connections and reservations across the whole process |
| `max_connections_per_peer` | Connections for one authenticated peer ID across all swarms |
| `max_connections_per_address` | Connections sharing one observed IP, including shared NATs |
| `max_connections_per_prefix` | Connections sharing the selected address-family prefix |
| `max_sources` | Distinct observed addresses retained concurrently |
| `ipv4_prefix_length` | IPv4 prefix length, 0 through 32 |
| `ipv6_prefix_length` | IPv6 prefix length, 0 through 128 |

Counts must be positive. The peer limit and source-table size cannot exceed the process
limit. The address limit cannot exceed the prefix limit, which cannot exceed the process
limit. Invalid configuration fails node startup before any swarm is spawned.

Existing per-swarm transport and peer-manager checks still apply. Effective capacity is
the intersection of those checks and this shared budget. Committee, trusted, bootstrap,
and ordinary peers all consume these counters; this implementation grants no source or
aggregate exemption and creates no trusted reservation. An existing peer-manager
privilege cannot exceed the source budget. Production protected-capacity policy remains
an acceptance decision, including how it composes with [#1448](https://github.com/Telcoin-Association/telcoin-network/pull/1448).
The established ceiling bounds concurrency, rather than every process resource: pending
handshakes, stream buffers, RPC work, and the execution engine need their own finite budgets.

Every denial increments `tn_network.source_admission_denied_total` with `network`,
`direction` (`in` or `out`), and `reason` labels. The reason is the snake-case
`AdmissionError` variant, for example `peer_full` or `process_full`. A denial logs at
`warn` when the peer manager treats the peer as important (for example a committee
member), at `error` when the accounting lock is poisoned, and at `debug` otherwise.

A full budget refuses committee members on inbound connections and on our own outbound
dials. Node startup therefore logs a warning when this budget is configured. Do not
enable it on a validator until a protected-capacity policy exists. Candidate policies:
reserve a share of `max_connections` for important peers; admit important peers past
`ProcessFull` and `SourcesFull` while they are still charged; or evict the lowest-scored
ordinary lease. A committee member is important only after the peer manager learns its
network key.

## Qualification still required

Record the hardware, memory and descriptor headroom, supported worker counts, honest
observer/catch-up populations, simultaneous reconnect multiplicity, measured NAT sharing,
and IPv4/IPv6 deployment policy before proposing production values. Account for the
primary and every active worker together. Address and prefix limits must admit the
measured legitimate population without making identity rotation an escape from occupancy.

Tests exercise a synthetic population of four identities behind one NAT across three
budget instances sharing one state, including reconnects. They establish accounting
semantics, not measured fair scheduling or representative network liveness. Before
enabling production policy, run real primary/worker swarms with shared-NAT observers,
reconnect/catch-up traffic, and hostile contention, and record each swarm's opportunity,
failure rate, and convergence time against agreed thresholds. Reserved service capacity
and deployment measurements remain open. This draft does not close #1467.

## Validation

The production accounting core can be tested without workspace dependencies:

```sh
rustc +1.94 --edition=2021 --test --crate-name source_admission \
  tools/source-admission/tests.rs -D warnings -D missing_docs \
  -D missing_debug_implementations -D rust_2018_idioms -o /tmp/source-admission-tests
/tmp/source-admission-tests
```

The crate's adapter tests exercise the pinned libp2p failure and close events:

```sh
cargo +1.94 test --locked -p tn-network-libp2p source_admission
```

Full workspace integration checks and the repository's required attestation are also
required before review readiness. A skipped draft CI lane does not provide that evidence.
