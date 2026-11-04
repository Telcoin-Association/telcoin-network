# QUIC incoming observations

Each primary swarm and each configured worker owns an `IncomingStats` object. The existing
`tn_network` recorder exports its observations with `network="primary"` or `worker-{id}`.
Worker IDs come from local configuration. Remote addresses, peer IDs, token contents and raw
errors never enter this family. Restarting a swarm resets its internal source counters, while
exported counters retain prior lifetimes and add the new source deltas. Restarting the process
resets the recorder. Prometheus `rate` handles process counter resets.

Transport work starts when Quinn receives a datagram. A swarm `IncomingConnection` event is
emitted only after the listener accepts an Initial and starts its TLS upgrade. It does not
mark the start of token parsing, queueing, Retry, or overload drops.

## Resources and sampling

The Prometheus exporter replaces metric-name dots with underscores.

| Exported name | Unit and scope | Meaning |
| --- | --- | --- |
| `tn_network_quic_incoming_queued` | attempts, live per swarm | Actual Quinn incoming slab occupancy, including the endpoint receive queue and incomings held for an application decision. Released by accept, failed accept, Retry, refuse, ignore, or endpoint destruction. |
| `tn_network_quic_incoming_limit` | attempts, live per swarm | Sum of effective `ServerConfig.max_incoming` across listening endpoints, including the dependency default when no override is supplied. |
| `tn_network_quic_incoming_endpoints` | endpoints, live per swarm | Listening endpoints contributing to that ceiling. Client-only endpoints contribute zero. |
| `tn_network_quic_incoming_handshakes` | upgrades, live per swarm | Accepted inbound upgrades, until success, error, timeout or cancellation. These have already left the incoming queue. |
| `tn_network_pending_incoming_connections` | connections, live per swarm | libp2p's pending incoming count, after transport admission. |
| `tn_network_quic_incoming_outcomes_total` | events, cumulative per swarm | Fixed stage, decision and reason counters below. |

Live gauges return to zero on shutdown. Queue and ceiling changes are observed at their actual
protocol mutation sites. Export sampling occurs after network loop events and at least once
per second while the event loop can run, including periods with only pre-swarm traffic.
Short queue spikes can fall between samples; cap-denial counters retain saturation evidence.
Gauges are independent relaxed atomic observations, not a transactionally consistent snapshot.
Very large native counts follow Prometheus floating-point rounding without u32 truncation.
The process total is the sum of the primary and worker series, not another transport limit.
Service-permit gauges describe application admission and are independent of all these resources.

## Fixed outcomes

| Stage | Decision | Reason | Observation site |
| --- | --- | --- | --- |
| listener | retry | unvalidated_address | A Retry was issued for transmission, without starting a handshake. |
| listener | accept | handshake_started | `Incoming::accept` succeeded, before the swarm event. |
| listener | refuse | retry_not_permitted | The policy refused an unvalidated incoming that cannot be retried. |
| listener | ignore | retry_failed | The policy ignored the incoming returned by a failed Retry. |
| endpoint | validate | retry_token | An Initial carries a successfully validated Retry token. |
| endpoint | validate | address_token | An Initial carries a successfully validated NEW_TOKEN token. |
| endpoint | refuse | invalid_retry_token | A decoded Retry token fails expiry or address validation, producing INVALID_TOKEN. |
| endpoint | revalidate | undecodable_token | A nonempty opaque token fails authentication or decoding and is treated as absent. |
| endpoint | revalidate | invalid_address_token | NEW_TOKEN address, expiry or replay validation fails and the token is treated as absent. |
| endpoint | ignore | max_incoming | A new Initial is dropped at the effective incoming ceiling, before token parsing and expensive Initial-key derivation. |
| endpoint | ignore | connection_ids | A new Initial is dropped because connection IDs are exhausted. |
| endpoint | ignore | incoming_buffer_bytes | A datagram for a queued attempt is dropped at its per-attempt or total byte cap. |
| endpoint | refuse | application_refuse | An application explicitly calls the endpoint refusal API. |
| endpoint | ignore | application_ignore | An application explicitly calls the endpoint ignore API. |
| endpoint | accept_error | accept_failed | An accept call fails, including a stale Initial with no close response. |

Events describe stages rather than a partition of unique connections. One Initial may produce
a token-validation event and later an accept event. A policy refusal can also produce an endpoint
API refusal. Retransmitted or separately processed Initials can contribute multiple events.
The buffer counter counts dropped datagrams, not bytes. Do not sum this table into a connection
count or treat it as wire delivery acknowledgement.

In pinned Quinn 0.11.18, an unvalidated incoming permits Retry, so the two policy fallback rows
normally remain zero. Their observations are installed, but these branches are unreachable for
ordinary input under that contract. Real endpoint refuse, ignore and accept-error observations
are separately exercised. The legacy retried, accepted, refused, ignored and budget-yield counters
remain available. Legacy refused includes failed accepts and does not prove a close was sent.

## Retry queries

Use the same five-minute interval and aggregation labels for both rates:

```promql
sum by (instance, network) (
  rate(tn_network_quic_incoming_outcomes_total{
    stage="listener", decision="retry", reason="unvalidated_address"
  }[5m])
)
```

```promql
sum by (instance, network) (
  rate(tn_network_quic_incoming_outcomes_total{
    stage="endpoint", decision="validate", reason="retry_token"
  }[5m])
)
```

If these expressions are recorded as `tn:quic_retry_issued:rate5m` and
`tn:quic_retry_return:rate5m`, the return/issued diagnostic is:

```promql
(tn:quic_retry_return:rate5m / on (instance, network) tn:quic_retry_issued:rate5m)
and on (instance, network) (tn:quic_retry_issued:rate5m > 0)
```

An idle or missing denominator produces no ratio. Display it as unavailable or idle, without
filling it with zero. Rates need enough scrape samples. A return can cross the interval boundary,
so the ratio can exceed one. NEW_TOKEN reconnects are intentionally excluded. Look at the two
rates independently as well as their ratio.

Compare token failures and cap drops over the same interval:

```promql
sum by (instance, network, reason) (
  rate(tn_network_quic_incoming_outcomes_total{
    reason=~"invalid_retry_token|undecodable_token|invalid_address_token|max_incoming|incoming_buffer_bytes"
  }[5m])
)
```

Non-returning traffic is a diagnostic signal. Packet loss, delay, expired tokens, client retries,
restarts and incoming saturation can all reduce returns. The ratio alone cannot establish spoofing.
Quinn does not expose loss of stateless Retry packets or datagrams discarded before its UDP socket
receives them. Those observations are unavailable, and no zero-valued loss series is exported.
When node_exporter provides host UDP counters, inspect `rate(node_netstat_Udp_RcvbufErrors[5m])`
and `rate(node_netstat_Udp_InErrors[5m])` alongside these panels. They are host-wide datagrams,
reset on host restart, and cannot attribute loss to a worker. Keep missing host counters absent.
Use client-side packet capture or the isolated interoperability fixtures to locate wire loss.

Packet parsing and version negotiation also have outcomes before these observation sites.
Short or undecodable packets, unsupported versions, invalid reserved bits and early destination
CID errors have no dedicated series in this observer. Use endpoint tracing or packet capture
for those gaps. They must not be represented by synthetic zero counters. The incoming-cap
check precedes token parsing, so a cap-denied Initial has a cap outcome without a token outcome.

## Measurement companions (#1741)

Two histograms give the honest-service figures that the inbound capacity qualification
(`docs/src/network/inbound-capacity-qualification.md`, read by `etc/quic-qualification/measure.py`)
takes from the node. Both carry the `network` label (`primary` or `worker-{id}`), observe
seconds, are scoped per swarm and reset on process restart. Their other label is a fixed enum.

| Series | Label | Observation |
| --- | --- | --- |
| `tn_network_connection_established_seconds` | `direction`: `inbound`, `outbound` | The swarm's handshake time from the dial or the accept to the established event. |
| `tn_network_outbound_request_duration_seconds` | `outcome`: `response`, `failure` | The client-side round trip of an outbound consensus request, from the send to its response or to its failure (an outbound failure or a peer disconnect). |

The outbound direction is the honest dial handshake time. The honest dial success fraction
combines the existing counters, whose direction label is spelled `out`:

```promql
sum by (instance, network) (increase(tn_network_connections_established_total{direction="out"}[5m]))
/
(
  sum by (instance, network) (increase(tn_network_connections_established_total{direction="out"}[5m]))
  + sum by (instance, network) (increase(tn_network_dial_failures_total[5m]))
)
```

The p99 handshake time of outbound connections:

```promql
histogram_quantile(0.99, sum by (le) (rate(tn_network_connection_established_seconds_bucket{direction="outbound"}[1m])))
```

The process-wide histogram buckets run from 0.005 s to 30 s in 12 steps, so a p99.9 estimate
is bucket-limited: `histogram_quantile` interpolates inside one bucket and returns the upper
bound of the last finite bucket when the quantile falls in `+Inf`. The scrape tool records the
bucket bounds it used beside every quantile. An idle denominator yields no fraction; show it
as unavailable, never as zero.

## Dependency and verification

The pinned dependency's public endpoint stats omit invalid-token outcomes, incoming slab
occupancy and cap denials. `patches/quinn-proto` adds an optional shared atomic observer at those
decision sites. It installs no recorder and changes no token, Retry, queue or cryptographic policy.
The carried libp2p transport shares it with the existing `IncomingStats`; metrics-disabled users
can still use both transports without installing a recorder.

Protocol tests use the existing deterministic packet harness and fake time. They cover queue
release, real Retry returns, expired and opaque tokens, refusal, ignore, failed accept, effective
configuration and saturation across 1,000 source addresses and ports. Candidate tests exercise Retry behavior
and consume the same observations. Metric tests pin the primary and multiple-worker label sets
and shutdown cleanup. The staging-compatible observation workflow runs these checks together.
