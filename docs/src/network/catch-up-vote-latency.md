# Vote latency and per-peer requests during catch-up

This page records how the node measures vote latency and per-peer request
load while one validator catches up, and how those measurements turn into
per-peer limits. It is the written half of issue #1742. The executable half is
the `catch_up_contention` harness in `crates/network-libp2p/src/tests/`.

A validator that catches up opens sync streams to the committee peers. Those
streams share a QUIC connection with the vote requests, the epoch record
requests, and every other request-response protocol between the same two
peers. A per-peer limit that is set below the honest peak of that shared
connection slows votes. A limit that is set without a measured peak protects
nothing. The limits need the honest peak, and the honest peak needs a harness
that produces it on demand.

## What the harness measures

| Quantity | Definition | Source |
| --- | --- | --- |
| Vote latency, sync connection | Round trip of a `Vote` request from committee peer 0 to the catch-up peer, while that connection also carries the catch-up sync streams | Harness clock around `send_request` |
| Vote latency, sync connection, after catch-up | The same round trip after the catch-up task has completed | Harness clock |
| Vote latency, plain connection | Round trip of a `Vote` request from committee peer 0 to committee peer 1, a connection with no sync streams | Harness clock |
| In-flight requests per peer | Peak and distribution of concurrent honest requests that a single peer has open at one time | Harness gauge around each round trip |
| Sync streams per connection | Peak concurrent sync streams the catch-up peer holds open on each committee connection | Harness gauge around each stream |
| Hostile streams per connection | Peak concurrent streams a hostile peer holds open on each committee connection | Harness gauge around each stream |
| Established connections | Established peers of the catch-up peer at the end of the arm | `NetworkHandle` |
| Receive credit per connection | The allocated receive credit, bounded by the QUIC connection data window | `NetworkProcessBudget::allocate` and `QuicConfig` |
| Resident harness bytes | Peak sync streams times two frames times the frame size | Derived from the stream peak |
| Process resident set | Peak and final resident set of the test process | `/proc/self/status` on Linux, `ps` on macOS |
| Epoch record service | Count of `EpochRecord` requests served and failed on every committee peer | Harness counters |
| Swarm metrics | Sampled peaks of `tn_network.inbound_requests_pending` for the `vote` and `epoch_record` classes, and the final `inbound_requests_shed_total` and `inbound_requests_failed_total` counters | `metrics` debugging recorder |

Each latency series reports the sample count, p50, p99, p99.9 and max in
milliseconds with the nearest-rank rule. A p99 needs at least 100 samples and
a p99.9 needs at least 1000 samples. Below those counts the field is `null`,
not an estimate.

## Declared bounds

The bounds below were declared before the regression run. A bound that is
declared after the run is not a bound.

| Name | Bound | Environment override | Owner | Date | Status |
| --- | --- | --- | --- | --- | --- |
| `vote_p99_sync_connection_ms` | 250 ms | `TN_M3_VOTE_P99_MS` | Onyeka Obi (MavenRain) | 2026-10-23 | proposed; accepted when this page merges |
| `epoch_record_service_percent` | 100 percent served, zero failed | none | Onyeka Obi (MavenRain) | 2026-10-23 | proposed; accepted when this page merges |
| `catch_up_completion_factor` | regression completion at most 3 times baseline completion | `TN_M3_CATCH_UP_FACTOR` | Onyeka Obi (MavenRain) | 2026-10-23 | proposed; accepted when this page merges |
| `honest_sync_streams_within_derived_ceiling` | honest sync stream peak at or below the derived per-connection ceiling | none | Onyeka Obi (MavenRain) | 2026-10-23 | proposed; accepted when this page merges |

The harness records every bound, its owner, date, status, the observed value
and a verdict in the evidence record. A verdict is `pass`, `fail`, `unknown`
when the observation is missing, or `not_applicable` when the bound needs a
baseline that the arm does not have.

## Baseline run

The baseline arm runs the workload with the default network configuration and
no hostile peers. The workload is one committee of four peers, one catch-up
peer, and vote traffic between every pair of honest peers. Each committee
peer sends an `EpochRecord` request to every other committee peer on a fixed
interval. The catch-up peer opens a fixed number of sync streams to every
committee peer and runs a fixed number of exchanges on each stream. Each
exchange streams a fixed number of frames of a fixed size.

Run the report with:

```sh
cargo test -p tn-network-libp2p --lib -- --ignored --nocapture --test-threads=1 catch_up_contention_report
```

The `catch_up_contention_smoke` test runs both arms with a small workload in
the normal test suite. It checks the plumbing. It does not produce evidence.

## Regression run

The regression arm runs the same workload, with these changes:

* The derived limits are applied through a `NetworkProcessBudget` on every
  peer.
* Hostile peers join. Each hostile peer holds twice the derived stream limit
  open on every committee connection and sends twice the derived in-flight
  limit of `Vote` requests in a loop.

Both arms run in one process in one test invocation, so the two arms share
the host, the toolchain, the node revision and the workload. The regression
evidence record names the baseline evidence record it was derived from.

## Derivation of each per-peer limit

Every limit comes from a baseline peak with one rule:

```text
headroom(peak) = ceil(peak * 1.5), floor 1
```

| Limit | Derivation | Where it lands |
| --- | --- | --- |
| In-flight requests per peer | `headroom(max in-flight peak over honest peers)` | No configuration field exists; the harness records it as a proposed constant |
| Sync streams per connection | `headroom(max sync stream peak over connections) + 8 + in-flight limit` | `max_inbound_streams = limit * connections` in the budget |
| Receive credit per connection | `max(headroom(resident harness bytes peak), 2 * frame size * stream limit, 1 MiB)` | `max_receive_credit_bytes = credit * connections` in the budget |
| Connections per peer | unchanged, `MAX_ESTABLISHED_CONNECTIONS_PER_PEER` | `max_established_connections_per_peer` |

The protocol stream allowance of 8 covers the identify, ping, gossipsub,
Kademlia and request-response streams that the harness does not count. The
in-flight limit is added to the stream limit because every in-flight
request-response exchange holds one stream on the same connection.

The budget divides its totals across the swarm count and the connection
count with floor division. The harness sets the swarm count to one and the
connection count to twice the peer count, so the per-connection values that
`allocate` returns equal the derived limits.

## Evidence record

Each arm writes one JSON file. The location is `$TN_M3_EVIDENCE_DIR` when the
variable is set, or `target/m3-evidence/<arm>-<unix seconds>.json` under the
workspace root. Link the files from the issue. Do not paste them.

| Field | Content |
| --- | --- |
| `arm` | `baseline` or `regression` |
| `node_revision` | `TN_NODE_REVISION` when set, else `git rev-parse HEAD` |
| `cargo_lock_hash` | `git hash-object Cargo.lock` |
| `quic_patch_revision` | last commit that touched `patches/libp2p-quic` |
| `features` | debug assertions, target OS and architecture, crate test configuration |
| `configuration` | the workload, the applied `process_budget`, the derived limits and the derivation rule |
| `topology` | role of every peer, swarms and workers per peer, honest and hostile peer counts, shared addresses, admission policy |
| `host` | OS, architecture, available parallelism, network, firewall and generator limits |
| `metrics` | metric definitions, sampling interval, trial count, the baseline reference, the instrumentation overhead note and the full arm outcome |
| `thresholds` | every declared bound with observed value and verdict |
| `limitations` | the list below, verbatim |

## Limitations

* Every peer listens on loopback. Wide-area latency, loss and path MTU are not
  in the measurement.
* Each peer runs one swarm. A node runs one primary swarm and one worker
  swarm, so repeat the run per role when the worker budget matters.
* The harness serves inbound sync streams itself. The node responder limits,
  its deny path and its per-peer concurrency cap are not exercised.
* The resident set is that of the whole test process, which hosts every peer.
  It is an upper bound for one peer, not a measurement of one peer.
* A p99 needs 100 samples and a p99.9 needs 1000 samples. Short windows
  report `null` for those fields.
* Hostile peers are cooperative tasks with committee identities. They exercise
  the limits, not the admission policy for unknown peers.
* Protocol streams such as identify, ping and gossipsub are not counted. The
  stream limit carries a fixed allowance for them.
* The swarm pending gauges are shared across every peer in the process. A
  sampled peak is the last writer at the sample instant, not a per-peer peak.
* Every number is host dependent. Compare two runs only when they share the
  evidence record fields above.
* A lower constant without a passing regression run does not close this
  issue. The evidence record is the artefact.

## Completion evidence for #1742

| Checkbox in the issue | Evidence |
| --- | --- |
| Baseline measurements with an evidence record | The `baseline` evidence record from one report run, linked from the issue |
| Per-peer limits derived from the baseline with the rule above | The `derived_limits` and `derivation_rule` fields of the `regression` evidence record |
| Regression run under the derived limits with hostile peers and every bound at `pass` or `not_applicable` | The `thresholds` field of the `regression` evidence record, and the passing `catch_up_contention_report` test |
