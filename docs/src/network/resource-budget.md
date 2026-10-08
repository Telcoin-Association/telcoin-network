# Established connection calibration

The optional `process_budget` in the YAML `network-config` file allocates established QUIC resources across the
primary and every configured worker swarm. Production limits and deployment qualification remain
pending [issue #1433](https://github.com/Telcoin-Association/telcoin-network/issues/1433).
Omitting this section preserves the existing eight connections per peer and existing QUIC settings.

## Allocation

The following is an arithmetic example, **not a calibrated production recommendation**:

```yaml
process_budget:
  swarm_count: 2
  max_established_connections: 144
  max_established_connections_per_peer: 8
  max_inbound_streams: 4096
  max_receive_credit_bytes: 1073741824
```

For `S` swarms and process connection ceiling `C`, each swarm receives `floor(C / S)` connections.
Let `A = S * floor(C / S)`. Each connection receives at most `floor(max_inbound_streams / A)` incoming
bidirectional streams and `floor(max_receive_credit_bytes / A)` bytes of connection receive credit.
Remainders stay unused. Transport values saturate at `u32::MAX`, and explicitly lower `quic_config`
settings remain lower. Stream receive credit cannot exceed the effective connection receive credit.
In this example there are 72 connections per swarm, 28 streams per connection and 7,456,540 bytes
of credit per connection. These are capacities, not observed usage.

The node checks `swarm_count` against one primary plus every configured worker before spawning
any swarm. Include workers currently inactive on chain, since their swarms still run. Changing the
worker count requires recalculating the allocation and restarting the node. An allocation too small
to give each swarm one connection, or each connection one stream/byte, fails startup.

Inbound and outbound connections, committee members, trusted peers and ordinary peers all consume
the same established limits. Configured bootstrap identities reserve admission capacity within
those limits; total and per-peer ceilings still apply. Runtime bootstrap or logical trust changes
do not expand this startup reservation set. Swarms cannot borrow each other's allocation, which
preserves primary connection capacity when workers fill their allocation.

Bulk traffic can still delay votes or epoch records within a swarm. The
[inbound service class](#inbound-service-classes) metrics expose this delay; admission policy and
message scheduling need joint calibration. The [public hub capacity profile](hub-capacity.md)
documents the additional bounded stream admission and its qualification envelope.

Receive credit is advertised protocol capacity. It is not RSS or a bound on application buffers,
tasks, CPU, locally initiated streams, pre-admission handshake state, or transient transport state
before an established connection is accepted. Measure those independently. TCP is not configured
by the current consensus swarm builder. Other network servers in the process require separate
headroom.

## Inbound service classes

Each swarm sends inbound requests and gossip to the application through one bounded queue. The swarm
puts each inbound message in one service class:

| Class | Messages |
| --- | --- |
| `vote` | Primary vote requests (critical) |
| `epoch_record` | Primary epoch record requests (critical) |
| `certificate_sync` | Certificate catch-up requests on the request-response protocol (bulk) |
| `batch` | Worker batch reports. `ReportBatch` is the 2f+1 quorum-ack request, so it is critical on worker swarms |
| `gossip` | Gossip messages |
| `other` | Peer exchange, stream catch-up, batch fetch and all other requests |

No current primary request uses `certificate_sync`. Certificate catch-up and batch fetch use the
stream protocol, and the swarm cannot see their class before it forwards them, so they are `other`.
Thus `other` is not a low-priority class.

The classes only label the metrics below. They do not change admission, queue space or scheduling.
A message is shed, whatever its class, for one of three reasons. `queue_full`: the queue is full.
`unsubscribed`: no application task receives from a regular queue. The primary network event queue
does not use this reason. `admission`: the application drops the request at its admission check. A
shed request gets no response: the swarm drops the response channel, libp2p closes the stream, and
the requester gets a stream error at once. It does not wait for its request timeout. The swarm does
not schedule by priority, so unanswered requests of any class can fill the queue and make the swarm
shed votes.

## Observations

The connection metrics below have the configured `network` label (`primary`, `worker-0`, etc.). The
rejection and denial counters also have a `reason` label:

| Metric | Meaning |
| --- | --- |
| `tn_network_established_connections` | Current established connections across both directions |
| `tn_network_established_connection_limit` | Per-swarm ceiling; zero means the legacy unbounded total |
| `tn_network_inbound_streams_per_connection_limit` | Effective incoming stream capacity per connection |
| `tn_network_receive_credit_per_connection_bytes` | Effective advertised credit per connection |
| `tn_network_connection_limit_rejections_total` | Connections refused by a connection limit. `reason`: `pending_incoming`, `pending_outgoing`, `established_incoming`, `established_outgoing`, `established_per_peer`, `established_total` or `unknown` |
| `tn_network_inbound_connections_denied_total` | Inbound connections denied by a limit. `reason`: `pending_incoming_limit`, `established_per_peer_limit`, `established_total_limit` or `other_limit` |

Connection occupancy updates after swarm event processing. Scrapes can miss short peaks, so record
the sampling interval and use transport tracing when measuring peak streams or retained buffers.
The connection count times the credit ceiling describes configured capacity on established
connections, not bytes currently retained. Sum that product over every swarm on the same node.
Never interpret missing samples as zero. The connection gauges do not give active-stream occupancy.

The service class metrics also have a `class` label with the six values above. The shed counter also
has a `reason` label (`queue_full`, `unsubscribed` or `admission`). The failure counter also has an
`outcome` label (`timeout`, `omitted`, `closed`, `io` or `unsupported`). The node registers every
class, reason and outcome series at zero when the swarm starts, so a missing series means that the
metric is not exported, not zero.

| Metric | Meaning |
| --- | --- |
| `tn_network_inbound_requests_pending` | Aggregate pending inbound requests, labeled by `network` |
| `tn_network_inbound_requests_pending_by_class` | Inbound requests waiting for an application response, labeled by `network` and `class` |
| `tn_network_inbound_request_service_seconds` | Time from sending a request to the application to sending its response. Answered requests only |
| `tn_network_inbound_requests_shed_total` | Inbound messages dropped before service, by the swarm or at application admission |
| `tn_network_inbound_requests_failed_total` | Inbound requests sent to the application that got no response |

The exporter can render the service time as a summary (quantiles with `_sum` and `_count`) or as
buckets. These metrics give queue occupancy and service time by class, not network round-trip time.
An `admission` shed also counts as an `omitted` failure, so do not add the shed and failure counters.

## Reproducible calibration record

Use ten validators, one primary and one worker swarm per node, on 8 CPU / 32 GiB hosts as the
reference topology. Repeat for other supported worker counts. Record committee size, trusted peer
population, admission exemptions, RTT/loss, storage, database size, catch-up distance, load generator
revision/commands and the baseline node revision. Keep consensus, execution and storage headroom
explicit in the process CPU/RSS/state budgets. Agree vote and epoch-record latency, persistence and
catch-up regression thresholds with maintainers before selecting production limits.

`tools/network-budget/capture.py` records observations for an **externally driven** workload. It does
not start validators, generate hostile traffic, instrument QUIC internals, or certify acceptance.
Create a JSON manifest with these fields:

| Field | Required content |
| --- | --- |
| `revision` | Full 40-character commit of the node build |
| `build_command` | Exact toolchain, flags and build command |
| `topology` | Integer `validators`, `workers_per_node`, `cpus_per_node`, `ram_bytes_per_node` |
| `nodes` | One object per validator, with unique `name` and `metrics_url` |
| `artifacts` | Paths relative to the manifest for binaries, lockfile, all node configs, topology and threshold decisions |
| `workload` | Exact commands, load generator revision, duration, baseline identity and traffic/topology details |
| `decisions` | Agreed thresholds, process headroom and rationale, or an explicit pending decision |

For example, a node entry is `{"name":"validator-0","metrics_url":"http://validator-0:9100/metrics"}`.
Hash inputs are streamed; the tool records hashes and paths, so archive the corresponding artifacts
with the run. Capture the source tree including any patch used to build a dirty revision.

```sh
python3 tools/network-budget/capture.py calibration.json results/baseline --phase baseline --samples 600
python3 tools/network-budget/capture.py candidate.json results/catch-up --phase catch-up --samples 600
python3 -m unittest discover -s tools/network-budget -p 'test_*.py'
```

Use a fresh output directory for every run. The collector saves provenance, timestamped JSONL
observations and scrape errors. It accepts only fixed resource metric names and topology-bounded
labels, limits each HTTP response to 8 MiB, and exits nonzero if any scrape fails. Missing metrics and
swarm labels are listed explicitly. If the exporter does not expose `reth_process_resident_memory_bytes`
or `reth_process_cpu_seconds_total`, collect RSS and CPU with the deployment's OS/container observer and
archive those traces separately. Include task counts, active streams and retained buffers from
dedicated tracing. The collector's result always leaves acceptance pending. Each node entry can also
set `rpc_url`. The collector then reads `eth_blockNumber` at each sample for persistence and catch-up,
and records a missing value as missing, never as zero.

### Harness, evaluation and derivation

`tools/network-budget/harness.py` runs the phases over ssh from a JSON inventory. `plan` prints every
command and runs nothing. `setup` generates keys and genesis on the hosts. `run` stages the network
config for one build, starts the nodes and calls `capture.py`. Only the candidate build writes
`process_budget`, and the harness refuses a template that already sets it. The hostile and mixed
phases need a load generator command in the inventory: `generator`, with a `{target}` placeholder,
and `hostile_targets`. The generator must print one JSON object. No generator ships with this tool.
Without one, the harness does not run these phases, and their thresholds stay pending.

Before the capture starts, `run` polls each `metrics_url` until it answers, for at most
`ready_timeout_secs` (default 60). `stop` sends SIGTERM and waits for at most `stop_timeout_secs`
(default 60). Then it sends SIGKILL. It keeps the pid file until the process exits, and it fails if
the process is still alive. `begin` refuses a pid file that names a live process, and it appends to
`node.log` after a start marker. In the reconnect phase, `phase.json` records an expected-down
window for each restarted node. `run` ignores scrape failures only when all failures are accounted
for inside those windows. An unexplained nonzero collector exit remains a failure.

`evaluate.py` gives pass, fail or pending for each threshold. Missing data is pending, never zero.
A threshold is pending when any manifest node or swarm lacks the metric and class it needs in a
phase. Counter deltas and CPU rates require at least two samples per series. A record with a scrape
error, missing metrics or missing swarms also makes the phase pending outside an expected-down
window. Scrapes overlapping those windows are excluded from measurements. Each
service p99 is the worst (node, network) pair. `critical-failures` does not include the reconnect
phase, because planned restarts close in-flight streams. `critical-sheds` includes it.
It exits nonzero on any fail, and acceptance always stays "pending maintainer decision".
`derive.py` proposes a `process_budget` from the baseline honest phases: the peak times the headroom,
divided like the node allocation. Its transport input is a JSON file with peaks from tracing. It
refuses to run when an input is missing or has a gap that keeps `evaluate.py` pending. `thresholds.proposed.json` holds the proposed thresholds.
Each threshold has the status "proposed" and a rationale that cites the `Parameters` defaults.
Maintainers accept or change them in review.

```sh
python3 tools/network-budget/harness.py inventory.json plan --output results
python3 tools/network-budget/harness.py inventory.json run --build baseline --phase steady --output results
python3 tools/network-budget/evaluate.py tools/network-budget/thresholds.proposed.json results
python3 tools/network-budget/derive.py results transport.json --headroom 1.5 --output proposed-budget.json
```

Run and archive the following phases against baseline and candidate builds with identical state:

1. Honest steady state, sustained catch-up and reconnect overlap. Measure peaks and tails for all
   swarms, including trusted peers and configured inactive workers.
2. Hostile attempts to fill connection and stream ceilings from both one identity and many identities.
   Confirm rejection counters rise, occupancy stays within allocations, and closing connections
   allows recovery. Include committee/trusted identities where the test deployment permits it.
3. Mixed catch-up and ceiling pressure while publishing votes and epoch records. Measure service
   occupancy and tail latency by message class, persistence throughput and catch-up completion.
4. Repeat after reconnects and at every supported worker count. Account for handshake/transient state
   independently from established connection capacity.

Compare each workload with its pinned baseline and agreed thresholds. Record maximum connections,
concurrent streams, credit, retained bytes, tasks, RSS, CPU and service occupancy separately. Leave
missing measurements and threshold decisions pending. Only a maintainer-approved derivation and
passing workload regressions can select defaults or complete #1433.
