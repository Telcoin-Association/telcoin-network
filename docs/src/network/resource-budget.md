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
the same established limits. No peer bypass is installed in the connection-limit behaviour. Peer
admission and trust rules remain separate. Swarms cannot borrow each other's allocation, which
preserves primary connection capacity when workers fill their allocation. This does not reserve
service within a swarm: hostile peers can occupy its slots, and bulk traffic can still delay votes
or epoch records. Admission policy and message scheduling need joint calibration.

Receive credit is advertised protocol capacity. It is not RSS or a bound on application buffers,
tasks, CPU, locally initiated streams, pre-admission handshake state, or transient transport state
before an established connection is accepted. Measure those independently. TCP is not configured
by the current consensus swarm builder. Other network servers in the process require separate
headroom.

## Observations

The Prometheus names below have only the configured `network` label (`primary`, `worker-0`, etc.):

| Metric | Meaning |
| --- | --- |
| `tn_network_established_connections` | Current established connections across both directions |
| `tn_network_established_connection_limit` | Per-swarm ceiling; zero means the legacy unbounded total |
| `tn_network_inbound_streams_per_connection_limit` | Effective incoming stream capacity per connection |
| `tn_network_receive_credit_per_connection_bytes` | Effective advertised credit per connection |
| `tn_network_connection_limit_rejections_total` | Rejections by the total or per-peer connection ceiling |

Connection occupancy updates after swarm event processing. Scrapes can miss short peaks, so record
the sampling interval and use transport tracing when measuring peak streams or retained buffers.
The connection count times the credit ceiling describes configured capacity on established
connections, not bytes currently retained. Sum that product over every swarm on the same node.
Never interpret missing samples as zero. Neither active-stream occupancy nor vote latency is
provided by these new gauges.

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
swarm labels are listed explicitly. If the exporter does not expose `process_resident_memory_bytes`
or `process_cpu_seconds_total`, collect RSS and CPU with the deployment's OS/container observer and
archive those traces separately. Include task counts, active streams, retained buffers and service
occupancy from dedicated tracing. The collector's result always leaves acceptance pending.

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
