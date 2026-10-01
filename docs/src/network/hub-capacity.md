# Public hub capacity profile

`tools/hub-capacity/profile-v1.json` selects an experimental public hub profile
for one primary and two worker swarms. JSON is valid YAML: copy it to the node's
network configuration path, preserving the deployment's bootstrap peers and
hostname. Restart after changing the profile. Inactive configured workers are
included in its allocation.

No live population run has been recorded for this candidate. Its unit tests and
synthetic scoring fixtures do not qualify capacity or complete issue #1476.
Validator validated-address and handshake qualification retains its Launch scope.

## Coupled bounds

| Resource | Per swarm | Whole hub process |
| --- | ---: | ---: |
| Ordinary connected or dialing peers | 64 | 192 peer slots |
| Established connections | 86 | 258 |
| Established connections per PeerId | 1 | Source admission additionally caps each PeerId at 3 |
| Negotiated inbound streams per connection | 16 | 4,128 |
| Receive credit per connection | floor(1 GiB / 258) bytes | At most 1 GiB |
| Observed address rows | Shared accounting | 258 |
| Connections per observed address | Shared accounting | 64 |
| Connections per IPv4 /24 or IPv6 /64 | Shared accounting | 192 |
| Banned, disconnected, temporarily banned peers | 512 in each table | Three swarm allocations |
| Gossip target / low / high / outbound floor | 12 / 8 / 16 / 4 per topic | Primary and every worker |

The ordinary limit excludes peers with a current retention privilege. The 22
remaining connection slots accommodate 8 operator-provisioned DAO observers and
12 distinct committee peers across previous/current/next committees, plus 2
bootstrap/hub identities. Provision
DAO identities as trusted peers and bound the protected population to this
envelope. Additional protected identities compete for these slots. Protocol
violations can still cause bans, and every peer remains subject to the hard
process, source, stream, and receive-credit limits.

Network allocations do not bound RSS: engine state, record storage, caches, RPC,
cryptography, queues, and allocator overhead also consume memory. Qualification
therefore measures the whole process alongside every primary/worker allocation.
Record retention protects operator and committee records under the existing
finite record/provider quotas. TTL remains 48 hours, publication 12 hours,
and replication 1 hour.

| Serve class | Whole hub process concurrency |
| --- | ---: |
| Primary sync/epoch streams | 5 |
| Primary epoch-record RPC serving | 5, separate from vote handling and stream serving |
| Primary shed response tasks | 8 |
| Worker batch streams | 5 per worker, 10 total |
| Worker shed response tasks | 8 per worker, 16 total |
| Worker gossip prefetch | 8 per worker, 16 total |

Existing class admission also caps pending requests from one peer at 2. Bulk
admission uses nonblocking semaphore acquisition; prefetch also deduplicates.
These limits do not establish committee liveness under concurrent public traffic.
The six `serve_limits` fields select these independent budgets. Omitted fields
preserve the existing defaults; zero limits are rejected. The shipped profile
pins all six values explicitly. Each primary and worker exports its allocation
and occupancy, including idle swarms. A reserved task counts before its first
poll and releases its measurement on completion, failure, or cancellation.

## Predeclared qualification

```sh
python3 -I tools/hub-capacity/qualify.py template --output declaration.json
python3 -I tools/hub-capacity/qualify.py freeze declaration.json --output plan.json
```

Before freezing, fill in exact source revisions, build commands, binary SHA-256,
the baseline's effective configuration, hub IDs, a live adapter command, and
hardware/network setup. Archive the frozen plan before traffic starts. A changed
configuration or envelope requires a new plan and complete baseline/candidate run.

The template proposes two hubs, each with 4 dedicated CPUs, 8 GiB RAM, a 25 Mbit/s
link, 50 ms RTT, and 0.1% loss. It declares 64 public peers (16 sharing one NAT),
8 DAO observers, 12 committee identities across rotation, and 2 workers per hub.
Each phase lasts at least 10 minutes. The PR author selects these experimental
criteria under the requester's instruction to use engineering judgment.

Each scenario requires 99% success. The template sets absolute p99 limits:
joins 8 s, shared-NAT reconnects 20 s, gossip beyond a direct hub 3 s,
record/submit-URL resolution 3 s, concurrent sync 30 s, committee progress 1.5 s,
and DAO connectivity checks 1 s. It declares minimum attempts for every scenario.
Whole-process RSS must stay at or below 4 GiB, CPU at or below 3 cores per sample
interval, queue occupancy at or below 100, and DAO connectivity at 8 throughout.
Application progress must never regress or stall for more than 15 seconds.

## Collection and concurrent workload

`collect.py` measures Linux hub processes through `/proc` and their Prometheus
endpoints. It verifies the running executable, declared revision, exact profile,
and frozen driver command. It rejects process restarts, missing metrics, sparse
samples, and incomplete workloads. It retains raw process data, Prometheus
expositions, topology, workload output, and operation logs with SHA-256 hashes.
The collector supports hubs in local network namespaces; separate physical hosts
need collection on each host and a coordinator that preserves the same evidence.

Declare the same eight `dao_observers` BLS identities in both phases. Every
candidate observer must appear in `bootstrap_peers`, whose existing trusted-peer
policy supplies retention protection. Startup rejects more than eight declared
observers or an observer missing from this trusted set. The scorer checks DAO
connectivity on the primary and both workers, and requires the declared ordinary
population to be observed on every swarm.

Collector bindings name every hub's PID, metrics URL, revision, profile file, and
monotonic application-progress metric selector. Use a consensus or canonical
execution progress metric, rather than a network event counter. For example:

```json
{
  "hubs": {
    "hub-0": {
      "pid": 1234,
      "metrics_url": "http://127.0.0.1:9100/metrics",
      "revision": "REPLACE_WITH_SOURCE_SHA",
      "profile_path": "candidate-network.json",
      "progress": {"name": "REPLACE_WITH_APPLICATION_PROGRESS_METRIC"}
    }
  },
  "workload": ["python3", "-B", "-I", "tools/hub-capacity/workload.py", "plan.json", "manifest.json", "--manifest-sha256", "REPLACE_WITH_MANIFEST_SHA256"],
  "topology_artifact": "topology.json"
}
```

Add `hub-1` for the two-hub envelope. The exact shell-quoted workload argument
vector must equal the frozen `adapter_command`. Build the peer population,
provision the DAO identities, and wait for the declared connections before
starting measurement. Preserve the commands that establish CPU/RAM constraints,
25 Mbps links, 50 ms RTT, loss, and shared-NAT placement in `topology.json`.
Spread protected identities and other sources across the declared subnets so
the address and prefix accounting matches the intended NAT envelope.

`workload.py` schedules all eight scenarios concurrently for the frozen duration,
with at most sixteen executing agent commands per scenario and no unbounded
waiting queue. A scenario definition supplies `concurrency` and an `agents` list,
each entry containing a distinct `identity` and an executable `argv` vector.
Public joins require 64 identities, shared-NAT reconnects require 16, and DAO
connectivity requires 8. Agent commands perform real protocol operations and
return JSON with `operation_id`, `scenario`, boolean `success`, a refusal reason
on failure, and a raw protocol `trace` on success. The driver supplies the first
two values through `HUB_CAPACITY_OPERATION_ID` and `HUB_CAPACITY_SCENARIO`.
Successful gossip also returns a `route` of distinct sender, relay, and receiver
identities. The driver records measured latency, completion time, argv, raw
stdout/stderr, timeouts, and admission failures, including unsuccessful attempts.
Protocol-specific agent implementations and the reproducible live population
run still remain to be completed. The unit fixtures are synthetic.

```sh
python3 -B -I tools/hub-capacity/collect.py plan.json bindings-baseline.json \
  --phase baseline --output baseline-run
python3 -B -I tools/hub-capacity/collect.py plan.json bindings-candidate.json \
  --phase candidate --output candidate-run
```

```sh
python3 -I tools/hub-capacity/qualify.py score plan.json baseline-run/evidence.json candidate-run/evidence.json \
  --output report.json
```

The scorer binds evidence to the frozen plan, revision, binary and configuration
digests, and envelope; checks all swarms and scenarios; and verifies raw artifact
SHA-256. Artifact paths are absolute or relative to their evidence file. Split
logs into at most 64 files of at most 64 MiB per phase. Sparse/missing/nonfinite
data and direct-only gossip fail validation. Reports retain both phases;
candidate threshold failures exit 1 and invalid evidence exits 2. A passing report
still requires review of raw traffic/topology/overlap traces.
