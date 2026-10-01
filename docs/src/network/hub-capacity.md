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

## Evidence and remaining live adapter work

`qualify.py` scores measurements; it does not launch nodes or generate traffic.
A live adapter must implement the evidence format exercised in `test_qualify.py`
using real measurements and retained raw telemetry/workload artifacts. The
fixtures are synthetic. A live adapter, occupancy instrumentation, and a
reproducible population run remain required to complete the issue.

Use `tools/network-budget/capture.py` for existing allocation and process metrics.
Add source-table occupancy, queue and serve-task occupancy, application progress,
and DAO connectivity. Missing metrics remain missing. Capture every hub and
every primary/worker at intervals of at most 5 seconds. Each operation records a
unique ID, completion time, latency, success, and rejection reason. Successful
gossip includes a verified route of at least two hops. Retain topology and overlap
traces proving that public traffic and sync ran concurrently with committee work.

```sh
python3 -I tools/hub-capacity/qualify.py score plan.json baseline.json candidate.json \
  --output report.json
```

The scorer binds evidence to the frozen plan, revision, binary and configuration
digests, and envelope; checks all swarms and scenarios; and verifies raw artifact
SHA-256. Artifact paths are absolute or relative to their evidence file. Split
logs into at most 64 files of at most 64 MiB per phase. Sparse/missing/nonfinite
data and direct-only gossip fail validation. Reports retain both phases;
candidate threshold failures exit 1 and invalid evidence exits 2. A passing report
still requires review of raw traffic/topology/overlap traces.
