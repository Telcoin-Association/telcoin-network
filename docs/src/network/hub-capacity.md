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

The GitHub Actions envelope declares two hubs, each with one assigned CPU and
3 GiB RAM. The workstation template uses four CPUs and 8 GiB per hub. Both use a
25 Mbit/s link, 50 ms RTT and 0.1% loss, with 64 ordinary public peers,
including sixteen behind one kernel NAT, eight DAO observers, four active
validators and two workers per hub. The profile reserves twelve committee
identities across previous, current and next committees. Four active validators
exercise the three-swarm process budget within that reservation.

Each phase lasts at least ten minutes. The PR author selected the experimental
criteria under the requester's instruction to use engineering judgment.
`qualify.py freeze` validates and hashes the exact profiles, revisions, binaries,
workload and thresholds before measurement. Changing a profile or envelope
requires a new frozen plan and complete baseline/candidate run.

Every scenario requires 99% success. The p99 limits are joins 8 s, shared-NAT
restarts 20 s, gossip beyond a direct hub 3 s, record and submit-URL resolution
3 s, concurrent sync 30 s, committee requests 1.5 s, and DAO connectivity 1 s.
The plan also declares minimum attempts for each scenario. Committee request
observations retain failures and cancellations. Completed requests determine
success and latency; cancellations must remain at or below 35%, reflecting the
certifier's cancellation of obsolete proposals and requests after quorum.

For GitHub Actions, whole-process RSS must stay at or below 2 GiB and CPU at or
below 0.75 cores per sample interval. The workstation limits are 4 GiB and three
cores. Both require queue occupancy at or below 100 and DAO connectivity at eight
on the primary and each worker throughout measurement. Executed-chain progress
must never regress or stall for more than fifteen seconds.

## Collection and concurrent workload

`docker-run.py` creates a private Linux topology, prepares a fresh local chain
with ID 4476, freezes the plan and runs baseline and candidate phases. Both use
the same binaries, identities, link conditions and concurrent workload. The
baseline uses default network limits with the same bootstrap peers and DAO
identities. The candidate uses `profile-v1.json`. Each phase starts with fresh
databases and waits ninety seconds before measurement.

The `github-actions` preset needs four CPUs and at least 14 GiB RAM. Each hub has
a separate one-CPU, 3 GiB container. The coordinator has two other CPUs and 8 GiB.
The default `workstation` preset needs twelve CPUs and at least 24 GiB RAM, with
four CPUs and 8 GiB for each of the three containers. The coordinator hosts the
other two validators and
72 persistent peer processes. Participant namespaces receive 25 Mbit/s,
25 ms egress delay and 0.1% loss. Sixteen namespace peers share the coordinator's
SNAT address. The coordinator shares a private PID namespace with the hubs so
the collector can inspect their actual processes. Cleanup removes only the
containers and network created by the invocation.

Download the `hub-capacity-linux-arm64-<revision>` artifact from the CI run for the
recorded binary revision. The runner verifies the revision and binary digests and
requires a clean, committed worktree. It permits an ancestor artifact when only
the Python qualification scripts or this document have changed, and rejects changed
code, dependency, profile, workflow, or other inputs. `source-provenance.json` and
the frozen plan record both the binary and qualification revisions. A live run can
start once CI retains the binaries; acceptance also requires the workspace tests,
Clippy, and compiling mutation checks to pass.

```sh
docker build -t tn-capacity-1476-runtime:ubuntu24 tools/hub-capacity
python3 -B -I tools/hub-capacity/docker-run.py \
  --binaries /absolute/path/to/extracted-ci-artifact \
  --output /absolute/path/to/new-qualification-directory
```

Use `--runner-envelope github-actions` for the smaller declared envelope.
The required `hub-capacity-qualification` CI job runs this preset after workspace
compilation, tests, Clippy and compiling mutations pass. It uses the same commit's
attested binaries and retains raw evidence and diagnostics for ninety days.
The artifact excludes generated validator key files and executable copies.

The peer example uses the production libp2p network, deterministic local test
identities and three persistent swarms. Commands perform authenticated joins,
fresh signature-validated record queries, worker submit-URL resolution and
nonempty ACK/DATA/END epoch transfers. Shared-NAT commands restart the actual
peer process, preserve its keys and verify connections to both hubs on every
swarm. DAO checks observe those same live authenticated connections.


Transaction traffic uses 512 offline-signed chain-4476 transactions from the
public Anvil test account funded in the local genesis. `cast` must be installed.
Each transaction carries 32 KiB of deterministic calldata. The signed fixture
is hashed into the workload manifest before freezing: 128 transactions seed
the warmup, and 384 are submitted at a fixed cadence during measurement.
Every RPC acknowledgement and canonical batch-selection observation is retained.

Bulk commands run in bursts of eight, divided between the two hubs. Each command
simultaneously transfers a completed primary epoch pack and four executed batches
on worker-0 and worker-1. The coordinator selects the first completed epoch with
four distinct nonempty executed batches, using retained canonical block responses.
Peers decode each returned batch and verify its digest against those observations.
Successful worker transfers require at least 128 KiB each. Missing workers,
empty transfers, incomplete frame sequences and mismatched digests fail the command.

Gossip receipts retain message ID, author, authenticated forwarding peer and
receipt time. The log service correlates each receipt with the actual publisher
event. Successful routes require three distinct identities and publication
after measurement starts. Committee observations come from every validator's
production JSON log, with the measured hubs' actual vote-request latency,
completion, failure and cancellation data.

`workload.py` schedules all eight scenarios concurrently with bounded command
concurrency. Its frozen manifest binds agent identities and argv; every reply
must match its operation nonce, scenario and identity. Failed commands remain
in the operation log and success denominator. Reconnect scheduling offsets NAT
restarts from ordinary joins.

`collect.py` checks executable SHA-256, argv, CPU affinity, cgroup CPU/RAM
limits, declared revision, exact profile and frozen driver command. It measures
whole-process user and system CPU, RSS, primary and both worker allocations,
class tasks, queues, rejection counters and source-accounting rows. Its progress
selector is `tn_engine_canonical_height`, updated after consensus execution.
Process restarts, missing metrics, sparse sampling, invalid observations and
incomplete workloads fail collection.

The output retains the frozen plan, workload manifest, runtime and topology
declarations, public deployment hashes, raw process/Prometheus samples, all four
validator logs and nonce-bound operation traces. The final scorer verifies raw
artifact hashes and writes baseline/candidate results to `report.json`.
Publish the evidence directories, plan, manifest, report and public topology
inputs. Private keys under `deployment/templates` belong only to the disposable
local fixture. Unit tests and topology smokes alone do not qualify hub capacity.
