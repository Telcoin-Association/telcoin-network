# Validator joins through open hubs

A governance admission must resolve the validator's BLS-signed primary record and each worker
record before the corresponding closed swarm can use its transport identity. A connection to
a hub, a cached genesis stub, or a next-committee notice alone does not establish readiness.

The qualification uses four current validators, one fresh validator, one open hub, and two
workers per node. It submits the normal registry mint, stake, and activation transactions.
The fresh validator starts with a new database and the hub as its only bootstrap. On the
disposable Linux runner, an owner-matched UDP ACL restricts that process to the hub's three
QUIC ports until its authenticated committee records resolve. Existing validators keep their
direct connections. The ACL is then released, all three swarms must close, and a committed
consensus header must name the fresh validator as leader. After the hub stops, consensus
must advance and the direct committee connections must survive.

## Predeclared acceptance limits

These limits qualify the accelerated loopback fixture. They are not measurements of a
provider's propagation delay or of a production WAN. The fixture uses 15-second epochs,
a one-second peer heartbeat and transition grace, a 300-second snapshot lease, and two
workers charging fee 7. The heartbeat is scaled with the accelerated epochs so admission
reconciliation can observe grace expiry before the next transition.
Each invocation runs five independent samples per healthy/unavailable hub condition without
retrying a failed qualification. In the unavailable governance case, the fresh process has
no QUIC access for a complete 15-second epoch. Every swarm must remain below resolution
and direct-readiness thresholds while existing consensus advances. Only hub access is then
restored; direct validator access remains blocked until authenticated window resolution.

| Observation | Maximum wait per sample | Required evidence |
| --- | --- | --- |
| Publication after governance transactions | 120 seconds | Hub has authenticated records for the entire five-validator window on every swarm |
| Publication to resolution | 60 seconds | Fresh and existing nodes resolve the entire window on every swarm |
| Resolution to direct connection | 60 seconds | Fresh node connects directly to at least three current validators on every swarm |
| Connection to consensus readiness | 120 seconds | Registry activates the validator, every swarm reaches Closed, and the fresh validator leads a committed header |
| Consensus after hub loss | 120 seconds | Block height increases and every swarm retains direct current-validator connections |
| Isolated primary or worker stage | 30 seconds | Signed publication, cold resolution, direct links, then current activation |

The isolated swarm cases also attempt a real unavailable hub endpoint through the full
minimum Grace interval. They require unresolved counts to stay unchanged, no false join
connection, and a successful request between existing validators before the hub starts.
Separate record cases exercise publisher binding rejection, signed address replacement,
re-keying, stale replay rejection, and contradictory overlapping committee notices for
primary, worker 0, and worker 1. Unresolved and contradictory state follow the admission
fallback contract; they must never be reported as successful authenticated resolution.

## Operational lead time

Begin signed address publication and provider ACL preparation at least **15 minutes before
the next-committee notice**. This is a conservative provisional planning margin chosen for
this qualification, not an approved provider service-level agreement. If the provider needs
longer, move publication earlier. Require its propagation acknowledgement and successful
outside-to-validator and validator-to-hub QUIC probes before submitting governance admission.
Keep a complete epoch available for remediation before expected activation.

For primary and every configured worker, verify the advertised IP, UDP port, role, worker
identifier, transport public key, and BLS binding. Configure bidirectional direct validator
QUIC access as well as hub access. Check each worker's advertised submission endpoint
separately. A provider ACL approval for one worker does not approve the other worker or the
primary. After address or transport-key replacement, repeat the probes against the newest
signed record and confirm the old record cannot overwrite it.

Use `tn_network_admission_resolved_window` and `tn_network_admission_required_window` to
observe the full previous/current/next window, with the `network` label identifying each
swarm. Only authenticated records and the local member count as resolved. Compare current
resolution and direct connection metrics separately. Window resolution is discovery
evidence, not a consensus readiness signal. If a hub is unavailable or any required record
or ACL probe is missing, postpone admission rather than infer success from elapsed time.
Established consensus must remain independent of any particular hub.

## Reproduce and retain evidence

Run `bash etc/hub-join-qualification.sh` on a disposable Linux runner with passwordless
`sudo`, `iptables`, `setpriv`, Python 3, the pinned Rust toolchain, cargo-nextest, and the
contracts submodule. The fixture temporarily delegates only its fresh node directory to
numeric UID 59599 and restores directory ownership and the process-specific ACL on exit.
The qualification workflow runs the same command and uploads its logs even on failure.

After successful baseline samples, the lane mutates the new full-window resolution getter.
All seven swarm qualifications and both governance qualifications must fail as tests, then
the exact original source is restored. Compilation failure cannot satisfy this check.

The evidence directory records the exact checkout and submodule revisions, toolchain,
kernel, five timing samples per role and hub condition, five governance samples per hub condition, and
per-stage p50, p95, and maximum values. Node logs preserve each attempt independently.
The signed-record swarm fixture reports policy activation separately from the governance
fixture's actual consensus participation. Preserve both: one cannot substitute for the other.

## Qualification results (2026-10-02)

[Run 36993984423](https://github.com/Telcoin-Association/telcoin-network/actions/runs/36993984423)
passed all four unmodified repository regression lanes, 35 swarm qualifications, and ten
governance qualifications: five healthy-hub and five unavailable-hub samples, without retries.
Every governance sample reached Closed on primary and both workers, committed a header led
by the fresh validator, verified its epoch certificate, and continued consensus after hub loss.
The resolution-getter mutation failed all seven swarm tests and both governance tests as
tests, rather than compilation failures.

The tested checkout was `b00eb690fdc1438d6580826ceb7c7d5f64995e4e`, GitHub's PR merge
commit for source head `1393a68dbada73ba79ac84b94e127d5a097281ec`. Both commits have the
same source tree. The contracts revision was `10cc12b7db43e2fbab67dc6a87fa5e159716bdc0`;
the runner used Rust 1.94.1 and Linux 6.17.0-1022-azure on x86_64. The topology and
accelerated parameters are those declared above, with loopback QUIC and a fresh database
for each joining validator. The unavailable case blocks fresh-process UDP from launch
through a minimum 15-second observation interval after governance receipts. The summary's
`hub_unavailable_seconds` records that enforced interval, rather than total process uptime.

[Raw samples and environment](evidence/hub-join-2026-10-02.json) are preserved byte-for-byte
from the successful run, SHA-256 `6832f161ca2fc2debb7709a731491d122ed2223037d8b3f2e80145bc81177980`.
Each row below has five samples. Percentiles use nearest rank; p95 equals the maximum with
this sample size and does not establish a WAN percentile or confidence interval. Governance
timings are observed milestones after governance receipts, including metric and polling
resolution, rather than timestamps of the first wire publication or provider propagation.

| Hub condition | Governance stage | p50 (ms) | p95 / maximum (ms) | Limit (ms) |
| --- | --- | --- | --- | --- |
| Healthy | Publication | 17,507 | 47,839 | 120,000 |
| Healthy | Publication to resolution | 10,061 | 10,118 | 60,000 |
| Healthy | Resolution to connection | 1,153 | 1,191 | 60,000 |
| Healthy | Connection to consensus readiness | 5,007 | 5,988 | 120,000 |
| Unavailable | Publication | 22,413 | 36,824 | 120,000 |
| Unavailable | Publication to resolution | 10,091 | 10,113 | 60,000 |
| Unavailable | Resolution to connection | 1,129 | 2,173 | 60,000 |
| Unavailable | Connection to consensus readiness | 4,996 | 5,018 | 120,000 |

The isolated swarm fixture separates authenticated publication, cold resolution, direct
connection, and policy activation. Its activation measurement is not consensus participation.
The unavailable endpoint held resolution incomplete through Grace while existing validators
completed direct requests, then the same join path recovered once the hub started.

| Swarm / hub condition | Publication p50 / max (ms) | Resolution p50 / max (ms) | Connection p50 / max (ms) | Policy activation p50 / max (ms) |
| --- | --- | --- | --- | --- |
| Primary / healthy | 62 / 67 | 540 / 549 | 4 / 6 | 1,004 / 1,008 |
| Worker 0 / healthy | 55 / 55 | 536 / 542 | 1 / 5 | 1,005 / 1,010 |
| Worker 1 / healthy | 62 / 63 | 537 / 555 | 3 / 5 | 1,005 / 1,011 |
| Primary / unavailable | 10,060 / 10,061 | 537 / 544 | 2 / 4 | 1,005 / 1,010 |
| Worker 0 / unavailable | 10,057 / 10,058 | 530 / 543 | 3 / 4 | 1,005 / 1,006 |
| Worker 1 / unavailable | 10,058 / 10,059 | 529 / 543 | 3 / 5 | 1,004 / 1,010 |
