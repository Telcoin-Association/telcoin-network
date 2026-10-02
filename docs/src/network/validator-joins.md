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
a one-second transition grace, a 300-second snapshot lease, and two workers charging fee 7.
Each invocation runs five independent samples without retrying a failed qualification.

| Observation | Maximum wait per sample | Required evidence |
| --- | --- | --- |
| Publication after governance transactions | 120 seconds | Hub has authenticated records for the entire five-validator window on every swarm |
| Publication to resolution | 60 seconds | Fresh node resolves the entire window on every swarm |
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
All seven swarm qualifications and the governance qualification must fail as tests, then
the exact original source is restored. Compilation failure cannot satisfy this check.

The evidence directory records the exact checkout and submodule revisions, toolchain,
kernel, five timing samples per role and hub condition, five governance samples, and
per-stage p50, p95, and maximum values. Node logs preserve each attempt independently.
The signed-record swarm fixture reports policy activation separately from the governance
fixture's actual consensus participation. Preserve both: one cannot substitute for the other.
