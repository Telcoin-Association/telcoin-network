# QUIC release qualification

These fixtures implement the release checks in [issue #1432](https://github.com/Telcoin-Association/telcoin-network/issues/1432).
Runtime qualification is performed on the attest box. A loopback matrix pass does
not establish deployment resilience or replace representative hardware evidence.

## Fixtures and ordinary CI

Three independent Cargo workspaces build the same process fixture: stock
libp2p-quic 0.13.1, stock 0.14.0, and the candidate using the node's vendored 0.14.0
transport. Each has a committed lockfile and separate build directory. The current
stock and candidate QUIC, TLS, Quinn, quinn-proto and rustls versions must match
the node lockfile. The controller rejects version drift.

All fixtures include the production `QuicConfig` source and its mapping. An empty
JSON configuration selects node defaults. Configuration events record all shared
settings. The candidate also includes production `QuicIncomingLimits` and applies
the node's Retry, queue and polling policy, recording its effective limits and
aggregate outcomes. Stock fixtures report `node_incoming_policy.applied: false`.
The fixtures isolate transport behavior from storage, consensus and peer policy.

Ordinary CI builds, tests and lints all three fixtures, checks the hardware report
validator, and runs all nine dialer/listener directions. Each direction verifies
three authenticated connections and nine 32 KiB bidirectional stream exchanges.
Application acknowledgements synchronize reconnects without sleeps. A second
phase reorders client datagrams and drops a server reply, then checks the same
traffic. A blackhole relay records resolved outbound and inbound timeout behavior,
including paths that never produce an accepted Incoming event. Default handshake
and idle timeouts are 65 and 30 seconds; measured effective deadlines are retained,
not inferred from configuration or asserted against wall-clock scheduling.

Candidate tests use real Quinn packets, TLS handshakes and production listener
polling. They cover valid, malformed, expired, replayed and incorrectly bound
tokens, listener restart with fresh token keys, all four Quinn Incoming outcomes,
and concurrent reconnects with bounded polling and preserved wakeups. A disabled
Retry mutation must fail assertions on the attest box; a compilation failure is
not accepted as mutation evidence.

Resolved Quinn semantics matter: Retry tokens bind the complete socket address
and are not single-use. NEW_TOKEN validation binds the IP, permits port migration
and uses the enabled Bloom replay log. Undecryptable tokens can be treated as
absent and rechallenged. A replayed Retry can reach server acceptance while the
new client rejects the old original connection identity. Tests distinguish server
reachability from a successful authenticated connection. With the pinned Quinn,
unvalidated Incoming always permits Retry, so the node's defensive Refuse/Ignore
fallbacks cannot be exercised honestly through that state. The tests exercise
the real Quinn Refuse/Ignore APIs and explicitly check this reachability condition.

## Attest box

Use Linux, Python 3.11+, the pinned Rust toolchains, `ip`, `tcpdump`, and permission
to create network namespaces and raw sockets. The isolated lane requires root or
noninteractive sudo. Use a clean checkout of the exact committed candidate and
fresh output directories outside the source tree. Run the existing full-workspace
attestation as well as the additional QUIC lane:

```sh
make attest
make quic-attest \
  QUIC_EVIDENCE_DIR=/var/tmp/quic-1432-evidence \
  QUIC_QUALIFICATION_REPORT=/var/tmp/quic-1432-hardware/report.json
```

`make quic-attest` builds and tests each independent fixture, records dependency
feature trees and binary hashes, runs all nine directions, runs the node's existing
listener scheduling and peer reconnect regressions, performs isolated source
validation and the Retry mutation, then validates and copies hardware evidence.
Its scoped node tests supplement `make attest`; they do not replace it. Missing
hardware evidence or any failed step prevents a passing qualification result.

The privileged harness creates two disconnected namespaces joined only by an
owned veth pair. It checks absence of default routes and never changes host
firewall settings. A genuine encrypted QUIC Initial uses a controlled forged
source that cannot receive the reply. Stock acceptance and candidate Retry are
compared, both forged packets must exist in the pcap, and honest candidate
connections and streams must succeed afterward. Namespaces are removed on exit.
This is a source-validation regression, not a volumetric attack simulation.

## Representative NIC evidence

Prepare `qualification.example.json` as a report in its own evidence directory.
The template contains nulls intentionally and cannot pass validation. Operators
must select ingress, resource and progress thresholds before measurement, based
on the deployment's capacity and service objectives. No production thresholds are
invented by the fixtures. Record timezone-aware selection and measurement times.

Measure actual node binaries on representative NICs with the host firewall
disabled in the isolated qualification environment. Record host/kernel,
NIC/interface/driver/offloads, representativeness, RTT and load conditions in the
environment and raw artifacts. Keep received ingress inside the declared envelope
and below uplink saturation, and report generator capacity separately. Use diverse
real sources, including at least two sources that complete Retry. Exercise each
configured primary/worker swarm separately and then the complete node. Include
every worker in `swarm_roles`, not just the template's worker-0.

Retain a healthy baseline, established traffic, honest committee reconnects and
application telemetry during load. The report requires CPU percent (summed across
cores), RSS bytes, socket drop counts, queue peak (bytes, with queue scope declared
in telemetry), established traffic p99 latency in milliseconds and throughput in
bits/second, reconnect success ratio, and command/timer/consensus progress counts.
Select positive lower bounds for throughput, reconnects and progress, and upper
bounds for resource use, queues, drops and latency before measurement.

The existing host collector can supply resource/NIC observations:

```sh
python3 -I etc/quic-qualification/collect.py \
  --pid 12345 --interface enp1s0 --manifest /var/tmp/run.json \
  --duration 120 --interval 1 --output /var/tmp/quic-1432-hardware/host
```

It does not measure consensus or establish that declared acceptance claims are
true. Supply raw traffic and application telemetry, node binary/configuration,
firewall observations and collector artifacts, with relative paths and SHA-256
hashes. The validator checks completeness, candidate identity, bounds and artifact
integrity. Review the raw evidence as part of release approval. The attest bundle
retains the report and its referenced artifacts alongside fixture results.

## Cadence and retention

| Lane | Cadence | Required evidence |
| --- | --- | --- |
| Ordinary CI | Every PR, merge group, weekly and manual dispatch | Nine directions, token/poll regressions, reconnects, loss/reordering, effective deadlines, JSON events, binary hashes and lockfiles |
| Isolated privileged networking | Before feature release and after relevant transport changes | Genuine forged Initial before/after comparison, pcap, no-accept Retry, honest traffic, node scheduling/reconnect tests and mutation receipt |
| Representative NICs | Before deployment, and after hardware/network changes | Candidate node builds, declared bounds, below-saturation load, per-swarm/full-node observations and raw provenance artifacts |

Retain the candidate commit, toolchains, environment, cadence, commands, lockfiles,
binary hashes, logs, captures and raw hardware evidence. A failed or interrupted
lane remains failed. Passing these checks does not resolve independent aggregate
source-budget work in #1434 or promise resistance at all offered loads.

Keep both stock versions and their directions throughout rollout and while
supported deployments can run 0.13.1. Retire the old fixture only in an explicit
maintenance PR after transport owners verify the deployment inventory and record
the compatibility window's end. This suite sets no calendar retirement date.
