# Inbound capacity qualification

This runbook measures how much inbound QUIC traffic a validator on
production-class NICs receives before honest service leaves the agreed bounds,
for [issue #1741](https://github.com/Telcoin-Association/telcoin-network/issues/1741).
It gates nothing. Its outputs are an uplink sizing figure per node configuration
and a decision on the dedicated inbound accept task.

## Purpose

Three questions, answered per configuration from the same hosts and workload:

1. The highest received packet rate at which honest service stays inside the
   predeclared thresholds (the uplink sizing figure).
2. Whether a larger UDP receive buffer changes that figure.
3. Whether a dedicated task for inbound connection accepts changes that figure,
   and so whether the task is mandatory, optional, or not needed.

The tooling lives in `etc/quic-qualification/` beside the passive host
collector. Nothing here changes the shipped transport.

## Environment

Use a devnet on production-class NICs with the exact node revision under test,
including the carried QUIC patch revision. Host firewall enforcement stays off
on the qualification hosts, with evidence recorded. Honest committee traffic and
fresh honest dials continue throughout every run. Record the link speed, the
effective MTU, NIC offloads, the UDP receive-buffer sysctls, the CPU allocation
and the kernel; `collect.py` captures the host side of these at the start and
end of each run.

## Approvals

Operators approve the hosts, the NICs and the attack range before any run.
Forged-source traffic leaves the generator host only inside that approved
isolated range. The generator refuses any rate whose wire cost exceeds the
declared fraction of the link, so every run stays below saturation.

## Configurations

| Configuration | How it is set | What is recorded |
| --- | --- | --- |
| 1. plain | The shipped build with the carried patch and the default receive buffer | node revision, carried patch revision, lockfile hash |
| 2. receive_buffer | Host sysctls `net.core.rmem_max` and `net.core.rmem_default` raised before node start | the requested size and the effective size read from the collector's `ss` capture (`skmem` `rb`) |
| 3. accept_task | `quic_config: { inbound_accept_task: { mode: spawned, capacity: 64 } }` in `network-config` | the node revision and the bound: at most `capacity` accepted handshakes wait for the swarm |

Configuration 2 is a host setting; the repository has no receive-buffer knob.
Configuration 3 is an opt-in measurement prototype, default off. With the flag
off the listener path is the shipped one. With it on, each listener spawns one
task that waits for attempts, applies the Retry policy and queues accepted
handshakes in a bounded channel for the swarm. The bound is explicit and
recorded with every run.

## Traffic

`generate.py` ships the two swarms. Both use forged IPv4 sources, both stay
below saturation, and both report offered figures only.

```sh
sudo python3 -I etc/quic-qualification/generate.py initial \
  --template /var/tmp/initial.bin --source-range 198.51.100.0/24 \
  --target 192.0.2.10:9000 --rate 20000 --duration 120 --link-mbps 10000 \
  --output /var/tmp/run-01-initial.json

sudo python3 -I etc/quic-qualification/generate.py peerlist \
  --sources /var/tmp/peer-addresses.txt --size 64 \
  --target 192.0.2.10:9000 --rate 20000 --duration 120 --link-mbps 10000 \
  --output /var/tmp/run-01-peerlist.json
```

The `initial` swarm replays one genuine QUIC Initial (captured from the
`quic-initial` fixture in `testing/quic-interop`) from sources drawn across the
approved range. The `peerlist` swarm sends small datagrams whose forged sources
are the addresses on the node's committee, trusted and bootstrap peer lists,
supplied by the operator from the node's configuration and peer records.

Run one swarm, then the other, then both together, with honest traffic
continuing in every arm. Repeat with order varied and matched unloaded
baselines.

## Measurements

Start the collector and the scrape before the generators and stop them after:

```sh
python3 -I etc/quic-qualification/collect.py \
  --pid 12345 --interface enp1s0 --manifest /var/tmp/run-01.json \
  --duration 180 --interval 1 --output /var/tmp/run-01-collector

python3 -I etc/quic-qualification/measure.py \
  --metrics http://127.0.0.1:9101/metrics --duration 180 --interval 1 \
  --generator /var/tmp/run-01-initial.json --generator /var/tmp/run-01-peerlist.json \
  --output /var/tmp/run-01-measurement.json
```

The node exports metrics only with `--metrics <addr>`. Each figure and where it
comes from:

| Figure | Source | Meaning |
| --- | --- | --- |
| offered rate | generator summary | packets and bits the generator put on the wire, per swarm |
| received rate | `measure.py`, `tn_network_quic_incoming_outcomes_total` | attempts the validator's listener observed, by stage and decision, and in total |
| kernel socket drops | `collect.py`, per-socket drop deltas | datagrams the kernel dropped before the node read them |
| process CPU and RSS | `measure.py`, `reth_process_*` | CPU cores in use and resident memory over the run |
| latency on established connections | `measure.py`, `tn_network_outbound_request_duration_seconds` | p50, p99 and p99.9 of honest request round trips |
| honest dial success and handshake time | `measure.py`, `tn_network_connections_established_total`, `tn_network_dial_failures_total`, `tn_network_connection_established_seconds` | fraction of fresh honest dials that established, and how long the handshake took |
| consensus progress | `measure.py`, `tn_primary_round`, `tn_primary_committed_round`, `tn_primary_subdags_committed_total` | rounds advanced and commits per second |

Offered and received figures stay separate in every output. Quantiles come from
the node's global histogram buckets (0.005 s to 30 s), so p99.9 resolution is
bucket-limited; the measurement reports the bucket bounds it used and flags a
quantile that fell into the overflow bucket.

## Evidence record

Build one record per run from the manifest and the three outputs:

```sh
python3 -I etc/quic-qualification/record.py build \
  --manifest /var/tmp/run-01.json --collector /var/tmp/run-01-collector \
  --measurement /var/tmp/run-01-measurement.json \
  --generator /var/tmp/run-01-initial.json --generator /var/tmp/run-01-peerlist.json \
  --configuration plain --output /var/tmp/run-01-evidence.json
```

The record carries the five groups the issue asks for: `build`, `topology`,
`host_and_traffic`, `method` and `thresholds`, plus `open_limitations` and the
SHA-256 of every input under `artifacts`. Each threshold carries its value,
owner and date from the manifest and a `pass`, `fail` or `unknown` result with
the reason. A missing input is `null` plus a limitation, never a guess. Link the
full logs and raw artifacts from the record rather than pasting them into it.

## Outputs

Per configuration, the capacity figure is the highest received rate among the
runs whose thresholds all pass:

```sh
python3 -I etc/quic-qualification/record.py capacity \
  --records /var/tmp/run-0*-evidence.json --output /var/tmp/plain-capacity.json
```

Post the figures in the related security advisory, not on the public issue.
Record the accept-task decision there too, and summarize it on the issue
without the figures, in this shape:

> Dedicated inbound accept task: mandatory | optional | not needed.
> Reason: the capacity figure for `accept_task` compared with `plain` and
> `receive_buffer` under the same thresholds, and the open limitations.

## Related

- [Issue #1462](https://github.com/Telcoin-Association/telcoin-network/issues/1462)
- [Issue #1691](https://github.com/Telcoin-Association/telcoin-network/issues/1691)
- [Issue #1680](https://github.com/Telcoin-Association/telcoin-network/issues/1680)
- [Issue #1432](https://github.com/Telcoin-Association/telcoin-network/issues/1432)
- The two security advisories linked from `etc/quic-qualification/README.md`
- [Transport](transport.md) for the `inbound_accept_task` configuration row
