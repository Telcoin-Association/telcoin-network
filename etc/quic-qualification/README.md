# QUIC ingress qualification

This is shared measurement tooling for [the UDP receive-path investigation](https://github.com/Telcoin-Association/telcoin-network/security/advisories/GHSA-rf54-hppq-6j4c)
and [the endpoint-lock investigation](https://github.com/Telcoin-Association/telcoin-network/security/advisories/GHSA-5pxp-f3g9-vcwx).
It collects evidence from an actual running node without generating traffic or
changing socket, firewall, or NIC configuration. It introduces no transport fix.

**Deployment qualification is pending.** No representative-hardware bottleneck,
throughput collapse point, severity, or preferred socket layout is established by
this tooling. Parser fixtures and the Linux loopback smoke test are tool checks.
They are not deployment results. The collector always records
`qualification: not_evaluated`; a human-reviewed report supplies the decision.

## Prepare a controlled run

Use representative Linux node and load-generator hosts on an isolated test
network. Run the actual node build containing the minimum Retry implementation
and integrated scheduling fixes. Record their exact revisions, the build command,
toolchain, features, lockfile hash and any dirty diff. The advisory's original
`579aa551` source baseline alone does not establish that these prerequisites exist.

Copy `run.example.json` outside the source tree and fill it in for each run. Null
fields are missing evidence, not defaults. The collector preserves the manifest;
it does not validate the supplied claims. Define acceptance thresholds and the
below-saturation envelope before measuring. Map every primary and worker listener
to its address, swarm role and socket inode. Record host/kernel/NIC details,
topology, MTU, link speed, offloads, runtime threads, CPU allocation and cgroup
limits. Collect node and generator build/configuration artifacts together.

Disable host firewall enforcement on the controlled qualification hosts and
record evidence of that state, including upstream filtering. The collector reads
available firewall rules but never disables them. Missing privileges, empty
command output or an unavailable firewall tool cannot establish the state of all
filtering layers. Keep a separate record when rules are managed outside the host.

Keep established committee traffic and newly initiated honest connections active
throughout each loaded run. Capture successes, failures, timeouts, offered and
achieved throughput, and latency samples at their sources. Include failed and
timed-out attempts in denominators. Record synchronized clocks and the exact
measurement interval used to correlate those results with the node samples.

## Collect

Requirements: Python 3.10 or later, Linux procfs/sysfs, permission to inspect the
target process, and the same network namespace as the target and selected NIC.
Install `iproute2` and `ethtool` for socket and NIC diagnostics. Firewall tools and
some counters may require additional privileges; their errors remain in the
artifacts. Do not run the generator on the measured node unless its CPU/resource
allocation is an explicit experimental variable.

Diagnostic commands use `PATH=/usr/sbin:/usr/bin:/sbin:/bin` and `LC_ALL=C`,
without inheriting the caller's environment. Install tools in those trusted paths.

Run one collector per node process. Replace the PID, interface and manifest path:

```sh
python3 -I etc/quic-qualification/collect.py \
  --pid 12345 --interface enp1s0 --manifest /var/tmp/run-01.json \
  --duration 120 --interval 1 --output /var/tmp/quic-run-01
```

The output directory must not exist. Collection creates:

| Artifact | Evidence |
| --- | --- |
| `metadata.json` | Supplied run manifest, kernel/CPU details, target identity and binary hash, initial host/socket diagnostics |
| `samples.jsonl` | Timestamped socket tables and process-owned socket selection, namespace UDP and NIC counters, process CPU/scheduling and host CPU/softirq/softnet data |
| `summary.json` | Capture status, elapsed time, sample count, measured mean sample interval, counter and per-socket drop deltas, and final host/socket diagnostics |

Host diagnostics run outside the sampled interval. Samples bracket their reads
with monotonic timestamps; they are not atomic kernel snapshots. Use the actual
sample timestamps when deriving rates. The requested interval is a minimum pause
between snapshots, not a guaranteed sampling frequency. Compare a collector-on
control with a collector-off control to quantify observer overhead.
`mean_sample_interval_seconds` is the elapsed time between the first and last
sample starts divided by `samples - 1`, or `null` with fewer than two samples.

Exit zero means the sampling loop completed for the same process identity. It
does not mean every observation was available or that qualification passed.

| Exit code | Meaning |
| --- | --- |
| `0` | Capture completed with `capture_status: complete` |
| `2` | Usage, preflight or output-directory creation failed before collection started |
| `3` | Collection started but was incomplete, interrupted or invalidated; inspect partial artifacts |

The other `capture_status` values are `incomplete` for a collection I/O or value
error, `interrupted` for Ctrl-C, and `invalid_process_identity` for a missing or
changed target identity. `capture_error` records an encountered I/O or value error.
An interrupt during initial diagnostics or metadata writing still attempts to
write a summary. A summary write failure returns `3` and reports the error on stderr.
Missing tools, denied reads and truncated parsable data are explicit errors.
A disappearing FD invalidates that sample's socket selection instead of silently
reporting an empty set. PID exit/reuse, namespace change or a change to the
executable path, device or inode invalidates the run. Re-execution of the same
binary and changes that occur entirely between observations can escape detection.
Interruptions and I/O failures leave partial evidence; a missing summary is an
incomplete capture. Never overwrite or combine such a capture with a later run.

Raw artifacts can include private addresses and deployment details. Keep them in
the controlled investigation's artifact store and link them from the private
report instead of committing machine dumps.

## Run one shared matrix

For each load case, exercise one swarm at a time and then all swarms concurrently.
Include both IPv4 and IPv6 when deployed. Record the exercised family in
`workload.ip_family` as `ipv4`, `ipv6`, or `dual` for a run exercising both;
`null` means missing evidence. Use repeated, order-varied runs with
matched unloaded baselines, warmup and measurement durations. Continue the same
honest traffic in every arm, including a reconnect/new-connection workload.

| Case | Required generator evidence |
| --- | --- |
| Unloaded baseline | Honest offered load, completed work and connection results |
| Malformed/small datagrams | Relevant sizes, packet mix and achieved packet rate |
| Controlled spoofed Initials | Owned test address space, Initial validity and achieved arrival rate |
| Diverse real-address clients completing Retry | Source population, Retry completion, admission outcomes and achieved rate |

`generate.py` ships the two forged-source swarms for the spoofed Initial and
peer-list rows (see below). Honest-traffic drivers and real-address Retry
clients are still external. Pin the external tools and commands in the
manifest, and retain raw per-attempt results. Bound each experiment to the predeclared below-saturation range using
measured ingress, link utilization and generator telemetry. A configured send
rate alone is insufficient. NIC octets also contain honest and unrelated traffic
and omit some wire overhead; they do not alone prove wire-rate headroom.

## Attribute before choosing a remedy

The data sources have different scopes:

| Signal | Scope and interpretation |
| --- | --- |
| Socket `rx_queue_bytes` and `drops` | Kernel queue memory and drops for the selected process's socket inode; drops alone do not identify their cause |
| `Udp` and `Udp6` SNMP counters | Entire network namespace, including unrelated processes; inspect `RcvbufErrors` alongside socket evidence |
| NIC counters and `ethtool -S` | Entire interface; account for unrelated traffic, driver-specific counters and offloads |
| `host_softnet` (`/proc/net/softnet_stat`) | Raw per-CPU receive processing, backlog-drop and `time_squeeze` counters at every sample; interpret the running kernel's layout alongside `netdev_max_backlog`, `netdev_budget` and `netdev_budget_usecs` in host diagnostics |
| `ss` `skmem` diagnostics | Match `ino` to sampled sockets; `rb` describes receive-buffer capacity, distinct from current queued memory |
| Process/host CPU and scheduling counters | Context for CPU pressure; they do not measure endpoint-lock hold/wait time |

See the [kernel SNMP counter documentation](https://docs.kernel.org/networking/snmp_counter.html)
and the [`ss` manual](https://man7.org/linux/man-pages/man8/ss.8.html) for counter and
socket-memory semantics. Preserve raw socket tables. Socket creation/closure and
inode changes require lifetime-aware analysis; do not subtract different sockets.
The summary accumulates NIC/SNMP deltas only across comparable samples. Any
observed reset or missing sample makes the aggregate nonnumeric, even when later
samples recover. Unobserved wrap/reset between samples remains a limitation.
Host diagnostics also record `net.ipv4.udp_rmem_min` alongside the receive-buffer
and UDP memory settings.

`socket_drop_deltas` summarizes drops separately for `udp` and `udp6`, keyed by
inode. Only inodes with unchanged local and remote addresses in every sample are
retained. Creation, disappearance or address changes exclude that inode for the
rest of the run. A failed socket observation makes that family's aggregate
`unavailable`; a counter decrease leaves that inode's result `reset`, even after
recovery. An empty successful mapping means no socket qualified for the entire
sample sequence, not that the process dropped no packets. Queue memory remains a
raw gauge, not a cumulative counter. Unobserved inode reuse between samples is
still possible, so retain the raw tables for lifetime-aware analysis. Incomplete,
interrupted and invalidated captures omit both kinds of summary deltas.

Join these captures with temporary endpoint-lock, driver-work and task-scheduling
instrumentation from the same build and run. Kernel counters and a same-endpoint
versus sibling-endpoint contrast cannot, by themselves, isolate lock contention
from socket/driver effects. Report uncertainty when attribution is incomplete.

Compare receive-buffer tuning, separate ingress/egress endpoints or multiple
sockets only when that attribution supports the candidate. First establish
whether host configuration alone suffices. For each candidate retain honest
throughput, latency and connection results alongside the target bottleneck.
Record effective socket buffers instead of inferring them from sysctl maxima.

Use one report for both advisories. Include an evidence inventory, baseline and
matrix results, missing evidence, threshold outcomes, attribution and the chosen
decision. A justified no-change decision is a valid investigation result, not an
implemented fix. An inconclusive or incomplete experiment remains unqualified.

If a socket-layout change is selected, its follow-up PR must verify connection-ID
routing, loss/reordering, restart, stock-peer interoperability, actual kernel
traffic distribution, explicit resource/concurrency bounds, and affected
established-resource/reconnect/deployment regressions. A failing qualification
must pass with the selected remedy before readiness is claimed. These experiments
do not gate a minimum Retry release that passes its own release checks.

## Generate forged-source swarms

`generate.py` puts forged IPv4 UDP datagrams on the wire from an approved
isolated range. It needs Linux, root (a raw socket with `IP_HDRINCL`) and the
stdlib only. It refuses IPv6 targets, any rate whose wire cost exceeds
`--max-link-fraction` of `--link-mbps`, and a template that is not a plausible
QUIC Initial (at least 1200 bytes, long header, nonzero version). It reports
offered figures only; received figures come from the node.

```sh
sudo python3 -I etc/quic-qualification/generate.py initial \
  --template initial.bin --source-range 198.51.100.0/24 --source-count 4096 \
  --target 192.0.2.10:9000 --rate 20000 --duration 120 --link-mbps 10000 \
  --output run-01-initial.json

sudo python3 -I etc/quic-qualification/generate.py peerlist \
  --sources peers.txt --size 64 \
  --target 192.0.2.10:9000 --rate 20000 --duration 120 --link-mbps 10000 \
  --output run-01-peerlist.json
```

`peers.txt` holds one `ip` or `ip:port` per line (comments with `#`). The
summary records the schedule, offered packets and bytes, the achieved fraction
of the requested rate, send errors by errno name (`EPERM`, `ENOBUFS`, `EAGAIN`
and others), the source population and the template hash. `--dry-run` builds
the first packets and the summary without a socket. Exit codes: 0 complete,
2 preflight or usage failure, 3 interrupted (partial counts kept).

## Measure from the node's metrics

`measure.py` scrapes the node's Prometheus endpoint at a fixed interval and
derives received rates, consensus progress, process CPU and RSS, honest dial
success, handshake time and request latency quantiles from the scrapes. It
needs the node started with `--metrics <addr>` and the stdlib only.

```sh
python3 -I etc/quic-qualification/measure.py \
  --metrics http://127.0.0.1:9101/metrics --duration 180 --interval 1 \
  --generator run-01-initial.json --generator run-01-peerlist.json \
  --output run-01-measurement.json
```

`--from-files <dir>` replays saved `scrape-*.txt` files instead. A failed
scrape is recorded under `gaps`, never as zeros, and a metric absent from every
scrape is listed under `unavailable`. Quantiles use the node's global histogram
buckets and carry the bucket bounds, so a p99.9 that fell into the overflow
bucket is flagged rather than reported as a point. Exit codes: 0 complete,
2 usage failure, 3 fewer than two scrapes or any gap.

## Build the evidence record

`record.py build` joins the manifest, the collector's `metadata.json` and
`summary.json`, the measurement and the generator summaries into one record
with the groups `build`, `topology`, `host_and_traffic`, `method` and
`thresholds`, plus `open_limitations` and the SHA-256 of every input.

```sh
python3 -I etc/quic-qualification/record.py build \
  --manifest run-01.json --collector run-01-collector \
  --measurement run-01-measurement.json \
  --generator run-01-initial.json --generator run-01-peerlist.json \
  --configuration plain --output run-01-evidence.json

python3 -I etc/quic-qualification/record.py capacity \
  --records run-0*-evidence.json --output plain-capacity.json
```

`--configuration` is one of `plain`, `receive_buffer` or `accept_task`. The
effective receive buffer comes from the collector's `ss -uamnep` capture
(`skmem` `rb` on the node's sockets), never from a sysctl alone. Each threshold
in `predeclared_acceptance` is `{value, owner, date}` and is evaluated to
`pass`, `fail` or `unknown` with a reason. `capacity` takes the highest received
rate among the records whose thresholds all pass for one configuration. A record
is `fail` when one threshold fails, `unknown` when none fails and one is unknown,
and the figure reports `null` with a reason when a record is `unknown` or no
record passes. Exit codes: 0 no
limitations, 2 manifest unusable, 3 one or more limitations recorded.

## Run order

Fill `run.example.json`, start `collect.py` and `measure.py`, run the swarms
with `generate.py`, stop the scrapes, then `record.py build` and `record.py
capacity`. The runbook with the full procedure is
`docs/src/network/inbound-capacity-qualification.md`.

## Check the tooling

```sh
python3 -I -m unittest discover -s etc/quic-qualification -p 'test_*.py' -v
```

The fixture tests are deterministic and run without node binaries or load traffic.
Linux additionally exercises procfs/sysfs collection against an owned loopback UDP
socket. The dedicated workflow runs this suite without modifying the Rust lanes.
