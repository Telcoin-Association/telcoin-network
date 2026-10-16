# Host network observability

This page documents the host-level network pressure signals and the committee connectivity gauges that issue #1688 asks for. It covers collection, scope, thresholds, alerts, operator actions and the label policy.

## Scope

The node exports consensus and networking metrics on `--metrics`. It does not see what the kernel or the firewall drops before a packet reaches the QUIC listener. This page adds three host signals and one node signal:

- bounded firewall rule counters from named nft counters;
- UDP receive buffer errors from the kernel SNMP counters;
- conntrack occupancy against the configured maximum, with an explicit mode;
- unique reachable current-committee peers against the committee size, per network.

Every query on the dashboard and in the rules is scoped by `job` and `instance`. Node series add `network`. Interface series add `device`. No label carries an address, a peer identity, a URL or an error string.

## Collection

### node_exporter

Run one node_exporter per host in the host network namespace. The compose profile runs `prom/node-exporter:v1.12.1` with these flags:

```text
--path.rootfs=/host
--collector.textfile.directory=/textfile
--web.listen-address=127.0.0.1:9100
```

Keep the exporter on loopback or on the management network. Do not expose port 9100 to the public internet.

### Textfile collector

`etc/host-metrics/tn-host-network.sh` writes the host series the exporter cannot produce itself. Install the script and its units:

```sh
sudo install -m 0755 etc/host-metrics/tn-host-network.sh /usr/local/bin/tn-host-network.sh
sudo install -m 0644 etc/host-metrics/tn-host-network.service etc/host-metrics/tn-host-network.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tn-host-network.timer
```

The timer runs the collector 30 seconds after boot and every 15 seconds after that. The service writes `tn_host_network.prom.<pid>` under `/var/lib/node_exporter/textfile` and renames it into place, so the exporter never reads a partial file. The script needs root for `nft`. It needs `nft` and `jq` for the firewall series. Without them it reports `tn_host_firewall_collector_available 0` and no firewall series.

Name the nft counters you want to observe with the `tn_` prefix. Use `tn_drop_` for drop rules and `tn_accept_` for accept rules:

```text
nft add counter inet filter tn_drop_public_udp
nft add rule inet filter input udp dport 49590 ip saddr != @validators counter name tn_drop_public_udp drop
```

### Compose profile

`etc/compose.yaml` has an `observability` profile with node_exporter, Prometheus (`prom/prometheus:v3.15.0`) and Grafana (`grafana/grafana:13.2.4`). The profile is off by default, so `make up` is unchanged. Start it with:

```sh
GF_SECURITY_ADMIN_PASSWORD=change-me docker compose -f etc/compose.yaml --profile observability up -d
```

All three services bind to loopback: Prometheus on 9090, Grafana on 3000 and node_exporter on 9100. Grafana has anonymous access off. The admin password comes from `GF_SECURITY_ADMIN_PASSWORD`; the default `telcoin-local` is for a local stack only. Each validator binds `--metrics` to its fixed bridge address `10.10.0.2N:9101` so the host Prometheus can scrape it. The port is not published.

### Prometheus jobs

| Job | Targets | Instance label | Series |
| --- | --- | --- | --- |
| `telcoin-node` | `10.10.0.21` to `10.10.0.24`, port 9101 | `validator1` to `validator4` | `tn_*` node series |
| `node` | `127.0.0.1:9100` | the host | `node_*`, `up` and the `tn_host_*` textfile series |

Rules load from `etc/prometheus/rules/*.rules.yaml`. The scrape and evaluation interval is 15 seconds.

## Series reference

| Series | Source | Scope | Reset behaviour |
| --- | --- | --- | --- |
| `tn_host_firewall_rule_packets_total{rule}` | nft named counter | host, per rule | resets on reboot or `nft reset counters`; use `rate` or `increase` |
| `tn_host_firewall_rule_bytes_total{rule}` | nft named counter | host, per rule | same as packets |
| `tn_host_firewall_rules_truncated` | collector | host | gauge; 1 when more than 32 `tn_` counters exist |
| `tn_host_firewall_collector_available` | collector | host | gauge; 0 when nft or jq is missing |
| `tn_host_conntrack_mode{mode}` | collector | host | one-hot gauge: `tracked`, `notrack` or `unsupported` |
| `tn_host_udp_rmem_max_bytes` | `/proc/sys/net/core/rmem_max` | host | gauge |
| `tn_host_network_collector_info{version}` | collector | host | gauge; collector version |
| `node_netstat_Udp_RcvbufErrors`, `node_netstat_Udp6_RcvbufErrors` | kernel SNMP | host | resets on reboot; use `rate` |
| `node_nf_conntrack_entries`, `node_nf_conntrack_entries_limit` | node_exporter conntrack collector | host | gauges; absent when the module is not loaded |
| `node_network_receive_drop_total{device}`, `node_network_receive_errs_total{device}` | node_exporter netdev collector | host, per interface | resets on reboot; use `rate` |
| `node_textfile_mtime_seconds{file}` | node_exporter textfile collector | host | gauge; collector freshness |
| `up{job="node"}` | Prometheus | host | 0 when the exporter does not answer |
| `tn_network_committee_size{network}` | node | node, per network | gauge; 0 until the first committee is known |
| `tn_network_committee_peers_expected{network}` | node | node, per network | gauge; 0 until the first committee is known |
| `tn_network_committee_peers_connected{network}` | node | node, per network | gauge; 0 until the first committee is known |

Recording rules keep the baselines operators use for threshold selection:

| Recording rule | Meaning |
| --- | --- |
| `tn_host:firewall_drop_packets:rate5m` | packets per second across all `drop_*` counters, per host |
| `tn_host:udp_rcvbuf_errors:rate5m` | UDP receive buffer errors per second, IPv4 and IPv6, per host |
| `tn_host:conntrack_occupancy:ratio` | entries divided by the limit, only while the host tracks connections |
| `tn_network:committee_coverage:ratio` | connected divided by expected committee peers, only while a committee is known |

## Scope caveats

### Counter reset

nft counters and the kernel SNMP counters reset on reboot. nft counters also reset on `nft reset counters`. All rules and panels use `rate` or `increase`. Both treat a drop in a counter as a reset and do not alert on the reset alone. The unit test file `telcoin-host-network.test.yaml` proves it with a counter that resets in the middle of the window.

### Network namespace

The collector and node_exporter read the host network namespace. A container that runs them in its own namespace reports the container, not the host. The compose profile uses `network_mode: host` and `pid: host` for this reason. A node in a container still shares the host firewall and conntrack table, so the host series describe the pressure the node sees.

### Interface

Interface drops and errors are per device. The `device` variable of the dashboard selects the interface. Pick the interface that carries consensus traffic. The firewall, UDP and conntrack series are host wide and have no interface label.

### Exporter scope

One exporter observes one host. Several validators on one host share the same host series under one `instance` of the `node` job. The node series keep their own `instance` under the `telcoin-node` job. Nothing merges across instances: every rule keeps `instance` in its result and every panel legend shows it.

### Explicit states

A missing signal is never a healthy zero. The state panel of the dashboard shows one of `exporter-down`, `collector-absent`, `stale` or `ok` for the collector, and one of `tracked`, `notrack`, `unsupported` or `collector-failed` for conntrack. An empty drop panel next to a `collector-absent` state means no data, not no drops.

- `notrack`: the ruleset contains a `notrack` statement. Flows that match it do not enter the conntrack table, so occupancy does not describe them. `TnHostConntrackHigh` is suppressed while the mode is `notrack`.
- `unsupported`: `/proc/sys/net/netfilter/nf_conntrack_max` is absent. The module is not loaded or the kernel has no conntrack. `TnHostConntrackUnsupported` fires after 15 minutes so the state is reviewed, not ignored.
- The collector cannot detect `notrack` without `nft`. A host that uses iptables `NOTRACK` targets shows `tracked`. Document the exception for such hosts.

## Committee connectivity semantics

The node sets three gauges per network (`primary` and `worker-{id}`):

- `tn_network_committee_size`: the number of members of the current committee.
- `tn_network_committee_peers_expected`: `size - 1` when this node is a current member, else `size`. Every network is a full mesh between committee members, so a member expects every other member and an observer expects all of them.
- `tn_network_committee_peers_connected`: the number of unique current-committee members with at least one established connection on this network.

All three are 0 until the node learns its first committee. The rules use `expected > 0` as the restart guard, so a node that boots fires nothing until the committee is known. At an epoch change the previous committee is not counted: a member that leaves the committee drops out of `connected` without lowering coverage.

The generic `tn_network_connected_peers` and `tn_network_known_peers` gauges count every peer. The dashboard overlays `connected_peers` on the committee panel only to show that committee coverage moves on its own. The unit test has `connected_peers` rising while committee coverage falls, and the alerts still fire.

## Alerts

Every alert has a `severity` label (`info`, `warning` or `critical`), a `summary`, a `description`, an `action` and a `runbook_url` that points at a section of this page. Every expression carries a `job` matcher.

The thresholds below are launch defaults. To re-derive one, record the matching recording rule for a quiet launch window of at least one week, take the 99th percentile of that baseline, and add a margin of at least 50 percent. Keep the `for` duration at or above two scrape intervals so a single scrape cannot fire an alert.

### Exporter and collector state

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| `TnHostExporterDown` | `up{job="node"} == 0` for 2 minutes | critical | Restart node_exporter. Check the loopback bind and the scrape target. |
| `TnHostCollectorAbsent` | exporter up without `tn_host_network.prom` for 10 minutes | warning | Install the collector script and enable the timer. Check the textfile directory flag. |
| `TnHostCollectorStale` | file older than 120 seconds for 5 minutes | warning | Read `journalctl -u tn-host-network.service`. Fix nft, jq or permissions. |

### Firewall drops

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| `TnHostFirewallDropsRising` | `increase` of a `drop_*` counter over 5 minutes above 100, for 5 minutes | warning | Compare with the QUIC Retry and inbound request panels. A rise that matches committee loss means a blocked peer. A rise with stable committee coverage is a scan or flood the firewall absorbs. |

Launch default: 100 packets per 5 minutes. Re-derive it from `tn_host:firewall_drop_packets:rate5m` multiplied by 300.

### UDP receive buffer errors

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| `TnHostUdpRcvbufErrors` | `increase` of `RcvbufErrors` over 5 minutes above 0, for 10 minutes | warning | Compare `tn_host_udp_rmem_max_bytes` with the buffer the node requests. Raise `net.core.rmem_max` and `net.core.rmem_default`. Check CPU starvation of the node. |

Launch default: any sustained error. A host that shows a constant low rate at launch gets its own threshold from `tn_host:udp_rcvbuf_errors:rate5m`.

### Conntrack occupancy

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| `TnHostConntrackHigh` | entries divided by limit above 0.8 for 5 minutes while the mode is `tracked` | warning | Raise `net.netfilter.nf_conntrack_max`, add a `notrack` rule for the consensus UDP ports, or block the flow churn at the edge. |
| `TnHostConntrackUnsupported` | mode `unsupported`, or the exporter's conntrack collector fails, for 15 minutes | info | Load `nf_conntrack` or enable the collector. If the host runs without conntrack on purpose, document it and silence the alert for that instance. |

Launch default: 0.8. Re-derive it from `tn_host:conntrack_occupancy:ratio`.

### Committee connectivity

| Alert | Condition | Severity | Action |
| --- | --- | --- | --- |
| `TnCommitteeConnectivityDegraded` | expected above 0 and connected below expected, for 5 minutes | warning | Find the missing members from their own gauges. Check firewall drops and QUIC Retry on both sides. Confirm the address manifest. |
| `TnCommitteeConnectivityLost` | expected above 0 and connected below two thirds of expected, for 2 minutes | critical | Treat as an incident. Check host drops, conntrack and UDP errors on this host. Confirm the advertised addresses. Fail over to a sentry path if the private network is impaired. |

Launch default: two thirds. Re-derive it from `tn_network:committee_coverage:ratio`. A committee of four tolerates one missing member before quorum is at risk.

## Controlled scenarios

Run these on a staging host to confirm the signals before launch. The `host-scenarios` job of `.github/workflows/host-network-observability.yaml` runs the first four on every pull request that touches the artifacts, then checks the collector output with `promtool check metrics`.

| Scenario | Procedure | Expected observation | CI |
| --- | --- | --- | --- |
| Firewall drops | Add a `tn_drop_*` counter and a drop rule for a probe port. Send 50 datagrams to it. Run the collector. | `tn_host_firewall_rule_packets_total{rule="drop_<name>"}` is at least 50. `TnHostFirewallDropsRising` fires above the threshold. | yes |
| notrack | Insert a `notrack` rule before the drop rule. Run the collector. | `tn_host_conntrack_mode{mode="notrack"}` is 1. `TnHostConntrackHigh` is suppressed. | yes |
| UDP receive buffer pressure | Set `net.core.rmem_max` and `net.core.rmem_default` to 2304. Start a UDP listener that does not read. Send 300 datagrams of 1000 bytes. | The `RcvbufErrors` column of `/proc/net/snmp` increases. `TnHostUdpRcvbufErrors` fires after 10 minutes. | yes |
| Conntrack pressure | Set `net.netfilter.nf_conntrack_max` to 64. Open more than 64 UDP flows. | `nf_conntrack_count` reaches 80 percent of the limit. `TnHostConntrackHigh` fires while the mode is `tracked`. | yes |
| Committee loss and recovery | Block one committee member's address on the primary port. Wait. Unblock it. | `tn_network_committee_peers_connected` falls by one while `tn_network_connected_peers` is unchanged or rising. `TnCommitteeConnectivityDegraded` fires after 5 minutes and clears after the unblock. | no (needs a committee) |

The unit test file `etc/prometheus/rules/telcoin-host-network.test.yaml` covers the rule side of every scenario: a counter reset, an absent exporter, several instances without merging, a `notrack` host, an `unsupported` host, a restart with zero gauges, and committee loss with recovery.

## Label vocabulary policy

- Host series carry only `job`, `instance`, `rule`, `mode`, `device`, `file`, `collector` and `version`. Node committee series carry only `network`.
- `rule` values come from nft counter names with the `tn_` prefix. The collector sorts them and keeps the first 32. `tn_host_firewall_rules_truncated` is 1 when it cut the list. An attacker cannot create nft counters, so the operator bounds the vocabulary.
- `mode` has three values. `network` has `primary` and `worker-{id}`.
- Committee connectivity is a count. There is no per-peer label, not even for committee members. A per-peer breakdown needs a bounded lifecycle for the label values and is out of scope here.
- No label carries a source address, a peer identity, a URL or an error text. The artifact test in `crates/network-libp2p/src/tests/ops_artifacts_tests.rs` rejects any dashboard query that matches on `addr`, `address`, `ip`, `src`, `peer_id` or `url`, and any node or host query without `job` and `instance` matchers.

## Related

- Issue #1672 (gateway dashboards) and issue #930: the scoping and label policy on this page applies to the work tracked there.
- [Validator production operations](validator-operations.md): firewall configuration and capacity monitoring.
