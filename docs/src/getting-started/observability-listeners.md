# Private observability listeners

The node has two operator listeners: health (`--healthcheck`) and metrics (`--metrics`).
Both are off unless configured.
Both bind only the address the operator writes.
Both log one `operator listener bound` line at startup, under the `tn::listener` target, with three fields: `listener` (`healthcheck` or `metrics`), `listen_on` (the bound socket address) and `exposure` (how far it reaches).
An `exposure=all-interfaces` bind is also logged at `warn`.

The listeners are unauthenticated and have no rate limiting.
Reach is the control: a loopback bind is reachable only from the host, and a private interface bind only from hosts that route to that interface.
A host firewall is defense in depth, not the binding contract.

## Bind forms

| Value | Health bind | Exposure |
| --- | --- | --- |
| `8080` | `127.0.0.1:8080` | `loopback` |
| `127.0.0.1:8080` | `127.0.0.1:8080` | `loopback` |
| `[::1]:8080` | `[::1]:8080` | `loopback` (IPv6) |
| `10.10.0.21:8080` | `10.10.0.21:8080` | `interface` |
| `0.0.0.0:8080` | every IPv4 interface | `all-interfaces` (explicit, warned) |
| `[::]:8080` | every interface | `all-interfaces` (explicit, warned) |

`--metrics` takes the socket address forms only; that is unchanged.
A bare port is accepted for `--healthcheck` only, and it binds IPv4 loopback.
A wildcard bind is never implied; it has to be written out.

## Migration from the port-only flag

| Case | Before | After |
| --- | --- | --- |
| `--healthcheck 8080` | bound `0.0.0.0:8080` | binds `127.0.0.1:8080` |
| `HEALTHCHECK_TCP_PORT=8080` | bound `0.0.0.0:8080` | binds `127.0.0.1:8080` |
| Probe from inside the same container or host | reached the listener | unchanged |
| Probe from another host, or a kubelet probe | reached the wildcard listener | refused; write `--healthcheck 0.0.0.0:8080` or the node's private address |
| Flag unset | no listener | unchanged |
| Invalid value (`127.0.0.1`, `nope`, `70000`) | clap error | clap error, exit code 2, nothing starts |
| Port in use, or address not on this host | listener silently skipped | startup fails with the OS error naming the address |

Routes and readiness semantics are unchanged: liveness on every other path, `/health/workers` for transaction acceptance, and `/health/network` for cached swarm reachability (`503` while not-ready).

## Native deployment

Bind both listeners to loopback and run the collector on the same host:

```bash
telcoin-network node --chain mainnet --healthcheck 127.0.0.1:8080 --metrics 127.0.0.1:9101
```

Prometheus on the same host:

```yaml
scrape_configs:
  - job_name: telcoin-node
    static_configs:
      - targets: ["127.0.0.1:9101"]
```

Health from the same host:

```bash
curl -sS http://127.0.0.1:8080/health/network
```

A remote collector reaches a loopback listener through an SSH tunnel (`ssh -N -L 9101:127.0.0.1:9101 node-host`), or through a local agent that relays, such as Prometheus in agent mode with `remote_write`.

A host with a separate management network can bind the listeners to that interface instead: `--metrics 10.0.0.5:9101 --healthcheck 10.0.0.5:8080`.
Allow only the collector's address in the host firewall.
The bind is the contract; the firewall is defense in depth.

## Container deployment

`etc/compose.yaml` publishes no health or metrics listener under `ports:`: each binds its container's compose-network address, or loopback inside the container.
`node-record-api` is the exception, with no listener of its own for health: its probes share its API listener, which is published on host loopback.

| Service | Bridge address | Health | Metrics |
| --- | --- | --- | --- |
| `validator1`..`validator4` | `10.10.0.21`-`10.10.0.24` | `--healthcheck 10.10.0.2N:8080` | `--metrics 10.10.0.2N:9101` |
| `observer` | `10.10.0.31` | `--healthcheck 10.10.0.31:8080` | `--metrics 10.10.0.31:9101` |
| `worker-gateway1`..`worker-gateway4` | `10.10.0.41`-`10.10.0.44` | `--probe-addr 127.0.0.1:8546`, inside the container | `--metrics 10.10.0.4N:9101` |
| `node-record-api` | `10.10.0.51` | `/healthz` and `/readyz` on its API listener, published on `127.0.0.1:8090` | `10.10.0.51:9101` |

The compose `healthcheck:` probes `/health/network` on each validator and on the observer, `/ready/any` on each gateway's probe listener, and `/readyz` on `node-record-api`, each from inside its container.

The `observability` profile of `etc/compose.yaml` runs Prometheus in the host network namespace, bound to `127.0.0.1:9090`, so on a Linux host it scrapes the bridge addresses without any published metrics port.

```bash
docker compose -f etc/compose.yaml --profile observability up --build --remove-orphans --detach
```

[Host network observability](host-network-observability.md#compose-profile) lists its targets and dashboards.

Manual health query from inside a validator container (the image has no curl):

```bash
docker compose -f etc/compose.yaml exec validator1 bash -c 'exec 3<>/dev/tcp/10.10.0.21/8080; printf "GET /health/network HTTP/1.1\r\n\r\n" >&3; head -c 200 <&3'
```

Single-container deployments keep loopback binds and run the collector as a sidecar that shares the network namespace (`network_mode: "service:<node>"`).

## Port publication review

- Never add `- 9101:9101` or `- 8080:8080` under `ports:`. A published port on `0.0.0.0` republishes the listener on every host interface.
- Publish to host loopback (`127.0.0.1:9101:9101`) when a host-local tool needs the listener.
- `--http.addr 0.0.0.0` is the RPC listener. It is governed separately and is not covered here.
- In `etc/compose.yaml` only the gateways' client ports (8545-8542) are published on every interface.
  The validators' RPC is published as debug ports on host loopback only (`127.0.0.1:18545:8545` to `127.0.0.1:18542:8545`), and so are the observer's RPC (`127.0.0.1:8541:8545`) and the `node-record-api` API (`127.0.0.1:8090:8080`).
- A wildcard `--metrics 0.0.0.0:9101` or `--healthcheck 0.0.0.0:8080` is an explicit choice. It shows as `exposure=all-interfaces` with a warning.

## Gateway exposure

A worker gateway has three listeners: the client port (`--listen-addr`), the probe listener (`--probe-addr`) and metrics (`--metrics`).
Only the client port is meant for clients; the probe and metrics listeners are unauthenticated, and the probe listener has no connection cap.
In `etc/compose.yaml` each gateway binds its client port to `0.0.0.0:8545`, published on host port 8545, 8544, 8543 or 8542 on every interface.
It binds its probe listener to `127.0.0.1:8546` inside its container, where only the compose healthcheck reaches it, and its metrics to its bridge address, `10.10.0.4N:9101`, unpublished.
`node-record-api` binds its metrics to `10.10.0.51:9101`, unpublished, and publishes its API on host loopback only.
For production, `bin/worker-gateway/README.md` ("Firewalling") keeps the gateway's metrics port inside the monitoring network and its probe port reachable from the orchestrator only.

## Metric label policy

- No per-address labels: no IP, socket, multiaddr or hostname is ever a label value.
- Per-peer labels only for committee or trusted peers, with every other peer under one shared value, so the series count stays bounded by the peers that were in the set since the node started.
- Raw errors, URLs and attacker-supplied identities are never label values.
- Every label vocabulary is bounded: a static string, a Rust enum with fixed arms, a small worker id, or the BLS key of a peer the previous rule allows.

### Audit of the tree at this change

Every labelled series in the twelve metrics modules was classified at the base of this change.

| Series (label keys) | Value source | Class |
| --- | --- | --- |
| `tn_epoch_runs_total` (`mode`) | `RunEpochMode`, 3 arms | enum |
| `tn_epoch_provider_fault_retries_total` (`read`) | `&'static str` literals | static |
| `tn_info` (`version`) | `&'static str` build version | static |
| `tn_node_mode` (`mode`) | literal array, 3 values | static |
| `tn_batch_builder_*` (`worker`, `reason`) | `WorkerId`; `BlockSealError`, 8 arms | worker id, enum |
| `tn_worker_*` (`worker`, `reason`, `source`, `tier`, `peer`) | `WorkerId`; `QuorumWaiterError` 6 arms (`Rpc` payload dropped), `ForwardDropReason`, `SyncShedReason`; `local` / `remote`; `PeerTier`, 2 arms; `peer` on the two batch request counters only: a current committee member's BLS key, else `other` | worker id, enum, static, committee key |
| `tn_reth_*` (`reason`, `call`) | `InvalidTxSkipReason` 5 arms, `HandoffDropReason` 7 arms, `ForwardDropReason` 5 arms; `&'static str` literals | enum, static |
| `tn_network_*` (`network`, `kind`, `class`, `outcome`, `reason`, `direction`, `severity`) | `primary` or `worker-{id}`; `ServiceClass` 6 arms, `InboundFailureOutcome` 5 arms, `ConnectionLimitReason` 7 arms, `InboundDenial` 4 arms, `ShedReason` 3 arms; literals | worker id, enum, static |
| `tn_storage_write_failures_total` (`env`, `op`) | `&'static str` env constants; `WriteOp`, 6 arms | static, enum |
| reth passthrough (`reth_*`) | reth's own labels, copied unchanged | reth |

Result: 0 violations.
No label carries a libp2p peer id, hostname, IP, socket, multiaddr, URL, raw error string or remote-supplied topic.
One label carries a BLS key: `peer` on `tn_worker_batch_requests_denied_total` and `tn_worker_batch_requests_failed_total`.
Its value is the key of a current committee member, and every other peer records under the single value `other`.
The worker's committee set is replaced when each epoch starts and is empty until the first one starts, so a request denied or failed before then records as `other`.
The worker caches counters for at most the committee plus `other`, because it drops the members that left the committee before it adds a new entry.
The exported series are not cleaned up: the recorder sets no idle timeout, so a departed member's series stays at its last value until the node restarts.
Each of the two counters therefore has, per worker, at most one series for each committee member seen since startup, plus `other`.
The only error `Display` text that reaches label code (`tn_network_connection_limit_rejections_total`) is matched against fixed strings, and only the bounded enum becomes the label.

To add a metric: derive the label value from an enum `as_str` that returns `&'static str`, register the series at zero where the owning crate registers its handles, and never format a remote-supplied value into a label.
