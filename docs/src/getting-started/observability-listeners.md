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

`etc/compose.yaml` binds each validator's listeners to its compose-network address (`--healthcheck 10.10.0.21:8080 --metrics 10.10.0.21:9101` for validator1, and so on) and publishes no health or metrics `ports:`.
The compose `healthcheck:` on each validator probes `/health/network` from inside the container.

`etc/compose.observability.yaml` adds a Prometheus service on the same network that scrapes the four validators.
Its UI is published on host loopback only (`127.0.0.1:9090:9090`).

```bash
cd etc
docker compose -f compose.yaml -f compose.observability.yaml up
```

Manual health query from inside a validator container (the image has no curl):

```bash
docker compose exec validator1 bash -c 'exec 3<>/dev/tcp/10.10.0.21/8080; printf "GET /health/network HTTP/1.1\r\n\r\n" >&3; head -c 200 <&3'
```

Single-container deployments keep loopback binds and run the collector as a sidecar that shares the network namespace (`network_mode: "service:<node>"`).

## Port publication review

- Never add `- 9101:9101` or `- 8080:8080` under `ports:`. A published port on `0.0.0.0` republishes the listener on every host interface.
- Publish to host loopback (`127.0.0.1:9101:9101`) when a host-local tool needs the listener.
- `--http.addr 0.0.0.0` is the RPC listener. It is governed separately and is not covered here.
- A wildcard `--metrics 0.0.0.0:9101` or `--healthcheck 0.0.0.0:8080` is an explicit choice. It shows as `exposure=all-interfaces` with a warning.

## Gateway exposure

The gateway service and its metrics-port exposure are tracked in issue #1672.
This page covers the node's own listeners only.

## Metric label policy

- No per-address labels: no IP, socket, multiaddr or hostname is ever a label value.
- Per-peer labels only for committee or trusted peers, with a bounded lifecycle and cleanup when the peer leaves the set.
- Raw errors, URLs and attacker-supplied identities are never label values.
- Every label vocabulary is bounded: a static string, a Rust enum with fixed arms, or a small worker id.

### Audit of the tree at this change

Every labelled series in the twelve metrics modules was classified at the base of this change.

| Series (label keys) | Value source | Class |
| --- | --- | --- |
| `tn_epoch_runs_total` (`mode`) | `RunEpochMode`, 3 arms | enum |
| `tn_epoch_provider_fault_retries_total` (`read`) | `&'static str` literals | static |
| `tn_info` (`version`) | `&'static str` build version | static |
| `tn_node_mode` (`mode`) | literal array, 3 values | static |
| `tn_batch_builder_*` (`worker`, `reason`) | `WorkerId`; `BlockSealError`, 8 arms | worker id, enum |
| `tn_worker_*` (`worker`, `reason`, `source`) | `WorkerId`; `QuorumWaiterError` 6 arms (`Rpc` payload dropped), `ForwardDropReason`, `SyncShedReason`; `local` / `remote` | worker id, enum, static |
| `tn_reth_*` (`reason`, `call`) | `InvalidTxSkipReason` 5 arms, `HandoffDropReason` 7 arms, `ForwardDropReason` 5 arms; `&'static str` literals | enum, static |
| `tn_network_*` (`network`, `kind`, `class`, `outcome`, `reason`, `direction`, `severity`) | `primary` or `worker-{id}`; `ServiceClass` 6 arms, `InboundFailureOutcome` 5 arms, `ConnectionLimitReason` 7 arms, `InboundDenial` 4 arms, `ShedReason` 3 arms; literals | worker id, enum, static |
| `tn_storage_write_failures_total` (`env`, `op`) | `&'static str` env constants; `WriteOp`, 6 arms | static, enum |
| reth passthrough (`reth_*`) | reth's own labels, copied unchanged | reth |

Result: 0 violations.
No label carries a peer id, BLS key, hostname, IP, socket, multiaddr, URL, raw error string or remote-supplied topic.
No per-peer labels exist, so no cleanup on committee rotation is needed.
The only error `Display` text that reaches label code (`tn_network_connection_limit_rejections_total`) is matched against fixed strings, and only the bounded enum becomes the label.

To add a metric: derive the label value from an enum `as_str` that returns `&'static str`, register the series at zero where the owning crate registers its handles, and never format a remote-supplied value into a label.
