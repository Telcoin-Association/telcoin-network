# worker-gateway

A stateless reverse proxy that fronts a Telcoin Network worker's JSON-RPC
endpoint. It forwards the full JSON-RPC surface (`eth_*` / `net_*` / `web3_*` /
`tn_*`) verbatim to a ready upstream worker, gates traffic on a polled
per-worker readiness signal, and exposes its own liveness and readiness
endpoints so an orchestrator can route around it.

Because every instance is stateless and identical, the gateway can be scaled
horizontally: any replica can serve any request. This is PR2 of the epic
(issue #712) and implements the crate skeleton plus the proxy core. Edge
protections (rate/size limits, `eth_sendRawTransaction` sanity checks) and
observability/deployment (Prometheus, Dockerfile, k8s/HPA) land in PR3 and PR4.

## Scope (v1)

- HTTP-only. WebSocket (`eth_subscribe`) pass-through is deliberately out of
  scope: subscriptions are per-connection stateful and cannot survive a replica
  dying, which breaks the stateless-scaling invariant. Point subscription
  clients at a worker's WS endpoint behind your own ingress.
- Static upstream configuration (no hot reload, no dynamic discovery).
- Forwards to the single ready worker (`worker_id` `0`). The config models a
  worker list so the method-aware routing follow-up can select among several
  without a config change; v1 implements none of that selection.
- TLS termination and auth/API keys are out of scope; run the gateway behind
  your own ingress/mTLS.

## Readiness contract

The gateway polls each upstream node's readiness endpoint
(`GET /health/workers`, added in PR1) and expects the versioned envelope:

```json
{
  "version": 1,
  "workers": [
    { "worker_id": 0, "accepting_transactions": true }
  ]
}
```

A worker is considered ready only when its entry reports
`accepting_transactions: true`. Every other outcome, an unreachable node, a
timed-out poll, a malformed payload, or the worker missing from the list, marks
the upstream **not-ready**, so the gateway fails closed. Unknown fields and
newer envelope versions are tolerated (forward compatible).

## Configuration

Configure the upstream list either inline (single upstream) or via a YAML file.

Inline:

```
worker-gateway \
  --upstream-rpc-url http://127.0.0.1:8545 \
  --upstream-readiness-url http://127.0.0.1:8551/health/workers \
  --worker-id 0
```

YAML (`--config gateway.yaml`):

```yaml
upstreams:
  - worker_id: 0
    rpc_url: "http://127.0.0.1:8545"
    readiness_url: "http://127.0.0.1:8551/health/workers"
```

### Flags and environment variables

Every flag has an environment-variable fallback.

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--listen-addr` | `WORKER_GATEWAY_LISTEN_ADDR` | `0.0.0.0:8545` | Client JSON-RPC + `/health` + `/ready`. |
| `--config` | `WORKER_GATEWAY_CONFIG` | (none) | YAML upstream list. |
| `--upstream-rpc-url` | `WORKER_GATEWAY_UPSTREAM_RPC_URL` | (none) | Inline upstream JSON-RPC URL. |
| `--upstream-readiness-url` | `WORKER_GATEWAY_UPSTREAM_READINESS_URL` | (none) | Inline upstream readiness URL. |
| `--worker-id` | `WORKER_GATEWAY_WORKER_ID` | `0` | Inline upstream worker id. |
| `--readiness-poll-interval` | `WORKER_GATEWAY_READINESS_POLL_INTERVAL` | `5s` | Readiness poll cadence. |
| `--readiness-poll-timeout` | `WORKER_GATEWAY_READINESS_POLL_TIMEOUT` | `2s` | Per-poll timeout. |
| `--upstream-connect-timeout` | `WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT` | `2s` | Upstream connect timeout. |
| `--upstream-request-timeout` | `WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT` | `30s` | Upstream per-request deadline. |
| `--graceful-shutdown-timeout` | `GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Drain deadline on SIGTERM. |
| `--log-filter` | `RUST_LOG` | `info` | Tracing filter directive. |

Durations use `humantime` syntax (`5s`, `2m`, `500ms`).

## Gateway endpoints

- `GET /health`: liveness, always `200 OK` while the process runs.
- `GET /ready`: readiness, `200` when at least one upstream is ready, else
  `503` with `{"ready": false}`.
- everything else (i.e. `POST /`): forwarded to a ready upstream worker.

## Behaviour on failure

Client requests always receive a well-formed JSON-RPC 2.0 error (never a bare
connection reset) when the gateway cannot serve them. The request `id` is
echoed when it can be recovered.

| Condition | HTTP | JSON-RPC error code |
| --- | --- | --- |
| No upstream ready | `503` | `-32000` |
| Upstream unreachable | `502` | `-32001` |
| Upstream request timed out | `504` | `-32002` |

## Graceful shutdown

On SIGTERM (or ctrl-c) the gateway stops accepting new connections and drains
in-flight requests, up to `--graceful-shutdown-timeout`. Requests still running
after the deadline are force-closed.
