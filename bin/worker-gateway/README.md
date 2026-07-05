# worker-gateway

A stateless reverse proxy that fronts a Telcoin Network worker's JSON-RPC
endpoint. It forwards the full JSON-RPC method surface (`eth_*` / `net_*` /
`web3_*` / `tn_*`) unchanged to a ready upstream worker, gates traffic on a
polled per-worker readiness signal, and exposes its own liveness and readiness
endpoints so an orchestrator can route around it. "Unchanged" applies to the
request method, JSON-RPC body, and content type; the header contract is
deliberately minimal (see Scope).

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
- Header forwarding is minimal. Upstream gets the request method, body, and
  `Content-Type`, plus `X-Forwarded-For` / `X-Forwarded-Proto` (real client
  identity) and the `X-TN-Gateway` hop marker (loop protection). The client
  gets the upstream status, body, and `Content-Type`. All other headers are
  dropped in both directions; in particular CORS is not terminated here, so
  browser dApps need CORS handled at the ingress (or a later PR).
- The request path and query string are not forwarded: every request goes to
  the configured upstream base URL (JSON-RPC carries its method in the body,
  so `POST /` is the whole HTTP surface).

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
  --listen-addr 0.0.0.0:8080 \
  --upstream-rpc-url http://127.0.0.1:8545 \
  --upstream-readiness-url http://127.0.0.1:8551/health/workers \
  --worker-id 0
```

The default listen port (`8545`) deliberately matches the worker's default RPC
port so the gateway is a drop-in edge for clients; on a single host that means
`--listen-addr` must be set (as above). An upstream URL that points back at
the gateway's own listen address is rejected at startup, so defaults plus a
loopback upstream fail fast instead of looping.

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
| `--header-read-timeout` | `WORKER_GATEWAY_HEADER_READ_TIMEOUT` | `10s` | Inbound header read deadline (slow-loris guard). |
| `--max-connections` | `WORKER_GATEWAY_MAX_CONNECTIONS` | `500` | Concurrent inbound connection cap. |
| `--graceful-shutdown-timeout` | `WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Drain deadline on SIGTERM. |
| `--log-filter` | `RUST_LOG` | `info` | Tracing filter directive. |

Durations use `humantime` syntax (`5s`, `2m`, `500ms`).

## Connection handling

Every inbound connection is served with a header read deadline
(`--header-read-timeout`), `TCP_NODELAY`, and a global concurrency cap
(`--max-connections`; further connections wait in the OS accept backlog).
Each request additionally has a whole-request deadline of
`--upstream-request-timeout` + `--header-read-timeout` covering the body read
and the upstream response headers, so a request body trickled in below the
size limit cannot hold a slot indefinitely.

Upstream response bodies are streamed through, never buffered whole, so
response size does not translate into gateway memory; a stalled stream is
bounded by the upstream request timeout.

Every forwarded request carries the `X-TN-Gateway` hop marker, and an inbound
request that already carries it is rejected (HTTP `508`), so a misconfigured
upstream or VIP that points back at a gateway breaks the loop at the first
revisit instead of exhausting file descriptors.

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
| Request body too large | `413` | `-32003` |
| Proxy loop detected | `508` | `-32004` |
| Request deadline exceeded | `408` | `-32005` |
| Request body unreadable (client aborted) | `400` | `-32600` |

The gateway's own codes sit in the JSON-RPC server-error range
(`-32000..=-32099`), which upstream servers also use for their errors;
disambiguate by HTTP status and message, not by code alone (`-32600` is the
spec's standard "Invalid Request" code).

## Graceful shutdown

On SIGTERM (or ctrl-c) the gateway stops accepting new connections and drains
in-flight requests, up to `--graceful-shutdown-timeout`. Requests still running
after the deadline are force-closed.
