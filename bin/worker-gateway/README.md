# worker-gateway

A stateless reverse proxy that fronts a Telcoin Network worker's JSON-RPC endpoint.
It forwards JSON-RPC calls (`eth_*` / `net_*` / `web3_*` / `tn_*`) unchanged to a ready upstream worker, gates traffic on a polled per-worker readiness signal, and exposes its own liveness and readiness endpoints so an orchestrator can route around it.
With `--redirect-queries` it sends only transaction submissions to the worker and every other call to a public RPC (see [Query redirect](#query-redirect)); a validator's gateways should always run that way (see [Operator guidance](#operator-guidance)).
"Unchanged" applies to the request method, JSON-RPC body, and content type; the header contract is deliberately minimal (see Scope).

Because every instance is stateless and identical, the gateway can be scaled
horizontally: any replica can serve any request. This is PR4 of the epic
(issue #712): it adds observability and deployment (a Prometheus `/metrics`
endpoint, a container image, and reference Kubernetes manifests including an
autoscaler keyed on the in-flight-request gauge) on top of the PR2 proxy core
and the PR3 edge protections.

## Scope (v1)

The [production-readiness review](docs/production-readiness.md) evaluates this gateway as the public endpoint of a validator, lists its findings by severity, and gives the plan for each one.

- HTTP-only. WebSocket (`eth_subscribe`) pass-through is deliberately out of
  scope: subscriptions are per-connection stateful and cannot survive a replica
  dying, which breaks the stateless-scaling invariant. Point subscription
  clients at a worker's WS endpoint behind your own ingress.
- Static upstream configuration (no hot reload, no dynamic discovery).
- Calls for the worker go to the first ready worker in configuration order; there is no load balancing across workers.
  With `--redirect-queries`, only `eth_sendRawTransaction` and `eth_sendRawTransactionSync` go to the worker and every other call goes to the query URL (see [Query redirect](#query-redirect)).
- TLS termination and auth/API keys are out of scope; run the gateway behind
  your own ingress/mTLS.
- Header forwarding is minimal. Upstream gets the request method, body, and
  `Content-Type`, plus `X-Forwarded-For` / `X-Forwarded-Proto` (real client
  identity) and the `X-TN-Gateway` hop marker (loop protection; calls sent to
  the `--redirect-queries` URL carry `X-TN-Gateway-Redirect` instead). The client
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
| `--redirect-queries` | `WORKER_GATEWAY_REDIRECT_QUERIES` | (none) | JSON-RPC endpoint (`http` or `https`) for every call except transaction submissions; see [Query redirect](#query-redirect). |
| `--readiness-poll-interval` | `WORKER_GATEWAY_READINESS_POLL_INTERVAL` | `5s` | Readiness poll cadence. |
| `--readiness-poll-timeout` | `WORKER_GATEWAY_READINESS_POLL_TIMEOUT` | `2s` | Per-poll timeout. |
| `--upstream-connect-timeout` | `WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT` | `2s` | Upstream connect timeout. |
| `--upstream-request-timeout` | `WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT` | `30s` | Upstream per-request deadline. |
| `--header-read-timeout` | `WORKER_GATEWAY_HEADER_READ_TIMEOUT` | `10s` | Inbound header read deadline (slow-loris guard). |
| `--max-connections` | `WORKER_GATEWAY_MAX_CONNECTIONS` | `500` | Concurrent inbound connection cap. |
| `--tcp-user-timeout` | `WORKER_GATEWAY_TCP_USER_TIMEOUT` | `30s` | Transport-stall deadline (`TCP_USER_TIMEOUT`, Linux; `0` disables). |
| `--max-connection-duration` | `WORKER_GATEWAY_MAX_CONNECTION_DURATION` | `10m` | Hard cap on one connection's total lifetime (`0` disables). |
| `--max-request-bytes` | `WORKER_GATEWAY_MAX_REQUEST_BYTES` | `1048576` | Max request body size, in bytes (1 MiB; see [Request size](#request-size)). |
| `--max-inflight-request-bytes` | `WORKER_GATEWAY_MAX_INFLIGHT_REQUEST_BYTES` | `536870912` | Request-body bytes held across all in-flight requests (512 MiB; at least `--max-request-bytes`, at most 4294967295; `0` disables; see [Request size](#request-size)). |
| `--rate-limit-per-ip` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP` | `100` | Per-IP requests/second (`0` disables). |
| `--rate-limit-per-ip-burst` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_BURST` | `0` | Per-IP burst (`0` derives 2×rate). |
| `--rate-limit-per-ip-v6-prefix` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_V6_PREFIX` | `64` | IPv6 prefix (bits) the client address is masked to before it keys its bucket. |
| `--rate-limit-per-ip-v4-prefix` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_V4_PREFIX` | `32` | IPv4 prefix (bits) the client address is masked to before it keys its bucket. |
| `--rate-limit-global` | `WORKER_GATEWAY_RATE_LIMIT_GLOBAL` | `3000` | Gateway-wide requests/second (`0` disables). |
| `--rate-limit-global-burst` | `WORKER_GATEWAY_RATE_LIMIT_GLOBAL_BURST` | `0` | Global burst (`0` derives 2×rate). |
| `--graceful-shutdown-timeout` | `WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Drain deadline on SIGTERM. |
| `--metrics` | `WORKER_GATEWAY_METRICS_ADDR` | (none) | Prometheus scrape endpoint address (`GET /metrics`); unset disables metrics. |
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
response size does not translate into gateway memory. A stalled *upstream* is
bounded by the upstream request timeout; a stalled or slow-reading *client*
(the response-side slow loris: that timeout is only observed while the body
is being polled, which downstream backpressure prevents) is bounded by two
write-path guards instead: `TCP_USER_TIMEOUT` (`--tcp-user-timeout`, Linux
kernels) closes a connection whose peer stops acknowledging written data
(note it replaces the kernel's default ~15min retransmit budget for that
socket, so a peer black-holed past the deadline is dropped where stock TCP
might have recovered), and a connection-lifetime cap
(`--max-connection-duration`) closes any connection, keep-alive sessions
included, that outlives it, catching a client that trickles reads too slowly
to be worth a slot but fast enough to defeat the transport guard. The cap
closes abruptly: an exchange in flight on a long-lived keep-alive session is
cut off at the cap, so size it well above the longest legitimate transfer.
It must be at least the gateway's single-request bound
(`--header-read-timeout` + the whole-request deadline above) so the first
request on a connection can never be cut off.

Every request forwarded to a worker carries the `X-TN-Gateway` hop marker (calls
sent to the `--redirect-queries` URL carry `X-TN-Gateway-Redirect` instead, see
[Query redirect](#query-redirect)), and an inbound
request that already carries it is rejected (HTTP `508`), so a misconfigured
upstream or VIP that points back at a gateway breaks the loop at the first
revisit instead of exhausting file descriptors.

## Edge protections

### Rate limiting

Two token-bucket limiters shed load before a request is buffered or forwarded:

- A **per-client** limiter (`--rate-limit-per-ip`, requests/second, with
  `--rate-limit-per-ip-burst`) caps what one source can take of that budget.
- A **global** limiter (`--rate-limit-global` / `--rate-limit-global-burst`)
  caps aggregate throughput to roughly what the upstream workers can absorb.

Either limiter is disabled by setting its rate to `0`; a `0` burst derives twice
the sustained rate. An over-limit request receives a JSON-RPC `429` (see below),
never a bare reset.

#### Prefix keying

The per-client bucket is keyed on the client's **network prefix**, not its bare
address: the peer address has its host bits cleared before the bucket is looked
up. Keyed on the bare address, a client that rotates its source address gets a
fresh, full bucket per address and never accumulates spent budget, so only the
global limit applies to it; a single IPv6 `/64` makes that trivial.

- `--rate-limit-per-ip-v6-prefix` (default `64`) is the IPv6 prefix. A `/64` is
  the smallest subnet routed to one customer, so every address a client can pick
  inside its own allocation shares one bucket.
- `--rate-limit-per-ip-v4-prefix` (default `32`) is the IPv4 prefix. A `/32` is
  a single address, so the IPv4 path behaves exactly as it did before prefix
  keying and unrelated customers behind one carrier-grade NAT are never grouped
  onto a shared bucket. Narrow it only if your clients genuinely map to larger
  IPv4 allocations.

A prefix wider than its address family allows (`/33` for IPv4, `/129` for IPv6)
is rejected at startup rather than clamped. On a dual-stack listener an IPv4
peer is reported in the mapped `::ffff:a.b.c.d` form; those are unmapped and
keyed by the **IPv4** prefix, so they are metered per address rather than all
landing in one `/64`.

> **Residual limitation.** Prefix keying bounds rotation *within* one allocation,
> not across allocations. An attacker holding many distinct allocations (several
> `/64`s, a spread of unrelated IPv4 addresses, or a botnet) still earns one
> bucket per allocation, and only the global limiter caps their total. Prefix
> keying raises the cost of the attack from free to the price of address space;
> it does not eliminate it. Size `--rate-limit-global` accordingly.

The client identity is the immediate TCP peer. Run the gateway **edge-facing**:
behind an untrusted L7 proxy the peer is that proxy, so per-IP limiting would
meter the proxy, not the real client. Terminate client identity at that proxy,
or put the per-IP limit there.

> The default rates (`100`/s per IP, `3000`/s global) are conservative starting
> points, not tuned figures. Set them to your workers' measured capacity before
> relying on them; they can also be disabled entirely (`0`) if you rate-limit at
> the ingress.

The gateway's own `GET /health` and `GET /ready` probes are **exempt** from rate
limiting, so an orchestrator's liveness/readiness checks keep succeeding under a
flood (rate-limiting them would make the orchestrator kill or depool the pod at
the worst possible moment).

Per-IP state is bounded: idle buckets are swept periodically and the number of
tracked IPs is capped, so a wide spread of source IPs cannot grow memory without
limit.

### Request size

`--max-request-bytes` (default 1 MiB) caps the buffered request body; a larger
body is rejected with a JSON-RPC "request too large" error (`413`, `-32003`)
before forwarding.
A declared `Content-Length` over the cap is rejected before any of the body is read.

`--max-inflight-request-bytes` (default 512 MiB) budgets the request-body bytes held across all in-flight requests.
Each request reserves its `Content-Length`, or the whole `--max-request-bytes` when its body is chunked, before the body is read, and returns the reservation once the request has been forwarded or rejected.
A request that does not fit in what is left gets a JSON-RPC error at once (`503`, `-32010`) instead of waiting; its `id` echoes as `null`, since its body is never read.
Clients should retry it after a short backoff.
The `/health` and `/ready` probes are exempt, and a rate-limited request costs no budget.
The budget must be at least `--max-request-bytes`, so a chunked request can always fit; `0` disables it.

Size it from both ends:

- **Large enough for a submission.** The worker's transaction pool admits at
  most 128 KiB of raw transaction (reth's `DEFAULT_MAX_TX_INPUT_BYTES`, which
  the node does not override). Hex-encoded inside an `eth_sendRawTransaction`
  call that is about 256 KiB, so the 1 MiB default fits the largest admissible
  submission with room to spare, or a batch of three. If the node raises
  `--txpool.max-tx-input-bytes`, raise this flag to at least twice that value
  plus a little for the JSON envelope. There is no point going above the
  worker's own request cap, 15 MiB (the node's `--rpc.max-request-size`
  default of `15`, in MiB): the worker rejects anything larger anyway.
- **Small enough for memory.** The whole body is buffered before it is forwarded, and every open connection can hold one, so peak request memory is about `--max-connections` × `--max-request-bytes` plus per-connection and runtime overhead.
  Keep that well under the container's memory limit.
  A held body costs more than its size, because the connection's read buffer stays allocated while the request is in flight: 500 held 1 MiB bodies peaked at about 712 MiB, so budget about 1.5 × `--max-connections` × `--max-request-bytes` plus 64 MiB.
  With the defaults that is about 814 MiB, which the 1Gi limit in the reference manifest (`deploy/k8s/deployment.yaml`) covers.
  If you raise either flag, raise the limit with it, or lower one of the two flags until the product fits, for example `--max-connections 128` for about 128 MiB.

### Transaction screening

A single `eth_sendRawTransaction` call is decoded far enough to reject, at the
edge, the two cases the worker would also reject — an undecodable payload and an
EIP-4844 blob transaction (the network does not accept blobs) — saving a wasted
upstream round-trip. The decode uses the same pooled wire format the worker's
RPC accepts and never recovers the signer, so it cannot reject a transaction the
worker would accept. Batches (JSON arrays) and every other method are forwarded
unchanged and validated upstream: by the worker, or, with `--redirect-queries`,
by the query URL for everything that is not a submission.

## Query redirect

On a validator, set `--redirect-queries <URL>` so that the worker receives transaction submissions and nothing else.
With the flag set, `eth_sendRawTransaction` and `eth_sendRawTransactionSync` go to the first ready worker, and every other call goes to the URL, typically a public RPC.
Method names match exactly and case-sensitively.
Everything else counts as a query: `eth_sendTransaction` (no node configures a signer, so the worker could only refuse it), `tn_*` and `debug_*` calls, and any body the gateway cannot read as submissions, such as one that is not JSON, an empty batch, a `method` that is not a string, or bytes after the JSON value.

The URL may be `http` or `https`; worker URLs stay `http` only.
An `https` URL needs the system CA certificates, which the image installs.
The URL must not point at the gateway itself or at a worker's RPC host and port, and plain `http` to a host that is not a loopback or private address logs a warning at startup.

A batch goes to the worker only when every element is a submission.
A batch that mixes submissions with other calls goes, whole, to the query URL; otherwise a client could put one submission in front of any number of reads and push them all onto the validator.
The public RPC accepts submissions too, so the client loses nothing, but a submission inside a mixed batch reaches the network through the public RPC rather than this validator.
`tn_worker_gateway_mixed_batches_total` counts these batches; splitting a batch and merging the two responses is not implemented.

Routing happens after the [transaction screen](#transaction-screening), so an undecodable submission is still refused at the gateway.
Calls sent to the query URL carry `X-TN-Gateway-Redirect: 1`, `X-Forwarded-For` and `X-Forwarded-Proto`, but not `X-TN-Gateway`, so a public RPC behind a gateway of its own does not reject them as a loop.
A gateway with `--redirect-queries` set answers an inbound request carrying `X-TN-Gateway-Redirect` with `508` / `-32004`, which catches a query URL that leads back to a redirecting gateway; a gateway without the flag forwards such a request normally.
The gateway follows no HTTP redirects: a `3xx` from either upstream is passed to the client as is, without its `Location` header, so a query upstream cannot bounce a read onto the worker.
Requests to either upstream carry a `tn-worker-gateway/<version>` user agent.

`/ready` still means "this gateway can take submissions".
The query URL gets no readiness probe and no fallback: when it fails, the client gets `502` or `504` and the call is never retried on the worker, which would put the read load on the validator just when the public RPC is struggling.

| Worker | Query URL | `/ready` | Submissions | Other calls |
| --- | --- | --- | --- | --- |
| up | up | `200` | worker | query URL |
| down | up | `503` | `503` / `-32000` | query URL |
| up | down | `200` | worker | `502` / `-32001` or `504` / `-32002`, no fallback |

Reads answered by the query URL come from a node that has not seen this validator's transaction pool, every one of them reaches that node from the gateway's address, and each carries the client's address in `X-Forwarded-For`; see [Split routing](#split-routing) before advertising the endpoint.
`/ready` reports only whether submissions can be served, so a front that drops a gateway on `503` (the reference `readinessProbe`, a health-checked DNS record) also stops its reads while the worker is down; probe `/health` instead if reads must survive a worker outage.

The reverse topology, a gateway that sends submissions to a validator's worker and every other call to an observer's RPC, can be expressed with the same two settings, but it is not a supported deployment yet.

## Gateway endpoints

- `GET /health`: liveness, always `200 OK` while the process runs.
- `GET /ready`: readiness, `200` when at least one upstream is ready, else
  `503` with `{"ready": false}`.
- everything else (i.e. `POST /`): forwarded to a ready upstream worker, or,
  with `--redirect-queries`, to the query URL unless it is a submission.

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
| Rate limit exceeded | `429` | `-32006` |
| Raw transaction undecodable | `400` | `-32007` |
| Unsupported transaction type (EIP-4844 blob) | `400` | `-32008` |
| In-flight request byte budget exhausted | `503` | `-32010` |
| Request body unreadable (client aborted) | `400` | `-32600` |

The gateway's own codes sit in the JSON-RPC server-error range
(`-32000..=-32099`), which upstream servers also use for their errors;
disambiguate by HTTP status and message, not by code alone (`-32600` is the
spec's standard "Invalid Request" code).

## Graceful shutdown

On SIGTERM (or ctrl-c) the gateway stops accepting new connections and drains
in-flight requests, up to `--graceful-shutdown-timeout`. Requests still running
after the deadline are force-closed.

## Observability

Pass `--metrics <addr>` (or `WORKER_GATEWAY_METRICS_ADDR`) to expose a Prometheus
scrape endpoint at `GET /metrics` on its own listener, separate from the client
JSON-RPC port. Leave it unset to disable metrics entirely (the instrumentation
then compiles to no-ops). The endpoint is unauthenticated, so bind it to an
internal interface, not the public edge.

The endpoint reuses the node's `tn-metrics` recorder, so the gateway's
`tn_worker_gateway_*` series render alongside a node's `tn_*` metrics under one
Prometheus/Grafana setup. A ready-to-import Grafana dashboard is provided at
`grafana-worker-gateway.json`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tn_worker_gateway_inflight_requests` | gauge | | Proxied requests currently in flight; the intended autoscaling signal. |
| `tn_worker_gateway_requests_total` | counter | `outcome` (`forwarded` / `rejected`) | Proxied requests by terminal outcome. |
| `tn_worker_gateway_rejections_total` | counter | `reason` | Rejected proxied requests, broken down by reason (the conditions in the failure table above). |
| `tn_worker_gateway_request_duration_seconds` | histogram | | End-to-end proxied-request latency. |
| `tn_worker_gateway_upstream_ready` | gauge | `worker_id` | Per-worker readiness as last polled (`1` ready, `0` not-ready). |
| `tn_worker_gateway_routed_requests_total` | counter | `route` (`worker` / `query`), `result` (`forwarded` / `unreachable` / `timeout`) | Forward attempts by route, with their transport result. |
| `tn_worker_gateway_mixed_batches_total` | counter | | Batches sent whole to the `--redirect-queries` URL because they mixed submissions with other calls. |
| `tn_worker_gateway_inflight_request_bytes` | gauge | | Request-body bytes held against `--max-inflight-request-bytes` by requests in flight. |

The gateway's own `/health` and `/ready` probes are not proxied and are excluded
from these series, so they reflect real client load only. The scrape also
carries a `tn_info{version}` build gauge and process metrics; the process
metrics render under a `reth_` prefix (`reth_process_*`), an artifact of the
shared recorder's reth-compatible naming.

## Operator guidance

The [production-readiness review](docs/production-readiness.md#operator-guidance) gives the findings behind each rule below.

### Validators: always redirect queries

Set `--redirect-queries` on every gateway in front of a validator.
Without it, every read reaches the worker, including the `tn_*` calls the node says a validator should not serve publicly, and slow reads through one gateway can use up the worker's RPC connection limit for every gateway.
Point it at an `https` public RPC for the same chain.

### Sizing for N gateways

Every limit is per process, so the worker sees the sum over all gateways.

- **Rate.** The worker receives up to N × `--rate-limit-global` calls per second, where N is the largest number of gateways that can run at once: the HPA's `maxReplicas` if you install it (10 in the reference manifest).
  Size `--rate-limit-global` as the worker's budget divided by that N.
- **Worker connections.** N × `--max-connections` can exceed the worker's `--rpc.max-connections` (500 by default), and the worker answers `429` to everything over its limit.
  With `--redirect-queries` only submissions reach the worker, and they finish quickly except `eth_sendRawTransactionSync`, which can hold a worker connection for up to 30 s.
  Raise the worker's limit above N × `--max-connections`, or accept that a flood of Sync calls through one gateway can make the worker refuse submissions from the others.
- **Memory.** Peak request memory per gateway is about `--max-connections` × `--max-request-bytes` plus overhead (see [Request size](#request-size)); the reference manifest's 1Gi limit covers the defaults.

### Split routing

With `--redirect-queries`, reads are answered by a node that has not seen this validator's transaction pool.

- `eth_getTransactionCount(.., "pending")`, `eth_getTransactionByHash` and receipts right after a submission can lag, so clients that send several transactions in a row should track their own nonces.
- Fee quotes come from the public node and can lag an epoch boundary.
- A submission inside a mixed batch goes to the public RPC with the rest of the batch and enters the network there.
- Every redirected read reaches the public RPC from the gateway's address, so its per-IP limits apply to all of the gateway's clients together; agree limits with its operator before advertising the endpoint.
- Each redirected call carries the client's address in `X-Forwarded-For`, so the public RPC's operator sees your clients' addresses.

### DNS and the DDoS front

- Publish the gateways behind health-checked DNS or a load balancer that probes each gateway's `/ready`; with plain round-robin records a dead gateway keeps receiving its share of clients until someone edits the zone.
  `/ready` means "can take submissions", so a gateway whose worker is down drops out even though it still serves reads; every gateway shares the worker, so probe `/health` instead if reads must survive a worker outage.
- Lock the domain at the registrar and enable DNSSEC where the provider supports it; a hijacked name serves forged state to every client.
- Absorb packet floods in front of the gateways.
  A front that terminates TCP makes every client share the front's rate-limit buckets, because the gateway keys its limits on the TCP peer; use an L4 front that preserves client addresses, or set the per-IP limit for the front's addresses.
- If you use a front, firewall the gateways so that only the front reaches them; a gateway reachable directly bypasses it.

### Firewalling

- The worker's RPC port: reachable from the gateway hosts only.
- The node's `--healthcheck` port: reachable from the gateway hosts only.
  It is unauthenticated and serves one connection at a time, so a few idle connections from anyone else make every gateway report not ready.
- The gateway's metrics port: inside the monitoring network only.
- The gateway-to-worker hop is plaintext `http`; when it leaves a network you control, run it through a tunnel.

## Deployment

A container image is built by `bin/worker-gateway/Dockerfile`, which mirrors the
node's `etc/Dockerfile` (rust:1.94 builder, slim debian runtime, non-root user).
Build it from the repository root:

```
docker build -f bin/worker-gateway/Dockerfile -t telcoin-worker-gateway:latest .
```

Reference Kubernetes manifests live under `deploy/k8s/` (a Deployment, Service,
ServiceMonitor, and a HorizontalPodAutoscaler keyed on the
`tn_worker_gateway_inflight_requests` gauge). They are a starting point, not a
turnkey install: see `deploy/README.md` for the placeholders to replace and the
prometheus-adapter rule the autoscaler needs.

Before putting gateways in front of a validator, read the [production-readiness review](docs/production-readiness.md), in particular its operator guidance on DNS, the DDoS front, firewalling and sizing for N gateways.
