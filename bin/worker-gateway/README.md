# worker-gateway

A mostly stateless reverse proxy that fronts a Telcoin Network worker's JSON-RPC endpoint.
It forwards JSON-RPC calls (`eth_*` / `net_*` / `web3_*`) unchanged to a ready upstream worker, refuses the namespaces the [method policy](#method-policy) denies (`tn_*`, `debug_*`, `trace_*` and `admin_*` by default), gates traffic on a polled per-worker readiness signal, and exposes its own liveness and readiness endpoints so an orchestrator can route around it.
With `--redirect-queries` it sends transaction submissions, and the few reads only the worker's pool can answer, to the worker and every other call to a public RPC (see [Query redirect](#query-redirect)); a validator's gateways should always run that way (see [Operator guidance](#operator-guidance)).
"Unchanged" applies to the JSON-RPC body and content type of a `POST`, the only method the gateway forwards; the header contract is deliberately minimal (see Scope).

Because every instance is mostly stateless and identical, the gateway can be scaled horizontally: any replica can serve any request.
Each replica keeps its own bounded set of recent submission hashes, which only steers where a lookup goes (see [Reads kept on the worker](#reads-kept-on-the-worker)).
Besides the proxy and its edge protections, the crate ships a Prometheus `/metrics` endpoint, a container image, and reference Kubernetes manifests, including an autoscaler keyed on the in-flight-request gauge.

## Scope (v1)

The [production-readiness review](docs/production-readiness.md) evaluates this gateway as the public endpoint of a validator, lists its findings by severity, and gives the plan for each one.

- HTTP-only. WebSocket (`eth_subscribe`) pass-through is deliberately out of
  scope: subscriptions are per-connection stateful and cannot survive a replica
  dying, which breaks the stateless-scaling invariant. Point subscription
  clients at a worker's WS endpoint behind your own ingress.
- Static upstream configuration (no hot reload, no dynamic discovery).
- Calls for the worker go to the first ready worker in configuration order; there is no load balancing across workers.
  With `--redirect-queries`, only `eth_sendRawTransaction`, `eth_sendRawTransactionSync` and the [reads kept on the worker](#reads-kept-on-the-worker) go to the worker, and every other call goes to the query URL (see [Query redirect](#query-redirect)).
- TLS termination for clients and auth/API keys are out of scope; run the gateway behind your own ingress/mTLS.
  The hop to the workers can use TLS (see [TLS to workers](#tls-to-workers)).
- Header forwarding is minimal.
  Upstream gets a `POST` with the request body and `Content-Type`, plus `X-Forwarded-For` / `X-Forwarded-Proto` (the client identity the gateway resolved, never the client's own values; see [Client identity behind a front](#client-identity-behind-a-front)) and the `X-TN-Gateway` hop marker (loop protection; calls sent to the `--redirect-queries` URL carry `X-TN-Gateway-Redirect` instead, plus the `--redirect-queries-header` header when one is set; see [Query upstream header](#query-upstream-header)).
  The client gets the upstream status, body, and `Content-Type` (an error status without a JSON body gets a JSON-RPC error body; see [Behaviour on failure](#behaviour-on-failure)); from the `--redirect-queries` URL only a JSON `Content-Type` (`application/json` or an `application/*+json` type) passes, and anything else, or none, becomes `application/json`.
  All other headers are dropped in both directions.
  With `--cors-allowed-origins` set the gateway adds CORS headers of its own (see [CORS](#cors)); without it, browser dApps on another origin need CORS handled at the ingress.
  Every response on `--listen-addr` carries `X-Content-Type-Options: nosniff`, so a browser never reads a body served on the validator's origin as HTML or script; the only exceptions are hyper's own protocol errors (see [Behaviour on failure](#behaviour-on-failure)).
- The request path and query string are not forwarded: a `POST` to any path but `/health`, `/ready` and `/ready/any` goes to the configured upstream base URL, since JSON-RPC carries its method in the body (see [Gateway endpoints](#gateway-endpoints)).

## Readiness contract

The gateway polls each upstream node's readiness endpoint
(`GET /health/workers`) and expects the versioned envelope:

```json
{
  "version": 1,
  "workers": [
    { "worker_id": 0, "accepting_transactions": true }
  ]
}
```

A poll succeeds only when the worker's entry reports `accepting_transactions: true`.
Every other outcome is a failed poll: an unreachable node, a timed-out poll, an HTTP error status, a malformed payload, a body over 64 KiB, the worker missing from the list, or the worker not accepting transactions.
Unknown fields and newer envelope versions are tolerated (forward compatible).

Every upstream starts **not-ready**, so the gateway fails closed until an upstream's first run of successful polls.
After that, readiness changes only on a run of consecutive results, so one slow poll does not drop every gateway that polls the same node:

- a ready upstream turns not-ready after `--readiness-failure-threshold` failed polls in a row (default 3);
- a not-ready upstream turns ready after `--readiness-success-threshold` successful polls in a row (default 2), at startup too, so at the defaults an upstream serves from its second poll, one interval after the gateway starts.

Each cycle polls every upstream at once, each poll bounded by `--readiness-poll-timeout`, so a hanging upstream does not delay the polls of the others.
The body is read up to 64 KiB: a larger declared `Content-Length` is refused before any of the body is read, and a body without one is cut off at the cap.

### Passive health on the RPC path

The poll measures the node's health listener, not the worker's RPC port, so the gateway also counts its own forwards to each worker.
A forward that fails to connect, a connect timeout included, counts toward `--upstream-failure-threshold` (default 3), and a forward that gets any response, an HTTP error included, resets the count.
A timeout or any other failure after the request was sent neither counts nor resets the count, because the client's method and params decide how long the worker takes (`eth_sendRawTransactionSync` waits up to 30 s for inclusion).
A worker that accepts connections but stops answering is left to the poller.
At the threshold the upstream is marked not-ready until the poller sees `--readiness-success-threshold` successful polls in a row again, and later requests go to the next ready upstream in configuration order.
The request that saw the failure is answered with its own error (`502` or `504`) and is never retried on another upstream.
Forwards to the `--redirect-queries` URL are not counted.
Neither is a forward the gateway's own DNS lookup cap refused (see [DNS and the DDoS front](#dns-and-the-ddos-front)): that refusal says nothing about the worker, so a burst of it cannot mark a healthy worker not-ready.
`--upstream-failure-threshold 0` turns passive health off.

While the health listener answers and the RPC port stays down, the upstream cycles: the poller turns it ready after its run of successful polls, and the next `--upstream-failure-threshold` worker requests fail before it is marked again.

### Readiness logs

Each transition is logged with the worker id and the upstream's RPC and readiness origins (scheme, host and port, never the full URL): at `info` with the run of successful polls when an upstream becomes ready, and at `warn` with a `cause` when it becomes not-ready.
The cause is that of the last failed poll (`timeout`, `HTTP status <code>`, `connection error (<class>)`, `malformed payload`, `body too large`, `worker not accepting transactions`), or `rpc path` for a mark from the forwards.
An upstream that has never been ready logs the cause of its first failed poll at `info`, once, so a gateway that never becomes ready says why at the default log level.
Every other failed poll logs at `debug`.

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

Startup also rejects a self-pointing upstream URL written as an IPv4-mapped IPv6 literal such as `[::ffff:127.0.0.1]`, an upstream list that names one worker id twice, and a zero `--readiness-poll-interval`, `--readiness-poll-timeout`, `--upstream-connect-timeout`, `--upstream-request-timeout` or `--header-read-timeout`, none of which has a disabled setting.

The gateway ignores `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`: forwarded calls and readiness polls connect to the configured URLs directly.
Neither follows an HTTP redirect: a readiness poll that gets a `3xx` counts as not ready, so only a `2xx` answer from the configured readiness URL can mark its worker ready.

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
| `--listen-addr` | `WORKER_GATEWAY_LISTEN_ADDR` | `0.0.0.0:8545` | Client JSON-RPC + `/health` + `/ready` + `/ready/any`. |
| `--probe-addr` | `WORKER_GATEWAY_PROBE_ADDR` | (none) | Separate listener for `/health`, `/ready` and `/ready/any` only, outside the connection cap and rate limits; unset opens none (see [Gateway endpoints](#gateway-endpoints)). |
| `--config` | `WORKER_GATEWAY_CONFIG` | (none) | YAML upstream list. |
| `--upstream-rpc-url` | `WORKER_GATEWAY_UPSTREAM_RPC_URL` | (none) | Inline upstream JSON-RPC URL. |
| `--upstream-readiness-url` | `WORKER_GATEWAY_UPSTREAM_READINESS_URL` | (none) | Inline upstream readiness URL. |
| `--worker-id` | `WORKER_GATEWAY_WORKER_ID` | `0` | Inline upstream worker id. |
| `--redirect-queries` | `WORKER_GATEWAY_REDIRECT_QUERIES` | (none) | JSON-RPC endpoint (`http` or `https`) for every call except transaction submissions and the reads kept on the worker; see [Query redirect](#query-redirect). |
| `--redirect-queries-header` | `WORKER_GATEWAY_REDIRECT_QUERIES_HEADER` | (none) | One `Name: value` header sent to the query URL only (redirected calls, the query probe and the startup chain-id check), never to a worker, and never logged; requires `--redirect-queries`; see [Query upstream header](#query-upstream-header). |
| `--redirect-queries-header-file` | `WORKER_GATEWAY_REDIRECT_QUERIES_HEADER_FILE` | (none) | File holding the `--redirect-queries-header` line (trimmed), for a secret; requires `--redirect-queries`; not with `--redirect-queries-header`. |
| `--recent-submission-ttl` | `WORKER_GATEWAY_RECENT_SUBMISSION_TTL` | `60s` | With `--redirect-queries`, how long lookups of a forwarded submission's hash go to the worker (`0` disables); see [Reads kept on the worker](#reads-kept-on-the-worker). |
| `--denied-method-prefixes` | `WORKER_GATEWAY_DENIED_METHOD_PREFIXES` | `tn_,debug_,trace_,admin_` | Comma-separated method-name prefixes refused when `--redirect-queries` is unset (empty allows every method); see [Method policy](#method-policy). |
| `--readiness-poll-interval` | `WORKER_GATEWAY_READINESS_POLL_INTERVAL` | `5s` | Readiness poll cadence. |
| `--readiness-poll-timeout` | `WORKER_GATEWAY_READINESS_POLL_TIMEOUT` | `2s` | Per-poll timeout. |
| `--readiness-failure-threshold` | `WORKER_GATEWAY_READINESS_FAILURE_THRESHOLD` | `3` | Failed polls in a row that turn a ready upstream not-ready (at least 1); see [Readiness contract](#readiness-contract). |
| `--readiness-success-threshold` | `WORKER_GATEWAY_READINESS_SUCCESS_THRESHOLD` | `2` | Successful polls in a row that turn a not-ready upstream ready, at startup too (at least 1). |
| `--upstream-failure-threshold` | `WORKER_GATEWAY_UPSTREAM_FAILURE_THRESHOLD` | `3` | Worker forwards in a row that fail to connect, a connect timeout included, before the upstream is marked not-ready (`0` disables); a timeout or failure after the request was sent neither counts nor resets the count, because the client's method and params decide how long the worker takes, and a worker that accepts connections but stops answering is left to the poller; see [Passive health on the RPC path](#passive-health-on-the-rpc-path). |
| `--upstream-connect-timeout` | `WORKER_GATEWAY_UPSTREAM_CONNECT_TIMEOUT` | `2s` | Upstream connect timeout. |
| `--upstream-request-timeout` | `WORKER_GATEWAY_UPSTREAM_REQUEST_TIMEOUT` | `35s` | Upstream per-request deadline; the default outlasts reth's 30 s `eth_sendRawTransactionSync` wait. |
| `--query-request-timeout` | `WORKER_GATEWAY_QUERY_REQUEST_TIMEOUT` | `10s` | Per-request deadline for the `--redirect-queries` URL (`0` = use `--upstream-request-timeout`); the default drops to `--upstream-request-timeout` when that is shorter, and a value set here must not exceed it. |
| `--upstream-ca-cert` | `WORKER_GATEWAY_UPSTREAM_CA_CERT` | (none) | PEM file of extra CA certificates trusted for `https` upstreams, on top of the system roots; see [TLS to workers](#tls-to-workers). |
| `--upstream-client-cert` | `WORKER_GATEWAY_UPSTREAM_CLIENT_CERT` | (none) | PEM client certificate chain presented to an `https` upstream that asks for one; requires `--upstream-client-key`. |
| `--upstream-client-key` | `WORKER_GATEWAY_UPSTREAM_CLIENT_KEY` | (none) | Unencrypted PEM private key for `--upstream-client-cert`; requires `--upstream-client-cert`. |
| `--dns-cache-ttl` | `WORKER_GATEWAY_DNS_CACHE_TTL` | `30s` | How long an upstream host's resolved addresses are reused (`0` disables the cache); see [DNS and the DDoS front](#dns-and-the-ddos-front). |
| `--max-concurrent-dns-lookups` | `WORKER_GATEWAY_MAX_CONCURRENT_DNS_LOOKUPS` | `8` | Upstream DNS lookups that may run at once, for the proxy and the readiness poller each (at least `1`); a lookup over the cap fails at once and does not count toward `--upstream-failure-threshold`. |
| `--header-read-timeout` | `WORKER_GATEWAY_HEADER_READ_TIMEOUT` | `10s` | Inbound header read deadline (slow-loris guard); a `--proxy-protocol` connection from a listed front gets one more for its PROXY header. |
| `--max-connections` | `WORKER_GATEWAY_MAX_CONNECTIONS` | `500` | Concurrent inbound connection cap. |
| `--max-connections-per-ip` | `WORKER_GATEWAY_MAX_CONNECTIONS_PER_IP` | `32` | Concurrent inbound connections per client prefix, keyed on the resolved client (the PROXY header's source, else the peer); connections from `--trusted-proxies` are not counted (`0` disables; see [Connection handling](#connection-handling)). |
| `--max-inflight-submissions` | `WORKER_GATEWAY_MAX_INFLIGHT_SUBMISSIONS` | `256` | In-flight cap for requests made only of submissions; over it the gateway answers `503` at once (`0` = unlimited). |
| `--max-inflight-queries` | `WORKER_GATEWAY_MAX_INFLIGHT_QUERIES` | `256` | In-flight cap for every other request; over it the gateway answers `503` at once (`0` = unlimited). Without `--redirect-queries` it is lowered to at most `--max-upstream-inflight` less a fifth (80 by default), so submissions keep the rest of the worker slots (see [Per-route capacity](#per-route-capacity)). |
| `--max-upstream-inflight` | `WORKER_GATEWAY_MAX_UPSTREAM_INFLIGHT` | `100` | Cap on concurrent requests to the worker; over it the gateway answers `503` at once (`0` = unlimited; see [Sizing for N gateways](#sizing-for-n-gateways)). |
| `--tcp-user-timeout` | `WORKER_GATEWAY_TCP_USER_TIMEOUT` | `30s` | Transport-stall deadline (`TCP_USER_TIMEOUT`, Linux; `0` disables). |
| `--max-connection-duration` | `WORKER_GATEWAY_MAX_CONNECTION_DURATION` | `10m` | Cap on one connection's lifetime; an exchange in flight at the cap gets the whole-request deadline to finish (`0` disables). |
| `--max-request-bytes` | `WORKER_GATEWAY_MAX_REQUEST_BYTES` | `1048576` | Max request body size, in bytes (1 MiB; see [Request size](#request-size)). |
| `--max-batch-len` | `WORKER_GATEWAY_MAX_BATCH_LEN` | `50` | Max calls in one JSON-RPC batch (`0` means unlimited), never more than the smallest enabled rate-limit burst; a longer batch gets `413` / `-32003` (see [Batches](#batches)). |
| `--max-inflight-request-bytes` | `WORKER_GATEWAY_MAX_INFLIGHT_REQUEST_BYTES` | `536870912` | Request-body bytes held across all in-flight requests (512 MiB; at least `--max-request-bytes`, at most 4294967295; `0` disables; see [Request size](#request-size)). |
| `--http1-max-buf-size` | `WORKER_GATEWAY_HTTP1_MAX_BUF_SIZE` | `65536` | Per-connection read buffer cap, in bytes (64 KiB; minimum 8192; a request head must fit in it; see [Request size](#request-size)). |
| `--rate-limit-per-ip` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP` | `100` | Per-IP requests/second (`0` disables). |
| `--rate-limit-per-ip-burst` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_BURST` | `0` | Per-IP burst (`0` derives 2×rate). |
| `--rate-limit-per-ip-v6-prefix` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_V6_PREFIX` | `64` | IPv6 prefix (bits) the client address is masked to before it keys its rate-limit bucket and its `--max-connections-per-ip` count. |
| `--rate-limit-per-ip-v4-prefix` | `WORKER_GATEWAY_RATE_LIMIT_PER_IP_V4_PREFIX` | `32` | IPv4 prefix (bits) the client address is masked to before it keys its rate-limit bucket and its `--max-connections-per-ip` count. |
| `--rate-limit-global` | `WORKER_GATEWAY_RATE_LIMIT_GLOBAL` | `3000` | Gateway-wide requests/second (`0` disables). |
| `--rate-limit-global-burst` | `WORKER_GATEWAY_RATE_LIMIT_GLOBAL_BURST` | `0` | Global burst (`0` derives 2×rate). |
| `--rate-limit-submissions` | `WORKER_GATEWAY_RATE_LIMIT_SUBMISSIONS` | `0` | Gateway-wide requests/second reserved for submissions, burst 2×rate (`0` disables; see [Rate limiting](#rate-limiting)). |
| `--trusted-proxies` | `WORKER_GATEWAY_TRUSTED_PROXIES` | (none) | Comma-separated CIDR ranges (a bare address is one host) of the fronts whose `X-Forwarded-For`, `X-Forwarded-Proto` and PROXY header the gateway believes; unset trusts none. See [Client identity behind a front](#client-identity-behind-a-front). |
| `--proxy-protocol` | `WORKER_GATEWAY_PROXY_PROTOCOL` | `false` | Expect a PROXY protocol v2 header on every connection from a `--trusted-proxies` peer (the env value is `true` or `false`); requires `--trusted-proxies`. |
| `--shutdown-delay` | `WORKER_GATEWAY_SHUTDOWN_DELAY` | `0s` | How long to keep serving after SIGTERM, with `/ready` and `/ready/any` reporting draining, before the listeners close (`0` disables; at most `60s`); see [Graceful shutdown](#graceful-shutdown). |
| `--graceful-shutdown-timeout` | `WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Drain deadline on SIGTERM. |
| `--metrics` | `WORKER_GATEWAY_METRICS_ADDR` | (none) | Prometheus scrape endpoint address (`GET /metrics`); unset disables metrics. |
| `--cors-allowed-origins` | `WORKER_GATEWAY_CORS_ALLOWED_ORIGINS` | (none) | Comma-separated browser origins (`scheme://host[:port]`) allowed to call the gateway cross-origin, or `*` for any; unset disables CORS; see [CORS](#cors). |
| `--access-log-sample` | `WORKER_GATEWAY_ACCESS_LOG_SAMPLE` | `0` | Fraction of proxied requests written to the access log, from `0` to `1` (`0` disables); see [Access log](#access-log). |
| `--log-filter` | `RUST_LOG` | `info` | Tracing filter directive. |
| `--log-format` | `WORKER_GATEWAY_LOG_FORMAT` | `text` | Log line format: `text` or `json` (one JSON object per line). `text` is coloured only when stdout is a terminal and `NO_COLOR` is unset or empty; `json` is never coloured. |

Durations use `humantime` syntax (`5s`, `2m`, `500ms`).

### TLS to workers

Worker RPC and readiness URLs may be `http` or `https`.
The node serves both ports as plain `http`, so an `https` URL points at a TLS terminator in front of them, such as a reverse proxy on the worker host.
The forwarding client and the readiness poller share one TLS configuration, and the `--redirect-queries` URL uses it too.

```
worker-gateway \
  --upstream-rpc-url https://worker-0.internal:8545 \
  --upstream-readiness-url https://worker-0.internal:8551/health/workers \
  --upstream-ca-cert /etc/worker-gateway/worker-ca.pem \
  --upstream-client-cert /etc/worker-gateway/gateway.pem \
  --upstream-client-key /etc/worker-gateway/gateway-key.pem
```

- **System roots.** An `https` upstream is verified against the system CA certificates (or the bundle that `SSL_CERT_FILE` or `SSL_CERT_DIR` names), which the image installs.
  When an `https` URL is configured without `--upstream-ca-cert` and the system store holds no certificate, the gateway refuses to start instead of failing every call.
- **Custom CA.** `--upstream-ca-cert` adds every certificate in a PEM bundle to the trusted roots, so a worker certificate from a private CA needs only that CA's file.
  The system roots stay trusted alongside it.
  The certificate must name the host in the URL: a DNS name for a host name, an IP address entry for an IP literal.
- **Client certificate.** `--upstream-client-cert` and `--upstream-client-key`, given together, present a client certificate for mutual TLS, so the terminator can refuse anything but the gateway.
  The key must be an unencrypted PEM key (PKCS#8, PKCS#1 or SEC1).
  The certificate is sent only to a server that asks for one, which can include the `--redirect-queries` URL.
- **Startup checks.** The files are read once, at startup, so restart the gateway to pick up a renewed certificate.
  An unreadable or empty file, a CA file without a certificate, one client flag without the other, or a key that does not match its certificate stops the gateway at startup.
  The error names the flag, and the path of a file that cannot be read or parsed, but never quotes a file's content.
- **Fail closed.** A worker whose certificate does not chain to a trusted root or does not name the URL's host, or whose terminator refuses the client certificate, fails the handshake.
  Its readiness poll then fails, so it is not ready: `/ready` answers `503` and submissions get `503` / `-32000`, and a forward sent before the next poll notices gets `502` / `-32001`.
- **Plaintext warning.** A worker RPC or readiness URL that is plain `http` to a host that is not a loopback or private address logs a warning at startup with the worker id and the URL's origin, like the `--redirect-queries` warning.
  A domain name other than `localhost` counts as public, because the gateway does not resolve it.
  When the hop leaves a network you control, use `https` or a tunnel.

### CORS

CORS is off by default: the gateway adds no CORS header, so a browser dApp served from another origin cannot read its responses unless a front handles CORS.
Set `--cors-allowed-origins` to let browsers call the gateway directly, either to a comma-separated list of origins (`--cors-allowed-origins https://app.example.com,http://localhost:3000`) or to `*` for any origin.
Each origin is `http` or `https`, written as `scheme://host[:port]` with no path, query, fragment or credentials, the way a browser sends it in `Origin`; host case and a default port are normalised, so `https://App.example.com:443` means `https://app.example.com`.
An invalid entry, an empty entry, and a `*` beside other entries all fail startup, and the error names the entry's position in the list rather than its text.

With the flag set, the gateway answers every `OPTIONS` preflight itself, ahead of the `POST`-only rule, and never forwards it; without the flag an `OPTIONS` gets the same `405` as any other method but `POST` (see [Gateway endpoints](#gateway-endpoints)).
The answer allows `POST` with a `Content-Type` header, and the browser may cache it for an hour (`Access-Control-Max-Age: 3600`); a page that sends any other request header, such as `Authorization`, fails its preflight.
A request from an allowed origin gets `Access-Control-Allow-Origin` on its response (the origin itself, or `*` with the wildcard), gateway error responses included.
Origins match exactly, so `https://app.example.com` does not allow `http://app.example.com`, `https://app.example.com:8443` or `https://www.app.example.com`.
A request from any other origin is still served, like a request without `Origin`, but its response carries no `Access-Control-Allow-Origin`, so the browser withholds it from the page.
A preflight from such an origin still lists the allowed method and header and the max age, which grant nothing without `Access-Control-Allow-Origin`.
CORS protects users' browsers, not the gateway, and does not replace the edge protections.
Preflights are answered ahead of the [rate limiter](#rate-limiting) and spend no token, and a `429` still carries the CORS header so the page can read it.
`/health` and `/ready` sit behind the CORS layer like the proxied path; `/ready/any` and the `--probe-addr` listener do not, so they never carry a CORS header and answer an `OPTIONS` with `405`.

## Connection handling

Every inbound connection is served with a header read deadline
(`--header-read-timeout`), `TCP_NODELAY`, and a global concurrency cap
(`--max-connections`; further connections wait in the OS accept backlog).
With `--proxy-protocol`, a connection from a listed front first has up to
`--header-read-timeout` for its PROXY header and then the full deadline again
for its request headers, so a slow client behind a front that sends the PROXY
header only with the client's first bytes can hold a connection slot for twice
`--header-read-timeout` before its headers complete.
Each request additionally has a whole-request deadline of
`--upstream-request-timeout` + `--header-read-timeout` covering the body read
and the upstream response headers, so a request body trickled in below the
size limit cannot hold a slot indefinitely.
The probe listener (`--probe-addr`) has no connection cap: it applies the header read deadline, serves one request per connection and refuses a request head over 8 KiB; see [Gateway endpoints](#gateway-endpoints).

A per-client cap (`--max-connections-per-ip`, default `32`; `0` disables it) keeps one client from holding every `--max-connections` slot.
A client is its network prefix: the connection's client address masked by `--rate-limit-per-ip-v4-prefix` / `--rate-limit-per-ip-v6-prefix` exactly as the per-client rate limiter masks it (see [Prefix keying](#prefix-keying)), and the masking applies even when per-IP rate limiting is off.
The client address is the one [Client identity behind a front](#client-identity-behind-a-front) starts from: the peer, or with `--proxy-protocol` the source a listed front's PROXY header names, so behind an L4 balancer that sends PROXY headers each client counts on its own.
The check runs as soon as that address is known, right after `accept` or right after the PROXY header is read, and before the connection is served: a connection whose prefix already holds the cap is closed at once without a response (a connection-level limit has no request to answer, so it cannot send a JSON-RPC error), and the global slot it was accepted into is returned immediately.
Each refusal increments `tn_worker_gateway_connections_refused_total{reason="per_ip_cap"}` and logs at debug level with the peer, the resolved client and the prefix key.
A connection whose client address is in `--trusted-proxies` is not counted at all: `X-Forwarded-For` attributes its requests to clients one at a time (the per-IP rate limit still meters each of them), and counting the front would put every client behind it under one cap, so the 33rd connection from the front would be refused.
Every other connection counts, the `/health`, `/ready` and `/ready/any` probes on this listener included (the `--probe-addr` listener has no per-client cap), so behind a NAT, load balancer or proxy that hides client addresses and is not listed in `--trusted-proxies`, all clients share one prefix; list the front (with `X-Forwarded-For` or PROXY protocol), or raise the cap or disable it there.

> **Residual limitation.** The per-client cap bounds one prefix, not many.
> `ceil(--max-connections / --max-connections-per-ip)` distinct prefixes still hold every slot: 16 at the defaults (500 / 32), which is 16 IPv4 addresses, or on an IPv6 (or dual-stack) listener a single `/60` under the default `/64` key (a residential `/56` holds 256 keys).
> A listed front's own connections are not counted at all, so they are bounded by `--max-connections` alone.
> Across prefixes only `--max-connections` bounds the total, so absorb wide floods in front of the gateway (see [DNS and the DDoS front](#dns-and-the-ddos-front)).

Upstream response bodies are streamed through, never buffered whole (except
the short, capped answer to a submission whose hash is remembered; see
[Reads kept on the worker](#reads-kept-on-the-worker)), so
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
to be worth a slot but fast enough to defeat the transport guard. At the cap
the connection stops taking new requests: an idle keep-alive session closes
at once, and an exchange in flight gets the whole-request deadline above to
finish before the connection is closed, so a response still streaming then
is cut off; size the cap well above the longest legitimate transfer.
It must be at least the gateway's single-request bound
(`--header-read-timeout` + the whole-request deadline above, plus one more
`--header-read-timeout` for the PROXY header with `--proxy-protocol`) so the
first request on a connection can never be cut off.
With the default timeouts that bound is 55 s (10 s + 35 s + 10 s), or 65 s with `--proxy-protocol`, and an explicit `--max-connection-duration` below it fails startup.

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
  `--rate-limit-per-ip-burst`) caps how many requests one source gets through.
- A **global** limiter (`--rate-limit-global` / `--rate-limit-global-burst`)
  caps aggregate throughput to roughly what the upstream workers can absorb.

The per-client bucket is checked first, and only a request it admits takes a global token, so a source over its own limit cannot drain the shared budget the other clients depend on (WG-08 in the [production-readiness review](docs/production-readiness.md)).
If the global bucket then refuses the request, its per-client token is refunded, so a global refusal costs the client nothing either.

Either limiter is disabled by setting its rate to `0`; a `0` burst derives twice
the sustained rate. An over-limit request receives a JSON-RPC `429` (see below),
never a bare reset.

A batch costs one token per call, the first before its body is read and the rest once its calls are counted, in the same order and with the same refund; see [Batches](#batches).
A third, optional bucket reserves rate for submissions.
With `--rate-limit-submissions` set (requests per second, burst twice the rate), `--rate-limit-global` governs queries and `--rate-limit-submissions` governs submissions, using the classes of [Per-route capacity](#per-route-capacity), so a read flood that empties the global bucket no longer refuses submissions, and a submission flood over its own budget no longer spends the read budget.
The edge cannot tell the classes apart before the body is read, so there it runs only the per-IP limit, which still covers both classes and still refuses an over-limit client before its body is read.
Once the request is read and classified, its first call is charged to the global bucket for a query or to the submission bucket for a submission.
A refusal there is a `429` and gives the per-IP token back, so a refused submission spends no global token and a refused query no submission token.
Such a `429` comes after the body has been read (up to `--max-request-bytes`) and parsed, where without the flag the global `429` leaves before the body is read; that work is bounded by `--max-connections`, the per-client connection cap (`--max-connections-per-ip`) and the per-IP limit.
A submission over its own budget is refused even while the global bucket has tokens, since letting it spend them would hand the read budget back to a submission flood.
A batch then pays for its calls beyond the first from the per-IP and global buckets whatever its class, so while reads have emptied the global bucket a submission batch gets the `429` that a single submission does not; that refusal leaves the first call's tokens spent, as it does without the flag.
With `--redirect-queries` set, the worker receives up to N × `--rate-limit-submissions` submission requests per second from N gateways, plus the [reads kept on the worker](#reads-kept-on-the-worker), which are queries here (see [Sizing for N gateways](#sizing-for-n-gateways)).
Without it the worker also receives every admitted query, mixed batches with their submissions included, so it sees up to N × (`--rate-limit-global` + `--rate-limit-submissions`) requests per second.
Without the flag the edge check is unchanged.

#### Prefix keying

The per-client bucket is keyed on the client's **network prefix**, not its bare
address: the client address has its host bits cleared before the bucket is looked
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

The client identity is the immediate TCP peer, unless that peer is listed in
`--trusted-proxies`: then it is the client the proxy forwarded the request for
(see [Client identity behind a front](#client-identity-behind-a-front)). Behind
a front that is not listed, the peer is that front, so per-IP limiting meters
the front, not the real client. Run the gateway **edge-facing**, behind an L4
front that preserves client source addresses, or behind a front listed in
`--trusted-proxies`: the reference `deploy/k8s/service.yaml` is a ClusterIP
Service, which is not reachable from outside the cluster on its own, so external
clients reach a deployment built from it through a front (for a LoadBalancer
Service, set `externalTrafficPolicy: Local` to keep their addresses).

> The default rates (`100`/s per IP, `3000`/s global) are conservative starting
> points, not tuned figures. Set them to your workers' measured capacity before
> relying on them; they can also be disabled entirely (`0`) if you rate-limit at
> the ingress.

The gateway's own `GET /health`, `GET /ready` and `GET /ready/any` probes are **exempt** from rate limiting, so an orchestrator's liveness/readiness checks keep succeeding under a flood (rate-limiting them would make the orchestrator kill or depool the pod at the worst possible moment).
The probe listener (`--probe-addr`) has no rate limit at all.

Per-IP state is bounded: idle buckets are swept every minute and the number of
tracked IPs is capped, so a wide spread of source IPs cannot grow memory without
limit. Once the table is full (100,000 entries), a newly seen client gets no
bucket of its own: every client the table cannot track shares one overflow
bucket with the per-client rate and burst, so together they get one client's
budget however many prefixes they spread over. One IPv6 `/48` (65,536 `/64`s)
still fits in the table with a bucket per `/64`; the overflow bucket meters only
the prefixes beyond the cap. A client keeps its own bucket only while it is
busy: the sweep drops every bucket that has refilled, and a client that returns
while the table is full shares the overflow bucket too.

> **Residual limitation.** A source holding more than 100,000 prefixes (two
> `/48`s, or one `/47`) can refill the table after each sweep and then keep the
> overflow bucket empty, so clients without an entry get `429` until the next
> sweep while the global bucket looks healthy. If that matters for your
> deployment, limit new source prefixes at the ingress.

#### Client identity behind a front

A front that terminates TCP (an ingress controller, an HTTP proxy, a load
balancer in proxy mode) is the peer of every connection it relays, so without
help every client behind it shares the front's per-IP bucket. Two mechanisms
carry the client address through such a front. Both apply only to a peer listed
in `--trusted-proxies` (comma-separated IPv4 and IPv6 CIDR ranges; a bare
address is a single host):

- **`X-Forwarded-For`**, for an HTTP proxy or ingress. A request from a listed
  peer is attributed to the right-most `X-Forwarded-For` entry that is not
  itself listed: every `X-Forwarded-For` line is read as one comma-separated
  chain, walked from the right, listed addresses are skipped, and the walk stops
  at the first unlisted one. Entries to its left are never read. An unparsable
  entry reached first, a missing or empty chain, or a chain of nothing but
  listed addresses falls back to the peer.
- **PROXY protocol v2** (`--proxy-protocol`), for a TCP load balancer that
  cannot add headers. Every connection from a listed peer must open with a
  binary PROXY protocol v2 header for TCP over IPv4 or IPv6, and the source
  address in it takes the peer's place. A `LOCAL` header (the balancer's own
  health check) keeps the balancer's address. A missing or malformed header, a
  header for another protocol, or one not complete within
  `--header-read-timeout` closes the connection without a response; a peer
  that is not listed is served as plain HTTP. The `X-Forwarded-For` rule then
  runs against the address from the header, so it applies only when that
  address is itself listed (an HTTP proxy behind the balancer).

**What a listed front must do.** The gateway believes a listed front's headers
because it assumes the front wrote them, so check each front before listing it:

- An HTTP front must append the address it received the connection from to
  `X-Forwarded-For` (nginx:
  `proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;`, HAProxy:
  `option forwardfor` without `if-none`), or replace the header with that
  address (nginx: `$remote_addr`), and must set `X-Forwarded-Proto` itself. A
  front that passes the client's own `X-Forwarded-For` through unchanged, as a
  plain nginx `proxy_pass` or HAProxy without `option forwardfor` does, lets
  every client choose its own rate-limit key.
- A TCP front must send a PROXY protocol v2 header on every connection before
  `--proxy-protocol` is turned on, and must keep sending it. If a listed front
  relays a connection without one, the gateway reads the client's own first
  bytes as the header, so the client can name any source address.
- Never list a TCP front without `--proxy-protocol`: it relays the client's own
  `X-Forwarded-For` untouched.

**Spoofing rule.** A client writes every header it sends and every byte its
connection opens with, so the gateway believes `X-Forwarded-For`,
`X-Forwarded-Proto` and the PROXY header only from a listed peer, and only the
entries a listed hop vouched for. From any other peer they are ignored and the
client is the peer. Behind a listed front, a client cannot choose its own
rate-limit bucket as long as every listed front does what the list above
requires. List only the fronts' own addresses: a range that also covers clients
(or other workloads that can reach the gateway) lets them spoof, and a `/0`
range logs a warning at startup. Firewall the gateways so that only the front
reaches them (see [Firewalling](#firewalling)).

An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) matches as IPv4 in the peer, the
chain and the ranges alike, so a dual-stack listener needs no extra ranges. A
range with bits set below its prefix (`10.0.0.1/8`), a prefix wider than its
family, an empty element, or `--proxy-protocol` without `--trusted-proxies` is a
startup error.

Toward the upstream, `X-Forwarded-For` is the peer alone, or the resolved client
followed by the listed peer that vouched for it; the inbound chain is never
copied. `X-Forwarded-Proto` is copied from a listed peer when its right-most
value is `http` or `https`, and is `http` otherwise.

The per-client connection cap (`--max-connections-per-ip`, see [Connection handling](#connection-handling)) keys on the same resolved address at connection level: the PROXY header's source counts against its own prefix, and a connection whose address is listed, a front sending `X-Forwarded-For` or a `LOCAL` health check, is not counted, since its requests belong to many clients.

### Per-route capacity

Every request is classified, with or without `--redirect-queries`.
A request made only of `eth_sendRawTransaction` and `eth_sendRawTransactionSync` calls is a submission; everything else, including a batch that mixes the two kinds, is a query.

- Each class has its own in-flight cap: `--max-inflight-submissions` and `--max-inflight-queries`, 256 each by default (`0` means unlimited; without `--redirect-queries` the query cap is lowered, see below).
  A request whose class has no free slot is answered at once with `503` / `-32009` ("gateway overloaded; retry later") and `Retry-After: 1`, never queued, so a stalled query upstream fills only the query slots and submissions keep theirs.
  The inbound connection cap (`--max-connections`) still bounds both classes together, so keep `--max-inflight-queries` well below it: the difference is what a saturated query route leaves for submissions, and the gateway warns at startup when the query cap is unlimited or not below `--max-connections`.
- Every forward to a worker, of either class, also takes a slot on `--max-upstream-inflight` (100 by default, `0` means unlimited), with the same `503` when none is free.
  Without `--redirect-queries` that is every request, so reads and submissions share the cap.
  The gateway then lowers `--max-inflight-queries` at startup to at most `--max-upstream-inflight` less a fifth of it (80 with the defaults, and an unlimited query cap is lowered too), and logs a warning when it lowers a value you set.
  Unless the worker cap is 1, reads then never hold every worker slot: the fifth held back stays free for submissions.
  The worker cap bounds how much of the worker's shared RPC connection limit one gateway can hold; size it with [Sizing for N gateways](#sizing-for-n-gateways).
- With `--redirect-queries`, a request to the query URL has its own deadline, `--query-request-timeout` (10 s by default, at most `--upstream-request-timeout`, `0` to use that instead), so a query URL that accepts reads and never answers frees each query slot after 10 s rather than 35 s.
  Requests to the worker keep `--upstream-request-timeout`.

A class slot is held from classification until the response body has been sent to the client, or until the request fails, so a query upstream that sends its head and then stalls still holds only query slots.
A client that reads its response slowly holds its query slot for the whole transfer too: with the defaults, 80 slow readers (256 with `--redirect-queries`) put every other read on `503`, while submissions keep their own slots.
On Linux `TCP_USER_TIMEOUT` (`--tcp-user-timeout`, 30 s) ends a read whose client stops acknowledging, and `--max-connection-duration` (10 m) ends one that trickles; on other hosts only `--max-connection-duration` does (see [Connection handling](#connection-handling)), and the per-IP limit and `--max-connections-per-ip` bound how many such reads one client starts.
A worker slot is released when the worker's response head arrives: by then the worker has built the whole response and freed its own connection permit.
`tn_worker_gateway_route_inflight{route}` and `tn_worker_gateway_upstream_inflight` show the slots in use, and every `503` from a cap counts as `tn_worker_gateway_rejections_total{reason="overloaded"}`.

### Request size

`--max-request-bytes` (default 1 MiB) caps the buffered request body; a larger
body is rejected with a JSON-RPC "request too large" error (`413`, `-32003`)
before forwarding.
A declared `Content-Length` over the cap is rejected before any of the body is read.

`--max-inflight-request-bytes` (default 512 MiB) budgets the request-body bytes held across all in-flight requests.
Each request reserves its `Content-Length`, or the whole `--max-request-bytes` when its body is chunked, before the body is read, and returns the reservation once the request has been forwarded or rejected.
A request that does not fit in what is left gets a JSON-RPC error at once (`503`, `-32010`, with `Retry-After: 1`) instead of waiting; its `id` echoes as `null`, since its body is never read.
Clients should retry it after a short backoff.
The `/health`, `/ready` and `/ready/any` probes are exempt, and a rate-limited request costs no budget.
The budget must be at least `--max-request-bytes`, so a chunked request can always fit; `0` disables it.

`--http1-max-buf-size` (default 64 KiB) caps each connection's read buffer.
hyper's own default lets the buffer grow to about 408 KiB, and it stays allocated while the connection is open, so with it a held 1 MiB body cost about 1.42 MiB.
The minimum is 8 KiB, which hyper enforces.
A request's head (request line and headers) must fit in the buffer, or the request is answered `431`; the cap also bounds how much of a streamed response is queued per connection.

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
- **Small enough for memory.** The whole body is buffered before it is forwarded, and `--max-inflight-request-bytes` bounds the bytes all in-flight requests hold at once, whatever the connection count.
  A body with a declared length is read into one buffer of exactly that size, so a held body costs about its reservation.
  Each open connection also keeps a read buffer of up to `--http1-max-buf-size`.
  So peak request memory is about `--max-inflight-request-bytes` + `--max-connections` × `--http1-max-buf-size` + 64 MiB for the process baseline and response streaming; keep that under the container's memory limit.
  With the defaults that is 512 MiB + 500 × 64 KiB + 64 MiB, about 608 MiB, which the 1Gi limit in the reference manifest (`deploy/k8s/deployment.yaml`) covers.
  In a test process that also ran the client and the upstream, 128 parallel 1 MiB bodies against a 64 MiB budget peaked 81 to 83 MiB above the idle process over five runs, about 1.3 times the budget.
  That excess includes the read buffers of the client's, the gateway's and the upstream's connections, whose number the test scales with the budget, so the gateway's own share was not measured separately.
  Each extra connection adds at most `--http1-max-buf-size`.
  Raising `--max-request-bytes` does not raise the memory bound, but it makes the budget cheaper to exhaust: a request reserves its declared length (or the whole cap when chunked) from its head and holds it until the request deadline (`--upstream-request-timeout` + `--header-read-timeout`, 45 s by default) even if no body byte arrives.
  About `--max-inflight-request-bytes` / `--max-request-bytes` such idle requests (512 at the defaults, 35 at a 15 MiB cap) make every other body-carrying request fail with `-32010`.
  On an internet-facing gateway keep that quotient at or above `--max-connections` (the gateway warns at startup otherwise); the per-client connection cap (`--max-connections-per-ip`) bounds how many of them one client can hold.
  The budget is shared by every client and no client prefix's share of it is capped, so below that point a few clients together, or one client when the quotient is at or below `--max-connections-per-ip`, can hold all of it with idle requests.
  If the limit has to stay lower, lower the budget, for example `--max-inflight-request-bytes 268435456` (256 MiB) for about 352 MiB.
  A 256 MiB budget is exhausted by 256 idle requests, so on an internet-facing gateway lower `--max-connections` to 256 with it, which brings the figure to about 336 MiB.

### Batches

A JSON-RPC batch (a JSON array of calls) is held to the same budget as the calls it carries.

- **Length cap.** `--max-batch-len` (default 50, `0` means unlimited) caps the calls in one batch.
  The effective cap is the smaller of `--max-batch-len` and the smallest enabled rate-limit burst, and startup logs a warning when a burst lowers it.
  A longer batch is refused whole with `413` / `-32003`, the message "batch too long" and a `null` id, before it is screened or forwarded, whichever upstream it would go to.
  The gateway stops counting one element past the cap, so an oversized batch costs no more than the cap to read.
- **One token per call.** A batch of K calls costs K rate-limit tokens, from the per-IP and the global bucket alike.
  The first token is taken before the body is read (with `--rate-limit-submissions`, only its per-IP part, see [Rate limiting](#rate-limiting)); the other K - 1 are charged once the body is read and its calls counted, and a refusal is a `429` / `-32006` with a `null` id.
  The K - 1 come from the per-IP bucket first, the same client's bucket the first token came from (behind a front listed in `--trusted-proxies`, the forwarded client's, see [Client identity behind a front](#client-identity-behind-a-front)), or the shared overflow bucket when the per-client table is full, so a client over its own limit spends none of the global budget on them; a global refusal refunds them, and each bucket is charged all or nothing.
  They come from the global bucket whatever the batch's class, with `--rate-limit-submissions` set too (see [Rate limiting](#rate-limiting)).
  A batch needs K tokens at once and a bucket never holds more than its burst, which is why the length cap stops at the smallest burst: a longer batch gets the final `413` rather than a `429` that no retry could satisfy.
  The defaults (200 per IP, 6000 global, against 50) leave the cap at 50.
- **Every submission is screened.** In a batch made only of `eth_sendRawTransaction` and `eth_sendRawTransactionSync` calls, each element is screened like a single call (see [Transaction screening](#transaction-screening)).
  The first refused element answers for the whole batch with `-32007` or `-32008` and that element's `id`, and nothing is forwarded.
  A submission with named params is not screened; the worker validates it.
- **Mixed batches are unchanged.** A batch that mixes submissions with other calls is not screened and is forwarded whole: to the `--redirect-queries` URL when it is set (see [Query redirect](#query-redirect)), to the worker otherwise.

Every element counts towards the cap and the tokens whatever its shape: an element that is not a call object is still one element, and counts as another call when the batch is classified.
The gateway reads a body as the worker's JSON-RPC server does, leading whitespace included, so no prefix can hide a batch from the count.
A batch the gateway cannot read to its end is refused whole with `400` / `-32600` and a `null` id, before it is charged or forwarded, so a batch's length is never unknown.
That covers bytes after the batch and a truncated batch, which the worker refuses whole anyway, and an element holding a value the gateway's JSON reader refuses although it is valid JSON, such as a number out of range (`1e400`) or an unpaired surrogate escape (`"\udc00"`), which the worker would skip while running the rest of the batch.

### Transaction screening

Every `eth_sendRawTransaction` and `eth_sendRawTransactionSync` call object, alone or in a batch made only of submissions, is decoded far enough to reject, at the edge, the two cases the worker would also reject: an undecodable payload, and a transaction type the network does not accept (EIP-4844 blob and EIP-7702 set-code transactions).
This saves a wasted upstream round-trip.
The decode uses the same pooled wire format the worker's RPC accepts and never recovers the signer, so it cannot reject a transaction the worker would accept.

- The method name is read as the worker reads it, unicode escapes included.
- The raw transaction is the first positional param, written as a hex string or as an array of byte values, the two shapes the worker accepts.
- In a batch, the first refused element answers for the whole batch, with that element's `id`, and nothing is forwarded.
- A submission with named params (`"params": {...}`) is not screened; the worker reads and validates those itself.
- Only call objects are screened: a batch element written in positional form (a JSON array) counts as another call and is not screened.

Mixed batches and every other method the [method policy](#method-policy) allows are forwarded unchanged and validated upstream: by the worker, or, with `--redirect-queries`, by the query URL for everything the worker does not serve.

### Method policy

Without `--redirect-queries` every call goes to the worker, so the gateway refuses the namespaces a validator should not serve to the public before forwarding.
A call whose `method` starts with an entry of `--denied-method-prefixes` (default `tn_,debug_,trace_,admin_`) gets `403` / `-32011` "method not allowed through this gateway" with its request `id`, and never reaches the worker.
The node says a validator should not expose the `tn` namespace publicly; `debug_`, `trace_` and `admin_` cover what an operator may have enabled on the worker for its own use.
A batch that holds a refused call is refused whole, with one error object whose `id` is `null`, and no part of it is forwarded.
The check runs after the length checks and before anything else is charged: a refused call costs only the one rate-limit token the gateway takes before reading the body, none of a batch's other tokens, and no in-flight slot.

Matching is case-sensitive and anchored at the start of the name, as the worker matches method names: under `tn_`, neither `TN_info` nor `eth_tn_x` is refused, and the worker answers both as unknown methods.
The gateway reads method names the way the worker's server does: leading whitespace, form feed included, is skipped, a name spelled with unicode escapes is unescaped before the match, and a batch element that is not an object does not hide the calls around it.
A body that does not start with `{` or `[` once leading whitespace is skipped, or whose `method` is not a string, is forwarded, and the worker rejects it on its own.
A batch the gateway cannot read to its end is refused as unreadable before the policy runs, with `400` / `-32600` (see [Batches](#batches)), because the worker would still run the calls around the element it cannot read.
A single call that starts with `{` but that the gateway cannot read, such as one holding a number out of range (`1e400`) or an unpaired surrogate escape, is refused like a denied call rather than forwarded unchecked.
The names are read in the same single pass over the body that counts, screens and routes it; with a deny list in force that pass reads every call, where it would otherwise skip a single call that names no submission or pool read, and the rest of a batch once the batch mixes submissions with other calls.

Entries are separated by commas and surrounding whitespace is ignored.
An empty value (`--denied-method-prefixes=` or an empty `WORKER_GATEWAY_DENIED_METHOD_PREFIXES`) allows every method.
With `--redirect-queries` set the list is not applied: routing is unchanged, and the query URL decides what it serves.
Startup logs the policy in force on the "edge protections configured" line (`method_policy`), and warns when the list is empty and no redirect is set.
Refused calls are counted in `tn_worker_gateway_rejections_total` with reason `method_not_allowed`; they are logged at debug only, since the method is the client's choice rather than a fault the operator can fix.

## Query redirect

On a validator, set `--redirect-queries <URL>` so that the worker receives transaction submissions and only the reads that must see its pool.
With the flag set, `eth_sendRawTransaction` and `eth_sendRawTransactionSync` go to the first ready worker, and so do the [reads kept on the worker](#reads-kept-on-the-worker); every other call goes to the URL, typically a public RPC.
Method names match exactly and case-sensitively, after unicode escapes are decoded, as the worker reads them.
Everything else counts as a query: `eth_sendTransaction` (no node configures a signer, so the worker could only refuse it), `tn_*` and `debug_*` calls, and any body the gateway cannot read as submissions, such as one that is not JSON, an empty batch, a `method` that is not a string, or a single call with bytes after it.
A batch the gateway cannot read to its end is refused rather than routed (see [Batches](#batches)).

The URL may be `http` or `https`, as worker URLs may.
An `https` URL needs the system CA certificates, which the image installs; `--upstream-ca-cert` and the client certificate apply to it as well (see [TLS to workers](#tls-to-workers)).
The URL must not point at the gateway itself or at a worker's RPC or readiness host and port; an IPv4-mapped IPv6 literal such as `[::ffff:10.0.0.7]` counts as the IPv4 address it names.
A URL on a worker's host at another port logs a warning at startup, and so does plain `http` to a host that is not a loopback or private address.
A value that is not a valid URL stops startup with an error that says what is wrong without repeating the value, which can carry an API key.

A batch goes to the worker only when every element is a submission.
A batch that mixes submissions with other calls, an element that is not a call object included, goes, whole, to the query URL; otherwise a client could put one submission in front of any number of reads and push them all onto the validator.
The public RPC accepts submissions too, so the client loses nothing, but a submission inside a mixed batch reaches the network through the public RPC rather than this validator.
`tn_worker_gateway_mixed_batches_total` counts these batches; splitting a batch and merging the two responses is not implemented.

Routing happens after the [transaction screen](#transaction-screening), so an undecodable submission is still refused at the gateway.
Calls sent to the query URL carry `X-TN-Gateway-Redirect: 1`, `X-Forwarded-For` and `X-Forwarded-Proto`, but not `X-TN-Gateway`, so a public RPC behind a gateway of its own does not reject them as a loop.
A gateway with `--redirect-queries` set answers an inbound request carrying `X-TN-Gateway-Redirect` with `508` / `-32004`, which catches a query URL that leads back to a redirecting gateway; a gateway without the flag forwards such a request normally.
The gateway follows no HTTP redirects: a `3xx` from either upstream is passed to the client with its status, without its `Location` header (and, unless its body is JSON, with the `-32012` error as its body), so a query upstream cannot bounce a read onto the worker.
Requests to either upstream carry a `tn-worker-gateway/<version>` user agent.

`/ready` still means "this gateway can take submissions"; `/ready/any` reports whether it can serve any route (see [Gateway endpoints](#gateway-endpoints)).
The query URL gets no readiness gate and no fallback: when it fails, the client gets `502` or `504` and the call is never retried on the worker, which would put the read load on the validator just when the public RPC is struggling.
The gateway does probe it, with one `eth_chainId` call per `--readiness-poll-interval` carrying `X-TN-Gateway-Redirect: 1` (and the [query upstream header](#query-upstream-header) when one is set) and bounded by `--readiness-poll-timeout`, but only to report read availability on `/ready/any`; the result never changes routing.
A `2xx` reply whose JSON body has a `result` member counts as up; anything else, including a reply over 64 KiB, counts as down.
Like a worker's readiness, the probed state changes only on a run of `--readiness-success-threshold` answered probes or `--readiness-failure-threshold` failed ones in a row.
A query URL that accepts reads and never answers holds each one for `--query-request-timeout`, and reads beyond `--max-inflight-queries` get `503` / `-32009` at once; submissions have their own slots and keep reaching the worker (see [Per-route capacity](#per-route-capacity)).

| Worker | Query URL | `/ready` | `/ready/any` | Submissions and reads kept on the worker | Other calls |
| --- | --- | --- | --- | --- | --- |
| up | up | `200` | `200` | worker | query URL |
| down | up | `503` | `200` | `503` / `-32000` | query URL |
| up | down | `200` | `200` | worker | `502` / `-32001` or `504` / `-32002`, no fallback |
| down | down | `503` | `503` | `503` / `-32000` | `502` / `-32001` or `504` / `-32002`, no fallback |

Reads answered by the query URL come from a node that has not seen this validator's transaction pool, every one of them reaches that node from the gateway's address, and each carries the client's address in `X-Forwarded-For`; see [Split routing](#split-routing) before advertising the endpoint.
`/ready` reports only whether submissions can be served, so a front that drops a gateway on its `503` (a health-checked DNS record, a load balancer) also stops its reads while the worker is down; probe `/ready/any` instead, as the reference `readinessProbe` does, to keep the gateway in rotation while it can serve reads.

The reverse topology, a gateway that sends submissions to a validator's worker and every other call to an observer's RPC, can be expressed with the same two settings, but it is not a supported deployment yet.

### Reads kept on the worker

The query URL answers from a node that has not seen this validator's transaction pool, because the worker's pool does not gossip its transactions (`propagate: false` in `crates/tn-reth/src/txn_pool.rs`).
A client that sends several transactions in a row and asks that node for its next nonce in between gets the nonce from before its own submissions, and reuses it.
So with `--redirect-queries` set, two kinds of read go to the worker instead of the query URL:

- `eth_getTransactionCount` whose second parameter is the string `"pending"`.
- `eth_getTransactionByHash` and `eth_getTransactionReceipt` whose first parameter is the hash of a submission this gateway forwarded to the worker within `--recent-submission-ttl` (default `60s`).

The rule is exact:

- Only a single call counts.
  A batch keeps the classification above: it goes to the worker only when every element is a submission, so a batch holding one of these reads goes, whole, to the query URL.
- Method names and the `"pending"` tag match exactly and case-sensitively, after unicode escapes are decoded, as the worker reads them.
  A block parameter given any other way, such as `{"blockNumber": "pending"}`, or left out (which means `latest`) sends the nonce read to the query URL.
- Every other read at `pending`, such as `eth_call`, `eth_getBalance` and `eth_estimateGas`, still goes to the query URL and can lag the worker's pool.
- A hash is remembered only for a single `eth_sendRawTransaction` that passed the [transaction screen](#transaction-screening) and that the worker accepted: a `2xx` answer carrying a JSON-RPC `result`.
  The worker reports a rejected transaction as a JSON-RPC `error` inside a `200`, so the gateway reads the worker's short answer to a single submission whole, up to the request's length plus 4 KiB, and passes it on unchanged; every other answer streams through.
  An answer past that cap, or one whose body fails or runs past `--upstream-request-timeout`, is lost: the client gets `502` / `-32001`, and the forward counts as `body_failed` (see [Behaviour on failure](#behaviour-on-failure)).
  A submission inside a batch, an `eth_sendRawTransactionSync` (which returns the receipt itself), and a submission the worker rejected, with an HTTP error status or a JSON-RPC `error`, are not remembered.
- The gateway remembers at most 10,000 hashes and forgets the oldest first, so a flood of submissions can push a hash out early, and its lookups then go to the query URL.
- Each gateway remembers only the submissions it forwarded itself, so a lookup that a load balancer sends to another gateway goes to the query URL.
- With more than one upstream worker, each worker has its own pool and these reads go to whichever worker is first ready when they arrive: after a failover, or when a preferred worker comes back, a read can reach a worker that did not take the submission, and it gets the answer the query URL would give.
- `--recent-submission-ttl 0` turns the hash routing off; nonce reads at `pending` still go to the worker.

These reads are gated on worker readiness like submissions: with no worker ready they get `503` / `-32000` and are not sent to the query URL, which could only give the stale answer.
They count on `tn_worker_gateway_routed_requests_total{route="worker"}`, and the per-IP and global rate limits bound what they cost the validator.
They are reads for [Per-route capacity](#per-route-capacity): each holds a `--max-inflight-queries` slot, and a `--max-upstream-inflight` slot like a submission, so while a stalled query URL holds every query slot they are refused with `503` / `-32009` as well.
That bound assumes the worker runs with its default `--rpc.pending-block none`, where a nonce read at `pending` is a state lookup plus a pool lookup; with `full`, the worker may build a pending block from its pool to answer one, which costs more than a submission.

### Query upstream header

Set `--redirect-queries-header "<Name>: <value>"` to send one header, typically the public RPC's API key, on every call to the query URL.
It goes to the query URL only and never to a worker, and it lets the public RPC meter this gateway by key instead of by the one address that all redirected reads share.
Every client of the gateway uses the key: each call that is not a submission or one of the [reads kept on the worker](#reads-kept-on-the-worker), whatever its method and parameters, reaches the query URL carrying the header.
The gateway's own probe of the query URL carries it too, so a query URL that refuses calls without the key is not reported down on `/ready/any`.
Issue a key limited to what this endpoint should serve publicly (no archive, `debug_*` or `trace_*` access, no tier or billing it does not need), and size its quota for all of the gateway's clients together.
For a secret, use `--redirect-queries-header-file <path>` instead: the file holds the same line (a mounted secret, for example), which keeps the value out of the process arguments and environment, and whitespace around the line, a trailing newline included, is trimmed.
Either flag requires `--redirect-queries`, and setting both is a startup error.
A line that is not one valid `Name: value` header also stops startup, with an error that names the problem but never the value.
On a command line, quote the whole line: unquoted, the shell splits it at the space after the colon, and the argument parser rejects the value as a stray argument and prints it in that error, before the gateway's own checks run.

The value is never logged: the startup line records only whether a header is configured, debug output of the gateway's settings and state prints `<redacted>` in its place, and `--help` hides the environment variable's value.
Use an `https` query URL with a header; over plain `http` the header crosses the network in clear.

### Chain-id check

At startup a gateway with `--redirect-queries` sends `eth_chainId` to the first configured worker and to the query URL, the second call carrying the header.
When the two chain ids differ it logs a warning with both, since reads would then describe a different chain from the one submissions reach; when they match it logs at info.
The check runs in the background, so it never delays the listener, and `--upstream-request-timeout` bounds each call.
When either call fails (the upstream is down, answers with an error status, or returns no hex chain id), the gateway logs the reason at info and serves anyway.
The check runs once; a query URL that changes chains later is not detected.

## Gateway endpoints

- `GET /health`: liveness, always `200 OK` while the process runs.
- `GET /ready`: readiness, `200` when at least one upstream is ready, else
  `503` with `{"ready": false}`; during the shutdown delay, `503` with
  `{"ready": false, "draining": true}`.
- `GET /ready/any`: read availability, `200` when the gateway can serve any route, else `503`.
  It answers `200` when `/ready` would, or when `--redirect-queries` is set and the gateway's probes find the query URL up (see [Query redirect](#query-redirect)); without `--redirect-queries` it is exactly `/ready`.
  The body is `{"ready": bool, "submissions": bool, "queries": bool}`, where `submissions` is what `/ready` reports outside shutdown and `queries` whether the query URL is up.
  During the shutdown delay it is `503` with `"ready": false` and `"draining": true` added, like `/ready`.
- `POST` to any other path: JSON-RPC, forwarded to a ready upstream worker unless the [method policy](#method-policy) refuses it, or, with `--redirect-queries`, to the query URL unless it is a submission or one of the [reads kept on the worker](#reads-kept-on-the-worker).
  The path is not forwarded, so `POST /` and `POST /anything` are the same call.
- `OPTIONS`, with `--cors-allowed-origins` set: a CORS preflight, answered by the gateway and never forwarded, on every path but `/ready/any` (see [CORS](#cors)).
- Any other request gets the `405` / `-32600` error envelope, or the `429` when it is over a rate limit (see [Behaviour on failure](#behaviour-on-failure)), and reaches no upstream.
  On `/health`, `/ready` and `/ready/any` that means every method but `GET` and `HEAD`, `POST` included: the rate limiter exempts the probe paths, so JSON-RPC sent there is refused rather than proxied past the limits.

The three probes answer on `--listen-addr`, exempt from rate limiting but inside the client connection cap.
With `--probe-addr <addr>` set, they also answer on a listener of their own that serves nothing else (a request for any other path gets `404`), with no connection cap, no rate limit and no body limit.
Each connection to it serves one request, whose head must arrive within the header read deadline and be at most 8 KiB; a larger head gets `431`.
A flood that holds every client connection then cannot make the probes time out, as it can on the client port, where a timed-out liveness probe gets a healthy pod restarted.
Point orchestrator probes at it, and keep it reachable from the orchestrator only, since it has no connection cap.
It keeps answering through the shutdown delay, so probes aimed at it see the draining `503`, and stops accepting when the client listener does (see [Graceful shutdown](#graceful-shutdown)).
`--probe-addr` must not collide with `--listen-addr`; the gateway refuses to start if it does.

## Behaviour on failure

A request that reaches the gateway's handlers receives a well-formed JSON-RPC 2.0 error (never a bare connection reset) when the gateway cannot serve it.
The error carries the HTTP status in the table below and a JSON-RPC error object as its body (`Content-Type: application/json`); there is no option to answer gateway errors with `200`.
A client library that treats every non-`2xx` status as a transport failure reports these as HTTP errors, so read the body whatever the status.
The request `id` is echoed when it can be recovered from the body.
It is `null` for a batch (see below), and always `null` on a `405`, `408`, `413`, `429`, the `503` for an exhausted byte budget (`-32010`) or the `400` for an unreadable body, which the gateway answers without recovering the id.
An `id` that is an array, an object, a boolean or a string over 256 bytes is echoed as `null`.

The exceptions are a request whose head hyper, the HTTP library underneath, cannot parse (below), a connection that does not finish its request head within `--header-read-timeout` (closed without a response), a connection over `--max-connections-per-ip`, which is closed before any request is read, and an exchange still running when the connection-lifetime grace or `--graceful-shutdown-timeout` ends (see [Connection handling](#connection-handling) and [Graceful shutdown](#graceful-shutdown)).
Hyper answers an unparseable request head itself, before the request reaches the gateway's service, with a bare `400` (malformed request line or headers), `414` (request target longer than 65,534 bytes) or `431` (request head too large, or too many headers), without a JSON-RPC body or `X-Content-Type-Options: nosniff`; the gateway cannot intercept these.

| Condition | HTTP | JSON-RPC error code |
| --- | --- | --- |
| No upstream ready | `503` | `-32000` |
| Upstream unreachable | `502` | `-32001` |
| Upstream request timed out | `504` | `-32002` |
| Request body too large | `413` | `-32003` |
| Batch longer than `--max-batch-len` | `413` | `-32003` |
| Proxy loop detected | `508` | `-32004` |
| Request deadline exceeded | `408` | `-32005` |
| Rate limit exceeded | `429` | `-32006` |
| Raw transaction undecodable | `400` | `-32007` |
| Unsupported transaction type (EIP-4844 blob or EIP-7702 set-code; the message names which) | `400` | `-32008` |
| Request body unreadable (client aborted), or a batch the gateway cannot read to its end | `400` | `-32600` |
| Method other than `POST` (other than `GET` or `HEAD` on `/health`, `/ready` and `/ready/any`) | `405` | `-32600` |
| In-flight cap reached (overloaded) | `503` | `-32009` |
| In-flight request byte budget exhausted | `503` | `-32010` |
| Method refused by the [method policy](#method-policy) (no `--redirect-queries`) | `403` | `-32011` |
| Upstream answered an error status without a JSON body | the upstream's | `-32012` |

The two capacity `503`s, `-32009` and `-32010`, carry `Retry-After: 1`: a slot or a byte reservation frees as soon as a request in flight finishes, so a client or proxy that honours the header waits a second instead of retrying at once.

An upstream error answer with a JSON body (`application/json` or any `+json`
subtype) is the upstream's own JSON-RPC error and passes through unchanged:
status, body and content type. Any other non-2xx answer (a worker's `403`,
`415` or `429` text/plain, or a `3xx`) keeps its status but has its body
replaced by the `-32012` error, which echoes the request `id`. The
`upstream_error` outcome also covers refusals a client can cause (a request
whose `Content-Type` is not JSON gets the worker's `415`), so it is not by
itself a worker-saturation signal.

An upstream body that fails after the status and headers have gone out (the
upstream closes or resets mid-stream) cannot be turned into an error any more:
the client gets the upstream's status with a truncated body (a chunked
response without its final chunk), and the gateway logs the failure at debug
and counts it as `result="body_failed"` in
`tn_worker_gateway_routed_requests_total`.
The worker's answer to a single submission whose hash the gateway would remember is read whole before any of it goes out (see [Reads kept on the worker](#reads-kept-on-the-worker)), so a failure there still becomes an error: the client gets `502` / `-32001`, and the forward counts as `body_failed` only, without a `forwarded`.

The `408` message says whether the request may have been delivered.
"request did not complete within the gateway's deadline" means the deadline passed before the gateway started sending the request upstream, so no upstream received it and it is safe to send again.
"request did not complete within the gateway's deadline; the request may have reached the upstream" means it passed after that point, so a submission may already be on its way into the network: look the transaction up by hash before sending it again.
An upstream's own `408` is not rewritten; the client gets it with the upstream's status and body.
Nor does the deadline cut a request whose upstream has already answered: the client gets that answer.

A `504`, and a `502`, do not mean the request was not delivered: the gateway may already have sent it when the upstream timed out or the connection failed.
With the default timeouts a slow worker produces the `504`, not the `408`.
Treat either like the second `408` message and look a submission up by hash before sending it again.

A gateway error answering a batch (a JSON array) is one error object with `id: null`, not an array of per-call errors.
The gateway refuses a batch only as a whole: it never splits one, and it decides a `405` or `429` before reading the body.
That is the shape JSON-RPC 2.0 prescribes when a batch cannot be taken as a whole (invalid JSON or an empty array).
A batch the upstream answers comes back as the upstream sent it.

The gateway's own codes sit in the JSON-RPC server-error range
(`-32000..=-32099`), which upstream servers also use for their errors;
disambiguate by HTTP status and message, not by code alone (`-32600` is the
spec's standard "Invalid Request" code, used for both the unreadable-body
`400` and the `405`).

## Graceful shutdown

On SIGTERM (or ctrl-c) the gateway shuts down in two phases.

1. Shutdown delay (`--shutdown-delay`, off by default): the gateway keeps accepting and serving new connections, but `/ready` and `/ready/any` answer `503` with `"ready": false` and `"draining": true`.
   Fronts that act on either probe (DNS health checks, external load balancers) see the `503` and stop routing new clients here before the listener closes, provided they act within the delay.
   In Kubernetes the Service drops a terminating pod from its ready endpoints as soon as the pod is deleted, whatever its readiness probe says; there the delay covers the time that removal takes to reach kube-proxy and load balancers, as a `preStop` sleep would.
   The readiness poller keeps running, so requests accepted during the delay are routed on live upstream readiness.
2. Drain: the gateway stops accepting new connections and drains in-flight requests, up to `--graceful-shutdown-timeout`.
   Each connection finishes the exchange in flight and closes; an idle one closes at once.
   Requests still running after the deadline are force-closed.

The probe listener (`--probe-addr`) follows the same phases: it keeps answering through the delay and stops accepting at the same moment as the client listener.
With the delay at `0` the first phase is skipped: the listeners close as soon as the signal arrives, and a front that has not noticed yet sees refused connections.
Set the delay to at least the time your fronts outside Kubernetes take to act on a failing `/ready` or `/ready/any` (health-check interval times unhealthy threshold, plus the record TTL for DNS).
A request accepted just before the listener closes can run for the whole request deadline, `--upstream-request-timeout` plus `--header-read-timeout` (45s at the defaults), so the `30s` default drain can cut a slow `eth_sendRawTransactionSync` short; set the drain timeout to that deadline if those answers must survive a shutdown.
Keep the delay plus the drain timeout plus about 1s inside the orchestrator's termination grace period.
The reference manifest uses a 5s delay, a 45s drain and a 55s grace period; the 5s covers Kubernetes endpoint removal only (see `deploy/README.md`).

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
| `tn_worker_gateway_inflight_requests` | gauge | | Proxied requests from their buffered body until the relayed response's body has finished streaming (or the gateway's own error); the request body read and requests refused before the proxy handler (by the rate limiter or the byte budget) are not counted. The intended autoscaling signal. |
| `tn_worker_gateway_requests_total` | counter | `outcome` (`forwarded` / `rejected` / `upstream_error`) | Proxied requests by terminal outcome; `upstream_error` is a request whose upstream answered an error status without a JSON body. |
| `tn_worker_gateway_rejections_total` | counter | `reason` | Rejected and `upstream_error` proxied requests, broken down by reason (the conditions in the failure table above). |
| `tn_worker_gateway_request_duration_seconds` | histogram | | Time from a proxied request's buffered body to the end of its response body (or the gateway's own error); not end-to-end, as it leaves out the request body read and requests refused before the proxy handler (by the rate limiter or the byte budget). |
| `tn_worker_gateway_upstream_ready` | gauge | `worker_id`, `upstream` (the RPC URL's origin, `scheme://host:port`) | Per-worker published readiness, after the poll thresholds and passive health (`1` ready, `0` not-ready); nodes whose workers share an id keep separate series. |
| `tn_worker_gateway_routed_requests_total` | counter | `route` (`worker` / `query`), `result` (`forwarded` / `upstream_error` / `unreachable` / `timeout` / `body_failed`) | Forward attempts by route, with their result; `body_failed` counts a forwarded response whose body then failed mid-stream, in addition to its `forwarded`, and a worker's answer to a submission that was lost while the gateway read it whole, without one (see [Reads kept on the worker](#reads-kept-on-the-worker)); the reads kept on the worker count under `worker`. A forward cut short by the whole-request deadline has no result here; it shows as `tn_worker_gateway_rejections_total{reason="request_timeout"}`. |
| `tn_worker_gateway_mixed_batches_total` | counter | | Batches sent whole to the `--redirect-queries` URL because they mixed submissions with other calls (an element that is not a call object counts as another call). |
| `tn_worker_gateway_route_inflight` | gauge | `route` (`submission` / `query`) | Requests holding a slot on their class's in-flight cap. |
| `tn_worker_gateway_upstream_inflight` | gauge | | Requests this gateway is forwarding to the worker, under `--max-upstream-inflight`. |
| `tn_worker_gateway_inflight_request_bytes` | gauge | | Request-body bytes held against `--max-inflight-request-bytes` by requests in flight. |
| `tn_worker_gateway_dns_lookups_total` | counter | `result` (`hit` / `miss` / `error` / `rejected`) | Upstream name resolutions: answered from the cache (fresh or stale), lookup started, lookup failed, or refused because every lookup slot was busy. A request that waits on a lookup already running for its host adds nothing. A stale answer whose background refresh is refused counts as both `hit` and `rejected`, and the request is still answered. The proxy and the readiness poller share the series. |
| `tn_worker_gateway_connections_refused_total` | counter | `reason` (`per_ip_cap`) | Inbound connections closed before any request is read, by reason (`per_ip_cap`: the connection's client prefix already held `--max-connections-per-ip` connections; see [Connection handling](#connection-handling)). |

The gateway's own `/health`, `/ready` and `/ready/any` probes are not proxied and
are excluded from these series, so they reflect real client load only.
A request to `/health`, `/ready` or `/ready/any` with a method other than
`GET` or `HEAD` is refused with the `405`, counted as a `method_not_post`
rejection, and is not rate-limited.
`tn_worker_gateway_connections_refused_total` is the exception: it counts connections before any request is read, so a probe connection to the client listener refused at the per-client cap is counted with the rest (the `--probe-addr` listener has no per-client cap).

Per-request warnings (a proxy or redirect loop, a batch the gateway cannot
read to its end or one over `--max-batch-len`, a screened transaction, no
ready upstream, an oversized body, a worker's answer to a submission that
could not be read whole, and, per route, a forwarding failure or an
upstream error answer) are throttled per call site, so neither clients nor a
failing upstream can turn request volume into log volume: the first is logged,
then at most one line per 10 s, whose `suppressed` field counts the warnings
skipped since the previous line. A burst that stops is summarized within 20 s
by a line that names its call site (`site`) and the count it suppressed. The
counters above still count every request, including the rejections that log
nothing per request (`405`, `429`, `408`, `-32010`, `-32011` and an aborted body). The scrape also
carries a `tn_info{version}` build gauge and process metrics; the process
metrics render under a `reth_` prefix (`reth_process_*`), an artifact of the
shared recorder's reth-compatible naming.

### Access log

Pass `--access-log-sample <rate>` (or `WORKER_GATEWAY_ACCESS_LOG_SAMPLE`) to write one `info` line per sampled request on the client listener under the tracing target `gateway::access`.
The rate is a fraction from `0` to `1`: `1` logs every request, `0.01` one in a hundred, and the default `0` turns the log off.
A value outside that range, or one that is not a number, stops the gateway at startup.

Sampling is deterministic and uses no random source.
The gateway counts the requests and logs the `n`th one when `floor(n * rate)` is greater than `floor((n - 1) * rate)`, so exactly `floor(n * rate)` of the first `n` requests are logged, evenly spread: at `0.5` every second request, at `0.25` every fourth.
The probes are neither counted nor logged: `/health` and `/ready` are exempt by path, `/ready/any` is outside the log, and the `--probe-addr` listener has no access log.
The log sits outside every layer that answers a request itself, so such a request is logged like any other: a `429` from the rate limiter, a `413` or `503` from the request-size checks, a `405` for a method other than `POST`, a `408`, and, with `--cors-allowed-origins` set, a CORS preflight.

| Field | Meaning |
| --- | --- |
| `client` | The resolved client address (the peer, or behind a `--trusted-proxies` front the client it forwarded for; see [Client identity behind a front](#client-identity-behind-a-front)), never the front's own, masked to the per-IP rate limiter's prefix (`--rate-limit-per-ip-v4-prefix`, `--rate-limit-per-ip-v6-prefix`), so a line names a client the way the limiter meters it. At the default IPv4 `/32` this is the full address. |
| `class` | `submission` for a single `eth_sendRawTransaction` or `eth_sendRawTransactionSync` call, `batch` for any JSON-RPC batch whatever it holds, `query` for any other request body, and `unknown` when the gateway answered before reading the body. It does not depend on `--redirect-queries`. |
| `route` | `worker` or `query` for a request forwarded to that upstream, whatever the forward's result, a timeout included; `rejected` for a request the gateway answered itself (rate limit, request size or in-flight byte budget, a method other than `POST`, a CORS preflight, a request timeout before the forward started, loop marker, unreadable or over-long batch, denied method, transaction screen, submission budget, in-flight cap, no ready worker). |
| `status` | The HTTP status returned to the client, or `499` when the connection closed before the gateway answered (client disconnect, `--max-connection-duration`, shutdown). |
| `latency_ms` | Whole milliseconds from the request reaching the gateway until the response head is ready, or until the connection closed for a `499`; a streamed response body may still be in flight. |
| `bytes_in` | The request body's length in bytes, or, when the gateway answered before reading the body, the `Content-Length` the client declared: unverified, so a client can inflate it, and `0` for a chunked body that declares none. |

A line never carries the request URL, a header value, the body, the params, or a method name beyond its class.
Under `--log-format json` a line is one object with `target` `gateway::access` and the same fields under `fields`, next to `message` `request`.
With the default `--log-filter info` the lines join the gateway's normal output; a filter such as `warn,gateway::access=info` keeps them while quieting the rest.
The line rate follows the raw request rate, ahead of the rate limiter: it is `rate` times whatever clients send, `429`s included, with no ceiling of its own, so size the rate for the request rate a flood can reach at the listener, not for `--rate-limit-global`.
One client is no exception: its `429`s are logged too, so its lines are bounded by how fast it can send on its `--max-connections-per-ip` connections, not by its rate limit.
Lines are written synchronously to standard output, so a log consumer that stops reading stalls request handling, `/health` and `/ready` included, once the pipe fills; keep the rate low on a public edge and send standard output to a consumer that keeps up.

## Operator guidance

The [production-readiness review](docs/production-readiness.md#operator-guidance) gives the findings behind each rule below.

### Validators: always redirect queries

Set `--redirect-queries` on every gateway in front of a validator.
Without it, reads reach the worker, and slow reads through one gateway can use up the worker's RPC connection limit for every gateway.
The [method policy](#method-policy) refuses the `tn_*` calls the node says a validator should not serve publicly, but every other read still lands on the validator.
Point it at an `https` public RPC for the same chain.

### Sizing for N gateways

Every limit is per process, so the worker sees the sum over all gateways.

- **Rate.** The worker receives up to N × `--rate-limit-global` calls per second, where N is the largest number of gateways that can run at once: the HPA's `maxReplicas` if you install it (10 in the reference manifest).
  Size `--rate-limit-global` as the worker's budget divided by that N.
  With `--rate-limit-submissions` and `--redirect-queries` both set, the worker receives submissions, up to N × `--rate-limit-submissions` requests per second, plus the [reads kept on the worker](#reads-kept-on-the-worker), which count against `--rate-limit-global` like every other read; size that flag the same way.
  With `--rate-limit-submissions` set and no redirect, the worker receives both budgets, so keep N × (`--rate-limit-global` + `--rate-limit-submissions`) within the worker's budget.
- **Worker connections.** Each gateway sends at most `--max-upstream-inflight` requests to the worker at once, so N gateways hold up to N × `--max-upstream-inflight` of the worker's `--rpc.max-connections` (500 by default), and the worker answers `429` to everything over its limit.
  Keep N × `--max-upstream-inflight` below the worker's `--rpc.max-connections`.
  The reference manifest does not on its own: its HPA allows 10 replicas, and 10 × 100 (the default cap) is 1000, above the worker's 500.
  Lower the cap to 49 or less (10 × 49 = 490), lower `maxReplicas`, or raise the worker's `--rpc.max-connections` above N × the cap.
  With `--redirect-queries` only submissions and the [reads kept on the worker](#reads-kept-on-the-worker) reach the worker, and they finish quickly except `eth_sendRawTransactionSync`, which can hold a worker connection for up to 30 s (reth's `--rpc.send-raw-transaction-sync-timeout`, inside the gateway's 35 s `--upstream-request-timeout`); a gateway whose cap is full answers `503` / `-32009` itself instead of letting the worker refuse submissions from every gateway.
- **Memory.** Peak request memory per gateway is about `--max-inflight-request-bytes` + `--max-connections` × `--http1-max-buf-size` + 64 MiB (see [Request size](#request-size)); the reference manifest's 1Gi limit covers the defaults.

### Split routing

With `--redirect-queries`, reads are answered by a node that has not seen this validator's transaction pool.

- Nonce reads at `pending`, and lookups of a submission made through the same gateway, go to the worker (see [Reads kept on the worker](#reads-kept-on-the-worker)).
  Other reads at `pending`, every read inside a batch, and a lookup that reaches another gateway or arrives after `--recent-submission-ttl` can still lag, so clients that send several transactions in a row should track their own nonces where they can.
- Fee quotes come from the public node and can lag an epoch boundary.
- A submission inside a mixed batch goes to the public RPC with the rest of the batch and enters the network there.
- Every redirected read reaches the public RPC from the gateway's address, so its per-IP limits apply to all of the gateway's clients together; agree limits with its operator before advertising the endpoint, or have it meter the gateway by a key sent with `--redirect-queries-header` (see [Query upstream header](#query-upstream-header)).
- Each redirected call carries the client's address in `X-Forwarded-For`, so the public RPC's operator sees your clients' addresses.

### DNS and the DDoS front

- Publish the gateways behind health-checked DNS or a load balancer that probes each gateway's `/ready/any`; with plain round-robin records a dead gateway keeps receiving its share of clients until someone edits the zone.
  `/ready/any` keeps a gateway published while it can serve reads; `/ready` means "can take submissions", and every gateway shares the worker, so with it every gateway drops out at once when the worker is down, reads included.
- Lock the domain at the registrar and enable DNSSEC where the provider supports it; a hijacked name serves forged state to every client.
- Absorb packet floods in front of the gateways.
  A front that terminates TCP, or one that SNATs (such as a Kubernetes `LoadBalancer` or `NodePort` Service left at `externalTrafficPolicy: Cluster`), makes every client share the front's addresses, because the gateway keys its limits on the TCP peer, unless the front is listed in `--trusted-proxies` and passes the client address in `X-Forwarded-For` or a PROXY protocol v2 header (see [Client identity behind a front](#client-identity-behind-a-front)).
  An unlisted front's clients share its rate-limit buckets and its `--max-connections-per-ip` count (default `32`), so every connection past 32 open at once from one front prefix is closed without a response, health probes from that prefix included.
  A listed front is not counted by `--max-connections-per-ip`, and with PROXY protocol each client behind it counts on its own (see [Connection handling](#connection-handling)).
  Otherwise use an L4 front that preserves client addresses (`externalTrafficPolicy: Local` on Kubernetes), or set the per-IP rate limit for the front's addresses and raise `--max-connections-per-ip` to the front's upstream connection pool size, or set it to `0`.
- If you use a front, firewall the gateways so that only the front reaches them; a gateway reachable directly bypasses it.

The gateway resolves the names in its own upstream URLs (worker RPC and readiness URLs, `--redirect-queries`) through a cache with a cap on concurrent lookups.
Without one, a `--redirect-queries` host that resolved slowly under steady reads filled the process's blocking pool with stuck lookups, readiness polls timed out behind them, and submissions got `503`.

- Each host's answer is reused for `--dns-cache-ttl` (30 s by default), and only one lookup per host runs at a time; requests that need the same name wait for the lookup already running, for up to `--upstream-connect-timeout`, and then get `504`.
  A failed connection does not discard the cached answer, so an upstream that moves to a new address is followed only once its answer expires.
- At most `--max-concurrent-dns-lookups` lookups (8 by default) run at once for the proxy, and as many again for the readiness poller.
  A request that needs a new lookup while every slot is busy fails at once as an unreachable upstream (`502`) instead of queueing; that refusal is the gateway's own, so it does not count toward `--upstream-failure-threshold`.
  A lookup keeps its slot until the system resolver answers, even after the request that started it has given up, so slow lookups never hold more than twice the cap in threads.
- An expired answer is still served while one lookup refreshes it in the background, and it keeps being served when the refresh fails or cannot start, for up to a day past its expiry.
  A name that stays unresolvable longer than that is dropped, and requests to it fail until it resolves again.
  This includes a record you delete on purpose: the gateway keeps sending submissions and readiness polls to its last addresses for up to a day, so restart the gateway after retiring an upstream's DNS name.
- The readiness poller has its own cache and its own cap of the same size, so query traffic to a slow host can never delay or refuse a readiness poll and turn into `503`s for submissions.
  The poller's probe of the `--redirect-queries` URL is the exception: it travels like a read, through the proxy's cache and cap, so it sees the query host's name the way reads do.
  The node's health listener closes the connection after each response, so without the cache a hostname readiness URL needs a lookup on every poll.
- `--dns-cache-ttl=0` turns the cache off: every new upstream connection looks its host up again, still one lookup per host at a time and within the cap.
  Upstream URLs with IP literals never use the resolver.
- A steady rate of `tn_worker_gateway_dns_lookups_total{result="rejected"}` means more upstream names are resolving slowly at once than the cap allows.
  Fix the resolver before raising the cap: every slot is a thread blocked in the system resolver.

### Firewalling

- The worker's RPC port: reachable from the gateway hosts only.
- The node's `--healthcheck` port: reachable from the gateway hosts only.
  It is unauthenticated and serves one connection at a time, so a few idle connections from anyone else make every gateway report not ready.
- The gateway's metrics port: inside the monitoring network only.
- The gateway's probe port (`--probe-addr`): reachable from the orchestrator only; it has no connection cap.
- When the gateway-to-worker hop leaves a network you control, use `https` worker URLs, ideally with a client certificate (see [TLS to workers](#tls-to-workers)), or run it through a tunnel; plain `http` to a public address logs a warning at startup.

## Deployment

A container image is built by `bin/worker-gateway/Dockerfile`, which mirrors the
node's `etc/Dockerfile` (rust:1.94 builder, slim debian runtime, non-root user).
Build it from the repository root:

```
docker build -f bin/worker-gateway/Dockerfile -t telcoin-worker-gateway:<tag> .
```

Reference Kubernetes manifests live under `deploy/k8s/` (a Deployment, Service,
ServiceMonitor, and a HorizontalPodAutoscaler keyed on the
`tn_worker_gateway_inflight_requests` gauge). They are a starting point, not a
turnkey install: see `deploy/README.md` for the placeholders to replace and the
prometheus-adapter rule the autoscaler needs.

The reference Service is a `ClusterIP`, so clients reach the gateways through a
front, and the gateways see that front as every client's address until client
identity is configured (see
[Client identity behind a front](#client-identity-behind-a-front)):

- **Behind an ingress controller or another HTTP proxy**, set `--trusted-proxies`
  to the narrowest ranges that cover the proxy's own addresses, and configure
  the proxy to append the address it received the connection from to
  `X-Forwarded-For` (or replace the header with it) and to set
  `X-Forwarded-Proto`, as
  [Client identity behind a front](#client-identity-behind-a-front) requires. A
  range as wide as the cluster's pod network also trusts every other pod that
  can reach the gateway.
- **Behind an L4 load balancer**, first check whether it preserves client
  addresses: a `LoadBalancer` or `NodePort` Service with
  `externalTrafficPolicy: Local` does, and the gateway then needs neither
  mechanism. A balancer that terminates TCP needs PROXY protocol v2 enabled on
  it, and the gateway started with `--proxy-protocol` and `--trusted-proxies`
  set to the addresses the balancer connects from. Every connection from those
  addresses must then carry the header, so keep the kubelet's probe source (the
  node address, for probes on the `rpc` port) out of `--trusted-proxies`, or
  the probes are closed and the pod never turns ready. The probe listener
  (`--probe-addr`, which the reference manifest's probes target) never reads
  the header.

Before putting gateways in front of a validator, read the [production-readiness review](docs/production-readiness.md), in particular its operator guidance on DNS, the DDoS front, firewalling and sizing for N gateways.
