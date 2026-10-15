# Worker gateway deployment manifests

These are **reference manifests** for running the Telcoin Network worker gateway
on Kubernetes. Treat them as a starting point, not a turnkey deployment: they
encode sane defaults and the gateway's runtime facts, but you must fill in the
placeholders below and adapt them to your cluster before applying.

```
deploy/
  k8s/
    deployment.yaml      # apps/v1 Deployment (hardened pod securityContext, replicas spread over nodes and zones)
    service.yaml         # v1 ClusterIP Services: worker-gateway (rpc 8545), worker-gateway-metrics (metrics 9100)
    servicemonitor.yaml  # monitoring.coreos.com/v1 ServiceMonitor (Prometheus Operator)
    hpa.yaml             # autoscaling/v2 HPA driven by in-flight requests (2 to 10 replicas)
    pdb.yaml             # policy/v1 PodDisruptionBudget (at most one pod evicted at a time)
```

The Deployment sets no `spec.replicas`, because the HPA owns the count; set `spec.replicas` if you do not install the HPA.

A Grafana dashboard for the metrics these expose lives at
`../grafana-worker-gateway.json`.

## Placeholders to replace

- **Image ref** (`deployment.yaml`): `telcoin-worker-gateway:<tag>` is a
  placeholder for a local build tag. Point it at your registry with a versioned
  tag, e.g. `registry.example.com/telcoin/worker-gateway:<tag>`, and pin it by
  digest (`...:<tag>@sha256:<digest>`) so every replica runs the same build;
  `docker buildx imagetools inspect <ref>:<tag>` prints the digest after a push.
  Do not deploy `:latest`.
- **Upstream worker endpoints** (`deployment.yaml`):
  `WORKER_GATEWAY_UPSTREAM_RPC_URL` and `WORKER_GATEWAY_UPSTREAM_READINESS_URL`
  are placeholders pointing at an in-cluster worker Service DNS name. Replace
  them with your actual worker Service. For anything beyond a single inline
  upstream, drop those two env vars and mount a config file instead: set
  `WORKER_GATEWAY_CONFIG` (equivalently `--config`) to a path backed by a
  ConfigMap volume. The gateway requires at least one upstream and will refuse
  to start without one.
- **Healthcheck port** (`deployment.yaml`): the readiness URL is not on the
  worker RPC. It is the `/health/workers` route of the node's healthcheck
  listener, which the node opens only when started with `--healthcheck <port>`
  (`HEALTHCHECK_TCP_PORT`); there is no default port. Replace
  `<HEALTHCHECK_TCP_PORT>` in
  `http://<node>:<HEALTHCHECK_TCP_PORT>/health/workers` with that port and make
  sure the node's Service exposes it to the gateway pods. Until you do, the URL
  does not parse and the gateway exits at startup. The listener is
  unauthenticated and serves one connection at a time, so admit only the
  gateways to it.
- **Query upstream** (`deployment.yaml`): `WORKER_GATEWAY_REDIRECT_QUERIES` (equivalently `--redirect-queries`) sends every call except `eth_sendRawTransaction` and `eth_sendRawTransactionSync` to a public JSON-RPC endpoint for the same chain, so the validator's worker receives only submissions (see the README's "Query redirect" section).
  Replace `<PUBLIC_RPC_HOST>` with that endpoint, preferably over `https`; until you do, the URL does not parse and the gateway exits at startup.
  Remove the variable only for a gateway that should send every call to its worker, never in front of a validator.
- **Client identity** (`deployment.yaml`): the Service is a `ClusterIP`, so clients arrive through a front, and until `WORKER_GATEWAY_TRUSTED_PROXIES` (equivalently `--trusted-proxies`) or PROXY protocol is set the gateway sees the front as every client: all of them share one `--max-connections-per-ip` (32) and `--rate-limit-per-ip` (100/s) budget per front address.
  Behind an ingress controller or another HTTP proxy, replace `<FRONT_CIDRS>` with the narrowest ranges that cover the proxy pods (not the whole pod network), and have the proxy append the address it received the connection from to `X-Forwarded-For` (see the README's "Client identity behind a front" section).
  Behind an L4 load balancer that terminates TCP, enable PROXY protocol v2 on it and replace the variable with `WORKER_GATEWAY_PROXY_PROTOCOL` and `WORKER_GATEWAY_PROXY_PROTOCOL_FROM` (equivalently `--proxy-protocol` and `--proxy-protocol-from`) set to the addresses the balancer connects from.
  Until you do one of these, `<FRONT_CIDRS>` does not parse and the gateway exits at startup.
  Remove the variable without a replacement only behind a front that preserves client addresses, such as a `LoadBalancer` Service with `externalTrafficPolicy: Local`.

## Ports and endpoints

- `rpc` / `8545` -- client JSON-RPC plus the gateway's own `/health` (liveness, always 200), `/ready` (200 when an upstream worker is ready, else 503) and `/ready/any` (200 when the gateway can serve any route, else 503).
  The probes answer here for fronts that can only check the client port; they are exempt from rate limiting but share the client connection cap.
- `probe` / `8546` -- the same three probes on a **separate** listener, enabled by `WORKER_GATEWAY_PROBE_ADDR` (equivalently `--probe-addr <addr>`), which serves nothing else.
  It sits outside the client connection cap and the rate limits, so a connection flood that holds every client slot cannot make the kubelet's probes time out and get a healthy pod restarted; both probes in the Deployment target it.
  It has no connection cap of its own, so keep it reachable from the pod's node only.
  Leaving it out of the Service, as `service.yaml` does, is not enough: any pod in the cluster can still reach it on the pod IP.
  A NetworkPolicy that admits only `8545` and `9100`, like the example below, shuts other pods out (on a network plugin that enforces NetworkPolicy) and still lets the kubelet's probes in, because a pod always accepts connections from its own node.
- `metrics` / `9100` -- Prometheus `/metrics`, enabled by
  `WORKER_GATEWAY_METRICS_ADDR` (equivalently `--metrics <addr>`). This is a
  **separate** listener from the client port.

Whatever exposes the `rpc` port must preserve client addresses: the gateway keys `--max-connections-per-ip` and the per-IP rate limit (`--rate-limit-per-ip`) on the resolved client address (the PROXY source or the believed `X-Forwarded-For` entry), so behind an L4 front that neither keeps the source address nor sends a PROXY header (`--proxy-protocol`), both apply to the front's own address and every client shares one budget.
A front listed in `--trusted-proxies` whose `X-Forwarded-For` is believed is not held to the connection cap; its requests are rate-limited per client.

The Deployment sets `WORKER_GATEWAY_SHUTDOWN_DELAY` (equivalently `--shutdown-delay`) to `5s`.
After SIGTERM the gateway keeps accepting and serving for those 5s while `/ready` and `/ready/any` answer `503` with `"ready": false` and `"draining": true`, on the client port and on the probe port alike; then both listeners close and the gateway drains in-flight requests.
The 5s covers the endpoint controller dropping the terminating pod from the Service, which happens when the pod is deleted whatever the readiness probe says, and that change reaching kube-proxy and any load balancer fed from the endpoints.
An external load balancer or DNS health check that probes `/ready` or `/ready/any` stops routing here within those 5s only if its interval times its unhealthy threshold (plus the record TTL for DNS) fits in them; for a slower front, raise the delay and the grace period below with it.
The container has no `preStop` sleep: a sleep only postpones SIGTERM, and fronts outside Kubernetes never learn from it that the pod is going away.
The Deployment also sets `WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT` (equivalently `--graceful-shutdown-timeout`, default 30s) to `45s`, the gateway's whole-request deadline at the defaults (`--upstream-request-timeout` 35s plus `--header-read-timeout` 10s), so a request accepted just before the listener closes, such as an `eth_sendRawTransactionSync` waiting for its receipt, can still finish; the drain ends as soon as the last request does.
The `terminationGracePeriodSeconds: 55` in the Deployment covers the 5s delay plus the 45s drain and the gateway's 1s join margin, 51s in total, so the process has room to drain in-flight proxied requests before the kubelet escalates to SIGKILL.
If you raise the delay, the drain timeout or `--upstream-request-timeout`, raise this too.

## Readiness and reads

`/ready` reports whether the gateway can take submissions: `200` when an upstream worker is ready, else `503`.
`/ready/any` reports whether it can serve any route: `200` when `/ready` would, or when the gateway's probes find the `WORKER_GATEWAY_REDIRECT_QUERIES` endpoint up (one `eth_chainId` call per `--readiness-poll-interval`, which flips its state after the same runs of results as a worker's readiness), else `503`.
Without a query redirect, `/ready/any` is exactly `/ready`.

Every replica polls the same worker, so when the worker is down every replica's `/ready` fails at once.
A `readinessProbe` on `/ready` would then take every replica out of the Service, and reads would stop too, although the query upstream could still serve them; removing the replicas gains submissions nothing, since every replica would answer them with the same `503` / `-32000`.
The Deployment's `readinessProbe` therefore uses `/ready/any`: the replicas stay in the Service while reads can be served, submissions get `503` / `-32000` until the worker is back, and a replica leaves only when neither the worker nor the query upstream answers.

The same choice applies to an external load balancer or DNS health check, which has to check the client port (`8545`), since the probe port is not exposed.
Check `/ready/any` to keep a gateway published while it can serve reads; that suits a validator's gateways, which all share one worker.
Check `/ready` only when a gateway that cannot take submissions should receive no traffic at all, for example when the front can send submissions to another validator's gateways instead.
On the client port the probes share the client connection cap, so a connection flood can still make an external check time out.
With `--proxy-protocol`, a check from an address in `--proxy-protocol-from` (by default `--trusted-proxies`) must open with a PROXY header too (a `LOCAL` one will do), or the gateway closes it, counts it under `tn_worker_gateway_connections_refused_total{reason="proxy_header"}` and logs a warning; a bare TCP connect check that sends nothing is closed without either.

## Memory limit

The gateway buffers each request body whole before it forwards it, and `--max-inflight-request-bytes` (default 512 MiB) bounds the bytes all in-flight requests hold at once; a request that does not fit is refused with `503` / `-32010` instead of being buffered.
A body with a declared length is read into one buffer of exactly that size, so a held body costs about its reservation.
Each open connection also keeps a read buffer of up to `--http1-max-buf-size` (default 64 KiB).
Size the container's memory limit as `--max-inflight-request-bytes` + `--max-connections` × `--http1-max-buf-size` + 64 MiB for the process baseline and response streaming.
At the defaults that is 512 MiB + 500 × 64 KiB + 64 MiB, about 608 MiB, and the Deployment's 1Gi limit leaves about 416 MiB over that.
Set the memory request to cover the same bound (the Deployment requests 768Mi): below it the pod is scheduled as if it needed less than clients can make it hold, and under node memory pressure the kubelet evicts the gateways first.
In a test process that also ran the client and the upstream, 128 parallel 1 MiB bodies against a 64 MiB budget peaked 81 to 83 MiB above the idle process, about 1.3 times the budget, the read buffers of all three sides' connections included.
Each extra connection adds at most `--http1-max-buf-size`.
Raising `--max-request-bytes` does not raise the memory bound, but it makes the budget cheaper to exhaust: a request reserves its declared length (or the whole cap when chunked) from its head and holds it until the request deadline (`--upstream-request-timeout` + `--header-read-timeout`, 45 s by default) even if no body byte arrives.
About `--max-inflight-request-bytes` / `--max-request-bytes` such idle requests (512 at the defaults, 35 at a 15 MiB cap) make every other body-carrying request fail with `-32010`.
On an internet-facing gateway keep that quotient at or above `--max-connections` (the gateway warns at startup otherwise); the per-client connection cap (`--max-connections-per-ip`) bounds how many of them one client can hold.
If the limit has to stay lower, lower the budget (`--max-inflight-request-bytes 268435456`, 256 MiB, needs about 352 MiB); a 256 MiB budget is exhausted by 256 idle requests, so on an internet-facing gateway lower `--max-connections` to 256 with it (about 336 MiB).
The probe port is not counted in the formula: each open probe connection holds up to about 24 KiB of buffers for at most `--header-read-timeout`, and nothing caps how many are open, which is why it must stay unreachable from clients.

## Metrics scraping (ServiceMonitor)

`servicemonitor.yaml` uses `monitoring.coreos.com/v1`, which is provided by the
**Prometheus Operator**. It requires the Operator's CRDs to be installed in the
cluster and a Prometheus instance whose `serviceMonitorSelector` matches these
labels. If you scrape Prometheus some other way (static config, annotations,
Grafana Alloy, ...), delete this file and point your scraper at the `metrics`
port / `/metrics` path instead.

## Autoscaling (HPA + prometheus-adapter)

`hpa.yaml` scales on a `Pods` custom metric,
`tn_worker_gateway_inflight_requests` (target average 50 in-flight per pod,
between 2 and 10 replicas). Kubernetes does **not** know this metric natively:
you need [prometheus-adapter](https://github.com/kubernetes-sigs/prometheus-adapter)
(or an equivalent `custom.metrics.k8s.io` provider) to read the gauge from
Prometheus and surface it on the custom metrics API as a per-pod metric.

A minimal prometheus-adapter rule that exposes the gauge per pod:

```yaml
rules:
  - seriesQuery: 'tn_worker_gateway_inflight_requests{namespace!="",pod!=""}'
    resources:
      overrides:
        namespace:
          resource: namespace
        pod:
          resource: pod
    name:
      # keep the series name as-is so the HPA metric name matches
      matches: "^(.*)$"
      as: "$1"
    metricsQuery: 'avg(<<.Series>>{<<.LabelMatchers>>}) by (<<.GroupBy>>)'
```

Notes:
- The gauge itself carries no per-pod label; prometheus-adapter attaches the
  `pod`/`namespace` association from the target Prometheus scrape labels, which
  is why the `ServiceMonitor` (or your scrape config) must land pods with those
  labels. The `metricsQuery` averages the series per pod so the HPA's
  `AverageValue` target compares like with like.
- Verify the metric is live before trusting the HPA:
  `kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1/namespaces/<ns>/pods/*/tn_worker_gateway_inflight_requests"`.

`behavior` sets a 300s scale-down stabilization window (conservative, avoids
flapping when traffic dips) and a 30s scale-up window (react quickly to load).

## Security note: the metrics port is unauthenticated

The `/metrics` endpoint on port `9100` has **no authentication**.
Keep it cluster-internal: only its own `ClusterIP` Service, `worker-gateway-metrics`, carries the port, and the ServiceMonitor selects that Service by its `app.kubernetes.io/component: metrics` label.
Never change that Service's type or put an Ingress or LoadBalancer in front of it.
The client Service, `worker-gateway`, carries only `rpc`, so changing its type to reach clients does not publish `9100`.
Any pod in the cluster can still reach `9100` on the pod IP, so consider a NetworkPolicy that only admits your Prometheus scraper to it, for example:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: worker-gateway-metrics
spec:
  podSelector:
    matchLabels:
      app: worker-gateway
  policyTypes: [Ingress]
  ingress:
    # allow client RPC from any source: a rule without `from` admits every
    # peer that can reach the pod, inside or outside the cluster
    - ports:
        - port: 8545
    # restrict metrics to the monitoring namespace only
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: monitoring
      ports:
        - port: 9100
```

Adjust the `namespaceSelector` to wherever your Prometheus / prometheus-adapter
runs.
