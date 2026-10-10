# Worker gateway deployment manifests

These are **reference manifests** for running the Telcoin Network worker gateway
on Kubernetes. Treat them as a starting point, not a turnkey deployment: they
encode sane defaults and the gateway's runtime facts, but you must fill in the
placeholders below and adapt them to your cluster before applying.

```
deploy/
  k8s/
    deployment.yaml      # apps/v1 Deployment (2 replicas, hardened pod securityContext)
    service.yaml         # v1 ClusterIP Service (rpc 8545, metrics 9100)
    servicemonitor.yaml  # monitoring.coreos.com/v1 ServiceMonitor (Prometheus Operator)
    hpa.yaml             # autoscaling/v2 HPA driven by in-flight requests
```

A Grafana dashboard for the metrics these expose lives at
`../grafana-worker-gateway.json`.

## Placeholders to replace

- **Image ref** (`deployment.yaml`): `telcoin-worker-gateway:latest` is a local
  build tag. Point it at your registry, e.g.
  `registry.example.com/telcoin/worker-gateway:<tag>`.
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

## Ports and endpoints

- `rpc` / `8545` -- client JSON-RPC plus the gateway's own `/health` (liveness,
  always 200) and `/ready` (readiness, 200 when an upstream worker is ready,
  else 503). All three share this one port.
- `metrics` / `9100` -- Prometheus `/metrics`, enabled by
  `WORKER_GATEWAY_METRICS_ADDR` (equivalently `--metrics <addr>`). This is a
  **separate** listener from the client port.

The container has a `preStop` hook that sleeps 5s before the kubelet sends
SIGTERM. The gateway stops accepting connections as soon as SIGTERM arrives, so
the sleep gives the endpoint controller and any load balancer time to stop
routing new requests to a terminating pod. The
`terminationGracePeriodSeconds: 40` in the Deployment covers the preStop sleep
plus the gateway's own `--graceful-shutdown-timeout`
(`WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT`, default 30s), 36s in total with the
gateway's 1s join margin, so the process has room to drain in-flight proxied
requests before the kubelet escalates to SIGKILL. If you raise the gateway's
drain timeout or the preStop sleep, raise this too.

## Readiness and reads

The `readinessProbe` uses `/ready`, which reports whether the gateway can take submissions.
Every replica polls the same worker, so when the worker is down every replica leaves the Service at once, and reads stop too even though the query upstream could still serve them.
If reads must survive a worker outage, point the `readinessProbe` at `/health` instead; while the worker is down, submissions then get `503` / `-32000` from the gateway and reads keep working.
The same choice applies to an external load balancer or DNS health check.

## Memory limit

The gateway buffers each request body whole before it forwards it, and `--max-inflight-request-bytes` (default 512 MiB) bounds the bytes all in-flight requests hold at once; a request that does not fit is refused with `503` / `-32010` instead of being buffered.
A body with a declared length is read into one buffer of exactly that size, so a held body costs about its reservation.
Each open connection also keeps a read buffer of up to `--http1-max-buf-size` (default 64 KiB).
Size the container's memory limit as `--max-inflight-request-bytes` + `--max-connections` × `--http1-max-buf-size` + 64 MiB for the process baseline and response streaming.
At the defaults that is 512 MiB + 500 × 64 KiB + 64 MiB, about 608 MiB, and the Deployment's 1Gi limit leaves about 416 MiB over that.
In a test process that also ran the client and the upstream, 128 parallel 1 MiB bodies against a 64 MiB budget peaked 81 to 83 MiB above the idle process, about 1.3 times the budget, the read buffers of all three sides' connections included.
Each extra connection adds at most `--http1-max-buf-size`.
Raising `--max-request-bytes` does not raise the memory bound, but it makes the budget cheaper to exhaust: a request reserves its declared length (or the whole cap when chunked) from its head and holds it until the request deadline (`--upstream-request-timeout` + `--header-read-timeout`, 40 s by default) even if no body byte arrives.
About `--max-inflight-request-bytes` / `--max-request-bytes` such idle requests (512 at the defaults, 35 at a 15 MiB cap) make every other body-carrying request fail with `-32010`.
On an internet-facing gateway keep that quotient at or above `--max-connections` (the gateway warns at startup otherwise); a per-IP connection cap is tracked in #1599.
If the limit has to stay lower, lower the budget (`--max-inflight-request-bytes 268435456`, 256 MiB, needs about 352 MiB); a 256 MiB budget is exhausted by 256 idle requests, so on an internet-facing gateway lower `--max-connections` to 256 with it (about 336 MiB).

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

The `/metrics` endpoint on port `9100` has **no authentication**. Keep it
cluster-internal: the Service is `ClusterIP` (not exposed externally), and you
should not add an Ingress or LoadBalancer in front of the metrics port. Consider
a NetworkPolicy that only admits your Prometheus scraper to `9100`, for example:

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
    # allow client RPC from anywhere in-cluster
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
