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

`/ready` reports whether the gateway can take submissions: `200` when an upstream worker is ready, else `503`.
`/ready/any` reports whether it can serve any route: `200` when `/ready` would, or when the `WORKER_GATEWAY_REDIRECT_QUERIES` endpoint answered the gateway's last probe (one `eth_chainId` call per `--readiness-poll-interval`), else `503`.
Without a query redirect, `/ready/any` is exactly `/ready`.

Every replica polls the same worker, so when the worker is down every replica's `/ready` fails at once.
A `readinessProbe` on `/ready` would then take every replica out of the Service, and reads would stop too, although the query upstream could still serve them; removing the replicas gains submissions nothing, since every replica would answer them with the same `503` / `-32000`.
The Deployment's `readinessProbe` therefore uses `/ready/any`: the replicas stay in the Service while reads can be served, submissions get `503` / `-32000` until the worker is back, and a replica leaves only when neither the worker nor the query upstream answers.

The same choice applies to an external load balancer or DNS health check, which has to check the client port (`8545`), since the probe port is not exposed.
Check `/ready/any` to keep a gateway published while it can serve reads; that suits a validator's gateways, which all share one worker.
Check `/ready` only when a gateway that cannot take submissions should receive no traffic at all, for example when the front can send submissions to another validator's gateways instead.
On the client port the probes share the client connection cap, so a connection flood can still make an external check time out.

## Memory limit

The gateway buffers each request body whole before it forwards it, and every open connection can hold one body.
A held body costs more than its size, because the connection's read buffer (up to about 400 KiB) stays allocated while the request is in flight: 500 held 1 MiB bodies peaked at about 712 MiB.
Size the container's memory limit as 1.5 × `--max-connections` × `--max-request-bytes` plus 64 MiB for the process baseline and response streaming.
At the defaults that is 1.5 × 500 × 1 MiB + 64 MiB, about 814 MiB, and the Deployment's 1Gi limit leaves about 300 MiB over the measured peak.
If you raise either flag, raise the limit with it; if the limit has to stay lower, lower one of the flags until the product fits (`--max-connections 256` needs about 448 MiB).
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
