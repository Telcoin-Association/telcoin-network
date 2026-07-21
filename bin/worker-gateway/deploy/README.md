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

## Ports and endpoints

- `rpc` / `8545` -- client JSON-RPC plus the gateway's own `/health` (liveness,
  always 200) and `/ready` (readiness, 200 when an upstream worker is ready,
  else 503). All three share this one port.
- `metrics` / `9100` -- Prometheus `/metrics`, enabled by
  `WORKER_GATEWAY_METRICS_ADDR` (equivalently `--metrics <addr>`). This is a
  **separate** listener from the client port.

The `terminationGracePeriodSeconds: 40` in the Deployment is deliberately larger
than the gateway's own `--graceful-shutdown-timeout`
(`WORKER_GATEWAY_GRACEFUL_SHUTDOWN_TIMEOUT`, default 30s) so the process has room
to drain in-flight proxied requests before the kubelet escalates to SIGKILL. If
you raise the gateway's drain timeout, raise this too.

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
