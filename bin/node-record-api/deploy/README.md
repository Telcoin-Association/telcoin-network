# node-record-api deployment manifests

These are **reference manifests** for running the Telcoin Network node-record API on
Kubernetes. Treat them as a starting point, not a turnkey deployment: they encode sane
defaults and the daemon's runtime facts, but you must fill in the placeholders below and adapt
them to your cluster before applying.

```
deploy/
  k8s/
    deployment.yaml      # apps/v1 Deployment (1 replica, hardened pod securityContext, committee ConfigMap)
    service.yaml         # v1 ClusterIP Service (api 8080, metrics 9100)
    servicemonitor.yaml  # monitoring.coreos.com/v1 ServiceMonitor (Prometheus Operator)
```

There is deliberately no autoscaler: the daemon is a single in-memory cache refreshed from the
DHT on a minutes-scale interval, and every replica would dial the same bootstrap peers. Run one
replica; put a CDN or the `Cache-Control: public, max-age=30` header to work if the site's read
load grows.

## Placeholders to replace

- **Image ref** (`deployment.yaml`): `telcoin-node-record-api:latest` is a local build tag.
  Point it at your registry, e.g. `registry.example.com/telcoin/node-record-api:<tag>`.
- **Committee file** (`deployment.yaml`): the `node-record-api-committee` ConfigMap is created
  from the repository's `chain-configs/testnet/committee.yaml` in the manifest's comment; create
  it for the chain you are deploying against. It supplies both the tracked key floor and the
  worker DHT bootstrap addresses.
- **Chain id** (`deployment.yaml`): `NODE_RECORD_API_CHAIN_ID` is `2017` (testnet). A wrong
  chain id does not fail loudly; it yields an empty directory.
- **Live committee** (optional): set `NODE_RECORD_API_RPC_URL` to a node's JSON-RPC to track
  committee rotation and schedule refreshes at epoch boundaries. Remember it returns the
  committee, not every validator; the committee file is the floor for the rest.

## Ports and endpoints

- `api` / `8080`: the `/v1/*` routes plus the daemon's own `/healthz` (liveness, always 200) and
  `/readyz` (readiness, 200 once the first cycle is done and a fresh record is cached, else
  503). All share this one port.
- `metrics` / `9100`: Prometheus `/metrics`, enabled by `NODE_RECORD_API_METRICS_ADDR`
  (equivalently `--metrics <addr>`). This is a **separate** listener from the API port.

The `terminationGracePeriodSeconds: 40` in the Deployment is deliberately larger than the
daemon's own `--graceful-shutdown-timeout` (`NODE_RECORD_API_GRACEFUL_SHUTDOWN_TIMEOUT`, default
30s) so the process has room to drain in-flight requests before the kubelet escalates to
SIGKILL. If you raise the daemon's drain timeout, raise this too.

The readiness probe's `initialDelaySeconds` gives the first refresh cycle time to complete: one
QUIC dial per bootstrap peer plus one lookup per tracked key, bounded by `--query-timeout`.

## Metrics scraping (ServiceMonitor)

`servicemonitor.yaml` uses `monitoring.coreos.com/v1`, which is provided by the **Prometheus
Operator**. It requires the Operator's CRDs to be installed in the cluster and a Prometheus
instance whose `serviceMonitorSelector` matches these labels. If you scrape Prometheus some
other way (static config, annotations, Grafana Alloy, ...), delete this file and point your
scraper at the `metrics` port / `/metrics` path instead.

Alert on `tn_node_record_api_last_successful_refresh_timestamp_seconds`: if it stops advancing
for more than a couple of refresh intervals the DHT (or every key source) is unreachable.

## Security note: the metrics port is unauthenticated

The `/metrics` endpoint on port `9100` has **no authentication**. Keep it cluster-internal: the
Service is `ClusterIP` (not exposed externally), and you should not add an Ingress or
LoadBalancer in front of the metrics port. Consider a NetworkPolicy that only admits your
Prometheus scraper to `9100`, for example:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: node-record-api-metrics
spec:
  podSelector:
    matchLabels:
      app: node-record-api
  policyTypes: [Ingress]
  ingress:
    # allow api traffic from anywhere in-cluster (front it with your ingress for the site)
    - ports:
        - port: 8080
    # restrict metrics to the monitoring namespace only
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: monitoring
      ports:
        - port: 9100
```

Adjust the `namespaceSelector` to wherever your Prometheus runs.

## Egress

The pod dials the worker DHT bootstrap peers over **UDP/QUIC** (`udp/49594` by default on
testnet). An egress policy or NAT that drops UDP leaves every cycle failing with
`no_bootstrap_peer_reachable`.
