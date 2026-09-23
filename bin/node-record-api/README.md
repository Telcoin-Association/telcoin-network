# node-record-api

A lightweight HTTP/JSON daemon that backs a public website listing validators' advertised
JSON-RPC endpoints. On a schedule it fetches BLS-signed node records from a Telcoin Network
worker kademlia DHT, caches the verified records in memory, and serves them over a small
read-only HTTP surface. It never runs a node.

## How records reach it

A validator that opts in to advertising RPC publishes a signed `NodeRecord` to its worker DHT,
keyed by its BLS public key. The record carries the validator's network identity, its DHT
multiaddr, a timestamp, and (on worker records only) an `RpcInfo { http, ws }` pair.

The daemon reads those records through the `tn-kad-client` crate: it dials the configured
bootstrap peers over QUIC, negotiates kademlia on the chain- and worker-namespaced protocol
name, and issues a `GET_VALUE` per tracked key. Every copy the lookup returns is verified the
same way the node verifies records it learns from peers, so a served record is
**self-authenticating**: its signature was checked against the BLS key it is published under, for
exactly this `(chain_id, worker)` domain, and its kademlia publisher matches the network identity
it names. Nothing is trusted because of which peer served it.

This puts **no load on any validator's RPC** to obtain the records: the DHT is the only source.
The optional `--rpc-url` is used only to learn the current committee's keys and the epoch
boundary, a handful of cheap calls per refresh cycle against one node of the operator's choosing.

### One DHT client per refresh cycle

Each cycle spawns a fresh `KadClient` and shuts it down at the end. When a node sits at its
peer target, its peer manager prunes non-validator, non-kad-routable peers first, which is
exactly this client, and then temporarily bans the pruned **peer id** (not the IP). A long-lived
client would be refused for the ban duration on every later cycle; a per-cycle client has a
fresh ephemeral identity each time and is never the peer that was banned. See the
`refresh` module docs for the exact functions involved.

## Key sources

The DHT is keyed by BLS public key, so the daemon needs a list of keys to query. Three sources
exist and combine as **`live ∪ floor`**:

| Source | Flag | Role | Notes |
| --- | --- | --- | --- |
| RPC committee | `--rpc-url` | live | `tn_getCurrentEpochInfo` + `tn_getCommitteeBlsPubkeys(epochId)` each cycle. Also yields the epoch boundary for scheduling. |
| Committee file | `--committee-file` | floor | Authorities of a `committee.yaml`. Re-read every cycle; a file that stops parsing keeps the last good set. Also supplies bootstrap addresses when `--bootstrap` is unset. |
| Static list | `--keys-file` | floor | YAML list of base58 or `0x`-hex keys. Every entry must parse (startup error otherwise). |

> **`tn_getCommitteeBlsPubkeys` returns the COMMITTEE, not all validators.** A staked validator
> that is not in the current committee but advertises RPC is invisible to the RPC source. Put
> such validators in the committee file or the static list so they are tracked regardless.

If the live source fails on a cycle, its previous set is reused (a transient RPC blip must not
shrink the directory) and `tn_node_record_api_key_source_failures_total{source="rpc"}` is
incremented.

## Refresh and cache semantics

- Cycles run every `--refresh-interval` (missed ticks are skipped, never bursted). With
  `--rpc-url`, an extra cycle runs at each epoch boundary plus `--epoch-grace` so the new
  committee is picked up promptly. The boundary is derived as the node derives it: the timestamp
  of block `blockHeight - 1` plus `epochDuration`.
- **Newest wins.** A fetched record replaces the cached one only when its signed timestamp is
  not older; a lagging peer serving a stale copy cannot roll the cache back.
- **Failures keep the entry.** A miss or lookup error leaves the cached record in place without
  advancing its `fetched_at`, so it ages into `stale` (older than two refresh intervals) and,
  after `--record-ttl`, out of the cache.
- A key absent from the tracked set for `--absent-cycles-before-evict` consecutive cycles is
  evicted (a validator that left both the committee and the floor).

## Configuration

Every flag has a `NODE_RECORD_API_<FLAG>` environment fallback. Durations use `humantime`
syntax (`5s`, `2m`, `24h`).

| Flag | Env | Default | Description |
| --- | --- | --- | --- |
| `--listen-addr` | `NODE_RECORD_API_LISTEN_ADDR` | `0.0.0.0:8080` | API + `/healthz` + `/readyz`. |
| `--metrics` | `NODE_RECORD_API_METRICS_ADDR` | (none) | Prometheus `/metrics` on a separate listener; unset disables. |
| `--chain-id` | `NODE_RECORD_API_CHAIN_ID` | **required** | Chain whose worker DHT is read (`2017` for testnet). No default: a wrong value silently yields `NoPeerAnswered` / `InvalidRecords` for every key. |
| `--worker-id` | `NODE_RECORD_API_WORKER_ID` | `0` | Which worker DHT to read. |
| `--bootstrap` | `NODE_RECORD_API_BOOTSTRAP` | (from committee file) | Worker DHT bootstrap multiaddr with `/p2p/`; repeatable, comma-separated in the env. |
| `--committee-file` | `NODE_RECORD_API_COMMITTEE_FILE` | (none) | `committee.yaml`: key floor + bootstrap addresses. |
| `--keys-file` | `NODE_RECORD_API_KEYS_FILE` | (none) | YAML list of keys: static floor. |
| `--rpc-url` | `NODE_RECORD_API_RPC_URL` | (none) | Node JSON-RPC (`http`/`https`): live committee + epoch scheduling. |
| `--rpc-timeout` | `NODE_RECORD_API_RPC_TIMEOUT` | `10s` | Per-request deadline for those JSON-RPC calls. |
| `--refresh-interval` | `NODE_RECORD_API_REFRESH_INTERVAL` | `5m` | Refresh cadence; must exceed `--query-timeout`. |
| `--epoch-grace` | `NODE_RECORD_API_EPOCH_GRACE` | `30s` | Delay after an epoch boundary before the extra refresh. |
| `--query-timeout` | `NODE_RECORD_API_QUERY_TIMEOUT` | `15s` | Per-lookup DHT deadline (also bounds the bootstrap dial). |
| `--lookup-concurrency` | `NODE_RECORD_API_LOOKUP_CONCURRENCY` | `4` | Lookups in flight at once (>= 1). |
| `--record-ttl` | `NODE_RECORD_API_RECORD_TTL` | `24h` | Evict records not refreshed for this long; must exceed `--refresh-interval`. |
| `--absent-cycles-before-evict` | `NODE_RECORD_API_ABSENT_CYCLES_BEFORE_EVICT` | `3` | Evict keys absent from the tracked set this many cycles (>= 1). |
| `--header-read-timeout` | `NODE_RECORD_API_HEADER_READ_TIMEOUT` | `10s` | Inbound header read deadline (slow-loris guard). |
| `--request-timeout` | `NODE_RECORD_API_REQUEST_TIMEOUT` | `5s` | Whole-request deadline. |
| `--max-connections` | `NODE_RECORD_API_MAX_CONNECTIONS` | `500` | Concurrent inbound connection cap. |
| `--tcp-user-timeout` | `NODE_RECORD_API_TCP_USER_TIMEOUT` | `30s` | Transport-stall deadline (`TCP_USER_TIMEOUT`, Linux; `0` disables). |
| `--max-connection-duration` | `NODE_RECORD_API_MAX_CONNECTION_DURATION` | `10m` | Hard cap on one connection's lifetime; must be >= header + request timeouts (`0` disables). |
| `--max-request-bytes` | `NODE_RECORD_API_MAX_REQUEST_BYTES` | `16384` | Max request body (the API is `GET`-only). |
| `--rate-limit-per-ip` | `NODE_RECORD_API_RATE_LIMIT_PER_IP` | `100` | Per-IP requests/second (`0` disables). |
| `--rate-limit-per-ip-burst` | `NODE_RECORD_API_RATE_LIMIT_PER_IP_BURST` | `0` | Per-IP burst (`0` derives 2×rate). |
| `--rate-limit-per-ip-v6-prefix` | `NODE_RECORD_API_RATE_LIMIT_PER_IP_V6_PREFIX` | `64` | IPv6 prefix the client address is masked to. |
| `--rate-limit-per-ip-v4-prefix` | `NODE_RECORD_API_RATE_LIMIT_PER_IP_V4_PREFIX` | `32` | IPv4 prefix the client address is masked to. |
| `--rate-limit-global` | `NODE_RECORD_API_RATE_LIMIT_GLOBAL` | `3000` | Daemon-wide requests/second (`0` disables). |
| `--rate-limit-global-burst` | `NODE_RECORD_API_RATE_LIMIT_GLOBAL_BURST` | `0` | Global burst (`0` derives 2×rate). |
| `--graceful-shutdown-timeout` | `NODE_RECORD_API_GRACEFUL_SHUTDOWN_TIMEOUT` | `30s` | Drain deadline on SIGTERM. |
| `--log-filter` | `NODE_RECORD_API_LOG_FILTER` | `info` | Tracing filter directive. |

Startup validation: at least one bootstrap source (`--bootstrap` or `--committee-file`); at
least one key source (`--rpc-url`, `--committee-file`, or `--keys-file`); every bootstrap address
carries `/p2p/`; `--refresh-interval` > 0 and > `--query-timeout`; `--record-ttl` >
`--refresh-interval`; `--max-connection-duration` >= `--header-read-timeout` +
`--request-timeout` (or `0`); `--lookup-concurrency` and `--absent-cycles-before-evict` >= 1;
`--rpc-url` is `http` or `https`. Each rule fails startup with the offending flag named.

### Example: testnet from the committee file

```
node-record-api \
  --chain-id 2017 \
  --committee-file chain-configs/testnet/committee.yaml \
  --listen-addr 127.0.0.1:8080
```

The committee file supplies both the key floor (its five authorities) and the worker-0 DHT
bootstrap addresses (`udp/49594`). Add `--rpc-url https://rpc.example/` to track committee
rotation live.

## HTTP contract

| Route | Behaviour |
| --- | --- |
| `GET /healthz`, `GET /health` | `200 {"status":"ok"}` while the process lives. |
| `GET /readyz`, `GET /ready` | `200` once the first cycle is done **and** at least one cached record is not stale; else `503 {"error":"not_ready","message":"..."}`. |
| `GET /v1/records` | Envelope plus every cached record. |
| `GET /v1/records/{key}` | One record; `{key}` is base58 or `0x`-hex. `400 {"error":"invalid_key","message":...}` on a bad key, `404 {"error":"not_found"}` when unknown. |
| `GET /v1/rpcs` | Envelope plus only the records with `rpc != null`: the site payload. |

`HEAD` is answered for every `GET`. CORS allows `GET`/`HEAD`/`OPTIONS` from any origin, never
credentials. Every `/v1/*` success carries `Cache-Control: public, max-age=30`. Every error,
including `429` (rate limited), `408` (request deadline), and unmatched paths, is a JSON object
with a stable `error` code.

Example `GET /v1/rpcs`:

```json
{
  "chain_id": 2017,
  "network": "worker-0",
  "generated_at": 1789000000,
  "last_refresh_at": 1788999900,
  "keys_tracked": 5,
  "records": [
    {
      "bls_pubkey": {
        "base58": "pDmpE29YEhr93MPVPCGkWx3BsCcow4oExmxv5viJz2GTjsaXwc3YwKxE1CFdVuTVSudsFDutFSVmtgF98Abs56JZPfGQs6GzbqXDFkGA1eZx2edkwfP2Q6eRLo8coLhvTNj",
        "hex": "0x8a9d...c3e1"
      },
      "network_pubkey": "4XTTM8bKFPoKfVL1UwN1vBMjCWCFVKbKpKUha12qr2Bi86ip4",
      "peer_id": "12D3KooWHGdbYerasxspuhZW4KqNMbbpqvLNCG6LGpS5yx6b8x2x",
      "multiaddrs": ["/ip4/35.203.41.59/udp/49594/quic-v1"],
      "rpc": { "http": "https://rpc.validator.example:8545/", "ws": "wss://rpc.validator.example:8546/" },
      "record_timestamp": 1788999850,
      "record_age_seconds": 150,
      "fetched_at": 1788999900,
      "fetch_age_seconds": 100,
      "stale": false,
      "copies_seen": 3,
      "verification": "bls_self_signed",
      "network": "worker-0"
    }
  ]
}
```

`record_timestamp` is the validator's signed publication time; `fetched_at` is when this daemon
last fetched the record; `stale` is `fetch_age_seconds` beyond two refresh intervals;
`copies_seen` is how many valid replicas the last lookup returned.

## Readiness semantics

`/readyz` is `200` iff the first refresh cycle has completed and at least one cached record is
fresh. It is `503` before the first cycle, after a cycle that fetched nothing, and once every
cached record has aged past two refresh intervals (the DHT has been unreachable for at least
that long). `/healthz` is always `200` while the process runs.

## Observability

Pass `--metrics <addr>` to expose Prometheus metrics at `GET /metrics` on its own listener,
separate from the API port. The endpoint reuses the node's `tn-metrics` recorder, so these
`tn_node_record_api_*` series render alongside a node's `tn_*` metrics. It is unauthenticated;
bind it to an internal interface.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `tn_node_record_api_requests_total` | counter | `route`, `status` | HTTP requests by matched route and status. |
| `tn_node_record_api_request_duration_seconds` | histogram | `route` | HTTP request latency. |
| `tn_node_record_api_inflight_requests` | gauge | | HTTP requests in flight. |
| `tn_node_record_api_refresh_cycles_total` | counter | `outcome` (`ok` / `partial` / `failed`) | Refresh cycles: every key resolved / some / none. |
| `tn_node_record_api_last_successful_refresh_timestamp_seconds` | gauge | | Unix time of the last cycle that fetched a record. **The primary alert signal.** |
| `tn_node_record_api_refresh_duration_seconds` | histogram | | Wall time per cycle. |
| `tn_node_record_api_keys_tracked` | gauge | | Tracked key set size. |
| `tn_node_record_api_records_cached` | gauge | | Records in the cache. |
| `tn_node_record_api_records_stale` | gauge | | Cached records past the staleness threshold. |
| `tn_node_record_api_records_with_rpc` | gauge | | Cached records advertising RPC. |
| `tn_node_record_api_lookup_failures_total` | counter | `reason` | Lookups with no record: a `KadClientError` variant in snake_case, or `not_found`. |
| `tn_node_record_api_key_source_failures_total` | counter | `source` | Key source refreshes that failed (`rpc`, `committee_file`). |

## Deployment

A container image is built by `bin/node-record-api/Dockerfile` (rust:1.94 builder, slim debian
runtime, non-root user). Build it from the repository root:

```
docker build -f bin/node-record-api/Dockerfile -t telcoin-node-record-api:latest .
```

Run it against testnet with the committee file from the repository:

```
docker run --rm -p 8080:8080 \
  -v "$PWD/chain-configs/testnet/committee.yaml:/etc/node-record-api/committee.yaml:ro" \
  telcoin-node-record-api:latest \
  --chain-id 2017 \
  --committee-file /etc/node-record-api/committee.yaml

curl -s 127.0.0.1:8080/readyz
curl -s 127.0.0.1:8080/v1/rpcs
```

Reference Kubernetes manifests live under `deploy/k8s/` (a single-replica Deployment with a
mounted committee ConfigMap, a Service, and a ServiceMonitor); see `deploy/README.md`.

## Dependencies

This crate enables `reqwest`'s `rustls-tls-native-roots` feature so an `https://` `--rpc-url`
works. The workspace `reqwest` declares no TLS backend, but feature unification already pulls
rustls into `reqwest 0.12` workspace-wide through alloy/reth, so this adds no new crate and
changes nothing for `worker-gateway` (which stays HTTP-only by its own startup check).

`ratelimit.rs` and the accept loop in `server.rs` are copies of the worker gateway's; keep the
two in sync until a shared `crates/tn-http-edge` is extracted.
