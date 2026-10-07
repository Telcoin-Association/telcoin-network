# Enabling Namespaces

A Telcoin Network node serves JSON-RPC over three transports, and each transport serves its own set of [namespaces](README.md). This page covers the flags that choose them, the limits that keep the expensive ones in check, and how to confirm what a node actually serves.

## Transports

| Transport | Enabled by | Listens on | Namespaces |
| --- | --- | --- | --- |
| HTTP | `--http` | `--http.addr` (default `127.0.0.1`), `--http.port` (default `8545`) | `--http.api` |
| WebSocket | `--ws` | `--ws.addr` (default `127.0.0.1`), `--ws.port` (default `8546`) | `--ws.api` |
| IPC | on unless `--ipcdisable` | `--ipcpath` (default `/tmp/tn.ipc`) | always the default set; there is no `--ipc.api` |

Subscriptions (`eth_subscribe`) need a WebSocket or IPC connection.

A node that runs more than one worker starts one RPC server per worker. Worker 0 uses the ports and IPC path above; every other worker binds its own derived ports and IPC path, which the node logs at startup (`worker rpc endpoints resolved`). Every worker's server serves the same namespace selection.

## Choosing namespaces

`--http.api` and `--ws.api` take a comma-separated list of namespace names. The names a node accepts are `eth`, `net`, `web3`, `rpc`, `tn`, `debug` and `trace`.

| Value | Namespaces served |
| --- | --- |
| flag not given | `eth`, `net`, `web3`, `rpc`, `tn` |
| `all` | `eth`, `net`, `web3`, `rpc`, `tn` |
| a list, for example `eth,net,web3` | exactly the listed namespaces |
| `none` | nothing |

The rules behind the table:

- A list is served exactly as written. `--http.api eth,net,web3` serves no `tn_*` methods; add `tn` to the list to keep them.
- `debug` and `trace` are served only when named in the list; `all` never includes them. Naming either one logs a warning at startup.
- Names are lowercase and case-sensitive: `TN` or `Eth` is not recognised. Only `all` and `none` are accepted in any case.
- A name the node does not serve (`admin`, `txpool`, `ots`, a misspelling) is dropped from the selection with a startup warning that names it and lists the supported names. The node still starts and serves the rest of the list.
- `all` and `none` must be the whole value. When the first entry is `all` or `none` the rest of the list is ignored, so `all,debug` serves the default set without `debug`. Write the full list instead: `eth,net,web3,rpc,tn,debug`.
- HTTP and WebSocket are configured separately. A namespace named only in `--ws.api` is not served over HTTP.

### HTTP and WebSocket on one port

When HTTP and WebSocket listen on the same address and port, one server answers both protocols, so the two selections must be identical, and so must `--http.corsdomain` and `--ws.origins` when both are set. The RPC server refuses to start otherwise. A missing flag counts as the default set, so on a shared port give `--http.api` and `--ws.api` the same list, or leave both out: `--http.api eth,net,web3` with no `--ws.api` does not start.

## Why debug and trace are off by default

Telcoin Network nodes keep the full chain history, so every block since genesis can be re-executed. `debug_trace*` and `trace_*` methods do exactly that: a single block trace, or a `trace_filter` over a range of blocks, replays every transaction in the EVM and can take seconds of CPU time and a large response.

If you serve them:

- run them on a separate node from your public RPC and from any validator;
- keep the endpoint behind authentication (`--rpc.jwtsecret`) or a proxy that allows only the methods you need;
- set `--rpc.max-tracing-requests` low (2 to 4 on a shared host) and `--rpc.max-trace-filter-blocks` small;
- cap response size with `--rpc.max-response-size`.

Some `debug_*` methods are registered but do nothing.

## Limits and access flags

| Flag | Default | Effect |
| --- | --- | --- |
| `--rpc.max-tracing-requests` | CPU cores minus 2, at least 2 | Concurrent tracing requests, shared by `debug_trace*`, `debug_executionWitness*`, `trace_*` and `eth_callBundle`; further requests wait for a free slot |
| `--rpc.max-trace-filter-blocks` | `100` | Largest block range one `trace_filter` request may cover |
| `--rpc.max-blocks-per-filter` | `100000` (`0` = no limit) | Largest block range for `eth_getLogs` and log filters |
| `--rpc.max-logs-per-response` | `20000` (`0` = no limit) | Most logs one `eth_getLogs` or filter response may return |
| `--rpc.gascap` | `50000000` | Gas limit for `eth_call`, `eth_estimateGas` and the debug and trace call methods |
| `--rpc.max-response-size` | `160` (MB) | Largest response the server sends |
| `--rpc.max-connections` | `500` | Concurrent RPC connections |
| `--http.corsdomain` | none | Origins allowed to call the HTTP server from a browser |
| `--ws.origins` | none | Origins allowed to open a WebSocket connection |
| `--rpc.jwtsecret` | none | Hex-encoded secret; when set, HTTP and WebSocket requests need a JWT bearer token |

## Examples

These show only the RPC flags; add them to the `telcoin-network node` command you already run.

A public RPC node (an observer) serving the default set on all interfaces:

```
--http --http.addr 0.0.0.0 --ws --ws.addr 0.0.0.0
```

A validator serving the minimum on its private interface. Keep `eth`: observers forward `eth_sendRawTransaction` to a committee validator's advertised RPC endpoint, and leave `tn` to non-validating nodes:

```
--http --http.addr 10.0.0.5 --http.api eth,net,web3
```

A local development node with every namespace, including tracing:

```
--http --http.api eth,net,web3,rpc,tn,debug,trace
```

## Verify

`rpc_modules` lists the namespaces a transport serves, when `rpc` is part of its selection:

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"rpc_modules","params":[],"id":1}'
```

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": { "eth": "1.0", "net": "1.0", "rpc": "1.0", "tn": "1.0", "web3": "1.0" }
}
```

A namespace that is not served answers every one of its methods with the JSON-RPC "method not found" error:

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x..."],"id":1}'
```

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": { "code": -32601, "message": "Method not found" }
}
```

The node builds the `rpc_modules` answer once, for the first transport whose selection includes `rpc`, in the order HTTP, WS, IPC, and every transport then returns that list. When HTTP and WS both serve `rpc` with different selections, the answer on WS and IPC is HTTP's list. Tracked in [#1588](https://github.com/Telcoin-Association/telcoin-network/issues/1588).

## Upgrading from earlier releases

Earlier releases served `tn` on every enabled transport whatever `--http.api` or `--ws.api` said, including `none`. `tn` is now selected like the other namespaces:

- an explicit list must name `tn` to keep the `tn_*` methods, for example `--http.api eth,net,web3,tn`;
- `none` serves nothing;
- with no `--http.api` or `--ws.api` flag, or with `all`, the transport now also serves `rpc_modules`.
- with HTTP and WebSocket on the same port, a list on only one of the two flags no longer starts, because the other transport now resolves to the default set; give both flags the same list.
