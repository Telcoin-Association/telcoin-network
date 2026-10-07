# Public RPC nodes

A public RPC node is an observer that serves the JSON-RPC API to the internet.
It answers reads from its own copy of the chain.
With `--forward-txs`, it also relays every `eth_sendRawTransaction` and `eth_sendRawTransactionSync` call to a dedicated validator and returns that validator's answer to the client.
The validator's RPC address stays private: only the public nodes dial it, and no client ever sees it.

Without `--forward-txs`, an observer puts a submitted transaction into its own pool and forwards it later, in the background, to the committee validator that owns the sender, at the RPC endpoint that validator advertises in its node record (see [Observer](../architecture/network.md#observer)).
That path needs validators to publish an RPC endpoint, and the client receives a hash before any validator has accepted the transaction.
With `--forward-txs`, the client waits for the validator's verdict, and the validator receives nothing from the public nodes except transactions.

## How forwarding works

The node replaces the two submission methods on every transport that serves the `eth` namespace: HTTP, WebSocket and IPC.
Every other method is served locally, as on any observer.

- `eth_sendRawTransaction`: the node runs its local checks (the `--rpc.txfeecap` guard, plus the [sanitize checks](#sanitizing-submissions) when enabled), then sends the client's bytes to the first target as `eth_sendRawTransaction`. The target's hash or error object goes back to the client. The node's own transaction pool is never touched.
- `eth_sendRawTransactionSync`: the node runs the same checks, forwards a plain `eth_sendRawTransaction`, then waits up to 30 seconds for the transaction's receipt on its own chain. The validator only ever receives `eth_sendRawTransaction`, so a client cannot hold a validator connection open for the confirmation wait.

## Validator side

Pick one validator, or a small set, to receive the public nodes' transactions.
On each of those validators:

1. Enable the HTTP RPC server with `--http`, and bind it with `--http.addr` to an interface the public nodes can reach. The default address is `127.0.0.1`. The default module set includes `eth`, which is the only namespace forwarding uses.
2. Firewall the RPC port (TCP, `--http.port`, default 8545) so that only the public nodes' static IP addresses can connect. Share the address only with the operators who run the public nodes.
3. Protect the link. Plain `http://` sends every signed transaction in cleartext between the public node and the validator. Use an `https://` target or a private link such as a VPN or a peered private network. The node's RPC server has no TLS option of its own, so an `https://` target means a TLS-terminating reverse proxy in front of the validator's RPC port.

For example, the validator adds these flags to its usual command line:

```sh
telcoin-network node \
    --http \
    --http.addr 10.20.0.5 \
    --http.port 8545
```

A node with several workers serves RPC per worker: worker 0 listens on `--http.port` and worker `k` listens 200 × `k` ports below it.
Point the public nodes at the worker 0 port.

A target URL may carry a path for a reverse proxy, and a `user:password@` prefix, which is sent as an HTTP basic-auth header.
A username without a password is refused at startup, because the node would send no credentials for it.
Over plain `http://` those credentials travel in cleartext too.

## Public node configuration

| Flag | Default | Description |
| --- | --- | --- |
| `--forward-txs <TARGET[,TARGET...]>` | none | Comma-separated, ordered failover list of validator RPC targets. Also read from `TN_FORWARD_TXS`. |
| `--sanitize-txs` | `false` | Decode and check each transaction locally before forwarding. Requires `--forward-txs`. |

The list is one value.
Passing `--forward-txs` twice is an error rather than a merge.
When both the flag and `TN_FORWARD_TXS` are set, the flag wins.
Prefer the environment variable: it keeps the targets out of the process command line, which other users on the host can read with `ps`, and `--help` never prints its value.

```sh
export TN_FORWARD_TXS='https://tx.validator.example.com,10.20.0.5:8545'
telcoin-network node \
    --datadir /var/lib/telcoin \
    --chain adiri \
    --http --http.addr 0.0.0.0 \
    --ws --ws.addr 0.0.0.0 \
    --metrics 127.0.0.1:9101 \
    --sanitize-txs
```

A malformed list stops the node at startup.
The error names the faulty entry by its position in the list, but the command-line parser also quotes the whole value it refused, whether it came from the flag or from `TN_FORWARD_TXS`.
A list that carries `user:password@` credentials therefore prints them to stderr, so keep that output out of shared logs and support tickets.
The node does not dial any target at startup, so an unreachable target does not stop it; the first submission that reaches it fails over.
At startup the node logs one `info` line, `forwarding raw transaction submissions`, with the number of targets and the sanitize setting.
The node's own log lines and its metrics name a target by its index in the list, never by its URL, and no client response names a target at all.
The HTTP library underneath does log the address it dials: each new connection to a target writes `connecting to <ip:port>` and `connected to <ip:port>` at `debug`.
The default `--log.file.filter debug` puts those lines in the node's log file, and stdout carries them too whenever it logs at `debug`, through `-vvvv` or a `RUST_LOG` that enables `debug`.
To keep target addresses out of both, add `--log.file.filter "debug,hyper_util::client::legacy::connect=info"` and `--log.stdout.filter "hyper_util::client::legacy::connect=info"` to the node's flags.
Treat this as hygiene, not protection: the firewall is what protects the validator, and the setup must stay safe if its address becomes known.

### Target syntax

Each entry is trimmed and normalized to the URL the node dials.

| Entry | Dials | Rule |
| --- | --- | --- |
| `https://tx.validator.example.com` | `https://tx.validator.example.com`, port 443 | A URL keeps its scheme's default port: 443 for `https`, 80 for `http`. |
| `https://proxy.example.com:9443/tn/rpc` | as written | A URL may carry a port and a path. Query strings and fragments are refused. |
| `10.20.0.5:8545` | `http://10.20.0.5:8545/` | `ip:port`. |
| `10.20.0.5` | `http://10.20.0.5:8545/` | An address without a port dials 8545. |
| `fd00::5` | `http://[fd00::5]:8545/` | A bare IPv6 address always dials 8545 and cannot carry a port: `fd00::5:8566` is the address `fd00::5:8566` on port 8545. |
| `[fd00::5]:8566` | `http://[fd00::5]:8566/` | Bracket an IPv6 address to name a port. |
| `[fd00::5]` | `http://[fd00::5]:8545/` | A bracketed IPv6 address without a port dials 8545. |
| `validator.example.com` | `http://validator.example.com:8545/` | `host[:port]`, where the host is an IPv4 address or a domain name; the port defaults to 8545. |
| `validator.example.com:8566` | `http://validator.example.com:8566/` | `host:port`. |

The rest of the rules:

- An entry without a scheme dials plain `http://`, so a TLS target always needs the `https://` prefix.
- A URL must use `http` or `https` and name a host. Port 0 is refused.
- An entry without a scheme is `host[:port]` only: no path, no credentials. Use a full URL for those.
- The list may name at most 8 targets.
- Empty entries, including the ones a leading, doubled or trailing comma creates, are refused.
- Two entries that normalize to the same URL are refused. `10.20.0.5`, `10.20.0.5:8545` and `http://10.20.0.5:8545` are all the same target.
- Domain names are resolved when the node connects, not at startup, so a DNS change takes effect on the next connection. The name is also what TLS verifies.
- A target must be a validator that takes submissions itself. Never point a target at the node itself or at another node that forwards. A target that leads back to a forwarding node, directly or through other public nodes, passes each submission around until the attempt times out and this node moves to its next target, so clients receive `-32603` only when every target loops. A chain that ends at a validator misbehaves too. The middle node spends up to 15 seconds on its own failover, so this node can give up after its 5-second attempt and move on while the middle node still delivers the transaction. When the middle node reaches none of its targets, its own `-32603` reaches this node as a JSON-RPC error, so it goes to the client unchanged, counts as `upstream_error` and is never failed over.

## Failover

The list order is the failover order.
For each submission the node tries the targets one at a time, healthy targets first in list order, then demoted targets in list order.

The node moves to the next target only when an attempt fails below the JSON-RPC layer:

- the connection is refused, reset or cannot be made;
- no reply arrives within 5 seconds, including the wait for a free request slot;
- the target answers with a non-2xx HTTP status other than 413;
- the response body is larger than 64 KiB;
- the target answers 2xx with a body that is not a JSON-RPC reply to the request.

A JSON-RPC error reply never triggers failover.
The target answered, so its verdict (`nonce too low`, `already known`, `replacement transaction underpriced`, and so on) goes back to the client unchanged and no other target is tried.
A target that answers HTTP 413 (request too large) does not trigger failover either: the request is at fault, not the target, and every other target would receive the same upload.
The client receives the oversized-request error and the target keeps its place.

A target that fails an attempt is demoted for 30 seconds: during that time it is tried only after every healthy target.
When the 30 seconds run out, one submission tries the target in its place in the list as a probe, and the others keep trying it last until the probe succeeds or fails.
A target that still hangs therefore costs one 5-second wait per cooldown, not one per submission.
A target that answers, with a hash or with a JSON-RPC error, returns to its place in the list at once.
Demoted targets are still tried, so a list whose targets are all demoted keeps working as soon as one of them recovers.

One submission spends at most 15 seconds across all targets.
Each attempt gets 5 seconds or whatever remains of the 15, whichever is less, so at most three targets that time out are tried; a target that refuses the connection fails fast and leaves the budget for the next one.
An attempt that the budget cuts short of 5 seconds neither demotes its target nor counts as a failure, since the target never had its full 5 seconds.
When every target has failed or the budget runs out, the client receives the fixed [unavailable error](#errors) and the node logs a `warn` line, at most once every 30 seconds.
Each failed attempt, and each JSON-RPC error a target returns, is logged at `debug` under the target `tn::rpc::forward`, naming the target by its index in the list.

At most 128 requests are in flight to one target at a time.
Further submissions wait for a slot inside their 5-second attempt timeout.
This bounds the load one public node can put on its validator.
All worker lanes of the public node share one forwarder: one ordered list, one demotion state, one connection pool and one set of metrics per process.

## Sanitizing submissions

`--sanitize-txs` decides how much the public node checks before a transaction reaches the validator.

Off (the default), the node checks only `--rpc.txfeecap`.
With the default cap of 0 it decodes nothing, and the client's bytes go to the validator exactly as received.
With a cap set, it decodes each transaction, without recovering the signer, to price it, so empty and undecodable bytes are refused locally as well.
Anything that passes, including junk, reaches the validator, which spends its own CPU on decoding and signature recovery before it refuses it.

On, the node runs these checks in order and refuses the transaction locally at the first one that fails, with the error object a non-forwarding Telcoin Network node returns for a transaction with that one defect:

1. The bytes are empty.
2. The bytes do not decode as a signed transaction. The network form of an EIP-4844 transaction decodes here and is refused at step 5.
3. The maximum fee is above `--rpc.txfeecap`.
4. The signer cannot be recovered, which includes signatures with a high `s` value.
5. The transaction type is not legacy, EIP-2930 or EIP-1559. EIP-4844 blob and EIP-7702 transactions are refused.
6. The transaction names a chain id other than the node's. A legacy transaction without a chain id passes.

Nonce, balance and fee-market checks stay with the validator.
So do the data size, gas limit and tip checks, which a validator runs before the chain id check, so a transaction with several defects can receive a different error from the public node than from a validator; both refuse it.
Sanitizing costs one decode and one signature recovery per submission on the public node.
In return the validator receives only well-formed, signed transactions of an allowed type for this chain.
Turn it on for any node that faces the internet.

## Errors

| Situation | Client receives |
| --- | --- |
| The validator accepts the transaction | The validator's transaction hash. For `eth_sendRawTransactionSync`, the receipt from the public node's own chain, or reth's confirmation-timeout error if the transaction does not execute within 30 seconds. |
| The validator returns a JSON-RPC error | That error object unchanged: `code`, `message` and `data`. |
| Over `--rpc.txfeecap` (with or without sanitizing) | `-32000` `tx fee (X wei) exceeds the configured cap (Y wei)`, from the public node. |
| Sanitize: empty, undecodable, or bad signature | `-32602` `empty transaction data`, `failed to decode signed transaction`, or `invalid transaction signature`. |
| Sanitize: EIP-4844, EIP-7702, or another type outside the allowlist | `-32003` `transaction type not supported`. |
| Sanitize: wrong chain id | `-32000` `invalid chain ID`. |
| A target answers HTTP 413 (request too large) | `-32007` `Request is too big`, with no `data`. No other target is tried. |
| One target fails | Nothing. The next target is tried. |
| Every target fails, or the 15-second budget runs out | `-32603` `transaction submission unavailable`, with no `data`. |

The unavailable error is one fixed object.
It names no target and carries no timing detail, so a client cannot learn the validator's address or which target failed.

A timeout does not mean the validator refused the transaction.
If the validator accepted it but its reply took longer than 5 seconds, the public node moves on: the next target may answer `already known`, and if no target answers, the client receives `-32603`.
Either way the transaction can still execute.
A client that retries the same signed bytes receives the validator's `already known` error.
Treat that as accepted and follow the transaction by hash with [eth\_getTransactionReceipt](../rpc-methods/eth_gettransactionreceipt.md).
If the retry lands on a different validator, both may hold the transaction; it still executes only once.

## Limitations

- A forwarded transaction is not in the public node's pool. Until it executes, `eth_getTransactionByHash` on that node returns `null` for it, and `eth_getTransactionCount` with the `pending` tag does not count it. A wallet that takes its next nonce from the `pending` count on the same node reuses a nonce when it sends a second transaction before the first executes, so send several transactions in a row only from a client that tracks its own nonces.
- `eth_sendTransaction` is not forwarded. It fails with `unknown account`, as on every Telcoin Network node (see [eth\_sendTransaction](../rpc-methods/eth_sendtransaction.md)).
- Every worker lane on the public node forwards to the same list, and a target is the validator's worker 0 endpoint, so the validator's worker 0 receives every forwarded transaction.
- On a public node with several workers, lane `k`'s `eth_gasPrice` quotes worker `k`'s base fee, but the transaction lands on the validator's worker 0 lane, whose base fee may differ. A transaction priced for lane `k` can sit below worker 0's base fee: the client receives a hash, and `eth_sendRawTransactionSync` times out.
- A JSON-RPC batch is not forwarded as a batch. Each submission in it is forwarded as its own request and uses its own request slot.
- Submissions over WebSocket and IPC are forwarded the same way as over HTTP.
- `eth_sendRawTransactionSync` waits for the receipt on the public node, so a public node that lags the chain returns the confirmation-timeout error for transactions that did execute.

## Metrics

With `--metrics`, the public node exports two counters.
Both are registered at zero at startup, every label combination included, so a missing series means forwarding is off.

| Metric | Labels | Counts |
| --- | --- | --- |
| `tn_reth_rpc_tx_forwarded_total` | `outcome`: `accepted`, `upstream_error`, `rejected_locally`, `unavailable` | Every submission, by how it ended. |
| `tn_reth_rpc_tx_forward_target_failures_total` | `target`: the target's position in the list, starting at `0`; `kind`: `timeout`, `transport`, `malformed` | Every failed attempt against one target. |

The outcomes split all submissions the forwarding handler received:

- `accepted`: a target returned a hash.
- `upstream_error`: a target returned a JSON-RPC error, which the client received unchanged, or a target answered HTTP 413 and the client received `-32007`. This is normal traffic: nonce too low, already known, underpriced. Alert when it climbs while `accepted` stays flat: a target that answers every request with an error, such as `method not found` from a port that does not serve the `eth` namespace, is never failed over.
- `rejected_locally`: the fee cap or a sanitize check refused the transaction before any target saw it.
- `unavailable`: no target answered and the client received `-32603`. Alert on any increase: while it lasts, the node refuses every submission.

The failure kinds:

- `timeout`: no reply within the attempt timeout.
- `transport`: the connection failed, the target answered with a non-2xx status other than 413, or the body was over 64 KiB, empty, or not JSON (an HTML error page from a proxy, for example).
- `malformed`: the target answered 2xx with JSON that is not a JSON-RPC reply to the request: it does not parse as a JSON-RPC response, carries another request's `id`, or its result is not a transaction hash.

Labels name a target by its index, never by its URL.
The target list is private configuration, and a metrics endpoint is not.

## Running on a committee member

`--forward-txs` is meant for observers.
A committee member started with it keeps running and logs one `warn` line per process.
Its RPC submissions then bypass its own pool and go to the targets, so its own workers never batch the transactions sent to its RPC.

## Why the node forwards

Two other designs were considered.
A gateway process in front of the observer would put a buffering HTTP/1-only hop in front of every read, with no WebSocket and no HTTP/2, and it would need its own signer recovery, request routing, batch splitting and TLS client.
A separate public-RPC binary would duplicate both the gateway and the node.
Forwarding inside the node changes only the two submission methods, keeps reads on HTTP, WebSocket and IPC exactly as before, reuses the fee-cap guard and the node's decode helpers, and runs as one process per public node.

## Possible extension

An opt-in pool mirror could also insert each forwarded transaction into the public node's own pool, without that pool forwarding it a second time, so `eth_getTransactionByHash` and the `pending` nonce on the public node see it before it executes.
It is not built.
