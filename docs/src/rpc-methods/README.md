# Namespaces

Telcoin Network nodes serve the Ethereum JSON-RPC interface over HTTP, WebSocket and IPC, so wallets, dapps and tooling built for Ethereum can read the chain and submit transactions without changes.

Methods are grouped into namespaces. A namespace is the prefix before the underscore in a method name: `eth_call` belongs to `eth`, `tn_getCurrentEpoch` to `tn`. An operator chooses which namespaces each transport serves with `--http.api` and `--ws.api`; [Enabling Namespaces](enabling-namespaces.md) explains the flags, the defaults and how to check what a node serves.

| Namespace | On by default | Cost to serve | What it covers |
| --- | --- | --- | --- |
| [eth](eth/README.md) | yes | low | Ethereum-compatible chain, account, transaction, simulation and filter methods |
| [net](net/README.md) | yes | low | Chain id, listening state and the worker network's peer count |
| [web3](web3/README.md) | yes | low | Client version and Keccak-256 hashing |
| [rpc](rpc/README.md) | yes | trivial | The namespaces the transport serves |
| [tn](tn/README.md) | yes | low to moderate (blocking reads are capped at 64 concurrent requests) | Telcoin Network consensus, epoch, validator and staking data |
| [debug](debug/README.md) | no, only when named | high | Raw block and transaction data and geth-style EVM tracing |
| [trace](trace/README.md) | no, only when named | high | Parity-style call traces, replays and trace filters |

`debug` and `trace` re-execute historical transactions against archive state. One request can cost far more than an `eth_*` call, so they are never part of the default set or of `all`.

## Recommended exposure

| Node role | `--http.api` / `--ws.api` | Notes |
| --- | --- | --- |
| Core validator | `eth,net,web3` | Serve RPC only on a private or gateway interface; see [network topology](../getting-started/validator-operations.md#network-topology). Keep `eth`: observers forward `eth_sendRawTransaction` to a committee validator's advertised RPC endpoint. |
| Public RPC (observer) | `eth,net,web3,rpc,tn` | The default set; no flag is needed. |
| Private archive or indexer | `eth,net,web3,rpc,tn,debug,trace` | Keep behind authentication or a method allowlist, and size `--rpc.max-tracing-requests` for the host. |

The JSON-RPC methods are documented per namespace in the sections below. Each page lists the parameters, the result, and a `curl` request with its response.
