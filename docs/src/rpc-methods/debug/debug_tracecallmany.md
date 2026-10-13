# debug\_traceCallMany

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Fee fields at `latest` and `pending`**
>
> At `latest` and `pending` this method prices the call against the block header's base fee, not the worker's current epoch base fee that `eth_call` and `eth_estimateGas` use. A request that sets `gasPrice`, `maxFeePerGas` or `maxPriorityFeePerGas` can therefore be accepted by `eth_call` and rejected here, or the reverse; requests without fee fields are unaffected. Tracked in [#1587](https://github.com/Telcoin-Association/telcoin-network/issues/1587).

Runs bundles of calls in sequence on one block's state, without creating transactions, and returns a trace for every call. Each call sees the state changes of the calls before it, across bundles. Each bundle after the first runs with the block number one higher and the timestamp 12 seconds later than the bundle before it.

When all of the block's transactions are replayed (the default), the bundles run on the state after the block. With a smaller `transactionIndex` the node starts from the block's parent state and replays that many of the block's transactions first; this path does not apply the block's pre-transaction system calls.

#### Parameters

`Array` - The bundles, run in order. Each bundle is an object:

* `transactions`: `Array` - Transaction call objects, with the fields [debug\_traceCall](debug_tracecall.md#parameters) takes.
* `blockOverride`: `Object` - (optional) Block environment overrides for this bundle's calls, such as `number`, `time`, `gasLimit`, `coinbase` and `baseFee`.

`Object` - (optional) The state context:

* `blockNumber`: `QUANTITY|TAG|DATA` - (optional) The block to run on: a hexadecimal block number, a string tag such as `latest` or `pending`, or a 32-byte block hash. Defaults to `latest`.
* `transactionIndex`: `Number` - (optional) How many of the block's transactions to replay before the bundles, as a JSON integer. `-1`, the default, replays all of them.

`Object` - (optional) The tracing options: the fields [debug\_traceTransaction](debug_tracetransaction.md#parameters) takes (`tracer`, `tracerConfig`, `enableMemory`, `disableStack`, `disableStorage`, `enableReturnData`), plus `stateOverrides`, which is applied once, before the first call. `blockOverrides` and `txIndex` are ignored here; use each bundle's `blockOverride` and the state context instead.

#### Returns

`Array` - One array per bundle, each holding one trace per call in the bundle, in the shapes [debug\_traceTransaction](debug_tracetransaction.md#returns) describes. An empty bundle list fails with error `-32602` `bundles are empty.`

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceCallMany","params":[
  [{"transactions":[{
   "from":"0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
   "to":"0x0000000000000000000000000000000000000000",
   "gas":"0x5208",
   "value":"0x64"}]}],
  {"blockNumber":"latest","transactionIndex":-1},
  {"tracer":"callTracer"}],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    [
      {
        "from": "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
        "gas": "0x5208",
        "gasUsed": "0x5208",
        "to": "0x0000000000000000000000000000000000000000",
        "input": "0x",
        "value": "0x64",
        "type": "CALL"
      }
    ]
  ]
}
```

[source](https://reth.rs/jsonrpc/debug)
