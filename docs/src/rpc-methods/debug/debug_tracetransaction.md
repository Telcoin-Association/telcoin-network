# debug\_traceTransaction

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Re-executes a mined transaction and returns the trace the chosen tracer builds. The node starts from the state before the transaction's block, applies the block's pre-transaction system calls, replays the transactions that precede it in the block, and then traces the transaction itself.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`DATA`, 32 Bytes - The transaction hash.

`Object` - (optional) The tracing options. When omitted, the struct logger runs with its defaults:

* `tracer`: `String` - (optional) The tracer to run: `callTracer`, `flatCallTracer`, `prestateTracer`, `4byteTracer`, `muxTracer`, `noopTracer` or `erc7562Tracer`, or the source code of a JavaScript tracer. Without it, the struct logger records every executed opcode, with its stack unless `disableStack` is set and its memory when `enableMemory` is set. Every built-in tracer but `4byteTracer` and `noopTracer` also records each call with its input and output. A trace that records more than 160 MiB of steps and calls fails with error `-32603` `trace too large`. For steps, and for the call input and output that `callTracer` and `flatCallTracer` return in hex, the response limit (`--rpc.max-response-size`, 160 MB by default) would have refused a trace that large anyway; the struct logger and `prestateTracer` return no call input or output, so they also fail on a transaction whose calls copy more than 160 MiB of input or output. Set `disableStack`, or use `callTracer`, for transactions that run many opcodes. JavaScript tracers need a node built with the `js-tracer` feature, which is off by default; other nodes answer error `-32603` `JS Tracer is not enabled`. See [JavaScript tracers](https://github.com/Telcoin-Association/telcoin-network/blob/main/crates/tn-reth/js-tracer.md).
* `tracerConfig`: `Object` - (optional) Settings for the chosen tracer, for example `{"onlyTopCall":true}` or `{"withLog":true}` for `callTracer`, or `{"diffMode":true}` for `prestateTracer`.
* `enableMemory`: `Boolean` - (optional) Struct logger only: record memory at each step. Defaults to `false`.
* `disableStack`: `Boolean` - (optional) Struct logger only: leave out the stack at each step. Defaults to `false`.
* `disableStorage`: `Boolean` - (optional) Struct logger only: leave out the storage slots each `SLOAD` and `SSTORE` touches. Defaults to `false`.
* `enableReturnData`: `Boolean` - (optional) Struct logger only: record the last call's return data at each step. Defaults to `false`.

#### Returns

`Object` - The tracer's output. Two common shapes:

The struct logger (no `tracer`) returns:

* `failed`: `Boolean` - Whether the transaction failed.
* `gas`: `Number` - Gas used, as a decimal JSON number.
* `returnValue`: `DATA` - The transaction's return data.
* `structLogs`: `Array` - One object per executed opcode with `pc`, `op`, `gas`, `gasCost` and `depth` (decimal numbers except `op`), plus `stack`, `memory`, `storage` and `returnData` when the options above enable them. A plain value transfer runs no code, so for the example transaction the struct logger returns `{"failed":false,"gas":21000,"returnValue":"0x","structLogs":[]}`.

`callTracer` returns the top-level call frame:

* `from`: `DATA`, 20 Bytes - The caller.
* `gas`: `QUANTITY` - Gas available to the call.
* `gasUsed`: `QUANTITY` - Gas used by the call, including intrinsic gas for the top-level frame.
* `to`: `DATA`, 20 Bytes - The callee, or the new contract for `CREATE` and `CREATE2`; omitted when there is none.
* `input`: `DATA` - The call data.
* `output`: `DATA` - The return data; omitted when there is none.
* `error`: `String` - The error, if the call failed; omitted otherwise.
* `revertReason`: `String` - The decoded revert reason, if the call reverted; omitted otherwise.
* `calls`: `Array` - Child call frames of the same shape; omitted when there are none.
* `logs`: `Array` - Logs emitted by the call, with `tracerConfig` `{"withLog":true}`; omitted when there are none.
* `value`: `QUANTITY` - The value transferred.
* `type`: `String` - The call type, such as `CALL`, `STATICCALL`, `DELEGATECALL`, `CREATE` or `CREATE2`.

A hash the node does not know fails with error `-32000` `transaction not found` (the code geth uses; other `eth` lookups keep `-32001`).

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e",{"tracer":"callTracer"}],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "from": "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
    "gas": "0x5208",
    "gasUsed": "0x5208",
    "to": "0x0000000000000000000000000000000000000000",
    "input": "0x",
    "value": "0x64",
    "type": "CALL"
  }
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_tracetransaction)
