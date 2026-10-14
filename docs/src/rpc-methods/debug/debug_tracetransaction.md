# debug\_traceTransaction

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Re-executes a mined transaction and returns the trace the chosen tracer builds. The node starts from the state before the transaction's block, applies the block's pre-transaction system calls, replays the transactions that precede it in the block, and then traces the transaction itself.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`DATA`, 32 Bytes - The transaction hash.

`Object` - (optional) The tracing options. When omitted, the struct logger runs with its defaults:

* `tracer`: `String` - (optional) The tracer to run: `callTracer`, `flatCallTracer`, `prestateTracer`, `4byteTracer`, `muxTracer`, `noopTracer` or `erc7562Tracer`, or the source code of a JavaScript tracer. Without it, the struct logger records every executed opcode.
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

A hash the node does not know fails with error `-32001` `transaction not found`.

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
