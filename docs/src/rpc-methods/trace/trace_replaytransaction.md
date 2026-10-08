# trace\_replayTransaction

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Replays a mined transaction and returns the trace types requested: the call frames, the executed instructions, the state changes, or any combination. The node re-executes the transaction's block up to and including the transaction. A transaction the node does not know fails with `-32001` and the message `transaction not found`.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`DATA`, 32 Bytes - The transaction hash.

`Array` - The trace types to return, any of `"trace"`, `"vmTrace"` and `"stateDiff"`.

#### Returns

`Object` - The requested traces:

* `output`: `DATA` - The return data of the top-level call; `0x` for a plain transfer.
* `stateDiff`: `Object` - With `stateDiff` requested, the state changes keyed by account address; otherwise `null`. Each account has `balance`, `code`, `nonce` and `storage` (keyed by slot). Each value is `"="` when unchanged, `{"+": value}` when created, `{"-": value}` when removed, or `{"*": {"from": old, "to": new}}` when changed.
* `trace`: `Array` - With `trace` requested, the call frames, top-level first; otherwise `[]`. Each frame has `type`, `action`, `error` (only when the frame failed), `result`, `subtraces` and `traceAddress`, with the meanings [trace\_transaction](trace_transaction.md) gives them, and no block or transaction fields. The top-level frame's `action.gas` and `result.gasUsed` leave out the intrinsic gas, so for a plain transfer with a 21,000 gas limit both are `0x0`.
* `vmTrace`: `Object` - With `vmTrace` requested, the instructions the top-level call executed; otherwise `null`. It has `code` (`DATA`, the code that ran) and `ops`, one entry per instruction with `cost` (`Number`, its gas cost), `pc` (`Number`, the program counter), `ex` (its effects: `used`, `push` for the stack items it pushed, `mem` for a memory write and `store` for a storage write), `sub` (the nested `vmTrace` of a CALL or CREATE, or `null`), and, when present, `op` (the opcode name) and `idx`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"trace_replayTransaction","params":["0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e",["trace"]],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "output": "0x",
    "stateDiff": null,
    "trace": [
      {
        "type": "call",
        "action": {
          "from": "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
          "callType": "call",
          "gas": "0x0",
          "input": "0x",
          "to": "0x0000000000000000000000000000000000000000",
          "value": "0x64"
        },
        "result": {
          "gasUsed": "0x0",
          "output": "0x"
        },
        "subtraces": 0,
        "traceAddress": []
      }
    ],
    "vmTrace": null
  }
}
```

[source](https://reth.rs/jsonrpc/trace)
