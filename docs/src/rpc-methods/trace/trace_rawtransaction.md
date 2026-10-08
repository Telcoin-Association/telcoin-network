# trace\_rawTransaction

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Fee fields at `latest` and `pending`**
>
> At `latest` and `pending` this method checks the transaction's fee cap against the block header's base fee, not the worker's current epoch base fee that the transaction pool and `eth_call` use. A signed transaction always carries a fee cap, so a transaction the node would accept can fail here with a fee-cap error, or the reverse. Tracing against a numbered block avoids the difference. Tracked in [#1587](https://github.com/Telcoin-Association/telcoin-network/issues/1587).

Executes a signed raw transaction on the state of a block and returns the trace types requested, without sending it to the transaction pool. The node recovers the sender from the signature, so the transaction must be signed; to trace an unsigned call, use [trace\_call](trace_call.md).

The example traces the transfer that block 1 holds against the state at block `0x0`, which is the state block 1 executed it on.

#### Parameters

`DATA` - The signed transaction, EIP-2718 encoded, as [eth\_sendRawTransaction](../eth/eth_sendrawtransaction.md) takes it.

`Array` - The trace types to return, any of `"trace"`, `"vmTrace"` and `"stateDiff"`.

`QUANTITY|TAG|DATA` - (optional) The block whose state the transaction runs on: a hexadecimal block number, a 32-byte block hash, or one of the string tags `latest`, `earliest`, `safe` or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). Defaults to `latest`.

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
 --data '{"jsonrpc":"2.0","method":"trace_rawTransaction","params":["0x02f8648207e18080078252089400000000000000000000000000000000000000006480c080a094e74c15812b53176f8000333c19c62852cf6f4e465adc9ce79f467f13a35736a005b01594de729950fc2153a8768ee98c0c2eb20b6e8c501903ca540083890429",["trace"],"0x0"],"id":1}'
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
