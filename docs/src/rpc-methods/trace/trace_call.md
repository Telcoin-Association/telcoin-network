# trace\_call

> [!NOTE]
> Served only when `trace` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Fee fields at `latest` and `pending`**
>
> At `latest` and `pending` this method prices the call against the block header's base fee, not the worker's current epoch base fee that `eth_call` and `eth_estimateGas` use. A request that sets `gasPrice`, `maxFeePerGas` or `maxPriorityFeePerGas` can therefore be accepted by `eth_call` and rejected here, or the reverse; requests without fee fields are unaffected. Tracked in [#1587](https://github.com/Telcoin-Association/telcoin-network/issues/1587).

Executes a call on the state of a block without creating a transaction, and returns the trace types requested: the call frames, the executed instructions, the state changes, or any combination. Nothing is sent or stored.

#### Parameters

`Object` - The transaction call object, as for [eth\_call](../eth/eth_call.md):

* `from`: `DATA`, 20 bytes - (optional) The address the call is sent from.
* `to`: `DATA`, 20 bytes - The address the call is directed to. Leave it out to simulate a contract creation.
* `gas`: `QUANTITY` - (optional) The gas limit for the call. The top-level trace's `action.gas` is this limit minus the call's intrinsic gas.
* `gasPrice`: `QUANTITY` - (optional) The price paid per unit of gas, for a legacy-style call.
* `maxFeePerGas`: `QUANTITY` - (optional) The most, in wei, the sender pays per unit of gas, base fee and priority fee together.
* `maxPriorityFeePerGas`: `QUANTITY` - (optional) The most, in wei, the sender pays per unit of gas above the base fee.
* `value`: `QUANTITY` - (optional) The value sent with the call, in wei.
* `input`: `DATA` - (optional) The call data. `data` is accepted as an alias.

`Array` - The trace types to return, any of `"trace"`, `"vmTrace"` and `"stateDiff"`.

`QUANTITY|TAG|DATA` - (optional) The block whose state the call runs on: a hexadecimal block number, a 32-byte block hash, or one of the string tags `latest`, `earliest`, `safe` or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). Defaults to `latest`.

`Object` - (optional) State overrides, keyed by account address. Each entry may set `balance`, `nonce`, `code`, `state` (replaces the account's whole storage) or `stateDiff` (overrides single storage slots).

`Object` - (optional) Block overrides for the block the call runs in, such as `number`, `time`, `gasLimit`, `coinbase` or `baseFee`.

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
 --data '{"jsonrpc":"2.0","method":"trace_call","params":[{
  "from":"0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
  "to":"0x0000000000000000000000000000000000000000",
  "gas":"0x5208",
  "value":"0x64"},
  ["trace"],
  "latest"],"id":1}'
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
