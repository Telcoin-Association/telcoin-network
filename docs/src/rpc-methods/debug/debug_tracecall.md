# debug\_traceCall

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Fee fields at `latest` and `pending`**
>
> At `latest` and `pending` this method prices the call against the block header's base fee, not the worker's current epoch base fee that `eth_call` and `eth_estimateGas` use. A request that sets `gasPrice`, `maxFeePerGas` or `maxPriorityFeePerGas` can therefore be accepted by `eth_call` and rejected here, or the reverse; requests without fee fields are unaffected. Tracked in [#1587](https://github.com/Telcoin-Association/telcoin-network/issues/1587).

Runs a call as [eth\_call](../eth/eth_call.md) does, without creating a transaction, and returns the trace the chosen tracer builds. Without `txIndex` the call runs on the state after the given block. With `txIndex` the node starts from the block's parent state, applies the block's pre-transaction system calls, replays the block's transactions before that index, and runs the call on top.

#### Parameters

`Object` - The transaction call object:

* `from`: `DATA`, 20 bytes - (optional) The address the call is sent from.
* `to`: `DATA`, 20 bytes - The address the call is directed to.
* `gas`: `QUANTITY` - (optional) Hexadecimal value of the gas provided for the call.
* `gasPrice`: `QUANTITY` - (optional) Hexadecimal value of the `gasPrice` used for each paid gas.
* `maxPriorityFeePerGas`: `QUANTITY` - (optional) Hexadecimal maximum fee, in Wei, the sender is willing to pay per gas above the base fee.
* `maxFeePerGas`: `QUANTITY` - (optional) Hexadecimal maximum total fee (base fee + priority fee), in Wei, the sender is willing to pay per gas.
* `value`: `QUANTITY` - (optional) Hexadecimal of the value sent with the call.
* `data`: `DATA` - (optional) Hash of the method signature and encoded parameters. See [Ethereum contract ABI specification](https://docs.soliditylang.org/en/latest/abi-spec.html).

`block parameter`: `QUANTITY|TAG|DATA` - (optional) Hexadecimal block number, or one of the string tags `latest`, `pending`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). Defaults to `latest`.

`Object` - (optional) The tracing options: the fields [debug\_traceTransaction](debug_tracetransaction.md#parameters) takes (`tracer`, `tracerConfig`, `enableMemory`, `disableStack`, `disableStorage`, `enableReturnData`), plus:

* `stateOverrides`: `Object` - (optional) Account overrides applied before the call, keyed by address, each with any of `balance`, `nonce`, `code`, and `state` or `stateDiff`.
* `blockOverrides`: `Object` - (optional) Block environment overrides, such as `number`, `time`, `gasLimit`, `coinbase` and `baseFee`.
* `txIndex`: `QUANTITY` - (optional) Run the call on the state just before the block's transaction at this index. An index at or past the block's transaction count fails with error `-32602` `tx_index <index> out of bounds for block with <count> transactions`.

#### Returns

`Object` - The tracer's output for the call, in the shapes [debug\_traceTransaction](debug_tracetransaction.md#returns) describes.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceCall","params":[{
  "from":"0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
  "to":"0x0000000000000000000000000000000000000000",
  "gas":"0x5208",
  "value":"0x64"},
  "latest",
  {"tracer":"callTracer"}],"id":1}'
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

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_tracecall)
