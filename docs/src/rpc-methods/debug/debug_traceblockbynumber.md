# debug\_traceBlockByNumber

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Re-executes every transaction of a block the node holds and returns one trace per transaction. The node starts from the parent block's state, applies the block's pre-transaction system calls, and then traces the transactions in order, each on the state the previous one left.

Tracing the genesis block (`0x0`) fails with error `-32001` `block not found: hash 0x0000000000000000000000000000000000000000000000000000000000000000`: the tracer looks up the genesis block's parent, whose hash is zero. `trace_block` returns `[]` for genesis instead.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`block parameter`: `QUANTITY|TAG` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). A block the node does not have fails with error `-32001` and a message such as `block not found: 0x5`.

`Object` - (optional) The tracing options, as for [debug\_traceTransaction](debug_tracetransaction.md#parameters): `tracer`, `tracerConfig`, and the struct logger fields `enableMemory`, `disableStack`, `disableStorage` and `enableReturnData`. When omitted, the struct logger runs with its defaults.

#### Returns

`Array` - One object per transaction, in block order:

* `result`: `Object` - The tracer's output for the transaction, in the shape [debug\_traceTransaction](debug_tracetransaction.md#returns) describes.
* `txHash`: `DATA`, 32 Bytes - The transaction hash.

A block without transactions returns `[]`. If any transaction fails to trace, the whole request fails with an error.

With the struct logger, each `result` is a struct log object; for the example block it is `{"failed":false,"gas":21000,"returnValue":"0x","structLogs":[]}`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":["0x1",{"tracer":"callTracer"}],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "result": {
        "from": "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
        "gas": "0x5208",
        "gasUsed": "0x5208",
        "to": "0x0000000000000000000000000000000000000000",
        "input": "0x",
        "value": "0x64",
        "type": "CALL"
      },
      "txHash": "0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e"
    }
  ]
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_traceblockbynumber)
