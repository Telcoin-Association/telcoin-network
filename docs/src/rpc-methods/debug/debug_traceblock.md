# debug\_traceBlock

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Decodes an RLP-encoded block, re-executes every transaction in it, and returns one trace per transaction. The block itself does not need to be in the node's database, but its parent does: the node starts from the parent's state, applies the block's pre-transaction system calls, and then traces the transactions in order. A block whose parent the node does not have fails with error `-32001` `block not found: hash <parent hash>`.

Tracing the genesis block (`0x0`) fails with error `-32001` `block not found: hash 0x0000000000000000000000000000000000000000000000000000000000000000`: the tracer looks up the genesis block's parent, whose hash is zero. `trace_block` returns `[]` for genesis instead.

Traces cover the block's transactions only. The system calls a block runs before its transactions (EIP-4788 beacon root, EIP-2935 block hashes) and the epoch-closing system calls (`applyIncentives` and `concludeEpoch`) are not transactions, so no tracer shows them.

#### Parameters

`DATA` - The RLP-encoded block, as [debug\_getRawBlock](debug_getrawblock.md) returns it. Bytes that do not decode as a block fail with error `-32603`.

`Object` - (optional) The tracing options, as for [debug\_traceTransaction](debug_tracetransaction.md#parameters): `tracer`, `tracerConfig`, and the struct logger fields `enableMemory`, `disableStack`, `disableStorage` and `enableReturnData`. When omitted, the struct logger runs with its defaults.

#### Returns

`Array` - One object per transaction, in block order:

* `result`: `Object` - The tracer's output for the transaction, in the shape [debug\_traceTransaction](debug_tracetransaction.md#returns) describes.
* `txHash`: `DATA`, 32 Bytes - The transaction hash.

A block without transactions returns `[]`. If any transaction fails to trace, the whole request fails with an error.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_traceBlock","params":["0xf902cdf9025da05f4827e84833a6da2a79b60373cdb82b5142ef13b1f56ac4790f2c6cfe3112cda04ef7ab685b6104388fca116acbe551cfa24018eb490cfcb7d0a26146a09ae25f94342b9c7c28d3f8680675ab13cf5f215897ae9f95a010620e8dae725248ad5102bde4780354184a4c99b67d31cc857879b93b56c12aa00a7b8d761adb0cfb421ea88e77c2e06ac53a9e9923353ca9b92f6fa92ee033f1a0f78dfb743fbd92ade140711c8bbc542b5e307f0ab7984eff35d751969fe57efab901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080018401c9c380825208846ac5cdc480a01fdc430e2e6fef312e3e6a5a122420cd8b5b825bd853869f9d3d9793721140a088000000000000000107a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a047b526d2ce612759ecbd3a2152384a7bf714b83b377323d1b931678148ef4584a0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855f869b86702f8648207e18080078252089400000000000000000000000000000000000000006480c080a094e74c15812b53176f8000333c19c62852cf6f4e465adc9ce79f467f13a35736a005b01594de729950fc2153a8768ee98c0c2eb20b6e8c501903ca540083890429c0c0",{"tracer":"callTracer"}],"id":1}'
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

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_traceblock)
