# debug\_getRawHeader

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns a block header as RLP-encoded bytes.

#### Parameters

`block parameter`: `QUANTITY|TAG|DATA` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). When the node has no pending block, the `pending` tag fails with error `-32603` `Pending block not supported`.

#### Returns

`DATA` - The RLP-encoded header. For a block the node does not have, the result is `0x` (empty bytes), not an error.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_getRawHeader","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0xf9025da05f4827e84833a6da2a79b60373cdb82b5142ef13b1f56ac4790f2c6cfe3112cda04ef7ab685b6104388fca116acbe551cfa24018eb490cfcb7d0a26146a09ae25f94342b9c7c28d3f8680675ab13cf5f215897ae9f95a010620e8dae725248ad5102bde4780354184a4c99b67d31cc857879b93b56c12aa00a7b8d761adb0cfb421ea88e77c2e06ac53a9e9923353ca9b92f6fa92ee033f1a0f78dfb743fbd92ade140711c8bbc542b5e307f0ab7984eff35d751969fe57efab901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080018401c9c380825208846ac5cdc480a01fdc430e2e6fef312e3e6a5a122420cd8b5b825bd853869f9d3d9793721140a088000000000000000107a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a047b526d2ce612759ecbd3a2152384a7bf714b83b377323d1b931678148ef4584a0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_getrawheader)
