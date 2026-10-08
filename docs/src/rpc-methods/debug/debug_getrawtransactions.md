# debug\_getRawTransactions

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns every transaction of a block in its EIP-2718 binary encoding, in block order.

#### Parameters

`block parameter`: `QUANTITY|TAG|DATA` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

`Array` - One `DATA` element per transaction, each encoded as [debug\_getRawTransaction](debug_getrawtransaction.md) returns it. A block without transactions, or a block the node does not have, returns `[]`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_getRawTransactions","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    "0x02f8648207e18080078252089400000000000000000000000000000000000000006480c080a094e74c15812b53176f8000333c19c62852cf6f4e465adc9ce79f467f13a35736a005b01594de729950fc2153a8768ee98c0c2eb20b6e8c501903ca540083890429"
  ]
}
```

[source](https://reth.rs/jsonrpc/debug)
