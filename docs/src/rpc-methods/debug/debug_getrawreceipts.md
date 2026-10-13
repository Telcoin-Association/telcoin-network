# debug\_getRawReceipts

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns the receipts of a block in their EIP-2718 binary encoding, in transaction order.

#### Parameters

`block parameter`: `QUANTITY|TAG|DATA` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

`Array` - One `DATA` element per receipt: the transaction type byte (none for a legacy transaction) followed by the RLP list of status, cumulative gas used, logs bloom and logs. A block without transactions, or a block the node does not have, returns `[]`.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_getRawReceipts","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    "0x02f9010801825208b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c0" // type 0x02, status 1, cumulative gas 0x5208, empty bloom, no logs
  ]
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_getrawreceipts)
