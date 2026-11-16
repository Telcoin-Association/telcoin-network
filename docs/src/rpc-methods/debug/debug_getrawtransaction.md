# debug\_getRawTransaction

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Returns a transaction in its EIP-2718 binary encoding: the type byte followed by the RLP payload, or the bare RLP for a legacy transaction.

#### Parameters

`DATA`, 32 Bytes - The transaction hash.

#### Returns

`DATA` - The EIP-2718 encoded transaction, or `null` when the node does not know the transaction.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_getRawTransaction","params":["0xa794c7eb915c26fdb77242159053c1a36241bcd016b671fb07abe3fa9050805e"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x02f8648207e18080078252089400000000000000000000000000000000000000006480c080a094e74c15812b53176f8000333c19c62852cf6f4e465adc9ce79f467f13a35736a005b01594de729950fc2153a8768ee98c0c2eb20b6e8c501903ca540083890429" // EIP-1559 transfer of 100 wei
}
```

[source](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debug_getrawtransaction)
