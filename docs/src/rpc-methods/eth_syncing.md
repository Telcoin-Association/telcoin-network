# eth\_syncing

#### Parameters

`None`

#### Returns

`Boolean` - Telcoin Network nodes always return `false`.

Standard Ethereum clients return either a sync-status object (`startingBlock`, `currentBlock`, `highestBlock`) or `false` when not syncing. Telcoin Network nodes execute consensus output directly and always report `false` here, even while a node is still catching up to the rest of the network.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": false
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_syncing)
