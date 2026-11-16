# eth\_getUncleCountByBlockNumber

Telcoin Network's consensus produces no uncle blocks, so this method returns `0x0` for every existing block (`null` when the block is not found). It is served for Ethereum API compatibility.

#### Parameters

`QUANTITY|TAG` - Hexadecimal of a block number, or the string `"latest"`, `"earliest"`, `"safe"` or `"finalized"`, see the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block)

#### Returns

`QUANTITY` - Hexadecimal of the number of uncles in this block.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getUncleCountByBlockNumber","params":["0x6038C1"],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0x0"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getunclecountbyblocknumber)
