# eth\_getUncleCountByBlockHash

Telcoin Network's consensus produces no uncle blocks, so this method returns `0x0` for every existing block (`null` when the block is not found). It is served for Ethereum API compatibility.

#### Parameters

`DATA`, 32 Bytes - Hash of a block

#### Returns

`QUANTITY` - Hexadecimal of the number of uncles in this block.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getUncleCountByBlockHash","params":["0x908539f74911930cb0e7201a43dbcce8743a58f1d49e2bc271159fab3c6cb8fb"],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0x0"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_getunclecountbyblockhash)
