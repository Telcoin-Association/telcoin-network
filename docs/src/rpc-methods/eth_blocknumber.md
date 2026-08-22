---
description: Returns the current latest block number.
---

# eth\_blockNumber

#### Parameters

`None`

#### Returns

`QUANTITY` - Hexadecimal value of the current block number the client is on.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}'
```

#### Result

```
{
"jsonrpc":"2.0",
"result":"0x7328cb", //7547083
"id":83
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_blocknumber)
