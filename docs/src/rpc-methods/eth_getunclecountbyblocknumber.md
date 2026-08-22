---
description: >-
  Returns the number of uncles in a block from a block matching the given block
  number.
---

# eth\_getUncleCountByBlockNumber

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
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getunclecountbyblocknumber)
