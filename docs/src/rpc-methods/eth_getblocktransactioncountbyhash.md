---
description: >-
  Returns the number of transactions in a block from a block matching the given
  block hash.
---

# eth\_getBlockTransactionCountByHash

#### Parameters

`DATA`, 32 Bytes - Hash of a block

#### Returns

`QUANTITY` - Hexadecimal of the number of transactions in this block.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getBlockTransactionCountByHash","params":["0x5fb9fff1f0f914360abfefb0eac9c7d4fc733007c62c7caf1860d4052776b7d7"],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0x1" // 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getblocktransactioncountbyhash)
