---
description: >-
  Returns an estimate of how much priority fee, in wei, you need to be included
  in a block.
---

# eth\_maxPriorityFeePerGas

#### Parameters

`None`

#### Returns

`QUANTITY` - Hexadecimal of the priority fee, in wei, needed to be included in a block.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_maxPriorityFeePerGas","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x3b9aca00",
  "id": 1
}
```

[source](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth\_maxpriorityfeepergas)
