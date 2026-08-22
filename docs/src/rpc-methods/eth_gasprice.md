---
description: Returns an estimate of the current price per gas in wei.
---

# eth\_gasPrice

#### Parameters

`None`

#### Returns

`QUANTITY`- Hexadecimal integer of the current gas price in wei.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_gasPrice","params":[],"id":73}'
```

#### Result

```
{
  "id":73,
  "jsonrpc": "2.0",
  "result": "0x3b9aca07" // 1000000007 Wei
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_gasprice)
