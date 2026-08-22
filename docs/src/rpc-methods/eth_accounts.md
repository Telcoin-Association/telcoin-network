---
description: Returns a list of addresses owned by client
---

# eth\_accounts

#### Parameters

`None`

#### Returns

`Array` of `DATA`, 20 Bytes - Addresses owned by the client.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_accounts","params":[],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": []
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_accounts)
