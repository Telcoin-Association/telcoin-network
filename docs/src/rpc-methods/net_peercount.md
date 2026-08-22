---
description: Returns number of peers currently connected to the client.
---

# net\_peerCount

#### Parameters

`None`

#### Returns

`QUANTITY` - integer of the number of connected peers.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0x3" //3
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#net\_version)
