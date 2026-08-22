# eth\_newBlockFilter

#### Parameters

`None`

#### Returns

`DATA` - A filter id.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_newBlockFilter","params":[],"id":73}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x6f3ccb09d7153d2bba6b59f8f0ede3b1",
  "id": 73
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_newblockfilter)
