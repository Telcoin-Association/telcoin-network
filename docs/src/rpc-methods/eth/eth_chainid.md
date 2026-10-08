# eth\_chainId

#### Parameters

`None`

#### Returns

`chainId` - Hexadecimal value as a string representing the integer of the current chain id.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":62}'
```

#### Result

```
{
  "id":62,
  "jsonrpc": "2.0",
  "result": "0x7e1"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_chainid)
