# net\_version

On Telcoin Network this is the chain id as a decimal string: `"2017"` on adiri. It is the same number [eth\_chainId](../eth/eth_chainid.md) returns in hex (`0x7e1`).

#### Parameters

`None`

#### Returns

`String` - The chain id in decimal.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"net_version","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "2017"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#net_version)
