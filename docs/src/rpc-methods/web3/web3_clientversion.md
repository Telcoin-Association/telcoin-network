# web3\_clientVersion

On Telcoin Network this returns the node's Telcoin Network version string, `<crate version> (<git commit sha>)`, not a geth or reth client string. A node built from the repository's Docker image, as adiri is, reports the full 40-character commit sha, as in the example below; a binary built with `cargo` from a git checkout reports the short sha.

#### Parameters

`None`

#### Returns

`String` - The node's version string.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"web3_clientVersion","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0.1.0 (5736cc30012c5ff25913898e318a74df308f13d9)"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#web3_clientversion)
