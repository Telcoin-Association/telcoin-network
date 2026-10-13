# net\_listening

On Telcoin Network this method always returns `true`, whether or not the node has any peers. Use [net\_peerCount](net_peercount.md) to see how many peers the node is connected to.

#### Parameters

`None`

#### Returns

`Boolean` - `true` when listening, otherwise `false`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"net_listening","params":[],"id":67}'
```

#### Result

```
{
    "id":67,
    "jsonrpc":"2.0",
    "result":true
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#net_listening)
