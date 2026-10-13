# net\_peerCount

On Telcoin Network this is the number of peers connected to the node's worker network, not devp2p peers: Telcoin Network does not run devp2p. The node refreshes the count every 15 seconds, so a new or dropped connection can take up to 15 seconds to show, and the count reads `0x0` while the node is starting up, until the worker network is running.

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

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#net_peercount)
