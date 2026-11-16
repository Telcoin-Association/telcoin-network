# eth\_syncing

#### Parameters

`None`

#### Returns

`Object|Boolean` - A sync-status object while the node is syncing, `false` otherwise.

The node reports syncing from startup until its first epoch sets its mode, and while it is a committee validator catching up on consensus output before it takes part again. A caught-up validator and an observer report `false`. The sync-status object has these fields:

* `startingBlock`: `QUANTITY` - The node's latest block when its RPC server started.
* `currentBlock`: `QUANTITY` - The node's latest executed block.
* `highestBlock`: `QUANTITY` - The same as `currentBlock`: the node does not report the network's tip.
* `stages`: `Array` - reth's stage checkpoints, each a `name` and a `block`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": false
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_syncing)
