# tn\_getCurrentEpoch

Returns the current epoch number as the `ConsensusRegistry` contract records it, read from the state at this node's canonical tip, the newest block it has executed. The value moves to the next epoch when the node executes the last block of an epoch, whose `concludeEpoch` system call starts the next epoch in the registry; see [Epoch Boundaries](../../epoch-boundaries.md). A node that is still catching up reports the epoch of its own tip, which can be behind the epoch of the consensus headers it has already seen through [tn\_latestConsensusHeader](tn_latestconsensusheader.md).

#### Parameters

`None`

#### Returns

`Number` - The current epoch number, as a JSON number.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getCurrentEpoch","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": 601
}
```
