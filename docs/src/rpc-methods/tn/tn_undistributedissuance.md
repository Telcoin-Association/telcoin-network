# tn\_undistributedIssuance

Returns the issuance the `ConsensusRegistry` contract has not yet distributed to validators, read from the contract state at this node's canonical tip, the newest block it has executed. Rewards are distributed at each epoch boundary by the `applyIncentives` system call; see [Epoch Boundaries](../../epoch-boundaries.md).

#### Parameters

`None`

#### Returns

`QUANTITY` - The undistributed issuance, in wei.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_undistributedIssuance","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x3"
}
```
