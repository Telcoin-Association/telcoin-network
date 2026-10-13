# tn\_getNextCommitteeSize

Returns the committee size for the next epoch, as the `ConsensusRegistry` contract records it in the state at this node's canonical tip, the newest block it has executed. When the protocol selects the next committee at an epoch boundary, it shuffles the eligible validators and trims the list to this size; see [Epoch Boundaries](../../epoch-boundaries.md).

#### Parameters

`None`

#### Returns

`Number` - The number of validators in the next epoch's committee, as a JSON number.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getNextCommitteeSize","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": 5
}
```
