# tn\_getCurrentEpochInfo

Returns the `ConsensusRegistry` contract's entry for the current epoch: its committee, issuance, starting block and duration. The entry is read from the contract state at this node's canonical tip, the newest block it has executed, so a node that is still catching up reports the epoch of its own tip, and the answer moves to the next epoch when the node executes the block that closes the current one.

This is contract state. It is distinct from [tn\_epochRecord](tn_epochrecord.md), which serves the consensus layer's signed epoch records from the node's database and has no record for the current epoch until it ends. [tn\_getEpochInfo](tn_getepochinfo.md) returns the same entry for another epoch.

#### Parameters

`None`

#### Returns

`Object` - The epoch info object:

* `committee`: `Array` - The execution addresses (`DATA`, 20 bytes) of the epoch's committee members.
* `epochIssuance`: `QUANTITY` - The TEL issuance, in wei, the epoch distributes among the validators that led consensus commits during it; see [Epoch Boundaries](../../epoch-boundaries.md).
* `blockHeight`: `Number` - The execution block height at which the epoch started and its committee became active.
* `epochId`: `Number` - The epoch number.
* `epochDuration`: `Number` - The epoch's duration in seconds, set at the start of the epoch from the registry's stake configuration.
* `stakeVersion`: `Number` - The stake configuration version used for the epoch's reward calculations.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getCurrentEpochInfo","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "committee": [
      "0x0033a370616805b1fd275b7ffab83fc41d665ccb",
      "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024",
      "0x7489025dfbaad94f2366d88a62989147d9c8b5d3",
      "0x89dab9f6fdc569c1bcdbd6493f25b7040b55dc79",
      "0xefaacf04b92298a88200aa50aa6bb7bfce587b17"
    ],
    "epochIssuance": "0x576f23131f3c2780000",
    "blockHeight": 500811,
    "epochId": 601,
    "epochDuration": 21600,
    "stakeVersion": 0
  }
}
```
