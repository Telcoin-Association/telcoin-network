# tn\_getCommitteeValidators

Returns the committee validators for the given epoch, read from the `ConsensusRegistry` contract at the node's canonical tip.

The registry keeps committees only for a window around the current epoch: the three epochs before it, the current epoch, and the next two, whose committees are already chosen. An epoch outside that window reverts with the registry's `InvalidEpoch(uint32)` error. Each object is the validator's record as it is now, not as it was during that epoch, so a past committee member that has since begun to exit shows its current status. [tn\_getCommitteeBlsPubkeys](tn_getcommitteeblspubkeys.md) returns the same committee's BLS keys in the same order.

#### Parameters

`Number` - The epoch number, as a JSON number. A hex string is rejected with error `-32602`.

#### Returns

`Array` - The epoch's committee in the order the registry stores it, one object per validator:

* `validatorAddress`: `DATA`, 20 bytes - The validator's execution address, which also identifies its ConsensusNFT.
* `activationEpoch`: `Number` - The epoch in which the validator became, or will become, active.
* `exitEpoch`: `Number` - The epoch in which the validator exited; `0` until it begins to exit and `4294967295` while it is `PendingExit`.
* `currentStatus`: `String` - The validator's current status, for example `"Active"` or `"PendingExit"`.
* `isRetired`: `Boolean` - Whether the validator is permanently retired.
* `stakeVersion`: `Number` - The stake version the validator staked under or last upgraded to; see [How Staking Works](../../staking/how-staking-works.md).
* `region`: `Number` - The geographic region governance assigned to the validator, `0` when unspecified. The node does not use it.

The numeric fields are JSON numbers, not hex strings.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getCommitteeValidators","params":[601],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "validatorAddress": "0x0033a370616805b1fd275b7ffab83fc41d665ccb",
      "activationEpoch": 0,
      "exitEpoch": 0,
      "currentStatus": "Active",
      "isRetired": false,
      "stakeVersion": 0,
      "region": 0
    },
    {
      "validatorAddress": "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024",
      "activationEpoch": 0,
      "exitEpoch": 0,
      "currentStatus": "Active",
      "isRetired": false,
      "stakeVersion": 0,
      "region": 0
    },
    {
      "validatorAddress": "0x7489025dfbaad94f2366d88a62989147d9c8b5d3",
      "activationEpoch": 0,
      "exitEpoch": 0,
      "currentStatus": "Active",
      "isRetired": false,
      "stakeVersion": 0,
      "region": 0
    },
    {
      "validatorAddress": "0x89dab9f6fdc569c1bcdbd6493f25b7040b55dc79",
      "activationEpoch": 0,
      "exitEpoch": 0,
      "currentStatus": "Active",
      "isRetired": false,
      "stakeVersion": 0,
      "region": 0
    },
    {
      "validatorAddress": "0xefaacf04b92298a88200aa50aa6bb7bfce587b17",
      "activationEpoch": 0,
      "exitEpoch": 0,
      "currentStatus": "Active",
      "isRetired": false,
      "stakeVersion": 0,
      "region": 0
    }
  ]
}
```
