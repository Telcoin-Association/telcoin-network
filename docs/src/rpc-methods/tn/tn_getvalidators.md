# tn\_getValidators

Returns the validators the `ConsensusRegistry` contract holds under one status, read at the node's canonical tip. Pass `"Any"` to return validators of every status; `"Undefined"` reverts on-chain.

The registry keeps one set of validators per status and has no query for `"Any"`, so the node builds that answer itself: it reads the `Staked`, `PendingActivation`, `Active`, `PendingExit` and `Exited` sets against the same block and concatenates them in that order. A validator is in exactly one set at a time. Retired validators are in no set, so neither a single status nor `"Any"` returns them; [tn\_getValidator](tn_getvalidator.md) still answers for a retired address.

#### Parameters

`String` - The validator status by name: `"Staked"`, `"PendingActivation"`, `"Active"`, `"PendingExit"`, `"Exited"` or `"Any"`. Names are case-sensitive, and a number is rejected as an invalid parameter. `"Undefined"` reverts with the registry's `InvalidStatus(uint8)` error; an unrecognised name reverts with empty revert data.

#### Returns

`Array` - The validators with the requested status, in the registry's set order, one object each:

* `validatorAddress`: `DATA`, 20 bytes - The validator's execution address, which also identifies its ConsensusNFT.
* `activationEpoch`: `Number` - The epoch in which the validator became, or will become, active.
* `exitEpoch`: `Number` - The epoch in which the validator exited; `0` until it begins to exit and `4294967295` while it is `PendingExit`.
* `currentStatus`: `String` - The validator's status: `"Staked"`, `"PendingActivation"`, `"Active"`, `"PendingExit"` or `"Exited"`.
* `isRetired`: `Boolean` - Whether the validator is permanently retired; always `false` here, since retired validators are in no status set.
* `stakeVersion`: `Number` - The stake version the validator staked under or last upgraded to; see [How Staking Works](../../staking/how-staking-works.md).
* `region`: `Number` - The geographic region governance assigned to the validator, `0` when unspecified. The node does not use it.

The numeric fields are JSON numbers, not hex strings. An empty status set returns `[]`.

A revert is reported as `eth_call` reports one: error code `3`, a message that starts with `execution reverted`, and the ABI-encoded revert data in `data`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getValidators","params":["Active"],"id":1}'
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
      "validatorAddress": "0x89dab9f6fdc569c1bcdbd6493f25b7040b55dc79",
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
      "validatorAddress": "0xefaacf04b92298a88200aa50aa6bb7bfce587b17",
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
    }
  ]
}
```
