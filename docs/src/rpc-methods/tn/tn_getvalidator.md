# tn\_getValidator

Returns the `ValidatorInfo` record for a given validator address, read from the `ConsensusRegistry` contract at the node's canonical tip.

The address must hold a ConsensusNFT or belong to a retired validator; any other address reverts with the registry's `InvalidTokenId(uint256)` error. An address that holds a ConsensusNFT but has not staked yet returns a record with `currentStatus` `"Undefined"` and the numeric fields at `0`. A retired validator's record is kept after its ConsensusNFT is gone and reports `currentStatus` `"Any"` with `isRetired` `true`.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`Object` - The validator's record:

* `validatorAddress`: `DATA`, 20 bytes - The validator's execution address, which also identifies its ConsensusNFT.
* `activationEpoch`: `Number` - The epoch in which the validator became, or will become, active.
* `exitEpoch`: `Number` - The epoch in which the validator exited; `0` until it begins to exit and `4294967295` while it is `PendingExit`.
* `currentStatus`: `String` - The validator's status: `"Undefined"`, `"Staked"`, `"PendingActivation"`, `"Active"`, `"PendingExit"`, `"Exited"`, or `"Any"` once retired.
* `isRetired`: `Boolean` - Whether the validator is permanently retired; a retired address can never rejoin.
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
 --data '{"jsonrpc":"2.0","method":"tn_getValidator","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "validatorAddress": "0x3518b301b86ceb53b5a3dff62e55cd43ef59d024",
    "activationEpoch": 0,
    "exitEpoch": 0,
    "currentStatus": "Active",
    "isRetired": false,
    "stakeVersion": 0,
    "region": 0
  }
}
```
