# tn\_getBalanceBreakdown

Returns the balance breakdown (outstanding balance, initial stake, rewards) for a given validator address, read from the `ConsensusRegistry` contract at the node's canonical tip. The contract returns three unnamed `uint256` values; the node names them in the object below. For an address that was never a validator, `outstandingBalance` and `rewards` are `0x0` and `initialStake` is the stake amount of stake version 0.

#### Parameters

`DATA`, 20 bytes - The validator's execution address.

#### Returns

`Object` - The balance breakdown:

* `outstandingBalance`: `QUANTITY` - Hexadecimal of the validator's balance held by the registry, in wei: its stake plus the rewards it has not claimed.
* `initialStake`: `QUANTITY` - Hexadecimal of the stake amount of the validator's stake version, in wei; see [How Staking Works](../../staking/how-staking-works.md).
* `rewards`: `QUANTITY` - Hexadecimal of the claimable rewards accrued, in wei: the part of `outstandingBalance` above `initialStake`, or `0x0` if there is none. The same value [tn\_getRewards](tn_getrewards.md) returns.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_getBalanceBreakdown","params":["0x3518b301b86ceb53b5a3dff62e55cd43ef59d024"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "outstandingBalance": "0x3cb42afc2b7a0162dfb99", // about 4,586,648 TEL
    "initialStake": "0xd3c21bcecceda1000000", // 1,000,000 TEL
    "rewards": "0x2f78093f3eab2752dfb99" // about 3,586,648 TEL
  }
}
```
